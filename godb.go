package godb

import (
	"context"
	"database/sql"
	"reflect"
	"time"
	"database/sql/driver"
	"regexp"
	"fmt"
	"strings"
)

type TypeConverter interface {
	// ToDb converts val to another type. Called before INSERT/UPDATE operations
	ToDb(val interface{}) (interface{}, error)

	// FromDb returns a CustomScanner appropriate for this type. This will be used
	// to hold values returned from SELECT queries.
	//
	// In particular the CustomScanner returned should implement a Binder
	// function appropriate for the Go type you wish to convert the db value to
	//
	// If bool==false, then no custom scanner will be used for this field.
	FromDb(target interface{}) (CustomScanner, bool)
}

type SqlQueryRunner interface {
	WithContext(ctx context.Context) SqlQueryRunner
	Get(i interface{}, keys ...interface{}) (interface{}, error)
	Insert(list ...interface{}) error
	Update(list ...interface{}) (int64, error)
	Delete(list ...interface{}) (int64, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(i interface{}, query string, args ...interface{}) ([]interface{}, error)
	SelectInt(query string, args ...interface{}) (int64, error)
	SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error)
	SelectFloat(query string, args ...interface{}) (float64, error)
	SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error)
	SelectStr(query string, args ...interface{}) (string, error)
	SelectNullStr(query string, args ...interface{}) (sql.NullString, error)
	SelectOne(holder interface{}, query string, args ...interface{}) error
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type SqlTyper interface {
	SqlType() driver.Value
}

// for fields that exists in DB table, but not exists in struct
type dummyField struct{}

// Scan implements the Scanner interface.
func (nt *dummyField) Scan(value interface{}) error {
	return nil
}

func maybeExpandNamedQuery(dbUtils *DbUtils, query string, args []interface{}) (string, []interface{}) {
	var (
		arg    = args[0]
		argval = reflect.ValueOf(arg)
	)
	if argval.Kind() == reflect.Ptr {
		argval = argval.Elem()
	}

	if argval.Kind() == reflect.Map && argval.Type().Key().Kind() == reflect.String {
		return expandNamedQuery(dbUtils, query, func(key string) reflect.Value {
			return argval.MapIndex(reflect.ValueOf(key))
		})
	}
	if argval.Kind() != reflect.Struct {
		return query, args
	}
	if _, ok := arg.(time.Time); ok {
		// time.Time is driver.Value
		return query, args
	}
	if _, ok := arg.(driver.Valuer); ok {
		// driver.Valuer will be converted to driver.Value.
		return query, args
	}

	fmt.Println(query)

	return expandNamedQuery(dbUtils, query, argval.FieldByName)
}


var keyRegexp = regexp.MustCompile(`:[[:word:]]+`)


func expandNamedQuery(dbUtils *DbUtils, query string, keyGetter func(key string) reflect.Value) (string, []interface{}) {
	var (
		n    int
		args []interface{}
	)
	return keyRegexp.ReplaceAllStringFunc(query, func(key string) string {
		val := keyGetter(key[1:])
		if !val.IsValid() {
			return key
		}
		args = append(args, val.Interface())
		newVar := dbUtils.Dialect.BindVar(n)
		n++
		return newVar
	}), args
}

func maybeExpandNamedQueryAndExec(queryRunner SqlQueryRunner, query string, args ...interface{}) (sql.Result, error) {

	dbUtils:=extractDbUtils(queryRunner)
	if len(args) == 1 {
		query, args = maybeExpandNamedQuery(dbUtils, query, args)
	}
	//return exec(queryRunner, query, args...)
	return dbUtils.Db.Exec(query,args...)
}

func extractDbUtils(queryRunner SqlQueryRunner) *DbUtils {
	switch db := queryRunner.(type) {
	case *DbUtils:
		return db
	case *Transaction:
		return db.dbUtils
	}
	return nil
}

/*
func extractExecutorAndContext(e SqlQueryRunner) (reflect.Value, context.Context) {
	switch m := e.(type) {
	case *DbUtils:
		return reflect.ValueOf(m.Db), m.ctx
	case *Transaction:
		return reflect.ValueOf(m.tx), m.ctx
	}
	return reflect.ValueOf(nil), nil
}*/

func columnToFieldIndex(m *DbUtils, t reflect.Type,name string, cols []string) ([][]int, error) {

	colToFieldIndex := make([][]int, len(cols))

	tableMapped := false
	table := tableOrNil(m, t, name)
	if table != nil {
		tableMapped = true
	}
	missingColNames := []string{}

	for x := range cols {
		colName := strings.ToLower(cols[x])
		field, found := t.FieldByNameFunc(func(fieldName string) bool {

			field, _ := t.FieldByName(fieldName)
			cArguments := strings.Split(field.Tag.Get("db"), ",")
			fieldName = cArguments[0]

			if fieldName == "-" {
				return false
			} else if fieldName == "" {
				fieldName = field.Name
			}
			if tableMapped {

				colMap := colMapOrNil(table, fieldName)
				//fmt.Println(colMap)
				//fmt.Println(fieldName)
				if colMap != nil {
					fieldName = colMap.ColumnName
				}
			}
			return colName == strings.ToLower(fieldName)
		})
		if found {
			colToFieldIndex[x] = field.Index
		}
		if colToFieldIndex[x] == nil {
			missingColNames = append(missingColNames, colName)
		}
	}
	if len(missingColNames) > 0 {
		return colToFieldIndex, &NoFieldInTypeError{
			TypeName:        t.Name(),
			MissingColNames: missingColNames,
		}
	}
	return colToFieldIndex, nil
}

func tableFor(dbUtils *DbUtils, t reflect.Type, i interface{}) (*TableMap, error) {

	table, err := dbUtils.TableFor(t, true)
	if err != nil {
		return nil, err
	}
	return table, nil
}

func get(dbUtils *DbUtils, queryRunner SqlQueryRunner, i interface{},
	keys ...interface{}) (interface{}, error) {

	t, err := toType(i)
	if err != nil {
		return nil, err
	}

	table, err := tableFor(dbUtils, t, i)
	if err != nil {
		return nil, err
	}

	plan := table.bindGet()

	v := reflect.New(t)

	dest := make([]interface{}, len(plan.argFields))

	conv := dbUtils.TypeConverter
	custScan := make([]CustomScanner, 0)

	for x, fieldName := range plan.argFields {

		f := v.Elem().FieldByName(fieldName)
		target := f.Addr().Interface()
		if conv != nil {
			scanner, ok := conv.FromDb(target)
			if ok {
				target = scanner.Holder
				custScan = append(custScan, scanner)
			}
		}
		dest[x] = target
	}

	row := queryRunner.QueryRow(plan.query, keys...)

	err = row.Scan(dest...)
	if err != nil {
		if err == sql.ErrNoRows {

			err = nil
		}
		return nil, err
	}

	for _, c := range custScan {
		err = c.Bind()
		if err != nil {
			return nil, err
		}
	}


	return v.Interface(),nil
}

func delete(dbUtils *DbUtils, queryRunner SqlQueryRunner, list ...interface{}) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := dbUtils.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}


		bi, err := table.bindDelete(elem)
		if err != nil {
			return -1, err
		}

		res, err := queryRunner.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		count += rows

	}

	return count, nil
}

func update(dbUtils *DbUtils, queryRunner SqlQueryRunner, list ...interface{}) (int64, error) {

	count := int64(0)

	for _, ptr := range list {
		table, elem, err := dbUtils.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}
		bi, err := table.bindUpdate(elem)

		if err != nil {
			return -1, err
		}

		res, err := queryRunner.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		count += rows
	}


	return count, nil
}

func insert(dbUtils *DbUtils, queryRunner SqlQueryRunner, list ...interface{}) error {

	for i, ptr := range list {
        fmt.Println(i)
		table, elem, err := dbUtils.tableForPointer(ptr, false)

		if err != nil {
			return err
		}
		bi,err:=table.insert(elem)
		fmt.Println(bi)

		if err != nil {
			return err
		}

		if bi.autoIncrIdx > -1 {
			f := elem.FieldByName(bi.autoIncrFieldName)
			switch inserter := dbUtils.Dialect.(type) {
			case IntegerAutoIncrInserter:
				id, err := inserter.InsertAutoIncr(queryRunner, bi.query, bi.args...)
				if err != nil {
					return err
				}
				k := f.Kind()
				if (k == reflect.Int) || (k == reflect.Int16) || (k == reflect.Int32) || (k == reflect.Int64) {
					f.SetInt(id)
				} else if (k == reflect.Uint) || (k == reflect.Uint16) || (k == reflect.Uint32) || (k == reflect.Uint64) {
					f.SetUint(uint64(id))
				} else {
					return fmt.Errorf("godb: cannot set autoincrement value on non-Int field. SQL=%s  autoIncrIdx=%d autoIncrFieldName=%s", bi.query, bi.autoIncrIdx, bi.autoIncrFieldName)
				}
			case TargetedAutoIncrInserter:
				err := inserter.InsertAutoIncrToTarget(queryRunner, bi.query, f.Addr().Interface(), bi.args...)
				if err != nil {
					return err
				}
			case TargetQueryInserter:
				var idQuery = table.ColMap(bi.autoIncrFieldName).GeneratedIdQuery
				if idQuery == "" {
					return fmt.Errorf("godb: cannot set %s value if its ColumnMap.GeneratedIdQuery is empty", bi.autoIncrFieldName)
				}
				err := inserter.InsertQueryToTarget(queryRunner, bi.query, idQuery, f.Addr().Interface(), bi.args...)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("godb: cannot use autoincrement fields on dialects that do not implement an autoincrementing interface")
			}
		}else {
			_, err := queryRunner.Exec(bi.query, bi.args...)
			if err != nil {
				return err
			}
		}

	}

	return nil
}


func query(queryRunner SqlQueryRunner, query string, args ...interface{}) (*sql.Rows, error) {
	switch m := queryRunner.(type) {
	case *DbUtils:
		return m.Db.Query(query,args...)
	case *Transaction:
		return m.tx.Query(query,args...)
	}
	return nil, nil
}

func queryRow(queryRunner SqlQueryRunner, query string, args ...interface{}) *sql.Row {
	switch m := queryRunner.(type) {
	case *DbUtils:
		return m.Db.QueryRow(query,args...)
	case *Transaction:
		return m.tx.QueryRow(query,args...)
	}

	return nil
}

func begin(dbUtils *DbUtils) (*sql.Tx, error) {
	if dbUtils.ctx != nil {
		return dbUtils.Db.BeginTx(dbUtils.ctx, nil)
	}

	return dbUtils.Db.Begin()
}

func prepare(queryRunner SqlQueryRunner, query string) (*sql.Stmt, error) {
	dbUtils:=extractDbUtils(queryRunner)

	if dbUtils.ctx != nil {
		return dbUtils.Db.PrepareContext(dbUtils.ctx, query)
	}

	return dbUtils.Db.Prepare(query)
}
