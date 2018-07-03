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

func columnToFieldIndex(m *DbUtils, t reflect.Type, cols []string) ([][]int, error) {

	colToFieldIndex := make([][]int, len(cols))

	missingColNames := []string{}

	for x := range cols {
		colName := strings.ToLower(cols[x])
		field, found := t.FieldByNameFunc(func(fieldName string) bool {

			field, _ := t.FieldByName(fieldName)
			fmt.Println(fieldName)
			cArguments := strings.Split(field.Tag.Get("db"), ",")
			fieldName = cArguments[0]

			if fieldName == "-" {
				return false
			} else if fieldName == "" {
				fieldName = field.Name
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
		fmt.Println(missingColNames)
		return colToFieldIndex, &NoFieldInTypeError{
			TypeName:        t.Name(),
			MissingColNames: missingColNames,
		}
	}
	return colToFieldIndex, nil
}



func get(dbUtils *DbUtils, queryRunner SqlQueryRunner, i interface{},
	keys ...interface{}) (interface{}, error) {
    return nil,nil
}

func delete(dbUtils *DbUtils, queryRunner SqlQueryRunner, list ...interface{}) (int64, error) {
	return 0, nil
}

func update(dbUtils *DbUtils, queryRunner SqlQueryRunner, list ...interface{}) (int64, error) {
	return 0, nil
}

func insert(dbUtils *DbUtils, queryRunner SqlQueryRunner, list ...interface{}) error {

	for _, ptr := range list {

		table, elem, err := dbUtils.tableForPointer(ptr, false)
		if err != nil {
			return err
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



