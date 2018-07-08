package godb

import (
	"database/sql"
	"context"
	"reflect"
	"strings"
	"fmt"
	"strconv"
	"database/sql/driver"
	"errors"
)
var (
	DefaultCacheSize = 200
)
/*
type cacheStruct struct {
	value reflect.Value
	idx   int
}

type DbUtils struct {
	Db *sql.DB
	reflectCache      map[reflect.Type]*cacheStruct
	reflectCacheMutex sync.RWMutex
}*/

type DbUtils struct {
	ctx           context.Context
	Db            *sql.DB
	tables        []*TableMap
	Dialect       Dialect
	TypeConverter TypeConverter
}


func Open(driverName string,dataSourceName string)(*DbUtils,error)  {
	db,err:=sql.Open(driverName,dataSourceName);
	if err!=nil {
		return nil,err
	}
	//return &DbUtils{Db:db,reflectCache:make(map[reflect.Type]*cacheStruct)},nil
	return &DbUtils{Db:db},nil

}

func (dbUtils *DbUtils) WithContext(ctx context.Context) SqlQueryRunner {
	copy := &DbUtils{}
	*copy = *dbUtils
	copy.ctx = ctx
	return copy
}

func (dbUtils *DbUtils) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return get(dbUtils, dbUtils, i, keys...)
}

func (dbUtils *DbUtils) Insert(list ...interface{}) error {

	return insert(dbUtils, dbUtils, list...)
}

func (dbUtils *DbUtils) Update(list ...interface{}) (int64, error) {
	return update(dbUtils, dbUtils, list...)
}

func (dbUtils *DbUtils) Delete(list ...interface{}) (int64, error) {
	return delete(dbUtils, dbUtils, list...)
}


func (dbUtils *DbUtils) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return selectlist(dbUtils,dbUtils,i,query,args...)
}

// Exec runs an arbitrary SQL statement.  args represent the bind parameters.
// This is equivalent to running:  Exec() using database/sql
func (dbUtils *DbUtils) Exec(query string, args ...interface{}) (sql.Result, error) {

	return maybeExpandNamedQueryAndExec(dbUtils,query,args...)

}

// SelectInt is a convenience wrapper around the gorp.SelectInt function
func (dbUtils *DbUtils) SelectInt(query string, args ...interface{}) (int64, error) {
	return SelectInt(dbUtils, query, args...)
}

// SelectNullInt is a convenience wrapper around the gorp.SelectNullInt function
func (dbUtils *DbUtils) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	return sql.NullInt64{}, nil
}

// SelectFloat is a convenience wrapper around the gorp.SelectFloat function
func (dbUtils *DbUtils) SelectFloat(query string, args ...interface{}) (float64, error) {
	return 0, nil
}

// SelectNullFloat is a convenience wrapper around the gorp.SelectNullFloat function
func (dbUtils *DbUtils) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	return sql.NullFloat64{}, nil
}

// SelectStr is a convenience wrapper around the gorp.SelectStr function
func (dbUtils *DbUtils) SelectStr(query string, args ...interface{}) (string, error) {
	return "", nil
}

// SelectNullStr is a convenience wrapper around the gorp.SelectNullStr function
func (dbUtils *DbUtils) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	return sql.NullString{}, nil
}

// SelectOne is a convenience wrapper around the gorp.SelectOne function
func (dbUtils *DbUtils) SelectOne(holder interface{}, query string, args ...interface{}) error {
	return SelectOne(dbUtils,dbUtils,holder,query,args...)
}

func (dbUtils *DbUtils) QueryRow(query string, args ...interface{}) *sql.Row {

	fmt.Println("queryRow")
	return queryRow(dbUtils,query,args...)
}

func (dbUtils *DbUtils) Query(q string, args ...interface{}) (*sql.Rows, error) {
	return query(dbUtils, q, args...)
}

func (dbUtils *DbUtils) AddTable(i interface{}) *TableMap {
	return dbUtils.AddTableWithName(i, "")
}

func (dbUtils *DbUtils) AddTableWithName(i interface{}, name string) *TableMap {
	return dbUtils.AddTableWithNameAndSchema(i, "", name)
}

func (dbUtils *DbUtils) AddTableWithNameAndSchema(i interface{}, schema string, name string) *TableMap {
	t := reflect.TypeOf(i)
	if name == "" {
		name = t.Name()
	}

	// check if we have a table for this type already
	// if so, update the name and return the existing pointer
	for i := range dbUtils.tables {
		table := dbUtils.tables[i]
		if table.gotype == t {
			table.TableName = name
			return table
		}
	}

	tmap := &TableMap{gotype: t, TableName: name, SchemaName: schema, dbUtils: dbUtils}
	var primaryKey []*ColumnMap

	tmap.Columns, primaryKey = dbUtils.readStructColumns(t)
	dbUtils.tables = append(dbUtils.tables, tmap)
	if len(primaryKey) > 0 {
		tmap.keys = append(tmap.keys, primaryKey...)
	}

	return tmap
}

func (dbUtils *DbUtils) readStructColumns(t reflect.Type) (cols []*ColumnMap, primaryKey []*ColumnMap) {

	primaryKey = make([]*ColumnMap, 0)

	n := t.NumField()
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {

			subcols, subpk := dbUtils.readStructColumns(f.Type)
			// Don't append nested fields that have the same field
			// name as an already-mapped field.
			for _, subcol := range subcols {
				shouldAppend := true
				for _, col := range cols {
					if !subcol.Transient && subcol.fieldName == col.fieldName {
						shouldAppend = false
						break
					}
				}
				if shouldAppend {
					cols = append(cols, subcol)
				}
			}
			if subpk != nil {
				primaryKey = append(primaryKey, subpk...)
			}
		}else{
			cArguments := strings.Split(f.Tag.Get("db"), ",")
			columnName := cArguments[0]
			var maxSize int
			var defaultValue string
			var isAuto bool
			var isPK bool
			var isNotNull bool
			for _, argString := range cArguments[1:] {
				argString = strings.TrimSpace(argString)
				arg := strings.SplitN(argString, ":", 2)

				// check mandatory/unexpected option values
				switch arg[0] {
				case "size", "default":
					// options requiring value
					if len(arg) == 1 {
						panic(fmt.Sprintf("missing option value for option %v on field %v", arg[0], f.Name))
					}
				default:
					// options where value is invalid (currently all other options)
					if len(arg) == 2 {
						panic(fmt.Sprintf("unexpected option value for option %v on field %v", arg[0], f.Name))
					}
				}

				switch arg[0] {
				case "size":
					maxSize, _ = strconv.Atoi(arg[1])
				case "default":
					defaultValue = arg[1]
				case "primarykey":
					isPK = true
				case "autoincrement":
					isAuto = true
				case "notnull":
					isNotNull = true
				default:
					panic(fmt.Sprintf("Unrecognized tag option for field %v: %v", f.Name, arg))
				}
			}
			if columnName == "" {
				columnName = f.Name
			}

			gotype := f.Type
			valueType := gotype
			if valueType.Kind() == reflect.Ptr {
				valueType = valueType.Elem()
			}
			value := reflect.New(valueType).Interface()
			if dbUtils.TypeConverter != nil {
				// Make a new pointer to a value of type gotype and
				// pass it to the TypeConverter's FromDb method to see
				// if a different type should be used for the column
				// type during table creation.
				scanner, useHolder := dbUtils.TypeConverter.FromDb(value)
				if useHolder {
					value = scanner.Holder
					gotype = reflect.TypeOf(value)
				}
			}
			if typer, ok := value.(SqlTyper); ok {
				gotype = reflect.TypeOf(typer.SqlType())
			}else if valuer, ok := value.(driver.Valuer); ok {
				// Only check for driver.Valuer if SqlTyper wasn't
				// found.
				v, err := valuer.Value()
				if err == nil && v != nil {
					gotype = reflect.TypeOf(v)
				}
			}
			cm := &ColumnMap{
				ColumnName:   columnName,
				DefaultValue: defaultValue,
				Transient:    columnName == "-",
				fieldName:    f.Name,
				gotype:       gotype,
				isPK:         isPK,
				isAutoIncr:   isAuto,
				isNotNull:    isNotNull,
				MaxSize:      maxSize,
			}
			if isPK {
				primaryKey = append(primaryKey, cm)
			}
			shouldAppend := true
			for index, col := range cols {
				if !col.Transient && col.fieldName == cm.fieldName {
					cols[index] = cm
					shouldAppend = false
					break
				}
			}
			if shouldAppend {
				cols = append(cols, cm)
			}

		}
	}
	return
}



func (dbUtils *DbUtils) CreateTables() error {
	return dbUtils.createTables(false)
}

func (dbUtils *DbUtils) CreateTablesIfNotExists() error {
	return dbUtils.createTables(true)
}

func (dbUtils *DbUtils) createTables(ifNotExists bool) error {
	var err error
	for i := range dbUtils.tables {
		table := dbUtils.tables[i]
		sql := table.CreateTableSql(ifNotExists)
		_, err = dbUtils.Exec(sql)
		if err != nil {
			return err
		}
	}
	return err
}

func (dbUtils *DbUtils) DropTables() error {
	return dbUtils.dropTables(false)
}

// DropTablesIfExists is the same as DropTables, but uses the "if exists" clause to
// avoid errors for tables that do not exist.
func (dbUtils *DbUtils) DropTablesIfExists() error {
	return dbUtils.dropTables(true)
}

// Goes through all the registered tables, dropping them one by one.
// If an error is encountered, then it is returned and the rest of
// the tables are not dropped.
func (dbUtils *DbUtils) dropTables(addIfExists bool) (err error) {
	for _, table := range dbUtils.tables {
		err = dbUtils.dropTableImpl(table, addIfExists)
		if err != nil {
			return err
		}
	}

	return err
}

// Implementation of dropping a single table.
func (dbUtils *DbUtils) dropTable(t reflect.Type, name string, addIfExists bool) error {
	table := tableOrNil(dbUtils, t, name)
	if table == nil {
		return fmt.Errorf("table %s was not registered", table.TableName)
	}

	return dbUtils.dropTableImpl(table, addIfExists)
}

func (dbUtils *DbUtils) dropTableImpl(table *TableMap, ifExists bool) (err error) {
	tableDrop := "drop table"
	if ifExists {
		tableDrop = dbUtils.Dialect.IfTableExists(tableDrop, table.SchemaName, table.TableName)
	}
	_, err = dbUtils.Exec(fmt.Sprintf("%s %s;", tableDrop, dbUtils.Dialect.QuotedTableForQuery(table.SchemaName, table.TableName)))
	return err
}
func tableOrNil(dbUtils *DbUtils, t reflect.Type, name string) *TableMap {

	if name!=""{
		for i := range dbUtils.tables {
			table := dbUtils.tables[i]
			if table.TableName == name {
				return table
			}
		}
	}

	for i := range dbUtils.tables {
		table := dbUtils.tables[i]
		if table.gotype == t {
			return table
		}
	}
	return nil
}

func (dbUtils *DbUtils) TableFor(t reflect.Type, checkPK bool) (*TableMap, error) {
	table := tableOrNil(dbUtils, t, "")
	if table == nil {
		return nil, fmt.Errorf("no table found for type: %v", t.Name())
	}

	if checkPK && len(table.keys) < 1 {
		e := fmt.Sprintf("godb: no keys defined for table: %s",
			table.TableName)
		return nil, errors.New(e)
	}

	return table, nil
}

func (dbUtils *DbUtils) tableForPointer(ptr interface{}, checkPK bool) (*TableMap, reflect.Value, error) {

	ptrv := reflect.ValueOf(ptr)

	fmt.Println(ptrv.Kind())

	if ptrv.Kind() != reflect.Ptr {
		e := fmt.Sprintf("godb: passed non-pointer: %v (kind=%v)", ptr,
			ptrv.Kind())
		return nil, reflect.Value{}, errors.New(e)
	}
	elem := ptrv.Elem()
	ifc := elem.Interface()
	var t *TableMap
	var err error
	etype := reflect.TypeOf(ifc)
	t, err = dbUtils.TableFor(etype, checkPK)
	if err != nil {
		return nil, reflect.Value{}, err
	}

	return t, elem, nil
}

func (dbUtils *DbUtils) Begin() (*Transaction, error) {

	tx, err := begin(dbUtils)
	if err != nil {
		return nil, err
	}
	return &Transaction{
		dbUtils:  dbUtils,
		tx:       tx,
		closed:   false,
	}, nil
}

func (dbUtils *DbUtils) Prepare(query string) (*sql.Stmt, error) {

	return prepare(dbUtils, query)
}


/*
func (dbUtils *DbUtils) reflectNew(typ reflect.Type) reflect.Value {
	dbUtils.reflectCacheMutex.Lock()
	defer dbUtils.reflectCacheMutex.Unlock()
	cs, ok := dbUtils.reflectCache[typ]
	if !ok || cs.idx+1 > DefaultCacheSize-1 {
		cs = &cacheStruct{reflect.MakeSlice(reflect.SliceOf(typ), DefaultCacheSize, DefaultCacheSize), 0}
		dbUtils.reflectCache[typ] = cs
	} else {
		cs.idx = cs.idx + 1
	}
	return cs.value.Index(cs.idx).Addr()
}*/
/*
func (dbUtils *DbUtils) Query(query string, args ...interface{}) (*RowsMap, error) {
	//rows, err := db.DB.Query(query, args...)
	rows,err:=dbUtils.Db.Query(query)
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, err
	}
	return &RowsMap{rows, dbUtils}, nil
}
*/
