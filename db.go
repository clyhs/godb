package godb

import (
	"database/sql"
	"context"
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
	return nil, nil
}

// Exec runs an arbitrary SQL statement.  args represent the bind parameters.
// This is equivalent to running:  Exec() using database/sql
func (dbUtils *DbUtils) Exec(query string, args ...interface{}) (sql.Result, error) {

	return nil,nil

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
	return nil
}

func (dbUtils *DbUtils) QueryRow(query string, args ...interface{}) *sql.Row {

	return nil
}

func (dbUtils *DbUtils) Query(q string, args ...interface{}) (*sql.Rows, error) {

	return query(dbUtils, q, args...)
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
