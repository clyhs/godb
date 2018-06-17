package godb

import (
	"database/sql"
	"reflect"
	"sync"
)
var (
	DefaultCacheSize = 200
)

type cacheStruct struct {
	value reflect.Value
	idx   int
}

type DB struct {
	*sql.DB
	reflectCache      map[reflect.Type]*cacheStruct
	reflectCacheMutex sync.RWMutex
}


func Open(driverName string,dataSourceName string)(*DB,error)  {
	db,err:=sql.Open(driverName,dataSourceName);
	if err!=nil {
		return nil,err
	}
	return &DB{DB:db,reflectCache:make(map[reflect.Type]*cacheStruct)},nil
}

func (db *DB) reflectNew(typ reflect.Type) reflect.Value {
	db.reflectCacheMutex.Lock()
	defer db.reflectCacheMutex.Unlock()
	cs, ok := db.reflectCache[typ]
	if !ok || cs.idx+1 > DefaultCacheSize-1 {
		cs = &cacheStruct{reflect.MakeSlice(reflect.SliceOf(typ), DefaultCacheSize, DefaultCacheSize), 0}
		db.reflectCache[typ] = cs
	} else {
		cs.idx = cs.idx + 1
	}
	return cs.value.Index(cs.idx).Addr()
}

func (db *DB) Query(query string, args ...interface{}) (*Rows, error) {
	rows, err := db.DB.Query(query, args...)
	if err != nil {
		if rows != nil {
			rows.Close()
		}
		return nil, err
	}
	return &Rows{rows, db}, nil
}

