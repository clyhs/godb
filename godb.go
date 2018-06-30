package godb

import (
	"context"
	"database/sql"
	"reflect"
	"time"
	"database/sql/driver"
	"regexp"
)

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
	return nil
}

