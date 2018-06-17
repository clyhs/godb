package godb

import (
	"database/sql"
	"errors"
	"reflect"
	"fmt"
)

type Rows struct {
	*sql.Rows
	db *DB
}

func (rs *Rows)ScanStructByIndex(dest ...interface{}) error {

	if len(dest) == 0 {
		return errors.New("at least one struct")
	}
	vvvs := make([]reflect.Value, len(dest))
	for i, s := range dest {
		vv := reflect.ValueOf(s)
		if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Struct {
			return errors.New("dest should be a struct's pointer")
		}

		vvvs[i] = vv.Elem()
	}

	cols, err := rs.Columns()
	if err != nil {
		return err
	}
	newDest := make([]interface{}, len(cols))

	var i = 0
	for _, vvv := range vvvs {
		for j := 0; j < vvv.NumField(); j++ {
			newDest[i] = vvv.Field(j).Addr().Interface()
			i = i + 1
		}
	}

	return rs.Rows.Scan(newDest...)
}

func (rs *Rows) ScanMap(dest interface{}) error {
	vv := reflect.ValueOf(dest)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return errors.New("dest should be a map's pointer")
	}

	cols, err := rs.Columns()
	if err != nil {
		return err
	}

	newDest := make([]interface{}, len(cols))
	vvv := vv.Elem()

	for i, _ := range cols {
		newDest[i] = rs.db.reflectNew(vvv.Type().Elem()).Interface()
	}

	err = rs.Rows.Scan(newDest...)
	if err != nil {
		return err
	}

	for i, name := range cols {
		vname := reflect.ValueOf(name)
		fmt.Println(reflect.TypeOf(newDest[i]))
		fmt.Println(reflect.ValueOf(newDest[i]))
		fmt.Println(reflect.ValueOf(newDest[i]).Elem())
		vvv.SetMapIndex(vname, reflect.ValueOf(newDest[i]).Elem())
	}

	return nil
}

func (rs *Rows) ScanSlice(dest interface{}) error {
	vv := reflect.ValueOf(dest)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Slice {
		return errors.New("dest should be a slice's pointer")
	}

	vvv := vv.Elem()
	cols, err := rs.Columns()
	if err != nil {
		return err
	}

	newDest := make([]interface{}, len(cols))

	for j := 0; j < len(cols); j++ {
		if j >= vvv.Len() {
			newDest[j] = reflect.New(vvv.Type().Elem()).Interface()
		} else {
			newDest[j] = vvv.Index(j).Addr().Interface()
		}
	}

	err = rs.Rows.Scan(newDest...)
	if err != nil {
		return err
	}

	srcLen := vvv.Len()
	for i := srcLen; i < len(cols); i++ {
		vvv = reflect.Append(vvv, reflect.ValueOf(newDest[i]).Elem())
	}
	return nil
}

type Row struct {
	rows *Rows
	// One of these two will be non-nil:
	err error // deferred error for easy chaining
}

func (row *Row) ScanStructByIndex(dest interface{}) error {
	if row.err != nil {
		return row.err
	}
	defer row.rows.Close()

	if !row.rows.Next() {
		if err := row.rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := row.rows.ScanStructByIndex(dest)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	return row.rows.Close()
}