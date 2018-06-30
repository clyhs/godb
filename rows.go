package godb

import (
	"database/sql"
)

type RowsMap struct {
	Rows *sql.Rows
	dbUtils *DbUtils
}


/*
func (rsMap *RowsMap)ScanStructByIndex(dest ...interface{}) error {

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

	cols, err := rsMap.Rows.Columns()
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

	return rsMap.Rows.Scan(newDest...)
}

func (rsMap *RowsMap) ScanMap(dest interface{}) error {
	vv := reflect.ValueOf(dest)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Map {
		return errors.New("dest should be a map's pointer")
	}

	cols, err := rsMap.Rows.Columns()
	if err != nil {
		return err
	}

	newDest := make([]interface{}, len(cols))
	vvv := vv.Elem()

	for i, _ := range cols {
		newDest[i] = rsMap.DbUtils.reflectNew(vvv.Type().Elem()).Interface()

	}

	err = rsMap.Rows.Scan(newDest...)
	if err != nil {
		return err
	}

	for i, name := range cols {
		vname := reflect.ValueOf(name)
		//fmt.Println(reflect.TypeOf(newDest[i]))
		//fmt.Println(reflect.ValueOf(newDest[i]))
		//fmt.Println(reflect.ValueOf(newDest[i]).Elem())
		vvv.SetMapIndex(vname, reflect.ValueOf(newDest[i]).Elem())
	}

	return nil
}

func (rsMap *RowsMap) ScanSlice(dest interface{}) error {
	vv := reflect.ValueOf(dest)
	if vv.Kind() != reflect.Ptr || vv.Elem().Kind() != reflect.Slice {
		return errors.New("dest should be a slice's pointer")
	}

	vvv := vv.Elem()
	cols, err := rsMap.Rows.Columns()
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

	err = rsMap.Rows.Scan(newDest...)
	if err != nil {
		return err
	}

	srcLen := vvv.Len()
	for i := srcLen; i < len(cols); i++ {
		vvv = reflect.Append(vvv, reflect.ValueOf(newDest[i]).Elem())
	}
	return nil
}





type RowMap struct {
	RowsMap *RowsMap
	// One of these two will be non-nil:
	Err error // deferred error for easy chaining
}

func (rowMap *RowMap) ScanStructByIndex(dest interface{}) error {
	if rowMap.Err != nil {
		return rowMap.Err
	}
	defer rowMap.RowsMap.Rows.Close()

	if !rowMap.RowsMap.Rows.Next() {
		if err := rowMap.RowsMap.Rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	err := rowMap.RowsMap.ScanStructByIndex(dest)
	if err != nil {
		return err
	}
	// Make sure the query can be processed to completion with no errors.
	return rowMap.RowsMap.Rows.Close()
}

*/