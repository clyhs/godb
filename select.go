package godb

import (
	"database/sql"
	"fmt"
	"reflect"
)

func SelectInt(queryRunner SqlQueryRunner, query string, args ...interface{}) (int64, error) {
	var h int64
	err := selectVal(queryRunner, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return h, nil
}

func SelectNullInt(queryRunner SqlQueryRunner, query string, args ...interface{}) (sql.NullInt64, error) {
	var h sql.NullInt64
	err := selectVal(queryRunner, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

func SelectFloat(queryRunner SqlQueryRunner, query string, args ...interface{}) (float64, error) {
	var h float64
	err := selectVal(queryRunner, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return h, nil
}

func SelectNullFloat(queryRunner SqlQueryRunner, query string, args ...interface{}) (sql.NullFloat64, error) {
	var h sql.NullFloat64
	err := selectVal(queryRunner, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

func SelectStr(queryRunner SqlQueryRunner, query string, args ...interface{}) (string, error) {
	var h string
	err := selectVal(queryRunner, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	return h, nil
}

func SelectNullStr(queryRunner SqlQueryRunner, query string, args ...interface{}) (sql.NullString, error) {
	var h sql.NullString
	err := selectVal(queryRunner, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

func selectVal(queryRunner SqlQueryRunner, holder interface{}, query string, args ...interface{}) error {

	if len(args) == 1 {
		switch m := queryRunner.(type) {
		case *DbUtils:
			query, args = maybeExpandNamedQuery(m, query, args)
		case *Transaction:
			query, args = maybeExpandNamedQuery(m.dbUtils, query, args)
		}
	}

	fmt.Println(len(args))
	fmt.Println(query)

	rows, err := queryRunner.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	return rows.Scan(holder)
}

func SelectOne(dbUtils *DbUtils, queryRunner SqlQueryRunner, holder interface{}, query string, args ...interface{}) error {

	t:=reflect.TypeOf(holder)
	if t.Kind() ==reflect.Ptr{
		t = t.Elem()
		fmt.Println(t)
	}else {
		return fmt.Errorf("godb: SelectOne holder must be a pointer, but got: %t", holder)
	}

	isptr := false
	if t.Kind() == reflect.Ptr {
		isptr = true
		t = t.Elem()
	}else{
		fmt.Println(t.Kind())
	}

	fmt.Println(isptr)

	if t.Kind() == reflect.Struct {
		var nonFatalErr error
		list, err := rawselect(dbUtils, queryRunner, holder, query, args...)

		if err != nil {
			if !NonFatalError(err) { // FIXME: double negative, rename NonFatalError to FatalError
				return err
			}
			nonFatalErr = err
		}

		dest := reflect.ValueOf(holder)
		if isptr {
			dest = dest.Elem()
		}

		if list != nil && len(list) > 0 {

			fmt.Println(len(list))
			if len(list) > 1 {
				return fmt.Errorf("godb: multiple rows returned for: %s - %v", query, args)
			}

			// Initialize if nil
			if dest.IsNil() {
				dest.Set(reflect.New(t))
			}
			src := reflect.ValueOf(list[0])
			dest.Elem().Set(src.Elem())
		}else{
			return sql.ErrNoRows
		}

		return nonFatalErr
	}

	return selectVal(dbUtils, holder, query, args...)
}

func rawselect(dbUtils *DbUtils, queryRunner SqlQueryRunner, i interface{}, query string,
	args ...interface{}) ([]interface{}, error) {

	var (
		appendToSlice   = false // Write results to i directly?
		intoStruct      = true  // Selecting into a struct?
		pointerElements = true  // Are the slice elements pointers (vs values)?
	)
	t, err := toType(i)
	if err != nil {
		var err2 error
		if t, err2 = toSliceType(i); t == nil {
			if err2 != nil {
				return nil, err2
			}
			return nil, err
		}
		pointerElements = t.Kind() == reflect.Ptr
		if pointerElements {
			t = t.Elem()
		}
		appendToSlice = true
		intoStruct = t.Kind() == reflect.Struct
	}

	fmt.Println(appendToSlice)

	if len(args) == 1 {
		query, args = maybeExpandNamedQuery(dbUtils, query, args)
	}

	fmt.Println(query)


	rows, err := queryRunner.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !intoStruct && len(cols) > 1 {
		return nil, fmt.Errorf("godb: select into non-struct slice requires 1 column, got %d", len(cols))
	}

	var nonFatalErr error

	var colToFieldIndex [][]int

	colToFieldIndex, err = columnToFieldIndex(dbUtils, t, cols)
	if err != nil {

		if !NonFatalError(err) {
			return nil, err
		}
		nonFatalErr = err
	}


	fmt.Println(colToFieldIndex)

	//list       = make([]interface{}, 0)
	var (
		list       = make([]interface{}, 0)
		//sliceValue = reflect.Indirect(reflect.ValueOf(i))
	)


	for{
		if !rows.Next() {
			// if error occured return rawselect
			if rows.Err() != nil {
				return nil, rows.Err()
			}
			// time to exit from outer "for" loop
			break

		}
		v := reflect.New(t)

		dest := make([]interface{}, len(cols))

		for x := range cols {
			f := v.Elem()
			if intoStruct {
				index := colToFieldIndex[x]
				if index == nil {
					continue
				}
				f = f.FieldByIndex(index)
			}
			target := f.Addr().Interface()
			dest[x] = target
		}

		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		list = append(list, v.Interface())
	}

	return list, nonFatalErr
}

func selectlist(dbUtils *DbUtils, queryRunner SqlQueryRunner, i interface{}, query string,
	args ...interface{}) ([]interface{}, error) {

	var nonFatalErr error

	list, err := rawselect(dbUtils, queryRunner, i, query, args...)
	if err != nil {
		if !NonFatalError(err) {
			return nil, err
		}
		nonFatalErr = err
	}


	return list, nonFatalErr
}