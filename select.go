package godb

import (
	"database/sql"
	"fmt"
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
	return nil
}