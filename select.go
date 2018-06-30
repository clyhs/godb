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