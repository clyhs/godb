package godb

import "database/sql"

type Rows struct {
	*sql.Rows
	db *DB
}
