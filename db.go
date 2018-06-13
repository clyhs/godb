package godb

import "database/sql"

type DB struct {
	*sql.DB
}
