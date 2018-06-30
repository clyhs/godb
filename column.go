package godb

import "reflect"

type ColumnMap struct {
	// Column name in db table
	ColumnName string

	// If true, this column is skipped in generated SQL statements
	Transient bool

	// If true, " unique" is added to create table statements.
	// Not used elsewhere
	Unique bool

	// Query used for getting generated id after insert
	GeneratedIdQuery string

	// Passed to Dialect.ToSqlType() to assist in informing the
	// correct column type to map to in CreateTables()
	MaxSize int

	DefaultValue string

	fieldName  string
	gotype     reflect.Type
	isPK       bool
	isAutoIncr bool
	isNotNull  bool

}

