package godb

import "reflect"

type Dialect interface {
	// adds a suffix to any query, usually ";"
	QuerySuffix() string

	// ToSqlType returns the SQL column type to use when creating a
	// table of the given Go Type.  maxsize can be used to switch based on
	// size.  For example, in MySQL []byte could map to BLOB, MEDIUMBLOB,
	// or LONGBLOB depending on the maxsize
	ToSqlType(val reflect.Type, maxsize int, isAutoIncr bool) string

	// string to append to primary key column definitions
	AutoIncrStr() string

	// string to bind autoincrement columns to. Empty string will
	// remove reference to those columns in the INSERT statement.
	AutoIncrBindValue() string

	AutoIncrInsertSuffix(col *ColumnMap) string

	// string to append to "create table" statement for vendor specific
	// table attributes
	CreateTableSuffix() string

	// string to append to "create index" statement
	CreateIndexSuffix() string

	// string to append to "drop index" statement
	DropIndexSuffix() string

	// string to truncate tables
	TruncateClause() string

	// bind variable string to use when forming SQL statements
	// in many dbs it is "?", but Postgres appears to use $1
	//
	// i is a zero based index of the bind variable in this statement
	//
	BindVar(i int) string

	// Handles quoting of a field name to ensure that it doesn't raise any
	// SQL parsing exceptions by using a reserved word as a field name.
	QuoteField(field string) string

	// Handles building up of a schema.database string that is compatible with
	// the given dialect
	//
	// schema - The schema that <table> lives in
	// table - The table name
	QuotedTableForQuery(schema string, table string) string

	// Existence clause for table creation / deletion
	IfSchemaNotExists(command, schema string) string
	IfTableExists(command, schema, table string) string
	IfTableNotExists(command, schema, table string) string
} 