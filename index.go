package godb

type IndexMap struct {
	// Index name in db table
	IndexName string

	// If true, " unique" is added to create index statements.
	// Not used elsewhere
	Unique bool

	// Index type supported by Dialect
	// Postgres:  B-tree, Hash, GiST and GIN.
	// Mysql: Btree, Hash.
	// Sqlite: nil.
	IndexType string

	// Columns name for single and multiple indexes
	columns []string
} 
