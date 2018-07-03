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

func (idx *IndexMap) Rename(indname string) *IndexMap {
	idx.IndexName = indname
	return idx
}

// SetUnique adds "unique" to the create index statements for this
// index, if b is true.
func (idx *IndexMap) SetUnique(b bool) *IndexMap {
	idx.Unique = b
	return idx
}

// SetIndexType specifies the index type supported by chousen SQL Dialect
func (idx *IndexMap) SetIndexType(indtype string) *IndexMap {
	idx.IndexType = indtype
	return idx
}