package godb

import (
	"reflect"
	"fmt"
	"bytes"
	"strings"
)

type TableMap struct {
	TableName      string
	SchemaName     string
	gotype         reflect.Type
	Columns        []*ColumnMap
	keys           []*ColumnMap
	indexes        []*IndexMap
	uniqueTogether [][]string
	dbUtils        *DbUtils
}


func (t *TableMap) SetKeys(isAutoIncr bool, fieldNames ...string) *TableMap {

	if isAutoIncr && len(fieldNames) != 1 {
		panic(fmt.Sprintf(
			"godb: SetKeys: fieldNames length must be 1 if key is auto-increment. (Saw %v fieldNames)",
			len(fieldNames)))
	}
	t.keys = make([]*ColumnMap, 0)

	for _, name := range fieldNames {
		colmap := t.ColMap(name)
		colmap.isPK = true
		colmap.isAutoIncr = isAutoIncr
		t.keys = append(t.keys, colmap)
	}

	return t
}

func (t *TableMap) ColMap(field string) *ColumnMap {
	col := colMapOrNil(t, field)
	if col == nil {
		e := fmt.Sprintf("No ColumnMap in table %s type %s with field %s",
			t.TableName, t.gotype.Name(), field)

		panic(e)
	}
	return col
}

func colMapOrNil(t *TableMap, field string) *ColumnMap {
	for _, col := range t.Columns {
		if col.fieldName == field || col.ColumnName == field {
			return col
		}
	}
	return nil
}

func (t *TableMap) SetUniqueTogether(fieldNames ...string) *TableMap {
	if len(fieldNames) < 2 {
		panic(fmt.Sprintf(
			"godb: SetUniqueTogether: must provide at least two fieldNames to set uniqueness constraint."))
	}

	columns := make([]string, 0)
	for _, name := range fieldNames {
		columns = append(columns, name)
	}
	t.uniqueTogether = append(t.uniqueTogether, columns)

	return t
}

func (t *TableMap) IdxMap(field string) *IndexMap {
	for _, idx := range t.indexes {
		if idx.IndexName == field {
			return idx
		}
	}
	return nil
}

func (t *TableMap) AddIndex(name string, idxtype string, columns []string) *IndexMap {
	// check if we have a index with this name already
	for _, idx := range t.indexes {
		if idx.IndexName == name {
			return idx
		}
	}
	for _, icol := range columns {
		if res := t.ColMap(icol); res == nil {
			e := fmt.Sprintf("No ColumnName in table %s to create index on", t.TableName)
			panic(e)
		}
	}

	idx := &IndexMap{IndexName: name, Unique: false, IndexType: idxtype, columns: columns}
	t.indexes = append(t.indexes, idx)
	return idx
}

func (t *TableMap) CreateTableSql(ifNotExists bool) string {

	s := bytes.Buffer{}

	dialect := t.dbUtils.Dialect

	if strings.TrimSpace(t.SchemaName) != "" {
		schemaCreate := "create schema"
		if ifNotExists {
			s.WriteString(dialect.IfSchemaNotExists(schemaCreate, t.SchemaName))
		} else {
			s.WriteString(schemaCreate)
		}
		s.WriteString(fmt.Sprintf(" %s;", t.SchemaName))
	}

	tableCreate := "create table"
	if ifNotExists {
		s.WriteString(dialect.IfTableNotExists(tableCreate, t.SchemaName, t.TableName))
	} else {
		s.WriteString(tableCreate)
	}
	s.WriteString(fmt.Sprintf(" %s (", dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

	x := 0
	for _, col := range t.Columns {
		if !col.Transient {
			if x > 0 {
				s.WriteString(", ")
			}
			stype := dialect.ToSqlType(col.gotype, col.MaxSize, col.isAutoIncr)
			s.WriteString(fmt.Sprintf("%s %s", dialect.QuoteField(col.ColumnName), stype))

			if col.isPK || col.isNotNull {
				s.WriteString(" not null")
			}
			if col.isPK && len(t.keys) == 1 {
				s.WriteString(" primary key")
			}
			if col.Unique {
				s.WriteString(" unique")
			}
			if col.isAutoIncr {
				s.WriteString(fmt.Sprintf(" %s", dialect.AutoIncrStr()))
			}

			x++
		}
	}

	if len(t.keys) > 1 {
		s.WriteString(", primary key (")
		for x := range t.keys {
			if x > 0 {
				s.WriteString(", ")
			}
			s.WriteString(dialect.QuoteField(t.keys[x].ColumnName))
		}
		s.WriteString(")")
	}
	if len(t.uniqueTogether) > 0 {
		for _, columns := range t.uniqueTogether {
			s.WriteString(", unique (")
			for i, column := range columns {
				if i > 0 {
					s.WriteString(", ")
				}
				s.WriteString(dialect.QuoteField(column))
			}
			s.WriteString(")")
		}
	}
	s.WriteString(") ")
	s.WriteString(dialect.CreateTableSuffix())
	s.WriteString(dialect.QuerySuffix())
	return s.String()
}