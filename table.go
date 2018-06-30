package godb

import "reflect"

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
