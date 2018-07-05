package godb

import (
	"reflect"
	"fmt"
	"bytes"
	"strings"
	"sync"
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


type CustomScanner struct {
	// After a row is scanned, Holder will contain the value from the database column.
	// Initialize the CustomScanner with the concrete Go type you wish the database
	// driver to scan the raw column into.
	Holder interface{}
	// Target typically holds a pointer to the target struct field to bind the Holder
	// value to.
	Target interface{}
	// Binder is a custom function that converts the holder value to the target type
	// and sets target accordingly.  This function should return error if a problem
	// occurs converting the holder to the target.
	Binder func(holder interface{}, target interface{}) error
}

func (me CustomScanner) Bind() error {
	return me.Binder(me.Holder, me.Target)
}

type bindPlan struct {
	query             string
	argFields         []string
	keyFields         []string
	versField         string
	autoIncrIdx       int
	autoIncrFieldName string
	once              sync.Once
}

func (plan *bindPlan) createBindInstance(elem reflect.Value, conv TypeConverter) (bindInstance, error) {
	bi := bindInstance{query: plan.query, autoIncrIdx: plan.autoIncrIdx, autoIncrFieldName: plan.autoIncrFieldName, versField: plan.versField}
	if plan.versField != "" {
		bi.existingVersion = elem.FieldByName(plan.versField).Int()
	}

	var err error

	for i := 0; i < len(plan.argFields); i++ {
		k := plan.argFields[i]
		if k == "" {

		} else {
			val := elem.FieldByName(k).Interface()
			if conv != nil {
				val, err = conv.ToDb(val)
				if err != nil {
					return bindInstance{}, err
				}
			}
			bi.args = append(bi.args, val)
		}
	}

	for i := 0; i < len(plan.keyFields); i++ {
		k := plan.keyFields[i]
		val := elem.FieldByName(k).Interface()
		if conv != nil {
			val, err = conv.ToDb(val)
			if err != nil {
				return bindInstance{}, err
			}
		}
		bi.keys = append(bi.keys, val)
	}

	return bi, nil
}

type bindInstance struct {
	query             string
	args              []interface{}
	keys              []interface{}
	existingVersion   int64
	versField         string
	autoIncrIdx       int
	autoIncrFieldName string
}

func (t *TableMap)insert(elem reflect.Value) (bindInstance, error)  {

	plan :=&bindPlan{}
	plan.once.Do(func() {
		plan.autoIncrIdx = -1

		s := bytes.Buffer{}
		s2 := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("insert into %s (", t.dbUtils.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

		x := 0
		first := true
		for y := range t.Columns {
			col := t.Columns[y]
			if !(col.isAutoIncr && t.dbUtils.Dialect.AutoIncrBindValue() == "") {
				if !col.Transient {
					if !first {
						s.WriteString(",")
						s2.WriteString(",")
					}
					s.WriteString(t.dbUtils.Dialect.QuoteField(col.ColumnName))

					if col.isAutoIncr {
						s2.WriteString(t.dbUtils.Dialect.AutoIncrBindValue())
						plan.autoIncrIdx = y
						plan.autoIncrFieldName = col.fieldName
					} else {
						if col.DefaultValue == "" {
							s2.WriteString(t.dbUtils.Dialect.BindVar(x))

							plan.argFields = append(plan.argFields, col.fieldName)

							x++
						} else {
							s2.WriteString(col.DefaultValue)
						}
					}
					first = false
				}
			} else {
				plan.autoIncrIdx = y
				plan.autoIncrFieldName = col.fieldName
			}
		}
		s.WriteString(") values (")
		s.WriteString(s2.String())
		s.WriteString(")")
		if plan.autoIncrIdx > -1 {
			s.WriteString(t.dbUtils.Dialect.AutoIncrInsertSuffix(t.Columns[plan.autoIncrIdx]))
		}
		s.WriteString(t.dbUtils.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	fmt.Println(plan)

	return plan.createBindInstance(elem, t.dbUtils.TypeConverter)
}


func (t *TableMap) bindGet() *bindPlan {
	plan := &bindPlan{}
	plan.once.Do(func() {
		s := bytes.Buffer{}
		s.WriteString("select ")

		x := 0
		for _, col := range t.Columns {
			if !col.Transient {
				if x > 0 {
					s.WriteString(",")
				}
				s.WriteString(t.dbUtils.Dialect.QuoteField(col.ColumnName))
				plan.argFields = append(plan.argFields, col.fieldName)
				x++
			}
		}
		s.WriteString(" from ")
		s.WriteString(t.dbUtils.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName))
		s.WriteString(" where ")
		for x := range t.keys {
			col := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbUtils.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbUtils.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, col.fieldName)
		}
		s.WriteString(t.dbUtils.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	fmt.Println(plan.query)

	return plan
}

func (t *TableMap) bindUpdate(elem reflect.Value) (bindInstance, error) {


	plan := &bindPlan{}
	plan.once.Do(func() {
		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("update %s set ", t.dbUtils.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))
		x := 0

		for y := range t.Columns {
			col := t.Columns[y]
			if !col.isAutoIncr && !col.Transient{
				if x > 0 {
					s.WriteString(", ")
				}
				s.WriteString(t.dbUtils.Dialect.QuoteField(col.ColumnName))
				s.WriteString("=")
				s.WriteString(t.dbUtils.Dialect.BindVar(x))


				plan.argFields = append(plan.argFields, col.fieldName)

				x++
			}
		}

		s.WriteString(" where ")
		for y := range t.keys {
			col := t.keys[y]
			if y > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbUtils.Dialect.QuoteField(col.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbUtils.Dialect.BindVar(x))

			plan.argFields = append(plan.argFields, col.fieldName)
			plan.keyFields = append(plan.keyFields, col.fieldName)
			x++
		}

		s.WriteString(t.dbUtils.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	return plan.createBindInstance(elem, t.dbUtils.TypeConverter)
}


func (t *TableMap) bindDelete(elem reflect.Value) (bindInstance, error) {
	plan := &bindPlan{}
	plan.once.Do(func() {
		s := bytes.Buffer{}
		s.WriteString(fmt.Sprintf("delete from %s", t.dbUtils.Dialect.QuotedTableForQuery(t.SchemaName, t.TableName)))

		for y := range t.Columns {
			col := t.Columns[y]
			if !col.Transient {

			}
		}

		s.WriteString(" where ")
		for x := range t.keys {
			k := t.keys[x]
			if x > 0 {
				s.WriteString(" and ")
			}
			s.WriteString(t.dbUtils.Dialect.QuoteField(k.ColumnName))
			s.WriteString("=")
			s.WriteString(t.dbUtils.Dialect.BindVar(x))

			plan.keyFields = append(plan.keyFields, k.fieldName)
			plan.argFields = append(plan.argFields, k.fieldName)
		}

		s.WriteString(t.dbUtils.Dialect.QuerySuffix())

		plan.query = s.String()
	})

	return plan.createBindInstance(elem, t.dbUtils.TypeConverter)
}