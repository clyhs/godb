package godb

import (
	"testing"
	"encoding/json"
	"time"
	"errors"
	"fmt"
	"reflect"
	"database/sql/driver"
	"strconv"
)

type Student struct {
	Id       int64
	Created  int64
	Updated  int64
	Name     string
	ClassId  int64
	IsGood   bool
}

type StudentTag struct {
	Id       int64 `db:"s_id, primarykey, autoincrement"`
	Created  int64 `db:"createdTime"`
	Updated  int64 `db:"updated"`
	Name     string
	ClassId  int64 `db:"class_id"`
	IsGood   bool  `db:"is_good"`
}

type StudentTransientTag struct {
	Id       int64 `db:"s_id"`
	Created  int64 `db:"createdTime"`
	Updated  int64 `db:"-"`
	Name     string
	ClassId  int64 `db:"class_id"`
	IsGood   bool  `db:"is_good"`
}

type OverStudent struct {
	Student
	Id string
}

type IdCreated struct {
	Id      int64
	Created int64
}

type IdCreatedExternal struct {
	IdCreated
	External int64
}

type CustomStringType string
type CustomDate struct {
	time.Time
}


type TypeConversionExample struct {
	Id          int64
	StudentJSON Student
	Name        CustomStringType
}

type testTypeConverter struct{}

func (me testTypeConverter) ToDb(val interface{}) (interface{}, error) {

	switch t := val.(type) {
	case Student:
		b, err := json.Marshal(t)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case CustomStringType:
		return string(t), nil
	case CustomDate:
		return t.Time, nil
	}

	return val, nil
}

func (me testTypeConverter) FromDb(target interface{}) (CustomScanner, bool) {
	switch target.(type) {
	case *Student:
		binder := func(holder, target interface{}) error {
			s, ok := holder.(*string)
			if !ok {
				return errors.New("FromDb: Unable to convert Person to *string")
			}
			b := []byte(*s)
			return json.Unmarshal(b, target)
		}
		return CustomScanner{new(string), target, binder}, true
	case *CustomStringType:
		binder := func(holder, target interface{}) error {
			s, ok := holder.(*string)
			if !ok {
				return errors.New("FromDb: Unable to convert CustomStringType to *string")
			}
			st, ok := target.(*CustomStringType)
			if !ok {
				return errors.New(fmt.Sprint("FromDb: Unable to convert target to *CustomStringType: ", reflect.TypeOf(target)))
			}
			*st = CustomStringType(*s)
			return nil
		}
		return CustomScanner{new(string), target, binder}, true
	case *CustomDate:
		binder := func(holder, target interface{}) error {
			t, ok := holder.(*time.Time)
			if !ok {
				return errors.New("FromDb: Unable to convert CustomDate to *time.Time")
			}
			dateTarget, ok := target.(*CustomDate)
			if !ok {
				return errors.New(fmt.Sprint("FromDb: Unable to convert target to *CustomDate: ", reflect.TypeOf(target)))
			}
			dateTarget.Time = *t
			return nil
		}
		return CustomScanner{new(time.Time), target, binder}, true
	}

	return CustomScanner{}, false
}


type StudentValuerScanner struct {
	Student
}

// Value implements "database/sql/driver".Valuer.  It will be automatically
// run by the "database/sql" package when inserting/updating data.
func (s StudentValuerScanner) Value() (driver.Value, error) {
	return s.Id, nil
}

// Scan implements "database/sql".Scanner.  It will be automatically run
// by the "database/sql" package when reading column data into a field
// of type PersonValuerScanner.
func (s *StudentValuerScanner) Scan(value interface{}) (err error) {
	switch src := value.(type) {
	case []byte:
		// TODO: this case is here for mysql only.  For some reason,
		// one (both?) of the mysql libraries opt to pass us a []byte
		// instead of an int64 for the bigint column.  We should add
		// table tests around valuers/scanners and try to solve these
		// types of odd discrepencies to make it easier for users of
		// gorp to migrate to other database engines.
		s.Id, err = strconv.ParseInt(string(src), 10, 64)
	case int64:
		// Most libraries pass in the type we'd expect.
		s.Id = src
	default:
		typ := reflect.TypeOf(value)
		return fmt.Errorf("Expected person value to be convertible to int64, got %v (type %s)", value, typ)
	}
	return
}

type Person struct {
	Id       int64 `db:"id"`
	Name     string
	Address  string
}

var person = Person{}

type WithTime struct {
	Id   int64
	Time time.Time
}

type WithNullTime struct {
	Id   int64
	Time NullTime
}

func TestTableMap_Create(t *testing.T)  {

	dbUtils:=initDB()
	dbUtils.AddTableWithName(Student{},"t_student").SetKeys(true, "Id")
	dbUtils.AddTableWithName(StudentTag{},"t_student_tag")
	dbUtils.AddTableWithName(StudentTransientTag{},"t_student_ts_tag").SetKeys(true, "s_id")
	dbUtils.AddTableWithName(OverStudent{}, "t_student_over").SetKeys(false, "Id")
	dbUtils.AddTableWithName(IdCreated{}, "t_id_created").SetKeys(true, "Id")
	dbUtils.AddTableWithName(TypeConversionExample{}, "t_type_conv").SetKeys(true, "Id")

	dbUtils.AddTableWithName(Person{}, "t_person").SetKeys(true, "Id").AddIndex("PersonIndex", "Btree", []string{"Name"}).SetUnique(true)

	dbUtils.AddTableWithName(WithTime{}, "t_time_test").SetKeys(true, "Id")
	dbUtils.AddTableWithName(WithNullTime{}, "t_nulltime_test").SetKeys(false, "Id")

	dbUtils.TypeConverter = testTypeConverter{}
	err:=dbUtils.CreateTablesIfNotExists()

	if err!=nil{
		panic(err)
	}
}

func createTable() *DbUtils  {
	dbUtils:=initDB()
	dbUtils.AddTableWithName(Student{},"t_student").SetKeys(true, "Id")
	dbUtils.AddTableWithName(StudentTag{},"t_student_tag")
	dbUtils.AddTableWithName(StudentTransientTag{},"t_student_ts_tag").SetKeys(true, "s_id")
	dbUtils.AddTableWithName(OverStudent{}, "t_student_over").SetKeys(false, "Id")
	dbUtils.AddTableWithName(IdCreated{}, "t_id_created").SetKeys(true, "Id")
	dbUtils.AddTableWithName(TypeConversionExample{}, "t_type_conv").SetKeys(true, "Id")

	dbUtils.AddTableWithName(Person{}, "t_person").SetKeys(true, "Id").AddIndex("PersonIndex", "Btree", []string{"Name"}).SetUnique(true)

	dbUtils.AddTableWithName(WithTime{}, "t_time_test").SetKeys(true, "Id")
	dbUtils.AddTableWithName(WithNullTime{}, "t_nulltime_test").SetKeys(false, "Id")

	dbUtils.TypeConverter = testTypeConverter{}
	err:=dbUtils.CreateTablesIfNotExists()

	if err!=nil{
		panic(err)
	}
	return dbUtils
}

func TestDbUtils_Insert(t *testing.T) {
	dbUtils:=createTable()

	p := &Student{Name:"cly",IsGood:true}

	err:=_insert(dbUtils,p)
	if err!=nil {
		panic(err)
	}

}

func _insert(dbUtils *DbUtils, list ...interface{}) error {
	err:=dbUtils.Insert(list...)
	return err
}
