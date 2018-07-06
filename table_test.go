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
	_ "github.com/go-sql-driver/mysql"
	"strings"
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


type WithCustomDate struct {
	Id    int64
	Added CustomDate
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
			fmt.Println("time")
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
	dbUtils.AddTableWithName(WithCustomDate{}, "t_customdate").SetKeys(true, "Id")

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

func TestCustomDate_insert(t *testing.T)  {
	test1 := &WithCustomDate{Added: CustomDate{Time: time.Now().Truncate(time.Second)}}
	dbUtils:=createTable()

	err:=dbUtils.Insert(test1)

	if err != nil {
		t.Errorf("Could not insert struct with custom date field: %s", err)
		t.FailNow()
	}
	result, err :=dbUtils.Get(new(WithCustomDate),test1.Id)

	t.Skip("TestCustomDateType can't run Get() with the mysql driver; skipping the rest of this test...")

	if err != nil {
		t.Errorf("Could not get struct with custom date field: %s", err)
		t.FailNow()
	}
	fmt.Println(result)
	test2 := result.(*WithCustomDate)
	if test2.Added.UTC() != test1.Added.UTC() {
		t.Errorf("Custom dates do not match: %v != %v", test2.Added.UTC(), test1.Added.UTC())
	}else{
		fmt.Println("good")
	}
}

func TestDbUtils_withtime(t *testing.T)  {
	test1:=&WithTime{Time:time.Now().Truncate(time.Second)}
	dbUtils:=createTable()
	err:=dbUtils.Insert(test1)
	if err!=nil{
		panic(err)
	}
	result ,err:=dbUtils.Get(new(WithTime),test1.Id)
	if err!=nil{
		panic(err)
	}
	fmt.Println(result)
}

type PersonUInt32 struct {
	Id   uint32
	Name string
}

type PersonUInt64 struct {
	Id   uint64
	Name string
}

type PersonUInt16 struct {
	Id   uint16
	Name string
}

func TestDbUtils_UIntPrimaryKey(t *testing.T)  {

	dbUtils:=initDB()
	dbUtils.AddTable(PersonUInt64{}).SetKeys(true, "Id")
	dbUtils.AddTable(PersonUInt32{}).SetKeys(true, "Id")
	dbUtils.AddTable(PersonUInt16{}).SetKeys(true, "Id")
	err := dbUtils.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbUtils)

	p1 := &PersonUInt64{0, "name1"}
	p2 := &PersonUInt32{0, "name2"}
	p3 := &PersonUInt16{0, "name3"}
	err = dbUtils.Insert(p1, p2, p3)
	if err != nil {
		t.Error(err)
	}
	if p1.Id != 1 {
		t.Errorf("%d != 1", p1.Id)
	}
	if p2.Id != 1 {
		t.Errorf("%d != 1", p2.Id)
	}
	if p3.Id != 1 {
		t.Errorf("%d != 1", p3.Id)
	}
}

type UniqueColumns struct {
	FirstName string
	LastName  string
	City      string
	ZipCode   int64
}

func Test_SetUniqueTogether(t *testing.T) {
	dbUtils := initDB()
	dbUtils.AddTable(UniqueColumns{}).SetUniqueTogether("FirstName", "LastName").SetUniqueTogether("City", "ZipCode")
	err := dbUtils.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbUtils)

	n1 := &UniqueColumns{"Steve", "Jobs", "Cupertino", 95014}
	err = dbUtils.Insert(n1)
	if err != nil {
		t.Error(err)
	}

	// Should fail because of the first constraint
	n2 := &UniqueColumns{"Steve", "Jobs", "Sunnyvale", 94085}
	err = dbUtils.Insert(n2)
	if err == nil {
		t.Error(err)
	}
	// "unique" for Postgres/SQLite, "Duplicate entry" for MySQL
	errLower := strings.ToLower(err.Error())
	if !strings.Contains(errLower, "unique") && !strings.Contains(errLower, "duplicate entry") {
		t.Error(err)
	}

	// Should also fail because of the second unique-together
	n3 := &UniqueColumns{"Steve", "Wozniak", "Cupertino", 95014}
	err = dbUtils.Insert(n3)
	if err == nil {
		t.Error(err)
	}
	// "unique" for Postgres/SQLite, "Duplicate entry" for MySQL
	errLower = strings.ToLower(err.Error())
	if !strings.Contains(errLower, "unique") && !strings.Contains(errLower, "duplicate entry") {
		t.Error(err)
	}

	// This one should finally succeed
	n4 := &UniqueColumns{"Steve", "Wozniak", "Sunnyvale", 94085}
	err = dbUtils.Insert(n4)
	if err != nil {
		t.Error(err)
	}
}

type PersistentUser struct {
	Key            int32
	Id             string
	PassedTraining bool
}

func Test_PersistentUser(t *testing.T) {
	dbUtils := initDB()
	dbUtils.Exec("drop table if exists PersistentUser")
	table := dbUtils.AddTable(PersistentUser{}).SetKeys(false, "Key")
	table.ColMap("Key").Rename("mykey")
	err := dbUtils.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbUtils)
	fmt.Println("create table...")

	pu := &PersistentUser{43, "33r", false}
	err = dbUtils.Insert(pu)
	if err != nil {
		panic(err)
	}

	fmt.Println("insert...")
	// prove we can pass a pointer into Get
	pu2, err := dbUtils.Get(pu, pu.Key)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(pu, pu2) {
		t.Errorf("%v!=%v", pu, pu2)
	}

	fmt.Println("select...")
	arr, err := dbUtils.Select(pu, "select * from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(pu, arr[0]) {
		t.Errorf("%v!=%v", pu, arr[0])
	}

	// prove we can get the results back in a slice
	fmt.Println("insert2...")
	var puArr []*PersistentUser
	_, err = dbUtils.Select(&puArr, "select * from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu, puArr[0]) {
		t.Errorf("%v!=%v", pu, puArr[0])
	}

	// prove we can get the results back in a non-pointer slice
	fmt.Println("select...")
	var puValues []PersistentUser
	_, err = dbUtils.Select(&puValues, "select * from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if len(puValues) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(*pu, puValues[0]) {
		t.Errorf("%v!=%v", *pu, puValues[0])
	}

	fmt.Println("select[]...")
	// prove we can get the results back in a string slice
	var idArr []*string
	_, err = dbUtils.Select(&idArr, "select "+columnName(dbUtils, PersistentUser{}, "Id")+" from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if len(idArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu.Id, *idArr[0]) {
		t.Errorf("%v!=%v", pu.Id, *idArr[0])
	}

	// prove we can get the results back in an int slice
	fmt.Println("select[] int32...")
	var keyArr []*int32
	_, err = dbUtils.Select(&keyArr, "select mykey from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if len(keyArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu.Key, *keyArr[0]) {
		t.Errorf("%v!=%v", pu.Key, *keyArr[0])
	}

	// prove we can get the results back in a bool slice
	var passedArr []*bool
	_, err = dbUtils.Select(&passedArr, "select "+columnName(dbUtils, PersistentUser{}, "PassedTraining")+" from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if len(passedArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu.PassedTraining, *passedArr[0]) {
		t.Errorf("%v!=%v", pu.PassedTraining, *passedArr[0])
	}

	// prove we can get the results back in a non-pointer slice
	var stringArr []string
	_, err = dbUtils.Select(&stringArr, "select "+columnName(dbUtils, PersistentUser{}, "Id")+" from "+tableName(dbUtils, PersistentUser{}))
	if err != nil {
		panic(err)
	}
	if len(stringArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu.Id, stringArr[0]) {
		t.Errorf("%v!=%v", pu.Id, stringArr[0])
	}
}



func _insert(dbUtils *DbUtils, list ...interface{}) error {
	err:=dbUtils.Insert(list...)
	return err
}

func close(dbUtils *DbUtils)  {
	dbUtils.Db.Close()
}

func tableName(dbUtils *DbUtils, i interface{}) string {
	t := reflect.TypeOf(i)
	if table, err := dbUtils.TableFor(t, false); table != nil && err == nil {
		return dbUtils.Dialect.QuoteField(table.TableName)
	}
	return t.Name()
}

func columnName(dbUtils *DbUtils, i interface{}, fieldName string) string {
	t := reflect.TypeOf(i)
	if table, err := dbUtils.TableFor(t, false); table != nil && err == nil {
		return dbUtils.Dialect.QuoteField(table.ColMap(fieldName).ColumnName)
	}
	return fieldName
}