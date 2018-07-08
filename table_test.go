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
	"database/sql"
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
	PersonJSON  Person2
	Name        CustomStringType
}

type testTypeConverter struct{}

func (me testTypeConverter) ToDb(val interface{}) (interface{}, error) {

	switch t := val.(type) {
	case Person2:
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
	case *Person2:
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
	/*
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
		*/
	// prove we can get the results back in a slice
	fmt.Println("select2...")
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

func TestDbutils_NamedQueryMap(t *testing.T)  {
	dbUtils := initDB()
	dbUtils.Exec("drop table if exists PersistentUser")
	table := dbUtils.AddTable(PersistentUser{}).SetKeys(false, "Key")
	table.ColMap("Key").Rename("mykey")
	err := dbUtils.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbUtils)
	pu := &PersistentUser{43, "33r", false}
	pu2 := &PersistentUser{500, "abc", false}
	err = dbUtils.Insert(pu, pu2)
	if err != nil {
		panic(err)
	}

	// Test simple case
	var puArr []*PersistentUser
	_, err = dbUtils.Select(&puArr, "select * from "+tableName(dbUtils, PersistentUser{})+" where mykey = :Key", map[string]interface{}{
		"Key": 43,
	})
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu, puArr[0]) {
		t.Errorf("%v!=%v", pu, puArr[0])
	}

	puArr = nil
	_, err = dbUtils.Select(&puArr, "select * from "+tableName(dbUtils, PersistentUser{})+" where mykey = :Key", map[string]int{
		"Key": 43,
	})
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}

	puArr = nil
	_, err = dbUtils.Select(&puArr, `
select * from `+tableName(dbUtils, PersistentUser{})+`
 where mykey = :Key
   and `+columnName(dbUtils, PersistentUser{}, "PassedTraining")+` = :PassedTraining
   and `+columnName(dbUtils, PersistentUser{}, "Id")+` = :Id`, map[string]interface{}{
		"Key":            43,
		"PassedTraining": false,
		"Id":             "33r",
	})
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}

	puArr = nil
	_, err = dbUtils.Select(&puArr, `
select * from `+tableName(dbUtils, PersistentUser{})+`
 where mykey = :Key
   and `+columnName(dbUtils, PersistentUser{}, "Id")+` != 'abc:def'`, map[string]interface{}{
		"Key":            43,
		"PassedTraining": false,
	})
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}

	// Test to delete with Exec and named params.
	result, err := dbUtils.Exec("delete from "+tableName(dbUtils, PersistentUser{})+" where mykey = :Key", map[string]interface{}{
		"Key": 43,
	})
	count, err := result.RowsAffected()
	if err != nil {
		t.Errorf("Failed to exec: %s", err)
		t.FailNow()
	}
	if count != 1 {
		t.Errorf("Expected 1 persistentuser to be deleted, but %d deleted", count)
	}


}

func TestDbUtils_NameQueryStruct(t *testing.T) {

	dbmap := initDB()
	dbmap.Exec("drop table if exists PersistentUser")
	table := dbmap.AddTable(PersistentUser{}).SetKeys(false, "Key")
	table.ColMap("Key").Rename("mykey")
	err := dbmap.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbmap)
	pu := &PersistentUser{43, "33r", false}
	pu2 := &PersistentUser{500, "abc", false}
	err = dbmap.Insert(pu, pu2)
	if err != nil {
		panic(err)
	}

	// Test select self
	var puArr []*PersistentUser
	_, err = dbmap.Select(&puArr, `
select * from `+tableName(dbmap, PersistentUser{})+`
 where mykey = :Key
   and `+columnName(dbmap, PersistentUser{}, "PassedTraining")+` = :PassedTraining
   and `+columnName(dbmap, PersistentUser{}, "Id")+` = :Id`, pu)
	if err != nil {
		t.Errorf("Failed to select: %s", err)
		t.FailNow()
	}
	if len(puArr) != 1 {
		t.Errorf("Expected one persistentuser, found none")
	}
	if !reflect.DeepEqual(pu, puArr[0]) {
		t.Errorf("%v!=%v", pu, puArr[0])
	}

	// Test delete self.
	result, err := dbmap.Exec(`delete from `+tableName(dbmap, PersistentUser{})+`where mykey = :Key
                   and `+columnName(dbmap, PersistentUser{}, "PassedTraining")+` = :PassedTraining
                   and `+columnName(dbmap, PersistentUser{}, "Id")+` = :Id`, pu)
	count, err := result.RowsAffected()
	if err != nil {
		t.Errorf("Failed to exec: %s", err)
		t.FailNow()
	}
	if count != 1 {
		t.Errorf("Expected 1 persistentuser to be deleted, but %d deleted", count)
	}
}

type Invoice struct {
	Id       int64
	Created  int64
	Updated  int64
	Memo     string
	PersonId int64
	IsPaid   bool
}

func Test_ReturnsNonNilSlice(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)
	noResultsSQL := "select * from invoice_test where " + columnName(dbmap, Invoice{}, "Id") + "=99999"
	var r1 []*Invoice
	rawSelect(dbmap, &r1, noResultsSQL)
	if r1 == nil {
		t.Errorf("r1==nil")
	}

	r2 := rawSelect(dbmap, Invoice{}, noResultsSQL)
	if r2 == nil {
		t.Errorf("r2==nil")
	}
}

type Person2 struct {
	Id      int64
	Created int64
	Updated int64
	FName   string
	LName   string
	Version int64
}


type TableWithNull struct {
	Id      int64
	Str     sql.NullString
	Int64   sql.NullInt64
	Float64 sql.NullFloat64
	Bool    sql.NullBool
	Bytes   []byte
}

func Test_DoubleAddTable(t *testing.T) {
	dbmap := initDB()
	t1 := dbmap.AddTable(TableWithNull{}).SetKeys(false, "Id")
	t2 := dbmap.AddTable(TableWithNull{})
	dbmap.CreateTablesIfNotExists()
	if t1 != t2 {
		t.Errorf("%v != %v", t1, t2)
	}
}

// what happens if a legacy table has a null value?
func Test_NullValues(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTable(TableWithNull{}).SetKeys(false, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	// insert a row directly
	rawExec(dbmap, "insert into "+tableName(dbmap, TableWithNull{})+" values (10, null, "+
		"null, null, null, null)")

	// try to load it
	expected := &TableWithNull{Id: 10}
	obj := _get(dbmap, TableWithNull{}, 10)
	t1 := obj.(*TableWithNull)
	if !reflect.DeepEqual(expected, t1) {
		t.Errorf("%v != %v", expected, t1)
	}

	// update it
	t1.Str = sql.NullString{"hi", true}
	expected.Str = t1.Str
	t1.Int64 = sql.NullInt64{999, true}
	expected.Int64 = t1.Int64
	t1.Float64 = sql.NullFloat64{53.33, true}
	expected.Float64 = t1.Float64
	t1.Bool = sql.NullBool{true, true}
	expected.Bool = t1.Bool
	t1.Bytes = []byte{1, 30, 31, 33}
	expected.Bytes = t1.Bytes
	_update(dbmap, t1)

	obj = _get(dbmap, TableWithNull{}, 10)
	t1 = obj.(*TableWithNull)
	if t1.Str.String != "hi" {
		t.Errorf("%s != hi", t1.Str.String)
	}
	if !reflect.DeepEqual(expected, t1) {
		t.Errorf("%v != %v", expected, t1)
	}
}

type PersonValuerScanner struct {
	Person2
}

// Value implements "database/sql/driver".Valuer.  It will be automatically
// run by the "database/sql" package when inserting/updating data.
func (p PersonValuerScanner) Value() (driver.Value, error) {
	return p.Id, nil
}

// Scan implements "database/sql".Scanner.  It will be automatically run
// by the "database/sql" package when reading column data into a field
// of type PersonValuerScanner.
func (p *PersonValuerScanner) Scan(value interface{}) (err error) {
	switch src := value.(type) {
	case []byte:
		// TODO: this case is here for mysql only.  For some reason,
		// one (both?) of the mysql libraries opt to pass us a []byte
		// instead of an int64 for the bigint column.  We should add
		// table tests around valuers/scanners and try to solve these
		// types of odd discrepencies to make it easier for users of
		// gorp to migrate to other database engines.
		p.Id, err = strconv.ParseInt(string(src), 10, 64)
	case int64:
		// Most libraries pass in the type we'd expect.
		p.Id = src
	default:
		typ := reflect.TypeOf(value)
		return fmt.Errorf("Expected person value to be convertible to int64, got %v (type %s)", value, typ)
	}
	return
}

type InvoiceWithValuer struct {
	Id      int64
	Created int64
	Updated int64
	Memo    string
	Person  PersonValuerScanner `db:"personid"`
	IsPaid  bool
}

func Test_ScannerValuer(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(PersonValuerScanner{}, "person_test").SetKeys(true, "Id")
	dbmap.AddTableWithName(InvoiceWithValuer{}, "invoice_test").SetKeys(true, "Id")
	err := dbmap.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbmap)

	pv := PersonValuerScanner{}
	pv.FName = "foo"
	pv.LName = "bar"
	err = dbmap.Insert(&pv)
	if err != nil {
		t.Errorf("Could not insert PersonValuerScanner using Person table: %v", err)
		t.FailNow()
	}

	inv := InvoiceWithValuer{}
	inv.Memo = "foo"
	inv.Person = pv
	err = dbmap.Insert(&inv)
	if err != nil {
		t.Errorf("Could not insert InvoiceWithValuer using Invoice table: %v", err)
		t.FailNow()
	}

	res, err := dbmap.Get(InvoiceWithValuer{}, inv.Id)
	if err != nil {
		t.Errorf("Could not get InvoiceWithValuer: %v", err)
		t.FailNow()
	}
	dbInv := res.(*InvoiceWithValuer)

	if dbInv.Person.Id != pv.Id {
		t.Errorf("InvoiceWithValuer got wrong person ID: %d (expected) != %d (actual)", pv.Id, dbInv.Person.Id)
	}
}

func TestColumnProps(t *testing.T) {
	dbmap := initDB()
	t1 := dbmap.AddTable(Invoice{}).SetKeys(true, "Id")
	t1.ColMap("Created").Rename("date_created")
	t1.ColMap("Updated").SetTransient(true)
	t1.ColMap("Memo").SetMaxSize(10)
	t1.ColMap("PersonId").SetUnique(true)

	err := dbmap.CreateTablesIfNotExists()
	if err != nil {
		panic(err)
	}
	defer close(dbmap)

	// test transient
	inv := &Invoice{0, 0, 1, "my invoice", 0, true}
	_insert(dbmap, inv)
	obj := _get(dbmap, Invoice{}, inv.Id)
	inv = obj.(*Invoice)
	if inv.Updated != 0 {
		t.Errorf("Saved transient column 'Updated'")
	}

	// test max size
	inv.Memo = "this memo is too long"
	err = dbmap.Insert(inv)
	if err == nil {
		t.Errorf("max size exceeded, but Insert did not fail.")
	}

	// test unique - same person id
	inv = &Invoice{0, 0, 1, "my invoice2", 0, false}
	err = dbmap.Insert(inv)
	if err == nil {
		t.Errorf("same PersonId inserted, but Insert did not fail.")
	}
}

func Test_Transaction(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	inv1 := &Invoice{0, 100, 200, "t1", 0, true}
	inv2 := &Invoice{0, 100, 200, "t2", 0, false}

	trans, err := dbmap.Begin()
	if err != nil {
		panic(err)
	}
	trans.Insert(inv1, inv2)
	err = trans.Commit()
	if err != nil {
		panic(err)
	}

	obj, err := dbmap.Get(Invoice{}, inv1.Id)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(inv1, obj) {
		t.Errorf("%v != %v", inv1, obj)
	}
	obj, err = dbmap.Get(Invoice{}, inv2.Id)
	if err != nil {
		panic(err)
	}
	if !reflect.DeepEqual(inv2, obj) {
		t.Errorf("%v != %v", inv2, obj)
	}
}

func Test_Savepoint(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	inv1 := &Invoice{0, 100, 200, "unpaid", 0, false}

	trans, err := dbmap.Begin()
	if err != nil {
		panic(err)
	}
	trans.Insert(inv1)

	var checkMemo = func(want string) {
		memo, err := trans.SelectStr("select " + columnName(dbmap, Invoice{}, "Memo") + " from invoice_test")
		if err != nil {
			panic(err)
		}
		if memo != want {
			t.Errorf("%q != %q", want, memo)
		}
	}
	checkMemo("unpaid")

	err = trans.Savepoint("foo")
	if err != nil {
		panic(err)
	}
	checkMemo("unpaid")

	inv1.Memo = "paid"
	_, err = trans.Update(inv1)
	if err != nil {
		panic(err)
	}
	checkMemo("paid")

	err = trans.RollbackToSavepoint("foo")
	if err != nil {
		panic(err)
	}
	checkMemo("unpaid")

	err = trans.Rollback()
	if err != nil {
		panic(err)
	}
}

func Test_Multiple(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	inv1 := &Invoice{0, 100, 200, "a", 0, false}
	inv2 := &Invoice{0, 100, 200, "b", 0, true}
	_insert(dbmap, inv1, inv2)

	inv1.Memo = "c"
	inv2.Memo = "d"
	_update(dbmap, inv1, inv2)

	count := _del(dbmap, inv1, inv2)
	if count != 2 {
		t.Errorf("%d != 2", count)
	}
}

type WithIgnoredColumn struct {
	internal int64 `db:"-"`
	Id       int64
	Created  int64
}

func Test_WithIgnoredColumn(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(WithIgnoredColumn{}, "ignored_column_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	ic := &WithIgnoredColumn{-1, 0, 1}
	_insert(dbmap, ic)
	expected := &WithIgnoredColumn{0, 1, 1}
	ic2 := _get(dbmap, WithIgnoredColumn{}, ic.Id).(*WithIgnoredColumn)

	if !reflect.DeepEqual(expected, ic2) {
		t.Errorf("%v != %v", expected, ic2)
	}
	if _del(dbmap, ic) != 1 {
		t.Errorf("Did not delete row with Id: %d", ic.Id)
		return
	}
	if _get(dbmap, WithIgnoredColumn{}, ic.Id) != nil {
		t.Errorf("Found id: %d after Delete()", ic.Id)
	}
}

func Test_TypeConversionExample(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Person2{}, "person_test").SetKeys(true, "Id")
	dbmap.AddTableWithName(TypeConversionExample{}, "type_conv_test").SetKeys(true, "Id")
	dbmap.TypeConverter = testTypeConverter{}
	dbmap.CreateTablesIfNotExists()

	defer close(dbmap)

	p := Person2{FName: "Bob", LName: "Smith"}
	tc := &TypeConversionExample{-1, p, CustomStringType("hi")}
	_insert(dbmap, tc)

	expected := &TypeConversionExample{1, p, CustomStringType("hi")}
	tc2 := _get(dbmap, TypeConversionExample{}, tc.Id).(*TypeConversionExample)
	if !reflect.DeepEqual(expected, tc2) {
		t.Errorf("tc2 %v != %v", expected, tc2)
	}

	tc2.Name = CustomStringType("hi2")
	tc2.PersonJSON = Person2{FName: "Jane", LName: "Doe"}
	_update(dbmap, tc2)

	expected = &TypeConversionExample{1, tc2.PersonJSON, CustomStringType("hi2")}
	tc3 := _get(dbmap, TypeConversionExample{}, tc.Id).(*TypeConversionExample)
	if !reflect.DeepEqual(expected, tc3) {
		t.Errorf("tc3 %v != %v", expected, tc3)
	}

	if _del(dbmap, tc) != 1 {
		t.Errorf("Did not delete row with Id: %d", tc.Id)
	}

}

type WithEmbeddedStruct struct {
	Id int64
	Names
}

type Names struct {
	FirstName string
	LastName  string
}




func Test_WithEmbeddedStruct(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(WithEmbeddedStruct{}, "embedded_struct_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	es := &WithEmbeddedStruct{-1, Names{FirstName: "Alice", LastName: "Smith"}}
	_insert(dbmap, es)
	expected := &WithEmbeddedStruct{1, Names{FirstName: "Alice", LastName: "Smith"}}
	es2 := _get(dbmap, WithEmbeddedStruct{}, es.Id).(*WithEmbeddedStruct)
	if !reflect.DeepEqual(expected, es2) {
		t.Errorf("%v != %v", expected, es2)
	}

	es2.FirstName = "Bob"
	expected.FirstName = "Bob"
	_update(dbmap, es2)
	es2 = _get(dbmap, WithEmbeddedStruct{}, es.Id).(*WithEmbeddedStruct)
	if !reflect.DeepEqual(expected, es2) {
		t.Errorf("%v != %v", expected, es2)
	}

	ess := rawSelect(dbmap, WithEmbeddedStruct{}, "select * from embedded_struct_test")
	if !reflect.DeepEqual(es2, ess[0]) {
		t.Errorf("%v != %v", es2, ess[0])
	}
}


type WithStringPk struct {
	Id   string
	Name string
}

func Test_WithStringPk(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(WithStringPk{}, "string_pk_test").SetKeys(true, "Id")
	//dbmap.CreateTablesIfNotExists()
	_, err := dbmap.Exec("create table string_pk_test (Id varchar(255), Name varchar(255));")
	if err != nil {
		t.Errorf("couldn't create string_pk_test: %v", err)
	}
	defer close(dbmap)

	row := &WithStringPk{"1", "foo"}
	err = dbmap.Insert(row)
	if err == nil {
		t.Errorf("Expected error when inserting into table w/non Int PK and autoincr set true")
	}
}

func Test_SqlQueryRunnerInterfaceSelects(t *testing.T) {
	dbMapType := reflect.TypeOf(&DbUtils{})
	sqlExecutorType := reflect.TypeOf((*SqlQueryRunner)(nil)).Elem()
	numDbMapMethods := dbMapType.NumMethod()
	for i := 0; i < numDbMapMethods; i += 1 {
		dbMapMethod := dbMapType.Method(i)
		if !strings.HasPrefix(dbMapMethod.Name, "Select") {
			continue
		}
		if _, found := sqlExecutorType.MethodByName(dbMapMethod.Name); !found {
			t.Errorf("Method %s is defined on godb.DbUtils but not implemented in SqlQueryRunner",
				dbMapMethod.Name)
		}
	}
}



func Test_NullTime(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(WithNullTime{}, "nulltime_test").SetKeys(false, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	// if time is null
	ent := &WithNullTime{
		Id: 0,
		Time: NullTime{
			Valid: false,
		}}
	err := dbmap.Insert(ent)
	if err != nil {
		t.Errorf("failed insert on %s", err.Error())
	}
	err = dbmap.SelectOne(ent, `select * from nulltime_test where `+columnName(dbmap, WithNullTime{}, "Id")+`=:Id`, map[string]interface{}{
		"Id": ent.Id,
	})
	if err != nil {
		t.Errorf("failed select on %s", err.Error())
	}
	if ent.Time.Valid {
		t.Error("NullTime returns valid but expected null.")
	}

	// if time is not null
	ts, err := time.Parse(time.Stamp, "Jan 2 15:04:05")
	ent = &WithNullTime{
		Id: 1,
		Time: NullTime{
			Valid: true,
			Time:  ts,
		}}
	err = dbmap.Insert(ent)
	if err != nil {
		t.Errorf("failed insert on %s", err.Error())
	}
	err = dbmap.SelectOne(ent, `select * from nulltime_test where `+columnName(dbmap, WithNullTime{}, "Id")+`=:Id`, map[string]interface{}{
		"Id": ent.Id,
	})
	if err != nil {
		t.Errorf("failed select on %s", err.Error())
	}
	if !ent.Time.Valid {
		t.Error("NullTime returns invalid but expected valid.")
	}
	if ent.Time.Time.UTC() != ts.UTC() {
		t.Errorf("expect %v but got %v.", ts, ent.Time.Time)
	}

	return
}



type Times struct {
	One time.Time
	Two time.Time
}

type EmbeddedTime struct {
	Id string
	Times
}

func Test_WithTime(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(WithTime{}, "time_test").SetKeys(true, "Id")
	dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	t1 := parseTimeOrPanic("2006-01-02 15:04:05 -0700 MST",
		"2013-08-09 21:30:43 +0800 CST")
	w1 := WithTime{1, t1}
	_insert(dbmap, &w1)

	obj := _get(dbmap, WithTime{}, w1.Id)
	w2 := obj.(*WithTime)
	if w1.Time.UnixNano() != w2.Time.UnixNano() {
		t.Errorf("%v != %v", w1, w2)
	}
}

func parseTimeOrPanic(format, date string) time.Time {
	t1, err := time.Parse(format, date)
	if err != nil {
		panic(err)
	}
	return t1
}

func Test_EmbeddedTime(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTable(EmbeddedTime{}).SetKeys(false, "Id")
	defer close(dbmap)
	err := dbmap.CreateTables()
	if err != nil {
		t.Fatal(err)
	}

	time1 := parseTimeOrPanic("2006-01-02 15:04:05", "2013-08-09 21:30:43")

	t1 := &EmbeddedTime{Id: "abc", Times: Times{One: time1, Two: time1.Add(10 * time.Second)}}
	_insert(dbmap, t1)

	x := _get(dbmap, EmbeddedTime{}, t1.Id)
	t2, _ := x.(*EmbeddedTime)
	if t1.One.UnixNano() != t2.One.UnixNano() || t1.Two.UnixNano() != t2.Two.UnixNano() {
		t.Errorf("%v != %v", t1, t2)
	}
}

type InvoicePersonView struct {
	InvoiceId     int64
	PersonId      int64
	Memo          string
	FName         string
	LegacyVersion int64
}

func Test_InvoicePersonView(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "Id")
	dbmap.AddTableWithName(Person2{}, "person_test").SetKeys(true, "Id")
    dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	// Create some rows
	p1 := &Person2{0, 0, 0, "bob", "smith", 0}
	dbmap.Insert(p1)

	// notice how we can wire up p1.Id to the invoice easily
	inv1 := &Invoice{0, 0, 0, "xmas order", p1.Id, false}
	dbmap.Insert(inv1)

	// Run your query
	query := "select i." + columnName(dbmap, Invoice{}, "Id") + " InvoiceId, p." + columnName(dbmap, Person{}, "Id") + " PersonId, i." + columnName(dbmap, Invoice{}, "Memo") + ", p." + columnName(dbmap, Person{}, "FName") + " " +
		"from invoice_test i, person_test p " +
		"where i." + columnName(dbmap, Invoice{}, "PersonId") + " = p." + columnName(dbmap, Person{}, "Id")

	// pass a slice of pointers to Select()
	// this avoids the need to type assert after the query is run
	var list []*InvoicePersonView
	_, err := dbmap.Select(&list, query)
	if err != nil {
		panic(err)
	}

	// this should test true
	expected := &InvoicePersonView{inv1.Id, p1.Id, inv1.Memo, p1.FName, 0}
	if !reflect.DeepEqual(list[0], expected) {
		t.Errorf("%v != %v", list[0], expected)
	}
}

type FNameOnly struct {
	FName string
}

func TestSelectTooManyCols(t *testing.T) {
	dbmap := initDB()
	dbmap.AddTableWithName(Person2{}, "person_test").SetKeys(true, "Id")
dbmap.CreateTablesIfNotExists()
	defer close(dbmap)

	p1 := &Person2{0, 0, 0, "bob", "smith", 0}
	p2 := &Person2{0, 0, 0, "jane", "doe", 0}
	_insert(dbmap, p1)
	_insert(dbmap, p2)

	obj := _get(dbmap, Person2{}, p1.Id)
	p1 = obj.(*Person2)
	obj = _get(dbmap, Person2{}, p2.Id)
	p2 = obj.(*Person2)

	params := map[string]interface{}{
		"Id": p1.Id,
	}

	var p3 FNameOnly
	err := dbmap.SelectOne(&p3, "select * from person_test where "+columnName(dbmap, Person{}, "Id")+"=:Id", params)
	if err != nil {
		if !NonFatalError(err) {
			t.Error(err)
		}
	} else {
		t.Errorf("Non-fatal error expected")
	}

	if p1.FName != p3.FName {
		t.Errorf("%v != %v", p1.FName, p3.FName)
	}

	var pSlice []FNameOnly
	_, err = dbmap.Select(&pSlice, "select * from person_test order by "+columnName(dbmap, Person{}, "FName")+" asc")
	if err != nil {
		if !NonFatalError(err) {
			t.Error(err)
		}
	} else {
		t.Errorf("Non-fatal error expected")
	}

	if p1.FName != pSlice[0].FName {
		t.Errorf("%v != %v", p1.FName, pSlice[0].FName)
	}
	if p2.FName != pSlice[1].FName {
		t.Errorf("%v != %v", p2.FName, pSlice[1].FName)
	}
}



func rawSelect(dbmap *DbUtils, i interface{}, query string, args ...interface{}) []interface{} {
	list, err := dbmap.Select(i, query, args...)
	if err != nil {
		panic(err)
	}
	return list
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

func rawExec(dbmap *DbUtils, query string, args ...interface{}) sql.Result {
	res, err := dbmap.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

func _get(dbmap *DbUtils, i interface{}, keys ...interface{}) interface{} {
	obj, err := dbmap.Get(i, keys...)
	if err != nil {
		panic(err)
	}

	return obj
}

func _update(dbmap *DbUtils, list ...interface{}) int64 {
	count, err := dbmap.Update(list...)
	if err != nil {
		panic(err)
	}
	return count
}

func _del(dbmap *DbUtils, list ...interface{}) int64 {
	count, err := dbmap.Delete(list...)
	if err != nil {
		panic(err)
	}

	return count
}