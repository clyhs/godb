package godb

import (
	"database/sql"
	"testing"
	"fmt"
	"reflect"
)

func initDB() *DbUtils {

	db, err := sql.Open("mysql", "root:123456@/testdb?charset=utf8");
	if err != nil {
		panic("Error connecting to db: " + err.Error())
	}
    dialect :=&MySQLDialect{Engine:"InnoDB",Encoding:"utf8"}

    dbUtils :=&DbUtils{Db:db,Dialect:dialect}

	return dbUtils
}



func TestDbUtils_SelectInt(t *testing.T) {
	dbUtils:=initDB()

	i64 := selectInt(dbUtils, "select id from t_test where username='cly0'")

	fmt.Println(i64)

	n := selectNullInt(dbUtils, "select id from t_test where username='notfound'")
	if !reflect.DeepEqual(n, sql.NullInt64{0, false}) {
		t.Errorf("nullint %v != 0,false", n)
	}

	fmt.Println(n)

}

func TestDbUtils_SelectOne(t *testing.T) {
	dbUtils:=initDB()
	var u User
	params :=map[string]interface{}{"id":401}
	err:=dbUtils.SelectOne(&u,"select * from t_test where id=:id",params)
	if err!=nil{
		panic(err)
	}
    //{401 cly0 123456 1.2 1 {2018-06-13 14:58:50 +0000 UTC true}}
	fmt.Println(u)


}

type NameOnly struct {
	Username string
}

func TestDbUtils_SelectOne2(t *testing.T) {
	dbUtils:=initDB()
	var u NameOnly
	params :=map[string]interface{}{"id":401}
	err:=dbUtils.SelectOne(&u,"select username from t_test where id=:id",params)
	if err!=nil{
		panic(err)
	}
	//{401 cly0 123456 1.2 1 {2018-06-13 14:58:50 +0000 UTC true}}
	fmt.Println(u)
}

func TestDbUtils_selectlist(t *testing.T) {

	dbUtils:=initDB()
	var u []NameOnly
	//params :=map[string]interface{}{"id":401}
	list, err:=dbUtils.Select(&u,"select username from t_test ")
	if err!=nil{
		panic(err)
	}
	//{401 cly0 123456 1.2 1 {2018-06-13 14:58:50 +0000 UTC true}}
	fmt.Println(list)
}



func selectInt(dbUtils *DbUtils, query string, args ...interface{}) int64 {
	i64, err := SelectInt(dbUtils, query, args...)
	if err != nil {
		panic(err)
	}
	return i64
}

func selectNullInt(dbUtils *DbUtils, query string, args ...interface{}) sql.NullInt64 {
	i64, err := SelectNullInt(dbUtils, query, args...)
	if err != nil {
		panic(err)
	}

	return i64
}



