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
