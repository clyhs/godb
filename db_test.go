package godb

import (
	_ "github.com/go-sql-driver/mysql"
	"time"
	"fmt"
	"testing"
	"reflect"
)

var(
	createTableSql = "CREATE TABLE IF NOT EXISTS `t_test` (" +
		"`id` INTEGER PRIMARY KEY AUTO_INCREMENT NOT NULL, " +
		"`username` varchar(50) NULL, " +
		"`password` VARCHAR(50) default null, " +
		"`price` float default NULL, " +
		"`sex` integer default 0, " +
		"`createdTime` datetime)" +
		" ENGINE=MyISAM DEFAULT CHARSET=utf8;"
)
/*
+-------------+-------------+------+-----+---------+----------------+
| Field       | Type        | Null | Key | Default | Extra          |
+-------------+-------------+------+-----+---------+----------------+
| id          | int(11)     | NO   | PRI | NULL    | auto_increment |
| username    | varchar(50) | YES  |     | NULL    |                |
| password    | varchar(50) | YES  |     | NULL    |                |
| price       | float       | YES  |     | NULL    |                |
| sex         | int(11)     | YES  |     | 0       |                |
| createdTime | datetime    | YES  |     | NULL    |                |
+-------------+-------------+------+-----+---------+----------------+
 */

type User struct {
	Id       int
	Username string
	Password string
	Price    float32
	Sex      int
	CreateTime time.Time
}

func init()  {
	fmt.Println("db_test init...")
}

func testopen() (*DB,error) {

	db,err:=Open("mysql","root:123456@/testdb?charset=utf8&parseTime=true")
	if err!=nil{
		return nil,err
	}
	return db,nil
}

func Test_db(t *testing.T)  {
	fmt.Println("test_db...")

	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	_, err= db.Exec(createTableSql)
	if err != nil {
		t.Error(err)
	}
    /*
	for i := 0; i < 100; i++ {
		name:="cly"+strconv.Itoa(i)
		_, err = db.Exec("insert into t_test (`username`, password, price, sex, createdTime) values (?,?,?,?,?)",
			name, "123456", 1.2, 1, time.Now())
		if err != nil {
			t.Error(err)
		}
	}*/

	rows,err := db.Query("select * from t_test")
	for rows.Next()  {
		var Id int
		var Username ,Password string
		var Price float32
		var Sex int
		var CreatedTime time.Time
		err = rows.Scan(&Id,&Username,&Password,&Price,&Sex,&CreatedTime)
		if err!=nil {
			t.Error(err)
		}
		fmt.Println(Id,Username,Password,Price,Sex,CreatedTime)
	}

}

func Test_Query(t *testing.T)  {
	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()


	rows,err := db.Query("select * from t_test limit 0,5")
	for rows.Next()  {
		var Id int
		var Username ,Password string
		var Price float32
		var Sex int
		var CreatedTime time.Time
		err = rows.Scan(&Id,&Username,&Password,&Price,&Sex,&CreatedTime)
		if err!=nil {
			t.Error(err)
		}
		fmt.Println(Id,Username,Password,Price,Sex,CreatedTime)
	}
	rows.Close()

}

func Test_ScanStructByIndex(t *testing.T)  {

	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows,err := db.Query("select * from t_test limit 0,5")
	for rows.Next() {
		var user User
		err = rows.ScanStructByIndex(&user)
		if err != nil {
			t.Error(err)
		}
		fmt.Println(user)

	}
	rows.Close()

}

func Test_ScanMap(t *testing.T)  {

	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query("select * from t_test limit 0,10")
	if err != nil {
		t.Error(err)
	}

	for rows.Next() {
		m := make(map[string]interface{})
		err = rows.ScanMap(&m)
		if err != nil {
			t.Error(err)
		}
		fmt.Println(m)
	}

	rows.Close()
}

func Test_select(t *testing.T)  {

	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows,err := db.Query("select * from t_test limit 0,10")

	columns, _ := rows.Columns()
	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		//将行数据保存到record字典
		err = rows.Scan(scanArgs...)
		record := make(map[string]interface{})
		for i, col := range values {
			if col != nil {
				if(reflect.TypeOf(col) ==reflect.TypeOf(time.Time{}) ){
					record[columns[i]] =  col;
				}else{
					record[columns[i]] = string(col.([]byte));
				}
			}
		}
		fmt.Println(record)
	}
}

func Test_ReflectMap(t *testing.T)  {
	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows,err := db.Query("select * from t_test limit 0,10")

	cols, _ := rows.Columns()
	newDest := make([]interface{}, len(cols))

	for rows.Next() {
		m := make(map[string]interface{})
		vv := reflect.ValueOf(&m)
		vvv := vv.Elem()
		for i, _ := range cols {
			//var value reflect.Value
			value := reflect.MakeSlice(reflect.SliceOf(vvv.Type().Elem()), DefaultCacheSize, DefaultCacheSize)
			newDest[i] = value.Index(i).Addr().Interface()
		}
		err = rows.Scan(newDest...)
		if err != nil {
			t.Error(err)
		}

		for i, name := range cols {
			vname := reflect.ValueOf(name)
			vvv.SetMapIndex(vname, reflect.ValueOf(newDest[i]).Elem())
		}

		fmt.Println(m)

	}

}

func Test_ScanSlice(t *testing.T)  {

	db,err:=testopen()
	if err!=nil{
		t.Error(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query("select * from t_test limit 0,10")
	if err != nil {
		t.Error(err)
	}

	cols, _ := rows.Columns()


	for rows.Next() {
		slice := make([]interface{}, len(cols))
		err = rows.ScanSlice(&slice)
		if err != nil {
			t.Error(err)
		}
		t.Log(slice)
		fmt.Println(slice)
	}

	rows.Close()
}