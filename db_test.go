package godb

import (
	_ "github.com/go-sql-driver/mysql"
	"time"
	"fmt"
	"testing"
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

