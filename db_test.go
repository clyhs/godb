package godb

import (
	_ "github.com/go-sql-driver/mysql"
	"time"
	"fmt"
	"testing"
	"strconv"
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

func Test_db(t *testing.T)  {
	fmt.Println("test_db...")

	db,err:=Open("mysql","root:123456@/testdb?charset=utf8")
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

	for i := 0; i < 100; i++ {
		name:="cly"+strconv.Itoa(i)
		_, err = db.Exec("insert into t_test (`username`, password, price, sex, createdTime) values (?,?,?,?,?)",
			name, "123456", 1.2, 1, time.Now())
		if err != nil {
			t.Error(err)
		}
	}

}
