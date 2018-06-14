package godb

import (
	"time"
	"testing"
	"fmt"
)

func Test_StructToSlice(t *testing.T)  {
	user:=User{Id:1,
		Username:"cly",
		Password:"123",
		Price:0.1,
		Sex:1,
		CreateTime:time.Now()}

	query,args,err:=StructToSlice("select * from t_test where `username`=?Username",&user);
	if err!=nil{
		t.Error(err)
	}
	fmt.Println(query)
	fmt.Println(args)

}

func Test_MapToSlice(t *testing.T)  {

	var m map[string]interface{}
	m = make(map[string]interface{})
	m["Username"] = "cly"
	m["Id"] = 1
	query,args,err:=MapToSlice("select * from t_test where `id`=?Id",&m);
	if err!=nil{
		t.Error(err)
	}
	fmt.Println(query)
	fmt.Println(args)
}