package godb

import (
	"testing"
	"fmt"
	"time"
	"reflect"
)

func Test_StructToSlice(t *testing.T)  {
	user:=User{Id:1,
		Username:"cly",
		Password:"123",
		Price:0.1,
		Sex:1,
		CreatedTime:NullTime{time.Now(),true},
	}

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

func Test_totype(t *testing.T)  {

	user:=&User{}
	t1 ,err:=toType(user)
	if err!=nil{
		panic(err)
	}
	fmt.Println(t1)

}

func Test_toslicetype(t *testing.T)  {
	//users :=[]*User{}

	//users = append(users,&User{Id:1})

	//t1:=reflect.TypeOf(users)

	//fmt.Println(t1)


    /*
	t1,err :=toSliceType(users)

	if err!=nil{
		panic(err)
	}
	fmt.Println(t1)*/

	user :=&User{Id:1}
	t1:=reflect.TypeOf(user)
	fmt.Println(t1);

	if t1.Kind() == reflect.Ptr{
		t1 = t1.Elem()
	}

	fmt.Println(t1)

	if t1.Kind() == reflect.Struct{
		fmt.Println(t1.Kind())
	}

	t2,_ :=toType(user)
	fmt.Println(t2)

	t3,_ :=toSliceType(user)

	fmt.Println(t3)

}
