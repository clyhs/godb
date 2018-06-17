package godb

import (
	"testing"
	"time"
	"fmt"
	"reflect"
)

func Test_BaseType(t *testing.T)  {

	t1 :=time.Time{}
	val:= reflect.TypeOf(t1)


	fmt.Println(val == reflect.TypeOf(time.Time{}))
	
}
