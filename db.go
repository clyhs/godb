package godb

import "database/sql"

type DB struct {
	*sql.DB
}

func Open(driverName string,dataSourceName string)(*DB,error)  {
	db,err:=sql.Open(driverName,dataSourceName);
	if err!=nil {
		return nil,err
	}
	return &DB{db},nil
}