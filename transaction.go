package godb

import (
	"context"
	"database/sql"
)

type Transaction struct {
	ctx      context.Context
	dbUtils  *DbUtils
	tx       *sql.Tx
	closed   bool
}

func (t *Transaction) WithContext(ctx context.Context) SqlQueryRunner {
	copy := &Transaction{}
	*copy = *t
	copy.ctx = ctx
	return copy
}

// Insert has the same behavior as DbMap.Insert(), but runs in a transaction.
func (t *Transaction) Insert(list ...interface{}) error {
	return insert(t.dbUtils, t, list...)
}

// Update had the same behavior as DbMap.Update(), but runs in a transaction.
func (t *Transaction) Update(list ...interface{}) (int64, error) {
	return update(t.dbUtils, t, list...)
}

// Delete has the same behavior as DbMap.Delete(), but runs in a transaction.
func (t *Transaction) Delete(list ...interface{}) (int64, error) {
	return delete(t.dbUtils, t, list...)
}

// Get has the same behavior as DbMap.Get(), but runs in a transaction.
func (t *Transaction) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	return get(t.dbUtils, t, i, keys...)
}

// Select has the same behavior as DbMap.Select(), but runs in a transaction.
func (t *Transaction) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	return nil, nil
}

// Exec has the same behavior as DbMap.Exec(), but runs in a transaction.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	return nil, nil
}

// SelectInt is a convenience wrapper around the gorp.SelectInt function.
func (t *Transaction) SelectInt(query string, args ...interface{}) (int64, error) {
	return 0, nil
}

// SelectNullInt is a convenience wrapper around the gorp.SelectNullInt function.
func (t *Transaction) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	return sql.NullInt64{}, nil
}

// SelectFloat is a convenience wrapper around the gorp.SelectFloat function.
func (t *Transaction) SelectFloat(query string, args ...interface{}) (float64, error) {
	return 0, nil
}

// SelectNullFloat is a convenience wrapper around the gorp.SelectNullFloat function.
func (t *Transaction) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	return sql.NullFloat64{}, nil
}

// SelectStr is a convenience wrapper around the gorp.SelectStr function.
func (t *Transaction) SelectStr(query string, args ...interface{}) (string, error) {
	return "", nil
}

// SelectNullStr is a convenience wrapper around the gorp.SelectNullStr function.
func (t *Transaction) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	return sql.NullString{}, nil
}

// SelectOne is a convenience wrapper around the gorp.SelectOne function.
func (t *Transaction) SelectOne(holder interface{}, query string, args ...interface{}) error {
	return nil
}


func (t *Transaction) QueryRow(query string, args ...interface{}) *sql.Row {

	return nil
}

func (t *Transaction) Query(q string, args ...interface{}) (*sql.Rows, error) {

	return nil, nil
}