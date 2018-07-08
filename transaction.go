package godb

import (
	"context"
	"database/sql"
	"fmt"
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
	return selectlist(t.dbUtils, t, i, query, args...)
}

// Exec has the same behavior as DbMap.Exec(), but runs in a transaction.
func (t *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	return maybeExpandNamedQueryAndExec(t, query, args...)
}

// SelectInt is a convenience wrapper around the gorp.SelectInt function.
func (t *Transaction) SelectInt(query string, args ...interface{}) (int64, error) {
	return SelectInt(t, query, args...)
}

// SelectNullInt is a convenience wrapper around the gorp.SelectNullInt function.
func (t *Transaction) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	return SelectNullInt(t, query, args...)
}

// SelectFloat is a convenience wrapper around the gorp.SelectFloat function.
func (t *Transaction) SelectFloat(query string, args ...interface{}) (float64, error) {
	return SelectFloat(t, query, args...)
}

// SelectNullFloat is a convenience wrapper around the gorp.SelectNullFloat function.
func (t *Transaction) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	return SelectNullFloat(t, query, args...)
}

// SelectStr is a convenience wrapper around the gorp.SelectStr function.
func (t *Transaction) SelectStr(query string, args ...interface{}) (string, error) {
	return SelectStr(t, query, args...)
}

// SelectNullStr is a convenience wrapper around the gorp.SelectNullStr function.
func (t *Transaction) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	return SelectNullStr(t, query, args...)
}

// SelectOne is a convenience wrapper around the gorp.SelectOne function.
func (t *Transaction) SelectOne(holder interface{}, query string, args ...interface{}) error {
	return SelectOne(t.dbUtils, t, holder, query, args...)
}

func (t *Transaction) Commit() error {
	if !t.closed {
		t.closed = true

		return t.tx.Commit()
	}

	return sql.ErrTxDone
}

// Rollback rolls back the underlying database transaction.
func (t *Transaction) Rollback() error {
	if !t.closed {
		t.closed = true

		return t.tx.Rollback()
	}

	return sql.ErrTxDone
}

func (t *Transaction) Savepoint(name string) error {
	query := "savepoint " + t.dbUtils.Dialect.QuoteField(name)

	fmt.Println(query)
	_, err := maybeExpandNamedQueryAndExec(t, query)
	return err
}

// RollbackToSavepoint rolls back to the savepoint with the given name. The
// name is interpolated directly into the SQL SAVEPOINT statement, so you must
// sanitize it if it is derived from user input.
func (t *Transaction) RollbackToSavepoint(savepoint string) error {
	query := "rollback to savepoint " + t.dbUtils.Dialect.QuoteField(savepoint)

	_, err := maybeExpandNamedQueryAndExec(t, query)
	return err
}

func (t *Transaction) QueryRow(query string, args ...interface{}) *sql.Row {

	return queryRow(t, query, args...)
}

func (t *Transaction) Query(q string, args ...interface{}) (*sql.Rows, error) {

	return query(t, q, args...)
}