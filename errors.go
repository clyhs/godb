package godb

import (
	"errors"
	"fmt"
)

var(
	ErrNullPointer = errors.New("t should be a pointer")
)

type NoFieldInTypeError struct {
	TypeName        string
	MissingColNames []string
}

func (err *NoFieldInTypeError) Error() string {
	return fmt.Sprintf("godb: no fields %+v in type %s", err.MissingColNames, err.TypeName)
}

// returns true if the error is non-fatal (ie, we shouldn't immediately return)
func NonFatalError(err error) bool {
	switch err.(type) {
	case *NoFieldInTypeError:
		return true
	default:
		return false
	}
}