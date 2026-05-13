package taosWS

import (
	"database/sql/driver"
	"fmt"
)

type BadConnError struct {
	err error
	ctx string
}

func NewBadConnError(err error) *BadConnError {
	return &BadConnError{err: err}
}

func NewBadConnErrorWithCtx(err error, ctx string) *BadConnError {
	return &BadConnError{err: err, ctx: ctx}
}

func (*BadConnError) Unwrap() error {
	return driver.ErrBadConn
}

func (e *BadConnError) Error() string {
	if len(e.ctx) == 0 {
		return e.err.Error()
	}
	return fmt.Sprintf("error %s: context: %s", e.err.Error(), e.ctx)
}
