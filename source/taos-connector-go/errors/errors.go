//! TDengine error codes.
//! THIS IS AUTO GENERATED FROM TDENGINE <taoserror.h>, MAKE SURE YOU KNOW WHAT YOU ARE CHANING.

package errors

import "fmt"

type TaosError struct {
	Code   int32
	ErrStr string
}

const (
	SUCCESS int32 = 0
	//revive:disable-next-line
	TSC_INVALID_CONNECTION int32 = 0x020B
	UNKNOWN                int32 = 0xffff
)

func (e *TaosError) Error() string {
	if e.Code != UNKNOWN {
		return fmt.Sprintf("[0x%x] %s", e.Code, e.ErrStr)
	}
	return e.ErrStr
}

var (
	ErrTscInvalidConnection = &TaosError{
		Code:   TSC_INVALID_CONNECTION,
		ErrStr: "Invalid connection",
	}
)

func NewError(code int, errStr string) error {
	return &TaosError{
		Code:   int32(code) & 0xffff,
		ErrStr: errStr,
	}
}
