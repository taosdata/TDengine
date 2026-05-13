package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"strings"
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
)

// TaosSetConfig int   taos_set_config(const char *config);
func TaosSetConfig(params map[string]string) error {
	if len(params) == 0 {
		return nil
	}
	buf := &strings.Builder{}
	for k, v := range params {
		buf.WriteString(k)
		buf.WriteString(" ")
		buf.WriteString(v)
	}
	cConfig := C.CString(buf.String())
	defer C.free(unsafe.Pointer(cConfig))
	result := (C.struct_setConfRet)(C.taos_set_config(cConfig))
	if int(result.retCode) == -5 || int(result.retCode) == 0 {
		return nil
	}
	buf.Reset()
	for _, c := range result.retMsg {
		if c == 0 {
			break
		}
		buf.WriteByte(byte(c))
	}
	return &errors.TaosError{Code: int32(result.retCode) & 0xffff, ErrStr: buf.String()}
}
