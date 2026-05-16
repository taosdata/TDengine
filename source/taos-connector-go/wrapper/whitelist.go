package wrapper

/*
#cgo CFLAGS: -IC:/TDengine/include -I/usr/include
#cgo linux LDFLAGS: -L/usr/lib -ltaos
#cgo windows LDFLAGS: -LC:/TDengine/driver -ltaos
#cgo darwin LDFLAGS: -L/usr/local/lib -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
extern void WhitelistCallback(void *param, int code, TAOS *taos, int numOfWhiteLists, uint64_t* pWhiteLists);
void taos_fetch_whitelist_a_wrapper(TAOS *taos, void *param){
	return taos_fetch_whitelist_a(taos, WhitelistCallback, param);
};
*/
import "C"
import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

// typedef void (*__taos_async_whitelist_fn_t)(void *param, int code, TAOS *taos, int numOfWhiteLists, uint64_t* pWhiteLists);

// TaosFetchWhitelistA DLL_EXPORT void taos_fetch_whitelist_a(TAOS *taos, __taos_async_whitelist_fn_t fp, void *param);
func TaosFetchWhitelistA(taosConnect unsafe.Pointer, caller cgo.Handle) {
	C.taos_fetch_whitelist_a_wrapper(taosConnect, caller.Pointer())
}
