/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package taosSql

/*
#cgo CFLAGS : -I/usr/include
#cgo LDFLAGS: -L/usr/lib -ltaos
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"

import (
	"errors"
	"unsafe"
)

func (mc *taosConn) taosConnect(ip, user, pass, db string, port int) (taos unsafe.Pointer, err error){
	cuser := C.CString(user)
	cpass := C.CString(pass)
	cip   := C.CString(ip)
	cdb   := C.CString("")
	defer C.free(unsafe.Pointer(cip))
	defer C.free(unsafe.Pointer(cuser))
	defer C.free(unsafe.Pointer(cpass))
	defer C.free(unsafe.Pointer(cdb))

	taosObj := C.taos_connect(cip, cuser, cpass, cdb, (C.int)(port))
    if taosObj == nil {
        return nil, errors.New("taos_connect() fail!")
    }

    return (unsafe.Pointer)(taosObj), nil
} 

func (mc *taosConn) taosQuery(sqlstr string) (int, error) {
	taosLog.Printf("taosQuery() input sql:%s\n", sqlstr)

    csqlstr   := C.CString(sqlstr)
	defer C.free(unsafe.Pointer(csqlstr))
    code := int(C.taos_query(mc.taos, csqlstr))

    if 0 != code {
    	mc.taos_error()
    	errStr := C.GoString(C.taos_errstr(mc.taos))
    	taosLog.Println("taos_query() failed:", errStr)
	    return 0, errors.New(errStr)
    }

    // read result and save into mc struct
    num_fields := int(C.taos_field_count(mc.taos))
    if 0 == num_fields { // there are no select and show kinds of commands
        mc.affectedRows = int(C.taos_affected_rows(mc.taos))
        mc.insertId     = 0
    }

    return num_fields, nil
}

func (mc *taosConn) taos_close() {
	C.taos_close(mc.taos)
}

func (mc *taosConn) taos_error() {
    // free local resouce: allocated memory/metric-meta refcnt
    //var pRes unsafe.Pointer
    pRes := C.taos_use_result(mc.taos)
    C.taos_free_result(pRes)
}
