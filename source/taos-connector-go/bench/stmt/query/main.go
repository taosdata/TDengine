package main

import (
	"database/sql/driver"
	"fmt"
	"net/http"
	"unsafe"

	_ "net/http/pprof"

	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

func main() {
	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			panic(err)
		}
	}()
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		panic(err)
	}
	defer wrapper.TaosClose(conn)
	err = exec(conn, "create database if not exists b_stmt precision 'ns' keep 36500")
	if err != nil {
		panic(err)
	}
	err = exec(conn, "use b_stmt")
	if err != nil {
		panic(err)
	}
	err = exec(conn, "create table if not exists all_type(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20))")
	if err != nil {
		panic(err)
	}
	sql := "insert into all_type values(now,true,1,2,3,4,5,6,7,8,9,10,'11','12')"
	err = exec(conn, sql)
	if err != nil {
		panic(err)
	}
	rows, err := StmtQuery(conn, "select * from all_type where c3 = ?", param.NewParam(1).AddTinyint(2))
	if err != nil {
		panic(err)
	}
	fmt.Println(rows)
}

func exec(conn unsafe.Pointer, sql string) error {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return errors.NewError(code, errStr)
	}
	return nil
}

func StmtQuery(conn unsafe.Pointer, sql string, params *param.Param) (rows [][]driver.Value, err error) {
	stmt := wrapper.TaosStmtInit(conn)
	if stmt == nil {
		err = errors.NewError(0xffff, "failed to init stmt")
		return
	}
	defer wrapper.TaosStmtClose(stmt)
	code := wrapper.TaosStmtPrepare(stmt, sql)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt)
		return nil, errors.NewError(code, errStr)
	}
	value := params.GetValues()
	code = wrapper.TaosStmtBindParam(stmt, value)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt)
		return nil, errors.NewError(code, errStr)
	}
	code = wrapper.TaosStmtExecute(stmt)
	if code != 0 {
		errStr := wrapper.TaosStmtErrStr(stmt)
		return nil, errors.NewError(code, errStr)
	}
	res := wrapper.TaosStmtUseResult(stmt)
	numFields := wrapper.TaosFieldCount(res)
	rowsHeader, err := wrapper.ReadColumn(res, numFields)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	var data [][]driver.Value
	for {
		blockSize, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != int(errors.SUCCESS) {
			errStr := wrapper.TaosErrorStr(res)
			err := errors.NewError(code, errStr)
			wrapper.TaosFreeResult(res)
			return nil, err
		}
		if blockSize == 0 {
			break
		}
		d, err := parser.ReadBlock(block, blockSize, rowsHeader.ColTypes, precision)
		if err != nil {
			wrapper.TaosFreeResult(res)
			return nil, err
		}
		data = append(data, d...)
	}
	wrapper.TaosFreeResult(res)
	return data, nil
}
