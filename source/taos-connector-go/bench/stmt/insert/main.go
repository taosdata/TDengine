package main

import (
	"log"
	"net/http"
	"time"
	"unsafe"

	_ "net/http/pprof"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
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
	sql := "insert into all_type values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	for {
		now := time.Now()
		p := param.NewParam(14).
			AddTimestamp(now, common.PrecisionNanoSecond).
			AddBool(true).
			AddTinyint(1).
			AddSmallint(2).
			AddInt(3).
			AddBigint(4).
			AddUTinyint(5).
			AddUSmallint(6).
			AddUInt(7).
			AddUBigint(8).
			AddFloat(9).
			AddDouble(10).
			AddBinary([]byte("11")).
			AddNchar("12")
		insertStmt := wrapper.TaosStmtInit(conn)
		code := wrapper.TaosStmtPrepare(insertStmt, sql)
		if code != 0 {
			errStr := wrapper.TaosStmtErrStr(insertStmt)
			err = errors.NewError(code, errStr)
			panic(err)
		}
		code = wrapper.TaosStmtBindParam(insertStmt, p.GetValues())
		if code != 0 {
			errStr := wrapper.TaosStmtErrStr(insertStmt)
			err = errors.NewError(code, errStr)
			panic(err)
		}
		code = wrapper.TaosStmtAddBatch(insertStmt)
		if code != 0 {
			errStr := wrapper.TaosStmtErrStr(insertStmt)
			err = errors.NewError(code, errStr)
			panic(err)
		}
		code = wrapper.TaosStmtExecute(insertStmt)
		if code != 0 {
			errStr := wrapper.TaosStmtErrStr(insertStmt)
			err = errors.NewError(code, errStr)
			panic(err)
		}
		affectedRows := wrapper.TaosStmtAffectedRowsOnce(insertStmt)
		if affectedRows != 1 {
			log.Fatalf("expect 1 got %d", affectedRows)
		}
		code = wrapper.TaosStmtClose(insertStmt)
		if code != 0 {
			errStr := wrapper.TaosStmtErrStr(insertStmt)
			err = errors.NewError(code, errStr)
			panic(err)
		}
		time.Sleep(time.Microsecond)
	}
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
