package testtools

import (
	"database/sql/driver"
	"fmt"
	"time"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
)

func Exec(conn unsafe.Pointer, sql string) error {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return errors.NewError(code, errStr)
	}
	return nil
}

func Query(conn unsafe.Pointer, sql string) ([][]driver.Value, error) {
	res := wrapper.TaosQuery(conn, sql)
	defer wrapper.TaosFreeResult(res)
	code := wrapper.TaosError(res)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(res)
		return nil, errors.NewError(code, errStr)
	}
	fileCount := wrapper.TaosNumFields(res)
	rh, err := wrapper.ReadColumn(res, fileCount)
	if err != nil {
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	var result [][]driver.Value
	for {
		columns, errCode, block := wrapper.TaosFetchRawBlock(res)
		if errCode != 0 {
			errStr := wrapper.TaosErrorStr(res)
			return nil, errors.NewError(errCode, errStr)
		}
		if columns == 0 {
			break
		}
		r, err := parser.ReadBlock(block, columns, rh.ColTypes, precision)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}
	return result, nil
}

func EnsureDBCreated(dbName string) error {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		return err
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < 100; i++ {
		value, err := Query(conn, fmt.Sprintf(`select * from performance_schema.perf_trans where db = '%s'`, dbName))
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value, err = Query(conn, fmt.Sprintf(`select * from information_schema.ins_databases where name = '%s'`, dbName))
			if err != nil {
				return err
			}
			if len(value) > 0 {
				return nil
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
	return fmt.Errorf("db %s not created after waiting", dbName)
}

func EnsureTokenCreated(tokenName string) error {
	conn, err := wrapper.TaosConnect("", "root", "taosdata", "", 0)
	if err != nil {
		return err
	}
	defer wrapper.TaosClose(conn)
	for i := 0; i < 100; i++ {
		value, err := Query(conn, `select * from performance_schema.perf_trans where oper = 'create-token'`)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			value, err := Query(conn, fmt.Sprintf(`select * from information_schema.ins_tokens where name = '%s'`, tokenName))
			if err != nil {
				return err
			}
			if len(value) > 0 {
				return nil
			}
		}
		time.Sleep(time.Millisecond * 500)
	}
	return fmt.Errorf("token not created after waiting")
}
