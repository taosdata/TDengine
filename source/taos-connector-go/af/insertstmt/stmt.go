package insertstmt

import (
	"database/sql/driver"
	"errors"
	"unsafe"

	"github.com/taosdata/driver-go/v3/af/locker"
	"github.com/taosdata/driver-go/v3/common/param"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

type InsertStmt struct {
	stmt unsafe.Pointer
}

func NewInsertStmt(taosConn unsafe.Pointer) *InsertStmt {
	locker.Lock()
	stmt := wrapper.TaosStmtInit(taosConn)
	locker.Unlock()
	return &InsertStmt{stmt: stmt}
}

func NewInsertStmtWithReqID(taosConn unsafe.Pointer, reqID int64) *InsertStmt {
	locker.Lock()
	stmt := wrapper.TaosStmtInitWithReqID(taosConn, reqID)
	locker.Unlock()
	return &InsertStmt{stmt: stmt}
}

func (stmt *InsertStmt) Prepare(sql string) error {
	locker.Lock()
	code := wrapper.TaosStmtPrepare(stmt.stmt, sql)
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	isInsert, code := wrapper.TaosStmtIsInsert(stmt.stmt)
	if code != 0 {
		return stmt.stmtErr(code)
	}
	if !isInsert {
		return errors.New("only support insert")
	}
	return nil
}

func (stmt *InsertStmt) SetTableName(name string) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBName(stmt.stmt, name)
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	return nil
}

func (stmt *InsertStmt) SetSubTableName(name string) error {
	locker.Lock()
	code := wrapper.TaosStmtSetSubTBName(stmt.stmt, name)
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	return nil
}

func (stmt *InsertStmt) SetTableNameWithTags(tableName string, tags *param.Param) error {
	locker.Lock()
	code := wrapper.TaosStmtSetTBNameTags(stmt.stmt, tableName, tags.GetValues())
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	return nil
}

func (stmt *InsertStmt) BindParam(params []*param.Param, bindType *param.ColumnType) error {
	data := make([][]driver.Value, len(params))
	for columnIndex, columnData := range params {
		value := columnData.GetValues()
		data[columnIndex] = value
	}
	columnTypes, err := bindType.GetValue()
	if err != nil {
		return err
	}
	locker.Lock()
	code := wrapper.TaosStmtBindParamBatch(stmt.stmt, data, columnTypes)
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	return nil
}

func (stmt *InsertStmt) AddBatch() error {
	locker.Lock()
	code := wrapper.TaosStmtAddBatch(stmt.stmt)
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	return nil
}

func (stmt *InsertStmt) Execute() error {
	locker.Lock()
	code := wrapper.TaosStmtExecute(stmt.stmt)
	locker.Unlock()
	if code != 0 {
		return stmt.stmtErr(code)
	}
	return nil
}

func (stmt *InsertStmt) GetAffectedRows() int {
	return wrapper.TaosStmtAffectedRowsOnce(stmt.stmt)
}

func (stmt *InsertStmt) Close() error {
	locker.Lock()
	code := wrapper.TaosStmtClose(stmt.stmt)
	locker.Unlock()
	var err error
	if code != 0 {
		err = stmt.stmtErr(code)
	}
	stmt.stmt = nil
	return err
}

func (stmt *InsertStmt) stmtErr(code int) error {
	errStr := wrapper.TaosStmtErrStr(stmt.stmt)
	return taosError.NewError(code, errStr)
}
