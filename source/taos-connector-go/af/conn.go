package af

import "C"
import (
	"database/sql/driver"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/af/async"
	"github.com/taosdata/driver-go/v3/af/insertstmt"
	"github.com/taosdata/driver-go/v3/af/locker"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/handler"
)

type Connector struct {
	taos     unsafe.Pointer
	timezone *time.Location
}

// NewConnector New connector with TDengine connection
func NewConnector(taos unsafe.Pointer) (*Connector, error) {
	if taos == nil {
		return nil, errors.ErrTscInvalidConnection
	}
	return &Connector{taos: taos}, nil
}

// Open New connector with TDengine connection information
func Open(host, user, pass, db string, port int) (*Connector, error) {
	if len(user) == 0 {
		user = common.DefaultUser
	}
	if len(pass) == 0 {
		pass = common.DefaultPassword
	}
	locker.Lock()
	tc, err := wrapper.TaosConnect(host, user, pass, db, port)
	locker.Unlock()
	if err != nil {
		return nil, err
	}
	return &Connector{taos: tc}, nil
}

func (conn *Connector) SetTimezone(timezone string) error {
	tz, err := common.ParseTimezone(timezone)
	if err != nil {
		return err
	}
	locker.Lock()
	defer locker.Unlock()
	code := wrapper.TaosOptionsConnection(conn.taos, common.TSDB_OPTION_CONNECTION_TIMEZONE, &timezone)
	if code != 0 {
		return errors.NewError(code, wrapper.TaosErrorStr(nil))
	}
	conn.timezone = tz
	return nil
}

// Close Release TDengine connection
func (conn *Connector) Close() error {
	locker.Lock()
	wrapper.TaosClose(conn.taos)
	locker.Unlock()
	conn.taos = nil
	return nil
}

// StmtExecute Execute sql through stmt
func (conn *Connector) StmtExecute(sql string, params *param.Param) (res driver.Result, err error) {
	stmt := NewStmt(conn.taos)
	if stmt == nil {
		return nil, &errors.TaosError{Code: 0xffff, ErrStr: "failed to init stmt"}
	}
	stmt.SetTimezone(conn.timezone)
	defer func() {
		_ = stmt.Close()
	}()
	return conn.stmtExecute(stmt, sql, params)
}

// StmtExecuteWithReqID Execute sql through stmt with reqID
func (conn *Connector) StmtExecuteWithReqID(sql string, params *param.Param, reqID int64) (res driver.Result, err error) {
	stmt := NewStmtWithReqID(conn.taos, reqID)
	if stmt == nil {
		err = &errors.TaosError{Code: 0xffff, ErrStr: "failed to init stmt"}
		return
	}
	stmt.SetTimezone(conn.timezone)
	defer func() {
		_ = stmt.Close()
	}()
	return conn.stmtExecute(stmt, sql, params)
}

func (conn *Connector) stmtExecute(stmt *Stmt, sql string, params *param.Param) (res driver.Result, err error) {
	err = stmt.Prepare(sql)
	if err != nil {
		return nil, err
	}
	err = stmt.BindRow(params)
	if err != nil {
		return nil, err
	}
	err = stmt.AddBatch()
	if err != nil {
		return nil, err
	}
	err = stmt.Execute()
	if err != nil {
		return nil, err
	}
	result := stmt.GetAffectedRows()
	return driver.RowsAffected(result), nil
}

// Exec Execute sql
func (conn *Connector) Exec(query string, args ...driver.Value) (driver.Result, error) {
	if conn.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		prepared, err := common.InterpolateParams(query, common.ValueArgsToNamedValueArgs(args))
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	asyncHandler := async.GetHandler()
	defer async.PutHandler(asyncHandler)
	result := conn.taosQuery(query, asyncHandler, 0)
	return conn.processExecResult(result)
}

// ExecWithReqID Execute sql with reqID
func (conn *Connector) ExecWithReqID(query string, reqID int64, args ...driver.Value) (driver.Result, error) {
	if conn.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		prepared, err := common.InterpolateParams(query, common.ValueArgsToNamedValueArgs(args))
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	asyncHandler := async.GetHandler()
	defer async.PutHandler(asyncHandler)
	result := conn.taosQuery(query, asyncHandler, reqID)
	return conn.processExecResult(result)
}

func (conn *Connector) processExecResult(result *handler.AsyncResult) (driver.Result, error) {
	defer func() {
		if result != nil && result.Res != nil {
			locker.Lock()
			wrapper.TaosFreeResult(result.Res)
			locker.Unlock()
		}
	}()
	res := result.Res
	if code := wrapper.TaosError(res); code != int(errors.SUCCESS) {
		errStr := wrapper.TaosErrorStr(res)
		return nil, errors.NewError(code, errStr)
	}
	affectRows := wrapper.TaosAffectedRows(res)
	return driver.RowsAffected(affectRows), nil
}

// Query Execute query sql
func (conn *Connector) Query(query string, args ...driver.Value) (driver.Rows, error) {
	if conn.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		prepared, err := common.InterpolateParams(query, common.ValueArgsToNamedValueArgs(args))
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	h := async.GetHandler()
	result := conn.taosQuery(query, h, 0)
	return conn.processQueryResult(result, h)
}

// QueryWithReqID Execute query sql with reqID
func (conn *Connector) QueryWithReqID(query string, reqID int64, args ...driver.Value) (driver.Rows, error) {
	if conn.taos == nil {
		return nil, driver.ErrBadConn
	}
	if len(args) != 0 {
		prepared, err := common.InterpolateParams(query, common.ValueArgsToNamedValueArgs(args))
		if err != nil {
			return nil, err
		}
		query = prepared
	}
	h := async.GetHandler()
	result := conn.taosQuery(query, h, reqID)
	return conn.processQueryResult(result, h)
}

func (conn *Connector) processQueryResult(result *handler.AsyncResult, h *handler.Handler) (driver.Rows, error) {
	res := result.Res
	if code := wrapper.TaosError(res); code != int(errors.SUCCESS) {
		async.PutHandler(h)
		errStr := wrapper.TaosErrorStr(res)
		locker.Lock()
		wrapper.TaosFreeResult(result.Res)
		locker.Unlock()
		return nil, errors.NewError(code, errStr)
	}
	numFields := wrapper.TaosNumFields(res)
	rowsHeader, err := wrapper.ReadColumn(res, numFields)
	if err != nil {
		async.PutHandler(h)
		return nil, err
	}
	precision := wrapper.TaosResultPrecision(res)
	rs := newRows(h, res, rowsHeader, precision, false, conn.timezone)
	return rs, nil
}

func (conn *Connector) taosQuery(sqlStr string, handler *handler.Handler, reqID int64) *handler.AsyncResult {
	locker.Lock()
	if reqID == 0 {
		wrapper.TaosQueryA(conn.taos, sqlStr, handler.Handler)
	} else {
		wrapper.TaosQueryAWithReqID(conn.taos, sqlStr, handler.Handler, reqID)
	}
	locker.Unlock()
	r := <-handler.Caller.QueryResult
	return r
}

// InsertStmt Prepare batch insert stmt
func (conn *Connector) InsertStmt() *insertstmt.InsertStmt {
	return insertstmt.NewInsertStmt(conn.taos)
}

// Stmt Prepare stmt
func (conn *Connector) Stmt() *Stmt {
	stmt := NewStmt(conn.taos)
	if stmt != nil {
		stmt.SetTimezone(conn.timezone)
	}
	return stmt
}

// Stmt2 Prepare stmt2
func (conn *Connector) Stmt2(reqID int64, singleTableBindOnce bool) *Stmt2 {
	stmt2 := NewStmt2(conn.taos, reqID, singleTableBindOnce)
	if stmt2 != nil {
		stmt2.SetTimezone(conn.timezone)
	}
	return stmt2
}

// InsertStmtWithReqID Prepare batch insert stmt with reqID
func (conn *Connector) InsertStmtWithReqID(reqID int64) *insertstmt.InsertStmt {
	return insertstmt.NewInsertStmtWithReqID(conn.taos, reqID)
}

// SelectDB Execute `use db`
func (conn *Connector) SelectDB(db string) error {
	locker.Lock()
	code := wrapper.TaosSelectDB(conn.taos, db)
	locker.Unlock()
	if code != 0 {
		return errors.NewError(code, wrapper.TaosErrorStr(nil))
	}
	return nil
}

// InfluxDBInsertLines Insert data using influxdb line format
// Deprecated
func (conn *Connector) InfluxDBInsertLines(lines []string, precision string) error {
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(conn.taos, lines, wrapper.InfluxDBLineProtocol, precision)
	locker.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
		return errors.NewError(code, errStr)
	}
	locker.Lock()
	wrapper.TaosFreeResult(result)
	locker.Unlock()
	return nil
}

// OpenTSDBInsertTelnetLines Insert data using opentsdb telnet format
// Deprecated
func (conn *Connector) OpenTSDBInsertTelnetLines(lines []string) error {
	locker.Lock()
	result := wrapper.TaosSchemalessInsert(conn.taos, lines, wrapper.OpenTSDBTelnetLineProtocol, "")
	locker.Unlock()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
		return errors.NewError(code, errStr)
	}
	wrapper.TaosFreeResult(result)
	return nil
}

// OpenTSDBInsertJsonPayload Insert data using opentsdb json format
// Deprecated
func (conn *Connector) OpenTSDBInsertJsonPayload(payload string) error {
	result := wrapper.TaosSchemalessInsert(conn.taos, []string{payload}, wrapper.OpenTSDBJsonFormatProtocol, "")
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
		return errors.NewError(code, errStr)
	}
	locker.Lock()
	wrapper.TaosFreeResult(result)
	locker.Unlock()
	return nil
}

func (conn *Connector) GetTableVGroupID(db, table string) (vgID int, err error) {
	var code int
	vgID, code = wrapper.TaosGetTableVgID(conn.taos, db, table)
	if code != 0 {
		err = errors.NewError(code, wrapper.TaosErrorStr(nil))
	}
	return
}

// InfluxDBInsertLinesWithReqID Insert data using influxdb line format
func (conn *Connector) InfluxDBInsertLinesWithReqID(lines string, precision string, reqID int64, ttl int, tbNameKey string) error {
	locker.Lock()
	_, result := wrapper.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn.taos, lines, wrapper.InfluxDBLineProtocol, precision, ttl, reqID, tbNameKey)
	locker.Unlock()
	defer func() {
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
	}()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		return errors.NewError(code, errStr)
	}
	return nil
}

// OpenTSDBInsertTelnetLinesWithReqID Insert data using opentsdb telnet format
func (conn *Connector) OpenTSDBInsertTelnetLinesWithReqID(lines string, reqID int64, ttl int, tbNameKey string) error {
	locker.Lock()
	_, result := wrapper.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn.taos, lines, wrapper.OpenTSDBTelnetLineProtocol, "", ttl, reqID, tbNameKey)
	locker.Unlock()
	defer func() {
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
	}()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		return errors.NewError(code, errStr)
	}
	return nil
}

// OpenTSDBInsertJsonPayloadWithReqID Insert data using opentsdb json format
func (conn *Connector) OpenTSDBInsertJsonPayloadWithReqID(payload string, reqID int64, ttl int, tbNameKey string) error {
	locker.Lock()
	_, result := wrapper.TaosSchemalessInsertRawTTLWithReqIDTBNameKey(conn.taos, payload, wrapper.OpenTSDBJsonFormatProtocol, "", ttl, reqID, tbNameKey)
	locker.Unlock()
	defer func() {
		locker.Lock()
		wrapper.TaosFreeResult(result)
		locker.Unlock()
	}()
	code := wrapper.TaosError(result)
	if code != 0 {
		errStr := wrapper.TaosErrorStr(result)
		return errors.NewError(code, errStr)
	}
	return nil
}
