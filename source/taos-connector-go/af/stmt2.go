package af

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/af/async"
	"github.com/taosdata/driver-go/v3/af/locker"
	"github.com/taosdata/driver-go/v3/common/stmt"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

type Stmt2 struct {
	stmt2    unsafe.Pointer
	isInsert *bool
	fields   []*stmt.Stmt2AllField
	//colFields    []*stmt.StmtField
	//tagFields    []*stmt.StmtField
	caller       *Stmt2CallBackCaller
	affectedRows int
	queryResult  unsafe.Pointer
	handle       cgo.Handle
	timezone     *time.Location
}

type Stmt2Result struct {
	Res      unsafe.Pointer
	Affected int
	N        int
}

type Stmt2CallBackCaller struct {
	ExecResult chan *Stmt2Result
}

type Stmt2CallBackCallerPool struct {
	pool chan cgo.Handle
}

const Stmt2CBPoolSize = 10000

func NewStmt2CallBackCallerPool(size int) *Stmt2CallBackCallerPool {
	return &Stmt2CallBackCallerPool{
		pool: make(chan cgo.Handle, size),
	}
}

func (p *Stmt2CallBackCallerPool) Get() (cgo.Handle, *Stmt2CallBackCaller) {
	select {
	case h := <-p.pool:
		return h, h.Value().(*Stmt2CallBackCaller)
	default:
		c := &Stmt2CallBackCaller{
			ExecResult: make(chan *Stmt2Result, 1),
		}
		return cgo.NewHandle(c), c
	}
}

func (p *Stmt2CallBackCallerPool) Put(h cgo.Handle) {
	select {
	case p.pool <- h:
	default:
		h.Delete()
	}
}

func (s *Stmt2CallBackCaller) ExecCall(res unsafe.Pointer, affected int, code int) {
	s.ExecResult <- &Stmt2Result{
		Res:      res,
		Affected: affected,
		N:        code,
	}
}

var GlobalStmt2CallBackCallerPool = NewStmt2CallBackCallerPool(Stmt2CBPoolSize)

func NewStmt2(taosConn unsafe.Pointer, reqID int64, singleTableBindOnce bool) *Stmt2 {
	handle, caller := GlobalStmt2CallBackCallerPool.Get()
	locker.Lock()
	stmt2 := wrapper.TaosStmt2Init(taosConn, reqID, true, singleTableBindOnce, handle)
	locker.Unlock()
	return &Stmt2{
		stmt2:  stmt2,
		handle: handle,
		caller: caller,
	}
}

func (s *Stmt2) SetTimezone(tz *time.Location) {
	s.timezone = tz
}

func (s *Stmt2) Prepare(sql string) error {
	locker.Lock()
	defer locker.Unlock()
	code := wrapper.TaosStmt2Prepare(s.stmt2, sql)
	if code != 0 {
		return fmt.Errorf("prepare stmt2 error:%s, sql:%s", wrapper.TaosStmt2Error(s.stmt2), sql)
	}
	isInsert, code := wrapper.TaosStmt2IsInsert(s.stmt2)
	if code != 0 {
		return fmt.Errorf("get stmt2 isInsert error:%s, sql:%s", wrapper.TaosStmt2Error(s.stmt2), sql)
	}
	s.isInsert = &isInsert
	if !isInsert {
		s.fields = nil
	} else {
		fields, err := s.getFields()
		if err != nil {
			s.isInsert = nil
			return fmt.Errorf("get stmt2 col fields error:%s, sql:%s", err.Error(), sql)
		}
		s.fields = fields
	}
	return nil
}

// Bind binds the parameters to the stmt2.
// The params type must equal to the DB type.
// DBType               | GoType
// -----------------------------
// BOOL                 | bool
// TINYINT              | int8
// SMALLINT             | int16
// INT                  | int32
// BIGINT               | int64
// TINYINT UNSIGNED     | uint8
// SMALLINT UNSIGNED    | uint16
// INT UNSIGNED         | uint32
// BIGINT UNSIGNED      | uint64
// FLOAT                | float32
// DOUBLE               | float64
// TIMESTAMP            | time.Time
// BINARY               | []byte
// NCHAR                | string/[]byte
// VARBINARY            | []byte
// GEOMETRY             | []byte
// JSON                 | []byte
// DECIMAL/DECIMAL64    | string
// BLOB                 | []byte/string
func (s *Stmt2) Bind(params []*stmt.TaosStmt2BindData) error {
	if s.isInsert == nil {
		return errors.New("stmt2 is not prepared")
	}
	locker.Lock()
	defer locker.Unlock()
	buffer, err := stmt.MarshalStmt2Binary(params, *s.isInsert, s.fields)
	if err != nil {
		return err
	}
	err = wrapper.TaosStmt2BindBinary(s.stmt2, buffer, -1)
	return err
}

func (s *Stmt2) Execute() error {
	if s.isInsert == nil {
		return errors.New("stmt2 is not prepared")
	}
	locker.Lock()
	code := wrapper.TaosStmt2Exec(s.stmt2)
	locker.Unlock()
	if code != 0 {
		return s.stmt2Err(code)
	}
	r := <-s.caller.ExecResult
	if r.N != 0 {
		return s.stmt2Err(r.N)
	}
	s.queryResult = r.Res
	s.affectedRows = r.Affected
	return nil
}

func (s *Stmt2) GetAffectedRows() int {
	return s.affectedRows
}

func (s *Stmt2) UseResult() (driver.Rows, error) {
	if s.queryResult == nil {
		return nil, taosError.NewError(0xffff, "result is nil!")
	}
	numFields := wrapper.TaosNumFields(s.queryResult)
	rowsHeader, err := wrapper.ReadColumn(s.queryResult, numFields)
	if err != nil {
		return nil, err
	}
	h := async.GetHandler()
	precision := wrapper.TaosResultPrecision(s.queryResult)
	rs := newRows(h, s.queryResult, rowsHeader, precision, true, s.timezone)
	return rs, nil
}

func (s *Stmt2) Close() error {
	if s.stmt2 == nil {
		return nil
	}
	locker.Lock()
	code := wrapper.TaosStmt2Close(s.stmt2)
	locker.Unlock()
	s.stmt2 = nil
	GlobalStmt2CallBackCallerPool.Put(s.handle)
	if code != 0 {
		return s.stmt2Err(code)
	}
	return nil
}

func (s *Stmt2) getFields() ([]*stmt.Stmt2AllField, error) {
	code, count, cFields := wrapper.TaosStmt2GetFields(s.stmt2)
	if code != 0 {
		return nil, s.stmt2Err(code)
	}
	defer func() {
		wrapper.TaosStmt2FreeFields(s.stmt2, cFields)
	}()
	if count == 0 {
		return nil, nil
	}
	fields := wrapper.Stmt2ParseAllFields(count, cFields)
	return fields, nil
}

func (s *Stmt2) stmt2Err(code int) error {
	errStr := wrapper.TaosStmt2Error(s.stmt2)
	return taosError.NewError(code, errStr)
}
