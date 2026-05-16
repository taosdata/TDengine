package recordsql

import (
	"container/list"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/innerjson"
	"github.com/taosdata/taosadapter/v3/tools/parsebindbinary"
)

type StmtAction int

const (
	StmtActionPrepare StmtAction = iota
	StmtActionBind
	StmtActionExecute
)

func (a StmtAction) String() string {
	switch a {
	case StmtActionPrepare:
		return "prepare"
	case StmtActionBind:
		return "bind"
	case StmtActionExecute:
		return "exec"
	default:
		return strconv.Itoa(int(a))
	}
}

type StmtRecord struct {
	IP           string         // Client ip
	SourcePort   string         // source port
	User         string         // Username
	ConnType     ConnType       // ConnType (HTTP,WS)
	QID          uint64         // Query ID
	AppName      string         // application name
	StmtPointer  uintptr        // statement pointer
	Action       StmtAction     // statement action
	ResultCode   int            // result code
	StartTime    time.Time      // start time
	EndTime      time.Time      // end time
	SQL          string         // prepared SQL
	AffectedRows int            // execute affected rows
	BindData     []byte         // bind data
	mission      *RecordMission // mission to which this record belongs
	ele          *list.Element  // element in the record recordList
	sync.Mutex                  // lock for thread safety
}

func (r *StmtRecord) init(stmtPointer uintptr, action StmtAction, ip string, port string, AppName string, user string, connType ConnType, qid uint64, startTime time.Time) {
	r.StmtPointer = stmtPointer
	r.Action = action
	r.IP = ip
	r.User = user
	r.ConnType = connType
	r.QID = qid
	r.StartTime = startTime
	r.SourcePort = port
	r.AppName = AppName
}

func (r *StmtRecord) InitPrepare(stmtPointer uintptr, ip string, port string, AppName string, user string, connType ConnType, qid uint64, startTime time.Time, sql string) {
	r.Lock()
	defer r.Unlock()
	r.init(stmtPointer, StmtActionPrepare, ip, port, AppName, user, connType, qid, startTime)
	r.SQL = sql
}

func (r *StmtRecord) InitBind(stmtPointer uintptr, ip string, port string, AppName string, user string, connType ConnType, qid uint64, startTime time.Time, bindData []byte) {
	r.Lock()
	defer r.Unlock()
	r.init(stmtPointer, StmtActionBind, ip, port, AppName, user, connType, qid, startTime)
	r.BindData = bindData
}

func (r *StmtRecord) InitExecute(stmtPointer uintptr, ip string, port string, AppName string, user string, connType ConnType, qid uint64, startTime time.Time) {
	r.Lock()
	defer r.Unlock()
	r.init(stmtPointer, StmtActionExecute, ip, port, AppName, user, connType, qid, startTime)
}

func (r *StmtRecord) SetPrepareEnd(resultCode int) {
	end := time.Now()
	r.Lock()
	defer r.Unlock()
	r.ResultCode = resultCode
	r.EndTime = end
}

func (r *StmtRecord) SetBindEnd(resultCode int) {
	end := time.Now()
	r.Lock()
	defer r.Unlock()
	r.ResultCode = resultCode
	r.EndTime = end
}

func (r *StmtRecord) SetExecuteEnd(resultCode int, affectedRows int) {
	end := time.Now()
	r.Lock()
	defer r.Unlock()
	r.ResultCode = resultCode
	r.AffectedRows = affectedRows
	r.EndTime = end
}

func (r *StmtRecord) write() {
	if r.mission != nil && r.mission.recordList != nil {
		val := r.mission.recordList.Remove(r.ele)
		if val != nil {
			r.mission.writeStmtRecord(r)
		}
	}
}

func (r *StmtRecord) reset() {
	r.StmtPointer = 0
	r.Action = 0
	r.IP = ""
	r.User = ""
	r.ConnType = 0
	r.QID = 0
	r.StartTime = time.Time{}
	r.EndTime = time.Time{}
	r.ResultCode = 0
	r.SQL = ""
	r.AffectedRows = 0
	r.BindData = nil
	r.SourcePort = ""
	r.AppName = ""
	r.ele = nil
	r.mission = nil
}

const (
	StmtTSIndex = iota
	StmtIPIndex
	StmtSourcePortIndex
	StmtAppNameIndex
	StmtUserIndex
	StmtConnTypeIndex
	StmtQIDIndex
	StmtStartTimeIndex
	StmtStmtPointerIndex
	StmtActionIndex
	StmtResultCodeIndex
	StmtDurationIndex
	StmtDataIndex
	StmtFiledCount
)

func (r *StmtRecord) toRow() []string {
	now := time.Now()
	row := make([]string, StmtFiledCount)
	row[StmtTSIndex] = now.Format(ResultTimeFormat)
	row[StmtIPIndex] = r.IP
	row[StmtSourcePortIndex] = r.SourcePort
	row[StmtAppNameIndex] = r.AppName
	row[StmtUserIndex] = r.User
	row[StmtConnTypeIndex] = r.ConnType.String()
	row[StmtQIDIndex] = fmt.Sprintf("0x%x", r.QID)
	row[StmtStartTimeIndex] = r.StartTime.Format(ResultTimeFormat)
	row[StmtStmtPointerIndex] = fmt.Sprintf("0x%x", r.StmtPointer)
	row[StmtActionIndex] = r.Action.String()
	if r.ResultCode == 0 {
		row[StmtResultCodeIndex] = "0"
	} else {
		row[StmtResultCodeIndex] = fmt.Sprintf("0x%x", r.ResultCode&0xffff)
	}
	// calculate duration
	if !r.EndTime.IsZero() {
		row[StmtDurationIndex] = strconv.FormatInt(r.EndTime.Sub(r.StartTime).Microseconds(), 10)
	} else {
		row[StmtDurationIndex] = "-1"
	}
	switch r.Action {
	case StmtActionBind:
		row[StmtDataIndex] = parseBindData(r.BindData)
	case StmtActionExecute:
		row[StmtDataIndex] = strconv.Itoa(r.AffectedRows)
	case StmtActionPrepare:
		row[StmtDataIndex] = r.SQL
	}
	return row
}

func parseBindData(data []byte) (result string) {
	defer func() {
		if r := recover(); r != nil {
			result = fmt.Sprintf("%x", data)
		}
	}()

	var err error
	result, err = tryParseBindData(data)
	if err != nil {
		return fmt.Sprintf("%x", data)
	}

	return result
}

func tryParseBindData(data []byte) (string, error) {
	bindv, err := parsebindbinary.ParseStmt2BindV(data)
	if err != nil {
		return "", err
	}
	bs, err := innerjson.Marshal(bindv)
	if err != nil {
		return "", err
	}
	return bytesutil.ToUnsafeString(bs), nil
}

var stmtRecordPool sync.Pool

func GetStmtRecord() (record *StmtRecord, running bool) {
	logger.Tracef("start get stmt record")
	mission := getMission(RecordTypeStmt)
	if mission == nil {
		logger.Tracef("no record mission running")
		return nil, false
	}
	mission.runningLock.RLock()
	defer mission.runningLock.RUnlock()
	if !mission.running {
		logger.Tracef("record mission is not running")
		return nil, false
	}
	cachedRecord := stmtRecordPool.Get()
	if cachedRecord == nil {
		record = &StmtRecord{}
	} else {
		record = cachedRecord.(*StmtRecord)
	}
	record.mission = mission
	mission.currentCount.Add(1)
	ele := mission.recordList.Add(record)
	record.ele = ele
	logger.Tracef("get stmt record success")
	return record, true
}

func PutStmtRecord(record *StmtRecord) {
	record.write()
	record.reset()
	stmtRecordPool.Put(record)
}
