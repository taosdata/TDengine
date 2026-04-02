package recordsql

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetStmtRecord(t *testing.T) {
	t.Run("no mission", func(t *testing.T) {
		setMission(RecordTypeStmt, nil)
		record, running := GetStmtRecord()
		assert.Nil(t, record)
		assert.False(t, running)
	})

	t.Run("mission not running", func(t *testing.T) {
		mission := &RecordMission{recordType: RecordTypeStmt, running: false}
		setMission(RecordTypeStmt, mission)
		record, running := GetStmtRecord()
		assert.Nil(t, record)
		assert.False(t, running)
	})

	t.Run("mission running", func(t *testing.T) {
		mission := &RecordMission{
			recordType: RecordTypeStmt,
			running:    true,
			recordList: NewRecordList(),
		}
		setMission(RecordTypeStmt, mission)
		defer setMission(RecordTypeStmt, nil)

		record, running := GetStmtRecord()
		require.NotNil(t, record)
		assert.True(t, running)
		assert.Equal(t, mission, record.mission)
		assert.NotNil(t, record.ele)
	})
}

func TestPutStmtRecord(t *testing.T) {
	mission := &RecordMission{
		recordType: RecordTypeStmt,
		running:    true,
		recordList: NewRecordList(),
		logger:     logrus.NewEntry(logrus.New()),
		csvWriter:  csv.NewWriter(&mockWriter{}),
	}
	setMission(RecordTypeStmt, mission)
	defer setMission(RecordTypeStmt, nil)

	record, _ := GetStmtRecord()
	require.NotNil(t, record)
	record.SQL = "SELECT 1"

	PutStmtRecord(record)

	// Verify record was reset and returned to pool
	assert.Empty(t, record.SQL)
	assert.Nil(t, record.mission)
	assert.Nil(t, record.ele)

	record2, _ := GetStmtRecord()
	assert.NotNil(t, record2)
	assert.Equal(t, record, record2, "Should return the same record from pool")
}

func TestStmtRecordInit(t *testing.T) {
	r := &StmtRecord{}
	sql := "SELECT * FROM test where id = ?"
	ip := "127.0.0.1"
	user := "testuser"
	connType := HTTPType
	qid := uint64(12345)
	startTime := time.Now()
	port := "38000"
	appName := "testapp"

	bindData := []byte{0x01, 0x02, 0x03}

	r.InitPrepare(1, ip, port, appName, user, connType, qid, startTime, sql)

	assert.Equal(t, sql, r.SQL)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, startTime, r.StartTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)

	r.SetPrepareEnd(-123)
	assert.Equal(t, -123, r.ResultCode)

	r.reset()
	assert.Equal(t, "", r.SQL)
	assert.Equal(t, "", r.IP)
	assert.Equal(t, "", r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.Equal(t, time.Time{}, r.StartTime)
	assert.Equal(t, "", r.AppName)
	assert.Equal(t, "", r.SourcePort)

	r.InitBind(2, ip, port, appName, user, connType, qid, startTime, bindData)
	assert.Equal(t, bindData, r.BindData)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, startTime, r.StartTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)

	r.SetBindEnd(-123)
	assert.Equal(t, -123, r.ResultCode)

	r.reset()
	assert.Equal(t, []byte(nil), r.BindData)
	assert.Equal(t, "", r.IP)
	assert.Equal(t, "", r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.Equal(t, time.Time{}, r.StartTime)
	assert.Equal(t, "", r.AppName)
	assert.Equal(t, "", r.SourcePort)

	r.InitExecute(3, ip, port, appName, user, connType, qid, startTime)
	assert.Equal(t, ip, r.IP)
	assert.Equal(t, user, r.User)
	assert.Equal(t, connType, r.ConnType)
	assert.Equal(t, qid, r.QID)
	assert.Equal(t, startTime, r.StartTime)
	assert.Equal(t, appName, r.AppName)
	assert.Equal(t, port, r.SourcePort)

	r.SetExecuteEnd(-123, 123)
	assert.Equal(t, -123, r.ResultCode)
	assert.Equal(t, 123, r.AffectedRows)
	r.reset()
	assert.Equal(t, []byte(nil), r.BindData)
	assert.Equal(t, "", r.IP)
	assert.Equal(t, "", r.User)
	assert.Equal(t, ConnType(0), r.ConnType)
	assert.Equal(t, uint64(0), r.QID)
	assert.Equal(t, time.Time{}, r.StartTime)
	assert.Equal(t, "", r.AppName)
	assert.Equal(t, "", r.SourcePort)

}

var validBindData = []byte{
	// total Length
	0x2f, 0x00, 0x00, 0x00,
	// tableCount
	0x03, 0x00, 0x00, 0x00,
	// TagCount
	0x00, 0x00, 0x00, 0x00,
	// ColCount
	0x00, 0x00, 0x00, 0x00,
	// TableNamesOffset
	0x1c, 0x00, 0x00, 0x00,
	// TagsOffset
	0x00, 0x00, 0x00, 0x00,
	// ColOffset
	0x00, 0x00, 0x00, 0x00,
	// table names
	// TableNameLength
	0x06, 0x00,
	0x01, 0x00,
	0x06, 0x00,
	// test1
	0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
	// nil
	0x00,
	// test2
	0x74, 0x65, 0x73, 0x74, 0x32, 0x00,
}

var invalidBindData = []byte{
	// total Length
	0x42, 0x00, 0x00, 0x00,
	// tableCount
	0x01, 0x00, 0x00, 0x00,
	// TagCount
	0x01, 0x00, 0x00, 0x00,
	// ColCount
	0x00, 0x00, 0x00, 0x00,
	// TableNamesOffset
	0x1c, 0x00, 0x00, 0x00,
	// TagsOffset
	0x24, 0x00, 0x00, 0x00,
	// ColOffset
	0x00, 0x00, 0x00, 0x00,
	// table names
	// table name length
	0x06, 0x00,
	// table name buffer
	0x74, 0x65, 0x73, 0x74, 0x31, 0x00,

	// tags
	// table length
	0x1a, 0x00, 0x00, 0x00,
	//table 0 tags
	//tag 0
	//total length
	0x1a, 0x00, 0x00, 0x00,
	//type wrong!!
	0xff, 0x00, 0x00, 0x00,
	//num
	0x01, 0x00, 0x00, 0x00,
	//is null
	0x00,
	// haveLength
	0x00,
	// buffer length
	0x08, 0x00, 0x00, 0x00,
	0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,
}

func Test_tryParseBindData(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid bind data",
			args: args{
				data: validBindData,
			},
			want: `{"count":3,"table_names":["test1","","test2"],"tags":null,"cols":null}`,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.NoError(t, err, i...)
			},
		},
		{
			name: "invalid bind data - too short",
			args: args{
				data: invalidBindData,
			},
			want: "",
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.Error(t, err, i...)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tryParseBindData(tt.args.data)
			if !tt.wantErr(t, err, fmt.Sprintf("tryParseBindData(%v)", tt.args.data)) {
				return
			}
			assert.Equalf(t, tt.want, got, "tryParseBindData(%v)", tt.args.data)
		})
	}
}

func Test_parseBindData(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name       string
		args       args
		wantResult string
	}{
		{
			name: "valid bind data",
			args: args{
				data: validBindData,
			},
			wantResult: `{"count":3,"table_names":["test1","","test2"],"tags":null,"cols":null}`,
		},
		{
			name: "invalid bind data",
			args: args{
				data: invalidBindData,
			},
			wantResult: "420000000100000001000000000000001c000000240000000000000006007465737431001a0000001a000000ff00000001000000000008000000322b800d92010000",
		},
		{
			name: "invalid bind data - too short",
			args: args{
				data: []byte{0x01, 0x02},
			},
			wantResult: "0102",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.wantResult, parseBindData(tt.args.data), "parseBindData(%v)", tt.args.data)
		})
	}
}

func TestStmtRecordToRow(t *testing.T) {
	now := time.Now()
	r := &StmtRecord{}
	sql := "SELECT * FROM test where id = ?"
	ip := "127.0.0.1"
	user := "testuser"
	connType := WSType
	qid := uint64(0x12345)
	startTime := now
	sourcePort := "38000"
	appName := "testapp"
	stmtPointer := uintptr(0x12345678)
	r.InitPrepare(stmtPointer, ip, sourcePort, appName, user, connType, qid, startTime, sql)
	row := r.toRow()
	checkBaseInfo := func(rowData []string) {
		rowTime, err := time.ParseInLocation(ResultTimeFormat, rowData[StmtTSIndex], time.Local)
		require.NoError(t, err)
		assert.False(t, rowTime.IsZero())
		assert.Greater(t, time.Now().UnixNano(), rowTime.UnixNano())
		assert.Equal(t, ip, rowData[StmtIPIndex])
		assert.Equal(t, sourcePort, rowData[StmtSourcePortIndex])
		assert.Equal(t, appName, rowData[StmtAppNameIndex])
		assert.Equal(t, r.User, rowData[StmtUserIndex])
		assert.Equal(t, "ws", rowData[StmtConnTypeIndex])
		assert.Equal(t, "0x12345", rowData[StmtQIDIndex])
		assert.Equal(t, now.Format(ResultTimeFormat), rowData[StmtStartTimeIndex])
		assert.Equal(t, "0x12345678", rowData[StmtStmtPointerIndex])
	}
	checkBaseInfo(row)
	assert.Equal(t, "prepare", row[StmtActionIndex])
	assert.Equal(t, "0", row[StmtResultCodeIndex])
	assert.Equal(t, "-1", row[StmtDurationIndex])
	assert.Equal(t, sql, row[StmtDataIndex])

	r.SetPrepareEnd(0)
	row = r.toRow()
	checkBaseInfo(row)
	assert.Equal(t, "prepare", row[StmtActionIndex])
	assert.Equal(t, "0", row[StmtResultCodeIndex])
	assert.NotEqual(t, "0", row[StmtDurationIndex])
	duration, err := strconv.Atoi(row[StmtDurationIndex])
	assert.NoError(t, err)
	assert.Greater(t, duration, 0)
	assert.Equal(t, sql, row[StmtDataIndex])

	r.reset()
	r.InitBind(stmtPointer, ip, sourcePort, appName, user, connType, qid, startTime, validBindData)
	row = r.toRow()
	checkBaseInfo(row)
	assert.Equal(t, "bind", row[StmtActionIndex])
	assert.Equal(t, "0", row[StmtResultCodeIndex])
	assert.Equal(t, "-1", row[StmtDurationIndex])
	assert.Equal(t, `{"count":3,"table_names":["test1","","test2"],"tags":null,"cols":null}`, row[StmtDataIndex])

	r.SetBindEnd(0)
	row = r.toRow()
	checkBaseInfo(row)
	assert.Equal(t, "bind", row[StmtActionIndex])
	assert.Equal(t, "0", row[StmtResultCodeIndex])
	assert.NotEqual(t, "0", row[StmtDurationIndex])
	duration, err = strconv.Atoi(row[StmtDurationIndex])
	assert.NoError(t, err)
	assert.Greater(t, duration, 0)
	assert.Equal(t, `{"count":3,"table_names":["test1","","test2"],"tags":null,"cols":null}`, row[StmtDataIndex])

	r.reset()
	r.InitExecute(stmtPointer, ip, sourcePort, appName, user, connType, qid, startTime)
	row = r.toRow()
	checkBaseInfo(row)
	assert.Equal(t, "exec", row[StmtActionIndex])
	assert.Equal(t, "0", row[StmtResultCodeIndex])
	assert.Equal(t, "-1", row[StmtDurationIndex])
	assert.Equal(t, "0", row[StmtDataIndex])

	r.SetExecuteEnd(-1, 10)
	row = r.toRow()
	checkBaseInfo(row)
	assert.Equal(t, "exec", row[StmtActionIndex])
	assert.Equal(t, "0xffff", row[StmtResultCodeIndex])
	assert.NotEqual(t, "0", row[StmtDurationIndex])
	duration, err = strconv.Atoi(row[StmtDurationIndex])
	assert.NoError(t, err)
	assert.Greater(t, duration, 0)
	assert.Equal(t, "10", row[StmtDataIndex])
}
