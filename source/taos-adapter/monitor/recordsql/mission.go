package recordsql

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

const DefaultRecordSqlEndTime = "2300-01-01 00:00:00"
const DefaultFlushInterval = 5 * time.Second
const (
	InputTimeFormat  = "2006-01-02 15:04:05"
	ResultTimeFormat = "2006-01-02 15:04:05.000000"
)

const (
	TSIndex = iota
	SQLIndex
	IPIndex
	UserIndex
	ConnTypeIndex
	QIDIndex
	ReceiveTimeIndex
	FreeTimeIndex
	QueryDurationIndex
	FetchDurationIndex
	GetConnDurationIndex
	TotalDurationIndex
	SourcePortIndex
	AppNameIndex
	FiledCount
)

type outputWriter interface {
	Write([]string) error
	Flush()
}

type RecordType int

const (
	RecordTypeSQL RecordType = iota + 1
	RecordTypeStmt
)

var recordTypes = []RecordType{RecordTypeSQL, RecordTypeStmt}

func (recordType RecordType) String() string {
	switch recordType {
	case RecordTypeSQL:
		return "Sql"
	case RecordTypeStmt:
		return "Stmt"
	default:
		return fmt.Sprintf("UNKNOWN %d", int(recordType))
	}
}

type RecordMission struct {
	recordType   RecordType
	running      bool
	runningLock  sync.RWMutex
	ctx          context.Context
	cancelFunc   context.CancelFunc
	currentCount atomic.Int32
	startTime    time.Time
	endTime      time.Time
	csvWriter    outputWriter
	startChan    chan struct{}
	writeLock    sync.Mutex
	recordList   *RecordList
	logger       *logrus.Entry
	closeOnce    sync.Once
}

func NewRecordMission(recordType RecordType, startTime, endTime time.Time, writer outputWriter, logger *logrus.Entry) *RecordMission {
	ctx, cancel := context.WithDeadline(context.Background(), endTime)
	return &RecordMission{
		recordType: recordType,
		startTime:  startTime,
		endTime:    endTime,
		csvWriter:  writer,
		ctx:        ctx,
		cancelFunc: cancel,
		startChan:  make(chan struct{}, 1),
		recordList: NewRecordList(),
		logger:     logger,
	}
}

type RecordState struct {
	Exists            bool   `json:"exists"`
	Running           bool   `json:"running"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	CurrentConcurrent int32  `json:"current_concurrent"`
}

type GlobalMission struct {
	RecordSqlMission  *RecordMission
	RecordSqlLock     sync.RWMutex
	RecordStmtMission *RecordMission
	RecordStmtLock    sync.RWMutex
}

var globalMission = &GlobalMission{}

// record sql logger
var logger = log.GetLogger("RSQ")

func StartRecordSql(startTime, endTime string, location string) error {
	return StartRecordWithTestWriter(RecordTypeSQL, startTime, endTime, location, nil)
}

func StartRecordStmt(startTime, endTime string, location string) error {
	return StartRecordWithTestWriter(RecordTypeStmt, startTime, endTime, location, nil)
}

func StartRecordWithTestWriter(recordType RecordType, startTime, endTime string, location string, testWriter io.Writer) error {
	logger.Tracef("start record mission, recordType: %s, startTime: %s, endTime: %s, location: %s", recordType, startTime, endTime, location)
	if IsRunning(recordType) {
		return fmt.Errorf("record mission is already running")
	}

	loc := time.Local
	var err error
	if location != "" {
		loc, err = time.LoadLocation(location)
		if err != nil {
			return fmt.Errorf("invalid location format: %s, error: %s", location, err)
		}
	}
	startTimeParsed, err := time.ParseInLocation(InputTimeFormat, startTime, loc)
	if err != nil {
		return fmt.Errorf("invalid start time format: %s, error: %s", startTime, err)
	}
	endTimeParsed, err := time.ParseInLocation(InputTimeFormat, endTime, loc)
	if err != nil {
		return fmt.Errorf("invalid end time format: %s, error: %s", endTime, err)
	}
	if startTimeParsed.After(endTimeParsed) {
		return fmt.Errorf("start time %s is after end time %s", startTime, endTime)
	}
	if endTimeParsed.Before(time.Now()) {
		return fmt.Errorf("end time %s is in the past", endTime)
	}
	writer := testWriter
	if writer == nil {
		writer, err = getRotateWriter(recordType)
		if err != nil {
			return fmt.Errorf("error getting rotate writer: %s", err)
		}
	}
	csvWriter := csv.NewWriter(writer)
	mission := NewRecordMission(recordType, startTimeParsed, endTimeParsed, csvWriter, logger)
	duration := time.Until(mission.startTime)
	if duration <= 0 {
		// run immediately
		mission.setRunning(true)
		go mission.run(DefaultFlushInterval)
		close(mission.startChan)
	} else {
		go mission.start()
	}
	setMission(recordType, mission)
	return nil
}

type MissionInfo struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

func stopRecordMission(recordType RecordType) *MissionInfo {
	mission := getMission(recordType)
	if mission == nil {
		return nil
	}
	mission.Stop()
	return &MissionInfo{
		StartTime: mission.startTime.Format(InputTimeFormat),
		EndTime:   mission.endTime.Format(InputTimeFormat),
	}
}

func StopRecordSqlMission() *MissionInfo {
	return stopRecordMission(RecordTypeSQL)
}

func StopRecordStmtMission() *MissionInfo {
	return stopRecordMission(RecordTypeStmt)
}

func IsRunning(recordType RecordType) bool {
	mission := getMission(recordType)
	if mission == nil {
		return false
	}
	return mission.isRunning()
}

func getMission(recordType RecordType) *RecordMission {
	switch recordType {
	case RecordTypeSQL:
		globalMission.RecordSqlLock.RLock()
		defer globalMission.RecordSqlLock.RUnlock()
		return globalMission.RecordSqlMission
	case RecordTypeStmt:
		globalMission.RecordStmtLock.RLock()
		defer globalMission.RecordStmtLock.RUnlock()
		return globalMission.RecordStmtMission
	}
	return nil
}

func setMission(recordType RecordType, mission *RecordMission) {
	switch recordType {
	case RecordTypeSQL:
		globalMission.RecordSqlLock.Lock()
		defer globalMission.RecordSqlLock.Unlock()
		globalMission.RecordSqlMission = mission
	case RecordTypeStmt:
		globalMission.RecordStmtLock.Lock()
		defer globalMission.RecordStmtLock.Unlock()
		globalMission.RecordStmtMission = mission
	}
}

func (c *RecordMission) isRunning() bool {
	c.runningLock.RLock()
	defer c.runningLock.RUnlock()
	return c.running
}

func (c *RecordMission) setRunning(running bool) {
	c.runningLock.Lock()
	defer c.runningLock.Unlock()
	c.running = running
}

func (c *RecordMission) start() {
	select {
	case <-c.startChan:
		// already started
		return
	default:
	}
	defer func() {
		close(c.startChan)
	}()
	duration := time.Until(c.startTime)
	c.logger.Infof("start recording after %s", duration.String())
	timer := time.NewTimer(duration)
	defer func() {
		timer.Stop()
	}()
	select {
	case <-timer.C:
		c.setRunning(true)
		go c.run(DefaultFlushInterval)
		return
	case <-c.ctx.Done():
		return
	}
}

func (c *RecordMission) run(flushInterval time.Duration) {
	c.logger.Infof("start recording sql")
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			c.Stop()
		case <-ticker.C:
			if c.writeLock.TryLock() {
				c.csvWriter.Flush()
				c.writeLock.Unlock()
			}
		}
	}
}

func (c *RecordMission) Stop() {
	c.closeOnce.Do(func() {
		c.logger.Infof("stop recording sql")
		// set flag false to stop
		c.setRunning(false)
		// cancel the context to stop the goroutine
		c.cancelFunc()
		// wait for the start to complete
		<-c.startChan
		// write all remaining records to the csv file
		switch c.recordType {
		case RecordTypeSQL:
			records := c.recordList.RemoveAllSqlRecords()
			for i := 0; i < len(records); i++ {
				c.writeSqlRecord(records[i])
			}
		case RecordTypeStmt:
			records := c.recordList.RemoveAllStmtRecords()
			for i := 0; i < len(records); i++ {
				c.writeStmtRecord(records[i])
			}
		default:
			c.logger.Errorf("unknown record type: %d", c.recordType)
		}
		// flush csv
		c.csvWriter.Flush()
		setMission(c.recordType, nil)
	})
}

func (c *RecordMission) writeSqlRecord(record *SQLRecord) {
	logger.Tracef("write record: %s", record.SQL)
	c.currentCount.Add(-1)
	record.Lock()
	defer record.Unlock()
	if len(record.SQL) == 0 {
		logger.Warn("record SQL is empty, skip writing")
		return
	}
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	row := record.toRow()
	c.logger.Tracef("writing record to csv: %s", row)
	err := c.csvWriter.Write(row)
	if err != nil {
		c.logger.Errorf("error writing record to csv: %s, data: %s", err, row)
	}
}

func (c *RecordMission) writeStmtRecord(record *StmtRecord) {
	logger.Tracef("write stmt record: %d", record.StmtPointer)
	c.currentCount.Add(-1)
	record.Lock()
	defer record.Unlock()
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	row := record.toRow()
	c.logger.Tracef("writing record to csv: %s", row)
	err := c.csvWriter.Write(row)
	if err != nil {
		c.logger.Errorf("error writing record to csv: %s, data: %s", err, row)
	}
}

func GetSqlMissionState() RecordState {
	return getMissionState(RecordTypeSQL)
}

func GetStmtMissionState() RecordState {
	return getMissionState(RecordTypeStmt)
}

func getMissionState(recordType RecordType) RecordState {
	mission := getMission(recordType)
	if mission == nil {
		return RecordState{
			Exists: false,
		}
	}
	mission.runningLock.RLock()
	defer mission.runningLock.RUnlock()
	state := RecordState{
		Exists:            true,
		Running:           mission.running,
		StartTime:         mission.startTime.Format(InputTimeFormat),
		EndTime:           mission.endTime.Format(InputTimeFormat),
		CurrentConcurrent: mission.currentCount.Load(),
	}
	return state
}

func Init() error {
	var errs []error
	if config.Conf.Log.EnableSqlToCsvLogging {
		err := StartRecordSql(time.Now().Format(InputTimeFormat), DefaultRecordSqlEndTime, "")
		if err != nil {
			errs = append(errs, err)
		}
	}
	if config.Conf.Log.EnableStmtToCsvLogging {
		err := StartRecordStmt(time.Now().Format(InputTimeFormat), DefaultRecordSqlEndTime, "")
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func Close() {
	// close the global record mission and writer
	for _, recordType := range recordTypes {
		mission := getMission(recordType)
		if mission != nil {
			mission.Stop()
		}
		setMission(recordType, nil)
	}
	// close the global rotate writer if it exists
	if globalSQLRotateWriter != nil {
		_ = globalSQLRotateWriter.Close()
	}
	if globalStmtRotateWriter != nil {
		_ = globalStmtRotateWriter.Close()
	}
}
