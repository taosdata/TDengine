package log

import (
	"collector/common"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/sirupsen/logrus"
	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
)

type DataDump struct {
	rotator    *rotatelogs.RotateLogs
	ch         chan [][]string
	done       chan struct{}
	writerDone chan struct{}
	isOPCUa    bool
	logger     *logrus.Entry
}

func NewDataDump(dumpPath string, keep int64, isOPCUa bool) (*DataDump, error) {
	err := makeDirIfNotExist(dumpPath)
	if err != nil {
		return nil, err
	}
	filePath := path.Join(dumpPath, "opc_data.dump.%Y%m%d%H%M")
	rotate, err := rotatelogs.New(filePath,
		rotatelogs.WithMaxAge(time.Duration(keep)*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour))
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file: %w", err)
	}
	dumper := DataDump{
		rotator:    rotate,
		ch:         make(chan [][]string, 100),
		done:       make(chan struct{}),
		writerDone: make(chan struct{}),
		logger:     logger.WithField("module", "dump"),
		isOPCUa:    isOPCUa,
	}
	dumper.startToDump()
	return &dumper, nil
}

func (c *DataDump) startToDump() {
	go func() {
		defer close(c.writerDone)
		writer := csv.NewWriter(c.rotator)
		defer writer.Flush()
		if err := writer.Error(); err != nil {
			c.logger.Errorf("failed to flush csv writer: %v", err)
		}

		for {
			select {
			case <-c.done:
				return
			case item := <-c.ch:
				if err := writer.WriteAll(item); err != nil {
					c.logger.Errorf("failed to write csv: %v", err)
				}
			}
		}
	}()
}

const TimeFormat = "01/02 15:04:05.000"

func (c *DataDump) Dump(values []*common.NodeValue) {
	if len(values) == 0 {
		return
	}
	defer func() {
		// avoid close channel panic
		recover()
	}()
	items := make([][]string, 0, len(values))
	for _, value := range values {
		status := ""
		if c.isOPCUa {
			status = ua.StatusCodes[ua.StatusCode(value.Status)].Name
		} else {
			status = strconv.FormatInt(value.Status, 10)
		}
		item := []string{
			value.IDStr,                                // id
			value.Name,                                 // name
			value.StartTime.Format(TimeFormat),         // start time
			value.FinishTime.Format(TimeFormat),        // finish time
			value.Timestamp.Local().Format(TimeFormat), // time from opc
			fmt.Sprintf("%v", value.Value),             // value
			value.ValueType.String(),                   // value type
			status,                                     // status
		}
		items = append(items, item)
	}
	if len(c.ch) == cap(c.ch) {
		c.logger.Warn("dump channel is full")
	}
	c.ch <- items
}

func (c *DataDump) Close() {
	c.logger.Info("close csv dumper")
	close(c.done)
	<-c.writerDone
	close(c.ch)
	_ = c.rotator.Close()
}

func makeDirIfNotExist(path string) error {
	file, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create path: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to check path: %w", err)
	}
	if !file.IsDir() {
		return fmt.Errorf("path exists and is not a directory")
	}
	return nil
}
