package log

import (
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
)

type DataDump struct {
	rotator    *rotatelogs.RotateLogs
	ch         chan []string
	done       chan struct{}
	writerDone chan struct{}
	logger     *logrus.Entry
}

func NewDataDump(dumpPath string, keep int64) (*DataDump, error) {
	err := makeDirIfNotExist(dumpPath)
	if err != nil {
		return nil, err
	}
	filePath := path.Join(dumpPath, "mqtt.dump.%Y%m%d%H%M")
	rotate, err := rotatelogs.New(filePath,
		rotatelogs.WithMaxAge(time.Duration(keep)*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour))
	if err != nil {
		return nil, fmt.Errorf("failed to create dump file: %w", err)
	}
	dumper := DataDump{
		rotator:    rotate,
		ch:         make(chan []string, 100),
		done:       make(chan struct{}),
		writerDone: make(chan struct{}),
		logger:     logger.WithField("module", "dump"),
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
		ticker := time.NewTicker(time.Second * 1)
		defer ticker.Stop()
		for {
			select {
			case <-c.done:
				return
			case item := <-c.ch:
				if err := writer.Write(item); err != nil {
					c.logger.Errorf("failed to write csv: %v", err)
				}
			case <-ticker.C:
				writer.Flush()
			}
		}
	}()
}

const TimeFormat = "01/02 15:04:05.000"

func (c *DataDump) Dump(ts time.Time, qos byte, topic string, payload []byte) {
	defer func() {
		// avoid close channel panic
		recover()
	}()
	item := []string{
		ts.Format(TimeFormat),             // ts
		strconv.FormatInt(int64(qos), 10), // qos
		topic,                             // topic
		string(payload),                   // payload
	}
	if len(c.ch) == cap(c.ch) {
		c.logger.Warn("dump channel is full")
	}
	c.ch <- item
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
