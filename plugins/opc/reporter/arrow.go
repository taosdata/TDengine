package reporter

import (
	"collector/buffer"
	"collector/common"
	"collector/log"
	"collector/types"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/sirupsen/logrus"
)

type ArrowReporter struct {
	messageList  *buffer.MessageList
	writer       *ipc.Writer
	reader       *ipc.Reader
	appendFunc   types.AppendFunc
	logger       *logrus.Entry
	conn         *net.TCPConn
	address      *net.TCPAddr
	schema       *arrow.Schema
	allocator    memory.Allocator
	batchTimeout time.Duration
	ctx          context.Context
}

func NewArrowReporter(ctx context.Context, id int, remote string, t types.ValueType, batchSize int, batchTimeout time.Duration) (*ArrowReporter, error) {
	reportType, exists := types.ReporterTypeMap[t]
	if !exists {
		return nil, fmt.Errorf("unsupported type %d", t)
	}
	address, err := net.ResolveTCPAddr("tcp", remote)
	if err != nil {
		return nil, fmt.Errorf("resolve remote address error %s", err)
	}
	conn, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		return nil, fmt.Errorf("conn to remote tcp error %s", err)
	}
	schema := reportType.Schema
	writer := ipc.NewWriter(conn, ipc.WithSchema(schema))
	reader, err := ipc.NewReader(conn, ipc.WithDelayReadSchema(true))
	if err != nil {
		conn.Close()
		return nil, err
	}
	logger := log.GetLogger("arrow").WithField("type", t.String()).WithField("id", id)
	return &ArrowReporter{
		messageList:  buffer.NewMessageList(ctx, batchSize, batchTimeout, logger),
		allocator:    memory.NewGoAllocator(),
		writer:       writer,
		appendFunc:   reportType.AppendFunc,
		reader:       reader,
		conn:         conn,
		schema:       schema,
		address:      address,
		logger:       logger,
		batchTimeout: batchTimeout,
		ctx:          ctx,
	}, nil
}

func (r *ArrowReporter) startReceiveMessage() {
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				r.logger.Info("arrow reporter exit")
				return
			case list := <-r.messageList.C:
				r.logger.Debugf("receive message list data count %d", len(list))
				err := r.upload(list)
				if err != nil {
					r.logger.Errorf("upload error %s", err)
				}
			}
		}
	}()
}

func (r *ArrowReporter) upload(list []*common.NodeValue) error {
	defer r.messageList.TryGet()
	defer func() {
		for _, value := range list {
			common.PutNodeValue(value)
		}
	}()
	r.logger.Debugf("upload data count %d", len(list))
	recordBuilder := array.NewRecordBuilder(r.allocator, r.schema)
	defer recordBuilder.Release()
	idField := recordBuilder.Field(0).(*array.StringBuilder) // id
	defer idField.Release()
	nameField := recordBuilder.Field(1).(*array.StringBuilder) // name
	defer nameField.Release()
	tsField := recordBuilder.Field(2).(*array.TimestampBuilder) // ts
	defer tsField.Release()
	clientTsField := recordBuilder.Field(3).(*array.TimestampBuilder) // now
	defer clientTsField.Release()
	valueField := recordBuilder.Field(4) // value
	defer valueField.Release()
	statusField := recordBuilder.Field(5).(*array.Int64Builder) // status
	defer statusField.Release()
	requestField := recordBuilder.Field(6).(*array.TimestampBuilder) // request time
	defer requestField.Release()
	for _, msg := range list {
		idField.Append(msg.IDStr)
		nameField.Append(msg.Name)
		tsField.Append(arrow.Timestamp(msg.Timestamp.UnixMilli()))
		clientTsField.Append(arrow.Timestamp(msg.FinishTime.UnixMilli()))
		requestField.Append(arrow.Timestamp(msg.StartTime.UnixMilli()))
		if msg.Value == nil {
			valueField.AppendNull()
		} else {
			err := r.appendFunc(valueField, msg.Value)
			if err != nil {
				r.logger.WithError(err).WithField("identifier", msg.IDStr).Error("append value error")
				return err
			}
		}
		statusField.Append(msg.Status)
	}
	record := recordBuilder.NewRecord()
	defer record.Release()
	r.logger.Debugf("reported data count: %d", len(list))
	err := r.writer.Write(record)
	if err != nil {
		return err
	}
	start := time.Now()
	haveNext := r.reader.Next()
	if !haveNext {
		err = errors.New("ipc does not get ack response")
		r.logger.WithError(err).Error("get response")
		return err
	}
	responseRecord := r.reader.Record()
	defer responseRecord.Release()
	respTime := time.Since(start)
	r.logger.Debugf("read response time %dms", respTime.Milliseconds())
	if respTime > r.batchTimeout {
		r.logger.WithField("responseTime", respTime).WithField("batchTimeout", r.batchTimeout).Warn("read response time too long")
	}
	var code string
	var message string
	for i, col := range responseRecord.Columns() {
		if responseRecord.ColumnName(i) == "code" {
			code = col.ValueStr(0)
		}
		if responseRecord.ColumnName(i) == "message" {
			message = col.ValueStr(0)
		}
	}

	if code != "0" {
		err = fmt.Errorf("upload to taosx and ack error. code %s message %s ", code, message)
		r.logger.WithError(err).Error("upload to taosx and ack error")
	}

	return nil
}

func (r *ArrowReporter) Report(list []*common.NodeValue) {
	r.logger.Debug("report to message list")
	r.messageList.Add(list)
}

func (r *ArrowReporter) Close() {
	r.writer.Close()
	r.conn.Close()
}
