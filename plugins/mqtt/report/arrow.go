package report

import (
	"fmt"
	"net"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosx/plugins/mqtt/log"
)

type ArrowReporter struct {
	logger    *logrus.Entry
	allocator memory.Allocator
	conn      *net.TCPConn
	schema    *arrow.Schema
	writer    *ipc.Writer
	address   *net.TCPAddr
}

var meta = arrow.MetadataFrom(map[string]string{
	"version": "1.0",
	"stream":  "flat",
	"ack":     "none",
})

func NewArrowReporter(remote string) (*ArrowReporter, error) {
	address, err := net.ResolveTCPAddr("tcp", remote)
	if err != nil {
		return nil, fmt.Errorf("resolve remote address error %s", err)
	}
	conn, err := net.DialTCP("tcp", nil, address)
	if err != nil {
		return nil, fmt.Errorf("conn to remote tcp error %s", err)
	}
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},
			{Name: "topic", Type: arrow.BinaryTypes.String},
			{Name: "qos", Type: &arrow.Uint8Type{}},
			{Name: "payload", Type: arrow.BinaryTypes.Binary},
		},
		&meta,
	)
	writer := ipc.NewWriter(conn, ipc.WithSchema(schema))
	return &ArrowReporter{
		allocator: memory.NewGoAllocator(),
		writer:    writer,
		conn:      conn,
		schema:    schema,
		address:   address,
		logger:    log.GetLogger("arrow").WithField("address", conn.LocalAddr().String()),
	}, nil
}

func (r *ArrowReporter) Report(list []*Message) error {
	r.logger.Debugf("report %#v", list)
	recordBuilder := array.NewRecordBuilder(r.allocator, r.schema)
	defer recordBuilder.Release()
	tsField := recordBuilder.Field(0).(*array.TimestampBuilder)
	defer tsField.Release()
	topicField := recordBuilder.Field(1).(*array.StringBuilder)
	defer topicField.Release()
	qosField := recordBuilder.Field(2).(*array.Uint8Builder)
	defer qosField.Release()
	payloadField := recordBuilder.Field(3).(*array.BinaryBuilder)
	defer payloadField.Release()
	for i := 0; i < len(list); i++ {
		tsField.Append(arrow.Timestamp(list[i].TS))
		topicField.Append(list[i].Topic)
		qosField.Append(list[i].Qos)
		payloadField.Append(list[i].Payload)
	}
	record := recordBuilder.NewRecord()
	defer record.Release()
	r.logger.Debugf("report data%#v:", list)
	return r.writer.Write(record)
}

func (r *ArrowReporter) Close() {
	r.writer.Close()
	r.conn.Close()
}
