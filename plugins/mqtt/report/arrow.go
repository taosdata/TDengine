package report

import (
	"fmt"
	"math"
	"net"
	"time"

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
	reader    *ipc.Reader
	address   *net.TCPAddr
	id        int
}

var meta = arrow.MetadataFrom(map[string]string{
	"version": "1.0",
	"stream":  "flat",
	"ack":     "lush",
})

func NewArrowReporter(remote string, id int) (*ArrowReporter, error) {
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
			{Name: "payload", Type: arrow.BinaryTypes.String},
		},
		&meta,
	)
	logger := log.GetLogger("arrow").WithField("address", conn.LocalAddr().String()).WithField("remote", conn.RemoteAddr().String()).WithField("reporter_id", id)
	logger.Debugln("create ipc writer")
	writer := ipc.NewWriter(conn, ipc.WithSchema(schema))

	logger.Debugln("create ipc reader")
	reader, err := ipc.NewReader(conn, ipc.WithDelayReadSchema(true))
	if err != nil {
		log.GetLogger("arrow").Errorf("create ipc reader error %s", err)
		return nil, fmt.Errorf("create ipc reader error %s", err)
	}
	return &ArrowReporter{
		allocator: memory.NewGoAllocator(),
		writer:    writer,
		reader:    reader,
		conn:      conn,
		schema:    schema,
		address:   address,
		logger:    logger,
		id:        id,
	}, nil
}

func (r *ArrowReporter) Report(list []*Message) error {
	r.logger.Debugf("report count %d", len(list))
	recordBuilder := array.NewRecordBuilder(r.allocator, r.schema)
	defer recordBuilder.Release()
	tsField := recordBuilder.Field(0).(*array.TimestampBuilder)
	defer tsField.Release()
	topicField := recordBuilder.Field(1).(*array.StringBuilder)
	defer topicField.Release()
	qosField := recordBuilder.Field(2).(*array.Uint8Builder)
	defer qosField.Release()
	payloadField := recordBuilder.Field(3).(*array.StringBuilder)
	defer payloadField.Release()
	total := 0
	for i := 0; i < len(list); i++ {
		if len(list[i].Payload) > math.MaxInt32 {
			r.logger.Errorf("payload length %d larger than max Int32\n", len(list[i].Payload))
			return fmt.Errorf("payload length %d larger than max Int32", len(list[i].Payload))
		}
		total += len(list[i].Payload)
		if total >= math.MaxInt32 {
			r.logger.Warnf("payload total length %d larger than max Int32\n", total)
			err := r.Write(recordBuilder)
			if err != nil {
				return err
			}
			total = len(list[i].Payload)
		}
		tsField.Append(arrow.Timestamp(list[i].TS))
		topicField.Append(list[i].Topic)
		qosField.Append(list[i].Qos)
		payloadField.BinaryBuilder.Append(list[i].Payload)
	}
	err := r.Write(recordBuilder)
	if err != nil {
		return err
	}
	ack, err := r.reader.Read()
	if ack == nil {
		return nil
	}
	defer ack.Release()
	r.logger.Tracef("ack received (%d, %d)", ack.NumRows(), ack.NumCols())
	if ack.Column(0).(*array.Int32).Value(0) != 0 {
		msg := string(ack.Column(1).(*array.Binary).Value(0))
		r.logger.Warnf("ack error %s", msg)
		return fmt.Errorf("ack error %s", msg)
	}
	return err
}

func (r *ArrowReporter) Write(recordBuilder *array.RecordBuilder) error {
	record := recordBuilder.NewRecord()
	defer record.Release()
	start := time.Now()
	defer func() {
		end := time.Now()
		if end.Sub(start) > time.Second {
			r.logger.Warnf("write %d rows cost %s", record.NumRows(), end.Sub(start))
		}
	}()
	if record.NumRows() > 0 {
		r.logger.Debugf("write %d rows", record.NumRows())
		return r.writer.Write(record)
	}
	return nil
}

func (r *ArrowReporter) Close() {
	r.writer.Close()
	r.conn.Close()
}
