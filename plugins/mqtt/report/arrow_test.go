package report

import (
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func TestArrowReporter(t *testing.T) {
	// Mock TCP server
	server, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Error(err)
	}
	defer server.Close()

	// Start reporter
	reporter, err := NewArrowReporter(server.Addr().String(), 0)
	if err != nil {
		t.Error(err)
	}
	defer reporter.conn.Close()

	// Check schema
	if len(reporter.schema.Fields()) != 4 {
		t.Errorf("expected 4 fields, got %d", len(reporter.schema.Fields()))
	}

	// Generate test data
	recordBuilder := array.NewRecordBuilder(reporter.allocator, reporter.schema)
	defer recordBuilder.Release()
	tsField := recordBuilder.Field(0).(*array.TimestampBuilder)
	defer tsField.Release()
	topicField := recordBuilder.Field(1).(*array.StringBuilder)
	defer topicField.Release()
	qosField := recordBuilder.Field(2).(*array.Uint8Builder)
	defer qosField.Release()
	payloadField := recordBuilder.Field(3).(*array.StringBuilder)
	defer payloadField.Release()
	tsField.Append(arrow.Timestamp(10))
	topicField.Append("test")
	qosField.Append(1)
	payloadField.Append("payload")
	record := recordBuilder.NewRecord()
	defer record.Release()
	assert.NoError(t, reporter.writer.Write(record))

	// Read test data from TCP server
	conn, err := server.AcceptTCP()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	reader, err := ipc.NewReader(conn)
	assert.NoError(t, err)
	defer reader.Release()
	meta := reader.Schema()
	assert.True(t, reporter.schema.Equal(meta))
	if reader.Next() {
		r := reader.Record()
		assert.True(t, array.RecordEqual(record, r))
		r.Release()
	} else {
		t.Error("expect get record")
	}
	reporter.Close()
}

type mockMessage struct {
	topic   string
	qos     byte
	payload []byte
}

func (m *mockMessage) Duplicate() bool {
	return false
}

func (m *mockMessage) Qos() byte {
	return m.qos
}

func (m *mockMessage) Retained() bool {
	return false
}

func (m *mockMessage) Topic() string {
	return m.topic
}

func (m *mockMessage) MessageID() uint16 {
	return 0
}

func (m *mockMessage) Payload() []byte {
	return m.payload
}

func (m *mockMessage) Ack() {
	return
}

func TestReport(t *testing.T) {
	// Mock TCP server
	server, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Error(err)
	}
	defer server.Close()

	go accept(t, server, make(chan struct{}))

	// Start reporter
	reporter, err := NewArrowReporter(server.Addr().String(), 0)
	if err != nil {
		t.Error(err)
	}
	defer reporter.conn.Close()

	// Generate test data
	timestamp := int64(1234567890)
	topic := "testTopic"
	qos := byte(2)
	payload := []byte("testPayload")
	recordBuilder := array.NewRecordBuilder(reporter.allocator, reporter.schema)
	defer recordBuilder.Release()
	tsField := recordBuilder.Field(0).(*array.TimestampBuilder)
	defer tsField.Release()
	topicField := recordBuilder.Field(1).(*array.StringBuilder)
	defer topicField.Release()
	qosField := recordBuilder.Field(2).(*array.Uint8Builder)
	defer qosField.Release()
	payloadField := recordBuilder.Field(3).(*array.StringBuilder)
	defer payloadField.Release()
	tsField.Append(arrow.Timestamp(timestamp))
	topicField.Append(topic)
	qosField.Append(qos)
	payloadField.BinaryBuilder.Append(payload)
	record := recordBuilder.NewRecord()
	defer record.Release()

	// Write test data using Report()
	err = reporter.Report([]*Message{{
		TS:      timestamp,
		Topic:   topic,
		Qos:     qos,
		Payload: payload,
	}})
	if err != nil {
		t.Error(err)
	}

	reporter.Close()
}

func accept(t *testing.T, server *net.TCPListener, finish chan struct{}) {
	defer func() {
		finish <- struct{}{}
		time.Sleep(time.Second * 5)
	}()
	conn, err := server.AcceptTCP()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()
	reader, err := ipc.NewReader(conn)
	assert.NoError(t, err)
	defer reader.Release()
	meta := reader.Schema()
	metadata := arrow.MetadataFrom(map[string]string{
		"version": "1.0",
		"stream":  "flat",
		"ack":     "ack",
	})
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Millisecond}},
			{Name: "topic", Type: arrow.BinaryTypes.String},
			{Name: "qos", Type: &arrow.Uint8Type{}},
			{Name: "payload", Type: arrow.BinaryTypes.String},
		},
		&metadata,
	)
	assert.True(t, schema.Equal(meta))

	ackSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "code", Type: &arrow.Int32Type{}},
			{Name: "message", Type: arrow.BinaryTypes.String},
			{Name: "context", Type: arrow.BinaryTypes.Binary},
		},
		nil,
	)
	allocator := memory.NewGoAllocator()
	writer := ipc.NewWriter(conn, ipc.WithSchema(ackSchema))
	defer writer.Close()
	if reader.Next() {
		r := reader.Record()
		assert.Equal(t, "testTopic", r.Column(1).(*array.String).Value(0))
		assert.Equal(t, uint8(2), r.Column(2).(*array.Uint8).Value(0))
		assert.Equal(t, "testPayload", r.Column(3).(*array.String).Value(0))
		t.Log("record check pass")
		r.Release()

		recordBuilder := array.NewRecordBuilder(allocator, ackSchema)
		codeBuilder := recordBuilder.Field(0).(*array.Int32Builder)
		defer codeBuilder.Release()
		codeBuilder.Append(0)

		messageBuilder := recordBuilder.Field(0).(*array.BinaryBuilder)
		defer messageBuilder.Release()
		messageBuilder.Append([]byte("OK"))

		contextBuilder := recordBuilder.Field(0).(*array.BinaryBuilder)
		defer contextBuilder.Release()
		contextBuilder.Append([]byte("context data"))

		writer.Write(recordBuilder.NewRecord())
	} else {
		t.Error("expect get data")
	}
}
