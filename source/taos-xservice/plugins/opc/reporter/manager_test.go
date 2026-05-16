package reporter

import (
	"collector/common"
	"collector/config"
	"collector/types"
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	server, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go handleServer(t, server)
	addr := server.Addr().String()
	reportConfig := config.ReportConfig{
		Remote:       addr,
		Concurrent:   1,
		BatchSize:    1,
		BatchTimeout: 1,
	}
	manager := NewManager(ctx, reportConfig)
	reporter, err := manager.GetReporter("test_id", types.UINT32)
	assert.NoError(t, err)
	assert.NotNil(t, reporter)
	reporter.Report([]*common.NodeValue{
		{
			IDStr:      "test_id",
			Name:       "test",
			Timestamp:  time.Unix(1700791658, 0),
			StartTime:  time.Unix(1700791658, 0),
			FinishTime: time.Unix(1700791658, 0),
			Value:      uint32(32),
			ValueType:  types.UINT32,
			Status:     int64(ua.StatusOK),
		},
	})
	time.Sleep(time.Second * 2)
	manager.Close()
	cancel()
	time.Sleep(time.Millisecond * 100)
}

func handleServer(t *testing.T, server *net.TCPListener) {
	for {
		// Read test data from TCP server
		conn, err := server.AcceptTCP()
		if err != nil {
			return
		}
		go handle(t, conn)
	}
}

func handle(t *testing.T, conn *net.TCPConn) {
	defer conn.Close()
	reader, err := ipc.NewReader(conn)
	if err != nil {
		t.Error(err)
		return
	}
	defer reader.Release()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "code", Type: arrow.BinaryTypes.String},
			{Name: "message", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
	writer := ipc.NewWriter(conn, ipc.WithSchema(schema))
	defer writer.Close()
	recordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer recordBuilder.Release()

	field0 := recordBuilder.Field(0).(*array.StringBuilder)
	field1 := recordBuilder.Field(1).(*array.StringBuilder)
	field0.Append("0")
	field1.Append("")
	record := recordBuilder.NewRecord()
	defer record.Release()
	for {
		if reader.Next() {
			r := reader.Record()
			r.Release()
			err = writer.Write(record)
			if err != nil {
				t.Error(err)
			}
		} else {
			break
		}
	}
}
