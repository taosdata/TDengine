package main

import (
	"fmt"
	"net"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

func main() {
	server, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6051})
	if err != nil {
		panic(err)
	}
	defer server.Close()
	for {
		// Read test data from TCP server
		conn, err := server.AcceptTCP()
		if err != nil {
			return
		}
		go handle(conn)
	}
}

func handle(conn *net.TCPConn) {
	defer conn.Close()
	reader, err := ipc.NewReader(conn)
	if err != nil {
		fmt.Println(err)
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
			fmt.Println(time.Now(), r.NumRows())
			r.Release()
			err = writer.Write(record)
			if err != nil {
				fmt.Println(err)
				return
			}
		} else {
			break
		}
	}
}
