package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
)

var testConfig = `
log_level = "debug"
remote = "%s"

[mqtt]
address = "tcp://127.0.0.1:1883"
version = "%s"
client_id = "mqtt_test_all"
username = "user"
password = "pass"
keep_alive = 60
clean_session = true

[topics]
"topic1" = 0

[dump]
enable = true
path = "%s"
keep = 7
`

func Test_All(t *testing.T) {
	server, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Error(err)
	}
	defer server.Close()
	finish := make(chan struct{})
	go accept(t, server, finish)
	tmpDir, err := os.MkdirTemp("", "*")
	tmpDir = strings.ReplaceAll(tmpDir, "\\", "\\\\")
	configStr := fmt.Sprintf(testConfig, server.Addr().String(), "3.0", tmpDir)
	t.Log(configStr)

	f, err := os.CreateTemp("", "*")
	assert.NoError(t, err)
	_, err = f.Write([]byte(configStr))
	assert.NoError(t, err)
	_ = f.Close()
	defer os.Remove(f.Name())
	os.Args = append(os.Args, "-c", f.Name())
	go main()
	time.Sleep(time.Second)
	opt := mqtt.NewClientOptions()
	opt.AddBroker("tcp://127.0.0.1:1883")
	connected := make(chan struct{})
	opt.SetOnConnectHandler(func(client mqtt.Client) {
		connected <- struct{}{}
	})

	mqttClient := mqtt.NewClient(opt)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Error on Client.Connect(): %v", token.Error())
	}
	<-connected
	err = mqttClient.Publish("topic1", 1, false, "value1").Error()
	assert.NoError(t, err)

	<-finish
	time.Sleep(time.Second)
	files, err := findFilesWithPrefix(tmpDir, "mqtt.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	assert.NoError(t, err)
	r := bufio.NewReader(bytes.NewReader(data))
	l, _, err := r.ReadLine()
	assert.NoError(t, err)
	assert.Contains(t, string(l), "0,topic1,value1")
	l, _, err = r.ReadLine()
	assert.ErrorIs(t, err, io.EOF)
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
		assert.Equal(t, "topic1", r.Column(1).(*array.String).Value(0))
		assert.Equal(t, uint8(0), r.Column(2).(*array.Uint8).Value(0))
		assert.Equal(t, "value1", r.Column(3).(*array.String).Value(0))
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

func findFilesWithPrefix(rootPath, prefix string) ([]string, error) {
	var matchingFiles []string

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			fileName := info.Name()
			if strings.HasPrefix(fileName, prefix) {
				matchingFiles = append(matchingFiles, path)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return matchingFiles, nil
}
