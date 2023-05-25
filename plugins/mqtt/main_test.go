package main

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
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
`

func Test_All(t *testing.T) {
	server, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Error(err)
	}
	defer server.Close()
	finish := make(chan struct{})
	go accept(t, server, finish)
	configStr := fmt.Sprintf(testConfig, server.Addr().String(), "3.0")
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
		"ack":     "none",
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
	if reader.Next() {
		r := reader.Record()
		assert.Equal(t, "topic1", r.Column(1).(*array.String).Value(0))
		assert.Equal(t, uint8(0), r.Column(2).(*array.Uint8).Value(0))
		assert.Equal(t, "value1", r.Column(3).(*array.String).Value(0))
		t.Log("record check pass")
		r.Release()
	} else {
		t.Error("expect get data")
	}
}
