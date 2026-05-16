package tmq

import (
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	taosError "github.com/taosdata/driver-go/v3/errors"
)

func TestDataMessage_String(t *testing.T) {
	t.Parallel()

	data := []*Data{
		{TableName: "table1", Data: [][]driver.Value{{1, "data1"}}},
		{TableName: "table2", Data: [][]driver.Value{{2, "data2"}}},
	}
	message := &DataMessage{
		TopicPartition: TopicPartition{
			Topic:     stringPtr("test-topic"),
			Partition: 0,
			Offset:    100,
		},
		dbName: "test-db",
		topic:  "test-topic",
		data:   data,
		offset: 100,
	}

	want := `DataMessage: test-topic[test-db]:[{"TableName":"table1","Data":[[1,"data1"]]},{"TableName":"table2","Data":[[2,"data2"]]}]`

	if got := message.String(); got != want {
		t.Errorf("DataMessage.String() = %v, want %v", got, want)
	}
}

func TestMetaMessage_String(t *testing.T) {
	t.Parallel()

	meta := &Meta{
		Type:      "type",
		TableName: "table",
		TableType: "tableType",
	}
	message := &MetaMessage{
		TopicPartition: TopicPartition{
			Topic:     stringPtr("test-topic"),
			Partition: 0,
			Offset:    100,
		},
		dbName: "test-db",
		topic:  "test-topic",
		offset: 100,
		meta:   meta,
	}

	want := `MetaMessage: test-topic[test-db]:{"type":"type","tableName":"table","tableType":"tableType","createList":null,"columns":null,"using":"","tagNum":0,"tags":null,"tableNameList":null,"alterType":0,"colName":"","colNewName":"","colType":0,"colLength":0,"colValue":"","colValueNull":false}`

	if got := message.String(); got != want {
		t.Errorf("MetaMessage.String() = %v, want %v", got, want)
	}
}

func TestMetaDataMessage_String(t *testing.T) {
	t.Parallel()

	meta := &Meta{
		Type:      "type",
		TableName: "table",
		TableType: "tableType",
	}
	data := []*Data{
		{TableName: "table1", Data: [][]driver.Value{{1, "data1"}}},
		{TableName: "table2", Data: [][]driver.Value{{2, "data2"}}},
	}
	metaData := &MetaData{
		Meta: meta,
		Data: data,
	}
	message := &MetaDataMessage{
		TopicPartition: TopicPartition{
			Topic:     stringPtr("test-topic"),
			Partition: 0,
			Offset:    100,
		},
		dbName:   "test-db",
		topic:    "test-topic",
		offset:   100,
		metaData: metaData,
	}

	want := `MetaDataMessage: test-topic[test-db]:{"Meta":{"type":"type","tableName":"table","tableType":"tableType","createList":null,"columns":null,"using":"","tagNum":0,"tags":null,"tableNameList":null,"alterType":0,"colName":"","colNewName":"","colType":0,"colLength":0,"colValue":"","colValueNull":false},"Data":[{"TableName":"table1","Data":[[1,"data1"]]},{"TableName":"table2","Data":[[2,"data2"]]}]}`
	if got := message.String(); got != want {
		t.Errorf("MetaDataMessage.String() = %v, want %v", got, want)
	}
}

func TestNewTMQError(t *testing.T) {
	t.Parallel()

	code := 123
	str := "test error"
	err := NewTMQError(code, str)

	if err.code != code {
		t.Errorf("NewTMQError() code = %v, want %v", err.code, code)
	}

	if err.str != str {
		t.Errorf("NewTMQError() str = %v, want %v", err.str, str)
	}
}

func TestNewTMQErrorWithErr(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name string
		err  error
		code int
		str  string
	}{
		{
			name: "TaosError",
			err: &taosError.TaosError{
				Code:   456,
				ErrStr: "taos error",
			},
			code: 456,
			str:  "taos error",
		},
		{
			name: "OtherError",
			err:  fmt.Errorf("other error"),
			code: ErrorOther,
			str:  "other error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewTMQErrorWithErr(tc.err)

			if err.code != tc.code {
				t.Errorf("NewTMQErrorWithErr() code = %v, want %v", err.code, tc.code)
			}

			if err.str != tc.str {
				t.Errorf("NewTMQErrorWithErr() str = %v, want %v", err.str, tc.str)
			}
		})
	}
}

func TestError_String(t *testing.T) {
	t.Parallel()

	code := 789
	str := "test error"
	err := Error{code: code, str: str}
	want := fmt.Sprintf("[0x%x] %s", code, str)

	if got := err.String(); got != want {
		t.Errorf("Error.String() = %v, want %v", got, want)
	}
}

func TestError_Error(t *testing.T) {
	t.Parallel()

	code := 789
	str := "test error"
	err := Error{code: code, str: str}
	want := fmt.Sprintf("[0x%x] %s", code, str)

	if got := err.Error(); got != want {
		t.Errorf("Error.Error() = %v, want %v", got, want)
	}
}

func TestError_Code(t *testing.T) {
	t.Parallel()

	code := 789
	err := Error{code: code}

	if got := err.Code(); got != code {
		t.Errorf("Error.Code() = %v, want %v", got, code)
	}
}

func TestMetaMessage_Offset(t *testing.T) {
	t.Parallel()

	message := &MetaMessage{
		offset: 100,
	}

	want := Offset(100)
	if got := message.Offset(); got != want {
		t.Errorf("Offset() = %v, want %v", got, want)
	}
}

func TestMetaMessage_SetDbName(t *testing.T) {
	t.Parallel()

	message := &MetaMessage{}
	message.SetDbName("test-db")

	want := "test-db"
	if got := message.DBName(); got != want {
		t.Errorf("DBName() = %v, want %v", got, want)
	}
}

func TestMetaMessage_SetTopic(t *testing.T) {
	t.Parallel()

	message := &MetaMessage{}
	message.SetTopic("test-topic")

	want := "test-topic"
	if got := message.Topic(); got != want {
		t.Errorf("Topic() = %v, want %v", got, want)
	}
}

func TestMetaMessage_SetOffset(t *testing.T) {
	t.Parallel()

	message := &MetaMessage{}
	message.SetOffset(200)

	want := Offset(200)
	if got := message.Offset(); got != want {
		t.Errorf("Offset() = %v, want %v", got, want)
	}
}

func TestMetaMessage_SetMeta(t *testing.T) {
	t.Parallel()

	meta := &Meta{}
	message := &MetaMessage{}
	message.SetMeta(meta)

	want := meta
	if got := message.Value(); got != want {
		t.Errorf("Value() = %v, want %v", got, want)
	}
}

func TestDataMessage_SetDbName(t *testing.T) {
	t.Parallel()

	message := &DataMessage{}
	message.SetDbName("test-db")

	want := "test-db"
	if got := message.DBName(); got != want {
		t.Errorf("DBName() = %v, want %v", got, want)
	}
}

func TestDataMessage_SetTopic(t *testing.T) {
	t.Parallel()

	message := &DataMessage{}
	message.SetTopic("test-topic")

	want := "test-topic"
	if got := message.Topic(); got != want {
		t.Errorf("Topic() = %v, want %v", got, want)
	}
}

func TestDataMessage_SetData(t *testing.T) {
	t.Parallel()

	data := []*Data{
		{TableName: "table1", Data: [][]driver.Value{{1, "data1"}}},
		{TableName: "table2", Data: [][]driver.Value{{2, "data2"}}},
	}
	message := &DataMessage{}
	message.SetData(data)

	want := data
	assert.Equal(t, want, message.Value())
}

func TestDataMessage_SetOffset(t *testing.T) {
	t.Parallel()

	message := &DataMessage{}
	message.SetOffset(200)

	want := Offset(200)
	if got := message.Offset(); got != want {
		t.Errorf("Offset() = %v, want %v", got, want)
	}
}

func TestMetaDataMessage_SetDbName(t *testing.T) {
	t.Parallel()

	message := &MetaDataMessage{}
	message.SetDbName("test-db")

	want := "test-db"
	if got := message.DBName(); got != want {
		t.Errorf("DBName() = %v, want %v", got, want)
	}
}

func TestMetaDataMessage_SetTopic(t *testing.T) {
	t.Parallel()

	message := &MetaDataMessage{}
	message.SetTopic("test-topic")

	want := "test-topic"
	if got := message.Topic(); got != want {
		t.Errorf("Topic() = %v, want %v", got, want)
	}
}

func TestMetaDataMessage_SetOffset(t *testing.T) {
	t.Parallel()

	message := &MetaDataMessage{}
	message.SetOffset(200)

	want := Offset(200)
	if got := message.Offset(); got != want {
		t.Errorf("Offset() = %v, want %v", got, want)
	}
}

func TestMetaDataMessage_SetMetaData(t *testing.T) {
	t.Parallel()

	metaData := &MetaData{}
	message := &MetaDataMessage{}
	message.SetMetaData(metaData)

	want := metaData
	if got := message.Value(); got != want {
		t.Errorf("Value() = %v, want %v", got, want)
	}
}
