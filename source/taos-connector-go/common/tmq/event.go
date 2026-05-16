package tmq

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	taosError "github.com/taosdata/driver-go/v3/errors"
)

type Data struct {
	TableName string
	Data      [][]driver.Value
}
type Event interface {
	String() string
}

type Error struct {
	code int
	str  string
}

const ErrorOther = 0xffff

func NewTMQError(code int, str string) Error {
	return Error{
		code: code,
		str:  str,
	}
}

func NewTMQErrorWithErr(err error) Error {
	tErr, ok := err.(*taosError.TaosError)
	if ok {
		return Error{
			code: int(tErr.Code),
			str:  tErr.ErrStr,
		}
	}
	return Error{
		code: ErrorOther,
		str:  err.Error(),
	}
}

func (e Error) String() string {
	return fmt.Sprintf("[0x%x] %s", e.code, e.str)
}

func (e Error) Error() string {
	return e.String()
}

func (e Error) Code() int {
	return e.code
}

type Message interface {
	Topic() string
	DBName() string
	Value() interface{}
	Offset() int64
}

type DataMessage struct {
	TopicPartition TopicPartition
	dbName         string
	topic          string
	data           []*Data
	offset         Offset
}

func (m *DataMessage) String() string {
	data, _ := json.Marshal(m.data)
	return fmt.Sprintf("DataMessage: %s[%s]:%s", m.topic, m.dbName, string(data))
}

func (m *DataMessage) SetDbName(dbName string) {
	m.dbName = dbName
}

func (m *DataMessage) SetTopic(topic string) {
	m.topic = topic
}

func (m *DataMessage) SetData(data []*Data) {
	m.data = data
}

func (m *DataMessage) SetOffset(offset Offset) {
	m.offset = offset
}

func (m *DataMessage) Topic() string {
	return m.topic
}

func (m *DataMessage) DBName() string {
	return m.dbName
}

func (m *DataMessage) Value() interface{} {
	return m.data
}

func (m *DataMessage) Offset() Offset {
	return m.offset
}

type MetaMessage struct {
	TopicPartition TopicPartition
	dbName         string
	topic          string
	offset         Offset
	meta           *Meta
}

func (m *MetaMessage) Offset() Offset {
	return m.offset
}

func (m *MetaMessage) String() string {
	data, _ := json.Marshal(m.meta)
	return fmt.Sprintf("MetaMessage: %s[%s]:%s", m.topic, m.dbName, string(data))
}

func (m *MetaMessage) SetDbName(dbName string) {
	m.dbName = dbName
}

func (m *MetaMessage) SetTopic(topic string) {
	m.topic = topic
}

func (m *MetaMessage) SetOffset(offset Offset) {
	m.offset = offset
}

func (m *MetaMessage) SetMeta(meta *Meta) {
	m.meta = meta
}

func (m *MetaMessage) Topic() string {
	return m.topic
}

func (m *MetaMessage) DBName() string {
	return m.dbName
}

func (m *MetaMessage) Value() interface{} {
	return m.meta
}

type MetaDataMessage struct {
	TopicPartition TopicPartition
	dbName         string
	topic          string
	offset         Offset
	metaData       *MetaData
}

func (m *MetaDataMessage) Offset() Offset {
	return m.offset
}

func (m *MetaDataMessage) String() string {
	data, _ := json.Marshal(m.metaData)
	return fmt.Sprintf("MetaDataMessage: %s[%s]:%s", m.topic, m.dbName, string(data))
}

func (m *MetaDataMessage) SetDbName(dbName string) {
	m.dbName = dbName
}

func (m *MetaDataMessage) SetTopic(topic string) {
	m.topic = topic
}

func (m *MetaDataMessage) SetOffset(offset Offset) {
	m.offset = offset
}

func (m *MetaDataMessage) SetMetaData(metaData *MetaData) {
	m.metaData = metaData
}

type MetaData struct {
	Meta *Meta
	Data []*Data
}

func (m *MetaDataMessage) Topic() string {
	return m.topic
}

func (m *MetaDataMessage) DBName() string {
	return m.dbName
}

func (m *MetaDataMessage) Value() interface{} {
	return m.metaData
}
