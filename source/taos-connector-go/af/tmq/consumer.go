package tmq

import (
	"database/sql/driver"
	"errors"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/parser"
	"github.com/taosdata/driver-go/v3/common/tmq"
	taosError "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

type Consumer struct {
	cConsumer  unsafe.Pointer
	dataParser *parser.TMQRawDataParser
	timezone   *time.Location
}

// NewConsumer Create new TMQ consumer with TMQ config
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error) {
	confStruct, err := configMapToConfig(conf)
	if err != nil {
		return nil, err
	}
	defer confStruct.destroy()
	cConsumer, err := wrapper.TMQConsumerNew(confStruct.cConfig)
	if err != nil {
		return nil, err
	}
	consumer := &Consumer{
		cConsumer:  cConsumer,
		dataParser: parser.NewTMQRawDataParser(),
		timezone:   confStruct.timezone,
	}
	return consumer, nil
}

func configMapToConfig(m *tmq.ConfigMap) (*config, error) {
	c := newConfig()
	confCopy := m.Clone()
	for k, v := range confCopy {
		vv, ok := v.(string)
		if !ok {
			c.destroy()
			return nil, errors.New("config value requires string")
		}
		if k == "timezone" {
			tz, err := common.ParseTimezone(vv)
			if err != nil {
				c.destroy()
				return nil, err
			}
			c.timezone = tz
			continue
		}
		err := c.setConfig(k, vv)
		if err != nil {
			c.destroy()
			return nil, err
		}
	}
	return c, nil
}

type RebalanceCb func(*Consumer, tmq.Event) error

func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	return c.SubscribeTopics([]string{topic}, rebalanceCb)
}

func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error {
	topicList := wrapper.TMQListNew()
	defer wrapper.TMQListDestroy(topicList)
	for _, topic := range topics {
		errCode := wrapper.TMQListAppend(topicList, topic)
		if errCode != 0 {
			return tmqError(errCode)
		}
	}
	errCode := wrapper.TMQSubscribe(c.cConsumer, topicList)
	if errCode != 0 {
		return tmqError(errCode)
	}
	return nil
}

// Unsubscribe TMQ unsubscribe
func (c *Consumer) Unsubscribe() error {
	errCode := wrapper.TMQUnsubscribe(c.cConsumer)
	if errCode != taosError.SUCCESS {
		return tmqError(errCode)
	}
	return nil
}

// Poll consumer poll message with timeout
func (c *Consumer) Poll(timeoutMs int) tmq.Event {
	message := wrapper.TMQConsumerPoll(c.cConsumer, int64(timeoutMs))
	if message == nil {
		return nil
	}
	defer func() {
		wrapper.TaosFreeResult(message)
	}()
	topic := wrapper.TMQGetTopicName(message)
	db := wrapper.TMQGetDBName(message)
	resultType := wrapper.TMQGetResType(message)
	offset := tmq.Offset(wrapper.TMQGetVgroupOffset(message))
	vgID := wrapper.TMQGetVgroupID(message)
	switch resultType {
	case common.TMQ_RES_DATA:
		result := &tmq.DataMessage{}
		result.SetDbName(db)
		result.SetTopic(topic)
		data, err := c.getData(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		result.SetData(data)
		result.SetOffset(offset)
		result.TopicPartition = tmq.TopicPartition{
			Topic:     &topic,
			Partition: vgID,
			Offset:    offset,
		}
		return result
	case common.TMQ_RES_TABLE_META:
		result := &tmq.MetaMessage{}
		result.SetDbName(db)
		result.SetTopic(topic)
		meta, err := c.getMeta(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		result.SetMeta(meta)
		result.SetOffset(offset)
		result.TopicPartition = tmq.TopicPartition{
			Topic:     &topic,
			Partition: vgID,
			Offset:    offset,
		}
		return result
	case common.TMQ_RES_METADATA:
		result := &tmq.MetaDataMessage{}
		result.SetDbName(db)
		result.SetTopic(topic)
		result.SetOffset(offset)
		data, err := c.getData(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		meta, err := c.getMeta(message)
		if err != nil {
			return tmq.NewTMQErrorWithErr(err)
		}
		result.SetMetaData(&tmq.MetaData{
			Meta: meta,
			Data: data,
		})
		result.TopicPartition = tmq.TopicPartition{
			Topic:     &topic,
			Partition: vgID,
			Offset:    offset,
		}
		return result
	default:
		return tmq.NewTMQError(0xfffff, "invalid tmq message type")
	}
}

func (c *Consumer) getMeta(message unsafe.Pointer) (*tmq.Meta, error) {
	var meta tmq.Meta
	p := wrapper.TMQGetJsonMeta(message)
	if p != nil {
		data := wrapper.ParseJsonMeta(p)
		wrapper.TMQFreeJsonMeta(p)
		err := jsoniter.Unmarshal(data, &meta)
		if err != nil {
			return nil, err
		}
		return &meta, nil
	}
	return &meta, nil
}

func (c *Consumer) getData(message unsafe.Pointer) ([]*tmq.Data, error) {
	errCode, raw := wrapper.TMQGetRaw(message)
	if errCode != taosError.SUCCESS {
		errStr := wrapper.TaosErrorStr(message)
		err := taosError.NewError(int(errCode), errStr)
		return nil, err
	}
	_, _, rawPtr := wrapper.ParseRawMeta(raw)
	blockInfos, err := c.dataParser.Parse(rawPtr)
	if err != nil {
		return nil, err
	}
	var tmqData []*tmq.Data
	for i := 0; i < len(blockInfos); i++ {
		var data [][]driver.Value
		if c.timezone == nil {
			data, err = parser.ReadBlockSimple(blockInfos[i].RawBlock, blockInfos[i].Precision)
		} else {
			data, err = parser.ReadBlockSimpleWithTimeFormat(blockInfos[i].RawBlock, blockInfos[i].Precision, c.FormatTime)
		}
		if err != nil {
			return nil, err
		}
		tmqData = append(tmqData, &tmq.Data{
			TableName: blockInfos[i].TableName,
			Data:      data,
		})
	}
	return tmqData, nil
}

func (c *Consumer) FormatTime(ts int64, precision int) driver.Value {
	return common.TimestampConvertToTimeWithLocation(ts, precision, c.timezone)
}

func (c *Consumer) Commit() ([]tmq.TopicPartition, error) {
	errCode := wrapper.TMQCommitSync(c.cConsumer, nil)
	if errCode != taosError.SUCCESS {
		return nil, tmqError(errCode)
	}
	partitions, err := c.Assignment()
	if err != nil {
		return nil, err
	}
	return c.Committed(partitions, 0)
}

func (c *Consumer) Assignment() (partitions []tmq.TopicPartition, err error) {
	errCode, list := wrapper.TMQSubscription(c.cConsumer)
	if errCode != taosError.SUCCESS {
		return nil, tmqError(errCode)
	}
	defer wrapper.TMQListDestroy(list)
	size := wrapper.TMQListGetSize(list)
	topics := wrapper.TMQListToCArray(list, int(size))
	for _, topic := range topics {
		errCode, assignment := wrapper.TMQGetTopicAssignment(c.cConsumer, topic)
		if errCode != taosError.SUCCESS {
			return nil, tmqError(errCode)
		}
		for i := 0; i < len(assignment); i++ {
			topicName := topic
			partitions = append(partitions, tmq.TopicPartition{
				Topic:     &topicName,
				Partition: assignment[i].VGroupID,
				Offset:    tmq.Offset(assignment[i].Offset),
			})
		}
	}
	return partitions, nil
}

func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error {
	errCode := wrapper.TMQOffsetSeek(c.cConsumer, *partition.Topic, partition.Partition, int64(partition.Offset))
	if errCode != taosError.SUCCESS {
		return tmqError(errCode)
	}
	return nil
}

func (c *Consumer) Committed(partitions []tmq.TopicPartition, timeoutMs int) (offsets []tmq.TopicPartition, err error) {
	offsets = make([]tmq.TopicPartition, len(partitions))
	for i := 0; i < len(partitions); i++ {
		cOffset := wrapper.TMQCommitted(c.cConsumer, *partitions[i].Topic, partitions[i].Partition)
		offset := tmq.Offset(cOffset)
		if !offset.Valid() {
			return nil, tmqError(int32(offset))
		}
		offsets[i] = tmq.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    offset,
		}
	}
	return
}

func (c *Consumer) CommitOffsets(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error) {
	for i := 0; i < len(offsets); i++ {
		errCode := wrapper.TMQCommitOffsetSync(c.cConsumer, *offsets[i].Topic, offsets[i].Partition, int64(offsets[i].Offset))
		if errCode != taosError.SUCCESS {
			return nil, tmqError(errCode)
		}
	}
	return c.Committed(offsets, 0)
}

func (c *Consumer) Position(partitions []tmq.TopicPartition) (offsets []tmq.TopicPartition, err error) {
	offsets = make([]tmq.TopicPartition, len(partitions))
	for i := 0; i < len(partitions); i++ {
		position := wrapper.TMQPosition(c.cConsumer, *partitions[i].Topic, partitions[i].Partition)
		if position < 0 {
			return nil, tmqError(int32(position))
		}
		offsets[i] = tmq.TopicPartition{
			Topic:     partitions[i].Topic,
			Partition: partitions[i].Partition,
			Offset:    tmq.Offset(position),
		}
	}
	return
}

// Close release consumer
func (c *Consumer) Close() error {
	errCode := wrapper.TMQConsumerClose(c.cConsumer)
	if errCode != 0 {
		return tmqError(errCode)
	}
	return nil
}

func tmqError(errCode int32) error {
	errStr := wrapper.TMQErr2Str(errCode)
	return taosError.NewError(int(errCode), errStr)
}
