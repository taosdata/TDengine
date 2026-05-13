package tmq

import (
	"database/sql/driver"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/tmq"
	wsunified "github.com/taosdata/driver-go/v3/ws/unified"
	wsproto "github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// Consumer is the ws/tmq compatibility shell. Runtime logic delegates to ws/unified.
type Consumer struct {
	unifiedConsumer *wsunified.TMQConsumer
}

type WSError = wsunified.WSError

// NewConsumer creates a tmq consumer via unified implementation.
func NewConsumer(conf *tmq.ConfigMap) (*Consumer, error) {
	unifiedConsumer, err := wsunified.NewTMQConsumer(conf)
	if err != nil {
		return nil, err
	}
	return &Consumer{unifiedConsumer: unifiedConsumer}, nil
}

func (c *Consumer) unified() (*wsunified.TMQConsumer, error) {
	if c == nil || c.unifiedConsumer == nil {
		return nil, wsunified.ErrTMQConsumerUninitialized
	}
	return c.unifiedConsumer, nil
}

const (
	TMQSubscribe          = wsproto.TMQActionSubscribe
	TMQPoll               = wsproto.TMQActionPoll
	TMQFetchRaw           = wsproto.TMQActionFetchRaw
	TMQFetchJsonMeta      = wsproto.TMQActionFetchJSONMeta
	TMQCommit             = wsproto.TMQActionCommit
	TMQUnsubscribe        = wsproto.TMQActionUnsubscribe
	TMQGetTopicAssignment = wsproto.TMQActionAssignment
	TMQSeek               = wsproto.TMQActionSeek
	TMQCommitOffset       = wsproto.TMQActionCommitOffset
	TMQCommitted          = wsproto.TMQActionCommitted
	TMQPosition           = wsproto.TMQActionPosition
)

//revive:disable-next-line
var ClosedErr = wsunified.ClosedErr

type RebalanceCb func(*Consumer, tmq.Event) error

func adaptRebalanceCb(owner *Consumer, cb RebalanceCb) wsunified.RebalanceCb {
	if cb == nil {
		return nil
	}
	return func(_ *wsunified.TMQConsumer, event tmq.Event) error {
		return cb(owner, event)
	}
}

// Close consumer. This function can be called multiple times.
func (c *Consumer) Close() error {
	consumer, err := c.unified()
	if err != nil {
		return err
	}
	return consumer.Close()
}

func (c *Consumer) Subscribe(topic string, rebalanceCb RebalanceCb) error {
	consumer, err := c.unified()
	if err != nil {
		return err
	}
	return consumer.Subscribe(topic, adaptRebalanceCb(c, rebalanceCb))
}

func (c *Consumer) SubscribeTopics(topics []string, rebalanceCb RebalanceCb) error {
	consumer, err := c.unified()
	if err != nil {
		return err
	}
	return consumer.SubscribeTopics(topics, adaptRebalanceCb(c, rebalanceCb))
}

// Poll messages.
func (c *Consumer) Poll(timeoutMs int) tmq.Event {
	consumer, err := c.unified()
	if err != nil {
		return tmq.NewTMQErrorWithErr(err)
	}
	return consumer.Poll(timeoutMs)
}

func (c *Consumer) FormatTime(ts int64, precision int) driver.Value {
	if consumer, err := c.unified(); err == nil {
		return consumer.FormatTime(ts, precision)
	}
	return common.TimestampConvertToTimeWithLocation(ts, precision, nil)
}

func (c *Consumer) Commit() ([]tmq.TopicPartition, error) {
	consumer, err := c.unified()
	if err != nil {
		return nil, err
	}
	return consumer.Commit()
}

func (c *Consumer) Unsubscribe() error {
	consumer, err := c.unified()
	if err != nil {
		return err
	}
	return consumer.Unsubscribe()
}

func (c *Consumer) Assignment() ([]tmq.TopicPartition, error) {
	consumer, err := c.unified()
	if err != nil {
		return nil, err
	}
	return consumer.Assignment()
}

func (c *Consumer) Seek(partition tmq.TopicPartition, ignoredTimeoutMs int) error {
	consumer, err := c.unified()
	if err != nil {
		return err
	}
	return consumer.Seek(partition, ignoredTimeoutMs)
}

func (c *Consumer) Committed(partitions []tmq.TopicPartition, timeoutMs int) ([]tmq.TopicPartition, error) {
	consumer, err := c.unified()
	if err != nil {
		return nil, err
	}
	return consumer.Committed(partitions, timeoutMs)
}

func (c *Consumer) CommitOffsets(offsets []tmq.TopicPartition) ([]tmq.TopicPartition, error) {
	consumer, err := c.unified()
	if err != nil {
		return nil, err
	}
	return consumer.CommitOffsets(offsets)
}

func (c *Consumer) Position(partitions []tmq.TopicPartition) ([]tmq.TopicPartition, error) {
	consumer, err := c.unified()
	if err != nil {
		return nil, err
	}
	return consumer.Position(partitions)
}
