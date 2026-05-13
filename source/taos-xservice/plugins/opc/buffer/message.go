package buffer

import (
	"collector/common"
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type MessageList struct {
	C            chan []*common.NodeValue
	lock         sync.Mutex
	count        atomic.Int64
	batchSize    int
	buffer       []*common.NodeValue
	list         *list.List
	batchTimeout time.Duration
	ticker       *time.Ticker
	full         chan struct{}
	get          chan struct{}
	logger       *logrus.Entry
	ctx          context.Context
}

func NewMessageList(ctx context.Context, batchSize int, batchTimeout time.Duration, logger *logrus.Entry) *MessageList {
	ml := &MessageList{
		ctx:          ctx,
		list:         list.New(),
		C:            make(chan []*common.NodeValue, 1),
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		buffer:       make([]*common.NodeValue, 0, batchSize),
		full:         make(chan struct{}, 0),
		get:          make(chan struct{}, 1),
		logger:       logger,
	}
	go func() {
		ml.process()
	}()
	return ml
}

func (m *MessageList) process() {
	m.ticker = time.NewTicker(m.batchTimeout)
	defer func() {
		m.ticker.Stop()
	}()
	for {
		select {
		case <-m.ctx.Done():
			m.logger.Info("message list process exit")
			return
		case <-m.ticker.C:
			m.logger.Debug("batch timeout,try send message to writer")
			m.trySend()
		case <-m.full:
			m.logger.Debug("batch full,try send message to writer")
			m.trySend()
		case <-m.get:
			m.logger.Debug("writer want get,try send message to writer")
			m.trySend()
		}
	}
}

func (m *MessageList) Add(message []*common.NodeValue) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.buffer != nil && len(m.buffer) < m.batchSize {
		delta := m.batchSize - len(m.buffer)
		if delta >= len(message) {
			m.buffer = append(m.buffer, message...)
			m.count.Add(int64(len(message)))
		} else {
			m.buffer = append(m.buffer, message[:delta]...)
			m.count.Add(int64(delta))
			for i := delta; i < len(message); i++ {
				m.list.PushBack(message[i])
				m.count.Add(1)
			}
			select {
			case m.full <- struct{}{}:
			default:
				// ignore repeated signals
			}
		}
		return
	} else {
		for i := 0; i < len(message); i++ {
			m.count.Add(1)
			m.list.PushBack(message[i])
		}
		select {
		case m.full <- struct{}{}:
		default:
			// ignore repeated signals
		}
	}
}

func (m *MessageList) tidy() {
	if len(m.buffer) < m.batchSize {
		if m.list.Len() > 0 {
			delta := m.batchSize - len(m.buffer)
			for i := 0; i < delta; i++ {
				element := m.list.Front()
				if element == nil {
					break
				}
				m.buffer = append(m.buffer, element.Value.(*common.NodeValue))
				m.list.Remove(element)
			}
		}
	}
}

func (m *MessageList) Length() int {
	return int(m.count.Load()) + len(m.C)*m.batchSize
}

func (m *MessageList) trySend() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.tidy()
	if len(m.buffer) > 0 {
		select {
		case m.C <- m.buffer:
			m.count.Add(-int64(len(m.buffer)))
			m.buffer = make([]*common.NodeValue, 0, m.batchSize)
			m.tidy()
			if len(m.buffer) == m.batchSize {
				select {
				case m.full <- struct{}{}:
				default:
					// ignore repeated signals
				}
			}
		default:
			m.logger.Warnf("writer is busy, put message back")
		}
	}
}

func (m *MessageList) TryGet() {
	select {
	case m.get <- struct{}{}:
	default:
	}
}
