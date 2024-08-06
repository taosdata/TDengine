package report

import (
	"container/list"
	"sync"

	"github.com/taosdata/taosx/plugins/mqtt/log"
)

type Message struct {
	TS      int64
	Topic   string
	Qos     byte
	Payload []byte
}

type MessageList struct {
	list      *list.List
	lock      sync.Mutex
	c         chan struct{}
	sent      bool
	batchSize int
	id        int
}

func NewMessageList(batchSize int, id int) *MessageList {
	m := &MessageList{list: list.New(), c: make(chan struct{}, 1), batchSize: batchSize, id: id}
	return m
}

func (m *MessageList) Add(message *Message) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.list.PushBack(message)
	if m.list.Len() >= m.batchSize && !m.sent {
		log.GetLogger("message").WithField("id", m.id).WithField("cap", m.batchSize).WithField("len", m.list.Len()).Debugln("reaches batch size")
		select {
		case m.c <- struct{}{}:
			m.sent = true
		default:
		}
	}
}

func (m *MessageList) GetAll() []*Message {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.sent = false
	if m.list.Len() > 0 {
		values := make([]*Message, 0, m.list.Len())
		for e := m.list.Front(); e != nil; e = e.Next() {
			values = append(values, e.Value.(*Message))
		}
		m.list = list.New()
		return values
	}
	return nil
}

func (m *MessageList) C() <-chan struct{} {
	return m.c
}
