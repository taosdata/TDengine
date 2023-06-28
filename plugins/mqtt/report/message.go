package report

import (
	"container/list"
	"sync"
)

type Message struct {
	TS      int64
	Topic   string
	Qos     byte
	Payload []byte
}

type MessageList struct {
	list *list.List
	lock sync.RWMutex
	c    chan struct{}
	sent bool
}

func NewMessageList() *MessageList {
	return &MessageList{list: list.New(), c: make(chan struct{}, 1)}
}

func (m *MessageList) Add(message *Message) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.list.PushBack(message)
	if !m.sent {
		m.c <- struct{}{}
		m.sent = true
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
