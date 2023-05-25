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
}

func NewMessageList() *MessageList {
	return &MessageList{list: list.New()}
}

func (m *MessageList) Add(message *Message) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.list.PushBack(message)
}

func (m *MessageList) GetAll() []*Message {
	m.lock.Lock()
	defer m.lock.Unlock()
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
