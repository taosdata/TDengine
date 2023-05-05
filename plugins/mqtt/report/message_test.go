package report

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageList(t *testing.T) {
	messages := NewMessageList()

	msg1 := &mockMessage{topic: "test", payload: []byte("hello")}
	msg2 := &mockMessage{topic: "test", payload: []byte("world")}
	messages.Add(&Message{Message: msg1})
	messages.Add(&Message{Message: msg2})

	allMessages := messages.GetAll()

	assert.Equal(t, 2, len(allMessages))
	assert.Equal(t, msg1.Topic(), allMessages[0].Message.Topic())
	assert.Equal(t, msg2.Topic(), allMessages[1].Message.Topic())
	assert.Equal(t, msg1.Payload(), allMessages[0].Message.Payload())
	assert.Equal(t, msg2.Payload(), allMessages[1].Message.Payload())
	assert.Equal(t, 0, messages.list.Len())
}
