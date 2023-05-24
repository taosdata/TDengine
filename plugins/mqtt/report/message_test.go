package report

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageList(t *testing.T) {
	messages := NewMessageList()

	messages.Add(&Message{Topic: "test", Payload: []byte("hello")})
	messages.Add(&Message{Topic: "test", Payload: []byte("world")})

	allMessages := messages.GetAll()

	assert.Equal(t, 2, len(allMessages))
	assert.Equal(t, "test", allMessages[0].Topic)
	assert.Equal(t, "test", allMessages[1].Topic)
	assert.Equal(t, []byte("hello"), allMessages[0].Payload)
	assert.Equal(t, []byte("world"), allMessages[1].Payload)
	assert.Equal(t, 0, messages.list.Len())
}
