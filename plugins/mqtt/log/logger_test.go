package log

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_defaultPool_Put(t *testing.T) {
	b := bufferPool.Get()
	b.WriteByte('a')
	s := b.String()
	assert.Equal(t, "a", s)
	bufferPool.Put(b)
}

func TestTaosLogFormatter_Format(t *testing.T) {
	formatter := &TaosLogFormatter{}
	timestamp := time.Now()
	entry := &logrus.Entry{
		Time:    timestamp,
		Level:   logrus.InfoLevel,
		Message: "This is a test message.",
		Data: logrus.Fields{
			"key1": "value1",
		},
	}

	want := fmt.Sprintf("%s %s info \"This is a test message.\" key1=value1\n",
		timestamp.Format("01/02 15:04:05.000000"), ServerID)

	got, err := formatter.Format(entry)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if string(got) != want {
		t.Errorf("Unexpected format. Want %s, got %s", want, got)
	}
}
func TestGetLogger(t *testing.T) {
	err := SetLevel("")
	assert.Error(t, err)
	err = SetLevel("debug")
	assert.NoError(t, err)
	logger1 := GetLogger("test")
	logger1.Println("get logger with model test")
}
