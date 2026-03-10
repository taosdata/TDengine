package log

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taoskeeper/infrastructure/config"
)

func TestConfigLog(t *testing.T) {
	config.InitConfig()
	config.Conf.LogLevel = "debug"
	ConfigLog()
	debug, _ := logrus.ParseLevel("debug")
	assert.Equal(t, logger.Level, debug)
	assert.Equal(t, true, IsDebug())
	fmt.Print(GetLogNow(true), GetLogDuration(true, time.Now()))
	Close(context.Background())
}

func TestGetLogDuration_NotDebug_ReturnsZeroDuration(t *testing.T) {
	start := time.Now().Add(-2 * time.Second)
	got := GetLogDuration(false, start)
	if got != zeroDuration {
		t.Fatalf("got %v, want %v (zeroDuration)", got, zeroDuration)
	}
}

func TestGetLogLevel(t *testing.T) {
	level := GetLogLevel()
	assert.Equal(t, logger.Level, level)
}

func TestGetLogNow_NotDebug_ReturnsZeroTime(t *testing.T) {
	got := GetLogNow(false)
	if !got.Equal(zeroTime) {
		t.Fatalf("expected zeroTime, got %v", got)
	}
}

func TestTaosLogFormatterFormat_NewBufferWhenNil(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Time:    time.Unix(0, 0),
		Message: "hello",
	}
	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	if entry.Buffer != nil {
		t.Fatalf("expected entry.Buffer to remain nil when not provided")
	}
	if !strings.Contains(string(out), "hello") {
		t.Fatalf("formatted output missing message, got %q", string(out))
	}
}

func TestTaosLogFormatterFormat_UsesProvidedEntryBuffer(t *testing.T) {
	tf := &TaosLogFormatter{}
	buf := &bytes.Buffer{}
	buf.WriteString("SENTINEL")
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Time:    time.Unix(0, 0),
		Message: "world",
		Buffer:  buf,
	}
	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	if strings.Contains(entry.Buffer.String(), "SENTINEL") {
		t.Fatalf("entry.Buffer still contains old data, got %q", entry.Buffer.String())
	}
	if !strings.Contains(entry.Buffer.String(), "world") {
		t.Fatalf("entry.Buffer missing new message, got %q", entry.Buffer.String())
	}
	if !strings.Contains(string(out), "world") {
		t.Fatalf("formatted output missing message, got %q", string(out))
	}
}

func TestTaosLogFormatterFormat_WritesDefaultCLIWhenNoModelKey(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Time:    time.Unix(0, 0),
		Message: "hello",
	}

	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	if !strings.Contains(string(out), " CLI ") {
		t.Fatalf("expected formatted log to contain ' CLI ', got: %q", string(out))
	}
}

func TestTaosLogFormatterFormat_LevelPanic_WritesPANIC(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.PanicLevel,
		Time:    time.Unix(0, 0),
		Message: "panic-msg",
	}

	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	if !strings.Contains(string(out), " PANIC ") {
		t.Fatalf("formatted output should contain ' PANIC ', got: %q", string(out))
	}
}

func TestTaosLogFormatterFormat_LevelFatal_WritesFATAL(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.FatalLevel,
		Time:    time.Unix(0, 0),
		Message: "fatal-msg",
	}

	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	if !strings.Contains(string(out), " FATAL ") {
		t.Fatalf("formatted output should contain ' FATAL ', got: %q", string(out))
	}
}

func TestTaosLogFormatterFormat_LevelDebug_WritesDEBUG(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.DebugLevel,
		Time:    time.Unix(0, 0),
		Message: "debug-msg",
	}

	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	if !strings.Contains(string(out), " DEBUG ") {
		t.Fatalf("formatted output should contain ' DEBUG ', got: %q", string(out))
	}
}

func TestTaosLogFormatterFormat_KeysAppend_ExcludesModelAndReqID_IncludesOthers(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Time:    time.Unix(0, 0),
		Message: "msg",
		Data: logrus.Fields{
			config.ModelKey: "TestModel",
			config.ReqIDKey: uint64(0x1a2b),
			"user":          "alice",
			"code":          200,
		},
	}

	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	s := string(out)

	if !strings.Contains(s, ", user:alice") {
		t.Fatalf("expected appended field ', user:alice', got: %q", s)
	}
	if !strings.Contains(s, ", code:200") {
		t.Fatalf("expected appended field ', code:200', got: %q", s)
	}

	if strings.Contains(s, ", "+config.ModelKey+":") {
		t.Fatalf("model key should not appear in appended fields, got: %q", s)
	}
	if strings.Contains(s, ", "+config.ReqIDKey+":") {
		t.Fatalf("req_id should not appear in appended fields, got: %q", s)
	}
}

func TestTaosLogFormatterFormat_AppendsExtraFields(t *testing.T) {
	tf := &TaosLogFormatter{}
	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.InfoLevel,
		Time:    time.Unix(0, 0),
		Message: "msg",
		Data: logrus.Fields{
			config.ModelKey: "ModelX",
			config.ReqIDKey: uint64(0x12ab),
			"user":          "alice",
			"code":          200,
		},
	}

	out, err := tf.Format(entry)
	if err != nil {
		t.Fatalf("Format error: %v", err)
	}
	s := string(out)

	if !strings.Contains(s, ", user:alice") {
		t.Fatalf("expected appended field ', user:alice', got: %q", s)
	}
	if !strings.Contains(s, ", code:200") {
		t.Fatalf("expected appended field ', code:200', got: %q", s)
	}
}

type errWC struct{ err error }

func (w errWC) Write(p []byte) (int, error) { return 0, w.err }
func (w errWC) Close() error                { return nil }

func TestFileHookFire_ReturnsFlushError(t *testing.T) {
	sentinel := errors.New("flush boom")
	fh := &FileHook{
		formatter: globalLogFormatter,
		writer:    errWC{err: sentinel},
		buf:       &bytes.Buffer{},
	}

	entry := &logrus.Entry{
		Logger:  logrus.New(),
		Level:   logrus.PanicLevel,
		Time:    time.Unix(0, 0),
		Message: "msg",
	}

	assert.EqualError(t, fh.Fire(entry), sentinel.Error())
}

func TestSetLevel_Invalid_ReturnsErrorAndLeavesLevelUnchanged(t *testing.T) {
	old := GetLogLevel()

	err := SetLevel("not_a_level")
	if err == nil {
		t.Fatalf("expected error for invalid level, got nil")
	}

	if GetLogLevel() != old {
		t.Fatalf("log level changed on error, got %v, want %v", GetLogLevel(), old)
	}
}
