package log

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// taosLinePattern validates the TAOS log format:
// MM/DD HH:MM:SS.UUUUUU <PID> WSC <LEVEL> QID:0x<hex> <message>
var taosLinePattern = regexp.MustCompile(
	`^\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{6} \d+ WSC (DEBUG |INFO  |WARN  |ERROR )QID:0x[0-9a-f]+ .+\n$`,
)

func resetGlobals() {
	atomic.StoreInt32(&globalLogLevel, int32(LogLevelSilent))
	atomic.StoreUint32(&packetLogOn, 0)
	atomic.StoreInt32(&maxPacketLogBytes, 512)
	globalLogMu.Lock()
	globalLogWriter = ioutil.Discard
	globalLogger = nil
	globalLogMu.Unlock()
}

// --- Functional tests ---

func TestDefaultSilent(t *testing.T) {
	resetGlobals()
	var buf bytes.Buffer
	SetOutput(&buf)
	// default is Silent — nothing should be written
	Info(0, "should not appear")
	Error(0, "should not appear either")
	if buf.Len() != 0 {
		t.Fatalf("expected no output at Silent level, got: %q", buf.String())
	}
}

func TestLevelFiltering(t *testing.T) {
	cases := []struct {
		setLevel LogLevel
		logLevel LogLevel
		expect   bool
	}{
		{LogLevelDebug, LogLevelDebug, true},
		{LogLevelDebug, LogLevelInfo, true},
		{LogLevelDebug, LogLevelError, true},
		{LogLevelInfo, LogLevelDebug, false},
		{LogLevelInfo, LogLevelInfo, true},
		{LogLevelInfo, LogLevelWarn, true},
		{LogLevelWarn, LogLevelInfo, false},
		{LogLevelWarn, LogLevelWarn, true},
		{LogLevelError, LogLevelWarn, false},
		{LogLevelError, LogLevelError, true},
		{LogLevelSilent, LogLevelError, false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("set=%d/log=%d", tc.setLevel, tc.logLevel), func(t *testing.T) {
			resetGlobals()
			var buf bytes.Buffer
			SetOutput(&buf)
			SetLevel(tc.setLevel)
			logMsg(tc.logLevel, 1, "test message")
			got := buf.Len() > 0
			if got != tc.expect {
				t.Errorf("expected output=%v, got=%v", tc.expect, got)
			}
		})
	}
}

func TestTAOSFormat(t *testing.T) {
	resetGlobals()
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LogLevelDebug)

	Info(0x001a000000000042, "reconnect succeeded, elapsed: 12 ms")
	line := buf.String()

	if !taosLinePattern.MatchString(line) {
		t.Fatalf("line does not match TAOS format pattern.\nGot:    %q\nExpect: MM/DD HH:MM:SS.UUUUUU <PID> WSC INFO  QID:0x<hex> <msg>\\n", line)
	}
	if !strings.Contains(line, fmt.Sprintf(" %d WSC ", os.Getpid())) {
		t.Fatalf("pid field not found in line: %q", line)
	}
	if !strings.Contains(line, "QID:0x1a000000000042") {
		t.Fatalf("QID not found in line: %q", line)
	}
	if !strings.Contains(line, " INFO  QID:") {
		t.Fatalf("level field 'INFO' not found in line: %q", line)
	}
	if !strings.Contains(line, "reconnect succeeded, elapsed: 12 ms") {
		t.Fatalf("message not found in line: %q", line)
	}
}

func TestQIDZero(t *testing.T) {
	resetGlobals()
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LogLevelDebug)

	Debug(0, "zero qid")
	line := buf.String()

	if !strings.Contains(line, "QID:0x0") {
		t.Fatalf("zero QID not formatted correctly: %q", line)
	}
}

func TestQIDMaxUint64(t *testing.T) {
	resetGlobals()
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LogLevelDebug)

	Debug(0xffffffffffffffff, "max qid")
	line := buf.String()

	if !strings.Contains(line, "QID:0xffffffffffffffff") {
		t.Fatalf("max QID not formatted correctly: %q", line)
	}
}

func TestLevelTags(t *testing.T) {
	levels := []struct {
		level LogLevel
		tag   string
	}{
		{LogLevelDebug, "DEBUG "},
		{LogLevelInfo, "INFO  "},
		{LogLevelWarn, "WARN  "},
		{LogLevelError, "ERROR "},
	}
	for _, tc := range levels {
		t.Run(strings.TrimSpace(tc.tag), func(t *testing.T) {
			resetGlobals()
			var buf bytes.Buffer
			SetOutput(&buf)
			SetLevel(LogLevelDebug)
			logMsg(tc.level, 0, "test")
			if !strings.Contains(buf.String(), "WSC "+tc.tag+"QID:0x0") {
				t.Fatalf("expected level field %q in: %q", tc.tag, buf.String())
			}
		})
	}
}

func TestFormattedLog(t *testing.T) {
	resetGlobals()
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(LogLevelDebug)

	Infof(42, "connected to %s, elapsed: %d ms", "ws://host:6041", 15)
	line := buf.String()

	if !taosLinePattern.MatchString(line) {
		t.Fatalf("formatted line does not match TAOS pattern: %q", line)
	}
	if !strings.Contains(line, "connected to ws://host:6041, elapsed: 15 ms") {
		t.Fatalf("formatted message not found: %q", line)
	}
}

func TestSetOutputSwitchesWriter(t *testing.T) {
	resetGlobals()
	var buf1, buf2 bytes.Buffer
	SetOutput(&buf1)
	SetLevel(LogLevelInfo)

	Info(0, "to buf1")
	if buf1.Len() == 0 {
		t.Fatal("expected output to buf1")
	}

	SetOutput(&buf2)
	Info(0, "to buf2")
	if buf2.Len() == 0 {
		t.Fatal("expected output to buf2")
	}
}

func TestSetOutputNilResets(t *testing.T) {
	resetGlobals()
	SetOutput(nil)
	globalLogMu.RLock()
	w := globalLogWriter
	globalLogMu.RUnlock()
	if w == nil {
		t.Fatal("SetOutput(nil) should not set writer to nil")
	}
}

// --- Custom Logger tests ---

type recordingLogger struct {
	mu      sync.Mutex
	entries []logEntry
}

type logEntry struct {
	level LogLevel
	qid   uint64
	msg   string
}

func (r *recordingLogger) Log(level LogLevel, qid uint64, msg string) {
	r.mu.Lock()
	r.entries = append(r.entries, logEntry{level, qid, msg})
	r.mu.Unlock()
}

func TestCustomLogger(t *testing.T) {
	resetGlobals()
	recorder := &recordingLogger{}
	SetLogger(recorder)
	SetLevel(LogLevelInfo)

	Info(0x42, "custom message")
	Debugf(0x43, "should not appear because level is Info")

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(recorder.entries))
	}
	e := recorder.entries[0]
	if e.level != LogLevelInfo || e.qid != 0x42 || e.msg != "custom message" {
		t.Fatalf("unexpected entry: %+v", e)
	}
}

func TestCustomLoggerBypassesBuiltinFormat(t *testing.T) {
	resetGlobals()
	var builtin bytes.Buffer
	SetOutput(&builtin)
	recorder := &recordingLogger{}
	SetLogger(recorder)
	SetLevel(LogLevelDebug)

	Info(0, "routed to custom")
	if builtin.Len() != 0 {
		t.Fatal("custom logger set but output went to built-in writer")
	}
	recorder.mu.Lock()
	n := len(recorder.entries)
	recorder.mu.Unlock()
	if n != 1 {
		t.Fatalf("expected 1 entry in custom logger, got %d", n)
	}
}

func TestSetLoggerNilRevertsToBuiltin(t *testing.T) {
	resetGlobals()
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLogger(&recordingLogger{})
	SetLevel(LogLevelInfo)

	Info(0, "to custom")
	if buf.Len() != 0 {
		t.Fatal("should not write to built-in when custom is set")
	}

	SetLogger(nil)
	Info(0, "to builtin")
	if buf.Len() == 0 {
		t.Fatal("SetLogger(nil) should revert to built-in")
	}
}

// --- GetLevel ---

func TestGetLevel(t *testing.T) {
	resetGlobals()
	SetLevel(LogLevelWarn)
	if GetLevel() != LogLevelWarn {
		t.Fatalf("expected Warn, got %d", GetLevel())
	}
}

// --- IsDebugEnabled / IsInfoEnabled ---

func TestIsDebugEnabled(t *testing.T) {
	resetGlobals()
	SetLevel(LogLevelDebug)
	if !IsDebugEnabled() {
		t.Fatal("expected IsDebugEnabled=true at Debug level")
	}
	SetLevel(LogLevelInfo)
	if IsDebugEnabled() {
		t.Fatal("expected IsDebugEnabled=false at Info level")
	}
}

func TestIsInfoEnabled(t *testing.T) {
	resetGlobals()
	SetLevel(LogLevelInfo)
	if !IsInfoEnabled() {
		t.Fatal("expected IsInfoEnabled=true at Info level")
	}
	SetLevel(LogLevelWarn)
	if IsInfoEnabled() {
		t.Fatal("expected IsInfoEnabled=false at Warn level")
	}
}

func TestPacketLoggingSwitch(t *testing.T) {
	resetGlobals()
	if IsPacketLoggingEnabled() {
		t.Fatal("packet logging should be disabled by default")
	}
	SetPacketLogging(true)
	if !IsPacketLoggingEnabled() {
		t.Fatal("packet logging should be enabled")
	}
	SetPacketLogging(false)
	if IsPacketLoggingEnabled() {
		t.Fatal("packet logging should be disabled after reset")
	}
}

func TestMaxPacketLogBytes(t *testing.T) {
	resetGlobals()
	if GetMaxPacketLogBytes() != 512 {
		t.Fatalf("expected default 512, got %d", GetMaxPacketLogBytes())
	}
	SetMaxPacketLogBytes(512)
	if GetMaxPacketLogBytes() != 512 {
		t.Fatalf("expected 512, got %d", GetMaxPacketLogBytes())
	}
	SetMaxPacketLogBytes(0)
	if GetMaxPacketLogBytes() != 0 {
		t.Fatalf("expected 0, got %d", GetMaxPacketLogBytes())
	}
}

// --- Helper tests ---

func TestTruncateText(t *testing.T) {
	if got := TruncateText("hello", 10); got != "hello" {
		t.Fatalf("unexpected: %q", got)
	}
	if got := TruncateText("hello world", 5); got != "hello...(truncated)" {
		t.Fatalf("unexpected: %q", got)
	}
	if got := TruncateText("hi", 0); got != "hi" {
		t.Fatalf("unexpected: %q", got)
	}
}

func TestHexPreview(t *testing.T) {
	data := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	if got := HexPreview(data, 10); got != "deadbeef01" {
		t.Fatalf("unexpected: %q", got)
	}
	if got := HexPreview(data, 3); got != "deadbe...(truncated)" {
		t.Fatalf("unexpected: %q", got)
	}
}

// --- Concurrency test ---

func TestConcurrentSafety(t *testing.T) {
	resetGlobals()
	SetOutput(ioutil.Discard)
	SetLevel(LogLevelDebug)

	var wg sync.WaitGroup
	const goroutines = 50
	const iterations = 200

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				qid := uint64(id*1000 + j)
				switch j % 4 {
				case 0:
					Debug(qid, "concurrent debug")
				case 1:
					Infof(qid, "concurrent info %d", j)
				case 2:
					Warn(qid, "concurrent warn")
				case 3:
					Errorf(qid, "concurrent error %d", j)
				}
			}
		}(i)
	}

	// concurrent SetLevel calls
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iterations; j++ {
			switch j % 3 {
			case 0:
				SetLevel(LogLevelDebug)
			case 1:
				SetLevel(LogLevelInfo)
			case 2:
				SetLevel(LogLevelWarn)
			}
		}
	}()

	// concurrent SetLogger calls
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := &recordingLogger{}
		for j := 0; j < iterations; j++ {
			if j%2 == 0 {
				SetLogger(r)
			} else {
				SetLogger(nil)
			}
		}
	}()

	wg.Wait()
}

// --- Performance comparison benchmarks ---

// Baseline: a raw no-op function call (measures call overhead)
func BenchmarkBaseline_Noop(b *testing.B) {
	for i := 0; i < b.N; i++ {
		noop(uint64(i), "baseline message")
	}
}

//go:noinline
func noop(qid uint64, msg string) {}

// Disabled: log call at Silent level (should be near-zero cost)
func BenchmarkDisabled_FixedMsg(b *testing.B) {
	resetGlobals()
	// level is Silent by default
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info(uint64(i), "this should not be logged")
	}
}

// Disabled: formatted log call at Silent level
func BenchmarkDisabled_Formatted(b *testing.B) {
	resetGlobals()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Infof(uint64(i), "formatted %d %s", i, "test")
	}
}

// Enabled: built-in TAOS format, fixed message, write to ioutil.Discard
func BenchmarkEnabled_FixedMsg_Discard(b *testing.B) {
	resetGlobals()
	SetOutput(ioutil.Discard)
	SetLevel(LogLevelDebug)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info(uint64(i), "reconnect succeeded, elapsed: 12 ms")
	}
}

// Enabled: built-in TAOS format, formatted message, write to ioutil.Discard
func BenchmarkEnabled_Formatted_Discard(b *testing.B) {
	resetGlobals()
	SetOutput(ioutil.Discard)
	SetLevel(LogLevelDebug)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Infof(uint64(i), "connected to %s, elapsed: %d ms", "ws://host:6041", 15)
	}
}

// Enabled: custom Logger (minimal implementation)
func BenchmarkEnabled_CustomLogger(b *testing.B) {
	resetGlobals()
	SetLogger(nopLogger{})
	SetLevel(LogLevelDebug)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info(uint64(i), "reconnect succeeded, elapsed: 12 ms")
	}
}

type nopLogger struct{}

func (nopLogger) Log(LogLevel, uint64, string) {}

// Enabled: built-in format, parallel writes to ioutil.Discard
func BenchmarkEnabled_Parallel(b *testing.B) {
	resetGlobals()
	SetOutput(ioutil.Discard)
	SetLevel(LogLevelDebug)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			Info(i, "parallel log message")
			i++
		}
	})
}

// Disabled: parallel calls at Silent level
func BenchmarkDisabled_Parallel(b *testing.B) {
	resetGlobals()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			Info(i, "should not be logged")
			i++
		}
	})
}
