// Package log provides a global, lightweight logger for the TDengine Go driver
// websocket layer. Output follows the TAOS logging specification:
//
//	MM/DD HH:MM:SS.UUUUUU <PID> WSC INFO  QID:0x1a000000000042 reconnect succeeded, elapsed: 12 ms
//
// By default all output is suppressed (LogLevelSilent). Call SetLevel to enable.
//
// Two output modes:
//   - Built-in mode (default): TAOS-formatted lines written to an io.Writer (os.Stderr by default).
//   - Custom mode: call SetLogger to route messages through a user-provided Logger interface,
//     bypassing the built-in formatter entirely. This allows integration with zap, logrus, slog, etc.
//
// All functions are safe for concurrent use.
package log

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// LogLevel controls which log messages are emitted.
type LogLevel int32

const (
	LogLevelDebug  LogLevel = 0
	LogLevelInfo   LogLevel = 1
	LogLevelWarn   LogLevel = 2
	LogLevelError  LogLevel = 3
	LogLevelSilent LogLevel = 100 // default — all output suppressed
)

// Logger allows users to route driver log messages into their own framework.
// When set via SetLogger, the built-in TAOS formatter is bypassed entirely.
// Implementations must be safe for concurrent use.
type Logger interface {
	Log(level LogLevel, qid uint64, msg string)
}

// levelLabel maps LogLevel to the taosadapter-style fixed-width label.
var levelLabel = [4]string{"DEBUG ", "INFO  ", "WARN  ", "ERROR "}

// global log state
var (
	globalLogLevel              = int32(LogLevelSilent)
	globalLogWriter   io.Writer = os.Stderr
	globalLogger      Logger    // nil means built-in mode
	globalLogMu       sync.RWMutex
	processID         = os.Getpid()
	packetLogOn       uint32
	maxPacketLogBytes int32 = 512 // default max bytes for packet content in logs
)

// bufPool avoids per-line heap allocation.
var bufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 512)
		return &buf
	},
}

// SetLevel sets the global log level. Goroutine-safe.
// Default is LogLevelSilent (all output suppressed).
func SetLevel(level LogLevel) {
	atomic.StoreInt32(&globalLogLevel, int32(level))
}

// GetLevel returns the current global log level.
func GetLevel() LogLevel {
	return LogLevel(atomic.LoadInt32(&globalLogLevel))
}

// SetPacketLogging controls whether packet send/receive content logs are enabled.
// This is consumed by websocket request paths and is disabled by default.
func SetPacketLogging(enabled bool) {
	if enabled {
		atomic.StoreUint32(&packetLogOn, 1)
		return
	}
	atomic.StoreUint32(&packetLogOn, 0)
}

// IsPacketLoggingEnabled reports whether packet content logging is enabled.
func IsPacketLoggingEnabled() bool {
	return atomic.LoadUint32(&packetLogOn) == 1
}

// SetMaxPacketLogBytes sets the maximum number of bytes to include in packet
// content logs. Binary payloads are hex-encoded, text payloads are truncated
// to this limit. Values <= 0 mean unlimited. Default is 512.
func SetMaxPacketLogBytes(n int) {
	atomic.StoreInt32(&maxPacketLogBytes, int32(n))
}

// GetMaxPacketLogBytes returns the current max-bytes limit for packet content logs.
func GetMaxPacketLogBytes() int {
	return int(atomic.LoadInt32(&maxPacketLogBytes))
}

// SetOutput sets the built-in TAOS-format output destination.
// Pass nil to reset to os.Stderr. Has no effect when a custom Logger is set.
func SetOutput(w io.Writer) {
	globalLogMu.Lock()
	if w == nil {
		globalLogWriter = os.Stderr
	} else {
		globalLogWriter = w
	}
	globalLogMu.Unlock()
}

// SetLogger installs a custom Logger that receives all enabled log messages.
// When set, the built-in TAOS formatter is bypassed; SetOutput has no effect.
// Pass nil to revert to built-in mode.
func SetLogger(l Logger) {
	globalLogMu.Lock()
	globalLogger = l
	globalLogMu.Unlock()
}

// IsDebugEnabled reports whether Debug-level messages would be emitted.
// Use to guard expensive argument construction.
func IsDebugEnabled() bool {
	return int32(LogLevelDebug) >= atomic.LoadInt32(&globalLogLevel)
}

// IsInfoEnabled reports whether Info-level messages would be emitted.
func IsInfoEnabled() bool {
	return int32(LogLevelInfo) >= atomic.LoadInt32(&globalLogLevel)
}

// ---- Fixed-message entry points (zero alloc when disabled) ----

// Debug logs a message at Debug level.
func Debug(qid uint64, msg string) { logMsg(LogLevelDebug, qid, msg) }

// Info logs a message at Info level.
func Info(qid uint64, msg string) { logMsg(LogLevelInfo, qid, msg) }

// Warn logs a message at Warn level.
func Warn(qid uint64, msg string) { logMsg(LogLevelWarn, qid, msg) }

// Error logs a message at Error level.
func Error(qid uint64, msg string) { logMsg(LogLevelError, qid, msg) }

// ---- Formatted entry points (fmt.Sprintf only runs when enabled) ----

// Debugf logs a formatted message at Debug level.
func Debugf(qid uint64, format string, args ...interface{}) {
	logFmt(LogLevelDebug, qid, format, args)
}

// Infof logs a formatted message at Info level.
func Infof(qid uint64, format string, args ...interface{}) {
	logFmt(LogLevelInfo, qid, format, args)
}

// Warnf logs a formatted message at Warn level.
func Warnf(qid uint64, format string, args ...interface{}) {
	logFmt(LogLevelWarn, qid, format, args)
}

// Errorf logs a formatted message at Error level.
func Errorf(qid uint64, format string, args ...interface{}) {
	logFmt(LogLevelError, qid, format, args)
}

// ---- Helpers for message content ----

// TruncateText returns s truncated to maxLen with a "...(truncated)" suffix if needed.
func TruncateText(s string, maxLen int) string {
	if maxLen <= 0 || len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "...(truncated)"
}

// HexPreview returns a hex-encoded preview of the first maxBytes of data.
func HexPreview(data []byte, maxBytes int) string {
	if maxBytes <= 0 || len(data) <= maxBytes {
		return hex.EncodeToString(data)
	}
	return hex.EncodeToString(data[:maxBytes]) + "...(truncated)"
}

// ---- internal ----

func logMsg(level LogLevel, qid uint64, msg string) {
	if int32(level) < atomic.LoadInt32(&globalLogLevel) {
		return
	}
	globalLogMu.RLock()
	custom := globalLogger
	globalLogMu.RUnlock()
	if custom != nil {
		custom.Log(level, qid, msg)
		return
	}
	writeLogLine(level, qid, msg)
}

func logFmt(level LogLevel, qid uint64, format string, args []interface{}) {
	if int32(level) < atomic.LoadInt32(&globalLogLevel) {
		return
	}
	globalLogMu.RLock()
	custom := globalLogger
	globalLogMu.RUnlock()
	if custom != nil {
		custom.Log(level, qid, fmt.Sprintf(format, args...))
		return
	}
	writeLogLine(level, qid, fmt.Sprintf(format, args...))
}

// writeLogLine formats and writes one TAOS-format log line.
//
// Format: MM/DD HH:MM:SS.UUUUUU <PID> WSC <LEVEL> QID:0x<hex> <message>\n
func writeLogLine(level LogLevel, qid uint64, msg string) {
	now := time.Now()
	bp := bufPool.Get().(*[]byte)
	buf := (*bp)[:0]

	// MM/DD HH:MM:SS.UUUUUU
	month := int(now.Month())
	day := now.Day()
	hour, min, sec := now.Hour(), now.Minute(), now.Second()
	usec := now.Nanosecond() / 1000

	buf = appendInt2(buf, month)
	buf = append(buf, '/')
	buf = appendInt2(buf, day)
	buf = append(buf, ' ')
	buf = appendInt2(buf, hour)
	buf = append(buf, ':')
	buf = appendInt2(buf, min)
	buf = append(buf, ':')
	buf = appendInt2(buf, sec)
	buf = append(buf, '.')
	buf = appendInt6(buf, usec)

	// THREAD_ID uses current process ID.
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, int64(processID), 10)

	// MOD
	buf = append(buf, ' ', 'W', 'S', 'C')

	// Level
	label := "INFO  "
	idx := int(level)
	if idx >= 0 && idx < len(levelLabel) {
		label = levelLabel[idx]
	}
	buf = append(buf, ' ')
	buf = append(buf, label...)

	// QID:0x<hex>
	buf = append(buf, 'Q', 'I', 'D', ':', '0', 'x')
	buf = appendHex(buf, qid)

	// message
	buf = append(buf, ' ')
	buf = append(buf, msg...)
	buf = append(buf, '\n')

	globalLogMu.RLock()
	w := globalLogWriter
	globalLogMu.RUnlock()
	_, _ = w.Write(buf)

	*bp = buf
	bufPool.Put(bp)
}

func appendInt2(buf []byte, v int) []byte {
	return append(buf, byte('0'+v/10), byte('0'+v%10))
}

func appendInt6(buf []byte, v int) []byte {
	return append(buf,
		byte('0'+v/100000),
		byte('0'+(v/10000)%10),
		byte('0'+(v/1000)%10),
		byte('0'+(v/100)%10),
		byte('0'+(v/10)%10),
		byte('0'+v%10),
	)
}

const hexDigits = "0123456789abcdef"

func appendHex(buf []byte, v uint64) []byte {
	if v == 0 {
		return append(buf, '0')
	}
	started := false
	for shift := 60; shift >= 0; shift -= 4 {
		nibble := (v >> uint(shift)) & 0xf
		if !started {
			if nibble == 0 {
				continue
			}
			started = true
		}
		buf = append(buf, hexDigits[nibble])
	}
	return buf
}
