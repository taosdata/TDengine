# log Package Guide

`github.com/taosdata/driver-go/v3/log` provides global logging for the driver.
By default, logging is disabled (`LogLevelSilent`).

## 1. Enable built-in logging quickly

```go
package main

import (
	"os"

	tLog "github.com/taosdata/driver-go/v3/log"
)

func init() {
	tLog.SetOutput(os.Stdout)        // default is os.Stderr
	tLog.SetLevel(tLog.LogLevelInfo) // enable logs
}
```

## 2. Plug in your own logging framework

The `log` package exposes a `Logger` interface. Implement it and call `SetLogger`:

```go
package main

import (
	"fmt"

	tLog "github.com/taosdata/driver-go/v3/log"
)

type MyLogger struct{}

func (l *MyLogger) Log(level tLog.LogLevel, qid uint64, msg string) {
	// Replace this with your logger call (zap/logrus/slog, etc.)
	fmt.Printf("level=%d qid=0x%x msg=%s\n", level, qid, msg)
}

func init() {
	tLog.SetLogger(&MyLogger{})
	tLog.SetLevel(tLog.LogLevelDebug)
}
```

Notes:

- After `SetLogger(custom)`, built-in TAOS formatting is fully bypassed.
- `SetOutput` no longer takes effect while a custom logger is set.
- Call `SetLogger(nil)` to switch back to built-in mode.
- Your custom `Logger` implementation must be concurrency-safe.

### 2.1 Example: integrate with `log/slog` (Go 1.21+)

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	tLog "github.com/taosdata/driver-go/v3/log"
)

type SlogAdapter struct {
	logger *slog.Logger
}

func (a *SlogAdapter) Log(level tLog.LogLevel, qid uint64, msg string) {
	a.logger.LogAttrs(
		context.Background(),
		toSlogLevel(level),
		msg,
		slog.String("qid", "0x"+strconv.FormatUint(qid, 16)),
	)
}

func toSlogLevel(level tLog.LogLevel) slog.Level {
	switch level {
	case tLog.LogLevelDebug:
		return slog.LevelDebug
	case tLog.LogLevelInfo:
		return slog.LevelInfo
	case tLog.LogLevelWarn:
		return slog.LevelWarn
	case tLog.LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func init() {
	base := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	tLog.SetLogger(&SlogAdapter{logger: base})
	tLog.SetLevel(tLog.LogLevelInfo)
}
```

If your application is on Go < 1.21, use another logger adapter (for example zap/logrus), or upgrade and use `log/slog`.

## 3. Configurable methods and what they do

| Method | Purpose | Default | Notes |
| --- | --- | --- | --- |
| `SetLevel(level LogLevel)` | Sets global log filtering level | `LogLevelSilent` | Global setting; usually set once at startup |
| `SetOutput(w io.Writer)` | Sets output target for built-in logger | `os.Stderr` | Effective only when no custom logger is installed; `w=nil` resets to `os.Stderr` |
| `SetLogger(l Logger)` | Installs/switches custom logger | `nil` (built-in mode) | `l=nil` switches back to built-in mode |
| `SetPacketLogging(enabled bool)` | Enables packet content logging for WebSocket send/receive | `false` | Controls packet-content logs only; actual output is still filtered by `SetLevel` |
| `SetMaxPacketLogBytes(n int)` | Limits max bytes of packet content in logs | `512` | `n<=0` means unlimited; binary is hex preview, text is truncated |

Common read/check helpers (optional):

- `GetLevel()`: returns current log level.
- `IsPacketLoggingEnabled()`: returns packet logging switch.
- `GetMaxPacketLogBytes()`: returns packet preview size.
- `IsDebugEnabled()` / `IsInfoEnabled()`: guard expensive log argument construction.

## 4. Recommended configuration order

1. Choose output mode first: `SetLogger(custom)` or `SetOutput(writer)`.
2. Set level: `SetLevel(...)`.
3. For temporary packet troubleshooting, enable `SetPacketLogging(true)` and set `SetMaxPacketLogBytes(...)`, then make sure `SetLevel` is visible (for example `LogLevelInfo`).

## 5. Notes

- This package uses global settings and affects driver logging process-wide.
- Configuration and log writes are safe for concurrent use.
- Packet logging includes redaction and truncation, but in production you should still enable it carefully and keep level/length controlled.
