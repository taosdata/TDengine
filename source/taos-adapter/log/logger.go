package log

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/version"
)

var logger = logrus.New()
var ServerID = randomID()
var globalLogFormatter = &TaosLogFormatter{}
var finish = make(chan struct{})
var exit = make(chan struct{})

var bufferPool = &defaultPool{
	pool: &sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	},
}

type defaultPool struct {
	pool *sync.Pool
}

func (p *defaultPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.pool.Put(buf)
}

func (p *defaultPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

type FileHook struct {
	formatter logrus.Formatter
	writer    io.Writer
	buf       *bytes.Buffer
	sync.Mutex
}

func NewFileHook(formatter logrus.Formatter, writer io.WriteCloser) *FileHook {
	fh := &FileHook{formatter: formatter, writer: writer, buf: &bytes.Buffer{}}
	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			select {
			case <-ticker.C:
				//can be optimized by tryLock
				fh.Lock()
				if fh.buf.Len() > 0 {
					// flush log ignore error, because it have been printed to stderr
					_ = fh.flush()
				}
				fh.Unlock()
			case <-exit:
				fh.Lock()
				_ = fh.flush()
				fh.Unlock()
				err := writer.Close()
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, "close log file error:", err)
				}
				ticker.Stop()
				close(finish)
				return
			}
		}
	}()
	return fh
}

func (f *FileHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (f *FileHook) Fire(entry *logrus.Entry) error {
	if entry.Buffer == nil {
		entry.Buffer = bufferPool.Get()
		defer func() {
			bufferPool.Put(entry.Buffer)
			entry.Buffer = nil
		}()
	}
	data, err := f.formatter.Format(entry)
	if err != nil {
		return err
	}
	f.Lock()
	f.buf.Write(data)
	if f.buf.Len() > 1024 || entry.Level == logrus.FatalLevel || entry.Level == logrus.PanicLevel {
		err = f.flush()
	}
	f.Unlock()
	return err
}

func (f *FileHook) flush() error {
	_, err := f.writer.Write(f.buf.Bytes())
	f.buf.Reset()
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "write log error:", err)
	}
	return err
}

var once sync.Once

func ConfigLog() {
	once.Do(func() {
		err := SetLevel(config.Conf.LogLevel)
		if err != nil {
			panic(err)
		}
		writer, err := rotatelogs.New(
			filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapter_%d_%%Y%%m%%d.log", version.CUS_PROMPT, config.Conf.InstanceID)),
			rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
			rotatelogs.WithRotationTime(time.Hour*24),
			rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
			rotatelogs.WithReservedDiskSize(int64(config.Conf.Log.ReservedDiskSize)),
			rotatelogs.WithRotateGlobPattern(filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapter_%d_*.log*", version.CUS_PROMPT, config.Conf.InstanceID))),
			rotatelogs.WithCompress(config.Conf.Log.Compress),
			rotatelogs.WithCleanLockFile(filepath.Join(config.Conf.Log.Path, fmt.Sprintf(".%sadapter_%d_rotate_lock", version.CUS_PROMPT, config.Conf.InstanceID))),
			rotatelogs.ForceNewFile(),
			rotatelogs.WithMaxAge(time.Hour*24*time.Duration(config.Conf.Log.KeepDays)),
		)
		if err != nil {
			panic(err)
		}
		_, _ = fmt.Fprintln(writer, "==================================================")
		_, _ = fmt.Fprintln(writer, "                new log file")
		_, _ = fmt.Fprintln(writer, "==================================================")
		hook := NewFileHook(globalLogFormatter, writer)
		logger.AddHook(hook)
		if config.Conf.Log.EnableRecordHttpSql {
			sqlWriter, err := rotatelogs.New(
				filepath.Join(config.Conf.Log.Path, fmt.Sprintf("httpsql_%d_%%Y%%m%%d%%H%%M.log", config.Conf.InstanceID)),
				rotatelogs.WithRotationCount(config.Conf.Log.SqlRotationCount),
				rotatelogs.WithRotationTime(config.Conf.Log.SqlRotationTime),
				rotatelogs.WithRotationSize(int64(config.Conf.Log.SqlRotationSize)),
				rotatelogs.WithReservedDiskSize(int64(config.Conf.Log.ReservedDiskSize)),
				rotatelogs.WithRotateGlobPattern(filepath.Join(config.Conf.Log.Path, fmt.Sprintf("httpsql_%d_*.log*", config.Conf.InstanceID))),
				rotatelogs.WithCompress(config.Conf.Log.Compress),
				rotatelogs.WithCleanLockFile(filepath.Join(config.Conf.Log.Path, fmt.Sprintf(".httpsql_%d_rotate_lock", config.Conf.InstanceID))),
				rotatelogs.ForceNewFile(),
				rotatelogs.WithMaxAge(time.Hour*24*time.Duration(config.Conf.Log.KeepDays)),
			)
			if err != nil {
				panic(err)
			}
			sqlLogger.SetFormatter(&TaosSqlLogFormatter{})
			sqlLogger.SetOutput(sqlWriter)
		}
	})
}

func SetLevel(level string) error {
	l, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logger.SetLevel(l)
	return nil
}

func GetLogger(model string) *logrus.Entry {
	return logger.WithFields(logrus.Fields{config.ModelKey: model})
}

func init() {
	logrus.SetBufferPool(bufferPool)
	logger.SetFormatter(globalLogFormatter)
	setLoggerOutPut(logger)
}

const DisableDisplayLogEnv = "GO_TEST_DISABLE_DISPLAY_LOG"

func setLoggerOutPut(logger *logrus.Logger) {
	if _, exists := os.LookupEnv(DisableDisplayLogEnv); exists {
		logger.SetOutput(io.Discard)
	} else {
		logger.SetOutput(os.Stdout)
	}
}

func randomID() string {
	return fmt.Sprintf("%08d", os.Getpid())
}

type TaosLogFormatter struct {
}

func (t *TaosLogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	b.Reset()
	b.WriteString(entry.Time.Format("01/02 15:04:05.000000"))
	b.WriteByte(' ')
	b.WriteString(ServerID)
	b.WriteByte(' ')
	v, exist := entry.Data[config.ModelKey]
	if exist && v != nil {
		b.WriteString(v.(string))
		b.WriteByte(' ')
	} else {
		b.WriteString("CLI ")
	}
	switch entry.Level {
	case logrus.PanicLevel:
		b.WriteString("PANIC ")
	case logrus.FatalLevel:
		b.WriteString("FATAL ")
	case logrus.ErrorLevel:
		b.WriteString("ERROR ")
	case logrus.WarnLevel:
		b.WriteString("WARN  ")
	case logrus.InfoLevel:
		b.WriteString("INFO  ")
	case logrus.DebugLevel:
		b.WriteString("DEBUG ")
	case logrus.TraceLevel:
		b.WriteString("TRACE ")
	}

	// ws session id
	v, exist = entry.Data[config.SessionIDKey]
	if exist && v != nil {
		b.WriteString(config.SessionIDKey)
		b.WriteByte(':')
		fmt.Fprintf(b, "0x%x, ", v)
	}

	// request id
	v, exist = entry.Data[config.ReqIDKey]
	if exist && v != nil {
		b.WriteString(config.ReqIDKey)
		b.WriteByte(':')
		fmt.Fprintf(b, "0x%x ", v)
	}
	if len(entry.Message) > 0 && entry.Message[len(entry.Message)-1] == '\n' {
		b.WriteString(entry.Message[:len(entry.Message)-1])
	} else {
		b.WriteString(entry.Message)
	}
	// sort the keys
	keys := make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		if k == config.ModelKey || k == config.SessionIDKey || k == config.ReqIDKey {
			continue
		}
		keys = append(keys, k)
	}
	for _, k := range keys {
		value := entry.Data[k]
		b.WriteString(", ")
		b.WriteString(k)
		b.WriteByte(':')
		fmt.Fprintf(b, "%v", value)
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func IsDebug() bool {
	return logger.IsLevelEnabled(logrus.DebugLevel)
}

func GetLogLevel() logrus.Level {
	return logger.Level
}

var zeroTime = time.Time{}
var zeroDuration = time.Duration(0)

func GetLogNow(isDebug bool) time.Time {
	if isDebug {
		return time.Now()
	}
	return zeroTime
}
func GetLogDuration(isDebug bool, s time.Time) time.Duration {
	if isDebug {
		return time.Since(s)
	}
	return zeroDuration
}

const MaxLogSqlLength = 1024

func GetLogSql(sql string) string {
	if len(sql) > MaxLogSqlLength {
		return sql[:MaxLogSqlLength] + "...(truncated)"
	}
	return sql
}

func Close(ctx context.Context) {
	close(exit)
	select {
	case <-finish:
		return
	case <-ctx.Done():
		return
	}
}
