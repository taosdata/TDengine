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
	"github.com/taosdata/taoskeeper/infrastructure/config"

	"github.com/taosdata/taoskeeper/version"
)

var logger = logrus.New()
var ServerID = randomID()
var globalLogFormatter = &TaosLogFormatter{}
var finish = make(chan struct{})
var exist = make(chan struct{})

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
					fh.flush()
				}
				fh.Unlock()
			case <-exist:
				fh.Lock()
				fh.flush()
				fh.Unlock()
				writer.Close()
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
			filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%skeeper_%d_%%Y%%m%%d.log", version.CUS_PROMPT, config.Conf.InstanceID)),
			rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
			rotatelogs.WithRotationTime(time.Hour*24),
			rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
			rotatelogs.WithReservedDiskSize(int64(config.Conf.Log.ReservedDiskSize)),
			rotatelogs.WithRotateGlobPattern(filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%skeeper_%d_*.log*", version.CUS_PROMPT, config.Conf.InstanceID))),
			rotatelogs.WithCompress(config.Conf.Log.Compress),
			rotatelogs.WithCleanLockFile(filepath.Join(config.Conf.Log.Path, fmt.Sprintf(".%skeeper_%d_rotate_lock", version.CUS_PROMPT, config.Conf.InstanceID))),
			rotatelogs.ForceNewFile(),
			rotatelogs.WithMaxAge(time.Hour*24*time.Duration(config.Conf.Log.KeepDays)),
		)
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(writer, "==================================================")
		fmt.Fprintln(writer, "                new log file")
		fmt.Fprintln(writer, "==================================================")
		fmt.Fprintf(writer, "config:%+v\n", config.Conf)

		fmt.Fprintf(writer, "%-45s%v\n", "version", version.Version)
		fmt.Fprintf(writer, "%-45s%v\n", "gitinfo", version.CommitID)
		fmt.Fprintf(writer, "%-45s%v\n", "buildinfo", version.BuildInfo)

		hook := NewFileHook(globalLogFormatter, writer)
		logger.AddHook(hook)
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
	logger.SetOutput(os.Stdout)
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
		if k == config.ModelKey || k == config.ReqIDKey {
			continue
		}
		keys = append(keys, k)
	}
	for _, k := range keys {
		v := entry.Data[k]
		if k == config.ReqIDKey && v == nil {
			continue
		}
		b.WriteString(", ")
		b.WriteString(k)
		b.WriteByte(':')
		fmt.Fprintf(b, "%v", v)
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

func Close(ctx context.Context) {
	close(exist)
	select {
	case <-finish:
		return
	case <-ctx.Done():
		return
	}
}
