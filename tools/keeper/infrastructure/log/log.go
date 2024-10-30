package log

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	rotatelogs "github.com/huskar-t/file-rotatelogs/v2"
	"github.com/sirupsen/logrus"
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

func NewFileHook(formatter logrus.Formatter, writer io.Writer) *FileHook {
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
			path.Join(config.Conf.Log.Path, fmt.Sprintf("%skeeper_%%Y_%%m_%%d_%%H_%%M.log", version.CUS_PROMPT)),
			rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
			rotatelogs.WithRotationTime(config.Conf.Log.RotationTime),
			rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
		)
		if err != nil {
			panic(err)
		}
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
	return logger.WithFields(logrus.Fields{"model": model})
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
	b.WriteString(entry.Level.String())
	b.WriteString(` "`)
	b.WriteString(entry.Message)
	b.WriteByte('"')
	for k, v := range entry.Data {
		b.WriteByte(' ')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(fmt.Sprintf("%v", v))
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

func IsDebug() bool {
	return logger.IsLevelEnabled(logrus.DebugLevel)
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
