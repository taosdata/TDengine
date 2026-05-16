package telegraflog

import (
	"github.com/influxdata/telegraf"
	"github.com/sirupsen/logrus"
)

type WrapperLogger struct {
	logger *logrus.Entry
}

func NewWrapperLogger(logger *logrus.Entry) *WrapperLogger {
	return &WrapperLogger{
		logger: logger,
	}
}

func (w *WrapperLogger) Errorf(format string, args ...interface{}) {
	w.logger.Errorf(format, args...)
}

func (w *WrapperLogger) Error(args ...interface{}) {
	w.logger.Error(args...)
}

func (w *WrapperLogger) Warnf(format string, args ...interface{}) {
	w.logger.Warnf(format, args...)
}

func (w *WrapperLogger) Warn(args ...interface{}) {
	w.logger.Warn(args...)
}

func (w *WrapperLogger) Infof(format string, args ...interface{}) {
	w.logger.Infof(format, args...)
}

func (w *WrapperLogger) Info(args ...interface{}) {
	w.logger.Info(args...)
}

func (w *WrapperLogger) Debugf(format string, args ...interface{}) {
	w.logger.Debugf(format, args...)
}

func (w *WrapperLogger) Debug(args ...interface{}) {
	w.logger.Debug(args...)
}

func (w *WrapperLogger) Tracef(format string, args ...interface{}) {
	w.logger.Tracef(format, args...)
}

func (w *WrapperLogger) Trace(args ...interface{}) {
	w.logger.Trace(args...)
}

func (w *WrapperLogger) Level() telegraf.LogLevel {
	switch w.logger.Logger.Level {
	case logrus.PanicLevel:
		return telegraf.Error
	case logrus.FatalLevel:
		return telegraf.Error
	case logrus.ErrorLevel:
		return telegraf.Error
	case logrus.WarnLevel:
		return telegraf.Warn
	case logrus.InfoLevel:
		return telegraf.Info
	case logrus.DebugLevel:
		return telegraf.Debug
	case logrus.TraceLevel:
		return telegraf.Trace
	}
	return telegraf.None
}

func (w *WrapperLogger) AddAttribute(key string, value interface{}) {
	w.logger = w.logger.WithField(key, value)
}
