package telegraflog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/influxdata/telegraf"
	"github.com/sirupsen/logrus"
)

func newTestLogger() (*WrapperLogger, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	baseLogger := logrus.New()
	baseLogger.SetOutput(buf)
	baseLogger.SetFormatter(&logrus.TextFormatter{DisableTimestamp: true, DisableColors: true})
	entry := logrus.NewEntry(baseLogger)
	return NewWrapperLogger(entry), buf
}

func TestWrapperLoggerLogsAllLevels(t *testing.T) {
	logger, buf := newTestLogger()
	logger.logger.Logger.SetLevel(logrus.TraceLevel)
	logger.Errorf("errorf: %s", "err")
	logger.Error("error")
	logger.Warnf("warnf: %s", "warn")
	logger.Warn("warn")
	logger.Infof("infof: %s", "info")
	logger.Info("info")
	logger.Debugf("debugf: %s", "debug")
	logger.Debug("debug")
	logger.Tracef("tracef: %s", "trace")
	logger.Trace("trace")

	logs := buf.String()
	for _, want := range []string{
		"errorf: err", "error",
		"warnf: warn", "warn",
		"infof: info", "info",
		"debugf: debug", "debug",
		"tracef: trace", "trace",
	} {
		if !strings.Contains(logs, want) {
			t.Errorf("log output missing: %s", want)
		}
	}
}

func TestWrapperLoggerAddAttribute(t *testing.T) {
	logger, buf := newTestLogger()
	logger.AddAttribute("key", "value")
	logger.Info("with attribute")
	logs := buf.String()
	if !strings.Contains(logs, "key=value") {
		t.Error("attribute not found in log output")
	}
}

func TestWrapperLoggerLevelMapping(t *testing.T) {
	logger, _ := newTestLogger()
	for _, tc := range []struct {
		setLevel  logrus.Level
		wantLevel telegraf.LogLevel
	}{
		{logrus.PanicLevel, telegraf.Error},
		{logrus.FatalLevel, telegraf.Error},
		{logrus.ErrorLevel, telegraf.Error},
		{logrus.WarnLevel, telegraf.Warn},
		{logrus.InfoLevel, telegraf.Info},
		{logrus.DebugLevel, telegraf.Debug},
		{logrus.TraceLevel, telegraf.Trace},
		{100, telegraf.None},
	} {
		logger.logger.Logger.SetLevel(tc.setLevel)
		got := logger.Level()
		if got != tc.wantLevel {
			t.Errorf("for logrus level %v, want telegraf level %v, got %v", tc.setLevel, tc.wantLevel, got)
		}
	}
}
