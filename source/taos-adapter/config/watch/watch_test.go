package watch

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestOnConfigChange(t *testing.T) {
	config.Init()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "config.toml")
	logger := logrus.New().WithField("test", "TestOnConfigChange")
	// file not exists
	OnConfigChange(file, fsnotify.Create, logger)
	// create file, no content
	f, err := os.Create(file)
	require.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
	OnConfigChange(file, fsnotify.Create, logger)
	// invalid reject content
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM [']"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	// valid reject content
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	got := config.Conf.Reject.GetRejectQuerySqlRegex()
	assert.Equal(t, []*regexp.Regexp{regexp.MustCompile("^SELECT * FROM .*")}, got)
	// log level change
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']\n[log]\nlevel = 'debug'"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	got = config.Conf.Reject.GetRejectQuerySqlRegex()
	assert.Equal(t, []*regexp.Regexp{regexp.MustCompile("^SELECT * FROM .*")}, got)
	logLevel := log.GetLogLevel()
	assert.Equal(t, logrus.DebugLevel, logLevel)
	// wrong log level
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']\n[log]\nlevel = 'dbg'"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	got = config.Conf.Reject.GetRejectQuerySqlRegex()
	assert.Equal(t, []*regexp.Regexp{regexp.MustCompile("^SELECT * FROM .*")}, got)
	logLevel = log.GetLogLevel()
	assert.Equal(t, logrus.DebugLevel, logLevel)
}
