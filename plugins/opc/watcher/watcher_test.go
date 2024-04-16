package watcher

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNewWatcher(t *testing.T) {
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "test")
	assert.NoError(t, err)
	defer f.Close()
	cb := func(file string) {
		t.Log(file)
	}
	logger := logrus.New().WithField("test", "watcher")
	w, err := NewWatcher(logger, cb, f.Name())
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)
}

func TestWatcher_loop(t *testing.T) {
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "test")
	assert.NoError(t, err)
	defer f.Close()
	cb := func(file string) {
		t.Log(file)
	}
	logger := logrus.New().WithField("test", "watcher")
	w, err := NewWatcher(logger, cb, f.Name())
	assert.NoError(t, err)
	go w.loop()
	err = os.WriteFile(f.Name(), []byte("test"), 0644)
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)
}
