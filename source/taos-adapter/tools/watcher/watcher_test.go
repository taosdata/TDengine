package watcher

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWatcher(t *testing.T) {
	tmpDir := t.TempDir()
	f, err := os.CreateTemp(tmpDir, "test")
	assert.NoError(t, err)
	defer func() {
		err = f.Close()
		assert.NoError(t, err)
	}()
	cb := func(file string, op fsnotify.Op, _ *logrus.Entry) {
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
	defer func() {
		err = f.Close()
		assert.NoError(t, err)
	}()
	gotChange := make(chan struct{})
	cb := func(file string, op fsnotify.Op, _ *logrus.Entry) {
		t.Log(file, op)
		gotChange <- struct{}{}
	}
	logger := logrus.New().WithField("test", "watcher")
	w, err := NewWatcher(logger, cb, f.Name())
	assert.NoError(t, err)
	// write file
	err = os.WriteFile(f.Name(), []byte("test"), 0644)
	assert.NoError(t, err)
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("did not get change notification")
	case <-gotChange:
		assert.Equal(t, 0, len(gotChange))
	}

	select {
	case <-time.After(1 * time.Second):
	case <-gotChange:
		t.Fatal("should not get change notification after close")
	}
	// move file
	newPath := f.Name() + "_new"
	err = os.WriteFile(newPath, []byte("test_new"), 0644)
	require.NoError(t, err)
	err = os.Rename(newPath, f.Name())
	require.NoError(t, err)
	select {
	case <-time.After(2 * time.Second):
		t.Fatal("did not get change notification")
	case <-gotChange:
		assert.Equal(t, 0, len(gotChange))
		bs, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		assert.Equal(t, "test_new", string(bs))
	}
	err = w.Close()
	assert.NoError(t, err)
	timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer func() {
		cancel()
	}()
	select {
	case <-timeout.Done():
		t.Error("timed out")
	case <-w.exit:
	}
	_, ok := <-w.watcher.Errors
	assert.False(t, ok)
	_, ok = <-w.watcher.Events
	assert.False(t, ok)
}

func TestWatcherloopExit(t *testing.T) {
	w := &Watcher{
		logger: logrus.New().WithField("test", "watcher"),
		exit:   make(chan struct{}),
		watcher: &fsnotify.Watcher{
			Errors: make(chan error),
			Events: make(chan fsnotify.Event),
		},
	}
	go w.loop()
	time.Sleep(time.Millisecond * 10)
	close(w.watcher.Errors)
	<-w.exit

	w = &Watcher{
		logger: logrus.New().WithField("test", "watcher"),
		exit:   make(chan struct{}),
		watcher: &fsnotify.Watcher{
			Errors: make(chan error),
			Events: make(chan fsnotify.Event),
		},
	}
	go w.loop()
	time.Sleep(time.Millisecond * 10)
	close(w.watcher.Events)
	<-w.exit

	w = &Watcher{
		logger: logrus.New().WithField("test", "watcher"),
		exit:   make(chan struct{}),
		watcher: &fsnotify.Watcher{
			Errors: make(chan error),
			Events: make(chan fsnotify.Event),
		},
	}
	go w.loop()
	time.Sleep(time.Millisecond * 10)
	w.watcher.Errors <- fmt.Errorf("test error")
	close(w.watcher.Errors)
	<-w.exit
}
