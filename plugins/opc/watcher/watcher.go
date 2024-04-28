package watcher

import (
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

type Watcher struct {
	filePath string
	watcher  *fsnotify.Watcher
	logger   *logrus.Entry
	cb       func(file string)
}

func NewWatcher(logger *logrus.Entry, callback func(file string), file string) (*Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	absPath, err := filepath.Abs(filepath.Dir(file))
	if err != nil {
		return nil, err
	}
	fileName := filepath.Base(file)
	filePath := filepath.Join(absPath, fileName)
	err = w.Add(absPath)
	if err != nil {
		return nil, err
	}
	watcher := &Watcher{watcher: w, filePath: filePath, logger: logger, cb: callback}
	go watcher.loop()
	return watcher, nil
}

func (w *Watcher) loop() {
	defer func() {
		w.logger.Info("Watcher loop ended")
	}()
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			if event.Name == w.filePath && (event.Op == fsnotify.Write || event.Op == fsnotify.Create) {
				w.cb(event.Name)
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			w.logger.Error(err)
		}
	}
}

func (w *Watcher) Close() error {
	return w.watcher.Close()
}
