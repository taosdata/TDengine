package plugin

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/log"
)

var logger = log.GetLogger("PLG")

type Plugin interface {
	Init(r gin.IRouter) error
	Start() error
	Stop() error
	String() string
	Version() string
}

var plugins = map[string]Plugin{}

func Register(plugin Plugin) {
	name := fmt.Sprintf("%s/%s", plugin.String(), plugin.Version())
	if _, ok := plugins[name]; ok {
		logger.Panicf("duplicate registration of plugin %s", name)
	}
	plugins[name] = plugin
}

func Init(r gin.IRouter) {
	for name, plugin := range plugins {
		logger.Debugf("init plugin %s", name)
		router := r.Group(name)
		err := plugin.Init(router)
		if err != nil {
			logger.WithError(err).Panicf("init plugin %s", name)
		}
	}
	logger.Debug("all plugin init finish")
}

func Start() {
	for name, plugin := range plugins {
		err := plugin.Start()
		if err != nil {
			logger.WithError(err).Panicf("start plugin %s", name)
		}
	}
	logger.Debug("all plugin start finish")
}

func Stop() {
	for name, plugin := range plugins {
		err := plugin.Stop()
		if err != nil {
			logger.WithError(err).Warnf("stop plugin %s", name)
		}
	}
}

func StopWithCtx(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		Stop()
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}
}
