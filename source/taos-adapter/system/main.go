package system

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"
	_ "time/tzdata" // load time zone data

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/kardianos/service"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/config/watch"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/tools/watcher"
	"github.com/taosdata/taosadapter/v3/version"
)

var logger = log.GetLogger("SYS")

var testProg *program

func Init() *gin.Engine {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	err := recordsql.Init()
	if err != nil {
		logger.Fatal("record sql init failed: ", err)
	}
	keys := viper.AllKeys()
	sort.Strings(keys)
	logger.Info("                     global config")
	logger.Info("=================================================================")
	for _, key := range keys {
		if key == "version" {
			continue
		}
		v := viper.Get(key)
		if v == "" {
			v = `""`
		}
		logger.Infof("%-45s%v", key, v)
	}
	logger.Infof("%-45s%v", "version", version.Version)
	logger.Infof("%-45s%v", "gitinfo", version.CommitID)
	logger.Infof("%-45s%v", "buildinfo", version.BuildInfo)
	logger.Info("=================================================================")

	logger.Infof("start server: %s", log.ServerID)
	router := createRouter(config.Conf.Debug, config.Conf.Cors, true)
	controllers := controller.GetControllers()
	for _, webController := range controllers {
		webController.Init(router)
	}
	plugin.RegisterGenerateAuth(router)
	plugin.Init(router)
	plugin.Start()
	return router
}

func createRouter(debug bool, corsConf *config.CorsConfig, enableGzip bool) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(log.GinLog())
	router.Use(log.GinRecoverLog())
	if debug {
		//docs.SwaggerInfo.Schemes = []string{"http", "https"}
		//router.GET("/swagger/*any", swagger.WrapHandler(files.Handler))
		pprof.Register(router)
	}
	if enableGzip {
		router.Use(gzip.Gzip(gzip.DefaultCompression, gzip.WithDecompressFn(gzip.DefaultDecompressHandle)))
	}
	router.Use(cors.New(corsConf.GetConfig()))
	return router
}

func Start(router *gin.Engine, startHttpServer func(server *http.Server)) {
	w, err := watcher.NewWatcher(logger, watch.OnConfigChange, config.Conf.ConfigFile)
	if err != nil {
		logger.Fatal("watcher init failed: ", err)
	}
	prg := newProgram(router, startHttpServer, w)
	svcConfig := &service.Config{
		Name:        fmt.Sprintf("%sadapter", version.CUS_PROMPT),
		DisplayName: fmt.Sprintf("%sadapter", version.CUS_PROMPT),
		Description: fmt.Sprintf("%sAdapter is a %s’s companion tool and is a bridge/adapter between %s cluster and application.", version.CUS_PROMPT, version.CUS_NAME, version.CUS_NAME),
	}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		logger.Fatal(err)
	}
	testProg = prg
	err = s.Run()
	if err != nil {
		logger.Fatal(err)
	}
}

type program struct {
	router          *gin.Engine
	server          *http.Server
	startHttpServer func(server *http.Server)
	watcher         *watcher.Watcher
	exit            chan struct{}
}

func newProgram(router *gin.Engine, startHttpServer func(server *http.Server), w *watcher.Watcher) *program {
	server := &http.Server{
		Addr:              ":" + strconv.Itoa(config.Conf.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return &program{
		router:          router,
		server:          server,
		startHttpServer: startHttpServer,
		watcher:         w,
		exit:            make(chan struct{}),
	}
}

func (p *program) Start(s service.Service) error {
	if service.Interactive() {
		logger.Info("Running in terminal.")
	} else {
		logger.Info("Running under service manager.")
	}
	monitor.StartMonitor()
	go p.startHttpServer(p.server)
	go p.registerLoop()
	logger.Infof("server on: %d", config.Conf.Port)
	return nil
}

func (p *program) registerLoop() {
	defer func() {
		logger.Debug("registerLoop exited")
	}()
	duration := config.Conf.Register.Duration
	registerType := fmt.Sprintf("%sadapter", version.CUS_PROMPT)
	instance := config.Conf.Register.Instance
	desc := config.Conf.Register.Description
	expire := int32(config.Conf.Register.Expire)
	doRegister(instance, registerType, desc, expire)
	ticker := time.NewTicker(time.Duration(duration) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			doRegister(instance, registerType, desc, expire)
		case <-p.exit:
			return
		}
	}
}

func doRegister(instance, registerType, desc string, expire int32) {
	logger.Debugf("register instance: %s, type: %s, desc: %s, expire: %d", instance, registerType, desc, expire)
	code := syncinterface.TaosRegisterInstance(instance, registerType, desc, expire, logger, log.IsDebug())
	if code != 0 {
		logger.Errorf("register instance failed, code: %d", code)
	}
	logger.Trace("register instance success")
}

func (p *program) Stop(s service.Service) error {
	logger.Println("Shutdown WebServer ...")
	close(p.exit)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		if err := p.server.Shutdown(ctx); err != nil {
			logger.Println("WebServer Shutdown error:", err)
		}
	}()
	logger.Println("Close Watcher ...")
	err := p.watcher.Close()
	if err != nil {
		logger.Println("Watcher Close error:", err)
	}
	logger.Println("Stop Plugins ...")
	plugin.StopWithCtx(ctx)
	logger.Println("Server exiting")
	recordsql.Close()
	ctxLog, cancelLog := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLog()
	logger.Println("Flushing Log")
	log.Close(ctxLog)
	return nil
}
