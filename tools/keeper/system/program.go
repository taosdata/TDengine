package system

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/kardianos/service"
	"github.com/taosdata/go-utils/web"
	"github.com/taosdata/taoskeeper/api"
	"github.com/taosdata/taoskeeper/cmd"
	"github.com/taosdata/taoskeeper/infrastructure/config"
	"github.com/taosdata/taoskeeper/infrastructure/log"
	"github.com/taosdata/taoskeeper/monitor"
	"github.com/taosdata/taoskeeper/process"
	"github.com/taosdata/taoskeeper/version"
)

var logger = log.GetLogger("PRG")

// Global memoryStore reference for cleanup on shutdown
var memoryStore *process.MemoryStore

func Init() *http.Server {
	conf := config.InitConfig()
	log.ConfigLog()

	if len(conf.Transfer) > 0 || len(conf.Drop) > 0 {
		cmd := cmd.NewCommand(conf)
		cmd.Process(conf)
		os.Exit(0)
		return nil
	}

	router := CreateRouter(false, &conf.Cors)
	router.Use(log.GinLog())
	router.Use(log.GinRecoverLog())

	reporter := api.NewReporter(conf)
	reporter.Init(router)
	monitor.StartMonitor(conf.Metrics.Cluster, conf, reporter)

	// v2: Create memory store and parser (must be before route registration)
	// Use configurable TTL from config file (convert seconds to duration)
	var err error
	memoryStore, err = process.NewMemoryStore(time.Duration(conf.Prometheus.CacheTTL) * time.Second)
	if err != nil {
		logger.Errorf("Failed to create memory store: %v", err)
		panic(err)
	}
	metricParser := api.NewMetricParser(memoryStore, conf.Prometheus.IncludeTables)
	router.Use(api.MetricCacheMiddleware(metricParser))
	if len(conf.Prometheus.IncludeTables) > 0 {
		logger.Infof("Memory cache mode (v2) enabled, additional tables: %v", conf.Prometheus.IncludeTables)
	} else {
		logger.Info("Memory cache mode (v2) enabled")
	}

	go func() {
		// wait for monitor to all metric received
		time.Sleep(time.Second * 35)

		processor := process.NewProcessor(conf)
		node := api.NewNodeExporter(processor, memoryStore, reporter)
		node.Init(router)

		if version.IsEnterprise == "true" {
			zabbix := api.NewZabbix(processor)
			zabbix.Init(router)
		}
	}()

	checkHealth := api.NewCheckHealth(version.Version)
	checkHealth.Init(router)

	if version.IsEnterprise == "true" {
		if conf.Audit.Enable {
			audit := api.NewAudit(conf)
			audit.Init(router)
		}
	}

	gen_metric := api.NewGeneralMetric(conf)
	if err := gen_metric.Init(router); err != nil {
		panic(err)
	}

	adapter := api.NewAdapter(conf, gen_metric)
	if err := adapter.Init(router); err != nil {
		panic(err)
	}

	server := &http.Server{
		Addr:              conf.Host + ":" + strconv.Itoa(conf.Port),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return server
}

func Start(server *http.Server) {
	prg := newProgram(server)
	svcConfig := &service.Config{
		Name:        "taoskeeper",
		DisplayName: "taoskeeper",
		Description: "taosKeeper is a tool for TDengine that exports monitoring metrics",
	}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		logger.Fatal(err)
	}
	if err := s.Run(); err != nil {
		logger.Fatal(err)
	}
}

type program struct {
	server *http.Server
}

func newProgram(server *http.Server) *program {
	return &program{server: server}
}

func (p *program) Start(s service.Service) error {
	if service.Interactive() {
		logger.Info("Running in terminal.")
	} else {
		logger.Info("Running under service manager.")
	}

	server := p.server
	go func() {
		fail := func(err error) {
			if err == nil || err == http.ErrServerClosed {
				return
			}
			logger.Errorf("taoskeeper start up fail: %v", err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			log.Close(ctx)
			cancel()
			os.Exit(1)
		}

		if ssl := config.Conf.SSL; ssl.Enable {
			logger.Infof("Starting HTTPS service at %s", server.Addr)
			fail(server.ListenAndServeTLS(ssl.CertFile, ssl.KeyFile))
		} else {
			logger.Infof("Starting HTTP service at %s", server.Addr)
			fail(server.ListenAndServe())
		}
	}()
	return nil
}

func (p *program) Stop(s service.Service) error {
	logger.Println("Shutdown WebServer ...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.server.Shutdown(ctx); err != nil {
		logger.Println("WebServer Shutdown error:", err)
	}

	// Stop memoryStore cleanup goroutine
	if memoryStore != nil {
		logger.Println("Stopping memory store cleanup goroutine")
		memoryStore.Close()
	}

	logger.Println("Server exiting")
	ctxLog, cancelLog := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLog()
	logger.Println("Flushing Log")
	log.Close(ctxLog)
	return nil
}

func CreateRouter(debug bool, corsConf *web.CorsConfig) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(cors.New(corsConf.GetConfig()))
	return router
}
