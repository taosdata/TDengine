package config

import (
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Monitor struct {
	Disable                   bool
	CollectDuration           time.Duration
	InCGroup                  bool
	PauseQueryMemoryThreshold float64
	PauseAllMemoryThreshold   float64
	Identity                  string
}

func initMonitor() {
	viper.SetDefault("monitor.disable", true)
	_ = viper.BindEnv("monitor.disable", "TAOS_ADAPTER_MONITOR_DISABLE")
	pflag.Bool("monitor.disable", true, `Whether to disable monitoring. Env "TAOS_ADAPTER_MONITOR_DISABLE"`)

	viper.SetDefault("monitor.collectDuration", time.Second*3)
	_ = viper.BindEnv("monitor.collectDuration", "TAOS_ADAPTER_MONITOR_COLLECT_DURATION")
	pflag.Duration("monitor.collectDuration", time.Second*3, `Set monitor duration. Env "TAOS_ADAPTER_MONITOR_COLLECT_DURATION"`)

	viper.SetDefault("monitor.incgroup", false)
	_ = viper.BindEnv("monitor.incgroup", "TAOS_ADAPTER_MONITOR_INCGROUP")
	pflag.Bool("monitor.incgroup", false, `Whether running in cgroup. Env "TAOS_ADAPTER_MONITOR_INCGROUP"`)

	viper.SetDefault("monitor.pauseQueryMemoryThreshold", 70)
	_ = viper.BindEnv("monitor.pauseQueryMemoryThreshold", "TAOS_ADAPTER_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD")
	pflag.Float64("monitor.pauseQueryMemoryThreshold", 70, `Memory percentage threshold for pause query. Env "TAOS_ADAPTER_MONITOR_PAUSE_QUERY_MEMORY_THRESHOLD"`)

	viper.SetDefault("monitor.pauseAllMemoryThreshold", 80)
	_ = viper.BindEnv("monitor.pauseAllMemoryThreshold", "TAOS_ADAPTER_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD")
	pflag.Float64("monitor.pauseAllMemoryThreshold", 80, `Memory percentage threshold for pause all. Env "TAOS_ADAPTER_MONITOR_PAUSE_ALL_MEMORY_THRESHOLD"`)

	viper.SetDefault("monitor.identity", "")
	_ = viper.BindEnv("monitor.identity", "TAOS_ADAPTER_MONITOR_IDENTITY")
	pflag.String("monitor.identity", "", `The identity of the current instance, or 'hostname:port' if it is empty. Env "TAOS_ADAPTER_MONITOR_IDENTITY"`)
}

func (p *Monitor) setValue() {
	p.Disable = viper.GetBool("monitor.disable")
	p.CollectDuration = viper.GetDuration("monitor.collectDuration")
	p.InCGroup = viper.GetBool("monitor.incgroup")
	p.PauseQueryMemoryThreshold = viper.GetFloat64("monitor.pauseQueryMemoryThreshold")
	p.PauseAllMemoryThreshold = viper.GetFloat64("monitor.pauseAllMemoryThreshold")
	p.Identity = viper.GetString("monitor.identity")
}
