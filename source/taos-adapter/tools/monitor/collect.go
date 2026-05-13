package monitor

import (
	"math"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
)

type ProcessInterface interface {
	Percent(interval time.Duration) (float64, error)
	MemoryPercent() (float32, error)
	MemoryInfo() (*process.MemoryInfoStat, error)
}

type SysCollector interface {
	CpuPercent() (float64, error)
	MemPercent() (float64, error)
}

type NormalCollector struct {
	p ProcessInterface
}

func NewNormalCollector() (*NormalCollector, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	return &NormalCollector{p: p}, nil
}

func (n *NormalCollector) CpuPercent() (float64, error) {
	cpuPercent, err := n.p.Percent(0)
	if err != nil {
		return 0, err
	}
	return cpuPercent / float64(runtime.NumCPU()), nil
}

func (n *NormalCollector) MemPercent() (float64, error) {
	memPercent, err := n.p.MemoryPercent()
	if err != nil {
		return 0, err
	}
	return float64(memPercent), nil
}

const (
	CGroupCpuQuotaPath  = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
	CGroupCpuPeriodPath = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
	CGroupMemLimitPath  = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
)

type CGProcessInterface interface {
	Percent(interval time.Duration) (float64, error)
	MemoryInfo() (*process.MemoryInfoStat, error)
}

type CGroupCollector struct {
	p           CGProcessInterface
	cpuCore     float64
	totalMemory uint64
}

type ReadUintFunc func(path string) (uint64, error)

func NewCGroupCollector(readUint ReadUintFunc) (*CGroupCollector, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	cpuPeriod, err := readUint(CGroupCpuPeriodPath)
	if err != nil {
		return nil, err
	}
	cpuQuota, err := readUint(CGroupCpuQuotaPath)
	if err != nil {
		return nil, err
	}
	cpuCore := float64(cpuQuota) / float64(cpuPeriod)
	limitMemory, err := readUint(CGroupMemLimitPath)
	if err != nil {
		return nil, err
	}
	machineMemory, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}
	totalMemory := uint64(math.Min(float64(limitMemory), float64(machineMemory.Total)))
	return &CGroupCollector{p: p, cpuCore: cpuCore, totalMemory: totalMemory}, nil
}

func (c *CGroupCollector) CpuPercent() (float64, error) {
	cpuPercent, err := c.p.Percent(0)
	if err != nil {
		return 0, err
	}
	cpuPercent = cpuPercent / c.cpuCore
	return cpuPercent, nil
}

func (c *CGroupCollector) MemPercent() (float64, error) {
	memInfo, err := c.p.MemoryInfo()
	if err != nil {
		return 0, err
	}
	return 100 * float64(memInfo.RSS) / float64(c.totalMemory), nil
}
