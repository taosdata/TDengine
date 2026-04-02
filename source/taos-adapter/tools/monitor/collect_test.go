package monitor

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockProcess implements ProcessInterface
type MockProcess struct {
	cpuPercent float64
	memPercent float32
	memInfo    *process.MemoryInfoStat
	errCpu     error
	errMem     error
}

func (m *MockProcess) Percent(_ time.Duration) (float64, error) {
	return m.cpuPercent, m.errCpu
}

func (m *MockProcess) MemoryPercent() (float32, error) {
	return m.memPercent, m.errMem
}

func (m *MockProcess) MemoryInfo() (*process.MemoryInfoStat, error) {
	return m.memInfo, m.errMem
}

func TestNormalCollector(t *testing.T) {
	tests := []struct {
		name         string
		cpuPercent   float64
		memPercent   float32
		mockErrCpu   error
		mockErrMem   error
		expectedCpu  float64
		expectedMem  float64
		expectCpuErr bool
		expectMemErr bool
	}{
		{
			name:        "Success",
			cpuPercent:  5000,
			memPercent:  2048,
			expectedCpu: 5000 / float64(runtime.NumCPU()),
			expectedMem: 2048,
		},
		{
			name:         "CPU Error",
			mockErrCpu:   errors.New("cpu error"),
			expectCpuErr: true,
		},
		{
			name:         "Memory Error",
			mockErrMem:   errors.New("memory error"),
			expectMemErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProcess := &MockProcess{
				cpuPercent: tt.cpuPercent,
				memPercent: tt.memPercent,
				errCpu:     tt.mockErrCpu,
				errMem:     tt.mockErrMem,
			}
			normalCollector := &NormalCollector{p: mockProcess}

			cpu, err := normalCollector.CpuPercent()
			if tt.expectCpuErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedCpu, cpu)
			}

			mem, err := normalCollector.MemPercent()
			if tt.expectMemErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMem, mem)
			}
		})
	}
}

// Define a new mock struct for testing
type MockProcessForCGroup struct {
	mock.Mock
}

func (m *MockProcessForCGroup) Percent(interval time.Duration) (float64, error) {
	args := m.Called(interval)
	return args.Get(0).(float64), args.Error(1)
}

func (m *MockProcessForCGroup) MemoryInfo() (*process.MemoryInfoStat, error) {
	args := m.Called()
	return args.Get(0).(*process.MemoryInfoStat), args.Error(1)
}

func TestCGroupCollector_CpuPercent(t *testing.T) {
	// Set up mock process
	mockProcess := new(MockProcessForCGroup)
	mockProcess.On("Percent", mock.Anything).Return(2000.0, nil)

	// Set up collector
	collector := &CGroupCollector{
		p:       mockProcess,
		cpuCore: 2.0, // Assuming 2 CPU cores for testing
	}

	// Call the method
	cpuPercent, err := collector.CpuPercent()
	assert.NoError(t, err)
	assert.Equal(t, 1000.0, cpuPercent) // 2000.0 / 2.0 = 1000.0
}

func TestCGroupCollector_MemPercent(t *testing.T) {
	// Set up mock process
	mockProcess := new(MockProcessForCGroup)
	mockProcess.On("MemoryInfo").Return(&process.MemoryInfoStat{RSS: 500 * 1024 * 1024}, nil) // 500 MB
	collector := &CGroupCollector{
		p:           mockProcess,
		totalMemory: 1000 * 1024 * 1024, // 1000 MB
	}

	// Call the method
	memPercent, err := collector.MemPercent()
	assert.NoError(t, err)
	assert.Equal(t, 50.0, memPercent) // (500MB / 1000MB) * 100 = 50%
}

func TestCGroupCollector_ErrorHandling(t *testing.T) {
	// Set up mock process for error case
	mockProcess := new(MockProcessForCGroup)
	mockProcess.On("MemoryInfo").Return((*process.MemoryInfoStat)(nil), errors.New("memory info error"))

	collector := &CGroupCollector{
		p:           mockProcess,
		totalMemory: 1000 * 1024 * 1024, // 1000 MB
	}

	// Call the method and expect an error
	memPercent, err := collector.MemPercent()
	assert.Error(t, err)
	assert.Equal(t, 0.0, memPercent)
}

func TestNewCGroupCollector(t *testing.T) {
	// Mock the readUint function
	readUintMock := func(path string) (uint64, error) {
		if path == CGroupCpuPeriodPath {
			return 100000, nil
		}
		if path == CGroupCpuQuotaPath {
			return 200000, nil
		}
		if path == CGroupMemLimitPath {
			return 300000000, nil // 300MB
		}
		return 0, errors.New("unknown path")
	}

	collector, err := NewCGroupCollector(readUintMock)
	assert.NoError(t, err)
	assert.NotNil(t, collector)
	assert.Equal(t, 2.0, collector.cpuCore)                         // 200000 / 100000 = 2
	assert.LessOrEqual(t, uint64(300000000), collector.totalMemory) // 300MB
}
