package monitor

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewNormalCollector(t *testing.T) {
	c, err := NewNormalCollector()
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.NotNil(t, c.p)
}

func TestNormalCollectorCpuPercentRange(t *testing.T) {
	c, err := NewNormalCollector()
	assert.NoError(t, err)

	_, err = c.CpuPercent()
	assert.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	val, err := c.CpuPercent()
	assert.NoError(t, err)

	assert.False(t, math.IsNaN(val) || math.IsInf(val, 0))
	assert.GreaterOrEqual(t, val, 0.0)
	assert.LessOrEqual(t, val, 100.0)
}

func TestNormalCollectorMemPercentRange(t *testing.T) {
	c, err := NewNormalCollector()
	assert.NoError(t, err)

	val, err := c.MemPercent()
	assert.NoError(t, err)
	if math.IsNaN(val) || math.IsInf(val, 0) {
		t.Fatalf("MemPercent returned invalid value: %v", val)
	}
	if val < 0 || val > 100 {
		t.Fatalf("MemPercent out of expected range [0,100]: %v", val)
	}
}
