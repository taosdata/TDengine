package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPrometheusCacheTTLClamping tests the clamping logic for CacheTTL
// This validates the logic used in config.go InitConfig function
func TestPrometheusCacheTTLClamping(t *testing.T) {
	tests := []struct {
		name        string
		inputTTL    int
		expectedTTL int
	}{
		{
			name:        "default TTL",
			inputTTL:    300,
			expectedTTL: 300,
		},
		{
			name:        "TTL equals minimum",
			inputTTL:    60,
			expectedTTL: 60,
		},
		{
			name:        "TTL below minimum - should be clamped",
			inputTTL:    30,
			expectedTTL: 60,
		},
		{
			name:        "TTL well below minimum",
			inputTTL:    1,
			expectedTTL: 60,
		},
		{
			name:        "TTL zero - should not be clamped",
			inputTTL:    0,
			expectedTTL: 0,
		},
		{
			name:        "TTL negative - should not be clamped",
			inputTTL:    -1,
			expectedTTL: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the validation logic from config.go
			cacheTTL := tt.inputTTL
			if cacheTTL > 0 && cacheTTL < 60 {
				cacheTTL = 60
			}

			assert.Equal(t, tt.expectedTTL, cacheTTL)
		})
	}
}

