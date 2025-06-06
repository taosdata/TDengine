package db

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// TestLogHook captures log entries for verification
type TestLogHook struct {
	Entries []logrus.Entry
}

func (h *TestLogHook) Fire(entry *logrus.Entry) error {
	h.Entries = append(h.Entries, *entry)
	return nil
}

func (h *TestLogHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func TestExecuteWithRetry(t *testing.T) {
	tests := []struct {
		name                string
		maxRetries          int
		baseDelay           time.Duration
		maxDelay            time.Duration
		failures            int           // Number of simulated failures
		expectedAttempts    int           // Expected number of attempts
		expectedFinalDelay  time.Duration // Expected final delay before success
		shouldSucceed       bool          // Should the operation succeed?
		expectedError       error         // Expected error if not successful
		cancelAfterAttempts int           // Cancel context after this many attempts
	}{
		{
			name:               "Succeeds immediately",
			baseDelay:          10 * time.Millisecond,
			maxDelay:           100 * time.Millisecond,
			failures:           0,
			expectedAttempts:   1,
			expectedFinalDelay: 10 * time.Millisecond,
			shouldSucceed:      true,
		},
		{
			name:               "Succeeds after one failure",
			baseDelay:          10 * time.Millisecond,
			maxDelay:           100 * time.Millisecond,
			failures:           1,
			expectedAttempts:   2,
			expectedFinalDelay: 20 * time.Millisecond,
			shouldSucceed:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Setup log capturing
			logger := logrus.New()
			testHook := &TestLogHook{}
			logger.Hooks.Add(testHook)
			logger.Level = logrus.DebugLevel

			// Counters and error simulation
			var attemptCounter int32
			failuresRemaining := tt.failures

			config := RetryConfig{
				BaseDelay: tt.baseDelay,
				MaxDelay:  tt.maxDelay,
				Logger:    logger.WithField("test", tt.name),
			}

			// Simulated operation function
			op := func() (int, error) {
				attempt := int(atomic.AddInt32(&attemptCounter, 1))

				// Simulate failures
				if failuresRemaining > 0 {
					failuresRemaining--
					return 0, errors.New("simulated error")
				}

				return attempt, nil
			}

			startTime := time.Now()
			result, err := executeWithRetry(config, op)
			elapsed := time.Since(startTime)

			// Verify results
			if tt.shouldSucceed {
				if err != nil {
					t.Fatalf("expected success but got error: %v", err)
				}
				if result != tt.expectedAttempts {
					t.Errorf("expected result %d, got %d", tt.expectedAttempts, result)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error but got success")
				}
				if !errors.Is(err, tt.expectedError) {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			}

			// Verify attempt count
			if int(attemptCounter) != tt.expectedAttempts {
				t.Errorf("expected %d attempts, got %d", tt.expectedAttempts, attemptCounter)
			}
			// Verify delay time
			if len(testHook.Entries) > 1 {
				lastEntry := testHook.Entries[len(testHook.Entries)-2] // Last error log
				delayField, exists := lastEntry.Data["retry_delay"]
				if !exists {
					t.Error("missing retry_delay field in log entry")
				} else if delayField != tt.expectedFinalDelay.String() {
					t.Errorf("expected final delay %v, got %v", tt.expectedFinalDelay, delayField)
				}
			}

			// Verify total execution time
			minExpectedTime := time.Duration(0)
			currentDelay := tt.baseDelay
			for i := 1; i < tt.expectedAttempts; i++ {
				minExpectedTime += currentDelay
				currentDelay = time.Duration(math.Min(float64(currentDelay)*2, float64(tt.maxDelay)))
			}

			if elapsed < minExpectedTime {
				t.Errorf("execution time %v is shorter than expected minimum %v", elapsed, minExpectedTime)
			}
		})
	}
}
