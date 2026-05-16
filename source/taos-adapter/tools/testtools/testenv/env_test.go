package testenv

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsEnterpriseTest(t *testing.T) {
	old, exists := os.LookupEnv(EnterpriseTestEnvVar)
	if exists {
		defer func() {
			_ = os.Setenv(EnterpriseTestEnvVar, old)
		}()
	} else {
		defer func() {
			_ = os.Unsetenv(EnterpriseTestEnvVar)
		}()
	}
	t.Setenv(EnterpriseTestEnvVar, "true")
	assert.True(t, IsEnterpriseTest())
	_ = os.Unsetenv(EnterpriseTestEnvVar)
	assert.False(t, IsEnterpriseTest())
}
