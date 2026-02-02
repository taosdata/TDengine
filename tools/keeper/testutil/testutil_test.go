package testutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTestUsername(t *testing.T) {
	os.Setenv("TEST_USERNAME", "test_user")
	defer os.Unsetenv("TEST_USERNAME")
	username := TestUsername()
	assert.Equal(t, "test_user", username)
}

func TestTestPassword(t *testing.T) {
	os.Setenv("TEST_PASSWORD", "test_pass")
	defer os.Unsetenv("TEST_PASSWORD")
	password := TestPassword()
	assert.Equal(t, "test_pass", password)
}
