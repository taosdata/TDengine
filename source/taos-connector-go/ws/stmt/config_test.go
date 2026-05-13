package stmt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetConnectionTimezone(t *testing.T) {
	cfg := NewConfig("", 1)
	err := cfg.SetConnectionTimezone("Europe/Paris")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Timezone.String() != "Europe/Paris" {
		t.Fatalf("expected Europe/Paris, got %s", cfg.Timezone.String())
	}

	err = cfg.SetConnectionTimezone("Invalid/Timezone")
	assert.Error(t, err)
	err = cfg.SetConnectionTimezone("Local")
	assert.Error(t, err)
	err = cfg.SetConnectionTimezone("")
	assert.Error(t, err)
}
