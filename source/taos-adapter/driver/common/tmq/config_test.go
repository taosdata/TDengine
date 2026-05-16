package tmq

import (
	"fmt"
	"reflect"
	"testing"
)

func TestConfigMap_Get(t *testing.T) {
	t.Parallel()

	config := ConfigMap{
		"key1": "value1",
		"key2": 123,
	}

	t.Run("Existing Key", func(t *testing.T) {
		want := "value1"
		if got, err := config.Get("key1", nil); err != nil || got != want {
			t.Errorf("Get() = %v, want %v (error: %v)", got, want, err)
		}
	})

	t.Run("Type Mismatch", func(t *testing.T) {
		wantErr := fmt.Errorf("key2 expects type string, not int")
		if got, err := config.Get("key2", "default"); err == nil || got != nil || err.Error() != wantErr.Error() {
			t.Errorf("Get() = %v, want error: %v", got, wantErr)
		}
	})

	t.Run("Non-Existing Key with Default Value", func(t *testing.T) {
		want := "default"
		if got, err := config.Get("key3", "default"); err != nil || got != want {
			t.Errorf("Get() = %v, want %v (error: %v)", got, want, err)
		}
	})
}

func TestConfigMap_Clone(t *testing.T) {
	t.Parallel()

	config := ConfigMap{
		"key1": "value1",
		"key2": 123,
	}

	clone := config.Clone()

	if !reflect.DeepEqual(config, clone) {
		t.Errorf("Clone() = %v, want %v", clone, config)
	}
}
