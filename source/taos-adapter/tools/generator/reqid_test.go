package generator

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestMain(m *testing.M) {
	config.Init()
	os.Exit(m.Run())

}
func TestGetReqID(t *testing.T) {
	// Create a map to store the generated IDs
	ids := make(map[int64]bool)

	// Call GetReqID multiple times
	for i := 0; i < 100; i++ {
		id := GetReqID()

		// Check if the ID is unique
		if _, exists := ids[id]; exists {
			t.Errorf("GetReqID() returned a duplicate ID: %v", id)
		}

		// Store the ID in the map
		ids[id] = true
	}

	// Call GetReqID multiple times
	for i := 0; i < 100; i++ {
		// Check if the reqIncrement resets after it exceeds 0x00ffffffffffffff
		GetReqID()

		// Check if the ID is unique
		atomic.StoreInt64(&reqIncrement, 0x00ffffffffffffff+1)
		id := GetReqID()
		if _, exists := ids[id]; exists {
			if id != int64(config.Conf.InstanceID)<<56|1 {
				t.Errorf("GetReqID() returned a duplicate ID: %v", id)
			}

			// Store the ID in the map
			ids[id] = true
		} else {
			t.Errorf("GetReqID() did not reset the reqIncrement correctly, got: %v, want: %v", id, uint64(config.Conf.InstanceID)<<56|1)
		}
	}
}

func TestGetSessionID(t *testing.T) {
	// Create a map to store the generated IDs
	ids := make(map[int64]bool)

	// Call GetSessionID multiple times
	for i := 0; i < 100; i++ {
		id := GetSessionID()

		// Check if the ID is unique
		if _, exists := ids[id]; exists {
			t.Errorf("GetSessionID() returned a duplicate ID: %v", id)
		}

		// Store the ID in the map
		ids[id] = true
	}

}

func TestGetUploadKeeperReqID(t *testing.T) {
	// Create a map to store the generated IDs
	ids := make(map[int64]bool)
	id := GetUploadKeeperReqID()
	assert.Equal(t, 0x0000000000000100|int64(config.Conf.InstanceID)<<56, id)
	// Call GetUploadKeeperReqID multiple times
	for i := 0; i < 100; i++ {
		id := GetUploadKeeperReqID()

		// Check if the ID is unique
		if _, exists := ids[id]; exists {
			t.Errorf("GetUploadKeeperReqID() returned a duplicate ID: %v", id)
		}

		// Store the ID in the map
		ids[id] = true
	}
	atomic.StoreUint32(&uploadKeeperReqID, 0xffffffff)
	id = GetUploadKeeperReqID()
	assert.Equal(t, 0x0000000000000000|int64(config.Conf.InstanceID)<<56, id)
}
