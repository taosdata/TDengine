package wrapper

import (
	"net"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

func TestWhitelistCallback_ErrorCode(t *testing.T) {
	// Create a channel to receive the result
	resultChan := make(chan *WhitelistResult, 1)
	handle := cgo.NewHandle(resultChan)
	// Simulate an error (code != 0)
	go WhitelistCallback(handle.Pointer(), 1, nil, 0, nil)

	// Expect the result to have an error code
	result := <-resultChan
	assert.Equal(t, int32(1), result.ErrCode)
	assert.Nil(t, result.IPNets) // No IPs should be returned
}

func TestWhitelistCallback_Success(t *testing.T) {
	// Prepare the test data: a list of byte slices representing IPs and masks
	ipList := []byte{
		192, 168, 1, 1, 24, // 192.168.1.1/24
		0, 0, 0,
		10, 0, 0, 1, 16, // 10.0.0.1/16
		0, 0, 0,
	}

	// Create a channel to receive the result
	resultChan := make(chan *WhitelistResult, 1)

	// Cast the byte slice to an unsafe pointer
	pWhiteLists := unsafe.Pointer(&ipList[0])
	handle := cgo.NewHandle(resultChan)
	// Simulate a successful callback (code == 0)
	go WhitelistCallback(handle.Pointer(), 0, nil, 2, pWhiteLists)

	// Expect the result to have two IPNets
	result := <-resultChan
	assert.Equal(t, int32(0), result.ErrCode)
	assert.Len(t, result.IPNets, 2)

	// Validate the first IPNet (192.168.1.1/24)
	assert.Equal(t, net.IPv4(192, 168, 1, 1).To4(), result.IPNets[0].IP)

	ones, _ := result.IPNets[0].Mask.Size()
	assert.Equal(t, 24, ones)

	// Validate the second IPNet (10.0.0.1/16)
	assert.Equal(t, net.IPv4(10, 0, 0, 1).To4(), result.IPNets[1].IP)
	ones, _ = result.IPNets[1].Mask.Size()
	assert.Equal(t, 16, ones)
}
