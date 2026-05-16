package wrapper

import (
	"net"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

func TestWhitelistCallback_ErrorCode(t *testing.T) {
	// Create a channel to receive the result
	resultChan := make(chan *WhitelistResult, 1)
	handle := cgo.NewHandle(resultChan)
	// Simulate an error (code != 0)
	go WhitelistCallback(handle.Pointer(), 1, nil, 0, nil)

	// Expect the result to have an error code
	result := <-resultChan
	assert.NotNil(t, result.Err)
	assert.Equal(t, int32(1), result.Err.(*errors.TaosError).Code)
	assert.Nil(t, result.AllowIPNets) // No IPs should be returned
	assert.Nil(t, result.BlockIPNets) // No IPs should be returned

	tests := []struct {
		name   string
		ipList []string
	}{
		{
			name:   "invalid length",
			ipList: []string{""},
		},
		{
			name:   "invalid format",
			ipList: []string{"+ invalid_ip_format"},
		},
		{
			name:   "invalid mask",
			ipList: []string{"x 192.168.0.0/16"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ipList := tt.ipList
			addrList := make([]unsafe.Pointer, len(ipList))
			for i := 0; i < len(ipList); i++ {
				bs := make([]byte, len(ipList[i])+1)
				copy(bs, ipList[i])
				addrList[i] = unsafe.Pointer(&bs[0])
			}
			pWhiteLists := unsafe.Pointer(&addrList[0])
			go WhitelistCallback(handle.Pointer(), 0, nil, len(ipList), pWhiteLists)
			result = <-resultChan
			assert.NotNil(t, result.Err)
			assert.Equal(t, int32(0xffff), result.Err.(*errors.TaosError).Code)
			assert.Nil(t, result.AllowIPNets) // No IPs should be returned
			assert.Nil(t, result.BlockIPNets) // No IPs should be returned
		})
	}
}

func TestWhitelistCallback_Success(t *testing.T) {
	// Prepare the test data: a list of byte slices representing IPs and masks
	ipList := []string{
		"+ 192.168.1.1/24",
		"+ 10.0.0.1/16",
		"+ 2001:db8::/32",
		"- 192.168.1.100/32",
		"- 2001:db8:fffa:10a0::/128",
	}
	addrList := make([]unsafe.Pointer, len(ipList))
	for i := 0; i < len(ipList); i++ {
		bs := make([]byte, len(ipList[i])+1)
		copy(bs, ipList[i])
		addrList[i] = unsafe.Pointer(&bs[0])
	}

	// Create a channel to receive the result
	resultChan := make(chan *WhitelistResult, 1)

	// Cast the byte slice to an unsafe pointer
	pWhiteLists := unsafe.Pointer(&addrList[0])
	handle := cgo.NewHandle(resultChan)
	// Simulate a successful callback (code == 0)
	go WhitelistCallback(handle.Pointer(), 0, nil, 5, pWhiteLists)

	// Expect the result to have two IPNets
	result := <-resultChan
	assert.Nil(t, result.Err)
	assert.Len(t, result.AllowIPNets, 3)
	assert.Len(t, result.BlockIPNets, 2)

	// Validate the first IPNet (192.168.1.1/24)
	assert.Equal(t, net.IPv4(192, 168, 1, 0).To4(), result.AllowIPNets[0].IP)

	ones, _ := result.AllowIPNets[0].Mask.Size()
	assert.Equal(t, 24, ones)

	// Validate the second IPNet (10.0.0.1/16)
	assert.Equal(t, net.IPv4(10, 0, 0, 0).To4(), result.AllowIPNets[1].IP)
	ones, _ = result.AllowIPNets[1].Mask.Size()
	assert.Equal(t, 16, ones)

	// Validate the third IPNet (2001:db8::/32)
	assert.Equal(t, net.ParseIP("2001:db8::"), result.AllowIPNets[2].IP)
	ones, _ = result.AllowIPNets[2].Mask.Size()
	assert.Equal(t, 32, ones)

	// Validate the first block IPNet (192.168.1.100/32)
	assert.Equal(t, net.IPv4(192, 168, 1, 100).To4(), result.BlockIPNets[0].IP)
	ones, _ = result.BlockIPNets[0].Mask.Size()
	assert.Equal(t, 32, ones)

	// Validate the second block IPNet (2001:db8:fffa:10a0::/128)
	assert.Equal(t, net.ParseIP("2001:db8:fffa:10a0::"), result.BlockIPNets[1].IP)
	ones, _ = result.BlockIPNets[1].Mask.Size()
	assert.Equal(t, 128, ones)
}
