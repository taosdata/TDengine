package wrapper

import "C"
import (
	"net"
	"unsafe"

	"github.com/taosdata/driver-go/v3/wrapper/cgo"
)

type WhitelistResult struct {
	ErrCode int32
	IPNets  []*net.IPNet
}

//export WhitelistCallback
func WhitelistCallback(param unsafe.Pointer, code int, taosConnect unsafe.Pointer, numOfWhiteLists int, pWhiteLists unsafe.Pointer) {
	c := (*(*cgo.Handle)(param)).Value().(chan *WhitelistResult)
	if code != 0 {
		c <- &WhitelistResult{ErrCode: int32(code)}
		return
	}
	ips := make([]*net.IPNet, 0, numOfWhiteLists)
	for i := 0; i < numOfWhiteLists; i++ {
		ipNet := make([]byte, 8)
		for j := 0; j < 8; j++ {
			ipNet[j] = *(*byte)(unsafe.Pointer(uintptr(pWhiteLists) + uintptr(i*8) + uintptr(j)))
		}
		ip := net.IP{ipNet[0], ipNet[1], ipNet[2], ipNet[3]}
		ones := int(ipNet[4])
		ipMask := net.CIDRMask(ones, 32)
		ips = append(ips, &net.IPNet{IP: ip, Mask: ipMask})
	}
	c <- &WhitelistResult{IPNets: ips}
}
