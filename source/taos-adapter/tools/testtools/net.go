package testtools

import (
	"math/rand"
	"net"
	"strconv"
)

func GetRandomRemoteAddr() string {
	var ip net.IP
	if rand.Intn(2) == 0 {
		// Generate random IPv4
		ip = net.IPv4(byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)))
	} else {
		// Generate random IPv6
		ip = net.IP{byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)),
			byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))}
	}
	port := rand.Intn(65535-1024) + 1024 // Random port between 1024 and 65535
	return net.JoinHostPort(ip.String(), strconv.Itoa(port))
}
