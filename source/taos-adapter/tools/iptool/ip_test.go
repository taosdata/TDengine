package iptool

import (
	"net"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetRealIPWithXRealIPHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("X-Real-Ip", "192.168.1.1")

	ip := GetRealIP(req)

	assert.Equal(t, "192.168.1.1", ip.String())
}

func TestGetRealIPWithRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)
	req.RemoteAddr = "192.168.1.2:1234"

	ip := GetRealIP(req)

	assert.Equal(t, "192.168.1.2", ip.String())
}

func TestGetRealIPWithNoIP(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)

	ip := GetRealIP(req)
	host, _, _ := net.SplitHostPort(req.RemoteAddr)
	assert.Equal(t, host, ip.String())
}

func TestParseIpv6(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)

	req.RemoteAddr = "[fe80::4720:bdc2:e3a7:b2bc]:6041"
	ip := GetRealIP(req)
	assert.Equal(t, "fe80::4720:bdc2:e3a7:b2bc", ip.String())

	// zone with %15 is used to specify the network interface in IPv6
	req.RemoteAddr = "[fe80::4720:bdc2:e3a7:b2bd%15]:6041"
	ip = GetRealIP(req)
	assert.Equal(t, "fe80::4720:bdc2:e3a7:b2bd", ip.String())

	// wrong ipv6 format
	req.RemoteAddr = "[fe80::4720:bdc2:e3a7:gggg]:6041"
	ip = GetRealIP(req)
	assert.Nil(t, ip)
}

func TestGetRealPortWithXRealPortHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("X-Real-Port", "80")

	port, err := GetRealPort(req)
	assert.NoError(t, err)
	assert.Equal(t, "80", port)

	req.Header.Set("X-Real-Port", "-80")
	port, err = GetRealPort(req)
	assert.Error(t, err)
	assert.Equal(t, "", port)
}

func TestGetRealPortWithRemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com", nil)
	//ipv6
	req.RemoteAddr = "[fe80::4720:bdc2:e3a7:b2bc]:6041"
	port, err := GetRealPort(req)
	assert.NoError(t, err)
	assert.Equal(t, "6041", port)

	//ipv4
	req.RemoteAddr = "192.168.1.1:1234"
	port, err = GetRealPort(req)
	assert.NoError(t, err)
	assert.Equal(t, "1234", port)

	//invalid port
	req.RemoteAddr = "192.168.1.1:-1234"
	port, err = GetRealPort(req)
	assert.Error(t, err)
	assert.Equal(t, "", port)

	//wrong format
	req.RemoteAddr = "abc[]:123"
	port, err = GetRealPort(req)
	assert.Error(t, err)
	assert.Equal(t, "", port)

	//empty port
	req.RemoteAddr = "192.168.1.1:"
	port, err = GetRealPort(req)
	assert.NoError(t, err)
	assert.Equal(t, "", port)
}
