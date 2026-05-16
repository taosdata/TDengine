package iptool

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

func GetRealIP(r *http.Request) net.IP {
	ip := r.Header.Get("X-Real-Ip")
	if ip != "" {
		return net.ParseIP(ip)
	}
	host, _, _ := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	parsedIP := net.ParseIP(host)
	if parsedIP != nil {
		return parsedIP
	}
	// Fallback to resolving the host if the host has zone information
	ipAddr, err := net.ResolveIPAddr("ip", host)
	if err != nil {
		return nil
	}
	return ipAddr.IP
}

func GetRealPort(r *http.Request) (string, error) {
	port := r.Header.Get("X-Real-Port")
	if port != "" {
		_, err := strconv.ParseUint(port, 10, 32)
		if err != nil {
			return "", err
		}
		return port, nil
	}

	_, port, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err != nil {
		return "", err
	}
	if port != "" {
		_, err = strconv.ParseUint(port, 10, 32)
		if err != nil {
			return "", err
		}
		return port, nil
	}

	return port, nil
}
