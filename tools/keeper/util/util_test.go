package util

import (
	"testing"
)

func TestHandleIp(t *testing.T) {
	tests := []struct {
		name string
		host string
		want string
	}{
		{
			name: "IPv4 address",
			host: "192.168.1.1",
			want: "192.168.1.1",
		},
		{
			name: "IPv6 address",
			host: "2001:db8::1",
			want: "[2001:db8::1]",
		},
		{
			name: "Link-local without scope",
			host: "fe80::a6bb:6dff:fed9:9817",
			want: "[fe80::a6bb:6dff:fed9:9817]",
		},
		{
			name: "Link-local with scope",
			host: "fe80::a6bb:6dff:fed9:9817%eno1",
			want: "[fe80::a6bb:6dff:fed9:9817%eno1]",
		},
		{
			name: "Hostname",
			host: "example.com",
			want: "example.com",
		},
		{
			name: "Invalid IP",
			host: "256.256.256.2567",
			want: "256.256.256.2567",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := HandleIp(tc.host)
			if got != tc.want {
				t.Errorf("HandleIp(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
	}
}
