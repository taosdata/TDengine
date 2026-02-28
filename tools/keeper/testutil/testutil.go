package testutil

import "os"

func TestUsername() string {
	if v := os.Getenv("TEST_USERNAME"); v != "" {
		return v
	}
	return "root"
}

func TestPassword() string {
	if v := os.Getenv("TEST_PASSWORD"); v != "" {
		return v
	}
	return "taosdata"
}
