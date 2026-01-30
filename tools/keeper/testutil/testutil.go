package testutil

import "os"

func TestUsername() string {
	return os.Getenv("TEST_USERNAME")
}

func TestPassword() string {
	return os.Getenv("TEST_PASSWORD")
}
