package testutil

import "os"

func TestUsername() string {
	return os.Getenv("KEEPER_TEST_USERNAME")
}

func TestPassword() string {
	return os.Getenv("KEEPER_TEST_PASSWORD")
}
