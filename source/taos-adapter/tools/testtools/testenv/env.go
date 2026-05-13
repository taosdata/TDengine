package testenv

import "os"

const EnterpriseTestEnvVar = "GO_TEST_ENTERPRISE"

func IsEnterpriseTest() bool {
	if _, ok := os.LookupEnv(EnterpriseTestEnvVar); ok {
		return true
	}
	return false
}
