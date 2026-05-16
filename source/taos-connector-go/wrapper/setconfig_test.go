package wrapper

import (
	"testing"
)

// @author: xftan
// @date: 2022/1/27 17:27
// @description: test taos_set_config
func TestSetConfig(t *testing.T) {
	source := map[string]string{
		"numOfThreadsPerCore":   "1.000000",
		"rpcTimer":              "300",
		"rpcForceTcp":           "0",
		"rpcMaxTime":            "600",
		"compressMsgSize":       "-1",
		"maxSQLLength":          "1048576",
		"maxWildCardsLength":    "100",
		"maxNumOfOrderedRes":    "100000",
		"keepColumnName":        "0",
		"timezone":              "Asia/Shanghai (CST, +0800)",
		"locale":                "C.UTF-8",
		"charset":               "UTF-8",
		"numOfLogLines":         "10000000",
		"asyncLog":              "1",
		"debugFlag":             "135",
		"rpcDebugFlag":          "131",
		"tmrDebugFlag":          "131",
		"cDebugFlag":            "131",
		"jniDebugFlag":          "131",
		"odbcDebugFlag":         "131",
		"uDebugFlag":            "131",
		"qDebugFlag":            "131",
		"maxBinaryDisplayWidth": "30",
		"tempDir":               "/tmp/",
	}
	err := TaosSetConfig(source)
	if err != nil {
		t.Error(err)
	}
}
