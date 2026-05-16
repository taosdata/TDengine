package log

import (
	"collector/common"
	"collector/types"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"
	"github.com/stretchr/testify/assert"
)

var uaValues = []*common.NodeValue{
	{
		IDStr:      "test_id",
		Name:       "test",
		Timestamp:  time.Unix(1700791658, 0),
		StartTime:  time.Unix(1700791658, 0),
		FinishTime: time.Unix(1700791658, 0),
		Value:      uint32(32),
		ValueType:  types.UINT32,
		Status:     int64(ua.StatusOK),
	},
	{
		IDStr:      "test_id2",
		Name:       "test2",
		Timestamp:  time.Unix(1700791658, 0),
		StartTime:  time.Unix(1700791658, 0),
		FinishTime: time.Unix(1700791658, 0),
		Value:      int32(32),
		ValueType:  types.INT32,
		Status:     int64(ua.StatusOK),
	},
}

var daValues = []*common.NodeValue{
	{
		IDStr:      "test_id",
		Name:       "test",
		Timestamp:  time.Unix(1700791658, 0),
		StartTime:  time.Unix(1700791658, 0),
		FinishTime: time.Unix(1700791658, 0),
		Value:      uint32(32),
		ValueType:  types.UINT32,
		Status:     192,
	},
	{
		IDStr:      "test_id2",
		Name:       "test2",
		Timestamp:  time.Unix(1700791658, 0),
		StartTime:  time.Unix(1700791658, 0),
		FinishTime: time.Unix(1700791658, 0),
		Value:      int32(32),
		ValueType:  types.INT32,
		Status:     192,
	},
}

func TestNewDataDump(t *testing.T) {
	tmpDir := t.TempDir()
	dump, err := NewDataDump(tmpDir, 1, false)
	assert.NoError(t, err)
	dump.Dump(daValues)
	time.Sleep(time.Second)
	dump.Close()
	files, err := findFilesWithPrefix(tmpDir, "opc_data.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	assert.NoError(t, err)
	ts := time.Unix(1700791658, 0).Local().Format(TimeFormat)
	expect := fmt.Sprintf("test_id,test,%s,%s,%s,32,UINT32,192\ntest_id2,test2,%s,%s,%s,32,INT32,192\n", ts, ts, ts, ts, ts, ts)
	assert.Equal(t, expect, string(data))
	os.Remove(files[0])

	dump, err = NewDataDump(tmpDir, 1, true)
	assert.NoError(t, err)
	dump.Dump(uaValues)
	time.Sleep(time.Second)
	dump.Close()
	files, err = findFilesWithPrefix(tmpDir, "opc_data.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err = os.ReadFile(files[0])
	assert.NoError(t, err)
	expect = fmt.Sprintf("test_id,test,%s,%s,%s,32,UINT32,StatusGood\ntest_id2,test2,%s,%s,%s,32,INT32,StatusGood\n", ts, ts, ts, ts, ts, ts)
	assert.Equal(t, expect, string(data))
}

func findFilesWithPrefix(rootPath, prefix string) ([]string, error) {
	var matchingFiles []string

	err := filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			fileName := info.Name()
			if strings.HasPrefix(fileName, prefix) {
				matchingFiles = append(matchingFiles, path)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return matchingFiles, nil
}
