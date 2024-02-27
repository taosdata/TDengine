package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewDataDump(t *testing.T) {
	tmpDir := t.TempDir()
	dump, err := NewDataDump(tmpDir, 1)
	assert.NoError(t, err)
	ts := time.Unix(1700791658, 0).Local().Format(TimeFormat)
	dump.Dump(time.Unix(1700791658, 0), 0, "test_topic", []byte("test_payload"))
	time.Sleep(time.Second)
	dump.Close()
	files, err := findFilesWithPrefix(tmpDir, "mqtt.dump")
	assert.NoError(t, err)
	assert.Len(t, files, 1)
	data, err := os.ReadFile(files[0])
	assert.NoError(t, err)
	expect := fmt.Sprintf("%s,0,test_topic,test_payload\n", ts)
	assert.Equal(t, expect, string(data))
	os.Remove(files[0])
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
