package recordsql

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/version"
)

func TestGetRotateWriter(t *testing.T) {
	tmpDir := t.TempDir()
	for _, recordType := range recordTypes {
		doTestGetRotateWriter(t, tmpDir, recordType)
	}
	_, err := getRotateWriter(-100)
	require.Error(t, err, "Expected error for invalid record type")
}

func doTestGetRotateWriter(t *testing.T, tmpDir string, recordType RecordType) {
	if globalSQLRotateWriter != nil {
		err := globalSQLRotateWriter.Close()
		assert.NoError(t, err, "Failed to close globalSQLRotateWriter")
		globalSQLRotateWriter = nil
	}
	defer func() {
		if globalSQLRotateWriter != nil {
			err := globalSQLRotateWriter.Close()
			assert.NoError(t, err, "Failed to close globalSQLRotateWriter")
			globalSQLRotateWriter = nil
		}
	}()
	oldPath := config.Conf.Log.Path
	defer func() {
		config.Conf.Log.Path = oldPath
	}()
	config.Conf.Log.Path = "/"
	_, err := getRotateWriter(recordType)
	require.Error(t, err, "Expected error when log path is root directory")
	config.Conf.Log.Path = tmpDir
	writer, err := getRotateWriter(recordType)
	require.NoError(t, err)
	defer func() {
		err = writer.Close()
		assert.NoError(t, err, "Failed to close writer")
	}()
	_, err = writer.Write([]byte("test"))
	require.NoError(t, err)
	files, err := getRecordFiles(tmpDir, recordType)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(files))
	assert.Equal(t, filepath.Base(writer.CurrentFileName()), files[0])
	writer, err = getRotateWriter(recordType)
	require.NoError(t, err)
	files, err = getRecordFiles(tmpDir, recordType)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(files), "Expected two files after rotation")
	recordFile := ""
	for _, file := range files {
		if !strings.HasSuffix(file, ".csv") {
			recordFile = file
		}
	}
	assert.NotEmpty(t, recordFile, "Expected a record file without .csv suffix")
	assert.Equal(t, filepath.Base(writer.CurrentFileName()), recordFile)
}

func getRecordFiles(dir string, recordType RecordType) ([]string, error) {
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && strings.HasPrefix(info.Name(), fmt.Sprintf("%sadapter%s_", version.CUS_PROMPT, recordType)) && !strings.HasSuffix(info.Name(), "_lock") {
			files = append(files, info.Name())
		}
		return nil
	})
	return files, err
}
