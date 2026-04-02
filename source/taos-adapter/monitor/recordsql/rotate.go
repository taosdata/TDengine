package recordsql

import (
	"fmt"
	"path/filepath"
	"time"

	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/version"
)

var globalSQLRotateWriter *rotatelogs.RotateLogs
var globalStmtRotateWriter *rotatelogs.RotateLogs

func getRotateWriter(recordType RecordType) (*rotatelogs.RotateLogs, error) {
	var err error
	switch recordType {
	case RecordTypeSQL:
		if globalSQLRotateWriter == nil {
			globalSQLRotateWriter, err = newRotateWriter(recordType)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize rotate writer: %s", err)
			}
		} else {
			err = globalSQLRotateWriter.Rotate()
			if err != nil {
				return nil, fmt.Errorf("failed to rotate file: %s", err)
			}
		}
		return globalSQLRotateWriter, nil
	case RecordTypeStmt:
		if globalStmtRotateWriter == nil {
			globalStmtRotateWriter, err = newRotateWriter(recordType)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize rotate writer: %s", err)
			}
		} else {
			err = globalStmtRotateWriter.Rotate()
			if err != nil {
				return nil, fmt.Errorf("failed to rotate file: %s", err)
			}
		}
		return globalStmtRotateWriter, nil
	}
	return nil, fmt.Errorf("unknown record type: %d", recordType)
}

func newRotateWriter(recordType RecordType) (*rotatelogs.RotateLogs, error) {
	return rotatelogs.New(
		filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapter%s_%d_%%Y%%m%%d.csv", version.CUS_PROMPT, recordType, config.Conf.InstanceID)),
		rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
		rotatelogs.WithRotationTime(time.Hour*24),
		rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
		rotatelogs.WithReservedDiskSize(int64(config.Conf.Log.ReservedDiskSize)),
		rotatelogs.WithRotateGlobPattern(filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapter%s_%d_*.csv*", version.CUS_PROMPT, recordType, config.Conf.InstanceID))),
		rotatelogs.WithCompress(config.Conf.Log.Compress),
		rotatelogs.WithCleanLockFile(filepath.Join(config.Conf.Log.Path, fmt.Sprintf(".%sadapter%s_%d_rotate_lock", version.CUS_PROMPT, recordType, config.Conf.InstanceID))),
		rotatelogs.ForceNewFile(),
		rotatelogs.WithMaxAge(time.Hour*24*time.Duration(config.Conf.Log.KeepDays)),
	)
}
