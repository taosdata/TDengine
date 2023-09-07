package common

import (
	"fmt"
	"os"
)

func InSlice[T string | int64 | int32 | int | float64 | float32 | ValueType](ele T, slice []T) bool {
	for _, e := range slice {
		if e == ele {
			return true
		}
	}
	return false
}

func MakeDirIfNotExist(path string) error {
	file, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return fmt.Errorf("failed to create path: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to check path: %w", err)
	}
	if !file.IsDir() {
		return fmt.Errorf("path exists and is not a directory")
	}
	return nil
}
