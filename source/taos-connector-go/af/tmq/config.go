package tmq

import (
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
)

type config struct {
	cConfig  unsafe.Pointer
	timezone *time.Location
}

func newConfig() *config {
	return &config{cConfig: wrapper.TMQConfNew()}
}

func (c *config) setConfig(key string, value string) error {
	errCode := wrapper.TMQConfSet(c.cConfig, key, value)
	if errCode != errors.SUCCESS {
		errStr := wrapper.TMQErr2Str(errCode)
		return errors.NewError(int(errCode), errStr)
	}
	return nil
}

// Destroy Release TMQ config
func (c *config) destroy() {
	wrapper.TMQConfDestroy(c.cConfig)
}
