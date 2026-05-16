package taosWS

import (
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2023/10/13 11:26
// @description: test bad conn error
func TestBadConnError(t *testing.T) {
	nothingErr := errors.New("error")
	err := NewBadConnError(nothingErr)
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Equal(t, "error", err.Error())
	err = NewBadConnErrorWithCtx(nothingErr, "nothing")
	assert.ErrorIs(t, err, driver.ErrBadConn)
	assert.Equal(t, "error error: context: nothing", err.Error())
}
