package wrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

func TestGetWhiteList(t *testing.T) {
	conn, err := TaosConnect("", "root", "taosdata", "", 0)
	assert.NoError(t, err)
	defer TaosClose(conn)
	c := make(chan *WhitelistResult, 1)
	handler := cgo.NewHandle(c)
	TaosFetchIPWhitelistA(conn, handler)
	data := <-c
	assert.Nil(t, data.Err)
	assert.Equal(t, 2, len(data.AllowIPNets))
	assert.Equal(t, "0.0.0.0/0", data.AllowIPNets[0].String())
	assert.Equal(t, "::/0", data.AllowIPNets[1].String())
	assert.Empty(t, data.BlockIPNets)
}
