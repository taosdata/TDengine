package testtool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPQuery(t *testing.T) {
	resp, err := HTTPQuery("select 1")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), resp.Data[0][0])
}
