package common

import (
	"context"
	"fmt"
	"math/bits"
	"os"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/taosdata/driver-go/v3/common/pointer"
)

var tUUIDHashId int64
var serialNo int64
var pid int64

func init() {
	var tUUID = uuid.New().String()
	tUUIDHashId = (int64(murmurHash32([]byte(tUUID), uint32(len(tUUID)))) & 0x07ff) << 52
	pid = (int64(os.Getpid()) & 0x0f) << 48
}

func GetReqID() int64 {
	ts := (time.Now().UnixNano() / 1e6) >> 8
	val := atomic.AddInt64(&serialNo, 1)
	return tUUIDHashId | pid | ((ts & 0x3ffffff) << 20) | (val & 0xfffff)
}

const (
	c1 uint32 = 0xcc9e2d51
	c2 uint32 = 0x1b873593
)

// MurmurHash32 returns the MurmurHash3 sum of data.
func murmurHash32(data []byte, seed uint32) uint32 {
	h1 := seed

	nBlocks := len(data) / 4
	p := unsafe.Pointer(&data[0])
	for i := 0; i < nBlocks; i++ {
		k1 := *(*uint32)(pointer.AddUintptr(p, uintptr(i*4)))

		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2

		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*4 + h1 + 0xe6546b64
	}

	tail := data[nBlocks*4:]

	var k1 uint32
	switch len(tail) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(len(data))

	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16

	return h1
}

func GetReqIDFromCtx(ctx context.Context) (int64, error) {
	var reqIDValue int64
	var ok bool
	reqID := ctx.Value(ReqIDKey)
	if reqID != nil {
		reqIDValue, ok = reqID.(int64)
		if !ok {
			return 0, fmt.Errorf("invalid taos_req_id: %v, should be int64, got %T", reqID, reqID)
		}
		return reqIDValue, nil
	}
	return 0, nil
}
