package generator

import (
	"sync/atomic"

	"github.com/taosdata/taosadapter/v3/config"
)

var reqIncrement int64

func GetReqID() int64 {
	id := atomic.AddInt64(&reqIncrement, 1)
	if id > 0x00ffffffffffffff {
		atomic.StoreInt64(&reqIncrement, 1)
		id = 1
	}
	reqId := int64(config.Conf.InstanceID)<<56 | id
	return reqId
}

var sessionID int64

func GetSessionID() int64 {
	return atomic.AddInt64(&sessionID, 1)
}

var uploadKeeperReqID uint32

func GetUploadKeeperReqID() int64 {
	// 0 instanceID
	// 1-2 must be 0
	// 3-6 increment
	// 7 must be 0
	id := atomic.AddUint32(&uploadKeeperReqID, 1)
	reqId := int64(config.Conf.InstanceID)<<56 | int64(id)<<8
	return reqId
}
