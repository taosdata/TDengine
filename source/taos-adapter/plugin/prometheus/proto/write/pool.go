package prompb

import (
	"sync"
)

var writeRequestPool sync.Pool

func GetWriteRequest() *WriteRequest {
	v := writeRequestPool.Get()
	if v == nil {
		return &WriteRequest{}
	}
	return v.(*WriteRequest)
}

func PutWriteRequest(wr *WriteRequest) {
	wr.Reset()
	writeRequestPool.Put(wr)
}
