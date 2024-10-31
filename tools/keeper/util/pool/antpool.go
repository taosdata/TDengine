package pool

import (
	"github.com/panjf2000/ants/v2"
)

var GoroutinePool *ants.Pool

func Init(size int) {
	var err error
	GoroutinePool, err = ants.NewPool(size)
	if err != nil {
		panic(err)
	}
}
