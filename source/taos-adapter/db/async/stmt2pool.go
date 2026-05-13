package async

import (
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/wrapper/cgo"
)

type Stmt2Result struct {
	Res      unsafe.Pointer
	Affected int
	N        int
}

type Stmt2CallBackCaller struct {
	ExecResult chan *Stmt2Result
}

type Stmt2CallBackCallerPool struct {
	pool chan cgo.Handle
}

const Stmt2CBPoolSize = 10000

func NewStmt2CallBackCallerPool(size int) *Stmt2CallBackCallerPool {
	return &Stmt2CallBackCallerPool{
		pool: make(chan cgo.Handle, size),
	}
}

func (p *Stmt2CallBackCallerPool) Get() (cgo.Handle, *Stmt2CallBackCaller) {
	select {
	case handle := <-p.pool:
		c := handle.Value().(*Stmt2CallBackCaller)
		// cleanup channel
		for {
			select {
			case <-c.ExecResult:
			default:
				return handle, c
			}
		}
	default:
		c := &Stmt2CallBackCaller{
			ExecResult: make(chan *Stmt2Result, 1),
		}
		return cgo.NewHandle(c), c
	}
}

func (p *Stmt2CallBackCallerPool) Put(h cgo.Handle) {
	select {
	case p.pool <- h:
	default:
		h.Delete()
	}
}

func (s *Stmt2CallBackCaller) ExecCall(res unsafe.Pointer, affected int, code int) {
	s.ExecResult <- &Stmt2Result{
		Res:      res,
		Affected: affected,
		N:        code,
	}
}

var GlobalStmt2CallBackCallerPool = NewStmt2CallBackCallerPool(Stmt2CBPoolSize)
