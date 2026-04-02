// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cgo

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// Handle provides a way to pass values that contain Go pointers
// (pointers to memory allocated by Go) between Go and C without
// breaking the cgo pointer passing rules. A Handle is an integer
// value that can represent any Go value. A Handle can be passed
// through C and back to Go, and Go code can use the Handle to
// retrieve the original Go value.
//
// The underlying type of Handle is guaranteed to fit in an integer type
// that is large enough to hold the bit pattern of any pointer. The zero
// value of a Handle is not valid, and thus is safe to use as a sentinel
// in C APIs.

type Handle uintptr

// NewHandle returns a handle for a given value.
//
// The handle is valid until the program calls Delete on it. The handle
// uses resources, and this package assumes that C code may hold on to
// the handle, so a program must explicitly call Delete when the handle
// is no longer needed.
//
// The intended use is to pass the returned handle to C code, which
// passes it back to Go, which calls Value.
func NewHandle(v interface{}) Handle {
	h := atomic.AddUintptr(&handleIdx, 1)
	if h == 0 {
		panic("runtime/cgo: ran out of handle space")
	}

	handles.Store(h, v)
	handle := Handle(h)
	handlePointers.Store(h, &handle)
	return handle
}

// Value returns the associated Go value for a valid handle.
//
// The method panics if the handle is invalid.
func (h Handle) Value() interface{} {
	v, ok := handles.Load(uintptr(h))
	if !ok {
		panic("runtime/cgo: misuse of an invalid Handle")
	}
	return v
}

func (h Handle) Pointer() unsafe.Pointer {
	p, ok := handlePointers.Load(uintptr(h))
	if !ok {
		panic("runtime/cgo: misuse of an invalid Handle")
	}
	return unsafe.Pointer(p.(*Handle))
}

// Delete invalidates a handle. This method should only be called once
// the program no longer needs to pass the handle to C and the C code
// no longer has a copy of the handle value.
//
// The method panics if the handle is invalid.
func (h Handle) Delete() {
	handles.Delete(uintptr(h))
	handlePointers.Delete(uintptr(h))
}

var (
	handles        = sync.Map{} // map[Handle]interface{}
	handlePointers = sync.Map{} // map[Handle]*Handle
	handleIdx      uintptr      // atomic
)
