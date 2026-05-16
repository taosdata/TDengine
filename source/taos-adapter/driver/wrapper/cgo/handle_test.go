// Copyright 2021 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cgo

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2022/1/27 17:21
// @description: test cgo handler
func TestHandle(t *testing.T) {
	v := 42

	tests := []struct {
		v1 interface{}
		v2 interface{}
	}{
		{v1: v, v2: v},
		{v1: &v, v2: &v},
		{v1: nil, v2: nil},
	}

	for _, tt := range tests {
		h1 := NewHandle(tt.v1)
		h2 := NewHandle(tt.v2)

		if uintptr(h1) == 0 || uintptr(h2) == 0 {
			t.Fatalf("NewHandle returns zero")
		}

		if uintptr(h1) == uintptr(h2) {
			t.Fatalf("Duplicated Go values should have different handles, but got equal")
		}

		h1v := h1.Value()
		h2v := h2.Value()
		if !reflect.DeepEqual(h1v, h2v) || !reflect.DeepEqual(h1v, tt.v1) {
			t.Fatalf("Value of a Handle got wrong, got %+v %+v, want %+v", h1v, h2v, tt.v1)
		}

		h1.Delete()
		h2.Delete()
	}

	siz := 0
	handles.Range(func(k, v interface{}) bool {
		siz++
		return true
	})
	if siz != 0 {
		t.Fatalf("handles are not cleared, got %d, want %d", siz, 0)
	}
}

func TestPointer(t *testing.T) {
	v := 42
	h := NewHandle(&v)
	p := h.Pointer()
	assert.Equal(t, *(*Handle)(p), h)
	h.Delete()
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Fatalf("Pointer should panic")
	}()
	h.Pointer()
}

func TestInvalidValue(t *testing.T) {
	v := 42
	h := NewHandle(&v)
	h.Delete()
	defer func() {
		if r := recover(); r != nil {
			return
		}
		t.Fatalf("Value should panic")
	}()
	h.Value()
}

func BenchmarkHandle(b *testing.B) {
	b.Run("non-concurrent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			h := NewHandle(i)
			_ = h.Value()
			h.Delete()
		}
	})
	b.Run("concurrent", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			var v int
			for pb.Next() {
				h := NewHandle(v)
				_ = h.Value()
				h.Delete()
			}
		})
	})
}
