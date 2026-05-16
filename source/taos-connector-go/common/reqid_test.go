package common

import (
	"testing"
)

func BenchmarkGetReqID(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			GetReqID()
		}
	})
}

func BenchmarkGetReqIDParallel(b *testing.B) {
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			GetReqID()
		}
	})
}

// @author: xftan
// @date: 2023/10/13 11:20
// @description: test get req id
func TestGetReqID(t *testing.T) {
	t.Log(GetReqID())
}

// @author: xftan
// @date: 2023/10/13 11:20
// @description: test MurmurHash
func TestMurmurHash(t *testing.T) {
	if murmurHash32([]byte("driver-go"), 0) != 3037880692 {
		t.Fatal("fail")
	}
}
