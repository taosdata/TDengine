package random

import "testing"

func TestDeterministicSequence(t *testing.T) {
	a := New(12345)
	b := New(12345)
	for i := 0; i < 200; i++ {
		if a.Uint64() != b.Uint64() {
			t.Fatalf("sequence mismatch at %d", i)
		}
	}
}

func TestSerializeDeserialize(t *testing.T) {
	r := New(987654321)
	_ = r.Uint64()
	_ = r.Uint64()
	state := r.Serialize()

	r2 := New(1)
	if err := r2.Deserialize(state); err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}
	for i := 0; i < 100; i++ {
		if r.Uint64() != r2.Uint64() {
			t.Fatalf("deserialized stream mismatch at %d", i)
		}
	}
}
