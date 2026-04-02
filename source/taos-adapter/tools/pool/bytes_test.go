package pool

import (
	"bytes"
	"testing"
)

func TestByteBuffer(t *testing.T) {
	var bb ByteBuffer

	n, err := bb.Write(nil)
	if err != nil {
		t.Fatalf("cannot write nil: %s", err)
	}
	if n != 0 {
		t.Fatalf("unexpected n when writing nil; got %d; want %d", n, 0)
	}
	if len(bb.B) != 0 {
		t.Fatalf("unexpected len(bb.B) after writing nil; got %d; want %d", len(bb.B), 0)
	}

	n, err = bb.Write([]byte{})
	if err != nil {
		t.Fatalf("cannot write empty slice: %s", err)
	}
	if n != 0 {
		t.Fatalf("unexpected n when writing empty slice; got %d; want %d", n, 0)
	}
	if len(bb.B) != 0 {
		t.Fatalf("unexpected len(bb.B) after writing empty slice; got %d; want %d", len(bb.B), 0)
	}

	data1 := []byte("123")
	n, err = bb.Write(data1)
	if err != nil {
		t.Fatalf("cannot write %q: %s", data1, err)
	}
	if n != len(data1) {
		t.Fatalf("unexpected n when writing %q; got %d; want %d", data1, n, len(data1))
	}
	if string(bb.B) != string(data1) {
		t.Fatalf("unexpected bb.B; got %q; want %q", bb.B, data1)
	}

	data2 := []byte("1")
	n, err = bb.Write(data2)
	if err != nil {
		t.Fatalf("cannot write %q: %s", data2, err)
	}
	if n != len(data2) {
		t.Fatalf("unexpected n when writing %q; got %d; want %d", data2, n, len(data2))
	}
	if string(bb.B) != string(data1)+string(data2) {
		t.Fatalf("unexpected bb.B; got %q; want %q", bb.B, string(data1)+string(data2))
	}

	bb.Reset()
	if string(bb.B) != "" {
		t.Fatalf("unexpected bb.B after reset; got %q; want %q", bb.B, "")
	}
}

func TestByteBufferReadFrom(t *testing.T) {
	var bbPool ByteBufferPool

	t.Run("zero_bytes", func(t *testing.T) {
		t.Parallel()
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		src := bytes.NewBufferString("")
		n, err := bb.ReadFrom(src)
		if err != nil {
			t.Fatalf("error when reading empty string: %s", err)
		}
		if n != 0 {
			t.Fatalf("unexpected number of bytes read; got %d; want %d", n, 0)
		}
		if len(bb.B) != 0 {
			t.Fatalf("unexpejcted len(bb.B); got %d; want %d", len(bb.B), 0)
		}
	})

	t.Run("non_zero_bytes", func(t *testing.T) {
		t.Parallel()
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		s := "foobarbaz"
		src := bytes.NewBufferString(s)
		n, err := bb.ReadFrom(src)
		if err != nil {
			t.Fatalf("error when reading non-empty string: %s", err)
		}
		if n != int64(len(s)) {
			t.Fatalf("unexpected number of bytes read; got %d; want %d", n, len(s))
		}
		if string(bb.B) != s {
			t.Fatalf("unexpected value read; got %q; want %q", bb.B, s)
		}
	})

	t.Run("big_number_of_bytes", func(t *testing.T) {
		t.Parallel()
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		b := make([]byte, 1024*1024+234)
		for i := range b {
			b[i] = byte(i)
		}
		src := bytes.NewBuffer(b)
		n, err := bb.ReadFrom(src)
		if err != nil {
			t.Fatalf("cannot read big value: %s", err)
		}
		if n != int64(len(b)) {
			t.Fatalf("unexpected number of bytes read; got %d; want %d", n, len(b))
		}
		if string(bb.B) != string(b) {
			t.Fatalf("unexpected value read; got %q; want %q", bb.B, b)
		}
	})

	t.Run("non_empty_bb", func(t *testing.T) {
		t.Parallel()
		bb := bbPool.Get()
		defer bbPool.Put(bb)
		prefix := []byte("prefix")
		bb.B = append(bb.B[:0], prefix...)
		s := "aosdfdsafdjsf"
		src := bytes.NewBufferString(s)
		n, err := bb.ReadFrom(src)
		if err != nil {
			t.Fatalf("cannot read to non-empty bb: %s", err)
		}
		if n != int64(len(s)) {
			t.Fatalf("unexpected number of bytes read; got %d; want %d", n, len(s))
		}
		if len(bb.B) != len(prefix)+len(s) {
			t.Fatalf("unexpected bb.B len; got %d; want %d", len(bb.B), len(prefix)+len(s))
		}
		if string(bb.B[:len(prefix)]) != string(prefix) {
			t.Fatalf("unexpected prefix; got %q; want %q", bb.B[:len(prefix)], prefix)
		}
		if string(bb.B[len(prefix):]) != s {
			t.Fatalf("unexpected data read; got %q; want %q", bb.B[len(prefix):], s)
		}
	})
}
