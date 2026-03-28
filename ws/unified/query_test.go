package unified

import (
	"encoding/binary"
	"errors"
	"testing"

	taosErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

func buildFetchRawBlockRespBytes(code uint32, msg string, completed bool, block []byte) []byte {
	msgLen := len(msg)
	resp := make([]byte, 55+msgLen+len(block))
	binary.LittleEndian.PutUint16(resp[16:], proto.BinaryProtocolVersion1)
	binary.LittleEndian.PutUint32(resp[34:], code)
	binary.LittleEndian.PutUint32(resp[38:], uint32(msgLen))
	copy(resp[42:], []byte(msg))
	if completed {
		resp[50+msgLen] = 1
	}
	binary.LittleEndian.PutUint32(resp[51+msgLen:], uint32(len(block)))
	copy(resp[55+msgLen:], block)
	return resp
}

// TestParseFetchRawBlockResponseCompleted verifies the expected behavior for this scenario.
func TestParseFetchRawBlockResponseCompleted(t *testing.T) {
	resp := buildFetchRawBlockRespBytes(0, "", true, nil)
	block, completed, err := parseFetchRawBlockResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	if !completed {
		t.Fatal("expected completed=true")
	}
	if block != nil {
		t.Fatalf("unexpected block: %v", block)
	}
}

// TestParseFetchRawBlockResponseWithBlock verifies the expected behavior for this scenario.
func TestParseFetchRawBlockResponseWithBlock(t *testing.T) {
	want := []byte{1, 2, 3, 4}
	resp := buildFetchRawBlockRespBytes(0, "", false, want)
	block, completed, err := parseFetchRawBlockResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	if completed {
		t.Fatal("expected completed=false")
	}
	if string(block) != string(want) {
		t.Fatalf("unexpected block: %v", block)
	}
}

// TestParseFetchRawBlockResponseServerError verifies the expected behavior for this scenario.
func TestParseFetchRawBlockResponseServerError(t *testing.T) {
	resp := buildFetchRawBlockRespBytes(0x2603, "mock error", false, nil)
	_, _, err := parseFetchRawBlockResponse(resp)
	if err == nil {
		t.Fatal("expected error")
	}
	var terr *taosErrors.TaosError
	if !errors.As(err, &terr) {
		t.Fatalf("expected taos error, got %T", err)
	}
}

// TestParseFetchRawBlockResponseInvalid verifies the expected behavior for this scenario.
func TestParseFetchRawBlockResponseInvalid(t *testing.T) {
	_, _, err := parseFetchRawBlockResponse([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected invalid response error")
	}
	if !IsErrorType(err, ErrorTypeProtocol) {
		t.Fatalf("expected protocol error, got: %v", err)
	}
}
