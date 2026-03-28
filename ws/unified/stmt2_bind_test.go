package unified

import (
	"encoding/binary"
	"testing"

	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

// TestBuildStmt2BindBinaryRequest verifies the expected behavior for this scenario.
func TestBuildStmt2BindBinaryRequest(t *testing.T) {
	payload := []byte{1, 2, 3}
	req := buildStmt2BindBinaryRequest(11, 22, payload, proto.Stmt2BindAllColumns)
	if len(req) != 33 {
		t.Fatalf("unexpected request length: %d", len(req))
	}
	if binary.LittleEndian.Uint64(req[0:8]) != 11 {
		t.Fatalf("unexpected req id: %d", binary.LittleEndian.Uint64(req[0:8]))
	}
	if binary.LittleEndian.Uint64(req[8:16]) != 22 {
		t.Fatalf("unexpected stmt id: %d", binary.LittleEndian.Uint64(req[8:16]))
	}
	if binary.LittleEndian.Uint64(req[16:24]) != proto.Stmt2BindMessage {
		t.Fatalf("unexpected action: %d", binary.LittleEndian.Uint64(req[16:24]))
	}
	if binary.LittleEndian.Uint16(req[24:26]) != proto.Stmt2BindProtocolVersion1 {
		t.Fatalf("unexpected protocol version: %d", binary.LittleEndian.Uint16(req[24:26]))
	}
	if binary.LittleEndian.Uint32(req[26:30]) != 0xFFFFFFFF {
		t.Fatalf("unexpected col index: %d", binary.LittleEndian.Uint32(req[26:30]))
	}
	if req[30] != 1 || req[31] != 2 || req[32] != 3 {
		t.Fatalf("unexpected payload: %v", req[30:])
	}
}
