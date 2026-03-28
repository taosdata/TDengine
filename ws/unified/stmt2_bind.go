package unified

import (
	"encoding/binary"
	"math"

	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

func buildStmt2BindPayload(bindData []*commonstmt.TaosStmt2BindData, isInsert bool, fields []*commonstmt.Stmt2AllField) ([]byte, error) {
	return commonstmt.MarshalStmt2Binary(bindData, isInsert, fields)
}

func buildStmt2BindBinaryRequest(reqID uint64, stmtID uint64, bindPayload []byte, colIndex int32) []byte {
	header := make([]byte, 30)
	binary.LittleEndian.PutUint64(header[0:], reqID)
	binary.LittleEndian.PutUint64(header[8:], stmtID)
	binary.LittleEndian.PutUint64(header[16:], proto.Stmt2BindMessage)
	binary.LittleEndian.PutUint16(header[24:], proto.Stmt2BindProtocolVersion1)
	if colIndex < 0 {
		binary.LittleEndian.PutUint32(header[26:], math.MaxUint32)
	} else {
		binary.LittleEndian.PutUint32(header[26:], uint32(colIndex))
	}
	out := make([]byte, 0, len(header)+len(bindPayload))
	out = append(out, header...)
	out = append(out, bindPayload...)
	return out
}
