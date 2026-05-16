package ws

import (
	"context"
	"encoding/binary"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/tools/bytesutil"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type commonResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func commonErrorResponse(ctx context.Context, session *melody.Session, logger *logrus.Entry, action string, reqID uint64, code int, message string) {
	data := &commonResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, data)
}

func commonSuccessResponse(ctx context.Context, session *melody.Session, logger *logrus.Entry, action string, reqID uint64) {
	data := &commonResp{
		Action: action,
		ReqID:  reqID,
		Timing: wstool.GetDuration(ctx),
	}
	wstool.WSWriteJson(session, logger, data)
}

func fetchRawBlockErrorResponse(session *melody.Session, logger *logrus.Entry, code int, message string, reqID uint64, resultID uint64, t uint64) {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + len(message) + 8 + 1
	buf := make([]byte, bufLength)
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(FetchRawBlockMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], t)
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], uint32(code&0xffff))
	binary.LittleEndian.PutUint32(buf[38:], uint32(len(message)))
	copy(buf[42:], message)
	binary.LittleEndian.PutUint64(buf[42+len(message):], resultID)
	buf[42+len(message)+8] = 1
	wstool.WSWriteBinary(session, buf, logger)
}

func fetchRawBlockFinishResponse(session *melody.Session, logger *logrus.Entry, reqID uint64, resultID uint64, t uint64) {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + 8 + 1
	buf := make([]byte, bufLength)
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(FetchRawBlockMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], t)
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], 0)
	binary.LittleEndian.PutUint32(buf[38:], 0)
	binary.LittleEndian.PutUint64(buf[42:], resultID)
	buf[50] = 1
	wstool.WSWriteBinary(session, buf, logger)
}

func fetchRawBlockMessage(buf []byte, reqID uint64, resultID uint64, t uint64, blockLength int32, rawBlock unsafe.Pointer) []byte {
	bufLength := 8 + 8 + 2 + 8 + 8 + 4 + 4 + 8 + 1 + 4 + int(blockLength)
	if cap(buf) < bufLength {
		buf = make([]byte, 0, bufLength)
	}
	buf = buf[:bufLength]
	binary.LittleEndian.PutUint64(buf, 0xffffffffffffffff)
	binary.LittleEndian.PutUint64(buf[8:], uint64(FetchRawBlockMessage))
	binary.LittleEndian.PutUint16(buf[16:], 1)
	binary.LittleEndian.PutUint64(buf[18:], t)
	binary.LittleEndian.PutUint64(buf[26:], reqID)
	binary.LittleEndian.PutUint32(buf[34:], 0)
	binary.LittleEndian.PutUint32(buf[38:], 0)
	binary.LittleEndian.PutUint64(buf[42:], resultID)
	buf[50] = 0
	binary.LittleEndian.PutUint32(buf[51:], uint32(blockLength))
	bytesutil.Copy(rawBlock, buf, 55, int(blockLength))
	return buf
}

type stmtErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	StmtID  uint64 `json:"stmt_id"`
}

func stmtErrorResponse(ctx context.Context, session *melody.Session, logger *logrus.Entry, action string, reqID uint64, code int, message string, stmtID uint64) {
	resp := &stmtErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  wstool.GetDuration(ctx),
		StmtID:  stmtID,
	}
	wstool.WSWriteJson(session, logger, resp)
}
