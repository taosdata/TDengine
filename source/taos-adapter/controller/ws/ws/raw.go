package ws

import (
	"context"
	"unsafe"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common/parser"
	"github.com/taosdata/taosadapter/v3/tools"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

func (h *messageHandler) binaryTMQRawMessage(ctx context.Context, session *melody.Session, action string, reqID uint64, message []byte, logger *logrus.Entry, isDebug bool) {
	p0 := unsafe.Pointer(&message[0])
	length := *(*uint32)(tools.AddPointer(p0, uintptr(24)))
	metaType := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
	data := tools.AddPointer(p0, uintptr(30))
	logger.Tracef("get write raw message, length:%d, metaType:%d", length, metaType)
	code := syncinterface.TMQWriteRaw(h.conn, length, metaType, data, logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TMQErr2Str(code, logger, isDebug)
		logger.Errorf("write raw meta error, code:%d, msg:%s", code, errStr)
		commonErrorResponse(ctx, session, logger, action, reqID, int(code), errStr)
		return
	}
	logger.Trace("write raw meta success")
	commonSuccessResponse(ctx, session, logger, action, reqID)
}

func (h *messageHandler) binaryRawBlockMessage(ctx context.Context, session *melody.Session, action string, reqID uint64, message []byte, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	p0 := unsafe.Pointer(&message[0])
	numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
	tableNameLength := *(*uint16)(tools.AddPointer(p0, uintptr(28)))
	tableName := make([]byte, tableNameLength)
	for i := 0; i < int(tableNameLength); i++ {
		tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
	}
	rawBlock := tools.AddPointer(p0, uintptr(30+tableNameLength))
	logger.Tracef("raw block message, table:%s, rows:%d", tableName, numOfRows)
	code := syncinterface.TaosWriteRawBlockWithReqID(h.conn, int(numOfRows), rawBlock, string(tableName), int64(innerReqID), logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TMQErr2Str(int32(code), logger, isDebug)
		logger.Errorf("write raw meta error, code:%d, msg:%s", code, errStr)
		commonErrorResponse(ctx, session, logger, action, reqID, code, errStr)
		return
	}
	logger.Trace("write raw meta success")
	commonSuccessResponse(ctx, session, logger, action, reqID)
}

func (h *messageHandler) binaryRawBlockMessageWithFields(ctx context.Context, session *melody.Session, action string, reqID uint64, message []byte, innerReqID uint64, logger *logrus.Entry, isDebug bool) {
	p0 := unsafe.Pointer(&message[0])
	numOfRows := *(*int32)(tools.AddPointer(p0, uintptr(24)))
	tableNameLength := int(*(*uint16)(tools.AddPointer(p0, uintptr(28))))
	tableName := make([]byte, tableNameLength)
	for i := 0; i < tableNameLength; i++ {
		tableName[i] = *(*byte)(tools.AddPointer(p0, uintptr(30+i)))
	}
	rawBlock := tools.AddPointer(p0, uintptr(30+tableNameLength))
	blockLength := int(parser.RawBlockGetLength(rawBlock))
	numOfColumn := int(parser.RawBlockGetNumOfCols(rawBlock))
	fieldsBlock := tools.AddPointer(p0, uintptr(30+tableNameLength+blockLength))
	code := syncinterface.TaosWriteRawBlockWithFieldsWithReqID(h.conn, int(numOfRows), rawBlock, string(tableName), fieldsBlock, numOfColumn, int64(innerReqID), logger, isDebug)
	if code != 0 {
		errStr := syncinterface.TMQErr2Str(int32(code), logger, isDebug)
		logger.Errorf("write raw meta error, code:%d, err:%s", code, errStr)
		commonErrorResponse(ctx, session, logger, action, reqID, code, errStr)
		return
	}
	logger.Trace("write raw meta success")
	commonSuccessResponse(ctx, session, logger, action, reqID)
}
