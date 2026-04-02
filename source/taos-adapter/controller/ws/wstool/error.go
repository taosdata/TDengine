package wstool

import (
	"context"

	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type WSErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func WSErrorMsg(ctx context.Context, session *melody.Session, logger *logrus.Entry, code int, message string, action string, reqID uint64) {
	data := &WSErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  GetDuration(ctx),
	}
	WSWriteJson(session, logger, data)
}
func WSError(ctx context.Context, session *melody.Session, logger *logrus.Entry, err error, action string, reqID uint64) {
	e, is := err.(*tErrors.TaosError)
	if is {
		WSErrorMsg(ctx, session, logger, int(e.Code)&0xffff, e.ErrStr, action, reqID)
	} else {
		WSErrorMsg(ctx, session, logger, 0xffff, err.Error(), action, reqID)
	}
}
