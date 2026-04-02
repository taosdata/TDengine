package ws

import (
	"github.com/sirupsen/logrus"
)

type freeResultRequest struct {
	ReqID uint64 `json:"req_id"`
	ID    uint64 `json:"id"`
}

func (h *messageHandler) freeResult(req freeResultRequest, logger *logrus.Entry) {
	logger.Tracef("free result by id, id:%d", req.ID)
	h.queryResults.FreeResultByID(req.ID, logger)
}
