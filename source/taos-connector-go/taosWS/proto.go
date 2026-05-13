package taosWS

import unifiedproto "github.com/taosdata/driver-go/v3/ws/unified/proto"

type RespInterface = unifiedproto.RespInterface

type BaseResp = unifiedproto.BaseResp

type WSConnectReq = unifiedproto.WSConnectReq
type WSConnectResp = unifiedproto.WSConnectResp
type WSQueryReq = unifiedproto.WSQueryReq
type WSQueryResp = unifiedproto.WSQueryResp
type WSFetchReq = unifiedproto.WSFetchReq
type WSFetchResp = unifiedproto.WSFetchResp
type WSFetchBlockReq = unifiedproto.WSFetchBlockReq
type WSFreeResultReq = unifiedproto.WSFreeResultReq
type WSAction = unifiedproto.WSAction

type StmtPrepareRequest = unifiedproto.StmtPrepareRequest
type StmtPrepareResponse = unifiedproto.StmtPrepareResponse
type StmtInitReq = unifiedproto.StmtInitReq
type StmtInitResp = unifiedproto.StmtInitResp
type StmtCloseRequest = unifiedproto.StmtCloseRequest
type StmtCloseResponse = unifiedproto.StmtCloseResponse
type StmtGetColFieldsRequest = unifiedproto.StmtGetColFieldsRequest
type StmtGetColFieldsResponse = unifiedproto.StmtGetColFieldsResponse
type StmtBindResponse = unifiedproto.StmtBindResponse
type StmtAddBatchRequest = unifiedproto.StmtAddBatchRequest
type StmtAddBatchResponse = unifiedproto.StmtAddBatchResponse
type StmtExecRequest = unifiedproto.StmtExecRequest
type StmtExecResponse = unifiedproto.StmtExecResponse
type StmtUseResultRequest = unifiedproto.StmtUseResultRequest
type StmtUseResultResponse = unifiedproto.StmtUseResultResponse

const (
	BindMessage = unifiedproto.BindMessage
)
