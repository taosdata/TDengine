#include "qwMsg.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qwInt.h"
#include "qworker.h"
#include "tcommon.h"
#include "tmsg.h"
#include "tname.h"
#include "tgrant.h"

int32_t qwMallocFetchRsp(int8_t rpcMalloc, int32_t length, SRetrieveTableRsp **rsp) {
  int32_t msgSize = sizeof(SRetrieveTableRsp) + length;

  SRetrieveTableRsp *pRsp =
      (SRetrieveTableRsp *)(rpcMalloc ? rpcReallocCont(*rsp, msgSize) : taosMemoryRealloc(*rsp, msgSize));
  if (NULL == pRsp) {
    qError("rpcMallocCont %d failed", msgSize);
    QW_RET(terrno);
  }

  if (NULL == *rsp) {
    TAOS_MEMSET(pRsp, 0, sizeof(SRetrieveTableRsp));
  }

  *rsp = pRsp;

  return TSDB_CODE_SUCCESS;
}

void qwBuildFetchRsp(void *msg, SOutputData *input, int32_t len, int32_t rawDataLen, bool qComplete) {
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)msg;

  rsp->useconds = htobe64(input->useconds);
  rsp->completed = qComplete;
  rsp->precision = input->precision;
  rsp->compressed = input->compressed;
  rsp->payloadLen = htonl(rawDataLen);
  rsp->compLen = htonl(len);
  rsp->numOfRows = htobe64(input->numOfRows);
  rsp->numOfCols = htonl(input->numOfCols);
  rsp->numOfBlocks = htonl(input->numOfBlocks);
}

void qwFreeFetchRsp(SQWTaskCtx *ctx, void *msg) {
  if (NULL == msg) {
    return;
  }
  
  if (NULL == ctx || !ctx->localExec) {
    rpcFreeCont(msg);
  }
}


int32_t qwCloneSubQRsp(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void** ppRes, int32_t* dataLen, bool* toFetch, SQWRspItem* pItem) {
  SRetrieveTableRsp* pRsp = NULL;
  int32_t tcode = qwMallocFetchRsp(!ctx->localExec, pItem->dataLen, &pRsp);
  if (tcode) {
    QW_TASK_ELOG("qwMallocFetchRsp size %d, localExec:%d failed, error:%s", pItem->dataLen, ctx->localExec, tstrerror(tcode));
    return tcode;
  }

  memcpy(pRsp, pItem->rsp, sizeof(*pRsp) + pItem->dataLen);

  *ppRes = pRsp;
  *dataLen = pItem->dataLen;
  int32_t code = ctx->subQRes.code;
  
  QW_TASK_DLOG("subQ task got res from rspList, rsp:%p, dataLen:%d, code:%d", *ppRes, *dataLen, code);

  if (toFetch) {
    *toFetch = false;
  }

  return code;
}

int32_t qwSaveSubQueryFetchRsp(QW_FPARAMS_DEF, SQWTaskCtx *ctx, SQWRspItem* pItem, void* rsp, int32_t dataLen, int32_t code) {
  SQWSubQRes* pRes = &ctx->subQRes;
  if (code) {
    pRes->code = code;
  }
  
  pItem->dataLen = dataLen;

  SRetrieveTableRsp* pRsp = NULL;
  if (rsp && dataLen >= 0) {
    int32_t tcode = qwMallocFetchRsp(!ctx->localExec, dataLen, &pRsp);
    if (tcode) {
      QW_TASK_ELOG("qwMallocFetchRsp size %d, localExec:%d failed, error:%s", dataLen, ctx->localExec, tstrerror(tcode));
      return tcode;
    }

    memcpy(pRsp, rsp, sizeof(*pRsp) + dataLen);
  }
  
  pItem->rsp = pRsp;

  return code;
}

int32_t qwChkSaveScalarSubQRsp(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void* rsp, int32_t dataLen, int32_t code, bool queryEnd) {
  SRetrieveTableRsp* pRsp = (SRetrieveTableRsp*)rsp;
  if (!queryEnd || NULL == rsp || be64toh(pRsp->numOfRows) > 1) {
    QW_TASK_ELOG("invalid subQ rsp, queryEnd:%d, numOfRows:%" PRId64, queryEnd, pRsp ? be64toh(pRsp->numOfRows) : 0);
    code = TSDB_CODE_PAR_INVALID_SCALAR_SUBQ_RES_ROWS;
  }

  code = qwSaveSubQueryFetchRsp(QW_FPARAMS(), ctx, &ctx->subQRes.scalarRsp, rsp, dataLen, code);

  ctx->subQRes.fetchDone = queryEnd;

  return code;
}

int32_t qwSaveAndHandleWailtList(QW_FPARAMS_DEF, SQWTaskCtx *ctx, SQWRspItem* newItem, bool fetchDone) {
  int32_t code = 0;
  
  taosWWaitLockLatch(&ctx->subQRes.lock);
  if (NULL == taosArrayPush(ctx->subQRes.rspList, newItem)) {
    qwFreeFetchRsp(ctx, newItem->rsp);
    QW_TASK_ELOG("taosArrayPush SQWRspItem to list failed, error:%s", tstrerror(terrno));
    code = terrno;
    if (TSDB_CODE_SUCCESS == ctx->subQRes.code) {
      ctx->subQRes.code = code;
    }
  }

  ctx->subQRes.fetchDone = fetchDone;

  QW_TASK_DLOG("%s code:%d, fetchDone:%d, rspListSize:%d", __func__, ctx->subQRes.code, fetchDone, (int32_t)taosArrayGetSize(ctx->subQRes.rspList));

  int32_t waitNum = taosArrayGetSize(ctx->subQRes.waitList);
  if (TSDB_CODE_SUCCESS != ctx->subQRes.code) {
    for (int32_t i = 0; i < waitNum; ++i) {
      SQWWaitItem* pWaitItem = taosArrayGet(ctx->subQRes.waitList, i);
      code = qwBuildAndSendFetchRsp(ctx, pWaitItem->reqMsgType + 1, &pWaitItem->connInfo, NULL, 0, ctx->subQRes.code);
    }

    taosArrayClear(ctx->subQRes.waitList);

    taosWUnLockLatch(&ctx->subQRes.lock);

    return ctx->subQRes.code;
  }

  void* pRsp = NULL;
  int32_t dataLen = 0;
  for (int32_t i = 0; i < waitNum; ++i) {
    SQWWaitItem* pWaitItem = taosArrayGet(ctx->subQRes.waitList, i);
    SQWRspItem* pRspItem = taosArrayGet(ctx->subQRes.rspList, pWaitItem->blockIdx);
    if (NULL == pRspItem) {
      ctx->subQRes.code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      QW_TASK_ELOG("subQ srcTask 0x%" PRIx64 " with invalid blockIdx %" PRIu64 ", currentBlockNum:%d", 
        pWaitItem->srcTaskId, pWaitItem->blockIdx, (int32_t)taosArrayGetSize(ctx->subQRes.rspList));
        
      code = qwBuildAndSendFetchRsp(ctx, pWaitItem->reqMsgType + 1, &pWaitItem->connInfo, NULL, 0, ctx->subQRes.code);
      if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == ctx->subQRes.code) {
        ctx->subQRes.code = code;
      }

      continue;
    }
    
    code = qwCloneSubQRsp(QW_FPARAMS(), ctx, &pRsp, &dataLen, NULL, pRspItem);
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == ctx->subQRes.code) {
      ctx->subQRes.code = code;
    }
    code = qwBuildAndSendFetchRsp(ctx, pWaitItem->reqMsgType + 1, &pWaitItem->connInfo, (SRetrieveTableRsp*)pRsp, dataLen, ctx->subQRes.code);
    if (TSDB_CODE_SUCCESS != code && TSDB_CODE_SUCCESS == ctx->subQRes.code) {
      ctx->subQRes.code = code;
    }
  }

  taosArrayClear(ctx->subQRes.waitList);

  taosWUnLockLatch(&ctx->subQRes.lock);

  return ctx->subQRes.code;
}

int32_t qwChkSaveNonScalarSubQRsp(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void* rsp, int32_t dataLen, int32_t code, bool queryEnd) {
  if (NULL == rsp || TSDB_CODE_SUCCESS != code) {
    ctx->subQRes.code = code;
    return code;
  }
  
  if (NULL == ctx->subQRes.rspList) {
    ctx->subQRes.rspList = taosArrayInit(32, sizeof(SQWRspItem));
    if (NULL == ctx->subQRes.rspList) {
      QW_TASK_ELOG("taosArrayInit SQWRspItem list failed, error:%s", tstrerror(terrno));
      return terrno;
    }
  }

  SQWRspItem item = {0};
  code = qwSaveSubQueryFetchRsp(QW_FPARAMS(), ctx, &item, rsp, dataLen, code);
  if (code) {
    return code;
  }

  return qwSaveAndHandleWailtList(QW_FPARAMS(), ctx, &item, queryEnd);
}

int32_t qwChkSaveSubQFetchRsp(QW_FPARAMS_DEF, SQWTaskCtx *ctx, void* rsp, int32_t dataLen, int32_t code, bool queryEnd) {
  return QW_IS_SCALAR_SUBQ(ctx) ? qwChkSaveScalarSubQRsp(QW_FPARAMS(), ctx, rsp, dataLen, code, queryEnd) : qwChkSaveNonScalarSubQRsp(QW_FPARAMS(), ctx, rsp, dataLen, code, queryEnd);
}


int32_t qwBuildAndSendErrorRsp(int32_t rspType, SRpcHandleInfo *pConn, int32_t code) {
  SRpcMsg rpcRsp = {
      .msgType = rspType,
      .pCont = NULL,
      .contLen = 0,
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendQueryRsp(int32_t rspType, SRpcHandleInfo *pConn, int32_t code, SQWTaskCtx *ctx) {
  int64_t         affectedRows = ctx ? ctx->affectedRows : 0;
  SQueryTableRsp  rsp = {0};
  rsp.code = code;
  rsp.affectedRows = affectedRows;
  rsp.tbVerInfo = ctx->tbInfo;

  int32_t msgSize = tSerializeSQueryTableRsp(NULL, 0, &rsp);
  if (msgSize < 0) {
    qError("tSerializeSQueryTableRsp failed");
    QW_RET(msgSize);
  }
  
  void *pRsp = rpcMallocCont(msgSize);
  if (NULL == pRsp) {
    qError("rpcMallocCont %d failed", msgSize);
    QW_RET(terrno);
  }

  msgSize = tSerializeSQueryTableRsp(pRsp, msgSize, &rsp);
  if (msgSize < 0) {
    qError("tSerializeSQueryTableRsp %d failed", msgSize);
    QW_RET(msgSize);
  }

  SRpcMsg rpcRsp = {
      .msgType = rspType,
      .pCont = pRsp,
      .contLen = msgSize,
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendExplainRsp(SRpcHandleInfo *pConn, SArray *pExecList) {
  SExplainExecInfo *pInfo = taosArrayGet(pExecList, 0);
  SExplainRsp       rsp = {.numOfPlans = taosArrayGetSize(pExecList), .subplanInfo = pInfo};

  int32_t contLen = tSerializeSExplainRsp(NULL, 0, &rsp);
  if (contLen < 0) {
    qError("tSerializeSExplainRsp failed, error: %x", terrno);
    QW_RET(terrno);
  }
  void   *pRsp = rpcMallocCont(contLen);
  if (NULL == pRsp) {
    QW_RET(terrno);
  }
  contLen = tSerializeSExplainRsp(pRsp, contLen, &rsp);
  if (contLen < 0) {
    qError("tSerializeSExplainRsp second failed, error: %x", terrno);
    QW_RET(terrno);
  }

  SRpcMsg rpcRsp = {
      .msgType = TDMT_SCH_EXPLAIN_RSP,
      .pCont = pRsp,
      .contLen = contLen,
      .code = 0,
      .info = *pConn,
  };

  rpcRsp.info.ahandle = NULL;
  tmsgSendRsp(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendHbRsp(SRpcHandleInfo *pConn, SSchedulerHbRsp *pStatus, int32_t code) {
  int32_t contLen = tSerializeSSchedulerHbRsp(NULL, 0, pStatus);
  if (contLen < 0) {
    qError("tSerializeSSchedulerHbRsp failed, error: %x", terrno);
    QW_RET(terrno);
  }

  void   *pRsp = rpcMallocCont(contLen);
  if (NULL == pRsp) {
    QW_RET(terrno);
  }
  contLen = tSerializeSSchedulerHbRsp(pRsp, contLen, pStatus);
  if (contLen < 0) {
    qError("tSerializeSSchedulerHbRsp second failed, error: %x", terrno);
    QW_RET(terrno);
  }

  SRpcMsg rpcRsp = {
      .msgType = TDMT_SCH_QUERY_HEARTBEAT_RSP,
      .contLen = contLen,
      .pCont = pRsp,
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendFetchRsp(SQWTaskCtx *ctx, int32_t rspType, SRpcHandleInfo *pConn, SRetrieveTableRsp *pRsp, int32_t dataLength,
                               int32_t code) {
  if (NULL == pRsp) {
    pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    if (NULL == pRsp) {
      QW_RET(terrno);
    }
    TAOS_MEMSET(pRsp, 0, sizeof(SRetrieveTableRsp));
    dataLength = 0;
  }

  SRpcMsg rpcRsp = {
      .msgType = rspType,
      .pCont = pRsp,
      .contLen = sizeof(*pRsp) + dataLength,
      .code = code,
      .info = *pConn,
  };

  rpcRsp.info.compressed = pRsp->compressed;
  tmsgSendRsp(&rpcRsp);

  if (NULL != ctx) {
    ctx->lastAckTs = taosGetTimestampSec();
  }

  return TSDB_CODE_SUCCESS;
}

#if 0
int32_t qwBuildAndSendCancelRsp(SRpcHandleInfo *pConn, int32_t code) {
  STaskCancelRsp *pRsp = (STaskCancelRsp *)rpcMallocCont(sizeof(STaskCancelRsp));
  if (NULL == pRsp) {
    QW_RET(terrno);
  }
  pRsp->code = code;

  SRpcMsg rpcRsp = {
      .msgType = TDMT_SCH_CANCEL_TASK_RSP,
      .pCont = pRsp,
      .contLen = sizeof(*pRsp),
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendDropRsp(SRpcHandleInfo *pConn, int32_t code) {
  STaskDropRsp *pRsp = (STaskDropRsp *)rpcMallocCont(sizeof(STaskDropRsp));
  if (NULL == pRsp) {
    QW_RET(terrno);
  }
  pRsp->code = code;

  SRpcMsg rpcRsp = {
      .msgType = TDMT_SCH_DROP_TASK_RSP,
      .pCont = pRsp,
      .contLen = sizeof(*pRsp),
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t qwBuildAndSendDropMsg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn) {
  STaskDropReq qMsg;
  qMsg.header.vgId = mgmt->nodeId;
  qMsg.header.contLen = 0;
  qMsg.sId = sId;
  qMsg.queryId = qId;
  qMsg.clientId = cId;
  qMsg.taskId = tId;
  qMsg.refId = rId;
  qMsg.execId = eId;
  
  int32_t msgSize = tSerializeSTaskDropReq(NULL, 0, &qMsg);
  if (msgSize < 0) {
    QW_SCH_TASK_ELOG("tSerializeSTaskDropReq get size, msgSize:%d", msgSize);
    QW_ERR_RET(msgSize);
  }
  
  void *msg = rpcMallocCont(msgSize);
  if (NULL == msg) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", msgSize);
    QW_ERR_RET(terrno);
  }

  msgSize = tSerializeSTaskDropReq(msg, msgSize, &qMsg);
  if (msgSize < 0) {
    QW_SCH_TASK_ELOG("tSerializeSTaskDropReq failed, msgSize:%d", msgSize);
    rpcFreeCont(msg);
    QW_ERR_RET(msgSize);
  }

  SRpcMsg pNewMsg = {
      .msgType = TDMT_SCH_DROP_TASK,
      .pCont = msg,
      .contLen = msgSize,
      .code = 0,
      .info = *pConn,
  };

  int32_t code = tmsgPutToQueue(&mgmt->msgCb, FETCH_QUEUE, &pNewMsg);
  if (TSDB_CODE_SUCCESS != code) {
    QW_SCH_TASK_ELOG("put drop task msg to queue failed, vgId:%d, code:%s", mgmt->nodeId, tstrerror(code));
    QW_ERR_RET(code);
  }

  QW_SCH_TASK_DLOG("drop task msg put to queue, vgId:%d", mgmt->nodeId);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCQueryMsg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn) {
  SQueryContinueReq *req = (SQueryContinueReq *)rpcMallocCont(sizeof(SQueryContinueReq));
  if (NULL == req) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", (int32_t)sizeof(SQueryContinueReq));
    QW_ERR_RET(terrno);
  }

  req->header.vgId = mgmt->nodeId;
  req->sId = sId;
  req->queryId = qId;
  req->clientId = cId;
  req->taskId = tId;
  req->execId = eId;

  SRpcMsg pNewMsg = {
      .msgType = TDMT_SCH_QUERY_CONTINUE,
      .pCont = req,
      .contLen = sizeof(SQueryContinueReq),
      .code = 0,
      .info = *pConn,
  };

  int32_t code = tmsgPutToQueue(&mgmt->msgCb, QUERY_QUEUE, &pNewMsg);
  if (TSDB_CODE_SUCCESS != code) {
    QW_SCH_TASK_ELOG("put query continue msg to queue failed, vgId:%d, code:%s", mgmt->nodeId, tstrerror(code));
    QW_ERR_RET(code);
  }

  QW_SCH_TASK_DLOG("query continue msg put to queue, vgId:%d", mgmt->nodeId);

  return TSDB_CODE_SUCCESS;
}

int32_t qwRegisterQueryBrokenLinkArg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn) {
  STaskDropReq qMsg;
  qMsg.header.vgId = mgmt->nodeId;
  qMsg.header.contLen = 0;
  qMsg.sId = sId;
  qMsg.queryId = qId;
  qMsg.clientId = cId;
  qMsg.taskId = tId;
  qMsg.refId = rId;
  qMsg.execId = eId;
  
  int32_t msgSize = tSerializeSTaskDropReq(NULL, 0, &qMsg);
  if (msgSize < 0) {
    QW_SCH_TASK_ELOG("tSerializeSTaskDropReq get size, msgSize:%d", msgSize);
    QW_ERR_RET(msgSize);
  }
  
  void *msg = rpcMallocCont(msgSize);
  if (NULL == msg) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", msgSize);
    QW_ERR_RET(terrno);
  }

  msgSize = tSerializeSTaskDropReq(msg, msgSize, &qMsg);
  if (msgSize < 0) {
    QW_SCH_TASK_ELOG("tSerializeSTaskDropReq failed, msgSize:%d", msgSize);
    rpcFreeCont(msg);
    QW_ERR_RET(msgSize);
  }

  SRpcMsg brokenMsg = {
      .msgType = TDMT_SCH_DROP_TASK,
      .pCont = msg,
      .contLen = msgSize,
      .code = TSDB_CODE_RPC_BROKEN_LINK,
      .info = *pConn,
  };

  tmsgRegisterBrokenLinkArg(&brokenMsg);

  return TSDB_CODE_SUCCESS;
}

int32_t qwRegisterHbBrokenLinkArg(SQWorker *mgmt, uint64_t clientId, SRpcHandleInfo *pConn) {
  SSchedulerHbReq req = {0};
  req.header.vgId = mgmt->nodeId;
  req.clientId = clientId;

  int32_t msgSize = tSerializeSSchedulerHbReq(NULL, 0, &req);
  if (msgSize < 0) {
    QW_SCH_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
    QW_ERR_RET(msgSize);
  }
  void *msg = rpcMallocCont(msgSize);
  if (NULL == msg) {
    QW_SCH_ELOG("calloc %d failed", msgSize);
    QW_ERR_RET(terrno);
  }

  msgSize = tSerializeSSchedulerHbReq(msg, msgSize, &req);
  if (msgSize < 0) {
    QW_SCH_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
    rpcFreeCont(msg);
    QW_ERR_RET(msgSize);
  }

  SRpcMsg brokenMsg = {
      .msgType = TDMT_SCH_QUERY_HEARTBEAT,
      .pCont = msg,
      .contLen = msgSize,
      .code = TSDB_CODE_RPC_BROKEN_LINK,
      .info = *pConn,
  };

  tmsgRegisterBrokenLinkArg(&brokenMsg);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerPreprocessQueryMsg(void *qWorkerMgmt, SRpcMsg *pMsg, bool chkGrant, int32_t* qType) {
  if (NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t       code = 0;
  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;
  SSubQueryMsg  msg = {0};
  if (tDeserializeSSubQueryMsg(pMsg->pCont, pMsg->contLen, &msg) < 0) {
    QW_ELOG("tDeserializeSSubQueryMsg failed, contLen:%d", pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (chkGrant) {
    if ((!TEST_SHOW_REWRITE_MASK(msg.msgMask))) {
      if ((code = taosGranted(TSDB_GRANT_ALL)) < 0) {
        QW_ELOG("query failed cause of grant expired, msgMask:%d", msg.msgMask);
        tFreeSSubQueryMsg(&msg);
        QW_ERR_RET(code);
      }
      if ((TEST_VIEW_MASK(msg.msgMask)) && ((code = taosGranted(TSDB_GRANT_VIEW)) < 0)) {
        QW_ELOG("query failed cause of view grant expired, msgMask:%d", msg.msgMask);
        tFreeSSubQueryMsg(&msg);
        QW_ERR_RET(code);
      }
      if ((TEST_AUDIT_MASK(msg.msgMask)) && ((code = taosGranted(TSDB_GRANT_AUDIT)) < 0)) {
        QW_ELOG("query failed cause of audit grant expired, msgMask:%d", msg.msgMask);
        tFreeSSubQueryMsg(&msg);
        QW_ERR_RET(code);
      }
    }
  }

  uint64_t sId = msg.sId;
  uint64_t qId = msg.queryId;
  uint64_t cId = msg.clientId;
  uint64_t tId = msg.taskId;
  int64_t  rId = msg.refId;
  int32_t  eId = msg.execId;

  *qType = msg.taskType;  // task type: TASK_TYPE_HQUERY or TASK_TYPE_QUERY

  SQWMsg qwMsg = {
      .msgType = pMsg->msgType, .msg = msg.msg, .msgLen = msg.msgLen, .connInfo = pMsg->info, .msgMask = msg.msgMask, .subQType = msg.subQType};

  QW_SCH_TASK_DLOG("prerocessQuery start, handle:%p, SQL:%s", pMsg->info.handle, msg.sql);
  code = qwPreprocessQuery(QW_FPARAMS(), &qwMsg);
  QW_SCH_TASK_DLOG("prerocessQuery end, handle:%p, code:%x", pMsg->info.handle, code);

  tFreeSSubQueryMsg(&msg);

  return code;
}

int32_t qWorkerAbortPreprocessQueryMsg(void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t       code = 0;
  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;
  SSubQueryMsg msg = {0};
  if (tDeserializeSSubQueryMsg(pMsg->pCont, pMsg->contLen, &msg) < 0) {
    QW_ELOG("tDeserializeSSubQueryMsg failed, contLen:%d", pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg.sId;
  uint64_t qId = msg.queryId;
  uint64_t cId = msg.clientId;
  uint64_t tId = msg.taskId;
  int64_t  rId = msg.refId;
  int32_t  eId = msg.execId;

  QW_SCH_TASK_DLOG("Abort prerocessQuery start, handle:%p", pMsg->info.handle);
  code = qwAbortPrerocessQuery(QW_FPARAMS());
  QW_SCH_TASK_DLOG("Abort prerocessQuery end, handle:%p, code:%x", pMsg->info.handle, code);

  tFreeSSubQueryMsg(&msg);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t       code = 0;
  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, QUERY_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.queryProcessed, 1);

  SSubQueryMsg  msg = {0};
  if (tDeserializeSSubQueryMsg(pMsg->pCont, pMsg->contLen, &msg) < 0) {
    QW_ELOG("tDeserializeSSubQueryMsg failed, contLen:%d", pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg.sId;
  uint64_t qId = msg.queryId;
  uint64_t cId = msg.clientId;
  uint64_t tId = msg.taskId;
  int64_t  rId = msg.refId;
  int32_t  eId = msg.execId;

  SQWMsg qwMsg = {.node = node, .msg = msg.msg, .msgLen = msg.msgLen, .connInfo = pMsg->info, .msgType = pMsg->msgType, .code = pMsg->code, .subEndPoints = msg.subEndPoints};
  qwMsg.msgInfo.explain = msg.explain;
  qwMsg.msgInfo.taskType = msg.taskType;
  qwMsg.msgInfo.needFetch = msg.needFetch;
  qwMsg.msgInfo.compressMsg = msg.compress;

  QW_SCH_TASK_DLOG("processQuery start, node:%p, type:%s, compress:%d, handle:%p, SQL:%s, code:0x%x, subEndPointsNum:%d", 
    node, TMSG_INFO(pMsg->msgType), msg.compress, pMsg->info.handle, msg.sql, qwMsg.code, (int32_t)taosArrayGetSize(qwMsg.subEndPoints));

  code = qwProcessQuery(QW_FPARAMS(), &qwMsg, msg.sql);
  msg.sql = NULL;

  QW_SCH_TASK_DLOG("processQuery end, node:%p, code:%x", node, code);
  tFreeSSubQueryMsg(&msg);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessCQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t            code = 0;
  int8_t             status = 0;
  bool               queryDone = false;
  SQueryContinueReq *msg = (SQueryContinueReq *)pMsg->pCont;
  bool               needStop = false;
  SQWTaskCtx        *handles = NULL;
  SQWorker          *mgmt = (SQWorker *)qWorkerMgmt;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, QUERY_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.cqueryProcessed, 1);

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid cquery msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t cId = msg->clientId;
  uint64_t tId = msg->taskId;
  int64_t  rId = 0;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connInfo = pMsg->info};

  QW_SCH_TASK_DLOG("processCQuery start, node:%p, handle:%p", node, pMsg->info.handle);

  code = qwProcessCQuery(QW_FPARAMS(), &qwMsg);

  QW_SCH_TASK_DLOG("processCQuery end, node:%p, code:0x%x", node, code);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResFetchReq req = {0};
  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.fetchProcessed, 1);

  if (tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    QW_ELOG("tDeserializeSResFetchReq %d failed", pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = req.sId;
  uint64_t qId = req.queryId;
  uint64_t cId = req.clientId;
  uint64_t tId = req.taskId;
  int64_t  rId = 0;
  int32_t  eId = req.execId;

  SQWMsg qwMsg = {.req = &req, .node = node, .msg = req.pOpParam, .msgLen = 0, .connInfo = pMsg->info, .msgType = pMsg->msgType};

  QW_SCH_TASK_DLOG("processFetch start, node:%p, handle:%p", node, pMsg->info.handle);

  int32_t code = qwProcessFetch(QW_FPARAMS(), &qwMsg);

  QW_SCH_TASK_DLOG("processFetch end, node:%p, code:%x", node, code);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessRspMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  SQWorker *mgmt = (SQWorker *)qWorkerMgmt;
  if (mgmt) {
    QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE));
    QW_STAT_INC(mgmt->stat.msgStat.rspProcessed, 1);
  }

  qProcessRspMsg(NULL, pMsg, NULL);
  pMsg->pCont = NULL;
  return TSDB_CODE_SUCCESS;
}

#if 0
int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SQWorker       *mgmt = (SQWorker *)qWorkerMgmt;
  int32_t         code = 0;
  STaskCancelReq *msg = pMsg->pCont;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.cancelProcessed, 1);

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task cancel msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->clientId = be64toh(msg->clientId);
  msg->taskId = be64toh(msg->taskId);
  msg->refId = be64toh(msg->refId);
  msg->execId = ntohl(msg->execId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t cId = msg->clientId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connInfo = pMsg->info};

  // QW_ERR_JRET(qwCancelTask(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendCancelRsp(&qwMsg.connInfo, code));
  QW_SCH_TASK_DLOG("cancel rsp send, handle:%p, code:%x - %s", qwMsg.connInfo.handle, code, tstrerror(code));

  return TSDB_CODE_SUCCESS;
}
#endif

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t       code = 0;
  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.dropProcessed, 1);

  STaskDropReq  msg = {0};
  if (tDeserializeSTaskDropReq(pMsg->pCont, pMsg->contLen, &msg) < 0) {
    QW_ELOG("tDeserializeSTaskDropReq failed, contLen:%d", pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg.sId;
  uint64_t qId = msg.queryId;
  uint64_t cId = msg.clientId;
  uint64_t tId = msg.taskId;
  int64_t  rId = msg.refId;
  int32_t  eId = msg.execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .code = pMsg->code, .connInfo = pMsg->info};

  if (TSDB_CODE_RPC_BROKEN_LINK == pMsg->code) {
    QW_SCH_TASK_DLOG("receive drop task due to network broken, error:%s", tstrerror(pMsg->code));
  }

  QW_SCH_TASK_DLOG("processDrop start, node:%p, handle:%p", node, pMsg->info.handle);

  code = qwProcessDrop(QW_FPARAMS(), &qwMsg);

  QW_SCH_TASK_DLOG("processDrop end, node:%p, code:%x", node, code);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessNotifyMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t       code = 0;
  SQWorker     *mgmt = (SQWorker *)qWorkerMgmt;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.notifyProcessed, 1);

  STaskNotifyReq  msg = {0};
  if (tDeserializeSTaskNotifyReq(pMsg->pCont, pMsg->contLen, &msg) < 0) {
    QW_ELOG("tDeserializeSTaskNotifyReq failed, contLen:%d", pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg.sId;
  uint64_t qId = msg.queryId;
  uint64_t cId = msg.clientId;
  uint64_t tId = msg.taskId;
  int64_t  rId = msg.refId;
  int32_t  eId = msg.execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .code = pMsg->code, .connInfo = pMsg->info, .msgType = msg.type};

  QW_SCH_TASK_DLOG("processNotify start, node:%p, handle:%p", node, pMsg->info.handle);

  code = qwProcessNotify(QW_FPARAMS(), &qwMsg);

  QW_SCH_TASK_DLOG("processNotify end, node:%p, code:%x", node, code);

  return TSDB_CODE_SUCCESS;
}


int32_t qWorkerProcessHbMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t         code = 0;
  SSchedulerHbReq req = {0};
  SQWorker       *mgmt = (SQWorker *)qWorkerMgmt;
  uint64_t        clientId = 0;

  QW_ERR_RET(qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE));
  QW_STAT_INC(mgmt->stat.msgStat.hbProcessed, 1);

  if (NULL == pMsg->pCont) {
    QW_ELOG("invalid hb msg, msg:%p, msgLen:%d", pMsg->pCont, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (tDeserializeSSchedulerHbReq(pMsg->pCont, pMsg->contLen, &req)) {
    QW_ELOG("invalid hb msg, msg:%p, msgLen:%d", pMsg->pCont, pMsg->contLen);
    tFreeSSchedulerHbReq(&req);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  clientId = req.clientId;

  SQWMsg   qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .code = pMsg->code, .connInfo = pMsg->info};
  if (TSDB_CODE_RPC_BROKEN_LINK == pMsg->code) {
    QW_SCH_DLOG("receive Hb msg due to network broken, error:%s", tstrerror(pMsg->code));
  }

  QW_SCH_DLOG("processHb start, node:%p, handle:%p", node, pMsg->info.handle);

  code = qwProcessHb(mgmt, &qwMsg, &req);

  QW_SCH_DLOG("processHb end, node:%p, code:%x", node, code);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessDeleteMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, SDeleteRes *pRes) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t     code = 0;
  SVDeleteReq req = {0};
  SQWorker   *mgmt = (SQWorker *)qWorkerMgmt;

  QW_STAT_INC(mgmt->stat.msgStat.deleteProcessed, 1);

  QW_ERR_RET(tDeserializeSVDeleteReq(pMsg->pCont, pMsg->contLen, &req));

  uint64_t sId = req.sId;
  uint64_t qId = req.queryId;
  uint64_t cId = req.clientId;
  uint64_t tId = req.taskId;
  int64_t  rId = 0;
  int32_t  eId = -1;
  pRes->source = req.source;

  SQWMsg qwMsg = {.node = node, .msg = req.msg, .msgLen = req.phyLen, .connInfo = pMsg->info};
  QW_SCH_TASK_DLOG("processDelete start, node:%p, handle:%p, sql:%s", node, pMsg->info.handle, req.sql);
  taosMemoryFreeClear(req.sql);

  QW_ERR_JRET(qwProcessDelete(QW_FPARAMS(), &qwMsg, pRes));

  taosMemoryFreeClear(req.msg);
  QW_SCH_TASK_DLOG("processDelete end, node:%p", node);

_return:

  QW_RET(code);
}
