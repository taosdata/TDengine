#include "qwMsg.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qworker.h"
#include "qwInt.h"
#include "tcommon.h"
#include "tmsg.h"
#include "tname.h"

int32_t qwMallocFetchRsp(int32_t length, SRetrieveTableRsp **rsp) {
  int32_t msgSize = sizeof(SRetrieveTableRsp) + length;

  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcReallocCont(*rsp, msgSize);
  if (NULL == pRsp) {
    qError("rpcMallocCont %d failed", msgSize);
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (NULL == *rsp) {
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
  }
  
  *rsp = pRsp;

  return TSDB_CODE_SUCCESS;
}

void qwBuildFetchRsp(void *msg, SOutputData *input, int32_t len, bool qComplete) {
  SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)msg;

  rsp->useconds = htobe64(input->useconds);
  rsp->completed = qComplete;
  rsp->precision = input->precision;
  rsp->compressed = input->compressed;
  rsp->compLen = htonl(len);
  rsp->numOfRows = htonl(input->numOfRows);
  rsp->numOfCols = htonl(input->numOfCols);
  rsp->numOfBlocks = htonl(input->numOfBlocks);
}

void qwFreeFetchRsp(void *msg) {
  if (msg) {
    rpcFreeCont(msg);
  }
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
  STbVerInfo* tbInfo = ctx ? &ctx->tbInfo : NULL;
  int64_t affectedRows = ctx ? ctx->affectedRows : 0;
  SQueryTableRsp *pRsp = (SQueryTableRsp *)rpcMallocCont(sizeof(SQueryTableRsp));
  pRsp->code = htonl(code);
  pRsp->affectedRows = htobe64(affectedRows);
  if (tbInfo) {
    strcpy(pRsp->tbFName, tbInfo->tbFName);
    pRsp->sversion = htonl(tbInfo->sversion);
    pRsp->tversion = htonl(tbInfo->tversion);
  }

  SRpcMsg rpcRsp = {
      .msgType = rspType,
      .pCont = pRsp,
      .contLen = sizeof(*pRsp),
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendExplainRsp(SRpcHandleInfo *pConn, SArray* pExecList) {
  SExplainExecInfo* pInfo = taosArrayGet(pExecList, 0);
  SExplainRsp rsp = {.numOfPlans = taosArrayGetSize(pExecList), .subplanInfo = pInfo};

  int32_t contLen = tSerializeSExplainRsp(NULL, 0, &rsp);
  void *  pRsp = rpcMallocCont(contLen);
  tSerializeSExplainRsp(pRsp, contLen, &rsp);

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
  void *  pRsp = rpcMallocCont(contLen);
  tSerializeSSchedulerHbRsp(pRsp, contLen, pStatus);

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

int32_t qwBuildAndSendFetchRsp(int32_t rspType, SRpcHandleInfo *pConn, SRetrieveTableRsp *pRsp, int32_t dataLength, int32_t code) {
  if (NULL == pRsp) {
    pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
    dataLength = 0;
  }

  SRpcMsg rpcRsp = {
      .msgType = rspType,
      .pCont = pRsp,
      .contLen = sizeof(*pRsp) + dataLength,
      .code = code,
      .info = *pConn,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCancelRsp(SRpcHandleInfo *pConn, int32_t code) {
  STaskCancelRsp *pRsp = (STaskCancelRsp *)rpcMallocCont(sizeof(STaskCancelRsp));
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

int32_t qwBuildAndSendDropMsg(QW_FPARAMS_DEF, SRpcHandleInfo *pConn) {
  STaskDropReq *req = (STaskDropReq *)rpcMallocCont(sizeof(STaskDropReq));
  if (NULL == req) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", (int32_t)sizeof(STaskDropReq));
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  req->header.vgId = mgmt->nodeId;
  req->sId = sId;
  req->queryId = qId;
  req->taskId = tId;
  req->refId = rId;
  req->execId = eId;

  SRpcMsg pNewMsg = {
      .msgType = TDMT_SCH_DROP_TASK,
      .pCont = req,
      .contLen = sizeof(STaskDropReq),
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
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  req->header.vgId = mgmt->nodeId;
  req->sId = sId;
  req->queryId = qId;
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
  STaskDropReq *req = (STaskDropReq *)rpcMallocCont(sizeof(STaskDropReq));
  if (NULL == req) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", (int32_t)sizeof(STaskDropReq));
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  req->header.vgId = htonl(mgmt->nodeId);
  req->sId = htobe64(sId);
  req->queryId = htobe64(qId);
  req->taskId = htobe64(tId);
  req->refId = htobe64(rId);

  SRpcMsg brokenMsg = {
      .msgType = TDMT_SCH_DROP_TASK,
      .pCont = req,
      .contLen = sizeof(STaskDropReq),
      .code = TSDB_CODE_RPC_BROKEN_LINK,
      .info = *pConn,
  };

  tmsgRegisterBrokenLinkArg(&brokenMsg);

  return TSDB_CODE_SUCCESS;
}

int32_t qwRegisterHbBrokenLinkArg(SQWorker *mgmt, uint64_t sId, SRpcHandleInfo *pConn) {
  SSchedulerHbReq req = {0};
  req.header.vgId = mgmt->nodeId;
  req.sId = sId;

  int32_t msgSize = tSerializeSSchedulerHbReq(NULL, 0, &req);
  if (msgSize < 0) {
    QW_SCH_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  void *msg = rpcMallocCont(msgSize);
  if (NULL == msg) {
    QW_SCH_ELOG("calloc %d failed", msgSize);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  if (tSerializeSSchedulerHbReq(msg, msgSize, &req) < 0) {
    QW_SCH_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
    taosMemoryFree(msg);
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
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

int32_t qWorkerPreprocessQueryMsg(void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t       code = 0;
  SSubQueryMsg *msg = pMsg->pCont;
  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;

  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    QW_ELOG("invalid query msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->refId = be64toh(msg->refId);
  msg->execId = ntohl(msg->execId);
  msg->phyLen = ntohl(msg->phyLen);
  msg->sqlLen = ntohl(msg->sqlLen);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.msgType = pMsg->msgType, .msg = msg->msg + msg->sqlLen, .msgLen = msg->phyLen, .connInfo = pMsg->info};

  QW_SCH_TASK_DLOG("prerocessQuery start, handle:%p", pMsg->info.handle);
  QW_ERR_RET(qwPreprocessQuery(QW_FPARAMS(), &qwMsg));
  QW_SCH_TASK_DLOG("prerocessQuery end, handle:%p", pMsg->info.handle);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerAbortPreprocessQueryMsg(void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SSubQueryMsg *msg = pMsg->pCont;
  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;
  int32_t  eId = msg->execId;

  QW_SCH_TASK_DLOG("Abort prerocessQuery start, handle:%p", pMsg->info.handle);
  qwAbortPrerocessQuery(QW_FPARAMS());
  QW_SCH_TASK_DLOG("Abort prerocessQuery end, handle:%p", pMsg->info.handle);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t       code = 0;
  SSubQueryMsg *msg = pMsg->pCont;
  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;

  qwUpdateTimeInQueue(mgmt, ts, QUERY_QUEUE);
  QW_STAT_INC(mgmt->stat.msgStat.queryProcessed, 1);

  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    QW_ELOG("invalid query msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.node = node, .msg = msg->msg + msg->sqlLen, .msgLen = msg->phyLen, .connInfo = pMsg->info, .msgType = pMsg->msgType};
  qwMsg.msgInfo.explain = msg->explain;
  qwMsg.msgInfo.taskType = msg->taskType;
  qwMsg.msgInfo.needFetch = msg->needFetch;
  
  char * sql = strndup(msg->msg, msg->sqlLen);
  QW_SCH_TASK_DLOG("processQuery start, node:%p, type:%s, handle:%p, SQL:%s", node, TMSG_INFO(pMsg->msgType), pMsg->info.handle, sql);
  QW_ERR_JRET(qwProcessQuery(QW_FPARAMS(), &qwMsg, sql));

_return:

  QW_SCH_TASK_DLOG("processQuery end, node:%p, code:%d", node, code);

  return code;
}

int32_t qWorkerProcessCQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  int32_t            code = 0;
  int8_t             status = 0;
  bool               queryDone = false;
  SQueryContinueReq *msg = (SQueryContinueReq *)pMsg->pCont;
  bool               needStop = false;
  SQWTaskCtx *       handles = NULL;
  SQWorker *         mgmt = (SQWorker *)qWorkerMgmt;

  qwUpdateTimeInQueue(mgmt, ts, QUERY_QUEUE);
  QW_STAT_INC(mgmt->stat.msgStat.cqueryProcessed, 1);

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid cquery msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = 0;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connInfo = pMsg->info};

  QW_SCH_TASK_DLOG("processCQuery start, node:%p, handle:%p", node, pMsg->info.handle);

  QW_ERR_RET(qwProcessCQuery(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processCQuery end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResFetchReq *msg = pMsg->pCont;
  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;

  qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE);
  QW_STAT_INC(mgmt->stat.msgStat.fetchProcessed, 1);

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid fetch msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->execId = ntohl(msg->execId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = 0;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connInfo = pMsg->info, .msgType = pMsg->msgType};

  QW_SCH_TASK_DLOG("processFetch start, node:%p, handle:%p", node, pMsg->info.handle);

  QW_ERR_RET(qwProcessFetch(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processFetch end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessRspMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  SQWorker *      mgmt = (SQWorker *)qWorkerMgmt;
  if (mgmt) {
    qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE);
    QW_STAT_INC(mgmt->stat.msgStat.rspProcessed, 1);
  }

  qProcessRspMsg(NULL, pMsg, NULL);
  pMsg->pCont = NULL;
  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SQWorker *      mgmt = (SQWorker *)qWorkerMgmt;
  int32_t         code = 0;
  STaskCancelReq *msg = pMsg->pCont;

  qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE);
  QW_STAT_INC(mgmt->stat.msgStat.cancelProcessed, 1);

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task cancel msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->refId = be64toh(msg->refId);
  msg->execId = ntohl(msg->execId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
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

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t       code = 0;
  STaskDropReq *msg = pMsg->pCont;
  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;

  qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE);
  QW_STAT_INC(mgmt->stat.msgStat.dropProcessed, 1);

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid task drop msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->refId = be64toh(msg->refId);
  msg->execId = ntohl(msg->execId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;
  int32_t  eId = msg->execId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .code = pMsg->code, .connInfo = pMsg->info};

  if (TSDB_CODE_RPC_BROKEN_LINK == pMsg->code) {
    QW_SCH_TASK_DLOG("receive drop task due to network broken, error:%s", tstrerror(pMsg->code));
  }

  QW_SCH_TASK_DLOG("processDrop start, node:%p, handle:%p", node, pMsg->info.handle);

  QW_ERR_RET(qwProcessDrop(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processDrop end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessHbMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, int64_t ts) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t         code = 0;
  SSchedulerHbReq req = {0};
  SQWorker *      mgmt = (SQWorker *)qWorkerMgmt;

  qwUpdateTimeInQueue(mgmt, ts, FETCH_QUEUE);
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

  uint64_t sId = req.sId;
  SQWMsg   qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .code = pMsg->code, .connInfo = pMsg->info};
  if (TSDB_CODE_RPC_BROKEN_LINK == pMsg->code) {
    QW_SCH_DLOG("receive Hb msg due to network broken, error:%s", tstrerror(pMsg->code));
  }

  QW_SCH_DLOG("processHb start, node:%p, handle:%p", node, pMsg->info.handle);

  QW_ERR_RET(qwProcessHb(mgmt, &qwMsg, &req));

  QW_SCH_DLOG("processHb end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}


int32_t qWorkerProcessDeleteMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg, SDeleteRes *pRes) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t       code = 0;
  SVDeleteReq req = {0};
  SQWorker *    mgmt = (SQWorker *)qWorkerMgmt;

  QW_STAT_INC(mgmt->stat.msgStat.deleteProcessed, 1);

  tDeserializeSVDeleteReq(pMsg->pCont, pMsg->contLen, &req);
  
  uint64_t sId = req.sId;
  uint64_t qId = req.queryId;
  uint64_t tId = req.taskId;
  int64_t  rId = 0;
  int32_t  eId = -1;

  SQWMsg qwMsg = {.node = node, .msg = req.msg, .msgLen = req.phyLen, .connInfo = pMsg->info};
  QW_SCH_TASK_DLOG("processDelete start, node:%p, handle:%p, sql:%s", node, pMsg->info.handle, req.sql);
  taosMemoryFreeClear(req.sql);

  QW_ERR_JRET(qwProcessDelete(QW_FPARAMS(), &qwMsg, pRes));

  QW_SCH_TASK_DLOG("processDelete end, node:%p", node);

_return:

  QW_RET(code);
}


