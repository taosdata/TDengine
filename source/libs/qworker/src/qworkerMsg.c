#include "qworker.h"
#include "tcommon.h"
#include "executor.h"
#include "planner.h"
#include "query.h"
#include "qworkerInt.h"
#include "qworkerMsg.h"
#include "tmsg.h"
#include "tname.h"
#include "dataSinkMgt.h"


int32_t qwMallocFetchRsp(int32_t length, SRetrieveTableRsp **rsp) {
  int32_t msgSize = sizeof(SRetrieveTableRsp) + length;
  
  SRetrieveTableRsp *pRsp = (SRetrieveTableRsp *)rpcMallocCont(msgSize);
  if (NULL == pRsp) {
    qError("rpcMallocCont %d failed", msgSize);
    QW_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  memset(pRsp, 0, sizeof(SRetrieveTableRsp));

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
}


void qwFreeFetchRsp(void *msg) {
  if (msg) {
    rpcFreeCont(msg);
  }
}

int32_t qwBuildAndSendQueryRsp(SQWConnInfo *pConn, int32_t code) {
  SQueryTableRsp rsp = {.code = code};
  
  int32_t contLen = tSerializeSQueryTableRsp(NULL, 0, &rsp);
  void *msg = rpcMallocCont(contLen);
  tSerializeSQueryTableRsp(msg, contLen, &rsp);

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_QUERY_RSP,
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .pCont   = msg,
    .contLen = contLen,
    .code    = code,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendReadyRsp(SQWConnInfo *pConn, int32_t code) {
  SResReadyRsp *pRsp = (SResReadyRsp *)rpcMallocCont(sizeof(SResReadyRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_RES_READY_RSP,
    .handle  = pConn->handle,
    .ahandle = NULL,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendExplainRsp(SQWConnInfo *pConn, SExplainExecInfo *execInfo, int32_t num) {  
  SExplainRsp rsp = {.numOfPlans = num, .subplanInfo = execInfo};

  int32_t contLen = tSerializeSExplainRsp(NULL, 0, &rsp);
  void *pRsp = rpcMallocCont(contLen);
  tSerializeSExplainRsp(pRsp, contLen, &rsp);

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_EXPLAIN_RSP,
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .pCont   = pRsp,
    .contLen = contLen,
    .code    = 0,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendHbRsp(SQWConnInfo *pConn, SSchedulerHbRsp *pStatus, int32_t code) {
  int32_t contLen = tSerializeSSchedulerHbRsp(NULL, 0, pStatus);
  void *pRsp = rpcMallocCont(contLen);
  tSerializeSSchedulerHbRsp(pRsp, contLen, pStatus);

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_QUERY_HEARTBEAT_RSP,
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .pCont   = pRsp,
    .contLen = contLen,
    .code    = code,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendFetchRsp(SQWConnInfo *pConn, SRetrieveTableRsp *pRsp, int32_t dataLength, int32_t code) {
  if (NULL == pRsp) {
    pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
    dataLength = 0;
  }

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_FETCH_RSP,
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp) + dataLength,
    .code    = code,
  };

  tmsgSendRsp(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCancelRsp(SQWConnInfo *pConn, int32_t code) {
  STaskCancelRsp *pRsp = (STaskCancelRsp *)rpcMallocCont(sizeof(STaskCancelRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_CANCEL_TASK_RSP,
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  tmsgSendRsp(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendDropRsp(SQWConnInfo *pConn, int32_t code) {
  STaskDropRsp *pRsp = (STaskDropRsp *)rpcMallocCont(sizeof(STaskDropRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_DROP_TASK_RSP,
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  tmsgSendRsp(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendShowRsp(SRpcMsg *pMsg, int32_t code) {
  int32_t         numOfCols = 6;
  SVShowTablesRsp showRsp = {0};

  // showRsp.showId = 1;
  showRsp.tableMeta.pSchemas = taosMemoryCalloc(numOfCols, sizeof(SSchema));
  if (showRsp.tableMeta.pSchemas == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  col_id_t  cols = 0;
  SSchema *pSchema = showRsp.tableMeta.pSchemas;

  const SSchema *s = tGetTbnameColumnSchema();
  *pSchema = createSchema(s->type, s->bytes, ++cols, "name");
  pSchema++;

  int32_t type = TSDB_DATA_TYPE_TIMESTAMP;
  *pSchema = createSchema(type, tDataTypes[type].bytes, ++cols, "created");
  pSchema++;

  type = TSDB_DATA_TYPE_SMALLINT;
  *pSchema = createSchema(type, tDataTypes[type].bytes, ++cols, "columns");
  pSchema++;

  *pSchema = createSchema(s->type, s->bytes, ++cols, "stable");
  pSchema++;

  type = TSDB_DATA_TYPE_BIGINT;
  *pSchema = createSchema(type, tDataTypes[type].bytes, ++cols, "uid");
  pSchema++;

  type = TSDB_DATA_TYPE_INT;
  *pSchema = createSchema(type, tDataTypes[type].bytes, ++cols, "vgId");

  assert(cols == numOfCols);
  showRsp.tableMeta.numOfColumns = cols;

  int32_t bufLen = tSerializeSShowRsp(NULL, 0, &showRsp);
  void   *pBuf = rpcMallocCont(bufLen);
  tSerializeSShowRsp(pBuf, bufLen, &showRsp);

  SRpcMsg rpcMsg = {
      .handle = pMsg->handle,
      .ahandle = pMsg->ahandle,
      .pCont = pBuf,
      .contLen = bufLen,
      .code = code,
  };

  tmsgSendRsp(&rpcMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendShowFetchRsp(SRpcMsg *pMsg, SVShowTablesFetchReq* pFetchReq) {
  SVShowTablesFetchRsp *pRsp = (SVShowTablesFetchRsp *)rpcMallocCont(sizeof(SVShowTablesFetchRsp));
  int32_t handle = htonl(pFetchReq->id);

  pRsp->numOfRows = 0;
  SRpcMsg rpcMsg = {
      .handle  = pMsg->handle,
      .ahandle = pMsg->ahandle,
      .pCont   = pRsp,
      .contLen = sizeof(*pRsp),
      .code    = 0,
  };

  tmsgSendRsp(&rpcMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCQueryMsg(QW_FPARAMS_DEF, SQWConnInfo *pConn) {
  SQueryContinueReq * req = (SQueryContinueReq *)rpcMallocCont(sizeof(SQueryContinueReq));
  if (NULL == req) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", (int32_t)sizeof(SQueryContinueReq));
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  req->header.vgId = mgmt->nodeId;
  req->sId = sId;
  req->queryId = qId;
  req->taskId = tId;

  SRpcMsg pNewMsg = {
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .msgType = TDMT_VND_QUERY_CONTINUE,
    .pCont   = req,
    .contLen = sizeof(SQueryContinueReq),
    .code    = 0,
  };

  int32_t code = tmsgPutToQueue(&mgmt->msgCb, QUERY_QUEUE, &pNewMsg);
  if (TSDB_CODE_SUCCESS != code) {
    QW_SCH_TASK_ELOG("put query continue msg to queue failed, vgId:%d, code:%s", mgmt->nodeId, tstrerror(code));
    rpcFreeCont(req);
    QW_ERR_RET(code);
  }

  QW_SCH_TASK_DLOG("query continue msg put to queue, vgId:%d", mgmt->nodeId);

  return TSDB_CODE_SUCCESS;
}


int32_t qwRegisterBrokenLinkArg(QW_FPARAMS_DEF, SQWConnInfo *pConn) {
  STaskDropReq * req = (STaskDropReq *)rpcMallocCont(sizeof(STaskDropReq));
  if (NULL == req) {
    QW_SCH_TASK_ELOG("rpcMallocCont %d failed", (int32_t)sizeof(STaskDropReq));
    QW_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }  

  req->header.vgId = htonl(mgmt->nodeId);
  req->sId = htobe64(sId);
  req->queryId = htobe64(qId);
  req->taskId = htobe64(tId);
  req->refId = htobe64(rId);
  
  SRpcMsg pMsg = {
    .handle  = pConn->handle,
    .ahandle = pConn->ahandle,
    .msgType = TDMT_VND_DROP_TASK,
    .pCont   = req,
    .contLen = sizeof(STaskDropReq),
    .code    = TSDB_CODE_RPC_NETWORK_UNAVAIL,
  };
  
  tmsgRegisterBrokenLinkArg(&mgmt->msgCb, &pMsg);

  return TSDB_CODE_SUCCESS;
}


int32_t qWorkerProcessQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  int32_t code = 0;
  SSubQueryMsg *msg = pMsg->pCont;
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  
  if (NULL == msg || pMsg->contLen <= sizeof(*msg)) {
    QW_ELOG("invalid query msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  msg->sId     = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId  = be64toh(msg->taskId);
  msg->refId   = be64toh(msg->refId);
  msg->phyLen  = ntohl(msg->phyLen);
  msg->sqlLen  = ntohl(msg->sqlLen);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;

  SQWMsg qwMsg = {.node = node, .msg = msg->msg + msg->sqlLen, .msgLen = msg->phyLen};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  char* sql = strndup(msg->msg, msg->sqlLen);
  QW_SCH_TASK_DLOG("processQuery start, node:%p, handle:%p, sql:%s", node, pMsg->handle, sql);
  taosMemoryFreeClear(sql);

  QW_ERR_RET(qwProcessQuery(QW_FPARAMS(), &qwMsg, msg->taskType, msg->explain));

  QW_SCH_TASK_DLOG("processQuery end, node:%p", node);

  return TSDB_CODE_SUCCESS;  
}

int32_t qWorkerProcessCQueryMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  int32_t code = 0;
  int8_t status = 0;
  bool queryDone = false;
  SQueryContinueReq *msg = (SQueryContinueReq *)pMsg->pCont;
  bool needStop = false;
  SQWTaskCtx *handles = NULL;
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;

  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid cquery msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t rId = 0;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  QW_SCH_TASK_DLOG("processCQuery start, node:%p, handle:%p", node, pMsg->handle);

  QW_ERR_RET(qwProcessCQuery(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processCQuery end, node:%p", node);

  return TSDB_CODE_SUCCESS;    
}

int32_t qWorkerProcessReadyMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg){
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  SResReadyReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid task ready msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t rId = 0;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  QW_SCH_TASK_DLOG("processReady start, node:%p, handle:%p", node, pMsg->handle);

  QW_ERR_RET(qwProcessReady(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processReady end, node:%p", node);
  
  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessStatusMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SSchTasksStatusReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task status msg");
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  msg->sId = htobe64(msg->sId);
  uint64_t sId = msg->sId;

  SSchedulerStatusRsp *sStatus = NULL;
  
  //QW_ERR_JRET(qwGetSchTasksStatus(qWorkerMgmt, msg->sId, &sStatus));

_return:

  //QW_ERR_RET(qwBuildAndSendStatusRsp(pMsg, sStatus));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SResFetchReq *msg = pMsg->pCont;
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid fetch msg, msg:%p, msgLen:%d", msg, pMsg->contLen);  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t rId = 0;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  QW_SCH_TASK_DLOG("processFetch start, node:%p, handle:%p", node, pMsg->handle);

  QW_ERR_RET(qwProcessFetch(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processFetch end, node:%p", node);

  return TSDB_CODE_SUCCESS;  
}

int32_t qWorkerProcessFetchRsp(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  qProcessFetchRsp(NULL, pMsg, NULL);
  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessCancelMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  int32_t code = 0;
  STaskCancelReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task cancel msg");  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->refId = be64toh(msg->refId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  //QW_ERR_JRET(qwCancelTask(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendCancelRsp(&qwMsg.connInfo, code));
  QW_SCH_TASK_DLOG("cancel rsp send, handle:%p, code:%x - %s", qwMsg.connInfo.handle, code, tstrerror(code));

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessDropMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  STaskDropReq *msg = pMsg->pCont;
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    QW_ELOG("invalid task drop msg, msg:%p, msgLen:%d", msg, pMsg->contLen);
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->refId = be64toh(msg->refId);

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;
  int64_t  rId = msg->refId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .code = pMsg->code};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  if (TSDB_CODE_RPC_NETWORK_UNAVAIL == pMsg->code) {
    QW_SCH_TASK_DLOG("receive drop task due to network broken, error:%s", tstrerror(pMsg->code));    
  }

  QW_SCH_TASK_DLOG("processDrop start, node:%p, handle:%p", node, pMsg->handle);

  QW_ERR_RET(qwProcessDrop(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processDrop end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessHbMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SSchedulerHbReq req = {0};
  SQWorkerMgmt *mgmt = (SQWorkerMgmt *)qWorkerMgmt;
  
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
  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0};
  qwMsg.connInfo.handle = pMsg->handle;
  qwMsg.connInfo.ahandle = pMsg->ahandle;

  QW_SCH_DLOG("processHb start, node:%p, handle:%p", node, pMsg->handle);

  QW_ERR_RET(qwProcessHb(mgmt, &qwMsg, &req));

  QW_SCH_DLOG("processHb end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessShowMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SVShowTablesReq *pReq = pMsg->pCont;
  QW_RET(qwBuildAndSendShowRsp(pMsg, code));
}

int32_t qWorkerProcessShowFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SVShowTablesFetchReq *pFetchReq = pMsg->pCont;
  QW_RET(qwBuildAndSendShowFetchRsp(pMsg, pFetchReq));
}
