#include "qworker.h"
#include <common.h>
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

int32_t qwBuildAndSendQueryRsp(void *connection, int32_t code) {
  SRpcMsg *pMsg = (SRpcMsg *)connection;
  SQueryTableRsp *pRsp = (SQueryTableRsp *)rpcMallocCont(sizeof(SQueryTableRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_QUERY_RSP,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendReadyRsp(void *connection, int32_t code) {
  SRpcMsg *pMsg = (SRpcMsg *)connection;
  SResReadyRsp *pRsp = (SResReadyRsp *)rpcMallocCont(sizeof(SResReadyRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_RES_READY_RSP,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendStatusRsp(SRpcMsg *pMsg, SSchedulerStatusRsp *sStatus) {
  int32_t size = 0;
  
  if (sStatus) {
    size = sizeof(SSchedulerStatusRsp) + sizeof(sStatus->status[0]) * sStatus->num;
  } else {
    size = sizeof(SSchedulerStatusRsp);
  }
  
  SSchedulerStatusRsp *pRsp = (SSchedulerStatusRsp *)rpcMallocCont(size);

  if (sStatus) {
    memcpy(pRsp, sStatus, size);
  } else {
    pRsp->num = 0;
  }

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_TASKS_STATUS_RSP,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = size,
    .code    = 0,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendFetchRsp(void *connection, SRetrieveTableRsp *pRsp, int32_t dataLength, int32_t code) {
  SRpcMsg *pMsg = (SRpcMsg *)connection;
  
  if (NULL == pRsp) {
    pRsp = (SRetrieveTableRsp *)rpcMallocCont(sizeof(SRetrieveTableRsp));
    memset(pRsp, 0, sizeof(SRetrieveTableRsp));
    dataLength = 0;
  }

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_FETCH_RSP,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp) + dataLength,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);

  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCancelRsp(SRpcMsg *pMsg, int32_t code) {
  STaskCancelRsp *pRsp = (STaskCancelRsp *)rpcMallocCont(sizeof(STaskCancelRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_CANCEL_TASK_RSP,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendDropRsp(void *connection, int32_t code) {
  SRpcMsg *pMsg = (SRpcMsg *)connection;
  STaskDropRsp *pRsp = (STaskDropRsp *)rpcMallocCont(sizeof(STaskDropRsp));
  pRsp->code = code;

  SRpcMsg rpcRsp = {
    .msgType = TDMT_VND_DROP_TASK_RSP,
    .handle  = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .pCont   = pRsp,
    .contLen = sizeof(*pRsp),
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendShowRsp(SRpcMsg *pMsg, int32_t code) {
  int32_t numOfCols = 6;
  int32_t msgSize = sizeof(SVShowTablesRsp) + sizeof(SSchema) * numOfCols;

  SVShowTablesRsp *pRsp = (SVShowTablesRsp *)rpcMallocCont(msgSize);

  int32_t  cols = 0;
  SSchema *pSchema = pRsp->metaInfo.pSchema;

  const SSchema *s = tGetTbnameColumnSchema();
  *pSchema = createSchema(s->type, htonl(s->bytes), htonl(++cols), "name");
  pSchema++;

  int32_t type = TSDB_DATA_TYPE_TIMESTAMP;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "created");
  pSchema++;

  type = TSDB_DATA_TYPE_SMALLINT;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "columns");
  pSchema++;

  *pSchema = createSchema(s->type, htonl(s->bytes), htonl(++cols), "stable");
  pSchema++;

  type = TSDB_DATA_TYPE_BIGINT;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "uid");
  pSchema++;

  type = TSDB_DATA_TYPE_INT;
  *pSchema = createSchema(type, htonl(tDataTypes[type].bytes), htonl(++cols), "vgId");

  assert(cols == numOfCols);
  pRsp->metaInfo.numOfColumns = htonl(cols);

  SRpcMsg rpcMsg = {
      .handle  = pMsg->handle,
      .ahandle = pMsg->ahandle,
      .pCont   = pRsp,
      .contLen = msgSize,
      .code    = code,
  };

  rpcSendResponse(&rpcMsg);
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

  rpcSendResponse(&rpcMsg);
  return TSDB_CODE_SUCCESS;
}

int32_t qwBuildAndSendCQueryMsg(SQWorkerMgmt *mgmt, uint64_t sId, uint64_t qId, uint64_t tId, void *connection) {
  SRpcMsg *pMsg = (SRpcMsg *)connection;
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
    .handle = pMsg->handle,
    .ahandle = pMsg->ahandle,
    .msgType = TDMT_VND_QUERY_CONTINUE,
    .pCont   = req,
    .contLen = sizeof(SQueryContinueReq),
    .code    = 0,
  };

  int32_t code = (*mgmt->putToQueueFp)(mgmt->nodeObj, &pNewMsg);
  if (TSDB_CODE_SUCCESS != code) {
    QW_SCH_TASK_ELOG("put query continue msg to queue failed, vgId:%d, code:%s", mgmt->nodeId, tstrerror(code));
    rpcFreeCont(req);
    QW_ERR_RET(code);
  }

  QW_SCH_TASK_DLOG("put task continue exec msg to query queue, vgId:%d", mgmt->nodeId);

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

  msg->sId = be64toh(msg->sId);
  msg->queryId = be64toh(msg->queryId);
  msg->taskId = be64toh(msg->taskId);
  msg->contentLen = ntohl(msg->contentLen);
  
  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;

  SQWMsg qwMsg = {.node = node, .msg = msg->msg, .msgLen = msg->contentLen, .connection = pMsg};

  QW_SCH_TASK_DLOG("processQuery start, node:%p", node);

  QW_RET(qwProcessQuery(QW_FPARAMS(), &qwMsg, msg->taskType));

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

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connection = pMsg};

  QW_SCH_TASK_DLOG("processCQuery start, node:%p", node);

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

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connection = pMsg};

  QW_SCH_TASK_DLOG("processReady start, node:%p", node);

  QW_ERR_RET(qwProcessReady(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId, &qwMsg));

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

  QW_ERR_RET(qwBuildAndSendStatusRsp(pMsg, sStatus));

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

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connection = pMsg};

  QW_SCH_TASK_DLOG("processFetch start, node:%p", node);

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

  int32_t code = 0;
  STaskCancelReq *msg = pMsg->pCont;
  if (NULL == msg || pMsg->contLen < sizeof(*msg)) {
    qError("invalid task cancel msg");  
    QW_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }  

  msg->sId = htobe64(msg->sId);
  msg->queryId = htobe64(msg->queryId);
  msg->taskId = htobe64(msg->taskId);

  //QW_ERR_JRET(qwCancelTask(qWorkerMgmt, msg->sId, msg->queryId, msg->taskId));

_return:

  QW_ERR_RET(qwBuildAndSendCancelRsp(pMsg, code));

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

  uint64_t sId = msg->sId;
  uint64_t qId = msg->queryId;
  uint64_t tId = msg->taskId;

  SQWMsg qwMsg = {.node = node, .msg = NULL, .msgLen = 0, .connection = pMsg};

  QW_SCH_TASK_DLOG("processDrop start, node:%p", node);

  QW_ERR_RET(qwProcessDrop(QW_FPARAMS(), &qwMsg));

  QW_SCH_TASK_DLOG("processDrop end, node:%p", node);

  return TSDB_CODE_SUCCESS;
}

int32_t qWorkerProcessShowMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t code = 0;
  SVShowTablesReq *pReq = pMsg->pCont;
  QW_ERR_RET(qwBuildAndSendShowRsp(pMsg, code));
}

int32_t qWorkerProcessShowFetchMsg(void *node, void *qWorkerMgmt, SRpcMsg *pMsg) {
  if (NULL == node || NULL == qWorkerMgmt || NULL == pMsg) {
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SVShowTablesFetchReq *pFetchReq = pMsg->pCont;
  QW_ERR_RET(qwBuildAndSendShowFetchRsp(pMsg, pFetchReq));
}


