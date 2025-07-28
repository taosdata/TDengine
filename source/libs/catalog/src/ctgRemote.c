/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "ctgRemote.h"
#include "catalogInt.h"
#include "query.h"
#include "systable.h"
#include "tname.h"
#include "tref.h"
#include "trpc.h"

typedef void* (*MallocType)(int64_t);

int32_t ctgHandleBatchRsp(SCtgJob* pJob, SCtgTaskCallbackParam* cbParam, SDataBuf* pMsg, int32_t rspCode) {
  int32_t       code = 0;
  SCatalog*     pCtg = pJob->pCtg;
  int32_t       taskNum = taosArrayGetSize(cbParam->taskId);
  SDataBuf      taskMsg = *pMsg;
  int32_t       msgNum = 0;
  SBatchRsp     batchRsp = {0};
  SBatchRspMsg  rsp = {0};
  SBatchRspMsg* pRsp = NULL;

  if (TSDB_CODE_SUCCESS == rspCode && pMsg->pData && (pMsg->len > 0)) {
    code = tDeserializeSBatchRsp(pMsg->pData, pMsg->len, &batchRsp);
    if (code < 0) {
      ctgError("tDeserializeSBatchRsp failed, msgLen:%d", pMsg->len);
      CTG_ERR_RET(code);
    }

    msgNum = taosArrayGetSize(batchRsp.pRsps);
  }

  if (taskNum != msgNum && 0 != msgNum) {
    ctgError("taskNum %d mis-match msgNum %d", taskNum, msgNum);
    msgNum = 0;
  }

  ctgDebug("QID:0x%" PRIx64 ", catalog got batch:%d rsp:%s", pJob->queryId, cbParam->batchId,
           TMSG_INFO(cbParam->reqType + 1));

  SHashObj* pBatchs = taosHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pBatchs) {
    ctgError("taosHashInit %d batch failed", taskNum);
    CTG_ERR_JRET(terrno);
  }

  for (int32_t i = 0; i < taskNum; ++i) {
    int32_t* taskId = taosArrayGet(cbParam->taskId, i);
    if (NULL == taskId) {
      ctgError("taosArrayGet %d taskId failed, total:%d", i, (int32_t)taosArrayGetSize(cbParam->taskId));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    int32_t* msgIdx = taosArrayGet(cbParam->msgIdx, i);
    if (NULL == msgIdx) {
      ctgError("taosArrayGet %d msgIdx failed, total:%d", i, (int32_t)taosArrayGetSize(cbParam->msgIdx));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SCtgTask* pTask = taosArrayGet(pJob->pTasks, *taskId);
    if (NULL == pTask) {
      ctgError("taosArrayGet %d SCtgTask failed, total:%d", *taskId, (int32_t)taosArrayGetSize(pJob->pTasks));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    if (msgNum > 0) {
      pRsp = taosArrayGet(batchRsp.pRsps, i);

      if (pRsp->msgIdx != *msgIdx) {
        ctgError("rsp msgIdx %d mis-match msgIdx %d", pRsp->msgIdx, *msgIdx);

        pRsp = &rsp;
        pRsp->msgIdx = *msgIdx;
        pRsp->reqType = -1;
        pRsp->rspCode = 0;
        taskMsg.msgType = -1;
        taskMsg.pData = NULL;
        taskMsg.len = 0;
      } else {
        taskMsg.msgType = pRsp->reqType;
        taskMsg.pData = pRsp->msg;
        taskMsg.len = pRsp->msgLen;
      }
    } else {
      pRsp = &rsp;
      pRsp->msgIdx = *msgIdx;
      pRsp->reqType = -1;
      pRsp->rspCode = 0;
      taskMsg.msgType = -1;
      taskMsg.pData = NULL;
      taskMsg.len = 0;
    }

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = pRsp->msgIdx;
    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq.msgIdx);
    if (NULL == pMsgCtx) {
      ctgError("task:%d, get SCtgMsgCtx failed, taskType:%d", tReq.msgIdx, pTask->type);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    pMsgCtx->pBatchs = pBatchs;

    ctgDebug("QID:0x%" PRIx64 ", catalog task:%d handle rsp:%s, idx:%d pBatchs:%p", pJob->queryId, pTask->taskId,
             TMSG_INFO(taskMsg.msgType + 1), pRsp->msgIdx, pBatchs);

    (void)(*gCtgAsyncFps[pTask->type].handleRspFp)(
        &tReq, pRsp->reqType, &taskMsg, (pRsp->rspCode ? pRsp->rspCode : rspCode));  // error handled internal
  }

  CTG_ERR_JRET(ctgLaunchBatchs(pJob->pCtg, pJob, pBatchs));

_return:

  taosArrayDestroyEx(batchRsp.pRsps, tFreeSBatchRspMsg);

  ctgFreeBatchs(pBatchs);
  CTG_RET(code);
}

int32_t ctgProcessRspMsg(void* out, int32_t reqType, char* msg, int32_t msgSize, int32_t rspCode, char* target) {
  int32_t code = 0;

  switch (reqType) {
    case TDMT_MND_QNODE_LIST: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for qnode list, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("process qnode list rsp failed, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(code);
      }

      qDebug("got qnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(out));
      break;
    }
    case TDMT_MND_DNODE_LIST: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for dnode list, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("process dnode list rsp failed, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(code);
      }

      qDebug("got dnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(*(SArray**)out));
      break;
    }
    case TDMT_MND_USE_DB: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("db:%s, error rsp for use db, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("db:%s, process use db rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("db:%s, got db vgInfo from mnode", target);
      break;
    }
    case TDMT_MND_GET_DB_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("db:%s, error rsp for get db cfg, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("db:%s, process get db cfg rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("db:%s, got db cfg from mnode", target);
      break;
    }
    case TDMT_MND_GET_INDEX: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("index:%s, error rsp for get index, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("index:%s, process get index rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("index:%s, got index from mnode", target);
      break;
    }
    case TDMT_MND_GET_TABLE_INDEX: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("tb:%s, error rsp for get table index, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process get table index rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got table index from mnode", target);
      break;
    }
    case TDMT_MND_RETRIEVE_FUNC: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("func:%s, error rsp for get udf, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("func:%s, Process get udf rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("func:%s, got udf from mnode", target);
      break;
    }
    case TDMT_MND_GET_USER_AUTH: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("user:%s, error rsp for get user auth, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("user:%s, process get user auth rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("user:%s, got user auth from mnode", target);
      break;
    }
    case TDMT_MND_TABLE_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("tb:%s, stablemeta not exist in mnode", target);
          return TSDB_CODE_SUCCESS;
        }

        qError("tb:%s, error rsp for stablemeta from mnode, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process mnode stablemeta rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got table meta from mnode", target);
      break;
    }
    case TDMT_VND_TABLE_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("tb:%s, tablemeta not exist in vnode", target);
          return TSDB_CODE_SUCCESS;
        }

        qError("tb:%s, error rsp for table meta from vnode, code:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process vnode tablemeta rsp failed, code:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got table meta from vnode", target);
      break;
    }
    case TDMT_VND_TABLE_NAME: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("tb:%s, tablemeta not exist in vnode", target);
          return TSDB_CODE_SUCCESS;
        }

        qError("tb:%s, error rsp for table meta from vnode, code:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process vnode tablemeta rsp failed, code:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got table meta from vnode", target);
      break;
    }
    case TDMT_VND_TABLE_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("tb:%s, error rsp for table cfg from vnode, code:%s,", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process vnode tb cfg rsp failed, code:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got table cfg from vnode", target);
      break;
    }
    case TDMT_MND_TABLE_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("tb:%s, error rsp for stb cfg from mnode, error:%s", target, tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, Process mnode stb cfg rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got stb cfg from mnode", target);
      break;
    }
    case TDMT_MND_SERVER_VERSION: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for svr ver from mnode, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("process svr ver rsp failed, error:%s", tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("got svr ver from mnode");
      break;
    }
    case TDMT_MND_VIEW_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (TSDB_CODE_MND_VIEW_NOT_EXIST == rspCode) {
          qDebug("no success rsp for get view-meta, error:%s, viewFName:%s", tstrerror(rspCode), target);
        } else {
          qError("error rsp for get view-meta, error:%s, viewFName:%s", tstrerror(rspCode), target);
        }
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("view:%s, process get view-meta rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("view:%s, got view-meta from mnode", target);
      break;
    }
    case TDMT_MND_GET_TSMA:
    case TDMT_MND_GET_TABLE_TSMA: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (TSDB_CODE_MND_SMA_NOT_EXIST != rspCode) {
          qError("tb:%s, error rsp for get table tsma, error:%s", target, tstrerror(rspCode));
        }
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process get table tsma rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("tb:%s, got table tsma from mnode", target);
      break;
    }
    case TDMT_MND_GET_STREAM_PROGRESS: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        CTG_ERR_RET(rspCode);
      }
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("tb:%s, process get stream progress rsp failed, error:%s", target, tstrerror(code));
        CTG_ERR_RET(code);
      }
      break;
    }
    case TDMT_VND_VSTB_REF_DBS: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        CTG_ERR_RET(rspCode);
      }
      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get vnode virtual subtable ref dbs rsp failed, err: %s, tbFName: %s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }
      break;
    }
    default:
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("get error rsp, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      qError("invalid req type %s", TMSG_INFO(reqType));
      return TSDB_CODE_APP_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ctgHandleMsgCallback(void* param, SDataBuf* pMsg, int32_t rspCode) {
  SCtgTaskCallbackParam* cbParam = (SCtgTaskCallbackParam*)param;
  int32_t                code = 0;
  SCtgJob*               pJob = NULL;

  CTG_API_JENTER();

  pJob = taosAcquireRef(gCtgMgmt.jobPool, cbParam->refId);
  if (NULL == pJob) {
    qDebug("catalog job refId 0x%" PRIx64 " already dropped", cbParam->refId);
    goto _return;
  }

  SCatalog* pCtg = pJob->pCtg;

  if (TDMT_VND_BATCH_META == cbParam->reqType || TDMT_MND_BATCH_META == cbParam->reqType || TDMT_SND_BATCH_META == cbParam->reqType) {
    CTG_ERR_JRET(ctgHandleBatchRsp(pJob, cbParam, pMsg, rspCode));
  } else {
    int32_t* taskId = taosArrayGet(cbParam->taskId, 0);
    if (NULL == taskId) {
      ctgError("taosArrayGet %d taskId failed, total:%d", 0, (int32_t)taosArrayGetSize(cbParam->taskId));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    SCtgTask* pTask = taosArrayGet(pJob->pTasks, *taskId);
    if (NULL == pTask) {
      ctgError("taosArrayGet %d SCtgTask failed, total:%d", *taskId, (int32_t)taosArrayGetSize(pJob->pTasks));
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    qDebug("QID:0x%" PRIx64 ", catalog task:%d handle rsp:%s", pJob->queryId, pTask->taskId,
           TMSG_INFO(cbParam->reqType + 1));

#if CTG_BATCH_FETCH
    SHashObj* pBatchs =
        taosHashInit(CTG_DEFAULT_BATCH_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    if (NULL == pBatchs) {
      ctgError("taosHashInit %d batch failed", CTG_DEFAULT_BATCH_NUM);
      CTG_ERR_JRET(terrno);
    }

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
    if (NULL == pMsgCtx) {
      ctgError("task:%d, get SCtgMsgCtx failed, taskType:%d", -1, pTask->type);
      CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
    }

    pMsgCtx->pBatchs = pBatchs;
#endif

    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;

    CTG_ERR_JRET((*gCtgAsyncFps[pTask->type].handleRspFp)(&tReq, cbParam->reqType, pMsg, rspCode));

#if CTG_BATCH_FETCH
    CTG_ERR_JRET(ctgLaunchBatchs(pJob->pCtg, pJob, pBatchs));
#endif
  }

_return:

  taosMemoryFree(pMsg->pData);
  taosMemoryFree(pMsg->pEpSet);

  if (pJob) {
    int32_t code2 = taosReleaseRef(gCtgMgmt.jobPool, cbParam->refId);
    if (code2) {
      qError("release catalog job refId:%" PRId64 " failed, error:%s", cbParam->refId, tstrerror(code2));
    }
  }

  CTG_API_LEAVE(code);
}

int32_t ctgMakeMsgSendInfo(SCtgJob* pJob, SArray* pTaskId, int32_t batchId, SArray* pMsgIdx, int32_t msgType,
                           SMsgSendInfo** pMsgSendInfo) {
  int32_t       code = 0;
  SMsgSendInfo* msgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == msgSendInfo) {
    qError("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    CTG_ERR_JRET(terrno);
  }

  SCtgTaskCallbackParam* param = taosMemoryCalloc(1, sizeof(SCtgTaskCallbackParam));
  if (NULL == param) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgTaskCallbackParam));
    taosMemoryFree(msgSendInfo);
    CTG_ERR_JRET(terrno);
  }

  param->reqType = msgType;
  param->queryId = pJob->queryId;
  param->refId = pJob->refId;
  param->taskId = pTaskId;
  param->batchId = batchId;
  param->msgIdx = pMsgIdx;

  msgSendInfo->param = param;
  msgSendInfo->paramFreeFp = ctgFreeMsgSendParam;
  msgSendInfo->fp = ctgHandleMsgCallback;

  *pMsgSendInfo = msgSendInfo;

  return TSDB_CODE_SUCCESS;

_return:

  taosArrayDestroy(pTaskId);
  destroySendMsgInfo(msgSendInfo);

  CTG_RET(code);
}

int32_t ctgAsyncSendMsg(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgJob* pJob, SArray* pTaskId, int32_t batchId,
                        SArray* pMsgIdx, char* dbFName, int32_t vgId, int32_t msgType, void** msg, uint32_t msgSize) {
  int32_t       code = 0;
  SMsgSendInfo* pMsgSendInfo = NULL;
  CTG_ERR_JRET(ctgMakeMsgSendInfo(pJob, pTaskId, batchId, pMsgIdx, msgType, &pMsgSendInfo));

  CTG_ERR_JRET(ctgUpdateSendTargetInfo(pMsgSendInfo, msgType, dbFName, vgId));

  pMsgSendInfo->requestId = pConn->requestId;
  pMsgSendInfo->requestObjRefId = pConn->requestObjRefId;
  pMsgSendInfo->msgInfo.pData = *msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgInfo.handle = NULL;
  pMsgSendInfo->msgType = msgType;
  *msg = NULL;

  code = asyncSendMsgToServer(pConn->pTrans, &pConn->mgmtEps, NULL, pMsgSendInfo);
  pMsgSendInfo = NULL;
  if (code) {
    ctgError("asyncSendMsgToSever failed, error: %s", tstrerror(code));
    CTG_ERR_JRET(code);
  }

  ctgDebug("QID:0x%" PRIx64 ", catalog req msg sent, type:%s", pJob->queryId, TMSG_INFO(msgType));
  return TSDB_CODE_SUCCESS;

_return:

  if (pMsgSendInfo) {
    // msg will be freed outside.
    destroySendMsgInfo(pMsgSendInfo);
  }

  CTG_RET(code);
}

int32_t ctgAddBatch(SCatalog* pCtg, int32_t vgId, SRequestConnInfo* pConn, SCtgTaskReq* tReq, int32_t msgType,
                    void* msg, uint32_t msgSize) {
  int32_t     code = 0;
  SCtgTask*   pTask = tReq->pTask;
  SCtgJob*    pJob = pTask->pJob;
  SCtgBatch   newBatch = {0};
  SBatchMsg   req = {0};
  bool        toSnode = false;
  SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  if (NULL == pMsgCtx) {
    ctgError("task:%d, get SCtgMsgCtx failed, taskType:%d", tReq->msgIdx, pTask->type);
    CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
  }

  SHashObj*  pBatchs = pMsgCtx->pBatchs;
  SCtgBatch* pBatch = taosHashGet(pBatchs, &vgId, sizeof(vgId));
  if (NULL == pBatch) {
    newBatch.pMsgs = taosArrayInit(pJob->subTaskNum, sizeof(SBatchMsg));
    newBatch.pTaskIds = taosArrayInit(pJob->subTaskNum, sizeof(int32_t));
    newBatch.pMsgIdxs = taosArrayInit(pJob->subTaskNum, sizeof(int32_t));
    if (NULL == newBatch.pMsgs || NULL == newBatch.pTaskIds || NULL == newBatch.pMsgIdxs) {
      taosArrayDestroy(newBatch.pMsgs);
      taosArrayDestroy(newBatch.pTaskIds);
      taosArrayDestroy(newBatch.pMsgIdxs);
      CTG_ERR_JRET(terrno);
    }

    newBatch.conn = *pConn;

    req.msgIdx = tReq->msgIdx;
    req.msgType = msgType;
    req.msgLen = msgSize;
    req.msg = msg;
    if (NULL == taosArrayPush(newBatch.pMsgs, &req)) {
      CTG_ERR_JRET(terrno);
    }
    msg = NULL;
    if (NULL == taosArrayPush(newBatch.pTaskIds, &pTask->taskId)) {
      CTG_ERR_JRET(terrno);
    }
    if (NULL == taosArrayPush(newBatch.pMsgIdxs, &req.msgIdx)) {
      CTG_ERR_JRET(terrno);
    }

    if (vgId > 0) {
      SName* pName = NULL;
      if (TDMT_VND_TABLE_CFG == msgType) {
        SCtgTbCfgCtx* ctx = (SCtgTbCfgCtx*)pTask->taskCtx;
        pName = ctx->pName;
      } else if (TDMT_VND_TABLE_META == msgType || TDMT_VND_TABLE_NAME == msgType ||
                 TDMT_VND_VSTB_REF_DBS == msgType) {
        if (CTG_TASK_GET_TB_META_BATCH == pTask->type) {
          SCtgTbMetasCtx* ctx = (SCtgTbMetasCtx*)pTask->taskCtx;
          SCtgFetch*      fetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
          CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, fetch, &pName));
        } else if (CTG_TASK_GET_TB_TSMA == pTask->type) {
          SCtgTbTSMACtx* pCtx = pTask->taskCtx;
          SCtgTSMAFetch* pFetch = taosArrayGet(pCtx->pFetches, tReq->msgIdx);
          STablesReq*    pTbReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
          pName = taosArrayGet(pTbReq->pTables, pFetch->tbIdx);
          if (NULL == pName) {
            ctgError("fail to get %d SName, totalTables:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pTbReq->pTables));
            CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
          }
        } else if (CTG_TASK_GET_TB_NAME == pTask->type) {
          SCtgTbNamesCtx* ctx = (SCtgTbNamesCtx*)pTask->taskCtx;
          SCtgFetch*      fetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
          CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, fetch, &pName));
        } else {
          SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
          pName = ctx->pName;
        }
      } else if (TDMT_MND_GET_STREAM_PROGRESS == msgType) {
        SCtgTbTSMACtx* pCtx = pTask->taskCtx;
        SCtgTSMAFetch* pFetch = taosArrayGet(pCtx->pFetches, tReq->msgIdx);
        if (NULL == pFetch) {
          ctgError("fail to get %d SCtgTSMAFetch, totalFetchs:%d", tReq->msgIdx,
                   (int32_t)taosArrayGetSize(pCtx->pFetches));
          CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
        }
        STablesReq* pTbReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
        if (NULL == pTbReq) {
          ctgError("fail to get %d STablesReq, totalTables:%d", pFetch->dbIdx, (int32_t)taosArrayGetSize(pCtx->pNames));
          CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
        }
        pName = taosArrayGet(pTbReq->pTables, pFetch->tbIdx);
        if (NULL == pName) {
          ctgError("fail to get %d SName, totalTables:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pTbReq->pTables));
          CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
        }
        toSnode = true;
      } else {
        ctgError("invalid vnode msgType %d", msgType);
        CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      (void)tNameGetFullDbName(pName, newBatch.dbFName);
    }

    newBatch.msgType = (vgId > 0) ? (toSnode ? TDMT_SND_BATCH_META : TDMT_VND_BATCH_META) : TDMT_MND_BATCH_META;
    newBatch.batchId = atomic_add_fetch_32(&pJob->batchId, 1);

    if (0 != taosHashPut(pBatchs, &vgId, sizeof(vgId), &newBatch, sizeof(newBatch))) {
      CTG_ERR_JRET(terrno);
    }

    qDebug("QID:0x%" PRIx64 ", job:0x%" PRIx64 ", catalog task:%d, %s req added to batch:%d, target vgId:%d",
           pTask->pJob->queryId, pTask->pJob->refId, pTask->taskId, TMSG_INFO(msgType), newBatch.batchId, vgId);

    return TSDB_CODE_SUCCESS;
  }

  req.msgIdx = tReq->msgIdx;
  req.msgType = msgType;
  req.msgLen = msgSize;
  req.msg = msg;
  if (NULL == taosArrayPush(pBatch->pMsgs, &req)) {
    CTG_ERR_JRET(terrno);
  }
  msg = NULL;
  if (NULL == taosArrayPush(pBatch->pTaskIds, &pTask->taskId)) {
    CTG_ERR_JRET(terrno);
  }
  if (NULL == taosArrayPush(pBatch->pMsgIdxs, &req.msgIdx)) {
    CTG_ERR_JRET(terrno);
  }

  if (vgId > 0) {
    SName* pName = NULL;
    if (TDMT_VND_TABLE_CFG == msgType) {
      SCtgTbCfgCtx* ctx = (SCtgTbCfgCtx*)pTask->taskCtx;
      pName = ctx->pName;
    } else if (TDMT_VND_TABLE_META == msgType || TDMT_VND_TABLE_NAME == msgType ||
               TDMT_VND_VSTB_REF_DBS == msgType) {
      if (CTG_TASK_GET_TB_META_BATCH == pTask->type) {
        SCtgTbMetasCtx* ctx = (SCtgTbMetasCtx*)pTask->taskCtx;
        SCtgFetch*      fetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
        CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, fetch, &pName));
      } else if (CTG_TASK_GET_TB_TSMA == pTask->type) {
        SCtgTbTSMACtx* pCtx = pTask->taskCtx;
        SCtgTSMAFetch* pFetch = taosArrayGet(pCtx->pFetches, tReq->msgIdx);
        STablesReq*    pTbReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
        pName = taosArrayGet(pTbReq->pTables, pFetch->tbIdx);
        if (NULL == pName) {
          ctgError("fail to get %d SName, totalTables:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pTbReq->pTables));
          CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
        }
      } else if (CTG_TASK_GET_TB_NAME == pTask->type) {
        SCtgTbMetasCtx* ctx = (SCtgTbMetasCtx*)pTask->taskCtx;
        SCtgFetch*      fetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
        CTG_ERR_JRET(ctgGetFetchName(ctx->pNames, fetch, &pName));
      } else {
        SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
        pName = ctx->pName;
      }
    } else if (TDMT_MND_GET_STREAM_PROGRESS == msgType) {
      SCtgTbTSMACtx* pCtx = pTask->taskCtx;
      SCtgTSMAFetch* pFetch = taosArrayGet(pCtx->pFetches, tReq->msgIdx);
      if (NULL == pFetch) {
        ctgError("fail to get %d SCtgTSMAFetch, totalFetchs:%d", tReq->msgIdx,
                 (int32_t)taosArrayGetSize(pCtx->pFetches));
        CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }
      STablesReq* pTbReq = taosArrayGet(pCtx->pNames, pFetch->dbIdx);
      if (NULL == pTbReq) {
        ctgError("fail to get %d STablesReq, totalTables:%d", pFetch->dbIdx, (int32_t)taosArrayGetSize(pCtx->pNames));
        CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }
      pName = taosArrayGet(pTbReq->pTables, pFetch->tbIdx);
      if (NULL == pName) {
        ctgError("fail to get %d SName, totalTables:%d", pFetch->tbIdx, (int32_t)taosArrayGetSize(pTbReq->pTables));
        CTG_ERR_JRET(TSDB_CODE_CTG_INTERNAL_ERROR);
      }
    } else {
      ctgError("invalid vnode msgType %d", msgType);
      CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
    }

    (void)tNameGetFullDbName(pName, pBatch->dbFName);
  }

  qDebug("QID:0x%" PRIx64 ", job:0x%" PRIx64 ", catalog task:%d, %s req added to batch:%d, target vgId:%d",
         pTask->pJob->queryId, pTask->pJob->refId, pTask->taskId, TMSG_INFO(msgType), pBatch->batchId, vgId);

  return TSDB_CODE_SUCCESS;

_return:

  ctgFreeBatch(&newBatch);
  taosMemoryFree(msg);

  return code;
}

int32_t ctgBuildBatchReqMsg(SCtgBatch* pBatch, int32_t vgId, void** msg, int32_t* pSize) {
  int32_t num = taosArrayGetSize(pBatch->pMsgs);
  if (num >= CTG_MAX_REQ_IN_BATCH) {
    qError("too many msgs %d in one batch request", num);
    CTG_ERR_RET(TSDB_CODE_CTG_INVALID_INPUT);
  }

  SBatchReq batchReq = {0};

  batchReq.header.vgId = vgId;
  batchReq.pMsgs = pBatch->pMsgs;

  int32_t msgSize = tSerializeSBatchReq(NULL, 0, &batchReq);
  if (msgSize < 0) {
    qError("tSerializeSBatchReq failed");
    CTG_ERR_RET(msgSize);
  }

  *msg = taosMemoryCalloc(1, msgSize);
  if (NULL == (*msg)) {
    qError("calloc batchReq msg failed, size:%d", msgSize);
    CTG_ERR_RET(terrno);
  }
  msgSize = tSerializeSBatchReq(*msg, msgSize, &batchReq);
  if (msgSize < 0) {
    taosMemoryFree(*msg);
    qError("tSerializeSBatchReq failed");
    CTG_ERR_RET(msgSize);
  }

  *pSize = msgSize;

  qTrace("batch:%d, batch req to vgId:%d msg built with %d meta reqs", pBatch->batchId, vgId, num);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgLaunchBatchs(SCatalog* pCtg, SCtgJob* pJob, SHashObj* pBatchs) {
  int32_t code = 0;
  void*   msg = NULL;
  void*   p = taosHashIterate(pBatchs, NULL);
  while (NULL != p) {
    size_t     len = 0;
    int32_t*   vgId = taosHashGetKey(p, &len);
    SCtgBatch* pBatch = (SCtgBatch*)p;
    int32_t    msgSize = 0;

    qDebug("QID:0x%" PRIx64 ", job:0x%" PRIx64 ", catalog start to launch batch:%d", pJob->queryId, pJob->refId,
           pBatch->batchId);

    CTG_ERR_JRET(ctgBuildBatchReqMsg(pBatch, *vgId, &msg, &msgSize));
    code = ctgAsyncSendMsg(pCtg, &pBatch->conn, pJob, pBatch->pTaskIds, pBatch->batchId, pBatch->pMsgIdxs,
                           pBatch->dbFName, *vgId, pBatch->msgType, &msg, msgSize);
    pBatch->pTaskIds = NULL;
    CTG_ERR_JRET(code);

    p = taosHashIterate(pBatchs, p);
  }

  return TSDB_CODE_SUCCESS;

_return:

  if (p) {
    taosHashCancelIterate(pBatchs, p);
  }
  if (msg) {
    taosMemoryFree(msg);
  }

  CTG_RET(code);
}

int32_t ctgGetQnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SArray* out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_QNODE_LIST;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get qnode list from mnode, mgmtEpInUse:%d", pConn->mgmtEps.inUse);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build qnode list msg failed, error:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosArrayInit(4, sizeof(SQueryNodeLoad));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, pOut, NULL));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SArray** out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_DNODE_LIST;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get dnode list from mnode, mgmtEpInUse:%d", pConn->mgmtEps.inUse);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build dnode list msg failed, error:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, NULL, NULL));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDBVgInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SBuildUseDBInput* input, SUseDbOutput* out,
                                SCtgTaskReq* tReq) {
  char*     msg = NULL;
  int32_t   msgLen = 0;
  int32_t   reqType = TDMT_MND_USE_DB;
  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("db:%s, try to get db vgInfo from mnode", input->db);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](input, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("db:%s, build use db msg failed, code:%s", input->db, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SUseDbOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, input->db));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, input->db));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDBCfgFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* out,
                             SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_DB_CFG;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("db:%s, try to get db cfg from mnode", dbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)dbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("db:%s, build get db cfg msg failed, code:%s", dbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, pOut, (char*)dbFName));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_GET_DB_CFG,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)dbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetIndexInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* indexName, SIndexInfo* out,
                                 SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_INDEX;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("index:%s, try to get index from mnode", indexName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)indexName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("index:%s, build get index msg failed, code:%s", indexName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SIndexInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, pOut, (char*)indexName));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)indexName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbIndexFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SName* name, STableIndex* out,
                               SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_TABLE_INDEX;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;
  char tbFName[TSDB_TABLE_FNAME_LEN];

  ctgDebug("tb:%s, try to get tb index from mnode", tbFName);

  int32_t code = tNameExtractFullName(name, tbFName);
  if (code) {
    ctgError("tb:%s, tNameExtractFullName failed, code:%s, type:%d, dbName:%s", name->tname, tstrerror(code),
             name->type, name->dbname);
    CTG_ERR_RET(code);
  }

  code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)tbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build get index msg failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableIndex));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, pOut, (char*)tbFName));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetUdfInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* funcName, SFuncInfo* out,
                               SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_RETRIEVE_FUNC;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("func:%s, try to get udf info from mnode", funcName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)funcName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("func:%s, build get udf msg failed, code:%s", funcName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SFuncInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, pOut, (char*)funcName));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)funcName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetUserDbAuthFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* user, SGetUserAuthRsp* out,
                                  SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_USER_AUTH;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("user:%s, try to get user auth from mnode", user);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)user, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("user:%s, build get user auth msg failed, code:%s", user, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SGetUserAuthRsp));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, pOut, (char*)user));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)user));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbMetaFromMnodeImpl(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, const char* tbName,
                                  STableMetaOutput* out, SCtgTaskReq* tReq) {
  SCtgTask*        pTask = tReq ? tReq->pTask : NULL;
  SBuildTableInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = tbName};
  char*            msg = NULL;
  SEpSet*          pVnodeEpSet = NULL;
  int32_t          msgLen = 0;
  int32_t          reqType = TDMT_MND_TABLE_META;
  char             tbFName[TSDB_TABLE_FNAME_LEN];
  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", dbFName, tbName);
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  ctgDebug("tb:%s, try to get table meta from mnode", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build mnode stablemeta msg failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableMetaOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, tbFName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbMetaFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableMetaOutput* out,
                              SCtgTaskReq* tReq) {
  char dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pTableName, dbFName);

  return ctgGetTbMetaFromMnodeImpl(pCtg, pConn, dbFName, (char*)pTableName->tname, out, tReq);
}

int32_t ctgGetTbMetaFromVnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SVgroupInfo* vgroupInfo,
                              STableMetaOutput* out, SCtgTaskReq* tReq) {
  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  uint8_t   autoCreateCtb = tReq ? tReq->autoCreateCtb : 0;
  char      dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pTableName, dbFName);
  int32_t reqType = (pTask && pTask->type == CTG_TASK_GET_TB_NAME ? TDMT_VND_TABLE_NAME : TDMT_VND_TABLE_META);
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  (void)snprintf(tbFName, sizeof(tbFName), "%s.%s", dbFName, pTableName->tname);
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("tb:%s, try to get table meta from vnode, vgId:%d, ep num:%d, ep:%s:%u", tbFName, vgroupInfo->vgId,
           vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port);

  SBuildTableInput bInput = {.vgId = vgroupInfo->vgId,
                             .option = reqType == TDMT_VND_TABLE_NAME ? REQ_OPT_TBUID : REQ_OPT_TBNAME,
                             .autoCreateCtb = autoCreateCtb,
                             .dbFName = dbFName,
                             .tbName = (char*)tNameGetTableName(pTableName)};
  char*            msg = NULL;
  int32_t          msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build vnode tablemeta msg failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableMetaOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    SRequestConnInfo vConn = {.pTrans = pConn->pTrans,
                              .requestId = pConn->requestId,
                              .requestObjRefId = pConn->requestObjRefId,
                              .mgmtEps = vgroupInfo->epSet};

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, tbFName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, vgroupInfo->vgId, &vConn, tReq, reqType, msg, msgLen));
#else
    SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
    char           dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(ctx->pName, dbFName);
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, &vConn, pTask->pJob, pTaskId, -1, NULL, dbFName, ctx->vgId, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableCfgFromVnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                                SVgroupInfo* vgroupInfo, STableCfg** out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_VND_TABLE_CFG;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;
  char dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pTableName, dbFName);
  SBuildTableInput bInput = {.vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char*)pTableName->tname};

  int32_t code = tNameExtractFullName(pTableName, tbFName);
  if (code) {
    ctgError("tb:%s, tNameExtractFullName failed, code:%s, type:%d, dbName:%s", pTableName->tname, tstrerror(code),
             pTableName->type, pTableName->dbname);
    CTG_ERR_RET(code);
  }

  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("tb:%s, try to get table cfg from vnode, vgId:%d, ep num:%d, ep %s:%d", tbFName, vgroupInfo->vgId,
           vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port);

  code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build get tb cfg msg failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, NULL, (char*)tbFName));

    SRequestConnInfo vConn = {.pTrans = pConn->pTrans,
                              .requestId = pConn->requestId,
                              .requestObjRefId = pConn->requestObjRefId,
                              .mgmtEps = vgroupInfo->epSet};
#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, vgroupInfo->vgId, &vConn, &tReq, reqType, msg, msgLen));
#else
    SCtgTbCfgCtx* ctx = (SCtgTbCfgCtx*)pTask->taskCtx;
    char          dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(ctx->pName, dbFName);
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, &vConn, pTask->pJob, pTaskId, -1, NULL, dbFName, ctx->pVgInfo->vgId, reqType, msg,
                            msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTableCfgFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableCfg** out,
                                SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_TABLE_CFG;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;
  char dbFName[TSDB_DB_FNAME_LEN];
  (void)tNameGetFullDbName(pTableName, dbFName);
  SBuildTableInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = (char*)pTableName->tname};

  int32_t code = tNameExtractFullName(pTableName, tbFName);
  if (code) {
    ctgError("tb:%s, tNameExtractFullName failed, code:%s, type:%d, dbName:%s", pTableName->tname, tstrerror(code),
             pTableName->type, pTableName->dbname);
    CTG_ERR_RET(code);
  }

  ctgDebug("tb:%s, try to get table cfg from mnode", tbFName);

  code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build get tb cfg msg failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, NULL, (char*)tbFName));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetSvrVerFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, char** out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_SERVER_VERSION;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  qDebug("try to get svr ver from mnode");

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("build get svr ver msg failed, code:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, -1), reqType, NULL, NULL));

#if CTG_BATCH_FETCH
    SCtgTaskReq tReq;
    tReq.pTask = pTask;
    tReq.msgIdx = -1;
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, &tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetViewInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pName, SViewMetaOutput* out,
                                SCtgTaskReq* tReq) {
  char*     msg = NULL;
  int32_t   msgLen = 0;
  int32_t   reqType = TDMT_MND_VIEW_META;
  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;
  char    fullName[TSDB_TABLE_FNAME_LEN];
  int32_t code = tNameExtractFullName(pName, fullName);
  if (code) {
    ctgError("view:%s, tNameExtractFullName failed, code:%s, type:%d, dbName:%s", pName->tname, tstrerror(code), pName->type,
             pName->dbname);
    CTG_ERR_RET(code);
  }

  ctgDebug("view:%s, try to get view info from mnode", fullName);

  code = queryBuildMsg[TMSG_INDEX(reqType)](fullName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("view:%s, build view-meta msg failed, code:%s", fullName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, POINTER_BYTES);
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, fullName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, fullName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbTSMAFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* name, STableTSMAInfoRsp* out,
                              SCtgTaskReq* tReq, int32_t reqType) {
  char*     msg = NULL;
  int32_t   msgLen = 0;
  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t code = tNameExtractFullName(name, tbFName);
  if (code) {
    ctgError("tb:%s, tNameExtractFullName failed, code:%s, type:%d, dbName:%s", name->tname, tstrerror(code),
             name->type, name->dbname);
    CTG_ERR_RET(code);
  }

  ctgDebug("tb:%s, try to get tb index from mnode", tbFName);

  code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)tbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build get index msg failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableTSMAInfoRsp));
    if (NULL == pOut) {
      CTG_ERR_RET(terrno);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, (char*)tbFName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetStreamProgressFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTbName,
                                      SStreamProgressRsp* out, SCtgTaskReq* tReq, void* bInput, int32_t nodeId) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_STREAM_PROGRESS;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  int32_t code = tNameExtractFullName(pTbName, tbFName);
  if (code) {
    ctgError("tb:%s, tNameExtractFullName failed, code:%s, type:%d, dbName:%s", pTbName->tname, tstrerror(code),
             pTbName->type, pTbName->dbname);
    CTG_ERR_RET(code);
  }

  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;

  code = queryBuildMsg[TMSG_INDEX(reqType)](bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("tb:%s, build get stream progress failed, code:%s", tbFName, tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    SStreamProgressRsp* pOut = taosMemoryCalloc(1, sizeof(SStreamProgressRsp));
    if (!pOut) {
      CTG_ERR_RET(terrno);
    }
    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, (char*)tbFName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, nodeId, pConn, tReq, reqType, msg, msgLen));
#else
    char dbFName[TSDB_DB_FNAME_LEN];
    (void)tNameGetFullDbName(pTbName, dbFName);
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(terrno);
    }
    if (NULL == taosArrayPush(pTaskId, &pTask->taskId)) {
      taosArrayDestroy(pTaskId);
      CTG_ERR_RET(terrno);
    }

    CTG_RET(
        ctgAsyncSendMsg(pCtg, &vConn, pTask->pJob, pTaskId, -1, NULL, dbFName, vgroupInfo->vgId, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  CTG_ERR_RET(rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp));

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetVStbRefDbsFromVnode(SCatalog* pCtg, SRequestConnInfo* pConn, int64_t suid, SVgroupInfo* vgroupInfo, SCtgTaskReq* tReq) {
  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemMalloc : (MallocType)rpcMallocCont;
  int32_t reqType = TDMT_VND_VSTB_REF_DBS;
  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("try to get vstb's ref dbs from vnode, vgId:%d, ep num:%d, ep %s:%d, suid:%" PRIu64, vgroupInfo->vgId,
           vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port, suid);

  char*            msg = NULL;
  int32_t          msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&suid, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build vnode vsubtables meta msg failed, code:%x, suid:%" PRIu64, code, suid);
    CTG_ERR_RET(code);
  }

  SRequestConnInfo vConn = {.pTrans = pConn->pTrans,
                            .requestId = pConn->requestId,
                            .requestObjRefId = pConn->requestObjRefId,
                            .mgmtEps = vgroupInfo->epSet};

  return ctgAddBatch(pCtg, vgroupInfo->vgId, &vConn, tReq, reqType, msg, msgLen);
}


