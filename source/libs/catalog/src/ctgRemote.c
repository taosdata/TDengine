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
    if (tDeserializeSBatchRsp(pMsg->pData, pMsg->len, &batchRsp) < 0) {
      ctgError("tDeserializeSBatchRsp failed, msgLen:%d", pMsg->len);
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    msgNum = taosArrayGetSize(batchRsp.pRsps);
  }

  if (ASSERTS(taskNum == msgNum || 0 == msgNum, "taskNum %d mis-match msgNum %d", taskNum, msgNum)) {
    msgNum = 0;
  }

  ctgDebug("QID:0x%" PRIx64 " ctg got batch %d rsp %s", pJob->queryId, cbParam->batchId,
           TMSG_INFO(cbParam->reqType + 1));

  SHashObj* pBatchs = taosHashInit(taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
  if (NULL == pBatchs) {
    ctgError("taosHashInit %d batch failed", taskNum);
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < taskNum; ++i) {
    int32_t*  taskId = taosArrayGet(cbParam->taskId, i);
    int32_t*  msgIdx = taosArrayGet(cbParam->msgIdx, i);
    SCtgTask* pTask = taosArrayGet(pJob->pTasks, *taskId);
    if (msgNum > 0) {
      pRsp = taosArrayGet(batchRsp.pRsps, i);

      if (ASSERTS(pRsp->msgIdx == *msgIdx, "rsp msgIdx %d mis-match msgIdx %d", pRsp->msgIdx, *msgIdx)) {
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
    pMsgCtx->pBatchs = pBatchs;

    ctgDebug("QID:0x%" PRIx64 " ctg task %d idx %d start to handle rsp %s, pBatchs: %p", pJob->queryId, pTask->taskId,
             pRsp->msgIdx, TMSG_INFO(taskMsg.msgType + 1), pBatchs);

    (*gCtgAsyncFps[pTask->type].handleRspFp)(&tReq, pRsp->reqType, &taskMsg, (pRsp->rspCode ? pRsp->rspCode : rspCode));
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
        qError("Process qnode list rsp failed, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(code);
      }

      qDebug("Got qnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(out));
      break;
    }
    case TDMT_MND_DNODE_LIST: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for dnode list, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process dnode list rsp failed, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(code);
      }

      qDebug("Got dnode list from mnode, listNum:%d", (int32_t)taosArrayGetSize(*(SArray**)out));
      break;
    }
    case TDMT_MND_USE_DB: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for use db, error:%s, dbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process use db rsp failed, error:%s, dbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got db vgInfo from mnode, dbFName:%s", target);
      break;
    }
    case TDMT_MND_GET_DB_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get db cfg, error:%s, db:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get db cfg rsp failed, error:%s, db:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got db cfg from mnode, dbFName:%s", target);
      break;
    }
    case TDMT_MND_GET_INDEX: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get index, error:%s, indexName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get index rsp failed, error:%s, indexName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got index from mnode, indexName:%s", target);
      break;
    }
    case TDMT_MND_GET_TABLE_INDEX: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get table index, error:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get table index rsp failed, error:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got table index from mnode, tbFName:%s", target);
      break;
    }
    case TDMT_MND_RETRIEVE_FUNC: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get udf, error:%s, funcName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get udf rsp failed, error:%s, funcName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got udf from mnode, funcName:%s", target);
      break;
    }
    case TDMT_MND_GET_USER_AUTH: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for get user auth, error:%s, user:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process get user auth rsp failed, error:%s, user:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got user auth from mnode, user:%s", target);
      break;
    }
    case TDMT_MND_TABLE_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("stablemeta not exist in mnode, tbFName:%s", target);
          return TSDB_CODE_SUCCESS;
        }

        qError("error rsp for stablemeta from mnode, error:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process mnode stablemeta rsp failed, error:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got table meta from mnode, tbFName:%s", target);
      break;
    }
    case TDMT_VND_TABLE_META: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        if (CTG_TABLE_NOT_EXIST(rspCode)) {
          SET_META_TYPE_NULL(((STableMetaOutput*)out)->metaType);
          qDebug("tablemeta not exist in vnode, tbFName:%s", target);
          return TSDB_CODE_SUCCESS;
        }

        qError("error rsp for table meta from vnode, code:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process vnode tablemeta rsp failed, code:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got table meta from vnode, tbFName:%s", target);
      break;
    }
    case TDMT_VND_TABLE_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for table cfg from vnode, code:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process vnode tb cfg rsp failed, code:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got table cfg from vnode, tbFName:%s", target);
      break;
    }
    case TDMT_MND_TABLE_CFG: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for stb cfg from mnode, error:%s, tbFName:%s", tstrerror(rspCode), target);
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process mnode stb cfg rsp failed, error:%s, tbFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got stb cfg from mnode, tbFName:%s", target);
      break;
    }
    case TDMT_MND_SERVER_VERSION: {
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("error rsp for svr ver from mnode, error:%s", tstrerror(rspCode));
        CTG_ERR_RET(rspCode);
      }

      code = queryProcessMsgRsp[TMSG_INDEX(reqType)](out, msg, msgSize);
      if (code) {
        qError("Process svr ver rsp failed, error:%s", tstrerror(code));
        CTG_ERR_RET(code);
      }

      qDebug("Got svr ver from mnode");
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
        qError("Process get view-meta rsp failed, error:%s, viewFName:%s", tstrerror(code), target);
        CTG_ERR_RET(code);
      }

      qDebug("Got view-meta from mnode, viewFName:%s", target);
      break;
    }
    default:
      if (TSDB_CODE_SUCCESS != rspCode) {
        qError("Got error rsp, error:%s", tstrerror(rspCode));
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
    qDebug("ctg job refId 0x%" PRIx64 " already dropped", cbParam->refId);
    goto _return;
  }

  SCatalog* pCtg = pJob->pCtg;

  if (TDMT_VND_BATCH_META == cbParam->reqType || TDMT_MND_BATCH_META == cbParam->reqType) {
    CTG_ERR_JRET(ctgHandleBatchRsp(pJob, cbParam, pMsg, rspCode));
  } else {
    int32_t*  taskId = taosArrayGet(cbParam->taskId, 0);
    SCtgTask* pTask = taosArrayGet(pJob->pTasks, *taskId);

    qDebug("QID:0x%" PRIx64 " ctg task %d start to handle rsp %s", pJob->queryId, pTask->taskId,
           TMSG_INFO(cbParam->reqType + 1));

#if CTG_BATCH_FETCH
    SHashObj* pBatchs =
        taosHashInit(CTG_DEFAULT_BATCH_NUM, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_NO_LOCK);
    if (NULL == pBatchs) {
      ctgError("taosHashInit %d batch failed", CTG_DEFAULT_BATCH_NUM);
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, -1);
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
    taosReleaseRef(gCtgMgmt.jobPool, cbParam->refId);
  }

  CTG_API_LEAVE(code);
}

int32_t ctgMakeMsgSendInfo(SCtgJob* pJob, SArray* pTaskId, int32_t batchId, SArray* pMsgIdx, int32_t msgType,
                           SMsgSendInfo** pMsgSendInfo) {
  int32_t       code = 0;
  SMsgSendInfo* msgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == msgSendInfo) {
    qError("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCtgTaskCallbackParam* param = taosMemoryCalloc(1, sizeof(SCtgTaskCallbackParam));
  if (NULL == param) {
    qError("calloc %d failed", (int32_t)sizeof(SCtgTaskCallbackParam));
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
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
                        SArray* pMsgIdx, char* dbFName, int32_t vgId, int32_t msgType, void* msg, uint32_t msgSize) {
  int32_t       code = 0;
  SMsgSendInfo* pMsgSendInfo = NULL;
  CTG_ERR_JRET(ctgMakeMsgSendInfo(pJob, pTaskId, batchId, pMsgIdx, msgType, &pMsgSendInfo));

  ctgUpdateSendTargetInfo(pMsgSendInfo, msgType, dbFName, vgId);

  pMsgSendInfo->requestId = pConn->requestId;
  pMsgSendInfo->requestObjRefId = pConn->requestObjRefId;
  pMsgSendInfo->msgInfo.pData = msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgInfo.handle = NULL;
  pMsgSendInfo->msgType = msgType;

  int64_t transporterId = 0;
  code = asyncSendMsgToServer(pConn->pTrans, &pConn->mgmtEps, &transporterId, pMsgSendInfo);
  pMsgSendInfo = NULL;
  if (code) {
    ctgError("asyncSendMsgToSever failed, error: %s", tstrerror(code));
    CTG_ERR_JRET(code);
  }

  ctgDebug("ctg req msg sent, reqId:0x%" PRIx64 ", msg type:%d, %s", pJob->queryId, msgType, TMSG_INFO(msgType));
  return TSDB_CODE_SUCCESS;

_return:

  if (pMsgSendInfo) {
    destroySendMsgInfo(pMsgSendInfo);
  }

  CTG_RET(code);
}

int32_t ctgAddBatch(SCatalog* pCtg, int32_t vgId, SRequestConnInfo* pConn, SCtgTaskReq* tReq, int32_t msgType,
                    void* msg, uint32_t msgSize) {
  int32_t     code = 0;
  SCtgTask*   pTask = tReq->pTask;
  SCtgMsgCtx* pMsgCtx = CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx);
  SHashObj*   pBatchs = pMsgCtx->pBatchs;
  SCtgJob*    pJob = pTask->pJob;
  SCtgBatch*  pBatch = taosHashGet(pBatchs, &vgId, sizeof(vgId));
  SCtgBatch   newBatch = {0};
  SBatchMsg   req = {0};

  if (NULL == pBatch) {
    newBatch.pMsgs = taosArrayInit(pJob->subTaskNum, sizeof(SBatchMsg));
    newBatch.pTaskIds = taosArrayInit(pJob->subTaskNum, sizeof(int32_t));
    newBatch.pMsgIdxs = taosArrayInit(pJob->subTaskNum, sizeof(int32_t));
    if (NULL == newBatch.pMsgs || NULL == newBatch.pTaskIds || NULL == newBatch.pMsgIdxs) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    newBatch.conn = *pConn;

    req.msgIdx = tReq->msgIdx;
    req.msgType = msgType;
    req.msgLen = msgSize;
    req.msg = msg;
    if (NULL == taosArrayPush(newBatch.pMsgs, &req)) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    msg = NULL;
    if (NULL == taosArrayPush(newBatch.pTaskIds, &pTask->taskId)) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }
    if (NULL == taosArrayPush(newBatch.pMsgIdxs, &req.msgIdx)) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    if (vgId > 0) {
      SName* pName = NULL;
      if (TDMT_VND_TABLE_CFG == msgType) {
        SCtgTbCfgCtx* ctx = (SCtgTbCfgCtx*)pTask->taskCtx;
        pName = ctx->pName;
      } else if (TDMT_VND_TABLE_META == msgType) {
        if (CTG_TASK_GET_TB_META_BATCH == pTask->type) {
          SCtgTbMetasCtx* ctx = (SCtgTbMetasCtx*)pTask->taskCtx;
          SCtgFetch*      fetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
          pName = ctgGetFetchName(ctx->pNames, fetch);
        } else {
          SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
          pName = ctx->pName;
        }
      } else {
        ctgError("invalid vnode msgType %d", msgType);
        CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
      }

      tNameGetFullDbName(pName, newBatch.dbFName);
    }

    newBatch.msgType = (vgId > 0) ? TDMT_VND_BATCH_META : TDMT_MND_BATCH_META;
    newBatch.batchId = atomic_add_fetch_32(&pJob->batchId, 1);

    if (0 != taosHashPut(pBatchs, &vgId, sizeof(vgId), &newBatch, sizeof(newBatch))) {
      CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    ctgDebug("task %d %s req added to batch %d, target vgId %d", pTask->taskId, TMSG_INFO(msgType), newBatch.batchId,
             vgId);

    return TSDB_CODE_SUCCESS;
  }

  req.msgIdx = tReq->msgIdx;
  req.msgType = msgType;
  req.msgLen = msgSize;
  req.msg = msg;
  if (NULL == taosArrayPush(pBatch->pMsgs, &req)) {
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }
  msg = NULL;
  if (NULL == taosArrayPush(pBatch->pTaskIds, &pTask->taskId)) {
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }
  if (NULL == taosArrayPush(pBatch->pMsgIdxs, &req.msgIdx)) {
    CTG_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (vgId > 0) {
    SName* pName = NULL;
    if (TDMT_VND_TABLE_CFG == msgType) {
      SCtgTbCfgCtx* ctx = (SCtgTbCfgCtx*)pTask->taskCtx;
      pName = ctx->pName;
    } else if (TDMT_VND_TABLE_META == msgType) {
      if (CTG_TASK_GET_TB_META_BATCH == pTask->type) {
        SCtgTbMetasCtx* ctx = (SCtgTbMetasCtx*)pTask->taskCtx;
        SCtgFetch*      fetch = taosArrayGet(ctx->pFetchs, tReq->msgIdx);
        pName = ctgGetFetchName(ctx->pNames, fetch);
      } else {
        SCtgTbMetaCtx* ctx = (SCtgTbMetaCtx*)pTask->taskCtx;
        pName = ctx->pName;
      }
    } else {
      ctgError("invalid vnode msgType %d", msgType);
      CTG_ERR_JRET(TSDB_CODE_APP_ERROR);
    }

    tNameGetFullDbName(pName, pBatch->dbFName);
  }

  ctgDebug("task %d %s req added to batch %d, target vgId %d", pTask->taskId, TMSG_INFO(msgType), pBatch->batchId,
           vgId);

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
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  *msg = taosMemoryCalloc(1, msgSize);
  if (NULL == (*msg)) {
    qError("calloc batchReq msg failed, size:%d", msgSize);
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }
  if (tSerializeSBatchReq(*msg, msgSize, &batchReq) < 0) {
    qError("tSerializeSBatchReq failed");
    CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  *pSize = msgSize;

  qDebug("batch req %d to vg %d msg built with %d meta reqs", pBatch->batchId, vgId, num);

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

    ctgDebug("QID:0x%" PRIx64 " ctg start to launch batch %d", pJob->queryId, pBatch->batchId);

    CTG_ERR_JRET(ctgBuildBatchReqMsg(pBatch, *vgId, &msg, &msgSize));
    code = ctgAsyncSendMsg(pCtg, &pBatch->conn, pJob, pBatch->pTaskIds, pBatch->batchId, pBatch->pMsgIdxs,
                           pBatch->dbFName, *vgId, pBatch->msgType, msg, msgSize);
    pBatch->pTaskIds = NULL;
    CTG_ERR_JRET(code);

    p = taosHashIterate(pBatchs, p);
  }

  return TSDB_CODE_SUCCESS;

_return:

  if (p) {
    taosHashCancelIterate(pBatchs, p);
  }
  taosMemoryFree(msg);

  CTG_RET(code);
}

int32_t ctgGetQnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SArray* out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_QNODE_LIST;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get qnode list from mnode, mgmtEpInUse:%d", pConn->mgmtEps.inUse);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build qnode list msg failed, error:%s", tstrerror(code));
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosArrayInit(4, sizeof(SQueryNodeLoad));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, NULL));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SArray** out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_DNODE_LIST;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

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
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get db vgInfo from mnode, dbFName:%s", input->db);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](input, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build use db msg failed, code:%x, db:%s", code, input->db);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SUseDbOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, input->db));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, input->db));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetDBCfgFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* out,
                             SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_DB_CFG;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get db cfg from mnode, dbFName:%s", dbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)dbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get db cfg msg failed, code:%x, db:%s", code, dbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SDbCfgInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = TDMT_MND_GET_DB_CFG,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)dbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetIndexInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* indexName, SIndexInfo* out,
                                 SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_INDEX;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get index from mnode, indexName:%s", indexName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)indexName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get index msg failed, code:%x, db:%s", code, indexName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SIndexInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)indexName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbIndexFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SName* name, STableIndex* out,
                               SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_TABLE_INDEX;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;
  char tbFName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(name, tbFName);

  ctgDebug("try to get tb index from mnode, tbFName:%s", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)tbFName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get index msg failed, code:%s, tbFName:%s", tstrerror(code), tbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableIndex));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetUdfInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* funcName, SFuncInfo* out,
                               SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_RETRIEVE_FUNC;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get udf info from mnode, funcName:%s", funcName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)funcName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get udf msg failed, code:%x, db:%s", code, funcName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SFuncInfo));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)funcName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetUserDbAuthFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* user, SGetUserAuthRsp* out,
                                  SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_GET_USER_AUTH;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get user auth from mnode, user:%s", user);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)]((void*)user, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get user auth msg failed, code:%x, db:%s", code, user);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(SGetUserAuthRsp));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)user));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbMetaFromMnodeImpl(SCatalog* pCtg, SRequestConnInfo* pConn, char* dbFName, char* tbName,
                                  STableMetaOutput* out, SCtgTaskReq* tReq) {
  SCtgTask*        pTask = tReq ? tReq->pTask : NULL;
  SBuildTableInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = tbName};
  char*            msg = NULL;
  SEpSet*          pVnodeEpSet = NULL;
  int32_t          msgLen = 0;
  int32_t          reqType = TDMT_MND_TABLE_META;
  char             tbFName[TSDB_TABLE_FNAME_LEN];
  sprintf(tbFName, "%s.%s", dbFName, tbName);
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  ctgDebug("try to get table meta from mnode, tbFName:%s", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build mnode stablemeta msg failed, code:%x", code);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableMetaOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, tbFName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetTbMetaFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableMetaOutput* out,
                              SCtgTaskReq* tReq) {
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);

  return ctgGetTbMetaFromMnodeImpl(pCtg, pConn, dbFName, (char*)pTableName->tname, out, tReq);
}

int32_t ctgGetTbMetaFromVnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SVgroupInfo* vgroupInfo,
                              STableMetaOutput* out, SCtgTaskReq* tReq) {
  SCtgTask* pTask = tReq ? tReq->pTask : NULL;
  char      dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);
  int32_t reqType = TDMT_VND_TABLE_META;
  char    tbFName[TSDB_TABLE_FNAME_LEN];
  sprintf(tbFName, "%s.%s", dbFName, pTableName->tname);
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("try to get table meta from vnode, vgId:%d, ep num:%d, ep %s:%d, tbFName:%s", vgroupInfo->vgId,
           vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port, tbFName);

  SBuildTableInput bInput = {
      .vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char*)tNameGetTableName(pTableName)};
  char*   msg = NULL;
  int32_t msgLen = 0;

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build vnode tablemeta msg failed, code:%x, tbFName:%s", code, tbFName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, sizeof(STableMetaOutput));
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
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
    tNameGetFullDbName(ctx->pName, dbFName);
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, &vConn, pTask->pJob, pTaskId, -1, NULL, dbFName, ctx->vgId, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp);

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
  tNameExtractFullName(pTableName, tbFName);
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);
  SBuildTableInput bInput = {.vgId = vgroupInfo->vgId, .dbFName = dbFName, .tbName = (char*)pTableName->tname};

  SEp* pEp = &vgroupInfo->epSet.eps[vgroupInfo->epSet.inUse];
  ctgDebug("try to get table cfg from vnode, vgId:%d, ep num:%d, ep %s:%d, tbFName:%s", vgroupInfo->vgId,
           vgroupInfo->epSet.numOfEps, pEp->fqdn, pEp->port, tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get tb cfg msg failed, code:%s, tbFName:%s", tstrerror(code), tbFName);
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
    tNameGetFullDbName(ctx->pName, dbFName);
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

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
  rpcSendRecv(pConn->pTrans, &vgroupInfo->epSet, &rpcMsg, &rpcRsp);

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
  tNameExtractFullName(pTableName, tbFName);
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;
  char dbFName[TSDB_DB_FNAME_LEN];
  tNameGetFullDbName(pTableName, dbFName);
  SBuildTableInput bInput = {.vgId = 0, .dbFName = dbFName, .tbName = (char*)pTableName->tname};

  ctgDebug("try to get table cfg from mnode, tbFName:%s", tbFName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](&bInput, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get tb cfg msg failed, code:%s, tbFName:%s", tstrerror(code), tbFName);
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, (char*)tbFName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}

int32_t ctgGetSvrVerFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, char** out, SCtgTask* pTask) {
  char*   msg = NULL;
  int32_t msgLen = 0;
  int32_t reqType = TDMT_MND_SERVER_VERSION;
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;

  qDebug("try to get svr ver from mnode");

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](NULL, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build get svr ver msg failed, code:%s", tstrerror(code));
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
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

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
  void* (*mallocFp)(int64_t) = pTask ? (MallocType)taosMemoryMalloc : (MallocType)rpcMallocCont;
  char fullName[TSDB_TABLE_FNAME_LEN];
  tNameExtractFullName(pName, fullName);

  ctgDebug("try to get view info from mnode, viewFName:%s", fullName);

  int32_t code = queryBuildMsg[TMSG_INDEX(reqType)](fullName, &msg, 0, &msgLen, mallocFp);
  if (code) {
    ctgError("Build view-meta msg failed, code:%x, viewFName:%s", code, fullName);
    CTG_ERR_RET(code);
  }

  if (pTask) {
    void* pOut = taosMemoryCalloc(1, POINTER_BYTES);
    if (NULL == pOut) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }

    CTG_ERR_RET(ctgUpdateMsgCtx(CTG_GET_TASK_MSGCTX(pTask, tReq->msgIdx), reqType, pOut, fullName));

#if CTG_BATCH_FETCH
    CTG_RET(ctgAddBatch(pCtg, 0, pConn, tReq, reqType, msg, msgLen));
#else
    SArray* pTaskId = taosArrayInit(1, sizeof(int32_t));
    if (NULL == pTaskId) {
      CTG_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
    }
    taosArrayPush(pTaskId, &pTask->taskId);

    CTG_RET(ctgAsyncSendMsg(pCtg, pConn, pTask->pJob, pTaskId, -1, NULL, NULL, 0, reqType, msg, msgLen));
#endif
  }

  SRpcMsg rpcMsg = {
      .msgType = reqType,
      .pCont = msg,
      .contLen = msgLen,
  };

  SRpcMsg rpcRsp = {0};
  rpcSendRecv(pConn->pTrans, &pConn->mgmtEps, &rpcMsg, &rpcRsp);

  CTG_ERR_RET(ctgProcessRspMsg(out, reqType, rpcRsp.pCont, rpcRsp.contLen, rpcRsp.code, fullName));

  rpcFreeCont(rpcRsp.pCont);

  return TSDB_CODE_SUCCESS;
}


