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

#include "catalog.h"
#include "command.h"
#include "query.h"
#include "schedulerInt.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"


int32_t schValidateReceivedMsgType(SSchJob *pJob, SSchTask *pTask, int32_t msgType) {
  int32_t lastMsgType = SCH_GET_TASK_LASTMSG_TYPE(pTask);
  int32_t taskStatus = SCH_GET_TASK_STATUS(pTask);
  int32_t reqMsgType = msgType - 1;
  switch (msgType) {
    case TDMT_SCH_LINK_BROKEN:
    case TDMT_VND_EXPLAIN_RSP:
      return TSDB_CODE_SUCCESS;
    case TDMT_VND_QUERY_RSP:  // query_rsp may be processed later than ready_rsp
      if (lastMsgType != reqMsgType && -1 != lastMsgType) {
        SCH_TASK_DLOG("rsp msg type mis-match, last sent msgType:%s, rspType:%s", TMSG_INFO(lastMsgType),
                      TMSG_INFO(msgType));
      }

      if (taskStatus != JOB_TASK_STATUS_EXECUTING && taskStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
        SCH_TASK_DLOG("rsp msg conflicted with task status, status:%s, rspType:%s", jobTaskStatusStr(taskStatus),
                      TMSG_INFO(msgType));
      }

      SCH_SET_TASK_LASTMSG_TYPE(pTask, -1);
      return TSDB_CODE_SUCCESS;
    case TDMT_VND_FETCH_RSP:
      if (lastMsgType != reqMsgType && -1 != lastMsgType) {
        SCH_TASK_ELOG("rsp msg type mis-match, last sent msgType:%s, rspType:%s", TMSG_INFO(lastMsgType),
                      TMSG_INFO(msgType));
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      if (taskStatus != JOB_TASK_STATUS_EXECUTING && taskStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
        SCH_TASK_ELOG("rsp msg conflicted with task status, status:%s, rspType:%s", jobTaskStatusStr(taskStatus),
                      TMSG_INFO(msgType));
        SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      SCH_SET_TASK_LASTMSG_TYPE(pTask, -1);
      return TSDB_CODE_SUCCESS;
    case TDMT_VND_CREATE_TABLE_RSP:
    case TDMT_VND_DROP_TABLE_RSP:
    case TDMT_VND_ALTER_TABLE_RSP:
    case TDMT_VND_SUBMIT_RSP:
      break;
    default:
      SCH_TASK_ELOG("unknown rsp msg, type:%s, status:%s", TMSG_INFO(msgType), jobTaskStatusStr(taskStatus));
      SCH_ERR_RET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  if (lastMsgType != reqMsgType) {
    SCH_TASK_ELOG("rsp msg type mis-match, last sent msgType:%s, rspType:%s", TMSG_INFO(lastMsgType),
                  TMSG_INFO(msgType));
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  if (taskStatus != JOB_TASK_STATUS_EXECUTING && taskStatus != JOB_TASK_STATUS_PARTIAL_SUCCEED) {
    SCH_TASK_ELOG("rsp msg conflicted with task status, status:%s, rspType:%s", jobTaskStatusStr(taskStatus),
                  TMSG_INFO(msgType));
    SCH_ERR_RET(TSDB_CODE_SCH_STATUS_ERROR);
  }

  SCH_SET_TASK_LASTMSG_TYPE(pTask, -1);

  return TSDB_CODE_SUCCESS;
}

// Note: no more task error processing, handled in function internal
int32_t schHandleResponseMsg(SSchJob *pJob, SSchTask *pTask, int32_t msgType, char *msg, int32_t msgSize,
                             int32_t rspCode) {
  int32_t code = 0;
  int8_t  status = 0;

  if (schJobNeedToStop(pJob, &status)) {
    SCH_TASK_ELOG("rsp not processed cause of job status, job status:%s, rspCode:0x%x", jobTaskStatusStr(status),
                  rspCode);
    taosMemoryFreeClear(msg);              
    SCH_RET(atomic_load_32(&pJob->errCode));
  }

  SCH_ERR_JRET(schValidateReceivedMsgType(pJob, pTask, msgType));

  switch (msgType) {
    case TDMT_VND_CREATE_TABLE_RSP: {
      SVCreateTbBatchRsp batchRsp = {0};
      if (msg) {
        SDecoder coder = {0};
        tDecoderInit(&coder, msg, msgSize);
        code = tDecodeSVCreateTbBatchRsp(&coder, &batchRsp);
        if (TSDB_CODE_SUCCESS == code && batchRsp.nRsps > 0) {
          for (int32_t i = 0; i < batchRsp.nRsps; ++i) {
            SVCreateTbRsp *rsp = batchRsp.pRsps + i;
            if (TSDB_CODE_SUCCESS != rsp->code) {
              code = rsp->code;
              tDecoderClear(&coder);
              SCH_ERR_JRET(code);
            }
          }
        }
        tDecoderClear(&coder);
        SCH_ERR_JRET(code);
      }

      SCH_ERR_JRET(rspCode);
      taosMemoryFreeClear(msg);              
      
      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));
      break;
    }
    case TDMT_VND_DROP_TABLE_RSP: {
      SVDropTbBatchRsp batchRsp = {0};
      if (msg) {
        SDecoder coder = {0};
        tDecoderInit(&coder, msg, msgSize);
        code = tDecodeSVDropTbBatchRsp(&coder, &batchRsp);
        if (TSDB_CODE_SUCCESS == code && batchRsp.nRsps > 0) {
          for (int32_t i = 0; i < batchRsp.nRsps; ++i) {
            SVDropTbRsp *rsp = batchRsp.pRsps + i;
            if (TSDB_CODE_SUCCESS != rsp->code) {
              code = rsp->code;
              tDecoderClear(&coder);
              SCH_ERR_JRET(code);
            }
          }
        }
        tDecoderClear(&coder);
        SCH_ERR_JRET(code);
      }

      SCH_ERR_JRET(rspCode);
      taosMemoryFreeClear(msg);              
      
      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));
      break;
    }
    case TDMT_VND_ALTER_TABLE_RSP: {
      SVAlterTbRsp rsp = {0};
      if (msg) {
        SDecoder coder = {0};
        tDecoderInit(&coder, msg, msgSize);
        code = tDecodeSVAlterTbRsp(&coder, &rsp);
        tDecoderClear(&coder);
        SCH_ERR_JRET(code);
        SCH_ERR_JRET(rsp.code);
      }

      SCH_ERR_JRET(rspCode);

      if (NULL == msg) {
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      taosMemoryFreeClear(msg);              
      
      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));
      break;
    }
    case TDMT_VND_SUBMIT_RSP: {
      SCH_ERR_JRET(rspCode);

      if (msg) {
        SDecoder    coder = {0};
        SSubmitRsp *rsp = taosMemoryMalloc(sizeof(*rsp));
        tDecoderInit(&coder, msg, msgSize);
        code = tDecodeSSubmitRsp(&coder, rsp);
        if (code) {
          SCH_TASK_ELOG("decode submitRsp failed, code:%d", code);
          tFreeSSubmitRsp(rsp);
          SCH_ERR_JRET(code);
        }

        if (rsp->nBlocks > 0) {
          for (int32_t i = 0; i < rsp->nBlocks; ++i) {
            SSubmitBlkRsp *blk = rsp->pBlocks + i;
            if (TSDB_CODE_SUCCESS != blk->code) {
              code = blk->code;
              tFreeSSubmitRsp(rsp);
              SCH_ERR_JRET(code);
            }
          }
        }

        atomic_add_fetch_32(&pJob->resNumOfRows, rsp->affectedRows);
        SCH_TASK_DLOG("submit succeed, affectedRows:%d", rsp->affectedRows);

        SCH_LOCK(SCH_WRITE, &pJob->resLock);
        if (pJob->queryRes) {
          SSubmitRsp *sum = pJob->queryRes;
          sum->affectedRows += rsp->affectedRows;
          sum->nBlocks += rsp->nBlocks;
          sum->pBlocks = taosMemoryRealloc(sum->pBlocks, sum->nBlocks * sizeof(*sum->pBlocks));
          memcpy(sum->pBlocks + sum->nBlocks - rsp->nBlocks, rsp->pBlocks, rsp->nBlocks * sizeof(*sum->pBlocks));
          taosMemoryFree(rsp->pBlocks);
          taosMemoryFree(rsp);
        } else {
          pJob->queryRes = rsp;
        }
        SCH_UNLOCK(SCH_WRITE, &pJob->resLock);
      }

      taosMemoryFreeClear(msg);              

      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));

      break;
    }
    case TDMT_VND_QUERY_RSP: {
      SQueryTableRsp *rsp = (SQueryTableRsp *)msg;

      SCH_ERR_JRET(rspCode);
      if (NULL == msg) {
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }
      SCH_ERR_JRET(rsp->code);

      SCH_ERR_JRET(schSaveJobQueryRes(pJob, rsp));

      taosMemoryFreeClear(msg);              
      
      SCH_ERR_RET(schProcessOnTaskSuccess(pJob, pTask));

      break;
    }
    case TDMT_VND_EXPLAIN_RSP: {
      SCH_ERR_JRET(rspCode);
      if (NULL == msg) {
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      if (!SCH_IS_EXPLAIN_JOB(pJob)) {
        SCH_TASK_ELOG("invalid msg received for none explain query, msg type:%s", TMSG_INFO(msgType));
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      if (pJob->resData) {
        SCH_TASK_ELOG("explain result is already generated, res:%p", pJob->resData);
        SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      SExplainRsp rsp = {0};
      if (tDeserializeSExplainRsp(msg, msgSize, &rsp)) {
        taosMemoryFree(rsp.subplanInfo);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SRetrieveTableRsp *pRsp = NULL;
      SCH_ERR_JRET(qExplainUpdateExecInfo(pJob->explainCtx, &rsp, pTask->plan->id.groupId, &pRsp));

      if (pRsp) {
        SCH_ERR_JRET(schProcessOnExplainDone(pJob, pTask, pRsp));
      }
      break;
    }
    case TDMT_VND_FETCH_RSP: {
      SRetrieveTableRsp *rsp = (SRetrieveTableRsp *)msg;

      SCH_ERR_JRET(rspCode);
      if (NULL == msg) {
        SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
      }

      if (SCH_IS_EXPLAIN_JOB(pJob)) {
        if (rsp->completed) {
          SRetrieveTableRsp *pRsp = NULL;
          SCH_ERR_JRET(qExecExplainEnd(pJob->explainCtx, &pRsp));
          if (pRsp) {
            SCH_ERR_JRET(schProcessOnExplainDone(pJob, pTask, pRsp));
          }

          taosMemoryFreeClear(msg);              

          return TSDB_CODE_SUCCESS;
        }

        atomic_val_compare_exchange_32(&pJob->remoteFetch, 1, 0);

        SCH_ERR_JRET(schFetchFromRemote(pJob));

        taosMemoryFreeClear(msg);              

        return TSDB_CODE_SUCCESS;
      }

      if (pJob->resData) {
        SCH_TASK_ELOG("got fetch rsp while res already exists, res:%p", pJob->resData);
        taosMemoryFreeClear(rsp);
        SCH_ERR_JRET(TSDB_CODE_SCH_STATUS_ERROR);
      }

      atomic_store_ptr(&pJob->resData, rsp);
      atomic_add_fetch_32(&pJob->resNumOfRows, htonl(rsp->numOfRows));

      if (rsp->completed) {
        SCH_SET_TASK_STATUS(pTask, JOB_TASK_STATUS_SUCCEED);
      }

      SCH_TASK_DLOG("got fetch rsp, rows:%d, complete:%d", htonl(rsp->numOfRows), rsp->completed);

      msg = NULL;              

      schProcessOnDataFetched(pJob);
      break;
    }
    case TDMT_VND_DROP_TASK_RSP: {
      // SHOULD NEVER REACH HERE
      SCH_TASK_ELOG("invalid status to handle drop task rsp, refId:%" PRIx64, pJob->refId);
      SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
      break;
    }
    case TDMT_SCH_LINK_BROKEN:
      SCH_TASK_ELOG("link broken received, error:%x - %s", rspCode, tstrerror(rspCode));
      SCH_ERR_JRET(rspCode);
      break;
    default:
      SCH_TASK_ELOG("unknown rsp msg, type:%d, status:%s", msgType, SCH_GET_TASK_STATUS_STR(pTask));
      SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(msg);              

  SCH_RET(schProcessOnTaskFailure(pJob, pTask, code));
}


int32_t schHandleCallback(void *param, const SDataBuf *pMsg, int32_t msgType, int32_t rspCode) {
  int32_t                code = 0;
  SSchTaskCallbackParam *pParam = (SSchTaskCallbackParam *)param;
  SSchTask              *pTask = NULL;

  SSchJob *pJob = schAcquireJob(pParam->refId);
  if (NULL == pJob) {
    qWarn("QID:0x%" PRIx64 ",TID:0x%" PRIx64 "taosAcquireRef job failed, may be dropped, refId:%" PRIx64,
          pParam->queryId, pParam->taskId, pParam->refId);
    SCH_ERR_JRET(TSDB_CODE_QRY_JOB_FREED);
  }

  schGetTaskFromTaskList(pJob->execTasks, pParam->taskId, &pTask);
  if (NULL == pTask) {
    if (TDMT_VND_EXPLAIN_RSP == msgType) {
      schGetTaskFromTaskList(pJob->succTasks, pParam->taskId, &pTask);
    } else {
      SCH_JOB_ELOG("task not found in execTask list, refId:%" PRIx64 ", taskId:%" PRIx64, pParam->refId,
                   pParam->taskId);
      SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
    }
  }

  if (NULL == pTask) {
    SCH_JOB_ELOG("task not found in execList & succList, refId:%" PRIx64 ", taskId:%" PRIx64, pParam->refId,
                 pParam->taskId);
    SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SCH_TASK_DLOG("rsp msg received, type:%s, handle:%p, code:%s", TMSG_INFO(msgType), pMsg->handle, tstrerror(rspCode));

  SCH_SET_TASK_HANDLE(pTask, pMsg->handle);
  schUpdateTaskExecNodeHandle(pTask, pMsg->handle, rspCode);
  
  SCH_ERR_JRET(schHandleResponseMsg(pJob, pTask, msgType, pMsg->pData, pMsg->len, rspCode));

_return:
  
  if (pJob) {
    schReleaseJob(pParam->refId);
  }

  taosMemoryFreeClear(param);
  SCH_RET(code);
}

int32_t schHandleSubmitCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_SUBMIT_RSP, code);
}

int32_t schHandleCreateTbCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_CREATE_TABLE_RSP, code);
}

int32_t schHandleDropTbCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_DROP_TABLE_RSP, code);
}

int32_t schHandleAlterTbCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_ALTER_TABLE_RSP, code);
}

int32_t schHandleQueryCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_QUERY_RSP, code);
}

int32_t schHandleFetchCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_FETCH_RSP, code);
}

int32_t schHandleExplainCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  return schHandleCallback(param, pMsg, TDMT_VND_EXPLAIN_RSP, code);
}

int32_t schHandleDropCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  SSchTaskCallbackParam *pParam = (SSchTaskCallbackParam *)param;
  qDebug("QID:%" PRIx64 ",TID:%" PRIx64 " drop task rsp received, code:%x", pParam->queryId, pParam->taskId, code);
  taosMemoryFreeClear(param);
  return TSDB_CODE_SUCCESS;
}

int32_t schHandleLinkBrokenCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  SSchCallbackParamHeader *head = (SSchCallbackParamHeader *)param;
  rpcReleaseHandle(pMsg->handle, TAOS_CONN_CLIENT);

  qDebug("handle %p is broken", pMsg->handle);

  if (head->isHbParam) {
    SSchHbCallbackParam *hbParam = (SSchHbCallbackParam *)param;
    SSchTrans            trans = {.pTrans = hbParam->pTrans, .pHandle = NULL};
    SCH_ERR_RET(schUpdateHbConnection(&hbParam->nodeEpId, &trans));

    SCH_ERR_RET(schBuildAndSendHbMsg(&hbParam->nodeEpId));
  } else {
    SCH_ERR_RET(schHandleCallback(param, pMsg, TDMT_SCH_LINK_BROKEN, code));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schGenerateCallBackInfo(SSchJob *pJob, SSchTask *pTask, int32_t msgType, SMsgSendInfo **pMsgSendInfo) {
  int32_t       code = 0;
  SMsgSendInfo *msgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == msgSendInfo) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SSchTaskCallbackParam *param = taosMemoryCalloc(1, sizeof(SSchTaskCallbackParam));
  if (NULL == param) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SSchTaskCallbackParam));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  __async_send_cb_fn_t fp = NULL;
  SCH_ERR_JRET(schGetCallbackFp(msgType, &fp));

  param->queryId = pJob->queryId;
  param->refId = pJob->refId;
  param->taskId = SCH_TASK_ID(pTask);
  param->transport = pJob->pTrans;

  msgSendInfo->param = param;
  msgSendInfo->fp = fp;

  *pMsgSendInfo = msgSendInfo;

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFree(param);
  taosMemoryFree(msgSendInfo);

  SCH_RET(code);
}


int32_t schGetCallbackFp(int32_t msgType, __async_send_cb_fn_t *fp) {
  switch (msgType) {
    case TDMT_VND_CREATE_TABLE:
      *fp = schHandleCreateTbCallback;
      break;
    case TDMT_VND_DROP_TABLE:
      *fp = schHandleDropTbCallback;
      break;
    case TDMT_VND_ALTER_TABLE:
      *fp = schHandleAlterTbCallback;
      break;
    case TDMT_VND_SUBMIT:
      *fp = schHandleSubmitCallback;
      break;
    case TDMT_VND_QUERY:
      *fp = schHandleQueryCallback;
      break;
    case TDMT_VND_EXPLAIN:
      *fp = schHandleExplainCallback;
      break;
    case TDMT_VND_FETCH:
      *fp = schHandleFetchCallback;
      break;
    case TDMT_VND_DROP_TASK:
      *fp = schHandleDropCallback;
      break;
    case TDMT_VND_QUERY_HEARTBEAT:
      *fp = schHandleHbCallback;
      break;
    case TDMT_SCH_LINK_BROKEN:
      *fp = schHandleLinkBrokenCallback;
      break;
    default:
      qError("unknown msg type for callback, msgType:%d", msgType);
      SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schMakeHbCallbackParam(SSchJob *pJob, SSchTask *pTask, void **pParam) {
  SSchHbCallbackParam *param = taosMemoryCalloc(1, sizeof(SSchHbCallbackParam));
  if (NULL == param) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SSchHbCallbackParam));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  param->head.isHbParam = true;

  SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);

  param->nodeEpId.nodeId = addr->nodeId;
  SEp* pEp = SCH_GET_CUR_EP(addr);
  strcpy(param->nodeEpId.ep.fqdn, pEp->fqdn);
  param->nodeEpId.ep.port = pEp->port;
  param->pTrans = pJob->pTrans;

  *pParam = param;

  return TSDB_CODE_SUCCESS;
}

int32_t schCloneHbRpcCtx(SRpcCtx *pSrc, SRpcCtx *pDst) {
  int32_t code = 0;
  memcpy(pDst, pSrc, sizeof(SRpcCtx));
  pDst->brokenVal.val = NULL;
  pDst->args = NULL;

  SCH_ERR_RET(schCloneSMsgSendInfo(pSrc->brokenVal.val, &pDst->brokenVal.val));

  pDst->args = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (NULL == pDst->args) {
    qError("taosHashInit %d RpcCtx failed", 1);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SRpcCtxVal dst = {0};
  void      *pIter = taosHashIterate(pSrc->args, NULL);
  while (pIter) {
    SRpcCtxVal *pVal = (SRpcCtxVal *)pIter;
    int32_t    *msgType = taosHashGetKey(pIter, NULL);

    dst = *pVal;
    dst.val = NULL;

    SCH_ERR_JRET(schCloneSMsgSendInfo(pVal->val, &dst.val));

    if (taosHashPut(pDst->args, msgType, sizeof(*msgType), &dst, sizeof(dst))) {
      qError("taosHashPut msg %d to rpcCtx failed", *msgType);
      (*pSrc->freeFunc)(dst.val);
      SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    pIter = taosHashIterate(pSrc->args, pIter);
  }

  return TSDB_CODE_SUCCESS;

_return:

  schFreeRpcCtx(pDst);
  SCH_RET(code);
}


int32_t schMakeHbRpcCtx(SSchJob *pJob, SSchTask *pTask, SRpcCtx *pCtx) {
  int32_t              code = 0;
  SSchHbCallbackParam *param = NULL;
  SMsgSendInfo        *pMsgSendInfo = NULL;
  SQueryNodeAddr      *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
  SQueryNodeEpId       epId = {0};

  epId.nodeId = addr->nodeId;
  memcpy(&epId.ep, SCH_GET_CUR_EP(addr), sizeof(SEp));

  pCtx->args = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (NULL == pCtx->args) {
    SCH_TASK_ELOG("taosHashInit %d RpcCtx failed", 1);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  param = taosMemoryCalloc(1, sizeof(SSchHbCallbackParam));
  if (NULL == param) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SSchHbCallbackParam));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  int32_t              msgType = TDMT_VND_QUERY_HEARTBEAT_RSP;
  __async_send_cb_fn_t fp = NULL;
  SCH_ERR_JRET(schGetCallbackFp(TDMT_VND_QUERY_HEARTBEAT, &fp));

  param->nodeEpId = epId;
  param->pTrans = pJob->pTrans;

  pMsgSendInfo->param = param;
  pMsgSendInfo->fp = fp;

  SRpcCtxVal ctxVal = {.val = pMsgSendInfo, .clone = schCloneSMsgSendInfo};
  if (taosHashPut(pCtx->args, &msgType, sizeof(msgType), &ctxVal, sizeof(ctxVal))) {
    SCH_TASK_ELOG("taosHashPut msg %d to rpcCtx failed", msgType);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_ERR_JRET(schMakeBrokenLinkVal(pJob, pTask, &pCtx->brokenVal, true));
  pCtx->freeFunc = schFreeRpcCtxVal;

  return TSDB_CODE_SUCCESS;

_return:

  taosHashCleanup(pCtx->args);
  taosMemoryFreeClear(param);
  taosMemoryFreeClear(pMsgSendInfo);

  SCH_RET(code);
}

int32_t schRegisterHbConnection(SSchJob *pJob, SSchTask *pTask, SQueryNodeEpId *epId, bool *exist) {
  int32_t     code = 0;
  SSchHbTrans hb = {0};

  hb.trans.pTrans = pJob->pTrans;

  SCH_ERR_RET(schMakeHbRpcCtx(pJob, pTask, &hb.rpcCtx));

  code = taosHashPut(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId), &hb, sizeof(SSchHbTrans));
  if (code) {
    schFreeRpcCtx(&hb.rpcCtx);

    if (HASH_NODE_EXIST(code)) {
      *exist = true;
      return TSDB_CODE_SUCCESS;
    }

    qError("taosHashPut hb trans failed, nodeId:%d, fqdn:%s, port:%d", epId->nodeId, epId->ep.fqdn, epId->ep.port);
    SCH_ERR_RET(code);
  }

  return TSDB_CODE_SUCCESS;
}


int32_t schBuildAndSendHbMsg(SQueryNodeEpId *nodeEpId) {
  SSchedulerHbReq req = {0};
  int32_t         code = 0;
  SRpcCtx         rpcCtx = {0};
  SSchTrans       trans = {0};
  int32_t         msgType = TDMT_VND_QUERY_HEARTBEAT;

  req.header.vgId = nodeEpId->nodeId;
  req.sId = schMgmt.sId;
  memcpy(&req.epId, nodeEpId, sizeof(SQueryNodeEpId));

  SSchHbTrans *hb = taosHashGet(schMgmt.hbConnections, nodeEpId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    qError("taosHashGet hb connection failed, nodeId:%d, fqdn:%s, port:%d", nodeEpId->nodeId, nodeEpId->ep.fqdn,
           nodeEpId->ep.port);
    SCH_ERR_RET(code);
  }

  SCH_LOCK(SCH_WRITE, &hb->lock);
  code = schCloneHbRpcCtx(&hb->rpcCtx, &rpcCtx);
  memcpy(&trans, &hb->trans, sizeof(trans));
  SCH_UNLOCK(SCH_WRITE, &hb->lock);

  SCH_ERR_RET(code);

  int32_t msgSize = tSerializeSSchedulerHbReq(NULL, 0, &req);
  if (msgSize < 0) {
    qError("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }
  void *msg = taosMemoryCalloc(1, msgSize);
  if (NULL == msg) {
    qError("calloc hb req %d failed", msgSize);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (tSerializeSSchedulerHbReq(msg, msgSize, &req) < 0) {
    qError("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SMsgSendInfo *pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    qError("calloc SMsgSendInfo failed");
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SSchTaskCallbackParam *param = taosMemoryCalloc(1, sizeof(SSchTaskCallbackParam));
  if (NULL == param) {
    qError("calloc SSchTaskCallbackParam failed");
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  __async_send_cb_fn_t fp = NULL;
  SCH_ERR_JRET(schGetCallbackFp(msgType, &fp));

  param->transport = trans.pTrans;

  pMsgSendInfo->param = param;
  pMsgSendInfo->msgInfo.pData = msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgInfo.handle = trans.pHandle;
  pMsgSendInfo->msgType = msgType;
  pMsgSendInfo->fp = fp;

  int64_t transporterId = 0;
  SEpSet  epSet = {.inUse = 0, .numOfEps = 1};
  memcpy(&epSet.eps[0], &nodeEpId->ep, sizeof(nodeEpId->ep));

  qDebug("start to send hb msg, pTrans:%p, pHandle:%p, fqdn:%s, port:%d", trans.pTrans, trans.pHandle,
         nodeEpId->ep.fqdn, nodeEpId->ep.port);

  code = asyncSendMsgToServerExt(trans.pTrans, &epSet, &transporterId, pMsgSendInfo, true, &rpcCtx);
  if (code) {
    qError("fail to send hb msg, pTrans:%p, pHandle:%p, fqdn:%s, port:%d, error:%x - %s", trans.pTrans,
           trans.pHandle, nodeEpId->ep.fqdn, nodeEpId->ep.port, code, tstrerror(code));
    SCH_ERR_JRET(code);
  }

  qDebug("hb msg sent");
  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(msg);
  taosMemoryFreeClear(param);
  taosMemoryFreeClear(pMsgSendInfo);
  schFreeRpcCtx(&rpcCtx);
  SCH_RET(code);
}


int32_t schEnsureHbConnection(SSchJob *pJob, SSchTask *pTask) {
  SQueryNodeAddr *addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
  SQueryNodeEpId  epId = {0};

  epId.nodeId = addr->nodeId;

  SEp* pEp = SCH_GET_CUR_EP(addr);
  strcpy(epId.ep.fqdn, pEp->fqdn);
  epId.ep.port = pEp->port;

  SSchHbTrans *hb = taosHashGet(schMgmt.hbConnections, &epId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    bool exist = false;
    SCH_ERR_RET(schRegisterHbConnection(pJob, pTask, &epId, &exist));
    if (!exist) {
      SCH_ERR_RET(schBuildAndSendHbMsg(&epId));
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t schUpdateHbConnection(SQueryNodeEpId *epId, SSchTrans *trans) {
  int32_t      code = 0;
  SSchHbTrans *hb = NULL;

  hb = taosHashGet(schMgmt.hbConnections, epId, sizeof(SQueryNodeEpId));
  if (NULL == hb) {
    qError("taosHashGet hb connection failed, nodeId:%d, fqdn:%s, port:%d", epId->nodeId, epId->ep.fqdn, epId->ep.port);
    SCH_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
  }

  SCH_LOCK(SCH_WRITE, &hb->lock);
  memcpy(&hb->trans, trans, sizeof(*trans));
  SCH_UNLOCK(SCH_WRITE, &hb->lock);

  qDebug("hb connection updated, sId:%" PRIx64 ", nodeId:%d, fqdn:%s, port:%d, pTrans:%p, pHandle:%p", schMgmt.sId,
         epId->nodeId, epId->ep.fqdn, epId->ep.port, trans->pTrans, trans->pHandle);

  return TSDB_CODE_SUCCESS;
}

int32_t schHandleHbCallback(void *param, const SDataBuf *pMsg, int32_t code) {
  SSchedulerHbRsp rsp = {0};
  SSchTaskCallbackParam *pParam = (SSchTaskCallbackParam *)param;

  if (code) {
    qError("hb rsp error:%s", tstrerror(code));
    SCH_ERR_JRET(code);
  }

  if (tDeserializeSSchedulerHbRsp(pMsg->pData, pMsg->len, &rsp)) {
    qError("invalid hb rsp msg, size:%d", pMsg->len);
    SCH_ERR_JRET(TSDB_CODE_QRY_INVALID_INPUT);
  }

  SSchTrans trans = {0};
  trans.pTrans = pParam->transport;
  trans.pHandle = pMsg->handle;

  SCH_ERR_JRET(schUpdateHbConnection(&rsp.epId, &trans));

  int32_t taskNum = (int32_t)taosArrayGetSize(rsp.taskStatus);
  qDebug("%d task status in hb rsp, nodeId:%d, fqdn:%s, port:%d", taskNum, rsp.epId.nodeId, rsp.epId.ep.fqdn,
         rsp.epId.ep.port);

  for (int32_t i = 0; i < taskNum; ++i) {
    STaskStatus *taskStatus = taosArrayGet(rsp.taskStatus, i);

    SSchJob *pJob = schAcquireJob(taskStatus->refId);
    if (NULL == pJob) {
      qWarn("job not found, refId:0x%" PRIx64 ",QID:0x%" PRIx64 ",TID:0x%" PRIx64, taskStatus->refId,
            taskStatus->queryId, taskStatus->taskId);
      // TODO DROP TASK FROM SERVER!!!!
      continue;
    }

    // TODO

    SCH_JOB_DLOG("TID:0x%" PRIx64 " task status in server: %s", taskStatus->taskId,
                 jobTaskStatusStr(taskStatus->status));

    schReleaseJob(taskStatus->refId);
  }

_return:

  tFreeSSchedulerHbRsp(&rsp);
  taosMemoryFree(param);

  SCH_RET(code);
}

int32_t schMakeCallbackParam(SSchJob *pJob, SSchTask *pTask, void **pParam) {
  SSchTaskCallbackParam *param = taosMemoryCalloc(1, sizeof(SSchTaskCallbackParam));
  if (NULL == param) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SSchTaskCallbackParam));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  param->queryId = pJob->queryId;
  param->refId = pJob->refId;
  param->taskId = SCH_TASK_ID(pTask);
  param->transport = pJob->pTrans;

  *pParam = param;

  return TSDB_CODE_SUCCESS;
}

int32_t schMakeBrokenLinkVal(SSchJob *pJob, SSchTask *pTask, SRpcBrokenlinkVal *brokenVal, bool isHb) {
  int32_t       code = 0;
  SMsgSendInfo *pMsgSendInfo = NULL;

  pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
  if (NULL == pMsgSendInfo) {
    SCH_TASK_ELOG("calloc %d failed", (int32_t)sizeof(SMsgSendInfo));
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  if (isHb) {
    SCH_ERR_JRET(schMakeHbCallbackParam(pJob, pTask, &pMsgSendInfo->param));
  } else {
    SCH_ERR_JRET(schMakeCallbackParam(pJob, pTask, &pMsgSendInfo->param));
  }

  int32_t              msgType = TDMT_SCH_LINK_BROKEN;
  __async_send_cb_fn_t fp = NULL;
  SCH_ERR_JRET(schGetCallbackFp(msgType, &fp));

  pMsgSendInfo->fp = fp;

  brokenVal->msgType = msgType;
  brokenVal->val = pMsgSendInfo;
  brokenVal->clone = schCloneSMsgSendInfo;

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(pMsgSendInfo->param);
  taosMemoryFreeClear(pMsgSendInfo);

  SCH_RET(code);
}

int32_t schMakeQueryRpcCtx(SSchJob *pJob, SSchTask *pTask, SRpcCtx *pCtx) {
  int32_t       code = 0;
  SMsgSendInfo *pExplainMsgSendInfo = NULL;

  pCtx->args = taosHashInit(1, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  if (NULL == pCtx->args) {
    SCH_TASK_ELOG("taosHashInit %d RpcCtx failed", 1);
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_ERR_JRET(schGenerateCallBackInfo(pJob, pTask, TDMT_VND_EXPLAIN, &pExplainMsgSendInfo));

  int32_t    msgType = TDMT_VND_EXPLAIN_RSP;
  SRpcCtxVal ctxVal = {.val = pExplainMsgSendInfo, .clone = schCloneSMsgSendInfo};
  if (taosHashPut(pCtx->args, &msgType, sizeof(msgType), &ctxVal, sizeof(ctxVal))) {
    SCH_TASK_ELOG("taosHashPut msg %d to rpcCtx failed", msgType);
    SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  SCH_ERR_JRET(schMakeBrokenLinkVal(pJob, pTask, &pCtx->brokenVal, false));
  pCtx->freeFunc = schFreeRpcCtxVal;

  return TSDB_CODE_SUCCESS;

_return:

  taosHashCleanup(pCtx->args);

  if (pExplainMsgSendInfo) {
    taosMemoryFreeClear(pExplainMsgSendInfo->param);
    taosMemoryFreeClear(pExplainMsgSendInfo);
  }

  SCH_RET(code);
}

int32_t schCloneCallbackParam(SSchCallbackParamHeader *pSrc, SSchCallbackParamHeader **pDst) {
  if (pSrc->isHbParam) {
    SSchHbCallbackParam *dst = taosMemoryMalloc(sizeof(SSchHbCallbackParam));
    if (NULL == dst) {
      qError("malloc SSchHbCallbackParam failed");
      SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
    }

    memcpy(dst, pSrc, sizeof(*dst));
    *pDst = (SSchCallbackParamHeader *)dst;

    return TSDB_CODE_SUCCESS;
  }

  SSchTaskCallbackParam *dst = taosMemoryMalloc(sizeof(SSchTaskCallbackParam));
  if (NULL == dst) {
    qError("malloc SSchTaskCallbackParam failed");
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  memcpy(dst, pSrc, sizeof(*dst));
  *pDst = (SSchCallbackParamHeader *)dst;

  return TSDB_CODE_SUCCESS;
}

int32_t schCloneSMsgSendInfo(void *src, void **dst) {
  SMsgSendInfo *pSrc = src;
  int32_t       code = 0;
  SMsgSendInfo *pDst = taosMemoryMalloc(sizeof(*pSrc));
  if (NULL == pDst) {
    qError("malloc SMsgSendInfo for rpcCtx failed, len:%d", (int32_t)sizeof(*pSrc));
    SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
  }

  memcpy(pDst, pSrc, sizeof(*pSrc));
  pDst->param = NULL;

  SCH_ERR_JRET(schCloneCallbackParam(pSrc->param, (SSchCallbackParamHeader **)&pDst->param));

  *dst = pDst;

  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(pDst);
  SCH_RET(code);
}


int32_t schAsyncSendMsg(SSchJob *pJob, SSchTask *pTask, void *transport, SEpSet *epSet, int32_t msgType, void *msg,
                        uint32_t msgSize, bool persistHandle, SRpcCtx *ctx) {
  int32_t code = 0;

  SSchTrans *trans = (SSchTrans *)transport;

  SMsgSendInfo *pMsgSendInfo = NULL;
  SCH_ERR_JRET(schGenerateCallBackInfo(pJob, pTask, msgType, &pMsgSendInfo));

  pMsgSendInfo->msgInfo.pData = msg;
  pMsgSendInfo->msgInfo.len = msgSize;
  pMsgSendInfo->msgInfo.handle = trans->pHandle;
  pMsgSendInfo->msgType = msgType;

  qDebug("start to send %s msg to node[%d,%s,%d], refId:%" PRIx64 "pTrans:%p, pHandle:%p", TMSG_INFO(msgType),
         ntohl(((SMsgHead *)msg)->vgId), epSet->eps[epSet->inUse].fqdn, epSet->eps[epSet->inUse].port, pJob->refId,
         trans->pTrans, trans->pHandle);

  int64_t transporterId = 0;
  code = asyncSendMsgToServerExt(trans->pTrans, epSet, &transporterId, pMsgSendInfo, persistHandle, ctx);
  if (code) {
    SCH_ERR_JRET(code);
  }

  SCH_TASK_DLOG("req msg sent, refId:%" PRIx64 ", type:%d, %s", pJob->refId, msgType, TMSG_INFO(msgType));
  return TSDB_CODE_SUCCESS;

_return:

  if (pMsgSendInfo) {
    taosMemoryFreeClear(pMsgSendInfo->param);
    taosMemoryFreeClear(pMsgSendInfo);
  }

  SCH_RET(code);
}

int32_t schBuildAndSendMsg(SSchJob *pJob, SSchTask *pTask, SQueryNodeAddr *addr, int32_t msgType) {
  uint32_t msgSize = 0;
  void    *msg = NULL;
  int32_t  code = 0;
  bool     isCandidateAddr = false;
  bool     persistHandle = false;
  SRpcCtx  rpcCtx = {0};

  if (NULL == addr) {
    addr = taosArrayGet(pTask->candidateAddrs, pTask->candidateIdx);
    isCandidateAddr = true;
  }

  SEpSet epSet = addr->epSet;

  switch (msgType) {
    case TDMT_VND_CREATE_TABLE:
    case TDMT_VND_DROP_TABLE:
    case TDMT_VND_ALTER_TABLE:
    case TDMT_VND_SUBMIT: {
      msgSize = pTask->msgLen;
      msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      memcpy(msg, pTask->msg, msgSize);
      break;
    }

    case TDMT_VND_QUERY: {
      SCH_ERR_RET(schMakeQueryRpcCtx(pJob, pTask, &rpcCtx));

      uint32_t len = strlen(pJob->sql);
      msgSize = sizeof(SSubQueryMsg) + pTask->msgLen + len;
      msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SSubQueryMsg *pMsg = msg;
      pMsg->header.vgId = htonl(addr->nodeId);
      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);
      pMsg->refId = htobe64(pJob->refId);
      pMsg->taskType = TASK_TYPE_TEMP;
      pMsg->explain = SCH_IS_EXPLAIN_JOB(pJob);
      pMsg->phyLen = htonl(pTask->msgLen);
      pMsg->sqlLen = htonl(len);

      memcpy(pMsg->msg, pJob->sql, len);
      memcpy(pMsg->msg + len, pTask->msg, pTask->msgLen);

      persistHandle = true;
      break;
    }
    case TDMT_VND_FETCH: {
      msgSize = sizeof(SResFetchReq);
      msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      SResFetchReq *pMsg = msg;

      pMsg->header.vgId = htonl(addr->nodeId);

      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);

      break;
    }
    case TDMT_VND_DROP_TASK: {
      msgSize = sizeof(STaskDropReq);
      msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        SCH_TASK_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      STaskDropReq *pMsg = msg;

      pMsg->header.vgId = htonl(addr->nodeId);

      pMsg->sId = htobe64(schMgmt.sId);
      pMsg->queryId = htobe64(pJob->queryId);
      pMsg->taskId = htobe64(pTask->taskId);
      pMsg->refId = htobe64(pJob->refId);
      break;
    }
    case TDMT_VND_QUERY_HEARTBEAT: {
      SCH_ERR_RET(schMakeHbRpcCtx(pJob, pTask, &rpcCtx));

      SSchedulerHbReq req = {0};
      req.sId = schMgmt.sId;
      req.header.vgId = addr->nodeId;
      req.epId.nodeId = addr->nodeId;
      memcpy(&req.epId.ep, SCH_GET_CUR_EP(addr), sizeof(SEp));

      msgSize = tSerializeSSchedulerHbReq(NULL, 0, &req);
      if (msgSize < 0) {
        SCH_JOB_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      msg = taosMemoryCalloc(1, msgSize);
      if (NULL == msg) {
        SCH_JOB_ELOG("calloc %d failed", msgSize);
        SCH_ERR_RET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }
      if (tSerializeSSchedulerHbReq(msg, msgSize, &req) < 0) {
        SCH_JOB_ELOG("tSerializeSSchedulerHbReq hbReq failed, size:%d", msgSize);
        SCH_ERR_JRET(TSDB_CODE_QRY_OUT_OF_MEMORY);
      }

      persistHandle = true;
      break;
    }
    default:
      SCH_TASK_ELOG("unknown msg type to send, msgType:%d", msgType);
      SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
      break;
  }

  SCH_SET_TASK_LASTMSG_TYPE(pTask, msgType);

  SSchTrans trans = {.pTrans = pJob->pTrans, .pHandle = SCH_GET_TASK_HANDLE(pTask)};
  SCH_ERR_JRET(schAsyncSendMsg(pJob, pTask, &trans, &epSet, msgType, msg, msgSize, persistHandle,
                               (rpcCtx.args ? &rpcCtx : NULL)));

  if (msgType == TDMT_VND_QUERY) {
    SCH_ERR_RET(schRecordTaskExecNode(pJob, pTask, addr, trans.pHandle));
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_SET_TASK_LASTMSG_TYPE(pTask, -1);
  schFreeRpcCtx(&rpcCtx);

  taosMemoryFreeClear(msg);
  SCH_RET(code);
}



