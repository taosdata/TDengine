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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "mmInt.h"
#include "streamMsg.h"
#include "stream.h"
#include "streamReader.h"

#define PROCESS_THRESHOLD (2000 * 1000)

static inline int32_t mmAcquire(SMnodeMgmt *pMgmt) {
  int32_t code = 0;
  (void)taosThreadRwlockRdlock(&pMgmt->lock);
  if (pMgmt->stopped) {
    code = TSDB_CODE_MNODE_STOPPED;
  } else {
    (void)atomic_add_fetch_32(&pMgmt->refCount, 1);
  }
  (void)taosThreadRwlockUnlock(&pMgmt->lock);
  return code;
}

static inline void mmRelease(SMnodeMgmt *pMgmt) {
  (void)taosThreadRwlockRdlock(&pMgmt->lock);
  (void)atomic_sub_fetch_32(&pMgmt->refCount, 1);
  (void)taosThreadRwlockUnlock(&pMgmt->lock);
}

static inline void mmSendRsp(SRpcMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {
      .code = code,
      .pCont = pMsg->info.rsp,
      .contLen = pMsg->info.rspLen,
      .info = pMsg->info,
  };
  tmsgSendRsp(&rsp);
}

static void mmProcessRpcMsg(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  pMsg->info.node = pMgmt->pMnode;

  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, get from mnode queue, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

  int32_t code = mndProcessRpcMsg(pMsg, pInfo);

  if (pInfo->timestamp != 0) {
    int64_t cost = taosGetTimestampUs() - pInfo->timestamp;
    if (cost > PROCESS_THRESHOLD) {
      dGWarn("worker:%d,message has been processed for too long, type:%s, cost: %" PRId64 "s", pInfo->threadNum,
             TMSG_INFO(pMsg->msgType), cost / (1000 * 1000));
    }
  }

  if (IsReq(pMsg) && pMsg->info.handle != NULL && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    if (code != 0 && terrno != 0) code = terrno;
    mmSendRsp(pMsg, code);
  } else {
    rpcFreeCont(pMsg->info.rsp);
    pMsg->info.rsp = NULL;
  }

  if (code == TSDB_CODE_SYN_NOT_LEADER || code == TSDB_CODE_SYN_RESTORING) {
    mndPostProcessQueryMsg(pMsg);
  }

  dGTrace("msg:%p is freed, code:%s", pMsg, tstrerror(code));
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void mmProcessSyncMsg(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  pMsg->info.node = pMgmt->pMnode;

  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, get from mnode-sync queue", pMsg);

  SMsgHead *pHead = pMsg->pCont;
  pHead->contLen = ntohl(pHead->contLen);
  pHead->vgId = ntohl(pHead->vgId);

  int32_t code = mndProcessSyncMsg(pMsg);

  dGTrace("msg:%p, is freed, code:0x%x", pMsg, code);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}

static void mmProcessStreamHbMsg(SQueueInfo *pInfo, SRpcMsg *pMsg) {
  SMnodeMgmt *pMgmt = pInfo->ahandle;
  pMsg->info.node = pMgmt->pMnode;

  const STraceId *trace = &pMsg->info.traceId;
  dGTrace("msg:%p, get from mnode-stream-mgmt queue", pMsg);

  (void)mndProcessStreamHb(pMsg);

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
}


static inline int32_t mmPutMsgToWorker(SMnodeMgmt *pMgmt, SSingleWorker *pWorker, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  int32_t         code = 0;
  if ((code = mmAcquire(pMgmt)) == 0) {
    if(tsSyncLogHeartbeat){
      dGInfo("msg:%p, put into %s queue, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
    }
    else{
      dGTrace("msg:%p, put into %s queue, type:%s", pMsg, pWorker->name, TMSG_INFO(pMsg->msgType));
    }
    code = taosWriteQitem(pWorker->queue, pMsg);
    mmRelease(pMgmt);
    return code;
  } else {
    dGTrace("msg:%p, failed to put into %s queue since %s, type:%s", pMsg, pWorker->name, tstrerror(code),
            TMSG_INFO(pMsg->msgType));
    return code;
  }
}

int32_t mmPutMsgToWriteQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->writeWorker, pMsg);
}

int32_t mmPutMsgToArbQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->arbWorker, pMsg);
}

int32_t mmPutMsgToSyncQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->syncWorker, pMsg);
}

int32_t mmPutMsgToSyncRdQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->syncRdWorker, pMsg);
}

int32_t mmPutMsgToReadQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->readWorker, pMsg);
}

int32_t mmPutMsgToStatusQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->statusWorker, pMsg);
}

int32_t mmPutMsgToQueryQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  int32_t         code = 0;
  int32_t         qType = 0;
  const STraceId *trace = &pMsg->info.traceId;

  if (NULL == pMgmt->pMnode) {
    code = TSDB_CODE_MNODE_NOT_FOUND;
    dGError("msg:%p, stop to pre-process in mnode since %s, type:%s", pMsg, tstrerror(code), TMSG_INFO(pMsg->msgType));
    return code;
  }

  pMsg->info.node = pMgmt->pMnode;
  if ((code = mndPreProcessQueryMsg(pMsg, &qType)) != 0) {
    dGError("msg:%p, failed to pre-process in mnode since %s, type:%s", pMsg, tstrerror(code),
            TMSG_INFO(pMsg->msgType));
    return code;
  }

  if (qType == TASK_TYPE_QUERY) {
    return mmPutMsgToWorker(pMgmt, &pMgmt->queryWorker, pMsg);
  } else if (qType == TASK_TYPE_HQUERY) {
    return mmPutMsgToWorker(pMgmt, &pMgmt->mqueryWorker, pMsg);
  } else {
    code = TSDB_CODE_INVALID_PARA;
    dGError("msg:%p, invalid task qType:%d, not put into (m)query queue, type:%s", pMsg, qType,
            TMSG_INFO(pMsg->msgType));
    return code;
  }
}

int32_t mmPutMsgToFetchQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return mmPutMsgToWorker(pMgmt, &pMgmt->fetchWorker, pMsg);
}

int32_t mmPutMsgToStreamMgmtQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  return tAddTaskIntoDispatchWorkerPool(&pMgmt->streamMgmtWorkerPool, pMsg);
}

int32_t mmPutMsgToStreamReaderQueue(SMnodeMgmt *pMgmt, SRpcMsg *pMsg) {
  const STraceId *trace = &pMsg->info.traceId;
  int32_t         code = 0;
  if ((code = mmAcquire(pMgmt)) == 0) {
    dGDebug("msg:%p, put into %s queue, type:%s", pMsg, pMgmt->streamReaderPool.name, TMSG_INFO(pMsg->msgType));
    code = taosWriteQitem(pMgmt->pStreamReaderQ, pMsg);
    mmRelease(pMgmt);
    return code;
  } else {
    dGDebug("msg:%p, failed to put into %s queue since %s, type:%s", pMsg, pMgmt->streamReaderPool.name, tstrerror(code),
            TMSG_INFO(pMsg->msgType));
    return code;
  }
}


int32_t mmPutMsgToQueue(SMnodeMgmt *pMgmt, EQueueType qtype, SRpcMsg *pRpc) {
  int32_t code;

  SSingleWorker *pWorker = NULL;
  switch (qtype) {
    case WRITE_QUEUE:
      pWorker = &pMgmt->writeWorker;
      break;
    case QUERY_QUEUE:
      pWorker = &pMgmt->queryWorker;
      break;
    case FETCH_QUEUE:
      pWorker = &pMgmt->fetchWorker;
      break;
    case READ_QUEUE:
      pWorker = &pMgmt->readWorker;
      break;
    case STATUS_QUEUE:
      pWorker = &pMgmt->statusWorker;
      break;
    case ARB_QUEUE:
      pWorker = &pMgmt->arbWorker;
      break;
    case SYNC_QUEUE:
      pWorker = &pMgmt->syncWorker;
      break;
    case SYNC_RD_QUEUE:
      pWorker = &pMgmt->syncRdWorker;
      break;
    default:
      code = TSDB_CODE_INVALID_PARA;
  }

  if (pWorker == NULL) return code;

  SRpcMsg *pMsg;
  code = taosAllocateQitem(sizeof(SRpcMsg), RPC_QITEM, pRpc->contLen, (void **)&pMsg);
  if (code) return code;

  memcpy(pMsg, pRpc, sizeof(SRpcMsg));
  pRpc->pCont = NULL;

  dTrace("msg:%p, is created and will put into %s queue, type:%s len:%d", pMsg, pWorker->name, TMSG_INFO(pRpc->msgType),
         pRpc->contLen);
  code = mmPutMsgToWorker(pMgmt, pWorker, pMsg);
  if (code != 0) {
    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
  return code;
}

int32_t mmDispatchStreamHbMsg(struct SDispatchWorkerPool* pPool, void* pParam, int32_t *pWorkerIdx) {
  SRpcMsg* pMsg = (SRpcMsg*)pParam;
  SStreamMsgGrpHeader* pHeader = (SStreamMsgGrpHeader*)pMsg->pCont;
  *pWorkerIdx = pHeader->streamGid % tsNumOfMnodeStreamMgmtThreads;
  return TSDB_CODE_SUCCESS;
}

static int32_t mmProcessStreamFetchMsg(SMnodeMgmt *pMgmt, SRpcMsg* pMsg) {
  int32_t            code = 0;
  int32_t            lino = 0;
  void*              buf = NULL;
  size_t             size = 0;
  SSDataBlock*       pBlock = NULL;
  void*              taskAddr = NULL;
  SArray*            pResList = NULL;
  
  SResFetchReq req = {0};
  STREAM_CHECK_CONDITION_GOTO(tDeserializeSResFetchReq(pMsg->pCont, pMsg->contLen, &req) < 0,
                              TSDB_CODE_QRY_INVALID_INPUT);
  SArray* calcInfoList = (SArray*)qStreamGetReaderInfo(req.queryId, req.taskId, &taskAddr);
  STREAM_CHECK_NULL_GOTO(calcInfoList, terrno);

  STREAM_CHECK_CONDITION_GOTO(req.execId < 0, TSDB_CODE_INVALID_PARA);
  SStreamTriggerReaderCalcInfo* sStreamReaderCalcInfo = taosArrayGetP(calcInfoList, req.execId);
  STREAM_CHECK_NULL_GOTO(sStreamReaderCalcInfo, terrno);
  void* pTask = sStreamReaderCalcInfo->pTask;
  ST_TASK_DLOG("mnode %s start", __func__);

  if (req.reset || sStreamReaderCalcInfo->pTaskInfo == NULL) {
    qDestroyTask(sStreamReaderCalcInfo->pTaskInfo);
    int64_t uid = 0;
    if (req.dynTbname) {
      SArray* vals = req.pStRtFuncInfo->pStreamPartColVals;
      for (int32_t i = 0; i < taosArrayGetSize(vals); ++i) {
        SStreamGroupValue* pValue = taosArrayGet(vals, i);
        if (pValue != NULL && pValue->isTbname) {
          uid = pValue->uid;
          break;
        }
      }
    }
    
    SReadHandle handle = {0};
    handle.mnd = pMgmt->pMnode;
    handle.uid = uid;
    handle.pMsgCb = &pMgmt->msgCb;

    //initStorageAPI(&handle.api);

    TSWAP(sStreamReaderCalcInfo->rtInfo.funcInfo, *req.pStRtFuncInfo);
    handle.streamRtInfo = &sStreamReaderCalcInfo->rtInfo;

    STREAM_CHECK_RET_GOTO(qCreateStreamExecTaskInfo(&sStreamReaderCalcInfo->pTaskInfo,
                                                    sStreamReaderCalcInfo->calcScanPlan, &handle, NULL, MNODE_HANDLE,
                                                    req.taskId));


    STREAM_CHECK_RET_GOTO(qSetTaskId(sStreamReaderCalcInfo->pTaskInfo, req.taskId, req.queryId));
  }

  if (req.pOpParam != NULL) {
    qUpdateOperatorParam(sStreamReaderCalcInfo->pTaskInfo, req.pOpParam);
  }
  
  pResList = taosArrayInit(4, POINTER_BYTES);
  STREAM_CHECK_NULL_GOTO(pResList, terrno);
  uint64_t ts = 0;
  bool     hasNext = false;
  STREAM_CHECK_RET_GOTO(qExecTaskOpt(sStreamReaderCalcInfo->pTaskInfo, pResList, &ts, &hasNext, NULL, true));

  for(size_t i = 0; i < taosArrayGetSize(pResList); i++){
    SSDataBlock* pBlock = taosArrayGetP(pResList, i);
    if (pBlock == NULL) continue;
    printDataBlock(pBlock, __func__, "streemFetch", ((SStreamTask*)pTask)->streamId);
    if (sStreamReaderCalcInfo->rtInfo.funcInfo.withExternalWindow && pBlock != NULL) {
      STREAM_CHECK_RET_GOTO(qStreamFilter(pBlock, sStreamReaderCalcInfo->pFilterInfo, NULL));
      printDataBlock(pBlock, __func__, "fetch filter", ((SStreamTask*)pTask)->streamId);
    }
  }

  STREAM_CHECK_RET_GOTO(streamBuildFetchRsp(pResList, hasNext, &buf, &size, TSDB_TIME_PRECISION_MILLI));
  ST_TASK_DLOG("%s end:", __func__);

end:
  taosArrayDestroy(pResList);
  streamReleaseTask(taskAddr);

  STREAM_PRINT_LOG_END(code, lino);
  SRpcMsg rsp = {.msgType = pMsg->msgType + 1, .info = pMsg->info, .pCont = buf, .contLen = size, .code = code};
  tmsgSendRsp(&rsp);
  tDestroySResFetchReq(&req);
  return code;
}

int32_t mmProcessStreamReaderMsg(SMnodeMgmt *pMgmt, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  pMsg->info.node = pMgmt->pMnode;

  const STraceId *trace = &pMsg->info.traceId;
  dDebug("msg:%p, get from mnode-stream-reader queue", pMsg);

  if (pMsg->msgType == TDMT_STREAM_FETCH || pMsg->msgType == TDMT_STREAM_FETCH_FROM_CACHE) {
    TAOS_CHECK_EXIT(mmProcessStreamFetchMsg(pMgmt, pMsg));
  } else {
    dError("unknown msg type:%d in stream reader queue", pMsg->msgType);
    TAOS_CHECK_EXIT(TSDB_CODE_APP_ERROR);
  }

_exit:

  if (code != 0) {                                                         
    dError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}


static void mmProcessStreamReaderQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SMnodeMgmt *pMnode = pInfo->ahandle;
  SRpcMsg   *pMsg = NULL;

  for (int32_t i = 0; i < numOfMsgs; ++i) {
    if (taosGetQitem(qall, (void **)&pMsg) == 0) continue;
    const STraceId *trace = &pMsg->info.traceId;
    dGDebug("msg:%p, get from mnode-stream-reader queue", pMsg);

    terrno = 0;
    int32_t code = mmProcessStreamReaderMsg(pMnode, pMsg);

    dGDebug("msg:%p, is freed, code:0x%x [mmProcessStreamReaderQueue]", pMsg, code);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}


int32_t mmStartWorker(SMnodeMgmt *pMgmt) {
  int32_t          code = 0;
  SSingleWorkerCfg qCfg = {
      .min = tsNumOfMnodeQueryThreads,
      .max = tsNumOfMnodeQueryThreads,
      .name = "mnode-query",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
      .poolType = QUERY_AUTO_QWORKER_POOL,
  };
  if ((code = tSingleWorkerInit(&pMgmt->queryWorker, &qCfg)) != 0) {
    dError("failed to start mnode-query worker since %s", tstrerror(code));
    return code;
  }

  tsNumOfQueryThreads += tsNumOfMnodeQueryThreads;

  SSingleWorkerCfg mqCfg = {
      .min = 4,
      .max = 4,
      .name = "mnode-mquery",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
      .poolType = QUERY_AUTO_QWORKER_POOL,
  };
  if ((code = tSingleWorkerInit(&pMgmt->mqueryWorker, &mqCfg)) != 0) {
    dError("failed to start mnode-mquery worker since %s", tstrerror(code));
    return code;
  }

  tsNumOfQueryThreads += 4;

  SSingleWorkerCfg fCfg = {
      .min = tsNumOfMnodeFetchThreads,
      .max = tsNumOfMnodeFetchThreads,
      .name = "mnode-fetch",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->fetchWorker, &fCfg)) != 0) {
    dError("failed to start mnode-fetch worker since %s", tstrerror(code));
    return code;
  }

  SSingleWorkerCfg rCfg = {
      .min = tsNumOfMnodeReadThreads,
      .max = tsNumOfMnodeReadThreads,
      .name = "mnode-read",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->readWorker, &rCfg)) != 0) {
    dError("failed to start mnode-read worker since %s", tstrerror(code));
    return code;
  }

  SSingleWorkerCfg stautsCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-status",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->statusWorker, &stautsCfg)) != 0) {
    dError("failed to start mnode-status worker since %s", tstrerror(code));
    return code;
  }

  SSingleWorkerCfg wCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-write",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->writeWorker, &wCfg)) != 0) {
    dError("failed to start mnode-write worker since %s", tstrerror(code));
    return code;
  }

  SSingleWorkerCfg sCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-sync",
      .fp = (FItem)mmProcessSyncMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->syncWorker, &sCfg)) != 0) {
    dError("failed to start mnode mnode-sync worker since %s", tstrerror(code));
    return code;
  }

  SSingleWorkerCfg scCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-sync-rd",
      .fp = (FItem)mmProcessSyncMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->syncRdWorker, &scCfg)) != 0) {
    dError("failed to start mnode mnode-sync-rd worker since %s", tstrerror(code));
    return code;
  }

  SSingleWorkerCfg arbCfg = {
      .min = 1,
      .max = 1,
      .name = "mnode-arb",
      .fp = (FItem)mmProcessRpcMsg,
      .param = pMgmt,
  };
  if ((code = tSingleWorkerInit(&pMgmt->arbWorker, &arbCfg)) != 0) {
    dError("failed to start mnode mnode-arb worker since %s", tstrerror(code));
    return code;
  }

  SDispatchWorkerPool* pPool = &pMgmt->streamMgmtWorkerPool;
  pPool->max = tsNumOfMnodeStreamMgmtThreads;
  pPool->name = "mnode-stream-mgmt";
  code = tDispatchWorkerInit(pPool);
  if (code != 0) {
    dError("failed to start mnode stream-mgmt worker since %s", tstrerror(code));
    return code;
  }
  code = tDispatchWorkerAllocQueue(pPool, pMgmt, (FItem)mmProcessStreamHbMsg, mmDispatchStreamHbMsg);
  if (code != 0) {
    dError("failed to start mnode stream-mgmt worker since %s", tstrerror(code));
    return code;
  }

  SWWorkerPool *pStreamReaderPool = &pMgmt->streamReaderPool;
  pStreamReaderPool->name = "mnode-st-reader";
  pStreamReaderPool->max = 2;
  if ((code = tWWorkerInit(pStreamReaderPool)) != 0) return code;

  pMgmt->pStreamReaderQ = tWWorkerAllocQueue(&pMgmt->streamReaderPool, pMgmt, mmProcessStreamReaderQueue);

  dDebug("mnode workers are initialized");
  return code;
}

void mmStopWorker(SMnodeMgmt *pMgmt) {
  while (pMgmt->refCount > 0) taosMsleep(10);

  tSingleWorkerCleanup(&pMgmt->queryWorker);
  tSingleWorkerCleanup(&pMgmt->mqueryWorker);
  tSingleWorkerCleanup(&pMgmt->fetchWorker);
  tSingleWorkerCleanup(&pMgmt->readWorker);
  tSingleWorkerCleanup(&pMgmt->statusWorker);
  tSingleWorkerCleanup(&pMgmt->writeWorker);
  tSingleWorkerCleanup(&pMgmt->arbWorker);
  tSingleWorkerCleanup(&pMgmt->syncWorker);
  tSingleWorkerCleanup(&pMgmt->syncRdWorker);
  tDispatchWorkerCleanup(&pMgmt->streamMgmtWorkerPool);
  tWWorkerFreeQueue(&pMgmt->streamReaderPool, pMgmt->pStreamReaderQ);
  tWWorkerCleanup(&pMgmt->streamReaderPool);
  
  dDebug("mnode workers are closed");
}
