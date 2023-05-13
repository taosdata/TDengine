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

#include "tq.h"
#include "vnd.h"

<<<<<<< HEAD
=======
#if 0
void tqTmrRspFunc(void* param, void* tmrId) {
  STqHandle* pHandle = (STqHandle*)param;
  atomic_store_8(&pHandle->pushHandle.tmrStopped, 1);
}

static int32_t tqLoopExecFromQueue(STQ* pTq, STqHandle* pHandle, SStreamDataSubmit** ppSubmit, SMqDataRsp* pRsp) {
  SStreamDataSubmit* pSubmit = *ppSubmit;
  while (pSubmit != NULL) {
    if (tqLogScanExec(pTq, &pHandle->execHandle, pSubmit->data, pRsp, 0) < 0) {
    }
    // update processed
    atomic_store_64(&pHandle->pushHandle.processedVer, pSubmit->ver);
    streamQueueProcessSuccess(&pHandle->pushHandle.inputQ);
    streamDataSubmitDestroy(pSubmit);
    if (pRsp->blockNum > 0) {
      *ppSubmit = pSubmit;
      return 0;
    } else {
      pSubmit = streamQueueNextItem(&pHandle->pushHandle.inputQ);
    }
  }
  *ppSubmit = pSubmit;
  return -1;
}

int32_t tqExecFromInputQ(STQ* pTq, STqHandle* pHandle) {
  SMqDataRsp rsp = {0};
  // 1. guard and set status executing
  int8_t execStatus = atomic_val_compare_exchange_8(&pHandle->pushHandle.execStatus, TASK_EXEC_STATUS__IDLE,
                                                    TASK_EXEC_STATUS__EXECUTING);
  if (execStatus == TASK_EXEC_STATUS__IDLE) {
    SStreamDataSubmit* pSubmit = NULL;
    // 2. check processedVer
    // 2.1. if not missed, get msg from queue
    // 2.2. if missed, scan wal
    pSubmit = streamQueueNextItem(&pHandle->pushHandle.inputQ);
    while (pHandle->pushHandle.processedVer <= pSubmit->ver) {
      // read from wal
    }
    while (pHandle->pushHandle.processedVer > pSubmit->ver + 1) {
      streamQueueProcessSuccess(&pHandle->pushHandle.inputQ);
      streamDataSubmitDestroy(pSubmit);
      pSubmit = streamQueueNextItem(&pHandle->pushHandle.inputQ);
      if (pSubmit == NULL) break;
    }
    // 3. exec, after each success, update processed ver
    // first run
    if (tqLoopExecFromQueue(pTq, pHandle, &pSubmit, &rsp) == 0) {
      goto SEND_RSP;
    }
    // set exec status closing
    atomic_store_8(&pHandle->pushHandle.execStatus, TASK_EXEC_STATUS__CLOSING);
    // second run
    if (tqLoopExecFromQueue(pTq, pHandle, &pSubmit, &rsp) == 0) {
      goto SEND_RSP;
    }
    // set exec status idle
    atomic_store_8(&pHandle->pushHandle.execStatus, TASK_EXEC_STATUS__IDLE);
  }
SEND_RSP:
  // 4. if get result
  // 4.1 set exec input status blocked and exec status idle
  atomic_store_8(&pHandle->pushHandle.execStatus, TASK_EXEC_STATUS__IDLE);
  // 4.2 rpc send
  rsp.rspOffset = pHandle->pushHandle.processedVer;
  /*if (tqSendPollRsp(pTq, pMsg, pReq, &rsp) < 0) {*/
  /*return -1;*/
  /*}*/
  // 4.3 clear rpc info
  memset(&pHandle->pushHandle.rpcInfo, 0, sizeof(SRpcHandleInfo));
  return 0;
}

int32_t tqOpenPushHandle(STQ* pTq, STqHandle* pHandle) {
  memset(&pHandle->pushHandle, 0, sizeof(STqPushHandle));
  pHandle->pushHandle.inputQ.queue = taosOpenQueue();
  pHandle->pushHandle.inputQ.qall = taosAllocateQall();
  if (pHandle->pushHandle.inputQ.queue == NULL || pHandle->pushHandle.inputQ.qall == NULL) {
    if (pHandle->pushHandle.inputQ.queue) {
      taosCloseQueue(pHandle->pushHandle.inputQ.queue);
    }
    if (pHandle->pushHandle.inputQ.qall) {
      taosFreeQall(pHandle->pushHandle.inputQ.qall);
    }
    return -1;
  }
  return 0;
}

int32_t tqPreparePush(STQ* pTq, STqHandle* pHandle, int64_t reqId, const SRpcHandleInfo* pInfo, int64_t processedVer,
                      int64_t timeout) {
  memcpy(&pHandle->pushHandle.rpcInfo, pInfo, sizeof(SRpcHandleInfo));
  atomic_store_64(&pHandle->pushHandle.reqId, reqId);
  atomic_store_64(&pHandle->pushHandle.processedVer, processedVer);
  atomic_store_8(&pHandle->pushHandle.inputStatus, TASK_INPUT_STATUS__NORMAL);
  atomic_store_8(&pHandle->pushHandle.tmrStopped, 0);
  taosTmrReset(tqTmrRspFunc, (int32_t)timeout, pHandle, tqMgmt.timer, &pHandle->pushHandle.timerId);
  return 0;
}

int32_t tqEnqueue(STqHandle* pHandle, SStreamDataSubmit* pSubmit) {
  int8_t inputStatus = atomic_load_8(&pHandle->pushHandle.inputStatus);
  if (inputStatus == TASK_INPUT_STATUS__NORMAL) {
    SStreamDataSubmit* pSubmitClone = streamSubmitBlockClone(pSubmit);
    if (pSubmitClone == NULL) {
      return -1;
    }
    taosWriteQitem(pHandle->pushHandle.inputQ.queue, pSubmitClone);
    return 0;
  }
  return -1;
}

int32_t tqSendExecReq(STQ* pTq, STqHandle* pHandle) {
  //
  return 0;
}

int32_t tqPushMsgNew(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver, SRpcHandleInfo handleInfo) {
  if (msgType != TDMT_VND_SUBMIT) return 0;
  void*       pIter = NULL;
  STqHandle*  pHandle = NULL;
  SSubmitReq* pReq = (SSubmitReq*)msg;
  int32_t     workerId = 4;
  int64_t     fetchOffset = ver;

  while (1) {
    pIter = taosHashIterate(pTq->pushMgr, pIter);
    if (pIter == NULL) break;
    pHandle = *(STqHandle**)pIter;

    taosWLockLatch(&pHandle->pushHandle.lock);

    SMqDataRsp rsp = {0};
    rsp.reqOffset = pHandle->pushHandle.reqOffset;
    rsp.blockData = taosArrayInit(0, sizeof(void*));
    rsp.blockDataLen = taosArrayInit(0, sizeof(int32_t));

    if (msgType == TDMT_VND_SUBMIT) {
      tqLogScanExec(pTq, &pHandle->execHandle, pReq, &rsp, workerId);
    } else {
      tqError("tq push unexpected msg type %d", msgType);
    }

    if (rsp.blockNum == 0) {
      taosWUnLockLatch(&pHandle->pushHandle.lock);
      continue;
    }

    rsp.rspOffset = fetchOffset;

    int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqDataBlkRsp(NULL, &rsp);
    void*   buf = rpcMallocCont(tlen);
    if (buf == NULL) {
      // todo free
      return -1;
    }

    ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
    ((SMqRspHead*)buf)->epoch = pHandle->pushHandle.epoch;
    ((SMqRspHead*)buf)->consumerId = pHandle->pushHandle.consumerId;

    void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
    tEncodeSMqDataBlkRsp(&abuf, &rsp);

    SRpcMsg resp = {
        .info = pHandle->pushHandle.rpcInfo,
        .pCont = buf,
        .contLen = tlen,
        .code = 0,
    };
    tmsgSendRsp(&resp);

    memset(&pHandle->pushHandle.rpcInfo, 0, sizeof(SRpcHandleInfo));
    taosWUnLockLatch(&pHandle->pushHandle.lock);

    tqDebug("vgId:%d offset %" PRId64 " from consumer:%" PRId64 ", (epoch %d) send rsp, block num: %d, req:%" PRId64 ", rsp:%" PRId64,
            TD_VID(pTq->pVnode), fetchOffset, pHandle->pushHandle.consumerId, pHandle->pushHandle.epoch, rsp.blockNum,
            rsp.reqOffset, rsp.rspOffset);

    // TODO destroy
    taosArrayDestroy(rsp.blockData);
    taosArrayDestroy(rsp.blockDataLen);
  }

  return 0;
}
#endif

typedef struct {
  void*   pKey;
  int64_t keyLen;
} SItem;

static void recordPushedEntry(SArray* cachedKey, void* pIter);
static void doRemovePushedEntry(SArray* pCachedKeys, STQ* pTq);

static void freeItem(void* param) {
  SItem* p = (SItem*)param;
  taosMemoryFree(p->pKey);
}

static void doPushDataForEntry(void* pIter, STqExecHandle* pExec, STQ* pTq, int64_t ver, int32_t vgId, char* pData,
                               int32_t dataLen, SArray* pCachedKey) {
  STqPushEntry* pPushEntry = *(STqPushEntry**)pIter;

  SMqDataRsp* pRsp = pPushEntry->pDataRsp;
  if (pRsp->reqOffset.version >= ver) {
    tqDebug("vgId:%d, push entry req version %" PRId64 ", while push version %" PRId64 ", skip", vgId,
            pRsp->reqOffset.version, ver);
    return;
  }

  qTaskInfo_t pTaskInfo = pExec->task;

  // prepare scan mem data
  SPackedData submit = {.msgStr = pData, .msgLen = dataLen, .ver = ver};

  if (qStreamSetScanMemData(pTaskInfo, submit) != 0) {
    return;
  }
  qStreamSetOpen(pTaskInfo);
  // here start to scan submit block to extract the subscribed data
  int32_t totalRows = 0;

  while (1) {
    SSDataBlock* pDataBlock = NULL;
    uint64_t     ts = 0;
    if (qExecTask(pTaskInfo, &pDataBlock, &ts) < 0) {
      tqDebug("vgId:%d, tq exec error since %s", vgId, terrstr());
    }

    if (pDataBlock == NULL) {
      break;
    }

    tqAddBlockDataToRsp(pDataBlock, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
    pRsp->blockNum++;
    totalRows += pDataBlock->info.rows;
  }

  tqDebug("vgId:%d, tq handle push, subkey:%s, block num:%d, rows:%d", vgId, pPushEntry->subKey, pRsp->blockNum,
          totalRows);

  if (pRsp->blockNum > 0) {
    tqOffsetResetToLog(&pRsp->rspOffset, ver);
    tqPushDataRsp(pPushEntry, vgId);
    recordPushedEntry(pCachedKey, pIter);
  }
}

>>>>>>> enh/3.0
int32_t tqPushMsg(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver) {

  if (msgType == TDMT_VND_SUBMIT) {
    tqProcessSubmitReqForSubscribe(pTq);
  }

  int32_t numOfTasks = streamMetaGetNumOfTasks(pTq->pStreamMeta);
  tqDebug("handle submit, restore:%d, size:%d", pTq->pVnode->restored, numOfTasks);

  // push data for stream processing:
  // 1. the vnode has already been restored.
  // 2. the vnode should be the leader.
  // 3. the stream is not suspended yet.
  if (!tsDisableStream && vnodeIsRoleLeader(pTq->pVnode) && pTq->pVnode->restored) {
    if (numOfTasks == 0) {
      return 0;
    }

    if (msgType == TDMT_VND_SUBMIT) {
      tqStartStreamTasks(pTq);
    }

    if (msgType == TDMT_VND_DELETE) {
      tqProcessDelReq(pTq, POINTER_SHIFT(msg, sizeof(SMsgHead)), msgLen - sizeof(SMsgHead), ver);
    }
  }

  return 0;
}

int32_t tqRegisterPushHandle(STQ* pTq, void* handle, SRpcMsg* pMsg) {
  int32_t    vgId = TD_VID(pTq->pVnode);
  STqHandle* pHandle = (STqHandle*)handle;

  if (pHandle->msg == NULL) {
    pHandle->msg = taosMemoryCalloc(1, sizeof(SRpcMsg));
    memcpy(pHandle->msg, pMsg, sizeof(SRpcMsg));
    pHandle->msg->pCont = rpcMallocCont(pMsg->contLen);
  } else {
    tqPushDataRsp(pTq, pHandle);
    void* tmp = pHandle->msg->pCont;
    memcpy(pHandle->msg, pMsg, sizeof(SRpcMsg));
    pHandle->msg->pCont = tmp;
  }

<<<<<<< HEAD
  memcpy(pHandle->msg->pCont, pMsg->pCont, pMsg->contLen);
  pHandle->msg->contLen = pMsg->contLen;
  int32_t ret = taosHashPut(pTq->pPushMgr, pHandle->subKey, strlen(pHandle->subKey), &pHandle, POINTER_BYTES);
  tqDebug("vgId:%d data is over, ret:%d, consumerId:0x%" PRIx64 ", register to pHandle:%p, pCont:%p, len:%d", vgId, ret,
          pHandle->consumerId, pHandle, pHandle->msg->pCont, pHandle->msg->contLen);
=======
  pPushEntry->pHandle = pHandle;
  pPushEntry->info = pRpcMsg->info;
  memcpy(pPushEntry->subKey, pTqHandle->subKey, TSDB_SUBSCRIBE_KEY_LEN);

  if (type == TMQ_MSG_TYPE__TAOSX_RSP) {
    pPushEntry->pDataRsp = taosMemoryCalloc(1, sizeof(STaosxRsp));
    memcpy(pPushEntry->pDataRsp, pDataRsp, sizeof(STaosxRsp));
  } else if (type == TMQ_MSG_TYPE__POLL_RSP) {
    pPushEntry->pDataRsp = taosMemoryCalloc(1, sizeof(SMqDataRsp));
    memcpy(pPushEntry->pDataRsp, pDataRsp, sizeof(SMqDataRsp));
  }

  SMqRspHead* pHead = &pPushEntry->pDataRsp->head;

  pHead->consumerId = consumerId;
  pHead->epoch = pRequest->epoch;
  pHead->mqMsgType = type;

  taosHashPut(pTq->pPushMgr, pTqHandle->subKey, strlen(pTqHandle->subKey), &pPushEntry, sizeof(void*));

  tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s offset:%" PRId64 ", vgId:%d save handle to push mgr, total:%d",
          consumerId, pTqHandle->subKey, pDataRsp->reqOffset.version, vgId, taosHashGetSize(pTq->pPushMgr));
>>>>>>> enh/3.0
  return 0;
}

int32_t tqUnregisterPushHandle(STQ* pTq, void *handle) {
  STqHandle *pHandle = (STqHandle*)handle;
  int32_t    vgId = TD_VID(pTq->pVnode);

  int32_t ret = taosHashRemove(pTq->pPushMgr, pHandle->subKey, strlen(pHandle->subKey));
  tqError("vgId:%d remove pHandle:%p,ret:%d consumer Id:0x%" PRIx64, vgId, pHandle, ret, pHandle->consumerId);

  if(pHandle->msg != NULL) {
    tqPushDataRsp(pTq, pHandle);

<<<<<<< HEAD
    rpcFreeCont(pHandle->msg->pCont);
    taosMemoryFree(pHandle->msg);
    pHandle->msg = NULL;
=======
    if (rspConsumer) {  // rsp the old consumer with empty block.
      tqPushDataRsp(*pEntry, vgId);
    }

    taosHashRemove(pTq->pPushMgr, pKey, keyLen);
>>>>>>> enh/3.0
  }

  return 0;
}
