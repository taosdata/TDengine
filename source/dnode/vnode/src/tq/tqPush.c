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

#if 0
void tqTmrRspFunc(void* param, void* tmrId) {
  STqHandle* pHandle = (STqHandle*)param;
  atomic_store_8(&pHandle->pushHandle.tmrStopped, 1);
}

static int32_t tqLoopExecFromQueue(STQ* pTq, STqHandle* pHandle, SStreamDataSubmit** ppSubmit, SMqDataRsp* pRsp) {
  SStreamDataSubmit* pSubmit = *ppSubmit;
  while (pSubmit != NULL) {
    ASSERT(pSubmit->ver == pHandle->pushHandle.processedVer + 1);
    if (tqLogScanExec(pTq, &pHandle->execHandle, pSubmit->data, pRsp, 0) < 0) {
      /*ASSERT(0);*/
    }
    // update processed
    atomic_store_64(&pHandle->pushHandle.processedVer, pSubmit->ver);
    streamQueueProcessSuccess(&pHandle->pushHandle.inputQ);
    streamDataSubmitRefDec(pSubmit);
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
      streamDataSubmitRefDec(pSubmit);
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
    SStreamDataSubmit* pSubmitClone = streamSubmitRefClone(pSubmit);
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
      // TODO
      ASSERT(0);
    }

    if (rsp.blockNum == 0) {
      taosWUnLockLatch(&pHandle->pushHandle.lock);
      continue;
    }

    ASSERT(taosArrayGetSize(rsp.blockData) == rsp.blockNum);
    ASSERT(taosArrayGetSize(rsp.blockDataLen) == rsp.blockNum);

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

    tqDebug("vgId:%d, offset %" PRId64 " from consumer:%" PRId64 ", (epoch %d) send rsp, block num: %d, reqOffset:%" PRId64 ", rspOffset:%" PRId64,
            TD_VID(pTq->pVnode), fetchOffset, pHandle->pushHandle.consumerId, pHandle->pushHandle.epoch, rsp.blockNum,
            rsp.reqOffset, rsp.rspOffset);

    // TODO destroy
    taosArrayDestroy(rsp.blockData);
    taosArrayDestroy(rsp.blockDataLen);
  }

  return 0;
}
#endif

int tqPushMsg(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver) {
  tqDebug("vgId:%d, tq push msg ver %" PRId64 ", type: %s", pTq->pVnode->config.vgId, ver, TMSG_INFO(msgType));

  if (msgType == TDMT_VND_SUBMIT) {
    // lock push mgr to avoid potential msg lost
    taosWLockLatch(&pTq->pushLock);
    tqDebug("vgId:%d, push handle num %d", pTq->pVnode->config.vgId, taosHashGetSize(pTq->pPushMgr));
    if (taosHashGetSize(pTq->pPushMgr) != 0) {
      SArray* cachedKeys = taosArrayInit(0, sizeof(void*));
      SArray* cachedKeyLens = taosArrayInit(0, sizeof(size_t));
      void*   data = taosMemoryMalloc(msgLen);
      if (data == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tqError("failed to copy data for stream since out of memory");
        taosArrayDestroyP(cachedKeys, (FDelete)taosMemoryFree);
        taosArrayDestroy(cachedKeyLens);
        return -1;
      }
      memcpy(data, msg, msgLen);
      SSubmitReq* pReq = (SSubmitReq*)data;
      pReq->version = ver;

      void* pIter = NULL;
      while (1) {
        pIter = taosHashIterate(pTq->pPushMgr, pIter);
        if (pIter == NULL) break;
        STqPushEntry* pPushEntry = *(STqPushEntry**)pIter;

        STqHandle* pHandle = taosHashGet(pTq->pHandle, pPushEntry->subKey, strlen(pPushEntry->subKey));
        if (pHandle == NULL) {
          tqDebug("vgId:%d, cannot find handle %s", pTq->pVnode->config.vgId, pPushEntry->subKey);
          continue;
        }
        if (pPushEntry->dataRsp.reqOffset.version >= ver) {
          tqDebug("vgId:%d, push entry req version %" PRId64 ", while push version %" PRId64 ", skip",
                  pTq->pVnode->config.vgId, pPushEntry->dataRsp.reqOffset.version, ver);
          continue;
        }
        STqExecHandle* pExec = &pHandle->execHandle;
        qTaskInfo_t    task = pExec->task;

        SMqDataRsp* pRsp = &pPushEntry->dataRsp;

        // prepare scan mem data
        qStreamScanMemData(task, pReq);

        // exec
        while (1) {
          SSDataBlock* pDataBlock = NULL;
          uint64_t     ts = 0;
          if (qExecTask(task, &pDataBlock, &ts) < 0) {
            ASSERT(0);
          }

          if (pDataBlock == NULL) {
            break;
          }

          tqAddBlockDataToRsp(pDataBlock, pRsp, pExec->numOfCols, pTq->pVnode->config.tsdbCfg.precision);
          pRsp->blockNum++;
        }

        tqDebug("vgId:%d, tq handle push, subkey: %s, block num: %d", pTq->pVnode->config.vgId, pPushEntry->subKey,
                pRsp->blockNum);
        if (pRsp->blockNum > 0) {
          // set offset
          tqOffsetResetToLog(&pRsp->rspOffset, ver);
          // remove from hash
          size_t kLen;
          void*  key = taosHashGetKey(pIter, &kLen);
          void*  keyCopy = taosMemoryMalloc(kLen);
          memcpy(keyCopy, key, kLen);

          taosArrayPush(cachedKeys, &keyCopy);
          taosArrayPush(cachedKeyLens, &kLen);

          tqPushDataRsp(pTq, pPushEntry);
        }
      }
      // delete entry
      for (int32_t i = 0; i < taosArrayGetSize(cachedKeys); i++) {
        void*  key = taosArrayGetP(cachedKeys, i);
        size_t kLen = *(size_t*)taosArrayGet(cachedKeyLens, i);
        if (taosHashRemove(pTq->pPushMgr, key, kLen) != 0) {
          ASSERT(0);
        }
      }
      taosArrayDestroyP(cachedKeys, (FDelete)taosMemoryFree);
      taosArrayDestroy(cachedKeyLens);
      taosMemoryFree(data);
    }
    // unlock
    taosWUnLockLatch(&pTq->pushLock);
  }

  if (vnodeIsRoleLeader(pTq->pVnode)) {
    if (taosHashGetSize(pTq->pStreamMeta->pTasks) == 0) return 0;
    if (msgType == TDMT_VND_SUBMIT) {
      void* data = taosMemoryMalloc(msgLen);
      if (data == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        tqError("failed to copy data for stream since out of memory");
        return -1;
      }
      memcpy(data, msg, msgLen);
      SSubmitReq* pReq = (SSubmitReq*)data;
      pReq->version = ver;

      tqProcessSubmitReq(pTq, data, ver);
    }
    if (msgType == TDMT_VND_DELETE) {
      tqProcessDelReq(pTq, POINTER_SHIFT(msg, sizeof(SMsgHead)), msgLen - sizeof(SMsgHead), ver);
    }
  }

  return 0;
}
