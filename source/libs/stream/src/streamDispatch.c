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

#include "streamInt.h"
#include "tmisce.h"
#include "trpc.h"
#include "ttimer.h"

typedef struct SBlockName {
  uint32_t hashValue;
  char     parTbName[TSDB_TABLE_NAME_LEN];
} SBlockName;

static void    doMonitorDispatchData(void* param, void* tmrId);
static int32_t doSendDispatchMsg(SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t vgId, SEpSet* pEpSet);
static int32_t streamAddBlockIntoDispatchMsg(const SSDataBlock* pBlock, SStreamDispatchReq* pReq);
static int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock,
                                       int64_t groupId, int64_t now);
static int32_t tInitStreamDispatchReq(SStreamDispatchReq* pReq, const SStreamTask* pTask, int32_t vgId,
                                      int32_t numOfBlocks, int64_t dstTaskId, int32_t type);
static int32_t getFailedDispatchInfo(SDispatchMsgInfo* pMsgInfo, int64_t now);
static bool    isDispatchRspTimeout(SDispatchEntry* pEntry, int64_t now);
static void    addDispatchEntry(SDispatchMsgInfo* pMsgInfo, int32_t nodeId, int64_t now, bool lock);

void initRpcMsg(SRpcMsg* pMsg, int32_t msgType, void* pCont, int32_t contLen) {
  pMsg->msgType = msgType;
  pMsg->pCont = pCont;
  pMsg->contLen = contLen;
}

static void initDispatchInfo(SDispatchMsgInfo* pInfo, int32_t msgId) {
  pInfo->startTs = taosGetTimestampMs();
  pInfo->rspTs = -1;
  pInfo->msgId = msgId;
}

static void clearDispatchInfo(SDispatchMsgInfo* pInfo) {
  pInfo->startTs = -1;
  pInfo->msgId = -1;
  pInfo->rspTs = -1;
}

static void updateDispatchInfo(SDispatchMsgInfo* pInfo, int64_t recvTs) { pInfo->rspTs = recvTs; }

static int32_t tInitStreamDispatchReq(SStreamDispatchReq* pReq, const SStreamTask* pTask, int32_t vgId,
                                      int32_t numOfBlocks, int64_t dstTaskId, int32_t type) {
  pReq->streamId = pTask->id.streamId;
  pReq->srcVgId = vgId;
  pReq->stage = pTask->pMeta->stage;
  pReq->msgId = pTask->msgInfo.msgId;
  pReq->upstreamTaskId = pTask->id.taskId;
  pReq->upstreamChildId = pTask->info.selfChildId;
  pReq->upstreamNodeId = pTask->info.nodeId;
  pReq->upstreamRelTaskId = pTask->streamTaskId.taskId;
  pReq->blockNum = numOfBlocks;
  pReq->taskId = dstTaskId;
  pReq->type = type;

  pReq->data = taosArrayInit(numOfBlocks, POINTER_BYTES);
  pReq->dataLen = taosArrayInit(numOfBlocks, sizeof(int32_t));
  if (pReq->data == NULL || pReq->dataLen == NULL) {
    taosArrayDestroyP(pReq->data, taosMemoryFree);
    taosArrayDestroy(pReq->dataLen);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

void streamTaskSendRetrieveRsp(SStreamRetrieveReq* pReq, SRpcMsg* pRsp) {
  void* buf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamRetrieveRsp));
  ((SMsgHead*)buf)->vgId = htonl(pReq->srcNodeId);

  SStreamRetrieveRsp* pCont = POINTER_SHIFT(buf, sizeof(SMsgHead));
  pCont->streamId = pReq->streamId;
  pCont->rspToTaskId = pReq->srcTaskId;
  pCont->rspFromTaskId = pReq->dstTaskId;
  pRsp->pCont = buf;
  pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamRetrieveRsp);
  tmsgSendRsp(pRsp);
}

int32_t streamTaskBroadcastRetrieveReq(SStreamTask* pTask, SStreamRetrieveReq* req) {
  int32_t code = 0;
  void*   buf = NULL;
  int32_t sz = taosArrayGetSize(pTask->upstreamInfo.pList);
  for (int32_t i = 0; i < sz; i++) {
    req->reqId = tGenIdPI64();
    SStreamUpstreamEpInfo* pEpInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    req->dstNodeId = pEpInfo->nodeId;
    req->dstTaskId = pEpInfo->taskId;
    int32_t len;

    tEncodeSize(tEncodeStreamRetrieveReq, req, len, code);
    if (code != 0) {
      return code;
    }

    buf = rpcMallocCont(sizeof(SMsgHead) + len);
    if (buf == NULL) {
      return terrno;
    }

    ((SMsgHead*)buf)->vgId = htonl(pEpInfo->nodeId);
    void*    abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, len);
    code = tEncodeStreamRetrieveReq(&encoder, req);
    tEncoderClear(&encoder);

    if (code < 0) {
      stError("s-task:%s failed encode stream retrieve req, code:%s", pTask->id.idStr, tstrerror(code));
      rpcFreeCont(buf);
      return code;
    }

    SRpcMsg rpcMsg = {0};
    initRpcMsg(&rpcMsg, TDMT_STREAM_RETRIEVE, buf, len + sizeof(SMsgHead));

    code = tmsgSendReq(&pEpInfo->epSet, &rpcMsg);
    if (code != 0) {
      rpcFreeCont(buf);
      return code;
    }

    stDebug("s-task:%s (child %d) send retrieve req to task:0x%x (vgId:%d),QID:0x%" PRIx64, pTask->id.idStr,
            pTask->info.selfChildId, pEpInfo->taskId, pEpInfo->nodeId, req->reqId);
  }

  return code;
}

static int32_t buildStreamRetrieveReq(SStreamTask* pTask, const SSDataBlock* pBlock, SStreamRetrieveReq* req) {
  SRetrieveTableRsp* pRetrieve = NULL;

  int32_t len = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock) + PAYLOAD_PREFIX_LEN;

  pRetrieve = taosMemoryCalloc(1, len);
  if (pRetrieve == NULL) return terrno;

  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
  pRetrieve->useconds = 0;
  pRetrieve->precision = TSDB_DEFAULT_PRECISION;
  pRetrieve->compressed = 0;
  pRetrieve->completed = 1;
  pRetrieve->streamBlockType = pBlock->info.type;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);
  pRetrieve->numOfCols = htonl(numOfCols);
  pRetrieve->skey = htobe64(pBlock->info.window.skey);
  pRetrieve->ekey = htobe64(pBlock->info.window.ekey);
  pRetrieve->version = htobe64(pBlock->info.version);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data + PAYLOAD_PREFIX_LEN, numOfCols);
  if (actualLen < 0) {
    taosMemoryFree(pRetrieve);
    return terrno;
  }
  SET_PAYLOAD_LEN(pRetrieve->data, actualLen, actualLen);

  int32_t payloadLen = actualLen + PAYLOAD_PREFIX_LEN;
  pRetrieve->payloadLen = htonl(payloadLen);
  pRetrieve->compLen = htonl(payloadLen);
  pRetrieve->compressed = 0;

  req->streamId = pTask->id.streamId;
  req->srcNodeId = pTask->info.nodeId;
  req->srcTaskId = pTask->id.taskId;
  req->pRetrieve = pRetrieve;
  req->retrieveLen = len;
  return 0;
}

int32_t streamBroadcastToUpTasks(SStreamTask* pTask, const SSDataBlock* pBlock) {
  SStreamRetrieveReq req;
  int32_t            code = buildStreamRetrieveReq(pTask, pBlock, &req);
  if (code != 0) {
    return code;
  }

  code = streamTaskBroadcastRetrieveReq(pTask, &req);
  taosMemoryFree(req.pRetrieve);

  return code;
}

// no need to do anything if failed
int32_t streamSendCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = 0;
  SRpcMsg msg = {0};

  int32_t tlen;
  tEncodeSize(tEncodeStreamTaskCheckReq, pReq, tlen, code);
  if (code < 0) {
    return code;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = htonl(nodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamTaskCheckReq(&encoder, pReq)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    return code;
  }

  tEncoderClear(&encoder);
  initRpcMsg(&msg, TDMT_VND_STREAM_TASK_CHECK, buf, tlen + sizeof(SMsgHead));
  stDebug("s-task:%s (level:%d) send check msg to s-task:0x%" PRIx64 ":0x%x (vgId:%d)", pTask->id.idStr,
          pTask->info.taskLevel, pReq->streamId, pReq->downstreamTaskId, nodeId);

  return tmsgSendReq(pEpSet, &msg);
}

void destroyDispatchMsg(SStreamDispatchReq* pReq, int32_t numOfVgroups) {
  for (int32_t i = 0; i < numOfVgroups; i++) {
    taosArrayDestroyP(pReq[i].data, taosMemoryFree);
    taosArrayDestroy(pReq[i].dataLen);
  }

  taosMemoryFree(pReq);
}

void clearBufferedDispatchMsg(SStreamTask* pTask) {
  SDispatchMsgInfo* pMsgInfo = &pTask->msgInfo;
  if (pMsgInfo->pData != NULL) {
    destroyDispatchMsg(pMsgInfo->pData, streamTaskGetNumOfDownstream(pTask));
  }

  streamMutexLock(&pMsgInfo->lock);

  pMsgInfo->checkpointId = -1;
  pMsgInfo->transId = -1;
  pMsgInfo->pData = NULL;
  pMsgInfo->dispatchMsgType = 0;

  clearDispatchInfo(pMsgInfo);

  taosArrayClear(pTask->msgInfo.pSendInfo);
  streamMutexUnlock(&pMsgInfo->lock);
}

static SStreamDispatchReq* createDispatchDataReq(SStreamTask* pTask, const SStreamDataBlock* pData) {
  int32_t code = 0;
  int32_t type = pTask->outputInfo.type;
  int32_t num = streamTaskGetNumOfDownstream(pTask);

  if (type != TASK_OUTPUT__SHUFFLE_DISPATCH && type != TASK_OUTPUT__FIXED_DISPATCH) {
    terrno = TSDB_CODE_INVALID_PARA;
    stError("s-task:%s invalid dispatch type:%d not dispatch data", pTask->id.idStr, type);
    return NULL;
  }

  SStreamDispatchReq* pReqs = taosMemoryCalloc(num, sizeof(SStreamDispatchReq));
  if (pReqs == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgroups = taosArrayGetSize(vgInfo);

    for (int32_t i = 0; i < numOfVgroups; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      if (pVgInfo == NULL) {
        continue;
      }

      code = tInitStreamDispatchReq(&pReqs[i], pTask, pData->srcVgId, 0, pVgInfo->taskId, pData->type);
      if (code != TSDB_CODE_SUCCESS) {
        destroyDispatchMsg(pReqs, numOfVgroups);
        terrno = code;
        return NULL;
      }
    }
  } else {  // shuffle dispatch
    int32_t numOfBlocks = taosArrayGetSize(pData->blocks);
    int32_t downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;

    code = tInitStreamDispatchReq(pReqs, pTask, pData->srcVgId, numOfBlocks, downstreamTaskId, pData->type);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pReqs);
      terrno = code;
      return NULL;
    }
  }

  return pReqs;
}

static int32_t doBuildDispatchMsg(SStreamTask* pTask, const SStreamDataBlock* pData) {
  int32_t code = 0;
  int64_t now = taosGetTimestampMs();
  int32_t numOfBlocks = taosArrayGetSize(pData->blocks);

  if (!(numOfBlocks != 0 && pTask->msgInfo.pData == NULL)) {
    stError("s-task:%s dispatch block number:%d, exist not rsp dispatch msg:%p, abort build new dispatch msg",
            pTask->id.idStr, numOfBlocks, pTask->msgInfo.pData);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  pTask->msgInfo.dispatchMsgType = pData->type;

  if (pData->type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
    SSDataBlock* p = taosArrayGet(pData->blocks, 0);
    if (p == NULL) {
      return terrno;
    }

    pTask->msgInfo.checkpointId = p->info.version;
    pTask->msgInfo.transId = p->info.window.ekey;
  }

  SStreamDispatchReq* pReqs = createDispatchDataReq(pTask, pData);
  if (pReqs == NULL) {
    stError("s-task:%s failed to create dispatch req", pTask->id.idStr);
    return terrno;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);
      if (pDataBlock == NULL) {
        destroyDispatchMsg(pReqs, 1);
        return terrno;
      }

      code = streamAddBlockIntoDispatchMsg(pDataBlock, pReqs);
      if (code != TSDB_CODE_SUCCESS) {
        destroyDispatchMsg(pReqs, 1);
        return code;
      }
    }

    addDispatchEntry(&pTask->msgInfo, pTask->outputInfo.fixedDispatcher.nodeId, now, true);
    pTask->msgInfo.pData = pReqs;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgroups = taosArrayGetSize(vgInfo);

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);
      if (pDataBlock == NULL) {
        destroyDispatchMsg(pReqs, numOfVgroups);
        return terrno;
      }

      // TODO: do not use broadcast
      if (pDataBlock->info.type == STREAM_DELETE_RESULT || pDataBlock->info.type == STREAM_CHECKPOINT ||
          pDataBlock->info.type == STREAM_TRANS_STATE) {
        for (int32_t j = 0; j < numOfVgroups; j++) {
          code = streamAddBlockIntoDispatchMsg(pDataBlock, &pReqs[j]);
          if (code != 0) {
            destroyDispatchMsg(pReqs, numOfVgroups);
            return code;
          }

          // it's a new vnode to receive dispatch msg, so add one
          if (pReqs[j].blockNum == 0) {
            SVgroupInfo* pDstVgroupInfo = taosArrayGet(vgInfo, j);
            if (pDstVgroupInfo == NULL) {
              destroyDispatchMsg(pReqs, numOfVgroups);
              return terrno;
            }
            addDispatchEntry(&pTask->msgInfo, pDstVgroupInfo->vgId, now, true);
          }

          pReqs[j].blockNum++;
        }

        continue;
      }

      code = streamSearchAndAddBlock(pTask, pReqs, pDataBlock, pDataBlock->info.id.groupId, now);
      if (code != 0) {
        destroyDispatchMsg(pReqs, numOfVgroups);
        return code;
      }
    }

    pTask->msgInfo.pData = pReqs;
  }

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    stDebug("s-task:%s build dispatch msg success, msgId:%d, stage:%" PRId64 " %p", pTask->id.idStr,
            pTask->execInfo.dispatch, pTask->pMeta->stage, pTask->msgInfo.pData);
  } else {
    int32_t numOfBranches = taosArrayGetSize(pTask->msgInfo.pSendInfo);
    stDebug("s-task:%s build dispatch msg success, msgId:%d, stage:%" PRId64 " dstVgNum:%d %p", pTask->id.idStr,
            pTask->execInfo.dispatch, pTask->pMeta->stage, numOfBranches, pTask->msgInfo.pData);
  }

  return code;
}

static int32_t sendDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pDispatchMsg) {
  int32_t     code = 0;
  const char* id = pTask->id.idStr;
  int32_t     msgId = pTask->msgInfo.msgId;

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    int32_t vgId = pTask->outputInfo.fixedDispatcher.nodeId;
    SEpSet* pEpSet = &pTask->outputInfo.fixedDispatcher.epSet;
    int32_t downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;

    stDebug("s-task:%s (child taskId:%d) fix-dispatch %d block(s) to s-task:0x%x (vgId:%d), msgId:%d", id,
            pTask->info.selfChildId, 1, downstreamTaskId, vgId, msgId);

    code = doSendDispatchMsg(pTask, pDispatchMsg, vgId, pEpSet);
  } else {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgroups = taosArrayGetSize(vgInfo);
    int32_t numOfBranches = taosArrayGetSize(pTask->msgInfo.pSendInfo);

    stDebug("s-task:%s (child taskId:%d) start to shuffle-dispatch blocks to %d/%d vgroup(s), msgId:%d", id,
            pTask->info.selfChildId, numOfBranches, numOfVgroups, msgId);

    int32_t numOfSend = 0;
    for (int32_t i = 0; i < numOfVgroups; i++) {
      if (pDispatchMsg[i].blockNum > 0) {
        SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
        if (pVgInfo == NULL) {
          code = terrno;
          break;
        }

        stDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to vgId:%d", id, pTask->info.selfChildId,
                pDispatchMsg[i].blockNum, pVgInfo->vgId);

        code = doSendDispatchMsg(pTask, &pDispatchMsg[i], pVgInfo->vgId, &pVgInfo->epSet);
        if (code < 0) {
          break;
        }

        // no need to try remain, all already send.
        if (++numOfSend == numOfBranches) {
          break;
        }
      }
    }

    stDebug("s-task:%s complete shuffle-dispatch blocks to all %d vnodes, msgId:%d", id, numOfVgroups, msgId);
  }

  return code;
}

static void setNotInDispatchMonitor(SDispatchMsgInfo* pMsgInfo) {
  streamMutexLock(&pMsgInfo->lock);
  pMsgInfo->inMonitor = 0;
  streamMutexUnlock(&pMsgInfo->lock);
}

static void setResendInfo(SDispatchEntry* pEntry, int64_t now) {
  pEntry->sendTs = now;
  pEntry->rspTs = -1;
  pEntry->retryCount += 1;
  pEntry->status = TSDB_CODE_SUCCESS;
}

static void addDispatchEntry(SDispatchMsgInfo* pMsgInfo, int32_t nodeId, int64_t now, bool lock) {
  SDispatchEntry entry = {.nodeId = nodeId, .rspTs = -1, .status = 0, .sendTs = now};

  if (lock) {
    streamMutexLock(&pMsgInfo->lock);
  }

  void* p = taosArrayPush(pMsgInfo->pSendInfo, &entry);
  if (p == NULL) {
    stError("failed to add dispatch info");
  }

  if (lock) {
    streamMutexUnlock(&pMsgInfo->lock);
  }
}

static void doSendFailedDispatch(SStreamTask* pTask, SDispatchEntry* pEntry, int64_t now, const char* pMsg) {
  SStreamDispatchReq* pReq = pTask->msgInfo.pData;

  int32_t msgId = pTask->msgInfo.msgId;
  SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
  int32_t numOfVgroups = taosArrayGetSize(vgInfo);

  setResendInfo(pEntry, now);
  for (int32_t j = 0; j < numOfVgroups; ++j) {
    SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, j);
    if (pVgInfo == NULL) {
      continue;
    }

    if (pVgInfo->vgId == pEntry->nodeId) {
      int32_t code = doSendDispatchMsg(pTask, &pReq[j], pVgInfo->vgId, &pVgInfo->epSet);
      stDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to vgId:%d for %s, msgId:%d, code:%s",
              pTask->id.idStr, pTask->info.selfChildId, pReq[j].blockNum, pVgInfo->vgId, pMsg, msgId, tstrerror(code));
      break;
    }
  }
}

static void doMonitorDispatchData(void* param, void* tmrId) {
  SStreamTask*      pTask = param;
  const char*       id = pTask->id.idStr;
  int32_t           vgId = pTask->pMeta->vgId;
  SDispatchMsgInfo* pMsgInfo = &pTask->msgInfo;
  int32_t           msgId = pMsgInfo->msgId;
  int32_t           code = 0;
  int64_t           now = taosGetTimestampMs();
  bool              inDispatch = true;

  stDebug("s-task:%s start monitor dispatch data", id);

  if (streamTaskShouldStop(pTask)) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s should stop, abort from timer, ref:%d", pTask->id.idStr, ref);
    setNotInDispatchMonitor(pMsgInfo);
    return;
  }

  // slave task not handle the dispatch, downstream not ready will break the monitor timer
  // follower not handle the dispatch rsp
  if ((pTask->pMeta->role == NODE_ROLE_FOLLOWER) || (pTask->status.downstreamReady != 1)) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stError("s-task:%s vgId:%d follower or downstream not ready, jump out of monitor tmr, ref:%d", id, vgId, ref);
    setNotInDispatchMonitor(pMsgInfo);
    return;
  }

  streamMutexLock(&pMsgInfo->lock);
  if (pTask->outputq.status == TASK_OUTPUT_STATUS__NORMAL) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s not in dispatch procedure, abort from timer, ref:%d", pTask->id.idStr, ref);

    pMsgInfo->inMonitor = 0;
    inDispatch = false;
  }
  streamMutexUnlock(&pMsgInfo->lock);

  if (!inDispatch) {
    return;
  }

  int32_t numOfFailed = getFailedDispatchInfo(pMsgInfo, now);
  if (numOfFailed == 0) {
    stDebug("s-task:%s no error occurs, check again in %dms", id, DISPATCH_RETRY_INTERVAL_MS);
    streamStartMonitorDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS);
    return;
  }

  {
    SStreamDispatchReq* pReq = pTask->msgInfo.pData;

    if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      stDebug("s-task:%s (child taskId:%d) retry shuffle-dispatch to down streams, msgId:%d", id,
              pTask->info.selfChildId, msgId);

      int32_t numOfRetry = 0;
      for (int32_t i = 0; i < taosArrayGetSize(pTask->msgInfo.pSendInfo); ++i) {
        SDispatchEntry* pEntry = taosArrayGet(pTask->msgInfo.pSendInfo, i);
        if (pEntry == NULL) {
          continue;
        }

        if (pEntry->status == TSDB_CODE_SUCCESS && pEntry->rspTs > 0) {
          continue;
        }

        // downstream not rsp yet beyond threshold that is 10s
        if (isDispatchRspTimeout(pEntry, now)) {  // not respond yet beyonds 30s, re-send data
          doSendFailedDispatch(pTask, pEntry, now, "timeout");
          numOfRetry += 1;
          continue;
        }

        // downstream inputQ is closed
        if (pEntry->status == TASK_INPUT_STATUS__BLOCKED) {
          doSendFailedDispatch(pTask, pEntry, now, "downstream inputQ blocked");
          numOfRetry += 1;
          continue;
        }

        // handle other errors
        if (pEntry->status != TSDB_CODE_SUCCESS) {
          doSendFailedDispatch(pTask, pEntry, now, "downstream error");
          numOfRetry += 1;
        }
      }

      stDebug("s-task:%s complete retry shuffle-dispatch blocks to all %d vnodes, msgId:%d", pTask->id.idStr,
              numOfRetry, msgId);
    } else {
      int32_t dstVgId = pTask->outputInfo.fixedDispatcher.nodeId;
      SEpSet* pEpSet = &pTask->outputInfo.fixedDispatcher.epSet;
      int32_t downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;

      int32_t         s = taosArrayGetSize(pTask->msgInfo.pSendInfo);
      SDispatchEntry* pEntry = taosArrayGet(pTask->msgInfo.pSendInfo, 0);
      if (pEntry != NULL) {
        setResendInfo(pEntry, now);
        code = doSendDispatchMsg(pTask, pReq, dstVgId, pEpSet);

        stDebug("s-task:%s (child taskId:%d) fix-dispatch %d block(s) to s-task:0x%x (vgId:%d), msgId:%d, code:%s", id,
                pTask->info.selfChildId, 1, downstreamTaskId, dstVgId, msgId, tstrerror(code));
      } else {
        stError("s-task:%s invalid index 0, size:%d", id, s);
      }
    }
  }

  if (streamTaskShouldStop(pTask)) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s should stop, abort from timer, ref:%d", pTask->id.idStr, ref);
    setNotInDispatchMonitor(pMsgInfo);
  } else {
    streamStartMonitorDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS);
  }
}

void streamStartMonitorDispatchData(SStreamTask* pTask, int64_t waitDuration) {
  int32_t vgId = pTask->pMeta->vgId;
  streamTmrStart(doMonitorDispatchData, waitDuration, pTask, streamTimer, &pTask->msgInfo.pRetryTmr, vgId,
                 "dispatch-monitor");
}

static int32_t doAddDispatchBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock,
                                  SArray* vgInfo, uint32_t hashValue, int64_t now, bool* pFound) {
  size_t  numOfVgroups = taosArrayGetSize(vgInfo);
  int32_t code = 0;

  *pFound = false;

  for (int32_t j = 0; j < numOfVgroups; j++) {
    SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, j);
    if (pVgInfo == NULL) {
      continue;
    }

    if (hashValue >= pVgInfo->hashBegin && hashValue <= pVgInfo->hashEnd) {
      if ((code = streamAddBlockIntoDispatchMsg(pDataBlock, &pReqs[j])) < 0) {
        stError("s-task:%s failed to add dispatch block, code:%s", pTask->id.idStr, tstrerror(terrno));
        return code;
      }

      if (pReqs[j].blockNum == 0) {
        SVgroupInfo* pDstVgroupInfo = taosArrayGet(vgInfo, j);
        if (pDstVgroupInfo != NULL) {
          addDispatchEntry(&pTask->msgInfo, pDstVgroupInfo->vgId, now, false);
        }
      }

      pReqs[j].blockNum++;
      *pFound = true;
      break;
    }
  }

  return code;
}

int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock, int64_t groupId,
                                int64_t now) {
  bool     found = false;
  uint32_t hashValue = 0;
  int32_t  code = 0;
  SArray*  vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;

  if (pTask->pNameMap == NULL) {
    pTask->pNameMap = tSimpleHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (pTask->pNameMap == NULL) {
      stError("s-task:%s failed to init the name map, code:%s", pTask->id.idStr, tstrerror(terrno));
      return terrno;
    }
  }

  void* pVal = tSimpleHashGet(pTask->pNameMap, &groupId, sizeof(int64_t));
  if (pVal) {
    SBlockName* pBln = (SBlockName*)pVal;
    hashValue = pBln->hashValue;
    if (!pDataBlock->info.parTbName[0]) {
      memset(pDataBlock->info.parTbName, 0, TSDB_TABLE_NAME_LEN);
      memcpy(pDataBlock->info.parTbName, pBln->parTbName, strlen(pBln->parTbName));
    }
  } else {
    char ctbName[TSDB_TABLE_FNAME_LEN] = {0};
    if (pDataBlock->info.parTbName[0]) {
      if (pTask->subtableWithoutMd5 != 1 && !isAutoTableName(pDataBlock->info.parTbName) &&
          !alreadyAddGroupId(pDataBlock->info.parTbName, groupId) && groupId != 0) {
        if (pTask->ver == SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
          code = buildCtbNameAddGroupId(NULL, pDataBlock->info.parTbName, groupId, sizeof(pDataBlock->info.parTbName));
        } else if (pTask->ver > SSTREAM_TASK_SUBTABLE_CHANGED_VER) {
          code = buildCtbNameAddGroupId(pTask->outputInfo.shuffleDispatcher.stbFullName, pDataBlock->info.parTbName,
                                        groupId, sizeof(pDataBlock->info.parTbName));
        }
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
    } else {
      code = buildCtbNameByGroupIdImpl(pTask->outputInfo.shuffleDispatcher.stbFullName, groupId,
                                       pDataBlock->info.parTbName);
      if (code) {
        stError("s-task:%s failed to build child table name for group:%" PRId64 ", code:%s", pTask->id.idStr, groupId,
                tstrerror(code));
      }
    }

    snprintf(ctbName, TSDB_TABLE_NAME_LEN, "%s.%s", pTask->outputInfo.shuffleDispatcher.dbInfo.db,
             pDataBlock->info.parTbName);
    /*uint32_t hashValue = MurmurHash3_32(ctbName, strlen(ctbName));*/
    SUseDbRsp* pDbInfo = &pTask->outputInfo.shuffleDispatcher.dbInfo;
    hashValue =
        taosGetTbHashVal(ctbName, strlen(ctbName), pDbInfo->hashMethod, pDbInfo->hashPrefix, pDbInfo->hashSuffix);
    SBlockName bln = {0};
    bln.hashValue = hashValue;
    memcpy(bln.parTbName, pDataBlock->info.parTbName, strlen(pDataBlock->info.parTbName));

    // failed to put into name buffer, no need to do anything
    if (tSimpleHashGetSize(pTask->pNameMap) < MAX_BLOCK_NAME_NUM) {  // allow error, and do nothing
      code = tSimpleHashPut(pTask->pNameMap, &groupId, sizeof(int64_t), &bln, sizeof(SBlockName));
    }
  }

  streamMutexLock(&pTask->msgInfo.lock);
  code = doAddDispatchBlock(pTask, pReqs, pDataBlock, vgInfo, hashValue, now, &found);
  streamMutexUnlock(&pTask->msgInfo.lock);

  if (code) {
    return code;
  }

  if (!found) {
    stError("s-task:%s not found req hash value:%u, failed to add dispatch block", pTask->id.idStr, hashValue);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  } else {
    return 0;
  }
}

int32_t streamDispatchStreamBlock(SStreamTask* pTask) {
  const char*            id = pTask->id.idStr;
  int32_t                code = 0;
  SStreamDataBlock*      pBlock = NULL;
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  int32_t                old = 0;

  int32_t numOfElems = streamQueueGetNumOfItems(pTask->outputq.queue);
  if (numOfElems > 0) {
    double  size = SIZE_IN_MiB(taosQueueMemorySize(pTask->outputq.queue->pQueue));
    int32_t numOfUnAccessed = streamQueueGetNumOfUnAccessedItems(pTask->outputq.queue);
    stDebug("s-task:%s start to dispatch intermediate block to downstream, elem in outputQ:%d/%d, size:%.2fMiB", id,
            numOfUnAccessed, numOfElems, size);
  }

  // to make sure only one dispatch is running
  old = atomic_val_compare_exchange_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL, TASK_OUTPUT_STATUS__WAIT);
  if (old != TASK_OUTPUT_STATUS__NORMAL) {
    stDebug("s-task:%s wait for dispatch rsp, not dispatch now, output status:%d", id, old);
    return 0;
  }

  if (pInfo->dispatchTrigger) {
    if ((pInfo->activeId != 0) && (pInfo->failedId < pInfo->activeId)) {
      stDebug("s-task:%s already send checkpoint-trigger, no longer dispatch any other data", id);
      atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);
      return 0;
    } else {
      stDebug("s-task:%s dispatch trigger set, and ignore since current active checkpointId:%" PRId64 " failed", id,
              pInfo->activeId);
    }
  }

  if (pTask->msgInfo.pData != NULL) {
    stFatal("s-task:%s not rsp data:%p exist, should not dispatch msg now", id, pTask->msgInfo.pData);
  } else {
    stDebug("s-task:%s start to dispatch msg, set output status:%d", id, pTask->outputq.status);
  }

  while (1) {
    streamQueueNextItem(pTask->outputq.queue, (SStreamQueueItem**)&pBlock);
    if (pBlock == NULL) {
      atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);
      stDebug("s-task:%s not dispatch since no elems in outputQ, output status:%d", id, pTask->outputq.status);
      return 0;
    }

    int32_t type = pBlock->type;
    if (!(type == STREAM_INPUT__DATA_BLOCK || type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
          type == STREAM_INPUT__TRANS_STATE)) {
      stError("s-task:%s invalid dispatch block type:%d", id, type);
      return TSDB_CODE_INTERNAL_ERROR;
    }

    pTask->execInfo.dispatch += 1;

    streamMutexLock(&pTask->msgInfo.lock);
    initDispatchInfo(&pTask->msgInfo, pTask->execInfo.dispatch);
    streamMutexUnlock(&pTask->msgInfo.lock);

    code = doBuildDispatchMsg(pTask, pBlock);
    if (code == 0) {
      destroyStreamDataBlock(pBlock);
    } else {  // todo handle build dispatch msg failed
    }

    if (type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
      // outputQ should be empty here, otherwise, set the checkpoint failed due to the retrieve req happens
      if (streamQueueGetNumOfUnAccessedItems(pTask->outputq.queue) > 0) {
        stError(
            "s-task:%s items are still in outputQ due to downstream retrieve, failed to init and discard "
            "checkpoint-trigger dispatch",
            pTask->id.idStr);
        streamTaskSetCheckpointFailed(pTask);
        clearBufferedDispatchMsg(pTask);
        continue;
      }

      code = streamTaskInitTriggerDispatchInfo(pTask);
      if (code != TSDB_CODE_SUCCESS) {  // todo handle error
      }
    }

    break;
  }

  code = sendDispatchMsg(pTask, pTask->msgInfo.pData);

  // todo: secure the timerActive and start timer in after lock pTask->lock
  streamMutexLock(&pTask->lock);
  bool shouldStop = streamTaskShouldStop(pTask);
  streamMutexUnlock(&pTask->lock);

  if (shouldStop) {
    stDebug("s-task:%s in stop/dropping status, not start dispatch monitor tmr", id);
  } else {
    streamMutexLock(&pTask->msgInfo.lock);
    if (pTask->msgInfo.inMonitor == 0) {
      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s start dispatch monitor tmr in %dms, ref:%d, dispatch code:%s", id, DISPATCH_RETRY_INTERVAL_MS,
              ref, tstrerror(code));
      streamStartMonitorDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS);
      pTask->msgInfo.inMonitor = 1;
    } else {
      stDebug("s-task:%s already in dispatch monitor tmr", id);
    }

    streamMutexUnlock(&pTask->msgInfo.lock);
  }

  // this block can not be deleted until it has been sent to downstream task successfully.
  return TSDB_CODE_SUCCESS;
}

int32_t initCheckpointReadyMsg(SStreamTask* pTask, int32_t upstreamNodeId, int32_t upstreamTaskId, int32_t childId,
                               int64_t checkpointId, SRpcMsg* pMsg) {
  int32_t code = 0;
  int32_t tlen = 0;
  void*   buf = NULL;

  SStreamCheckpointReadyMsg req = {0};
  req.downstreamNodeId = pTask->pMeta->vgId;
  req.downstreamTaskId = pTask->id.taskId;
  req.streamId = pTask->id.streamId;
  req.checkpointId = checkpointId;
  req.childId = childId;
  req.upstreamNodeId = upstreamNodeId;
  req.upstreamTaskId = upstreamTaskId;

  tEncodeSize(tEncodeStreamCheckpointReadyMsg, &req, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    return terrno;
  }

  ((SMsgHead*)buf)->vgId = htonl(req.upstreamNodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamCheckpointReadyMsg(&encoder, &req)) < 0) {
    rpcFreeCont(buf);
    tEncoderClear(&encoder);
    return code;
  }
  tEncoderClear(&encoder);

  initRpcMsg(pMsg, TDMT_STREAM_TASK_CHECKPOINT_READY, buf, tlen + sizeof(SMsgHead));
  return TSDB_CODE_SUCCESS;
}

static int32_t doTaskChkptStatusCheck(SStreamTask* pTask, int32_t num) {
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo*        pTmrInfo = &pActiveInfo->chkptReadyMsgTmr;
  const char*            id = pTask->id.idStr;
  int32_t                vgId = pTask->pMeta->vgId;

  if (pTmrInfo->launchChkptId != pActiveInfo->activeId) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d ready-msg send tmr launched by previous checkpoint procedure, checkpointId:%" PRId64
           ", quit, ref:%d",
           id, vgId, pTmrInfo->launchChkptId, ref);
    return -1;
  }

  // active checkpoint info is cleared for now
  if ((pActiveInfo->activeId == 0) || (pActiveInfo->transId == 0) || (num == 0) || (pTask->chkInfo.startTs == 0)) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d active checkpoint may be cleared, quit from readyMsg send tmr, ref:%d", id, vgId, ref);
    return -1;
  }

  if (taosArrayGetSize(pTask->upstreamInfo.pList) != num) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stWarn("s-task:%s vgId:%d upstream number:%d not equals sent readyMsg:%d, quit from readyMsg send tmr, ref:%d", id,
           vgId, (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList), num, ref);
    return -1;
  }

  return 0;
}

static int32_t doFindNotConfirmUpstream(SArray** ppNotRspList, SArray* pList, int32_t num, int32_t vgId, int32_t level,
                                        const char* id) {
  SArray* pTmp = taosArrayInit(4, sizeof(int32_t));
  if (pTmp == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < num; ++i) {
    STaskCheckpointReadyInfo* pInfo = taosArrayGet(pList, i);
    if (pInfo == NULL) {
      continue;
    }

    if (pInfo->sendCompleted == 1) {
      continue;
    }

    void* p = taosArrayPush(pTmp, &pInfo->upstreamTaskId);
    if (p == NULL) {
      stError("s-task:%s vgId:%d failed to record not rsp task, code: out of memory", id, vgId);
      return terrno;
    } else {
      stDebug("s-task:%s vgId:%d level:%d checkpoint-ready rsp from upstream:0x%x not confirmed yet", id, vgId, level,
              pInfo->upstreamTaskId);
    }
  }

  *ppNotRspList = pTmp;
  return 0;
}

static void doSendChkptReadyMsg(SStreamTask* pTask, SArray* pNotRspList, int64_t checkpointId, SArray* pReadyList) {
  int32_t     code = 0;
  int32_t     num = taosArrayGetSize(pReadyList);
  const char* id = pTask->id.idStr;

  for (int32_t i = 0; i < taosArrayGetSize(pNotRspList); ++i) {
    int32_t* pTaskId = taosArrayGet(pNotRspList, i);
    if (pTaskId == NULL) {
      continue;
    }

    for (int32_t j = 0; j < num; ++j) {
      STaskCheckpointReadyInfo* pReadyInfo = taosArrayGet(pReadyList, j);
      if (pReadyInfo == NULL) {
        continue;
      }

      if (*pTaskId == pReadyInfo->upstreamTaskId) {  // send msg again

        SRpcMsg msg = {0};
        code = initCheckpointReadyMsg(pTask, pReadyInfo->upstreamNodeId, pReadyInfo->upstreamTaskId,
                                      pReadyInfo->childId, checkpointId, &msg);
        if (code == TSDB_CODE_SUCCESS) {
          code = tmsgSendReq(&pReadyInfo->upstreamNodeEpset, &msg);
          if (code == TSDB_CODE_SUCCESS) {
            stDebug("s-task:%s level:%d checkpoint-ready msg sent to upstream:0x%x again", id, pTask->info.taskLevel,
                    pReadyInfo->upstreamTaskId);
          } else {
            stError("s-task:%s failed to send checkpoint-ready msg, try nex time in 10s", id);
          }
        } else {
          stError("s-task:%s failed to prepare the checkpoint-ready msg, try nex time in 10s", id);
        }
      }
    }
  }
}

static int32_t chkptReadyMsgSendHelper(SStreamTask* pTask, SArray* pNotRspList) {
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo*        pTmrInfo = &pActiveInfo->chkptReadyMsgTmr;
  SArray*                pList = pActiveInfo->pReadyMsgList;
  int32_t                num = taosArrayGetSize(pList);
  int32_t                vgId = pTask->pMeta->vgId;
  int32_t                checkpointId = pActiveInfo->activeId;
  const char*            id = pTask->id.idStr;
  int32_t                notRsp = 0;

  int32_t code = doTaskChkptStatusCheck(pTask, num);
  if (code) {
    return code;
  }

  code = doFindNotConfirmUpstream(&pNotRspList, pList, num, vgId, pTask->info.taskLevel, id);
  if (code) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stError("s-task:%s failed to find not rsp checkpoint-ready downstream, code:%s, out of tmr, ref:%d", id,
            tstrerror(code), ref);
    return code;
  }

  notRsp = taosArrayGetSize(pNotRspList);
  if (notRsp == 0) {
    streamClearChkptReadyMsg(pActiveInfo);
  } else {
    doSendChkptReadyMsg(pTask, pNotRspList, checkpointId, pList);
  }

  return code;
}

static void chkptReadyMsgSendMonitorFn(void* param, void* tmrId) {
  SStreamTask*           pTask = param;
  int32_t                vgId = pTask->pMeta->vgId;
  const char*            id = pTask->id.idStr;
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  SStreamTmrInfo*        pTmrInfo = &pActiveInfo->chkptReadyMsgTmr;
  SArray*                pNotRspList = NULL;
  int32_t                code = 0;
  int32_t                notRsp = 0;

  // check the status every 100ms
  if (streamTaskShouldStop(pTask)) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d status:stop, quit from monitor checkpoint-trigger, ref:%d", id, vgId, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  if (++pTmrInfo->activeCounter < 50) {
    streamTmrStart(chkptReadyMsgSendMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle, vgId,
                   "chkpt-ready-monitor");
    return;
  }

  // reset tmr
  pTmrInfo->activeCounter = 0;
  stDebug("s-task:%s in sending checkpoint-ready msg monitor tmr", id);

  streamMutexLock(&pTask->lock);
  SStreamTaskState state = streamTaskGetStatus(pTask);
  streamMutexUnlock(&pTask->lock);

  // 1. check status in the first place
  if (state.state != TASK_STATUS__CK) {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug("s-task:%s vgId:%d status:%s not in checkpoint, quit from monitor checkpoint-ready, ref:%d", id, vgId,
            state.name, ref);
    streamMetaReleaseTask(pTask->pMeta, pTask);
    return;
  }

  streamMutexLock(&pActiveInfo->lock);
  code = chkptReadyMsgSendHelper(pTask, pNotRspList);
  streamMutexUnlock(&pActiveInfo->lock);

  if (code != TSDB_CODE_SUCCESS) {
    streamMetaReleaseTask(pTask->pMeta, pTask);
    taosArrayDestroy(pNotRspList);
    return;
  }

  notRsp = taosArrayGetSize(pNotRspList);
  if (notRsp > 0) {  // send checkpoint-ready msg again
    stDebug("s-task:%s start to monitor checkpoint-ready msg recv status in 10s", id);
    streamTmrStart(chkptReadyMsgSendMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle, vgId,
                   "chkpt-ready-monitor");
  } else {
    int32_t ref = streamCleanBeforeQuitTmr(pTmrInfo, pTask);
    stDebug(
        "s-task:%s vgId:%d checkpoint-ready msg confirmed by all upstream task(s), clear checkpoint-ready msg and quit "
        "from timer, ref:%d",
        id, vgId, ref);
    // release should be the last execution, since pTask may be destroy after it immidiately.
    streamMetaReleaseTask(pTask->pMeta, pTask);
  }

  taosArrayDestroy(pNotRspList);
}

// this function is usually invoked by sink/agg task
int32_t streamTaskSendCheckpointReadyMsg(SStreamTask* pTask) {
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;

  const char* id = pTask->id.idStr;
  int32_t     vgId = pTask->pMeta->vgId;
  SArray*     pList = pActiveInfo->pReadyMsgList;

  streamMutexLock(&pActiveInfo->lock);

  int32_t num = taosArrayGetSize(pList);
  if (taosArrayGetSize(pTask->upstreamInfo.pList) != num) {
    stError("s-task:%s invalid number of sent readyMsg:%d to upstream:%d", id, num,
            (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList));
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  for (int32_t i = 0; i < num; ++i) {
    STaskCheckpointReadyInfo* pInfo = taosArrayGet(pList, i);
    if (pInfo == NULL) {
      continue;
    }

    SRpcMsg msg = {0};
    int32_t code = initCheckpointReadyMsg(pTask, pInfo->upstreamNodeId, pInfo->upstreamTaskId, pInfo->childId,
                                          pInfo->checkpointId, &msg);
    if (code == TSDB_CODE_SUCCESS) {
      code = tmsgSendReq(&pInfo->upstreamNodeEpset, &msg);
      if (code == TSDB_CODE_SUCCESS) {
        stDebug("s-task:%s level:%d checkpoint-ready msg sent to upstream:0x%x", id, pTask->info.taskLevel,
                pInfo->upstreamTaskId);
      } else {
        stError("s-task:%s failed to send checkpoint-ready msg, try nex time in 10s", id);
      }
    } else {
      stError("s-task:%s failed to prepare the checkpoint-ready msg, try nex time in 10s", id);
    }
  }

  stDebug("s-task:%s level:%d checkpoint-ready msg sent to all %d upstreams", id, pTask->info.taskLevel, num);

  // start to check if checkpoint ready msg has successfully received by upstream tasks.
  if (pTask->info.taskLevel == TASK_LEVEL__SINK || pTask->info.taskLevel == TASK_LEVEL__AGG) {
    SStreamTmrInfo* pTmrInfo = &pActiveInfo->chkptReadyMsgTmr;

    int8_t old = atomic_val_compare_exchange_8(&pTmrInfo->isActive, 0, 1);
    if (old == 0) {
      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s start checkpoint-ready monitor in 10s, ref:%d ", pTask->id.idStr, ref);
      streamMetaAcquireOneTask(pTask);

      streamTmrStart(chkptReadyMsgSendMonitorFn, 200, pTask, streamTimer, &pTmrInfo->tmrHandle, vgId,
                     "chkpt-ready-monitor");

      // mark the timer monitor checkpointId
      pTmrInfo->launchChkptId = pActiveInfo->activeId;
    } else {
      stError("s-task:%s previous checkpoint-ready monitor tmr is set, not start new one", pTask->id.idStr);
    }
  }

  streamMutexUnlock(&pActiveInfo->lock);
  return TSDB_CODE_SUCCESS;
}

// this function is only invoked by source task, and send rsp to mnode
int32_t streamTaskSendCheckpointSourceRsp(SStreamTask* pTask) {
  SArray* pList = pTask->chkInfo.pActiveInfo->pReadyMsgList;

  streamMutexLock(&pTask->chkInfo.pActiveInfo->lock);
  if (taosArrayGetSize(pList) == 1) {
    STaskCheckpointReadyInfo* pInfo = taosArrayGet(pList, 0);
    if (pInfo != NULL) {
      tmsgSendRsp(&pInfo->msg);
      taosArrayClear(pList);
      stDebug("s-task:%s level:%d checkpoint-source rsp completed msg sent to mnode", pTask->id.idStr,
              pTask->info.taskLevel);
    } else {
      // todo
    }
  } else {
    stDebug("s-task:%s level:%d already send checkpoint-source rsp success to mnode", pTask->id.idStr,
            pTask->info.taskLevel);
  }

  streamMutexUnlock(&pTask->chkInfo.pActiveInfo->lock);
  return TSDB_CODE_SUCCESS;
}

int32_t streamAddBlockIntoDispatchMsg(const SSDataBlock* pBlock, SStreamDispatchReq* pReq) {
  int32_t dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock) + PAYLOAD_PREFIX_LEN;
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) {
    return terrno;
  }

  SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
  pRetrieve->useconds = 0;
  pRetrieve->precision = TSDB_DEFAULT_PRECISION;
  pRetrieve->compressed = 0;
  pRetrieve->completed = 1;
  pRetrieve->streamBlockType = pBlock->info.type;
  pRetrieve->numOfRows = htobe64((int64_t)pBlock->info.rows);
  pRetrieve->skey = htobe64(pBlock->info.window.skey);
  pRetrieve->ekey = htobe64(pBlock->info.window.ekey);
  pRetrieve->version = htobe64(pBlock->info.version);
  pRetrieve->watermark = htobe64(pBlock->info.watermark);
  memcpy(pRetrieve->parTbName, pBlock->info.parTbName, TSDB_TABLE_NAME_LEN);

  int32_t numOfCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  pRetrieve->numOfCols = htonl(numOfCols);

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data + PAYLOAD_PREFIX_LEN, numOfCols);
  if (actualLen < 0) {
    taosMemoryFree(buf);
    return terrno;
  }

  SET_PAYLOAD_LEN(pRetrieve->data, actualLen, actualLen);

  int32_t payloadLen = actualLen + PAYLOAD_PREFIX_LEN;
  pRetrieve->payloadLen = htonl(payloadLen);
  pRetrieve->compLen = htonl(payloadLen);

  payloadLen += sizeof(SRetrieveTableRsp);

  void* px = taosArrayPush(pReq->dataLen, &payloadLen);
  if (px == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  px = taosArrayPush(pReq->data, &buf);
  if (px == NULL) {
    taosMemoryFree(buf);
    return terrno;
  }

  pReq->totalLen += dataStrLen;
  return 0;
}

int32_t doSendDispatchMsg(SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t vgId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = 0;
  SRpcMsg msg = {0};

  // serialize
  int32_t tlen;
  tEncodeSize(tEncodeStreamDispatchReq, pReq, tlen, code);
  if (code < 0) {
    goto FAIL;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    code = terrno;
    goto FAIL;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamDispatchReq(&encoder, pReq)) < 0) {
    tEncoderClear(&encoder);
    goto FAIL;
  }
  tEncoderClear(&encoder);

  initRpcMsg(&msg, pTask->msgInfo.msgType, buf, tlen + sizeof(SMsgHead));
  stDebug("s-task:%s dispatch msg to taskId:0x%x vgId:%d data msg", pTask->id.idStr, pReq->taskId, vgId);

  return tmsgSendReq(pEpSet, &msg);

FAIL:
  if (buf) {
    rpcFreeCont(buf);
  }

  if (code == -1) {
    code = TSDB_CODE_INVALID_MSG;
  }

  return code;
}

int32_t streamTaskBuildCheckpointSourceRsp(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SRpcMsg* pMsg,
                                           int32_t setCode) {
  int32_t  len = 0;
  int32_t  code = 0;
  SEncoder encoder;

  SStreamCheckpointSourceRsp rsp = {
      .checkpointId = pReq->checkpointId,
      .taskId = pReq->taskId,
      .nodeId = pReq->nodeId,
      .streamId = pReq->streamId,
      .expireTime = pReq->expireTime,
      .mnodeId = pReq->mnodeId,
      .success = (setCode == TSDB_CODE_SUCCESS) ? 1 : 0,
  };

  tEncodeSize(tEncodeStreamCheckpointSourceRsp, &rsp, len, code);
  if (code < 0) {
    return code;
  }

  void* pBuf = rpcMallocCont(sizeof(SMsgHead) + len);
  if (pBuf == NULL) {
    return terrno;
  }

  ((SMsgHead*)pBuf)->vgId = htonl(pReq->mnodeId);
  void* abuf = POINTER_SHIFT(pBuf, sizeof(SMsgHead));

  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  code = tEncodeStreamCheckpointSourceRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  if (code < 0) {
    rpcFreeCont(pBuf);
    return code;
  }

  code = TMIN(code, 0);
  initRpcMsg(pMsg, 0, pBuf, sizeof(SMsgHead) + len);

  pMsg->code = setCode;
  pMsg->info = *pRpcInfo;
  return code;
}

int32_t streamAddCheckpointSourceRspMsg(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo,
                                        SStreamTask* pTask) {
  STaskCheckpointReadyInfo info = {
      .recvTs = taosGetTimestampMs(), .transId = pReq->transId, .checkpointId = pReq->checkpointId};

  // todo retry until it success
  int32_t code = streamTaskBuildCheckpointSourceRsp(pReq, pRpcInfo, &info.msg, TSDB_CODE_SUCCESS);
  if (code) {
    stError("s-task:%s failed to build checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
    return code;
  }

  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;
  streamMutexLock(&pActiveInfo->lock);

  int32_t size = taosArrayGetSize(pActiveInfo->pReadyMsgList);
  if (size > 0) {
    STaskCheckpointReadyInfo* pReady = taosArrayGet(pActiveInfo->pReadyMsgList, 0);
    if (pReady == NULL) {
      return terrno;
    }

    if (pReady->transId == pReq->transId) {
      stWarn("s-task:%s repeatly recv checkpoint source msg from mnode, checkpointId:%" PRId64 ", ignore",
             pTask->id.idStr, pReq->checkpointId);
    } else {
      stError("s-task:%s checkpointId:%" PRId64 " transId:%d not completed, new transId:%d checkpointId:%" PRId64
              " recv from mnode",
              pTask->id.idStr, pReady->checkpointId, pReady->transId, pReq->transId, pReq->checkpointId);
    }
  } else {
    void* px = taosArrayPush(pActiveInfo->pReadyMsgList, &info);
    if (px != NULL) {
      stDebug("s-task:%s add checkpoint source rsp msg, total:%d", pTask->id.idStr, size + 1);
    } else {
      stError("s-task:%s failed to add readyMsg, code: out of memory", pTask->id.idStr);
    }
  }

  streamMutexUnlock(&pActiveInfo->lock);
  return TSDB_CODE_SUCCESS;
}

void initCheckpointReadyInfo(STaskCheckpointReadyInfo* pReadyInfo, int32_t upstreamNodeId, int32_t upstreamTaskId,
                             int32_t childId, SEpSet* pEpset, int64_t checkpointId) {
  pReadyInfo->upstreamTaskId = upstreamTaskId;
  pReadyInfo->upstreamNodeEpset = *pEpset;
  pReadyInfo->upstreamNodeId = upstreamNodeId;
  pReadyInfo->recvTs = taosGetTimestampMs();
  pReadyInfo->checkpointId = checkpointId;
  pReadyInfo->childId = childId;
}

static int32_t doAddChkptReadyMsg(SStreamTask* pTask, STaskCheckpointReadyInfo* pInfo) {
  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;

  void* px = taosArrayPush(pActiveInfo->pReadyMsgList, pInfo);
  if (px == NULL) {
    stError("s-task:%s failed to add readyMsg info, code: out of memory", pTask->id.idStr);
    return terrno;
  }

  int32_t numOfRecv = taosArrayGetSize(pActiveInfo->pReadyMsgList);
  int32_t total = streamTaskGetNumOfUpstream(pTask);
  if (numOfRecv == total) {
    stDebug("s-task:%s recv checkpoint-trigger from all upstream, continue", pTask->id.idStr);
    pActiveInfo->allUpstreamTriggerRecv = 1;
  } else {
    stDebug("s-task:%s %d/%d checkpoint-trigger recv", pTask->id.idStr, numOfRecv, total);
  }

  return 0;
}

int32_t streamAddCheckpointReadyMsg(SStreamTask* pTask, int32_t upstreamTaskId, int32_t index, int64_t checkpointId) {
  int32_t                  code = 0;
  STaskCheckpointReadyInfo info = {0};

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    return TSDB_CODE_SUCCESS;
  }

  SStreamUpstreamEpInfo* pInfo = NULL;
  streamTaskGetUpstreamTaskEpInfo(pTask, upstreamTaskId, &pInfo);
  if (pInfo == NULL) {
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  initCheckpointReadyInfo(&info, pInfo->nodeId, pInfo->taskId, pInfo->childId, &pInfo->epSet, checkpointId);

  stDebug("s-task:%s (level:%d) prepare checkpoint-ready msg to upstream s-task:0x%" PRIx64 "-0x%x (vgId:%d) idx:%d",
          pTask->id.idStr, pTask->info.taskLevel, pTask->id.streamId, pInfo->taskId, pInfo->nodeId, index);

  SActiveCheckpointInfo* pActiveInfo = pTask->chkInfo.pActiveInfo;

  streamMutexLock(&pActiveInfo->lock);
  code = doAddChkptReadyMsg(pTask, &info);
  streamMutexUnlock(&pActiveInfo->lock);
  return code;
}

void streamClearChkptReadyMsg(SActiveCheckpointInfo* pActiveInfo) {
  if (pActiveInfo == NULL) {
    return;
  }

  for (int i = 0; i < taosArrayGetSize(pActiveInfo->pReadyMsgList); i++) {
    STaskCheckpointReadyInfo* pInfo = taosArrayGet(pActiveInfo->pReadyMsgList, i);
    if (pInfo != NULL) {
      rpcFreeCont(pInfo->msg.pCont);
    }
  }

  taosArrayClear(pActiveInfo->pReadyMsgList);
}

// this message has been sent successfully, let's try next one.
static int32_t handleDispatchSuccessRsp(SStreamTask* pTask, int32_t downstreamId, int32_t downstreamNodeId) {
  stDebug("s-task:%s destroy dispatch msg:%p", pTask->id.idStr, pTask->msgInfo.pData);

  int64_t el = taosGetTimestampMs() - pTask->msgInfo.startTs;
  bool    delayDispatch = (pTask->msgInfo.dispatchMsgType == STREAM_INPUT__CHECKPOINT_TRIGGER);

  clearBufferedDispatchMsg(pTask);

  // put data into inputQ of current task is also allowed
  if (pTask->inputq.status == TASK_INPUT_STATUS__BLOCKED) {
    pTask->inputq.status = TASK_INPUT_STATUS__NORMAL;
    stDebug("s-task:%s downstream task:0x%x resume to normal from inputQ blocking, blocking time:%" PRId64 "ms",
            pTask->id.idStr, downstreamId, el);
  } else {
    stDebug("s-task:%s dispatch completed, elapsed time:%" PRId64 "ms", pTask->id.idStr, el);
  }

  // now ready for next data output
  atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);

  // otherwise, continue dispatch the first block to down stream task in pipeline
  if (delayDispatch) {
    return 0;
  } else {
    return streamDispatchStreamBlock(pTask);
  }
}

static bool setDispatchRspInfo(SDispatchMsgInfo* pMsgInfo, int32_t vgId, int32_t code, int64_t now, int32_t* pNotRsp,
                               int32_t* pFailed, const char* id) {
  int32_t numOfRsp = 0;
  int32_t numOfFailed = 0;

  bool    allRsp = false;
  int32_t numOfDispatchBranch = taosArrayGetSize(pMsgInfo->pSendInfo);

  *pNotRsp = 0;
  *pFailed = 0;

  for (int32_t i = 0; i < numOfDispatchBranch; ++i) {
    SDispatchEntry* pEntry = taosArrayGet(pMsgInfo->pSendInfo, i);
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->rspTs != -1) {
      numOfRsp += 1;
    }
  }

  for (int32_t j = 0; j < numOfDispatchBranch; ++j) {
    SDispatchEntry* pEntry = taosArrayGet(pMsgInfo->pSendInfo, j);
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->nodeId == vgId) {
      if (pEntry->rspTs != -1) {
        stDebug("s-task:%s dispatch rsp has already recved at:%" PRId64 ", ignore this rsp, msgId:%d", id,
                pEntry->rspTs, pMsgInfo->msgId);
        allRsp = false;
      } else {
        pEntry->rspTs = now;
        pEntry->status = code;
        numOfRsp += 1;
        allRsp = (numOfRsp == numOfDispatchBranch);

        stDebug("s-task:%s record the rsp recv, ts:%" PRId64 " code:%d, idx:%d, total recv:%d/%d", id, now, code, j,
                numOfRsp, numOfDispatchBranch);
      }
      break;
    }
  }

  // this code may be error code.
  for (int32_t i = 0; i < numOfDispatchBranch; ++i) {
    SDispatchEntry* pEntry = taosArrayGet(pMsgInfo->pSendInfo, i);
    if (pEntry->status != TSDB_CODE_SUCCESS || isDispatchRspTimeout(pEntry, now)) {
      numOfFailed += 1;
    }
  }

  *pFailed = numOfFailed;
  *pNotRsp = numOfDispatchBranch - numOfRsp;

  return allRsp;
}

bool isDispatchRspTimeout(SDispatchEntry* pEntry, int64_t now) {
  return (pEntry->rspTs == -1) && (now - pEntry->sendTs) > 30 * 1000;
}

int32_t getFailedDispatchInfo(SDispatchMsgInfo* pMsgInfo, int64_t now) {
  int32_t numOfFailed = 0;
  streamMutexLock(&pMsgInfo->lock);

  for (int32_t j = 0; j < taosArrayGetSize(pMsgInfo->pSendInfo); ++j) {
    SDispatchEntry* pEntry = taosArrayGet(pMsgInfo->pSendInfo, j);
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->status != TSDB_CODE_SUCCESS || isDispatchRspTimeout(pEntry, now)) {
      numOfFailed += 1;
    }
  }
  streamMutexUnlock(&pMsgInfo->lock);
  return numOfFailed;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code) {
  const char*            id = pTask->id.idStr;
  int32_t                vgId = pTask->pMeta->vgId;
  SDispatchMsgInfo*      pMsgInfo = &pTask->msgInfo;
  int64_t                now = taosGetTimestampMs();
  bool                   allRsp = false;
  int32_t                notRsp = 0;
  int32_t                numOfFailed = 0;
  bool                   triggerDispatchRsp = false;
  SActiveCheckpointInfo* pInfo = pTask->chkInfo.pActiveInfo;
  int64_t                tmpCheckpointId = -1;
  int32_t                tmpTranId = -1;
  const char*            pStatus = NULL;

  // we only set the dispatch msg info for current checkpoint trans
  streamMutexLock(&pTask->lock);
  SStreamTaskState s = streamTaskGetStatus(pTask);
  triggerDispatchRsp = (s.state == TASK_STATUS__CK) && (pInfo->activeId == pMsgInfo->checkpointId) &&
                       (pInfo->transId == pMsgInfo->transId);
  tmpCheckpointId = pInfo->activeId;
  tmpTranId = pInfo->transId;
  pStatus = s.name;
  streamMutexUnlock(&pTask->lock);

  streamMutexLock(&pMsgInfo->lock);
  int32_t msgId = pMsgInfo->msgId;

  // follower not handle the dispatch rsp
  if ((pTask->pMeta->role == NODE_ROLE_FOLLOWER) || (pTask->status.downstreamReady != 1)) {
    stError("s-task:%s vgId:%d is follower or just re-launched, not handle the dispatch rsp, discard it", id, vgId);
    streamMutexUnlock(&pMsgInfo->lock);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  // discard invalid dispatch rsp msg
  if ((pRsp->msgId != msgId) || (pRsp->stage != pTask->pMeta->stage)) {
    stError("s-task:%s vgId:%d not expect rsp, expected: msgId:%d, stage:%" PRId64 " actual msgId:%d, stage:%" PRId64
            " discard it",
            id, vgId, msgId, pTask->pMeta->stage, pRsp->msgId, pRsp->stage);
    streamMutexUnlock(&pMsgInfo->lock);
    return TSDB_CODE_INVALID_MSG;
  }

  if (code != TSDB_CODE_SUCCESS) {
    // dispatch message failed: network error, or node not available.
    // in case of the input queue is full, the code will be TSDB_CODE_SUCCESS, the and pRsp->inputStatus will be set
    // flag. Here we need to retry dispatch this message to downstream task immediately. handle the case the failure
    // happened too fast.
    if (code == TSDB_CODE_STREAM_TASK_NOT_EXIST) {  // destination task does not exist, not retry anymore
      stError("s-task:%s failed to dispatch msg to task:0x%x(vgId:%d), msgId:%d no retry, since task destroyed already",
              id, pRsp->downstreamTaskId, pRsp->downstreamNodeId, msgId);
      allRsp = setDispatchRspInfo(pMsgInfo, pRsp->downstreamNodeId, TSDB_CODE_SUCCESS, now, &notRsp, &numOfFailed, id);
    } else {
      stError("s-task:%s failed to dispatch msgId:%d to task:0x%x(vgId:%d), code:%s, add to retry list", id, msgId,
              pRsp->downstreamTaskId, pRsp->downstreamNodeId, tstrerror(code));
      allRsp = setDispatchRspInfo(pMsgInfo, pRsp->downstreamNodeId, code, now, &notRsp, &numOfFailed, id);
    }

  } else {  // code == 0
    if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
      pTask->inputq.status = TASK_INPUT_STATUS__BLOCKED;
      // block the input of current task, to push pressure to upstream
      allRsp = setDispatchRspInfo(pMsgInfo, pRsp->downstreamNodeId, pRsp->inputStatus, now, &notRsp, &numOfFailed, id);
      stTrace("s-task:%s inputQ of downstream task:0x%x(vgId:%d) is full, wait for retry dispatch", id,
              pRsp->downstreamTaskId, pRsp->downstreamNodeId);
    } else {
      if (pRsp->inputStatus == TASK_INPUT_STATUS__REFUSED) {
        // todo handle the role-changed during checkpoint generation, add test case
        stError(
            "s-task:%s downstream task:0x%x(vgId:%d) refused the dispatch msg, downstream may become follower or "
            "restart already, treat it as success",
            id, pRsp->downstreamTaskId, pRsp->downstreamNodeId);
      }

      allRsp = setDispatchRspInfo(pMsgInfo, pRsp->downstreamNodeId, TSDB_CODE_SUCCESS, now, &notRsp, &numOfFailed, id);

      {
        bool delayDispatch = (pMsgInfo->dispatchMsgType == STREAM_INPUT__CHECKPOINT_TRIGGER);
        if (delayDispatch) {
          // we only set the dispatch msg info for current checkpoint trans
          if (triggerDispatchRsp) {
            stDebug("s-task:%s checkpoint-trigger msg to 0x%x rsp for checkpointId:%" PRId64 " transId:%d confirmed",
                    pTask->id.idStr, pRsp->downstreamTaskId, pMsgInfo->checkpointId, pMsgInfo->transId);

            streamTaskSetTriggerDispatchConfirmed(pTask, pRsp->downstreamNodeId);
          } else {
            stWarn("s-task:%s checkpoint-trigger msg rsp for checkpointId:%" PRId64
                   " transId:%d discard, current status:%s, active checkpointId:%" PRId64
                   " active transId:%d, since expired",
                   pTask->id.idStr, pMsgInfo->checkpointId, pMsgInfo->transId, pStatus, tmpCheckpointId, tmpTranId);
          }
        }
      }
    }
  }

  streamMutexUnlock(&pMsgInfo->lock);

  if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    if (!allRsp) {
      stDebug(
          "s-task:%s recv dispatch rsp, msgId:%d from 0x%x(vgId:%d), downstream task input status:%d code:%s, "
          "waiting for %d rsp",
          id, msgId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->inputStatus, tstrerror(code), notRsp);
    } else {
      stDebug(
          "s-task:%s recv dispatch rsp, msgId:%d from 0x%x(vgId:%d), downstream task input status:%d code:%s, all "
          "rsp",
          id, msgId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->inputStatus, tstrerror(code));
    }
  } else {
    stDebug("s-task:%s recv fix-dispatch rsp, msgId:%d from 0x%x(vgId:%d), downstream task input status:%d code:%s", id,
            msgId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->inputStatus, tstrerror(code));
  }

  // all msg rsp already, continue
  // we need to re-try send dispatch msg to downstream tasks
  if (allRsp && (numOfFailed == 0)) {
    // trans-state msg has been sent to downstream successfully. let's transfer the fill-history task state
    if (pMsgInfo->dispatchMsgType == STREAM_INPUT__TRANS_STATE) {
      stDebug("s-task:%s dispatch trans-state msgId:%d to downstream successfully, start to prepare transfer state", id,
              msgId);
      if (pTask->info.fillHistory != 1) {
        stFatal("s-task:%s unexpected dispatch rsp, not scan-history task, not recv this dispatch rsp", id);
      }

      code = streamTransferStatePrepare(pTask);
      if (code != TSDB_CODE_SUCCESS) {  // todo: do nothing if error happens
      }

      clearBufferedDispatchMsg(pTask);

      // now ready for next data output
      atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);
    } else {
      // this message has been sent successfully, let's try next one.
      code = handleDispatchSuccessRsp(pTask, pRsp->downstreamTaskId, pRsp->downstreamNodeId);
    }
  }

  return code;
}

static int32_t buildDispatchRsp(const SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t status, void** pBuf) {
  *pBuf = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  if (*pBuf == NULL) {
    return terrno;
  }

  ((SMsgHead*)(*pBuf))->vgId = htonl(pReq->upstreamNodeId);

  if (((SMsgHead*)(*pBuf))->vgId == 0) {
    return TSDB_CODE_INVALID_MSG;
  }

  SStreamDispatchRsp* pDispatchRsp = POINTER_SHIFT((*pBuf), sizeof(SMsgHead));

  pDispatchRsp->stage = htobe64(pReq->stage);
  pDispatchRsp->msgId = htonl(pReq->msgId);
  pDispatchRsp->inputStatus = status;
  pDispatchRsp->streamId = htobe64(pReq->streamId);
  pDispatchRsp->upstreamNodeId = htonl(pReq->upstreamNodeId);
  pDispatchRsp->upstreamTaskId = htonl(pReq->upstreamTaskId);
  pDispatchRsp->downstreamNodeId = htonl(pTask->info.nodeId);
  pDispatchRsp->downstreamTaskId = htonl(pTask->id.taskId);

  return TSDB_CODE_SUCCESS;
}

static int32_t streamTaskAppendInputBlocks(SStreamTask* pTask, const SStreamDispatchReq* pReq) {
  int8_t status = 0;

  SStreamDataBlock* pBlock = NULL;

  int32_t code = createStreamBlockFromDispatchMsg(pReq, pReq->type, pReq->srcVgId, &pBlock);
  if (code) {
    streamTaskInputFail(pTask);
    status = TASK_INPUT_STATUS__FAILED;
    stError("vgId:%d, s-task:%s failed to receive dispatch msg, reason: out of memory", pTask->pMeta->vgId,
            pTask->id.idStr);
  } else {
    if (pBlock->type == STREAM_INPUT__TRANS_STATE) {
      pTask->status.appendTranstateBlock = true;
    }

    code = streamTaskPutDataIntoInputQ(pTask, (SStreamQueueItem*)pBlock);
    // input queue is full, upstream is blocked now
    status = (code == TSDB_CODE_SUCCESS) ? TASK_INPUT_STATUS__NORMAL : TASK_INPUT_STATUS__BLOCKED;
  }

  return status;
}

int32_t streamProcessDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pReq, SRpcMsg* pRsp) {
  int32_t      status = 0;
  SStreamMeta* pMeta = pTask->pMeta;
  const char*  id = pTask->id.idStr;

  stDebug("s-task:%s receive dispatch msg from taskId:0x%x(vgId:%d), msgLen:%" PRId64 ", msgId:%d", id,
          pReq->upstreamTaskId, pReq->upstreamNodeId, pReq->totalLen, pReq->msgId);

  SStreamUpstreamEpInfo* pInfo = NULL;
  streamTaskGetUpstreamTaskEpInfo(pTask, pReq->upstreamTaskId, &pInfo);
  if (pInfo == NULL) {
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  if (pMeta->role == NODE_ROLE_FOLLOWER) {
    stError("s-task:%s task on follower received dispatch msgs, dispatch msg rejected", id);
    status = TASK_INPUT_STATUS__REFUSED;
  } else {
    if (pReq->stage > pInfo->stage) {
      // upstream task has restarted/leader-follower switch/transferred to other dnodes
      stError("s-task:%s upstream task:0x%x (vgId:%d) has restart/leader-switch/vnode-transfer, prev stage:%" PRId64
              ", current:%" PRId64 " dispatch msg rejected",
              id, pReq->upstreamTaskId, pReq->upstreamNodeId, pInfo->stage, pReq->stage);
      status = TASK_INPUT_STATUS__REFUSED;
    } else {
      if (!pInfo->dataAllowed) {
        stWarn("s-task:%s data from task:0x%x is denied, since inputQ is closed for it", id, pReq->upstreamTaskId);
        status = TASK_INPUT_STATUS__BLOCKED;
      } else {
        // This task has received the checkpoint req from the upstream task, from which all the messages should be
        // blocked. Note that there is no race condition here.
        if (pReq->type == STREAM_INPUT__CHECKPOINT_TRIGGER) {
          streamTaskCloseUpstreamInput(pTask, pReq->upstreamTaskId);
          stDebug("s-task:%s close inputQ for upstream:0x%x, msgId:%d", id, pReq->upstreamTaskId, pReq->msgId);
        } else if (pReq->type == STREAM_INPUT__TRANS_STATE) {
          stDebug("s-task:%s recv trans-state msgId:%d from upstream:0x%x", id, pReq->msgId, pReq->upstreamTaskId);
        }

        status = streamTaskAppendInputBlocks(pTask, pReq);
      }
    }
  }

  {
    // do send response with the input status
    int32_t code = buildDispatchRsp(pTask, pReq, status, &pRsp->pCont);
    if (code != TSDB_CODE_SUCCESS) {
      stError("s-task:%s failed to build dispatch rsp, msgId:%d, code:%s", id, pReq->msgId, tstrerror(code));
      return code;
    }

    pRsp->contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
    tmsgSendRsp(pRsp);
  }

  return streamTrySchedExec(pTask);
}
