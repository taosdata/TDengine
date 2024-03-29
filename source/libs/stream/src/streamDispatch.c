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

typedef struct {
  int32_t upStreamTaskId;
  SEpSet  upstreamNodeEpset;
  SRpcMsg msg;
} SStreamChkptReadyInfo;

static void    doRetryDispatchData(void* param, void* tmrId);
static int32_t doSendDispatchMsg(SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t vgId, SEpSet* pEpSet);
static int32_t streamAddBlockIntoDispatchMsg(const SSDataBlock* pBlock, SStreamDispatchReq* pReq);
static int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock,
                                       int32_t vgSz, int64_t groupId);
static int32_t tInitStreamDispatchReq(SStreamDispatchReq* pReq, const SStreamTask* pTask, int32_t vgId,
                                      int32_t numOfBlocks, int64_t dstTaskId, int32_t type);

void initRpcMsg(SRpcMsg* pMsg, int32_t msgType, void* pCont, int32_t contLen) {
  pMsg->msgType = msgType;
  pMsg->pCont = pCont;
  pMsg->contLen = contLen;
}

int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->stage) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->msgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->srcVgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->type) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->type) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamRelTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->blockNum) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->totalLen) < 0) return -1;
  ASSERT(taosArrayGetSize(pReq->data) == pReq->blockNum);
  ASSERT(taosArrayGetSize(pReq->dataLen) == pReq->blockNum);
  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t len = *(int32_t*)taosArrayGet(pReq->dataLen, i);
    void*   data = taosArrayGetP(pReq->data, i);
    if (tEncodeI32(pEncoder, len) < 0) return -1;
    if (tEncodeBinary(pEncoder, data, len) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->stage) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->msgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->srcVgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->type) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->type) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamRelTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->blockNum) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->totalLen) < 0) return -1;

  ASSERT(pReq->blockNum > 0);
  pReq->data = taosArrayInit(pReq->blockNum, sizeof(void*));
  pReq->dataLen = taosArrayInit(pReq->blockNum, sizeof(int32_t));
  for (int32_t i = 0; i < pReq->blockNum; i++) {
    int32_t  len1;
    uint64_t len2;
    void*    data;
    if (tDecodeI32(pDecoder, &len1) < 0) return -1;
    if (tDecodeBinaryAlloc(pDecoder, &data, &len2) < 0) return -1;
    ASSERT(len1 == len2);
    taosArrayPush(pReq->dataLen, &len1);
    taosArrayPush(pReq->data, &data);
  }

  tEndDecode(pDecoder);
  return 0;
}

static int32_t tInitStreamDispatchReq(SStreamDispatchReq* pReq, const SStreamTask* pTask, int32_t vgId,
                                      int32_t numOfBlocks, int64_t dstTaskId, int32_t type) {
  pReq->streamId = pTask->id.streamId;
  pReq->srcVgId = vgId;
  pReq->stage = pTask->pMeta->stage;
  pReq->msgId = pTask->execInfo.dispatch;
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

void tDeleteStreamDispatchReq(SStreamDispatchReq* pReq) {
  taosArrayDestroyP(pReq->data, taosMemoryFree);
  taosArrayDestroy(pReq->dataLen);
}

int32_t tEncodeStreamRetrieveReq(SEncoder* pEncoder, const SStreamRetrieveReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->reqId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->dstNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->dstTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->srcNodeId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->srcTaskId) < 0) return -1;
  if (tEncodeBinary(pEncoder, (const uint8_t*)pReq->pRetrieve, pReq->retrieveLen) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamRetrieveReq(SDecoder* pDecoder, SStreamRetrieveReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->reqId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->dstNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->dstTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->srcNodeId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->srcTaskId) < 0) return -1;
  uint64_t len = 0;
  if (tDecodeBinaryAlloc(pDecoder, (void**)&pReq->pRetrieve, &len) < 0) return -1;
  pReq->retrieveLen = (int32_t)len;
  tEndDecode(pDecoder);
  return 0;
}

void tDeleteStreamRetrieveReq(SStreamRetrieveReq* pReq) { taosMemoryFree(pReq->pRetrieve); }

void sendRetrieveRsp(SStreamRetrieveReq *pReq, SRpcMsg* pRsp){
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

int32_t broadcastRetrieveMsg(SStreamTask* pTask, SStreamRetrieveReq* req) {
  int32_t code = 0;
  void*   buf = NULL;
  int32_t sz = taosArrayGetSize(pTask->upstreamInfo.pList);
  ASSERT(sz > 0);
  for (int32_t i = 0; i < sz; i++) {
    req->reqId = tGenIdPI64();
    SStreamChildEpInfo* pEpInfo = taosArrayGetP(pTask->upstreamInfo.pList, i);
    req->dstNodeId = pEpInfo->nodeId;
    req->dstTaskId = pEpInfo->taskId;
    int32_t len;
    tEncodeSize(tEncodeStreamRetrieveReq, req, len, code);
    if (code != 0) {
      ASSERT(0);
      return code;
    }

    buf = rpcMallocCont(sizeof(SMsgHead) + len);
    if (buf == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      return code;
    }

    ((SMsgHead*)buf)->vgId = htonl(pEpInfo->nodeId);
    void*    abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, len);
    tEncodeStreamRetrieveReq(&encoder, req);
    tEncoderClear(&encoder);

    SRpcMsg rpcMsg = {0};
    initRpcMsg(&rpcMsg, TDMT_STREAM_RETRIEVE, buf, len + sizeof(SMsgHead));

    code = tmsgSendReq(&pEpInfo->epSet, &rpcMsg);
    if (code != 0) {
      ASSERT(0);
      rpcFreeCont(buf);
      return code;
    }

    buf = NULL;
    stDebug("s-task:%s (child %d) send retrieve req to task:0x%x (vgId:%d), reqId:0x%" PRIx64, pTask->id.idStr,
            pTask->info.selfChildId, pEpInfo->taskId, pEpInfo->nodeId, req->reqId);
  }
  return code;
}

static int32_t buildStreamRetrieveReq(SStreamTask* pTask, const SSDataBlock* pBlock, SStreamRetrieveReq* req){
  SRetrieveTableRsp* pRetrieve = NULL;
  int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);

  pRetrieve = taosMemoryCalloc(1, dataStrLen);
  if (pRetrieve == NULL) return TSDB_CODE_OUT_OF_MEMORY;

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

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);

  req->streamId = pTask->id.streamId;
  req->srcNodeId = pTask->info.nodeId;
  req->srcTaskId = pTask->id.taskId;
  req->pRetrieve = pRetrieve;
  req->retrieveLen = dataStrLen;
  return 0;
}

int32_t streamBroadcastToUpTasks(SStreamTask* pTask, const SSDataBlock* pBlock) {
  SStreamRetrieveReq req;
  int32_t code = buildStreamRetrieveReq(pTask, pBlock, &req);
  if(code != 0){
    return code;
  }

  code = broadcastRetrieveMsg(pTask, &req);
  taosMemoryFree(req.pRetrieve);

  return code;
}

int32_t streamSendCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  int32_t tlen;
  tEncodeSize(tEncodeStreamTaskCheckReq, pReq, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMsgHead*)buf)->vgId = htonl(nodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamTaskCheckReq(&encoder, pReq)) < 0) {
    rpcFreeCont(buf);
    return code;
  }
  tEncoderClear(&encoder);

  initRpcMsg(&msg, TDMT_VND_STREAM_TASK_CHECK, buf, tlen + sizeof(SMsgHead));
  stDebug("s-task:%s (level:%d) send check msg to s-task:0x%" PRIx64 ":0x%x (vgId:%d)", pTask->id.idStr,
          pTask->info.taskLevel, pReq->streamId, pReq->downstreamTaskId, nodeId);

  tmsgSendReq(pEpSet, &msg);
  return 0;
}

void destroyDispatchMsg(SStreamDispatchReq* pReq, int32_t numOfVgroups) {
  for (int32_t i = 0; i < numOfVgroups; i++) {
    taosArrayDestroyP(pReq[i].data, taosMemoryFree);
    taosArrayDestroy(pReq[i].dataLen);
  }

  taosMemoryFree(pReq);
}

int32_t getNumOfDispatchBranch(SStreamTask* pTask) {
  return (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH)
             ? 1
             : taosArrayGetSize(pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos);
}

static int32_t doBuildDispatchMsg(SStreamTask* pTask, const SStreamDataBlock* pData) {
  int32_t code = 0;
  int32_t numOfBlocks = taosArrayGetSize(pData->blocks);
  ASSERT(numOfBlocks != 0 && pTask->msgInfo.pData == NULL);

  pTask->msgInfo.dispatchMsgType = pData->type;

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    SStreamDispatchReq* pReq = taosMemoryCalloc(1, sizeof(SStreamDispatchReq));

    int32_t downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;
    code = tInitStreamDispatchReq(pReq, pTask, pData->srcVgId, numOfBlocks, downstreamTaskId, pData->type);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);
      code = streamAddBlockIntoDispatchMsg(pDataBlock, pReq);
      if (code != TSDB_CODE_SUCCESS) {
        destroyDispatchMsg(pReq, 1);
        return code;
      }
    }

    pTask->msgInfo.pData = pReq;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    int32_t rspCnt = atomic_load_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt);
    ASSERT(rspCnt == 0);

    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgroups = taosArrayGetSize(vgInfo);

    SStreamDispatchReq* pReqs = taosMemoryCalloc(numOfVgroups, sizeof(SStreamDispatchReq));
    if (pReqs == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < numOfVgroups; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      code = tInitStreamDispatchReq(&pReqs[i], pTask, pData->srcVgId, 0, pVgInfo->taskId, pData->type);
      if (code != TSDB_CODE_SUCCESS) {
        destroyDispatchMsg(pReqs, numOfVgroups);
        return code;
      }
    }

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);

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
            atomic_add_fetch_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt, 1);
          }

          pReqs[j].blockNum++;
        }

        continue;
      }

      code = streamSearchAndAddBlock(pTask, pReqs, pDataBlock, numOfVgroups, pDataBlock->info.id.groupId);
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
    stDebug("s-task:%s build dispatch msg success, msgId:%d, stage:%" PRId64 " dstVgNum:%d %p", pTask->id.idStr,
            pTask->execInfo.dispatch, pTask->pMeta->stage, pTask->outputInfo.shuffleDispatcher.waitingRspCnt,
            pTask->msgInfo.pData);
  }

  return code;
}

static int32_t sendDispatchMsg(SStreamTask* pTask, SStreamDispatchReq* pDispatchMsg) {
  int32_t     code = 0;
  int32_t     msgId = pTask->execInfo.dispatch;
  const char* id = pTask->id.idStr;

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    int32_t vgId = pTask->outputInfo.fixedDispatcher.nodeId;
    SEpSet* pEpSet = &pTask->outputInfo.fixedDispatcher.epSet;
    int32_t downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;

    stDebug("s-task:%s (child taskId:%d) fix-dispatch %d block(s) to s-task:0x%x (vgId:%d), id:%d", id,
            pTask->info.selfChildId, 1, downstreamTaskId, vgId, msgId);

    code = doSendDispatchMsg(pTask, pDispatchMsg, vgId, pEpSet);
  } else {
    SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgroups = taosArrayGetSize(vgInfo);

    int32_t actualVgroups = pTask->outputInfo.shuffleDispatcher.waitingRspCnt;
    stDebug("s-task:%s (child taskId:%d) start to shuffle-dispatch blocks to %d/%d vgroup(s), msgId:%d", id,
            pTask->info.selfChildId, actualVgroups, numOfVgroups, msgId);

    int32_t numOfSend = 0;
    for (int32_t i = 0; i < numOfVgroups; i++) {
      if (pDispatchMsg[i].blockNum > 0) {
        SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
        stDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to vgId:%d", pTask->id.idStr,
                pTask->info.selfChildId, pDispatchMsg[i].blockNum, pVgInfo->vgId);

        code = doSendDispatchMsg(pTask, &pDispatchMsg[i], pVgInfo->vgId, &pVgInfo->epSet);
        if (code < 0) {
          break;
        }

        // no need to try remain, all already send.
        if (++numOfSend == actualVgroups) {
          break;
        }
      }
    }

    stDebug("s-task:%s complete shuffle-dispatch blocks to all %d vnodes, msgId:%d", pTask->id.idStr, numOfVgroups,
            msgId);
  }

  return code;
}

static void doRetryDispatchData(void* param, void* tmrId) {
  SStreamTask* pTask = param;
  const char*  id = pTask->id.idStr;
  int32_t      msgId = pTask->execInfo.dispatch;

  if (streamTaskShouldStop(pTask)) {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s should stop, abort from timer, ref:%d", pTask->id.idStr, ref);
    return;
  }

  ASSERT(pTask->outputq.status == TASK_OUTPUT_STATUS__WAIT);

  int32_t code = 0;

  {
    SArray* pList = taosArrayDup(pTask->msgInfo.pRetryList, NULL);
    taosArrayClear(pTask->msgInfo.pRetryList);

    SStreamDispatchReq* pReq = pTask->msgInfo.pData;

    if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
      SArray* vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
      int32_t numOfVgroups = taosArrayGetSize(vgInfo);

      int32_t numOfFailed = taosArrayGetSize(pList);
      stDebug("s-task:%s (child taskId:%d) retry shuffle-dispatch blocks to %d vgroup(s), msgId:%d", id,
              pTask->info.selfChildId, numOfFailed, msgId);

      for (int32_t i = 0; i < numOfFailed; i++) {
        int32_t vgId = *(int32_t*)taosArrayGet(pList, i);

        for (int32_t j = 0; j < numOfVgroups; ++j) {
          SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, j);
          if (pVgInfo->vgId == vgId) {
            stDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to vgId:%d", pTask->id.idStr,
                    pTask->info.selfChildId, pReq[j].blockNum, pVgInfo->vgId);

            code = doSendDispatchMsg(pTask, &pReq[j], pVgInfo->vgId, &pVgInfo->epSet);
            if (code < 0) {
              break;
            }
          }
        }
      }

      stDebug("s-task:%s complete re-try shuffle-dispatch blocks to all %d vnodes, msgId:%d", pTask->id.idStr,
              numOfFailed, msgId);
    } else {
      int32_t vgId = pTask->outputInfo.fixedDispatcher.nodeId;
      SEpSet* pEpSet = &pTask->outputInfo.fixedDispatcher.epSet;
      int32_t downstreamTaskId = pTask->outputInfo.fixedDispatcher.taskId;

      stDebug("s-task:%s (child taskId:%d) fix-dispatch %d block(s) to s-task:0x%x (vgId:%d), id:%d", id,
              pTask->info.selfChildId, 1, downstreamTaskId, vgId, msgId);

      code = doSendDispatchMsg(pTask, pReq, vgId, pEpSet);
    }

    taosArrayDestroy(pList);
  }

  if (code != TSDB_CODE_SUCCESS) {
    if (!streamTaskShouldStop(pTask)) {
      //      stDebug("s-task:%s reset the waitRspCnt to be 0 before launch retry dispatch", pTask->id.idStr);
      //      atomic_store_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt, 0);
      if (streamTaskShouldPause(pTask)) {
        streamRetryDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS * 10);
      } else {
        streamRetryDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS);
      }
    } else {
      int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s should stop, abort from timer, ref:%d", pTask->id.idStr, ref);
    }
  } else {
    int32_t ref = atomic_sub_fetch_32(&pTask->status.timerActive, 1);
    stDebug("s-task:%s send success, jump out of timer, ref:%d", pTask->id.idStr, ref);
  }
}

void streamRetryDispatchData(SStreamTask* pTask, int64_t waitDuration) {
  pTask->msgInfo.retryCount++;

  stWarn("s-task:%s retry send dispatch data in %" PRId64 "ms, in timer msgId:%d, retryTimes:%d", pTask->id.idStr,
         waitDuration, pTask->execInfo.dispatch, pTask->msgInfo.retryCount);

  if (pTask->msgInfo.pTimer != NULL) {
    taosTmrReset(doRetryDispatchData, waitDuration, pTask, streamTimer, &pTask->msgInfo.pTimer);
  } else {
    pTask->msgInfo.pTimer = taosTmrStart(doRetryDispatchData, waitDuration, pTask, streamTimer);
  }
}

int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock, int32_t vgSz,
                                int64_t groupId) {
  uint32_t hashValue = 0;
  SArray*  vgInfo = pTask->outputInfo.shuffleDispatcher.dbInfo.pVgroupInfos;
  if (pTask->pNameMap == NULL) {
    pTask->pNameMap = tSimpleHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
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
      if(pTask->ver >= SSTREAM_TASK_SUBTABLE_CHANGED_VER &&
          pTask->subtableWithoutMd5 != 1 &&
          !isAutoTableName(pDataBlock->info.parTbName) &&
          !alreadyAddGroupId(pDataBlock->info.parTbName) &&
          groupId != 0){
        buildCtbNameAddGroupId(pDataBlock->info.parTbName, groupId);
      }
    } else {
      buildCtbNameByGroupIdImpl(pTask->outputInfo.shuffleDispatcher.stbFullName, groupId, pDataBlock->info.parTbName);
    }
    snprintf(ctbName, TSDB_TABLE_NAME_LEN, "%s.%s", pTask->outputInfo.shuffleDispatcher.dbInfo.db, pDataBlock->info.parTbName);
    /*uint32_t hashValue = MurmurHash3_32(ctbName, strlen(ctbName));*/
    SUseDbRsp* pDbInfo = &pTask->outputInfo.shuffleDispatcher.dbInfo;
    hashValue = taosGetTbHashVal(ctbName, strlen(ctbName), pDbInfo->hashMethod, pDbInfo->hashPrefix, pDbInfo->hashSuffix);
    SBlockName bln = {0};
    bln.hashValue = hashValue;
    memcpy(bln.parTbName, pDataBlock->info.parTbName, strlen(pDataBlock->info.parTbName));
    if (tSimpleHashGetSize(pTask->pNameMap) < MAX_BLOCK_NAME_NUM) {
      tSimpleHashPut(pTask->pNameMap, &groupId, sizeof(int64_t), &bln, sizeof(SBlockName));
    }
  }

  bool found = false;
  // TODO: optimize search
  int32_t j;
  for (j = 0; j < vgSz; j++) {
    SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, j);
    ASSERT(pVgInfo->vgId > 0);

    if (hashValue >= pVgInfo->hashBegin && hashValue <= pVgInfo->hashEnd) {
      if (streamAddBlockIntoDispatchMsg(pDataBlock, &pReqs[j]) < 0) {
        return -1;
      }

      if (pReqs[j].blockNum == 0) {
        atomic_add_fetch_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt, 1);
      }

      pReqs[j].blockNum++;
      found = true;
      break;
    }
  }
  ASSERT(found);
  return 0;
}

int32_t streamDispatchStreamBlock(SStreamTask* pTask) {
  ASSERT((pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH ||
          pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH));

  const char* id = pTask->id.idStr;
  int32_t     numOfElems = streamQueueGetNumOfItems(pTask->outputq.queue);
  if (numOfElems > 0) {
    double size = SIZE_IN_MiB(taosQueueMemorySize(pTask->outputq.queue->pQueue));
    stDebug("s-task:%s start to dispatch intermediate block to downstream, elem in outputQ:%d, size:%.2fMiB", id,
            numOfElems, size);
  }

  // to make sure only one dispatch is running
  int8_t old =
      atomic_val_compare_exchange_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL, TASK_OUTPUT_STATUS__WAIT);
  if (old != TASK_OUTPUT_STATUS__NORMAL) {
    stDebug("s-task:%s wait for dispatch rsp, not dispatch now, output status:%d", id, old);
    return 0;
  }

  if (pTask->chkInfo.dispatchCheckpointTrigger) {
    stDebug("s-task:%s already send checkpoint trigger, not dispatch anymore", id);
    atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);
    return 0;
  }

  ASSERT(pTask->msgInfo.pData == NULL);
  stDebug("s-task:%s start to dispatch msg, set output status:%d", id, pTask->outputq.status);

  SStreamDataBlock* pBlock = streamQueueNextItem(pTask->outputq.queue);
  if (pBlock == NULL) {
    atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);
    stDebug("s-task:%s not dispatch since no elems in outputQ, output status:%d", id, pTask->outputq.status);
    return 0;
  }

  ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK || pBlock->type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
         pBlock->type == STREAM_INPUT__TRANS_STATE);

  pTask->execInfo.dispatch += 1;
  pTask->msgInfo.startTs = taosGetTimestampMs();

  int32_t code = doBuildDispatchMsg(pTask, pBlock);
  if (code == 0) {
    destroyStreamDataBlock(pBlock);
  } else {  // todo handle build dispatch msg failed
  }

  int32_t retryCount = 0;
  while (1) {
    code = sendDispatchMsg(pTask, pTask->msgInfo.pData);
    if (code == TSDB_CODE_SUCCESS) {
      break;
    }

    stDebug("s-task:%s failed to dispatch msg:%d to downstream, code:%s, output status:%d, retry cnt:%d", id,
            pTask->execInfo.dispatch, tstrerror(terrno), pTask->outputq.status, retryCount);

    // todo deal with only partially success dispatch case
    atomic_store_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt, 0);
    if (terrno == TSDB_CODE_APP_IS_STOPPING) {  // in case of this error, do not retry anymore
      destroyDispatchMsg(pTask->msgInfo.pData, getNumOfDispatchBranch(pTask));
      pTask->msgInfo.pData = NULL;
      return code;
    }

    if (++retryCount > MAX_CONTINUE_RETRY_COUNT) {  // add to timer to retry
      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug(
          "s-task:%s failed to dispatch msg to downstream for %d times, code:%s, add timer to retry in %dms, ref:%d",
          pTask->id.idStr, retryCount, tstrerror(terrno), DISPATCH_RETRY_INTERVAL_MS, ref);

      streamRetryDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS);
      break;
    }
  }

  // this block can not be deleted until it has been sent to downstream task successfully.
  return TSDB_CODE_SUCCESS;
}

// this function is usually invoked by sink/agg task
int32_t streamTaskSendCheckpointReadyMsg(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->pReadyMsgList);
  ASSERT(taosArrayGetSize(pTask->upstreamInfo.pList) == num);

  for (int32_t i = 0; i < num; ++i) {
    SStreamChkptReadyInfo* pInfo = taosArrayGet(pTask->pReadyMsgList, i);
    tmsgSendReq(&pInfo->upstreamNodeEpset, &pInfo->msg);

    stDebug("s-task:%s level:%d checkpoint ready msg sent to upstream:0x%x", pTask->id.idStr, pTask->info.taskLevel,
            pInfo->upStreamTaskId);
  }

  taosArrayClear(pTask->pReadyMsgList);
  stDebug("s-task:%s level:%d checkpoint ready msg sent to all %d upstreams", pTask->id.idStr, pTask->info.taskLevel,
          num);

  return TSDB_CODE_SUCCESS;
}

// this function is only invoked by source task, and send rsp to mnode
int32_t streamTaskSendCheckpointSourceRsp(SStreamTask* pTask) {
  taosThreadMutexLock(&pTask->lock);

  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE);

  if (taosArrayGetSize(pTask->pReadyMsgList) == 1) {
    SStreamChkptReadyInfo* pInfo = taosArrayGet(pTask->pReadyMsgList, 0);
    tmsgSendRsp(&pInfo->msg);

    taosArrayClear(pTask->pReadyMsgList);
    stDebug("s-task:%s level:%d source checkpoint completed msg sent to mnode", pTask->id.idStr, pTask->info.taskLevel);
  } else {
    stDebug("s-task:%s level:%d already send rsp checkpoint success to mnode", pTask->id.idStr, pTask->info.taskLevel);
  }

  taosThreadMutexUnlock(&pTask->lock);
  return TSDB_CODE_SUCCESS;
}

int32_t streamAddBlockIntoDispatchMsg(const SSDataBlock* pBlock, SStreamDispatchReq* pReq) {
  int32_t dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
  void*   buf = taosMemoryCalloc(1, dataStrLen);
  if (buf == NULL) return -1;

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

  int32_t actualLen = blockEncode(pBlock, pRetrieve->data, numOfCols);
  actualLen += sizeof(SRetrieveTableRsp);
  ASSERT(actualLen <= dataStrLen);
  taosArrayPush(pReq->dataLen, &actualLen);
  taosArrayPush(pReq->data, &buf);

  pReq->totalLen += dataStrLen;
  return 0;
}

int32_t doSendDispatchMsg(SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t vgId, SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  // serialize
  int32_t tlen;
  tEncodeSize(tEncodeStreamDispatchReq, pReq, tlen, code);
  if (code < 0) {
    goto FAIL;
  }

  code = -1;
  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    goto FAIL;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamDispatchReq(&encoder, pReq)) < 0) {
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

  return code;
}

int32_t buildCheckpointSourceRsp(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SRpcMsg* pMsg,
                                 int8_t isSucceed) {
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
      .success = isSucceed,
  };

  tEncodeSize(tEncodeStreamCheckpointSourceRsp, &rsp, len, code);
  if (code < 0) {
    return code;
  }

  void* pBuf = rpcMallocCont(sizeof(SMsgHead) + len);
  if (pBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)pBuf)->vgId = htonl(pReq->mnodeId);
  void* abuf = POINTER_SHIFT(pBuf, sizeof(SMsgHead));

  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  tEncodeStreamCheckpointSourceRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  initRpcMsg(pMsg, 0, pBuf, sizeof(SMsgHead) + len);
  pMsg->info = *pRpcInfo;
  return 0;
}

int32_t streamAddCheckpointSourceRspMsg(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo, SStreamTask* pTask,
                                        int8_t isSucceed) {
  SStreamChkptReadyInfo info = {0};
  buildCheckpointSourceRsp(pReq, pRpcInfo, &info.msg, isSucceed);

  if (pTask->pReadyMsgList == NULL) {
    pTask->pReadyMsgList = taosArrayInit(4, sizeof(SStreamChkptReadyInfo));
  }

  taosArrayPush(pTask->pReadyMsgList, &info);
  stDebug("s-task:%s add checkpoint source rsp msg, total:%d", pTask->id.idStr,
          (int32_t)taosArrayGetSize(pTask->pReadyMsgList));
  return TSDB_CODE_SUCCESS;
}

int32_t streamAddCheckpointReadyMsg(SStreamTask* pTask, int32_t upstreamTaskId, int32_t index, int64_t checkpointId) {
  int32_t code = 0;
  int32_t tlen = 0;
  void*   buf = NULL;
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    return TSDB_CODE_SUCCESS;
  }

  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, upstreamTaskId);

  SStreamCheckpointReadyMsg req = {0};
  req.downstreamNodeId = pTask->pMeta->vgId;
  req.downstreamTaskId = pTask->id.taskId;
  req.streamId = pTask->id.streamId;
  req.checkpointId = checkpointId;
  req.childId = pInfo->childId;
  req.upstreamNodeId = pInfo->nodeId;
  req.upstreamTaskId = pInfo->taskId;

  tEncodeSize(tEncodeStreamCheckpointReadyMsg, &req, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMsgHead*)buf)->vgId = htonl(req.upstreamNodeId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamCheckpointReadyMsg(&encoder, &req)) < 0) {
    rpcFreeCont(buf);
    return code;
  }
  tEncoderClear(&encoder);

  ASSERT(req.upstreamTaskId != 0);

  SStreamChkptReadyInfo info = {.upStreamTaskId = pInfo->taskId, .upstreamNodeEpset = pInfo->epSet};
  initRpcMsg(&info.msg, TDMT_STREAM_TASK_CHECKPOINT_READY, buf, tlen + sizeof(SMsgHead));

  stDebug("s-task:%s (level:%d) prepare checkpoint ready msg to upstream s-task:0x%" PRIx64
          ":0x%x (vgId:%d) idx:%d, vgId:%d",
          pTask->id.idStr, pTask->info.taskLevel, req.streamId, req.upstreamTaskId, req.upstreamNodeId, index,
          req.upstreamNodeId);

  if (pTask->pReadyMsgList == NULL) {
    pTask->pReadyMsgList = taosArrayInit(4, sizeof(SStreamChkptReadyInfo));
  }

  taosArrayPush(pTask->pReadyMsgList, &info);
  return 0;
}

void streamClearChkptReadyMsg(SStreamTask* pTask) {
  if (pTask->pReadyMsgList == NULL) {
    return;
  }

  for (int i = 0; i < taosArrayGetSize(pTask->pReadyMsgList); i++) {
    SStreamChkptReadyInfo* pInfo = taosArrayGet(pTask->pReadyMsgList, i);
    rpcFreeCont(pInfo->msg.pCont);
  }
  taosArrayClear(pTask->pReadyMsgList);
}

// this message has been sent successfully, let's try next one.
static int32_t handleDispatchSuccessRsp(SStreamTask* pTask, int32_t downstreamId) {
  stDebug("s-task:%s destroy dispatch msg:%p", pTask->id.idStr, pTask->msgInfo.pData);
  destroyDispatchMsg(pTask->msgInfo.pData, getNumOfDispatchBranch(pTask));

  bool delayDispatch = (pTask->msgInfo.dispatchMsgType == STREAM_INPUT__CHECKPOINT_TRIGGER);
  if (delayDispatch) {
    pTask->chkInfo.dispatchCheckpointTrigger = true;
  }

  pTask->msgInfo.pData = NULL;
  pTask->msgInfo.dispatchMsgType = 0;

  int64_t el = taosGetTimestampMs() - pTask->msgInfo.startTs;

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
    streamDispatchStreamBlock(pTask);
  }

  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code) {
  const char* id = pTask->id.idStr;
  int32_t     vgId = pTask->pMeta->vgId;
  int32_t     msgId = pTask->execInfo.dispatch;

#if 0
  // for test purpose, build  the failure case
  if (pTask->msgInfo.dispatchMsgType == STREAM_INPUT__CHECKPOINT_TRIGGER) {
    pRsp->inputStatus = TASK_INPUT_STATUS__REFUSED;
  }
#endif

  // follower not handle the dispatch rsp
  if ((pTask->pMeta->role == NODE_ROLE_FOLLOWER) || (pTask->status.downstreamReady != 1)) {
    stError("s-task:%s vgId:%d is follower or task just re-launched, not handle the dispatch rsp, discard it", id,
            vgId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  // discard invalid dispatch rsp msg
  if ((pRsp->msgId != msgId) || (pRsp->stage != pTask->pMeta->stage)) {
    stError("s-task:%s vgId:%d not expect rsp, expected: msgId:%d, stage:%" PRId64 " actual msgId:%d, stage:%" PRId64
            " discard it",
            id, vgId, msgId, pTask->pMeta->stage, pRsp->msgId, pRsp->stage);
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
    } else {
      stError("s-task:%s failed to dispatch msgId:%d to task:0x%x(vgId:%d), code:%s, add to retry list", id, msgId,
              pRsp->downstreamTaskId, pRsp->downstreamNodeId, tstrerror(code));
      taosThreadMutexLock(&pTask->lock);
      taosArrayPush(pTask->msgInfo.pRetryList, &pRsp->downstreamNodeId);
      taosThreadMutexUnlock(&pTask->lock);
    }

  } else {  // code == 0
    if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
      pTask->inputq.status = TASK_INPUT_STATUS__BLOCKED;
      // block the input of current task, to push pressure to upstream
      taosThreadMutexLock(&pTask->lock);
      taosArrayPush(pTask->msgInfo.pRetryList, &pRsp->downstreamNodeId);
      taosThreadMutexUnlock(&pTask->lock);

      stWarn("s-task:%s inputQ of downstream task:0x%x(vgId:%d) is full, wait for %dms and retry dispatch", id,
             pRsp->downstreamTaskId, pRsp->downstreamNodeId, DISPATCH_RETRY_INTERVAL_MS);
    } else if (pRsp->inputStatus == TASK_INPUT_STATUS__REFUSED) {
      // todo handle the agg task failure, add test case
      if (pTask->msgInfo.dispatchMsgType == STREAM_INPUT__CHECKPOINT_TRIGGER &&
          pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
        stError("s-task:%s failed to dispatch checkpoint-trigger msg, checkpointId:%" PRId64
                ", set the current checkpoint failed, and send rsp to mnode",
                id, pTask->chkInfo.checkpointingId);
        { // send checkpoint failure msg to mnode directly
          pTask->chkInfo.failedId = pTask->chkInfo.checkpointingId;   // record the latest failed checkpoint id
          pTask->chkInfo.checkpointingId = pTask->chkInfo.checkpointingId;
          streamTaskSendCheckpointSourceRsp(pTask);
        }
      } else {
        stError("s-task:%s downstream task:0x%x(vgId:%d) refused the dispatch msg, treat it as success", id,
                pRsp->downstreamTaskId, pRsp->downstreamNodeId);
      }
    }
  }

  int32_t leftRsp = 0;
  if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    leftRsp = atomic_sub_fetch_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt, 1);
    ASSERT(leftRsp >= 0);

    if (leftRsp > 0) {
      stDebug(
          "s-task:%s recv dispatch rsp, msgId:%d from 0x%x(vgId:%d), downstream task input status:%d code:%s, waiting "
          "for %d rsp",
          id, msgId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->inputStatus, tstrerror(code), leftRsp);
    } else {
      stDebug(
          "s-task:%s recv dispatch rsp, msgId:%d from 0x%x(vgId:%d), downstream task input status:%d code:%s, all rsp",
          id, msgId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->inputStatus, tstrerror(code));
    }
  } else {
    stDebug("s-task:%s recv fix-dispatch rsp, msgId:%d from 0x%x(vgId:%d), downstream task input status:%d code:%s", id,
            msgId, pRsp->downstreamTaskId, pRsp->downstreamNodeId, pRsp->inputStatus, tstrerror(code));
  }

  ASSERT(leftRsp >= 0);

  // all msg rsp already, continue
  if (leftRsp == 0) {
    ASSERT(pTask->outputq.status == TASK_OUTPUT_STATUS__WAIT);

    // we need to re-try send dispatch msg to downstream tasks
    int32_t numOfFailed = taosArrayGetSize(pTask->msgInfo.pRetryList);
    if (numOfFailed > 0) {
      if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
        atomic_store_32(&pTask->outputInfo.shuffleDispatcher.waitingRspCnt, numOfFailed);
        stDebug("s-task:%s waiting rsp set to be %d", id, pTask->outputInfo.shuffleDispatcher.waitingRspCnt);
      }

      int32_t ref = atomic_add_fetch_32(&pTask->status.timerActive, 1);
      stDebug("s-task:%s failed to dispatch msg to downstream, add into timer to retry in %dms, ref:%d",
              pTask->id.idStr, DISPATCH_RETRY_INTERVAL_MS, ref);

      streamRetryDispatchData(pTask, DISPATCH_RETRY_INTERVAL_MS);
    } else {  // this message has been sent successfully, let's try next one.
      pTask->msgInfo.retryCount = 0;

      // transtate msg has been sent to downstream successfully. let's transfer the fill-history task state
      if (pTask->msgInfo.dispatchMsgType == STREAM_INPUT__TRANS_STATE) {
        stDebug("s-task:%s dispatch transtate msgId:%d to downstream successfully, start to transfer state", id, msgId);
        ASSERT(pTask->info.fillHistory == 1);

        code = streamTransferStateToStreamTask(pTask);
        if (code != TSDB_CODE_SUCCESS) {  // todo: do nothing if error happens
        }

        // now ready for next data output
        atomic_store_8(&pTask->outputq.status, TASK_OUTPUT_STATUS__NORMAL);
      } else {
        handleDispatchSuccessRsp(pTask, pRsp->downstreamTaskId);
      }
    }
  }

  return 0;
}

int32_t tEncodeStreamTaskUpdateMsg(SEncoder* pEncoder, const SStreamTaskNodeUpdateMsg* pMsg) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pMsg->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pMsg->taskId) < 0) return -1;

  int32_t size = taosArrayGetSize(pMsg->pNodeList);
  if (tEncodeI32(pEncoder, size) < 0) return -1;

  for (int32_t i = 0; i < size; ++i) {
    SNodeUpdateInfo* pInfo = taosArrayGet(pMsg->pNodeList, i);
    if (tEncodeI32(pEncoder, pInfo->nodeId) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pInfo->prevEp) < 0) return -1;
    if (tEncodeSEpSet(pEncoder, &pInfo->newEp) < 0) return -1;
  }

  // todo this new attribute will be result in being incompatible with previous version
  if (tEncodeI32(pEncoder, pMsg->transId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeStreamTaskUpdateMsg(SDecoder* pDecoder, SStreamTaskNodeUpdateMsg* pMsg) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pMsg->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pMsg->taskId) < 0) return -1;

  int32_t size = 0;
  if (tDecodeI32(pDecoder, &size) < 0) return -1;
  pMsg->pNodeList = taosArrayInit(size, sizeof(SNodeUpdateInfo));
  for (int32_t i = 0; i < size; ++i) {
    SNodeUpdateInfo info = {0};
    if (tDecodeI32(pDecoder, &info.nodeId) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &info.prevEp) < 0) return -1;
    if (tDecodeSEpSet(pDecoder, &info.newEp) < 0) return -1;
    taosArrayPush(pMsg->pNodeList, &info);
  }

  if (tDecodeI32(pDecoder, &pMsg->transId) < 0) return -1;

  tEndDecode(pDecoder);
  return 0;
}
