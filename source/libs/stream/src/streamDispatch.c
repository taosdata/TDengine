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
#include "trpc.h"
#include "ttimer.h"
#include "tmisce.h"

#define MAX_BLOCK_NAME_NUM         1024
#define DISPATCH_RETRY_INTERVAL_MS 300
#define MAX_CONTINUE_RETRY_COUNT   5

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
static int32_t doDispatchScanHistoryFinishMsg(SStreamTask* pTask, const SStreamScanHistoryFinishReq* pReq, int32_t vgId,
                                              SEpSet* pEpSet);

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
  if (tEncodeI32(pEncoder, pReq->srcVgId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->type) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->type) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamChildId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
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
  if (tDecodeI32(pDecoder, &pReq->srcVgId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->type) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->type) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamChildId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamNodeId) < 0) return -1;
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
  pReq->upstreamTaskId = pTask->id.taskId;
  pReq->upstreamChildId = pTask->info.selfChildId;
  pReq->upstreamNodeId = pTask->info.nodeId;
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

int32_t streamBroadcastToChildren(SStreamTask* pTask, const SSDataBlock* pBlock) {
  int32_t            code = -1;
  SRetrieveTableRsp* pRetrieve = NULL;
  void*              buf = NULL;
  int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);

  pRetrieve = taosMemoryCalloc(1, dataStrLen);
  if (pRetrieve == NULL) return -1;

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

  SStreamRetrieveReq req = {
      .streamId = pTask->id.streamId,
      .srcNodeId = pTask->info.nodeId,
      .srcTaskId = pTask->id.taskId,
      .pRetrieve = pRetrieve,
      .retrieveLen = dataStrLen,
  };

  int32_t sz = taosArrayGetSize(pTask->pUpstreamInfoList);
  ASSERT(sz > 0);
  for (int32_t i = 0; i < sz; i++) {
    req.reqId = tGenIdPI64();
    SStreamChildEpInfo* pEpInfo = taosArrayGetP(pTask->pUpstreamInfoList, i);
    req.dstNodeId = pEpInfo->nodeId;
    req.dstTaskId = pEpInfo->taskId;
    int32_t len;
    tEncodeSize(tEncodeStreamRetrieveReq, &req, len, code);
    if (code < 0) {
      ASSERT(0);
      return -1;
    }

    buf = rpcMallocCont(sizeof(SMsgHead) + len);
    if (buf == NULL) {
      goto CLEAR;
    }

    ((SMsgHead*)buf)->vgId = htonl(pEpInfo->nodeId);
    void*    abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
    SEncoder encoder;
    tEncoderInit(&encoder, abuf, len);
    tEncodeStreamRetrieveReq(&encoder, &req);
    tEncoderClear(&encoder);

    SRpcMsg rpcMsg = {0};
    initRpcMsg(&rpcMsg, TDMT_STREAM_RETRIEVE, buf, len + sizeof(SMsgHead));

    if (tmsgSendReq(&pEpInfo->epSet, &rpcMsg) < 0) {
      ASSERT(0);
      goto CLEAR;
    }

    buf = NULL;
    qDebug("s-task:%s (child %d) send retrieve req to task:0x%x (vgId:%d), reqId:0x%" PRIx64, pTask->id.idStr,
           pTask->info.selfChildId, pEpInfo->taskId, pEpInfo->nodeId, req.reqId);
  }
  code = 0;

CLEAR:
  taosMemoryFree(pRetrieve);
  rpcFreeCont(buf);
  return code;
}

int32_t streamDispatchCheckMsg(SStreamTask* pTask, const SStreamTaskCheckReq* pReq, int32_t nodeId, SEpSet* pEpSet) {
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
  qDebug("s-task:%s (level:%d) send check msg to s-task:0x%" PRIx64 ":0x%x (vgId:%d)", pTask->id.idStr,
         pTask->info.taskLevel, pReq->streamId, pReq->downstreamTaskId, nodeId);

  tmsgSendReq(pEpSet, &msg);
  return 0;
}

static int32_t doDispatchAllBlocks(SStreamTask* pTask, const SStreamDataBlock* pData) {
  int32_t code = 0;
  int32_t numOfBlocks = taosArrayGetSize(pData->blocks);
  ASSERT(numOfBlocks != 0);

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    SStreamDispatchReq req = {0};

    int32_t downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    code = tInitStreamDispatchReq(&req, pTask, pData->srcVgId, numOfBlocks, downstreamTaskId, pData->type);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);

      code = streamAddBlockIntoDispatchMsg(pDataBlock, &req);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroyP(req.data, taosMemoryFree);
        taosArrayDestroy(req.dataLen);
        return code;
      }
    }

    int32_t vgId = pTask->fixedEpDispatcher.nodeId;
    SEpSet* pEpSet = &pTask->fixedEpDispatcher.epSet;

    qDebug("s-task:%s (child taskId:%d) fix-dispatch %d block(s) to s-task:0x%x (vgId:%d)", pTask->id.idStr,
           pTask->info.selfChildId, numOfBlocks, downstreamTaskId, vgId);

    code = doSendDispatchMsg(pTask, &req, vgId, pEpSet);
    taosArrayDestroyP(req.data, taosMemoryFree);
    taosArrayDestroy(req.dataLen);
    return code;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    int32_t rspCnt = atomic_load_32(&pTask->shuffleDispatcher.waitingRspCnt);
    ASSERT(rspCnt == 0);

    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t vgSz = taosArrayGetSize(vgInfo);

    SStreamDispatchReq* pReqs = taosMemoryCalloc(vgSz, sizeof(SStreamDispatchReq));
    if (pReqs == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    for (int32_t i = 0; i < vgSz; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      code = tInitStreamDispatchReq(&pReqs[i], pTask, pData->srcVgId, 0, pVgInfo->taskId, pData->type);
      if (code != TSDB_CODE_SUCCESS) {
        goto FAIL_SHUFFLE_DISPATCH;
      }
    }

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);

      // TODO: do not use broadcast
      if (pDataBlock->info.type == STREAM_DELETE_RESULT || pDataBlock->info.type == STREAM_CHECKPOINT || pDataBlock->info.type == STREAM_TRANS_STATE) {
        for (int32_t j = 0; j < vgSz; j++) {
          if (streamAddBlockIntoDispatchMsg(pDataBlock, &pReqs[j]) < 0) {
            goto FAIL_SHUFFLE_DISPATCH;
          }

          if (pReqs[j].blockNum == 0) {
            atomic_add_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
          }
          pReqs[j].blockNum++;
        }

        continue;
      }

      if (streamSearchAndAddBlock(pTask, pReqs, pDataBlock, vgSz, pDataBlock->info.id.groupId) < 0) {
        goto FAIL_SHUFFLE_DISPATCH;
      }
    }

    qDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to %d vgroups", pTask->id.idStr,
           pTask->info.selfChildId, numOfBlocks, vgSz);

    for (int32_t i = 0; i < vgSz; i++) {
      if (pReqs[i].blockNum > 0) {
        SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
        qDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to vgId:%d", pTask->id.idStr,
               pTask->info.selfChildId, pReqs[i].blockNum, pVgInfo->vgId);

        code = doSendDispatchMsg(pTask, &pReqs[i], pVgInfo->vgId, &pVgInfo->epSet);
        if (code < 0) {
          goto FAIL_SHUFFLE_DISPATCH;
        }
      }
    }

    code = 0;

    FAIL_SHUFFLE_DISPATCH:
    for (int32_t i = 0; i < vgSz; i++) {
      taosArrayDestroyP(pReqs[i].data, taosMemoryFree);
      taosArrayDestroy(pReqs[i].dataLen);
    }

    taosMemoryFree(pReqs);
  }

  return code;
}

static void doRetryDispatchData(void* param, void* tmrId) {
  SStreamTask* pTask = param;

  if (streamTaskShouldStop(&pTask->status)) {
    atomic_sub_fetch_8(&pTask->status.timerActive, 1);
    qDebug("s-task:%s should stop, abort from timer", pTask->id.idStr);
    return;
  }

  ASSERT(pTask->outputInfo.status == TASK_OUTPUT_STATUS__WAIT);

  int32_t code = doDispatchAllBlocks(pTask, pTask->msgInfo.pData);
  if (code != TSDB_CODE_SUCCESS) {
    if (!streamTaskShouldStop(&pTask->status)) {
      qDebug("s-task:%s reset the waitRspCnt to be 0 before launch retry dispatch", pTask->id.idStr);
      atomic_store_32(&pTask->shuffleDispatcher.waitingRspCnt, 0);
      if (streamTaskShouldPause(&pTask->status)) {
        streamRetryDispatchStreamBlock(pTask, DISPATCH_RETRY_INTERVAL_MS * 10);
      } else {
        streamRetryDispatchStreamBlock(pTask, DISPATCH_RETRY_INTERVAL_MS);
      }
    } else {
      atomic_sub_fetch_8(&pTask->status.timerActive, 1);
      qDebug("s-task:%s should stop, abort from timer", pTask->id.idStr);
    }
  } else {
    atomic_sub_fetch_8(&pTask->status.timerActive, 1);
  }
}

void streamRetryDispatchStreamBlock(SStreamTask* pTask, int64_t waitDuration) {
  qError("s-task:%s dispatch data in %" PRId64 "ms", pTask->id.idStr, waitDuration);
  taosTmrReset(doRetryDispatchData, waitDuration, pTask, streamEnv.timer, &pTask->launchTaskTimer);
}

int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock, int32_t vgSz,
                                int64_t groupId) {
  uint32_t hashValue = 0;
  SArray*  vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
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
    char* ctbName = taosMemoryCalloc(1, TSDB_TABLE_FNAME_LEN);
    if (ctbName == NULL) {
      return -1;
    }

    if (pDataBlock->info.parTbName[0]) {
      snprintf(ctbName, TSDB_TABLE_NAME_LEN, "%s.%s", pTask->shuffleDispatcher.dbInfo.db, pDataBlock->info.parTbName);
    } else {
      buildCtbNameByGroupIdImpl(pTask->shuffleDispatcher.stbFullName, groupId, pDataBlock->info.parTbName);
      snprintf(ctbName, TSDB_TABLE_NAME_LEN, "%s.%s", pTask->shuffleDispatcher.dbInfo.db, pDataBlock->info.parTbName);
    }

    /*uint32_t hashValue = MurmurHash3_32(ctbName, strlen(ctbName));*/
    SUseDbRsp* pDbInfo = &pTask->shuffleDispatcher.dbInfo;
    hashValue =
        taosGetTbHashVal(ctbName, strlen(ctbName), pDbInfo->hashMethod, pDbInfo->hashPrefix, pDbInfo->hashSuffix);
    taosMemoryFree(ctbName);
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
        atomic_add_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
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
  ASSERT((pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH || pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH));

  const char* id = pTask->id.idStr;
  int32_t numOfElems = streamQueueGetNumOfItems(pTask->outputInfo.queue);
  if (numOfElems > 0) {
    double size = SIZE_IN_MB(taosQueueMemorySize(pTask->outputInfo.queue->pQueue));
    qDebug("s-task:%s start to dispatch intermediate block to downstream, elem in outputQ:%d, size:%.2fMiB", id, numOfElems, size);
  }

  // to make sure only one dispatch is running
  int8_t old =
      atomic_val_compare_exchange_8(&pTask->outputInfo.status, TASK_OUTPUT_STATUS__NORMAL, TASK_OUTPUT_STATUS__WAIT);
  if (old != TASK_OUTPUT_STATUS__NORMAL) {
    qDebug("s-task:%s wait for dispatch rsp, not dispatch now, output status:%d", id, old);
    return 0;
  }

  ASSERT(pTask->msgInfo.pData == NULL);
  qDebug("s-task:%s start to dispatch msg, set output status:%d", id, pTask->outputInfo.status);

  SStreamDataBlock* pBlock = streamQueueNextItem(pTask->outputInfo.queue);
  if (pBlock == NULL) {
    atomic_store_8(&pTask->outputInfo.status, TASK_OUTPUT_STATUS__NORMAL);
    qDebug("s-task:%s not dispatch since no elems in outputQ, output status:%d", id, pTask->outputInfo.status);
    return 0;
  }

  pTask->msgInfo.pData = pBlock;
  ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK || pBlock->type == STREAM_INPUT__CHECKPOINT_TRIGGER ||
         pBlock->type == STREAM_INPUT__TRANS_STATE);

  int32_t retryCount = 0;

  while (1) {
    int32_t code = doDispatchAllBlocks(pTask, pBlock);
    if (code == TSDB_CODE_SUCCESS) {
      break;
    }

    qDebug("s-task:%s failed to dispatch msg to downstream, code:%s, output status:%d, retry cnt:%d", id,
           tstrerror(terrno), pTask->outputInfo.status, retryCount);

    // todo deal with only partially success dispatch case
    atomic_store_32(&pTask->shuffleDispatcher.waitingRspCnt, 0);
    if (terrno == TSDB_CODE_APP_IS_STOPPING) {  // in case of this error, do not retry anymore
      destroyStreamDataBlock(pTask->msgInfo.pData);
      pTask->msgInfo.pData = NULL;
      return code;
    }

    if (++retryCount > MAX_CONTINUE_RETRY_COUNT) {  // add to timer to retry
      qDebug("s-task:%s failed to dispatch msg to downstream for %d times, code:%s, add timer to retry in %dms",
             pTask->id.idStr, retryCount, tstrerror(terrno), DISPATCH_RETRY_INTERVAL_MS);
      streamRetryDispatchStreamBlock(pTask, DISPATCH_RETRY_INTERVAL_MS);
      break;
    }
  }

  // this block can not be deleted until it has been sent to downstream task successfully.
  return TSDB_CODE_SUCCESS;
}

int32_t streamDispatchScanHistoryFinishMsg(SStreamTask* pTask) {
  SStreamScanHistoryFinishReq req = {
      .streamId = pTask->id.streamId,
      .childId = pTask->info.selfChildId,
      .upstreamTaskId = pTask->id.taskId,
      .upstreamNodeId = pTask->pMeta->vgId,
  };

  // serialize
  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    req.downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    pTask->notReadyTasks = 1;
    doDispatchScanHistoryFinishMsg(pTask, &req, pTask->fixedEpDispatcher.nodeId, &pTask->fixedEpDispatcher.epSet);
  } else if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
    int32_t numOfVgs = taosArrayGetSize(vgInfo);
    pTask->notReadyTasks = numOfVgs;

    qDebug("s-task:%s send scan-history data complete msg to downstream (shuffle-dispatch) %d tasks, status:%s", pTask->id.idStr,
           numOfVgs, streamGetTaskStatusStr(pTask->status.taskStatus));
    for (int32_t i = 0; i < numOfVgs; i++) {
      SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
      req.downstreamTaskId = pVgInfo->taskId;
      doDispatchScanHistoryFinishMsg(pTask, &req, pVgInfo->vgId, &pVgInfo->epSet);
    }
  } else {
    qDebug("s-task:%s no downstream tasks, invoke scan-history finish rsp directly", pTask->id.idStr);
    streamProcessScanHistoryFinishRsp(pTask);
  }

  return 0;
}

// this function is usually invoked by sink/agg task
int32_t streamTaskSendCheckpointReadyMsg(SStreamTask* pTask) {
  int32_t num = taosArrayGetSize(pTask->pReadyMsgList);
  ASSERT(taosArrayGetSize(pTask->pUpstreamInfoList) == num);

  for (int32_t i = 0; i < num; ++i) {
    SStreamChkptReadyInfo* pInfo = taosArrayGet(pTask->pReadyMsgList, i);
    tmsgSendReq(&pInfo->upstreamNodeEpset, &pInfo->msg);

    qDebug("s-task:%s level:%d checkpoint ready msg sent to upstream:0x%x", pTask->id.idStr, pTask->info.taskLevel,
           pInfo->upStreamTaskId);
  }

  taosArrayClear(pTask->pReadyMsgList);
  qDebug("s-task:%s level:%d checkpoint ready msg sent to all %d upstreams", pTask->id.idStr, pTask->info.taskLevel, num);

  return TSDB_CODE_SUCCESS;
}

// this function is only invoked by source task, and send rsp to mnode
int32_t streamTaskSendCheckpointSourceRsp(SStreamTask* pTask) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__SOURCE && taosArrayGetSize(pTask->pReadyMsgList) == 1);
  SStreamChkptReadyInfo* pInfo = taosArrayGet(pTask->pReadyMsgList, 0);

  tmsgSendRsp(&pInfo->msg);

  taosArrayClear(pTask->pReadyMsgList);
  qDebug("s-task:%s level:%d source checkpoint completed msg sent to mnode", pTask->id.idStr, pTask->info.taskLevel);

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

int32_t doDispatchScanHistoryFinishMsg(SStreamTask* pTask, const SStreamScanHistoryFinishReq* pReq, int32_t vgId,
                                       SEpSet* pEpSet) {
  void*   buf = NULL;
  int32_t code = -1;
  SRpcMsg msg = {0};

  int32_t tlen;
  tEncodeSize(tEncodeStreamScanHistoryFinishReq, pReq, tlen, code);
  if (code < 0) {
    return -1;
  }

  buf = rpcMallocCont(sizeof(SMsgHead) + tlen);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  ((SMsgHead*)buf)->vgId = htonl(vgId);
  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));

  SEncoder encoder;
  tEncoderInit(&encoder, abuf, tlen);
  if ((code = tEncodeStreamScanHistoryFinishReq(&encoder, pReq)) < 0) {
    if (buf) {
      rpcFreeCont(buf);
    }
    return code;
  }

  tEncoderClear(&encoder);

  initRpcMsg(&msg, TDMT_VND_STREAM_SCAN_HISTORY_FINISH, buf, tlen + sizeof(SMsgHead));

  tmsgSendReq(pEpSet, &msg);
  const char* pStatus = streamGetTaskStatusStr(pTask->status.taskStatus);
  qDebug("s-task:%s status:%s dispatch scan-history finish msg to taskId:0x%x (vgId:%d)", pTask->id.idStr, pStatus,
         pReq->downstreamTaskId, vgId);
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
  qDebug("s-task:%s dispatch msg to taskId:0x%x vgId:%d data msg", pTask->id.idStr, pReq->taskId, vgId);

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

int32_t streamAddCheckpointSourceRspMsg(SStreamCheckpointSourceReq* pReq, SRpcHandleInfo* pRpcInfo,
                                        SStreamTask* pTask, int8_t isSucceed) {
  SStreamChkptReadyInfo info = {0};
  buildCheckpointSourceRsp(pReq, pRpcInfo, &info.msg, isSucceed);

  if (pTask->pReadyMsgList == NULL) {
    pTask->pReadyMsgList = taosArrayInit(4, sizeof(SStreamChkptReadyInfo));
  }

  taosArrayPush(pTask->pReadyMsgList, &info);
  qDebug("s-task:%s add checkpoint source rsp msg, total:%d", pTask->id.idStr, (int32_t)taosArrayGetSize(pTask->pReadyMsgList));
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
  info.msg.info.noResp = 1;  // refactor later.

  qDebug("s-task:%s (level:%d) prepare checkpoint ready msg to upstream s-task:0x%" PRIx64 ":0x%x (vgId:%d) idx:%d",
         pTask->id.idStr, pTask->info.taskLevel, req.streamId, req.upstreamTaskId, req.downstreamNodeId, index);

  if (pTask->pReadyMsgList == NULL) {
    pTask->pReadyMsgList = taosArrayInit(4, sizeof(SStreamChkptReadyInfo));
  }

  taosArrayPush(pTask->pReadyMsgList, &info);
  return 0;
}

int32_t tEncodeCompleteHistoryDataMsg(SEncoder* pEncoder, const SStreamCompleteHistoryMsg* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->downstreamNode) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamNodeId) < 0) return -1;
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeCompleteHistoryDataMsg(SDecoder* pDecoder, SStreamCompleteHistoryMsg* pRsp) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pRsp->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->downstreamNode) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pRsp->upstreamNodeId) < 0) return -1;
  tEndDecode(pDecoder);
  return 0;
}

int32_t streamTaskBuildScanhistoryRspMsg(SStreamTask* pTask, SStreamScanHistoryFinishReq* pReq, void** pBuffer, int32_t* pLen) {
  int32_t  len = 0;
  int32_t  code = 0;
  SEncoder encoder;

  SStreamCompleteHistoryMsg msg = {
      .streamId = pReq->streamId,
      .upstreamTaskId = pReq->upstreamTaskId,
      .upstreamNodeId = pReq->upstreamNodeId,
      .downstreamId = pReq->downstreamTaskId,
      .downstreamNode = pTask->pMeta->vgId,
  };

  tEncodeSize(tEncodeCompleteHistoryDataMsg, &msg, len, code);
  if (code < 0) {
    return code;
  }

  void* pBuf = rpcMallocCont(sizeof(SMsgHead) + len);
  if (pBuf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  ((SMsgHead*)pBuf)->vgId = htonl(pReq->upstreamNodeId);

  void* abuf = POINTER_SHIFT(pBuf, sizeof(SMsgHead));

  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  tEncodeCompleteHistoryDataMsg(&encoder, &msg);
  tEncoderClear(&encoder);

  *pBuffer = pBuf;
  *pLen = len;
  return 0;
}

int32_t streamAddEndScanHistoryMsg(SStreamTask* pTask, SRpcHandleInfo* pRpcInfo, SStreamScanHistoryFinishReq* pReq) {
  void*   pBuf = NULL;
  int32_t len = 0;

  streamTaskBuildScanhistoryRspMsg(pTask, pReq, &pBuf, &len);
  SStreamChildEpInfo* pInfo = streamTaskGetUpstreamTaskEpInfo(pTask, pReq->upstreamTaskId);

  SStreamContinueExecInfo info = {.taskId = pReq->upstreamTaskId, .epset = pInfo->epSet};
  initRpcMsg(&info.msg, 0, pBuf, sizeof(SMsgHead) + len);
  info.msg.info = *pRpcInfo;

  taosThreadMutexLock(&pTask->lock);
  if (pTask->pRspMsgList == NULL) {
    pTask->pRspMsgList = taosArrayInit(4, sizeof(SStreamContinueExecInfo));
  }
  taosArrayPush(pTask->pRspMsgList, &info);
  taosThreadMutexUnlock(&pTask->lock);

  int32_t num = taosArrayGetSize(pTask->pRspMsgList);
  qDebug("s-task:%s add scan history finish rsp msg for task:0x%x, total:%d", pTask->id.idStr, pReq->upstreamTaskId,
         num);
  return TSDB_CODE_SUCCESS;
}

int32_t streamNotifyUpstreamContinue(SStreamTask* pTask) {
  ASSERT(pTask->info.taskLevel == TASK_LEVEL__AGG || pTask->info.taskLevel == TASK_LEVEL__SINK);

  int32_t num = taosArrayGetSize(pTask->pRspMsgList);
  for (int32_t i = 0; i < num; ++i) {
    SStreamContinueExecInfo* pInfo = taosArrayGet(pTask->pRspMsgList, i);
    tmsgSendRsp(&pInfo->msg);

    qDebug("s-task:%s level:%d notify upstream:0x%x to continue process data in WAL", pTask->id.idStr, pTask->info.taskLevel,
           pInfo->taskId);
  }

  taosArrayClear(pTask->pRspMsgList);
  qDebug("s-task:%s level:%d checkpoint ready msg sent to all %d upstreams", pTask->id.idStr, pTask->info.taskLevel,
         num);
  return 0;
}

int32_t streamProcessDispatchRsp(SStreamTask* pTask, SStreamDispatchRsp* pRsp, int32_t code) {
  const char* id = pTask->id.idStr;

  if (code != TSDB_CODE_SUCCESS) {
    // dispatch message failed: network error, or node not available.
    // in case of the input queue is full, the code will be TSDB_CODE_SUCCESS, the and pRsp>inputStatus will be set
    // flag. here we need to retry dispatch this message to downstream task immediately. handle the case the failure
    // happened too fast.
    // todo handle the shuffle dispatch failure
    if (code == TSDB_CODE_STREAM_TASK_NOT_EXIST) { // destination task does not exist, not retry anymore
      qWarn("s-task:%s failed to dispatch msg to task:0x%x, no retry, since it is destroyed already", id, pRsp->downstreamTaskId);
    } else {
      qError("s-task:%s failed to dispatch msg to task:0x%x, code:%s, retry cnt:%d", id, pRsp->downstreamTaskId,
             tstrerror(code), ++pTask->msgInfo.retryCount);
      int32_t ret = doDispatchAllBlocks(pTask, pTask->msgInfo.pData);
      if (ret != TSDB_CODE_SUCCESS) {
      }
    }

    return TSDB_CODE_SUCCESS;
  }

  qDebug("s-task:%s recv dispatch rsp from 0x%x, downstream task input status:%d code:%d", id, pRsp->downstreamTaskId,
         pRsp->inputStatus, code);

  // there are other dispatch message not response yet
  if (pTask->outputInfo.type == TASK_OUTPUT__SHUFFLE_DISPATCH) {
    int32_t leftRsp = atomic_sub_fetch_32(&pTask->shuffleDispatcher.waitingRspCnt, 1);
    qDebug("s-task:%s is shuffle, left waiting rsp %d", id, leftRsp);
    if (leftRsp > 0) {
      return 0;
    }
  }

  // transtate msg has been sent to downstream successfully. let's transfer the fill-history task state
  SStreamDataBlock* p = pTask->msgInfo.pData;
  if (p->type == STREAM_INPUT__TRANS_STATE) {
    qDebug("s-task:%s dispatch transtate msg to downstream successfully, start to transfer state", id);
    ASSERT(pTask->info.fillHistory == 1);
    code = streamTransferStateToStreamTask(pTask);
    if (code != TSDB_CODE_SUCCESS) {  // todo: do nothing if error happens
    }

    return TSDB_CODE_SUCCESS;
  }

  pTask->msgInfo.retryCount = 0;
  ASSERT(pTask->outputInfo.status == TASK_OUTPUT_STATUS__WAIT);

  qDebug("s-task:%s output status is set to:%d", id, pTask->outputInfo.status);

  // the input queue of the (down stream) task that receive the output data is full,
  // so the TASK_INPUT_STATUS_BLOCKED is rsp
  if (pRsp->inputStatus == TASK_INPUT_STATUS__BLOCKED) {
    pTask->inputInfo.status = TASK_INPUT_STATUS__BLOCKED;   // block the input of current task, to push pressure to upstream
    double el = 0;
    if (pTask->msgInfo.blockingTs == 0) {
      pTask->msgInfo.blockingTs = taosGetTimestampMs();  // record the blocking start time
    } else {
      el = (taosGetTimestampMs() - pTask->msgInfo.blockingTs) / 1000.0;
    }

    int8_t ref = atomic_add_fetch_8(&pTask->status.timerActive, 1);
    qError("s-task:%s inputQ of downstream task:0x%x is full, time:%" PRId64
           " wait for %dms and retry dispatch data, total wait:%.2fSec ref:%d",
           id, pRsp->downstreamTaskId, pTask->msgInfo.blockingTs, DISPATCH_RETRY_INTERVAL_MS, el, ref);
    streamRetryDispatchStreamBlock(pTask, DISPATCH_RETRY_INTERVAL_MS);
  } else {  // pipeline send data in output queue
    // this message has been sent successfully, let's try next one.
    destroyStreamDataBlock(pTask->msgInfo.pData);
    pTask->msgInfo.pData = NULL;

    if (pTask->msgInfo.blockingTs != 0) {
      int64_t el = taosGetTimestampMs() - pTask->msgInfo.blockingTs;
      qDebug("s-task:%s downstream task:0x%x resume to normal from inputQ blocking, blocking time:%" PRId64 "ms", id,
             pRsp->downstreamTaskId, el);
      pTask->msgInfo.blockingTs = 0;

      // put data into inputQ of current task is also allowed
      pTask->inputInfo.status = TASK_INPUT_STATUS__NORMAL;
    }

    // now ready for next data output
    atomic_store_8(&pTask->outputInfo.status, TASK_OUTPUT_STATUS__NORMAL);

    // otherwise, continue dispatch the first block to down stream task in pipeline
    streamDispatchStreamBlock(pTask);
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

  tEndDecode(pDecoder);
  return 0;
}
