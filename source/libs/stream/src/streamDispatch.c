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
#include "ttimer.h"

#define MAX_BLOCK_NAME_NUM         1024
#define DISPATCH_RETRY_INTERVAL_MS 300
#define MAX_CONTINUE_RETRY_COUNT   5

typedef struct SBlockName {
  uint32_t hashValue;
  char     parTbName[TSDB_TABLE_NAME_LEN];
} SBlockName;

static void initRpcMsg(SRpcMsg* pMsg, int32_t msgType, void* pCont, int32_t contLen) {
    pMsg->msgType = msgType;
    pMsg->pCont = pCont;
    pMsg->contLen = contLen;
}

static int32_t tEncodeStreamDispatchReq(SEncoder* pEncoder, const SStreamDispatchReq* pReq) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeI64(pEncoder, pReq->streamId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->taskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->upstreamTaskId) < 0) return -1;
  if (tEncodeI32(pEncoder, pReq->dataSrcVgId) < 0) return -1;
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

static int32_t streamAddBlockIntoDispatchMsg(const SSDataBlock* pBlock, SStreamDispatchReq* pReq) {
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

int32_t tDecodeStreamDispatchReq(SDecoder* pDecoder, SStreamDispatchReq* pReq) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeI64(pDecoder, &pReq->streamId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->taskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->upstreamTaskId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pReq->dataSrcVgId) < 0) return -1;
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

int32_t tInitStreamDispatchReq(SStreamDispatchReq* pReq, const SStreamTask* pTask, int32_t vgId, int32_t numOfBlocks,
                               int64_t dstTaskId) {
  pReq->streamId = pTask->id.streamId;
  pReq->dataSrcVgId = vgId;
  pReq->upstreamTaskId = pTask->id.taskId;
  pReq->upstreamChildId = pTask->info.selfChildId;
  pReq->upstreamNodeId = pTask->info.nodeId;
  pReq->blockNum = numOfBlocks;
  pReq->taskId = dstTaskId;

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

  int32_t sz = taosArrayGetSize(pTask->pUpstreamEpInfoList);
  ASSERT(sz > 0);
  for (int32_t i = 0; i < sz; i++) {
    req.reqId = tGenIdPI64();
    SStreamChildEpInfo* pEpInfo = taosArrayGetP(pTask->pUpstreamEpInfoList, i);
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

    SRpcMsg rpcMsg = {.code = 0, .msgType = TDMT_STREAM_RETRIEVE, .pCont = buf, .contLen = sizeof(SMsgHead) + len};
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

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = TDMT_STREAM_TASK_CHECK;

  qDebug("s-task:%s (level:%d) dispatch check msg to s-task:%" PRIx64 ":0x%x (vgId:%d)", pTask->id.idStr,
         pTask->info.taskLevel, pReq->streamId, pReq->downstreamTaskId, nodeId);

  tmsgSendReq(pEpSet, &msg);
  return 0;
}

int32_t streamDoDispatchScanHistoryFinishMsg(SStreamTask* pTask, const SStreamScanHistoryFinishReq* pReq, int32_t vgId,
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

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = TDMT_STREAM_SCAN_HISTORY_FINISH;

  tmsgSendReq(pEpSet, &msg);

  const char* pStatus = streamGetTaskStatusStr(pTask->status.taskStatus);
  qDebug("s-task:%s status:%s dispatch scan-history finish msg to taskId:0x%x (vgId:%d)", pTask->id.idStr, pStatus,
         pReq->downstreamTaskId, vgId);
  return 0;
}

static int32_t doSendDispatchMsg(SStreamTask* pTask, const SStreamDispatchReq* pReq, int32_t vgId, SEpSet* pEpSet) {
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

  msg.contLen = tlen + sizeof(SMsgHead);
  msg.pCont = buf;
  msg.msgType = pTask->msgInfo.msgType;

  qDebug("s-task:%s dispatch msg to taskId:0x%x vgId:%d data msg", pTask->id.idStr, pReq->taskId, vgId);
  return tmsgSendReq(pEpSet, &msg);

FAIL:
  if (buf) {
    rpcFreeCont(buf);
  }

  return code;
}

int32_t streamSearchAndAddBlock(SStreamTask* pTask, SStreamDispatchReq* pReqs, SSDataBlock* pDataBlock, int32_t vgSz,
                                int64_t groupId) {
  uint32_t   hashValue = 0;
  SArray* vgInfo = pTask->shuffleDispatcher.dbInfo.pVgroupInfos;
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

int32_t streamDispatchAllBlocks(SStreamTask* pTask, const SStreamDataBlock* pData) {
  int32_t code = 0;

  int32_t numOfBlocks = taosArrayGetSize(pData->blocks);
  ASSERT(numOfBlocks != 0);

  if (pTask->outputInfo.type == TASK_OUTPUT__FIXED_DISPATCH) {
    SStreamDispatchReq req = {0};

    int32_t downstreamTaskId = pTask->fixedEpDispatcher.taskId;
    code = tInitStreamDispatchReq(&req, pTask, pData->srcVgId, numOfBlocks, downstreamTaskId);
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
      code = tInitStreamDispatchReq(&pReqs[i], pTask, pData->srcVgId, 0, pVgInfo->taskId);
      if (code != TSDB_CODE_SUCCESS) {
        goto FAIL_SHUFFLE_DISPATCH;
      }
    }

    for (int32_t i = 0; i < numOfBlocks; i++) {
      SSDataBlock* pDataBlock = taosArrayGet(pData->blocks, i);

      // TODO: do not use broadcast
      if (pDataBlock->info.type == STREAM_DELETE_RESULT) {

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

    qDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to %d vgroups", pTask->id.idStr, pTask->info.selfChildId,
           numOfBlocks, vgSz);

    for (int32_t i = 0; i < vgSz; i++) {
      if (pReqs[i].blockNum > 0) {
        SVgroupInfo* pVgInfo = taosArrayGet(vgInfo, i);
        qDebug("s-task:%s (child taskId:%d) shuffle-dispatch blocks:%d to vgId:%d", pTask->id.idStr, pTask->info.selfChildId,
               pReqs[i].blockNum, pVgInfo->vgId);

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
  ASSERT(pTask->outputInfo.status == TASK_OUTPUT_STATUS__WAIT);

  int32_t code = streamDispatchAllBlocks(pTask, pTask->msgInfo.pData);
  if (code != TSDB_CODE_SUCCESS) {
    qDebug("s-task:%s reset the waitRspCnt to be 0 before launch retry dispatch", pTask->id.idStr);
    atomic_store_32(&pTask->shuffleDispatcher.waitingRspCnt, 0);
    streamRetryDispatchStreamBlock(pTask, DISPATCH_RETRY_INTERVAL_MS);
  }
}

void streamRetryDispatchStreamBlock(SStreamTask* pTask, int64_t waitDuration) {
  qError("s-task:%s dispatch data in %"PRId64"ms", pTask->id.idStr, waitDuration);
  taosTmrReset(doRetryDispatchData, waitDuration, pTask, streamEnv.timer, &pTask->launchTaskTimer);
}

int32_t streamDispatchStreamBlock(SStreamTask* pTask) {
  STaskOutputInfo* pInfo = &pTask->outputInfo;
  ASSERT((pInfo->type == TASK_OUTPUT__FIXED_DISPATCH || pInfo->type == TASK_OUTPUT__SHUFFLE_DISPATCH));

  int32_t numOfElems = taosQueueItemSize(pInfo->queue->queue);
  if (numOfElems > 0) {
    qDebug("s-task:%s try to dispatch intermediate result block to downstream, elem in outputQ:%d", pTask->id.idStr,
           numOfElems);
  }

  // to make sure only one dispatch is running
  int8_t old = atomic_val_compare_exchange_8(&pInfo->status, TASK_OUTPUT_STATUS__NORMAL, TASK_OUTPUT_STATUS__WAIT);
  if (old != TASK_OUTPUT_STATUS__NORMAL) {
    qDebug("s-task:%s wait for dispatch rsp, not dispatch now, output status:%d", pTask->id.idStr, old);
    return 0;
  }

  ASSERT(pTask->msgInfo.pData == NULL);
  qDebug("s-task:%s start to dispatch msg, set output status:%d", pTask->id.idStr, pInfo->status);

  SStreamDataBlock* pBlock = streamQueueNextItem(pInfo->queue);
  if (pBlock == NULL) {
    atomic_store_8(&pInfo->status, TASK_OUTPUT_STATUS__NORMAL);
    qDebug("s-task:%s not dispatch since no elems in outputQ, output status:%d", pTask->id.idStr, pInfo->status);
    return 0;
  }

  pTask->msgInfo.pData = pBlock;
  ASSERT(pBlock->type == STREAM_INPUT__DATA_BLOCK);

  int32_t retryCount = 0;

  while (1) {
    int32_t code = streamDispatchAllBlocks(pTask, pBlock);
    if (code == TSDB_CODE_SUCCESS) {
      break;
    }

    qDebug("s-task:%s failed to dispatch msg to downstream, code:%s, output status:%d, retry cnt:%d", pTask->id.idStr,
           tstrerror(terrno), pInfo->status, retryCount);

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

int32_t streamAddEndScanHistoryMsg(SStreamTask* pTask, SRpcHandleInfo* pRpcInfo, SStreamScanHistoryFinishReq* pReq) {
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

    qDebug("s-task:%s level:%d notify upstream:0x%x to continue process data from WAL", pTask->id.idStr, pTask->info.taskLevel,
           pInfo->taskId);
  }

  taosArrayClear(pTask->pRspMsgList);
  qDebug("s-task:%s level:%d checkpoint ready msg sent to all %d upstreams", pTask->id.idStr, pTask->info.taskLevel,
         num);
  return 0;
}
