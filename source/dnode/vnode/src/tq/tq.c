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
#define _DEFAULT_SOURCE

#include "tcompare.h"
#include "tqInt.h"
#include "tqMetaStore.h"

int tqInit() {
  int8_t old = atomic_val_compare_exchange_8(&tqMgmt.inited, 0, 1);
  if (old == 1) return 0;

  tqMgmt.timer = taosTmrInit(0, 0, 0, "TQ");
  return 0;
}

void tqCleanUp() {
  int8_t old = atomic_val_compare_exchange_8(&tqMgmt.inited, 1, 0);
  if (old == 0) return;
  taosTmrStop(tqMgmt.timer);
  taosTmrCleanUp(tqMgmt.timer);
}

STQ* tqOpen(const char* path, SWal* pWal, SMeta* pMeta, STqCfg* tqConfig, SMemAllocatorFactory* allocFac) {
  STQ* pTq = malloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->tqConfig = tqConfig;
  pTq->pWal = pWal;
  pTq->pMeta = pMeta;
#if 0
  pTq->tqMemRef.pAllocatorFactory = allocFac;
  pTq->tqMemRef.pAllocator = allocFac->create(allocFac);
  if (pTq->tqMemRef.pAllocator == NULL) {
    // TODO: error code of buffer pool
  }
#endif
  pTq->tqMeta =
      tqStoreOpen(pTq, path, (FTqSerialize)tqSerializeConsumer, (FTqDeserialize)tqDeserializeConsumer, free, 0);
  if (pTq->tqMeta == NULL) {
    free(pTq);
#if 0
    allocFac->destroy(allocFac, pTq->tqMemRef.pAllocator);
#endif
    return NULL;
  }

  return pTq;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    tfree(pTq->path);
    free(pTq);
  }
  // TODO
}

int tqPushMsg(STQ* pTq, void* p, int64_t version) {
  // add reference
  // judge and launch new query
  return 0;
}

int tqCommit(STQ* pTq) { return tqStorePersist(pTq->tqMeta); }

int32_t tqGetTopicHandleSize(const STqTopic* pTopic) {
  return strlen(pTopic->topicName) + strlen(pTopic->sql) + strlen(pTopic->logicalPlan) + strlen(pTopic->physicalPlan) +
         strlen(pTopic->qmsg) + sizeof(int64_t) * 3;
}

int32_t tqGetConsumerHandleSize(const STqConsumer* pConsumer) {
  int     num = taosArrayGetSize(pConsumer->topics);
  int32_t sz = 0;
  for (int i = 0; i < num; i++) {
    STqTopic* pTopic = taosArrayGet(pConsumer->topics, i);
    sz += tqGetTopicHandleSize(pTopic);
  }
  return sz;
}

static FORCE_INLINE int32_t tEncodeSTqTopic(void** buf, const STqTopic* pTopic) {
  int32_t tlen = 0;
  tlen += taosEncodeString(buf, pTopic->topicName);
  /*tlen += taosEncodeString(buf, pTopic->sql);*/
  /*tlen += taosEncodeString(buf, pTopic->logicalPlan);*/
  /*tlen += taosEncodeString(buf, pTopic->physicalPlan);*/
  tlen += taosEncodeString(buf, pTopic->qmsg);
  tlen += taosEncodeFixedI64(buf, pTopic->persistedOffset);
  tlen += taosEncodeFixedI64(buf, pTopic->committedOffset);
  tlen += taosEncodeFixedI64(buf, pTopic->currentOffset);
  return tlen;
}

static FORCE_INLINE const void* tDecodeSTqTopic(const void* buf, STqTopic* pTopic) {
  buf = taosDecodeStringTo(buf, pTopic->topicName);
  /*buf = taosDecodeString(buf, &pTopic->sql);*/
  /*buf = taosDecodeString(buf, &pTopic->logicalPlan);*/
  /*buf = taosDecodeString(buf, &pTopic->physicalPlan);*/
  buf = taosDecodeString(buf, &pTopic->qmsg);
  buf = taosDecodeFixedI64(buf, &pTopic->persistedOffset);
  buf = taosDecodeFixedI64(buf, &pTopic->committedOffset);
  buf = taosDecodeFixedI64(buf, &pTopic->currentOffset);
  return buf;
}

static FORCE_INLINE int32_t tEncodeSTqConsumer(void** buf, const STqConsumer* pConsumer) {
  int32_t sz;

  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, pConsumer->consumerId);
  tlen += taosEncodeFixedI64(buf, pConsumer->epoch);
  tlen += taosEncodeString(buf, pConsumer->cgroup);
  sz = taosArrayGetSize(pConsumer->topics);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    STqTopic* pTopic = taosArrayGet(pConsumer->topics, i);
    tlen += tEncodeSTqTopic(buf, pTopic);
  }
  return tlen;
}

static FORCE_INLINE const void* tDecodeSTqConsumer(const void* buf, STqConsumer* pConsumer) {
  int32_t sz;

  buf = taosDecodeFixedI64(buf, &pConsumer->consumerId);
  buf = taosDecodeFixedI64(buf, &pConsumer->epoch);
  buf = taosDecodeStringTo(buf, pConsumer->cgroup);
  buf = taosDecodeFixedI32(buf, &sz);
  pConsumer->topics = taosArrayInit(sz, sizeof(STqTopic));
  if (pConsumer->topics == NULL) return NULL;
  for (int32_t i = 0; i < sz; i++) {
    STqTopic pTopic;
    buf = tDecodeSTqTopic(buf, &pTopic);
    taosArrayPush(pConsumer->topics, &pTopic);
  }
  return buf;
}

int tqSerializeConsumer(const STqConsumer* pConsumer, STqSerializedHead** ppHead) {
  int32_t sz = tEncodeSTqConsumer(NULL, pConsumer);

  if (sz > (*ppHead)->ssize) {
    void* tmpPtr = realloc(*ppHead, sizeof(STqSerializedHead) + sz);
    if (tmpPtr == NULL) {
      free(*ppHead);
      terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
      return -1;
    }
    *ppHead = tmpPtr;
    (*ppHead)->ssize = sz;
  }

  void* ptr = (*ppHead)->content;
  void* abuf = ptr;
  tEncodeSTqConsumer(&abuf, pConsumer);

  return 0;
}

int32_t tqDeserializeConsumer(STQ* pTq, const STqSerializedHead* pHead, STqConsumer** ppConsumer) {
  const void* str = pHead->content;
  *ppConsumer = calloc(1, sizeof(STqConsumer));
  if (*ppConsumer == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }
  if (tDecodeSTqConsumer(str, *ppConsumer) == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }
  STqConsumer* pConsumer = *ppConsumer;
  int32_t      sz = taosArrayGetSize(pConsumer->topics);
  for (int32_t i = 0; i < sz; i++) {
    STqTopic* pTopic = taosArrayGet(pConsumer->topics, i);
    pTopic->pReadhandle = walOpenReadHandle(pTq->pWal);
    if (pTopic->pReadhandle == NULL) {
      ASSERT(false);
    }
    for (int i = 0; i < TQ_BUFFER_SIZE; i++) {
      pTopic->buffer.output[i].status = 0;
      STqReadHandle* pReadHandle = tqInitSubmitMsgScanner(pTq->pMeta);
      SReadHandle    handle = {.reader = pReadHandle, .meta = pTq->pMeta};
      pTopic->buffer.output[i].pReadHandle = pReadHandle;
      pTopic->buffer.output[i].task = qCreateStreamExecTaskInfo(pTopic->qmsg, &handle);
    }
  }

  return 0;
}

int32_t tqProcessConsumeReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqConsumeReq* pReq = pMsg->pCont;
  int64_t        reqId = pReq->reqId;
  int64_t        consumerId = pReq->consumerId;
  int64_t        fetchOffset = pReq->offset;
  int64_t        blockingTime = pReq->blockingTime;

  SMqConsumeRsp rsp = {.consumerId = consumerId, .numOfTopics = 0, .pBlockData = NULL};

  /*printf("vg %d get consume req\n", pReq->head.vgId);*/

  STqConsumer* pConsumer = tqHandleGet(pTq->tqMeta, consumerId);
  if (pConsumer == NULL) {
    pMsg->pCont = NULL;
    pMsg->contLen = 0;
    pMsg->code = -1;
    rpcSendResponse(pMsg);
    return 0;
  }
  int sz = taosArrayGetSize(pConsumer->topics);

  for (int i = 0; i < sz; i++) {
    STqTopic* pTopic = taosArrayGet(pConsumer->topics, i);
    // TODO: support multiple topic in one req
    if (strcmp(pTopic->topicName, pReq->topic) != 0) {
      ASSERT(false);
      continue;
    }

    if (pReq->reqType == TMQ_REQ_TYPE_COMMIT_ONLY) {
      pTopic->committedOffset = pReq->offset;
      pMsg->pCont = NULL;
      pMsg->contLen = 0;
      pMsg->code = 0;
      rpcSendResponse(pMsg);
      return 0;
    }

    if (pReq->reqType == TMQ_REQ_TYPE_CONSUME_AND_COMMIT) {
      pTopic->committedOffset = pReq->offset - 1;
    }

    rsp.committedOffset = pTopic->committedOffset;
    rsp.reqOffset = pReq->offset;
    rsp.skipLogNum = 0;

    if (fetchOffset <= pTopic->committedOffset) {
      fetchOffset = pTopic->committedOffset + 1;
    }
    /*printf("vg %d fetch Offset %ld\n", pReq->head.vgId, fetchOffset);*/
    int8_t    pos;
    int8_t    skip = 0;
    SWalHead* pHead;
    while (1) {
      pos = fetchOffset % TQ_BUFFER_SIZE;
      skip = atomic_val_compare_exchange_8(&pTopic->buffer.output[pos].status, 0, 1);
      if (skip == 1) {
        // do nothing
        break;
      }
      if (walReadWithHandle(pTopic->pReadhandle, fetchOffset) < 0) {
        // check err
        atomic_store_8(&pTopic->buffer.output[pos].status, 0);
        skip = 1;
        break;
      }
      // read until find TDMT_VND_SUBMIT
      pHead = pTopic->pReadhandle->pHead;
      if (pHead->head.msgType == TDMT_VND_SUBMIT) {
        break;
      }
      rsp.skipLogNum++;
      if (walReadWithHandle(pTopic->pReadhandle, fetchOffset) < 0) {
        atomic_store_8(&pTopic->buffer.output[pos].status, 0);
        skip = 1;
        break;
      }
      atomic_store_8(&pTopic->buffer.output[pos].status, 0);
      fetchOffset++;
    }
    if (skip == 1) continue;
    SSubmitMsg* pCont = (SSubmitMsg*)&pHead->head.body;
    qTaskInfo_t task = pTopic->buffer.output[pos].task;

    qSetStreamInput(task, pCont);

    // SArray<SSDataBlock>
    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    while (1) {
      SSDataBlock* pDataBlock;
      uint64_t     ts;
      if (qExecTask(task, &pDataBlock, &ts) < 0) {
        break;
      }
      if (pDataBlock != NULL) {
        taosArrayPush(pRes, pDataBlock);
      } else {
        break;
      }
    }
    // TODO copy
    rsp.schemas = pTopic->buffer.output[pos].pReadHandle->pSchemaWrapper;
    rsp.rspOffset = fetchOffset;

    atomic_store_8(&pTopic->buffer.output[pos].status, 0);

    if (taosArrayGetSize(pRes) == 0) {
      taosArrayDestroy(pRes);
      fetchOffset++;
      continue;
    } else {
      rsp.numOfTopics++;
    }

    rsp.pBlockData = pRes;

#if 0
    pTopic->buffer.output[pos].dst = pRes;
    if (pTopic->buffer.firstOffset == -1 || pReq->offset < pTopic->buffer.firstOffset) {
      pTopic->buffer.firstOffset = pReq->offset;
    }
    if (pTopic->buffer.lastOffset == -1 || pReq->offset > pTopic->buffer.lastOffset) {
      pTopic->buffer.lastOffset = pReq->offset;
    }
#endif
  }
  int32_t tlen = tEncodeSMqConsumeRsp(NULL, &rsp);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    pMsg->code = -1;
    return -1;
  }
  void* abuf = buf;
  tEncodeSMqConsumeRsp(&abuf, &rsp);

  if (rsp.pBlockData) {
    taosArrayDestroyEx(rsp.pBlockData, (void (*)(void*))tDeleteSSDataBlock);
    rsp.pBlockData = NULL;
  }

  pMsg->pCont = buf;
  pMsg->contLen = tlen;
  pMsg->code = 0;
  rpcSendResponse(pMsg);
  return 0;
}

int32_t tqProcessRebReq(STQ* pTq, char* msg) {
  SMqMVRebReq req = {0};
  tDecodeSMqMVRebReq(msg, &req);

  STqConsumer* pConsumer = tqHandleGet(pTq->tqMeta, req.oldConsumerId);
  ASSERT(pConsumer);
  pConsumer->consumerId = req.newConsumerId;
  tqHandleMovePut(pTq->tqMeta, req.newConsumerId, pConsumer);
  tqHandleCommit(pTq->tqMeta, req.newConsumerId);
  tqHandlePurge(pTq->tqMeta, req.oldConsumerId);
  terrno = TSDB_CODE_SUCCESS;
  return 0;
}

int32_t tqProcessSetConnReq(STQ* pTq, char* msg) {
  SMqSetCVgReq req = {0};
  tDecodeSMqSetCVgReq(msg, &req);

  /*printf("vg %d set to consumer from %ld to %ld\n", req.vgId, req.oldConsumerId, req.newConsumerId);*/
  STqConsumer* pConsumer = calloc(1, sizeof(STqConsumer));
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }

  strcpy(pConsumer->cgroup, req.cgroup);
  pConsumer->topics = taosArrayInit(0, sizeof(STqTopic));
  pConsumer->consumerId = req.consumerId;
  pConsumer->epoch = 0;

  STqTopic* pTopic = calloc(1, sizeof(STqTopic));
  if (pTopic == NULL) {
    taosArrayDestroy(pConsumer->topics);
    free(pConsumer);
    return -1;
  }
  strcpy(pTopic->topicName, req.topicName);
  pTopic->sql = req.sql;
  pTopic->logicalPlan = req.logicalPlan;
  pTopic->physicalPlan = req.physicalPlan;
  pTopic->qmsg = req.qmsg;
  pTopic->committedOffset = -1;
  pTopic->currentOffset = -1;

  pTopic->buffer.firstOffset = -1;
  pTopic->buffer.lastOffset = -1;
  pTopic->pReadhandle = walOpenReadHandle(pTq->pWal);
  if (pTopic->pReadhandle == NULL) {
    ASSERT(false);
  }
  for (int i = 0; i < TQ_BUFFER_SIZE; i++) {
    pTopic->buffer.output[i].status = 0;
    STqReadHandle* pReadHandle = tqInitSubmitMsgScanner(pTq->pMeta);
    SReadHandle    handle = {.reader = pReadHandle, .meta = pTq->pMeta};
    pTopic->buffer.output[i].pReadHandle = pReadHandle;
    pTopic->buffer.output[i].task = qCreateStreamExecTaskInfo(req.qmsg, &handle);
  }
  taosArrayPush(pConsumer->topics, pTopic);
  tqHandleMovePut(pTq->tqMeta, req.consumerId, pConsumer);
  tqHandleCommit(pTq->tqMeta, req.consumerId);
  terrno = TSDB_CODE_SUCCESS;
  return 0;
}
