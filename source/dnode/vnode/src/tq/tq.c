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
  pTq->tqMeta = tqStoreOpen(path, (FTqSerialize)tqSerializeConsumer, (FTqDeserialize)tqDeserializeConsumer, free, 0);
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

int tqCommit(STQ* pTq) {
  // do nothing
  return 0;
}

int tqSerializeConsumer(const STqConsumerHandle* pConsumer, STqSerializedHead** ppHead) {
  int32_t num = taosArrayGetSize(pConsumer->topics);
  int32_t sz = sizeof(STqSerializedHead) + sizeof(int64_t) * 2 + TSDB_TOPIC_FNAME_LEN +
               num * (sizeof(int64_t) + TSDB_TOPIC_FNAME_LEN);
  if (sz > (*ppHead)->ssize) {
    void* tmpPtr = realloc(*ppHead, sz);
    if (tmpPtr == NULL) {
      free(*ppHead);
      return -1;
    }
    *ppHead = tmpPtr;
    (*ppHead)->ssize = sz;
  }

  void* ptr = (*ppHead)->content;
  *(int64_t*)ptr = pConsumer->consumerId;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  *(int64_t*)ptr = pConsumer->epoch;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  memcpy(ptr, pConsumer->topics, TSDB_TOPIC_FNAME_LEN);
  ptr = POINTER_SHIFT(ptr, TSDB_TOPIC_FNAME_LEN);
  *(int32_t*)ptr = num;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  for (int32_t i = 0; i < num; i++) {
    STqTopicHandle* pTopic = taosArrayGet(pConsumer->topics, i);
    memcpy(ptr, pTopic->topicName, TSDB_TOPIC_FNAME_LEN);
    ptr = POINTER_SHIFT(ptr, TSDB_TOPIC_FNAME_LEN);
    *(int64_t*)ptr = pTopic->committedOffset;
    POINTER_SHIFT(ptr, sizeof(int64_t));
  }

  return 0;
}

const void* tqDeserializeConsumer(const STqSerializedHead* pHead, STqConsumerHandle** ppConsumer) {
  STqConsumerHandle* pConsumer = *ppConsumer;
  const void*        ptr = pHead->content;
  pConsumer->consumerId = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  pConsumer->epoch = *(int64_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int64_t));
  memcpy(pConsumer->cgroup, ptr, TSDB_TOPIC_FNAME_LEN);
  ptr = POINTER_SHIFT(ptr, TSDB_TOPIC_FNAME_LEN);
  int32_t sz = *(int32_t*)ptr;
  ptr = POINTER_SHIFT(ptr, sizeof(int32_t));
  pConsumer->topics = taosArrayInit(sz, sizeof(STqTopicHandle));
  for (int32_t i = 0; i < sz; i++) {
    /*STqTopicHandle* topicHandle = */
    /*taosArrayPush(pConsumer->topics, );*/
  }
  return NULL;
}

int32_t tqProcessConsumeReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqConsumeReq* pReq = pMsg->pCont;
  int64_t        reqId = pReq->reqId;
  int64_t        consumerId = pReq->consumerId;
  int64_t        fetchOffset = pReq->offset;
  int64_t        blockingTime = pReq->blockingTime;

  SMqConsumeRsp rsp = {.consumerId = consumerId, .numOfTopics = 0, .pBlockData = NULL};

  /*printf("vg %d get consume req\n", pReq->head.vgId);*/

  STqConsumerHandle* pConsumer = tqHandleGet(pTq->tqMeta, consumerId);
  if (pConsumer == NULL) {
    pMsg->pCont = NULL;
    pMsg->contLen = 0;
    pMsg->code = -1;
    rpcSendResponse(pMsg);
    return 0;
  }
  int sz = taosArrayGetSize(pConsumer->topics);

  for (int i = 0; i < sz; i++) {
    STqTopicHandle* pTopic = taosArrayGet(pConsumer->topics, i);
    // TODO: support multiple topic in one req
    if (strcmp(pTopic->topicName, pReq->topic) != 0) {
      /*ASSERT(false);*/
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

  STqConsumerHandle* pConsumer = tqHandleGet(pTq->tqMeta, req.oldConsumerId);
  ASSERT(pConsumer);
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
  STqConsumerHandle* pConsumer = calloc(1, sizeof(STqConsumerHandle));
  if (pConsumer == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return -1;
  }

  strcpy(pConsumer->cgroup, req.cgroup);
  pConsumer->topics = taosArrayInit(0, sizeof(STqTopicHandle));
  pConsumer->consumerId = req.consumerId;
  pConsumer->epoch = 0;

  STqTopicHandle* pTopic = calloc(1, sizeof(STqTopicHandle));
  if (pTopic == NULL) {
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
