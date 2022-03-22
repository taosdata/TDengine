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

#include "tcompare.h"
#include "tqInt.h"
#include "tqMetaStore.h"

int32_t tqInit() { return tqPushMgrInit(); }

void tqCleanUp() { tqPushMgrCleanUp(); }

STQ* tqOpen(const char* path, SWal* pWal, SMeta* pVnodeMeta, STqCfg* tqConfig, SMemAllocatorFactory* allocFac) {
  STQ* pTq = malloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->tqConfig = tqConfig;
  pTq->pWal = pWal;
  pTq->pVnodeMeta = pVnodeMeta;
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

  pTq->tqPushMgr = tqPushMgrOpen();
  if (pTq->tqPushMgr == NULL) {
    // free store
    free(pTq);
    return NULL;
  }

  pTq->pStreamTasks = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  return pTq;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    tfree(pTq->path);
    free(pTq);
  }
  // TODO
}

int tqPushMsg(STQ* pTq, void* msg, tmsg_t msgType, int64_t version) {
  if (msgType != TDMT_VND_SUBMIT) return 0;
  void* pIter = taosHashIterate(pTq->tqPushMgr->pHash, NULL);
  while (pIter != NULL) {
    STqPusher* pusher = *(STqPusher**)pIter;
    if (pusher->type == TQ_PUSHER_TYPE__STREAM) {
      STqStreamPusher* streamPusher = (STqStreamPusher*)pusher;
      // repack
      STqStreamToken* token = malloc(sizeof(STqStreamToken));
      if (token == NULL) {
        taosHashCancelIterate(pTq->tqPushMgr->pHash, pIter);
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        return -1;
      }
      token->type = TQ_STREAM_TOKEN__DATA;
      token->data = msg;
      // set input
      // exec
    }
    // send msg to ep
  }
  // iterate hash
  // process all msg
  // if waiting
  // memcpy and send msg to fetch thread
  // TODO: add reference
  // if handle waiting, launch query and response to consumer
  //
  // if no waiting handle, return
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
  /*tlen += taosEncodeFixedI64(buf, pTopic->persistedOffset);*/
  /*tlen += taosEncodeFixedI64(buf, pTopic->committedOffset);*/
  /*tlen += taosEncodeFixedI64(buf, pTopic->currentOffset);*/
  return tlen;
}

static FORCE_INLINE const void* tDecodeSTqTopic(const void* buf, STqTopic* pTopic) {
  buf = taosDecodeStringTo(buf, pTopic->topicName);
  /*buf = taosDecodeString(buf, &pTopic->sql);*/
  /*buf = taosDecodeString(buf, &pTopic->logicalPlan);*/
  /*buf = taosDecodeString(buf, &pTopic->physicalPlan);*/
  buf = taosDecodeString(buf, &pTopic->qmsg);
  /*buf = taosDecodeFixedI64(buf, &pTopic->persistedOffset);*/
  /*buf = taosDecodeFixedI64(buf, &pTopic->committedOffset);*/
  /*buf = taosDecodeFixedI64(buf, &pTopic->currentOffset);*/
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
    for (int j = 0; j < TQ_BUFFER_SIZE; j++) {
      pTopic->buffer.output[j].status = 0;
      STqReadHandle* pReadHandle = tqInitSubmitMsgScanner(pTq->pVnodeMeta);
      SReadHandle    handle = {
             .reader = pReadHandle,
             .meta = pTq->pVnodeMeta,
      };
      pTopic->buffer.output[j].pReadHandle = pReadHandle;
      pTopic->buffer.output[j].task = qCreateStreamExecTaskInfo(pTopic->qmsg, &handle);
    }
  }

  return 0;
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqPollReq* pReq = pMsg->pCont;
  int64_t     consumerId = pReq->consumerId;
  int64_t     fetchOffset;
  int64_t     blockingTime = pReq->blockingTime;

  if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__EARLIEAST) {
    fetchOffset = 0;
  } else if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__LATEST) {
    fetchOffset = walGetLastVer(pTq->pWal);
  } else {
    fetchOffset = pReq->currentOffset + 1;
  }

  SMqPollRsp rsp = {
      /*.consumerId = consumerId,*/
      .numOfTopics = 0,
      .pBlockData = NULL,
  };

  STqConsumer* pConsumer = tqHandleGet(pTq->tqMeta, consumerId);
  if (pConsumer == NULL) {
    pMsg->pCont = NULL;
    pMsg->contLen = 0;
    pMsg->code = -1;
    rpcSendResponse(pMsg);
    return 0;
  }

  int sz = taosArrayGetSize(pConsumer->topics);
  ASSERT(sz == 1);
  STqTopic* pTopic = taosArrayGet(pConsumer->topics, 0);
  ASSERT(strcmp(pTopic->topicName, pReq->topic) == 0);
  ASSERT(pConsumer->consumerId == consumerId);

  rsp.reqOffset = pReq->currentOffset;
  rsp.skipLogNum = 0;

  SWalHead* pHead;
  while (1) {
    /*if (fetchOffset > walGetLastVer(pTq->pWal) || walReadWithHandle(pTopic->pReadhandle, fetchOffset) < 0) {*/
    if (walReadWithHandle(pTopic->pReadhandle, fetchOffset) < 0) {
      // TODO: no more log, set timer to wait blocking time
      // if data inserted during waiting, launch query and
      // response to user
      break;
    }
    int8_t pos = fetchOffset % TQ_BUFFER_SIZE;
    pHead = pTopic->pReadhandle->pHead;
    if (pHead->head.msgType == TDMT_VND_SUBMIT) {
      SSubmitReq* pCont = (SSubmitReq*)&pHead->head.body;
      qTaskInfo_t task = pTopic->buffer.output[pos].task;
      qSetStreamInput(task, pCont, STREAM_DATA_TYPE_SUBMIT_BLOCK);
      SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
      while (1) {
        SSDataBlock* pDataBlock;
        uint64_t     ts;
        if (qExecTask(task, &pDataBlock, &ts) < 0) {
          ASSERT(false);
        }
        if (pDataBlock == NULL) {
          fetchOffset++;
          pos = fetchOffset % TQ_BUFFER_SIZE;
          rsp.skipLogNum++;
          break;
        }

        taosArrayPush(pRes, pDataBlock);
        rsp.schema = pTopic->buffer.output[pos].pReadHandle->pSchemaWrapper;
        rsp.rspOffset = fetchOffset;

        rsp.numOfTopics = 1;
        rsp.pBlockData = pRes;

        int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqPollRsp(NULL, &rsp);
        void*   buf = rpcMallocCont(tlen);
        if (buf == NULL) {
          pMsg->code = -1;
          return -1;
        }
        ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
        ((SMqRspHead*)buf)->epoch = pReq->epoch;
        ((SMqRspHead*)buf)->consumerId = consumerId;

        void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
        tEncodeSMqPollRsp(&abuf, &rsp);
        /*taosArrayDestroyEx(rsp.pBlockData, (void (*)(void*))tDeleteSSDataBlock);*/
        pMsg->pCont = buf;
        pMsg->contLen = tlen;
        pMsg->code = 0;
        rpcSendResponse(pMsg);
        return 0;
      }
    } else {
      fetchOffset++;
      rsp.skipLogNum++;
    }
  }

  /*if (blockingTime != 0) {*/
  /*tqAddClientPusher(pTq->tqPushMgr, pMsg, consumerId, blockingTime);*/
  /*} else {*/
  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqPollRsp(NULL, &rsp);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    pMsg->code = -1;
    return -1;
  }
  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqPollRsp(&abuf, &rsp);
  rsp.pBlockData = NULL;
  pMsg->pCont = buf;
  pMsg->contLen = tlen;
  pMsg->code = 0;
  rpcSendResponse(pMsg);
  /*}*/

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
  /*pTopic->committedOffset = -1;*/
  /*pTopic->currentOffset = -1;*/

  pTopic->buffer.firstOffset = -1;
  pTopic->buffer.lastOffset = -1;
  pTopic->pReadhandle = walOpenReadHandle(pTq->pWal);
  if (pTopic->pReadhandle == NULL) {
    ASSERT(false);
  }
  for (int i = 0; i < TQ_BUFFER_SIZE; i++) {
    pTopic->buffer.output[i].status = 0;
    STqReadHandle* pReadHandle = tqInitSubmitMsgScanner(pTq->pVnodeMeta);
    SReadHandle    handle = {
           .reader = pReadHandle,
           .meta = pTq->pVnodeMeta,
    };
    pTopic->buffer.output[i].pReadHandle = pReadHandle;
    pTopic->buffer.output[i].task = qCreateStreamExecTaskInfo(req.qmsg, &handle);
  }
  taosArrayPush(pConsumer->topics, pTopic);
  tqHandleMovePut(pTq->tqMeta, req.consumerId, pConsumer);
  tqHandleCommit(pTq->tqMeta, req.consumerId);
  terrno = TSDB_CODE_SUCCESS;
  return 0;
}

int32_t tqProcessTaskDeploy(STQ* pTq, char* msg, int32_t msgLen) {
  SStreamTask* pTask = malloc(sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SCoder decoder;
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, (uint8_t*)msg, msgLen, TD_DECODER);
  tDecodeSStreamTask(&decoder, pTask);
  tCoderClear(&decoder);

  taosHashPut(pTq->pStreamTasks, &pTask->taskId, sizeof(int32_t), pTask, sizeof(SStreamTask));

  return 0;
}

int32_t tqProcessTaskExec(STQ* pTq, SRpcMsg* msg) {
  //
  return 0;
}
