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

void tqDebugShowSSData(SArray* dataBlocks);

int32_t tqInit() { return tqPushMgrInit(); }

void tqCleanUp() { tqPushMgrCleanUp(); }

STQ* tqOpen(const char* path, SVnode* pVnode, SWal* pWal, SMeta* pVnodeMeta, STqCfg* tqConfig,
            SMemAllocatorFactory* allocFac) {
  STQ* pTq = malloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->tqConfig = tqConfig;
  pTq->pVnode = pVnode;
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

int tqPushMsg(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t version) {
  if (msgType != TDMT_VND_SUBMIT) return 0;
  void* data = malloc(msgLen);
  if (data == NULL) {
    return -1;
  }
  memcpy(data, msg, msgLen);
  SRpcMsg req = {
      .msgType = TDMT_VND_STREAM_TRIGGER,
      .pCont = data,
      .contLen = msgLen,
  };
  tmsgPutToQueue(&pTq->pVnode->msgCb, FETCH_QUEUE, &req);

#if 0
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
#endif
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

int32_t tqExpandTask(STQ* pTq, SStreamTask* pTask, int32_t parallel) {
  ASSERT(parallel <= 8);
  pTask->numOfRunners = parallel;
  for (int32_t i = 0; i < parallel; i++) {
    STqReadHandle* pReadHandle = tqInitSubmitMsgScanner(pTq->pVnodeMeta);
    SReadHandle    handle = {
           .reader = pReadHandle,
           .meta = pTq->pVnodeMeta,
    };
    pTask->runner[i].inputHandle = pReadHandle;
    pTask->runner[i].executor = qCreateStreamExecTaskInfo(pTask->qmsg, &handle);
  }
  return 0;
}

int32_t tqProcessTaskDeploy(STQ* pTq, char* msg, int32_t msgLen) {
  SStreamTask* pTask = malloc(sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SCoder decoder;
  tCoderInit(&decoder, TD_LITTLE_ENDIAN, (uint8_t*)msg, msgLen, TD_DECODER);
  if (tDecodeSStreamTask(&decoder, pTask) < 0) {
    ASSERT(0);
  }
  tCoderClear(&decoder);

  tqExpandTask(pTq, pTask, 8);
  taosHashPut(pTq->pStreamTasks, &pTask->taskId, sizeof(int32_t), pTask, sizeof(SStreamTask));

  return 0;
}

static char* formatTimestamp(char* buf, int64_t val, int precision) {
  time_t  tt;
  int32_t ms = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    tt = (time_t)(val / 1000000000);
    ms = val % 1000000000;
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    tt = (time_t)(val / 1000000);
    ms = val % 1000000;
  } else {
    tt = (time_t)(val / 1000);
    ms = val % 1000;
  }

  /* comment out as it make testcases like select_with_tags.sim fail.
    but in windows, this may cause the call to localtime crash if tt < 0,
    need to find a better solution.
    if (tt < 0) {
      tt = 0;
    }
    */

#ifdef WINDOWS
  if (tt < 0) tt = 0;
#endif
  if (tt <= 0 && ms < 0) {
    tt--;
    if (precision == TSDB_TIME_PRECISION_NANO) {
      ms += 1000000000;
    } else if (precision == TSDB_TIME_PRECISION_MICRO) {
      ms += 1000000;
    } else {
      ms += 1000;
    }
  }

  struct tm* ptm = localtime(&tt);
  size_t     pos = strftime(buf, 35, "%Y-%m-%d %H:%M:%S", ptm);

  if (precision == TSDB_TIME_PRECISION_NANO) {
    sprintf(buf + pos, ".%09d", ms);
  } else if (precision == TSDB_TIME_PRECISION_MICRO) {
    sprintf(buf + pos, ".%06d", ms);
  } else {
    sprintf(buf + pos, ".%03d", ms);
  }

  return buf;
}
void tqDebugShowSSData(SArray* dataBlocks) {
  char    pBuf[128];
  int32_t sz = taosArrayGetSize(dataBlocks);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pDataBlock = taosArrayGet(dataBlocks, i);
    int32_t      colNum = pDataBlock->info.numOfCols;
    int32_t      rows = pDataBlock->info.rows;
    for (int32_t j = 0; j < rows; j++) {
      printf("|");
      for (int32_t k = 0; k < colNum; k++) {
        SColumnInfoData* pColInfoData = taosArrayGet(pDataBlock->pDataBlock, k);
        void*            var = POINTER_SHIFT(pColInfoData->pData, j * pColInfoData->info.bytes);
        switch (pColInfoData->info.type) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            formatTimestamp(pBuf, *(uint64_t*)var, TSDB_TIME_PRECISION_MILLI);
            printf(" %25s |", pBuf);
            break;
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_UINT:
            printf(" %15d |", *(int32_t*)var);
            break;
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_UBIGINT:
            printf(" %15ld |", *(int64_t*)var);
            break;
        }
      }
      printf("\n");
    }
  }
}

int32_t tqProcessStreamTrigger(STQ* pTq, void* data, int32_t dataLen) {
  void* pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pTq->pStreamTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = (SStreamTask*)pIter;
    if (!pTask->pipeSource) continue;

    int32_t workerId = 0;
    void*   exec = pTask->runner[workerId].executor;
    qSetStreamInput(exec, data, STREAM_DATA_TYPE_SUBMIT_BLOCK);
    SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
    while (1) {
      SSDataBlock* output;
      uint64_t     ts;
      if (qExecTask(exec, &output, &ts) < 0) {
        ASSERT(false);
      }
      if (output == NULL) {
        break;
      }
      taosArrayPush(pRes, output);
    }
    if (pTask->pipeSink) {
      // write back
      /*printf("reach end\n");*/
      tqDebugShowSSData(pRes);
    } else {
      int32_t tlen = sizeof(SStreamExecMsgHead) + tEncodeDataBlocks(NULL, pRes);
      void*   buf = rpcMallocCont(tlen);
      if (buf == NULL) {
        return -1;
      }
      void* abuf = POINTER_SHIFT(buf, sizeof(SStreamExecMsgHead));
      tEncodeDataBlocks(abuf, pRes);
      tmsg_t type;

      if (pTask->nextOpDst == STREAM_NEXT_OP_DST__VND) {
        type = TDMT_VND_TASK_EXEC;
      } else {
        type = TDMT_SND_TASK_EXEC;
      }

      SRpcMsg reqMsg = {
          .pCont = buf,
          .contLen = tlen,
          .code = 0,
          .msgType = type,
      };
      tmsgSendReq(&pTq->pVnode->msgCb, &pTask->NextOpEp, &reqMsg);
    }
  }
  return 0;
}

int32_t tqProcessTaskExec(STQ* pTq, SRpcMsg* msg) {
  SStreamTaskExecReq* pReq = msg->pCont;

  int32_t taskId = pReq->head.streamTaskId;
  int32_t workerType = pReq->head.workerType;

  SStreamTask* pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  // assume worker id is 1
  int32_t workerId = 1;
  void*   exec = pTask->runner[workerId].executor;
  int32_t sz = taosArrayGetSize(pReq->data);
  printf("input data:\n");
  tqDebugShowSSData(pReq->data);
  SArray* pRes = taosArrayInit(0, sizeof(void*));
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* input = taosArrayGet(pReq->data, i);
    SSDataBlock* output;
    uint64_t     ts;
    qSetStreamInput(exec, input, STREAM_DATA_TYPE_SSDATA_BLOCK);
    if (qExecTask(exec, &output, &ts) < 0) {
      ASSERT(0);
    }
    if (output == NULL) {
      break;
    }
    taosArrayPush(pRes, &output);
  }
  printf("output data:\n");
  tqDebugShowSSData(pRes);

  return 0;
}
