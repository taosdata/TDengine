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

int32_t tqInit() {
  //
  return 0;
}

void tqCleanUp() {}

STQ* tqOpen(const char* path, SVnode* pVnode, SWal* pWal) {
  STQ* pTq = taosMemoryMalloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->pVnode = pVnode;
  pTq->pWal = pWal;

#if 0
  pTq->tqMeta = tqStoreOpen(pTq, path, (FTqSerialize)tqSerializeConsumer, (FTqDeserialize)tqDeserializeConsumer,
                            (FTqDelete)taosMemoryFree, 0);
  if (pTq->tqMeta == NULL) {
    taosMemoryFree(pTq);
    return NULL;
  }
#endif

  pTq->execs = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);

  pTq->pStreamTasks = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  pTq->pushMgr = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);

  return pTq;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    taosMemoryFreeClear(pTq->path);
    taosMemoryFree(pTq);
  }
  // TODO
}

static void tdSRowDemo() {
#define DEMO_N_COLS 3

  int16_t     schemaVersion = 0;
  int32_t     numOfCols = DEMO_N_COLS;  // ts + int
  SRowBuilder rb = {0};

  SSchema schema[DEMO_N_COLS] = {
      {.type = TSDB_DATA_TYPE_TIMESTAMP, .colId = 1, .name = "ts", .bytes = 8, .flags = SCHEMA_SMA_ON},
      {.type = TSDB_DATA_TYPE_INT, .colId = 2, .name = "c1", .bytes = 4, .flags = SCHEMA_SMA_ON},
      {.type = TSDB_DATA_TYPE_INT, .colId = 3, .name = "c2", .bytes = 4, .flags = SCHEMA_SMA_ON}};

  SSchema*  pSchema = schema;
  STSchema* pTSChema = tdGetSTSChemaFromSSChema(&pSchema, numOfCols);

  tdSRowInit(&rb, schemaVersion);
  tdSRowSetTpInfo(&rb, numOfCols, pTSChema->flen);
  int32_t maxLen = TD_ROW_MAX_BYTES_FROM_SCHEMA(pTSChema);
  void*   row = taosMemoryCalloc(1, maxLen);  // make sure the buffer is enough

  // set row buf
  tdSRowResetBuf(&rb, row);

  for (int32_t idx = 0; idx < pTSChema->numOfCols; ++idx) {
    STColumn* pColumn = pTSChema->columns + idx;
    if (idx == 0) {
      int64_t tsKey = 1651234567;
      tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, &tsKey, true, pColumn->offset, idx);
    } else if (idx == 1) {
      int32_t val1 = 10;
      tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NORM, &val1, true, pColumn->offset, idx);
    } else {
      tdAppendColValToRow(&rb, pColumn->colId, pColumn->type, TD_VTYPE_NONE, NULL, true, pColumn->offset, idx);
    }
  }

  // print
  tdSRowPrint(row, pTSChema, __func__);

  taosMemoryFree(pTSChema);
}

int32_t tqPushMsgNew(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver) {
  if (msgType != TDMT_VND_SUBMIT) return 0;
  void*       pIter = NULL;
  STqExec*    pExec = NULL;
  SSubmitReq* pReq = (SSubmitReq*)msg;
  int32_t     workerId = 4;
  int64_t     fetchOffset = ver;

  while (1) {
    pIter = taosHashIterate(pTq->pushMgr, pIter);
    if (pIter == NULL) break;
    pExec = *(STqExec**)pIter;

    taosWLockLatch(&pExec->pushHandle.lock);

    SRpcMsg* pMsg = atomic_load_ptr(&pExec->pushHandle.handle);
    ASSERT(pMsg);

    SMqDataBlkRsp rsp = {0};
    rsp.reqOffset = pExec->pushHandle.reqOffset;
    rsp.blockData = taosArrayInit(0, sizeof(void*));
    rsp.blockDataLen = taosArrayInit(0, sizeof(int32_t));

    if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
      qTaskInfo_t task = pExec->task[workerId];
      ASSERT(task);
      qSetStreamInput(task, pReq, STREAM_DATA_TYPE_SUBMIT_BLOCK);
      while (1) {
        SSDataBlock* pDataBlock = NULL;
        uint64_t     ts = 0;
        if (qExecTask(task, &pDataBlock, &ts) < 0) {
          ASSERT(0);
        }
        if (pDataBlock == NULL) break;

        ASSERT(pDataBlock->info.rows != 0);
        ASSERT(pDataBlock->info.numOfCols != 0);

        int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pDataBlock);
        void*              buf = taosMemoryCalloc(1, dataStrLen);
        SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
        pRetrieve->useconds = ts;
        pRetrieve->precision = TSDB_DEFAULT_PRECISION;
        pRetrieve->compressed = 0;
        pRetrieve->completed = 1;
        pRetrieve->numOfRows = htonl(pDataBlock->info.rows);

        // TODO enable compress
        int32_t actualLen = 0;
        blockCompressEncode(pDataBlock, pRetrieve->data, &actualLen, pDataBlock->info.numOfCols, false);
        actualLen += sizeof(SRetrieveTableRsp);
        ASSERT(actualLen <= dataStrLen);
        taosArrayPush(rsp.blockDataLen, &actualLen);
        taosArrayPush(rsp.blockData, &buf);
        rsp.blockNum++;
      }
    } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
      STqReadHandle* pReader = pExec->pExecReader[workerId];
      tqReadHandleSetMsg(pReader, pReq, 0);
      while (tqNextDataBlock(pReader)) {
        SSDataBlock block = {0};
        if (tqRetrieveDataBlock(&block.pDataBlock, pReader, &block.info.groupId, &block.info.uid, &block.info.rows,
                                &block.info.numOfCols) < 0) {
          ASSERT(0);
        }
        int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(&block);
        void*              buf = taosMemoryCalloc(1, dataStrLen);
        SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
        /*pRetrieve->useconds = 0;*/
        pRetrieve->precision = TSDB_DEFAULT_PRECISION;
        pRetrieve->compressed = 0;
        pRetrieve->completed = 1;
        pRetrieve->numOfRows = htonl(block.info.rows);

        // TODO enable compress
        int32_t actualLen = 0;
        blockCompressEncode(&block, pRetrieve->data, &actualLen, block.info.numOfCols, false);
        actualLen += sizeof(SRetrieveTableRsp);
        ASSERT(actualLen <= dataStrLen);
        taosArrayPush(rsp.blockDataLen, &actualLen);
        taosArrayPush(rsp.blockData, &buf);
        rsp.blockNum++;
      }
    } else {
      ASSERT(0);
    }

    if (rsp.blockNum == 0) {
      taosWUnLockLatch(&pExec->pushHandle.lock);
      continue;
    }

    ASSERT(taosArrayGetSize(rsp.blockData) == rsp.blockNum);
    ASSERT(taosArrayGetSize(rsp.blockDataLen) == rsp.blockNum);

    rsp.rspOffset = fetchOffset;

    int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqDataBlkRsp(NULL, &rsp);
    void*   buf = rpcMallocCont(tlen);
    if (buf == NULL) {
      pMsg->code = -1;
      return -1;
    }

    ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
    ((SMqRspHead*)buf)->epoch = pExec->pushHandle.epoch;
    ((SMqRspHead*)buf)->consumerId = pExec->pushHandle.consumerId;

    void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
    tEncodeSMqDataBlkRsp(&abuf, &rsp);
    pMsg->pCont = buf;
    pMsg->contLen = tlen;
    pMsg->code = 0;
    tmsgSendRsp(pMsg);

    atomic_store_ptr(&pExec->pushHandle.handle, NULL);
    taosWUnLockLatch(&pExec->pushHandle.lock);

    tqDebug("vg %d offset %ld from consumer %ld (epoch %d) send rsp, block num: %d, reqOffset: %ld, rspOffset: %ld",
            TD_VID(pTq->pVnode), fetchOffset, pExec->pushHandle.consumerId, pExec->pushHandle.epoch, rsp.blockNum,
            rsp.reqOffset, rsp.rspOffset);

    // TODO destroy
    taosArrayDestroy(rsp.blockData);
    taosArrayDestroy(rsp.blockDataLen);
  }

  return 0;
}

int tqPushMsg(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver) {
  if (msgType != TDMT_VND_SUBMIT) return 0;

  // make sure msgType == TDMT_VND_SUBMIT
  if (tsdbUpdateSmaWindow(pTq->pVnode->pTsdb, msg, ver) != 0) {
    return -1;
  }

  if (taosHashGetSize(pTq->pStreamTasks) == 0) return 0;

  void* data = taosMemoryMalloc(msgLen);
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

  return 0;
}

int tqCommit(STQ* pTq) {
  // do nothing
  /*return tqStorePersist(pTq->tqMeta);*/
  return 0;
}

int32_t tqGetTopicHandleSize(const STqTopic* pTopic) {
  return strlen(pTopic->topicName) + strlen(pTopic->sql) + strlen(pTopic->physicalPlan) + strlen(pTopic->qmsg) +
         sizeof(int64_t) * 3;
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
  tlen += taosEncodeFixedI32(buf, pConsumer->epoch);
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
  buf = taosDecodeFixedI32(buf, &pConsumer->epoch);
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
    void* tmpPtr = taosMemoryRealloc(*ppHead, sizeof(STqSerializedHead) + sz);
    if (tmpPtr == NULL) {
      taosMemoryFree(*ppHead);
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
  *ppConsumer = taosMemoryCalloc(1, sizeof(STqConsumer));
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
      STqReadHandle* pReadHandle = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
      SReadHandle    handle = {
             .reader = pReadHandle,
             .meta = pTq->pVnode->pMeta,
             .pMsgCb = &pTq->pVnode->msgCb,
      };
      pTopic->buffer.output[j].pReadHandle = pReadHandle;
      pTopic->buffer.output[j].task = qCreateStreamExecTaskInfo(pTopic->qmsg, &handle);
    }
  }

  return 0;
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg, int32_t workerId) {
  SMqPollReq* pReq = pMsg->pCont;
  int64_t     consumerId = pReq->consumerId;
  int64_t     waitTime = pReq->waitTime;
  int32_t     reqEpoch = pReq->epoch;
  int64_t     fetchOffset;

  // get offset to fetch message
  if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__EARLIEAST) {
    fetchOffset = walGetFirstVer(pTq->pWal);
  } else if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__LATEST) {
    fetchOffset = walGetLastVer(pTq->pWal);
  } else {
    fetchOffset = pReq->currentOffset + 1;
  }

  tqDebug("tmq poll: consumer %ld (epoch %d) recv poll req in vg %d, req %ld %ld", consumerId, pReq->epoch,
          TD_VID(pTq->pVnode), pReq->currentOffset, fetchOffset);

  STqExec* pExec = taosHashGet(pTq->execs, pReq->subKey, strlen(pReq->subKey));
  ASSERT(pExec);

  int32_t consumerEpoch = atomic_load_32(&pExec->epoch);
  while (consumerEpoch < reqEpoch) {
    consumerEpoch = atomic_val_compare_exchange_32(&pExec->epoch, consumerEpoch, reqEpoch);
  }

  SWalHead* pHeadWithCkSum = taosMemoryMalloc(sizeof(SWalHead) + 2048);
  if (pHeadWithCkSum == NULL) {
    return -1;
  }

  walSetReaderCapacity(pExec->pWalReader, 2048);

  SMqDataBlkRsp rsp = {0};
  rsp.reqOffset = pReq->currentOffset;
  rsp.withSchema = pExec->withSchema;
  rsp.blockData = taosArrayInit(0, sizeof(void*));
  rsp.blockDataLen = taosArrayInit(0, sizeof(int32_t));
  rsp.blockSchema = taosArrayInit(0, sizeof(void*));

  while (1) {
    consumerEpoch = atomic_load_32(&pExec->epoch);
    if (consumerEpoch > reqEpoch) {
      tqDebug("tmq poll: consumer %ld (epoch %d) vg %d offset %ld, found new consumer epoch %d discard req epoch %d",
              consumerId, pReq->epoch, TD_VID(pTq->pVnode), fetchOffset, consumerEpoch, reqEpoch);
      break;
    }

    taosThreadMutexLock(&pExec->pWalReader->mutex);

    if (walFetchHead(pExec->pWalReader, fetchOffset, pHeadWithCkSum) < 0) {
      tqDebug("tmq poll: consumer %ld (epoch %d) vg %d offset %ld, no more log to return", consumerId, pReq->epoch,
              TD_VID(pTq->pVnode), fetchOffset);
      taosThreadMutexUnlock(&pExec->pWalReader->mutex);
      break;
    }

    if (pHeadWithCkSum->head.msgType != TDMT_VND_SUBMIT) {
      walSkipFetchBody(pExec->pWalReader, pHeadWithCkSum);
    } else {
      walFetchBody(pExec->pWalReader, &pHeadWithCkSum);
    }

    SWalReadHead* pHead = &pHeadWithCkSum->head;

    taosThreadMutexUnlock(&pExec->pWalReader->mutex);

#if 0
    SWalReadHead* pHead;
    if (walReadWithHandle_s(pExec->pWalReader, fetchOffset, &pHead) < 0) {
      // TODO: no more log, set timer to wait blocking time
      // if data inserted during waiting, launch query and
      // response to user
      tqDebug("tmq poll: consumer %ld (epoch %d) vg %d offset %ld, no more log to return", consumerId, pReq->epoch,
             TD_VID(pTq->pVnode), fetchOffset);

#if 0
      // add to pushMgr
      taosWLockLatch(&pExec->pushHandle.lock);

      pExec->pushHandle.consumerId = consumerId;
      pExec->pushHandle.epoch = reqEpoch;
      pExec->pushHandle.reqOffset = rsp.reqOffset;
      pExec->pushHandle.skipLogNum = rsp.skipLogNum;
      pExec->pushHandle.handle = pMsg;

      taosWUnLockLatch(&pExec->pushHandle.lock);

      // TODO add timer

      // TODO: the pointer will always be valid?
      taosHashPut(pTq->pushMgr, &consumerId, sizeof(int64_t), &pExec, sizeof(void*));
      taosArrayDestroy(rsp.blockData);
      taosArrayDestroy(rsp.blockDataLen);
      return 0;
#endif

    break;
  }
#endif

    tqDebug("tmq poll: consumer %ld (epoch %d) iter log, vg %d offset %ld msgType %d", consumerId, pReq->epoch,
            TD_VID(pTq->pVnode), fetchOffset, pHead->msgType);

    if (pHead->msgType == TDMT_VND_SUBMIT) {
      SSubmitReq* pCont = (SSubmitReq*)&pHead->body;
      // table subscribe
      if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
        qTaskInfo_t task = pExec->task[workerId];
        ASSERT(task);
        qSetStreamInput(task, pCont, STREAM_DATA_TYPE_SUBMIT_BLOCK);
        while (1) {
          SSDataBlock* pDataBlock = NULL;
          uint64_t     ts = 0;
          if (qExecTask(task, &pDataBlock, &ts) < 0) {
            ASSERT(0);
          }
          if (pDataBlock == NULL) break;

          ASSERT(pDataBlock->info.rows != 0);
          ASSERT(pDataBlock->info.numOfCols != 0);

          int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pDataBlock);
          void*              buf = taosMemoryCalloc(1, dataStrLen);
          SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
          pRetrieve->useconds = ts;
          pRetrieve->precision = TSDB_DEFAULT_PRECISION;
          pRetrieve->compressed = 0;
          pRetrieve->completed = 1;
          pRetrieve->numOfRows = htonl(pDataBlock->info.rows);

          // TODO enable compress
          int32_t actualLen = 0;
          blockCompressEncode(pDataBlock, pRetrieve->data, &actualLen, pDataBlock->info.numOfCols, false);
          actualLen += sizeof(SRetrieveTableRsp);
          ASSERT(actualLen <= dataStrLen);
          taosArrayPush(rsp.blockDataLen, &actualLen);
          taosArrayPush(rsp.blockData, &buf);

          if (pExec->withSchema) {
            SSchemaWrapper* pSW = tCloneSSchemaWrapper(pExec->pExecReader[workerId]->pSchemaWrapper);
            taosArrayPush(rsp.blockSchema, &pSW);
          }

          rsp.blockNum++;
        }
        // db subscribe
      } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
        STqReadHandle* pReader = pExec->pExecReader[workerId];
        tqReadHandleSetMsg(pReader, pCont, 0);
        while (tqNextDataBlock(pReader)) {
          SSDataBlock block = {0};
          if (tqRetrieveDataBlock(&block.pDataBlock, pReader, &block.info.groupId, &block.info.uid, &block.info.rows,
                                  &block.info.numOfCols) < 0) {
            ASSERT(0);
          }
          int32_t            dataStrLen = sizeof(SRetrieveTableRsp) + blockGetEncodeSize(&block);
          void*              buf = taosMemoryCalloc(1, dataStrLen);
          SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)buf;
          /*pRetrieve->useconds = 0;*/
          pRetrieve->precision = TSDB_DEFAULT_PRECISION;
          pRetrieve->compressed = 0;
          pRetrieve->completed = 1;
          pRetrieve->numOfRows = htonl(block.info.rows);

          // TODO enable compress
          int32_t actualLen = 0;
          blockCompressEncode(&block, pRetrieve->data, &actualLen, block.info.numOfCols, false);
          actualLen += sizeof(SRetrieveTableRsp);
          ASSERT(actualLen <= dataStrLen);
          taosArrayPush(rsp.blockDataLen, &actualLen);
          taosArrayPush(rsp.blockData, &buf);

          SSchemaWrapper* pSW = tCloneSSchemaWrapper(pExec->pExecReader[workerId]->pSchemaWrapper);
          taosArrayPush(rsp.blockSchema, &pSW);

          rsp.blockNum++;
        }
      } else {
        ASSERT(0);
      }
    }

    // TODO batch optimization:
    // TODO continue scan until meeting batch requirement
    if (rsp.blockNum != 0) break;
    rsp.skipLogNum++;
    fetchOffset++;
  }

  ASSERT(taosArrayGetSize(rsp.blockData) == rsp.blockNum);
  ASSERT(taosArrayGetSize(rsp.blockDataLen) == rsp.blockNum);

  if (rsp.blockNum != 0)
    rsp.rspOffset = fetchOffset;
  else
    rsp.rspOffset = fetchOffset - 1;

  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqDataBlkRsp(NULL, &rsp);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    pMsg->code = -1;
    return -1;
  }

  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;
  ((SMqRspHead*)buf)->consumerId = consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqDataBlkRsp(&abuf, &rsp);
  pMsg->pCont = buf;
  pMsg->contLen = tlen;
  pMsg->code = 0;
  tmsgSendRsp(pMsg);

  tqDebug("vg %d offset %ld from consumer %ld (epoch %d) send rsp, block num: %d, reqOffset: %ld, rspOffset: %ld",
          TD_VID(pTq->pVnode), fetchOffset, consumerId, pReq->epoch, rsp.blockNum, rsp.reqOffset, rsp.rspOffset);

  // TODO destroy
  taosArrayDestroy(rsp.blockData);
  taosArrayDestroy(rsp.blockDataLen);
  taosArrayDestroyP(rsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);

  return 0;
}

#if 0
int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg, int32_t workerId) {
  SMqPollReq* pReq = pMsg->pCont;
  int64_t     consumerId = pReq->consumerId;
  int64_t     fetchOffset;
  int64_t     blockingTime = pReq->blockingTime;
  int32_t     reqEpoch = pReq->epoch;

  if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__EARLIEAST) {
    fetchOffset = walGetFirstVer(pTq->pWal);
  } else if (pReq->currentOffset == TMQ_CONF__RESET_OFFSET__LATEST) {
    fetchOffset = walGetLastVer(pTq->pWal);
  } else {
    fetchOffset = pReq->currentOffset + 1;
  }

  tqDebug("tmq poll: consumer %ld (epoch %d) recv poll req in vg %d, req %ld %ld", consumerId, pReq->epoch,
         TD_VID(pTq->pVnode), pReq->currentOffset, fetchOffset);

  SMqPollRspV2 rspV2 = {0};
  rspV2.dataLen = 0;

  STqConsumer* pConsumer = tqHandleGet(pTq->tqMeta, consumerId);
  if (pConsumer == NULL) {
    vWarn("tmq poll: consumer %ld (epoch %d) not found in vg %d", consumerId, pReq->epoch, TD_VID(pTq->pVnode));
    pMsg->pCont = NULL;
    pMsg->contLen = 0;
    pMsg->code = -1;
    tmsgSendRsp(pMsg);
    return 0;
  }

  int32_t consumerEpoch = atomic_load_32(&pConsumer->epoch);
  while (consumerEpoch < reqEpoch) {
    consumerEpoch = atomic_val_compare_exchange_32(&pConsumer->epoch, consumerEpoch, reqEpoch);
  }

  STqTopic* pTopic = NULL;
  int32_t   topicSz = taosArrayGetSize(pConsumer->topics);
  for (int32_t i = 0; i < topicSz; i++) {
    STqTopic* topic = taosArrayGet(pConsumer->topics, i);
    // TODO race condition
    ASSERT(pConsumer->consumerId == consumerId);
    if (strcmp(topic->topicName, pReq->topic) == 0) {
      pTopic = topic;
      break;
    }
  }
  if (pTopic == NULL) {
    vWarn("tmq poll: consumer %ld (epoch %d) topic %s not found in vg %d", consumerId, pReq->epoch, pReq->topic,
          TD_VID(pTq->pVnode));
    pMsg->pCont = NULL;
    pMsg->contLen = 0;
    pMsg->code = -1;
    tmsgSendRsp(pMsg);
    return 0;
  }

  tqDebug("poll topic %s from consumer %ld (epoch %d) vg %d", pTopic->topicName, consumerId, pReq->epoch,
         TD_VID(pTq->pVnode));

  rspV2.reqOffset = pReq->currentOffset;
  rspV2.skipLogNum = 0;

  while (1) {
    /*if (fetchOffset > walGetLastVer(pTq->pWal) || walReadWithHandle(pTopic->pReadhandle, fetchOffset) < 0) {*/
    // TODO
    consumerEpoch = atomic_load_32(&pConsumer->epoch);
    if (consumerEpoch > reqEpoch) {
      tqDebug("tmq poll: consumer %ld (epoch %d) vg %d offset %ld, found new consumer epoch %d discard req epoch %d",
             consumerId, pReq->epoch, TD_VID(pTq->pVnode), fetchOffset, consumerEpoch, reqEpoch);
      break;
    }
    SWalReadHead* pHead;
    if (walReadWithHandle_s(pTopic->pReadhandle, fetchOffset, &pHead) < 0) {
      // TODO: no more log, set timer to wait blocking time
      // if data inserted during waiting, launch query and
      // response to user
      tqDebug("tmq poll: consumer %ld (epoch %d) vg %d offset %ld, no more log to return", consumerId, pReq->epoch,
             TD_VID(pTq->pVnode), fetchOffset);
      break;
    }
    tqDebug("tmq poll: consumer %ld (epoch %d) iter log, vg %d offset %ld msgType %d", consumerId, pReq->epoch,
           TD_VID(pTq->pVnode), fetchOffset, pHead->msgType);
    /*int8_t pos = fetchOffset % TQ_BUFFER_SIZE;*/
    /*pHead = pTopic->pReadhandle->pHead;*/
    if (pHead->msgType == TDMT_VND_SUBMIT) {
      SSubmitReq* pCont = (SSubmitReq*)&pHead->body;
      qTaskInfo_t task = pTopic->buffer.output[workerId].task;
      ASSERT(task);
      qSetStreamInput(task, pCont, STREAM_DATA_TYPE_SUBMIT_BLOCK);
      SArray* pRes = taosArrayInit(0, sizeof(SSDataBlock));
      while (1) {
        SSDataBlock* pDataBlock = NULL;
        uint64_t     ts;
        if (qExecTask(task, &pDataBlock, &ts) < 0) {
          ASSERT(false);
        }
        if (pDataBlock == NULL) {
          /*pos = fetchOffset % TQ_BUFFER_SIZE;*/
          break;
        }

        taosArrayPush(pRes, pDataBlock);
      }

      if (taosArrayGetSize(pRes) == 0) {
        tqDebug("tmq poll: consumer %ld (epoch %d) iter log, vg %d skip log %ld since not wanted", consumerId,
               pReq->epoch, TD_VID(pTq->pVnode), fetchOffset);
        fetchOffset++;
        rspV2.skipLogNum++;
        taosArrayDestroy(pRes);
        continue;
      }
      rspV2.rspOffset = fetchOffset;

      int32_t blockSz = taosArrayGetSize(pRes);
      int32_t dataBlockStrLen = 0;
      for (int32_t i = 0; i < blockSz; i++) {
        SSDataBlock* pBlock = taosArrayGet(pRes, i);
        dataBlockStrLen += sizeof(SRetrieveTableRsp) + blockGetEncodeSize(pBlock);
      }

      void* dataBlockBuf = taosMemoryMalloc(dataBlockStrLen);
      if (dataBlockBuf == NULL) {
        pMsg->code = -1;
        taosMemoryFree(pHead);
      }

      rspV2.blockData = dataBlockBuf;

      int32_t pos;
      rspV2.blockPos = taosArrayInit(blockSz, sizeof(int32_t));
      for (int32_t i = 0; i < blockSz; i++) {
        pos = 0;
        SSDataBlock*       pBlock = taosArrayGet(pRes, i);
        SRetrieveTableRsp* pRetrieve = (SRetrieveTableRsp*)dataBlockBuf;
        pRetrieve->useconds = 0;
        pRetrieve->precision = 0;
        pRetrieve->compressed = 0;
        pRetrieve->completed = 1;
        pRetrieve->numOfRows = htonl(pBlock->info.rows);
        blockCompressEncode(pBlock, pRetrieve->data, &pos, pBlock->info.numOfCols, false);
        taosArrayPush(rspV2.blockPos, &rspV2.dataLen);

        int32_t totLen = sizeof(SRetrieveTableRsp) + pos;
        pRetrieve->compLen = htonl(totLen);
        rspV2.dataLen += totLen;
        dataBlockBuf = POINTER_SHIFT(dataBlockBuf, totLen);
      }
      ASSERT(POINTER_DISTANCE(dataBlockBuf, rspV2.blockData) <= dataBlockStrLen);

      int32_t msgLen = sizeof(SMqRspHead) + tEncodeSMqPollRspV2(NULL, &rspV2);
      void*   buf = rpcMallocCont(msgLen);

      ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
      ((SMqRspHead*)buf)->epoch = pReq->epoch;
      ((SMqRspHead*)buf)->consumerId = consumerId;

      void* msgBodyBuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
      tEncodeSMqPollRspV2(&msgBodyBuf, &rspV2);

      /*rsp.pBlockData = pRes;*/

      /*taosArrayDestroyEx(rsp.pBlockData, (void (*)(void*))tDeleteSSDataBlock);*/
      pMsg->pCont = buf;
      pMsg->contLen = msgLen;
      pMsg->code = 0;
      tqDebug("vg %d offset %ld msgType %d from consumer %ld (epoch %d) actual rsp", TD_VID(pTq->pVnode), fetchOffset,
             pHead->msgType, consumerId, pReq->epoch);
      tmsgSendRsp(pMsg);
      taosMemoryFree(pHead);
      return 0;
    } else {
      taosMemoryFree(pHead);
      fetchOffset++;
      rspV2.skipLogNum++;
    }
  }

  /*if (blockingTime != 0) {*/
  /*tqAddClientPusher(pTq->tqPushMgr, pMsg, consumerId, blockingTime);*/
  /*} else {*/

  rspV2.rspOffset = fetchOffset - 1;

  int32_t tlen = sizeof(SMqRspHead) + tEncodeSMqPollRspV2(NULL, &rspV2);
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    pMsg->code = -1;
    return -1;
  }
  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;
  ((SMqRspHead*)buf)->consumerId = consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
  tEncodeSMqPollRspV2(&abuf, &rspV2);
  pMsg->pCont = buf;
  pMsg->contLen = tlen;
  pMsg->code = 0;
  tmsgSendRsp(pMsg);
  tqDebug("vg %d offset %ld from consumer %ld (epoch %d) not rsp", TD_VID(pTq->pVnode), fetchOffset, consumerId,
         pReq->epoch);
  /*}*/

  return 0;
}
#endif

// TODO: persist meta into tdb
int32_t tqProcessVgChangeReq(STQ* pTq, char* msg, int32_t msgLen) {
  SMqRebVgReq req = {0};
  tDecodeSMqRebVgReq(msg, &req);
  // todo lock
  STqExec* pExec = taosHashGet(pTq->execs, req.subKey, strlen(req.subKey));
  if (pExec == NULL) {
    ASSERT(req.oldConsumerId == -1);
    ASSERT(req.newConsumerId != -1);
    STqExec exec = {0};
    pExec = &exec;
    /*taosInitRWLatch(&pExec->lock);*/

    memcpy(pExec->subKey, req.subKey, TSDB_SUBSCRIBE_KEY_LEN);
    pExec->consumerId = req.newConsumerId;
    pExec->epoch = -1;

    pExec->subType = req.subType;
    pExec->withTbName = req.withTbName;
    pExec->withSchema = req.withSchema;
    pExec->withTag = req.withTag;

    pExec->qmsg = req.qmsg;
    req.qmsg = NULL;

    pExec->pWalReader = walOpenReadHandle(pTq->pVnode->pWal);
    for (int32_t i = 0; i < 5; i++) {
      pExec->pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
      if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
        SReadHandle handle = {
            .reader = pExec->pExecReader[i],
            .meta = pTq->pVnode->pMeta,
            .pMsgCb = &pTq->pVnode->msgCb,
        };
        pExec->task[i] = qCreateStreamExecTaskInfo(pExec->qmsg, &handle);
        ASSERT(pExec->task[i]);
      } else {
        pExec->task[i] = NULL;
      }
    }
    taosHashPut(pTq->execs, req.subKey, strlen(req.subKey), pExec, sizeof(STqExec));
    return 0;
  } else {
    /*if (req.newConsumerId != -1) {*/
    /*taosWLockLatch(&pExec->lock);*/
    ASSERT(pExec->consumerId == req.oldConsumerId);
    // TODO handle qmsg and exec modification
    atomic_store_32(&pExec->epoch, -1);
    atomic_store_64(&pExec->consumerId, req.newConsumerId);
    atomic_add_fetch_32(&pExec->epoch, 1);
    /*taosWUnLockLatch(&pExec->lock);*/
    return 0;
    /*} else {*/
    // TODO
    /*taosHashRemove(pTq->tqMetaNew, req.subKey, strlen(req.subKey));*/
    /*return 0;*/
    /*}*/
  }
}

void tqTableSink(SStreamTask* pTask, void* vnode, int64_t ver, void* data) {
  const SArray* pRes = (const SArray*)data;
  SVnode*       pVnode = (SVnode*)vnode;

  ASSERT(pTask->tbSink.pTSchema);
  SSubmitReq* pReq = tdBlockToSubmit(pRes, pTask->tbSink.pTSchema, true, pTask->tbSink.stbUid, pVnode->config.vgId);
  /*tPrintFixedSchemaSubmitReq(pReq, pTask->tbSink.pTSchema);*/
  // build write msg
  SRpcMsg msg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont = pReq,
      .contLen = ntohl(pReq->length),
  };

  ASSERT(tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) == 0);
}

int32_t tqExpandTask(STQ* pTq, SStreamTask* pTask, int32_t parallel) {
  if (pTask->execType != TASK_EXEC__NONE) {
    // expand runners
    pTask->exec.numOfRunners = parallel;
    pTask->exec.runners = taosMemoryCalloc(parallel, sizeof(SStreamRunner));
    if (pTask->exec.runners == NULL) {
      return -1;
    }
    for (int32_t i = 0; i < parallel; i++) {
      STqReadHandle* pStreamReader = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
      SReadHandle    handle = {
             .reader = pStreamReader,
             .meta = pTq->pVnode->pMeta,
             .pMsgCb = &pTq->pVnode->msgCb,
      };
      pTask->exec.runners[i].inputHandle = pStreamReader;
      pTask->exec.runners[i].executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle);
      ASSERT(pTask->exec.runners[i].executor);
    }
  }

  if (pTask->sinkType == TASK_SINK__TABLE) {
    pTask->tbSink.vnode = pTq->pVnode;
    pTask->tbSink.tbSinkFunc = tqTableSink;
  }

  return 0;
}

int32_t tqProcessTaskDeploy(STQ* pTq, char* msg, int32_t msgLen) {
  SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeSStreamTask(&decoder, pTask) < 0) {
    ASSERT(0);
  }
  tDecoderClear(&decoder);

  // exec
  if (tqExpandTask(pTq, pTask, 4) < 0) {
    ASSERT(0);
  }

  // sink
  pTask->ahandle = pTq->pVnode;
  if (pTask->sinkType == TASK_SINK__SMA) {
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->sinkType == TASK_SINK__TABLE) {
    ASSERT(pTask->tbSink.pSchemaWrapper);
    ASSERT(pTask->tbSink.pSchemaWrapper->pSchema);
    pTask->tbSink.pTSchema =
        tdGetSTSChemaFromSSChema(&pTask->tbSink.pSchemaWrapper->pSchema, pTask->tbSink.pSchemaWrapper->nCols);
    ASSERT(pTask->tbSink.pTSchema);
  }

  taosHashPut(pTq->pStreamTasks, &pTask->taskId, sizeof(int32_t), pTask, sizeof(SStreamTask));

  return 0;
}

int32_t tqProcessStreamTrigger(STQ* pTq, void* data, int32_t dataLen, int32_t workerId) {
  void* pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pTq->pStreamTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = (SStreamTask*)pIter;

    if (streamExecTask(pTask, &pTq->pVnode->msgCb, data, STREAM_DATA_TYPE_SUBMIT_BLOCK, workerId) < 0) {
      // TODO
    }
  }
  return 0;
}

int32_t tqProcessTaskExec(STQ* pTq, char* msg, int32_t msgLen, int32_t workerId) {
  SStreamTaskExecReq req;
  tDecodeSStreamTaskExecReq(msg, &req);

  int32_t taskId = req.taskId;
  ASSERT(taskId);

  SStreamTask* pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  ASSERT(pTask);

  if (streamExecTask(pTask, &pTq->pVnode->msgCb, req.data, STREAM_DATA_TYPE_SSDATA_BLOCK, workerId) < 0) {
    // TODO
  }
  return 0;
}
