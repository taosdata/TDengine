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
#include "tdbInt.h"

int32_t tqInit() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tqMgmt.inited, 0, 2);
    if (old != 2) break;
  }

  if (old == 0) {
    tqMgmt.timer = taosTmrInit(10000, 100, 10000, "TQ");
    if (tqMgmt.timer == NULL) {
      atomic_store_8(&tqMgmt.inited, 0);
      return -1;
    }
    atomic_store_8(&tqMgmt.inited, 1);
  }
  return 0;
}

void tqCleanUp() {
  int8_t old;
  while (1) {
    old = atomic_val_compare_exchange_8(&tqMgmt.inited, 1, 2);
    if (old != 2) break;
  }

  if (old == 1) {
    taosTmrCleanUp(tqMgmt.timer);
    atomic_store_8(&tqMgmt.inited, 0);
  }
}

int tqExecKeyCompare(const void* pKey1, int32_t kLen1, const void* pKey2, int32_t kLen2) {
  return strcmp(pKey1, pKey2);
}

int32_t tqStoreExec(STQ* pTq, const char* key, const STqExec* pExec) {
  int32_t code;
  int32_t vlen;
  tEncodeSize(tEncodeSTqExec, pExec, vlen, code);
  ASSERT(code == 0);

  void* buf = taosMemoryCalloc(1, vlen);
  if (buf == NULL) {
    ASSERT(0);
  }

  SEncoder encoder;
  tEncoderInit(&encoder, buf, vlen);

  if (tEncodeSTqExec(&encoder, pExec) < 0) {
    ASSERT(0);
  }

  TXN txn;

  if (tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    ASSERT(0);
  }

  if (tdbBegin(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  if (tdbTbUpsert(pTq->pExecStore, key, (int)strlen(key), buf, vlen, &txn) < 0) {
    ASSERT(0);
  }

  if (tdbCommit(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  tEncoderClear(&encoder);
  taosMemoryFree(buf);
  return 0;
}

STQ* tqOpen(const char* path, SVnode* pVnode, SWal* pWal) {
  STQ* pTq = taosMemoryMalloc(sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_TQ_OUT_OF_MEMORY;
    return NULL;
  }
  pTq->path = strdup(path);
  pTq->pVnode = pVnode;
  pTq->pWal = pWal;

  pTq->execs = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);

  pTq->pStreamTasks = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);

  pTq->pushMgr = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);

  if (tdbOpen(path, 16 * 1024, 1, &pTq->pMetaStore) < 0) {
    ASSERT(0);
  }

  if (tdbTbOpen("exec", -1, -1, tqExecKeyCompare, pTq->pMetaStore, &pTq->pExecStore) < 0) {
    ASSERT(0);
  }

  TXN txn;

  if (tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, 0) < 0) {
    ASSERT(0);
  }

  /*if (tdbBegin(pTq->pMetaStore, &txn) < 0) {*/
  /*ASSERT(0);*/
  /*}*/

  TBC* pCur;
  if (tdbTbcOpen(pTq->pExecStore, &pCur, &txn) < 0) {
    ASSERT(0);
  }

  void* pKey;
  int   kLen;
  void* pVal;
  int   vLen;

  tdbTbcMoveToFirst(pCur);
  SDecoder decoder;
  while (tdbTbcNext(pCur, &pKey, &kLen, &pVal, &vLen) == 0) {
    STqExec exec;
    tDecoderInit(&decoder, (uint8_t*)pVal, vLen);
    tDecodeSTqExec(&decoder, &exec);
    exec.pWalReader = walOpenReadHandle(pTq->pVnode->pWal);
    if (exec.subType == TOPIC_SUB_TYPE__TABLE) {
      for (int32_t i = 0; i < 5; i++) {
        exec.pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);

        SReadHandle handle = {
            .reader = exec.pExecReader[i],
            .meta = pTq->pVnode->pMeta,
            .pMsgCb = &pTq->pVnode->msgCb,
        };
        exec.task[i] = qCreateStreamExecTaskInfo(exec.qmsg, &handle);
        ASSERT(exec.task[i]);
      }
    } else {
      for (int32_t i = 0; i < 5; i++) {
        exec.pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
      }
      exec.pDropTbUid = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    }
    taosHashPut(pTq->execs, pKey, kLen, &exec, sizeof(STqExec));
  }

  if (tdbTxnClose(&txn) < 0) {
    ASSERT(0);
  }

  return pTq;
}

void tqClose(STQ* pTq) {
  if (pTq) {
    taosMemoryFreeClear(pTq->path);
    taosHashCleanup(pTq->execs);
    taosHashCleanup(pTq->pStreamTasks);
    taosHashCleanup(pTq->pushMgr);
    tdbClose(pTq->pMetaStore);
    taosMemoryFree(pTq);
  }
  // TODO
}

int32_t tEncodeSTqExec(SEncoder* pEncoder, const STqExec* pExec) {
  if (tStartEncode(pEncoder) < 0) return -1;
  if (tEncodeCStr(pEncoder, pExec->subKey) < 0) return -1;
  if (tEncodeI64(pEncoder, pExec->consumerId) < 0) return -1;
  if (tEncodeI32(pEncoder, pExec->epoch) < 0) return -1;
  if (tEncodeI8(pEncoder, pExec->subType) < 0) return -1;
  if (tEncodeI8(pEncoder, pExec->withTbName) < 0) return -1;
  if (tEncodeI8(pEncoder, pExec->withSchema) < 0) return -1;
  if (tEncodeI8(pEncoder, pExec->withTag) < 0) return -1;
  if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    if (tEncodeCStr(pEncoder, pExec->qmsg) < 0) return -1;
  }
  tEndEncode(pEncoder);
  return pEncoder->pos;
}

int32_t tDecodeSTqExec(SDecoder* pDecoder, STqExec* pExec) {
  if (tStartDecode(pDecoder) < 0) return -1;
  if (tDecodeCStrTo(pDecoder, pExec->subKey) < 0) return -1;
  if (tDecodeI64(pDecoder, &pExec->consumerId) < 0) return -1;
  if (tDecodeI32(pDecoder, &pExec->epoch) < 0) return -1;
  if (tDecodeI8(pDecoder, &pExec->subType) < 0) return -1;
  if (tDecodeI8(pDecoder, &pExec->withTbName) < 0) return -1;
  if (tDecodeI8(pDecoder, &pExec->withSchema) < 0) return -1;
  if (tDecodeI8(pDecoder, &pExec->withTag) < 0) return -1;
  if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
    if (tDecodeCStrAlloc(pDecoder, &pExec->qmsg) < 0) return -1;
  }
  tEndDecode(pDecoder);
  return 0;
}
int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd) {
  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pTq->execs, pIter);
    if (pIter == NULL) break;
    STqExec* pExec = (STqExec*)pIter;
    if (pExec->subType == TOPIC_SUB_TYPE__DB) {
      if (!isAdd) {
        int32_t sz = taosArrayGetSize(tbUidList);
        for (int32_t i = 0; i < sz; i++) {
          int64_t tbUid = *(int64_t*)taosArrayGet(tbUidList, i);
          taosHashPut(pExec->pDropTbUid, &tbUid, sizeof(int64_t), NULL, 0);
        }
      }
    } else {
      for (int32_t i = 0; i < 5; i++) {
        int32_t code = qUpdateQualifiedTableId(pExec->task[i], tbUidList, isAdd);
        ASSERT(code == 0);
      }
    }
  }
  return 0;
}

int32_t tqPushMsgNew(STQ* pTq, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver, SRpcHandleInfo handleInfo) {
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
      qSetStreamInput(task, pReq, STREAM_DATA_TYPE_SUBMIT_BLOCK, false);
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

    SRpcMsg resp = {.info = handleInfo, .pCont = buf, .contLen = tlen, .code = 0};
    tmsgSendRsp(&resp);

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
  if (msgType == TDMT_VND_SUBMIT) {
    if (taosHashGetSize(pTq->pStreamTasks) == 0) return 0;

    if (tdUpdateExpireWindow(pTq->pVnode->pSma, msg, ver) != 0) {
      // TODO handle sma error
    }
    void* data = taosMemoryMalloc(msgLen);
    if (data == NULL) {
      return -1;
    }
    memcpy(data, msg, msgLen);

    tqProcessStreamTrigger(pTq, data);
  }

  return 0;
}

int tqCommit(STQ* pTq) {
  // do nothing
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
    fetchOffset = walGetCommittedVer(pTq->pWal);
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
  rsp.blockTbName = taosArrayInit(0, sizeof(void*));

  int8_t withTbName = pExec->withTbName;
  if (pReq->withTbName != -1) {
    withTbName = pReq->withTbName;
  }
  rsp.withTbName = withTbName;

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
      ASSERT(walSkipFetchBody(pExec->pWalReader, pHeadWithCkSum) == 0);
    } else {
      ASSERT(walFetchBody(pExec->pWalReader, &pHeadWithCkSum) == 0);
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
        qSetStreamInput(task, pCont, STREAM_DATA_TYPE_SUBMIT_BLOCK, false);
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

          if (withTbName) {
            SMetaReader mr = {0};
            metaReaderInit(&mr, pTq->pVnode->pMeta, 0);
            int64_t uid = pExec->pExecReader[workerId]->msgIter.uid;
            if (metaGetTableEntryByUid(&mr, uid) < 0) {
              ASSERT(0);
            }
            char* tbName = strdup(mr.me.name);
            taosArrayPush(rsp.blockTbName, &tbName);
            metaReaderClear(&mr);
          }

          rsp.blockNum++;
        }
        // db subscribe
      } else if (pExec->subType == TOPIC_SUB_TYPE__DB) {
        rsp.withSchema = 1;
        STqReadHandle* pReader = pExec->pExecReader[workerId];
        tqReadHandleSetMsg(pReader, pCont, 0);
        while (tqNextDataBlockFilterOut(pReader, pExec->pDropTbUid)) {
          SSDataBlock block = {0};
          if (tqRetrieveDataBlock(&block.pDataBlock, pReader, &block.info.groupId, &block.info.uid, &block.info.rows,
                                  &block.info.numOfCols) < 0) {
            if (terrno == TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND) continue;
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
          if (withTbName) {
            SMetaReader mr = {0};
            metaReaderInit(&mr, pTq->pVnode->pMeta, 0);
            if (metaGetTableEntryByUid(&mr, block.info.uid) < 0) {
              ASSERT(0);
            }
            char* tbName = strdup(mr.me.name);
            taosArrayPush(rsp.blockTbName, &tbName);
            metaReaderClear(&mr);
          }

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

  taosMemoryFree(pHeadWithCkSum);
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

  SRpcMsg resp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};
  tmsgSendRsp(&resp);

  tqDebug("vg %d offset %ld from consumer %ld (epoch %d) send rsp, block num: %d, reqOffset: %ld, rspOffset: %ld",
          TD_VID(pTq->pVnode), fetchOffset, consumerId, pReq->epoch, rsp.blockNum, rsp.reqOffset, rsp.rspOffset);

  // TODO destroy
  taosArrayDestroy(rsp.blockData);
  taosArrayDestroy(rsp.blockDataLen);
  taosArrayDestroyP(rsp.blockSchema, (FDelete)tDeleteSSchemaWrapper);
  taosArrayDestroyP(rsp.blockTbName, (FDelete)taosMemoryFree);

  return 0;
}

int32_t tqProcessVgDeleteReq(STQ* pTq, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;

  int32_t code = taosHashRemove(pTq->execs, pReq->subKey, strlen(pReq->subKey));
  ASSERT(code == 0);

  TXN txn;

  if (tdbTxnOpen(&txn, 0, tdbDefaultMalloc, tdbDefaultFree, NULL, TDB_TXN_WRITE | TDB_TXN_READ_UNCOMMITTED) < 0) {
    ASSERT(0);
  }

  if (tdbBegin(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  if (tdbTbDelete(pTq->pExecStore, pReq->subKey, (int)strlen(pReq->subKey), &txn) < 0) {
    /*ASSERT(0);*/
  }

  if (tdbCommit(pTq->pMetaStore, &txn) < 0) {
    ASSERT(0);
  }

  return 0;
}

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
    if (pExec->subType == TOPIC_SUB_TYPE__TABLE) {
      for (int32_t i = 0; i < 5; i++) {
        pExec->pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);

        SReadHandle handle = {
            .reader = pExec->pExecReader[i],
            .meta = pTq->pVnode->pMeta,
            .pMsgCb = &pTq->pVnode->msgCb,
        };
        pExec->task[i] = qCreateStreamExecTaskInfo(pExec->qmsg, &handle);
        ASSERT(pExec->task[i]);
      }
    } else {
      for (int32_t i = 0; i < 5; i++) {
        pExec->pExecReader[i] = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
      }
      pExec->pDropTbUid = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    }
    taosHashPut(pTq->execs, req.subKey, strlen(req.subKey), pExec, sizeof(STqExec));

    if (tqStoreExec(pTq, req.subKey, pExec) < 0) {
      // TODO
    }
    return 0;
  } else {
    /*ASSERT(pExec->consumerId == req.oldConsumerId);*/
    // TODO handle qmsg and exec modification
    atomic_store_32(&pExec->epoch, -1);
    atomic_store_64(&pExec->consumerId, req.newConsumerId);
    atomic_add_fetch_32(&pExec->epoch, 1);

    if (tqStoreExec(pTq, req.subKey, pExec) < 0) {
      // TODO
    }
    return 0;
  }
}

void tqTableSink(SStreamTask* pTask, void* vnode, int64_t ver, void* data) {
  const SArray* pRes = (const SArray*)data;
  SVnode*       pVnode = (SVnode*)vnode;

  ASSERT(pTask->tbSink.pTSchema);
  SSubmitReq* pReq = tdBlockToSubmit(pRes, pTask->tbSink.pTSchema, true, pTask->tbSink.stbUid,
                                     pTask->tbSink.stbFullName, pVnode->config.vgId);
  /*tPrintFixedSchemaSubmitReq(pReq, pTask->tbSink.pTSchema);*/
  // build write msg
  SRpcMsg msg = {
      .msgType = TDMT_VND_SUBMIT,
      .pCont = pReq,
      .contLen = ntohl(pReq->length),
  };

  ASSERT(tmsgPutToQueue(&pVnode->msgCb, WRITE_QUEUE, &msg) == 0);
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

  pTask->status = TASK_STATUS__IDLE;
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;

  pTask->inputQ = taosOpenQueue();
  pTask->outputQ = taosOpenQueue();
  pTask->inputQAll = taosAllocateQall();
  pTask->outputQAll = taosAllocateQall();

  if (pTask->inputQ == NULL || pTask->outputQ == NULL || pTask->inputQAll == NULL || pTask->outputQAll == NULL)
    goto FAIL;

  // exec
  if (pTask->execType != TASK_EXEC__NONE) {
    // expand runners
    STqReadHandle* pStreamReader = tqInitSubmitMsgScanner(pTq->pVnode->pMeta);
    SReadHandle    handle = {
           .reader = pStreamReader,
           .meta = pTq->pVnode->pMeta,
           .pMsgCb = &pTq->pVnode->msgCb,
           .vnode = pTq->pVnode,
    };
    pTask->exec.inputHandle = pStreamReader;
    pTask->exec.executor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle);
    ASSERT(pTask->exec.executor);
  }

  // sink
  pTask->ahandle = pTq->pVnode;
  if (pTask->sinkType == TASK_SINK__SMA) {
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->sinkType == TASK_SINK__TABLE) {
    pTask->tbSink.vnode = pTq->pVnode;
    pTask->tbSink.tbSinkFunc = tqTableSink;

    ASSERT(pTask->tbSink.pSchemaWrapper);
    ASSERT(pTask->tbSink.pSchemaWrapper->pSchema);

    pTask->tbSink.pTSchema =
        tdGetSTSChemaFromSSChema(&pTask->tbSink.pSchemaWrapper->pSchema, pTask->tbSink.pSchemaWrapper->nCols);
    ASSERT(pTask->tbSink.pTSchema);
  }

  taosHashPut(pTq->pStreamTasks, &pTask->taskId, sizeof(int32_t), pTask, sizeof(SStreamTask));

  return 0;
FAIL:
  if (pTask->inputQ) taosCloseQueue(pTask->inputQ);
  if (pTask->outputQ) taosCloseQueue(pTask->outputQ);
  if (pTask->inputQAll) taosFreeQall(pTask->inputQAll);
  if (pTask->outputQAll) taosFreeQall(pTask->outputQAll);
  if (pTask) taosMemoryFree(pTask);
  return -1;
}

int32_t tqProcessStreamTrigger(STQ* pTq, SSubmitReq* pReq) {
  void*              pIter = NULL;
  bool               failed = false;
  SStreamDataSubmit* pSubmit = NULL;

  pSubmit = streamDataSubmitNew(pReq);
  if (pSubmit == NULL) {
    failed = true;
  }

  while (1) {
    pIter = taosHashIterate(pTq->pStreamTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = (SStreamTask*)pIter;
    if (pTask->inputType != STREAM_INPUT__DATA_SUBMIT) continue;

    int8_t inputStatus = atomic_load_8(&pTask->inputStatus);
    if (inputStatus == TASK_INPUT_STATUS__NORMAL) {
      if (failed) {
        atomic_store_8(&pTask->inputStatus, TASK_INPUT_STATUS__FAILED);
        continue;
      }

      streamDataSubmitRefInc(pSubmit);
      SStreamDataSubmit* pSubmitClone = taosAllocateQitem(sizeof(SStreamDataSubmit), DEF_QITEM);
      memcpy(pSubmitClone, pSubmit, sizeof(SStreamDataSubmit));
      taosWriteQitem(pTask->inputQ, pSubmitClone);

      int8_t execStatus = atomic_load_8(&pTask->status);
      if (execStatus == TASK_STATUS__IDLE || execStatus == TASK_STATUS__CLOSING) {
        SStreamTaskRunReq* pRunReq = taosMemoryMalloc(sizeof(SStreamTaskRunReq));
        if (pRunReq == NULL) continue;
        // TODO: do we need htonl?
        pRunReq->head.vgId = pTq->pVnode->config.vgId;
        pRunReq->streamId = pTask->streamId;
        pRunReq->taskId = pTask->taskId;
        SRpcMsg msg = {
            .msgType = TDMT_VND_TASK_RUN,
            .pCont = pRunReq,
            .contLen = sizeof(SStreamTaskRunReq),
        };
        tmsgPutToQueue(&pTq->pVnode->msgCb, FETCH_QUEUE, &msg);
      }

    } else {
      // blocked or stopped, do nothing
    }
  }

  if (pSubmit) {
    streamDataSubmitRefDec(pSubmit);
    taosFreeQitem(pSubmit);
  }

  return failed ? -1 : 0;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  //
  SStreamTaskRunReq* pReq = pMsg->pCont;
  int32_t            taskId = pReq->taskId;
  SStreamTask*       pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamTaskProcessRunReq(pTask, &pTq->pVnode->msgCb);
  return 0;
}

int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamDispatchReq* pReq = pMsg->pCont;
  int32_t             taskId = pReq->taskId;
  SStreamTask*        pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessDispatchReq(pTask, &pTq->pVnode->msgCb, pReq, pMsg);
  return 0;
}

int32_t tqProcessTaskRecoverReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRecoverReq* pReq = pMsg->pCont;
  int32_t                taskId = pReq->taskId;
  SStreamTask*           pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessRecoverReq(pTask, &pTq->pVnode->msgCb, pReq, pMsg);
  return 0;
}

int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = pMsg->pCont;
  int32_t             taskId = pRsp->taskId;
  SStreamTask*        pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessDispatchRsp(pTask, &pTq->pVnode->msgCb, pRsp);
  return 0;
}

int32_t tqProcessTaskRecoverRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRecoverRsp* pRsp = pMsg->pCont;
  int32_t                taskId = pRsp->taskId;
  SStreamTask*           pTask = taosHashGet(pTq->pStreamTasks, &taskId, sizeof(int32_t));
  streamProcessRecoverRsp(pTask, pRsp);
  return 0;
}
