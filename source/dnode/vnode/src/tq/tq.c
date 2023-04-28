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

// 0: not init
// 1: already inited
// 2: wait to be inited or cleaup
#define WAL_READ_TASKS_ID       (-1)

static int32_t tqInitialize(STQ* pTq);

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
    if (streamInit() < 0) {
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
    streamCleanUp();
    atomic_store_8(&tqMgmt.inited, 0);
  }
}

static void destroyTqHandle(void* data) {
  STqHandle* pData = (STqHandle*)data;
  qDestroyTask(pData->execHandle.task);
  if (pData->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    taosMemoryFreeClear(pData->execHandle.execCol.qmsg);
  } else if (pData->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    tqCloseReader(pData->execHandle.pTqReader);
    walCloseReader(pData->pWalReader);
    taosHashCleanup(pData->execHandle.execDb.pFilterOutTbUid);
  } else if (pData->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    walCloseReader(pData->pWalReader);
    tqCloseReader(pData->execHandle.pTqReader);
  }
  if(pData->msg != NULL) {
    rpcFreeCont(pData->msg->pCont);
    taosMemoryFree(pData->msg);
    pData->msg = NULL;
  }
}

static bool tqOffsetLessOrEqual(const STqOffset* pLeft, const STqOffset* pRight) {
  return pLeft->val.type == TMQ_OFFSET__LOG && pRight->val.type == TMQ_OFFSET__LOG &&
         pLeft->val.version <= pRight->val.version;
}

STQ* tqOpen(const char* path, SVnode* pVnode) {
  STQ* pTq = taosMemoryCalloc(1, sizeof(STQ));
  if (pTq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pTq->path = taosStrdup(path);
  pTq->pVnode = pVnode;
  pTq->walLogLastVer = pVnode->pWal->vers.lastVer;

  pTq->pHandle = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pTq->pHandle, destroyTqHandle);

  taosInitRWLatch(&pTq->lock);
  pTq->pPushMgr = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);

  pTq->pCheckInfo = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pTq->pCheckInfo, (FDelete)tDeleteSTqCheckInfo);

  tqInitialize(pTq);
  return pTq;
}

int32_t tqInitialize(STQ* pTq) {
  if (tqMetaOpen(pTq) < 0) {
    return -1;
  }

  pTq->pOffsetStore = tqOffsetOpen(pTq);
  if (pTq->pOffsetStore == NULL) {
    return -1;
  }

  pTq->pStreamMeta = streamMetaOpen(pTq->path, pTq, (FTaskExpand*)tqExpandTask, pTq->pVnode->config.vgId);
  if (pTq->pStreamMeta == NULL) {
    return -1;
  }

  // the version is kept in task's meta data
  // todo check if this version is required or not
  if (streamLoadTasks(pTq->pStreamMeta, walGetCommittedVer(pTq->pVnode->pWal)) < 0) {
    return -1;
  }

  return 0;
}

void tqClose(STQ* pTq) {
  if (pTq == NULL) {
    return;
  }

  tqOffsetClose(pTq->pOffsetStore);
  taosHashCleanup(pTq->pHandle);
  taosHashCleanup(pTq->pPushMgr);
  taosHashCleanup(pTq->pCheckInfo);
  taosMemoryFree(pTq->path);
  tqMetaClose(pTq);
  streamMetaClose(pTq->pStreamMeta);
  taosMemoryFree(pTq);
}

void tqNotifyClose(STQ* pTq) {
  if (pTq != NULL) {
    taosWLockLatch(&pTq->pStreamMeta->lock);

    void* pIter = NULL;
    while (1) {
      pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
      if (pIter == NULL) {
        break;
      }

      SStreamTask* pTask = *(SStreamTask**)pIter;
      tqDebug("vgId:%d s-task:%s set dropping flag", pTq->pStreamMeta->vgId, pTask->id.idStr);
      pTask->status.taskStatus = TASK_STATUS__STOP;

      int64_t st = taosGetTimestampMs();
      qKillTask(pTask->exec.pExecutor, TSDB_CODE_SUCCESS);
      int64_t el = taosGetTimestampMs() - st;
      tqDebug("vgId:%d s-task:%s is closed in %" PRId64 "ms", pTq->pStreamMeta->vgId, pTask->id.idStr, el);
    }

    taosWUnLockLatch(&pTq->pStreamMeta->lock);
  }
}

static int32_t doSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, const SMqDataRsp* pRsp, int32_t epoch,
                             int64_t consumerId, int32_t type) {
  int32_t len = 0;
  int32_t code = 0;

  if (type == TMQ_MSG_TYPE__POLL_RSP) {
    tEncodeSize(tEncodeSMqDataRsp, pRsp, len, code);
  } else if (type == TMQ_MSG_TYPE__TAOSX_RSP) {
    tEncodeSize(tEncodeSTaosxRsp, (STaosxRsp*)pRsp, len, code);
  }

  if (code < 0) {
    return -1;
  }

  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMqRspHead*)buf)->mqMsgType = type;
  ((SMqRspHead*)buf)->epoch = epoch;
  ((SMqRspHead*)buf)->consumerId = consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);

  if (type == TMQ_MSG_TYPE__POLL_RSP) {
    tEncodeSMqDataRsp(&encoder, pRsp);
  } else if (type == TMQ_MSG_TYPE__TAOSX_RSP) {
    tEncodeSTaosxRsp(&encoder, (STaosxRsp*)pRsp);
  }

  tEncoderClear(&encoder);

  SRpcMsg rsp = {
      .info = *pRpcHandleInfo,
      .pCont = buf,
      .contLen = tlen,
      .code = 0,
  };

  tmsgSendRsp(&rsp);
  return 0;
}

int32_t tqPushDataRsp(STQ* pTq, STqHandle* pHandle) {
  SMqDataRsp dataRsp = {0};
  dataRsp.head.consumerId = pHandle->consumerId;
  dataRsp.head.epoch = pHandle->epoch;
  dataRsp.head.mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
  doSendDataRsp(&pHandle->msg->info, &dataRsp, pHandle->epoch, pHandle->consumerId, TMQ_MSG_TYPE__POLL_RSP);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, tListLen(buf1), &dataRsp.reqOffset);
  tFormatOffset(buf2, tListLen(buf2), &dataRsp.rspOffset);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) push rsp, block num: %d, req:%s, rsp:%s",
          TD_VID(pTq->pVnode), dataRsp.head.consumerId, dataRsp.head.epoch, dataRsp.blockNum, buf1, buf2);
  return 0;
}

int32_t tqSendDataRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataRsp* pRsp, int32_t type) {
  doSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, 80, &pRsp->reqOffset);
  tFormatOffset(buf2, 80, &pRsp->rspOffset);

  tqDebug("vgId:%d consumer:0x%" PRIx64 " (epoch %d) send rsp, block num:%d, req:%s, rsp:%s, reqId:0x%" PRIx64,
          TD_VID(pTq->pVnode), pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2, pReq->reqId);

  return 0;
}

int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  STqOffset offset = {0};
  int32_t   vgId = TD_VID(pTq->pVnode);

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeSTqOffset(&decoder, &offset) < 0) {
    return -1;
  }

  tDecoderClear(&decoder);

  if (offset.val.type == TMQ_OFFSET__SNAPSHOT_DATA || offset.val.type == TMQ_OFFSET__SNAPSHOT_META) {
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:snapshot) uid:%" PRId64 ", ts:%" PRId64,
            offset.subKey, vgId, offset.val.uid, offset.val.ts);
  } else if (offset.val.type == TMQ_OFFSET__LOG) {
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:log) version:%" PRId64, offset.subKey, vgId,
            offset.val.version);
    if (offset.val.version + 1 == sversion) {
      offset.val.version += 1;
    }
  } else {
    tqError("invalid commit offset type:%d", offset.val.type);
    return -1;
  }

  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, offset.subKey);
  if (pSavedOffset != NULL && tqOffsetLessOrEqual(&offset, pSavedOffset)) {
    return 0;  // no need to update the offset value
  }

  // save the new offset value
  if (tqOffsetWrite(pTq->pOffsetStore, &offset) < 0) {
    return -1;
  }

  if (offset.val.type == TMQ_OFFSET__LOG) {
    STqHandle* pHandle = taosHashGet(pTq->pHandle, offset.subKey, strlen(offset.subKey));
    if (pHandle && (walRefVer(pHandle->pRef, offset.val.version) < 0)) {
      return -1;
    }
  }

  return 0;
}

int32_t tqCheckColModifiable(STQ* pTq, int64_t tbUid, int32_t colId) {
  void* pIter = NULL;

  while (1) {
    pIter = taosHashIterate(pTq->pCheckInfo, pIter);
    if (pIter == NULL) {
      break;
    }

    STqCheckInfo* pCheck = (STqCheckInfo*)pIter;

    if (pCheck->ntbUid == tbUid) {
      int32_t sz = taosArrayGetSize(pCheck->colIdList);
      for (int32_t i = 0; i < sz; i++) {
        int16_t forbidColId = *(int16_t*)taosArrayGet(pCheck->colIdList, i);
        if (forbidColId == colId) {
          taosHashCancelIterate(pTq->pCheckInfo, pIter);
          return -1;
        }
      }
    }
  }

  return 0;
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqPollReq req = {0};
  if (tDeserializeSMqPollReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pMsg->contLen);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int64_t      consumerId = req.consumerId;
  int32_t      reqEpoch = req.epoch;
  STqOffsetVal reqOffset = req.reqOffset;
  int32_t      vgId = TD_VID(pTq->pVnode);

  // 1. find handle
  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    tqError("tmq poll: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", consumerId, vgId, req.subKey);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // 2. check re-balance status
  taosRLockLatch(&pTq->lock);
  if (pHandle->consumerId != consumerId) {
    tqDebug("ERROR tmq poll: consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            consumerId, TD_VID(pTq->pVnode), req.subKey, pHandle->consumerId);
    terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    taosRUnLockLatch(&pTq->lock);
    return -1;
  }
  taosRUnLockLatch(&pTq->lock);

  // 3. update the epoch value
  taosWLockLatch(&pTq->lock);
  int32_t savedEpoch = pHandle->epoch;
  if (savedEpoch < reqEpoch) {
    tqDebug("tmq poll: consumer:0x%" PRIx64 " epoch update from %d to %d by poll req", consumerId, savedEpoch,
            reqEpoch);
    pHandle->epoch = reqEpoch;
  }
  taosWUnLockLatch(&pTq->lock);

  char buf[80];
  tFormatOffset(buf, 80, &reqOffset);
  tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey %s, recv poll req vgId:%d, req:%s, reqId:0x%" PRIx64,
          consumerId, req.epoch, pHandle->subKey, vgId, buf, req.reqId);

  return tqExtractDataForMq(pTq, pHandle, &req, pMsg);
}

int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;

  tqDebug("vgId:%d, tq process delete sub req %s", pTq->pVnode->config.vgId, pReq->subKey);
  int32_t code = 0;
//  taosWLockLatch(&pTq->lock);
//  int32_t code = taosHashRemove(pTq->pPushMgr, pReq->subKey, strlen(pReq->subKey));
//  if (code != 0) {
//    tqDebug("vgId:%d, tq remove push handle %s", pTq->pVnode->config.vgId, pReq->subKey);
//  }
//  taosWUnLockLatch(&pTq->lock);

  STqHandle* pHandle = taosHashGet(pTq->pHandle, pReq->subKey, strlen(pReq->subKey));
  if (pHandle) {
    // walCloseRef(pHandle->pWalReader->pWal, pHandle->pRef->refId);
    if (pHandle->pRef) {
      walCloseRef(pTq->pVnode->pWal, pHandle->pRef->refId);
    }
    code = taosHashRemove(pTq->pHandle, pReq->subKey, strlen(pReq->subKey));
    if (code != 0) {
      tqError("cannot process tq delete req %s, since no such handle", pReq->subKey);
    }
  }

  code = tqOffsetDelete(pTq->pOffsetStore, pReq->subKey);
  if (code != 0) {
    tqError("cannot process tq delete req %s, since no such offset in cache", pReq->subKey);
  }

  if (tqMetaDeleteHandle(pTq, pReq->subKey) < 0) {
    tqError("cannot process tq delete req %s, since no such offset in tdb", pReq->subKey);
  }
  return 0;
}

int32_t tqProcessAddCheckInfoReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  STqCheckInfo info = {0};
  SDecoder     decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeSTqCheckInfo(&decoder, &info) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  tDecoderClear(&decoder);
  if (taosHashPut(pTq->pCheckInfo, info.topic, strlen(info.topic), &info, sizeof(STqCheckInfo)) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  if (tqMetaSaveCheckInfo(pTq, info.topic, msg, msgLen) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

int32_t tqProcessDelCheckInfoReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  if (taosHashRemove(pTq->pCheckInfo, msg, strlen(msg)) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  if (tqMetaDeleteCheckInfo(pTq, msg) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

int32_t tqProcessSubscribeReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqRebVgReq req = {0};
  tDecodeSMqRebVgReq(msg, &req);

  SVnode* pVnode = pTq->pVnode;
  int32_t vgId = TD_VID(pVnode);

  tqDebug("vgId:%d, tq process sub req:%s, Id:0x%" PRIx64 " -> Id:0x%" PRIx64, pVnode->config.vgId, req.subKey,
          req.oldConsumerId, req.newConsumerId);

  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    if (req.oldConsumerId != -1) {
      tqError("vgId:%d, build new consumer handle %s for consumer:0x%" PRIx64 ", but old consumerId:0x%" PRIx64,
              req.vgId, req.subKey, req.newConsumerId, req.oldConsumerId);
    }

    if (req.newConsumerId == -1) {
      tqError("vgId:%d, tq invalid re-balance request, new consumerId %" PRId64 "", req.vgId, req.newConsumerId);
      taosMemoryFree(req.qmsg);
      return 0;
    }

    STqHandle tqHandle = {0};
    pHandle = &tqHandle;

    uint64_t oldConsumerId = pHandle->consumerId;
    memcpy(pHandle->subKey, req.subKey, TSDB_SUBSCRIBE_KEY_LEN);
    pHandle->consumerId = req.newConsumerId;
    pHandle->epoch = -1;

    pHandle->execHandle.subType = req.subType;
    pHandle->fetchMeta = req.withMeta;

    // TODO version should be assigned and refed during preprocess
    SWalRef* pRef = walRefCommittedVer(pVnode->pWal);
    if (pRef == NULL) {
      taosMemoryFree(req.qmsg);
      return -1;
    }

    int64_t ver = pRef->refVer;
    pHandle->pRef = pRef;

    SReadHandle handle = {
        .meta = pVnode->pMeta, .vnode = pVnode, .initTableReader = true, .initTqReader = true, .version = ver};
    pHandle->snapshotVer = ver;

    if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      pHandle->execHandle.execCol.qmsg = req.qmsg;
      req.qmsg = NULL;

      pHandle->execHandle.task = qCreateQueueExecTaskInfo(pHandle->execHandle.execCol.qmsg, &handle, vgId,
                                                          &pHandle->execHandle.numOfCols, req.newConsumerId);
      void* scanner = NULL;
      qExtractStreamScanner(pHandle->execHandle.task, &scanner);
      pHandle->execHandle.pTqReader = qExtractReaderFromStreamScanner(scanner);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      pHandle->pWalReader = walOpenReader(pVnode->pWal, NULL);
      pHandle->execHandle.pTqReader = tqOpenReader(pVnode);

      pHandle->execHandle.execDb.pFilterOutTbUid =
          taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
      buildSnapContext(handle.meta, handle.version, 0, pHandle->execHandle.subType, pHandle->fetchMeta,
                       (SSnapContext**)(&handle.sContext));

      pHandle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &handle, vgId, NULL, req.newConsumerId);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      pHandle->pWalReader = walOpenReader(pVnode->pWal, NULL);
      pHandle->execHandle.execTb.suid = req.suid;

      SArray* tbUidList = taosArrayInit(0, sizeof(int64_t));
      vnodeGetCtbIdList(pVnode, req.suid, tbUidList);
      tqDebug("vgId:%d, tq try to get all ctb, suid:%" PRId64, pVnode->config.vgId, req.suid);
      for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
        int64_t tbUid = *(int64_t*)taosArrayGet(tbUidList, i);
        tqDebug("vgId:%d, idx %d, uid:%" PRId64, vgId, i, tbUid);
      }
      pHandle->execHandle.pTqReader = tqOpenReader(pVnode);
      tqReaderSetTbUidList(pHandle->execHandle.pTqReader, tbUidList);
      taosArrayDestroy(tbUidList);

      buildSnapContext(handle.meta, handle.version, req.suid, pHandle->execHandle.subType, pHandle->fetchMeta,
                       (SSnapContext**)(&handle.sContext));
      pHandle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &handle, vgId, NULL, req.newConsumerId);
    }

    taosHashPut(pTq->pHandle, req.subKey, strlen(req.subKey), pHandle, sizeof(STqHandle));
    tqDebug("try to persist handle %s consumer:0x%" PRIx64 " , old consumer:0x%" PRIx64, req.subKey,
            pHandle->consumerId, oldConsumerId);
    if (tqMetaSaveHandle(pTq, req.subKey, pHandle) < 0) {
      taosMemoryFree(req.qmsg);
      return -1;
    }
  } else {
    if (pHandle->consumerId == req.newConsumerId) {  // do nothing
      tqInfo("vgId:%d consumer:0x%" PRIx64 " remains, no switch occurs", req.vgId, req.newConsumerId);
      atomic_store_32(&pHandle->epoch, -1);
      atomic_add_fetch_32(&pHandle->epoch, 1);
      taosMemoryFree(req.qmsg);
      return tqMetaSaveHandle(pTq, req.subKey, pHandle);
    } else {
      tqInfo("vgId:%d switch consumer from Id:0x%" PRIx64 " to Id:0x%" PRIx64, req.vgId, pHandle->consumerId,
             req.newConsumerId);

      // kill executing task
      qTaskInfo_t pTaskInfo = pHandle->execHandle.task;
      if (pTaskInfo != NULL) {
        qKillTask(pTaskInfo, TSDB_CODE_SUCCESS);
      }

      taosWLockLatch(&pTq->lock);
      atomic_store_32(&pHandle->epoch, 0);

      // remove if it has been register in the push manager, and return one empty block to consumer
      tqUnregisterPushHandle(pTq, pHandle);

      atomic_store_64(&pHandle->consumerId, req.newConsumerId);

      if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
        qStreamCloseTsdbReader(pTaskInfo);
      }

      taosWUnLockLatch(&pTq->lock);
      if (tqMetaSaveHandle(pTq, req.subKey, pHandle) < 0) {
        taosMemoryFree(req.qmsg);
        return -1;
      }
    }
  }

  taosMemoryFree(req.qmsg);
  return 0;
}

int32_t tqExpandTask(STQ* pTq, SStreamTask* pTask, int64_t ver) {
  int32_t vgId = TD_VID(pTq->pVnode);
  pTask->id.idStr = createStreamTaskIdStr(pTask->id.streamId, pTask->id.taskId);
  pTask->refCnt = 1;
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->inputQueue = streamQueueOpen();
  pTask->outputQueue = streamQueueOpen();

  if (pTask->inputQueue == NULL || pTask->outputQueue == NULL) {
    return -1;
  }

  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;
  pTask->pMsgCb = &pTq->pVnode->msgCb;
  pTask->pMeta = pTq->pStreamMeta;
  pTask->chkInfo.version = ver;
  pTask->chkInfo.currentVer = ver;

  // expand executor
  if (pTask->fillHistory) {
    pTask->status.taskStatus = TASK_STATUS__WAIT_DOWNSTREAM;
  } else {
    pTask->status.taskStatus = TASK_STATUS__RESTORE;
  }

  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pTask, false, -1, -1);
    if (pTask->pState == NULL) {
      return -1;
    }

    SReadHandle handle = {
        .meta = pTq->pVnode->pMeta, .vnode = pTq->pVnode, .initTqReader = 1, .pStateBackend = pTask->pState};

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }

  } else if (pTask->taskLevel == TASK_LEVEL__AGG) {
    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pTask, false, -1, -1);
    if (pTask->pState == NULL) {
      return -1;
    }

    int32_t numOfVgroups = (int32_t)taosArrayGetSize(pTask->childEpInfo);
    SReadHandle mgHandle = { .vnode = NULL, .numOfVgroups = numOfVgroups, .pStateBackend = pTask->pState};

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &mgHandle, vgId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }
  }

  // sink
  /*pTask->ahandle = pTq->pVnode;*/
  if (pTask->outputType == TASK_OUTPUT__SMA) {
    pTask->smaSink.vnode = pTq->pVnode;
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->outputType == TASK_OUTPUT__TABLE) {
    pTask->tbSink.vnode = pTq->pVnode;
    pTask->tbSink.tbSinkFunc = tqSinkToTablePipeline2;

    int32_t   ver1 = 1;
    SMetaInfo info = {0};
    int32_t   code = metaGetInfo(pTq->pVnode->pMeta, pTask->tbSink.stbUid, &info, NULL);
    if (code == TSDB_CODE_SUCCESS) {
      ver1 = info.skmVer;
    }

    SSchemaWrapper* pschemaWrapper = pTask->tbSink.pSchemaWrapper;
    pTask->tbSink.pTSchema = tBuildTSchema(pschemaWrapper->pSchema, pschemaWrapper->nCols, ver1);
    if(pTask->tbSink.pTSchema == NULL) {
      return -1;
    }
  }

  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    pTask->exec.pWalReader = walOpenReader(pTq->pVnode->pWal, NULL);
  }

  streamSetupTrigger(pTask);
  tqInfo("vgId:%d expand stream task, s-task:%s, checkpoint ver:%" PRId64 " child id:%d, level:%d", vgId, pTask->id.idStr,
         pTask->chkInfo.version, pTask->selfChildId, pTask->taskLevel);

  // next valid version will add one
  pTask->chkInfo.version += 1;
  return 0;
}

int32_t tqProcessStreamTaskCheckReq(STQ* pTq, SRpcMsg* pMsg) {
  char*               msgStr = pMsg->pCont;
  char*               msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t             msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamTaskCheckReq req;
  SDecoder            decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeSStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);
  int32_t             taskId = req.downstreamTaskId;
  SStreamTaskCheckRsp rsp = {
      .reqId = req.reqId,
      .streamId = req.streamId,
      .childId = req.childId,
      .downstreamNodeId = req.downstreamNodeId,
      .downstreamTaskId = req.downstreamTaskId,
      .upstreamNodeId = req.upstreamNodeId,
      .upstreamTaskId = req.upstreamTaskId,
  };

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    rsp.status = (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__NORMAL) ? 1 : 0;
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);

    tqDebug("tq recv task check req(reqId:0x%" PRIx64
            ") %d at node %d task status:%d, check req from task %d at node %d, rsp status %d",
            rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, pTask->status.taskStatus, rsp.upstreamTaskId,
            rsp.upstreamNodeId, rsp.status);
  } else {
    rsp.status = 0;
    tqDebug("tq recv task check(taskId:%d not built yet) req(reqId:0x%" PRIx64
            ") %d at node %d, check req from task %d at node %d, rsp status %d",
            taskId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.upstreamTaskId, rsp.upstreamNodeId,
            rsp.status);
  }

  SEncoder encoder;
  int32_t  code;
  int32_t  len;
  tEncodeSize(tEncodeSStreamTaskCheckRsp, &rsp, len, code);
  if (code < 0) {
    tqError("unable to encode rsp %d", __LINE__);
    return -1;
  }

  void* buf = rpcMallocCont(sizeof(SMsgHead) + len);
  ((SMsgHead*)buf)->vgId = htonl(req.upstreamNodeId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  tEncodeSStreamTaskCheckRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  SRpcMsg rspMsg = { .code = 0, .pCont = buf, .contLen = sizeof(SMsgHead) + len, .info = pMsg->info };
  tmsgSendRsp(&rspMsg);
  return 0;
}

int32_t tqProcessStreamTaskCheckRsp(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  int32_t             code;
  SStreamTaskCheckRsp rsp;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  code = tDecodeSStreamTaskCheckRsp(&decoder, &rsp);
  if (code < 0) {
    tDecoderClear(&decoder);
    return -1;
  }

  tDecoderClear(&decoder);
  tqDebug("tq recv task check rsp(reqId:0x%" PRIx64 ") %d at node %d check req from task %d at node %d, status %d",
          rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, rsp.upstreamTaskId);
  if (pTask == NULL) {
    return -1;
  }

  code = streamProcessTaskCheckRsp(pTask, &rsp, sversion);
  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return code;
}

int32_t tqProcessTaskDeployReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  int32_t code;
#if 0
  code = streamMetaAddSerializedTask(pTq->pStreamMeta, version, msg, msgLen);
  if (code < 0) return code;
#endif
  if (tsDisableStream) {
    return 0;
  }

  // 1.deserialize msg and build task
  SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    return -1;
  }

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  code = tDecodeStreamTask(&decoder, pTask);
  if (code < 0) {
    tDecoderClear(&decoder);
    taosMemoryFree(pTask);
    return -1;
  }

  tDecoderClear(&decoder);

  // 2.save task, use the newest commit version as the initial start version of stream task.
  code = streamMetaAddDeployedTask(pTq->pStreamMeta, sversion, pTask);
  if (code < 0) {
    tqError("vgId:%d failed to add s-task:%s, total:%d", TD_VID(pTq->pVnode), pTask->id.idStr,
            streamMetaGetNumOfTasks(pTq->pStreamMeta));
    return -1;
  }

  // 3.go through recover steps to fill history
  if (pTask->fillHistory) {
    streamTaskCheckDownstream(pTask, sversion);
  }

  tqDebug("vgId:%d s-task:%s is deployed and add meta from mnd, status:%d, total:%d", TD_VID(pTq->pVnode),
          pTask->id.idStr, pTask->status.taskStatus, streamMetaGetNumOfTasks(pTq->pStreamMeta));
  return 0;
}

int32_t tqProcessTaskRecover1Req(STQ* pTq, SRpcMsg* pMsg) {
  int32_t code;
  char*   msg = pMsg->pCont;
  int32_t msgLen = pMsg->contLen;

  SStreamRecoverStep1Req* pReq = (SStreamRecoverStep1Req*)msg;
  SStreamTask*            pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  if (pTask == NULL) {
    return -1;
  }

  // check param
  int64_t fillVer1 = pTask->chkInfo.version;
  if (fillVer1 <= 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  // do recovery step 1
  streamSourceRecoverScanStep1(pTask);

  if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__DROPPING) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  }

  // build msg to launch next step
  SStreamRecoverStep2Req req;
  code = streamBuildSourceRecover2Req(pTask, &req);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  streamMetaReleaseTask(pTq->pStreamMeta, pTask);

  if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__DROPPING) {
    return 0;
  }

  // serialize msg
  int32_t len = sizeof(SStreamRecoverStep1Req);

  void* serializedReq = rpcMallocCont(len);
  if (serializedReq == NULL) {
    return -1;
  }

  memcpy(serializedReq, &req, len);

  // dispatch msg
  SRpcMsg rpcMsg = {
      .code = 0,
      .contLen = len,
      .msgType = TDMT_VND_STREAM_RECOVER_BLOCKING_STAGE,
      .pCont = serializedReq,
  };

  tmsgPutToQueue(&pTq->pVnode->msgCb, WRITE_QUEUE, &rpcMsg);

  return 0;
}

int32_t tqProcessTaskRecover2Req(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  int32_t                 code;
  SStreamRecoverStep2Req* pReq = (SStreamRecoverStep2Req*)msg;
  SStreamTask*            pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  if (pTask == NULL) {
    return -1;
  }

  // do recovery step 2
  code = streamSourceRecoverScanStep2(pTask, sversion);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__DROPPING) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  }

  // restore param
  code = streamRestoreParam(pTask);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  // set status normal
  code = streamSetStatusNormal(pTask);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  // dispatch recover finish req to all related downstream task
  code = streamDispatchRecoverFinishReq(pTask);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  atomic_store_8(&pTask->fillHistory, 0);
  streamMetaSaveTask(pTq->pStreamMeta, pTask);

  streamMetaReleaseTask(pTq->pStreamMeta, pTask);

  return 0;
}

int32_t tqProcessTaskRecoverFinishReq(STQ* pTq, SRpcMsg* pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamRecoverFinishReq req;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeSStreamRecoverFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  // find task
  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, req.taskId);
  if (pTask == NULL) {
    return -1;
  }
  // do process request
  if (streamProcessRecoverFinishReq(pTask, req.childId) < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return 0;
}

int32_t tqProcessTaskRecoverFinishRsp(STQ* pTq, SRpcMsg* pMsg) {
  //
  return 0;
}

int32_t tqProcessDelReq(STQ* pTq, void* pReq, int32_t len, int64_t ver) {
  bool        failed = false;
  SDecoder*   pCoder = &(SDecoder){0};
  SDeleteRes* pRes = &(SDeleteRes){0};

  pRes->uidList = taosArrayInit(0, sizeof(tb_uid_t));
  if (pRes->uidList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    failed = true;
  }

  tDecoderInit(pCoder, pReq, len);
  tDecodeDeleteRes(pCoder, pRes);
  tDecoderClear(pCoder);

  int32_t sz = taosArrayGetSize(pRes->uidList);
  if (sz == 0 || pRes->affectedRows == 0) {
    taosArrayDestroy(pRes->uidList);
    return 0;
  }
  SSDataBlock* pDelBlock = createSpecialDataBlock(STREAM_DELETE_DATA);
  blockDataEnsureCapacity(pDelBlock, sz);
  pDelBlock->info.rows = sz;
  pDelBlock->info.version = ver;

  for (int32_t i = 0; i < sz; i++) {
    // start key column
    SColumnInfoData* pStartCol = taosArrayGet(pDelBlock->pDataBlock, START_TS_COLUMN_INDEX);
    colDataSetVal(pStartCol, i, (const char*)&pRes->skey, false);  // end key column
    SColumnInfoData* pEndCol = taosArrayGet(pDelBlock->pDataBlock, END_TS_COLUMN_INDEX);
    colDataSetVal(pEndCol, i, (const char*)&pRes->ekey, false);
    // uid column
    SColumnInfoData* pUidCol = taosArrayGet(pDelBlock->pDataBlock, UID_COLUMN_INDEX);
    int64_t*         pUid = taosArrayGet(pRes->uidList, i);
    colDataSetVal(pUidCol, i, (const char*)pUid, false);

    colDataSetNULL(taosArrayGet(pDelBlock->pDataBlock, GROUPID_COLUMN_INDEX), i);
    colDataSetNULL(taosArrayGet(pDelBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX), i);
    colDataSetNULL(taosArrayGet(pDelBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX), i);
  }

  taosArrayDestroy(pRes->uidList);

  int32_t* pRef = taosMemoryMalloc(sizeof(int32_t));
  *pRef = 1;

  taosWLockLatch(&pTq->pStreamMeta->lock);

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) {
      continue;
    }

    qDebug("s-task:%s delete req enqueue, ver: %" PRId64, pTask->id.idStr, ver);

    if (!failed) {
      SStreamRefDataBlock* pRefBlock = taosAllocateQitem(sizeof(SStreamRefDataBlock), DEF_QITEM, 0);
      pRefBlock->type = STREAM_INPUT__REF_DATA_BLOCK;
      pRefBlock->pBlock = pDelBlock;
      pRefBlock->dataRef = pRef;
      atomic_add_fetch_32(pRefBlock->dataRef, 1);

      if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pRefBlock) < 0) {
        atomic_sub_fetch_32(pRef, 1);
        taosFreeQitem(pRefBlock);
        continue;
      }

      if (streamSchedExec(pTask) < 0) {
        qError("s-task:%s stream task launch failed", pTask->id.idStr);
        continue;
      }

    } else {
      streamTaskInputFail(pTask);
    }
  }

  taosWUnLockLatch(&pTq->pStreamMeta->lock);

  int32_t ref = atomic_sub_fetch_32(pRef, 1);
  if (ref == 0) {
    blockDataDestroy(pDelBlock);
    taosMemoryFree(pRef);
  }

#if 0
    SStreamDataBlock* pStreamBlock = taosAllocateQitem(sizeof(SStreamDataBlock), DEF_QITEM, 0);
    pStreamBlock->type = STREAM_INPUT__DATA_BLOCK;
    pStreamBlock->blocks = taosArrayInit(0, sizeof(SSDataBlock));
    SSDataBlock block = {0};
    assignOneDataBlock(&block, pDelBlock);
    block.info.type = STREAM_DELETE_DATA;
    taosArrayPush(pStreamBlock->blocks, &block);

    if (!failed) {
      if (tAppendDataToInputQueue(pTask, (SStreamQueueItem*)pStreamBlock) < 0) {
        qError("stream task input del failed, task id %d", pTask->id.taskId);
        continue;
      }

      if (streamSchedExec(pTask) < 0) {
        qError("stream task launch failed, task id %d", pTask->id.taskId);
        continue;
      }
    } else {
      streamTaskInputFail(pTask);
    }
  }
  blockDataDestroy(pDelBlock);
#endif
  return 0;
}

int32_t tqProcessSubmitReqForSubscribe(STQ* pTq) {
  int32_t vgId = TD_VID(pTq->pVnode);

  taosWLockLatch(&pTq->lock);
  if(taosHashGetSize(pTq->pPushMgr) > 0){
    void *pIter = taosHashIterate(pTq->pPushMgr, NULL);
    while(pIter){
      STqHandle* pHandle = *(STqHandle**)pIter;
      tqDebug("vgId:%d start set submit for pHandle:%p, consume id:0x%"PRIx64, vgId, pHandle, pHandle->consumerId);
      if(ASSERT(pHandle->msg != NULL)){
        tqError("pHandle->msg should not be null");
        break;
      }else{
        SRpcMsg msg = {.msgType = TDMT_VND_TMQ_CONSUME, .pCont = pHandle->msg->pCont, .contLen = pHandle->msg->contLen, .info = pHandle->msg->info};
        tmsgPutToQueue(&pTq->pVnode->msgCb, QUERY_QUEUE, &msg);
        taosMemoryFree(pHandle->msg);
        pHandle->msg = NULL;
      }
      pIter = taosHashIterate(pTq->pPushMgr, pIter);
    }
    taosHashClear(pTq->pPushMgr);
  }
  // unlock
  taosWUnLockLatch(&pTq->lock);

  return 0;
}

int32_t tqProcessSubmitReq(STQ* pTq, SPackedData submit) {
#if 0
  void* pIter = NULL;
  SStreamDataSubmit2* pSubmit = streamDataSubmitNew(submit, STREAM_INPUT__DATA_SUBMIT);
  if (pSubmit == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("failed to create data submit for stream since out of memory");
    saveOffsetForAllTasks(pTq, submit.ver);
    return -1;
  }

  SArray* pInputQueueFullTasks = taosArrayInit(4, POINTER_BYTES);

  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) {
      continue;
    }

    if (pTask->status.taskStatus == TASK_STATUS__RECOVER_PREPARE || pTask->status.taskStatus == TASK_STATUS__WAIT_DOWNSTREAM) {
      tqDebug("stream task:%d skip push data, not ready for processing, status %d", pTask->id.taskId,
              pTask->status.taskStatus);
      continue;
    }

    // check if offset value exists
    char key[128] = {0};
    createStreamTaskOffsetKey(key, pTask->id.streamId, pTask->id.taskId);

    if (tInputQueueIsFull(pTask)) {
      STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, key);

      int64_t ver = submit.ver;
      if (pOffset == NULL) {
        doSaveTaskOffset(pTq->pOffsetStore, key, submit.ver);
      } else {
        ver = pOffset->val.version;
      }

      tqDebug("s-task:%s input queue is full, discard submit block, ver:%" PRId64, pTask->id.idStr, ver);
      taosArrayPush(pInputQueueFullTasks, &pTask);
      continue;
    }

    // check if offset value exists
    STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, key);
    ASSERT(pOffset == NULL);

    addSubmitBlockNLaunchTask(pTq->pOffsetStore, pTask, pSubmit, key, submit.ver);
  }

  streamDataSubmitDestroy(pSubmit);
  taosFreeQitem(pSubmit);
#endif

  tqStartStreamTasks(pTq);
  return 0;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  int32_t taskId = pReq->taskId;
  int32_t vgId = TD_VID(pTq->pVnode);

  if (taskId == WAL_READ_TASKS_ID) {  // all tasks are extracted submit data from the wal
    tqStreamTasksScanWal(pTq);
    return 0;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask != NULL) {
    if (pTask->status.taskStatus == TASK_STATUS__NORMAL) {
      tqDebug("vgId:%d s-task:%s start to process run req", vgId, pTask->id.idStr);
      streamProcessRunReq(pTask);
    } else if (pTask->status.taskStatus == TASK_STATUS__RESTORE) {
      tqDebug("vgId:%d s-task:%s start to process block from wal, last chk point:%" PRId64, vgId,
              pTask->id.idStr, pTask->chkInfo.version);
      streamProcessRunReq(pTask);
    } else {
      tqDebug("vgId:%d s-task:%s ignore run req since not in ready state", vgId, pTask->id.idStr);
    }

    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    tqStartStreamTasks(pTq);
    return 0;
  } else {
    tqError("vgId:%d failed to found s-task, taskId:%d", vgId, taskId);
    return -1;
  }
}

int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg, bool exec) {
  char*              msgStr = pMsg->pCont;
  char*              msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamDispatchReq(&decoder, &req);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, req.taskId);
  if (pTask) {
    SRpcMsg rsp = { .info = pMsg->info, .code = 0 };
    streamProcessDispatchReq(pTask, &req, &rsp, exec);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  } else {
    tDeleteStreamDispatchReq(&req);
    return -1;
  }
}

int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg) {
  SStreamDispatchRsp* pRsp = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t             taskId = ntohl(pRsp->upstreamTaskId);
  SStreamTask*        pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  tqDebug("recv dispatch rsp, code:%x", pMsg->code);
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  } else {
    return -1;
  }
}

int32_t tqProcessTaskDropReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SVDropStreamTaskReq* pReq = (SVDropStreamTaskReq*)msg;
  streamMetaRemoveTask(pTq->pStreamMeta, pReq->taskId);
  return 0;
}

int32_t tqProcessTaskRetrieveReq(STQ* pTq, SRpcMsg* pMsg) {
  char*              msgStr = pMsg->pCont;
  char*              msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t            msgLen = pMsg->contLen - sizeof(SMsgHead);
  SStreamRetrieveReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);
  int32_t      taskId = req.dstTaskId;
  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = { .info = pMsg->info, .code = 0 };
    streamProcessRetrieveReq(pTask, &req, &rsp);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    tDeleteStreamRetrieveReq(&req);
    return 0;
  } else {
    tDeleteStreamRetrieveReq(&req);
    return -1;
  }
}

int32_t tqProcessTaskRetrieveRsp(STQ* pTq, SRpcMsg* pMsg) {
  //
  return 0;
}

int32_t vnodeEnqueueStreamMsg(SVnode* pVnode, SRpcMsg* pMsg) {
  STQ*      pTq = pVnode->pTq;
  SMsgHead* msgStr = pMsg->pCont;
  char*     msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t   msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t   code = 0;

  SStreamDispatchReq req;
  SDecoder           decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  if (tDecodeStreamDispatchReq(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    goto FAIL;
  }
  tDecoderClear(&decoder);

  int32_t taskId = req.taskId;

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = { .info = pMsg->info, .code = 0 };
    streamProcessDispatchReq(pTask, &req, &rsp, false);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
    return 0;
  } else {
    tDeleteStreamDispatchReq(&req);
  }

  code = TSDB_CODE_STREAM_TASK_NOT_EXIST;

FAIL:
  if (pMsg->info.handle == NULL) return -1;

  SMsgHead* pRspHead = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  if (pRspHead == NULL) {
    SRpcMsg rsp = { .code = TSDB_CODE_OUT_OF_MEMORY, .info = pMsg->info };
    tqDebug("send dispatch error rsp, code: %x", code);
    tmsgSendRsp(&rsp);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
    return -1;
  }

  pRspHead->vgId = htonl(req.upstreamNodeId);
  SStreamDispatchRsp* pRsp = POINTER_SHIFT(pRspHead, sizeof(SMsgHead));
  pRsp->streamId = htobe64(req.streamId);
  pRsp->upstreamTaskId = htonl(req.upstreamTaskId);
  pRsp->upstreamNodeId = htonl(req.upstreamNodeId);
  pRsp->downstreamNodeId = htonl(pVnode->config.vgId);
  pRsp->downstreamTaskId = htonl(req.taskId);
  pRsp->inputStatus = TASK_OUTPUT_STATUS__NORMAL;

  SRpcMsg rsp = {
      .code = code, .info = pMsg->info, .contLen = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp), .pCont = pRspHead};
  tqDebug("send dispatch error rsp, code: %x", code);
  tmsgSendRsp(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
  return -1;
}

int32_t tqCheckLogInWal(STQ* pTq, int64_t sversion) { return sversion <= pTq->walLogLastVer; }

int32_t tqStartStreamTasks(STQ* pTq) {
  int32_t vgId = TD_VID(pTq->pVnode);

  SStreamMeta* pMeta = pTq->pStreamMeta;
  taosWLockLatch(&pMeta->lock);
  int32_t numOfTasks = taosHashGetSize(pTq->pStreamMeta->pTasks);
  if (numOfTasks == 0) {
    tqInfo("vgId:%d no stream tasks exists", vgId);
    taosWUnLockLatch(&pTq->pStreamMeta->lock);
    return 0;
  }

  pMeta->walScan += 1;

  if (pMeta->walScan > 1) {
    tqDebug("vgId:%d wal read task has been launched, remain scan times:%d", vgId, pMeta->walScan);
    taosWUnLockLatch(&pTq->pStreamMeta->lock);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed restore stream tasks, code:%s", vgId, terrstr(terrno));
    taosWUnLockLatch(&pTq->pStreamMeta->lock);
    return -1;
  }

  tqDebug("vgId:%d start wal scan stream tasks, tasks:%d", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = WAL_READ_TASKS_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);
  taosWUnLockLatch(&pTq->pStreamMeta->lock);

  return 0;
}
