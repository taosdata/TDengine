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
static int32_t tqInitialize(STQ* pTq);

static FORCE_INLINE bool tqIsHandleExec(STqHandle* pHandle) { return TMQ_HANDLE_STATUS_EXEC == pHandle->status; }
static FORCE_INLINE void tqSetHandleExec(STqHandle* pHandle) { pHandle->status = TMQ_HANDLE_STATUS_EXEC; }
static FORCE_INLINE void tqSetHandleIdle(STqHandle* pHandle) { pHandle->status = TMQ_HANDLE_STATUS_IDLE; }

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

void tqDestroyTqHandle(void* data) {
  STqHandle* pData = (STqHandle*)data;
  qDestroyTask(pData->execHandle.task);

  if (pData->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    taosMemoryFreeClear(pData->execHandle.execCol.qmsg);
  } else if (pData->execHandle.subType == TOPIC_SUB_TYPE__DB) {
    tqReaderClose(pData->execHandle.pTqReader);
    walCloseReader(pData->pWalReader);
    taosHashCleanup(pData->execHandle.execDb.pFilterOutTbUid);
  } else if (pData->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
    walCloseReader(pData->pWalReader);
    tqReaderClose(pData->execHandle.pTqReader);
    taosMemoryFreeClear(pData->execHandle.execTb.qmsg);
    nodesDestroyNode(pData->execHandle.execTb.node);
  }
  if (pData->msg != NULL) {
    rpcFreeCont(pData->msg->pCont);
    taosMemoryFree(pData->msg);
    pData->msg = NULL;
  }
}

static bool tqOffsetEqual(const STqOffset* pLeft, const STqOffset* pRight) {
  return pLeft->val.type == TMQ_OFFSET__LOG && pRight->val.type == TMQ_OFFSET__LOG &&
         pLeft->val.version == pRight->val.version;
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
  taosHashSetFreeFp(pTq->pHandle, tqDestroyTqHandle);

  taosInitRWLatch(&pTq->lock);
  pTq->pPushMgr = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);

  pTq->pCheckInfo = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pTq->pCheckInfo, (FDelete)tDeleteSTqCheckInfo);

  int32_t code = tqInitialize(pTq);
  if (code != TSDB_CODE_SUCCESS) {
    tqClose(pTq);
    return NULL;
  } else {
    return pTq;
  }
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

  void* pIter = taosHashIterate(pTq->pPushMgr, NULL);
  while (pIter) {
    STqHandle* pHandle = *(STqHandle**)pIter;
    int32_t    vgId = TD_VID(pTq->pVnode);

    if(pHandle->msg != NULL) {
      tqPushEmptyDataRsp(pHandle, vgId);
      rpcFreeCont(pHandle->msg->pCont);
      taosMemoryFree(pHandle->msg);
      pHandle->msg = NULL;
    }
    pIter = taosHashIterate(pTq->pPushMgr, pIter);
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

static bool hasStreamTaskInTimer(SStreamMeta* pMeta) {
  bool inTimer = false;

  taosWLockLatch(&pMeta->lock);

  void* pIter = NULL;
  while(1) {
    pIter = taosHashIterate(pMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->status.timerActive == 1) {
      inTimer = true;
    }
  }

  taosWUnLockLatch(&pMeta->lock);

  return inTimer;
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
      tqDebug("vgId:%d s-task:%s set closing flag", pTq->pStreamMeta->vgId, pTask->id.idStr);
      pTask->status.taskStatus = TASK_STATUS__STOP;

      int64_t st = taosGetTimestampMs();
      qKillTask(pTask->exec.pExecutor, TSDB_CODE_SUCCESS);

      int64_t el = taosGetTimestampMs() - st;
      tqDebug("vgId:%d s-task:%s is closed in %" PRId64 " ms", pTq->pStreamMeta->vgId, pTask->id.idStr, el);
    }

    taosWUnLockLatch(&pTq->pStreamMeta->lock);

    tqDebug("vgId:%d start to check all tasks", pTq->pStreamMeta->vgId);

    int64_t st = taosGetTimestampMs();

    while(hasStreamTaskInTimer(pTq->pStreamMeta)) {
      tqDebug("vgId:%d some tasks in timer, wait for 100ms and recheck", pTq->pStreamMeta->vgId);
      taosMsleep(100);
    }

    int64_t el = taosGetTimestampMs() - st;
    tqDebug("vgId:%d all stream tasks are not in timer, continue close, elapsed time:%"PRId64" ms", pTq->pStreamMeta->vgId, el);
  }
}

//static int32_t doSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, const SMqDataRsp* pRsp, int32_t epoch,
//                             int64_t consumerId, int32_t type) {
//  int32_t len = 0;
//  int32_t code = 0;
//
//  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP) {
//    tEncodeSize(tEncodeMqDataRsp, pRsp, len, code);
//  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
//    tEncodeSize(tEncodeSTaosxRsp, (STaosxRsp*)pRsp, len, code);
//  }
//
//  if (code < 0) {
//    return -1;
//  }
//
//  int32_t tlen = sizeof(SMqRspHead) + len;
//  void*   buf = rpcMallocCont(tlen);
//  if (buf == NULL) {
//    return -1;
//  }
//
//  ((SMqRspHead*)buf)->mqMsgType = type;
//  ((SMqRspHead*)buf)->epoch = epoch;
//  ((SMqRspHead*)buf)->consumerId = consumerId;
//
//  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));
//
//  SEncoder encoder = {0};
//  tEncoderInit(&encoder, abuf, len);
//
//  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP) {
//    tEncodeMqDataRsp(&encoder, pRsp);
//  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
//    tEncodeSTaosxRsp(&encoder, (STaosxRsp*)pRsp);
//  }
//
//  tEncoderClear(&encoder);
//
//  SRpcMsg rsp = {
//      .info = *pRpcHandleInfo,
//      .pCont = buf,
//      .contLen = tlen,
//      .code = 0,
//  };
//
//  tmsgSendRsp(&rsp);
//  return 0;
//}

int32_t tqPushEmptyDataRsp(STqHandle* pHandle, int32_t vgId) {
  SMqPollReq req = {0};
  if (tDeserializeSMqPollReq(pHandle->msg->pCont, pHandle->msg->contLen, &req) < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pHandle->msg->contLen);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, &req);
  dataRsp.blockNum = 0;
  dataRsp.rspOffset = dataRsp.reqOffset;
  char buf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.reqOffset);
  tqInfo("tqPushEmptyDataRsp to consumer:0x%"PRIx64 " vgId:%d, offset:%s, reqId:0x%" PRIx64, req.consumerId, vgId, buf, req.reqId);

  tqSendDataRsp(pHandle, pHandle->msg, &req, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
  tDeleteMqDataRsp(&dataRsp);
  return 0;
}

//int32_t tqPushDataRsp(STqHandle* pHandle, int32_t vgId) {
//  SMqDataRsp dataRsp = {0};
//  dataRsp.head.consumerId = pHandle->consumerId;
//  dataRsp.head.epoch = pHandle->epoch;
//  dataRsp.head.mqMsgType = TMQ_MSG_TYPE__POLL_RSP;
//
//  int64_t sver = 0, ever = 0;
//  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
//  tqDoSendDataRsp(&pHandle->msg->info, &dataRsp, pHandle->epoch, pHandle->consumerId, TMQ_MSG_TYPE__POLL_RSP, sver,
//                  ever);
//
//  char buf1[TSDB_OFFSET_LEN] = {0};
//  char buf2[TSDB_OFFSET_LEN] = {0};
//  tFormatOffset(buf1, tListLen(buf1), &dataRsp.reqOffset);
//  tFormatOffset(buf2, tListLen(buf2), &dataRsp.rspOffset);
//  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) push rsp, block num: %d, req:%s, rsp:%s", vgId,
//          dataRsp.head.consumerId, dataRsp.head.epoch, dataRsp.blockNum, buf1, buf2);
//  return 0;
//}

int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataRsp* pRsp,
                      int32_t type, int32_t vgId) {
  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  tqDoSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type, sver, ever);

  char buf1[TSDB_OFFSET_LEN] = {0};
  char buf2[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(buf1, TSDB_OFFSET_LEN, &pRsp->reqOffset);
  tFormatOffset(buf2, TSDB_OFFSET_LEN, &pRsp->rspOffset);

  tqDebug("tmq poll vgId:%d consumer:0x%" PRIx64 " (epoch %d) send rsp, block num:%d, req:%s, rsp:%s, reqId:0x%" PRIx64, vgId,
          pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2, pReq->reqId);

  return 0;
}

int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVgOffset vgOffset = {0};
  int32_t     vgId = TD_VID(pTq->pVnode);

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeMqVgOffset(&decoder, &vgOffset) < 0) {
    return -1;
  }

  tDecoderClear(&decoder);

  STqOffset* pOffset = &vgOffset.offset;

  if (pOffset->val.type == TMQ_OFFSET__SNAPSHOT_DATA || pOffset->val.type == TMQ_OFFSET__SNAPSHOT_META) {
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:snapshot) uid:%" PRId64 ", ts:%" PRId64,
            pOffset->subKey, vgId, pOffset->val.uid, pOffset->val.ts);
  } else if (pOffset->val.type == TMQ_OFFSET__LOG) {
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:log) version:%" PRId64, pOffset->subKey, vgId,
            pOffset->val.version);
  } else {
    tqError("invalid commit offset type:%d", pOffset->val.type);
    return -1;
  }

  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, pOffset->subKey);
  if (pSavedOffset != NULL && tqOffsetEqual(pOffset, pSavedOffset)) {
    tqInfo("not update the offset, vgId:%d sub:%s since committed:%" PRId64 " less than/equal to existed:%" PRId64,
            vgId, pOffset->subKey, pOffset->val.version, pSavedOffset->val.version);
    return 0;  // no need to update the offset value
  }

  // save the new offset value
  if (tqOffsetWrite(pTq->pOffsetStore, pOffset) < 0) {
    return -1;
  }

  return 0;
}

int32_t tqProcessSeekReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqSeekReq  req = {0};
  int32_t     vgId = TD_VID(pTq->pVnode);
  SRpcMsg     rsp = {.info = pMsg->info};
  int         code = 0;

  if (tDeserializeSMqSeekReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  tqDebug("tmq seek: consumer:0x%" PRIx64 " vgId:%d, subkey %s", req.consumerId, vgId, req.subKey);

  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    tqWarn("tmq seek: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", req.consumerId, vgId, req.subKey);
    code = 0;
    goto end;
  }

  // 2. check consumer-vg assignment status
  taosRLockLatch(&pTq->lock);
  if (pHandle->consumerId != req.consumerId) {
    tqError("ERROR tmq seek: consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            req.consumerId, vgId, req.subKey, pHandle->consumerId);
    taosRUnLockLatch(&pTq->lock);
    code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    goto end;
  }

  //if consumer register to push manager, push empty to consumer to change vg status from TMQ_VG_STATUS__WAIT to TMQ_VG_STATUS__IDLE,
  //otherwise poll data failed after seek.
  tqUnregisterPushHandle(pTq, pHandle);
  taosRUnLockLatch(&pTq->lock);

end:
  rsp.code = code;
  tmsgSendRsp(&rsp);
  return 0;

//  SMqVgOffset vgOffset = {0};
//  int32_t     vgId = TD_VID(pTq->pVnode);
//
//  SDecoder decoder;
//  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
//  if (tDecodeMqVgOffset(&decoder, &vgOffset) < 0) {
//    tqError("vgId:%d failed to decode seek msg", vgId);
//    return -1;
//  }
//
//  tDecoderClear(&decoder);
//
//  tqDebug("topic:%s, vgId:%d process offset seek by consumer:0x%" PRIx64 ", req offset:%" PRId64,
//          vgOffset.offset.subKey, vgId, vgOffset.consumerId, vgOffset.offset.val.version);
//
//  STqOffset* pOffset = &vgOffset.offset;
//  if (pOffset->val.type != TMQ_OFFSET__LOG) {
//    tqError("vgId:%d, subKey:%s invalid seek offset type:%d", vgId, pOffset->subKey, pOffset->val.type);
//    return -1;
//  }
//
//  STqHandle* pHandle = taosHashGet(pTq->pHandle, pOffset->subKey, strlen(pOffset->subKey));
//  if (pHandle == NULL) {
//    tqError("tmq seek: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", vgOffset.consumerId, vgId, pOffset->subKey);
//    terrno = TSDB_CODE_INVALID_MSG;
//    return -1;
//  }
//
//  // 2. check consumer-vg assignment status
//  taosRLockLatch(&pTq->lock);
//  if (pHandle->consumerId != vgOffset.consumerId) {
//    tqDebug("ERROR tmq seek: consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
//            vgOffset.consumerId, vgId, pOffset->subKey, pHandle->consumerId);
//    terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
//    taosRUnLockLatch(&pTq->lock);
//    return -1;
//  }
//  taosRUnLockLatch(&pTq->lock);
//
//  // 3. check the offset info
//  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, pOffset->subKey);
//  if (pSavedOffset != NULL) {
//    if (pSavedOffset->val.type != TMQ_OFFSET__LOG) {
//      tqError("invalid saved offset type, vgId:%d sub:%s", vgId, pOffset->subKey);
//      return 0;  // no need to update the offset value
//    }
//
//    if (pSavedOffset->val.version == pOffset->val.version) {
//      tqDebug("vgId:%d subKey:%s no need to seek to %" PRId64 " prev offset:%" PRId64, vgId, pOffset->subKey,
//              pOffset->val.version, pSavedOffset->val.version);
//      return 0;
//    }
//  }
//
//  int64_t sver = 0, ever = 0;
//  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
//  if (pOffset->val.version < sver) {
//    pOffset->val.version = sver;
//  } else if (pOffset->val.version > ever) {
//    pOffset->val.version = ever;
//  }
//
//  // save the new offset value
//  if (pSavedOffset != NULL) {
//    tqDebug("vgId:%d sub:%s seek to:%" PRId64 " prev offset:%" PRId64, vgId, pOffset->subKey, pOffset->val.version,
//            pSavedOffset->val.version);
//  } else {
//    tqDebug("vgId:%d sub:%s seek to:%" PRId64 " not saved yet", vgId, pOffset->subKey, pOffset->val.version);
//  }
//
//  if (tqOffsetWrite(pTq->pOffsetStore, pOffset) < 0) {
//    tqError("failed to save offset, vgId:%d sub:%s seek to %" PRId64, vgId, pOffset->subKey, pOffset->val.version);
//    return -1;
//  }
//
//  tqDebug("topic:%s, vgId:%d consumer:0x%" PRIx64 " offset is update to:%" PRId64, vgOffset.offset.subKey, vgId,
//          vgOffset.consumerId, vgOffset.offset.val.version);
//
//  return 0;
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

int32_t tqProcessPollPush(STQ* pTq, SRpcMsg* pMsg) {
  int32_t vgId = TD_VID(pTq->pVnode);
  taosWLockLatch(&pTq->lock);
  if (taosHashGetSize(pTq->pPushMgr) > 0) {
    void* pIter = taosHashIterate(pTq->pPushMgr, NULL);

    while (pIter) {
      STqHandle* pHandle = *(STqHandle**)pIter;
      tqInfo("vgId:%d start set submit for pHandle:%p, consumer:0x%" PRIx64, vgId, pHandle, pHandle->consumerId);

      if (ASSERT(pHandle->msg != NULL)) {
        tqError("pHandle->msg should not be null");
        taosHashCancelIterate(pTq->pPushMgr, pIter);
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
  taosWUnLockLatch(&pTq->lock);
  return 0;
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqPollReq req = {0};
  int        code = 0;
  if (tDeserializeSMqPollReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pMsg->contLen);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int64_t      consumerId = req.consumerId;
  int32_t      reqEpoch = req.epoch;
  STqOffsetVal reqOffset = req.reqOffset;
  int32_t      vgId = TD_VID(pTq->pVnode);
  STqHandle*   pHandle = NULL;

  while (1) {
    taosWLockLatch(&pTq->lock);
    // 1. find handle
    pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
    if (pHandle == NULL) {
      do{
        if (tqMetaGetHandle(pTq, req.subKey) == 0){
          pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
          if(pHandle != NULL){
            break;
          }
        }
        tqError("tmq poll: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", consumerId, vgId, req.subKey);
        terrno = TSDB_CODE_INVALID_MSG;
        taosWUnLockLatch(&pTq->lock);
        return -1;
      }while(0);
    }

    // 2. check re-balance status
    if (pHandle->consumerId != consumerId) {
      tqError("ERROR tmq poll: consumer:0x%" PRIx64
              " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
              consumerId, TD_VID(pTq->pVnode), req.subKey, pHandle->consumerId);
      terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
      taosWUnLockLatch(&pTq->lock);
      return -1;
    }

    bool exec = tqIsHandleExec(pHandle);
    if (!exec) {
      tqSetHandleExec(pHandle);
      //      qSetTaskCode(pHandle->execHandle.task, TDB_CODE_SUCCESS);
      tqDebug("tmq poll: consumer:0x%" PRIx64 " vgId:%d, topic:%s, set handle exec, pHandle:%p", consumerId, vgId,
              req.subKey, pHandle);
      taosWUnLockLatch(&pTq->lock);
      break;
    }
    taosWUnLockLatch(&pTq->lock);

    tqDebug("tmq poll: consumer:0x%" PRIx64
            "vgId:%d, topic:%s, subscription is executing, wait for 10ms and retry, pHandle:%p",
            consumerId, vgId, req.subKey, pHandle);
    taosMsleep(10);
  }

  // 3. update the epoch value
  if (pHandle->epoch < reqEpoch) {
    tqDebug("tmq poll: consumer:0x%" PRIx64 " epoch update from %d to %d by poll req", consumerId, pHandle->epoch,
            reqEpoch);
    pHandle->epoch = reqEpoch;
  }

  char buf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(buf, TSDB_OFFSET_LEN, &reqOffset);
  tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey %s, recv poll req vgId:%d, req:%s, reqId:0x%" PRIx64,
          consumerId, req.epoch, pHandle->subKey, vgId, buf, req.reqId);

  code = tqExtractDataForMq(pTq, pHandle, &req, pMsg);
  tqSetHandleIdle(pHandle);

  tqDebug("tmq poll: consumer:0x%" PRIx64 " vgId:%d, topic:%s, set handle idle, pHandle:%p", consumerId, vgId,
          req.subKey, pHandle);
  return code;
}

int32_t tqProcessVgCommittedInfoReq(STQ* pTq, SRpcMsg* pMsg) {
  void* data = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);

  SMqVgOffset vgOffset = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)data, len);
  if (tDecodeMqVgOffset(&decoder, &vgOffset) < 0) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tDecoderClear(&decoder);

  STqOffset* pOffset = &vgOffset.offset;
  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, pOffset->subKey);
  if (pSavedOffset == NULL) {
    return TSDB_CODE_TMQ_NO_COMMITTED;
  }
  vgOffset.offset = *pSavedOffset;

  int32_t code = 0;
  tEncodeSize(tEncodeMqVgOffset, &vgOffset, len, code);
  if (code < 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  void* buf = rpcMallocCont(len);
  if (buf == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  SEncoder encoder;
  tEncoderInit(&encoder, buf, len);
  tEncodeMqVgOffset(&encoder, &vgOffset);
  tEncoderClear(&encoder);

  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = len, .code = 0};

  tmsgSendRsp(&rsp);

  return 0;
}

int32_t tqProcessVgWalInfoReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqPollReq req = {0};
  if (tDeserializeSMqPollReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pMsg->contLen);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  int64_t      consumerId = req.consumerId;
  STqOffsetVal reqOffset = req.reqOffset;
  int32_t      vgId = TD_VID(pTq->pVnode);

  // 1. find handle
  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s not found", consumerId, vgId, req.subKey);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // 2. check re-balance status
  taosRLockLatch(&pTq->lock);
  if (pHandle->consumerId != consumerId) {
    tqDebug("ERROR consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            consumerId, vgId, req.subKey, pHandle->consumerId);
    terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    taosRUnLockLatch(&pTq->lock);
    return -1;
  }
  taosRUnLockLatch(&pTq->lock);

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, &req);

  if (req.useSnapshot == true) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s snapshot not support wal info", consumerId, vgId, req.subKey);
    terrno = TSDB_CODE_INVALID_PARA;
    tDeleteMqDataRsp(&dataRsp);
    return -1;
  }

  dataRsp.rspOffset.type = TMQ_OFFSET__LOG;

  if (reqOffset.type == TMQ_OFFSET__LOG) {
    dataRsp.rspOffset.version = reqOffset.version;
  } else if(reqOffset.type < 0){
    STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, req.subKey);
    if (pOffset != NULL) {
      if (pOffset->val.type != TMQ_OFFSET__LOG) {
        tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s, no valid wal info", consumerId, vgId, req.subKey);
        terrno = TSDB_CODE_INVALID_PARA;
        tDeleteMqDataRsp(&dataRsp);
        return -1;
      }

      dataRsp.rspOffset.version = pOffset->val.version;
      tqInfo("consumer:0x%" PRIx64 " vgId:%d subkey:%s get assignment from store:%"PRId64, consumerId, vgId, req.subKey, dataRsp.rspOffset.version);
    }else{
      if (reqOffset.type == TMQ_OFFSET__RESET_EARLIEST) {
        dataRsp.rspOffset.version = sver;  // not consume yet, set the earliest position
      } else if (reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
        dataRsp.rspOffset.version = ever;
      }
      tqInfo("consumer:0x%" PRIx64 " vgId:%d subkey:%s get assignment from init:%"PRId64, consumerId, vgId, req.subKey, dataRsp.rspOffset.version);
    }
  } else {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s invalid offset type:%d", consumerId, vgId, req.subKey,
            reqOffset.type);
    terrno = TSDB_CODE_INVALID_PARA;
    tDeleteMqDataRsp(&dataRsp);
    return -1;
  }

  tqDoSendDataRsp(&pMsg->info, &dataRsp, req.epoch, req.consumerId, TMQ_MSG_TYPE__WALINFO_RSP, sver, ever);
  tDeleteMqDataRsp(&dataRsp);
  return 0;
}

int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;
  int32_t        vgId = TD_VID(pTq->pVnode);

  tqInfo("vgId:%d, tq process delete sub req %s", vgId, pReq->subKey);
  int32_t code = 0;

  taosWLockLatch(&pTq->lock);
  STqHandle* pHandle = taosHashGet(pTq->pHandle, pReq->subKey, strlen(pReq->subKey));
  if (pHandle) {
    while (tqIsHandleExec(pHandle)) {
      tqDebug("vgId:%d, topic:%s, subscription is executing, wait for 10ms and retry, pHandle:%p", vgId,
              pHandle->subKey, pHandle);
      taosMsleep(10);
    }

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
  taosWUnLockLatch(&pTq->lock);

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
  int         ret = 0;
  SMqRebVgReq req = {0};
  SDecoder    dc = {0};

  tDecoderInit(&dc, msg, msgLen);

  // decode req
  if (tDecodeSMqRebVgReq(&dc, &req) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&dc);
    return -1;
  }

  tqInfo("vgId:%d, tq process sub req:%s, Id:0x%" PRIx64 " -> Id:0x%" PRIx64, pTq->pVnode->config.vgId, req.subKey,
          req.oldConsumerId, req.newConsumerId);

  STqHandle* pHandle = NULL;
  while(1){
    pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
    if (pHandle || tqMetaGetHandle(pTq, req.subKey) < 0){
      break;
    }
  }

  if (pHandle == NULL) {
    if (req.oldConsumerId != -1) {
      tqError("vgId:%d, build new consumer handle %s for consumer:0x%" PRIx64 ", but old consumerId:0x%" PRIx64,
              req.vgId, req.subKey, req.newConsumerId, req.oldConsumerId);
    }
    if (req.newConsumerId == -1) {
      tqError("vgId:%d, tq invalid re-balance request, new consumerId %" PRId64 "", req.vgId, req.newConsumerId);
      goto end;
    }
    STqHandle handle = {0};
    ret = tqCreateHandle(pTq, &req, &handle);
    if(ret < 0){
      tqDestroyTqHandle(&handle);
      goto end;
    }
    ret = tqMetaSaveHandle(pTq, req.subKey, &handle);
  } else {
    taosWLockLatch(&pTq->lock);

    if (pHandle->consumerId == req.newConsumerId) {  // do nothing
      tqInfo("vgId:%d no switch consumer:0x%" PRIx64 " remains, because redo wal log", req.vgId, req.newConsumerId);
    } else {
      tqInfo("vgId:%d switch consumer from Id:0x%" PRIx64 " to Id:0x%" PRIx64, req.vgId, pHandle->consumerId, req.newConsumerId);
      atomic_store_64(&pHandle->consumerId, req.newConsumerId);
      //    atomic_add_fetch_32(&pHandle->epoch, 1);

      // kill executing task
      //    if(tqIsHandleExec(pHandle)) {
      //      qTaskInfo_t pTaskInfo = pHandle->execHandle.task;
      //      if (pTaskInfo != NULL) {
      //        qKillTask(pTaskInfo, TSDB_CODE_SUCCESS);
      //      }

      //      if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      //        qStreamCloseTsdbReader(pTaskInfo);
      //      }
      //    }
      // remove if it has been register in the push manager, and return one empty block to consumer
      tqUnregisterPushHandle(pTq, pHandle);
      ret = tqMetaSaveHandle(pTq, req.subKey, pHandle);
    }
    taosWUnLockLatch(&pTq->lock);
  }

end:
  tDecoderClear(&dc);
  return ret;
}

void freePtr(void* ptr) { taosMemoryFree(*(void**)ptr); }

int32_t tqExpandTask(STQ* pTq, SStreamTask* pTask, int64_t ver) {
  int32_t vgId = TD_VID(pTq->pVnode);

  pTask->id.idStr = createStreamTaskIdStr(pTask->id.streamId, pTask->id.taskId);
  pTask->refCnt = 1;
  pTask->status.schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->inputQueue = streamQueueOpen(512 << 10);
  pTask->outputInfo.queue = streamQueueOpen(512 << 10);

  if (pTask->inputQueue == NULL || pTask->outputInfo.queue == NULL) {
    tqError("s-task:%s failed to prepare the input/output queue, initialize task failed", pTask->id.idStr);
    return -1;
  }

  pTask->tsInfo.init = taosGetTimestampMs();
  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputInfo.status = TASK_OUTPUT_STATUS__NORMAL;
  pTask->pMsgCb = &pTq->pVnode->msgCb;
  pTask->pMeta = pTq->pStreamMeta;

  // backup the initial status, and set it to be TASK_STATUS__INIT
  pTask->chkInfo.version = ver;
  pTask->chkInfo.currentVer = ver;

  pTask->dataRange.range.maxVer = ver;
  pTask->dataRange.range.minVer = ver;

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    SStreamTask* pSateTask = pTask;
    SStreamTask task = {0};
    if (pTask->info.fillHistory) {
      task.id = pTask->streamTaskId;
      task.pMeta = pTask->pMeta;
      pSateTask = &task;
    }

    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pSateTask, false, -1, -1);
    if (pTask->pState == NULL) {
      return -1;
    }

    SReadHandle handle = {.vnode = pTq->pVnode,
                          .initTqReader = 1,
                          .pStateBackend = pTask->pState,
                          .fillHistory = pTask->info.fillHistory,
                          .winRange = pTask->dataRange.window};
    initStorageAPI(&handle.api);

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }

    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    SStreamTask* pSateTask = pTask;
    SStreamTask task = {0};
    if (pTask->info.fillHistory) {
      task.id = pTask->streamTaskId;
      task.pMeta = pTask->pMeta;
      pSateTask = &task;
    }
    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pSateTask, false, -1, -1);
    if (pTask->pState == NULL) {
      return -1;
    }

    int32_t     numOfVgroups = (int32_t)taosArrayGetSize(pTask->pUpstreamEpInfoList);
    SReadHandle handle = {.vnode = NULL,
                          .numOfVgroups = numOfVgroups,
                          .pStateBackend = pTask->pState,
                          .fillHistory = pTask->info.fillHistory,
                          .winRange = pTask->dataRange.window};
    initStorageAPI(&handle.api);

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }
    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  }

  // sink
  if (pTask->outputInfo.type == TASK_OUTPUT__SMA) {
    pTask->smaSink.vnode = pTq->pVnode;
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->outputInfo.type == TASK_OUTPUT__TABLE) {
    pTask->tbSink.vnode = pTq->pVnode;
    pTask->tbSink.tbSinkFunc = tqSinkToTablePipeline;

    int32_t   ver1 = 1;
    SMetaInfo info = {0};
    int32_t   code = metaGetInfo(pTq->pVnode->pMeta, pTask->tbSink.stbUid, &info, NULL);
    if (code == TSDB_CODE_SUCCESS) {
      ver1 = info.skmVer;
    }

    SSchemaWrapper* pschemaWrapper = pTask->tbSink.pSchemaWrapper;
    pTask->tbSink.pTSchema = tBuildTSchema(pschemaWrapper->pSchema, pschemaWrapper->nCols, ver1);
    if (pTask->tbSink.pTSchema == NULL) {
      return -1;
    }
    pTask->tbSink.pTblInfo = tSimpleHashInit(10240, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    tSimpleHashSetFreeFp(pTask->tbSink.pTblInfo, freePtr);
  }

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    SWalFilterCond cond = {.deleteMsg = 1};  // delete msg also extract from wal files
    pTask->exec.pWalReader = walOpenReader(pTq->pVnode->pWal, &cond);
  }

  // reset the task status from unfinished transaction
  if (pTask->status.taskStatus == TASK_STATUS__PAUSE) {
    tqWarn("s-task:%s reset task status to be normal, kept in meta status: Paused", pTask->id.idStr);
    pTask->status.taskStatus = TASK_STATUS__NORMAL;
  }

  taosThreadMutexInit(&pTask->lock, NULL);
  streamSetupScheduleTrigger(pTask);

  tqInfo("vgId:%d expand stream task, s-task:%s, checkpoint ver:%" PRId64
         " child id:%d, level:%d, fill-history:%d, trigger:%" PRId64 " ms, disable pause",
         vgId, pTask->id.idStr, pTask->chkInfo.version, pTask->info.selfChildId, pTask->info.taskLevel,
         pTask->info.fillHistory, pTask->triggerParam);

  // next valid version will add one
  pTask->chkInfo.version += 1;
  return 0;
}

int32_t tqProcessStreamTaskCheckReq(STQ* pTq, SRpcMsg* pMsg) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamTaskCheckReq req;
  SDecoder            decoder;

  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamTaskCheckReq(&decoder, &req);
  tDecoderClear(&decoder);

  int32_t taskId = req.downstreamTaskId;

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
  if (pTask != NULL) {
    rsp.status = streamTaskCheckStatus(pTask);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);

    const char* pStatus = streamGetTaskStatusStr(pTask->status.taskStatus);
    tqDebug("s-task:%s status:%s, recv task check req(reqId:0x%" PRIx64 ") task:0x%x (vgId:%d), ready:%d",
            pTask->id.idStr, pStatus, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  } else {
    rsp.status = 0;
    tqDebug("tq recv task check(taskId:0x%x not built yet) req(reqId:0x%" PRIx64 ") from task:0x%x (vgId:%d), rsp status %d",
            taskId, rsp.reqId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  }

  return streamSendCheckRsp(pTq->pStreamMeta, &req, &rsp, &pMsg->info, taskId);
}

int32_t tqProcessStreamTaskCheckRsp(STQ* pTq, int64_t sversion, SRpcMsg* pMsg) {
  char* pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);

  int32_t             code;
  SStreamTaskCheckRsp rsp;

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pReq, len);
  code = tDecodeStreamTaskCheckRsp(&decoder, &rsp);

  if (code < 0) {
    tDecoderClear(&decoder);
    return -1;
  }

  tDecoderClear(&decoder);
  tqDebug("tq task:0x%x (vgId:%d) recv check rsp(reqId:0x%" PRIx64 ") from 0x%x (vgId:%d) status %d",
          rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.status);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, rsp.upstreamTaskId);
  if (pTask == NULL) {
    tqError("tq failed to locate the stream task:0x%x (vgId:%d), it may have been destroyed", rsp.upstreamTaskId,
            pTq->pStreamMeta->vgId);
    terrno = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    return -1;
  }

  code = streamProcessCheckRsp(pTask, &rsp);
  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return code;
}

int32_t tqProcessTaskDeployReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  int32_t code = 0;
  int32_t vgId = TD_VID(pTq->pVnode);

  if (tsDisableStream) {
    return 0;
  }

  // 1.deserialize msg and build task
  SStreamTask* pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (pTask == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed to create stream task due to out of memory, alloc size:%d", vgId,
            (int32_t)sizeof(SStreamTask));
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

  SStreamMeta* pStreamMeta = pTq->pStreamMeta;

  // 2.save task, use the newest commit version as the initial start version of stream task.
  int32_t taskId = pTask->id.taskId;
  bool    added = false;

  taosWLockLatch(&pStreamMeta->lock);
  code = streamMetaRegisterTask(pStreamMeta, sversion, pTask, &added);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pStreamMeta);

  if (code < 0) {
    tqError("vgId:%d failed to add s-task:0x%x, total:%d", vgId, pTask->id.taskId, numOfTasks);
    tFreeStreamTask(pTask);
    taosWUnLockLatch(&pStreamMeta->lock);
    return -1;
  }

  // not added into meta store
  if (!added) {
    tqWarn("vgId:%d failed to add s-task:0x%x, already exists in meta store", vgId, taskId);
    tFreeStreamTask(pTask);
    pTask = NULL;
  }

  taosWUnLockLatch(&pStreamMeta->lock);

  tqDebug("vgId:%d s-task:0x%x is deployed and add into meta, numOfTasks:%d", vgId, taskId, numOfTasks);

  // 3. It's an fill history task, do nothing. wait for the main task to start it
  SStreamTask* p = streamMetaAcquireTask(pStreamMeta, taskId);
  if (p != NULL) { // reset the downstreamReady flag.
    streamTaskCheckDownstreamTasks(p);
  }

  streamMetaReleaseTask(pStreamMeta, p);
  return 0;
}

int32_t tqProcessTaskScanHistory(STQ* pTq, SRpcMsg* pMsg) {
  SStreamScanHistoryReq* pReq = (SStreamScanHistoryReq*)pMsg->pCont;
  SStreamMeta*           pMeta = pTq->pStreamMeta;

  int32_t      code = TSDB_CODE_SUCCESS;
  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->taskId);
  if (pTask == NULL) {
    tqError("vgId:%d failed to acquire stream task:0x%x during stream recover, task may have been destroyed",
            pMeta->vgId, pReq->taskId);
    return -1;
  }

  // do recovery step1
  const char* id = pTask->id.idStr;
  const char* pStatus = streamGetTaskStatusStr(pTask->status.taskStatus);
  tqDebug("s-task:%s start scan-history stage(step 1), status:%s", id, pStatus);

  if (pTask->tsInfo.step1Start == 0) {
    ASSERT(pTask->status.pauseAllowed == false);
    pTask->tsInfo.step1Start = taosGetTimestampMs();
    if (pTask->info.fillHistory == 1) {
      streamTaskEnablePause(pTask);
    }
  } else {
    tqDebug("s-task:%s resume from paused, start ts:%"PRId64, pTask->id.idStr, pTask->tsInfo.step1Start);
  }

  // we have to continue retrying to successfully execute the scan history task.
  int8_t schedStatus = atomic_val_compare_exchange_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE,
                                                     TASK_SCHED_STATUS__WAITING);
  if (schedStatus != TASK_SCHED_STATUS__INACTIVE) {
    tqError(
        "s-task:%s failed to start scan-history in first stream time window since already started, unexpected "
        "sched-status:%d",
        id, schedStatus);
    return 0;
  }

  if (pTask->info.fillHistory == 1) {
    ASSERT(pTask->status.pauseAllowed == true);
  }

  streamSourceScanHistoryData(pTask);
  if (pTask->status.taskStatus == TASK_STATUS__PAUSE) {
    double el = (taosGetTimestampMs() - pTask->tsInfo.step1Start) / 1000.0;
    tqDebug("s-task:%s is paused in the step1, elapsed time:%.2fs, sched-status:%d", pTask->id.idStr, el,
            TASK_SCHED_STATUS__INACTIVE);
    atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
    return 0;
  }

  // the following procedure should be executed, no matter status is stop/pause or not
  double el = (taosGetTimestampMs() - pTask->tsInfo.step1Start) / 1000.0;
  tqDebug("s-task:%s scan-history stage(step 1) ended, elapsed time:%.2fs", id, el);

  if (pTask->info.fillHistory) {
    SVersionRange* pRange = NULL;
    SStreamTask*   pStreamTask = NULL;
    bool           done = false;

    // 1. get the related stream task
    pStreamTask = streamMetaAcquireTask(pMeta, pTask->streamTaskId.taskId);
    if (pStreamTask == NULL) {
      // todo delete this task, if the related stream task is dropped
      qError("failed to find s-task:0x%x, it may have been destroyed, drop fill-history task:%s",
             pTask->streamTaskId.taskId, pTask->id.idStr);

      tqDebug("s-task:%s fill-history task set status to be dropping", id);

      streamMetaUnregisterTask(pMeta, pTask->id.taskId);
      streamMetaReleaseTask(pMeta, pTask);
      return -1;
    }

    ASSERT(pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE);

    // 2. it cannot be paused, when the stream task in TASK_STATUS__SCAN_HISTORY status. Let's wait for the
    // stream task get ready for scan history data
    while (pStreamTask->status.taskStatus == TASK_STATUS__SCAN_HISTORY) {
      tqDebug(
          "s-task:%s level:%d related stream task:%s(status:%s) not ready for halt, wait for it and recheck in 100ms",
          id, pTask->info.taskLevel, pStreamTask->id.idStr, streamGetTaskStatusStr(pStreamTask->status.taskStatus));
      taosMsleep(100);
    }

    // now we can stop the stream task execution
    streamTaskHalt(pStreamTask);

    tqDebug("s-task:%s level:%d sched-status:%d is halt by fill-history task:%s", pStreamTask->id.idStr,
            pStreamTask->info.taskLevel, pStreamTask->status.schedStatus, id);

    // if it's an source task, extract the last version in wal.
    pRange = &pTask->dataRange.range;
    int64_t latestVer = walReaderGetCurrentVer(pStreamTask->exec.pWalReader);
    done = streamHistoryTaskSetVerRangeStep2(pTask, latestVer);

    if (done) {
      pTask->tsInfo.step2Start = taosGetTimestampMs();
      streamTaskEndScanWAL(pTask);
    } else {
      STimeWindow* pWindow = &pTask->dataRange.window;
      tqDebug("s-task:%s level:%d verRange:%" PRId64 " - %" PRId64 " window:%" PRId64 "-%" PRId64
              ", do secondary scan-history from WAL after halt the related stream task:%s",
              id, pTask->info.taskLevel, pRange->minVer, pRange->maxVer, pWindow->skey, pWindow->ekey,
              pStreamTask->id.idStr);
      ASSERT(pTask->status.schedStatus == TASK_SCHED_STATUS__WAITING);

      pTask->tsInfo.step2Start = taosGetTimestampMs();
      streamSetParamForStreamScannerStep2(pTask, pRange, pWindow);

      int64_t dstVer = pTask->dataRange.range.minVer - 1;

      pTask->chkInfo.currentVer = dstVer;
      walReaderSetSkipToVersion(pTask->exec.pWalReader, dstVer);
      tqDebug("s-task:%s wal reader start scan WAL verRange:%" PRId64 "-%" PRId64 ", set sched-status:%d", id, dstVer,
              pTask->dataRange.range.maxVer, TASK_SCHED_STATUS__INACTIVE);

      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);

      // set the fill-history task to be normal
      if (pTask->info.fillHistory == 1) {
        streamSetStatusNormal(pTask);
      }

      tqStartStreamTasks(pTq);
    }

    streamMetaReleaseTask(pMeta, pTask);
    streamMetaReleaseTask(pMeta, pStreamTask);
  } else {
    // todo update the chkInfo version for current task.
    // this task has an associated history stream task, so we need to scan wal from the end version of
    // history scan. The current version of chkInfo.current is not updated during the history scan
    STimeWindow* pWindow = &pTask->dataRange.window;

    if (pTask->historyTaskId.taskId == 0) {
      *pWindow = (STimeWindow){INT64_MIN, INT64_MAX};
      tqDebug(
          "s-task:%s scan-history in stream time window completed, no related fill-history task, reset the time "
          "window:%" PRId64 " - %" PRId64,
          id, pWindow->skey, pWindow->ekey);
      qStreamInfoResetTimewindowFilter(pTask->exec.pExecutor);
    } else {
      // when related fill-history task exists, update the fill-history time window only when the
      // state transfer is completed.
      tqDebug(
          "s-task:%s scan-history in stream time window completed, now start to handle data from WAL, start "
          "ver:%" PRId64 ", window:%" PRId64 " - %" PRId64,
          id, pTask->chkInfo.currentVer, pWindow->skey, pWindow->ekey);
    }

    // notify the downstream agg tasks that upstream tasks are ready to processing the WAL data, update the
    code = streamTaskScanHistoryDataComplete(pTask);
    streamMetaReleaseTask(pMeta, pTask);

    // when all source task complete to scan history data in stream time window, they are allowed to handle stream data
    // at the same time.
    return code;
  }

  return 0;
}

// notify the downstream tasks to transfer executor state after handle all history blocks.
int32_t tqProcessTaskTransferStateReq(STQ* pTq, SRpcMsg* pMsg) {
  char*   pReq = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);

  SStreamTransferReq req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pReq, len);
  int32_t code = tDecodeStreamScanHistoryFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  tqDebug("vgId:%d start to process transfer state msg, from s-task:0x%x", pTq->pStreamMeta->vgId, req.downstreamTaskId);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, req.downstreamTaskId);
  if (pTask == NULL) {
    tqError("failed to find task:0x%x, it may have been dropped already. process transfer state failed", req.downstreamTaskId);
    return -1;
  }

  int32_t remain = streamAlignTransferState(pTask);
  if (remain > 0) {
    tqDebug("s-task:%s receive upstream transfer state msg, remain:%d", pTask->id.idStr, remain);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  }

  // transfer the ownership of executor state
  tqDebug("s-task:%s all upstream tasks send transfer msg, open transfer state flag", pTask->id.idStr);
  ASSERT(pTask->streamTaskId.taskId != 0 && pTask->info.fillHistory == 1);

  pTask->status.transferState = true;

  streamSchedExec(pTask);
  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return 0;
}

int32_t tqProcessTaskScanHistoryFinishReq(STQ* pTq, SRpcMsg* pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamScanHistoryFinishReq req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeStreamScanHistoryFinishReq(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, req.downstreamTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d process scan history finish msg, failed to find task:0x%x, it may be destroyed",
            pTq->pStreamMeta->vgId, req.downstreamTaskId);
    return -1;
  }

  tqDebug("s-task:%s receive scan-history finish msg from task:0x%x", pTask->id.idStr, req.upstreamTaskId);

  int32_t code = streamProcessScanHistoryFinishReq(pTask, &req, &pMsg->info);
  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return code;
}

int32_t tqProcessTaskScanHistoryFinishRsp(STQ* pTq, SRpcMsg* pMsg) {
  char*   msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  // deserialize
  SStreamCompleteHistoryMsg req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  tDecodeCompleteHistoryDataMsg(&decoder, &req);
  tDecoderClear(&decoder);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, req.upstreamTaskId);
  if (pTask == NULL) {
    tqError("vgId:%d process scan history finish rsp, failed to find task:0x%x, it may be destroyed",
            pTq->pStreamMeta->vgId, req.upstreamTaskId);
    return -1;
  }

  int32_t remain = atomic_sub_fetch_32(&pTask->notReadyTasks, 1);
  if (remain > 0) {
    tqDebug("s-task:%s scan-history finish rsp received from downstream task:0x%x, remain:%d not send finish rsp",
            pTask->id.idStr, req.downstreamId, remain);
  } else {
    tqDebug(
        "s-task:%s scan-history finish rsp received from downstream task:0x%x, all downstream tasks rsp scan-history "
        "completed msg", pTask->id.idStr, req.downstreamId);
    streamProcessScanHistoryFinishRsp(pTask);
  }

  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return 0;
}

int32_t extractDelDataBlock(const void* pData, int32_t len, int64_t ver, SStreamRefDataBlock** pRefBlock) {
  SDecoder*   pCoder = &(SDecoder){0};
  SDeleteRes* pRes = &(SDeleteRes){0};

  (*pRefBlock) = NULL;

  pRes->uidList = taosArrayInit(0, sizeof(tb_uid_t));
  if (pRes->uidList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tDecoderInit(pCoder, (uint8_t*)pData, len);
  tDecodeDeleteRes(pCoder, pRes);
  tDecoderClear(pCoder);

  int32_t numOfTables = taosArrayGetSize(pRes->uidList);
  if (numOfTables == 0 || pRes->affectedRows == 0) {
    taosArrayDestroy(pRes->uidList);
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pDelBlock = createSpecialDataBlock(STREAM_DELETE_DATA);
  blockDataEnsureCapacity(pDelBlock, numOfTables);
  pDelBlock->info.rows = numOfTables;
  pDelBlock->info.version = ver;

  for (int32_t i = 0; i < numOfTables; i++) {
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
  *pRefBlock = taosAllocateQitem(sizeof(SStreamRefDataBlock), DEF_QITEM, 0);
  if ((*pRefBlock) == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (*pRefBlock)->type = STREAM_INPUT__REF_DATA_BLOCK;
  (*pRefBlock)->pBlock = pDelBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  int32_t taskId = pReq->taskId;
  int32_t vgId = TD_VID(pTq->pVnode);

  if (taskId == STREAM_TASK_STATUS_CHECK_ID) {
    tqStreamTasksStatusCheck(pTq);
    return 0;
  }

  if (taskId == EXTRACT_DATA_FROM_WAL_ID) {  // all tasks are extracted submit data from the wal
    tqStreamTasksScanWal(pTq);
    return 0;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask != NULL) {
    // even in halt status, the data in inputQ must be processed
    int8_t st = pTask->status.taskStatus;
    if (st == TASK_STATUS__NORMAL || st == TASK_STATUS__SCAN_HISTORY) {
      tqDebug("vgId:%d s-task:%s start to process block from inputQ, last chk point:%" PRId64, vgId, pTask->id.idStr,
              pTask->chkInfo.version);
      streamProcessRunReq(pTask);
    } else {
      atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
      tqDebug("vgId:%d s-task:%s ignore run req since not in ready state, status:%s, sched-status:%d", vgId,
              pTask->id.idStr, streamGetTaskStatusStr(pTask->status.taskStatus), pTask->status.schedStatus);
    }

    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    tqStartStreamTasks(pTq);
    return 0;
  } else { // NOTE: pTask->status.schedStatus is not updated since it is not be handled by the run exec.
    // todo add one function to handle this
    tqError("vgId:%d failed to found s-task, taskId:0x%x may have been dropped", vgId, taskId);
    return -1;
  }
}

int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg, bool exec) {
  char*   msgStr = pMsg->pCont;
  char*   msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t msgLen = pMsg->contLen - sizeof(SMsgHead);

  SStreamDispatchReq req = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamDispatchReq(&decoder, &req);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, req.taskId);
  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    streamProcessDispatchMsg(pTask, &req, &rsp, exec);
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

  int32_t vgId = pTq->pStreamMeta->vgId;
  if (pTask) {
    streamProcessDispatchRsp(pTask, pRsp, pMsg->code);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  } else {
    tqDebug("vgId:%d failed to handle the dispatch rsp, since find task:0x%x failed", vgId, taskId);
    return TSDB_CODE_INVALID_MSG;
  }
}

int32_t tqProcessTaskDropReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SVDropStreamTaskReq* pReq = (SVDropStreamTaskReq*)msg;
  tqDebug("vgId:%d receive msg to drop stream task:0x%x", TD_VID(pTq->pVnode), pReq->taskId);
  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  if (pTask == NULL) {
    tqError("vgId:%d failed to acquire s-task:0x%x when dropping it", pTq->pStreamMeta->vgId, pReq->taskId);
    return 0;
  }

  streamMetaUnregisterTask(pTq->pStreamMeta, pReq->taskId);
  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return 0;
}

int32_t tqProcessTaskPauseReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SVPauseStreamTaskReq* pReq = (SVPauseStreamTaskReq*)msg;

  SStreamMeta* pMeta = pTq->pStreamMeta;
  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->taskId);
  if (pTask == NULL) {
    tqError("vgId:%d process pause req, failed to acquire task:0x%x, it may have been dropped already", pMeta->vgId,
            pReq->taskId);
    // since task is in [STOP|DROPPING] state, it is safe to assume the pause is active
    return TSDB_CODE_SUCCESS;
  }

  tqDebug("s-task:%s receive pause msg from mnode", pTask->id.idStr);
  streamTaskPause(pTask);

  SStreamTask* pHistoryTask = NULL;
  if (pTask->historyTaskId.taskId != 0) {
    pHistoryTask = streamMetaAcquireTask(pMeta, pTask->historyTaskId.taskId);
    if (pHistoryTask == NULL) {
      tqError("vgId:%d process pause req, failed to acquire fill-history task:0x%x, it may have been dropped already",
              pMeta->vgId, pTask->historyTaskId.taskId);
      streamMetaReleaseTask(pMeta, pTask);

      // since task is in [STOP|DROPPING] state, it is safe to assume the pause is active
      return TSDB_CODE_SUCCESS;
    }

    tqDebug("s-task:%s fill-history task handle paused along with related stream task", pHistoryTask->id.idStr);

    streamTaskPause(pHistoryTask);
    streamMetaReleaseTask(pMeta, pHistoryTask);
  }

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

int32_t tqProcessTaskResumeImpl(STQ* pTq, SStreamTask* pTask, int64_t sversion, int8_t igUntreated) {
  int32_t vgId = pTq->pStreamMeta->vgId;
  if (pTask == NULL) {
    return -1;
  }

  // todo: handle the case: resume from halt to pause/ from halt to normal/ from pause to normal
  streamTaskResume(pTask);

  int32_t level = pTask->info.taskLevel;
  int8_t  status = pTask->status.taskStatus;
  if (status == TASK_STATUS__NORMAL || status == TASK_STATUS__SCAN_HISTORY) {
    // no lock needs to secure the access of the version
    if (igUntreated && level == TASK_LEVEL__SOURCE && !pTask->info.fillHistory) {
      // discard all the data  when the stream task is suspended.
      walReaderSetSkipToVersion(pTask->exec.pWalReader, sversion);
      tqDebug("vgId:%d s-task:%s resume to exec, prev paused version:%" PRId64 ", start from vnode ver:%" PRId64
              ", schedStatus:%d",
              vgId, pTask->id.idStr, pTask->chkInfo.currentVer, sversion, pTask->status.schedStatus);
    } else {  // from the previous paused version and go on
      tqDebug("vgId:%d s-task:%s resume to exec, from paused ver:%" PRId64 ", vnode ver:%" PRId64 ", schedStatus:%d",
              vgId, pTask->id.idStr, pTask->chkInfo.currentVer, sversion, pTask->status.schedStatus);
    }

    if (level == TASK_LEVEL__SOURCE && pTask->info.fillHistory && pTask->status.taskStatus == TASK_STATUS__SCAN_HISTORY) {
      streamStartScanHistoryAsync(pTask, igUntreated);
    } else if (level == TASK_LEVEL__SOURCE && (taosQueueItemSize(pTask->inputQueue->queue) == 0)) {
      tqStartStreamTasks(pTq);
    } else {
      streamSchedExec(pTask);
    }
  }

  streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  return 0;
}

int32_t tqProcessTaskResumeReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SVResumeStreamTaskReq* pReq = (SVResumeStreamTaskReq*)msg;
  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  int32_t code = tqProcessTaskResumeImpl(pTq, pTask, sversion, pReq->igUntreated);
  if (code != 0) {
    return code;
  }

  SStreamTask* pHistoryTask = streamMetaAcquireTask(pTq->pStreamMeta, pTask->historyTaskId.taskId);
  if (pHistoryTask) {
    code = tqProcessTaskResumeImpl(pTq, pHistoryTask, sversion, pReq->igUntreated);
  }

  return code;
}

int32_t tqProcessTaskRetrieveReq(STQ* pTq, SRpcMsg* pMsg) {
  char*    msgStr = pMsg->pCont;
  char*    msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t  msgLen = pMsg->contLen - sizeof(SMsgHead);
  SDecoder decoder;

  SStreamRetrieveReq req;
  tDecoderInit(&decoder, (uint8_t*)msgBody, msgLen);
  tDecodeStreamRetrieveReq(&decoder, &req);
  tDecoderClear(&decoder);

  int32_t      taskId = req.dstTaskId;
  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);

  if (pTask) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
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
  if (pTask != NULL) {
    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    streamProcessDispatchMsg(pTask, &req, &rsp, false);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
    return 0;
  } else {

    tDeleteStreamDispatchReq(&req);
  }

  code = TSDB_CODE_STREAM_TASK_NOT_EXIST;

FAIL:
  if (pMsg->info.handle == NULL) {
    tqError("s-task:0x%x vgId:%d msg handle is null, abort enqueue dispatch msg", pTq->pStreamMeta->vgId, taskId);
    return -1;
  }

  SMsgHead* pRspHead = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  if (pRspHead == NULL) {
    SRpcMsg rsp = {.code = TSDB_CODE_OUT_OF_MEMORY, .info = pMsg->info};
    tqError("s-task:0x%x send dispatch error rsp, code:%s", taskId, tstrerror(code));
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

  int32_t len = sizeof(SMsgHead) + sizeof(SStreamDispatchRsp);
  SRpcMsg rsp = { .code = code, .info = pMsg->info, .contLen = len, .pCont = pRspHead};
  tqError("s-task:0x%x send dispatch error rsp, code:%s", taskId, tstrerror(code));

  tmsgSendRsp(&rsp);
  rpcFreeCont(pMsg->pCont);
  taosFreeQitem(pMsg);
  return -1;
}

int32_t tqCheckLogInWal(STQ* pTq, int64_t sversion) { return sversion <= pTq->walLogLastVer; }

