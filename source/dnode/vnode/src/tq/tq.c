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
#define WAL_READ_TASKS_ID (-1)

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

static void destroyTqHandle(void* data) {
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
      tqDebug("vgId:%d s-task:%s is closed in %" PRId64 " ms", pTq->pStreamMeta->vgId, pTask->id.idStr, el);
    }

    taosWUnLockLatch(&pTq->pStreamMeta->lock);
  }
}

static int32_t doSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, const SMqDataRsp* pRsp, int32_t epoch,
                             int64_t consumerId, int32_t type) {
  int32_t len = 0;
  int32_t code = 0;

  if (type == TMQ_MSG_TYPE__POLL_RSP) {
    tEncodeSize(tEncodeMqDataRsp, pRsp, len, code);
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
    tEncodeMqDataRsp(&encoder, pRsp);
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

int32_t tqPushDataRsp(STqHandle* pHandle, int32_t vgId) {
  SMqDataRsp dataRsp = {0};
  dataRsp.head.consumerId = pHandle->consumerId;
  dataRsp.head.epoch = pHandle->epoch;
  dataRsp.head.mqMsgType = TMQ_MSG_TYPE__POLL_RSP;

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  tqDoSendDataRsp(&pHandle->msg->info, &dataRsp, pHandle->epoch, pHandle->consumerId, TMQ_MSG_TYPE__POLL_RSP, sver,
                  ever);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, tListLen(buf1), &dataRsp.reqOffset);
  tFormatOffset(buf2, tListLen(buf2), &dataRsp.rspOffset);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) push rsp, block num: %d, req:%s, rsp:%s", vgId,
          dataRsp.head.consumerId, dataRsp.head.epoch, dataRsp.blockNum, buf1, buf2);
  return 0;
}

int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataRsp* pRsp,
                      int32_t type, int32_t vgId) {
  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  tqDoSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type, sver, ever);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, 80, &pRsp->reqOffset);
  tFormatOffset(buf2, 80, &pRsp->rspOffset);

  tqDebug("vgId:%d consumer:0x%" PRIx64 " (epoch %d) send rsp, block num:%d, req:%s, rsp:%s, reqId:0x%" PRIx64, vgId,
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
    if (pOffset->val.version + 1 == sversion) {
      pOffset->val.version += 1;
    }
  } else {
    tqError("invalid commit offset type:%d", pOffset->val.type);
    return -1;
  }

  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, pOffset->subKey);
  if (pSavedOffset != NULL && tqOffsetLessOrEqual(pOffset, pSavedOffset)) {
    tqDebug("not update the offset, vgId:%d sub:%s since committed:%" PRId64 " less than/equal to existed:%" PRId64,
            vgId, pOffset->subKey, pOffset->val.version, pSavedOffset->val.version);
    return 0;  // no need to update the offset value
  }

  // save the new offset value
  if (tqOffsetWrite(pTq->pOffsetStore, pOffset) < 0) {
    return -1;
  }

  return 0;
}

int32_t tqProcessSeekReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVgOffset vgOffset = {0};
  int32_t     vgId = TD_VID(pTq->pVnode);

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeMqVgOffset(&decoder, &vgOffset) < 0) {
    tqError("vgId:%d failed to decode seek msg", vgId);
    return -1;
  }

  tDecoderClear(&decoder);

  tqDebug("topic:%s, vgId:%d process offset seek by consumer:0x%" PRIx64 ", req offset:%" PRId64,
          vgOffset.offset.subKey, vgId, vgOffset.consumerId, vgOffset.offset.val.version);

  STqOffset* pOffset = &vgOffset.offset;
  if (pOffset->val.type != TMQ_OFFSET__LOG) {
    tqError("vgId:%d, subKey:%s invalid seek offset type:%d", vgId, pOffset->subKey, pOffset->val.type);
    return -1;
  }

  STqHandle* pHandle = taosHashGet(pTq->pHandle, pOffset->subKey, strlen(pOffset->subKey));
  if (pHandle == NULL) {
    tqError("tmq seek: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", vgOffset.consumerId, vgId, pOffset->subKey);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  // 2. check consumer-vg assignment status
  taosRLockLatch(&pTq->lock);
  if (pHandle->consumerId != vgOffset.consumerId) {
    tqDebug("ERROR tmq seek: consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            vgOffset.consumerId, vgId, pOffset->subKey, pHandle->consumerId);
    terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    taosRUnLockLatch(&pTq->lock);
    return -1;
  }
  taosRUnLockLatch(&pTq->lock);

  // 3. check the offset info
  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, pOffset->subKey);
  if (pSavedOffset != NULL) {
    if (pSavedOffset->val.type != TMQ_OFFSET__LOG) {
      tqError("invalid saved offset type, vgId:%d sub:%s", vgId, pOffset->subKey);
      return 0;  // no need to update the offset value
    }

    if (pSavedOffset->val.version == pOffset->val.version) {
      tqDebug("vgId:%d subKey:%s no need to seek to %" PRId64 " prev offset:%" PRId64, vgId, pOffset->subKey,
              pOffset->val.version, pSavedOffset->val.version);
      return 0;
    }
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  if (pOffset->val.version < sver) {
    pOffset->val.version = sver;
  } else if (pOffset->val.version > ever) {
    pOffset->val.version = ever;
  }

  // save the new offset value
  if (pSavedOffset != NULL) {
    tqDebug("vgId:%d sub:%s seek to:%" PRId64 " prev offset:%" PRId64, vgId, pOffset->subKey, pOffset->val.version,
            pSavedOffset->val.version);
  } else {
    tqDebug("vgId:%d sub:%s seek to:%" PRId64 " not saved yet", vgId, pOffset->subKey, pOffset->val.version);
  }

  if (tqOffsetWrite(pTq->pOffsetStore, pOffset) < 0) {
    tqError("failed to save offset, vgId:%d sub:%s seek to %" PRId64, vgId, pOffset->subKey, pOffset->val.version);
    return -1;
  }

  tqDebug("topic:%s, vgId:%d consumer:0x%" PRIx64 " offset is update to:%" PRId64, vgOffset.offset.subKey, vgId,
          vgOffset.consumerId, vgOffset.offset.val.version);

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
      tqError("tmq poll: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", consumerId, vgId, req.subKey);
      terrno = TSDB_CODE_INVALID_MSG;
      taosWUnLockLatch(&pTq->lock);
      return -1;
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
      tqDebug("tmq poll: consumer:0x%" PRIx64 "vgId:%d, topic:%s, set handle exec, pHandle:%p", consumerId, vgId,
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

  char buf[80];
  tFormatOffset(buf, 80, &reqOffset);
  tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey %s, recv poll req vgId:%d, req:%s, reqId:0x%" PRIx64,
          consumerId, req.epoch, pHandle->subKey, vgId, buf, req.reqId);

  code = tqExtractDataForMq(pTq, pHandle, &req, pMsg);
  tqSetHandleIdle(pHandle);

  tqDebug("tmq poll: consumer:0x%" PRIx64 "vgId:%d, topic:%s, , set handle idle, pHandle:%p", consumerId, vgId,
          req.subKey, pHandle);
  return code;
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

  STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, req.subKey);
  if (pOffset != NULL) {
    if (pOffset->val.type != TMQ_OFFSET__LOG) {
      tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s use snapshot, no valid wal info", consumerId, vgId, req.subKey);
      terrno = TSDB_CODE_INVALID_PARA;
      tDeleteMqDataRsp(&dataRsp);
      return -1;
    }

    dataRsp.rspOffset.type = TMQ_OFFSET__LOG;
    dataRsp.rspOffset.version = pOffset->val.version;
  } else {
    if (req.useSnapshot == true) {
      tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s snapshot not support wal info", consumerId, vgId, req.subKey);
      terrno = TSDB_CODE_INVALID_PARA;
      tDeleteMqDataRsp(&dataRsp);
      return -1;
    }

    dataRsp.rspOffset.type = TMQ_OFFSET__LOG;

    if (reqOffset.type == TMQ_OFFSET__LOG) {
      int64_t currentVer = walReaderGetCurrentVer(pHandle->execHandle.pTqReader->pWalReader);
      if (currentVer == -1) {  // not start to read data from wal yet, return req offset directly
        dataRsp.rspOffset.version = reqOffset.version;
      } else {
        dataRsp.rspOffset.version = currentVer;  // return current consume offset value
      }
    } else if (reqOffset.type == TMQ_OFFSET__RESET_EARLIEAST) {
      dataRsp.rspOffset.version = sver;  // not consume yet, set the earliest position
    } else if (reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
      dataRsp.rspOffset.version = ever;
    } else {
      tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s invalid offset type:%d", consumerId, vgId, req.subKey,
              reqOffset.type);
      terrno = TSDB_CODE_INVALID_PARA;
      tDeleteMqDataRsp(&dataRsp);
      return -1;
    }
  }

  tqDoSendDataRsp(&pMsg->info, &dataRsp, req.epoch, req.consumerId, TMQ_MSG_TYPE__WALINFO_RSP, sver, ever);
  return 0;
}

int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;
  int32_t        vgId = TD_VID(pTq->pVnode);

  tqDebug("vgId:%d, tq process delete sub req %s", vgId, pReq->subKey);
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
      goto end;
    }

    STqHandle tqHandle = {0};
    pHandle = &tqHandle;

    memcpy(pHandle->subKey, req.subKey, TSDB_SUBSCRIBE_KEY_LEN);
    pHandle->consumerId = req.newConsumerId;
    pHandle->epoch = -1;

    pHandle->execHandle.subType = req.subType;
    pHandle->fetchMeta = req.withMeta;

    // TODO version should be assigned and refed during preprocess
    SWalRef* pRef = walRefCommittedVer(pVnode->pWal);
    if (pRef == NULL) {
      ret = -1;
      goto end;
    }

    int64_t ver = pRef->refVer;
    pHandle->pRef = pRef;

    SReadHandle handle = {.vnode = pVnode, .initTableReader = true, .initTqReader = true, .version = ver};
    initStorageAPI(&handle.api);

    pHandle->snapshotVer = ver;

    if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      pHandle->execHandle.execCol.qmsg = taosStrdup(req.qmsg);

      pHandle->execHandle.task = qCreateQueueExecTaskInfo(pHandle->execHandle.execCol.qmsg, &handle, vgId,
                                                          &pHandle->execHandle.numOfCols, req.newConsumerId);
      void* scanner = NULL;
      qExtractStreamScanner(pHandle->execHandle.task, &scanner);
      pHandle->execHandle.pTqReader = qExtractReaderFromStreamScanner(scanner);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      pHandle->pWalReader = walOpenReader(pVnode->pWal, NULL);
      pHandle->execHandle.pTqReader = tqReaderOpen(pVnode);

      pHandle->execHandle.execDb.pFilterOutTbUid =
          taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
      buildSnapContext(handle.vnode, handle.version, 0, pHandle->execHandle.subType, pHandle->fetchMeta,
                       (SSnapContext**)(&handle.sContext));

      pHandle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &handle, vgId, NULL, req.newConsumerId);
    } else if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      pHandle->pWalReader = walOpenReader(pVnode->pWal, NULL);
      pHandle->execHandle.execTb.suid = req.suid;
      pHandle->execHandle.execTb.qmsg = taosStrdup(req.qmsg);

      if (strcmp(pHandle->execHandle.execTb.qmsg, "") != 0) {
        if (nodesStringToNode(pHandle->execHandle.execTb.qmsg, &pHandle->execHandle.execTb.node) != 0) {
          tqError("nodesStringToNode error in sub stable, since %s, vgId:%d, subkey:%s consumer:0x%" PRIx64, terrstr(),
                  pVnode->config.vgId, req.subKey, pHandle->consumerId);
          return -1;
        }
      }

      buildSnapContext(handle.vnode, handle.version, req.suid, pHandle->execHandle.subType, pHandle->fetchMeta,
                       (SSnapContext**)(&handle.sContext));
      pHandle->execHandle.task = qCreateQueueExecTaskInfo(NULL, &handle, vgId, NULL, req.newConsumerId);

      SArray* tbUidList = NULL;
      ret = qGetTableList(req.suid, pVnode, pHandle->execHandle.execTb.node, &tbUidList, pHandle->execHandle.task);
      if (ret != TDB_CODE_SUCCESS) {
        tqError("qGetTableList error:%d vgId:%d, subkey:%s consumer:0x%" PRIx64, ret, pVnode->config.vgId, req.subKey,
                pHandle->consumerId);
        taosArrayDestroy(tbUidList);
        goto end;
      }
      tqDebug("tq try to get ctb for stb subscribe, vgId:%d, subkey:%s consumer:0x%" PRIx64 " suid:%" PRId64,
              pVnode->config.vgId, req.subKey, pHandle->consumerId, req.suid);
      pHandle->execHandle.pTqReader = tqReaderOpen(pVnode);
      tqReaderSetTbUidList(pHandle->execHandle.pTqReader, tbUidList, NULL);
      taosArrayDestroy(tbUidList);
    }

    taosHashPut(pTq->pHandle, req.subKey, strlen(req.subKey), pHandle, sizeof(STqHandle));
    tqDebug("try to persist handle %s consumer:0x%" PRIx64, req.subKey, pHandle->consumerId);
    ret = tqMetaSaveHandle(pTq, req.subKey, pHandle);
    goto end;
  } else {
    taosWLockLatch(&pTq->lock);

    if (pHandle->consumerId == req.newConsumerId) {  // do nothing
      tqInfo("vgId:%d consumer:0x%" PRIx64 " remains, no switch occurs, should not reach here", req.vgId,
             req.newConsumerId);
    } else {
      tqInfo("vgId:%d switch consumer from Id:0x%" PRIx64 " to Id:0x%" PRIx64, req.vgId, pHandle->consumerId,
             req.newConsumerId);
      atomic_store_64(&pHandle->consumerId, req.newConsumerId);
    }
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
    taosWUnLockLatch(&pTq->lock);
    ret = tqMetaSaveHandle(pTq, req.subKey, pHandle);
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
  pTask->outputQueue = streamQueueOpen(512 << 10);

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
  pTask->status.taskStatus = (pTask->fillHistory) ? TASK_STATUS__WAIT_DOWNSTREAM : TASK_STATUS__NORMAL;

  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pTask, false, -1, -1);
    if (pTask->pState == NULL) {
      return -1;
    }

    SReadHandle handle = {.vnode = pTq->pVnode, .initTqReader = 1, .pStateBackend = pTask->pState};
    initStorageAPI(&handle.api);

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }

    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  } else if (pTask->taskLevel == TASK_LEVEL__AGG) {
    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pTask, false, -1, -1);
    if (pTask->pState == NULL) {
      return -1;
    }

    int32_t     numOfVgroups = (int32_t)taosArrayGetSize(pTask->childEpInfo);
    SReadHandle handle = {.vnode = NULL, .numOfVgroups = numOfVgroups, .pStateBackend = pTask->pState};
    initStorageAPI(&handle.api);

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }

    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  }

  // sink
  if (pTask->outputType == TASK_OUTPUT__SMA) {
    pTask->smaSink.vnode = pTq->pVnode;
    pTask->smaSink.smaSink = smaHandleRes;
  } else if (pTask->outputType == TASK_OUTPUT__TABLE) {
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

  if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
    SWalFilterCond cond = {.deleteMsg = 1};  // delete msg also extract from wal files
    pTask->exec.pWalReader = walOpenReader(pTq->pVnode->pWal, &cond);
  }

  streamSetupTrigger(pTask);

  tqInfo("vgId:%d expand stream task, s-task:%s, checkpoint ver:%" PRId64 " child id:%d, level:%d", vgId,
         pTask->id.idStr, pTask->chkInfo.version, pTask->selfChildId, pTask->taskLevel);

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

  if (pTask != NULL) {
    rsp.status = streamTaskCheckStatus(pTask);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);

    tqDebug("s-task:%s recv task check req(reqId:0x%" PRIx64
            ") %d at node %d task status:%d, check req from task %d at node %d, rsp status %d",
            pTask->id.idStr, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, pTask->status.taskStatus,
            rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);
  } else {
    rsp.status = 0;
    tqDebug("tq recv task check(taskId:0x%x not built yet) req(reqId:0x%" PRIx64
            ") %d at node %d, check req from task:0x%x at node %d, rsp status %d",
            taskId, rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.upstreamTaskId, rsp.upstreamNodeId,
            rsp.status);
  }

  SEncoder encoder;
  int32_t  code;
  int32_t  len;

  tEncodeSize(tEncodeSStreamTaskCheckRsp, &rsp, len, code);
  if (code < 0) {
    tqError("vgId:%d failed to encode task check rsp, task:0x%x", pTq->pStreamMeta->vgId, taskId);
    return -1;
  }

  void* buf = rpcMallocCont(sizeof(SMsgHead) + len);
  ((SMsgHead*)buf)->vgId = htonl(req.upstreamNodeId);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMsgHead));
  tEncoderInit(&encoder, (uint8_t*)abuf, len);
  tEncodeSStreamTaskCheckRsp(&encoder, &rsp);
  tEncoderClear(&encoder);

  SRpcMsg rspMsg = {.code = 0, .pCont = buf, .contLen = sizeof(SMsgHead) + len, .info = pMsg->info};

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
  tqDebug("tq recv task check rsp(reqId:0x%" PRIx64 ") %d at node %d check req from task:0x%x at node %d, status %d",
          rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, rsp.upstreamTaskId);
  if (pTask == NULL) {
    tqError("tq failed to locate the stream task:0x%x vgId:%d, it may have been destroyed", rsp.upstreamTaskId,
            pTq->pStreamMeta->vgId);
    return -1;
  }

  code = streamProcessTaskCheckRsp(pTask, &rsp, sversion);
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

  // 2.save task, use the newest commit version as the initial start version of stream task.
  taosWLockLatch(&pTq->pStreamMeta->lock);
  code = streamMetaAddDeployedTask(pTq->pStreamMeta, sversion, pTask);
  int32_t numOfTasks = streamMetaGetNumOfTasks(pTq->pStreamMeta);
  if (code < 0) {
    tqError("vgId:%d failed to add s-task:%s, total:%d", vgId, pTask->id.idStr, numOfTasks);
    taosWUnLockLatch(&pTq->pStreamMeta->lock);
    return -1;
  }

  taosWUnLockLatch(&pTq->pStreamMeta->lock);

  // 3.go through recover steps to fill history
  if (pTask->fillHistory) {
    streamTaskCheckDownstream(pTask, sversion);
  }

  tqDebug("vgId:%d s-task:%s is deployed and add meta from mnd, status:%d, total:%d", vgId, pTask->id.idStr,
          pTask->status.taskStatus, numOfTasks);
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
  tqDebug("s-task:%s start non-blocking recover stage(step 1) scan", pTask->id.idStr);
  int64_t st = taosGetTimestampMs();

  streamSourceRecoverScanStep1(pTask);
  if (atomic_load_8(&pTask->status.taskStatus) == TASK_STATUS__DROPPING) {
    tqDebug("s-task:%s is dropped, abort recover in step1", pTask->id.idStr);

    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  tqDebug("s-task:%s non-blocking recover stage(step 1) ended, elapsed time:%.2fs", pTask->id.idStr, el);

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
    tqError("s-task:%s failed to prepare the step2 stage, out of memory", pTask->id.idStr);
    return -1;
  }

  memcpy(serializedReq, &req, len);

  // dispatch msg
  tqDebug("s-task:%s start recover block stage", pTask->id.idStr);

  SRpcMsg rpcMsg = {
      .code = 0, .contLen = len, .msgType = TDMT_VND_STREAM_RECOVER_BLOCKING_STAGE, .pCont = serializedReq};
  tmsgPutToQueue(&pTq->pVnode->msgCb, WRITE_QUEUE, &rpcMsg);
  return 0;
}

int32_t tqProcessTaskRecover2Req(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  int32_t code = 0;

  SStreamRecoverStep2Req* pReq = (SStreamRecoverStep2Req*)msg;

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  if (pTask == NULL) {
    return -1;
  }

  // do recovery step 2
  int64_t st = taosGetTimestampMs();
  tqDebug("s-task:%s start step2 recover, ts:%" PRId64, pTask->id.idStr, st);

  code = streamSourceRecoverScanStep2(pTask, sversion);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  qDebug("s-task:%s set start wal scan start ver:%"PRId64, pTask->id.idStr, sversion);

  walReaderSeekVer(pTask->exec.pWalReader, sversion);
  pTask->chkInfo.currentVer = sversion;

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
  tqDebug("s-task:%s blocking stage completed, set the status to be normal", pTask->id.idStr);
  code = streamSetStatusNormal(pTask);
  if (code < 0) {
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return -1;
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  tqDebug("s-task:%s step2 recover finished, el:%.2fs", pTask->id.idStr, el);

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

int32_t extractDelDataBlock(const void* pData, int32_t len, int64_t ver, SStreamRefDataBlock** pRefBlock) {
  SDecoder*   pCoder = &(SDecoder){0};
  SDeleteRes* pRes = &(SDeleteRes){0};

  *pRefBlock = NULL;

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
  if (pRefBlock == NULL) {
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

  if (taskId == WAL_READ_TASKS_ID) {  // all tasks are extracted submit data from the wal
    tqStreamTasksScanWal(pTq);
    return 0;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask != NULL) {
    if (pTask->status.taskStatus == TASK_STATUS__NORMAL) {
      tqDebug("vgId:%d s-task:%s start to process block from wal, last chk point:%" PRId64, vgId, pTask->id.idStr,
              pTask->chkInfo.version);
      streamProcessRunReq(pTask);
    } else {
      if (streamTaskShouldPause(&pTask->status)) {
        atomic_store_8(&pTask->status.schedStatus, TASK_SCHED_STATUS__INACTIVE);
      }
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

  streamMetaRemoveTask(pTq->pStreamMeta, pReq->taskId);
  return 0;
}

int32_t tqProcessTaskPauseReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SVPauseStreamTaskReq* pReq = (SVPauseStreamTaskReq*)msg;
  SStreamTask*          pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  if (pTask) {
    tqDebug("vgId:%d s-task:%s set pause flag", pTq->pStreamMeta->vgId, pTask->id.idStr);
    atomic_store_8(&pTask->status.keepTaskStatus, pTask->status.taskStatus);
    atomic_store_8(&pTask->status.taskStatus, TASK_STATUS__PAUSE);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  }
  return 0;
}

int32_t tqProcessTaskResumeReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SVResumeStreamTaskReq* pReq = (SVResumeStreamTaskReq*)msg;

  int32_t      vgId = pTq->pStreamMeta->vgId;
  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, pReq->taskId);
  if (pTask) {
    atomic_store_8(&pTask->status.taskStatus, pTask->status.keepTaskStatus);

    // no lock needs to secure the access of the version
    if (pReq->igUntreated && pTask->taskLevel == TASK_LEVEL__SOURCE) {
      // discard all the data  when the stream task is suspended.
      walReaderSetSkipToVersion(pTask->exec.pWalReader, sversion);
      tqDebug("vgId:%d s-task:%s resume to exec, prev paused version:%" PRId64 ", start from vnode ver:%" PRId64
              ", schedStatus:%d",
              vgId, pTask->id.idStr, pTask->chkInfo.currentVer, sversion, pTask->status.schedStatus);
    } else {  // from the previous paused version and go on
      tqDebug("vgId:%d s-task:%s resume to exec, from paused ver:%" PRId64 ", vnode ver:%" PRId64 ", schedStatus:%d",
              vgId, pTask->id.idStr, pTask->chkInfo.currentVer, sversion, pTask->status.schedStatus);
    }

    if (pTask->taskLevel == TASK_LEVEL__SOURCE && taosQueueItemSize(pTask->inputQueue->queue) == 0) {
      tqStartStreamTasks(pTq);
    } else {
      streamSchedExec(pTask);
    }
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
  } else {
    tqError("vgId:%d failed to find the s-task:0x%x for resume stream task", vgId, pReq->taskId);
  }

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
  if (pTask) {
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
  if (pMsg->info.handle == NULL) return -1;

  SMsgHead* pRspHead = rpcMallocCont(sizeof(SMsgHead) + sizeof(SStreamDispatchRsp));
  if (pRspHead == NULL) {
    SRpcMsg rsp = {.code = TSDB_CODE_OUT_OF_MEMORY, .info = pMsg->info};
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
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;

  taosWLockLatch(&pMeta->lock);

  int32_t numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  if (numOfTasks == 0) {
    tqInfo("vgId:%d no stream tasks exist", vgId);
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }

  pMeta->walScanCounter += 1;

  if (pMeta->walScanCounter > 1) {
    tqDebug("vgId:%d wal read task has been launched, remain scan times:%d", vgId, pMeta->walScanCounter);
    taosWUnLockLatch(&pMeta->lock);
    return 0;
  }

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed to create msg to start wal scanning to launch stream tasks, code:%s", vgId, terrstr());
    taosWUnLockLatch(&pMeta->lock);
    return -1;
  }

  tqDebug("vgId:%d create msg to start wal scan to launch stream tasks, numOfTasks:%d", vgId, numOfTasks);
  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = WAL_READ_TASKS_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);
  taosWUnLockLatch(&pMeta->lock);

  return 0;
}
