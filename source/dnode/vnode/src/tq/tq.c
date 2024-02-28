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
#include "tqCommon.h"
#include "vnd.h"

// 0: not init
// 1: already inited
// 2: wait to be inited or cleanup
static int32_t tqInitialize(STQ* pTq);

static FORCE_INLINE bool tqIsHandleExec(STqHandle* pHandle) { return TMQ_HANDLE_STATUS_EXEC == pHandle->status; }
static FORCE_INLINE void tqSetHandleExec(STqHandle* pHandle) { pHandle->status = TMQ_HANDLE_STATUS_EXEC; }
static FORCE_INLINE void tqSetHandleIdle(STqHandle* pHandle) { pHandle->status = TMQ_HANDLE_STATUS_IDLE; }

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
  if (pData->block != NULL) {
    blockDataDestroy(pData->block);
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

  int32_t vgId = TD_VID(pTq->pVnode);
  pTq->pStreamMeta = streamMetaOpen(pTq->path, pTq, (FTaskExpand*)tqExpandTask, vgId, -1, tqStartTaskCompleteCallback);
  if (pTq->pStreamMeta == NULL) {
    return -1;
  }

  if (streamMetaLoadAllTasks(pTq->pStreamMeta) < 0) {
    return -1;
  }

  return 0;
}

void tqClose(STQ* pTq) {
  qDebug("start to close tq");
  if (pTq == NULL) {
    return;
  }

  void* pIter = taosHashIterate(pTq->pPushMgr, NULL);
  while (pIter) {
    STqHandle* pHandle = *(STqHandle**)pIter;
    int32_t    vgId = TD_VID(pTq->pVnode);

    if (pHandle->msg != NULL) {
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

  qDebug("end to close tq");
  taosMemoryFree(pTq);
}

void tqNotifyClose(STQ* pTq) {
  if (pTq == NULL) {
    return;
  }
  streamMetaNotifyClose(pTq->pStreamMeta);
}

int32_t tqPushEmptyDataRsp(STqHandle* pHandle, int32_t vgId) {
  SMqPollReq req = {0};
  if (tDeserializeSMqPollReq(pHandle->msg->pCont, pHandle->msg->contLen, &req) < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pHandle->msg->contLen);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, req.reqOffset);
  dataRsp.blockNum = 0;
  char buf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.reqOffset);
  tqInfo("tqPushEmptyDataRsp to consumer:0x%" PRIx64 " vgId:%d, offset:%s, reqId:0x%" PRIx64, req.consumerId, vgId, buf,
         req.reqId);

  tqSendDataRsp(pHandle, pHandle->msg, &req, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
  tDeleteMqDataRsp(&dataRsp);
  return 0;
}

int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataRsp* pRsp,
                      int32_t type, int32_t vgId) {
  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  tqDoSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type, sver, ever);

  char buf1[TSDB_OFFSET_LEN] = {0};
  char buf2[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(buf1, TSDB_OFFSET_LEN, &pRsp->reqOffset);
  tFormatOffset(buf2, TSDB_OFFSET_LEN, &pRsp->rspOffset);

  tqDebug("tmq poll vgId:%d consumer:0x%" PRIx64 " (epoch %d) send rsp, block num:%d, req:%s, rsp:%s, reqId:0x%" PRIx64,
          vgId, pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2, pReq->reqId);

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
  SMqSeekReq req = {0};
  int32_t    vgId = TD_VID(pTq->pVnode);
  SRpcMsg    rsp = {.info = pMsg->info};
  int        code = 0;

  if (tDeserializeSMqSeekReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto end;
  }

  tqDebug("tmq seek: consumer:0x%" PRIx64 " vgId:%d, subkey %s", req.consumerId, vgId, req.subKey);
  taosWLockLatch(&pTq->lock);

  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    tqWarn("tmq seek: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", req.consumerId, vgId, req.subKey);
    code = 0;
    taosWUnLockLatch(&pTq->lock);
    goto end;
  }

  // 2. check consumer-vg assignment status
  if (pHandle->consumerId != req.consumerId) {
    tqError("ERROR tmq seek: consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            req.consumerId, vgId, req.subKey, pHandle->consumerId);
    taosWUnLockLatch(&pTq->lock);
    code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    goto end;
  }

  // if consumer register to push manager, push empty to consumer to change vg status from TMQ_VG_STATUS__WAIT to
  // TMQ_VG_STATUS__IDLE, otherwise poll data failed after seek.
  tqUnregisterPushHandle(pTq, pHandle);
  taosWUnLockLatch(&pTq->lock);

end:
  rsp.code = code;
  tmsgSendRsp(&rsp);
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

int32_t tqProcessPollPush(STQ* pTq, SRpcMsg* pMsg) {
  int32_t vgId = TD_VID(pTq->pVnode);
  taosWLockLatch(&pTq->lock);
  if (taosHashGetSize(pTq->pPushMgr) > 0) {
    void* pIter = taosHashIterate(pTq->pPushMgr, NULL);

    while (pIter) {
      STqHandle* pHandle = *(STqHandle**)pIter;
      tqDebug("vgId:%d start set submit for pHandle:%p, consumer:0x%" PRIx64, vgId, pHandle, pHandle->consumerId);

      if (ASSERT(pHandle->msg != NULL)) {
        tqError("pHandle->msg should not be null");
        taosHashCancelIterate(pTq->pPushMgr, pIter);
        break;
      } else {
        SRpcMsg msg = {.msgType = TDMT_VND_TMQ_CONSUME,
                       .pCont = pHandle->msg->pCont,
                       .contLen = pHandle->msg->contLen,
                       .info = pHandle->msg->info};
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
      do {
        if (tqMetaGetHandle(pTq, req.subKey) == 0) {
          pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
          if (pHandle != NULL) {
            break;
          }
        }
        tqError("tmq poll: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", consumerId, vgId, req.subKey);
        terrno = TSDB_CODE_INVALID_MSG;
        taosWUnLockLatch(&pTq->lock);
        return -1;
      } while (0);
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
            " vgId:%d, topic:%s, subscription is executing, wait for 10ms and retry, pHandle:%p",
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
  void*   data = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t len = pMsg->contLen - sizeof(SMsgHead);

  SMqVgOffset vgOffset = {0};

  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)data, len);
  if (tDecodeMqVgOffset(&decoder, &vgOffset) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  tDecoderClear(&decoder);

  STqOffset* pOffset = &vgOffset.offset;
  STqOffset* pSavedOffset = tqOffsetRead(pTq->pOffsetStore, pOffset->subKey);
  if (pSavedOffset == NULL) {
    terrno = TSDB_CODE_TMQ_NO_COMMITTED;
    return terrno;
  }
  vgOffset.offset = *pSavedOffset;

  int32_t code = 0;
  tEncodeSize(tEncodeMqVgOffset, &vgOffset, len, code);
  if (code < 0) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }

  void* buf = rpcMallocCont(len);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
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
  taosRLockLatch(&pTq->lock);
  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s not found", consumerId, vgId, req.subKey);
    terrno = TSDB_CODE_INVALID_MSG;
    taosRUnLockLatch(&pTq->lock);
    return -1;
  }

  // 2. check re-balance status
  if (pHandle->consumerId != consumerId) {
    tqDebug("ERROR consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            consumerId, vgId, req.subKey, pHandle->consumerId);
    terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
    taosRUnLockLatch(&pTq->lock);
    return -1;
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  taosRUnLockLatch(&pTq->lock);

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, req.reqOffset);

  if (req.useSnapshot == true) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s snapshot not support wal info", consumerId, vgId, req.subKey);
    terrno = TSDB_CODE_INVALID_PARA;
    tDeleteMqDataRsp(&dataRsp);
    return -1;
  }

  dataRsp.rspOffset.type = TMQ_OFFSET__LOG;

  if (reqOffset.type == TMQ_OFFSET__LOG) {
    dataRsp.rspOffset.version = reqOffset.version;
  } else if (reqOffset.type < 0) {
    STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, req.subKey);
    if (pOffset != NULL) {
      if (pOffset->val.type != TMQ_OFFSET__LOG) {
        tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s, no valid wal info", consumerId, vgId, req.subKey);
        terrno = TSDB_CODE_INVALID_PARA;
        tDeleteMqDataRsp(&dataRsp);
        return -1;
      }

      dataRsp.rspOffset.version = pOffset->val.version;
      tqInfo("consumer:0x%" PRIx64 " vgId:%d subkey:%s get assignment from store:%" PRId64, consumerId, vgId,
             req.subKey, dataRsp.rspOffset.version);
    } else {
      if (reqOffset.type == TMQ_OFFSET__RESET_EARLIEST) {
        dataRsp.rspOffset.version = sver;  // not consume yet, set the earliest position
      } else if (reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
        dataRsp.rspOffset.version = ever;
      }
      tqInfo("consumer:0x%" PRIx64 " vgId:%d subkey:%s get assignment from init:%" PRId64, consumerId, vgId, req.subKey,
             dataRsp.rspOffset.version);
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

  STqHandle* pHandle = taosHashGet(pTq->pHandle, pReq->subKey, strlen(pReq->subKey));
  if (pHandle) {
    while (1) {
      taosWLockLatch(&pTq->lock);
      bool exec = tqIsHandleExec(pHandle);

      if (exec) {
        tqInfo("vgId:%d, topic:%s, subscription is executing, delete wait for 10ms and retry, pHandle:%p", vgId,
               pHandle->subKey, pHandle);
        taosWUnLockLatch(&pTq->lock);
        taosMsleep(10);
        continue;
      }
      if (pHandle->pRef) {
        walCloseRef(pTq->pVnode->pWal, pHandle->pRef->refId);
      }

      tqUnregisterPushHandle(pTq, pHandle);

      code = taosHashRemove(pTq->pHandle, pReq->subKey, strlen(pReq->subKey));
      if (code != 0) {
        tqError("cannot process tq delete req %s, since no such handle", pReq->subKey);
      }
      taosWUnLockLatch(&pTq->lock);
      break;
    }
  }

  taosWLockLatch(&pTq->lock);
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

  tDecoderInit(&dc, (uint8_t*)msg, msgLen);

  // decode req
  if (tDecodeSMqRebVgReq(&dc, &req) < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    tDecoderClear(&dc);
    return -1;
  }

  tqInfo("vgId:%d, tq process sub req:%s, Id:0x%" PRIx64 " -> Id:0x%" PRIx64, pTq->pVnode->config.vgId, req.subKey,
         req.oldConsumerId, req.newConsumerId);

  STqHandle* pHandle = NULL;
  while (1) {
    pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
    if (pHandle) {
      break;
    }
    taosRLockLatch(&pTq->lock);
    ret = tqMetaGetHandle(pTq, req.subKey);
    taosRUnLockLatch(&pTq->lock);

    if (ret < 0) {
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
    if (ret < 0) {
      tqDestroyTqHandle(&handle);
      goto end;
    }
    taosWLockLatch(&pTq->lock);
    ret = tqMetaSaveHandle(pTq, req.subKey, &handle);
    taosWUnLockLatch(&pTq->lock);
  } else {
    while (1) {
      taosWLockLatch(&pTq->lock);
      bool exec = tqIsHandleExec(pHandle);
      if (exec) {
        tqInfo("vgId:%d, topic:%s, subscription is executing, sub wait for 10ms and retry, pHandle:%p",
               pTq->pVnode->config.vgId, pHandle->subKey, pHandle);
        taosWUnLockLatch(&pTq->lock);
        taosMsleep(10);
        continue;
      }
      if (pHandle->consumerId == req.newConsumerId) {  // do nothing
        tqInfo("vgId:%d no switch consumer:0x%" PRIx64 " remains, because redo wal log", req.vgId, req.newConsumerId);
      } else {
        tqInfo("vgId:%d switch consumer from Id:0x%" PRIx64 " to Id:0x%" PRIx64, req.vgId, pHandle->consumerId,
               req.newConsumerId);
        atomic_store_64(&pHandle->consumerId, req.newConsumerId);
        atomic_store_32(&pHandle->epoch, 0);
        tqUnregisterPushHandle(pTq, pHandle);
        ret = tqMetaSaveHandle(pTq, req.subKey, pHandle);
      }
      taosWUnLockLatch(&pTq->lock);
      break;
    }
  }

end:
  tDecoderClear(&dc);
  return ret;
}

static void freePtr(void* ptr) { taosMemoryFree(*(void**)ptr); }

static STaskId replaceStreamTaskId(SStreamTask* pTask) {
  ASSERT(pTask->info.fillHistory);
  STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};

  pTask->id.streamId = pTask->streamTaskId.streamId;
  pTask->id.taskId = pTask->streamTaskId.taskId;

  return id;
}

static void restoreStreamTaskId(SStreamTask* pTask, STaskId* pId) {
  ASSERT(pTask->info.fillHistory);
  pTask->id.taskId = pId->taskId;
  pTask->id.streamId = pId->streamId;
}

int32_t tqExpandTask(STQ* pTq, SStreamTask* pTask, int64_t nextProcessVer) {
  int32_t vgId = TD_VID(pTq->pVnode);
  tqDebug("s-task:0x%x start to expand task", pTask->id.taskId);

  int32_t code = streamTaskInit(pTask, pTq->pStreamMeta, &pTq->pVnode->msgCb, nextProcessVer);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    STaskId taskId = {0};
    if (pTask->info.fillHistory) {
      taskId = replaceStreamTaskId(pTask);
    }

    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pTask, false, -1, -1);
    if (pTask->pState == NULL) {
      tqError("s-task:%s (vgId:%d) failed to open state for task", pTask->id.idStr, vgId);
      return -1;
    }

    tqDebug("s-task:%s state:%p", pTask->id.idStr, pTask->pState);
    if (pTask->info.fillHistory) {
      restoreStreamTaskId(pTask, &taskId);
    }

    SReadHandle handle = {
        .checkpointId = pTask->chkInfo.checkpointId,
        .vnode = pTq->pVnode,
        .initTqReader = 1,
        .pStateBackend = pTask->pState,
        .fillHistory = pTask->info.fillHistory,
        .winRange = pTask->dataRange.window,
    };

    initStorageAPI(&handle.api);

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId, pTask->id.taskId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }

    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    STaskId taskId = {0};
    if (pTask->info.fillHistory) {
      taskId = replaceStreamTaskId(pTask);
    }

    pTask->pState = streamStateOpen(pTq->pStreamMeta->path, pTask, false, -1, -1);
    if (pTask->pState == NULL) {
      tqError("s-task:%s (vgId:%d) failed to open state for task", pTask->id.idStr, vgId);
      return -1;
    } else {
      tqDebug("s-task:%s state:%p", pTask->id.idStr, pTask->pState);
    }

    if (pTask->info.fillHistory) {
      restoreStreamTaskId(pTask, &taskId);
    }

    SReadHandle handle = {
        .checkpointId = pTask->chkInfo.checkpointId,
        .vnode = NULL,
        .numOfVgroups = (int32_t)taosArrayGetSize(pTask->upstreamInfo.pList),
        .pStateBackend = pTask->pState,
        .fillHistory = pTask->info.fillHistory,
        .winRange = pTask->dataRange.window,
    };

    initStorageAPI(&handle.api);

    pTask->exec.pExecutor = qCreateStreamExecTaskInfo(pTask->exec.qmsg, &handle, vgId, pTask->id.taskId);
    if (pTask->exec.pExecutor == NULL) {
      return -1;
    }
    qSetTaskId(pTask->exec.pExecutor, pTask->id.taskId, pTask->id.streamId);
  }

  // sink
  STaskOutputInfo* pOutputInfo = &pTask->outputInfo;
  if (pOutputInfo->type == TASK_OUTPUT__SMA) {
    pOutputInfo->smaSink.vnode = pTq->pVnode;
    pOutputInfo->smaSink.smaSink = smaHandleRes;
  } else if (pOutputInfo->type == TASK_OUTPUT__TABLE) {
    pOutputInfo->tbSink.vnode = pTq->pVnode;
    pOutputInfo->tbSink.tbSinkFunc = tqSinkDataIntoDstTable;

    int32_t   ver1 = 1;
    SMetaInfo info = {0};
    code = metaGetInfo(pTq->pVnode->pMeta, pOutputInfo->tbSink.stbUid, &info, NULL);
    if (code == TSDB_CODE_SUCCESS) {
      ver1 = info.skmVer;
    }

    SSchemaWrapper* pschemaWrapper = pOutputInfo->tbSink.pSchemaWrapper;
    pOutputInfo->tbSink.pTSchema = tBuildTSchema(pschemaWrapper->pSchema, pschemaWrapper->nCols, ver1);
    if (pOutputInfo->tbSink.pTSchema == NULL) {
      return -1;
    }

    pOutputInfo->tbSink.pTblInfo = tSimpleHashInit(10240, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    tSimpleHashSetFreeFp(pOutputInfo->tbSink.pTblInfo, freePtr);
  }

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    SWalFilterCond cond = {.deleteMsg = 1};  // delete msg also extract from wal files
    pTask->exec.pWalReader = walOpenReader(pTq->pVnode->pWal, &cond, pTask->id.taskId);
  }

  streamTaskResetUpstreamStageInfo(pTask);
  streamSetupScheduleTrigger(pTask);
  SCheckpointInfo* pChkInfo = &pTask->chkInfo;

  // checkpoint ver is the kept version, handled data should be the next version.
  if (pChkInfo->checkpointId != 0) {
    pChkInfo->nextProcessVer = pChkInfo->checkpointVer + 1;
    pChkInfo->processedVer = pChkInfo->checkpointVer;
    tqInfo("s-task:%s restore from the checkpointId:%" PRId64 " ver:%" PRId64 " currentVer:%" PRId64, pTask->id.idStr,
           pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer);
  }

  char* p = streamTaskGetStatus(pTask)->name;
  const char* pNext = streamTaskGetStatusStr(pTask->status.taskStatus);

  if (pTask->info.fillHistory) {
    tqInfo("vgId:%d expand stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
           " nextProcessVer:%" PRId64
           " child id:%d, level:%d, cur-status:%s, next-status:%s fill-history:%d, related stream task:0x%x "
           "trigger:%" PRId64 " ms, inputVer:%" PRId64,
           vgId, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
           pTask->info.selfChildId, pTask->info.taskLevel, p, pNext, pTask->info.fillHistory,
           (int32_t)pTask->streamTaskId.taskId, pTask->info.triggerParam, nextProcessVer);
  } else {
    tqInfo(
        "vgId:%d expand stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
        " nextProcessVer:%" PRId64
        " child id:%d, level:%d, cur-status:%s next-status:%s fill-history:%d, related fill-task:0x%x trigger:%" PRId64
        " ms, inputVer:%" PRId64,
        vgId, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
        pTask->info.selfChildId, pTask->info.taskLevel, p, pNext, pTask->info.fillHistory,
        (int32_t)pTask->hTaskInfo.id.taskId, pTask->info.triggerParam, nextProcessVer);
  }

  return 0;
}

int32_t tqProcessTaskCheckReq(STQ* pTq, SRpcMsg* pMsg) { return tqStreamTaskProcessCheckReq(pTq->pStreamMeta, pMsg); }

int32_t tqProcessTaskCheckRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessCheckRsp(pTq->pStreamMeta, pMsg, vnodeIsRoleLeader(pTq->pVnode));
}

int32_t tqProcessTaskDeployReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  return tqStreamTaskProcessDeployReq(pTq->pStreamMeta, &pTq->pVnode->msgCb, sversion, msg, msgLen,
                                      vnodeIsRoleLeader(pTq->pVnode), pTq->pVnode->restored);
}

static void doStartFillhistoryStep2(SStreamTask* pTask, SStreamTask* pStreamTask, STQ* pTq) {
  const char*    id = pTask->id.idStr;
  int64_t        nextProcessedVer = pStreamTask->hTaskInfo.haltVer;
  SVersionRange* pRange = &pTask->dataRange.range;

  // if it's an source task, extract the last version in wal.
  bool done = streamHistoryTaskSetVerRangeStep2(pTask, nextProcessedVer);
  pTask->execInfo.step2Start = taosGetTimestampMs();

  if (done) {
    qDebug("s-task:%s scan wal(step 2) verRange:%" PRId64 "-%" PRId64 " ended, elapsed time:%.2fs", id, pRange->minVer,
           pRange->maxVer, 0.0);
    streamTaskPutTranstateIntoInputQ(pTask);
    streamExecTask(pTask);  // exec directly
  } else {
    STimeWindow* pWindow = &pTask->dataRange.window;
    tqDebug("s-task:%s level:%d verRange:%" PRId64 " - %" PRId64 " window:%" PRId64 "-%" PRId64
            ", do secondary scan-history from WAL after halt the related stream task:%s",
            id, pTask->info.taskLevel, pRange->minVer, pRange->maxVer, pWindow->skey, pWindow->ekey,
            pStreamTask->id.idStr);
    ASSERT(pTask->status.schedStatus == TASK_SCHED_STATUS__WAITING);

    streamSetParamForStreamScannerStep2(pTask, pRange, pWindow);

    int64_t dstVer = pTask->dataRange.range.minVer;
    pTask->chkInfo.nextProcessVer = dstVer;

    walReaderSetSkipToVersion(pTask->exec.pWalReader, dstVer);
    tqDebug("s-task:%s wal reader start scan WAL verRange:%" PRId64 "-%" PRId64 ", set sched-status:%d", id, dstVer,
            pTask->dataRange.range.maxVer, TASK_SCHED_STATUS__INACTIVE);

    /*int8_t status = */ streamTaskSetSchedStatusInactive(pTask);

    // now the fill-history task starts to scan data from wal files.
    int32_t code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_SCANHIST_DONE);
    if (code == TSDB_CODE_SUCCESS) {
      tqScanWalAsync(pTq, false);
    }
  }
}

// this function should be executed by only one thread, so we set an sentinel to protect this function
int32_t tqProcessTaskScanHistory(STQ* pTq, SRpcMsg* pMsg) {
  SStreamScanHistoryReq* pReq = (SStreamScanHistoryReq*)pMsg->pCont;
  SStreamMeta*           pMeta = pTq->pStreamMeta;
  int32_t                code = TSDB_CODE_SUCCESS;

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId);
  if (pTask == NULL) {
    tqError("vgId:%d failed to acquire stream task:0x%x during scan history data, task may have been destroyed",
            pMeta->vgId, pReq->taskId);
    return -1;
  }

  // do recovery step1
  const char* id = pTask->id.idStr;
  char*       pStatus = streamTaskGetStatus(pTask)->name;

  // avoid multi-thread exec
  while (1) {
    int32_t sentinel = atomic_val_compare_exchange_32(&pTask->status.inScanHistorySentinel, 0, 1);
    if (sentinel != 0) {
      tqDebug("s-task:%s already in scan-history func, wait for 100ms, and try again", id);
      taosMsleep(100);
    } else {
      break;
    }
  }

  // let's decide which step should be executed now
  if (pTask->execInfo.step1Start == 0) {
    int64_t ts = taosGetTimestampMs();
    pTask->execInfo.step1Start = ts;
    tqDebug("s-task:%s start scan-history stage(step 1), status:%s, step1 startTs:%" PRId64, id, pStatus, ts);
  } else {
    if (pTask->execInfo.step2Start == 0) {
      tqDebug("s-task:%s continue exec scan-history(step1), original step1 startTs:%" PRId64 ", already elapsed:%.2fs",
              id, pTask->execInfo.step1Start, pTask->execInfo.step1El);
    } else {
      tqDebug("s-task:%s already in step2, no need to scan-history data, step2 startTs:%" PRId64, id,
              pTask->execInfo.step2Start);

      atomic_store_32(&pTask->status.inScanHistorySentinel, 0);
      streamMetaReleaseTask(pMeta, pTask);
      return 0;
    }
  }

  // we have to continue retrying to successfully execute the scan history task.
  if (!streamTaskSetSchedStatusWait(pTask)) {
    tqError(
        "s-task:%s failed to start scan-history in first stream time window since already started, unexpected "
        "sched-status:%d",
        id, pTask->status.schedStatus);
    atomic_store_32(&pTask->status.inScanHistorySentinel, 0);
    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  }

  int64_t              st = taosGetTimestampMs();
  SScanhistoryDataInfo retInfo = streamScanHistoryData(pTask, st);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  pTask->execInfo.step1El += el;

  if (retInfo.ret == TASK_SCANHISTORY_QUIT || retInfo.ret == TASK_SCANHISTORY_REXEC) {
    int8_t status = streamTaskSetSchedStatusInactive(pTask);
    atomic_store_32(&pTask->status.inScanHistorySentinel, 0);

    if (retInfo.ret == TASK_SCANHISTORY_REXEC) {
      streamReExecScanHistoryFuture(pTask, retInfo.idleTime);
    } else {
      SStreamTaskState* p = streamTaskGetStatus(pTask);
      ETaskStatus       s = p->state;

      if (s == TASK_STATUS__PAUSE) {
        tqDebug("s-task:%s is paused in the step1, elapsed time:%.2fs total:%.2fs, sched-status:%d", pTask->id.idStr,
                el, pTask->execInfo.step1El, status);
      } else if (s == TASK_STATUS__STOP || s == TASK_STATUS__DROPPING) {
        tqDebug("s-task:%s status:%p not continue scan-history data, total elapsed time:%.2fs quit", pTask->id.idStr,
                p->name, pTask->execInfo.step1El);
      }
    }

    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  }

  // the following procedure should be executed, no matter status is stop/pause or not
  tqDebug("s-task:%s scan-history(step 1) ended, elapsed time:%.2fs", id, pTask->execInfo.step1El);

  if (pTask->info.fillHistory) {
    SStreamTask* pStreamTask = NULL;

    // 1. get the related stream task
    pStreamTask = streamMetaAcquireTask(pMeta, pTask->streamTaskId.streamId, pTask->streamTaskId.taskId);
    if (pStreamTask == NULL) {
      tqError("failed to find s-task:0x%" PRIx64 ", it may have been destroyed, drop related fill-history task:%s",
              pTask->streamTaskId.taskId, pTask->id.idStr);

      tqDebug("s-task:%s fill-history task set status to be dropping and drop it", id);
      streamBuildAndSendDropTaskMsg(pTask->pMsgCb, pMeta->vgId, &pTask->id, 0);

      atomic_store_32(&pTask->status.inScanHistorySentinel, 0);
      streamMetaReleaseTask(pMeta, pTask);
      return -1;
    }

    ASSERT(pStreamTask->info.taskLevel == TASK_LEVEL__SOURCE);

    code = streamTaskHandleEvent(pStreamTask->status.pSM, TASK_EVENT_HALT);
    if (code == TSDB_CODE_SUCCESS) {
      doStartFillhistoryStep2(pTask, pStreamTask, pTq);
    } else {
      tqError("s-task:%s failed to halt s-task:%s, not launch step2", id, pStreamTask->id.idStr);
    }

    streamMetaReleaseTask(pMeta, pStreamTask);
  } else {
    ASSERT(0);
  }

  atomic_store_32(&pTask->status.inScanHistorySentinel, 0);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  // extracted submit data from wal files for all tasks
  if (pReq->reqType == STREAM_EXEC_T_EXTRACT_WAL_DATA) {
    tqScanWal(pTq);
    return 0;
  }

  int32_t code = tqStreamTaskProcessRunReq(pTq->pStreamMeta, pMsg, vnodeIsRoleLeader(pTq->pVnode));

  // let's continue scan data in the wal files
  if (code == 0 && (pReq->reqType >= 0 || pReq->reqType == STREAM_EXEC_T_RESUME_TASK)) {
    tqScanWalAsync(pTq, false);
  }

  return code;
}

int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessDispatchReq(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessDispatchRsp(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskDropReq(STQ* pTq, char* msg, int32_t msgLen) {
  return tqStreamTaskProcessDropReq(pTq->pStreamMeta, msg, msgLen);
}

int32_t tqProcessTaskPauseReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  return tqStreamTaskProcessTaskPauseReq(pTq->pStreamMeta, msg);
}

int32_t tqProcessTaskResumeReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  return tqStreamTaskProcessTaskResumeReq(pTq, sversion, msg, true);
}

int32_t tqProcessTaskRetrieveReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessRetrieveReq(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskRetrieveRsp(STQ* pTq, SRpcMsg* pMsg) {
  //
  return 0;
}

int32_t tqProcessTaskCheckPointSourceReq(STQ* pTq, SRpcMsg* pMsg, SRpcMsg* pRsp) {
  int32_t      vgId = TD_VID(pTq->pVnode);
  SStreamMeta* pMeta = pTq->pStreamMeta;
  char*        msg = POINTER_SHIFT(pMsg->pCont, sizeof(SMsgHead));
  int32_t      len = pMsg->contLen - sizeof(SMsgHead);
  int32_t      code = 0;

  // disable auto rsp to mnode
  pRsp->info.handle = NULL;

  SStreamCheckpointSourceReq req = {0};
  SDecoder                   decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, len);
  if (tDecodeStreamCheckpointSourceReq(&decoder, &req) < 0) {
    code = TSDB_CODE_MSG_DECODE_ERROR;
    tDecoderClear(&decoder);
    tqError("vgId:%d failed to decode checkpoint-source msg, code:%s", vgId, tstrerror(code));
    SRpcMsg rsp = {0};
    buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
    tmsgSendRsp(&rsp);  // error occurs
    return code;
  }
  tDecoderClear(&decoder);

  if (!vnodeIsRoleLeader(pTq->pVnode)) {
    tqDebug("vgId:%d not leader, ignore checkpoint-source msg, s-task:0x%x", vgId, req.taskId);
    SRpcMsg rsp = {0};
    buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
    tmsgSendRsp(&rsp);  // error occurs
    return TSDB_CODE_SUCCESS;
  }

  if (!pTq->pVnode->restored) {
    tqDebug("vgId:%d checkpoint-source msg received during restoring, s-task:0x%x ignore it", vgId, req.taskId);
    SRpcMsg rsp = {0};
    buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
    tmsgSendRsp(&rsp);  // error occurs
    return TSDB_CODE_SUCCESS;
  }

  SStreamTask* pTask = streamMetaAcquireTask(pMeta, req.streamId, req.taskId);
  if (pTask == NULL) {
    tqError("vgId:%d failed to find s-task:0x%x, ignore checkpoint msg. it may have been destroyed", vgId, req.taskId);
    SRpcMsg rsp = {0};
    buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
    tmsgSendRsp(&rsp);  // error occurs
    return TSDB_CODE_SUCCESS;
  }

  if (pTask->status.downstreamReady != 1) {
    pTask->chkInfo.failedId = req.checkpointId;  // record the latest failed checkpoint id
    pTask->chkInfo.checkpointingId = req.checkpointId;
    pTask->chkInfo.transId = req.transId;

    tqError("s-task:%s not ready for checkpoint, since downstream not ready, ignore this checkpoint:%" PRId64
            ", transId:%d set it failed",
            pTask->id.idStr, req.checkpointId, req.transId);
    streamMetaReleaseTask(pMeta, pTask);

    SRpcMsg rsp = {0};
    buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
    tmsgSendRsp(&rsp);  // error occurs
    return TSDB_CODE_SUCCESS;
  }

  // todo save the checkpoint failed info
  taosThreadMutexLock(&pTask->lock);
  ETaskStatus status = streamTaskGetStatus(pTask)->state;

  if (req.mndTrigger == 1) {
    if (status == TASK_STATUS__HALT || status == TASK_STATUS__PAUSE) {
      tqError("s-task:%s not ready for checkpoint, since it is halt, ignore checkpoint:%" PRId64 ", set it failure",
              pTask->id.idStr, req.checkpointId);

      taosThreadMutexUnlock(&pTask->lock);
      streamMetaReleaseTask(pMeta, pTask);

      SRpcMsg rsp = {0};
      buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
      tmsgSendRsp(&rsp);  // error occurs

      return TSDB_CODE_SUCCESS;
    }
  } else {
//    ASSERT(status == TASK_STATUS__HALT);
    if (status != TASK_STATUS__HALT) {
      tqError("s-task:%s should in halt status, let's halt it directly", pTask->id.idStr);
//      streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_HALT);
    }
  }

  // check if the checkpoint msg already sent or not.
  if (status == TASK_STATUS__CK) {
    tqWarn("s-task:%s recv checkpoint-source msg again checkpointId:%" PRId64
           " transId:%d already received, ignore this msg and continue process checkpoint",
           pTask->id.idStr, pTask->chkInfo.checkpointingId, req.transId);

    taosThreadMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pMeta, pTask);

    return TSDB_CODE_SUCCESS;
  }

  streamProcessCheckpointSourceReq(pTask, &req);
  taosThreadMutexUnlock(&pTask->lock);

  qInfo("s-task:%s (vgId:%d) level:%d receive checkpoint-source msg chkpt:%" PRId64 ", transId:%d", pTask->id.idStr,
        vgId, pTask->info.taskLevel, req.checkpointId, req.transId);

  code = streamAddCheckpointSourceRspMsg(&req, &pMsg->info, pTask, 1);
  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {0};
    buildCheckpointSourceRsp(&req, &pMsg->info, &rsp, 0);
    tmsgSendRsp(&rsp);  // error occurs
    return code;
  }

  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

// downstream task has complete the stream task checkpoint procedure, let's start the handle the rsp by execute task
int32_t tqProcessTaskCheckpointReadyMsg(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessCheckpointReadyMsg(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskUpdateReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessUpdateReq(pTq->pStreamMeta, &pTq->pVnode->msgCb, pMsg, pTq->pVnode->restored);
}

int32_t tqProcessTaskResetReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessTaskResetReq(pTq->pStreamMeta, pMsg);
}

// this function is needed, do not try to remove it.
int32_t tqProcessStreamHbRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamProcessStreamHbRsp(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessStreamReqCheckpointRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamProcessReqCheckpointRsp(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskCheckpointReadyRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamProcessCheckpointReadyRsp(pTq->pStreamMeta, pMsg);
}
