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
#include "osDef.h"
#include "taoserror.h"
#include "tqCommon.h"
#include "tstream.h"
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
  if (pData->pRef) {
    walCloseRef(pData->pRef->pWal, pData->pRef->refId);
  }
}

static bool tqOffsetEqual(const STqOffset* pLeft, const STqOffset* pRight) {
  return pLeft->val.type == TMQ_OFFSET__LOG && pRight->val.type == TMQ_OFFSET__LOG &&
         pLeft->val.version == pRight->val.version;
}

int32_t tqOpen(const char* path, SVnode* pVnode) {
  STQ* pTq = taosMemoryCalloc(1, sizeof(STQ));
  if (pTq == NULL) {
    return terrno;
  }
  pVnode->pTq = pTq;
  pTq->path = taosStrdup(path);
  pTq->pVnode = pVnode;

  pTq->pHandle = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (pTq->pHandle == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(pTq->pHandle, tqDestroyTqHandle);

  taosInitRWLatch(&pTq->lock);

  pTq->pPushMgr = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pTq->pPushMgr == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pTq->pCheckInfo = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pTq->pCheckInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(pTq->pCheckInfo, (FDelete)tDeleteSTqCheckInfo);

  pTq->pOffset = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  if (pTq->pOffset == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(pTq->pOffset, (FDelete)tDeleteSTqOffset);

  return tqInitialize(pTq);
}

int32_t tqInitialize(STQ* pTq) {
  int32_t vgId = TD_VID(pTq->pVnode);
  int32_t code = streamMetaOpen(pTq->path, pTq, tqBuildStreamTask, tqExpandStreamTask, vgId, -1,
                                tqStartTaskCompleteCallback, &pTq->pStreamMeta);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  streamMetaLoadAllTasks(pTq->pStreamMeta);
  return tqMetaOpen(pTq);
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

  taosHashCleanup(pTq->pHandle);
  taosHashCleanup(pTq->pPushMgr);
  taosHashCleanup(pTq->pCheckInfo);
  taosHashCleanup(pTq->pOffset);
  taosMemoryFree(pTq->path);
  tqMetaClose(pTq);

  int32_t vgId = pTq->pStreamMeta->vgId;
  streamMetaClose(pTq->pStreamMeta);

  qDebug("vgId:%d end to close tq", vgId);
  taosMemoryFree(pTq);
}

void tqNotifyClose(STQ* pTq) {
  if (pTq == NULL) {
    return;
  }
  streamMetaNotifyClose(pTq->pStreamMeta);
}

void tqPushEmptyDataRsp(STqHandle* pHandle, int32_t vgId) {
  int32_t    code = 0;
  SMqPollReq req = {0};
  code = tDeserializeSMqPollReq(pHandle->msg->pCont, pHandle->msg->contLen, &req);
  if (code < 0) {
    tqError("tDeserializeSMqPollReq %d failed, code:%d", pHandle->msg->contLen, code);
    return;
  }

  SMqDataRsp dataRsp = {0};
  code = tqInitDataRsp(&dataRsp.common, req.reqOffset);
  if (code != 0) {
    tqError("tqInitDataRsp failed, code:%d", code);
    return;
  }
  dataRsp.common.blockNum = 0;
  char buf[TSDB_OFFSET_LEN] = {0};
  (void)tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.common.reqOffset);
  tqInfo("tqPushEmptyDataRsp to consumer:0x%" PRIx64 " vgId:%d, offset:%s,QID:0x%" PRIx64, req.consumerId, vgId, buf,
         req.reqId);

  code = tqSendDataRsp(pHandle, pHandle->msg, &req, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
  if (code != 0) {
    tqError("tqSendDataRsp failed, code:%d", code);
  }
  tDeleteMqDataRsp(&dataRsp);
}

int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const void* pRsp, int32_t type,
                      int32_t vgId) {
  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  char buf1[TSDB_OFFSET_LEN] = {0};
  char buf2[TSDB_OFFSET_LEN] = {0};
  (void)tFormatOffset(buf1, TSDB_OFFSET_LEN, &((SMqDataRspCommon*)pRsp)->reqOffset);
  (void)tFormatOffset(buf2, TSDB_OFFSET_LEN, &((SMqDataRspCommon*)pRsp)->rspOffset);

  tqDebug("tmq poll vgId:%d consumer:0x%" PRIx64 " (epoch %d) send rsp, block num:%d, req:%s, rsp:%s,QID:0x%" PRIx64,
          vgId, pReq->consumerId, pReq->epoch, ((SMqDataRspCommon*)pRsp)->blockNum, buf1, buf2, pReq->reqId);

  return tqDoSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type, sver, ever);
}

int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVgOffset vgOffset = {0};
  int32_t     vgId = TD_VID(pTq->pVnode);

  int32_t  code = 0;
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)msg, msgLen);
  if (tDecodeMqVgOffset(&decoder, &vgOffset) < 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto end;
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
    code = TSDB_CODE_INVALID_MSG;
    goto end;
  }

  STqOffset* pSavedOffset = NULL;
  code = tqMetaGetOffset(pTq, pOffset->subKey, &pSavedOffset);
  if (code == 0 && tqOffsetEqual(pOffset, pSavedOffset)) {
    tqInfo("not update the offset, vgId:%d sub:%s since committed:%" PRId64 " less than/equal to existed:%" PRId64,
           vgId, pOffset->subKey, pOffset->val.version, pSavedOffset->val.version);
    goto end;  // no need to update the offset value
  }

  // save the new offset value
  if (taosHashPut(pTq->pOffset, pOffset->subKey, strlen(pOffset->subKey), pOffset, sizeof(STqOffset))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (tqMetaSaveInfo(pTq, pTq->pOffsetStore, pOffset->subKey, strlen(pOffset->subKey), msg,
                     msgLen - sizeof(vgOffset.consumerId)) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
end:
  tOffsetDestroy(&vgOffset.offset.val);
  return code;
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
        int16_t* pForbidColId = taosArrayGet(pCheck->colIdList, i);
        if (pForbidColId == NULL) {
          continue;
        }

        if ((*pForbidColId) == colId) {
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

      if (pHandle->msg == NULL) {
        tqError("pHandle->msg should not be null");
        taosHashCancelIterate(pTq->pPushMgr, pIter);
        break;
      } else {
        SRpcMsg msg = {.msgType = TDMT_VND_TMQ_CONSUME,
                       .pCont = pHandle->msg->pCont,
                       .contLen = pHandle->msg->contLen,
                       .info = pHandle->msg->info};
        if (tmsgPutToQueue(&pTq->pVnode->msgCb, QUERY_QUEUE, &msg) != 0){
          tqError("vgId:%d tmsgPutToQueue failed, consumer:0x%" PRIx64, vgId, pHandle->consumerId);
        }
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
  int        code = tDeserializeSMqPollReq(pMsg->pCont, pMsg->contLen, &req);
  if (code < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pMsg->contLen);
    terrno = TSDB_CODE_INVALID_MSG;
    goto END;
  }

  int64_t      consumerId = req.consumerId;
  int32_t      reqEpoch = req.epoch;
  STqOffsetVal reqOffset = req.reqOffset;
  int32_t      vgId = TD_VID(pTq->pVnode);
  STqHandle*   pHandle = NULL;

  while (1) {
    taosWLockLatch(&pTq->lock);
    // 1. find handle
    code = tqMetaGetHandle(pTq, req.subKey, &pHandle);
    if (code != TDB_CODE_SUCCESS) {
      tqError("tmq poll: consumer:0x%" PRIx64 " vgId:%d subkey %s not found", consumerId, vgId, req.subKey);
      terrno = TSDB_CODE_INVALID_MSG;
      taosWUnLockLatch(&pTq->lock);
      return -1;
    }

    // 2. check rebalance status
    if (pHandle->consumerId != consumerId) {
      tqError("ERROR tmq poll: consumer:0x%" PRIx64
              " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
              consumerId, TD_VID(pTq->pVnode), req.subKey, pHandle->consumerId);
      terrno = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
      taosWUnLockLatch(&pTq->lock);
      code = -1;
      goto END;
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
  (void)tFormatOffset(buf, TSDB_OFFSET_LEN, &reqOffset);
  tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey %s, recv poll req vgId:%d, req:%s,QID:0x%" PRIx64,
          consumerId, req.epoch, pHandle->subKey, vgId, buf, req.reqId);

  code = tqExtractDataForMq(pTq, pHandle, &req, pMsg);
  tqSetHandleIdle(pHandle);

  tqDebug("tmq poll: consumer:0x%" PRIx64 " vgId:%d, topic:%s, set handle idle, pHandle:%p", consumerId, vgId,
          req.subKey, pHandle);

END:
  tDestroySMqPollReq(&req);
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

  STqOffset* pSavedOffset = NULL;
  int32_t    code = tqMetaGetOffset(pTq, vgOffset.offset.subKey, &pSavedOffset);
  if (code != 0) {
    return TSDB_CODE_TMQ_NO_COMMITTED;
  }
  vgOffset.offset = *pSavedOffset;

  tEncodeSize(tEncodeMqVgOffset, &vgOffset, len, code);
  if (code < 0) {
    return TAOS_GET_TERRNO(TSDB_CODE_INVALID_PARA);
  }

  void* buf = rpcMallocCont(len);
  if (buf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, len);
  code = tEncodeMqVgOffset(&encoder, &vgOffset);
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(TSDB_CODE_INVALID_PARA);
  }

  SRpcMsg rsp = {.info = pMsg->info, .pCont = buf, .contLen = len, .code = 0};

  tmsgSendRsp(&rsp);
  return 0;
}

int32_t tqProcessVgWalInfoReq(STQ* pTq, SRpcMsg* pMsg) {
  int32_t    code = 0;
  SMqPollReq req = {0};
  if (tDeserializeSMqPollReq(pMsg->pCont, pMsg->contLen, &req) < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pMsg->contLen);
    return TSDB_CODE_INVALID_MSG;
  }

  int64_t      consumerId = req.consumerId;
  STqOffsetVal reqOffset = req.reqOffset;
  int32_t      vgId = TD_VID(pTq->pVnode);

  // 1. find handle
  taosRLockLatch(&pTq->lock);
  STqHandle* pHandle = taosHashGet(pTq->pHandle, req.subKey, strlen(req.subKey));
  if (pHandle == NULL) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s not found", consumerId, vgId, req.subKey);
    taosRUnLockLatch(&pTq->lock);
    return TSDB_CODE_INVALID_MSG;
  }

  // 2. check rebalance status
  if (pHandle->consumerId != consumerId) {
    tqDebug("ERROR consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
            consumerId, vgId, req.subKey, pHandle->consumerId);
    taosRUnLockLatch(&pTq->lock);
    return TSDB_CODE_TMQ_CONSUMER_MISMATCH;
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  taosRUnLockLatch(&pTq->lock);

  SMqDataRsp dataRsp = {0};
  code = tqInitDataRsp(&dataRsp.common, req.reqOffset);
  if (code != 0) {
    return code;
  }

  if (req.useSnapshot == true) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s snapshot not support wal info", consumerId, vgId, req.subKey);
    code = TSDB_CODE_INVALID_PARA;
    goto END;
  }

  dataRsp.common.rspOffset.type = TMQ_OFFSET__LOG;

  if (reqOffset.type == TMQ_OFFSET__LOG) {
    dataRsp.common.rspOffset.version = reqOffset.version;
  } else if (reqOffset.type < 0) {
    STqOffset* pOffset = NULL;
    code = tqMetaGetOffset(pTq, req.subKey, &pOffset);
    if (code == 0) {
      if (pOffset->val.type != TMQ_OFFSET__LOG) {
        tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s, no valid wal info", consumerId, vgId, req.subKey);
        code = TSDB_CODE_INVALID_PARA;
        goto END;
      }

      dataRsp.common.rspOffset.version = pOffset->val.version;
      tqInfo("consumer:0x%" PRIx64 " vgId:%d subkey:%s get assignment from store:%" PRId64, consumerId, vgId,
             req.subKey, dataRsp.common.rspOffset.version);
    } else {
      if (reqOffset.type == TMQ_OFFSET__RESET_EARLIEST) {
        dataRsp.common.rspOffset.version = sver;  // not consume yet, set the earliest position
      } else if (reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
        dataRsp.common.rspOffset.version = ever;
      }
      tqInfo("consumer:0x%" PRIx64 " vgId:%d subkey:%s get assignment from init:%" PRId64, consumerId, vgId, req.subKey,
             dataRsp.common.rspOffset.version);
    }
  } else {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s invalid offset type:%d", consumerId, vgId, req.subKey,
            reqOffset.type);
    code = TSDB_CODE_INVALID_PARA;
    goto END;
  }

  code = tqDoSendDataRsp(&pMsg->info, &dataRsp, req.epoch, req.consumerId, TMQ_MSG_TYPE__WALINFO_RSP, sver, ever);

END:
  tDeleteMqDataRsp(&dataRsp);
  return code;
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
  if (taosHashRemove(pTq->pOffset, pReq->subKey, strlen(pReq->subKey)) != 0) {
    tqError("cannot process tq delete req %s, since no such offset in hash", pReq->subKey);
  }
  if (tqMetaDeleteInfo(pTq, pTq->pOffsetStore, pReq->subKey, strlen(pReq->subKey)) != 0) {
    tqError("cannot process tq delete req %s, since no such offset in tdb", pReq->subKey);
  }

  if (tqMetaDeleteInfo(pTq, pTq->pExecStore, pReq->subKey, strlen(pReq->subKey)) < 0) {
    tqError("cannot process tq delete req %s, since no such offset in tdb", pReq->subKey);
  }
  taosWUnLockLatch(&pTq->lock);

  return 0;
}

int32_t tqProcessAddCheckInfoReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  STqCheckInfo info = {0};
  int32_t      code = tqMetaDecodeCheckInfo(&info, msg, msgLen);
  if (code != 0) {
    return code;
  }

  code = taosHashPut(pTq->pCheckInfo, info.topic, strlen(info.topic), &info, sizeof(STqCheckInfo));
  if (code != 0) {
    tDeleteSTqCheckInfo(&info);
    return code;
  }

  return tqMetaSaveInfo(pTq, pTq->pCheckStore, info.topic, strlen(info.topic), msg, msgLen);
}

int32_t tqProcessDelCheckInfoReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  if (taosHashRemove(pTq->pCheckInfo, msg, strlen(msg)) < 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  return tqMetaDeleteInfo(pTq, pTq->pCheckStore, msg, strlen(msg));
}

int32_t tqProcessSubscribeReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  int         ret = 0;
  SMqRebVgReq req = {0};
  SDecoder    dc = {0};

  tDecoderInit(&dc, (uint8_t*)msg, msgLen);
  ret = tDecodeSMqRebVgReq(&dc, &req);
  if (ret < 0) {
    goto end;
  }

  tqInfo("vgId:%d, tq process sub req:%s, Id:0x%" PRIx64 " -> Id:0x%" PRIx64, pTq->pVnode->config.vgId, req.subKey,
         req.oldConsumerId, req.newConsumerId);

  taosRLockLatch(&pTq->lock);
  STqHandle* pHandle = NULL;
  int32_t code = tqMetaGetHandle(pTq, req.subKey, &pHandle);
  if (code != 0){
    tqInfo("vgId:%d, tq process sub req:%s, no such handle, create new one", pTq->pVnode->config.vgId, req.subKey);
  }
  taosRUnLockLatch(&pTq->lock);
  if (pHandle == NULL) {
    if (req.oldConsumerId != -1) {
      tqError("vgId:%d, build new consumer handle %s for consumer:0x%" PRIx64 ", but old consumerId:0x%" PRIx64,
              req.vgId, req.subKey, req.newConsumerId, req.oldConsumerId);
    }
    if (req.newConsumerId == -1) {
      tqError("vgId:%d, tq invalid rebalance request, new consumerId %" PRId64 "", req.vgId, req.newConsumerId);
      ret = TSDB_CODE_INVALID_PARA;
      goto end;
    }
    STqHandle handle = {0};
    ret = tqMetaCreateHandle(pTq, &req, &handle);
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

int32_t tqBuildStreamTask(void* pTqObj, SStreamTask* pTask, int64_t nextProcessVer) {
  STQ*    pTq = (STQ*)pTqObj;
  int32_t vgId = TD_VID(pTq->pVnode);
  tqDebug("s-task:0x%x start to build task", pTask->id.taskId);

  int32_t code = streamTaskInit(pTask, pTq->pStreamMeta, &pTq->pVnode->msgCb, nextProcessVer);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pTask->pBackend = NULL;

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
      return terrno;
    }

    pOutputInfo->tbSink.pTblInfo = tSimpleHashInit(10240, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (pOutputInfo->tbSink.pTblInfo == NULL) {
      tqError("vgId:%d failed init sink tableInfo, code:%s", vgId, tstrerror(terrno));
      return terrno;
    }

    tSimpleHashSetFreeFp(pOutputInfo->tbSink.pTblInfo, freePtr);
  }

  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    SWalFilterCond cond = {.deleteMsg = 1};  // delete msg also extract from wal files
    pTask->exec.pWalReader = walOpenReader(pTq->pVnode->pWal, &cond, pTask->id.taskId);
    if (pTask->exec.pWalReader == NULL) {
      tqError("vgId:%d failed init wal reader, code:%s", vgId, tstrerror(terrno));
      return terrno;
    }
  }

  streamTaskResetUpstreamStageInfo(pTask);
  streamSetupScheduleTrigger(pTask);

  SCheckpointInfo* pChkInfo = &pTask->chkInfo;
  tqSetRestoreVersionInfo(pTask);

  char*       p = streamTaskGetStatus(pTask).name;
  const char* pNext = streamTaskGetStatusStr(pTask->status.taskStatus);

  if (pTask->info.fillHistory) {
    tqInfo("vgId:%d build stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
           " nextProcessVer:%" PRId64
           " child id:%d, level:%d, cur-status:%s, next-status:%s fill-history:%d, related stream task:0x%x "
           "delaySched:%" PRId64 " ms, inputVer:%" PRId64,
           vgId, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
           pTask->info.selfChildId, pTask->info.taskLevel, p, pNext, pTask->info.fillHistory,
           (int32_t)pTask->streamTaskId.taskId, pTask->info.delaySchedParam, nextProcessVer);
  } else {
    tqInfo("vgId:%d build stream task, s-task:%s, checkpointId:%" PRId64 " checkpointVer:%" PRId64
           " nextProcessVer:%" PRId64
           " child id:%d, level:%d, cur-status:%s next-status:%s fill-history:%d, related fill-task:0x%x "
           "delaySched:%" PRId64 " ms, inputVer:%" PRId64,
           vgId, pTask->id.idStr, pChkInfo->checkpointId, pChkInfo->checkpointVer, pChkInfo->nextProcessVer,
           pTask->info.selfChildId, pTask->info.taskLevel, p, pNext, pTask->info.fillHistory,
           (int32_t)pTask->hTaskInfo.id.taskId, pTask->info.delaySchedParam, nextProcessVer);

    if (pChkInfo->checkpointVer > pChkInfo->nextProcessVer) {
      tqError("vgId:%d build stream task, s-task:%s, checkpointVer:%" PRId64 " > nextProcessVer:%" PRId64, vgId,
              pTask->id.idStr, pChkInfo->checkpointVer, pChkInfo->nextProcessVer);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
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
  SVersionRange* pStep2Range = &pTask->step2Range;
  int32_t        vgId = pTask->pMeta->vgId;

  // if it's an source task, extract the last version in wal.
  bool done = streamHistoryTaskSetVerRangeStep2(pTask, nextProcessedVer);
  pTask->execInfo.step2Start = taosGetTimestampMs();

  if (done) {
    qDebug("s-task:%s scan wal(step 2) verRange:%" PRId64 "-%" PRId64 " ended, elapsed time:%.2fs", id,
           pStep2Range->minVer, pStep2Range->maxVer, 0.0);
    int32_t code = streamTaskPutTranstateIntoInputQ(pTask);  // todo: msg lost.
    if (code) {
      qError("s-task:%s failed put trans-state into inputQ, code:%s", id, tstrerror(code));
    }
    (void)streamExecTask(pTask);  // exec directly
  } else {
    STimeWindow* pWindow = &pTask->dataRange.window;
    tqDebug("s-task:%s level:%d verRange:%" PRId64 "-%" PRId64 " window:%" PRId64 "-%" PRId64
            ", do secondary scan-history from WAL after halt the related stream task:%s",
            id, pTask->info.taskLevel, pStep2Range->minVer, pStep2Range->maxVer, pWindow->skey, pWindow->ekey,
            pStreamTask->id.idStr);
    if (pTask->status.schedStatus != TASK_SCHED_STATUS__WAITING) {
      tqError("s-task:%s level:%d unexpected sched-status:%d", id, pTask->info.taskLevel, pTask->status.schedStatus);
    }

    int32_t code = streamSetParamForStreamScannerStep2(pTask, pStep2Range, pWindow);
    if (code) {
      tqError("s-task:%s level:%d failed to set step2 param", id, pTask->info.taskLevel);
    }

    int64_t dstVer = pStep2Range->minVer;
    pTask->chkInfo.nextProcessVer = dstVer;

    walReaderSetSkipToVersion(pTask->exec.pWalReader, dstVer);
    tqDebug("s-task:%s wal reader start scan WAL verRange:%" PRId64 "-%" PRId64 ", set sched-status:%d", id, dstVer,
            pStep2Range->maxVer, TASK_SCHED_STATUS__INACTIVE);

    int8_t status = streamTaskSetSchedStatusInactive(pTask);

    // now the fill-history task starts to scan data from wal files.
    code = streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_SCANHIST_DONE);
    if (code == TSDB_CODE_SUCCESS) {
      code = tqScanWalAsync(pTq, false);
      if (code) {
        tqError("vgId:%d failed to start scan wal file, code:%s", vgId, tstrerror(code));
      }
    }
  }
}

int32_t handleStep2Async(SStreamTask* pStreamTask, void* param) {
  STQ* pTq = param;

  SStreamMeta* pMeta = pStreamTask->pMeta;
  STaskId      hId = pStreamTask->hTaskInfo.id;
  SStreamTask* pTask = NULL;
  int32_t      code = streamMetaAcquireTask(pStreamTask->pMeta, hId.streamId, hId.taskId, &pTask);
  if (pTask == NULL) {
    tqWarn("s-task:0x%x failed to acquired it to exec step 2, scan wal quit", (int32_t)hId.taskId);
    return TSDB_CODE_SUCCESS;
  }

  doStartFillhistoryStep2(pTask, pStreamTask, pTq);

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

// this function should be executed by only one thread, so we set an sentinel to protect this function
int32_t tqProcessTaskScanHistory(STQ* pTq, SRpcMsg* pMsg) {
  SStreamScanHistoryReq* pReq = (SStreamScanHistoryReq*)pMsg->pCont;
  SStreamMeta*           pMeta = pTq->pStreamMeta;
  int32_t                code = TSDB_CODE_SUCCESS;

  SStreamTask* pTask = NULL;
  code = streamMetaAcquireTask(pMeta, pReq->streamId, pReq->taskId, &pTask);
  if (pTask == NULL) {
    tqError("vgId:%d failed to acquire stream task:0x%x during scan history data, task may have been destroyed",
            pMeta->vgId, pReq->taskId);
    return -1;
  }

  // do recovery step1
  const char* id = pTask->id.idStr;
  char*       pStatus = streamTaskGetStatus(pTask).name;

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
      streamExecScanHistoryInFuture(pTask, retInfo.idleTime);
    } else {
      SStreamTaskState p = streamTaskGetStatus(pTask);
      ETaskStatus      s = p.state;

      if (s == TASK_STATUS__PAUSE) {
        tqDebug("s-task:%s is paused in the step1, elapsed time:%.2fs total:%.2fs, sched-status:%d", pTask->id.idStr,
                el, pTask->execInfo.step1El, status);
      } else if (s == TASK_STATUS__STOP || s == TASK_STATUS__DROPPING) {
        tqDebug("s-task:%s status:%p not continue scan-history data, total elapsed time:%.2fs quit", pTask->id.idStr,
                p.name, pTask->execInfo.step1El);
      }
    }

    streamMetaReleaseTask(pMeta, pTask);
    return 0;
  }

  // the following procedure should be executed, no matter status is stop/pause or not
  tqDebug("s-task:%s scan-history(step 1) ended, elapsed time:%.2fs", id, pTask->execInfo.step1El);

  if (pTask->info.fillHistory != 1) {
    tqError("s-task:%s fill-history is disabled, unexpected", id);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }

  // 1. get the related stream task
  SStreamTask* pStreamTask = NULL;
  code = streamMetaAcquireTask(pMeta, pTask->streamTaskId.streamId, pTask->streamTaskId.taskId, &pStreamTask);
  if (pStreamTask == NULL) {
    tqError("failed to find s-task:0x%" PRIx64 ", it may have been destroyed, drop related fill-history task:%s",
            pTask->streamTaskId.taskId, pTask->id.idStr);

    tqDebug("s-task:%s fill-history task set status to be dropping", id);
    code = streamBuildAndSendDropTaskMsg(pTask->pMsgCb, pMeta->vgId, &pTask->id, 0);

    atomic_store_32(&pTask->status.inScanHistorySentinel, 0);
    streamMetaReleaseTask(pMeta, pTask);
    return code;  // todo: handle failure
  }

  if (pStreamTask->info.taskLevel != TASK_LEVEL__SOURCE) {
    tqError("s-task:%s fill-history task related stream task level:%d, unexpected", id, pStreamTask->info.taskLevel);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  code = streamTaskHandleEventAsync(pStreamTask->status.pSM, TASK_EVENT_HALT, handleStep2Async, pTq);

  streamMetaReleaseTask(pMeta, pStreamTask);

  atomic_store_32(&pTask->status.inScanHistorySentinel, 0);
  streamMetaReleaseTask(pMeta, pTask);
  return code;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  // extracted submit data from wal files for all tasks
  if (pReq->reqType == STREAM_EXEC_T_EXTRACT_WAL_DATA) {
    return tqScanWal(pTq);
  }

  int32_t code = tqStreamTaskProcessRunReq(pTq->pStreamMeta, pMsg, vnodeIsRoleLeader(pTq->pVnode));
  if (code) {
    tqError("vgId:%d failed to create task run req, code:%s", TD_VID(pTq->pVnode), tstrerror(code));
    return code;
  }

  // let's continue scan data in the wal files
  if (pReq->reqType >= 0 || pReq->reqType == STREAM_EXEC_T_RESUME_TASK) {
    code = tqScanWalAsync(pTq, false);  // it's ok to failed
    if (code) {
      tqError("vgId:%d failed to start scan wal file, code:%s", pTq->pStreamMeta->vgId, tstrerror(code));
    }
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

int32_t tqProcessTaskUpdateCheckpointReq(STQ* pTq, char* msg, int32_t msgLen) {
  return tqStreamTaskProcessUpdateCheckpointReq(pTq->pStreamMeta, pTq->pVnode->restored, msg);
}

int32_t tqProcessTaskConsenChkptIdReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessConsenChkptIdReq(pTq->pStreamMeta, pMsg);
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

int32_t tqProcessTaskRetrieveRsp(STQ* pTq, SRpcMsg* pMsg) { return 0; }

int32_t tqStreamProgressRetrieveReq(STQ* pTq, SRpcMsg* pMsg) {
  char*               msgStr = pMsg->pCont;
  char*               msgBody = POINTER_SHIFT(msgStr, sizeof(SMsgHead));
  int32_t             msgLen = pMsg->contLen - sizeof(SMsgHead);
  int32_t             code = 0;
  SStreamProgressReq  req;
  char*               pRspBuf = taosMemoryCalloc(1, sizeof(SMsgHead) + sizeof(SStreamProgressRsp));
  SStreamProgressRsp* pRsp = POINTER_SHIFT(pRspBuf, sizeof(SMsgHead));
  if (!pRspBuf) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = -1;
    goto _OVER;
  }

  code = tDeserializeStreamProgressReq(msgBody, msgLen, &req);
  if (code == TSDB_CODE_SUCCESS) {
    code = tqGetStreamExecInfo(pTq->pVnode, req.streamId, &pRsp->progressDelay, &pRsp->fillHisFinished);
  }
  if (code == TSDB_CODE_SUCCESS) {
    pRsp->fetchIdx = req.fetchIdx;
    pRsp->subFetchIdx = req.subFetchIdx;
    pRsp->vgId = req.vgId;
    pRsp->streamId = req.streamId;
    code = tSerializeStreamProgressRsp(pRsp, sizeof(SStreamProgressRsp) + sizeof(SMsgHead), pRsp);
    if (code) {
      goto _OVER;
    }

    SRpcMsg rsp = {.info = pMsg->info, .code = 0};
    rsp.pCont = pRspBuf;
    pRspBuf = NULL;
    rsp.contLen = sizeof(SMsgHead) + sizeof(SStreamProgressRsp);
    tmsgSendRsp(&rsp);
  }

_OVER:
  if (pRspBuf) {
    taosMemoryFree(pRspBuf);
  }
  return code;
}

// no matter what kinds of error happened, make sure the mnode will receive the success execution code.
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
    int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
    if (ret) {  // suppress the error in build checkpointsource rsp
      tqError("s-task:0x%x failed to build checkpoint-source rsp, code:%s", req.taskId, tstrerror(code));
    }

    tmsgSendRsp(&rsp);         // error occurs
    return TSDB_CODE_SUCCESS;  // always return success to mnode, todo: handle failure of build and send msg to mnode
  }
  tDecoderClear(&decoder);

  if (!vnodeIsRoleLeader(pTq->pVnode)) {
    tqDebug("vgId:%d not leader, ignore checkpoint-source msg, s-task:0x%x", vgId, req.taskId);
    SRpcMsg rsp = {0};
    int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
    if (ret) {  // suppress the error in build checkpointsource rsp
      tqError("s-task:0x%x failed to build checkpoint-source rsp, code:%s", req.taskId, tstrerror(code));
    }

    tmsgSendRsp(&rsp);         // error occurs
    return TSDB_CODE_SUCCESS;  // always return success to mnode, todo: handle failure of build and send msg to mnode
  }

  if (!pTq->pVnode->restored) {
    tqDebug("vgId:%d checkpoint-source msg received during restoring, checkpointId:%" PRId64
            ", transId:%d s-task:0x%x ignore it",
            vgId, req.checkpointId, req.transId, req.taskId);
    SRpcMsg rsp = {0};
    int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
    if (ret) {  // suppress the error in build checkpointsource rsp
      tqError("s-task:0x%x failed to build checkpoint-source rsp, code:%s", req.taskId, tstrerror(code));
    }

    tmsgSendRsp(&rsp);         // error occurs
    return TSDB_CODE_SUCCESS;  // always return success to mnode, , todo: handle failure of build and send msg to mnode
  }

  SStreamTask* pTask = NULL;
  code = streamMetaAcquireTask(pMeta, req.streamId, req.taskId, &pTask);
  if (pTask == NULL || code != 0) {
    tqError("vgId:%d failed to find s-task:0x%x, ignore checkpoint msg. checkpointId:%" PRId64
            " transId:%d it may have been destroyed",
            vgId, req.taskId, req.checkpointId, req.transId);
    SRpcMsg rsp = {0};
    int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
    if (ret) {  // suppress the error in build checkpointsource rsp
      tqError("s-task:%s failed to build checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
    }
    tmsgSendRsp(&rsp);  // error occurs
    return TSDB_CODE_SUCCESS;
  }

  if (pTask->status.downstreamReady != 1) {
    streamTaskSetFailedChkptInfo(pTask, req.transId, req.checkpointId);  // record the latest failed checkpoint id
    tqError("s-task:%s not ready for checkpoint, since downstream not ready, ignore this checkpointId:%" PRId64
            ", transId:%d set it failed",
            pTask->id.idStr, req.checkpointId, req.transId);
    streamMetaReleaseTask(pMeta, pTask);

    SRpcMsg rsp = {0};
    int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
    if (ret) {  // suppress the error in build checkpointsource rsp
      tqError("s-task:%s failed to build checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
    }

    tmsgSendRsp(&rsp);         // error occurs
    return TSDB_CODE_SUCCESS;  // todo retry handle error
  }

  // todo save the checkpoint failed info
  streamMutexLock(&pTask->lock);
  ETaskStatus status = streamTaskGetStatus(pTask).state;

  if (req.mndTrigger == 1) {
    if (status == TASK_STATUS__HALT || status == TASK_STATUS__PAUSE) {
      tqError("s-task:%s not ready for checkpoint, since it is halt, ignore checkpointId:%" PRId64 ", set it failure",
              pTask->id.idStr, req.checkpointId);

      streamMutexUnlock(&pTask->lock);
      streamMetaReleaseTask(pMeta, pTask);

      SRpcMsg rsp = {0};
      int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
      if (ret) {  // suppress the error in build checkpointsource rsp
        tqError("s-task:%s failed to build checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
      }

      tmsgSendRsp(&rsp);  // error occurs
      return TSDB_CODE_SUCCESS;
    }
  } else {
    if (status != TASK_STATUS__HALT) {
      tqError("s-task:%s should in halt status, let's halt it directly", pTask->id.idStr);
      //      streamTaskHandleEvent(pTask->status.pSM, TASK_EVENT_HALT);
    }
  }

  // check if the checkpoint msg already sent or not.
  if (status == TASK_STATUS__CK) {
    int64_t checkpointId = 0;
    streamTaskGetActiveCheckpointInfo(pTask, NULL, &checkpointId);

    tqWarn("s-task:%s repeatly recv checkpoint-source msg checkpointId:%" PRId64
           " transId:%d already handled, ignore msg and continue process checkpoint",
           pTask->id.idStr, checkpointId, req.transId);

    streamMutexUnlock(&pTask->lock);
    streamMetaReleaseTask(pMeta, pTask);

    return TSDB_CODE_SUCCESS;
  } else {  // checkpoint already finished, and not in checkpoint status
    if (req.checkpointId <= pTask->chkInfo.checkpointId) {
      tqWarn("s-task:%s repeatly recv checkpoint-source msg checkpointId:%" PRId64
             " transId:%d already handled, return success",
             pTask->id.idStr, req.checkpointId, req.transId);

      streamMutexUnlock(&pTask->lock);
      streamMetaReleaseTask(pMeta, pTask);

      SRpcMsg rsp = {0};
      int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
      if (ret) {  // suppress the error in build checkpointsource rsp
        tqError("s-task:%s failed to build checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
      }

      tmsgSendRsp(&rsp);  // error occurs

      return TSDB_CODE_SUCCESS;
    }
  }

  code = streamProcessCheckpointSourceReq(pTask, &req);
  streamMutexUnlock(&pTask->lock);

  if (code) {
    qError("s-task:%s (vgId:%d) failed to process checkpoint-source req, code:%s", pTask->id.idStr, vgId,
           tstrerror(code));
    return code;
  }

  if (req.mndTrigger) {
    tqInfo("s-task:%s (vgId:%d) level:%d receive checkpoint-source msg chkpt:%" PRId64 ", transId:%d, ",
           pTask->id.idStr, vgId, pTask->info.taskLevel, req.checkpointId, req.transId);
  } else {
    const char* pPrevStatus = streamTaskGetStatusStr(streamTaskGetPrevStatus(pTask));
    tqInfo("s-task:%s (vgId:%d) level:%d receive checkpoint-source msg chkpt:%" PRId64
           ", transId:%d after transfer-state, prev status:%s",
           pTask->id.idStr, vgId, pTask->info.taskLevel, req.checkpointId, req.transId, pPrevStatus);
  }

  code = streamAddCheckpointSourceRspMsg(&req, &pMsg->info, pTask);
  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {0};
    int32_t ret = streamTaskBuildCheckpointSourceRsp(&req, &pMsg->info, &rsp, TSDB_CODE_SUCCESS);
    if (ret) {  // suppress the error in build checkpointsource rsp
      tqError("s-task:%s failed to build checkpoint-source rsp, code:%s", pTask->id.idStr, tstrerror(code));
    }
    tmsgSendRsp(&rsp);  // error occurs
    return TSDB_CODE_SUCCESS;
  }

  streamMetaReleaseTask(pMeta, pTask);
  return TSDB_CODE_SUCCESS;
}

// downstream task has complete the stream task checkpoint procedure, let's start the handle the rsp by execute task
int32_t tqProcessTaskCheckpointReadyMsg(STQ* pTq, SRpcMsg* pMsg) {
  int32_t vgId = TD_VID(pTq->pVnode);

  SRetrieveChkptTriggerReq* pReq = (SRetrieveChkptTriggerReq*)pMsg->pCont;
  if (!vnodeIsRoleLeader(pTq->pVnode)) {
    tqError("vgId:%d not leader, ignore the retrieve checkpoint-trigger msg from 0x%x", vgId,
            (int32_t)pReq->downstreamTaskId);
    return TSDB_CODE_STREAM_NOT_LEADER;
  }

  return tqStreamTaskProcessCheckpointReadyMsg(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskUpdateReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessUpdateReq(pTq->pStreamMeta, &pTq->pVnode->msgCb, pMsg, pTq->pVnode->restored);
}

int32_t tqProcessTaskResetReq(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessTaskResetReq(pTq->pStreamMeta, pMsg->pCont);
}

int32_t tqProcessTaskRetrieveTriggerReq(STQ* pTq, SRpcMsg* pMsg) {
  int32_t vgId = TD_VID(pTq->pVnode);

  SRetrieveChkptTriggerReq* pReq = (SRetrieveChkptTriggerReq*)pMsg->pCont;
  if (!vnodeIsRoleLeader(pTq->pVnode)) {
    tqError("vgId:%d not leader, ignore the retrieve checkpoint-trigger msg from 0x%x", vgId,
            (int32_t)pReq->downstreamTaskId);
    return TSDB_CODE_STREAM_NOT_LEADER;
  }

  return tqStreamTaskProcessRetrieveTriggerReq(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskRetrieveTriggerRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamTaskProcessRetrieveTriggerRsp(pTq->pStreamMeta, pMsg);
}

// this function is needed, do not try to remove it.
int32_t tqProcessStreamHbRsp(STQ* pTq, SRpcMsg* pMsg) { return tqStreamProcessStreamHbRsp(pTq->pStreamMeta, pMsg); }

int32_t tqProcessStreamReqCheckpointRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamProcessReqCheckpointRsp(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskCheckpointReadyRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamProcessCheckpointReadyRsp(pTq->pStreamMeta, pMsg);
}

int32_t tqProcessTaskChkptReportRsp(STQ* pTq, SRpcMsg* pMsg) {
  return tqStreamProcessChkptReportRsp(pTq->pStreamMeta, pMsg);
}

