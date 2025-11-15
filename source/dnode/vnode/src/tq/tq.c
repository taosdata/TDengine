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
#include "stream.h"
#include "vnd.h"

// 0: not init
// 1: already inited
// 2: wait to be inited or cleanup
static int32_t tqInitialize(STQ* pTq);

static FORCE_INLINE bool tqIsHandleExec(STqHandle* pHandle) { return pHandle != NULL ? TMQ_HANDLE_STATUS_EXEC == pHandle->status : true; }
static FORCE_INLINE void tqSetHandleExec(STqHandle* pHandle) { if (pHandle != NULL) pHandle->status = TMQ_HANDLE_STATUS_EXEC; }
static FORCE_INLINE void tqSetHandleIdle(STqHandle* pHandle) { if (pHandle != NULL) pHandle->status = TMQ_HANDLE_STATUS_IDLE; }
#define MAX_LOOP 100

void tqDestroyTqHandle(void* data) {
  if (data == NULL) return;
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
  taosHashCleanup(pData->tableCreateTimeHash);
}

static bool tqOffsetEqual(const STqOffset* pLeft, const STqOffset* pRight) {
  if (pLeft == NULL || pRight == NULL) {
    return false;
  }
  return pLeft->val.type == TMQ_OFFSET__LOG && pRight->val.type == TMQ_OFFSET__LOG &&
         pLeft->val.version == pRight->val.version;
}

int32_t tqOpen(const char* path, SVnode* pVnode) {
  if (path == NULL || pVnode == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool ignoreTq = pVnode->mounted && !taosCheckExistFile(path);
  if (ignoreTq) {
    return 0;
  }

  STQ* pTq = taosMemoryCalloc(1, sizeof(STQ));
  if (pTq == NULL) {
    return terrno;
  }

  pVnode->pTq = pTq;
  pTq->pVnode = pVnode;

  pTq->path = taosStrdup(path);
  if (pTq->path == NULL) {
    return terrno;
  }

  pTq->pHandle = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  if (pTq->pHandle == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pTq->pHandle, tqDestroyTqHandle);

  taosInitRWLatch(&pTq->lock);

  pTq->pPushMgr = taosHashInit(64, MurmurHash3_32, false, HASH_NO_LOCK);
  if (pTq->pPushMgr == NULL) {
    return terrno;
  }

  pTq->pCheckInfo = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pTq->pCheckInfo == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pTq->pCheckInfo, (FDelete)tDeleteSTqCheckInfo);

  pTq->pOffset = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_ENTRY_LOCK);
  if (pTq->pOffset == NULL) {
    return terrno;
  }
  taosHashSetFreeFp(pTq->pOffset, (FDelete)tDeleteSTqOffset);

  return tqInitialize(pTq);
}

int32_t tqInitialize(STQ* pTq) {
  if (pTq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  return tqMetaOpen(pTq);
}

void tqClose(STQ* pTq) {
  qDebug("start to close tq");
  if (pTq == NULL) {
    return;
  }

  int32_t vgId = 0;
  if (pTq->pVnode != NULL) {
    vgId = TD_VID(pTq->pVnode);
  }

  void* pIter = taosHashIterate(pTq->pPushMgr, NULL);
  while (pIter) {
    STqHandle* pHandle = *(STqHandle**)pIter;
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
  qDebug("vgId:%d end to close tq", vgId);

  taosMemoryFree(pTq);
}

void tqPushEmptyDataRsp(STqHandle* pHandle, int32_t vgId) {
  if (pHandle == NULL) {
    return;
  }
  int32_t    code = 0;
  SMqPollReq req = {0};
  code = tDeserializeSMqPollReq(pHandle->msg->pCont, pHandle->msg->contLen, &req);
  if (code < 0) {
    tqError("tDeserializeSMqPollReq %d failed, code:%d", pHandle->msg->contLen, code);
    return;
  }

  SMqDataRsp dataRsp = {0};
  code = tqInitDataRsp(&dataRsp, req.reqOffset);
  if (code != 0) {
    tqError("tqInitDataRsp failed, code:%d", code);
    return;
  }
  dataRsp.blockNum = 0;
  char buf[TSDB_OFFSET_LEN] = {0};
  (void)tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.reqOffset);
  tqInfo("tqPushEmptyDataRsp to consumer:0x%" PRIx64 " vgId:%d, offset:%s, QID:0x%" PRIx64, req.consumerId, vgId, buf,
         req.reqId);

  code = tqSendDataRsp(pHandle, pHandle->msg, &req, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
  if (code != 0) {
    tqError("tqSendDataRsp failed, code:%d", code);
  }
  tDeleteMqDataRsp(&dataRsp);
}

int32_t tqSendDataRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, SMqDataRsp* pRsp, int32_t type,
                      int32_t vgId) {
  if (pHandle == NULL || pMsg == NULL || pReq == NULL || pRsp == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);

  char buf1[TSDB_OFFSET_LEN] = {0};
  char buf2[TSDB_OFFSET_LEN] = {0};
  (void)tFormatOffset(buf1, TSDB_OFFSET_LEN, &(pRsp->reqOffset));
  (void)tFormatOffset(buf2, TSDB_OFFSET_LEN, &(pRsp->rspOffset));

  tqDebug("tmq poll vgId:%d consumer:0x%" PRIx64 " (epoch %d) start to send rsp, block num:%d, req:%s, rsp:%s, QID:0x%" PRIx64,
          vgId, pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2, pReq->reqId);

  return tqDoSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type, sver, ever);
}

int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  if (pTq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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

  // if (pOffset->val.type == TMQ_OFFSET__LOG && vgOffset.markWal) {
  //   int32_t ret = walSetKeepVersion(pTq->pVnode->pWal, pOffset->val.version);
  //   tqDebug("set wal reader keep version to %" PRId64 " for vgId:%d sub:%s, code:%d", pOffset->val.version, vgId,
  //           pOffset->subKey, ret);
  // }
  // save the new offset value
  code = taosHashPut(pTq->pOffset, pOffset->subKey, strlen(pOffset->subKey), pOffset, sizeof(STqOffset));
  if (code != 0) {
    goto end;
  }

  return 0;
end:
  tOffsetDestroy(&vgOffset.offset.val);
  return code;
}

int32_t tqProcessSeekReq(STQ* pTq, SRpcMsg* pMsg) {
  if (pTq == NULL || pMsg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
    code = TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
    taosWUnLockLatch(&pTq->lock);
    goto end;
  }

  // 2. check consumer-vg assignment status
  if (pHandle->consumerId != req.consumerId) {
    tqError("ERROR tmq seek, consumer:0x%" PRIx64 " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
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
  if (pTq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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

int32_t tqProcessPollPush(STQ* pTq) {
  if (pTq == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
  if (pTq == NULL || pMsg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SMqPollReq req = {0};
  int        code = tDeserializeSMqPollReq(pMsg->pCont, pMsg->contLen, &req);
  if (code < 0) {
    tqError("tDeserializeSMqPollReq %d failed", pMsg->contLen);
    code = TSDB_CODE_INVALID_MSG;
    goto END;
  }
  if (req.rawData == 1){
    req.uidHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (req.uidHash == NULL) {
      tqError("tq poll rawData taosHashInit failed");
      code = terrno;
      goto END;
    }
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
      tqError("tmq poll: consumer:0x%" PRIx64 " vgId:%d subkey %s not found, msg:%s", consumerId, vgId, req.subKey, tstrerror(code));
      taosWUnLockLatch(&pTq->lock);
      return code;
    }

    // 2. check rebalance status
    if (pHandle->consumerId != consumerId) {
      tqError("ERROR tmq poll: consumer:0x%" PRIx64
              " vgId:%d, subkey %s, mismatch for saved handle consumer:0x%" PRIx64,
              consumerId, TD_VID(pTq->pVnode), req.subKey, pHandle->consumerId);
      code = TSDB_CODE_TMQ_CONSUMER_MISMATCH;
      taosWUnLockLatch(&pTq->lock);
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
  tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey %s, recv poll req vgId:%d, req:%s, QID:0x%" PRIx64,
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
  if (pTq == NULL || pMsg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
  if (pTq == NULL || pMsg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
    return TSDB_CODE_MND_SUBSCRIBE_NOT_EXIST;
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
  code = tqInitDataRsp(&dataRsp, req.reqOffset);
  if (code != 0) {
    return code;
  }

  if (req.useSnapshot == true) {
    tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s snapshot not support wal info", consumerId, vgId, req.subKey);
    code = TSDB_CODE_INVALID_PARA;
    goto END;
  }

  dataRsp.rspOffset.type = TMQ_OFFSET__LOG;

  if (reqOffset.type == TMQ_OFFSET__LOG) {
    dataRsp.rspOffset.version = reqOffset.version;
  } else if (reqOffset.type < 0) {
    STqOffset* pOffset = NULL;
    code = tqMetaGetOffset(pTq, req.subKey, &pOffset);
    if (code == 0) {
      if (pOffset->val.type != TMQ_OFFSET__LOG) {
        tqError("consumer:0x%" PRIx64 " vgId:%d subkey:%s, no valid wal info", consumerId, vgId, req.subKey);
        code = TSDB_CODE_INVALID_PARA;
        goto END;
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
    code = TSDB_CODE_INVALID_PARA;
    goto END;
  }

  code = tqDoSendDataRsp(&pMsg->info, &dataRsp, req.epoch, req.consumerId, TMQ_MSG_TYPE__WALINFO_RSP, sver, ever);

END:
  tDeleteMqDataRsp(&dataRsp);
  return code;
}

int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  if (pTq == NULL || msg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
  if (pTq == NULL || msg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  STqCheckInfo info = {0};
  int32_t      code = tqMetaDecodeCheckInfo(&info, msg, msgLen >= 0 ? msgLen : 0);
  if (code != 0) {
    return code;
  }

  code = taosHashPut(pTq->pCheckInfo, info.topic, strlen(info.topic), &info, sizeof(STqCheckInfo));
  if (code != 0) {
    tDeleteSTqCheckInfo(&info);
    return code;
  }
  taosWLockLatch(&pTq->lock);
  code  = tqMetaSaveInfo(pTq, pTq->pCheckStore, info.topic, strlen(info.topic), msg, msgLen >= 0 ? msgLen : 0);
  taosWUnLockLatch(&pTq->lock);
  return code;
}

int32_t tqProcessDelCheckInfoReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  if (pTq == NULL || msg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (taosHashRemove(pTq->pCheckInfo, msg, strlen(msg)) < 0) {
    return TSDB_CODE_TSC_INTERNAL_ERROR;
  }
  taosWLockLatch(&pTq->lock);
  int32_t code = tqMetaDeleteInfo(pTq, pTq->pCheckStore, msg, strlen(msg));
  taosWUnLockLatch(&pTq->lock);
  return code;
}

int32_t tqProcessSubscribeReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  if (pTq == NULL || msg == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
    tqInfo("vgId:%d, tq process sub req:%s, no such handle, create new one, msg:%s", pTq->pVnode->config.vgId, req.subKey, tstrerror(code));
  }
  taosRUnLockLatch(&pTq->lock);
  if (pHandle == NULL) {
    if (req.oldConsumerId != -1) {
      tqError("vgId:%d, build new consumer handle %s for consumer:0x%" PRIx64 ", but old consumerId:0x%" PRIx64,
              req.vgId, req.subKey, req.newConsumerId, req.oldConsumerId);
    }
    if (req.newConsumerId == -1) {
      tqError("vgId:%d, tq invalid rebalance request, new consumerId %" PRId64, req.vgId, req.newConsumerId);
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
    int maxLoop = MAX_LOOP;
    while (maxLoop-- > 0) {
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
