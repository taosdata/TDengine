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

#define IS_OFFSET_RESET_TYPE(_t)  ((_t) < 0)
#define ALL_STREAM_TASKS_ID       (-1)

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
}

static void tqPushEntryFree(void* data) {
  STqPushEntry* p = *(void**)data;
  if (p->pDataRsp->head.mqMsgType == TMQ_MSG_TYPE__POLL_RSP) {
    tDeleteSMqDataRsp(p->pDataRsp);
  } else if (p->pDataRsp->head.mqMsgType == TMQ_MSG_TYPE__TAOSX_RSP) {
    tDeleteSTaosxRsp((STaosxRsp*)p->pDataRsp);
  }

  taosMemoryFree(p->pDataRsp);
  taosMemoryFree(p);
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
  pTq->pPushMgr = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  taosHashSetFreeFp(pTq->pPushMgr, tqPushEntryFree);

  pTq->pCheckInfo = taosHashInit(64, MurmurHash3_32, true, HASH_ENTRY_LOCK);
  taosHashSetFreeFp(pTq->pCheckInfo, (FDelete)tDeleteSTqCheckInfo);

  if (tqMetaOpen(pTq) < 0) {
    return NULL;
  }

  pTq->pOffsetStore = tqOffsetOpen(pTq);
  if (pTq->pOffsetStore == NULL) {
    return NULL;
  }

  pTq->pStreamMeta = streamMetaOpen(path, pTq, (FTaskExpand*)tqExpandTask, pTq->pVnode->config.vgId);
  if (pTq->pStreamMeta == NULL) {
    return NULL;
  }

  if (streamLoadTasks(pTq->pStreamMeta, walGetCommittedVer(pVnode->pWal)) < 0) {
    return NULL;
  }

  return pTq;
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

int32_t tqSendMetaPollRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqMetaRsp* pRsp) {
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeSMqMetaRsp, pRsp, len, code);
  if (code < 0) {
    return -1;
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return -1;
  }

  ((SMqRspHead*)buf)->mqMsgType = TMQ_MSG_TYPE__POLL_META_RSP;
  ((SMqRspHead*)buf)->epoch = pReq->epoch;
  ((SMqRspHead*)buf)->consumerId = pReq->consumerId;

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  tEncodeSMqMetaRsp(&encoder, pRsp);
  tEncoderClear(&encoder);

  SRpcMsg resp = {
      .info = pMsg->info,
      .pCont = buf,
      .contLen = tlen,
      .code = 0,
  };
  tmsgSendRsp(&resp);

  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) send rsp, res msg type %d, offset type:%d",
          TD_VID(pTq->pVnode), pReq->consumerId, pReq->epoch, pRsp->resMsgType, pRsp->rspOffset.type);

  return 0;
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
    tEncodeSTaosxRsp(&encoder, (STaosxRsp*) pRsp);
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

int32_t tqPushDataRsp(STQ* pTq, STqPushEntry* pPushEntry) {
  SMqDataRsp* pRsp = pPushEntry->pDataRsp;
  SMqRspHead* pHeader = &pPushEntry->pDataRsp->head;
  doSendDataRsp(&pPushEntry->info, pRsp, pHeader->epoch, pHeader->consumerId, pHeader->mqMsgType);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, tListLen(buf1), &pRsp->reqOffset);
  tFormatOffset(buf2, tListLen(buf2), &pRsp->rspOffset);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) push rsp, block num: %d, req:%s, rsp:%s",
          TD_VID(pTq->pVnode), pRsp->head.consumerId, pRsp->head.epoch, pRsp->blockNum, buf1, buf2);
  return 0;
}

int32_t tqSendDataRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqDataRsp* pRsp, int32_t type) {
  doSendDataRsp(&pMsg->info, pRsp, pReq->epoch, pReq->consumerId, type);

  char buf1[80] = {0};
  char buf2[80] = {0};
  tFormatOffset(buf1, 80, &pRsp->reqOffset);
  tFormatOffset(buf2, 80, &pRsp->rspOffset);

  tqDebug("vgId:%d consumer:0x%" PRIx64 " (epoch %d) send rsp, block num:%d, req:%s, rsp:%s, reqId:0x%"PRIx64,
          TD_VID(pTq->pVnode), pReq->consumerId, pReq->epoch, pRsp->blockNum, buf1, buf2, pReq->reqId);

  return 0;
}

int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  STqOffset offset = {0};
  int32_t vgId = TD_VID(pTq->pVnode);

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
    tqDebug("receive offset commit msg to %s on vgId:%d, offset(type:log) version:%" PRId64, offset.subKey,
            vgId, offset.val.version);
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

static int32_t tqInitDataRsp(SMqDataRsp* pRsp, const SMqPollReq* pReq, int8_t subType) {
  pRsp->reqOffset = pReq->reqOffset;

  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));

  if (pRsp->blockData == NULL || pRsp->blockDataLen == NULL) {
    return -1;
  }

  pRsp->withTbName = 0;
  pRsp->withSchema = false;
  return 0;
}

static int32_t tqInitTaosxRsp(STaosxRsp* pRsp, const SMqPollReq* pReq) {
  pRsp->reqOffset = pReq->reqOffset;

  pRsp->withTbName = 1;
  pRsp->withSchema = 1;
  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));
  pRsp->blockTbName = taosArrayInit(0, sizeof(void*));
  pRsp->blockSchema = taosArrayInit(0, sizeof(void*));

  if (pRsp->blockData == NULL || pRsp->blockDataLen == NULL || pRsp->blockTbName == NULL || pRsp->blockSchema == NULL) {
    return -1;
  }

  return 0;
}

static int32_t extractResetOffsetVal(STqOffsetVal* pOffsetVal, STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                     SRpcMsg* pMsg, bool* pBlockReturned) {
  uint64_t     consumerId = pRequest->consumerId;
  STqOffsetVal reqOffset = pRequest->reqOffset;
  STqOffset*   pOffset = tqOffsetRead(pTq->pOffsetStore, pRequest->subKey);
  int32_t      vgId = TD_VID(pTq->pVnode);

  *pBlockReturned = false;

  // In this vnode, data has been polled by consumer for this topic, so let's continue from the last offset value.
  if (pOffset != NULL) {
    *pOffsetVal = pOffset->val;

    char formatBuf[80];
    tFormatOffset(formatBuf, 80, pOffsetVal);
    tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, existed offset found, offset reset to %s and continue. reqId:0x%"PRIx64,
            consumerId, pHandle->subKey, vgId, formatBuf, pRequest->reqId);
    return 0;
  } else {
    // no poll occurs in this vnode for this topic, let's seek to the right offset value.
    if (reqOffset.type == TMQ_OFFSET__RESET_EARLIEAST) {
      if (pRequest->useSnapshot) {
        tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey:%s, vgId:%d, (earliest) set offset to be snapshot",
                consumerId, pHandle->subKey, vgId);

        if (pHandle->fetchMeta) {
          tqOffsetResetToMeta(pOffsetVal, 0);
        } else {
          tqOffsetResetToData(pOffsetVal, 0, 0);
        }
      } else {
        pHandle->pRef = walRefFirstVer(pTq->pVnode->pWal, pHandle->pRef);
        if (pHandle->pRef == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }

        // offset set to previous version when init
        tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer - 1);
      }
    } else if (reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
      if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
        SMqDataRsp dataRsp = {0};
        tqInitDataRsp(&dataRsp, pRequest, pHandle->execHandle.subType);

        tqOffsetResetToLog(&dataRsp.rspOffset, walGetLastVer(pTq->pVnode->pWal));
        tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, (latest) offset reset to %" PRId64, consumerId,
                pHandle->subKey, vgId, dataRsp.rspOffset.version);
        int32_t code = tqSendDataRsp(pTq, pMsg, pRequest, &dataRsp, TMQ_MSG_TYPE__POLL_RSP);
        tDeleteSMqDataRsp(&dataRsp);

        *pBlockReturned = true;
        return code;
      } else {
        STaosxRsp taosxRsp = {0};
        tqInitTaosxRsp(&taosxRsp, pRequest);
        tqOffsetResetToLog(&taosxRsp.rspOffset, walGetLastVer(pTq->pVnode->pWal));
        int32_t code = tqSendDataRsp(pTq, pMsg, pRequest, (SMqDataRsp*)&taosxRsp, TMQ_MSG_TYPE__TAOSX_RSP);
        tDeleteSTaosxRsp(&taosxRsp);

        *pBlockReturned = true;
        return code;
      }
    } else if (reqOffset.type == TMQ_OFFSET__RESET_NONE) {
      tqError("tmq poll: subkey:%s, no offset committed for consumer:0x%" PRIx64 " in vg %d, subkey %s, reset none failed",
              pHandle->subKey, consumerId, vgId, pRequest->subKey);
      terrno = TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
      return -1;
    }
  }

  return 0;
}

static int32_t extractDataAndRspForNormalSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                                   SRpcMsg* pMsg, STqOffsetVal* pOffset) {
  uint64_t consumerId = pRequest->consumerId;
  int32_t  vgId = TD_VID(pTq->pVnode);

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, pRequest, pHandle->execHandle.subType);

  // lock
  taosWLockLatch(&pTq->lock);

  qSetTaskId(pHandle->execHandle.task, consumerId, pRequest->reqId);
  int code = tqScanData(pTq, pHandle, &dataRsp, pOffset);
  if(code != 0) {
    goto end;
  }

  // till now, all data has been transferred to consumer, new data needs to push client once arrived.
  if (dataRsp.blockNum == 0 && dataRsp.reqOffset.type == TMQ_OFFSET__LOG &&
      dataRsp.reqOffset.version == dataRsp.rspOffset.version && pHandle->consumerId == pRequest->consumerId) {
    code = tqRegisterPushHandle(pTq, pHandle, pRequest, pMsg, &dataRsp, TMQ_MSG_TYPE__POLL_RSP);
    taosWUnLockLatch(&pTq->lock);
    return code;
  }


  code = tqSendDataRsp(pTq, pMsg, pRequest, (SMqDataRsp*)&dataRsp, TMQ_MSG_TYPE__POLL_RSP);

  // NOTE: this pHandle->consumerId may have been changed already.

end:
  {
    char buf[80] = {0};
    tFormatOffset(buf, 80, &dataRsp.rspOffset);
    tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, rsp block:%d, rsp offset type:%s, reqId:0x%" PRIx64 " code:%d",
            consumerId, pHandle->subKey, vgId, dataRsp.blockNum, buf, pRequest->reqId, code);
    taosWUnLockLatch(&pTq->lock);
    tDeleteSMqDataRsp(&dataRsp);
  }
  return code;
}


static int32_t extractDataAndRspForDbStbSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg, STqOffsetVal *offset) {
  int code = 0;
  int32_t      vgId = TD_VID(pTq->pVnode);
  SWalCkHead*  pCkHead = NULL;
  SMqMetaRsp metaRsp = {0};
  STaosxRsp taosxRsp = {0};
  tqInitTaosxRsp(&taosxRsp, pRequest);

  if (offset->type != TMQ_OFFSET__LOG) {
    if (tqScanTaosx(pTq, pHandle, &taosxRsp, &metaRsp, offset) < 0) {
      return -1;
    }

    if (metaRsp.metaRspLen > 0) {
      code = tqSendMetaPollRsp(pTq, pMsg, pRequest, &metaRsp);
      tqDebug("tmq poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send meta offset type:%d,uid:%" PRId64 ",ts:%" PRId64,
              pRequest->consumerId, pHandle->subKey, vgId, metaRsp.rspOffset.type, metaRsp.rspOffset.uid, metaRsp.rspOffset.ts);
      taosMemoryFree(metaRsp.metaRsp);
      tDeleteSTaosxRsp(&taosxRsp);
      return code;
    }

    tqDebug("taosx poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send data blockNum:%d, offset type:%d,uid:%" PRId64
    ",ts:%" PRId64,pRequest->consumerId, pHandle->subKey, vgId, taosxRsp.blockNum, taosxRsp.rspOffset.type, taosxRsp.rspOffset.uid,taosxRsp.rspOffset.ts);
    if (taosxRsp.blockNum > 0) {
      code = tqSendDataRsp(pTq, pMsg, pRequest, (SMqDataRsp*)&taosxRsp, TMQ_MSG_TYPE__TAOSX_RSP);
      tDeleteSTaosxRsp(&taosxRsp);
      return code;
    }else {
      *offset = taosxRsp.rspOffset;
    }
  }


  if (offset->type == TMQ_OFFSET__LOG) {
    int64_t fetchVer = offset->version + 1;
    pCkHead = taosMemoryMalloc(sizeof(SWalCkHead) + 2048);
    if (pCkHead == NULL) {
      tDeleteSTaosxRsp(&taosxRsp);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    walSetReaderCapacity(pHandle->pWalReader, 2048);
    int totalRows = 0;
    while (1) {
      int32_t savedEpoch = atomic_load_32(&pHandle->epoch);
      if (savedEpoch > pRequest->epoch) {
        tqWarn("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey:%s vgId:%d offset %" PRId64
          ", found new consumer epoch %d, discard req epoch %d", pRequest->consumerId, pRequest->epoch, pHandle->subKey, vgId, fetchVer, savedEpoch, pRequest->epoch);
        break;
      }

      if (tqFetchLog(pTq, pHandle, &fetchVer, &pCkHead, pRequest->reqId) < 0) {
        tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer);
        code = tqSendDataRsp(pTq, pMsg, pRequest, (SMqDataRsp*)&taosxRsp, TMQ_MSG_TYPE__TAOSX_RSP);
        tDeleteSTaosxRsp(&taosxRsp);
        taosMemoryFreeClear(pCkHead);
        return code;
      }

      SWalCont* pHead = &pCkHead->head;
      tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, vgId:%d offset %" PRId64 " msgType %d", pRequest->consumerId,
              pRequest->epoch, vgId, fetchVer, pHead->msgType);

      // process meta
      if (pHead->msgType != TDMT_VND_SUBMIT) {
        if(totalRows > 0) {
          tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer - 1);
          code = tqSendDataRsp(pTq, pMsg, pRequest, (SMqDataRsp*)&taosxRsp, TMQ_MSG_TYPE__TAOSX_RSP);
          tDeleteSTaosxRsp(&taosxRsp);
          taosMemoryFreeClear(pCkHead);
          return code;
        }

        tqDebug("fetch meta msg, ver:%" PRId64 ", type:%s", pHead->version, TMSG_INFO(pHead->msgType));
        tqOffsetResetToLog(&metaRsp.rspOffset, fetchVer);
        metaRsp.resMsgType = pHead->msgType;
        metaRsp.metaRspLen = pHead->bodyLen;
        metaRsp.metaRsp = pHead->body;
        if (tqSendMetaPollRsp(pTq, pMsg, pRequest, &metaRsp) < 0) {
          code = -1;
          taosMemoryFreeClear(pCkHead);
          tDeleteSTaosxRsp(&taosxRsp);
          return code;
        }
        code = 0;
        taosMemoryFreeClear(pCkHead);
        tDeleteSTaosxRsp(&taosxRsp);
        return code;
      }

      // process data
      SPackedData submit = {
          .msgStr = POINTER_SHIFT(pHead->body, sizeof(SSubmitReq2Msg)),
          .msgLen = pHead->bodyLen - sizeof(SSubmitReq2Msg),
          .ver = pHead->version,
      };

      if (tqTaosxScanLog(pTq, pHandle, submit, &taosxRsp, &totalRows) < 0) {
        tqError("tmq poll: tqTaosxScanLog error %" PRId64 ", in vgId:%d, subkey %s", pRequest->consumerId, vgId,
                pRequest->subKey);
        taosMemoryFreeClear(pCkHead);
        tDeleteSTaosxRsp(&taosxRsp);
        return -1;
      }

      if (totalRows >= 4096 || taosxRsp.createTableNum > 0) {
        tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer);
        code = tqSendDataRsp(pTq, pMsg, pRequest, (SMqDataRsp*)&taosxRsp, TMQ_MSG_TYPE__TAOSX_RSP);
        tDeleteSTaosxRsp(&taosxRsp);
        taosMemoryFreeClear(pCkHead);
        return code;
      } else {
        fetchVer++;
      }
    }
  }

  tDeleteSTaosxRsp(&taosxRsp);
  taosMemoryFreeClear(pCkHead);
  return 0;
}

static int32_t doPollDataForMq(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg) {
  int32_t      code = -1;
  STqOffsetVal offset = {0};
  STqOffsetVal reqOffset = pRequest->reqOffset;

  // 1. reset the offset if needed
  if (IS_OFFSET_RESET_TYPE(reqOffset.type)) {
    // handle the reset offset cases, according to the consumer's choice.
    bool blockReturned = false;
    code = extractResetOffsetVal(&offset, pTq, pHandle, pRequest, pMsg, &blockReturned);
    if (code != 0) {
      return code;
    }

    // empty block returned, quit
    if (blockReturned) {
      return 0;
    }
  } else { // use the consumer specified offset
    // the offset value can not be monotonious increase??
    offset = reqOffset;
  }

  // this is a normal subscribe requirement
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    return extractDataAndRspForNormalSubscribe(pTq, pHandle, pRequest, pMsg, &offset);
  }

  // todo handle the case where re-balance occurs.
  // for taosx
  return extractDataAndRspForDbStbSubscribe(pTq, pHandle, pRequest, pMsg, &offset);
}

int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg) {
  SMqPollReq   req = {0};
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
    tqDebug("tmq poll: consumer:0x%" PRIx64 " epoch update from %d to %d by poll req", consumerId, savedEpoch, reqEpoch);
    pHandle->epoch = reqEpoch;
  }
  taosWUnLockLatch(&pTq->lock);

  char buf[80];
  tFormatOffset(buf, 80, &reqOffset);
  tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d), subkey %s, recv poll req vgId:%d, req:%s, reqId:0x%" PRIx64,
          consumerId, req.epoch, pHandle->subKey, vgId, buf, req.reqId);

  return doPollDataForMq(pTq, pHandle, &req, pMsg);
}

int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t sversion, char* msg, int32_t msgLen) {
  SMqVDeleteReq* pReq = (SMqVDeleteReq*)msg;

  tqDebug("vgId:%d, tq process delete sub req %s", pTq->pVnode->config.vgId, pReq->subKey);

  taosWLockLatch(&pTq->lock);
  int32_t code = taosHashRemove(pTq->pPushMgr, pReq->subKey, strlen(pReq->subKey));
  if (code != 0) {
    tqDebug("vgId:%d, tq remove push handle %s", pTq->pVnode->config.vgId, pReq->subKey);
  }
  taosWUnLockLatch(&pTq->lock);

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

      pHandle->execHandle.task =
          qCreateQueueExecTaskInfo(pHandle->execHandle.execCol.qmsg, &handle, vgId, &pHandle->execHandle.numOfCols, req.newConsumerId);
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
      atomic_store_32(&pHandle->epoch, -1);

      // remove if it has been register in the push manager, and return one empty block to consumer
      tqUnregisterPushHandle(pTq, req.subKey, (int32_t)strlen(req.subKey), pHandle->consumerId, true);

      atomic_store_64(&pHandle->consumerId, req.newConsumerId);
      atomic_add_fetch_32(&pHandle->epoch, 1);

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
  // todo extract method
  char buf[128] = {0};
  sprintf(buf, "0x%"PRIx64"-%d", pTask->id.streamId, pTask->id.taskId);

  int32_t vgId = TD_VID(pTq->pVnode);
  pTask->id.idStr = taosStrdup(buf);
  pTask->refCnt = 1;
  pTask->schedStatus = TASK_SCHED_STATUS__INACTIVE;
  pTask->inputQueue = streamQueueOpen();
  pTask->outputQueue = streamQueueOpen();

  if (pTask->inputQueue == NULL || pTask->outputQueue == NULL) {
    return -1;
  }

  pTask->inputStatus = TASK_INPUT_STATUS__NORMAL;
  pTask->outputStatus = TASK_OUTPUT_STATUS__NORMAL;
  pTask->pMsgCb = &pTq->pVnode->msgCb;
  pTask->pMeta = pTq->pStreamMeta;

  // expand executor
  if (pTask->fillHistory) {
    pTask->taskStatus = TASK_STATUS__WAIT_DOWNSTREAM;
  } else {
    pTask->taskStatus = TASK_STATUS__RESTORE;
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

    int32_t ver1 = 1;
    SMetaInfo info = {0};
    int32_t code = metaGetInfo(pTq->pVnode->pMeta, pTask->tbSink.stbUid, &info, NULL);
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
    pTask->exec.pTqReader = tqOpenReader(pTq->pVnode);
    if (pTask->exec.pTqReader == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    SArray* pList = qGetQueriedTableListInfo(pTask->exec.pExecutor);
    tqReaderAddTbUidList(pTask->exec.pTqReader, pList);
  }

  streamSetupTrigger(pTask);
  tqInfo("vgId:%d expand stream task, s-task:%s, ver:%" PRId64 " child id:%d, level:%d", vgId, pTask->id.idStr,
         pTask->chkInfo.version, pTask->selfChildId, pTask->taskLevel);
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
  if (pTask && atomic_load_8(&pTask->taskStatus) == TASK_STATUS__NORMAL) {
    rsp.status = 1;
  } else {
    rsp.status = 0;
  }

  if (pTask) streamMetaReleaseTask(pTq->pStreamMeta, pTask);

  tqDebug("tq recv task check req(reqId:0x%" PRIx64 ") %d at node %d check req from task %d at node %d, status %d",
          rsp.reqId, rsp.downstreamTaskId, rsp.downstreamNodeId, rsp.upstreamTaskId, rsp.upstreamNodeId, rsp.status);

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

  SRpcMsg rspMsg = {
      .code = 0,
      .pCont = buf,
      .contLen = sizeof(SMsgHead) + len,
      .info = pMsg->info,
  };

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

  // 2.save task
  code = streamMetaAddTask(pTq->pStreamMeta, sversion, pTask);
  if (code < 0) {
    return -1;
  }

  // 3.go through recover steps to fill history
  if (pTask->fillHistory) {
    streamTaskCheckDownstream(pTask, sversion);
  }

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

  if (atomic_load_8(&pTask->taskStatus) == TASK_STATUS__DROPPING) {
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

  if (atomic_load_8(&pTask->taskStatus) == TASK_STATUS__DROPPING) {
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

  if (atomic_load_8(&pTask->taskStatus) == TASK_STATUS__DROPPING) {
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

  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) continue;

    qDebug("delete req enqueue stream task: %d, ver: %" PRId64, pTask->id.taskId, ver);

    if (!failed) {
      SStreamRefDataBlock* pRefBlock = taosAllocateQitem(sizeof(SStreamRefDataBlock), DEF_QITEM, 0);
      pRefBlock->type = STREAM_INPUT__REF_DATA_BLOCK;
      pRefBlock->pBlock = pDelBlock;
      pRefBlock->dataRef = pRef;
      atomic_add_fetch_32(pRefBlock->dataRef, 1);

      if (tAppendDataForStream(pTask, (SStreamQueueItem*)pRefBlock) < 0) {
        qError("stream task input del failed, task id %d", pTask->id.taskId);

        atomic_sub_fetch_32(pRef, 1);
        taosFreeQitem(pRefBlock);
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

  int32_t ref = atomic_sub_fetch_32(pRef, 1);
  /*A(ref >= 0);*/
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
      if (tAppendDataForStream(pTask, (SStreamQueueItem*)pStreamBlock) < 0) {
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

static int32_t addSubmitBlockNLaunchTask(STqOffsetStore* pOffsetStore, SStreamTask* pTask, SStreamDataSubmit2* pSubmit,
                                         const char* key, int64_t ver) {
  doSaveTaskOffset(pOffsetStore, key, ver);
  int32_t code = tqAddInputBlockNLaunchTask(pTask, (SStreamQueueItem*)pSubmit, ver);

  // remove the offset, if all functions are completed successfully.
  if (code == TSDB_CODE_SUCCESS) {
    tqOffsetDelete(pOffsetStore, key);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tqProcessSubmitReq(STQ* pTq, SPackedData submit) {
  void* pIter = NULL;

  SStreamDataSubmit2* pSubmit = streamDataSubmitNew(submit, STREAM_INPUT__DATA_SUBMIT);
  if (pSubmit == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("failed to create data submit for stream since out of memory");
    saveOffsetForAllTasks(pTq, submit.ver);
    return -1;
  }

  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel != TASK_LEVEL__SOURCE) {
      continue;
    }

    if (pTask->taskStatus == TASK_STATUS__RECOVER_PREPARE || pTask->taskStatus == TASK_STATUS__WAIT_DOWNSTREAM) {
      tqDebug("stream task:%d skip push data, not ready for processing, status %d", pTask->id.taskId,
              pTask->taskStatus);
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

      tqDebug("s-task:%s input queue is full, do nothing, start ver:%" PRId64, pTask->id.idStr, ver);
      continue;
    }

    // check if offset value exists
    STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, key);
    if (pOffset != NULL) {
      // seek the stored version and extract data from WAL
      int32_t code = tqSeekVer(pTask->exec.pTqReader, pOffset->val.version, "");

      // all data has been retrieved from WAL, let's try submit block directly.
      if (code == TSDB_CODE_SUCCESS) {  // all data retrieved, abort
        // append the data for the stream
        SFetchRet ret = {.data.info.type = STREAM_NORMAL};
        terrno = 0;

        tqNextBlock(pTask->exec.pTqReader, &ret);
        if (ret.fetchType == FETCH_TYPE__DATA) {
          code = launchTaskForWalBlock(pTask, &ret, pOffset);
          if (code != TSDB_CODE_SUCCESS) {
            continue;
          }
        } else {  // FETCH_TYPE__NONE, let's try submit block directly
          tqDebug("s-task:%s data in WAL are all consumed, try data in submit message", pTask->id.idStr);
          addSubmitBlockNLaunchTask(pTq->pOffsetStore, pTask, pSubmit, key, submit.ver);
        }

        // do nothing if failed, since the offset value is kept already
      } else {  // failed to seek to the WAL version
        // todo handle the case where offset has been deleted in WAL, due to stream computing too slow
        tqDebug("s-task:%s data in WAL are all consumed, try data in submit msg", pTask->id.idStr);
        addSubmitBlockNLaunchTask(pTq->pOffsetStore, pTask, pSubmit, key, submit.ver);
      }
    } else {
      addSubmitBlockNLaunchTask(pTq->pOffsetStore, pTask, pSubmit, key, submit.ver);
    }
  }

  streamDataSubmitDestroy(pSubmit);
  taosFreeQitem(pSubmit);

  return 0;
}

int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg) {
  SStreamTaskRunReq* pReq = pMsg->pCont;

  int32_t taskId = pReq->taskId;
  int32_t vgId = TD_VID(pTq->pVnode);

  if (taskId == ALL_STREAM_TASKS_ID) {  // all tasks are restored from the wal
    tqDoRestoreSourceStreamTasks(pTq);
    return 0;
  } else {
    SStreamTask* pTask = streamMetaAcquireTaskEx(pTq->pStreamMeta, taskId);
    if (pTask != NULL) {
      if (pTask->taskStatus == TASK_STATUS__NORMAL) {
        tqDebug("vgId:%d s-task:%s start to process run req", vgId, pTask->id.idStr);
        streamProcessRunReq(pTask);
      } else if (pTask->taskStatus == TASK_STATUS__RESTORE) {
        tqDebug("vgId:%d s-task:%s start to process in restore procedure from last chk point:%" PRId64, vgId,
                pTask->id.idStr, pTask->chkInfo.version);
        streamProcessRunReq(pTask);
      } else {
        tqDebug("vgId:%d s-task:%s ignore run req since not in ready state", vgId, pTask->id.idStr);
      }

      streamMetaReleaseTask(pTq->pStreamMeta, pTask);
      return 0;
    } else {
      tqError("vgId:%d failed to found s-task, taskId:%d", vgId, taskId);
      return -1;
    }
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
  int32_t taskId = req.taskId;

  SStreamTask* pTask = streamMetaAcquireTask(pTq->pStreamMeta, taskId);
  if (pTask) {
    SRpcMsg rsp = {
        .info = pMsg->info,
        .code = 0,
    };
    streamProcessDispatchReq(pTask, &req, &rsp, exec);
    streamMetaReleaseTask(pTq->pStreamMeta, pTask);
    return 0;
  } else {
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

int32_t tqRestoreStreamTasks(STQ* pTq) {
  int32_t vgId = TD_VID(pTq->pVnode);

  SStreamTaskRunReq* pRunReq = rpcMallocCont(sizeof(SStreamTaskRunReq));
  if (pRunReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d failed restore stream tasks, code:%s", vgId, terrstr(terrno));
    return -1;
  }

  int32_t numOfTasks = taosHashGetSize(pTq->pStreamMeta->pRestoreTasks);
  tqInfo("vgId:%d start restoring stream tasks, total tasks:%d", vgId, numOfTasks);
  initOffsetForAllRestoreTasks(pTq);

  pRunReq->head.vgId = vgId;
  pRunReq->streamId = 0;
  pRunReq->taskId = ALL_STREAM_TASKS_ID;

  SRpcMsg msg = {.msgType = TDMT_STREAM_TASK_RUN, .pCont = pRunReq, .contLen = sizeof(SStreamTaskRunReq)};
  tmsgPutToQueue(&pTq->pVnode->msgCb, STREAM_QUEUE, &msg);

  return 0;
}
