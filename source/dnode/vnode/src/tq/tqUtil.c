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

static int32_t tqSendMetaPollRsp(STQ* pTq, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqMetaRsp* pRsp);

char* createStreamTaskIdStr(int64_t streamId, int32_t taskId) {
  char buf[128] = {0};
  sprintf(buf, "0x%" PRIx64 "-%d", streamId, taskId);
  return taosStrdup(buf);
}

int32_t tqAddInputBlockNLaunchTask(SStreamTask* pTask, SStreamQueueItem* pQueueItem, int64_t ver) {
  int32_t code = tAppendDataToInputQueue(pTask, pQueueItem);
  if (code < 0) {
    tqError("s-task:%s failed to put into queue, too many, next start ver:%" PRId64, pTask->id.idStr, ver);
    return -1;
  }

  if (streamSchedExec(pTask) < 0) {
    tqError("stream task:%d failed to be launched, code:%s", pTask->id.taskId, tstrerror(terrno));
    return -1;
  }

  return TSDB_CODE_SUCCESS;
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
    if (pRsp->blockData != NULL) {
      pRsp->blockData = taosArrayDestroy(pRsp->blockData);
    }

    if (pRsp->blockDataLen != NULL) {
      pRsp->blockDataLen = taosArrayDestroy(pRsp->blockDataLen);
    }

    if (pRsp->blockTbName != NULL) {
      pRsp->blockTbName = taosArrayDestroy(pRsp->blockTbName);
    }

    if (pRsp->blockSchema != NULL) {
      pRsp->blockSchema = taosArrayDestroy(pRsp->blockSchema);
    }
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

  qSetTaskId(pHandle->execHandle.task, consumerId, pRequest->reqId);
  int code = tqScanData(pTq, pHandle, &dataRsp, pOffset);
  if(code != 0) {
    goto end;
  }

//   till now, all data has been transferred to consumer, new data needs to push client once arrived.
  if (dataRsp.blockNum == 0 && dataRsp.reqOffset.type == TMQ_OFFSET__LOG &&
      dataRsp.reqOffset.version == dataRsp.rspOffset.version && pHandle->consumerId == pRequest->consumerId) {
//    code = tqRegisterPushHandle(pTq, pHandle, pRequest, pMsg, &dataRsp, TMQ_MSG_TYPE__POLL_RSP);
    // lock
    taosWLockLatch(&pTq->lock);
//    tqDebug("data is over, register to handle:%p, msg:%p", pHandle, pHandle->msg);
    if(pHandle->msg == NULL){
      pHandle->msg = taosMemoryCalloc(1, sizeof(SRpcMsg));
    }

    memcpy(pHandle->msg, pMsg, sizeof(SRpcMsg));
    pHandle->msg->pCont = rpcMallocCont(pMsg->contLen);
    memcpy(pHandle->msg->pCont, pMsg->pCont, pMsg->contLen);
    pHandle->msg->contLen = pMsg->contLen;
    tqDebug("data is over, register to handle:%p, pCont:%p, len:%d", pHandle, pHandle->msg->pCont, pHandle->msg->contLen);
    taosHashPut(pTq->pPushMgr, &pHandle->consumerId, sizeof(int64_t), &pHandle, POINTER_BYTES);
    taosWUnLockLatch(&pTq->lock);
    tDeleteSMqDataRsp(&dataRsp);
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
//    taosWUnLockLatch(&pTq->lock);
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
      tDeleteSTaosxRsp(&taosxRsp);
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

int32_t tqExtractDataForMq(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg) {
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
