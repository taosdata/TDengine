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

static int32_t tqSendMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq,
                                 const SMqMetaRsp* pRsp, int32_t vgId);
static int32_t tqSendBatchMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq,
                                      const SMqBatchMetaRsp* pRsp, int32_t vgId);

int32_t tqInitDataRsp(SMqDataRsp* pRsp, STqOffsetVal pOffset) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  tqDebug("%s called", __FUNCTION__ );
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);

  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockData, code, lino, END, terrno);

  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));
  TSDB_CHECK_NULL(pRsp->blockDataLen, code, lino, END, terrno);

  tOffsetCopy(&pRsp->reqOffset, &pOffset);
  tOffsetCopy(&pRsp->rspOffset, &pOffset);
  pRsp->withTbName = 0;
  pRsp->withSchema = false;

END:
  if (code != 0){
    tqError("%s failed at:%d, code:%s", __FUNCTION__ , lino, tstrerror(code));
  }
  return code;
}

static int32_t tqInitTaosxRsp(SMqDataRsp* pRsp, STqOffsetVal pOffset) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  tqDebug("%s called", __FUNCTION__ );
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  tOffsetCopy(&pRsp->reqOffset, &pOffset);
  tOffsetCopy(&pRsp->rspOffset, &pOffset);

  pRsp->withTbName = 1;
  pRsp->withSchema = 1;
  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockData, code, lino, END, terrno);\

  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));
  TSDB_CHECK_NULL(pRsp->blockDataLen, code, lino, END, terrno);

  pRsp->blockTbName = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockTbName, code, lino, END, terrno);

  pRsp->blockSchema = taosArrayInit(0, sizeof(void*));
  TSDB_CHECK_NULL(pRsp->blockSchema, code, lino, END, terrno);

END:
  if (code != 0){
    tqError("%s failed at:%d, code:%s", __FUNCTION__ , lino, tstrerror(code));
    taosArrayDestroy(pRsp->blockData);
    taosArrayDestroy(pRsp->blockDataLen);
    taosArrayDestroy(pRsp->blockTbName);
    taosArrayDestroy(pRsp->blockSchema);
  }
  return code;
}

static int32_t extractResetOffsetVal(STqOffsetVal* pOffsetVal, STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                     SRpcMsg* pMsg, bool* pBlockReturned) {
  if (pOffsetVal == NULL || pTq == NULL || pHandle == NULL || pRequest == NULL || pMsg == NULL || pBlockReturned == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  uint64_t   consumerId = pRequest->consumerId;
  STqOffset* pOffset = NULL;
  int32_t    code = tqMetaGetOffset(pTq, pRequest->subKey, &pOffset);
  int32_t    vgId = TD_VID(pTq->pVnode);

  *pBlockReturned = false;
  // In this vnode, data has been polled by consumer for this topic, so let's continue from the last offset value.
  if (code == 0) {
    tOffsetCopy(pOffsetVal, &pOffset->val);

    char formatBuf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(formatBuf, TSDB_OFFSET_LEN, pOffsetVal);
    tqDebug("tmq poll: consumer:0x%" PRIx64
                ", subkey %s, vgId:%d, existed offset found, offset reset to %s and continue.QID:0x%" PRIx64,
            consumerId, pHandle->subKey, vgId, formatBuf, pRequest->reqId);
    return 0;
  } else {
    // no poll occurs in this vnode for this topic, let's seek to the right offset value.
    if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_EARLIEST) {
      if (pRequest->useSnapshot) {
        tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey:%s, vgId:%d, (earliest) set offset to be snapshot",
                consumerId, pHandle->subKey, vgId);
        if (pHandle->fetchMeta) {
          tqOffsetResetToMeta(pOffsetVal, 0);
        } else {
          SValue val = {0};
          tqOffsetResetToData(pOffsetVal, 0, 0, val);
        }
      } else {
        walRefFirstVer(pTq->pVnode->pWal, pHandle->pRef);
        tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer);
      }
    } else if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
      walRefLastVer(pTq->pVnode->pWal, pHandle->pRef);
      SMqDataRsp dataRsp = {0};
      tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer + 1);

      code = tqInitDataRsp(&dataRsp, *pOffsetVal);
      if (code != 0) {
        return code;
      }
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, (latest) offset reset to %" PRId64, consumerId,
              pHandle->subKey, vgId, dataRsp.rspOffset.version);
      code = tqSendDataRsp(pHandle, pMsg, pRequest, &dataRsp, (pRequest->rawData == 1) ? TMQ_MSG_TYPE__POLL_RAW_DATA_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
      tDeleteMqDataRsp(&dataRsp);

      *pBlockReturned = true;
      return code;
    } else if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_NONE) {
      tqError("tmq poll: subkey:%s, no offset committed for consumer:0x%" PRIx64
                  " in vg %d, subkey %s, reset none failed",
              pHandle->subKey, consumerId, vgId, pRequest->subKey);
      return TSDB_CODE_TQ_NO_COMMITTED_OFFSET;
    }
  }

  return 0;
}

static int32_t extractDataAndRspForNormalSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                                   SRpcMsg* pMsg, STqOffsetVal* pOffset) {
  int32_t    code = TDB_CODE_SUCCESS;
  int32_t    lino = 0;
  tqDebug("%s called", __FUNCTION__ );
  uint64_t consumerId = pRequest->consumerId;
  int32_t  vgId = TD_VID(pTq->pVnode);
  terrno = 0;

  SMqDataRsp dataRsp = {0};
  code = tqInitDataRsp(&dataRsp, *pOffset);
  TSDB_CHECK_CODE(code, lino, end);

  code = qSetTaskId(pHandle->execHandle.task, consumerId, pRequest->reqId);
  TSDB_CHECK_CODE(code, lino, end);

  code = tqScanData(pTq, pHandle, &dataRsp, pOffset, pRequest);
  if (code != 0 && terrno != TSDB_CODE_WAL_LOG_NOT_EXIST) {
    goto end;
  }

  //   till now, all data has been transferred to consumer, new data needs to push client once arrived.
  if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST && dataRsp.blockNum == 0) {
    // lock
    taosWLockLatch(&pTq->lock);
    int64_t ver = walGetCommittedVer(pTq->pVnode->pWal);
    if (dataRsp.rspOffset.version > ver) {  // check if there are data again to avoid lost data
      code = tqRegisterPushHandle(pTq, pHandle, pMsg);
      taosWUnLockLatch(&pTq->lock);
      goto end;
    }
    taosWUnLockLatch(&pTq->lock);
  }

  // reqOffset represents the current date offset, may be changed if wal not exists
  tOffsetCopy(&dataRsp.reqOffset, pOffset);
  code = tqSendDataRsp(pHandle, pMsg, pRequest, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);

end:
  {
    char buf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.rspOffset);
    if (code != 0){
      tqError("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, rsp block:%d, rsp offset type:%s, QID:0x%" PRIx64 " error msg:%s, line:%d",
              consumerId, pHandle->subKey, vgId, dataRsp.blockNum, buf, pRequest->reqId, tstrerror(code), lino);
    } else {
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, rsp block:%d, rsp offset type:%s, QID:0x%" PRIx64 " success",
              consumerId, pHandle->subKey, vgId, dataRsp.blockNum, buf, pRequest->reqId);
    }

    tDeleteMqDataRsp(&dataRsp);
    return code;
  }
}

#define PROCESS_EXCLUDED_MSG(TYPE, DECODE_FUNC, DELETE_FUNC)                                               \
  SDecoder decoder = {0};                                                                                  \
  TYPE     req = {0};                                                                                      \
  void*    data = POINTER_SHIFT(pHead->body, sizeof(SMsgHead));                                            \
  int32_t  len = pHead->bodyLen - sizeof(SMsgHead);                                                        \
  tDecoderInit(&decoder, data, len);                                                                       \
  if (DECODE_FUNC(&decoder, &req) == 0 && (req.source & TD_REQ_FROM_TAOX) != 0) {                          \
    tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, jump meta for, vgId:%d offset %" PRId64 \
            " msgType %d",                                                                                 \
            pRequest->consumerId, pRequest->epoch, vgId, fetchVer, pHead->msgType);                        \
    fetchVer++;                                                                                            \
    DELETE_FUNC(&req);                                                                                     \
    tDecoderClear(&decoder);                                                                               \
    continue;                                                                                              \
  }                                                                                                        \
  DELETE_FUNC(&req);                                                                                       \
  tDecoderClear(&decoder);

static void tDeleteCommon(void* parm) {}
static void tDeleteAlterTable(SVAlterTbReq* req) {
  taosArrayDestroy(req->pMultiTag);
}

#define POLL_RSP_TYPE(pRequest,taosxRsp) \
taosxRsp.createTableNum > 0 ? TMQ_MSG_TYPE__POLL_DATA_META_RSP : \
(pRequest->rawData == 1 ? TMQ_MSG_TYPE__POLL_RAW_DATA_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP)

static int32_t buildBatchMeta(SMqBatchMetaRsp *btMetaRsp, int16_t type, int32_t bodyLen, void* body){
  int32_t         code = 0;

  if (!btMetaRsp->batchMetaReq) {
    btMetaRsp->batchMetaReq = taosArrayInit(4, POINTER_BYTES);
    TQ_NULL_GO_TO_END(btMetaRsp->batchMetaReq);
    btMetaRsp->batchMetaLen = taosArrayInit(4, sizeof(int32_t));
    TQ_NULL_GO_TO_END(btMetaRsp->batchMetaLen);
  }

  SMqMetaRsp tmpMetaRsp = {0};
  tmpMetaRsp.resMsgType = type;
  tmpMetaRsp.metaRspLen = bodyLen;
  tmpMetaRsp.metaRsp = body;
  uint32_t len = 0;
  tEncodeSize(tEncodeMqMetaRsp, &tmpMetaRsp, len, code);
  if (TSDB_CODE_SUCCESS != code) {
    tqError("tmq extract meta from log, tEncodeMqMetaRsp error");
    goto END;
  }
  int32_t tLen = sizeof(SMqRspHead) + len;
  void*   tBuf = taosMemoryCalloc(1, tLen);
  TQ_NULL_GO_TO_END(tBuf);
  void*    metaBuff = POINTER_SHIFT(tBuf, sizeof(SMqRspHead));
  SEncoder encoder = {0};
  tEncoderInit(&encoder, metaBuff, len);
  code = tEncodeMqMetaRsp(&encoder, &tmpMetaRsp);
  tEncoderClear(&encoder);

  if (code < 0) {
    tqError("tmq extract meta from log, tEncodeMqMetaRsp error");
    goto END;
  }
  TQ_NULL_GO_TO_END (taosArrayPush(btMetaRsp->batchMetaReq, &tBuf));
  TQ_NULL_GO_TO_END (taosArrayPush(btMetaRsp->batchMetaLen, &tLen));

END:
  return code;
}

static int32_t buildCreateTbBatchReqBinary(SMqDataRsp *taosxRsp, void** pBuf, int32_t *len){
  int32_t code = 0;
  SVCreateTbBatchReq pReq = {0};
  pReq.nReqs = taosArrayGetSize(taosxRsp->createTableReq);
  pReq.pArray = taosArrayInit(1, sizeof(struct SVCreateTbReq));
  TQ_NULL_GO_TO_END(pReq.pArray);
  for (int i = 0; i < taosArrayGetSize(taosxRsp->createTableReq); i++){
    void   *createTableReq = taosArrayGetP(taosxRsp->createTableReq, i);
    TQ_NULL_GO_TO_END(taosArrayPush(pReq.pArray, createTableReq));
  }
  tEncodeSize(tEncodeSVCreateTbBatchReq, &pReq, *len, code);
  if (code < 0) {
    goto END;
  }
  *len += sizeof(SMsgHead);
  *pBuf = taosMemoryMalloc(*len);
  TQ_NULL_GO_TO_END(pBuf);
  SEncoder coder = {0};
  tEncoderInit(&coder, POINTER_SHIFT(*pBuf, sizeof(SMsgHead)), *len);
  code = tEncodeSVCreateTbBatchReq(&coder, &pReq);
  tEncoderClear(&coder);

END:
  taosArrayDestroy(pReq.pArray);
  return code;
}

#define SEND_BATCH_META_RSP \
tqOffsetResetToLog(&btMetaRsp.rspOffset, fetchVer);\
code = tqSendBatchMetaPollRsp(pHandle, pMsg, pRequest, &btMetaRsp, vgId);\
goto END;

#define SEND_DATA_RSP \
tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer);\
code = tqSendDataRsp(pHandle, pMsg, pRequest, &taosxRsp, POLL_RSP_TYPE(pRequest, taosxRsp), vgId);\
goto END;
static int32_t extractDataAndRspForDbStbSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                                  SRpcMsg* pMsg, STqOffsetVal* offset) {
  int32_t         vgId = TD_VID(pTq->pVnode);
  SMqDataRsp      taosxRsp = {0};
  SMqBatchMetaRsp btMetaRsp = {0};
  int32_t         code = 0;

  TQ_ERR_GO_TO_END(tqInitTaosxRsp(&taosxRsp, *offset));
  if (offset->type != TMQ_OFFSET__LOG) {
    TQ_ERR_GO_TO_END(tqScanTaosx(pTq, pHandle, &taosxRsp, &btMetaRsp, offset, pRequest));

    if (taosArrayGetSize(btMetaRsp.batchMetaReq) > 0) {
      code = tqSendBatchMetaPollRsp(pHandle, pMsg, pRequest, &btMetaRsp, vgId);
      tqDebug("tmq poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send meta offset type:%d,uid:%" PRId64 ",ts:%" PRId64,
              pRequest->consumerId, pHandle->subKey, vgId, btMetaRsp.rspOffset.type, btMetaRsp.rspOffset.uid,btMetaRsp.rspOffset.ts);
      goto END;
    }

    tqDebug("taosx poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send data blockNum:%d, offset type:%d,uid:%" PRId64",ts:%" PRId64,
            pRequest->consumerId, pHandle->subKey, vgId, taosxRsp.blockNum, taosxRsp.rspOffset.type, taosxRsp.rspOffset.uid, taosxRsp.rspOffset.ts);
    if (taosxRsp.blockNum > 0) {
      code = tqSendDataRsp(pHandle, pMsg, pRequest, &taosxRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
      goto END;
    } else {
      tOffsetCopy(offset, &taosxRsp.rspOffset);
    }
  }

  if (offset->type == TMQ_OFFSET__LOG) {
    walReaderVerifyOffset(pHandle->pWalReader, offset);
    int64_t fetchVer = offset->version;

    uint64_t st = taosGetTimestampMs();
    int      totalRows = 0;
    int32_t  totalMetaRows = 0;
    while (1) {
      int32_t savedEpoch = atomic_load_32(&pHandle->epoch);
      if (savedEpoch > pRequest->epoch) {
        tqError("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, savedEpoch error, vgId:%d offset %" PRId64,
                pRequest->consumerId, pRequest->epoch, vgId, fetchVer);
        code = TSDB_CODE_TQ_INTERNAL_ERROR;
        goto END;
      }

      if (tqFetchLog(pTq, pHandle, &fetchVer, pRequest->reqId) < 0) {
        if (totalMetaRows > 0) {
          SEND_BATCH_META_RSP
        }
        SEND_DATA_RSP
      }

      SWalCont* pHead = &pHandle->pWalReader->pHead->head;
      tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, vgId:%d offset %" PRId64 " msgType %s",
              pRequest->consumerId, pRequest->epoch, vgId, fetchVer, TMSG_INFO(pHead->msgType));

      // process meta
      if (pHead->msgType != TDMT_VND_SUBMIT) {
        if (totalRows > 0) {
          SEND_DATA_RSP
        }

        if ((pRequest->sourceExcluded & TD_REQ_FROM_TAOX) != 0) {
          if (pHead->msgType == TDMT_VND_CREATE_TABLE) {
            PROCESS_EXCLUDED_MSG(SVCreateTbBatchReq, tDecodeSVCreateTbBatchReq, tDeleteSVCreateTbBatchReq)
          } else if (pHead->msgType == TDMT_VND_ALTER_TABLE) {
            PROCESS_EXCLUDED_MSG(SVAlterTbReq, tDecodeSVAlterTbReq, tDeleteAlterTable)
          } else if (pHead->msgType == TDMT_VND_CREATE_STB || pHead->msgType == TDMT_VND_ALTER_STB) {
            PROCESS_EXCLUDED_MSG(SVCreateStbReq, tDecodeSVCreateStbReq, tDeleteCommon)
          } else if (pHead->msgType == TDMT_VND_DELETE) {
            PROCESS_EXCLUDED_MSG(SDeleteRes, tDecodeDeleteRes, tDeleteCommon)
          }
        }

        tqDebug("fetch meta msg, ver:%" PRId64 ", vgId:%d, type:%s, enable batch meta:%d", pHead->version, vgId,
                TMSG_INFO(pHead->msgType), pRequest->enableBatchMeta);
        if (!pRequest->enableBatchMeta && !pRequest->useSnapshot) {
          SMqMetaRsp metaRsp = {0};
          tqOffsetResetToLog(&metaRsp.rspOffset, fetchVer + 1);
          metaRsp.resMsgType = pHead->msgType;
          metaRsp.metaRspLen = pHead->bodyLen;
          metaRsp.metaRsp = pHead->body;
          code = tqSendMetaPollRsp(pHandle, pMsg, pRequest, &metaRsp, vgId);
          goto END;
        }
        code = buildBatchMeta(&btMetaRsp, pHead->msgType, pHead->bodyLen, pHead->body);
        fetchVer++;
        if (code != 0){
          goto END;
        }
        totalMetaRows++;
        if ((taosArrayGetSize(btMetaRsp.batchMetaReq) >= tmqRowSize) || (taosGetTimestampMs() - st > pRequest->timeout)) {
          SEND_BATCH_META_RSP
        }
        continue;
      }

      if (totalMetaRows > 0 && pHandle->fetchMeta != ONLY_META) {
        SEND_BATCH_META_RSP
      }

      // process data
      SPackedData submit = {
          .msgStr = POINTER_SHIFT(pHead->body, sizeof(SSubmitReq2Msg)),
          .msgLen = pHead->bodyLen - sizeof(SSubmitReq2Msg),
          .ver = pHead->version,
      };

      TQ_ERR_GO_TO_END(tqTaosxScanLog(pTq, pHandle, submit, &taosxRsp, &totalRows, pRequest));

      if (pHandle->fetchMeta == ONLY_META && taosArrayGetSize(taosxRsp.createTableReq) > 0){
        int32_t len = 0;
        void *pBuf = NULL;
        code = buildCreateTbBatchReqBinary(&taosxRsp, &pBuf, &len);
        if (code == 0){
          code = buildBatchMeta(&btMetaRsp, TDMT_VND_CREATE_TABLE, len, pBuf);
        }
        taosMemoryFree(pBuf);
        for (int i = 0; i < taosArrayGetSize(taosxRsp.createTableReq); i++) {
          void* pCreateTbReq = taosArrayGetP(taosxRsp.createTableReq, i);
          if (pCreateTbReq != NULL) {
            tDestroySVSubmitCreateTbReq(pCreateTbReq, TSDB_MSG_FLG_DECODE);
          }
          taosMemoryFree(pCreateTbReq);
        }
        taosArrayDestroy(taosxRsp.createTableReq);
        taosxRsp.createTableReq = NULL;
        fetchVer++;
        if (code != 0){
          goto END;
        }
        totalMetaRows++;
        if ((taosArrayGetSize(btMetaRsp.batchMetaReq) >= tmqRowSize) ||
            (taosGetTimestampMs() - st > pRequest->timeout) ||
            (!pRequest->enableBatchMeta && !pRequest->useSnapshot)) {
          SEND_BATCH_META_RSP
        }
        continue;
      }

      if ((pRequest->rawData == 0 && totalRows >= pRequest->minPollRows) ||
          (taosGetTimestampMs() - st > pRequest->timeout) ||
          (pRequest->rawData != 0 && (taosArrayGetSize(taosxRsp.blockData) > pRequest->minPollRows ||
                                      terrno == TSDB_CODE_TMQ_RAW_DATA_SPLIT))) {
        if (terrno == TSDB_CODE_TMQ_RAW_DATA_SPLIT){
          terrno = 0;
        } else{
          fetchVer++;
        }
        SEND_DATA_RSP
      } else {
        fetchVer++;
      }
    }
  }

END:
  if (code != 0){
    tqError("tmq poll: tqTaosxScanLog error. consumerId:0x%" PRIx64 ", in vgId:%d, subkey %s", pRequest->consumerId, vgId,
            pRequest->subKey);
  }
  tDeleteMqBatchMetaRsp(&btMetaRsp);
  tDeleteSTaosxRsp(&taosxRsp);
  return code;
}

int32_t tqExtractDataForMq(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg) {
  if (pTq == NULL || pHandle == NULL || pRequest == NULL || pMsg == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t      code = 0;
  STqOffsetVal reqOffset = {0};
  tOffsetCopy(&reqOffset, &pRequest->reqOffset);

  // reset the offset if needed
  if (IS_OFFSET_RESET_TYPE(pRequest->reqOffset.type)) {
    bool blockReturned = false;
    code = extractResetOffsetVal(&reqOffset, pTq, pHandle, pRequest, pMsg, &blockReturned);
    if (code != 0) {
      goto END;
    }

    // empty block returned, quit
    if (blockReturned) {
      goto END;
    }
  } else if (reqOffset.type == 0) {  // use the consumer specified offset
    uError("req offset type is 0");
    code = TSDB_CODE_TMQ_INVALID_MSG;
    goto END;
  }

  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    code = extractDataAndRspForNormalSubscribe(pTq, pHandle, pRequest, pMsg, &reqOffset);
  } else {
    code = extractDataAndRspForDbStbSubscribe(pTq, pHandle, pRequest, pMsg, &reqOffset);
  }

END:
  if (code != 0){
    uError("failed to extract data for mq, msg:%s", tstrerror(code));
  }
  tOffsetDestroy(&reqOffset);
  return code;
}

static void initMqRspHead(SMqRspHead* pMsgHead, int32_t type, int32_t epoch, int64_t consumerId, int64_t sver,
                          int64_t ever) {
  if (pMsgHead == NULL) {
    return;
  }
  pMsgHead->consumerId = consumerId;
  pMsgHead->epoch = epoch;
  pMsgHead->mqMsgType = type;
  pMsgHead->walsver = sver;
  pMsgHead->walever = ever;
}

int32_t tqSendBatchMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq,
                               const SMqBatchMetaRsp* pRsp, int32_t vgId) {
  if (pHandle == NULL || pMsg == NULL || pReq == NULL || pRsp == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqBatchMetaRsp, pRsp, len, code);
  if (code < 0) {
    return TAOS_GET_TERRNO(code);
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return TAOS_GET_TERRNO(terrno);
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  initMqRspHead(buf, TMQ_MSG_TYPE__POLL_BATCH_META_RSP, pReq->epoch, pReq->consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeMqBatchMetaRsp(&encoder, pRsp);
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(code);
  }
  SRpcMsg resp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&resp);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) send rsp, res msg type: batch meta, size:%ld offset type:%d",
          vgId, pReq->consumerId, pReq->epoch, taosArrayGetSize(pRsp->batchMetaReq), pRsp->rspOffset.type);

  return 0;
}

int32_t tqSendMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqMetaRsp* pRsp,
                          int32_t vgId) {
  if (pHandle == NULL || pMsg == NULL || pReq == NULL || pRsp == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqMetaRsp, pRsp, len, code);
  if (code < 0) {
    return TAOS_GET_TERRNO(code);
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return TAOS_GET_TERRNO(TSDB_CODE_OUT_OF_MEMORY);
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  initMqRspHead(buf, TMQ_MSG_TYPE__POLL_META_RSP, pReq->epoch, pReq->consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  code = tEncodeMqMetaRsp(&encoder, pRsp);
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(code);
  }

  SRpcMsg resp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&resp);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) send rsp, res msg type %d, offset type:%d", vgId,
          pReq->consumerId, pReq->epoch, pRsp->resMsgType, pRsp->rspOffset.type);

  return 0;
}

int32_t tqDoSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, SMqDataRsp* pRsp, int32_t epoch, int64_t consumerId,
                        int32_t type, int64_t sver, int64_t ever) {
  if (pRpcHandleInfo == NULL || pRsp == NULL) {
    return TSDB_CODE_TMQ_INVALID_MSG;
  }
  int32_t len = 0;
  int32_t code = 0;

  if (type == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP){
    pRsp->withSchema = 0;
  }
  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP ||
      type == TMQ_MSG_TYPE__WALINFO_RSP ||
      type == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    tEncodeSize(tEncodeMqDataRsp, pRsp, len, code);
  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    tEncodeSize(tEncodeSTaosxRsp, pRsp, len, code);
  }

  if (code < 0) {
    return TAOS_GET_TERRNO(code);
  }

  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return terrno;
  }

  SMqRspHead* pHead = (SMqRspHead*)buf;
  initMqRspHead(pHead, type, epoch, consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);

  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP ||
      type == TMQ_MSG_TYPE__WALINFO_RSP ||
      type == TMQ_MSG_TYPE__POLL_RAW_DATA_RSP) {
    code = tEncodeMqDataRsp(&encoder, pRsp);
  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    code = tEncodeSTaosxRsp(&encoder, pRsp);
  }
  tEncoderClear(&encoder);
  if (code < 0) {
    rpcFreeCont(buf);
    return TAOS_GET_TERRNO(code);
  }
  SRpcMsg rsp = {.info = *pRpcHandleInfo, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&rsp);
  return 0;
}
