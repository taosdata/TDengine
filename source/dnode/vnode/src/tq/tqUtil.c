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

int32_t tqInitDataRsp(SMqDataRsp* pRsp, STqOffsetVal pOffset) {
  pRsp->reqOffset = pOffset;
  pRsp->rspOffset = pOffset;

  pRsp->blockData = taosArrayInit(0, sizeof(void*));
  pRsp->blockDataLen = taosArrayInit(0, sizeof(int32_t));

  if (pRsp->blockData == NULL || pRsp->blockDataLen == NULL) {
    return -1;
  }

  pRsp->withTbName = 0;
  pRsp->withSchema = false;
  return 0;
}

void tqUpdateNodeStage(STQ* pTq, bool isLeader) {
  SSyncState state = syncGetState(pTq->pVnode->sync);
  streamMetaUpdateStageRole(pTq->pStreamMeta, state.term, isLeader);
}

static int32_t tqInitTaosxRsp(STaosxRsp* pRsp, STqOffsetVal pOffset) {
  pRsp->reqOffset = pOffset;
  pRsp->rspOffset = pOffset;

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
  uint64_t   consumerId = pRequest->consumerId;
  STqOffset* pOffset = tqOffsetRead(pTq->pOffsetStore, pRequest->subKey);
  int32_t    vgId = TD_VID(pTq->pVnode);

  *pBlockReturned = false;
  // In this vnode, data has been polled by consumer for this topic, so let's continue from the last offset value.
  if (pOffset != NULL) {
    *pOffsetVal = pOffset->val;

    char formatBuf[TSDB_OFFSET_LEN] = {0};
    tFormatOffset(formatBuf, TSDB_OFFSET_LEN, pOffsetVal);
    tqDebug("tmq poll: consumer:0x%" PRIx64
            ", subkey %s, vgId:%d, existed offset found, offset reset to %s and continue. reqId:0x%" PRIx64,
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
          tqOffsetResetToData(pOffsetVal, 0, 0);
        }
      } else {
        walRefFirstVer(pTq->pVnode->pWal, pHandle->pRef);
        tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer);
      }
    } else if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_LATEST) {
      walRefLastVer(pTq->pVnode->pWal, pHandle->pRef);
      SMqDataRsp dataRsp = {0};
      tqOffsetResetToLog(pOffsetVal, pHandle->pRef->refVer + 1);

      tqInitDataRsp(&dataRsp, *pOffsetVal);
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, (latest) offset reset to %" PRId64, consumerId,
              pHandle->subKey, vgId, dataRsp.rspOffset.version);
      int32_t code = tqSendDataRsp(pHandle, pMsg, pRequest, &dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
      tDeleteMqDataRsp(&dataRsp);

      *pBlockReturned = true;
      return code;
    } else if (pRequest->reqOffset.type == TMQ_OFFSET__RESET_NONE) {
      tqError("tmq poll: subkey:%s, no offset committed for consumer:0x%" PRIx64
              " in vg %d, subkey %s, reset none failed",
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
  terrno = 0;

  SMqDataRsp dataRsp = {0};
  tqInitDataRsp(&dataRsp, *pOffset);

  qSetTaskId(pHandle->execHandle.task, consumerId, pRequest->reqId);
  int code = tqScanData(pTq, pHandle, &dataRsp, pOffset, pRequest);
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

  dataRsp.reqOffset = *pOffset;  // reqOffset represents the current date offset, may be changed if wal not exists
  code = tqSendDataRsp(pHandle, pMsg, pRequest, (SMqDataRsp*)&dataRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);

end : {
  char buf[TSDB_OFFSET_LEN] = {0};
  tFormatOffset(buf, TSDB_OFFSET_LEN, &dataRsp.rspOffset);
  tqDebug("tmq poll: consumer:0x%" PRIx64 ", subkey %s, vgId:%d, rsp block:%d, rsp offset type:%s, reqId:0x%" PRIx64
          " code:%d",
          consumerId, pHandle->subKey, vgId, dataRsp.blockNum, buf, pRequest->reqId, code);
  tDeleteMqDataRsp(&dataRsp);
  return code;
}
}

#define PROCESS_EXCLUDED_MSG(TYPE, DECODE_FUNC) \
  SDecoder           decoder = {0};\
  TYPE               req = {0}; \
  void*   data = POINTER_SHIFT(pHead->body, sizeof(SMsgHead)); \
  int32_t len = pHead->bodyLen - sizeof(SMsgHead); \
  tDecoderInit(&decoder, data, len); \
  if (DECODE_FUNC(&decoder, &req) == 0 && (req.source & TD_REQ_FROM_TAOX) != 0) { \
    tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, jump meta for, vgId:%d offset %" PRId64 " msgType %d",  \
            pRequest->consumerId, pRequest->epoch, vgId, fetchVer, pHead->msgType); \
    fetchVer++; \
    tDecoderClear(&decoder); \
    continue; \
  } \
  tDecoderClear(&decoder);


static int32_t extractDataAndRspForDbStbSubscribe(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest,
                                                  SRpcMsg* pMsg, STqOffsetVal* offset) {
  int        code = 0;
  int32_t    vgId = TD_VID(pTq->pVnode);
  SMqMetaRsp metaRsp = {0};
  STaosxRsp  taosxRsp = {0};
  tqInitTaosxRsp(&taosxRsp, *offset);

  if (offset->type != TMQ_OFFSET__LOG) {
    if (tqScanTaosx(pTq, pHandle, &taosxRsp, &metaRsp, offset) < 0) {
      code = -1;
      goto end;
    }

    if (metaRsp.metaRspLen > 0) {
      code = tqSendMetaPollRsp(pHandle, pMsg, pRequest, &metaRsp, vgId);
      tqDebug("tmq poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send meta offset type:%d,uid:%" PRId64
              ",ts:%" PRId64,
              pRequest->consumerId, pHandle->subKey, vgId, metaRsp.rspOffset.type, metaRsp.rspOffset.uid,
              metaRsp.rspOffset.ts);
      taosMemoryFree(metaRsp.metaRsp);
      goto end;
    }

    tqDebug("taosx poll: consumer:0x%" PRIx64 " subkey:%s vgId:%d, send data blockNum:%d, offset type:%d,uid:%" PRId64
            ",ts:%" PRId64,
            pRequest->consumerId, pHandle->subKey, vgId, taosxRsp.blockNum, taosxRsp.rspOffset.type,
            taosxRsp.rspOffset.uid, taosxRsp.rspOffset.ts);
    if (taosxRsp.blockNum > 0) {
      code = tqSendDataRsp(pHandle, pMsg, pRequest, (SMqDataRsp*)&taosxRsp, TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
      goto end;
    } else {
      *offset = taosxRsp.rspOffset;
    }
  }

  if (offset->type == TMQ_OFFSET__LOG) {
    walReaderVerifyOffset(pHandle->pWalReader, offset);
    int64_t fetchVer = offset->version;

    uint64_t st = taosGetTimestampMs();
    int      totalRows = 0;
    while (1) {
      int32_t savedEpoch = atomic_load_32(&pHandle->epoch);
      ASSERT(savedEpoch <= pRequest->epoch);

      if (tqFetchLog(pTq, pHandle, &fetchVer, pRequest->reqId) < 0) {
        tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer);
        code = tqSendDataRsp(
            pHandle, pMsg, pRequest, (SMqDataRsp*)&taosxRsp,
            taosxRsp.createTableNum > 0 ? TMQ_MSG_TYPE__POLL_DATA_META_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
        goto end;
      }

      SWalCont* pHead = &pHandle->pWalReader->pHead->head;
      tqDebug("tmq poll: consumer:0x%" PRIx64 " (epoch %d) iter log, vgId:%d offset %" PRId64 " msgType %d",
              pRequest->consumerId, pRequest->epoch, vgId, fetchVer, pHead->msgType);

      // process meta
      if (pHead->msgType != TDMT_VND_SUBMIT) {
        if (totalRows > 0) {
          tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer);
          code = tqSendDataRsp(
              pHandle, pMsg, pRequest, (SMqDataRsp*)&taosxRsp,
              taosxRsp.createTableNum > 0 ? TMQ_MSG_TYPE__POLL_DATA_META_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
          goto end;
        }

        if ((pRequest->sourceExcluded & TD_REQ_FROM_TAOX) != 0) {
          if (pHead->msgType == TDMT_VND_CREATE_TABLE) {
            PROCESS_EXCLUDED_MSG(SVCreateTbBatchReq, tDecodeSVCreateTbBatchReq)
          } else if (pHead->msgType == TDMT_VND_ALTER_TABLE) {
            PROCESS_EXCLUDED_MSG(SVAlterTbReq, tDecodeSVAlterTbReq)
          } else if (pHead->msgType == TDMT_VND_CREATE_STB || pHead->msgType == TDMT_VND_ALTER_STB) {
            PROCESS_EXCLUDED_MSG(SVCreateStbReq, tDecodeSVCreateStbReq)
          } else if (pHead->msgType == TDMT_VND_DELETE) {
            fetchVer++;
            continue;
          }
        }

        tqDebug("fetch meta msg, ver:%" PRId64 ", type:%s", pHead->version, TMSG_INFO(pHead->msgType));
        tqOffsetResetToLog(&metaRsp.rspOffset, fetchVer + 1);
        metaRsp.resMsgType = pHead->msgType;
        metaRsp.metaRspLen = pHead->bodyLen;
        metaRsp.metaRsp = pHead->body;
        code = tqSendMetaPollRsp(pHandle, pMsg, pRequest, &metaRsp, vgId);
        goto end;
      }

      // process data
      SPackedData submit = {
          .msgStr = POINTER_SHIFT(pHead->body, sizeof(SSubmitReq2Msg)),
          .msgLen = pHead->bodyLen - sizeof(SSubmitReq2Msg),
          .ver = pHead->version,
      };

      code = tqTaosxScanLog(pTq, pHandle, submit, &taosxRsp, &totalRows, pRequest->sourceExcluded);
      if (code < 0) {
        tqError("tmq poll: tqTaosxScanLog error %" PRId64 ", in vgId:%d, subkey %s", pRequest->consumerId, vgId,
                pRequest->subKey);
        goto end;
      }

      if (totalRows >= 4096 || (taosGetTimestampMs() - st > 1000)) {
        tqOffsetResetToLog(&taosxRsp.rspOffset, fetchVer + 1);
        code = tqSendDataRsp(
            pHandle, pMsg, pRequest, (SMqDataRsp*)&taosxRsp,
            taosxRsp.createTableNum > 0 ? TMQ_MSG_TYPE__POLL_DATA_META_RSP : TMQ_MSG_TYPE__POLL_DATA_RSP, vgId);
        goto end;
      } else {
        fetchVer++;
      }
    }
  }

end:

  tDeleteSTaosxRsp(&taosxRsp);
  return code;
}

int32_t tqExtractDataForMq(STQ* pTq, STqHandle* pHandle, const SMqPollReq* pRequest, SRpcMsg* pMsg) {
  STqOffsetVal reqOffset = pRequest->reqOffset;

  // 1. reset the offset if needed
  if (IS_OFFSET_RESET_TYPE(pRequest->reqOffset.type)) {
    // handle the reset offset cases, according to the consumer's choice.
    bool    blockReturned = false;
    int32_t code = extractResetOffsetVal(&reqOffset, pTq, pHandle, pRequest, pMsg, &blockReturned);
    if (code != 0) {
      return code;
    }

    // empty block returned, quit
    if (blockReturned) {
      return 0;
    }
  } else if (reqOffset.type == 0) {  // use the consumer specified offset
    uError("req offset type is 0");
    return TSDB_CODE_TMQ_INVALID_MSG;
  }

  // this is a normal subscribe requirement
  if (pHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
    return extractDataAndRspForNormalSubscribe(pTq, pHandle, pRequest, pMsg, &reqOffset);
  } else {
    // for taosx
    return extractDataAndRspForDbStbSubscribe(pTq, pHandle, pRequest, pMsg, &reqOffset);
  }
}

static void initMqRspHead(SMqRspHead* pMsgHead, int32_t type, int32_t epoch, int64_t consumerId, int64_t sver,
                          int64_t ever) {
  pMsgHead->consumerId = consumerId;
  pMsgHead->epoch = epoch;
  pMsgHead->mqMsgType = type;
  pMsgHead->walsver = sver;
  pMsgHead->walever = ever;
}

int32_t tqSendMetaPollRsp(STqHandle* pHandle, const SRpcMsg* pMsg, const SMqPollReq* pReq, const SMqMetaRsp* pRsp,
                          int32_t vgId) {
  int32_t len = 0;
  int32_t code = 0;
  tEncodeSize(tEncodeMqMetaRsp, pRsp, len, code);
  if (code < 0) {
    return -1;
  }
  int32_t tlen = sizeof(SMqRspHead) + len;
  void*   buf = rpcMallocCont(tlen);
  if (buf == NULL) {
    return -1;
  }

  int64_t sver = 0, ever = 0;
  walReaderValidVersionRange(pHandle->execHandle.pTqReader->pWalReader, &sver, &ever);
  initMqRspHead(buf, TMQ_MSG_TYPE__POLL_META_RSP, pReq->epoch, pReq->consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);
  tEncodeMqMetaRsp(&encoder, pRsp);
  tEncoderClear(&encoder);

  SRpcMsg resp = {.info = pMsg->info, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&resp);
  tqDebug("vgId:%d, from consumer:0x%" PRIx64 " (epoch %d) send rsp, res msg type %d, offset type:%d", vgId,
          pReq->consumerId, pReq->epoch, pRsp->resMsgType, pRsp->rspOffset.type);

  return 0;
}

int32_t tqDoSendDataRsp(const SRpcHandleInfo* pRpcHandleInfo, const SMqDataRsp* pRsp, int32_t epoch, int64_t consumerId,
                        int32_t type, int64_t sver, int64_t ever) {
  int32_t len = 0;
  int32_t code = 0;

  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP || type == TMQ_MSG_TYPE__WALINFO_RSP) {
    tEncodeSize(tEncodeMqDataRsp, pRsp, len, code);
  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
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

  SMqRspHead* pHead = (SMqRspHead*)buf;
  initMqRspHead(pHead, type, epoch, consumerId, sver, ever);

  void* abuf = POINTER_SHIFT(buf, sizeof(SMqRspHead));

  SEncoder encoder = {0};
  tEncoderInit(&encoder, abuf, len);

  if (type == TMQ_MSG_TYPE__POLL_DATA_RSP || type == TMQ_MSG_TYPE__WALINFO_RSP) {
    tEncodeMqDataRsp(&encoder, pRsp);
  } else if (type == TMQ_MSG_TYPE__POLL_DATA_META_RSP) {
    tEncodeSTaosxRsp(&encoder, (STaosxRsp*)pRsp);
  }

  tEncoderClear(&encoder);
  SRpcMsg rsp = {.info = *pRpcHandleInfo, .pCont = buf, .contLen = tlen, .code = 0};

  tmsgSendRsp(&rsp);
  return 0;
}

int32_t tqExtractDelDataBlock(const void* pData, int32_t len, int64_t ver, void** pRefBlock, int32_t type) {
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
  if (type == 0) {
    *pRefBlock = taosAllocateQitem(sizeof(SStreamRefDataBlock), DEF_QITEM, 0);
    if (*pRefBlock == NULL) {
      blockDataCleanup(pDelBlock);
      taosMemoryFree(pDelBlock);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    ((SStreamRefDataBlock*)(*pRefBlock))->type = STREAM_INPUT__REF_DATA_BLOCK;
    ((SStreamRefDataBlock*)(*pRefBlock))->pBlock = pDelBlock;
  } else if (type == 1) {
    *pRefBlock = pDelBlock;
  } else {
    ASSERTS(0, "unknown type:%d", type);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tqGetStreamExecInfo(SVnode* pVnode, int64_t streamId, int64_t* pDelay, bool* fhFinished) {
  SStreamMeta* pMeta = pVnode->pTq->pStreamMeta;
  int32_t      numOfTasks = taosArrayGetSize(pMeta->pTaskList);
  int32_t      code = TSDB_CODE_SUCCESS;

  if (pDelay != NULL) {
    *pDelay = 0;
  }

  *fhFinished = false;

  if (numOfTasks <= 0) {
    return code;
  }

  // extract the required source task for a given stream, identified by streamId
  for (int32_t i = 0; i < numOfTasks; ++i) {
    STaskId* pId = taosArrayGet(pMeta->pTaskList, i);
    if (pId->streamId != streamId) {
      continue;
    }

    SStreamTask** ppTask = taosHashGet(pMeta->pTasksMap, pId, sizeof(*pId));
    if (ppTask == NULL) {
      tqError("vgId:%d failed to acquire task:0x%" PRIx64 " in retrieving progress", pMeta->vgId, pId->taskId);
      continue;
    }

    if ((*ppTask)->info.taskLevel != TASK_LEVEL__SOURCE) {
      continue;
    }

    // here we get the required stream source task
    SStreamTask* pTask = *ppTask;
    *fhFinished = !HAS_RELATED_FILLHISTORY_TASK(pTask);

    int64_t ver = walReaderGetCurrentVer(pTask->exec.pWalReader);

    SVersionRange verRange = {0};
    walReaderValidVersionRange(pTask->exec.pWalReader, &verRange.minVer, &verRange.maxVer);

    SWalReader* pReader = walOpenReader(pTask->exec.pWalReader->pWal, NULL, 0);
    if (pReader == NULL) {
      tqError("failed to open wal reader to extract exec progress, vgId:%d", pMeta->vgId);
      continue;
    }

    int64_t cur = 0;
    int64_t latest = 0;

    code = walFetchHead(pReader, ver);
    if (code != TSDB_CODE_SUCCESS) {
      cur = pReader->pHead->head.ingestTs;
    }

    code = walFetchHead(pReader, verRange.maxVer);
    if (code != TSDB_CODE_SUCCESS) {
      latest = pReader->pHead->head.ingestTs;
    }

    if (pDelay != NULL) {  // delay in ms
      *pDelay = (latest - cur) / 1000;
    }

    walCloseReader(pReader);
  }

  return TSDB_CODE_SUCCESS;
}
