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

#include "tmsg.h"
#include "tq.h"

bool isValValidForTable(STqHandle* pHandle, SWalCont* pHead) {
  if (pHandle->execHandle.subType != TOPIC_SUB_TYPE__TABLE) {
    return true;
  }

  int16_t msgType = pHead->msgType;
  char*   body = pHead->body;
  int32_t bodyLen = pHead->bodyLen;

  int64_t  tbSuid = pHandle->execHandle.execTb.suid;
  int64_t  realTbSuid = 0;
  SDecoder coder;
  void*    data = POINTER_SHIFT(body, sizeof(SMsgHead));
  int32_t  len = bodyLen - sizeof(SMsgHead);
  tDecoderInit(&coder, data, len);

  if (msgType == TDMT_VND_CREATE_STB || msgType == TDMT_VND_ALTER_STB) {
    SVCreateStbReq req = {0};
    if (tDecodeSVCreateStbReq(&coder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  } else if (msgType == TDMT_VND_DROP_STB) {
    SVDropStbReq req = {0};
    if (tDecodeSVDropStbReq(&coder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  } else if (msgType == TDMT_VND_CREATE_TABLE) {
    SVCreateTbBatchReq req = {0};
    if (tDecodeSVCreateTbBatchReq(&coder, &req) < 0) {
      goto end;
    }

    int32_t        needRebuild = 0;
    SVCreateTbReq* pCreateReq = NULL;
    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      pCreateReq = req.pReqs + iReq;
      if (pCreateReq->type == TSDB_CHILD_TABLE && pCreateReq->ctb.suid == tbSuid) {
        needRebuild++;
      }
    }
    if (needRebuild == 0) {
      // do nothing
    } else if (needRebuild == req.nReqs) {
      realTbSuid = tbSuid;
    } else {
      realTbSuid = tbSuid;
      SVCreateTbBatchReq reqNew = {0};
      reqNew.pArray = taosArrayInit(req.nReqs, sizeof(struct SVCreateTbReq));
      for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
        pCreateReq = req.pReqs + iReq;
        if (pCreateReq->type == TSDB_CHILD_TABLE && pCreateReq->ctb.suid == tbSuid) {
          reqNew.nReqs++;
          taosArrayPush(reqNew.pArray, pCreateReq);
        }
      }

      int     tlen;
      int32_t ret = 0;
      tEncodeSize(tEncodeSVCreateTbBatchReq, &reqNew, tlen, ret);
      void* buf = taosMemoryMalloc(tlen);
      if (NULL == buf) {
        taosArrayDestroy(reqNew.pArray);
        for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
          pCreateReq = req.pReqs + iReq;
          taosMemoryFreeClear(pCreateReq->comment);
          if (pCreateReq->type == TSDB_CHILD_TABLE) {
            taosArrayDestroy(pCreateReq->ctb.tagName);
          }
        }
        goto end;
      }
      SEncoder coderNew = {0};
      tEncoderInit(&coderNew, buf, tlen - sizeof(SMsgHead));
      tEncodeSVCreateTbBatchReq(&coderNew, &reqNew);
      tEncoderClear(&coderNew);
      memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
      pHead->bodyLen = tlen + sizeof(SMsgHead);
      taosMemoryFree(buf);
      taosArrayDestroy(reqNew.pArray);
    }

    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      pCreateReq = req.pReqs + iReq;
      taosMemoryFreeClear(pCreateReq->comment);
      taosMemoryFreeClear(pCreateReq->sql);
      if (pCreateReq->type == TSDB_CHILD_TABLE) {
        taosArrayDestroy(pCreateReq->ctb.tagName);
      }
    }
  } else if (msgType == TDMT_VND_ALTER_TABLE) {
    SVAlterTbReq req = {0};

    if (tDecodeSVAlterTbReq(&coder, &req) < 0) {
      goto end;
    }

    SMetaReader mr = {0};
    metaReaderDoInit(&mr, pHandle->execHandle.pTqReader->pVnodeMeta, 0);

    if (metaGetTableEntryByName(&mr, req.tbName) < 0) {
      metaReaderClear(&mr);
      goto end;
    }
    realTbSuid = mr.me.ctbEntry.suid;
    metaReaderClear(&mr);
  } else if (msgType == TDMT_VND_DROP_TABLE) {
    SVDropTbBatchReq req = {0};

    if (tDecodeSVDropTbBatchReq(&coder, &req) < 0) {
      goto end;
    }

    int32_t      needRebuild = 0;
    SVDropTbReq* pDropReq = NULL;
    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      pDropReq = req.pReqs + iReq;

      if (pDropReq->suid == tbSuid) {
        needRebuild++;
      }
    }
    if (needRebuild == 0) {
      // do nothing
    } else if (needRebuild == req.nReqs) {
      realTbSuid = tbSuid;
    } else {
      realTbSuid = tbSuid;
      SVDropTbBatchReq reqNew = {0};
      reqNew.pArray = taosArrayInit(req.nReqs, sizeof(SVDropTbReq));
      for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
        pDropReq = req.pReqs + iReq;
        if (pDropReq->suid == tbSuid) {
          reqNew.nReqs++;
          taosArrayPush(reqNew.pArray, pDropReq);
        }
      }

      int     tlen;
      int32_t ret = 0;
      tEncodeSize(tEncodeSVDropTbBatchReq, &reqNew, tlen, ret);
      void* buf = taosMemoryMalloc(tlen);
      if (NULL == buf) {
        taosArrayDestroy(reqNew.pArray);
        goto end;
      }
      SEncoder coderNew = {0};
      tEncoderInit(&coderNew, buf, tlen - sizeof(SMsgHead));
      tEncodeSVDropTbBatchReq(&coderNew, &reqNew);
      tEncoderClear(&coderNew);
      memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
      pHead->bodyLen = tlen + sizeof(SMsgHead);
      taosMemoryFree(buf);
      taosArrayDestroy(reqNew.pArray);
    }
  } else if (msgType == TDMT_VND_DELETE) {
    SDeleteRes req = {0};
    if (tDecodeDeleteRes(&coder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  }

end:
  tDecoderClear(&coder);
  return tbSuid == realTbSuid;
}

int32_t tqFetchLog(STQ* pTq, STqHandle* pHandle, int64_t* fetchOffset, uint64_t reqId) {
  int32_t code = -1;
  int32_t vgId = TD_VID(pTq->pVnode);
  int64_t id = pHandle->pWalReader->readerId;

  int64_t offset = *fetchOffset;
  int64_t lastVer = walGetLastVer(pHandle->pWalReader->pWal);
  int64_t committedVer = walGetCommittedVer(pHandle->pWalReader->pWal);
  int64_t appliedVer = walGetAppliedVer(pHandle->pWalReader->pWal);

  wDebug("vgId:%d, start to fetch wal, index:%" PRId64 ", last:%" PRId64 " commit:%" PRId64 ", applied:%" PRId64
         ", 0x%" PRIx64,
         vgId, offset, lastVer, committedVer, appliedVer, id);

  while (offset <= appliedVer) {
    if (walFetchHead(pHandle->pWalReader, offset) < 0) {
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", (epoch %d) vgId:%d offset %" PRId64
              ", no more log to return, reqId:0x%" PRIx64 " 0x%" PRIx64,
              pHandle->consumerId, pHandle->epoch, vgId, offset, reqId, id);
      goto END;
    }

    tqDebug("vgId:%d, consumer:0x%" PRIx64 " taosx get msg ver %" PRId64 ", type: %s, reqId:0x%" PRIx64 " 0x%" PRIx64,
            vgId, pHandle->consumerId, offset, TMSG_INFO(pHandle->pWalReader->pHead->head.msgType), reqId, id);

    if (pHandle->pWalReader->pHead->head.msgType == TDMT_VND_SUBMIT) {
      code = walFetchBody(pHandle->pWalReader);
      goto END;
    } else {
      if (pHandle->fetchMeta != WITH_DATA) {
        SWalCont* pHead = &(pHandle->pWalReader->pHead->head);
        if (IS_META_MSG(pHead->msgType) && !(pHead->msgType == TDMT_VND_DELETE && pHandle->fetchMeta == ONLY_META)) {
          code = walFetchBody(pHandle->pWalReader);
          if (code < 0) {
            goto END;
          }

          pHead = &(pHandle->pWalReader->pHead->head);
          if (isValValidForTable(pHandle, pHead)) {
            code = 0;
            goto END;
          } else {
            offset++;
            code = -1;
            continue;
          }
        }
      }
      code = walSkipFetchBody(pHandle->pWalReader);
      if (code < 0) {
        goto END;
      }
      offset++;
    }
    code = -1;
  }

END:
  *fetchOffset = offset;
  return code;
}

STqReader* tqReaderOpen(SVnode* pVnode) {
  STqReader* pReader = taosMemoryCalloc(1, sizeof(STqReader));
  if (pReader == NULL) {
    return NULL;
  }

  pReader->pWalReader = walOpenReader(pVnode->pWal, NULL, 0);
  if (pReader->pWalReader == NULL) {
    taosMemoryFree(pReader);
    return NULL;
  }

  pReader->pVnodeMeta = pVnode->pMeta;
  pReader->pColIdList = NULL;
  pReader->cachedSchemaVer = 0;
  pReader->cachedSchemaSuid = 0;
  pReader->pSchemaWrapper = NULL;
  pReader->tbIdHash = NULL;
  pReader->pResBlock = createDataBlock();
  return pReader;
}

void tqReaderClose(STqReader* pReader) {
  if (pReader == NULL) return;

  // close wal reader
  if (pReader->pWalReader) {
    walCloseReader(pReader->pWalReader);
  }

  if (pReader->pSchemaWrapper) {
    tDeleteSchemaWrapper(pReader->pSchemaWrapper);
  }

  if (pReader->pColIdList) {
    taosArrayDestroy(pReader->pColIdList);
  }

  // free hash
  blockDataDestroy(pReader->pResBlock);
  taosHashCleanup(pReader->tbIdHash);
  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
  taosMemoryFree(pReader);
}

int32_t tqReaderSeek(STqReader* pReader, int64_t ver, const char* id) {
  if (walReaderSeekVer(pReader->pWalReader, ver) < 0) {
    return -1;
  }
  tqDebug("wal reader seek to ver:%" PRId64 " %s", ver, id);
  return 0;
}

int32_t extractMsgFromWal(SWalReader* pReader, void** pItem, int64_t maxVer, const char* id) {
  int32_t code = 0;

  while (1) {
    code = walNextValidMsg(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SWalCont* pCont = &pReader->pHead->head;
    int64_t   ver = pCont->version;
    if (ver > maxVer) {
      tqDebug("maxVer in WAL:%" PRId64 " reached, current:%" PRId64 ", do not scan wal anymore, %s", maxVer, ver, id);
      return TSDB_CODE_SUCCESS;
    }

    if (pCont->msgType == TDMT_VND_SUBMIT) {
      void*   pBody = POINTER_SHIFT(pCont->body, sizeof(SSubmitReq2Msg));
      int32_t len = pCont->bodyLen - sizeof(SSubmitReq2Msg);

      void* data = taosMemoryMalloc(len);
      if (data == NULL) {
        // todo: for all stream in this vnode, keep this offset in the offset files, and wait for a moment, and then
        // retry
        code = TSDB_CODE_OUT_OF_MEMORY;
        terrno = code;

        tqError("vgId:%d, failed to copy submit data for stream processing, since out of memory", 0);
        return code;
      }

      memcpy(data, pBody, len);
      SPackedData data1 = (SPackedData){.ver = ver, .msgLen = len, .msgStr = data};

      *pItem = (SStreamQueueItem*)streamDataSubmitNew(&data1, STREAM_INPUT__DATA_SUBMIT);
      if (*pItem == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        terrno = code;
        tqError("%s failed to create data submit for stream since out of memory", id);
        return code;
      }
    } else if (pCont->msgType == TDMT_VND_DELETE) {
      void*   pBody = POINTER_SHIFT(pCont->body, sizeof(SMsgHead));
      int32_t len = pCont->bodyLen - sizeof(SMsgHead);

      code = tqExtractDelDataBlock(pBody, len, ver, (void**)pItem, 0);
      if (code == TSDB_CODE_SUCCESS) {
        if (*pItem == NULL) {
          tqDebug("s-task:%s empty delete msg, discard it, len:%d, ver:%" PRId64, id, len, ver);
          // we need to continue check next data in the wal files.
          continue;
        } else {
          tqDebug("s-task:%s delete msg extract from WAL, len:%d, ver:%" PRId64, id, len, ver);
        }
      } else {
        terrno = code;
        tqError("s-task:%s extract delete msg from WAL failed, code:%s", id, tstrerror(code));
        return code;
      }

    } else {
      ASSERT(0);
    }

    return code;
  }
}

// todo ignore the error in wal?
bool tqNextBlockInWal(STqReader* pReader, const char* id, int sourceExcluded) {
  SWalReader*  pWalReader = pReader->pWalReader;
  SSDataBlock* pDataBlock = NULL;

  uint64_t st = taosGetTimestampMs();
  while (1) {
    // try next message in wal file
    if (walNextValidMsg(pWalReader) < 0) {
      return false;
    }

    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int64_t ver = pWalReader->pHead->head.version;

    tqReaderSetSubmitMsg(pReader, pBody, bodyLen, ver);
    pReader->nextBlk = 0;
    int32_t numOfBlocks = taosArrayGetSize(pReader->submit.aSubmitTbData);
    while (pReader->nextBlk < numOfBlocks) {
      tqTrace("tq reader next data block %d/%d, len:%d %" PRId64, pReader->nextBlk, numOfBlocks, pReader->msg.msgLen,
              pReader->msg.ver);

      SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
      if ((pSubmitTbData->flags & sourceExcluded) != 0) {
        pReader->nextBlk += 1;
        continue;
      }
      if (pReader->tbIdHash == NULL || taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t)) != NULL) {
        tqTrace("tq reader return submit block, uid:%" PRId64, pSubmitTbData->uid);
        SSDataBlock* pRes = NULL;
        int32_t      code = tqRetrieveDataBlock(pReader, &pRes, NULL);
        if (code == TSDB_CODE_SUCCESS && pRes->info.rows > 0) {
          if (pDataBlock == NULL) {
            pDataBlock = createOneDataBlock(pRes, true);
          } else {
            blockDataMerge(pDataBlock, pRes);
          }
        }
      } else {
        pReader->nextBlk += 1;
        tqTrace("tq reader discard submit block, uid:%" PRId64 ", continue", pSubmitTbData->uid);
      }
    }
    tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
    pReader->msg.msgStr = NULL;

    if (pDataBlock != NULL) {
      blockDataCleanup(pReader->pResBlock);
      copyDataBlock(pReader->pResBlock, pDataBlock);
      blockDataDestroy(pDataBlock);
      return true;
    } else {
      qTrace("stream scan return empty, all %d submit blocks consumed, %s", numOfBlocks, id);
    }

    if (taosGetTimestampMs() - st > 1000) {
      return false;
    }
  }
}

int32_t tqReaderSetSubmitMsg(STqReader* pReader, void* msgStr, int32_t msgLen, int64_t ver) {
  pReader->msg.msgStr = msgStr;
  pReader->msg.msgLen = msgLen;
  pReader->msg.ver = ver;

  tqDebug("tq reader set msg %p %d", msgStr, msgLen);
  SDecoder decoder;

  tDecoderInit(&decoder, pReader->msg.msgStr, pReader->msg.msgLen);
  if (tDecodeSubmitReq(&decoder, &pReader->submit) < 0) {
    tDecoderClear(&decoder);
    tqError("DecodeSSubmitReq2 error, msgLen:%d, ver:%" PRId64, msgLen, ver);
    return -1;
  }

  tDecoderClear(&decoder);
  return 0;
}

SWalReader* tqGetWalReader(STqReader* pReader) { return pReader->pWalReader; }

SSDataBlock* tqGetResultBlock(STqReader* pReader) { return pReader->pResBlock; }

int64_t tqGetResultBlockTime(STqReader* pReader) { return pReader->lastTs; }

bool tqNextBlockImpl(STqReader* pReader, const char* idstr) {
  if (pReader->msg.msgStr == NULL) {
    return false;
  }

  int32_t numOfBlocks = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < numOfBlocks) {
    tqDebug("try next data block, len:%d ver:%" PRId64 " index:%d/%d, %s", pReader->msg.msgLen, pReader->msg.ver,
            (pReader->nextBlk + 1), numOfBlocks, idstr);

    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
    if (pReader->tbIdHash == NULL) {
      return true;
    }

    void* ret = taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t));
    if (ret != NULL) {
      tqDebug("block found, ver:%" PRId64 ", uid:%" PRId64 ", %s", pReader->msg.ver, pSubmitTbData->uid, idstr);
      return true;
    } else {
      tqDebug("discard submit block, uid:%" PRId64 ", total queried tables:%d continue %s", pSubmitTbData->uid,
              taosHashGetSize(pReader->tbIdHash), idstr);
    }

    pReader->nextBlk++;
  }

  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
  pReader->nextBlk = 0;
  pReader->msg.msgStr = NULL;

  return false;
}

bool tqNextDataBlockFilterOut(STqReader* pReader, SHashObj* filterOutUids) {
  if (pReader->msg.msgStr == NULL) return false;

  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < blockSz) {
    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
    if (filterOutUids == NULL) return true;

    void* ret = taosHashGet(filterOutUids, &pSubmitTbData->uid, sizeof(int64_t));
    if (ret == NULL) {
      return true;
    }
    pReader->nextBlk++;
  }

  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
  pReader->nextBlk = 0;
  pReader->msg.msgStr = NULL;

  return false;
}

int32_t tqMaskBlock(SSchemaWrapper* pDst, SSDataBlock* pBlock, const SSchemaWrapper* pSrc, char* mask) {
  int32_t code;

  int32_t cnt = 0;
  for (int32_t i = 0; i < pSrc->nCols; i++) {
    cnt += mask[i];
  }

  pDst->nCols = cnt;
  pDst->pSchema = taosMemoryCalloc(cnt, sizeof(SSchema));
  if (pDst->pSchema == NULL) {
    return -1;
  }

  int32_t j = 0;
  for (int32_t i = 0; i < pSrc->nCols; i++) {
    if (mask[i]) {
      pDst->pSchema[j++] = pSrc->pSchema[i];
      SColumnInfoData colInfo =
          createColumnInfoData(pSrc->pSchema[i].type, pSrc->pSchema[i].bytes, pSrc->pSchema[i].colId);
      code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != 0) {
        return -1;
      }
    }
  }
  return 0;
}

static int32_t buildResSDataBlock(SSDataBlock* pBlock, SSchemaWrapper* pSchema, const SArray* pColIdList) {
  if (blockDataGetNumOfCols(pBlock) > 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfCols = taosArrayGetSize(pColIdList);

  if (numOfCols == 0) {  // all columns are required
    for (int32_t i = 0; i < pSchema->nCols; ++i) {
      SSchema*        pColSchema = &pSchema->pSchema[i];
      SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);

      int32_t code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != TSDB_CODE_SUCCESS) {
        blockDataFreeRes(pBlock);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  } else {
    if (numOfCols > pSchema->nCols) {
      numOfCols = pSchema->nCols;
    }

    int32_t i = 0;
    int32_t j = 0;
    while (i < pSchema->nCols && j < numOfCols) {
      SSchema* pColSchema = &pSchema->pSchema[i];
      col_id_t colIdSchema = pColSchema->colId;

      col_id_t colIdNeed = *(col_id_t*)taosArrayGet(pColIdList, j);
      if (colIdSchema < colIdNeed) {
        i++;
      } else if (colIdSchema > colIdNeed) {
        j++;
      } else {
        SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);
        int32_t         code = blockDataAppendColInfo(pBlock, &colInfo);
        if (code != TSDB_CODE_SUCCESS) {
          return -1;
        }
        i++;
        j++;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doSetVal(SColumnInfoData* pColumnInfoData, int32_t rowIndex, SColVal* pColVal) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (IS_STR_DATA_TYPE(pColVal->type)) {
    char val[65535 + 2] = {0};
    if (COL_VAL_IS_VALUE(pColVal)) {
      if (pColVal->value.pData != NULL) {
        memcpy(varDataVal(val), pColVal->value.pData, pColVal->value.nData);
      }
      varDataSetLen(val, pColVal->value.nData);
      code = colDataSetVal(pColumnInfoData, rowIndex, val, false);
    } else {
      colDataSetNULL(pColumnInfoData, rowIndex);
    }
  } else {
    code = colDataSetVal(pColumnInfoData, rowIndex, (void*)&pColVal->value.val, !COL_VAL_IS_VALUE(pColVal));
  }

  return code;
}

int32_t tqRetrieveDataBlock(STqReader* pReader, SSDataBlock** pRes, const char* id) {
  tqTrace("tq reader retrieve data block %p, index:%d", pReader->msg.msgStr, pReader->nextBlk);
  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk++);

  SSDataBlock* pBlock = pReader->pResBlock;
  *pRes = pBlock;

  blockDataCleanup(pBlock);

  int32_t vgId = pReader->pWalReader->pWal->cfg.vgId;
  int32_t sversion = pSubmitTbData->sver;
  int64_t suid = pSubmitTbData->suid;
  int64_t uid = pSubmitTbData->uid;
  pReader->lastTs = pSubmitTbData->ctimeMs;

  pBlock->info.id.uid = uid;
  pBlock->info.version = pReader->msg.ver;

  if ((suid != 0 && pReader->cachedSchemaSuid != suid) || (suid == 0 && pReader->cachedSchemaUid != uid) ||
      (pReader->cachedSchemaVer != sversion)) {
    tDeleteSchemaWrapper(pReader->pSchemaWrapper);

    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, uid, sversion, 1);
    if (pReader->pSchemaWrapper == NULL) {
      tqWarn("vgId:%d, cannot found schema wrapper for table: suid:%" PRId64 ", uid:%" PRId64
             "version %d, possibly dropped table",
             vgId, suid, uid, pReader->cachedSchemaVer);
      pReader->cachedSchemaSuid = 0;
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }

    pReader->cachedSchemaUid = uid;
    pReader->cachedSchemaSuid = suid;
    pReader->cachedSchemaVer = sversion;

    ASSERT(pReader->cachedSchemaVer == pReader->pSchemaWrapper->version);
    if (blockDataGetNumOfCols(pBlock) == 0) {
      int32_t code = buildResSDataBlock(pReader->pResBlock, pReader->pSchemaWrapper, pReader->pColIdList);
      if (code != TSDB_CODE_SUCCESS) {
        tqError("vgId:%d failed to build data block, code:%s", vgId, tstrerror(code));
        return code;
      }
    }
  }

  int32_t numOfRows = 0;
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    numOfRows = pCol->nVal;
  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
  }

  if (blockDataEnsureCapacity(pBlock, numOfRows) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pBlock->info.rows = numOfRows;

  int32_t colActual = blockDataGetNumOfCols(pBlock);

  // convert and scan one block
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SArray* pCols = pSubmitTbData->aCol;
    int32_t numOfCols = taosArrayGetSize(pCols);
    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    while (targetIdx < colActual) {
      if (sourceIdx >= numOfCols) {
        tqError("tqRetrieveDataBlock sourceIdx:%d >= numOfCols:%d", sourceIdx, numOfCols);
        return -1;
      }

      SColData*        pCol = taosArrayGet(pCols, sourceIdx);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      SColVal          colVal;

      if (pCol->nVal != numOfRows) {
        tqError("tqRetrieveDataBlock pCol->nVal:%d != numOfRows:%d", pCol->nVal, numOfRows);
        return -1;
      }

      if (pCol->cid < pColData->info.colId) {
        sourceIdx++;
      } else if (pCol->cid == pColData->info.colId) {
        for (int32_t i = 0; i < pCol->nVal; i++) {
          tColDataGetValue(pCol, i, &colVal);
          int32_t code = doSetVal(pColData, i, &colVal);
          if (code != TSDB_CODE_SUCCESS) {
            return code;
          }
        }
        sourceIdx++;
        targetIdx++;
      } else {
        colDataSetNNULL(pColData, 0, pCol->nVal);
        targetIdx++;
      }
    }
  } else {
    SArray*         pRows = pSubmitTbData->aRowP;
    SSchemaWrapper* pWrapper = pReader->pSchemaWrapper;
    STSchema*       pTSchema = tBuildTSchema(pWrapper->pSchema, pWrapper->nCols, pWrapper->version);

    for (int32_t i = 0; i < numOfRows; i++) {
      SRow*   pRow = taosArrayGetP(pRows, i);
      int32_t sourceIdx = 0;

      for (int32_t j = 0; j < colActual; j++) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, j);
        while (1) {
          SColVal colVal;
          tRowGet(pRow, pTSchema, sourceIdx, &colVal);
          if (colVal.cid < pColData->info.colId) {
            //            tqDebug("colIndex:%d column id:%d in row, ignore, the required colId:%d, total cols in
            //            schema:%d",
            //                    sourceIdx, colVal.cid, pColData->info.colId, pTSchema->numOfCols);
            sourceIdx++;
            continue;
          } else if (colVal.cid == pColData->info.colId) {
            int32_t code = doSetVal(pColData, i, &colVal);
            if (code != TSDB_CODE_SUCCESS) {
              return code;
            }

            sourceIdx++;
            break;
          } else {
            colDataSetNULL(pColData, i);
            break;
          }
        }
      }
    }

    taosMemoryFreeClear(pTSchema);
  }

  return 0;
}

// todo refactor:
int32_t tqRetrieveTaosxBlock(STqReader* pReader, SArray* blocks, SArray* schemas, SSubmitTbData** pSubmitTbDataRet) {
  tqDebug("tq reader retrieve data block %p, %d", pReader->msg.msgStr, pReader->nextBlk);

  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
  pReader->nextBlk++;

  if (pSubmitTbDataRet) {
    *pSubmitTbDataRet = pSubmitTbData;
  }

  int32_t sversion = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  pReader->lastBlkUid = uid;

  tDeleteSchemaWrapper(pReader->pSchemaWrapper);
  pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, uid, sversion, 1);
  if (pReader->pSchemaWrapper == NULL) {
    tqWarn("vgId:%d, cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
           pReader->pWalReader->pWal->cfg.vgId, uid, pReader->cachedSchemaVer);
    pReader->cachedSchemaSuid = 0;
    terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
    return -1;
  }

  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;
  int32_t         numOfRows = 0;

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SArray*   pCols = pSubmitTbData->aCol;
    SColData* pCol = taosArrayGet(pCols, 0);
    numOfRows = pCol->nVal;
  } else {
    SArray* pRows = pSubmitTbData->aRowP;
    numOfRows = taosArrayGetSize(pRows);
  }

  int32_t curRow = 0;
  int32_t lastRow = 0;
  char*   assigned = taosMemoryCalloc(1, pSchemaWrapper->nCols);
  if (assigned == NULL) return -1;

  // convert and scan one block
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SArray* pCols = pSubmitTbData->aCol;
    int32_t numOfCols = taosArrayGetSize(pCols);
    for (int32_t i = 0; i < numOfRows; i++) {
      bool buildNew = false;

      for (int32_t j = 0; j < numOfCols; j++) {
        SColData* pCol = taosArrayGet(pCols, j);
        SColVal   colVal;
        tColDataGetValue(pCol, i, &colVal);
        if (curRow == 0) {
          assigned[j] = !COL_VAL_IS_NONE(&colVal);
          buildNew = true;
        } else {
          bool currentRowAssigned = !COL_VAL_IS_NONE(&colVal);
          if (currentRowAssigned != assigned[j]) {
            assigned[j] = currentRowAssigned;
            buildNew = true;
          }
        }
      }

      if (buildNew) {
        if (taosArrayGetSize(blocks) > 0) {
          SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
          pLastBlock->info.rows = curRow - lastRow;
          lastRow = curRow;
        }

        SSDataBlock     block = {0};
        SSchemaWrapper* pSW = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
        if (pSW == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto FAIL;
        }

        if (tqMaskBlock(pSW, &block, pSchemaWrapper, assigned) < 0) {
          blockDataFreeRes(&block);
          tDeleteSchemaWrapper(pSW);
          goto FAIL;
        }
        tqTrace("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
                (int32_t)taosArrayGetSize(block.pDataBlock));

        block.info.id.uid = uid;
        block.info.version = pReader->msg.ver;
        if (blockDataEnsureCapacity(&block, numOfRows - curRow) < 0) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          blockDataFreeRes(&block);
          tDeleteSchemaWrapper(pSW);
          goto FAIL;
        }
        taosArrayPush(blocks, &block);
        taosArrayPush(schemas, &pSW);
      }

      SSDataBlock* pBlock = taosArrayGetLast(blocks);

      tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
              (int32_t)taosArrayGetSize(blocks));

      int32_t targetIdx = 0;
      int32_t sourceIdx = 0;
      int32_t colActual = blockDataGetNumOfCols(pBlock);
      while (targetIdx < colActual) {
        SColData*        pCol = taosArrayGet(pCols, sourceIdx);
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
        SColVal          colVal;

        if (pCol->cid < pColData->info.colId) {
          sourceIdx++;
        } else if (pCol->cid == pColData->info.colId) {
          tColDataGetValue(pCol, i, &colVal);
          if (doSetVal(pColData, curRow - lastRow, &colVal) != TDB_CODE_SUCCESS) {
            goto FAIL;
          }
          sourceIdx++;
          targetIdx++;
        }
      }

      curRow++;
    }
  } else {
    SSchemaWrapper* pWrapper = pReader->pSchemaWrapper;
    STSchema*       pTSchema = tBuildTSchema(pWrapper->pSchema, pWrapper->nCols, pWrapper->version);
    SArray*         pRows = pSubmitTbData->aRowP;

    for (int32_t i = 0; i < numOfRows; i++) {
      SRow* pRow = taosArrayGetP(pRows, i);
      bool  buildNew = false;

      for (int32_t j = 0; j < pTSchema->numOfCols; j++) {
        SColVal colVal;
        tRowGet(pRow, pTSchema, j, &colVal);
        if (curRow == 0) {
          assigned[j] = !COL_VAL_IS_NONE(&colVal);
          buildNew = true;
        } else {
          bool currentRowAssigned = !COL_VAL_IS_NONE(&colVal);
          if (currentRowAssigned != assigned[j]) {
            assigned[j] = currentRowAssigned;
            buildNew = true;
          }
        }
      }

      if (buildNew) {
        if (taosArrayGetSize(blocks) > 0) {
          SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
          pLastBlock->info.rows = curRow - lastRow;
          lastRow = curRow;
        }

        SSDataBlock     block = {0};
        SSchemaWrapper* pSW = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
        if (pSW == NULL) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          goto FAIL;
        }

        if (tqMaskBlock(pSW, &block, pSchemaWrapper, assigned) < 0) {
          blockDataFreeRes(&block);
          tDeleteSchemaWrapper(pSW);
          goto FAIL;
        }
        tqTrace("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
                (int32_t)taosArrayGetSize(block.pDataBlock));

        block.info.id.uid = uid;
        block.info.version = pReader->msg.ver;
        if (blockDataEnsureCapacity(&block, numOfRows - curRow) < 0) {
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          blockDataFreeRes(&block);
          tDeleteSchemaWrapper(pSW);
          goto FAIL;
        }
        taosArrayPush(blocks, &block);
        taosArrayPush(schemas, &pSW);
      }

      SSDataBlock* pBlock = taosArrayGetLast(blocks);

      tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
              (int32_t)taosArrayGetSize(blocks));

      int32_t targetIdx = 0;
      int32_t sourceIdx = 0;
      int32_t colActual = blockDataGetNumOfCols(pBlock);
      while (targetIdx < colActual) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
        SColVal          colVal;
        tRowGet(pRow, pTSchema, sourceIdx, &colVal);

        if (colVal.cid < pColData->info.colId) {
          sourceIdx++;
        } else if (colVal.cid == pColData->info.colId) {
          if (doSetVal(pColData, curRow - lastRow, &colVal) != TDB_CODE_SUCCESS) {
            goto FAIL;
          }
          sourceIdx++;
          targetIdx++;
        }
      }
      curRow++;
    }

    taosMemoryFreeClear(pTSchema);
  }

  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  pLastBlock->info.rows = curRow - lastRow;

  taosMemoryFree(assigned);
  return 0;

FAIL:
  taosMemoryFree(assigned);
  return -1;
}

void tqReaderSetColIdList(STqReader* pReader, SArray* pColIdList) { pReader->pColIdList = pColIdList; }

int tqReaderSetTbUidList(STqReader* pReader, const SArray* tbUidList, const char* id) {
  if (pReader->tbIdHash) {
    taosHashClear(pReader->tbIdHash);
  } else {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  }

  if (pReader->tbIdHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  tqDebug("s-task:%s %d tables are set to be queried target table", id, (int32_t)taosArrayGetSize(tbUidList));
  return 0;
}

int tqReaderAddTbUidList(STqReader* pReader, const SArray* pTableUidList) {
  if (pReader->tbIdHash == NULL) {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  int32_t numOfTables = taosArrayGetSize(pTableUidList);
  for (int i = 0; i < numOfTables; i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(pTableUidList, i);
    taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

bool tqReaderIsQueriedTable(STqReader* pReader, uint64_t uid) {
  return taosHashGet(pReader->tbIdHash, &uid, sizeof(uint64_t));
}

bool tqCurrentBlockConsumed(const STqReader* pReader) { return pReader->msg.msgStr == NULL; }

int tqReaderRemoveTbUidList(STqReader* pReader, const SArray* tbUidList) {
  for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    taosHashRemove(pReader->tbIdHash, pKey, sizeof(int64_t));
  }

  return 0;
}

// todo update the table list in wal reader
int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd) {
  void*   pIter = NULL;
  int32_t vgId = TD_VID(pTq->pVnode);

  // update the table list for each consumer handle
  taosWLockLatch(&pTq->lock);
  while (1) {
    pIter = taosHashIterate(pTq->pHandle, pIter);
    if (pIter == NULL) {
      break;
    }

    STqHandle* pTqHandle = (STqHandle*)pIter;
    if (pTqHandle->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      int32_t code = qUpdateTableListForStreamScanner(pTqHandle->execHandle.task, tbUidList, isAdd);
      if (code != 0) {
        tqError("update qualified table error for %s", pTqHandle->subKey);
        continue;
      }
    } else if (pTqHandle->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      if (!isAdd) {
        int32_t sz = taosArrayGetSize(tbUidList);
        for (int32_t i = 0; i < sz; i++) {
          int64_t tbUid = *(int64_t*)taosArrayGet(tbUidList, i);
          taosHashPut(pTqHandle->execHandle.execDb.pFilterOutTbUid, &tbUid, sizeof(int64_t), NULL, 0);
        }
      }
    } else if (pTqHandle->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      if (isAdd) {
        SArray* list = NULL;
        int     ret = qGetTableList(pTqHandle->execHandle.execTb.suid, pTq->pVnode, pTqHandle->execHandle.execTb.node,
                                    &list, pTqHandle->execHandle.task);
        if (ret != TDB_CODE_SUCCESS) {
          tqError("qGetTableList in tqUpdateTbUidList error:%d handle %s consumer:0x%" PRIx64, ret, pTqHandle->subKey,
                  pTqHandle->consumerId);
          taosArrayDestroy(list);
          taosHashCancelIterate(pTq->pHandle, pIter);
          taosWUnLockLatch(&pTq->lock);

          return ret;
        }
        tqReaderSetTbUidList(pTqHandle->execHandle.pTqReader, list, NULL);
        taosArrayDestroy(list);
      } else {
        tqReaderRemoveTbUidList(pTqHandle->execHandle.pTqReader, tbUidList);
      }
    }
  }
  taosWUnLockLatch(&pTq->lock);

  // update the table list handle for each stream scanner/wal reader
  streamMetaWLock(pTq->pStreamMeta);
  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasksMap, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
      int32_t code = qUpdateTableListForStreamScanner(pTask->exec.pExecutor, tbUidList, isAdd);
      if (code != 0) {
        tqError("vgId:%d, s-task:%s update qualified table error for stream task", vgId, pTask->id.idStr);
        continue;
      }
    }
  }

  streamMetaWUnLock(pTq->pStreamMeta);
  return 0;
}
