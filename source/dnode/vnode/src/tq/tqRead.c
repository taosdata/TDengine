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
  SDecoder dcoder = {0};
  void*    data = POINTER_SHIFT(body, sizeof(SMsgHead));
  int32_t  len = bodyLen - sizeof(SMsgHead);
  tDecoderInit(&dcoder, data, len);

  if (msgType == TDMT_VND_CREATE_STB || msgType == TDMT_VND_ALTER_STB) {
    SVCreateStbReq req = {0};
    if (tDecodeSVCreateStbReq(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  } else if (msgType == TDMT_VND_DROP_STB) {
    SVDropStbReq req = {0};
    if (tDecodeSVDropStbReq(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  } else if (msgType == TDMT_VND_CREATE_TABLE) {
    SVCreateTbBatchReq req = {0};
    if (tDecodeSVCreateTbBatchReq(&dcoder, &req) < 0) {
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
      if (reqNew.pArray == NULL) {
        tDeleteSVCreateTbBatchReq(&req);
        goto end;
      }
      for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
        pCreateReq = req.pReqs + iReq;
        if (pCreateReq->type == TSDB_CHILD_TABLE && pCreateReq->ctb.suid == tbSuid) {
          reqNew.nReqs++;
          if (taosArrayPush(reqNew.pArray, pCreateReq) == NULL) {
            taosArrayDestroy(reqNew.pArray);
            tDeleteSVCreateTbBatchReq(&req);
            goto end;
          }
        }
      }

      int     tlen = 0;
      int32_t ret = 0;
      tEncodeSize(tEncodeSVCreateTbBatchReq, &reqNew, tlen, ret);
      void* buf = taosMemoryMalloc(tlen);
      if (NULL == buf) {
        taosArrayDestroy(reqNew.pArray);
        tDeleteSVCreateTbBatchReq(&req);
        goto end;
      }
      SEncoder coderNew = {0};
      tEncoderInit(&coderNew, buf, tlen - sizeof(SMsgHead));
      ret = tEncodeSVCreateTbBatchReq(&coderNew, &reqNew);
      tEncoderClear(&coderNew);
      if (ret < 0) {
        taosMemoryFree(buf);
        taosArrayDestroy(reqNew.pArray);
        tDeleteSVCreateTbBatchReq(&req);
        goto end;
      }
      (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
      pHead->bodyLen = tlen + sizeof(SMsgHead);
      taosMemoryFree(buf);
      taosArrayDestroy(reqNew.pArray);
    }

    tDeleteSVCreateTbBatchReq(&req);
  } else if (msgType == TDMT_VND_ALTER_TABLE) {
    SVAlterTbReq req = {0};

    if (tDecodeSVAlterTbReq(&dcoder, &req) < 0) {
      goto end;
    }

    SMetaReader mr = {0};
    metaReaderDoInit(&mr, pHandle->execHandle.pTqReader->pVnodeMeta, META_READER_LOCK);

    if (metaGetTableEntryByName(&mr, req.tbName) < 0) {
      metaReaderClear(&mr);
      goto end;
    }
    realTbSuid = mr.me.ctbEntry.suid;
    metaReaderClear(&mr);
  } else if (msgType == TDMT_VND_DROP_TABLE) {
    SVDropTbBatchReq req = {0};

    if (tDecodeSVDropTbBatchReq(&dcoder, &req) < 0) {
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
      if (reqNew.pArray == NULL) {
        goto end;
      }
      for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
        pDropReq = req.pReqs + iReq;
        if (pDropReq->suid == tbSuid) {
          reqNew.nReqs++;
          if (taosArrayPush(reqNew.pArray, pDropReq) == NULL) {
            taosArrayDestroy(reqNew.pArray);
            goto end;
          }
        }
      }

      int     tlen = 0;
      int32_t ret = 0;
      tEncodeSize(tEncodeSVDropTbBatchReq, &reqNew, tlen, ret);
      void* buf = taosMemoryMalloc(tlen);
      if (NULL == buf) {
        taosArrayDestroy(reqNew.pArray);
        goto end;
      }
      SEncoder coderNew = {0};
      tEncoderInit(&coderNew, buf, tlen - sizeof(SMsgHead));
      ret = tEncodeSVDropTbBatchReq(&coderNew, &reqNew);
      tEncoderClear(&coderNew);
      if (ret != 0) {
        taosMemoryFree(buf);
        taosArrayDestroy(reqNew.pArray);
        goto end;
      }
      (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
      pHead->bodyLen = tlen + sizeof(SMsgHead);
      taosMemoryFree(buf);
      taosArrayDestroy(reqNew.pArray);
    }
  } else if (msgType == TDMT_VND_DELETE) {
    SDeleteRes req = {0};
    if (tDecodeDeleteRes(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  }

end:
  tDecoderClear(&dcoder);
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

  tqDebug("vgId:%d, start to fetch wal, index:%" PRId64 ", last:%" PRId64 " commit:%" PRId64 ", applied:%" PRId64
          ", 0x%" PRIx64,
          vgId, offset, lastVer, committedVer, appliedVer, id);

  while (offset <= appliedVer) {
    if (walFetchHead(pHandle->pWalReader, offset) < 0) {
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", (epoch %d) vgId:%d offset %" PRId64
              ", no more log to return,QID:0x%" PRIx64 " 0x%" PRIx64,
              pHandle->consumerId, pHandle->epoch, vgId, offset, reqId, id);
      goto END;
    }

    tqDebug("vgId:%d, consumer:0x%" PRIx64 " taosx get msg ver %" PRId64 ", type: %s,QID:0x%" PRIx64 " 0x%" PRIx64,
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

bool tqGetTablePrimaryKey(STqReader* pReader) { return pReader->hasPrimaryKey; }

void tqSetTablePrimaryKey(STqReader* pReader, int64_t uid) {
  bool            ret = false;
  SSchemaWrapper* schema = metaGetTableSchema(pReader->pVnodeMeta, uid, -1, 1);
  if (schema && schema->nCols >= 2 && schema->pSchema[1].flags & COL_IS_KEY) {
    ret = true;
  }
  tDeleteSchemaWrapper(schema);
  pReader->hasPrimaryKey = ret;
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
  pReader->pResBlock = NULL;

  int32_t code = createDataBlock(&pReader->pResBlock);
  if (code) {
    terrno = code;
  }

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
    TAOS_CHECK_RETURN(walNextValidMsg(pReader));

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

      (void)memcpy(data, pBody, len);
      SPackedData data1 = (SPackedData){.ver = ver, .msgLen = len, .msgStr = data};

      code = streamDataSubmitNew(&data1, STREAM_INPUT__DATA_SUBMIT, (SStreamDataSubmit**)pItem);
      if (code != 0) {
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
      tqError("s-task:%s invalid msg type:%d, ver:%" PRId64, id, pCont->msgType, ver);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }

    return code;
  }
}

bool tqNextBlockInWal(STqReader* pReader, const char* id, int sourceExcluded) {
  SWalReader* pWalReader = pReader->pWalReader;

  int64_t st = taosGetTimestampMs();
  while (1) {
    int32_t numOfBlocks = taosArrayGetSize(pReader->submit.aSubmitTbData);
    while (pReader->nextBlk < numOfBlocks) {
      tqTrace("tq reader next data block %d/%d, len:%d %" PRId64, pReader->nextBlk, numOfBlocks, pReader->msg.msgLen,
              pReader->msg.ver);

      SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
      if (pSubmitTbData == NULL) {
        tqError("tq reader next data block %d/%d, len:%d %" PRId64, pReader->nextBlk, numOfBlocks, pReader->msg.msgLen,
                pReader->msg.ver);
        return false;
      }
      if ((pSubmitTbData->flags & sourceExcluded) != 0) {
        pReader->nextBlk += 1;
        continue;
      }
      if (pReader->tbIdHash == NULL || taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t)) != NULL) {
        tqTrace("tq reader return submit block, uid:%" PRId64, pSubmitTbData->uid);
        SSDataBlock* pRes = NULL;
        int32_t      code = tqRetrieveDataBlock(pReader, &pRes, NULL);
        if (code == TSDB_CODE_SUCCESS) {
          return true;
        }
      } else {
        pReader->nextBlk += 1;
        tqTrace("tq reader discard submit block, uid:%" PRId64 ", continue", pSubmitTbData->uid);
      }
    }

    tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
    pReader->msg.msgStr = NULL;

    int64_t elapsed = taosGetTimestampMs() - st;
    if (elapsed > 1000 || elapsed < 0) {
      return false;
    }

    // try next message in wal file
    if (walNextValidMsg(pWalReader) < 0) {
      return false;
    }

    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int64_t ver = pWalReader->pHead->head.version;
    if (tqReaderSetSubmitMsg(pReader, pBody, bodyLen, ver) != 0) {
      return false;
    }
    pReader->nextBlk = 0;
  }
}

int32_t tqReaderSetSubmitMsg(STqReader* pReader, void* msgStr, int32_t msgLen, int64_t ver) {
  pReader->msg.msgStr = msgStr;
  pReader->msg.msgLen = msgLen;
  pReader->msg.ver = ver;

  tqDebug("tq reader set msg %p %d", msgStr, msgLen);
  SDecoder decoder = {0};

  tDecoderInit(&decoder, pReader->msg.msgStr, pReader->msg.msgLen);
  int32_t code = tDecodeSubmitReq(&decoder, &pReader->submit);
  if (code != 0) {
    tDecoderClear(&decoder);
    tqError("DecodeSSubmitReq2 error, msgLen:%d, ver:%" PRId64, msgLen, ver);
    return code;
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
    if (pSubmitTbData == NULL) {
      return false;
    }
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
    if (pSubmitTbData == NULL) return false;
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
  int32_t code = 0;

  int32_t cnt = 0;
  for (int32_t i = 0; i < pSrc->nCols; i++) {
    cnt += mask[i];
  }

  pDst->nCols = cnt;
  pDst->pSchema = taosMemoryCalloc(cnt, sizeof(SSchema));
  if (pDst->pSchema == NULL) {
    return TAOS_GET_TERRNO(terrno);
  }

  int32_t j = 0;
  for (int32_t i = 0; i < pSrc->nCols; i++) {
    if (mask[i]) {
      pDst->pSchema[j++] = pSrc->pSchema[i];
      SColumnInfoData colInfo =
          createColumnInfoData(pSrc->pSchema[i].type, pSrc->pSchema[i].bytes, pSrc->pSchema[i].colId);
      code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != 0) {
        return code;
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

      col_id_t* pColIdNeed = (col_id_t*)taosArrayGet(pColIdList, j);
      if (pColIdNeed == NULL) {
        break;
      }
      if (colIdSchema < *pColIdNeed) {
        i++;
      } else if (colIdSchema > *pColIdNeed) {
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

  if (IS_VAR_DATA_TYPE(pColVal->value.type)) {
    char val[65535 + 2] = {0};
    if (COL_VAL_IS_VALUE(pColVal)) {
      if (pColVal->value.pData != NULL) {
        (void)memcpy(varDataVal(val), pColVal->value.pData, pColVal->value.nData);
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
  int32_t        code = 0;
  int32_t        line = 0;
  STSchema*      pTSchema = NULL;
  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk++);
  TSDB_CHECK_NULL(pSubmitTbData, code, line, END, terrno);
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
      return TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
    }

    pReader->cachedSchemaUid = uid;
    pReader->cachedSchemaSuid = suid;
    pReader->cachedSchemaVer = sversion;

    if (pReader->cachedSchemaVer != pReader->pSchemaWrapper->version) {
      tqError("vgId:%d, schema version mismatch, suid:%" PRId64 ", uid:%" PRId64 ", version:%d, cached version:%d",
              vgId, suid, uid, sversion, pReader->pSchemaWrapper->version);
      return TSDB_CODE_TQ_INTERNAL_ERROR;
    }
    if (blockDataGetNumOfCols(pBlock) == 0) {
      code = buildResSDataBlock(pReader->pResBlock, pReader->pSchemaWrapper, pReader->pColIdList);
      TSDB_CHECK_CODE(code, line, END);
    }
  }

  int32_t numOfRows = 0;
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SColData* pCol = taosArrayGet(pSubmitTbData->aCol, 0);
    TSDB_CHECK_NULL(pCol, code, line, END, terrno);
    numOfRows = pCol->nVal;
  } else {
    numOfRows = taosArrayGetSize(pSubmitTbData->aRowP);
  }

  code = blockDataEnsureCapacity(pBlock, numOfRows);
  TSDB_CHECK_CODE(code, line, END);
  pBlock->info.rows = numOfRows;
  int32_t colActual = blockDataGetNumOfCols(pBlock);

  // convert and scan one block
  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    SArray* pCols = pSubmitTbData->aCol;
    int32_t numOfCols = taosArrayGetSize(pCols);
    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    while (targetIdx < colActual) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      TSDB_CHECK_NULL(pColData, code, line, END, terrno);
      if (sourceIdx >= numOfCols) {
        tqError("lostdata tqRetrieveDataBlock sourceIdx:%d >= numOfCols:%d", sourceIdx, numOfCols);
        colDataSetNNULL(pColData, 0, numOfRows);
        targetIdx++;
        continue;
      }

      SColData* pCol = taosArrayGet(pCols, sourceIdx);
      TSDB_CHECK_NULL(pCol, code, line, END, terrno);
      SColVal colVal = {0};
      tqTrace("lostdata colActual:%d, sourceIdx:%d, targetIdx:%d, numOfCols:%d, source cid:%d, dst cid:%d", colActual,
              sourceIdx, targetIdx, numOfCols, pCol->cid, pColData->info.colId);
      if (pCol->cid < pColData->info.colId) {
        sourceIdx++;
      } else if (pCol->cid == pColData->info.colId) {
        for (int32_t i = 0; i < pCol->nVal; i++) {
          tColDataGetValue(pCol, i, &colVal);
          code = doSetVal(pColData, i, &colVal);
          TSDB_CHECK_CODE(code, line, END);
        }
        sourceIdx++;
        targetIdx++;
      } else {
        colDataSetNNULL(pColData, 0, numOfRows);
        targetIdx++;
      }
    }
  } else {
    SArray*         pRows = pSubmitTbData->aRowP;
    SSchemaWrapper* pWrapper = pReader->pSchemaWrapper;
    pTSchema = tBuildTSchema(pWrapper->pSchema, pWrapper->nCols, pWrapper->version);
    TSDB_CHECK_NULL(pTSchema, code, line, END, terrno);

    for (int32_t i = 0; i < numOfRows; i++) {
      SRow* pRow = taosArrayGetP(pRows, i);
      TSDB_CHECK_NULL(pRow, code, line, END, terrno);
      int32_t sourceIdx = 0;
      for (int32_t j = 0; j < colActual; j++) {
        SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, j);
        TSDB_CHECK_NULL(pColData, code, line, END, terrno);

        while (1) {
          SColVal colVal = {0};
          code = tRowGet(pRow, pTSchema, sourceIdx, &colVal);
          TSDB_CHECK_CODE(code, line, END);

          if (colVal.cid < pColData->info.colId) {
            sourceIdx++;
            continue;
          } else if (colVal.cid == pColData->info.colId) {
            code = doSetVal(pColData, i, &colVal);
            TSDB_CHECK_CODE(code, line, END);
            sourceIdx++;
            break;
          } else {
            colDataSetNULL(pColData, i);
            break;
          }
        }
      }
    }
  }

END:
  if (code != 0) {
    tqError("tqRetrieveDataBlock failed, line:%d, code:%d", line, code);
  }
  taosMemoryFreeClear(pTSchema);
  return code;
}

#define PROCESS_VAL                                      \
  if (curRow == 0) {                                     \
    assigned[j] = !COL_VAL_IS_NONE(&colVal);             \
    buildNew = true;                                     \
  } else {                                               \
    bool currentRowAssigned = !COL_VAL_IS_NONE(&colVal); \
    if (currentRowAssigned != assigned[j]) {             \
      assigned[j] = currentRowAssigned;                  \
      buildNew = true;                                   \
    }                                                    \
  }

#define SET_DATA                                                     \
  if (colVal.cid < pColData->info.colId) {                           \
    sourceIdx++;                                                     \
  } else if (colVal.cid == pColData->info.colId) {                   \
    TQ_ERR_GO_TO_END(doSetVal(pColData, curRow - lastRow, &colVal)); \
    sourceIdx++;                                                     \
    targetIdx++;                                                     \
  }

static int32_t processBuildNew(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas,
                               SSchemaWrapper* pSchemaWrapper, char* assigned, int32_t numOfRows, int32_t curRow,
                               int32_t* lastRow) {
  int32_t         code = 0;
  SSchemaWrapper* pSW = NULL;
  SSDataBlock*    block = NULL;
  if (taosArrayGetSize(blocks) > 0) {
    SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pLastBlock);
    pLastBlock->info.rows = curRow - *lastRow;
    *lastRow = curRow;
  }

  block = taosMemoryCalloc(1, sizeof(SSDataBlock));
  TQ_NULL_GO_TO_END(block);

  pSW = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
  TQ_NULL_GO_TO_END(pSW);

  TQ_ERR_GO_TO_END(tqMaskBlock(pSW, block, pSchemaWrapper, assigned));
  tqTrace("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
          (int32_t)taosArrayGetSize(block->pDataBlock));

  block->info.id.uid = pSubmitTbData->uid;
  block->info.version = pReader->msg.ver;
  TQ_ERR_GO_TO_END(blockDataEnsureCapacity(block, numOfRows - curRow));
  TQ_NULL_GO_TO_END(taosArrayPush(blocks, block));
  TQ_NULL_GO_TO_END(taosArrayPush(schemas, &pSW));
  pSW = NULL;
  taosMemoryFreeClear(block);

END:
  tDeleteSchemaWrapper(pSW);
  blockDataFreeRes(block);
  taosMemoryFree(block);
  return code;
}
static int32_t tqProcessColData(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas) {
  int32_t code = 0;
  int32_t curRow = 0;
  int32_t lastRow = 0;

  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;
  char*           assigned = taosMemoryCalloc(1, pSchemaWrapper->nCols);
  TQ_NULL_GO_TO_END(assigned);

  SArray*   pCols = pSubmitTbData->aCol;
  SColData* pCol = taosArrayGet(pCols, 0);
  TQ_NULL_GO_TO_END(pCol);
  int32_t numOfRows = pCol->nVal;
  int32_t numOfCols = taosArrayGetSize(pCols);
  for (int32_t i = 0; i < numOfRows; i++) {
    bool buildNew = false;

    for (int32_t j = 0; j < numOfCols; j++) {
      pCol = taosArrayGet(pCols, j);
      TQ_NULL_GO_TO_END(pCol);
      SColVal colVal = {0};
      tColDataGetValue(pCol, i, &colVal);
      PROCESS_VAL
    }

    if (buildNew) {
      TQ_ERR_GO_TO_END(processBuildNew(pReader, pSubmitTbData, blocks, schemas, pSchemaWrapper, assigned, numOfRows,
                                       curRow, &lastRow));
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pBlock);

    tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    int32_t colActual = blockDataGetNumOfCols(pBlock);
    while (targetIdx < colActual) {
      pCol = taosArrayGet(pCols, sourceIdx);
      TQ_NULL_GO_TO_END(pCol);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      TQ_NULL_GO_TO_END(pColData);
      SColVal colVal = {0};
      tColDataGetValue(pCol, i, &colVal);
      SET_DATA
    }

    curRow++;
  }
  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  pLastBlock->info.rows = curRow - lastRow;

END:
  taosMemoryFree(assigned);
  return code;
}

int32_t tqProcessRowData(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas) {
  int32_t   code = 0;
  STSchema* pTSchema = NULL;

  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;
  char*           assigned = taosMemoryCalloc(1, pSchemaWrapper->nCols);
  TQ_NULL_GO_TO_END(assigned);

  int32_t curRow = 0;
  int32_t lastRow = 0;
  SArray* pRows = pSubmitTbData->aRowP;
  int32_t numOfRows = taosArrayGetSize(pRows);
  pTSchema = tBuildTSchema(pSchemaWrapper->pSchema, pSchemaWrapper->nCols, pSchemaWrapper->version);

  for (int32_t i = 0; i < numOfRows; i++) {
    bool  buildNew = false;
    SRow* pRow = taosArrayGetP(pRows, i);
    TQ_NULL_GO_TO_END(pRow);

    for (int32_t j = 0; j < pTSchema->numOfCols; j++) {
      SColVal colVal = {0};
      TQ_ERR_GO_TO_END(tRowGet(pRow, pTSchema, j, &colVal));
      PROCESS_VAL
    }

    if (buildNew) {
      TQ_ERR_GO_TO_END(processBuildNew(pReader, pSubmitTbData, blocks, schemas, pSchemaWrapper, assigned, numOfRows,
                                       curRow, &lastRow));
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pBlock);

    tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    int32_t colActual = blockDataGetNumOfCols(pBlock);
    while (targetIdx < colActual) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      SColVal          colVal = {0};
      TQ_ERR_GO_TO_END(tRowGet(pRow, pTSchema, sourceIdx, &colVal));
      SET_DATA
    }

    curRow++;
  }
  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  pLastBlock->info.rows = curRow - lastRow;

END:
  taosMemoryFreeClear(pTSchema);
  taosMemoryFree(assigned);
  return code;
}

int32_t tqRetrieveTaosxBlock(STqReader* pReader, SArray* blocks, SArray* schemas, SSubmitTbData** pSubmitTbDataRet) {
  tqDebug("tq reader retrieve data block %p, %d", pReader->msg.msgStr, pReader->nextBlk);
  SSDataBlock* block = NULL;

  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
  if (pSubmitTbData == NULL) {
    return terrno;
  }
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
    return TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
  }

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    return tqProcessColData(pReader, pSubmitTbData, blocks, schemas);
  } else {
    return tqProcessRowData(pReader, pSubmitTbData, blocks, schemas);
  }
}

void tqReaderSetColIdList(STqReader* pReader, SArray* pColIdList) { pReader->pColIdList = pColIdList; }

void tqReaderSetTbUidList(STqReader* pReader, const SArray* tbUidList, const char* id) {
  if (pReader->tbIdHash) {
    taosHashClear(pReader->tbIdHash);
  } else {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      tqError("s-task:%s failed to init hash table", id);
      return;
    }
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    if (pKey && taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0) != 0) {
      tqError("s-task:%s failed to add table uid:%" PRId64 " to hash", id, *pKey);
      continue;
    }
  }

  tqDebug("s-task:%s %d tables are set to be queried target table", id, (int32_t)taosArrayGetSize(tbUidList));
}

void tqReaderAddTbUidList(STqReader* pReader, const SArray* pTableUidList) {
  if (pReader->tbIdHash == NULL) {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      tqError("failed to init hash table");
      return;
    }
  }

  int32_t numOfTables = taosArrayGetSize(pTableUidList);
  for (int i = 0; i < numOfTables; i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(pTableUidList, i);
    if (taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0) != 0) {
      tqError("failed to add table uid:%" PRId64 " to hash", *pKey);
      continue;
    }
  }
}

bool tqReaderIsQueriedTable(STqReader* pReader, uint64_t uid) {
  return taosHashGet(pReader->tbIdHash, &uid, sizeof(uint64_t));
}

bool tqCurrentBlockConsumed(const STqReader* pReader) { return pReader->msg.msgStr == NULL; }

void tqReaderRemoveTbUidList(STqReader* pReader, const SArray* tbUidList) {
  for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    if (pKey && taosHashRemove(pReader->tbIdHash, pKey, sizeof(int64_t)) != 0) {
      tqError("failed to remove table uid:%" PRId64 " from hash", *pKey);
    }
  }
}

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
          int64_t* tbUid = (int64_t*)taosArrayGet(tbUidList, i);
          if (tbUid &&
              taosHashPut(pTqHandle->execHandle.execDb.pFilterOutTbUid, tbUid, sizeof(int64_t), NULL, 0) != 0) {
            tqError("failed to add table uid:%" PRId64 " to hash", *tbUid);
            continue;
          }
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
    if ((pTask->info.taskLevel == TASK_LEVEL__SOURCE) && (pTask->exec.pExecutor != NULL)) {
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
