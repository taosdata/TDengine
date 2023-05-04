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
    metaReaderInit(&mr, pHandle->execHandle.pTqReader->pVnodeMeta, 0);

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

int32_t tqFetchLog(STQ* pTq, STqHandle* pHandle, int64_t* fetchOffset, SWalCkHead** ppCkHead, uint64_t reqId) {
  int32_t code = 0;
  int32_t vgId = TD_VID(pTq->pVnode);

  taosThreadMutexLock(&pHandle->pWalReader->mutex);
  int64_t offset = *fetchOffset;

  while (1) {
    if (walFetchHead(pHandle->pWalReader, offset, *ppCkHead) < 0) {
      tqDebug("tmq poll: consumer:0x%" PRIx64 ", (epoch %d) vgId:%d offset %" PRId64 ", no more log to return, reqId:0x%"PRIx64,
              pHandle->consumerId, pHandle->epoch, vgId, offset, reqId);
      *fetchOffset = offset - 1;
      code = -1;
      goto END;
    }

    tqDebug("vgId:%d, consumer:0x%" PRIx64 " taosx get msg ver %" PRId64 ", type: %s, reqId:0x%" PRIx64, vgId,
            pHandle->consumerId, offset, TMSG_INFO((*ppCkHead)->head.msgType), reqId);

    if ((*ppCkHead)->head.msgType == TDMT_VND_SUBMIT) {
      code = walFetchBody(pHandle->pWalReader, ppCkHead);

      if (code < 0) {
        *fetchOffset = offset;
        code = -1;
        goto END;
      }
      *fetchOffset = offset;
      code = 0;
      goto END;
    } else {
      if (pHandle->fetchMeta) {
        SWalCont* pHead = &((*ppCkHead)->head);
        if (IS_META_MSG(pHead->msgType)) {
          code = walFetchBody(pHandle->pWalReader, ppCkHead);
          if (code < 0) {
            *fetchOffset = offset;
            code = -1;
            goto END;
          }

          if (isValValidForTable(pHandle, pHead)) {
            *fetchOffset = offset;
            code = 0;
            goto END;
          } else {
            offset++;
            continue;
          }
        }
      }
      code = walSkipFetchBody(pHandle->pWalReader, *ppCkHead);
      if (code < 0) {
        *fetchOffset = offset;
        code = -1;
        goto END;
      }
      offset++;
    }
  }

END:
  taosThreadMutexUnlock(&pHandle->pWalReader->mutex);
  return code;
}

STqReader* tqReaderOpen(SVnode* pVnode) {
  STqReader* pReader = taosMemoryCalloc(1, sizeof(STqReader));
  if (pReader == NULL) {
    return NULL;
  }

  pReader->pWalReader = walOpenReader(pVnode->pWal, NULL);
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

void tqCloseReader(STqReader* pReader) {
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

int32_t tqSeekVer(STqReader* pReader, int64_t ver, const char* id) {
  if (walReadSeekVer(pReader->pWalReader, ver) < 0) {
    return -1;
  }
  tqDebug("wal reader seek to ver:%"PRId64" %s", ver, id);
  return 0;
}

int32_t extractSubmitMsgFromWal(SWalReader* pReader, SPackedData* pPackedData) {
  if (walNextValidMsg(pReader) < 0) {
    return -1;
  }

  void*   pBody = POINTER_SHIFT(pReader->pHead->head.body, sizeof(SSubmitReq2Msg));
  int32_t len = pReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
  int64_t ver = pReader->pHead->head.version;

  void* data = taosMemoryMalloc(len);
  if (data == NULL) {
    // todo: for all stream in this vnode, keep this offset in the offset files, and wait for a moment, and then retry
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tqError("vgId:%d, failed to copy submit data for stream processing, since out of memory", 0);
    return -1;
  }

  memcpy(data, pBody, len);
  *pPackedData = (SPackedData){.ver = ver, .msgLen = len, .msgStr = data};
  return 0;
}

// todo ignore the error in wal?
int32_t tqNextBlockInWal(STqReader* pReader) {
  SWalReader* pWalReader = pReader->pWalReader;

  while(1) {
    SArray* pBlockList = pReader->submit.aSubmitTbData;
    if (pBlockList == NULL || pReader->nextBlk >= taosArrayGetSize(pBlockList)) {

      // try next message in wal file
      if (walNextValidMsg(pWalReader) < 0) {
        return FETCH_TYPE__NONE;
      }

      void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
      int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
      int64_t ver = pWalReader->pHead->head.version;

      SDecoder decoder = {0};
      tDecoderInit(&decoder, pBody, bodyLen);
      if (tDecodeSubmitReq(&decoder, &pReader->submit) < 0) {
        tDecoderClear(&decoder);
        tqError("decode wal file error, msgLen:%d, ver:%"PRId64, bodyLen, ver);
        return FETCH_TYPE__NONE;
      }

      tDecoderClear(&decoder);
      pReader->nextBlk = 0;
    }

    size_t numOfBlocks = taosArrayGetSize(pReader->submit.aSubmitTbData);
    while (pReader->nextBlk < numOfBlocks) {
      tqDebug("tq reader next data block %p, %d %" PRId64 " %d", pReader->msg.msgStr, pReader->msg.msgLen,
              pReader->msg.ver, pReader->nextBlk);

      SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);

      if (pReader->tbIdHash == NULL) {
        int32_t code = tqRetrieveDataBlock(pReader->pResBlock, pReader, NULL);
        if (code == TSDB_CODE_SUCCESS && pReader->pResBlock->info.rows > 0) {
          return FETCH_TYPE__DATA;
        }
      }

      void* ret = taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t));
      if (ret != NULL) {
        tqDebug("tq reader return submit block, uid:%"PRId64", ver:%"PRId64, pSubmitTbData->uid, pReader->msg.ver);

        int32_t code = tqRetrieveDataBlock(pReader->pResBlock, pReader, NULL);
        if (code == TSDB_CODE_SUCCESS && pReader->pResBlock->info.rows > 0) {
          return FETCH_TYPE__DATA;
        }
      } else {
        pReader->nextBlk += 1;
        tqDebug("tq reader discard submit block, uid:%"PRId64", continue", pSubmitTbData->uid);
      }
    }

    tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
    pReader->msg.msgStr = NULL;
  }
}

int32_t tqNextBlock(STqReader* pReader, SSDataBlock* pBlock) {
  while (1) {
    if (pReader->msg.msgStr == NULL) {
      if (walNextValidMsg(pReader->pWalReader) < 0) {
        return FETCH_TYPE__NONE;
      }

      void*   pBody = POINTER_SHIFT(pReader->pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
      int32_t bodyLen = pReader->pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
      int64_t ver = pReader->pWalReader->pHead->head.version;

      tqReaderSetSubmitMsg(pReader, pBody, bodyLen, ver);
    }

    while (tqNextBlockImpl(pReader)) {
      int32_t code = tqRetrieveDataBlock(pReader->pResBlock, pReader, NULL);
      if (code != TSDB_CODE_SUCCESS || pBlock->info.rows == 0) {
        continue;
      }

      return FETCH_TYPE__DATA;
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
    tqError("DecodeSSubmitReq2 error, msgLen:%d, ver:%"PRId64, msgLen, ver);
    return -1;
  }

  tDecoderClear(&decoder);
  return 0;
}

bool tqNextBlockImpl(STqReader* pReader) {
  if (pReader->msg.msgStr == NULL) {
    return false;
  }

  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < blockSz) {
    tqDebug("tq reader next data block %p, %d %" PRId64 " %d", pReader->msg.msgStr, pReader->msg.msgLen,
            pReader->msg.ver, pReader->nextBlk);

    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
    if (pReader->tbIdHash == NULL) {
      return true;
    }

    void* ret = taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t));
    if (ret != NULL) {
      tqDebug("tq reader block found, ver:%"PRId64", uid:%"PRId64, pReader->msg.ver, pSubmitTbData->uid);
      return true;
    } else {
      tqDebug("tq reader discard submit block, uid:%"PRId64", continue", pSubmitTbData->uid);
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

int32_t tqRetrieveDataBlock(SSDataBlock* pBlock, STqReader* pReader, SSubmitTbData** pSubmitTbDataRet) {
  tqDebug("tq reader retrieve data block %p, index:%d", pReader->msg.msgStr, pReader->nextBlk);

  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk++);
  if (pSubmitTbDataRet) {
    *pSubmitTbDataRet = pSubmitTbData;
  }

  int32_t sversion = pSubmitTbData->sver;
  int64_t suid = pSubmitTbData->suid;
  int64_t uid = pSubmitTbData->uid;
  pReader->lastBlkUid = uid;

  pBlock->info.id.uid = uid;
  pBlock->info.version = pReader->msg.ver;

  if (pReader->cachedSchemaSuid == 0 || pReader->cachedSchemaVer != sversion || pReader->cachedSchemaSuid != suid) {
    tDeleteSchemaWrapper(pReader->pSchemaWrapper);

    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, uid, sversion, 1);
    if (pReader->pSchemaWrapper == NULL) {
      tqWarn("vgId:%d, cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
             pReader->pWalReader->pWal->cfg.vgId, uid, pReader->cachedSchemaVer);
      pReader->cachedSchemaSuid = 0;
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }

    pReader->cachedSchemaSuid = suid;
    pReader->cachedSchemaVer = sversion;

    SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;

    int32_t numOfCols = taosArrayGetSize(pReader->pColIdList);
    if (numOfCols == 0) {  // all columns are required
      for (int32_t i = 0; i < pSchemaWrapper->nCols; ++i) {
        SSchema*        pColSchema = &pSchemaWrapper->pSchema[i];
        SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);

        int32_t code = blockDataAppendColInfo(pBlock, &colInfo);
        if (code != TSDB_CODE_SUCCESS) {
          blockDataFreeRes(pBlock);
          return -1;
        }
      }
    } else {
      if (numOfCols > pSchemaWrapper->nCols) {
        numOfCols = pSchemaWrapper->nCols;
      }

      int32_t i = 0;
      int32_t j = 0;
      while (i < pSchemaWrapper->nCols && j < numOfCols) {
        SSchema* pColSchema = &pSchemaWrapper->pSchema[i];
        col_id_t colIdSchema = pColSchema->colId;

        col_id_t colIdNeed = *(col_id_t*)taosArrayGet(pReader->pColIdList, j);
        if (colIdSchema < colIdNeed) {
          i++;
        } else if (colIdSchema > colIdNeed) {
          j++;
        } else {
          SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);
          int32_t         code = blockDataAppendColInfo(pBlock, &colInfo);
          if (code != TSDB_CODE_SUCCESS) {
            goto FAIL;
          }
          i++;
          j++;
        }
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
    goto FAIL;
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
        goto FAIL;
      }

      SColData*        pCol = taosArrayGet(pCols, sourceIdx);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      SColVal          colVal;

      if (pCol->nVal != numOfRows) {
        tqError("tqRetrieveDataBlock pCol->nVal:%d != numOfRows:%d", pCol->nVal, numOfRows);
        goto FAIL;
      }

      if (pCol->cid < pColData->info.colId) {
        sourceIdx++;
      } else if (pCol->cid == pColData->info.colId) {
        for (int32_t i = 0; i < pCol->nVal; i++) {
          tColDataGetValue(pCol, i, &colVal);
          if (IS_STR_DATA_TYPE(colVal.type)) {
            if (colVal.value.pData != NULL) {
              char val[65535 + 2] = {0};
              memcpy(varDataVal(val), colVal.value.pData, colVal.value.nData);
              varDataSetLen(val, colVal.value.nData);
              if (colDataAppend(pColData, i, val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
                goto FAIL;
              }
            } else {
              colDataSetNULL(pColData, i);
            }
          } else {
            if (colDataAppend(pColData, i, (void*)&colVal.value.val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
              goto FAIL;
            }
          }
        }
        sourceIdx++;
        targetIdx++;
      } else {
        for (int32_t i = 0; i < pCol->nVal; i++) {
          colDataSetNULL(pColData, i);
        }

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
            sourceIdx++;
            continue;
          } else if (colVal.cid == pColData->info.colId) {
            if (IS_STR_DATA_TYPE(colVal.type)) {
              if (colVal.value.pData != NULL) {
                char val[65535 + 2] = {0};
                memcpy(varDataVal(val), colVal.value.pData, colVal.value.nData);
                varDataSetLen(val, colVal.value.nData);
                if (colDataAppend(pColData, i, val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
                  goto FAIL;
                }
              } else {
                colDataSetNULL(pColData, i);
              }
            } else {
              if (colDataAppend(pColData, i, (void*)&colVal.value.val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
                goto FAIL;
              }
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

FAIL:
  blockDataFreeRes(pBlock);
  return -1;
}

int32_t tqRetrieveTaosxBlock(STqReader* pReader, SArray* blocks, SArray* schemas, SSubmitTbData** pSubmitTbDataRet) {
  tqDebug("tq reader retrieve data block %p, %d", pReader->msg.msgStr, pReader->nextBlk);

  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
  pReader->nextBlk++;

  if (pSubmitTbDataRet) *pSubmitTbDataRet = pSubmitTbData;
  int32_t sversion = pSubmitTbData->sver;
  int64_t suid = pSubmitTbData->suid;
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
        tqDebug("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
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

      tqDebug("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
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

          if (IS_STR_DATA_TYPE(colVal.type)) {
            if (colVal.value.pData != NULL) {
              char val[65535 + 2];
              memcpy(varDataVal(val), colVal.value.pData, colVal.value.nData);
              varDataSetLen(val, colVal.value.nData);
              if (colDataAppend(pColData, curRow - lastRow, val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
                goto FAIL;
              }
            } else {
              colDataSetNULL(pColData, curRow - lastRow);
            }
          } else {
            if (colDataAppend(pColData, curRow - lastRow, (void*)&colVal.value.val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
              goto FAIL;
            }
          }
          sourceIdx++;
          targetIdx++;
        }
      }

      curRow++;
    }
  } else {
    SSchemaWrapper* pWrapper = pReader->pSchemaWrapper;
    STSchema* pTSchema = tBuildTSchema(pWrapper->pSchema, pWrapper->nCols, pWrapper->version);
    SArray* pRows = pSubmitTbData->aRowP;

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
        tqDebug("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
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

      tqDebug("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
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
          if (IS_STR_DATA_TYPE(colVal.type)) {
            if (colVal.value.pData != NULL) {
              char val[65535 + 2];
              memcpy(varDataVal(val), colVal.value.pData, colVal.value.nData);
              varDataSetLen(val, colVal.value.nData);
              if (colDataAppend(pColData, curRow - lastRow, val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
                goto FAIL;
              }
            } else {
              colDataSetNULL(pColData, curRow - lastRow);
            }
          } else {
            if (colDataAppend(pColData, curRow - lastRow, (void*)&colVal.value.val, !COL_VAL_IS_VALUE(&colVal)) < 0) {
              goto FAIL;
            }
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

int tqReaderSetTbUidList(STqReader* pReader, const SArray* tbUidList) {
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
        SArray*     qa = taosArrayInit(4, sizeof(tb_uid_t));
        SMetaReader mr = {0};
        metaReaderInit(&mr, pTq->pVnode->pMeta, 0);
        for (int32_t i = 0; i < taosArrayGetSize(tbUidList); ++i) {
          uint64_t* id = (uint64_t*)taosArrayGet(tbUidList, i);

          int32_t code = metaGetTableEntryByUidCache(&mr, *id);
          if (code != TSDB_CODE_SUCCESS) {
            tqError("failed to get table meta, uid:%" PRIu64 " code:%s", *id, tstrerror(terrno));
            continue;
          }

          tDecoderClear(&mr.coder);
          if (mr.me.type != TSDB_CHILD_TABLE || mr.me.ctbEntry.suid != pTqHandle->execHandle.execTb.suid) {
            tqDebug("table uid %" PRId64 " does not add to tq handle", *id);
            continue;
          }

          tqDebug("table uid %" PRId64 " add to tq handle", *id);
          taosArrayPush(qa, id);
        }

        metaReaderClear(&mr);
        if (taosArrayGetSize(qa) > 0) {
          tqReaderAddTbUidList(pTqHandle->execHandle.pTqReader, qa);
        }

        taosArrayDestroy(qa);
      } else {
        tqReaderRemoveTbUidList(pTqHandle->execHandle.pTqReader, tbUidList);
      }
    }
  }

  // update the table list handle for each stream scanner/wal reader
  taosWLockLatch(&pTq->pStreamMeta->lock);
  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) {
      break;
    }

    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
      int32_t code = qUpdateTableListForStreamScanner(pTask->exec.pExecutor, tbUidList, isAdd);
      if (code != 0) {
        tqError("vgId:%d, s-task:%s update qualified table error for stream task", vgId, pTask->id.idStr);
        continue;
      }
    }
  }

  taosWUnLockLatch(&pTq->pStreamMeta->lock);

  return 0;
}
