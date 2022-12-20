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
    metaReaderInit(&mr, pHandle->execHandle.pExecReader->pVnodeMeta, 0);

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
  } else {
    ASSERT(0);
  }

end:
  tDecoderClear(&coder);
  return tbSuid == realTbSuid;
}

int64_t tqFetchLog(STQ* pTq, STqHandle* pHandle, int64_t* fetchOffset, SWalCkHead** ppCkHead) {
  int32_t code = 0;
  taosThreadMutexLock(&pHandle->pWalReader->mutex);
  int64_t offset = *fetchOffset;

  while (1) {
    if (walFetchHead(pHandle->pWalReader, offset, *ppCkHead) < 0) {
      tqDebug("tmq poll: consumer:%" PRId64 ", (epoch %d) vgId:%d offset %" PRId64 ", no more log to return",
              pHandle->consumerId, pHandle->epoch, TD_VID(pTq->pVnode), offset);
      *fetchOffset = offset - 1;
      code = -1;
      goto END;
    }

    tqDebug("vgId:%d, taosx get msg ver %" PRId64 ", type: %s", pTq->pVnode->config.vgId, offset,
            TMSG_INFO((*ppCkHead)->head.msgType));

    if ((*ppCkHead)->head.msgType == TDMT_VND_SUBMIT) {
      code = walFetchBody(pHandle->pWalReader, ppCkHead);

      if (code < 0) {
        ASSERT(0);
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
            ASSERT(0);
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
        ASSERT(0);
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

STqReader* tqOpenReader(SVnode* pVnode) {
  STqReader* pReader = taosMemoryMalloc(sizeof(STqReader));
  if (pReader == NULL) {
    return NULL;
  }

  pReader->pWalReader = walOpenReader(pVnode->pWal, NULL);
  if (pReader->pWalReader == NULL) {
    taosMemoryFree(pReader);
    return NULL;
  }

  pReader->pVnodeMeta = pVnode->pMeta;
  pReader->pMsg = NULL;
  pReader->ver = -1;
  pReader->pColIdList = NULL;
  pReader->cachedSchemaVer = 0;
  pReader->cachedSchemaSuid = 0;
  pReader->pSchema = NULL;
  pReader->pSchemaWrapper = NULL;
  pReader->tbIdHash = NULL;
  return pReader;
}

void tqCloseReader(STqReader* pReader) {
  // close wal reader
  if (pReader->pWalReader) {
    walCloseReader(pReader->pWalReader);
  }
  // free cached schema
  if (pReader->pSchema) {
    taosMemoryFree(pReader->pSchema);
  }
  if (pReader->pSchemaWrapper) {
    tDeleteSSchemaWrapper(pReader->pSchemaWrapper);
  }
  if (pReader->pColIdList) {
    taosArrayDestroy(pReader->pColIdList);
  }
  // free hash
  taosHashCleanup(pReader->tbIdHash);
  taosMemoryFree(pReader);
}

int32_t tqSeekVer(STqReader* pReader, int64_t ver) {
  if (walReadSeekVer(pReader->pWalReader, ver) < 0) {
    ASSERT(pReader->pWalReader->curInvalid);
    ASSERT(pReader->pWalReader->curVersion == ver);
    return -1;
  }
  ASSERT(pReader->pWalReader->curVersion == ver);
  return 0;
}

int32_t tqNextBlock(STqReader* pReader, SFetchRet* ret) {
  bool fromProcessedMsg = pReader->pMsg != NULL;

  while (1) {
    if (!fromProcessedMsg) {
      if (walNextValidMsg(pReader->pWalReader) < 0) {
        pReader->ver =
            pReader->pWalReader->curVersion - (pReader->pWalReader->curInvalid | pReader->pWalReader->curStopped);
        ret->offset.type = TMQ_OFFSET__LOG;
        ret->offset.version = pReader->ver;
        ret->fetchType = FETCH_TYPE__NONE;
        tqDebug("return offset %" PRId64 ", no more valid", ret->offset.version);
        ASSERT(ret->offset.version >= 0);
        return -1;
      }
      void* body = pReader->pWalReader->pHead->head.body;
#if 0
      if (pReader->pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
        // TODO do filter
        ret->fetchType = FETCH_TYPE__META;
        ret->meta = pReader->pWalReader->pHead->head.body;
        return 0;
      } else {
#endif
      tqReaderSetDataMsg(pReader, body, pReader->pWalReader->pHead->head.version);
#if 0
      }
#endif
    }

    while (tqNextDataBlock(pReader)) {
      // TODO mem free
      memset(&ret->data, 0, sizeof(SSDataBlock));
      int32_t code = tqRetrieveDataBlock(&ret->data, pReader);
      if (code != 0 || ret->data.info.rows == 0) {
        ASSERT(0);
        continue;
      }
      ret->fetchType = FETCH_TYPE__DATA;
      tqDebug("return data rows %d", ret->data.info.rows);
      return 0;
    }

    if (fromProcessedMsg) {
      ret->offset.type = TMQ_OFFSET__LOG;
      ret->offset.version = pReader->ver;
      ASSERT(pReader->ver >= 0);
      ret->fetchType = FETCH_TYPE__SEP;
      tqDebug("return offset %" PRId64 ", processed finish", ret->offset.version);
      return 0;
    }
  }
}

int32_t tqReaderSetDataMsg(STqReader* pReader, const SSubmitReq* pMsg, int64_t ver) {
  pReader->pMsg = pMsg;

  if (tInitSubmitMsgIter(pMsg, &pReader->msgIter) < 0) return -1;
  while (true) {
    if (tGetSubmitMsgNext(&pReader->msgIter, &pReader->pBlock) < 0) return -1;
    if (pReader->pBlock == NULL) break;
  }

  if (tInitSubmitMsgIter(pMsg, &pReader->msgIter) < 0) return -1;
  pReader->ver = ver;
  memset(&pReader->blkIter, 0, sizeof(SSubmitBlkIter));
  return 0;
}

bool tqNextDataBlock(STqReader* pReader) {
  if (pReader->pMsg == NULL) return false;
  while (1) {
    if (tGetSubmitMsgNext(&pReader->msgIter, &pReader->pBlock) < 0) {
      return false;
    }
    if (pReader->pBlock == NULL) {
      pReader->pMsg = NULL;
      return false;
    }

    if (pReader->tbIdHash == NULL) {
      return true;
    }
    void* ret = taosHashGet(pReader->tbIdHash, &pReader->msgIter.uid, sizeof(int64_t));
    /*tqDebug("search uid %" PRId64, pHandle->msgIter.uid);*/
    if (ret != NULL) {
      /*tqDebug("find   uid %" PRId64, pHandle->msgIter.uid);*/
      return true;
    }
  }
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

bool tqNextDataBlockFilterOut(STqReader* pHandle, SHashObj* filterOutUids) {
  while (1) {
    if (tGetSubmitMsgNext(&pHandle->msgIter, &pHandle->pBlock) < 0) {
      return false;
    }
    if (pHandle->pBlock == NULL) return false;

    ASSERT(pHandle->tbIdHash == NULL);
    void* ret = taosHashGet(filterOutUids, &pHandle->msgIter.uid, sizeof(int64_t));
    if (ret == NULL) {
      return true;
    }
  }
  return false;
}

int32_t tqRetrieveDataBlock(SSDataBlock* pBlock, STqReader* pReader) {
  // TODO: cache multiple schema
  int32_t sversion = htonl(pReader->pBlock->sversion);
  if (pReader->cachedSchemaSuid == 0 || pReader->cachedSchemaVer != sversion ||
      pReader->cachedSchemaSuid != pReader->msgIter.suid) {
    if (pReader->pSchema) taosMemoryFree(pReader->pSchema);
    pReader->pSchema = metaGetTbTSchema(pReader->pVnodeMeta, pReader->msgIter.uid, sversion, 1);
    if (pReader->pSchema == NULL) {
      tqWarn("cannot found tsschema for table: uid:%" PRId64 " (suid:%" PRId64 "), version %d, possibly dropped table",
             pReader->msgIter.uid, pReader->msgIter.suid, pReader->cachedSchemaVer);
      /*ASSERT(0);*/
      pReader->cachedSchemaSuid = 0;
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }

    if (pReader->pSchemaWrapper) tDeleteSSchemaWrapper(pReader->pSchemaWrapper);
    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, pReader->msgIter.uid, sversion, 1);
    if (pReader->pSchemaWrapper == NULL) {
      tqWarn("cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
             pReader->msgIter.uid, pReader->cachedSchemaVer);
      /*ASSERT(0);*/
      pReader->cachedSchemaSuid = 0;
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }
    pReader->cachedSchemaVer = sversion;
    pReader->cachedSchemaSuid = pReader->msgIter.suid;
  }

  STSchema*       pTschema = pReader->pSchema;
  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;

  int32_t colNumNeed = taosArrayGetSize(pReader->pColIdList);

  if (colNumNeed == 0) {
    int32_t colMeta = 0;
    while (colMeta < pSchemaWrapper->nCols) {
      SSchema*        pColSchema = &pSchemaWrapper->pSchema[colMeta];
      SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);
      int32_t         code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto FAIL;
      }
      colMeta++;
    }
  } else {
    if (colNumNeed > pSchemaWrapper->nCols) {
      colNumNeed = pSchemaWrapper->nCols;
    }

    int32_t colMeta = 0;
    int32_t colNeed = 0;
    while (colMeta < pSchemaWrapper->nCols && colNeed < colNumNeed) {
      SSchema* pColSchema = &pSchemaWrapper->pSchema[colMeta];
      col_id_t colIdSchema = pColSchema->colId;
      col_id_t colIdNeed = *(col_id_t*)taosArrayGet(pReader->pColIdList, colNeed);
      if (colIdSchema < colIdNeed) {
        colMeta++;
      } else if (colIdSchema > colIdNeed) {
        colNeed++;
      } else {
        SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);
        int32_t         code = blockDataAppendColInfo(pBlock, &colInfo);
        if (code != TSDB_CODE_SUCCESS) {
          goto FAIL;
        }
        colMeta++;
        colNeed++;
      }
    }
  }

  if (blockDataEnsureCapacity(pBlock, pReader->msgIter.numOfRows) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto FAIL;
  }

  int32_t colActual = blockDataGetNumOfCols(pBlock);

  STSRowIter iter = {0};
  tdSTSRowIterInit(&iter, pTschema);
  STSRow* row;
  int32_t curRow = 0;

  tInitSubmitBlkIter(&pReader->msgIter, pReader->pBlock, &pReader->blkIter);

  pBlock->info.id.uid = pReader->msgIter.uid;
  pBlock->info.rows = pReader->msgIter.numOfRows;
  pBlock->info.version = pReader->pMsg->version;
  pBlock->info.dataLoad = 1;

  while ((row = tGetSubmitBlkNext(&pReader->blkIter)) != NULL) {
    tdSTSRowIterReset(&iter, row);
    // get all wanted col of that block
    for (int32_t i = 0; i < colActual; i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      SCellVal         sVal = {0};
      if (!tdSTSRowIterFetch(&iter, pColData->info.colId, pColData->info.type, &sVal)) {
        break;
      }
      if (colDataAppend(pColData, curRow, sVal.val, sVal.valType != TD_VTYPE_NORM) < 0) {
        goto FAIL;
      }
    }
    curRow++;
  }
  return 0;

FAIL:
  blockDataFreeRes(pBlock);
  return -1;
}

int32_t tqRetrieveTaosxBlock(STqReader* pReader, SArray* blocks, SArray* schemas) {
  int32_t sversion = htonl(pReader->pBlock->sversion);

  if (pReader->cachedSchemaSuid == 0 || pReader->cachedSchemaVer != sversion ||
      pReader->cachedSchemaSuid != pReader->msgIter.suid) {
    if (pReader->pSchema) taosMemoryFree(pReader->pSchema);
    pReader->pSchema = metaGetTbTSchema(pReader->pVnodeMeta, pReader->msgIter.uid, sversion, 1);
    if (pReader->pSchema == NULL) {
      tqWarn("cannot found tsschema for table: uid:%" PRId64 " (suid:%" PRId64 "), version %d, possibly dropped table",
             pReader->msgIter.uid, pReader->msgIter.suid, pReader->cachedSchemaVer);
      /*ASSERT(0);*/
      pReader->cachedSchemaSuid = 0;
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }

    if (pReader->pSchemaWrapper) tDeleteSSchemaWrapper(pReader->pSchemaWrapper);
    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, pReader->msgIter.uid, sversion, 1);
    if (pReader->pSchemaWrapper == NULL) {
      tqWarn("cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
             pReader->msgIter.uid, pReader->cachedSchemaVer);
      /*ASSERT(0);*/
      pReader->cachedSchemaSuid = 0;
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }
    pReader->cachedSchemaVer = sversion;
    pReader->cachedSchemaSuid = pReader->msgIter.suid;
  }

  STSchema*       pTschema = pReader->pSchema;
  SSchemaWrapper* pSchemaWrapper = pReader->pSchemaWrapper;

  int32_t colAtMost = pSchemaWrapper->nCols;

  int32_t curRow = 0;
  int32_t lastRow = 0;

  char* assigned = taosMemoryCalloc(1, pSchemaWrapper->nCols);
  if (assigned == NULL) return -1;

  tInitSubmitBlkIter(&pReader->msgIter, pReader->pBlock, &pReader->blkIter);
  STSRowIter iter = {0};
  tdSTSRowIterInit(&iter, pTschema);
  STSRow* row;

  while ((row = tGetSubmitBlkNext(&pReader->blkIter)) != NULL) {
    bool buildNew = false;
    tdSTSRowIterReset(&iter, row);

    tqDebug("vgId:%d, row of block %d", pReader->pWalReader->pWal->cfg.vgId, curRow);
    for (int32_t i = 0; i < colAtMost; i++) {
      SCellVal sVal = {0};
      if (!tdSTSRowIterFetch(&iter, pSchemaWrapper->pSchema[i].colId, pSchemaWrapper->pSchema[i].type, &sVal)) {
        break;
      }
      tqDebug("vgId:%d, %d col, type %d", pReader->pWalReader->pWal->cfg.vgId, i, sVal.valType);
      if (curRow == 0) {
        assigned[i] = sVal.valType != TD_VTYPE_NONE;
        buildNew = true;
      } else {
        bool currentRowAssigned = sVal.valType != TD_VTYPE_NONE;
        if (currentRowAssigned != assigned[i]) {
          assigned[i] = currentRowAssigned;
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
      SSDataBlock*    pBlock = createDataBlock();
      SSchemaWrapper* pSW = taosMemoryCalloc(1, sizeof(SSchemaWrapper));
      if (tqMaskBlock(pSW, pBlock, pSchemaWrapper, assigned) < 0) {
        blockDataDestroy(pBlock);
        goto FAIL;
      }
      SSDataBlock block = {0};
      assignOneDataBlock(&block, pBlock);
      blockDataDestroy(pBlock);

      tqDebug("vgId:%d, build new block, col %d", pReader->pWalReader->pWal->cfg.vgId,
              (int32_t)taosArrayGetSize(block.pDataBlock));

      taosArrayPush(blocks, &block);
      taosArrayPush(schemas, &pSW);
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    pBlock->info.id.uid = pReader->msgIter.uid;
    pBlock->info.rows = 0;
    pBlock->info.version = pReader->pMsg->version;

    tqDebug("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    if (blockDataEnsureCapacity(pBlock, pReader->msgIter.numOfRows - curRow) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto FAIL;
    }

    tdSTSRowIterReset(&iter, row);
    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      SCellVal         sVal = {0};

      if (!tdSTSRowIterFetch(&iter, pColData->info.colId, pColData->info.type, &sVal)) {
        break;
      }

      ASSERT(sVal.valType != TD_VTYPE_NONE);

      if (colDataAppend(pColData, curRow, sVal.val, sVal.valType == TD_VTYPE_NULL) < 0) {
        goto FAIL;
      }
      tqDebug("vgId:%d, row %d col %d append %d", pReader->pWalReader->pWal->cfg.vgId, curRow, i,
              sVal.valType == TD_VTYPE_NULL);
    }
    curRow++;
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

int tqReaderAddTbUidList(STqReader* pReader, const SArray* tbUidList) {
  if (pReader->tbIdHash == NULL) {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    taosHashPut(pReader->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

int tqReaderRemoveTbUidList(STqReader* pReader, const SArray* tbUidList) {
  ASSERT(pReader->tbIdHash != NULL);

  for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    taosHashRemove(pReader->tbIdHash, pKey, sizeof(int64_t));
  }

  return 0;
}

int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd) {
  void* pIter = NULL;
  while (1) {
    pIter = taosHashIterate(pTq->pHandle, pIter);
    if (pIter == NULL) break;
    STqHandle* pExec = (STqHandle*)pIter;
    if (pExec->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      int32_t code = qUpdateQualifiedTableId(pExec->execHandle.task, tbUidList, isAdd);
      ASSERT(code == 0);
    } else if (pExec->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      if (!isAdd) {
        int32_t sz = taosArrayGetSize(tbUidList);
        for (int32_t i = 0; i < sz; i++) {
          int64_t tbUid = *(int64_t*)taosArrayGet(tbUidList, i);
          taosHashPut(pExec->execHandle.execDb.pFilterOutTbUid, &tbUid, sizeof(int64_t), NULL, 0);
        }
      }
    } else if (pExec->execHandle.subType == TOPIC_SUB_TYPE__TABLE) {
      if (isAdd) {
        SArray*     qa = taosArrayInit(4, sizeof(tb_uid_t));
        SMetaReader mr = {0};
        metaReaderInit(&mr, pTq->pVnode->pMeta, 0);
        for (int32_t i = 0; i < taosArrayGetSize(tbUidList); ++i) {
          uint64_t* id = (uint64_t*)taosArrayGet(tbUidList, i);

          int32_t code = metaGetTableEntryByUidCache(&mr, *id);
          if (code != TSDB_CODE_SUCCESS) {
            qError("failed to get table meta, uid:%" PRIu64 " code:%s", *id, tstrerror(terrno));
            continue;
          }

          tDecoderClear(&mr.coder);

          if (mr.me.type != TSDB_CHILD_TABLE || mr.me.ctbEntry.suid != pExec->execHandle.execTb.suid) {
            tqDebug("table uid %" PRId64 " does not add to tq handle", *id);
            continue;
          }
          tqDebug("table uid %" PRId64 " add to tq handle", *id);
          taosArrayPush(qa, id);
        }
        metaReaderClear(&mr);
        if (taosArrayGetSize(qa) > 0) {
          tqReaderAddTbUidList(pExec->execHandle.pExecReader, qa);
        }
        taosArrayDestroy(qa);
      } else {
        // TODO handle delete table from stb
      }
    } else {
      ASSERT(0);
    }
  }
  while (1) {
    pIter = taosHashIterate(pTq->pStreamMeta->pTasks, pIter);
    if (pIter == NULL) break;
    SStreamTask* pTask = *(SStreamTask**)pIter;
    if (pTask->taskLevel == TASK_LEVEL__SOURCE) {
      int32_t code = qUpdateQualifiedTableId(pTask->exec.executor, tbUidList, isAdd);
      ASSERT(code == 0);
    }
  }
  return 0;
}
