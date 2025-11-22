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

static int32_t tqCollectPhysicalTables(STqReader* pReader, const char* idstr);

static void processCreateTbMsg(SDecoder* dcoder, SWalCont* pHead, STqReader* pReader, int64_t* realTbSuid, int64_t tbSuid) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t        needRebuild = 0;
  SVCreateTbReq* pCreateReq = NULL;
  SVCreateTbBatchReq reqNew = {0};
  void* buf = NULL;
  SVCreateTbBatchReq req = {0};
  code = tDecodeSVCreateTbBatchReq(dcoder, &req);
  if (code < 0) {
    lino = __LINE__;
    goto end;
  }

  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pCreateReq = req.pReqs + iReq;
    if (pCreateReq->type == TSDB_CHILD_TABLE && pCreateReq->ctb.suid == tbSuid &&
        taosHashGet(pReader->tbIdHash, &pCreateReq->uid, sizeof(int64_t)) != NULL) {  
      needRebuild++;
    }
  }
  if (needRebuild == 0) {
    // do nothing
  } else if (needRebuild == req.nReqs) {
    *realTbSuid = tbSuid;
  } else {
    *realTbSuid = tbSuid;
    reqNew.pArray = taosArrayInit(req.nReqs, sizeof(struct SVCreateTbReq));
    if (reqNew.pArray == NULL) {
      code = terrno;
      lino = __LINE__;
      goto end;
    }
    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      pCreateReq = req.pReqs + iReq;
      if (pCreateReq->type == TSDB_CHILD_TABLE && pCreateReq->ctb.suid == tbSuid &&
          taosHashGet(pReader->tbIdHash, &pCreateReq->uid, sizeof(int64_t)) != NULL) {
        reqNew.nReqs++;
        if (taosArrayPush(reqNew.pArray, pCreateReq) == NULL) {
          code = terrno;
          lino = __LINE__;
          goto end;
        }
      }
    }

    int     tlen = 0;
    tEncodeSize(tEncodeSVCreateTbBatchReq, &reqNew, tlen, code);
    buf = taosMemoryMalloc(tlen);
    if (NULL == buf || code < 0) {
      lino = __LINE__;
      goto end;
    }
    SEncoder coderNew = {0};
    tEncoderInit(&coderNew, buf, tlen);
    code = tEncodeSVCreateTbBatchReq(&coderNew, &reqNew);
    tEncoderClear(&coderNew);
    if (code < 0) {
      lino = __LINE__;
      goto end;
    }
    (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
    pHead->bodyLen = tlen + sizeof(SMsgHead);
  }

end:
  taosMemoryFree(buf);
  taosArrayDestroy(reqNew.pArray);
  tDeleteSVCreateTbBatchReq(&req);
  if (code < 0) {
    tqError("processCreateTbMsg failed, code:%d, line:%d", code, lino);
  }
}

static void processAlterTbMsg(SDecoder* dcoder, STqReader* pReader, int64_t* realTbSuid) {
  SVAlterTbReq req = {0};
  SMetaReader mr = {0};
  int32_t lino = 0;
  int32_t code = tDecodeSVAlterTbReq(dcoder, &req);
  if (code < 0) {
    lino = __LINE__;
    goto end;
  }

  metaReaderDoInit(&mr, pReader->pVnodeMeta, META_READER_LOCK);

  code = metaGetTableEntryByName(&mr, req.tbName);
  if (code < 0) {
    lino = __LINE__;
    goto end;
  }
  if (taosHashGet(pReader->tbIdHash, &mr.me.uid, sizeof(int64_t)) != NULL) {
    *realTbSuid = mr.me.ctbEntry.suid;
  }

end:
  taosArrayDestroy(req.pMultiTag);
  metaReaderClear(&mr);  
  if (code < 0) {
    tqError("processAlterTbMsg failed, code:%d, line:%d", code, lino);
  }
} 

static void processDropTbMsg(SDecoder* dcoder, SWalCont* pHead, STqReader* pReader, int64_t* realTbSuid, int64_t tbSuid) {
  SVDropTbBatchReq req = {0};
  SVDropTbBatchReq reqNew = {0};
  void* buf = NULL;
  int32_t lino = 0;
  int32_t code = tDecodeSVDropTbBatchReq(dcoder, &req);
  if (code < 0) {
    lino = __LINE__;
    goto end;
  }

  int32_t      needRebuild = 0;
  SVDropTbReq* pDropReq = NULL;
  for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
    pDropReq = req.pReqs + iReq;

    if (pDropReq->suid == tbSuid &&
        taosHashGet(pReader->tbIdHash, &pDropReq->uid, sizeof(int64_t)) != NULL) {
      needRebuild++;
    }
  }
  if (needRebuild == 0) {
    // do nothing
  } else if (needRebuild == req.nReqs) {
    *realTbSuid = tbSuid;
  } else {
    *realTbSuid = tbSuid;
    reqNew.pArray = taosArrayInit(req.nReqs, sizeof(SVDropTbReq));
    if (reqNew.pArray == NULL) {
      code = terrno;
      lino = __LINE__;
      goto end;
    }
    for (int32_t iReq = 0; iReq < req.nReqs; iReq++) {
      pDropReq = req.pReqs + iReq;
      if (pDropReq->suid == tbSuid &&
          taosHashGet(pReader->tbIdHash, &pDropReq->uid, sizeof(int64_t)) != NULL) {
        reqNew.nReqs++;
        if (taosArrayPush(reqNew.pArray, pDropReq) == NULL) {
          code = terrno;
          lino = __LINE__;
          goto end;
        }
      }
    }

    int     tlen = 0;
    tEncodeSize(tEncodeSVDropTbBatchReq, &reqNew, tlen, code);
    buf = taosMemoryMalloc(tlen);
    if (NULL == buf || code < 0) {
      lino = __LINE__;
      goto end;
    }
    SEncoder coderNew = {0};
    tEncoderInit(&coderNew, buf, tlen);
    code = tEncodeSVDropTbBatchReq(&coderNew, &reqNew);
    tEncoderClear(&coderNew);
    if (code != 0) {
      lino = __LINE__;
      goto end;
    }
    (void)memcpy(pHead->body + sizeof(SMsgHead), buf, tlen);
    pHead->bodyLen = tlen + sizeof(SMsgHead);
  }

end:
  taosMemoryFree(buf);
  taosArrayDestroy(reqNew.pArray);
  if (code < 0) {
    tqError("processDropTbMsg failed, code:%d, line:%d", code, lino);
  }
}

bool isValValidForTable(STqHandle* pHandle, SWalCont* pHead) {
  int32_t code = 0;
  int32_t lino = 0;
  if (pHandle == NULL || pHead == NULL) {
    return false;
  }
  if (pHandle->execHandle.subType != TOPIC_SUB_TYPE__TABLE) {
    return true;
  }

  STqExecHandle* pExec = &pHandle->execHandle;
  STqReader* pReader = pExec->pTqReader;

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
    processCreateTbMsg(&dcoder, pHead, pReader, &realTbSuid, tbSuid);
  } else if (msgType == TDMT_VND_ALTER_TABLE) {
    processAlterTbMsg(&dcoder, pReader, &realTbSuid);
  } else if (msgType == TDMT_VND_DROP_TABLE) {
    processDropTbMsg(&dcoder, pHead, pReader, &realTbSuid, tbSuid);
  } else if (msgType == TDMT_VND_DELETE) {
    SDeleteRes req = {0};
    if (tDecodeDeleteRes(&dcoder, &req) < 0) {
      goto end;
    }
    realTbSuid = req.suid;
  }

end:
  tDecoderClear(&dcoder);
  bool tmp = tbSuid == realTbSuid;
  tqDebug("%s suid:%" PRId64 " realSuid:%" PRId64 " return:%d", __FUNCTION__, tbSuid, realTbSuid, tmp);
  return tmp;
}

int32_t tqFetchLog(STQ* pTq, STqHandle* pHandle, int64_t* fetchOffset, uint64_t reqId) {
  if (pTq == NULL || pHandle == NULL || fetchOffset == NULL) {
    return -1;
  }
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
              ", no more log to return, QID:0x%" PRIx64 " 0x%" PRIx64,
              pHandle->consumerId, pHandle->epoch, vgId, offset, reqId, id);
      goto END;
    }

    tqDebug("vgId:%d, consumer:0x%" PRIx64 " taosx get msg ver %" PRId64 ", type:%s, QID:0x%" PRIx64 " 0x%" PRIx64,
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
  tqDebug("vgId:%d, end to fetch wal, code:%d , index:%" PRId64 ", last:%" PRId64 " commit:%" PRId64
          ", applied:%" PRId64 ", 0x%" PRIx64,
          vgId, code, offset, lastVer, committedVer, appliedVer, id);
  return code;
}

bool tqGetTablePrimaryKey(STqReader* pReader) {
  if (pReader == NULL) {
    return false;
  }
  return pReader->hasPrimaryKey;
}

void tqSetTablePrimaryKey(STqReader* pReader, int64_t uid) {
  tqDebug("%s:%p uid:%" PRId64, __FUNCTION__, pReader, uid);

  if (pReader == NULL) {
    return;
  }
  bool            ret = false;
  SSchemaWrapper* schema = metaGetTableSchema(pReader->pVnodeMeta, uid, -1, 1, NULL, 0);
  if (schema && schema->nCols >= 2 && schema->pSchema[1].flags & COL_IS_KEY) {
    ret = true;
  }
  tDeleteSchemaWrapper(schema);
  pReader->hasPrimaryKey = ret;
}

STqReader* tqReaderOpen(SVnode* pVnode) {
  tqDebug("%s:%p", __FUNCTION__, pVnode);
  if (pVnode == NULL) {
    return NULL;
  }
  STqReader* pReader = taosMemoryCalloc(1, sizeof(STqReader));
  if (pReader == NULL) {
    return NULL;
  }

  pReader->pWalReader = walOpenReader(pVnode->pWal, 0);
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
  tqDebug("%s:%p", __FUNCTION__, pReader);
  if (pReader == NULL) return;

  // close wal reader
  if (pReader->pWalReader) {
    walCloseReader(pReader->pWalReader);
  }

  if (pReader->pSchemaWrapper) {
    tDeleteSchemaWrapper(pReader->pSchemaWrapper);
  }

  taosMemoryFree(pReader->extSchema);
  if (pReader->pColIdList) {
    taosArrayDestroy(pReader->pColIdList);
  }

  // free hash
  blockDataDestroy(pReader->pResBlock);
  taosHashCleanup(pReader->tbIdHash);
  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);

  taosHashCleanup(pReader->vtSourceScanInfo.pVirtualTables);
  taosHashCleanup(pReader->vtSourceScanInfo.pPhysicalTables);
  taosLRUCacheCleanup(pReader->vtSourceScanInfo.pPhyTblSchemaCache);
  taosMemoryFree(pReader);
}

int32_t tqReaderSeek(STqReader* pReader, int64_t ver, const char* id) {
  if (pReader == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (walReaderSeekVer(pReader->pWalReader, ver) < 0) {
    return terrno;
  }
  tqDebug("wal reader seek to ver:%" PRId64 " %s", ver, id);
  return 0;
}

bool tqNextBlockInWal(STqReader* pReader, const char* id, int sourceExcluded) {
  if (pReader == NULL) {
    return false;
  }
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
    if (walNextValidMsg(pWalReader, false) < 0) {
      return false;
    }

    void*   pBody = POINTER_SHIFT(pWalReader->pHead->head.body, sizeof(SSubmitReq2Msg));
    int32_t bodyLen = pWalReader->pHead->head.bodyLen - sizeof(SSubmitReq2Msg);
    int64_t ver = pWalReader->pHead->head.version;
    SDecoder decoder = {0};
    if (tqReaderSetSubmitMsg(pReader, pBody, bodyLen, ver, NULL, &decoder) != 0) {
      tDecoderClear(&decoder);
      return false;
    }
    tDecoderClear(&decoder);
    pReader->nextBlk = 0;
  }
}

int32_t tqReaderSetSubmitMsg(STqReader* pReader, void* msgStr, int32_t msgLen, int64_t ver, SArray* rawList, SDecoder* decoder) {
  if (pReader == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  pReader->msg.msgStr = msgStr;
  pReader->msg.msgLen = msgLen;
  pReader->msg.ver = ver;

  tqTrace("tq reader set msg pointer:%p, msg len:%d", msgStr, msgLen);

  tDecoderInit(decoder, pReader->msg.msgStr, pReader->msg.msgLen);
  int32_t code = tDecodeSubmitReq(decoder, &pReader->submit, rawList);

  if (code != 0) {
    tqError("DecodeSSubmitReq2 error, msgLen:%d, ver:%" PRId64, msgLen, ver);
  }

  return code;
}

void tqReaderClearSubmitMsg(STqReader* pReader) {
  tDestroySubmitReq(&pReader->submit, TSDB_MSG_FLG_DECODE);
  pReader->nextBlk = 0;
  pReader->msg.msgStr = NULL;
}

SWalReader* tqGetWalReader(STqReader* pReader) {
  if (pReader == NULL) {
    return NULL;
  }
  return pReader->pWalReader;
}

SSDataBlock* tqGetResultBlock(STqReader* pReader) {
  if (pReader == NULL) {
    return NULL;
  }
  return pReader->pResBlock;
}

int64_t tqGetResultBlockTime(STqReader* pReader) {
  if (pReader == NULL) {
    return 0;
  }
  return pReader->lastTs;
}

bool tqNextBlockImpl(STqReader* pReader, const char* idstr) {
  int32_t code = false;
  int32_t lino = 0;
  int64_t uid = 0;
  TSDB_CHECK_NULL(pReader, code, lino, END, false);
  TSDB_CHECK_NULL(pReader->msg.msgStr, code, lino, END, false);
  TSDB_CHECK_NULL(pReader->tbIdHash, code, lino, END, true);

  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < blockSz) {
    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
    TSDB_CHECK_NULL(pSubmitTbData, code, lino, END, false);
    uid = pSubmitTbData->uid;
    void* ret = taosHashGet(pReader->tbIdHash, &pSubmitTbData->uid, sizeof(int64_t));
    TSDB_CHECK_CONDITION(ret == NULL, code, lino, END, true);

    tqTrace("iterator data block in hash continue, progress:%d/%d, total queried tables:%d, uid:%" PRId64,
            pReader->nextBlk, blockSz, taosHashGetSize(pReader->tbIdHash), uid);
    pReader->nextBlk++;
  }

  tqReaderClearSubmitMsg(pReader);
  tqTrace("iterator data block end, total block num:%d, uid:%" PRId64, blockSz, uid);

END:
  tqTrace("%s:%d return:%s, uid:%" PRId64, __FUNCTION__, lino, code ? "true" : "false", uid);
  return code;
}

bool tqNextDataBlockFilterOut(STqReader* pReader, SHashObj* filterOutUids) {
  int32_t code = false;
  int32_t lino = 0;
  int64_t uid = 0;

  TSDB_CHECK_NULL(pReader, code, lino, END, false);
  TSDB_CHECK_NULL(pReader->msg.msgStr, code, lino, END, false);
  TSDB_CHECK_NULL(filterOutUids, code, lino, END, true);

  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < blockSz) {
    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
    TSDB_CHECK_NULL(pSubmitTbData, code, lino, END, false);
    uid = pSubmitTbData->uid;
    void* ret = taosHashGet(filterOutUids, &pSubmitTbData->uid, sizeof(int64_t));
    TSDB_CHECK_NULL(ret, code, lino, END, true);
    tqTrace("iterator data block in hash jump block, progress:%d/%d, uid:%" PRId64, pReader->nextBlk, blockSz, uid);
    pReader->nextBlk++;
  }
  tqReaderClearSubmitMsg(pReader);
  tqTrace("iterator data block end, total block num:%d, uid:%" PRId64, blockSz, uid);

END:
  tqTrace("%s:%d get data:%s, uid:%" PRId64, __FUNCTION__, lino, code ? "true" : "false", uid);
  return code;
}

int32_t tqMaskBlock(SSchemaWrapper* pDst, SSDataBlock* pBlock, const SSchemaWrapper* pSrc, char* mask,
                    SExtSchema* extSrc) {
  if (pDst == NULL || pBlock == NULL || pSrc == NULL || mask == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
      if (extSrc != NULL) {
        decimalFromTypeMod(extSrc[i].typeMod, &colInfo.info.precision, &colInfo.info.scale);
      }
      code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != 0) {
        return code;
      }
    }
  }
  return 0;
}

static int32_t buildResSDataBlock(STqReader* pReader, SSchemaWrapper* pSchema, const SArray* pColIdList) {
  if (pReader == NULL || pSchema == NULL || pColIdList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  SSDataBlock* pBlock = pReader->pResBlock;
  if (blockDataGetNumOfCols(pBlock) > 0) {
    blockDataDestroy(pBlock);
    int32_t code = createDataBlock(&pReader->pResBlock);
    if (code) {
      return code;
    }
    pBlock = pReader->pResBlock;

    pBlock->info.id.uid = pReader->cachedSchemaUid;
    pBlock->info.version = pReader->msg.ver;
  }

  int32_t numOfCols = taosArrayGetSize(pColIdList);

  if (numOfCols == 0) {  // all columns are required
    for (int32_t i = 0; i < pSchema->nCols; ++i) {
      SSchema*        pColSchema = &pSchema->pSchema[i];
      SColumnInfoData colInfo = createColumnInfoData(pColSchema->type, pColSchema->bytes, pColSchema->colId);

      if (IS_DECIMAL_TYPE(pColSchema->type) && pReader->extSchema != NULL) {
        decimalFromTypeMod(pReader->extSchema[i].typeMod, &colInfo.info.precision, &colInfo.info.scale);
      }
      int32_t code = blockDataAppendColInfo(pBlock, &colInfo);
      if (code != TSDB_CODE_SUCCESS) {
        blockDataFreeRes(pBlock);
        return terrno;
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
        if (IS_DECIMAL_TYPE(pColSchema->type) && pReader->extSchema != NULL) {
          decimalFromTypeMod(pReader->extSchema[i].typeMod, &colInfo.info.precision, &colInfo.info.scale);
        }
        int32_t code = blockDataAppendColInfo(pBlock, &colInfo);
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

static int32_t doSetBlobVal(SColumnInfoData* pColumnInfoData, int32_t idx, SColVal* pColVal, SBlobSet* pBlobRow2) {
  int32_t code = 0;
  if (pColumnInfoData == NULL || pColVal == NULL || pBlobRow2 == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  // TODO(yhDeng)
  if (COL_VAL_IS_VALUE(pColVal)) {
    char* val = taosMemCalloc(1, pColVal->value.nData + sizeof(BlobDataLenT));
    if (val == NULL) {
      return terrno;
    }

    uint64_t seq = 0;
    int32_t  len = 0;
    if (pColVal->value.pData != NULL) {
      if (tGetU64(pColVal->value.pData, &seq) < 0){
        TAOS_CHECK_RETURN(TSDB_CODE_INVALID_PARA);
      }
      SBlobItem item = {0};
      code = tBlobSetGet(pBlobRow2, seq, &item);
      if (code != 0) {
        taosMemoryFree(val);
        terrno = code;
        uError("tq set blob val, idx:%d, get blob item failed, seq:%" PRIu64 ", code:%d", idx, seq, code);
        return code;
      }

      val = taosMemRealloc(val, item.len + sizeof(BlobDataLenT));
      (void)memcpy(blobDataVal(val), item.data, item.len);
      len = item.len;
    }

    blobDataSetLen(val, len);
    code = colDataSetVal(pColumnInfoData, idx, val, false);

    taosMemoryFree(val);
  } else {
    colDataSetNULL(pColumnInfoData, idx);
  }
  return code;
}
static int32_t doSetVal(SColumnInfoData* pColumnInfoData, int32_t rowIndex, SColVal* pColVal) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (IS_VAR_DATA_TYPE(pColVal->value.type)) {
    if (COL_VAL_IS_VALUE(pColVal)) {
      char val[65535 + 2] = {0};
      if (pColVal->value.pData != NULL) {
        (void)memcpy(varDataVal(val), pColVal->value.pData, pColVal->value.nData);
      }
      varDataSetLen(val, pColVal->value.nData);
      code = colDataSetVal(pColumnInfoData, rowIndex, val, false);
    } else {
      colDataSetNULL(pColumnInfoData, rowIndex);
    }
  } else {
    code = colDataSetVal(pColumnInfoData, rowIndex, VALUE_GET_DATUM(&pColVal->value, pColVal->value.type),
                         !COL_VAL_IS_VALUE(pColVal));
  }

  return code;
}

int32_t tqRetrieveDataBlock(STqReader* pReader, SSDataBlock** pRes, const char* id) {
  if (pReader == NULL || pRes == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  tqDebug("tq reader retrieve data block %p, index:%d", pReader->msg.msgStr, pReader->nextBlk);
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
    taosMemoryFree(pReader->extSchema);
    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, uid, sversion, 1, &pReader->extSchema, 0);
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
    code = buildResSDataBlock(pReader, pReader->pSchemaWrapper, pReader->pColIdList);
    TSDB_CHECK_CODE(code, line, END);
    pBlock = pReader->pResBlock;
    *pRes = pBlock;
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

      uint8_t isBlob = IS_STR_DATA_BLOB(pColData->info.type) ? 1 : 0;

      SColData* pCol = taosArrayGet(pCols, sourceIdx);
      TSDB_CHECK_NULL(pCol, code, line, END, terrno);
      SColVal colVal = {0};
      tqTrace("lostdata colActual:%d, sourceIdx:%d, targetIdx:%d, numOfCols:%d, source cid:%d, dst cid:%d", colActual,
              sourceIdx, targetIdx, numOfCols, pCol->cid, pColData->info.colId);
      if (pCol->cid < pColData->info.colId) {
        sourceIdx++;
      } else if (pCol->cid == pColData->info.colId) {
        for (int32_t i = 0; i < pCol->nVal; i++) {
          code = tColDataGetValue(pCol, i, &colVal);
          TSDB_CHECK_CODE(code, line, END);

          if (isBlob == 0) {
            code = doSetVal(pColData, i, &colVal);
          } else {
            code = doSetBlobVal(pColData, i, &colVal, pSubmitTbData->pBlobSet);
          }
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

        uint8_t isBlob = IS_STR_DATA_BLOB(pColData->info.type) ? 1 : 0;
        while (1) {
          SColVal colVal = {0};
          code = tRowGet(pRow, pTSchema, sourceIdx, &colVal);
          TSDB_CHECK_CODE(code, line, END);

          if (colVal.cid < pColData->info.colId) {
            sourceIdx++;
            continue;
          } else if (colVal.cid == pColData->info.colId) {
            if (isBlob == 0) {
              code = doSetVal(pColData, i, &colVal);
            } else {
              code = doSetBlobVal(pColData, i, &colVal, pSubmitTbData->pBlobSet);
            }

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
    tqError("tqRetrieveDataBlock failed, line:%d, msg:%s", line, tstrerror(code));
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

#define SET_DATA                                                                                    \
  if (colVal.cid < pColData->info.colId) {                                                          \
    sourceIdx++;                                                                                    \
  } else if (colVal.cid == pColData->info.colId) {                                                  \
    if (IS_STR_DATA_BLOB(pColData->info.type)) {                                                    \
      TQ_ERR_GO_TO_END(doSetBlobVal(pColData, curRow - lastRow, &colVal, pSubmitTbData->pBlobSet)); \
    } else {                                                                                        \
      TQ_ERR_GO_TO_END(doSetVal(pColData, curRow - lastRow, &colVal));                              \
    }                                                                                               \
    sourceIdx++;                                                                                    \
    targetIdx++;                                                                                    \
  } else {                                                                                          \
    colDataSetNULL(pColData, curRow - lastRow);                                                     \
    targetIdx++;                                                                                    \
  }

static int32_t processBuildNew(STqReader* pReader, SSubmitTbData* pSubmitTbData, SArray* blocks, SArray* schemas,
                               char* assigned, int32_t numOfRows, int32_t curRow, int32_t* lastRow) {
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

  TQ_ERR_GO_TO_END(tqMaskBlock(pSW, block, pReader->pSchemaWrapper, assigned, pReader->extSchema));
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
  if (code != 0) {
    tqError("processBuildNew failed, code:%d", code);
  }
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
  tqTrace("vgId:%d, tqProcessColData start, col num: %d, rows:%d", pReader->pWalReader->pWal->cfg.vgId, numOfCols,
          numOfRows);
  for (int32_t i = 0; i < numOfRows; i++) {
    bool buildNew = false;

    for (int32_t j = 0; j < pSchemaWrapper->nCols; j++) {
      int32_t k = 0;
      for (; k < numOfCols; k++) {
        pCol = taosArrayGet(pCols, k);
        TQ_NULL_GO_TO_END(pCol);
        if (pSchemaWrapper->pSchema[j].colId == pCol->cid) {
          SColVal colVal = {0};
          TQ_ERR_GO_TO_END(tColDataGetValue(pCol, i, &colVal));
          PROCESS_VAL
          tqTrace("assign[%d] = %d, nCols:%d", j, assigned[j], numOfCols);
          break;
        }
      }
      if (k >= numOfCols) {
        // this column is not in the current row, so we set it to NULL
        assigned[j] = 0;
        buildNew = true;
      }
    }

    if (buildNew) {
      TQ_ERR_GO_TO_END(processBuildNew(pReader, pSubmitTbData, blocks, schemas, assigned, numOfRows, curRow, &lastRow));
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pBlock);

    tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    int32_t colActual = blockDataGetNumOfCols(pBlock);
    while (targetIdx < colActual && sourceIdx < numOfCols) {
      pCol = taosArrayGet(pCols, sourceIdx);
      TQ_NULL_GO_TO_END(pCol);
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      TQ_NULL_GO_TO_END(pColData);
      SColVal colVal = {0};
      TQ_ERR_GO_TO_END(tColDataGetValue(pCol, i, &colVal));
      SET_DATA
      tqTrace("targetIdx:%d sourceIdx:%d colActual:%d", targetIdx, sourceIdx, colActual);
    }

    curRow++;
  }
  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  pLastBlock->info.rows = curRow - lastRow;
  tqTrace("vgId:%d, tqProcessColData end, col num: %d, rows:%d, block num:%d", pReader->pWalReader->pWal->cfg.vgId,
          numOfCols, numOfRows, (int)taosArrayGetSize(blocks));
END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("vgId:%d, process col data failed, code:%d", pReader->pWalReader->pWal->cfg.vgId, code);
  }
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
  TQ_NULL_GO_TO_END(pTSchema);
  tqTrace("vgId:%d, tqProcessRowData start, rows:%d", pReader->pWalReader->pWal->cfg.vgId, numOfRows);

  for (int32_t i = 0; i < numOfRows; i++) {
    bool  buildNew = false;
    SRow* pRow = taosArrayGetP(pRows, i);
    TQ_NULL_GO_TO_END(pRow);

    for (int32_t j = 0; j < pTSchema->numOfCols; j++) {
      SColVal colVal = {0};
      TQ_ERR_GO_TO_END(tRowGet(pRow, pTSchema, j, &colVal));
      PROCESS_VAL
      tqTrace("assign[%d] = %d, nCols:%d", j, assigned[j], pTSchema->numOfCols);
    }

    if (buildNew) {
      TQ_ERR_GO_TO_END(processBuildNew(pReader, pSubmitTbData, blocks, schemas, assigned, numOfRows, curRow, &lastRow));
    }

    SSDataBlock* pBlock = taosArrayGetLast(blocks);
    TQ_NULL_GO_TO_END(pBlock);

    tqTrace("vgId:%d, taosx scan, block num: %d", pReader->pWalReader->pWal->cfg.vgId,
            (int32_t)taosArrayGetSize(blocks));

    int32_t targetIdx = 0;
    int32_t sourceIdx = 0;
    int32_t colActual = blockDataGetNumOfCols(pBlock);
    while (targetIdx < colActual && sourceIdx < pTSchema->numOfCols) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, targetIdx);
      TQ_NULL_GO_TO_END(pColData);
      SColVal          colVal = {0};
      TQ_ERR_GO_TO_END(tRowGet(pRow, pTSchema, sourceIdx, &colVal));
      SET_DATA
      tqTrace("targetIdx:%d sourceIdx:%d colActual:%d", targetIdx, sourceIdx, colActual);
    }

    curRow++;
  }
  SSDataBlock* pLastBlock = taosArrayGetLast(blocks);
  if (pLastBlock != NULL) {
    pLastBlock->info.rows = curRow - lastRow;
  }

  tqTrace("vgId:%d, tqProcessRowData end, rows:%d, block num:%d", pReader->pWalReader->pWal->cfg.vgId, numOfRows,
          (int)taosArrayGetSize(blocks));
END:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("vgId:%d, process row data failed, code:%d", pReader->pWalReader->pWal->cfg.vgId, code);
  }
  taosMemoryFreeClear(pTSchema);
  taosMemoryFree(assigned);
  return code;
}

static int32_t buildCreateTbInfo(SMqDataRsp* pRsp, SVCreateTbReq* pCreateTbReq) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   createReq = NULL;
  TSDB_CHECK_NULL(pRsp, code, lino, END, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pCreateTbReq, code, lino, END, TSDB_CODE_INVALID_PARA);

  if (pRsp->createTableNum == 0) {
    pRsp->createTableLen = taosArrayInit(0, sizeof(int32_t));
    TSDB_CHECK_NULL(pRsp->createTableLen, code, lino, END, terrno);
    pRsp->createTableReq = taosArrayInit(0, sizeof(void*));
    TSDB_CHECK_NULL(pRsp->createTableReq, code, lino, END, terrno);
  }

  uint32_t len = 0;
  tEncodeSize(tEncodeSVCreateTbReq, pCreateTbReq, len, code);
  TSDB_CHECK_CODE(code, lino, END);
  createReq = taosMemoryCalloc(1, len);
  TSDB_CHECK_NULL(createReq, code, lino, END, terrno);

  SEncoder encoder = {0};
  tEncoderInit(&encoder, createReq, len);
  code = tEncodeSVCreateTbReq(&encoder, pCreateTbReq);
  tEncoderClear(&encoder);
  TSDB_CHECK_CODE(code, lino, END);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->createTableLen, &len), code, lino, END, terrno);
  TSDB_CHECK_NULL(taosArrayPush(pRsp->createTableReq, &createReq), code, lino, END, terrno);
  pRsp->createTableNum++;
  tqTrace("build create table info msg success");

END:
  if (code != 0) {
    tqError("%s failed at %d, failed to build create table info msg:%s", __FUNCTION__, lino, tstrerror(code));
    taosMemoryFree(createReq);
  }
  return code;
}

int32_t tqRetrieveTaosxBlock(STqReader* pReader, SMqDataRsp* pRsp, SArray* blocks, SArray* schemas,
                             SSubmitTbData** pSubmitTbDataRet, SArray* rawList, int8_t fetchMeta) {
  tqTrace("tq reader retrieve data block msg pointer:%p, index:%d", pReader->msg.msgStr, pReader->nextBlk);
  SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
  if (pSubmitTbData == NULL) {
    return terrno;
  }
  pReader->nextBlk++;

  if (pSubmitTbDataRet) {
    *pSubmitTbDataRet = pSubmitTbData;
  }

  if (fetchMeta == ONLY_META) {
    if (pSubmitTbData->pCreateTbReq != NULL) {
      if (pRsp->createTableReq == NULL) {
        pRsp->createTableReq = taosArrayInit(0, POINTER_BYTES);
        if (pRsp->createTableReq == NULL) {
          return terrno;
        }
      }
      if (taosArrayPush(pRsp->createTableReq, &pSubmitTbData->pCreateTbReq) == NULL) {
        return terrno;
      }
      pSubmitTbData->pCreateTbReq = NULL;
    }
    return 0;
  }

  int32_t sversion = pSubmitTbData->sver;
  int64_t uid = pSubmitTbData->uid;
  pReader->lastBlkUid = uid;

  tDeleteSchemaWrapper(pReader->pSchemaWrapper);
  taosMemoryFreeClear(pReader->extSchema);
  pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, uid, sversion, 1, &pReader->extSchema, 0);
  if (pReader->pSchemaWrapper == NULL) {
    tqWarn("vgId:%d, cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
           pReader->pWalReader->pWal->cfg.vgId, uid, pReader->cachedSchemaVer);
    pReader->cachedSchemaSuid = 0;
    return TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
  }

  if (pSubmitTbData->pCreateTbReq != NULL) {
    int32_t code = buildCreateTbInfo(pRsp, pSubmitTbData->pCreateTbReq);
    if (code != 0) {
      return code;
    }
  } else if (rawList != NULL) {
    if (taosArrayPush(schemas, &pReader->pSchemaWrapper) == NULL) {
      return terrno;
    }
    pReader->pSchemaWrapper = NULL;
    return 0;
  }

  if (pSubmitTbData->flags & SUBMIT_REQ_COLUMN_DATA_FORMAT) {
    return tqProcessColData(pReader, pSubmitTbData, blocks, schemas);
  } else {
    return tqProcessRowData(pReader, pSubmitTbData, blocks, schemas);
  }
}

int32_t tqReaderSetColIdList(STqReader* pReader, SArray* pColIdList, const char* id) {
  if (pReader == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  pReader->pColIdList = pColIdList;
  return tqCollectPhysicalTables(pReader, id);
}

int32_t tqReaderSetTbUidList(STqReader* pReader, const SArray* tbUidList, const char* id) {
  if (pReader == NULL || tbUidList == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  if (pReader->tbIdHash) {
    taosHashClear(pReader->tbIdHash);
  } else {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
    if (pReader->tbIdHash == NULL) {
      tqError("s-task:%s failed to init hash table", id);
      return terrno;
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
  return TSDB_CODE_SUCCESS;
}

void tqReaderAddTbUidList(STqReader* pReader, const SArray* pTableUidList) {
  if (pReader == NULL || pTableUidList == NULL) {
    return;
  }
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
  if (pReader == NULL) {
    return false;
  }
  return taosHashGet(pReader->tbIdHash, &uid, sizeof(uint64_t)) != NULL;
}

bool tqCurrentBlockConsumed(const STqReader* pReader) {
  if (pReader == NULL) {
    return false;
  }
  return pReader->msg.msgStr == NULL;
}

void tqReaderRemoveTbUidList(STqReader* pReader, const SArray* tbUidList) {
  if (pReader == NULL || tbUidList == NULL) {
    return;
  }
  for (int32_t i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t* pKey = (int64_t*)taosArrayGet(tbUidList, i);
    if (pKey && taosHashRemove(pReader->tbIdHash, pKey, sizeof(int64_t)) != 0) {
      tqError("failed to remove table uid:%" PRId64 " from hash", *pKey);
    }
  }
}

int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd) {
  if (pTq == NULL) {
    return 0;  // mounted vnode may have no tq
  }
  if (tbUidList == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
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
        if (ret == 0) {
          ret = tqReaderSetTbUidList(pTqHandle->execHandle.pTqReader, list, NULL);
        }                            
        if (ret != TDB_CODE_SUCCESS) {
          tqError("qGetTableList in tqUpdateTbUidList error:%d handle %s consumer:0x%" PRIx64, ret, pTqHandle->subKey,
                  pTqHandle->consumerId);
          taosArrayDestroy(list);
          taosHashCancelIterate(pTq->pHandle, pIter);
          taosWUnLockLatch(&pTq->lock);

          return ret;
        }
        taosArrayDestroy(list);
      } else {
        tqReaderRemoveTbUidList(pTqHandle->execHandle.pTqReader, tbUidList);
      }
    }
  }
  taosWUnLockLatch(&pTq->lock);
  return 0;
}

static void destroySourceScanTables(void* ptr) {
  SArray** pTables = ptr;
  if (pTables && *pTables) {
    taosArrayDestroy(*pTables);
    *pTables = NULL;
  }
}

static int32_t compareSVTColInfo(const void* p1, const void* p2) {
  SVTColInfo* pCol1 = (SVTColInfo*)p1;
  SVTColInfo* pCol2 = (SVTColInfo*)p2;
  if (pCol1->vColId == pCol2->vColId) {
    return 0;
  } else if (pCol1->vColId < pCol2->vColId) {
    return -1;
  } else {
    return 1;
  }
}

int32_t tqReaderSetVtableInfo(STqReader* pReader, void* vnode, void* ptr, SSHashObj* pVtableInfos,
                              SSDataBlock** ppResBlock, const char* idstr) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SStorageAPI*       pAPI = ptr;
  SVTSourceScanInfo* pScanInfo = NULL;
  SHashObj*          pVirtualTables = NULL;
  SMetaReader        metaReader = {0};
  SVTColInfo         colInfo = {0};
  SSchemaWrapper*    schema = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(vnode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pAPI, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pScanInfo = &pReader->vtSourceScanInfo;
  taosHashCleanup(pScanInfo->pVirtualTables);
  pScanInfo->pVirtualTables = NULL;

  if (tSimpleHashGetSize(pVtableInfos) == 0) {
    goto _end;
  }

  pVirtualTables = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(pVirtualTables, code, lino, _end, terrno);
  taosHashSetFreeFp(pVirtualTables, destroySourceScanTables);

  int32_t iter = 0;
  void*   px = tSimpleHashIterate(pVtableInfos, NULL, &iter);
  while (px != NULL) {
    int64_t vTbUid = *(int64_t*)tSimpleHashGetKey(px, NULL);
    SArray* pColInfos = taosArrayInit(8, sizeof(SVTColInfo));
    TSDB_CHECK_NULL(pColInfos, code, lino, _end, terrno);
    code = taosHashPut(pVirtualTables, &vTbUid, sizeof(int64_t), &pColInfos, POINTER_BYTES);
    TSDB_CHECK_CODE(code, lino, _end);

    SSHashObj* pPhysicalTables = *(SSHashObj**)px;
    int32_t    iterIn = 0;
    void*      pxIn = tSimpleHashIterate(pPhysicalTables, NULL, &iterIn);
    while (pxIn != NULL) {
      char* physicalTableName = tSimpleHashGetKey(pxIn, NULL);
      pAPI->metaReaderFn.clearReader(&metaReader);
      pAPI->metaReaderFn.initReader(&metaReader, vnode, META_READER_LOCK, &pAPI->metaFn);
      code = pAPI->metaReaderFn.getTableEntryByName(&metaReader, physicalTableName);
      TSDB_CHECK_CODE(code, lino, _end);
      pAPI->metaReaderFn.readerReleaseLock(&metaReader);
      colInfo.pTbUid = metaReader.me.uid;

      switch (metaReader.me.type) {
        case TSDB_CHILD_TABLE: {
          int64_t suid = metaReader.me.ctbEntry.suid;
          pAPI->metaReaderFn.clearReader(&metaReader);
          pAPI->metaReaderFn.initReader(&metaReader, vnode, META_READER_LOCK, &pAPI->metaFn);
          code = pAPI->metaReaderFn.getTableEntryByUid(&metaReader, suid);
          TSDB_CHECK_CODE(code, lino, _end);
          pAPI->metaReaderFn.readerReleaseLock(&metaReader);
          schema = &metaReader.me.stbEntry.schemaRow;
          break;
        }
        case TSDB_NORMAL_TABLE: {
          schema = &metaReader.me.ntbEntry.schemaRow;
          break;
        }
        default: {
          tqError("invalid table type: %d", metaReader.me.type);
          code = TSDB_CODE_INVALID_PARA;
          TSDB_CHECK_CODE(code, lino, _end);
        }
      }

      SArray* pCols = *(SArray**)pxIn;
      int32_t ncols = taosArrayGetSize(pCols);
      for (int32_t i = 0; i < ncols; ++i) {
        SColIdName* pCol = taosArrayGet(pCols, i);
        colInfo.vColId = pCol->colId;

        for (int32_t j = 0; j < schema->nCols; ++j) {
          if (strncmp(pCol->colName, schema->pSchema[j].name, strlen(schema->pSchema[j].name)) == 0) {
            colInfo.pColId = schema->pSchema[j].colId;
            void* px = taosArrayPush(pColInfos, &colInfo);
            TSDB_CHECK_NULL(px, code, lino, _end, terrno);
            break;
          }
        }
      }

      taosArraySort(pColInfos, compareSVTColInfo);
      pxIn = tSimpleHashIterate(pPhysicalTables, pxIn, &iterIn);
    }

    px = tSimpleHashIterate(pVtableInfos, px, &iter);
  }

  pScanInfo->pVirtualTables = pVirtualTables;
  pVirtualTables = NULL;

  // set the result data block
  if (pReader->pResBlock) {
    blockDataDestroy(pReader->pResBlock);
  }
  pReader->pResBlock = *ppResBlock;
  *ppResBlock = NULL;

  // update reader callback for vtable source scan
  pAPI->tqReaderFn.tqNextBlockImpl = tqNextVTableSourceBlockImpl;
  pAPI->tqReaderFn.tqReaderIsQueriedTable = tqReaderIsQueriedSourceTable;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  pAPI->metaReaderFn.clearReader(&metaReader);
  if (pVirtualTables != NULL) {
    taosHashCleanup(pVirtualTables);
  }
  return code;
}

static int32_t tqCollectPhysicalTables(STqReader* pReader, const char* idstr) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SVTSourceScanInfo* pScanInfo = NULL;
  SHashObj*          pVirtualTables = NULL;
  SHashObj*          pPhysicalTables = NULL;
  void*              pIter = NULL;
  void*              px = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pScanInfo = &pReader->vtSourceScanInfo;
  taosHashCleanup(pScanInfo->pPhysicalTables);
  pScanInfo->pPhysicalTables = NULL;
  taosLRUCacheCleanup(pScanInfo->pPhyTblSchemaCache);
  pScanInfo->pPhyTblSchemaCache = NULL;
  pScanInfo->nextVirtualTableIdx = -1;
  pScanInfo->metaFetch = 0;
  pScanInfo->cacheHit = 0;

  pVirtualTables = pScanInfo->pVirtualTables;
  if (taosHashGetSize(pVirtualTables) == 0 || taosArrayGetSize(pReader->pColIdList) == 0) {
    goto _end;
  }

  pPhysicalTables = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  TSDB_CHECK_NULL(pPhysicalTables, code, lino, _end, terrno);
  taosHashSetFreeFp(pPhysicalTables, destroySourceScanTables);

  pIter = taosHashIterate(pVirtualTables, NULL);
  while (pIter != NULL) {
    int64_t vTbUid = *(int64_t*)taosHashGetKey(pIter, NULL);
    SArray* pColInfos = *(SArray**)pIter;
    TSDB_CHECK_NULL(pColInfos, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

    // Traverse all required columns and collect corresponding physical tables
    int32_t nColInfos = taosArrayGetSize(pColInfos);
    int32_t nOutputCols = taosArrayGetSize(pReader->pColIdList);
    for (int32_t i = 0, j = 0; i < nColInfos && j < nOutputCols;) {
      SVTColInfo* pCol = taosArrayGet(pColInfos, i);
      col_id_t    colIdNeed = *(col_id_t*)taosArrayGet(pReader->pColIdList, j);
      if (pCol->vColId < colIdNeed) {
        i++;
      } else if (pCol->vColId > colIdNeed) {
        j++;
      } else {
        SArray* pRelatedVTs = NULL;
        px = taosHashGet(pPhysicalTables, &pCol->pTbUid, sizeof(int64_t));
        if (px == NULL) {
          pRelatedVTs = taosArrayInit(8, sizeof(int64_t));
          TSDB_CHECK_NULL(pRelatedVTs, code, lino, _end, terrno);
          code = taosHashPut(pPhysicalTables, &pCol->pTbUid, sizeof(int64_t), &pRelatedVTs, POINTER_BYTES);
          if (code != TSDB_CODE_SUCCESS) {
            taosArrayDestroy(pRelatedVTs);
            TSDB_CHECK_CODE(code, lino, _end);
          }
        } else {
          pRelatedVTs = *(SArray**)px;
        }
        if (taosArrayGetSize(pRelatedVTs) == 0 || *(int64_t*)taosArrayGetLast(pRelatedVTs) != vTbUid) {
          px = taosArrayPush(pRelatedVTs, &vTbUid);
          TSDB_CHECK_NULL(px, code, lino, _end, terrno);
        }
        i++;
        j++;
      }
    }
    pIter = taosHashIterate(pVirtualTables, pIter);
  }

  pScanInfo->pPhysicalTables = pPhysicalTables;
  pPhysicalTables = NULL;

  if (taosHashGetSize(pScanInfo->pPhysicalTables) > 0) {
    pScanInfo->pPhyTblSchemaCache = taosLRUCacheInit(1024 * 128, -1, .5);
    TSDB_CHECK_NULL(pScanInfo->pPhyTblSchemaCache, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  if (pIter != NULL) {
    taosHashCancelIterate(pReader->tbIdHash, pIter);
  }
  if (pPhysicalTables != NULL) {
    taosHashCleanup(pPhysicalTables);
  }
  return code;
}

static void freeTableSchemaCache(const void* key, size_t keyLen, void* value, void* ud) {
  if (value) {
    SSchemaWrapper* pSchemaWrapper = value;
    tDeleteSchemaWrapper(pSchemaWrapper);
  }
}

bool tqNextVTableSourceBlockImpl(STqReader* pReader, const char* idstr) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  SVTSourceScanInfo* pScanInfo = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pScanInfo = &pReader->vtSourceScanInfo;
  if (pReader->msg.msgStr == NULL || taosHashGetSize(pScanInfo->pPhysicalTables) == 0) {
    return false;
  }

  if (pScanInfo->nextVirtualTableIdx >= 0) {
    // The data still needs to be converted into the virtual table result block
    return true;
  }

  int32_t blockSz = taosArrayGetSize(pReader->submit.aSubmitTbData);
  while (pReader->nextBlk < blockSz) {
    SSubmitTbData* pSubmitTbData = taosArrayGet(pReader->submit.aSubmitTbData, pReader->nextBlk);
    TSDB_CHECK_NULL(pSubmitTbData, code, lino, _end, terrno);
    int64_t pTbUid = pSubmitTbData->uid;
    void*   px = taosHashGet(pScanInfo->pPhysicalTables, &pTbUid, sizeof(int64_t));
    if (px != NULL) {
      SArray* pRelatedVTs = *(SArray**)px;
      if (taosArrayGetSize(pRelatedVTs) > 0) {
        pScanInfo->nextVirtualTableIdx = 0;
        return true;
      }
    }
    tqTrace("iterator data block in hash jump block, progress:%d/%d, uid:%" PRId64, pReader->nextBlk, blockSz, pTbUid);
    pReader->nextBlk++;
  }

  tqReaderClearSubmitMsg(pReader);
  tqTrace("iterator data block end, total block num:%d", blockSz);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tqError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
  }
  return false;
}

bool tqReaderIsQueriedSourceTable(STqReader* pReader, uint64_t uid) {
  if (pReader == NULL) {
    return false;
  }
  return taosHashGet(pReader->vtSourceScanInfo.pPhysicalTables, &uid, sizeof(uint64_t)) != NULL;
}
