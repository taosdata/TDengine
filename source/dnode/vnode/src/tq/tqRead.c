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
          *fetchOffset = offset;
          code = 0;
          goto END;
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
      if (pReader->pWalReader->pHead->head.msgType != TDMT_VND_SUBMIT) {
        // TODO do filter
        ret->fetchType = FETCH_TYPE__META;
        ret->meta = pReader->pWalReader->pHead->head.body;
        return 0;
      } else {
        tqReaderSetDataMsg(pReader, body, pReader->pWalReader->pHead->head.version);
      }
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
      return 0;
    }

    if (fromProcessedMsg) {
      ret->offset.type = TMQ_OFFSET__LOG;
      ret->offset.version = pReader->ver;
      ASSERT(pReader->ver >= 0);
      ret->fetchType = FETCH_TYPE__NONE;
      tqDebug("return offset %" PRId64 ", processed finish", ret->offset.version);
      return 0;
    }
  }
}

int32_t tqReaderSetDataMsg(STqReader* pReader, SSubmitReq* pMsg, int64_t ver) {
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
    pReader->pSchema = metaGetTbTSchema(pReader->pVnodeMeta, pReader->msgIter.uid, sversion);
    if (pReader->pSchema == NULL) {
      tqWarn("cannot found tsschema for table: uid:%" PRId64 " (suid:%" PRId64 "), version %d, possibly dropped table",
             pReader->msgIter.uid, pReader->msgIter.suid, pReader->cachedSchemaVer);
      /*ASSERT(0);*/
      terrno = TSDB_CODE_TQ_TABLE_SCHEMA_NOT_FOUND;
      return -1;
    }

    if (pReader->pSchemaWrapper) tDeleteSSchemaWrapper(pReader->pSchemaWrapper);
    pReader->pSchemaWrapper = metaGetTableSchema(pReader->pVnodeMeta, pReader->msgIter.uid, sversion, true);
    if (pReader->pSchemaWrapper == NULL) {
      tqWarn("cannot found schema wrapper for table: suid:%" PRId64 ", version %d, possibly dropped table",
             pReader->msgIter.uid, pReader->cachedSchemaVer);
      /*ASSERT(0);*/
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

  pBlock->info.uid = pReader->msgIter.uid;
  pBlock->info.rows = pReader->msgIter.numOfRows;
  pBlock->info.version = pReader->pMsg->version;

  while ((row = tGetSubmitBlkNext(&pReader->blkIter)) != NULL) {
    tdSTSRowIterReset(&iter, row);
    // get all wanted col of that block
    for (int32_t i = 0; i < colActual; i++) {
      SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, i);
      SCellVal         sVal = {0};
      if (!tdSTSRowIterNext(&iter, pColData->info.colId, pColData->info.type, &sVal)) {
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

void tqReaderSetColIdList(STqReader* pReadHandle, SArray* pColIdList) { pReadHandle->pColIdList = pColIdList; }

int tqReaderSetTbUidList(STqReader* pReader, const SArray* tbUidList) {
  if (pReader->tbIdHash) {
    taosHashClear(pReader->tbIdHash);
  } else {
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
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
    pReader->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
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
    pIter = taosHashIterate(pTq->handles, pIter);
    if (pIter == NULL) break;
    STqHandle* pExec = (STqHandle*)pIter;
    if (pExec->execHandle.subType == TOPIC_SUB_TYPE__COLUMN) {
      int32_t code = qUpdateQualifiedTableId(pExec->execHandle.execCol.task, tbUidList, isAdd);
      ASSERT(code == 0);
    } else if (pExec->execHandle.subType == TOPIC_SUB_TYPE__DB) {
      if (!isAdd) {
        int32_t sz = taosArrayGetSize(tbUidList);
        for (int32_t i = 0; i < sz; i++) {
          int64_t tbUid = *(int64_t*)taosArrayGet(tbUidList, i);
          taosHashPut(pExec->execHandle.execDb.pFilterOutTbUid, &tbUid, sizeof(int64_t), NULL, 0);
        }
      }
    } else {
      // tq update id
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
