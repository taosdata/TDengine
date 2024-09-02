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

#include "functionMgt.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbReadUtil.h"

#define HASTYPE(_type, _t) (((_type) & (_t)) == (_t))

static int32_t setFirstLastResColToNull(SColumnInfoData* pCol, int32_t row) {
  char* buf = taosMemoryCalloc(1, pCol->info.bytes);
  if (buf == NULL) {
    return terrno;
  }

  SFirstLastRes* pRes = (SFirstLastRes*)((char*)buf + VARSTR_HEADER_SIZE);
  pRes->bytes = 0;
  pRes->hasResult = true;
  pRes->isNull = true;
  varDataSetLen(buf, pCol->info.bytes - VARSTR_HEADER_SIZE);
  int32_t code = colDataSetVal(pCol, row, buf, false);
  taosMemoryFree(buf);

  return code;
}

static int32_t saveOneRowForLastRaw(SLastCol* pColVal, SCacheRowsReader* pReader, const int32_t slotId,
                                 SColumnInfoData* pColInfoData, int32_t numOfRows) {
  SColVal* pVal = &pColVal->colVal;
  int32_t code = 0;

  // allNullRow = false;
  if (IS_VAR_DATA_TYPE(pColVal->colVal.value.type)) {
    if (!COL_VAL_IS_VALUE(&pColVal->colVal)) {
      colDataSetNULL(pColInfoData, numOfRows);
    } else {
      varDataSetLen(pReader->transferBuf[slotId], pVal->value.nData);

      memcpy(varDataVal(pReader->transferBuf[slotId]), pVal->value.pData, pVal->value.nData);
      code = colDataSetVal(pColInfoData, numOfRows, pReader->transferBuf[slotId], false);
    }
  } else {
    code = colDataSetVal(pColInfoData, numOfRows, (const char*)&pVal->value.val, !COL_VAL_IS_VALUE(pVal));
  }

  return code;
}

static int32_t saveOneRow(SArray* pRow, SSDataBlock* pBlock, SCacheRowsReader* pReader, const int32_t* slotIds,
                          const int32_t* dstSlotIds, void** pRes, const char* idStr) {
  int32_t numOfRows = pBlock->info.rows;
  int32_t code = 0;

  if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST)) {
    uint64_t       ts = TSKEY_MIN;
    SFirstLastRes* p = NULL;
    col_id_t       colId = -1;

    SArray* funcTypeBlockArray = taosArrayInit(pReader->numOfCols, sizeof(int32_t));
    if (funcTypeBlockArray == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotIds[i]);
      if (pColInfoData == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }

      int32_t funcType = FUNCTION_TYPE_CACHE_LAST;
      if (pReader->pFuncTypeList != NULL && taosArrayGetSize(pReader->pFuncTypeList) > i) {
        void* pVal = taosArrayGet(pReader->pFuncTypeList, i);
        if (pVal == NULL) {
          return TSDB_CODE_INVALID_PARA;
        }

        funcType = *(int32_t*) pVal;
        pVal = taosArrayGet(pReader->pFuncTypeList, i);
        if (pVal == NULL) {
          return TSDB_CODE_INVALID_PARA;
        }

        void* px = taosArrayInsert(funcTypeBlockArray, dstSlotIds[i], pVal);
        if (px == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }
      }

      if (slotIds[i] == -1) {
        if (FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
          colDataSetNULL(pColInfoData, numOfRows);
          continue;
        }

        code = setFirstLastResColToNull(pColInfoData, numOfRows);
        if (code) {
          return code;
        }
        continue;
      }

      int32_t   slotId = slotIds[i];
      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, i);
      if (pColVal == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }

      colId = pColVal->colVal.cid;
      if (FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
        code = saveOneRowForLastRaw(pColVal, pReader, slotId, pColInfoData, numOfRows);
        if (code) {
          return code;
        }

        continue;
      }

      p = (SFirstLastRes*)varDataVal(pRes[i]);

      p->ts = pColVal->rowKey.ts;
      ts = p->ts;
      p->isNull = !COL_VAL_IS_VALUE(&pColVal->colVal);
      // allNullRow = p->isNull & allNullRow;
      if (!p->isNull) {
        if (IS_VAR_DATA_TYPE(pColVal->colVal.value.type)) {
          varDataSetLen(p->buf, pColVal->colVal.value.nData);

          memcpy(varDataVal(p->buf), pColVal->colVal.value.pData, pColVal->colVal.value.nData);
          p->bytes = pColVal->colVal.value.nData + VARSTR_HEADER_SIZE;  // binary needs to plus the header size
        } else {
          memcpy(p->buf, &pColVal->colVal.value.val, pReader->pSchema->columns[slotId].bytes);
          p->bytes = pReader->pSchema->columns[slotId].bytes;
        }
      }

      // pColInfoData->info.bytes includes the VARSTR_HEADER_SIZE, need to subtract it
      p->hasResult = true;
      varDataSetLen(pRes[i], pColInfoData->info.bytes - VARSTR_HEADER_SIZE);
      code = colDataSetVal(pColInfoData, numOfRows, (const char*)pRes[i], false);
      if (code) {
        return code;
      }
    }

    for (int32_t idx = 0; idx < taosArrayGetSize(pBlock->pDataBlock); ++idx) {
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, idx);
      if (pCol == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }

      if (idx < funcTypeBlockArray->size) {
        void* pVal = taosArrayGet(funcTypeBlockArray, idx);
        if (pVal == NULL) {
          return TSDB_CODE_INVALID_PARA;
        }

        int32_t funcType = *(int32_t*)pVal;
        if (FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
          continue;
        }
      }

      if (pCol->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID && pCol->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
        if (ts == TSKEY_MIN) {
          colDataSetNULL(pCol, numOfRows);
        } else {
          code = colDataSetVal(pCol, numOfRows, (const char*)&ts, false);
          if (code) {
            return code;
          }
        }
        continue;
      } else if (pReader->numOfCols == 1 && idx != dstSlotIds[0] && (pCol->info.colId == colId || colId == -1)) {
        if (p && !p->isNull) {
          code = colDataSetVal(pCol, numOfRows, p->buf, false);
          if (code) {
            return code;
          }
        } else {
          colDataSetNULL(pCol, numOfRows);
        }
      }
    }

    // pBlock->info.rows += allNullRow ? 0 : 1;
    ++pBlock->info.rows;
    taosArrayDestroy(funcTypeBlockArray);
  } else if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST_ROW)) {
    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotIds[i]);
      if (pColInfoData == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }

      int32_t slotId = slotIds[i];
      if (slotId == -1) {
        colDataSetNULL(pColInfoData, numOfRows);
        continue;
      }

      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, i);
      if (pColVal == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }

      code = saveOneRowForLastRaw(pColVal, pReader, slotId, pColInfoData, numOfRows);
      if (code) {
        return code;
      }
    }

    // pBlock->info.rows += allNullRow ? 0 : 1;
    ++pBlock->info.rows;
  } else {
    tsdbError("invalid retrieve type:%d, %s", pReader->type, idStr);
    return TSDB_CODE_INVALID_PARA;
  }

  return code;
}

static int32_t setTableSchema(SCacheRowsReader* p, uint64_t suid, const char* idstr) {
  int32_t numOfTables = p->numOfTables;

  if (suid != 0) {
    p->pSchema = metaGetTbTSchema(p->pVnode->pMeta, suid, -1, 1);
    if (p->pSchema == NULL) {
      tsdbWarn("stable:%" PRIu64 " has been dropped, failed to retrieve cached rows, %s", suid, idstr);
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
  } else {
    for (int32_t i = 0; i < numOfTables; ++i) {
      uint64_t uid = p->pTableList[i].uid;
      p->pSchema = metaGetTbTSchema(p->pVnode->pMeta, uid, -1, 1);
      if (p->pSchema != NULL) {
        break;
      }

      tsdbWarn("table:%" PRIu64 " has been dropped, failed to retrieve cached rows, %s", uid, idstr);
    }

    // all queried tables have been dropped already, return immediately.
    if (p->pSchema == NULL) {
      tsdbWarn("all queried tables has been dropped, try next group, %s", idstr);
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbReuseCacherowsReader(void* reader, void* pTableIdList, int32_t numOfTables) {
  SCacheRowsReader* pReader = (SCacheRowsReader*)reader;

  pReader->pTableList = pTableIdList;
  pReader->numOfTables = numOfTables;
  pReader->lastTs = INT64_MIN;
  destroySttBlockReader(pReader->pLDataIterArray, NULL);
  pReader->pLDataIterArray = taosArrayInit(4, POINTER_BYTES);

  return (pReader->pLDataIterArray != NULL) ? TSDB_CODE_SUCCESS : TSDB_CODE_OUT_OF_MEMORY;
}

int32_t tsdbCacherowsReaderOpen(void* pVnode, int32_t type, void* pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                SArray* pCidList, int32_t* pSlotIds, uint64_t suid, void** pReader, const char* idstr,
                                SArray* pFuncTypeList, SColumnInfo* pPkCol, int32_t numOfPks) {
  *pReader = NULL;
  SCacheRowsReader* p = taosMemoryCalloc(1, sizeof(SCacheRowsReader));
  if (p == NULL) {
    return terrno;
  }

  p->type = type;
  p->pVnode = pVnode;
  p->pTsdb = p->pVnode->pTsdb;
  p->info.verRange = (SVersionRange){.minVer = 0, .maxVer = INT64_MAX};
  p->info.suid = suid;
  p->numOfCols = numOfCols;
  p->pCidList = pCidList;
  p->pSlotIds = pSlotIds;
  p->pFuncTypeList = pFuncTypeList;

  p->rowKey.numOfPKs = numOfPks;
  if (numOfPks > 0) {
    p->rowKey.pks[0].type = pPkCol->type;
    if (IS_VAR_DATA_TYPE(pPkCol->type)) {
      p->rowKey.pks[0].pData = taosMemoryCalloc(1, pPkCol->bytes);
    }

    p->pkColumn = *pPkCol;
  }

  if (numOfTables == 0) {
    *pReader = p;
    return TSDB_CODE_SUCCESS;
  }

  p->pTableList = pTableIdList;
  p->numOfTables = numOfTables;

  int32_t code = setTableSchema(p, suid, idstr);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbCacherowsReaderClose(p);
    return code;
  }

  p->transferBuf = taosMemoryCalloc(p->pSchema->numOfCols, POINTER_BYTES);
  if (p->transferBuf == NULL) {
    tsdbCacherowsReaderClose(p);
    return terrno;
  }

  for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE(p->pSchema->columns[i].type)) {
      p->transferBuf[i] = taosMemoryMalloc(p->pSchema->columns[i].bytes);
      if (p->transferBuf[i] == NULL) {
        tsdbCacherowsReaderClose(p);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }

  p->idstr = taosStrdup(idstr);
  code = taosThreadMutexInit(&p->readerMutex, NULL);
  if (code) {
    tsdbCacherowsReaderClose(p);
    return code;
  }

  p->lastTs = INT64_MIN;

  *pReader = p;
  return code;
}

void tsdbCacherowsReaderClose(void* pReader) {
  SCacheRowsReader* p = pReader;
  if (p == NULL) {
    return;
  }

  if (p->pSchema != NULL && p->transferBuf != NULL) {
    for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
      taosMemoryFreeClear(p->transferBuf[i]);
    }

    taosMemoryFree(p->transferBuf);
    taosMemoryFree(p->pSchema);
  }

  taosMemoryFree(p->pCurrSchema);

  if (p->rowKey.numOfPKs > 0) {
    for (int32_t i = 0; i < p->rowKey.numOfPKs; i++) {
      if (IS_VAR_DATA_TYPE(p->rowKey.pks[i].type)) {
        taosMemoryFree(p->rowKey.pks[i].pData);
      }
    }
  }

  if (p->pLDataIterArray) {
    destroySttBlockReader(p->pLDataIterArray, NULL);
  }

  if (p->pFileReader) {
    (void) tsdbDataFileReaderClose(&p->pFileReader);
    p->pFileReader = NULL;
  }

  taosMemoryFree((void*)p->idstr);
  (void) taosThreadMutexDestroy(&p->readerMutex);

  if (p->pTableMap) {
    void*   pe = NULL;
    int32_t iter = 0;
    while ((pe = tSimpleHashIterate(p->pTableMap, pe, &iter)) != NULL) {
      STableLoadInfo* pInfo = *(STableLoadInfo**)pe;
      taosArrayDestroy(pInfo->pTombData);
      pInfo->pTombData = NULL;
    }

    tSimpleHashCleanup(p->pTableMap);
  }
  if (p->uidList) {
    taosMemoryFree(p->uidList);
  }

  taosMemoryFree(pReader);
}

static int32_t tsdbCacheQueryReseek(void* pQHandle) {
  int32_t           code = 0;
  SCacheRowsReader* pReader = pQHandle;

  code = taosThreadMutexTryLock(&pReader->readerMutex);
  if (code == 0) {
    // pause current reader's state if not paused, save ts & version for resuming
    // just wait for the big all tables' snapshot untaking for now

    code = TSDB_CODE_VND_QUERY_BUSY;
    (void)taosThreadMutexUnlock(&pReader->readerMutex);

    return code;
  } else if (code == EBUSY) {
    return TSDB_CODE_VND_QUERY_BUSY;
  } else {
    return -1;
  }
}

int32_t tsdbRetrieveCacheRows(void* pReader, SSDataBlock* pResBlock, const int32_t* slotIds, const int32_t* dstSlotIds,
                              SArray* pTableUidList) {
  if (pReader == NULL || pResBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t           code = TSDB_CODE_SUCCESS;
  bool              hasRes = false;
  SArray*           pRow = NULL;
  void**            pRes = NULL;
  SCacheRowsReader* pr = pReader;
  int32_t           pkBufLen = 0;

  pr->pReadSnap = NULL;
  pRow = taosArrayInit(TARRAY_SIZE(pr->pCidList), sizeof(SLastCol));
  if (pRow == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  pRes = taosMemoryCalloc(pr->numOfCols, POINTER_BYTES);
  if (pRes == NULL) {
    code = terrno;
    goto _end;
  }

  pkBufLen = (pr->rowKey.numOfPKs > 0) ? pr->pkColumn.bytes : 0;
  for (int32_t j = 0; j < pr->numOfCols; ++j) {
    int32_t bytes = (slotIds[j] == -1) ? 1 : pr->pSchema->columns[slotIds[j]].bytes;

    pRes[j] = taosMemoryCalloc(1, sizeof(SFirstLastRes) + bytes + pkBufLen + VARSTR_HEADER_SIZE);
    if (pRes[j] == NULL) {
      code = terrno;
      goto _end;
    }

    SFirstLastRes* p = (SFirstLastRes*)varDataVal(pRes[j]);
    p->ts = INT64_MIN;
  }

  (void)taosThreadMutexLock(&pr->readerMutex);
  code = tsdbTakeReadSnap2((STsdbReader*)pr, tsdbCacheQueryReseek, &pr->pReadSnap);
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }

  int8_t ltype = (pr->type & CACHESCAN_RETRIEVE_LAST) >> 3;

  STableKeyInfo* pTableList = pr->pTableList;

  // retrieve the only one last row of all tables in the uid list.
  if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_SINGLE)) {
    SArray* pLastCols = taosArrayInit(pr->numOfCols, sizeof(SLastCol));
    if (pLastCols == NULL) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _end;
    }

    for (int32_t i = 0; i < pr->numOfCols; ++i) {
      int32_t slotId = slotIds[i];
      if (slotId == -1) {
        SLastCol p = {.rowKey.ts = INT64_MIN, .colVal.value.type = TSDB_DATA_TYPE_BOOL, .colVal.flag = CV_FLAG_NULL};
        void*    px = taosArrayPush(pLastCols, &p);
        if (px == NULL) {
          code = TSDB_CODE_OUT_OF_MEMORY;
          goto _end;
        }
        continue;
      }
      struct STColumn* pCol = &pr->pSchema->columns[slotId];
      SLastCol         p = {.rowKey.ts = INT64_MIN, .colVal.value.type = pCol->type, .colVal.flag = CV_FLAG_NULL};

      if (pr->rowKey.numOfPKs > 0) {
        p.rowKey.numOfPKs = pr->rowKey.numOfPKs;
        for (int32_t j = 0; j < pr->rowKey.numOfPKs; j++) {
          p.rowKey.pks[j].type = pr->pkColumn.type;
          if (IS_VAR_DATA_TYPE(pr->pkColumn.type)) {

            p.rowKey.pks[j].pData = taosMemoryCalloc(1, pr->pkColumn.bytes);
            if (p.rowKey.pks[j].pData == NULL) {
              code = terrno;
              goto _end;
            }
          }
        }
      }

      if (IS_VAR_DATA_TYPE(pCol->type)) {
        p.colVal.value.pData = taosMemoryCalloc(pCol->bytes, sizeof(char));
        if (p.colVal.value.pData == NULL) {
          code = terrno;
          goto _end;
        }
      }

      void* px = taosArrayPush(pLastCols, &p);
      if (px == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _end;
      }
    }

    int64_t st = taosGetTimestampUs();
    int64_t totalLastTs = INT64_MAX;
    for (int32_t i = 0; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pTableList[i].uid;

      code = tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype);
      if (code == -1) {// fix the invalid return code
        code = 0;
      } else if (code != 0) {
        goto _end;
      }

      if (TARRAY_SIZE(pRow) <= 0 || COL_VAL_IS_NONE(&((SLastCol*)TARRAY_DATA(pRow))[0].colVal)) {
        taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
        continue;
      }

      {
        bool    hasNotNullRow = true;
        int64_t singleTableLastTs = INT64_MAX;
        for (int32_t k = 0; k < pr->numOfCols; ++k) {
          if (slotIds[k] == -1) continue;
          SLastCol* p = taosArrayGet(pLastCols, k);
          if (p == NULL) {
            return TSDB_CODE_INVALID_PARA;
          }

          SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, k);
          if (pColVal == NULL) {
            return TSDB_CODE_INVALID_PARA;
          }

          if (tRowKeyCompare(&pColVal->rowKey, &p->rowKey) > 0) {
            if (!COL_VAL_IS_VALUE(&pColVal->colVal) && HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST)) {
              if (!COL_VAL_IS_VALUE(&p->colVal)) {
                hasNotNullRow = false;
              }
              // For all of cols is null, the last null col of last table will be save
              if (i != pr->numOfTables - 1 || k != pr->numOfCols - 1 || hasRes) {
                continue;
              }
            }

            hasRes = true;
            p->rowKey.ts = pColVal->rowKey.ts;
            for (int32_t j = 0; j < p->rowKey.numOfPKs; j++) {
              if (IS_VAR_DATA_TYPE(p->rowKey.pks[j].type)) {
                memcpy(p->rowKey.pks[j].pData, pColVal->rowKey.pks[j].pData, pColVal->rowKey.pks[j].nData);
                p->rowKey.pks[j].nData = pColVal->rowKey.pks[j].nData;
              } else {
                p->rowKey.pks[j].val = pColVal->rowKey.pks[j].val;
              }
            }

            if (k == 0) {
              if (TARRAY_SIZE(pTableUidList) == 0) {
                void* px = taosArrayPush(pTableUidList, &uid);
                if (px == NULL) {
                  code = TSDB_CODE_OUT_OF_MEMORY;
                  goto _end;
                }
              } else {
                taosArraySet(pTableUidList, 0, &uid);
              }
            }

            if (pColVal->rowKey.ts < singleTableLastTs && HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST)) {
              singleTableLastTs = pColVal->rowKey.ts;
            }

            if (!IS_VAR_DATA_TYPE(pColVal->colVal.value.type)) {
              p->colVal = pColVal->colVal;
            } else {
              if (COL_VAL_IS_VALUE(&pColVal->colVal)) {
                memcpy(p->colVal.value.pData, pColVal->colVal.value.pData, pColVal->colVal.value.nData);
              }

              p->colVal.value.nData = pColVal->colVal.value.nData;
              p->colVal.value.type = pColVal->colVal.value.type;
              p->colVal.flag = pColVal->colVal.flag;
              p->colVal.cid = pColVal->colVal.cid;
            }
          }
        }

        if (hasNotNullRow) {
          if (INT64_MAX == totalLastTs || (INT64_MAX != singleTableLastTs && totalLastTs < singleTableLastTs)) {
            totalLastTs = singleTableLastTs;
          }
          double cost = (taosGetTimestampUs() - st) / 1000.0;
          if (cost > tsCacheLazyLoadThreshold) {
            pr->lastTs = totalLastTs;
          }
        }
      }

      taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
    }

    if (hasRes) {
      code = saveOneRow(pLastCols, pResBlock, pr, slotIds, dstSlotIds, pRes, pr->idstr);
      if (code) {
        goto _end;
      }
    }

    taosArrayDestroyEx(pLastCols, tsdbCacheFreeSLastColItem);
  } else if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_ALL)) {
    for (int32_t i = pr->tableIndex; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pTableList[i].uid;

      if ((code = tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype)) != 0) {
        if (code == -1) {// fix the invalid return code
          code = 0;
        } else if (code != 0) {
          goto _end;
        }
      }

      if (TARRAY_SIZE(pRow) <= 0 || COL_VAL_IS_NONE(&((SLastCol*)TARRAY_DATA(pRow))[0].colVal)) {
        taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
        continue;
      }

      code = saveOneRow(pRow, pResBlock, pr, slotIds, dstSlotIds, pRes, pr->idstr);
      if (code) {
        goto _end;
      }

      taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);

      void* px = taosArrayPush(pTableUidList, &uid);
      if (px == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        goto _end;
      }

      ++pr->tableIndex;
      if (pResBlock->info.rows >= pResBlock->info.capacity) {
        goto _end;
      }
    }
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }

_end:
  tsdbUntakeReadSnap2((STsdbReader*)pr, pr->pReadSnap, true);
  pr->pReadSnap = NULL;

  if (pr->pCurFileSet) {
    pr->pCurFileSet = NULL;
  }

  (void)taosThreadMutexUnlock(&pr->readerMutex);

  if (pRes != NULL) {
    for (int32_t j = 0; j < pr->numOfCols; ++j) {
      taosMemoryFree(pRes[j]);
    }
  }

  taosMemoryFree(pRes);
  taosArrayDestroy(pRow);

  return code;
}
