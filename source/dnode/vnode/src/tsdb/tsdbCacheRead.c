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

#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tsdb.h"

#define HASTYPE(_type, _t) (((_type) & (_t)) == (_t))

static int32_t saveOneRow(SArray* pRow, SSDataBlock* pBlock, SCacheRowsReader* pReader, const int32_t* slotIds,
                          const int32_t* dstSlotIds, void** pRes, const char* idStr) {
  int32_t numOfRows = pBlock->info.rows;
  bool    allNullRow = true;

  if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST)) {
    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotIds[i]);
      SFirstLastRes*   p = (SFirstLastRes*)varDataVal(pRes[i]);
      int32_t          slotId = slotIds[i];
      SLastCol*        pColVal = (SLastCol*)taosArrayGet(pRow, i);

      p->ts = pColVal->ts;
      p->isNull = !COL_VAL_IS_VALUE(&pColVal->colVal);
      allNullRow = p->isNull & allNullRow;

      if (!p->isNull) {
        if (IS_VAR_DATA_TYPE(pColVal->colVal.type)) {
          varDataSetLen(p->buf, pColVal->colVal.value.nData);

          memcpy(varDataVal(p->buf), pColVal->colVal.value.pData, pColVal->colVal.value.nData);
          p->bytes = pColVal->colVal.value.nData + VARSTR_HEADER_SIZE;  // binary needs to plus the header size
        } else {
          memcpy(p->buf, &pColVal->colVal.value, pReader->pSchema->columns[slotId].bytes);
          p->bytes = pReader->pSchema->columns[slotId].bytes;
        }
      }

      // pColInfoData->info.bytes includes the VARSTR_HEADER_SIZE, need to substruct it
      p->hasResult = true;
      varDataSetLen(pRes[i], pColInfoData->info.bytes - VARSTR_HEADER_SIZE);
      colDataSetVal(pColInfoData, numOfRows, (const char*)pRes[i], false);
    }

    pBlock->info.rows += allNullRow ? 0 : 1;
  } else if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST_ROW)) {
    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotIds[i]);

      int32_t   slotId = slotIds[i];
      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, i);
      SColVal*  pVal = &pColVal->colVal;

      allNullRow = false;
      if (IS_VAR_DATA_TYPE(pColVal->colVal.type)) {
        if (!COL_VAL_IS_VALUE(&pColVal->colVal)) {
          colDataSetNULL(pColInfoData, numOfRows);
        } else {
          varDataSetLen(pReader->transferBuf[slotId], pVal->value.nData);

          memcpy(varDataVal(pReader->transferBuf[slotId]), pVal->value.pData, pVal->value.nData);
          colDataSetVal(pColInfoData, numOfRows, pReader->transferBuf[slotId], false);
        }
      } else {
        colDataSetVal(pColInfoData, numOfRows, (const char*)&pVal->value.val, !COL_VAL_IS_VALUE(pVal));
      }
    }

    pBlock->info.rows += allNullRow ? 0 : 1;
  } else {
    tsdbError("invalid retrieve type:%d, %s", pReader->type, idStr);
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
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

  resetLastBlockLoadInfo(pReader->pLoadInfo);

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbCacherowsReaderOpen(void* pVnode, int32_t type, void* pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                SArray* pCidList, int32_t* pSlotIds, uint64_t suid, void** pReader, const char* idstr) {
  *pReader = NULL;
  SCacheRowsReader* p = taosMemoryCalloc(1, sizeof(SCacheRowsReader));
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  p->type = type;
  p->pVnode = pVnode;
  p->pTsdb = p->pVnode->pTsdb;
  p->verRange = (SVersionRange){.minVer = 0, .maxVer = UINT64_MAX};
  p->numOfCols = numOfCols;
  p->pCidList = pCidList;
  p->pSlotIds = pSlotIds;
  p->suid = suid;

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
    return TSDB_CODE_OUT_OF_MEMORY;
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

  SVnodeCfg* pCfg = &((SVnode*)pVnode)->config;
  int32_t    numOfStt = pCfg->sttTrigger;
  p->pLoadInfo = tCreateLastBlockLoadInfo(p->pSchema, NULL, 0, numOfStt);
  if (p->pLoadInfo == NULL) {
    tsdbCacherowsReaderClose(p);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  p->pDataIter = taosMemoryCalloc(pCfg->sttTrigger, sizeof(SLDataIter));
  if (p->pDataIter == NULL) {
    tsdbCacherowsReaderClose(p);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  p->idstr = taosStrdup(idstr);
  taosThreadMutexInit(&p->readerMutex, NULL);

  p->lastTs = INT64_MIN;

  *pReader = p;
  return TSDB_CODE_SUCCESS;
}

void* tsdbCacherowsReaderClose(void* pReader) {
  SCacheRowsReader* p = pReader;
  if (p == NULL) {
    return NULL;
  }

  if (p->pSchema != NULL) {
    for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
      taosMemoryFreeClear(p->transferBuf[i]);
    }

    taosMemoryFree(p->transferBuf);
    taosMemoryFree(p->pSchema);
  }

  taosMemoryFree(p->pDataIter);
  taosMemoryFree(p->pCurrSchema);

  destroyLastBlockLoadInfo(p->pLoadInfo);

  taosMemoryFree((void*)p->idstr);
  taosThreadMutexDestroy(&p->readerMutex);

  taosMemoryFree(pReader);
  return NULL;
}

static void freeItem(void* pItem) {
  SLastCol* pCol = (SLastCol*)pItem;
  if (IS_VAR_DATA_TYPE(pCol->colVal.type) && pCol->colVal.value.pData) {
    taosMemoryFree(pCol->colVal.value.pData);
  }
}

static int32_t tsdbCacheQueryReseek(void* pQHandle) {
  int32_t           code = 0;
  SCacheRowsReader* pReader = pQHandle;

  code = taosThreadMutexTryLock(&pReader->readerMutex);
  if (code == 0) {
    // pause current reader's state if not paused, save ts & version for resuming
    // just wait for the big all tables' snapshot untaking for now

    code = TSDB_CODE_VND_QUERY_BUSY;

    taosThreadMutexUnlock(&pReader->readerMutex);

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

  SCacheRowsReader* pr = pReader;
  int32_t           code = TSDB_CODE_SUCCESS;
  SArray*           pRow = taosArrayInit(TARRAY_SIZE(pr->pCidList), sizeof(SLastCol));
  bool              hasRes = false;
  SArray*           pLastCols = NULL;

  void** pRes = taosMemoryCalloc(pr->numOfCols, POINTER_BYTES);
  if (pRes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  for (int32_t j = 0; j < pr->numOfCols; ++j) {
    pRes[j] =
        taosMemoryCalloc(1, sizeof(SFirstLastRes) + pr->pSchema->columns[/*-1 == slotIds[j] ? 0 : */ slotIds[j]].bytes +
                                VARSTR_HEADER_SIZE);
    SFirstLastRes* p = (SFirstLastRes*)varDataVal(pRes[j]);
    p->ts = INT64_MIN;
  }

  pLastCols = taosArrayInit(pr->numOfCols, sizeof(SLastCol));
  if (pLastCols == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  for (int32_t i = 0; i < pr->numOfCols; ++i) {
    int32_t          slotId = slotIds[i];
    struct STColumn* pCol = &pr->pSchema->columns[slotId];
    SLastCol         p = {.ts = INT64_MIN, .colVal.type = pCol->type, .colVal.flag = CV_FLAG_NULL};

    if (IS_VAR_DATA_TYPE(pCol->type)) {
      p.colVal.value.pData = taosMemoryCalloc(pCol->bytes, sizeof(char));
    }
    taosArrayPush(pLastCols, &p);
  }

  taosThreadMutexLock(&pr->readerMutex);
  code = tsdbTakeReadSnap((STsdbReader*)pr, tsdbCacheQueryReseek, &pr->pReadSnap);
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }
  pr->pDataFReader = NULL;
  pr->pDataFReaderLast = NULL;

  int8_t ltype = (pr->type & CACHESCAN_RETRIEVE_LAST) >> 3;

  // retrieve the only one last row of all tables in the uid list.
  if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_SINGLE)) {
    int64_t st = taosGetTimestampUs();
    int64_t totalLastTs = INT64_MAX;

    for (int32_t i = 0; i < pr->numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = &pr->pTableList[i];

      tsdbCacheGetBatch(pr->pTsdb, pKeyInfo->uid, pRow, pr, ltype);
      // tsdbCacheGet(pr->pTsdb, pKeyInfo->uid, pRow, pr, ltype);
      if (TARRAY_SIZE(pRow) <= 0) {
        taosArrayClearEx(pRow, freeItem);
        // taosArrayClear(pRow);
        continue;
      }
      SLastCol* pColVal = taosArrayGet(pRow, 0);
      if (COL_VAL_IS_NONE(&pColVal->colVal)) {
        taosArrayClearEx(pRow, freeItem);
        // taosArrayClear(pRow);
        continue;
      }

      {
        bool    hasNotNullRow = true;
        int64_t singleTableLastTs = INT64_MAX;
        for (int32_t k = 0; k < pr->numOfCols; ++k) {
          SLastCol* p = taosArrayGet(pLastCols, k);
          SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, k);

          if (pColVal->ts > p->ts) {
            if (!COL_VAL_IS_VALUE(&pColVal->colVal) && HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST)) {
              if (!COL_VAL_IS_VALUE(&p->colVal)) {
                hasNotNullRow = false;
              }
              continue;
            }

            hasRes = true;
            p->ts = pColVal->ts;
            if (k == 0) {
              if (TARRAY_SIZE(pTableUidList) == 0) {
                taosArrayPush(pTableUidList, &pKeyInfo->uid);
              } else {
                taosArraySet(pTableUidList, 0, &pKeyInfo->uid);
              }
            }

            if (pColVal->ts < singleTableLastTs && HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST)) {
              singleTableLastTs = pColVal->ts;
            }

            if (!IS_VAR_DATA_TYPE(pColVal->colVal.type)) {
              p->colVal = pColVal->colVal;
            } else {
              if (COL_VAL_IS_VALUE(&pColVal->colVal)) {
                memcpy(p->colVal.value.pData, pColVal->colVal.value.pData, pColVal->colVal.value.nData);
              }

              p->colVal.value.nData = pColVal->colVal.value.nData;
              p->colVal.type = pColVal->colVal.type;
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

      taosArrayClearEx(pRow, freeItem);
      // taosArrayClear(pRow);
    }

    if (hasRes) {
      saveOneRow(pLastCols, pResBlock, pr, slotIds, dstSlotIds, pRes, pr->idstr);
    }
  } else if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_ALL)) {
    for (int32_t i = pr->tableIndex; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pr->pTableList[i].uid;

      tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype);
      if (TARRAY_SIZE(pRow) <= 0) {
        taosArrayClearEx(pRow, freeItem);
        // taosArrayClear(pRow);
        continue;
      }
      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, 0);
      if (COL_VAL_IS_NONE(&pColVal->colVal)) {
        taosArrayClearEx(pRow, freeItem);
        // taosArrayClear(pRow);
        continue;
      }

      saveOneRow(pRow, pResBlock, pr, slotIds, dstSlotIds, pRes, pr->idstr);
      taosArrayClearEx(pRow, freeItem);
      // taosArrayClear(pRow);

      taosArrayPush(pTableUidList, &uid);

      ++pr->tableIndex;
      if (pResBlock->info.rows >= pResBlock->info.capacity) {
        goto _end;
      }
    }
  } else {
    code = TSDB_CODE_INVALID_PARA;
  }

_end:
  tsdbDataFReaderClose(&pr->pDataFReaderLast);
  tsdbDataFReaderClose(&pr->pDataFReader);

  resetLastBlockLoadInfo(pr->pLoadInfo);
  tsdbUntakeReadSnap((STsdbReader*)pr, pr->pReadSnap, true);
  taosThreadMutexUnlock(&pr->readerMutex);

  if (pRes != NULL) {
    for (int32_t j = 0; j < pr->numOfCols; ++j) {
      taosMemoryFree(pRes[j]);
    }
  }

  taosMemoryFree(pRes);
  // taosArrayDestroyEx(pRow, freeItem);
  taosArrayDestroy(pRow);
  taosArrayDestroyEx(pLastCols, freeItem);

  return code;
}
