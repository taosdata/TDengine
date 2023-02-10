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
                          void** pRes, const char* idStr) {
  int32_t numOfRows = pBlock->info.rows;

  if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST)) {
    bool allNullRow = true;

    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
      SFirstLastRes*   p = (SFirstLastRes*)varDataVal(pRes[i]);

      if (slotIds[i] == -1) {  // the primary timestamp
        SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, 0);
        p->ts = pColVal->ts;
        p->bytes = TSDB_KEYSIZE;
        *(int64_t*)p->buf = pColVal->ts;
        allNullRow = false;
      } else {
        int32_t   slotId = slotIds[i];
        SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, slotId);

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
      }

      // pColInfoData->info.bytes includes the VARSTR_HEADER_SIZE, need to substruct it
      p->hasResult = true;
      varDataSetLen(pRes[i], pColInfoData->info.bytes - VARSTR_HEADER_SIZE);
      colDataAppend(pColInfoData, numOfRows, (const char*)pRes[i], false);
    }

    pBlock->info.rows += allNullRow ? 0 : 1;
  } else if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST_ROW)) {
    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

      if (slotIds[i] == -1) {
        SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, 0);
        colDataAppend(pColInfoData, numOfRows, (const char*)&pColVal->ts, false);
      } else {
        int32_t   slotId = slotIds[i];
        SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, slotId);
        SColVal*  pVal = &pColVal->colVal;

        if (IS_VAR_DATA_TYPE(pColVal->colVal.type)) {
          if (!COL_VAL_IS_VALUE(&pColVal->colVal)) {
            colDataAppendNULL(pColInfoData, numOfRows);
          } else {
            varDataSetLen(pReader->transferBuf[slotId], pVal->value.nData);
            memcpy(varDataVal(pReader->transferBuf[slotId]), pVal->value.pData, pVal->value.nData);
            colDataAppend(pColInfoData, numOfRows, pReader->transferBuf[slotId], false);
          }
        } else {
          colDataAppend(pColInfoData, numOfRows, (const char*)&pVal->value.val, !COL_VAL_IS_VALUE(pVal));
        }
      }
    }

    pBlock->info.rows += 1;
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
      taosMemoryFree(p);
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
      taosMemoryFree(p);
      tsdbWarn("all queried tables has been dropped, try next group, %s", idstr);
      return TSDB_CODE_PAR_TABLE_NOT_EXIST;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbCacherowsReaderOpen(void* pVnode, int32_t type, void* pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                uint64_t suid, void** pReader, const char* idstr) {
  *pReader = NULL;
  SCacheRowsReader* p = taosMemoryCalloc(1, sizeof(SCacheRowsReader));
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  p->type = type;
  p->pVnode = pVnode;
  p->numOfCols = numOfCols;
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

  p->pLoadInfo = tCreateLastBlockLoadInfo(p->pSchema, NULL, 0);
  if (p->pLoadInfo == NULL) {
    tsdbCacherowsReaderClose(p);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  p->idstr = taosMemoryStrDup(idstr);

  *pReader = p;
  return TSDB_CODE_SUCCESS;
}

void* tsdbCacherowsReaderClose(void* pReader) {
  SCacheRowsReader* p = pReader;

  if (p->pSchema != NULL) {
    for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
      taosMemoryFreeClear(p->transferBuf[i]);
    }

    taosMemoryFree(p->transferBuf);
    taosMemoryFree(p->pSchema);
  }

  destroyLastBlockLoadInfo(p->pLoadInfo);

  taosMemoryFree((void*)p->idstr);
  taosMemoryFree(pReader);
  return NULL;
}

static int32_t doExtractCacheRow(SCacheRowsReader* pr, SLRUCache* lruCache, uint64_t uid, SArray** pRow,
                                 LRUHandle** h) {
  int32_t code = TSDB_CODE_SUCCESS;
  *pRow = NULL;

  if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST_ROW)) {
    code = tsdbCacheGetLastrowH(lruCache, uid, pr, h);
  } else {
    code = tsdbCacheGetLastH(lruCache, uid, pr, h);
  }

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // no data in the table of Uid
  if (*h != NULL) {
    *pRow = (SArray*)taosLRUCacheValue(lruCache, *h);
  }

  return code;
}

static void freeItem(void* pItem) {
  SLastCol* pCol = (SLastCol*)pItem;
  if (IS_VAR_DATA_TYPE(pCol->colVal.type)) {
    taosMemoryFree(pCol->colVal.value.pData);
  }
}

int32_t tsdbRetrieveCacheRows(void* pReader, SSDataBlock* pResBlock, const int32_t* slotIds, SArray* pTableUidList) {
  if (pReader == NULL || pResBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SCacheRowsReader* pr = pReader;

  int32_t    code = TSDB_CODE_SUCCESS;
  SLRUCache* lruCache = pr->pVnode->pTsdb->lruCache;
  LRUHandle* h = NULL;
  SArray*    pRow = NULL;
  bool       hasRes = false;
  SArray*    pLastCols = NULL;

  void** pRes = taosMemoryCalloc(pr->numOfCols, POINTER_BYTES);
  if (pRes == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  for (int32_t j = 0; j < pr->numOfCols; ++j) {
    pRes[j] = taosMemoryCalloc(1, sizeof(SFirstLastRes) + pr->pSchema->columns[slotIds[j]].bytes + VARSTR_HEADER_SIZE);
    SFirstLastRes* p = (SFirstLastRes*)varDataVal(pRes[j]);
    p->ts = INT64_MIN;
  }

  pLastCols = taosArrayInit(pr->pSchema->numOfCols, sizeof(SLastCol));
  if (pLastCols == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  for (int32_t i = 0; i < pr->pSchema->numOfCols; ++i) {
    struct STColumn* pCol = &pr->pSchema->columns[i];
    SLastCol         p = {.ts = INT64_MIN, .colVal.type = pCol->type};

    if (IS_VAR_DATA_TYPE(pCol->type)) {
      p.colVal.value.pData = taosMemoryCalloc(pCol->bytes, sizeof(char));
    }
    taosArrayPush(pLastCols, &p);
  }

  code = tsdbTakeReadSnap(pr->pVnode->pTsdb, &pr->pReadSnap, "cache-l");
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }

  pr->pDataFReader = NULL;
  pr->pDataFReaderLast = NULL;

  // retrieve the only one last row of all tables in the uid list.
  if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_SINGLE)) {
    for (int32_t i = 0; i < pr->numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = &pr->pTableList[i];

      code = doExtractCacheRow(pr, lruCache, pKeyInfo->uid, &pRow, &h);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }

      if (h == NULL) {
        continue;
      }

      {
        for (int32_t k = 0; k < pr->numOfCols; ++k) {
          int32_t slotId = slotIds[k];

          if (slotId == -1) {  // the primary timestamp
            SLastCol* p = taosArrayGet(pLastCols, 0);
            SLastCol* pCol = (SLastCol*)taosArrayGet(pRow, 0);
            if (pCol->ts > p->ts) {
              hasRes = true;
              p->ts = pCol->ts;
              p->colVal = pCol->colVal;

              // only set value for last row query
              if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST_ROW)) {
                if (taosArrayGetSize(pTableUidList) == 0) {
                  taosArrayPush(pTableUidList, &pKeyInfo->uid);
                } else {
                  taosArraySet(pTableUidList, 0, &pKeyInfo->uid);
                }
              }
            }
          } else {
            SLastCol* p = taosArrayGet(pLastCols, slotId);
            SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, slotId);

            if (pColVal->ts > p->ts) {
              if (!COL_VAL_IS_VALUE(&pColVal->colVal) && HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST)) {
                continue;
              }

              hasRes = true;
              p->ts = pColVal->ts;

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
        }
      }

      tsdbCacheRelease(lruCache, h);
    }

    if (hasRes) {
      saveOneRow(pLastCols, pResBlock, pr, slotIds, pRes, pr->idstr);
    }

  } else if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_ALL)) {
    for (int32_t i = pr->tableIndex; i < pr->numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = &pr->pTableList[i];
      code = doExtractCacheRow(pr, lruCache, pKeyInfo->uid, &pRow, &h);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }

      if (h == NULL) {
        continue;
      }

      saveOneRow(pRow, pResBlock, pr, slotIds, pRes, pr->idstr);
      // TODO reset the pRes

      taosArrayPush(pTableUidList, &pKeyInfo->uid);
      tsdbCacheRelease(lruCache, h);

      pr->tableIndex += 1;
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

  tsdbUntakeReadSnap(pr->pVnode->pTsdb, pr->pReadSnap, "cache-l");
  resetLastBlockLoadInfo(pr->pLoadInfo);

  for (int32_t j = 0; j < pr->numOfCols; ++j) {
    taosMemoryFree(pRes[j]);
  }

  taosMemoryFree(pRes);
  taosArrayDestroyEx(pLastCols, freeItem);
  return code;
}
