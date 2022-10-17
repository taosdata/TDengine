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

typedef struct SCacheRowsReader {
  SVnode*   pVnode;
  STSchema* pSchema;
  uint64_t  uid;
  char**    transferBuf;  // todo remove it soon
  int32_t   numOfCols;
  int32_t   type;
  int32_t   tableIndex;  // currently returned result tables
  SArray*   pTableList;  // table id list
} SCacheRowsReader;

static void saveOneRow(SArray* pRow, SSDataBlock* pBlock, SCacheRowsReader* pReader, const int32_t* slotIds,
                       SFirstLastRes** pRes) {
  ASSERT(pReader->numOfCols <= taosArrayGetSize(pBlock->pDataBlock));
  int32_t numOfRows = pBlock->info.rows;

  for (int32_t i = 0; i < pReader->numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    if (slotIds[i] == -1) {  // the primary timestamp
      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, 0);
      pRes[i]->ts = pColVal->ts;
      memcpy(pRes[i]->buf, &pColVal->ts, TSDB_KEYSIZE);
    } else {
      int32_t   slotId = slotIds[i];
      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, slotId);

      pRes[i]->ts = pColVal->ts;
      pRes[i]->isNull = !COL_VAL_IS_VALUE(&pColVal->colVal);

      if (!pRes[i]->isNull) {
        if (IS_VAR_DATA_TYPE(pColVal->colVal.type)) {
          varDataSetLen(pRes[i]->buf, pColVal->colVal.value.nData);
          memcpy(varDataVal(pRes[i]->buf), pColVal->colVal.value.pData, pColVal->colVal.value.nData);
        } else {
          memcpy(pRes[i]->buf, &pColVal->colVal.value, pReader->pSchema->columns[slotId].bytes);
        }
      }
    }

    pRes[i]->hasResult = true;
    colDataAppend(pColInfoData, numOfRows, (const char*)pRes[i], false);
  }

  pBlock->info.rows += 1;
}

int32_t tsdbCacherowsReaderOpen(void* pVnode, int32_t type, SArray* pTableIdList, int32_t numOfCols, void** pReader) {
  *pReader = NULL;

  SCacheRowsReader* p = taosMemoryCalloc(1, sizeof(SCacheRowsReader));
  if (p == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  p->type = type;
  p->pVnode = pVnode;
  p->numOfCols = numOfCols;

  if (taosArrayGetSize(pTableIdList) == 0) {
    *pReader = p;
    return TSDB_CODE_SUCCESS;
  }

  STableKeyInfo* pKeyInfo = taosArrayGet(pTableIdList, 0);
  p->pSchema = metaGetTbTSchema(p->pVnode->pMeta, pKeyInfo->uid, -1, 1);
  p->pTableList = pTableIdList;

  p->transferBuf = taosMemoryCalloc(p->pSchema->numOfCols, POINTER_BYTES);
  if (p->transferBuf == NULL) {
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

  taosMemoryFree(pReader);
  return NULL;
}

static int32_t doExtractCacheRow(SCacheRowsReader* pr, SLRUCache* lruCache, uint64_t uid, SArray** pRow,
                                 LRUHandle** h) {
  int32_t code = TSDB_CODE_SUCCESS;
  if ((pr->type & CACHESCAN_RETRIEVE_LAST_ROW) == CACHESCAN_RETRIEVE_LAST_ROW) {
    code = tsdbCacheGetLastrowH(lruCache, uid, pr->pVnode->pTsdb, h);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // no data in the table of Uid
    if (*h != NULL) {
      *pRow = (SArray*)taosLRUCacheValue(lruCache, *h);
    }
  } else {
    code = tsdbCacheGetLastH(lruCache, uid, pr->pVnode->pTsdb, h);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // no data in the table of Uid
    if (*h != NULL) {
      *pRow = (SArray*)taosLRUCacheValue(lruCache, *h);
    }
  }

  return code;
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
  size_t     numOfTables = taosArrayGetSize(pr->pTableList);
  bool       hasRes = false;

  SFirstLastRes** pRes = taosMemoryCalloc(pr->numOfCols, POINTER_BYTES);
  for (int32_t j = 0; j < pr->numOfCols; ++j) {
    pRes[j] = taosMemoryCalloc(1, sizeof(SFirstLastRes) + pr->pSchema->columns[slotIds[j]].bytes);
    pRes[j]->ts = INT64_MIN;
  }

  // retrieve the only one last row of all tables in the uid list.
  if ((pr->type & CACHESCAN_RETRIEVE_TYPE_SINGLE) == CACHESCAN_RETRIEVE_TYPE_SINGLE) {
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = taosArrayGet(pr->pTableList, i);

      code = doExtractCacheRow(pr, lruCache, pKeyInfo->uid, &pRow, &h);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (h == NULL) {
        continue;
      }

      {
        for (int32_t k = 0; k < pr->numOfCols; ++k) {
          SColumnInfoData* pColInfoData = taosArrayGet(pResBlock->pDataBlock, k);

          if (slotIds[k] == -1) {  // the primary timestamp
            SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, k);
            if (pColVal->ts > pRes[k]->ts || !pRes[k]->hasResult) {
              hasRes = true;
              pRes[k]->hasResult = true;
              pRes[k]->ts = pColVal->ts;
              memcpy(pRes[k]->buf, &pColVal->ts, TSDB_KEYSIZE);

              colDataAppend(pColInfoData, 1, (const char*)pRes[k], false);
            }
          } else {
            int32_t   slotId = slotIds[k];
            SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, slotId);

            if (pColVal->ts > pRes[k]->ts || !pRes[k]->hasResult) {
              hasRes = true;
              pRes[k]->hasResult = true;
              pRes[k]->ts = pColVal->ts;

              pRes[k]->isNull = !COL_VAL_IS_VALUE(&pColVal->colVal);
              if (!pRes[k]->isNull) {
                if (IS_VAR_DATA_TYPE(pColVal->colVal.type)) {
                  varDataSetLen(pRes[k]->buf, pColVal->colVal.value.nData);
                  memcpy(varDataVal(pRes[k]->buf), pColVal->colVal.value.pData, pColVal->colVal.value.nData);
                } else {
                  memcpy(pRes[k]->buf, &pColVal->colVal.value, pr->pSchema->columns[slotId].bytes);
                }
              }

              colDataAppend(pColInfoData, 1, (const char*)pRes[k], false);
            }
          }
        }
      }

      tsdbCacheRelease(lruCache, h);
    }

    if (hasRes) {
      pResBlock->info.rows = 1;
    }

  } else if ((pr->type & CACHESCAN_RETRIEVE_TYPE_ALL) == CACHESCAN_RETRIEVE_TYPE_ALL) {
    for (int32_t i = pr->tableIndex; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = taosArrayGet(pr->pTableList, i);
      code = doExtractCacheRow(pr, lruCache, pKeyInfo->uid, &pRow, &h);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (h == NULL) {
        continue;
      }

      saveOneRow(pRow, pResBlock, pr, slotIds, pRes);
      // TODO reset the pRes

      taosArrayPush(pTableUidList, &pKeyInfo->uid);
      tsdbCacheRelease(lruCache, h);

      pr->tableIndex += 1;
      if (pResBlock->info.rows >= pResBlock->info.capacity) {
        return TSDB_CODE_SUCCESS;
      }
    }
  } else {
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}
