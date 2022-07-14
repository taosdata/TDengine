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

typedef struct SLastrowReader {
  SVnode*   pVnode;
  STSchema* pSchema;
  uint64_t  uid;
  char**    transferBuf;  // todo remove it soon
  int32_t   numOfCols;
  int32_t   type;
  int32_t   tableIndex;  // currently returned result tables
  SArray*   pTableList;  // table id list
} SLastrowReader;

static void saveOneRow(STSRow* pRow, SSDataBlock* pBlock, SLastrowReader* pReader, const int32_t* slotIds) {
  ASSERT(pReader->numOfCols <= taosArrayGetSize(pBlock->pDataBlock));
  int32_t numOfRows = pBlock->info.rows;

  SColVal colVal = {0};
  for (int32_t i = 0; i < pReader->numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);

    if (slotIds[i] == -1) {
      colDataAppend(pColInfoData, numOfRows, (const char*)&pRow->ts, false);
    } else {
      int32_t slotId = slotIds[i];

      tTSRowGetVal(pRow, pReader->pSchema, slotId, &colVal);

      if (IS_VAR_DATA_TYPE(colVal.type)) {
        if (colVal.isNull || colVal.isNone) {
          colDataAppendNULL(pColInfoData, numOfRows);
        } else {
          varDataSetLen(pReader->transferBuf[slotId], colVal.value.nData);
          memcpy(varDataVal(pReader->transferBuf[slotId]), colVal.value.pData, colVal.value.nData);
          colDataAppend(pColInfoData, numOfRows, pReader->transferBuf[slotId], false);
        }
      } else {
        colDataAppend(pColInfoData, numOfRows, (const char*)&colVal.value, colVal.isNull || colVal.isNone);
      }
    }
  }

  pBlock->info.rows += 1;
}

int32_t tsdbLastRowReaderOpen(void* pVnode, int32_t type, SArray* pTableIdList, int32_t numOfCols, void** pReader) {
  SLastrowReader* p = taosMemoryCalloc(1, sizeof(SLastrowReader));
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
  p->pSchema = metaGetTbTSchema(p->pVnode->pMeta, pKeyInfo->uid, -1);
  p->pTableList = pTableIdList;

  p->transferBuf = taosMemoryCalloc(p->pSchema->numOfCols, POINTER_BYTES);
  for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE(p->pSchema->columns[i].type)) {
      p->transferBuf[i] = taosMemoryMalloc(p->pSchema->columns[i].bytes);
    }
  }

  *pReader = p;
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbLastrowReaderClose(void* pReader) {
  SLastrowReader* p = pReader;

  if (p->pSchema != NULL) {
    for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
      taosMemoryFreeClear(p->transferBuf[i]);
    }

    taosMemoryFree(p->transferBuf);
    taosMemoryFree(p->pSchema);
  }

  taosMemoryFree(pReader);
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbRetrieveLastRow(void* pReader, SSDataBlock* pResBlock, const int32_t* slotIds, SArray* pTableUidList) {
  if (pReader == NULL || pResBlock == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SLastrowReader* pr = pReader;

  SLRUCache* lruCache = pr->pVnode->pTsdb->lruCache;
  LRUHandle* h = NULL;
  STSRow*    pRow = NULL;
  size_t     numOfTables = taosArrayGetSize(pr->pTableList);

  // retrieve the only one last row of all tables in the uid list.
  if (pr->type == LASTROW_RETRIEVE_TYPE_SINGLE) {
    int64_t lastKey = INT64_MIN;
    bool    internalResult = false;
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = taosArrayGet(pr->pTableList, i);

      int32_t code = tsdbCacheGetLastrowH(lruCache, pKeyInfo->uid, pr->pVnode->pTsdb, &h);
      // int32_t code = tsdbCacheGetLastH(lruCache, pKeyInfo->uid, pr->pVnode->pTsdb, &h);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (h == NULL) {
        continue;
      }

      pRow = (STSRow*)taosLRUCacheValue(lruCache, h);
      // SArray* pLast = (SArray*)taosLRUCacheValue(lruCache, h);
      // tsdbCacheLastArray2Row(pLast, &pRow, pr->pSchema);
      if (pRow->ts > lastKey) {
        // Set result row into the same rowIndex repeatly, so we need to check if the internal result row has already
        // appended or not.
        if (internalResult) {
          pResBlock->info.rows -= 1;
          taosArrayClear(pTableUidList);
        }

        saveOneRow(pRow, pResBlock, pr, slotIds);
        taosArrayPush(pTableUidList, &pKeyInfo->uid);
        internalResult = true;
        lastKey = pRow->ts;
      }

      tsdbCacheRelease(lruCache, h);
    }
  } else if (pr->type == LASTROW_RETRIEVE_TYPE_ALL) {
    for (int32_t i = pr->tableIndex; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = taosArrayGet(pr->pTableList, i);

      int32_t code = tsdbCacheGetLastrowH(lruCache, pKeyInfo->uid, pr->pVnode->pTsdb, &h);
      // int32_t code = tsdbCacheGetLastH(lruCache, pKeyInfo->uid, pr->pVnode->pTsdb, &h);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      // no data in the table of Uid
      if (h == NULL) {
        continue;
      }

      pRow = (STSRow*)taosLRUCacheValue(lruCache, h);
      // SArray* pLast = (SArray*)taosLRUCacheValue(lruCache, h);
      // tsdbCacheLastArray2Row(pLast, &pRow, pr->pSchema);

      saveOneRow(pRow, pResBlock, pr, slotIds);
      taosArrayPush(pTableUidList, &pKeyInfo->uid);

      // taosMemoryFree(pRow);
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
