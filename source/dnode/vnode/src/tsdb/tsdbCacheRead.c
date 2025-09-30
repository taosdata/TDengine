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
#include "functionResInfo.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbReadUtil.h"

#define HASTYPE(_type, _t) (((_type) & (_t)) == (_t))

static int32_t setFirstLastResColToNull(SColumnInfoData* pCol, int32_t row) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  char*          buf = NULL;
  SFirstLastRes* pRes = NULL;

  TSDB_CHECK_NULL(pCol, code, lino, _end, TSDB_CODE_INVALID_PARA);

  buf = taosMemoryCalloc(1, pCol->info.bytes);
  TSDB_CHECK_NULL(buf, code, lino, _end, terrno);

  pRes = (SFirstLastRes*)((char*)buf + VARSTR_HEADER_SIZE);
  pRes->bytes = 0;
  pRes->hasResult = true;
  pRes->isNull = true;
  varDataSetLen(buf, pCol->info.bytes - VARSTR_HEADER_SIZE);
  code = colDataSetVal(pCol, row, buf, false);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (buf != NULL) {
    taosMemoryFreeClear(buf);
  }
  return code;
}

typedef struct SLastColWithDstSlotId {
  SLastCol *col;
  int32_t  dstSlotId;
} SLastColWithDstSlotId;

int32_t tCompareLastColWithDstSlotId(const void* a, const void* b) {
  const SLastColWithDstSlotId* colA = (const SLastColWithDstSlotId*)a;
  const SLastColWithDstSlotId* colB = (const SLastColWithDstSlotId*)b;
  return -1 * tRowKeyCompare(&colA->col->rowKey, &colB->col->rowKey);
}

static int32_t saveMultiRows(SArray* pRow, SSDataBlock* pResBlock, 
  const int32_t* dstSlotIds, const char* idStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t* nextRowIndex = NULL;
  SArray* pRowWithDstSlotId = NULL;

  TSDB_CHECK_NULL(pResBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (pRow->size > 0) {
    TSDB_CHECK_NULL(dstSlotIds, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  pRowWithDstSlotId = taosArrayInit(pRow->size, sizeof(SLastColWithDstSlotId));
  TSDB_CHECK_NULL(pRowWithDstSlotId, code, lino, _end, terrno);

  // sort pRow by ts and pk in descending order
  for (int32_t i = 0; i < pRow->size; ++i) {
    SLastColWithDstSlotId colWithDstSlotId = {0};
    colWithDstSlotId.col = taosArrayGet(pRow, i);
    colWithDstSlotId.dstSlotId = dstSlotIds[i];
    if (taosArrayPush(pRowWithDstSlotId, &colWithDstSlotId) == NULL) {
      code = terrno;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }
  taosArraySort(pRowWithDstSlotId, tCompareLastColWithDstSlotId);

  int32_t blockSize = taosArrayGetSize(pResBlock->pDataBlock);
  // row index of next row to fill value
  nextRowIndex = taosMemCalloc(blockSize, sizeof(int32_t));
  if (NULL == nextRowIndex) {
    code = terrno;
    TSDB_CHECK_CODE(code, lino, _end);
  }
  for (int32_t i = 0; i < blockSize; ++i) {
    nextRowIndex[i] = pResBlock->info.rows;
  }

  // assign sorted result to res datablock
  int64_t minTs = TSKEY_MAX;
  for (int32_t i = 0; i < pRowWithDstSlotId->size; ++i) {
    SLastColWithDstSlotId* pColWithDstSlotId = (SLastColWithDstSlotId*)taosArrayGet(pRowWithDstSlotId, i);
    SLastCol* pCol = pColWithDstSlotId->col;
    int32_t   dstSlotId = pColWithDstSlotId->dstSlotId;
    int64_t   colTs = pCol->rowKey.ts;
    if (TSKEY_MIN == colTs || COL_VAL_IS_NONE(&pCol->colVal)) {
      // if colTs equals to TSKEY_MIN, col value is null
      // mainly because this col has no cache or cache is invalid
      continue;
    }

    // update row index(row number) and minimal ts
    if (colTs < minTs) {
      // find smaller timestamp, which means a new row appears
      minTs = colTs;
      code = blockDataEnsureCapacity(pResBlock, pResBlock->info.rows+1);
      TSDB_CHECK_CODE(code, lino, _end);
      pResBlock->info.rows += 1;
    }
    int32_t rowIndex = pResBlock->info.rows - 1; // row index of this new row
    for (int32_t j = 0; j < blockSize; ++j) {
      SColumnInfoData* pDstCol = (SColumnInfoData*)taosArrayGet(pResBlock->pDataBlock, j);
      TSDB_CHECK_NULL(pDstCol, code, lino, _end, TSDB_CODE_INVALID_PARA);

      if (pDstCol->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID && 
        pDstCol->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
        // update timestamp column
        code = colDataSetVal(pDstCol, rowIndex, (const char*)&colTs, false);
        TSDB_CHECK_CODE(code, lino, _end);
        nextRowIndex[j] = rowIndex + 1;
      } else if (j == dstSlotId) {
        // update destinate column
        // 1. set previous rows to null
        for (int32_t prevRowIndex = nextRowIndex[j]; prevRowIndex < rowIndex; ++prevRowIndex) {
          colDataSetNULL(pDstCol, prevRowIndex);
          pDstCol->hasNull = true;
        }
        // 2. set current row to this value
        SColVal* pColVal = &pCol->colVal;
        if (IS_VAR_DATA_TYPE(pColVal->value.type)) {
          if (COL_VAL_IS_NULL(pColVal)) {
            colDataSetNULL(pDstCol, rowIndex);
          } else {
            char* tmp = taosMemCalloc(1, VARSTR_HEADER_SIZE + pColVal->value.nData);
            TSDB_CHECK_NULL(tmp, code, lino, _end, terrno);
            varDataSetLen(tmp, pColVal->value.nData);
            TAOS_UNUSED(memcpy(varDataVal(tmp), pColVal->value.pData, pColVal->value.nData));
            code = colDataSetVal(pDstCol, rowIndex, (const char*)tmp, false);
            TSDB_CHECK_CODE(code, lino, _end);
            taosMemFreeClear(tmp);
          }
        } else {
          code = colDataSetVal(pDstCol, rowIndex,
            VALUE_GET_DATUM(&pColVal->value, pColVal->value.type), COL_VAL_IS_NULL(pColVal));
          TSDB_CHECK_CODE(code, lino, _end);
        }
        // 3. update nextRowIndex
        nextRowIndex[j] = rowIndex + 1;
      } else {
        // do nothing for other columns
      }
    }
  }

  // filling res datablock with NULL
  for (int32_t j = 0; j < blockSize; ++j) {
    SColumnInfoData* pDstCol = (SColumnInfoData*)taosArrayGet(pResBlock->pDataBlock, j);
    for (int32_t prevRowIndex = nextRowIndex[j]; prevRowIndex < pResBlock->info.rows; ++prevRowIndex) {
      colDataSetNULL(pDstCol, prevRowIndex);
      pDstCol->hasNull = true;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s, %s", __func__, lino, tstrerror(code), idStr);
  }

  if (nextRowIndex != NULL) {
    taosMemFreeClear(nextRowIndex);
  }
  if (NULL != pRowWithDstSlotId) {
    taosArrayDestroy(pRowWithDstSlotId);
  }
  return code;
}

static int32_t setTableSchema(SCacheRowsReader* p, uint64_t suid, const char* idstr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfTables = 0;

  TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);

  numOfTables = p->numOfTables;

  if (suid != 0) {
    code = metaGetTbTSchemaNotNull(p->pVnode->pMeta, suid, -1, 1, &p->pSchema);
    if (TSDB_CODE_SUCCESS != code) {
      tsdbWarn("stable:%" PRIu64 " has been dropped, failed to retrieve cached rows, %s", suid, idstr);
      if (code == TSDB_CODE_NOT_FOUND) {
        code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
      }
      TSDB_CHECK_CODE(code, lino, _end);
    }
  } else {
    for (int32_t i = 0; i < numOfTables; ++i) {
      uint64_t uid = p->pTableList[i].uid;
      code = metaGetTbTSchemaMaybeNull(p->pVnode->pMeta, uid, -1, 1, &p->pSchema);
      TSDB_CHECK_CODE(code, lino, _end);
      if (p->pSchema != NULL) {
        break;
      }

      tsdbWarn("table:%" PRIu64 " has been dropped, failed to retrieve cached rows, %s", uid, idstr);
    }

    // all queried tables have been dropped already, return immediately.
    if (p->pSchema == NULL) {
      tsdbWarn("all queried tables has been dropped, try next group, %s", idstr);
      code = TSDB_CODE_PAR_TABLE_NOT_EXIST;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbReuseCacherowsReader(void* reader, void* pTableIdList, int32_t numOfTables) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SCacheRowsReader* pReader = (SCacheRowsReader*)reader;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pReader->pTableList = pTableIdList;
  pReader->numOfTables = numOfTables;
  pReader->lastTs = INT64_MIN;
  destroySttBlockReader(pReader->pLDataIterArray, NULL);
  pReader->pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
  TSDB_CHECK_NULL(pReader->pLDataIterArray, code, lino, _end, terrno);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbCacherowsReaderOpen(void* pVnode, int32_t type, void* pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                SArray* pCidList, int32_t* pSlotIds, uint64_t suid, void** pReader, const char* idstr,
                                SArray* pFuncTypeList, SColumnInfo* pPkCol, int32_t numOfPks) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  SCacheRowsReader* p = NULL;

  TSDB_CHECK_NULL(pVnode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pReader = NULL;
  p = taosMemoryCalloc(1, sizeof(SCacheRowsReader));
  TSDB_CHECK_NULL(p, code, lino, _end, terrno);

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
    TSDB_CHECK_NULL(pPkCol, code, lino, _end, TSDB_CODE_INVALID_PARA);
    p->rowKey.pks[0].type = pPkCol->type;
    if (IS_VAR_DATA_TYPE(pPkCol->type)) {
      p->rowKey.pks[0].pData = taosMemoryCalloc(1, pPkCol->bytes);
      if (p->rowKey.pks[0].pData == NULL) {
        taosMemoryFreeClear(p);
        code = terrno;
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }

    p->pkColumn = *pPkCol;
  }

  if (numOfTables == 0) {
    *pReader = p;
    p = NULL;
    goto _end;
  }

  p->pTableList = pTableIdList;
  p->numOfTables = numOfTables;

  code = setTableSchema(p, suid, idstr);
  TSDB_CHECK_CODE(code, lino, _end);

  p->transferBuf = taosMemoryCalloc(p->pSchema->numOfCols, POINTER_BYTES);
  TSDB_CHECK_NULL(p->transferBuf, code, lino, _end, terrno);

  for (int32_t i = 0; i < p->pSchema->numOfCols; ++i) {
    if (IS_VAR_DATA_TYPE(p->pSchema->columns[i].type)) {
      p->transferBuf[i] = taosMemoryMalloc(p->pSchema->columns[i].bytes);
      TSDB_CHECK_NULL(p->transferBuf[i], code, lino, _end, terrno);
    }
  }

  if (idstr != NULL) {
    p->idstr = taosStrdup(idstr);
    TSDB_CHECK_NULL(p->idstr, code, lino, _end, terrno);
  }
  code = taosThreadMutexInit(&p->readerMutex, NULL);
  TSDB_CHECK_CODE(code, lino, _end);

  p->lastTs = INT64_MIN;

  *pReader = p;
  p = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    *pReader = NULL;
  }
  if (p != NULL) {
    tsdbCacherowsReaderClose(p);
  }
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
    p->pLDataIterArray = NULL;
  }

  if (p->pFileReader) {
    tsdbDataFileReaderClose(&p->pFileReader);
    p->pFileReader = NULL;
  }

  taosMemoryFree((void*)p->idstr);
  (void)taosThreadMutexDestroy(&p->readerMutex);

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
                              SArray* pTableUidList, bool* pGotAll) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  bool              hasRes = false;
  SArray*           pRow = NULL;
  SCacheRowsReader* pr = NULL;
  int32_t           pkBufLen = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pResBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pr = pReader;

  pr->pReadSnap = NULL;
  pRow = taosArrayInit(TARRAY_SIZE(pr->pCidList), sizeof(SLastCol));
  TSDB_CHECK_NULL(pRow, code, lino, _end, terrno);

  (void)taosThreadMutexLock(&pr->readerMutex);
  code = tsdbTakeReadSnap2((STsdbReader*)pr, tsdbCacheQueryReseek, &pr->pReadSnap, pr->idstr);
  TSDB_CHECK_CODE(code, lino, _end);

  int8_t ltype = (pr->type & CACHESCAN_RETRIEVE_LAST) >> 3;

  STableKeyInfo* pTableList = pr->pTableList;

  // retrieve combined last/last_row data of all tables in the uid list.
  if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_SINGLE)) {
    SArray* pLastCols = taosArrayInit(pr->numOfCols, sizeof(SLastCol));
    TSDB_CHECK_NULL(pLastCols, code, lino, _end, terrno);

    for (int32_t i = 0; i < pr->numOfCols; ++i) {
      int32_t slotId = slotIds[i];
      if (slotId == -1) {
        SLastCol p = {.rowKey.ts = INT64_MIN, .colVal.value.type = TSDB_DATA_TYPE_BOOL, .colVal.flag = CV_FLAG_NULL};
        void*    px = taosArrayPush(pLastCols, &p);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);
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
            TSDB_CHECK_NULL(p.rowKey.pks[j].pData, code, lino, _end, terrno);
          }
        }
      }

      if (IS_VAR_DATA_TYPE(pCol->type) || pCol->type == TSDB_DATA_TYPE_DECIMAL) {
        p.colVal.value.pData = taosMemoryCalloc(pCol->bytes, sizeof(char));
        TSDB_CHECK_NULL(p.colVal.value.pData, code, lino, _end, terrno);
      }

      void* px = taosArrayPush(pLastCols, &p);
      TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    }

    int64_t totalLastTs = INT64_MAX;
    for (int32_t i = 0; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pTableList[i].uid;

      code = tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype);
      if (code == -1) {  // fix the invalid return code
        code = 0;
      }
      TSDB_CHECK_CODE(code, lino, _end);

      if (TARRAY_SIZE(pRow) <= 0) {
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
                valueCloneDatum(p->rowKey.pks + j, pColVal->rowKey.pks + j, p->rowKey.pks[j].type);
              }
            }

            if (k == 0) {
              if (TARRAY_SIZE(pTableUidList) == 0) {
                void* px = taosArrayPush(pTableUidList, &uid);
                TSDB_CHECK_NULL(px, code, lino, _end, terrno);
              } else {
                taosArraySet(pTableUidList, 0, &uid);
              }
            }

            if (pColVal->rowKey.ts < singleTableLastTs && HASTYPE(pr->type, CACHESCAN_RETRIEVE_LAST)) {
              singleTableLastTs = pColVal->rowKey.ts;
            }

            if (p->colVal.value.type != pColVal->colVal.value.type) {
              // check for type/cid mismatch
              tsdbError("last cache type mismatch, uid:%" PRIu64
                        ", schema-type:%d, slotId:%d, cache-type:%d, cache-col:%d",
                        uid, p->colVal.value.type, slotIds[k], pColVal->colVal.value.type, pColVal->colVal.cid);
              taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
              code = TSDB_CODE_INVALID_PARA;
              goto _end;
            }

            if (!IS_VAR_DATA_TYPE(pColVal->colVal.value.type) && pColVal->colVal.value.type != TSDB_DATA_TYPE_DECIMAL) {
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
        }
      }

      taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
    }

    if (hasRes) {
      code = saveMultiRows(pLastCols, pResBlock, dstSlotIds, pr->idstr);
      TSDB_CHECK_CODE(code, lino, _end);
    }

    taosArrayDestroyEx(pLastCols, tsdbCacheFreeSLastColItem);
  } else if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_ALL)) {
    // retrieve data for each table
    int32_t i = pr->tableIndex;
    for (; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pTableList[i].uid;

      if ((code = tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype)) != 0) {
        if (code == -1) {  // fix the invalid return code
          code = 0;
        }
        TSDB_CHECK_CODE(code, lino, _end);
      }

      if (TARRAY_SIZE(pRow) <= 0) {
        taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
        continue;
      }

      code = saveMultiRows(pRow, pResBlock, dstSlotIds, pr->idstr);
      TSDB_CHECK_CODE(code, lino, _end);

      taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
      ++pr->tableIndex;
      if (pResBlock->info.rows > 0) {
        void* px = taosArrayPush(pTableUidList, &uid);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);
        break;
      }
    }

    if (pGotAll && i == pr->numOfTables) {
      *pGotAll = true;
    }
  } else {
    code = TSDB_CODE_INVALID_PARA;
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  tsdbUntakeReadSnap2((STsdbReader*)pr, pr->pReadSnap, true);
  pr->pReadSnap = NULL;

  if (pr->pCurFileSet) {
    pr->pCurFileSet = NULL;
  }

  (void)taosThreadMutexUnlock(&pr->readerMutex);

  taosArrayDestroy(pRow);

  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
