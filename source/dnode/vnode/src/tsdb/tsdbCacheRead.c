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

static int32_t saveOneRowForLastRaw(SLastCol* pColVal, SCacheRowsReader* pReader, const int32_t slotId,
                                    SColumnInfoData* pColInfoData, int32_t numOfRows) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SColVal* pVal = NULL;

  TSDB_CHECK_NULL(pColVal, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pVal = &pColVal->colVal;

  // allNullRow = false;
  if (IS_VAR_DATA_TYPE(pColVal->colVal.value.type)) {
    if (!COL_VAL_IS_VALUE(&pColVal->colVal)) {
      colDataSetNULL(pColInfoData, numOfRows);
    } else {
      TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
      if (IS_STR_DATA_BLOB(pColVal->colVal.value.type)) {
        blobDataSetLen(pReader->transferBuf[slotId], pVal->value.nData);

        memcpy(blobDataVal(pReader->transferBuf[slotId]), pVal->value.pData, pVal->value.nData);
        code = colDataSetVal(pColInfoData, numOfRows, pReader->transferBuf[slotId], false);
        TSDB_CHECK_CODE(code, lino, _end);

      } else {
        varDataSetLen(pReader->transferBuf[slotId], pVal->value.nData);

        memcpy(varDataVal(pReader->transferBuf[slotId]), pVal->value.pData, pVal->value.nData);
        code = colDataSetVal(pColInfoData, numOfRows, pReader->transferBuf[slotId], false);
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
  } else {
    code = colDataSetVal(pColInfoData, numOfRows, VALUE_GET_DATUM(&pVal->value, pColVal->colVal.value.type),
                         !COL_VAL_IS_VALUE(pVal));
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t saveOneRow(SArray* pRow, SSDataBlock* pBlock, SCacheRowsReader* pReader, const int32_t* slotIds,
                          const int32_t* dstSlotIds, void** pRes, const char* idStr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfRows = 0;
  SArray* funcTypeBlockArray = NULL;

  TSDB_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (pReader->numOfCols > 0) {
    TSDB_CHECK_NULL(slotIds, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(dstSlotIds, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(pRes, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  numOfRows = pBlock->info.rows;

  if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST)) {
    uint64_t       ts = TSKEY_MIN;
    SFirstLastRes* p = NULL;
    col_id_t       colId = -1;

    funcTypeBlockArray = taosArrayInit(pReader->numOfCols, sizeof(int32_t));
    TSDB_CHECK_NULL(funcTypeBlockArray, code, lino, _end, terrno);

    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotIds[i]);
      TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);

      int32_t funcType = FUNCTION_TYPE_CACHE_LAST;
      if (pReader->pFuncTypeList != NULL && taosArrayGetSize(pReader->pFuncTypeList) > i) {
        void* pVal = taosArrayGet(pReader->pFuncTypeList, i);
        TSDB_CHECK_NULL(pVal, code, lino, _end, TSDB_CODE_INVALID_PARA);

        funcType = *(int32_t*)pVal;
        pVal = taosArrayGet(pReader->pFuncTypeList, i);
        TSDB_CHECK_NULL(pVal, code, lino, _end, TSDB_CODE_INVALID_PARA);

        void* px = taosArrayInsert(funcTypeBlockArray, dstSlotIds[i], pVal);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);
      }

      if (slotIds[i] == -1) {
        if (FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
          colDataSetNULL(pColInfoData, numOfRows);
          continue;
        }

        code = setFirstLastResColToNull(pColInfoData, numOfRows);
        TSDB_CHECK_CODE(code, lino, _end);
        continue;
      }

      int32_t   slotId = slotIds[i];
      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, i);
      TSDB_CHECK_NULL(pColVal, code, lino, _end, TSDB_CODE_INVALID_PARA);

      colId = pColVal->colVal.cid;
      if (FUNCTION_TYPE_CACHE_LAST_ROW == funcType) {
        code = saveOneRowForLastRaw(pColVal, pReader, slotId, pColInfoData, numOfRows);
        TSDB_CHECK_CODE(code, lino, _end);

        continue;
      }

      p = (SFirstLastRes*)varDataVal(pRes[i]);

      p->ts = pColVal->rowKey.ts;
      ts = p->ts;
      p->isNull = !COL_VAL_IS_VALUE(&pColVal->colVal);
      // allNullRow = p->isNull & allNullRow;
      if (!p->isNull) {
        if (IS_VAR_DATA_TYPE(pColVal->colVal.value.type)) {
          if (IS_STR_DATA_BLOB(pColVal->colVal.value.type)) {
            blobDataSetLen(p->buf, pColVal->colVal.value.nData);

            // Fix null pointer error: check if pData is NULL before memcpy
            if (pColVal->colVal.value.pData != NULL && pColVal->colVal.value.nData > 0) {
              memcpy(blobDataVal(p->buf), pColVal->colVal.value.pData, pColVal->colVal.value.nData);
            }
            p->bytes = pColVal->colVal.value.nData + BLOBSTR_HEADER_SIZE;  // binary needs to plus the header size
          } else {
            varDataSetLen(p->buf, pColVal->colVal.value.nData);

            // Fix null pointer error: check if pData is NULL before memcpy
            if (pColVal->colVal.value.pData != NULL && pColVal->colVal.value.nData > 0) {
              memcpy(varDataVal(p->buf), pColVal->colVal.value.pData, pColVal->colVal.value.nData);
            }
            p->bytes = pColVal->colVal.value.nData + VARSTR_HEADER_SIZE;  // binary needs to plus the header size
          }
        } else {
          memcpy(p->buf, VALUE_GET_DATUM(&pColVal->colVal.value, pColVal->colVal.value.type),
                 pReader->pSchema->columns[slotId].bytes);
          p->bytes = pReader->pSchema->columns[slotId].bytes;
        }
      }

      // pColInfoData->info.bytes includes the VARSTR_HEADER_SIZE, need to subtract it
      p->hasResult = true;
      varDataSetLen(pRes[i], pColInfoData->info.bytes - VARSTR_HEADER_SIZE);
      code = colDataSetVal(pColInfoData, numOfRows, (const char*)pRes[i], false);
      TSDB_CHECK_CODE(code, lino, _end);
    }

    for (int32_t idx = 0; idx < taosArrayGetSize(pBlock->pDataBlock); ++idx) {
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, idx);
      TSDB_CHECK_NULL(pCol, code, lino, _end, TSDB_CODE_INVALID_PARA);

      if (idx < funcTypeBlockArray->size) {
        void* pVal = taosArrayGet(funcTypeBlockArray, idx);
        TSDB_CHECK_NULL(pVal, code, lino, _end, TSDB_CODE_INVALID_PARA);

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
          TSDB_CHECK_CODE(code, lino, _end);
        }
        continue;
      } else if (pReader->numOfCols == 1 && idx != dstSlotIds[0] && (pCol->info.colId == colId || colId == -1)) {
        if (p && !p->isNull) {
          code = colDataSetVal(pCol, numOfRows, p->buf, false);
          TSDB_CHECK_CODE(code, lino, _end);
        } else {
          colDataSetNULL(pCol, numOfRows);
        }
      }
    }

    // pBlock->info.rows += allNullRow ? 0 : 1;
    ++pBlock->info.rows;
  } else if (HASTYPE(pReader->type, CACHESCAN_RETRIEVE_LAST_ROW)) {
    for (int32_t i = 0; i < pReader->numOfCols; ++i) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotIds[i]);
      TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);

      int32_t slotId = slotIds[i];
      if (slotId == -1) {
        colDataSetNULL(pColInfoData, numOfRows);
        continue;
      }

      SLastCol* pColVal = (SLastCol*)taosArrayGet(pRow, i);
      TSDB_CHECK_NULL(pColVal, code, lino, _end, TSDB_CODE_INVALID_PARA);

      code = saveOneRowForLastRaw(pColVal, pReader, slotId, pColInfoData, numOfRows);
      TSDB_CHECK_CODE(code, lino, _end);
    }

    // pBlock->info.rows += allNullRow ? 0 : 1;
    ++pBlock->info.rows;
  } else {
    tsdbError("invalid retrieve type:%d, %s", pReader->type, idStr);
    code = TSDB_CODE_INVALID_PARA;
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (funcTypeBlockArray != NULL) {
    taosArrayDestroy(funcTypeBlockArray);
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
  void**            pRes = NULL;
  SCacheRowsReader* pr = NULL;
  int32_t           pkBufLen = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pResBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pr = pReader;

  pr->pReadSnap = NULL;
  pRow = taosArrayInit(TARRAY_SIZE(pr->pCidList), sizeof(SLastCol));
  TSDB_CHECK_NULL(pRow, code, lino, _end, terrno);

  pRes = taosMemoryCalloc(pr->numOfCols, POINTER_BYTES);
  TSDB_CHECK_NULL(pRes, code, lino, _end, terrno);

  pkBufLen = (pr->rowKey.numOfPKs > 0) ? pr->pkColumn.bytes : 0;
  for (int32_t j = 0; j < pr->numOfCols; ++j) {
    int32_t bytes = (slotIds[j] == -1) ? 1 : pr->pSchema->columns[slotIds[j]].bytes;

    pRes[j] = taosMemoryCalloc(1, sizeof(SFirstLastRes) + bytes + pkBufLen + VARSTR_HEADER_SIZE);
    TSDB_CHECK_NULL(pRes[j], code, lino, _end, terrno);

    SFirstLastRes* p = (SFirstLastRes*)varDataVal(pRes[j]);
    p->ts = INT64_MIN;
  }

  (void)taosThreadMutexLock(&pr->readerMutex);
  code = tsdbTakeReadSnap2((STsdbReader*)pr, tsdbCacheQueryReseek, &pr->pReadSnap, pr->idstr);
  TSDB_CHECK_CODE(code, lino, _end);

  int8_t ltype = (pr->type & CACHESCAN_RETRIEVE_LAST) >> 3;

  STableKeyInfo* pTableList = pr->pTableList;

  // retrieve the only one last row of all tables in the uid list.
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

    int64_t st = taosGetTimestampUs();
    int64_t totalLastTs = INT64_MAX;
    for (int32_t i = 0; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pTableList[i].uid;

      code = tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype);
      if (code == -1) {  // fix the invalid return code
        code = 0;
      }
      TSDB_CHECK_CODE(code, lino, _end);

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
          double cost = (taosGetTimestampUs() - st) / 1000.0;
          if (cost > tsCacheLazyLoadThreshold) {
            // pr->lastTs = totalLastTs;
          }
        }
      }

      taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
    }

    if (hasRes) {
      code = saveOneRow(pLastCols, pResBlock, pr, slotIds, dstSlotIds, pRes, pr->idstr);
      TSDB_CHECK_CODE(code, lino, _end);
    }

    taosArrayDestroyEx(pLastCols, tsdbCacheFreeSLastColItem);
  } else if (HASTYPE(pr->type, CACHESCAN_RETRIEVE_TYPE_ALL)) {
    int32_t i = pr->tableIndex;
    for (; i < pr->numOfTables; ++i) {
      tb_uid_t uid = pTableList[i].uid;

      if ((code = tsdbCacheGetBatch(pr->pTsdb, uid, pRow, pr, ltype)) != 0) {
        if (code == -1) {  // fix the invalid return code
          code = 0;
        }
        TSDB_CHECK_CODE(code, lino, _end);
      }

      if (TARRAY_SIZE(pRow) <= 0 || COL_VAL_IS_NONE(&((SLastCol*)TARRAY_DATA(pRow))[0].colVal)) {
        taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);
        continue;
      }

      code = saveOneRow(pRow, pResBlock, pr, slotIds, dstSlotIds, pRes, pr->idstr);
      TSDB_CHECK_CODE(code, lino, _end);

      taosArrayClearEx(pRow, tsdbCacheFreeSLastColItem);

      void* px = taosArrayPush(pTableUidList, &uid);
      TSDB_CHECK_NULL(px, code, lino, _end, terrno);

      ++pr->tableIndex;
      if (pResBlock->info.rows >= pResBlock->info.capacity) {
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

  if (pRes != NULL) {
    for (int32_t j = 0; j < pr->numOfCols; ++j) {
      taosMemoryFree(pRes[j]);
    }
  }

  taosMemoryFree(pRes);
  taosArrayDestroy(pRow);

  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}
