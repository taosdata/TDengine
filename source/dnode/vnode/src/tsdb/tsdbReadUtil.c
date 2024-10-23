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

#include "tsdbReadUtil.h"
#include "tsdb.h"
#include "tsdbDataFileRW.h"
#include "tsdbFS2.h"
#include "tsdbMerge.h"
#include "tsdbUtil2.h"
#include "tsimplehash.h"

static bool overlapWithDelSkylineWithoutVer(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord,
                                            int32_t order);

static int32_t initBlockScanInfoBuf(SBlockInfoBuf* pBuf, int32_t numOfTables) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  int32_t              num = 0;
  int32_t              remainder = 0;
  STableBlockScanInfo* p = NULL;
  const void*          px = NULL;

  TSDB_CHECK_CONDITION(pBuf && pBuf->numPerBucket > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfTables >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  num = numOfTables / pBuf->numPerBucket;
  remainder = numOfTables % pBuf->numPerBucket;

  if (pBuf->pData == NULL) {
    pBuf->pData = taosArrayInit(num + 1, POINTER_BYTES);
    TSDB_CHECK_NULL(pBuf->pData, code, lino, _end, terrno);
  }

  for (int32_t i = 0; i < num; ++i) {
    p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    TSDB_CHECK_NULL(p, code, lino, _end, terrno);

    px = taosArrayPush(pBuf->pData, &p);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    p = NULL;
  }

  if (remainder > 0) {
    p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    TSDB_CHECK_NULL(p, code, lino, _end, terrno);

    px = taosArrayPush(pBuf->pData, &p);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    p = NULL;
  }

  pBuf->numOfTables = numOfTables;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (p) {
    taosMemoryFreeClear(p);
  }
  return code;
}

int32_t uidComparFunc(const void* p1, const void* p2) {
  uint64_t pu1 = *(const uint64_t*)p1;
  uint64_t pu2 = *(const uint64_t*)p2;
  if (pu1 == pu2) {
    return 0;
  } else {
    return (pu1 < pu2) ? -1 : 1;
  }
}

int32_t ensureBlockScanInfoBuf(SBlockInfoBuf* pBuf, int32_t numOfTables) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  int32_t              num = 0;
  int32_t              remainder = 0;
  STableBlockScanInfo* p = NULL;
  const void*          px = NULL;

  TSDB_CHECK_CONDITION(pBuf && pBuf->numPerBucket > 0 && pBuf->numOfTables >= 0, code, lino, _end,
                       TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfTables >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (numOfTables <= pBuf->numOfTables) {
    return code;
  }

  remainder = pBuf->numOfTables % pBuf->numPerBucket;
  if (remainder > 0) {
    TSDB_CHECK_CONDITION(taosArrayGetSize(pBuf->pData) > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
    p = *(STableBlockScanInfo**)taosArrayPop(pBuf->pData);
    taosMemoryFreeClear(p);
    pBuf->numOfTables -= remainder;
  }

  num = (numOfTables - pBuf->numOfTables) / pBuf->numPerBucket;
  remainder = (numOfTables - pBuf->numOfTables) % pBuf->numPerBucket;

  if (pBuf->pData == NULL) {
    pBuf->pData = taosArrayInit(num + 1, POINTER_BYTES);
    TSDB_CHECK_NULL(pBuf->pData, code, lino, _end, terrno);
  }

  for (int32_t i = 0; i < num; ++i) {
    p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    TSDB_CHECK_NULL(p, code, lino, _end, terrno);

    px = taosArrayPush(pBuf->pData, &p);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    p = NULL;
  }

  if (remainder > 0) {
    p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    TSDB_CHECK_NULL(p, code, lino, _end, terrno);

    px = taosArrayPush(pBuf->pData, &p);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    p = NULL;
  }

  pBuf->numOfTables = numOfTables;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (p) {
    taosMemoryFreeClear(p);
  }
  return code;
}

void clearBlockScanInfoBuf(SBlockInfoBuf* pBuf) {
  if (pBuf == NULL) return;
  if (pBuf->pData != NULL) {
    taosArrayDestroyP(pBuf->pData, (FDelete)taosMemoryFree);
    pBuf->pData = NULL;
  }
}

int32_t getPosInBlockInfoBuf(const SBlockInfoBuf* pBuf, int32_t index, STableBlockScanInfo** pInfo) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  int32_t               bucketIndex = 0;
  STableBlockScanInfo** pBucket = NULL;

  TSDB_CHECK_CONDITION(pBuf && pBuf->numPerBucket > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(index >= 0 && index < pBuf->numOfTables, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pInfo = NULL;

  bucketIndex = index / pBuf->numPerBucket;
  pBucket = taosArrayGet(pBuf->pData, bucketIndex);
  TSDB_CHECK_NULL(pBucket, code, lino, _end, terrno);

  *pInfo = (*pBucket) + (index % pBuf->numPerBucket);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getTableBlockScanInfo(SSHashObj* pTableMap, uint64_t uid, STableBlockScanInfo** pInfo, const char* id) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  STableBlockScanInfo** pVal = NULL;

  TSDB_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(id, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pVal = (STableBlockScanInfo**)tSimpleHashGet(pTableMap, &uid, sizeof(uid));
  if (pVal == NULL) {
    int32_t size = tSimpleHashGetSize(pTableMap);
    tsdbError("failed to locate the uid:%" PRIu64 " in query table uid list, total tables:%d, %s", uid, size, id);
    code = TSDB_CODE_INVALID_PARA;
    TSDB_CHECK_CODE(code, lino, _end);
  }
  *pInfo = *pVal;
  TSDB_CHECK_NULL(*pInfo, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t initRowKey(SRowKey* pKey, int64_t ts, int32_t numOfPks, int32_t type, int32_t len, bool asc) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfPks == 0 || numOfPks == 1, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pKey->numOfPKs = (uint8_t)numOfPks;
  pKey->ts = ts;

  if (numOfPks > 0) {
    pKey->pks[0].type = (uint8_t)type;

    if (IS_NUMERIC_TYPE(type)) {
      if (asc) {
        switch (type) {
          case TSDB_DATA_TYPE_BIGINT: {
            pKey->pks[0].val = INT64_MIN;
            break;
          }
          case TSDB_DATA_TYPE_INT: {
            int32_t min = INT32_MIN;
            (void)memcpy(&pKey->pks[0].val, &min, tDataTypes[type].bytes);
            break;
          }
          case TSDB_DATA_TYPE_SMALLINT: {
            int16_t min = INT16_MIN;
            (void)memcpy(&pKey->pks[0].val, &min, tDataTypes[type].bytes);
            break;
          }
          case TSDB_DATA_TYPE_TINYINT: {
            int8_t min = INT8_MIN;
            (void)memcpy(&pKey->pks[0].val, &min, tDataTypes[type].bytes);
            break;
          }
          case TSDB_DATA_TYPE_UTINYINT:
          case TSDB_DATA_TYPE_USMALLINT:
          case TSDB_DATA_TYPE_UINT:
          case TSDB_DATA_TYPE_UBIGINT: {
            pKey->pks[0].val = 0;
            break;
          }
          default:
            code = TSDB_CODE_INVALID_PARA;
            TSDB_CHECK_CODE(code, lino, _end);
        }
      } else {
        switch (type) {
          case TSDB_DATA_TYPE_BIGINT:
            pKey->pks[0].val = INT64_MAX;
            break;
          case TSDB_DATA_TYPE_INT:
            pKey->pks[0].val = INT32_MAX;
            break;
          case TSDB_DATA_TYPE_SMALLINT:
            pKey->pks[0].val = INT16_MAX;
            break;
          case TSDB_DATA_TYPE_TINYINT:
            pKey->pks[0].val = INT8_MAX;
            break;
          case TSDB_DATA_TYPE_UBIGINT:
            pKey->pks[0].val = UINT64_MAX;
            break;
          case TSDB_DATA_TYPE_UINT:
            pKey->pks[0].val = UINT32_MAX;
            break;
          case TSDB_DATA_TYPE_USMALLINT:
            pKey->pks[0].val = UINT16_MAX;
            break;
          case TSDB_DATA_TYPE_UTINYINT:
            pKey->pks[0].val = UINT8_MAX;
            break;
          default:
            code = TSDB_CODE_INVALID_PARA;
            TSDB_CHECK_CODE(code, lino, _end);
        }
      }
    } else {
      TSDB_CHECK_CONDITION(IS_VAR_DATA_TYPE(type), code, lino, _end, TSDB_CODE_INVALID_PARA);
      pKey->pks[0].nData = 0;
      pKey->pks[0].pData = taosMemoryCalloc(1, len);
      TSDB_CHECK_NULL(pKey->pks[0].pData, code, lino, _end, terrno);

      if (!asc) {
        pKey->numOfPKs = 2;
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void clearRowKey(SRowKey* pKey) {
  if (pKey == NULL || pKey->numOfPKs == 0 || (IS_NUMERIC_TYPE(pKey->pks[0].type))) {
    return;
  }
  taosMemoryFreeClear(pKey->pks[0].pData);
}

static int32_t initLastProcKey(STableBlockScanInfo* pScanInfo, const STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfPks = 0;
  int32_t type = 0;
  int32_t bytes = 0;
  int64_t ts = 0;
  int64_t nextTs = 0;
  bool    asc = false;

  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  numOfPks = pReader->suppInfo.numOfPks;
  type = pReader->suppInfo.pk.type;
  bytes = pReader->suppInfo.pk.bytes;
  asc = ASCENDING_TRAVERSE(pReader->info.order);

  if (asc) {
    nextTs = pReader->info.window.skey;
    ts = (nextTs > INT64_MIN) ? (nextTs - 1) : nextTs;
  } else {
    nextTs = pReader->info.window.ekey;
    ts = (nextTs < INT64_MAX) ? (nextTs + 1) : nextTs;
  }

  code = initRowKey(&pScanInfo->lastProcKey, ts, numOfPks, type, bytes, asc);
  TSDB_CHECK_CODE(code, lino, _end);

  code = initRowKey(&pScanInfo->sttKeyInfo.nextProcKey, nextTs, numOfPks, type, bytes, asc);
  TSDB_CHECK_CODE(code, lino, _end);

  code = initRowKey(&pScanInfo->sttRange.skey, INT64_MAX, numOfPks, type, bytes, asc);
  TSDB_CHECK_CODE(code, lino, _end);

  code = initRowKey(&pScanInfo->sttRange.ekey, INT64_MIN, numOfPks, type, bytes, asc);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t initTableBlockScanInfo(STableBlockScanInfo* pScanInfo, uint64_t uid, SSHashObj* pTableMap,
                               STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pScanInfo->uid = uid;
  INIT_KEYRANGE(&pScanInfo->sttRange);
  INIT_TIMEWINDOW(&pScanInfo->filesetWindow);

  pScanInfo->cleanSttBlocks = false;
  pScanInfo->sttBlockReturned = false;

  code = initLastProcKey(pScanInfo, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  pScanInfo->sttKeyInfo.status = STT_FILE_READER_UNINIT;
  code = tSimpleHashPut(pTableMap, &pScanInfo->uid, sizeof(uint64_t), &pScanInfo, POINTER_BYTES);
  TSDB_CHECK_CODE(code, lino, _end);

  tsdbTrace("%p check table uid:%" PRId64 " from lastKey:%" PRId64 " %s", pReader, pScanInfo->uid,
            pScanInfo->lastProcKey.ts, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// NOTE: speedup the whole processing by preparing the buffer for STableBlockScanInfo in batch model
int32_t createDataBlockScanInfo(STsdbReader* pTsdbReader, SBlockInfoBuf* pBuf, const STableKeyInfo* idList,
                                STableUidList* pUidList, int32_t numOfTables, SSHashObj** pHashObj) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  SSHashObj* pTableMap = NULL;
  int64_t    st = 0;

  TSDB_CHECK_NULL(pTsdbReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION((idList != NULL) || (numOfTables == 0), code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pUidList, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfTables >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pHashObj, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pHashObj = NULL;

  // allocate buffer in order to load data blocks from file
  // todo use simple hash instead, optimize the memory consumption
  pTableMap = tSimpleHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  TSDB_CHECK_NULL(pTableMap, code, lino, _end, terrno);

  st = taosGetTimestampUs();
  code = initBlockScanInfoBuf(pBuf, numOfTables);
  TSDB_CHECK_CODE(code, lino, _end);

  pUidList->tableUidList = taosMemoryMalloc(numOfTables * sizeof(uint64_t));
  TSDB_CHECK_NULL(pUidList->tableUidList, code, lino, _end, terrno);

  pUidList->currentIndex = 0;

  for (int32_t j = 0; j < numOfTables; ++j) {
    pUidList->tableUidList[j] = idList[j].uid;

    STableBlockScanInfo* pScanInfo = NULL;
    code = getPosInBlockInfoBuf(pBuf, j, &pScanInfo);
    TSDB_CHECK_CODE(code, lino, _end);

    code = initTableBlockScanInfo(pScanInfo, idList[j].uid, pTableMap, pTsdbReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  taosSort(pUidList->tableUidList, numOfTables, sizeof(uint64_t), uidComparFunc);

  pTsdbReader->cost.createScanInfoList = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("%p create %d tables scan-info, size:%.2f Kb, elapsed time:%.2f ms, %s", pTsdbReader, numOfTables,
            (sizeof(STableBlockScanInfo) * numOfTables) / 1024.0, pTsdbReader->cost.createScanInfoList,
            pTsdbReader->idStr);

  *pHashObj = pTableMap;
  pTableMap = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pTableMap) {
    destroyAllBlockScanInfo(&pTableMap);
  }
  return code;
}

void resetAllDataBlockScanInfo(const SSHashObj* pTableMap, int64_t ts, int32_t step) {
  STableBlockScanInfo** p = NULL;
  int32_t               iter = 0;

  while ((p = tSimpleHashIterate(pTableMap, p, &iter)) != NULL) {
    STableBlockScanInfo* pInfo = *p;
    if (pInfo == NULL) {
      continue;
    }

    pInfo->iterInit = false;
    pInfo->iter.hasVal = false;
    pInfo->iiter.hasVal = false;

    if (pInfo->iter.iter != NULL) {
      pInfo->iter.iter = tsdbTbDataIterDestroy(pInfo->iter.iter);
    }

    if (pInfo->iiter.iter != NULL) {
      pInfo->iiter.iter = tsdbTbDataIterDestroy(pInfo->iiter.iter);
    }

    taosArrayDestroy(pInfo->delSkyline);
    pInfo->delSkyline = NULL;
    pInfo->lastProcKey.ts = ts;
    // todo check the nextProcKey info
    pInfo->sttKeyInfo.nextProcKey.ts = ts + step;
  }
}

void clearBlockScanInfo(STableBlockScanInfo* p) {
  if (p == NULL) {
    return;
  }

  p->iterInit = false;
  p->iter.hasVal = false;
  p->iiter.hasVal = false;
  p->sttKeyInfo.status = STT_FILE_READER_UNINIT;

  if (p->iter.iter != NULL) {
    p->iter.iter = tsdbTbDataIterDestroy(p->iter.iter);
  }

  if (p->iiter.iter != NULL) {
    p->iiter.iter = tsdbTbDataIterDestroy(p->iiter.iter);
  }

  taosArrayDestroy(p->delSkyline);
  p->delSkyline = NULL;
  taosArrayDestroy(p->pBlockList);
  p->pBlockList = NULL;
  taosArrayDestroy(p->pBlockIdxList);
  p->pBlockIdxList = NULL;
  taosArrayDestroy(p->pMemDelData);
  p->pMemDelData = NULL;
  taosArrayDestroy(p->pFileDelData);
  p->pFileDelData = NULL;

  clearRowKey(&p->lastProcKey);
  clearRowKey(&p->sttRange.skey);
  clearRowKey(&p->sttRange.ekey);
  clearRowKey(&p->sttKeyInfo.nextProcKey);
}

void destroyAllBlockScanInfo(SSHashObj** pTableMap) {
  STableBlockScanInfo** p = NULL;
  int32_t               iter = 0;

  if (pTableMap == NULL || *pTableMap == NULL) {
    return;
  }

  while ((p = tSimpleHashIterate(*pTableMap, p, &iter)) != NULL) {
    clearBlockScanInfo(*p);
  }

  tSimpleHashCleanup(*pTableMap);
  *pTableMap = NULL;
}

static void doCleanupInfoForNextFileset(STableBlockScanInfo* pScanInfo) {
  if (pScanInfo == NULL) {
    return;
  }
  // reset the index in last block when handing a new file
  taosArrayClear(pScanInfo->pBlockList);
  taosArrayClear(pScanInfo->pBlockIdxList);
  taosArrayClear(pScanInfo->pFileDelData);  // del data from each file set
  pScanInfo->cleanSttBlocks = false;
  pScanInfo->numOfRowsInStt = 0;
  pScanInfo->sttBlockReturned = false;
  INIT_KEYRANGE(&pScanInfo->sttRange);
  INIT_TIMEWINDOW(&pScanInfo->filesetWindow);
  pScanInfo->sttKeyInfo.status = STT_FILE_READER_UNINIT;
}

void cleanupInfoForNextFileset(const SSHashObj* pTableMap) {
  STableBlockScanInfo** p = NULL;
  int32_t               iter = 0;

  while ((p = tSimpleHashIterate(pTableMap, p, &iter)) != NULL) {
    doCleanupInfoForNextFileset(*p);
  }
}

// brin records iterator
void initBrinRecordIter(SBrinRecordIter* pIter, SDataFileReader* pReader, SArray* pList) {
  if (pIter == NULL) {
    return;
  }

  pIter->blockIndex = -1;
  pIter->recordIndex = -1;

  pIter->pReader = pReader;
  pIter->pBrinBlockList = pList;
}

int32_t getNextBrinRecord(SBrinRecordIter* pIter, SBrinRecord** pRecord) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRecord, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pRecord = NULL;

  if (pIter->blockIndex == -1 || (pIter->recordIndex + 1) >= pIter->block.numOfRecords) {
    pIter->blockIndex += 1;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pBrinBlockList)) {
      goto _end;
    }

    TSDB_CHECK_CONDITION(pIter->blockIndex >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
    pIter->pCurrentBlk = taosArrayGet(pIter->pBrinBlockList, pIter->blockIndex);
    TSDB_CHECK_NULL(pIter->pCurrentBlk, code, lino, _end, terrno);

    tBrinBlockClear(&pIter->block);
    TSDB_CHECK_NULL(pIter->pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
    code = tsdbDataFileReadBrinBlock(pIter->pReader, pIter->pCurrentBlk, &pIter->block);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("failed to read brinBlock from file, code:%s", tstrerror(code));
      TSDB_CHECK_CODE(code, lino, _end);
    }

    pIter->recordIndex = -1;
  }

  pIter->recordIndex += 1;
  code = tBrinBlockGet(&pIter->block, pIter->recordIndex, &pIter->record);
  TSDB_CHECK_CODE(code, lino, _end);
  *pRecord = &pIter->record;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void clearBrinBlockIter(SBrinRecordIter* pIter) {
  if (pIter != NULL) {
    tBrinBlockDestroy(&pIter->block);
  }
}

// initialize the file block access order
//  sort the file blocks according to the offset of each data block in the files
static void cleanupBlockOrderSupporter(SBlockOrderSupporter* pSup) {
  if (pSup == NULL) {
    return;
  }

  taosMemoryFreeClear(pSup->numOfBlocksPerTable);
  taosMemoryFreeClear(pSup->indexPerTable);

  if (pSup->pDataBlockInfo != NULL) {
    for (int32_t i = 0; i < pSup->numOfTables; ++i) {
      SBlockOrderWrapper* pBlockInfo = pSup->pDataBlockInfo[i];
      taosMemoryFreeClear(pBlockInfo);
    }

    taosMemoryFreeClear(pSup->pDataBlockInfo);
  }
}

static int32_t initBlockOrderSupporter(SBlockOrderSupporter* pSup, int32_t numOfTables) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pSup, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfTables >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pSup->pDataBlockInfo = taosMemoryCalloc(numOfTables, POINTER_BYTES);
  TSDB_CHECK_NULL(pSup->pDataBlockInfo, code, lino, _end, terrno);
  pSup->indexPerTable = taosMemoryCalloc(numOfTables, sizeof(int32_t));
  TSDB_CHECK_NULL(pSup->indexPerTable, code, lino, _end, terrno);
  pSup->numOfBlocksPerTable = taosMemoryCalloc(numOfTables, sizeof(int32_t));
  TSDB_CHECK_NULL(pSup->numOfBlocksPerTable, code, lino, _end, terrno);
  pSup->numOfTables = 0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t fileDataBlockOrderCompar(const void* pLeft, const void* pRight, void* param) {
  int32_t                     leftIndex = 0;
  int32_t                     rightIndex = 0;
  int32_t                     leftTableBlockIndex = 0;
  int32_t                     rightTableBlockIndex = 0;
  const SBlockOrderSupporter* pSupporter = NULL;
  const SBlockOrderWrapper*   pLeftBlock = NULL;
  const SBlockOrderWrapper*   pRightBlock = NULL;

  leftIndex = *(const int32_t*)pLeft;
  rightIndex = *(const int32_t*)pRight;
  pSupporter = (const SBlockOrderSupporter*)param;

  leftTableBlockIndex = pSupporter->indexPerTable[leftIndex];
  rightTableBlockIndex = pSupporter->indexPerTable[rightIndex];

  if (leftTableBlockIndex >= pSupporter->numOfBlocksPerTable[leftIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex >= pSupporter->numOfBlocksPerTable[rightIndex]) {
    /* right block is empty */
    return -1;
  }

  pLeftBlock = &pSupporter->pDataBlockInfo[leftIndex][leftTableBlockIndex];
  pRightBlock = &pSupporter->pDataBlockInfo[rightIndex][rightTableBlockIndex];

  return pLeftBlock->offset > pRightBlock->offset ? 1 : -1;
}

int32_t recordToBlockInfo(SFileDataBlockInfo* pBlockInfo, const SBrinRecord* record) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  const SRowKey* pFirstKey = NULL;
  const SRowKey* pLastKey = NULL;

  TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(record, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pBlockInfo->uid = record->uid;
  pBlockInfo->firstKey = record->firstKey.key.ts;
  pBlockInfo->lastKey = record->lastKey.key.ts;
  pBlockInfo->minVer = record->minVer;
  pBlockInfo->maxVer = record->maxVer;
  pBlockInfo->blockOffset = record->blockOffset;
  pBlockInfo->smaOffset = record->smaOffset;
  pBlockInfo->blockSize = record->blockSize;
  pBlockInfo->blockKeySize = record->blockKeySize;
  pBlockInfo->smaSize = record->smaSize;
  pBlockInfo->numRow = record->numRow;
  pBlockInfo->count = record->count;

  pFirstKey = &record->firstKey.key;
  pLastKey = &record->lastKey.key;
  TSDB_CHECK_CONDITION((pFirstKey->numOfPKs == pLastKey->numOfPKs), code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (pFirstKey->numOfPKs > 0) {
    TSDB_CHECK_CONDITION((IS_NUMERIC_TYPE(pFirstKey->pks[0].type) || IS_VAR_DATA_TYPE(pFirstKey->pks[0].type)) &&
                             (pFirstKey->pks[0].type == pLastKey->pks[0].type),
                         code, lino, _end, TSDB_CODE_INVALID_PARA);
    if (IS_NUMERIC_TYPE(pFirstKey->pks[0].type)) {
      pBlockInfo->firstPk.val = pFirstKey->pks[0].val;
      pBlockInfo->lastPk.val = pLastKey->pks[0].val;
    } else {
      int32_t keyLen = pFirstKey->pks[0].nData;
      TSDB_CHECK_CONDITION((keyLen == 0) || (keyLen > 0 && pFirstKey->pks[0].pData != NULL), code, lino, _end,
                           TSDB_CODE_INVALID_PARA);
      char* p = taosMemoryMalloc(keyLen + VARSTR_HEADER_SIZE);
      TSDB_CHECK_NULL(p, code, lino, _end, terrno);
      memcpy(varDataVal(p), pFirstKey->pks[0].pData, keyLen);
      varDataSetLen(p, keyLen);
      pBlockInfo->firstPk.pData = (uint8_t*)p;

      keyLen = pLastKey->pks[0].nData;
      TSDB_CHECK_CONDITION((keyLen == 0) || (keyLen > 0 && pLastKey->pks[0].pData != NULL), code, lino, _end,
                           TSDB_CODE_INVALID_PARA);
      p = taosMemoryMalloc(keyLen + VARSTR_HEADER_SIZE);
      TSDB_CHECK_NULL(p, code, lino, _end, terrno);
      memcpy(varDataVal(p), pLastKey->pks[0].pData, keyLen);
      varDataSetLen(p, keyLen);
      pBlockInfo->lastPk.pData = (uint8_t*)p;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void freePkItem(void* pItem) {
  SFileDataBlockInfo* p = pItem;
  if (p != NULL) {
    taosMemoryFreeClear(p->firstPk.pData);
    taosMemoryFreeClear(p->lastPk.pData);
  }
}

void clearDataBlockIterator(SDataBlockIter* pIter, bool needFree) {
  if (pIter == NULL) {
    return;
  }

  pIter->index = -1;
  pIter->numOfBlocks = 0;

  if (needFree) {
    taosArrayClearEx(pIter->blockList, freePkItem);
  } else {
    taosArrayClear(pIter->blockList);
  }
}

void cleanupDataBlockIterator(SDataBlockIter* pIter, bool needFree) {
  if (pIter == NULL) {
    return;
  }

  pIter->index = -1;
  pIter->numOfBlocks = 0;
  if (needFree) {
    taosArrayDestroyEx(pIter->blockList, freePkItem);
  } else {
    taosArrayDestroy(pIter->blockList);
  }
  pIter->blockList = NULL;
}

int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks,
                          const SArray* pTableList) {
  int32_t                   code = TSDB_CODE_SUCCESS;
  int32_t                   lino = 0;
  bool                      asc = false;
  int32_t                   numOfTables = 0;
  int64_t                   st = 0;
  int64_t                   et = 0;
  int32_t                   cnt = 0;
  SBlockOrderSupporter      sup = {0};
  SMultiwayMergeTreeInfo*   pTree = NULL;
  STableBlockScanInfo*      pTableScanInfo = NULL;
  const SFileDataBlockInfo* pBlockInfo = NULL;
  const void*               px = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfBlocks >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pReader->info.order);
  clearDataBlockIterator(pBlockIter, shouldFreePkBuf(&pReader->suppInfo));
  pBlockIter->numOfBlocks = numOfBlocks;

  // access data blocks according to the offset of each block in asc/desc order.
  numOfTables = (int32_t)taosArrayGetSize(pTableList);

  st = taosGetTimestampUs();
  code = initBlockOrderSupporter(&sup, numOfTables);
  TSDB_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < numOfTables; ++i) {
    pTableScanInfo = taosArrayGetP(pTableList, i);
    TSDB_CHECK_CONDITION((pTableScanInfo != NULL) && (pTableScanInfo->pBlockIdxList != NULL), code, lino, _end,
                         TSDB_CODE_INVALID_PARA);

    int32_t             num = (int32_t)taosArrayGetSize(pTableScanInfo->pBlockList);
    SBlockOrderWrapper* buf = taosMemoryMalloc(sizeof(SBlockOrderWrapper) * num);
    TSDB_CHECK_NULL(buf, code, lino, _end, terrno);
    sup.numOfBlocksPerTable[sup.numOfTables] = num;
    sup.pDataBlockInfo[sup.numOfTables] = buf;
    sup.numOfTables++;

    for (int32_t k = 0; k < num; ++k) {
      pBlockInfo = taosArrayGet(pTableScanInfo->pBlockList, k);
      TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

      sup.pDataBlockInfo[i][k] =
          (SBlockOrderWrapper){.uid = pTableScanInfo->uid, .offset = pBlockInfo->blockOffset, .pInfo = pTableScanInfo};
      cnt++;
    }
  }

  TSDB_CHECK_CONDITION((numOfBlocks == cnt) && (sup.numOfTables == numOfTables), code, lino, _end,
                       TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockIter->blockList, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // since there is only one table qualified, blocks are not sorted
  if (numOfTables == 1) {
    pTableScanInfo = taosArrayGetP(pTableList, 0);
    for (int32_t i = 0; i < numOfBlocks; ++i) {
      STableDataBlockIdx tableDataBlockIdx = {.globalIndex = i};
      px = taosArrayPush(pTableScanInfo->pBlockIdxList, &tableDataBlockIdx);
      TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    }

    px = taosArrayAddAll(pBlockIter->blockList, pTableScanInfo->pBlockList);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);

    taosArrayDestroy(pTableScanInfo->pBlockList);
    pTableScanInfo->pBlockList = NULL;

    et = taosGetTimestampUs();
    tsdbDebug("%p create blocks info struct completed for one table, %d blocks not sorted, elapsed time:%.2f ms %s",
              pReader, numOfBlocks, (et - st) / 1000.0, pReader->idStr);

    pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
    goto _end;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %s", pReader, cnt, sup.numOfTables,
            pReader->idStr);

  code = tMergeTreeCreate(&pTree, sup.numOfTables, &sup, fileDataBlockOrderCompar);
  TSDB_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < numOfBlocks; ++i) {
    int32_t pos = tMergeTreeGetChosenIndex(pTree);
    int32_t index = sup.indexPerTable[pos]++;
    pTableScanInfo = sup.pDataBlockInfo[pos][index].pInfo;

    pBlockInfo = taosArrayGet(pTableScanInfo->pBlockList, index);
    TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, terrno);

    px = taosArrayPush(pBlockIter->blockList, pBlockInfo);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);

    STableDataBlockIdx tableDataBlockIdx = {.globalIndex = i};
    px = taosArrayPush(pTableScanInfo->pBlockIdxList, &tableDataBlockIdx);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);

    // set data block index overflow, in order to disable the offset comparator
    if (sup.indexPerTable[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.indexPerTable[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    code = tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
    TSDB_CHECK_CODE(code, lino, _end);
  }

  for (int32_t i = 0; i < numOfTables; ++i) {
    pTableScanInfo = taosArrayGetP(pTableList, i);
    taosArrayDestroy(pTableScanInfo->pBlockList);
    pTableScanInfo->pBlockList = NULL;
  }

  et = taosGetTimestampUs();
  tsdbDebug("%p %d data blocks access order completed, elapsed time:%.2f ms %s", pReader, numOfBlocks,
            (et - st) / 1000.0, pReader->idStr);

  pBlockIter->index = asc ? 0 : (numOfBlocks - 1);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  cleanupBlockOrderSupporter(&sup);
  if (pTree != NULL) {
    tMergeTreeDestroy(&pTree);
  }
  return code;
}

bool blockIteratorNext(SDataBlockIter* pBlockIter) {
  bool asc = false;

  if (pBlockIter == NULL) {
    return false;
  }

  asc = ASCENDING_TRAVERSE(pBlockIter->order);
  if ((pBlockIter->index >= pBlockIter->numOfBlocks - 1 && asc) || (pBlockIter->index <= 0 && (!asc))) {
    return false;
  }
  pBlockIter->index += asc ? 1 : -1;
  return true;
}

typedef enum {
  BLK_CHECK_CONTINUE = 0x1,
  BLK_CHECK_QUIT = 0x2,
} ETombBlkCheckEnum;

static int32_t loadNextStatisticsBlock(SSttFileReader* pSttFileReader, STbStatisBlock* pStatisBlock,
                                       const TStatisBlkArray* pStatisBlkArray, int32_t numOfRows, int32_t* i,
                                       int32_t* j);
static int32_t doCheckTombBlock(STombBlock* pBlock, STsdbReader* pReader, int32_t numOfTables, int32_t* j,
                                ETombBlkCheckEnum* pRet) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  STombRecord          record = {0};
  uint64_t             uid = 0;
  STableBlockScanInfo* pScanInfo = NULL;

  TSDB_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION((*j >= 0) && (*j < numOfTables), code, lino, _end, TSDB_CODE_INVALID_PARA);

  uid = pReader->status.uidList.tableUidList[*j];
  code = getTableBlockScanInfo(pReader->status.pTableMap, uid, &pScanInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pScanInfo->pFileDelData == NULL) {
    pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
    TSDB_CHECK_NULL(pScanInfo->pFileDelData, code, lino, _end, terrno);
  }

  for (int32_t k = 0; k < pBlock->numOfRecords; ++k) {
    code = tTombBlockGet(pBlock, k, &record);
    TSDB_CHECK_CODE(code, lino, _end);

    if (record.suid < pReader->info.suid) {
      continue;
    }

    if (record.suid > pReader->info.suid) {
      *pRet = BLK_CHECK_QUIT;
      goto _end;
    }

    if (uid < record.uid) {
      while ((*j) < numOfTables && pReader->status.uidList.tableUidList[*j] < record.uid) {
        (*j) += 1;
      }

      if ((*j) >= numOfTables) {
        *pRet = BLK_CHECK_QUIT;
        goto _end;
      }

      uid = pReader->status.uidList.tableUidList[*j];
      code = getTableBlockScanInfo(pReader->status.pTableMap, uid, &pScanInfo, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);

      if (pScanInfo->pFileDelData == NULL) {
        pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
        TSDB_CHECK_NULL(pScanInfo->pFileDelData, code, lino, _end, terrno);
      }
    }

    if (record.uid < uid) {
      continue;
    }

    TSDB_CHECK_CONDITION((record.suid == pReader->info.suid) && (uid == record.uid), code, lino, _end,
                         TSDB_CODE_INTERNAL_ERROR);

    if (record.version <= pReader->info.verRange.maxVer) {
      SDelData    delData = {.version = record.version, .sKey = record.skey, .eKey = record.ekey};
      const void* px = taosArrayPush(pScanInfo->pFileDelData, &delData);
      TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    }
  }

  *pRet = BLK_CHECK_CONTINUE;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    *pRet = BLK_CHECK_QUIT;
  }
  return code;
}

// load tomb data API
static int32_t doLoadTombDataFromTombBlk(const TTombBlkArray* pTombBlkArray, STsdbReader* pReader, void* pFileReader,
                                         bool isFile) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  const STableUidList* pList = NULL;
  int32_t              numOfTables = 0;
  ETombBlkCheckEnum    ret = BLK_CHECK_CONTINUE;
  STombBlock           block = {0};

  TSDB_CHECK_NULL(pTombBlkArray, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pFileReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pList = &pReader->status.uidList;
  numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);

  TSDB_CHECK_CONDITION(numOfTables == 0 || (pList->tableUidList != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);
  for (int32_t i = 0, j = 0; (i < pTombBlkArray->size) && (j < numOfTables); ++i) {
    const STombBlk* pTombBlk = &pTombBlkArray->data[i];
    if (pTombBlk->maxTbid.suid < pReader->info.suid) {
      i += 1;
      continue;
    }

    if (pTombBlk->minTbid.suid > pReader->info.suid) {
      break;
    }

    TSDB_CHECK_CONDITION(
        (pTombBlk->minTbid.suid <= pReader->info.suid) && (pTombBlk->maxTbid.suid >= pReader->info.suid), code, lino,
        _end, TSDB_CODE_INTERNAL_ERROR);

    if (pTombBlk->maxTbid.suid == pReader->info.suid && pTombBlk->maxTbid.uid < pList->tableUidList[0]) {
      i += 1;
      continue;
    }

    if (pTombBlk->minTbid.suid == pReader->info.suid && pTombBlk->minTbid.uid > pList->tableUidList[numOfTables - 1]) {
      break;
    }

    code = isFile ? tsdbDataFileReadTombBlock(pFileReader, &pTombBlkArray->data[i], &block)
                  : tsdbSttFileReadTombBlock(pFileReader, &pTombBlkArray->data[i], &block);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doCheckTombBlock(&block, pReader, numOfTables, &j, &ret);
    TSDB_CHECK_CODE(code, lino, _end);
    if (ret == BLK_CHECK_QUIT) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  tTombBlockDestroy(&block);
  return code;
}

int32_t loadDataFileTombDataForAll(STsdbReader* pReader) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  const TTombBlkArray* pBlkArray = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if ((pReader->status.pCurrentFileset == NULL) || (pReader->status.pCurrentFileset->farr[TSDB_FTYPE_TOMB] == NULL)) {
    return TSDB_CODE_SUCCESS;
  }

  code = tsdbDataFileReadTombBlk(pReader->pFileReader, &pBlkArray);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doLoadTombDataFromTombBlk(pBlkArray, pReader, pReader->pFileReader, true);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t loadSttTombDataForAll(STsdbReader* pReader, SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pLoadInfo) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  const TTombBlkArray* pBlkArray = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = tsdbSttFileReadTombBlk(pSttFileReader, &pBlkArray);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doLoadTombDataFromTombBlk(pBlkArray, pReader, pSttFileReader, false);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t loadMemTombData(SArray** ppMemDelData, STbData* pMemTbData, const STbData* piMemTbData, int64_t ver) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SArray*         pMemDelData = NULL;
  const SDelData* p = NULL;
  const void*     px = NULL;

  TSDB_CHECK_NULL(ppMemDelData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (*ppMemDelData == NULL) {
    *ppMemDelData = taosArrayInit(4, sizeof(SDelData));
    TSDB_CHECK_NULL(*ppMemDelData, code, lino, _end, terrno);
  }

  pMemDelData = *ppMemDelData;

  if (pMemTbData != NULL) {
    taosRLockLatch(&pMemTbData->lock);
    p = pMemTbData->pHead;
    while (p) {
      if (p->version <= ver) {
        px = taosArrayPush(pMemDelData, p);
        if (px == NULL) {
          taosRUnLockLatch(&pMemTbData->lock);
          TSDB_CHECK_NULL(px, code, lino, _end, terrno);
        }
      }
      p = p->pNext;
    }
    taosRUnLockLatch(&pMemTbData->lock);
  }

  if (piMemTbData != NULL) {
    p = piMemTbData->pHead;
    while (p) {
      if (p->version <= ver) {
        px = taosArrayPush(pMemDelData, p);
        TSDB_CHECK_NULL(px, code, lino, _end, terrno);
      }
      p = p->pNext;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getNumOfRowsInSttBlock(SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pBlockLoadInfo,
                               TStatisBlkArray* pStatisBlkArray, uint64_t suid, const uint64_t* pUidList,
                               int32_t numOfTables, int32_t* pNumOfRows) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  int32_t           num = 0;
  int32_t           blkIndex = 0;
  int32_t           recIndex = 0;
  int32_t           uidIndex = 0;
  int64_t           st = 0;
  const SStatisBlk* p = NULL;
  STbStatisBlock*   pStatisBlock = NULL;

  TSDB_CHECK_NULL(pSttFileReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockLoadInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pStatisBlkArray, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfTables >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pNumOfRows, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pNumOfRows = 0;

  while ((blkIndex < TARRAY2_SIZE(pStatisBlkArray)) && (pStatisBlkArray->data[blkIndex].maxTbid.suid < suid)) {
    blkIndex++;
  }

  if (blkIndex >= TARRAY2_SIZE(pStatisBlkArray)) {
    goto _end;
  }

  p = &pStatisBlkArray->data[blkIndex];
  pStatisBlock = taosMemoryCalloc(1, sizeof(STbStatisBlock));
  TSDB_CHECK_NULL(pStatisBlock, code, lino, _end, terrno);

  code = tStatisBlockInit(pStatisBlock);
  TSDB_CHECK_CODE(code, lino, _end);

  st = taosGetTimestampUs();
  code = tsdbSttFileReadStatisBlock(pSttFileReader, p, pStatisBlock);
  TSDB_CHECK_CODE(code, lino, _end);
  pBlockLoadInfo->cost.loadStatisBlocks += 1;
  pBlockLoadInfo->cost.statisElapsedTime += (taosGetTimestampUs() - st) / 1000.0;

  while (recIndex < pStatisBlock->numOfRecords && ((int64_t*)pStatisBlock->suids.data)[recIndex] < suid) {
    ++recIndex;
  }

  if (recIndex >= pStatisBlock->numOfRecords) {
    goto _end;
  }

  while (blkIndex < TARRAY2_SIZE(pStatisBlkArray) && uidIndex < numOfTables) {
    p = &pStatisBlkArray->data[blkIndex];
    if (p->minTbid.suid > suid) {
      goto _end;
    }

    uint64_t uid = pUidList[uidIndex];

    if (((int64_t*)pStatisBlock->uids.data)[recIndex] == uid) {
      num += ((int64_t*)pStatisBlock->counts.data)[recIndex];
      uidIndex++;
      recIndex++;
      code = loadNextStatisticsBlock(pSttFileReader, pStatisBlock, pStatisBlkArray, pStatisBlock->numOfRecords,
                                     &blkIndex, &recIndex);
      TSDB_CHECK_CODE(code, lino, _end);
    } else if (((int64_t*)pStatisBlock->uids.data)[recIndex] < uid) {
      recIndex++;
      code = loadNextStatisticsBlock(pSttFileReader, pStatisBlock, pStatisBlkArray, pStatisBlock->numOfRecords,
                                     &blkIndex, &recIndex);
      TSDB_CHECK_CODE(code, lino, _end);
    } else {
      uidIndex += 1;
    }
  }

  *pNumOfRows = num;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s with %p failed at line %d since %s", __func__, pSttFileReader, lino, tstrerror(code));
  }
  if (pStatisBlock) {
    tStatisBlockDestroy(pStatisBlock);
    taosMemoryFreeClear(pStatisBlock);
  }
  return code;
}

// load next stt statistics block
static int32_t loadNextStatisticsBlock(SSttFileReader* pSttFileReader, STbStatisBlock* pStatisBlock,
                                       const TStatisBlkArray* pStatisBlkArray, int32_t numOfRows, int32_t* blkIndex,
                                       int32_t* recIndex) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  if ((*recIndex) >= numOfRows) {
    (*blkIndex) += 1;
    (*recIndex) = 0;
    if ((*blkIndex) < TARRAY2_SIZE(pStatisBlkArray)) {
      code = tsdbSttFileReadStatisBlock(pSttFileReader, &pStatisBlkArray->data[(*blkIndex)], pStatisBlock);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doAdjustValidDataIters(SArray* pLDIterList, int32_t numOfFileObj) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  int32_t     size = 0;
  int32_t     inc = 0;
  SLDataIter* pIter = NULL;

  TSDB_CHECK_CONDITION((pLDIterList != NULL) || (numOfFileObj == 0), code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(numOfFileObj >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  size = taosArrayGetSize(pLDIterList);

  if (size < numOfFileObj) {
    inc = numOfFileObj - size;
    for (int32_t k = 0; k < inc; ++k) {
      pIter = taosMemoryCalloc(1, sizeof(SLDataIter));
      TSDB_CHECK_NULL(pIter, code, lino, _end, terrno);
      const void* px = taosArrayPush(pLDIterList, &pIter);
      TSDB_CHECK_NULL(px, code, lino, _end, terrno);
      pIter = NULL;
    }
  } else if (size > numOfFileObj) {  // remove unused LDataIter
    inc = size - numOfFileObj;

    for (int32_t i = 0; i < inc; ++i) {
      pIter = taosArrayPop(pLDIterList);
      destroyLDataIter(pIter);
      pIter = NULL;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pIter != NULL) {
    taosMemoryFreeClear(pIter);
  }
  return code;
}

int32_t adjustSttDataIters(SArray* pSttFileBlockIterArray, STFileSet* pFileSet) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  numOfLevels = 0;
  SSttLvl* pSttLevel = NULL;
  SArray*  pList = NULL;

  TSDB_CHECK_NULL(pSttFileBlockIterArray, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pFileSet, code, lino, _end, TSDB_CODE_INVALID_PARA);

  numOfLevels = pFileSet->lvlArr->size;

  // add the list/iter placeholder
  while (taosArrayGetSize(pSttFileBlockIterArray) < numOfLevels) {
    pList = taosArrayInit(4, POINTER_BYTES);
    TSDB_CHECK_NULL(pList, code, lino, _end, terrno);
    const void* px = taosArrayPush(pSttFileBlockIterArray, &pList);
    TSDB_CHECK_NULL(px, code, lino, _end, terrno);
    pList = NULL;
  }

  for (int32_t j = 0; j < numOfLevels; ++j) {
    pSttLevel = pFileSet->lvlArr->data[j];
    TSDB_CHECK_NULL(pSttLevel, code, lino, _end, TSDB_CODE_INVALID_PARA);
    pList = taosArrayGetP(pSttFileBlockIterArray, j);
    code = doAdjustValidDataIters(pList, TARRAY2_SIZE(pSttLevel->fobjArr));
    TSDB_CHECK_CODE(code, lino, _end);
    pList = NULL;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pList != NULL) {
    taosMemoryFreeClear(pList);
  }
  return code;
}

int32_t tsdbGetRowsInSttFiles(STFileSet* pFileSet, SArray* pSttFileBlockIterArray, STsdb* pTsdb, SMergeTreeConf* pConf,
                              const char* pstr) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfRows = 0;
  int32_t numOfLevels = 0;

  TSDB_CHECK_NULL(pFileSet, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pSttFileBlockIterArray, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTsdb, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION((pConf != NULL) && (pConf->pReader != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pstr, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // no data exists, go to end
  numOfLevels = pFileSet->lvlArr->size;
  if (numOfLevels == 0) {
    goto _end;
  }

  // add the list/iter placeholder
  code = adjustSttDataIters(pSttFileBlockIterArray, pFileSet);
  TSDB_CHECK_CODE(code, lino, _end);

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SSttLvl* pSttLevel = pFileSet->lvlArr->data[j];
    SArray*  pList = taosArrayGetP(pSttFileBlockIterArray, j);

    for (int32_t i = 0; i < taosArrayGetSize(pList); ++i) {  // open all last file
      SLDataIter* pIter = taosArrayGetP(pList, i);

      // open stt file reader if not opened yet
      // if failed to open this stt file, ignore the error and try next one
      if (pIter->pReader == NULL) {
        SSttFileReaderConfig conf = {.tsdb = pTsdb, .szPage = pTsdb->pVnode->config.tsdbPageSize};
        conf.file[0] = *pSttLevel->fobjArr->data[i]->f;

        const char* pName = pSttLevel->fobjArr->data[i]->fname;
        code = tsdbSttFileReaderOpen(pName, &conf, &pIter->pReader);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("open stt file reader error. file:%s, code %s, %s", pName, tstrerror(code), pstr);
          continue;
        }
      }

      if (pIter->pBlockLoadInfo == NULL) {
        code = tCreateSttBlockLoadInfo(pConf->pSchema, pConf->pCols, pConf->numOfCols, &pIter->pBlockLoadInfo);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("failed to create block load info, code: out of memory, %s", pstr);
          continue;
        }
      }

      // load stt blocks statis for all stt-blocks, to decide if the data of queried table exists in current stt file
      TStatisBlkArray* pStatisBlkArray = NULL;
      code = tsdbSttFileReadStatisBlk(pIter->pReader, (const TStatisBlkArray**)&pStatisBlkArray);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("failed to load stt block statistics, code:%s, %s", tstrerror(code), pstr);
        continue;
      }

      // extract rows from each stt file one-by-one
      STsdbReader* pReader = pConf->pReader;
      int32_t      numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
      uint64_t*    pUidList = pReader->status.uidList.tableUidList;
      int32_t      n = 0;
      code = getNumOfRowsInSttBlock(pIter->pReader, pIter->pBlockLoadInfo, pStatisBlkArray, pConf->suid, pUidList,
                                    numOfTables, &n);
      numOfRows += n;
      if (code) {
        tsdbError("%s failed to get rows in stt blocks, code:%s", pstr, tstrerror(code));
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return numOfRows;
}

static bool overlapHelper(const STimeWindow* pLeft, TSKEY minKey, TSKEY maxKey) {
  return (pLeft != NULL) && (pLeft->ekey >= minKey) && (pLeft->skey <= maxKey);
}

static bool overlapWithTimeWindow(STimeWindow* p1, STimeWindow* pQueryWindow, STableBlockScanInfo* pBlockScanInfo,
                                  int32_t order) {
  SIterInfo*   pMemIter = NULL;
  SIterInfo*   pIMemIter = NULL;
  STbData*     pTbData = NULL;
  STimeWindow* pFileWin = NULL;

  if (p1 == NULL || pQueryWindow == NULL || pBlockScanInfo == NULL) {
    return false;
  }

  // overlap with query window
  if (!(p1->skey >= pQueryWindow->skey && p1->ekey <= pQueryWindow->ekey)) {
    return true;
  }

  pMemIter = &pBlockScanInfo->iter;
  pIMemIter = &pBlockScanInfo->iiter;

  // overlap with mem data
  if (pMemIter->hasVal) {
    pTbData = pMemIter->iter->pTbData;
    if (overlapHelper(p1, pTbData->minKey, pTbData->maxKey)) {
      return true;
    }
  }

  // overlap with imem data
  if (pIMemIter->hasVal) {
    pTbData = pIMemIter->iter->pTbData;
    if (overlapHelper(p1, pTbData->minKey, pTbData->maxKey)) {
      return true;
    }
  }

  // overlap with data file block
  pFileWin = &pBlockScanInfo->filesetWindow;
  if ((taosArrayGetSize(pBlockScanInfo->pBlockIdxList) > 0) && overlapHelper(p1, pFileWin->skey, pFileWin->ekey)) {
    return true;
  }

  // overlap with deletion skyline
  SBrinRecord record = {.firstKey = p1->skey, .lastKey = p1->ekey};
  if (overlapWithDelSkylineWithoutVer(pBlockScanInfo, &record, order)) {
    return true;
  }

  return false;
}

static int32_t sortUidComparFn(const void* p1, const void* p2) {
  const SSttKeyRange* px1 = p1;
  const SSttKeyRange* px2 = p2;

  int32_t ret = tRowKeyCompare(&px1->skey, &px2->skey);
  return ret;
}

bool isCleanSttBlock(SArray* pKeyRangeList, STimeWindow* pQueryWindow, STableBlockScanInfo* pScanInfo, int32_t order) {
  int32_t       num = 0;
  SSttKeyRange* pRange = NULL;
  STimeWindow   w;

  num = taosArrayGetSize(pKeyRangeList);
  if (num == 0) {
    return false;
  }

  // check if it overlap with del skyline
  taosArraySort(pKeyRangeList, sortUidComparFn);

  pRange = taosArrayGet(pKeyRangeList, 0);
  if (pRange == NULL) {
    return false;
  }

  w = (STimeWindow){.skey = pRange->skey.ts, .ekey = pRange->ekey.ts};
  if (overlapWithTimeWindow(&w, pQueryWindow, pScanInfo, order)) {
    return false;
  }

  for (int32_t i = 0; i < num - 1; ++i) {
    SSttKeyRange* p1 = taosArrayGet(pKeyRangeList, i);
    SSttKeyRange* p2 = taosArrayGet(pKeyRangeList, i + 1);
    if (p1 == NULL || p2 == NULL) {
      return false;
    }

    if (p1->ekey.ts >= p2->skey.ts) {
      return false;
    }

    w = (STimeWindow){.skey = p2->skey.ts, .ekey = p2->ekey.ts};
    if (overlapWithTimeWindow(&w, pQueryWindow, pScanInfo, order)) {
      return false;
    }
  }

  return true;
}

static bool doCheckDatablockOverlap(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord,
                                    int32_t startIndex) {
  if (pBlockScanInfo == NULL || pRecord == NULL || startIndex < 0) {
    return false;
  }

  size_t num = taosArrayGetSize(pBlockScanInfo->delSkyline);

  for (int32_t i = startIndex; i < num; i += 1) {
    TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, i);
    if (p == NULL) {
      return false;
    }

    if (p->ts >= pRecord->firstKey.key.ts && p->ts <= pRecord->lastKey.key.ts) {
      if (p->version >= pRecord->minVer) {
        return true;
      }
    } else if (p->ts < pRecord->firstKey.key.ts) {  // p->ts < pBlock->minKey.ts
      if (p->version >= pRecord->minVer) {
        if (i < num - 1) {
          TSDBKEY* pnext = taosArrayGet(pBlockScanInfo->delSkyline, i + 1);
          if (pnext == NULL) {
            return false;
          }

          if (pnext->ts >= pRecord->firstKey.key.ts) {
            return true;
          }
        } else {  // it must be the last point
          if (!(p->version == 0)) {
            tsdbError("unexpected version:%" PRId64, p->version);
          }
        }
      }
    } else {  // (p->ts > pBlock->maxKey.ts) {
      return false;
    }
  }

  return false;
}

static bool doCheckDatablockOverlapWithoutVersion(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord,
                                                  int32_t startIndex) {
  if (pBlockScanInfo == NULL || pRecord == NULL || startIndex < 0) {
    return false;
  }

  size_t num = taosArrayGetSize(pBlockScanInfo->delSkyline);

  for (int32_t i = startIndex; i < num; i += 1) {
    TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, i);
    if (p == NULL) {
      return false;
    }

    if (p->ts >= pRecord->firstKey.key.ts && p->ts <= pRecord->lastKey.key.ts) {
      return true;
    } else if (p->ts < pRecord->firstKey.key.ts) {  // p->ts < pBlock->minKey.ts
      if (i < num - 1) {
        TSDBKEY* pnext = taosArrayGet(pBlockScanInfo->delSkyline, i + 1);
        if (pnext == NULL) {
          return false;
        }

        if (pnext->ts >= pRecord->firstKey.key.ts) {
          return true;
        }
      }
    } else {  // (p->ts > pBlock->maxKey.ts) {
      return false;
    }
  }

  return false;
}

bool overlapWithDelSkyline(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord, int32_t order) {
  if (pBlockScanInfo == NULL || pRecord == NULL) {
    return false;
  }

  if (pBlockScanInfo->delSkyline == NULL || (taosArrayGetSize(pBlockScanInfo->delSkyline) == 0)) {
    return false;
  }

  // ts is not overlap
  TSDBKEY* pFirst = taosArrayGet(pBlockScanInfo->delSkyline, 0);
  TSDBKEY* pLast = taosArrayGetLast(pBlockScanInfo->delSkyline);
  if (pFirst == NULL || pLast == NULL) {
    return false;
  }

  if (pRecord->firstKey.key.ts > pLast->ts || pRecord->lastKey.key.ts < pFirst->ts) {
    return false;
  }

  // version is not overlap
  if (ASCENDING_TRAVERSE(order)) {
    return doCheckDatablockOverlap(pBlockScanInfo, pRecord, pBlockScanInfo->fileDelIndex);
  } else {
    int32_t index = pBlockScanInfo->fileDelIndex;
    while (1) {
      TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, index);
      if (p == NULL) {
        return false;
      }

      if (p->ts > pRecord->firstKey.key.ts && index > 0) {
        index -= 1;
      } else {  // find the first point that is smaller than the minKey.ts of dataBlock.
        if (p->ts == pRecord->firstKey.key.ts && p->version < pRecord->maxVer && index > 0) {
          index -= 1;
        }
        break;
      }
    }

    return doCheckDatablockOverlap(pBlockScanInfo, pRecord, index);
  }
}

bool overlapWithDelSkylineWithoutVer(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord, int32_t order) {
  if (pBlockScanInfo == NULL || pRecord == NULL) {
    return false;
  }

  if (pBlockScanInfo->delSkyline == NULL || (taosArrayGetSize(pBlockScanInfo->delSkyline) == 0)) {
    return false;
  }

  // ts is not overlap
  TSDBKEY* pFirst = taosArrayGet(pBlockScanInfo->delSkyline, 0);
  TSDBKEY* pLast = taosArrayGetLast(pBlockScanInfo->delSkyline);
  if (pFirst == NULL || pLast == NULL) {
    return false;
  }

  if (pRecord->firstKey.key.ts > pLast->ts || pRecord->lastKey.key.ts < pFirst->ts) {
    return false;
  }

  // version is not overlap
  if (ASCENDING_TRAVERSE(order)) {
    return doCheckDatablockOverlapWithoutVersion(pBlockScanInfo, pRecord, pBlockScanInfo->fileDelIndex);
  } else {
    int32_t index = pBlockScanInfo->fileDelIndex;
    while (1) {
      TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, index);
      if (p == NULL) {
        return false;
      }

      if (p->ts > pRecord->firstKey.key.ts && index > 0) {
        index -= 1;
      } else {  // find the first point that is smaller than the minKey.ts of dataBlock.
        if (p->ts == pRecord->firstKey.key.ts && index > 0) {
          index -= 1;
        }
        break;
      }
    }

    return doCheckDatablockOverlapWithoutVersion(pBlockScanInfo, pRecord, index);
  }
}
