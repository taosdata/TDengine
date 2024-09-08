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
  int32_t num = numOfTables / pBuf->numPerBucket;
  int32_t remainder = numOfTables % pBuf->numPerBucket;

  if (pBuf->pData == NULL) {
    pBuf->pData = taosArrayInit(num + 1, POINTER_BYTES);
    if (pBuf->pData == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  for (int32_t i = 0; i < num; ++i) {
    char* p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return terrno;
    }

    void* px = taosArrayPush(pBuf->pData, &p);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (remainder > 0) {
    char* p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return terrno;
    }
    void* px = taosArrayPush(pBuf->pData, &p);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  pBuf->numOfTables = numOfTables;

  return TSDB_CODE_SUCCESS;
}

int32_t uidComparFunc(const void* p1, const void* p2) {
  uint64_t pu1 = *(uint64_t*)p1;
  uint64_t pu2 = *(uint64_t*)p2;
  if (pu1 == pu2) {
    return 0;
  } else {
    return (pu1 < pu2) ? -1 : 1;
  }
}

int32_t ensureBlockScanInfoBuf(SBlockInfoBuf* pBuf, int32_t numOfTables) {
  if (numOfTables <= pBuf->numOfTables) {
    return TSDB_CODE_SUCCESS;
  }

  if (pBuf->numOfTables > 0) {
    STableBlockScanInfo** p = (STableBlockScanInfo**)taosArrayPop(pBuf->pData);
    taosMemoryFree(*p);
    pBuf->numOfTables /= pBuf->numPerBucket;
  }

  int32_t num = (numOfTables - pBuf->numOfTables) / pBuf->numPerBucket;
  int32_t remainder = (numOfTables - pBuf->numOfTables) % pBuf->numPerBucket;
  if (pBuf->pData == NULL) {
    pBuf->pData = taosArrayInit(num + 1, POINTER_BYTES);
    if (pBuf->pData == NULL) {
      return terrno;
    }
  }

  for (int32_t i = 0; i < num; ++i) {
    char* p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return terrno;
    }

    void* px = taosArrayPush(pBuf->pData, &p);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (remainder > 0) {
    char* p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return terrno;
    }
    void* px = taosArrayPush(pBuf->pData, &p);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  pBuf->numOfTables = numOfTables;

  return TSDB_CODE_SUCCESS;
}

void clearBlockScanInfoBuf(SBlockInfoBuf* pBuf) {
  size_t num = taosArrayGetSize(pBuf->pData);
  for (int32_t i = 0; i < num; ++i) {
    char** p = taosArrayGet(pBuf->pData, i);
    if (p != NULL) {
      taosMemoryFree(*p);
    }
  }

  taosArrayDestroy(pBuf->pData);
}

int32_t getPosInBlockInfoBuf(SBlockInfoBuf* pBuf, int32_t index, STableBlockScanInfo** pInfo) {
  *pInfo = NULL;

  int32_t bucketIndex = index / pBuf->numPerBucket;
  char**  pBucket = taosArrayGet(pBuf->pData, bucketIndex);
  if (pBucket == NULL) {
    return TSDB_CODE_NOT_FOUND;
  }

  *pInfo = (STableBlockScanInfo*)((*pBucket) + (index % pBuf->numPerBucket) * sizeof(STableBlockScanInfo));
  return TSDB_CODE_SUCCESS;
}

int32_t getTableBlockScanInfo(SSHashObj* pTableMap, uint64_t uid, STableBlockScanInfo** pInfo, const char* id) {
  *pInfo = *(STableBlockScanInfo**)tSimpleHashGet(pTableMap, &uid, sizeof(uid));
  if (pInfo == NULL) {
    int32_t size = tSimpleHashGetSize(pTableMap);
    tsdbError("failed to locate the uid:%" PRIu64 " in query table uid list, total tables:%d, %s", uid, size, id);
    return TSDB_CODE_INVALID_PARA;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t initRowKey(SRowKey* pKey, int64_t ts, int32_t numOfPks, int32_t type, int32_t len, bool asc) {
  pKey->numOfPKs = numOfPks;
  pKey->ts = ts;

  if (numOfPks > 0) {
    pKey->pks[0].type = type;

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
            return TSDB_CODE_INVALID_PARA;
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
            return TSDB_CODE_INVALID_PARA;
        }
      }
    } else {
      pKey->pks[0].pData = taosMemoryCalloc(1, len);
      pKey->pks[0].nData = 0;

      if (pKey->pks[0].pData == NULL) {
        return terrno;
      }

      if (!asc) {
        pKey->numOfPKs = 2;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

void clearRowKey(SRowKey* pKey) {
  if (pKey == NULL || pKey->numOfPKs == 0 || (!IS_VAR_DATA_TYPE(pKey->pks[0].type))) {
    return;
  }
  taosMemoryFreeClear(pKey->pks[0].pData);
}

static int32_t initLastProcKey(STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  int32_t code = 0;
  int32_t numOfPks = pReader->suppInfo.numOfPks;
  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);
  int8_t  type = pReader->suppInfo.pk.type;
  int32_t bytes = pReader->suppInfo.pk.bytes;

  SRowKey* pRowKey = &pScanInfo->lastProcKey;
  if (asc) {
    int64_t skey = pReader->info.window.skey;
    int64_t ts = (skey > INT64_MIN) ? (skey - 1) : skey;

    code = initRowKey(pRowKey, ts, numOfPks, type, bytes, asc);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = initRowKey(&pScanInfo->sttKeyInfo.nextProcKey, skey, numOfPks, type, bytes, asc);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  } else {
    int64_t ekey = pReader->info.window.ekey;
    int64_t ts = (ekey < INT64_MAX) ? (ekey + 1) : ekey;

    code = initRowKey(pRowKey, ts, numOfPks, type, bytes, asc);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = initRowKey(&pScanInfo->sttKeyInfo.nextProcKey, ekey, numOfPks, type, bytes, asc);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  code = initRowKey(&pScanInfo->sttRange.skey, INT64_MAX, numOfPks, type, bytes, asc);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = initRowKey(&pScanInfo->sttRange.ekey, INT64_MIN, numOfPks, type, bytes, asc);
  return code;
}

int32_t initTableBlockScanInfo(STableBlockScanInfo* pScanInfo, uint64_t uid, SSHashObj* pTableMap,
                               STsdbReader* pReader) {
  pScanInfo->uid = uid;
  INIT_KEYRANGE(&pScanInfo->sttRange);
  INIT_TIMEWINDOW(&pScanInfo->filesetWindow);

  pScanInfo->cleanSttBlocks = false;
  pScanInfo->sttBlockReturned = false;

  int32_t code = initLastProcKey(pScanInfo, pReader);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pScanInfo->sttKeyInfo.status = STT_FILE_READER_UNINIT;
  code = tSimpleHashPut(pTableMap, &pScanInfo->uid, sizeof(uint64_t), &pScanInfo, POINTER_BYTES);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  tsdbTrace("%p check table uid:%" PRId64 " from lastKey:%" PRId64 " %s", pReader, pScanInfo->uid,
            pScanInfo->lastProcKey.ts, pReader->idStr);
  return code;
}

// NOTE: speedup the whole processing by preparing the buffer for STableBlockScanInfo in batch model
int32_t createDataBlockScanInfo(STsdbReader* pTsdbReader, SBlockInfoBuf* pBuf, const STableKeyInfo* idList,
                                STableUidList* pUidList, int32_t numOfTables, SSHashObj** pHashObj) {
  int32_t code = 0;
  *pHashObj = NULL;

  // allocate buffer in order to load data blocks from file
  // todo use simple hash instead, optimize the memory consumption
  SSHashObj* pTableMap = tSimpleHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pTableMap == NULL) {
    return terrno;
  }

  int64_t st = taosGetTimestampUs();
  code = initBlockScanInfoBuf(pBuf, numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    tSimpleHashCleanup(pTableMap);
    return code;
  }

  pUidList->tableUidList = taosMemoryMalloc(numOfTables * sizeof(uint64_t));
  if (pUidList->tableUidList == NULL) {
    tSimpleHashCleanup(pTableMap);
    return terrno;
  }

  pUidList->currentIndex = 0;

  for (int32_t j = 0; j < numOfTables; ++j) {
    pUidList->tableUidList[j] = idList[j].uid;

    STableBlockScanInfo* pScanInfo = NULL;
    code = getPosInBlockInfoBuf(pBuf, j, &pScanInfo);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    code = initTableBlockScanInfo(pScanInfo, idList[j].uid, pTableMap, pTsdbReader);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  taosSort(pUidList->tableUidList, numOfTables, sizeof(uint64_t), uidComparFunc);

  pTsdbReader->cost.createScanInfoList = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("%p create %d tables scan-info, size:%.2f Kb, elapsed time:%.2f ms, %s", pTsdbReader, numOfTables,
            (sizeof(STableBlockScanInfo) * numOfTables) / 1024.0, pTsdbReader->cost.createScanInfoList,
            pTsdbReader->idStr);

  *pHashObj = pTableMap;
  return code;
}

void resetAllDataBlockScanInfo(SSHashObj* pTableMap, int64_t ts, int32_t step) {
  void*   p = NULL;
  int32_t iter = 0;

  while ((p = tSimpleHashIterate(pTableMap, p, &iter)) != NULL) {
    STableBlockScanInfo* pInfo = *(STableBlockScanInfo**)p;

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

void destroyAllBlockScanInfo(SSHashObj* pTableMap) {
  void*   p = NULL;
  int32_t iter = 0;

  while ((p = tSimpleHashIterate(pTableMap, p, &iter)) != NULL) {
    clearBlockScanInfo(*(STableBlockScanInfo**)p);
  }

  tSimpleHashCleanup(pTableMap);
}

static void doCleanupInfoForNextFileset(STableBlockScanInfo* pScanInfo) {
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

void cleanupInfoForNextFileset(SSHashObj* pTableMap) {
  STableBlockScanInfo** p = NULL;

  int32_t iter = 0;
  while ((p = tSimpleHashIterate(pTableMap, p, &iter)) != NULL) {
    doCleanupInfoForNextFileset(*p);
  }
}

// brin records iterator
void initBrinRecordIter(SBrinRecordIter* pIter, SDataFileReader* pReader, SArray* pList) {
  (void)memset(&pIter->block, 0, sizeof(SBrinBlock));
  (void)memset(&pIter->record, 0, sizeof(SBrinRecord));
  pIter->blockIndex = -1;
  pIter->recordIndex = -1;

  pIter->pReader = pReader;
  pIter->pBrinBlockList = pList;
}

int32_t getNextBrinRecord(SBrinRecordIter* pIter, SBrinRecord** pRecord) {
  *pRecord = NULL;

  if (pIter->blockIndex == -1 || (pIter->recordIndex + 1) >= pIter->block.numOfRecords) {
    pIter->blockIndex += 1;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pBrinBlockList)) {
      return TSDB_CODE_SUCCESS;
    }

    pIter->pCurrentBlk = taosArrayGet(pIter->pBrinBlockList, pIter->blockIndex);
    if (pIter->pCurrentBlk == NULL) {
      return TSDB_CODE_INVALID_PARA;
    }

    (void)tBrinBlockClear(&pIter->block);
    int32_t code = tsdbDataFileReadBrinBlock(pIter->pReader, pIter->pCurrentBlk, &pIter->block);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("failed to read brinBlock from file, code:%s", tstrerror(code));
      return code;
    }

    pIter->recordIndex = -1;
  }

  pIter->recordIndex += 1;
  int32_t code = tBrinBlockGet(&pIter->block, pIter->recordIndex, &pIter->record);
  *pRecord = &pIter->record;

  return code;
}

void clearBrinBlockIter(SBrinRecordIter* pIter) { (void)tBrinBlockDestroy(&pIter->block); }

// initialize the file block access order
//  sort the file blocks according to the offset of each data block in the files
static void cleanupBlockOrderSupporter(SBlockOrderSupporter* pSup) {
  taosMemoryFreeClear(pSup->numOfBlocksPerTable);
  taosMemoryFreeClear(pSup->indexPerTable);

  for (int32_t i = 0; i < pSup->numOfTables; ++i) {
    SBlockOrderWrapper* pBlockInfo = pSup->pDataBlockInfo[i];
    taosMemoryFreeClear(pBlockInfo);
  }

  taosMemoryFreeClear(pSup->pDataBlockInfo);
}

static int32_t initBlockOrderSupporter(SBlockOrderSupporter* pSup, int32_t numOfTables) {
  pSup->pDataBlockInfo = taosMemoryCalloc(1, POINTER_BYTES * numOfTables);
  pSup->indexPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  pSup->numOfBlocksPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  pSup->numOfTables = 0;
  if (pSup->pDataBlockInfo == NULL || pSup->indexPerTable == NULL || pSup->numOfBlocksPerTable == NULL) {
    cleanupBlockOrderSupporter(pSup);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fileDataBlockOrderCompar(const void* pLeft, const void* pRight, void* param) {
  int32_t leftIndex = *(int32_t*)pLeft;
  int32_t rightIndex = *(int32_t*)pRight;

  SBlockOrderSupporter* pSupporter = (SBlockOrderSupporter*)param;

  int32_t leftTableBlockIndex = pSupporter->indexPerTable[leftIndex];
  int32_t rightTableBlockIndex = pSupporter->indexPerTable[rightIndex];

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerTable[leftIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerTable[rightIndex]) {
    /* right block is empty */
    return -1;
  }

  SBlockOrderWrapper* pLeftBlock = &pSupporter->pDataBlockInfo[leftIndex][leftTableBlockIndex];
  SBlockOrderWrapper* pRightBlock = &pSupporter->pDataBlockInfo[rightIndex][rightTableBlockIndex];

  return pLeftBlock->offset > pRightBlock->offset ? 1 : -1;
}

void recordToBlockInfo(SFileDataBlockInfo* pBlockInfo, SBrinRecord* record) {
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

  SRowKey* pFirstKey = &record->firstKey.key;
  if (pFirstKey->numOfPKs > 0) {
    if (IS_NUMERIC_TYPE(pFirstKey->pks[0].type)) {
      pBlockInfo->firstPk.val = pFirstKey->pks[0].val;
      pBlockInfo->lastPk.val = record->lastKey.key.pks[0].val;
    } else {
      char* p = taosMemoryCalloc(1, pFirstKey->pks[0].nData + VARSTR_HEADER_SIZE);
      memcpy(varDataVal(p), pFirstKey->pks[0].pData, pFirstKey->pks[0].nData);
      varDataSetLen(p, pFirstKey->pks[0].nData);
      pBlockInfo->firstPk.pData = (uint8_t*)p;

      int32_t keyLen = record->lastKey.key.pks[0].nData;
      p = taosMemoryCalloc(1, keyLen + VARSTR_HEADER_SIZE);
      memcpy(varDataVal(p), record->lastKey.key.pks[0].pData, keyLen);
      varDataSetLen(p, keyLen);
      pBlockInfo->lastPk.pData = (uint8_t*)p;
    }
  }
}

static void freePkItem(void* pItem) {
  SFileDataBlockInfo* p = pItem;
  taosMemoryFreeClear(p->firstPk.pData);
  taosMemoryFreeClear(p->lastPk.pData);
}

void clearDataBlockIterator(SDataBlockIter* pIter, bool needFree) {
  pIter->index = -1;
  pIter->numOfBlocks = 0;

  if (needFree) {
    taosArrayClearEx(pIter->blockList, freePkItem);
  } else {
    taosArrayClear(pIter->blockList);
  }
}

void cleanupDataBlockIterator(SDataBlockIter* pIter, bool needFree) {
  pIter->index = -1;
  pIter->numOfBlocks = 0;
  if (needFree) {
    taosArrayDestroyEx(pIter->blockList, freePkItem);
  } else {
    taosArrayDestroy(pIter->blockList);
  }
}

int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks, SArray* pTableList) {
  bool asc = ASCENDING_TRAVERSE(pReader->info.order);

  SBlockOrderSupporter sup = {0};
  clearDataBlockIterator(pBlockIter, shouldFreePkBuf(&pReader->suppInfo));

  pBlockIter->numOfBlocks = numOfBlocks;

  // access data blocks according to the offset of each block in asc/desc order.
  int32_t numOfTables = taosArrayGetSize(pTableList);

  int64_t st = taosGetTimestampUs();
  int32_t code = initBlockOrderSupporter(&sup, numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t cnt = 0;

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableBlockScanInfo* pTableScanInfo = taosArrayGetP(pTableList, i);

    size_t num = taosArrayGetSize(pTableScanInfo->pBlockList);
    sup.numOfBlocksPerTable[sup.numOfTables] = num;

    char* buf = taosMemoryMalloc(sizeof(SBlockOrderWrapper) * num);
    if (buf == NULL) {
      cleanupBlockOrderSupporter(&sup);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    sup.pDataBlockInfo[sup.numOfTables] = (SBlockOrderWrapper*)buf;

    for (int32_t k = 0; k < num; ++k) {
      SFileDataBlockInfo* pBlockInfo = taosArrayGet(pTableScanInfo->pBlockList, k);
      if (pBlockInfo == NULL) {
        return TSDB_CODE_INVALID_PARA;
      }

      sup.pDataBlockInfo[sup.numOfTables][k] =
          (SBlockOrderWrapper){.uid = pTableScanInfo->uid, .offset = pBlockInfo->blockOffset, .pInfo = pTableScanInfo};
      cnt++;
    }

    sup.numOfTables += 1;
  }

  if (numOfBlocks != cnt && sup.numOfTables != numOfTables) {
    cleanupBlockOrderSupporter(&sup);
    return TSDB_CODE_INVALID_PARA;
  }

  // since there is only one table qualified, blocks are not sorted
  if (sup.numOfTables == 1) {
    STableBlockScanInfo* pTableScanInfo = taosArrayGetP(pTableList, 0);
    for (int32_t i = 0; i < numOfBlocks; ++i) {
      STableDataBlockIdx tableDataBlockIdx = {.globalIndex = i};
      void*              px = taosArrayPush(pTableScanInfo->pBlockIdxList, &tableDataBlockIdx);
      if (px == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    void* p = taosArrayAddAll(pBlockIter->blockList, pTableScanInfo->pBlockList);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayDestroy(pTableScanInfo->pBlockList);
    pTableScanInfo->pBlockList = NULL;

    int64_t et = taosGetTimestampUs();
    tsdbDebug("%p create blocks info struct completed for one table, %d blocks not sorted, elapsed time:%.2f ms %s",
              pReader, numOfBlocks, (et - st) / 1000.0, pReader->idStr);

    pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
    cleanupBlockOrderSupporter(&sup);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %s", pReader, cnt, sup.numOfTables,
            pReader->idStr);

  SMultiwayMergeTreeInfo* pTree = NULL;

  uint8_t ret = tMergeTreeCreate(&pTree, sup.numOfTables, &sup, fileDataBlockOrderCompar);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanupBlockOrderSupporter(&sup);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;
  while (numOfTotal < cnt) {
    int32_t pos = tMergeTreeGetChosenIndex(pTree);
    int32_t index = sup.indexPerTable[pos]++;

    SFileDataBlockInfo* pBlockInfo = taosArrayGet(sup.pDataBlockInfo[pos][index].pInfo->pBlockList, index);
    if (pBlockInfo == NULL) {
      return TSDB_CODE_INVALID_PARA;
    }

    void* px = taosArrayPush(pBlockIter->blockList, pBlockInfo);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    STableBlockScanInfo* pTableScanInfo = sup.pDataBlockInfo[pos][index].pInfo;
    STableDataBlockIdx   tableDataBlockIdx = {.globalIndex = numOfTotal};

    px = taosArrayPush(pTableScanInfo->pBlockIdxList, &tableDataBlockIdx);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    // set data block index overflow, in order to disable the offset comparator
    if (sup.indexPerTable[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.indexPerTable[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    numOfTotal += 1;
    code = tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableBlockScanInfo* pTableScanInfo = taosArrayGetP(pTableList, i);
    taosArrayDestroy(pTableScanInfo->pBlockList);
    pTableScanInfo->pBlockList = NULL;
  }

  int64_t et = taosGetTimestampUs();
  tsdbDebug("%p %d data blocks access order completed, elapsed time:%.2f ms %s", pReader, numOfBlocks,
            (et - st) / 1000.0, pReader->idStr);
  cleanupBlockOrderSupporter(&sup);
  taosMemoryFree(pTree);

  pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
  return TSDB_CODE_SUCCESS;
}

bool blockIteratorNext(SDataBlockIter* pBlockIter, const char* idStr) {
  bool asc = ASCENDING_TRAVERSE(pBlockIter->order);

  int32_t step = asc ? 1 : -1;
  if ((pBlockIter->index >= pBlockIter->numOfBlocks - 1 && asc) || (pBlockIter->index <= 0 && (!asc))) {
    return false;
  }

  pBlockIter->index += step;
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
  int32_t     code = 0;
  STombRecord record = {0};

  uint64_t             uid = pReader->status.uidList.tableUidList[*j];
  STableBlockScanInfo* pScanInfo = NULL;

  code = getTableBlockScanInfo(pReader->status.pTableMap, uid, &pScanInfo, pReader->idStr);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (pScanInfo->pFileDelData == NULL) {
    pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
    if (pScanInfo->pFileDelData == NULL) {
      return terrno;
    }
  }

  for (int32_t k = 0; k < pBlock->numOfRecords; ++k) {
    code = tTombBlockGet(pBlock, k, &record);
    if (code != TSDB_CODE_SUCCESS) {
      *pRet = BLK_CHECK_QUIT;
      return code;
    }

    if (record.suid < pReader->info.suid) {
      continue;
    }

    if (record.suid > pReader->info.suid) {
      *pRet = BLK_CHECK_QUIT;
      return TSDB_CODE_SUCCESS;
    }

    if (uid < record.uid) {
      while ((*j) < numOfTables && pReader->status.uidList.tableUidList[*j] < record.uid) {
        (*j) += 1;
      }

      if ((*j) >= numOfTables) {
        *pRet = BLK_CHECK_QUIT;
        return TSDB_CODE_SUCCESS;
      }

      uid = pReader->status.uidList.tableUidList[*j];
      code = getTableBlockScanInfo(pReader->status.pTableMap, uid, &pScanInfo, pReader->idStr);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (pScanInfo->pFileDelData == NULL) {
        pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
        if (pScanInfo->pFileDelData == NULL) {
          return terrno;
        }
      }
    }

    if (record.uid < uid) {
      continue;
    }

    if (!(record.suid == pReader->info.suid && uid == record.uid)) {
      tsdbError("tsdb reader failed at: %s:%d", __func__, __LINE__);
      return TSDB_CODE_INTERNAL_ERROR;
    }

    if (record.version <= pReader->info.verRange.maxVer) {
      SDelData delData = {.version = record.version, .sKey = record.skey, .eKey = record.ekey};
      void*    px = taosArrayPush(pScanInfo->pFileDelData, &delData);
      if (px == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }

  *pRet = BLK_CHECK_CONTINUE;
  return TSDB_CODE_SUCCESS;
}

// load tomb data API
static int32_t doLoadTombDataFromTombBlk(const TTombBlkArray* pTombBlkArray, STsdbReader* pReader, void* pFileReader,
                                         bool isFile) {
  int32_t        code = 0;
  STableUidList* pList = &pReader->status.uidList;
  int32_t        numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);

  int32_t i = 0, j = 0;
  while (i < pTombBlkArray->size && j < numOfTables) {
    STombBlk* pTombBlk = &pTombBlkArray->data[i];
    if (pTombBlk->maxTbid.suid < pReader->info.suid) {
      i += 1;
      continue;
    }

    if (pTombBlk->minTbid.suid > pReader->info.suid) {
      break;
    }

    if (!(pTombBlk->minTbid.suid <= pReader->info.suid && pTombBlk->maxTbid.suid >= pReader->info.suid)) {
      tsdbError("tsdb reader failed at: %s:%d", __func__, __LINE__);
      return TSDB_CODE_INTERNAL_ERROR;
    }
    if (pTombBlk->maxTbid.suid == pReader->info.suid && pTombBlk->maxTbid.uid < pList->tableUidList[0]) {
      i += 1;
      continue;
    }

    if (pTombBlk->minTbid.suid == pReader->info.suid && pTombBlk->minTbid.uid > pList->tableUidList[numOfTables - 1]) {
      break;
    }

    STombBlock block = {0};
    code = isFile ? tsdbDataFileReadTombBlock(pFileReader, &pTombBlkArray->data[i], &block)
                  : tsdbSttFileReadTombBlock(pFileReader, &pTombBlkArray->data[i], &block);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    //    uint64_t uid = pReader->status.uidList.tableUidList[j];

    //    STableBlockScanInfo* pScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, uid, pReader->idStr);
    //    if (pScanInfo->pFileDelData == NULL) {
    //      pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
    //    }

    ETombBlkCheckEnum ret = 0;
    code = doCheckTombBlock(&block, pReader, numOfTables, &j, &ret);

    tTombBlockDestroy(&block);
    if (code != TSDB_CODE_SUCCESS || ret == BLK_CHECK_QUIT) {
      return code;
    }

    i += 1;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t loadDataFileTombDataForAll(STsdbReader* pReader) {
  if (pReader->status.pCurrentFileset == NULL || pReader->status.pCurrentFileset->farr[3] == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const TTombBlkArray* pBlkArray = NULL;

  int32_t code = tsdbDataFileReadTombBlk(pReader->pFileReader, &pBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return doLoadTombDataFromTombBlk(pBlkArray, pReader, pReader->pFileReader, true);
}

int32_t loadSttTombDataForAll(STsdbReader* pReader, SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pLoadInfo) {
  const TTombBlkArray* pBlkArray = NULL;
  int32_t              code = tsdbSttFileReadTombBlk(pSttFileReader, &pBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  return doLoadTombDataFromTombBlk(pBlkArray, pReader, pSttFileReader, false);
}

int32_t loadMemTombData(SArray** ppMemDelData, STbData* pMemTbData, STbData* piMemTbData, int64_t ver) {
  if (*ppMemDelData == NULL) {
    *ppMemDelData = taosArrayInit(4, sizeof(SDelData));
    if (*ppMemDelData == NULL) {
      return terrno;
    }
  }

  SArray* pMemDelData = *ppMemDelData;

  SDelData* p = NULL;
  if (pMemTbData != NULL) {
    taosRLockLatch(&pMemTbData->lock);
    p = pMemTbData->pHead;
    while (p) {
      if (p->version <= ver) {
        void* px = taosArrayPush(pMemDelData, p);
        if (px == NULL) {
          taosRUnLockLatch(&pMemTbData->lock);
          return terrno;
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
        void* px = taosArrayPush(pMemDelData, p);
        if (px == NULL) {
          return terrno;
        }
      }
      p = p->pNext;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t getNumOfRowsInSttBlock(SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pBlockLoadInfo,
                               TStatisBlkArray* pStatisBlkArray, uint64_t suid, const uint64_t* pUidList,
                               int32_t numOfTables, int32_t* pNumOfRows) {
  int32_t num = 0;
  int32_t code = 0;
  int32_t lino = 0;

  if (pNumOfRows != 0) {
    *pNumOfRows = 0;
  }

  if (TARRAY2_SIZE(pStatisBlkArray) <= 0) {
    return code;
  }

  int32_t i = 0;
  while ((i < TARRAY2_SIZE(pStatisBlkArray)) && (pStatisBlkArray->data[i].maxTbid.suid < suid)) {
    ++i;
  }

  if (i >= TARRAY2_SIZE(pStatisBlkArray)) {
    return code;
  }

  SStatisBlk*     p = &pStatisBlkArray->data[i];
  STbStatisBlock* pStatisBlock = taosMemoryCalloc(1, sizeof(STbStatisBlock));
  TSDB_CHECK_NULL(pStatisBlock, code, lino, _err, terrno);

  code = tStatisBlockInit(pStatisBlock);
  TSDB_CHECK_CODE(code, lino, _err);

  int64_t st = taosGetTimestampMs();
  code = tsdbSttFileReadStatisBlock(pSttFileReader, p, pStatisBlock);
  TSDB_CHECK_CODE(code, lino, _err);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  pBlockLoadInfo->cost.loadStatisBlocks += 1;
  pBlockLoadInfo->cost.statisElapsedTime += el;

  int32_t index = 0;
  while (index < pStatisBlock->numOfRecords && ((int64_t*)pStatisBlock->suids.data)[index] < suid) {
    ++index;
  }

  if (index >= pStatisBlock->numOfRecords) {
    code = tStatisBlockDestroy(pStatisBlock);
    taosMemoryFreeClear(pStatisBlock);
    *pNumOfRows = num;
    return code;
  }

  int32_t j = index;
  int32_t uidIndex = 0;
  while (i < TARRAY2_SIZE(pStatisBlkArray) && uidIndex < numOfTables) {
    p = &pStatisBlkArray->data[i];
    if (p->minTbid.suid > suid) {
      code = tStatisBlockDestroy(pStatisBlock);
      taosMemoryFreeClear(pStatisBlock);
      *pNumOfRows = num;
      return code;
    }

    uint64_t uid = pUidList[uidIndex];

    if (((int64_t*)pStatisBlock->uids.data)[j] == uid) {
      num += ((int64_t*)pStatisBlock->counts.data)[j];
      uidIndex += 1;
      j += 1;
      code = loadNextStatisticsBlock(pSttFileReader, pStatisBlock, pStatisBlkArray, pStatisBlock->numOfRecords, &i, &j);
      TSDB_CHECK_CODE(code, lino, _err);
    } else if (((int64_t*)pStatisBlock->uids.data)[j] < uid) {
      j += 1;
      code = loadNextStatisticsBlock(pSttFileReader, pStatisBlock, pStatisBlkArray, pStatisBlock->numOfRecords, &i, &j);
      TSDB_CHECK_CODE(code, lino, _err);
    } else {
      uidIndex += 1;
    }
  }

  tStatisBlockDestroy(pStatisBlock);
  taosMemoryFreeClear(pStatisBlock);
  *pNumOfRows = num;
  return code;

_err:
  tsdbError("%p failed to get number of rows in stt block, %s at line:%d code:%s", pSttFileReader, __func__, lino,
            tstrerror(code));
  return code;
}

// load next stt statistics block
static int32_t loadNextStatisticsBlock(SSttFileReader* pSttFileReader, STbStatisBlock* pStatisBlock,
                                    const TStatisBlkArray* pStatisBlkArray, int32_t numOfRows, int32_t* i, int32_t* j) {
  if ((*j) >= numOfRows) {
    (*i) += 1;
    (*j) = 0;
    if ((*i) < TARRAY2_SIZE(pStatisBlkArray)) {
      int32_t code = tsdbSttFileReadStatisBlock(pSttFileReader, &pStatisBlkArray->data[(*i)], pStatisBlock);
      if (code != 0) {
        tsdbError("%p failed to read statisBlock, code:%s", pSttFileReader, tstrerror(code));
        return code;
      }
    }
  }

  return 0;
}

int32_t doAdjustValidDataIters(SArray* pLDIterList, int32_t numOfFileObj) {
  int32_t size = taosArrayGetSize(pLDIterList);

  if (size < numOfFileObj) {
    int32_t inc = numOfFileObj - size;
    for (int32_t k = 0; k < inc; ++k) {
      SLDataIter* pIter = taosMemoryCalloc(1, sizeof(SLDataIter));
      if (!pIter) {
        return terrno;
      }
      void* px = taosArrayPush(pLDIterList, &pIter);
      if (px == NULL) {
        taosMemoryFree(pIter);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  } else if (size > numOfFileObj) {  // remove unused LDataIter
    int32_t inc = size - numOfFileObj;

    for (int i = 0; i < inc; ++i) {
      SLDataIter* pIter = taosArrayPop(pLDIterList);
      destroyLDataIter(pIter);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t adjustSttDataIters(SArray* pSttFileBlockIterArray, STFileSet* pFileSet) {
  int32_t numOfLevels = pFileSet->lvlArr->size;
  int32_t code = 0;

  // add the list/iter placeholder
  while (taosArrayGetSize(pSttFileBlockIterArray) < numOfLevels) {
    SArray* pList = taosArrayInit(4, POINTER_BYTES);
    if (pList == NULL) {
      return terrno;
    }
    void* px = taosArrayPush(pSttFileBlockIterArray, &pList);
    if (px == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SSttLvl* pSttLevel = pFileSet->lvlArr->data[j];
    SArray*  pList = taosArrayGetP(pSttFileBlockIterArray, j);
    code = doAdjustValidDataIters(pList, TARRAY2_SIZE(pSttLevel->fobjArr));
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbGetRowsInSttFiles(STFileSet* pFileSet, SArray* pSttFileBlockIterArray, STsdb* pTsdb, SMergeTreeConf* pConf,
                              const char* pstr) {
  int32_t numOfRows = 0;
  int32_t code = 0;

  // no data exists, go to end
  int32_t numOfLevels = pFileSet->lvlArr->size;
  if (numOfLevels == 0) {
    return numOfRows;
  }

  // add the list/iter placeholder
  code = adjustSttDataIters(pSttFileBlockIterArray, pFileSet);
  if (code != TSDB_CODE_SUCCESS) {
    return numOfRows;
  }

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

  return numOfRows;
}

static bool overlapHelper(const STimeWindow* pLeft, TSKEY minKey, TSKEY maxKey) {
  return (pLeft->ekey >= minKey) && (pLeft->skey <= maxKey);
}

static bool overlapWithTimeWindow(STimeWindow* p1, STimeWindow* pQueryWindow, STableBlockScanInfo* pBlockScanInfo,
                                  int32_t order) {
  // overlap with query window
  if (!(p1->skey >= pQueryWindow->skey && p1->ekey <= pQueryWindow->ekey)) {
    return true;
  }

  SIterInfo* pMemIter = &pBlockScanInfo->iter;
  SIterInfo* pIMemIter = &pBlockScanInfo->iiter;

  // overlap with mem data
  if (pMemIter->hasVal) {
    STbData* pTbData = pMemIter->iter->pTbData;
    if (overlapHelper(p1, pTbData->minKey, pTbData->maxKey)) {
      return true;
    }
  }

  // overlap with imem data
  if (pIMemIter->hasVal) {
    STbData* pITbData = pIMemIter->iter->pTbData;
    if (overlapHelper(p1, pITbData->minKey, pITbData->maxKey)) {
      return true;
    }
  }

  // overlap with data file block
  STimeWindow* pFileWin = &pBlockScanInfo->filesetWindow;
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
  // check if it overlap with del skyline
  taosArraySort(pKeyRangeList, sortUidComparFn);

  int32_t num = taosArrayGetSize(pKeyRangeList);
  if (num == 0) {
    return false;
  }

  SSttKeyRange* pRange = taosArrayGet(pKeyRangeList, 0);
  if (pRange == NULL) {
    return false;
  }

  STimeWindow w = {.skey = pRange->skey.ts, .ekey = pRange->ekey.ts};
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

    STimeWindow w2 = {.skey = p2->skey.ts, .ekey = p2->ekey.ts};
    bool        overlap = overlapWithTimeWindow(&w2, pQueryWindow, pScanInfo, order);
    if (overlap) {
      return false;
    }
  }

  return true;
}

static bool doCheckDatablockOverlap(STableBlockScanInfo* pBlockScanInfo, const SBrinRecord* pRecord,
                                    int32_t startIndex) {
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
