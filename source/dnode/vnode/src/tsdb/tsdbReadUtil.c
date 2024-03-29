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
#include "osDef.h"
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
  }

  for (int32_t i = 0; i < num; ++i) {
    char* p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayPush(pBuf->pData, &p);
  }

  if (remainder > 0) {
    char* p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pBuf->pData, &p);
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
  }

  for (int32_t i = 0; i < num; ++i) {
    char* p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayPush(pBuf->pData, &p);
  }

  if (remainder > 0) {
    char* p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pBuf->pData, &p);
  }

  pBuf->numOfTables = numOfTables;

  return TSDB_CODE_SUCCESS;
}

void clearBlockScanInfoBuf(SBlockInfoBuf* pBuf) {
  size_t num = taosArrayGetSize(pBuf->pData);
  for (int32_t i = 0; i < num; ++i) {
    char** p = taosArrayGet(pBuf->pData, i);
    taosMemoryFree(*p);
  }

  taosArrayDestroy(pBuf->pData);
}

void* getPosInBlockInfoBuf(SBlockInfoBuf* pBuf, int32_t index) {
  int32_t bucketIndex = index / pBuf->numPerBucket;
  char**  pBucket = taosArrayGet(pBuf->pData, bucketIndex);
  return (*pBucket) + (index % pBuf->numPerBucket) * sizeof(STableBlockScanInfo);
}

STableBlockScanInfo* getTableBlockScanInfo(SSHashObj* pTableMap, uint64_t uid, const char* id) {
  STableBlockScanInfo** p = tSimpleHashGet(pTableMap, &uid, sizeof(uid));
  if (p == NULL || *p == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    int32_t size = tSimpleHashGetSize(pTableMap);
    tsdbError("failed to locate the uid:%" PRIu64 " in query table uid list, total tables:%d, %s", uid, size, id);
    return NULL;
  }

  return *p;
}

// NOTE: speedup the whole processing by preparing the buffer for STableBlockScanInfo in batch model
SSHashObj* createDataBlockScanInfo(STsdbReader* pTsdbReader, SBlockInfoBuf* pBuf, const STableKeyInfo* idList,
                                   STableUidList* pUidList, int32_t numOfTables) {
  // allocate buffer in order to load data blocks from file
  // todo use simple hash instead, optimize the memory consumption
  SSHashObj* pTableMap = tSimpleHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pTableMap == NULL) {
    return NULL;
  }

  int64_t st = taosGetTimestampUs();
  initBlockScanInfoBuf(pBuf, numOfTables);

  pUidList->tableUidList = taosMemoryMalloc(numOfTables * sizeof(uint64_t));
  if (pUidList->tableUidList == NULL) {
    tSimpleHashCleanup(pTableMap);
    return NULL;
  }

  pUidList->currentIndex = 0;

  for (int32_t j = 0; j < numOfTables; ++j) {
    STableBlockScanInfo* pScanInfo = getPosInBlockInfoBuf(pBuf, j);

    pScanInfo->uid = idList[j].uid;
    INIT_TIMEWINDOW(&pScanInfo->sttWindow);
    INIT_TIMEWINDOW(&pScanInfo->filesetWindow);

    pScanInfo->cleanSttBlocks = false;
    pScanInfo->sttBlockReturned = false;

    pUidList->tableUidList[j] = idList[j].uid;

    if (ASCENDING_TRAVERSE(pTsdbReader->info.order)) {
      int64_t skey = pTsdbReader->info.window.skey;
      pScanInfo->lastProcKey = (skey > INT64_MIN) ? (skey - 1) : skey;
      pScanInfo->sttKeyInfo.nextProcKey = skey;
    } else {
      int64_t ekey = pTsdbReader->info.window.ekey;
      pScanInfo->lastProcKey = (ekey < INT64_MAX) ? (ekey + 1) : ekey;
      pScanInfo->sttKeyInfo.nextProcKey = ekey;
    }

    pScanInfo->sttKeyInfo.status = STT_FILE_READER_UNINIT;
    tSimpleHashPut(pTableMap, &pScanInfo->uid, sizeof(uint64_t), &pScanInfo, POINTER_BYTES);
    tsdbTrace("%p check table uid:%" PRId64 " from lastKey:%" PRId64 " %s", pTsdbReader, pScanInfo->uid,
              pScanInfo->lastProcKey, pTsdbReader->idStr);
  }

  taosSort(pUidList->tableUidList, numOfTables, sizeof(uint64_t), uidComparFunc);

  pTsdbReader->cost.createScanInfoList = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("%p create %d tables scan-info, size:%.2f Kb, elapsed time:%.2f ms, %s", pTsdbReader, numOfTables,
            (sizeof(STableBlockScanInfo) * numOfTables) / 1024.0, pTsdbReader->cost.createScanInfoList,
            pTsdbReader->idStr);

  return pTableMap;
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

    pInfo->delSkyline = taosArrayDestroy(pInfo->delSkyline);
    pInfo->lastProcKey = ts;
    pInfo->sttKeyInfo.nextProcKey = ts + step;
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

  p->delSkyline = taosArrayDestroy(p->delSkyline);
  p->pBlockList = taosArrayDestroy(p->pBlockList);
  p->pBlockIdxList = taosArrayDestroy(p->pBlockIdxList);
  p->pMemDelData = taosArrayDestroy(p->pMemDelData);
  p->pFileDelData = taosArrayDestroy(p->pFileDelData);
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
  INIT_TIMEWINDOW(&pScanInfo->sttWindow);
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
  memset(&pIter->block, 0, sizeof(SBrinBlock));
  memset(&pIter->record, 0, sizeof(SBrinRecord));
  pIter->blockIndex = -1;
  pIter->recordIndex = -1;

  pIter->pReader = pReader;
  pIter->pBrinBlockList = pList;
}

SBrinRecord* getNextBrinRecord(SBrinRecordIter* pIter) {
  if (pIter->blockIndex == -1 || (pIter->recordIndex + 1) >= TARRAY2_SIZE(pIter->block.numRow)) {
    pIter->blockIndex += 1;
    if (pIter->blockIndex >= taosArrayGetSize(pIter->pBrinBlockList)) {
      return NULL;
    }

    pIter->pCurrentBlk = taosArrayGet(pIter->pBrinBlockList, pIter->blockIndex);

    tBrinBlockClear(&pIter->block);
    int32_t code = tsdbDataFileReadBrinBlock(pIter->pReader, pIter->pCurrentBlk, &pIter->block);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("failed to read brinBlock from file, code:%s", tstrerror(code));
      return NULL;
    }

    pIter->recordIndex = -1;
  }

  pIter->recordIndex += 1;
  tBrinBlockGet(&pIter->block, pIter->recordIndex, &pIter->record);
  return &pIter->record;
}

void clearBrinBlockIter(SBrinRecordIter* pIter) { tBrinBlockDestroy(&pIter->block); }

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
  pSup->numOfBlocksPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  pSup->indexPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  pSup->pDataBlockInfo = taosMemoryCalloc(1, POINTER_BYTES * numOfTables);

  if (pSup->numOfBlocksPerTable == NULL || pSup->indexPerTable == NULL || pSup->pDataBlockInfo == NULL) {
    cleanupBlockOrderSupporter(pSup);
    return TSDB_CODE_OUT_OF_MEMORY;
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

static void recordToBlockInfo(SFileDataBlockInfo* pBlockInfo, SBrinRecord* record){
  pBlockInfo->uid = record->uid;
  pBlockInfo->firstKey = record->firstKey;
  pBlockInfo->lastKey = record->lastKey;
  pBlockInfo->minVer = record->minVer;
  pBlockInfo->maxVer = record->maxVer;
  pBlockInfo->blockOffset = record->blockOffset;
  pBlockInfo->smaOffset = record->smaOffset;
  pBlockInfo->blockSize = record->blockSize;
  pBlockInfo->blockKeySize = record->blockKeySize;
  pBlockInfo->smaSize = record->smaSize;
  pBlockInfo->numRow = record->numRow;
  pBlockInfo->count = record->count;
}

int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks, SArray* pTableList) {
  bool asc = ASCENDING_TRAVERSE(pReader->info.order);

  SBlockOrderSupporter sup = {0};
  pBlockIter->numOfBlocks = numOfBlocks;
  taosArrayClear(pBlockIter->blockList);

  pBlockIter->pTableMap = pReader->status.pTableMap;

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
    //    ASSERT(pTableScanInfo->pBlockList != NULL && taosArrayGetSize(pTableScanInfo->pBlockList) > 0);

    size_t num = taosArrayGetSize(pTableScanInfo->pBlockList);
    sup.numOfBlocksPerTable[sup.numOfTables] = num;

    char* buf = taosMemoryMalloc(sizeof(SBlockOrderWrapper) * num);
    if (buf == NULL) {
      cleanupBlockOrderSupporter(&sup);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    sup.pDataBlockInfo[sup.numOfTables] = (SBlockOrderWrapper*)buf;

    for (int32_t k = 0; k < num; ++k) {
      SBrinRecord* pRecord = taosArrayGet(pTableScanInfo->pBlockList, k);
      sup.pDataBlockInfo[sup.numOfTables][k] =
          (SBlockOrderWrapper){.uid = pTableScanInfo->uid, .offset = pRecord->blockOffset, .pInfo = pTableScanInfo};
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
    if (pTableScanInfo->pBlockIdxList == NULL) {
      pTableScanInfo->pBlockIdxList = taosArrayInit(numOfBlocks, sizeof(STableDataBlockIdx));
    }
    for (int32_t i = 0; i < numOfBlocks; ++i) {
      SFileDataBlockInfo blockInfo = {.tbBlockIdx = i};
      SBrinRecord* record = (SBrinRecord*)taosArrayGet(sup.pDataBlockInfo[0][i].pInfo->pBlockList, i);
      recordToBlockInfo(&blockInfo, record);

      taosArrayPush(pBlockIter->blockList, &blockInfo);
      STableDataBlockIdx tableDataBlockIdx = {.globalIndex = i};
      taosArrayPush(pTableScanInfo->pBlockIdxList, &tableDataBlockIdx);
    }
    pTableScanInfo->pBlockList = taosArrayDestroy(pTableScanInfo->pBlockList);

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

    SFileDataBlockInfo blockInfo = {.tbBlockIdx = index};
    SBrinRecord* record = (SBrinRecord*)taosArrayGet(sup.pDataBlockInfo[pos][index].pInfo->pBlockList, index);
    recordToBlockInfo(&blockInfo, record);

    taosArrayPush(pBlockIter->blockList, &blockInfo);
    STableBlockScanInfo* pTableScanInfo = sup.pDataBlockInfo[pos][index].pInfo;
    if (pTableScanInfo->pBlockIdxList == NULL) {
      size_t szTableDataBlocks = taosArrayGetSize(pTableScanInfo->pBlockList);
      pTableScanInfo->pBlockIdxList = taosArrayInit(szTableDataBlocks, sizeof(STableDataBlockIdx));
    }
    STableDataBlockIdx tableDataBlockIdx = {.globalIndex = numOfTotal};
    taosArrayPush(pTableScanInfo->pBlockIdxList, &tableDataBlockIdx);
    // set data block index overflow, in order to disable the offset comparator
    if (sup.indexPerTable[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.indexPerTable[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    numOfTotal += 1;
    tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
  }

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableBlockScanInfo* pTableScanInfo = taosArrayGetP(pTableList, i);
    pTableScanInfo->pBlockList = taosArrayDestroy(pTableScanInfo->pBlockList);
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

static void    loadNextStatisticsBlock(SSttFileReader* pSttFileReader, STbStatisBlock* pStatisBlock,
                                       const TStatisBlkArray* pStatisBlkArray, int32_t numOfRows, int32_t* i, int32_t* j);
static int32_t doCheckTombBlock(STombBlock* pBlock, STsdbReader* pReader, int32_t numOfTables, int32_t* j,
                                ETombBlkCheckEnum* pRet) {
  int32_t     code = 0;
  STombRecord record = {0};

  uint64_t             uid = pReader->status.uidList.tableUidList[*j];
  STableBlockScanInfo* pScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, uid, pReader->idStr);
  if (pScanInfo->pFileDelData == NULL) {
    pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
  }

  for (int32_t k = 0; k < TARRAY2_SIZE(pBlock->suid); ++k) {
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
      pScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, uid, pReader->idStr);
      if (pScanInfo->pFileDelData == NULL) {
        pScanInfo->pFileDelData = taosArrayInit(4, sizeof(SDelData));
      }
    }

    if (record.uid < uid) {
      continue;
    }

    ASSERT(record.suid == pReader->info.suid && uid == record.uid);

    if (record.version <= pReader->info.verRange.maxVer) {
      SDelData delData = {.version = record.version, .sKey = record.skey, .eKey = record.ekey};
      taosArrayPush(pScanInfo->pFileDelData, &delData);
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

    ASSERT(pTombBlk->minTbid.suid <= pReader->info.suid && pTombBlk->maxTbid.suid >= pReader->info.suid);
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

void loadMemTombData(SArray** ppMemDelData, STbData* pMemTbData, STbData* piMemTbData, int64_t ver) {
  if (*ppMemDelData == NULL) {
    *ppMemDelData = taosArrayInit(4, sizeof(SDelData));
  }

  SArray* pMemDelData = *ppMemDelData;

  SDelData* p = NULL;
  if (pMemTbData != NULL) {
    taosRLockLatch(&pMemTbData->lock);
    p = pMemTbData->pHead;
    while (p) {
      if (p->version <= ver) {
        taosArrayPush(pMemDelData, p);
      }

      p = p->pNext;
    }
    taosRUnLockLatch(&pMemTbData->lock);
  }

  if (piMemTbData != NULL) {
    p = piMemTbData->pHead;
    while (p) {
      if (p->version <= ver) {
        taosArrayPush(pMemDelData, p);
      }
      p = p->pNext;
    }
  }
}

int32_t getNumOfRowsInSttBlock(SSttFileReader* pSttFileReader, SSttBlockLoadInfo* pBlockLoadInfo,
                               TStatisBlkArray* pStatisBlkArray, uint64_t suid, const uint64_t* pUidList,
                               int32_t numOfTables) {
  int32_t num = 0;

  if (TARRAY2_SIZE(pStatisBlkArray) <= 0) {
    return 0;
  }

  int32_t i = 0;
  while ((i < TARRAY2_SIZE(pStatisBlkArray)) && (pStatisBlkArray->data[i].maxTbid.suid < suid)) {
    ++i;
  }

  if (i >= TARRAY2_SIZE(pStatisBlkArray)) {
    return 0;
  }

  SStatisBlk*     p = &pStatisBlkArray->data[i];
  STbStatisBlock* pStatisBlock = taosMemoryCalloc(1, sizeof(STbStatisBlock));
  tStatisBlockInit(pStatisBlock);

  int64_t st = taosGetTimestampMs();
  tsdbSttFileReadStatisBlock(pSttFileReader, p, pStatisBlock);

  double el = (taosGetTimestampMs() - st) / 1000.0;
  pBlockLoadInfo->cost.loadStatisBlocks += 1;
  pBlockLoadInfo->cost.statisElapsedTime += el;

  int32_t index = 0;
  while (index < TARRAY2_SIZE(pStatisBlock->suid) && pStatisBlock->suid->data[index] < suid) {
    ++index;
  }

  if (index >= TARRAY2_SIZE(pStatisBlock->suid)) {
    tStatisBlockDestroy(pStatisBlock);
    taosMemoryFreeClear(pStatisBlock);
    return num;
  }

  int32_t j = index;
  int32_t uidIndex = 0;
  while (i < TARRAY2_SIZE(pStatisBlkArray) && uidIndex < numOfTables) {
    p = &pStatisBlkArray->data[i];
    if (p->minTbid.suid > suid) {
      tStatisBlockDestroy(pStatisBlock);
      taosMemoryFreeClear(pStatisBlock);
      return num;
    }

    uint64_t uid = pUidList[uidIndex];

    if (pStatisBlock->uid->data[j] == uid) {
      num += pStatisBlock->count->data[j];
      uidIndex += 1;
      j += 1;
      loadNextStatisticsBlock(pSttFileReader, pStatisBlock, pStatisBlkArray, pStatisBlock->suid->size, &i, &j);
    } else if (pStatisBlock->uid->data[j] < uid) {
      j += 1;
      loadNextStatisticsBlock(pSttFileReader, pStatisBlock, pStatisBlkArray, pStatisBlock->suid->size, &i, &j);
    } else {
      uidIndex += 1;
    }
  }

  tStatisBlockDestroy(pStatisBlock);
  taosMemoryFreeClear(pStatisBlock);
  return num;
}

// load next stt statistics block
static void loadNextStatisticsBlock(SSttFileReader* pSttFileReader, STbStatisBlock* pStatisBlock,
                                    const TStatisBlkArray* pStatisBlkArray, int32_t numOfRows, int32_t* i, int32_t* j) {
  if ((*j) >= numOfRows) {
    (*i) += 1;
    (*j) = 0;
    if ((*i) < TARRAY2_SIZE(pStatisBlkArray)) {
      tsdbSttFileReadStatisBlock(pSttFileReader, &pStatisBlkArray->data[(*i)], pStatisBlock);
    }
  }
}

void doAdjustValidDataIters(SArray* pLDIterList, int32_t numOfFileObj) {
  int32_t size = taosArrayGetSize(pLDIterList);

  if (size < numOfFileObj) {
    int32_t inc = numOfFileObj - size;
    for (int32_t k = 0; k < inc; ++k) {
      SLDataIter* pIter = taosMemoryCalloc(1, sizeof(SLDataIter));
      taosArrayPush(pLDIterList, &pIter);
    }
  } else if (size > numOfFileObj) {  // remove unused LDataIter
    int32_t inc = size - numOfFileObj;

    for (int i = 0; i < inc; ++i) {
      SLDataIter* pIter = taosArrayPop(pLDIterList);
      destroyLDataIter(pIter);
    }
  }
}

int32_t adjustSttDataIters(SArray* pSttFileBlockIterArray, STFileSet* pFileSet) {
  int32_t numOfLevels = pFileSet->lvlArr->size;

  // add the list/iter placeholder
  while (taosArrayGetSize(pSttFileBlockIterArray) < numOfLevels) {
    SArray* pList = taosArrayInit(4, POINTER_BYTES);
    taosArrayPush(pSttFileBlockIterArray, &pList);
  }

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SSttLvl* pSttLevel = pFileSet->lvlArr->data[j];
    SArray*  pList = taosArrayGetP(pSttFileBlockIterArray, j);
    doAdjustValidDataIters(pList, TARRAY2_SIZE(pSttLevel->fobjArr));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbGetRowsInSttFiles(STFileSet* pFileSet, SArray* pSttFileBlockIterArray, STsdb* pTsdb, SMergeTreeConf* pConf,
                              const char* pstr) {
  int32_t numOfRows = 0;

  // no data exists, go to end
  int32_t numOfLevels = pFileSet->lvlArr->size;
  if (numOfLevels == 0) {
    return numOfRows;
  }

  // add the list/iter placeholder
  adjustSttDataIters(pSttFileBlockIterArray, pFileSet);

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
        int32_t     code = tsdbSttFileReaderOpen(pName, &conf, &pIter->pReader);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("open stt file reader error. file:%s, code %s, %s", pName, tstrerror(code), pstr);
          continue;
        }
      }

      if (pIter->pBlockLoadInfo == NULL) {
        pIter->pBlockLoadInfo = tCreateSttBlockLoadInfo(pConf->pSchema, pConf->pCols, pConf->numOfCols);
      }

      // load stt blocks statis for all stt-blocks, to decide if the data of queried table exists in current stt file
      TStatisBlkArray* pStatisBlkArray = NULL;
      int32_t          code = tsdbSttFileReadStatisBlk(pIter->pReader, (const TStatisBlkArray**)&pStatisBlkArray);
      if (code != TSDB_CODE_SUCCESS) {
        tsdbError("failed to load stt block statistics, code:%s, %s", tstrerror(code), pstr);
        continue;
      }

      // extract rows from each stt file one-by-one
      STsdbReader* pReader = pConf->pReader;
      int32_t      numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
      uint64_t*    pUidList = pReader->status.uidList.tableUidList;
      numOfRows += getNumOfRowsInSttBlock(pIter->pReader, pIter->pBlockLoadInfo, pStatisBlkArray, pConf->suid, pUidList,
                                          numOfTables);
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
  const STimeWindow* px1 = p1;
  const STimeWindow* px2 = p2;
  if (px1->skey == px2->skey) {
    return 0;
  } else {
    return px1->skey < px2->skey ? -1 : 1;
  }
}

bool isCleanSttBlock(SArray* pTimewindowList, STimeWindow* pQueryWindow, STableBlockScanInfo* pScanInfo,
                     int32_t order) {
  // check if it overlap with del skyline
  taosArraySort(pTimewindowList, sortUidComparFn);

  int32_t num = taosArrayGetSize(pTimewindowList);
  if (num == 0) {
    return false;
  }

  STimeWindow* p = taosArrayGet(pTimewindowList, 0);
  if (overlapWithTimeWindow(p, pQueryWindow, pScanInfo, order)) {
    return false;
  }

  for (int32_t i = 0; i < num - 1; ++i) {
    STimeWindow* p1 = taosArrayGet(pTimewindowList, i);
    STimeWindow* p2 = taosArrayGet(pTimewindowList, i + 1);

    if (p1->ekey >= p2->skey) {
      return false;
    }

    bool overlap = overlapWithTimeWindow(p2, pQueryWindow, pScanInfo, order);
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
    if (p->ts >= pRecord->firstKey && p->ts <= pRecord->lastKey) {
      if (p->version >= pRecord->minVer) {
        return true;
      }
    } else if (p->ts < pRecord->firstKey) {  // p->ts < pBlock->minKey.ts
      if (p->version >= pRecord->minVer) {
        if (i < num - 1) {
          TSDBKEY* pnext = taosArrayGet(pBlockScanInfo->delSkyline, i + 1);
          if (pnext->ts >= pRecord->firstKey) {
            return true;
          }
        } else {  // it must be the last point
          ASSERT(p->version == 0);
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
    if (p->ts >= pRecord->firstKey && p->ts <= pRecord->lastKey) {
      return true;
    } else if (p->ts < pRecord->firstKey) {  // p->ts < pBlock->minKey.ts
      if (i < num - 1) {
        TSDBKEY* pnext = taosArrayGet(pBlockScanInfo->delSkyline, i + 1);
        if (pnext->ts >= pRecord->firstKey) {
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
  if (pRecord->firstKey > pLast->ts || pRecord->lastKey < pFirst->ts) {
    return false;
  }

  // version is not overlap
  if (ASCENDING_TRAVERSE(order)) {
    return doCheckDatablockOverlap(pBlockScanInfo, pRecord, pBlockScanInfo->fileDelIndex);
  } else {
    int32_t index = pBlockScanInfo->fileDelIndex;
    while (1) {
      TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, index);
      if (p->ts > pRecord->firstKey && index > 0) {
        index -= 1;
      } else {  // find the first point that is smaller than the minKey.ts of dataBlock.
        if (p->ts == pRecord->firstKey && p->version < pRecord->maxVer && index > 0) {
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
  if (pRecord->firstKey > pLast->ts || pRecord->lastKey < pFirst->ts) {
    return false;
  }

  // version is not overlap
  if (ASCENDING_TRAVERSE(order)) {
    return doCheckDatablockOverlapWithoutVersion(pBlockScanInfo, pRecord, pBlockScanInfo->fileDelIndex);
  } else {
    int32_t index = pBlockScanInfo->fileDelIndex;
    while (1) {
      TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, index);
      if (p->ts > pRecord->firstKey && index > 0) {
        index -= 1;
      } else {  // find the first point that is smaller than the minKey.ts of dataBlock.
        if (p->ts == pRecord->firstKey && index > 0) {
          index -= 1;
        }
        break;
      }
    }

    return doCheckDatablockOverlapWithoutVersion(pBlockScanInfo, pRecord, index);
  }
}