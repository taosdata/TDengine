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

#include "dataSinkInt.h"
#include "dataSinkMgt.h"
#include "executorInt.h"
#include "planner.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tqueue.h"

extern SDataSinkStat gDataSinkStat;

typedef struct SSfCacheEntry {
  int32_t dataLen;
  int32_t numOfRows;
  int32_t numOfCols;
  int8_t  compressed;
  char    data[];
} SSfCacheEntry;

typedef struct SSfBucketInfo {
  TdThreadMutex    mutex;
  int8_t           status;
  bool             lastBlkLocked;
  SArray*          pBlockList;
  SArray*          pRowIdxList;
  SSfCacheEntry*   pCurrent;
} SSfBucketInfo;

typedef struct SDataShuffleHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  SDataBlockDescNode* pSchema;
  SSlotDescNode*      pSlot;
  _hash_fn_t          hashFp;
  int32_t             numOfCols;
  int32_t             status;
  int32_t             bucketNum;
  int32_t             maxBlockPerBucket;
  SSfBucketInfo*      pBuckets;
  int16_t*            pBucketIdx;
  int32_t             bucketMapSize;
  char*               pBucketMap;
  bool                queryEnd;
  uint64_t            useconds;
  uint64_t            cachedSize;
  TdThreadMutex       mutex;
} SDataShuffleHandle;

// clang-format off
// data format:
// +----------------+------------------+--------------+--------------+------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// |SDataCacheEntry |  version         | total length | numOfRows    |     group id     | col1_schema | col2_schema | col3_schema... | column#1 length, column#2 length...| col1 bitmap | col1 data | col2 bitmap | col2 data | .... |                |  (4 bytes)   |(8 bytes)
// |                |  sizeof(int32_t) |sizeof(int32) | sizeof(int32)| sizeof(uint64_t) | (sizeof(int8_t)+sizeof(int32_t))*numOfCols | sizeof(int32_t) * numOfCols        | actual size |           |
// +----------------+------------------+--------------+--------------+------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
// recorded in the first segment, next to the struct header
// clang-format on
static void sfToDataCacheEntry(SDataShuffleHandle* pHandle, const SInputData* pInput, SDataShuffleBuf* pBuf) {
  int32_t numOfCols = 0;
  SNode*  pNode;

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pBuf->pData;
  pEntry->compressed = 0;
  pEntry->numOfRows = pInput->pData->info.rows;
  pEntry->numOfCols = numOfCols;
  pEntry->dataLen = 0;

  pBuf->useSize = sizeof(SDataCacheEntry);
  pEntry->dataLen = blockEncode(pInput->pData, pEntry->data, numOfCols);
  //  ASSERT(pEntry->numOfRows == *(int32_t*)(pEntry->data + 8));
  //  ASSERT(pEntry->numOfCols == *(int32_t*)(pEntry->data + 8 + 4));

  pBuf->useSize += pEntry->dataLen;

  atomic_add_fetch_64(&pHandle->cachedSize, pEntry->dataLen);
  atomic_add_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);
}

static bool sfAllocBuf(SDataShuffleHandle* pShuffler, const SInputData* pInput, SDataShuffleBuf* pBuf) {
  /*
    uint32_t capacity = pShuffler->pManager->cfg.maxDataBlockNumPerQuery;
    if (taosQueueItemSize(pShuffler->pDataBlocks) > capacity) {
      qError("SinkNode queue is full, no capacity, max:%d, current:%d, no capacity", capacity,
             taosQueueItemSize(pShuffler->pDataBlocks));
      return false;
    }
  */

  pBuf->allocSize = sizeof(SDataCacheEntry) + blockGetEncodeSize(pInput->pData);

  pBuf->pData = taosMemoryMalloc(pBuf->allocSize);
  if (pBuf->pData == NULL) {
    qError("SinkNode failed to malloc memory, size:%d, code:%d", pBuf->allocSize, TAOS_SYSTEM_ERROR(errno));
  }

  return NULL != pBuf->pData;
}

static int32_t sfUpdateStatus(SDataShuffleHandle* pShuffler) {
  taosThreadMutexLock(&pShuffler->mutex);
  int32_t blockNums = taosQueueItemSize(pShuffler->pDataBlocks);
  int32_t status =
      (0 == blockNums ? DS_BUF_EMPTY
                      : (blockNums < pShuffler->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pShuffler->status = status;
  taosThreadMutexUnlock(&pShuffler->mutex);
  return status;
}

static FORCE_INLINE int16_t sfGetColDataBucketIdx(SDataShuffleHandle* pShuffler, SColumnInfoData* pCol, int32_t i) {
  void* pData = colDataGetData(pCol, i);
  return (*pShuffler->hashFp)(pData, colDataGetLen(pCol, pData)) % pShuffler->bucketNum;
}

static int32_t sfAcquireBucketLastBlock(SDataShuffleHandle* pShuffler, SSfBucketInfo* pBucket, SSDataBlock* pSrc, int32_t rowNum, SSDataBlock** ppRes, bool* bucketFull) {
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pBlock = NULL;
  
  atomic_store_8((int8_t*)pBucket->lastBlkLocked, 1);
  taosThreadMutexLock(&pBucket->mutex);
  int32_t blockNum = taosArrayGetSize(pBucket->pBlockList);
  if (blockNum > 0) {
    pBlock = taosArrayGetP(pBucket->pBlockList, blockNum - 1);
    if ((pBlock->info.rows + rowNum) > 4096) {
      pBlock = NULL;
    }
  }
  if (NULL == pBlock) {
    code = cloneEmptyBlockFromBlock(&pBlock, pSrc);
    if (code) {
      taosThreadMutexUnlock(&pBucket->mutex);
      return code;
    }
    taosArrayPush(pBucket->pBlockList, &pBlock);
  }
  taosThreadMutexUnlock(&pBucket->mutex);

  code = blockDataEnsureCapacity(pBlock, pBlock->info.rows + rowNum);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  *bucketFull = taosArrayGetSize(pBucket->pBlockList) >= pShuffler->maxBlockPerBucket;
  *ppRes = pBlock;

  return code;
}

static FORCE_INLINE void sfReleaseBucketLastBlock(SSfBucketInfo* pBucket) {
  atomic_store_8((int8_t*)pBucket->lastBlkLocked, 0);
}

static int32_t sfPutDataBlockImpl(SDataShuffleHandle* pShuffler, SSfBucketInfo* pBucket, SSDataBlock* pSrc, bool* bucketFull) {
  int32_t code = TSDB_CODE_SUCCESS;
  SSDataBlock* pBlock = NULL;
  int32_t rowNum = taosArrayGetSize(pBucket->pRowIdxList);
  int32_t colNum = taosArrayGetSize(pSrc->pDataBlock);
  code = sfAcquireBucketLastBlock(pShuffler, pBucket, pSrc, rowNum, &pBlock, bucketFull);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  int32_t* rowIdx = NULL;
  int32_t capacity = 0;
  for (int32_t i = 0; i < rowNum; ++i) {
    rowIdx = taosArrayGet(pBucket->pRowIdxList, i);
    for (int32_t n = 0; n < colNum; ++n) {
      SColumnInfoData* pCol2 = taosArrayGet(pBlock->pDataBlock, n);
      SColumnInfoData* pCol1 = taosArrayGet(pSrc->pDataBlock, n);
      
      colDataSetVal(pCol2, pBlock->info.rows, colDataGetData(pCol1, rowIdx), colDataIsNull_s(pCol1, rowIdx));
      pBlock->info.rows++;
    }
  }
  
  sfReleaseBucketLastBlock(pBucket);

  return code;
}

static int32_t sfPutDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  int32_t code = TSDB_CODE_SUCCESS;
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  int16_t idx = 0;
  int32_t bucketIdx = 0;

  memset(pShuffler->pBucketMap, 0, pShuffler->bucketMapSize);
  
  SColumnInfoData* pCol = taosArrayGet(pInput->pData->pDataBlock, pShuffler->pSlot->slotId);
  for (int32_t i = 0; i < pInput->pData->info.rows; ++i) {
    idx = sfGetColDataBucketIdx(pShuffler, pCol, i);
    taosArrayPush(pShuffler->pBuckets[idx].pRowIdxList, &i);
    if (colDataIsNull_f(pShuffler->pBucketMap, idx)) {
      continue;
    }
    colDataSetNull_f(pShuffler->pBucketMap, idx);
    pShuffler->pBucketIdx[bucketIdx++] = idx;
  }

  SSfBucketInfo* pBucket = NULL;
  bool full = false;
  bool bucketFull = false;
  for (int32_t i = 0; i < bucketIdx; ++i) {
    pBucket = &pShuffler->pBuckets[i];
    code = sfPutDataBlockImpl(pShuffler, pBucket, pInput->pData, &bucketFull);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    if (bucketFull) {
      full = true;
    }
  }

  *pContinue = !full;
  
  return TSDB_CODE_SUCCESS;
}

static int32_t sfGetCacheEntry(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  int32_t              code = 0;
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;

  if (!sfAllocBuf(pShuffler, pInput, pBuf)) {
    taosFreeQitem(pBuf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  sfToDataCacheEntry(pShuffler, pInput, pBuf);
  code = taosWriteQitem(pShuffler->pDataBlocks, pBuf);
  if (code != 0) {
    return code;
  }

  int32_t status = sfUpdateStatus(pShuffler);
  *pContinue = (status == DS_BUF_LOW || status == DS_BUF_EMPTY);
  return TSDB_CODE_SUCCESS;
}


static void sfEndPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  taosThreadMutexLock(&pShuffler->mutex);
  pShuffler->queryEnd = true;
  pShuffler->useconds = useconds;
  taosThreadMutexUnlock(&pShuffler->mutex);
}

static void sfReset(struct SDataSinkHandle* pHandle) {
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  taosThreadMutexLock(&pShuffler->mutex);
  pShuffler->queryEnd = false;
  taosThreadMutexUnlock(&pShuffler->mutex);
}

static void sfGetDataLength(SDataSinkHandle* pHandle, int32_t bucketId, int64_t* pLen, bool* pQueryEnd) {
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  if (taosQueueEmpty(pShuffler->pDataBlocks)) {
    *pQueryEnd = pShuffler->queryEnd;
    *pLen = 0;
    return;
  }

  SDataShuffleBuf* pBuf = NULL;
  taosReadQitem(pShuffler->pDataBlocks, (void**)&pBuf);
  if (pBuf != NULL) {
    memcpy(&pShuffler->nextOutput, pBuf, sizeof(SDataShuffleBuf));
    taosFreeQitem(pBuf);
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pShuffler->nextOutput.pData;
  *pLen = pEntry->dataLen;

  //  ASSERT(pEntry->numOfRows == *(int32_t*)(pEntry->data + 8));
  //  ASSERT(pEntry->numOfCols == *(int32_t*)(pEntry->data + 8 + 4));

  *pQueryEnd = pShuffler->queryEnd;
  qDebug("got data len %" PRId64 ", row num %d in sink", *pLen,
         ((SDataCacheEntry*)(pShuffler->nextOutput.pData))->numOfRows);
}


static int32_t sfGetDataBlock(SDataSinkHandle* pHandle, SOutputData* pOutput) {
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  if (NULL == pShuffler->nextOutput.pData) {
    ASSERT(pShuffler->queryEnd);
    pOutput->useconds = pShuffler->useconds;
    pOutput->precision = pShuffler->pSchema->precision;
    pOutput->bufStatus = DS_BUF_EMPTY;
    pOutput->queryEnd = pShuffler->queryEnd;
    return TSDB_CODE_SUCCESS;
  }
  SDataCacheEntry* pEntry = (SDataCacheEntry*)(pShuffler->nextOutput.pData);
  memcpy(pOutput->pData, pEntry->data, pEntry->dataLen);
  pOutput->numOfRows = pEntry->numOfRows;
  pOutput->numOfCols = pEntry->numOfCols;
  pOutput->compressed = pEntry->compressed;

  atomic_sub_fetch_64(&pShuffler->cachedSize, pEntry->dataLen);
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  taosMemoryFreeClear(pShuffler->nextOutput.pData);  // todo persistent
  pOutput->bufStatus = sfUpdateStatus(pShuffler);
  taosThreadMutexLock(&pShuffler->mutex);
  pOutput->queryEnd = pShuffler->queryEnd;
  pOutput->useconds = pShuffler->useconds;
  pOutput->precision = pShuffler->pSchema->precision;
  taosThreadMutexUnlock(&pShuffler->mutex);

  return TSDB_CODE_SUCCESS;
}

static int32_t destroyDataShuffler(SDataSinkHandle* pHandle) {
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pShuffler->cachedSize);
  if (NULL != pShuffler->pBuckets) {
    for (int32_t i = 0; i < pShuffler->bucketNum; ++i) {
      if (pShuffler->pBuckets[i].pBlockList) {
        taosArrayDestroy(pShuffler->pBuckets[i].pBlockList);
        taosThreadMutexDestroy(&pShuffler->pBuckets[i].mutex);
      }
      taosMemoryFree(pShuffler->pBuckets[i].pCurrent);
      taosArrayDestroy(pShuffler->pBuckets[i].pRowIdxList);
    }
  }
  taosMemoryFree(pShuffler->pBucketIdx);
  taosThreadMutexDestroy(&pShuffler->mutex);
  taosMemoryFree(pShuffler->pManager);
  return TSDB_CODE_SUCCESS;
}

static int32_t sfGetCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;

  *size = atomic_load_64(&pShuffler->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataShuffler(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle) {
  int32_t code = TSDB_CODE_SUCCESS;
  SDataShufflerNode* pNode = (SDataShufflerNode*)pDataSink;
  SDataShuffleHandle* shuffler = taosMemoryCalloc(1, sizeof(SDataShuffleHandle));
  if (NULL == shuffler) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }

  taosThreadMutexInit(&shuffler->mutex, NULL);
  shuffler->pManager = pManager;
  shuffler->pBuckets = taosMemoryCalloc(pNode->bucketNum, sizeof(*shuffler->pBuckets));
  if (NULL == shuffler->pBuckets) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  shuffler->pBucketIdx = taosMemoryMalloc(pNode->bucketNum * sizeof(*shuffler->pBucketIdx));
  if (NULL == shuffler->pBucketIdx) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  shuffler->bucketMapSize = BitmapLen(pNode->bucketNum);
  shuffler->pBucketMap = taosMemoryMalloc(shuffler->bucketMapSize);
  if (NULL == shuffler->pBucketMap) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  shuffler->maxBlockPerBucket = TMAX(pManager->cfg.maxDataBlockNumPerQuery / pNode->bucketNum, 3);
  for (int32_t i = 0; i < pNode->bucketNum; ++i) {
    shuffler->pBuckets[i].pBlockList = taosArrayInit(shuffler->maxBlockPerBucket, POINTER_BYTES);
    if (NULL == shuffler->pBuckets[i].pBlockList) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _return;
    }
    taosThreadMutexInit(&shuffler->pBuckets[i].mutex, NULL);
    shuffler->pBuckets[i].pRowIdxList = taosArrayInit(4096/pNode->bucketNum, sizeof(int32_t));
    if (NULL == shuffler->pBuckets[i].pRowIdxList) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _return;
    }
  }
  shuffler->hashFp = taosGetDefaultHashFunction(pNode->pSlot->dataType.type);
  shuffler->sink.fPut = sfPutDataBlock;
  shuffler->sink.fEndPut = sfEndPut;
  shuffler->sink.fReset = sfReset;
  shuffler->sink.fGetLen = sfGetDataLength;
  shuffler->sink.fGetData = sfGetDataBlock;
  shuffler->sink.fDestroy = destroyDataShuffler;
  shuffler->sink.fGetCacheSize = sfGetCacheSize;
  shuffler->pSlot = pNode->pSlot;
  shuffler->bucketNum = pNode->bucketNum;
  shuffler->pSchema = pDataSink->pInputDataBlockDesc;
  shuffler->status = DS_BUF_EMPTY;
  shuffler->queryEnd = false;
  shuffler->numOfCols = 0;
  FOREACH(pNode, shuffler->pSchema->pSlots) {
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->output) {
      ++shuffler->numOfCols;
    }
  }
  
  *pHandle = shuffler;
  return TSDB_CODE_SUCCESS;

_return:

  if (shuffler) {
    destroyDataShuffler(shuffler);
  } else {
    taosMemoryFree(pManager);
  }
  terrno = code;
  return code;
}
