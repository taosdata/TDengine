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

typedef struct SDataShuffleBuf {
  int32_t useSize;
  int32_t allocSize;
  char*   pData;
} SDataShuffleBuf;

typedef struct SDataCacheEntry {
  int32_t dataLen;
  int32_t numOfRows;
  int32_t numOfCols;
  int8_t  compressed;
  char    data[];
} SDataCacheEntry;

typedef struct SDataShuffleHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  SDataBlockDescNode* pSchema;
  STaosQueue*         pDataBlocks;
  SDataShuffleBuf     nextOutput;
  int32_t             status;
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
static void toDataCacheEntry(SDataShuffleHandle* pHandle, const SInputData* pInput, SDataShuffleBuf* pBuf) {
  int32_t numOfCols = 0;
  SNode*  pNode;
  FOREACH(pNode, pHandle->pSchema->pSlots) {
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->output) {
      ++numOfCols;
    }
  }
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

static bool allocBuf(SDataShuffleHandle* pShuffler, const SInputData* pInput, SDataShuffleBuf* pBuf) {
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

static int32_t getStatus(SDataShuffleHandle* pShuffler) {
  taosThreadMutexLock(&pShuffler->mutex);
  int32_t status = pShuffler->status;
  taosThreadMutexUnlock(&pShuffler->mutex);
  return status;
}

static int32_t sfPutDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  int32_t              code = 0;
  SDataShuffleHandle* pShuffler = (SDataShuffleHandle*)pHandle;
  SDataShuffleBuf*    pBuf = taosAllocateQitem(sizeof(SDataShuffleBuf), DEF_QITEM, 0);
  if (NULL == pBuf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (!allocBuf(pShuffler, pInput, pBuf)) {
    taosFreeQitem(pBuf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  toDataCacheEntry(pShuffler, pInput, pBuf);
  code = taosWriteQitem(pShuffler->pDataBlocks, pBuf);
  if (code != 0) {
    return code;
  }

  int32_t status = updateStatus(pShuffler);
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

static void sfGetDataLength(SDataSinkHandle* pHandle, int64_t* pLen, bool* pQueryEnd) {
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
  taosMemoryFreeClear(pShuffler->nextOutput.pData);
  while (!taosQueueEmpty(pShuffler->pDataBlocks)) {
    SDataShuffleBuf* pBuf = NULL;
    taosReadQitem(pShuffler->pDataBlocks, (void**)&pBuf);
    if (pBuf != NULL) {
      taosMemoryFreeClear(pBuf->pData);
      taosFreeQitem(pBuf);
    }
  }
  taosCloseQueue(pShuffler->pDataBlocks);
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
  SDataShuffleHandle* shuffler = taosMemoryCalloc(1, sizeof(SDataShuffleHandle));
  if (NULL == shuffler) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  shuffler->sink.fPut = sfPutDataBlock;
  shuffler->sink.fEndPut = sfEndPut;
  shuffler->sink.fReset = sfReset;
  shuffler->sink.fGetLen = sfGetDataLength;
  shuffler->sink.fGetData = sfGetDataBlock;
  shuffler->sink.fDestroy = destroyDataShuffler;
  shuffler->sink.fGetCacheSize = sfGetCacheSize;
  shuffler->pManager = pManager;
  shuffler->pSchema = pDataSink->pInputDataBlockDesc;
  shuffler->status = DS_BUF_EMPTY;
  shuffler->queryEnd = false;
  shuffler->pDataBlocks = taosOpenQueue();
  taosThreadMutexInit(&shuffler->mutex, NULL);
  if (NULL == shuffler->pDataBlocks) {
    taosMemoryFree(shuffler);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  *pHandle = shuffler;
  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFree(pManager);
  return terrno;
}
