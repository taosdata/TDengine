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

typedef struct SDataDispatchBuf {
  int32_t useSize;
  int32_t allocSize;
  char*   pData;
} SDataDispatchBuf;

typedef struct SDataCacheEntry {
  int32_t dataLen;
  int32_t numOfRows;
  int32_t numOfCols;
  int8_t  compressed;
  char    data[];
} SDataCacheEntry;

typedef struct SDataDispatchHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  SDataBlockDescNode* pSchema;
  STaosQueue*         pDataBlocks;
  SDataDispatchBuf    nextOutput;
  int32_t             status;
  bool                queryEnd;
  uint64_t            useconds;
  uint64_t            cachedSize;
  TdThreadMutex       mutex;
} SDataDispatchHandle;

// clang-format off
// data format:
// +----------------+------------------+--------------+--------------+------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// |SDataCacheEntry |  version         | total length | numOfRows    |     group id     | col1_schema | col2_schema | col3_schema... | column#1 length, column#2 length...| col1 bitmap | col1 data | col2 bitmap | col2 data |
// |                |  sizeof(int32_t) |sizeof(int32) | sizeof(int32)| sizeof(uint64_t) | (sizeof(int8_t)+sizeof(int32_t))*numOfCols | sizeof(int32_t) * numOfCols        | actual size |           |                         |
// +----------------+------------------+--------------+--------------+------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
// recorded in the first segment, next to the struct header
// clang-format on
static void toDataCacheEntry(SDataDispatchHandle* pHandle, const SInputData* pInput, SDataDispatchBuf* pBuf) {
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

static bool allocBuf(SDataDispatchHandle* pDispatcher, const SInputData* pInput, SDataDispatchBuf* pBuf) {
  /*
    uint32_t capacity = pDispatcher->pManager->cfg.maxDataBlockNumPerQuery;
    if (taosQueueItemSize(pDispatcher->pDataBlocks) > capacity) {
      qError("SinkNode queue is full, no capacity, max:%d, current:%d, no capacity", capacity,
             taosQueueItemSize(pDispatcher->pDataBlocks));
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

static int32_t updateStatus(SDataDispatchHandle* pDispatcher) {
  taosThreadMutexLock(&pDispatcher->mutex);
  int32_t blockNums = taosQueueItemSize(pDispatcher->pDataBlocks);
  int32_t status =
      (0 == blockNums ? DS_BUF_EMPTY
                      : (blockNums < pDispatcher->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pDispatcher->status = status;
  taosThreadMutexUnlock(&pDispatcher->mutex);
  return status;
}

static int32_t getStatus(SDataDispatchHandle* pDispatcher) {
  taosThreadMutexLock(&pDispatcher->mutex);
  int32_t status = pDispatcher->status;
  taosThreadMutexUnlock(&pDispatcher->mutex);
  return status;
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  int32_t              code = 0;
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  SDataDispatchBuf*    pBuf = taosAllocateQitem(sizeof(SDataDispatchBuf), DEF_QITEM, 0);
  if (NULL == pBuf) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  if (!allocBuf(pDispatcher, pInput, pBuf)) {
    taosFreeQitem(pBuf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  toDataCacheEntry(pDispatcher, pInput, pBuf);
  code = taosWriteQitem(pDispatcher->pDataBlocks, pBuf);
  if (code != 0) {
    return code;
  }

  int32_t status = updateStatus(pDispatcher);
  *pContinue = (status == DS_BUF_LOW || status == DS_BUF_EMPTY);
  return TSDB_CODE_SUCCESS;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  taosThreadMutexLock(&pDispatcher->mutex);
  pDispatcher->queryEnd = true;
  pDispatcher->useconds = useconds;
  taosThreadMutexUnlock(&pDispatcher->mutex);
}

static void resetDispatcher(struct SDataSinkHandle* pHandle) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  taosThreadMutexLock(&pDispatcher->mutex);
  pDispatcher->queryEnd = false;
  taosThreadMutexUnlock(&pDispatcher->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int64_t* pLen, bool* pQueryEnd) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  if (taosQueueEmpty(pDispatcher->pDataBlocks)) {
    *pQueryEnd = pDispatcher->queryEnd;
    *pLen = 0;
    return;
  }

  SDataDispatchBuf* pBuf = NULL;
  taosReadQitem(pDispatcher->pDataBlocks, (void**)&pBuf);
  if (pBuf != NULL) {
    memcpy(&pDispatcher->nextOutput, pBuf, sizeof(SDataDispatchBuf));
    taosFreeQitem(pBuf);
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pDispatcher->nextOutput.pData;
  *pLen = pEntry->dataLen;

  //  ASSERT(pEntry->numOfRows == *(int32_t*)(pEntry->data + 8));
  //  ASSERT(pEntry->numOfCols == *(int32_t*)(pEntry->data + 8 + 4));

  *pQueryEnd = pDispatcher->queryEnd;
  qDebug("got data len %" PRId64 ", row num %d in sink", *pLen,
         ((SDataCacheEntry*)(pDispatcher->nextOutput.pData))->numOfRows);
}


static int32_t getDataBlock(SDataSinkHandle* pHandle, SOutputData* pOutput) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  if (NULL == pDispatcher->nextOutput.pData) {
    ASSERT(pDispatcher->queryEnd);
    pOutput->useconds = pDispatcher->useconds;
    pOutput->precision = pDispatcher->pSchema->precision;
    pOutput->bufStatus = DS_BUF_EMPTY;
    pOutput->queryEnd = pDispatcher->queryEnd;
    return TSDB_CODE_SUCCESS;
  }
  SDataCacheEntry* pEntry = (SDataCacheEntry*)(pDispatcher->nextOutput.pData);
  memcpy(pOutput->pData, pEntry->data, pEntry->dataLen);
  pOutput->numOfRows = pEntry->numOfRows;
  pOutput->numOfCols = pEntry->numOfCols;
  pOutput->compressed = pEntry->compressed;

  atomic_sub_fetch_64(&pDispatcher->cachedSize, pEntry->dataLen);
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  taosMemoryFreeClear(pDispatcher->nextOutput.pData);  // todo persistent
  pOutput->bufStatus = updateStatus(pDispatcher);
  taosThreadMutexLock(&pDispatcher->mutex);
  pOutput->queryEnd = pDispatcher->queryEnd;
  pOutput->useconds = pDispatcher->useconds;
  pOutput->precision = pDispatcher->pSchema->precision;
  taosThreadMutexUnlock(&pDispatcher->mutex);

  return TSDB_CODE_SUCCESS;
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pDispatcher->cachedSize);
  taosMemoryFreeClear(pDispatcher->nextOutput.pData);
  while (!taosQueueEmpty(pDispatcher->pDataBlocks)) {
    SDataDispatchBuf* pBuf = NULL;
    taosReadQitem(pDispatcher->pDataBlocks, (void**)&pBuf);
    if (pBuf != NULL) {
      taosMemoryFreeClear(pBuf->pData);
      taosFreeQitem(pBuf);
    }
  }
  taosCloseQueue(pDispatcher->pDataBlocks);
  taosThreadMutexDestroy(&pDispatcher->mutex);
  taosMemoryFree(pDispatcher->pManager);
  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataDispatcher(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle) {
  SDataDispatchHandle* dispatcher = taosMemoryCalloc(1, sizeof(SDataDispatchHandle));
  if (NULL == dispatcher) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  dispatcher->sink.fPut = putDataBlock;
  dispatcher->sink.fEndPut = endPut;
  dispatcher->sink.fReset = resetDispatcher;
  dispatcher->sink.fGetLen = getDataLength;
  dispatcher->sink.fGetData = getDataBlock;
  dispatcher->sink.fDestroy = destroyDataSinker;
  dispatcher->sink.fGetCacheSize = getCacheSize;
  dispatcher->pManager = pManager;
  dispatcher->pSchema = pDataSink->pInputDataBlockDesc;
  dispatcher->status = DS_BUF_EMPTY;
  dispatcher->queryEnd = false;
  dispatcher->pDataBlocks = taosOpenQueue();
  taosThreadMutexInit(&dispatcher->mutex, NULL);
  if (NULL == dispatcher->pDataBlocks) {
    taosMemoryFree(dispatcher);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _return;
  }
  *pHandle = dispatcher;
  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFree(pManager);
  return terrno;
}
