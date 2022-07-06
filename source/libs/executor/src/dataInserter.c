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
#include "executorimpl.h"
#include "planner.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tglobal.h"
#include "tqueue.h"

extern SDataSinkStat gDataSinkStat;

typedef struct SDataInserterBuf {
  int32_t useSize;
  int32_t allocSize;
  char*   pData;
} SDataInserterBuf;

typedef struct SDataCacheEntry {
  int32_t dataLen;
  int32_t numOfRows;
  int32_t numOfCols;
  int8_t  compressed;
  char    data[];
} SDataCacheEntry;

typedef struct SDataInserterHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  SDataBlockDescNode* pSchema;
  SDataDeleterNode*   pDeleter;
  SDeleterParam*      pParam;
  STaosQueue*         pDataBlocks;
  SDataInserterBuf    nextOutput;
  int32_t             status;
  bool                queryEnd;
  uint64_t            useconds;
  uint64_t            cachedSize;
  TdThreadMutex       mutex;
} SDataInserterHandle;

static bool needCompress(const SSDataBlock* pData, int32_t numOfCols) {
  if (tsCompressColData < 0 || 0 == pData->info.rows) {
    return false;
  }

  for (int32_t col = 0; col < numOfCols; ++col) {
    SColumnInfoData* pColRes = taosArrayGet(pData->pDataBlock, col);
    int32_t          colSize = pColRes->info.bytes * pData->info.rows;
    if (NEEDTO_COMPRESS_QUERY(colSize)) {
      return true;
    }
  }

  return false;
}

static void toDataCacheEntry(SDataInserterHandle* pHandle, const SInputData* pInput, SDataInserterBuf* pBuf) {
  int32_t numOfCols = LIST_LENGTH(pHandle->pSchema->pSlots);

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pBuf->pData;
  pEntry->compressed = 0;
  pEntry->numOfRows = pInput->pData->info.rows;
  pEntry->numOfCols = taosArrayGetSize(pInput->pData->pDataBlock);
  pEntry->dataLen = sizeof(SDeleterRes);

  ASSERT(1 == pEntry->numOfRows);
  ASSERT(1 == pEntry->numOfCols);

  pBuf->useSize = sizeof(SDataCacheEntry);

  SColumnInfoData* pColRes = (SColumnInfoData*)taosArrayGet(pInput->pData->pDataBlock, 0);

  SDeleterRes* pRes = (SDeleterRes*)pEntry->data;
  pRes->suid = pHandle->pParam->suid;
  pRes->uidList = pHandle->pParam->pUidList;
  pRes->skey = pHandle->pDeleter->deleteTimeRange.skey;
  pRes->ekey = pHandle->pDeleter->deleteTimeRange.ekey;
  pRes->affectedRows = *(int64_t*)pColRes->pData;

  pBuf->useSize += pEntry->dataLen;
  
  atomic_add_fetch_64(&pHandle->cachedSize, pEntry->dataLen); 
  atomic_add_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen); 
}

static bool allocBuf(SDataInserterHandle* pDeleter, const SInputData* pInput, SDataInserterBuf* pBuf) {
  uint32_t capacity = pDeleter->pManager->cfg.maxDataBlockNumPerQuery;
  if (taosQueueItemSize(pDeleter->pDataBlocks) > capacity) {
    qError("SinkNode queue is full, no capacity, max:%d, current:%d, no capacity", capacity,
           taosQueueItemSize(pDeleter->pDataBlocks));
    return false;
  }

  pBuf->allocSize = sizeof(SDataCacheEntry) + sizeof(SDeleterRes);

  pBuf->pData = taosMemoryMalloc(pBuf->allocSize);
  if (pBuf->pData == NULL) {
    qError("SinkNode failed to malloc memory, size:%d, code:%d", pBuf->allocSize, TAOS_SYSTEM_ERROR(errno));
  }

  return NULL != pBuf->pData;
}

static int32_t updateStatus(SDataInserterHandle* pDeleter) {
  taosThreadMutexLock(&pDeleter->mutex);
  int32_t blockNums = taosQueueItemSize(pDeleter->pDataBlocks);
  int32_t status =
      (0 == blockNums ? DS_BUF_EMPTY
                      : (blockNums < pDeleter->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pDeleter->status = status;
  taosThreadMutexUnlock(&pDeleter->mutex);
  return status;
}

static int32_t getStatus(SDataInserterHandle* pDeleter) {
  taosThreadMutexLock(&pDeleter->mutex);
  int32_t status = pDeleter->status;
  taosThreadMutexUnlock(&pDeleter->mutex);
  return status;
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  SDataInserterHandle* pDeleter = (SDataInserterHandle*)pHandle;
  SDataInserterBuf*    pBuf = taosAllocateQitem(sizeof(SDataInserterBuf), DEF_QITEM);
  if (NULL == pBuf || !allocBuf(pDeleter, pInput, pBuf)) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  toDataCacheEntry(pDeleter, pInput, pBuf);
  taosWriteQitem(pDeleter->pDataBlocks, pBuf);
  *pContinue = (DS_BUF_LOW == updateStatus(pDeleter) ? true : false);
  return TSDB_CODE_SUCCESS;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataInserterHandle* pDeleter = (SDataInserterHandle*)pHandle;
  taosThreadMutexLock(&pDeleter->mutex);
  pDeleter->queryEnd = true;
  pDeleter->useconds = useconds;
  taosThreadMutexUnlock(&pDeleter->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int32_t* pLen, bool* pQueryEnd) {
  SDataInserterHandle* pDeleter = (SDataInserterHandle*)pHandle;
  if (taosQueueEmpty(pDeleter->pDataBlocks)) {
    *pQueryEnd = pDeleter->queryEnd;
    *pLen = 0;
    return;
  }

  SDataInserterBuf* pBuf = NULL;
  taosReadQitem(pDeleter->pDataBlocks, (void**)&pBuf);
  memcpy(&pDeleter->nextOutput, pBuf, sizeof(SDataInserterBuf));
  taosFreeQitem(pBuf);
  *pLen = ((SDataCacheEntry*)(pDeleter->nextOutput.pData))->dataLen;
  *pQueryEnd = pDeleter->queryEnd;
  qDebug("got data len %d, row num %d in sink", *pLen, ((SDataCacheEntry*)(pDeleter->nextOutput.pData))->numOfRows);
}

static int32_t getDataBlock(SDataSinkHandle* pHandle, SOutputData* pOutput) {
  SDataInserterHandle* pDeleter = (SDataInserterHandle*)pHandle;
  if (NULL == pDeleter->nextOutput.pData) {
    assert(pDeleter->queryEnd);
    pOutput->useconds = pDeleter->useconds;
    pOutput->precision = pDeleter->pSchema->precision;
    pOutput->bufStatus = DS_BUF_EMPTY;
    pOutput->queryEnd = pDeleter->queryEnd;
    return TSDB_CODE_SUCCESS;
  }
  SDataCacheEntry* pEntry = (SDataCacheEntry*)(pDeleter->nextOutput.pData);
  memcpy(pOutput->pData, pEntry->data, pEntry->dataLen);
  pOutput->numOfRows = pEntry->numOfRows;
  pOutput->numOfCols = pEntry->numOfCols;
  pOutput->compressed = pEntry->compressed;

  atomic_sub_fetch_64(&pDeleter->cachedSize, pEntry->dataLen);  
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen); 

  taosMemoryFreeClear(pDeleter->nextOutput.pData);  // todo persistent
  pOutput->bufStatus = updateStatus(pDeleter);
  taosThreadMutexLock(&pDeleter->mutex);
  pOutput->queryEnd = pDeleter->queryEnd;
  pOutput->useconds = pDeleter->useconds;
  pOutput->precision = pDeleter->pSchema->precision;
  taosThreadMutexUnlock(&pDeleter->mutex);
  
  return TSDB_CODE_SUCCESS;
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataInserterHandle* pDeleter = (SDataInserterHandle*)pHandle;
  atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pDeleter->cachedSize);
  taosMemoryFreeClear(pDeleter->nextOutput.pData);
  while (!taosQueueEmpty(pDeleter->pDataBlocks)) {
    SDataInserterBuf* pBuf = NULL;
    taosReadQitem(pDeleter->pDataBlocks, (void**)&pBuf);
    taosMemoryFreeClear(pBuf->pData);
    taosFreeQitem(pBuf);
  }
  taosCloseQueue(pDeleter->pDataBlocks);
  taosThreadMutexDestroy(&pDeleter->mutex);
  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataInserterHandle* pDispatcher = (SDataInserterHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataInserter(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle, void *pParam) {
  SDataInserterHandle* inserter = taosMemoryCalloc(1, sizeof(SDataInserterHandle));
  if (NULL == inserter) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }

  SDataDeleterNode* pDeleterNode = (SDataDeleterNode *)pDataSink;
  inserter->sink.fPut = putDataBlock;
  inserter->sink.fEndPut = endPut;
  inserter->sink.fGetLen = getDataLength;
  inserter->sink.fGetData = getDataBlock;
  inserter->sink.fDestroy = destroyDataSinker;
  inserter->sink.fGetCacheSize = getCacheSize;
  inserter->pManager = pManager;
  inserter->pDeleter = pDeleterNode;
  inserter->pSchema = pDataSink->pInputDataBlockDesc;
  inserter->pParam = pParam;
  inserter->status = DS_BUF_EMPTY;
  inserter->queryEnd = false;
  inserter->pDataBlocks = taosOpenQueue();
  taosThreadMutexInit(&inserter->mutex, NULL);
  if (NULL == inserter->pDataBlocks) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  *pHandle = inserter;
  return TSDB_CODE_SUCCESS;
}
