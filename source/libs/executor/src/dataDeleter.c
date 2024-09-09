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

typedef struct SDataDeleterBuf {
  int32_t useSize;
  int32_t allocSize;
  char*   pData;
} SDataDeleterBuf;

typedef struct SDataCacheEntry {
  int32_t dataLen;
  int32_t numOfRows;
  int32_t numOfCols;
  int8_t  compressed;
  char    data[];
} SDataCacheEntry;

typedef struct SDataDeleterHandle {
  SDataSinkHandle     sink;
  SDataSinkManager*   pManager;
  SDataBlockDescNode* pSchema;
  SDataDeleterNode*   pDeleter;
  SDeleterParam*      pParam;
  STaosQueue*         pDataBlocks;
  SDataDeleterBuf     nextOutput;
  int32_t             status;
  bool                queryEnd;
  uint64_t            useconds;
  uint64_t            cachedSize;
  TdThreadMutex       mutex;
} SDataDeleterHandle;

static int32_t toDataCacheEntry(SDataDeleterHandle* pHandle, const SInputData* pInput, SDataDeleterBuf* pBuf) {
  int32_t numOfCols = LIST_LENGTH(pHandle->pSchema->pSlots);

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pBuf->pData;
  pEntry->compressed = 0;
  pEntry->numOfRows = pInput->pData->info.rows;
  pEntry->numOfCols = taosArrayGetSize(pInput->pData->pDataBlock);
  pEntry->dataLen = sizeof(SDeleterRes);

  pBuf->useSize = sizeof(SDataCacheEntry);

  SColumnInfoData* pColRes = (SColumnInfoData*)taosArrayGet(pInput->pData->pDataBlock, 0);
  if (NULL == pColRes) {
    QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  SColumnInfoData* pColSKey = (SColumnInfoData*)taosArrayGet(pInput->pData->pDataBlock, 1);
  if (NULL == pColSKey) {
    QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  SColumnInfoData* pColEKey = (SColumnInfoData*)taosArrayGet(pInput->pData->pDataBlock, 2);
  if (NULL == pColEKey) {
    QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }

  SDeleterRes* pRes = (SDeleterRes*)pEntry->data;
  pRes->suid = pHandle->pParam->suid;
  pRes->uidList = pHandle->pParam->pUidList;
  tstrncpy(pRes->tableName, pHandle->pDeleter->tableFName, sizeof(pRes->tableName));
  tstrncpy(pRes->tsColName, pHandle->pDeleter->tsColName, sizeof(pRes->tsColName));
  pRes->affectedRows = *(int64_t*)pColRes->pData;

  if (pRes->affectedRows) {
    pRes->skey = *(int64_t*)pColSKey->pData;
    pRes->ekey = *(int64_t*)pColEKey->pData;
    if (pRes->skey > pRes->ekey) {
      qError("data delter skey:%" PRId64 " is bigger than ekey:%" PRId64, pRes->skey, pRes->ekey);
      QRY_ERR_RET(TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }
  } else {
    pRes->skey = pHandle->pDeleter->deleteTimeRange.skey;
    pRes->ekey = pHandle->pDeleter->deleteTimeRange.ekey;
  }

  qDebug("delete %" PRId64 " rows, from %" PRId64 " to %" PRId64 "", pRes->affectedRows, pRes->skey, pRes->ekey);

  pBuf->useSize += pEntry->dataLen;

  (void)atomic_add_fetch_64(&pHandle->cachedSize, pEntry->dataLen);
  (void)atomic_add_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  return TSDB_CODE_SUCCESS;
}

static int32_t allocBuf(SDataDeleterHandle* pDeleter, const SInputData* pInput, SDataDeleterBuf* pBuf) {
  uint32_t capacity = pDeleter->pManager->cfg.maxDataBlockNumPerQuery;
  if (taosQueueItemSize(pDeleter->pDataBlocks) > capacity) {
    qError("SinkNode queue is full, no capacity, max:%d, current:%d, no capacity", capacity,
           taosQueueItemSize(pDeleter->pDataBlocks));
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pBuf->allocSize = sizeof(SDataCacheEntry) + sizeof(SDeleterRes);

  pBuf->pData = taosMemoryMalloc(pBuf->allocSize);
  if (pBuf->pData == NULL) {
    qError("SinkNode failed to malloc memory, size:%d, code:%d", pBuf->allocSize, TAOS_SYSTEM_ERROR(errno));
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t updateStatus(SDataDeleterHandle* pDeleter) {
  (void)taosThreadMutexLock(&pDeleter->mutex);
  int32_t blockNums = taosQueueItemSize(pDeleter->pDataBlocks);
  int32_t status =
      (0 == blockNums ? DS_BUF_EMPTY
                      : (blockNums < pDeleter->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pDeleter->status = status;
  (void)taosThreadMutexUnlock(&pDeleter->mutex);

  return status;
}

static int32_t getStatus(SDataDeleterHandle* pDeleter) {
  (void)taosThreadMutexLock(&pDeleter->mutex);
  int32_t status = pDeleter->status;
  (void)taosThreadMutexUnlock(&pDeleter->mutex);

  return status;
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  SDataDeleterHandle* pDeleter = (SDataDeleterHandle*)pHandle;
  SDataDeleterBuf*    pBuf = NULL;

  int32_t code = taosAllocateQitem(sizeof(SDataDeleterBuf), DEF_QITEM, 0, (void**)&pBuf);
  if (code) {
    return code;
  }

  code = allocBuf(pDeleter, pInput, pBuf);
  if (code) {
    taosFreeQitem(pBuf);
    return code;
  }

  QRY_ERR_JRET(toDataCacheEntry(pDeleter, pInput, pBuf));
  QRY_ERR_JRET(taosWriteQitem(pDeleter->pDataBlocks, pBuf));
  *pContinue = (DS_BUF_LOW == updateStatus(pDeleter) ? true : false);

  return TSDB_CODE_SUCCESS;

_return:

  taosFreeQitem(pBuf);

  return code;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataDeleterHandle* pDeleter = (SDataDeleterHandle*)pHandle;
  (void)taosThreadMutexLock(&pDeleter->mutex);
  pDeleter->queryEnd = true;
  pDeleter->useconds = useconds;
  (void)taosThreadMutexUnlock(&pDeleter->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int64_t* pLen, int64_t* pRawLen, bool* pQueryEnd) {
  SDataDeleterHandle* pDeleter = (SDataDeleterHandle*)pHandle;
  if (taosQueueEmpty(pDeleter->pDataBlocks)) {
    *pQueryEnd = pDeleter->queryEnd;
    *pLen = 0;
    return;
  }

  SDataDeleterBuf* pBuf = NULL;
  (void)taosReadQitem(pDeleter->pDataBlocks, (void**)&pBuf);
  if (pBuf != NULL) {
    TAOS_MEMCPY(&pDeleter->nextOutput, pBuf, sizeof(SDataDeleterBuf));
    taosFreeQitem(pBuf);
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pDeleter->nextOutput.pData;
  *pLen = pEntry->dataLen;
  *pRawLen = pEntry->dataLen;

  *pQueryEnd = pDeleter->queryEnd;
  qDebug("got data len %" PRId64 ", row num %d in sink", *pLen,
         ((SDataCacheEntry*)(pDeleter->nextOutput.pData))->numOfRows);
}

static int32_t getDataBlock(SDataSinkHandle* pHandle, SOutputData* pOutput) {
  SDataDeleterHandle* pDeleter = (SDataDeleterHandle*)pHandle;
  if (NULL == pDeleter->nextOutput.pData) {
    if (!pDeleter->queryEnd) {
      qError("empty res while query not end in data deleter");
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }
    pOutput->useconds = pDeleter->useconds;
    pOutput->precision = pDeleter->pSchema->precision;
    pOutput->bufStatus = DS_BUF_EMPTY;
    pOutput->queryEnd = pDeleter->queryEnd;
    return TSDB_CODE_SUCCESS;
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)(pDeleter->nextOutput.pData);
  TAOS_MEMCPY(pOutput->pData, pEntry->data, pEntry->dataLen);
  pDeleter->pParam->pUidList = NULL;
  pOutput->numOfRows = pEntry->numOfRows;
  pOutput->numOfCols = pEntry->numOfCols;
  pOutput->compressed = pEntry->compressed;

  (void)atomic_sub_fetch_64(&pDeleter->cachedSize, pEntry->dataLen);
  (void)atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  taosMemoryFreeClear(pDeleter->nextOutput.pData);  // todo persistent
  pOutput->bufStatus = updateStatus(pDeleter);
  (void)taosThreadMutexLock(&pDeleter->mutex);
  pOutput->queryEnd = pDeleter->queryEnd;
  pOutput->useconds = pDeleter->useconds;
  pOutput->precision = pDeleter->pSchema->precision;
  (void)taosThreadMutexUnlock(&pDeleter->mutex);

  return TSDB_CODE_SUCCESS;
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataDeleterHandle* pDeleter = (SDataDeleterHandle*)pHandle;
  (void)atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pDeleter->cachedSize);
  taosMemoryFreeClear(pDeleter->nextOutput.pData);
  taosArrayDestroy(pDeleter->pParam->pUidList);
  taosMemoryFree(pDeleter->pParam);
  while (!taosQueueEmpty(pDeleter->pDataBlocks)) {
    SDataDeleterBuf* pBuf = NULL;
    (void)taosReadQitem(pDeleter->pDataBlocks, (void**)&pBuf);

    if (pBuf != NULL) {
      taosMemoryFreeClear(pBuf->pData);
      taosFreeQitem(pBuf);
    }
  }
  taosCloseQueue(pDeleter->pDataBlocks);
  (void)taosThreadMutexDestroy(&pDeleter->mutex);

  taosMemoryFree(pDeleter->pManager);

  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataDeleterHandle* pDispatcher = (SDataDeleterHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataDeleter(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle,
                          void* pParam) {
  int32_t code = TSDB_CODE_SUCCESS;

  SDataDeleterHandle* deleter = taosMemoryCalloc(1, sizeof(SDataDeleterHandle));
  if (NULL == deleter) {
    code = terrno;
    taosMemoryFree(pParam);
    goto _end;
  }

  SDataDeleterNode* pDeleterNode = (SDataDeleterNode*)pDataSink;
  deleter->sink.fPut = putDataBlock;
  deleter->sink.fEndPut = endPut;
  deleter->sink.fGetLen = getDataLength;
  deleter->sink.fGetData = getDataBlock;
  deleter->sink.fDestroy = destroyDataSinker;
  deleter->sink.fGetCacheSize = getCacheSize;
  deleter->pManager = pManager;
  deleter->pDeleter = pDeleterNode;
  deleter->pSchema = pDataSink->pInputDataBlockDesc;

  if (pParam == NULL) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    qError("invalid input param in creating data deleter, code%s", tstrerror(code));
    goto _end;
  }

  deleter->pParam = pParam;
  deleter->status = DS_BUF_EMPTY;
  deleter->queryEnd = false;
  code = taosOpenQueue(&deleter->pDataBlocks);
  if (code) {
    goto _end;
  }
  code = taosThreadMutexInit(&deleter->mutex, NULL);
  if (code) {
    goto _end;
  }

  *pHandle = deleter;
  return code;

_end:

  if (deleter != NULL) {
    (void)destroyDataSinker((SDataSinkHandle*)deleter);
    taosMemoryFree(deleter);
  } else {
    taosMemoryFree(pManager);
  }

  return code;
}
