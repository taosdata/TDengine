/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
  SDataSinkNode*      pSinkNode;
  SDataBlockDescNode* pSchema;
  SDataDeleterNode*   pDeleter;
  SDeleterParam*      pParam;
  STaosQueue*         pDataBlocks;
  SDataDeleterBuf     nextOutput;
  int32_t             status;
  bool                queryEnd;
  uint64_t            useconds;
  uint64_t            cachedSize;
  uint64_t            flags;
  TdThreadMutex       mutex;
  // range mode (DELETE with an ordinary column filter) accumulates across every input block and
  // emits a single result from endPut, because the consumer reads exactly one SDeleterRes. This
  // list never leaves the process: vnodePreProcessDeleteMsg (the sole reader, via
  // qwGetDeleteResFromSink) consumes it once, on the leader, before Raft/WAL ever sees anything.
  SArray*             pIntervals;  // SArray<SDeleteInterval>
  int64_t             affectedRows;
  bool                rangeResPut;
} SDataDeleterHandle;

// Upper bound on the ranges a single delete may produce on one vgroup. Merging matched rows into
// coarser ranges is not an option: a range wipes out everything inside it, matched or not.
#define DELETER_MAX_INTERVALS 100000

static int32_t appendDeleteInterval(SDataDeleterHandle* pHandle, uint64_t uid, int64_t ts) {
  int32_t size = taosArrayGetSize(pHandle->pIntervals);
  if (size > 0) {
    SDeleteInterval* pLast = (SDeleteInterval*)taosArrayGetLast(pHandle->pIntervals);
    // Rows arrive grouped by table and ascending by ts, so a repeated timestamp only extends the
    // range that is already open.
    if (pLast->uid == uid && ts == pLast->ekey) {
      return TSDB_CODE_SUCCESS;
    }
  }

  if (size >= DELETER_MAX_INTERVALS) {
    qError("delete column filter produced more than %d ranges on one vgroup", DELETER_MAX_INTERVALS);
    return TSDB_CODE_PAR_DELETE_FILTER_TOO_MANY_RANGES;
  }

  SDeleteInterval interval = {.uid = uid, .skey = ts, .ekey = ts};
  if (NULL == taosArrayPush(pHandle->pIntervals, &interval)) {
    return terrno;
  }
  return TSDB_CODE_SUCCESS;
}

// Turn the matched rows of one block into ranges. The scan has already applied the column filter, so
// every row here matches and each one contributes the degenerate range [ts, ts].
static int32_t collectDeleteIntervals(SDataDeleterHandle* pHandle, const SInputData* pInput) {
  const SSDataBlock* pBlock = pInput->pData;
  int32_t            numOfRows = pBlock->info.rows;
  if (numOfRows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t tsSlotId = pHandle->pDeleter->tsSlotId;
  if (tsSlotId < 0 || tsSlotId >= taosArrayGetSize(pBlock->pDataBlock)) {
    qError("invalid ts slot id %d in data deleter", tsSlotId);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  SColumnInfoData* pTsCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, tsSlotId);
  if (NULL == pTsCol || NULL == pTsCol->pData) {
    qError("ts column not materialized in data deleter, tsSlotId:%d numOfCols:%d", tsSlotId,
           (int32_t)taosArrayGetSize(pBlock->pDataBlock));
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  uint64_t uid = pBlock->info.id.uid;
  int64_t* pTs = (int64_t*)pTsCol->pData;
  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t code = appendDeleteInterval(pHandle, uid, pTs[i]);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }

  pHandle->affectedRows += numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t putRangeDeleteRes(SDataDeleterHandle* pHandle) {
  SDataDeleterBuf* pBuf = NULL;
  int32_t          code = taosAllocateQitem(sizeof(SDataDeleterBuf), DEF_QITEM, 0, (void**)&pBuf);
  if (code) {
    return code;
  }

  pBuf->allocSize = sizeof(SDataCacheEntry) + sizeof(SDeleterRes);
  pBuf->pData = taosMemoryMalloc(pBuf->allocSize);
  if (NULL == pBuf->pData) {
    taosFreeQitem(pBuf);
    return terrno;
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pBuf->pData;
  pEntry->compressed = 0;
  pEntry->numOfRows = 0;
  pEntry->numOfCols = 0;
  pEntry->dataLen = sizeof(SDeleterRes);
  pBuf->useSize = sizeof(SDataCacheEntry) + pEntry->dataLen;

  SDeleterRes* pRes = (SDeleterRes*)pEntry->data;
  TAOS_MEMSET(pRes, 0, sizeof(SDeleterRes));
  pRes->suid = pHandle->pParam->suid;
  pRes->uidList = pHandle->pParam->pUidList;
  tstrncpy(pRes->tableName, pHandle->pDeleter->tableFName, sizeof(pRes->tableName));
  tstrncpy(pRes->tsColName, pHandle->pDeleter->tsColName, sizeof(pRes->tsColName));
  pRes->secureDelete = pHandle->pDeleter->secureDelete | pHandle->pParam->secureDelete;
  pRes->affectedRows = pHandle->affectedRows;
  pRes->pIntervals = pHandle->pIntervals;
  pHandle->pIntervals = NULL;  // ownership moves to the result

  // Bounding window over the ranges, reported for observability only; vnodePreProcessDeleteMsg
  // fans pRes->pIntervals out into N independent point-delete proposals and never encodes this
  // window (or pIntervals itself) onto the wire.
  pRes->skey = INT64_MAX;
  pRes->ekey = INT64_MIN;
  int32_t numOfIntervals = taosArrayGetSize(pRes->pIntervals);
  for (int32_t i = 0; i < numOfIntervals; ++i) {
    SDeleteInterval* pInterval = (SDeleteInterval*)taosArrayGet(pRes->pIntervals, i);
    if (pInterval->skey < pRes->skey) pRes->skey = pInterval->skey;
    if (pInterval->ekey > pRes->ekey) pRes->ekey = pInterval->ekey;
  }
  if (0 == numOfIntervals) {
    pRes->skey = pHandle->pDeleter->deleteTimeRange.skey;
    pRes->ekey = pHandle->pDeleter->deleteTimeRange.ekey;
  }

  qDebug("delete %" PRId64 " rows through %d ranges", pRes->affectedRows,
         (int32_t)taosArrayGetSize(pRes->pIntervals));

  (void)atomic_add_fetch_64(&pHandle->cachedSize, pEntry->dataLen);
  (void)atomic_add_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  code = taosWriteQitem(pHandle->pDataBlocks, pBuf);
  if (code) {
    // pRes (and the pIntervals ownership moved into it above) never reaches a reader when the
    // buffer doesn't make it onto the queue, so free it here and undo the size accounting done
    // just above -- otherwise the interval array leaks and cachedSize/gDataSinkStat.cachedSize
    // stay permanently inflated by this buffer's size.
    taosArrayDestroy(pRes->pIntervals);
    (void)atomic_sub_fetch_64(&pHandle->cachedSize, pEntry->dataLen);
    (void)atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);
    taosMemoryFreeClear(pBuf->pData);
    taosFreeQitem(pBuf);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

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

  // The buffer comes from taosMemoryMalloc, so zero it before use: any field not assigned below
  // (pIntervals in particular) would otherwise be read and freed as a garbage pointer downstream.
  SDeleterRes* pRes = (SDeleterRes*)pEntry->data;
  TAOS_MEMSET(pRes, 0, sizeof(SDeleterRes));
  pRes->suid = pHandle->pParam->suid;
  pRes->uidList = pHandle->pParam->pUidList;
  tstrncpy(pRes->tableName, pHandle->pDeleter->tableFName, sizeof(pRes->tableName));
  tstrncpy(pRes->tsColName, pHandle->pDeleter->tsColName, sizeof(pRes->tsColName));
  pRes->affectedRows = *(int64_t*)pColRes->pData;
  pRes->secureDelete = pHandle->pDeleter->secureDelete | pHandle->pParam->secureDelete;

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

  qDebug("delete %" PRId64 " rows, from %" PRId64 " to %" PRId64, pRes->affectedRows, pRes->skey, pRes->ekey);

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
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  pBuf->allocSize = sizeof(SDataCacheEntry) + sizeof(SDeleterRes);

  pBuf->pData = taosMemoryMalloc(pBuf->allocSize);
  if (pBuf->pData == NULL) {
    qError("SinkNode failed to malloc memory, size:%d, code:%d", pBuf->allocSize, TAOS_SYSTEM_ERROR(ERRNO));
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t updateStatus(SDataDeleterHandle* pDeleter) {
  (void)taosThreadMutexLock(&pDeleter->mutex);
  int32_t blockNums = taosQueueItemSize(pDeleter->pDataBlocks);
  uint64_t cachedSize = atomic_load_64(&pDeleter->cachedSize);
  int32_t status = dsCalcBufStatus(&pDeleter->pManager->cfg, blockNums, cachedSize);
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

  // In range mode the scan feeds many blocks but the consumer reads exactly one SDeleterRes, so the
  // ranges are accumulated here and the single result is emitted from endPut.
  if (pDeleter->pDeleter->rangeMode) {
    int32_t code = collectDeleteIntervals(pDeleter, pInput);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
    *pContinue = true;
    return TSDB_CODE_SUCCESS;
  }

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

  // Emit the accumulated result before publishing queryEnd, so a reader that observes the end also
  // observes the result.
  if (pDeleter->pDeleter->rangeMode && !pDeleter->rangeResPut) {
    int32_t code = putRangeDeleteRes(pDeleter);
    if (TSDB_CODE_SUCCESS != code) {
      qError("failed to put delete ranges into sink, code:%s", tstrerror(code));
    }
    pDeleter->rangeResPut = true;
    (void)updateStatus(pDeleter);
  }

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
  taosReadQitem(pDeleter->pDataBlocks, (void**)&pBuf);
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
  pDeleter->pParam->pUidList = NULL;  // ownership moved to the caller together with pIntervals
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
  taosArrayDestroy(pDeleter->pIntervals);  // NULL once ownership moved into the result
  pDeleter->pIntervals = NULL;
  taosArrayDestroy(pDeleter->pParam->pUidList);
  taosMemoryFree(pDeleter->pParam);
  while (!taosQueueEmpty(pDeleter->pDataBlocks)) {
    SDataDeleterBuf* pBuf = NULL;
    taosReadQitem(pDeleter->pDataBlocks, (void**)&pBuf);

    if (pBuf != NULL) {
      taosMemoryFreeClear(pBuf->pData);
      taosFreeQitem(pBuf);
    }
  }
  taosCloseQueue(pDeleter->pDataBlocks);
  (void)taosThreadMutexDestroy(&pDeleter->mutex);
  nodesDestroyNode((SNode*)pDeleter->pSinkNode);
  pDeleter->pSinkNode = NULL;

  taosMemoryFree(pDeleter->pManager);

  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataDeleterHandle* pDispatcher = (SDataDeleterHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}


static int32_t getSinkFlags(struct SDataSinkHandle* pHandle, uint64_t* pFlags) {
  SDataDeleterHandle* pDispatcher = (SDataDeleterHandle*)pHandle;

  *pFlags = atomic_load_64(&pDispatcher->flags);
  return TSDB_CODE_SUCCESS;
}


int32_t createDataDeleter(SDataSinkManager* pManager, SDataSinkNode** ppDataSink, DataSinkHandle* pHandle,
                          void* pParam) {
  SDataSinkNode* pDataSink = *ppDataSink;
  int32_t code = TSDB_CODE_SUCCESS;
  if (pParam == NULL) {
    code = TSDB_CODE_QRY_INVALID_INPUT;
    qError("invalid input param in creating data deleter, code%s", tstrerror(code));
    goto _end;
  }

  SDeleterParam* pDeleterParam = (SDeleterParam*)pParam;

  SDataDeleterHandle* deleter = taosMemoryCalloc(1, sizeof(SDataDeleterHandle));
  if (NULL == deleter) {
    code = terrno;
    taosArrayDestroy(pDeleterParam->pUidList);
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
  deleter->sink.fGetFlags = getSinkFlags;
  deleter->pManager = pManager;
  deleter->pDeleter = pDeleterNode;
  deleter->pSchema = pDataSink->pInputDataBlockDesc;
  deleter->pSinkNode = pDataSink;
  *ppDataSink = NULL;

  deleter->pParam = pParam;
  deleter->status = DS_BUF_EMPTY;
  deleter->queryEnd = false;
  if (pDeleterNode->rangeMode) {
    deleter->pIntervals = taosArrayInit(16, sizeof(SDeleteInterval));
    if (NULL == deleter->pIntervals) {
      code = terrno;
      goto _end;
    }
  }
  code = taosOpenQueue(&deleter->pDataBlocks);
  if (code) {
    goto _end;
  }
  deleter->flags = DS_FLAG_USE_MEMPOOL;
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

  nodesDestroyNode((SNode *)*ppDataSink);
  *ppDataSink = NULL;
  
  return code;
}
