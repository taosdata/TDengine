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
  int32_t rawLen;
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
  void*               pCompressBuf;
  int32_t             bufSize;
  TdThreadMutex       mutex;
} SDataDispatchHandle;

static int32_t inputSafetyCheck(SDataDispatchHandle* pHandle, const SInputData* pInput)  {
  if(!tsEnableSafetyCheck) {
    return TSDB_CODE_SUCCESS;
  }
  if (pInput == NULL || pInput->pData == NULL || pInput->pData->info.rows <= 0) {
    qError("invalid input data");
    return TSDB_CODE_QRY_INVALID_INPUT;
  }
  SDataBlockDescNode* pSchema = pHandle->pSchema;
  if (pSchema == NULL || pSchema->outputRowSize > pInput->pData->info.rowSize) {
    qError("invalid schema");
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  SNode*  pNode;
  int32_t numOfCols = 0;
  int32_t realOutputRowSize = 0;
  FOREACH(pNode, pHandle->pSchema->pSlots) {
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->output) {
      realOutputRowSize += pSlotDesc->dataType.bytes;
      ++numOfCols;
    } else {
      break;
    }
  }
  if (realOutputRowSize !=  pSchema->outputRowSize) {
    qError("invalid schema, realOutputRowSize:%d, outputRowSize:%d", realOutputRowSize, pSchema->outputRowSize);
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  if (numOfCols > taosArrayGetSize(pInput->pData->pDataBlock)) {
    qError("invalid column number, schema:%d, input:%zu", numOfCols, taosArrayGetSize(pInput->pData->pDataBlock));
    return TSDB_CODE_QRY_INVALID_INPUT;
  }

  int32_t colNum = 0;
  FOREACH(pNode, pHandle->pSchema->pSlots) {
    SSlotDescNode* pSlotDesc = (SSlotDescNode*)pNode;
    if (pSlotDesc->output) {
      SColumnInfoData* pColInfoData = taosArrayGet(pInput->pData->pDataBlock, colNum);
      if (pColInfoData == NULL) {
        return -1;
      }
      if (pColInfoData->info.bytes < 0) {
        qError("invalid column bytes, schema:%d, input:%d", pSlotDesc->dataType.bytes, pColInfoData->info.bytes);
        return TSDB_CODE_TSC_INTERNAL_ERROR;
      }
      if (!IS_VAR_DATA_TYPE(pColInfoData->info.type) &&
          TYPE_BYTES[pColInfoData->info.type] != pColInfoData->info.bytes) {
        qError("invalid column bytes, schema:%d, input:%d", TYPE_BYTES[pColInfoData->info.type],
               pColInfoData->info.bytes);
        return TSDB_CODE_TSC_INTERNAL_ERROR;
      }
      if (pColInfoData->info.type != pSlotDesc->dataType.type) {
        qError("invalid column type, schema:%d, input:%d", pSlotDesc->dataType.type, pColInfoData->info.type);
        return TSDB_CODE_QRY_INVALID_INPUT;
      }
      if (pColInfoData->info.bytes != pSlotDesc->dataType.bytes) {
        qError("invalid column bytes, schema:%d, input:%d", pSlotDesc->dataType.bytes, pColInfoData->info.bytes);
        return TSDB_CODE_QRY_INVALID_INPUT;
      }

      if (IS_INVALID_TYPE(pColInfoData->info.type)) {
        qError("invalid column type, type:%d", pColInfoData->info.type);
        return TSDB_CODE_TSC_INTERNAL_ERROR;
      }
      ++colNum;
    }
  }


  return TSDB_CODE_SUCCESS;
}

// clang-format off
// data format:
// +----------------+------------------+--------------+--------------+------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// |SDataCacheEntry |  version         | total length | numOfRows    |     group id     | col1_schema | col2_schema | col3_schema... | column#1 length, column#2 length...| col1 bitmap | col1 data | col2 bitmap | col2 data |
// |                |  sizeof(int32_t) |sizeof(int32) | sizeof(int32)| sizeof(uint64_t) | (sizeof(int8_t)+sizeof(int32_t))*numOfCols | sizeof(int32_t) * numOfCols        | actual size |           |                         |
// +----------------+------------------+--------------+--------------+------------------+--------------------------------------------+------------------------------------+-------------+-----------+-------------+-----------+
// The length of bitmap is decided by number of rows of this data block, and the length of each column data is
// recorded in the first segment, next to the struct header
// clang-format on
static int32_t toDataCacheEntry(SDataDispatchHandle* pHandle, const SInputData* pInput, SDataDispatchBuf* pBuf) {
  int32_t numOfCols = 0;
  SNode*  pNode;

  int32_t code = inputSafetyCheck(pHandle, pInput);
  if (code) {
    qError("failed to check input data, code:%d", code);
    return code;
  }

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
  pEntry->rawLen = 0;

  pBuf->useSize = sizeof(SDataCacheEntry);

  {
    // allocate additional 8 bytes to avoid invalid write if compress failed to reduce the size
    size_t dataEncodeBufSize = pBuf->allocSize + 8;
    if ((pBuf->allocSize > tsCompressMsgSize) && (tsCompressMsgSize > 0) && pHandle->pManager->cfg.compress) {
      if (pHandle->pCompressBuf == NULL) {
        pHandle->pCompressBuf = taosMemoryMalloc(dataEncodeBufSize);
        if (NULL == pHandle->pCompressBuf) {
          QRY_RET(terrno);
        }
        pHandle->bufSize = dataEncodeBufSize;
      } else {
        if (pHandle->bufSize < dataEncodeBufSize) {
          pHandle->bufSize = dataEncodeBufSize;
          void* p = taosMemoryRealloc(pHandle->pCompressBuf, pHandle->bufSize);
          if (p != NULL) {
            pHandle->pCompressBuf = p;
          } else {
            qError("failed to prepare compress buf:%d, code: %x", pHandle->bufSize, terrno);
            return terrno;
          }
        }
      }

      int32_t dataLen = blockEncode(pInput->pData, pHandle->pCompressBuf, dataEncodeBufSize, numOfCols);
      if(dataLen < 0) {
        qError("failed to encode data block, code: %d", dataLen);
        return terrno;
      }
      int32_t len =
          tsCompressString(pHandle->pCompressBuf, dataLen, 1, pEntry->data, pBuf->allocSize, ONE_STAGE_COMP, NULL, 0);
      if (len < dataLen) {
        pEntry->compressed = 1;
        pEntry->dataLen = len;
        pEntry->rawLen = dataLen;
      } else {  // no need to compress data
        pEntry->compressed = 0;
        pEntry->dataLen = dataLen;
        pEntry->rawLen = dataLen;
        TAOS_MEMCPY(pEntry->data, pHandle->pCompressBuf, dataLen);
      }
    } else {
      pEntry->dataLen = blockEncode(pInput->pData, pEntry->data,  pBuf->allocSize, numOfCols);
      if(pEntry->dataLen < 0) {
        qError("failed to encode data block, code: %d", pEntry->dataLen);
        return terrno;
      }
      pEntry->rawLen = pEntry->dataLen;
    }
  }

  pBuf->useSize += pEntry->dataLen;

  (void)atomic_add_fetch_64(&pHandle->cachedSize, pEntry->dataLen);
  (void)atomic_add_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  return TSDB_CODE_SUCCESS;
}

static int32_t allocBuf(SDataDispatchHandle* pDispatcher, const SInputData* pInput, SDataDispatchBuf* pBuf) {
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
    qError("SinkNode failed to malloc memory, size:%d, code:%x", pBuf->allocSize, terrno);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t updateStatus(SDataDispatchHandle* pDispatcher) {
  (void)taosThreadMutexLock(&pDispatcher->mutex);
  int32_t blockNums = taosQueueItemSize(pDispatcher->pDataBlocks);
  int32_t status =
      (0 == blockNums ? DS_BUF_EMPTY
                      : (blockNums < pDispatcher->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pDispatcher->status = status;
  (void)taosThreadMutexUnlock(&pDispatcher->mutex);
  return status;
}

static int32_t getStatus(SDataDispatchHandle* pDispatcher) {
  (void)taosThreadMutexLock(&pDispatcher->mutex);
  int32_t status = pDispatcher->status;
  (void)taosThreadMutexUnlock(&pDispatcher->mutex);
  return status;
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  int32_t              code = 0;
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  SDataDispatchBuf*    pBuf = NULL;

  code = taosAllocateQitem(sizeof(SDataDispatchBuf), DEF_QITEM, 0, (void**)&pBuf);
  if (code) {
    return code;
  }

  code = allocBuf(pDispatcher, pInput, pBuf);
  if (code) {
    taosFreeQitem(pBuf);
    return code;
  }

  QRY_ERR_JRET(toDataCacheEntry(pDispatcher, pInput, pBuf));
  QRY_ERR_JRET(taosWriteQitem(pDispatcher->pDataBlocks, pBuf));

  int32_t status = updateStatus(pDispatcher);
  *pContinue = (status == DS_BUF_LOW || status == DS_BUF_EMPTY);
  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFreeClear(pBuf->pData);
  taosFreeQitem(pBuf);
  return code;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  (void)taosThreadMutexLock(&pDispatcher->mutex);
  pDispatcher->queryEnd = true;
  pDispatcher->useconds = useconds;
  (void)taosThreadMutexUnlock(&pDispatcher->mutex);
}

static void resetDispatcher(struct SDataSinkHandle* pHandle) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  (void)taosThreadMutexLock(&pDispatcher->mutex);
  pDispatcher->queryEnd = false;
  (void)taosThreadMutexUnlock(&pDispatcher->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int64_t* pLen, int64_t* pRowLen, bool* pQueryEnd) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  if (taosQueueEmpty(pDispatcher->pDataBlocks)) {
    *pQueryEnd = pDispatcher->queryEnd;
    *pLen = 0;
    return;
  }

  SDataDispatchBuf* pBuf = NULL;
  taosReadQitem(pDispatcher->pDataBlocks, (void**)&pBuf);
  if (pBuf != NULL) {
    TAOS_MEMCPY(&pDispatcher->nextOutput, pBuf, sizeof(SDataDispatchBuf));
    taosFreeQitem(pBuf);
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)pDispatcher->nextOutput.pData;
  *pLen = pEntry->dataLen;
  *pRowLen = pEntry->rawLen;

  *pQueryEnd = pDispatcher->queryEnd;
  qDebug("got data len %" PRId64 ", row num %d in sink", *pLen,
         ((SDataCacheEntry*)(pDispatcher->nextOutput.pData))->numOfRows);
}

static int32_t getDataBlock(SDataSinkHandle* pHandle, SOutputData* pOutput) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  if (NULL == pDispatcher->nextOutput.pData) {
    if (!pDispatcher->queryEnd) {
      qError("empty res while query not end in data dispatcher");
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    pOutput->useconds = pDispatcher->useconds;
    pOutput->precision = pDispatcher->pSchema->precision;
    pOutput->bufStatus = DS_BUF_EMPTY;
    pOutput->queryEnd = pDispatcher->queryEnd;
    return TSDB_CODE_SUCCESS;
  }

  SDataCacheEntry* pEntry = (SDataCacheEntry*)(pDispatcher->nextOutput.pData);
  TAOS_MEMCPY(pOutput->pData, pEntry->data, pEntry->dataLen);
  pOutput->numOfRows = pEntry->numOfRows;
  pOutput->numOfCols = pEntry->numOfCols;
  pOutput->compressed = pEntry->compressed;

  (void)atomic_sub_fetch_64(&pDispatcher->cachedSize, pEntry->dataLen);
  (void)atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pEntry->dataLen);

  taosMemoryFreeClear(pDispatcher->nextOutput.pData);  // todo persistent
  pOutput->bufStatus = updateStatus(pDispatcher);
  
  (void)taosThreadMutexLock(&pDispatcher->mutex);
  pOutput->queryEnd = pDispatcher->queryEnd;
  pOutput->useconds = pDispatcher->useconds;
  pOutput->precision = pDispatcher->pSchema->precision;
  (void)taosThreadMutexUnlock(&pDispatcher->mutex);

  return TSDB_CODE_SUCCESS;
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  (void)atomic_sub_fetch_64(&gDataSinkStat.cachedSize, pDispatcher->cachedSize);
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
  taosMemoryFreeClear(pDispatcher->pCompressBuf);
  pDispatcher->bufSize = 0;

  (void)taosThreadMutexDestroy(&pDispatcher->mutex);
  taosMemoryFree(pDispatcher->pManager);
  return TSDB_CODE_SUCCESS;
}

static int32_t getCacheSize(struct SDataSinkHandle* pHandle, uint64_t* size) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;

  *size = atomic_load_64(&pDispatcher->cachedSize);
  return TSDB_CODE_SUCCESS;
}

int32_t createDataDispatcher(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle) {
  int32_t code;

  SDataDispatchHandle* dispatcher = taosMemoryCalloc(1, sizeof(SDataDispatchHandle));
  if (NULL == dispatcher) {
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
  pManager = NULL;
  dispatcher->pSchema = pDataSink->pInputDataBlockDesc;
  dispatcher->status = DS_BUF_EMPTY;
  dispatcher->queryEnd = false;
  code = taosOpenQueue(&dispatcher->pDataBlocks);
  if (code) {
    terrno = code;
    goto _return;
  }
  code = taosThreadMutexInit(&dispatcher->mutex, NULL);
  if (code) {
    terrno = code;
    goto _return;
  }

  *pHandle = dispatcher;
  return TSDB_CODE_SUCCESS;

_return:

  taosMemoryFree(pManager);
  
  if (dispatcher) {
    dsDestroyDataSinker(dispatcher);
  }
  return terrno;
}
