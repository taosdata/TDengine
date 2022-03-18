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
#include "planner.h"
#include "tcompression.h"
#include "tglobal.h"
#include "tqueue.h"
#include "executorimpl.h"

typedef struct SDataDispatchBuf {
  int32_t useSize;
  int32_t allocSize;
  char* pData;
} SDataDispatchBuf;

typedef struct SDataCacheEntry {
  int32_t dataLen;
  int32_t numOfRows;
  int8_t  compressed;
  char    data[];
} SDataCacheEntry;

typedef struct SDataDispatchHandle {
  SDataSinkHandle sink;
  SDataSinkManager* pManager;
  SDataBlockDescNode* pSchema;
  STaosQueue* pDataBlocks;
  SDataDispatchBuf nextOutput;
  int32_t status;
  bool queryEnd;
  uint64_t useconds;
  pthread_mutex_t mutex;
} SDataDispatchHandle;

static bool needCompress(const SSDataBlock* pData, const SDataBlockDescNode* pSchema) {
  if (tsCompressColData < 0 || 0 == pData->info.rows) {
    return false;
  }

  int32_t numOfCols = LIST_LENGTH(pSchema->pSlots);
  for (int32_t col = 0; col < numOfCols; ++col) {
    SColumnInfoData* pColRes = taosArrayGet(pData->pDataBlock, col);
    int32_t colSize = pColRes->info.bytes * pData->info.rows;
    if (NEEDTO_COMPRESS_QUERY(colSize)) {
      return true;
    }
  }

  return false;
}

static int32_t compressColData(SColumnInfoData *pColRes, int32_t numOfRows, char *data, int8_t compressed) {
  int32_t colSize = pColRes->info.bytes * numOfRows;
  return (*(tDataTypes[pColRes->info.type].compFunc))(
      pColRes->pData, colSize, numOfRows, data, colSize + COMP_OVERFLOW_BYTES, compressed, NULL, 0);
}

static void copyData(const SInputData* pInput, const SDataBlockDescNode* pSchema, char* data, int8_t compressed, int32_t *compLen) {
  int32_t numOfCols = LIST_LENGTH(pSchema->pSlots);
  int32_t *compSizes = (int32_t*)data;
  if (compressed) {
    data += numOfCols * sizeof(int32_t);
  }

  for (int32_t col = 0; col < numOfCols; ++col) {
    SColumnInfoData* pColRes = taosArrayGet(pInput->pData->pDataBlock, col);
    if (compressed) {
      compSizes[col] = compressColData(pColRes, pInput->pData->info.rows, data, compressed);
      data += compSizes[col];
      *compLen += compSizes[col];
      compSizes[col] = htonl(compSizes[col]);
    } else {
      memmove(data, pColRes->pData, pColRes->info.bytes * pInput->pData->info.rows);
      data += pColRes->info.bytes * pInput->pData->info.rows;
    }
  }
}

// data format with compress: SDataCacheEntry | cols_data_offset | col1_data col2_data ... | numOfTables | STableIdInfo STableIdInfo ...
// data format: SDataCacheEntry | col1_data col2_data ... | numOfTables | STableIdInfo STableIdInfo ...
static void toDataCacheEntry(const SDataDispatchHandle* pHandle, const SInputData* pInput, SDataDispatchBuf* pBuf) {
  SDataCacheEntry* pEntry = (SDataCacheEntry*)pBuf->pData;
  pEntry->compressed = (int8_t)needCompress(pInput->pData, pHandle->pSchema);
  pEntry->numOfRows = pInput->pData->info.rows;
  pEntry->dataLen = 0;

  pBuf->useSize = sizeof(SRetrieveTableRsp);
  copyData(pInput, pHandle->pSchema, pEntry->data, pEntry->compressed, &pEntry->dataLen);
  if (0 == pEntry->compressed) {
    pEntry->dataLen = pHandle->pSchema->resultRowSize * pInput->pData->info.rows;
  }
  pBuf->useSize += pEntry->dataLen;
  // todo completed
}

static bool allocBuf(SDataDispatchHandle* pDispatcher, const SInputData* pInput, SDataDispatchBuf* pBuf) {
  uint32_t capacity = pDispatcher->pManager->cfg.maxDataBlockNumPerQuery;
  if (taosQueueSize(pDispatcher->pDataBlocks) > capacity) {
    qError("SinkNode queue is full, no capacity, max:%d, current:%d, no capacity", capacity,
           taosQueueSize(pDispatcher->pDataBlocks));
    return false;
  }

  // struct size + data payload + length for each column
  pBuf->allocSize = sizeof(SRetrieveTableRsp) + pDispatcher->pSchema->resultRowSize * pInput->pData->info.rows + pInput->pData->info.numOfCols * sizeof(int32_t);
  pBuf->pData = malloc(pBuf->allocSize);
  if (pBuf->pData == NULL) {
    qError("SinkNode failed to malloc memory, size:%d, code:%d", pBuf->allocSize, TAOS_SYSTEM_ERROR(errno));
  }

  return NULL != pBuf->pData;
}

static int32_t updateStatus(SDataDispatchHandle* pDispatcher) {
  pthread_mutex_lock(&pDispatcher->mutex);
  int32_t blockNums = taosQueueSize(pDispatcher->pDataBlocks);
  int32_t status = (0 == blockNums ? DS_BUF_EMPTY :
      (blockNums < pDispatcher->pManager->cfg.maxDataBlockNumPerQuery ? DS_BUF_LOW : DS_BUF_FULL));
  pDispatcher->status = status;
  pthread_mutex_unlock(&pDispatcher->mutex);
  return status;
}

static int32_t getStatus(SDataDispatchHandle* pDispatcher) {
  pthread_mutex_lock(&pDispatcher->mutex);
  int32_t status = pDispatcher->status;
  pthread_mutex_unlock(&pDispatcher->mutex);
  return status;
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SInputData* pInput, bool* pContinue) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  SDataDispatchBuf* pBuf = taosAllocateQitem(sizeof(SDataDispatchBuf));
  if (NULL == pBuf || !allocBuf(pDispatcher, pInput, pBuf)) {
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  toDataCacheEntry(pDispatcher, pInput, pBuf);
  taosWriteQitem(pDispatcher->pDataBlocks, pBuf);
  *pContinue = (DS_BUF_LOW == updateStatus(pDispatcher) ? true : false);
  return TSDB_CODE_SUCCESS;
}

static void endPut(struct SDataSinkHandle* pHandle, uint64_t useconds) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  pthread_mutex_lock(&pDispatcher->mutex);
  pDispatcher->queryEnd = true;
  pDispatcher->useconds = useconds;
  pthread_mutex_unlock(&pDispatcher->mutex);
}

static void getDataLength(SDataSinkHandle* pHandle, int32_t* pLen, bool* pQueryEnd) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  if (taosQueueEmpty(pDispatcher->pDataBlocks)) {
    *pQueryEnd = pDispatcher->queryEnd;
    *pLen = 0;
    return;
  }
  SDataDispatchBuf* pBuf = NULL;
  taosReadQitem(pDispatcher->pDataBlocks, (void**)&pBuf);
  memcpy(&pDispatcher->nextOutput, pBuf, sizeof(SDataDispatchBuf));
  taosFreeQitem(pBuf);
  *pLen = ((SDataCacheEntry*)(pDispatcher->nextOutput.pData))->dataLen;
  *pQueryEnd = pDispatcher->queryEnd;    
}

static int32_t getDataBlock(SDataSinkHandle* pHandle, SOutputData* pOutput) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  if (NULL == pDispatcher->nextOutput.pData) {
    assert(pDispatcher->queryEnd);
    pOutput->useconds = pDispatcher->useconds;
    pOutput->precision = pDispatcher->pSchema->precision;
    return TSDB_CODE_SUCCESS;
  }
  SDataCacheEntry* pEntry = (SDataCacheEntry*)(pDispatcher->nextOutput.pData);
  memcpy(pOutput->pData, pEntry->data, pEntry->dataLen);
  pOutput->numOfRows = pEntry->numOfRows;
  pOutput->compressed = pEntry->compressed;
  tfree(pDispatcher->nextOutput.pData);  // todo persistent
  pOutput->bufStatus = updateStatus(pDispatcher);
  pthread_mutex_lock(&pDispatcher->mutex);
  pOutput->queryEnd = pDispatcher->queryEnd;
  pOutput->useconds = pDispatcher->useconds;
  pOutput->precision = pDispatcher->pSchema->precision;
  pthread_mutex_unlock(&pDispatcher->mutex);
  return TSDB_CODE_SUCCESS;
}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  tfree(pDispatcher->nextOutput.pData);
  while (!taosQueueEmpty(pDispatcher->pDataBlocks)) {
    SDataDispatchBuf* pBuf = NULL;
    taosReadQitem(pDispatcher->pDataBlocks, (void**)&pBuf);
    tfree(pBuf->pData);
    taosFreeQitem(pBuf);
  }
  taosCloseQueue(pDispatcher->pDataBlocks);
  pthread_mutex_destroy(&pDispatcher->mutex);
}

int32_t createDataDispatcher(SDataSinkManager* pManager, const SDataSinkNode* pDataSink, DataSinkHandle* pHandle) {
  SDataDispatchHandle* dispatcher = calloc(1, sizeof(SDataDispatchHandle));
  if (NULL == dispatcher) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  dispatcher->sink.fPut = putDataBlock;
  dispatcher->sink.fEndPut = endPut;
  dispatcher->sink.fGetLen = getDataLength;
  dispatcher->sink.fGetData = getDataBlock;
  dispatcher->sink.fDestroy = destroyDataSinker;
  dispatcher->pManager = pManager;
  dispatcher->pSchema = pDataSink->pInputDataBlockDesc;
  dispatcher->status = DS_BUF_EMPTY;
  dispatcher->queryEnd = false;
  dispatcher->pDataBlocks = taosOpenQueue();
  pthread_mutex_init(&dispatcher->mutex, NULL);
  if (NULL == dispatcher->pDataBlocks) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_QRY_OUT_OF_MEMORY;
  }
  *pHandle = dispatcher;
  return TSDB_CODE_SUCCESS;
}
