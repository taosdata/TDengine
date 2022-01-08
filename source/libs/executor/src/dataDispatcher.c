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

#define GET_BUF_DATA(buf) (buf)->pData + (buf)->pos
#define GET_BUF_REMAIN(buf) (buf)->remain

typedef struct SBuf {
  int32_t size;
  int32_t pos;
  int32_t remain;
  char* pData;
} SBuf;

typedef struct SDataDispatchHandle {
  SDataSinkHandle sink;
  SDataBlockSchema schema;
  STaosQueue* pDataBlocks;
  SBuf buf;
} SDataDispatchHandle;

static bool needCompress(const SSDataBlock* pData, const SDataBlockSchema* pSchema) {
  if (tsCompressColData < 0 || 0 == pData->info.rows) {
    return false;
  }

  for (int32_t col = 0; col < pSchema->numOfCols; ++col) {
    SColumnInfoData* pColRes = taosArrayGet(pData->pDataBlock, col);
    int32_t colSize = pColRes->info.bytes * pData->info.rows;
    if (NEEDTO_COMPRESS_QUERY(colSize)) {
      return true;
    }
  }

  return false;
}

static int32_t compressQueryColData(SColumnInfoData *pColRes, int32_t numOfRows, char *data, int8_t compressed) {
  int32_t colSize = pColRes->info.bytes * numOfRows;
  return (*(tDataTypes[pColRes->info.type].compFunc))(
      pColRes->pData, colSize, numOfRows, data, colSize + COMP_OVERFLOW_BYTES, compressed, NULL, 0);
}

static void doCopyQueryResultToMsg(const SDataResult* pRes, const SDataBlockSchema* pSchema, char* data, int8_t compressed, int32_t *compLen) {
  int32_t *compSizes = (int32_t*)data;
  if (compressed) {
    data += pSchema->numOfCols * sizeof(int32_t);
  }

  for (int32_t col = 0; col < pSchema->numOfCols; ++col) {
    SColumnInfoData* pColRes = taosArrayGet(pRes->pData->pDataBlock, col);
    if (compressed) {
      compSizes[col] = compressQueryColData(pColRes, pRes->pData->info.rows, data, compressed);
      data += compSizes[col];
      *compLen += compSizes[col];
      compSizes[col] = htonl(compSizes[col]);
    } else {
      memmove(data, pColRes->pData, pColRes->info.bytes * pRes->pData->info.rows);
      data += pColRes->info.bytes * pRes->pData->info.rows;
    }
  }

  int32_t numOfTables = (int32_t) taosHashGetSize(pRes->pTableRetrieveTsMap);
  *(int32_t*)data = htonl(numOfTables);
  data += sizeof(int32_t);

  STableIdInfo* item = taosHashIterate(pRes->pTableRetrieveTsMap, NULL);
  while (item) {
    STableIdInfo* pDst = (STableIdInfo*)data;
    pDst->uid = htobe64(item->uid);
    pDst->key = htobe64(item->key);
    data += sizeof(STableIdInfo);
    item = taosHashIterate(pRes->pTableRetrieveTsMap, item);
  }
}

static void toRetrieveResult(SDataDispatchHandle* pHandle, const SDataResult* pRes, char* pData, int32_t* pContLen) {
  SRetrieveTableRsp* pRsp = (SRetrieveTableRsp*)pData;
  pRsp->useconds = htobe64(pRes->profile.elapsedTime);
  pRsp->precision = htons(pHandle->schema.precision);
  pRsp->compressed = (int8_t)needCompress(pRes->pData, &(pHandle->schema));
  pRsp->numOfRows = htonl(pRes->pData->info.rows);

  *pContLen = sizeof(int32_t) + sizeof(STableIdInfo) * taosHashGetSize(pRes->pTableRetrieveTsMap) + sizeof(SRetrieveTableRsp);
  doCopyQueryResultToMsg(pRes, &pHandle->schema, pRsp->data, pRsp->compressed, &pRsp->compLen);
  *pContLen += (pRsp->compressed ? pRsp->compLen : pHandle->schema.resultRowSize * pRes->pData->info.rows);

  pRsp->compLen = htonl(pRsp->compLen);
  // todo completed
}

static int32_t putDataBlock(SDataSinkHandle* pHandle, const SDataResult* pRes) {
  SDataDispatchHandle* pDispatcher = (SDataDispatchHandle*)pHandle;
  int32_t useSize = 0;
  toRetrieveResult(pDispatcher, pRes, GET_BUF_DATA(&pDispatcher->buf), &useSize);
}

static int32_t getDataBlock(SDataSinkHandle* pHandle, char* pData, int32_t* pLen) {

}

static int32_t destroyDataSinker(SDataSinkHandle* pHandle) {

}

int32_t createDataDispatcher(const SDataSink* pDataSink, DataSinkHandle* pHandle) {
  SDataDispatchHandle* dispatcher = calloc(1, sizeof(SDataDispatchHandle));
  if (NULL == dispatcher) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  dispatcher->sink.fPut = putDataBlock;
  dispatcher->sink.fGet = getDataBlock;
  dispatcher->sink.fDestroy = destroyDataSinker;
  dispatcher->pDataBlocks = taosOpenQueue();
  if (NULL == dispatcher->pDataBlocks) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return TSDB_CODE_FAILED;
  }
  *pHandle = dispatcher;
  return TSDB_CODE_SUCCESS;
}
