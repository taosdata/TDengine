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

#ifndef TDENGINE_COMMON_H
#define TDENGINE_COMMON_H

#include "taosdef.h"
#include "tarray.h"
#include "tmsg.h"
#include "tvariant.h"
// typedef struct STimeWindow {
//   TSKEY skey;
//   TSKEY ekey;
// } STimeWindow;

// typedef struct {
//   int32_t dataLen;
//   char    name[TSDB_TABLE_FNAME_LEN];
//   char   *data;
// } STagData;

// typedef struct SSchema {
//   uint8_t type;
//   char    name[TSDB_COL_NAME_LEN];
//   int16_t colId;
//   int16_t bytes;
// } SSchema;

#define TMQ_REQ_TYPE_COMMIT_ONLY 0
#define TMQ_REQ_TYPE_CONSUME_ONLY 1
#define TMQ_REQ_TYPE_CONSUME_AND_COMMIT 2

typedef struct {
  uint32_t  numOfTables;
  SArray*   pGroupList;
  SHashObj* map;  // speedup acquire the tableQueryInfo by table uid
} STableGroupInfo;

typedef struct SColumnDataAgg {
  int16_t colId;
  int64_t sum;
  int64_t max;
  int64_t min;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
} SColumnDataAgg;

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rows;
  int32_t     numOfCols;
  int64_t     uid;
} SDataBlockInfo;

typedef struct SConstantItem {
  SColumnInfo info;
  int32_t     startRow;  // run-length-encoding to save the space for multiple rows
  int32_t     endRow;
  SVariant    value;
} SConstantItem;

// info.numOfCols = taosArrayGetSize(pDataBlock) + taosArrayGetSize(pConstantList);
typedef struct SSDataBlock {
  SColumnDataAgg* pBlockAgg;
  SArray*         pDataBlock;  // SArray<SColumnInfoData>
  SArray* pConstantList;       // SArray<SConstantItem>, it is a constant/tags value of the corresponding result value.
  SDataBlockInfo info;
} SSDataBlock;

// pBlockAgg->numOfNull == info.rows, all data are null
// pBlockAgg->numOfNull == 0, no data are null.
typedef struct SColumnInfoData {
  SColumnInfo info;        // TODO filter info needs to be removed
  char*       nullbitmap;  //
  char*       pData;       // the corresponding block data in memory
} SColumnInfoData;

static FORCE_INLINE int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock) {
  int64_t tbUid = pBlock->info.uid;
  int32_t numOfCols = pBlock->info.numOfCols;
  int32_t rows = pBlock->info.rows;
  int32_t sz = taosArrayGetSize(pBlock->pDataBlock);

  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, tbUid);
  tlen += taosEncodeFixedI32(buf, numOfCols);
  tlen += taosEncodeFixedI32(buf, rows);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    tlen += taosEncodeFixedI16(buf, pColData->info.colId);
    tlen += taosEncodeFixedI16(buf, pColData->info.type);
    tlen += taosEncodeFixedI16(buf, pColData->info.bytes);
    int32_t colSz = rows * pColData->info.bytes;
    tlen += taosEncodeBinary(buf, pColData->pData, colSz);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeDataBlock(const void* buf, SSDataBlock* pBlock) {
  int32_t sz;

  buf = taosDecodeFixedI64(buf, &pBlock->info.uid);
  buf = taosDecodeFixedI32(buf, &pBlock->info.numOfCols);
  buf = taosDecodeFixedI32(buf, &pBlock->info.rows);
  buf = taosDecodeFixedI32(buf, &sz);
  pBlock->pDataBlock = taosArrayInit(sz, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData data = {0};
    buf = taosDecodeFixedI16(buf, &data.info.colId);
    buf = taosDecodeFixedI16(buf, &data.info.type);
    buf = taosDecodeFixedI16(buf, &data.info.bytes);
    int32_t colSz = pBlock->info.rows * data.info.bytes;
    buf = taosDecodeBinary(buf, (void**)&data.pData, colSz);
    taosArrayPush(pBlock->pDataBlock, &data);
  }
  return (void*)buf;
}

static FORCE_INLINE int32_t tEncodeSMqConsumeRsp(void** buf, const SMqConsumeRsp* pRsp) {
  int32_t tlen = 0;
  int32_t sz = 0;
  tlen += taosEncodeFixedI64(buf, pRsp->consumerId);
  tlen += taosEncodeFixedI64(buf, pRsp->committedOffset);
  tlen += taosEncodeFixedI64(buf, pRsp->reqOffset);
  tlen += taosEncodeFixedI64(buf, pRsp->rspOffset);
  tlen += taosEncodeFixedI32(buf, pRsp->skipLogNum);
  tlen += taosEncodeFixedI32(buf, pRsp->numOfTopics);
  if (pRsp->numOfTopics == 0) return tlen;
  tlen += tEncodeSSchemaWrapper(buf, pRsp->schemas);
  if (pRsp->pBlockData) {
    sz = taosArrayGetSize(pRsp->pBlockData);
  }
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock* pBlock = (SSDataBlock*)taosArrayGet(pRsp->pBlockData, i);
    tlen += tEncodeDataBlock(buf, pBlock);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeSMqConsumeRsp(void* buf, SMqConsumeRsp* pRsp) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pRsp->consumerId);
  buf = taosDecodeFixedI64(buf, &pRsp->committedOffset);
  buf = taosDecodeFixedI64(buf, &pRsp->reqOffset);
  buf = taosDecodeFixedI64(buf, &pRsp->rspOffset);
  buf = taosDecodeFixedI32(buf, &pRsp->skipLogNum);
  buf = taosDecodeFixedI32(buf, &pRsp->numOfTopics);
  if (pRsp->numOfTopics == 0) return buf;
  pRsp->schemas = (SSchemaWrapper*)calloc(1, sizeof(SSchemaWrapper));
  if (pRsp->schemas == NULL) return NULL;
  buf = tDecodeSSchemaWrapper(buf, pRsp->schemas);
  buf = taosDecodeFixedI32(buf, &sz);
  pRsp->pBlockData = taosArrayInit(sz, sizeof(SSDataBlock));
  for (int32_t i = 0; i < sz; i++) {
    SSDataBlock block = {0};
    tDecodeDataBlock(buf, &block);
    taosArrayPush(pRsp->pBlockData, &block);
  }
  return buf;
}

static FORCE_INLINE void tDeleteSSDataBlock(SSDataBlock* pBlock) {
  if (pBlock == NULL) {
    return;
  }

  // int32_t numOfOutput = pBlock->info.numOfCols;
  int32_t sz = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < sz; ++i) {
    SColumnInfoData* pColInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    tfree(pColInfoData->pData);
  }

  taosArrayDestroy(pBlock->pDataBlock);
  tfree(pBlock->pBlockAgg);
  // tfree(pBlock);
}

static FORCE_INLINE void tDeleteSMqConsumeRsp(SMqConsumeRsp* pRsp) {
  if (pRsp->schemas) {
    if (pRsp->schemas->nCols) {
      tfree(pRsp->schemas->pSchema);
    }
    free(pRsp->schemas);
  }
  taosArrayDestroyEx(pRsp->pBlockData, (void (*)(void*))tDeleteSSDataBlock);
  pRsp->pBlockData = NULL;
  // for (int i = 0; i < taosArrayGetSize(pRsp->pBlockData); i++) {
  // SSDataBlock* pDataBlock = (SSDataBlock*)taosArrayGet(pRsp->pBlockData, i);
  // tDeleteSSDataBlock(pDataBlock);
  //}
}

//======================================================================================================================
// the following structure shared by parser and executor
typedef struct SColumn {
  uint64_t    uid;
  char        name[TSDB_COL_NAME_LEN];
  int8_t      flag;  // column type: normal column, tag, or user-input column (integer/float/string)
  SColumnInfo info;
} SColumn;

typedef struct SLimit {
  int64_t limit;
  int64_t offset;
} SLimit;

typedef struct SOrder {
  uint32_t order;
  SColumn  col;
} SOrder;

typedef struct SGroupbyExpr {
  SArray* columnInfo;  // SArray<SColIndex>, group by columns information
  bool    groupbyTag;  // group by tag or column
} SGroupbyExpr;

// the structure for sql function in select clause
typedef struct SSqlExpr {
  char    token[TSDB_COL_NAME_LEN];  // original token
  SSchema resSchema;

  int32_t  numOfCols;
  SColumn* pColumns;     // data columns that are required by query
  int32_t  interBytes;   // inter result buffer size
  int16_t  numOfParams;  // argument value of each function
  SVariant param[3];     // parameters are not more than 3
} SSqlExpr;

typedef struct SExprInfo {
  struct SSqlExpr   base;
  struct tExprNode* pExpr;
} SExprInfo;

typedef struct SStateWindow {
  SColumn col;
} SStateWindow;

typedef struct SSessionWindow {
  int64_t gap;  // gap between two session window(in microseconds)
  SColumn col;
} SSessionWindow;

#define QUERY_ASC_FORWARD_STEP 1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#endif  // TDENGINE_COMMON_H
