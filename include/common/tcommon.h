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

#ifndef _TD_COMMON_DEF_H_
#define _TD_COMMON_DEF_H_

#include "taosdef.h"
#include "tarray.h"
#include "tmsg.h"
#include "tvariant.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  TMQ_CONF__RESET_OFFSET__LATEST = -1,
  TMQ_CONF__RESET_OFFSET__EARLIEAST = -2,
  TMQ_CONF__RESET_OFFSET__NONE = -3,
};

enum {
  TMQ_MSG_TYPE__DUMMY = 0,
  TMQ_MSG_TYPE__POLL_RSP,
  TMQ_MSG_TYPE__EP_RSP,
};

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
  STimeWindow    window;
  int32_t        rows;
  int32_t        rowSize;
  int16_t        numOfCols;
  int16_t        hasVarCol;
  union {int64_t uid; int64_t blockId;};
} SDataBlockInfo;

typedef struct SConstantItem {
  SColumnInfo info;
  int32_t     startRow;  // run-length-encoding to save the space for multiple rows
  int32_t     endRow;
  SVariant    value;
} SConstantItem;

// info.numOfCols = taosArrayGetSize(pDataBlock) + taosArrayGetSize(pConstantList);
typedef struct SSDataBlock {
  SColumnDataAgg *pBlockAgg;
  SArray         *pDataBlock;    // SArray<SColumnInfoData>
  SArray         *pConstantList; // SArray<SConstantItem>, it is a constant/tags value of the corresponding result value.
  SDataBlockInfo  info;
} SSDataBlock;

typedef struct SVarColAttr {
  int32_t* offset;    // start position for each entry in the list
  uint32_t length;    // used buffer size that contain the valid data
  uint32_t allocLen;  // allocated buffer size
} SVarColAttr;

// pBlockAgg->numOfNull == info.rows, all data are null
// pBlockAgg->numOfNull == 0, no data are null.
typedef struct SColumnInfoData {
  SColumnInfo info;     // TODO filter info needs to be removed
  bool        hasNull;  // if current column data has null value.
  char*       pData;    // the corresponding block data in memory
  union {
    char*       nullbitmap;  // bitmap, one bit for each item in the list
    SVarColAttr varmeta;
  };
} SColumnInfoData;

static FORCE_INLINE int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock) {
  int64_t tbUid = pBlock->info.uid;
  int16_t numOfCols = pBlock->info.numOfCols;
  int16_t hasVarCol = pBlock->info.hasVarCol;
  int32_t rows = pBlock->info.rows;
  int32_t sz = taosArrayGetSize(pBlock->pDataBlock);

  int32_t tlen = 0;
  tlen += taosEncodeFixedI64(buf, tbUid);
  tlen += taosEncodeFixedI16(buf, numOfCols);
  tlen += taosEncodeFixedI16(buf, hasVarCol);
  tlen += taosEncodeFixedI32(buf, rows);
  tlen += taosEncodeFixedI32(buf, sz);
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData* pColData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    tlen += taosEncodeFixedI16(buf, pColData->info.colId);
    tlen += taosEncodeFixedI16(buf, pColData->info.type);
    tlen += taosEncodeFixedI32(buf, pColData->info.bytes);
    int32_t colSz = rows * pColData->info.bytes;
    tlen += taosEncodeBinary(buf, pColData->pData, colSz);
  }
  return tlen;
}

static FORCE_INLINE void* tDecodeDataBlock(const void* buf, SSDataBlock* pBlock) {
  int32_t sz;

  buf = taosDecodeFixedI64(buf, &pBlock->info.uid);
  buf = taosDecodeFixedI16(buf, &pBlock->info.numOfCols);
  buf = taosDecodeFixedI16(buf, &pBlock->info.hasVarCol);
  buf = taosDecodeFixedI32(buf, &pBlock->info.rows);
  buf = taosDecodeFixedI32(buf, &sz);
  pBlock->pDataBlock = taosArrayInit(sz, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < sz; i++) {
    SColumnInfoData data = {0};
    buf = taosDecodeFixedI16(buf, &data.info.colId);
    buf = taosDecodeFixedI16(buf, &data.info.type);
    buf = taosDecodeFixedI32(buf, &data.info.bytes);
    int32_t colSz = pBlock->info.rows * data.info.bytes;
    buf = taosDecodeBinary(buf, (void**)&data.pData, colSz);
    taosArrayPush(pBlock->pDataBlock, &data);
  }
  return (void*)buf;
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

static FORCE_INLINE int32_t tEncodeSMqPollRsp(void** buf, const SMqPollRsp* pRsp) {
  int32_t tlen = 0;
  int32_t sz = 0;
  tlen += taosEncodeFixedI64(buf, pRsp->consumerId);
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

static FORCE_INLINE void* tDecodeSMqPollRsp(void* buf, SMqPollRsp* pRsp) {
  int32_t sz;
  buf = taosDecodeFixedI64(buf, &pRsp->consumerId);
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

static FORCE_INLINE void tDeleteSMqConsumeRsp(SMqPollRsp* pRsp) {
  if (pRsp->schemas) {
    if (pRsp->schemas->nCols) {
      tfree(pRsp->schemas->pSchema);
    }
    free(pRsp->schemas);
  }
  taosArrayDestroyEx(pRsp->pBlockData, (void (*)(void*))tDeleteSSDataBlock);
  pRsp->pBlockData = NULL;
}

//======================================================================================================================
// the following structure shared by parser and executor
typedef struct SColumn {
  union {
    uint64_t uid;
    int64_t  dataBlockId;
  };

  union {
    int16_t colId;
    int16_t slotId;
  };

  char    name[TSDB_COL_NAME_LEN];
  int8_t  flag;  // column type: normal column, tag, or user-input column (integer/float/string)
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
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

typedef struct SFunctParam {
  int32_t  type;
  SColumn *pCol;
  SVariant param;
} SFunctParam;

// the structure for sql function in select clause
typedef struct SResSchame {
  int8_t  type;
  int32_t colId;
  int32_t bytes;
  int32_t precision;
  int32_t scale;
  char    name[TSDB_COL_NAME_LEN];
} SResSchema;

// TODO move away to executor.h
typedef struct SExprBasicInfo {
  SResSchema   resSchema;
  int16_t      numOfParams;  // argument value of each function
  SFunctParam *pParam;
} SExprBasicInfo;

typedef struct SExprInfo {
  struct SExprBasicInfo  base;
  struct tExprNode      *pExpr;
} SExprInfo;

typedef struct SStateWindow {
  SColumn col;
} SStateWindow;

typedef struct SSessionWindow {
  int64_t gap;  // gap between two session window(in microseconds)
  SColumn col;
} SSessionWindow;

#define QUERY_ASC_FORWARD_STEP  1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DEF_H_*/
