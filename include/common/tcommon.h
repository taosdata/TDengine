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

enum {
  STREAM_TRIGGER__AT_ONCE = 1,
  STREAM_TRIGGER__WINDOW_CLOSE,
  STREAM_TRIGGER__BY_COUNT,
  STREAM_TRIGGER__BY_BATCH_COUNT,
  STREAM_TRIGGER__BY_EVENT_TIME,
};

typedef struct {
  uint32_t  numOfTables;
  SArray*   pGroupList;
  SHashObj* map;  // speedup acquire the tableQueryInfo by table uid
} STableGroupInfo;

typedef struct SColumnDataAgg {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SColumnDataAgg;

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rows;
  int32_t     rowSize;
  union {
    int64_t uid;        // from which table of uid, comes from this data block
    int64_t blockId;
  };
  uint64_t    groupId;  // no need to serialize
  int16_t     numOfCols;
  int16_t     hasVarCol;
  int16_t     capacity;
} SDataBlockInfo;

typedef struct SSDataBlock {
  SColumnDataAgg* pBlockAgg;
  SArray*         pDataBlock;  // SArray<SColumnInfoData>
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

void*   blockDataDestroy(SSDataBlock* pBlock);
int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock);
void*   tDecodeDataBlock(const void* buf, SSDataBlock* pBlock);

int32_t tEncodeDataBlocks(void** buf, const SArray* blocks);
void*   tDecodeDataBlocks(const void* buf, SArray** blocks);
void    colDataDestroy(SColumnInfoData* pColData);

static FORCE_INLINE void blockDestroyInner(SSDataBlock* pBlock) {
  // WARNING: do not use info.numOfCols,
  // sometimes info.numOfCols != array size
  int32_t numOfOutput = taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData* pColInfoData = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
    colDataDestroy(pColInfoData);
  }

  taosArrayDestroy(pBlock->pDataBlock);
  taosMemoryFreeClear(pBlock->pBlockAgg);
}

static FORCE_INLINE void tDeleteSSDataBlock(SSDataBlock* pBlock) { blockDestroyInner(pBlock); }

static FORCE_INLINE int32_t tEncodeSMqPollRsp(void** buf, const SMqPollRsp* pRsp) {
  int32_t tlen = 0;
  int32_t sz = 0;
  // tlen += taosEncodeFixedI64(buf, pRsp->consumerId);
  tlen += taosEncodeFixedI64(buf, pRsp->reqOffset);
  tlen += taosEncodeFixedI64(buf, pRsp->rspOffset);
  tlen += taosEncodeFixedI32(buf, pRsp->skipLogNum);
  tlen += taosEncodeFixedI32(buf, pRsp->numOfTopics);
  if (pRsp->numOfTopics == 0) return tlen;
  tlen += taosEncodeSSchemaWrapper(buf, pRsp->schema);
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
  // buf = taosDecodeFixedI64(buf, &pRsp->consumerId);
  buf = taosDecodeFixedI64(buf, &pRsp->reqOffset);
  buf = taosDecodeFixedI64(buf, &pRsp->rspOffset);
  buf = taosDecodeFixedI32(buf, &pRsp->skipLogNum);
  buf = taosDecodeFixedI32(buf, &pRsp->numOfTopics);
  if (pRsp->numOfTopics == 0) return buf;
  pRsp->schema = (SSchemaWrapper*)taosMemoryCalloc(1, sizeof(SSchemaWrapper));
  if (pRsp->schema == NULL) return NULL;
  buf = taosDecodeSSchemaWrapper(buf, pRsp->schema);
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
  if (pRsp->schema) {
    if (pRsp->schema->nCols) {
      taosMemoryFreeClear(pRsp->schema->pSchema);
    }
    taosMemoryFree(pRsp->schema);
  }
  taosArrayDestroyEx(pRsp->pBlockData, (void (*)(void*))blockDestroyInner);
  pRsp->pBlockData = NULL;
}

//======================================================================================================================
// the following structure shared by parser and executor
typedef struct SColumn {
  union {
    uint64_t uid;
    int64_t  dataBlockId;
  };

  int16_t colId;
  int16_t slotId;

  char    name[TSDB_COL_NAME_LEN];
  int8_t  flag;  // column type: normal column, tag, or user-input column (integer/float/string)
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SColumn;

typedef struct STableBlockDistInfo {
  uint16_t  rowSize;
  uint16_t  numOfFiles;
  uint32_t  numOfTables;
  uint64_t  totalSize;
  uint64_t  totalRows;
  int32_t   maxRows;
  int32_t   minRows;
  int32_t   firstSeekTimeUs;
  uint32_t  numOfRowsInMemTable;
  uint32_t  numOfSmallBlocks;
  SArray   *dataBlockInfos;
} STableBlockDistInfo;

enum {
  FUNC_PARAM_TYPE_VALUE = 0x1,
  FUNC_PARAM_TYPE_COLUMN= 0x2,
};

typedef struct SFunctParam {
  int32_t  type;
  SColumn* pCol;
  SVariant param;
} SFunctParam;

// the structure for sql function in select clause
typedef struct SResSchame {
  int8_t  type;
  int32_t slotId;
  int32_t bytes;
  int32_t precision;
  int32_t scale;
  char    name[TSDB_COL_NAME_LEN];
} SResSchema;

// TODO move away to executor.h
typedef struct SExprBasicInfo {
  SResSchema   resSchema;
  int16_t      numOfParams;  // argument value of each function
  SFunctParam* pParam;
} SExprBasicInfo;

typedef struct SExprInfo {
  struct SExprBasicInfo base;
  struct tExprNode*     pExpr;
} SExprInfo;

#define QUERY_ASC_FORWARD_STEP 1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DEF_H_*/
