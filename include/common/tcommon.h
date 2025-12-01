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

#include "tarray.h"
#include "tmsg.h"
#include "tvariant.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define IS_META_MSG(x) ( \
     x == TDMT_VND_CREATE_STB     \
  || x == TDMT_VND_ALTER_STB      \
  || x == TDMT_VND_DROP_STB       \
  || x == TDMT_VND_CREATE_TABLE   \
  || x == TDMT_VND_ALTER_TABLE    \
  || x == TDMT_VND_DROP_TABLE     \
  || x == TDMT_VND_DELETE         \
)
// clang-format on

typedef bool (*state_key_cmpr_fn)(void* pKey1, void* pKey2);

typedef struct STableKeyInfo {
  uint64_t uid;
  uint64_t groupId;
} STableKeyInfo;

typedef struct SWinKey {
  uint64_t groupId;
  TSKEY    ts;
  int32_t  numInGroup;
} SWinKey;

typedef struct SSessionKey {
  STimeWindow win;
  uint64_t    groupId;
} SSessionKey;

typedef int64_t COUNT_TYPE;

typedef struct SVersionRange {
  int64_t minVer;
  int64_t maxVer;
} SVersionRange;

static inline int winKeyCmprImpl(const void* pKey1, const void* pKey2) {
  SWinKey* pWin1 = (SWinKey*)pKey1;
  SWinKey* pWin2 = (SWinKey*)pKey2;

  if (pWin1->groupId > pWin2->groupId) {
    return 1;
  } else if (pWin1->groupId < pWin2->groupId) {
    return -1;
  }

  if (pWin1->ts > pWin2->ts) {
    return 1;
  } else if (pWin1->ts < pWin2->ts) {
    return -1;
  }

  return 0;
}

static inline int winKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  return winKeyCmprImpl(pKey1, pKey2);
}

typedef struct {
  uint64_t groupId;
  TSKEY    ts;
  int32_t  exprIdx;
} STupleKey;

static inline int STupleKeyCmpr(const void* pKey1, int kLen1, const void* pKey2, int kLen2) {
  STupleKey* pTuple1 = (STupleKey*)pKey1;
  STupleKey* pTuple2 = (STupleKey*)pKey2;

  if (pTuple1->groupId > pTuple2->groupId) {
    return 1;
  } else if (pTuple1->groupId < pTuple2->groupId) {
    return -1;
  }

  if (pTuple1->ts > pTuple2->ts) {
    return 1;
  } else if (pTuple1->ts < pTuple2->ts) {
    return -1;
  }

  if (pTuple1->exprIdx > pTuple2->exprIdx) {
    return 1;
  } else if (pTuple1->exprIdx < pTuple2->exprIdx) {
    return -1;
  }

  return 0;
}

enum {
  TMQ_MSG_TYPE__POLL_DATA_RSP = 0,
  TMQ_MSG_TYPE__POLL_META_RSP,
  TMQ_MSG_TYPE__EP_RSP,
  TMQ_MSG_TYPE__POLL_DATA_META_RSP,
  TMQ_MSG_TYPE__WALINFO_RSP,
  TMQ_MSG_TYPE__POLL_BATCH_META_RSP,
  TMQ_MSG_TYPE__POLL_RAW_DATA_RSP,
};

static const char* const tmqMsgTypeStr[] = {
    "data", "meta", "ask ep", "meta data", "wal info", "batch meta", "raw data"
};

enum {
  STREAM_INPUT__DATA_SUBMIT = 1,
  STREAM_INPUT__DATA_BLOCK,
  STREAM_INPUT__MERGED_SUBMIT,
  STREAM_INPUT__RECALCULATE,
  STREAM_INPUT__DATA_RETRIEVE,
  STREAM_INPUT__GET_RES,
  STREAM_INPUT__CHECKPOINT,
  STREAM_INPUT__CHECKPOINT_TRIGGER,
  STREAM_INPUT__TRANS_STATE,
  STREAM_INPUT__REF_DATA_BLOCK,
  STREAM_INPUT__DESTROY,
};

typedef enum EStreamType {
  STREAM_NORMAL = 1,
  STREAM_INVERT,
  STREAM_CLEAR,
  STREAM_INVALID,
  STREAM_GET_ALL,
  STREAM_DELETE_RESULT,
  STREAM_DELETE_DATA,
  STREAM_RETRIEVE,
  STREAM_PULL_DATA,
  STREAM_PULL_OVER,
  STREAM_FILL_OVER,
  STREAM_CHECKPOINT,
  STREAM_CREATE_CHILD_TABLE,
  STREAM_TRANS_STATE,
  STREAM_MID_RETRIEVE,
  STREAM_PARTITION_DELETE_DATA,
  STREAM_GET_RESULT,
  STREAM_DROP_CHILD_TABLE,
  STREAM_NOTIFY_EVENT,
  STREAM_RECALCULATE_DATA,
  STREAM_RECALCULATE_DELETE,
  STREAM_RECALCULATE_START,
  STREAM_RECALCULATE_END,
} EStreamType;

#pragma pack(push, 1)
typedef struct SColumnDataAgg {
  int32_t colId;
  int16_t numOfNull;
  union {
    struct {
      int64_t sum;
      int64_t max;
      int64_t min;
    };
    struct {
      uint64_t decimal128Sum[2];
      uint64_t decimal128Max[2];
      uint64_t decimal128Min[2];
      uint8_t  overflow;
    };
  };
} SColumnDataAgg;
#pragma pack(pop)

#define DECIMAL_AGG_FLAG 0x80000000

#define COL_AGG_GET_SUM_PTR(pAggs, dataType) \
  (!IS_DECIMAL_TYPE(dataType) ? (void*)&pAggs->sum : (void*)pAggs->decimal128Sum)

#define COL_AGG_GET_MAX_PTR(pAggs, dataType) \
  (!IS_DECIMAL_TYPE(dataType) ? (void*)&pAggs->max : (void*)pAggs->decimal128Max)

#define COL_AGG_GET_MIN_PTR(pAggs, dataType) \
  (!IS_DECIMAL_TYPE(dataType) ? (void*)&pAggs->min : (void*)pAggs->decimal128Min)

typedef struct SBlockID {
  // The uid of table, from which current data block comes. And it is always 0, if current block is the
  // result of calculation.
  uint64_t uid;

  // Block id, acquired and assigned from executor, which created according to the hysical planner. Block id is used
  // to mark the stage of exec task.
  uint64_t blockId;

  // Generated by group/partition by [value|tags]. Created and assigned by table-scan operator, group-by operator,
  // and partition by operator.
  uint64_t groupId;
} SBlockID;

typedef struct SPkInfo {
  int8_t  type;
  int32_t bytes;
  union {
    int64_t  val;
    uint8_t* pData;
  } skey;
  union {
    int64_t  val;
    uint8_t* pData;
  } ekey;
} SPkInfo;

typedef struct SDataBlockInfo {
  STimeWindow window;
  int32_t     rowSize;
  uint32_t    capacity;
  int64_t     rows;  // todo hide this attribute
  SBlockID    id;
  int16_t     hasVarCol;
  int16_t     dataLoad;  // denote if the data is loaded or not
  uint8_t     scanFlag;
  bool        blankFill;
  SValue      pks[2];

  // TODO: optimize and remove following
  int64_t     version;    // used for stream, and need serialization
  int32_t     childId;    // used for stream, do not serialize
  EStreamType type;       // used for stream, do not serialize
  STimeWindow calWin;     // used for stream, do not serialize
  TSKEY       watermark;  // used for stream

  char parTbName[TSDB_TABLE_NAME_LEN];  // used for stream partition
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
  char* pData;  // the corresponding block data in memory
  union {
    char*       nullbitmap;  // bitmap, one bit for each item in the list
    SVarColAttr varmeta;
  };
  SColumnInfo info;        // column info
  bool        hasNull;     // if current column data has null value.
  bool        reassigned;  // if current column data is reassigned.
} SColumnInfoData;

typedef struct SQueryTableDataCond {
  uint64_t     suid;
  int32_t      order;  // desc|asc order to iterate the data block
  int32_t      numOfCols;
  SColumnInfo* colList;
  int32_t*     pSlotList;  // the column output destation slot, and it may be null
  int32_t      type;       // data block load type:
  bool         skipRollup;
  STimeWindow  twindows;
  STimeWindow  extTwindows[2];
  int64_t      startVersion;
  int64_t      endVersion;
  bool         notLoadData;  // response the actual data, not only the rows in the attribute of info.row of ssdatablock
  bool         cacheSttStatis;
} SQueryTableDataCond;

int32_t tEncodeDataBlock(void** buf, const SSDataBlock* pBlock);
void*   tDecodeDataBlock(const void* buf, SSDataBlock* pBlock);

void colDataDestroy(SColumnInfoData* pColData);

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
  int16_t colType;  // column type: normal column, tag, or window column
  int16_t type;
  int32_t bytes;
  uint8_t precision;
  uint8_t scale;
} SColumn;

typedef struct STableBlockDistInfo {
  uint32_t rowSize;
  uint16_t numOfFiles;
  uint32_t numOfTables;
  uint32_t numOfBlocks;
  uint64_t totalSize;
  uint64_t totalRows;
  int32_t  maxRows;
  int32_t  minRows;
  int32_t  defMinRows;
  int32_t  defMaxRows;
  int32_t  firstSeekTimeUs;
  uint32_t numOfInmemRows;
  uint32_t numOfSttRows;
  uint32_t numOfVgroups;
  int32_t  blockRowsHisto[20];
} STableBlockDistInfo;

int32_t tSerializeBlockDistInfo(void* buf, int32_t bufLen, const STableBlockDistInfo* pInfo);
int32_t tDeserializeBlockDistInfo(void* buf, int32_t bufLen, STableBlockDistInfo* pInfo);

typedef struct SDBBlockUsageInfo {
  uint64_t dataInDiskSize;
  uint64_t walInDiskSize;
  uint64_t rawDataSize;
} SDBBlockUsageInfo;

int32_t tSerializeBlockDbUsage(void* buf, int32_t bufLen, const SDBBlockUsageInfo* pInfo);
int32_t tDeserializeBlockDbUsage(void* buf, int32_t bufLen, SDBBlockUsageInfo* pInfo);

enum {
  FUNC_PARAM_TYPE_VALUE = 0x1,
  FUNC_PARAM_TYPE_COLUMN = 0x2,
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

typedef struct SAggSupporter  SAggSupporter;
typedef struct SExprSupp      SExprSupp;
typedef struct SGroupResInfo  SGroupResInfo;
typedef struct SResultRow     SResultRow;
typedef struct SResultRowInfo SResultRowInfo;
typedef struct SExecTaskInfo  SExecTaskInfo;
typedef struct SRollupCtx {
  void*           pTsdb;     // STsdb*
  void*           pTargets;  // SNodeList*
  void*           pBuf;
  SExprSupp*      exprSup;
  SAggSupporter*  aggSup;
  SResultRow*     resultRow;
  SResultRowInfo* resultRowInfo;
  SGroupResInfo*  pGroupResInfo;
  SExecTaskInfo*  pTaskInfo;
  SSDataBlock*    pInputBlock;  // input data block for rollup
  SSDataBlock*    pResBlock;    // result data block for rollup
  SArray*         pColValArr;   // used the generate the aggregate row
  int32_t         rowSize;
  int32_t         maxBufRows;    // max buffer rows for aggregation
  int64_t         winTotalRows;  // number of total rows for current window
  int64_t         winStartTs;    // start timestamp of current window
} SRollupCtx;

typedef struct {
  const char* key;
  size_t      keyLen;
  uint8_t     type;
  union {
    const char* value;
    int64_t     i;
    uint64_t    u;
    double      d;
    float       f;
  };
  size_t length;
  bool   keyEscaped;
  bool   valueEscaped;
} SSmlKv;

#define QUERY_ASC_FORWARD_STEP  1
#define QUERY_DESC_FORWARD_STEP -1

#define GET_FORWARD_DIRECTION_FACTOR(ord) (((ord) == TSDB_ORDER_ASC) ? QUERY_ASC_FORWARD_STEP : QUERY_DESC_FORWARD_STEP)

#define SORT_QSORT_T              0x1
#define SORT_SPILLED_MERGE_SORT_T 0x2
typedef struct SSortExecInfo {
  int32_t sortMethod;
  int32_t sortBuffer;
  int32_t loops;       // loop count
  int32_t writeBytes;  // write io bytes
  int32_t readBytes;   // read io bytes
} SSortExecInfo;

typedef struct SNonSortExecInfo {
  int32_t blkNums;
} SNonSortExecInfo;

typedef struct STUidTagInfo {
  char*    name;
  uint64_t uid;
  void*    pTagVal;
} STUidTagInfo;

// stream special block column

#define START_TS_COLUMN_INDEX           0
#define END_TS_COLUMN_INDEX             1
#define UID_COLUMN_INDEX                2
#define GROUPID_COLUMN_INDEX            3
#define CALCULATE_START_TS_COLUMN_INDEX 4
#define CALCULATE_END_TS_COLUMN_INDEX   5
#define TABLE_NAME_COLUMN_INDEX         6
#define PRIMARY_KEY_COLUMN_INDEX        7

//steam get result block column
#define DATA_TS_COLUMN_INDEX            0
#define DATA_VERSION_COLUMN_INDEX       1

// stream create table block column
#define UD_TABLE_NAME_COLUMN_INDEX 0
#define UD_GROUPID_COLUMN_INDEX    1
#define UD_TAG_COLUMN_INDEX        2

// stream notify event block column
#define NOTIFY_EVENT_STR_COLUMN_INDEX 0

int32_t taosGenCrashJsonMsg(int signum, char** pMsg, int64_t clusterId, int64_t startTime);
int32_t dumpConfToDataBlock(SSDataBlock* pBlock, int32_t startCol, char* likePattern);

#define TSMA_RES_STB_POSTFIX          "_tsma_res_stb_"
#define MD5_OUTPUT_LEN                32
#define TSMA_RES_STB_EXTRA_COLUMN_NUM 4  // 3 columns: _wstart, _wend, _wduration, 1 tag: tbname

static inline bool isTsmaResSTb(const char* stbName) {
  static bool showTsmaTables = true;
  if (showTsmaTables) return false;
  const char* pos = strstr(stbName, TSMA_RES_STB_POSTFIX);
  if (pos && strlen(stbName) == (pos - stbName) + strlen(TSMA_RES_STB_POSTFIX)) {
    return true;
  }
  return false;
}

static inline STypeMod typeGetTypeModFromColInfo(const SColumnInfo* pCol) {
  return typeGetTypeMod(pCol->type, pCol->precision, pCol->scale, pCol->bytes);
}

static inline STypeMod typeGetTypeModFromCol(const SColumn* pCol) {
  return typeGetTypeMod(pCol->type, pCol->precision, pCol->scale, pCol->bytes);
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_DEF_H_*/
