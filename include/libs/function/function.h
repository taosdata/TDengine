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

#ifndef TDENGINE_FUNCTION_H
#define TDENGINE_FUNCTION_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tcommon.h"
#include "tsimplehash.h"
#include "tvariant.h"
#include "functionResInfo.h"

struct SqlFunctionCtx;
struct SResultRowEntryInfo;

struct SFunctionNode;
struct SExprSupp;
typedef struct SScalarParam SScalarParam;
typedef struct SStreamState SStreamState;

typedef struct SFuncExecEnv {
  int32_t calcMemSize;
} SFuncExecEnv;

typedef bool (*FExecGetEnv)(struct SFunctionNode *pFunc, SFuncExecEnv *pEnv);
typedef void (*FExecCleanUp)(struct SqlFunctionCtx* pCtx);
typedef int32_t (*FExecInit)(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo *pResultCellInfo);
typedef int32_t (*FExecProcess)(struct SqlFunctionCtx *pCtx);
typedef int32_t (*FExecFinalize)(struct SqlFunctionCtx *pCtx, SSDataBlock *pBlock);
typedef int32_t (*FScalarExecProcess)(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
typedef int32_t (*FExecCombine)(struct SqlFunctionCtx *pDestCtx, struct SqlFunctionCtx *pSourceCtx);
typedef int32_t (*FExecDecode)(struct SqlFunctionCtx *pCtx, const char *buf, struct SResultRowEntryInfo *pResultCellInfo, int32_t version);
typedef int32_t (*processFuncByRow)(SArray* pCtx);  // array of SqlFunctionCtx

typedef struct SScalarFuncExecFuncs {
  FExecGetEnv        getEnv;
  FScalarExecProcess process;
} SScalarFuncExecFuncs;

typedef struct SFuncExecFuncs {
  FExecGetEnv      getEnv;
  FExecInit        init;
  FExecProcess     process;
  FExecFinalize    finalize;
  FExecCombine     combine;
  FExecCleanUp     cleanup;
  FExecDecode      decode;
  processFuncByRow processFuncByRow;
} SFuncExecFuncs;

#define MAX_INTERVAL_TIME_WINDOW 10000000  // maximum allowed time windows in final results

#define TOP_BOTTOM_QUERY_LIMIT    100
#define FUNCTIONS_NAME_MAX_LENGTH 32

#define FUNCTION_RESULT_INFO_VERSION 1

typedef struct SResultRowEntryInfo {
  bool     initialized : 1;  // output buffer has been initialized
  bool     complete : 1;     // query has completed
  uint8_t  isNullRes : 6;    // the result is null
  uint16_t numOfRes;         // num of output result in current buffer. NOT NULL RESULT
} SResultRowEntryInfo;

// determine the real data need to calculated the result
enum {
  BLK_DATA_NOT_LOAD = 0x0,
  BLK_DATA_SMA_LOAD = 0x1,
  BLK_DATA_DATA_LOAD = 0x3,
  BLK_DATA_FILTEROUT = 0x4,  // discard current data block since it is not qualified for filter
};

enum {
  MAIN_SCAN = 0x0u,
  REVERSE_SCAN = 0x1u,  // todo remove it
  PRE_SCAN = 0x2u,      // pre-scan belongs to the main scan and occurs before main scan
};

struct SPoint1;
struct SqlFunctionCtx;
struct SResultRowEntryInfo;

// for selectivity query, the corresponding tag value is assigned if the data is qualified
typedef struct SSubsidiaryResInfo {
  int16_t                 num;
  int32_t                 rowLen;
  char                   *buf;  // serialize data buffer
  struct SqlFunctionCtx **pCtx;
} SSubsidiaryResInfo;

typedef struct SResultDataInfo {
  int16_t  precision;
  int16_t  scale;
  int16_t  type;
  uint16_t bytes;
  int32_t  interBufSize;
} SResultDataInfo;

#define GET_RES_INFO(ctx)        ((ctx)->resultInfo)
#define GET_ROWCELL_INTERBUF(_c) ((void *)((char *)(_c) + sizeof(SResultRowEntryInfo)))

typedef struct SInputColumnInfoData {
  int32_t           totalRows;        // total rows in current columnar data
  int32_t           startRowIndex;    // handle started row index
  int64_t           numOfRows;        // the number of rows needs to be handled
  bool              blankFill;        // fill blank data to block for empty table
  int32_t           numOfInputCols;   // PTS is not included
  bool              colDataSMAIsSet;  // if agg is set or not
  SColumnInfoData  *pPTS;             // primary timestamp column
  SColumnInfoData  *pPrimaryKey;      // primary key column
  SColumnInfoData **pData;
  SColumnDataAgg  **pColumnDataAgg;
  uint64_t uid;  // table uid, used to set the tag value when building the final query result for selectivity functions.
} SInputColumnInfoData;

typedef struct SSerializeDataHandle {
  struct SDiskbasedBuf *pBuf;
  int32_t               currentPage;
  SStreamState          *pState;
} SSerializeDataHandle;

// incremental state storage

typedef struct SBackendCfWrapper {
  void          *rocksdb;
  void         **pHandle;
  void          *writeOpts;
  void          *readOpts;
  void         **cfOpts;
  void          *dbOpt;
  void          *param;
  void          *env;
  SListNode     *pComparNode;
  void          *pBackend;
  void          *compactFactory;
  TdThreadRwlock rwLock;
  bool           remove;
  int64_t        backendId;
  char           idstr[64];
} SBackendCfWrapper;

typedef struct STdbState {
  SBackendCfWrapper *pBackendCfWrapper;
  int64_t            backendCfWrapperId;
  char               idstr[64];

  struct SStreamTask *pOwner;
  void               *db;
  void               *pStateDb;
  void               *pFuncStateDb;
  void               *pFillStateDb;  // todo refactor
  void               *pSessionStateDb;
  void               *pParNameDb;
  void               *pParTagDb;
  void               *txn;
} STdbState;

typedef struct SResultRowStore {
  int32_t (*resultRowPut)(struct SExprSupp *pSup, const char* inBuf, size_t inBufSize, char **outBuf, size_t *outBufSize);
  int32_t (*resultRowGet)(struct SExprSupp *pSup, const char* inBuf, size_t inBufSize, char **outBuf, size_t *outBufSize);
} SResultRowStore;

struct SStreamState {
  STdbState               *pTdbState;
  struct SStreamFileState *pFileState;
  int32_t                  number;
  SSHashObj               *parNameMap;
  int32_t                  taskId;
  int64_t                  streamId;
  int64_t                  streamBackendRid;
  int8_t                   dump;
  int32_t                  tsIndex;
  SResultRowStore          pResultRowStore;
  struct SExprSupp        *pExprSupp;
};

typedef struct SFunctionStateStore {
  int32_t (*streamStateFuncPut)(SStreamState *pState, const SWinKey *key, const void *value, int32_t vLen);
  int32_t (*streamStateFuncGet)(SStreamState *pState, const SWinKey *key, void **ppVal, int32_t *pVLen);
} SFunctionStateStore;

typedef struct SFuncInputRow {
  TSKEY ts;
  bool isDataNull;
  char* pData;
  char* pPk;

  SSDataBlock* block; // prev row block or src block
  int32_t rowIndex; // prev row block ? 0 : rowIndex in srcBlock

  //TODO:
  // int32_t startOffset; // for diff, derivative
  // SPoint1 startPoint; // for twa
} SFuncInputRow;

typedef struct SFuncInputRowIter {
  bool  hasPrev;
 
  SInputColumnInfoData* pInput;
  SColumnInfoData* pDataCol;
  SColumnInfoData* pPkCol;
  TSKEY* tsList;
  int32_t rowIndex;
  int32_t inputEndIndex;
  SSDataBlock* pSrcBlock;

  TSKEY prevBlockTsEnd;
  bool prevIsDataNull;
  char* pPrevData;
  char* pPrevPk;
  SSDataBlock* pPrevRowBlock; // pre one row block

  uint64_t groupId;
  bool hasGroupId;

  bool finalRow;
} SFuncInputRowIter;

// sql function runtime context
typedef struct SqlFunctionCtx {
  SInputColumnInfoData input;
  SResultDataInfo      resDataInfo;
  uint32_t             order;          // data block scanner order: asc|desc
  uint8_t              isPseudoFunc;   // denote current function is pseudo function or not [added for perf reason]
  uint8_t              isNotNullFunc;  // not return null value.
  uint8_t              scanFlag;       // record current running step, default: 0
  int16_t              functionId;     // function id
  char                *pOutput;        // final result output buffer, point to sdata->data
  // input parameter, e.g., top(k, 20), the number of results of top query is kept in param
  SFunctParam *param;
  // corresponding output buffer for timestamp of each result, e.g., diff/csum
  SColumnInfoData     *pTsOutput;
  int32_t              numOfParams;
  int32_t              offset;
  SResultRowEntryInfo *resultInfo;
  SSubsidiaryResInfo   subsidiaries;
  SPoint1              start;
  SPoint1              end;
  SFuncExecFuncs       fpSet;
  SScalarFuncExecFuncs sfp;
  struct SExprInfo    *pExpr;
  struct SSDataBlock  *pSrcBlock;
  struct SSDataBlock  *pDstBlock;  // used by indefinite rows function to set selectivity
  SSerializeDataHandle saveHandle;
  int32_t              exprIdx;
  char                *udfName;
  SFunctionStateStore *pStore;
  bool                 hasPrimaryKey;
  SFuncInputRowIter    rowIter;
  bool                 bInputFinished;
  bool                 hasWindowOrGroup; // denote that the function is used with time window or group
  bool                 needCleanup; // denote that the function need to be cleaned up
} SqlFunctionCtx;

typedef struct tExprNode {
  int32_t nodeType;
  union {
    struct {                                                          // function node
      char                  functionName[FUNCTIONS_NAME_MAX_LENGTH];  // todo refactor
      int32_t               functionId;
      int32_t               num;
      struct SFunctionNode *pFunctNode;
      int32_t               functionType;
    } _function;

    struct {
      struct SNode *pRootNode;
    } _optrRoot;
  };
} tExprNode;

struct SScalarParam {
  bool             colAlloced;
  SColumnInfoData *columnData;
  SHashObj        *pHashFilter;
  int32_t          hashValueType;
  void            *param;  // other parameter, such as meta handle from vnode, to extract table name/tag value
  int32_t          numOfRows;
  int32_t          numOfQualified;  // number of qualified elements in the final results
};

#define cleanupResultRowEntry(p)  p->initialized = false
#define isRowEntryCompleted(p)   (p->complete)
#define isRowEntryInitialized(p) (p->initialized)

typedef struct SPoint {
  int64_t key;
  void   *val;
} SPoint;

void taosGetLinearInterpolationVal(SPoint *point, int32_t outputType, SPoint *point1, SPoint *point2,
                                      int32_t inputType);

#define LEASTSQUARES_DOUBLE_ITEM_LENGTH 25
#define LEASTSQUARES_BUFF_LENGTH 128
#define DOUBLE_PRECISION_DIGITS "16e"

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FUNCTION_H
