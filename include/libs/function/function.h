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

#include "tbuffer.h"
#include "tcommon.h"
#include "tvariant.h"

struct SqlFunctionCtx;
struct SResultRowEntryInfo;

struct SFunctionNode;
typedef struct SScalarParam SScalarParam;

typedef struct SFuncExecEnv {
  int32_t calcMemSize;
} SFuncExecEnv;

typedef bool (*FExecGetEnv)(struct SFunctionNode* pFunc, SFuncExecEnv* pEnv);
typedef bool (*FExecInit)(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo* pResultCellInfo);
typedef int32_t (*FExecProcess)(struct SqlFunctionCtx *pCtx);
typedef int32_t (*FExecFinalize)(struct SqlFunctionCtx *pCtx, SSDataBlock* pBlock);
typedef int32_t (*FScalarExecProcess)(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

typedef struct SScalarFuncExecFuncs {
  FExecGetEnv getEnv;
  FScalarExecProcess process;
} SScalarFuncExecFuncs;

typedef struct SFuncExecFuncs {
  FExecGetEnv getEnv;
  FExecInit init;
  FExecProcess process;
  FExecFinalize finalize;
} SFuncExecFuncs;

typedef struct SFileBlockInfo {
  int32_t numBlocksOfStep;
} SFileBlockInfo;

#define TSDB_BLOCK_DIST_STEP_ROWS 8
#define MAX_INTERVAL_TIME_WINDOW  1000000  // maximum allowed time windows in final results

#define FUNCTION_TYPE_SCALAR       1
#define FUNCTION_TYPE_AGG          2

#define TOP_BOTTOM_QUERY_LIMIT    100
#define FUNCTIONS_NAME_MAX_LENGTH 16

#define FUNCTION_INVALID_ID  -1
#define FUNCTION_COUNT        0
#define FUNCTION_SUM          1
#define FUNCTION_AVG          2
#define FUNCTION_MIN          3
#define FUNCTION_MAX          4
#define FUNCTION_STDDEV       5
#define FUNCTION_PERCT        6
#define FUNCTION_APERCT       7
#define FUNCTION_FIRST        8
#define FUNCTION_LAST         9
#define FUNCTION_LAST_ROW     10
#define FUNCTION_TOP          11
#define FUNCTION_BOTTOM       12
#define FUNCTION_SPREAD       13
#define FUNCTION_TWA          14
#define FUNCTION_LEASTSQR     15

#define FUNCTION_TS           16
#define FUNCTION_TS_DUMMY     17
#define FUNCTION_TAG_DUMMY    18
#define FUNCTION_TS_COMP      19

#define FUNCTION_TAG          20
#define FUNCTION_PRJ          21

#define FUNCTION_TAGPRJ       22
#define FUNCTION_ARITHM       23
#define FUNCTION_DIFF         24

#define FUNCTION_FIRST_DST    25
#define FUNCTION_LAST_DST     26
#define FUNCTION_STDDEV_DST   27
#define FUNCTION_INTERP       28

#define FUNCTION_RATE         29
#define FUNCTION_IRATE        30
#define FUNCTION_TID_TAG      31
#define FUNCTION_DERIVATIVE   32
#define FUNCTION_BLKINFO      33


#define FUNCTION_COV          38

typedef struct SResultRowEntryInfo {
  bool     initialized:1;     // output buffer has been initialized
  bool     complete:1;        // query has completed
  uint8_t  isNullRes:6;       // the result is null
  uint8_t  numOfRes;          // num of output result in current buffer
} SResultRowEntryInfo;

// determine the real data need to calculated the result
enum {
  BLK_DATA_NOT_LOAD  = 0x0,
  BLK_DATA_SMA_LOAD  = 0x1,
  BLK_DATA_DATA_LOAD = 0x3,
  BLK_DATA_FILTEROUT = 0x4,   // discard current data block since it is not qualified for filter
};

enum {
  MAIN_SCAN     = 0x0u,
  REVERSE_SCAN  = 0x1u,  // todo remove it
  REPEAT_SCAN   = 0x2u,  //repeat scan belongs to the master scan
  MERGE_STAGE   = 0x20u,
};

typedef struct SPoint1 {
  int64_t   key;
  union{double  val; char* ptr;};
} SPoint1;

struct SqlFunctionCtx;
struct SResultRowEntryInfo;

//for selectivity query, the corresponding tag value is assigned if the data is qualified
typedef struct SSubsidiaryResInfo {
  int16_t num;
  struct SqlFunctionCtx **pCtx;
} SSubsidiaryResInfo;

typedef struct SResultDataInfo {
  int16_t precision;
  int16_t scale;
  int16_t type;
  int16_t bytes;
  int32_t interBufSize;
} SResultDataInfo;

#define GET_RES_INFO(ctx)        ((ctx)->resultInfo)
#define GET_ROWCELL_INTERBUF(_c) ((void*) ((char*)(_c) + sizeof(SResultRowEntryInfo)))

typedef struct SInputColumnInfoData {
  int32_t           totalRows;      // total rows in current columnar data
  int32_t           startRowIndex;  // handle started row index
  int32_t           numOfRows;      // the number of rows needs to be handled
  int32_t           numOfInputCols; // PTS is not included
  bool              colDataAggIsSet;// if agg is set or not
  SColumnInfoData  *pPTS;           // primary timestamp column
  SColumnInfoData **pData;
  SColumnDataAgg  **pColumnDataAgg;
  uint64_t          uid;            // table uid, used to set the tag value when building the final query result for selectivity functions.
} SInputColumnInfoData;

// sql function runtime context
typedef struct SqlFunctionCtx {
  SInputColumnInfoData   input;
  SResultDataInfo        resDataInfo;
  uint32_t               order;  // data block scanner order: asc|desc
  uint8_t                scanFlag;  // record current running step, default: 0
  int16_t                functionId;    // function id
  char                  *pOutput;       // final result output buffer, point to sdata->data
  int32_t                numOfParams;
  SFunctParam           *param;         // input parameter, e.g., top(k, 20), the number of results for top query is kept in param
  int64_t               *ptsList;       // corresponding timestamp array list
  SColumnInfoData       *pTsOutput;     // corresponding output buffer for timestamp of each result, e.g., top/bottom*/
  int32_t                offset;
  SVariant               tag;
  struct  SResultRowEntryInfo *resultInfo;
  SSubsidiaryResInfo     subsidiaries;
  SPoint1                start;
  SPoint1                end;
  SFuncExecFuncs         fpSet;
  SScalarFuncExecFuncs   sfp;
  struct SExprInfo      *pExpr;
  struct SDiskbasedBuf  *pBuf;
  struct SSDataBlock    *pSrcBlock;
  int32_t                curBufPage;

  char                   udfName[TSDB_FUNC_NAME_LEN];
} SqlFunctionCtx;

enum {
  TEXPR_NODE_DUMMY     = 0x0,
  TEXPR_BINARYEXPR_NODE= 0x1,
  TEXPR_UNARYEXPR_NODE = 0x2,
  TEXPR_FUNCTION_NODE  = 0x3,
  TEXPR_COL_NODE       = 0x4,
  TEXPR_VALUE_NODE     = 0x8,
};

typedef struct tExprNode {
  int32_t nodeType;
  union {
    SSchema            *pSchema;// column node
    struct SVariant    *pVal;   // value node

    struct {// function node
      char              functionName[FUNCTIONS_NAME_MAX_LENGTH];  // todo refactor
      int32_t           functionId;
      int32_t           num;
      struct SFunctionNode    *pFunctNode;
    } _function;

    struct {
      struct SNode* pRootNode;
    } _optrRoot;
  };
} tExprNode;

void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *));

typedef struct SAggFunctionInfo {
  char      name[FUNCTIONS_NAME_MAX_LENGTH];
  int8_t    type;         // Scalar function or aggregation function
  uint32_t  functionId;   // Function Id
  int8_t    sFunctionId;  // Transfer function for super table query
  uint16_t  status;

  bool (*init)(SqlFunctionCtx *pCtx, struct SResultRowEntryInfo* pResultCellInfo);  // setup the execute environment
  void (*addInput)(SqlFunctionCtx *pCtx);

  // finalizer must be called after all exec has been executed to generated final result.
  void (*finalize)(SqlFunctionCtx *pCtx);
  void (*combine)(SqlFunctionCtx *pCtx);

  int32_t (*dataReqFunc)(SqlFunctionCtx *pCtx, STimeWindow* w, int32_t colId);
} SAggFunctionInfo;

struct SScalarParam {
  SColumnInfoData *columnData;
  SHashObj        *pHashFilter;
  void            *param;  // other parameter, such as meta handle from vnode, to extract table name/tag value
  int32_t          numOfRows;
};

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, SResultDataInfo* pInfo, int16_t extLength,
                          bool isSuperTable);

bool qIsValidUdf(SArray* pUdfInfo, const char* name, int32_t len, int32_t* functionId);

void resetResultRowEntryResult(SqlFunctionCtx* pCtx, int32_t num);
void cleanupResultRowEntry(struct SResultRowEntryInfo* pCell);
int32_t getNumOfResult(SqlFunctionCtx* pCtx, int32_t num, SSDataBlock* pResBlock);
bool isRowEntryCompleted(struct SResultRowEntryInfo* pEntry);
bool isRowEntryInitialized(struct SResultRowEntryInfo* pEntry);

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// fill api
struct SFillInfo;
struct SFillColInfo;

typedef struct SPoint {
  int64_t key;
  void *  val;
} SPoint;

//void taosFillSetStartInfo(struct SFillInfo* pFillInfo, int32_t numOfRows, TSKEY endKey);
//void taosResetFillInfo(struct SFillInfo* pFillInfo, TSKEY startTimestamp);
//void taosFillSetInputDataBlock(struct SFillInfo* pFillInfo, const struct SSDataBlock* pInput);
//struct SFillColInfo* createFillColInfo(SExprInfo* pExpr, int32_t numOfOutput, const SValueNode* val);
//bool taosFillHasMoreResults(struct SFillInfo* pFillInfo);
//
//struct SFillInfo* taosCreateFillInfo(int32_t order, TSKEY skey, int32_t numOfTags, int32_t capacity, int32_t numOfCols,
//                                     SInterval* pInterval, int32_t fillType,
//                                     struct SFillColInfo* pCol, const char* id);
//
//void* taosDestroyFillInfo(struct SFillInfo *pFillInfo);
//int64_t taosFillResultDataBlock(struct SFillInfo* pFillInfo, void** output, int32_t capacity);
//int64_t getFillInfoStart(struct SFillInfo *pFillInfo);

int32_t taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2, int32_t inputType);

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// udf api
struct SUdfInfo;

void qAddUdfInfo(uint64_t id, struct SUdfInfo* pUdfInfo);
void qRemoveUdfInfo(uint64_t id, struct SUdfInfo* pUdfInfo);

/**
 * create udfd proxy, called once in process that call doSetupUdf/callUdfxxx/doTeardownUdf
 * @return error code
 */
int32_t udfcOpen();

/**
 * destroy udfd proxy
 * @return error code
 */
int32_t udfcClose();

/**
 * start udfd that serves udf function invocation under dnode startDnodeId
 * @param startDnodeId
 * @return
 */
int32_t udfStartUdfd(int32_t startDnodeId);
/**
 * stop udfd
 * @return
 */
int32_t udfStopUdfd();
#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FUNCTION_H
