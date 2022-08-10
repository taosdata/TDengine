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
typedef int32_t (*FExecCombine)(struct SqlFunctionCtx *pDestCtx, struct SqlFunctionCtx *pSourceCtx);

typedef struct SScalarFuncExecFuncs {
  FExecGetEnv getEnv;
  FScalarExecProcess process;
} SScalarFuncExecFuncs;

typedef struct SFuncExecFuncs {
  FExecGetEnv getEnv;
  FExecInit init;
  FExecProcess process;
  FExecFinalize finalize;
  FExecCombine combine;
} SFuncExecFuncs;

typedef struct SFileBlockInfo {
  int32_t numBlocksOfStep;
} SFileBlockInfo;

#define MAX_INTERVAL_TIME_WINDOW  1000000  // maximum allowed time windows in final results

#define TOP_BOTTOM_QUERY_LIMIT    100
#define FUNCTIONS_NAME_MAX_LENGTH 16

typedef struct SResultRowEntryInfo {
  bool     initialized:1;     // output buffer has been initialized
  bool     complete:1;        // query has completed
  uint8_t  isNullRes:6;       // the result is null
  uint16_t numOfRes;          // num of output result in current buffer. NOT NULL RESULT
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
  SColumnInfoData       *pTsOutput;     // corresponding output buffer for timestamp of each result, e.g., top/bottom*/
  int32_t                offset;
  struct  SResultRowEntryInfo *resultInfo;
  SSubsidiaryResInfo     subsidiaries;
  SPoint1                start;
  SPoint1                end;
  SFuncExecFuncs         fpSet;
  SScalarFuncExecFuncs   sfp;
  struct SExprInfo      *pExpr;
  struct SDiskbasedBuf  *pBuf;
  struct SSDataBlock    *pSrcBlock;
  struct SSDataBlock    *pDstBlock; // used by indifinite rows function to set selectivity
  int32_t                curBufPage;
  bool                   increase;

  char                   udfName[TSDB_FUNC_NAME_LEN];
} SqlFunctionCtx;

enum {
  TEXPR_BINARYEXPR_NODE= 0x1,
  TEXPR_UNARYEXPR_NODE = 0x2,
};

typedef struct tExprNode {
  int32_t nodeType;
  union {
    struct {// function node
      char     functionName[FUNCTIONS_NAME_MAX_LENGTH];  // todo refactor
      int32_t  functionId;
      int32_t  num;
      struct SFunctionNode  *pFunctNode;
    } _function;

    struct {
      struct SNode* pRootNode;
    } _optrRoot;
  };
} tExprNode;

void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *));

struct SScalarParam {
  bool             colAlloced;
  SColumnInfoData *columnData;
  SHashObj        *pHashFilter;
  int32_t          hashValueType;
  void            *param;  // other parameter, such as meta handle from vnode, to extract table name/tag value
  int32_t          numOfRows;
};

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, SResultDataInfo* pInfo, int16_t extLength,
                          bool isSuperTable);

void resetResultRowEntryResult(SqlFunctionCtx* pCtx, int32_t num);
void cleanupResultRowEntry(struct SResultRowEntryInfo* pCell);
int32_t getNumOfResult(SqlFunctionCtx* pCtx, int32_t num, SSDataBlock* pResBlock);
bool isRowEntryCompleted(struct SResultRowEntryInfo* pEntry);
bool isRowEntryInitialized(struct SResultRowEntryInfo* pEntry);

typedef struct SPoint {
  int64_t key;
  void *  val;
} SPoint;

int32_t taosGetLinearInterpolationVal(SPoint* point, int32_t outputType, SPoint* point1, SPoint* point2, int32_t inputType);

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// udf api
struct SUdfInfo;

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
