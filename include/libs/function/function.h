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

#include "common.h"
#include "tvariant.h"
#include "tbuffer.h"

#define MAX_INTERVAL_TIME_WINDOW 1000000  // maximum allowed time windows in final results

#define FUNCTION_SCALAR       1
#define FUNCTION_AGG          2

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

#define FUNCTION_HISTOGRAM    34
#define FUNCTION_HLL          35
#define FUNCTION_MODE         36
#define FUNCTION_SAMPLE       37

typedef struct SPoint1 {
  int64_t   key;
  union{double  val; char* ptr;};
} SPoint1;

struct SQLFunctionCtx;
struct SResultRowCellInfo;

//for selectivity query, the corresponding tag value is assigned if the data is qualified
typedef struct SExtTagsInfo {
  int16_t                 tagsLen;      // keep the tags data for top/bottom query result
  int16_t                 numOfTagCols;
  struct SQLFunctionCtx **pTagCtxList;
} SExtTagsInfo;

// sql function runtime context
typedef struct SQLFunctionCtx {
  int32_t      size;      // number of rows
  void *       pInput;    // input data buffer
  uint32_t     order;     // asc|desc
  int16_t      inputType;
  int16_t      inputBytes;

  int16_t      outputType;
  int16_t      outputBytes;   // size of results, determined by function and input column data type
  int32_t      interBufBytes; // internal buffer size
  bool         hasNull;       // null value exist in current block
  bool         requireNull;   // require null in some function
  bool         stableQuery;
  int16_t      functionId;    // function id
  char *       pOutput;       // final result output buffer, point to sdata->data
  uint8_t      currentStage;  // record current running step, default: 0
  int64_t      startTs;       // timestamp range of current query when function is executed on a specific data block
  int32_t      numOfParams;
  SVariant     param[4];      // input parameter, e.g., top(k, 20), the number of results for top query is kept in param
  int64_t     *ptsList;       // corresponding timestamp array list
  void        *ptsOutputBuf;  // corresponding output buffer for timestamp of each result, e.g., top/bottom*/
  SVariant     tag;

  bool         isSmaSet;
  SColumnDataAgg sma;
  struct  SResultRowCellInfo *resultInfo;
  SExtTagsInfo tagInfo;
  SPoint1      start;
  SPoint1      end;
} SQLFunctionCtx;

enum {
  TEXPR_NODE_DUMMY     = 0x0,
  TEXPR_BINARYEXPR_NODE= 0x1,
  TEXPR_UNARYEXPR_NODE = 0x2,
  TEXPR_COL_NODE       = 0x4,
  TEXPR_VALUE_NODE     = 0x8,
};

typedef struct tExprNode {
  uint8_t nodeType;
  union {
    struct {
      union {
        int32_t         optr;   // binary operator
        int32_t         functionId;// unary operator
      };
      void             *info;   // support filter operation on this expression only available for leaf node
      struct tExprNode *pLeft;  // left child pointer
      struct tExprNode *pRight; // right child pointer
    } _node;

    SSchema            *pSchema;// column node
    struct SVariant    *pVal;   // value node
  };
} tExprNode;

void exprTreeToBinary(SBufferWriter* bw, tExprNode* pExprTree);
void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *));

typedef struct SAggFunctionInfo {
  char     name[FUNCTIONS_NAME_MAX_LENGTH];
  int8_t   type;         // Scalar function or aggregation function
  uint8_t  functionId;   // Function Id
  int8_t   sFunctionId;  // Transfer function for super table query
  uint16_t status;

  bool (*init)(SQLFunctionCtx *pCtx, struct SResultRowCellInfo* pResultCellInfo);  // setup the execute environment
  void (*exec)(SQLFunctionCtx *pCtx);

  // finalizer must be called after all exec has been executed to generated final result.
  void (*xFinalize)(SQLFunctionCtx *pCtx);
  void (*mergeFunc)(SQLFunctionCtx *pCtx);

  int32_t (*dataReqFunc)(SQLFunctionCtx *pCtx, STimeWindow* w, int32_t colId);
} SAggFunctionInfo;

typedef struct SScalarFunctionInfo {
  char     name[FUNCTIONS_NAME_MAX_LENGTH];
  int8_t   type;              // scalar function or aggregation function
  uint8_t  functionId;        // index of scalar function

  bool    (*init)(SQLFunctionCtx *pCtx, struct SResultRowCellInfo* pResultCellInfo);  // setup the execute environment
  void    (*exec)(SQLFunctionCtx *pCtx);
} SScalarFunctionInfo;

typedef struct SResultDataInfo {
  int16_t type;
  int16_t bytes;
  int32_t intermediateBytes;
} SResultDataInfo;


typedef struct SMultiFunctionsDesc {
  bool stableQuery;
  bool groupbyColumn;
  bool simpleAgg;
  bool arithmeticOnAgg;
  bool projectionQuery;
  bool hasFilter;
  bool onlyTagQuery;
  bool orderProjectQuery;
  bool stateWindow;
  bool globalMerge;
  bool multigroupResult;
  bool blockDistribution;
  bool timewindow;
  bool topbotQuery;
  bool interpQuery;
  bool distinct;
  bool join;
} SMultiFunctionsDesc;

int32_t getResultDataInfo(int32_t dataType, int32_t dataBytes, int32_t functionId, int32_t param, SResultDataInfo* pInfo, int16_t extLength,
                          bool isSuperTable);

/**
 * If the given name is a valid built-in sql function, the value of true will be returned.
 * @param name
 * @param len
 * @return
 */
int32_t qIsBuiltinFunction(const char* name, int32_t len);

bool qIsValidUdf(SArray* pUdfInfo, const char* name, int32_t len, int32_t* functionId);

const char* qGetFunctionName(int32_t functionId);

void extractFunctionDesc(SArray* pFunctionIdList, SMultiFunctionsDesc* pDesc);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_FUNCTION_H
