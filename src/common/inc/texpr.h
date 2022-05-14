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

#ifndef TDENGINE_TEXPR_H
#define TDENGINE_TEXPR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"

#include "taosmsg.h"
#include "taosdef.h"
#include "tskiplist.h"
#include "tbuffer.h"
#include "tvariant.h"

struct tExprNode;
struct SSchema;

#define QUERY_COND_REL_PREFIX_IN "IN|"
#define QUERY_COND_REL_PREFIX_LIKE "LIKE|"
#define QUERY_COND_REL_PREFIX_MATCH "MATCH|"
#define QUERY_COND_REL_PREFIX_NMATCH "NMATCH|"

#define QUERY_COND_REL_PREFIX_IN_LEN   3
#define QUERY_COND_REL_PREFIX_LIKE_LEN 5
#define QUERY_COND_REL_PREFIX_MATCH_LEN 6
#define QUERY_COND_REL_PREFIX_NMATCH_LEN 7

#define TSDB_FUNC_FLAG_SCALAR       0x4000
#define TSDB_FUNC_IS_SCALAR(id)     ((((id) > 0)) && (((id) & TSDB_FUNC_FLAG_SCALAR) != 0))
#define TSDB_FUNC_SCALAR_INDEX(id)  ((id) & ~TSDB_FUNC_FLAG_SCALAR)

///////////////////////////////////////////
// SCALAR FUNCTIONS
#define TSDB_FUNC_SCALAR_POW              (TSDB_FUNC_FLAG_SCALAR | 0x0000)
#define TSDB_FUNC_SCALAR_LOG              (TSDB_FUNC_FLAG_SCALAR | 0x0001)
#define TSDB_FUNC_SCALAR_ABS              (TSDB_FUNC_FLAG_SCALAR | 0x0002)
#define TSDB_FUNC_SCALAR_ACOS             (TSDB_FUNC_FLAG_SCALAR | 0x0003)
#define TSDB_FUNC_SCALAR_ASIN             (TSDB_FUNC_FLAG_SCALAR | 0x0004)
#define TSDB_FUNC_SCALAR_ATAN             (TSDB_FUNC_FLAG_SCALAR | 0x0005)
#define TSDB_FUNC_SCALAR_COS              (TSDB_FUNC_FLAG_SCALAR | 0x0006)
#define TSDB_FUNC_SCALAR_SIN              (TSDB_FUNC_FLAG_SCALAR | 0x0007)
#define TSDB_FUNC_SCALAR_TAN              (TSDB_FUNC_FLAG_SCALAR | 0x0008)
#define TSDB_FUNC_SCALAR_SQRT             (TSDB_FUNC_FLAG_SCALAR | 0x0009)
#define TSDB_FUNC_SCALAR_CEIL             (TSDB_FUNC_FLAG_SCALAR | 0x000A)
#define TSDB_FUNC_SCALAR_FLOOR            (TSDB_FUNC_FLAG_SCALAR | 0x000B)
#define TSDB_FUNC_SCALAR_ROUND            (TSDB_FUNC_FLAG_SCALAR | 0x000C)
#define TSDB_FUNC_SCALAR_CONCAT           (TSDB_FUNC_FLAG_SCALAR | 0x000D)
#define TSDB_FUNC_SCALAR_LENGTH           (TSDB_FUNC_FLAG_SCALAR | 0x000E)
#define TSDB_FUNC_SCALAR_CONCAT_WS        (TSDB_FUNC_FLAG_SCALAR | 0x000F)
#define TSDB_FUNC_SCALAR_CHAR_LENGTH      (TSDB_FUNC_FLAG_SCALAR | 0x0010)
#define TSDB_FUNC_SCALAR_CAST             (TSDB_FUNC_FLAG_SCALAR | 0x0011)
#define TSDB_FUNC_SCALAR_LOWER            (TSDB_FUNC_FLAG_SCALAR | 0x0012)
#define TSDB_FUNC_SCALAR_UPPER            (TSDB_FUNC_FLAG_SCALAR | 0x0013)
#define TSDB_FUNC_SCALAR_LTRIM            (TSDB_FUNC_FLAG_SCALAR | 0x0014)
#define TSDB_FUNC_SCALAR_RTRIM            (TSDB_FUNC_FLAG_SCALAR | 0x0015)
#define TSDB_FUNC_SCALAR_SUBSTR           (TSDB_FUNC_FLAG_SCALAR | 0x0016)
#define TSDB_FUNC_SCALAR_NOW              (TSDB_FUNC_FLAG_SCALAR | 0x0017)
#define TSDB_FUNC_SCALAR_TODAY            (TSDB_FUNC_FLAG_SCALAR | 0x0018)
#define TSDB_FUNC_SCALAR_TIMEZONE         (TSDB_FUNC_FLAG_SCALAR | 0x0019)
#define TSDB_FUNC_SCALAR_TO_ISO8601       (TSDB_FUNC_FLAG_SCALAR | 0x001A)
#define TSDB_FUNC_SCALAR_TO_UNIXTIMESTAMP (TSDB_FUNC_FLAG_SCALAR | 0x001B)
#define TSDB_FUNC_SCALAR_TIMETRUNCATE     (TSDB_FUNC_FLAG_SCALAR | 0x001C)
#define TSDB_FUNC_SCALAR_TIMEDIFF         (TSDB_FUNC_FLAG_SCALAR | 0x001D)

#define TSDB_FUNC_SCALAR_NUM_FUNCTIONS 30

#define TSDB_FUNC_SCALAR_NAME_MAX_LEN 16

typedef struct {
  int16_t type;
  int16_t bytes;
  int16_t numOfRows;
  char* data;
} tExprOperandInfo;

typedef void (*_expr_scalar_function_t)(int16_t functionId, tExprOperandInfo* pInputs, int32_t numInputs, tExprOperandInfo* pOutput, int32_t order);

_expr_scalar_function_t getExprScalarFunction(uint16_t scalar);

typedef struct tScalarFunctionInfo{
  int16_t functionId; // scalar function id & ~TSDB_FUNC_FLAG_SCALAR == index
  char    name[TSDB_FUNC_SCALAR_NAME_MAX_LEN];
  _expr_scalar_function_t scalarFunc;
} tScalarFunctionInfo;

/* global scalar sql functions array */
extern struct tScalarFunctionInfo aScalarFunctions[TSDB_FUNC_SCALAR_NUM_FUNCTIONS];


typedef bool (*__result_filter_fn_t)(const void *, void *);
typedef void (*__do_filter_suppl_fn_t)(void *, void *);

enum {
  TSQL_NODE_DUMMY = 0x0,
  TSQL_NODE_EXPR  = 0x1,
  TSQL_NODE_COL   = 0x2,
  TSQL_NODE_VALUE = 0x4,
  TSQL_NODE_FUNC  = 0x8,
  TSQL_NODE_TYPE  = 0x10
};

/**
 * this structure is used to filter data in tags, so the offset of filtered tag column in tagdata string is required
 */
typedef struct tQueryInfo {
  uint8_t       optr;     // expression operator
  SSchema       sch;      // schema of tags
  char*         q;
  __compar_fn_t compare;  // filter function
  bool          indexed;  // indexed columns
} tQueryInfo;

typedef struct tExprNode {
  uint8_t nodeType;
  union {
    struct {
      uint8_t           optr;   // filter operator
      uint8_t           hasPK;  // 0: do not contain primary filter, 1: contain
      void             *info;   // support filter operation on this expression only available for leaf node
      struct tExprNode *pLeft;  // left child pointer
      struct tExprNode *pRight; // right child pointer
    } _node;

    struct SSchema     *pSchema;

    tVariant           *pVal;

    struct {
      int16_t functionId;
      int32_t numChildren;
      struct tExprNode **pChildren;
    } _func;

    TAOS_FIELD        *pType;
  };
  int16_t resultType;
  int16_t resultBytes;
  int32_t precision;
} tExprNode;

typedef struct SExprTraverseSupp {
  __result_filter_fn_t   nodeFilterFn;
  __do_filter_suppl_fn_t setupInfoFn;
  void                  *pExtInfo;
} SExprTraverseSupp;

void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *));

int32_t exprTreeValidateTree(char* msgbuf, tExprNode *pExpr);

void exprTreeToBinary(SBufferWriter* bw, tExprNode* pExprTree);
tExprNode* exprTreeFromBinary(const void* data, size_t size);
tExprNode* exprdup(tExprNode* pTree);

void exprTreeToBinary(SBufferWriter* bw, tExprNode* pExprTree);

bool exprTreeApplyFilter(tExprNode *pExpr, const void *pItem, SExprTraverseSupp *param);

void exprTreeNodeTraverse(tExprNode *pExpr, int32_t numOfRows, tExprOperandInfo *output, void *param, int32_t order,
                                  char *(*getSourceDataBlock)(void *, const char*, int32_t));

void buildFilterSetFromBinary(void **q, const char *buf, int32_t len);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TEXPR_H
