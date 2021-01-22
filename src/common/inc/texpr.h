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

#ifndef TDENGINE_TAST_H
#define TDENGINE_TAST_H

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

#define QUERY_COND_REL_PREFIX_IN_LEN   3
#define QUERY_COND_REL_PREFIX_LIKE_LEN 5

typedef bool (*__result_filter_fn_t)(const void *, void *);
typedef void (*__do_filter_suppl_fn_t)(void *, void *);

enum {
  TSQL_NODE_DUMMY = 0x0,
  TSQL_NODE_EXPR  = 0x1,
  TSQL_NODE_COL   = 0x2,
  TSQL_NODE_VALUE = 0x4,
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
      uint8_t optr;             // filter operator
      uint8_t hasPK;            // 0: do not contain primary filter, 1: contain
      void *  info;             // support filter operation on this expression only available for leaf node

      struct tExprNode *pLeft;  // left child pointer
      struct tExprNode *pRight; // right child pointer
    } _node;
    struct SSchema *pSchema;
    tVariant *      pVal;
  };
} tExprNode;

typedef struct SExprTraverseSupp {
  __result_filter_fn_t   nodeFilterFn;
  __do_filter_suppl_fn_t setupInfoFn;
  void *                 pExtInfo;
} SExprTraverseSupp;

void tExprTreeDestroy(tExprNode *pNode, void (*fp)(void *));

tExprNode* exprTreeFromBinary(const void* data, size_t size);
tExprNode* exprTreeFromTableName(const char* tbnameCond);

void exprTreeToBinary(SBufferWriter* bw, tExprNode* pExprTree);

bool exprTreeApplayFilter(tExprNode *pExpr, const void *pItem, SExprTraverseSupp *param);

typedef void (*_arithmetic_operator_fn_t)(void *left, int32_t numLeft, int32_t leftType, void *right, int32_t numRight,
                                          int32_t rightType, void *output, int32_t order);

void arithmeticTreeTraverse(tExprNode *pExprs, int32_t numOfRows, char *pOutput, void *param, int32_t order,
                            char *(*cb)(void *, const char*, int32_t));

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TAST_H
