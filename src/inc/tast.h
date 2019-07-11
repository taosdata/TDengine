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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsql.h"

struct tSQLBinaryExpr;
struct SSchema;
struct tSkipList;
struct tSkipListNode;

enum {
  TSQL_NODE_EXPR = 0x1,
  TSQL_NODE_COL = 0x2,
  TSQL_NODE_VALUE = 0x4,
};

typedef struct tSQLSyntaxNode {
  uint8_t nodeType;
  int16_t colId;  // for schema, the id of column
  union {
    struct tSQLBinaryExpr *pExpr;
    struct SSchema *       pSchema;
    tVariant *             pVal;
  };
} tSQLSyntaxNode;

typedef struct tSQLBinaryExpr {
  uint8_t nSQLBinaryOptr;
  uint8_t filterOnPrimaryKey;  // 0: do not contain primary filter, 1: contain
  // primary key

  tSQLSyntaxNode *pLeft;
  tSQLSyntaxNode *pRight;
} tSQLBinaryExpr;

#define TAST_NODE_TYPE_INDEX_ENTRY 0
#define TAST_NODE_TYPE_METER_PTR 1

typedef struct tQueryResultset {
  void ** pRes;
  int64_t num;
  int32_t nodeType;
} tQueryResultset;

typedef struct tQueryInfo {
  int32_t         offset;   // offset value in tags
  int32_t         colIdx;   // index of column in schema
  struct SSchema *pSchema;  // schema of tags
  tVariant        q;        // queries cond
  uint8_t         optr;
  __compar_fn_t   comparator;
} tQueryInfo;

void tSQLBinaryExprFromString(tSQLBinaryExpr **pExpr, struct SSchema *pSchema, int32_t numOfCols, char *src,
                              int32_t len);

void tSQLBinaryExprToString(tSQLBinaryExpr *pExpr, char *dst, int32_t *len);

void tSQLBinaryExprDestroy(tSQLBinaryExpr **pExprs);

void tSQLBinaryExprTraverse(tSQLBinaryExpr *pExprs, struct tSkipList *pSkipList, struct SSchema *pSchema,
                            int32_t numOfCols, bool (*fp)(struct tSkipListNode *, void *), tQueryResultset *result);

void tSQLBinaryExprCalcTraverse(tSQLBinaryExpr *pExprs, int32_t numOfRows, char *pOutput, void *param, int32_t order,
                                char *(*cb)(void *, char *, int32_t));

void tSQLBinaryExprTrv(tSQLBinaryExpr *pExprs, int32_t *val, int16_t *ids);

bool tSQLElemFilterCallback(struct tSkipListNode *pNode, void *param);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TAST_H
