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

#ifndef TDENGINE_QPLAN_H
#define TDENGINE_QPLAN_H

#include "qExecutor.h"

struct SQueryInfo;

typedef struct SQueryNodeBasicInfo {
  int32_t type;
  char   *name;
} SQueryNodeBasicInfo;

typedef struct SQueryTableInfo {
  char     *tableName;
  STableId  id;
} SQueryTableInfo;

typedef struct SQueryNode {
  SQueryNodeBasicInfo info;
  SQueryTableInfo    tableInfo;
  SSchema           *pSchema;      // the schema of the input SSDatablock
  int32_t            numOfCols;    // number of input columns
  SExprInfo         *pExpr;        // the query functions or sql aggregations
  int32_t            numOfOutput;  // number of result columns, which is also the number of pExprs

  void              *pExtInfo;     // additional information
  // previous operator to generated result for current node to process
  // in case of join, multiple prev nodes exist.
  SArray            *pPrevNodes;// upstream nodes
  struct SQueryNode *nextNode;
} SQueryNode;

SQueryNode* qCreateQueryPlan(struct SQueryInfo* pQueryInfo);
void* qDestroyQueryPlan(SQueryNode* pQueryNode);

char* queryPlanToString(SQueryNode* pQueryNode);

SArray* createTableScanPlan(SQueryAttr* pQueryAttr);
SArray* createExecOperatorPlan(SQueryAttr* pQueryAttr);
SArray* createGlobalMergePlan(SQueryAttr* pQueryAttr);

#endif  // TDENGINE_QPLAN_H
