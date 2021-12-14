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

#ifndef _TD_PLANNER_H_
#define _TD_PLANNER_H_

#ifdef __cplusplus
extern "C" {
#endif

#define QUERY_TYPE_MERGE       1
#define QUERY_TYPE_PARTIAL     2
#define QUERY_TYPE_SCAN        3

enum OPERATOR_TYPE_E {
  OP_TableScan         = 1,
  OP_DataBlocksOptScan = 2,
  OP_TableSeqScan      = 3,
  OP_TagScan           = 4,
  OP_TableBlockInfoScan= 5,
  OP_Aggregate         = 6,
  OP_Project           = 7,
  OP_Groupby           = 8,
  OP_Limit             = 9,
  OP_SLimit            = 10,
  OP_TimeWindow        = 11,
  OP_SessionWindow     = 12,
  OP_StateWindow       = 22,
  OP_Fill              = 13,
  OP_MultiTableAggregate     = 14,
  OP_MultiTableTimeInterval  = 15,
//  OP_DummyInput        = 16,   //TODO remove it after fully refactor.
//  OP_MultiwayMergeSort = 17,   // multi-way data merge into one input stream.
//  OP_GlobalAggregate   = 18,   // global merge for the multi-way data sources.
  OP_Filter            = 19,
  OP_Distinct          = 20,
  OP_Join              = 21,
  OP_AllTimeWindow     = 23,
  OP_AllMultiTableTimeInterval = 24,
  OP_Order             = 25,
  OP_Exchange          = 26,
};

struct SEpSet;
struct SPhyNode;
struct SQueryStmtInfo;

typedef struct SSubplan {
  int32_t   type;               // QUERY_TYPE_MERGE|QUERY_TYPE_PARTIAL|QUERY_TYPE_SCAN
  SArray   *pDatasource;          // the datasource subplan,from which to fetch the result
  struct SPhyNode *pNode;  // physical plan of current subplan
} SSubplan;

typedef struct SQueryDag {
  SArray  **pSubplans;
} SQueryDag;

/**
 * Create the physical plan for the query, according to the AST.
 */
int32_t qCreateQueryDag(const struct SQueryStmtInfo* pQueryInfo, struct SEpSet* pQnode, struct SQueryDag** pDag);

int32_t qExplainQuery(const struct SQueryStmtInfo* pQueryInfo, struct SEpSet* pQnode, char** str);

/**
 * Convert to subplan to string for the scheduler to send to the executor
 */
int32_t qSubPlanToString(struct SSubplan *pPhyNode, char** str);

/**
 * Destroy the physical plan.
 * @param pQueryPhyNode
 * @return
 */
void* qDestroyQueryDag(struct SQueryDag* pDag);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_H_*/