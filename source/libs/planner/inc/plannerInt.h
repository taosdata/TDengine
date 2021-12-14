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

#ifndef _TD_PLANNER_INT_H_
#define _TD_PLANNER_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "common.h"
#include "tarray.h"
#include "planner.h"
#include "taosmsg.h"

typedef struct SQueryNodeBasicInfo {
  int32_t   type;          // operator type
  char     *name;          // operator name
} SQueryNodeBasicInfo;

typedef struct SQueryDistPlanNodeInfo {
  bool      stableQuery;   // super table query or not
  int32_t   phase;         // merge|partial
  int32_t   type;          // operator type
  char     *name;          // operator name
  SEpSet   *sourceEp;      // data source epset
} SQueryDistPlanNodeInfo;

typedef struct SQueryTableInfo {
  char       *tableName;
  uint64_t    uid;
  STimeWindow window;
} SQueryTableInfo;

typedef struct SQueryPlanNode {
  SQueryNodeBasicInfo info;
  SSchema            *pSchema;      // the schema of the input SSDatablock
  int32_t             numOfCols;    // number of input columns
  SArray             *pExpr;        // the query functions or sql aggregations
  int32_t             numOfExpr;  // number of result columns, which is also the number of pExprs
  void               *pExtInfo;     // additional information
  // previous operator to generated result for current node to process
  // in case of join, multiple prev nodes exist.
  SArray             *pPrevNodes;   // upstream nodes
  struct SQueryPlanNode  *nextNode;
} SQueryPlanNode;

typedef struct SDataBlockSchema {
  int32_t             index;
  SSchema            *pSchema;      // the schema of the SSDatablock
  int32_t             numOfCols;    // number of columns
} SDataBlockSchema;

typedef struct SQueryPhyPlanNode {
  SQueryNodeBasicInfo info;
  SArray             *pTarget;      // target list to be computed at this node
  SArray             *qual;         // implicitly-ANDed qual conditions
  SDataBlockSchema    targetSchema;
  // children plan to generated result for current node to process
  // in case of join, multiple plan nodes exist.
  SArray             *pChildren;
} SQueryPhyPlanNode;

typedef struct SQueryScanPhyNode {
  SQueryPhyPlanNode node;
  uint64_t          uid;
} SQueryScanPhyNode;

typedef struct SQueryProjectPhyNode {
  SQueryPhyPlanNode node;
} SQueryProjectPhyNode;

typedef struct SQueryAggPhyNode {
  SQueryPhyPlanNode node;
  SArray           *pGroup;
  // SInterval
} SQueryAggPhyNode;

typedef struct SQueryProfileSummary {
  int64_t startTs;      // Object created and added into the message queue
  int64_t endTs;        // the timestamp when the task is completed
  int64_t cputime;      // total cpu cost, not execute elapsed time

  int64_t loadRemoteDataDuration;       // remote io time
  int64_t loadNativeDataDuration;       // native disk io time

  uint64_t loadNativeData; // blocks + SMA + header files
  uint64_t loadRemoteData; // remote data acquired by exchange operator.

  uint64_t waitDuration; // the time to waiting to be scheduled in queue does matter, so we need to record it
  int64_t  addQTs;       // the time to be added into the message queue, used to calculate the waiting duration in queue.

  uint64_t totalRows;
  uint64_t loadRows;
  uint32_t totalBlocks;
  uint32_t loadBlocks;
  uint32_t loadBlockAgg;
  uint32_t skipBlocks;
  uint64_t resultSize;   // generated result size in Kb.
} SQueryProfileSummary;

typedef struct SQueryTask {
  uint64_t            queryId; // query id
  uint64_t            taskId;  // task id
  SQueryPhyPlanNode *pNode;   // operator tree
  uint64_t            status;  // task status
  SQueryProfileSummary summary; // task execution summary
  void               *pOutputHandle; // result buffer handle, to temporarily keep the output result for next stage
} SQueryTask;

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_INT_H_*/