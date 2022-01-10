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

#include "query.h"
#include "tmsg.h"
#include "tarray.h"
#include "trpc.h"

#define QUERY_TYPE_MERGE       1
#define QUERY_TYPE_PARTIAL     2
#define QUERY_TYPE_SCAN        3
#define QUERY_TYPE_MODIFY      4

enum OPERATOR_TYPE_E {
  OP_Unknown,
#define INCLUDE_AS_ENUM
#include "plannerOp.h"
#undef INCLUDE_AS_ENUM
  OP_TotalNum
};

enum DATASINK_TYPE_E {
  DSINK_Unknown,
  DSINK_Dispatch,
  DSINK_Insert,
  DSINK_TotalNum
};

struct SEpSet;
struct SQueryStmtInfo;

typedef SSchema SSlotSchema;

typedef struct SDataBlockSchema {
  SSlotSchema *pSchema;
  int32_t      numOfCols;    // number of columns
  int32_t      resultRowSize;
  int16_t      precision;
} SDataBlockSchema;

typedef struct SQueryNodeBasicInfo {
  int32_t     type;          // operator type
  const char *name;          // operator name
} SQueryNodeBasicInfo;

typedef struct SDataSink {
  SQueryNodeBasicInfo info;
  SDataBlockSchema schema;
} SDataSink;

typedef struct SDataDispatcher {
  SDataSink sink;
} SDataDispatcher;

typedef struct SDataInserter {
  SDataSink sink;
  int32_t   numOfTables;
  uint32_t  size;
  char     *pData;
} SDataInserter;

typedef struct SPhyNode {
  SQueryNodeBasicInfo info;
  SArray             *pTargets;      // target list to be computed or scanned at this node
  SArray             *pConditions;   // implicitly-ANDed qual conditions
  SDataBlockSchema    targetSchema;
  // children plan to generated result for current node to process
  // in case of join, multiple plan nodes exist.
  SArray             *pChildren;
  struct SPhyNode    *pParent;
} SPhyNode;

typedef struct SScanPhyNode {
  SPhyNode    node;
  uint64_t    uid;  // unique id of the table
  int8_t      tableType;
} SScanPhyNode;

typedef SScanPhyNode SSystemTableScanPhyNode;
typedef SScanPhyNode STagScanPhyNode;

typedef struct STableScanPhyNode {
  SScanPhyNode scan;
  uint8_t      scanFlag;         // denotes reversed scan of data or not
  STimeWindow  window;
  SArray      *pTagsConditions; // implicitly-ANDed tag qual conditions
} STableScanPhyNode;

typedef STableScanPhyNode STableSeqScanPhyNode;

typedef struct SProjectPhyNode {
  SPhyNode node;
} SProjectPhyNode;

typedef struct SExchangePhyNode {
  SPhyNode    node;
  uint64_t    srcTemplateId; // template id of datasource suplans
  SArray     *pSrcEndPoints;  // SEpAddrMsg, scheduler fill by calling qSetSuplanExecutionNode
} SExchangePhyNode;

typedef struct SSubplanId {
  uint64_t queryId;
  uint64_t templateId;
  uint64_t subplanId;
} SSubplanId;

typedef struct SSubplan {
  SSubplanId id;           // unique id of the subplan
  int32_t    type;         // QUERY_TYPE_MERGE|QUERY_TYPE_PARTIAL|QUERY_TYPE_SCAN|QUERY_TYPE_MODIFY
  int32_t    msgType;      // message type for subplan, used to denote the send message type to vnode.
  int32_t    level;        // the execution level of current subplan, starting from 0 in a top-down manner.
  SQueryNodeAddr     execNode;    // for the scan/modify subplan, the optional execution node
  SArray    *pChildren;    // the datasource subplan,from which to fetch the result
  SArray    *pParents;     // the data destination subplan, get data from current subplan
  SPhyNode  *pNode;        // physical plan of current subplan
  SDataSink *pDataSink;    // data of the subplan flow into the datasink
} SSubplan;

typedef struct SQueryDag {
  uint64_t queryId;
  int32_t  numOfSubplans;
  SArray  *pSubplans; // SArray*<SArray*<SSubplan*>>. The execution level of subplan, starting from 0.
} SQueryDag;

struct SQueryNode;

 /**
  * Create the physical plan for the query, according to the AST.
  * @param pQueryInfo
  * @param pDag
  * @param requestId
  * @return
  */
int32_t qCreateQueryDag(const struct SQueryNode* pQueryInfo, struct SQueryDag** pDag, SSchema** pSchema, uint32_t* numOfResCols, uint64_t requestId);

// Set datasource of this subplan, multiple calls may be made to a subplan.
// @subplan subplan to be schedule
// @templateId templateId of a group of datasource subplans of this @subplan
// @ep one execution location of this group of datasource subplans 
void qSetSubplanExecutionNode(SSubplan* subplan, uint64_t templateId, SQueryNodeAddr* ep);

int32_t qExplainQuery(const struct SQueryNode* pQueryInfo, struct SEpSet* pQnode, char** str);

/**
 * Convert to subplan to string for the scheduler to send to the executor
 */
int32_t qSubPlanToString(const SSubplan* subplan, char** str, int32_t* len);

int32_t qStringToSubplan(const char* str, SSubplan** subplan);

void qDestroySubplan(SSubplan* pSubplan);

/**
 * Destroy the physical plan.
 * @param pQueryPhyNode
 * @return
 */
void qDestroyQueryDag(SQueryDag* pDag);

char* qDagToString(const SQueryDag* pDag);
SQueryDag* qStringToDag(const char* pStr);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANNER_H_*/
