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

#ifndef _TD_PLANN_NODES_H_
#define _TD_PLANN_NODES_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "querynodes.h"
#include "query.h"
#include "tname.h"

typedef struct SLogicNode {
  ENodeType type;
  SNodeList* pTargets; // SColumnNode
  SNode* pConditions;
  SNodeList* pChildren;
  struct SLogicNode* pParent;
} SLogicNode;

typedef enum EScanType {
  SCAN_TYPE_TAG,
  SCAN_TYPE_TABLE,
  SCAN_TYPE_SYSTEM_TABLE,
  SCAN_TYPE_STREAM
} EScanType;

typedef struct SScanLogicNode {
  SLogicNode node;
  SNodeList* pScanCols;
  struct STableMeta* pMeta;
  SVgroupsInfo* pVgroupList;
  EScanType scanType;
  uint8_t scanFlag;         // denotes reversed scan of data or not
  STimeWindow scanRange;
  SName tableName;
} SScanLogicNode;

typedef struct SJoinLogicNode {
  SLogicNode node;
  EJoinType joinType;
  SNode* pOnConditions;
} SJoinLogicNode;

typedef struct SAggLogicNode {
  SLogicNode node;
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
} SProjectLogicNode;

typedef struct SVnodeModifLogicNode {
  SLogicNode node;
  int32_t msgType;
  SArray* pDataBlocks;
  SVgDataBlocks* pVgDataBlocks;
} SVnodeModifLogicNode;

typedef struct SExchangeLogicNode {
  SLogicNode node;
  int32_t srcGroupId;
} SExchangeLogicNode;

typedef enum EWindowType {
  WINDOW_TYPE_INTERVAL = 1,
  WINDOW_TYPE_SESSION,
  WINDOW_TYPE_STATE
} EWindowType;

typedef struct SWindowLogicNode {
  SLogicNode node;
  EWindowType winType;
  SNodeList* pFuncs;
  int64_t interval;
  int64_t offset;
  int64_t sliding;
  int8_t  intervalUnit;
  int8_t  slidingUnit;
  SFillNode* pFill;
} SWindowLogicNode;

typedef enum ESubplanType {
  SUBPLAN_TYPE_MERGE = 1,
  SUBPLAN_TYPE_PARTIAL,
  SUBPLAN_TYPE_SCAN,
  SUBPLAN_TYPE_MODIFY
} ESubplanType;

typedef struct SSubplanId {
  uint64_t queryId;
  int32_t groupId;
  int32_t subplanId;
} SSubplanId;

typedef struct SSubLogicPlan {
  ENodeType type;
  SSubplanId id;
  SNodeList* pChildren;
  SNodeList* pParents;
  SLogicNode* pNode;
  ESubplanType subplanType;
  SVgroupsInfo* pVgroupList;
  int32_t level;
  int32_t splitFlag;
} SSubLogicPlan;

typedef struct SQueryLogicPlan {
  ENodeType type;
  int32_t totalLevel;
  SNodeList* pTopSubplans;
} SQueryLogicPlan;

typedef struct SSlotDescNode {
  ENodeType type;
  int16_t slotId;
  SDataType dataType;
  bool reserve;
  bool output;
  bool tag;
} SSlotDescNode;

typedef struct SDataBlockDescNode {
  ENodeType type;
  int16_t dataBlockId;
  SNodeList* pSlots;
  int32_t resultRowSize;
  int16_t precision;
} SDataBlockDescNode;

typedef struct SPhysiNode {
  ENodeType type;
  SDataBlockDescNode* pOutputDataBlockDesc;
  SNode* pConditions;
  SNodeList* pChildren;
  struct SPhysiNode* pParent;
} SPhysiNode;

typedef struct SScanPhysiNode {
  SPhysiNode node;
  SNodeList* pScanCols;
  uint64_t uid;           // unique id of the table
  int8_t tableType;
  int32_t order;         // scan order: TSDB_ORDER_ASC|TSDB_ORDER_DESC
  int32_t count;         // repeat count
  int32_t reverse;       // reverse scan count
  SName tableName;
} SScanPhysiNode;

typedef SScanPhysiNode STagScanPhysiNode;
typedef SScanPhysiNode SStreamScanPhysiNode;

typedef struct SSystemTableScanPhysiNode {
  SScanPhysiNode scan;
  SEpSet mgmtEpSet;
} SSystemTableScanPhysiNode;

typedef struct STableScanPhysiNode {
  SScanPhysiNode scan;
  uint8_t scanFlag;         // denotes reversed scan of data or not
  STimeWindow scanRange;
  SNode* pScanConditions;
} STableScanPhysiNode;

typedef STableScanPhysiNode STableSeqScanPhysiNode;

typedef struct SProjectPhysiNode {
  SPhysiNode node;
  SNodeList* pProjections;
} SProjectPhysiNode;

typedef struct SJoinPhysiNode {
  SPhysiNode node;
  EJoinType joinType;
  SNode* pOnConditions; // in or out tuple ?
  SNodeList* pTargets;
} SJoinPhysiNode;

typedef struct SAggPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;   // these are expression list of group_by_clause and parameter expression of aggregate function
  SNodeList* pGroupKeys; // SColumnRefNode list
  SNodeList* pAggFuncs;
} SAggPhysiNode;

typedef struct SDownstreamSourceNode {
  ENodeType type;
  SQueryNodeAddr addr;
  uint64_t taskId;
  uint64_t schedId;
} SDownstreamSourceNode;

typedef struct SExchangePhysiNode {
  SPhysiNode node;
  int32_t    srcGroupId;  // group id of datasource suplans
  SNodeList* pSrcEndPoints;  // element is SDownstreamSource, scheduler fill by calling qSetSuplanExecutionNode
} SExchangePhysiNode;

typedef struct SIntervalPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;   // these are expression list of parameter expression of function
  SNodeList* pFuncs;
  int64_t    interval;
  int64_t    offset;
  int64_t    sliding;
  int8_t     intervalUnit;
  int8_t     slidingUnit;
  SFillNode* pFill;
} SIntervalPhysiNode;

typedef struct SDataSinkNode {
  ENodeType type;
  SDataBlockDescNode* pInputDataBlockDesc;
} SDataSinkNode;

typedef struct SDataDispatcherNode {
  SDataSinkNode sink;
} SDataDispatcherNode;

typedef struct SDataInserterNode {
  SDataSinkNode sink;
  int32_t   numOfTables;
  uint32_t  size;
  char     *pData;
} SDataInserterNode;

typedef struct SSubplan {
  ENodeType type;
  SSubplanId id;           // unique id of the subplan
  ESubplanType subplanType;
  int32_t msgType;      // message type for subplan, used to denote the send message type to vnode.
  int32_t level;        // the execution level of current subplan, starting from 0 in a top-down manner.
  char dbFName[TSDB_DB_FNAME_LEN];
  SQueryNodeAddr execNode;    // for the scan/modify subplan, the optional execution node
  SQueryNodeStat execNodeStat; // only for scan subplan
  SNodeList* pChildren;    // the datasource subplan,from which to fetch the result
  SNodeList* pParents;     // the data destination subplan, get data from current subplan
  SPhysiNode* pNode;        // physical plan of current subplan
  SDataSinkNode* pDataSink;    // data of the subplan flow into the datasink
} SSubplan;

typedef struct SQueryPlan {
  ENodeType type;
  uint64_t queryId;
  int32_t numOfSubplans;
  SNodeList* pSubplans; // Element is SNodeListNode. The execution level of subplan, starting from 0.
} SQueryPlan;

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
