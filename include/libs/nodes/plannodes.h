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

#include "query.h"
#include "querynodes.h"
#include "tname.h"

typedef struct SLogicNode {
  ENodeType          type;
  SNodeList*         pTargets;  // SColumnNode
  SNode*             pConditions;
  SNodeList*         pChildren;
  struct SLogicNode* pParent;
  int32_t            optimizedFlag;
  uint8_t            precision;
} SLogicNode;

typedef enum EScanType { SCAN_TYPE_TAG = 1, SCAN_TYPE_TABLE, SCAN_TYPE_SYSTEM_TABLE, SCAN_TYPE_STREAM } EScanType;

typedef struct SScanLogicNode {
  SLogicNode         node;
  SNodeList*         pScanCols;
  SNodeList*         pScanPseudoCols;
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  EScanType          scanType;
  uint8_t            scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow        scanRange;
  SName              tableName;
  bool               showRewrite;
  double             ratio;
  SNodeList*         pDynamicScanFuncs;
  int32_t            dataRequired;
  int64_t            interval;
  int64_t            offset;
  int64_t            sliding;
  int8_t             intervalUnit;
  int8_t             slidingUnit;
  SNode*             pTagCond;
  int8_t             triggerType;
  int64_t            watermark;
  int16_t            tsColId;
} SScanLogicNode;

typedef struct SJoinLogicNode {
  SLogicNode node;
  EJoinType  joinType;
  SNode*     pOnConditions;
  bool       isSingleTableJoin;
} SJoinLogicNode;

typedef struct SAggLogicNode {
  SLogicNode node;
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
  char       stmtName[TSDB_TABLE_NAME_LEN];
  int64_t    limit;
  int64_t    offset;
  int64_t    slimit;
  int64_t    soffset;
} SProjectLogicNode;

typedef struct SVnodeModifLogicNode {
  SLogicNode     node;
  int32_t        msgType;
  SArray*        pDataBlocks;
  SVgDataBlocks* pVgDataBlocks;
} SVnodeModifLogicNode;

typedef struct SExchangeLogicNode {
  SLogicNode node;
  int32_t    srcGroupId;
  uint8_t    precision;
} SExchangeLogicNode;

typedef enum EWindowType { WINDOW_TYPE_INTERVAL = 1, WINDOW_TYPE_SESSION, WINDOW_TYPE_STATE } EWindowType;

typedef struct SWindowLogicNode {
  SLogicNode  node;
  EWindowType winType;
  SNodeList*  pFuncs;
  int64_t     interval;
  int64_t     offset;
  int64_t     sliding;
  int8_t      intervalUnit;
  int8_t      slidingUnit;
  int64_t     sessionGap;
  SNode*      pTspk;
  SNode*      pStateExpr;
  int8_t      triggerType;
  int64_t     watermark;
} SWindowLogicNode;

typedef struct SFillLogicNode {
  SLogicNode  node;
  EFillMode   mode;
  SNode*      pWStartTs;
  SNode*      pValues;  // SNodeListNode
  STimeWindow timeRange;
} SFillLogicNode;

typedef struct SSortLogicNode {
  SLogicNode node;
  SNodeList* pSortKeys;
} SSortLogicNode;

typedef struct SPartitionLogicNode {
  SLogicNode node;
  SNodeList* pPartitionKeys;
} SPartitionLogicNode;

typedef enum ESubplanType {
  SUBPLAN_TYPE_MERGE = 1,
  SUBPLAN_TYPE_PARTIAL,
  SUBPLAN_TYPE_SCAN,
  SUBPLAN_TYPE_MODIFY
} ESubplanType;

typedef struct SSubplanId {
  uint64_t queryId;
  int32_t  groupId;
  int32_t  subplanId;
} SSubplanId;

typedef struct SLogicSubplan {
  ENodeType     type;
  SSubplanId    id;
  SNodeList*    pChildren;
  SNodeList*    pParents;
  SLogicNode*   pNode;
  ESubplanType  subplanType;
  SVgroupsInfo* pVgroupList;
  int32_t       level;
  int32_t       splitFlag;
} SLogicSubplan;

typedef struct SQueryLogicPlan {
  ENodeType  type;
  SNodeList* pTopSubplans;
} SQueryLogicPlan;

typedef struct SSlotDescNode {
  ENodeType type;
  int16_t   slotId;
  SDataType dataType;
  bool      reserve;
  bool      output;
  bool      tag;
} SSlotDescNode;

typedef struct SDataBlockDescNode {
  ENodeType  type;
  int16_t    dataBlockId;
  SNodeList* pSlots;
  int32_t    totalRowSize;
  int32_t    outputRowSize;
  uint8_t    precision;
} SDataBlockDescNode;

typedef struct SPhysiNode {
  ENodeType           type;
  SDataBlockDescNode* pOutputDataBlockDesc;
  SNode*              pConditions;
  SNodeList*          pChildren;
  struct SPhysiNode*  pParent;
} SPhysiNode;

typedef struct SScanPhysiNode {
  SPhysiNode node;
  SNodeList* pScanCols;
  SNodeList* pScanPseudoCols;
  uint64_t   uid;  // unique id of the table
  int8_t     tableType;
  SName      tableName;
} SScanPhysiNode;

typedef SScanPhysiNode STagScanPhysiNode;

typedef struct SSystemTableScanPhysiNode {
  SScanPhysiNode scan;
  SEpSet         mgmtEpSet;
  bool           showRewrite;
  int32_t        accountId;
} SSystemTableScanPhysiNode;

typedef struct STableScanPhysiNode {
  SScanPhysiNode scan;
  uint8_t        scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow    scanRange;
  double         ratio;
  int32_t        dataRequired;
  SNodeList*     pDynamicScanFuncs;
  int64_t        interval;
  int64_t        offset;
  int64_t        sliding;
  int8_t         intervalUnit;
  int8_t         slidingUnit;
  int8_t         triggerType;
  int64_t        watermark;
  int16_t        tsColId;
} STableScanPhysiNode;

typedef STableScanPhysiNode STableSeqScanPhysiNode;
typedef STableScanPhysiNode SStreamScanPhysiNode;

typedef struct SProjectPhysiNode {
  SPhysiNode node;
  SNodeList* pProjections;
  int64_t    limit;
  int64_t    offset;
  int64_t    slimit;
  int64_t    soffset;
} SProjectPhysiNode;

typedef struct SJoinPhysiNode {
  SPhysiNode node;
  EJoinType  joinType;
  SNode*     pOnConditions;
  SNodeList* pTargets;
} SJoinPhysiNode;

typedef SJoinPhysiNode SSortMergeJoinPhysiNode;

typedef struct SAggPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of group_by_clause and parameter expression of aggregate function
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
} SAggPhysiNode;

typedef struct SDownstreamSourceNode {
  ENodeType      type;
  SQueryNodeAddr addr;
  uint64_t       taskId;
  uint64_t       schedId;
} SDownstreamSourceNode;

typedef struct SExchangePhysiNode {
  SPhysiNode node;
  int32_t    srcGroupId;     // group id of datasource suplans
  SNodeList* pSrcEndPoints;  // element is SDownstreamSource, scheduler fill by calling qSetSuplanExecutionNode
} SExchangePhysiNode;

typedef struct SWinodwPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of parameter expression of function
  SNodeList* pFuncs;
  SNode*     pTspk;  // timestamp primary key
  int8_t     triggerType;
  int64_t    watermark;
} SWinodwPhysiNode;

typedef struct SIntervalPhysiNode {
  SWinodwPhysiNode window;
  int64_t          interval;
  int64_t          offset;
  int64_t          sliding;
  int8_t           intervalUnit;
  int8_t           slidingUnit;
} SIntervalPhysiNode;

typedef SIntervalPhysiNode SStreamIntervalPhysiNode;

typedef struct SFillPhysiNode {
  SPhysiNode  node;
  EFillMode   mode;
  SNode*      pWStartTs;  // SColumnNode
  SNode*      pValues;    // SNodeListNode
  SNodeList*  pTargets;
  STimeWindow timeRange;
} SFillPhysiNode;

typedef struct SMultiTableIntervalPhysiNode {
  SIntervalPhysiNode interval;
  SNodeList*         pPartitionKeys;
} SMultiTableIntervalPhysiNode;

typedef struct SSessionWinodwPhysiNode {
  SWinodwPhysiNode window;
  int64_t          gap;
} SSessionWinodwPhysiNode;

typedef SSessionWinodwPhysiNode SStreamSessionWinodwPhysiNode;

typedef struct SStateWinodwPhysiNode {
  SWinodwPhysiNode window;
  SNode*           pStateKey;
} SStateWinodwPhysiNode;

typedef struct SSortPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;     // these are expression list of order_by_clause and parameter expression of aggregate function
  SNodeList* pSortKeys;  // element is SOrderByExprNode, and SOrderByExprNode::pExpr is SColumnNode
  SNodeList* pTargets;
} SSortPhysiNode;

typedef struct SPartitionPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of partition_by_clause
  SNodeList* pPartitionKeys;
  SNodeList* pTargets;
} SPartitionPhysiNode;

typedef struct SDataSinkNode {
  ENodeType           type;
  SDataBlockDescNode* pInputDataBlockDesc;
} SDataSinkNode;

typedef struct SDataDispatcherNode {
  SDataSinkNode sink;
} SDataDispatcherNode;

typedef struct SDataInserterNode {
  SDataSinkNode sink;
  int32_t       numOfTables;
  uint32_t      size;
  char*         pData;
} SDataInserterNode;

typedef struct SSubplan {
  ENodeType      type;
  SSubplanId     id;  // unique id of the subplan
  ESubplanType   subplanType;
  int32_t        msgType;  // message type for subplan, used to denote the send message type to vnode.
  int32_t        level;    // the execution level of current subplan, starting from 0 in a top-down manner.
  char           dbFName[TSDB_DB_FNAME_LEN];
  SQueryNodeAddr execNode;      // for the scan/modify subplan, the optional execution node
  SQueryNodeStat execNodeStat;  // only for scan subplan
  SNodeList*     pChildren;     // the datasource subplan,from which to fetch the result
  SNodeList*     pParents;      // the data destination subplan, get data from current subplan
  SPhysiNode*    pNode;         // physical plan of current subplan
  SDataSinkNode* pDataSink;     // data of the subplan flow into the datasink
  SNode*         pTagCond;
} SSubplan;

typedef enum EExplainMode { EXPLAIN_MODE_DISABLE = 1, EXPLAIN_MODE_STATIC, EXPLAIN_MODE_ANALYZE } EExplainMode;

typedef struct SExplainInfo {
  EExplainMode mode;
  bool         verbose;
  double       ratio;
} SExplainInfo;

typedef struct SQueryPlan {
  ENodeType    type;
  uint64_t     queryId;
  int32_t      numOfSubplans;
  SNodeList*   pSubplans;  // Element is SNodeListNode. The execution level of subplan, starting from 0.
  SExplainInfo explainInfo;
  SArray*      pPlaceholderValues;
} SQueryPlan;

void nodesWalkPhysiPlan(SNode* pNode, FNodeWalker walker, void* pContext);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
