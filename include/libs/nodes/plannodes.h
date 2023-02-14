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

#define SLOT_NAME_LEN TSDB_TABLE_NAME_LEN + TSDB_COL_NAME_LEN

typedef enum EDataOrderLevel {
  DATA_ORDER_LEVEL_NONE = 1,
  DATA_ORDER_LEVEL_IN_BLOCK,
  DATA_ORDER_LEVEL_IN_GROUP,
  DATA_ORDER_LEVEL_GLOBAL
} EDataOrderLevel;

typedef enum EGroupAction {
  GROUP_ACTION_NONE = 1,
  GROUP_ACTION_SET,
  GROUP_ACTION_KEEP,
  GROUP_ACTION_CLEAR
} EGroupAction;

typedef struct SLogicNode {
  ENodeType          type;
  SNodeList*         pTargets;  // SColumnNode
  SNode*             pConditions;
  SNodeList*         pChildren;
  struct SLogicNode* pParent;
  int32_t            optimizedFlag;
  uint8_t            precision;
  SNode*             pLimit;
  SNode*             pSlimit;
  EDataOrderLevel    requireDataOrder;  // requirements for input data
  EDataOrderLevel    resultDataOrder;   // properties of the output data
  EGroupAction       groupAction;
} SLogicNode;

typedef enum EScanType {
  SCAN_TYPE_TAG = 1,
  SCAN_TYPE_TABLE,
  SCAN_TYPE_SYSTEM_TABLE,
  SCAN_TYPE_STREAM,
  SCAN_TYPE_TABLE_MERGE,
  SCAN_TYPE_BLOCK_INFO,
  SCAN_TYPE_LAST_ROW,
  SCAN_TYPE_TABLE_COUNT
} EScanType;

typedef struct SScanLogicNode {
  SLogicNode    node;
  SNodeList*    pScanCols;
  SNodeList*    pScanPseudoCols;
  int8_t        tableType;
  uint64_t      tableId;
  uint64_t      stableId;
  SVgroupsInfo* pVgroupList;
  EScanType     scanType;
  uint8_t       scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow   scanRange;
  SName         tableName;
  bool          showRewrite;
  double        ratio;
  SNodeList*    pDynamicScanFuncs;
  int32_t       dataRequired;
  int64_t       interval;
  int64_t       offset;
  int64_t       sliding;
  int8_t        intervalUnit;
  int8_t        slidingUnit;
  SNode*        pTagCond;
  SNode*        pTagIndexCond;
  int8_t        triggerType;
  int64_t       watermark;
  int64_t       deleteMark;
  int8_t        igExpired;
  int8_t        igCheckUpdate;
  SArray*       pSmaIndexes;
  SNodeList*    pGroupTags;
  bool          groupSort;
  SNodeList*    pTags;      // for create stream
  SNode*        pSubtable;  // for create stream
  int8_t        cacheLastMode;
  bool          hasNormalCols;  // neither tag column nor primary key tag column
  bool          sortPrimaryKey;
  bool          igLastNull;
} SScanLogicNode;

typedef struct SJoinLogicNode {
  SLogicNode node;
  EJoinType  joinType;
  SNode*     pMergeCondition;
  SNode*     pOnConditions;
  bool       isSingleTableJoin;
  EOrder     inputTsOrder;
} SJoinLogicNode;

typedef struct SAggLogicNode {
  SLogicNode node;
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
  bool       hasLastRow;
  bool       hasLast;
  bool       hasTimeLineFunc;
  bool       onlyHasKeepOrderFunc;
  bool       hasGroupKeyOptimized;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
  char       stmtName[TSDB_TABLE_NAME_LEN];
  bool       ignoreGroupId;
} SProjectLogicNode;

typedef struct SIndefRowsFuncLogicNode {
  SLogicNode node;
  SNodeList* pFuncs;
  bool       isTailFunc;
  bool       isUniqueFunc;
  bool       isTimeLineFunc;
} SIndefRowsFuncLogicNode;

typedef struct SInterpFuncLogicNode {
  SLogicNode  node;
  SNodeList*  pFuncs;
  STimeWindow timeRange;
  int64_t     interval;
  EFillMode   fillMode;
  SNode*      pFillValues;  // SNodeListNode
  SNode*      pTimeSeries;  // SColumnNode
} SInterpFuncLogicNode;

typedef enum EModifyTableType { MODIFY_TABLE_TYPE_INSERT = 1, MODIFY_TABLE_TYPE_DELETE } EModifyTableType;

typedef struct SVnodeModifyLogicNode {
  SLogicNode       node;
  EModifyTableType modifyType;
  int32_t          msgType;
  SArray*          pDataBlocks;
  SVgDataBlocks*   pVgDataBlocks;
  SNode*           pAffectedRows;  // SColumnNode
  SNode*           pStartTs;       // SColumnNode
  SNode*           pEndTs;         // SColumnNode
  uint64_t         tableId;
  uint64_t         stableId;
  int8_t           tableType;  // table type
  char             tableName[TSDB_TABLE_NAME_LEN];
  char             tsColName[TSDB_COL_NAME_LEN];
  STimeWindow      deleteTimeRange;
  SVgroupsInfo*    pVgroupList;
  SNodeList*       pInsertCols;
} SVnodeModifyLogicNode;

typedef struct SExchangeLogicNode {
  SLogicNode node;
  int32_t    srcStartGroupId;
  int32_t    srcEndGroupId;
  bool       seqRecvData;
} SExchangeLogicNode;

typedef struct SMergeLogicNode {
  SLogicNode node;
  SNodeList* pMergeKeys;
  SNodeList* pInputs;
  int32_t    numOfChannels;
  int32_t    srcGroupId;
  bool       groupSort;
} SMergeLogicNode;

typedef enum EWindowType {
  WINDOW_TYPE_INTERVAL = 1,
  WINDOW_TYPE_SESSION,
  WINDOW_TYPE_STATE,
  WINDOW_TYPE_EVENT
} EWindowType;

typedef enum EWindowAlgorithm {
  INTERVAL_ALGO_HASH = 1,
  INTERVAL_ALGO_MERGE,
  INTERVAL_ALGO_STREAM_FINAL,
  INTERVAL_ALGO_STREAM_SEMI,
  INTERVAL_ALGO_STREAM_SINGLE,
  SESSION_ALGO_STREAM_SEMI,
  SESSION_ALGO_STREAM_FINAL,
  SESSION_ALGO_STREAM_SINGLE,
  SESSION_ALGO_MERGE,
} EWindowAlgorithm;

typedef struct SWindowLogicNode {
  SLogicNode       node;
  EWindowType      winType;
  SNodeList*       pFuncs;
  int64_t          interval;
  int64_t          offset;
  int64_t          sliding;
  int8_t           intervalUnit;
  int8_t           slidingUnit;
  int64_t          sessionGap;
  SNode*           pTspk;
  SNode*           pTsEnd;
  SNode*           pStateExpr;
  SNode*           pStartCond;
  SNode*           pEndCond;
  int8_t           triggerType;
  int64_t          watermark;
  int64_t          deleteMark;
  int8_t           igExpired;
  int8_t           igCheckUpdate;
  EWindowAlgorithm windowAlgo;
  EOrder           inputTsOrder;
  EOrder           outputTsOrder;
} SWindowLogicNode;

typedef struct SFillLogicNode {
  SLogicNode  node;
  EFillMode   mode;
  SNodeList*  pFillExprs;
  SNodeList*  pNotFillExprs;
  SNode*      pWStartTs;
  SNode*      pValues;  // SNodeListNode
  STimeWindow timeRange;
  EOrder      inputTsOrder;
} SFillLogicNode;

typedef struct SSortLogicNode {
  SLogicNode node;
  SNodeList* pSortKeys;
  bool       groupSort;
} SSortLogicNode;

typedef struct SPartitionLogicNode {
  SLogicNode node;
  SNodeList* pPartitionKeys;
  SNodeList* pTags;
  SNode*     pSubtable;
} SPartitionLogicNode;

typedef enum ESubplanType {
  SUBPLAN_TYPE_MERGE = 1,
  SUBPLAN_TYPE_PARTIAL,
  SUBPLAN_TYPE_SCAN,
  SUBPLAN_TYPE_MODIFY,
  SUBPLAN_TYPE_COMPUTE
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
  int32_t       numOfComputeNodes;
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
  char      name[SLOT_NAME_LEN];
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
  SNode*              pLimit;
  SNode*              pSlimit;
} SPhysiNode;

typedef struct SScanPhysiNode {
  SPhysiNode node;
  SNodeList* pScanCols;
  SNodeList* pScanPseudoCols;
  uint64_t   uid;  // unique id of the table
  uint64_t   suid;
  int8_t     tableType;
  SName      tableName;
} SScanPhysiNode;

typedef SScanPhysiNode STagScanPhysiNode;
typedef SScanPhysiNode SBlockDistScanPhysiNode;

typedef struct SLastRowScanPhysiNode {
  SScanPhysiNode scan;
  SNodeList*     pGroupTags;
  bool           groupSort;
  bool           ignoreNull;
} SLastRowScanPhysiNode;

typedef SLastRowScanPhysiNode STableCountScanPhysiNode;

typedef struct SSystemTableScanPhysiNode {
  SScanPhysiNode scan;
  SEpSet         mgmtEpSet;
  bool           showRewrite;
  int32_t        accountId;
  bool           sysInfo;
} SSystemTableScanPhysiNode;

typedef struct STableScanPhysiNode {
  SScanPhysiNode scan;
  uint8_t        scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow    scanRange;
  double         ratio;
  int32_t        dataRequired;
  SNodeList*     pDynamicScanFuncs;
  SNodeList*     pGroupTags;
  bool           groupSort;
  SNodeList*     pTags;
  SNode*         pSubtable;
  int64_t        interval;
  int64_t        offset;
  int64_t        sliding;
  int8_t         intervalUnit;
  int8_t         slidingUnit;
  int8_t         triggerType;
  int64_t        watermark;
  int8_t         igExpired;
  bool           assignBlockUid;
  int8_t         igCheckUpdate;
} STableScanPhysiNode;

typedef STableScanPhysiNode STableSeqScanPhysiNode;
typedef STableScanPhysiNode STableMergeScanPhysiNode;
typedef STableScanPhysiNode SStreamScanPhysiNode;

typedef struct SProjectPhysiNode {
  SPhysiNode node;
  SNodeList* pProjections;
  bool       mergeDataBlock;
  bool       ignoreGroupId;
} SProjectPhysiNode;

typedef struct SIndefRowsFuncPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;
  SNodeList* pFuncs;
} SIndefRowsFuncPhysiNode;

typedef struct SInterpFuncPhysiNode {
  SPhysiNode  node;
  SNodeList*  pExprs;
  SNodeList*  pFuncs;
  STimeWindow timeRange;
  int64_t     interval;
  int8_t      intervalUnit;
  EFillMode   fillMode;
  SNode*      pFillValues;  // SNodeListNode
  SNode*      pTimeSeries;  // SColumnNode
} SInterpFuncPhysiNode;

typedef struct SSortMergeJoinPhysiNode {
  SPhysiNode node;
  EJoinType  joinType;
  SNode*     pMergeCondition;
  SNode*     pOnConditions;
  SNodeList* pTargets;
  EOrder     inputTsOrder;
} SSortMergeJoinPhysiNode;

typedef struct SAggPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of group_by_clause and parameter expression of aggregate function
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
  bool       mergeDataBlock;
  bool       groupKeyOptimized;
} SAggPhysiNode;

typedef struct SDownstreamSourceNode {
  ENodeType      type;
  SQueryNodeAddr addr;
  uint64_t       taskId;
  uint64_t       schedId;
  int32_t        execId;
  int32_t        fetchMsgType;
  bool           localExec;
} SDownstreamSourceNode;

typedef struct SExchangePhysiNode {
  SPhysiNode node;
  // for set operators, there will be multiple execution groups under one exchange, and the ids of these execution
  // groups are consecutive
  int32_t    srcStartGroupId;
  int32_t    srcEndGroupId;
  bool       singleChannel;
  SNodeList* pSrcEndPoints;  // element is SDownstreamSource, scheduler fill by calling qSetSuplanExecutionNode
  bool       seqRecvData;
} SExchangePhysiNode;

typedef struct SMergePhysiNode {
  SPhysiNode node;
  SNodeList* pMergeKeys;
  SNodeList* pTargets;
  int32_t    numOfChannels;
  int32_t    srcGroupId;
  bool       groupSort;
} SMergePhysiNode;

typedef struct SWinodwPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of parameter expression of function
  SNodeList* pFuncs;
  SNode*     pTspk;   // timestamp primary key
  SNode*     pTsEnd;  // window end timestamp
  int8_t     triggerType;
  int64_t    watermark;
  int64_t    deleteMark;
  int8_t     igExpired;
  EOrder     inputTsOrder;
  EOrder     outputTsOrder;
  bool       mergeDataBlock;
} SWinodwPhysiNode;

typedef struct SIntervalPhysiNode {
  SWinodwPhysiNode window;
  int64_t          interval;
  int64_t          offset;
  int64_t          sliding;
  int8_t           intervalUnit;
  int8_t           slidingUnit;
} SIntervalPhysiNode;

typedef SIntervalPhysiNode SMergeIntervalPhysiNode;
typedef SIntervalPhysiNode SMergeAlignedIntervalPhysiNode;
typedef SIntervalPhysiNode SStreamIntervalPhysiNode;
typedef SIntervalPhysiNode SStreamFinalIntervalPhysiNode;
typedef SIntervalPhysiNode SStreamSemiIntervalPhysiNode;

typedef struct SFillPhysiNode {
  SPhysiNode  node;
  EFillMode   mode;
  SNodeList*  pFillExprs;
  SNodeList*  pNotFillExprs;
  SNode*      pWStartTs;  // SColumnNode
  SNode*      pValues;    // SNodeListNode
  STimeWindow timeRange;
  EOrder      inputTsOrder;
} SFillPhysiNode;

typedef SFillPhysiNode SStreamFillPhysiNode;

typedef struct SMultiTableIntervalPhysiNode {
  SIntervalPhysiNode interval;
  SNodeList*         pPartitionKeys;
} SMultiTableIntervalPhysiNode;

typedef struct SSessionWinodwPhysiNode {
  SWinodwPhysiNode window;
  int64_t          gap;
} SSessionWinodwPhysiNode;

typedef SSessionWinodwPhysiNode SStreamSessionWinodwPhysiNode;
typedef SSessionWinodwPhysiNode SStreamSemiSessionWinodwPhysiNode;
typedef SSessionWinodwPhysiNode SStreamFinalSessionWinodwPhysiNode;

typedef struct SStateWinodwPhysiNode {
  SWinodwPhysiNode window;
  SNode*           pStateKey;
} SStateWinodwPhysiNode;

typedef SStateWinodwPhysiNode SStreamStateWinodwPhysiNode;

typedef struct SEventWinodwPhysiNode {
  SWinodwPhysiNode window;
  SNode*           pStartCond;
  SNode*           pEndCond;
} SEventWinodwPhysiNode;

typedef SEventWinodwPhysiNode SStreamEventWinodwPhysiNode;

typedef struct SSortPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;     // these are expression list of order_by_clause and parameter expression of aggregate function
  SNodeList* pSortKeys;  // element is SOrderByExprNode, and SOrderByExprNode::pExpr is SColumnNode
  SNodeList* pTargets;
} SSortPhysiNode;

typedef SSortPhysiNode SGroupSortPhysiNode;

typedef struct SPartitionPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of partition_by_clause
  SNodeList* pPartitionKeys;
  SNodeList* pTargets;
} SPartitionPhysiNode;

typedef struct SStreamPartitionPhysiNode {
  SPartitionPhysiNode part;
  SNodeList*          pTags;
  SNode*              pSubtable;
} SStreamPartitionPhysiNode;

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
  void*         pData;
} SDataInserterNode;

typedef struct SQueryInserterNode {
  SDataSinkNode sink;
  SNodeList*    pCols;
  uint64_t      tableId;
  uint64_t      stableId;
  int8_t        tableType;  // table type
  char          tableName[TSDB_TABLE_NAME_LEN];
  int32_t       vgId;
  SEpSet        epSet;
  bool          explain;
} SQueryInserterNode;

typedef struct SDataDeleterNode {
  SDataSinkNode sink;
  uint64_t      tableId;
  int8_t        tableType;  // table type
  char          tableFName[TSDB_TABLE_NAME_LEN];
  char          tsColName[TSDB_COL_NAME_LEN];
  STimeWindow   deleteTimeRange;
  SNode*        pAffectedRows;
  SNode*        pStartTs;
  SNode*        pEndTs;
} SDataDeleterNode;

typedef struct SSubplan {
  ENodeType      type;
  SSubplanId     id;  // unique id of the subplan
  ESubplanType   subplanType;
  int32_t        msgType;  // message type for subplan, used to denote the send message type to vnode.
  int32_t        level;    // the execution level of current subplan, starting from 0 in a top-down manner.
  char           dbFName[TSDB_DB_FNAME_LEN];
  char           user[TSDB_USER_LEN];
  SQueryNodeAddr execNode;      // for the scan/modify subplan, the optional execution node
  SQueryNodeStat execNodeStat;  // only for scan subplan
  SNodeList*     pChildren;     // the datasource subplan,from which to fetch the result
  SNodeList*     pParents;      // the data destination subplan, get data from current subplan
  SPhysiNode*    pNode;         // physical plan of current subplan
  SDataSinkNode* pDataSink;     // data of the subplan flow into the datasink
  SNode*         pTagCond;
  SNode*         pTagIndexCond;
  bool           showRewrite;
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
} SQueryPlan;

const char* dataOrderStr(EDataOrderLevel order);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
