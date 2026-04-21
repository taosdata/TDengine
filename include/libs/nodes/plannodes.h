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

typedef enum EMergeType {
  MERGE_TYPE_SORT = 1,
  MERGE_TYPE_NON_SORT,
  MERGE_TYPE_COLUMNS,
  MERGE_TYPE_MAX_VALUE
} EMergeType;

typedef struct SLogicNode {
  ENodeType          type;
  bool               dynamicOp;
  bool               stmtRoot;
  SNodeList*         pTargets;  // SColumnNode
  SNode*             pConditions;
  SNodeList*         pChildren;
  struct SLogicNode* pParent;
  SNodeList*         pHint;
  int32_t            optimizedFlag;
  uint8_t            precision;
  SNode*             pLimit;
  SNode*             pSlimit;
  EDataOrderLevel    requireDataOrder;  // requirements for input data
  EDataOrderLevel    resultDataOrder;   // properties of the output data
  EGroupAction       groupAction;
  EOrder             inputTsOrder;
  EOrder             outputTsOrder;
  bool               forceCreateNonBlockingOptr;  // true if the operator can use non-blocking(pipeline) mode
  bool               splitDone;
} SLogicNode;

typedef enum EScanType {
  SCAN_TYPE_TAG = 1,
  SCAN_TYPE_TABLE,
  SCAN_TYPE_SYSTEM_TABLE,
  SCAN_TYPE_STREAM,
  SCAN_TYPE_TABLE_MERGE,
  SCAN_TYPE_BLOCK_INFO,
  SCAN_TYPE_LAST_ROW,
  SCAN_TYPE_TABLE_COUNT,
  SCAN_TYPE_EXTERNAL,  // federated query: external data source scan
} EScanType;

// ---- Federated query pushdown bit masks ----
// Used by Optimizer to mark what can be pushed to remote; Phase 1 = all 0 (no pushdown)
#define FQ_PUSHDOWN_FILTER     (1u << 0)
#define FQ_PUSHDOWN_PROJECTION (1u << 1)
#define FQ_PUSHDOWN_LIMIT      (1u << 2)
#define FQ_PUSHDOWN_AGG        (1u << 3)
#define FQ_PUSHDOWN_ORDER      (1u << 4)
#define FQ_PUSHDOWN_JOIN       (1u << 5)

typedef struct SScanLogicNode {
  SLogicNode         node;
  SNodeList*         pScanCols;
  SNodeList*         pScanPseudoCols;
  int8_t             tableType;
  uint64_t           tableId;
  uint64_t           stableId;
  SVgroupsInfo*      pVgroupList;
  EScanType          scanType;
  uint8_t            scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow        scanRange;
  STimeWindow*       pExtScanRange;
  SNode*             pTimeRange;  // for create stream
  SNode*             pExtTimeRange;  // for create stream
  SNode*             pPrimaryCond;   // for remote node, splited from filter conditions
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
  SNode*             pTagIndexCond;
  int8_t             triggerType;
  int64_t            watermark;
  int64_t            deleteMark;
  int8_t             igExpired;
  int8_t             igCheckUpdate;
  SArray*            pSmaIndexes;
  SArray*            pTsmas;
  SArray*            pTsmaTargetTbVgInfo;
  SArray*            pTsmaTargetTbInfo;
  SNodeList*         pGroupTags;
  bool               groupSort;
  int8_t             cacheLastMode;
  bool               hasNormalCols;  // neither tag column nor primary key tag column
  bool               sortPrimaryKey;
  bool               igLastNull;
  bool               groupOrderScan;
  bool               onlyMetaCtbIdx;    // for tag scan with no tbname
  bool               filesetDelimited;  // returned blocks delimited by fileset
  bool               isCountByTag;      // true if selectstmt hasCountFunc & part by tag/tbname
  SArray*            pFuncTypes;        // for last, last_row
  bool               paraTablesSort;    // for table merge scan
  bool               smallDataTsSort;   // disable row id sort for table merge scan
  bool               needSplit;
  bool               noPseudoRefAfterGrp;  // no pseudo columns referenced ater group/partition clause
  bool               virtualStableScan;
  bool               phTbnameScan;
  EStreamPlaceholder placeholderType;
  // --- external scan extension (valid only when scanType == SCAN_TYPE_EXTERNAL) ---
  uint32_t    fqPushdownFlags;                     // FQ_PUSHDOWN_* bitmask; Phase 1 = 0
  SNode*      pExtTableNode;  // cloned SExtTableNode carrying connection info for Planner → Physi transfer
  SNodeList*  pFqAggFuncs;    // Phase 2: pushdown-eligible aggregate function list
  SNodeList*  pFqGroupKeys;   // Phase 2: pushdown-eligible GROUP BY columns
  SNodeList*  pFqSortKeys;    // Phase 2: pushdown-eligible ORDER BY columns
  SNode*      pFqLimit;       // Phase 2: pushdown-eligible LIMIT
  SNodeList*  pFqJoinTables;  // Phase 2: pushdown-eligible JOIN tables
} SScanLogicNode;

typedef struct SJoinLogicNode {
  SLogicNode     node;
  EJoinType      joinType;
  EJoinSubType   subType;
  SNode*         pWindowOffset;
  SNode*         pJLimit;
  EJoinAlgorithm joinAlgo;
  SNode*         addPrimEqCond;
  SNode*         pPrimKeyEqCond;
  SNode*         pColEqCond;
  SNode*         pColOnCond;
  SNode*         pTagEqCond;
  SNode*         pTagOnCond;
  SNode*         pFullOnCond;  // except prim eq cond
  SNodeList*     pLeftEqNodes;
  SNodeList*     pRightEqNodes;
  bool           allEqTags;
  bool           isSingleTableJoin;
  bool           hasSubQuery;
  bool           isLowLevelJoin;
  bool           seqWinGroup;
  bool           grpJoin;
  bool           hashJoinHint;
  bool           batchScanHint;
  
  // FOR CONST JOIN
  bool           noPrimKeyEqCond;
  bool           leftConstPrimGot;
  bool           rightConstPrimGot;
  bool           leftNoOrderedSubQuery;
  bool           rightNoOrderedSubQuery;

  // FOR HASH JOIN
  int32_t     timeRangeTarget;  // table onCond filter
  STimeWindow timeRange;        // table onCond filter
  SNode*      pLeftOnCond;      // table onCond filter
  SNode*      pRightOnCond;     // table onCond filter
} SJoinLogicNode;

typedef struct SVirtualScanLogicNode {
  SLogicNode    node;
  bool          scanAllCols;
  SNodeList*    pScanCols;
  SNodeList*    pScanPseudoCols;
  int8_t        tableType;
  uint64_t      tableId;
  uint64_t      stableId;
  SVgroupsInfo* pVgroupList;
  EScanType     scanType;
  SName         tableName;
} SVirtualScanLogicNode;

typedef struct SAggLogicNode {
  SLogicNode node;
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
  bool       hasLastRow;
  bool       hasLast;
  bool       hasTimeLineFunc;
  bool       onlyHasKeepOrderFunc;
  bool       hasGroupKeyOptimized;
  bool       isGroupTb;
  bool       isPartTb;  // true if partition keys has tbname
  bool       hasGroup;
  SNodeList* pTsmaSubplans;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
  char       stmtName[TSDB_TABLE_NAME_LEN];
  bool       ignoreGroupId;
  bool       inputIgnoreGroup;
  bool       isSetOpProj;
} SProjectLogicNode;

typedef struct SIndefRowsFuncLogicNode {
  SLogicNode node;
  SNodeList* pFuncs;
  bool       isTailFunc;
  bool       isUniqueFunc;
  bool       isTimeLineFunc;
} SIndefRowsFuncLogicNode;

typedef struct SInterpFuncLogicNode {
  SLogicNode    node;
  SNodeList*    pFuncs;
  STimeWindow   timeRange;
  SNode*        pTimeRange; // STimeRangeNode for create stream
  int64_t       interval;
  int8_t        intervalUnit;
  int8_t        precision;
  EFillMode     fillMode;
  SNode*        pFillValues;  // SNodeListNode
  SNode*        pTimeSeries;  // SColumnNode
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  int64_t       surroundingTime;
} SInterpFuncLogicNode;

typedef struct SForecastFuncLogicNode {
  SLogicNode node;
  SNodeList* pFuncs;
} SForecastFuncLogicNode, SGenericAnalysisLogicNode;

typedef struct SGroupCacheLogicNode {
  SLogicNode node;
  bool       grpColsMayBeNull;
  bool       grpByUid;
  bool       globalGrp;
  bool       batchFetch;
  SNodeList* pGroupCols;
} SGroupCacheLogicNode;

typedef struct SDynQueryCtrlStbJoin {
  bool       batchFetch;
  SNodeList* pVgList;
  SNodeList* pUidList;
  bool       srcScan[2];
} SDynQueryCtrlStbJoin;

typedef struct SDynQueryCtrlVtbScan {
  bool          batchProcessChild;
  bool          hasPartition;
  bool          scanAllCols;
  bool          isSuperTable;
  bool          useTagScan;
  char          dbName[TSDB_DB_NAME_LEN];
  char          tbName[TSDB_TABLE_NAME_LEN];
  uint64_t      suid;
  uint64_t      uid;
  int32_t       rversion;
  SNodeList*    pOrgVgIds;
  SVgroupsInfo* pVgroupList;
  SNodeList*    pScanCols;
} SDynQueryCtrlVtbScan;


typedef struct SDynQueryCtrlVtbWindow {
  int32_t               wstartSlotId;
  int32_t               wendSlotId;
  int32_t               wdurationSlotId;
  bool                  isVstb;
  EStateWinExtendOption extendOption;
} SDynQueryCtrlVtbWindow;

typedef struct SDynQueryCtrlLogicNode {
  SLogicNode             node;
  EDynQueryType          qType;
  SDynQueryCtrlStbJoin   stbJoin;
  SDynQueryCtrlVtbScan   vtbScan;
  SDynQueryCtrlVtbWindow vtbWindow;
  bool                   dynTbname;
} SDynQueryCtrlLogicNode;

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
  int8_t           secureDelete;
} SVnodeModifyLogicNode;

typedef struct SExchangeLogicNode {
  SLogicNode node;
  int32_t    srcStartGroupId;
  int32_t    srcEndGroupId;
  bool       seqRecvData;
  bool       dynTbname;
} SExchangeLogicNode;

typedef struct SMergeLogicNode {
  SLogicNode node;
  SNodeList* pMergeKeys;
  SNodeList* pInputs;
  int32_t    numOfChannels;
  int32_t    numOfSubplans;
  int32_t    srcGroupId;
  int32_t    srcEndGroupId;
  bool       colsMerge;
  bool       needSort;
  bool       groupSort;
  bool       ignoreGroupId;
  bool       inputWithGroupId;
} SMergeLogicNode;

typedef enum EWindowAlgorithm {
  INTERVAL_ALGO_HASH = 1,
  INTERVAL_ALGO_MERGE,
  SESSION_ALGO_MERGE,
  EXTERNAL_ALGO_HASH,
  EXTERNAL_ALGO_MERGE,
} EWindowAlgorithm;

#define WINDOW_PART_HAS  0x01
#define WINDOW_PART_TB   0x02

typedef struct SWindowLogicNode {
  // for all window types
  SLogicNode            node;
  EWindowType           winType;
  SNodeList*            pFuncs;
  STimeWindow           timeRange;
  SNode*                pTimeRange;
  SNode*                pTspk;
  EWindowAlgorithm      windowAlgo;
  SNodeList*            pTsmaSubplans;
  // for interval window
  int64_t               interval;
  int64_t               offset;
  int64_t               sliding;
  int8_t                intervalUnit;
  int8_t                slidingUnit;
  // for session window
  int64_t               sessionGap;
  SNode*                pTsEnd;
  // for state window
  SNode*                pStateExpr;
  EStateWinExtendOption extendOption;
  // for event window
  SNode*                pStartCond;
  SNode*                pEndCond;
  // for event and state window
  int32_t               trueForType;
  int32_t               trueForCount;
  int64_t               trueForDuration;
  // for count window
  int64_t               windowCount;
  int64_t               windowSliding;
  SNodeList*            pColList;
  // for external window
  int8_t                indefRowsFunc; // for external window
  SNodeList*            pProjs;        // for external window
  bool                  isSingleTable; // for external window
  bool                  inputHasOrder; // for external window, whether input data is ordered
  bool                  extWinSplit;
  bool                  needGroupSort;
  bool                  calcWithPartition;
  int32_t               orgTableVgId;
  tb_uid_t              orgTableUid;

  // for external and interval window
  int8_t                partType;      // bit0 is for has partition, bit1 is for tb partition
  // for anomaly window
  SNodeList*            pAnomalyExpr;
  char                  anomalyOpt[TSDB_ANALYTIC_ALGO_OPTION_LEN];

  SNode*                pSubquery;
} SWindowLogicNode;

typedef struct SFillLogicNode {
  SLogicNode  node;
  EFillMode   mode;
  SNodeList*  pFillExprs;
  SNodeList*  pNotFillExprs;
  SNode*      pWStartTs;
  SNode*      pValues;  // SNodeListNode
  STimeWindow timeRange;
  SNode*      pTimeRange; // STimeRangeNode for create stream
  SNodeList*  pFillNullExprs;
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  SNode*      pSurroundingTime;
} SFillLogicNode;

typedef struct SSortLogicNode {
  SLogicNode node;
  SNodeList* pSortKeys;
  bool       groupSort;
  bool       skipPKSortOpt;
  bool       calcGroupId;
  bool       excludePkCol;  // exclude PK ts col when calc group id
} SSortLogicNode;

typedef struct SPartitionLogicNode {
  SLogicNode node;
  SNodeList* pPartitionKeys;
  SNodeList* pTags;
  SNode*     pSubtable;
  SNodeList* pAggFuncs;

  bool     needBlockOutputTsOrder;  // if true, partition output block will have ts order maintained
  int32_t  pkTsColId;
  uint64_t pkTsColTbId;
} SPartitionLogicNode;

typedef enum ESubplanType {
  SUBPLAN_TYPE_MERGE = 1,
  SUBPLAN_TYPE_PARTIAL,
  SUBPLAN_TYPE_SCAN,
  SUBPLAN_TYPE_MODIFY,
  SUBPLAN_TYPE_COMPUTE,
  SUBPLAN_TYPE_HSYSSCAN,   // high priority systable scan
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
  bool          processOneBlock;
  bool          dynTbname;
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
  int64_t    dataBlockId;
  SNodeList* pSlots;
  int32_t    totalRowSize;
  int32_t    outputRowSize;
  uint8_t    precision;
} SDataBlockDescNode;

typedef struct SPhysiNode {
  ENodeType           type;
  bool                dynamicOp;
  EOrder              inputTsOrder;
  EOrder              outputTsOrder;
  SDataBlockDescNode* pOutputDataBlockDesc;
  SNode*              pConditions;
  SNodeList*          pChildren;
  struct SPhysiNode*  pParent;
  SNode*              pLimit;
  SNode*              pSlimit;
  bool                forceCreateNonBlockingOptr;
} SPhysiNode;

typedef struct SScanPhysiNode {
  SPhysiNode node;
  SNodeList* pScanCols;
  SNodeList* pScanPseudoCols;
  uint64_t   uid;  // unique id of the table
  uint64_t   suid;
  int8_t     tableType;
  SName      tableName;
  bool       groupOrderScan;
  bool       virtualStableScan;
} SScanPhysiNode;

typedef struct STagScanPhysiNode {
  SScanPhysiNode scan;
  bool           onlyMetaCtbIdx;  // no tbname, tag index not used.
} STagScanPhysiNode;

typedef SScanPhysiNode SBlockDistScanPhysiNode;

typedef struct SVirtualScanPhysiNode {
  SScanPhysiNode scan;
  SNodeList*     pGroupTags;
  bool           groupSort;
  bool           scanAllCols;
  SNodeList*     pTargets;
  SNodeList*     pTags;
  SNode*         pSubtable;
  int8_t         igExpired;
  int8_t         igCheckUpdate;
}SVirtualScanPhysiNode;

typedef struct SLastRowScanPhysiNode {
  SScanPhysiNode scan;
  SNodeList*     pGroupTags;
  bool           groupSort;
  bool           ignoreNull;
  SNodeList*     pTargets;
  SArray*        pFuncTypes;
} SLastRowScanPhysiNode;

typedef SLastRowScanPhysiNode STableCountScanPhysiNode;

typedef struct SSystemTableScanPhysiNode {
  SScanPhysiNode scan;
  SEpSet         mgmtEpSet;
  int32_t        accountId;
  bool           showRewrite;
  bool           sysInfo;
  union {
    uint16_t privInfo;
    struct {
      uint16_t privLevel : 3;  // user privilege level
      uint16_t privInfoBasic : 1;
      uint16_t privInfoPrivileged : 1;
      uint16_t privInfoAudit : 1;
      uint16_t privInfoSec : 1;
      uint16_t privPerfBasic : 1;
      uint16_t privPerfPrivileged : 1;
      uint16_t reserved : 7;
    };
  };
} SSystemTableScanPhysiNode;

typedef struct STableScanPhysiNode {
  SScanPhysiNode scan;
  uint8_t        scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow    scanRange;
  STimeWindow*   pExtScanRange; 
  SNode*         pTimeRange;  // for create stream
  SNode*         pExtTimeRange;  // for create stream
  SNode*         pPrimaryCond;   // for remote node, splited from filter conditions
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
  bool           filesetDelimited;
  bool           needCountEmptyTable;
  bool           paraTablesSort;
  bool           smallDataTsSort;
} STableScanPhysiNode;

typedef STableScanPhysiNode STableSeqScanPhysiNode;
typedef STableScanPhysiNode STableMergeScanPhysiNode;
typedef STableScanPhysiNode SStreamScanPhysiNode;

// ---- Federated query: column type mapping entry ----
// Computed by Parser (extTypeNameToTDengineType()), written into physical plan,
// then passed to Connector for raw value → TDengine column binary conversion.
typedef struct SExtColTypeMapping {
  char      extTypeName[64];  // original external type name (e.g. "VARCHAR(255)", "INT4")
  SDataType tdType;           // mapped TDengine type: type, precision, scale, bytes
} SExtColTypeMapping;

// ---- Federated query: physical scan node ----
// Inherits SPhysiNode directly (NOT SScanPhysiNode): external scan has no uid/suid/tableType.
// All connection info is embedded here because Executor runs in taosd (server side) and
// cannot access Catalog (client-side libtaos). The physical plan is the only data channel
// from client to server.
//
// TWO USAGE MODES — determined by whether pRemotePlan is NULL:
//
// Mode 1 — Outer wrapper node (pRemotePlan != NULL):
//   Appears in the TDengine executor plan as the scan leaf.
//   pRemotePlan is a mini physi-plan sub-tree encoding the full SQL to push down:
//     [SProjectPhysiNode]? → [SSortPhysiNode]? → SFederatedScanPhysiNode(Mode 2 leaf)
//   nodesRemotePlanToSQL() walks pRemotePlan to generate the external SQL string.
//   pExtTable and pScanCols are NOT used for SQL generation in this mode.
//   Connection fields (srcHost/srcPort/…) provide the data source endpoint.
//
// Mode 2 — Inner leaf node (pRemotePlan == NULL):
//   Appears only INSIDE a pRemotePlan sub-tree.  Never directly in the executor plan.
//   pExtTable + pScanCols  → FROM clause and SELECT column list.
//   node.pConditions       → WHERE clause (simple push-downable predicates).
//   node.pLimit            → LIMIT / OFFSET clause.
//   Connection fields are NOT used (the outer Mode 1 node holds them).
typedef struct SFederatedScanPhysiNode {
  SPhysiNode  node;              // standard physi node header (pConditions, pLimit, pOutputDataBlockDesc, etc.)
  SNode*      pExtTable;         // SExtTableNode* — external table AST node  [used in Mode 2]
  SNodeList*  pScanCols;         // scan column list                           [used in Mode 2]
  SNode*      pRemotePlan;       // mini physi-plan sub-tree for SQL gen       [non-NULL = Mode 1]
  uint32_t    pushdownFlags;     // FQ_PUSHDOWN_* combination
  // --- connection info (copied from SExtTableNode by Planner) ---
  int8_t      sourceType;                      // EExtSourceType
  char        srcHost[TSDB_EXT_SOURCE_HOST_LEN];
  int32_t     srcPort;
  char        srcUser[TSDB_EXT_SOURCE_USER_LEN];
  char        srcPassword[TSDB_EXT_SOURCE_PASSWORD_LEN];  // shown as ****** in EXPLAIN
  char        srcDatabase[TSDB_EXT_SOURCE_DATABASE_LEN];
  char        srcSchema[TSDB_EXT_SOURCE_SCHEMA_LEN];
  char        srcOptions[TSDB_EXT_SOURCE_OPTIONS_LEN];
  // --- metadata version (copied from Catalog's SExtSource.meta_version) ---
  int64_t     metaVersion;       // connector pool uses this to detect config changes
  // --- column type mappings (computed by Parser, carried to Executor via plan) ---
  SExtColTypeMapping* pColTypeMappings;  // one entry per pScanCols column, in the same order
  int32_t             numColTypeMappings;
} SFederatedScanPhysiNode;

typedef struct SProjectPhysiNode {
  SPhysiNode node;
  SNodeList* pProjections;
  bool       mergeDataBlock;
  bool       ignoreGroupId;
  bool       inputIgnoreGroup;
} SProjectPhysiNode;

typedef struct SIndefRowsFuncPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;
  SNodeList* pFuncs;
} SIndefRowsFuncPhysiNode;

typedef struct SInterpFuncPhysiNode {
  SPhysiNode        node;
  SNodeList*        pExprs;
  SNodeList*        pFuncs;
  STimeWindow       timeRange;
  SNode*            pTimeRange;  // for stream
  int64_t           interval;
  int8_t            intervalUnit;
  int8_t            precision;
  EFillMode         fillMode;
  SNode*            pFillValues;  // SNodeListNode
  SNode*            pTimeSeries;  // SColumnNode
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  int64_t           surroundingTime;
} SInterpFuncPhysiNode;

typedef struct SForecastFuncPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;
  SNodeList* pFuncs;
} SForecastFuncPhysiNode, SGenericAnalysisPhysiNode;

typedef struct SSortMergeJoinPhysiNode {
  SPhysiNode   node;
  EJoinType    joinType;
  EJoinSubType subType;
  SNode*       pWindowOffset;
  SNode*       pJLimit;
  int32_t      asofOpType;
  SNode*       leftPrimExpr;
  SNode*       rightPrimExpr;
  int32_t      leftPrimSlotId;
  int32_t      rightPrimSlotId;
  SNodeList*   pEqLeft;
  SNodeList*   pEqRight;
  SNode*       pPrimKeyCond;  // remove
  SNode*       pColEqCond;    // remove
  SNode*       pColOnCond;
  SNode*       pFullOnCond;
  SNodeList*   pTargets;
  SQueryStat   inputStat[2];
  bool         seqWinGroup;
  bool         grpJoin;
} SSortMergeJoinPhysiNode;

typedef struct SHashJoinPhysiNode {
  SPhysiNode   node;
  EJoinType    joinType;
  EJoinSubType subType;
  SNode*       pWindowOffset;
  SNode*       pJLimit;
  SNodeList*   pOnLeft;
  SNodeList*   pOnRight;
  SNode*       leftPrimExpr;
  SNode*       rightPrimExpr;
  int32_t      leftPrimSlotId;
  int32_t      rightPrimSlotId;
  int32_t      timeRangeTarget;  // table onCond filter
  STimeWindow  timeRange;        // table onCond filter
  SNode*       pLeftOnCond;      // table onCond filter
  SNode*       pRightOnCond;     // table onCond filter
  SNode*       pFullOnCond;      // preFilter
  SNodeList*   pTargets;
  SQueryStat   inputStat[2];

  // only in planner internal
  SNode* pPrimKeyCond;
  SNode* pColEqCond;
  SNode* pTagEqCond;
} SHashJoinPhysiNode;

typedef struct SGroupCachePhysiNode {
  SPhysiNode node;
  bool       grpColsMayBeNull;
  bool       grpByUid;
  bool       globalGrp;
  bool       batchFetch;
  SNodeList* pGroupCols;
} SGroupCachePhysiNode;

typedef struct SStbJoinDynCtrlBasic {
  bool    batchFetch;
  int32_t vgSlot[2];
  int32_t uidSlot[2];
  bool    srcScan[2];
} SStbJoinDynCtrlBasic;

typedef struct SVtbScanDynCtrlBasic {
  bool       batchProcessChild;
  bool       hasPartition;
  bool       scanAllCols;
  bool       isSuperTable;
  char       dbName[TSDB_DB_NAME_LEN];
  char       tbName[TSDB_TABLE_NAME_LEN];
  uint64_t   suid;
  uint64_t   uid;
  int32_t    rversion;
  int32_t    accountId;
  SEpSet     mgmtEpSet;
  SNodeList *pScanCols;
  SNodeList *pOrgVgIds;
} SVtbScanDynCtrlBasic;

typedef struct SVtbWindowDynCtrlBasic {
  int32_t               wstartSlotId;
  int32_t               wendSlotId;
  int32_t               wdurationSlotId;
  bool                  isVstb;
  SNodeList*            pTargets;
  EStateWinExtendOption extendOption;
} SVtbWindowDynCtrlBasic;

typedef struct SDynQueryCtrlPhysiNode {
  SPhysiNode             node;
  EDynQueryType          qType;
  bool                   dynTbname;
  SStbJoinDynCtrlBasic   stbJoin;
  SVtbScanDynCtrlBasic   vtbScan;
  SVtbWindowDynCtrlBasic vtbWindow;
} SDynQueryCtrlPhysiNode;

typedef struct SAggPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of group_by_clause and parameter expression of aggregate function
  SNodeList* pGroupKeys;
  SNodeList* pAggFuncs;
  bool       mergeDataBlock;
  bool       groupKeyOptimized;
  bool       hasCountLikeFunc;
} SAggPhysiNode;


typedef struct SExchangePhysiNode {
  SPhysiNode node;
  // for set operators, there will be multiple execution groups under one exchange, and the ids of these execution
  // groups are consecutive
  int32_t    srcStartGroupId;
  int32_t    srcEndGroupId;
  SArray*    childrenVgIds;   // Array<int32_t>, generated by planner for explain mapping
  bool       grpSingleChannel;
  bool       singleSrc;
  SNodeList* pSrcEndPoints;  // element is SDownstreamSource, scheduler fill by calling qSetSuplanExecutionNode
  bool       seqRecvData;
  bool       dynTbname;
} SExchangePhysiNode;

typedef struct SMergePhysiNode {
  SPhysiNode node;
  EMergeType type;
  SNodeList* pMergeKeys;
  SNodeList* pTargets;
  int32_t    numOfChannels;
  int32_t    numOfSubplans;
  int32_t    srcGroupId;
  int32_t    srcEndGroupId;
  bool       groupSort;
  bool       ignoreGroupId;
  bool       inputWithGroupId;
} SMergePhysiNode;

typedef struct SWindowPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of parameter expression of function
  SNodeList* pFuncs;  // [all window]
  SNodeList* pProjs;  // [external window] only for external window
  SNode*     pTspk;   // [all window]      timestamp primary key
  SNode*     pTsEnd;  // [session window]  window end timestamp
  int8_t     unusedParam1;
  int64_t    unusedParam2;
  int64_t    unusedParam3;
  int8_t     unusedParam4;
  int8_t     indefRowsFunc; // [external window] identify whether the window has infinite rows
  bool       mergeDataBlock;// [interval window]
  int64_t    unusedParam5; // useless
} SWindowPhysiNode;

typedef struct SIntervalPhysiNode {
  SWindowPhysiNode window;
  int64_t          interval;
  int64_t          offset;
  int64_t          sliding;
  int8_t           intervalUnit;
  int8_t           slidingUnit;
  STimeWindow      timeRange;
} SIntervalPhysiNode;

typedef SIntervalPhysiNode SMergeIntervalPhysiNode;
typedef SIntervalPhysiNode SMergeAlignedIntervalPhysiNode;

typedef struct SFillPhysiNode {
  SPhysiNode  node;
  EFillMode   mode;
  SNodeList*  pFillExprs;
  SNodeList*  pNotFillExprs;
  SNode*      pWStartTs;  // SColumnNode
  SNode*      pValues;    // SNodeListNode
  STimeWindow timeRange;
  SNode*      pTimeRange;  // STimeRangeNode for create stream
  SNodeList*  pFillNullExprs;
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  SNode*      pSurroundingTime;
} SFillPhysiNode;

typedef struct SMultiTableIntervalPhysiNode {
  SIntervalPhysiNode interval;
  SNodeList*         pPartitionKeys;
} SMultiTableIntervalPhysiNode;

typedef struct SSessionWinodwPhysiNode {
  SWindowPhysiNode window;
  int64_t          gap;
} SSessionWinodwPhysiNode;

typedef struct SStateWindowPhysiNode {
  SWindowPhysiNode window;
  SNode*           pStateKey;
  ETrueForType     trueForType;
  int32_t          trueForCount;
  int64_t          trueForDuration;
  EStateWinExtendOption extendOption;
} SStateWindowPhysiNode;

typedef struct SEventWinodwPhysiNode {
  SWindowPhysiNode window;
  SNode*           pStartCond;
  SNode*           pEndCond;
  ETrueForType     trueForType;
  int32_t          trueForCount;
  int64_t          trueForDuration;
} SEventWinodwPhysiNode;

typedef struct SCountWindowPhysiNode {
  SWindowPhysiNode window;
  int64_t          windowCount;
  int64_t          windowSliding;
} SCountWindowPhysiNode;

typedef struct SAnomalyWindowPhysiNode {
  SWindowPhysiNode window;
  SNodeList*       pAnomalyKeys;
  char             anomalyOpt[TSDB_ANALYTIC_ALGO_OPTION_LEN];
} SAnomalyWindowPhysiNode;

typedef struct SExternalWindowPhysiNode {
  SWindowPhysiNode window;
  STimeWindow      timeRange;
  SNode*           pTimeRange;
  bool             isSingleTable;
  bool             inputHasOrder;
  bool             extWinSplit;
  bool             needGroupSort;
  bool             calcWithPartition;
  int32_t          orgTableVgId; // for vtable window query
  tb_uid_t         orgTableUid;  // for vtable window query
  SNode*           pSubquery;
} SExternalWindowPhysiNode;

typedef struct SSortPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;     // these are expression list of order_by_clause and parameter expression of aggregate function
  SNodeList* pSortKeys;  // element is SOrderByExprNode, and SOrderByExprNode::pExpr is SColumnNode
  SNodeList* pTargets;
  bool       calcGroupId;
  bool       excludePkCol;
} SSortPhysiNode;

typedef SSortPhysiNode SGroupSortPhysiNode;

typedef struct SPartitionPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of partition_by_clause
  SNodeList* pPartitionKeys;
  SNodeList* pTargets;

  bool    needBlockOutputTsOrder;
  int32_t tsSlotId;
} SPartitionPhysiNode;

typedef struct SDataSinkNode {
  ENodeType           type;
  SDataBlockDescNode* pInputDataBlockDesc;
} SDataSinkNode;

typedef struct SDataDispatcherNode {
  SDataSinkNode sink;
  bool          dynamicSchema;
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
  SNode*        pAffectedRows;  // usless
  SNode*        pStartTs;       // usless
  SNode*        pEndTs;         // usless
  int8_t        secureDelete;
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
  SNodeList*     pSubQ;         // the subqueries' subplans,from which to fetch the result
  SNode*         pTagCond;
  SNode*         pTagIndexCond;
  SSHashObj*     pVTables;      // for stream virtual tables
  bool           showRewrite;
  bool           isView;
  bool           isAudit;
  bool           dynamicRowThreshold;
  int32_t        rowsThreshold;
  bool           processOneBlock;
  bool           dynTbname;
} SSubplan;

typedef enum EExplainMode {
  EXPLAIN_MODE_DISABLE = 1,
  EXPLAIN_MODE_STATIC,
  EXPLAIN_MODE_ANALYZE
} EExplainMode;

typedef struct SExplainInfo {
  EExplainMode mode;
  bool         verbose;
  double       ratio;
} SExplainInfo;

typedef struct SQueryPlan {
  ENodeType     type;
  ESubQueryType subQType;
  uint64_t      queryId;
  int32_t       numOfSubplans;
  SNodeList*    pSubplans;  // Element is SNodeListNode. The execution level of subplan, starting from 0.
  SNodeList*    pChildren;  // Element is SQueryPlan*
  char*         subSql;
  SExplainInfo  explainInfo;
  void*         pPostPlan;
  bool          hasFederatedScan;  // true when plan contains at least one SCAN_TYPE_EXTERNAL node
} SQueryPlan;

const char* dataOrderStr(EDataOrderLevel order);

// ---------------------------------------------------------------------------
// Federated query: Plan-to-SQL API
// Defined in source/libs/nodes/src/nodesRemotePlanToSQL.c
// Callers: Module F (Executor), Module B (Connector), EXPLAIN output.
//
// nodesRemotePlanToSQL() — walk a Mode 1 outer SFederatedScanPhysiNode's
//   .pRemotePlan sub-tree and render the full SQL to send to the external source.
//   pRemotePlan  : the mini physi-plan tree (MUST NOT be NULL).
//   dialect      : target SQL dialect (MySQL / PostgreSQL / InfluxQL).
//   ppSQL        : OUT — heap-allocated result string; caller must taosMemoryFree().
//
// The tree must be rooted at one of:
//   SProjectPhysiNode → SSortPhysiNode → SFederatedScanPhysiNode(Mode 2 leaf)
//   SSortPhysiNode    → SFederatedScanPhysiNode(Mode 2 leaf)
//   SFederatedScanPhysiNode(Mode 2 leaf, pRemotePlan==NULL)
//
// nodesExprToExtSQL() — serialize a single expression subtree to a SQL fragment.
//   Returns TSDB_CODE_EXT_SYNTAX_UNSUPPORTED for unsupported expression types.
// ---------------------------------------------------------------------------
int32_t nodesRemotePlanToSQL(const SPhysiNode* pRemotePlan, EExtSQLDialect dialect,
                             char** ppSQL);
int32_t nodesExprToExtSQL(const SNode* pExpr, EExtSQLDialect dialect, char* buf, int32_t bufLen,
                          int32_t* pLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
