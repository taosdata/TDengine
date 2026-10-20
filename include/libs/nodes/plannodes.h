/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#define EX_INTERVAL_MAX_WINDOWS   100000
#define EX_INTERVAL_MAX_FUNCTIONS 16

typedef enum EDataOrderLevel {
  DATA_ORDER_LEVEL_NONE = 1,  // no timestamp order guarantee
  DATA_ORDER_LEVEL_IN_BLOCK,  // ordered only inside each data block
  DATA_ORDER_LEVEL_IN_GROUP,  // ordered inside each partition/group
  DATA_ORDER_LEVEL_GLOBAL     // ordered across all output rows
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

typedef enum ETimeLineSource {
  TIME_LINE_SOURCE_UNSPECIFIED = 0,
  TIME_LINE_SOURCE_PRIMARY_TS = 1,
  TIME_LINE_SOURCE_DEGRADED_TS = 2,
} ETimeLineSource;

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
  EDataOrderLevel    requireDataOrder;  // minimum data-order level required from this node's input
  EDataOrderLevel    resultDataOrder;   // data-order level guaranteed by this node's output
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

typedef enum EInterpMode {
  // Existing PREV -> MAIN -> NEXT protocol. Zero also leaves calloc-initialized
  // non-interp scans unaffected.
  INTERP_MODE_LEGACY = 0,
  // MAIN -> NEXT -> PREV for groups enumerated statically by Scan, including
  // no partition, BY tbname, and BY tags.
  INTERP_MODE_MAIN_FIRST,
  // Same phased scan order, while interpC concurrently manages groups derived
  // from PARTITION BY data columns. This does not mean multiple vgroups.
  INTERP_MODE_MULTI_GROUP,
  // Complete each static group before advancing; MAIN prefixes may stream.
  INTERP_MODE_GROUP_FIRST,
} EInterpMode;

// ---- Federated query pushdown bit masks ----
// Used by Optimizer to mark what can be pushed to remote; Phase 1 = all 0 (no pushdown)
#define FQ_PUSHDOWN_FILTER          (1u << 0)
#define FQ_PUSHDOWN_PROJECTION      (1u << 1)
#define FQ_PUSHDOWN_LIMIT           (1u << 2)
#define FQ_PUSHDOWN_AGG             (1u << 3)
#define FQ_PUSHDOWN_ORDER           (1u << 4)
#define FQ_PUSHDOWN_JOIN            (1u << 5)
#define FQ_PUSHDOWN_REQUIRED_FILTER (1u << 6)

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
  // Producer selector; scans not feeding interp must remain LEGACY.
  // Remove only after every interp scan path uses main-first.
  int8_t             interpMode;        // EInterpMode
  int8_t             interpFillMode;    // EFillMode of the interp above this scan; the ext-window scan
                                        // for the side a fill mode does not consume is skipped (0 = both)
  SNode*             pTimeRange;  // for create stream
  SNode*             pExtTimeRange;  // for create stream
  SNode*             pPrimaryCond;   // for remote node, splited from filter conditions
  SName              tableName;
  bool               showRewrite;
  int8_t             xnodeDsnOutputMode;
  double             ratio;
  SNodeList*         pDynamicScanFuncs;
  int32_t            dataRequired;
  bool               lastOnlyScan;
  int64_t            interval;
  int64_t            offset;
  int64_t            sliding;
  int8_t             intervalUnit;
  int8_t             slidingUnit;
  bool               fastInterval;
  bool               exInterval;
  int8_t             firstDayOfWeek;  /* 0-6, propagated from window logic node */
  void*              timezone;        /* timezone_t handle for interval alignment */
  char               timezoneName[TD_TIMEZONE_LEN]; /* IANA name for serialization */
  SNode*             pTagCond;
  SNode*             pTagIndexCond;
  int8_t             triggerType;
  int64_t            watermark;
  int64_t            deleteMark;
  int8_t             igExpired;
  int8_t             igCheckUpdate;
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
  bool               smallDataScanSort; // hint: emit Table Scan + Sort instead of Table Merge Scan
  bool               needSplit;
  bool               noPseudoRefAfterGrp;  // no pseudo columns referenced ater group/partition clause
  bool               virtualStableScan;
  bool               phTbnameScan;
  uint8_t            outputTsPrecision;  // cross-precision: convert TIMESTAMP cols to this precision at scan output (UINT8_MAX = no conversion). Set by vstableAggOptimize on source scans so partial aggregation runs in vtable precision.
  EStreamPlaceholder placeholderType;
  // --- external scan extension (valid only when scanType == SCAN_TYPE_EXTERNAL) ---
  uint32_t    fqPushdownFlags;                     // FQ_PUSHDOWN_* bitmask; Phase 1 = 0
  SNode*      pExtTableNode;  // cloned SExtTableNode carrying connection info for Planner  ->  Physi transfer
  SNodeList*  pFqAggFuncs;    // Phase 2: pushdown-eligible aggregate function list
  SNodeList*  pFqGroupKeys;   // Phase 2: pushdown-eligible GROUP BY columns
  SNodeList*  pFqSortKeys;    // Phase 2: pushdown-eligible ORDER BY columns
  SNode*      pFqLimit;       // Phase 2: pushdown-eligible LIMIT
  SNodeList*  pFqJoinTables;  // Phase 2: pushdown-eligible JOIN tables
  // Logical pushdown sub-plan set by the FqPushdown optimizer rule.
  // Contains the chain of pushed-down Sort/Project logic nodes (topmost first,
  // bottommost has pChildren=NULL  -  the scan itself is NOT in this chain).
  // Physical plan generation converts this to SFederatedScanPhysiNode.pRemotePlan.
  SNode*      pRemoteLogicPlan;
  // Saved external-column filters that must run remotely, independent of optional
  // query pushdown. Physical planning moves them into the remote leaf's pConditions
  // before serialization; they must never become local scan conditions.
  SNode*      pPushedConditions;
  bool        twoPassRescanSource;  // vtable two-pass: origin keeps reader alive for phase-2 rescan
  // --- table count scan extension (valid only when scanType == SCAN_TYPE_TABLE_COUNT) ---
  // ins_tables row type filter (TSDB_* table entry type); 0 = no filter.
  int8_t      tbCountType;
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
  bool           isCrossJoin;
  bool           isLowLevelJoin;
  bool           seqWinGroup;
  bool           grpJoin;
  bool           multiGroupJoin;
  // Algorithm hint consumed by planner; rules may reset it when the request is incompatible.
  EJoinAlgorithm joinAlgoHint;
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

  // TagRef support fields - for tag references only (NOT column references)
  SNodeList*    pTagRefSources;  // STagRefSourceLogicNode list for tag references
  SNodeList*    pLocalTags;      // Local tags (no reference) - use TagScan
  SNodeList*    pRefTagCols;     // Referenced tag columns - need source table scan
  SNodeList*    pConstRefTags;   // Tag columns referencing a vntb/ntb source tag - table-level
                                 // constants resolved by the scan operator itself
  SNode*        pTagFilterCond;  // Tag filter condition extracted from WHERE clause

  // Flags
  bool          hasTagRef;        // true if has tag references
  bool          hasLocalTag;      // true if has local tags
  bool          twoPassMode;      // PERCENTILE/repeat-scan: drive two passes (PRE_SCAN then MAIN_SCAN)
  bool          hasFilterCond;    // saved external filters require virtual-row merging before agg/window
  bool          hasConstRefTag;   // true if has const-ref tags (vntb/ntb source)
} SVirtualScanLogicNode;

// Tag reference column information
typedef struct STagRefColumn {
  ENodeType type;
  col_id_t  colId;        // Column ID in virtual table
  col_id_t  sourceColId;  // Tag column ID in source table
  char      colName[TSDB_COL_NAME_LEN];
  char      sourceColName[TSDB_COL_NAME_LEN];  // Tag name in source table
  int32_t   bytes;                             // Tag bytes
  int8_t    dataType;                          // Tag data type
} STagRefColumn;

// Tag reference source node - represents a source table that provides referenced tags
typedef struct STagRefSourceLogicNode {
  SLogicNode    node;
  SName         sourceTableName;  // Source table name (db.table)
  uint64_t      sourceSuid;       // Source super table uid
  int32_t       sourceId;         // Source ID for matching
  SNodeList*    pRefCols;         // STagRefColumn list - tags to fetch from this source
  SVgroupsInfo* pVgroupList;      // Vgroup information for this source table
  bool          isUsedInFilter;   // true if used in WHERE clause for filtering
  bool          isUsedInProjection; // true if used in SELECT clause for output
} STagRefSourceLogicNode;

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
  bool       hasMixedDistinct;    // optimizer set: child[0]=non-distinct, child[1]=distinct
  int32_t    numDistinctFuncs;    // number of distinct functions (trailing in pAggFuncs)
  SNodeList* pTsmaSubplans;
} SAggLogicNode;

typedef struct SProjectLogicNode {
  SLogicNode node;
  SNodeList* pProjections;
  char       stmtName[TSDB_TABLE_NAME_LEN];
  bool       ignoreGroupId;
  bool       inputIgnoreGroup;
  bool       isSetOpProj;
  bool       isUnionDedup;  // DS section5.3.8.6: true -> emit UNION (dedup), false -> UNION ALL
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
  int8_t        timelineSource;
  int8_t        interpMode;  // EInterpMode
  // Column-group membership remains required after interpMode fallback.
  bool          requireMainMembership;
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  int64_t       surroundingTime;
  void*         timezone;        // timezone_t handle for calendar/DST-aware EVERY stepping (d/w)
  char          timezoneName[TD_TIMEZONE_LEN]; /* IANA name for serialization */
} SInterpFuncLogicNode;

typedef struct SForecastFuncLogicNode {
  SLogicNode node;
  SNodeList* pFuncs;
} SForecastFuncLogicNode, SGenericAnalysisLogicNode;

typedef struct SRowsetSourceLogicNode {
  SLogicNode node;
  int32_t    colCount;
  int32_t    numBlocks;
  int32_t    totalRows;
  bool       hasPrimaryTs;   // first column is TIMESTAMP
  bool       isSortedByTs;   // rows are in ascending primary-ts order
  int16_t    primaryTsSlot;
  int32_t    blockBufLen;
  uint8_t*   pBlockBuf;  // owned; released by destroy; moved to physi node by physical planner (then set to NULL)
} SRowsetSourceLogicNode;

typedef struct SGroupCacheLogicNode {
  SLogicNode node;
  bool       grpColsMayBeNull;
  bool       grpByUid;
  bool       globalGrp;
  bool       batchFetch;
  bool       twoPassMode;  // single-group two-pass for repeat-scan functions (e.g. PERCENTILE)
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
  bool          hasLocalTag;       // Planner flag: true if STB has local (non-ref) tags
  bool          useTagScan;
  char          dbName[TSDB_DB_NAME_LEN];
  char          tbName[TSDB_TABLE_NAME_LEN];
  uint64_t      suid;
  uint64_t      uid;
  int32_t       rversion;
  SNodeList*    pOrgVgIds;
  SVgroupsInfo* pVgroupList;
  SNodeList*    pScanCols;
  // Tag-ref filter optimization: TERMINAL physical source info (resolved through chain)
  uint64_t      tagRefSourceSuid;     // Terminal physical STB UID (0 = not available)
  col_id_t      tagRefSourceColId;    // Terminal physical tag column ID
  int8_t        tagRefSourceColType;  // Terminal physical tag column data type
  char          tagRefTerminalColName[TSDB_COL_NAME_LEN]; // Terminal physical tag column name
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
  // DELETE with an ordinary column filter: the deleter derives precise ranges from the scanned ts
  // values, so count()/first()/last() are not computed and pStartTs carries the ts column instead.
  bool             deleteRangeMode;
} SVnodeModifyLogicNode;

typedef struct SExchangeLogicNode {
  SLogicNode node;
  int32_t    srcStartGroupId;
  int32_t    srcEndGroupId;
  bool       seqRecvData;
  bool       dynTbname;
  bool       fastInterval;
  bool       localBlockExchange;
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
  bool       outerGroupPhaseMode;
} SMergeLogicNode;

typedef enum EWindowAlgorithm {
  INTERVAL_ALGO_HASH = 1,
  INTERVAL_ALGO_MERGE,
  SESSION_ALGO_MERGE,
  EXTERNAL_ALGO_HASH,
  EXTERNAL_ALGO_MERGE,
} EWindowAlgorithm;

typedef enum EExtWinInputMode {
  EXT_WIN_INPUT_WINDOW_BOUND,
  EXT_WIN_INPUT_BROADCAST,
} EExtWinInputMode;

typedef struct SExtWindowFillInfo {
  EFillMode  mode;
  SNodeList* pFillExprs;
  SNode*     pFillValues;
} SExtWindowFillInfo;

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
  bool                  fastInterval;  // FAST_INTERVAL hint selected this node's dedicated execution path
  bool                  exInterval;    // EX_INTERVAL hint selected this node's dedicated execution path
  SNodeList*            pTsmaSubplans;
  // for interval window
  int64_t               interval;
  int64_t               offset;
  int64_t               sliding;
  int8_t                intervalUnit;
  int8_t                slidingUnit;
  bool                  hasFill;
  int8_t                firstDayOfWeek;  /* 0-6, from connection; default 4 (Thu) */
  void*                 timezone;        /* timezone_t handle for calendar alignment */
  char                  timezoneName[TD_TIMEZONE_LEN]; /* IANA name for serialization */
  // for session window
  int64_t               sessionGap;
  SNode*                pTsEnd;
  // for state window
  SNodeList*            pStateExprs;
  EStateWinExtendOption extendOption;
  // for event window
  SNode*                pStartCond;
  SNode*                pEndCond;
  // for event and state window
  int32_t               trueForType;
  int32_t               trueForCount;
  int64_t               trueForDuration;
  int32_t               startTrueForType;
  int32_t               startTrueForCount;
  int64_t               startTrueForDuration;
  int32_t               endTrueForType;
  int32_t               endTrueForCount;
  int64_t               endTrueForDuration;
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
  EExtWinInputMode      extWinInputMode;
  SNode*                pWindowConditions;
  SExtWindowFillInfo    extFill;
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
  int64_t     interval;
  int64_t     offset;
  int64_t     sliding;
  int8_t      intervalUnit;
  int8_t      slidingUnit;
  int8_t      firstDayOfWeek;
  void*       timezone;
  char        timezoneName[TD_TIMEZONE_LEN];
  STimeWindow timeRange;
  SNode*      pTimeRange; // STimeRangeNode for create stream
  SNodeList*  pFillNullExprs;
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  SNode*      pSurroundingTime;
  bool        indefRowsMode;
} SFillLogicNode;

typedef struct SWindowFuncLogicNode {
  SLogicNode node;
  SNodeList* pPartitionKeys;
  SNodeList* pOrderKeys;
  SNodeList* pFuncs;
  SNode*     pFrame;
} SWindowFuncLogicNode;

typedef struct SSortLogicNode {
  SLogicNode node;
  SNodeList* pSortKeys;
  bool       groupSort;
  bool       skipPKSortOpt;
  bool       joinInputSort;
  bool       calcGroupId;
  bool       excludePkCol;  // exclude PK ts col when calc group id
  // Leading sort keys used for group id; 0 keeps legacy behavior.
  int32_t numOfGroupKeys;
} SSortLogicNode;

typedef struct SDistinctFilterLogicNode {
  SLogicNode node;
  SNode*     pDistinctCol;   // the column expression to deduplicate on
  SNodeList* pGroupKeys;     // group-by columns for per-group dedup (NULL if no GROUP BY)
  int8_t     colType;        // TSDB data type of distinct column
  int32_t    colBytes;       // byte width of distinct column
  // interval-aware dedup (per-window distinct)
  bool       hasInterval;    // if true, dedup per interval window
  int64_t    interval;
  int64_t    offset;
  int64_t    sliding;
  int8_t     intervalUnit;
  int8_t     slidingUnit;
  int8_t     precision;      // time precision of the database
  int8_t     firstDayOfWeek; // 0-6
  void*      timezone;       // timezone_t handle
  char       timezoneName[TD_TIMEZONE_LEN];
} SDistinctFilterLogicNode;

typedef struct SPartitionLogicNode {
  SLogicNode node;
  SNodeList* pPartitionKeys;
  SNodeList* pTags;
  SNode*     pSubtable;
  SNodeList* pAggFuncs;
  // Rollback state for a mixed-key interp rewrite until post-split approval.
  SNodeList* pInterpPreSplitPartitionKeys;
  SNodeList* pInterpPreSplitGroupTags;

  bool     interpPreSplitGroupSort;
  bool     phasedMode;  // true for interp phase-aware partition execution
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

#define SUBPLAN_SPLIT_FLAG_LOCAL_BLOCK_EXCHANGE (1 << 3)

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
  bool          pushedDown;  // true when this whole subquery subplan has been absorbed by an ancestor FQ remote plan
  bool          execOnChildNode;
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
  EOrder              inputTsOrder;      // timestamp ordering of input data (ASC/DESC/UNKNOWN)
  EOrder              outputTsOrder;     // timestamp ordering of output data (ASC/DESC/UNKNOWN)
  EDataOrderLevel     requireDataOrder;  // minimum data order level this node requires from its input
  EDataOrderLevel     resultDataOrder;   // data order level this node guarantees for its output
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
  uint8_t    outputTsPrecision;  // cross-precision: convert TIMESTAMP cols to this precision at scan output (UINT8_MAX = no conversion)
  bool       useTriggerTableList;
} SScanPhysiNode;

typedef struct STagScanPhysiNode {
  SScanPhysiNode scan;
  bool           onlyMetaCtbIdx;  // no tbname, tag index not used.
} STagScanPhysiNode;

typedef SScanPhysiNode SBlockDistScanPhysiNode;

// Tag reference source physical node - represents a source table scan for referenced tags
typedef struct STagRefSourcePhysiNode {
  SPhysiNode    node;
  SName         sourceTableName;  // Source table name (db.table)
  uint64_t      sourceSuid;       // Source super table uid
  int32_t       sourceId;         // Source ID for matching
  SNodeList*    pRefCols;         // STagRefColumn list - tags to fetch from this source
  SVgroupsInfo* pVgroupList;      // Vgroup information for this source table
  SNodeList*    pScanCols;        // Columns to scan from source table
  bool          isUsedInFilter;   // true if used in WHERE clause for filtering
  bool          isUsedInProjection; // true if used in SELECT clause for output
} STagRefSourcePhysiNode;

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

  // TagRef support fields - for tag references only (NOT column references)
  SNodeList*     pTagRefSources;  // STagRefSourcePhysiNode list for tag references
  SNodeList*     pLocalTags;      // Local tags (no reference) - use TagScan
  SNodeList*     pRefTagCols;     // Referenced tag columns - need source table scan
  SNodeList*     pConstRefTags;   // Tag columns referencing a vntb/ntb source tag - table-level
                                  // constants resolved by the scan operator itself
  SNode*         pTagFilterCond;  // Tag filter condition extracted from WHERE clause

  // Flags
  bool           hasTagRef;        // true if has tag references
  bool           hasLocalTag;      // true if has local tags
  bool           hasConstRefTag;   // true if has const-ref tags (vntb/ntb source); carried in the
                                   // TLV/JSON plan contract (PHY_VIRTUAL_TABLE_SCAN_CODE_HAS_CONST_REF_TAG).
                                   // The pConstRefTags LIST is the executor's single decision basis
                                   // (ensureRefTagsResolved); this flag is only a drift sentinel -- the
                                   // executor qErrors when it disagrees with the list (see DS #5.5.3)
  SEpSet         mgmtEpSet;        // mnode endpoints for const-ref chain resolution hops. PLAN-TIME
                                   // SNAPSHOT, same pattern as SSystemTableScanPhysiNode.mgmtEpSet and
                                   // execNode.epSet: NOT an authoritative live topology -- the RPC layer
                                   // refreshes stale endpoints via response-attached epsets, a dropped
                                   // node surfaces as a not-exist broken chain. eps is an INLINE fixed
                                   // array (tmsg.h), so every copy/clone/decode is a complete value with
                                   // no shared pointer; ALL-ZERO means "not set" (remote hops unavailable
                                   // in const-ref resolution); each vscan node owns its own copy
  bool           twoPassMode;      // PERCENTILE/repeat-scan: drive two passes (PRE_SCAN then MAIN_SCAN)
}SVirtualScanPhysiNode;

typedef struct SLastRowScanPhysiNode {
  SScanPhysiNode scan;
  SNodeList*     pGroupTags;
  bool           groupSort;
  bool           ignoreNull;
  // The cache reader may collect multiple table results before this operator emits them.
  // Results are emitted as their original per-table group blocks.
  bool           batchMultiTableRead;
  SNodeList*     pTargets;
  SArray*        pFuncTypes;
  // Valid only for TABLE_COUNT_SCAN (BY_TYPE): ins_tables row type filter, 0 = no filter.
  // The struct is shared with LAST_ROW_SCAN, where this stays 0 and is still serialized.
  int8_t         tbCountType;
} SLastRowScanPhysiNode;

typedef SLastRowScanPhysiNode STableCountScanPhysiNode;

typedef struct SSystemTableScanPhysiNode {
  SScanPhysiNode scan;
  SEpSet         mgmtEpSet;
  int32_t        accountId;
  bool           showRewrite;
  int8_t         xnodeDsnOutputMode;
  bool           sysInfo;
  union {
    uint16_t privInfo;
    struct {
      uint16_t minSecLevel : 3;  // user min security level
      uint16_t privInfoBasic : 1;
      uint16_t privInfoPrivileged : 1;
      uint16_t privInfoAudit : 1;
      uint16_t privInfoSec : 1;
      uint16_t privPerfBasic : 1;
      uint16_t privPerfPrivileged : 1;
      uint16_t maxSecLevel : 3;  // user max security level
      uint16_t macMode   : 1;    // 1 = MAC mandatory
      uint16_t reserved : 3;
    };
  };
  // Client charset name for rendering NCHAR/JSON system-table tag values; empty
  // string means "use the server default charset" (tsCharsetCxt). Serialized in
  // TLV/JSON; restored to an SConvInfo* in the sysscan operator via taosConvInit.
  char charsetName[TD_CHARSET_LEN];
} SSystemTableScanPhysiNode;

typedef struct STableScanPhysiNode {
  SScanPhysiNode scan;
  uint8_t        scanSeq[2];  // first is scan count, and second is reverse scan count
  STimeWindow    scanRange;
  STimeWindow*   pExtScanRange;
  // Producer selector; scans not feeding interp must remain LEGACY.
  // Remove only after every interp scan path uses main-first.
  int8_t         interpMode;        // EInterpMode
  int8_t         interpFillMode;    // see SScanLogicNode
  SNode*         pTimeRange;  // for create stream
  SNode*         pExtTimeRange;  // for create stream
  SNode*         pPrimaryCond;   // for remote node, splited from filter conditions
  double         ratio;
  int32_t        dataRequired;
  bool           lastOnlyScan;
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
  bool           fastInterval;
  bool           exInterval;
  int8_t         firstDayOfWeek;  /* 0-6, propagated from interval logic node */
  void*          timezone;        /* timezone_t handle for interval alignment */
  char           timezoneName[TD_TIMEZONE_LEN]; /* IANA name for TLV serialization */
  bool           ownsTimezone;    /* true when timezone was tzalloc'd during deser */
  int8_t         triggerType;
  int64_t        watermark;
  int8_t         igExpired;
  bool           assignBlockUid;
  int8_t         igCheckUpdate;
  bool           filesetDelimited;
  bool           needCountEmptyTable;
  bool           paraTablesSort;
  bool           smallDataTsSort;
  bool           twoPassRescanSource;  // vtable two-pass: qworker keeps task/sink/reader alive for phase-2 rescan
} STableScanPhysiNode;

typedef STableScanPhysiNode STableSeqScanPhysiNode;
typedef STableScanPhysiNode STableMergeScanPhysiNode;
typedef STableScanPhysiNode SStreamScanPhysiNode;

// ---- Federated query: column type mapping entry ----
// Computed by Parser (extTypeNameToTDengineType()), written into physical plan,
// then passed to Connector for raw value  ->  TDengine column binary conversion.
typedef struct SExtColTypeMapping {
  char      extTypeName[64];  // original external type name (e.g. "VARCHAR(255)", "INT4")
  SDataType tdType;           // mapped TDengine type: type, precision, scale, bytes
  int8_t    tsPrecision;      // source-side timestamp precision for TIMESTAMP columns
                              // (TSDB_TIME_PRECISION_MICRO = 1, TSDB_TIME_PRECISION_NANO = 2);
                              // 0 for non-timestamp columns or when unknown.
  char      colName[65];      // TDengine column name (TSDB_COL_NAME_LEN=65); used by InfluxDB
                              // connector for name-based Arrow column lookup on SELECT *.
} SExtColTypeMapping;

typedef enum {
  EXT_TIMESTAMP_TZ_CLASS_UNKNOWN = 0,
  EXT_TIMESTAMP_TZ_CLASS_AWARE   = 1,
  EXT_TIMESTAMP_TZ_CLASS_NAIVE   = 2,
} EExtTimestampTzClass;

typedef struct SExtTimestampTypeHint {
  char   colName[TSDB_COL_NAME_LEN];  // remote column name as it appears in pRemotePlan
  char   extTypeName[64];             // original external type name for diagnostics
  int8_t tzClass;                     // EExtTimestampTzClass
} SExtTimestampTypeHint;
// ---- Federated query: physical scan node ----
// Inherits SPhysiNode directly (NOT SScanPhysiNode): external scan has no uid/suid/tableType.
// All connection info is embedded here because Executor runs in taosd (server side) and
// cannot access Catalog (client-side libtaos). The physical plan is the only data channel
// from client to server.
//
// TWO USAGE MODES  -  determined by whether pRemotePlan is NULL:
//
// Mode 1  -  Outer wrapper node (pRemotePlan != NULL):
//   Appears in the TDengine executor plan as the scan leaf.
//   pRemotePlan is a logic-plan sub-tree encoding the full SQL to push down:
//     [ProjectLogic]? -> [SortLogic]? -> [Agg/Join/...] -> ScanLogic leaf
//   nodesRemotePlanToSQL() walks pRemotePlan to generate the external SQL string.
//   pExtTable and pScanCols are NOT used for SQL generation in this mode.
//   Connection fields (srcHost/srcPort/...) provide the data source endpoint.
//
// Mode 2  -  Inner leaf node (pRemotePlan == NULL):
//   Appears only INSIDE a pRemotePlan sub-tree.  Never directly in the executor plan.
//   pExtTable + pScanCols   ->  FROM clause and SELECT column list.
//   node.pConditions        ->  WHERE clause (simple push-downable predicates).
//   node.pLimit             ->  LIMIT / OFFSET clause.
//   Connection fields are NOT used (the outer Mode 1 node holds them).
typedef struct SFederatedScanPhysiNode {
  SPhysiNode  node;              // standard physi node header (pConditions, pLimit, pOutputDataBlockDesc, etc.)
  SNode*      pExtTable;         // SExtTableNode*  -  external table AST node  [used in Mode 2]
  SNodeList*  pScanCols;         // scan column list                           [used in Mode 2]
  SNode*      pRemotePlan;       // logic-plan sub-tree for SQL gen            [non-NULL = Mode 1]
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
  // --- planner-provided remote timestamp type hints for runtime SQL rendering ---
  SExtTimestampTypeHint* pTimestampTypeHints;
  int32_t                numTimestampTypeHints;
  // --- two-pass scan support (for PERCENTILE and other REPEAT_SCAN_FUNC) ---
  bool                twoPassMode;       // true: FedScan must do PRE_SCAN pass then MAIN_SCAN pass
  // --- client timezone (filled by Planner from SPlanContext.timezoneName) ---
  // IANA timezone name or POSIX offset string (e.g. "Asia/Shanghai", "UTC", "Etc/GMT-8").
  // Executor reconstructs timezone_t via tzalloc(timezone) and passes it to the Connector
  // for converting timezone-naive source types (MySQL DATETIME, PG timestamp without tz, etc.)
  // to UTC epoch. Empty string means "use server global default timezone".
  char                timezone[TD_TIMEZONE_LEN];
  // --- time range for vstb dynamic queries (set by optimizer pushdown) ---
  STimeWindow         scanRange;         // INT64_MIN..INT64_MAX = no filter
  // True when this federate scan is a downstream of a virtual-table scan
  // (either static or VStb dispatch).  Used by the executor to gate
  // per-block TIMESTAMP precision conversion to the vtable's declared
  // precision; direct external-table queries leave this false.
  bool                underVTableScan;
  // --- stream calc scan time range (STimeRangeNode) ---
  // Set only for external scans under a stream calc clause whose SELECT has a
  // time-returning time range (e.g. _twstart/_twend or casted localtime
  // placeholders).  The external-table reader (ETR) evaluates it per fetch - the
  // same way vnodeStream.c processCalaTimeRange does for table scans - to bound
  // the remote query.  NULL otherwise.
  SNode*              pTimeRange;
} SFederatedScanPhysiNode;

typedef struct SProjectPhysiNode {
  SPhysiNode node;
  SNodeList* pProjections;
  bool       mergeDataBlock;
  bool       ignoreGroupId;
  bool       inputIgnoreGroup;
  bool       isUnionDedup;  // DS section5.3.8.6: true -> emit UNION (dedup), false -> UNION ALL
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
  int8_t            timelineSource;
  int8_t            interpMode;  // EInterpMode
  // Column-group membership remains required after interpMode fallback.
  bool              requireMainMembership;
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  int64_t           surroundingTime;
  void*             timezone;        // timezone_t handle; EVERY calendar units (d/w) are calendar/DST aware
  char              timezoneName[TD_TIMEZONE_LEN]; /* IANA name for TLV serialization */
  bool              ownsTimezone;    /* true when timezone was tzalloc'd during deser */
  bool              mergeDataBlock;
} SInterpFuncPhysiNode;

typedef struct SForecastFuncPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;
  SNodeList* pFuncs;
} SForecastFuncPhysiNode, SGenericAnalysisPhysiNode;

typedef struct SRowsetSourcePhysiNode {
  SPhysiNode node;           // QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE
  int32_t    colCount;
  int32_t    numBlocks;
  int32_t    totalRows;
  bool       hasPrimaryTs;   // first column is TIMESTAMP
  bool       isSortedByTs;   // rows are in ascending primary-ts order
  int16_t    primaryTsSlot;
  int32_t    blockBufLen;
  uint8_t*   pBlockBuf;      // SSDataBlock binary; owned; freed by destroy
} SRowsetSourcePhysiNode;

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
  bool   multiGroupJoin;
} SHashJoinPhysiNode;

// Theta join currently uses the hash join physical layout and wire codec.
typedef SHashJoinPhysiNode SThetaJoinPhysiNode;

typedef struct SGroupCachePhysiNode {
  SPhysiNode node;
  bool       grpColsMayBeNull;
  bool       grpByUid;
  bool       globalGrp;
  bool       batchFetch;
  bool       twoPassMode;  // single-group two-pass for repeat-scan functions (e.g. PERCENTILE)
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
  bool       hasLocalTag;       // Planner flag: true if STB has local (non-ref) tags
  char       dbName[TSDB_DB_NAME_LEN];
  char       tbName[TSDB_TABLE_NAME_LEN];
  uint64_t   suid;
  uint64_t   uid;
  int32_t    rversion;
  int32_t    accountId;
  SEpSet     mgmtEpSet;
  SNodeList *pScanCols;
  SNodeList *pOrgVgIds;
  // Tag-ref filter optimization: propagated from VirtualScanPhysiNode
  SNode*     pTagFilterCond;   // WHERE tag_ref_col = const condition
  SNodeList *pRefTagCols;      // SColumnNode list of ref-tag columns
  // Tag-ref filter optimization: TERMINAL physical source info (resolved through chain)
  uint64_t   tagRefSourceSuid;     // Terminal physical STB UID (0 = not available)
  col_id_t   tagRefSourceColId;    // Terminal physical tag column ID
  int8_t     tagRefSourceColType;  // Terminal physical tag column data type
  char       tagRefTerminalColName[TSDB_COL_NAME_LEN]; // Terminal physical tag column name
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
  bool       hasMixedDistinct;    // true when AGG has 2 children: child[0]=non-distinct, child[1]=distinct
  int32_t    numDistinctFuncs;    // number of distinct functions (trailing in pAggFuncs when hasMixedDistinct)
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
  bool       fastInterval;
  bool       localBlockExchange;
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
  bool       outerGroupPhaseMode;
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
  int8_t           firstDayOfWeek;  /* 0-6, resolved by client before dispatch */
  void*            timezone;        /* timezone_t handle; NULL  ->  server default */
  char             timezoneName[TD_TIMEZONE_LEN]; /* IANA name for TLV serialization */
  bool             ownsTimezone;    /* true when timezone was tzalloc'd during deser */
  bool             fastInterval;    /* FAST_INTERVAL execution path; false preserves the legacy operator */
  bool             exInterval;      /* EX_INTERVAL execution path; false preserves the legacy operator */
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
  int64_t     interval;
  int64_t     offset;
  int64_t     sliding;
  int8_t      intervalUnit;
  int8_t      slidingUnit;
  int8_t      firstDayOfWeek;
  void*       timezone;
  bool        ownsTimezone;
  char        timezoneName[TD_TIMEZONE_LEN];
  STimeWindow timeRange;
  SNode*      pTimeRange;  // STimeRangeNode for create stream
  SNodeList*  pFillNullExprs;
  // duration expression for surrounding_time (only for PREV/NEXT/NEAR)
  SNode*      pSurroundingTime;
  bool        indefRowsMode;
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
  SNodeList*       pStateKeys;
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
  ETrueForType     startTrueForType;
  int32_t          startTrueForCount;
  int64_t          startTrueForDuration;
  ETrueForType     endTrueForType;
  int32_t          endTrueForCount;
  int64_t          endTrueForDuration;
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
  bool             matchPartitionBy;
  EExtWinInputMode   extWinInputMode;
  SNode*             pWindowConditions;
  SExtWindowFillInfo extFill;
  int32_t          orgTableVgId; // for vtable window query
  tb_uid_t         orgTableUid;  // for vtable window query
  SNode*           pSubquery;
  bool               broadcastInputReusable;
  bool               broadcastReaderBatch;
} SExternalWindowPhysiNode;

typedef struct SWindowFuncPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;
  SNodeList* pPartitionKeys;
  SNodeList* pOrderKeys;
  SNodeList* pFuncs;
  SNode*     pFrame;
} SWindowFuncPhysiNode;

typedef struct SSortPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;     // these are expression list of order_by_clause and parameter expression of aggregate function
  SNodeList* pSortKeys;  // element is SOrderByExprNode, and SOrderByExprNode::pExpr is SColumnNode
  SNodeList* pTargets;
  bool       calcGroupId;
  bool       excludePkCol;
  // Leading sort keys used for group id; 0 keeps legacy behavior.
  int32_t numOfGroupKeys;
} SSortPhysiNode;

typedef SSortPhysiNode SGroupSortPhysiNode;

typedef struct SDistinctFilterPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;            // scalar expressions to evaluate before dedup (precalc)
  SNodeList* pTargets;
  int16_t    distinctColSlotId;  // slot of the distinct column in input block
  int8_t     colType;            // TSDB data type
  int32_t    colBytes;           // byte width
  int16_t    numGroupCols;       // number of group-by columns (0 if no GROUP BY); executor uses it as a boolean hasGroup flag
  // interval-aware dedup (per-window distinct)
  bool       hasInterval;
  int16_t    tsSlotId;           // slot of the primary timestamp column
  int64_t    interval;
  int64_t    offset;
  int64_t    sliding;
  int8_t     intervalUnit;
  int8_t     slidingUnit;
  int8_t     precision;
  int8_t     firstDayOfWeek;
  void*      timezone;           // timezone_t handle
  char       timezoneName[TD_TIMEZONE_LEN];
  bool       ownsTimezone;
} SDistinctFilterPhysiNode;

typedef struct SPartitionPhysiNode {
  SPhysiNode node;
  SNodeList* pExprs;  // these are expression list of partition_by_clause
  SNodeList* pPartitionKeys;
  SNodeList* pTargets;

  bool    phasedMode;  // true for interp phase-aware partition execution
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
  bool          localBlockExchange;
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
  // Column-filter driven delete: the child is the scan itself (no aggregation), and the sink derives
  // the precise ranges from the ts column at tsSlotId instead of from first()/last().
  bool          rangeMode;
  int32_t       tsSlotId;
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
  // Keep completed vnode task resources until scheduler drops the task.
  bool           delayFreeTask;
  bool           requiresAncestorContext;
  bool           inheritedMultiGroupJoin;
  bool           dynTbname;
  bool           localBlockExchange;
  bool           execOnChildNode;
  int16_t        userAppId;  // encoded from client user_app, propagated for server-side use
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
  timezone_t   tz;       /* session timezone for formatting timestamps */
} SExplainInfo;

typedef struct SCorrelationTimeRangeInitNode {
  ENodeType  type;
  int32_t    scopeId;
  uint8_t    precision;
  bool       interpRange;
  bool       allowNoTimeRange;
  SSubplanId targetSubplanId;
  int64_t    targetBlockId;
  ENodeType  targetNodeType;
  SNode*     pRangeExpr;
} SCorrelationTimeRangeInitNode;

typedef struct SCorrelationTemplateNode {
  ENodeType  type;
  int32_t    templateId;
  int32_t    scopeId;
  SDataType  resultType;
  char       resultName[TSDB_COL_NAME_LEN];
  SNodeList* pParams;           // Owned SCorrelationParamNode definitions.
  SNodeList* pNestedTemplates;  // Owned templates, separate from pChildren jobs.
  SNodeList* pTimeRangeInit;    // Owned SCorrelationTimeRangeInitNode descriptors.
  // Owned complete-DAG TLV bytes, immutable once frozen by P06. Copy bytes;
  // never call nodesCloneNode(SQueryPlan) to instantiate a template.
  int32_t    planLen;
  char*      pPlan;
} SCorrelationTemplateNode;

typedef struct SQueryPlan {
  ENodeType     type;
  ESubQueryType subQType;
  uint64_t      queryId;
  int32_t       numOfSubplans;
  SNodeList*    pSubplans;  // Element is SNodeListNode. The execution level of subplan, starting from 0.
  SNodeList*    pChildren;  // Element is SQueryPlan*
  char*         subSql;
  bool          ownsSubSql;  // Decoded plans own a copy; planner plans borrow AST SQL.
  SExplainInfo  explainInfo;
  void*         pPostPlan;
  bool          hasFederatedScan;  // true when plan contains at least one SCAN_TYPE_EXTERNAL node
  bool          hasScan;           // true when plan contains at least one local (vnode) scan
  SNodeList*    pCorrelationTemplates;  // Owned; not auto-started child jobs.
} SQueryPlan;

const char* dataOrderStr(EDataOrderLevel order);

// ---------------------------------------------------------------------------
// Federated query: Plan-to-SQL API
// Defined in source/libs/nodes/src/nodesRemotePlanToSQL.c
// Callers: Module F (Executor), Module B (Connector), EXPLAIN output.
//
// SNodesRemoteSQLCtx  -  optional callback context for resolving REMOTE_* nodes
//   during SQL generation.  Pass a populated struct from the Executor (which has
//   access to the sub-job context and qFetchRemoteNode).  Pass NULL from the
//   Connector and EXPLAIN  -  REMOTE_VALUE_LIST nodes will then cause the WHERE
//   clause to be omitted (best-effort, same behaviour as before).
// ---------------------------------------------------------------------------
typedef int32_t (*FResolveRemoteForSQL)(void* pCtx, int32_t subQIdx, SNode* pNode);
typedef struct SNodesRemoteSQLCtx {
  void*               pCtx;  // STaskSubJobCtx* from the executor task
  FResolveRemoteForSQL fp;   // qFetchRemoteNode
  // When true, column references in WHERE clauses are rendered as
  // "tableName"."colName" instead of just "colName".  Used when rendering
  // the body of a correlated EXISTS subquery that is pushed down to an
  // external source, where column prefixes are required for disambiguation.
  bool                includeTableName;
  // When true, column references are rendered as "tableAlias"."colName" using
  // the column's own tableAlias (the per-leg alias, e.g. s/r).  Set while
  // rendering a pushed-down JOIN (SELECT list, ON condition, WHERE) so that
  // same-named columns from the two legs are unambiguous.
  bool                useJoinLegAlias;
  // When true, SELECT-list output columns are aliased by their INTERNAL slot
  // name (aliasName, e.g. "expr_2") instead of the user-facing userAlias
  // (e.g. "count(s.voltage)").  Set while rendering a SUBQUERY body, because the
  // enclosing outer query references the subquery's output columns by their
  // internal slot name (the outer column's colName), not the user alias.
  bool                emitInternalAlias;
  // When true, SELECT-list raw column passthrough should preserve the column
  // reference name already exposed by the child query (colName) instead of
  // re-aliasing it with the current target's user-facing name. Used by UNION
  // branch wrappers, whose job is only to align branch schemas.
  bool                preserveOutputRefName;
  // When true, QUERY_NODE_COLUMN inside the current HAVING clause first tries
  // to resolve itself as a rewritten AGG output slot (for example the internal
  // alias of SUM(voltage)) and, on match, is re-rendered as the original
  // aggregate expression instead of a quoted column name.  This keeps remote
  // HAVING SQL valid for dialects that do not allow SELECT-list aliases there.
  bool                rewriteAggHavingColumn;
  // The current AGG node's pAggFuncs list.  Used only when
  // rewriteAggHavingColumn is true so the renderer can map a HAVING slot column
  // back to the corresponding aggregate expression.
  const SNodeList*    pHavingAggFuncs;
  // Current statement output namespace.  When rendering clauses above a
  // subquery / wrapper / derived-table FROM, plain column references with no
  // table qualifier must resolve against the SELECT-list outputs of that same
  // remote SQL statement, not against any base-table metadata.  The renderer
  // uses this list to normalize references like userAlias -> internal slot
  // aliasName consistently across WHERE / GROUP BY / HAVING / ORDER BY.
  const SNodeList*    pOutputSelectExprs;
  // True only while rendering the current statement's SELECT expression bodies.
  // In that context, column nodes must stay as the original expression inputs
  // and must not be rewritten as current-statement output-slot references.
  bool                renderingCurrentSelectExpr;
  // Current statement FROM child output namespace. When the current SELECT list
  // is rendered above a derived-table/subquery FROM, some projected pseudo
  // columns (for example _wend / _wduration over window aggregates) must be
  // synthesized from the child SELECT outputs rather than treated as raw
  // current-scope column references.
  const SNodeList*    pInputSelectExprs;
  // Current statement FROM child reference namespace aligned with
  // pInputSelectExprs. When the child emits internal aliases that differ from
  // the logical current-scope slot names, resolve current-scope column refs by
  // matching against this list first and then reading the aligned expression
  // from pInputSelectExprs.
  const SNodeList*    pInputRefExprs;
  // True when pInputSelectExprs comes from a real FROM child / derived-table
  // that is visible to the current SQL scope. False means the list only
  // represents merged same-scope child expressions and cannot be referenced by
  // alias in the current ORDER BY / GROUP BY / HAVING clauses.
  bool                inputSelectExprsFromChildSource;
  // Planner-provided remote timestamp type hints. Runtime SQL rendering cannot
  // access Catalog's SExtTableMeta after plan serialization, so type-sensitive
  // functions such as TO_UNIXTIMESTAMP use this remote-column -> timezone-class
  // mapping instead of pExtMeta.
  const SExtTimestampTypeHint* pTimestampTypeHints;
  int32_t             numTimestampTypeHints;
  // DS section5.2.6: client timezone name (e.g. "UTC", "Asia/Shanghai").
  // When non-NULL and non-empty, timestamp epoch values in WHERE conditions
  // are formatted to calendar strings using this timezone instead of the
  // server-side global timezone.  This ensures tz-naive external columns
  // (MySQL DATETIME, PG TIMESTAMP WITHOUT TIME ZONE) receive correctly
  // formatted filter values that match the client's interpretation.
  const char*         tzName;
  // Runtime-only output conversion controls. The executor enables these for
  // MySQL and PostgreSQL timestamp results after probing the connection's
  // timezone capability. They are not serialized as part of the physical plan.
  bool                convertTimestampOutputToEpoch;
  bool                timezoneConversionSupported;
  // Optional runtime-only half-open time range [skey, ekey). FederatedScan
  // supplies this for each VtableWindow batch; it is not persisted in pRemotePlan.
  const STimeWindow*  pRuntimeTimeRange;
} SNodesRemoteSQLCtx;

typedef enum {
  NODES_REMOTE_SELECT_SOURCE_UNKNOWN = 0,
  NODES_REMOTE_SELECT_SOURCE_PROJECT,
  NODES_REMOTE_SELECT_SOURCE_AGG,
  NODES_REMOTE_SELECT_SOURCE_PARTITION,
  NODES_REMOTE_SELECT_SOURCE_INDEF,
  NODES_REMOTE_SELECT_SOURCE_SCAN,
  NODES_REMOTE_SELECT_SOURCE_FROM_CHILD,
  NODES_REMOTE_SELECT_SOURCE_JOIN,
  NODES_REMOTE_SELECT_SOURCE_SETOP,
} ENodesRemoteSelectSource;

typedef struct SNodesRemoteSelectOutputs {
  SNodeList*                 pSelectExprs;
  SNodeList*                 pRefExprs;
  ENodesRemoteSelectSource   source;
  int32_t                    keyExprCount;
} SNodesRemoteSelectOutputs;

// nodesRemotePlanToSQL() - walk a Mode 1 outer SFederatedScanPhysiNode's
//   .pRemotePlan sub-tree and render the full SQL to send to the external source.
//   pRemotePlan  : the logic-plan tree (MUST NOT be NULL).
//   sourceType   : EExtSourceType value; the SQL dialect is selected internally.
//   pResolveCtx  : optional; when non-NULL, REMOTE_VALUE_LIST nodes are resolved
//                  via pResolveCtx->fp and emitted as IN (v1, v2, ...) in SQL.
//   ppSQL        : OUT  -  heap-allocated result string; caller must taosMemoryFree().
//
// The tree must be rooted at one of:
//   Project/Sort/Agg/Join/... logic nodes ending at an external ScanLogic leaf
//   or a directly renderable external ScanLogic root.
//
// nodesExprToExtSQL()  -  serialize a single expression subtree to a SQL fragment.
//   Returns TSDB_CODE_EXT_SYNTAX_UNSUPPORTED for unsupported expression types.
// ---------------------------------------------------------------------------
int32_t nodesCollectRemoteQuerySelectOutputs(const SLogicNode* pQueryRoot,
                                             SNodesRemoteSelectOutputs* pOutputs);
void nodesDestroyRemoteSelectOutputs(SNodesRemoteSelectOutputs* pOutputs);
// Borrowed Sort whose keys are emitted in the final remote SQL scope.
const SSortLogicNode* nodesGetRemoteQueryOutputSort(const SLogicNode* pRoot);
int32_t nodesRemotePlanToSQL(const SLogicNode* pRemotePlan, int8_t sourceType,
                             const SNodesRemoteSQLCtx* pResolveCtx,
                             char** ppSQL);
int32_t nodesExprToExtSQL(const SNode* pExpr, EExtSQLDialect dialect, char* buf, int32_t bufLen,
                          int32_t* pLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_PLANN_NODES_H_*/
