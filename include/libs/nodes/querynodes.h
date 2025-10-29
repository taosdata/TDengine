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

#ifndef _TD_QUERY_NODES_H_
#define _TD_QUERY_NODES_H_

#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "tvariant.h"
#include "ttypes.h"
#include "streamMsg.h"

#define TABLE_TOTAL_COL_NUM(pMeta) ((pMeta)->tableInfo.numOfColumns + (pMeta)->tableInfo.numOfTags)
#define TABLE_META_SIZE(pMeta) \
  (NULL == (pMeta) ? 0 : (sizeof(STableMeta) + TABLE_TOTAL_COL_NUM((pMeta)) * sizeof(SSchema) + (pMeta)->numOfColRefs * sizeof(SColRef)))
#define VGROUPS_INFO_SIZE(pInfo) \
  (NULL == (pInfo) ? 0 : (sizeof(SVgroupsInfo) + (pInfo)->numOfVgroups * sizeof(SVgroupInfo)))

typedef struct SAssociationNode {
  SNode** pPlace;
  SNode*  pAssociationNode;
} SAssociationNode;

typedef struct SRawExprNode {
  ENodeType nodeType;
  char*     p;
  uint32_t  n;
  SNode*    pNode;
  bool      isPseudoColumn;
} SRawExprNode;

typedef struct SExprNode {
  ENodeType type;
  SDataType resType;
  char      aliasName[TSDB_COL_NAME_LEN];
  char      userAlias[TSDB_COL_NAME_LEN];
  char      srcTable[TSDB_TABLE_NAME_LEN];
  SArray*   pAssociation;
  bool      asAlias;
  bool      asParam;
  bool      asPosition;
  bool      joinSrc;
  //bool      constValue;
  int32_t   projIdx;
  int32_t   relatedTo;
  int32_t   bindExprID;
} SExprNode;

typedef enum EColumnType {
  COLUMN_TYPE_COLUMN = 1,
  COLUMN_TYPE_TAG,
  COLUMN_TYPE_TBNAME,
  COLUMN_TYPE_WINDOW_START,
  COLUMN_TYPE_WINDOW_END,
  COLUMN_TYPE_WINDOW_DURATION,
  COLUMN_TYPE_GROUP_KEY,
  COLUMN_TYPE_IS_WINDOW_FILLED,
  COLUMN_TYPE_PLACE_HOLDER,
} EColumnType;

typedef struct SColumnNode {
  SExprNode   node;  // QUERY_NODE_COLUMN
  uint64_t    tableId;
  int8_t      tableType;
  col_id_t    colId;
  uint16_t    projIdx;  // the idx in project list, start from 1
  EColumnType colType;  // column or tag
  bool        hasIndex;
  bool        isPrimTs;
  char        dbName[TSDB_DB_NAME_LEN];
  char        tableName[TSDB_TABLE_NAME_LEN];
  char        tableAlias[TSDB_TABLE_NAME_LEN];
  char        colName[TSDB_COL_NAME_LEN];
  int16_t     dataBlockId;
  int16_t     slotId;
  int16_t     numOfPKs;
  bool        tableHasPk;
  bool        isPk;
  int32_t     projRefIdx;
  int32_t     resIdx;
  bool        hasDep;
  bool        hasRef;
  char        refDbName[TSDB_DB_NAME_LEN];
  char        refTableName[TSDB_TABLE_NAME_LEN];
  char        refColName[TSDB_COL_NAME_LEN];
} SColumnNode;

typedef struct SColumnRefNode {
  ENodeType type;
  char      colName[TSDB_COL_NAME_LEN];
  char      refDbName[TSDB_DB_NAME_LEN];
  char      refTableName[TSDB_TABLE_NAME_LEN];
  char      refColName[TSDB_COL_NAME_LEN];
} SColumnRefNode;

typedef struct STargetNode {
  ENodeType type;
  int16_t   dataBlockId;
  int16_t   slotId;
  SNode*    pExpr;
} STargetNode;

#define VALUE_FLAG_IS_DURATION    (1 << 0)
#define VALUE_FLAG_IS_TIME_OFFSET (1 << 1)

#define IS_DURATION_VAL(_flag)    ((_flag)&VALUE_FLAG_IS_DURATION)
#define IS_TIME_OFFSET_VAL(_flag) ((_flag)&VALUE_FLAG_IS_TIME_OFFSET)

typedef struct SValueNode {
  SExprNode  node;  // QUERY_NODE_VALUE
  char*      literal;
  int32_t    flag;
  bool       translate;
  bool       notReserved;
  bool       isNull;
  int16_t    placeholderNo;
  union {
    bool     b;
    int64_t  i;
    uint64_t u;
    double   d;
    char*    p;
  } datum;
  int64_t    typeData;
  int8_t     unit;
  timezone_t tz;
  void*      charsetCxt;
} SValueNode;

typedef struct SLeftValueNode {
  ENodeType type;
} SLeftValueNode;

typedef enum EHintOption {
  HINT_NO_BATCH_SCAN = 1,
  HINT_BATCH_SCAN,
  HINT_SORT_FOR_GROUP,
  HINT_PARTITION_FIRST,
  HINT_PARA_TABLES_SORT,
  HINT_SMALLDATA_TS_SORT,
  HINT_HASH_JOIN,
  HINT_SKIP_TSMA,
} EHintOption;

typedef struct SHintNode {
  ENodeType   type;
  EHintOption option;
  void*       value;
} SHintNode;

typedef struct SOperatorNode {
  SExprNode     node;  // QUERY_NODE_OPERATOR
  EOperatorType opType;
  SNode*        pLeft;
  SNode*        pRight;
  timezone_t    tz;
  void*         charsetCxt;
} SOperatorNode;

typedef struct SLogicConditionNode {
  SExprNode           node;  // QUERY_NODE_LOGIC_CONDITION
  ELogicConditionType condType;
  SNodeList*          pParameterList;
} SLogicConditionNode;

typedef struct SNodeListNode {
  SExprNode  node;  // QUERY_NODE_NODE_LIST
  SNodeList* pNodeList;
} SNodeListNode;

typedef enum ETrimType {
  TRIM_TYPE_LEADING = 1,
  TRIM_TYPE_TRAILING,
  TRIM_TYPE_BOTH,
} ETrimType;

typedef struct SFunctionNode {
  SExprNode  node;  // QUERY_NODE_FUNCTION
  char       functionName[TSDB_FUNC_NAME_LEN];
  int32_t    funcId;
  int32_t    funcType;
  SNodeList* pParameterList;
  int32_t    udfBufSize;
  bool       hasPk;
  int32_t    pkBytes;
  bool       hasOriginalFunc;
  int32_t    originalFuncId;
  ETrimType  trimType;
  bool       dual; // whether select stmt without from stmt, true for without.
  timezone_t tz;
  void      *charsetCxt;
  const struct SFunctionNode* pSrcFuncRef;
  SDataType  srcFuncInputType;
} SFunctionNode;

typedef struct STableNode {
  SExprNode node;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  char      tableAlias[TSDB_TABLE_NAME_LEN];
  uint8_t   precision;
  bool      singleTable;
  bool      inJoin;
} STableNode;

typedef struct SStreamNode {
  SExprNode node;
  char      dbName[TSDB_DB_NAME_LEN];
  char      streamName[TSDB_STREAM_NAME_LEN];
} SStreamNode;

struct STableMeta;

typedef struct STsmaTargetCTbInfo {
  char     tableName[TSDB_TABLE_NAME_LEN];  // child table or normal table name
  uint64_t uid;
} STsmaTargetTbInfo;

typedef struct SRealTableNode {
  STableNode         table;  // QUERY_NODE_REAL_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  char               qualDbName[TSDB_DB_NAME_LEN];  // SHOW qualDbName.TABLES
  double             ratio;
  SArray*            pSmaIndexes;
  int8_t             cacheLastMode;
  int8_t             stbRewrite;
  SArray*            pTsmas;
  SArray*            tsmaTargetTbVgInfo;  // SArray<SVgroupsInfo*>, used for child table or normal table only
  SArray*            tsmaTargetTbInfo;    // SArray<STsmaTargetTbInfo>, used for child table or normal table only
  EStreamPlaceholder placeholderType;
  bool               asSingleTable; // only used in stream calc query
} SRealTableNode;

typedef struct STempTableNode {
  STableNode table;  // QUERY_NODE_TEMP_TABLE
  SNode*     pSubquery;
} STempTableNode;

typedef struct SPlaceHolderTableNode {
  STableNode         table;  // QUERY_NODE_PLACE_HOLDER_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  EStreamPlaceholder placeholderType;
} SPlaceHolderTableNode;

typedef struct SVirtualTableNode {
  STableNode         table;  // QUERY_NODE_VIRTUAL_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  SNodeList*         refTables;
} SVirtualTableNode;

typedef struct SViewNode {
  STableNode         table;  // QUERY_NODE_REAL_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  char               qualDbName[TSDB_DB_NAME_LEN];  // SHOW qualDbName.TABLES
  double             ratio;
  SArray*            pSmaIndexes;
  int8_t             cacheLastMode;
} SViewNode;

#define JOIN_JLIMIT_MAX_VALUE 1024

#define IS_INNER_NONE_JOIN(_type, _stype) ((_type) == JOIN_TYPE_INNER && (_stype) == JOIN_STYPE_NONE)
#define IS_SEMI_JOIN(_stype)              ((_stype) == JOIN_STYPE_SEMI)
#define IS_WINDOW_JOIN(_stype)            ((_stype) == JOIN_STYPE_WIN)
#define IS_ASOF_JOIN(_stype)              ((_stype) == JOIN_STYPE_ASOF)

typedef enum EJoinType {
  JOIN_TYPE_INNER = 0,
  JOIN_TYPE_LEFT,
  JOIN_TYPE_RIGHT,
  JOIN_TYPE_FULL,
  JOIN_TYPE_MAX_VALUE
} EJoinType;

typedef enum EJoinSubType {
  JOIN_STYPE_NONE = 0,
  JOIN_STYPE_OUTER,
  JOIN_STYPE_SEMI,
  JOIN_STYPE_ANTI,
  JOIN_STYPE_ASOF,
  JOIN_STYPE_WIN,
  JOIN_STYPE_MAX_VALUE
} EJoinSubType;

typedef enum EJoinAlgorithm {
  JOIN_ALGO_UNKNOWN = 0,
  JOIN_ALGO_MERGE,
  JOIN_ALGO_HASH,
} EJoinAlgorithm;

typedef enum EDynQueryType {
  DYN_QTYPE_STB_HASH = 1,
  DYN_QTYPE_VTB_SCAN,
} EDynQueryType;

typedef struct SJoinTableNode {
  STableNode   table;  // QUERY_NODE_JOIN_TABLE
  EJoinType    joinType;
  EJoinSubType subType;
  SNode*       pWindowOffset;
  SNode*       pJLimit;
  SNode*       addPrimCond;
  bool         hasSubQuery;
  bool         isLowLevelJoin;
  bool         leftNoOrderedSubQuery;
  bool         rightNoOrderedSubQuery;
  //bool         condAlwaysTrue;
  //bool         condAlwaysFalse;
  SNode*       pLeft;
  SNode*       pRight;
  SNode*       pOnCond;
} SJoinTableNode;

typedef enum EGroupingSetType { GP_TYPE_NORMAL = 1 } EGroupingSetType;

typedef struct SGroupingSetNode {
  ENodeType        type;  // QUERY_NODE_GROUPING_SET
  EGroupingSetType groupingSetType;
  SNodeList*       pParameterList;
} SGroupingSetNode;

typedef enum EOrder { ORDER_ASC = 1, ORDER_DESC } EOrder;

typedef enum ENullOrder { NULL_ORDER_DEFAULT = 1, NULL_ORDER_FIRST, NULL_ORDER_LAST } ENullOrder;

typedef struct SOrderByExprNode {
  ENodeType  type;  // QUERY_NODE_ORDER_BY_EXPR
  SNode*     pExpr;
  EOrder     order;
  ENullOrder nullOrder;
} SOrderByExprNode;

typedef struct SLimitNode {
  ENodeType   type;  // QUERY_NODE_LIMIT
  SValueNode* limit;
  SValueNode* offset;
} SLimitNode;

typedef enum EStateWinExtendOption {
  STATE_WIN_EXTEND_OPTION_DEFAULT  = 0,
  STATE_WIN_EXTEND_OPTION_BACKWARD = 1,
  STATE_WIN_EXTEND_OPTION_FORWARD  = 2,
} EStateWinExtendOption;

typedef struct SStateWindowNode {
  ENodeType type;  // QUERY_NODE_STATE_WINDOW
  SNode*    pCol;  // timestamp primary key
  SNode*    pExpr;
  SNode*    pTrueForLimit;
  SNode*    pExtend;  // SValueNode
  SNode*    pZeroth;  // SValueNode
} SStateWindowNode;

typedef struct SSessionWindowNode {
  ENodeType    type;  // QUERY_NODE_SESSION_WINDOW
  SColumnNode* pCol;  // timestamp primary key
  SValueNode*  pGap;  // gap between two session window(in microseconds)
} SSessionWindowNode;

typedef struct SIntervalWindowNode {
  ENodeType   type;       // QUERY_NODE_INTERVAL_WINDOW
  SNode*      pCol;       // timestamp primary key
  SNode*      pInterval;  // SValueNode
  SNode*      pOffset;    // SValueNode
  SNode*      pSliding;   // SValueNode
  SNode*      pSOffset;   // SValueNode
  SNode*      pFill;
  STimeWindow timeRange;
  void*       timezone;
} SIntervalWindowNode;

typedef struct SEventWindowNode {
  ENodeType type;  // QUERY_NODE_EVENT_WINDOW
  SNode*    pCol;  // timestamp primary key
  SNode*    pStartCond;
  SNode*    pEndCond;
  SNode*    pTrueForLimit;
} SEventWindowNode;

typedef struct {
  ENodeType  type;  // QUERY_NODE_COUNT_WINDOW_PARAM
  int64_t    count;
  int64_t    sliding;
  SNodeList* pColList;
} SCountWindowArgs;

typedef struct SCountWindowNode {
  ENodeType  type;  // QUERY_NODE_COUNT_WINDOW
  SNode*     pCol;  // timestamp primary key
  int64_t    windowCount;
  int64_t    windowSliding;
  SNodeList* pColList;  // SColumnNodeList
} SCountWindowNode;

typedef struct SAnomalyWindowNode {
  ENodeType type;  // QUERY_NODE_ANOMALY_WINDOW
  SNode*    pCol;  // timestamp primary key
  SNode*    pExpr;
  char      anomalyOpt[TSDB_ANALYTIC_ALGO_OPTION_LEN];
} SAnomalyWindowNode;

typedef struct SSlidingWindowNode {
  ENodeType type;
  SNode*    pSlidingVal;
  SNode*    pOffset;
} SSlidingWindowNode;

typedef struct SExternalWindowNode {
  ENodeType   type;       // QUERY_NODE_EXTERNAL_WINDOW
  SNodeList*  pProjectionList;
  SNodeList*  pAggFuncList;
  STimeWindow timeRange;
  SNode*      pTimeRange;
  void*       timezone;
} SExternalWindowNode;

typedef struct SStreamTriggerOptions {
  ENodeType type; // QUERY_NODE_STREAM_TRIGGER_OPTIONS
  SNode*    pPreFilter;
  SNode*    pWaterMark;
  SNode*    pMaxDelay;
  SNode*    pExpiredTime;
  SNode*    pFillHisStartTime;
  int64_t   pEventType;
  int64_t   fillHistoryStartTime;
  bool      ignoreDisorder;
  bool      deleteRecalc;
  bool      deleteOutputTable;
  bool      fillHistory;
  bool      fillHistoryFirst;
  bool      calcNotifyOnly;
  bool      lowLatencyCalc;
  bool      forceOutput;
  bool      ignoreNoDataTrigger;
} SStreamTriggerOptions;

typedef struct SStreamTriggerNode {
  ENodeType   type;
  SNode*      pTriggerWindow; // S.*WindowNode
  SNode*      pTrigerTable;
  SNode*      pOptions; // SStreamTriggerOptions
  SNode*      pNotify; // SStreamNotifyOptions
  SNodeList*  pPartitionList;
} SStreamTriggerNode;

typedef struct SStreamOutTableNode {
  ENodeType             type;
  SNode*                pOutTable; // STableNode
  SNode*                pSubtable;
  SNodeList*            pTags; // SStreamTagDefNode
  SNodeList*            pCols; // SColumnDefNode
} SStreamOutTableNode;

typedef struct SStreamCalcRangeNode {
  ENodeType             type;
  bool                  calcAll;
  SNode*                pStart;
  SNode*                pEnd;
} SStreamCalcRangeNode;

typedef struct SStreamTagDefNode {
  ENodeType type;
  char      tagName[TSDB_COL_NAME_LEN];
  SDataType dataType;
  SNode*    pTagExpr;
} SStreamTagDefNode;

typedef struct SPeriodWindowNode {
  ENodeType type;  // QUERY_NODE_PERIOD_WINDOW
  SNode*    pPeroid;
  SNode*    pOffset;
} SPeriodWindowNode;

typedef enum EFillMode {
  FILL_MODE_NONE = 1,
  FILL_MODE_VALUE,
  FILL_MODE_VALUE_F,
  FILL_MODE_PREV,
  FILL_MODE_NULL,
  FILL_MODE_NULL_F,
  FILL_MODE_LINEAR,
  FILL_MODE_NEXT,
  FILL_MODE_NEAR,
} EFillMode;

typedef enum ETimeLineMode {
  TIME_LINE_NONE = 1,
  TIME_LINE_BLOCK,
  TIME_LINE_MULTI,
  TIME_LINE_GLOBAL,
} ETimeLineMode;

typedef enum EShowKind {
  SHOW_KIND_ALL = 1,
  SHOW_KIND_TABLES_NORMAL,
  SHOW_KIND_TABLES_CHILD,
  SHOW_KIND_TABLES_VIRTUAL,
  SHOW_KIND_DATABASES_USER,
  SHOW_KIND_DATABASES_SYSTEM
} EShowKind;

typedef struct SFillNode {
  ENodeType   type;  // QUERY_NODE_FILL
  EFillMode   mode;
  SNode*      pValues;    // SNodeListNode
  SNode*      pWStartTs;  // _wstart pseudo column
  STimeWindow timeRange;
  SNode*      pTimeRange; // STimeRangeNode for create stream
} SFillNode;

typedef struct SWhenThenNode {
  SExprNode node;  // QUERY_NODE_WHEN_THEN
  SNode*    pWhen;
  SNode*    pThen;
} SWhenThenNode;

typedef struct SCaseWhenNode {
  SExprNode  node;  // QUERY_NODE_CASE_WHEN
  SNode*     pCase;
  SNode*     pElse;
  SNodeList* pWhenThenList;
  timezone_t tz;
  void*      charsetCxt;
} SCaseWhenNode;

typedef struct SWindowOffsetNode {
  ENodeType type;          // QUERY_NODE_WINDOW_OFFSET
  SNode*    pStartOffset;  // SValueNode
  SNode*    pEndOffset;    // SValueNode
} SWindowOffsetNode;

typedef struct SRangeAroundNode {
  ENodeType type;
  SNode*    pRange;
  SNode*    pInterval;
} SRangeAroundNode;

typedef struct STimeRangeNode {
  ENodeType type; // QUERY_NODE_TIME_RANGE
  SNode*    pStart;
  SNode*    pEnd;
  bool      needCalc;
} STimeRangeNode;

typedef struct SExtWinTimeWindow {
  STimeWindow tw;
  int32_t     winOutIdx;
} SExtWinTimeWindow;

typedef struct SSelectStmt {
  ENodeType       type;  // QUERY_NODE_SELECT_STMT
  bool            isDistinct;
  STimeWindow     timeRange;
  SNode*          pTimeRange; // STimeRangeNode for create stream
  SNodeList*      pProjectionList;
  SNodeList*      pProjectionBindList;
  SNode*          pFromTable;
  SNode*          pWhere;
  SNodeList*      pPartitionByList;
  SNode*          pWindow;
  SNodeList*      pGroupByList;  // SGroupingSetNode
  SNode*          pHaving;
  SNode*          pRange;
  SNode*          pRangeAround;
  SNode*          pEvery;
  SNode*          pFill;
  SNodeList*      pOrderByList;  // SOrderByExprNode
  SLimitNode*     pLimit;
  SLimitNode*     pSlimit;
  SNodeList*      pHint;
  char            stmtName[TSDB_TABLE_NAME_LEN];
  uint8_t         precision;
  int32_t         selectFuncNum;
  int32_t         returnRows;  // EFuncReturnRows
  ETimeLineMode   timeLineCurMode;
  ETimeLineMode   timeLineResMode;
  int32_t         lastProcessByRowFuncId;
  bool            timeLineFromOrderBy;
  bool            isEmptyResult;
  bool            isSubquery;
  bool            hasAggFuncs;
  bool            hasRepeatScanFuncs;
  bool            hasIndefiniteRowsFunc;
  bool            hasMultiRowsFunc;
  bool            hasSelectFunc;
  bool            hasSelectValFunc;
  bool            hasOtherVectorFunc;
  bool            hasUniqueFunc;
  bool            hasTailFunc;
  bool            hasInterpFunc;
  bool            hasInterpPseudoColFunc;
  bool            hasForecastFunc;
  bool            hasForecastPseudoColFunc;
  bool            hasGenericAnalysisFunc;
  bool            hasLastRowFunc;
  bool            hasLastFunc;
  bool            hasTimeLineFunc;
  bool            hasCountFunc;
  bool            hasUdaf;
  bool            hasStateKey;
  bool            hasTwaOrElapsedFunc;
  bool            onlyHasKeepOrderFunc;
  bool            groupSort;
  bool            tagScan;
  bool            joinContains;
  bool            mixSysTableAndActualTable;
} SSelectStmt;

typedef enum ESetOperatorType { SET_OP_TYPE_UNION_ALL = 1, SET_OP_TYPE_UNION } ESetOperatorType;

typedef struct SSetOperator {
  ENodeType        type;  // QUERY_NODE_SET_OPERATOR
  ESetOperatorType opType;
  SNodeList*       pProjectionList;
  SNode*           pLeft;
  SNode*           pRight;
  SNodeList*       pOrderByList;  // SOrderByExprNode
  SNode*           pLimit;
  char             stmtName[TSDB_TABLE_NAME_LEN];
  uint8_t          precision;
  ETimeLineMode    timeLineResMode;
  bool             timeLineFromOrderBy;
  bool             joinContains;
} SSetOperator;

typedef enum ESqlClause {
  SQL_CLAUSE_FROM = 1,
  SQL_CLAUSE_WHERE,
  SQL_CLAUSE_PARTITION_BY,
  SQL_CLAUSE_WINDOW,
  SQL_CLAUSE_FILL,
  SQL_CLAUSE_GROUP_BY,
  SQL_CLAUSE_HAVING,
  SQL_CLAUSE_DISTINCT,
  SQL_CLAUSE_SELECT,
  SQL_CLAUSE_ORDER_BY
} ESqlClause;

typedef struct SDeleteStmt {
  ENodeType   type;        // QUERY_NODE_DELETE_STMT
  SNode*      pFromTable;  // FROM clause
  SNode*      pWhere;      // WHERE clause
  SNode*      pCountFunc;  // count the number of rows affected
  SNode*      pFirstFunc;  // the start timestamp when the data was actually deleted
  SNode*      pLastFunc;   // the end timestamp when the data was actually deleted
  SNode*      pTagCond;    // pWhere divided into pTagCond and timeRange
  STimeWindow timeRange;
  uint8_t     precision;
  bool        deleteZeroRows;
} SDeleteStmt;

typedef struct SInsertStmt {
  ENodeType  type;  // QUERY_NODE_INSERT_STMT
  SNode*     pTable;
  SNodeList* pCols;
  SNode*     pQuery;
  uint8_t    precision;
} SInsertStmt;

typedef struct SVgDataBlocks {
  SVgroupInfo vg;
  int32_t     numOfTables;  // number of tables in current submit block
  uint32_t    size;
  void*       pData;  // SSubmitReq + SSubmitBlk + ...
} SVgDataBlocks;

typedef void (*FFreeTableBlockHash)(SHashObj*);
typedef void (*FFreeVgourpBlockArray)(SArray*);
struct SStbRowsDataContext;
typedef void (*FFreeStbRowsDataContext)(struct SStbRowsDataContext*);
struct SCreateTbInfo;
struct SParseFileContext;
typedef void (*FDestroyParseFileContext)(struct SParseFileContext**);

typedef struct SVnodeModifyOpStmt {
  ENodeType             nodeType;
  ENodeType             sqlNodeType;
  SArray*               pDataBlocks;  // data block for each vgroup, SArray<SVgDataBlocks*>.
  uint32_t              insertType;   // insert data from [file|sql statement| bound statement]
  const char*           pSql;         // current sql statement position
  int32_t               totalRowsNum;
  int32_t               totalTbNum;
  SName                 targetTableName;
  SName                 usingTableName;
  const char*           pBoundCols;
  struct STableMeta*    pTableMeta;
  SNode*                pTagCond;
  SArray*               pTableTag;
  SHashObj*             pVgroupsHashObj;     // SHashObj<vgId, SVgInfo>
  SHashObj*             pTableBlockHashObj;  // SHashObj<tuid, STableDataCxt*>
  SHashObj*             pSubTableHashObj;    // SHashObj<table_name, STableMeta*>
  SHashObj*             pSuperTableHashObj;  // SHashObj<table_name, STableMeta*>
  SHashObj*             pTableNameHashObj;   // set of table names for refreshing meta, sync mode
  SHashObj*             pDbFNameHashObj;     // set of db names for refreshing meta, sync mode
  SHashObj*             pTableCxtHashObj;    // temp SHashObj<tuid, STableDataCxt*> for single request
  SArray*               pVgDataBlocks;       // SArray<SVgroupDataCxt*>
  SVCreateTbReq*        pCreateTblReq;
  TdFilePtr             fp;
  FFreeTableBlockHash   freeHashFunc;
  FFreeVgourpBlockArray freeArrayFunc;
  bool                  usingTableProcessing;
  bool                  fileProcessing;

  bool                        stbSyntax;
  struct SStbRowsDataContext* pStbRowsCxt;
  FFreeStbRowsDataContext     freeStbRowsCxtFunc;

  struct SCreateTbInfo*     pCreateTbInfo;
  struct SParseFileContext* pParFileCxt;
  FDestroyParseFileContext  destroyParseFileCxt;

  // CSV parser for resuming batch processing
  struct SCsvParser* pCsvParser;
} SVnodeModifyOpStmt;

typedef struct SExplainOptions {
  ENodeType type;
  bool      verbose;
  double    ratio;
} SExplainOptions;

typedef struct SExplainStmt {
  ENodeType        type;
  bool             analyze;
  SExplainOptions* pOptions;
  SNode*           pQuery;
} SExplainStmt;

typedef struct SCmdMsgInfo {
  int16_t msgType;
  SEpSet  epSet;
  void*   pMsg;
  int32_t msgLen;
} SCmdMsgInfo;

typedef enum EQueryExecMode {
  QUERY_EXEC_MODE_LOCAL = 1,
  QUERY_EXEC_MODE_RPC,
  QUERY_EXEC_MODE_SCHEDULE,
  QUERY_EXEC_MODE_EMPTY_RESULT
} EQueryExecMode;

typedef enum EQueryExecStage {
  QUERY_EXEC_STAGE_PARSE = 1,
  QUERY_EXEC_STAGE_ANALYSE,
  QUERY_EXEC_STAGE_SCHEDULE,
  QUERY_EXEC_STAGE_END
} EQueryExecStage;

typedef struct SQuery {
  ENodeType       type;
  EQueryExecStage execStage;
  EQueryExecMode  execMode;
  int32_t         msgType;
  int32_t         numOfResCols;
  int32_t         placeholderNum;
  int8_t          precision;
  bool            haveResultSet;
  bool            showRewrite;
  bool            stableQuery;
  SNode*          pPrevRoot;
  SNode*          pRoot;
  SNode*          pPostRoot;
  SSchema*        pResSchema;
  SCmdMsgInfo*    pCmdMsg;
  SArray*         pTargetTableList;
  SArray*         pTableList;
  SArray*         pDbList;
  SArray*         pPlaceholderValues;
  SNode*          pPrepareRoot;
  SExtSchema*     pResExtSchema;
} SQuery;

void nodesWalkSelectStmtImpl(SSelectStmt* pSelect, ESqlClause clause, FNodeWalker walker, void* pContext);
void nodesWalkSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeWalker walker, void* pContext);
void nodesRewriteSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeRewriter rewriter, void* pContext);

typedef enum ECollectColType { COLLECT_COL_TYPE_COL = 1, COLLECT_COL_TYPE_TAG, COLLECT_COL_TYPE_ALL } ECollectColType;
int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, ECollectColType type,
                            SNodeList** pCols);
int32_t nodesCollectColumnsExt(SSelectStmt* pSelect, ESqlClause clause, SSHashObj* pMultiTableAlias,
                               ECollectColType type, SNodeList** pCols);
int32_t nodesCollectColumnsFromNode(SNode* node, const char* pTableAlias, ECollectColType type, SNodeList** pCols);

typedef bool (*FFuncClassifier)(int32_t funcId);
int32_t nodesCollectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier,
                          SNodeList** pFuncs);
int32_t nodesCollectSelectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier,
                                SNodeList* pFuncs);

int32_t nodesCollectSpecialNodes(SSelectStmt* pSelect, ESqlClause clause, ENodeType type, SNodeList** pNodes);

bool nodesIsExprNode(const SNode* pNode);

bool nodesIsUnaryOp(const SOperatorNode* pOp);
bool nodesIsArithmeticOp(const SOperatorNode* pOp);
bool nodesIsBasicArithmeticOp(const SOperatorNode* pOp);
bool nodesIsComparisonOp(const SOperatorNode* pOp);
bool nodesIsJsonOp(const SOperatorNode* pOp);
bool nodesIsRegularOp(const SOperatorNode* pOp);
bool nodesIsMatchRegularOp(const SOperatorNode* pOp);
bool nodesIsBitwiseOp(const SOperatorNode* pOp);

bool nodesExprHasColumn(SNode* pNode);
bool nodesExprsHasColumn(SNodeList* pList);

void*   nodesGetValueFromNode(SValueNode* pNode);
int32_t nodesSetValueNodeValue(SValueNode* pNode, void* value);
char*   nodesGetStrValueFromNode(SValueNode* pNode);
int32_t nodesValueNodeToVariant(const SValueNode* pNode, SVariant* pVal);
int32_t nodesMakeValueNodeFromString(char* literal, SValueNode** ppValNode);
int32_t nodesMakeDurationValueNodeFromString(char* literal, SValueNode** ppValNode);
int32_t nodesMakeValueNodeFromBool(bool b, SValueNode** ppValNode);
int32_t nodesMakeValueNodeFromInt32(int32_t value, SNode** ppNode);
int32_t nodesMakeValueNodeFromInt64(int64_t value, SNode** ppNode);
int32_t nodesMakeValueNodeFromTimestamp(int64_t value, SNode** ppNode);

    char*   nodesGetFillModeString(EFillMode mode);
int32_t nodesMergeConds(SNode** pDst, SNodeList** pSrc);

const char* operatorTypeStr(EOperatorType type);
const char* logicConditionTypeStr(ELogicConditionType type);

bool nodesIsStar(SNode* pNode);
bool nodesIsTableStar(SNode* pNode);

char*   getJoinTypeString(EJoinType type);
char*   getJoinSTypeString(EJoinSubType type);
char*   getFullJoinTypeString(EJoinType type, EJoinSubType stype);
int32_t mergeJoinConds(SNode** ppDst, SNode** ppSrc);

void rewriteExprAliasName(SExprNode* pNode, int64_t num);
bool isRelatedToOtherExpr(SExprNode* pExpr);
bool nodesContainsColumn(SNode* pNode);
int32_t nodesMergeNode(SNode** pCond, SNode** pAdditionalCond);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_NODES_H_*/
