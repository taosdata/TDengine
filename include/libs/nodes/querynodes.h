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

#ifdef __cplusplus
extern "C" {
#endif

#include "nodes.h"
#include "tmsg.h"
#include "tvariant.h"

#define TABLE_TOTAL_COL_NUM(pMeta) ((pMeta)->tableInfo.numOfColumns + (pMeta)->tableInfo.numOfTags)
#define TABLE_META_SIZE(pMeta) \
  (NULL == (pMeta) ? 0 : (sizeof(STableMeta) + TABLE_TOTAL_COL_NUM((pMeta)) * sizeof(SSchema)))
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
  bool isPseudoColumn;
} SRawExprNode;

typedef struct SDataType {
  uint8_t type;
  uint8_t precision;
  uint8_t scale;
  int32_t bytes;
} SDataType;

typedef struct SExprNode {
  ENodeType type;
  SDataType resType;
  char      aliasName[TSDB_COL_NAME_LEN];
  char      userAlias[TSDB_COL_NAME_LEN];
  SArray*   pAssociation;
  bool      orderAlias;
  bool      asAlias;
  bool      asParam;
} SExprNode;

typedef enum EColumnType {
  COLUMN_TYPE_COLUMN = 1,
  COLUMN_TYPE_TAG,
  COLUMN_TYPE_TBNAME,
  COLUMN_TYPE_WINDOW_START,
  COLUMN_TYPE_WINDOW_END,
  COLUMN_TYPE_WINDOW_DURATION,
  COLUMN_TYPE_GROUP_KEY
} EColumnType;

typedef struct SColumnNode {
  SExprNode   node;  // QUERY_NODE_COLUMN
  uint64_t    tableId;
  int8_t      tableType;
  col_id_t    colId;
  uint16_t    projIdx;  // the idx in project list, start from 1
  EColumnType colType;  // column or tag
  bool        hasIndex;
  char        dbName[TSDB_DB_NAME_LEN];
  char        tableName[TSDB_TABLE_NAME_LEN];
  char        tableAlias[TSDB_TABLE_NAME_LEN];
  char        colName[TSDB_COL_NAME_LEN];
  int16_t     dataBlockId;
  int16_t     slotId;
} SColumnNode;

typedef struct SColumnRefNode {
  ENodeType type;
  SDataType resType;
  char      colName[TSDB_COL_NAME_LEN];
} SColumnRefNode;

typedef struct STargetNode {
  ENodeType type;
  int16_t   dataBlockId;
  int16_t   slotId;
  SNode*    pExpr;
} STargetNode;

typedef struct SValueNode {
  SExprNode node;  // QUERY_NODE_VALUE
  char*     literal;
  bool      isDuration;
  bool      translate;
  bool      notReserved;
  bool      isNull;
  int16_t   placeholderNo;
  union {
    bool     b;
    int64_t  i;
    uint64_t u;
    double   d;
    char*    p;
  } datum;
  int64_t typeData;
  int8_t  unit;
} SValueNode;

typedef struct SLeftValueNode {
  ENodeType type;
} SLeftValueNode;

typedef enum EHintOption {
  HINT_NO_BATCH_SCAN = 1,
  HINT_BATCH_SCAN,
  HINT_SORT_FOR_GROUP,
  HINT_PARTITION_FIRST,
  HINT_PARA_TABLES_SORT
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

typedef struct SFunctionNode {
  SExprNode  node;  // QUERY_NODE_FUNCTION
  char       functionName[TSDB_FUNC_NAME_LEN];
  int32_t    funcId;
  int32_t    funcType;
  SNodeList* pParameterList;
  int32_t    udfBufSize;
} SFunctionNode;

typedef struct STableNode {
  SExprNode node;
  char      dbName[TSDB_DB_NAME_LEN];
  char      tableName[TSDB_TABLE_NAME_LEN];
  char      tableAlias[TSDB_TABLE_NAME_LEN];
  uint8_t   precision;
  bool      singleTable;
} STableNode;

struct STableMeta;

typedef struct SRealTableNode {
  STableNode         table;  // QUERY_NODE_REAL_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  char               qualDbName[TSDB_DB_NAME_LEN];  // SHOW qualDbName.TABLES
  double             ratio;
  SArray*            pSmaIndexes;
  int8_t             cacheLastMode;
} SRealTableNode;

typedef struct STempTableNode {
  STableNode table;  // QUERY_NODE_TEMP_TABLE
  SNode*     pSubquery;
} STempTableNode;

typedef struct SViewNode {
  STableNode         table;  // QUERY_NODE_REAL_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo*      pVgroupList;
  char               qualDbName[TSDB_DB_NAME_LEN];  // SHOW qualDbName.TABLES
  double             ratio;
  SArray*            pSmaIndexes;
  int8_t             cacheLastMode;
} SViewNode;

typedef enum EJoinType { 
  JOIN_TYPE_INNER = 1,
  JOIN_TYPE_LEFT,
  JOIN_TYPE_RIGHT,
} EJoinType;

typedef enum EJoinAlgorithm { 
  JOIN_ALGO_UNKNOWN = 0,
  JOIN_ALGO_MERGE,
  JOIN_ALGO_HASH,
} EJoinAlgorithm;

typedef enum EDynQueryType {
  DYN_QTYPE_STB_HASH = 1,
} EDynQueryType;

typedef struct SJoinTableNode {
  STableNode table;  // QUERY_NODE_JOIN_TABLE
  EJoinType  joinType;
  bool       hasSubQuery;
  bool       isLowLevelJoin;
  SNode*     pLeft;
  SNode*     pRight;
  SNode*     pOnCond;
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
  ENodeType type;  // QUERY_NODE_LIMIT
  int64_t   limit;
  int64_t   offset;
} SLimitNode;

typedef struct SStateWindowNode {
  ENodeType type;  // QUERY_NODE_STATE_WINDOW
  SNode*    pCol;  // timestamp primary key
  SNode*    pExpr;
} SStateWindowNode;

typedef struct SSessionWindowNode {
  ENodeType    type;  // QUERY_NODE_SESSION_WINDOW
  SColumnNode* pCol;  // timestamp primary key
  SValueNode*  pGap;  // gap between two session window(in microseconds)
} SSessionWindowNode;

typedef struct SIntervalWindowNode {
  ENodeType type;       // QUERY_NODE_INTERVAL_WINDOW
  SNode*    pCol;       // timestamp primary key
  SNode*    pInterval;  // SValueNode
  SNode*    pOffset;    // SValueNode
  SNode*    pSliding;   // SValueNode
  SNode*    pFill;
} SIntervalWindowNode;

typedef struct SEventWindowNode {
  ENodeType type;  // QUERY_NODE_EVENT_WINDOW
  SNode*    pCol;  // timestamp primary key
  SNode*    pStartCond;
  SNode*    pEndCond;
} SEventWindowNode;

typedef struct SCountWindowNode {
  ENodeType type;  // QUERY_NODE_EVENT_WINDOW
  SNode*    pCol;  // timestamp primary key
  int64_t   windowCount;
  int64_t   windowSliding;
} SCountWindowNode;

typedef enum EFillMode {
  FILL_MODE_NONE = 1,
  FILL_MODE_VALUE,
  FILL_MODE_VALUE_F,
  FILL_MODE_PREV,
  FILL_MODE_NULL,
  FILL_MODE_NULL_F,
  FILL_MODE_LINEAR,
  FILL_MODE_NEXT
} EFillMode;

typedef enum ETimeLineMode {
  TIME_LINE_NONE = 1,
  TIME_LINE_MULTI,
  TIME_LINE_GLOBAL,
} ETimeLineMode;

typedef enum EShowKind {
  SHOW_KIND_ALL = 1,
  SHOW_KIND_TABLES_NORMAL,
  SHOW_KIND_TABLES_CHILD,
  SHOW_KIND_DATABASES_USER,
  SHOW_KIND_DATABASES_SYSTEM
} EShowKind;

typedef struct SFillNode {
  ENodeType   type;  // QUERY_NODE_FILL
  EFillMode   mode;
  SNode*      pValues;    // SNodeListNode
  SNode*      pWStartTs;  // _wstart pseudo column
  STimeWindow timeRange;
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
} SCaseWhenNode;

typedef struct SSelectStmt {
  ENodeType     type;  // QUERY_NODE_SELECT_STMT
  bool          isDistinct;
  SNodeList*    pProjectionList;
  SNode*        pFromTable;
  SNode*        pWhere;
  SNodeList*    pPartitionByList;
  SNodeList*    pTags;      // for create stream
  SNode*        pSubtable;  // for create stream
  SNode*        pWindow;
  SNodeList*    pGroupByList;  // SGroupingSetNode
  SNode*        pHaving;
  SNode*        pRange;
  SNode*        pEvery;
  SNode*        pFill;
  SNodeList*    pOrderByList;  // SOrderByExprNode
  SLimitNode*   pLimit;
  SLimitNode*   pSlimit;
  STimeWindow   timeRange;
  SNodeList*    pHint;
  char          stmtName[TSDB_TABLE_NAME_LEN];
  uint8_t       precision;
  int32_t       selectFuncNum;
  int32_t       returnRows;  // EFuncReturnRows
  ETimeLineMode timeLineResMode;
  bool          isEmptyResult;
  bool          isSubquery;
  bool          hasAggFuncs;
  bool          hasRepeatScanFuncs;
  bool          hasIndefiniteRowsFunc;
  bool          hasMultiRowsFunc;
  bool          hasSelectFunc;
  bool          hasSelectValFunc;
  bool          hasOtherVectorFunc;
  bool          hasUniqueFunc;
  bool          hasTailFunc;
  bool          hasInterpFunc;
  bool          hasInterpPseudoColFunc;
  bool          hasLastRowFunc;
  bool          hasLastFunc;
  bool          hasTimeLineFunc;
  bool          hasCountFunc;
  bool          hasUdaf;
  bool          hasStateKey;
  bool          onlyHasKeepOrderFunc;
  bool          groupSort;
  bool          tagScan;
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
  SHashObj*             pTableNameHashObj;   // set of table names for refreshing meta, sync mode
  SHashObj*             pDbFNameHashObj;     // set of db names for refreshing meta, sync mode
  SHashObj*             pTableCxtHashObj;    // temp SHashObj<tuid, STableDataCxt*> for single request
  SArray*               pVgDataBlocks;  // SArray<SVgroupDataCxt*>
  SVCreateTbReq*        pCreateTblReq;
  TdFilePtr             fp;
  FFreeTableBlockHash   freeHashFunc;
  FFreeVgourpBlockArray freeArrayFunc;
  bool                  usingTableProcessing;
  bool                  fileProcessing;

  bool                  stbSyntax;
  struct SStbRowsDataContext*  pStbRowsCxt;
  FFreeStbRowsDataContext     freeStbRowsCxtFunc;
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
  bool            haveResultSet;
  SNode*          pPrevRoot;
  SNode*          pRoot;
  SNode*          pPostRoot;
  int32_t         numOfResCols;
  SSchema*        pResSchema;
  int8_t          precision;
  SCmdMsgInfo*    pCmdMsg;
  int32_t         msgType;
  SArray*         pTargetTableList;
  SArray*         pTableList;
  SArray*         pDbList;
  bool            showRewrite;
  int32_t         placeholderNum;
  SArray*         pPlaceholderValues;
  SNode*          pPrepareRoot;
  bool            stableQuery;
} SQuery;

void nodesWalkSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeWalker walker, void* pContext);
void nodesRewriteSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeRewriter rewriter, void* pContext);

typedef enum ECollectColType { COLLECT_COL_TYPE_COL = 1, COLLECT_COL_TYPE_TAG, COLLECT_COL_TYPE_ALL } ECollectColType;
int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, ECollectColType type,
                            SNodeList** pCols);
int32_t nodesCollectColumnsFromNode(SNode* node, const char* pTableAlias, ECollectColType type, SNodeList** pCols);

typedef bool (*FFuncClassifier)(int32_t funcId);
int32_t nodesCollectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier, SNodeList** pFuncs);
int32_t nodesCollectSelectFuncs(SSelectStmt* pSelect, ESqlClause clause, char* tableAlias, FFuncClassifier classifier, SNodeList* pFuncs);

int32_t nodesCollectSpecialNodes(SSelectStmt* pSelect, ESqlClause clause, ENodeType type, SNodeList** pNodes);

bool nodesIsExprNode(const SNode* pNode);

bool nodesIsUnaryOp(const SOperatorNode* pOp);
bool nodesIsArithmeticOp(const SOperatorNode* pOp);
bool nodesIsComparisonOp(const SOperatorNode* pOp);
bool nodesIsJsonOp(const SOperatorNode* pOp);
bool nodesIsRegularOp(const SOperatorNode* pOp);
bool nodesIsBitwiseOp(const SOperatorNode* pOp);

bool nodesExprHasColumn(SNode* pNode);
bool nodesExprsHasColumn(SNodeList* pList);

void*   nodesGetValueFromNode(SValueNode* pNode);
int32_t nodesSetValueNodeValue(SValueNode* pNode, void* value);
char*   nodesGetStrValueFromNode(SValueNode* pNode);
void    nodesValueNodeToVariant(const SValueNode* pNode, SVariant* pVal);
SValueNode* nodesMakeValueNodeFromString(char* literal);
SValueNode* nodesMakeValueNodeFromBool(bool b);

char*   nodesGetFillModeString(EFillMode mode);
int32_t nodesMergeConds(SNode** pDst, SNodeList** pSrc);

const char* operatorTypeStr(EOperatorType type);
const char* logicConditionTypeStr(ELogicConditionType type);

bool nodesIsStar(SNode* pNode);
bool nodesIsTableStar(SNode* pNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_NODES_H_*/
