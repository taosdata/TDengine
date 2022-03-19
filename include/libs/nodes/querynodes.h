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

#define TABLE_TOTAL_COL_NUM(pMeta) ((pMeta)->tableInfo.numOfColumns + (pMeta)->tableInfo.numOfTags)
#define TABLE_META_SIZE(pMeta) (NULL == (pMeta) ? 0 : (sizeof(STableMeta) + TABLE_TOTAL_COL_NUM((pMeta)) * sizeof(SSchema)))
#define VGROUPS_INFO_SIZE(pInfo) (NULL == (pInfo) ? 0 : (sizeof(SVgroupsInfo) + (pInfo)->numOfVgroups * sizeof(SVgroupInfo)))

typedef struct SRawExprNode {
  ENodeType nodeType;
  char* p;
  uint32_t n;
  SNode* pNode;
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
  char aliasName[TSDB_COL_NAME_LEN];
  SNodeList* pAssociationList;
} SExprNode;

typedef enum EColumnType {
  COLUMN_TYPE_COLUMN = 1,
  COLUMN_TYPE_TAG
} EColumnType;

typedef struct SColumnNode {
  SExprNode node; // QUERY_NODE_COLUMN
  uint64_t tableId;
  int16_t colId;
  EColumnType colType; // column or tag
  char dbName[TSDB_DB_NAME_LEN];
  char tableName[TSDB_TABLE_NAME_LEN];
  char tableAlias[TSDB_TABLE_NAME_LEN];
  char colName[TSDB_COL_NAME_LEN];
  SNode* pProjectRef;
  int16_t dataBlockId;
  int16_t slotId;
} SColumnNode;

typedef struct STargetNode {
  ENodeType type;
  int16_t dataBlockId;
  int16_t slotId;
  SNode* pExpr;
} STargetNode;

typedef struct SValueNode {
  SExprNode node; // QUERY_NODE_VALUE
  char* literal;
  bool isDuration;
  bool translate;
  union {
    bool b;
    int64_t i;
    uint64_t u;
    double d;
    char* p;
  } datum;
  char unit;
} SValueNode;

typedef struct SOperatorNode {
  SExprNode node; // QUERY_NODE_OPERATOR
  EOperatorType opType;
  SNode* pLeft;
  SNode* pRight;
} SOperatorNode;


typedef struct SLogicConditionNode {
  SExprNode node; // QUERY_NODE_LOGIC_CONDITION
  ELogicConditionType condType;
  SNodeList* pParameterList;
} SLogicConditionNode;

typedef struct SNodeListNode {
  ENodeType type; // QUERY_NODE_NODE_LIST
  SDataType dataType;
  SNodeList* pNodeList;
} SNodeListNode;

typedef struct SFunctionNode {
  SExprNode node; // QUERY_NODE_FUNCTION
  char functionName[TSDB_FUNC_NAME_LEN];
  int32_t funcId;
  int32_t funcType;
  SNodeList* pParameterList;
} SFunctionNode;

typedef struct STableNode {
  SExprNode node;
  char dbName[TSDB_DB_NAME_LEN];
  char tableName[TSDB_TABLE_NAME_LEN];
  char tableAlias[TSDB_TABLE_NAME_LEN];
} STableNode;

struct STableMeta;

typedef struct SRealTableNode {
  STableNode table; // QUERY_NODE_REAL_TABLE
  struct STableMeta* pMeta;
  SVgroupsInfo* pVgroupList;
} SRealTableNode;

typedef struct STempTableNode {
  STableNode table; // QUERY_NODE_TEMP_TABLE
  SNode* pSubquery;
} STempTableNode;

typedef enum EJoinType {
  JOIN_TYPE_INNER = 1
} EJoinType;

typedef struct SJoinTableNode {
  STableNode table; // QUERY_NODE_JOIN_TABLE
  EJoinType joinType;
  SNode* pLeft;
  SNode* pRight;
  SNode* pOnCond;
} SJoinTableNode;

typedef enum EGroupingSetType {
  GP_TYPE_NORMAL = 1
} EGroupingSetType;

typedef struct SGroupingSetNode {
  ENodeType type; // QUERY_NODE_GROUPING_SET
  EGroupingSetType groupingSetType;
  SNodeList* pParameterList; 
} SGroupingSetNode;

typedef enum EOrder {
  ORDER_ASC = 1,
  ORDER_DESC
} EOrder;

typedef enum ENullOrder {
  NULL_ORDER_DEFAULT = 1,
  NULL_ORDER_FIRST,
  NULL_ORDER_LAST
} ENullOrder;

typedef struct SOrderByExprNode {
  ENodeType type; // QUERY_NODE_ORDER_BY_EXPR
  SNode* pExpr;
  EOrder order;
  ENullOrder nullOrder;
} SOrderByExprNode;

typedef struct SLimitNode {
  ENodeType type; // QUERY_NODE_LIMIT
  uint64_t limit;
  uint64_t offset;
} SLimitNode;

typedef struct SStateWindowNode {
  ENodeType type; // QUERY_NODE_STATE_WINDOW
  SNode* pCol;
} SStateWindowNode;

typedef struct SSessionWindowNode {
  ENodeType type; // QUERY_NODE_SESSION_WINDOW
  int64_t gap;             // gap between two session window(in microseconds)
  SNode* pCol;
} SSessionWindowNode;

typedef struct SIntervalWindowNode {
  ENodeType type; // QUERY_NODE_INTERVAL_WINDOW
  SNode* pInterval; // SValueNode
  SNode* pOffset;   // SValueNode
  SNode* pSliding;  // SValueNode
  SNode* pFill;
} SIntervalWindowNode;

typedef enum EFillMode {
  FILL_MODE_NONE = 1,
  FILL_MODE_VALUE,
  FILL_MODE_PREV,
  FILL_MODE_NULL,
  FILL_MODE_LINEAR,
  FILL_MODE_NEXT
} EFillMode;

typedef struct SFillNode {
  ENodeType type; // QUERY_NODE_FILL
  EFillMode mode;
  SNode* pValues; // SNodeListNode
} SFillNode;

typedef struct SSelectStmt {
  ENodeType type; // QUERY_NODE_SELECT_STMT
  bool isDistinct;
  SNodeList* pProjectionList; // SNode
  SNode* pFromTable;
  SNode* pWhere;
  SNodeList* pPartitionByList; // SNode
  SNode* pWindow;
  SNodeList* pGroupByList; // SGroupingSetNode
  SNode* pHaving;
  SNodeList* pOrderByList; // SOrderByExprNode
  SNode* pLimit;
  SNode* pSlimit;
} SSelectStmt;

typedef enum ESetOperatorType {
  SET_OP_TYPE_UNION_ALL = 1
} ESetOperatorType;

typedef struct SSetOperator {
  ENodeType type; // QUERY_NODE_SET_OPERATOR
  ESetOperatorType opType;
  SNode* pLeft;
  SNode* pRight;
  SNodeList* pOrderByList; // SOrderByExprNode
  SNode* pLimit;
} SSetOperator;

typedef enum ESqlClause {
  SQL_CLAUSE_FROM = 1,
  SQL_CLAUSE_WHERE,
  SQL_CLAUSE_PARTITION_BY,
  SQL_CLAUSE_WINDOW,
  SQL_CLAUSE_GROUP_BY,
  SQL_CLAUSE_HAVING,
  SQL_CLAUSE_SELECT,
  SQL_CLAUSE_ORDER_BY
} ESqlClause;


typedef enum {
  PAYLOAD_TYPE_KV = 0,
  PAYLOAD_TYPE_RAW = 1,
} EPayloadType;

typedef struct SVgDataBlocks {
  SVgroupInfo vg;
  int32_t     numOfTables;  // number of tables in current submit block
  uint32_t    size;
  char       *pData;        // SMsgDesc + SSubmitReq + SSubmitBlk + ...
} SVgDataBlocks;

typedef struct SVnodeModifOpStmt {
  ENodeType   nodeType;
  ENodeType   sqlNodeType;
  SArray*     pDataBlocks;         // data block for each vgroup, SArray<SVgDataBlocks*>.
  int8_t      schemaAttache;       // denote if submit block is built with table schema or not
  uint8_t     payloadType;         // EPayloadType. 0: K-V payload for non-prepare insert, 1: rawPayload for prepare insert
  uint32_t    insertType;          // insert data from [file|sql statement| bound statement]
  const char* sql;                 // current sql statement position
} SVnodeModifOpStmt;

void nodesWalkSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeWalker walker, void* pContext);
void nodesRewriteSelectStmt(SSelectStmt* pSelect, ESqlClause clause, FNodeRewriter rewriter, void* pContext);

int32_t nodesCollectColumns(SSelectStmt* pSelect, ESqlClause clause, const char* pTableAlias, SNodeList** pCols);

typedef bool (*FFuncClassifier)(int32_t funcId);
int32_t nodesCollectFuncs(SSelectStmt* pSelect, FFuncClassifier classifier, SNodeList** pFuncs);

bool nodesIsExprNode(const SNode* pNode);

bool nodesIsArithmeticOp(const SOperatorNode* pOp);
bool nodesIsComparisonOp(const SOperatorNode* pOp);
bool nodesIsJsonOp(const SOperatorNode* pOp);

bool nodesIsTimeorderQuery(const SNode* pQuery);
bool nodesIsTimelineQuery(const SNode* pQuery);

void* nodesGetValueFromNode(SValueNode *pNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_NODES_H_*/
