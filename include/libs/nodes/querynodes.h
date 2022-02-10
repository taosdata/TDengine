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
  ENodeType nodeType;
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
  int16_t colId;
  EColumnType colType; // column or tag
  char dbName[TSDB_DB_NAME_LEN];
  char tableName[TSDB_TABLE_NAME_LEN];
  char tableAlias[TSDB_TABLE_NAME_LEN];
  char colName[TSDB_COL_NAME_LEN];
  SNode* pProjectRef;
} SColumnNode;

typedef struct SValueNode {
  SExprNode node; // QUERY_NODE_VALUE
  char* literal;
  bool isDuration;
  union {
    bool b;
    int64_t i;
    uint64_t u;
    double d;
    char* p;
  } datum;
} SValueNode;

typedef enum EOperatorType {
  // arithmetic operator
  OP_TYPE_ADD = 1,
  OP_TYPE_SUB,
  OP_TYPE_MULTI,
  OP_TYPE_DIV,
  OP_TYPE_MOD,

  // comparison operator
  OP_TYPE_GREATER_THAN,
  OP_TYPE_GREATER_EQUAL,
  OP_TYPE_LOWER_THAN,
  OP_TYPE_LOWER_EQUAL,
  OP_TYPE_EQUAL,
  OP_TYPE_NOT_EQUAL,
  OP_TYPE_IN,
  OP_TYPE_NOT_IN,
  OP_TYPE_LIKE,
  OP_TYPE_NOT_LIKE,
  OP_TYPE_MATCH,
  OP_TYPE_NMATCH,

  // json operator
  OP_TYPE_JSON_GET_VALUE,
  OP_TYPE_JSON_CONTAINS
} EOperatorType;

typedef struct SOperatorNode {
  SExprNode node; // QUERY_NODE_OPERATOR
  EOperatorType opType;
  SNode* pLeft;
  SNode* pRight;
} SOperatorNode;

typedef enum ELogicConditionType {
  LOGIC_COND_TYPE_AND,
  LOGIC_COND_TYPE_OR,
  LOGIC_COND_TYPE_NOT,
} ELogicConditionType;

typedef struct SLogicConditionNode {
  ENodeType type; // QUERY_NODE_LOGIC_CONDITION
  ELogicConditionType condType;
  SNodeList* pParameterList;
} SLogicConditionNode;

typedef struct SIsNullCondNode {
  ENodeType type; // QUERY_NODE_IS_NULL_CONDITION
  SNode* pExpr;
  bool isNull;
} SIsNullCondNode;

typedef struct SNodeListNode {
  ENodeType type; // QUERY_NODE_NODE_LIST
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
  ENodeType type;
  char dbName[TSDB_DB_NAME_LEN];
  char tableName[TSDB_TABLE_NAME_LEN];
  char tableAlias[TSDB_TABLE_NAME_LEN];
} STableNode;

struct STableMeta;

typedef struct SRealTableNode {
  STableNode table; // QUERY_NODE_REAL_TABLE
  struct STableMeta* pMeta;
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

bool nodesIsExprNode(const SNode* pNode);

bool nodesIsArithmeticOp(const SOperatorNode* pOp);
bool nodesIsComparisonOp(const SOperatorNode* pOp);
bool nodesIsJsonOp(const SOperatorNode* pOp);

bool nodesIsTimeorderQuery(const SNode* pQuery);
bool nodesIsTimelineQuery(const SNode* pQuery);

#ifdef __cplusplus
}
#endif

#endif /*_TD_QUERY_NODES_H_*/