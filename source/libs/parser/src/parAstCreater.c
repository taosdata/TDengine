
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

#include "parAst.h"
#include "parUtil.h"
#include "ttime.h"

#define CHECK_OUT_OF_MEM(p) \
  do { \
    if (NULL == (p)) { \
      pCxt->valid = false; \
      snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "Out of memory"); \
      return NULL; \
    } \
  } while (0)

#define CHECK_RAW_EXPR_NODE(node) \
  do { \
    if (NULL == (node) || QUERY_NODE_RAW_EXPR != nodeType(node)) { \
      pCxt->valid = false; \
      return NULL; \
    } \
  } while (0)

SToken nil_token = { .type = TK_NK_NIL, .n = 0, .z = NULL };

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt) {
  pCxt->pQueryCxt = pParseCxt;
  pCxt->msgBuf.buf = pParseCxt->pMsg;
  pCxt->msgBuf.len = pParseCxt->msgLen;
  pCxt->notSupport = false;
  pCxt->valid = true;
  pCxt->pRootNode = NULL;
}

static void trimEscape(SToken* pName) {
  if (NULL != pName && pName->n > 1 && '`' == pName->z[0]) {
    pName->z += 1;
    pName->n -= 2;
  }
}

static bool checkUserName(SAstCreateContext* pCxt, SToken* pUserName) {
  if (NULL == pUserName) {
    pCxt->valid = false;
  } else {
    if (pUserName->n >= TSDB_USER_LEN) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
      pCxt->valid = false;
    }
  }
  if (pCxt->valid) {
    trimEscape(pUserName);
  }
  return pCxt->valid;
}

static bool checkPassword(SAstCreateContext* pCxt, const SToken* pPasswordToken, char* pPassword) {
  if (NULL == pPasswordToken) {
    pCxt->valid = false;
  } else if (pPasswordToken->n >= (TSDB_USET_PASSWORD_LEN - 2)) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    pCxt->valid = false;
  } else {
    strncpy(pPassword, pPasswordToken->z, pPasswordToken->n);
    strdequote(pPassword);
    if (strtrim(pPassword) <= 0) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_EMPTY);
      pCxt->valid = false;
    }
  }
  return pCxt->valid;
}

static bool checkAndSplitEndpoint(SAstCreateContext* pCxt, const SToken* pEp, char* pFqdn, int32_t* pPort) {
  if (NULL == pEp) {
    pCxt->valid = false;
  } else if (pEp->n >= TSDB_FQDN_LEN + 2 + 6) { // format 'fqdn:port'
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    pCxt->valid = false;
  } else {
    char ep[TSDB_FQDN_LEN + 2 + 6];
    strncpy(ep, pEp->z, pEp->n);
    strdequote(ep);
    strtrim(ep);
    char* pColon = strchr(ep, ':');
    if (NULL == pColon) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ENDPOINT);
      pCxt->valid = false;
    } else {
      strncpy(pFqdn, ep, pColon - ep);
      *pPort = strtol(pColon + 1, NULL, 10);
      if (*pPort >= UINT16_MAX || *pPort <= 0) {
        generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
        pCxt->valid = false;
      }
    }
  }
  return pCxt->valid;
}

static bool checkFqdn(SAstCreateContext* pCxt, const SToken* pFqdn) {
  if (NULL == pFqdn) {
    pCxt->valid = false;
  } else {
    if (pFqdn->n >= TSDB_FQDN_LEN) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
      pCxt->valid = false;
    }
  }
  return pCxt->valid;
}

static bool checkPort(SAstCreateContext* pCxt, const SToken* pPortToken, int32_t* pPort) {
  if (NULL == pPortToken) {
    pCxt->valid = false;
  } else {
    *pPort = strtol(pPortToken->z, NULL, 10);
    if (*pPort >= UINT16_MAX || *pPort <= 0) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
      pCxt->valid = false;
    }
  }
  return pCxt->valid;
}

static bool checkDbName(SAstCreateContext* pCxt, SToken* pDbName, bool query) {
  if (NULL == pDbName) {
    if (query && NULL == pCxt->pQueryCxt->db) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
      pCxt->valid = false;
    }
  } else {
    if (pDbName->n >= TSDB_DB_NAME_LEN) {
      generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pDbName->z);
      pCxt->valid = false;
    }
  }
  if (pCxt->valid) {
    trimEscape(pDbName);
  }
  return pCxt->valid;
}

static bool checkTableName(SAstCreateContext* pCxt, SToken* pTableName) {
  if (NULL != pTableName && pTableName->n >= TSDB_TABLE_NAME_LEN) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTableName->z);
    pCxt->valid = false;
    return false;
  }
  trimEscape(pTableName);
  return true;
}

static bool checkColumnName(SAstCreateContext* pCxt, SToken* pColumnName) {
  if (NULL != pColumnName && pColumnName->n >= TSDB_COL_NAME_LEN) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pColumnName->z);
    pCxt->valid = false;
    return false;
  }
  trimEscape(pColumnName);
  return true;
}

static bool checkIndexName(SAstCreateContext* pCxt, SToken* pIndexName) {
  if (NULL != pIndexName && pIndexName->n >= TSDB_INDEX_NAME_LEN) {
    generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pIndexName->z);
    pCxt->valid = false;
    return false;
  }
  trimEscape(pIndexName);
  return true;
}

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pToken->z;
  target->n = pToken->n;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pStart->z;
  target->n = (pEnd->z + pEnd->n) - pStart->z;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_RAW_EXPR_NODE(pNode);
  SRawExprNode* pRawExpr = (SRawExprNode*)pNode;
  SNode* pExpr = pRawExpr->pNode;
  strncpy(((SExprNode*)pExpr)->aliasName, pRawExpr->p, pRawExpr->n);
  taosMemoryFreeClear(pNode);
  return pExpr;
}

SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    pCxt->valid = false;
    return nil_token;
  }
  SRawExprNode* target = (SRawExprNode*)pNode;
  SToken t = { .type = 0, .z = target->p, .n = target->n};
  return t;
}

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  SNodeList* list = nodesMakeList();
  CHECK_OUT_OF_MEM(list);
  if (TSDB_CODE_SUCCESS != nodesListAppend(list, pNode)) {
    pCxt->valid = false;
  }
  return list;
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  if (TSDB_CODE_SUCCESS != nodesListAppend(pList, pNode)) {
    pCxt->valid = false;
  }
  return pList;
}

SNode* createColumnNode(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName) {
  if (!checkTableName(pCxt, pTableAlias) || !checkColumnName(pCxt, pColumnName)) {
    return NULL;
  }
  SColumnNode* col = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(col);
  if (NULL != pTableAlias) {
    strncpy(col->tableAlias, pTableAlias->z, pTableAlias->n);
  }
  strncpy(col->colName, pColumnName->z, pColumnName->n);
  return (SNode*)col;
}

SNodeList* addValueNodeFromTypeToList(SAstCreateContext* pCxt, SDataType dataType, SNodeList* pList) {
  char buf[64] = {0};
  //add value node for type
  snprintf(buf, sizeof(buf), "%u", dataType.type);
  SToken token = {.type = TSDB_DATA_TYPE_TINYINT, .n = strlen(buf), .z = buf};
  SNode* pNode = createValueNode(pCxt, token.type, &token);
  addNodeToList(pCxt, pList, pNode);

  //add value node for bytes
  memset(buf, 0, sizeof(buf));
  snprintf(buf, sizeof(buf), "%u", dataType.bytes);
  token.type = TSDB_DATA_TYPE_BIGINT;
  token.n = strlen(buf);
  token.z = buf;
  pNode = createValueNode(pCxt, token.type, &token);
  addNodeToList(pCxt, pList, pNode);

  return pList;
}

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  if (NULL != pLiteral) {
    val->literal = strndup(pLiteral->z, pLiteral->n);
    if (TK_NK_ID != pLiteral->type && (IS_VAR_DATA_TYPE(dataType) || TSDB_DATA_TYPE_TIMESTAMP == dataType)) {
      trimString(pLiteral->z, pLiteral->n, val->literal, pLiteral->n);
    }
    CHECK_OUT_OF_MEM(val->literal);
  }
  val->node.resType.type = dataType;
  val->node.resType.bytes = IS_VAR_DATA_TYPE(dataType) ? strlen(val->literal) : tDataTypes[dataType].bytes;
  if (TSDB_DATA_TYPE_TIMESTAMP == dataType) {
    val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  }
  val->isDuration = false;
  val->translate = false;
  return (SNode*)val;
}

SNode* createDurationValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->isDuration = true;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BIGINT;
  val->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
}

SNode* createDefaultDatabaseCondValue(SAstCreateContext* pCxt) {
  if (NULL == pCxt->pQueryCxt->db) {
    return NULL;
  }

  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strdup(pCxt->pQueryCxt->db);
  CHECK_OUT_OF_MEM(val->literal);
  val->isDuration = false;
  val->translate = false;
  val->node.resType.type = TSDB_DATA_TYPE_BINARY;
  val->node.resType.bytes = strlen(val->literal);
  val->node.resType.precision = TSDB_TIME_PRECISION_MILLI;
  return (SNode*)val;
}

SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2) {
  SLogicConditionNode* cond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->condType = type;
  cond->pParameterList = nodesMakeList();
  nodesListAppend(cond->pParameterList, pParam1);
  nodesListAppend(cond->pParameterList, pParam2);
  return (SNode*)cond;
}

SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight) {
  SOperatorNode* op = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  CHECK_OUT_OF_MEM(op);
  op->opType = type;
  op->pLeft = pLeft;
  op->pRight = pRight;
  return (SNode*)op;
}

SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND,
      createOperatorNode(pCxt, OP_TYPE_GREATER_EQUAL, pExpr, pLeft), createOperatorNode(pCxt, OP_TYPE_LOWER_EQUAL, nodesCloneNode(pExpr), pRight));
}

SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR,
      createOperatorNode(pCxt, OP_TYPE_LOWER_THAN, pExpr, pLeft), createOperatorNode(pCxt, OP_TYPE_GREATER_THAN, nodesCloneNode(pExpr), pRight));
}

SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  strncpy(func->functionName, pFuncName->z, pFuncName->n);
  func->pParameterList = pParameterList;
  return (SNode*)func;
}

SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList) {
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = pList;
  return (SNode*)list;
}

SNode* createNodeListNodeEx(SAstCreateContext* pCxt, SNode* p1, SNode* p2) {
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = nodesMakeList();
  CHECK_OUT_OF_MEM(list->pNodeList);
  nodesListAppend(list->pNodeList, p1);
  nodesListAppend(list->pNodeList, p2);
  return (SNode*)list;
}

SNode* createRealTableNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pTableAlias) {
  if (!checkDbName(pCxt, pDbName, true) || !checkTableName(pCxt, pTableName) || !checkTableName(pCxt, pTableAlias)) {
    return NULL;
  }
  SRealTableNode* realTable = (SRealTableNode*)nodesMakeNode(QUERY_NODE_REAL_TABLE);
  CHECK_OUT_OF_MEM(realTable);
  if (NULL != pDbName) {
    strncpy(realTable->table.dbName, pDbName->z, pDbName->n);
  } else {
    strcpy(realTable->table.dbName, pCxt->pQueryCxt->db);
  }
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    strncpy(realTable->table.tableAlias, pTableAlias->z, pTableAlias->n);
  } else {
    strncpy(realTable->table.tableAlias, pTableName->z, pTableName->n);
  }
  strncpy(realTable->table.tableName, pTableName->z, pTableName->n);
  if (NULL != pCxt->pQueryCxt->db) {
    strcpy(realTable->useDbName, pCxt->pQueryCxt->db);
  }  
  return (SNode*)realTable;
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias) {
  STempTableNode* tempTable = (STempTableNode*)nodesMakeNode(QUERY_NODE_TEMP_TABLE);
  CHECK_OUT_OF_MEM(tempTable);
  tempTable->pSubquery = pSubquery;
  if (NULL != pTableAlias && TK_NK_NIL != pTableAlias->type) {
    strncpy(tempTable->table.tableAlias, pTableAlias->z, pTableAlias->n);
  } else {
    sprintf(tempTable->table.tableAlias, "%p", tempTable);
  }
  if (QUERY_NODE_SELECT_STMT == nodeType(pSubquery)) {
    strcpy(((SSelectStmt*)pSubquery)->stmtName, tempTable->table.tableAlias);
  }
  return (SNode*)tempTable;
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond) {
  SJoinTableNode* joinTable = (SJoinTableNode*)nodesMakeNode(QUERY_NODE_JOIN_TABLE);
  CHECK_OUT_OF_MEM(joinTable);
  joinTable->joinType = type;
  joinTable->pLeft = pLeft;
  joinTable->pRight = pRight;
  joinTable->pOnCond = pJoinCond;
  return (SNode*)joinTable;
}

SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset) {
  SLimitNode* limitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  CHECK_OUT_OF_MEM(limitNode);
  limitNode->limit = strtol(pLimit->z, NULL, 10);
  if (NULL != pOffset) {
    limitNode->offset = strtol(pOffset->z, NULL, 10);
  }
  return (SNode*)limitNode;
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
  SOrderByExprNode* orderByExpr = (SOrderByExprNode*)nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR);
  CHECK_OUT_OF_MEM(orderByExpr);
  orderByExpr->pExpr = pExpr;
  orderByExpr->order = order;
  if (NULL_ORDER_DEFAULT == nullOrder) {
    nullOrder = (ORDER_ASC == order ? NULL_ORDER_FIRST : NULL_ORDER_LAST);
  }
  orderByExpr->nullOrder = nullOrder;
  return (SNode*)orderByExpr;
}

SNode* createSessionWindowNode(SAstCreateContext* pCxt, SNode* pCol, SNode* pGap) {
  SSessionWindowNode* session = (SSessionWindowNode*)nodesMakeNode(QUERY_NODE_SESSION_WINDOW);
  CHECK_OUT_OF_MEM(session);
  session->pCol = pCol;
  session->pGap = pGap;
  return (SNode*)session;
}

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr) {
  SStateWindowNode* state = (SStateWindowNode*)nodesMakeNode(QUERY_NODE_STATE_WINDOW);
  CHECK_OUT_OF_MEM(state);
  state->pExpr = pExpr;
  return (SNode*)state;
}

SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding, SNode* pFill) {
  SIntervalWindowNode* interval = (SIntervalWindowNode*)nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  CHECK_OUT_OF_MEM(interval);
  interval->pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  if (NULL == interval->pCol) {
    nodesDestroyNode(interval);
    CHECK_OUT_OF_MEM(interval->pCol);
  }
  ((SColumnNode*)interval->pCol)->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(((SColumnNode*)interval->pCol)->colName, PK_TS_COL_INTERNAL_NAME);
  interval->pInterval = pInterval;
  interval->pOffset = pOffset;
  interval->pSliding = pSliding;
  interval->pFill = pFill;
  return (SNode*)interval;
}

SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues) {
  SFillNode* fill = (SFillNode*)nodesMakeNode(QUERY_NODE_FILL);
  CHECK_OUT_OF_MEM(fill);
  fill->mode = mode;
  fill->pValues = pValues;
  return (SNode*)fill;
}

SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode) {
  SGroupingSetNode* groupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  CHECK_OUT_OF_MEM(groupingSet);
  groupingSet->groupingSetType = GP_TYPE_NORMAL;
  groupingSet->pParameterList = nodesMakeList();
  nodesListAppend(groupingSet->pParameterList, pNode);
  return (SNode*)groupingSet;
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias) {
  if (NULL == pNode || !pCxt->valid) {
    return pNode;
  }
  uint32_t maxLen = sizeof(((SExprNode*)pNode)->aliasName);
  strncpy(((SExprNode*)pNode)->aliasName, pAlias->z, pAlias->n > maxLen ? maxLen : pAlias->n);
  return pNode;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = pWhere;
  }
  return pStmt;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = pPartitionByList;
  }
  return pStmt;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = pWindow;
  }
  return pStmt;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = pGroupByList;
  }
  return pStmt;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = pHaving;
  }
  return pStmt;
}

SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pOrderByList = pOrderByList;
  }
  return pStmt;
}

SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pSlimit = pSlimit;
  }
  return pStmt;
}

SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pLimit = pLimit;
  }
  return pStmt;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  CHECK_OUT_OF_MEM(select);
  select->isDistinct = isDistinct;
  select->pProjectionList = pProjectionList;
  select->pFromTable = pTable;
  sprintf(select->stmtName, "%p", select);
  return (SNode*)select;
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {
  SSetOperator* setOp = (SSetOperator*)nodesMakeNode(QUERY_NODE_SET_OPERATOR);
  CHECK_OUT_OF_MEM(setOp);
  setOp->opType = type;
  setOp->pLeft = pLeft;
  setOp->pRight = pRight;
  return (SNode*)setOp;
}

SNode* createDatabaseOptions(SAstCreateContext* pCxt) {
  SDatabaseOptions* pOptions = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  return (SNode*)pOptions;
}

static bool checkAndSetKeepOption(SAstCreateContext* pCxt, SNodeList* pKeep, int32_t* pKeep0, int32_t* pKeep1, int32_t* pKeep2) {
  int32_t numOfKeep = LIST_LENGTH(pKeep);
  if (numOfKeep > 3 || numOfKeep < 1) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid number of keep options");
    return false;
  }

  int32_t daysToKeep0 = strtol(((SValueNode*)nodesListGetNode(pKeep, 0))->literal, NULL, 10);
  int32_t daysToKeep1 = numOfKeep > 1 ? strtol(((SValueNode*)nodesListGetNode(pKeep, 1))->literal, NULL, 10) : daysToKeep0;
  int32_t daysToKeep2 = numOfKeep > 2 ? strtol(((SValueNode*)nodesListGetNode(pKeep, 2))->literal, NULL, 10) : daysToKeep1;
  if (daysToKeep0 < TSDB_MIN_KEEP || daysToKeep1 < TSDB_MIN_KEEP || daysToKeep2 < TSDB_MIN_KEEP ||
      daysToKeep0 > TSDB_MAX_KEEP || daysToKeep1 > TSDB_MAX_KEEP || daysToKeep2 > TSDB_MAX_KEEP) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen,
        "invalid option keep: %d, %d, %d valid range: [%d, %d]", daysToKeep0, daysToKeep1, daysToKeep2, TSDB_MIN_KEEP, TSDB_MAX_KEEP);
    return false;
  }

  if (!((daysToKeep0 <= daysToKeep1) && (daysToKeep1 <= daysToKeep2))) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "invalid keep value, should be keep0 <= keep1 <= keep2");
    return false;
  }

  *pKeep0 = daysToKeep0;
  *pKeep1 = daysToKeep1;
  *pKeep2 = daysToKeep2;
  return true;
}

SNode* setDatabaseAlterOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption) {
  switch (pAlterOption->type) {
    case DB_OPTION_BLOCKS:
      ((SDatabaseOptions*)pOptions)->pNumOfBlocks = pAlterOption->pVal;
      break;
    case DB_OPTION_CACHE:
      ((SDatabaseOptions*)pOptions)->pCacheBlockSize = pAlterOption->pVal;
      break;
    case DB_OPTION_CACHELAST:
      ((SDatabaseOptions*)pOptions)->pCachelast = pAlterOption->pVal;
      break;
    case DB_OPTION_COMP:
      ((SDatabaseOptions*)pOptions)->pCompressionLevel = pAlterOption->pVal;
      break;
    case DB_OPTION_DAYS:
      ((SDatabaseOptions*)pOptions)->pDaysPerFile = pAlterOption->pVal;
      break;
    case DB_OPTION_FSYNC:
      ((SDatabaseOptions*)pOptions)->pFsyncPeriod = pAlterOption->pVal;
      break;
    case DB_OPTION_MAXROWS:
      ((SDatabaseOptions*)pOptions)->pMaxRowsPerBlock = pAlterOption->pVal;
      break;
    case DB_OPTION_MINROWS:
      ((SDatabaseOptions*)pOptions)->pMinRowsPerBlock = pAlterOption->pVal;
      break;
    case DB_OPTION_KEEP:
      ((SDatabaseOptions*)pOptions)->pKeep = pAlterOption->pList;
      break;
    case DB_OPTION_PRECISION:
      ((SDatabaseOptions*)pOptions)->pPrecision = pAlterOption->pVal;
      break;
    case DB_OPTION_QUORUM:
      ((SDatabaseOptions*)pOptions)->pQuorum = pAlterOption->pVal;
      break;
    case DB_OPTION_REPLICA:
      ((SDatabaseOptions*)pOptions)->pReplica = pAlterOption->pVal;
      break;
    case DB_OPTION_TTL:
      ((SDatabaseOptions*)pOptions)->pTtl = pAlterOption->pVal;
      break;
    case DB_OPTION_WAL:
      ((SDatabaseOptions*)pOptions)->pWalLevel = pAlterOption->pVal;
      break;
    case DB_OPTION_VGROUPS:
      ((SDatabaseOptions*)pOptions)->pNumOfVgroups = pAlterOption->pVal;
      break;
    case DB_OPTION_SINGLE_STABLE:
      ((SDatabaseOptions*)pOptions)->pSingleStable = pAlterOption->pVal;
      break;
    case DB_OPTION_STREAM_MODE:
      ((SDatabaseOptions*)pOptions)->pStreamMode = pAlterOption->pVal;
      break;
    case DB_OPTION_RETENTIONS:
      ((SDatabaseOptions*)pOptions)->pRetentions = pAlterOption->pList;
      break;
    default:
      break;
  }
  return pOptions;
}

SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pDbName, SNode* pOptions) {
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SCreateDatabaseStmt* pStmt = (SCreateDatabaseStmt*)nodesMakeNode(QUERY_NODE_CREATE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createDropDatabaseStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pDbName) {
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SDropDatabaseStmt* pStmt = (SDropDatabaseStmt*)nodesMakeNode(QUERY_NODE_DROP_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createAlterDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName, SNode* pOptions) {
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SAlterDatabaseStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createTableOptions(SAstCreateContext* pCxt) {
  STableOptions* pOptions = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  return (SNode*)pOptions;
}

SNode* setTableAlterOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption) {
  switch (pAlterOption->type) {
    case TABLE_OPTION_KEEP:
      ((STableOptions*)pOptions)->pKeep = pAlterOption->pList;
      break;
    case TABLE_OPTION_TTL:
      ((STableOptions*)pOptions)->pTtl = pAlterOption->pVal;
      break;
    case TABLE_OPTION_COMMENT:
      ((STableOptions*)pOptions)->pComments = pAlterOption->pVal;
      break;
    case TABLE_OPTION_SMA:
      ((STableOptions*)pOptions)->pSma = pAlterOption->pList;
      break;
    case TABLE_OPTION_FILE_FACTOR:
      ((STableOptions*)pOptions)->pFilesFactor = pAlterOption->pVal;
      break;
    case TABLE_OPTION_DELAY:
      ((STableOptions*)pOptions)->pDelay = pAlterOption->pVal;
      break;
    default:
      break;
  }
  return pOptions;
}

SNode* createColumnDefNode(SAstCreateContext* pCxt, const SToken* pColName, SDataType dataType, const SToken* pComment) {
  SColumnDefNode* pCol = (SColumnDefNode*)nodesMakeNode(QUERY_NODE_COLUMN_DEF);
  CHECK_OUT_OF_MEM(pCol);
  strncpy(pCol->colName, pColName->z, pColName->n);
  pCol->dataType = dataType;
  if (NULL != pComment) {
    trimString(pComment->z, pComment->n, pCol->comments, sizeof(pCol->comments));
  }
  pCol->sma = true;
  return (SNode*)pCol;
}

SDataType createDataType(uint8_t type) {
  SDataType dt = { .type = type, .precision = 0, .scale = 0, .bytes = tDataTypes[type].bytes };
  return dt;
}

SDataType createVarLenDataType(uint8_t type, const SToken* pLen) {
  SDataType dt = { .type = type, .precision = 0, .scale = 0, .bytes = strtol(pLen->z, NULL, 10) };
  return dt;
}

SNode* createCreateTableStmt(SAstCreateContext* pCxt,
    bool ignoreExists, SNode* pRealTable, SNodeList* pCols, SNodeList* pTags, SNode* pOptions) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SCreateTableStmt* pStmt = (SCreateTableStmt*)nodesMakeNode(QUERY_NODE_CREATE_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pCols = pCols;
  pStmt->pTags = pTags;
  pStmt->pOptions = (STableOptions*)pOptions;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createCreateSubTableClause(SAstCreateContext* pCxt,
    bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable, SNodeList* pSpecificTags, SNodeList* pValsOfTags) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SCreateSubTableClause* pStmt = nodesMakeNode(QUERY_NODE_CREATE_SUBTABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  strcpy(pStmt->useDbName, ((SRealTableNode*)pUseRealTable)->table.dbName);
  strcpy(pStmt->useTableName, ((SRealTableNode*)pUseRealTable)->table.tableName);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pSpecificTags = pSpecificTags;
  pStmt->pValsOfTags = pValsOfTags;
  nodesDestroyNode(pRealTable);
  nodesDestroyNode(pUseRealTable);
  return (SNode*)pStmt;
}

SNode* createCreateMultiTableStmt(SAstCreateContext* pCxt, SNodeList* pSubTables) {
  SCreateMultiTableStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_MULTI_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pSubTables = pSubTables;
  return (SNode*)pStmt;
}

SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SDropTableClause* pStmt = nodesMakeNode(QUERY_NODE_DROP_TABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables) {
  SDropTableStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pTables = pTables;
  return (SNode*)pStmt;
}

SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  SDropSuperTableStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_SUPER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createAlterTableOption(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_OPTIONS;
  pStmt->pOptions = (STableOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pColName, SDataType dataType) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pColName->z, pColName->n);
  pStmt->dataType = dataType;
  return (SNode*)pStmt;
}

SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pColName) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pColName->z, pColName->n);
  return (SNode*)pStmt;
}

SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, const SToken* pOldColName, const SToken* pNewColName) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pOldColName->z, pOldColName->n);
  strncpy(pStmt->newColName, pNewColName->z, pNewColName->n);
  return (SNode*)pStmt;
}

SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, const SToken* pTagName, SNode* pVal) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
  strncpy(pStmt->colName, pTagName->z, pTagName->n);
  pStmt->pVal = (SValueNode*)pVal;
  return (SNode*)pStmt;
}

SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SUseDatabaseStmt* pStmt = (SUseDatabaseStmt*)nodesMakeNode(QUERY_NODE_USE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  return (SNode*)pStmt;
}

static bool needDbShowStmt(ENodeType type) {
  return QUERY_NODE_SHOW_TABLES_STMT == type || QUERY_NODE_SHOW_STABLES_STMT == type || QUERY_NODE_SHOW_VGROUPS_STMT == type;
}

SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pDbName, SNode* pTbNamePattern) {
  if (needDbShowStmt(type) && NULL == pDbName && NULL == pCxt->pQueryCxt->db) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "db not specified");
    pCxt->valid = false;
    return NULL;
  }
  SShowStmt* pStmt = nodesMakeNode(type);;
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->pTbNamePattern = pTbNamePattern;
  return (SNode*)pStmt;
}

SNode* createShowCreateDatabaseStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SHOW_CREATE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createShowCreateTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  SNode* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, const SToken* pPassword) {
  char password[TSDB_USET_PASSWORD_LEN] = {0};
  if (!checkUserName(pCxt, pUserName) || !checkPassword(pCxt, pPassword, password)) {
    return NULL;
  }
  SCreateUserStmt* pStmt = (SCreateUserStmt*)nodesMakeNode(QUERY_NODE_CREATE_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  strcpy(pStmt->password, password);
  return (SNode*)pStmt;
}

SNode* createAlterUserStmt(SAstCreateContext* pCxt, SToken* pUserName, int8_t alterType, const SToken* pVal) {
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SAlterUserStmt* pStmt = (SAlterUserStmt*)nodesMakeNode(QUERY_NODE_ALTER_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  if (TSDB_ALTER_USER_PASSWD == alterType) {
    char password[TSDB_USET_PASSWORD_LEN] = {0};
    if (!checkPassword(pCxt, pVal, password)) {
      nodesDestroyNode(pStmt);
      return NULL;
    }
    strcpy(pStmt->password, password);
  }
  pStmt->alterType = alterType;
  return (SNode*)pStmt;
}

SNode* createDropUserStmt(SAstCreateContext* pCxt, SToken* pUserName) {
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SDropUserStmt* pStmt = (SDropUserStmt*)nodesMakeNode(QUERY_NODE_DROP_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  return (SNode*)pStmt;
}

SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort) {
  int32_t port = 0;
  char fqdn[TSDB_FQDN_LEN] = {0};
  if (NULL == pPort) {
    if (!checkAndSplitEndpoint(pCxt, pFqdn, fqdn, &port)) {
      return NULL;
    }
  } else if (!checkFqdn(pCxt, pFqdn) || !checkPort(pCxt, pPort, &port)) {
    return NULL;
  }
  SCreateDnodeStmt* pStmt = (SCreateDnodeStmt*)nodesMakeNode(QUERY_NODE_CREATE_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (NULL == pPort) {
    strcpy(pStmt->fqdn, fqdn);
  } else {
    strncpy(pStmt->fqdn, pFqdn->z, pFqdn->n);
  }
  pStmt->port = port;
  return (SNode*)pStmt;
}

SNode* createDropDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode) {
  SDropDnodeStmt* pStmt = (SDropDnodeStmt*)nodesMakeNode(QUERY_NODE_DROP_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (TK_NK_INTEGER == pDnode->type) {
    pStmt->dnodeId = strtol(pDnode->z, NULL, 10);
  } else {
    if (!checkAndSplitEndpoint(pCxt, pDnode, pStmt->fqdn, &pStmt->port)) {
      nodesDestroyNode(pStmt);
      return NULL;
    }
  }
  return (SNode*)pStmt;
}

SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig, const SToken* pValue) {
  SAlterDnodeStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = strtol(pDnode->z, NULL, 10);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, bool ignoreExists, SToken* pIndexName, SToken* pTableName, SNodeList* pCols, SNode* pOptions) {
  if (!checkIndexName(pCxt, pIndexName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SCreateIndexStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_INDEX_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->indexType = type;
  pStmt->ignoreExists = ignoreExists;
  strncpy(pStmt->indexName, pIndexName->z, pIndexName->n);
  strncpy(pStmt->tableName, pTableName->z, pTableName->n);
  pStmt->pCols = pCols;
  pStmt->pOptions = (SIndexOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset, SNode* pSliding) {
  SIndexOptions* pOptions = nodesMakeNode(QUERY_NODE_INDEX_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->pFuncs = pFuncs;
  pOptions->pInterval = pInterval;
  pOptions->pOffset = pOffset;
  pOptions->pSliding = pSliding;
  return (SNode*)pOptions;
}

SNode* createDropIndexStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pIndexName, SToken* pTableName) {
  if (!checkIndexName(pCxt, pIndexName) || !checkTableName(pCxt, pTableName)) {
    return NULL;
  }
  SDropIndexStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_INDEX_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  strncpy(pStmt->indexName, pIndexName->z, pIndexName->n);
  strncpy(pStmt->tableName, pTableName->z, pTableName->n);
  return (SNode*)pStmt;
}

SNode* createCreateComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  SCreateComponentNodeStmt* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = strtol(pDnodeId->z, NULL, 10);;
  return (SNode*)pStmt;
}

SNode* createDropComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  SDropComponentNodeStmt* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = strtol(pDnodeId->z, NULL, 10);;
  return (SNode*)pStmt;
}

SNode* createCreateTopicStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pTopicName, SNode* pQuery, const SToken* pSubscribeDbName) {
  SCreateTopicStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pQuery = pQuery;
  if (NULL != pSubscribeDbName) {
    strncpy(pStmt->subscribeDbName, pSubscribeDbName->z, pSubscribeDbName->n);
  }
  return (SNode*)pStmt;
}

SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pTopicName) {
  SDropTopicStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  SAlterLocalStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_LOCAL_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createDefaultExplainOptions(SAstCreateContext* pCxt) {
  SExplainOptions* pOptions = nodesMakeNode(QUERY_NODE_EXPLAIN_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->verbose = TSDB_DEFAULT_EXPLAIN_VERBOSE;
  pOptions->ratio = TSDB_DEFAULT_EXPLAIN_RATIO;
  return (SNode*)pOptions;
}

SNode* setExplainVerbose(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  ((SExplainOptions*)pOptions)->verbose = (0 == strncasecmp(pVal->z, "true", pVal->n));
  return pOptions;
}

SNode* setExplainRatio(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  ((SExplainOptions*)pOptions)->ratio = strtod(pVal->z, NULL);
  return pOptions;
}

SNode* createExplainStmt(SAstCreateContext* pCxt, bool analyze, SNode* pOptions, SNode* pQuery) {
  SExplainStmt* pStmt = nodesMakeNode(QUERY_NODE_EXPLAIN_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->analyze = analyze;
  pStmt->pOptions = (SExplainOptions*)pOptions;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
}

SNode* createDescribeStmt(SAstCreateContext* pCxt, SNode* pRealTable) {
  if (NULL == pRealTable) {
    return NULL;
  }
  SDescribeStmt* pStmt = nodesMakeNode(QUERY_NODE_DESCRIBE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createResetQueryCacheStmt(SAstCreateContext* pCxt) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_RESET_QUERY_CACHE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCompactStmt(SAstCreateContext* pCxt, SNodeList* pVgroups) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_COMPACT_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCreateFunctionStmt(SAstCreateContext* pCxt, bool aggFunc, const SToken* pFuncName, const SToken* pLibPath, SDataType dataType, int32_t bufSize) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_CREATE_FUNCTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createDropFunctionStmt(SAstCreateContext* pCxt, const SToken* pFuncName) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_DROP_FUNCTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCreateStreamStmt(SAstCreateContext* pCxt, const SToken* pStreamName, const SToken* pTableName, SNode* pQuery) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_CREATE_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createDropStreamStmt(SAstCreateContext* pCxt, const SToken* pStreamName) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_DROP_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createKillStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pId) {
  SNode* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createMergeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId1, const SToken* pVgId2) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_MERGE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createRedistributeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, SNodeList* pDnodes) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_REDISTRIBUTE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createSplitVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SPLIT_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createSyncdbStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SYNCDB_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}
