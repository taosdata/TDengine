
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

#include <regex.h>

#include "parAst.h"
#include "parUtil.h"
#include "ttime.h"

#define CHECK_OUT_OF_MEM(p)                                                      \
  do {                                                                           \
    if (NULL == (p)) {                                                           \
      pCxt->errCode = TSDB_CODE_OUT_OF_MEMORY;                                   \
      snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "Out of memory"); \
      return NULL;                                                               \
    }                                                                            \
  } while (0)

#define CHECK_PARSER_STATUS(pCxt)             \
  do {                                        \
    if (TSDB_CODE_SUCCESS != pCxt->errCode) { \
      return NULL;                            \
    }                                         \
  } while (0)

SToken nil_token = {.type = TK_NK_NIL, .n = 0, .z = NULL};

void initAstCreateContext(SParseContext* pParseCxt, SAstCreateContext* pCxt) {
  pCxt->pQueryCxt = pParseCxt;
  pCxt->msgBuf.buf = pParseCxt->pMsg;
  pCxt->msgBuf.len = pParseCxt->msgLen;
  pCxt->notSupport = false;
  pCxt->pRootNode = NULL;
  pCxt->placeholderNo = 0;
  pCxt->pPlaceholderValues = NULL;
  pCxt->errCode = TSDB_CODE_SUCCESS;
}

static void copyStringFormStringToken(SToken* pToken, char* pBuf, int32_t len) {
  if (pToken->n > 2) {
    strncpy(pBuf, pToken->z + 1, TMIN(pToken->n - 2, len - 1));
  }
}

static void trimEscape(SToken* pName) {
  // todo need to deal with `ioo``ii` -> ioo`ii
  if (NULL != pName && pName->n > 1 && '`' == pName->z[0]) {
    pName->z += 1;
    pName->n -= 2;
  }
}

static bool checkUserName(SAstCreateContext* pCxt, SToken* pUserName) {
  if (NULL == pUserName) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else {
    if (pUserName->n >= TSDB_USER_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    }
  }
  if (TSDB_CODE_SUCCESS == pCxt->errCode) {
    trimEscape(pUserName);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool invalidPassword(const char* pPassword) {
  regex_t regex;

  if (regcomp(&regex, "[ '\"`\\]", REG_EXTENDED | REG_ICASE) != 0) {
    return false;
  }

  /* Execute regular expression */
  int32_t res = regexec(&regex, pPassword, 0, NULL, 0);
  regfree(&regex);
  return 0 == res;
}

static bool checkPassword(SAstCreateContext* pCxt, const SToken* pPasswordToken, char* pPassword) {
  if (NULL == pPasswordToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pPasswordToken->n >= (TSDB_USET_PASSWORD_LEN + 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  } else {
    strncpy(pPassword, pPasswordToken->z, pPasswordToken->n);
    strdequote(pPassword);
    if (strtrim(pPassword) <= 0) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_PASSWD_EMPTY);
    } else if (invalidPassword(pPassword)) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PASSWD);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkAndSplitEndpoint(SAstCreateContext* pCxt, const SToken* pEp, char* pFqdn, int32_t* pPort) {
  if (NULL == pEp) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else if (pEp->n >= TSDB_FQDN_LEN + 2 + 6) {  // format 'fqdn:port'
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
  } else {
    char ep[TSDB_FQDN_LEN + 2 + 6];
    strncpy(ep, pEp->z, pEp->n);
    strdequote(ep);
    strtrim(ep);
    char* pColon = strchr(ep, ':');
    if (NULL == pColon) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_ENDPOINT);
    } else {
      strncpy(pFqdn, ep, pColon - ep);
      *pPort = taosStr2Int32(pColon + 1, NULL, 10);
      if (*pPort >= UINT16_MAX || *pPort <= 0) {
        pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
      }
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkFqdn(SAstCreateContext* pCxt, const SToken* pFqdn) {
  if (NULL == pFqdn) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else {
    if (pFqdn->n >= TSDB_FQDN_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_NAME_OR_PASSWD_TOO_LONG);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkPort(SAstCreateContext* pCxt, const SToken* pPortToken, int32_t* pPort) {
  if (NULL == pPortToken) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
  } else {
    *pPort = taosStr2Int32(pPortToken->z, NULL, 10);
    if (*pPort >= UINT16_MAX || *pPort <= 0) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_PORT);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkDbName(SAstCreateContext* pCxt, SToken* pDbName, bool demandDb) {
  if (NULL == pDbName) {
    if (demandDb && NULL == pCxt->pQueryCxt->db) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_DB_NOT_SPECIFIED);
    }
  } else {
    trimEscape(pDbName);
    if (pDbName->n >= TSDB_DB_NAME_LEN) {
      pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pDbName->z);
    }
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

static bool checkTableName(SAstCreateContext* pCxt, SToken* pTableName) {
  trimEscape(pTableName);
  if (NULL != pTableName && pTableName->n >= TSDB_TABLE_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pTableName->z);
    return false;
  }
  return true;
}

static bool checkColumnName(SAstCreateContext* pCxt, SToken* pColumnName) {
  trimEscape(pColumnName);
  if (NULL != pColumnName && pColumnName->n >= TSDB_COL_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pColumnName->z);
    return false;
  }
  return true;
}

static bool checkIndexName(SAstCreateContext* pCxt, SToken* pIndexName) {
  trimEscape(pIndexName);
  if (NULL != pIndexName && pIndexName->n >= TSDB_INDEX_NAME_LEN) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME, pIndexName->z);
    return false;
  }
  return true;
}

static bool checkComment(SAstCreateContext* pCxt, const SToken* pCommentToken, bool demand) {
  if (NULL == pCommentToken) {
    pCxt->errCode = demand ? TSDB_CODE_PAR_SYNTAX_ERROR : TSDB_CODE_SUCCESS;
  } else if (pCommentToken->n >= (TSDB_TB_COMMENT_LEN + 2)) {
    pCxt->errCode = generateSyntaxErrMsg(&pCxt->msgBuf, TSDB_CODE_PAR_COMMENT_TOO_LONG);
  }
  return TSDB_CODE_SUCCESS == pCxt->errCode;
}

SNode* createRawExprNode(SAstCreateContext* pCxt, const SToken* pToken, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pToken->z;
  target->n = pToken->n;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* createRawExprNodeExt(SAstCreateContext* pCxt, const SToken* pStart, const SToken* pEnd, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* target = (SRawExprNode*)nodesMakeNode(QUERY_NODE_RAW_EXPR);
  CHECK_OUT_OF_MEM(target);
  target->p = pStart->z;
  target->n = (pEnd->z + pEnd->n) - pStart->z;
  target->pNode = pNode;
  return (SNode*)target;
}

SNode* releaseRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SRawExprNode* pRawExpr = (SRawExprNode*)pNode;
  SNode*        pExpr = pRawExpr->pNode;
  if (nodesIsExprNode(pExpr)) {
    int32_t len = TMIN(sizeof(((SExprNode*)pExpr)->aliasName) - 1, pRawExpr->n);
    strncpy(((SExprNode*)pExpr)->aliasName, pRawExpr->p, len);
    ((SExprNode*)pExpr)->aliasName[len] = '\0';
  }
  taosMemoryFreeClear(pNode);
  return pExpr;
}

SToken getTokenFromRawExprNode(SAstCreateContext* pCxt, SNode* pNode) {
  if (NULL == pNode || QUERY_NODE_RAW_EXPR != nodeType(pNode)) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return nil_token;
  }
  SRawExprNode* target = (SRawExprNode*)pNode;
  SToken        t = {.type = 0, .z = target->p, .n = target->n};
  return t;
}

SNodeList* createNodeList(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeList* list = nodesMakeList();
  CHECK_OUT_OF_MEM(list);
  pCxt->errCode = nodesListAppend(list, pNode);
  return list;
}

SNodeList* addNodeToList(SAstCreateContext* pCxt, SNodeList* pList, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  pCxt->errCode = nodesListAppend(pList, pNode);
  return pList;
}

SNode* createColumnNode(SAstCreateContext* pCxt, SToken* pTableAlias, SToken* pColumnName) {
  CHECK_PARSER_STATUS(pCxt);
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

SNode* createValueNode(SAstCreateContext* pCxt, int32_t dataType, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  if (TK_NK_ID != pLiteral->type && TK_TIMEZONE != pLiteral->type &&
      (IS_VAR_DATA_TYPE(dataType) || TSDB_DATA_TYPE_TIMESTAMP == dataType)) {
    trimString(pLiteral->z, pLiteral->n, val->literal, pLiteral->n);
  }
  CHECK_OUT_OF_MEM(val->literal);
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
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
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

SNode* createPlaceholderValueNode(SAstCreateContext* pCxt, const SToken* pLiteral) {
  CHECK_PARSER_STATUS(pCxt);
  SValueNode* val = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  CHECK_OUT_OF_MEM(val);
  val->literal = strndup(pLiteral->z, pLiteral->n);
  CHECK_OUT_OF_MEM(val->literal);
  val->placeholderNo = ++pCxt->placeholderNo;
  if (NULL == pCxt->pPlaceholderValues) {
    pCxt->pPlaceholderValues = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    if (NULL == pCxt->pPlaceholderValues) {
      nodesDestroyNode(val);
      return NULL;
    }
  }
  taosArrayPush(pCxt->pPlaceholderValues, &val);
  return (SNode*)val;
}

SNode* createLogicConditionNode(SAstCreateContext* pCxt, ELogicConditionType type, SNode* pParam1, SNode* pParam2) {
  CHECK_PARSER_STATUS(pCxt);
  SLogicConditionNode* cond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  CHECK_OUT_OF_MEM(cond);
  cond->condType = type;
  cond->pParameterList = nodesMakeList();
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pParam1) && type == ((SLogicConditionNode*)pParam1)->condType) {
    nodesListAppendList(cond->pParameterList, ((SLogicConditionNode*)pParam1)->pParameterList);
    ((SLogicConditionNode*)pParam1)->pParameterList = NULL;
    nodesDestroyNode(pParam1);
  } else {
    nodesListAppend(cond->pParameterList, pParam1);
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(pParam2) && type == ((SLogicConditionNode*)pParam2)->condType) {
    nodesListAppendList(cond->pParameterList, ((SLogicConditionNode*)pParam2)->pParameterList);
    ((SLogicConditionNode*)pParam2)->pParameterList = NULL;
    nodesDestroyNode(pParam2);
  } else {
    nodesListAppend(cond->pParameterList, pParam2);
  }
  return (SNode*)cond;
}

SNode* createOperatorNode(SAstCreateContext* pCxt, EOperatorType type, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  SOperatorNode* op = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  CHECK_OUT_OF_MEM(op);
  op->opType = type;
  op->pLeft = pLeft;
  op->pRight = pRight;
  return (SNode*)op;
}

SNode* createBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_AND,
                                  createOperatorNode(pCxt, OP_TYPE_GREATER_EQUAL, pExpr, pLeft),
                                  createOperatorNode(pCxt, OP_TYPE_LOWER_EQUAL, nodesCloneNode(pExpr), pRight));
}

SNode* createNotBetweenAnd(SAstCreateContext* pCxt, SNode* pExpr, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  return createLogicConditionNode(pCxt, LOGIC_COND_TYPE_OR, createOperatorNode(pCxt, OP_TYPE_LOWER_THAN, pExpr, pLeft),
                                  createOperatorNode(pCxt, OP_TYPE_GREATER_THAN, nodesCloneNode(pExpr), pRight));
}

static SNode* createPrimaryKeyCol(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SColumnNode* pCol = nodesMakeNode(QUERY_NODE_COLUMN);
  CHECK_OUT_OF_MEM(pCol);
  pCol->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  strcpy(pCol->colName, PK_TS_COL_INTERNAL_NAME);
  return (SNode*)pCol;
}

SNode* createFunctionNode(SAstCreateContext* pCxt, const SToken* pFuncName, SNodeList* pParameterList) {
  CHECK_PARSER_STATUS(pCxt);
  if (0 == strncasecmp("_rowts", pFuncName->z, pFuncName->n) || 0 == strncasecmp("_c0", pFuncName->z, pFuncName->n)) {
    return createPrimaryKeyCol(pCxt);
  }
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  strncpy(func->functionName, pFuncName->z, pFuncName->n);
  func->pParameterList = pParameterList;
  return (SNode*)func;
}

SNode* createCastFunctionNode(SAstCreateContext* pCxt, SNode* pExpr, SDataType dt) {
  CHECK_PARSER_STATUS(pCxt);
  SFunctionNode* func = (SFunctionNode*)nodesMakeNode(QUERY_NODE_FUNCTION);
  CHECK_OUT_OF_MEM(func);
  strcpy(func->functionName, "cast");
  func->node.resType = dt;
  if (TSDB_DATA_TYPE_VARCHAR == dt.type) {
    func->node.resType.bytes = func->node.resType.bytes + VARSTR_HEADER_SIZE;
  } else if (TSDB_DATA_TYPE_NCHAR == dt.type) {
    func->node.resType.bytes = func->node.resType.bytes * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE;
  }
  nodesListMakeAppend(&func->pParameterList, pExpr);
  return (SNode*)func;
}

SNode* createNodeListNode(SAstCreateContext* pCxt, SNodeList* pList) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = pList;
  return (SNode*)list;
}

SNode* createNodeListNodeEx(SAstCreateContext* pCxt, SNode* p1, SNode* p2) {
  CHECK_PARSER_STATUS(pCxt);
  SNodeListNode* list = (SNodeListNode*)nodesMakeNode(QUERY_NODE_NODE_LIST);
  CHECK_OUT_OF_MEM(list);
  list->pNodeList = nodesMakeList();
  CHECK_OUT_OF_MEM(list->pNodeList);
  nodesListAppend(list->pNodeList, p1);
  nodesListAppend(list->pNodeList, p2);
  return (SNode*)list;
}

SNode* createRealTableNode(SAstCreateContext* pCxt, SToken* pDbName, SToken* pTableName, SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);
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
  return (SNode*)realTable;
}

SNode* createTempTableNode(SAstCreateContext* pCxt, SNode* pSubquery, const SToken* pTableAlias) {
  CHECK_PARSER_STATUS(pCxt);
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
  } else if (QUERY_NODE_SET_OPERATOR == nodeType(pSubquery)) {
    strcpy(((SSetOperator*)pSubquery)->stmtName, tempTable->table.tableAlias);
  }
  return (SNode*)tempTable;
}

SNode* createJoinTableNode(SAstCreateContext* pCxt, EJoinType type, SNode* pLeft, SNode* pRight, SNode* pJoinCond) {
  CHECK_PARSER_STATUS(pCxt);
  SJoinTableNode* joinTable = (SJoinTableNode*)nodesMakeNode(QUERY_NODE_JOIN_TABLE);
  CHECK_OUT_OF_MEM(joinTable);
  joinTable->joinType = type;
  joinTable->pLeft = pLeft;
  joinTable->pRight = pRight;
  joinTable->pOnCond = pJoinCond;
  return (SNode*)joinTable;
}

SNode* createLimitNode(SAstCreateContext* pCxt, const SToken* pLimit, const SToken* pOffset) {
  CHECK_PARSER_STATUS(pCxt);
  SLimitNode* limitNode = (SLimitNode*)nodesMakeNode(QUERY_NODE_LIMIT);
  CHECK_OUT_OF_MEM(limitNode);
  limitNode->limit = taosStr2Int64(pLimit->z, NULL, 10);
  if (NULL != pOffset) {
    limitNode->offset = taosStr2Int64(pOffset->z, NULL, 10);
  }
  return (SNode*)limitNode;
}

SNode* createOrderByExprNode(SAstCreateContext* pCxt, SNode* pExpr, EOrder order, ENullOrder nullOrder) {
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
  SSessionWindowNode* session = (SSessionWindowNode*)nodesMakeNode(QUERY_NODE_SESSION_WINDOW);
  CHECK_OUT_OF_MEM(session);
  session->pCol = (SColumnNode*)pCol;
  session->pGap = (SValueNode*)pGap;
  return (SNode*)session;
}

SNode* createStateWindowNode(SAstCreateContext* pCxt, SNode* pExpr) {
  CHECK_PARSER_STATUS(pCxt);
  SStateWindowNode* state = (SStateWindowNode*)nodesMakeNode(QUERY_NODE_STATE_WINDOW);
  CHECK_OUT_OF_MEM(state);
  state->pCol = createPrimaryKeyCol(pCxt);
  if (NULL == state->pCol) {
    nodesDestroyNode(state);
    CHECK_OUT_OF_MEM(state->pCol);
  }
  state->pExpr = pExpr;
  return (SNode*)state;
}

SNode* createIntervalWindowNode(SAstCreateContext* pCxt, SNode* pInterval, SNode* pOffset, SNode* pSliding,
                                SNode* pFill) {
  CHECK_PARSER_STATUS(pCxt);
  SIntervalWindowNode* interval = (SIntervalWindowNode*)nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW);
  CHECK_OUT_OF_MEM(interval);
  interval->pCol = createPrimaryKeyCol(pCxt);
  if (NULL == interval->pCol) {
    nodesDestroyNode(interval);
    CHECK_OUT_OF_MEM(interval->pCol);
  }
  interval->pInterval = pInterval;
  interval->pOffset = pOffset;
  interval->pSliding = pSliding;
  interval->pFill = pFill;
  return (SNode*)interval;
}

SNode* createFillNode(SAstCreateContext* pCxt, EFillMode mode, SNode* pValues) {
  CHECK_PARSER_STATUS(pCxt);
  SFillNode* fill = (SFillNode*)nodesMakeNode(QUERY_NODE_FILL);
  CHECK_OUT_OF_MEM(fill);
  fill->mode = mode;
  fill->pValues = pValues;
  fill->pWStartTs = nodesMakeNode(QUERY_NODE_FUNCTION);
  if (NULL == fill->pWStartTs) {
    nodesDestroyNode(fill);
    CHECK_OUT_OF_MEM(fill->pWStartTs);
  }
  strcpy(((SFunctionNode*)fill->pWStartTs)->functionName, "_wstartts");
  return (SNode*)fill;
}

SNode* createGroupingSetNode(SAstCreateContext* pCxt, SNode* pNode) {
  CHECK_PARSER_STATUS(pCxt);
  SGroupingSetNode* groupingSet = (SGroupingSetNode*)nodesMakeNode(QUERY_NODE_GROUPING_SET);
  CHECK_OUT_OF_MEM(groupingSet);
  groupingSet->groupingSetType = GP_TYPE_NORMAL;
  groupingSet->pParameterList = nodesMakeList();
  nodesListAppend(groupingSet->pParameterList, pNode);
  return (SNode*)groupingSet;
}

SNode* setProjectionAlias(SAstCreateContext* pCxt, SNode* pNode, const SToken* pAlias) {
  CHECK_PARSER_STATUS(pCxt);
  int32_t len = TMIN(sizeof(((SExprNode*)pNode)->aliasName) - 1, pAlias->n);
  strncpy(((SExprNode*)pNode)->aliasName, pAlias->z, len);
  ((SExprNode*)pNode)->aliasName[len] = '\0';
  strncpy(((SExprNode*)pNode)->userAlias, pAlias->z, len);
  ((SExprNode*)pNode)->userAlias[len] = '\0';
  return pNode;
}

SNode* addWhereClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWhere) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWhere = pWhere;
  }
  return pStmt;
}

SNode* addPartitionByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pPartitionByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pPartitionByList = pPartitionByList;
  }
  return pStmt;
}

SNode* addWindowClauseClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pWindow) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pWindow = pWindow;
  }
  return pStmt;
}

SNode* addGroupByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pGroupByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pGroupByList = pGroupByList;
  }
  return pStmt;
}

SNode* addHavingClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pHaving) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pHaving = pHaving;
  }
  return pStmt;
}

SNode* addOrderByClause(SAstCreateContext* pCxt, SNode* pStmt, SNodeList* pOrderByList) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pOrderByList = pOrderByList;
  }
  return pStmt;
}

SNode* addSlimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pSlimit) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pSlimit = (SLimitNode*)pSlimit;
  }
  return pStmt;
}

SNode* addLimitClause(SAstCreateContext* pCxt, SNode* pStmt, SNode* pLimit) {
  CHECK_PARSER_STATUS(pCxt);
  if (QUERY_NODE_SELECT_STMT == nodeType(pStmt)) {
    ((SSelectStmt*)pStmt)->pLimit = (SLimitNode*)pLimit;
  }
  return pStmt;
}

SNode* createSelectStmt(SAstCreateContext* pCxt, bool isDistinct, SNodeList* pProjectionList, SNode* pTable) {
  CHECK_PARSER_STATUS(pCxt);
  SSelectStmt* select = (SSelectStmt*)nodesMakeNode(QUERY_NODE_SELECT_STMT);
  CHECK_OUT_OF_MEM(select);
  select->isDistinct = isDistinct;
  select->pProjectionList = pProjectionList;
  select->pFromTable = pTable;
  sprintf(select->stmtName, "%p", select);
  select->isTimeOrderQuery = true;
  return (SNode*)select;
}

SNode* createSetOperator(SAstCreateContext* pCxt, ESetOperatorType type, SNode* pLeft, SNode* pRight) {
  CHECK_PARSER_STATUS(pCxt);
  SSetOperator* setOp = (SSetOperator*)nodesMakeNode(QUERY_NODE_SET_OPERATOR);
  CHECK_OUT_OF_MEM(setOp);
  setOp->opType = type;
  setOp->pLeft = pLeft;
  setOp->pRight = pRight;
  sprintf(setOp->stmtName, "%p", setOp);
  return (SNode*)setOp;
}

SNode* createDefaultDatabaseOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SDatabaseOptions* pOptions = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->buffer = TSDB_DEFAULT_BUFFER_PER_VNODE;
  pOptions->cachelast = TSDB_DEFAULT_CACHE_LAST_ROW;
  pOptions->compressionLevel = TSDB_DEFAULT_COMP_LEVEL;
  pOptions->daysPerFile = TSDB_DEFAULT_DAYS_PER_FILE;
  pOptions->fsyncPeriod = TSDB_DEFAULT_FSYNC_PERIOD;
  pOptions->maxRowsPerBlock = TSDB_DEFAULT_MAXROWS_FBLOCK;
  pOptions->minRowsPerBlock = TSDB_DEFAULT_MINROWS_FBLOCK;
  pOptions->keep[0] = TSDB_DEFAULT_KEEP;
  pOptions->keep[1] = TSDB_DEFAULT_KEEP;
  pOptions->keep[2] = TSDB_DEFAULT_KEEP;
  pOptions->pages = TSDB_DEFAULT_PAGES_PER_VNODE;
  pOptions->pagesize = TSDB_DEFAULT_PAGESIZE_PER_VNODE;
  pOptions->precision = TSDB_DEFAULT_PRECISION;
  pOptions->replica = TSDB_DEFAULT_DB_REPLICA;
  pOptions->strict = TSDB_DEFAULT_DB_STRICT;
  pOptions->walLevel = TSDB_DEFAULT_WAL_LEVEL;
  pOptions->numOfVgroups = TSDB_DEFAULT_VN_PER_DB;
  pOptions->singleStable = TSDB_DEFAULT_DB_SINGLE_STABLE;
  pOptions->schemaless = TSDB_DEFAULT_DB_SCHEMALESS;
  return (SNode*)pOptions;
}

SNode* createAlterDatabaseOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SDatabaseOptions* pOptions = nodesMakeNode(QUERY_NODE_DATABASE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->buffer = -1;
  pOptions->cachelast = -1;
  pOptions->compressionLevel = -1;
  pOptions->daysPerFile = -1;
  pOptions->fsyncPeriod = -1;
  pOptions->maxRowsPerBlock = -1;
  pOptions->minRowsPerBlock = -1;
  pOptions->keep[0] = -1;
  pOptions->keep[1] = -1;
  pOptions->keep[2] = -1;
  pOptions->pages = -1;
  pOptions->pagesize = -1;
  pOptions->precision = -1;
  pOptions->replica = -1;
  pOptions->strict = -1;
  pOptions->walLevel = -1;
  pOptions->numOfVgroups = -1;
  pOptions->singleStable = -1;
  pOptions->schemaless = -1;
  return (SNode*)pOptions;
}

SNode* setDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, EDatabaseOptionType type, void* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  switch (type) {
    case DB_OPTION_BUFFER:
      ((SDatabaseOptions*)pOptions)->buffer = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_CACHELAST:
      ((SDatabaseOptions*)pOptions)->cachelast = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_COMP:
      ((SDatabaseOptions*)pOptions)->compressionLevel = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_DAYS: {
      SToken* pToken = pVal;
      if (TK_NK_INTEGER == pToken->type) {
        ((SDatabaseOptions*)pOptions)->daysPerFile = taosStr2Int32(pToken->z, NULL, 10) * 1440;
      } else {
        ((SDatabaseOptions*)pOptions)->pDaysPerFile = (SValueNode*)createDurationValueNode(pCxt, pToken);
      }
      break;
    }
    case DB_OPTION_FSYNC:
      ((SDatabaseOptions*)pOptions)->fsyncPeriod = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_MAXROWS:
      ((SDatabaseOptions*)pOptions)->maxRowsPerBlock = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_MINROWS:
      ((SDatabaseOptions*)pOptions)->minRowsPerBlock = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_KEEP:
      ((SDatabaseOptions*)pOptions)->pKeep = pVal;
      break;
    case DB_OPTION_PAGES:
      ((SDatabaseOptions*)pOptions)->pages = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_PAGESIZE:
      ((SDatabaseOptions*)pOptions)->pagesize = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_PRECISION:
      copyStringFormStringToken((SToken*)pVal, ((SDatabaseOptions*)pOptions)->precisionStr,
                                sizeof(((SDatabaseOptions*)pOptions)->precisionStr));
      break;
    case DB_OPTION_REPLICA:
      ((SDatabaseOptions*)pOptions)->replica = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_STRICT:
      ((SDatabaseOptions*)pOptions)->strict = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_WAL:
      ((SDatabaseOptions*)pOptions)->walLevel = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_VGROUPS:
      ((SDatabaseOptions*)pOptions)->numOfVgroups = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_SINGLE_STABLE:
      ((SDatabaseOptions*)pOptions)->singleStable = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    case DB_OPTION_RETENTIONS:
      ((SDatabaseOptions*)pOptions)->pRetentions = pVal;
      break;
    case DB_OPTION_SCHEMALESS:
      ((SDatabaseOptions*)pOptions)->schemaless = taosStr2Int8(((SToken*)pVal)->z, NULL, 10);
      break;
    default:
      break;
  }
  return pOptions;
}

SNode* setAlterDatabaseOption(SAstCreateContext* pCxt, SNode* pOptions, SAlterOption* pAlterOption) {
  CHECK_PARSER_STATUS(pCxt);
  switch (pAlterOption->type) {
    case DB_OPTION_KEEP:
    case DB_OPTION_RETENTIONS:
      return setDatabaseOption(pCxt, pOptions, pAlterOption->type, pAlterOption->pList);
    default:
      break;
  }
  return setDatabaseOption(pCxt, pOptions, pAlterOption->type, &pAlterOption->val);
}

SNode* createCreateDatabaseStmt(SAstCreateContext* pCxt, bool ignoreExists, SToken* pDbName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SAlterDatabaseStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  pStmt->pOptions = (SDatabaseOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createDefaultTableOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STableOptions* pOptions = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->delay = TSDB_DEFAULT_ROLLUP_DELAY;
  pOptions->filesFactor = TSDB_DEFAULT_ROLLUP_FILE_FACTOR;
  pOptions->ttl = TSDB_DEFAULT_TABLE_TTL;
  return (SNode*)pOptions;
}

SNode* createAlterTableOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STableOptions* pOptions = nodesMakeNode(QUERY_NODE_TABLE_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->delay = -1;
  pOptions->filesFactor = -1;
  pOptions->ttl = -1;
  return (SNode*)pOptions;
}

SNode* setTableOption(SAstCreateContext* pCxt, SNode* pOptions, ETableOptionType type, void* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  switch (type) {
    case TABLE_OPTION_COMMENT:
      if (checkComment(pCxt, (SToken*)pVal, true)) {
        copyStringFormStringToken((SToken*)pVal, ((STableOptions*)pOptions)->comment,
                                  sizeof(((STableOptions*)pOptions)->comment));
      }
      break;
    case TABLE_OPTION_DELAY:
      ((STableOptions*)pOptions)->delay = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case TABLE_OPTION_FILE_FACTOR:
      ((STableOptions*)pOptions)->filesFactor = taosStr2Float(((SToken*)pVal)->z, NULL);
      break;
    case TABLE_OPTION_ROLLUP:
      ((STableOptions*)pOptions)->pRollupFuncs = pVal;
      break;
    case TABLE_OPTION_TTL:
      ((STableOptions*)pOptions)->ttl = taosStr2Int32(((SToken*)pVal)->z, NULL, 10);
      break;
    case TABLE_OPTION_SMA:
      ((STableOptions*)pOptions)->pSma = pVal;
      break;
    default:
      break;
  }
  return pOptions;
}

SNode* createColumnDefNode(SAstCreateContext* pCxt, SToken* pColName, SDataType dataType, const SToken* pComment) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pColName) || !checkComment(pCxt, pComment, false)) {
    return NULL;
  }
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
  SDataType dt = {.type = type, .precision = 0, .scale = 0, .bytes = tDataTypes[type].bytes};
  return dt;
}

SDataType createVarLenDataType(uint8_t type, const SToken* pLen) {
  SDataType dt = {.type = type, .precision = 0, .scale = 0, .bytes = taosStr2Int16(pLen->z, NULL, 10)};
  return dt;
}

SNode* createCreateTableStmt(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNodeList* pCols,
                             SNodeList* pTags, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
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

SNode* createCreateSubTableClause(SAstCreateContext* pCxt, bool ignoreExists, SNode* pRealTable, SNode* pUseRealTable,
                                  SNodeList* pSpecificTags, SNodeList* pValsOfTags, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
  SCreateMultiTableStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_MULTI_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pSubTables = pSubTables;
  return (SNode*)pStmt;
}

SNode* createDropTableClause(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTableClause* pStmt = nodesMakeNode(QUERY_NODE_DROP_TABLE_CLAUSE);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createDropTableStmt(SAstCreateContext* pCxt, SNodeList* pTables) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTableStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pTables = pTables;
  return (SNode*)pStmt;
}

SNode* createDropSuperTableStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDropSuperTableStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_SUPER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  pStmt->ignoreNotExists = ignoreNotExists;
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

static SNode* createAlterTableStmtFinalize(SNode* pRealTable, SAlterTableStmt* pStmt) {
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createAlterTableModifyOptions(SAstCreateContext* pCxt, SNode* pRealTable, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_OPTIONS;
  pStmt->pOptions = (STableOptions*)pOptions;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableAddModifyCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName,
                                    SDataType dataType) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pColName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pColName->z, pColName->n);
  pStmt->dataType = dataType;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableDropCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pColName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pColName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pColName->z, pColName->n);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableRenameCol(SAstCreateContext* pCxt, SNode* pRealTable, int8_t alterType, SToken* pOldColName,
                                 SToken* pNewColName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pOldColName) || !checkColumnName(pCxt, pNewColName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = alterType;
  strncpy(pStmt->colName, pOldColName->z, pOldColName->n);
  strncpy(pStmt->newColName, pNewColName->z, pNewColName->n);
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createAlterTableSetTag(SAstCreateContext* pCxt, SNode* pRealTable, SToken* pTagName, SNode* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkColumnName(pCxt, pTagName)) {
    return NULL;
  }
  SAlterTableStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_TABLE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->alterType = TSDB_ALTER_TABLE_UPDATE_TAG_VAL;
  strncpy(pStmt->colName, pTagName->z, pTagName->n);
  pStmt->pVal = (SValueNode*)pVal;
  return createAlterTableStmtFinalize(pRealTable, pStmt);
}

SNode* createUseDatabaseStmt(SAstCreateContext* pCxt, SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false)) {
    return NULL;
  }
  SUseDatabaseStmt* pStmt = (SUseDatabaseStmt*)nodesMakeNode(QUERY_NODE_USE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  return (SNode*)pStmt;
}

static bool needDbShowStmt(ENodeType type) {
  return QUERY_NODE_SHOW_TABLES_STMT == type || QUERY_NODE_SHOW_STABLES_STMT == type ||
         QUERY_NODE_SHOW_VGROUPS_STMT == type;
}

SNode* createShowStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pDbName, SNode* pTbNamePattern) {
  CHECK_PARSER_STATUS(pCxt);
  if (needDbShowStmt(type) && NULL == pDbName && NULL == pCxt->pQueryCxt->db) {
    snprintf(pCxt->pQueryCxt->pMsg, pCxt->pQueryCxt->msgLen, "db not specified");
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return NULL;
  }
  SShowStmt* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->pDbName = pDbName;
  pStmt->pTbNamePattern = pTbNamePattern;
  return (SNode*)pStmt;
}

SNode* createShowCreateDatabaseStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SHOW_CREATE_DATABASE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createShowCreateTableStmt(SAstCreateContext* pCxt, ENodeType type, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCreateUserStmt(SAstCreateContext* pCxt, SToken* pUserName, const SToken* pPassword) {
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
  if (!checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SDropUserStmt* pStmt = (SDropUserStmt*)nodesMakeNode(QUERY_NODE_DROP_USER_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->useName, pUserName->z, pUserName->n);
  return (SNode*)pStmt;
}

SNode* createCreateDnodeStmt(SAstCreateContext* pCxt, const SToken* pFqdn, const SToken* pPort) {
  CHECK_PARSER_STATUS(pCxt);
  int32_t port = 0;
  char    fqdn[TSDB_FQDN_LEN] = {0};
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
  CHECK_PARSER_STATUS(pCxt);
  SDropDnodeStmt* pStmt = (SDropDnodeStmt*)nodesMakeNode(QUERY_NODE_DROP_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  if (TK_NK_INTEGER == pDnode->type) {
    pStmt->dnodeId = taosStr2Int32(pDnode->z, NULL, 10);
  } else {
    if (!checkAndSplitEndpoint(pCxt, pDnode, pStmt->fqdn, &pStmt->port)) {
      nodesDestroyNode(pStmt);
      return NULL;
    }
  }
  return (SNode*)pStmt;
}

SNode* createAlterDnodeStmt(SAstCreateContext* pCxt, const SToken* pDnode, const SToken* pConfig,
                            const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterDnodeStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_DNODE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnode->z, NULL, 10);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createCreateIndexStmt(SAstCreateContext* pCxt, EIndexType type, bool ignoreExists, SToken* pIndexName,
                             SToken* pTableName, SNodeList* pCols, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkIndexName(pCxt, pIndexName) || !checkTableName(pCxt, pTableName) || !checkDbName(pCxt, NULL, true)) {
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

SNode* createIndexOption(SAstCreateContext* pCxt, SNodeList* pFuncs, SNode* pInterval, SNode* pOffset,
                         SNode* pSliding) {
  CHECK_PARSER_STATUS(pCxt);
  SIndexOptions* pOptions = nodesMakeNode(QUERY_NODE_INDEX_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->pFuncs = pFuncs;
  pOptions->pInterval = pInterval;
  pOptions->pOffset = pOffset;
  pOptions->pSliding = pSliding;
  return (SNode*)pOptions;
}

SNode* createDropIndexStmt(SAstCreateContext* pCxt, bool ignoreNotExists, SToken* pIndexName, SToken* pTableName) {
  CHECK_PARSER_STATUS(pCxt);
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
  CHECK_PARSER_STATUS(pCxt);
  SCreateComponentNodeStmt* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  ;
  return (SNode*)pStmt;
}

SNode* createDropComponentNodeStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pDnodeId) {
  CHECK_PARSER_STATUS(pCxt);
  SDropComponentNodeStmt* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->dnodeId = taosStr2Int32(pDnodeId->z, NULL, 10);
  ;
  return (SNode*)pStmt;
}

SNode* createTopicOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  STopicOptions* pOptions = nodesMakeNode(QUERY_NODE_TOPIC_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->withTable = false;
  pOptions->withSchema = false;
  pOptions->withTag = false;
  return (SNode*)pOptions;
}

SNode* createCreateTopicStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pTopicName, SNode* pQuery,
                             const SToken* pSubscribeDbName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateTopicStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  pStmt->ignoreExists = ignoreExists;
  pStmt->pQuery = pQuery;
  if (NULL != pSubscribeDbName) {
    strncpy(pStmt->subscribeDbName, pSubscribeDbName->z, pSubscribeDbName->n);
  }
  pStmt->pOptions = (STopicOptions*)pOptions;
  return (SNode*)pStmt;
}

SNode* createDropTopicStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pTopicName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropTopicStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_TOPIC_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createDropCGroupStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pCGroupId,
                            const SToken* pTopicName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropCGroupStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_CGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  strncpy(pStmt->topicName, pTopicName->z, pTopicName->n);
  strncpy(pStmt->cgroup, pCGroupId->z, pCGroupId->n);
  return (SNode*)pStmt;
}

SNode* createAlterLocalStmt(SAstCreateContext* pCxt, const SToken* pConfig, const SToken* pValue) {
  CHECK_PARSER_STATUS(pCxt);
  SAlterLocalStmt* pStmt = nodesMakeNode(QUERY_NODE_ALTER_LOCAL_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  trimString(pConfig->z, pConfig->n, pStmt->config, sizeof(pStmt->config));
  if (NULL != pValue) {
    trimString(pValue->z, pValue->n, pStmt->value, sizeof(pStmt->value));
  }
  return (SNode*)pStmt;
}

SNode* createDefaultExplainOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SExplainOptions* pOptions = nodesMakeNode(QUERY_NODE_EXPLAIN_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->verbose = TSDB_DEFAULT_EXPLAIN_VERBOSE;
  pOptions->ratio = TSDB_DEFAULT_EXPLAIN_RATIO;
  return (SNode*)pOptions;
}

SNode* setExplainVerbose(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  ((SExplainOptions*)pOptions)->verbose = (0 == strncasecmp(pVal->z, "true", pVal->n));
  return pOptions;
}

SNode* setExplainRatio(SAstCreateContext* pCxt, SNode* pOptions, const SToken* pVal) {
  CHECK_PARSER_STATUS(pCxt);
  ((SExplainOptions*)pOptions)->ratio = taosStr2Double(pVal->z, NULL);
  return pOptions;
}

SNode* createExplainStmt(SAstCreateContext* pCxt, bool analyze, SNode* pOptions, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SExplainStmt* pStmt = nodesMakeNode(QUERY_NODE_EXPLAIN_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->analyze = analyze;
  pStmt->pOptions = (SExplainOptions*)pOptions;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
}

SNode* createDescribeStmt(SAstCreateContext* pCxt, SNode* pRealTable) {
  CHECK_PARSER_STATUS(pCxt);
  SDescribeStmt* pStmt = nodesMakeNode(QUERY_NODE_DESCRIBE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strcpy(pStmt->dbName, ((SRealTableNode*)pRealTable)->table.dbName);
  strcpy(pStmt->tableName, ((SRealTableNode*)pRealTable)->table.tableName);
  nodesDestroyNode(pRealTable);
  return (SNode*)pStmt;
}

SNode* createResetQueryCacheStmt(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_RESET_QUERY_CACHE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCompactStmt(SAstCreateContext* pCxt, SNodeList* pVgroups) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_COMPACT_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createCreateFunctionStmt(SAstCreateContext* pCxt, bool ignoreExists, bool aggFunc, const SToken* pFuncName,
                                const SToken* pLibPath, SDataType dataType, int32_t bufSize) {
  CHECK_PARSER_STATUS(pCxt);
  if (pLibPath->n <= 2) {
    pCxt->errCode = TSDB_CODE_PAR_SYNTAX_ERROR;
    return NULL;
  }
  SCreateFunctionStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_FUNCTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreExists = ignoreExists;
  strncpy(pStmt->funcName, pFuncName->z, pFuncName->n);
  pStmt->isAgg = aggFunc;
  strncpy(pStmt->libraryPath, pLibPath->z + 1, pLibPath->n - 2);
  pStmt->outputDt = dataType;
  pStmt->bufSize = bufSize;
  return (SNode*)pStmt;
}

SNode* createDropFunctionStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pFuncName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropFunctionStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_FUNCTION_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->ignoreNotExists = ignoreNotExists;
  strncpy(pStmt->funcName, pFuncName->z, pFuncName->n);
  return (SNode*)pStmt;
}

SNode* createStreamOptions(SAstCreateContext* pCxt) {
  CHECK_PARSER_STATUS(pCxt);
  SStreamOptions* pOptions = nodesMakeNode(QUERY_NODE_STREAM_OPTIONS);
  CHECK_OUT_OF_MEM(pOptions);
  pOptions->triggerType = STREAM_TRIGGER_AT_ONCE;
  return (SNode*)pOptions;
}

SNode* createCreateStreamStmt(SAstCreateContext* pCxt, bool ignoreExists, const SToken* pStreamName, SNode* pRealTable,
                              SNode* pOptions, SNode* pQuery) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateStreamStmt* pStmt = nodesMakeNode(QUERY_NODE_CREATE_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->streamName, pStreamName->z, pStreamName->n);
  if (NULL != pRealTable) {
    strcpy(pStmt->targetDbName, ((SRealTableNode*)pRealTable)->table.dbName);
    strcpy(pStmt->targetTabName, ((SRealTableNode*)pRealTable)->table.tableName);
    nodesDestroyNode(pRealTable);
  }
  pStmt->ignoreExists = ignoreExists;
  pStmt->pOptions = (SStreamOptions*)pOptions;
  pStmt->pQuery = pQuery;
  return (SNode*)pStmt;
}

SNode* createDropStreamStmt(SAstCreateContext* pCxt, bool ignoreNotExists, const SToken* pStreamName) {
  CHECK_PARSER_STATUS(pCxt);
  SDropStreamStmt* pStmt = nodesMakeNode(QUERY_NODE_DROP_STREAM_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  strncpy(pStmt->streamName, pStreamName->z, pStreamName->n);
  pStmt->ignoreNotExists = ignoreNotExists;
  return (SNode*)pStmt;
}

SNode* createKillStmt(SAstCreateContext* pCxt, ENodeType type, const SToken* pId) {
  CHECK_PARSER_STATUS(pCxt);
  SKillStmt* pStmt = nodesMakeNode(type);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->targetId = taosStr2Int32(pId->z, NULL, 10);
  return (SNode*)pStmt;
}

SNode* createMergeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId1, const SToken* pVgId2) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_MERGE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createRedistributeVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId, SNodeList* pDnodes) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_REDISTRIBUTE_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createSplitVgroupStmt(SAstCreateContext* pCxt, const SToken* pVgId) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SPLIT_VGROUP_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createSyncdbStmt(SAstCreateContext* pCxt, const SToken* pDbName) {
  CHECK_PARSER_STATUS(pCxt);
  SNode* pStmt = nodesMakeNode(QUERY_NODE_SYNCDB_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  return pStmt;
}

SNode* createGrantStmt(SAstCreateContext* pCxt, int64_t privileges, SToken* pDbName, SToken* pUserName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false) || !checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SGrantStmt* pStmt = nodesMakeNode(QUERY_NODE_GRANT_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->privileges = privileges;
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  strncpy(pStmt->userName, pUserName->z, pUserName->n);
  return (SNode*)pStmt;
}

SNode* createRevokeStmt(SAstCreateContext* pCxt, int64_t privileges, SToken* pDbName, SToken* pUserName) {
  CHECK_PARSER_STATUS(pCxt);
  if (!checkDbName(pCxt, pDbName, false) || !checkUserName(pCxt, pUserName)) {
    return NULL;
  }
  SRevokeStmt* pStmt = nodesMakeNode(QUERY_NODE_REVOKE_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  pStmt->privileges = privileges;
  strncpy(pStmt->dbName, pDbName->z, pDbName->n);
  strncpy(pStmt->userName, pUserName->z, pUserName->n);
  return (SNode*)pStmt;
}
