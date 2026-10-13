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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"
#include <stdarg.h>

static int32_t nodesSqlAppend(char* buf, int32_t bufSize, int32_t* len, const char* fmt, ...) {
  if (NULL == buf || NULL == len || *len < 0 || *len >= bufSize) return TSDB_CODE_OUT_OF_BUFFER;
  va_list args;
  va_start(args, fmt);
  int32_t written = vsnprintf(buf + *len, bufSize - *len, fmt, args);
  va_end(args);
  if (written < 0 || written >= bufSize - *len) return TSDB_CODE_OUT_OF_BUFFER;
  *len += written;
  return TSDB_CODE_SUCCESS;
}

int32_t nodesNodeToSQLFormat(SNode *pNode, char *buf, int32_t bufSize, int32_t *len, bool longFormat);

const char *operatorTypeStr(EOperatorType type) {
  switch (type) {
    case OP_TYPE_ADD:
      return "+";
    case OP_TYPE_SUB:
      return "-";
    case OP_TYPE_MULTI:
      return "*";
    case OP_TYPE_DIV:
      return "/";
    case OP_TYPE_REM:
      return "%";
    case OP_TYPE_MINUS:
      return "-";
    case OP_TYPE_BIT_AND:
      return "&";
    case OP_TYPE_BIT_OR:
      return "|";
    case OP_TYPE_GREATER_THAN:
      return ">";
    case OP_TYPE_GREATER_EQUAL:
      return ">=";
    case OP_TYPE_LOWER_THAN:
      return "<";
    case OP_TYPE_LOWER_EQUAL:
      return "<=";
    case OP_TYPE_EQUAL:
      return "=";
    case OP_TYPE_NOT_EQUAL:
      return "<>";
    case OP_TYPE_IN:
      return "IN";
    case OP_TYPE_NOT_IN:
      return "NOT IN";
    case OP_TYPE_LIKE:
      return "LIKE";
    case OP_TYPE_NOT_LIKE:
      return "NOT LIKE";
    case OP_TYPE_MATCH:
      return "MATCH";
    case OP_TYPE_NMATCH:
      return "NMATCH";
    case OP_TYPE_IS_NULL:
      return "IS NULL";
    case OP_TYPE_IS_NOT_NULL:
      return "IS NOT NULL";
    case OP_TYPE_IS_TRUE:
      return "IS TRUE";
    case OP_TYPE_IS_FALSE:
      return "IS FALSE";
    case OP_TYPE_IS_UNKNOWN:
      return "IS UNKNOWN";
    case OP_TYPE_IS_NOT_TRUE:
      return "IS NOT TRUE";
    case OP_TYPE_IS_NOT_FALSE:
      return "IS NOT FALSE";
    case OP_TYPE_IS_NOT_UNKNOWN:
      return "IS NOT UNKNOWN";
    case OP_TYPE_EXISTS:
      return "EXISTS";
    case OP_TYPE_NOT_EXISTS:
      return "NOT EXISTS";
    case OP_TYPE_JSON_GET_VALUE:
      return "=>";
    case OP_TYPE_JSON_CONTAINS:
      return "CONTAINS";
    case OP_TYPE_ASSIGN:
      return "=";
    default:
      break;
  }
  return "UNKNOWN";
}

const char *operatorTypeStrEx(EOperatorType type, bool isNegative) {
  if (isNegative) {
    switch (type) {
      case OP_TYPE_IN:
        return "!= ANY";
      case OP_TYPE_NOT_IN:
        return "= ALL";
      default:
        break;
    }
    return "UNKNOWN";
  }

  return operatorTypeStr(type);
}

const char *logicConditionTypeStr(ELogicConditionType type) {
  switch (type) {
    case LOGIC_COND_TYPE_AND:
      return "AND";
    case LOGIC_COND_TYPE_OR:
      return "OR";
    case LOGIC_COND_TYPE_NOT:
      return "NOT";
    default:
      break;
  }
  return "UNKNOWN";
}

static int32_t sqlWindowBoundToSQL(SSqlWindowBound *pBound, char *buf, int32_t bufSize, int32_t *len) {
  switch (pBound->boundType) {
    case WINDOW_BOUND_UNBOUNDED_PRECEDING:
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "UNBOUNDED PRECEDING"));
      return TSDB_CODE_SUCCESS;
    case WINDOW_BOUND_N_PRECEDING:
      if (NULL == pBound->pOffset) {
        return TSDB_CODE_FAILED;
      }
      NODES_ERR_RET(nodesNodeToSQLFormat(pBound->pOffset, buf, bufSize, len, true));
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " PRECEDING"));
      return TSDB_CODE_SUCCESS;
    case WINDOW_BOUND_CURRENT_ROW:
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "CURRENT ROW"));
      return TSDB_CODE_SUCCESS;
    case WINDOW_BOUND_N_FOLLOWING:
      if (NULL == pBound->pOffset) {
        return TSDB_CODE_FAILED;
      }
      NODES_ERR_RET(nodesNodeToSQLFormat(pBound->pOffset, buf, bufSize, len, true));
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " FOLLOWING"));
      return TSDB_CODE_SUCCESS;
    case WINDOW_BOUND_UNBOUNDED_FOLLOWING:
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "UNBOUNDED FOLLOWING"));
      return TSDB_CODE_SUCCESS;
    default:
      break;
  }
  return TSDB_CODE_FAILED;
}

int32_t nodesNodeToSQL(SNode *pNode, char *buf, int32_t bufSize, int32_t *len) {
  return nodesNodeToSQLFormat(pNode, buf, bufSize, len, false);
}

int32_t nodesNodeToSQLFormat(SNode *pNode, char *buf, int32_t bufSize, int32_t *len, bool longFormat) {
  if (NULL == pNode || NULL == buf || NULL == len || *len < 0 || *len >= bufSize) {
    return TSDB_CODE_OUT_OF_BUFFER;
  }
  switch (pNode->type) {
    case QUERY_NODE_OUTER_REF: {
      SOuterRefNode *pRef = (SOuterRefNode *)pNode;
      if (NULL == buf || NULL == len || *len < 0 || *len >= bufSize) {
        return TSDB_CODE_OUT_OF_BUFFER;
      }
      int32_t written = pRef->tableAlias[0]
                            ? snprintf(buf + *len, bufSize - *len, "`%s`.`%s`", pRef->tableAlias, pRef->colName)
                            : snprintf(buf + *len, bufSize - *len, "`%s`", pRef->colName);
      if (written < 0 || written >= bufSize - *len) {
        return TSDB_CODE_OUT_OF_BUFFER;
      }
      *len += written;
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_CORRELATED_SUBQUERY: {
      SCorrelatedSubqueryNode *pSubquery = (SCorrelatedSubqueryNode *)pNode;
      if (NULL == pSubquery->pSubquerySql || '\0' == pSubquery->pSubquerySql[0]) {
        return TSDB_CODE_OPS_NOT_SUPPORT;
      }
      if (NULL == buf || NULL == len || *len < 0 || *len >= bufSize) {
        return TSDB_CODE_OUT_OF_BUFFER;
      }
      int32_t written = snprintf(buf + *len, bufSize - *len, "(%s)", pSubquery->pSubquerySql);
      if (written < 0 || written >= bufSize - *len) {
        return TSDB_CODE_OUT_OF_BUFFER;
      }
      *len += written;
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_CORRELATION_PARAM:
    case QUERY_NODE_CORRELATION_PARAM_BINDING:
    case QUERY_NODE_CORRELATION_TEMPLATE:
    case QUERY_NODE_CORRELATION_TIME_RANGE_INIT:
      // Internal parameters and templates have no user SQL placeholder syntax.
      return TSDB_CODE_OPS_NOT_SUPPORT;
    case QUERY_NODE_COLUMN: {
      SColumnNode *colNode = (SColumnNode *)pNode;
      if (colNode->dbName[0]) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "`%s`.", colNode->dbName));
      }

      if (colNode->tableAlias[0]) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "`%s`.", colNode->tableAlias));
      } else if (colNode->tableName[0]) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "`%s`.", colNode->tableName));
      }

      if (colNode->node.userAlias[0]) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "`%s`", colNode->node.userAlias));
      } else {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "`%s`", colNode->colName));
      }

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_VALUE: {
      SValueNode *colNode = (SValueNode *)pNode;
      char       *t = nodesGetStrValueFromNode(colNode);
      if (NULL == t) {
        // NULL type value (e.g. ELSE NULL in CASE WHEN)
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "NULL"));
        return TSDB_CODE_SUCCESS;
      }

      int32_t tlen = strlen(t);
      int32_t code = TSDB_CODE_SUCCESS;
      if (longFormat) {
        code = nodesSqlAppend(buf, bufSize, len, "%s", t);
      } else {
        if (tlen > 32) {
          code = nodesSqlAppend(buf, bufSize, len, "%.*s...%s", 32, t, t + tlen - 1);
        } else {
          code = nodesSqlAppend(buf, bufSize, len, "%s", t);
        }
      }

      taosMemoryFree(t);
      return code;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode *pOpNode = (SOperatorNode *)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "("));
      if (pOpNode->pLeft) {
        NODES_ERR_RET(nodesNodeToSQLFormat(pOpNode->pLeft, buf, bufSize, len, true));
      }

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " %s ", operatorTypeStrEx(pOpNode->opType, pOpNode->flag & OPERATOR_FLAG_NEGATIVE_OP)));

      if (pOpNode->pRight) {
        NODES_ERR_RET(nodesNodeToSQLFormat(pOpNode->pRight, buf, bufSize, len, true));
      }

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ")"));

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_SQL_WINDOW_SPEC: {
      SWindowSpecNode *pWin = (SWindowSpecNode *)pNode;
      SNode           *node = NULL;
      bool             first = true;

      if (pWin->refWindowName[0] != '\0' && NULL == pWin->pPartitionByList && NULL == pWin->pOrderByList &&
          NULL == pWin->pFrame) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "%s", pWin->refWindowName));
        return TSDB_CODE_SUCCESS;
      }

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "("));
      if (pWin->refWindowName[0] != '\0') {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "%s", pWin->refWindowName));
        first = false;
      }
      if (NULL != pWin->pPartitionByList) {
        if (!first) {
          NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " "));
        }
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "PARTITION BY "));
        bool firstExpr = true;
        FOREACH(node, pWin->pPartitionByList) {
          if (!firstExpr) {
            NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ", "));
          }
          NODES_ERR_RET(nodesNodeToSQLFormat(node, buf, bufSize, len, true));
          firstExpr = false;
        }
        first = false;
      }
      if (NULL != pWin->pOrderByList) {
        if (!first) {
          NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " "));
        }
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "ORDER BY "));
        bool firstExpr = true;
        FOREACH(node, pWin->pOrderByList) {
          if (!firstExpr) {
            NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ", "));
          }
          NODES_ERR_RET(nodesNodeToSQLFormat(node, buf, bufSize, len, true));
          firstExpr = false;
        }
        first = false;
      }
      if (NULL != pWin->pFrame) {
        if (!first) {
          NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " "));
        }
        NODES_ERR_RET(nodesNodeToSQLFormat(pWin->pFrame, buf, bufSize, len, true));
      }
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ")"));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_SQL_WINDOW_FRAME: {
      SWindowFrameNode *pFrame = (SWindowFrameNode *)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "%s BETWEEN ",
                       WINDOW_FRAME_UNIT_RANGE == pFrame->frameUnit ? "RANGE" : "ROWS"));
      NODES_ERR_RET(sqlWindowBoundToSQL(&pFrame->start, buf, bufSize, len));
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " AND "));
      NODES_ERR_RET(sqlWindowBoundToSQL(&pFrame->end, buf, bufSize, len));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_SQL_NAMED_WINDOW: {
      SNamedWindowNode *pWin = (SNamedWindowNode *)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "%s AS ", pWin->windowName));
      NODES_ERR_RET(nodesNodeToSQLFormat(pWin->pSpec, buf, bufSize, len, true));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode *pLogicNode = (SLogicConditionNode *)pNode;
      SNode               *node = NULL;
      bool                 first = true;

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "("));

      if (pLogicNode->condType == LOGIC_COND_TYPE_NOT) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "NOT "));
      }

      FOREACH(node, pLogicNode->pParameterList) {
        if (!first) {
          NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " %s ", logicConditionTypeStr(pLogicNode->condType)));
        }
        NODES_ERR_RET(nodesNodeToSQLFormat(node, buf, bufSize, len, true));
        first = false;
      }

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ")"));

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode *pFuncNode = (SFunctionNode *)pNode;
      SNode         *node = NULL;
      bool           first = true;

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "%s(", pFuncNode->functionName));

      FOREACH(node, pFuncNode->pParameterList) {
        if (!first) {
          NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ", "));
        }
        NODES_ERR_RET(nodesNodeToSQLFormat(node, buf, bufSize, len, true));
        first = false;
      }

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ")"));
      if (NULL != pFuncNode->pOver) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " OVER "));
        NODES_ERR_RET(nodesNodeToSQLFormat(pFuncNode->pOver, buf, bufSize, len, true));
      }

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *pListNode = (SNodeListNode *)pNode;
      SNode         *node = NULL;
      bool           first = true;
      int32_t        num = 0;

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "("));

      FOREACH(node, pListNode->pNodeList) {
        if (!first) {
          NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ", "));
          if (!longFormat) {
            if (++num >= 10) {
              NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "..."));
              break;
            }
          }
        }
        NODES_ERR_RET(nodesNodeToSQLFormat(node, buf, bufSize, len, true));
        first = false;
      }

      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, ")"));

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_VALUE: {
      SRemoteValueNode* pRemote = (SRemoteValueNode*)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "$(InitPlan %d)", pRemote->subQIdx + 1));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_VALUE_LIST: {
      SRemoteValueListNode* pRemote = (SRemoteValueListNode*)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "$(InitPlan %d)", pRemote->subQIdx + 1));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ROW: {
      SRemoteRowNode* pRemote = (SRemoteRowNode*)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "$(InitPlan %d)", pRemote->subQIdx + 1));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_ZERO_ROWS: {
      SRemoteZeroRowsNode* pRemote = (SRemoteZeroRowsNode*)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "$(InitPlan %d)", pRemote->subQIdx + 1));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_REMOTE_TABLE: {
      SRemoteTableNode* pRemote = (SRemoteTableNode*)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "$(InitPlan %d)", pRemote->subQIdx + 1));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_WHEN_THEN: {
      SWhenThenNode *pWhenThen = (SWhenThenNode *)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "WHEN "));
      NODES_ERR_RET(nodesNodeToSQLFormat(pWhenThen->pWhen, buf, bufSize, len, true));
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " THEN "));
      NODES_ERR_RET(nodesNodeToSQLFormat(pWhenThen->pThen, buf, bufSize, len, true));
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_CASE_WHEN: {
      SCaseWhenNode *pCaseWhen = (SCaseWhenNode *)pNode;
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, "CASE"));
      if (pCaseWhen->pCase) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " "));
        NODES_ERR_RET(nodesNodeToSQLFormat(pCaseWhen->pCase, buf, bufSize, len, true));
      }
      SNode *pWhenThen = NULL;
      FOREACH(pWhenThen, pCaseWhen->pWhenThenList) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " "));
        NODES_ERR_RET(nodesNodeToSQLFormat(pWhenThen, buf, bufSize, len, true));
      }
      if (pCaseWhen->pElse) {
        NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " ELSE "));
        NODES_ERR_RET(nodesNodeToSQLFormat(pCaseWhen->pElse, buf, bufSize, len, true));
      }
      NODES_ERR_RET(nodesSqlAppend(buf, bufSize, len, " END"));
      return TSDB_CODE_SUCCESS;
    }
    default:
      break;
  }

  nodesError("nodesNodeToSQL unknown node = %s", nodesNodeName(pNode->type));
  NODES_RET(TSDB_CODE_APP_ERROR);
}
