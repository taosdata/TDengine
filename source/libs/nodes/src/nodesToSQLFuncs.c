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

#include "cmdnodes.h"
#include "nodesUtil.h"
#include "plannodes.h"
#include "querynodes.h"
#include "taos.h"
#include "taoserror.h"
#include "thash.h"

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

int32_t nodesNodeToSQL(SNode *pNode, char *buf, int32_t bufSize, int32_t *len) {
  switch (pNode->type) {
    case QUERY_NODE_COLUMN: {
      SColumnNode *colNode = (SColumnNode *)pNode;
      if (colNode->dbName[0]) {
        *len += snprintf(buf + *len, bufSize - *len, "`%s`.", colNode->dbName);
      }

      if (colNode->tableAlias[0]) {
        *len += snprintf(buf + *len, bufSize - *len, "`%s`.", colNode->tableAlias);
      } else if (colNode->tableName[0]) {
        *len += snprintf(buf + *len, bufSize - *len, "`%s`.", colNode->tableName);
      }

      if (colNode->tableAlias[0]) {
        *len += snprintf(buf + *len, bufSize - *len, "`%s`", colNode->node.userAlias);
      } else {
        *len += snprintf(buf + *len, bufSize - *len, "%s", colNode->node.userAlias);
      }

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_VALUE: {
      SValueNode *colNode = (SValueNode *)pNode;
      char       *t = nodesGetStrValueFromNode(colNode);
      if (NULL == t) {
        nodesError("fail to get str value from valueNode");
        NODES_ERR_RET(TSDB_CODE_APP_ERROR);
      }

      int32_t tlen = strlen(t);
      if (tlen > 32) {
        *len += snprintf(buf + *len, bufSize - *len, "%.*s...%s", 32, t, t + tlen - 1);
      } else {
        *len += snprintf(buf + *len, bufSize - *len, "%s", t);
      }
      taosMemoryFree(t);

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode *pOpNode = (SOperatorNode *)pNode;
      *len += snprintf(buf + *len, bufSize - *len, "(");
      if (pOpNode->pLeft) {
        NODES_ERR_RET(nodesNodeToSQL(pOpNode->pLeft, buf, bufSize, len));
      }

      *len += snprintf(buf + *len, bufSize - *len, " %s ", operatorTypeStr(pOpNode->opType));

      if (pOpNode->pRight) {
        NODES_ERR_RET(nodesNodeToSQL(pOpNode->pRight, buf, bufSize, len));
      }

      *len += snprintf(buf + *len, bufSize - *len, ")");

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_LOGIC_CONDITION: {
      SLogicConditionNode *pLogicNode = (SLogicConditionNode *)pNode;
      SNode               *node = NULL;
      bool                 first = true;

      *len += snprintf(buf + *len, bufSize - *len, "(");

      FOREACH(node, pLogicNode->pParameterList) {
        if (!first) {
          *len += snprintf(buf + *len, bufSize - *len, " %s ", logicConditionTypeStr(pLogicNode->condType));
        }
        NODES_ERR_RET(nodesNodeToSQL(node, buf, bufSize, len));
        first = false;
      }

      *len += snprintf(buf + *len, bufSize - *len, ")");

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode *pFuncNode = (SFunctionNode *)pNode;
      SNode         *node = NULL;
      bool           first = true;

      *len += snprintf(buf + *len, bufSize - *len, "%s(", pFuncNode->functionName);

      FOREACH(node, pFuncNode->pParameterList) {
        if (!first) {
          *len += snprintf(buf + *len, bufSize - *len, ", ");
        }
        NODES_ERR_RET(nodesNodeToSQL(node, buf, bufSize, len));
        first = false;
      }

      *len += snprintf(buf + *len, bufSize - *len, ")");

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_NODE_LIST: {
      SNodeListNode *pListNode = (SNodeListNode *)pNode;
      SNode         *node = NULL;
      bool           first = true;
      int32_t        num = 0;

      *len += snprintf(buf + *len, bufSize - *len, "(");

      FOREACH(node, pListNode->pNodeList) {
        if (!first) {
          *len += snprintf(buf + *len, bufSize - *len, ", ");
          if (++num >= 10) {
            *len += snprintf(buf + *len, bufSize - *len, "...");
            break;
          }
        }
        NODES_ERR_RET(nodesNodeToSQL(node, buf, bufSize, len));
        first = false;
      }

      *len += snprintf(buf + *len, bufSize - *len, ")");

      return TSDB_CODE_SUCCESS;
    }
    default:
      break;
  }

  nodesError("nodesNodeToSQL unknown node = %s", nodesNodeName(pNode->type));
  NODES_RET(TSDB_CODE_APP_ERROR);
}
