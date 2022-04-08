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

char *gOperatorStr[] = {NULL, "+", "-", "*", "/", "%", "-", "&", "|", ">", ">=", "<", "<=", "=", "<>", 
                        "IN", "NOT IN", "LIKE", "NOT LIKE", "MATCH", "NMATCH", "IS NULL", "IS NOT NULL",
                        "IS TRUE", "IS FALSE", "IS UNKNOWN", "IS NOT TRUE", "IS NOT FALSE", "IS NOT UNKNOWN"};
char *gLogicConditionStr[] = {"AND", "OR", "NOT"};

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

      *len += snprintf(buf + *len, bufSize - *len, "`%s`", colNode->colName);
      
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_VALUE:{
      SValueNode *colNode = (SValueNode *)pNode;
      char *t = nodesGetStrValueFromNode(colNode);
      if (NULL == t) {
        nodesError("fail to get str value from valueNode");
        NODES_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      *len += snprintf(buf + *len, bufSize - *len, "%s", t);
      taosMemoryFree(t);
      
      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOpNode = (SOperatorNode*)pNode;
      *len += snprintf(buf + *len, bufSize - *len, "(");
      if (pOpNode->pLeft) {
        NODES_ERR_RET(nodesNodeToSQL(pOpNode->pLeft, buf, bufSize, len));
      }

      if (pOpNode->opType >= (sizeof(gOperatorStr) / sizeof(gOperatorStr[0]))) {
        nodesError("unknown operation type:%d", pOpNode->opType);
        NODES_ERR_RET(TSDB_CODE_QRY_APP_ERROR);
      }
      
      *len += snprintf(buf + *len, bufSize - *len, " %s ", gOperatorStr[pOpNode->opType]);

      if (pOpNode->pRight) {
        NODES_ERR_RET(nodesNodeToSQL(pOpNode->pRight, buf, bufSize, len));
      }    

      *len += snprintf(buf + *len, bufSize - *len, ")");

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_LOGIC_CONDITION:{
      SLogicConditionNode* pLogicNode = (SLogicConditionNode*)pNode;
      SNode* node = NULL;
      bool first = true;
      
      *len += snprintf(buf + *len, bufSize - *len, "(");
      
      FOREACH(node, pLogicNode->pParameterList) {
        if (!first) {
          *len += snprintf(buf + *len, bufSize - *len, " %s ", gLogicConditionStr[pLogicNode->condType]);
        }
        NODES_ERR_RET(nodesNodeToSQL(node, buf, bufSize, len));
        first = false;
      }

      *len += snprintf(buf + *len, bufSize - *len, ")");

      return TSDB_CODE_SUCCESS;
    }
    case QUERY_NODE_FUNCTION:{
      SFunctionNode* pFuncNode = (SFunctionNode*)pNode;
      SNode* node = NULL;
      bool first = true;
      
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
    case QUERY_NODE_NODE_LIST:{
      SNodeListNode* pListNode = (SNodeListNode *)pNode;
      SNode* node = NULL;
      bool first = true;
      
      *len += snprintf(buf + *len, bufSize - *len, "(");
      
      FOREACH(node, pListNode->pNodeList) {
        if (!first) {
          *len += snprintf(buf + *len, bufSize - *len, ", ");
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
  NODES_RET(TSDB_CODE_QRY_APP_ERROR);
}
