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

#include "nodes.h"

int32_t nodesNodeToString(const SNode* pNode, char** pStr, int32_t* pLen) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_COLUMN:
    case QUERY_NODE_VALUE:
    case QUERY_NODE_OPERATOR:
    case QUERY_NODE_LOGIC_CONDITION:
    case QUERY_NODE_IS_NULL_CONDITION:
    case QUERY_NODE_FUNCTION:
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
    case QUERY_NODE_GROUPING_SET:
    case QUERY_NODE_ORDER_BY_EXPR:
    case QUERY_NODE_LIMIT:
    case QUERY_NODE_STATE_WINDOW:
    case QUERY_NODE_SESSION_WINDOW:
    case QUERY_NODE_INTERVAL_WINDOW:
    case QUERY_NODE_SET_OPERATOR:
    case QUERY_NODE_SELECT_STMT:
    case QUERY_NODE_SHOW_STMT:
    default:
      break;
  }
}

int32_t nodesStringToNode(const char* pStr, SNode** pNode) {

}
