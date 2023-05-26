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

#include "querynodes.h"

#define MATCH_SCALAR_FIELD(fldname)           \
  do {                                          \
    if (p->fldname != pSub->fldname) return false; \
  } while (0)

#define MATCH_STRING(a, b) (((a) != NULL && (b) != NULL) ? (strcmp((a), (b)) == 0) : (a) == (b))

#define MATCH_VARDATA(a, b)                                                                                   \
  (((a) != NULL && (b) != NULL)                                                                                 \
       ? (varDataLen((a)) == varDataLen((b)) && memcmp(varDataVal((a)), varDataVal((b)), varDataLen((a))) == 0) \
       : (a) == (b))

#define MATCH_STRING_FIELD(fldname)                          \
  do {                                                         \
    if (!MATCH_STRING(p->fldname, pSub->fldname)) return false; \
  } while (0)

#define MATCH_VARDATA_FIELD(fldname)                          \
  do {                                                          \
    if (!MATCH_VARDATA(p->fldname, pSub->fldname)) return false; \
  } while (0)

#define MATCH_OBJECT_FIELD(fldname, matchFunc)          \
  do {                                                    \
    if (!matchFunc(p->fldname, pSub->fldname)) return false; \
  } while (0)

#define MATCH_NODE_FIELD(fldname)                            \
  do {                                                         \
    if (!nodesMatchNode(pSub->fldname, p->fldname)) return false; \
  } while (0)

#define MATCH_NODE_LIST_FIELD(fldname)                          \
  do {                                                            \
    if (!nodesListMatch(p->fldname, pSub->fldname)) return false; \
  } while (0)

    
bool nodesListMatchExists(const SNodeList* pList, const SNode* pTarget) {
  if (NULL == pList || NULL == pTarget) {
    return false;
  }
  SNode* node = NULL;
  bool exists = false;
  FOREACH(node, pList) {
    if (nodesMatchNode(node, pTarget)) {
      exists = true;
      break;
    }
  }

  return exists;
}

bool nodesListMatch(const SNodeList* pList, const SNodeList* pSubList) {
  if (pList == pSubList) {
    return true;
  }

  if (NULL == pList || NULL == pSubList) {
    return false;
  }

  if (pList->length != pSubList->length) {
    return false;
  }

  SNode* node = NULL;
  bool match = true;
  FOREACH(node, pList) {
    if (!nodesListMatchExists(pSubList, node)) {
      match = false;
      break;
    }
  }
  return match;
}

static bool columnNodeMatch(const SColumnNode* pSub, const SColumnNode* p) {
  if (0 == strcmp(p->colName, pSub->node.aliasName)) {
    return true;
  }
  return false;
}

static bool valueNodeMatch(const SValueNode* pSub, const SValueNode* p) {
  return nodesEqualNode((SNode*)pSub, (SNode*)p);
}

static bool operatorNodeMatch(const SOperatorNode* pSub, const SOperatorNode* p) {
  MATCH_SCALAR_FIELD(opType);
  MATCH_NODE_FIELD(pLeft);
  MATCH_NODE_FIELD(pRight);
  return true;
}

static bool logicConditionNodeMatch(const SLogicConditionNode* pSub, const SLogicConditionNode* p) {
  MATCH_SCALAR_FIELD(condType);
  MATCH_NODE_LIST_FIELD(pParameterList);
  return true;
}

static bool functionNodeMatch(const SFunctionNode* pSub, const SFunctionNode* p) {
  MATCH_SCALAR_FIELD(funcId);
  MATCH_STRING_FIELD(functionName);
  MATCH_NODE_LIST_FIELD(pParameterList);
  return true;
}

static bool whenThenNodeMatch(const SWhenThenNode* pSub, const SWhenThenNode* p) {
  MATCH_NODE_FIELD(pWhen);
  MATCH_NODE_FIELD(pThen);
  return true;
}

static bool caseWhenNodeMatch(const SCaseWhenNode* pSub, const SCaseWhenNode* p) {
  MATCH_NODE_FIELD(pCase);
  MATCH_NODE_FIELD(pElse);
  MATCH_NODE_LIST_FIELD(pWhenThenList);
  return true;
}

bool nodesMatchNode(const SNode* pSub, const SNode* p) {
  if (pSub == p) {
    return true;
  }

  if (NULL == pSub || NULL == p) {
    return false;
  }

  if (nodeType(pSub) != nodeType(p)) {
    return false;
  }

  switch (nodeType(p)) {
    case QUERY_NODE_COLUMN:
      return columnNodeMatch((const SColumnNode*)pSub, (const SColumnNode*)p);
    case QUERY_NODE_VALUE:
      return valueNodeMatch((const SValueNode*)pSub, (const SValueNode*)p);
    case QUERY_NODE_OPERATOR:
      return operatorNodeMatch((const SOperatorNode*)pSub, (const SOperatorNode*)p);
    case QUERY_NODE_LOGIC_CONDITION:
      return logicConditionNodeMatch((const SLogicConditionNode*)pSub, (const SLogicConditionNode*)p);
    case QUERY_NODE_FUNCTION:
      return functionNodeMatch((const SFunctionNode*)pSub, (const SFunctionNode*)p);
    case QUERY_NODE_WHEN_THEN:
      return whenThenNodeMatch((const SWhenThenNode*)pSub, (const SWhenThenNode*)p);
    case QUERY_NODE_CASE_WHEN:
      return caseWhenNodeMatch((const SCaseWhenNode*)pSub, (const SCaseWhenNode*)p);
    case QUERY_NODE_REAL_TABLE:
    case QUERY_NODE_TEMP_TABLE:
    case QUERY_NODE_JOIN_TABLE:
    case QUERY_NODE_GROUPING_SET:
    case QUERY_NODE_ORDER_BY_EXPR:
    case QUERY_NODE_LIMIT:
      return false;
    default:
      break;
  }

  return false;
}
