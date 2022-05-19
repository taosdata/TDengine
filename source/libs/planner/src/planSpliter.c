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

#include "planInt.h"

#define SPLIT_FLAG_MASK(n) (1 << n)

#define SPLIT_FLAG_STS SPLIT_FLAG_MASK(0)
#define SPLIT_FLAG_CTJ SPLIT_FLAG_MASK(1)

#define SPLIT_FLAG_SET_MASK(val, mask)  (val) |= (mask)
#define SPLIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SSplitContext {
  int32_t groupId;
  bool    split;
} SSplitContext;

typedef int32_t (*FSplit)(SSplitContext* pCxt, SLogicSubplan* pSubplan);

typedef struct SSplitRule {
  char*  pName;
  FSplit splitFunc;
} SSplitRule;

typedef struct SStsInfo {
  SScanLogicNode* pScan;
  SLogicSubplan*  pSubplan;
} SStsInfo;

typedef struct SCtjInfo {
  SScanLogicNode* pScan;
  SLogicSubplan*  pSubplan;
} SCtjInfo;

typedef struct SUaInfo {
  SProjectLogicNode* pProject;
  SLogicSubplan*     pSubplan;
} SUaInfo;

typedef struct SUnInfo {
  SAggLogicNode* pAgg;
  SLogicSubplan* pSubplan;
} SUnInfo;

typedef bool (*FSplFindSplitNode)(SLogicSubplan* pSubplan, void* pInfo);

static SLogicSubplan* splCreateScanSubplan(SSplitContext* pCxt, SScanLogicNode* pScan, int32_t flag) {
  SLogicSubplan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = (SLogicNode*)nodesCloneNode(pScan);
  TSWAP(pSubplan->pVgroupList, ((SScanLogicNode*)pSubplan->pNode)->pVgroupList);
  SPLIT_FLAG_SET_MASK(pSubplan->splitFlag, flag);
  return pSubplan;
}

static int32_t splCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SScanLogicNode* pScan,
                                     ESubplanType subplanType) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->precision = pScan->pMeta->tableInfo.precision;
  pExchange->node.pTargets = nodesCloneList(pScan->node.pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  if (NULL == pScan->node.pParent) {
    pSubplan->pNode = (SLogicNode*)pExchange;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pScan->node.pParent->pChildren) {
    if (nodesEqualNode(pNode, pScan)) {
      REPLACE_NODE(pExchange);
      nodesDestroyNode(pNode);
      return TSDB_CODE_SUCCESS;
    }
  }
  nodesDestroyNode(pExchange);
  return TSDB_CODE_FAILED;
}

static bool splMatch(SSplitContext* pCxt, SLogicSubplan* pSubplan, int32_t flag, FSplFindSplitNode func, void* pInfo) {
  if (!SPLIT_FLAG_TEST_MASK(pSubplan->splitFlag, flag)) {
    if (func(pSubplan, pInfo)) {
      return true;
    }
  }
  SNode* pChild;
  FOREACH(pChild, pSubplan->pChildren) {
    if (splMatch(pCxt, (SLogicSubplan*)pChild, flag, func, pInfo)) {
      return true;
    }
  }
  return false;
}

static SLogicNode* stsMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) && NULL != ((SScanLogicNode*)pNode)->pVgroupList &&
      ((SScanLogicNode*)pNode)->pVgroupList->numOfVgroups > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = stsMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool stsFindSplitNode(SLogicSubplan* pSubplan, SStsInfo* pInfo) {
  SLogicNode* pSplitNode = stsMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pScan = (SScanLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t stsSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SStsInfo info = {0};
  if (!splMatch(pCxt, pSubplan, SPLIT_FLAG_STS, (FSplFindSplitNode)stsFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code =
      nodesListMakeStrictAppend(&info.pSubplan->pChildren, splCreateScanSubplan(pCxt, info.pScan, SPLIT_FLAG_STS));
  if (TSDB_CODE_SUCCESS == code) {
    code = splCreateExchangeNode(pCxt, info.pSubplan, info.pScan, SUBPLAN_TYPE_MERGE);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

static bool ctjIsSingleTable(int8_t tableType) {
  return (TSDB_CHILD_TABLE == tableType || TSDB_NORMAL_TABLE == tableType);
}

static SLogicNode* ctjMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pNode)) {
    SLogicNode* pLeft = (SLogicNode*)nodesListGetNode(pNode->pChildren, 0);
    SLogicNode* pRight = (SLogicNode*)nodesListGetNode(pNode->pChildren, 1);
    if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pLeft) && ctjIsSingleTable(((SScanLogicNode*)pLeft)->pMeta->tableType) &&
        QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pRight) &&
        ctjIsSingleTable(((SScanLogicNode*)pRight)->pMeta->tableType)) {
      return pRight;
    }
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = ctjMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool ctjFindSplitNode(SLogicSubplan* pSubplan, SCtjInfo* pInfo) {
  SLogicNode* pSplitNode = ctjMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pScan = (SScanLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t ctjSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SCtjInfo info = {0};
  if (!splMatch(pCxt, pSubplan, SPLIT_FLAG_CTJ, (FSplFindSplitNode)ctjFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code =
      nodesListMakeStrictAppend(&info.pSubplan->pChildren, splCreateScanSubplan(pCxt, info.pScan, SPLIT_FLAG_CTJ));
  if (TSDB_CODE_SUCCESS == code) {
    code = splCreateExchangeNode(pCxt, info.pSubplan, info.pScan, info.pSubplan->subplanType);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

static bool unionIsChildSubplan(SLogicNode* pLogicNode, int32_t groupId) {
  if (QUERY_NODE_LOGIC_PLAN_EXCHANGE == nodeType(pLogicNode)) {
    return ((SExchangeLogicNode*)pLogicNode)->srcGroupId == groupId;
  }

  SNode* pChild;
  FOREACH(pChild, pLogicNode->pChildren) {
    bool isChild = unionIsChildSubplan((SLogicNode*)pChild, groupId);
    if (isChild) {
      return isChild;
    }
  }
  return false;
}

static int32_t unionMountSubplan(SLogicSubplan* pParent, SNodeList* pChildren) {
  SNode* pChild = NULL;
  WHERE_EACH(pChild, pChildren) {
    if (unionIsChildSubplan(pParent->pNode, ((SLogicSubplan*)pChild)->id.groupId)) {
      int32_t code = nodesListMakeAppend(&pParent->pChildren, pChild);
      if (TSDB_CODE_SUCCESS == code) {
        REPLACE_NODE(NULL);
        ERASE_NODE(pChildren);
        continue;
      } else {
        return code;
      }
    }
    WHERE_NEXT;
  }
  return TSDB_CODE_SUCCESS;
}

static SLogicSubplan* unionCreateSubplan(SSplitContext* pCxt, SLogicNode* pNode) {
  SLogicSubplan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = pNode;
  return pSubplan;
}

static int32_t unionSplitSubplan(SSplitContext* pCxt, SLogicSubplan* pUnionSubplan, SLogicNode* pSplitNode) {
  SNodeList* pSubplanChildren = pUnionSubplan->pChildren;
  pUnionSubplan->pChildren = NULL;

  int32_t code = TSDB_CODE_SUCCESS;

  SNode* pChild = NULL;
  FOREACH(pChild, pSplitNode->pChildren) {
    SLogicSubplan* pNewSubplan = unionCreateSubplan(pCxt, (SLogicNode*)pChild);
    code = nodesListMakeStrictAppend(&pUnionSubplan->pChildren, pNewSubplan);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(NULL);
      code = unionMountSubplan(pNewSubplan, pSubplanChildren);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pSubplanChildren);
    DESTORY_LIST(pSplitNode->pChildren);
  }
  return code;
}

static SLogicNode* uaMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = uaMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool uaFindSplitNode(SLogicSubplan* pSubplan, SUaInfo* pInfo) {
  SLogicNode* pSplitNode = uaMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pProject = (SProjectLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t uaCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SProjectLogicNode* pProject) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  // pExchange->precision = pScan->pMeta->tableInfo.precision;
  pExchange->node.pTargets = nodesCloneList(pProject->node.pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  if (NULL == pProject->node.pParent) {
    pSubplan->pNode = (SLogicNode*)pExchange;
    nodesDestroyNode(pProject);
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pProject->node.pParent->pChildren) {
    if (nodesEqualNode(pNode, pProject)) {
      REPLACE_NODE(pExchange);
      nodesDestroyNode(pNode);
      return TSDB_CODE_SUCCESS;
    }
  }
  nodesDestroyNode(pExchange);
  return TSDB_CODE_FAILED;
}

static int32_t uaSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SUaInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)uaFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pProject);
  if (TSDB_CODE_SUCCESS == code) {
    code = uaCreateExchangeNode(pCxt, info.pSubplan, info.pProject);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

static SLogicNode* unMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = unMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static int32_t unCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SAggLogicNode* pAgg) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  // pExchange->precision = pScan->pMeta->tableInfo.precision;
  pExchange->node.pTargets = nodesCloneList(pAgg->pGroupKeys);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  return nodesListMakeAppend(&pAgg->node.pChildren, pExchange);
}

static bool unFindSplitNode(SLogicSubplan* pSubplan, SUnInfo* pInfo) {
  SLogicNode* pSplitNode = unMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pAgg = (SAggLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t unSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SUnInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)unFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pAgg);
  if (TSDB_CODE_SUCCESS == code) {
    code = unCreateExchangeNode(pCxt, info.pSubplan, info.pAgg);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

static const SSplitRule splitRuleSet[] = {{.pName = "SuperTableScan", .splitFunc = stsSplit},
                                          {.pName = "ChildTableJoin", .splitFunc = ctjSplit},
                                          {.pName = "UnionAll", .splitFunc = uaSplit},
                                          {.pName = "Union", .splitFunc = unSplit}};

static const int32_t splitRuleNum = (sizeof(splitRuleSet) / sizeof(SSplitRule));

static int32_t applySplitRule(SLogicSubplan* pSubplan) {
  SSplitContext cxt = {.groupId = pSubplan->id.groupId + 1, .split = false};
  do {
    cxt.split = false;
    for (int32_t i = 0; i < splitRuleNum; ++i) {
      int32_t code = splitRuleSet[i].splitFunc(&cxt, pSubplan);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } while (cxt.split);
  return TSDB_CODE_SUCCESS;
}

static void doSetLogicNodeParent(SLogicNode* pNode, SLogicNode* pParent) {
  pNode->pParent = pParent;
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) { doSetLogicNodeParent((SLogicNode*)pChild, pNode); }
}

static void setLogicNodeParent(SLogicNode* pNode) { doSetLogicNodeParent(pNode, NULL); }

int32_t splitLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SLogicSubplan** pLogicSubplan) {
  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->pNode = nodesCloneNode(pLogicNode);
  if (NULL == pSubplan->pNode) {
    nodesDestroyNode(pSubplan);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIF == nodeType(pLogicNode)) {
    pSubplan->subplanType = SUBPLAN_TYPE_MODIFY;
    TSWAP(((SVnodeModifLogicNode*)pLogicNode)->pDataBlocks, ((SVnodeModifLogicNode*)pSubplan->pNode)->pDataBlocks);
  } else {
    pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = 1;
  setLogicNodeParent(pSubplan->pNode);

  int32_t code = applySplitRule(pSubplan);
  if (TSDB_CODE_SUCCESS == code) {
    *pLogicSubplan = pSubplan;
  } else {
    nodesDestroyNode(pSubplan);
  }

  return code;
}