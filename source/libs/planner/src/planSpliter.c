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

#define SPLIT_FLAG_MASK(n)    (1 << n)

#define SPLIT_FLAG_STS SPLIT_FLAG_MASK(0)

#define SPLIT_FLAG_SET_MASK(val, mask) (val) |= (mask)
#define SPLIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SSplitContext {
  int32_t errCode;
  int32_t groupId;
  bool match;
  void* pInfo;
} SSplitContext;

typedef int32_t (*FMatch)(SSplitContext* pCxt, SSubLogicPlan* pSubplan);
typedef int32_t (*FSplit)(SSplitContext* pCxt);

typedef struct SSplitRule {
  char* pName;
  FMatch matchFunc;
  FSplit splitFunc;
} SSplitRule;

typedef struct SStsInfo {
  SScanLogicNode* pScan;
  SSubLogicPlan* pSubplan;
} SStsInfo;

static SLogicNode* stsMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) && TSDB_SUPER_TABLE == ((SScanLogicNode*)pNode)->pMeta->tableType &&
      SCAN_TYPE_TOPIC != ((SScanLogicNode*)pNode)->scanType) {
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

static int32_t stsMatch(SSplitContext* pCxt, SSubLogicPlan* pSubplan) {
  if (SPLIT_FLAG_TEST_MASK(pSubplan->splitFlag, SPLIT_FLAG_STS)) {
    return TSDB_CODE_SUCCESS;
  }
  SLogicNode* pSplitNode = stsMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    SStsInfo* pInfo = calloc(1, sizeof(SStsInfo));
    CHECK_ALLOC(pInfo, TSDB_CODE_OUT_OF_MEMORY);
    pInfo->pScan = (SScanLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
    pCxt->pInfo = pInfo;
    pCxt->match = true;
    return TSDB_CODE_SUCCESS;
  }
  SNode* pChild;
  FOREACH(pChild, pSubplan->pChildren) {
    int32_t code = stsMatch(pCxt, (SSubLogicPlan*)pChild);
    if (TSDB_CODE_SUCCESS != code || pCxt->match) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static SSubLogicPlan* stsCreateScanSubplan(SSplitContext* pCxt, SScanLogicNode* pScan) {
  SSubLogicPlan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = (SLogicNode*)nodesCloneNode(pScan);
  TSWAP(pSubplan->pVgroupList, ((SScanLogicNode*)pSubplan->pNode)->pVgroupList, SVgroupsInfo*);
  SPLIT_FLAG_SET_MASK(pSubplan->splitFlag, SPLIT_FLAG_STS);
  return pSubplan;
}

static int32_t stsCreateExchangeNode(SSplitContext* pCxt, SSubLogicPlan* pSubplan, SScanLogicNode* pScan) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->node.pTargets = nodesCloneList(pScan->node.pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

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

static int32_t stsSplit(SSplitContext* pCxt) {
  SStsInfo* pInfo = pCxt->pInfo;
  if (NULL == pInfo->pSubplan->pChildren) {
    pInfo->pSubplan->pChildren = nodesMakeList();
    if (NULL == pInfo->pSubplan->pChildren) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  int32_t code = nodesListStrictAppend(pInfo->pSubplan->pChildren, stsCreateScanSubplan(pCxt, pInfo->pScan));
  if (TSDB_CODE_SUCCESS == code) {
    code = stsCreateExchangeNode(pCxt, pInfo->pSubplan, pInfo->pScan);
  }
  ++(pCxt->groupId);
  return code;
}

static const SSplitRule splitRuleSet[] = {
  { .pName = "SuperTableScan", .matchFunc = stsMatch, .splitFunc = stsSplit }
};

static const int32_t splitRuleNum = (sizeof(splitRuleSet) / sizeof(SSplitRule));

int32_t applySplitRule(SSubLogicPlan* pSubplan) {
  SSplitContext cxt = { .errCode = TSDB_CODE_SUCCESS, .groupId = pSubplan->id.groupId + 1, .match = false, .pInfo = NULL };
  bool split = false;
  do {
    split = false;
    for (int32_t i = 0; i < splitRuleNum; ++i) {
      cxt.match = false;
      int32_t code = splitRuleSet[i].matchFunc(&cxt, pSubplan);
      if (TSDB_CODE_SUCCESS == code && cxt.match) {
        code = splitRuleSet[i].splitFunc(&cxt);
        split = true;
      }
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
    }
  } while (split);
  return TSDB_CODE_SUCCESS;
}
