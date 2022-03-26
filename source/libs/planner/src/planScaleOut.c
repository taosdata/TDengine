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

typedef struct SScaleOutContext {
  SPlanContext* pPlanCxt;
  int32_t subplanId;
} SScaleOutContext;

static SLogicSubplan* singleCloneSubLogicPlan(SScaleOutContext* pCxt, SLogicSubplan* pSrc, int32_t level) {
  SLogicSubplan* pDst = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pDst) {
    return NULL;
  }
  pDst->pNode = nodesCloneNode(pSrc->pNode);
  if (NULL == pDst->pNode) {
    nodesDestroyNode(pDst);
    return NULL;
  }
  pDst->subplanType = pSrc->subplanType;
  pDst->level = level;
  pDst->id.queryId = pSrc->id.queryId;
  pDst->id.groupId = pSrc->id.groupId;
  pDst->id.subplanId = pCxt->subplanId++;
  return pDst;
}

static int32_t scaleOutForModify(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  SVnodeModifLogicNode* pNode = (SVnodeModifLogicNode*)pSubplan->pNode;
  size_t numOfVgroups = taosArrayGetSize(pNode->pDataBlocks);
  for (int32_t i = 0; i < numOfVgroups; ++i) {
    SLogicSubplan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
    if (NULL == pNewSubplan) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    ((SVnodeModifLogicNode*)pNewSubplan->pNode)->pVgDataBlocks = (SVgDataBlocks*)taosArrayGetP(pNode->pDataBlocks, i);
    if (TSDB_CODE_SUCCESS != nodesListStrictAppend(pGroup, pNewSubplan)) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t scaleOutForMerge(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  return nodesListStrictAppend(pGroup, singleCloneSubLogicPlan(pCxt, pSubplan, level));
}

static int32_t doSetScanVgroup(SLogicNode* pNode, const SVgroupInfo* pVgroup, bool* pFound) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    pScan->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
    if (NULL == pScan->pVgroupList) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    memcpy(pScan->pVgroupList->vgroups, pVgroup, sizeof(SVgroupInfo));
    *pFound = true;
    return TSDB_CODE_SUCCESS;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    int32_t code = doSetScanVgroup((SLogicNode*)pChild, pVgroup, pFound);
    if (TSDB_CODE_SUCCESS != code || *pFound) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t setScanVgroup(SLogicNode* pNode, const SVgroupInfo* pVgroup) {
  bool found = false;
  return doSetScanVgroup(pNode, pVgroup, &found);
}

static int32_t scaleOutForScan(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  if (pSubplan->pVgroupList && !pCxt->pPlanCxt->streamQuery) {
    int32_t code = TSDB_CODE_SUCCESS;
    for (int32_t i = 0; i < pSubplan->pVgroupList->numOfVgroups; ++i) {
      SLogicSubplan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
      if (NULL == pNewSubplan) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      code = setScanVgroup(pNewSubplan->pNode, pSubplan->pVgroupList->vgroups + i);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListStrictAppend(pGroup, pNewSubplan);
      }
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
    return code;
  } else {
    return scaleOutForMerge(pCxt, pSubplan, level, pGroup);
  }
}

static int32_t pushHierarchicalPlan(SNodeList* pParentsGroup, SNodeList* pCurrentGroup) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool topLevel = (0 == LIST_LENGTH(pParentsGroup));
  SNode* pChild = NULL;
  FOREACH(pChild, pCurrentGroup) {
    if (topLevel) {
      code = nodesListAppend(pParentsGroup, pChild);
    } else {
      SNode* pParent = NULL;
      FOREACH(pParent, pParentsGroup) {
        code = nodesListMakeAppend(&(((SLogicSubplan*)pParent)->pChildren), pChild);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeAppend(&(((SLogicSubplan*)pChild)->pParents), pParent);
        }
      }
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t doScaleOut(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t* pLevel, SNodeList* pParentsGroup) {
  SNodeList* pCurrentGroup = nodesMakeList();
  if (NULL == pCurrentGroup) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  switch (pSubplan->subplanType) {
    case SUBPLAN_TYPE_MERGE:
      code = scaleOutForMerge(pCxt, pSubplan, *pLevel, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_SCAN:
      code = scaleOutForScan(pCxt, pSubplan, *pLevel, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_MODIFY:
      code = scaleOutForModify(pCxt, pSubplan, *pLevel, pCurrentGroup);
      break;
    default:
      break;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pushHierarchicalPlan(pParentsGroup, pCurrentGroup);
    ++(*pLevel);
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild;
    FOREACH(pChild, pSubplan->pChildren) {
      code = doScaleOut(pCxt, (SLogicSubplan*)pChild, pLevel, pCurrentGroup);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pCurrentGroup);
  }

  return code;
}

static SQueryLogicPlan* makeQueryLogicPlan() {
  SQueryLogicPlan* pLogicPlan = (SQueryLogicPlan*)nodesMakeNode(QUERY_NODE_LOGIC_PLAN);
  if (NULL == pLogicPlan) {
    return NULL;
  }
  pLogicPlan->pTopSubplans = nodesMakeList();
  if (NULL == pLogicPlan->pTopSubplans) {
    nodesDestroyNode(pLogicPlan);
    return NULL;
  }
  return pLogicPlan;
}

int32_t scaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan) {
  SQueryLogicPlan* pPlan = makeQueryLogicPlan();
  if (NULL == pPlan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  SScaleOutContext cxt = { .pPlanCxt = pCxt, .subplanId = 1 };
  int32_t code = doScaleOut(&cxt, pLogicSubplan, &(pPlan->totalLevel), pPlan->pTopSubplans);
  if (TSDB_CODE_SUCCESS == code) {
    *pLogicPlan = pPlan;
  } else {
    nodesDestroyNode(pPlan);
  }

  return code;
}
