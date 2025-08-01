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
  int32_t       subplanId;
} SScaleOutContext;

static SLogicSubplan* singleCloneSubLogicPlan(SScaleOutContext* pCxt, SLogicSubplan* pSrc, int32_t level) {
  SLogicSubplan* pDst = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, (SNode**)&pDst);
  if (NULL == pDst) {
    terrno = code;
    return NULL;
  }
  pDst->pNode = NULL;
  code = nodesCloneNode((SNode*)pSrc->pNode, (SNode**)&pDst->pNode);
  if (NULL == pDst->pNode) {
    terrno = code;
    nodesDestroyNode((SNode*)pDst);
    return NULL;
  }
  pDst->subplanType = pSrc->subplanType;
  pDst->level = level;
  pDst->id.queryId = pSrc->id.queryId;
  pDst->id.groupId = pSrc->id.groupId;
  pDst->id.subplanId = pCxt->subplanId++;
  pDst->processOneBlock = pSrc->processOneBlock;
  pDst->dynTbname = pSrc->dynTbname;
  return pDst;
}

static int32_t doSetScanVgroup(SLogicNode* pNode, const SVgroupInfo* pVgroup, bool* pFound) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    SScanLogicNode* pScan = (SScanLogicNode*)pNode;
    if (!pScan->pVgroupList) {
      pScan->pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
      if (NULL == pScan->pVgroupList) {
        return terrno;
      }
    }
    memcpy(pScan->pVgroupList->vgroups, pVgroup, sizeof(SVgroupInfo));
    pScan->pVgroupList->numOfVgroups = 1;
    *pFound = true;
    return TSDB_CODE_SUCCESS;
  } else if (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(pNode)) {
    SDynQueryCtrlLogicNode* pCtrl = (SDynQueryCtrlLogicNode*)pNode;
    if (DYN_QTYPE_VTB_SCAN == pCtrl->qType) {
      pCtrl->vtbScan.pVgroupList = taosMemoryCalloc(1, sizeof(SVgroupsInfo) + sizeof(SVgroupInfo));
      if (NULL == pCtrl->vtbScan.pVgroupList) {
        return terrno;
      }
      memcpy(pCtrl->vtbScan.pVgroupList->vgroups, pVgroup, sizeof(SVgroupInfo));
      pCtrl->vtbScan.pVgroupList->numOfVgroups = 1;
      *pFound = true;
      return TSDB_CODE_SUCCESS;
    }
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

static int32_t scaleOutByVgroups(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pSubplan->pVgroupList->numOfVgroups; ++i) {
    SLogicSubplan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
    if (NULL == pNewSubplan) {
      return terrno;
    }
    code = setScanVgroup(pNewSubplan->pNode, pSubplan->pVgroupList->vgroups + i);
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesListStrictAppend(pGroup, (SNode*)pNewSubplan);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t scaleOutForMerge(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  if (QUERY_NODE_LOGIC_PLAN_DYN_QUERY_CTRL == nodeType(pSubplan->pNode) &&
      ((SDynQueryCtrlLogicNode*)pSubplan->pNode)->qType == DYN_QTYPE_VTB_SCAN) {
    return scaleOutByVgroups(pCxt, pSubplan, level, pGroup);
  } else {
    return nodesListStrictAppend(pGroup, (SNode*)singleCloneSubLogicPlan(pCxt, pSubplan, level));
  }
}

static int32_t scaleOutForInsertValues(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level,
                                       SNodeList* pGroup) {
  SVnodeModifyLogicNode* pNode = (SVnodeModifyLogicNode*)pSubplan->pNode;
  size_t                 numOfVgroups = taosArrayGetSize(pNode->pDataBlocks);
  int32_t code = 0;
  for (int32_t i = 0; i < numOfVgroups; ++i) {
    SLogicSubplan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
    if (NULL == pNewSubplan) {
      return terrno;
    }
    ((SVnodeModifyLogicNode*)pNewSubplan->pNode)->pVgDataBlocks = (SVgDataBlocks*)taosArrayGetP(pNode->pDataBlocks, i);
    if (TSDB_CODE_SUCCESS != (code = nodesListStrictAppend(pGroup, (SNode*)pNewSubplan))) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t scaleOutForInsert(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  SVnodeModifyLogicNode* pNode = (SVnodeModifyLogicNode*)pSubplan->pNode;
  if (NULL == pNode->node.pChildren) {
    return scaleOutForInsertValues(pCxt, pSubplan, level, pGroup);
  }
  return scaleOutForMerge(pCxt, pSubplan, level, pGroup);
}

static int32_t scaleOutForModify(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  SVnodeModifyLogicNode* pNode = (SVnodeModifyLogicNode*)pSubplan->pNode;
  if (MODIFY_TABLE_TYPE_DELETE == pNode->modifyType) {
    return scaleOutByVgroups(pCxt, pSubplan, level, pGroup);
  }
  return scaleOutForInsert(pCxt, pSubplan, level, pGroup);
}

static int32_t scaleOutForScan(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  if (pSubplan->pVgroupList) {
    return scaleOutByVgroups(pCxt, pSubplan, level, pGroup);
  } else {
    return scaleOutForMerge(pCxt, pSubplan, level, pGroup);
  }
}

static int32_t scaleOutForCompute(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pGroup) {
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pSubplan->numOfComputeNodes; ++i) {
    SLogicSubplan* pNewSubplan = singleCloneSubLogicPlan(pCxt, pSubplan, level);
    if (NULL == pNewSubplan) {
      return terrno;
    }
    code = nodesListStrictAppend(pGroup, (SNode*)pNewSubplan);
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  return code;
}

static int32_t pushHierarchicalPlanForCompute(SNodeList* pParentsGroup, SNodeList* pCurrentGroup) {
  SNode*  pChild = NULL;
  SNode*  pParent = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  if (pParentsGroup->length == pCurrentGroup->length) {  
    FORBOTH(pChild, pCurrentGroup, pParent, pParentsGroup) {
      code = nodesListMakeAppend(&(((SLogicSubplan*)pParent)->pChildren), pChild);
      if (TSDB_CODE_SUCCESS == code) {
        code = nodesListMakeAppend(&(((SLogicSubplan*)pChild)->pParents), pParent);
      }
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  } else {
    FOREACH(pChild, pCurrentGroup) {
      SNode* pParent = NULL;
      FOREACH(pParent, pParentsGroup) {
        code = nodesListMakeAppend(&(((SLogicSubplan*)pParent)->pChildren), pChild);
        if (TSDB_CODE_SUCCESS == code) {
          code = nodesListMakeAppend(&(((SLogicSubplan*)pChild)->pParents), pParent);
        }
      }
    }
  }
  
  return code;
}

static bool isComputeGroup(SNodeList* pGroup) {
  if (0 == LIST_LENGTH(pGroup)) {
    return false;
  }
  return SUBPLAN_TYPE_COMPUTE == ((SLogicSubplan*)nodesListGetNode(pGroup, 0))->subplanType;
}

static int32_t pushHierarchicalPlanForNormal(SNodeList* pParentsGroup, SNodeList* pCurrentGroup) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    topLevel = (0 == LIST_LENGTH(pParentsGroup));
  SNode*  pChild = NULL;
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

static int32_t pushHierarchicalPlan(SNodeList* pParentsGroup, SNodeList* pCurrentGroup) {
  if (isComputeGroup(pParentsGroup)) {
    return pushHierarchicalPlanForCompute(pParentsGroup, pCurrentGroup);
  }
  return pushHierarchicalPlanForNormal(pParentsGroup, pCurrentGroup);
}

static int32_t doScaleOut(SScaleOutContext* pCxt, SLogicSubplan* pSubplan, int32_t level, SNodeList* pParentsGroup) {
  SNodeList* pCurrentGroup = NULL;
  int32_t code = nodesMakeList(&pCurrentGroup);
  if (NULL == pCurrentGroup) {
    return code;
  }

  switch (pSubplan->subplanType) {
    case SUBPLAN_TYPE_MERGE:
      code = scaleOutForMerge(pCxt, pSubplan, level, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_SCAN:
      code = scaleOutForScan(pCxt, pSubplan, level, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_MODIFY:
      code = scaleOutForModify(pCxt, pSubplan, level, pCurrentGroup);
      break;
    case SUBPLAN_TYPE_COMPUTE:
      code = scaleOutForCompute(pCxt, pSubplan, level, pCurrentGroup);
      break;
    default:
      break;
  }

  if (TSDB_CODE_SUCCESS == code) {
    code = pushHierarchicalPlan(pParentsGroup, pCurrentGroup);
  }

  if (TSDB_CODE_SUCCESS == code) {
    SNode* pChild;
    FOREACH(pChild, pSubplan->pChildren) {
      code = doScaleOut(pCxt, (SLogicSubplan*)pChild, level + 1, pCurrentGroup);
      if (TSDB_CODE_SUCCESS != code) {
        break;
      }
    }
  }

  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyList(pCurrentGroup);
  } else {
    nodesClearList(pCurrentGroup);
  }

  return code;
}

static SQueryLogicPlan* makeQueryLogicPlan() {
  SQueryLogicPlan* pLogicPlan = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_PLAN, (SNode**)&pLogicPlan);
  if (NULL == pLogicPlan) {
    terrno = code;
    return NULL;
  }
  pLogicPlan->pTopSubplans = NULL;
  code = nodesMakeList(&pLogicPlan->pTopSubplans);
  if (NULL == pLogicPlan->pTopSubplans) {
    nodesDestroyNode((SNode*)pLogicPlan);
    terrno = code;
    return NULL;
  }
  return pLogicPlan;
}

int32_t scaleOutLogicPlan(SPlanContext* pCxt, SLogicSubplan* pLogicSubplan, SQueryLogicPlan** pLogicPlan) {
  SQueryLogicPlan* pPlan = makeQueryLogicPlan();
  if (NULL == pPlan) {
    return terrno;
  }

  SScaleOutContext cxt = {.pPlanCxt = pCxt, .subplanId = 1};
  int32_t          code = doScaleOut(&cxt, pLogicSubplan, 0, pPlan->pTopSubplans);
  if (TSDB_CODE_SUCCESS == code) {
    *pLogicPlan = pPlan;
  } else {
    nodesDestroyNode((SNode*)pPlan);
  }

  return code;
}
