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

#include "planTestUtil.h"
#include "planner.h"
#include "filter.h"
#include "scalar.h"
#include "executor.h"
#include "ttime.h"
#include "os.h"

using namespace std;

class PlanSubqeuryTest : public PlannerTestBase {};

static EDealRes findCorrelatedCall(SNode* pNode, void* pContext) {
  if (QUERY_NODE_CORRELATED_SUBQUERY == nodeType(pNode)) {
    *(const SCorrelatedSubqueryNode**)pContext = (const SCorrelatedSubqueryNode*)pNode;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes findCorrelationParam(SNode* pNode, void* pContext) {
  if (QUERY_NODE_CORRELATION_PARAM == nodeType(pNode)) {
    *(const SCorrelationParamNode**)pContext = (const SCorrelationParamNode*)pNode;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

struct BoundTimeParam {
  int64_t value;
  int32_t code;
};

static EDealRes bindTimeRangeParam(SNode** ppNode, void* pContext) {
  if (QUERY_NODE_CORRELATION_PARAM != nodeType(*ppNode)) {
    return DEAL_RES_CONTINUE;
  }
  BoundTimeParam* pBound = (BoundTimeParam*)pContext;
  SNode* pValue = nullptr;
  pBound->code = nodesMakeValueNodeFromTimestamp(pBound->value, &pValue);
  if (TSDB_CODE_SUCCESS != pBound->code) {
    return DEAL_RES_ERROR;
  }
  ((SValueNode*)pValue)->node.resType = ((SExprNode*)*ppNode)->resType;
  nodesDestroyNode(*ppNode);
  *ppNode = pValue;
  return DEAL_RES_IGNORE_CHILD;
}

static EDealRes findTimestampValue(SNode* pNode, void* pContext) {
  if (QUERY_NODE_VALUE == nodeType(pNode) &&
      TSDB_DATA_TYPE_TIMESTAMP == ((SValueNode*)pNode)->node.resType.type) {
    *(const SValueNode**)pContext = (const SValueNode*)pNode;
    return DEAL_RES_END;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes countCorrelatedCall(SNode* pNode, void* pContext) {
  if (QUERY_NODE_CORRELATED_SUBQUERY == nodeType(pNode)) {
    ++*(int32_t*)pContext;
    return DEAL_RES_IGNORE_CHILD;
  }
  return DEAL_RES_CONTINUE;
}

static void countProjectCallsInTree(const SPhysiNode* pNode, int32_t* pCount) {
  if (NULL == pNode) {
    return;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == nodeType((const SNode*)pNode)) {
    nodesWalkExprs(((const SProjectPhysiNode*)pNode)->pProjections, countCorrelatedCall, pCount);
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    countProjectCallsInTree((const SPhysiNode*)pChild, pCount);
  }
}

static int32_t countProjectCallsInPlan(const SQueryPlan* pPlan) {
  int32_t count = 0;
  SNode*  pLevel = NULL;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pSubplan = NULL;
    FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
      countProjectCallsInTree(((const SSubplan*)pSubplan)->pNode, &count);
    }
  }
  SNode* pChildPlan = NULL;
  FOREACH(pChildPlan, pPlan->pChildren) {
    count += countProjectCallsInPlan((const SQueryPlan*)pChildPlan);
  }
  return count;
}

static const SProjectPhysiNode* findProjectWithCorrelationParamInTree(const SPhysiNode* pNode) {
  if (NULL == pNode) {
    return NULL;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_PROJECT == nodeType((const SNode*)pNode)) {
    const SProjectPhysiNode* pProject = (const SProjectPhysiNode*)pNode;
    const SCorrelationParamNode* pParam = NULL;
    nodesWalkExprs(pProject->pProjections, findCorrelationParam, &pParam);
    if (NULL != pParam) {
      return pProject;
    }
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    const SProjectPhysiNode* pFound = findProjectWithCorrelationParamInTree((const SPhysiNode*)pChild);
    if (NULL != pFound) {
      return pFound;
    }
  }
  return NULL;
}

static const SProjectPhysiNode* findProjectWithCorrelationParamInPlan(const SQueryPlan* pPlan) {
  SNode* pLevel = NULL;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pSubplan = NULL;
    FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
      const SProjectPhysiNode* pFound = findProjectWithCorrelationParamInTree(((SSubplan*)pSubplan)->pNode);
      if (NULL != pFound) {
        return pFound;
      }
    }
  }
  return NULL;
}

static const SPhysiNode* findPhysiNodeInTree(const SPhysiNode* pNode, ENodeType type) {
  if (NULL == pNode) {
    return NULL;
  }
  if (type == nodeType((const SNode*)pNode)) {
    return pNode;
  }
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    const SPhysiNode* pFound = findPhysiNodeInTree((const SPhysiNode*)pChild, type);
    if (NULL != pFound) {
      return pFound;
    }
  }
  return NULL;
}

static const SPhysiNode* findPhysiNodeInPlan(const SQueryPlan* pPlan, ENodeType type) {
  SNode* pLevel = NULL;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pSubplan = NULL;
    FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
      const SPhysiNode* pFound = findPhysiNodeInTree(((SSubplan*)pSubplan)->pNode, type);
      if (NULL != pFound) {
        return pFound;
      }
    }
  }
  return NULL;
}

static int32_t countPhysiNodesInTree(const SPhysiNode* pNode, ENodeType type) {
  if (NULL == pNode) {
    return 0;
  }
  int32_t count = type == nodeType((const SNode*)pNode);
  SNode* pChild = NULL;
  FOREACH(pChild, pNode->pChildren) {
    count += countPhysiNodesInTree((const SPhysiNode*)pChild, type);
  }
  return count;
}

static int32_t countPhysiNodesInPlan(const SQueryPlan* pPlan, ENodeType type) {
  int32_t count = 0;
  SNode*  pLevel = NULL;
  FOREACH(pLevel, pPlan->pSubplans) {
    SNode* pSubplan = NULL;
    FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
      count += countPhysiNodesInTree(((SSubplan*)pSubplan)->pNode, type);
    }
  }
  return count;
}

TEST_F(PlanSubqeuryTest, basic) {
  useDb("root", "test");

  run("SELECT * FROM (SELECT * FROM t1)");

  run("SELECT LAST(c1) FROM (SELECT * FROM t1)");

  run("SELECT c1 FROM (SELECT TODAY() AS c1 FROM t1)");

  run("SELECT NOW() FROM t1");

  run("SELECT NOW() FROM (SELECT * FROM t1)");

  run("SELECT * FROM (SELECT NOW() FROM t1)");

  run("SELECT NOW() FROM (SELECT * FROM t1) ORDER BY ts");

  run("SELECT * FROM (SELECT AVG(c1) a FROM st1 INTERVAL(10s)) WHERE a > 1");
}

TEST_F(PlanSubqeuryTest, correlatedScalarTemplate) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_NE(nullptr, pPlan);
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                EXPECT_EQ(0, LIST_LENGTH(pPlan->pChildren));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                EXPECT_GT(pTemplate->scopeId, 0);
                EXPECT_GT(pTemplate->planLen, 0);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                const SQueryPlan* pInner = (const SQueryPlan*)decoded.get();
                EXPECT_GT(pInner->numOfSubplans, 0);
                EXPECT_EQ(0, LIST_LENGTH(pInner->pChildren));

                const SNodeListNode* pOuterLevel = (const SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
                ASSERT_NE(nullptr, pOuterLevel);
                const SSubplan* pOuterSubplan = (const SSubplan*)nodesListGetNode(pOuterLevel->pNodeList, 0);
                ASSERT_NE(nullptr, pOuterSubplan);
                ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_PROJECT, nodeType((SNode*)pOuterSubplan->pNode));
                const SProjectPhysiNode* pProject = (const SProjectPhysiNode*)pOuterSubplan->pNode;
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExprs(pProject->pProjections, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                ASSERT_EQ(1, LIST_LENGTH(pCall->pParamBindings));
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                ASSERT_EQ(CORRELATION_SOURCE_BLOCK, pBinding->sourceKind);
                ASSERT_EQ(QUERY_NODE_COLUMN, nodeType(pBinding->pSource));
                const SColumnNode* pSource = (const SColumnNode*)pBinding->pSource;
                EXPECT_EQ(pSource->dataBlockId, pBinding->sourceBlockId);
                EXPECT_EQ(pSource->slotId, pBinding->sourceSlotId);
                EXPECT_NE(pProject->node.pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);

                int32_t outputSlots = 0;
                SNode* pSlot = nullptr;
                FOREACH(pSlot, pProject->node.pOutputDataBlockDesc->pSlots) {
                  outputSlots += ((SSlotDescNode*)pSlot)->output;
                }
                EXPECT_EQ(1, outputSlots);
              });
}

TEST_F(PlanSubqeuryTest, nestedCorrelationTemplateBelongsToNoncorrelatedChild) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT SUM(h.c1 + (SELECT AVG(c.c1) FROM st1s1 c "
              "WHERE c.c1 = h.c1)) FROM t1 h) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                EXPECT_EQ(0, LIST_LENGTH(pPlan->pCorrelationTemplates));
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pChildren));
                const SQueryPlan* pChild = (const SQueryPlan*)nodesListGetNode(pPlan->pChildren, 0);
                ASSERT_NE(nullptr, pChild);
                ASSERT_EQ(1, LIST_LENGTH(pChild->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pChild->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                EXPECT_EQ(0, pTemplate->templateId);
              });
}

TEST_F(PlanSubqeuryTest, correlatedTimeRangeDescriptorOnlyForDerivedRange) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT _qstart FROM st1s1 h WHERE h.ts >= r.ts LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                ASSERT_GT(LIST_LENGTH(pTemplate->pTimeRangeInit), 0);
                const SCorrelationTimeRangeInitNode* pInit =
                    (const SCorrelationTimeRangeInitNode*)nodesListGetNode(pTemplate->pTimeRangeInit, 0);
                ASSERT_NE(nullptr, pInit);
                EXPECT_EQ(pTemplate->scopeId, pInit->scopeId);
                EXPECT_EQ(QUERY_NODE_PHYSICAL_PLAN_PROJECT, pInit->targetNodeType);
                ASSERT_NE(nullptr, pInit->pRangeExpr);
                const SCorrelationParamNode* pParam = nullptr;
                nodesWalkExpr(pInit->pRangeExpr, findCorrelationParam, &pParam);
                EXPECT_NE(nullptr, pParam);
                char* pMsg = nullptr;
                int32_t msgLen = 0;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg((const SNode*)pInit, &pMsg, &msgLen));
                std::unique_ptr<char, void (*)(char*)> msg(pMsg, [](char* p) { taosMemoryFree(p); });
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pMsg, msgLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                EXPECT_TRUE(nodesEqualNode((const SNode*)pInit, pDecoded));
                char* pJson = nullptr;
                int32_t jsonLen = 0;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString((const SNode*)pInit, false, &pJson, &jsonLen));
                std::unique_ptr<char, void (*)(char*)> json(pJson, [](char* p) { taosMemoryFree(p); });
                SNode* pFromJson = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(pJson, &pFromJson));
                std::unique_ptr<SNode, void (*)(SNode*)> fromJson(pFromJson, nodesDestroyNode);
                EXPECT_TRUE(nodesEqualNode((const SNode*)pInit, pFromJson));
              });
  runFullPlan("SELECT (SELECT h.c1 FROM st1s1 h WHERE h.ts >= r.ts LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                EXPECT_EQ(0, LIST_LENGTH(pTemplate->pTimeRangeInit));
              });
}

TEST_F(PlanSubqeuryTest, correlatedTimeRangeInitializesPrivateInstances) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT _qstart FROM st1s1 h WHERE h.ts >= r.ts LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                for (int64_t bound : {1000LL, 2000LL}) {
                  SNodeList* pInitList = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneList(pTemplate->pTimeRangeInit, &pInitList));
                  std::unique_ptr<SNodeList, void (*)(SNodeList*)> inits(pInitList, nodesDestroyList);
                  BoundTimeParam param = {};
                  param.value = bound;
                  param.code = TSDB_CODE_SUCCESS;
                  SNode* pInit = nullptr;
                  FOREACH(pInit, pInitList) {
                    nodesRewriteExpr(&((SCorrelationTimeRangeInitNode*)pInit)->pRangeExpr,
                                     bindTimeRangeParam, &param);
                  }
                  ASSERT_EQ(TSDB_CODE_SUCCESS, param.code);
                  const SCorrelationTimeRangeInitNode* pDescriptor =
                      (const SCorrelationTimeRangeInitNode*)nodesListGetNode(pInitList, 0);
                  ASSERT_NE(nullptr, pDescriptor);
                  SSubplan* pPrivate = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&pPrivate));
                  std::unique_ptr<SNode, void (*)(SNode*)> privatePlan((SNode*)pPrivate, nodesDestroyNode);
                  pPrivate->id = pDescriptor->targetSubplanId;
                  SProjectPhysiNode* pProject = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS,
                            nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, (SNode**)&pProject));
                  pPrivate->pNode = (SPhysiNode*)pProject;
                  ASSERT_EQ(TSDB_CODE_SUCCESS,
                            nodesMakeNode(QUERY_NODE_DATABLOCK_DESC,
                                          (SNode**)&pProject->node.pOutputDataBlockDesc));
                  pProject->node.pOutputDataBlockDesc->dataBlockId = pDescriptor->targetBlockId;
                  SNode* pValue = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromTimestamp(0, &pValue));
                  ((SValueNode*)pValue)->flag |= VALUE_FLAG_DEFERRED_QSTART;
                  ASSERT_EQ(TSDB_CODE_SUCCESS,
                            nodesListMakeStrictAppend(&pProject->pProjections, pValue));
                  SNode* pDurationValue = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt64(0, &pDurationValue));
                  ((SValueNode*)pDurationValue)->flag |= VALUE_FLAG_DEFERRED_QDURATION;
                  ASSERT_EQ(TSDB_CODE_SUCCESS,
                            nodesListMakeStrictAppend(&pProject->pProjections, pDurationValue));
                  ASSERT_EQ(TSDB_CODE_SUCCESS, qInitCorrelationTimeRange(pPrivate, pInitList));
                  const SValueNode* pQstart = nullptr;
                  nodesWalkExprs(pProject->pProjections, findTimestampValue, &pQstart);
                  ASSERT_NE(nullptr, pQstart);
                  EXPECT_EQ(bound, pQstart->datum.i);
                  EXPECT_EQ(0, pQstart->flag & VALUE_FLAG_DEFERRED_QUERY_TIME);
                  const SValueNode* pDuration = (const SValueNode*)nodesListGetNode(pProject->pProjections, 1);
                  EXPECT_TRUE(pDuration->isNull);
                  EXPECT_EQ(0, pDuration->flag & VALUE_FLAG_DEFERRED_QUERY_TIME);
                }
              });
}

TEST_F(PlanSubqeuryTest, correlatedDurationUsesClosedIntervalPrecision) {
  useDb("root", "test");
  auto checkDuration = [](const SQueryPlan* pPlan, int64_t bound, int64_t expected) {
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                ASSERT_GT(LIST_LENGTH(pTemplate->pTimeRangeInit), 0);
                SNode* pInitNode = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS,
                          nodesCloneNode(nodesListGetNode(pTemplate->pTimeRangeInit, 0), &pInitNode));
                SNodeList* pInitList = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInitList, pInitNode));
                std::unique_ptr<SNodeList, void (*)(SNodeList*)> inits(pInitList, nodesDestroyList);
                SCorrelationTimeRangeInitNode* pInit = (SCorrelationTimeRangeInitNode*)pInitNode;
                BoundTimeParam param = {};
                param.value = bound;
                param.code = TSDB_CODE_SUCCESS;
                nodesRewriteExpr(&pInit->pRangeExpr, bindTimeRangeParam, &param);
                ASSERT_EQ(TSDB_CODE_SUCCESS, param.code);

                SSubplan* pPrivate = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&pPrivate));
                std::unique_ptr<SNode, void (*)(SNode*)> privatePlan((SNode*)pPrivate, nodesDestroyNode);
                pPrivate->id = pInit->targetSubplanId;
                SProjectPhysiNode* pProject = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS,
                          nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PROJECT, (SNode**)&pProject));
                pPrivate->pNode = (SPhysiNode*)pProject;
                ASSERT_EQ(TSDB_CODE_SUCCESS,
                          nodesMakeNode(QUERY_NODE_DATABLOCK_DESC,
                                        (SNode**)&pProject->node.pOutputDataBlockDesc));
                pProject->node.pOutputDataBlockDesc->dataBlockId = pInit->targetBlockId;
                SNode* pValue = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt64(0, &pValue));
                ((SValueNode*)pValue)->flag |= VALUE_FLAG_DEFERRED_QDURATION;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pProject->pProjections, pValue));
                ASSERT_EQ(TSDB_CODE_SUCCESS, qInitCorrelationTimeRange(pPrivate, pInitList));
                const SValueNode* pDuration = (const SValueNode*)nodesListGetNode(pProject->pProjections, 0);
                EXPECT_FALSE(pDuration->isNull);
                EXPECT_EQ(expected, pDuration->datum.i);
              };
  runFullPlan("SELECT (SELECT _qduration FROM st1s1 h WHERE h.ts >= r.ts AND h.ts < r.ts + 100000 "
              "LIMIT 1) FROM t1 r",
              [checkDuration](const SQueryPlan* pPlan) { checkDuration(pPlan, 1000, 100000); });
  runFullPlan("SELECT (SELECT _qduration FROM st1s1 h WHERE h.ts >= r.ts AND h.ts < 1000 "
              "LIMIT 1) FROM t1 r",
              [checkDuration](const SQueryPlan* pPlan) { checkDuration(pPlan, 2000, 2); });
}

TEST_F(PlanSubqeuryTest, correlatedIntervalAndFillCarryTimeRangeDescriptors) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT COUNT(*) FROM st1s1 h WHERE h.ts >= r.ts AND h.ts < r.ts + 100000 "
              "INTERVAL(10s, AUTO) FILL(PREV) LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                bool hasInterval = false;
                bool hasFill = false;
                SNode* pInitNode = nullptr;
                FOREACH(pInitNode, pTemplate->pTimeRangeInit) {
                  const SCorrelationTimeRangeInitNode* pInit =
                      (const SCorrelationTimeRangeInitNode*)pInitNode;
                  hasInterval |= pInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL ||
                                 pInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL;
                  hasFill |= pInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_FILL;
                  EXPECT_EQ(pTemplate->scopeId, pInit->scopeId);
                  EXPECT_NE(nullptr, pInit->pRangeExpr);
                }
                EXPECT_TRUE(hasInterval);
                EXPECT_TRUE(hasFill);
              });
}

TEST_F(PlanSubqeuryTest, correlatedInterpAndFillCarryRangeDescriptors) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT INTERP(h.c1) FROM st1s1 h RANGE(r.ts, r.ts + 100000) "
              "EVERY(10s) FILL(LINEAR) LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                const SCorrelationTimeRangeInitNode* pInterpInit = nullptr;
                SNode* pInitNode = nullptr;
                FOREACH(pInitNode, pTemplate->pTimeRangeInit) {
                  const SCorrelationTimeRangeInitNode* pInit =
                      (const SCorrelationTimeRangeInitNode*)pInitNode;
                  EXPECT_TRUE(pInit->interpRange);
                  if (pInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC) {
                    pInterpInit = pInit;
                  }
                }
                ASSERT_NE(nullptr, pInterpInit);
                SNode* pClone = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode((const SNode*)pInterpInit, &pClone));
                SNodeList* pInitList = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInitList, pClone));
                std::unique_ptr<SNodeList, void (*)(SNodeList*)> inits(pInitList, nodesDestroyList);
                SCorrelationTimeRangeInitNode* pInit = (SCorrelationTimeRangeInitNode*)pClone;
                BoundTimeParam param = {};
                param.value = 123456;
                param.code = TSDB_CODE_SUCCESS;
                nodesRewriteExpr(&pInit->pRangeExpr, bindTimeRangeParam, &param);
                ASSERT_EQ(TSDB_CODE_SUCCESS, param.code);
                SSubplan* pPrivate = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&pPrivate));
                std::unique_ptr<SNode, void (*)(SNode*)> privatePlan((SNode*)pPrivate, nodesDestroyNode);
                pPrivate->id = pInit->targetSubplanId;
                SInterpFuncPhysiNode* pInterp = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS,
                          nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, (SNode**)&pInterp));
                pPrivate->pNode = (SPhysiNode*)pInterp;
                ASSERT_EQ(TSDB_CODE_SUCCESS,
                          nodesMakeNode(QUERY_NODE_DATABLOCK_DESC,
                                        (SNode**)&pInterp->node.pOutputDataBlockDesc));
                pInterp->node.pOutputDataBlockDesc->dataBlockId = pInit->targetBlockId;
                pInterp->fillMode = FILL_MODE_LINEAR;
                pInterp->interval = 10000;
                pInterp->intervalUnit = TIME_UNIT_SECOND;
                ASSERT_EQ(TSDB_CODE_SUCCESS, qInitCorrelationTimeRange(pPrivate, pInitList));
                EXPECT_EQ(123456, pInterp->timeRange.skey);
                EXPECT_EQ(223456, pInterp->timeRange.ekey);
              });
}

TEST_F(PlanSubqeuryTest, correlatedWindowAndFillInitializeAfterBinding) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT COUNT(*) FROM st1s1 h WHERE h.ts >= r.ts AND h.ts < r.ts + 100000 "
              "INTERVAL(10s, AUTO) FILL(PREV) LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                int32_t initialized = 0;
                SNode* pSource = nullptr;
                FOREACH(pSource, pTemplate->pTimeRangeInit) {
                  const SCorrelationTimeRangeInitNode* pSourceInit =
                      (const SCorrelationTimeRangeInitNode*)pSource;
                  bool isFill = pSourceInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_FILL;
                  bool isInterval = pSourceInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL ||
                                    pSourceInit->targetNodeType == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL;
                  if (!isFill && !isInterval) {
                    continue;
                  }
                  SNode* pClone = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(pSource, &pClone));
                  SNodeList* pInitList = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pInitList, pClone));
                  std::unique_ptr<SNodeList, void (*)(SNodeList*)> inits(pInitList, nodesDestroyList);
                  SCorrelationTimeRangeInitNode* pInit = (SCorrelationTimeRangeInitNode*)pClone;
                  BoundTimeParam param = {};
                  param.value = 123456;
                  param.code = TSDB_CODE_SUCCESS;
                  nodesRewriteExpr(&pInit->pRangeExpr, bindTimeRangeParam, &param);
                  ASSERT_EQ(TSDB_CODE_SUCCESS, param.code);

                  SSubplan* pPrivate = nullptr;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, (SNode**)&pPrivate));
                  std::unique_ptr<SNode, void (*)(SNode*)> privatePlan((SNode*)pPrivate, nodesDestroyNode);
                  pPrivate->id = pInit->targetSubplanId;
                  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(pInit->targetNodeType, (SNode**)&pPrivate->pNode));
                  ASSERT_EQ(TSDB_CODE_SUCCESS,
                            nodesMakeNode(QUERY_NODE_DATABLOCK_DESC,
                                          (SNode**)&pPrivate->pNode->pOutputDataBlockDesc));
                  pPrivate->pNode->pOutputDataBlockDesc->dataBlockId = pInit->targetBlockId;
                  if (isFill) {
                    SFillPhysiNode* pFill = (SFillPhysiNode*)pPrivate->pNode;
                    pFill->mode = FILL_MODE_PREV;
                    pFill->interval = pFill->sliding = 10000;
                    pFill->intervalUnit = pFill->slidingUnit = TIME_UNIT_SECOND;
                    pFill->offset = AUTO_DURATION_VALUE;
                  } else {
                    SIntervalPhysiNode* pInterval = (SIntervalPhysiNode*)pPrivate->pNode;
                    pInterval->interval = pInterval->sliding = 10000;
                    pInterval->intervalUnit = pInterval->slidingUnit = TIME_UNIT_SECOND;
                    pInterval->offset = AUTO_DURATION_VALUE;
                  }
                  ASSERT_EQ(TSDB_CODE_SUCCESS, qInitCorrelationTimeRange(pPrivate, pInitList));
                  const STimeWindow* pRange = isFill ? &((SFillPhysiNode*)pPrivate->pNode)->timeRange
                                                     : &((SIntervalPhysiNode*)pPrivate->pNode)->timeRange;
                  EXPECT_EQ(123456, pRange->skey);
                  EXPECT_EQ(223455, pRange->ekey);
                  int64_t offset = isFill ? ((SFillPhysiNode*)pPrivate->pNode)->offset
                                          : ((SIntervalPhysiNode*)pPrivate->pNode)->offset;
                  EXPECT_NE(AUTO_DURATION_VALUE, offset);
                  ++initialized;
                }
                EXPECT_GE(initialized, 2);
              });
}

TEST_F(PlanSubqeuryTest, deferredFillRangeChecksUseBoundPrecision) {
  for (int8_t precision : {TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO,
                           TSDB_TIME_PRECISION_NANO}) {
    int64_t ticksPerMs = precision == TSDB_TIME_PRECISION_MILLI ? 1 :
                         precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1000000;
    int64_t interval = 10000 * ticksPerMs;
    STimeWindow finite = {};
    finite.skey = 0;
    finite.ekey = interval * 10 - 1;
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              filterValidateFillTimeRange(&finite, interval, TIME_UNIT_SECOND, precision, false));
    STimeWindow tooLarge = {};
    tooLarge.skey = 0;
    tooLarge.ekey = interval * MAX_INTERVAL_TIME_WINDOW;
    EXPECT_EQ(TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE,
              filterValidateFillTimeRange(&tooLarge, interval, TIME_UNIT_SECOND, precision, false));
  }
  // TSWINDOW_*_INITIALIZER are C compound literals, which MSVC rejects in C++
  // (C4576), so spell the same values out here.
  STimeWindow unknown = {INT64_MIN, INT64_MAX};
  EXPECT_EQ(TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE,
            filterValidateFillTimeRange(&unknown, 10000, TIME_UNIT_SECOND, TSDB_TIME_PRECISION_MILLI, false));
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            filterValidateFillTimeRange(&unknown, 10000, TIME_UNIT_SECOND, TSDB_TIME_PRECISION_MILLI, true));
  STimeWindow empty = {INT64_MAX, INT64_MIN};
  EXPECT_EQ(TSDB_CODE_SUCCESS,
            filterValidateFillTimeRange(&empty, 10000, TIME_UNIT_SECOND, TSDB_TIME_PRECISION_MILLI, false));
}

TEST_F(PlanSubqeuryTest, correlatedSortKeepsOwnWorkStage) {
  useDb("root", "test");
  runFullPlan("SELECT r.c1 FROM t1 r ORDER BY "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1)",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SSortPhysiNode* pSort =
                    (const SSortPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_SORT);
                ASSERT_NE(nullptr, pSort);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExprs(pSort->pExprs, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                ASSERT_EQ(1, LIST_LENGTH(pCall->pParamBindings));
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                EXPECT_EQ(CORRELATION_SOURCE_BLOCK, pBinding->sourceKind);
                EXPECT_NE(pSort->node.pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);
              });
}

TEST_F(PlanSubqeuryTest, correlatedNestedTemplateInheritsAncestorParam) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT (SELECT z.c1 FROM t1 z WHERE z.c1 = r.c1 LIMIT 1) "
              "FROM st1s1 h LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pParent =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pParent);
                ASSERT_EQ(1, LIST_LENGTH(pParent->pNestedTemplates));
                const SCorrelationTemplateNode* pChild =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pParent->pNestedTemplates, 0);
                ASSERT_NE(nullptr, pChild);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pParent->pPlan, pParent->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                const SQueryPlan* pParentPlan = (const SQueryPlan*)decoded.get();
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pParentPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                ASSERT_NE(nullptr, pProject);
                const SCorrelatedSubqueryNode* pNestedCall = nullptr;
                nodesWalkExprs(pProject->pProjections, findCorrelatedCall, &pNestedCall);
                ASSERT_NE(nullptr, pNestedCall);
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pNestedCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                EXPECT_EQ(CORRELATION_SOURCE_INHERITED, pBinding->sourceKind);
                EXPECT_EQ(0, pBinding->inheritedParamId);
                EXPECT_EQ(nullptr, pBinding->pSource);
              });
}

TEST_F(PlanSubqeuryTest, correlatedNoFromTemplateKeepsVirtualInput) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT r.c1) FROM t1 r", [](const SQueryPlan* pPlan) {
    ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
    const SCorrelationTemplateNode* pTemplate =
        (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
    ASSERT_NE(nullptr, pTemplate);
    SNode* pDecoded = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
    std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
    const SQueryPlan* pInner = (const SQueryPlan*)decoded.get();
    const SProjectPhysiNode* pProject =
        (const SProjectPhysiNode*)findPhysiNodeInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
    ASSERT_NE(nullptr, pProject);
    EXPECT_EQ(0, LIST_LENGTH(pProject->node.pChildren));
    EXPECT_NE(nullptr, findProjectWithCorrelationParamInPlan(pInner));
    EXPECT_EQ(0, LIST_LENGTH(pInner->pChildren));
  });
}

TEST_F(PlanSubqeuryTest, correlatedGroupOutputUsesAggregateResultBlock) {
  useDb("root", "test");
  runFullPlan("SELECT r.c2, (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c2 LIMIT 1) "
              "FROM t1 r GROUP BY r.c2",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                ASSERT_NE(nullptr, pProject);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExprs(pProject->pProjections, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                EXPECT_EQ(CORRELATION_SOURCE_BLOCK, pBinding->sourceKind);
                const SAggPhysiNode* pAgg =
                    (const SAggPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
                ASSERT_NE(nullptr, pAgg);
                EXPECT_EQ(pAgg->node.pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);
              });
}

TEST_F(PlanSubqeuryTest, correlatedHavingUsesGroupOutputBlock) {
  useDb("root", "test");
  runFullPlan("SELECT r.c2 FROM t1 r GROUP BY r.c2 HAVING "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c2 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SAggPhysiNode* pAgg =
                    (const SAggPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
                ASSERT_NE(nullptr, pAgg);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pAgg->node.pConditions, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                EXPECT_EQ(CORRELATION_SOURCE_BLOCK, pBinding->sourceKind);
                EXPECT_EQ(pAgg->node.pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);
              });
}

TEST_F(PlanSubqeuryTest, correlatedPartitionUsesOwnWorkStage) {
  useDb("root", "test");
  runFullPlan("SELECT r.c2 FROM t1 r PARTITION BY "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1)",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SPartitionPhysiNode* pPart =
                    (const SPartitionPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PARTITION);
                ASSERT_NE(nullptr, pPart);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExprs(pPart->pExprs, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                const SPhysiNode* pInput = (const SPhysiNode*)nodesListGetNode(pPart->node.pChildren, 0);
                ASSERT_NE(nullptr, pInput);
                EXPECT_EQ(pInput->pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);
              });
}

TEST_F(PlanSubqeuryTest, correlatedInnerKeepsGroupHavingSortAndLimit) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT h.c2 FROM st1s1 h WHERE h.c1 = r.c1 "
              "GROUP BY h.c2 HAVING MAX(h.c1) > 0 ORDER BY h.c2 LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                const SQueryPlan* pInner = (const SQueryPlan*)decoded.get();
                const SAggPhysiNode* pAgg =
                    (const SAggPhysiNode*)findPhysiNodeInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
                ASSERT_NE(nullptr, pAgg);
                EXPECT_NE(nullptr, pAgg->node.pConditions);
                EXPECT_NE(nullptr, findPhysiNodeInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_SORT));
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                ASSERT_NE(nullptr, pProject);
                EXPECT_NE(nullptr, pProject->node.pLimit);
              });
}

TEST_F(PlanSubqeuryTest, correlatedInnerKeepsDistinct) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT DISTINCT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                const SQueryPlan* pInner = (const SQueryPlan*)decoded.get();
                const SAggPhysiNode* pDistinct =
                    (const SAggPhysiNode*)findPhysiNodeInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG);
                ASSERT_NE(nullptr, pDistinct);
                EXPECT_GT(LIST_LENGTH(pDistinct->pGroupKeys), 0);
              });
}

TEST_F(PlanSubqeuryTest, correlatedMixedAggregateKeepsInnerAggregateInput) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT AVG(h.c1 + r.c1) FROM st1s1 h LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                const SQueryPlan* pInner = (const SQueryPlan*)decoded.get();
                EXPECT_NE(nullptr, findPhysiNodeInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_HASH_AGG));
                EXPECT_EQ(1, LIST_LENGTH(pTemplate->pParams));
              });
}

TEST_F(PlanSubqeuryTest, correlatedAndOrdinarySubqueriesKeepSeparateOwnership) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1), "
              "(SELECT MAX(c1) FROM t1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                EXPECT_EQ(1, LIST_LENGTH(pPlan->pChildren));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                EXPECT_EQ(0, LIST_LENGTH(((SQueryPlan*)decoded.get())->pChildren));
              });
}

TEST_F(PlanSubqeuryTest, ordinaryDependencyInsideCorrelationStaysInRootJob) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT MAX(h.c1) FROM st1s1 h WHERE h.c1 = r.c1 "
              "AND h.c1 > (SELECT MIN(z.c1) FROM t1 z)) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pChildren));
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                EXPECT_EQ(0, LIST_LENGTH(((SQueryPlan*)decoded.get())->pChildren));
              });
}

TEST_F(PlanSubqeuryTest, correlatedCallInsideOrdinarySubqueryUsesOrdinaryPlan) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT (SELECT z.c1 FROM t1 z WHERE z.c1 = h.c1 LIMIT 1) "
              "FROM st1s1 h LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pChildren));
                EXPECT_EQ(0, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SQueryPlan* pOrdinary = (const SQueryPlan*)nodesListGetNode(pPlan->pChildren, 0);
                ASSERT_NE(nullptr, pOrdinary);
                ASSERT_EQ(1, LIST_LENGTH(pOrdinary->pCorrelationTemplates));
                const SCorrelationTemplateNode* pNested =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pOrdinary->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pNested);
                EXPECT_EQ(0, LIST_LENGTH(pNested->pNestedTemplates));
              });
}

TEST_F(PlanSubqeuryTest, correlatedFilterRetainsHiddenParameterColumn) {
  useDb("root", "test");
  runFullPlan("SELECT r.c2 FROM t1 r WHERE "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pScan);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pScan->node.pConditions, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                EXPECT_EQ(CORRELATION_SOURCE_BLOCK, pBinding->sourceKind);
                EXPECT_EQ(pScan->node.pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                ASSERT_NE(nullptr, pProject);
                int32_t outputSlots = 0;
                SNode* pSlot = nullptr;
                FOREACH(pSlot, pProject->node.pOutputDataBlockDesc->pSlots) {
                  outputSlots += ((SSlotDescNode*)pSlot)->output;
                }
                EXPECT_EQ(1, outputSlots);
              });
}

TEST_F(PlanSubqeuryTest, correlatedDistributedOuterUsesActualInputBlock) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT h.c1 FROM t1 h WHERE h.c1 = r.c1 LIMIT 1) FROM st1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                ASSERT_NE(nullptr, pProject);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExprs(pProject->pProjections, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                const SCorrelationParamBindingNode* pBinding =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                ASSERT_NE(nullptr, pBinding);
                const SPhysiNode* pInput = (const SPhysiNode*)nodesListGetNode(pProject->node.pChildren, 0);
                ASSERT_NE(nullptr, pInput);
                EXPECT_EQ(pInput->pOutputDataBlockDesc->dataBlockId, pBinding->sourceBlockId);
                EXPECT_GE(pPlan->numOfSubplans, 1);
              });
}

TEST_F(PlanSubqeuryTest, correlatedJoinOnKeepsBothInputIdentities) {
  useDb("root", "test");
  runFullPlan("SELECT a.c1 FROM t1 a JOIN t1 b ON a.ts = b.ts AND "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = a.c1 + b.c1 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SPhysiNode* pJoin = findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN);
                if (NULL == pJoin) {
                  pJoin = findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN);
                }
                ASSERT_NE(nullptr, pJoin);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                SNode* pOnCond = QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == nodeType((const SNode*)pJoin)
                                     ? ((const SHashJoinPhysiNode*)pJoin)->pFullOnCond
                                     : ((const SSortMergeJoinPhysiNode*)pJoin)->pFullOnCond;
                nodesWalkExpr(pOnCond, findCorrelatedCall, &pCall);
                ASSERT_NE(nullptr, pCall);
                ASSERT_EQ(2, LIST_LENGTH(pCall->pParamBindings));
                const SCorrelationParamBindingNode* pLeft =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 0);
                const SCorrelationParamBindingNode* pRight =
                    (const SCorrelationParamBindingNode*)nodesListGetNode(pCall->pParamBindings, 1);
                EXPECT_NE(pLeft->bindingId, pRight->bindingId);
                EXPECT_NE(pLeft->tableRefId, pRight->tableRefId);
                EXPECT_TRUE(pLeft->sourceBlockId != pRight->sourceBlockId ||
                            pLeft->sourceSlotId != pRight->sourceSlotId);
              });
}

TEST_F(PlanSubqeuryTest, correlatedUnionTemplateKeepsBothBranches) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 "
              "UNION ALL SELECT z.c1 FROM t1 z WHERE z.c1 = r.c1 LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                const SCorrelationTemplateNode* pTemplate =
                    (const SCorrelationTemplateNode*)nodesListGetNode(pPlan->pCorrelationTemplates, 0);
                ASSERT_NE(nullptr, pTemplate);
                SNode* pDecoded = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pTemplate->pPlan, pTemplate->planLen, &pDecoded));
                std::unique_ptr<SNode, void (*)(SNode*)> decoded(pDecoded, nodesDestroyNode);
                const SQueryPlan* pInner = (const SQueryPlan*)decoded.get();
                EXPECT_GE(countPhysiNodesInPlan(pInner, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN), 2);
                EXPECT_EQ(0, LIST_LENGTH(pInner->pChildren));
              });
}

TEST_F(PlanSubqeuryTest, doubleGroupBy) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM ("
      "SELECT c1 + c3 a, c1 + COUNT(*) b FROM t1 WHERE c2 = 'abc' GROUP BY c1, c3) "
      "WHERE a > 100 GROUP BY b");
}

TEST_F(PlanSubqeuryTest, innerSetOperator) {
  useDb("root", "test");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t1)");

  run("SELECT c1 FROM (SELECT c1 FROM t1 UNION SELECT c1 FROM t1)");
}

TEST_F(PlanSubqeuryTest, innerFill) {
  useDb("root", "test");

  run("SELECT cnt FROM (SELECT _WSTART ts, COUNT(*) cnt FROM t1 "
      "WHERE ts > '2022-04-01 00:00:00' and ts < '2022-04-30 23:59:59' INTERVAL(10s) FILL(LINEAR)) "
      "WHERE ts > '2022-04-06 00:00:00'");
}

TEST_F(PlanSubqeuryTest, innerOrderBy) {
  useDb("root", "test");

  run("SELECT c2 FROM (SELECT c2 FROM st1 ORDER BY c1, _rowts)");
}

TEST_F(PlanSubqeuryTest, outerInterval) {
  useDb("root", "test");

  run("SELECT COUNT(*) FROM (SELECT * FROM st1) INTERVAL(5s)");

  run("SELECT COUNT(*) + SUM(c1) FROM (SELECT * FROM st1) INTERVAL(5s)");

  run("SELECT COUNT(*) FROM (SELECT ts, TOP(c1, 10) FROM st1s1) INTERVAL(5s)");

  run("SELECT COUNT(*) FROM (SELECT ts, TOP(c1, 1) FROM st1s1) INTERVAL(5s)");
}

TEST_F(PlanSubqeuryTest, outerPartition) {
  useDb("root", "test");

  run("SELECT c1, COUNT(*) FROM (SELECT ts, c1 FROM st1) PARTITION BY c1");
}

TEST_F(PlanSubqeuryTest, correlatedOptimizerKeepsMaterializedCallForRepeatedRead) {
  useDb("root", "test");
  runFullPlan("SELECT x.v + x.v FROM (SELECT "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) AS v FROM t1 r) x",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                EXPECT_EQ(1, countProjectCallsInPlan(pPlan));
              });
  runFullPlan("SELECT (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1), "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) FROM t1 r",
              [](const SQueryPlan* pPlan) {
                ASSERT_EQ(2, LIST_LENGTH(pPlan->pCorrelationTemplates));
                EXPECT_EQ(2, countProjectCallsInPlan(pPlan));
              });
}

TEST_F(PlanSubqeuryTest, correlatedOptimizerKeepsLimitAfterCall) {
  useDb("root", "test");
  runFullPlan("SELECT (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) "
              "FROM t1 r LIMIT 1",
              [](const SQueryPlan* pPlan) {
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pProject);
                ASSERT_NE(nullptr, pScan);
                EXPECT_NE(nullptr, pProject->node.pLimit);
                EXPECT_EQ(nullptr, pScan->node.pLimit);
                EXPECT_EQ(1, countProjectCallsInPlan(pPlan));
              });
  runFullPlan("SELECT r.c2 FROM t1 r WHERE "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) > 0 LIMIT 1",
              [](const SQueryPlan* pPlan) {
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pProject);
                ASSERT_NE(nullptr, pScan);
                EXPECT_NE(nullptr, pProject->node.pLimit);
                EXPECT_EQ(nullptr, pScan->node.pLimit);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pScan->node.pConditions, findCorrelatedCall, &pCall);
                EXPECT_NE(nullptr, pCall);
              });
}

TEST_F(PlanSubqeuryTest, correlatedOptimizerExtractsOnlyFixedAndTimeCondition) {
  useDb("root", "test");
  runFullPlan("SELECT r.c2 FROM t1 r WHERE r.ts >= 1000 AND "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pScan);
                EXPECT_TRUE(IS_TSWINDOW_SPECIFIED(((const STableScanPhysiNode*)pScan)->scanRange));
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pScan->node.pConditions, findCorrelatedCall, &pCall);
                EXPECT_NE(nullptr, pCall);
                SFilterInfo* pFilter = nullptr;
                EXPECT_EQ(TSDB_CODE_SUCCESS,
                          filterInitFromNode(pScan->node.pConditions, &pFilter, FLT_OPTION_NO_REWRITE, nullptr));
                filterFreeInfo(pFilter);
              });
  runFullPlan("SELECT r.c2 FROM t1 r WHERE r.ts >= 1000 OR "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pScan);
                EXPECT_FALSE(IS_TSWINDOW_SPECIFIED(((const STableScanPhysiNode*)pScan)->scanRange));
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pScan->node.pConditions, findCorrelatedCall, &pCall);
                EXPECT_NE(nullptr, pCall);
              });
}

TEST_F(PlanSubqeuryTest, correlatedOptimizerDoesNotFoldCaseAway) {
  useDb("root", "test");
  runFullPlan("SELECT CASE WHEN 1 = 1 THEN 1 ELSE "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) END FROM t1 r",
              [](const SQueryPlan* pPlan) {
                EXPECT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                EXPECT_EQ(1, countProjectCallsInPlan(pPlan));
                const SProjectPhysiNode* pProject =
                    (const SProjectPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_PROJECT);
                ASSERT_NE(nullptr, pProject);
                SNode* pCopy = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(nodesListGetNode(pProject->pProjections, 0), &pCopy));
                SNode* pResult = nullptr;
                ASSERT_EQ(TSDB_CODE_SUCCESS, scalarCalculateConstants(pCopy, &pResult));
                std::unique_ptr<SNode, void (*)(SNode*)> result(pResult, nodesDestroyNode);
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(result.get(), findCorrelatedCall, &pCall);
                EXPECT_NE(nullptr, pCall);
              });
  runFullPlan("SELECT NULL = (SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.c1 LIMIT 1) "
              "FROM t1 r",
              [](const SQueryPlan* pPlan) {
                EXPECT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates));
                EXPECT_EQ(1, countProjectCallsInPlan(pPlan));
              });
}

TEST_F(PlanSubqeuryTest, correlatedAggregateCaseKeepsCallBindings) {
  useDb("root", "test");
  runFullPlan("SELECT SUM(CASE WHEN r.c1 > 0 THEN 0 ELSE "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c2 = r.c2) END) FROM t1 r",
              [](const SQueryPlan* pPlan) { EXPECT_EQ(1, LIST_LENGTH(pPlan->pCorrelationTemplates)); });
}

TEST_F(PlanSubqeuryTest, correlatedOptimizerKeepsTagCallOutOfStaticTagFilter) {
  useDb("root", "test");
  runFullPlan("SELECT r.c1 FROM st1 r WHERE r.tag1 > 4 AND "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.tag1 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                SNode* pLevel = nullptr;
                FOREACH(pLevel, pPlan->pSubplans) {
                  SNode* pSubplan = nullptr;
                  FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
                    EXPECT_FALSE(nodesExprHasCorrelation(((SSubplan*)pSubplan)->pTagCond));
                    EXPECT_FALSE(nodesExprHasCorrelation(((SSubplan*)pSubplan)->pTagIndexCond));
                  }
                }
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pScan);
                EXPECT_EQ(COND_TYPE_NORMAL, filterClassifyCondition(pScan->node.pConditions));
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pScan->node.pConditions, findCorrelatedCall, &pCall);
                EXPECT_NE(nullptr, pCall);
              });
  runFullPlan("SELECT r.c1 FROM st1 r WHERE r.tag1 > 4 OR "
              "(SELECT h.c1 FROM st1s1 h WHERE h.c1 = r.tag1 LIMIT 1) > 0",
              [](const SQueryPlan* pPlan) {
                SNode* pLevel = nullptr;
                FOREACH(pLevel, pPlan->pSubplans) {
                  SNode* pSubplan = nullptr;
                  FOREACH(pSubplan, ((SNodeListNode*)pLevel)->pNodeList) {
                    EXPECT_EQ(nullptr, ((SSubplan*)pSubplan)->pTagCond);
                    EXPECT_EQ(nullptr, ((SSubplan*)pSubplan)->pTagIndexCond);
                  }
                }
                const SScanPhysiNode* pScan =
                    (const SScanPhysiNode*)findPhysiNodeInPlan(pPlan, QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN);
                ASSERT_NE(nullptr, pScan);
                EXPECT_EQ(COND_TYPE_NORMAL, filterClassifyCondition(pScan->node.pConditions));
                const SCorrelatedSubqueryNode* pCall = nullptr;
                nodesWalkExpr(pScan->node.pConditions, findCorrelatedCall, &pCall);
                EXPECT_NE(nullptr, pCall);
              });
}
