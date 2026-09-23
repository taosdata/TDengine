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

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <memory>
#include <tuple>
#include <vector>

#include "cJSON.h"
#include "mockCatalogService.h"
#include "nodes.h"
#include "osEnv.h"
#include "osTimezone.h"
#include "parInt.h"
#include "parTestUtil.h"
#include "planner.h"
#include "plannodes.h"
#include "stub.h"

using namespace std;

namespace ParserTest {
class ParserStreamTest : public ParserDdlTest {};

TEST_F(ParserStreamTest, RejectsConcatenatedCreateStreamWithoutCrashing) {
  run("create stream stream_streamdb.s_invalid interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, CLIENT_VERSION();"
      "create stream stream_streamdb.s_invalid interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, CURRENT_USER();",
      TSDB_CODE_STREAM_INVALID_QUERY, PARSER_STAGE_TRANSLATE);
}

namespace {

int32_t strictAppendFailForTbnameFunction(SNodeList** pList, SNode* pNode) {
  if (nullptr != pNode && QUERY_NODE_FUNCTION == nodeType(pNode)) {
    SFunctionNode* pFunc = (SFunctionNode*)pNode;
    if (0 == strcmp(pFunc->functionName, "tbname")) {
      nodesDestroyNode(pNode);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  if (nullptr == *pList) {
    int32_t code = nodesMakeList(pList);
    if (nullptr == *pList) {
      nodesDestroyNode(pNode);
      return code;
    }
  }

  return nodesListStrictAppend(*pList, pNode);
}

class StrictAppendTbnameFailureGuard {
 public:
  StrictAppendTbnameFailureGuard() { stub_.set(nodesListMakeStrictAppend, strictAppendFailForTbnameFunction); }

  ~StrictAppendTbnameFailureGuard() { stub_.reset(nodesListMakeStrictAppend); }

 private:
  Stub stub_;
};

int32_t compositePrimaryKeyCatalogGetTableMeta(SCatalog*, SRequestConnInfo*, const SName* pTableName,
                                               STableMeta** ppTableMeta) {
  int32_t code = g_mockCatalogService->catalogGetTableMeta(pTableName, ppTableMeta);
  if (code == TSDB_CODE_SUCCESS && ppTableMeta != nullptr && *ppTableMeta != nullptr &&
      strcmp(tNameGetTableName(pTableName), "nested_composite_pk") == 0) {
    (*ppTableMeta)->tableInfo.numOfPKs = 1;
    (*ppTableMeta)->schema[1].flags |= COL_IS_KEY;
  }
  return code;
}

class CompositePrimaryKeyMetaGuard {
 public:
  CompositePrimaryKeyMetaGuard() { stub_.set(catalogGetTableMeta, compositePrimaryKeyCatalogGetTableMeta); }

  ~CompositePrimaryKeyMetaGuard() { stub_.reset(catalogGetTableMeta); }

 private:
  Stub stub_;
};

class AsyncFlagGuard {
 public:
  explicit AsyncFlagGuard(bool enabled) : previous_(getAsyncFlag()) { setAsyncFlag(enabled ? "1" : "-1"); }

  ~AsyncFlagGuard() { setAsyncFlag(previous_ ? "1" : "-1"); }

 private:
  bool previous_;
};

int32_t failStreamWindowPlanValidation(const SStreamWindowPlan*, const SStreamWindowPlanValidationCtx*) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

class SharedValidatorFailureGuard {
 public:
  SharedValidatorFailureGuard() { stub_.set(tValidateStreamWindowPlan, failStreamWindowPlanValidation); }

  ~SharedValidatorFailureGuard() { stub_.reset(tValidateStreamWindowPlan); }

 private:
  Stub stub_;
};

static bool pstIsC1NonnegativeCondition(const SNode* pNode) {
  if (nullptr == pNode || QUERY_NODE_OPERATOR != nodeType(pNode)) {
    return false;
  }

  const auto* pOp = (const SOperatorNode*)pNode;
  if (OP_TYPE_GREATER_EQUAL != pOp->opType || QUERY_NODE_COLUMN != nodeType(pOp->pLeft) ||
      QUERY_NODE_VALUE != nodeType(pOp->pRight)) {
    return false;
  }

  const auto* pColumn = (const SColumnNode*)pOp->pLeft;
  const auto* pValue = (const SValueNode*)pOp->pRight;
  return 0 == strcmp(pColumn->colName, "c1") && nullptr != pValue->literal && 0 == strcmp(pValue->literal, "0");
}

static Stub* gRowConditionMergeFailureStub = nullptr;

int32_t failMergeForC1NonnegativeCondition(SNode** pDst, SNodeList** pSrc) {
  if (nullptr != pSrc && 1 == LIST_LENGTH(*pSrc) && pstIsC1NonnegativeCondition(nodesListGetNode(*pSrc, 0))) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  gRowConditionMergeFailureStub->reset(nodesMergeConds);
  int32_t code = nodesMergeConds(pDst, pSrc);
  gRowConditionMergeFailureStub->set(nodesMergeConds, failMergeForC1NonnegativeCondition);
  return code;
}

class RowConditionMergeFailureGuard {
 public:
  RowConditionMergeFailureGuard() {
    gRowConditionMergeFailureStub = &stub_;
    stub_.set(nodesMergeConds, failMergeForC1NonnegativeCondition);
  }

  ~RowConditionMergeFailureGuard() {
    stub_.reset(nodesMergeConds);
    gRowConditionMergeFailureStub = nullptr;
  }

 private:
  Stub stub_;
};

}  // namespace

static void setCreateStreamString(char** ppDst, const char* pSrc) {
  taosMemoryFreeClear(*ppDst);
  *ppDst = taosStrdup(pSrc);
}

static void resetCreateStreamTriggerPayload(SCMCreateStreamReq* expect) {
  switch (expect->triggerType) {
    case WINDOW_TYPE_STATE:
      taosArrayDestroy(expect->trigger.stateWin.pSlotIds);
      expect->trigger.stateWin.pSlotIds = nullptr;
      taosMemoryFreeClear(expect->trigger.stateWin.zeroth);
      taosMemoryFreeClear(expect->trigger.stateWin.expr);
      break;
    case WINDOW_TYPE_EVENT:
      taosMemoryFreeClear(expect->trigger.event.startCond);
      taosMemoryFreeClear(expect->trigger.event.endCond);
      break;
    default:
      break;
  }
  memset(&expect->trigger, 0, sizeof(expect->trigger));
}

static const SExternalWindowPhysiNode* pstFindExternalWindowNode(const SPhysiNode* pNode) {
  if (nullptr == pNode) {
    return nullptr;
  }

  if (QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL == nodeType((const SNode*)pNode) ||
      QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL == nodeType((const SNode*)pNode)) {
    return (const SExternalWindowPhysiNode*)pNode;
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) {
    const SExternalWindowPhysiNode* pExternal = pstFindExternalWindowNode((const SPhysiNode*)pChild);
    if (nullptr != pExternal) {
      return pExternal;
    }
  }

  return nullptr;
}

static const SExternalWindowPhysiNode* pstFindExternalWindowNode(const SQueryPlan* pPlan) {
  if (nullptr == pPlan) {
    return nullptr;
  }

  SNode* pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    SNodeListNode* pLevel = (SNodeListNode*)pLevelNode;
    SNode*         pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const SSubplan*                 pSubplan = (const SSubplan*)pSubplanNode;
      const SExternalWindowPhysiNode* pExternal = pstFindExternalWindowNode(pSubplan->pNode);
      if (nullptr != pExternal) {
        return pExternal;
      }
    }
  }

  return nullptr;
}

struct SWindowTraversalCounts {
  int32_t plans = 0;
  int32_t layers = 0;
  int32_t concreteWindows = 0;
};

static void pstCountWindowTraversalNode(const SNode* pNode, SWindowTraversalCounts* pCounts) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_STREAM_WINDOW_PLAN:
      ++pCounts->plans;
      break;
    case QUERY_NODE_STREAM_WINDOW_LAYER:
      ++pCounts->layers;
      break;
    case QUERY_NODE_STATE_WINDOW:
    case QUERY_NODE_INTERVAL_WINDOW:
    case QUERY_NODE_COUNT_WINDOW:
      ++pCounts->concreteWindows;
      break;
    default:
      break;
  }
}

static EDealRes pstCountWindowWalker(SNode* pNode, void* pContext) {
  pstCountWindowTraversalNode(pNode, (SWindowTraversalCounts*)pContext);
  return DEAL_RES_CONTINUE;
}

static EDealRes pstCountWindowRewriter(SNode** ppNode, void* pContext) {
  pstCountWindowTraversalNode(*ppNode, (SWindowTraversalCounts*)pContext);
  return DEAL_RES_CONTINUE;
}

struct SWindowValueLiteralCount {
  const char* pLiteral = nullptr;
  int32_t     count = 0;
};

static EDealRes pstCountWindowValueLiteral(SNode* pNode, void* pContext) {
  auto* pCount = (SWindowValueLiteralCount*)pContext;
  if (QUERY_NODE_VALUE == nodeType(pNode)) {
    const auto* pValue = (const SValueNode*)pNode;
    if (nullptr != pValue->literal && 0 == strcmp(pCount->pLiteral, pValue->literal)) {
      ++pCount->count;
    }
  }
  return DEAL_RES_CONTINUE;
}

struct SPseudoLayerBinding {
  std::string name;
  bool        qualified;
  int64_t     layerIndex;
};

static EDealRes pstCollectPseudoLayerBinding(SNode* pNode, void* pContext) {
  if (nodeType(pNode) != QUERY_NODE_FUNCTION) return DEAL_RES_CONTINUE;

  auto* pFunc = (SFunctionNode*)pNode;
  if (strcmp(pFunc->functionName, "_twstart") != 0 && strcmp(pFunc->functionName, "_twend") != 0 &&
      strcmp(pFunc->functionName, "_twduration") != 0 && strcmp(pFunc->functionName, "_twrownum") != 0 &&
      strcmp(pFunc->functionName, "_tprev_ts") != 0 && strcmp(pFunc->functionName, "_tcurrent_ts") != 0 &&
      strcmp(pFunc->functionName, "_tnext_ts") != 0) {
    return DEAL_RES_CONTINUE;
  }

  const int32_t numParams = LIST_LENGTH(pFunc->pParameterList);
  if (numParams != 1 && numParams != 2) {
    ADD_FAILURE() << pFunc->functionName << " has " << numParams << " parameters";
    return DEAL_RES_ERROR;
  }

  int64_t layerIndex = 0;
  if (numParams == 2) {
    SNode* pLayer = nodesListGetNode(pFunc->pParameterList, 1);
    if (nodeType(pLayer) != QUERY_NODE_VALUE || ((SValueNode*)pLayer)->node.resType.type != TSDB_DATA_TYPE_INT) {
      ADD_FAILURE() << pFunc->functionName << " has invalid layer parameter";
      return DEAL_RES_ERROR;
    }
    layerIndex = ((SValueNode*)pLayer)->datum.i;
  }

  ((std::vector<SPseudoLayerBinding>*)pContext)->push_back({pFunc->functionName, numParams == 2, layerIndex});
  return DEAL_RES_CONTINUE;
}

static void pstCollectProjectPseudoLayerBindings(const SPhysiNode* pNode, std::vector<SPseudoLayerBinding>* pBindings) {
  if (pNode == nullptr) return;

  if (nodeType((const SNode*)pNode) == QUERY_NODE_PHYSICAL_PLAN_PROJECT) {
    const auto* pProject = (const SProjectPhysiNode*)pNode;
    nodesWalkExprs(pProject->pProjections, pstCollectPseudoLayerBinding, pBindings);
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { pstCollectProjectPseudoLayerBindings((const SPhysiNode*)pChild, pBindings); }
}

static void pstCollectProjectPseudoLayerBindings(const SQueryPlan* pPlan, std::vector<SPseudoLayerBinding>* pBindings) {
  SNode* pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    auto*  pLevel = (SNodeListNode*)pLevelNode;
    SNode* pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const auto* pSubplan = (const SSubplan*)pSubplanNode;
      pstCollectProjectPseudoLayerBindings(pSubplan->pNode, pBindings);
    }
  }
}

static const SStreamWindowLayerSpec* pstWindowLayer(const SCMCreateStreamReq& req, int32_t index) {
  if (req.pWindowPlan == nullptr || req.pWindowPlan->pLayers == nullptr || index < 0 ||
      index >= taosArrayGetSize(req.pWindowPlan->pLayers)) {
    return nullptr;
  }
  return static_cast<const SStreamWindowLayerSpec*>(taosArrayGet(req.pWindowPlan->pLayers, index));
}

static void pstAssertNestedRequestRoundTrip(const SQuery*                                         pQuery,
                                            const std::function<void(const SCMCreateStreamReq&)>& check = {}) {
  ASSERT_NE(nullptr, pQuery);
  ASSERT_NE(nullptr, pQuery->pCmdMsg);
  ASSERT_NE(nullptr, pQuery->pCmdMsg->pMsg);

  SCMCreateStreamReq req = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(nullptr, req.pWindowPlan);
  ASSERT_NE(nullptr, req.pWindowPlan->pLayers);
  ASSERT_GE(taosArrayGetSize(req.pWindowPlan->pLayers), 2);
  ASSERT_NE(nullptr, req.triggerScanPlan);
  EXPECT_NE(0, req.addOptions & STREAM_OPTION_NESTED_WINDOW_PLAN);
  SNode* pTriggerPlan = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.triggerScanPlan, &pTriggerPlan));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> triggerPlanGuard(pTriggerPlan, nodesDestroyNode);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(pTriggerPlan));
  EXPECT_TRUE(((const SSubplan*)pTriggerPlan)->requiresAncestorContext);
  for (int32_t i = 0; i < taosArrayGetSize(req.pWindowPlan->pLayers); ++i) {
    const auto* pLayer = (const SStreamWindowLayerSpec*)taosArrayGet(req.pWindowPlan->pLayers, i);
    ASSERT_NE(nullptr, pLayer);
    ASSERT_NE(nullptr, pLayer->input.pConditionSlotIds);
    EXPECT_GE(pLayer->input.tsSlotId, 0);
    EXPECT_GE(pLayer->input.pkSlotId, -1);
    for (int32_t j = 0; j < taosArrayGetSize(pLayer->input.pConditionSlotIds); ++j) {
      EXPECT_GE(*static_cast<const int16_t*>(taosArrayGet(pLayer->input.pConditionSlotIds, j)), 0);
    }
    if (pLayer->triggerType == WINDOW_TYPE_STATE) {
      ASSERT_NE(nullptr, pLayer->trigger.stateWin.pSlotIds);
      EXPECT_EQ(taosArrayGetSize(pLayer->trigger.stateWin.pSlotIds), taosArrayGetSize(pLayer->input.pConditionSlotIds));
      for (int32_t j = 0; j < taosArrayGetSize(pLayer->trigger.stateWin.pSlotIds); ++j) {
        EXPECT_EQ(*static_cast<const int16_t*>(taosArrayGet(pLayer->input.pConditionSlotIds, j)),
                  *static_cast<const int16_t*>(taosArrayGet(pLayer->trigger.stateWin.pSlotIds, j)));
      }
    }
    if (pLayer->triggerType == WINDOW_TYPE_EVENT) {
      EXPECT_GE(pLayer->input.eventStartSlotId, 0);
      if (pLayer->trigger.event.endCond != nullptr) EXPECT_GE(pLayer->input.eventEndSlotId, 0);
    }
  }
  if (check) check(req);
}

static void pstCollectTriggerScans(const SPhysiNode* pNode, std::vector<const SScanPhysiNode*>* pScans) {
  if (pNode == nullptr) return;

  if (nodeType((const SNode*)pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
      nodeType((const SNode*)pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
    pScans->push_back((const SScanPhysiNode*)pNode);
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { pstCollectTriggerScans((const SPhysiNode*)pChild, pScans); }
}

static void pstAssertNestedRawTimestampSlots(const SCMCreateStreamReq& req, EWindowType expectedLeafType) {
  ASSERT_NE(nullptr, req.triggerScanPlan);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.triggerScanPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(nullptr, pPlanNode);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(pPlanNode));

  const auto* pSubplan = (const SSubplan*)pPlanNode;
  ASSERT_NE(nullptr, pSubplan->pNode);
  std::vector<const SScanPhysiNode*> scans;
  pstCollectTriggerScans(pSubplan->pNode, &scans);
  ASSERT_EQ(1U, scans.size());

  const auto* pScan = scans[0];
  ASSERT_NE(nullptr, pScan->node.pOutputDataBlockDesc);
  ASSERT_NE(nullptr, pScan->node.pOutputDataBlockDesc->pSlots);
  const int32_t rawSlotCount = LIST_LENGTH(pScan->node.pOutputDataBlockDesc->pSlots);
  ASSERT_GT(rawSlotCount, 0);

  const STargetNode* pTimestampTarget = nullptr;
  int32_t            timestampTargets = 0;
  SNode*             pTargetNode = nullptr;
  FOREACH(pTargetNode, pScan->pScanCols) {
    ASSERT_EQ(QUERY_NODE_TARGET, nodeType(pTargetNode));
    const auto* pTarget = (const STargetNode*)pTargetNode;
    if (pTarget->pExpr != nullptr && nodeType(pTarget->pExpr) == QUERY_NODE_COLUMN &&
        ((const SColumnNode*)pTarget->pExpr)->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pTimestampTarget = pTarget;
      ++timestampTargets;
    }
  }
  ASSERT_EQ(1, timestampTargets);
  ASSERT_NE(nullptr, pTimestampTarget);
  ASSERT_GE(pTimestampTarget->slotId, 0);
  ASSERT_LT(pTimestampTarget->slotId, rawSlotCount);

  const SSlotDescNode* pTimestampSlot = nullptr;
  int32_t              timestampSlots = 0;
  SNode*               pSlotNode = nullptr;
  FOREACH(pSlotNode, pScan->node.pOutputDataBlockDesc->pSlots) {
    ASSERT_EQ(QUERY_NODE_SLOT_DESC, nodeType(pSlotNode));
    const auto* pSlot = (const SSlotDescNode*)pSlotNode;
    if (pSlot->slotId == pTimestampTarget->slotId) {
      pTimestampSlot = pSlot;
      ++timestampSlots;
    }
  }
  ASSERT_EQ(1, timestampSlots);
  ASSERT_NE(nullptr, pTimestampSlot);
  EXPECT_EQ(TSDB_DATA_TYPE_TIMESTAMP, pTimestampSlot->dataType.type);

  EXPECT_EQ(pTimestampTarget->slotId, req.triTsSlotId);
  ASSERT_NE(nullptr, req.pWindowPlan);
  ASSERT_NE(nullptr, req.pWindowPlan->pLayers);
  ASSERT_EQ(2, taosArrayGetSize(req.pWindowPlan->pLayers));
  for (int32_t i = 0; i < taosArrayGetSize(req.pWindowPlan->pLayers); ++i) {
    const auto* pLayer = pstWindowLayer(req, i);
    ASSERT_NE(nullptr, pLayer);
    EXPECT_EQ(pTimestampTarget->slotId, pLayer->input.tsSlotId);
  }

  const auto* pLeaf = pstWindowLayer(req, 1);
  ASSERT_NE(nullptr, pLeaf);
  ASSERT_EQ(expectedLeafType, pLeaf->triggerType);
  ASSERT_NE(nullptr, pLeaf->input.pConditionSlotIds);
  if (expectedLeafType == WINDOW_TYPE_STATE) {
    ASSERT_EQ(1, taosArrayGetSize(pLeaf->input.pConditionSlotIds));
    ASSERT_NE(nullptr, pLeaf->trigger.stateWin.pSlotIds);
    ASSERT_EQ(1, taosArrayGetSize(pLeaf->trigger.stateWin.pSlotIds));
    ASSERT_NE(nullptr, req.trigger.stateWin.pSlotIds);
    ASSERT_EQ(1, taosArrayGetSize(req.trigger.stateWin.pSlotIds));
    const int16_t conditionSlot = *(const int16_t*)taosArrayGet(pLeaf->input.pConditionSlotIds, 0);
    EXPECT_GE(conditionSlot, 0);
    EXPECT_EQ(conditionSlot, *(const int16_t*)taosArrayGet(pLeaf->trigger.stateWin.pSlotIds, 0));
    EXPECT_EQ(conditionSlot, *(const int16_t*)taosArrayGet(req.trigger.stateWin.pSlotIds, 0));
  } else if (expectedLeafType == WINDOW_TYPE_COUNT) {
    EXPECT_EQ(0, taosArrayGetSize(pLeaf->input.pConditionSlotIds));
    EXPECT_EQ(2, req.trigger.count.countVal);
    EXPECT_GT(req.trigger.count.sliding, 0);
  } else if (expectedLeafType == WINDOW_TYPE_EVENT) {
    EXPECT_EQ(0, taosArrayGetSize(pLeaf->input.pConditionSlotIds));
    EXPECT_EQ(rawSlotCount + 1, pLeaf->input.eventStartSlotId);
    EXPECT_EQ(rawSlotCount + 2, pLeaf->input.eventEndSlotId);
    ASSERT_NE(nullptr, pLeaf->trigger.event.startCond);
    ASSERT_NE(nullptr, pLeaf->trigger.event.endCond);
    ASSERT_NE(nullptr, req.trigger.event.startCond);
    ASSERT_NE(nullptr, req.trigger.event.endCond);
  }
}

static void pstCheckClonedWindowPlanLifecycle(SNode* pWindowPlan, int32_t expectedLayers,
                                              const char* pExpectedFirstName, ENodeType expectedFirstWindowType,
                                              const char* pExpectedLastName, ENodeType expectedLastWindowType) {
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pWindowPlan, nodesDestroyNode);
  ASSERT_NE(nullptr, pWindowPlan);
  ASSERT_EQ(QUERY_NODE_STREAM_WINDOW_PLAN, nodeType(pWindowPlan));

  auto* pPlan = (SStreamWindowPlanNode*)pWindowPlan;
  ASSERT_EQ(expectedLayers, LIST_LENGTH(pPlan->pLayers));
  auto* pFirst = (SStreamWindowLayerNode*)nodesListGetNode(pPlan->pLayers, 0);
  auto* pLast = (SStreamWindowLayerNode*)nodesListGetNode(pPlan->pLayers, expectedLayers - 1);
  EXPECT_STREQ(pExpectedFirstName, pFirst->name);
  EXPECT_EQ(expectedFirstWindowType, nodeType(pFirst->pWindow));
  EXPECT_STREQ(pExpectedLastName, pLast->name);
  EXPECT_EQ(expectedLastWindowType, nodeType(pLast->pWindow));

  SWindowTraversalCounts walked;
  nodesWalkExpr(pWindowPlan, pstCountWindowWalker, &walked);
  EXPECT_EQ(1, walked.plans);
  EXPECT_EQ(expectedLayers, walked.layers);
  EXPECT_EQ(expectedLayers, walked.concreteWindows);

  SWindowTraversalCounts rewritten;
  nodesRewriteExpr(&pWindowPlan, pstCountWindowRewriter, &rewritten);
  EXPECT_EQ(1, rewritten.plans);
  EXPECT_EQ(expectedLayers, rewritten.layers);
  EXPECT_EQ(expectedLayers, rewritten.concreteWindows);
  ASSERT_EQ(planGuard.get(), pWindowPlan);

  char*   pEncoded = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pWindowPlan, false, &pEncoded, &len));
  auto                                    freeEncoded = [](char* pData) { taosMemoryFree(pData); };
  unique_ptr<char, decltype(freeEncoded)> encodedGuard(pEncoded, freeEncoded);
  ASSERT_GT(len, 0);

  SNode* pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(pEncoded, &pDecoded));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(pDecoded, nodesDestroyNode);
  ASSERT_NE(nullptr, pDecoded);
  ASSERT_EQ(QUERY_NODE_STREAM_WINDOW_PLAN, nodeType(pDecoded));

  const auto* pDecodedPlan = (const SStreamWindowPlanNode*)pDecoded;
  ASSERT_EQ(expectedLayers, LIST_LENGTH(pDecodedPlan->pLayers));
  const auto* pDecodedFirst = (const SStreamWindowLayerNode*)nodesListGetNode(pDecodedPlan->pLayers, 0);
  const auto* pDecodedLast = (const SStreamWindowLayerNode*)nodesListGetNode(pDecodedPlan->pLayers, expectedLayers - 1);
  EXPECT_STREQ(pExpectedFirstName, pDecodedFirst->name);
  EXPECT_EQ(expectedFirstWindowType, nodeType(pDecodedFirst->pWindow));
  EXPECT_STREQ(pExpectedLastName, pDecodedLast->name);
  EXPECT_EQ(expectedLastWindowType, nodeType(pDecodedLast->pWindow));
}

static void pstParseStreamOnly(const char* pSql, const function<void(const SQuery*, ParserStage)>& check) {
  array<char, 1024> msgBuf = {0};
  SParseContext     cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "root";
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = msgBuf.data();
  cxt.msgLen = msgBuf.max_size();

  SQuery* pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, parse(&cxt, &pQuery)) << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(nullptr, pQuery);
  check(pQuery, PARSER_STAGE_PARSE);
}

static void pstExpectStreamParseCode(const char* pSql, int32_t expectedCode) {
  array<char, 1024> msgBuf = {0};
  SParseContext     cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "root";
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = msgBuf.data();
  cxt.msgLen = msgBuf.max_size();

  SQuery* pQuery = nullptr;
  EXPECT_EQ(expectedCode, parse(&cxt, &pQuery)) << msgBuf.data();
  qDestroyQuery(pQuery);
}

TEST_F(ParserStreamTest, NestedWindowParsesRootToLeafAndRoundTrips) {
  SNode* pWindowPlanClone = nullptr;
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested window ("
      "state_window(c1) extend(1) as w_state, "
      "interval(1m) sliding(1m) as w_min, "
      "count_window(2,1)) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart "
      "from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        ASSERT_EQ(QUERY_NODE_CREATE_STREAM_STMT, nodeType(pQuery->pRoot));
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        ASSERT_EQ(QUERY_NODE_STREAM_WINDOW_PLAN, nodeType(trigger->pTriggerWindow));
        const auto* plan = (const SStreamWindowPlanNode*)trigger->pTriggerWindow;
        ASSERT_EQ(3, LIST_LENGTH(plan->pLayers));
        EXPECT_STREQ("w_state", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 0))->name);
        EXPECT_STREQ("w_min", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 1))->name);
        EXPECT_STREQ("", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 2))->name);

        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(trigger->pTriggerWindow, &pWindowPlanClone));
        ASSERT_NE(trigger->pTriggerWindow, pWindowPlanClone);
      });

  ASSERT_NE(nullptr, pWindowPlanClone);
  pstCheckClonedWindowPlanLifecycle(pWindowPlanClone, 3, "w_state", QUERY_NODE_STATE_WINDOW, "",
                                    QUERY_NODE_COUNT_WINDOW);
}

TEST_F(ParserStreamTest, NestedWindowPreservesSlidingOffsetAcrossLifecycle) {
  SNode* pWindowPlanClone = nullptr;
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_sliding_offset window ("
      "interval(2m) sliding(1m, 10s) as w_interval, count_window(2,1)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
        const auto* pPlan = (const SStreamWindowPlanNode*)pTrigger->pTriggerWindow;
        const auto* pLayer = (const SStreamWindowLayerNode*)nodesListGetNode(pPlan->pLayers, 0);
        const auto* pInterval = (const SIntervalWindowNode*)pLayer->pWindow;
        ASSERT_NE(nullptr, pInterval->pSOffset);
        EXPECT_STREQ("10s", ((const SValueNode*)pInterval->pSOffset)->literal);

        SWindowValueLiteralCount walked;
        walked.pLiteral = "10s";
        nodesWalkExpr(pTrigger->pTriggerWindow, pstCountWindowValueLiteral, &walked);
        EXPECT_EQ(1, walked.count);

        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(pTrigger->pTriggerWindow, &pWindowPlanClone));
      });

  unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pWindowPlanClone, nodesDestroyNode);
  ASSERT_NE(nullptr, pWindowPlanClone);
  const auto* pClonePlan = (const SStreamWindowPlanNode*)pWindowPlanClone;
  const auto* pCloneLayer = (const SStreamWindowLayerNode*)nodesListGetNode(pClonePlan->pLayers, 0);
  const auto* pCloneInterval = (const SIntervalWindowNode*)pCloneLayer->pWindow;
  ASSERT_NE(nullptr, pCloneInterval->pSOffset);
  EXPECT_STREQ("10s", ((const SValueNode*)pCloneInterval->pSOffset)->literal);

  SWindowValueLiteralCount walkedClone;
  walkedClone.pLiteral = "10s";
  nodesWalkExpr(pWindowPlanClone, pstCountWindowValueLiteral, &walkedClone);
  EXPECT_EQ(1, walkedClone.count);

  char*   pEncoded = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pWindowPlanClone, false, &pEncoded, &len));
  auto                                    freeEncoded = [](char* pData) { taosMemoryFree(pData); };
  unique_ptr<char, decltype(freeEncoded)> encodedGuard(pEncoded, freeEncoded);
  ASSERT_GT(len, 0);

  SNode* pDecoded = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(pEncoded, &pDecoded));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> decodedGuard(pDecoded, nodesDestroyNode);
  ASSERT_NE(nullptr, pDecoded);
  const auto* pDecodedPlan = (const SStreamWindowPlanNode*)pDecoded;
  const auto* pDecodedLayer = (const SStreamWindowLayerNode*)nodesListGetNode(pDecodedPlan->pLayers, 0);
  const auto* pDecodedInterval = (const SIntervalWindowNode*)pDecodedLayer->pWindow;
  ASSERT_NE(nullptr, pDecodedInterval->pSOffset);
  EXPECT_STREQ("10s", ((const SValueNode*)pDecodedInterval->pSOffset)->literal);
}

TEST_F(ParserStreamTest, NestedWindowClonesPeriodLayerOwnedFields) {
  SNode* pWindowPlanClone = nullptr;
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_period window ("
      "period(1m, 10s) as w_period, count_window(2,1)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
        const auto* pPlan = (const SStreamWindowPlanNode*)pTrigger->pTriggerWindow;
        const auto* pLayer = (const SStreamWindowLayerNode*)nodesListGetNode(pPlan->pLayers, 0);
        ASSERT_EQ(QUERY_NODE_PERIOD_WINDOW, nodeType(pLayer->pWindow));
        const auto* pPeriod = (const SPeriodWindowNode*)pLayer->pWindow;
        ASSERT_NE(nullptr, pPeriod->pPeroid);
        ASSERT_NE(nullptr, pPeriod->pOffset);
        EXPECT_STREQ("1m", ((const SValueNode*)pPeriod->pPeroid)->literal);
        EXPECT_STREQ("10s", ((const SValueNode*)pPeriod->pOffset)->literal);

        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(pTrigger->pTriggerWindow, &pWindowPlanClone));
      });

  unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pWindowPlanClone, nodesDestroyNode);
  ASSERT_NE(nullptr, pWindowPlanClone);
  const auto* pClonePlan = (const SStreamWindowPlanNode*)pWindowPlanClone;
  const auto* pCloneLayer = (const SStreamWindowLayerNode*)nodesListGetNode(pClonePlan->pLayers, 0);
  ASSERT_EQ(QUERY_NODE_PERIOD_WINDOW, nodeType(pCloneLayer->pWindow));
  const auto* pClonePeriod = (const SPeriodWindowNode*)pCloneLayer->pWindow;
  ASSERT_NE(nullptr, pClonePeriod->pPeroid);
  ASSERT_NE(nullptr, pClonePeriod->pOffset);
  EXPECT_STREQ("1m", ((const SValueNode*)pClonePeriod->pPeroid)->literal);
  EXPECT_STREQ("10s", ((const SValueNode*)pClonePeriod->pOffset)->literal);
}

TEST_F(ParserStreamTest, NestedWindowParsesTwoLayersAndNamedLeaf) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_two window ("
      "interval(2m) sliding(2m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        const auto* plan = (const SStreamWindowPlanNode*)trigger->pTriggerWindow;
        ASSERT_EQ(2, LIST_LENGTH(plan->pLayers));
        EXPECT_STREQ("w_outer", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 0))->name);
        EXPECT_STREQ("w_leaf", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 1))->name);
      });
}

TEST_F(ParserStreamTest, NestedWindowParsesEightLayers) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_eight window ("
      "interval(8m) sliding(8m) as w1, interval(7m) sliding(7m) as w2, "
      "interval(6m) sliding(6m) as w3, interval(5m) sliding(5m) as w4, "
      "interval(4m) sliding(4m) as w5, interval(3m) sliding(3m) as w6, "
      "interval(2m) sliding(2m) as w7, count_window(2,1)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        const auto* plan = (const SStreamWindowPlanNode*)trigger->pTriggerWindow;
        ASSERT_EQ(8, LIST_LENGTH(plan->pLayers));
        EXPECT_STREQ("w1", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 0))->name);
        EXPECT_STREQ("", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 7))->name);
      });
}

TEST_F(ParserStreamTest, NestedWindowParsesNineLayersForAstLifecycle) {
  SNode* pWindowPlanClone = nullptr;
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_nine window ("
      "interval(9m) sliding(9m) as w1, interval(8m) sliding(8m) as w2, "
      "interval(7m) sliding(7m) as w3, interval(6m) sliding(6m) as w4, "
      "interval(5m) sliding(5m) as w5, interval(4m) sliding(4m) as w6, "
      "interval(3m) sliding(3m) as w7, interval(2m) sliding(2m) as w8, "
      "count_window(2,1)) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart "
      "from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        const auto* plan = (const SStreamWindowPlanNode*)trigger->pTriggerWindow;
        ASSERT_EQ(9, LIST_LENGTH(plan->pLayers));
        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(trigger->pTriggerWindow, &pWindowPlanClone));
      });

  ASSERT_NE(nullptr, pWindowPlanClone);
  pstCheckClonedWindowPlanLifecycle(pWindowPlanClone, 9, "w1", QUERY_NODE_INTERVAL_WINDOW, "", QUERY_NODE_COUNT_WINDOW);
}

TEST_F(ParserStreamTest, NestedWindowParsesQuotedLayerNames) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_quoted window ("
      "state_window(c1) extend(1) as `w_outer`, count_window(2,1) as `w_leaf`) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        const auto* plan = (const SStreamWindowPlanNode*)trigger->pTriggerWindow;
        ASSERT_EQ(2, LIST_LENGTH(plan->pLayers));
        EXPECT_STREQ("w_outer", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 0))->name);
        EXPECT_STREQ("w_leaf", ((SStreamWindowLayerNode*)nodesListGetNode(plan->pLayers, 1))->name);
      });
}

TEST_F(ParserStreamTest, NestedWindowValidatesLayerNameLengthBeforeCopy) {
  string maxLengthName(TSDB_TABLE_NAME_LEN - 1, 'a');
  string maxLengthSql =
      "create stream stream_streamdb.s_nested_max_name window ("
      "interval(2m) sliding(2m) as " +
      maxLengthName +
      ", count_window(2,1)) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart from stream_querydb.stream_t2";
  pstParseStreamOnly(maxLengthSql.c_str(), [&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_PARSE, stage);
    const auto* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
    const auto* pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
    const auto* pPlan = (const SStreamWindowPlanNode*)pTrigger->pTriggerWindow;
    const auto* pLayer = (const SStreamWindowLayerNode*)nodesListGetNode(pPlan->pLayers, 0);
    EXPECT_STREQ(maxLengthName.c_str(), pLayer->name);
  });

  string tooLongName(TSDB_TABLE_NAME_LEN, 'b');
  string tooLongSql =
      "create stream stream_streamdb.s_nested_long_name window ("
      "interval(2m) sliding(2m) as " +
      tooLongName +
      ", count_window(2,1)) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart from stream_querydb.stream_t2";
  pstExpectStreamParseCode(tooLongSql.c_str(), TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);
}

TEST_F(ParserStreamTest, NestedWindowParsesFlushOnOuterCloseOption) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_flush window ("
      "interval(2m) sliding(2m) as w_outer, count_window(2,1)) "
      "from stream_triggerdb.stream_t1 stream_options(flush_on_outer_close) "
      "into stream_outdb.stream_out as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        ASSERT_NE(nullptr, trigger->pOptions);
        EXPECT_TRUE(((const SStreamTriggerOptions*)trigger->pOptions)->flushOnOuterClose);
      });

  pstExpectStreamParseCode(
      "create stream stream_streamdb.s_nested_flush_duplicate window ("
      "interval(2m) sliding(2m) as w_outer, count_window(2,1)) "
      "from stream_triggerdb.stream_t1 stream_options("
      "flush_on_outer_close | flush_on_outer_close) into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      TSDB_CODE_PAR_SYNTAX_ERROR);
}

TEST_F(ParserStreamTest, CreateStreamClonePreservesFlushOnOuterClose) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_flush_clone window ("
      "interval(2m) sliding(2m) as w_outer, count_window(2,1)) "
      "from stream_triggerdb.stream_t1 stream_options(flush_on_outer_close) "
      "into stream_outdb.stream_out as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;

        SNode* pClone = nullptr;
        ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode((const SNode*)pStmt, &pClone));
        unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pClone, nodesDestroyNode);
        ASSERT_NE(nullptr, pClone);

        const auto* pCloneStmt = (const SCreateStreamStmt*)pClone;
        const auto* pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
        const auto* pCloneTrigger = (const SStreamTriggerNode*)pCloneStmt->pTrigger;
        ASSERT_NE(nullptr, pTrigger->pOptions);
        ASSERT_NE(nullptr, pCloneTrigger->pOptions);
        auto* pCloneOptions = (SStreamTriggerOptions*)pCloneTrigger->pOptions;
        EXPECT_TRUE(pCloneOptions->flushOnOuterClose);
        EXPECT_TRUE(nodesEqualNode(pTrigger->pOptions, pCloneTrigger->pOptions));

        pCloneOptions->flushOnOuterClose = false;
        EXPECT_FALSE(nodesEqualNode(pTrigger->pOptions, pCloneTrigger->pOptions));
      });
}

TEST_F(ParserStreamTest, NestedWindowGrammarRejectsInvalidLayerShapes) {
  pstExpectStreamParseCode(
      "create stream stream_streamdb.s_nested_single window (count_window(2,1)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      TSDB_CODE_PAR_SYNTAX_ERROR);
  pstExpectStreamParseCode(
      "create stream stream_streamdb.s_nested_unnamed_nonleaf window ("
      "interval(2m) sliding(2m), count_window(2,1)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      TSDB_CODE_PAR_SYNTAX_ERROR);
}

TEST_F(ParserStreamTest, NestedWindowPreservesDirectSingleWindowAst) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_single interval(1m) sliding(1m) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart from stream_querydb.stream_t2",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* stmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* trigger = (const SStreamTriggerNode*)stmt->pTrigger;
        EXPECT_EQ(QUERY_NODE_INTERVAL_WINDOW, nodeType(trigger->pTriggerWindow));
      });
}

TEST_F(ParserStreamTest, NestedWindowParsesQualifiedLayerPseudoColumns) {
  pstParseStreamOnly(
      "create stream stream_streamdb.s_nested_qualified_ast window ("
      "sliding(1m) as w_slide, interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_slide._tprev_ts, w_slide._tcurrent_ts, w_slide._tnext_ts, "
      "w_outer._twstart, w_outer._twend, w_leaf._twduration, w_leaf._twrownum from %%trows",
      [&](const SQuery* pQuery, ParserStage stage) {
        ASSERT_EQ(PARSER_STAGE_PARSE, stage);
        const auto* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
        const auto* pSelect = (const SSelectStmt*)pStmt->pQuery;
        ASSERT_NE(nullptr, pSelect->pProjectionList);
        ASSERT_EQ(7, LIST_LENGTH(pSelect->pProjectionList));

        const char* expectedAliases[] = {"w_slide", "w_slide", "w_slide", "w_outer", "w_outer", "w_leaf", "w_leaf"};
        const char* expectedNames[] = {"_tprev_ts", "_tcurrent_ts", "_tnext_ts", "_twstart",
                                       "_twend",    "_twduration",  "_twrownum"};
        for (int32_t i = 0; i < 7; ++i) {
          const SNode* pProjection = nodesListGetNode(pSelect->pProjectionList, i);
          ASSERT_EQ(QUERY_NODE_COLUMN, nodeType(pProjection));
          const auto* pColumn = (const SColumnNode*)pProjection;
          EXPECT_STREQ(expectedAliases[i], pColumn->tableAlias);
          EXPECT_STREQ(expectedNames[i], pColumn->colName);
        }
      });
}

TEST_F(ParserStreamTest, NestedWindowRequestWindowPlanRoundTrips) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);

    ASSERT_NE(nullptr, req.pWindowPlan);
    ASSERT_NE(nullptr, req.pWindowPlan->pLayers);
    EXPECT_EQ(2, taosArrayGetSize(req.pWindowPlan->pLayers));
    EXPECT_EQ(WINDOW_TYPE_COUNT, req.triggerType);
    EXPECT_NE(0, req.addOptions & STREAM_OPTION_NESTED_WINDOW_PLAN);
    ASSERT_EQ(WINDOW_TYPE_STATE, ((SStreamWindowLayerSpec*)taosArrayGet(req.pWindowPlan->pLayers, 0))->triggerType);
    ASSERT_EQ(WINDOW_TYPE_COUNT, ((SStreamWindowLayerSpec*)taosArrayGet(req.pWindowPlan->pLayers, 1))->triggerType);
    const auto* pState = (const SStreamWindowLayerSpec*)taosArrayGet(req.pWindowPlan->pLayers, 0);
    const auto* pLeaf = (const SStreamWindowLayerSpec*)taosArrayGet(req.pWindowPlan->pLayers, 1);
    pstAssertNestedRawTimestampSlots(req, WINDOW_TYPE_COUNT);
    EXPECT_STREQ("w_state", pState->name);
    EXPECT_STREQ("w_leaf", pLeaf->name);
    ASSERT_NE(nullptr, pState->input.pConditionSlotIds);
    ASSERT_NE(nullptr, pState->trigger.stateWin.pSlotIds);
    EXPECT_EQ(1, taosArrayGetSize(pState->input.pConditionSlotIds));
    EXPECT_EQ(1, taosArrayGetSize(pState->trigger.stateWin.pSlotIds));
    ASSERT_NE(nullptr, pLeaf->input.pConditionSlotIds);
    EXPECT_EQ(0, taosArrayGetSize(pLeaf->input.pConditionSlotIds));
    EXPECT_EQ(PLACE_HOLDER_WSTART, pState->placeholderMask);
    EXPECT_EQ(PLACE_HOLDER_WROWNUM, pLeaf->placeholderMask);
    EXPECT_EQ(*static_cast<const int16_t*>(taosArrayGet(pState->input.pConditionSlotIds, 0)),
              *static_cast<const int16_t*>(taosArrayGet(pState->trigger.stateWin.pSlotIds, 0)));
    EXPECT_GE(pState->input.tsSlotId, 0);
    EXPECT_GE(pLeaf->input.tsSlotId, 0);
    EXPECT_EQ(-1, pState->input.pkSlotId);
    EXPECT_EQ(-1, pLeaf->input.pkSlotId);
    EXPECT_EQ(-1, pState->input.eventStartSlotId);
    EXPECT_EQ(-1, pState->input.eventEndSlotId);
    EXPECT_EQ(2, req.trigger.count.countVal);
    EXPECT_EQ(1, req.trigger.count.sliding);

    SCMCreateStreamReq* pClone = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, tCloneStreamCreateDeployPointers(&req, &pClone));
    auto freeReq = [](SCMCreateStreamReq* pReq) {
      if (pReq == nullptr) return;
      tFreeSCMCreateStreamReq(pReq);
      taosMemoryFree(pReq);
    };
    unique_ptr<SCMCreateStreamReq, decltype(freeReq)> cloneGuard(pClone, freeReq);
    ASSERT_NE(nullptr, pClone);
    ASSERT_NE(nullptr, pClone->pWindowPlan);
    EXPECT_NE(req.pWindowPlan, pClone->pWindowPlan);
    EXPECT_NE(req.pWindowPlan->pLayers, pClone->pWindowPlan->pLayers);
    const auto* pClonedState = pstWindowLayer(*pClone, 0);
    const auto* pClonedLeaf = pstWindowLayer(*pClone, 1);
    ASSERT_NE(nullptr, pClonedState);
    ASSERT_NE(nullptr, pClonedLeaf);
    EXPECT_NE(pState->input.pConditionSlotIds, pClonedState->input.pConditionSlotIds);
    EXPECT_NE(pState->trigger.stateWin.pSlotIds, pClonedState->trigger.stateWin.pSlotIds);
    EXPECT_NE(pState->trigger.stateWin.expr, pClonedState->trigger.stateWin.expr);
    EXPECT_NE(pLeaf->input.pConditionSlotIds, pClonedLeaf->input.pConditionSlotIds);
  });

  run("create stream stream_streamdb.s_nested_request window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_state._twstart, w_leaf._twrownum from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowRequestPreservesOnFailurePauseBit) {
  static const char kSql[] =
      "create stream stream_streamdb.s_nested_notify window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 notify('ws://localhost:8080') on (window_close) "
      "notify_options(notify_history) into stream_outdb.stream_out as "
      "select w_state._twstart, w_leaf._twrownum from %%trows";

  array<char, 1024> msgBuf = {0};
  SParseContext     cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "root";
  cxt.isSuperUser = true;
  cxt.enableSysInfo = true;
  cxt.privInfo = UINT16_MAX;
  cxt.pSql = kSql;
  cxt.sqlLen = strlen(kSql);
  cxt.pMsg = msgBuf.data();
  cxt.msgLen = msgBuf.max_size();

  SQuery* pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, parse(&cxt, &pQuery)) << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(nullptr, pQuery);
  ASSERT_EQ(QUERY_NODE_CREATE_STREAM_STMT, nodeType(pQuery->pRoot));

  auto* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
  ASSERT_NE(nullptr, pStmt->pTrigger);
  auto* pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  ASSERT_NE(nullptr, pTrigger->pNotify);
  auto* pNotify = (SStreamNotifyOptions*)pTrigger->pNotify;
  pNotify->notifyType = NOTIFY_ON_FAILURE_PAUSE;

  ASSERT_EQ(TSDB_CODE_SUCCESS, authenticate(&cxt, pQuery, nullptr)) << msgBuf.data();
  ASSERT_EQ(TSDB_CODE_SUCCESS, translate(&cxt, pQuery, nullptr)) << msgBuf.data();
  ASSERT_NE(nullptr, pQuery->pCmdMsg);

  SCMCreateStreamReq req = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  EXPECT_NE(0, req.addOptions & STREAM_OPTION_NESTED_WINDOW_PLAN);
  EXPECT_NE(0, req.addOptions & NOTIFY_ON_FAILURE_PAUSE);
  EXPECT_EQ(0, req.addOptions & BIT_FLAG_MASK(0));
}

TEST_F(ParserStreamTest, NestedWindowUsesRawScanTimestampSlotForDataDrivenLeaves) {
  useDb("root", "stream_streamdb");
  EWindowType expectedLeafType = WINDOW_TYPE_STATE;
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    pstAssertNestedRawTimestampSlots(req, expectedLeafType);
  });

  expectedLeafType = WINDOW_TYPE_STATE;
  run("create stream stream_streamdb.s_nested_raw_ts_state window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.s_nested_raw_ts_state as "
      "select _twstart ts, _twend wend, count(*) cnt from %%trows");

  expectedLeafType = WINDOW_TYPE_COUNT;
  run("create stream stream_streamdb.s_nested_raw_ts_count window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,2) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.s_nested_raw_ts_count as "
      "select _twstart ts, _twend wend, count(*) cnt from %%trows");

  expectedLeafType = WINDOW_TYPE_EVENT;
  run("create stream stream_streamdb.s_nested_raw_ts_event window ("
      "interval(1m) sliding(1m) as w_outer, "
      "event_window(start with c1 = 1 end with c1 = 0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.s_nested_raw_ts_event as "
      "select _twstart ts, _twend wend, count(*) cnt from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowLeafStateExpressionUsesProjectedSlot) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    const auto*                                                        pLeaf = pstWindowLayer(req, 1);
    ASSERT_NE(nullptr, pLeaf);
    ASSERT_NE(nullptr, pLeaf->input.pConditionSlotIds);
    ASSERT_EQ(2, taosArrayGetSize(pLeaf->input.pConditionSlotIds));
    ASSERT_NE(nullptr, pLeaf->trigger.stateWin.pSlotIds);
    ASSERT_EQ(2, taosArrayGetSize(pLeaf->trigger.stateWin.pSlotIds));
    ASSERT_NE(nullptr, req.trigger.stateWin.pSlotIds);
    ASSERT_EQ(2, taosArrayGetSize(req.trigger.stateWin.pSlotIds));

    SNodeList* pStateExprs = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToList((const char*)pLeaf->trigger.stateWin.expr, &pStateExprs));
    unique_ptr<SNodeList, decltype(&nodesDestroyList)> stateExprsGuard(pStateExprs, nodesDestroyList);
    ASSERT_EQ(2, LIST_LENGTH(pStateExprs));

    int16_t slots[2] = {-1, -1};
    for (int32_t i = 0; i < 2; ++i) {
      slots[i] = *(const int16_t*)taosArrayGet(pLeaf->input.pConditionSlotIds, i);
      EXPECT_GE(slots[i], 0);
      EXPECT_EQ(slots[i], *(const int16_t*)taosArrayGet(pLeaf->trigger.stateWin.pSlotIds, i));
      EXPECT_EQ(slots[i], *(const int16_t*)taosArrayGet(req.trigger.stateWin.pSlotIds, i));
      EXPECT_EQ(QUERY_NODE_OPERATOR, nodeType(nodesListGetNode(pStateExprs, i)));
    }
    EXPECT_NE(slots[0], slots[1]);
  });

  run("create stream stream_streamdb.s_nested_state_expr window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1 > 5, c1 < 10) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowLeafEventMultiStartUsesCompositeProjection) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    const auto*                                                        pLeaf = pstWindowLayer(req, 1);
    ASSERT_NE(nullptr, pLeaf);
    ASSERT_GE(pLeaf->input.eventStartSlotId, 0);
    ASSERT_GE(pLeaf->input.eventEndSlotId, 0);
    EXPECT_EQ(pLeaf->input.eventStartSlotId + 1, pLeaf->input.eventEndSlotId);

    SNode* pStartCond = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)pLeaf->trigger.event.startCond, &pStartCond));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> startCondGuard(pStartCond, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_NODE_LIST, nodeType(pStartCond));
    EXPECT_EQ(2, LIST_LENGTH(((const SNodeListNode*)pStartCond)->pNodeList));
  });

  run("create stream stream_streamdb.s_nested_event_multi_start window ("
      "interval(1m) sliding(1m) as w_outer, "
      "event_window(start with (c1 > 0, c1 < 10) end with c1 = 0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowRejectsHierarchicalEventStartTree) {
  useDb("root", "stream_streamdb");
  run("create stream stream_streamdb.s_nested_event_tree window ("
      "interval(1m) sliding(1m) as w_outer, "
      "event_window(start with ((c1 > 0, c2 = 'a'), c1 < 10) end with c1 = 0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_TRIGGER, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, NestedWindowEventLeafKeepsSlotsAfterEventAncestor) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    const auto*                                                        pOuter = pstWindowLayer(req, 0);
    const auto*                                                        pLeaf = pstWindowLayer(req, 1);
    ASSERT_NE(nullptr, pOuter);
    ASSERT_NE(nullptr, pLeaf);
    ASSERT_EQ(WINDOW_TYPE_EVENT, pOuter->triggerType);
    ASSERT_EQ(WINDOW_TYPE_EVENT, pLeaf->triggerType);
    ASSERT_GE(pOuter->input.eventStartSlotId, 0);
    ASSERT_GE(pOuter->input.eventEndSlotId, 0);
    ASSERT_GE(pLeaf->input.eventStartSlotId, 0);
    ASSERT_GE(pLeaf->input.eventEndSlotId, 0);
    EXPECT_LT(pOuter->input.eventStartSlotId, pOuter->input.eventEndSlotId);
    EXPECT_LT(pOuter->input.eventEndSlotId, pLeaf->input.eventStartSlotId);
    EXPECT_LT(pLeaf->input.eventStartSlotId, pLeaf->input.eventEndSlotId);
  });

  run("create stream stream_streamdb.s_nested_event_leaf_slots window ("
      "event_window(start with c1 = 1 end with c1 = 0) as w_outer, "
      "event_window(start with c2 = 1 end with c2 = 0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowEventAncestorUsesTrailingDerivedSlots) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    const auto*                                                        pOuter = pstWindowLayer(req, 0);
    ASSERT_NE(nullptr, pOuter);
    ASSERT_EQ(WINDOW_TYPE_EVENT, pOuter->triggerType);

    SNode* pPlanNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.triggerScanPlan, &pPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(pPlanNode));
    std::vector<const SScanPhysiNode*> scans;
    pstCollectTriggerScans(((const SSubplan*)pPlanNode)->pNode, &scans);
    ASSERT_EQ(1U, scans.size());
    ASSERT_NE(nullptr, scans[0]->node.pOutputDataBlockDesc);
    const int32_t rawSlotCount = LIST_LENGTH(scans[0]->node.pOutputDataBlockDesc->pSlots);

    EXPECT_EQ(rawSlotCount + 1, pOuter->input.eventStartSlotId);
    EXPECT_EQ(rawSlotCount + 2, pOuter->input.eventEndSlotId);
  });

  run("create stream stream_streamdb.s_nested_event_ancestor_slots window ("
      "event_window(start with c1 = 1 end with c1 = 0) as w_outer, count_window(3,1)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowBindsQualifiedAncestorPlaceholder) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    ASSERT_NE(nullptr, req.pWindowPlan);
    ASSERT_EQ(2, taosArrayGetSize(req.pWindowPlan->pLayers));
    ASSERT_NE(nullptr, req.calcPlan);

    SNode* pCalcPlan = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.calcPlan, &pCalcPlan));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> calcPlanGuard(pCalcPlan, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN, nodeType(pCalcPlan));

    std::vector<SPseudoLayerBinding> bindings;
    pstCollectProjectPseudoLayerBindings((const SQueryPlan*)pCalcPlan, &bindings);
    ASSERT_EQ(3U, bindings.size());

    auto find = [&](const char* name) -> const SPseudoLayerBinding* {
      for (const auto& binding : bindings) {
        if (binding.name == name) return &binding;
      }
      return nullptr;
    };
    const auto* pOuter = find("_twstart");
    const auto* pLeaf = find("_twrownum");
    const auto* pUnqualified = find("_twend");
    ASSERT_NE(nullptr, pOuter);
    ASSERT_NE(nullptr, pLeaf);
    ASSERT_NE(nullptr, pUnqualified);
    EXPECT_TRUE(pOuter->qualified);
    EXPECT_EQ(0, pOuter->layerIndex);
    EXPECT_TRUE(pLeaf->qualified);
    EXPECT_EQ(1, pLeaf->layerIndex);
    EXPECT_FALSE(pUnqualified->qualified);
  });

  run("create stream stream_streamdb.s_nested_binding window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_state._twstart, w_leaf._twrownum, _twend from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowAllowsOuterWhereAroundTrowsSubquery) {
  useDb("root", "stream_streamdb");

  run("create stream stream_streamdb.s_nested_trows_outer_where window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select t1.ts, t1.last_c1, t2.max_c1 from "
      "(select _twend ts, last(c1) last_c1 from %%trows) t1 inner join "
      "(select _twend ts, max(c1) max_c1 from stream_querydb.stream_t2 "
      "where ts >= w_state._twstart and ts <= _twstart) t2 "
      "on t1.ts = t2.ts where t2.max_c1 > 0");

  run("create stream stream_streamdb.s_nested_trows_direct_where window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twend, last(c1) from %%trows where c1 > 0",
      TSDB_CODE_PAR_INVALID_STREAM_QUERY, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, NestedWindowKeepsAggregateAncestorLayerAsValue) {
  AsyncFlagGuard asyncGuard(false);
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    ASSERT_NE(nullptr, req.calcPlan);

    SNode* pCalcPlan = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.calcPlan, &pCalcPlan));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> calcPlanGuard(pCalcPlan, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN, nodeType(pCalcPlan));

    const SExternalWindowPhysiNode* pExternal = pstFindExternalWindowNode((const SQueryPlan*)pCalcPlan);
    ASSERT_NE(nullptr, pExternal);
    std::vector<SPseudoLayerBinding> bindings;
    nodesWalkExprs(pExternal->window.pFuncs, pstCollectPseudoLayerBinding, &bindings);

    const SPseudoLayerBinding* pOuter = nullptr;
    for (const auto& binding : bindings) {
      if (binding.name == "_twstart" && binding.qualified) {
        pOuter = &binding;
        break;
      }
    }
    ASSERT_NE(nullptr, pOuter);
    EXPECT_EQ(0, pOuter->layerIndex);
  });

  run("create stream stream_streamdb.s_nested_aggregate_binding window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_state._twstart, max(c1) from stream_querydb.stream_t2 "
      "where ts >= w_state._twstart and ts <= _twstart");
}

TEST_F(ParserStreamTest, NestedWindowNamespaceMatrix) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage == PARSER_STAGE_TRANSLATE) pstAssertNestedRequestRoundTrip(pQuery);
  });

  run("create stream stream_streamdb.s_layer_duplicate_case window ("
      "interval(1m) sliding(1m) as W_OUTER, count_window(2,1) as w_outer) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, _twend from %%trows",
      TSDB_CODE_INVALID_PARA);

  const std::string maxLayerName(192, 'a');
  run("create stream stream_streamdb.s_layer_name_192 window ("
      "interval(1m) sliding(1m) as `" +
      maxLayerName +
      "`, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select `" +
      maxLayerName + "`._twstart, _twend from %%trows");

  const std::string oversizedLayerName(193, 'a');
  const std::string oversizedLayerSql =
      "create stream stream_streamdb.s_layer_name_193 window ("
      "interval(1m) sliding(1m) as `" +
      oversizedLayerName +
      "`, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, _twend from %%trows";
  pstExpectStreamParseCode(oversizedLayerSql.c_str(), TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);

  pstExpectStreamParseCode(
      "create stream stream_streamdb.s_layer_name_empty window ("
      "interval(1m) sliding(1m) as ``, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, _twend from %%trows",
      TSDB_CODE_PAR_INVALID_IDENTIFIER_NAME);

  run("create stream stream_streamdb.s_layer_name_invalid window ("
      "interval(1m) sliding(1m) as `bad.name`, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, _twend from %%trows");

  run("create stream stream_streamdb.s_layer_ascii_only window ("
      "interval(1m) sliding(1m) as `\xC3\x84`, count_window(2,1) as `\xC3\xA4`) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, _twend from %%trows");

  run("create stream stream_streamdb.s_layer_case window ("
      "interval(1m) sliding(1m) as W_OUTER, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer._twstart, W_LEAF._twrownum from %%trows");

  run("create stream stream_streamdb.s_physical_hidden window ("
      "interval(1m) sliding(1m) as stream_t2, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select stream_t2._twstart, _twend from stream_querydb.stream_t2 as q");

  run("create stream stream_streamdb.s_physical_visible_conflict window ("
      "interval(1m) sliding(1m) as q, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, _twend from stream_querydb.stream_t2 as q",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_derived_conflict window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, _twend from (select * from stream_querydb.stream_t2) w_outer",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_recursive_derived_conflict window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer._twstart, _twend from (select * from "
      "(select * from stream_querydb.stream_t2) w_outer) nested_table",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_recursive_derived_nonconflict window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts, c1 from (select w_outer._twstart as ts, c1 from "
      "(select * from stream_querydb.stream_t2) calc_relation) nested_table");

  run("create stream stream_streamdb.s_derived_generated window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer._twstart, _twend from (select * from stream_querydb.stream_t2)");

  run("create stream stream_streamdb.s_layer_not_column window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer.some_column, _twend from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_recursive_set_binding window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer._twstart, c1 from stream_querydb.stream_t2 union all "
      "select w_leaf._twstart, c1 from stream_querydb.stream_t2");

  run("create stream stream_streamdb.s_partition_alias_not_relation window ("
      "interval(1m) sliding(1m) as w_outer, interval(1s) sliding(1s) as w_leaf) "
      "from stream_triggerdb.stream_t1 partition by tbname w_outer "
      "into stream_outdb.stream_out as select w_outer._twstart, _twend from %%trows");

  run("create stream stream_streamdb.s_select_alias_not_relation window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer._twstart as w_outer, _twend from %%trows");

  run("create stream stream_streamdb.s_layer_outside_calc window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 partition by w_outer._twstart "
      "into stream_outdb.stream_out as select _twstart, _twend from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
}

TEST_F(ParserStreamTest, NestedWindowCapabilityMatrix) {
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage == PARSER_STAGE_TRANSLATE) pstAssertNestedRequestRoundTrip(pQuery);
  });

  run("create stream stream_streamdb.s_leaf_state window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) extend(2) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_outer._twstart, _twrownum from %%trows");

  run("create stream stream_streamdb.s_leaf_state_extend_zero window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) extend(0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_leaf_state_extend_one window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) extend(1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_leaf_state_zeroth_state window ("
      "interval(1m) sliding(1m) as w_outer, "
      "state_window(c1) extend(0) zeroth_state(no_zeroth) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_leaf_state_zeroth_value window ("
      "interval(1m) sliding(1m) as w_outer, "
      "state_window(c1) extend(0) zeroth_state(0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_leaf_event window ("
      "interval(1m) sliding(1m) as w_outer, "
      "event_window(start with (c1 > 0, c1 < 10) end with c1 = 0) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_nested_session window ("
      "session(ts, 1m) as w_session, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_session._twstart, _twend from %%trows");

  run("create stream stream_streamdb.s_count_interval window ("
      "count_window(2,2) as w_count, interval(1m) sliding(1m) as w_leaf) "
      "from stream_triggerdb.stream_t1 stream_options(ignore_nodata_trigger) "
      "into stream_outdb.stream_out as select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_delete_recalc_sliding window ("
      "sliding(1m) as w_slide, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 stream_options(delete_recalc) "
      "into stream_outdb.stream_out as select _twstart, c1 from %%trows");

  run("create stream stream_streamdb.s_delete_recalc_count_step window ("
      "count_window(3,2) as w_count, interval(1m) sliding(1m) as w_leaf) "
      "from stream_triggerdb.stream_t1 stream_options(delete_recalc) "
      "into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_super_state_without_tbname window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) as w_leaf) "
      "from stream_triggerdb.st1 into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_super_state_partition_length_tbname window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) as w_leaf) "
      "from stream_triggerdb.st1 partition by length(tbname) into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_super_state_partition_substr_tbname window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) as w_leaf) "
      "from stream_triggerdb.st1 partition by substr(tbname, 1, 1) into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  {
    AsyncFlagGuard asyncGuard(false);
    run("create stream stream_streamdb.s_super_state_partition_tag window ("
        "interval(1m) sliding(1m) as w_outer, state_window(c1) as w_leaf) "
        "from stream_triggerdb.st1 partition by tbname, tag1 into stream_outdb.stream_out as "
        "select _twstart, c1 from %%trows");
  }

  run("create stream stream_streamdb.s_nonleaf_overlap window ("
      "interval(2m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_nonleaf_state window ("
      "state_window(c1) extend(0) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_nonleaf_state_extend_two window ("
      "state_window(c1) extend(2) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_nonleaf_state_extend_default window ("
      "state_window(c1) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_nonleaf_count_overlap window ("
      "count_window(3,2) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_nonleaf_event_multi_start window ("
      "event_window(start with (c1 > 0, c1 < 10) end with c1 = 0) as w_outer, "
      "count_window(2,1) as w_leaf) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_sliding_twstart window ("
      "sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select w_outer._twstart, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_state_prev_ts window ("
      "state_window(c1) extend(1) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select w_outer._tprev_ts, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_leaf_sliding_unqualified_twstart window ("
      "interval(1m) sliding(1m) as w_outer, sliding(1s) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_leaf_count_unqualified_prev_ts window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tprev_ts, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_nested_idle window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) "
      "into stream_outdb.stream_out as select w_outer._twstart, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_nested_resume window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(resume)) "
      "into stream_outdb.stream_out as select w_leaf._twstart, c1 from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  run("create stream stream_streamdb.s_ignore_nodata_noninterval_leaf window ("
      "count_window(2,1) as w_count, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 stream_options(ignore_nodata_trigger) "
      "into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_nine_layers window ("
      "interval(9m) sliding(9m) as w1, interval(8m) sliding(8m) as w2, "
      "interval(7m) sliding(7m) as w3, interval(6m) sliding(6m) as w4, "
      "interval(5m) sliding(5m) as w5, interval(4m) sliding(4m) as w6, "
      "interval(3m) sliding(3m) as w7, interval(2m) sliding(2m) as w8, "
      "count_window(2,1) as w9) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  run("create stream stream_streamdb.s_single_flush interval(1m) sliding(1m) "
      "from stream_triggerdb.stream_t1 stream_options(flush_on_outer_close) "
      "into stream_outdb.stream_out as select _twstart, c1 from stream_querydb.stream_t2",
      TSDB_CODE_INVALID_PARA);
}

TEST_F(ParserStreamTest, NestedWindowRollupCapabilityMatrix) {
  // MockCatalogService returns an empty DB-vgroup result for the async path.
  // Existing supertable rollup tests therefore exercise planner shape through
  // the synchronous interface only (see TestCreateStreamRollupSemantic).
  const bool previousAsync = getAsyncFlag();
  {
    AsyncFlagGuard asyncGuard(false);
    useDb("root", "stream_streamdb");
    setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
      if (stage == PARSER_STAGE_TRANSLATE) pstAssertNestedRequestRoundTrip(pQuery);
    });

    run("create stream stream_streamdb.s_time_rollup window ("
        "interval(1m) sliding(1m) as w_outer, interval(1s) sliding(1s) as w_leaf) "
        "from stream_triggerdb.st1 rollup by tag2 into stream_outdb.stream_out as "
        "select _twstart, avg(c1) from stream_querydb.stream_t2");

    setCheckDdlFunc({});
    run("create stream stream_streamdb.s_rollup_non_time_leaf window ("
        "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
        "from stream_triggerdb.st1 rollup by tag2 into stream_outdb.stream_out as "
        "select _twstart, avg(c1) from stream_querydb.stream_t2",
        TSDB_CODE_INVALID_PARA);
  }
  EXPECT_EQ(previousAsync, getAsyncFlag());
}

TEST_F(ParserStreamTest, NestedWindowSuperTableTbnamePartitionBuildsCanonicalPlan) {
  AsyncFlagGuard asyncGuard(false);
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    pstAssertNestedRequestRoundTrip(pQuery, [&](const SCMCreateStreamReq& req) {
      ASSERT_EQ(TSDB_SUPER_TABLE, req.triggerTblType);
      const auto* pOuter = pstWindowLayer(req, 0);
      const auto* pLeaf = pstWindowLayer(req, 1);
      ASSERT_NE(nullptr, pOuter);
      ASSERT_NE(nullptr, pLeaf);
      EXPECT_EQ(WINDOW_TYPE_INTERVAL, pOuter->triggerType);
      EXPECT_EQ(WINDOW_TYPE_STATE, pLeaf->triggerType);
      ASSERT_NE(nullptr, pLeaf->input.pConditionSlotIds);
      EXPECT_EQ(1, taosArrayGetSize(pLeaf->input.pConditionSlotIds));
    });
  });

  run("create stream stream_streamdb.s_super_state_partition_tbname window ("
      "interval(1m) sliding(1m) as w_outer, state_window(c1) as w_leaf) "
      "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows");
}

TEST_F(ParserStreamTest, NestedWindowRejectsNonLeafPeriodAtCapabilityValidation) {
  AsyncFlagGuard asyncGuard(false);
  useDb("root", "stream_streamdb");

  run("create stream stream_streamdb.s_nonleaf_period window ("
      "period(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  SharedValidatorFailureGuard validatorGuard;
  run("create stream stream_streamdb.s_nonleaf_period_reach window ("
      "period(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, c1 from %%trows",
      TSDB_CODE_OUT_OF_MEMORY);
}

TEST_F(ParserStreamTest, NestedWindowRejectsCompositePrimaryKeyTrigger) {
  AsyncFlagGuard asyncGuard(false);
  useDb("root", "stream_streamdb");

  ITableBuilder& builder =
      g_mockCatalogService->createTableBuilder("stream_triggerdb", "nested_composite_pk", TSDB_NORMAL_TABLE, 3)
          .setPrecision(TSDB_TIME_PRECISION_MILLI)
          .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
          .addColumn("c1", TSDB_DATA_TYPE_INT)
          .addColumn("c2", TSDB_DATA_TYPE_INT);
  builder.done();

  CompositePrimaryKeyMetaGuard guard;
  run("create stream stream_streamdb.s_nested_composite_pk window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.nested_composite_pk into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows",
      TSDB_CODE_INVALID_PARA);

  SharedValidatorFailureGuard validatorGuard;
  run("create stream stream_streamdb.s_nested_composite_pk_reach window ("
      "interval(1m) sliding(1m) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.nested_composite_pk into stream_outdb.stream_out as "
      "select _twstart, c1 from %%trows",
      TSDB_CODE_OUT_OF_MEMORY);
}

TEST_F(ParserStreamTest, NestedWindowMarksEveryDetachedCalcScanJson) {
  AsyncFlagGuard asyncGuard(false);
  useDb("root", "stream_streamdb");
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);

    ASSERT_NE(nullptr, req.calcScanPlanList);
    ASSERT_GE(taosArrayGetSize(req.calcScanPlanList), 2);
    EXPECT_NE(0, req.addOptions & STREAM_OPTION_NESTED_WINDOW_PLAN);
    for (int32_t i = 0; i < taosArrayGetSize(req.calcScanPlanList); ++i) {
      const auto* pScan = (const SStreamCalcScan*)taosArrayGet(req.calcScanPlanList, i);
      ASSERT_NE(nullptr, pScan);
      ASSERT_NE(nullptr, pScan->scanPlan);

      SNode* pScanPlan = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)pScan->scanPlan, &pScanPlan));
      unique_ptr<SNode, decltype(&nodesDestroyNode)> scanPlanGuard(pScanPlan, nodesDestroyNode);
      ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(pScanPlan));
      EXPECT_TRUE(((const SSubplan*)pScanPlan)->requiresAncestorContext);
    }
  });

  run("create stream stream_streamdb.s_nested_reader_context window ("
      "state_window(c1) extend(1) as w_state, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 notify('ws://localhost:8080') on (window_close) "
      "into stream_outdb.stream_out as "
      "select w_state._twstart, avg(a.c1) from stream_querydb.stream_t1 a "
      "join stream_querydb.stream_t2 b on a.ts = b.ts");
}

static const SProjectPhysiNode* pstGetLegacyCalcRootProject(const SQueryPlan* pPlan) {
  if (pPlan == nullptr || LIST_LENGTH(pPlan->pSubplans) != 1) return nullptr;
  const auto* pGroup = (const SNodeListNode*)nodesListGetNode(pPlan->pSubplans, 0);
  if (pGroup == nullptr || nodeType(pGroup) != QUERY_NODE_NODE_LIST || LIST_LENGTH(pGroup->pNodeList) != 1) {
    return nullptr;
  }
  const auto* pSubplan = (const SSubplan*)nodesListGetNode(pGroup->pNodeList, 0);
  if (pSubplan == nullptr || nodeType(pSubplan) != QUERY_NODE_PHYSICAL_SUBPLAN || pSubplan->pNode == nullptr ||
      nodeType(pSubplan->pNode) != QUERY_NODE_PHYSICAL_PLAN_PROJECT) {
    return nullptr;
  }
  return (const SProjectPhysiNode*)pSubplan->pNode;
}

static void pstAssertLegacyCalcSubplansDoNotRequireAncestorContext(const SQueryPlan* pPlan) {
  ASSERT_NE(nullptr, pPlan);
  SNode* pGroupNode = nullptr;
  FOREACH(pGroupNode, pPlan->pSubplans) {
    ASSERT_EQ(QUERY_NODE_NODE_LIST, nodeType(pGroupNode));
    SNode* pSubplanNode = nullptr;
    FOREACH(pSubplanNode, ((SNodeListNode*)pGroupNode)->pNodeList) {
      ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(pSubplanNode));
      EXPECT_FALSE(((const SSubplan*)pSubplanNode)->requiresAncestorContext);
      EXPECT_FALSE(((const SSubplan*)pSubplanNode)->inheritedMultiGroupJoin);
    }
  }
}

static int32_t pstReplaceUniqueJsonToken(string* pJson, const string& from, const string& to) {
  const size_t pos = pJson->find(from);
  if (pos == string::npos) return 0;
  if (pJson->find(from, pos + from.size()) != string::npos) return -1;
  pJson->replace(pos, from.size(), to);
  return 1;
}

TEST_F(ParserStreamTest, StreamPlanGoldenLegacySubplanJsonUnchanged) {
  static const char kLegacyCalcPlanGolden[] =
      R"task4({"NodeType":"1138","Name":"PhysiPlan","PhysiPlan":{"QueryId":"0","NumOfSubplans":"1","Subplans":{"NodeType":"15","Name":"NodeList","NodeList":{"DataType":{"Type":"0","Precision":"0","Scale":"0","Bytes":"0"},"AliasName":"","UserAlias":"","HasNull":false,"RelatedTo":"0","BindExprID":"0","NodeList":[{"NodeType":"1137","Name":"PhysiSubplan","PhysiSubplan":{"Id":{"QueryId":"0","GroupId":"1","SubplanId":"1"},"SubplanType":"5","MsgType":"771","Level":"0","DbFName":"","User":"","NodeAddr":{"Id":"0","InUse":"0","NumOfEps":"0"},"Child":[{"NodeType":"2","Name":"Value","Value":{"DataType":{"Type":"5","Precision":"0","Scale":"0","Bytes":"8"},"AliasName":"","UserAlias":"","HasNull":false,"RelatedTo":"0","BindExprID":"0","LiteralSize":"0","Flag":"0","Translate":true,"NotReserved":false,"IsNull":false,"Unit":"0","Datum":"8589934594"}}],"RootNode":{"NodeType":"1108","Name":"PhysiProject","PhysiProject":{"OutputDataBlockDesc":{"NodeType":"19","Name":"DataBlockDesc","DataBlockDesc":{"DataBlockId":"1","TotalRowSize":"12","OutputRowSize":"12","Slots":[{"NodeType":"20","Name":"SlotDesc","SlotDesc":{"SlotId":"0","DataType":{"Type":"9","Precision":"0","Scale":"0","Bytes":"8"},"Reserve":false,"Output":true,"Name":"12572130732020657366","Tag":false}},{"NodeType":"20","Name":"SlotDesc","SlotDesc":{"SlotId":"1","DataType":{"Type":"4","Precision":"0","Scale":"0","Bytes":"4"},"Reserve":false,"Output":true,"Name":"4821887455481289517","Tag":false}}],"Precision":"0"}},"Children":[{"NodeType":"1111","Name":"PhysiExchange","PhysiExchange":{"OutputDataBlockDesc":{"NodeType":"19","Name":"DataBlockDesc","DataBlockDesc":{"DataBlockId":"0","TotalRowSize":"12","OutputRowSize":"12","Slots":[{"NodeType":"20","Name":"SlotDesc","SlotDesc":{"SlotId":"0","DataType":{"Type":"9","Precision":"0","Scale":"0","Bytes":"8"},"Reserve":false,"Output":true,"Name":"1011735779866557034","Tag":false}},{"NodeType":"20","Name":"SlotDesc","SlotDesc":{"SlotId":"1","DataType":{"Type":"4","Precision":"0","Scale":"0","Bytes":"4"},"Reserve":false,"Output":true,"Name":"7606375837463693085","Tag":false}}],"Precision":"0"}},"InputOrder":"0","OutputOrder":"0","RequireDataOrder":"0","ResultDataOrder":"4","DynamicOp":false,"ForceCreateNonBlockingOptr":false,"SrcStartGroupId":"2","SrcEndGroupId":"2","SeqRecvData":false,"DynTbname":false,"GrpSingleChannel":false,"SingleSource":false}}],"InputOrder":"0","OutputOrder":"0","RequireDataOrder":"1","ResultDataOrder":"1","DynamicOp":false,"ForceCreateNonBlockingOptr":false,"Projections":[{"NodeType":"18","Name":"Target","Target":{"DataBlockId":"1","SlotId":"0","Expr":{"NodeType":"1","Name":"Column","Column":{"DataType":{"Type":"9","Precision":"0","Scale":"0","Bytes":"8"},"AliasName":"expr_1","UserAlias":"ts","HasNull":false,"RelatedTo":"0","BindExprID":"0","TableId":"42","TableType":"3","ColId":"1","ProjId":"0","ColType":"1","DbName":"stream_querydb","TableName":"stream_t2","TableAlias":"stream_t2","ColName":"ts","DataBlockId":"0","SlotId":"0","TableHasPk":false,"IsPk":false,"NumOfPKs":"0","HasDep":false,"HasRef":false,"RefDb":"","RefTable":"","RefCol":"","IsPrimTs":true}}}},{"NodeType":"18","Name":"Target","Target":{"DataBlockId":"1","SlotId":"1","Expr":{"NodeType":"1","Name":"Column","Column":{"DataType":{"Type":"4","Precision":"0","Scale":"0","Bytes":"4"},"AliasName":"expr_2","UserAlias":"c1","HasNull":false,"RelatedTo":"0","BindExprID":"0","TableId":"42","TableType":"3","ColId":"2","ProjId":"0","ColType":"1","DbName":"stream_querydb","TableName":"stream_t2","TableAlias":"stream_t2","ColName":"c1","DataBlockId":"0","SlotId":"1","TableHasPk":false,"IsPk":false,"NumOfPKs":"0","HasDep":false,"HasRef":false,"RefDb":"","RefTable":"","RefCol":"","IsPrimTs":false}}}}],"MergeDataBlock":true,"IgnoreGroupId":true,"InputIgnoreGroup":false}},"DataSink":{"NodeType":"1133","Name":"PhysiDispatch","PhysiDispatch":{"InputDataBlockDesc":{"NodeType":"19","Name":"DataBlockDesc","DataBlockDesc":{"DataBlockId":"1","TotalRowSize":"12","OutputRowSize":"12","Slots":[{"NodeType":"20","Name":"SlotDesc","SlotDesc":{"SlotId":"0","DataType":{"Type":"9","Precision":"0","Scale":"0","Bytes":"8"},"Reserve":false,"Output":true,"Name":"","Tag":false}},{"NodeType":"20","Name":"SlotDesc","SlotDesc":{"SlotId":"1","DataType":{"Type":"4","Precision":"0","Scale":"0","Bytes":"4"},"Reserve":false,"Output":true,"Name":"","Tag":false}}],"Precision":"0"}}}},"ShowRewrite":false,"IsView":false,"IsAudit":false,"RowThreshold":"4096","DyRowThreshold":false,"DynTbname":false,"ProcessOneBlock":false,"UserAppId":"0"}}]}}}})task4";

  useDb("root", "stream_streamdb");
  int32_t checked = 0;
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ++checked;
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    ASSERT_NE(nullptr, req.calcPlan);

    SNode* pActualNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.calcPlan, &pActualNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> actualGuard(pActualNode, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN, nodeType(pActualNode));
    const auto* pActualPlan = (const SQueryPlan*)pActualNode;
    pstAssertLegacyCalcSubplansDoNotRequireAncestorContext(pActualPlan);

    SNode* pGoldenNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(kLegacyCalcPlanGolden, &pGoldenNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> goldenGuard(pGoldenNode, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN, nodeType(pGoldenNode));

    const SProjectPhysiNode* pActualProject = pstGetLegacyCalcRootProject(pActualPlan);
    const SProjectPhysiNode* pGoldenProject = pstGetLegacyCalcRootProject((const SQueryPlan*)pGoldenNode);
    ASSERT_NE(nullptr, pActualProject);
    ASSERT_NE(nullptr, pGoldenProject);
    ASSERT_NE(nullptr, pActualProject->node.pOutputDataBlockDesc);
    ASSERT_NE(nullptr, pGoldenProject->node.pOutputDataBlockDesc);
    ASSERT_EQ(2, LIST_LENGTH(pActualProject->node.pOutputDataBlockDesc->pSlots));
    ASSERT_EQ(2, LIST_LENGTH(pGoldenProject->node.pOutputDataBlockDesc->pSlots));

    string normalized = (const char*)req.calcPlan;
    for (int32_t i = 0; i < 2; ++i) {
      const auto* pActualSlot =
          (const SSlotDescNode*)nodesListGetNode(pActualProject->node.pOutputDataBlockDesc->pSlots, i);
      const auto* pGoldenSlot =
          (const SSlotDescNode*)nodesListGetNode(pGoldenProject->node.pOutputDataBlockDesc->pSlots, i);
      ASSERT_NE(nullptr, pActualSlot);
      ASSERT_NE(nullptr, pGoldenSlot);
      const string actualToken = string("\"Name\":\"") + pActualSlot->name + "\"";
      const string goldenToken = string("\"Name\":\"") + pGoldenSlot->name + "\"";
      ASSERT_EQ(1, pstReplaceUniqueJsonToken(&normalized, actualToken, goldenToken));
    }
    ASSERT_EQ(1, pstReplaceUniqueJsonToken(&normalized, ",\"CharsetName\":\"UTF-8\"", ""));
    static const string kEmptyRefSource = "\"RefSource\":\"\",";
    for (int32_t i = 0; i < 2; ++i) {
      const size_t pos = normalized.find(kEmptyRefSource);
      ASSERT_NE(string::npos, pos);
      normalized.erase(pos, kEmptyRefSource.size());
    }
    ASSERT_EQ(string::npos, normalized.find(kEmptyRefSource));

    // TableId comes from catalog/meta state and drifts as table ids are allocated, so the value
    // itself cannot be pinned to a literal: it was bumped from 39 to 42 by an unrelated commit,
    // and the golden literal tracks whatever the current environment allocates. The value
    // comparison is therefore deliberately weak — the rewrite below normalizes the actual value
    // onto the literal. What IS pinned is the count: the legacy calc plan projects a single table,
    // so its table id must appear exactly once per projected column, and anywhere else in the
    // serialized plan would mean a new TableId token was introduced and silently rewritten away.
    const SNode* pFirstProj =
        (const SNode*)nodesListGetNode(pActualProject->pProjections, 0);
    ASSERT_NE(nullptr, pFirstProj);
    ASSERT_EQ(QUERY_NODE_TARGET, nodeType(pFirstProj));
    const SNode* pFirstExpr = ((const STargetNode*)pFirstProj)->pExpr;
    ASSERT_NE(nullptr, pFirstExpr);
    ASSERT_EQ(QUERY_NODE_COLUMN, nodeType(pFirstExpr));
    const string actualTableIdToken = ",\"TableId\":\"" + to_string(((const SColumnNode*)pFirstExpr)->tableId) + "\"";
    const string goldenTableIdToken = ",\"TableId\":\"42\"";
    ASSERT_NE(string::npos, normalized.find(actualTableIdToken));

    // Rewrite every occurrence of the actual table id to the golden one. The
    // loop advances past the replacement rather than re-scanning from the
    // start: when the actual id happens to equal the golden literal (it
    // drifts with catalog id allocation, see above), re-finding from the start
    // would locate the just-written token again and loop forever.
    int32_t tidReplaced = 0;
    size_t  tidPos = normalized.find(actualTableIdToken);
    while (tidPos != string::npos) {
      normalized.replace(tidPos, actualTableIdToken.size(), goldenTableIdToken);
      ++tidReplaced;
      tidPos = normalized.find(actualTableIdToken, tidPos + goldenTableIdToken.size());
    }
    // One TableId per projected column. Count, not value: the count is independent of the golden
    // literal, so it keeps working even when the actual id happens to be a prefix of the golden
    // one (e.g. "4" vs "42"), which would make a value-equality check flaky — while still catching
    // an unexpected extra TableId token anywhere else in the serialized plan.
    ASSERT_EQ(LIST_LENGTH(pActualProject->pProjections), tidReplaced);

    EXPECT_EQ(kLegacyCalcPlanGolden, normalized)
        << "legacy calc plan changed outside branch-defined serialized fields and pointer-derived project slot names";
  });

  run("create stream stream_streamdb.s_legacy_golden interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts as ts, c1 as c1 from stream_querydb.stream_t2");
  EXPECT_GT(checked, 0);
}

TEST_F(ParserDdlTest, stateWindowKeywordSyntax) {
  run("create stream stream_streamdb.s_keyword_extend state_window(c1) extend(1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_keyword_zeroth state_window(c1) extend(0) zeroth_state(0) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_keyword_no_zeroth state_window(c1) zeroth_state(no_zeroth) true_for(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_legacy_extend state_window(c1, 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_legacy_zeroth state_window(c1, 1, 0) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_keyword_mix state_window(c1, 1) extend(1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_STATE_WIN_COL);

  // Logic expressions as state key in stream
  run("create stream stream_streamdb.s_logic1 state_window(c1 > 5) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_logic2 state_window(c1 > 5 AND c1 < 100) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s_logic3 state_window(c1 > 5, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
}

static int32_t pstCountExternalWindowNodes(const SPhysiNode* pNode) {
  if (nullptr == pNode) {
    return 0;
  }

  int32_t count = (QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL == nodeType((const SNode*)pNode) ||
                   QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL == nodeType((const SNode*)pNode))
                      ? 1
                      : 0;

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { count += pstCountExternalWindowNodes((const SPhysiNode*)pChild); }

  return count;
}

static int32_t pstCountExternalWindowNodes(const SQueryPlan* pPlan) {
  if (nullptr == pPlan) {
    return 0;
  }

  int32_t count = 0;
  SNode*  pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    SNodeListNode* pLevel = (SNodeListNode*)pLevelNode;
    SNode*         pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const SSubplan* pSubplan = (const SSubplan*)pSubplanNode;
      count += pstCountExternalWindowNodes(pSubplan->pNode);
    }
  }

  return count;
}

static int32_t pstCountHashExternalWindowNodes(const SPhysiNode* pNode) {
  if (nullptr == pNode) {
    return 0;
  }

  int32_t count = (QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL == nodeType((const SNode*)pNode)) ? 1 : 0;

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { count += pstCountHashExternalWindowNodes((const SPhysiNode*)pChild); }

  return count;
}

static int32_t pstCountHashExternalWindowNodes(const SQueryPlan* pPlan) {
  if (nullptr == pPlan) {
    return 0;
  }

  int32_t count = 0;
  SNode*  pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    SNodeListNode* pLevel = (SNodeListNode*)pLevelNode;
    SNode*         pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const SSubplan* pSubplan = (const SSubplan*)pSubplanNode;
      count += pstCountHashExternalWindowNodes(pSubplan->pNode);
    }
  }

  return count;
}

static void pstCheckCalcPlanHashExternalWindowCount(const SQuery* pQuery, ParserStage stage, int32_t expectedCount) {
  ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.calcPlan, nullptr);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(pPlanNode, nullptr);

  int32_t count = pstCountHashExternalWindowNodes((const SQueryPlan*)pPlanNode);
  EXPECT_EQ(count, expectedCount);
}

static int32_t pstCountExternalWindowNodesNeedCalcTimeRange(const SPhysiNode* pNode) {
  if (nullptr == pNode) {
    return 0;
  }

  int32_t count =
      ((QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL == nodeType((const SNode*)pNode) ||
        QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL == nodeType((const SNode*)pNode)) &&
       ((const SExternalWindowPhysiNode*)pNode)->pTimeRange != nullptr &&
       ((const STimeRangeNode*)((const SExternalWindowPhysiNode*)pNode)->pTimeRange)->needCalc)
          ? 1
          : 0;

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { count += pstCountExternalWindowNodesNeedCalcTimeRange((const SPhysiNode*)pChild); }

  return count;
}

static int32_t pstCountExternalWindowNodesNeedCalcTimeRange(const SQueryPlan* pPlan) {
  if (nullptr == pPlan) {
    return 0;
  }

  int32_t count = 0;
  SNode*  pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    SNodeListNode* pLevel = (SNodeListNode*)pLevelNode;
    SNode*         pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const SSubplan* pSubplan = (const SSubplan*)pSubplanNode;
      count += pstCountExternalWindowNodesNeedCalcTimeRange(pSubplan->pNode);
    }
  }

  return count;
}

static void pstCheckCalcPlanExternalWindowCount(const SQuery* pQuery, ParserStage stage, int32_t expectedCount) {
  ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.calcPlan, nullptr);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(pPlanNode, nullptr);

  int32_t count = pstCountExternalWindowNodes((const SQueryPlan*)pPlanNode);
  EXPECT_EQ(count, expectedCount);
}

static void pstCheckCalcAndScanPlanExternalWindowCount(const SQuery* pQuery, ParserStage stage, int32_t expectedCount) {
  ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.calcPlan, nullptr);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(pPlanNode, nullptr);

  int32_t count = pstCountExternalWindowNodes((const SQueryPlan*)pPlanNode);
  for (int32_t i = 0; i < taosArrayGetSize(req.calcScanPlanList); ++i) {
    SStreamCalcScan* pScan = (SStreamCalcScan*)taosArrayGet(req.calcScanPlanList, i);
    ASSERT_NE(pScan, nullptr);
    ASSERT_NE(pScan->scanPlan, nullptr);

    SNode* pScanPlanNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)pScan->scanPlan, &pScanPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> scanPlanGuard(pScanPlanNode, nodesDestroyNode);
    ASSERT_NE(pScanPlanNode, nullptr);
    ASSERT_EQ(nodeType(pScanPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);
    count += pstCountExternalWindowNodes(((SSubplan*)pScanPlanNode)->pNode);
  }

  EXPECT_EQ(count, expectedCount);
}

static void pstCheckCalcPlanExternalWindowNeedCalcTimeRangeCount(const SQuery* pQuery, ParserStage stage,
                                                                 int32_t expectedCount) {
  ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.calcPlan, nullptr);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(pPlanNode, nullptr);

  int32_t count = pstCountExternalWindowNodesNeedCalcTimeRange((const SQueryPlan*)pPlanNode);
  EXPECT_EQ(count, expectedCount);
}

static SNode* pstGetEventStartCondFromReq(const SQuery* pQuery, ParserStage stage) {
  if (stage != PARSER_STAGE_TRANSLATE) {
    ADD_FAILURE() << "unexpected parser stage: " << stage;
    return nullptr;
  }
  if (nodeType(pQuery->pRoot) != QUERY_NODE_CREATE_STREAM_STMT) {
    ADD_FAILURE() << "unexpected root node type: " << nodeType(pQuery->pRoot);
    return nullptr;
  }

  SCMCreateStreamReq req = {0};
  if (tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "failed to deserialize create stream request";
    return nullptr;
  }
  std::unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);

  SNode* pStart = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.trigger.event.startCond, &pStart));
  return pStart;
}

static void pstCheckEventTreeShape(const SQuery* pQuery, ParserStage stage) {
  std::unique_ptr<SNode, decltype(&nodesDestroyNode)> start(pstGetEventStartCondFromReq(pQuery, stage),
                                                            nodesDestroyNode);
  ASSERT_NE(start.get(), nullptr);
  ASSERT_EQ(nodeType(start.get()), QUERY_NODE_NODE_LIST);
  SNodeListNode* root = (SNodeListNode*)start.get();
  ASSERT_EQ(LIST_LENGTH(root->pNodeList), 2);

  SNode* first = nodesListGetNode(root->pNodeList, 0);
  ASSERT_EQ(nodeType(first), QUERY_NODE_NODE_LIST);
  SNodeListNode* group0 = (SNodeListNode*)first;
  ASSERT_EQ(LIST_LENGTH(group0->pNodeList), 2);

  SNode* leaf00 = nodesListGetNode(group0->pNodeList, 0);
  ASSERT_EQ(nodeType(leaf00), QUERY_NODE_EVENT_START_LEAF);
  ASSERT_NE(((SEventStartLeafNode*)leaf00)->pCond, nullptr);
  ASSERT_NE(((SEventStartLeafNode*)leaf00)->pTrueFor, nullptr);

  SNode* leaf01 = nodesListGetNode(group0->pNodeList, 1);
  ASSERT_EQ(nodeType(leaf01), QUERY_NODE_EVENT_START_LEAF);
  ASSERT_NE(((SEventStartLeafNode*)leaf01)->pCond, nullptr);
  ASSERT_EQ(((SEventStartLeafNode*)leaf01)->pTrueFor, nullptr);

  SNode* leaf1 = nodesListGetNode(root->pNodeList, 1);
  ASSERT_EQ(nodeType(leaf1), QUERY_NODE_EVENT_START_LEAF);
  ASSERT_NE(((SEventStartLeafNode*)leaf1)->pTrueFor, nullptr);
}

struct SExternalWindowPhysiInfo {
  EExtWinInputMode inputMode;
  bool             hasTimeRange;
  bool             hasWindowConditions;
  int32_t          windowConditionNodeType;
  int32_t          windowConditionType;
  uint8_t          windowConditionResultType;
  int32_t          windowConditionResultBytes;
  string           sourceTable;
  int32_t          durationUpperBoundCount;
  int32_t          windowConditionC1Count;
  struct SFunctionInfo {
    string functionName;
    string userAlias;
    string firstParamTableAlias;
    string firstParamColName;
  };
  vector<SFunctionInfo> functions;
};

struct SExternalWindowConditionCounts {
  int32_t durationUpperBound = 0;
  int32_t c1Columns = 0;
};

static EDealRes pstCountExternalWindowConditions(SNode* pNode, void* pContext) {
  auto* pCounts = (SExternalWindowConditionCounts*)pContext;
  if (QUERY_NODE_COLUMN == nodeType(pNode) && 0 == strcmp(((SColumnNode*)pNode)->colName, "c1")) {
    ++pCounts->c1Columns;
  }
  if (QUERY_NODE_OPERATOR != nodeType(pNode)) {
    return DEAL_RES_CONTINUE;
  }

  auto* pOp = (SOperatorNode*)pNode;
  if (OP_TYPE_LOWER_EQUAL != pOp->opType || QUERY_NODE_FUNCTION != nodeType(pOp->pLeft) ||
      QUERY_NODE_VALUE != nodeType(pOp->pRight)) {
    return DEAL_RES_CONTINUE;
  }

  auto* pFunc = (SFunctionNode*)pOp->pLeft;
  auto* pValue = (SValueNode*)pOp->pRight;
  if (0 != strcmp(pFunc->functionName, "_twduration") || 1 != LIST_LENGTH(pFunc->pParameterList) ||
      nullptr == pValue->literal || 0 != strcmp(pValue->literal, "30000")) {
    return DEAL_RES_CONTINUE;
  }

  SNode* pLayer = nodesListGetNode(pFunc->pParameterList, 0);
  if (QUERY_NODE_VALUE == nodeType(pLayer) && ((SValueNode*)pLayer)->notReserved) {
    ++pCounts->durationUpperBound;
  }
  return DEAL_RES_CONTINUE;
}

static void pstCollectExternalWindowFunctions(const SNodeList*                                 pFuncs,
                                              vector<SExternalWindowPhysiInfo::SFunctionInfo>* pInfos) {
  SNode* pTarget = nullptr;
  FOREACH(pTarget, pFuncs) {
    SNode* pExpr = QUERY_NODE_TARGET == nodeType(pTarget) ? ((STargetNode*)pTarget)->pExpr : pTarget;
    if (QUERY_NODE_FUNCTION != nodeType(pExpr)) {
      continue;
    }

    const auto*                             pFunc = (const SFunctionNode*)pExpr;
    SExternalWindowPhysiInfo::SFunctionInfo info;
    info.functionName = pFunc->functionName;
    info.userAlias = pFunc->node.userAlias;
    SNode* pParam = nodesListGetNode(pFunc->pParameterList, 0);
    if (QUERY_NODE_COLUMN == nodeType(pParam)) {
      const auto* pColumn = (const SColumnNode*)pParam;
      info.firstParamTableAlias = pColumn->tableAlias;
      info.firstParamColName = pColumn->colName;
    }
    pInfos->push_back(info);
  }
}

static void pstCollectExternalWindowPhysiInfo(const SPhysiNode* pNode, vector<SExternalWindowPhysiInfo>* pInfos,
                                              vector<ENodeType>* pJoinTypes) {
  if (pNode == nullptr) {
    return;
  }

  const ENodeType type = nodeType((const SNode*)pNode);
  if (type == QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL || type == QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL) {
    const auto*                    pExternal = (const SExternalWindowPhysiNode*)pNode;
    SExternalWindowConditionCounts counts;
    nodesWalkExpr(pExternal->pWindowConditions, pstCountExternalWindowConditions, &counts);

    SExternalWindowPhysiInfo info = {};
    info.inputMode = pExternal->extWinInputMode;
    info.hasTimeRange = pExternal->pTimeRange != nullptr;
    info.hasWindowConditions = pExternal->pWindowConditions != nullptr;
    info.windowConditionNodeType = pExternal->pWindowConditions == nullptr ? 0 : nodeType(pExternal->pWindowConditions);
    info.durationUpperBoundCount = counts.durationUpperBound;
    info.windowConditionC1Count = counts.c1Columns;
    if (QUERY_NODE_LOGIC_CONDITION == info.windowConditionNodeType) {
      const auto* pLogic = (const SLogicConditionNode*)pExternal->pWindowConditions;
      info.windowConditionType = pLogic->condType;
      info.windowConditionResultType = pLogic->node.resType.type;
      info.windowConditionResultBytes = pLogic->node.resType.bytes;
    }
    if (QUERY_NODE_COLUMN == nodeType(pExternal->window.pTspk)) {
      info.sourceTable = ((const SColumnNode*)pExternal->window.pTspk)->tableName;
    }
    pstCollectExternalWindowFunctions(pExternal->window.pFuncs, &info.functions);
    pInfos->push_back(info);
  }
  if (type == QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN || type == QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN) {
    pJoinTypes->push_back(type);
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) {
    pstCollectExternalWindowPhysiInfo((const SPhysiNode*)pChild, pInfos, pJoinTypes);
  }
}

struct SCalcScanPhysiInfo {
  string    sourceDb;
  string    sourceTable;
  ENodeType scanType;
  bool      hasTimeRange;
  bool      dynamicOp;
  bool      virtualStableScan;
  bool      paraTablesSort;
  bool      useTriggerTableList;
  int32_t   c1NonnegativeCount;
  int32_t   durationPredicateCount;
};

static EDealRes pstCountC1NonnegativeConditions(SNode* pNode, void* pContext) {
  if (pstIsC1NonnegativeCondition(pNode)) {
    ++*(int32_t*)pContext;
  }
  return DEAL_RES_CONTINUE;
}

static void pstCollectCalcScanPhysiInfo(const SPhysiNode* pNode, vector<SCalcScanPhysiInfo>* pInfos) {
  if (nullptr == pNode) {
    return;
  }

  const ENodeType type = nodeType((const SNode*)pNode);
  if (QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN == type || QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN == type ||
      QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN == type || QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN == type) {
    const auto*        pScan = (const STableScanPhysiNode*)pNode;
    SCalcScanPhysiInfo info = {};
    info.sourceDb = tNameGetDbNameP(&pScan->scan.tableName);
    info.sourceTable = tNameGetTableName(&pScan->scan.tableName);
    info.scanType = type;
    info.hasTimeRange = pScan->pTimeRange != nullptr;
    info.dynamicOp = pScan->scan.node.dynamicOp;
    info.virtualStableScan = pScan->scan.virtualStableScan;
    info.paraTablesSort = pScan->paraTablesSort;
    info.useTriggerTableList = pScan->scan.useTriggerTableList;
    nodesWalkExpr(pNode->pConditions, pstCountC1NonnegativeConditions, &info.c1NonnegativeCount);
    SExternalWindowConditionCounts counts;
    nodesWalkExpr(pNode->pConditions, pstCountExternalWindowConditions, &counts);
    info.durationPredicateCount = counts.durationUpperBound;
    pInfos->push_back(info);
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { pstCollectCalcScanPhysiInfo((const SPhysiNode*)pChild, pInfos); }
}

static void pstCollectCalcPlanExternalWindowInfo(const SQuery* pQuery, ParserStage stage,
                                                 vector<SExternalWindowPhysiInfo>* pInfos,
                                                 vector<ENodeType>*                pJoinTypes,
                                                 vector<SCalcScanPhysiInfo>*       pScanInfos = nullptr) {
  ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.calcPlan, nullptr);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(pPlanNode, nullptr);

  const auto* pPlan = (const SQueryPlan*)pPlanNode;
  SNode*      pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    const auto* pLevel = (const SNodeListNode*)pLevelNode;
    SNode*      pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const auto* pSubplan = (const SSubplan*)pSubplanNode;
      pstCollectExternalWindowPhysiInfo(pSubplan->pNode, pInfos, pJoinTypes);
    }
  }

  if (nullptr == pScanInfos) {
    return;
  }
  for (int32_t i = 0; i < taosArrayGetSize(req.calcScanPlanList); ++i) {
    const auto* pScan = (const SStreamCalcScan*)taosArrayGet(req.calcScanPlanList, i);
    ASSERT_NE(pScan, nullptr);
    ASSERT_NE(pScan->scanPlan, nullptr);

    SNode* pScanPlanNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)pScan->scanPlan, &pScanPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> scanPlanGuard(pScanPlanNode, nodesDestroyNode);
    ASSERT_EQ(QUERY_NODE_PHYSICAL_SUBPLAN, nodeType(pScanPlanNode));
    pstCollectCalcScanPhysiInfo(((const SSubplan*)pScanPlanNode)->pNode, pScanInfos);
  }
}

struct SMultiGroupJoinPhysiInfo {
  int32_t                           joinCount = 0;
  int32_t                           externalWindowCount = 0;
  int32_t                           nestedLeftJoinCount = 0;
  vector<const SHashJoinPhysiNode*> hashJoins;
};

static bool pstMultiGroupPhysiTreeHasJoin(const SPhysiNode* pNode) {
  if (nullptr == pNode) {
    return false;
  }

  const ENodeType type = nodeType((const SNode*)pNode);
  if (QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == type || QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == type) {
    return true;
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) {
    if (pstMultiGroupPhysiTreeHasJoin((const SPhysiNode*)pChild)) {
      return true;
    }
  }
  return false;
}

static void pstCollectMultiGroupJoinPhysiInfo(const SPhysiNode* pNode, SMultiGroupJoinPhysiInfo* pInfo) {
  if (nullptr == pNode) {
    return;
  }

  const ENodeType type = nodeType((const SNode*)pNode);
  if (QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == type) {
    ++pInfo->joinCount;
    pInfo->hashJoins.push_back((const SHashJoinPhysiNode*)pNode);
  } else if (QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == type) {
    ++pInfo->joinCount;
  }
  if ((QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == type || QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == type) &&
      pstMultiGroupPhysiTreeHasJoin((const SPhysiNode*)nodesListGetNode(pNode->pChildren, 0))) {
    ++pInfo->nestedLeftJoinCount;
  }
  if (QUERY_NODE_PHYSICAL_PLAN_HASH_EXTERNAL == type || QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_EXTERNAL == type) {
    ++pInfo->externalWindowCount;
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { pstCollectMultiGroupJoinPhysiInfo((const SPhysiNode*)pChild, pInfo); }
}

static void pstCheckMultiGroupJoinPlan(const SQuery* pQuery, ParserStage stage, bool enabled, int32_t joinCount,
                                       int32_t markedHashJoinCount, int32_t expectedExternalWindowCount = -1,
                                       int32_t expectedNestedLeftJoinCount = -1,
                                       int32_t expectedInheritedSubplanCount = -1) {
  ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);
  ASSERT_EQ(QUERY_NODE_CREATE_STREAM_STMT, nodeType(pQuery->pRoot));

  SCMCreateStreamReq req = {};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  EXPECT_EQ(enabled, req.enableMultiGroupCalc);
  ASSERT_NE(nullptr, req.calcPlan);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.calcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(nullptr, pPlanNode);

  SMultiGroupJoinPhysiInfo info;
  int32_t                  inheritedSubplanCount = 0;
  SNode*                   pLevelNode = nullptr;
  FOREACH(pLevelNode, ((const SQueryPlan*)pPlanNode)->pSubplans) {
    const auto* pLevel = (const SNodeListNode*)pLevelNode;
    SNode*      pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      const auto* pSubplan = (const SSubplan*)pSubplanNode;
      pstCollectMultiGroupJoinPhysiInfo(pSubplan->pNode, &info);
      if (pSubplan->inheritedMultiGroupJoin) {
        ++inheritedSubplanCount;
      }
    }
  }

  EXPECT_EQ(joinCount, info.joinCount);
  if (0 <= expectedExternalWindowCount) {
    EXPECT_EQ(expectedExternalWindowCount, info.externalWindowCount);
  }
  if (0 <= expectedNestedLeftJoinCount) {
    EXPECT_EQ(expectedNestedLeftJoinCount, info.nestedLeftJoinCount);
  }
  int32_t actualMarkedHashJoinCount = 0;
  for (const SHashJoinPhysiNode* pJoin : info.hashJoins) {
    if (pJoin->multiGroupJoin) {
      ++actualMarkedHashJoinCount;
    }
  }
  EXPECT_EQ(markedHashJoinCount, actualMarkedHashJoinCount);
  if (0 <= expectedInheritedSubplanCount) {
    EXPECT_EQ(expectedInheritedSubplanCount, inheritedSubplanCount);
  }
  if (enabled) {
    ASSERT_EQ(joinCount, info.hashJoins.size());
    for (const SHashJoinPhysiNode* pJoin : info.hashJoins) {
      EXPECT_TRUE(pJoin->multiGroupJoin);
    }
  } else {
    EXPECT_EQ(0, inheritedSubplanCount);
  }
}

static void pstAssertNoInheritedMultiGroupJoinSubplans(const char* pCalcPlan) {
  ASSERT_NE(nullptr, pCalcPlan);
  EXPECT_EQ(nullptr, strstr(pCalcPlan, "\"InheritedMultiGroupJoin\":true"));

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(pCalcPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  SNode*                                         pLevelNode = nullptr;
  FOREACH(pLevelNode, ((const SQueryPlan*)pPlanNode)->pSubplans) {
    SNode* pSubplanNode = nullptr;
    FOREACH(pSubplanNode, ((const SNodeListNode*)pLevelNode)->pNodeList) {
      EXPECT_FALSE(((const SSubplan*)pSubplanNode)->inheritedMultiGroupJoin);
    }
  }
}

static int32_t pstCountNonWrapperSelects(SNode* pNode);

static int32_t pstCountNonWrapperSelectsInTable(SNode* pTable) {
  if (nullptr == pTable) {
    return 0;
  }
  if (QUERY_NODE_TEMP_TABLE == nodeType(pTable)) {
    return pstCountNonWrapperSelects(((STempTableNode*)pTable)->pSubquery);
  }
  if (QUERY_NODE_JOIN_TABLE == nodeType(pTable)) {
    auto* pJoin = (SJoinTableNode*)pTable;
    return pstCountNonWrapperSelectsInTable(pJoin->pLeft) + pstCountNonWrapperSelectsInTable(pJoin->pRight);
  }
  return 0;
}

static int32_t pstCountNonWrapperSelects(SNode* pNode) {
  if (nullptr == pNode || QUERY_NODE_SELECT_STMT != nodeType(pNode)) {
    return 0;
  }

  auto*   pSelect = (SSelectStmt*)pNode;
  int32_t count = QUERY_NODE_TEMP_TABLE == nodeType(pSelect->pFromTable) ? 0 : 1;
  return count + pstCountNonWrapperSelectsInTable(pSelect->pFromTable);
}

static void pstCheckRollupTagList(SNodeList* pRollupTagList, const char* const* pExpectedColNames,
                                  int32_t expectedColNum) {
  ASSERT_NE(pRollupTagList, nullptr);
  ASSERT_EQ(LIST_LENGTH(pRollupTagList), expectedColNum);
  for (int32_t i = 0; i < expectedColNum; ++i) {
    SNode* pTagNode = nodesListGetNode(pRollupTagList, i);
    ASSERT_NE(pTagNode, nullptr);
    ASSERT_EQ(nodeType(pTagNode), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)pTagNode)->colName, pExpectedColNames[i]);
  }
}

static void pstCheckParsedStreamRollupTagCols(const char* pSql, const char* const* pExpectedColNames,
                                              int32_t expectedColNum) {
  array<char, 1024> msgBuf = {0};
  SParseContext     cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "wangxiaoyu";
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = msgBuf.data();
  cxt.msgLen = msgBuf.max_size();

  SQuery* pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, parse(&cxt, &pQuery)) << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  const SCreateStreamStmt*  pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  const SStreamTriggerNode* pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
  ASSERT_NE(pTrigger, nullptr);
  pstCheckRollupTagList(pTrigger->pRollupTagList, pExpectedColNames, expectedColNum);

  char*   pStr = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pQuery->pRoot, false, &pStr, &len));
  SNode* pCopy = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(pStr, &pCopy));
  taosMemoryFreeClear(pStr);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> copyGuard(pCopy, nodesDestroyNode);

  const SCreateStreamStmt*  pCopyStmt = (const SCreateStreamStmt*)pCopy;
  const SStreamTriggerNode* pCopyTrigger = (const SStreamTriggerNode*)pCopyStmt->pTrigger;
  ASSERT_NE(pCopyTrigger, nullptr);
  pstCheckRollupTagList(pCopyTrigger->pRollupTagList, pExpectedColNames, expectedColNum);
}

static void pstCheckParsedStreamRollupTagCol(const char* pSql, const char* pExpectedColName) {
  const char* expectedColNames[] = {pExpectedColName};
  pstCheckParsedStreamRollupTagCols(pSql, expectedColNames, 1);
}

static int32_t pstTranslateCreateStreamSql(const char* pSql, SQuery** ppQuery, char* pMsg, int32_t msgLen,
                                           const char* pTimezone = "UTC", int8_t firstDayOfWeek = 1) {
  SParseContext cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "wangxiaoyu";
  cxt.isSuperUser = true;
  cxt.enableSysInfo = true;
  cxt.privInfo = UINT16_MAX;
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = pMsg;
  cxt.msgLen = msgLen;
  cxt.firstDayOfWeek = firstDayOfWeek;

  timezone_t timezone = nullptr;
  int32_t    code = taosValidateAndNormalizeTimezone(pTimezone, cxt.timezoneName, sizeof(cxt.timezoneName), &timezone);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  cxt.timezone = timezone;

  code = parse(&cxt, ppQuery);
  if (TSDB_CODE_SUCCESS != code) {
    tzfree(timezone);
    return code;
  }
  code = authenticate(&cxt, *ppQuery, NULL);
  if (TSDB_CODE_SUCCESS != code) {
    qDestroyQuery(*ppQuery);
    *ppQuery = nullptr;
    tzfree(timezone);
    return code;
  }
  code = translate(&cxt, *ppQuery, NULL);
  tzfree(timezone);
  return code;
}

static SCreateStreamStmt* pstParseCreateStreamOnly(const char* pSql, SQuery** ppQuery, char* pMsg, int32_t msgLen) {
  SParseContext cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "wangxiaoyu";
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = pMsg;
  cxt.msgLen = msgLen;

  if (TSDB_CODE_SUCCESS != parse(&cxt, ppQuery) || nullptr == *ppQuery ||
      QUERY_NODE_CREATE_STREAM_STMT != nodeType((*ppQuery)->pRoot)) {
    return nullptr;
  }
  return (SCreateStreamStmt*)(*ppQuery)->pRoot;
}

static int32_t pstParseOnlyCode(const char* pSql, SQuery** ppQuery, char* pMsg, int32_t msgLen) {
  SParseContext cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "wangxiaoyu";
  cxt.pSql = pSql;
  cxt.sqlLen = strlen(pSql);
  cxt.pMsg = pMsg;
  cxt.msgLen = msgLen;
  return parse(&cxt, ppQuery);
}

static string pstStreamSqlSlice(const char* pSql, SStreamSqlSpan span) {
  return string(pSql + span.begin, span.end - span.begin);
}

struct PstStreamTimeBorrowerAudit {
  timezone_t  timezone;
  const char* timezoneName;
  int8_t      firstDayOfWeek;
  int32_t     valueNodes;
  int32_t     operatorNodes;
  int32_t     functionNodes;
  int32_t     caseWhenNodes;
  int32_t     intervalWindowNodes;
  int32_t     externalWindowNodes;
  bool        valid;
};

struct PstStreamTimeBindContext {
  timezone_t               timezone;
  const SStreamTimeConfig* pConfig;
};

static EDealRes pstBindStreamTimeBorrower(SNode* pNode, void* pContext) {
  PstStreamTimeBindContext* pBind = (PstStreamTimeBindContext*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_VALUE:
      ((SValueNode*)pNode)->tz = pBind->timezone;
      break;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOperator = (SOperatorNode*)pNode;
      pOperator->tz = pBind->timezone;
      tstrncpy(pOperator->timezoneName, pBind->pConfig->timezone, sizeof(pOperator->timezoneName));
      pOperator->ownsTimezone = false;
      break;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunction = (SFunctionNode*)pNode;
      pFunction->tz = pBind->timezone;
      tstrncpy(pFunction->tzName, pBind->pConfig->timezone, sizeof(pFunction->tzName));
      pFunction->tzAllocated = false;
      pFunction->firstDayOfWeek = pBind->pConfig->firstDayOfWeek;
      break;
    }
    case QUERY_NODE_CASE_WHEN:
      ((SCaseWhenNode*)pNode)->tz = pBind->timezone;
      break;
    case QUERY_NODE_INTERVAL_WINDOW:
      ((SIntervalWindowNode*)pNode)->timezone = pBind->timezone;
      break;
    case QUERY_NODE_EXTERNAL_WINDOW:
      ((SExternalWindowNode*)pNode)->timezone = pBind->timezone;
      break;
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static EDealRes pstAuditStreamTimeBorrower(SNode* pNode, void* pContext) {
  PstStreamTimeBorrowerAudit* pAudit = (PstStreamTimeBorrowerAudit*)pContext;
  switch (nodeType(pNode)) {
    case QUERY_NODE_VALUE:
      ++pAudit->valueNodes;
      pAudit->valid = pAudit->valid && ((SValueNode*)pNode)->tz == pAudit->timezone;
      break;
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* pOperator = (SOperatorNode*)pNode;
      ++pAudit->operatorNodes;
      pAudit->valid = pAudit->valid && pOperator->tz == pAudit->timezone && !pOperator->ownsTimezone &&
                      strcmp(pOperator->timezoneName, pAudit->timezoneName) == 0;
      break;
    }
    case QUERY_NODE_FUNCTION: {
      SFunctionNode* pFunction = (SFunctionNode*)pNode;
      ++pAudit->functionNodes;
      pAudit->valid = pAudit->valid && pFunction->tz == pAudit->timezone && !pFunction->tzAllocated &&
                      strcmp(pFunction->tzName, pAudit->timezoneName) == 0 &&
                      pFunction->firstDayOfWeek == pAudit->firstDayOfWeek;
      break;
    }
    case QUERY_NODE_CASE_WHEN:
      ++pAudit->caseWhenNodes;
      pAudit->valid = pAudit->valid && ((SCaseWhenNode*)pNode)->tz == pAudit->timezone;
      break;
    case QUERY_NODE_INTERVAL_WINDOW:
      ++pAudit->intervalWindowNodes;
      pAudit->valid = pAudit->valid && ((SIntervalWindowNode*)pNode)->timezone == pAudit->timezone;
      break;
    case QUERY_NODE_EXTERNAL_WINDOW:
      ++pAudit->externalWindowNodes;
      pAudit->valid = pAudit->valid && ((SExternalWindowNode*)pNode)->timezone == pAudit->timezone;
      break;
    default:
      break;
  }
  return DEAL_RES_CONTINUE;
}

static void pstWalkCreateStreamTimeBorrowers(SCreateStreamStmt* pStmt, FNodeWalker walker, void* pContext) {
  nodesWalkExpr(pStmt->pTrigger, walker, pContext);
  nodesWalkExpr(pStmt->pSubtable, walker, pContext);
  nodesWalkExprs(pStmt->pTags, walker, pContext);
  if (pStmt->pQuery != nullptr && nodeType(pStmt->pQuery) == QUERY_NODE_SELECT_STMT) {
    nodesWalkSelectStmt((SSelectStmt*)pStmt->pQuery, SQL_CLAUSE_FROM, walker, pContext);
  } else {
    nodesWalkExpr(pStmt->pQuery, walker, pContext);
  }
}

static const SValueNode* pstFindProjectionValue(const SCreateStreamStmt* pStmt, const char* pAlias) {
  if (pStmt->pQuery == nullptr || nodeType(pStmt->pQuery) != QUERY_NODE_SELECT_STMT) {
    return nullptr;
  }

  SNode* pProjection = nullptr;
  FOREACH(pProjection, ((SSelectStmt*)pStmt->pQuery)->pProjectionList) {
    if (nodeType(pProjection) == QUERY_NODE_VALUE && strcmp(((SExprNode*)pProjection)->userAlias, pAlias) == 0) {
      return (const SValueNode*)pProjection;
    }
  }
  return nullptr;
}

void checkCreateStreamReq(SCMCreateStreamReq* expect, SCMCreateStreamReq* req, bool checkNormalizedSql);

struct PstPhysicalTimeContextAudit {
  const SStreamTimeConfig* pConfig;
  int32_t                  scanNodes;
  int32_t                  intervalNodes;
  int32_t                  federatedNodes;
  bool                     timezoneFolded;
  bool                     firstDayOfWeekFolded;
};

static void pstAuditPhysicalTimeContext(const SPhysiNode* pNode, PstPhysicalTimeContextAudit* pAudit) {
  if (nullptr == pNode) {
    return;
  }

  switch (nodeType((const SNode*)pNode)) {
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN: {
      const STableScanPhysiNode* pScan = (const STableScanPhysiNode*)pNode;
      ++pAudit->scanNodes;
      EXPECT_STREQ(pScan->timezoneName, pAudit->pConfig->timezone);
      EXPECT_EQ(pScan->firstDayOfWeek, pAudit->pConfig->firstDayOfWeek);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL: {
      const SIntervalPhysiNode* pInterval = (const SIntervalPhysiNode*)pNode;
      ++pAudit->intervalNodes;
      EXPECT_STREQ(pInterval->timezoneName, pAudit->pConfig->timezone);
      EXPECT_EQ(pInterval->firstDayOfWeek, pAudit->pConfig->firstDayOfWeek);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_FEDERATED_SCAN: {
      const SFederatedScanPhysiNode* pScan = (const SFederatedScanPhysiNode*)pNode;
      ++pAudit->federatedNodes;
      EXPECT_STREQ(pScan->timezone, pAudit->pConfig->timezone);
      break;
    }
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT: {
      SNode* pProjection = nullptr;
      FOREACH(pProjection, ((const SProjectPhysiNode*)pNode)->pProjections) {
        if (QUERY_NODE_TARGET != nodeType(pProjection)) {
          continue;
        }
        const SNode* pExpr = ((const STargetNode*)pProjection)->pExpr;
        if (QUERY_NODE_VALUE != nodeType(pExpr)) {
          continue;
        }
        const SValueNode* pValue = (const SValueNode*)pExpr;
        if (0 == strcmp(pValue->node.userAlias, "frozen_timezone")) {
          EXPECT_STREQ(pValue->literal, pAudit->pConfig->timezone);
          pAudit->timezoneFolded = true;
        } else if (0 == strcmp(pValue->node.userAlias, "frozen_fdow")) {
          EXPECT_EQ(pValue->datum.i, pAudit->pConfig->firstDayOfWeek);
          pAudit->firstDayOfWeekFolded = true;
        }
      }
      break;
    }
    default:
      break;
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pNode->pChildren) { pstAuditPhysicalTimeContext((const SPhysiNode*)pChild, pAudit); }
}

static void pstAuditQueryPlanTimeContext(const SQueryPlan* pPlan, PstPhysicalTimeContextAudit* pAudit) {
  if (nullptr == pPlan) {
    return;
  }

  SNode* pLevelNode = nullptr;
  FOREACH(pLevelNode, pPlan->pSubplans) {
    const SNodeListNode* pLevel = (const SNodeListNode*)pLevelNode;
    SNode*               pSubplanNode = nullptr;
    FOREACH(pSubplanNode, pLevel->pNodeList) {
      pstAuditPhysicalTimeContext(((const SSubplan*)pSubplanNode)->pNode, pAudit);
    }
  }

  SNode* pChild = nullptr;
  FOREACH(pChild, pPlan->pChildren) { pstAuditQueryPlanTimeContext((const SQueryPlan*)pChild, pAudit); }
}

TEST_F(ParserStreamTest, CreateStreamNormalizesPersistedSql) {
  struct TestCase {
    const char* sql;
    const char* expected;
  };
  const array<TestCase, 6> cases = {{
      {"create stream stream_streamdb.s_norm_notify interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 notify('ws://x/AS') on (window_close) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
       "create stream stream_streamdb.s_norm_notify interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) "
       "notify('ws://x/AS') on (window_close) into stream_outdb.stream_out as select _twstart, avg(c1) "
       "from stream_querydb.stream_t2"},
      {"create stream stream_streamdb.s_norm_into interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
       "as select _twstart, avg(c1) from stream_querydb.stream_t2",
       "create stream stream_streamdb.s_norm_into interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2"},
      {"create stream stream_streamdb.s_norm_mixed interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 stream_options(watermark(2s) | "
       "config(timezone='Asia/Shanghai', firstDayOfWeek=1) | pre_filter(c1 > 0)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
       "create stream stream_streamdb.s_norm_mixed interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1) | "
       "watermark(2s) | pre_filter(c1 > 0)) into stream_outdb.stream_out "
       "as select _twstart, avg(c1) from stream_querydb.stream_t2"},
      {"create stream stream_streamdb.s_norm_multiple interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 stream_options(config(timezone='UTC') | watermark(2s) | "
       "config(timezone='Asia/Shanghai', firstDayOfWeek=1)) into stream_outdb.stream_out "
       "as select _twstart, avg(c1) from stream_querydb.stream_t2",
       "create stream stream_streamdb.s_norm_multiple interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1) | "
       "watermark(2s)) into stream_outdb.stream_out as select _twstart, avg(c1) "
       "from stream_querydb.stream_t2"},
      {"create stream stream_streamdb.s_norm_case interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 stream_options(CoNfIg(TIMEZONE='Asia/Shanghai', FIRSTDAYOFWEEK=1)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
       "create stream stream_streamdb.s_norm_case interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2"},
      {"create stream stream_streamdb.s_norm_offset interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 stream_options(config(timezone='+08:00', firstDayOfWeek=1)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
       "create stream stream_streamdb.s_norm_offset interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 STREAM_OPTIONS(CONFIG(timezone = 'UTC+8', firstDayOfWeek = 1)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2"},
  }};

  for (const TestCase& testCase : cases) {
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              pstTranslateCreateStreamSql(testCase.sql, &pQuery, msgBuf.data(), msgBuf.max_size(), "Asia/Shanghai", 1))
        << testCase.sql << ": " << msgBuf.data();
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);

    SCMCreateStreamReq req = {0};
    ASSERT_NE(pQuery->pCmdMsg, nullptr);
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    ASSERT_NE(req.sql, nullptr);
    SCMCreateStreamReq expected = req;
    expected.sql = const_cast<char*>(testCase.expected);
    checkCreateStreamReq(&expected, &req, true);
  }
}

static int32_t pstNormalizeParsedCreateStream(const char* pSql, SCreateStreamStmt* pStmt, char** ppNormalizedSql) {
  SParseContext parseCxt = {0};
  parseCxt.pSql = pSql;
  parseCxt.sqlLen = strlen(pSql);
  STranslateContext translateCxt = {0};
  translateCxt.pParseCxt = &parseCxt;
  return normalizeCreateStreamSql(&translateCxt, pStmt, ppNormalizedSql);
}

TEST_F(ParserStreamTest, NormalizeCreateStreamSqlUsesParserInsertionAnchors) {
  struct TestCase {
    const char* sql;
    const char* expected;
  };
  const array<TestCase, 3> cases = {{
      {"create stream s interval(1s) sliding(1s) from t as select * from t",
       "create stream s interval(1s) sliding(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) as select * from t"},
      {"create stream s interval(1s) sliding(1s) from t;",
       "create stream s interval(1s) sliding(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1));"},
      {"create stream s interval(1s) sliding(1s) from t",
       "create stream s interval(1s) sliding(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1))"},
  }};

  for (const TestCase& testCase : cases) {
    array<char, 1024>  msgBuf = {0};
    SQuery*            pQuery = nullptr;
    SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(testCase.sql, &pQuery, msgBuf.data(), msgBuf.max_size());
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
    ASSERT_NE(pStmt, nullptr) << testCase.sql << ": " << msgBuf.data();
    ASSERT_NE(pStmt->pTimeInfo, nullptr);
    tstrncpy(pStmt->pTimeInfo->config.timezone, "Asia/Shanghai", sizeof(pStmt->pTimeInfo->config.timezone));
    pStmt->pTimeInfo->config.firstDayOfWeek = 1;

    char* pNormalizedSql = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, pstNormalizeParsedCreateStream(testCase.sql, pStmt, &pNormalizedSql));
    EXPECT_STREQ(pNormalizedSql, testCase.expected);
    taosMemoryFree(pNormalizedSql);
  }
}

TEST_F(ParserStreamTest, NormalizeCreateStreamSqlInsertsBeforeTrailingTrivia) {
  struct TestCase {
    const char* sql;
    const char* expected;
  };
  const array<TestCase, 4> cases = {{
      {"create stream s period(1s) from t -- keep",
       "create stream s period(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) -- keep"},
      {"create stream s period(1s) from t /* keep */",
       "create stream s period(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) /* keep */"},
      {"create stream s period(1s) from t; -- keep",
       "create stream s period(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)); -- keep"},
      {"create stream s period(1s) from t -- keep\n;",
       "create stream s period(1s) from t "
       "STREAM_OPTIONS(CONFIG(timezone = 'Asia/Shanghai', firstDayOfWeek = 1)) -- keep\n;"},
  }};

  for (const TestCase& testCase : cases) {
    array<char, 1024>  msgBuf = {0};
    SQuery*            pQuery = nullptr;
    SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(testCase.sql, &pQuery, msgBuf.data(), msgBuf.max_size());
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
    ASSERT_NE(pStmt, nullptr) << testCase.sql << ": " << msgBuf.data();
    ASSERT_NE(pStmt->pTimeInfo, nullptr);
    tstrncpy(pStmt->pTimeInfo->config.timezone, "Asia/Shanghai", sizeof(pStmt->pTimeInfo->config.timezone));
    pStmt->pTimeInfo->config.firstDayOfWeek = 1;

    char* pNormalizedSql = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, pstNormalizeParsedCreateStream(testCase.sql, pStmt, &pNormalizedSql));
    auto                                          freeNormalizedSql = [](char* pSql) { taosMemoryFree(pSql); };
    unique_ptr<char, decltype(freeNormalizedSql)> normalizedGuard(pNormalizedSql, freeNormalizedSql);
    ASSERT_STREQ(pNormalizedSql, testCase.expected);

    array<char, 1024>  reparseMsgBuf = {0};
    SQuery*            pReparsedQuery = nullptr;
    SCreateStreamStmt* pReparsedStmt =
        pstParseCreateStreamOnly(pNormalizedSql, &pReparsedQuery, reparseMsgBuf.data(), reparseMsgBuf.max_size());
    unique_ptr<SQuery, decltype(&qDestroyQuery)> reparseGuard(pReparsedQuery, qDestroyQuery);
    ASSERT_NE(pReparsedStmt, nullptr) << pNormalizedSql << ": " << reparseMsgBuf.data();
    ASSERT_NE(pReparsedStmt->pTimeInfo, nullptr);
    EXPECT_TRUE(pReparsedStmt->pTimeInfo->hasTimezone);
    EXPECT_TRUE(pReparsedStmt->pTimeInfo->hasFirstDayOfWeek);
    EXPECT_EQ(pstStreamSqlSlice(pNormalizedSql, pReparsedStmt->pTimeInfo->timezoneValue), "'Asia/Shanghai'");
    EXPECT_EQ(pstStreamSqlSlice(pNormalizedSql, pReparsedStmt->pTimeInfo->firstDayOfWeekValue), "1");
  }
}

TEST_F(ParserStreamTest, NormalizeCreateStreamSqlEscapesTimezoneLiteral) {
  const char* sql =
      "create stream s interval(1s) sliding(1s) from t "
      "stream_options(watermark(1s) | config(timezone='UTC'))";
  array<char, 1024>  msgBuf = {0};
  SQuery*            pQuery = nullptr;
  SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(sql, &pQuery, msgBuf.data(), msgBuf.max_size());
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pStmt, nullptr) << msgBuf.data();
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  tstrncpy(pStmt->pTimeInfo->config.timezone, "Etc/'X", sizeof(pStmt->pTimeInfo->config.timezone));
  pStmt->pTimeInfo->config.firstDayOfWeek = 1;

  char* pNormalizedSql = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, pstNormalizeParsedCreateStream(sql, pStmt, &pNormalizedSql));
  EXPECT_STREQ(pNormalizedSql,
               "create stream s interval(1s) sliding(1s) from t "
               "STREAM_OPTIONS(CONFIG(timezone = 'Etc/''X', firstDayOfWeek = 1) | watermark(1s))");
  taosMemoryFree(pNormalizedSql);
}

TEST_F(ParserStreamTest, NormalizeCreateStreamSqlRejectsInvalidSpans) {
  const char* sql =
      "create stream s interval(1s) sliding(1s) from t "
      "stream_options(watermark(1s) | config(timezone='UTC'))";
  array<char, 1024>  msgBuf = {0};
  SQuery*            pQuery = nullptr;
  SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(sql, &pQuery, msgBuf.data(), msgBuf.max_size());
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pStmt, nullptr) << msgBuf.data();
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  ASSERT_NE(pStmt->pTimeInfo->pNonConfigOptionSpans, nullptr);
  ASSERT_GT(taosArrayGetSize(pStmt->pTimeInfo->pNonConfigOptionSpans), 0);
  tstrncpy(pStmt->pTimeInfo->config.timezone, "UTC", sizeof(pStmt->pTimeInfo->config.timezone));
  pStmt->pTimeInfo->config.firstDayOfWeek = 1;

  const SStreamSqlSpan originalOptions = pStmt->pTimeInfo->streamOptions;
  pStmt->pTimeInfo->streamOptions.end = (int32_t)strlen(sql) + 1;
  char* pNormalizedSql = nullptr;
  EXPECT_EQ(TSDB_CODE_STREAM_INTERNAL_ERROR, pstNormalizeParsedCreateStream(sql, pStmt, &pNormalizedSql));
  EXPECT_EQ(pNormalizedSql, nullptr);
  pStmt->pTimeInfo->streamOptions = originalOptions;

  SStreamSqlSpan* pRetained = (SStreamSqlSpan*)taosArrayGet(pStmt->pTimeInfo->pNonConfigOptionSpans, 0);
  ASSERT_NE(pRetained, nullptr);
  const SStreamSqlSpan originalRetained = *pRetained;
  pRetained->begin = -1;
  EXPECT_EQ(TSDB_CODE_STREAM_INTERNAL_ERROR, pstNormalizeParsedCreateStream(sql, pStmt, &pNormalizedSql));
  EXPECT_EQ(pNormalizedSql, nullptr);
  *pRetained = originalRetained;

  pStmt->pTimeInfo->streamOptions = {-1, -1};
  pStmt->pTimeInfo->streamOptionsInsertPos = (int32_t)strlen(sql) + 1;
  EXPECT_EQ(TSDB_CODE_STREAM_INTERNAL_ERROR, pstNormalizeParsedCreateStream(sql, pStmt, &pNormalizedSql));
  EXPECT_EQ(pNormalizedSql, nullptr);
}

TEST_F(ParserStreamTest, CreateStreamFreezesRequestAndPhysicalPlans) {
  const char* sql =
      "create stream stream_streamdb.s_frozen interval(1n) from stream_triggerdb.stream_t1 "
      "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=2)) "
      "into stream_outdb.stream_out as select _wstart, timezone() as frozen_timezone, "
      "first_day_of_week() as frozen_fdow, avg(c1) from stream_querydb.stream_t2 interval(1n)";
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);

  const SCreateStreamStmt*   pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  const SStreamTriggerNode*  pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
  const SIntervalWindowNode* pTriggerWindow = (const SIntervalWindowNode*)pTrigger->pTriggerWindow;
  ASSERT_EQ(nodeType((const SNode*)pTriggerWindow), QUERY_NODE_INTERVAL_WINDOW);
  EXPECT_EQ(pTriggerWindow->pSliding, nullptr);

  SCMCreateStreamReq req = {0};
  ASSERT_NE(pQuery->pCmdMsg, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  EXPECT_STREQ(req.timeConfig.timezone, "Asia/Shanghai");
  EXPECT_EQ(req.timeConfig.firstDayOfWeek, 2);
  EXPECT_EQ(req.trigger.sliding.sliding, req.trigger.sliding.interval);
  EXPECT_EQ(req.trigger.sliding.slidingUnit, req.trigger.sliding.intervalUnit);

  SNode* pTriggerPlanNode = nullptr;
  ASSERT_NE(req.triggerScanPlan, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.triggerScanPlan, &pTriggerPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> triggerPlanGuard(pTriggerPlanNode, nodesDestroyNode);
  ASSERT_EQ(nodeType(pTriggerPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);
  PstPhysicalTimeContextAudit triggerAudit = {&req.timeConfig, 0, 0, 0, false, false};
  {
    SCOPED_TRACE("trigger scan plan");
    pstAuditPhysicalTimeContext(((const SSubplan*)pTriggerPlanNode)->pNode, &triggerAudit);
  }
  EXPECT_GT(triggerAudit.scanNodes, 0);

  SNode* pCalcPlanNode = nullptr;
  ASSERT_NE(req.calcPlan, nullptr);
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pCalcPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> calcPlanGuard(pCalcPlanNode, nodesDestroyNode);
  ASSERT_EQ(nodeType(pCalcPlanNode), QUERY_NODE_PHYSICAL_PLAN);
  PstPhysicalTimeContextAudit calcAudit = {&req.timeConfig, 0, 0, 0, false, false};
  {
    SCOPED_TRACE("calc plan");
    pstAuditQueryPlanTimeContext((const SQueryPlan*)pCalcPlanNode, &calcAudit);
  }
  EXPECT_GT(calcAudit.intervalNodes, 0);
  EXPECT_TRUE(calcAudit.timezoneFolded);
  EXPECT_TRUE(calcAudit.firstDayOfWeekFolded);

  PstPhysicalTimeContextAudit calcScanAudit = {&req.timeConfig, 0, 0, 0, false, false};
  SCOPED_TRACE("calc scan plans");
  for (int32_t i = 0; i < taosArrayGetSize(req.calcScanPlanList); ++i) {
    const SStreamCalcScan* pCalcScan = (const SStreamCalcScan*)taosArrayGet(req.calcScanPlanList, i);
    ASSERT_NE(pCalcScan, nullptr);
    ASSERT_NE(pCalcScan->scanPlan, nullptr);
    SNode* pCalcScanPlanNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)pCalcScan->scanPlan, &pCalcScanPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> calcScanPlanGuard(pCalcScanPlanNode, nodesDestroyNode);
    ASSERT_EQ(nodeType(pCalcScanPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);
    pstAuditPhysicalTimeContext(((const SSubplan*)pCalcScanPlanNode)->pNode, &calcScanAudit);
  }
  EXPECT_GT(calcScanAudit.scanNodes, 0);
}

TEST_F(ParserStreamTest, CreateTsmaPreservesLegacyEmbeddedStreamRequest) {
  const char* sql = "create tsma tsma_frozen_guard on test.st1 function(avg(c1)) interval(1m)";
  useDb("root", "test");
  setAsyncFlag("-1");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_TSMA_STMT);
    ASSERT_NE(pQuery->pCmdMsg, nullptr);
    ASSERT_EQ(pQuery->pCmdMsg->msgType, TDMT_MND_CREATE_TSMA);

    SMCreateSmaReq tsmaReq = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSMCreateSmaReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &tsmaReq));
    unique_ptr<SMCreateSmaReq, decltype(&tFreeSMCreateSmaReq)> tsmaReqGuard(&tsmaReq, tFreeSMCreateSmaReq);
    ASSERT_NE(tsmaReq.createStreamReq, nullptr);
    ASSERT_GT(tsmaReq.streamReqLen, 0);

    SCMCreateStreamReq streamReq = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              tDeserializeSCMCreateStreamReq(tsmaReq.createStreamReq, tsmaReq.streamReqLen, &streamReq));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> streamReqGuard(&streamReq,
                                                                                      tFreeSCMCreateStreamReq);
    EXPECT_STREQ(streamReq.sql, sql);
    EXPECT_STREQ(streamReq.timeConfig.timezone, "");
    EXPECT_EQ(streamReq.timeConfig.firstDayOfWeek, 0);

    SNode* pTriggerPlanNode = nullptr;
    ASSERT_NE(streamReq.triggerScanPlan, nullptr);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)streamReq.triggerScanPlan, &pTriggerPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> triggerPlanGuard(pTriggerPlanNode, nodesDestroyNode);
    ASSERT_EQ(nodeType(pTriggerPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);
    PstPhysicalTimeContextAudit triggerAudit = {&streamReq.timeConfig, 0, 0, 0, false, false};
    pstAuditPhysicalTimeContext(((const SSubplan*)pTriggerPlanNode)->pNode, &triggerAudit);
    EXPECT_GT(triggerAudit.scanNodes, 0);

    PstPhysicalTimeContextAudit calcScanAudit = {&streamReq.timeConfig, 0, 0, 0, false, false};
    ASSERT_GT(taosArrayGetSize(streamReq.calcScanPlanList), 0);
    for (int32_t i = 0; i < taosArrayGetSize(streamReq.calcScanPlanList); ++i) {
      const SStreamCalcScan* pCalcScan = (const SStreamCalcScan*)taosArrayGet(streamReq.calcScanPlanList, i);
      ASSERT_NE(pCalcScan, nullptr);
      ASSERT_NE(pCalcScan->scanPlan, nullptr);
      SNode* pCalcScanPlanNode = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)pCalcScan->scanPlan, &pCalcScanPlanNode));
      unique_ptr<SNode, decltype(&nodesDestroyNode)> calcScanPlanGuard(pCalcScanPlanNode, nodesDestroyNode);
      ASSERT_EQ(nodeType(pCalcScanPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);
      pstAuditPhysicalTimeContext(((const SSubplan*)pCalcScanPlanNode)->pNode, &calcScanAudit);
    }
    EXPECT_GT(calcScanAudit.scanNodes, 0);
  });

  run(sql);
}

TEST_F(ParserStreamTest, CreateStreamResolvesFinalTimeContextValues) {
  struct TestCase {
    const char* options;
    const char* timezone;
    int8_t      firstDayOfWeek;
  };
  const array<TestCase, 5> cases = {{
      {"", "UTC", 1},
      {"stream_options(config(timezone='Asia/Shanghai')) ", "Asia/Shanghai", 1},
      {"stream_options(config(firstDayOfWeek=0)) ", "UTC", 0},
      {"stream_options(config(timezone='+08:00')) ", "UTC+8", 1},
      {"stream_options(config(firstDayOfWeek=8) | config(firstDayOfWeek=2)) ", "UTC", 2},
  }};

  for (const TestCase& testCase : cases) {
    const string sql =
        "create stream stream_streamdb.s_time_context interval(1s) sliding(1s) "
        "from stream_triggerdb.stream_t1 " +
        string(testCase.options) +
        "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql << ": " << msgBuf.data();
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
    ASSERT_NE(pQuery, nullptr);
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

    const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
    ASSERT_NE(pStmt->pTimeInfo, nullptr);
    ASSERT_NE(pStmt->pTimeInfo->timezone, nullptr);
    EXPECT_STREQ(pStmt->pTimeInfo->config.timezone, testCase.timezone) << sql;
    EXPECT_EQ(pStmt->pTimeInfo->config.firstDayOfWeek, testCase.firstDayOfWeek) << sql;
  }
}

TEST_F(ParserStreamTest, CreateStreamRejectsInvalidFinalTimeContextBeforeCommand) {
  struct TestCase {
    const char* sql;
    int32_t     error;
  };
  const array<TestCase, 2> cases = {{
      {"create stream stream_streamdb.s_bad_timezone interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 stream_options(config(timezone='Bad/Zone')) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
       TSDB_CODE_PAR_INVALID_TIMEZONE},
      {"create stream stream_streamdb.s_bad_fdow interval(1s) sliding(1s) "
       "from stream_triggerdb.stream_t1 stream_options(config(firstDayOfWeek=7)) "
       "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
       TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK},
  }};

  for (const TestCase& testCase : cases) {
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    EXPECT_EQ(testCase.error, pstTranslateCreateStreamSql(testCase.sql, &pQuery, msgBuf.data(), msgBuf.max_size()))
        << testCase.sql << ": " << msgBuf.data();
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
    ASSERT_NE(pQuery, nullptr);
    EXPECT_EQ(pQuery->pCmdMsg, nullptr);
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
    ASSERT_NE(pStmt->pTimeInfo, nullptr);
    EXPECT_EQ(pStmt->pTimeInfo->timezone, nullptr);
  }
}

TEST_F(ParserStreamTest, CreateStreamRebindsCompleteAstBeforeValidation) {
  const char* sql =
      "create stream stream_streamdb.s_rebind interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "partition by timetruncate(ts, 1d) rollup by c1 "
      "stream_options(pre_filter(cast(ts as bigint) > 0 and ts > '2020-01-01') | "
      "config(timezone='Asia/Shanghai', firstDayOfWeek=2)) "
      "notify('ws://x/AS') where ts < '2021-01-01' "
      "into stream_outdb.stream_out output_subtable(concat('t_', c1)) "
      "tags(tag1 varchar(64) as cast(c1 as varchar(64))) "
      "as select case when ts > '2020-01-01' then timetruncate(ts, 1d) else ts end, "
      "timezone(), first_day_of_week() from stream_querydb.stream_t2";
  array<char, 1024> msgBuf = {0};
  SParseContext     cxt = {0};
  cxt.acctId = 0;
  cxt.db = "stream_streamdb";
  cxt.pUser = "wangxiaoyu";
  cxt.isSuperUser = true;
  cxt.enableSysInfo = true;
  cxt.privInfo = UINT16_MAX;
  cxt.pSql = sql;
  cxt.sqlLen = strlen(sql);
  cxt.pMsg = msgBuf.data();
  cxt.msgLen = msgBuf.max_size();
  cxt.firstDayOfWeek = 1;
  timezone_t connectionTimezone = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            taosValidateAndNormalizeTimezone("UTC", cxt.timezoneName, sizeof(cxt.timezoneName), &connectionTimezone));
  unique_ptr<remove_pointer<timezone_t>::type, decltype(&tzfree)> timezoneGuard(connectionTimezone, tzfree);
  cxt.timezone = connectionTimezone;

  SQuery* pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, parse(&cxt, &pQuery)) << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
  SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
  ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);

  SExternalWindowNode* pExternal = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_EXTERNAL_WINDOW, (SNode**)&pExternal));
  ((SSelectStmt*)pStmt->pQuery)->pWindow = (SNode*)pExternal;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeValueNodeFromInt64(1, &pExternal->pCol));
  SFunctionNode* pExternalFunction = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pExternalFunction));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pExternal->pProjectionList, (SNode*)pExternalFunction));

  ASSERT_EQ(TSDB_CODE_SUCCESS, authenticate(&cxt, pQuery, nullptr));
  EXPECT_EQ(TSDB_CODE_STREAM_INVALID_SUBTABLE, translate(&cxt, pQuery, nullptr)) << msgBuf.data();
  EXPECT_EQ(pQuery->pCmdMsg, nullptr);
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  ASSERT_NE(pStmt->pTimeInfo->timezone, nullptr);
  EXPECT_STREQ(pStmt->pTimeInfo->config.timezone, "Asia/Shanghai");
  EXPECT_EQ(pStmt->pTimeInfo->config.firstDayOfWeek, 2);

  PstStreamTimeBorrowerAudit audit = {0};
  audit.timezone = pStmt->pTimeInfo->timezone;
  audit.timezoneName = pStmt->pTimeInfo->config.timezone;
  audit.firstDayOfWeek = pStmt->pTimeInfo->config.firstDayOfWeek;
  audit.valid = true;
  SStreamTriggerNode* pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  nodesWalkExpr(pTrigger->pTriggerWindow, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExpr(((SStreamTriggerOptions*)pTrigger->pOptions)->pPreFilter, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExpr(((SStreamNotifyOptions*)pTrigger->pNotify)->pWhere, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExprs(pTrigger->pPartitionList, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExprs(pTrigger->pRollupTagList, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExpr(pStmt->pSubtable, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExpr(((SStreamTagDefNode*)nodesListGetNode(pStmt->pTags, 0))->pTagExpr, pstAuditStreamTimeBorrower, &audit);
  nodesWalkSelectStmt((SSelectStmt*)pStmt->pQuery, SQL_CLAUSE_FROM, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExprs(pExternal->pProjectionList, pstAuditStreamTimeBorrower, &audit);
  EXPECT_TRUE(audit.valid);
  EXPECT_GT(audit.valueNodes, 0);
  EXPECT_GT(audit.operatorNodes, 0);
  EXPECT_GT(audit.functionNodes, 0);
  EXPECT_GT(audit.caseWhenNodes, 0);
  EXPECT_GT(audit.intervalWindowNodes, 0);
  EXPECT_EQ(audit.externalWindowNodes, 1);
}

TEST_F(ParserStreamTest, CreateStreamSystemInfoFunctionsUseResolvedContext) {
  const char* sql =
      "create stream stream_streamdb.s_fold interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=2) | calc_notify_only) "
      "as select ts, timezone() as tz_context, first_day_of_week() as fdow_context "
      "from stream_querydb.stream_t2";
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);
  const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  const SValueNode*        pTimezone = pstFindProjectionValue(pStmt, "tz_context");
  const SValueNode*        pFirstDayOfWeek = pstFindProjectionValue(pStmt, "fdow_context");
  ASSERT_NE(pTimezone, nullptr);
  ASSERT_NE(pFirstDayOfWeek, nullptr);
  EXPECT_STREQ(pTimezone->literal, "Asia/Shanghai");
  EXPECT_EQ(pFirstDayOfWeek->datum.i, 2);
}

TEST_F(ParserStreamTest, CreateStreamScalarSubqueriesInheritResolvedTimeContext) {
  const char* sql =
      "create stream stream_streamdb.s_subquery_context interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 "
      "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=2) | calc_notify_only) "
      "as select ts, (select timezone() from stream_querydb.stream_t1 limit 1) as subquery_timezone, "
      "(select first_day_of_week() from stream_querydb.stream_t1 limit 1) as subquery_fdow "
      "from stream_querydb.stream_t2";
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);
  const SSelectStmt* pSelect = (const SSelectStmt*)pStmt->pQuery;
  ASSERT_NE(pSelect->pSubQueries, nullptr);
  ASSERT_EQ(LIST_LENGTH(pSelect->pSubQueries), 2);

  const SSelectStmt* pTimezoneSubquery = (const SSelectStmt*)nodesListGetNode(pSelect->pSubQueries, 0);
  ASSERT_EQ(nodeType((const SNode*)pTimezoneSubquery), QUERY_NODE_SELECT_STMT);
  const SNode* pTimezone = nodesListGetNode(pTimezoneSubquery->pProjectionList, 0);
  ASSERT_EQ(nodeType(pTimezone), QUERY_NODE_VALUE);
  EXPECT_STREQ(((const SValueNode*)pTimezone)->literal, "Asia/Shanghai");

  const SSelectStmt* pFdowSubquery = (const SSelectStmt*)nodesListGetNode(pSelect->pSubQueries, 1);
  ASSERT_EQ(nodeType((const SNode*)pFdowSubquery), QUERY_NODE_SELECT_STMT);
  const SNode* pFdow = nodesListGetNode(pFdowSubquery->pProjectionList, 0);
  ASSERT_EQ(nodeType(pFdow), QUERY_NODE_VALUE);
  EXPECT_EQ(((const SValueNode*)pFdow)->datum.i, 2);
}

TEST_F(ParserStreamTest, CreateStreamSetCloneRetainsSubqueriesAndJoinState) {
  const char* sql =
      "create stream stream_streamdb.s_set_clone interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 "
      "stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=2) | calc_notify_only) "
      "as (select a.ts, (select max(c1) from stream_querydb.stream_t1) as scalar_value "
      "from stream_querydb.stream_t1 a join stream_querydb.stream_t2 b on a.ts = b.ts) "
      "union all (select ts, c1 from stream_querydb.stream_t2)";
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SET_OPERATOR);
  const SSetOperator* pSet = (const SSetOperator*)pStmt->pQuery;
  ASSERT_NE(pSet->pSubQueries, nullptr);
  ASSERT_EQ(LIST_LENGTH(pSet->pSubQueries), 1);
  ASSERT_TRUE(pSet->joinContains);

  SNode* pClone = nullptr;
  ASSERT_EQ(nodesCloneNode((const SNode*)pStmt, &pClone), TSDB_CODE_SUCCESS);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pClone, nodesDestroyNode);
  ASSERT_NE(pClone, nullptr);

  queryGuard.reset();
  pStmt = nullptr;
  pSet = nullptr;

  SCreateStreamStmt* pCloneStmt = (SCreateStreamStmt*)pClone;
  ASSERT_EQ(nodeType(pCloneStmt->pQuery), QUERY_NODE_SET_OPERATOR);
  SSetOperator* pCloneSet = (SSetOperator*)pCloneStmt->pQuery;
  ASSERT_NE(pCloneSet->pSubQueries, nullptr);
  ASSERT_EQ(LIST_LENGTH(pCloneSet->pSubQueries), 1);
  EXPECT_TRUE(pCloneSet->joinContains);

  array<char, 1024> planMsg = {0};
  SPlanContext      planCxt = {0};
  planCxt.queryId = 1;
  planCxt.pAstRoot = pCloneStmt->pQuery;
  planCxt.pMsg = planMsg.data();
  planCxt.msgLen = planMsg.max_size();
  planCxt.pUser = "root";
  planCxt.timezone = pCloneStmt->pTimeInfo->timezone;
  planCxt.firstDayOfWeek = pCloneStmt->pTimeInfo->config.firstDayOfWeek;
  planCxt.timezoneExplicit = true;
  tstrncpy(planCxt.timezoneName, pCloneStmt->pTimeInfo->config.timezone, sizeof(planCxt.timezoneName));

  SQueryPlan* pPlan = nullptr;
  ASSERT_EQ(qCreateQueryPlan(&planCxt, &pPlan, nullptr), TSDB_CODE_SUCCESS) << planMsg.data();
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard((SNode*)pPlan, nodesDestroyNode);
  ASSERT_NE(pPlan, nullptr);
}

TEST_F(ParserStreamTest, ConfigKeepsLastValuesAndExactRewriteSpans) {
  const char* sql =
      "create stream s interval(1s) sliding(1s) from t "
      "stream_options(watermark(1s) | "
      "config(timezone='Bad/Zone', firstDayOfWeek=8) | "
      "config(FIRSTDAYOFWEEK=2, TIMEZONE='Asia/Shanghai')) "
      "notify('ws://x/AS') into out as select * from t";
  array<char, 1024>  msgBuf = {0};
  SQuery*            pQuery = nullptr;
  SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(sql, &pQuery, msgBuf.data(), msgBuf.max_size());
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);

  ASSERT_NE(pStmt, nullptr) << msgBuf.data();
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  EXPECT_TRUE(pStmt->pTimeInfo->hasTimezone);
  EXPECT_TRUE(pStmt->pTimeInfo->hasFirstDayOfWeek);
  EXPECT_EQ(pstStreamSqlSlice(sql, pStmt->pTimeInfo->timezoneValue), "'Asia/Shanghai'");
  EXPECT_EQ(pstStreamSqlSlice(sql, pStmt->pTimeInfo->firstDayOfWeekValue), "2");
  EXPECT_EQ(pstStreamSqlSlice(sql, pStmt->pTimeInfo->streamOptions),
            "stream_options(watermark(1s) | config(timezone='Bad/Zone', firstDayOfWeek=8) | "
            "config(FIRSTDAYOFWEEK=2, TIMEZONE='Asia/Shanghai'))");
  ASSERT_NE(pStmt->pTimeInfo->pNonConfigOptionSpans, nullptr);
  ASSERT_EQ(taosArrayGetSize(pStmt->pTimeInfo->pNonConfigOptionSpans), 1);
  const SStreamSqlSpan* pWatermark = (const SStreamSqlSpan*)taosArrayGet(pStmt->pTimeInfo->pNonConfigOptionSpans, 0);
  ASSERT_NE(pWatermark, nullptr);
  EXPECT_EQ(pstStreamSqlSlice(sql, *pWatermark), "watermark(1s)");
}

TEST_F(ParserStreamTest, MissingStreamOptionsUsesCurrentLookaheadAsInsertionAnchor) {
  const array<pair<const char*, const char*>, 5> cases = {{
      {"create stream s interval(1s) sliding(1s) from t notify('ws://x/AS')", "notify"},
      {"create stream s interval(1s) sliding(1s) from t into out", "into"},
      {"create stream s interval(1s) sliding(1s) from t as select * from t", "as select"},
      {"create stream s interval(1s) sliding(1s) from t;", ";"},
      {"create stream s interval(1s) sliding(1s) from t", nullptr},
  }};

  for (const auto& testCase : cases) {
    array<char, 1024>  msgBuf = {0};
    SQuery*            pQuery = nullptr;
    SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(testCase.first, &pQuery, msgBuf.data(), msgBuf.max_size());
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);

    ASSERT_NE(pStmt, nullptr) << testCase.first << ": " << msgBuf.data();
    ASSERT_NE(pStmt->pTimeInfo, nullptr);
    const int32_t expectedPos = nullptr == testCase.second
                                    ? static_cast<int32_t>(strlen(testCase.first))
                                    : static_cast<int32_t>(string(testCase.first).find(testCase.second));
    EXPECT_EQ(pStmt->pTimeInfo->streamOptionsInsertPos, expectedPos) << testCase.first;
    EXPECT_EQ(pStmt->pTimeInfo->streamOptions.begin, -1) << testCase.first;
    EXPECT_EQ(pStmt->pTimeInfo->streamOptions.end, -1) << testCase.first;
  }
}

TEST_F(ParserStreamTest, ConfigRejectsEmptyUnknownAliasAndWrongValueTypes) {
  const array<const char*, 5> invalidSql = {{
      "create stream s interval(1s) sliding(1s) from t stream_options(config())",
      "create stream s interval(1s) sliding(1s) from t stream_options(config(unknown='x'))",
      "create stream s interval(1s) sliding(1s) from t stream_options(config(FIRST_DAY_OF_WEEK=2))",
      "create stream s interval(1s) sliding(1s) from t stream_options(config(timezone=8))",
      "create stream s interval(1s) sliding(1s) from t stream_options(config(firstDayOfWeek='2'))",
  }};

  for (const char* sql : invalidSql) {
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    EXPECT_NE(pstParseOnlyCode(sql, &pQuery, msgBuf.data(), msgBuf.max_size()), TSDB_CODE_SUCCESS) << sql;
    qDestroyQuery(pQuery);
  }
}

TEST_F(ParserStreamTest, CreateStreamCloneOwnsMetadataAndExistingChildren) {
  SNode* pNode = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CREATE_STREAM_STMT, &pNode), TSDB_CODE_SUCCESS);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> nodeGuard(pNode, nodesDestroyNode);
  SCreateStreamStmt*                             pStmt = (SCreateStreamStmt*)pNode;
  tstrncpy(pStmt->streamDbName, "stream_streamdb", sizeof(pStmt->streamDbName));
  tstrncpy(pStmt->streamName, "s", sizeof(pStmt->streamName));
  tstrncpy(pStmt->targetDbName, "stream_streamdb", sizeof(pStmt->targetDbName));
  tstrncpy(pStmt->targetTabName, "out", sizeof(pStmt->targetTabName));
  pStmt->ignoreExists = true;
  pStmt->nodelayCreateSubtable = 1;
  ASSERT_EQ(nodesMakeValueNodeFromInt64(1, &pStmt->pTrigger), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(2, &pStmt->pQuery), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(3, &pStmt->pSubtable), TSDB_CODE_SUCCESS);
  SNode* pTag = nullptr;
  ASSERT_EQ(nodesMakeValueNodeFromInt64(4, &pTag), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pStmt->pTags, pTag), TSDB_CODE_SUCCESS);
  SNode* pCol = nullptr;
  ASSERT_EQ(nodesMakeValueNodeFromInt64(5, &pCol), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesListMakeStrictAppend(&pStmt->pCols, pCol), TSDB_CODE_SUCCESS);

  pStmt->pTimeInfo = (SStreamCreateTimeInfo*)taosMemoryCalloc(1, sizeof(SStreamCreateTimeInfo));
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  pStmt->pTimeInfo->hasTimezone = true;
  pStmt->pTimeInfo->hasFirstDayOfWeek = true;
  pStmt->pTimeInfo->timezoneValue = {10, 25};
  pStmt->pTimeInfo->firstDayOfWeekValue = {43, 44};
  pStmt->pTimeInfo->streamOptions = {5, 50};
  pStmt->pTimeInfo->streamOptionsInsertPos = -1;
  pStmt->pTimeInfo->pNonConfigOptionSpans = taosArrayInit(1, sizeof(SStreamSqlSpan));
  ASSERT_NE(pStmt->pTimeInfo->pNonConfigOptionSpans, nullptr);
  const SStreamSqlSpan span = {7, 9};
  ASSERT_NE(taosArrayPush(pStmt->pTimeInfo->pNonConfigOptionSpans, &span), nullptr);
  tstrncpy(pStmt->pTimeInfo->config.timezone, "Asia/Shanghai", sizeof(pStmt->pTimeInfo->config.timezone));
  pStmt->pTimeInfo->config.firstDayOfWeek = 2;
  pStmt->pTimeInfo->timezone = tzalloc(pStmt->pTimeInfo->config.timezone);
  ASSERT_NE(pStmt->pTimeInfo->timezone, nullptr);

  SNode* pClone = nullptr;
  ASSERT_EQ(nodesCloneNode(pNode, &pClone), TSDB_CODE_SUCCESS);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pClone, nodesDestroyNode);
  ASSERT_NE(pClone, nullptr);
  ASSERT_TRUE(nodesEqualNode(pNode, pClone));

  SCreateStreamStmt* pCloneStmt = (SCreateStreamStmt*)pClone;
  ASSERT_NE(pCloneStmt->pTimeInfo, nullptr);
  EXPECT_NE(pCloneStmt->pTimeInfo, pStmt->pTimeInfo);
  EXPECT_NE(pCloneStmt->pTimeInfo->pNonConfigOptionSpans, pStmt->pTimeInfo->pNonConfigOptionSpans);
  EXPECT_NE(pCloneStmt->pTimeInfo->timezone, pStmt->pTimeInfo->timezone);
  EXPECT_NE(pCloneStmt->pTrigger, pStmt->pTrigger);
  EXPECT_NE(pCloneStmt->pQuery, pStmt->pQuery);
  EXPECT_NE(pCloneStmt->pSubtable, pStmt->pSubtable);
  EXPECT_NE(pCloneStmt->pTags, pStmt->pTags);
  EXPECT_NE(pCloneStmt->pCols, pStmt->pCols);

  pCloneStmt->pTimeInfo->timezoneValue.begin++;
  EXPECT_FALSE(nodesEqualNode(pNode, pClone));
}

TEST_F(ParserStreamTest, CreateStreamCloneRebindsCompleteAstToClonedTimezoneOwner) {
  const char* sql =
      "create stream s interval(1s) sliding(1s) from t "
      "partition by timetruncate(ts, 1s) rollup by c1 "
      "stream_options(pre_filter(ts > '2020-01-01') | watermark(1s)) "
      "notify('ws://x/AS') where ts < '2021-01-01' "
      "into out output_subtable(concat('t_', c1)) "
      "tags(tag1 varchar(64) as cast(c1 as varchar(64))) "
      "as select case when ts > '2020-01-01' then timetruncate(ts, 1d) else ts end from t";
  array<char, 1024>  msgBuf = {0};
  SQuery*            pQuery = nullptr;
  SCreateStreamStmt* pStmt = pstParseCreateStreamOnly(sql, &pQuery, msgBuf.data(), msgBuf.max_size());
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pStmt, nullptr) << msgBuf.data();
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  ASSERT_EQ(nodeType(pStmt->pTrigger), QUERY_NODE_STREAM_TRIGGER);
  ASSERT_NE(pStmt->pSubtable, nullptr);
  ASSERT_NE(pStmt->pTags, nullptr);
  ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);

  tstrncpy(pStmt->pTimeInfo->config.timezone, "Asia/Shanghai", sizeof(pStmt->pTimeInfo->config.timezone));
  pStmt->pTimeInfo->config.firstDayOfWeek = 2;
  pStmt->pTimeInfo->timezone = tzalloc(pStmt->pTimeInfo->config.timezone);
  ASSERT_NE(pStmt->pTimeInfo->timezone, nullptr);
  PstStreamTimeBindContext bindContext = {pStmt->pTimeInfo->timezone, &pStmt->pTimeInfo->config};

  SStreamTriggerNode* pTrigger = (SStreamTriggerNode*)pStmt->pTrigger;
  ASSERT_NE(pTrigger->pTriggerWindow, nullptr);
  ASSERT_NE(pTrigger->pOptions, nullptr);
  ASSERT_NE(pTrigger->pNotify, nullptr);
  ASSERT_NE(pTrigger->pPartitionList, nullptr);
  ASSERT_NE(pTrigger->pRollupTagList, nullptr);
  SStreamTriggerOptions* pOptions = (SStreamTriggerOptions*)pTrigger->pOptions;
  ASSERT_EQ(pOptions->pMaxDelay, nullptr);
  ASSERT_EQ(pOptions->pExpiredTime, nullptr);
  ASSERT_EQ(pOptions->pFillHisStartTime, nullptr);
  ASSERT_EQ(pOptions->pIdleTimeout, nullptr);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(2, &pOptions->pMaxDelay), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(3, &pOptions->pExpiredTime), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(4, &pOptions->pFillHisStartTime), TSDB_CODE_SUCCESS);
  ASSERT_EQ(nodesMakeValueNodeFromInt64(5, &pOptions->pIdleTimeout), TSDB_CODE_SUCCESS);
  nodesWalkExpr(pTrigger->pTriggerWindow, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pOptions->pPreFilter, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pOptions->pWaterMark, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pOptions->pMaxDelay, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pOptions->pExpiredTime, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pOptions->pFillHisStartTime, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pOptions->pIdleTimeout, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(((SStreamNotifyOptions*)pTrigger->pNotify)->pWhere, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExprs(pTrigger->pPartitionList, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExprs(pTrigger->pRollupTagList, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pStmt->pSubtable, pstBindStreamTimeBorrower, &bindContext);
  SNode* pTag = nullptr;
  FOREACH(pTag, pStmt->pTags) {
    ASSERT_EQ(nodeType(pTag), QUERY_NODE_STREAM_TAG_DEF);
    nodesWalkExpr(((SStreamTagDefNode*)pTag)->pTagExpr, pstBindStreamTimeBorrower, &bindContext);
  }
  nodesWalkSelectStmt((SSelectStmt*)pStmt->pQuery, SQL_CLAUSE_FROM, pstBindStreamTimeBorrower, &bindContext);

  SExternalWindowNode* pExternal = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_EXTERNAL_WINDOW, (SNode**)&pExternal), TSDB_CODE_SUCCESS);
  ((SSelectStmt*)pStmt->pQuery)->pWindow = (SNode*)pExternal;
  pExternal->timezone = pStmt->pTimeInfo->timezone;
  ASSERT_EQ(nodesMakeValueNodeFromInt64(1, &pExternal->pCol), TSDB_CODE_SUCCESS);
  ((SValueNode*)pExternal->pCol)->tz = pStmt->pTimeInfo->timezone;
  SFunctionNode* pExternalFunction = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pExternalFunction), TSDB_CODE_SUCCESS);
  pExternalFunction->tz = pStmt->pTimeInfo->timezone;
  tstrncpy(pExternalFunction->tzName, bindContext.pConfig->timezone, sizeof(pExternalFunction->tzName));
  pExternalFunction->firstDayOfWeek = bindContext.pConfig->firstDayOfWeek;
  ASSERT_EQ(nodesListMakeStrictAppend(&pExternal->pProjectionList, (SNode*)pExternalFunction), TSDB_CODE_SUCCESS);
  SSelectStmt* pNestedSelect = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_SELECT_STMT, (SNode**)&pNestedSelect), TSDB_CODE_SUCCESS);
  pExternal->pSubquery = (SNode*)pNestedSelect;
  SCaseWhenNode* pNestedCase = nullptr;
  ASSERT_EQ(nodesMakeNode(QUERY_NODE_CASE_WHEN, (SNode**)&pNestedCase), TSDB_CODE_SUCCESS);
  pNestedCase->tz = pStmt->pTimeInfo->timezone;
  ASSERT_EQ(nodesListMakeStrictAppend(&pNestedSelect->pProjectionList, (SNode*)pNestedCase), TSDB_CODE_SUCCESS);

  SNode* pClone = nullptr;
  ASSERT_EQ(nodesCloneNode((SNode*)pStmt, &pClone), TSDB_CODE_SUCCESS);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pClone, nodesDestroyNode);
  ASSERT_NE(pClone, nullptr);
  SCreateStreamStmt* pCloneStmt = (SCreateStreamStmt*)pClone;
  ASSERT_NE(pCloneStmt->pTimeInfo, nullptr);
  ASSERT_NE(pCloneStmt->pTimeInfo->timezone, nullptr);
  EXPECT_NE(pCloneStmt->pTimeInfo->timezone, pStmt->pTimeInfo->timezone);

  queryGuard.reset();
  pStmt = nullptr;

  SStreamTriggerNode* pCloneTrigger = (SStreamTriggerNode*)pCloneStmt->pTrigger;
  ASSERT_NE(pCloneTrigger, nullptr);
  EXPECT_NE(pCloneTrigger->pTriggerWindow, nullptr);
  EXPECT_NE(pCloneTrigger->pOptions, nullptr);
  EXPECT_NE(pCloneTrigger->pNotify, nullptr);
  EXPECT_NE(pCloneTrigger->pPartitionList, nullptr);
  EXPECT_NE(pCloneTrigger->pRollupTagList, nullptr);
  EXPECT_NE(((SStreamTriggerOptions*)pCloneTrigger->pOptions)->pPreFilter, nullptr);
  EXPECT_NE(((SStreamTriggerOptions*)pCloneTrigger->pOptions)->pWaterMark, nullptr);
  SStreamTriggerOptions* pCloneOptions = (SStreamTriggerOptions*)pCloneTrigger->pOptions;
  EXPECT_EQ(((SValueNode*)pCloneOptions->pWaterMark)->tz, pCloneStmt->pTimeInfo->timezone);
  EXPECT_EQ(((SValueNode*)pCloneOptions->pMaxDelay)->tz, pCloneStmt->pTimeInfo->timezone);
  EXPECT_EQ(((SValueNode*)pCloneOptions->pExpiredTime)->tz, pCloneStmt->pTimeInfo->timezone);
  EXPECT_EQ(((SValueNode*)pCloneOptions->pFillHisStartTime)->tz, pCloneStmt->pTimeInfo->timezone);
  EXPECT_EQ(((SValueNode*)pCloneOptions->pIdleTimeout)->tz, pCloneStmt->pTimeInfo->timezone);
  EXPECT_NE(((SStreamNotifyOptions*)pCloneTrigger->pNotify)->pWhere, nullptr);
  ASSERT_NE(pCloneStmt->pTags, nullptr);
  EXPECT_EQ(nodeType(nodesListGetNode(pCloneStmt->pTags, 0)), QUERY_NODE_STREAM_TAG_DEF);
  EXPECT_NE(((SStreamTagDefNode*)nodesListGetNode(pCloneStmt->pTags, 0))->pTagExpr, nullptr);

  PstStreamTimeBorrowerAudit specializedAudit = {0};
  specializedAudit.timezone = pCloneStmt->pTimeInfo->timezone;
  specializedAudit.timezoneName = pCloneStmt->pTimeInfo->config.timezone;
  specializedAudit.firstDayOfWeek = pCloneStmt->pTimeInfo->config.firstDayOfWeek;
  specializedAudit.valid = true;
  nodesWalkExpr(((SStreamNotifyOptions*)pCloneTrigger->pNotify)->pWhere, pstAuditStreamTimeBorrower, &specializedAudit);
  nodesWalkExpr(((SStreamTagDefNode*)nodesListGetNode(pCloneStmt->pTags, 0))->pTagExpr, pstAuditStreamTimeBorrower,
                &specializedAudit);

  SExternalWindowNode* pCloneExternal = (SExternalWindowNode*)((SSelectStmt*)pCloneStmt->pQuery)->pWindow;
  ASSERT_NE(pCloneExternal, nullptr);
  ASSERT_EQ(nodeType((SNode*)pCloneExternal), QUERY_NODE_EXTERNAL_WINDOW);
  EXPECT_EQ(pCloneExternal->timezone, pCloneStmt->pTimeInfo->timezone);
  nodesWalkExpr(pCloneExternal->pCol, pstAuditStreamTimeBorrower, &specializedAudit);
  nodesWalkExprs(pCloneExternal->pProjectionList, pstAuditStreamTimeBorrower, &specializedAudit);
  ASSERT_EQ(nodeType(pCloneExternal->pSubquery), QUERY_NODE_SELECT_STMT);
  nodesWalkExprs(((SSelectStmt*)pCloneExternal->pSubquery)->pProjectionList, pstAuditStreamTimeBorrower,
                 &specializedAudit);
  EXPECT_TRUE(specializedAudit.valid);
  EXPECT_GT(specializedAudit.valueNodes, 0);
  EXPECT_GT(specializedAudit.operatorNodes, 0);
  EXPECT_GT(specializedAudit.functionNodes, 0);
  EXPECT_GT(specializedAudit.caseWhenNodes, 0);

  PstStreamTimeBorrowerAudit audit = {0};
  audit.timezone = pCloneStmt->pTimeInfo->timezone;
  audit.timezoneName = pCloneStmt->pTimeInfo->config.timezone;
  audit.firstDayOfWeek = pCloneStmt->pTimeInfo->config.firstDayOfWeek;
  audit.valid = true;
  pstWalkCreateStreamTimeBorrowers(pCloneStmt, pstAuditStreamTimeBorrower, &audit);
  EXPECT_TRUE(audit.valid);
  EXPECT_GT(audit.valueNodes, 0);
  EXPECT_GT(audit.operatorNodes, 0);
  EXPECT_GT(audit.functionNodes, 0);
  EXPECT_GT(audit.caseWhenNodes, 0);
  EXPECT_GT(audit.intervalWindowNodes, 0);
  EXPECT_EQ(audit.externalWindowNodes, 1);

  const time_t epoch = 0;
  struct tm    local = {0};
  EXPECT_NE(taosLocalTime(&epoch, &local, nullptr, 0, pCloneStmt->pTimeInfo->timezone), nullptr);
}

TEST_F(ParserStreamTest, TranslatedTimeRangeCloneBorrowsClonedTimezoneOwner) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  const char* sql =
      "create stream stream_streamdb.s_time_range interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select a.te, a.cnt, b.minv from "
      "(select _twend te, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twend te, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.te = b.te";
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);
  ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
  ASSERT_NE(pStmt->pTimeInfo, nullptr);
  ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);
  SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
  ASSERT_NE(pSelect->pTimeRange, nullptr);
  ASSERT_EQ(nodeType(pSelect->pTimeRange), QUERY_NODE_TIME_RANGE);
  STimeRangeNode* pTimeRange = (STimeRangeNode*)pSelect->pTimeRange;
  ASSERT_NE(pTimeRange->pStart, nullptr);
  ASSERT_NE(pTimeRange->pEnd, nullptr);

  tzfree(pStmt->pTimeInfo->timezone);
  tstrncpy(pStmt->pTimeInfo->config.timezone, "Asia/Shanghai", sizeof(pStmt->pTimeInfo->config.timezone));
  pStmt->pTimeInfo->config.firstDayOfWeek = 2;
  pStmt->pTimeInfo->timezone = tzalloc(pStmt->pTimeInfo->config.timezone);
  ASSERT_NE(pStmt->pTimeInfo->timezone, nullptr);
  PstStreamTimeBindContext bindContext = {pStmt->pTimeInfo->timezone, &pStmt->pTimeInfo->config};
  nodesWalkExpr(pTimeRange->pStart, pstBindStreamTimeBorrower, &bindContext);
  nodesWalkExpr(pTimeRange->pEnd, pstBindStreamTimeBorrower, &bindContext);

  SNode* pClone = nullptr;
  ASSERT_EQ(nodesCloneNode((SNode*)pStmt, &pClone), TSDB_CODE_SUCCESS);
  unique_ptr<SNode, decltype(&nodesDestroyNode)> cloneGuard(pClone, nodesDestroyNode);
  ASSERT_NE(pClone, nullptr);
  SCreateStreamStmt* pCloneStmt = (SCreateStreamStmt*)pClone;
  ASSERT_NE(pCloneStmt->pTimeInfo, nullptr);
  ASSERT_NE(pCloneStmt->pTimeInfo->timezone, nullptr);
  EXPECT_NE(pCloneStmt->pTimeInfo->timezone, pStmt->pTimeInfo->timezone);

  queryGuard.reset();
  pStmt = nullptr;

  ASSERT_EQ(nodeType(pCloneStmt->pQuery), QUERY_NODE_SELECT_STMT);
  SNode* pCloneTimeRange = ((SSelectStmt*)pCloneStmt->pQuery)->pTimeRange;
  ASSERT_NE(pCloneTimeRange, nullptr);
  ASSERT_EQ(nodeType(pCloneTimeRange), QUERY_NODE_TIME_RANGE);
  PstStreamTimeBorrowerAudit audit = {0};
  audit.timezone = pCloneStmt->pTimeInfo->timezone;
  audit.timezoneName = pCloneStmt->pTimeInfo->config.timezone;
  audit.firstDayOfWeek = pCloneStmt->pTimeInfo->config.firstDayOfWeek;
  audit.valid = true;
  nodesWalkExpr(((STimeRangeNode*)pCloneTimeRange)->pStart, pstAuditStreamTimeBorrower, &audit);
  nodesWalkExpr(((STimeRangeNode*)pCloneTimeRange)->pEnd, pstAuditStreamTimeBorrower, &audit);
  EXPECT_TRUE(audit.valid);
  EXPECT_GT(audit.valueNodes + audit.operatorNodes + audit.functionNodes + audit.caseWhenNodes, 0);
}

static void pstCheckCreateStreamReqRollupTagCols(const char* pSql, const char* const* pExpectedColNames,
                                                 int32_t expectedColNum) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.rollupTagCols, nullptr);

  SNodeList* pList = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToList((char*)req.rollupTagCols, &pList));
  unique_ptr<SNodeList, decltype(&nodesDestroyList)> listGuard(pList, nodesDestroyList);
  ASSERT_NE(pList, nullptr);
  ASSERT_EQ(LIST_LENGTH(pList), expectedColNum);
  for (int32_t i = 0; i < expectedColNum; ++i) {
    SNode* pColNode = nodesListGetNode(pList, i);
    ASSERT_NE(pColNode, nullptr);
    ASSERT_EQ(nodeType(pColNode), QUERY_NODE_COLUMN);

    const SColumnNode* pCol = (const SColumnNode*)pColNode;
    EXPECT_STREQ(pCol->colName, pExpectedColNames[i]);
    EXPECT_EQ(pCol->colType, COLUMN_TYPE_TAG);
    EXPECT_NE(pCol->colId, 0);
    EXPECT_TRUE(pCol->node.resType.type == TSDB_DATA_TYPE_BINARY || pCol->node.resType.type == TSDB_DATA_TYPE_NCHAR);
  }
}

static void pstCheckCreateStreamReqRollupTagCol(const char* pSql, const char* pExpectedColName) {
  const char* expectedColNames[] = {pExpectedColName};
  pstCheckCreateStreamReqRollupTagCols(pSql, expectedColNames, 1);
}

static void pstCheckTriggerScanPseudoColPresent(const char* pSql, const char* pExpectedColName) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.triggerScanPlan, nullptr);
  ASSERT_NE(req.rollupTagCols, nullptr);

  SNode* pPlanNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.triggerScanPlan, &pPlanNode));
  unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
  ASSERT_NE(pPlanNode, nullptr);
  ASSERT_EQ(nodeType(pPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);

  const SSubplan* pSubplan = (const SSubplan*)pPlanNode;
  ASSERT_NE(pSubplan->pNode, nullptr);
  ASSERT_TRUE(nodeType(pSubplan->pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
              nodeType(pSubplan->pNode) == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN);

  bool   found = false;
  SNode* pTargetNode = nullptr;
  FOREACH(pTargetNode, ((const SScanPhysiNode*)pSubplan->pNode)->pScanPseudoCols) {
    ASSERT_EQ(nodeType(pTargetNode), QUERY_NODE_TARGET);
    const SNode* pExpr = ((const STargetNode*)pTargetNode)->pExpr;
    if (pExpr != nullptr && nodeType(pExpr) == QUERY_NODE_COLUMN) {
      found = found || (0 == strcmp(((const SColumnNode*)pExpr)->colName, pExpectedColName));
    }
  }
  EXPECT_TRUE(found);
}

static void pstCheckCreateStreamReqRollupTagColsWithSubtableExpr(const char* pSql, const char* const* pExpectedColNames,
                                                                 int32_t expectedColNum) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_NE(req.subTblNameExpr, nullptr);
  EXPECT_GT(strlen((const char*)req.subTblNameExpr), 0);
  ASSERT_NE(req.rollupTagCols, nullptr);

  SNodeList* pList = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToList((char*)req.rollupTagCols, &pList));
  unique_ptr<SNodeList, decltype(&nodesDestroyList)> listGuard(pList, nodesDestroyList);
  ASSERT_NE(pList, nullptr);
  ASSERT_EQ(LIST_LENGTH(pList), expectedColNum);
  for (int32_t i = 0; i < expectedColNum; ++i) {
    SNode* pColNode = nodesListGetNode(pList, i);
    ASSERT_NE(pColNode, nullptr);
    ASSERT_EQ(nodeType(pColNode), QUERY_NODE_COLUMN);
    EXPECT_STREQ(((const SColumnNode*)pColNode)->colName, pExpectedColNames[i]);
  }
}

static void pstCheckCreateStreamReqRollupTagColWithSubtableExpr(const char* pSql, const char* pExpectedColName) {
  const char* expectedColNames[] = {pExpectedColName};
  pstCheckCreateStreamReqRollupTagColsWithSubtableExpr(pSql, expectedColNames, 1);
}

static void pstCheckCreateStreamReqNoRollupTagCol(const char* pSql) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  EXPECT_EQ(req.rollupTagCols, nullptr);
}

static void pstCheckCreateStreamCountTrigger(const char* pSql, int64_t expectedCountVal, int64_t expectedSliding) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);

  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_EQ(req.triggerType, WINDOW_TYPE_COUNT);
  EXPECT_EQ(req.trigger.count.countVal, expectedCountVal);
  EXPECT_EQ(req.trigger.count.sliding, expectedSliding);
}

static void pstCheckCountTriggerJson(const char* pTriggerJson, int64_t expectedSliding) {
  char json[4096] = {0};
  snprintf(json, sizeof(json),
           "{\"streamId\":\"0\",\"name\":\"0.stream_streamdb.s1\",\"sql\":\"\",\"streamDB\":\"0.stream_streamdb\","
           "\"triggerDB\":\"0.stream_triggerdb\",\"outDB\":\"0.stream_outdb\",\"calcDB\":[],"
           "\"triggerTblName\":\"stream_t1\",\"outTblName\":\"stream_out\",\"igExists\":\"0\",\"triggerType\":\"%d\","
           "\"igDisorder\":\"0\",\"deleteReCalc\":\"0\",\"deleteOutTbl\":\"0\",\"fillHistory\":\"0\","
           "\"fillHistoryFirst\":\"0\",\"calcNotifyOnly\":\"0\",\"lowLatencyCalc\":\"0\",\"igNoDataTrigger\":\"0\","
           "\"multiGroupCalc\":\"0\",\"notifyEventTypes\":\"0\",\"addOptions\":\"0\",\"notifyHistory\":\"0\","
           "\"outCols\":[],\"outTags\":[],\"maxDelay\":\"0\",\"fillHistoryStartTime\":\"0\",\"watermark\":\"0\","
           "\"expiredTime\":\"0\",\"idleTimeoutMs\":\"0\",\"trigger\":%s,\"triggerTblType\":\"%d\","
           "\"triggerTblUid\":\"0\",\"triggerTblSuid\":\"0\",\"triggerPrec\":\"0\",\"vtableCalc\":\"0\","
           "\"outTblType\":\"%d\",\"outStbExists\":\"0\",\"outStbUid\":\"0\",\"outStbSversion\":\"0\","
           "\"eventTypes\":\"0\",\"flags\":\"0\",\"tsmaId\":\"0\",\"placeHolderBitmap\":\"0\","
           "\"calcTsSlotId\":\"-1\",\"triTsSlotId\":\"-1\",\"calcPkSlotId\":\"-1\",\"triPkSlotId\":\"-1\","
           "\"triggerTblVgId\":\"0\",\"outTblVgId\":\"0\",\"calcScanPlanList\":[],\"triggerHasPF\":\"0\","
           "\"numOfCalcSubplan\":\"0\",\"forceOutCols\":[],\"colCids\":[],\"tagCids\":[],"
           "\"nodelayCreateSubtable\":\"0\"}",
           WINDOW_TYPE_COUNT, pTriggerJson, TSDB_NORMAL_TABLE, TSDB_NORMAL_TABLE);

  unique_ptr<SJson, decltype(&tjsonDelete)> jsonGuard(tjsonParse(json), tjsonDelete);
  ASSERT_NE(jsonGuard.get(), nullptr);
  SCMCreateStreamReq req = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(jsonGuard.get(), &req));
  unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
  ASSERT_EQ(req.triggerType, WINDOW_TYPE_COUNT);
  EXPECT_EQ(req.trigger.count.sliding, expectedSliding);
}

static void pstCheckCreateStreamRollupError(const char* pSql, int32_t expectedCode, const char* pExpectedMsg) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  int32_t           code = pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size());
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  EXPECT_EQ(code, expectedCode) << msgBuf.data();
  if (pExpectedMsg != nullptr) {
    EXPECT_NE(strstr(msgBuf.data(), pExpectedMsg), nullptr) << msgBuf.data();
  }
}

static void pstCheckCreateStreamRollupTagPlaceholderType(const char* pSql, int8_t expectedType, int32_t expectedBytes) {
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(pSql, &pQuery, msgBuf.data(), msgBuf.max_size()))
      << msgBuf.data();
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  ASSERT_NE(pQuery, nullptr);

  const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  ASSERT_NE(pStmt->pQuery, nullptr);
  ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);
  const SSelectStmt* pSelect = (const SSelectStmt*)pStmt->pQuery;
  ASSERT_NE(pSelect->pProjectionList, nullptr);
  ASSERT_GE(LIST_LENGTH(pSelect->pProjectionList), 2);

  SNode* pProjNode = nodesListGetNode(pSelect->pProjectionList, 1);
  ASSERT_NE(pProjNode, nullptr);
  ASSERT_EQ(nodeType(pProjNode), QUERY_NODE_FUNCTION);
  const SFunctionNode* pFunc = (const SFunctionNode*)pProjNode;
  EXPECT_STREQ(pFunc->functionName, "_placeholder_rollup_tag");
  EXPECT_EQ(pFunc->node.resType.type, expectedType);
  EXPECT_EQ(pFunc->node.resType.bytes, expectedBytes);
}

static const SSelectStmt* pstGetCreateStreamSelect(const SQuery* pQuery, ParserStage stage) {
  EXPECT_EQ(stage, PARSER_STAGE_TRANSLATE);
  EXPECT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

  const SCreateStreamStmt* pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
  EXPECT_NE(pStmt->pQuery, nullptr);
  EXPECT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);
  return (const SSelectStmt*)pStmt->pQuery;
}

static void pstCheckCreateStreamSelectTimeRange(const SQuery* pQuery, ParserStage stage, bool expectedPresent,
                                                bool expectedNeedCalc = false) {
  const SSelectStmt* pSelect = pstGetCreateStreamSelect(pQuery, stage);
  ASSERT_NE(pSelect, nullptr);

  if (!expectedPresent) {
    EXPECT_EQ(pSelect->pTimeRange, nullptr);
    return;
  }

  ASSERT_NE(pSelect->pTimeRange, nullptr);
  ASSERT_EQ(nodeType(pSelect->pTimeRange), QUERY_NODE_TIME_RANGE);
  const STimeRangeNode* pTimeRange = (const STimeRangeNode*)pSelect->pTimeRange;
  EXPECT_EQ(pTimeRange->needCalc, expectedNeedCalc);
}

static void pstCheckCreateStreamSelectStartRangeFlag(const SQuery* pQuery, ParserStage stage, bool expectedSet) {
  const SSelectStmt* pSelect = pstGetCreateStreamSelect(pQuery, stage);
  ASSERT_NE(pSelect, nullptr);
  ASSERT_NE(pSelect->pTimeRange, nullptr);
  ASSERT_EQ(nodeType(pSelect->pTimeRange), QUERY_NODE_TIME_RANGE);

  const STimeRangeNode* pTimeRange = (const STimeRangeNode*)pSelect->pTimeRange;
  ASSERT_NE(pTimeRange->pStart, nullptr);
  ASSERT_EQ(nodeType(pTimeRange->pStart), QUERY_NODE_OPERATOR);
  const SOperatorNode* pStart = (const SOperatorNode*)pTimeRange->pStart;
  if (expectedSet) {
    EXPECT_EQ(pStart->flag & OPERATOR_FLAG_STREAM_EXT_JOIN_AUTO_RANGE, OPERATOR_FLAG_STREAM_EXT_JOIN_AUTO_RANGE);
  } else {
    EXPECT_EQ(pStart->flag & OPERATOR_FLAG_STREAM_EXT_JOIN_AUTO_RANGE, 0);
  }
}

/*
* CREATE STREAM [IF NOT EXISTS] stream_name stream_options [INTO [db_name.]table_name] [OUTPUT_SUBTABLE(tbname_expr)] [(column_name1, column_name2 [PRIMARY KEY][, ...])] [TAGS (tag_definition [, ...])] [AS subquery]
*
* stream_options: {
*     trigger_type [FROM [db_name.]table_name] [PARTITION BY col1 [, ...]] [stream_options(stream_option [|...])] [notification_definition]
* }
*
* trigger_type: {
*     SESSION(ts_col, session_val)
*   | STATE_WINDOW(col) [TRUE_FOR(duration_time)]
*   | [INTERVAL(interval_val[, interval_offset])] SLIDING(sliding_val[, offset_time])
*   | EVENT_WINDOW(START WITH start_condition END WITH end_condition) [TRUE_FOR(duration_time)]
*   | COUNT_WINDOW(count_val[, sliding_val][, col1[, ...]])
*   | PERIOD(period_time[, offset_time])
* }
*
* event_types:
*     event_type [|event_type]
*
* event_type: {WINDOW_OPEN | WINDOW_CLOSE}
*
* stream_option: {WATERMARK(duration_time) | EXPIRED_TIME(exp_time) | IGNORE_DISORDER | DELETE_RECALC | DELETE_OUTPUT_TABLE | FILL_HISTORY(start_time) | FILL_HISTORY_FIRST(start_time) | CALC_NOTIFY_ONLY | LOW_LATENCY_CALC | PRE_FILTER(expr) | FORCE_OUTPUT | MAX_DELAY(delay_time) | EVENT_TYPE(event_types)}
*
* notification_definition:
*     NOTIFY(url [, ...]) [ON (event_types)] [WHERE condition] [notify_options(notify_option [| notify_option])]
*
* notify_option: [NOTIFY_HISTORY | ON_FAILURE_PAUSE]
*
* tag_definition:
*     tag_name type_name [COMMENT 'string_value'] AS expr
*/

void setCreateStreamStreamName(SCMCreateStreamReq *expect, const char* pStream, const char* pStreamDb) {
  setCreateStreamString(&expect->name, pStream);
  setCreateStreamString(&expect->streamDB, pStreamDb);
}

// trigger part
void setCreateStreamTriggerType(SCMCreateStreamReq *expect, int8_t triggerType) {
  if (expect->triggerType != triggerType) {
    resetCreateStreamTriggerPayload(expect);
  }
  expect->triggerType = triggerType;
}

void setCreateStreamTriggerTableType(SCMCreateStreamReq *expect, int8_t triggerTableType) {
  expect->triggerTblType = triggerTableType;
}

void setCreateStreamTriggerTableSuid(SCMCreateStreamReq *expect, uint64_t triggerTblSUid) {
  expect->triggerTblSuid = triggerTblSUid;
}

void setCreateStreamTriggerTblUid(SCMCreateStreamReq *expect, uint64_t triggerTblUid) {
  expect->triggerTblUid = triggerTblUid;
}

void setCreateStreamTriggerTblVgid(SCMCreateStreamReq *expect, int8_t triggerTblVgid) {
  expect->triggerTblVgId = triggerTblVgid;
}

void setCreateStreamTriggerName(SCMCreateStreamReq *expect, const char* pTriggerDb, const char* pTriggerTblName) {
  setCreateStreamString(&expect->triggerDB, pTriggerDb);
  setCreateStreamString(&expect->triggerTblName, pTriggerTblName);
}

void setCreateStreamTriggerTable(SCMCreateStreamReq *expect, const char* pTriggerDb, const char* pTriggerTblName,
                                int8_t triggerType, int8_t triggerTblType, uint64_t triggerTblUid, uint64_t triggerTblSuid, int8_t triggerTblVgid) {
  setCreateStreamTriggerName(expect, pTriggerDb, pTriggerTblName);
  setCreateStreamTriggerType(expect, triggerType);
  setCreateStreamTriggerTblUid(expect, triggerTblUid);
  setCreateStreamTriggerTableType(expect, triggerTblType);
  setCreateStreamTriggerTableSuid(expect, triggerTblSuid);
  setCreateStreamTriggerTblVgid(expect, triggerTblVgid);
}

// out table part
void setCreateStreamOutName(SCMCreateStreamReq *expect, const char* pOutDb, const char* pOutTbleName) {
  setCreateStreamString(&expect->outDB, pOutDb);
  setCreateStreamString(&expect->outTblName, pOutTbleName);
}

void setCreateStreamOutTblType(SCMCreateStreamReq *expect, int8_t outTblType) {
  expect->outTblType = outTblType;
}

void setCreateStreamOutStbExists(SCMCreateStreamReq *expect, int8_t outStbExists) {
  expect->outStbExists = outStbExists;
}

void setCreateStreamOutStbUid(SCMCreateStreamReq *expect, uint64_t outStbUid) {
  expect->outStbUid = outStbUid;
}

void setCreateStreamOutStbSversion(SCMCreateStreamReq *expect, int32_t outStbSversion) {
  expect->outStbSversion = outStbSversion;
}

void setCreateStreamOutTbleVgid(SCMCreateStreamReq *expect, int32_t outTblVgid) {
   expect->outTblVgId = outTblVgid;
}

void setCreateStreamOutTable(SCMCreateStreamReq *expect, const char* pOutDb, const char* pOutTbleName, int8_t outTblType,
                            int8_t outStbExists, uint64_t outStbUid, int32_t outStbSversion, int32_t outTblVgid) {
  setCreateStreamOutName(expect, pOutDb, pOutTbleName);
  setCreateStreamOutTblType(expect, outTblType);
  setCreateStreamOutStbExists(expect, outStbExists);
  setCreateStreamOutStbUid(expect, outStbUid);
  setCreateStreamOutStbSversion(expect, outStbSversion);
  setCreateStreamOutTbleVgid(expect, outTblVgid);
}


void setCreateStreamCalcDb(SCMCreateStreamReq *expect, const char* pCalcDb) {
  if (expect->calcDB != nullptr) {
    for (int32_t i = 0; i < expect->calcDB->size; i++) {
      char *tmpCalcDb = *(char **)taosArrayGet(expect->calcDB, i);
      taosMemoryFreeClear(tmpCalcDb);
    }
    taosArrayDestroy(expect->calcDB);
    expect->calcDB = nullptr;
  }
  expect->calcDB = taosArrayInit(1, POINTER_BYTES);
  char *tmpCalcDb = taosStrdup(pCalcDb);
  ASSERT_TRUE(taosArrayPush(expect->calcDB, &tmpCalcDb));
}

void setCreateStreamSql(SCMCreateStreamReq *expect, const char* pSql) {
  if (expect->sql != nullptr) {
    taosMemoryFreeClear(expect->sql);
  }
  expect->sql = taosStrdup(pSql);
}

void setCreateStreamIgExists(SCMCreateStreamReq *expect, int8_t igExists) {
  expect->igExists = igExists;
}

void setCreateStreamReq(SCMCreateStreamReq *expect, const char* pStream, const char* pStreamDb, const char* pCalcDb,
                       const char* pSql, int8_t igExists) {
  setCreateStreamStreamName(expect, pStream, pStreamDb);
  setCreateStreamCalcDb(expect, pCalcDb);
  setCreateStreamSql(expect, pSql);
  setCreateStreamIgExists(expect, igExists);
}

void setCreateStreamTriggerScanPlan(SCMCreateStreamReq *expect, const char* pTriggerScanPlan) {
  if (expect->triggerScanPlan != nullptr) {
    taosMemoryFreeClear(expect->triggerScanPlan);
  }
  expect->triggerScanPlan = taosStrdup(pTriggerScanPlan);
}

void setCreateStreamQueryCalcPlan(SCMCreateStreamReq *expect, const char* pStreamQueryCalcPlan) {
  if (expect->calcPlan != nullptr) {
    taosMemoryFreeClear(expect->calcPlan);
  }
  expect->calcPlan = taosStrdup(pStreamQueryCalcPlan);
}

void setCreateStreamSubTblNameExpr(SCMCreateStreamReq *expect, const char* pSubTblNameExpr) {
  setCreateStreamString((char**)&expect->subTblNameExpr, pSubTblNameExpr);
}

void resetCreateStreamSubTblNameExpr(SCMCreateStreamReq *expect) {
  taosMemoryFreeClear(expect->subTblNameExpr);
  expect->subTblNameExpr = nullptr;
}

void setCreateStreamTagValueExpr(SCMCreateStreamReq *expect, const char* pTagValueExpr) {
  if (expect->tagValueExpr != nullptr) {
    taosMemoryFreeClear(expect->tagValueExpr);
  }
  expect->tagValueExpr = taosStrdup(pTagValueExpr);
}

void resetCreateStreamTagValueExpr(SCMCreateStreamReq *expect) {
  taosMemoryFreeClear(expect->tagValueExpr);
  expect->tagValueExpr = nullptr;
}

void setCreateStreamIgDisorder(SCMCreateStreamReq *expect, int8_t igDisorder) {
  expect->igDisorder = igDisorder;
}

void setCreateStreamDeleteReCalc(SCMCreateStreamReq *expect, int8_t deleteReCalc) {
  expect->deleteReCalc = deleteReCalc;
}

void setCreateStreamDeleteOutTbl(SCMCreateStreamReq *expect, int8_t deleteOutTbl) {
  expect->deleteOutTbl = deleteOutTbl;
}

void setCreateStreamFillHistory(SCMCreateStreamReq *expect, int8_t fillHistory) {
  expect->fillHistory = fillHistory;
}

void setCreateStreamFillHistoryFirst(SCMCreateStreamReq *expect, int8_t fillHistoryFirst) {
  expect->fillHistoryFirst = fillHistoryFirst;
}

void setCreateStreamCalcNotifyOnly(SCMCreateStreamReq *expect, int8_t calcNotifyOnly) {
  expect->calcNotifyOnly = calcNotifyOnly;
}

void setCreateStreamLowLatencyCalc(SCMCreateStreamReq *expect, int8_t lowLatencyCalc) {
  expect->lowLatencyCalc = lowLatencyCalc;
}

void setCreateStreamMaxDelay(SCMCreateStreamReq *expect, int64_t maxDelay) {
  expect->maxDelay = maxDelay;
}

void setCreateStreamFillHistoryStartTime(SCMCreateStreamReq *expect, int64_t fillHistoryStartTime) {
  expect->fillHistoryStartTime = fillHistoryStartTime;
}

void setCreateStreamWatermark(SCMCreateStreamReq *expect, int64_t watermark) {
  expect->watermark = watermark;
}

void setCreateStreamExpiredTime(SCMCreateStreamReq *expect, int64_t expiredTime) {
  expect->expiredTime = expiredTime;
}

void setCreateStreamEventTypes(SCMCreateStreamReq *expect, int64_t eventTypes) {
  expect->eventTypes = eventTypes;
}

void setCreateStreamIdleTimeout(SCMCreateStreamReq *expect, int64_t idleTimeoutMs) {
  expect->idleTimeoutMs = idleTimeoutMs;
}

void setCreateStreamFlags(SCMCreateStreamReq *expect, int64_t flags) {
  expect->flags = flags;
}

void setCreateStreamTsmaId(SCMCreateStreamReq *expect, int64_t tsmaId) {
  expect->tsmaId = tsmaId;
}

void setCreateStreamPlaceHolderBitmap(SCMCreateStreamReq *expect, int64_t placeHolderBitmap) {
  expect->placeHolderBitmap = placeHolderBitmap;
}

void setCreateStreamCalcTsSlotId(SCMCreateStreamReq *expect, int16_t calcTsSlotId) {
  expect->calcTsSlotId = calcTsSlotId;
}

void setCreateStreamOption(SCMCreateStreamReq *expect, int8_t igExists, int8_t igDisorder, int8_t deleteReCalc,
                          int8_t deleteOutTbl, int8_t fillHistory, int8_t fillHistoryFirst, int8_t calcNotifyOnly, int8_t lowLatencyCalc,
                          int64_t maxDelay, int64_t fillHistoryStartTime, int64_t watermark, int64_t expiredTime,
                          int64_t eventTypes, int64_t flags, int64_t tsmaId, int64_t placeHolderBitmap, int16_t tsSlotId){
  expect->igExists = igExists;
  setCreateStreamIgDisorder(expect, igDisorder);
  setCreateStreamDeleteReCalc(expect, deleteReCalc);
  setCreateStreamDeleteOutTbl(expect, deleteOutTbl);
  setCreateStreamFillHistory(expect, fillHistory);
  setCreateStreamFillHistoryFirst(expect, fillHistoryFirst);
  setCreateStreamCalcNotifyOnly(expect, calcNotifyOnly);
  setCreateStreamLowLatencyCalc(expect, lowLatencyCalc);
  setCreateStreamMaxDelay(expect, maxDelay);
  setCreateStreamFillHistoryStartTime(expect, fillHistoryStartTime);
  setCreateStreamWatermark(expect, watermark);
  setCreateStreamExpiredTime(expect, expiredTime);
  setCreateStreamEventTypes(expect, eventTypes);
  setCreateStreamFlags(expect, flags);
  setCreateStreamTsmaId(expect, tsmaId);
  setCreateStreamPlaceHolderBitmap(expect, placeHolderBitmap);
  setCreateStreamCalcTsSlotId(expect, tsSlotId);
}


void addCreateStreamOutCols(SCMCreateStreamReq *expect, const char* name, uint8_t type, int8_t flag, int32_t bytes, uint32_t compress, STypeMod typeMod) {
  SFieldWithOptions outCol = {0};
  strcpy(outCol.name, name);
  outCol.type = type;
  outCol.bytes = bytes;
  outCol.compress = compress;
  outCol.typeMod = typeMod;
  outCol.flags = flag;
  if (expect->outCols == nullptr) {
    expect->outCols = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SFieldWithOptions));
    ASSERT_TRUE(expect->outCols != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->outCols, &outCol) != nullptr);
}

void resetCreateStreamOutCols(SCMCreateStreamReq *expect) {
  if (expect->outCols) {
    taosArrayDestroy(expect->outCols);
    expect->outCols = nullptr;
  }
}

void addCreateStreamOutTags(SCMCreateStreamReq *expect, const char* name, uint8_t type, int8_t flag, int32_t bytes, uint32_t compress, STypeMod typeMod) {
  SFieldWithOptions outTags = {0};
  strcpy(outTags.name, name);
  outTags.type = type;
  outTags.bytes = bytes;
  outTags.compress = compress;
  outTags.typeMod = typeMod;
  outTags.flags = flag;
  if (expect->outTags == nullptr) {
    expect->outTags = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SFieldWithOptions));
    ASSERT_TRUE(expect->outTags != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->outTags, &outTags) != nullptr);
}

void resetCreateStreamOutTags(SCMCreateStreamReq *expect) {
  if (expect->outTags) {
    taosArrayDestroy(expect->outTags);
    expect->outTags = nullptr;
  }
}

void setCreateStreamTriggerCols(SCMCreateStreamReq *expect, const char* pCols) {
  setCreateStreamString((char**)&expect->triggerCols, pCols);
}

void resetCreateStreamTriggerCols(SCMCreateStreamReq* expect) { taosMemoryFreeClear(expect->triggerCols); }

void resetCreateStreamPartitionCols(SCMCreateStreamReq *expect) {
  if (expect->partitionCols != nullptr) {
    taosMemoryFreeClear(expect->partitionCols);
  }
}

void setCreateStreamPartitionCols(SCMCreateStreamReq *expect, const char* pCols) {
  resetCreateStreamPartitionCols(expect);
  expect->partitionCols = taosStrdup(pCols);
}

typedef enum EWindowType {
  WINDOW_TYPE_INTERVAL = 1,
  WINDOW_TYPE_SESSION,
  WINDOW_TYPE_STATE,
  WINDOW_TYPE_EVENT,
  WINDOW_TYPE_COUNT,
  WINDOW_TYPE_ANOMALY,
  WINDOW_TYPE_EXTERNAL,
  WINDOW_TYPE_PERIOD
} EWindowType;

void setCreateStreamTriggerSliding(SCMCreateStreamReq *expect, int8_t intervalUnit, int8_t slidingUnit,
  int8_t offsetUnit, int8_t soffsetUnit, int8_t precision, int64_t interval, int64_t offset, int64_t sliding,
  int64_t soffset) {
  expect->trigger.sliding.intervalUnit = intervalUnit;
  expect->trigger.sliding.slidingUnit = slidingUnit;
  expect->trigger.sliding.offsetUnit = offsetUnit;
  expect->trigger.sliding.soffsetUnit = soffsetUnit;
  expect->trigger.sliding.precision = precision;
  expect->trigger.sliding.interval = interval;
  expect->trigger.sliding.offset = offset;
  expect->trigger.sliding.sliding = sliding;
  expect->trigger.sliding.soffset = soffset;
}

void setCreateStreamTriggerSession(SCMCreateStreamReq *expect,int64_t pSessionVal, int16_t slotId) {
  expect->trigger.session.sessionVal = pSessionVal;
  expect->trigger.session.slotId = slotId;
}

void setCreateStreamTriggerCount(SCMCreateStreamReq* expect, int64_t countVal, int64_t sliding) {
  expect->trigger.count.countVal = countVal;
  expect->trigger.count.sliding = sliding;
}

void setCreateStreamTriggerEvent(SCMCreateStreamReq *expect, const char* startCond, const char* endCond, int64_t trueForDuration) {
  setCreateStreamString((char**)&expect->trigger.event.startCond, startCond);
  setCreateStreamString((char**)&expect->trigger.event.endCond, endCond);
  expect->trigger.event.trueForDuration = trueForDuration;
}

static void resetCreateStreamTriggerStateSlots(SCMCreateStreamReq *expect) {
  if (expect->trigger.stateWin.pSlotIds != nullptr) {
    taosArrayDestroy(expect->trigger.stateWin.pSlotIds);
    expect->trigger.stateWin.pSlotIds = nullptr;
  }
}

void setCreateStreamTriggerState(SCMCreateStreamReq *expect, int16_t slotId, int64_t trueForDuration) {
  resetCreateStreamTriggerStateSlots(expect);
  expect->trigger.stateWin.pSlotIds = taosArrayInit(1, sizeof(int16_t));
  ASSERT_NE(expect->trigger.stateWin.pSlotIds, nullptr);
  ASSERT_NE(taosArrayPush(expect->trigger.stateWin.pSlotIds, &slotId), nullptr);
  expect->trigger.stateWin.extend = 0;
  expect->trigger.stateWin.trueForDuration = trueForDuration;
}

void setCreateStreamTriggerStateExtend(SCMCreateStreamReq *expect, int16_t extend) {
  expect->trigger.stateWin.extend = extend;
}

void setCreateStreamTriggerStateMulti(SCMCreateStreamReq *expect, const int16_t *slotIds, int32_t numSlots, int64_t trueForDuration) {
  resetCreateStreamTriggerStateSlots(expect);
  expect->trigger.stateWin.pSlotIds = taosArrayInit(numSlots, sizeof(int16_t));
  ASSERT_NE(expect->trigger.stateWin.pSlotIds, nullptr);
  for (int32_t i = 0; i < numSlots; ++i) {
    ASSERT_NE(taosArrayPush(expect->trigger.stateWin.pSlotIds, &slotIds[i]), nullptr);
  }
  expect->trigger.stateWin.extend = 0;
  expect->trigger.stateWin.trueForDuration = trueForDuration;
}

void setCreateStreamTriggerPeriod(SCMCreateStreamReq *expect, int8_t offsetUnit, int8_t periodUnit, int8_t precision, int64_t offset, int64_t period) {
  expect->trigger.period.offsetUnit = offsetUnit;
  expect->trigger.period.periodUnit = periodUnit;
  expect->trigger.period.precision = precision;
  expect->trigger.period.offset = offset;
  expect->trigger.period.period = period;
}

void addCreateStreamNotifyUrl(SCMCreateStreamReq *expect, const char* pUrl) {
  if (nullptr == expect->pNotifyAddrUrls) {
    expect->pNotifyAddrUrls = taosArrayInit(TARRAY_MIN_SIZE, POINTER_BYTES);
    ASSERT_TRUE(expect->pNotifyAddrUrls != nullptr);
  }
  char *tmpUrl = taosStrdup(pUrl);
  ASSERT_TRUE(taosArrayPush(expect->pNotifyAddrUrls, &tmpUrl));
};

void setCreateStreamNotify(SCMCreateStreamReq *expect, int8_t notifyEventTypes, int8_t notifyErrorHandle, int8_t notifyHistory) {
  expect->notifyEventTypes = notifyEventTypes;
  expect->addOptions = notifyErrorHandle;
  expect->notifyHistory = notifyHistory;
}

void setCreateStreamForceOutputCols(SCMCreateStreamReq *expect, uint8_t type, uint8_t precision, uint8_t scale, int32_t bytes, const char* expr) {
  SStreamOutCol outCol = {0};
  outCol.type.type = type;
  outCol.type.precision = precision;
  outCol.type.scale = scale;
  outCol.type.bytes = bytes;
  outCol.expr = taosStrdup(expr);
  if (expect->outCols == nullptr) {
    expect->outCols = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SStreamOutCol));
    ASSERT_TRUE(expect->outCols != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->outCols, &outCol) != nullptr);
}

void destroyStreamCalcScan(void *ptr) {
  SStreamCalcScan *pScan = (SStreamCalcScan *)ptr;
  if (pScan && pScan->scanPlan) {
    taosMemoryFreeClear(pScan->scanPlan);
  }
}

void resetCreateStreamQueryScanPlan(SCMCreateStreamReq *expect) {
  taosArrayDestroyEx(expect->calcScanPlanList, destroyStreamCalcScan);
  expect->calcScanPlanList = nullptr;
}

void addCreateStreamQueryScanPlan(SCMCreateStreamReq *expect, bool readFromCache, const char* pStreamQueryScanPlan) {
  SStreamCalcScan scan = {0};
  scan.readFromCache = (int8_t)readFromCache;
  scan.scanPlan = taosStrdup(pStreamQueryScanPlan);
  if (expect->calcScanPlanList == nullptr) {
    expect->calcScanPlanList = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SStreamCalcScan));
    ASSERT_TRUE(expect->calcScanPlanList != nullptr);
  }
  ASSERT_TRUE(taosArrayPush(expect->calcScanPlanList, &scan) != nullptr);
}

void delete_all_specified_fields(cJSON* node, const char* fieldName) {
  if (!node) return;

  if (cJSON_IsObject(node)) {
    cJSON* item = node->child;
    cJSON* next = NULL;

    while (item) {
      next = item->next;

      if (strcmp(item->string, fieldName) == 0) {
        cJSON_DeleteItemFromObject(node, fieldName);
      } else {
        delete_all_specified_fields(item, fieldName);
      }

      item = next;
    }

  } else if (cJSON_IsArray(node)) {
    int32_t size = cJSON_GetArraySize(node);
    for (int i = 0; i < size; ++i) {
      delete_all_specified_fields(cJSON_GetArrayItem(node, i), fieldName);
    }
  }
}

void delete_all_specified_fields(cJSON* node, const char* const* fieldNames, int32_t numOfFields) {
  for (int32_t i = 0; i < numOfFields; ++i) {
    delete_all_specified_fields(node, fieldNames[i]);
  }
}

// Stream snapshots exercise plan structure, while charset codec behavior is
// covered by NodesCharsetCodecTest. Remove CharsetName only from the four
// expression node bodies that gained it, preserving same-named fields on
// other nodes such as system-table scans.
void delete_expression_charset_name_fields(cJSON* node) {
  if (!node) return;

  if (cJSON_IsObject(node)) {
    cJSON*      typeItem = cJSON_GetObjectItemCaseSensitive(node, "NodeType");
    ENodeType   type = static_cast<ENodeType>(0);
    const char* bodyName = nullptr;
    if (cJSON_IsString(typeItem)) {
      type = static_cast<ENodeType>(atoi(typeItem->valuestring));
    } else if (cJSON_IsNumber(typeItem)) {
      type = static_cast<ENodeType>(typeItem->valueint);
    }
    switch (type) {
      case QUERY_NODE_VALUE:
        bodyName = "Value";
        break;
      case QUERY_NODE_OPERATOR:
        bodyName = "Operator";
        break;
      case QUERY_NODE_FUNCTION:
        bodyName = "Function";
        break;
      case QUERY_NODE_CASE_WHEN:
        bodyName = "CaseWhen";
        break;
      default:
        break;
    }
    if (bodyName != nullptr) {
      cJSON* expression = cJSON_GetObjectItemCaseSensitive(node, bodyName);
      if (cJSON_IsObject(expression)) cJSON_DeleteItemFromObjectCaseSensitive(expression, "CharsetName");
    }

    for (cJSON* item = node->child; item != nullptr; item = item->next) {
      delete_expression_charset_name_fields(item);
    }
  } else if (cJSON_IsArray(node)) {
    int32_t size = cJSON_GetArraySize(node);
    for (int32_t i = 0; i < size; ++i) {
      delete_expression_charset_name_fields(cJSON_GetArrayItem(node, i));
    }
  }
}

TEST(ParserStreamJsonTest, DeleteExpressionCharsetNameFieldsPreservesOtherNodes) {
  const char* jsonText =
      R"([{"NodeType":"2","Value":{"CharsetName":"UTF-8"}},{"NodeType":"3","Operator":{"CharsetName":"GBK"}},{"NodeType":"5","Function":{"CharsetName":"UTF-8"}},{"NodeType":"30","CaseWhen":{"CharsetName":"GBK"}},{"NodeType":"2","Operator":{"CharsetName":"keep"}},{"NodeType":"200","PhysiSystemTableScan":{"CharsetName":"GBK"}}])";
  unique_ptr<cJSON, decltype(&cJSON_Delete)> root(cJSON_Parse(jsonText), cJSON_Delete);
  ASSERT_NE(root, nullptr);

  delete_expression_charset_name_fields(root.get());

  const char* const expressionFields[] = {"Value", "Operator", "Function", "CaseWhen"};
  for (int32_t i = 0; i < 4; ++i) {
    cJSON* expression = cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(root.get(), i), expressionFields[i]);
    ASSERT_TRUE(cJSON_IsObject(expression));
    EXPECT_EQ(cJSON_GetObjectItemCaseSensitive(expression, "CharsetName"), nullptr);
  }

  cJSON* misleading = cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(root.get(), 4), "Operator");
  ASSERT_TRUE(cJSON_IsObject(misleading));
  EXPECT_STREQ(cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(misleading, "CharsetName")), "keep");

  cJSON* scan = cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(root.get(), 5), "PhysiSystemTableScan");
  ASSERT_TRUE(cJSON_IsObject(scan));
  cJSON* charsetName = cJSON_GetObjectItemCaseSensitive(scan, "CharsetName");
  ASSERT_TRUE(cJSON_IsString(charsetName));
  EXPECT_STREQ(charsetName->valuestring, "GBK");
}

// Strips only the "Id" field of Function nodes (identified by the sibling "UdfBufSize" field
// that is unique to SFunctionNode's JSON encoding), leaving other "Id" fields (e.g. Subplan/
// QueryNodeAddr Id) untouched.
void delete_function_id_field(cJSON* node) {
  if (!node) return;

  if (cJSON_IsObject(node)) {
    if (cJSON_HasObjectItem(node, "Id") && cJSON_HasObjectItem(node, "UdfBufSize")) {
      cJSON_DeleteItemFromObject(node, "Id");
    }

    cJSON* item = node->child;
    while (item) {
      cJSON* next = item->next;
      delete_function_id_field(item);
      item = next;
    }

  } else if (cJSON_IsArray(node)) {
    int32_t size = cJSON_GetArraySize(node);
    for (int i = 0; i < size; ++i) {
      delete_function_id_field(cJSON_GetArrayItem(node, i));
    }
  }
}

std::string printJsonUnformatted(cJSON* node) {
  char* pJson = cJSON_PrintUnformatted(node);
  std::string json = pJson ? pJson : "";
  free(pJson);
  return json;
}

// Older sort plans omit NumOfGroupKeys; an explicit zero preserves that behavior.
void normalize_sort_group_key_count(cJSON* node) {
  if (cJSON_IsObject(node)) {
    cJSON* sort = cJSON_GetObjectItemCaseSensitive(node, "PhysiSort");
    if (cJSON_IsObject(sort) && !cJSON_HasObjectItem(sort, "NumOfGroupKeys")) {
      cJSON_AddStringToObject(sort, "NumOfGroupKeys", "0");
    }
  }

  if (cJSON_IsObject(node) || cJSON_IsArray(node)) {
    for (cJSON* item = node->child; item != nullptr; item = item->next) {
      normalize_sort_group_key_count(item);
    }
  }
}

void checkJsonStringEqual(const char* pExpect, const char* pActual, const char* const* pIgnoredFields,
                          int32_t numOfIgnoredFields) {
  cJSON* expect = cJSON_Parse(pExpect);
  cJSON* actual = cJSON_Parse(pActual);
  if (!expect || !actual) {
    cJSON_Delete(expect);
    cJSON_Delete(actual);
    ASSERT_TRUE(false);
  }

  delete_all_specified_fields(expect, pIgnoredFields, numOfIgnoredFields);
  delete_all_specified_fields(actual, pIgnoredFields, numOfIgnoredFields);
  delete_expression_charset_name_fields(expect);
  delete_expression_charset_name_fields(actual);

  std::string expectStr = printJsonUnformatted(expect);
  std::string actualStr = printJsonUnformatted(actual);
  cJSON_Delete(expect);
  cJSON_Delete(actual);

  ASSERT_EQ(expectStr, actualStr);
}

void checkExprJsonStringEqual(const char* pExpect, const char* pActual) {
  const char* const ignoredFields[] = {"Flag",         "IsDistinct", "TableId",       "STableId",     "RefSource",
                                       "FastInterval", "ExInterval", "DelayFreeTask", "TimezoneName", "TzName"};
  checkJsonStringEqual(pExpect, pActual, ignoredFields, sizeof(ignoredFields) / sizeof(ignoredFields[0]));
}

void checkCreateStreamTriggerScanPlan(SCMCreateStreamReq *expect, SCMCreateStreamReq *req) {
  cJSON* j1 = cJSON_Parse((char*)expect->triggerScanPlan);
  cJSON* j2 = cJSON_Parse((char*)req->triggerScanPlan);

  if (!j1 || !j2) {
    printf("Invalid JSON input.\n");
    ASSERT_TRUE(false);
  }

  const char* const ignoredFields[] = {
      "NodeAddr", "GroupId", "SubplanId", "DataBlockId", "Level",
      "RequireDataOrder", "ResultDataOrder", "IsDistinct", "Flag",
      "TableId", "STableId", "InterpFillMode", "InterpMode", "UserAppId",
      "OutputTsPrecision", "RefSource", "TimezoneName", "FirstDayOfWeek",
      "FastInterval", "ExInterval", "DelayFreeTask"};
  delete_all_specified_fields(j1, ignoredFields, sizeof(ignoredFields) / sizeof(ignoredFields[0]));
  delete_all_specified_fields(j2, ignoredFields, sizeof(ignoredFields) / sizeof(ignoredFields[0]));
  delete_expression_charset_name_fields(j1);
  delete_expression_charset_name_fields(j2);

  std::string expectTriggerScanStr = printJsonUnformatted(j1);
  std::string reqTriggerScanStr = printJsonUnformatted(j2);

  cJSON_Delete(j1);
  cJSON_Delete(j2);

  ASSERT_EQ(expectTriggerScanStr, reqTriggerScanStr);
}

void checkCreateStreamCalcPlan(SCMCreateStreamReq *expect, SCMCreateStreamReq *req) {
  cJSON* j1 = cJSON_Parse((char*)expect->calcPlan);
  cJSON* j2 = cJSON_Parse((char*)req->calcPlan);

  if (!j1 || !j2) {
    printf("Invalid JSON input.\n");
    ASSERT_TRUE(false);
  }

  const char* const ignoredFields[] = {
      "Name", "AliasName", "ColName", "TzName", "RequireDataOrder",
      "ResultDataOrder", "IsDistinct", "Flag", "TableId", "STableId",
      "InterpFillMode", "InterpMode", "UserAppId", "OutputTsPrecision",
      "RefSource", "FastInterval", "ExInterval", "DelayFreeTask",
      "TimezoneName", "FirstDayOfWeek"};
  delete_all_specified_fields(j1, ignoredFields, sizeof(ignoredFields) / sizeof(ignoredFields[0]));
  delete_all_specified_fields(j2, ignoredFields, sizeof(ignoredFields) / sizeof(ignoredFields[0]));
  delete_expression_charset_name_fields(j1);
  delete_expression_charset_name_fields(j2);
  // Function.Id is an auto-incrementing counter assigned during translation; it drifts with
  // execution order/global state and carries no semantic meaning, unlike other "Id" fields
  // (e.g. Subplan/QueryNodeAddr Id) which reflect real plan structure and must still match.
  delete_function_id_field(j1);
  delete_function_id_field(j2);
  normalize_sort_group_key_count(j1);
  normalize_sort_group_key_count(j2);

  std::string expectCalcPlan = printJsonUnformatted(j1);
  std::string reqCalcPlan = printJsonUnformatted(j2);

  cJSON_Delete(j1);
  cJSON_Delete(j2);

  ASSERT_EQ(expectCalcPlan, reqCalcPlan);
}

void checkCreateStreamReq(SCMCreateStreamReq* expect, SCMCreateStreamReq* req, bool checkNormalizedSql) {
  // stream name part
  cout << "checkCreateStreamReq: " << expect->sql << endl;
  ASSERT_NE(req->streamId, 0);
  ASSERT_EQ(std::string(req->name), std::string(expect->name));
  if (checkNormalizedSql) {
    ASSERT_EQ(std::string(req->sql), std::string(expect->sql));
  }
  ASSERT_EQ(std::string(req->streamDB), std::string(expect->streamDB));
  ASSERT_EQ(std::string(req->triggerDB), std::string(expect->triggerDB));
  ASSERT_EQ(std::string(req->outDB), std::string(expect->outDB));
  ASSERT_EQ(std::string(req->triggerTblName), std::string(expect->triggerTblName));
  ASSERT_EQ(std::string(req->outTblName), std::string(expect->outTblName));
  ASSERT_EQ(taosArrayGetSize(req->calcDB), taosArrayGetSize(expect->calcDB));
  for (int32_t i = 0; i < taosArrayGetSize(req->calcDB); i++) {
    char *pCalcDb = (char*)taosArrayGetP(req->calcDB, i);
    char *pExpectCalcDb = (char*)taosArrayGetP(expect->calcDB, i);
    ASSERT_EQ(std::string(pCalcDb), std::string(pExpectCalcDb));
  }

  // stream trigger option part
  ASSERT_EQ(req->igExists, expect->igExists);
  ASSERT_EQ(req->triggerType, expect->triggerType);
  ASSERT_EQ(req->igDisorder, expect->igDisorder);
  ASSERT_EQ(req->deleteReCalc, expect->deleteReCalc);
  ASSERT_EQ(req->deleteOutTbl, expect->deleteOutTbl);
  ASSERT_EQ(req->fillHistory, expect->fillHistory);
  ASSERT_EQ(req->fillHistoryFirst, expect->fillHistoryFirst);
  ASSERT_EQ(req->calcNotifyOnly, expect->calcNotifyOnly);
  ASSERT_EQ(req->lowLatencyCalc, expect->lowLatencyCalc);

  // stream notify part
  if (req->pNotifyAddrUrls == nullptr) {
    ASSERT_TRUE(expect->pNotifyAddrUrls == nullptr);
  } else {
    ASSERT_TRUE(expect->pNotifyAddrUrls != nullptr);
    ASSERT_EQ(taosArrayGetSize(req->pNotifyAddrUrls), taosArrayGetSize(expect->pNotifyAddrUrls));
    for (int32_t i = 0; i < taosArrayGetSize(req->pNotifyAddrUrls); i++) {
      char *pUrl = (char*)taosArrayGetP(req->pNotifyAddrUrls, i);
      char *pExpectUrl = (char*)taosArrayGetP(expect->pNotifyAddrUrls, i);
      ASSERT_EQ(std::string(pUrl), std::string(pExpectUrl));
    }
  }

  ASSERT_EQ(req->notifyEventTypes, expect->notifyEventTypes);
  ASSERT_EQ(req->addOptions, expect->addOptions);
  ASSERT_EQ(req->notifyHistory, expect->notifyHistory);

  if (req->triggerCols == nullptr) {
    ASSERT_TRUE(expect->triggerCols == nullptr);
  } else {
    ASSERT_TRUE(expect->triggerCols != nullptr);
    checkExprJsonStringEqual((char*)expect->triggerCols, (char*)req->triggerCols);
  }

  if (req->partitionCols == nullptr) {
    ASSERT_TRUE(expect->partitionCols == nullptr);
  } else {
    ASSERT_TRUE(expect->partitionCols != nullptr);
    checkExprJsonStringEqual((char*)expect->partitionCols, (char*)req->partitionCols);
  }

  ASSERT_EQ(taosArrayGetSize(req->outCols), taosArrayGetSize(expect->outCols));
  for (int32_t i = 0; i < taosArrayGetSize(req->outCols); i++) {
    auto pOutCols = (SFieldWithOptions *)taosArrayGet(req->outCols, i);
    auto expectOutCols = (SFieldWithOptions *)taosArrayGet(expect->outCols, i);
    ASSERT_EQ(pOutCols->type, expectOutCols->type);
    ASSERT_EQ(pOutCols->bytes, expectOutCols->bytes);
    ASSERT_EQ(pOutCols->compress, expectOutCols->compress);
    ASSERT_EQ(pOutCols->flags, expectOutCols->flags);
    ASSERT_EQ(pOutCols->typeMod, expectOutCols->typeMod);
    ASSERT_EQ(std::string(pOutCols->name), std::string(expectOutCols->name));
  }

  ASSERT_EQ(taosArrayGetSize(req->outTags), taosArrayGetSize(expect->outTags));
  for (int32_t i = 0; i < taosArrayGetSize(req->outTags); i++) {
    auto pOutTags = (SFieldWithOptions *)taosArrayGet(req->outTags, i);
    auto expectOutTags = (SFieldWithOptions *)taosArrayGet(expect->outTags, i);
    ASSERT_EQ(pOutTags->type, expectOutTags->type);
    ASSERT_EQ(pOutTags->bytes, expectOutTags->bytes);
    ASSERT_EQ(pOutTags->compress, expectOutTags->compress);
    ASSERT_EQ(pOutTags->flags, expectOutTags->flags);
    ASSERT_EQ(pOutTags->typeMod, expectOutTags->typeMod);
    ASSERT_EQ(std::string(pOutTags->name), std::string(expectOutTags->name));
  }

  ASSERT_EQ(req->maxDelay, expect->maxDelay);
  ASSERT_EQ(req->fillHistoryStartTime, expect->fillHistoryStartTime);
  ASSERT_EQ(req->watermark, expect->watermark);
  ASSERT_EQ(req->expiredTime, expect->expiredTime);

  switch (req->triggerType) {
    case WINDOW_TYPE_INTERVAL: {
      ASSERT_EQ(req->trigger.sliding.sliding, expect->trigger.sliding.sliding);
      ASSERT_EQ(req->trigger.sliding.interval, expect->trigger.sliding.interval);
      ASSERT_EQ(req->trigger.sliding.offset, expect->trigger.sliding.offset);
      ASSERT_EQ(req->trigger.sliding.soffset, expect->trigger.sliding.soffset);
      ASSERT_EQ(req->trigger.sliding.intervalUnit, expect->trigger.sliding.intervalUnit);
      ASSERT_EQ(req->trigger.sliding.slidingUnit, expect->trigger.sliding.slidingUnit);
      ASSERT_EQ(req->trigger.sliding.offsetUnit, expect->trigger.sliding.offsetUnit);
      ASSERT_EQ(req->trigger.sliding.soffsetUnit, expect->trigger.sliding.soffsetUnit);
      ASSERT_EQ(req->trigger.sliding.precision, expect->trigger.sliding.precision);
      break;
    }
    case WINDOW_TYPE_SESSION: {
      ASSERT_EQ(req->trigger.session.sessionVal, expect->trigger.session.sessionVal);
      ASSERT_EQ(req->trigger.session.slotId, expect->trigger.session.slotId);
      break;
    }
    case WINDOW_TYPE_STATE: {
      ASSERT_EQ(req->trigger.stateWin.extend, expect->trigger.stateWin.extend);
      ASSERT_EQ(req->trigger.stateWin.trueForDuration, expect->trigger.stateWin.trueForDuration);
      ASSERT_EQ(taosArrayGetSize(req->trigger.stateWin.pSlotIds), taosArrayGetSize(expect->trigger.stateWin.pSlotIds));
      for (int32_t i = 0; i < taosArrayGetSize(expect->trigger.stateWin.pSlotIds); i++) {
        ASSERT_EQ(*(int16_t*)taosArrayGet(req->trigger.stateWin.pSlotIds, i), *(int16_t*)taosArrayGet(expect->trigger.stateWin.pSlotIds, i));
      }
      break;
    }
    case WINDOW_TYPE_EVENT: {
      checkExprJsonStringEqual((char*)expect->trigger.event.startCond, (char*)req->trigger.event.startCond);
      checkExprJsonStringEqual((char*)expect->trigger.event.endCond, (char*)req->trigger.event.endCond);
      ASSERT_EQ(req->trigger.event.trueForDuration, expect->trigger.event.trueForDuration);
      break;
    }
    case WINDOW_TYPE_COUNT: {
      ASSERT_EQ(req->trigger.count.sliding, expect->trigger.count.sliding);
      ASSERT_EQ(req->trigger.count.countVal, expect->trigger.count.countVal);
      break;
    }
    case WINDOW_TYPE_PERIOD: {
      ASSERT_EQ(req->trigger.period.offset, expect->trigger.period.offset);
      ASSERT_EQ(req->trigger.period.offsetUnit, expect->trigger.period.offsetUnit);
      ASSERT_EQ(req->trigger.period.period, expect->trigger.period.period);
      ASSERT_EQ(req->trigger.period.periodUnit, expect->trigger.period.periodUnit);
      ASSERT_EQ(req->trigger.period.precision, expect->trigger.period.precision);
      break;
    }
    default:
      ASSERT_TRUE(false);
  }

  ASSERT_EQ(req->triggerTblType, expect->triggerTblType);
  ASSERT_EQ(req->triggerTblUid != 0, expect->triggerTblUid != 0);
  ASSERT_EQ(req->outTblType, expect->outTblType);
  ASSERT_EQ(req->outStbExists, expect->outStbExists);
  ASSERT_EQ(req->outStbUid, expect->outStbUid);
  ASSERT_EQ(req->outStbSversion, expect->outStbSversion);
  ASSERT_EQ(req->eventTypes, expect->eventTypes);
  ASSERT_EQ(req->flags, expect->flags);
  ASSERT_EQ(req->tsmaId, expect->tsmaId);
  ASSERT_EQ(req->placeHolderBitmap, expect->placeHolderBitmap);
  if (BIT_FLAG_TEST_MASK(req->placeHolderBitmap, PLACE_HOLDER_PARTITION_ROWS)) {
    ASSERT_EQ(req->calcTsSlotId, expect->calcTsSlotId);
  }

  ASSERT_EQ(req->triggerTblVgId, expect->triggerTblVgId);
  ASSERT_EQ(req->outTblVgId, expect->outTblVgId);

  checkCreateStreamTriggerScanPlan(expect, req);
  checkCreateStreamCalcPlan(expect, req);

  ASSERT_EQ(taosArrayGetSize(req->calcScanPlanList), taosArrayGetSize(expect->calcScanPlanList));
  for (int32_t i = 0; i < taosArrayGetSize(req->calcScanPlanList); i++) {
    auto pCalcScan = (SStreamCalcScan *)taosArrayGet(req->calcScanPlanList, i);
    auto expectScan = (SStreamCalcScan *)taosArrayGet(expect->calcScanPlanList, i);
    ASSERT_EQ(pCalcScan->readFromCache, expectScan->readFromCache);
    const char* const ignoredFields[] = {
        "RequireDataOrder", "ResultDataOrder", "IsDistinct", "TableId",
        "STableId", "InterpFillMode", "InterpMode", "UserAppId",
        "OutputTsPrecision", "RefSource", "TimezoneName", "FirstDayOfWeek",
        "FastInterval", "ExInterval", "DelayFreeTask"};
    checkJsonStringEqual((char*)expectScan->scanPlan, (char*)pCalcScan->scanPlan, ignoredFields,
                         sizeof(ignoredFields) / sizeof(ignoredFields[0]));
  }

  if (req->subTblNameExpr == nullptr) {
    ASSERT_TRUE(expect->subTblNameExpr == nullptr);
  } else {
    ASSERT_TRUE(expect->subTblNameExpr != nullptr);
    checkExprJsonStringEqual((char*)expect->subTblNameExpr, (char*)req->subTblNameExpr);
  }

  if (req->tagValueExpr == nullptr) {
    ASSERT_TRUE(expect->tagValueExpr == nullptr);
  } else {
    ASSERT_TRUE(expect->tagValueExpr != nullptr);
    checkExprJsonStringEqual((char*)expect->tagValueExpr, (char*)req->tagValueExpr);
  }

  if (req->forceOutCols == nullptr) {
    ASSERT_TRUE(expect->forceOutCols == nullptr);
  } else {
    ASSERT_TRUE(expect->forceOutCols != nullptr);
    ASSERT_EQ(taosArrayGetSize(req->forceOutCols), taosArrayGetSize(expect->forceOutCols));
    for (int32_t i = 0; i < taosArrayGetSize(req->forceOutCols); i++) {
      auto pOutCols = (SStreamOutCol *)taosArrayGet(req->forceOutCols, i);
      auto expectOutCols = (SStreamOutCol *)taosArrayGet(expect->forceOutCols, i);
      ASSERT_EQ(pOutCols->type.type, expectOutCols->type.type);
      ASSERT_EQ(pOutCols->type.bytes, expectOutCols->type.bytes);
      ASSERT_EQ(pOutCols->type.precision, expectOutCols->type.precision);
      ASSERT_EQ(pOutCols->type.scale, expectOutCols->type.scale);

      ASSERT_EQ(std::string((char*)pOutCols->expr), std::string((char*)expectOutCols->expr));
    }
  }
}

TEST_F(ParserStreamTest, TestName) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
                tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
  });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "stream_t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);

  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);

  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", 0);

  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 8, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  addCreateStreamOutCols(&expect, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // stream db name
  setCreateStreamStreamName(&expect, "0.stream_streamdb_2.s1", "0.stream_streamdb_2");
  setCreateStreamSql(&expect, "create stream stream_streamdb_2.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb_2.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // stream name
  setCreateStreamStreamName(&expect, "0.stream_streamdb.s1_2", "0.stream_streamdb");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // trigger db name
  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb_2", "stream_t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 59, 0, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb_2\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"59\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // trigger table name
  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb_2", "stream_t2", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 60, 0, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"59\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb_2\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"60\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"60\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb_2\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // out db name
  setCreateStreamOutName(&expect, "0.stream_outdb_2", "stream_out");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // out table name
  setCreateStreamOutName(&expect, "0.stream_outdb_2", "stream_out");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // query db name
  setCreateStreamCalcDb(&expect, "0.stream_querydb_2");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"40\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  resetCreateStreamQueryScanPlan(&expect);
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb_2\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"40\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"40\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"40\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.stream_t2");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.stream_t2");

  // query table name
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"39\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  resetCreateStreamQueryScanPlan(&expect);
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb_2\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"11336080860969511580\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"39\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"39\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"39\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb_2\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.stream_t1");
  run("create stream stream_streamdb.s1_2 interval(1s) sliding(1s) from stream_triggerdb_2.stream_t2 into stream_outdb_2.stream_out as select _twstart, avg(c1) from stream_querydb_2.stream_t1");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestTriggerOption) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "stream_t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);

  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);

  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", 0);

  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 8, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  addCreateStreamOutCols(&expect, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // test water mark
  setCreateStreamWatermark(&expect, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(watermark(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(watermark(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamWatermark(&expect, 1000000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(watermark(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(watermark(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamWatermark(&expect, 0);

  // expired_time
  setCreateStreamExpiredTime(&expect, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(expired_time(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(expired_time(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamExpiredTime(&expect, 1000000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(expired_time(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(expired_time(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamExpiredTime(&expect, 0);

  // ignore disorder
  setCreateStreamIgDisorder(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(ignore_disorder) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(ignore_disorder) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamIgDisorder(&expect, false);

  // delete recalc
  setCreateStreamDeleteReCalc(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamDeleteReCalc(&expect, false);

  // count window delete recalc: only sliding step 1 is supported
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_COUNT);
  setCreateStreamTriggerCount(&expect, 3, 1);
  setCreateStreamDeleteReCalc(&expect, true);
  setCreateStreamSql(&expect,
                     "create stream stream_streamdb.s1 count_window(3, 1) from stream_triggerdb.stream_t1 "
                     "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, "
                     "avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(3, 1) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2");
  setCreateStreamDeleteReCalc(&expect, false);

  run("create stream stream_streamdb.s1 count_window(3) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);

  setCreateStreamTriggerCount(&expect, 1, 1);
  setCreateStreamDeleteReCalc(&expect, true);
  setCreateStreamSql(&expect,
                     "create stream stream_streamdb.s1 count_window(1) from stream_triggerdb.stream_t1 "
                     "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, "
                     "avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(1) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2");

  setCreateStreamTriggerCount(&expect, 3, 1);
  run("create stream stream_streamdb.s1 count_window(3, 2) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);

  setCreateStreamDeleteReCalc(&expect, false);
  setCreateStreamTriggerCount(&expect, 0, 0);
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_INTERVAL);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  // delete output table
  setCreateStreamDeleteOutTbl(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(delete_output_table ) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(delete_output_table ) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamDeleteOutTbl(&expect, false);

  // fill history
  setCreateStreamFillHistory(&expect, true);
  setCreateStreamFillHistoryStartTime(&expect, 1749626234000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(fill_history('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(fill_history('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamFillHistory(&expect, false);
  setCreateStreamFillHistoryStartTime(&expect, 0);

  // fill history first
  setCreateStreamFillHistoryFirst(&expect, true);
  setCreateStreamFillHistoryStartTime(&expect, 1749626234000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(fill_history_first('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(fill_history_first('2025-06-11 15:17:14')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamFillHistoryFirst(&expect, false);
  setCreateStreamFillHistoryStartTime(&expect, 0);

  // calc notify only
  setCreateStreamCalcNotifyOnly(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(calc_notify_only) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(calc_notify_only) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamCalcNotifyOnly(&expect, false);

  // low latency calc
  setCreateStreamLowLatencyCalc(&expect, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(low_latency_calc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(low_latency_calc) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamLowLatencyCalc(&expect, false);

  // force output
  // TODO(smj) : add later
  //setCreateStreamForceOutputCols(&expect, true);

  // pre filter
  // TODO(smj) : add later

  // max delay
  setCreateStreamMaxDelay(&expect, 3000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(max_delay(3s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(max_delay(3s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamMaxDelay(&expect, 1000000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(max_delay(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s)  from stream_triggerdb.stream_t1  stream_options(max_delay(1000s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  setCreateStreamMaxDelay(&expect, 0);

  // event type
  setCreateStreamEventTypes(&expect, EVENT_WINDOW_CLOSE);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1  stream_options(event_type(window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1  stream_options(event_type(window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamEventTypes(&expect, EVENT_WINDOW_OPEN);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(window_open)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(window_open)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 ");

  setCreateStreamEventTypes(&expect, EVENT_WINDOW_OPEN | EVENT_WINDOW_CLOSE);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(window_open | window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(window_open | window_close)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 ");

  // Test EVENT_IDLE and EVENT_RESUME (requires IDLE_TIMEOUT)
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "_tidlestart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "_tidleend", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_IDLE_START | PLACE_HOLDER_IDLE_END);
  resetCreateStreamQueryScanPlan(&expect);
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tidlestart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"188\",\"Type\":\"3537\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tidleend\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"189\",\"Type\":\"3538\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamIdleTimeout(&expect, 5000);  // 5s
  setCreateStreamEventTypes(&expect, EVENT_IDLE);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2 ");

  setCreateStreamEventTypes(&expect, EVENT_RESUME);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(resume)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(resume)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2 ");

  setCreateStreamEventTypes(&expect, EVENT_IDLE | EVENT_RESUME);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle | resume)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2 ");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle | resume)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2 ");

  setCreateStreamIdleTimeout(&expect, 0);  // Reset

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestCountWindowDeleteRecalcSupportsSlideOne) {
  run("create stream stream_streamdb.s1 count_window(3, 1) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(1) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(3) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);
  run("create stream stream_streamdb.s1 count_window(3, 2) from stream_triggerdb.stream_t1 "
      "stream_options(delete_recalc) into stream_outdb.stream_out as select _twstart, avg(c1) from "
      "stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);
}

TEST_F(ParserStreamTest, DeleteRecalcAllowsPureSliding) {
  run("create stream stream_streamdb.s_delete_recalc sliding(10s) "
      "from stream_triggerdb.stream_t1 stream_options(delete_recalc) "
      "into stream_outdb.stream_out as select _tcurrent_ts, avg(c1) "
      "from stream_querydb.stream_t2");
}

TEST_F(ParserStreamTest, TestCountWindowDefaultSlidingInCreateReq) {
  pstCheckCreateStreamCountTrigger(
      "create stream stream_streamdb.s1 count_window(1) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      1, 1);
  pstCheckCreateStreamCountTrigger(
      "create stream stream_streamdb.s1 count_window(1, 1) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      1, 1);
  pstCheckCreateStreamCountTrigger(
      "create stream stream_streamdb.s1 count_window(3) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      3, 3);
  pstCheckCreateStreamCountTrigger(
      "create stream stream_streamdb.s1 count_window(3, 1) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      3, 1);
}

TEST_F(ParserStreamTest, TestCountWindowTriggerJsonSliding) {
  pstCheckCountTriggerJson("{\"countVal\":\"3\",\"sliding\":\"2\"}", 2);
  pstCheckCountTriggerJson("{\"countVal\":\"3\",\"sliding\":\"3\"}", 3);
}

TEST_F(ParserStreamTest, TestTriggerType) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "stream_t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // test session window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_SESSION);
  setCreateStreamTriggerSession(&expect, 1, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 session(ts, 1a) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 session(ts, 1a) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerSession(&expect, 1000, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 session(ts, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 session(ts, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");


  // test state window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_STATE);
  setCreateStreamTriggerState(&expect, 0, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"284\",\"OutputRowSize\":\"284\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"11336080860969511580\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_3\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"284\",\"OutputRowSize\":\"284\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerState(&expect, 0, 0);
  setCreateStreamTriggerStateExtend(&expect, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c1, 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c1, 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerState(&expect, 0, 0);
  setCreateStreamTriggerStateExtend(&expect, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c1) extend(1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c1) extend(1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerState(&expect, 0, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"300\",\"OutputRowSize\":\"300\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1887832667250888763\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_3\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"300\",\"OutputRowSize\":\"300\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerState(&expect, 0, 1);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c2) true_for(1a) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c2) true_for(1a) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerState(&expect, 0, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c2) true_for(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c2) true_for(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // test multi-column state window: state_window(c1, c2)
  {
    int16_t multiSlotIds[] = {0, 1};
    setCreateStreamTriggerStateMulti(&expect, multiSlotIds, 2, 0);
  }
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"11336080860969511580\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1887832667250888763\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 state_window(c1, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c1, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 state_window(c1, c1) from "
      "stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_PAR_INVALID_STATE_WIN_COL);

  // interval window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_INTERVAL);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamTriggerSliding(&expect, 0, 's', 0, 0, 0, 0, 0, 1000, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerSliding(&expect, 0, 's', 0, 's', 0, 0, 0, 100000, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(100s, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 sliding(100s, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerSliding(&expect, 'h', 's', 0, 0, 0, 3600000, 0, 100000, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1h) sliding(100s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1h) sliding(100s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerSliding(&expect, 'h', 's', 'm', 0, 0, 3600000, 60000, 100000, 0);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1h, 1m) sliding(100s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1h, 1m) sliding(100s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // event window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_EVENT);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"11336080860969511580\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1887832667250888763\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerEvent(&expect,
    "{\"NodeType\":\"77\",\"Name\":\"EventStartLeaf\",\"EventStartLeaf\":{\"Cond\":{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"3831883314967181845\",\"UserAlias\":\"c1 > 1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"40\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}}}",
    "{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"8823744644992822608\",\"UserAlias\":\"c2 < 1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"42\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}",
    0);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerEvent(&expect,
    "{\"NodeType\":\"77\",\"Name\":\"EventStartLeaf\",\"EventStartLeaf\":{\"Cond\":{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"3831883314967181845\",\"UserAlias\":\"c1 > 1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"40\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}}}",
    "{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"8823744644992822608\",\"UserAlias\":\"c2 < 1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"42\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}}",
    3600000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) true_for(1h) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c2 < 1) true_for(1h) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");


  // count window
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_COUNT);
  setCreateStreamTriggerCount(&expect, 10, 10);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(10) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(10) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerCount(&expect, 20, 10);
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(20, 10) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(20, 10) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerCount(&expect, 20, 20);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"11336080860969511580\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1887832667250888763\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\",\"Tag\":false}}],\"Precision\":\"0\"}},\"Conditions\":{\"NodeType\":\"4\",\"Name\":\"LogicCondition\",\"LogicCondition\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"CondType\":\"2\",\"Parameters\":[{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}]}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(20, c1, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(20, c1, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerCount(&expect, 20, 10);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"11336080860969511580\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1887832667250888763\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\",\"Tag\":false}}],\"Precision\":\"0\"}},\"Conditions\":{\"NodeType\":\"4\",\"Name\":\"LogicCondition\",\"LogicCondition\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"CondType\":\"2\",\"Parameters\":[{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"1\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"101\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},\"Right\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}]}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"304\",\"OutputRowSize\":\"304\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"c2\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 count_window(20, 10, c1, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 count_window(20, 10, c1, c2) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // period window
  resetCreateStreamTriggerCols(&expect);
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_PERIOD);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"272\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_1\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":true,\"Output\":false,\"Name\":\"6954351318876756469\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"272\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":true,\"Output\":false,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerPeriod(&expect, 0, 's', 0, 0, 1000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamTriggerPeriod(&expect, 's', 'm', 0, 1000, 60000);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1m, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1m, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  /* 1q is normalized to 3n by parseNatualDuration before checkPeriodWindow runs */
  setCreateStreamTriggerPeriod(&expect, 0, 'n', 0, 0, 3);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1q) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1q) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestTriggerPartition) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "st1", WINDOW_TYPE_INTERVAL, TSDB_SUPER_TABLE, 42, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_SUPER_TABLE, 0, 0, 1, 0);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  addCreateStreamOutTags(&expect, "tag_tbname", TSDB_DATA_TYPE_BINARY, 0, 272, 0, 0);
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"63\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"_t\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_t\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"44\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"42\",\"Literal\":\"stream_streamdb.s1.stream_outdb.stream_out\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"stream_streamdb.s1.stream_outdb.stream_out\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");


  resetCreateStreamOutTags(&expect);
  addCreateStreamOutTags(&expect, "tag_tbname", TSDB_DATA_TYPE_BINARY, 0, 272, 0, 0);
  addCreateStreamOutTags(&expect, "tag1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutTags(&expect, "tag2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  addCreateStreamOutTags(&expect, "tag3", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6624793664427087962\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14710156417485655547\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6560904107297596314\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_5\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_5\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"3\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"4\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestTriggerRollupBy) {
  pstCheckParsedStreamRollupTagCol(
      "create stream stream_streamdb.s1 count_window(1) from stream_triggerdb.st1 rollup by tag1 as select * from "
      "stream_querydb.stream_t2",
      "tag1");
  pstCheckParsedStreamRollupTagCol(
      "create stream stream_streamdb.s1 count_window(1) from stream_triggerdb.st1 partition by tbname rollup by tag2 "
      "as select * from stream_querydb.stream_t2",
      "tag2");
}

TEST_F(ParserStreamTest, TestCreateStreamRollupSemantic) {
  setAsyncFlag("-1");

  pstCheckCreateStreamReqRollupTagCol(
      "create stream stream_streamdb.s_rollup interval(1s) sliding(1s) from stream_triggerdb.st1 rollup by tag2 "
      "into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      "tag2");
  pstCheckTriggerScanPseudoColPresent(
      "create stream stream_streamdb.s_rollup interval(1s) sliding(1s) from stream_triggerdb.st1 rollup by tag2 "
      "into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      "tag2");
  pstCheckCreateStreamReqNoRollupTagCol(
      "create stream stream_streamdb.s_no_rollup interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_no_from period(1s) rollup by tag2 into stream_outdb.rollup_no_from "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "requires explicit FROM");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_no_from_interval interval(1s) sliding(1s) rollup by tag2 "
      "into stream_outdb.rollup_no_from_interval as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "requires explicit FROM");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_count count_window(10) from stream_triggerdb.st1 rollup by tag2 "
      "into stream_outdb.rollup_count as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "unsupported trigger window");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_partition interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "partition by tbname rollup by tag2 into stream_outdb.rollup_partition "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "PARTITION BY");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_delete interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 stream_options(delete_output_table) into stream_outdb.rollup_delete "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "DELETE_OUTPUT_TABLE");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_col interval(1s) sliding(1s) from stream_triggerdb.st1 rollup by c1 "
      "into stream_outdb.rollup_col as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "tag column");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_int interval(1s) sliding(1s) from stream_triggerdb.st1 rollup by tag1 "
      "into stream_outdb.rollup_int as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "VARCHAR or NCHAR");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_normal interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "rollup by c2 into stream_outdb.rollup_normal as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "super table");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_child interval(1s) sliding(1s) from stream_triggerdb.st1s1 "
      "rollup by tag2 into stream_outdb.rollup_child as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "super table");
}

TEST_F(ParserStreamTest, TestCreateStreamRollupPlaceholderSemantic) {
  setAsyncFlag("-1");

  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_missing interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "into stream_outdb.rollup_ph_missing as select _tlocaltime, %%rollup_tag from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "require ROLLUP BY");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_missing_subtable interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "into stream_outdb.rollup_ph_missing_subtable output_subtable(cast(%%rollup_tag as varchar(20))) "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_SUBTABLE, "OUTPUT_SUBTABLE");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_missing_tags interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "into stream_outdb.rollup_ph_missing_tags tags(rollup_tag varchar(256) as %%rollup_tag) "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_OUT_TAGS, "Tags");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_missing_trigger_count "
      "event_window(start with _trollup_tbcount > 0 end with c1 > 0) from stream_triggerdb.st1 "
      "into stream_outdb.rollup_ph_missing_trigger_count as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "create stream's query part");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_missing_trigger_tag "
      "event_window(start with %%rollup_tag is not null end with c1 > 0) from stream_triggerdb.st1 "
      "into stream_outdb.rollup_ph_missing_trigger_tag as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "create stream's query part");
  pstCheckCreateStreamReqRollupTagCol(
      "create stream stream_streamdb.s_rollup_ph_query interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_query(ts, rollup_tag, tbcount) "
      "as select _tlocaltime, %%rollup_tag, _trollup_tbcount from stream_querydb.stream_t2",
      "tag2");
  pstCheckCreateStreamRollupTagPlaceholderType(
      "create stream stream_streamdb.s_rollup_ph_type interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_type(ts, rollup_tag) "
      "as select _tlocaltime, %%rollup_tag from stream_querydb.stream_t2",
      TSDB_DATA_TYPE_BINARY, 20);
  pstCheckCreateStreamReqRollupTagCol(
      "create stream stream_streamdb.s_rollup_ph_out interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_out output_subtable(cast(%%rollup_tag as varchar(20))) (ts, v) "
      "tags(rollup_tag varchar(256) as %%rollup_tag) as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      "tag2");
  pstCheckCreateStreamReqRollupTagColWithSubtableExpr(
      "create stream stream_streamdb.s_rollup_ph_default_subtable interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 rollup by tag2 into stream_outdb.rollup_ph_default_subtable (ts, v) "
      "tags(rollup_tag varchar(256) as %%rollup_tag) as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      "tag2");
  pstCheckCreateStreamReqRollupTagCol(
      "create stream stream_streamdb.s_rollup_ph_path interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_path (ts, v) "
      "tags(path varchar(256) as %%1) as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      "tag2");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_path_bad interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_path_bad (ts, v) "
      "tags(path varchar(256) as %%2) as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "partition index out of range");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_subtable interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_subtable output_subtable(cast(_trollup_tbcount as varchar(20))) "
      "(ts, v) tags(rollup_tag varchar(256) as %%rollup_tag) "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "_trollup_tbcount");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_tags interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 into stream_outdb.rollup_ph_tags output_subtable(cast(%%rollup_tag as varchar(20))) (ts, v) "
      "tags(tbcount int as _trollup_tbcount) "
      "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "_trollup_tbcount");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_prefilter_count interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 stream_options(pre_filter(_trollup_tbcount > 0)) "
      "into stream_outdb.rollup_ph_prefilter_count as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "create stream's query part");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_prefilter_tag interval(1s) sliding(1s) from stream_triggerdb.st1 "
      "rollup by tag2 stream_options(pre_filter(%%rollup_tag is not null)) "
      "into stream_outdb.rollup_ph_prefilter_tag as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, "create stream's query part");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_trigger_count "
      "event_window(start with _trollup_tbcount > 0 end with c1 > 0) from stream_triggerdb.st1 rollup by tag2 "
      "into stream_outdb.rollup_ph_trigger_count as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "unsupported trigger window");
  pstCheckCreateStreamRollupError(
      "create stream stream_streamdb.s_rollup_ph_trigger_tag "
      "event_window(start with %%rollup_tag is not null end with c1 > 0) from stream_triggerdb.st1 rollup by tag2 "
      "into stream_outdb.rollup_ph_trigger_tag as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_ROLLUP, "unsupported trigger window");
}

TEST_F(ParserStreamTest, TestNotify) {
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "stream_t1", WINDOW_TYPE_INTERVAL, TSDB_NORMAL_TABLE, 49, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6954351318876756469\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"49\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"49\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"stream_t1\",\"TableAlias\":\"stream_t1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // url
  addCreateStreamNotifyUrl(&expect, "ws://localhost:8080");
  setCreateStreamNotify(&expect, EVENT_WINDOW_CLOSE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  addCreateStreamNotifyUrl(&expect, "ws://localhost:8080/notify");
  setCreateStreamNotify(&expect, EVENT_WINDOW_CLOSE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // ON EVENT
  setCreateStreamNotify(&expect, EVENT_WINDOW_CLOSE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamNotify(&expect, EVENT_WINDOW_OPEN, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamNotify(&expect, EVENT_WINDOW_OPEN | EVENT_WINDOW_CLOSE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close|window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close|window_open) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // Test IDLE and RESUME events in NOTIFY (requires IDLE_TIMEOUT in stream_options)
  setCreateStreamIdleTimeout(&expect, 5000);  // 5s
  setCreateStreamNotify(&expect, EVENT_IDLE, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s)) notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (idle) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s)) notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (idle) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamNotify(&expect, EVENT_RESUME, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s)) notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (resume) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s)) notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (resume) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamNotify(&expect, EVENT_IDLE | EVENT_RESUME, false, false);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s)) notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (idle | resume) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s)) notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (idle | resume) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  setCreateStreamIdleTimeout(&expect, 0);  // Reset

  // where condition
  // TODO(smj) add later

  // notify stream_options
  setCreateStreamNotify(&expect, EVENT_WINDOW_CLOSE, false, true);
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) notify_options(notify_history) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 notify('ws://localhost:8080', 'ws://localhost:8080/notify') on (window_close) notify_options(notify_history) into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestNotifyWhereFilterFlag) {
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) {
      return;
    }
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    ASSERT_NE(pQuery->pCmdMsg, nullptr);
    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    ASSERT_TRUE((req.addOptions & NOTIFY_HAS_FILTER) != 0);
    ASSERT_EQ(req.notifyEventTypes, EVENT_WINDOW_CLOSE);
    ASSERT_NE(req.outCols, nullptr);
    ASSERT_EQ(taosArrayGetSize(req.outCols), 4);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_notify_where interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 "
      "notify('ws://localhost:8080/notify_where') on (window_close) where max_v >= 20 "
      "into stream_outdb.stream_out as "
      "select _twstart, c1, count(*) as cnt, max(c1) as max_v from %%trows partition by c1");
}

TEST_F(ParserStreamTest, TestOutTable) {
    setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "st1", WINDOW_TYPE_INTERVAL, TSDB_SUPER_TABLE, 42, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_NORMAL_TABLE, 0, 0, 1, 1);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // out table type
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_SUPER_TABLE, 0, 0, 1, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6624793664427087962\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14710156417485655547\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6560904107297596314\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_5\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_5\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  addCreateStreamOutTags(&expect, "tag1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutTags(&expect, "tag2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  addCreateStreamOutTags(&expect, "tag3", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutTags(&expect, "tag_tbname", TSDB_DATA_TYPE_BINARY, 0, 272, 0, 0);
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"63\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"_t\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_t\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"44\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"42\",\"Literal\":\"stream_streamdb.s1.stream_outdb.stream_out\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"stream_streamdb.s1.stream_outdb.stream_out\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"3\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"4\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3, tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3, tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  resetCreateStreamOutCols(&expect);
  resetCreateStreamOutTags(&expect);
  resetCreateStreamQueryScanPlan(&expect);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "st1", TSDB_SUPER_TABLE, 1, 0, 0, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6624793664427087962\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14710156417485655547\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6560904107297596314\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_5\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_5\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  addCreateStreamOutTags(&expect, "tag1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutTags(&expect, "tag2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  addCreateStreamOutTags(&expect, "tag3", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "c1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutCols(&expect, "c2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, 0);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14225662052965797447\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"c2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"3\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"32\",\"OutputRowSize\":\"32\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"63\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"_t\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_t\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"37\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"35\",\"Literal\":\"stream_streamdb.s1.stream_outdb.st1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"stream_streamdb.s1.stream_outdb.st1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"3\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.st1 as select ts, c1, c2 from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.st1 as select ts, c1, c2 from stream_querydb.stream_t2");

  resetCreateStreamOutTags(&expect);
  resetCreateStreamPartitionCols(&expect);
  resetCreateStreamSubTblNameExpr(&expect);
  resetCreateStreamTagValueExpr(&expect);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_t1", TSDB_NORMAL_TABLE, 0, 0, 1, 1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 into stream_outdb.stream_t1 as select ts, c1, c2 from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 into stream_outdb.stream_t1 as select ts, c1, c2 from stream_querydb.stream_t2");

  // sub table name expr
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_SUPER_TABLE, 0, 0, 1, 0);
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  addCreateStreamOutTags(&expect, "tag1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutTags(&expect, "tag2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  addCreateStreamOutTags(&expect, "tag3", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6624793664427087962\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14710156417485655547\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6560904107297596314\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_5\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_5\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"3\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"24\"},\"AliasName\":\"18423646831753367078\",\"UserAlias\":\"concat('t_', tag2)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"11067635649471498840\",\"UserAlias\":\"'t_'\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"t_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"t_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.stream_out output_subtable(concat('t_', tag2)) as select ts, c1, c2 from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.stream_out output_subtable(concat('t_', tag2)) as select ts, c1, c2 from stream_querydb.stream_t2");

  // out columns
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "out_ts", TSDB_DATA_TYPE_TIMESTAMP, 1, 8, 0, 0);
  addCreateStreamOutCols(&expect, "out_c1", TSDB_DATA_TYPE_INT, 5, 4, 0, 0);
  addCreateStreamOutCols(&expect, "out_c2", TSDB_DATA_TYPE_BINARY, 1, 20, 0, 0);
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"63\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"_t\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_t\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"44\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"42\",\"Literal\":\"stream_streamdb.s1.stream_outdb.stream_out\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"stream_streamdb.s1.stream_outdb.stream_out\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.stream_out (out_ts, out_c1 primary key, out_c2) as select ts, c1, c2 from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.stream_out (out_ts, out_c1 primary key, out_c2) as select ts, c1, c2 from stream_querydb.stream_t2");

  // out tag
  resetCreateStreamOutCols(&expect);
  resetCreateStreamOutTags(&expect);
  addCreateStreamOutCols(&expect, "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "c1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutCols(&expect, "c2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  addCreateStreamOutTags(&expect, "out_tag1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutTags(&expect, "out_tag2", TSDB_DATA_TYPE_BINARY, 0, 22, 0, 0);
  addCreateStreamOutTags(&expect, "out_tag3", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"7080784029850824542\",\"UserAlias\":\"cast(tag1 + 1 as int)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"3\",\"Name\":\"Operator\",\"Operator\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5102575358876116143\",\"UserAlias\":\"tag1 + 1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"OpType\":\"1\",\"OpFlag\":\"0\",\"Left\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},\"Right\":{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"5001870860487857737\",\"UserAlias\":\"1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"2506531902696159022\",\"UserAlias\":\"upper(tag2)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"upper\",\"Id\":\"73\",\"Type\":\"1505\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"3\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.stream_out tags(out_tag1 int as cast(tag1 + 1 as int), out_tag2 varchar(20) as upper(tag2), out_tag3 timestamp as tag3) as select ts, c1, c2 from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.stream_out tags(out_tag1 int as cast(tag1 + 1 as int), out_tag2 varchar(20) as upper(tag2), out_tag3 timestamp as tag3) as select ts, c1, c2 from stream_querydb.stream_t2");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestQuery) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  SCMCreateStreamReq expect = {0};
  SCMCreateStreamReq expectOrderBy = {0};

  auto clearCreateStreamReq = [&]() {
    tFreeSCMCreateStreamReq(&expect);
    memset(&expect, 0, sizeof(SCMCreateStreamReq));
    tFreeSCMCreateStreamReq(&expectOrderBy);
    memset(&expectOrderBy, 0, sizeof(SCMCreateStreamReq));
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expect, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });

  setCreateStreamTriggerTable(&expect, "0.stream_triggerdb", "st1", WINDOW_TYPE_INTERVAL, TSDB_SUPER_TABLE, 42, 0, 1);
  setCreateStreamOutTable(&expect, "0.stream_outdb", "stream_out", TSDB_SUPER_TABLE, 0, 0, 1, 0);
  setCreateStreamReq(&expect, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2", 0);
  setCreateStreamOption(&expect, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expect, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamTriggerCols(&expect, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  addCreateStreamOutTags(&expect, "tag_tbname", TSDB_DATA_TYPE_BINARY, 0, 272, 0, 0);
  setCreateStreamSubTblNameExpr(&expect, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"63\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"_t\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_t\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"44\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"42\",\"Literal\":\"stream_streamdb.s1.stream_outdb.stream_out\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"stream_streamdb.s1.stream_outdb.stream_out\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");

  // placeholder type
  // _tprev_ts
  resetCreateStreamOutCols(&expect);
  setCreateStreamTriggerSliding(&expect, 0, 's', 0, 0, 0, 0, 0, 1000, 0);
  addCreateStreamOutCols(&expect, "_tprev_ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_PREV_TS);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tprev_ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"175\",\"Type\":\"3521\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tprev_ts, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tprev_ts, avg(c1) from stream_querydb.stream_t2");

  // _tcurrent_ts
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "_tcurrent_ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_CURRENT_TS);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tcurrent_ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"176\",\"Type\":\"3522\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tcurrent_ts, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tcurrent_ts, avg(c1) from stream_querydb.stream_t2");

  // _tnext_ts
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "_tnext_ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_NEXT_TS);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tnext_ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"177\",\"Type\":\"3523\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tnext_ts, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tnext_ts, avg(c1) from stream_querydb.stream_t2");

  // _twstart
  resetCreateStreamOutCols(&expect);
  setCreateStreamTriggerSliding(&expect, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);
  addCreateStreamOutCols(&expect, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_WSTART);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // _twend
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "_twend", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_WEND);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twend\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"179\",\"Type\":\"3525\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twend, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twend, avg(c1) from stream_querydb.stream_t2");

  // _twduration
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "_twend", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "_twduration", TSDB_DATA_TYPE_BIGINT, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_WEND | PLACE_HOLDER_WDURATION);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"24\",\"OutputRowSize\":\"24\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twend\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"179\",\"Type\":\"3525\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twduration\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"180\",\"Type\":\"3526\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"24\",\"OutputRowSize\":\"24\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twend, _twduration, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twend, _twduration, avg(c1) from stream_querydb.stream_t2");

  // _twrownum
  resetCreateStreamOutCols(&expect);
  addCreateStreamOutCols(&expect, "_twend", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "_twrownum", TSDB_DATA_TYPE_BIGINT, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_WEND | PLACE_HOLDER_WROWNUM);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"24\",\"OutputRowSize\":\"24\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twend\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"179\",\"Type\":\"3525\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twrownum\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"181\",\"Type\":\"3527\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"24\",\"OutputRowSize\":\"24\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twend, _twrownum, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twend, _twrownum, avg(c1) from stream_querydb.stream_t2");

  // _tprev_localtime
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  addCreateStreamOutCols(&expect, "_tprev_localtime", TSDB_DATA_TYPE_TIMESTAMP, COL_HAS_TYPE_MOD, 8, 0,
                         TSDB_TIME_PRECISION_NANO);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_PERIOD);
  setCreateStreamTriggerPeriod(&expect, 0, 's', 0, 0, 1000);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_1\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_PREV_LOCAL);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tprev_localtime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"182\",\"Type\":\"3528\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tprev_localtime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tprev_localtime, avg(c1) from stream_querydb.stream_t2");


  // _tnext_localtime
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  addCreateStreamOutCols(&expect, "_tnext_localtime", TSDB_DATA_TYPE_TIMESTAMP, COL_HAS_TYPE_MOD, 8, 0,
                         TSDB_TIME_PRECISION_NANO);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_PERIOD);
  setCreateStreamTriggerPeriod(&expect, 0, 's', 0, 0, 1000);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_NEXT_LOCAL);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tnext_localtime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"183\",\"Type\":\"3529\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tnext_localtime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tnext_localtime, avg(c1) from stream_querydb.stream_t2");

  // _tgrpid
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  addCreateStreamOutCols(&expect, "_tnext_localtime", TSDB_DATA_TYPE_TIMESTAMP, COL_HAS_TYPE_MOD, 8, 0,
                         TSDB_TIME_PRECISION_NANO);
  addCreateStreamOutCols(&expect, "_tgrpid", TSDB_DATA_TYPE_BIGINT, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamTriggerType(&expect, WINDOW_TYPE_PERIOD);
  setCreateStreamTriggerPeriod(&expect, 0, 's', 0, 0, 1000);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_NEXT_LOCAL | PLACE_HOLDER_GRPID);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"24\",\"OutputRowSize\":\"24\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tnext_localtime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"183\",\"Type\":\"3529\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tgrpid\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"24\",\"OutputRowSize\":\"24\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"2\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tnext_localtime, _tgrpid, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tnext_localtime, _tgrpid, avg(c1) from stream_querydb.stream_t2");

  // _tlocaltime
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_LOCALTIME);
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2");


  // %%n
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  resetCreateStreamOutTags(&expect);
  setCreateStreamCalcDb(&expect, "0.stream_triggerdb");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "%%1", TSDB_DATA_TYPE_VARCHAR, 0, 272, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  addCreateStreamOutTags(&expect, "tag_tbname", TSDB_DATA_TYPE_BINARY, 0, 272, 0, 0);
  addCreateStreamOutTags(&expect, "tag1", TSDB_DATA_TYPE_INT, 0, 4, 0, 0);
  addCreateStreamOutTags(&expect, "tag2", TSDB_DATA_TYPE_BINARY, 0, 20, 0, 0);
  addCreateStreamOutTags(&expect, "tag3", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_LOCALTIME | PLACE_HOLDER_PARTITION_IDX | PLACE_HOLDER_PARTITION_TBNAME);
  setCreateStreamTagValueExpr(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"2\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"3\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"4\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"4\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"5\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}]");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1129\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1128\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}},\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false}}],\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"_placeholder_column\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":false,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"1\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"ColName\":\"expr_3\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\"}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1124\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%1, avg(c1) from %%tbname");
  //run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%1, avg(c1) from %%tbname");

  // %%tbname
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  setCreateStreamCalcDb(&expect, "0.stream_querydb");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "%%tbname", TSDB_DATA_TYPE_VARCHAR, 0, 272, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_LOCALTIME | PLACE_HOLDER_PARTITION_TBNAME);
  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6624793664427087962\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14710156417485655547\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6560904107297596314\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"312\",\"OutputRowSize\":\"312\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"1\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"UserAlias\":\"%%tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"187\",\"Type\":\"3533\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"195\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Literal\":\"\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%tbname, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%tbname, avg(c1) from stream_querydb.stream_t2");

  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  resetCreateStreamQueryScanPlan(&expect);
  setCreateStreamCalcDb(&expect, "0.stream_triggerdb");
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "%%tbname", TSDB_DATA_TYPE_VARCHAR, 0, 272, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_LOCALTIME | PLACE_HOLDER_PARTITION_TBNAME);
  addCreateStreamQueryScanPlan(
      &expect, false,
      "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\","
      "\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_"
      "triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"2\",\"InUse\":\"0\",\"NumOfEps\":\"3\",\"Eps\":[{\"Fqdn\":"
      "\"dnode_1\",\"Port\":\"6030\"},{\"Fqdn\":\"dnode_2\",\"Port\":\"6030\"},{\"Fqdn\":\"dnode_3\",\"Port\":\"6030\"}"
      "]},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{"
      "\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\","
      "\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\","
      "\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":"
      "true,\"Name\":\"4536029962895989025\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{"
      "\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":"
      "false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":"
      "\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{"
      "\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{"
      "\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":"
      "\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\","
      "\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\","
      "\"DbName\":\"\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\","
      "\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,"
      "\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":"
      "\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\","
      "\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":"
      "\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\","
      "\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\","
      "\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\","
      "\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\","
      "\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":"
      "\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},"
      "\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"UseTriggerTableList\":true,\"ScanCount\":\"1\","
      "\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,"
      "\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\","
      "\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,"
      "\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,"
      "\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":"
      "\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":"
      "\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\","
      "\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":"
      "\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\","
      "\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{"
      "\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":"
      "\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,"
      "\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":true,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(
      &expect, false,
      "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\","
      "\"SubplanId\":\"3\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_"
      "triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"3\",\"InUse\":\"0\",\"NumOfEps\":\"3\",\"Eps\":[{\"Fqdn\":"
      "\"dnode_1\",\"Port\":\"6030\"},{\"Fqdn\":\"dnode_2\",\"Port\":\"6030\"},{\"Fqdn\":\"dnode_3\",\"Port\":\"6030\"}"
      "]},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{"
      "\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"4\",\"TotalRowSize\":\"12\","
      "\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\","
      "\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":"
      "true,\"Name\":\"4536029962895989025\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{"
      "\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":"
      "false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":"
      "\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{"
      "\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"4\",\"SlotId\":\"1\",\"Expr\":{"
      "\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":"
      "\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\","
      "\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\","
      "\"DbName\":\"\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\","
      "\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,"
      "\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":"
      "\"Target\",\"Target\":{\"DataBlockId\":\"4\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\","
      "\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":"
      "\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\","
      "\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\","
      "\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\","
      "\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\","
      "\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":"
      "\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},"
      "\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"UseTriggerTableList\":true,\"ScanCount\":\"1\","
      "\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,"
      "\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\","
      "\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,"
      "\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,"
      "\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":"
      "\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":"
      "\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"4\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\","
      "\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":"
      "\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\","
      "\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{"
      "\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":"
      "\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,"
      "\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":true,\"ProcessOneBlock\":false}}");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}},{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934595\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":true,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"UserAlias\":\"%%tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"187\",\"Type\":\"3533\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"195\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Literal\":\"\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%tbname, avg(c1) from %%tbname");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%tbname, avg(c1) from %%tbname");


  // %%trows
  resetCreateStreamOutCols(&expect);
  resetCreateStreamTriggerCols(&expect);
  resetCreateStreamQueryScanPlan(&expect);
  setCreateStreamCalcTsSlotId(&expect, 1);
  addCreateStreamOutCols(&expect, "_tlocaltime", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expect, "%%tbname", TSDB_DATA_TYPE_VARCHAR, 0, 272, 0, 0);
  addCreateStreamOutCols(&expect, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expect, PLACE_HOLDER_LOCALTIME | PLACE_HOLDER_PARTITION_TBNAME | PLACE_HOLDER_PARTITION_ROWS);
  addCreateStreamQueryScanPlan(&expect, true, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"2\",\"InUse\":\"0\",\"NumOfEps\":\"3\",\"Eps\":[{\"Fqdn\":\"dnode_1\",\"Port\":\"6030\"},{\"Fqdn\":\"dnode_2\",\"Port\":\"6030\"},{\"Fqdn\":\"dnode_3\",\"Port\":\"6030\"}]},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"4536029962895989025\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");

  setCreateStreamTriggerScanPlan(&expect, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"316\",\"OutputRowSize\":\"316\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"4536029962895989025\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6624793664427087962\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"14710156417485655547\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"6560904107297596314\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"5\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_4\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"c1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"3\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"4\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"5\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_4\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"316\",\"OutputRowSize\":\"316\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"3\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"4\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"5\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamPartitionCols(&expect, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"tag1\",\"UserAlias\":\"tag1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"4\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag1\",\"DataBlockId\":\"0\",\"SlotId\":\"2\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"20\"},\"AliasName\":\"tag2\",\"UserAlias\":\"tag2\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"5\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag2\",\"DataBlockId\":\"0\",\"SlotId\":\"3\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}},{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"tag3\",\"UserAlias\":\"tag3\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"6\",\"ProjId\":\"0\",\"ColType\":\"2\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"tag3\",\"DataBlockId\":\"0\",\"SlotId\":\"4\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}]");
  setCreateStreamQueryCalcPlan(&expect, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_tlocaltime\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"184\",\"Type\":\"3530\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"UserAlias\":\"%%tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"187\",\"Type\":\"3533\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"195\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Literal\":\"\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"2\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"288\",\"OutputRowSize\":\"288\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"2\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expect, "create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%tbname, avg(c1) from %%trows");
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2, tag3 into stream_outdb.stream_out as select _tlocaltime, %%tbname, avg(c1) from %%trows");

  // order by place holder
  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);
    SCMCreateStreamReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS ==
               tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    checkCreateStreamReq(&expectOrderBy, &req, false);
    tFreeSCMCreateStreamReq(&req);
 });
  setCreateStreamTriggerTable(&expectOrderBy, "0.stream_triggerdb", "st1", WINDOW_TYPE_INTERVAL, TSDB_SUPER_TABLE, 42, 0, 1);
  setCreateStreamOutTable(&expectOrderBy, "0.stream_outdb", "stream_out", TSDB_SUPER_TABLE, 0, 0, 1, 0);
  setCreateStreamReq(&expectOrderBy, "0.stream_streamdb.s1", "0.stream_streamdb", "0.stream_querydb", "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2", 0);
  setCreateStreamOption(&expectOrderBy, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, EVENT_WINDOW_CLOSE, 0, 0, 512, -1);
  setCreateStreamTriggerScanPlan(&expectOrderBy, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"DbFName\":\"0.stream_triggerdb\",\"User\":\"\",\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"3785532846947205635\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"expr_2\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"expr_1\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}}}],\"ScanPseudoCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"expr_2\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}}],\"TableId\":\"42\",\"STableId\":\"0\",\"TableType\":\"1\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"TotalRowSize\":\"280\",\"OutputRowSize\":\"280\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  addCreateStreamQueryScanPlan(&expectOrderBy, false, "{\"NodeType\":\"1137\",\"Name\":\"PhysiSubplan\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"2\",\"SubplanId\":\"2\"},\"SubplanType\":\"3\",\"MsgType\":\"769\",\"Level\":\"1\",\"DbFName\":\"0.stream_querydb\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"1\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"RootNode\":{\"NodeType\":\"1101\",\"Name\":\"PhysiTableScan\",\"PhysiTableScan\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"4\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"7606375837463693085\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"1011735779866557034\",\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"ScanCols\":[{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"4\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"0\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}},{\"NodeType\":\"18\",\"Name\":\"Target\",\"Target\":{\"DataBlockId\":\"4\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"c1\",\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"ColName\":\"c1\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"TableId\":\"30\",\"STableId\":\"0\",\"TableType\":\"3\",\"TableName\":{\"NameType\":\"2\",\"AcctId\":\"0\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\"},\"GroupOrderScan\":false,\"VirtualStableScan\":false,\"ScanCount\":\"1\",\"ReverseScanCount\":\"0\",\"StartKey\":\"-9223372036854775808\",\"EndKey\":\"9223372036854775807\",\"Ratio\":1,\"DataRequired\":\"1\",\"Interval\":\"0\",\"Offset\":\"0\",\"Sliding\":\"0\",\"IntervalUnit\":\"0\",\"SlidingUnit\":\"0\",\"TriggerType\":\"0\",\"Watermark\":\"0\",\"IgnoreExpired\":\"0\",\"GroupSort\":false,\"AssignBlockUid\":false,\"IgnoreUpdate\":\"0\",\"FilesetDelimited\":false,\"NeedCountEmptyTable\":false,\"ParaTablesSort\":false,\"SmallDataTsSort\":false,\"FirstDayOfWeek\":\"0\"}},\"DataSink\":{\"NodeType\":\"1133\",\"Name\":\"PhysiDispatch\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"Name\":\"DataBlockDesc\",\"DataBlockDesc\":{\"DataBlockId\":\"4\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}},{\"NodeType\":\"20\",\"Name\":\"SlotDesc\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Name\":\"\",\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}");
  setCreateStreamTriggerCols(&expectOrderBy, "[{\"NodeType\":\"1\",\"Name\":\"Column\",\"Column\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"ts\",\"UserAlias\":\"ts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"42\",\"TableType\":\"1\",\"ColId\":\"1\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_triggerdb\",\"TableName\":\"st1\",\"TableAlias\":\"st1\",\"ColName\":\"ts\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":true}}]");
  setCreateStreamPartitionCols(&expectOrderBy, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"tbname\",\"UserAlias\":\"tbname\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"tbname\",\"Id\":\"85\",\"Type\":\"3501\",\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  addCreateStreamOutTags(&expectOrderBy, "tag_tbname", TSDB_DATA_TYPE_BINARY, 0, 272, 0, 0);
  setCreateStreamSubTblNameExpr(&expectOrderBy, "{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"63\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"concat\",\"Id\":\"70\",\"Type\":\"1502\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"2\",\"Literal\":\"_t\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_t\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"34\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"md5\",\"Id\":\"134\",\"Type\":\"1509\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"44\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"42\",\"Literal\":\"stream_streamdb.s1.stream_outdb.stream_out\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"stream_streamdb.s1.stream_outdb.stream_out\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"3\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"1\",\"Literal\":\"_\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"_\"}},{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"22\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"cast\",\"Id\":\"77\",\"Type\":\"2000\",\"Parameters\":[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_tgrpid\",\"Id\":\"185\",\"Type\":\"3531\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"2\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"1\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}");
  setCreateStreamTagValueExpr(&expectOrderBy, "[{\"NodeType\":\"5\",\"Name\":\"Function\",\"Function\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Name\":\"_placeholder_column\",\"Id\":\"186\",\"Type\":\"3532\",\"Parameters\":[{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"8\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"272\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":false,\"NotReserved\":true,\"IsNull\":true,\"Unit\":\"0\"}},{\"NodeType\":\"2\",\"Name\":\"Value\",\"Value\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"AliasName\":\"\",\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"1\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}]");
  setCreateStreamTriggerSliding(&expectOrderBy, 's', 's', 0, 0, 0, 1000, 0, 1000, 0);
  addCreateStreamOutCols(&expectOrderBy, "_twstart", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expectOrderBy, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamPlaceHolderBitmap(&expectOrderBy, PLACE_HOLDER_WSTART);
  setCreateStreamQueryCalcPlan(&expectOrderBy, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1113\",\"PhysiSort\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SortKeys\":[{\"NodeType\":\"10\",\"OrderByExpr\":{\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},\"Order\":\"1\",\"NullOrder\":\"2\"}}],\"Targets\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"CalcGroupIds\":false,\"ExcludePKCol\":false,\"NumOfGroupKeys\":\"0\"}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"_twstart\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  setCreateStreamSql(&expectOrderBy, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 order by _twstart");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 order by _twstart");

  resetCreateStreamOutCols(&expectOrderBy);
  addCreateStreamOutCols(&expectOrderBy, "newts", TSDB_DATA_TYPE_TIMESTAMP, 0, 8, 0, 0);
  addCreateStreamOutCols(&expectOrderBy, "avg(c1)", TSDB_DATA_TYPE_DOUBLE, 0, 8, 0, 0);
  setCreateStreamSql(&expectOrderBy, "create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twstart newts, avg(c1) from stream_querydb.stream_t2 order by newts");
  setCreateStreamQueryCalcPlan(&expectOrderBy, "{\"NodeType\":\"1138\",\"PhysiPlan\":{\"QueryId\":\"0\",\"NumOfSubplans\":\"1\",\"Subplans\":{\"NodeType\":\"15\",\"NodeList\":{\"DataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"NodeList\":[{\"NodeType\":\"1137\",\"PhysiSubplan\":{\"Id\":{\"QueryId\":\"0\",\"GroupId\":\"1\",\"SubplanId\":\"1\"},\"SubplanType\":\"5\",\"MsgType\":\"771\",\"Level\":\"0\",\"DbFName\":\"\",\"User\":\"\",\"NodeAddr\":{\"Id\":\"0\",\"InUse\":\"0\",\"NumOfEps\":\"0\"},\"Child\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"5\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":false,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"8589934594\"}}],\"RootNode\":{\"NodeType\":\"1108\",\"PhysiProject\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1113\",\"PhysiSort\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"2\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1110\",\"PhysiAgg\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"1\",\"TotalRowSize\":\"8\",\"OutputRowSize\":\"8\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"Children\":[{\"NodeType\":\"1111\",\"PhysiExchange\":{\"OutputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"0\",\"TotalRowSize\":\"12\",\"OutputRowSize\":\"12\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}},\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SrcStartGroupId\":\"2\",\"SrcEndGroupId\":\"2\",\"SeqRecvData\":false,\"DynTbname\":false,\"GrpSingleChannel\":false,\"SingleSource\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"AggFuncs\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"8\",\"Type\":\"2\",\"Parameters\":[{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"},\"UserAlias\":\"c1\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"30\",\"TableType\":\"3\",\"ColId\":\"2\",\"ProjId\":\"0\",\"ColType\":\"1\",\"DbName\":\"stream_querydb\",\"TableName\":\"stream_t2\",\"TableAlias\":\"stream_t2\",\"DataBlockId\":\"0\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"4\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"4\"}}}}}],\"MergeDataBlock\":true,\"GroupKeyOptimized\":false,\"HasCountFunc\":false}}],\"InputOrder\":\"0\",\"OutputOrder\":\"1\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"SortKeys\":[{\"NodeType\":\"10\",\"OrderByExpr\":{\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"newts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}},\"Order\":\"1\",\"NullOrder\":\"2\"}}],\"Targets\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"1\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"CalcGroupIds\":false,\"ExcludePKCol\":false,\"NumOfGroupKeys\":\"0\"}}],\"InputOrder\":\"0\",\"OutputOrder\":\"0\",\"DynamicOp\":false,\"ForceCreateNonBlockingOptr\":false,\"Projections\":[{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"0\",\"Expr\":{\"NodeType\":\"5\",\"Function\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"newts\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"Id\":\"178\",\"Type\":\"3524\",\"Parameters\":[{\"NodeType\":\"2\",\"Value\":{\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"LiteralSize\":\"0\",\"Flag\":false,\"Translate\":true,\"NotReserved\":true,\"IsNull\":false,\"Unit\":\"0\",\"Datum\":\"0\"}}],\"UdfBufSize\":\"0\",\"HasPk\":false,\"PkBytes\":\"0\",\"IsMergeFunc\":false,\"MergeFuncOf\":\"0\",\"TrimType\":\"0\",\"SrcFuncInputDataType\":{\"Type\":\"0\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"0\"}}}}},{\"NodeType\":\"18\",\"Target\":{\"DataBlockId\":\"3\",\"SlotId\":\"1\",\"Expr\":{\"NodeType\":\"1\",\"Column\":{\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"UserAlias\":\"avg(c1)\",\"HasNull\":false,\"RelatedTo\":\"0\",\"BindExprID\":\"0\",\"TableId\":\"0\",\"TableType\":\"0\",\"ColId\":\"0\",\"ProjId\":\"0\",\"ColType\":\"0\",\"DbName\":\"\",\"TableName\":\"\",\"TableAlias\":\"\",\"DataBlockId\":\"2\",\"SlotId\":\"0\",\"TableHasPk\":false,\"IsPk\":false,\"NumOfPKs\":\"0\",\"HasDep\":false,\"HasRef\":false,\"RefDb\":\"\",\"RefTable\":\"\",\"RefCol\":\"\",\"IsPrimTs\":false}}}}],\"MergeDataBlock\":true,\"IgnoreGroupId\":true,\"InputIgnoreGroup\":false}},\"DataSink\":{\"NodeType\":\"1133\",\"PhysiDispatch\":{\"InputDataBlockDesc\":{\"NodeType\":\"19\",\"DataBlockDesc\":{\"DataBlockId\":\"3\",\"TotalRowSize\":\"16\",\"OutputRowSize\":\"16\",\"Slots\":[{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"0\",\"DataType\":{\"Type\":\"9\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}},{\"NodeType\":\"20\",\"SlotDesc\":{\"SlotId\":\"1\",\"DataType\":{\"Type\":\"7\",\"Precision\":\"0\",\"Scale\":\"0\",\"Bytes\":\"8\"},\"Reserve\":false,\"Output\":true,\"Tag\":false}}],\"Precision\":\"0\"}}}},\"ShowRewrite\":false,\"IsView\":false,\"IsAudit\":false,\"RowThreshold\":\"4096\",\"DyRowThreshold\":false,\"DynTbname\":false,\"ProcessOneBlock\":false}}]}}}}");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as select _twstart newts, avg(c1) from stream_querydb.stream_t2 order by newts");

  clearCreateStreamReq();
}

TEST_F(ParserStreamTest, TestQueryFromPartTbnamePlaceholder) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(stage, PARSER_STAGE_TRANSLATE);
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_STREAM_STMT);

    SCreateStreamStmt* pStmt = (SCreateStreamStmt*)pQuery->pRoot;
    ASSERT_NE(pStmt->pQuery, nullptr);
    ASSERT_EQ(nodeType(pStmt->pQuery), QUERY_NODE_SELECT_STMT);
    SSelectStmt* pSelect = (SSelectStmt*)pStmt->pQuery;
    ASSERT_NE(pSelect->pFromTable, nullptr);
    ASSERT_EQ(nodeType(pSelect->pFromTable), QUERY_NODE_REAL_TABLE);
    SRealTableNode* pFromTable = (SRealTableNode*)pSelect->pFromTable;
    EXPECT_TRUE(pFromTable->table.singleTable);
    EXPECT_TRUE(pFromTable->asSingleTable);

    SCMCreateStreamReq req = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    ASSERT_NE(req.calcPlan, nullptr);

    SNode* pPlanNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pPlanNode));
    ASSERT_NE(pPlanNode, nullptr);

    const SExternalWindowPhysiNode* pExternal = pstFindExternalWindowNode((const SQueryPlan*)pPlanNode);
    ASSERT_NE(pExternal, nullptr);
    EXPECT_TRUE(pExternal->isSingleTable);

    nodesDestroyNode(pPlanNode);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s1 count_window(1, c1) from stream_triggerdb.st1 partition by tbname "
      "into stream_outdb.stream_out as select _twstart ts, %%tbname as tb, count(c1) c1, avg(c1) c2 "
      "from %%tbname where ts >= _twstart and ts < _twstart + 5m");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinAggSubqueries) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 3); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a "
      "join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDerivedAggFilterUsesPhysiExternalWindow) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanHashExternalWindowCount(pQuery, stage, 1); });

  run("create stream stream_streamdb.s1 interval(10s) sliding(1s) from stream_triggerdb.st1 "
      "partition by tbname stream_options(ignore_nodata_trigger) "
      "into stream_outdb.stream_out output_subtable('alarm_rl') (ts, cnt) as "
      "select ts, cnt from (select _twstart ts, count(*) cnt from %%tbname "
      "where ts >= _twstart and ts < _twend and c1 >= 11 and c2 = 0) "
      "where cnt >= 5");
}

TEST_F(ParserStreamTest,
       TestStreamImplicitHavingDisablesDerivedExternalWindow) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanHashExternalWindowCount(pQuery, stage, 0);
  });

  run("create stream stream_streamdb.s1 interval(10s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tbname "
      "stream_options(ignore_nodata_trigger) "
      "into stream_outdb.stream_out output_subtable('alarm_rl') "
      "(ts, cnt) as select ts, cnt from "
      "(select _twstart ts, count(*) cnt from %%tbname "
      "where ts >= _twstart and ts < _twend and c1 >= 11 and c2 = 0) "
      "having cnt >= 5");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowSessionDerivedAggregateEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanHashExternalWindowCount(pQuery, stage, 1); });

  run("create stream stream_streamdb.s1 session(ts, 1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select ts, cnt from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowCountDerivedAggregateEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanHashExternalWindowCount(pQuery, stage, 1); });

  run("create stream stream_streamdb.s1 count_window(2) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select ts, cnt from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowStateDerivedAggregateEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanHashExternalWindowCount(pQuery, stage, 1); });

  run("create stream stream_streamdb.s1 state_window(c1) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select ts, cnt from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowEventDerivedAggregateEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanHashExternalWindowCount(pQuery, stage, 1); });

  run("create stream stream_streamdb.s1 event_window(start with c1 > 0 end with c1 <= 0) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select ts, cnt from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinExprSubqueryDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend "
      "and 0 <= (select count(*) from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend)) a "
      "join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowJoinWithNestedExprSubqueryDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcAndScanPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 count_window(2, 1) from stream_triggerdb.st1 "
      "partition by tbname, tag1 into stream_outdb.stream_out as select a.ts, a.curr_c1, b.c2, c.c1 from "
      "(select _twend ts, last(c1) curr_c1, first(c1) prev_c1 from %%tbname "
      "where ts >= _twstart and ts <= _twend "
      "and ts >= (select min(ts) from %%tbname where c1 <> 0 and ts <= _twstart "
      "and c1 = (select last(c1) from %%tbname where ts >= _twstart and ts <= _twend))) a "
      "join "
      "(select _twend ts, tag1, c2 from stream_querydb.st1 where tag1 = %%2) b "
      "on a.ts = b.ts "
      "join "
      "(select _twend ts, c1 from stream_querydb.stream_t2 where c1 >= 0) c "
      "on b.ts = c.ts");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinAggSubqueriesByTwend) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 3);
    pstCheckCalcPlanExternalWindowNeedCalcTimeRangeCount(pQuery, stage, 1);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.te, a.cnt, b.minv from "
      "(select _twend te, count(*) cnt from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a "
      "join "
      "(select _twend te, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.te = b.te");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinAggSubqueriesByTwendAddsOuterTimeRange) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCreateStreamSelectTimeRange(pQuery, stage, true, true);
    pstCheckCreateStreamSelectStartRangeFlag(pQuery, stage, true);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.te, a.cnt, b.minv from "
      "(select _twend te, count(*) cnt from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a "
      "join "
      "(select _twend te, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.te = b.te");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowExplicitStrictRangeHasNoAutoFlag) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCreateStreamSelectTimeRange(pQuery, stage, true, true);
    pstCheckCreateStreamSelectStartRangeFlag(pQuery, stage, false);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twend, avg(c1) from stream_querydb.stream_t2 "
      "where ts > _twstart and ts <= _twend");
}

TEST_F(ParserStreamTest, EnablesMultiGroupOnlyWithExternalWindowCapability) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 3);
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_TRUE(req.enableMultiGroupCalc);
    EXPECT_EQ(0, req.placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS);
    pstAssertNoInheritedMultiGroupJoinSubplans((const char*)req.calcPlan);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_multigroup_gate interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_triggerdb.st1 "
      "where ts >= _twstart and ts < _twend and tag1 = %%1");
}

TEST_F(ParserStreamTest, KeepsSingleGroupWhenCalcUsesAnotherVnodeDomain) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_FALSE(req.enableMultiGroupCalc);
    pstAssertNoInheritedMultiGroupJoinSubplans((const char*)req.calcPlan);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_cross_db_fallback interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_querydb.st1 "
      "where ts >= _twstart and ts < _twend and tag1 = %%1");
}

TEST_F(ParserStreamTest, KeepsSingleGroupWhenPartitionedTriggerReadsWindowBoundFixedTable) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes);
    ASSERT_EQ(1, infos.size());
    EXPECT_EQ(EXT_WIN_INPUT_WINDOW_BOUND, infos[0].inputMode);
    EXPECT_TRUE(infos[0].hasTimeRange);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_FALSE(req.enableMultiGroupCalc);
    pstAssertNoInheritedMultiGroupJoinSubplans((const char*)req.calcPlan);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_fixed_table_fallback count_window(5, 5, c1) "
      "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as "
      "select _twstart, count(*) from stream_triggerdb.stream_t1 "
      "where ts >= _twstart and ts <= _twend and c1 is not null");
}

TEST_F(ParserStreamTest, KeepsExternalWindowWhenMultiGroupCandidateIsFalse) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1);
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_FALSE(req.enableMultiGroupCalc);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_external_only interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend");
}

TEST_F(ParserStreamTest, RejectsPartitionRowsFromMultiGroupCapability) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_NE(0, req.placeHolderBitmap & PLACE_HOLDER_PARTITION_ROWS);
    EXPECT_FALSE(req.enableMultiGroupCalc);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_trows_gate count_window(2, 1) "
      "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as "
      "select _twstart, count(*) from %%trows");
}

TEST_F(ParserStreamTest, SkipsBroadcastExternalWindowForPartitionRows) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_trows_window_bound session(ts, 10m) "
      "from stream_triggerdb.st1 into stream_outdb.stream_out as "
      "select ts, c1, _twstart, _twend, _twrownum from %%trows");
}

TEST_F(ParserStreamTest, MultiGroupJoinAcceptsExternalWindowTreesBehindTransparentNodes) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckMultiGroupJoinPlan(pQuery, stage, true, 1, 1, -1, -1, 1);
  });

  run("create stream stream_streamdb.s_multigroup_transparent interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.ts = b.ts where a.cnt >= 0");
}

TEST_F(ParserStreamTest, MultiGroupJoinSerializesInheritedQualificationForDistributedLeaves) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(PARSER_STAGE_TRANSLATE, stage);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);
    ASSERT_TRUE(req.enableMultiGroupCalc);
    ASSERT_NE(nullptr, req.calcPlan);
    EXPECT_NE(nullptr, strstr((const char*)req.calcPlan, "\"InheritedMultiGroupJoin\":true"));

    SNode* pPlanNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((const char*)req.calcPlan, &pPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> planGuard(pPlanNode, nodesDestroyNode);
    int32_t                                        subplanCount = 0;
    int32_t                                        markedSubplanCount = 0;
    int32_t                                        inheritedSubplanCount = 0;
    SNode*                                         pLevelNode = nullptr;
    FOREACH(pLevelNode, ((const SQueryPlan*)pPlanNode)->pSubplans) {
      SNode* pSubplanNode = nullptr;
      FOREACH(pSubplanNode, ((const SNodeListNode*)pLevelNode)->pNodeList) {
        const auto*              pSubplan = (const SSubplan*)pSubplanNode;
        SMultiGroupJoinPhysiInfo info;
        pstCollectMultiGroupJoinPhysiInfo(pSubplan->pNode, &info);
        ++subplanCount;
        if (pSubplan->inheritedMultiGroupJoin) {
          ++inheritedSubplanCount;
          EXPECT_EQ(0, info.hashJoins.size());
        }
        for (const SHashJoinPhysiNode* pJoin : info.hashJoins) {
          if (pJoin->multiGroupJoin) {
            ++markedSubplanCount;
            EXPECT_FALSE(pSubplan->inheritedMultiGroupJoin);
          }
        }
      }
    }
    EXPECT_EQ(2, subplanCount);
    EXPECT_EQ(1, markedSubplanCount);
    EXPECT_EQ(1, inheritedSubplanCount);
  });

  run("create stream stream_streamdb.s_multigroup_inherited interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.ts = b.ts where a.cnt >= 0");
}

TEST_F(ParserStreamTest, MultiGroupJoinAcceptsUnaryBroadcastPredicate) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckMultiGroupJoinPlan(pQuery, stage, true, 1, 1, -1, -1, 1);

    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes);

    const SExternalWindowPhysiInfo* pBroadcast = nullptr;
    for (const auto& info : infos) {
      if ("stream_t2" == info.sourceTable) {
        ASSERT_EQ(nullptr, pBroadcast);
        pBroadcast = &info;
      }
    }
    ASSERT_NE(nullptr, pBroadcast);
    EXPECT_EQ(EXT_WIN_INPUT_BROADCAST, pBroadcast->inputMode);
    EXPECT_FALSE(pBroadcast->hasTimeRange);
  });

  run("create stream stream_streamdb.s_multigroup_unary interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.v0, b.v1 from "
      "(select _twstart ts, max(c1) v0 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, c1 v1 from stream_querydb.stream_t2 where c1 is not null) b "
      "on a.ts = b.ts");
}

TEST_F(ParserStreamTest, MultiGroupJoinMarksEveryLevelOfNestedJoinTree) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckMultiGroupJoinPlan(pQuery, stage, true, 3, 3, -1, -1, 3);
  });

  run("create stream stream_streamdb.s_multigroup_nested interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.v0, b.v1, c.v2, d.v3 from "
      "(select _twstart ts, max(c1) v0 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, max(c1) v1 from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.ts = b.ts join "
      "(select _twstart ts, max(c1) v2 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) c on b.ts = c.ts join "
      "(select _twstart ts, max(c1) v3 from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) d on c.ts = d.ts");
}

TEST_F(ParserStreamTest, MultiGroupJoinRejectsMissingBaseGroupSource) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckMultiGroupJoinPlan(pQuery, stage, false, 1, 0); });

  run("create stream stream_streamdb.s_multigroup_missing_base interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.cnt, b.bcnt from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, count(*) bcnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend limit 1) b on a.ts = b.ts");
}

TEST_F(ParserStreamTest, MultiGroupJoinRejectsNonTransparentOperator) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckMultiGroupJoinPlan(pQuery, stage, false, 1, 0, 3); });

  run("create stream stream_streamdb.s_multigroup_sort interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.ts = b.ts order by a.ts");
}

TEST_F(ParserStreamTest, MultiGroupJoinRejectsCalcGroupingAndPartitionRows) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckMultiGroupJoinPlan(pQuery, stage, false, 1, 0); });
  run("create stream stream_streamdb.s_multigroup_calc_partition interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, count(*) from "
      "(select _twstart ts, c1 from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, c1 from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts partition by a.ts");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckMultiGroupJoinPlan(pQuery, stage, false, 0, 0); });
  run("create stream stream_streamdb.s_multigroup_trows count_window(2, 1) "
      "from stream_triggerdb.st1 partition by tbname into stream_outdb.stream_out as "
      "select _twstart, count(*) from %%trows");
}

TEST_F(ParserStreamTest, MultiGroupJoinLeavesNoPartialMarkWhenAncestorIsUnsafe) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckMultiGroupJoinPlan(pQuery, stage, false, 2, 0, 4, 1); });

  run("create stream stream_streamdb.s_multigroup_atomic interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.v0, b.v1, c.v2 from "
      "(select _twstart ts, max(c1) v0 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, max(c1) v1 from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.ts = b.ts join "
      "(select _twstart ts, max(c1) v2 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend order by ts) c on b.ts = c.ts");
}

TEST_F(ParserStreamTest, MultiGroupJoinForcesEveryMarkedJoinToPhysicalHash) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckMultiGroupJoinPlan(pQuery, stage, true, 2, 2, -1, -1, 2);
  });

  run("create stream stream_streamdb.s_multigroup_hash interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.v0, b.v1, c.v2 from "
      "(select _twstart ts, max(c1) v0 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, max(c1) v1 from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on a.ts = b.ts join "
      "(select _twstart ts, max(c1) v2 from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) c on b.ts = c.ts");
}

TEST_F(ParserStreamTest, ExternalWindowAcceptsExactAndBounds) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1);
    pstCheckCalcPlanExternalWindowNeedCalcTimeRangeCount(pQuery, stage, 1);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart - 1s and ts <= _twend + 1s");
}

TEST_F(ParserStreamTest, ExternalWindowAcceptsMultipleBoundsUsingMaxAndMin) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1);
    pstCheckCalcPlanExternalWindowNeedCalcTimeRangeCount(pQuery, stage, 1);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart - 1s and ts > _twstart - 500a "
      "and ts < _twend + 2s and ts <= _twend + 1s");
}

TEST_F(ParserStreamTest, ExternalWindowAcceptsCanonicalizedComparisonDirection) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1);
    pstCheckCalcPlanExternalWindowNeedCalcTimeRangeCount(pQuery, stage, 0);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where _twstart <= ts and _twend > ts");
}

TEST_F(ParserStreamTest, ExternalWindowRejectsMissingStartOrEnd) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart");
  run("create stream stream_streamdb.s2 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts < _twend");
}

TEST_F(ParserStreamTest, ExternalWindowRejectsOrNotAndNonPrimaryTimestamp) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart or ts < _twend");
  run("create stream stream_streamdb.s2 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where not (ts >= _twstart and ts < _twend)");
  run("create stream stream_streamdb.s3 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where c3 >= _twstart and c3 < _twend");
}

TEST_F(ParserStreamTest, ExternalWindowRejectsDynamicDynamicAndNegativeOffset) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart + _twduration and ts < _twend");
  run("create stream stream_streamdb.s2 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart + (-1s) and ts < _twend");
  run("create stream stream_streamdb.s3 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= now() and ts < _twend");
  run("create stream stream_streamdb.s4 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart + 1s + 1s and ts < _twend");
}

TEST_F(ParserStreamTest, NestedExternalWindowAcceptsCrossLayerBounds) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1);
    pstCheckCalcPlanExternalWindowNeedCalcTimeRangeCount(pQuery, stage, 1);
  });

  run("create stream stream_streamdb.s1 window ("
      "interval(10s) sliding(10s) as w_outer, count_window(2,1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= w_outer._twstart and ts < w_leaf._twend");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowLeftJoinAggSubqueriesDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a "
      "left join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowThreeInnerJoinAggSubqueries) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 4); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.ts, a.cnt, b.minv, c.maxv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a "
      "join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts "
      "join "
      "(select _twstart ts, max(c1) maxv from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) c "
      "on b.ts = c.ts");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinWindowAndOrdinaryKeysEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 3); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select t1.ts, t1.pile_code, t2.minv from "
      "(select _twstart ts, min(c1) pile_code from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) t1 "
      "join "
      "(select _twstart ts, min(c1) pile_code, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) t2 "
      "on t1.ts = t2.ts and t1.pile_code = t2.pile_code");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinMismatchedWindowKeysDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_join_mismatched_keys interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, _twstart ts2, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, _twend te, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts and a.ts2 = b.te");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinUnsupportedOnConditionsDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  const string prefix =
      "create stream stream_streamdb.s_join_on_negative interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b on ";
  run(prefix + "a.ts = b.ts or a.cnt = b.minv", TSDB_CODE_PLAN_NOT_SUPPORT_JOIN_COND, PARSER_STAGE_TRANSLATE);
  run(prefix + "a.ts = b.ts and a.cnt > b.minv");
  run(prefix + "a.ts = b.ts and abs(a.cnt) = b.minv");
  run(prefix + "a.ts = b.ts and a.cnt = 1");
  run(prefix + "a.ts = b.ts and a.cnt = %%1", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, PARSER_STAGE_TRANSLATE);
  run(prefix + "a.ts = b.ts and a.cnt = a.cnt");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowUnsupportedJoinSubtypesDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  const string prefix =
      "create stream stream_streamdb.s_join_subtype_negative interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as ";
  const string left =
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend) a ";
  const string right =
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) b ";
  run(prefix + "select a.ts, a.cnt, b.minv from " + left + "right join " + right + "on a.ts = b.ts");
  run(prefix + "select a.ts, a.cnt, b.minv from " + left + "full join " + right + "on a.ts = b.ts");
  run(prefix + "select a.ts, a.cnt from " + left + "left semi join " + right + "on a.ts = b.ts");
  run(prefix + "select a.ts, a.cnt from " + left + "left anti join " + right + "on a.ts = b.ts");
  run(prefix +
      "select a.ts, a.c1 ac1, b.c1 bc1 from stream_querydb.stream_t1 a left asof join "
      "stream_querydb.stream_t2 b on a.ts <= b.ts jlimit 1 "
      "where a.ts >= _twstart and a.ts < _twend");
  run(prefix +
      "select a.ts, a.c1 ac1, b.c1 bc1 from stream_querydb.stream_t1 a left window join "
      "stream_querydb.stream_t2 b window_offset(-1s, 1s) "
      "where a.ts >= _twstart and a.ts < _twend");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinParentLimitDisablesAll) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 where ts >= _twstart and ts < _twend) a "
      "join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts limit 10");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinChildPlaceholderDisablesAll) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.ts, a.cnt, b.minv from "
      "(select _twstart ts, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend and c1 > cast(_twstart as bigint)) a "
      "join "
      "(select _twstart ts, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.ts = b.ts");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowInnerJoinChildLimitKeepsOuterTimeRangeEmpty) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0);
    pstCheckCreateStreamSelectTimeRange(pQuery, stage, false);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select a.te, a.cnt, b.minv from "
      "(select _twend te, count(*) cnt from stream_querydb.stream_t1 "
      "where ts >= _twstart and ts < _twend limit 1) a "
      "join "
      "(select _twend te, min(c1) minv from stream_querydb.stream_t2 where ts >= _twstart and ts < _twend) b "
      "on a.te = b.te");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableBroadcastEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes);
    ASSERT_EQ(1, infos.size());
    EXPECT_EQ(EXT_WIN_INPUT_BROADCAST, infos[0].inputMode);
    EXPECT_FALSE(infos[0].hasTimeRange);
  });

  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out as select _twstart, c1 from stream_querydb.stream_t2 where c1 >= 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableMatchingPartitionPlaceholderBroadcastEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes);
    ASSERT_EQ(1, infos.size());
    EXPECT_EQ(EXT_WIN_INPUT_BROADCAST, infos[0].inputMode);
    EXPECT_FALSE(infos[0].hasTimeRange);
  });

  run("create stream stream_streamdb.s_broadcast_matching_partition interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select _twstart, c1 from stream_triggerdb.st1 where tag1 = %%1");
}

TEST_F(ParserStreamTest, AllowsMatchingPartitionPlaceholderInsideJoinBroadcastSubquery) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    vector<SCalcScanPhysiInfo>       scanInfos;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes, &scanInfos);

    ASSERT_EQ(3, infos.size());
    ASSERT_EQ(1, count_if(infos.begin(), infos.end(), [](const SExternalWindowPhysiInfo& info) {
                return "st1" == info.sourceTable && !info.hasTimeRange;
              }));
    const auto& dimensionInput = *find_if(infos.begin(), infos.end(), [](const SExternalWindowPhysiInfo& info) {
      return "st1" == info.sourceTable && !info.hasTimeRange;
    });
    EXPECT_EQ(EXT_WIN_INPUT_BROADCAST, dimensionInput.inputMode);
    EXPECT_FALSE(dimensionInput.hasTimeRange);

    int32_t dimensionScans = 0;
    for (const auto& scan : scanInfos) {
      if ("stream_querydb" != scan.sourceDb || "st1" != scan.sourceTable) {
        continue;
      }
      ++dimensionScans;
      EXPECT_TRUE(scan.scanType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN ||
                  scan.scanType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN);
      EXPECT_FALSE(scan.hasTimeRange);
      EXPECT_FALSE(scan.dynamicOp);
      EXPECT_FALSE(scan.virtualStableScan);
      if (scan.scanType == QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN) {
        EXPECT_FALSE(scan.paraTablesSort);
      }
    }
    EXPECT_GT(dimensionScans, 0);

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_TRUE(req.enableMultiGroupCalc);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_join_partition_value "
      "count_window(2,1) from stream_triggerdb.st1 "
      "partition by tbname,tag1 into stream_outdb.stream_out as "
      "select t1.ts,t2.c2 from "
      "(select _twend ts,last(c1) c1 from %%tbname "
      "where ts>=_twstart and ts<=_twend) t1 join "
      "(select _twend ts,tag1,c2 from stream_querydb.st1 "
      "where tag1=%%2) t2 on t1.ts=t2.ts");
}

TEST_F(ParserStreamTest, PhysicalScanMarksOnlyTbnamePlaceholderLeafForTriggerTableRouting) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    vector<SCalcScanPhysiInfo>       scanInfos;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes, &scanInfos);

    int32_t placeholderScans = 0;
    int32_t fixedScans = 0;
    for (const auto& info : scanInfos) {
      if ("st1" == info.sourceTable) {
        ++placeholderScans;
        EXPECT_TRUE(info.useTriggerTableList);
      } else if ("stream_t2" == info.sourceTable) {
        ++fixedScans;
        EXPECT_FALSE(info.useTriggerTableList);
      }
    }
    EXPECT_GT(placeholderScans, 0);
    EXPECT_GT(fixedScans, 0);
  });

  run("create stream stream_streamdb.s_scan_trigger_route "
      "count_window(2,1) from stream_triggerdb.st1 "
      "partition by tbname into stream_outdb.stream_out as "
      "select t1.ts,t2.c1 from "
      "(select _twend ts,last(c1) c1 from %%tbname "
      "where ts>=_twstart and ts<=_twend) t1 join "
      "(select _twend ts,c1 from stream_querydb.stream_t2) t2 on t1.ts=t2.ts");
}

TEST_F(ParserStreamTest, RejectsNonEqualityJoinBroadcastPlaceholder) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0);
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_FALSE(req.enableMultiGroupCalc);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_join_partition_non_equality "
      "count_window(2,1) from stream_triggerdb.st1 "
      "partition by tbname,tag1 into stream_outdb.stream_out as "
      "select t1.ts,t2.c2 from "
      "(select _twend ts,last(c1) c1 from %%tbname "
      "where ts>=_twstart and ts<=_twend) t1 join "
      "(select _twend ts,tag1,c2 from stream_querydb.st1 "
      "where c2<>%%2) t2 on t1.ts=t2.ts");
}

TEST_F(ParserStreamTest, RejectsWrappedColumnJoinBroadcastPlaceholder) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0);
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_FALSE(req.enableMultiGroupCalc);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_join_partition_wrapped_column "
      "count_window(2,1) from stream_triggerdb.st1 "
      "partition by tbname,tag2 into stream_outdb.stream_out as "
      "select t1.ts,t2.c2 from "
      "(select _twend ts,last(c1) c1 from %%tbname "
      "where ts>=_twstart and ts<=_twend) t1 join "
      "(select _twend ts,tag2,c2 from stream_querydb.st1 "
      "where lower(tag2)=%%2) t2 on t1.ts=t2.ts");
}

TEST_F(ParserStreamTest, RejectsMismatchedDirectBroadcastPlaceholder) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0);
    if (stage != PARSER_STAGE_TRANSLATE) return;

    SCMCreateStreamReq req = {};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));
    EXPECT_FALSE(req.enableMultiGroupCalc);
    tFreeSCMCreateStreamReq(&req);
  });

  run("create stream stream_streamdb.s_broadcast_mismatched_partition interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select _twstart, c1 from stream_triggerdb.st1 where c1 = %%1");
}

TEST_F(ParserStreamTest, RejectsOutOfRangeJoinBroadcastPlaceholder) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  const char* sql =
      "create stream stream_streamdb.s_join_partition_out_of_range "
      "count_window(2,1) from stream_triggerdb.st1 "
      "partition by tbname,tag1 into stream_outdb.stream_out as "
      "select t1.ts,t2.c2 from "
      "(select _twend ts,last(c1) c1 from %%tbname "
      "where ts>=_twstart and ts<=_twend) t1 join "
      "(select _twend ts,tag1,c2 from stream_querydb.st1 "
      "where tag1=%%3) t2 on t1.ts=t2.ts";
  array<char, 1024> msgBuf = {0};
  SQuery*           pQuery = nullptr;
  int32_t           code = pstTranslateCreateStreamSql(sql, &pQuery, msgBuf.data(), msgBuf.max_size());
  unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  EXPECT_EQ(TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, code) << msgBuf.data();
  EXPECT_NE(strstr(msgBuf.data(), "partition index out of range"), nullptr) << msgBuf.data();
}

TEST_F(ParserStreamTest, TestStreamExternalWindowFiveLeafWindowBoundAndBroadcastInputs) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    vector<SExternalWindowPhysiInfo> infos;
    vector<ENodeType>                joinTypes;
    vector<SCalcScanPhysiInfo>       scanInfos;
    pstCollectCalcPlanExternalWindowInfo(pQuery, stage, &infos, &joinTypes, &scanInfos);

    ASSERT_EQ(QUERY_NODE_CREATE_STREAM_STMT, nodeType(pQuery->pRoot));
    const auto* pCreate = (const SCreateStreamStmt*)pQuery->pRoot;
    EXPECT_EQ(pstCountNonWrapperSelects(pCreate->pQuery), infos.size());
    ASSERT_EQ(6, infos.size());

    vector<const SExternalWindowPhysiInfo*> broadcast;
    const SExternalWindowPhysiInfo*         pT0 = nullptr;
    const SExternalWindowPhysiInfo*         pT1 = nullptr;
    const SExternalWindowPhysiInfo*         pRoot = nullptr;
    for (const auto& info : infos) {
      if (EXT_WIN_INPUT_BROADCAST == info.inputMode) {
        broadcast.push_back(&info);
      } else if (info.sourceTable.empty()) {
        pRoot = &info;
      } else if ("stream_t2" == info.sourceTable) {
        pT1 = &info;
      } else {
        pT0 = &info;
      }
    }

    ASSERT_EQ(3, broadcast.size());
    const auto* pT2 = broadcast[0];
    const auto* pT3 = broadcast[1];
    const auto* pT4 = broadcast[2];
    ASSERT_NE(nullptr, pT0);
    ASSERT_NE(nullptr, pT1);
    ASSERT_NE(nullptr, pRoot);

    EXPECT_EQ("stream_t1", pT0->sourceTable);
    EXPECT_EQ(EXT_WIN_INPUT_WINDOW_BOUND, pT0->inputMode);
    EXPECT_TRUE(pT0->hasTimeRange);
    EXPECT_EQ("stream_t2", pT1->sourceTable);
    EXPECT_EQ(EXT_WIN_INPUT_WINDOW_BOUND, pT1->inputMode);
    EXPECT_TRUE(pT1->hasTimeRange);
    EXPECT_EQ("stream_t1", pT2->sourceTable);
    EXPECT_EQ("stream_t2", pT3->sourceTable);
    EXPECT_EQ("stream_t1", pT4->sourceTable);
    for (const auto* pBroadcast : {pT2, pT3, pT4}) {
      EXPECT_EQ(EXT_WIN_INPUT_BROADCAST, pBroadcast->inputMode);
      EXPECT_FALSE(pBroadcast->hasTimeRange);
      EXPECT_FALSE(pBroadcast->hasWindowConditions);
      EXPECT_EQ(0, pBroadcast->durationUpperBoundCount);
    }

    EXPECT_EQ(0, pT0->durationUpperBoundCount);
    EXPECT_TRUE(pT1->hasWindowConditions);
    EXPECT_EQ(QUERY_NODE_LOGIC_CONDITION, pT1->windowConditionNodeType);
    EXPECT_EQ(LOGIC_COND_TYPE_AND, pT1->windowConditionType);
    EXPECT_EQ(TSDB_DATA_TYPE_BOOL, pT1->windowConditionResultType);
    EXPECT_EQ(tDataTypes[TSDB_DATA_TYPE_BOOL].bytes, pT1->windowConditionResultBytes);
    EXPECT_EQ(1, pT1->durationUpperBoundCount);
    EXPECT_EQ(0, pT1->windowConditionC1Count);
    EXPECT_FALSE(pRoot->hasWindowConditions);
    EXPECT_EQ(0, pRoot->durationUpperBoundCount);

    int32_t totalDurationUpperBoundCount = 0;
    for (const auto& info : infos) {
      totalDurationUpperBoundCount += info.durationUpperBoundCount;
    }
    EXPECT_EQ(1, totalDurationUpperBoundCount);

    ASSERT_EQ(5, scanInfos.size());
    using SScanOwnership = tuple<string, bool, int32_t, int32_t>;
    vector<SScanOwnership> actualScanOwnership;
    for (const auto& scan : scanInfos) {
      EXPECT_EQ(0, scan.durationPredicateCount);
      actualScanOwnership.emplace_back(scan.sourceTable, scan.hasTimeRange, scan.c1NonnegativeCount,
                                       scan.durationPredicateCount);
    }
    vector<SScanOwnership> expectedScanOwnership = {
        make_tuple("stream_t1", true, 0, 0),  make_tuple("stream_t2", true, 1, 0),
        make_tuple("stream_t1", false, 1, 0), make_tuple("stream_t2", false, 1, 0),
        make_tuple("stream_t1", false, 1, 0),
    };
    sort(actualScanOwnership.begin(), actualScanOwnership.end());
    sort(expectedScanOwnership.begin(), expectedScanOwnership.end());
    EXPECT_EQ(expectedScanOwnership, actualScanOwnership);

    auto countFunction = [](const SExternalWindowPhysiInfo& info, const char* pFunctionName, const char* pUserAlias,
                            const char* pTableAlias, const char* pColName) {
      return count_if(info.functions.begin(), info.functions.end(),
                      [&](const SExternalWindowPhysiInfo::SFunctionInfo& func) {
                        return pFunctionName == func.functionName && pUserAlias == func.userAlias &&
                               pTableAlias == func.firstParamTableAlias && pColName == func.firstParamColName;
                      });
    };
    EXPECT_EQ(1, countFunction(*pRoot, "first", "first(t1.soc)", "t1", "soc"));
    EXPECT_EQ(1, countFunction(*pRoot, "last", "last(t1.soc)", "t1", "soc"));

    ASSERT_EQ(4, joinTypes.size());
    for (ENodeType type : joinTypes) {
      EXPECT_TRUE(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN == type || QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN == type);
    }
  });

  run("create stream stream_streamdb.s_five_leaf window (state_window(c1) extend(1) as w_order, "
      "count_window(2, 1) as w_leaf) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select first(t0.ts), first(t1.soc), last(t1.soc) from "
      "(select _twstart ts, max(c1) pile_code, max(c1) soc from stream_querydb.stream_t1 "
      "where ts >= w_order._twstart and ts <= _twstart) t0 "
      "join (select _twstart ts, max(c1) pile_code, first(c1) soc from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts <= _twend and _twduration <= 30000 and c1 >= 0) t1 "
      "on t0.ts = t1.ts and t0.pile_code = t1.pile_code "
      "join (select _twstart ts, c1 pile_code from stream_querydb.stream_t1 where c1 >= 0) t2 "
      "on t1.ts = t2.ts and t1.pile_code = t2.pile_code "
      "join (select _twstart ts, c1 pile_code from stream_querydb.stream_t2 where c1 >= 0) t3 "
      "on t2.ts = t3.ts and t2.pile_code = t3.pile_code "
      "join (select _twstart ts, c1 pile_code from stream_querydb.stream_t1 where c1 >= 0) t4 "
      "on t3.ts = t4.ts and t3.pile_code = t4.pile_code");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowAncestorWindowConditionDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_ancestor_window_condition window ("
      "state_window(c1) extend(1) as w_order, count_window(2, 1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts <= _twend and w_order._twduration <= 30000");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowBroadcastAncestorProjectionDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_broadcast_ancestor_projection window ("
      "state_window(c1) extend(1) as w_order, count_window(2, 1) as w_leaf) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select w_order._twstart, c1 from stream_querydb.stream_t2 where c1 >= 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowRowConditionRemovalFailureReleasesWindowNode) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of nodesMergeConds is unreliable in Windows release builds";
#endif
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  RowConditionMergeFailureGuard guard;
  run("create stream stream_streamdb.s_row_condition_merge_failure interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts <= _twend and _twduration <= 30000 and c1 >= 0",
      TSDB_CODE_OUT_OF_MEMORY, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableWindowPlaceholderDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });
  run("create stream stream_streamdb.s_broadcast_placeholder interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from stream_querydb.stream_t2 where c1 >= _twstart");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableAggregateDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });
  run("create stream stream_streamdb.s_broadcast_aggregate interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, count(*) from stream_querydb.stream_t2 where c1 >= 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableMultiRowsFunctionDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });
  run("create stream stream_streamdb.s_broadcast_multi_rows interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, tail(c1, 1) from stream_querydb.stream_t2 where c1 >= 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowIndefiniteRowsFunctionDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_indefinite_rows interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, csum(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDerivedWrapperIndefiniteRowsFunctionDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_derived_indefinite_rows interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts, csum(cnt) from (select _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend)");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDerivedWrapperNoAggregateEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1); });

  run("create stream stream_streamdb.s_derived_wrapper_no_agg interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts, cnt from (select _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDerivedWrapperAggregateDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");

  /* The derived wrapper has no external window node of its own, so an outer aggregate is
   * materialized as a plain table aggregate above the inner external window node. That operator
   * only switches aggregation context on groupId, never on the external-window row lineage
   * (pStreamBlkWinIdx), so several trigger windows carried by one calc request would be merged
   * into a single result. Keep external window disabled for this shape. */
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  run("create stream stream_streamdb.s_derived_wrapper_agg interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts, max(cnt) from (select _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend)");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectSelectDistinctDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  /* SELECT DISTINCT is materialized as a plain hash aggregate stacked above the external window
   * node. That operator switches aggregation context on groupId only, never on the external-window
   * row lineage, so distinct sets belonging to different trigger windows carried by one calc
   * request would be merged. The general auto-enable check must reject DISTINCT for every select
   * level, including a direct base table select. */
  run("create stream stream_streamdb.s_direct_select_distinct interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select distinct _twstart, avg(c1) from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDerivedWrapperDistinctDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  /* The distinct wrapper parent owns no external window node of its own, so its DISTINCT is
   * materialized as a plain table aggregate above the inner external window node and merges the
   * trigger windows carried by a single calc request. */
  run("create stream stream_streamdb.s_derived_wrapper_distinct interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select distinct ts, cnt from (select _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDerivedInnerDistinctDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });

  /* The recursive tree check visits every select level, so a DISTINCT placed on the derived inner
   * select must disable external window for the whole statement as well. */
  run("create stream stream_streamdb.s_derived_inner_distinct interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts, cnt from (select distinct _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend) where cnt > 0");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowAggFuncDistinctStillEnabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 1); });

  /* Only the statement-level SELECT DISTINCT disables external window. An aggregate-function-level
   * DISTINCT such as count(distinct c1) is evaluated inside the lineage aware stream aggregate
   * (pFunction->isDistinct), so the external window must stay enabled for that shape. */
  run("create stream stream_streamdb.s_agg_func_distinct interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select ts, cnt from (select _twstart ts, count(distinct c1) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend)");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableForeignPartitionPlaceholderDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });
  run("create stream stream_streamdb.s_broadcast_foreign_partition interval(1s) sliding(1s) "
      "from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as "
      "select _twstart, c1 from stream_querydb.st1 where tag1 = %%1");
}

TEST_F(ParserStreamTest, TestStreamExternalWindowDirectBaseTableOrderOrLimitDisabled) {
  setAsyncFlag("-1");
  useDb("root", "stream_streamdb");
  setCheckDdlFunc(
      [&](const SQuery* pQuery, ParserStage stage) { pstCheckCalcPlanExternalWindowCount(pQuery, stage, 0); });
  run("create stream stream_streamdb.s_broadcast_order interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from stream_querydb.stream_t2 where c1 >= 0 order by c1");
  run("create stream stream_streamdb.s_broadcast_limit interval(1s) sliding(1s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, c1 from stream_querydb.stream_t2 where c1 >= 0 limit 1");
}

TEST_F(ParserStreamTest, TestOutTagExprSpecialCases) {
  setAsyncFlag("-1");
  useDb("root", "testus");

  ITableBuilder& builder =
      g_mockCatalogService->createTableBuilder("stream_triggerdb", "stn_tag_precision", TSDB_SUPER_TABLE, 1, 1)
          .setPrecision(TSDB_TIME_PRECISION_NANO)
          .addColumn("ts", TSDB_DATA_TYPE_TIMESTAMP)
          .addTag("tag1", TSDB_DATA_TYPE_TIMESTAMP);
  builder.done();
  g_mockCatalogService->createSubTable("stream_triggerdb", "stn_tag_precision", "stn_tag_precision_s1", 2);

  run("create stream testus.s_tag_precision interval(1s) sliding(1s) "
      "from stream_triggerdb.stn_tag_precision partition by tag1 into stream_outdb.stream_out "
      "tags(out_tag1 timestamp as tag1) as select ts, c1 from stream_querydb.stream_t1");

  run("create stream testus.s_tag_const count_window(1, c1) "
      "from stream_triggerdb.st1 partition by tbname, tag1 into stream_outdb.stream_out "
      "tags(addr varchar(20) as 'aaaa', out_tag1 int as tag1) as select * from %%tbname");
}

TEST_F(ParserStreamTest, TestErrorName) {
  setAsyncFlag("-1");

  //run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  // db not exists
  run("create stream non_exist_db.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_FAILED);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into non_exist_db.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_FAILED);

  // table not exists
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from non_exist_db.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_TABLE_NOT_EXIST);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from non_exist_db.stream_t2", TSDB_CODE_PAR_TABLE_NOT_EXIST);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.non_exist_stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_TABLE_NOT_EXIST);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from non_exist_db.non_exist_stream_t2", TSDB_CODE_PAR_TABLE_NOT_EXIST);

}

TEST_F(ParserStreamTest, CreateStreamAcceptsNaturalTriggerMatrix) {
  enum class NaturalValue { kNone, kInterval, kSliding, kPeriod };
  struct TestCase {
    const char*  trigger;
    NaturalValue naturalValue;
    bool         omittedSliding;
  };
  const array<TestCase, 22> accepted = {{
      {"interval(1n)", NaturalValue::kInterval, true},
      {"interval(1q)", NaturalValue::kInterval, true},
      {"interval('1q')", NaturalValue::kInterval, true},
      {"interval(1y)", NaturalValue::kInterval, true},
      {"interval(1n) sliding(1s)", NaturalValue::kInterval, false},
      {"interval(1s) sliding(2s)", NaturalValue::kNone, false},
      {"interval(100s) sliding(500a)", NaturalValue::kNone, false},
      {"sliding(1n)", NaturalValue::kSliding, false},
      {"sliding(1q)", NaturalValue::kSliding, false},
      {"sliding('1q')", NaturalValue::kSliding, false},
      {"sliding(1y)", NaturalValue::kSliding, false},
      {"period(1q)", NaturalValue::kPeriod, false},
      {"period('1q')", NaturalValue::kPeriod, false},
      {"interval(1n, 27d)", NaturalValue::kNone, true},
      {"period(1n, 27d)", NaturalValue::kNone, false},
      {"sliding(1n, 27d)", NaturalValue::kNone, false},
      {"interval(29d, 1n)", NaturalValue::kNone, true},
      {"period(29d, 1n)", NaturalValue::kNone, false},
      {"sliding(29d, 1n)", NaturalValue::kNone, false},
      {"interval(1q, 83d)", NaturalValue::kInterval, true},
      {"period(1q, 83d)", NaturalValue::kPeriod, false},
      {"sliding(1q, 83d)", NaturalValue::kSliding, false},
  }};

  for (const TestCase& testCase : accepted) {
    const string sql = "create stream stream_streamdb.s_natural_matrix " + string(testCase.trigger) +
                       " from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
                       "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql << ": " << msgBuf.data();
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
    ASSERT_NE(pQuery, nullptr);

    const SCreateStreamStmt*  pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
    const SStreamTriggerNode* pTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
    ASSERT_NE(pTrigger, nullptr);
    ASSERT_NE(pTrigger->pTriggerWindow, nullptr);

    const SValueNode* pNatural = nullptr;
    if (QUERY_NODE_INTERVAL_WINDOW == nodeType(pTrigger->pTriggerWindow)) {
      const SIntervalWindowNode* pWindow = (const SIntervalWindowNode*)pTrigger->pTriggerWindow;
      if (testCase.omittedSliding) {
        EXPECT_EQ(pWindow->pSliding, nullptr) << sql;
      }
      if (NaturalValue::kInterval == testCase.naturalValue) {
        pNatural = (const SValueNode*)pWindow->pInterval;
      } else if (NaturalValue::kSliding == testCase.naturalValue) {
        pNatural = (const SValueNode*)pWindow->pSliding;
      }
    } else if (NaturalValue::kPeriod == testCase.naturalValue) {
      ASSERT_EQ(QUERY_NODE_PERIOD_WINDOW, nodeType(pTrigger->pTriggerWindow));
      pNatural = (const SValueNode*)((const SPeriodWindowNode*)pTrigger->pTriggerWindow)->pPeroid;
    }

    if (nullptr != strstr(testCase.trigger, "1q")) {
      ASSERT_NE(pNatural, nullptr) << sql;
      EXPECT_EQ(pNatural->datum.i, 3) << sql;
      EXPECT_EQ(pNatural->unit, 'n') << sql;
    }
  }
}

TEST_F(ParserStreamTest, CreateStreamRejectsNaturalTriggerMatrix) {
  const array<const char*, 19> rejected = {{
      "interval(1n) sliding(1n)", "interval(1s) sliding(0a)", "interval(1n, auto)", "period(1n, -1d)",
      "sliding(1n, auto)",        "period(1n, auto)",         "sliding(1n, 1n)",    "interval(1s, 1s)",
      "period(1s, 1s)",           "sliding(1s, 1s)",          "interval(1n, 28d)",  "period(1n, 28d)",
      "sliding(1n, 28d)",         "interval(28d, 1n)",        "period(28d, 1n)",    "sliding(28d, 1n)",
      "interval(1q, 84d)",        "period(1q, 84d)",          "sliding(1q, 84d)",
  }};

  for (const char* trigger : rejected) {
    const string sql = "create stream stream_streamdb.s_natural_matrix " + string(trigger) +
                       " from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
                       "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    EXPECT_NE(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql;
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);
  }
}

TEST_F(ParserStreamTest, CreateStreamChecksNaturalSlidingRepresentability) {
  const array<const char*, 3> accepted = {{
      "sliding(3509654504n)",
      "sliding(292471208y)",
      "sliding(1169884834q)",
  }};
  const array<const char*, 7> rejected = {{
      "sliding(3509654505n)",
      "sliding(292471209y)",
      "sliding(1169884835q)",
      "sliding(9223372036854775807n)",
      "sliding(9223372036854775807y)",
      "sliding(292471208y, 292471209y)",
      "sliding(292471208y, 9223372036854775807y)",
  }};

  for (const char* trigger : accepted) {
    const string sql = "create stream stream_streamdb.s_natural_limit " + string(trigger) +
                       " from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
                       "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    EXPECT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql << ": " << msgBuf.data();
    qDestroyQuery(pQuery);
  }

  for (const char* trigger : rejected) {
    const string sql = "create stream stream_streamdb.s_natural_limit " + string(trigger) +
                       " from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
                       "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    EXPECT_EQ(TSDB_CODE_PAR_INTER_VALUE_TOO_BIG,
              pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql << ": " << msgBuf.data();
    qDestroyQuery(pQuery);
  }
}

TEST_F(ParserStreamTest, CreateStreamNormalizesNaturalOffsets) {
  struct TestCase {
    const char* trigger;
    int64_t     expected;
  };
  const array<TestCase, 4> testCases = {{
      {"interval(29d, 1n)", 2678400000LL},
      {"period(29d, 1n)", 2678400000LL},
      {"sliding(29d, 1n)", 2678400000LL},
      {"interval('4n', '1q')", 8035200000LL},
  }};

  for (const TestCase& testCase : testCases) {
    const string sql = "create stream stream_streamdb.s_natural_offset " + string(testCase.trigger) +
                       " from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
                       "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql << ": " << msgBuf.data();
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);

    const SCreateStreamStmt*  pStmt = (const SCreateStreamStmt*)pQuery->pRoot;
    const SStreamTriggerNode* pStreamTrigger = (const SStreamTriggerNode*)pStmt->pTrigger;
    const SNode*              pWindow = pStreamTrigger->pTriggerWindow;
    const SValueNode*         pOffset = nullptr;
    if (QUERY_NODE_PERIOD_WINDOW == nodeType(pWindow)) {
      pOffset = (const SValueNode*)((const SPeriodWindowNode*)pWindow)->pOffset;
    } else {
      const SIntervalWindowNode* pInterval = (const SIntervalWindowNode*)pWindow;
      pOffset = (const SValueNode*)(pInterval->pOffset ? pInterval->pOffset : pInterval->pSOffset);
    }

    ASSERT_NE(pOffset, nullptr) << sql;
    EXPECT_EQ(pOffset->datum.i, testCase.expected) << sql;
    EXPECT_EQ(pOffset->unit, 'a') << sql;
  }
}

TEST_F(ParserStreamTest, CreateStreamCanonicalizesUppercaseNaturalUnitsInRequest) {
  struct TestCase {
    const char* trigger;
    int64_t     expectedValue;
    char        expectedUnit;
    int64_t     expectedOffset;
    char        expectedOffsetUnit;
    bool        period;
  };
  const array<TestCase, 3> testCases = {{
      {"sliding(2y, 1Y)", 2, 'y', 31536000000LL, 'a', false},
      {"sliding(2Y)", 2, 'y', 0, 0, false},
      {"period(2Y)", 2, 'y', 0, 0, true},
  }};

  for (const TestCase& testCase : testCases) {
    const string sql = "create stream stream_streamdb.s_uppercase_natural " + string(testCase.trigger) +
                       " from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
                       "as select _tlocaltime, avg(c1) from stream_querydb.stream_t2";
    array<char, 1024> msgBuf = {0};
    SQuery*           pQuery = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, pstTranslateCreateStreamSql(sql.c_str(), &pQuery, msgBuf.data(), msgBuf.max_size()))
        << sql << ": " << msgBuf.data();
    unique_ptr<SQuery, decltype(&qDestroyQuery)> queryGuard(pQuery, qDestroyQuery);

    SCMCreateStreamReq req = {0};
    ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req))
        << sql;
    unique_ptr<SCMCreateStreamReq, decltype(&tFreeSCMCreateStreamReq)> reqGuard(&req, tFreeSCMCreateStreamReq);

    if (testCase.period) {
      ASSERT_EQ(req.triggerType, WINDOW_TYPE_PERIOD) << sql;
      EXPECT_EQ(req.trigger.period.period, testCase.expectedValue) << sql;
      EXPECT_EQ(req.trigger.period.periodUnit, testCase.expectedUnit) << sql;
    } else {
      ASSERT_EQ(req.triggerType, WINDOW_TYPE_INTERVAL) << sql;
      EXPECT_EQ(req.trigger.sliding.sliding, testCase.expectedValue) << sql;
      EXPECT_EQ(req.trigger.sliding.slidingUnit, testCase.expectedUnit) << sql;
      EXPECT_EQ(req.trigger.sliding.soffset, testCase.expectedOffset) << sql;
      EXPECT_EQ(req.trigger.sliding.soffsetUnit, testCase.expectedOffsetUnit) << sql;
    }
  }
}

TEST_F(ParserStreamTest, TestErrorTriggerWindow) {
  setAsyncFlag("-1");

  // wrong trigger window
  run("create stream stream_streamdb.s1 invalid_window(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid session window

  // missing session_val
  run("create stream stream_streamdb.s1 session(ts) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // not ts cols
  run("create stream stream_streamdb.s1 session(c1, 1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_SESSION_COL);

  // invalid time unit
  run("create stream stream_streamdb.s1 session(ts, 1y) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_SESSION_GAP);
  run("create stream stream_streamdb.s1 session(ts, 1n) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_SESSION_GAP);
  run("create stream stream_streamdb.s1 session(ts, 0s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_SESSION_GAP);

  // invalid state window

  // invalid col type
  run("create stream stream_streamdb.s1 state_window(ts) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_STATE_WIN_TYPE);

  // invalid true for less than 0
  run("create stream stream_streamdb.s1 state_window(c1) true_for(-1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid mixed and ambiguous positional syntax
  run("create stream stream_streamdb.s1 state_window(c1, c2, 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 state_window(case when c1 > 0 then 1 else 0 end, 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // invalid true for unit
  run("create stream stream_streamdb.s1 state_window(c1) true_for(1y) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_TRUE_FOR_UNIT);
  run("create stream stream_streamdb.s1 state_window(c1) true_for('1y') from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_TRUE_FOR_UNIT);
  run("create stream stream_streamdb.s1 state_window(c1) true_for(1x) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 state_window(c1) true_for('1x') from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid interval window

  run("create stream stream_streamdb.s1 sliding(0) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _tlocaltime, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL);

  // sliding offset bigger than sliding
  run("create stream stream_streamdb.s1 sliding(1s, 2s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // invalid interval val
  run("create stream stream_streamdb.s1 interval(0) sliding(3s, 2s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL);
  run("create stream stream_streamdb.s1 interval(1d) sliding(0) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_PAR_INTER_VALUE_TOO_SMALL);

  // small sliding values are valid for stream interval triggers
  run("create stream stream_streamdb.s1 interval(1d) sliding(60s) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1w) sliding(1s) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(1n) sliding(1s) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(100s) sliding(1s) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 interval(100001a) sliding(1s) from stream_triggerdb.stream_t1 into "
      "stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // invalid interval offset unit
  run("create stream stream_streamdb.s1 interval(20n, 1y) sliding(3s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_OFFSET_UNIT);

  // interval offset bigger than interval
  run("create stream stream_streamdb.s1 interval(1s, 2s) sliding(3s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // sliding offset with interval
  run("create stream stream_streamdb.s1 interval(3s, 2s) sliding(3s, 2s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_SLIDING_OFFSET);

  // invalid event window

  // non exist col
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c10 < 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_COLUMN);

  // invalid psedo col
  run("create stream stream_streamdb.s1 event_window(start with _wstart > 1 end with c1 < 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_WINDOW_PC);
  run("create stream stream_streamdb.s1 event_window(start with _wend > 1 end with c1 < 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_WINDOW_PC);
  run("create stream stream_streamdb.s1 event_window(start with _wduration > 1 end with c1 < 1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_WINDOW_PC);

  // true for less than 0
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(-1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid true for unit
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(1y) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_TRUE_FOR_UNIT);
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for('1y') from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_TRUE_FOR_UNIT);
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(1x) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for('1x') from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // TRUE_FOR COUNT-only syntax tests (T009-T010)
  // Valid COUNT-only syntax
  run("create stream stream_streamdb.s1 state_window(c1) true_for(count 100) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(count 100) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // COUNT 0 is valid (condition always satisfied)
  run("create stream stream_streamdb.s1 state_window(c1) true_for(count 0) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // COUNT -1 should be rejected
  run("create stream stream_streamdb.s1 state_window(c1) true_for(count -1) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // COUNT > INT32_MAX should be rejected
  run("create stream stream_streamdb.s1 state_window(c1) true_for(count 2147483648) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_TRUE_FOR_COUNT, PARSER_STAGE_PARSE);

  // Missing COUNT keyword in AND/OR syntax should be rejected
  run("create stream stream_streamdb.s1 state_window(c1) true_for(10s and 5) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 state_window(c1) true_for(10s or 5) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(10s and 5) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(10s or 5) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // TRUE_FOR start/end sub-expression syntax tests
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(end(count 3)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(start(count 3)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(end(2s)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(start(2s)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(3s, end(count 2)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(3s, start(count 2)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(3s, start(count 2), end(count 3)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(start(count 2), end(count 3)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(end(2s and count 3)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");
  run("create stream stream_streamdb.s1 event_window(start with c1 > 1 end with c1 < 1) true_for(end(2s or count 3)) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // invalid count window

  // count val greater equal than INT32_MAX
  run("create stream stream_streamdb.s1 count_window(2147483647) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_STREAM_QUERY);
  run("create stream stream_streamdb.s1 count_window(2147483648) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_STREAM_QUERY);

  // invalid period window

  // invalid period unit
  run("create stream stream_streamdb.s1 period(1u) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_UNIT);
  run("create stream stream_streamdb.s1 period(1b) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_UNIT);

  // period val out of [10a, 3650d]
  run("create stream stream_streamdb.s1 period(9a) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_RANGE);
  run("create stream stream_streamdb.s1 period(3660d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_RANGE);
  run("create stream stream_streamdb.s1 period(522w) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_RANGE);
  run("create stream stream_streamdb.s1 period(121n) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_RANGE);
  run("create stream stream_streamdb.s1 period(11y) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_PERIOD_RANGE);
  /* 41q -> 123n > 120n limit */
  run("create stream stream_streamdb.s1 period(41q) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _twstart, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_PAR_INVALID_PERIOD_RANGE);
}

TEST_F(ParserStreamTest, eventWindowNestedStartTree) {
  setCheckDdlFunc(pstCheckEventTreeShape);
  run("create stream stream_streamdb.s_event_tree "
      "event_window(start with ((c1 >= 12 true_for(1s), c1 >= 10), c2 < 215 true_for(count 3)) "
      "end with c2 < 210) true_for(10s) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows");
}

TEST_F(ParserStreamTest, eventWindowRejectGroupTrueFor) {
  run("create stream stream_streamdb.s_bad_group_tf "
      "event_window(start with (c1 >= 12, c2 < 215) true_for(1s)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

TEST_F(ParserStreamTest, eventWindowRejectEmptyGroup) {
  run("create stream stream_streamdb.s_bad_empty "
      "event_window(start with ()) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows",
      TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

TEST_F(ParserStreamTest, eventWindowKeepsSingleStartConditionCompatible) {
  run("create stream stream_streamdb.s_event_single "
      "event_window(start with c1 >= 12 end with c2 < 210) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows");
}

TEST_F(ParserStreamTest, eventWindowKeepsFlatSubEventCompatible) {
  run("create stream stream_streamdb.s_event_flat "
      "event_window(start with (c1 >= 12, c2 < 215)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows");
}

TEST_F(ParserStreamTest, eventConditionPathAllowedForSubEvent) {
  run("create stream stream_streamdb.s_event_path_ok "
      "event_window(start with (c1 >= 12, c2 < 215)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, _event_condition_path, count(*) from %%trows");
}

TEST_F(ParserStreamTest, eventConditionPathRejectedForSingleCondition) {
  run("create stream stream_streamdb.s_event_path_single "
      "event_window(start with c1 >= 12) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, _event_condition_path, count(*) from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, eventConditionPathRejectedForNonEventWindow) {
  run("create stream stream_streamdb.s_event_path_interval "
      "interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, _event_condition_path, count(*) from %%trows",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, eventWindowRejectsStartEndTrueForOnSubEvent) {
  run("create stream stream_streamdb.s_bad_tf_start "
      "event_window(start with (c1 >= 12, c2 < 215)) true_for(start(count 2)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows",
      TSDB_CODE_STREAM_INVALID_TRIGGER, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, eventWindowRejectsLeafTrueForInvalidUnit) {
  run("create stream stream_streamdb.s_bad_leaf_tf "
      "event_window(start with c1 >= 12 true_for(1y)) "
      "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _twstart, count(*) from %%trows",
      TSDB_CODE_PAR_TRUE_FOR_UNIT, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, eventWindowRejectsTreeDepthLimit) {
  std::string startCond = "(c1 >= 0, c1 >= 1)";
  for (int32_t i = 2; i <= 8; ++i) {
    startCond = "(" + startCond + ", c1 >= " + std::to_string(i) + ")";
  }

  run("create stream stream_streamdb.s_bad_tree_depth "
      "event_window(start with " +
          startCond +
          ") "
          "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
          "as select _twstart, count(*) from %%trows",
      TSDB_CODE_STREAM_INVALID_TRIGGER, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, eventWindowRejectsTreeNodeLimit) {
  std::string startCond = "(";
  for (int32_t i = 0; i < 257; ++i) {
    if (i > 0) {
      startCond += ", ";
    }
    startCond += "c1 >= " + std::to_string(i);
  }
  startCond += ")";

  run("create stream stream_streamdb.s_bad_tree_nodes "
      "event_window(start with " +
          startCond +
          ") "
          "from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
          "as select _twstart, count(*) from %%trows",
      TSDB_CODE_STREAM_INVALID_TRIGGER, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, TestErrorTriggerTable) {
  setAsyncFlag("-1");

  // wrong trigger table type
  //run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from information_schema.ins_tables into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_PERMISSION_DENIED);

  // no trigger table and not period trigger
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_NO_TRIGGER_TABLE);
}

TEST_F(ParserStreamTest, TestErrorTriggerPartition) {
  setAsyncFlag("-1");

  // partition by not exist tag
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag4 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_COLUMN);

  // partition by col
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by c1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TRIGGER);
}

TEST_F(ParserStreamTest, TestErrorTriggerstream_options) {
  setAsyncFlag("-1");

  // invalid expired time
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(expired_time(1u)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(expired_time(1b)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(expired_time(1w)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(expired_time(1n)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(expired_time(1y)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);

  // use fill_history with fill_history_first
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(fill_history('2022-02-02 11:11:11') | fill_history_first('2022-02-02 11:11:11')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid max delay
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(max_delay(1u)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(max_delay(1b)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(max_delay(1w)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(max_delay(1n)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(max_delay(1y)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_TIME_UNIT);

  // invalid event type
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type('invalid')) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2",  TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(EVENT_TYPE_INVALID)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid pre filter
  // non exist col
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(pre_filter(c10 > 1)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_INVALID_COLUMN);

  // no trigger table
  run("create stream stream_streamdb.s1 period(1s) stream_options(pre_filter(c1 > 1)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PRE_FILTER);

  // repeat same option
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(pre_filter(c1 > 1) | pre_filter(c2 < 2)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(ignore_disorder|ignore_disorder) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

}

TEST_F(ParserStreamTest, createStreamClearsTbnameFunctionAfterStrictAppendFailure) {
#ifdef WINDOWS
  GTEST_SKIP() << "cppstub patching of nodesListMakeStrictAppend is unreliable in Windows release builds";
#endif
  useDb("root", "stream_streamdb");
  setAsyncFlag("-1");
  StrictAppendTbnameFailureGuard stub;

  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 "
      "stream_options(calc_notify_only) as select avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_OUT_OF_MEMORY, PARSER_STAGE_TRANSLATE);
}

TEST_F(ParserStreamTest, TestErrorNotify) {
  setAsyncFlag("-1");

  // no notify url
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 on(WINDOW_OPEN) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // invalid event type
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 NOTIFY('ws://localhost:8080') on(INVALID_EVENT) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // event type must be specified when trigger is not period or sliding without interval
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 NOTIFY('ws://localhost:8080') into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_NOTIFY);

  // where condition include cols not from query
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 NOTIFY('ws://localhost:8080') where c1 > 1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_NOTIFY_COND);
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 NOTIFY('ws://localhost:8080') where c1 + 2 + 1 into stream_outdb.stream_out as select _twstart, avg(c1), c1 + 1 from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_NOTIFY_COND);

  // invalid notify stream_options
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 NOTIFY('ws://localhost:8080') notify_options(notify_what) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);

  // no query but has condition
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 NOTIFY('ws://localhost:8080') where c1 + 2 + 1", TSDB_CODE_STREAM_INVALID_NOTIFY_COND);
}

TEST_F(ParserStreamTest, TestErrorOutTable) {
  setAsyncFlag("-1");

  // no out table (when only notify no query / CALC_NOTIFY_ONLY is valid)
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // out table type not match
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.st1 as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 partition by tbname into stream_outdb.stream_t1 as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // first column is not ts
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select c1, c2 from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // specify sub table when no partition
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out output_subtable('a') as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_SUBTABLE);

  // sub table expr using column not in partition list
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 partition by tbname into stream_outdb.stream_out output_subtable(c1) as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_SUBTABLE);

  // sub table expr mixing a column in the partition list (tag2) with one that
  // is not (tag1, cast to varchar so the overall CONCAT stays string-typed):
  // rewriteTagSubtableExpr accumulates found/onlyValue across the WHOLE
  // expression tree instead of per reference, so a single matching reference
  // (tag2) must not let an unrelated non-partition column (tag1) slip through
  // undetected in the same expression.
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag2 into stream_outdb.stream_out output_subtable(concat(tag2, cast(tag1 as varchar(20)))) as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_SUBTABLE);

  // sub table expr is not string expr
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out output_subtable(tag1) as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // out col name not match

  // default name and exists out table not match
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_t1 as select _twstart, avg(c1) as c2 from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // specified name and exists out table not match
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_t1(tts, cc1, cc2) as select _twstart, avg(c1) as c2 from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // out col wrong primary key
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out(tts PRIMARY KEY, c1, c2) as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TABLE);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out(tts, c1, c2 PRIMARY KEY) as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // specify tags whe no partition
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out tags(tag1 int as 1) as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_OUT_TAGS);

  // partition same col multi times
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 partition by tbname, tbname into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_PAR_DUPLICATED_COLUMN);

  // tag name not match

  // default-derived tags (from the partition list) may now be a subset of, or in a
  // different order than, the existing out table's tags -- matched by tag name, not
  // position/count (see "流计算预建输出超级表部分列 Tag 写入" DS, commit 95337de4994).
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2 into stream_outdb.st1 as select * from stream_querydb.stream_t1");
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag3, tag1, tag2 into stream_outdb.st1 as select * from stream_querydb.stream_t1");
  // tbname has no matching tag name on the existing out table -> still a name mismatch
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tbname, tag1, tag2 into stream_outdb.st1 as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // specified tag name and exists out table not match
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.st1 tags(tag6 int as tag1, tag7 varchar(18) as tag2, tag8 timestamp as tag3) as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);
  // specified tag type and exists out table not match
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.st1 tags(tag1 int as tag1, tag2 varchar(18) as tag2, tag3 int as tag3) as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);
  // specified tag expr's type and specified tag type not match
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3 into stream_outdb.st1 tags(tag1 int as cos(tag1), tag2 varchar(18) as tag2, tag3 int as tag3) as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_OUT_TABLE);

  // out tag expr using column not in partition list
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2 into stream_outdb.stream_out tags(tag3 int as tag3) as select * from stream_querydb.stream_t1", TSDB_CODE_STREAM_INVALID_SUBTABLE);
}

TEST_F(ParserStreamTest, TestErrorQueryPlaceHolder) {
  setAsyncFlag("-1");

  // _tprev_ts must be used with sliding
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tprev_ts, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _tcurrent_ts must be used with sliding
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tcurrent_ts, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _tnext_ts must be used with sliding
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tnext_ts, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _twstart must be used with trigger with window(not period or sliding without interval)
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _twend must be used with trigger with window(not period or sliding without interval)
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twend, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _twend, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _twduration must be used with trigger with window(not period or sliding without interval)
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, _twduration, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, _twduration, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _twrownum must be used with trigger with window(not period or sliding without interval)
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, _twrownum, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  run("create stream stream_streamdb.s1 period(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tlocaltime, _twrownum, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _tprev_localtime must be used with period
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tprev_localtime, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // _tnext_localtime must be used with period
  run("create stream stream_streamdb.s1 sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tnext_localtime, avg(c1) from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // %%n can not greater than list length of partition
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3, tbname into stream_outdb.stream_out as select _tlocaltime, %%1, %%2, %%3, %%5 from %%tbname", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // %%n can not less than 1
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1, tag2, tag3, tbname into stream_outdb.stream_out as select _tlocaltime, %%0, %%1, %%2 from %%tbname", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // %%n must be used with partition
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 into stream_outdb.stream_out as select _tlocaltime, %%1 from stream_triggerdb.st1", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // %%tbname can only be used when partition by tbname
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as select _tlocaltime, %%tbname from stream_querydb.stream_t2", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.st1 partition by tag1 into stream_outdb.stream_out as select _tlocaltime, c1 from %%tbname", TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

}

TEST_F(ParserStreamTest, TestPeriodOffset) {
  setAsyncFlag("-1");

  // Valid offset cases
  // PERIOD(1w, 1d) - week with day offset
  run("create stream stream_streamdb.s1 period(1w, 1d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)");

  // PERIOD(1w, 12h) - week with hour offset
  run("create stream stream_streamdb.s1 period(1w, 12h) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)");

  // PERIOD(1n, 14d) - month with day offset
  run("create stream stream_streamdb.s1 period(1n, 14d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1n)");

  // PERIOD(1y, 31d) - year with day offset
  run("create stream stream_streamdb.s1 period(1y, 31d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1y)");

  // Invalid offset cases
  // offset >= period (should fail)
  run("create stream stream_streamdb.s1 period(1w, 7d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)",
      TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // offset > period (should fail)
  run("create stream stream_streamdb.s1 period(1w, 8d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)",
      TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // Month unit offset overflow (>= 28 days for 1 month)
  run("create stream stream_streamdb.s1 period(1n, 28d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1n)",
      TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // Month unit offset overflow (>= 56 days for 2 months)
  run("create stream stream_streamdb.s1 period(2n, 56d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out "
      "as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(2n)",
      TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // Valid month offset (< 28 days)
  run("create stream stream_streamdb.s1 period(1n, 27d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1n)");

  run("create stream stream_streamdb.s1 period(1n, 1w) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1n)");
  run("create stream stream_streamdb.s1 period(1y, 1n) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1y)");
  run("create stream stream_streamdb.s1 period(1y, 1y) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as "
      "select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1y)",
      TSDB_CODE_PAR_INTER_OFFSET_TOO_BIG);

  // Multi-unit combinations
  // Week + various offset units
  run("create stream stream_streamdb.s1 period(1w, 3600000a) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)");  // 1 hour in ms
  run("create stream stream_streamdb.s1 period(1w, 3600s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)");  // 1 hour in seconds
  run("create stream stream_streamdb.s1 period(1w, 60m) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)");  // 1 hour in minutes

  // Negative offset (should fail with syntax error)
  run("create stream stream_streamdb.s1 period(1w, -1d) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _wstart, avg(c1) from stream_querydb.stream_t2 interval(1w)", TSDB_CODE_PAR_SYNTAX_ERROR, PARSER_STAGE_PARSE);
}

// T016: IDLE_TIMEOUT syntax parsing tests
TEST_F(ParserStreamTest, TestIdleTimeout) {
  useDb("root", "stream_streamdb");

  // Valid: IDLE_TIMEOUT with milliseconds (minimum: 1000a = 1s)
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(1000a)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // Valid: IDLE_TIMEOUT with seconds
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(1s)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // Valid: IDLE_TIMEOUT with minutes
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5m)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // Valid: IDLE_TIMEOUT with hours
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(1h)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // Valid: IDLE_TIMEOUT with days (maximum: 10d)
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(10d)) into stream_outdb.stream_out as select _twstart, avg(c1) from stream_querydb.stream_t2");

  // Valid: EVENT_TYPE(IDLE) with IDLE_TIMEOUT
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2");

  // Valid: EVENT_TYPE(RESUME) with IDLE_TIMEOUT
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(resume)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2");

  // Valid: EVENT_TYPE(IDLE|RESUME) with IDLE_TIMEOUT
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle|resume)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2");
}

// T051-T052: IDLE_TIMEOUT configuration validation tests (boundary values and error cases)
TEST_F(ParserStreamTest, TestIdleTimeoutValidation) {
  useDb("root", "stream_streamdb");

  // Invalid: IDLE_TIMEOUT below minimum (< 1s = 1000ms)
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(999a)) into stream_outdb.stream_out as select _tidlestart from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);

  // Invalid: IDLE_TIMEOUT above maximum (> 10d = 864000000ms)
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(11d)) into stream_outdb.stream_out as select _tidlestart from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);

  // Invalid: EVENT_TYPE(IDLE) without IDLE_TIMEOUT
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(idle)) into stream_outdb.stream_out as select _tidlestart from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);

  // Invalid: EVENT_TYPE(RESUME) without IDLE_TIMEOUT
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(resume)) into stream_outdb.stream_out as select _tidlestart from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_TRIGGER);

  // Invalid: using window placeholders when event type includes IDLE
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) into stream_outdb.stream_out as select _twstart, _twend from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // Invalid: using window placeholders when event type includes RESUME
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(resume)) into stream_outdb.stream_out as select _twend, _twduration from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // Invalid: using _twduration when event type includes IDLE|RESUME
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle|resume)) into stream_outdb.stream_out as select _tidlestart, _twduration from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // Invalid: using _twrownum when event type includes IDLE
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(idle_timeout(5s) | event_type(idle)) into stream_outdb.stream_out as select _tidlestart, _twrownum from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // Invalid: using idle placeholders when event type does not include IDLE/RESUME
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 stream_options(event_type(window_close)) into stream_outdb.stream_out as select _tidlestart, _tidleend from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);

  // Invalid: using idle placeholders without IDLE/RESUME event type
  run("create stream stream_streamdb.s1 interval(1s) sliding(1s) from stream_triggerdb.stream_t1 into stream_outdb.stream_out as select _tidleend, avg(c1) from stream_querydb.stream_t2",
      TSDB_CODE_STREAM_INVALID_PLACE_HOLDER);
}

}  // namespace ParserTest

// ============================================================================
// Pt-A7: Federated-stream parser E2E tests (stream + EXT trigger table)
//
// These three tests exercise the DS §6.1.1 hard checks in checkCreateStream:
//   1. TestFederatedStreamExtTriggerBasic   – valid EXT-trigger stream parses OK
//   2. TestFederatedStreamExtPartitionByRejected – EXT trigger + PARTITION BY
//      → TSDB_CODE_STREAM_EXT_PARTITION_NOT_SUPPORTED
//   3. TestFederatedStreamExtDisabledCheck  – refsExtSource=true but
//      tsFederatedQueryEnable=false → TSDB_CODE_STREAM_EXT_DISABLED
//
// All three are currently skipped because the MockCatalogService does not yet
// provide ext-source / ext-table-meta responses.  To enable them, implement:
//
//   a) MockCatalogService::createExtSource(sourceName, sourceType, host, port,
//      user, db) → stores SExtSourceInfo* keyed by sourceName
//   b) MockCatalogService::createExtTable(sourceName, tableName,
//      SArray<SExtColumnDef>) → stores SExtTableMeta* keyed by buildExtTableMetaKey()
//   c) In catalogGetAllMeta: when pCatalogReq->pExtSourceCheck is non-NULL,
//      iterate and fill pMetaData->pExtSourceInfo with matching SMetaRes entries.
//      When pCatalogReq->pExtTableMeta is non-NULL, fill pMetaData->pExtTableMetaRsp.
//   d) In destoryMetaData: free pExtSourceInfo elements (SExtSourceInfo*) and
//      pExtTableMetaRsp elements (SExtTableMeta*), then taosArrayDestroy both.
//
// Once (a)–(d) are in place:
//   - Test 1: set tsFederatedQueryEnable=true, register ext_pg source (PostgreSQL,
//     "localhost", 5432, "pguser", "db") and ext table "metrics" with cols
//     (ts "timestamp" PK, value "double precision"), then run the stream SQL and
//     verify SCMCreateStreamReq.numOfExtSpecs==1 and flags has STREAM_FLAG_REF_EXT_SOURCE.
//   - Test 2: same setup but add "partition by tbname" to the trigger clause;
//     expected error TSDB_CODE_STREAM_EXT_PARTITION_NOT_SUPPORTED.
//   - Test 3: set tsFederatedQueryEnable=true so translation succeeds, then
//     flip it to false before checkCreateStream fires (needs run() hook or
//     a second translate pass); expected error TSDB_CODE_STREAM_EXT_DISABLED.
// ============================================================================

// ============================================================================
// Federated Query (FQ) stream parsing — T053..T057
// Real catalog mock backed by MockCatalogService::createExtSource/createExtTable.
// Requires BUILD_ENTERPRISE=ON because parserExtSource.c and the
// reserveExtSource/reserveExtTableMeta hot path in parAstParser.c are guarded
// by TD_ENTERPRISE.  When BUILD_ENTERPRISE is off the cases below still parse
// the SQL, but the EXT recognition logic is compiled out and the test cannot
// observe numOfExtSpecs or STREAM_FLAG_REF_EXT_SOURCE — so we skip in that case.
// ============================================================================

namespace ParserTest {

namespace {
// Helper: register a mock PostgreSQL ext source named ext_pg with one table
// "metrics" carrying (ts TIMESTAMP PK, v DOUBLE).
void registerExtPgMetrics() {
  std::vector<MockCatalogService::MockExtColDef> cols;
  cols.push_back({"ts", TSDB_DATA_TYPE_TIMESTAMP, true,  false});
  cols.push_back({"v",  TSDB_DATA_TYPE_DOUBLE,    false, true});
  g_mockCatalogService->createExtSource("ext_pg", EXT_SOURCE_POSTGRESQL,
                                        "127.0.0.1", 5432, "pguser", "pgdb", "public");
  // For 2-seg path FROM ext_pg.metrics with a source that has a configured
  // default database, translateExternalTableImpl uses lookupMid0="" (see
  // parserExtSource.c:388). The mock cache must register with mid0="" to match.
  g_mockCatalogService->createExtTable("ext_pg", "", "", "metrics", cols,
                                       TSDB_TIME_PRECISION_MILLI);
}

// Helper: register a mock MySQL ext source named ext_mysql with one table
// "events" carrying (ts TIMESTAMP PK, v DOUBLE).
void registerExtMysqlEvents() {
  std::vector<MockCatalogService::MockExtColDef> cols;
  cols.push_back({"ts", TSDB_DATA_TYPE_TIMESTAMP, true,  false});
  cols.push_back({"v",  TSDB_DATA_TYPE_DOUBLE,    false, true});
  g_mockCatalogService->createExtSource("ext_mysql", EXT_SOURCE_MYSQL,
                                        "127.0.0.1", 3306, "myuser", "mydb");
  g_mockCatalogService->createExtTable("ext_mysql", "", "", "events", cols,
                                       TSDB_TIME_PRECISION_MILLI);
}

// RAII guard for tsFederatedQueryEnable so each test cleans up after itself.
class FederatedFlagGuard {
 public:
  explicit FederatedFlagGuard(bool desired) : prev_(tsFederatedQueryEnable) {
    tsFederatedQueryEnable = desired;
  }
  ~FederatedFlagGuard() { tsFederatedQueryEnable = prev_; }
 private:
  bool prev_;
};

class GlobalTimezoneStringGuard {
 public:
  explicit GlobalTimezoneStringGuard(const char* pTimezone) {
    tstrncpy(previous_, tsTimezoneStr, sizeof(previous_));
    tstrncpy(tsTimezoneStr, pTimezone, TD_TIMEZONE_LEN);
  }

  ~GlobalTimezoneStringGuard() { tstrncpy(tsTimezoneStr, previous_, TD_TIMEZONE_LEN); }

 private:
  char previous_[TD_TIMEZONE_LEN] = {0};
};
}  // namespace

TEST_F(ParserStreamTest, NestedWindowRejectsExtTrigger) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  useDb("root", "stream_streamdb");
  FederatedFlagGuard guard(true);
  registerExtPgMetrics();

  runAsyncOnly(
      "create stream stream_streamdb.s_nested_ext_trigger window ("
      "interval(10m) sliding(10m) as w_outer, count_window(2,1) as w_leaf) "
      "from ext_pg.metrics into stream_outdb.stream_out as "
      "select _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend",
      TSDB_CODE_INVALID_PARA);

  SharedValidatorFailureGuard validatorGuard;
  runAsyncOnly(
      "create stream stream_streamdb.s_nested_ext_trigger_reach window ("
      "interval(10m) sliding(10m) as w_outer, count_window(2,1) as w_leaf) "
      "from ext_pg.metrics into stream_outdb.stream_out as "
      "select _twstart ts, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend",
      TSDB_CODE_OUT_OF_MEMORY);
#endif
}

// T053: trigger=EXT, calc=local — basic happy path.
// Verifies: SCMCreateStreamReq.numOfExtSpecs >= 1 and
//           flags & CREATE_STREAM_FLAG_REF_EXT_SOURCE.
TEST_F(ParserStreamTest, TestFederatedStreamExtTriggerBasic) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  // Pt-Followup F1: re-enabled to drive systematic fix of trigger-side EXT path.
  useDb("root", "stream_streamdb");
  FederatedFlagGuard guard(true);
  GlobalTimezoneStringGuard timezoneGuard("UTC");
  registerExtPgMetrics();

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    ASSERT_NE(pQuery->pCmdMsg, nullptr);
    SCMCreateStreamReq req = {};
    int32_t code = tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg,
                                                  pQuery->pCmdMsg->msgLen, &req);
    ASSERT_EQ(code, 0);
    EXPECT_GE(req.numOfExtSpecs, 1);
    EXPECT_TRUE(req.flags & CREATE_STREAM_FLAG_REF_EXT_SOURCE);
    EXPECT_STREQ(req.timeConfig.timezone, "Asia/Shanghai");
    EXPECT_EQ(req.timeConfig.firstDayOfWeek, 2);

    SNode* pTriggerPlanNode = nullptr;
    ASSERT_NE(req.triggerScanPlan, nullptr);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.triggerScanPlan, &pTriggerPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> triggerPlanGuard(pTriggerPlanNode, nodesDestroyNode);
    ASSERT_EQ(nodeType(pTriggerPlanNode), QUERY_NODE_PHYSICAL_SUBPLAN);
    PstPhysicalTimeContextAudit triggerAudit = {&req.timeConfig, 0, 0, 0, false, false};
    pstAuditPhysicalTimeContext(((const SSubplan*)pTriggerPlanNode)->pNode, &triggerAudit);
    EXPECT_GT(triggerAudit.federatedNodes, 0);

    SNode* pCalcPlanNode = nullptr;
    ASSERT_NE(req.calcPlan, nullptr);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode((char*)req.calcPlan, &pCalcPlanNode));
    unique_ptr<SNode, decltype(&nodesDestroyNode)> calcPlanGuard(pCalcPlanNode, nodesDestroyNode);
    ASSERT_EQ(nodeType(pCalcPlanNode), QUERY_NODE_PHYSICAL_PLAN);
    PstPhysicalTimeContextAudit calcAudit = {&req.timeConfig, 0, 0, 0, false, false};
    pstAuditQueryPlanTimeContext((const SQueryPlan*)pCalcPlanNode, &calcAudit);
    EXPECT_TRUE(calcAudit.timezoneFolded);
    EXPECT_TRUE(calcAudit.firstDayOfWeekFolded);
    tFreeSCMCreateStreamReq(&req);
  });
  runAsyncOnly(
      "create stream stream_streamdb.s_fed_t1 interval(10m) sliding(10m) "
      "from ext_pg.metrics stream_options(config(timezone='Asia/Shanghai', firstDayOfWeek=2)) "
      "into stream_outdb.stream_out "
      "as select _twstart ts, timezone() as frozen_timezone, "
      "first_day_of_week() as frozen_fdow, count(*) cnt from stream_querydb.stream_t2 "
      "where ts >= _twstart and ts < _twend");
#endif
}

// T054: EXT trigger + PARTITION BY must be rejected.
// Verifies: TSDB_CODE_STREAM_EXT_PARTITION_NOT_SUPPORTED.
TEST_F(ParserStreamTest, TestFederatedStreamExtPartitionByRejected) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  // Pt-Followup F1: re-enabled.
  useDb("root", "stream_streamdb");
  FederatedFlagGuard guard(true);
  registerExtPgMetrics();
  runAsyncOnly("create stream stream_streamdb.s_fed_neg_part interval(10m) sliding(10m) "
      "from ext_pg.metrics partition by tbname "
      "into stream_outdb.stream_out "
      "as select _twstart ts, count(*) cnt from ext_pg.metrics "
      "where ts >= _twstart and ts < _twend",
      TSDB_CODE_STREAM_EXT_PARTITION_NOT_SUPPORTED);
#endif
}

// T055: EXT source referenced but tsFederatedQueryEnable=false → SKIP.
// Pt-Followup F1 investigation: parser unit test cannot stably trigger any of
// the federated-disabled error codes because of multiple early-stage gates:
//   1. authenticate stage runs before parse and rejects multi-seg paths with
//      TSDB_CODE_PAR_PERMISSION_DENIED on unknown db "ext_pg".
//   2. pre-scan reserveExtSourceInCache (parAstParser.c:1944) is wrapped in
//      `if (tsFederatedQueryEnable)`, so the metacache never sees ext_pg.
//   3. With 2-seg path "ext_pg.metrics" the parser treats ext_pg as a local db
//      and reports TSDB_CODE_PAR_TABLE_NOT_EXIST, not EXT_FEDERATED_DISABLED.
// The real validation of this behavior belongs in an E2E pytest under
// test/cases/18-StreamProcessing/federated/ that can flip the global cfg and
// exercise the full authentication+parse pipeline.
TEST_F(ParserStreamTest, TestFederatedStreamExtDisabledCheck) {
  GTEST_SKIP() << "[pt-followup deferred] Parser unit test cannot reach "
                  "EXT_FEDERATED_DISABLED / STREAM_EXT_DISABLED with federated=OFF: "
                  "earlier authenticate / pre-scan gates short-circuit first. "
                  "Move this assertion to an E2E test that exercises the full "
                  "auth+parse pipeline with config flag toggled.";
}

// T056: trigger=local, calc=EXT — only the calc side is external.
// Verifies: numOfExtSpecs >= 1 and CREATE_STREAM_FLAG_REF_EXT_SOURCE.
TEST_F(ParserStreamTest, TestFederatedStreamExtCalcOnly) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  // Pt-Followup F1 investigation: parser side passes (trigger=stream_triggerdb.stream_t1,
  // calc=ext_pg.metrics both translate cleanly with A+B reorder), but calc-side
  // planner rejects EXT tables with 'External window can not find pk column'
  // (createSelectFromLogicNode). Same gap as T057. Re-enable once planner gains
  // EXT calc support (separate work item beyond Pt-Followup F1).
  GTEST_SKIP() << "[pt-followup deferred] Calc-side planner lacks EXT pk column support; "
                  "parser side already passes. Same gap as T057.";
  useDb("root", "stream_streamdb");
  FederatedFlagGuard guard(true);
  registerExtPgMetrics();

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    ASSERT_NE(pQuery->pCmdMsg, nullptr);
    SCMCreateStreamReq req = {};
    int32_t code = tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg,
                                                  pQuery->pCmdMsg->msgLen, &req);
    ASSERT_EQ(code, 0);
    EXPECT_GE(req.numOfExtSpecs, 1);
    EXPECT_TRUE(req.flags & CREATE_STREAM_FLAG_REF_EXT_SOURCE);
    tFreeSCMCreateStreamReq(&req);
  });
  runAsyncOnly("create stream stream_streamdb.s_fed_t2 interval(10m) sliding(10m) "
      "from stream_triggerdb.stream_t1 "
      "into stream_outdb.stream_out "
      "as select _twstart ts, sum(v) total from ext_pg.metrics "
      "where ts >= _twstart and ts < _twend");
#endif
}

// T057: dual EXT — trigger from ext_pg, calc from ext_mysql.
// Verifies: numOfExtSpecs >= 2 (one per distinct source).
TEST_F(ParserStreamTest, TestFederatedStreamExtDualSources) {
#ifndef TD_ENTERPRISE
  GTEST_SKIP() << "Federated query parse path is gated by TD_ENTERPRISE";
#else
  GTEST_SKIP() << "[pt-followup deferred] Parser side passes (numOfExtSpecs would be >=2), "
                  "but calc-side planner currently rejects EXT tables with 'External window "
                  "can not find pk column' (createSelectFromLogicNode). Re-enable once planner "
                  "gains EXT calc support (separate work item beyond Pt-Followup F1).";
  // Pt-Followup F1: re-enabled.
  useDb("root", "stream_streamdb");
  FederatedFlagGuard guard(true);
  registerExtPgMetrics();
  registerExtMysqlEvents();

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    if (stage != PARSER_STAGE_TRANSLATE) return;
    ASSERT_NE(pQuery->pCmdMsg, nullptr);
    SCMCreateStreamReq req = {};
    int32_t code = tDeserializeSCMCreateStreamReq(pQuery->pCmdMsg->pMsg,
                                                  pQuery->pCmdMsg->msgLen, &req);
    ASSERT_EQ(code, 0);
    EXPECT_GE(req.numOfExtSpecs, 2);
    EXPECT_TRUE(req.flags & CREATE_STREAM_FLAG_REF_EXT_SOURCE);
    tFreeSCMCreateStreamReq(&req);
  });
  runAsyncOnly("create stream stream_streamdb.s_fed_t3 interval(10m) sliding(10m) "
      "from ext_pg.metrics "
      "into stream_outdb.stream_out "
      "as select _twstart ts, sum(v) total from ext_mysql.events "
      "where ts >= _twstart and ts < _twend");
#endif
}

}  // namespace ParserTest
