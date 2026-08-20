/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, AND/or modify
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

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "nodes.h"
#include "os.h"
#include "plannodes.h"
#include "querynodes.h"
#include "tconv.h"

extern "C" void nodesTestSetStrictAppendFailOnce(void);

class NodesCloneTest : public testing::Test {
 public:
  void registerCheckFunc(const std::function<void(const SNode*, const SNode*)>& func) { checkFunc_ = func; }

  void run(const SNode* pSrc) {
    SNode* pNew = NULL;
    int32_t code = nodesCloneNode(pSrc, &pNew);
    std::unique_ptr<SNode, void (*)(SNode*)> pDst(pNew, nodesDestroyNode);
    checkFunc_(pSrc, pDst.get());
  }

 private:
  std::function<void(const SNode*, const SNode*)> checkFunc_;
};

TEST(NodesCodeTest, queryPlanJsonRoundTripPreservesAllSubplanLevels) {
  SNode* pNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN, &pNode));
  std::unique_ptr<SNode, void (*)(SNode*)> planGuard(pNode, nodesDestroyNode);

  SQueryPlan* pPlan = (SQueryPlan*)pNode;
  SSubplan* subplans[3] = {nullptr};
  pPlan->queryId = 1;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pPlan->pSubplans));

  for (int32_t level = 0; level < 2; ++level) {
    SNode* pLevelNode = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_NODE_LIST, &pLevelNode));
    SNodeListNode* pLevel = (SNodeListNode*)pLevelNode;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pLevel->pNodeList));

    const int32_t subplanNum = (0 == level) ? 1 : 2;
    for (int32_t i = 0; i < subplanNum; ++i) {
      SNode* pSubplanNode = nullptr;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_SUBPLAN, &pSubplanNode));
      SSubplan* pSubplan = (SSubplan*)pSubplanNode;
      pSubplan->id.queryId = pPlan->queryId;
      pSubplan->id.groupId = 1;
      pSubplan->id.subplanId = pPlan->numOfSubplans + 1;
      pSubplan->level = level;
      subplans[pPlan->numOfSubplans] = pSubplan;
      ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListStrictAppend(pLevel->pNodeList, pSubplanNode));
      ++pPlan->numOfSubplans;
    }

    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListStrictAppend(pPlan->pSubplans, pLevelNode));
  }

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[0]->pChildren, (SNode*)subplans[1]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[0]->pChildren, (SNode*)subplans[2]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[1]->pParents, (SNode*)subplans[0]));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&subplans[2]->pParents, (SNode*)subplans[0]));

  char* pJson = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString((SNode*)pPlan, false, &pJson, nullptr));
  ASSERT_NE(nullptr, pJson);

  SNode* pRoundTripNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(pJson, &pRoundTripNode));
  taosMemoryFreeClear(pJson);
  std::unique_ptr<SNode, void (*)(SNode*)> roundTripGuard(pRoundTripNode, nodesDestroyNode);

  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN, nodeType(pRoundTripNode));
  SQueryPlan* pRoundTrip = (SQueryPlan*)pRoundTripNode;
  ASSERT_EQ(3, pRoundTrip->numOfSubplans);
  ASSERT_EQ(2, LIST_LENGTH(pRoundTrip->pSubplans));
  ASSERT_EQ(1, LIST_LENGTH(((SNodeListNode*)nodesListGetNode(pRoundTrip->pSubplans, 0))->pNodeList));
  ASSERT_EQ(2, LIST_LENGTH(((SNodeListNode*)nodesListGetNode(pRoundTrip->pSubplans, 1))->pNodeList));

  SSubplan* pRoundTripParent = (SSubplan*)nodesListGetNode(
      ((SNodeListNode*)nodesListGetNode(pRoundTrip->pSubplans, 0))->pNodeList, 0);
  SSubplan* pRoundTripChild1 = (SSubplan*)nodesListGetNode(
      ((SNodeListNode*)nodesListGetNode(pRoundTrip->pSubplans, 1))->pNodeList, 0);
  SSubplan* pRoundTripChild2 = (SSubplan*)nodesListGetNode(
      ((SNodeListNode*)nodesListGetNode(pRoundTrip->pSubplans, 1))->pNodeList, 1);
  ASSERT_EQ((SNode*)pRoundTripChild1, nodesListGetNode(pRoundTripParent->pChildren, 0));
  ASSERT_EQ((SNode*)pRoundTripChild2, nodesListGetNode(pRoundTripParent->pChildren, 1));
  ASSERT_EQ((SNode*)pRoundTripParent, nodesListGetNode(pRoundTripChild1->pParents, 0));
  ASSERT_EQ((SNode*)pRoundTripParent, nodesListGetNode(pRoundTripChild2->pParents, 0));
}

TEST_F(NodesCloneTest, tempTable) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    STempTableNode* pSrcNode = (STempTableNode*)pSrc;
    STempTableNode* pDstNode = (STempTableNode*)pDst;
    ASSERT_EQ(nodeType(pSrcNode->pSubquery), nodeType(pDstNode->pSubquery));
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_TEMP_TABLE, &pNew);
    srcNode.reset(pNew);
    STempTableNode* pNode = (STempTableNode*)srcNode.get();
    pNew = NULL;
    code = nodesMakeNode(QUERY_NODE_SELECT_STMT, &pNew);
    pNode->pSubquery = pNew;
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, joinTable) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SJoinTableNode* pSrcNode = (SJoinTableNode*)pSrc;
    SJoinTableNode* pDstNode = (SJoinTableNode*)pDst;
    ASSERT_EQ(pSrcNode->joinType, pDstNode->joinType);
    ASSERT_EQ(nodeType(pSrcNode->pLeft), nodeType(pDstNode->pLeft));
    ASSERT_EQ(nodeType(pSrcNode->pRight), nodeType(pDstNode->pRight));
    if (NULL != pSrcNode->pOnCond) {
      ASSERT_EQ(nodeType(pSrcNode->pOnCond), nodeType(pDstNode->pOnCond));
    }
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_JOIN_TABLE, &pNew);
    srcNode.reset(pNew);
    SJoinTableNode* pNode = (SJoinTableNode*)srcNode.get();
    pNode->joinType = JOIN_TYPE_INNER;
    code = nodesMakeNode(QUERY_NODE_REAL_TABLE, &pNode->pLeft);
    code = nodesMakeNode(QUERY_NODE_REAL_TABLE, &pNode->pRight);
    code = nodesMakeNode(QUERY_NODE_OPERATOR, &pNode->pOnCond);
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, stateWindow) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SStateWindowNode* pSrcNode = (SStateWindowNode*)pSrc;
    SStateWindowNode* pDstNode = (SStateWindowNode*)pDst;
    ASSERT_EQ(nodeType(pSrcNode->pCol), nodeType(pDstNode->pCol));
    ASSERT_EQ(LIST_LENGTH(pSrcNode->pExprList), LIST_LENGTH(pDstNode->pExprList));
    ASSERT_EQ(nodeType(nodesListGetNode(pSrcNode->pExprList, 0)), nodeType(nodesListGetNode(pDstNode->pExprList, 0)));
    ASSERT_EQ(nodeType(pSrcNode->pTrueForLimit), nodeType(pDstNode->pTrueForLimit));
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_STATE_WINDOW, &pNew);
    srcNode.reset(pNew);
    SStateWindowNode* pNode = (SStateWindowNode*)srcNode.get();
    code = nodesMakeNode(QUERY_NODE_COLUMN, &pNode->pCol);
    SNode* pExpr = NULL;
    code = nodesMakeNode(QUERY_NODE_OPERATOR, &pExpr);
    code = nodesListMakeAppend(&pNode->pExprList, pExpr);
    code = nodesMakeNode(QUERY_NODE_VALUE, &pNode->pTrueForLimit);
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, eventStartLeaf) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), QUERY_NODE_EVENT_START_LEAF);
    ASSERT_EQ(nodeType(pDst), QUERY_NODE_EVENT_START_LEAF);
    const SEventStartLeafNode* pSrcLeaf = (const SEventStartLeafNode*)pSrc;
    const SEventStartLeafNode* pDstLeaf = (const SEventStartLeafNode*)pDst;
    ASSERT_NE(pDstLeaf->pCond, nullptr);
    ASSERT_NE(pDstLeaf->pTrueFor, nullptr);
    EXPECT_NE(pDstLeaf->pCond, pSrcLeaf->pCond);
    EXPECT_NE(pDstLeaf->pTrueFor, pSrcLeaf->pTrueFor);
    EXPECT_EQ(nodeType(pDstLeaf->pCond), nodeType(pSrcLeaf->pCond));
    EXPECT_EQ(nodeType(pDstLeaf->pTrueFor), nodeType(pSrcLeaf->pTrueFor));
    EXPECT_EQ(((const STrueForNode*)pDstLeaf->pTrueFor)->count, 3);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = nullptr;
    int32_t code = nodesMakeNode(QUERY_NODE_EVENT_START_LEAF, &pNew);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    srcNode.reset(pNew);
    SEventStartLeafNode* pLeaf = (SEventStartLeafNode*)srcNode.get();
    code = nodesMakeNode(QUERY_NODE_OPERATOR, &pLeaf->pCond);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    code = nodesMakeNode(QUERY_NODE_TRUE_FOR, &pLeaf->pTrueFor);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    ((STrueForNode*)pLeaf->pTrueFor)->trueForType = TRUE_FOR_COUNT_ONLY;
    ((STrueForNode*)pLeaf->pTrueFor)->count = 3;
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, eventStartLeafJsonRoundTripWithTrueFor) {
  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);
  std::unique_ptr<SNode, void (*)(SNode*)> dstNode(nullptr, nodesDestroyNode);

  SNode*  pNew = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_EVENT_START_LEAF, &pNew);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  srcNode.reset(pNew);
  SEventStartLeafNode* pLeaf = (SEventStartLeafNode*)srcNode.get();
  code = nodesMakeNode(QUERY_NODE_OPERATOR, &pLeaf->pCond);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesMakeNode(QUERY_NODE_TRUE_FOR, &pLeaf->pTrueFor);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ((STrueForNode*)pLeaf->pTrueFor)->trueForType = TRUE_FOR_COUNT_ONLY;
  ((STrueForNode*)pLeaf->pTrueFor)->count = 3;

  char*   pStr = nullptr;
  int32_t len = 0;
  code = nodesNodeToString(srcNode.get(), false, &pStr, &len);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pStr, nullptr);
  ASSERT_GT(len, 0);

  SNode* pRoundTrip = nullptr;
  code = nodesStringToNode(pStr, &pRoundTrip);
  taosMemoryFreeClear(pStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  dstNode.reset(pRoundTrip);

  ASSERT_EQ(nodeType(dstNode.get()), QUERY_NODE_EVENT_START_LEAF);
  const SEventStartLeafNode* pDstLeaf = (const SEventStartLeafNode*)dstNode.get();
  ASSERT_NE(pDstLeaf->pCond, nullptr);
  ASSERT_NE(pDstLeaf->pTrueFor, nullptr);
  EXPECT_EQ(nodeType(pDstLeaf->pCond), QUERY_NODE_OPERATOR);
  EXPECT_EQ(nodeType(pDstLeaf->pTrueFor), QUERY_NODE_TRUE_FOR);
  EXPECT_EQ(((const STrueForNode*)pDstLeaf->pTrueFor)->trueForType, TRUE_FOR_COUNT_ONLY);
  EXPECT_EQ(((const STrueForNode*)pDstLeaf->pTrueFor)->count, 3);
}

TEST_F(NodesCloneTest, eventStartLeafJsonRoundTripWithoutTrueFor) {
  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);
  std::unique_ptr<SNode, void (*)(SNode*)> dstNode(nullptr, nodesDestroyNode);

  SNode*  pNew = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_EVENT_START_LEAF, &pNew);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  srcNode.reset(pNew);
  SEventStartLeafNode* pLeaf = (SEventStartLeafNode*)srcNode.get();
  code = nodesMakeNode(QUERY_NODE_OPERATOR, &pLeaf->pCond);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  char*   pStr = nullptr;
  int32_t len = 0;
  code = nodesNodeToString(srcNode.get(), false, &pStr, &len);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pStr, nullptr);
  ASSERT_GT(len, 0);

  SNode* pRoundTrip = nullptr;
  code = nodesStringToNode(pStr, &pRoundTrip);
  taosMemoryFreeClear(pStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  dstNode.reset(pRoundTrip);

  ASSERT_EQ(nodeType(dstNode.get()), QUERY_NODE_EVENT_START_LEAF);
  const SEventStartLeafNode* pDstLeaf = (const SEventStartLeafNode*)dstNode.get();
  ASSERT_NE(pDstLeaf->pCond, nullptr);
  EXPECT_EQ(pDstLeaf->pTrueFor, nullptr);
  EXPECT_EQ(nodeType(pDstLeaf->pCond), QUERY_NODE_OPERATOR);
}

TEST_F(NodesCloneTest, sessionWindow) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SSessionWindowNode* pSrcNode = (SSessionWindowNode*)pSrc;
    SSessionWindowNode* pDstNode = (SSessionWindowNode*)pDst;
    ASSERT_EQ(nodeType(pSrcNode->pCol), nodeType(pDstNode->pCol));
    ASSERT_EQ(nodeType(pSrcNode->pGap), nodeType(pDstNode->pGap));
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_SESSION_WINDOW, &pNew);
    srcNode.reset(pNew);
    SSessionWindowNode* pNode = (SSessionWindowNode*)srcNode.get();
    code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pNode->pCol);
    code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pNode->pGap);
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, intervalWindow) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SIntervalWindowNode* pSrcNode = (SIntervalWindowNode*)pSrc;
    SIntervalWindowNode* pDstNode = (SIntervalWindowNode*)pDst;
    ASSERT_EQ(nodeType(pSrcNode->pInterval), nodeType(pDstNode->pInterval));
    if (NULL != pSrcNode->pOffset) {
      ASSERT_EQ(nodeType(pSrcNode->pOffset), nodeType(pDstNode->pOffset));
    }
    if (NULL != pSrcNode->pSliding) {
      ASSERT_EQ(nodeType(pSrcNode->pSliding), nodeType(pDstNode->pSliding));
    }
    if (NULL != pSrcNode->pFill) {
      ASSERT_EQ(nodeType(pSrcNode->pFill), nodeType(pDstNode->pFill));
    }
    ASSERT_EQ(pSrcNode->timeRange.skey, pDstNode->timeRange.skey);
    ASSERT_EQ(pSrcNode->timeRange.ekey, pDstNode->timeRange.ekey);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
      SNode* pNew = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW, &pNew);
    srcNode.reset(pNew);
    SIntervalWindowNode* pNode = (SIntervalWindowNode*)srcNode.get();
    code =  nodesMakeNode(QUERY_NODE_VALUE, &pNode->pInterval);
    code = nodesMakeNode(QUERY_NODE_VALUE, &pNode->pOffset);
    code = nodesMakeNode(QUERY_NODE_VALUE, &pNode->pSliding);
    code = nodesMakeNode(QUERY_NODE_FILL, &pNode->pFill);
    pNode->timeRange.skey = 1666756692907;
    pNode->timeRange.ekey = 1666756699907;
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, fill) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SFillNode* pSrcNode = (SFillNode*)pSrc;
    SFillNode* pDstNode = (SFillNode*)pDst;
    ASSERT_EQ(pSrcNode->mode, pDstNode->mode);
    if (NULL != pSrcNode->pValues) {
      ASSERT_EQ(nodeType(pSrcNode->pValues), nodeType(pDstNode->pValues));
    }
    ASSERT_EQ(nodeType(pSrcNode->pWStartTs), nodeType(pDstNode->pWStartTs));
    ASSERT_EQ(pSrcNode->timeRange.skey, pDstNode->timeRange.skey);
    ASSERT_EQ(pSrcNode->timeRange.ekey, pDstNode->timeRange.ekey);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_FILL, &pNew);
    srcNode.reset(pNew);
    SFillNode* pNode = (SFillNode*)srcNode.get();
    pNode->mode = FILL_MODE_VALUE;
    code = nodesMakeNode(QUERY_NODE_NODE_LIST, &pNode->pValues);
    code = nodesMakeNode(QUERY_NODE_COLUMN, &pNode->pWStartTs);
    pNode->timeRange.skey = 1666756692907;
    pNode->timeRange.ekey = 1666756699907;
    return srcNode.get();
  }());
}

// The view expansion path clones an untranslated SELECT statement, so the
// clone must carry every field the parser has already filled in. RANGE /
// EVERY / FILL are set while building the AST, dropping them makes an INTERP
// view fail with "Missing RANGE clause, EVERY clause or FILL clause".
TEST_F(NodesCloneTest, selectStmtKeepsInterpClauses) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SSelectStmt* pSrcNode = (SSelectStmt*)pSrc;
    SSelectStmt* pDstNode = (SSelectStmt*)pDst;
    // Non-fatal assertions: every clause has to report on its own, a fatal
    // one would hide the clauses checked after the first missing field.
    EXPECT_NE(pDstNode->pRange, nullptr);
    EXPECT_NE(pDstNode->pRangeAround, nullptr);
    EXPECT_NE(pDstNode->pEvery, nullptr);
    EXPECT_NE(pDstNode->pFill, nullptr);
    if (NULL != pDstNode->pRange) {
      EXPECT_EQ(nodeType(pSrcNode->pRange), nodeType(pDstNode->pRange));
    }
    if (NULL != pDstNode->pRangeAround) {
      EXPECT_EQ(nodeType(pSrcNode->pRangeAround),
                nodeType(pDstNode->pRangeAround));
    }
    if (NULL != pDstNode->pEvery) {
      EXPECT_EQ(nodeType(pSrcNode->pEvery), nodeType(pDstNode->pEvery));
    }
    if (NULL != pDstNode->pFill) {
      EXPECT_EQ(nodeType(pSrcNode->pFill), nodeType(pDstNode->pFill));
    }
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);
  {
    SNode* pNew = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_SELECT_STMT, &pNew));
    srcNode.reset(pNew);
    SSelectStmt* pNode = (SSelectStmt*)srcNode.get();
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_OPERATOR, &pNode->pRange));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_RANGE_AROUND, &pNode->pRangeAround));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_VALUE, &pNode->pEvery));
    ASSERT_EQ(TSDB_CODE_SUCCESS,
              nodesMakeNode(QUERY_NODE_FILL, &pNode->pFill));
  }
  run(srcNode.get());
}

// The parser initializes several fields to something else than the calloc
// default: lastProcessByRowFuncId to -1 ("no process-by-row function seen
// yet"), timeRange to TSWINDOW_INITIALIZER. A clone leaving them at zero
// makes lastProcessByRowFuncId point at builtin function id 0 (count),
// which the parser then rejects as an illegal function combination.
TEST_F(NodesCloneTest, selectStmtKeepsParseTimeDefaults) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SSelectStmt* pSrcNode = (SSelectStmt*)pSrc;
    SSelectStmt* pDstNode = (SSelectStmt*)pDst;
    EXPECT_EQ(pSrcNode->lastProcessByRowFuncId,
              pDstNode->lastProcessByRowFuncId);
    EXPECT_EQ(pSrcNode->onlyHasKeepOrderFunc, pDstNode->onlyHasKeepOrderFunc);
    EXPECT_EQ(pSrcNode->tagScan, pDstNode->tagScan);
    EXPECT_EQ(pSrcNode->timeRange.skey, pDstNode->timeRange.skey);
    EXPECT_EQ(pSrcNode->timeRange.ekey, pDstNode->timeRange.ekey);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);
  {
    SNode* pNew = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_SELECT_STMT, &pNew));
    srcNode.reset(pNew);
    SSelectStmt* pNode = (SSelectStmt*)srcNode.get();
    pNode->lastProcessByRowFuncId = -1;
    pNode->onlyHasKeepOrderFunc = true;
    pNode->tagScan = true;
    // TSWINDOW_INITIALIZER is a C compound literal, which MSVC rejects in
    // C++ (C4576), so spell the same value out here.
    STimeWindow initRange = {INT64_MIN, INT64_MAX};
    TAOS_SET_OBJ_ALIGNED(&pNode->timeRange, initRange);
  }
  run(srcNode.get());
}

TEST_F(NodesCloneTest, logicSubplan) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SLogicSubplan* pSrcNode = (SLogicSubplan*)pSrc;
    SLogicSubplan* pDstNode = (SLogicSubplan*)pDst;
    ASSERT_EQ(pSrcNode->subplanType, pDstNode->subplanType);
    ASSERT_EQ(pSrcNode->level, pDstNode->level);
    ASSERT_EQ(pSrcNode->splitFlag, pDstNode->splitFlag);
    ASSERT_EQ(pSrcNode->numOfComputeNodes, pDstNode->numOfComputeNodes);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN, &pNew);
    srcNode.reset(pNew);
    SLogicSubplan* pNode = (SLogicSubplan*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, physiScan) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    STagScanPhysiNode* pSrcNode = (STagScanPhysiNode*)pSrc;
    STagScanPhysiNode* pDstNode = (STagScanPhysiNode*)pDst;
    ASSERT_EQ(pSrcNode->scan.uid, pDstNode->scan.uid);
    ASSERT_EQ(pSrcNode->scan.suid, pDstNode->scan.suid);
    ASSERT_EQ(pSrcNode->scan.tableType, pDstNode->scan.tableType);
    ASSERT_EQ(pSrcNode->onlyMetaCtbIdx, pDstNode->onlyMetaCtbIdx);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = nullptr;
    int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, &pNew);
    srcNode.reset(pNew);
    STagScanPhysiNode* pNode = (STagScanPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, physiSystemTableScan) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SSystemTableScanPhysiNode* pSrcNode = (SSystemTableScanPhysiNode*)pSrc;
    SSystemTableScanPhysiNode* pDstNode = (SSystemTableScanPhysiNode*)pDst;
    ASSERT_EQ(pSrcNode->showRewrite, pDstNode->showRewrite);
    ASSERT_EQ(pSrcNode->accountId, pDstNode->accountId);
    ASSERT_EQ(pSrcNode->sysInfo, pDstNode->sysInfo);
    // The client charset name must survive clone (plain char[] field copied by
    // physiSysTableScanCopy); without it a cloned/pushed-down plan loses it.
    ASSERT_STREQ(pSrcNode->charsetName, pDstNode->charsetName);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
      SNode* pNew = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, &pNew);
    srcNode.reset(pNew);
    SSystemTableScanPhysiNode* pNode = (SSystemTableScanPhysiNode*)srcNode.get();
    tstrncpy(pNode->charsetName, "GBK", TD_CHARSET_LEN);
    return srcNode.get();
  }());
}

TEST(NodesCollectTest, collectColumnsAppendFailureDoesNotDoubleFree) {
  SNode* pNode = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &pNode));
  std::unique_ptr<SNode, void (*)(SNode*)> nodeGuard(pNode, nodesDestroyNode);

  SColumnNode* pCol = (SColumnNode*)pNode;
  pCol->colType = COLUMN_TYPE_COLUMN;
  snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", "t1");
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", "c1");

  SNodeList* pCols = NULL;
  nodesTestSetStrictAppendFailOnce();
  int32_t code = nodesCollectColumnsFromNode(pNode, "t1", COLLECT_COL_TYPE_ALL, &pCols);

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(pCols, nullptr);
}

TEST_F(NodesCloneTest, physiPartition) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SPartitionPhysiNode* pSrcNode = (SPartitionPhysiNode*)pSrc;
    SPartitionPhysiNode* pDstNode = (SPartitionPhysiNode*)pDst;
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PARTITION, &pNew);
    srcNode.reset(pNew);
    SPartitionPhysiNode* pNode = (SPartitionPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, sqlWindowSpec) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    const SWindowSpecNode* src = (const SWindowSpecNode*)pSrc;
    const SWindowSpecNode* dst = (const SWindowSpecNode*)pDst;
    ASSERT_EQ(LIST_LENGTH(src->pPartitionByList), LIST_LENGTH(dst->pPartitionByList));
    ASSERT_EQ(LIST_LENGTH(src->pOrderByList), LIST_LENGTH(dst->pOrderByList));
    ASSERT_NE(dst->pFrame, nullptr);
    const SWindowFrameNode* srcFrame = (const SWindowFrameNode*)src->pFrame;
    const SWindowFrameNode* dstFrame = (const SWindowFrameNode*)dst->pFrame;
    ASSERT_EQ(srcFrame->frameUnit, dstFrame->frameUnit);
    ASSERT_EQ(srcFrame->start.boundType, dstFrame->start.boundType);
    ASSERT_EQ(srcFrame->end.boundType, dstFrame->end.boundType);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);
  {
    SNode*  pNew = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_SQL_WINDOW_SPEC, &pNew);
    ASSERT_EQ(TSDB_CODE_SUCCESS, code);
    srcNode.reset(pNew);

    SWindowSpecNode* spec = (SWindowSpecNode*)srcNode.get();
    SNode*           partCol = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &partCol));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&spec->pPartitionByList, partCol));

    SOrderByExprNode* order = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&order));
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &order->pExpr));
    order->order = ORDER_ASC;
    order->nullOrder = NULL_ORDER_FIRST;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&spec->pOrderByList, (SNode*)order));

    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_SQL_WINDOW_FRAME, &spec->pFrame));
    SWindowFrameNode* frame = (SWindowFrameNode*)spec->pFrame;
    frame->frameUnit = WINDOW_FRAME_UNIT_ROWS;
    frame->start.boundType = WINDOW_BOUND_N_PRECEDING;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &frame->start.pOffset));
    frame->end.boundType = WINDOW_BOUND_CURRENT_ROW;
  }
  run(srcNode.get());
}

TEST_F(NodesCloneTest, functionWithOverSpec) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    const SFunctionNode* src = (const SFunctionNode*)pSrc;
    const SFunctionNode* dst = (const SFunctionNode*)pDst;
    ASSERT_NE(src->pOver, nullptr);
    ASSERT_NE(dst->pOver, nullptr);
    ASSERT_EQ(nodeType(src->pOver), nodeType(dst->pOver));
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);
  {
    SNode* pNew = NULL;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_FUNCTION, &pNew));
    srcNode.reset(pNew);
    SFunctionNode* func = (SFunctionNode*)srcNode.get();
    tstrncpy(func->functionName, "avg", TSDB_FUNC_NAME_LEN);
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_SQL_WINDOW_SPEC, &func->pOver));
  }
  run(srcNode.get());
}

// ---- per-node charset propagation (feat/charset) ----
//
// value/operator/function/case-when nodes carry a charsetCxt stamped at parse
// time; it drives NCHAR<->MBS conversion for the querying client. It is a bare
// void* into the process-wide conv registry, propagated as a scalar on clone
// and serialized by charset NAME in both codecs (rebuilt via taosConvInit on
// decode). If any path drops it, a remote vnode silently falls back to the
// server charset and a non-UTF-8 client gets wrong results. These tests pin
// clone + the JSON and TLV round-trips for all four bearer types.

namespace {

// The four expression node types that carry charsetCxt.
const ENodeType kCharsetBearingTypes[] = {
    QUERY_NODE_VALUE,
    QUERY_NODE_OPERATOR,
    QUERY_NODE_FUNCTION,
    QUERY_NODE_CASE_WHEN,
};

SNode* makeCharsetStampedNode(ENodeType type, void* charsetCxt) {
  SNode*  pNode = nullptr;
  int32_t code = nodesMakeNode(type, &pNode);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_NE(pNode, nullptr);
  if (pNode == nullptr) return nullptr;
  switch (type) {
    case QUERY_NODE_VALUE: {
      SValueNode* value = (SValueNode*)pNode;
      value->charsetCxt = charsetCxt;
      value->translate = true;
      value->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
      value->node.resType.bytes = VARSTR_HEADER_SIZE + 4;
      value->datum.p = (char*)taosMemoryCalloc(1, value->node.resType.bytes);
      EXPECT_NE(value->datum.p, nullptr);
      if (value->datum.p == nullptr) return pNode;
      varDataSetLen(value->datum.p, 4);
      memcpy(varDataVal(value->datum.p), "\xd6\xd0\xce\xc4", 4);
      value->literal = taosStrdup("\xd6\xd0\xce\xc4");
      EXPECT_NE(value->literal, nullptr);
      break;
    }
    case QUERY_NODE_OPERATOR:
      ((SOperatorNode*)pNode)->charsetCxt = charsetCxt;
      break;
    case QUERY_NODE_FUNCTION:
      ((SFunctionNode*)pNode)->charsetCxt = charsetCxt;
      break;
    case QUERY_NODE_CASE_WHEN:
      ((SCaseWhenNode*)pNode)->charsetCxt = charsetCxt;
      break;
    default:
      break;
  }
  return pNode;
}

void checkRawGbkValuePreserved(const SNode* pNode) {
  const SValueNode* value = (const SValueNode*)pNode;
  ASSERT_EQ(value->node.resType.type, TSDB_DATA_TYPE_VARCHAR);
  ASSERT_NE(value->datum.p, nullptr);
  EXPECT_EQ(varDataLen(value->datum.p), 4);
  EXPECT_EQ(memcmp(varDataVal(value->datum.p), "\xd6\xd0\xce\xc4", 4), 0);
  ASSERT_NE(value->literal, nullptr);
  EXPECT_EQ(memcmp(value->literal, "\xd6\xd0\xce\xc4", 4), 0);
}

const void* getExprCharsetCxt(const SNode* pNode) {
  switch (nodeType(pNode)) {
    case QUERY_NODE_VALUE:
      return ((const SValueNode*)pNode)->charsetCxt;
    case QUERY_NODE_OPERATOR:
      return ((const SOperatorNode*)pNode)->charsetCxt;
    case QUERY_NODE_FUNCTION:
      return ((const SFunctionNode*)pNode)->charsetCxt;
    case QUERY_NODE_CASE_WHEN:
      return ((const SCaseWhenNode*)pNode)->charsetCxt;
    default:
      return nullptr;
  }
}

#pragma pack(push, 1)
struct TestTlvHeader {
  int16_t type;
  int32_t len;
};
#pragma pack(pop)

}  // namespace

TEST(NodesCharsetTest, cloneCarriesCharsetCxt) {
  void* gbk = taosConvInit("GBK");
  ASSERT_NE(gbk, nullptr);

  for (ENodeType type : kCharsetBearingTypes) {
    std::unique_ptr<SNode, void (*)(SNode*)> src(
        makeCharsetStampedNode(type, gbk), nodesDestroyNode);
    ASSERT_NE(src.get(), nullptr);
    ASSERT_EQ(getExprCharsetCxt(src.get()), gbk);

    SNode*  pClone = nullptr;
    int32_t code = nodesCloneNode(src.get(), &pClone);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    std::unique_ptr<SNode, void (*)(SNode*)> dst(pClone, nodesDestroyNode);

    // The fix copies charsetCxt as a scalar field; without it the clone keeps
    // the zero-initialized NULL and loses the connection charset.
    EXPECT_EQ(getExprCharsetCxt(dst.get()), gbk)
        << "charsetCxt not cloned for node type " << type;
    if (type == QUERY_NODE_VALUE) checkRawGbkValuePreserved(dst.get());
  }
}

class NodesCharsetCodecTest : public testing::Test {
 protected:
  void SetUp() override {
    // A dropped charset decodes to NULL, whose effective name is tsCharset;
    // decode also collapses a stamped name equal to tsCharset back to NULL.
    // Pin tsCharset to something other than the stamped charset so a codec
    // regression is observable (NULL -> "UTF-8" != "GBK").
    tstrncpy(savedCharset_, tsCharset, sizeof(savedCharset_));
    tstrncpy(tsCharset, "UTF-8", TD_CHARSET_LEN);
  }
  void TearDown() override {
    tstrncpy(tsCharset, savedCharset_, TD_CHARSET_LEN);
  }

  static void checkCharsetPreserved(const SNode* pOut) {
    const void* outCxt = getExprCharsetCxt(pOut);
    ASSERT_NE(outCxt, nullptr);
    EXPECT_STREQ(((const SConvInfo*)outCxt)->charset, "GBK");
  }

  char savedCharset_[TD_CHARSET_LEN] = {0};
};

TEST_F(NodesCharsetCodecTest, jsonRoundTripPreservesCharset) {
  void* gbk = taosConvInit("GBK");
  ASSERT_NE(gbk, nullptr);
  ASSERT_STREQ(((const SConvInfo*)gbk)->charset, "GBK");

  for (ENodeType type : kCharsetBearingTypes) {
    std::unique_ptr<SNode, void (*)(SNode*)> src(
        makeCharsetStampedNode(type, gbk), nodesDestroyNode);
    ASSERT_NE(src.get(), nullptr);

    char*   pStr = nullptr;
    int32_t len = 0;
    ASSERT_EQ(nodesNodeToString(src.get(), false, &pStr, &len),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(pStr, nullptr);

    SNode*  pOut = nullptr;
    int32_t code = nodesStringToNode(pStr, &pOut);
    taosMemoryFreeClear(pStr);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    std::unique_ptr<SNode, void (*)(SNode*)> out(pOut, nodesDestroyNode);

    SCOPED_TRACE("json node type " + std::to_string((int)type));
    checkCharsetPreserved(out.get());
    if (type == QUERY_NODE_VALUE) checkRawGbkValuePreserved(out.get());
  }
}

TEST_F(NodesCharsetCodecTest, jsonRejectsOverlongCharsetNameWithMessageLengthError) {
  void* gbk = taosConvInit("GBK");
  ASSERT_NE(gbk, nullptr);
  std::unique_ptr<SNode, void (*)(SNode*)> src(
      makeCharsetStampedNode(QUERY_NODE_VALUE, gbk), nodesDestroyNode);

  char*   pStr = nullptr;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToString(src.get(), false, &pStr, &len),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(pStr, nullptr);
  std::string json(pStr);
  taosMemoryFreeClear(pStr);

  const std::string original = "\"CharsetName\":\"GBK\"";
  size_t            pos = json.find(original);
  ASSERT_NE(pos, std::string::npos);
  json.replace(pos, original.size(),
               "\"CharsetName\":\"" + std::string(TD_CHARSET_LEN, 'A') + "\"");

  SNode* pOut = nullptr;
  EXPECT_EQ(nodesStringToNode(json.c_str(), &pOut),
            TSDB_CODE_INVALID_MSG_LEN);
  EXPECT_EQ(pOut, nullptr);
}

TEST_F(NodesCharsetCodecTest, msgRoundTripPreservesCharset) {
  void* gbk = taosConvInit("GBK");
  ASSERT_NE(gbk, nullptr);

  for (ENodeType type : kCharsetBearingTypes) {
    std::unique_ptr<SNode, void (*)(SNode*)> src(
        makeCharsetStampedNode(type, gbk), nodesDestroyNode);
    ASSERT_NE(src.get(), nullptr);

    char*   pMsg = nullptr;
    int32_t len = 0;
    ASSERT_EQ(nodesNodeToMsg(src.get(), &pMsg, &len), TSDB_CODE_SUCCESS);
    ASSERT_NE(pMsg, nullptr);

    SNode*  pOut = nullptr;
    int32_t code = nodesMsgToNode(pMsg, len, &pOut);
    taosMemoryFreeClear(pMsg);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
    std::unique_ptr<SNode, void (*)(SNode*)> out(pOut, nodesDestroyNode);

    SCOPED_TRACE("msg node type " + std::to_string((int)type));
    checkCharsetPreserved(out.get());
    if (type == QUERY_NODE_VALUE) checkRawGbkValuePreserved(out.get());
  }
}

TEST_F(NodesCharsetCodecTest, msgRejectsOverlongCharsetNameWithMessageLengthError) {
  void* gbk = taosConvInit("GBK");
  ASSERT_NE(gbk, nullptr);
  std::unique_ptr<SNode, void (*)(SNode*)> src(
      makeCharsetStampedNode(QUERY_NODE_VALUE, gbk), nodesDestroyNode);

  char*   pMsg = nullptr;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToMsg(src.get(), &pMsg, &len), TSDB_CODE_SUCCESS);
  ASSERT_NE(pMsg, nullptr);

  auto* root = (TestTlvHeader*)pMsg;
  ASSERT_EQ((int32_t)sizeof(TestTlvHeader) + ntohl(root->len), len);
  int32_t fieldOffset = sizeof(TestTlvHeader);
  int32_t lastOffset = -1;
  while (fieldOffset < len) {
    auto* field = (TestTlvHeader*)(pMsg + fieldOffset);
    int32_t fieldLen = ntohl(field->len);
    ASSERT_GE(fieldLen, 0);
    ASSERT_LE(fieldOffset + (int32_t)sizeof(TestTlvHeader) + fieldLen, len);
    lastOffset = fieldOffset;
    fieldOffset += sizeof(TestTlvHeader) + fieldLen;
  }
  ASSERT_EQ(fieldOffset, len);
  ASSERT_GE(lastOffset, 0);

  auto* charsetField = (TestTlvHeader*)(pMsg + lastOffset);
  ASSERT_EQ(ntohl(charsetField->len), 3);
  ASSERT_EQ(memcmp(pMsg + lastOffset + sizeof(TestTlvHeader), "GBK", 3), 0);

  int32_t delta = TD_CHARSET_LEN - 3;
  std::vector<char> malformed(len + delta);
  memcpy(malformed.data(), pMsg, lastOffset + sizeof(TestTlvHeader));
  memset(malformed.data() + lastOffset + sizeof(TestTlvHeader), 'A', TD_CHARSET_LEN);
  taosMemoryFreeClear(pMsg);

  root = (TestTlvHeader*)malformed.data();
  root->len = htonl(ntohl(root->len) + delta);
  charsetField = (TestTlvHeader*)(malformed.data() + lastOffset);
  charsetField->len = htonl(TD_CHARSET_LEN);

  SNode* pOut = nullptr;
  EXPECT_EQ(nodesMsgToNode(malformed.data(), (int32_t)malformed.size(), &pOut),
            TSDB_CODE_INVALID_MSG_LEN);
  EXPECT_EQ(pOut, nullptr);
}

// The system-table scan physical node carries the query charset as a NAME
// (SSystemTableScanPhysiNode.charsetName), serialized directly (not decoded to a
// charsetCxt) in both codecs; the sysscan operator restores it via taosConvInit
// to render NCHAR/JSON tag values in the client charset. These pin the TLV and
// JSON round-trips, including the absent-field (old plan) empty-name fallback.
namespace {

SSystemTableScanPhysiNode* makeSysScanNode(const char* charsetName) {
  SNode*  pNode = nullptr;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, &pNode);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_NE(pNode, nullptr);
  if (pNode == nullptr) return nullptr;
  SSystemTableScanPhysiNode* pScan = (SSystemTableScanPhysiNode*)pNode;
  tstrncpy(pScan->charsetName, charsetName, TD_CHARSET_LEN);
  return pScan;
}

}  // namespace

TEST(NodesSysScanCharsetTest, msgRoundTripPreservesCharsetName) {
  std::unique_ptr<SNode, void (*)(SNode*)> src((SNode*)makeSysScanNode("GBK"),
                                               nodesDestroyNode);
  ASSERT_NE(src.get(), nullptr);

  char*   pMsg = nullptr;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToMsg(src.get(), &pMsg, &len), TSDB_CODE_SUCCESS);
  ASSERT_NE(pMsg, nullptr);

  SNode*  pOut = nullptr;
  int32_t code = nodesMsgToNode(pMsg, len, &pOut);
  taosMemoryFreeClear(pMsg);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  std::unique_ptr<SNode, void (*)(SNode*)> out(pOut, nodesDestroyNode);
  EXPECT_STREQ(((SSystemTableScanPhysiNode*)out.get())->charsetName, "GBK");
}

TEST(NodesSysScanCharsetTest, jsonRoundTripPreservesCharsetName) {
  std::unique_ptr<SNode, void (*)(SNode*)> src((SNode*)makeSysScanNode("GBK"),
                                               nodesDestroyNode);
  ASSERT_NE(src.get(), nullptr);

  char*   pStr = nullptr;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToString(src.get(), false, &pStr, &len), TSDB_CODE_SUCCESS);
  ASSERT_NE(pStr, nullptr);

  SNode*  pOut = nullptr;
  int32_t code = nodesStringToNode(pStr, &pOut);
  taosMemoryFreeClear(pStr);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  std::unique_ptr<SNode, void (*)(SNode*)> out(pOut, nodesDestroyNode);
  EXPECT_STREQ(((SSystemTableScanPhysiNode*)out.get())->charsetName, "GBK");
}

TEST(NodesSysScanCharsetTest, emptyCharsetNameRoundTrips) {
  // Old plans carry no charset (empty name); it must round-trip to empty, which
  // the operator maps to the server default tsCharsetCxt.
  std::unique_ptr<SNode, void (*)(SNode*)> src((SNode*)makeSysScanNode(""),
                                               nodesDestroyNode);
  ASSERT_NE(src.get(), nullptr);

  char*   pMsg = nullptr;
  int32_t len = 0;
  ASSERT_EQ(nodesNodeToMsg(src.get(), &pMsg, &len), TSDB_CODE_SUCCESS);
  ASSERT_NE(pMsg, nullptr);
  SNode*  pOut = nullptr;
  int32_t code = nodesMsgToNode(pMsg, len, &pOut);
  taosMemoryFreeClear(pMsg);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  std::unique_ptr<SNode, void (*)(SNode*)> out(pOut, nodesDestroyNode);
  EXPECT_STREQ(((SSystemTableScanPhysiNode*)out.get())->charsetName, "");
}
