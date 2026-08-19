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

#include "nodes.h"
#include "plannodes.h"
#include "querynodes.h"

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
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
      SNode* pNew = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, &pNew);
    srcNode.reset(pNew);
    SSystemTableScanPhysiNode* pNode = (SSystemTableScanPhysiNode*)srcNode.get();
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
