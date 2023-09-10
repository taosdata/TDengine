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

class NodesCloneTest : public testing::Test {
 public:
  void registerCheckFunc(const std::function<void(const SNode*, const SNode*)>& func) { checkFunc_ = func; }

  void run(const SNode* pSrc) {
    std::unique_ptr<SNode, void (*)(SNode*)> pDst(nodesCloneNode(pSrc), nodesDestroyNode);
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_TEMP_TABLE));
    STempTableNode* pNode = (STempTableNode*)srcNode.get();
    pNode->pSubquery = nodesMakeNode(QUERY_NODE_SELECT_STMT);
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_JOIN_TABLE));
    SJoinTableNode* pNode = (SJoinTableNode*)srcNode.get();
    pNode->joinType = JOIN_TYPE_INNER;
    pNode->pLeft = nodesMakeNode(QUERY_NODE_REAL_TABLE);
    pNode->pRight = nodesMakeNode(QUERY_NODE_REAL_TABLE);
    pNode->pOnCond = nodesMakeNode(QUERY_NODE_OPERATOR);
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, stateWindow) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SStateWindowNode* pSrcNode = (SStateWindowNode*)pSrc;
    SStateWindowNode* pDstNode = (SStateWindowNode*)pDst;
    ASSERT_EQ(nodeType(pSrcNode->pCol), nodeType(pDstNode->pCol));
    ASSERT_EQ(nodeType(pSrcNode->pExpr), nodeType(pDstNode->pExpr));
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    srcNode.reset(nodesMakeNode(QUERY_NODE_STATE_WINDOW));
    SStateWindowNode* pNode = (SStateWindowNode*)srcNode.get();
    pNode->pCol = nodesMakeNode(QUERY_NODE_COLUMN);
    pNode->pExpr = nodesMakeNode(QUERY_NODE_OPERATOR);
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_SESSION_WINDOW));
    SSessionWindowNode* pNode = (SSessionWindowNode*)srcNode.get();
    pNode->pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
    pNode->pGap = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
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
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    srcNode.reset(nodesMakeNode(QUERY_NODE_INTERVAL_WINDOW));
    SIntervalWindowNode* pNode = (SIntervalWindowNode*)srcNode.get();
    pNode->pInterval = nodesMakeNode(QUERY_NODE_VALUE);
    pNode->pOffset = nodesMakeNode(QUERY_NODE_VALUE);
    pNode->pSliding = nodesMakeNode(QUERY_NODE_VALUE);
    pNode->pFill = nodesMakeNode(QUERY_NODE_FILL);
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_FILL));
    SFillNode* pNode = (SFillNode*)srcNode.get();
    pNode->mode = FILL_MODE_VALUE;
    pNode->pValues = nodesMakeNode(QUERY_NODE_NODE_LIST);
    pNode->pWStartTs = nodesMakeNode(QUERY_NODE_COLUMN);
    pNode->timeRange.skey = 1666756692907;
    pNode->timeRange.ekey = 1666756699907;
    return srcNode.get();
  }());
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN));
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN));
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
    srcNode.reset(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN));
    SSystemTableScanPhysiNode* pNode = (SSystemTableScanPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, physiStreamSemiSessionWinodw) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SStreamSemiSessionWinodwPhysiNode* pSrcNode = (SStreamSemiSessionWinodwPhysiNode*)pSrc;
    SStreamSemiSessionWinodwPhysiNode* pDstNode = (SStreamSemiSessionWinodwPhysiNode*)pDst;
    ASSERT_EQ(pSrcNode->gap, pDstNode->gap);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    srcNode.reset(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION));
    SStreamSemiSessionWinodwPhysiNode* pNode = (SStreamSemiSessionWinodwPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, physiStreamFinalSessionWinodw) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SStreamFinalSessionWinodwPhysiNode* pSrcNode = (SStreamFinalSessionWinodwPhysiNode*)pSrc;
    SStreamFinalSessionWinodwPhysiNode* pDstNode = (SStreamFinalSessionWinodwPhysiNode*)pDst;
    ASSERT_EQ(pSrcNode->gap, pDstNode->gap);
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    srcNode.reset(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION));
    SStreamFinalSessionWinodwPhysiNode* pNode = (SStreamFinalSessionWinodwPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, physiStreamPartition) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SStreamPartitionPhysiNode* pSrcNode = (SStreamPartitionPhysiNode*)pSrc;
    SStreamPartitionPhysiNode* pDstNode = (SStreamPartitionPhysiNode*)pDst;
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    srcNode.reset(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION));
    SStreamPartitionPhysiNode* pNode = (SStreamPartitionPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}

TEST_F(NodesCloneTest, physiPartition) {
  registerCheckFunc([](const SNode* pSrc, const SNode* pDst) {
    ASSERT_EQ(nodeType(pSrc), nodeType(pDst));
    SPartitionPhysiNode* pSrcNode = (SPartitionPhysiNode*)pSrc;
    SPartitionPhysiNode* pDstNode = (SPartitionPhysiNode*)pDst;
  });

  std::unique_ptr<SNode, void (*)(SNode*)> srcNode(nullptr, nodesDestroyNode);

  run([&]() {
    srcNode.reset(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_PARTITION));
    SPartitionPhysiNode* pNode = (SPartitionPhysiNode*)srcNode.get();
    return srcNode.get();
  }());
}
