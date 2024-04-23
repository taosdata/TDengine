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

#include <gtest/gtest.h>

#include "querynodes.h"

using namespace std;

static EDealRes rewriterTest(SNode** pNode, void* pContext) {
  EDealRes* pRes = (EDealRes*)pContext;
  if (QUERY_NODE_OPERATOR == nodeType(*pNode)) {
    SOperatorNode* pOp = (SOperatorNode*)(*pNode);
    if (QUERY_NODE_VALUE != nodeType(pOp->pLeft) || QUERY_NODE_VALUE != nodeType(pOp->pRight)) {
      *pRes = DEAL_RES_ERROR;
    }
    SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
    string tmp = to_string(stoi(((SValueNode*)(pOp->pLeft))->literal) + stoi(((SValueNode*)(pOp->pRight))->literal));
    pVal->literal = taosStrdup(tmp.c_str());
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)pVal;
  }
  return DEAL_RES_CONTINUE;
}

TEST(NodesTest, traverseTest) {
  SNode*         pRoot = (SNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  SOperatorNode* pOp = (SOperatorNode*)pRoot;
  SOperatorNode* pLeft = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
  pLeft->pLeft = (SNode*)nodesMakeNode(QUERY_NODE_VALUE);
  ((SValueNode*)(pLeft->pLeft))->literal = taosStrdup("10");
  pLeft->pRight = (SNode*)nodesMakeNode(QUERY_NODE_VALUE);
  ((SValueNode*)(pLeft->pRight))->literal = taosStrdup("5");
  pOp->pLeft = (SNode*)pLeft;
  pOp->pRight = (SNode*)nodesMakeNode(QUERY_NODE_VALUE);
  ((SValueNode*)(pOp->pRight))->literal = taosStrdup("3");

  EXPECT_EQ(nodeType(pRoot), QUERY_NODE_OPERATOR);
  EDealRes res = DEAL_RES_CONTINUE;
  nodesRewriteExprPostOrder(&pRoot, rewriterTest, &res);
  EXPECT_EQ(res, DEAL_RES_CONTINUE);
  EXPECT_EQ(nodeType(pRoot), QUERY_NODE_VALUE);
  EXPECT_EQ(string(((SValueNode*)pRoot)->literal), "18");
  nodesDestroyNode(pRoot);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
