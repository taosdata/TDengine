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

bool compareValueNode(SNode* pNode1, SNode* pNode2) {
  SValueNode* p1 = (SValueNode*)pNode1;
  SValueNode* p2 = (SValueNode*)pNode2;

  return p1->datum.i < p2->datum.i;
}

void assert_sort_result(SNodeList* pList) {
  SNode* pNode;
  int32_t i = 0;
  FOREACH(pNode, pList) {
    SValueNode* p = (SValueNode*)pNode;
    ASSERT_EQ(p->datum.i, i++);
  }
  SListCell* pCell = pList->pHead;
  ASSERT_TRUE(pCell->pPrev == NULL);
  ASSERT_TRUE(pList->pTail->pNext == NULL);
  int32_t len = 1;
  while (pCell) {
    if (pCell->pNext) {
      ASSERT_TRUE(pCell->pNext->pPrev == pCell);
    }
    pCell = pCell->pNext;
    if (pCell) len++;
  }
  ASSERT_EQ(len, pList->length);
}

TEST(NodesTest, sort) {
  SValueNode *vn1 = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  vn1->datum.i = 4;

  SValueNode *vn2 = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  vn2->datum.i = 3;

  SValueNode *vn3 = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  vn3->datum.i = 2;

  SValueNode *vn4 = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  vn4->datum.i = 1;

  SValueNode *vn5 = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
  vn5->datum.i = 0;

  SNodeList* l = NULL;
  nodesListMakeAppend(&l, (SNode*)vn1);
  nodesListMakeAppend(&l, (SNode*)vn2);
  nodesListMakeAppend(&l, (SNode*)vn3);
  nodesListMakeAppend(&l, (SNode*)vn4);
  nodesListMakeAppend(&l, (SNode*)vn5);

  nodesSortList(&l, compareValueNode);

  assert_sort_result(l);

  nodesDestroyList(l);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
