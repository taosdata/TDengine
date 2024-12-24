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
    SValueNode* pVal = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
    string tmp = to_string(stoi(((SValueNode*)(pOp->pLeft))->literal) + stoi(((SValueNode*)(pOp->pRight))->literal));
    pVal->literal = taosStrdup(tmp.c_str());
    nodesDestroyNode(*pNode);
    *pNode = (SNode*)pVal;
  }
  return DEAL_RES_CONTINUE;
}

TEST(NodesTest, traverseTest) {
  SNode*         pRoot = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR,(SNode**)&pRoot);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SOperatorNode* pOp = (SOperatorNode*)pRoot;
  SOperatorNode* pLeft = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pLeft));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pLeft->pLeft));
  ((SValueNode*)(pLeft->pLeft))->literal = taosStrdup("10");
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pLeft->pRight));
  ((SValueNode*)(pLeft->pRight))->literal = taosStrdup("5");
  pOp->pLeft = (SNode*)pLeft;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pOp->pRight));
  ((SValueNode*)(pOp->pRight))->literal = taosStrdup("3");

  EXPECT_EQ(nodeType(pRoot), QUERY_NODE_OPERATOR);
  EDealRes res = DEAL_RES_CONTINUE;
  nodesRewriteExprPostOrder(&pRoot, rewriterTest, &res);
  EXPECT_EQ(res, DEAL_RES_CONTINUE);
  EXPECT_EQ(nodeType(pRoot), QUERY_NODE_VALUE);
  EXPECT_EQ(string(((SValueNode*)pRoot)->literal), "18");
  nodesDestroyNode(pRoot);
}

int32_t compareValueNode(SNode* pNode1, SNode* pNode2) {
  SValueNode* p1 = (SValueNode*)pNode1;
  SValueNode* p2 = (SValueNode*)pNode2;

  if (p1->datum.i < p2->datum.i)
    return -1;
  else if (p1->datum.i > p2->datum.i)
    return 1;
  else
    return 0;
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
  SValueNode *vn1 = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&vn1));
  vn1->datum.i = 4;

  SValueNode *vn2 = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&vn2));
  vn2->datum.i = 3;

  SValueNode *vn3 = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&vn3));
  vn3->datum.i = 2;

  SValueNode *vn4 = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&vn4));
  vn4->datum.i = 1;

  SValueNode *vn5 = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&vn5));
  vn5->datum.i = 0;

  SNodeList* l = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&l, (SNode*)vn1));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&l, (SNode*)vn2));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&l, (SNode*)vn3));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&l, (SNode*)vn4));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeAppend(&l, (SNode*)vn5));

  nodesSortList(&l, compareValueNode);

  assert_sort_result(l);

  nodesDestroyList(l);
}

TEST(NodesTest, match) {
  SNode* pOperator = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOperator);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  SOperatorNode* pOp = (SOperatorNode*)pOperator;
  SOperatorNode* pLeft = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pLeft));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pLeft->pLeft));
  ((SValueNode*)(pLeft->pLeft))->literal = taosStrdup("10");
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pLeft->pRight));
  ((SValueNode*)(pLeft->pRight))->literal = taosStrdup("5");
  pOp->pLeft = (SNode*)pLeft;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pOp->pRight));
  ((SValueNode*)(pOp->pRight))->literal = taosStrdup("3");
  pOp->opType = OP_TYPE_GREATER_THAN;

  SNode* pOperatorClone = NULL;
  code = nodesCloneNode(pOperator, &pOperatorClone);
  ASSERT_TRUE(nodesMatchNode(pOperator, pOperatorClone));

  SNode* pValue = NULL;
  code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pValue);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ((SValueNode*)pValue)->literal = taosStrdup("10");
  ASSERT_FALSE(nodesMatchNode(pOperator, pValue));

  SNode* pValueClone = NULL;
  code = nodesCloneNode(pValue, &pValueClone);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(nodesMatchNode(pValue, pValueClone));
  nodesDestroyNode(pValue);
  nodesDestroyNode(pValueClone);

  SNode* pColumn = NULL, *pColumnClone = NULL;
  code = nodesMakeNode(QUERY_NODE_COLUMN, &pColumn);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  strcpy(((SColumnNode*)pColumn)->colName, "column");
  strcpy(((SColumnNode*)pColumn)->tableName, "table");
  strcpy(((SColumnNode*)pColumn)->dbName, "db");
  strcpy(((SColumnNode*)pColumn)->node.aliasName, "column");
  ASSERT_FALSE(nodesMatchNode(pOperator, pColumn));
  code = nodesCloneNode(pColumn, &pColumnClone);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(nodesMatchNode(pColumn, pColumnClone));
  nodesDestroyNode(pColumn);
  nodesDestroyNode(pColumnClone);

  SNode* pFunction = NULL, *pFunctionClone = NULL;
  code = nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunction);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ((SFunctionNode*)pFunction)->funcId = 1;
  strcpy(((SFunctionNode*)pFunction)->functionName, "now");
  ASSERT_FALSE(nodesMatchNode(pOperator, pFunction));
  code = nodesCloneNode(pFunction, &pFunctionClone);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(nodesMatchNode(pFunction, pFunctionClone));
  nodesDestroyNode(pFunctionClone);
  nodesDestroyNode(pFunction);

  SNode* pLogicCondition = NULL, *pLogicConditionClone = NULL;
  code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCondition);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ((SLogicConditionNode*)pLogicCondition)->condType = LOGIC_COND_TYPE_AND;
  ((SLogicConditionNode*)pLogicCondition)->pParameterList = NULL;
  code = nodesMakeList(&((SLogicConditionNode*)pLogicCondition)->pParameterList);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend((SNodeList*)((SLogicConditionNode*)pLogicCondition)->pParameterList, pOperator);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  code = nodesListAppend(((SLogicConditionNode*)pLogicCondition)->pParameterList, pOperatorClone);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);

  code = nodesCloneNode(pLogicCondition, &pLogicConditionClone);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_TRUE(nodesMatchNode(pLogicCondition, pLogicConditionClone));
  ASSERT_FALSE(nodesMatchNode(pLogicCondition, pFunctionClone));
  
  nodesDestroyNode(pLogicCondition);
  nodesDestroyNode(pLogicConditionClone);
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
