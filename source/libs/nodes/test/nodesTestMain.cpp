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

#include "nodesUtil.h"
#include "querynodes.h"
#include "taoserror.h"
#include "ttime.h"

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

// Both validators must reject the name and quote it back in terrMsg.
static void expectTimezoneRejected(const char* tzStr, const char* expectedMsg) {
  const char* label = (tzStr == nullptr) ? "<null>" : tzStr;

  taosClearErrMsg();
  EXPECT_EQ(TSDB_CODE_PAR_INVALID_TIMEZONE,
            taosValidateTimezone(tzStr, nullptr))
      << "taosValidateTimezone accepted " << label;
  EXPECT_STREQ(expectedMsg, terrMsg) << "input: " << label;

  char normalized[TD_TIMEZONE_LEN] = {0};
  taosClearErrMsg();
  EXPECT_EQ(TSDB_CODE_PAR_INVALID_TIMEZONE,
            taosValidateAndNormalizeTimezone(tzStr, normalized,
                                             sizeof(normalized), nullptr))
      << "taosValidateAndNormalizeTimezone accepted " << label;
  EXPECT_STREQ(expectedMsg, terrMsg) << "input: " << label;
}

TEST(NodesTest, invalidTimezoneErrorContainsName) {
  // The nodes layer delegates to taosValidateTimezone and must leave the
  // detailed message intact.
  const char* timezoneName = "UTC+2000";
  void*       timezone = nullptr;
  bool        ownsTimezone = false;

  taosClearErrMsg();
  int32_t code = nodesDecodeTimezoneNameInPlace(
      timezoneName, &timezone, &ownsTimezone);

  EXPECT_EQ(TSDB_CODE_PAR_INVALID_TIMEZONE, code);
  EXPECT_STREQ("Invalid timezone: 'UTC+2000'", terrMsg);
  EXPECT_EQ(nullptr, timezone);
  EXPECT_FALSE(ownsTimezone);
}

TEST(NodesTest, invalidTimezoneRejectPathsReportName) {
  // Missing name: reported as an empty quoted name.
  expectTimezoneRejected(nullptr, "Invalid timezone: ''");
  expectTimezoneRejected("", "Invalid timezone: ''");

  // Ambiguous uppercase abbreviations.
  expectTimezoneRejected("CST", "Invalid timezone: 'CST'");
  expectTimezoneRejected("EST", "Invalid timezone: 'EST'");

  // GMT series, in either case, bare or with an offset.
  expectTimezoneRejected("GMT", "Invalid timezone: 'GMT'");
  expectTimezoneRejected("GMT+8", "Invalid timezone: 'GMT+8'");
  expectTimezoneRejected("gmt-5", "Invalid timezone: 'gmt-5'");

  // Unknown name carrying no slash.
  expectTimezoneRejected("foobar", "Invalid timezone: 'foobar'");

  // Offset hours out of range.
  expectTimezoneRejected("UTC+2000", "Invalid timezone: 'UTC+2000'");
  expectTimezoneRejected("+2000", "Invalid timezone: '+2000'");

  // Offset hours that are not two digits.
  expectTimezoneRejected("+8:00", "Invalid timezone: '+8:00'");
  expectTimezoneRejected("+8", "Invalid timezone: '+8'");
}

TEST(NodesTest, timezoneNormalizeRejectsUndersizedBuffer) {
  char normalized[4] = {0};

  taosClearErrMsg();
  int32_t code = taosValidateAndNormalizeTimezone(
      "UTC", normalized, sizeof(normalized), nullptr);

  EXPECT_EQ(TSDB_CODE_PAR_INVALID_TIMEZONE, code);
  EXPECT_STREQ("Invalid timezone: 'UTC'", terrMsg);
}

TEST(NodesTest, validTimezonesAreStillAccepted) {
  // Guards the reject conditions above against an inverted sense.
  // The IANA name needs system tzdata, as elsewhere in the test suite.
  const char* accepted[] = {"UTC", "Z", "+08:00", "Asia/Shanghai"};

  for (const char* tzStr : accepted) {
    char normalized[TD_TIMEZONE_LEN] = {0};
    EXPECT_EQ(TSDB_CODE_SUCCESS, taosValidateTimezone(tzStr, nullptr))
        << "taosValidateTimezone rejected " << tzStr;
    EXPECT_EQ(TSDB_CODE_SUCCESS,
              taosValidateAndNormalizeTimezone(tzStr, normalized,
                                               sizeof(normalized), nullptr))
        << "taosValidateAndNormalizeTimezone rejected " << tzStr;
  }
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
