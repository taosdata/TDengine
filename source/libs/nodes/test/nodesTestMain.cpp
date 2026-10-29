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

#include <gtest/gtest.h>

#include <thread>
#include <vector>

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

TEST(NodesTest, mergeNodeTransfersAdditionalConditionToEmptyTarget) {
  SNode* pCondition = NULL;
  SNode* pAdditional = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pAdditional));
  SNode* pExpected = pAdditional;

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMergeNode(&pCondition, &pAdditional));
  EXPECT_EQ(pCondition, pExpected);
  EXPECT_EQ(pAdditional, nullptr);

  nodesDestroyNode(pCondition);
}

TEST(NodesTest, mergeNodeCreatesAndFromTwoValues) {
  SNode* pCondition = NULL;
  SNode* pAdditional = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pCondition));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pAdditional));
  SNode* pOriginalCondition = pCondition;
  SNode* pOriginalAdditional = pAdditional;

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMergeNode(&pCondition, &pAdditional));
  ASSERT_NE(pCondition, nullptr);
  EXPECT_EQ(pAdditional, nullptr);
  ASSERT_EQ(nodeType(pCondition), QUERY_NODE_LOGIC_CONDITION);
  EXPECT_EQ(((SLogicConditionNode*)pCondition)->condType, LOGIC_COND_TYPE_AND);
  EXPECT_EQ(LIST_LENGTH(((SLogicConditionNode*)pCondition)->pParameterList), 2);
  EXPECT_EQ(nodesListGetNode(((SLogicConditionNode*)pCondition)->pParameterList, 0), pOriginalAdditional);
  EXPECT_EQ(nodesListGetNode(((SLogicConditionNode*)pCondition)->pParameterList, 1), pOriginalCondition);

  nodesDestroyNode(pCondition);
}

TEST(NodesTest, mergeNodeTransfersAdditionalConditionToAndNode) {
  SNode* pCondition = NULL;
  SNode* pFirst = NULL;
  SNode* pAdditional = NULL;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, &pCondition));
  SLogicConditionNode* pAnd = (SLogicConditionNode*)pCondition;
  pAnd->condType = LOGIC_COND_TYPE_AND;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pFirst));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListMakeStrictAppend(&pAnd->pParameterList, pFirst));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_VALUE, &pAdditional));
  SNode* pOriginalAdditional = pAdditional;

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMergeNode(&pCondition, &pAdditional));
  EXPECT_EQ(pCondition, (SNode*)pAnd);
  EXPECT_EQ(pAdditional, nullptr);
  ASSERT_EQ(LIST_LENGTH(pAnd->pParameterList), 2);
  EXPECT_EQ(nodesListGetNode(pAnd->pParameterList, 0), pFirst);
  EXPECT_EQ(nodesListGetNode(pAnd->pParameterList, 1), pOriginalAdditional);
  nodesDestroyNode(pCondition);
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
  nodesDestroyNode(pFunctionClone);
  nodesDestroyNode(pFunction);
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

// Decoded timezone handles are borrowed from a process-wide cache keyed by name, so plan
// decode/clone doesn't tzalloc() (read + parse the zoneinfo file) for every node.
TEST(NodesTest, decodedTimezoneIsCachedAndBorrowed) {
  void* pTz1 = nullptr;
  void* pTz2 = nullptr;
  bool  owns1 = true;
  bool  owns2 = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pTz1, &owns1));
  char buf[TD_TIMEZONE_LEN] = {0};
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneName("Asia/Shanghai", buf, sizeof(buf), &pTz2, &owns2));

  ASSERT_NE(nullptr, pTz1);
  EXPECT_EQ(pTz1, pTz2) << "same name must resolve to the same cached handle";
  EXPECT_FALSE(owns1) << "cached handle must not be owned (tzfree'd) by the node";
  EXPECT_FALSE(owns2);
  EXPECT_STREQ("Asia/Shanghai", buf);

  void* pOther = nullptr;
  bool  ownsOther = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("UTC", &pOther, &ownsOther));
  EXPECT_NE(pTz1, pOther) << "different names must not share a handle";
  EXPECT_FALSE(ownsOther);
}

static SNode* makeColumnNode(const char* colName) {
  SColumnNode* pCol = nullptr;
  if (TSDB_CODE_SUCCESS != nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol)) return nullptr;
  tstrncpy(pCol->colName, colName, sizeof(pCol->colName));
  tstrncpy(pCol->node.aliasName, colName, sizeof(pCol->node.aliasName));
  return (SNode*)pCol;
}

static void expectTimezoneUsable(void* pTz, time_t sec) {
  struct tm tmLocal = {0};
  ASSERT_NE(nullptr, taosLocalTime(&sec, &tmLocal, nullptr, 0, (timezone_t)pTz));
  // 2025-01-01 00:00:00 UTC is 08:00 in Asia/Shanghai (no DST).
  EXPECT_EQ(8, tmLocal.tm_hour);
}

// Destroying decoded/cloned nodes must leave the shared cached handle intact: a later
// query with the same session timezone still gets a valid handle.
TEST(NodesTest, cachedTimezoneSurvivesNodeDecodeCloneDestroy) {
  const char* tzName = "Asia/Shanghai";
  void*       pCached = nullptr;
  bool        owns = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace(tzName, &pCached, &owns));

  SOperatorNode* pOp = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp));
  pOp->opType = OP_TYPE_EQUAL;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = sizeof(bool);
  pOp->pLeft = makeColumnNode("location");
  pOp->pRight = makeColumnNode("city");
  ASSERT_NE(nullptr, pOp->pLeft);
  ASSERT_NE(nullptr, pOp->pRight);
  tstrncpy(pOp->timezoneName, tzName, sizeof(pOp->timezoneName));

  for (int32_t i = 0; i < 3; ++i) {
    char*   pMsg = nullptr;
    int32_t len = 0;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg((SNode*)pOp, &pMsg, &len));
    SNode* pDecoded = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMsgToNode(pMsg, len, &pDecoded));
    taosMemoryFree(pMsg);
    ASSERT_NE(nullptr, pDecoded);
    SOperatorNode* pDecodedOp = (SOperatorNode*)pDecoded;
    EXPECT_STREQ(tzName, pDecodedOp->timezoneName);
    EXPECT_EQ(pCached, pDecodedOp->tz);
    EXPECT_FALSE(pDecodedOp->ownsTimezone);

    SNode* pClone = nullptr;
    ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(pDecoded, &pClone));
    SOperatorNode* pCloneOp = (SOperatorNode*)pClone;
    EXPECT_EQ(pCached, pCloneOp->tz);
    EXPECT_FALSE(pCloneOp->ownsTimezone);

    nodesDestroyNode(pClone);
    nodesDestroyNode(pDecoded);
  }
  nodesDestroyNode((SNode*)pOp);

  void* pAgain = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace(tzName, &pAgain, &owns));
  EXPECT_EQ(pCached, pAgain);
  expectTimezoneUsable(pAgain, (time_t)1735689600);  // 2025-01-01 00:00:00 UTC
}

// First use of a name from many threads at once must converge on a single cached handle
// (the losers of the insert race free their own copy). Run under ASan to catch leaks or
// double frees on that path.
TEST(NodesTest, cachedTimezoneConcurrentFirstUse) {
  const char*        tzName = "America/Sao_Paulo";  // not used by any other test here
  const int32_t      kThreads = 16;
  std::vector<void*> handles(kThreads, nullptr);
  std::vector<int32_t> codes(kThreads, -1);
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < kThreads; ++i) {
    threads.emplace_back([&, i]() {
      bool owns = true;
      codes[i] = nodesDecodeTimezoneNameInPlace(tzName, &handles[i], &owns);
      if (owns) codes[i] = -2;  // must be borrowed
    });
  }
  for (auto& t : threads) t.join();

  for (int32_t i = 0; i < kThreads; ++i) {
    ASSERT_EQ(TSDB_CODE_SUCCESS, codes[i]) << "thread " << i;
    ASSERT_NE(nullptr, handles[i]) << "thread " << i;
  }
  void* pCached = nullptr;
  bool  owns = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace(tzName, &pCached, &owns));
  for (int32_t i = 0; i < kThreads; ++i) {
    EXPECT_EQ(pCached, handles[i]) << "thread " << i << " got a handle that is not the cached one";
  }
}

extern "C" void nodesTestSetTimezoneCacheMax(int32_t max);

// Aliases of one zone file ("Asia//Shanghai", "Asia/./Shanghai", ...) must not each add a
// cache entry: only canonical names are cached; any other spelling gets a node-owned handle.
TEST(NodesTest, nonCanonicalTimezoneNamesAreNotCached) {
  nodesCleanupTimezoneCache();

  void* pCanon = nullptr;
  bool  owns = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pCanon, &owns));
  EXPECT_FALSE(owns);

  const char* aliases[] = {"Asia//Shanghai", "Asia/./Shanghai", "Asia/../Asia/Shanghai"};
  int32_t     decoded = 0;
  for (const char* alias : aliases) {
    void* pTz = nullptr;
    bool  aliasOwns = false;
    taosClearErrMsg();
    if (TSDB_CODE_SUCCESS != nodesDecodeTimezoneNameInPlace(alias, &pTz, &aliasOwns)) {
      continue;  // this tzdata/tz library rejects the spelling outright, which is also fine
    }
    ++decoded;
    ASSERT_NE(nullptr, pTz) << alias;
    EXPECT_TRUE(aliasOwns) << alias << " must be owned by the caller, not cached";
    EXPECT_NE(pCanon, pTz) << alias << " must not alias the cached canonical handle";
    expectTimezoneUsable(pTz, (time_t)1735689600);
    tzfree((timezone_t)pTz);
  }
  EXPECT_GT(decoded, 0) << "at least one alias spelling should resolve on this system";

  // Repeating the same alias keeps handing out fresh handles instead of growing the cache.
  void* pFirst = nullptr;
  void* pSecond = nullptr;
  bool  o1 = false, o2 = false;
  if (TSDB_CODE_SUCCESS == nodesDecodeTimezoneNameInPlace("Asia//Shanghai", &pFirst, &o1) &&
      TSDB_CODE_SUCCESS == nodesDecodeTimezoneNameInPlace("Asia//Shanghai", &pSecond, &o2)) {
    EXPECT_TRUE(o1);
    EXPECT_TRUE(o2);
    EXPECT_NE(pFirst, pSecond);
    tzfree((timezone_t)pFirst);
    tzfree((timezone_t)pSecond);
  }
  nodesCleanupTimezoneCache();
}

// Once the cache is full, new names fall back to node-owned handles and cached ones keep working.
TEST(NodesTest, timezoneCacheIsBounded) {
  nodesCleanupTimezoneCache();
  nodesTestSetTimezoneCacheMax(2);

  void *pA = nullptr, *pB = nullptr, *pC = nullptr, *pAAgain = nullptr;
  bool  ownsA = true, ownsB = true, ownsC = false, ownsAAgain = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pA, &ownsA));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Europe/London", &pB, &ownsB));
  EXPECT_FALSE(ownsA);
  EXPECT_FALSE(ownsB);

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("America/New_York", &pC, &ownsC));
  ASSERT_NE(nullptr, pC);
  EXPECT_TRUE(ownsC) << "third distinct name exceeds the limit: handle must be owned by the caller";

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pAAgain, &ownsAAgain));
  EXPECT_FALSE(ownsAAgain);
  EXPECT_EQ(pA, pAAgain) << "names admitted earlier stay cached";
  expectTimezoneUsable(pAAgain, (time_t)1735689600);

  tzfree((timezone_t)pC);  // owned by the caller
  nodesCleanupTimezoneCache();
  nodesTestSetTimezoneCacheMax(128);  // restore NODES_TZ_CACHE_MAX for the other tests
}

// A fill logic node must keep the ownership flag of its timezone handle: the JSON decode used
// to drop it, and clone/destroy must free an owned handle exactly once.
TEST(NodesTest, fillLogicNodeTimezoneOwnership) {
  nodesCleanupTimezoneCache();
  nodesTestSetTimezoneCacheMax(0);  // nothing is admitted: every decode is node-owned

  SFillLogicNode* pFill = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_LOGIC_PLAN_FILL, (SNode**)&pFill));
  tstrncpy(pFill->timezoneName, "Asia/Shanghai", sizeof(pFill->timezoneName));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesDecodeTimezoneNameInPlace(pFill->timezoneName, &pFill->timezone, &pFill->ownsTimezone));
  ASSERT_NE(nullptr, pFill->timezone);
  EXPECT_TRUE(pFill->ownsTimezone);

  SNode* pClone = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode((SNode*)pFill, &pClone));
  SFillLogicNode* pCloneFill = (SFillLogicNode*)pClone;
  EXPECT_TRUE(pCloneFill->ownsTimezone);
  EXPECT_NE(pFill->timezone, pCloneFill->timezone) << "an owned handle must not be shared with the clone";
  EXPECT_STREQ("Asia/Shanghai", pCloneFill->timezoneName);
  expectTimezoneUsable(pCloneFill->timezone, (time_t)1735689600);

  nodesDestroyNode(pClone);  // frees its own handle
  expectTimezoneUsable(pFill->timezone, (time_t)1735689600);  // original is still valid
  nodesDestroyNode((SNode*)pFill);  // frees the original's handle

  nodesTestSetTimezoneCacheMax(128);
  nodesCleanupTimezoneCache();
}

// Cleanup releases every cached handle; a later decode re-creates the cache and still works.
// Run under ASan to check the cached handles are freed exactly once.
TEST(NodesTest, timezoneCacheCleanupAndReuse) {
  void* pTz = nullptr;
  bool  owns = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pTz, &owns));
  ASSERT_NE(nullptr, pTz);
  EXPECT_FALSE(owns);

  nodesCleanupTimezoneCache();
  nodesCleanupTimezoneCache();  // idempotent

  void* pAgain = nullptr;
  owns = true;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pAgain, &owns));
  ASSERT_NE(nullptr, pAgain);
  EXPECT_FALSE(owns);
  expectTimezoneUsable(pAgain, (time_t)1735689600);  // 2025-01-01 00:00:00 UTC

  void* pSame = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesDecodeTimezoneNameInPlace("Asia/Shanghai", &pSame, &owns));
  EXPECT_EQ(pAgain, pSame) << "re-created cache must be used for later decodes";

  nodesCleanupTimezoneCache();
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
