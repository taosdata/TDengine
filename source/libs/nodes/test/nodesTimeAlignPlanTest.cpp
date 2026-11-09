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

// Plan-node contract tests for the TIME_ALIGN output columns carried by
// SVirtualScanPhysiNode.pAlignCols.  The planner writes this list, the plan is
// cloned client-side and serialized (JSON for explain, TLV for the wire), and
// the executor reads it to build the fill_forward stage.  A silent loss in any
// of those paths turns aligned values into NULLs, so each path is asserted
// explicitly.

#include <gtest/gtest.h>

#include <cctype>
#include <memory>
#include <string>

#include "nodes.h"
#include "plannodes.h"
#include "querynodes.h"

namespace {

using NodePtr = std::unique_ptr<SNode, decltype(&nodesDestroyNode)>;

SColumnNode* makeColumn(const char* name, col_id_t colId, bool alignOut, int16_t slotId) {
  SNode* pNode = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_COLUMN, &pNode));
  if (nullptr == pNode) return nullptr;
  SColumnNode* pCol = reinterpret_cast<SColumnNode*>(pNode);
  snprintf(pCol->colName, sizeof(pCol->colName), "%s", name);
  snprintf(pCol->node.aliasName, sizeof(pCol->node.aliasName), "%s", name);
  snprintf(pCol->tableAlias, sizeof(pCol->tableAlias), "%s", "vt");
  pCol->colId = colId;
  pCol->colType = COLUMN_TYPE_COLUMN;
  pCol->slotId = slotId;
  pCol->isAlignOut = alignOut;
  pCol->node.resType.type = TSDB_DATA_TYPE_DOUBLE;
  pCol->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;
  return pCol;
}

SNode* makeTarget(SColumnNode* pCol) {
  SNode* pNode = nullptr;
  EXPECT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_TARGET, &pNode));
  if (nullptr == pNode) return nullptr;
  reinterpret_cast<STargetNode*>(pNode)->pExpr = reinterpret_cast<SNode*>(pCol);
  return pNode;
}

// Builds the same shape the planner produces: scan columns are target nodes,
// align outputs are appended to the output targets and listed in pAlignCols.
void makeVirtualScan(NodePtr* ppScan) {
  SNode* pNode = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, &pNode));
  ASSERT_NE(nullptr, pNode);
  SVirtualScanPhysiNode* pScan = reinterpret_cast<SVirtualScanPhysiNode*>(pNode);

  SColumnNode* pTs = makeColumn("ts", 1, false, 0);
  ASSERT_NE(nullptr, pTs);
  pTs->node.resType.type = TSDB_DATA_TYPE_TIMESTAMP;
  pTs->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_TIMESTAMP].bytes;

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pScan->scan.pScanCols));
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesListStrictAppend(pScan->scan.pScanCols, makeTarget(pTs)));

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pScan->pTargets));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListStrictAppend(pScan->pTargets, reinterpret_cast<SNode*>(makeColumn("ts", 1, false, 0))));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListStrictAppend(pScan->pTargets, reinterpret_cast<SNode*>(makeColumn("voltage_a", 2, true, 1))));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListStrictAppend(pScan->pTargets, reinterpret_cast<SNode*>(makeColumn("current_a", 3, true, 2))));

  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesMakeList(&pScan->pAlignCols));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListStrictAppend(pScan->pAlignCols, reinterpret_cast<SNode*>(makeColumn("voltage_a", 2, true, 1))));
  ASSERT_EQ(TSDB_CODE_SUCCESS,
            nodesListStrictAppend(pScan->pAlignCols, reinterpret_cast<SNode*>(makeColumn("current_a", 3, true, 2))));
  *ppScan = NodePtr(pNode, nodesDestroyNode);
}

void expectAlignCols(const SVirtualScanPhysiNode* pScan) {
  ASSERT_NE(nullptr, pScan->pAlignCols);
  ASSERT_EQ(2, LIST_LENGTH(pScan->pAlignCols));

  const SColumnNode* pFirst = reinterpret_cast<const SColumnNode*>(nodesListGetNode(pScan->pAlignCols, 0));
  ASSERT_NE(nullptr, pFirst);
  EXPECT_EQ(2, pFirst->colId);
  EXPECT_STREQ("voltage_a", pFirst->colName);
  EXPECT_EQ(1, pFirst->slotId);
  EXPECT_TRUE(pFirst->isAlignOut);

  const SColumnNode* pSecond = reinterpret_cast<const SColumnNode*>(nodesListGetNode(pScan->pAlignCols, 1));
  ASSERT_NE(nullptr, pSecond);
  EXPECT_EQ(3, pSecond->colId);
  EXPECT_STREQ("current_a", pSecond->colName);
  EXPECT_EQ(2, pSecond->slotId);
  EXPECT_TRUE(pSecond->isAlignOut);

  // The source targets appended before the align outputs are not marked.
  const SColumnNode* pSource = reinterpret_cast<const SColumnNode*>(nodesListGetNode(pScan->pTargets, 0));
  ASSERT_NE(nullptr, pSource);
  EXPECT_FALSE(pSource->isAlignOut);
}

void eraseJsonField(std::string* pJson, const char* pField) {
  // Removes "field":<value> including one adjacent separator.  The value can be
  // a non-scalar (AlignCols is an array of column objects), so the scan tracks
  // bracket depth and string literals instead of looking for the first comma.
  const std::string field = std::string("\"") + pField + "\":";
  const size_t      start = pJson->find(field);
  ASSERT_NE(std::string::npos, start);

  size_t i = start + field.size();
  int    depth = 0;
  bool   inStr = false;
  for (; i < pJson->size(); ++i) {
    const char c = (*pJson)[i];
    if (inStr) {
      if ('\\' == c) {
        ++i;
      } else if ('"' == c) {
        inStr = false;
      }
      continue;
    }
    if ('"' == c) {
      inStr = true;
    } else if ('[' == c || '{' == c) {
      ++depth;
    } else if (']' == c || '}' == c) {
      if (0 == depth) break;
      --depth;
    } else if (0 == depth && ',' == c) {
      break;
    }
  }
  ASSERT_LT(i, pJson->size());

  if (',' == (*pJson)[i]) {
    size_t end = i + 1;
    pJson->erase(start, end - start);
  } else {
    ASSERT_EQ('}', (*pJson)[i]);
    size_t from = start;
    while (from > 0 && std::isspace(static_cast<unsigned char>((*pJson)[from - 1]))) --from;
    ASSERT_GT(from, 0u);
    ASSERT_EQ(',', (*pJson)[from - 1]);
    pJson->erase(from - 1, i - (from - 1));
  }
}

TEST(TimeAlignPlanTest, cloneKeepsAlignCols) {
  NodePtr pScan(nullptr, nodesDestroyNode);
  makeVirtualScan(&pScan);
  ASSERT_NE(nullptr, pScan.get());

  SNode* pCloneRaw = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesCloneNode(pScan.get(), &pCloneRaw));
  NodePtr pClone(pCloneRaw, nodesDestroyNode);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, nodeType(pClone.get()));
  expectAlignCols(reinterpret_cast<const SVirtualScanPhysiNode*>(pClone.get()));
}

TEST(TimeAlignPlanTest, jsonRoundTripKeepsAlignCols) {
  NodePtr pScan(nullptr, nodesDestroyNode);
  makeVirtualScan(&pScan);
  ASSERT_NE(nullptr, pScan.get());

  char* pJson = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pScan.get(), false, &pJson, nullptr));
  ASSERT_NE(nullptr, pJson);
  std::string json = pJson;
  taosMemoryFreeClear(pJson);
  EXPECT_NE(std::string::npos, json.find("\"AlignCols\""));

  SNode* pBack = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pBack));
  NodePtr pDecoded(pBack, nodesDestroyNode);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, nodeType(pDecoded.get()));
  expectAlignCols(reinterpret_cast<const SVirtualScanPhysiNode*>(pDecoded.get()));
}

TEST(TimeAlignPlanTest, tlvRoundTripKeepsAlignCols) {
  NodePtr pScan(nullptr, nodesDestroyNode);
  makeVirtualScan(&pScan);
  ASSERT_NE(nullptr, pScan.get());

  char*   pMsg = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToMsg(pScan.get(), &pMsg, &len));
  ASSERT_NE(nullptr, pMsg);
  ASSERT_GT(len, 0);

  SNode*  pBack = nullptr;
  int32_t code = nodesMsgToNode(pMsg, len, &pBack);
  taosMemoryFree(pMsg);
  ASSERT_EQ(TSDB_CODE_SUCCESS, code);
  NodePtr pDecoded(pBack, nodesDestroyNode);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, nodeType(pDecoded.get()));
  expectAlignCols(reinterpret_cast<const SVirtualScanPhysiNode*>(pDecoded.get()));
}

TEST(TimeAlignPlanTest, absentAlignColsDecodeAsNull) {
  // Plans produced before the feature carry no AlignCols field; the decoder
  // must leave the list NULL instead of failing the whole plan.
  NodePtr pScan(nullptr, nodesDestroyNode);
  makeVirtualScan(&pScan);
  ASSERT_NE(nullptr, pScan.get());

  char* pJson = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesNodeToString(pScan.get(), false, &pJson, nullptr));
  ASSERT_NE(nullptr, pJson);
  std::string json = pJson;
  taosMemoryFreeClear(pJson);
  eraseJsonField(&json, "AlignCols");

  SNode* pBack = nullptr;
  ASSERT_EQ(TSDB_CODE_SUCCESS, nodesStringToNode(json.c_str(), &pBack));
  NodePtr pDecoded(pBack, nodesDestroyNode);
  ASSERT_EQ(QUERY_NODE_PHYSICAL_PLAN_VIRTUAL_TABLE_SCAN, nodeType(pDecoded.get()));
  EXPECT_EQ(nullptr, reinterpret_cast<SVirtualScanPhysiNode*>(pDecoded.get())->pAlignCols);
}

}  // namespace
