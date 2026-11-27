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

#include <algorithm>
#include <string>
#include <vector>

#include "os.h"

#include "executorInt.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"
#include "tdef.h"

namespace {

constexpr int32_t TJT_TS_SLOT = 0;
constexpr int32_t TJT_KEY_SLOT = 1;
constexpr int32_t TJT_VAL_SLOT = 2;
constexpr int32_t TJT_COL_NUM = 3;

// Keep block identities distinct from column slots, as in physical plans.
constexpr int32_t TJT_LEFT_BLK_ID = 7;
constexpr int32_t TJT_RIGHT_BLK_ID = 8;
constexpr int32_t TJT_RES_BLK_ID = 9;

const int32_t tjtColTypes[TJT_COL_NUM] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT};

typedef struct {
  int64_t ts;
  int32_t key;
  int32_t value;
} STjtRow;

typedef struct {
  std::vector<SSDataBlock*> blocks;
  int32_t                   nextBlock;
} STjtSourceInfo;

void tjtDestroySource(void* param) {
  STjtSourceInfo* pInfo = static_cast<STjtSourceInfo*>(param);
  if (pInfo == nullptr) {
    return;
  }

  for (SSDataBlock* pBlock : pInfo->blocks) {
    blockDataDestroy(pBlock);
  }

  delete pInfo;
}

int32_t tjtSourceNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  STjtSourceInfo* pInfo = static_cast<STjtSourceInfo*>(pOperator->info);
  if (pInfo->nextBlock >= static_cast<int32_t>(pInfo->blocks.size())) {
    setOperatorCompleted(pOperator);
    *ppRes = nullptr;
    return TSDB_CODE_SUCCESS;
  }

  *ppRes = pInfo->blocks[pInfo->nextBlock++];
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* tjtCreateBlock(int32_t blockId, const std::vector<STjtRow>& rows) {
  SSDataBlock* pBlock = nullptr;
  EXPECT_EQ(createDataBlock(&pBlock), TSDB_CODE_SUCCESS);
  EXPECT_NE(pBlock, nullptr);

  pBlock->info.id.blockId = blockId;
  pBlock->info.calWin.skey = INT64_MIN;
  pBlock->info.calWin.ekey = INT64_MAX;
  pBlock->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < TJT_COL_NUM; ++i) {
    SColumnInfoData col = createColumnInfoData(tjtColTypes[i], tDataTypes[tjtColTypes[i]].bytes, i);
    EXPECT_EQ(blockDataAppendColInfo(pBlock, &col), TSDB_CODE_SUCCESS);
  }

  EXPECT_EQ(blockDataEnsureCapacity(pBlock, rows.size()), TSDB_CODE_SUCCESS);

  for (const STjtRow& row : rows) {
    SColumnInfoData* pTs = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, TJT_TS_SLOT));
    SColumnInfoData* pKey = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, TJT_KEY_SLOT));
    SColumnInfoData* pVal = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, TJT_VAL_SLOT));

    EXPECT_EQ(colDataSetVal(pTs, pBlock->info.rows, reinterpret_cast<const char*>(&row.ts), false), TSDB_CODE_SUCCESS);
    EXPECT_EQ(colDataSetVal(pKey, pBlock->info.rows, reinterpret_cast<const char*>(&row.key), false),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(colDataSetVal(pVal, pBlock->info.rows, reinterpret_cast<const char*>(&row.value), false),
              TSDB_CODE_SUCCESS);
    pBlock->info.rows++;
  }

  return pBlock;
}

SOperatorInfo* tjtCreateSourceOperator(int32_t blockId, const std::vector<std::vector<STjtRow>>& blocks,
                                       SExecTaskInfo* pTask) {
  STjtSourceInfo* pInfo = new STjtSourceInfo();
  pInfo->nextBlock = 0;
  for (const std::vector<STjtRow>& rows : blocks) {
    pInfo->blocks.push_back(tjtCreateBlock(blockId, rows));
  }

  SOperatorInfo* pOperator = static_cast<SOperatorInfo*>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  EXPECT_NE(pOperator, nullptr);
  initOperatorCostInfo(pOperator);
  setOperatorInfo(pOperator, "ThetaJoinTestSource", QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE, false, OP_NOT_OPENED, pInfo,
                  pTask);
  pOperator->resultDataBlockId = blockId;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, tjtSourceNext, nullptr, tjtDestroySource, optrDefaultBufFn,
                                         nullptr, optrDefaultGetNextExtFn, nullptr);
  return pOperator;
}

SColumnNode* tjtCreateColumn(int32_t blockId, int32_t slotId) {
  SColumnNode* pCol = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pCol)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pCol, nullptr);
  pCol->dataBlockId = blockId;
  pCol->slotId = slotId;
  pCol->node.resType.type = tjtColTypes[slotId % TJT_COL_NUM];
  pCol->node.resType.bytes = tDataTypes[pCol->node.resType.type].bytes;
  return pCol;
}

void tjtAppendTarget(SThetaJoinPhysiNode* pJoin, int32_t srcBlockId, int32_t srcSlotId, int32_t dstSlotId) {
  STargetNode* pTarget = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode**>(&pTarget)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pTarget, nullptr);

  pTarget->dataBlockId = TJT_RES_BLK_ID;
  pTarget->slotId = dstSlotId;
  pTarget->pExpr = reinterpret_cast<SNode*>(tjtCreateColumn(srcBlockId, srcSlotId));

  EXPECT_EQ(nodesListMakeStrictAppend(&pJoin->pTargets, reinterpret_cast<SNode*>(pTarget)), TSDB_CODE_SUCCESS);
}

void tjtCreateOutputDesc(SThetaJoinPhysiNode* pJoin) {
  SDataBlockDescNode* pDesc = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&pDesc)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pDesc, nullptr);
  pDesc->dataBlockId = TJT_RES_BLK_ID;

  int32_t totalRowSize = 0;
  for (int32_t i = 0; i < TJT_COL_NUM * 2; ++i) {
    int32_t        srcSlot = i % TJT_COL_NUM;
    SSlotDescNode* pSlot = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode**>(&pSlot)), TSDB_CODE_SUCCESS);
    EXPECT_NE(pSlot, nullptr);
    pSlot->slotId = i;
    pSlot->dataType.type = tjtColTypes[srcSlot];
    pSlot->dataType.bytes = tDataTypes[tjtColTypes[srcSlot]].bytes;
    totalRowSize += pSlot->dataType.bytes;
    EXPECT_EQ(nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode*>(pSlot)), TSDB_CODE_SUCCESS);
  }

  pDesc->totalRowSize = totalRowSize;
  pDesc->outputRowSize = totalRowSize;
  pJoin->node.pOutputDataBlockDesc = pDesc;
}

// Builds "left.key == right.key" over the joined output block, which is what
// the theta operator runs its ON filter against.
SNode* tjtCreateKeyEqualCond() {
  SOperatorNode* pOp = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode**>(&pOp)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pOp, nullptr);
  pOp->opType = OP_TYPE_EQUAL;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pOp->pLeft = reinterpret_cast<SNode*>(tjtCreateColumn(TJT_RES_BLK_ID, TJT_KEY_SLOT));
  pOp->pRight = reinterpret_cast<SNode*>(tjtCreateColumn(TJT_RES_BLK_ID, TJT_COL_NUM + TJT_KEY_SLOT));
  return reinterpret_cast<SNode*>(pOp);
}

SThetaJoinPhysiNode* tjtCreateThetaJoinNode(EJoinType joinType, EJoinSubType subType, SNode* pFullOnCond) {
  SThetaJoinPhysiNode* pJoin = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_THETA_JOIN, reinterpret_cast<SNode**>(&pJoin)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pJoin, nullptr);

  pJoin->joinType = joinType;
  pJoin->subType = subType;
  // The physical planner uses these compatibility fields for child block ids.
  pJoin->leftPrimSlotId = TJT_LEFT_BLK_ID;
  pJoin->rightPrimSlotId = TJT_RIGHT_BLK_ID;
  pJoin->timeRange.skey = INT64_MIN;
  pJoin->timeRange.ekey = INT64_MAX;
  pJoin->pFullOnCond = pFullOnCond;

  int32_t dstSlot = 0;
  for (int32_t slot = 0; slot < TJT_COL_NUM; ++slot) {
    tjtAppendTarget(pJoin, TJT_LEFT_BLK_ID, slot, dstSlot++);
  }
  for (int32_t slot = 0; slot < TJT_COL_NUM; ++slot) {
    tjtAppendTarget(pJoin, TJT_RIGHT_BLK_ID, slot, dstSlot++);
  }
  tjtCreateOutputDesc(pJoin);

  return pJoin;
}

SExecTaskInfo* tjtCreateTaskInfo(const char* taskId) {
  SExecTaskInfo* pTask = static_cast<SExecTaskInfo*>(taosMemoryCalloc(1, sizeof(SExecTaskInfo)));
  EXPECT_NE(pTask, nullptr);
  pTask->id.str = const_cast<char*>(taskId);
  return pTask;
}

std::string tjtCellToString(SSDataBlock* pBlock, int32_t slot, int32_t row) {
  SColumnInfoData* pCol = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, slot));
  if (colDataIsNull_s(pCol, row)) {
    return "NULL";
  }

  if (tjtColTypes[slot % TJT_COL_NUM] == TSDB_DATA_TYPE_TIMESTAMP) {
    return std::to_string(*reinterpret_cast<int64_t*>(colDataGetData(pCol, row)));
  }

  return std::to_string(*reinterpret_cast<int32_t*>(colDataGetData(pCol, row)));
}

std::string tjtRowToString(SSDataBlock* pBlock, int32_t row) {
  std::string value;
  for (int32_t slot = 0; slot < TJT_COL_NUM * 2; ++slot) {
    if (slot > 0) {
      value += "|";
    }
    value += tjtCellToString(pBlock, slot, row);
  }
  return value;
}

std::vector<std::string> tjtDrain(SOperatorInfo* pOperator) {
  std::vector<std::string> rows;

  while (true) {
    SSDataBlock* pBlock = nullptr;
    int32_t      code = pOperator->fpSet.getNextFn(pOperator, &pBlock);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    if (code != TSDB_CODE_SUCCESS || pBlock == nullptr) {
      break;
    }

    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      rows.push_back(tjtRowToString(pBlock, i));
    }
  }

  std::sort(rows.begin(), rows.end());
  return rows;
}

std::vector<std::string> tjtRunThetaJoin(EJoinType joinType, EJoinSubType subType,
                                         const std::vector<std::vector<STjtRow>>& leftBlocks,
                                         const std::vector<std::vector<STjtRow>>& rightBlocks, const char* taskId) {
  SExecTaskInfo* pTask = tjtCreateTaskInfo(taskId);
  SOperatorInfo* pLeft = tjtCreateSourceOperator(TJT_LEFT_BLK_ID, leftBlocks, pTask);
  SOperatorInfo* pRight = tjtCreateSourceOperator(TJT_RIGHT_BLK_ID, rightBlocks, pTask);
  SOperatorInfo* downstream[] = {pLeft, pRight};

  SThetaJoinPhysiNode* pJoinNode = tjtCreateThetaJoinNode(joinType, subType, tjtCreateKeyEqualCond());

  SOperatorInfo* pJoin = nullptr;
  EXPECT_EQ(createThetaJoinOperatorInfo(downstream, 2, pJoinNode, pTask, &pJoin), TSDB_CODE_SUCCESS);
  EXPECT_NE(pJoin, nullptr);

  std::vector<std::string> actual;
  if (pJoin != nullptr) {
    actual = tjtDrain(pJoin);
    destroyOperator(pJoin);
  } else {
    destroyOperator(pLeft);
    destroyOperator(pRight);
  }

  nodesDestroyNode(reinterpret_cast<SNode*>(pJoinNode));
  taosMemoryFree(pTask);
  return actual;
}

}  // namespace

TEST(thetaJoinTest, innerJoinTakesEachColumnFromItsOwnInput) {
  std::vector<std::vector<STjtRow>> left = {{
      {1000, 1, 11},
      {1001, 2, 22},
  }};
  std::vector<std::vector<STjtRow>> right = {{
      {2000, 1, 111},
      {2001, 3, 333},
  }};

  std::vector<std::string> actual =
      tjtRunThetaJoin(JOIN_TYPE_INNER, JOIN_STYPE_NONE, left, right, "thetaInnerProvenance");

  std::vector<std::string> expected = {"1000|1|11|2000|1|111"};
  EXPECT_EQ(actual, expected);
}

TEST(thetaJoinTest, leftOuterJoinEmitsUnmatchedRowsAcrossInputBlocks) {
  std::vector<std::vector<STjtRow>> left = {
      {{1000, 1, 11}, {1001, 2, 22}},
      {{1002, 3, 33}, {1003, 4, 44}},
      {{1004, 5, 55}, {1005, 6, 66}},
  };
  std::vector<std::vector<STjtRow>> right = {{
      {2000, 2, 222},
      {2001, 5, 555},
      {2002, 6, 666},
  }};

  auto actual = tjtRunThetaJoin(JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, left,
                                right, "thetaMultiBlockOuter");
  std::vector<std::string> expected = {
      "1000|1|11|NULL|NULL|NULL",
      "1001|2|22|2000|2|222",
      "1002|3|33|NULL|NULL|NULL",
      "1003|4|44|NULL|NULL|NULL",
      "1004|5|55|2001|5|555",
      "1005|6|66|2002|6|666",
  };
  EXPECT_EQ(actual, expected);
}

TEST(thetaJoinTest, antiJoinEmitsOnlyUnmatchedLeftRows) {
  std::vector<std::vector<STjtRow>> left = {{
      {1000, 1, 11},
      {1001, 2, 22},
      {1002, 3, 33},
  }};
  std::vector<std::vector<STjtRow>> right = {{
      {2000, 2, 222},
  }};

  std::vector<std::string> actual = tjtRunThetaJoin(JOIN_TYPE_LEFT, JOIN_STYPE_ANTI, left, right, "thetaAnti");

  std::vector<std::string> expected = {
      "1000|1|11|NULL|NULL|NULL",
      "1002|3|33|NULL|NULL|NULL",
  };
  EXPECT_EQ(actual, expected);
}

TEST(thetaJoinTest, innerJoinMatchesAcrossMultipleDownstreamBlocks) {
  std::vector<std::vector<STjtRow>> left = {
      {{1000, 1, 11}},
      {{1001, 2, 22}},
      {{1002, 3, 33}},
  };
  std::vector<std::vector<STjtRow>> right = {
      {{2000, 3, 333}},
      {{2001, 1, 111}},
  };

  std::vector<std::string> actual =
      tjtRunThetaJoin(JOIN_TYPE_INNER, JOIN_STYPE_NONE, left, right, "thetaMultiBlock");

  std::vector<std::string> expected = {
      "1000|1|11|2001|1|111",
      "1002|3|33|2000|3|333",
  };
  EXPECT_EQ(actual, expected);
}
