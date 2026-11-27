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

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#endif

#include <gtest/gtest.h>

#include <algorithm>
#include <csetjmp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "executor.h"
#include "executorInt.h"
#include "hashjoin.h"
#include "operator.h"
#include "os.h"
#include "querytask.h"
#include "stub.h"
#include "tdatablock.h"

extern "C" int32_t hJoinLaunchPrimExpr(SSDataBlock* pBlock, SHJoinTableCtx* pTable, int32_t startIdx, int32_t endIdx);

namespace {

constexpr int32_t HJT_LEFT_BLK_ID = 0;
constexpr int32_t HJT_RIGHT_BLK_ID = 1;
constexpr int32_t HJT_RES_BLK_ID = 2;
constexpr int32_t HJT_TS_SLOT = 0;
constexpr int32_t HJT_KEY_SLOT = 1;
constexpr int32_t HJT_VAL_SLOT = 2;
constexpr int32_t HJT_COL_NUM = 3;

int32_t hjtColTypes[HJT_COL_NUM] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT};

typedef struct {
  int64_t ts;
  int32_t key;
  int32_t value;
  bool    keyNull;
} SHjtRow;

typedef struct {
  std::vector<SSDataBlock*> blocks;
  int32_t                   nextBlock;
  int32_t                   failAtBlock;
  int32_t                   failCode;
} SHjtSourceInfo;

void hjtDestroySource(void* param) {
  SHjtSourceInfo* pInfo = static_cast<SHjtSourceInfo*>(param);
  if (pInfo == nullptr) {
    return;
  }

  for (SSDataBlock* pBlock : pInfo->blocks) {
    blockDataDestroy(pBlock);
  }

  delete pInfo;
}

int32_t hjtSourceNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  SHjtSourceInfo* pInfo = static_cast<SHjtSourceInfo*>(pOperator->info);
  if (pInfo->failAtBlock >= 0 && pInfo->nextBlock == pInfo->failAtBlock) {
    *ppRes = nullptr;
    return pInfo->failCode;
  }

  if (pInfo->nextBlock >= static_cast<int32_t>(pInfo->blocks.size())) {
    setOperatorCompleted(pOperator);
    *ppRes = nullptr;
    return TSDB_CODE_SUCCESS;
  }

  *ppRes = pInfo->blocks[pInfo->nextBlock++];
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* hjtCreateBlock(int32_t blockId, const std::vector<SHjtRow>& rows) {
  SSDataBlock* pBlock = nullptr;
  int32_t code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS || pBlock == nullptr) {
    ADD_FAILURE() << "createDataBlock failed, code=" << code;
    return nullptr;
  }
  std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> block(pBlock, blockDataDestroy);

  pBlock->info.id.blockId = blockId;
  pBlock->info.calWin.skey = INT64_MIN;
  pBlock->info.calWin.ekey = INT64_MAX;
  pBlock->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < HJT_COL_NUM; ++i) {
    SColumnInfoData col = createColumnInfoData(hjtColTypes[i], tDataTypes[hjtColTypes[i]].bytes, i);
    code = blockDataAppendColInfo(pBlock, &col);
    if (code != TSDB_CODE_SUCCESS) {
      ADD_FAILURE() << "blockDataAppendColInfo failed, code=" << code;
      return nullptr;
    }
  }

  code = blockDataEnsureCapacity(pBlock, rows.size());
  if (code != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "blockDataEnsureCapacity failed, code=" << code;
    return nullptr;
  }

  SColumnInfoData* pTs = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, HJT_TS_SLOT));
  SColumnInfoData* pKey = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, HJT_KEY_SLOT));
  SColumnInfoData* pVal = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, HJT_VAL_SLOT));
  for (const SHjtRow& row : rows) {
    EXPECT_EQ(colDataSetVal(pTs, pBlock->info.rows, reinterpret_cast<const char*>(&row.ts), false), TSDB_CODE_SUCCESS);
    EXPECT_EQ(colDataSetVal(pKey, pBlock->info.rows, reinterpret_cast<const char*>(&row.key), row.keyNull),
              TSDB_CODE_SUCCESS);
    EXPECT_EQ(colDataSetVal(pVal, pBlock->info.rows, reinterpret_cast<const char*>(&row.value), false),
              TSDB_CODE_SUCCESS);
    pBlock->info.rows++;
  }

  return block.release();
}

SOperatorInfo* hjtCreateSourceOperator(int32_t blockId, const std::vector<std::vector<SHjtRow>>& blocks,
                                       SExecTaskInfo* pTask) {
  SOperatorInfo* pOperator = static_cast<SOperatorInfo*>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  if (pOperator == nullptr) {
    ADD_FAILURE() << "alloc source operator failed";
    return nullptr;
  }

  auto freeOperator = [](SOperatorInfo* p) { taosMemoryFree(p); };
  std::unique_ptr<SOperatorInfo, decltype(freeOperator)> op(pOperator, freeOperator);
  std::unique_ptr<SHjtSourceInfo, decltype(&hjtDestroySource)> source(new SHjtSourceInfo(), hjtDestroySource);
  SHjtSourceInfo* pInfo = source.get();
  pInfo->blocks.reserve(blocks.size());
  pInfo->nextBlock = 0;
  pInfo->failAtBlock = -1;
  pInfo->failCode = TSDB_CODE_SUCCESS;
  for (const std::vector<SHjtRow>& rows : blocks) {
    SSDataBlock* pBlock = hjtCreateBlock(blockId, rows);
    if (pBlock == nullptr) return nullptr;
    pInfo->blocks.push_back(pBlock);
  }

  initOperatorCostInfo(pOperator);
  setOperatorInfo(pOperator, "HashJoinTestSource", QUERY_NODE_PHYSICAL_PLAN_ROWSET_SOURCE, false, OP_NOT_OPENED, pInfo,
                  pTask);
  pOperator->resultDataBlockId = blockId;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, hjtSourceNext, nullptr, hjtDestroySource, optrDefaultBufFn,
                                         nullptr, optrDefaultGetNextExtFn, nullptr);
  source.release();
  return op.release();
}

SOperatorInfo* hjtCreateFailingSourceOperator(int32_t blockId, const std::vector<std::vector<SHjtRow>>& blocks,
                                              SExecTaskInfo* pTask, int32_t failAtBlock, int32_t failCode) {
  SOperatorInfo* pOperator = hjtCreateSourceOperator(blockId, blocks, pTask);
  if (pOperator == nullptr) return nullptr;
  SHjtSourceInfo* pInfo = static_cast<SHjtSourceInfo*>(pOperator->info);
  pInfo->failAtBlock = failAtBlock;
  pInfo->failCode = failCode;
  return pOperator;
}

SColumnNode* hjtCreateColumn(int32_t blockId, int32_t slotId) {
  SColumnNode* pCol = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pCol)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pCol, nullptr);
  pCol->dataBlockId = blockId;
  pCol->slotId = slotId;
  pCol->node.resType.type = hjtColTypes[slotId];
  pCol->node.resType.bytes = tDataTypes[hjtColTypes[slotId]].bytes;
  return pCol;
}

SColumnNode* hjtCreateResultColumn(int32_t slotId) {
  SColumnNode* pCol = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pCol)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pCol, nullptr);
  pCol->dataBlockId = HJT_RES_BLK_ID;
  pCol->slotId = slotId;
  pCol->node.resType.type = hjtColTypes[slotId % HJT_COL_NUM];
  pCol->node.resType.bytes = tDataTypes[pCol->node.resType.type].bytes;
  return pCol;
}

SValueNode* hjtCreateIntValue(int32_t value) {
  SValueNode* pVal = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&pVal)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pVal, nullptr);
  pVal->node.resType.type = TSDB_DATA_TYPE_INT;
  pVal->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_INT].bytes;
  EXPECT_EQ(nodesSetValueNodeValue(pVal, &value), TSDB_CODE_SUCCESS);
  pVal->translate = true;
  return pVal;
}

SNode* hjtCreateRightValueEqualsCond(int32_t value) {
  SOperatorNode* pOp = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode**>(&pOp)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pOp, nullptr);
  pOp->opType = OP_TYPE_EQUAL;
  pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pOp->pLeft = reinterpret_cast<SNode*>(hjtCreateResultColumn(HJT_COL_NUM + HJT_VAL_SLOT));
  pOp->pRight = reinterpret_cast<SNode*>(hjtCreateIntValue(value));
  return reinterpret_cast<SNode*>(pOp);
}

void hjtAppendKey(SNodeList** ppList, int32_t blockId, int32_t slotId) {
  EXPECT_EQ(nodesListMakeStrictAppend(ppList, reinterpret_cast<SNode*>(hjtCreateColumn(blockId, slotId))),
            TSDB_CODE_SUCCESS);
}

void hjtAppendTarget(SHashJoinPhysiNode* pJoin, int32_t srcBlockId, int32_t srcSlotId, int32_t dstSlotId) {
  STargetNode* pTarget = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode**>(&pTarget)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pTarget, nullptr);

  pTarget->dataBlockId = HJT_RES_BLK_ID;
  pTarget->slotId = dstSlotId;
  pTarget->pExpr = reinterpret_cast<SNode*>(hjtCreateColumn(srcBlockId, srcSlotId));

  EXPECT_EQ(nodesListMakeStrictAppend(&pJoin->pTargets, reinterpret_cast<SNode*>(pTarget)), TSDB_CODE_SUCCESS);
}

void hjtCreateOutputDesc(SHashJoinPhysiNode* pJoin) {
  SDataBlockDescNode* pDesc = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&pDesc)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pDesc, nullptr);
  pDesc->dataBlockId = HJT_RES_BLK_ID;

  int32_t totalRowSize = 0;
  for (int32_t i = 0; i < HJT_COL_NUM * 2; ++i) {
    int32_t srcSlot = i % HJT_COL_NUM;
    SSlotDescNode* pSlot = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode**>(&pSlot)), TSDB_CODE_SUCCESS);
    EXPECT_NE(pSlot, nullptr);
    pSlot->slotId = i;
    pSlot->dataType.type = hjtColTypes[srcSlot];
    pSlot->dataType.bytes = tDataTypes[hjtColTypes[srcSlot]].bytes;
    totalRowSize += pSlot->dataType.bytes;
    EXPECT_EQ(nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode*>(pSlot)), TSDB_CODE_SUCCESS);
  }

  pDesc->totalRowSize = totalRowSize;
  pDesc->outputRowSize = totalRowSize;
  pJoin->node.pOutputDataBlockDesc = pDesc;
}

SHashJoinPhysiNode* hjtCreateHashJoinNode(EJoinType joinType, EJoinSubType subType, int64_t leftRows,
                                          int64_t rightRows, int64_t limit) {
  SHashJoinPhysiNode* pJoin = nullptr;
  EXPECT_EQ(nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, reinterpret_cast<SNode**>(&pJoin)), TSDB_CODE_SUCCESS);
  EXPECT_NE(pJoin, nullptr);
  if (pJoin == nullptr) return nullptr;

  pJoin->joinType = joinType;
  pJoin->subType = subType;
  pJoin->leftPrimSlotId = HJT_TS_SLOT;
  pJoin->rightPrimSlotId = HJT_TS_SLOT;
  pJoin->timeRange.skey = INT64_MIN;
  pJoin->timeRange.ekey = INT64_MAX;
  pJoin->inputStat[0].inputRowNum = leftRows;
  pJoin->inputStat[1].inputRowNum = rightRows;

  hjtAppendKey(&pJoin->pOnLeft, HJT_LEFT_BLK_ID, HJT_KEY_SLOT);
  hjtAppendKey(&pJoin->pOnRight, HJT_RIGHT_BLK_ID, HJT_KEY_SLOT);

  int32_t dstSlot = 0;
  for (int32_t slot = 0; slot < HJT_COL_NUM; ++slot) {
    hjtAppendTarget(pJoin, HJT_LEFT_BLK_ID, slot, dstSlot++);
  }
  for (int32_t slot = 0; slot < HJT_COL_NUM; ++slot) {
    hjtAppendTarget(pJoin, HJT_RIGHT_BLK_ID, slot, dstSlot++);
  }
  hjtCreateOutputDesc(pJoin);

  if (limit > 0) {
    SLimitNode* pLimit = nullptr;
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_LIMIT, reinterpret_cast<SNode**>(&pLimit)), TSDB_CODE_SUCCESS);
    EXPECT_NE(pLimit, nullptr);
    EXPECT_EQ(nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&pLimit->limit)), TSDB_CODE_SUCCESS);
    EXPECT_NE(pLimit->limit, nullptr);
    pLimit->limit->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    pLimit->limit->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
    pLimit->limit->datum.i = limit;
    pJoin->node.pLimit = reinterpret_cast<SNode*>(pLimit);
  }

  return pJoin;
}

SExecTaskInfo* hjtCreateTaskInfo(const char* taskId) {
  size_t taskIdLen = strlen(taskId) + 1;
  SExecTaskInfo* pTask =
      static_cast<SExecTaskInfo*>(taosMemoryCalloc(1, sizeof(SExecTaskInfo) + taskIdLen));
  if (pTask == nullptr) {
    ADD_FAILURE() << "failed to allocate hash join test task";
    return nullptr;
  }
  pTask->id.str = reinterpret_cast<char*>(pTask + 1);
  memcpy(pTask->id.str, taskId, taskIdLen);
  return pTask;
}

std::string hjtCellToString(SSDataBlock* pBlock, int32_t slot, int32_t row) {
  SColumnInfoData* pCol = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, slot));
  if (pCol == nullptr) {
    return "NULL";
  }
  if (colDataIsNull_s(pCol, row)) {
    return "NULL";
  }

  const char* pData = colDataGetData(pCol, row);
  if (pData == nullptr) {
    return "NULL";
  }
  if (pCol->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
    int64_t value = 0;
    memcpy(&value, pData, sizeof(value));
    return std::to_string(value);
  }

  int32_t value = 0;
  memcpy(&value, pData, sizeof(value));
  return std::to_string(value);
}

std::string hjtRowToString(SSDataBlock* pBlock, int32_t row) {
  std::string value;
  for (int32_t slot = 0; slot < HJT_COL_NUM * 2; ++slot) {
    if (slot > 0) {
      value += "|";
    }
    value += hjtCellToString(pBlock, slot, row);
  }
  return value;
}

std::vector<std::string> hjtDrain(SOperatorInfo* pOperator, int32_t* pBlockNum) {
  constexpr int32_t kMaxResultBlocks = 10000;
  std::vector<std::string> rows;
  *pBlockNum = 0;

  while (true) {
    SSDataBlock* pBlock = nullptr;
    int32_t code = pOperator->fpSet.getNextFn(pOperator, &pBlock);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
    if (code != TSDB_CODE_SUCCESS || pBlock == nullptr) {
      break;
    }

    if (++(*pBlockNum) > kMaxResultBlocks) {
      ADD_FAILURE() << "hash join did not finish within " << kMaxResultBlocks << " result blocks";
      return rows;
    }
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      rows.push_back(hjtRowToString(pBlock, i));
    }
  }

  std::sort(rows.begin(), rows.end());
  return rows;
}

int64_t hjtCountRows(const std::vector<std::vector<SHjtRow>>& blocks) {
  int64_t rows = 0;
  for (const std::vector<SHjtRow>& block : blocks) {
    rows += block.size();
  }
  return rows;
}

std::vector<std::vector<SHjtRow>> hjtDefaultLeftBlocks() {
  return {{
      {1000, 1, 10, false},
      {1001, 1, 11, false},
      {1002, 2, 20, false},
      {1003, 0, 30, true},
  }};
}

std::vector<std::vector<SHjtRow>> hjtDefaultRightBlocks() {
  return {{
      {2000, 1, 100, false},
      {2001, 1, 101, false},
      {2002, 3, 300, false},
      {2003, 0, 400, true},
  }};
}

std::vector<std::string> hjtRunHashJoin(EJoinType joinType, EJoinSubType subType,
                                        const std::vector<std::vector<SHjtRow>>& leftBlocks,
                                        const std::vector<std::vector<SHjtRow>>& rightBlocks, int64_t limit,
                                        int32_t* pBlockNum, const char* taskId, int32_t forcedResultCapacity = 0,
                                        SNode* pFullOnCond = nullptr, int64_t estimatedLeftRows = -1,
                                        int64_t estimatedRightRows = -1) {
  SExecTaskInfo* pTask = hjtCreateTaskInfo(taskId);
  if (pTask == nullptr) {
    nodesDestroyNode(pFullOnCond);
    return {};
  }

  SOperatorInfo* pLeft = hjtCreateSourceOperator(HJT_LEFT_BLK_ID, leftBlocks, pTask);
  SOperatorInfo* pRight = hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, rightBlocks, pTask);
  if (pLeft == nullptr || pRight == nullptr) {
    ADD_FAILURE() << "failed to create hash join source operator";
    if (pLeft != nullptr) {
      destroyOperator(pLeft);
    }
    if (pRight != nullptr) {
      destroyOperator(pRight);
    }
    taosMemoryFree(pTask);
    nodesDestroyNode(pFullOnCond);
    return {};
  }
  SOperatorInfo* downstream[] = {pLeft, pRight};
  SHashJoinPhysiNode* pJoinNode =
      hjtCreateHashJoinNode(joinType, subType, hjtCountRows(leftBlocks), hjtCountRows(rightBlocks), limit);
  if (pJoinNode == nullptr) {
    destroyOperator(pLeft);
    destroyOperator(pRight);
    taosMemoryFree(pTask);
    ADD_FAILURE() << "failed to create hash join node";
    nodesDestroyNode(pFullOnCond);
    return {};
  }
  pJoinNode->pFullOnCond = pFullOnCond;
  if (estimatedLeftRows >= 0) {
    pJoinNode->inputStat[0].inputRowNum = estimatedLeftRows;
  }
  if (estimatedRightRows >= 0) {
    pJoinNode->inputStat[1].inputRowNum = estimatedRightRows;
  }

  SOperatorInfo* pJoin = nullptr;
  int32_t createCode = createHashJoinOperatorInfo(downstream, 2, pJoinNode, pTask, &pJoin);
  if (createCode != TSDB_CODE_SUCCESS || pJoin == nullptr) {
    ADD_FAILURE() << "createHashJoinOperatorInfo failed, code=" << createCode;
    nodesDestroyNode(reinterpret_cast<SNode*>(pJoinNode));
    taosMemoryFree(pTask);
    return {};
  }

  std::vector<std::string> actual;
  if (forcedResultCapacity > 0) {
    SHJoinOperatorInfo* pInfo = static_cast<SHJoinOperatorInfo*>(pJoin->info);
    pInfo->finBlk->info.capacity = forcedResultCapacity;
    if (pInfo->midBlk != nullptr) {
      pInfo->midBlk->info.capacity = forcedResultCapacity;
    }
    pInfo->blkThreshold = (std::max)(1, static_cast<int32_t>(forcedResultCapacity * HJOIN_BLK_THRESHOLD_RATIO));
  }
  actual = hjtDrain(pJoin, pBlockNum);
  destroyOperator(pJoin);

  nodesDestroyNode(reinterpret_cast<SNode*>(pJoinNode));
  taosMemoryFree(pTask);
  return actual;
}

void hjtExpectRows(const std::vector<std::string>& actual, std::vector<std::string> expected) {
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(actual, expected);
}

void hjtExpectHashJoinJumpCode(SOperatorInfo* pJoin, SExecTaskInfo* pTask, int32_t expectedCode) {
  SSDataBlock* pBlock = nullptr;
  int32_t jumpCode = setjmp(pTask->env);
  if (jumpCode == 0) {
    int32_t code = pJoin->fpSet.getNextFn(pJoin, &pBlock);
    FAIL() << "expected longjmp code " << expectedCode << ", returned code " << code;
  } else {
    EXPECT_EQ(jumpCode, expectedCode);
    EXPECT_EQ(pTask->code, expectedCode);
  }
}

// Failure injection is scoped to constructor calls after all test inputs exist.
class ThetaConstructorFailureTest : public testing::Test {
 protected:
  static ThetaConstructorFailureTest* active;
  Stub stub;
  int allocation = 0;
  int failAt = 0;
  int destroyed = 0;

  static void* failCalloc(int64_t num, int64_t size) {
    if (++active->allocation == active->failAt) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return nullptr;
    }
    active->stub.reset(taosMemCalloc);
    void* result = taosMemCalloc(num, size);
    active->stub.set(taosMemCalloc, failCalloc);
    return result;
  }

  static int32_t failClone(const SNodeList*, SNodeList**) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  static void countDestroy(void* info) {
    ++active->destroyed;
    hjtDestroySource(info);
  }

  void TearDown() override { active = nullptr; }
};

ThetaConstructorFailureTest* ThetaConstructorFailureTest::active = nullptr;

TEST_F(ThetaConstructorFailureTest, releasesBothSourcesOnEarlyAndLaterFailure) {
  active = this;
  // 1 and 2 fail the initial calloc calls; 3 fails nodesCloneList.
  for (int failure : {1, 2, 3}) {
    SExecTaskInfo* task = hjtCreateTaskInfo("thetaConstructorFailure");
    ASSERT_NE(task, nullptr);
    SOperatorInfo* sources[] = {
        hjtCreateSourceOperator(HJT_LEFT_BLK_ID, hjtDefaultLeftBlocks(), task),
        hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, hjtDefaultRightBlocks(), task),
    };
    ASSERT_NE(sources[0], nullptr);
    ASSERT_NE(sources[1], nullptr);
    sources[0]->fpSet.closeFn = countDestroy;
    sources[1]->fpSet.closeFn = countDestroy;
    SHashJoinPhysiNode* node = hjtCreateHashJoinNode(JOIN_TYPE_INNER, JOIN_STYPE_OUTER, 4, 4, 0);
    ASSERT_NE(node, nullptr);
    node->leftPrimSlotId = HJT_LEFT_BLK_ID;
    node->rightPrimSlotId = HJT_RIGHT_BLK_ID;
    allocation = destroyed = 0;
    failAt = failure;
    if (failure < 3) {
      stub.set(taosMemCalloc, failCalloc);
    } else {
      stub.set(nodesCloneList, failClone);
    }
    SOperatorInfo* join = nullptr;
    int32_t code = createThetaJoinOperatorInfo(sources, 2, node, task, &join);
    stub.reset(taosMemCalloc);
    stub.reset(nodesCloneList);
    EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(task->code, code);
    EXPECT_EQ(join, nullptr);
    EXPECT_EQ(destroyed, 2);
    nodesDestroyNode(reinterpret_cast<SNode*>(node));
    taosMemoryFree(task);
  }
}

TEST(HashJoinExecutor, semiTimeRangeKeepsRowsAfterExcludedPrefix) {
  for (EJoinType type : {JOIN_TYPE_LEFT, JOIN_TYPE_RIGHT}) {
    SExecTaskInfo* task = hjtCreateTaskInfo("semiTimeRange");
    ASSERT_NE(task, nullptr);
    SOperatorInfo* sources[] = {
        hjtCreateSourceOperator(HJT_LEFT_BLK_ID, {{{1000, 1, 10, false}, {1001, 2, 20, false}}}, task),
        hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, {{{1000, 1, 100, false}, {1001, 2, 200, false}}}, task),
    };
    SHashJoinPhysiNode* node = hjtCreateHashJoinNode(type, JOIN_STYPE_SEMI, 2, 2, 0);
    ASSERT_NE(node, nullptr);
    node->timeRange.skey = 1001;
    node->timeRange.ekey = 1001;
    node->timeRangeTarget = 3;
    SOperatorInfo* join = nullptr;
    ASSERT_EQ(createHashJoinOperatorInfo(sources, 2, node, task, &join), TSDB_CODE_SUCCESS);
    int32_t blocks = 0;
    hjtExpectRows(hjtDrain(join, &blocks), {"1001|2|20|1001|2|200"});
    destroyOperator(join);
    nodesDestroyNode(reinterpret_cast<SNode*>(node));
    taosMemoryFree(task);
  }
}

TEST(HashJoinExecutor, fullOuterPreservesMixedPrecisionUnmatchedKeys) {
  for (bool nullKey : {false, true}) {
    SExecTaskInfo* task = hjtCreateTaskInfo("fullOuterMixedPrecision");
    ASSERT_NE(task, nullptr);
    SOperatorInfo* sources[] = {
        hjtCreateSourceOperator(HJT_LEFT_BLK_ID, {{{1000000, 1, 10, false}}}, task),
        hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, {{{2000, 2, 20, nullKey}}}, task),
    };
    SHashJoinPhysiNode* node = hjtCreateHashJoinNode(JOIN_TYPE_FULL, JOIN_STYPE_OUTER, 10, 1, 0);
    ASSERT_NE(node, nullptr);
    hjtAppendKey(&node->pOnLeft, HJT_LEFT_BLK_ID, HJT_TS_SLOT);
    hjtAppendKey(&node->pOnRight, HJT_RIGHT_BLK_ID, HJT_TS_SLOT);
    reinterpret_cast<SColumnNode*>(nodesListGetNode(node->pOnLeft, 1))->node.resType.precision =
        TSDB_TIME_PRECISION_MICRO;
    auto* leftInfo = static_cast<SHjtSourceInfo*>(sources[0]->info);
    static_cast<SColumnInfoData*>(taosArrayGet(leftInfo->blocks[0]->pDataBlock, HJT_TS_SLOT))->info.precision =
        TSDB_TIME_PRECISION_MICRO;
    reinterpret_cast<SSlotDescNode*>(nodesListGetNode(node->node.pOutputDataBlockDesc->pSlots, 0))->dataType.precision =
        TSDB_TIME_PRECISION_MICRO;
    SOperatorInfo* join = nullptr;
    ASSERT_EQ(createHashJoinOperatorInfo(sources, 2, node, task, &join), TSDB_CODE_SUCCESS);
    int32_t blocks = 0;
    hjtExpectRows(hjtDrain(join, &blocks), {
        "1000000|1|10|NULL|NULL|NULL",
        nullKey ? "NULL|NULL|NULL|2000|NULL|20" : "NULL|NULL|NULL|2000|2|20",
    });
    destroyOperator(join);
    nodesDestroyNode(reinterpret_cast<SNode*>(node));
    taosMemoryFree(task);
  }
}

TEST(HashJoinExecutor, constantPrimaryTimestampUsesOutputPrecision) {
  for (const auto& test : {std::pair<int8_t, int8_t>{TSDB_TIME_PRECISION_MILLI, TSDB_TIME_PRECISION_MICRO},
                           {TSDB_TIME_PRECISION_MICRO, TSDB_TIME_PRECISION_MILLI}}) {
    SSDataBlock* rawBlock = nullptr;
    ASSERT_EQ(createDataBlock(&rawBlock), TSDB_CODE_SUCCESS);
    std::unique_ptr<SSDataBlock, decltype(&blockDataDestroy)> block(rawBlock, blockDataDestroy);
    SColumnInfoData col = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0);
    col.info.precision = test.second;
    ASSERT_EQ(blockDataAppendColInfo(rawBlock, &col), TSDB_CODE_SUCCESS);
    ASSERT_EQ(blockDataEnsureCapacity(rawBlock, 2), TSDB_CODE_SUCCESS);

    SHJoinTableCtx table = {};
    SNode expression = {};
    table.primExpr = &expression;
    table.primCtx.type = HJ_PRIM_VALUE;
    table.primCtx.targetSlotId = 0;
    table.primCtx.precision = test.first;
    table.primCtx.constTs = test.first == TSDB_TIME_PRECISION_MILLI ? 1234 : 1234000;
    ASSERT_EQ(hJoinLaunchPrimExpr(rawBlock, &table, 0, 1), TSDB_CODE_SUCCESS);
    auto* output = static_cast<SColumnInfoData*>(taosArrayGet(rawBlock->pDataBlock, 0));
    int64_t expected = test.second == TSDB_TIME_PRECISION_MILLI ? 1234 : 1234000;
    for (int32_t row = 0; row < 2; ++row) {
      ASSERT_FALSE(colDataIsNull_s(output, row));
      EXPECT_EQ(*reinterpret_cast<int64_t*>(colDataGetData(output, row)), expected);
    }
  }
}

// Observe detached row ownership without relying on allocator reuse or a crash.
class HashJoinDetachedRowsTest : public testing::Test {
 protected:
  static HashJoinDetachedRowsTest* active;
  Stub stub;
  SBufRowInfo* rows[2] = {};
  int frees[2] = {};

  static void recordFree(void* ptr) {
    for (int i = 0; i < 2; ++i) {
      if (ptr == active->rows[i]) {
        ++active->frees[i];
        return;
      }
    }
    active->stub.reset(taosMemFree);
    taosMemFree(ptr);
    active->stub.set(taosMemFree, recordFree);
  }

  void TearDown() override {
    stub.reset(taosMemFree);
    for (auto* row : rows) taosMemoryFree(row);
    active = nullptr;
  }
};

HashJoinDetachedRowsTest* HashJoinDetachedRowsTest::active = nullptr;

TEST_F(HashJoinDetachedRowsTest, freesDetachedBatchWhenStoredPageIsMissing) {
  active = this;
  SHJoinOperatorInfo join = {};
  SHJoinTableCtx build = {};
  SHJoinTableCtx probe = {};
  SSDataBlock result = {};
  join.joinType = JOIN_TYPE_FULL;
  join.pBuild = &build;
  join.pProbe = &probe;
  join.finBlk = &result;
  join.blkThreshold = 8;
  join.ctx.limit = INT64_MAX;
  result.info.capacity = 8;
  join.pRowBufs = taosArrayInit(1, sizeof(SBufPageInfo));
  ASSERT_NE(join.pRowBufs, nullptr);
  for (auto& row : rows) {
    row = static_cast<SBufRowInfo*>(taosMemoryCalloc(1, sizeof(SBufRowInfo)));
    ASSERT_NE(row, nullptr);
  }
  rows[0]->next = rows[1];
  join.ctx.pBuildNullRows = rows[0];
  stub.set(taosMemFree, recordFree);
  EXPECT_EQ(hJoinEmitBuildNMatchRows(&join), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  stub.reset(taosMemFree);
  EXPECT_EQ(frees[0], 1);
  EXPECT_EQ(frees[1], 1);
  EXPECT_EQ(join.ctx.pBuildNullRows, nullptr);
  taosArrayDestroy(join.pRowBufs);
}

TEST(HashJoinExecutor, thetaHonorsCancellationBeforeAndAfterLoading) {
  for (bool loadFirst : {false, true}) {
    SExecTaskInfo* task = hjtCreateTaskInfo("thetaCancellation");
    ASSERT_NE(task, nullptr);
    SOperatorInfo* sources[] = {
        hjtCreateSourceOperator(HJT_LEFT_BLK_ID, hjtDefaultLeftBlocks(), task),
        hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, hjtDefaultRightBlocks(), task),
    };
    SHashJoinPhysiNode* node = hjtCreateHashJoinNode(JOIN_TYPE_INNER, JOIN_STYPE_OUTER, 4, 4, 0);
    ASSERT_NE(node, nullptr);
    node->leftPrimSlotId = HJT_LEFT_BLK_ID;
    node->rightPrimSlotId = HJT_RIGHT_BLK_ID;
    SOperatorInfo* join = nullptr;
    ASSERT_EQ(createThetaJoinOperatorInfo(sources, 2, node, task, &join), TSDB_CODE_SUCCESS);
    SSDataBlock* result = nullptr;
    if (loadFirst) {
      EXPECT_EQ(join->fpSet.getNextFn(join, &result), TSDB_CODE_SUCCESS);
      EXPECT_NE(result, nullptr);
    }
    task->code = TSDB_CODE_TSC_QUERY_KILLED;
    EXPECT_EQ(join->fpSet.getNextFn(join, &result), TSDB_CODE_TSC_QUERY_KILLED);
    EXPECT_EQ(result, nullptr);
    if (!loadFirst) {
      EXPECT_EQ(static_cast<SHjtSourceInfo*>(sources[0]->info)->nextBlock, 0);
      EXPECT_EQ(static_cast<SHjtSourceInfo*>(sources[1]->info)->nextBlock, 0);
    }
    destroyOperator(join);
    nodesDestroyNode(reinterpret_cast<SNode*>(node));
    taosMemoryFree(task);
  }
}

TEST(HashJoinExecutor, thetaRejectsInvalidTargetBeforeExecution) {
  for (bool expressionTarget : {false, true}) {
    SExecTaskInfo* task = hjtCreateTaskInfo("thetaInvalidTarget");
    ASSERT_NE(task, nullptr);
    SOperatorInfo* sources[] = {
        hjtCreateSourceOperator(HJT_LEFT_BLK_ID, hjtDefaultLeftBlocks(), task),
        hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, hjtDefaultRightBlocks(), task),
    };
    SHashJoinPhysiNode* node = hjtCreateHashJoinNode(JOIN_TYPE_INNER, JOIN_STYPE_OUTER, 4, 4, 0);
    ASSERT_NE(node, nullptr);
    node->leftPrimSlotId = HJT_LEFT_BLK_ID;
    node->rightPrimSlotId = HJT_RIGHT_BLK_ID;
    auto* target = reinterpret_cast<STargetNode*>(nodesListGetNode(node->pTargets, 0));
    if (expressionTarget) {
      nodesDestroyNode(target->pExpr);
      target->pExpr = nullptr;
      ASSERT_EQ(nodesMakeNode(QUERY_NODE_VALUE, &target->pExpr), TSDB_CODE_SUCCESS);
    } else {
      reinterpret_cast<SColumnNode*>(target->pExpr)->dataBlockId = 999;
    }
    SOperatorInfo* join = nullptr;
    EXPECT_EQ(createThetaJoinOperatorInfo(sources, 2, node, task, &join),
              TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    EXPECT_EQ(join, nullptr);
    nodesDestroyNode(reinterpret_cast<SNode*>(node));
    taosMemoryFree(task);
  }
}

TEST(HashJoinExecutor, leftOuterDuplicateAndUnmatchedProbeRows) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.leftOuterDuplicateAndUnmatchedProbeRows");
  hjtExpectRows(actual, {
                            "1000|1|10|2000|1|100",
                            "1000|1|10|2001|1|101",
                            "1001|1|11|2000|1|100",
                            "1001|1|11|2001|1|101",
                            "1002|2|20|NULL|NULL|NULL",
                            "1003|NULL|30|NULL|NULL|NULL",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, rightOuterDuplicateAndUnmatchedProbeRows) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_RIGHT, JOIN_STYPE_OUTER, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.rightOuterDuplicateAndUnmatchedProbeRows");
  hjtExpectRows(actual, {
                            "1000|1|10|2000|1|100",
                            "1000|1|10|2001|1|101",
                            "1001|1|11|2000|1|100",
                            "1001|1|11|2001|1|101",
                            "NULL|NULL|NULL|2002|3|300",
                            "NULL|NULL|NULL|2003|NULL|400",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, leftSemiKeepsMatchedProbeRowsOnce) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_LEFT, JOIN_STYPE_SEMI, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.leftSemiKeepsMatchedProbeRowsOnce");
  hjtExpectRows(actual, {
                            "1000|1|10|2001|1|101",
                            "1001|1|11|2001|1|101",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, leftSemiResidualLimitIgnoresFailedBuildCandidate) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_LEFT, JOIN_STYPE_SEMI, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 1, &blockNum,
                     "HashJoinExecutor.leftSemiResidualLimitIgnoresFailedBuildCandidate", 0,
                     hjtCreateRightValueEqualsCond(100));
  hjtExpectRows(actual, {
                            "1000|1|10|2000|1|100",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, fullOuterResidualKeepsMatchedBuildNodesUntilDestroy) {
  for (int32_t capacity : {1, 2, 16}) {
    int32_t blockNum = 0;
    std::vector<std::string> actual =
        hjtRunHashJoin(JOIN_TYPE_FULL, JOIN_STYPE_OUTER, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0,
                       &blockNum, "HashJoinExecutor.fullOuterResidualKeepsMatchedBuildNodesUntilDestroy", capacity,
                       hjtCreateRightValueEqualsCond(100), 10, 4);
    hjtExpectRows(actual, {
                              "1000|1|10|2000|1|100",
                              "1001|1|11|2000|1|100",
                              "1002|2|20|NULL|NULL|NULL",
                              "1003|NULL|30|NULL|NULL|NULL",
                              "NULL|NULL|NULL|2001|1|101",
                              "NULL|NULL|NULL|2002|3|300",
                              "NULL|NULL|NULL|2003|NULL|400",
                          });
  }
}

TEST(HashJoinExecutor, rightSemiKeepsMatchedProbeRowsOnce) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_RIGHT, JOIN_STYPE_SEMI, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.rightSemiKeepsMatchedProbeRowsOnce");
  hjtExpectRows(actual, {
                            "1001|1|11|2000|1|100",
                            "1001|1|11|2001|1|101",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, thetaSkipsEmptyInputBlocks) {
  SExecTaskInfo* pTask = hjtCreateTaskInfo("HashJoinExecutor.thetaSkipsEmptyInputBlocks");
  ASSERT_NE(pTask, nullptr);
  auto left = hjtDefaultLeftBlocks();
  auto right = hjtDefaultRightBlocks();
  left.insert(left.begin(), std::vector<SHjtRow>{});
  right.insert(right.begin(), std::vector<SHjtRow>{});
  right.push_back({});
  SOperatorInfo* downstream[] = {
      hjtCreateSourceOperator(HJT_LEFT_BLK_ID, left, pTask),
      hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, right, pTask),
  };
  if (downstream[0] == nullptr || downstream[1] == nullptr) {
    EXPECT_NE(downstream[0], nullptr);
    EXPECT_NE(downstream[1], nullptr);
    if (downstream[0] != nullptr) destroyOperator(downstream[0]);
    if (downstream[1] != nullptr) destroyOperator(downstream[1]);
    taosMemoryFree(pTask);
    return;
  }
  SHashJoinPhysiNode* pNode = hjtCreateHashJoinNode(JOIN_TYPE_INNER, JOIN_STYPE_OUTER, 4, 4, 0);
  if (pNode == nullptr) {
    destroyOperator(downstream[0]);
    destroyOperator(downstream[1]);
    taosMemoryFree(pTask);
    return;
  }
  pNode->leftPrimSlotId = HJT_LEFT_BLK_ID;
  pNode->rightPrimSlotId = HJT_RIGHT_BLK_ID;
  pNode->pFullOnCond = hjtCreateRightValueEqualsCond(100);
  SOperatorInfo* pJoin = nullptr;
  if (createThetaJoinOperatorInfo(downstream, 2, pNode, pTask, &pJoin) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "failed to create theta join operator";
    nodesDestroyNode((SNode*)pNode);
    taosMemoryFree(pTask);
    return;
  }
  int32_t blockNum = 0;
  hjtExpectRows(hjtDrain(pJoin, &blockNum), {
      "1000|1|10|2000|1|100", "1001|1|11|2000|1|100",
      "1002|2|20|2000|1|100", "1003|NULL|30|2000|1|100",
  });
  destroyOperator(pJoin);
  nodesDestroyNode((SNode*)pNode);
  taosMemoryFree(pTask);
}

TEST(HashJoinExecutor, thetaEmitsMultipleUnmatchedRowsAcrossCalls) {
  SExecTaskInfo* pTask = hjtCreateTaskInfo("HashJoinExecutor.thetaEmitsMultipleUnmatchedRowsAcrossCalls");
  ASSERT_NE(pTask, nullptr);
  std::vector<std::vector<SHjtRow>> left = {{{1000, 1, 10, false}, {1001, 2, 20, false}}};
  std::vector<std::vector<SHjtRow>> right = {{{2000, 3, 300, false}}};
  SOperatorInfo* downstream[] = {
      hjtCreateSourceOperator(HJT_LEFT_BLK_ID, left, pTask),
      hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, right, pTask),
  };
  if (downstream[0] == nullptr || downstream[1] == nullptr) {
    EXPECT_NE(downstream[0], nullptr);
    EXPECT_NE(downstream[1], nullptr);
    if (downstream[0] != nullptr) destroyOperator(downstream[0]);
    if (downstream[1] != nullptr) destroyOperator(downstream[1]);
    taosMemoryFree(pTask);
    return;
  }
  SHashJoinPhysiNode* pNode = hjtCreateHashJoinNode(JOIN_TYPE_FULL, JOIN_STYPE_OUTER, 2, 1, 0);
  if (pNode == nullptr) {
    destroyOperator(downstream[0]);
    destroyOperator(downstream[1]);
    taosMemoryFree(pTask);
    return;
  }
  pNode->leftPrimSlotId = HJT_LEFT_BLK_ID;
  pNode->rightPrimSlotId = HJT_RIGHT_BLK_ID;
  pNode->pFullOnCond = hjtCreateRightValueEqualsCond(100);
  SOperatorInfo* pJoin = nullptr;
  if (createThetaJoinOperatorInfo(downstream, 2, pNode, pTask, &pJoin) != TSDB_CODE_SUCCESS) {
    ADD_FAILURE() << "failed to create theta join operator";
    nodesDestroyNode((SNode*)pNode);
    taosMemoryFree(pTask);
    return;
  }
  int32_t blockNum = 0;
  hjtExpectRows(hjtDrain(pJoin, &blockNum), {
      "1000|1|10|NULL|NULL|NULL",
      "1001|2|20|NULL|NULL|NULL",
      "NULL|NULL|NULL|2000|3|300",
  });
  EXPECT_EQ(blockNum, 3);
  destroyOperator(pJoin);
  nodesDestroyNode((SNode*)pNode);
  taosMemoryFree(pTask);
}

TEST(HashJoinExecutor, leftAntiKeepsUnmatchedProbeRows) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_LEFT, JOIN_STYPE_ANTI, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.leftAntiKeepsUnmatchedProbeRows");
  hjtExpectRows(actual, {
                            "1002|2|20|NULL|NULL|NULL",
                            "1003|NULL|30|NULL|NULL|NULL",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, rightAntiKeepsUnmatchedProbeRows) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_RIGHT, JOIN_STYPE_ANTI, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.rightAntiKeepsUnmatchedProbeRows");
  hjtExpectRows(actual, {
                            "NULL|NULL|NULL|2002|3|300",
                            "NULL|NULL|NULL|2003|NULL|400",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, fullOuterDuplicateAndUnmatchedRows) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_FULL, JOIN_STYPE_OUTER, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.fullOuterDuplicateAndUnmatchedRows");
  std::vector<std::string> expected = {
      "1000|1|10|2000|1|100",
      "1000|1|10|2001|1|101",
      "1001|1|11|2000|1|100",
      "1001|1|11|2001|1|101",
      "1002|2|20|NULL|NULL|NULL",
      "1003|NULL|30|NULL|NULL|NULL",
      "NULL|NULL|NULL|2002|3|300",
      "NULL|NULL|NULL|2003|NULL|400",
  };

  hjtExpectRows(actual, expected);
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, fullOuterCopiesDuplicateUnmatchedBuildKeys) {
  std::vector<std::vector<SHjtRow>> leftBlocks = {{
      {1000, 1, 10, false},
      {1001, 1, 11, false},
      {1002, 1, 12, false},
  }};
  std::vector<std::vector<SHjtRow>> rightBlocks = {{{2000, 1, 999, false}}};

  int32_t blockNum = 0;
  std::vector<std::string> actual = hjtRunHashJoin(
      JOIN_TYPE_FULL, JOIN_STYPE_OUTER, leftBlocks, rightBlocks, 0, &blockNum,
      "HashJoinExecutor.fullOuterCopiesDuplicateUnmatchedBuildKeys", 0, hjtCreateRightValueEqualsCond(100), 1, 10);
  hjtExpectRows(actual, {
                            "1000|1|10|NULL|NULL|NULL",
                            "1001|1|11|NULL|NULL|NULL",
                            "1002|1|12|NULL|NULL|NULL",
                            "NULL|NULL|NULL|2000|1|999",
                        });
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, fullOuterSplitsRowsAcrossSmallResultBlocks) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_FULL, JOIN_STYPE_OUTER, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 0, &blockNum,
                     "HashJoinExecutor.fullOuterSplitsRowsAcrossSmallResultBlocks", 2);
  std::vector<std::string> expected = {
      "1000|1|10|2000|1|100",
      "1000|1|10|2001|1|101",
      "1001|1|11|2000|1|100",
      "1001|1|11|2001|1|101",
      "1002|2|20|NULL|NULL|NULL",
      "1003|NULL|30|NULL|NULL|NULL",
      "NULL|NULL|NULL|2002|3|300",
      "NULL|NULL|NULL|2003|NULL|400",
  };

  hjtExpectRows(actual, expected);
  EXPECT_GT(blockNum, 1);
}

TEST(HashJoinExecutor, fullOuterLimitStopsAfterResultLimitReached) {
  int32_t blockNum = 0;
  std::vector<std::string> actual =
      hjtRunHashJoin(JOIN_TYPE_FULL, JOIN_STYPE_OUTER, hjtDefaultLeftBlocks(), hjtDefaultRightBlocks(), 2, &blockNum,
                     "HashJoinExecutor.fullOuterLimitStopsAfterResultLimitReached");

  EXPECT_EQ(actual.size(), 2);
  EXPECT_EQ(blockNum, 1);
}

TEST(HashJoinExecutor, fullOuterLimitStopsDuringUnmatchedBuildBatch) {
  std::vector<std::vector<SHjtRow>> leftBlocks = {{
      {1000, 1, 10, false},
      {1001, 1, 11, false},
      {1002, 1, 12, false},
      {1003, 1, 13, false},
      {1004, 2, 20, false},
  }};
  std::vector<std::vector<SHjtRow>> rightBlocks = {{{2000, 2, 200, false}}};

  int32_t blockNum = 0;
  std::vector<std::string> actual = hjtRunHashJoin(
      JOIN_TYPE_FULL, JOIN_STYPE_OUTER, leftBlocks, rightBlocks, 2, &blockNum,
      "HashJoinExecutor.fullOuterLimitStopsDuringUnmatchedBuildBatch", 0, nullptr, 1, 10);

  ASSERT_EQ(actual.size(), 2);
  EXPECT_NE(std::find(actual.begin(), actual.end(), "1004|2|20|2000|2|200"), actual.end());
  EXPECT_EQ(blockNum, 2);
}

TEST(HashJoinExecutor, buildSideErrorPropagatesAndOperatorCanBeDestroyed) {
  SExecTaskInfo* pTask = hjtCreateTaskInfo("HashJoinExecutor.buildSideErrorPropagatesAndOperatorCanBeDestroyed");
  SOperatorInfo* pLeft = hjtCreateSourceOperator(HJT_LEFT_BLK_ID, hjtDefaultLeftBlocks(), pTask);
  SOperatorInfo* pRight =
      hjtCreateFailingSourceOperator(HJT_RIGHT_BLK_ID, hjtDefaultRightBlocks(), pTask, 0, TSDB_CODE_TSC_INTERNAL_ERROR);
  SOperatorInfo* downstream[] = {pLeft, pRight};
  SHashJoinPhysiNode* pJoinNode = hjtCreateHashJoinNode(JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, 4, 4, 0);

  SOperatorInfo* pJoin = nullptr;
  ASSERT_EQ(createHashJoinOperatorInfo(downstream, 2, pJoinNode, pTask, &pJoin), TSDB_CODE_SUCCESS);
  ASSERT_NE(pJoin, nullptr);

  hjtExpectHashJoinJumpCode(pJoin, pTask, TSDB_CODE_TSC_INTERNAL_ERROR);

  destroyOperator(pJoin);
  nodesDestroyNode(reinterpret_cast<SNode*>(pJoinNode));
  taosMemoryFree(pTask);
}

TEST(HashJoinExecutor, probeSideErrorPropagatesAndOperatorCanBeDestroyed) {
  SExecTaskInfo* pTask = hjtCreateTaskInfo("HashJoinExecutor.probeSideErrorPropagatesAndOperatorCanBeDestroyed");
  SOperatorInfo* pLeft =
      hjtCreateFailingSourceOperator(HJT_LEFT_BLK_ID, hjtDefaultLeftBlocks(), pTask, 0, TSDB_CODE_TSC_INTERNAL_ERROR);
  SOperatorInfo* pRight = hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, hjtDefaultRightBlocks(), pTask);
  SOperatorInfo* downstream[] = {pLeft, pRight};
  SHashJoinPhysiNode* pJoinNode = hjtCreateHashJoinNode(JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, 4, 4, 0);

  SOperatorInfo* pJoin = nullptr;
  ASSERT_EQ(createHashJoinOperatorInfo(downstream, 2, pJoinNode, pTask, &pJoin), TSDB_CODE_SUCCESS);
  ASSERT_NE(pJoin, nullptr);

  hjtExpectHashJoinJumpCode(pJoin, pTask, TSDB_CODE_TSC_INTERNAL_ERROR);

  destroyOperator(pJoin);
  nodesDestroyNode(reinterpret_cast<SNode*>(pJoinNode));
  taosMemoryFree(pTask);
}

TEST(HashJoinExecutor, killedTaskPropagatesAndOperatorCanBeDestroyed) {
  SExecTaskInfo* pTask = hjtCreateTaskInfo("HashJoinExecutor.killedTaskPropagatesAndOperatorCanBeDestroyed");
  SOperatorInfo* pLeft = hjtCreateSourceOperator(HJT_LEFT_BLK_ID, hjtDefaultLeftBlocks(), pTask);
  SOperatorInfo* pRight = hjtCreateSourceOperator(HJT_RIGHT_BLK_ID, hjtDefaultRightBlocks(), pTask);
  SOperatorInfo* downstream[] = {pLeft, pRight};
  SHashJoinPhysiNode* pJoinNode = hjtCreateHashJoinNode(JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, 4, 4, 0);

  SOperatorInfo* pJoin = nullptr;
  ASSERT_EQ(createHashJoinOperatorInfo(downstream, 2, pJoinNode, pTask, &pJoin), TSDB_CODE_SUCCESS);
  ASSERT_NE(pJoin, nullptr);

  pTask->code = TSDB_CODE_TSC_QUERY_KILLED;
  hjtExpectHashJoinJumpCode(pJoin, pTask, TSDB_CODE_TSC_QUERY_KILLED);

  destroyOperator(pJoin);
  nodesDestroyNode(reinterpret_cast<SNode*>(pJoinNode));
  taosMemoryFree(pTask);
}

}  // namespace

int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
