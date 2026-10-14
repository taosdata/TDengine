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

#include <csetjmp>
#include <cstdint>
#include <cstring>
#include <tuple>
#include <utility>
#include <vector>

#include "executorInt.h"
#include "hashjoin.h"
#include "nodes.h"
#include "operator.h"
#include "querytask.h"
#include "tdatablock.h"

namespace {

constexpr int64_t kLeftBlockId = 101;
constexpr int64_t kRightBlockId = 102;
constexpr int64_t kOutputBlockId = 201;

struct InputRow {
  int64_t ts;
  int32_t key;
  int32_t payload;
  bool    keyIsNull;
};

struct OutputRow {
  SBlockID id;
  int64_t  ts;
  int32_t  key;
  int32_t  leftPayload;
  int32_t  rightPayload;
};

struct InputBlockSpec {
  SSDataBlock*                             block;
  std::vector<std::pair<int32_t, int32_t>> lineage;
};

struct MockInputBlock {
  SSDataBlock* block;
  SArray*      lineage;
};

struct PullEvent {
  int32_t  side;
  uint64_t gid;
  bool     eog;
};

struct MockInputInfo {
  SArray*                 blocks;
  int32_t                 nextBlock;
  int32_t                 side;
  std::vector<PullEvent>* events;
  SHJoinOperatorInfo*     joinToExhaust;
  int32_t                 exhaustBeforeBlock;
};

struct ProtocolEvent {
  uint64_t                                 gid;
  int32_t                                  rows;
  bool                                     eog;
  std::vector<std::pair<int32_t, int32_t>> lineage;
};

int64_t encodeLineageSegment(int32_t winOutIdx, int32_t rowStartIdx) {
  int64_t value = 0;
  int32_t pair[2] = {winOutIdx, rowStartIdx};
  std::memcpy(&value, pair, sizeof(value));
  return value;
}

std::pair<int32_t, int32_t> decodeLineageSegment(const int64_t* pValue) {
  int32_t pair[2] = {0};
  std::memcpy(pair, pValue, sizeof(pair));
  return {pair[0], pair[1]};
}

SArray* makeLineage(const std::vector<std::pair<int32_t, int32_t>>& segments) {
  SArray* pLineage = taosArrayInit(static_cast<int32_t>(segments.size()), sizeof(int64_t));
  if (pLineage == nullptr) {
    return nullptr;
  }
  for (const auto& segment : segments) {
    int64_t value = encodeLineageSegment(segment.first, segment.second);
    if (taosArrayPush(pLineage, &value) == nullptr) {
      taosArrayDestroy(pLineage);
      return nullptr;
    }
  }
  return pLineage;
}

std::vector<std::pair<int32_t, int32_t>> copyLineage(const SArray* pLineage) {
  std::vector<std::pair<int32_t, int32_t>> result;
  if (pLineage == nullptr) {
    return result;
  }
  for (int32_t i = 0; i < taosArrayGetSize(pLineage); ++i) {
    result.push_back(decodeLineageSegment(static_cast<const int64_t*>(taosArrayGet(pLineage, i))));
  }
  return result;
}

void expectBlockId(const SBlockID& actual, const SBlockID& expected) {
  EXPECT_EQ(actual.uid, expected.uid);
  EXPECT_EQ(actual.blockId, expected.blockId);
  EXPECT_EQ(actual.groupId, expected.groupId);
  EXPECT_EQ(actual.baseGId, expected.baseGId);
}

int32_t mockInputNext(SOperatorInfo* pOperator, SSDataBlock** ppBlock) {
  MockInputInfo* pInput = static_cast<MockInputInfo*>(pOperator->info);
  *ppBlock = nullptr;
  while (pInput->nextBlock < taosArrayGetSize(pInput->blocks)) {
    int32_t         blockIndex = pInput->nextBlock++;
    MockInputBlock* pItem = static_cast<MockInputBlock*>(taosArrayGet(pInput->blocks, blockIndex));
    if (pItem != nullptr && pItem->block != nullptr) {
      if (pInput->joinToExhaust != nullptr && pInput->exhaustBeforeBlock == blockIndex) {
        SBufPageInfo* pPage = static_cast<SBufPageInfo*>(taosArrayGetLast(pInput->joinToExhaust->pRowBufs));
        if (pPage != nullptr) {
          pPage->offset = pPage->pageSize;
        }
      }
      if (pOperator->pTaskInfo->pStreamRuntimeInfo != nullptr) {
        pOperator->pTaskInfo->pStreamRuntimeInfo->funcInfo.pStreamBlkWinIdx = pItem->lineage;
      }
      if (pInput->events != nullptr) {
        pInput->events->push_back({pInput->side, pItem->block->info.id.baseGId, qIsMultiGroupEogBlock(pItem->block)});
      }
      *ppBlock = pItem->block;
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mockInputReset(SOperatorInfo* pOperator) {
  static_cast<MockInputInfo*>(pOperator->info)->nextBlock = 0;
  pOperator->status = OP_NOT_OPENED;
  return TSDB_CODE_SUCCESS;
}

void destroyMockInput(void* param) {
  MockInputInfo* pInput = static_cast<MockInputInfo*>(param);
  if (pInput == nullptr) {
    return;
  }
  if (pInput->blocks != nullptr) {
    for (int32_t i = 0; i < taosArrayGetSize(pInput->blocks); ++i) {
      MockInputBlock* pItem = static_cast<MockInputBlock*>(taosArrayGet(pInput->blocks, i));
      blockDataDestroy(pItem->block);
      taosArrayDestroy(pItem->lineage);
    }
    taosArrayDestroy(pInput->blocks);
  }
  taosMemoryFree(pInput);
}

int32_t makeMockInput(SExecTaskInfo* pTask, int64_t blockId, const std::vector<InputBlockSpec>& blocks, int32_t side,
                      std::vector<PullEvent>* events, SOperatorInfo** ppOperator) {
  MockInputInfo* pInput = static_cast<MockInputInfo*>(taosMemoryCalloc(1, sizeof(MockInputInfo)));
  SOperatorInfo* pOperator = static_cast<SOperatorInfo*>(taosMemoryCalloc(1, sizeof(SOperatorInfo)));
  if (pInput == nullptr || pOperator == nullptr) {
    taosMemoryFree(pInput);
    taosMemoryFree(pOperator);
    return terrno;
  }
  pInput->blocks = taosArrayInit(static_cast<int32_t>(blocks.size()), sizeof(MockInputBlock));
  if (pInput->blocks == nullptr) {
    destroyMockInput(pInput);
    taosMemoryFree(pOperator);
    return terrno;
  }
  pInput->side = side;
  pInput->events = events;
  pInput->exhaustBeforeBlock = -1;
  for (const InputBlockSpec& spec : blocks) {
    MockInputBlock item = {};
    item.block = spec.block;
    item.lineage = makeLineage(spec.lineage);
    if (item.lineage == nullptr || taosArrayPush(pInput->blocks, &item) == nullptr) {
      taosArrayDestroy(item.lineage);
      destroyMockInput(pInput);
      taosMemoryFree(pOperator);
      return terrno;
    }
  }

  pOperator->resultDataBlockId = blockId;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, mockInputNext, nullptr, destroyMockInput, optrDefaultBufFn,
                                         nullptr, optrDefaultGetNextExtFn, nullptr);
  setOperatorResetStateFn(pOperator, mockInputReset);
  setOperatorInfo(pOperator, "HashJoinMultiGroupInput", QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, false, OP_NOT_OPENED,
                  pInput, pTask);
  initOperatorCostInfo(pOperator);
  *ppOperator = pOperator;
  return TSDB_CODE_SUCCESS;
}

int32_t makeInputBlock(const SBlockID& id, const std::vector<InputRow>& rows, SSDataBlock** ppBlock) {
  SSDataBlock* pBlock = nullptr;
  int32_t      code = createDataBlock(&pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData tsCol = createColumnInfoData(TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 1);
  SColumnInfoData keyCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 2);
  SColumnInfoData payloadCol = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 3);
  if ((code = blockDataAppendColInfo(pBlock, &tsCol)) != TSDB_CODE_SUCCESS ||
      (code = blockDataAppendColInfo(pBlock, &keyCol)) != TSDB_CODE_SUCCESS ||
      (code = blockDataAppendColInfo(pBlock, &payloadCol)) != TSDB_CODE_SUCCESS ||
      (code = blockDataEnsureCapacity(pBlock, static_cast<int32_t>(rows.size()))) != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  SColumnInfoData* pTs = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0));
  SColumnInfoData* pKey = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 1));
  SColumnInfoData* pPayload = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 2));
  for (int32_t row = 0; row < static_cast<int32_t>(rows.size()); ++row) {
    const InputRow& value = rows[row];
    if ((code = colDataSetVal(pTs, row, reinterpret_cast<const char*>(&value.ts), false)) != TSDB_CODE_SUCCESS ||
        (code = colDataSetVal(pKey, row, value.keyIsNull ? nullptr : reinterpret_cast<const char*>(&value.key),
                              value.keyIsNull)) != TSDB_CODE_SUCCESS ||
        (code = colDataSetVal(pPayload, row, reinterpret_cast<const char*>(&value.payload), false)) !=
            TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return code;
    }
  }
  pBlock->info.rows = static_cast<int32_t>(rows.size());
  pBlock->info.id = id;
  if (!rows.empty()) {
    code = blockDataUpdateTsWindow(pBlock, 0);
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return code;
    }
  }
  *ppBlock = pBlock;
  return TSDB_CODE_SUCCESS;
}

int32_t appendColumnNode(SNodeList** ppList, int64_t blockId, int16_t slotId, int8_t type, int32_t bytes) {
  SColumnNode* pColumn = nullptr;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pColumn->node.resType.type = type;
  pColumn->node.resType.bytes = bytes;
  pColumn->dataBlockId = blockId;
  pColumn->slotId = slotId;
  pColumn->colId = slotId + 1;
  pColumn->colType = COLUMN_TYPE_COLUMN;
  code = nodesListMakeStrictAppend(ppList, reinterpret_cast<SNode*>(pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pColumn));
  }
  return code;
}

int32_t appendTarget(SHashJoinPhysiNode* pJoin, int64_t inputBlockId, int16_t inputSlot, int8_t type, int32_t bytes,
                     int16_t outputSlot) {
  SColumnNode* pColumn = nullptr;
  STargetNode* pTarget = nullptr;
  int32_t      code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pColumn));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pColumn->node.resType.type = type;
  pColumn->node.resType.bytes = bytes;
  pColumn->dataBlockId = inputBlockId;
  pColumn->slotId = inputSlot;
  pColumn->colId = inputSlot + 1;
  pColumn->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_TARGET, reinterpret_cast<SNode**>(&pTarget));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pTarget->dataBlockId = kOutputBlockId;
  pTarget->slotId = outputSlot;
  pTarget->pExpr = reinterpret_cast<SNode*>(pColumn);
  pColumn = nullptr;
  code = nodesListMakeStrictAppend(&pJoin->pTargets, reinterpret_cast<SNode*>(pTarget));
  if (code == TSDB_CODE_SUCCESS) {
    pTarget = nullptr;
  }

_error:
  nodesDestroyNode(reinterpret_cast<SNode*>(pTarget));
  nodesDestroyNode(reinterpret_cast<SNode*>(pColumn));
  return code;
}

int32_t makeOutputDesc(SDataBlockDescNode** ppDesc) {
  static const int8_t kTypes[] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT};
  static const int32_t kBytes[] = {sizeof(int64_t), sizeof(int32_t), sizeof(int32_t), sizeof(int32_t)};
  SDataBlockDescNode*  pDesc = nullptr;
  int32_t              code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, reinterpret_cast<SNode**>(&pDesc));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pDesc->dataBlockId = kOutputBlockId;
  for (int16_t slot = 0; slot < 4; ++slot) {
    SSlotDescNode* pSlot = nullptr;
    code = nodesMakeNode(QUERY_NODE_SLOT_DESC, reinterpret_cast<SNode**>(&pSlot));
    if (code != TSDB_CODE_SUCCESS) {
      nodesDestroyNode(reinterpret_cast<SNode*>(pDesc));
      return code;
    }
    pSlot->slotId = slot;
    pSlot->dataType.type = kTypes[slot];
    pSlot->dataType.bytes = kBytes[slot];
    code = nodesListMakeStrictAppend(&pDesc->pSlots, reinterpret_cast<SNode*>(pSlot));
    if (code != TSDB_CODE_SUCCESS) {
      nodesDestroyNode(reinterpret_cast<SNode*>(pSlot));
      nodesDestroyNode(reinterpret_cast<SNode*>(pDesc));
      return code;
    }
    pDesc->totalRowSize += kBytes[slot];
  }
  pDesc->outputRowSize = pDesc->totalRowSize;
  *ppDesc = pDesc;
  return TSDB_CODE_SUCCESS;
}

int32_t addPositiveLeftPayloadFilter(SHashJoinPhysiNode* pJoin) {
  SColumnNode*   pPayload = nullptr;
  SValueNode*    pZero = nullptr;
  SOperatorNode* pCondition = nullptr;
  int32_t        zero = 0;
  int32_t        code = nodesMakeNode(QUERY_NODE_COLUMN, reinterpret_cast<SNode**>(&pPayload));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pPayload->node.resType.type = TSDB_DATA_TYPE_INT;
  pPayload->node.resType.bytes = sizeof(int32_t);
  pPayload->dataBlockId = kOutputBlockId;
  pPayload->slotId = 2;
  pPayload->colId = 3;
  pPayload->colType = COLUMN_TYPE_COLUMN;

  code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&pZero));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pZero->node.resType.type = TSDB_DATA_TYPE_INT;
  pZero->node.resType.bytes = sizeof(int32_t);
  code = nodesSetValueNodeValue(pZero, &zero);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pZero->translate = true;

  code = nodesMakeNode(QUERY_NODE_OPERATOR, reinterpret_cast<SNode**>(&pCondition));
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  pCondition->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pCondition->node.resType.bytes = sizeof(bool);
  pCondition->opType = OP_TYPE_GREATER_THAN;
  pCondition->pLeft = reinterpret_cast<SNode*>(pPayload);
  pPayload = nullptr;
  pCondition->pRight = reinterpret_cast<SNode*>(pZero);
  pZero = nullptr;
  pJoin->node.pConditions = reinterpret_cast<SNode*>(pCondition);
  return TSDB_CODE_SUCCESS;

_error:
  nodesDestroyNode(reinterpret_cast<SNode*>(pCondition));
  nodesDestroyNode(reinterpret_cast<SNode*>(pZero));
  nodesDestroyNode(reinterpret_cast<SNode*>(pPayload));
  return code;
}

int32_t addConstantTrueFilter(SHashJoinPhysiNode* pJoin) {
  SValueNode* pTrue = nullptr;
  bool        value = true;
  int32_t     code = nodesMakeNode(QUERY_NODE_VALUE, reinterpret_cast<SNode**>(&pTrue));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pTrue->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pTrue->node.resType.bytes = sizeof(bool);
  code = nodesSetValueNodeValue(pTrue, &value);
  if (code != TSDB_CODE_SUCCESS) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pTrue));
    return code;
  }
  pTrue->translate = true;
  pJoin->node.pConditions = reinterpret_cast<SNode*>(pTrue);
  return TSDB_CODE_SUCCESS;
}

int32_t makeHashJoinNode(int64_t leftBlockId, int64_t rightBlockId, bool withFilter, bool buildTimeRange,
                         bool multiGroupJoin, SHashJoinPhysiNode** ppJoin, bool allQualifiedFilter = false,
                         int64_t leftInputRows = 1, int64_t rightInputRows = 1024) {
  SHashJoinPhysiNode* pJoin = nullptr;
  int32_t             code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, reinterpret_cast<SNode**>(&pJoin));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  pJoin->joinType = JOIN_TYPE_INNER;
  pJoin->subType = JOIN_STYPE_NONE;
  pJoin->multiGroupJoin = multiGroupJoin;
  pJoin->leftPrimSlotId = 0;
  pJoin->rightPrimSlotId = 0;
  if (buildTimeRange) {
    pJoin->timeRangeTarget = 0x1;
    pJoin->timeRange.skey = INT64_MIN;
    pJoin->timeRange.ekey = INT64_MAX;
  }
  pJoin->inputStat[0].inputRowNum = leftInputRows;
  pJoin->inputStat[1].inputRowNum = rightInputRows;
  if ((code = appendColumnNode(&pJoin->pOnLeft, leftBlockId, 0, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t))) !=
          TSDB_CODE_SUCCESS ||
      (code = appendColumnNode(&pJoin->pOnRight, rightBlockId, 0, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t))) !=
          TSDB_CODE_SUCCESS ||
      (code = appendColumnNode(&pJoin->pOnLeft, leftBlockId, 1, TSDB_DATA_TYPE_INT, sizeof(int32_t))) !=
          TSDB_CODE_SUCCESS ||
      (code = appendColumnNode(&pJoin->pOnRight, rightBlockId, 1, TSDB_DATA_TYPE_INT, sizeof(int32_t))) !=
          TSDB_CODE_SUCCESS ||
      (code = appendTarget(pJoin, leftBlockId, 0, TSDB_DATA_TYPE_TIMESTAMP, sizeof(int64_t), 0)) != TSDB_CODE_SUCCESS ||
      (code = appendTarget(pJoin, leftBlockId, 1, TSDB_DATA_TYPE_INT, sizeof(int32_t), 1)) != TSDB_CODE_SUCCESS ||
      (code = appendTarget(pJoin, leftBlockId, 2, TSDB_DATA_TYPE_INT, sizeof(int32_t), 2)) != TSDB_CODE_SUCCESS ||
      (code = appendTarget(pJoin, rightBlockId, 2, TSDB_DATA_TYPE_INT, sizeof(int32_t), 3)) != TSDB_CODE_SUCCESS ||
      (code = makeOutputDesc(&pJoin->node.pOutputDataBlockDesc)) != TSDB_CODE_SUCCESS ||
      (withFilter && (code = addPositiveLeftPayloadFilter(pJoin)) != TSDB_CODE_SUCCESS) ||
      (allQualifiedFilter && (code = addConstantTrueFilter(pJoin)) != TSDB_CODE_SUCCESS)) {
    nodesDestroyNode(reinterpret_cast<SNode*>(pJoin));
    return code;
  }
  *ppJoin = pJoin;
  return TSDB_CODE_SUCCESS;
}

int32_t fetchOne(SOperatorInfo* pJoin, SSDataBlock** ppBlock) {
  SExecTaskInfo* pTask = pJoin->pTaskInfo;
  int32_t        code = setjmp(pTask->env);
  if (code != TSDB_CODE_SUCCESS) {
    *ppBlock = nullptr;
    return code;
  }
  return pJoin->fpSet.getNextFn(pJoin, ppBlock);
}

int32_t appendOutputRows(const SSDataBlock* pBlock, std::vector<OutputRow>* pRows) {
  if (pBlock == nullptr) {
    return TSDB_CODE_SUCCESS;
  }
  SColumnInfoData* pTs = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 0));
  SColumnInfoData* pKey = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 1));
  SColumnInfoData* pLeft = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 2));
  SColumnInfoData* pRight = static_cast<SColumnInfoData*>(taosArrayGet(pBlock->pDataBlock, 3));
  if (pTs == nullptr || pKey == nullptr || pLeft == nullptr || pRight == nullptr) {
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }
  for (int32_t row = 0; row < pBlock->info.rows; ++row) {
    OutputRow output;
    output.id = pBlock->info.id;
    output.ts = *reinterpret_cast<int64_t*>(colDataGetData(pTs, row));
    output.key = *reinterpret_cast<int32_t*>(colDataGetData(pKey, row));
    output.leftPayload = *reinterpret_cast<int32_t*>(colDataGetData(pLeft, row));
    output.rightPayload = *reinterpret_cast<int32_t*>(colDataGetData(pRight, row));
    pRows->push_back(output);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t fetchAll(SOperatorInfo* pJoin, std::vector<OutputRow>* pRows) {
  for (int32_t call = 0; call < 100; ++call) {
    SSDataBlock* pBlock = nullptr;
    int32_t      code = fetchOne(pJoin, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pBlock == nullptr) {
      return TSDB_CODE_SUCCESS;
    }
    code = appendOutputRows(pBlock, pRows);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

int32_t fetchProtocol(SOperatorInfo* pJoin, std::vector<ProtocolEvent>* pProtocol, std::vector<OutputRow>* pRows) {
  for (int32_t call = 0; call < 1000; ++call) {
    SSDataBlock* pBlock = nullptr;
    int32_t      code = fetchOne(pJoin, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    if (pBlock == nullptr) {
      return TSDB_CODE_SUCCESS;
    }
    SStreamRuntimeFuncInfo* pRuntime = GET_STM_RTINFO(pJoin->pTaskInfo);
    pProtocol->push_back({pBlock->info.id.baseGId, static_cast<int32_t>(pBlock->info.rows),
                          qIsMultiGroupEogBlock(pBlock),
                          copyLineage(pRuntime == nullptr ? nullptr : pRuntime->pStreamBlkWinIdx)});
    code = appendOutputRows(pBlock, pRows);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
}

class HashJoinHarness {
 public:
  HashJoinHarness() {
    task_ = static_cast<SExecTaskInfo*>(taosMemoryCalloc(1, sizeof(SExecTaskInfo)));
    if (task_ != nullptr) {
      task_->id.str = const_cast<char*>("hash-join-multi-group-test");
    }
  }

  ~HashJoinHarness() {
    hJoinTestResetFailureHooks();
    destroyOperator(join_);
    nodesDestroyNode(reinterpret_cast<SNode*>(plan_));
    taosArrayDestroy(runtime_.funcInfo.pGroupCalcOrder);
    runtime_.funcInfo.pGroupCalcOrder = nullptr;
    runtime_.funcInfo.pStreamBlkWinIdx = nullptr;
    taosMemoryFree(task_);
  }

  int32_t Create(const std::vector<InputBlockSpec>& leftBlocks, const std::vector<InputBlockSpec>& rightBlocks,
                 const std::vector<uint64_t>& groupOrder, bool withFilter = false, int64_t leftBlockId = kLeftBlockId,
                 int64_t rightBlockId = kRightBlockId, bool buildTimeRange = false, bool physicalMultiGroup = true,
                 bool runtimeMultiGroup = true, bool streamExec = true, bool runtimePresent = true,
                 bool allQualifiedFilter = false, int64_t leftInputRows = 1, int64_t rightInputRows = 1024) {
    if (task_ == nullptr) {
      return terrno;
    }
    task_->execModel = streamExec ? OPTR_EXEC_MODEL_STREAM : OPTR_EXEC_MODEL_BATCH;
    task_->pStreamRuntimeInfo = runtimePresent ? &runtime_ : nullptr;
    runtime_.funcInfo.isMultiGroupCalc = runtimeMultiGroup;
    if (runtimePresent) {
      runtime_.funcInfo.pGroupCalcOrder = taosArrayInit(static_cast<int32_t>(groupOrder.size()), sizeof(uint64_t));
      if (runtime_.funcInfo.pGroupCalcOrder == nullptr) {
        return terrno;
      }
      for (uint64_t gid : groupOrder) {
        if (taosArrayPush(runtime_.funcInfo.pGroupCalcOrder, &gid) == nullptr) {
          return terrno;
        }
      }
    }
    SOperatorInfo* downstreams[2] = {nullptr, nullptr};
    int32_t        code = makeMockInput(task_, leftBlockId, leftBlocks, 0, &pulls_, &downstreams[0]);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    code = makeMockInput(task_, rightBlockId, rightBlocks, 1, &pulls_, &downstreams[1]);
    if (code != TSDB_CODE_SUCCESS) {
      destroyOperator(downstreams[0]);
      return code;
    }
    code = makeHashJoinNode(leftBlockId, rightBlockId, withFilter, buildTimeRange, physicalMultiGroup, &plan_,
                            allQualifiedFilter, leftInputRows, rightInputRows);
    if (code != TSDB_CODE_SUCCESS) {
      destroyOperator(downstreams[0]);
      destroyOperator(downstreams[1]);
      return code;
    }
    code = createHashJoinOperatorInfo(downstreams, 2, plan_, task_, &join_);
    if (code == TSDB_CODE_SUCCESS) {
      join_->resultDataBlockId = kOutputBlockId;
      task_->pRoot = join_;
    }
    return code;
  }

  int32_t CreateLegacy(const std::vector<InputBlockSpec>& leftBlocks, const std::vector<InputBlockSpec>& rightBlocks,
                       bool withFilter = false) {
    return Create(leftBlocks, rightBlocks, {}, withFilter, kLeftBlockId, kRightBlockId, false, false, false, false,
                  false);
  }

  void SetOutputCapacity(int32_t capacity, int32_t threshold) {
    SHJoinOperatorInfo* pInfo = info();
    pInfo->finBlk->info.capacity = capacity;
    if (pInfo->midBlk != nullptr) {
      pInfo->midBlk->info.capacity = capacity;
    }
    pInfo->blkThreshold = threshold;
  }

  void ExhaustCurrentRowPageBeforeBuildBlock(int32_t blockIndex) {
    MockInputInfo* pInput = static_cast<MockInputInfo*>(join_->pDownstream[info()->pBuild->downStreamIdx]->info);
    pInput->joinToExhaust = info();
    pInput->exhaustBeforeBlock = blockIndex;
  }

  SSDataBlock* DetachRightBlock(int32_t index) {
    MockInputInfo*  pInput = static_cast<MockInputInfo*>(join_->pDownstream[1]->info);
    MockInputBlock* pItem = static_cast<MockInputBlock*>(taosArrayGet(pInput->blocks, index));
    SSDataBlock*    pBlock = pItem->block;
    pItem->block = nullptr;
    return pBlock;
  }

  int32_t ResetTree() {
    int32_t code = join_->fpSet.resetStateFn(join_);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    for (int32_t i = 0; i < 2; ++i) {
      code = join_->pDownstream[i]->fpSet.resetStateFn(join_->pDownstream[i]);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    task_->code = TSDB_CODE_SUCCESS;
    return TSDB_CODE_SUCCESS;
  }

  void ClearRuntimeForReset() {
    runtime_.funcInfo.pStreamBlkWinIdx = nullptr;
    runtime_.funcInfo.isMultiGroupCalc = false;
    taosArrayDestroy(runtime_.funcInfo.pGroupCalcOrder);
    runtime_.funcInfo.pGroupCalcOrder = nullptr;
  }

  int32_t InstallRuntime(const std::vector<uint64_t>& groupOrder) {
    runtime_.funcInfo.pGroupCalcOrder = taosArrayInit(static_cast<int32_t>(groupOrder.size()), sizeof(uint64_t));
    if (runtime_.funcInfo.pGroupCalcOrder == nullptr) {
      return terrno;
    }
    for (uint64_t gid : groupOrder) {
      if (taosArrayPush(runtime_.funcInfo.pGroupCalcOrder, &gid) == nullptr) {
        return terrno;
      }
    }
    runtime_.funcInfo.isMultiGroupCalc = true;
    task_->pStreamRuntimeInfo = &runtime_;
    return TSDB_CODE_SUCCESS;
  }

  SOperatorInfo*                join() const { return join_; }
  SHJoinOperatorInfo*           info() const { return static_cast<SHJoinOperatorInfo*>(join_->info); }
  SExecTaskInfo*                task() const { return task_; }
  SStreamRuntimeFuncInfo*       runtime() { return &runtime_.funcInfo; }
  const std::vector<PullEvent>& pulls() const { return pulls_; }
  SOperatorInfo*                ReleaseJoin() {
    SOperatorInfo* pJoin = join_;
    join_ = nullptr;
    return pJoin;
  }

 private:
  SExecTaskInfo*         task_ = nullptr;
  SStreamRuntimeInfo     runtime_ = {};
  SHashJoinPhysiNode*    plan_ = nullptr;
  SOperatorInfo*         join_ = nullptr;
  std::vector<PullEvent> pulls_;
};

SBlockID blockId(uint64_t uid, int64_t block, uint64_t group, uint64_t baseGroup) {
  SBlockID id = {0};
  id.uid = uid;
  id.blockId = block;
  id.groupId = group;
  id.baseGId = baseGroup;
  return id;
}

SSDataBlock* inputBlock(const SBlockID& id, const std::vector<InputRow>& rows) {
  SSDataBlock* pBlock = nullptr;
  EXPECT_EQ(makeInputBlock(id, rows, &pBlock), TSDB_CODE_SUCCESS);
  return pBlock;
}

InputBlockSpec dataBlock(const SBlockID& id, const std::vector<InputRow>& rows,
                         std::vector<std::pair<int32_t, int32_t>> lineage = {{1, 0}}) {
  InputBlockSpec spec = {};
  spec.block = inputBlock(id, rows);
  spec.lineage = std::move(lineage);
  return spec;
}

InputBlockSpec eogBlock(uint64_t gid) {
  SSDataBlock* pBlock = inputBlock(blockId(0, 0, gid, gid), {});
  EXPECT_EQ(qSetMultiGroupEogBlock(pBlock, gid), TSDB_CODE_SUCCESS);
  InputBlockSpec spec = {};
  spec.block = pBlock;
  return spec;
}

void appendGroup(std::vector<InputBlockSpec>* pBlocks, const SBlockID& id, const std::vector<InputRow>& rows,
                 std::vector<std::pair<int32_t, int32_t>> lineage = {{1, 0}}) {
  if (!rows.empty()) {
    pBlocks->push_back(dataBlock(id, rows, std::move(lineage)));
  }
  pBlocks->push_back(eogBlock(id.baseGId));
}

void expectProtocol(const std::vector<ProtocolEvent>&                       actual,
                    const std::vector<std::tuple<uint64_t, int32_t, bool>>& expected) {
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i].gid, std::get<0>(expected[i])) << i;
    EXPECT_EQ(actual[i].rows, std::get<1>(expected[i])) << i;
    EXPECT_EQ(actual[i].eog, std::get<2>(expected[i])) << i;
  }
}

}  // namespace

TEST(HashJoinMultiGroupTest, FactoryLeavesPhysicalNodeOwnershipWithParentPlan) {
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create({eogBlock(101)}, {eogBlock(101)}, {101}), TSDB_CODE_SUCCESS);
  ASSERT_NE(harness.join(), nullptr);
  EXPECT_EQ(harness.join()->operatorType, QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN);
  EXPECT_EQ(harness.join()->pPhyNode, nullptr);
  EXPECT_TRUE(harness.info()->multiGroupJoin);
}

TEST(HashJoinMultiGroupTest, BuildsAndProbesOneCanonicalGroupAtATime) {
  const std::vector<uint64_t> order = {101, 202, 303};
  std::vector<InputBlockSpec> left;
  std::vector<InputBlockSpec> right;
  for (uint64_t gid : order) {
    appendGroup(&left, blockId(1000 + gid, 10 + gid, gid, gid), {{10, 1, static_cast<int32_t>(gid), false}});
    appendGroup(&right, blockId(2000 + gid, 20 + gid, gid, gid), {{10, 1, static_cast<int32_t>(gid + 1), false}},
                {{static_cast<int32_t>(gid), 0}});
  }
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, order), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol,
                 {{101, 1, false}, {101, 0, true}, {202, 1, false}, {202, 0, true}, {303, 1, false}, {303, 0, true}});
  ASSERT_EQ(rows.size(), 3U);
  ASSERT_EQ(harness.pulls().size(), 12U);
  for (size_t group = 0; group < order.size(); ++group) {
    const size_t offset = group * 4;
    EXPECT_EQ(harness.pulls()[offset].side, 0);
    EXPECT_FALSE(harness.pulls()[offset].eog);
    EXPECT_EQ(harness.pulls()[offset + 1].side, 0);
    EXPECT_TRUE(harness.pulls()[offset + 1].eog);
    EXPECT_EQ(harness.pulls()[offset + 2].side, 1);
    EXPECT_FALSE(harness.pulls()[offset + 2].eog);
    EXPECT_EQ(harness.pulls()[offset + 3].side, 1);
    EXPECT_TRUE(harness.pulls()[offset + 3].eog);
  }
}

TEST(HashJoinMultiGroupTest, EmptyBuildStillConsumesProbeAndEmitsEog) {
  std::vector<InputBlockSpec> left = {eogBlock(101)};
  std::vector<InputBlockSpec> right;
  appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 9, false}}, {{7, 0}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 0, true}});
  EXPECT_TRUE(rows.empty());
  ASSERT_EQ(harness.pulls().size(), 3U);
  EXPECT_EQ(harness.pulls()[1].side, 1);
  EXPECT_FALSE(harness.pulls()[1].eog);

  harness.ClearRuntimeForReset();
  hJoinTestFailHashInitOnce();
  ASSERT_EQ(harness.ResetTree(), TSDB_CODE_SUCCESS);
  EXPECT_EQ(harness.info()->pKeyHash, nullptr);
  EXPECT_EQ(harness.info()->pRowBufs, nullptr);
  EXPECT_EQ(harness.info()->hashOwnedBytes, 0U);

  ASSERT_EQ(harness.InstallRuntime({101}), TSDB_CODE_SUCCESS);
  protocol.clear();
  rows.clear();
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 0, true}});
  EXPECT_EQ(harness.info()->pKeyHash, nullptr);
  EXPECT_EQ(harness.info()->pRowBufs, nullptr);
  EXPECT_EQ(harness.info()->hashOwnedBytes, 0U);
}

TEST(HashJoinMultiGroupTest, EmptyProbeEmitsEogAndReleasesBuildState) {
  std::vector<InputBlockSpec> left;
  appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
  std::vector<InputBlockSpec> right = {eogBlock(101)};
  HashJoinHarness             harness;
  ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 0, true}});
  EXPECT_EQ(harness.info()->pKeyHash, nullptr);
  EXPECT_TRUE(harness.info()->pRowBufs == nullptr || taosArrayGetSize(harness.info()->pRowBufs) == 0);
  EXPECT_EQ(harness.info()->hashOwnedBytes, 0U);
}

TEST(HashJoinMultiGroupTest, BothSidesEmptyEmitOneEog) {
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create({eogBlock(101)}, {eogBlock(101)}, {101}), TSDB_CODE_SUCCESS);
  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 0, true}});
  EXPECT_TRUE(rows.empty());
}

TEST(HashJoinMultiGroupTest, MiddleEmptyGroupDoesNotStopLaterGroups) {
  const std::vector<uint64_t> order = {101, 202, 303};
  std::vector<InputBlockSpec> left;
  std::vector<InputBlockSpec> right;
  appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 101, false}});
  left.push_back(eogBlock(202));
  appendGroup(&left, blockId(3, 3, 303, 303), {{30, 3, 303, false}});
  appendGroup(&right, blockId(4, 4, 101, 101), {{10, 1, 11, false}}, {{7, 0}});
  right.push_back(eogBlock(202));
  appendGroup(&right, blockId(6, 6, 303, 303), {{30, 3, 33, false}}, {{9, 0}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, order), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 1, false}, {101, 0, true}, {202, 0, true}, {303, 1, false}, {303, 0, true}});
  ASSERT_EQ(rows.size(), 2U);
  EXPECT_EQ(rows[1].leftPayload, 303);
}

TEST(HashJoinMultiGroupTest, EogWaitsForThresholdAndRemainsOutput) {
  std::vector<InputBlockSpec> left;
  appendGroup(&left, blockId(1, 1, 101, 101),
              {{10, 1, 1, false}, {10, 1, 2, false}, {10, 1, 3, false}, {10, 1, 4, false}, {10, 1, 5, false}});
  std::vector<InputBlockSpec> right;
  appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 9, false}}, {{7, 0}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);
  harness.SetOutputCapacity(2, 2);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 2, false}, {101, 2, false}, {101, 1, false}, {101, 0, true}});
  ASSERT_EQ(rows.size(), 5U);
  for (size_t i = 0; i + 1 < protocol.size(); ++i) {
    EXPECT_FALSE(protocol[i].eog);
  }
}

TEST(HashJoinMultiGroupTest, RejectsBuildProbeOrderMismatch) {
  std::vector<InputBlockSpec> left;
  std::vector<InputBlockSpec> right;
  appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
  appendGroup(&left, blockId(2, 2, 202, 202), {{20, 2, 2, false}});
  appendGroup(&right, blockId(3, 3, 202, 202), {{20, 2, 3, false}});
  appendGroup(&right, blockId(4, 4, 101, 101), {{10, 1, 4, false}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101, 202}), TSDB_CODE_SUCCESS);
  SSDataBlock* pBlock = nullptr;
  EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  EXPECT_EQ(pBlock, nullptr);
}

TEST(HashJoinMultiGroupTest, RejectsDuplicateOrMissingEog) {
  {
    std::vector<InputBlockSpec> left;
    appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
    left.push_back(eogBlock(101));
    appendGroup(&left, blockId(2, 2, 202, 202), {{20, 2, 2, false}});
    std::vector<InputBlockSpec> right;
    appendGroup(&right, blockId(3, 3, 101, 101), {{10, 1, 3, false}});
    appendGroup(&right, blockId(4, 4, 202, 202), {{20, 2, 4, false}});
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101, 202}), TSDB_CODE_SUCCESS);
    std::vector<ProtocolEvent> protocol;
    std::vector<OutputRow>     rows;
    EXPECT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    std::vector<InputBlockSpec> left = {dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}})};
    appendGroup(&left, blockId(2, 2, 202, 202), {{20, 2, 2, false}});
    std::vector<InputBlockSpec> right;
    appendGroup(&right, blockId(3, 3, 101, 101), {{10, 1, 3, false}});
    appendGroup(&right, blockId(4, 4, 202, 202), {{20, 2, 4, false}});
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101, 202}), TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    std::vector<InputBlockSpec> left;
    appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
    left.push_back(eogBlock(101));
    std::vector<InputBlockSpec> right;
    appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 2, false}});
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);
    std::vector<ProtocolEvent> protocol;
    std::vector<OutputRow>     rows;
    EXPECT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    std::vector<InputBlockSpec> left;
    appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
    std::vector<InputBlockSpec> right;
    appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 2, false}});
    right.push_back(eogBlock(101));
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);
    std::vector<ProtocolEvent> protocol;
    std::vector<OutputRow>     rows;
    EXPECT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
}

TEST(HashJoinMultiGroupTest, RejectsEarlyChildEof) {
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}})},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}),
              TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}})}, {101}),
              TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
}

TEST(HashJoinMultiGroupTest, RejectsStaticRuntimeModeMismatch) {
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, false, false, false),
              TSDB_CODE_SUCCESS);
    ASSERT_NE(harness.info()->pKeyHash, nullptr);
    std::vector<OutputRow> rows;
    EXPECT_EQ(fetchAll(harness.join(), &rows), TSDB_CODE_SUCCESS);
    EXPECT_EQ(rows.size(), 1U);
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, true, true, false),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(harness.info()->pKeyHash, nullptr);
    ASSERT_EQ(harness.info()->pRowBufs, nullptr);
    std::vector<ProtocolEvent> protocol;
    std::vector<OutputRow>     rows;
    EXPECT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
    expectProtocol(protocol, {{101, 1, false}, {101, 0, true}});
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, true, false, false),
              TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, false, true, false),
              TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, true, false, true),
              TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, false, true, true),
              TSDB_CODE_SUCCESS);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
  {
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                             {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                             kLeftBlockId, kRightBlockId, false, true, false, false, false),
              TSDB_CODE_SUCCESS);
    ASSERT_EQ(harness.info()->pKeyHash, nullptr);
    ASSERT_EQ(harness.info()->pRowBufs, nullptr);
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
  }
}

TEST(HashJoinMultiGroupTest, StreamLegacyBothFalseAllocatesEagerState) {
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create({dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}), eogBlock(101)},
                           {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 2, false}}), eogBlock(101)}, {101}, false,
                           kLeftBlockId, kRightBlockId, false, false, false, true, true),
            TSDB_CODE_SUCCESS);
  ASSERT_NE(harness.info()->pKeyHash, nullptr);
  ASSERT_NE(harness.info()->pRowBufs, nullptr);

  std::vector<OutputRow> rows;
  ASSERT_EQ(fetchAll(harness.join(), &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0].leftPayload, 1);
  EXPECT_EQ(rows[0].rightPayload, 2);
}

TEST(HashJoinMultiGroupTest, DuplicateMatchesCopyProbeLineage) {
  std::vector<InputBlockSpec> left;
  appendGroup(&left, blockId(1, 1, 101, 101),
              {{10, 1, 11, false}, {10, 1, 12, false}, {20, 2, 21, false}, {20, 2, 22, false}, {20, 2, 23, false}});
  std::vector<InputBlockSpec> right;
  appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 101, false}, {10, 1, 102, false}, {20, 2, 201, false}},
              {{7, 0}, {8, 2}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 7, false}, {101, 0, true}});
  ASSERT_EQ(protocol[0].lineage, (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 4}}));
  ASSERT_EQ(rows.size(), 7U);
}

TEST(HashJoinMultiGroupTest, FinalFilterRebuildsOutputLineage) {
  std::vector<InputBlockSpec> left;
  appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, -1, false}, {10, 1, 2, false}, {10, 1, 3, false}});
  std::vector<InputBlockSpec> right;
  appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 9, false}}, {{7, 0}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101}, true), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 2, false}, {101, 0, true}});
  EXPECT_EQ(protocol[0].lineage, (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
  ASSERT_EQ(rows.size(), 2U);

  std::vector<InputBlockSpec> allLeft;
  appendGroup(&allLeft, blockId(3, 3, 101, 101),
              {{10, 1, 11, false}, {10, 1, 12, false}, {20, 2, 21, false}, {20, 2, 22, false}, {20, 2, 23, false}});
  std::vector<InputBlockSpec> allRight;
  appendGroup(&allRight, blockId(4, 4, 101, 101), {{10, 1, 101, false}, {10, 1, 102, false}, {20, 2, 201, false}},
              {{7, 0}, {8, 2}});
  HashJoinHarness allHarness;
  ASSERT_EQ(allHarness.Create(allLeft, allRight, {101}, false, kLeftBlockId, kRightBlockId, false, true, true, true,
                              true, true),
            TSDB_CODE_SUCCESS);

  protocol.clear();
  rows.clear();
  ASSERT_EQ(fetchProtocol(allHarness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 7, false}, {101, 0, true}});
  EXPECT_EQ(protocol[0].lineage, (std::vector<std::pair<int32_t, int32_t>>{{7, 0}, {8, 4}}));
  EXPECT_EQ(rows.size(), 7U);
}

TEST(HashJoinMultiGroupTest, FinalFilterRemovingAllRowsStillEmitsEmptyLineageEog) {
  std::vector<InputBlockSpec> left;
  appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, -2, false}, {10, 1, -1, false}});
  std::vector<InputBlockSpec> right;
  appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 9, false}}, {{7, 0}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101}, true), TSDB_CODE_SUCCESS);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 0, true}});
  ASSERT_EQ(protocol.size(), 1U);
  EXPECT_TRUE(protocol[0].lineage.empty());
  EXPECT_TRUE(rows.empty());
}

TEST(HashJoinMultiGroupTest, ProbeLineageSurvivesParentLookahead) {
  std::vector<InputBlockSpec> left;
  appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}, {20, 2, 2, false}});
  std::vector<InputBlockSpec> right = {dataBlock(blockId(2, 2, 101, 101), {{10, 1, 9, false}}, {{7, 0}}),
                                       dataBlock(blockId(3, 3, 101, 101), {{20, 2, 8, false}}, {{8, 0}}),
                                       eogBlock(101)};
  HashJoinHarness             harness;
  ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);

  SSDataBlock* pBlock = nullptr;
  ASSERT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_SUCCESS);
  ASSERT_NE(pBlock, nullptr);
  ASSERT_EQ(pBlock->info.rows, 1);
  EXPECT_EQ(copyLineage(harness.runtime()->pStreamBlkWinIdx), (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));

  SSDataBlock* pOriginal = harness.DetachRightBlock(1);
  ASSERT_NE(pOriginal, nullptr);
  blockDataDestroy(pOriginal);

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 1, false}, {101, 0, true}});
  EXPECT_EQ(protocol[0].lineage, (std::vector<std::pair<int32_t, int32_t>>{{8, 0}}));
  EXPECT_TRUE(protocol[1].lineage.empty());
}

TEST(HashJoinMultiGroupTest, NestedHashJoinsPreserveDataAndEogLineage) {
  std::vector<InputBlockSpec> lowerLeft;
  std::vector<InputBlockSpec> lowerRight;
  appendGroup(&lowerLeft, blockId(1, 1, 101, 101), {{10, 1, 11, false}});
  appendGroup(&lowerRight, blockId(2, 2, 101, 101), {{10, 1, 12, false}}, {{7, 0}});
  HashJoinHarness lower;
  ASSERT_EQ(lower.Create(lowerLeft, lowerRight, {101}), TSDB_CODE_SUCCESS);

  std::vector<InputBlockSpec> upperLeftBlocks;
  appendGroup(&upperLeftBlocks, blockId(3, 3, 101, 101), {{10, 1, 13, false}});
  SOperatorInfo* upperLeft = nullptr;
  ASSERT_EQ(makeMockInput(lower.task(), kLeftBlockId, upperLeftBlocks, 0, nullptr, &upperLeft), TSDB_CODE_SUCCESS);
  SHashJoinPhysiNode* upperPlan = nullptr;
  ASSERT_EQ(makeHashJoinNode(kLeftBlockId, kOutputBlockId, false, false, true, &upperPlan), TSDB_CODE_SUCCESS);
  SOperatorInfo* upper = nullptr;
  SOperatorInfo* downstreams[2] = {upperLeft, lower.ReleaseJoin()};
  ASSERT_EQ(createHashJoinOperatorInfo(downstreams, 2, upperPlan, lower.task(), &upper), TSDB_CODE_SUCCESS);
  upper->resultDataBlockId = kOutputBlockId;

  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(upper, &protocol, &rows), TSDB_CODE_SUCCESS);
  expectProtocol(protocol, {{101, 1, false}, {101, 0, true}});
  EXPECT_EQ(protocol[0].lineage, (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
  EXPECT_TRUE(protocol[1].lineage.empty());

  destroyOperator(upper);
  nodesDestroyNode(reinterpret_cast<SNode*>(upperPlan));
}

TEST(HashJoinMultiGroupTest, HashMemoryReturnsToBaselineAfterEveryEog) {
  std::vector<uint64_t>       order;
  std::vector<InputBlockSpec> left;
  std::vector<InputBlockSpec> right;
  for (uint64_t gid = 1; gid <= 32; ++gid) {
    order.push_back(gid);
    appendGroup(&left, blockId(100 + gid, 1000 + gid, gid, gid), {{static_cast<int64_t>(gid), 1, 1, false}});
    appendGroup(&right, blockId(200 + gid, 2000 + gid, gid, gid), {{static_cast<int64_t>(gid), 1, 2, false}});
  }
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, order), TSDB_CODE_SUCCESS);

  int32_t eogCount = 0;
  for (int32_t call = 0; call < 1000; ++call) {
    SSDataBlock* pBlock = nullptr;
    ASSERT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_SUCCESS);
    if (pBlock == nullptr) {
      break;
    }
    if (qIsMultiGroupEogBlock(pBlock)) {
      ++eogCount;
      EXPECT_EQ(harness.info()->pKeyHash, nullptr);
      EXPECT_TRUE(harness.info()->pRowBufs == nullptr || taosArrayGetSize(harness.info()->pRowBufs) == 0);
      EXPECT_EQ(harness.info()->hashOwnedBytes, 0U);
    }
  }
  EXPECT_EQ(eogCount, 32);
}

TEST(HashJoinMultiGroupTest, PeakMemoryTracksLargestGroupNotGroupSum) {
  std::vector<InputRow> equalBuildRows;
  equalBuildRows.reserve(2048);
  for (int32_t row = 0; row < 2048; ++row) {
    equalBuildRows.push_back({100000 + row, row, row, false});
  }

  std::vector<InputBlockSpec> baselineLeft;
  std::vector<InputBlockSpec> baselineRight;
  appendGroup(&baselineLeft, blockId(101, 1001, 1, 1), equalBuildRows);
  appendGroup(&baselineRight, blockId(201, 2001, 1, 1), {{100000, 0, 3, false}});
  HashJoinHarness baseline;
  ASSERT_EQ(baseline.Create(baselineLeft, baselineRight, {1}, false, kLeftBlockId, kRightBlockId, false, true, true,
                            true, true, false, 2048, 65537),
            TSDB_CODE_SUCCESS);
  std::vector<ProtocolEvent> baselineProtocol;
  std::vector<OutputRow>     baselineRows;
  ASSERT_EQ(fetchProtocol(baseline.join(), &baselineProtocol, &baselineRows), TSDB_CODE_SUCCESS);
  const uint64_t singleGroupPeak = baseline.info()->execInfo.hashMemPeakBytes;

  std::vector<uint64_t>       order;
  std::vector<InputBlockSpec> left;
  std::vector<InputBlockSpec> right;
  for (uint64_t gid = 1; gid <= 32; ++gid) {
    std::vector<InputRow> groupBuildRows = equalBuildRows;
    for (InputRow& row : groupBuildRows) {
      row.ts += static_cast<int64_t>(gid - 1) * 100000;
    }
    order.push_back(gid);
    appendGroup(&left, blockId(100 + gid, 1000 + gid, gid, gid), groupBuildRows);
    appendGroup(&right, blockId(200 + gid, 2000 + gid, gid, gid), {{static_cast<int64_t>(gid) * 100000, 0, 3, false}});
  }
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, order, false, kLeftBlockId, kRightBlockId, false, true, true, true, true, false,
                           static_cast<int64_t>(equalBuildRows.size() * order.size()), 65537),
            TSDB_CODE_SUCCESS);
  std::vector<ProtocolEvent> protocol;
  std::vector<OutputRow>     rows;
  ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), TSDB_CODE_SUCCESS);
  EXPECT_GE(singleGroupPeak, static_cast<uint64_t>(HASH_JOIN_DEFAULT_PAGE_SIZE));
  EXPECT_EQ(harness.info()->execInfo.hashMemPeakBytes, singleGroupPeak);
}

TEST(HashJoinMultiGroupTest, CurrentGroupAllocationFailureIsFatal) {
  {
    std::vector<InputBlockSpec> left;
    std::vector<InputBlockSpec> right;
    appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
    appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 2, false}});
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);
    ASSERT_EQ(harness.info()->pKeyHash, nullptr);
    ASSERT_EQ(harness.info()->pRowBufs, nullptr);

    hJoinTestFailHashInitOnce();
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(pBlock, nullptr);
  }
  {
    std::vector<InputBlockSpec> left;
    std::vector<InputBlockSpec> right;
    appendGroup(&left, blockId(1, 1, 101, 101), {{10, 1, 1, false}});
    appendGroup(&right, blockId(2, 2, 101, 101), {{10, 1, 2, false}});
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);

    hJoinTestFailPagePushOnce();
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(pBlock, nullptr);
    EXPECT_EQ(harness.info()->pKeyHash, nullptr);
    EXPECT_EQ(harness.info()->pRowBufs, nullptr);
  }
  {
    std::vector<InputBlockSpec> left = {
        dataBlock(blockId(1, 1, 101, 101), {{10, 1, 1, false}}),
        dataBlock(blockId(2, 2, 101, 101), {{20, 2, 2, false}}),
        eogBlock(101),
    };
    std::vector<InputBlockSpec> right;
    appendGroup(&right, blockId(3, 3, 101, 101), {{10, 1, 3, false}});
    HashJoinHarness harness;
    ASSERT_EQ(harness.Create(left, right, {101}), TSDB_CODE_SUCCESS);
    harness.ExhaustCurrentRowPageBeforeBuildBlock(1);

    hJoinTestFailAfterPageGrowthOnce();
    SSDataBlock* pBlock = nullptr;
    EXPECT_EQ(fetchOne(harness.join(), &pBlock), TSDB_CODE_OUT_OF_MEMORY);
    EXPECT_EQ(pBlock, nullptr);
  }
}

TEST(HashJoinMultiGroupTest, LegacyHashJoinIgnoresEogProtocolFields) {
  std::vector<InputBlockSpec> left = {dataBlock(blockId(1, 1, 101, 101), {{10, 7, 1, false}}), eogBlock(101)};
  std::vector<InputBlockSpec> right = {dataBlock(blockId(2, 2, 202, 202), {{10, 7, 2, false}}), eogBlock(202)};
  HashJoinHarness             harness;
  ASSERT_EQ(harness.CreateLegacy(left, right), TSDB_CODE_SUCCESS);
  ASSERT_NE(harness.info()->pKeyHash, nullptr);
  ASSERT_NE(harness.info()->pRowBufs, nullptr);

  std::vector<OutputRow> rows;
  ASSERT_EQ(fetchAll(harness.join(), &rows), TSDB_CODE_SUCCESS);
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows[0].leftPayload, 1);
  EXPECT_EQ(rows[0].rightPayload, 2);
}

TEST(HashJoinMultiGroupTest, RuntimeFilterIneligibleProbePreservesProtocolAndReset) {
  std::vector<InputBlockSpec> left, right;
  appendGroup(&left, blockId(1, 101, 101, 101), {{10, 1, 11, false}});
  appendGroup(&right, blockId(2, 102, 101, 101), {{10, 1, 21, false}}, {{7, 0}});
  HashJoinHarness harness;
  ASSERT_EQ(harness.Create(left, right, {101}), 0);
  for (int32_t pass = 0; pass < 2; ++pass) {
    std::vector<ProtocolEvent> protocol;
    std::vector<OutputRow>     rows;
    ASSERT_EQ(fetchProtocol(harness.join(), &protocol, &rows), 0);
    expectProtocol(protocol, {{101, 1, false}, {101, 0, true}});
    ASSERT_EQ(rows.size(), 1);
    EXPECT_EQ(rows[0].key, 1);
    EXPECT_EQ(protocol[0].lineage, (std::vector<std::pair<int32_t, int32_t>>{{7, 0}}));
    ASSERT_EQ(harness.ResetTree(), 0);
  }
}
