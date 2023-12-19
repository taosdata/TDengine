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
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#include "os.h"

#include "executor.h"
#include "executorInt.h"
#include "function.h"
#include "operator.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tvariant.h"

namespace {

typedef struct {
  bool    succeed;
  int32_t blkNum;
  int32_t rowNum;
  int32_t addRowNum;
  int32_t subRowNum;
  int32_t mismatchNum;
  int32_t matchNum;
} SJoinTestResInfo;

typedef struct {
  bool    filter;
  bool    asc;
  int32_t leftMaxRows;
  int32_t leftMaxGrpRows;
  int32_t rightMaxRows;
  int32_t rightMaxGrpRows;
  int32_t blkRows;
  int32_t colCond;
  int32_t joinType;
  int32_t subType;

  int32_t leftTotalRows;
  int32_t rightTotalRows;
  int32_t blkRowSize;

  int32_t colEqNum;
  int32_t colEqList[MAX_SLOT_NUM];

  int32_t colOnNum;
  int32_t colOnList[MAX_SLOT_NUM];

  int32_t leftFilterNum;
  int32_t leftFilterColList[MAX_SLOT_NUM];

  int32_t rightFilterNum;
  int32_t rightFilterColList[MAX_SLOT_NUM];

  int32_t keyInSlotIdx;
  int32_t keyOutSlotIdx;
  int32_t keyColOffset;
  
  int32_t resColNum;
  int32_t resColInSlot[MAX_SLOT_NUM * 2];
  int32_t resColList[MAX_SLOT_NUM * 2];
  int32_t resColOffset[MAX_SLOT_NUM * 2];
  int32_t resColSize;
  void*   resColBuf;

  int32_t colRowDataBufSize;
  void*   colRowDataBuf;
  int32_t colRowOffset[MAX_SLOT_NUM];

  int64_t curTs;

  int32_t leftBlkReadIdx;
  SArray* leftBlkList;
  int32_t rightBlkReadIdx;
  SArray* rightBlkList;  

  SSHashObj* jtResRows;

  SOperatorInfo* pJoinOp;
} SJoinTestCtrlInfo;

enum {
  TEST_NO_COND = 1,
  TEST_EQ_COND,
  TEST_ON_COND,
  TEST_FULL_COND
};

#define LEFT_BLK_ID       0
#define RIGHT_BLK_ID      1
#define RES_BLK_ID        2
#define MAX_SLOT_NUM      4

#define JT_KEY_SOLT_ID    3
int32_t jtInputColType[MAX_SLOT_NUM] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT};

char* jtColCondStr[] = {"", "NO COND", "EQ COND", "ON COND", "FULL COND"};
char* jtJoinTypeStr[] = {"INNER", "LEFT", "RIGHT", "FULL"};
char* jtSubTypeStr[] = {"", "OUTER", "SEMI", "ANTI", "ASOF", "WINDOW"};

int64_t TIMESTAMP_FILTER_VALUE = 1000000000;
int32_t INT_FILTER_VALUE = 32767;
int64_t BIGINT_FILTER_VALUE = 1000000000000000;

int64_t jtFilterValue[] = {TIMESTAMP_FILTER_VALUE, INT_FILTER_VALUE, INT_FILTER_VALUE, BIGINT_FILTER_VALUE};

bool jtErrorRerun = true;
bool jtInRerun = false;

SJoinTestCtrlInfo jtCtrl = {0};
SJoinTestResInfo jtRes = {0};



void printResRow(void* value, int32_t type) {
  for (int32_t i = 0; i < jtCtrl.resColNum; ++i) {
    int32_t slot = jtCtrl.resColInSlot[i];
    switch (jtInputColType[slot % MAX_SLOT_NUM]) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("\t%" PRId64 , *(int64_t*)(value + jtCtrl.resColOffset[slot]));
        break;
      case TSDB_DATA_TYPE_INT:
        printf("\t%d", *(int32_t*)(value + jtCtrl.resColOffset[slot]));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        printf("\t%d", *(int64_t*)(value + jtCtrl.resColOffset[slot]));
        break;
    }
  }
  printf("\t %s\n", 0 == type ? "-" : (1 == type ? "+" : ""));
}

SOperatorInfo* createDummyDownstreamOperators(int32_t num) {
  SOperatorInfo* p = taosMemoryCalloc(num, sizeof(SOperatorInfo));
  for (int32_t i = 0; i < num; ++i) {
    p->resultDataBlockId = i;
  }
  return p;
}

void createTargetSlotList(SSortMergeJoinPhysiNode* p) {
  int32_t leftTargetNum = taosRand() % MAX_SLOT_NUM;
  int32_t rightTargetNum = 0;
  if (0 == leftTargetNum) {
    do {
      rightTargetNum = taosRand() % MAX_SLOT_NUM;
    } while (0 == rightTargetNum);
  } else {
    leftTargetNum = taosRand() % MAX_SLOT_NUM;
  }

  memset(jtCtrl.resColList, 0, sizeof(jtCtrl.resColList));
  jtCtrl.resColSize = MAX_SLOT_NUM * 2 * sizeof(bool);

  int32_t idx = 0;
  int32_t dstIdx = 0;
  int32_t dstOffset = jtCtrl.resColSize;
  for (int32_t i = 0; i < leftTargetNum; ) {
    if (0 == i) {
      jtCtrl.resColList[JT_KEY_SOLT_ID] = 1;
      ++i;
      jtCtrl.keyInSlotIdx = JT_KEY_SOLT_ID;
      continue;
    }
    
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtrl.resColList[idx]) {
      continue;
    }
    jtCtrl.resColList[idx] = 1;
    ++i;
  }

  for (int32_t i = 0; i < rightTargetNum; ) {
    if (0 == i && leftTargetNum <= 0) {
      jtCtrl.resColList[MAX_SLOT_NUM + JT_KEY_SOLT_ID] = 1;
      ++i;
      jtCtrl.keyInSlotIdx = MAX_SLOT_NUM + JT_KEY_SOLT_ID;
      continue;
    }

    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtrl.resColList[MAX_SLOT_NUM + idx]) {
      continue;
    }
    jtCtrl.resColList[MAX_SLOT_NUM + idx] = 1;
    ++i;
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtrl.resColList[i]) {
      jtCtrl.resColOffset[dstIdx] = dstOffset;
      jtCtrl.resColInSlot[dstIdx] = i;
      if (jtCtrl.keyInSlotIdx == i) {
        jtCtrl.keyColOffset = dstOffset;
        jtCtrl.keyOutSlotIdx = dstIdx;
      }

      STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = LEFT_BLK_ID;
      pCol->slotId = i;
      pTarget->dataBlockId = RES_BLK_ID;
      pTarget->slotId = dstIdx++;
      pTarget->pExpr = (SNode*)pCol;
      dstOffset += tDataTypes[jtInputColType[i]].bytes;
      jtCtrl.resColSize += tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(p->pTargets, pTarget);
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtrl.resColList[MAX_SLOT_NUM + i]) {
      jtCtrl.resColOffset[dstIdx] = dstOffset;
      jtCtrl.resColInSlot[dstIdx] = i + MAX_SLOT_NUM;
      if (jtCtrl.keyInSlotIdx == (i + MAX_SLOT_NUM)) {
        jtCtrl.keyColOffset = dstOffset;
        jtCtrl.keyOutSlotIdx = dstIdx;
      }

      STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = RIGHT_BLK_ID;
      pCol->slotId = i;
      pTarget->dataBlockId = RES_BLK_ID;
      pTarget->slotId = dstIdx++;
      pTarget->pExpr = (SNode*)pCol;
      dstOffset += tDataTypes[jtInputColType[i]].bytes;
      jtCtrl.resColSize += tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(p->pTargets, pTarget);
    }
  }  

  jtCtrl.resColNum = leftTargetNum + rightTargetNum;
  jtCtrl.resColBuf = taosMemoryCalloc(jtCtrl.resColSize, 1);
}

void createColEqCond(SSortMergeJoinPhysiNode* p) {
  jtCtrl.colEqNum = 0;
  do {
    jtCtrl.colEqNum = taosRand() % MAX_SLOT_NUM;
  } while (0 == jtCtrl.colEqNum);

  int32_t idx = 0;
  memset(jtCtrl.colEqList, 0, sizeof(jtCtrl.colEqList));
  for (int32_t i = 0; i < jtCtrl.colEqNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtrl.colEqList[idx]) {
      continue;
    }
    jtCtrl.colEqList[idx] = 1;
    ++i;
  }

  SLogicConditionNode* pLogic = NULL;
  if (jtCtrl.colEqNum > 1) {
    pLogic = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtrl.colEqList[i]) {
      SColumnNode* pCol1 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol1->dataBlockId = LEFT_BLK_ID;
      pCol1->slotId = i;
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(p->pEqLeft, pCol1);

      SColumnNode* pCol2 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol2->dataBlockId = RIGHT_BLK_ID;
      pCol2->slotId = i;
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(p->pEqRight, pCol2);

      SOperatorNode* pOp = nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_EQUAL;
      pOp->pLeft = nodesCloneNode(pCol1);
      pOp->pRight = nodesCloneNode(pCol2);

      if (jtCtrl.colEqNum > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, pOp);
      } else {
        p->pFullOnCond = pOp;
        break;
      }
    }
  }

  if (jtCtrl.colEqNum > 1) {
    p->pFullOnCond = pLogic;
  }  
}

void createColOnCond(SSortMergeJoinPhysiNode* p) {
  jtCtrl.colOnNum = 0;
  do {
    jtCtrl.colOnNum = taosRand() % MAX_SLOT_NUM;
  } while (0 == jtCtrl.colOnNum || (jtCtrl.colOnNum + jtCtrl.colEqNum) > MAX_SLOT_NUM);

  int32_t idx = 0;
  memset(jtCtrl.colOnList, 0, sizeof(jtCtrl.colOnList));
  for (int32_t i = 0; i < jtCtrl.colOnNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtrl.colOnList[idx] || jtCtrl.colEqList[idx]) {
      continue;
    }
    jtCtrl.colOnList[idx] = 1;
    ++i;
  }

  SLogicConditionNode* pLogic = NULL;
  if (jtCtrl.colOnNum > 1) {
    pLogic = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtrl.colOnList[i]) {
      SColumnNode* pCol1 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol1->dataBlockId = LEFT_BLK_ID;
      pCol1->slotId = i;
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = (TSDB_DATA_TYPE_BINARY != jtInputColType[i]) ? tDataTypes[jtInputColType[i]].bytes : MJ_TEST_BINARY_BYTES;
      
      nodesListMakeStrictAppend(p->pEqLeft, pCol1);

      SColumnNode* pCol2 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol2->dataBlockId = RIGHT_BLK_ID;
      pCol2->slotId = i;
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = (TSDB_DATA_TYPE_BINARY != jtInputColType[i]) ? tDataTypes[jtInputColType[i]].bytes : MJ_TEST_BINARY_BYTES;
      
      nodesListMakeStrictAppend(p->pEqRight, pCol2);

      SOperatorNode* pOp = nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->pLeft = nodesCloneNode(pCol1);
      pOp->pRight = nodesCloneNode(pCol2);

      if (jtCtrl.colOnNum > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, pOp);
      } else {
        p->pColOnCond = pOp;
        break;
      }
    }
  }

  if (jtCtrl.colOnNum > 1) {
    p->pColOnCond = pLogic;
  }  

  mergeEqCond(&p->pFullOnCond, nodesCloneNode(p->pColOnCond));
}


void createColCond(SSortMergeJoinPhysiNode* p, int32_t cond) {
  jtCtrl.colCond = cond;
  switch (cond) {
    case TEST_NO_COND:
      jtCtrl.colEqNum = 0;
      jtCtrl.colOnNum = 0;
      memset(jtCtrl.colEqList, 0, sizeof(jtCtrl.colEqList));
      memset(jtCtrl.colOnList, 0, sizeof(jtCtrl.colOnList));
      break;
    case TEST_EQ_COND:
      createColEqCond(p);
      jtCtrl.colOnNum = 0;
      memset(jtCtrl.colOnList, 0, sizeof(jtCtrl.colOnList));
      break;
    case TEST_ON_COND:
      createColOnCond(p);
      jtCtrl.colEqNum = 0;
      memset(jtCtrl.colEqList, 0, sizeof(jtCtrl.colEqList));
      break;
    case TEST_FULL_COND:
      createColEqCond(p);
      createColOnCond(p);
      break;    
    default:
      break;
  }
}

void* getFilterValue(int32_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_TIMESTAMP:
      return &TIMESTAMP_FILTER_VALUE;
    case TSDB_DATA_TYPE_INT:
      return &INT_FILTER_VALUE;
    case TSDB_DATA_TYPE_BIGINT:
      return &BIGINT_FILTER_VALUE;
    default:
      return NULL;
  }
}

void createFilterCond(SSortMergeJoinPhysiNode* p, bool filter) {
  jtCtrl.filter = filter;
  if (!filter) {
    jtCtrl.leftFilterNum = 0;
    jtCtrl.rightFilterNum = 0;
    memset(jtCtrl.leftFilterColList, 0, sizeof(jtCtrl.leftFilterColList));
    memset(jtCtrl.rightFilterColList, 0, sizeof(jtCtrl.rightFilterColList));
    return;
  }
  
  jtCtrl.leftFilterNum = taosRand() % MAX_SLOT_NUM;
  if (0 == jtCtrl.leftFilterNum) {
    do {
      jtCtrl.rightFilterNum = taosRand() % MAX_SLOT_NUM;
    } while (0 == jtCtrl.rightFilterNum);
  } else {
    jtCtrl.rightFilterNum = taosRand() % MAX_SLOT_NUM;
  }

  int32_t idx = 0;
  memset(jtCtrl.leftFilterColList, 0, sizeof(jtCtrl.leftFilterColList));
  memset(jtCtrl.rightFilterColList, 0, sizeof(jtCtrl.rightFilterColList));
  for (int32_t i = 0; i < jtCtrl.leftFilterNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtrl.leftFilterColList[idx]) {
      continue;
    }
    jtCtrl.leftFilterColList[idx] = 1;
    ++i;
  }

  for (int32_t i = 0; i < jtCtrl.rightFilterNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtrl.rightFilterColList[idx]) {
      continue;
    }
    jtCtrl.rightFilterColList[idx] = 1;
    ++i;
  }

  SLogicConditionNode* pLogic = NULL;
  if ((jtCtrl.leftFilterNum + jtCtrl.rightFilterNum) > 1) {
    pLogic = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    pLogic->condType = taosRand() % 2 ? LOGIC_COND_TYPE_AND : LOGIC_COND_TYPE_OR;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtrl.leftFilterColList[i]) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = LEFT_BLK_ID;
      pCol->slotId = i;
      pCol->node.resType.type = jtInputColType[i];
      pCol->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;

      SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
      nodesSetValueNodeValue(pVal, getFilterValue(jtInputColType[i]));
      pVal->node.resType.type = jtInputColType[i];
      pVal->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;

      SOperatorNode* pOp = nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->pLeft = pCol;
      pOp->pRight = pVal;

      if ((jtCtrl.leftFilterNum + jtCtrl.rightFilterNum) > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, pOp);
      } else {
        p->node.pConditions = pOp;
        break;
      }
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtrl.rightFilterColList[i]) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = RIGHT_BLK_ID;
      pCol->slotId = i;
      pCol->node.resType.type = jtInputColType[i];
      pCol->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;

      SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
      nodesSetValueNodeValue(pVal, getFilterValue(jtInputColType[i]));
      pVal->node.resType.type = jtInputColType[i];
      pVal->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;

      SOperatorNode* pOp = nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->pLeft = pCol;
      pOp->pRight = pVal;

      if ((jtCtrl.leftFilterNum + jtCtrl.rightFilterNum) > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, pOp);
      } else {
        p->node.pConditions = pOp;
        break;
      }
    }
  }

  if ((jtCtrl.leftFilterNum + jtCtrl.rightFilterNum) > 1) {
    p->node.pConditions = pLogic;
  }  
}

void updateColRowInfo() {
  jtCtrl.blkRowSize = MAX_SLOT_NUM * sizeof(bool);

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    jtCtrl.colRowOffset[i] = jtCtrl.blkRowSize;
    jtCtrl.blkRowSize += tDataTypes[jtInputColType[i]].bytes;
  }
}

SSortMergeJoinPhysiNode* createDummySortMergeJoinPhysiNode(EJoinType type, EJoinSubType sub, int32_t cond, bool filter, bool asc) {
  SSortMergeJoinPhysiNode* p = taosMemoryCalloc(1, sizeof(SSortMergeJoinPhysiNode));
  p->joinType = type;
  p->subType = sub;
  p->leftPrimSlotId = LEFT_BLK_ID;
  p->rightPrimSlotId = RIGHT_BLK_ID;
  p->node.inputTsOrder = asc ? ORDER_ASC : ORDER_DESC;

  jtCtrl.joinType = type;
  jtCtrl.subType = sub;
  jtCtrl.asc = asc;

  createColCond(p, cond);
  createTargetSlotList(p);
  createFilterCond(p, filter);
  updateColRowInfo();

  return p;
}

SExecTaskInfo* createDummyTaskInfo(char* taskId) {
  SExecTaskInfo* p = taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  p->id.str = taskId;
}

SSDataBlock* createDummyBlock(int32_t blkId) {
  SSDataBlock* p = createDataBlock();

  p->info.id.blockId = blkId;
  p->info.type = STREAM_INVALID;
  p->info.calWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  p->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    SColumnInfoData idata =
        createColumnInfoData(jtInputColType[i]), (TSDB_DATA_TYPE_BINARY != jtInputColType[i]) ? tDataTypes[jtInputColType[i]].bytes : MJ_TEST_BINARY_BYTES, i);

    blockDataAppendColInfo(p, &idata);
  }

  return p;
}

void initJoinTest() {
  jtCtrl.leftBlkList = taosArrayInit(10, POINTER_BYTES);
  jtCtrl.rightBlkList = taosArrayInit(10, POINTER_BYTES);

  jtCtrl.jtResRows = tSimpleHashInit(10000000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
}

void createGrpRows(SSDataBlock** ppBlk, int32_t blkId, int32_t grpRows) {
  if (grpRows <= 0) {
    return;
  }

  if (NULL == ppBlk) {
    *ppBlk = createDummyBlock((blkId == LEFT_BLK_ID) ? LEFT_BLK_ID : RIGHT_BLK_ID);
    blockDataEnsureCapacity(*ppBlk, jtCtrl.blkRows);
    taosArrayPush((blkId == LEFT_BLK_ID) ? jtCtrl.leftBlkList : jtCtrl.rightBlkList, ppBlk);
  }

  int32_t tableOffset = 0;
  int32_t peerOffset = 0;
  bool keepRes = false;
  if (blkId == LEFT_BLK_ID) {
    if ((jtCtrl.joinType == JOIN_TYPE_LEFT || jtCtrl.joinType == JOIN_TYPE_FULL) && jtCtrl.subType != JOIN_STYPE_SEMI) {
      keepRes = true;
    }
    peerOffset = MAX_SLOT_NUM;
  } else {
    if ((jtCtrl.joinType == JOIN_TYPE_RIGHT || jtCtrl.joinType == JOIN_TYPE_FULL) && jtCtrl.subType != JOIN_STYPE_SEMI) {
      keepRes = true;
    }
    tableOffset = MAX_SLOT_NUM;
  }
  
  int32_t filterNum = (blkId == LEFT_BLK_ID) ? jtCtrl.leftFilterNum : jtCtrl.rightFilterNum;
  int32_t peerFilterNum = (blkId == LEFT_BLK_ID) ? jtCtrl.rightFilterNum : jtCtrl.leftFilterNum;
  int32_t* filterCol = (blkId == LEFT_BLK_ID) ? jtCtrl.leftFilterColList : jtCtrl.rightFilterColList;
  
  void* pData = NULL;
  int32_t tmpInt = 0;
  int64_t tmpBigint = 0;
  bool isNull = false;
  bool filterOut = false;
  for (int32_t i = 0; i < grpRows; ++i) {
    if ((*ppBlk)->info.rows >= (*ppBlk)->info.capacity) {
      *ppBlk = createDummyBlock((blkId == LEFT_BLK_ID) ? LEFT_BLK_ID : RIGHT_BLK_ID);
      blockDataEnsureCapacity(*ppBlk, jtCtrl.blkRows);
      taosArrayPush((blkId == LEFT_BLK_ID) ? jtCtrl.leftBlkList : jtCtrl.rightBlkList, ppBlk);
    }

    filterOut = peerFilterNum > 0 ? true : false;
    if (!filterOut) {
      memset(jtCtrl.resColBuf, 0, jtCtrl.resColSize);
    }
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          ++jtCtrl.curTs;
          pData = &jtCtrl.curTs;
          isNull = false;
          if (!filterOut && filterNum && filterCol[c] && jtCtrl.curTs <= TIMESTAMP_FILTER_VALUE) {
            filterOut = true;
          }
          break;
        case TSDB_DATA_TYPE_INT:
          if (taosRand() % 2) {
            tmpInt = (taosRand() % 2) ? INT_FILTER_VALUE + taosRand() % 3 : INT_FILTER_VALUE - taosRand() % 3;
            pData = &tmpInt;
            isNull = false;
            if (filterNum && filterCol[c] && tmpInt <= INT_FILTER_VALUE) {
              filterOut = true;
            }
          } else {
            isNull = true;
            filterOut = (filterNum && filterCol[c]) ? true : false;
          }
          break;
        case TSDB_DATA_TYPE_BIGINT:
          tmpBigint = (taosRand() % 2) ? BIGINT_FILTER_VALUE + taosRand() % 3 : BIGINT_FILTER_VALUE - taosRand() % 3;
          pData = &tmpBigint;
          isNull = false;
          if (filterNum && filterCol[c] && tmpBigint <= BIGINT_FILTER_VALUE) {
            filterOut = true;
          }
          break;
        default:
          break;
      }
      
      SColumnInfoData* pCol = taosArrayGet((*ppBlk)->pDataBlock, c);
      colDataSetVal(pCol, (*ppBlk)->info.rows, pData, isNull);

      if (keepRes && !filterOut && jtCtrl.resColList[tableOffset + c]) {
        if (isNull) {
          *(char*)(jtCtrl.resColBuf + tableOffset + c) = true;
        } else {
          memcpy(jtCtrl.resColBuf + jtCtrl.resColOffset[tableOffset + c], pData, tDataTypes[jtInputColType[c]].bytes);
        }
      }
    }

    if (keepRes && !filterOut) {
      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        if (jtCtrl.resColList[peerOffset + c]) {
          *(char*)(jtCtrl.resColBuf + peerOffset + c) = true;
        }
      }
      
      tSimpleHashPut(jtCtrl.jtResRows, jtCtrl.resColBuf + jtCtrl.keyColOffset, sizeof(int64_t), jtCtrl.resColBuf, jtCtrl.resColSize);
    }
    
    (*ppBlk)->info.rows++;
  }
}

void createRowData(SSDataBlock* pBlk, int64_t tbOffset, int32_t rowIdx) {
  int32_t tmpInt = 0;
  int64_t tmpBig = 0;
  void *pData = NULL;
  
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    SColumnInfoData* pCol = taosArrayGet(pBlk->pDataBlock, c);

    int32_t rv = taosRand() % 2;
    switch (jtInputColType[c]) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        *(int64_t*)(jtCtrl.colRowDataBuf + tbOffset + rowIdx * jtCtrl.blkRowSize + jtCtrl.colRowOffset[c]) = jtCtrl.curTs;
        colDataSetVal(pCol, pBlk->info.rows, &jtCtrl.curTs, false);
        break;
      case TSDB_DATA_TYPE_INT:
        if (0 == rv) {
          tmpInt = (taosRand() % 2) ? INT_FILTER_VALUE + taosRand() % 3 : INT_FILTER_VALUE - taosRand() % 3;
          *(int32_t*)(jtCtrl.colRowDataBuf + tbOffset + rowIdx * jtCtrl.blkRowSize + jtCtrl.colRowOffset[c]) = tmpInt;
          colDataSetVal(pCol, pBlk->info.rows, &tmpInt, false);
        } else {
          *(bool*)(jtCtrl.colRowDataBuf + tbOffset + rowIdx * jtCtrl.blkRowSize + c) = true;
          colDataSetVal(pCol, pBlk->info.rows, NULL, true);
        }
        break;
      case TSDB_DATA_TYPE_BIGINT:
        if (0 == rv) {
          tmpBig = (taosRand() % 2) ? BIGINT_FILTER_VALUE + taosRand() % 3 : BIGINT_FILTER_VALUE - taosRand() % 3;
          *(int64_t*)(jtCtrl.colRowDataBuf + tbOffset + rowIdx * jtCtrl.blkRowSize + jtCtrl.colRowOffset[c]) = tmpBig;
          colDataSetVal(pCol, pBlk->info.rows, &tmpBig, false);
        } else {
          *(bool*)(jtCtrl.colRowDataBuf + tbOffset + rowIdx * jtCtrl.blkRowSize + c) = true;
          colDataSetVal(pCol, pBlk->info.rows, NULL, true);
        }
        break;
      default:
        break;
    }
  }

}

void makeAppendBlkData(SSDataBlock** ppLeft, SSDataBlock** ppRight, int32_t leftGrpRows, int32_t rightGrpRows) {
  int64_t totalSize = (leftGrpRows + rightGrpRows) * jtCtrl.blkRowSize;
  int64_t rightOffset = leftGrpRows * jtCtrl.blkRowSize;

  if (jtCtrl.colRowDataBufSize < totalSize) {
    jtCtrl.colRowDataBuf = taosMemoryRealloc(jtCtrl.colRowDataBuf, totalSize);
  }

  memset(jtCtrl.colRowDataBuf, 0, totalSize);
  
  for (int32_t i = 0; i < leftGrpRows; ++i) {
    if ((*ppLeft)->info.rows >= (*ppLeft)->info.capacity) {
      *ppLeft = createDummyBlock(LEFT_BLK_ID);
      blockDataEnsureCapacity(*ppLeft, jtCtrl.blkRows);
      taosArrayPush(jtCtrl.leftBlkList, ppLeft);
    }

    createRowData(*ppLeft, 0, i);
  }

  for (int32_t i = 0; i < rightGrpRows; ++i) {
    if ((*ppRight)->info.rows >= (*ppRight)->info.capacity) {
      *ppRight = createDummyBlock(RIGHT_BLK_ID);
      blockDataEnsureCapacity(*ppRight, jtCtrl.blkRows);
      taosArrayPush(jtCtrl.rightBlkList, ppRight);
    }

    createRowData(*ppRight, rightOffset, i);
  }

}

void putNMatchRowToRes(void* lrow, int32_t tableOffset, int32_t peerOffset) {
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtrl.resColList[tableOffset + c]) {
      if (*(bool*)(lrow + c)) {
        *(bool*)(jtCtrl.resColBuf + tableOffset + c) = true;
      } else {
        memcpy(jtCtrl.resColBuf + jtCtrl.resColOffset[tableOffset + c], lrow + jtCtrl.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
  }

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtrl.resColList[peerOffset + c]) {
      *(bool*)(jtCtrl.resColBuf + peerOffset + c) = true;
    }
  }
  
  tSimpleHashPut(jtCtrl.jtResRows, jtCtrl.resColBuf + jtCtrl.keyColOffset, sizeof(int64_t), jtCtrl.resColBuf, jtCtrl.resColSize);
}

void putMatchRowToRes(void* lrow, void* rrow) {
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtrl.resColList[c]) {
      if (*(bool*)(lrow + c)) {
        *(bool*)(jtCtrl.resColBuf + c) = true;
      } else {
        memcpy(jtCtrl.resColBuf + jtCtrl.resColOffset[c], lrow + jtCtrl.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
  }

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtrl.resColList[MAX_SLOT_NUM + c]) {
      if (*(bool*)(lrow + c)) {
        *(bool*)(jtCtrl.resColBuf + MAX_SLOT_NUM + c) = true;
      } else {
        memcpy(jtCtrl.resColBuf + jtCtrl.resColOffset[MAX_SLOT_NUM + c], rrow + jtCtrl.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
  }
  
  tSimpleHashPut(jtCtrl.jtResRows, jtCtrl.resColBuf + jtCtrl.keyColOffset, sizeof(int64_t), jtCtrl.resColBuf, jtCtrl.resColSize);
}


void leftJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  bool rowMatch = false, filterOut = false;
  bool lNullValue = false, rNullValue = false;
  void* lValue = NULL, *rValue = NULL, *filterValue = NULL;
  int64_t rightTbOffset = jtCtrl.blkRowSize * leftGrpRows;
  
  for (int32_t l = 0; l < leftGrpRows; ++l) {
    void* lrow = jtCtrl.colRowDataBuf + jtCtrl.blkRowSize * l;
    
    rowMatch = false;
    lNullValue= false;
    filterOut = false;
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (*(bool*)(lrow + c)) {
        lNullValue = true;
      }
      
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          filterValue = &TIMESTAMP_FILTER_VALUE;
          break;
        case TSDB_DATA_TYPE_INT:
          filterValue = &INT_FILTER_VALUE;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          filterValue = &BIGINT_FILTER_VALUE;
          break;
        default:
          filterValue = NULL;
          break;
      }
      
      if (jtCtrl.leftFilterNum && jtCtrl.leftFilterColList[c] && ((*(bool*)(lrow + c)) || memcmp(lrow + jtCtrl.colRowOffset[c], filterValue, tDataTypes[jtInputColType[c]].bytes) <= 0)) {
        filterOut = true;
        break;
      }
    }

    if (filterOut) {
      continue;
    }

    if (lNullValue) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
      continue;
    }

    lValue = lrow + jtCtrl.colRowOffset[c];
    
    for (int32_t r = 0; r < rightGrpRows; ++r) {
      void* rrow = jtCtrl.colRowDataBuf + rightTbOffset + jtCtrl.blkRowSize * r;
      filterOut = false;

      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        if (*(bool*)(rrow + c)) {
          rNullValue = true;
        } else {
          rValue = rrow + jtCtrl.colRowOffset[c];
        }

        switch (jtInputColType[c]) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            filterValue = &TIMESTAMP_FILTER_VALUE;
            break;
          case TSDB_DATA_TYPE_INT:
            filterValue = &INT_FILTER_VALUE;
            break;
          case TSDB_DATA_TYPE_BIGINT:
            filterValue = &BIGINT_FILTER_VALUE;
            break;
          default:
            filterValue = NULL;
            break;
        }
      
        if (jtCtrl.colEqNum && jtCtrl.colEqList[c] && ((*(bool*)(rrow + c)) || memcmp(lValue, rValue, tDataTypes[jtInputColType[c]].bytes))) {
          filterOut = true;
          break;
        }

        if (jtCtrl.colOnNum && jtCtrl.colOnList[c] && ((*(bool*)(rrow + c)) || memcmp(lValue, rValue, tDataTypes[jtInputColType[c]].bytes) <= 0)) {
          filterOut = true;
          break;
        }

        if (jtCtrl.rightFilterNum && jtCtrl.rightFilterColList[c] && ((*(bool*)(rrow + c)) || memcmp(rValue, filterValue, tDataTypes[jtInputColType[c]].bytes) <= 0)) {
          filterOut = true;
          break;
        }
      }

      if (filterOut) {
        continue;
      }

      putMatchRowToRes(lrow, rrow);
      rowMatch = true;
    }

    if (!rowMatch) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
    }
  }
  

}

void createTsEqGrpRows(SSDataBlock** ppLeft, SSDataBlock** ppRight, int32_t probeBlkId, int32_t leftGrpRows, int32_t rightGrpRows) {
  if (leftGrpRows <= 0 && rightGrpRows <= 0) {
    return;
  }

  ++jtCtrl.curTs;

  if (NULL == ppLeft) {
    *ppLeft = createDummyBlock(LEFT_BLK_ID);
    blockDataEnsureCapacity(*ppLeft, jtCtrl.blkRows);
    taosArrayPush(jtCtrl.leftBlkList, ppLeft);
  }

  if (NULL == ppRight) {
    *ppRight = createDummyBlock(RIGHT_BLK_ID);
    blockDataEnsureCapacity(*ppRight, jtCtrl.blkRows);
    taosArrayPush(jtCtrl.rightBlkList, ppRight);
  }


  makeAppendBlkData(ppLeft, ppRight, leftGrpRows, rightGrpRows);

  leftJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
}


void createBothBlkRowsData(void) {
  SSDataBlock* pLeft = NULL;
  SSDataBlock* pRight = NULL;

  bool leftEnd = taosRand() % 2 == 0;
  bool rightEnd = taosRand() % 2 == 0;

  jtCtrl.leftTotalRows = taosRand() % jtCtrl.leftMaxRows;
  jtCtrl.rightTotalRows = taosRand() % jtCtrl.rightMaxRows;

  int32_t minTotalRows = TMIN(jtCtrl.leftTotalRows, jtCtrl.rightTotalRows);
  jtCtrl.curTs = TIMESTAMP_FILTER_VALUE - minTotalRows / 5; 

  int32_t leftTotalRows = 0, rightTotalRows = 0;
  int32_t leftGrpRows = 0, rightGrpRows = 0;
  int32_t grpType = 0;
  while (leftTotalRows < jtCtrl.leftTotalRows && rightTotalRows < jtCtrl.rightTotalRows) {
    if (leftTotalRows >= jtCtrl.leftTotalRows) {
      grpType = 1;
    } else if (rightTotalRows >= jtCtrl.rightTotalRows) {
      grpType = 0
    } else {
      grpType = taosRand() % 3;
    }

    leftGrpRows = taosRand() % jtCtrl.leftMaxGrpRows; 
    rightGrpRows = taosRand() % jtCtrl.rightMaxGrpRows; 

    if ((leftTotalRows + leftGrpRows) > jtCtrl.leftTotalRows) {
      leftGrpRows = jtCtrl.leftTotalRows - leftTotalRows;
    }

    if ((rightTotalRows + rightGrpRows) > jtCtrl.rightTotalRows) {
      rightGrpRows = jtCtrl.rightTotalRows - rightTotalRows;
    }

    switch (grpType) {
      case 0:
        createGrpRows(&pLeft, LEFT_BLK_ID, leftGrpRows);
        leftTotalRows += leftGrpRows;
        break;
      case 1:
        createGrpRows(&pRight, RIGHT_BLK_ID, rightGrpRows);
        rightTotalRows += rightGrpRows;
        break;
      case 2:
        createTsEqGrpRows(&pLeft, &pRight, leftGrpRows, rightGrpRows);
        leftTotalRows += leftGrpRows;
        rightTotalRows += rightGrpRows;
        break;
    }
  }
}

void createDummyBlkList(int32_t leftMaxRows, int32_t leftMaxGrpRows, int32_t rightMaxRows, int32_t rightMaxGrpRows, int32_t blkRows) {
  jtCtrl.leftMaxRows = leftMaxRows;
  jtCtrl.leftMaxGrpRows = leftMaxGrpRows;
  jtCtrl.rightMaxRows = rightMaxRows;
  jtCtrl.rightMaxGrpRows = rightMaxGrpRows;
  jtCtrl.blkRows = blkRows;

  createBothBlkRowsData();
}

void rerunBlockedHere() {
  while (jtInRerun) {
    taosSsleep(1);
  }
}


SSDataBlock* getDummyInputBlock(struct SOperatorInfo* pOperator, int32_t idx) {
  switch (idx) {
    case LEFT_BLK_ID:
      if (jtCtrl.leftBlkReadIdx >= taosArrayGetSize(jtCtrl.leftBlkList)) {
        return NULL;
      }
      return taosArrayGet(jtCtrl.leftBlkList, jtCtrl.leftBlkReadIdx++);
      break;
    case RIGHT_BLK_ID:
      if (jtCtrl.rightBlkReadIdx >= taosArrayGetSize(jtCtrl.rightBlkList)) {
        return NULL;
      }
      return taosArrayGet(jtCtrl.rightBlkList, jtCtrl.rightBlkReadIdx++);
      break;
    default:
      return NULL;
  }
}


void joinTestReplaceRetrieveFp() {
  static Stub stub;
  stub.set(getNextBlockFromDownstreamRemain, getDummyInputBlock);
  {
#ifdef WINDOWS
    AddrAny                       any;
    std::map<std::string, void *> result;
    any.get_func_addr("rpcSendRecv", result);
#endif
#ifdef LINUX
    AddrAny                       any("libtransport.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^rpcSendRecv$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, ctgTestRspDbVgroups);
    }
  }
}

void printColList(char* title, bool left, int32_t* colList, bool filter, char* opStr) {
  bool first = true;
  
  printf("\t%s:", title);
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (colList[i]) {
      if (!first) {
        printf(" AND ");
      }
      first = false;
      if (filter) {
        printf("%sc%d%s%" PRId64 , left ? "l" : "r", i, opStr, jtFilterValue[i]);
      } else {
        printf("lc%d%src%d", i, opStr, i);
      }
    }
  }
  printf("\n");
}

void printBasicInfo(char* caseName) {
  printf("\n TEST %s START\nBasic Info:\n\t asc:%d\n\t filter:%d\n\t leftMaxRows:%d\n\t leftMaxGrpRows:%d\n\t "
    "rightMaxRows:%d\n\t rightMaxGrpRows:%d\n\t blkRows:%d\n\t colCond:%s\n\t \n\tjoinType:%s\n\t "
    "subType:%s\n ", caseName, jtCtrl.asc, jtCtrl.filter, jtCtrl.leftMaxRows, jtCtrl.leftMaxGrpRows,
    jtCtrl.rightMaxRows, jtCtrl.rightMaxGrpRows, jtCtrl.blkRows, jtColCondStr[jtCtrl.colCond], jtJoinTypeStr[jtCtrl.joinType],
    jtSubTypeStr[jtCtrl.subType]);
    
  printf("\nInput Info:\n\t leftBlkRead:%d\n\t leftTotalBlk:%d\n\t leftTotalRows:%d\n\t rightBlkRead:%d\n\t "
    "rightTotalBlk:%d\n\t rightTotalRows:%d\n\t blkRowSize:%d\n\t leftCols:%s %s %s %s\n\t rightCols:%s %s %s %s\n", 
    caseName, jtCtrl.leftBlkReadIdx, taosArrayGetSize(jtCtrl.leftBlkList), 
    jtCtrl.leftTotalRows,  jtCtrl.rightBlkReadIdx, taosArrayGetSize(jtCtrl.rightBlkList), jtCtrl.rightTotalRows,
    jtCtrl.blkRowSize, tDataTypes[jtInputColType[0]].name, tDataTypes[jtInputColType[1]].name,
    tDataTypes[jtInputColType[2]].name, tDataTypes[jtInputColType[3]].name, tDataTypes[jtInputColType[0]].name, 
    tDataTypes[jtInputColType[1]].name, tDataTypes[jtInputColType[2]].name, tDataTypes[jtInputColType[3]].name,
    jtCtrl.colEqNum);

  if (jtCtrl.colEqNum) {
    printf("colEqNum:%d\n", jtCtrl.colEqNum);
    printColList("colEqList", false, jtCtrl.colEqList, false, "=");
  }

  if (jtCtrl.colOnNum) {
    printf("colOnNum:%d\n", jtCtrl.colOnNum);
    printColList("colOnList", false, jtCtrl.colOnList, false, ">");
  }  

  if (jtCtrl.colOnNum) {
    printf("colOnNum:%d\n", jtCtrl.colOnNum);
    printColList("colOnList", false, jtCtrl.colOnList, false, ">");
  }  

  if (jtCtrl.leftFilterNum) {
    printf("leftFilterNum:%d\n", jtCtrl.leftFilterNum);
    printColList("leftFilterList", true, jtCtrl.leftFilterColList, true, ">");
  }

  if (jtCtrl.rightFilterNum) {
    printf("rightFilterNum:%d\n", jtCtrl.rightFilterNum);
    printColList("rightFilterList", false, jtCtrl.rightFilterColList, true, ">");
  }

  printf("\tresColSize:%d\n\t resColNum:%d\n\t resColList:");
  for (int32_t i = 0; i < jtCtrl.resColNum; ++i) {
    int32_t s = jtCtrl.resColInSlot[i];
    printf("%sc%d[%s]\t", s >= MAX_SLOT_NUM ? "r" : "l", s, tDataTypes[jtInputColType[s]].name);
  }
}

void printOutputInfo() {
  printf("\nOutput Info:\n\t expectedRows:%d\n\t ", tSimpleHashGetSize(jtCtrl.jtResRows));
  printf("Actual Result:\n\t");
}

void printActualResInfo(int32_t expectedRows) {
  printf("\nActual Result Summary:\n\t blkNum:%d\n\t rowNum:%d%s\n\t +rows:%d%s\n\t "
    "-rows:%d%s\n\t mismatchRows:%d%s\n\t matchRows:%d%s\n", 
    jtRes.blkNum, jtRes.rowNum, jtRes.rowNum == expectedRows ? "" : "*",
    jtRes.addRowNum, jtRes.addRowNum ? "*" : "",
    jtRes.subRowNum, jtRes.subRowNum ? "*" : "",
    jtRes.mismatchNum, jtRes.mismatchNum ? "*" : "",
    jtRes.matchNum, jtRes.matchNum == expectedRows ? "" : "*");
}

void checkJoinDone(char* caseName) {
  int32_t iter = 0;
  void* p = NULL;
  while (NULL != (p = tSimpleHashIterate(jtCtrl.jtResRows, p, &iter))) {
    jtRes.succeed = false;
    jtRes.subRowNum++;
    printResRow(p, 0);
  }

  printActualResInfo(tSimpleHashGetSize(jtCtrl.jtResRows));
  printf("\n%s Final Result: %s\n", caseName, jtRes.succeed ? "SUCCEED" : "FAILED");

}

void checkJoinRes(SSDataBlock* pBlock) {
  jtRes.rowNum += pBlock->info.rows;
  jtRes.blkNum++;
  
  for (int32_t r = 0; r < pBlock->info.rows; ++r) {
    for (int32_t c = 0; c < jtCtrl.resColNum; ++c) {
      int32_t slot = jtCtrl.resColInSlot[c];
      SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, c);
      memset(jtCtrl.resColBuf, 0, jtCtrl.resColSize);
      switch (jtInputColType[slot % MAX_SLOT_NUM]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          if (colDataIsNull_s(pCol, r)) {
            *(bool*)(jtCtrl.resColBuf + slot) = true;
          } else {
            *(int64_t*)(jtCtrl.resColBuf + jtCtrl.resColOffset[slot]) = *(int64_t*)colDataGetData(pCol, r);
          }
          break;
        case TSDB_DATA_TYPE_INT:
          if (colDataIsNull_s(pCol, r)) {
            *(bool*)(jtCtrl.resColBuf + slot) = true;
          } else {
            *(int32_t*)(jtCtrl.resColBuf + jtCtrl.resColOffset[slot]) = *(int32_t*)colDataGetData(pCol, r);
          }
          break;
        default:
          break;
      }
    }
    
    void* value = tSimpleHashGet(jtCtrl.jtResRows, jtCtrl.resColBuf + jtCtrl.keyColOffset, sizeof(int64_t));
    if (NULL == value) {
      printResRow(jtCtrl.resColBuf, 1);
      jtRes.succeed = false;
      jtRes.addRowNum++;
      continue;
    }

    if (memcmp(value, jtCtrl.resColBuf, jtCtrl.resColSize)) {
      printResRow(jtCtrl.resColBuf, 1);
      printResRow(value, 0);
      tSimpleHashRemove(jtCtrl.jtResRows, jtCtrl.resColBuf + jtCtrl.keyColOffset, sizeof(int64_t));
      jtRes.succeed = false;
      jtRes.mismatchNum++;
      continue;
    }

    jtRes.matchNum++;
  }
}

void resetForJoinRerun(SOperatorInfo* pDownstreams, int32_t dsNum, SSortMergeJoinPhysiNode* pNode, SExecTaskInfo* pTask) {
  jtCtrl.leftBlkReadIdx = 0;
  jtCtrl.rightBlkReadIdx = 0;

  jtCtrl.pJoinOp = createMergeJoinOperatorInfo(&pDownstreams, 2, pNode, pTask);
  ASSERT_TRUE(NULL != jtCtrl.pJoinOp);
}

void handleJoinError(bool* contLoop) {
  if (jtRes.succeed) {
    *contLoop = false;
    return;
  }
  
  if (jtErrorRerun) {
    *contLoop = false;
    return;
  }

  jtInRerun = true;
  
  destroyMergeJoinOperator(jtCtrl.pJoinOp);
  jtCtrl.pJoinOp = NULL;
}


}  // namespace

TEST(leftOuterJoin, noCondTest) {
  char* caseName = "leftOuterJoin:noCondTest";
  SOperatorInfo* pDownstreams = createDummyDownstreamOperators(2);
  SSortMergeJoinPhysiNode* pNode = createDummySortMergeJoinPhysiNode(JOIN_TYPE_LEFT, JOIN_STYPE_OUTER, TEST_NO_COND, false, true);
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  bool contLoop = true;
  
  createDummyBlkList(10, 10, 10, 10, 2);
  
  while (contLoop) {
    rerunBlockedHere();
    resetForJoinRerun(pDownstreams, 2, pNode, pTask);
    printBasicInfo();
    printOutputInfo();
    
    SSDataBlock* pBlock = jtCtrl.pJoinOp->fpSet.getNextFn(jtCtrl.pJoinOp);
    if (NULL == pBlock) {
      checkJoinDone(caseName);
    } else {
      checkJoinRes(pBlock);
      continue;
    }

    handleJoinError(&contLoop);
  }
  
  //ASSERT_EQ(num, ekeyNum - pos + 1);
}


int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  initJoinTest();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



#pragma GCC diagnosti
