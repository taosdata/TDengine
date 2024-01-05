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
#pragma GCC diagnostic ignored "-Wformat"
#include <addr_any.h>

#include "os.h"

#include "executor.h"
#include "executorInt.h"
#include "function.h"
#include "operator.h"
#include "taos.h"
#include "tdatablock.h"
#include "tdef.h"
#include "tvariant.h"
#include "stub.h"
#include "querytask.h"


namespace {

typedef struct {
  bool    succeed;
  int32_t blkNum;
  int32_t rowNum;
  int32_t addRowNum;
  int32_t subRowNum;
  int32_t matchNum;
} SJoinTestResInfo;

typedef struct {
  int32_t maxResRows;
  int32_t maxResBlkRows;
  int64_t totalResRows;
  int64_t useMSecs;
  SArray* pHistory;
} SJoinTestStat;


enum {
  TEST_NO_COND = 1,
  TEST_EQ_COND,
  TEST_ON_COND,
  TEST_FULL_COND
};

#define COL_DISPLAY_WIDTH 18
#define JT_MAX_LOOP       1000

#define LEFT_BLK_ID       0
#define RIGHT_BLK_ID      1
#define RES_BLK_ID        2
#define MAX_SLOT_NUM      4

#define JT_KEY_SOLT_ID    (MAX_SLOT_NUM - 1)
int32_t jtInputColType[MAX_SLOT_NUM] = {TSDB_DATA_TYPE_TIMESTAMP, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_BIGINT};

char* jtColCondStr[] = {"", "NO COND", "EQ COND", "ON COND", "FULL COND"};
char* jtJoinTypeStr[] = {"INNER", "LEFT", "RIGHT", "FULL"};
char* jtSubTypeStr[] = {"", "OUTER", "SEMI", "ANTI", "ASOF", "WINDOW"};

int64_t TIMESTAMP_FILTER_VALUE = 10000000000;
int32_t INT_FILTER_VALUE = 200000000;
int64_t BIGINT_FILTER_VALUE = 3000000000000000;

int64_t jtFilterValue[] = {TIMESTAMP_FILTER_VALUE, INT_FILTER_VALUE, INT_FILTER_VALUE, BIGINT_FILTER_VALUE};

bool jtErrorRerun = false;
bool jtInRerun = false;

typedef struct {
  bool printTestInfo;
  bool printInputRow;
  bool printResRow;
  bool logHistory;
} SJoinTestCtrl;


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
  int32_t inputStat;

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
  char*   resColBuf;

  int32_t colRowDataBufSize;
  char*   colRowDataBuf;
  int32_t colRowOffset[MAX_SLOT_NUM];

  int64_t curTs;
  int64_t curKeyOffset;
  int32_t grpOffset[MAX_SLOT_NUM];

  int32_t leftBlkReadIdx;
  SArray* leftBlkList;
  int32_t rightBlkReadIdx;
  SArray* rightBlkList;  

  int64_t    resRows;
  SSHashObj* jtResRows;

  SOperatorInfo* pJoinOp;

  int32_t  loopIdx;

  int32_t  rightFinMatchNum;
  bool*    rightFinMatch;
} SJoinTestCtx;

typedef struct {
  SJoinTestResInfo res;
  SJoinTestCtx     ctx;
} SJoinTestHistory;

typedef struct {
  EJoinType joinType;
  EJoinSubType subType;
  int32_t cond;
  bool    filter;
  bool    asc;
  SExecTaskInfo* pTask;
} SJoinTestParam;


SJoinTestCtx jtCtx = {0};
SJoinTestCtrl jtCtrl = {1, 1, 1, 0};
SJoinTestStat jtStat = {0};
SJoinTestResInfo jtRes = {0};



void printResRow(char* value, int32_t type) {
  if (!jtCtrl.printResRow) {
    return;
  }
  
  printf(" ");
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    int32_t slot = jtCtx.resColInSlot[i];
    if (*(bool*)(value + slot)) {
      printf("%18s", " NULL");
      continue;
    }
    
    switch (jtInputColType[slot % MAX_SLOT_NUM]) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        printf("%18" PRId64 , *(int64_t*)(value + jtCtx.resColOffset[slot]));
        break;
      case TSDB_DATA_TYPE_INT:
        printf("%18d", *(int32_t*)(value + jtCtx.resColOffset[slot]));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        printf("%18" PRId64, *(int64_t*)(value + jtCtx.resColOffset[slot]));
        break;
    }
  }
  printf("\t %s\n", 0 == type ? "-" : (1 == type ? "+" : ""));
}

void pushResRow() {
  jtCtx.resRows++;
  
  int32_t* rows = (int32_t*)tSimpleHashGet(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
  if (rows) {
    (*rows)++;
  } else {
    int32_t n = 1;
    tSimpleHashPut(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize, &n, sizeof(n));
  }
}

void rmResRow() {
  int32_t* rows = (int32_t*)tSimpleHashGet(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
  if (rows) {
    (*rows)--;
    if ((*rows) == 0) {
      tSimpleHashRemove(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
    }
  } else {
    ASSERT(0);
  }
}

static int32_t jtMergeEqCond(SNode** ppDst, SNode** ppSrc) {
  if (NULL == *ppSrc) {
    return TSDB_CODE_SUCCESS;
  }
  if (NULL == *ppDst) {
    *ppDst = *ppSrc;
    *ppSrc = NULL;
    return TSDB_CODE_SUCCESS;
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppSrc)) {
    TSWAP(*ppDst, *ppSrc);
  }
  if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppDst)) {
    SLogicConditionNode* pLogic = (SLogicConditionNode*)*ppDst;
    if (QUERY_NODE_LOGIC_CONDITION == nodeType(*ppSrc)) {
      nodesListStrictAppendList(pLogic->pParameterList, ((SLogicConditionNode*)(*ppSrc))->pParameterList);
      ((SLogicConditionNode*)(*ppSrc))->pParameterList = NULL;
    } else {
      nodesListStrictAppend(pLogic->pParameterList, *ppSrc);
      *ppSrc = NULL;
    }
    nodesDestroyNode(*ppSrc);
    *ppSrc = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SLogicConditionNode* pLogicCond = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
  if (NULL == pLogicCond) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  pLogicCond->pParameterList = nodesMakeList();
  nodesListStrictAppend(pLogicCond->pParameterList, *ppSrc);
  nodesListStrictAppend(pLogicCond->pParameterList, *ppDst);

  *ppDst = (SNode*)pLogicCond;
  *ppSrc = NULL;

  return TSDB_CODE_SUCCESS;
}


void createDummyDownstreamOperators(int32_t num, SOperatorInfo** ppRes) {
  for (int32_t i = 0; i < num; ++i) {
    SOperatorInfo* p = (SOperatorInfo*)taosMemoryCalloc(1, sizeof(SOperatorInfo));
    p->resultDataBlockId = i;
    ppRes[i] = p;
  }
}

void createTargetSlotList(SSortMergeJoinPhysiNode* p) {
  jtCtx.resColNum = 0;
  memset(jtCtx.resColList, 0, sizeof(jtCtx.resColList));
  jtCtx.resColSize = MAX_SLOT_NUM * 2 * sizeof(bool);
  jtCtx.keyInSlotIdx = -1;

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colOnList[i] || jtCtx.colEqList[i] || jtCtx.leftFilterColList[i]) {
      jtCtx.resColList[i] = 1;
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colOnList[i] || jtCtx.colEqList[i] || jtCtx.rightFilterColList[i]) {
      jtCtx.resColList[MAX_SLOT_NUM + i] = 1;
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (0 == jtCtx.resColList[i]) {
      jtCtx.resColList[i]= taosRand() % 2;
    }

    if ((jtCtx.joinType == JOIN_TYPE_LEFT || jtCtx.joinType == JOIN_TYPE_FULL) && (i == JT_KEY_SOLT_ID)) {
      jtCtx.resColList[i] = 1;
    }

    if (jtCtx.resColList[i] && i == JT_KEY_SOLT_ID && (jtCtx.joinType == JOIN_TYPE_LEFT || jtCtx.joinType == JOIN_TYPE_FULL)) {
      jtCtx.keyInSlotIdx = JT_KEY_SOLT_ID;
    }
  }

  if (jtCtx.keyInSlotIdx < 0 || ((jtCtx.joinType == JOIN_TYPE_RIGHT || jtCtx.joinType == JOIN_TYPE_FULL))) {
    jtCtx.resColList[MAX_SLOT_NUM + JT_KEY_SOLT_ID]= 1;
    jtCtx.keyInSlotIdx = JT_KEY_SOLT_ID + MAX_SLOT_NUM;    
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (0 == jtCtx.resColList[MAX_SLOT_NUM + i]) {
      jtCtx.resColList[MAX_SLOT_NUM + i]= taosRand() % 2;
    }
  }

  int32_t idx = 0;
  int32_t dstIdx = 0;
  int32_t dstOffset = jtCtx.resColSize;

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.resColList[i]) {
      jtCtx.resColOffset[i] = dstOffset;
      jtCtx.resColInSlot[dstIdx] = i;
      if (jtCtx.keyInSlotIdx == i) {
        jtCtx.keyColOffset = dstOffset;
      }

      STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = LEFT_BLK_ID;
      pCol->slotId = i;
      pTarget->dataBlockId = RES_BLK_ID;
      pTarget->slotId = dstIdx++;
      pTarget->pExpr = (SNode*)pCol;
      dstOffset += tDataTypes[jtInputColType[i]].bytes;
      jtCtx.resColSize += tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(&p->pTargets, (SNode*)pTarget);

      jtCtx.resColNum++;
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.resColList[MAX_SLOT_NUM + i]) {
      jtCtx.resColOffset[MAX_SLOT_NUM + i] = dstOffset;
      jtCtx.resColInSlot[dstIdx] = i + MAX_SLOT_NUM;
      if (jtCtx.keyInSlotIdx == (i + MAX_SLOT_NUM)) {
        jtCtx.keyColOffset = dstOffset;
      }

      STargetNode* pTarget = (STargetNode*)nodesMakeNode(QUERY_NODE_TARGET);
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = RIGHT_BLK_ID;
      pCol->slotId = i;
      pTarget->dataBlockId = RES_BLK_ID;
      pTarget->slotId = dstIdx++;
      pTarget->pExpr = (SNode*)pCol;
      dstOffset += tDataTypes[jtInputColType[i]].bytes;
      jtCtx.resColSize += tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(&p->pTargets, (SNode*)pTarget);
      jtCtx.resColNum++;
    }
  }  

  jtCtx.resColBuf = (char*)taosMemoryRealloc(jtCtx.resColBuf, jtCtx.resColSize);
}

void createColEqCondStart(SSortMergeJoinPhysiNode* p) {
  jtCtx.colEqNum = 0;
  do {
    jtCtx.colEqNum = taosRand() % MAX_SLOT_NUM; // except TIMESTAMP
  } while (0 == jtCtx.colEqNum);

  int32_t idx = 0;
  memset(jtCtx.colEqList, 0, sizeof(jtCtx.colEqList));
  for (int32_t i = 0; i < jtCtx.colEqNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtx.colEqList[idx]) {
      continue;
    }
    if (TSDB_DATA_TYPE_TIMESTAMP == jtInputColType[idx]) {
      continue;
    }
    jtCtx.colEqList[idx] = 1;
    ++i;
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colEqList[i]) {
      SColumnNode* pCol1 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol1->dataBlockId = LEFT_BLK_ID;
      pCol1->slotId = i;
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(&p->pEqLeft, (SNode*)pCol1);

      SColumnNode* pCol2 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol2->dataBlockId = RIGHT_BLK_ID;
      pCol2->slotId = i;
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      nodesListMakeStrictAppend(&p->pEqRight, (SNode*)pCol2);
    }
  }
}

void createColOnCondStart(SSortMergeJoinPhysiNode* p) {
  jtCtx.colOnNum = 0;
  do {
    jtCtx.colOnNum = taosRand() % (MAX_SLOT_NUM + 1);
  } while (0 == jtCtx.colOnNum || (jtCtx.colOnNum + jtCtx.colEqNum) > MAX_SLOT_NUM);

  int32_t idx = 0;
  memset(jtCtx.colOnList, 0, sizeof(jtCtx.colOnList));
  for (int32_t i = 0; i < jtCtx.colOnNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtx.colOnList[idx] || jtCtx.colEqList[idx]) {
      continue;
    }
    jtCtx.colOnList[idx] = 1;
    ++i;
  }
}

int32_t getDstSlotId(int32_t srcIdx) {
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    if (jtCtx.resColInSlot[i] == srcIdx) {
      return i;
    }
  }

  return -1;
}


void createColEqCondEnd(SSortMergeJoinPhysiNode* p) {
  if (jtCtx.colEqNum <= 0) {
    return;
  }

  SLogicConditionNode* pLogic = NULL;
  if (jtCtx.colEqNum > 1) {
    pLogic = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    pLogic->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogic->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colEqList[i]) {
      SColumnNode* pCol1 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol1->dataBlockId = RES_BLK_ID;
      pCol1->slotId = getDstSlotId(i);
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      SColumnNode* pCol2 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol2->dataBlockId = RES_BLK_ID;
      pCol2->slotId = getDstSlotId(MAX_SLOT_NUM + i);
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;

      SOperatorNode* pOp = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_EQUAL;
      pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      pOp->pLeft = (SNode*)pCol1;
      pOp->pRight = (SNode*)pCol2;

      if (jtCtx.colEqNum > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp);
      } else {
        p->pFullOnCond = (SNode*)pOp;
        break;
      }
    }
  }

  if (jtCtx.colEqNum > 1) {
    p->pFullOnCond = (SNode*)pLogic;
  }  
}

void createColOnCondEnd(SSortMergeJoinPhysiNode* p) {
  if (jtCtx.colOnNum <= 0) {
    return;
  }

  SLogicConditionNode* pLogic = NULL;
  if (jtCtx.colOnNum > 1) {
    pLogic = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    pLogic->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogic->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colOnList[i]) {
      SColumnNode* pCol1 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol1->dataBlockId = RES_BLK_ID;
      pCol1->slotId = getDstSlotId(i);
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      SColumnNode* pCol2 = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol2->dataBlockId = RES_BLK_ID;
      pCol2->slotId = getDstSlotId(MAX_SLOT_NUM + i);
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      SOperatorNode* pOp = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      pOp->pLeft = (SNode*)pCol1;
      pOp->pRight = (SNode*)pCol2;

      if (jtCtx.colOnNum > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp);
      } else {
        p->pColOnCond = (SNode*)pOp;
        break;
      }
    }
  }

  if (jtCtx.colOnNum > 1) {
    p->pColOnCond = (SNode*)pLogic;
  }  

  SNode* pTmp = nodesCloneNode(p->pColOnCond);
  jtMergeEqCond(&p->pFullOnCond, &pTmp);
}



void createColCond(SSortMergeJoinPhysiNode* p, int32_t cond) {
  jtCtx.colCond = cond;
  switch (cond) {
    case TEST_NO_COND:
      jtCtx.colEqNum = 0;
      jtCtx.colOnNum = 0;
      memset(jtCtx.colEqList, 0, sizeof(jtCtx.colEqList));
      memset(jtCtx.colOnList, 0, sizeof(jtCtx.colOnList));
      break;
    case TEST_EQ_COND:
      createColEqCondStart(p);
      jtCtx.colOnNum = 0;
      memset(jtCtx.colOnList, 0, sizeof(jtCtx.colOnList));
      break;
    case TEST_ON_COND:
      createColOnCondStart(p);
      jtCtx.colEqNum = 0;
      memset(jtCtx.colEqList, 0, sizeof(jtCtx.colEqList));
      break;
    case TEST_FULL_COND:
      createColEqCondStart(p);
      createColOnCondStart(p);
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

void createFilterStart(SSortMergeJoinPhysiNode* p, bool filter) {
  jtCtx.filter = filter;
  if (!filter) {
    jtCtx.leftFilterNum = 0;
    jtCtx.rightFilterNum = 0;
    memset(jtCtx.leftFilterColList, 0, sizeof(jtCtx.leftFilterColList));
    memset(jtCtx.rightFilterColList, 0, sizeof(jtCtx.rightFilterColList));
    return;
  }
  
  jtCtx.leftFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
  if (0 == jtCtx.leftFilterNum) {
    do {
      jtCtx.rightFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
    } while (0 == jtCtx.rightFilterNum);
  } else {
    jtCtx.rightFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
  }

  int32_t idx = 0;
  memset(jtCtx.leftFilterColList, 0, sizeof(jtCtx.leftFilterColList));
  memset(jtCtx.rightFilterColList, 0, sizeof(jtCtx.rightFilterColList));
  for (int32_t i = 0; i < jtCtx.leftFilterNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtx.leftFilterColList[idx]) {
      continue;
    }
    jtCtx.leftFilterColList[idx] = 1;
    ++i;
  }

  for (int32_t i = 0; i < jtCtx.rightFilterNum; ) {
    idx = taosRand() % MAX_SLOT_NUM;
    if (jtCtx.rightFilterColList[idx]) {
      continue;
    }
    jtCtx.rightFilterColList[idx] = 1;
    ++i;
  }
}

void createFilterEnd(SSortMergeJoinPhysiNode* p, bool filter) {
  if (!filter || (jtCtx.leftFilterNum <= 0 && jtCtx.rightFilterNum <= 0)) {
    return;
  }
  
  SLogicConditionNode* pLogic = NULL;
  if ((jtCtx.leftFilterNum + jtCtx.rightFilterNum) > 1) {
    pLogic = (SLogicConditionNode*)nodesMakeNode(QUERY_NODE_LOGIC_CONDITION);
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.leftFilterColList[i]) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = RES_BLK_ID;
      pCol->slotId = getDstSlotId(i);
      pCol->node.resType.type = jtInputColType[i];
      pCol->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      sprintf(pCol->colName, "l%d", i);

      SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
      pVal->node.resType.type = jtInputColType[i];
      pVal->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      nodesSetValueNodeValue(pVal, getFilterValue(jtInputColType[i]));

      SOperatorNode* pOp = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->pLeft = (SNode*)pCol;
      pOp->pRight = (SNode*)pVal;

      if ((jtCtx.leftFilterNum + jtCtx.rightFilterNum) > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp);
      } else {
        p->node.pConditions = (SNode*)pOp;
        break;
      }
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.rightFilterColList[i]) {
      SColumnNode* pCol = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      pCol->dataBlockId = RES_BLK_ID;
      pCol->slotId = getDstSlotId(MAX_SLOT_NUM + i);
      pCol->node.resType.type = jtInputColType[i];
      pCol->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      sprintf(pCol->colName, "r%d", i);

      SValueNode* pVal = (SValueNode*)nodesMakeNode(QUERY_NODE_VALUE);
      pVal->node.resType.type = jtInputColType[i];
      pVal->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      nodesSetValueNodeValue(pVal, getFilterValue(jtInputColType[i]));

      SOperatorNode* pOp = (SOperatorNode*)nodesMakeNode(QUERY_NODE_OPERATOR);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->pLeft = (SNode*)pCol;
      pOp->pRight = (SNode*)pVal;

      if ((jtCtx.leftFilterNum + jtCtx.rightFilterNum) > 1) {
        nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp);
      } else {
        p->node.pConditions = (SNode*)pOp;
        break;
      }
    }
  }

  if ((jtCtx.leftFilterNum + jtCtx.rightFilterNum) > 1) {
    p->node.pConditions = (SNode*)pLogic;
  }  
}


void updateColRowInfo() {
  jtCtx.blkRowSize = MAX_SLOT_NUM * sizeof(bool);

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    jtCtx.colRowOffset[i] = jtCtx.blkRowSize;
    jtCtx.blkRowSize += tDataTypes[jtInputColType[i]].bytes;
  }
}

void createBlockDescNode(SDataBlockDescNode** ppNode) {
  SDataBlockDescNode* pDesc = (SDataBlockDescNode*)nodesMakeNode(QUERY_NODE_DATABLOCK_DESC);
  pDesc->dataBlockId = RES_BLK_ID;
  pDesc->totalRowSize = jtCtx.resColSize - MAX_SLOT_NUM * 2 * sizeof(bool);
  pDesc->outputRowSize = pDesc->totalRowSize;
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    SSlotDescNode* pSlot = (SSlotDescNode*)nodesMakeNode(QUERY_NODE_SLOT_DESC);
    pSlot->slotId = i;
    int32_t slotIdx = jtCtx.resColInSlot[i] >= MAX_SLOT_NUM ? jtCtx.resColInSlot[i] - MAX_SLOT_NUM : jtCtx.resColInSlot[i];
    pSlot->dataType.type = jtInputColType[slotIdx];
    pSlot->dataType.bytes = tDataTypes[pSlot->dataType.type].bytes;

    nodesListMakeStrictAppend(&pDesc->pSlots, (SNode *)pSlot);
  }

  *ppNode = pDesc;
}

SSortMergeJoinPhysiNode* createDummySortMergeJoinPhysiNode(EJoinType type, EJoinSubType sub, int32_t cond, bool filter, bool asc) {
  SSortMergeJoinPhysiNode* p = (SSortMergeJoinPhysiNode*)nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN);
  p->joinType = type;
  p->subType = sub;
  p->leftPrimSlotId = 0;
  p->rightPrimSlotId = 0;
  p->node.inputTsOrder = asc ? ORDER_ASC : ORDER_DESC;

  jtCtx.joinType = type;
  jtCtx.subType = sub;
  jtCtx.asc = asc;

  createColCond(p, cond);
  createFilterStart(p, filter);
  createTargetSlotList(p);
  createColEqCondEnd(p);
  createColOnCondEnd(p);
  createFilterEnd(p, filter);
  updateColRowInfo();
  createBlockDescNode(&p->node.pOutputDataBlockDesc);

  return p;
}

SExecTaskInfo* createDummyTaskInfo(char* taskId) {
  SExecTaskInfo* p = (SExecTaskInfo*)taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  p->id.str = taskId;

  return p;
}

SSDataBlock* createDummyBlock(int32_t blkId) {
  SSDataBlock* p = createDataBlock();

  p->info.id.blockId = blkId;
  p->info.type = STREAM_INVALID;
  p->info.calWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  p->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    SColumnInfoData idata =
        createColumnInfoData(jtInputColType[i], tDataTypes[jtInputColType[i]].bytes, i);

    blockDataAppendColInfo(p, &idata);
  }

  return p;
}

void createGrpRows(SSDataBlock** ppBlk, int32_t blkId, int32_t grpRows) {
  if (grpRows <= 0) {
    return;
  }

  if (NULL == *ppBlk) {
    *ppBlk = createDummyBlock((blkId == LEFT_BLK_ID) ? LEFT_BLK_ID : RIGHT_BLK_ID);
    blockDataEnsureCapacity(*ppBlk, jtCtx.blkRows);
    taosArrayPush((blkId == LEFT_BLK_ID) ? jtCtx.leftBlkList : jtCtx.rightBlkList, ppBlk);
  }

  jtCtx.inputStat |= (1 << blkId);

  int32_t tableOffset = 0;
  int32_t peerOffset = 0;
  bool keepRes = false;
  if (blkId == LEFT_BLK_ID) {
    if ((jtCtx.joinType == JOIN_TYPE_LEFT || jtCtx.joinType == JOIN_TYPE_FULL) && jtCtx.subType != JOIN_STYPE_SEMI) {
      keepRes = true;
    }
    peerOffset = MAX_SLOT_NUM;
  } else {
    if ((jtCtx.joinType == JOIN_TYPE_RIGHT || jtCtx.joinType == JOIN_TYPE_FULL) && jtCtx.subType != JOIN_STYPE_SEMI) {
      keepRes = true;
    }
    tableOffset = MAX_SLOT_NUM;
  }
  
  int32_t filterNum = (blkId == LEFT_BLK_ID) ? jtCtx.leftFilterNum : jtCtx.rightFilterNum;
  int32_t peerFilterNum = (blkId == LEFT_BLK_ID) ? jtCtx.rightFilterNum : jtCtx.leftFilterNum;
  int32_t* filterCol = (blkId == LEFT_BLK_ID) ? jtCtx.leftFilterColList : jtCtx.rightFilterColList;
  
  char* pData = NULL;
  int32_t tmpInt = 0;
  int64_t tmpBigint = 0;
  bool isNull = false;
  bool filterOut = false;
  int32_t vRange = TMAX(grpRows / 3, 3);
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    jtCtx.grpOffset[c] = c * TMAX(100, grpRows);
  }
  
  for (int32_t i = 0; i < grpRows; ++i) {
    if ((*ppBlk)->info.rows >= (*ppBlk)->info.capacity) {
      *ppBlk = createDummyBlock((blkId == LEFT_BLK_ID) ? LEFT_BLK_ID : RIGHT_BLK_ID);
      blockDataEnsureCapacity(*ppBlk, jtCtx.blkRows);
      taosArrayPush((blkId == LEFT_BLK_ID) ? jtCtx.leftBlkList : jtCtx.rightBlkList, ppBlk);
    }

    filterOut = (peerFilterNum > 0) ? true : false;
    if (!filterOut) {
      memset(jtCtx.resColBuf, 0, jtCtx.resColSize);
    }
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          ++jtCtx.curTs;
          pData = (char*)&jtCtx.curTs;
          isNull = false;
          if (!filterOut && filterNum && filterCol[c] && jtCtx.curTs <= TIMESTAMP_FILTER_VALUE) {
            filterOut = true;
          }
          break;
        case TSDB_DATA_TYPE_INT:
          if (taosRand() % 10) {
            tmpInt = (taosRand() % 2) ? INT_FILTER_VALUE + jtCtx.grpOffset[c] + taosRand() % vRange : INT_FILTER_VALUE - jtCtx.grpOffset[c] - taosRand() % vRange;
            pData = (char*)&tmpInt;
            isNull = false;
            if (filterNum && filterCol[c] && tmpInt <= INT_FILTER_VALUE) {
              filterOut = true;
            }
          } else {
            isNull = true;
            if (filterNum && filterCol[c]) {
              filterOut = true;
            }
          }
          break;
        case TSDB_DATA_TYPE_BIGINT:
          tmpBigint = (taosRand() % 2) ? BIGINT_FILTER_VALUE + jtCtx.curKeyOffset++ : BIGINT_FILTER_VALUE - jtCtx.curKeyOffset++;
          pData = (char*)&tmpBigint;
          isNull = false;
          if (filterNum && filterCol[c] && tmpBigint <= BIGINT_FILTER_VALUE) {
            filterOut = true;
          }
          break;
        default:
          break;
      }
      
      SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet((*ppBlk)->pDataBlock, c);
      colDataSetVal(pCol, (*ppBlk)->info.rows, pData, isNull);

      if (keepRes && !filterOut && jtCtx.resColList[tableOffset + c]) {
        if (isNull) {
          *(char*)(jtCtx.resColBuf + tableOffset + c) = true;
        } else {
          memcpy(jtCtx.resColBuf + jtCtx.resColOffset[tableOffset + c], pData, tDataTypes[jtInputColType[c]].bytes);
        }
      }
    }

    if (keepRes && !filterOut) {
      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        if (jtCtx.resColList[peerOffset + c]) {
          *(char*)(jtCtx.resColBuf + peerOffset + c) = true;
        }
      }
      
      pushResRow();
    }
    
    (*ppBlk)->info.rows++;
  }
}

void createRowData(SSDataBlock* pBlk, int64_t tbOffset, int32_t rowIdx, int32_t vRange) {
  int32_t tmpInt = 0;
  int64_t tmpBig = 0;
  
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlk->pDataBlock, c);

    int32_t rv = taosRand() % 10;
    switch (jtInputColType[c]) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        *(int64_t*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + jtCtx.colRowOffset[c]) = jtCtx.curTs;
        colDataSetVal(pCol, pBlk->info.rows, (char*)&jtCtx.curTs, false);
        break;
      case TSDB_DATA_TYPE_INT:
        if (rv) {
          tmpInt = (taosRand() % 2) ? INT_FILTER_VALUE + jtCtx.grpOffset[c] + taosRand() % vRange : INT_FILTER_VALUE - taosRand() % vRange;
          *(int32_t*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + jtCtx.colRowOffset[c]) = tmpInt;
          colDataSetVal(pCol, pBlk->info.rows, (char*)&tmpInt, false);
        } else {
          *(bool*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + c) = true;
          colDataSetVal(pCol, pBlk->info.rows, NULL, true);
        }
        break;
      case TSDB_DATA_TYPE_BIGINT:
        tmpBig = (taosRand() % 2) ? BIGINT_FILTER_VALUE + jtCtx.curKeyOffset++ : BIGINT_FILTER_VALUE - jtCtx.curKeyOffset++;
        *(int64_t*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + jtCtx.colRowOffset[c]) = tmpBig;
        colDataSetVal(pCol, pBlk->info.rows, (char*)&tmpBig, false);
        break;
      default:
        break;
    }
  }

  pBlk->info.rows++;
}

void makeAppendBlkData(SSDataBlock** ppLeft, SSDataBlock** ppRight, int32_t leftGrpRows, int32_t rightGrpRows) {
  int64_t totalSize = (leftGrpRows + rightGrpRows) * jtCtx.blkRowSize;
  int64_t rightOffset = leftGrpRows * jtCtx.blkRowSize;

  if (jtCtx.colRowDataBufSize < totalSize) {
    jtCtx.colRowDataBuf = (char*)taosMemoryRealloc(jtCtx.colRowDataBuf, totalSize);
  }

  memset(jtCtx.colRowDataBuf, 0, totalSize);

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    jtCtx.grpOffset[c] = c * TMAX(leftGrpRows, rightGrpRows);
  }

  int32_t vRange = TMAX(leftGrpRows / 100, 3);
  for (int32_t i = 0; i < leftGrpRows; ++i) {
    if ((*ppLeft)->info.rows >= (*ppLeft)->info.capacity) {
      *ppLeft = createDummyBlock(LEFT_BLK_ID);
      blockDataEnsureCapacity(*ppLeft, jtCtx.blkRows);
      taosArrayPush(jtCtx.leftBlkList, ppLeft);
    }

    createRowData(*ppLeft, 0, i, vRange);
  }

  vRange = TMAX(rightGrpRows / 100, 3);
  for (int32_t i = 0; i < rightGrpRows; ++i) {
    if ((*ppRight)->info.rows >= (*ppRight)->info.capacity) {
      *ppRight = createDummyBlock(RIGHT_BLK_ID);
      blockDataEnsureCapacity(*ppRight, jtCtx.blkRows);
      taosArrayPush(jtCtx.rightBlkList, ppRight);
    }

    createRowData(*ppRight, rightOffset, i, vRange);
  }

}

void putNMatchRowToRes(char* lrow, int32_t tableOffset, int32_t peerOffset) {
  memset(jtCtx.resColBuf, 0, jtCtx.resColSize);

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtx.resColList[tableOffset + c]) {
      if (*(bool*)(lrow + c)) {
        *(bool*)(jtCtx.resColBuf + tableOffset + c) = true;
      } else {
        memcpy(jtCtx.resColBuf + jtCtx.resColOffset[tableOffset + c], lrow + jtCtx.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
  }

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtx.resColList[peerOffset + c]) {
      *(bool*)(jtCtx.resColBuf + peerOffset + c) = true;
    }
  }
  
  pushResRow();
}

void putMatchRowToRes(char* lrow, char* rrow) {
  memset(jtCtx.resColBuf, 0, jtCtx.resColSize);
  
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtx.resColList[c]) {
      if (*(bool*)(lrow + c)) {
        *(bool*)(jtCtx.resColBuf + c) = true;
      } else {
        memcpy(jtCtx.resColBuf + jtCtx.resColOffset[c], lrow + jtCtx.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
  }

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (jtCtx.resColList[MAX_SLOT_NUM + c]) {
      if (*(bool*)(rrow + c)) {
        *(bool*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = true;
      } else {
        memcpy(jtCtx.resColBuf + jtCtx.resColOffset[MAX_SLOT_NUM + c], rrow + jtCtx.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
  }
  
  pushResRow();
}


void leftJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  bool leftMatch = false, rightMatch = false, filterOut = false;
  void* lValue = NULL, *rValue = NULL, *filterValue = NULL;
  int64_t lBig = 0, rBig = 0, fbig = 0;
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;
  
  for (int32_t l = 0; l < leftGrpRows; ++l) {
    char* lrow = jtCtx.colRowDataBuf + jtCtx.blkRowSize * l;
    
    filterOut = false;
    leftMatch = true;
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      lValue = lrow + jtCtx.colRowOffset[c];
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          fbig = TIMESTAMP_FILTER_VALUE;
          lBig = *(int64_t*)lValue;
          break;
        case TSDB_DATA_TYPE_INT:
          fbig = INT_FILTER_VALUE;
          lBig = *(int32_t*)lValue;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          fbig = BIGINT_FILTER_VALUE;
          lBig = *(int64_t*)lValue;
          break;
        default:
          break;
      }
      
      if (jtCtx.leftFilterNum && jtCtx.leftFilterColList[c] && ((*(bool*)(lrow + c)) || lBig <= fbig)) {
        filterOut = true;
        break;
      }

      if (jtCtx.colEqNum && jtCtx.colEqList[c] && (*(bool*)(lrow + c))) {
        leftMatch = false;
      }
    }

    if (filterOut) {
      continue;
    }

    if (false == leftMatch) {
      if (0 == jtCtx.rightFilterNum) {
        putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
      } 
      continue;
    }

    leftMatch = false;
    for (int32_t r = 0; r < rightGrpRows; ++r) {
      char* rrow = jtCtx.colRowDataBuf + rightTbOffset + jtCtx.blkRowSize * r;
      rightMatch = true;
      filterOut = false;

      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        lValue = lrow + jtCtx.colRowOffset[c];

        if (!*(bool*)(rrow + c)) {
          rValue = rrow + jtCtx.colRowOffset[c];
        }

        switch (jtInputColType[c]) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            fbig = TIMESTAMP_FILTER_VALUE;
            lBig = *(int64_t*)lValue;
            rBig = *(int64_t*)rValue;
            break;
          case TSDB_DATA_TYPE_INT:
            fbig = INT_FILTER_VALUE;
            lBig = *(int32_t*)lValue;
            rBig = *(int32_t*)rValue;
            break;
          case TSDB_DATA_TYPE_BIGINT:
            fbig = BIGINT_FILTER_VALUE;
            lBig = *(int64_t*)lValue;
            rBig = *(int64_t*)rValue;
            break;
          default:
            break;
        }
      
        if (jtCtx.colEqNum && jtCtx.colEqList[c] && ((*(bool*)(rrow + c)) || lBig != rBig)) {
          rightMatch = false;
          break;
        }

        if (jtCtx.colOnNum && jtCtx.colOnList[c] && ((*(bool*)(rrow + c)) || lBig <= rBig)) {
          rightMatch = false;
          break;
        }

        if (jtCtx.rightFilterNum && jtCtx.rightFilterColList[c] && ((*(bool*)(rrow + c)) || rBig <= fbig)) {
          filterOut = true;
        }
      }

      if (rightMatch) {
        leftMatch = true;
      }
      
      if (filterOut) {
        continue;
      }

      if (rightMatch) {
        putMatchRowToRes(lrow, rrow);
      }
    }

    if (!leftMatch && 0 == jtCtx.rightFilterNum) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
    }
  }
  

}


void fullJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  bool leftMatch = false, rightMatch = false, lfilterOut = false, rfilterOut = false;
  void* lValue = NULL, *rValue = NULL, *filterValue = NULL;
  int64_t lBig = 0, rBig = 0, fbig = 0;
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;

  memset(jtCtx.rightFinMatch, 0, rightGrpRows * sizeof(bool));
  
  for (int32_t l = 0; l < leftGrpRows; ++l) {
    char* lrow = jtCtx.colRowDataBuf + jtCtx.blkRowSize * l;
    
    lfilterOut = false;
    leftMatch = false;
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      lValue = lrow + jtCtx.colRowOffset[c];
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          fbig = TIMESTAMP_FILTER_VALUE;
          lBig = *(int64_t*)lValue;
          break;
        case TSDB_DATA_TYPE_INT:
          fbig = INT_FILTER_VALUE;
          lBig = *(int32_t*)lValue;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          fbig = BIGINT_FILTER_VALUE;
          lBig = *(int64_t*)lValue;
          break;
        default:
          break;
      }
      
      if (jtCtx.leftFilterNum && jtCtx.leftFilterColList[c] && ((*(bool*)(lrow + c)) || lBig <= fbig)) {
        lfilterOut = true;
      }
    }

    for (int32_t r = 0; r < rightGrpRows; ++r) {
      char* rrow = jtCtx.colRowDataBuf + rightTbOffset + jtCtx.blkRowSize * r;
      rightMatch = true;
      rfilterOut = false;

      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        lValue = lrow + jtCtx.colRowOffset[c];

        if (!*(bool*)(rrow + c)) {
          rValue = rrow + jtCtx.colRowOffset[c];
        }

        switch (jtInputColType[c]) {
          case TSDB_DATA_TYPE_TIMESTAMP:
            fbig = TIMESTAMP_FILTER_VALUE;
            lBig = *(int64_t*)lValue;
            rBig = *(int64_t*)rValue;
            break;
          case TSDB_DATA_TYPE_INT:
            fbig = INT_FILTER_VALUE;
            lBig = *(int32_t*)lValue;
            rBig = *(int32_t*)rValue;
            break;
          case TSDB_DATA_TYPE_BIGINT:
            fbig = BIGINT_FILTER_VALUE;
            lBig = *(int64_t*)lValue;
            rBig = *(int64_t*)rValue;
            break;
          default:
            break;
        }
      
        if (jtCtx.colEqNum && jtCtx.colEqList[c] && ((*(bool*)(lrow + c)) || (*(bool*)(rrow + c)) || lBig != rBig)) {
          rightMatch = false;
        }

        if (jtCtx.colOnNum && jtCtx.colOnList[c] && ((*(bool*)(lrow + c)) || (*(bool*)(rrow + c)) || lBig <= rBig)) {
          rightMatch = false;
        }

        if (jtCtx.rightFilterNum && jtCtx.rightFilterColList[c] && ((*(bool*)(rrow + c)) || rBig <= fbig)) {
          rfilterOut = true;
        }
      }

      if (rightMatch) {
        jtCtx.rightFinMatch[r] = true;
      }
      
      if (rfilterOut) {
        if (!rightMatch) {
          jtCtx.rightFinMatch[r] = true;
        }
        continue;
      }

      if (!lfilterOut && rightMatch) {
        putMatchRowToRes(lrow, rrow);
        leftMatch= true;
      }
    }

    if (!lfilterOut && !leftMatch && 0 == jtCtx.rightFilterNum) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
    }
  }

  if (0 == jtCtx.leftFilterNum) {
    for (int32_t r = 0; r < rightGrpRows; ++r) {
      if (!jtCtx.rightFinMatch[r]) {
        char* rrow = jtCtx.colRowDataBuf + rightTbOffset + jtCtx.blkRowSize * r;
        putNMatchRowToRes(rrow, MAX_SLOT_NUM, 0);
      }
    }
  }
}


void appendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  switch (jtCtx.joinType) {
    case JOIN_TYPE_LEFT:
      leftJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
      break;
    case JOIN_TYPE_FULL:
      fullJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
      break;
    default:
      break;
  }
}

void createTsEqGrpRows(SSDataBlock** ppLeft, SSDataBlock** ppRight, int32_t leftGrpRows, int32_t rightGrpRows) {
  if (leftGrpRows <= 0 && rightGrpRows <= 0) {
    return;
  }

  if (leftGrpRows > 0 && rightGrpRows > 0) {
    jtCtx.inputStat |= (1 << 2);
  }

  ++jtCtx.curTs;

  if (NULL == *ppLeft && leftGrpRows > 0) {
    *ppLeft = createDummyBlock(LEFT_BLK_ID);
    blockDataEnsureCapacity(*ppLeft, jtCtx.blkRows);
    taosArrayPush(jtCtx.leftBlkList, ppLeft);
  }

  if (NULL == *ppRight && rightGrpRows > 0) {
    *ppRight = createDummyBlock(RIGHT_BLK_ID);
    blockDataEnsureCapacity(*ppRight, jtCtx.blkRows);
    taosArrayPush(jtCtx.rightBlkList, ppRight);
  }


  makeAppendBlkData(ppLeft, ppRight, leftGrpRows, rightGrpRows);

  appendEqGrpRes(leftGrpRows, rightGrpRows);
}


void createBothBlkRowsData(void) {
  SSDataBlock* pLeft = NULL;
  SSDataBlock* pRight = NULL;

  jtCtx.leftTotalRows = taosRand() % jtCtx.leftMaxRows;
  jtCtx.rightTotalRows = taosRand() % jtCtx.rightMaxRows;

  int32_t minTotalRows = TMIN(jtCtx.leftTotalRows, jtCtx.rightTotalRows);
  jtCtx.curTs = TIMESTAMP_FILTER_VALUE - minTotalRows / 5; 

  int32_t leftTotalRows = 0, rightTotalRows = 0;
  int32_t leftGrpRows = 0, rightGrpRows = 0;
  int32_t grpType = 0;
  while (leftTotalRows < jtCtx.leftTotalRows || rightTotalRows < jtCtx.rightTotalRows) {
    if (leftTotalRows >= jtCtx.leftTotalRows) {
      grpType = 1;
    } else if (rightTotalRows >= jtCtx.rightTotalRows) {
      grpType = 0;
    } else {
      grpType = taosRand() % 10;
    }

    leftGrpRows = taosRand() % jtCtx.leftMaxGrpRows; 
    rightGrpRows = taosRand() % jtCtx.rightMaxGrpRows; 

    if ((leftTotalRows + leftGrpRows) > jtCtx.leftTotalRows) {
      leftGrpRows = jtCtx.leftTotalRows - leftTotalRows;
    }

    if ((rightTotalRows + rightGrpRows) > jtCtx.rightTotalRows) {
      rightGrpRows = jtCtx.rightTotalRows - rightTotalRows;
    }

    if (0 != grpType && 1 != grpType && (leftGrpRows <= 0 || rightGrpRows <= 0)) {
      if (leftGrpRows <= 0) {
        grpType = 1;
      } else {
        grpType = 0;
      }
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
      default:
        createTsEqGrpRows(&pLeft, &pRight, leftGrpRows, rightGrpRows);
        leftTotalRows += leftGrpRows;
        rightTotalRows += rightGrpRows;
        break;
    }
  }
}

void createDummyBlkList(int32_t leftMaxRows, int32_t leftMaxGrpRows, int32_t rightMaxRows, int32_t rightMaxGrpRows, int32_t blkRows) {
  jtCtx.leftMaxRows = leftMaxRows;
  jtCtx.leftMaxGrpRows = leftMaxGrpRows;
  jtCtx.rightMaxRows = rightMaxRows;
  jtCtx.rightMaxGrpRows = rightMaxGrpRows;
  jtCtx.blkRows = blkRows;

  int32_t maxGrpRows = TMAX(leftMaxGrpRows, rightMaxGrpRows);
  if (maxGrpRows > jtCtx.rightFinMatchNum) {
    jtCtx.rightFinMatchNum = maxGrpRows;
    jtCtx.rightFinMatch = (bool*)taosMemoryRealloc(jtCtx.rightFinMatch, maxGrpRows * sizeof(bool));
  }

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
      if (jtCtx.leftBlkReadIdx >= taosArrayGetSize(jtCtx.leftBlkList)) {
        return NULL;
      }
      return (SSDataBlock*)taosArrayGetP(jtCtx.leftBlkList, jtCtx.leftBlkReadIdx++);
      break;
    case RIGHT_BLK_ID:
      if (jtCtx.rightBlkReadIdx >= taosArrayGetSize(jtCtx.rightBlkList)) {
        return NULL;
      }
      return (SSDataBlock*)taosArrayGetP(jtCtx.rightBlkList, jtCtx.rightBlkReadIdx++);
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
    any.get_func_addr("getNextBlockFromDownstreamRemain", result);
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^getNextBlockFromDownstreamRemain$", result);
#endif
    for (const auto &f : result) {
      stub.set(f.second, getDummyInputBlock);
    }
  }
}

void printColList(char* title, bool left, int32_t* colList, bool filter, char* opStr) {
  bool first = true;
  
  printf("\t %s:", title);
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

void printInputRowData(SSDataBlock* pBlk, int32_t* rowIdx) {
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlk->pDataBlock, c);
    ASSERT(pCol->info.type == jtInputColType[c]);
    if (colDataIsNull_s(pCol, *rowIdx)) {
      printf("%18s", " NULL");
    } else {
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          printf("%18" PRId64, *(int64_t*)colDataGetData(pCol, *rowIdx));
          break;
        case TSDB_DATA_TYPE_INT:
          printf("%18d", *(int32_t*)colDataGetData(pCol, *rowIdx));
          break;
        default:
          ASSERT(0);
      }
    }
  }

  (*rowIdx)++;
}

void printInputData() {
  int32_t leftRowIdx = 0, rightRowIdx = 0;
  
  printf("\nInput Data:\n");
  while (jtCtx.leftBlkReadIdx < taosArrayGetSize(jtCtx.leftBlkList) || jtCtx.rightBlkReadIdx < taosArrayGetSize(jtCtx.rightBlkList)) {
    if (jtCtx.leftBlkReadIdx < taosArrayGetSize(jtCtx.leftBlkList)) {
      while (true) {
        SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.leftBlkList, jtCtx.leftBlkReadIdx);
        if (leftRowIdx < pBlk->info.rows) {
          printInputRowData(pBlk, &leftRowIdx);
          break;
        }
        
        printf("\t--------------------------blk end-------------------------------");
        jtCtx.leftBlkReadIdx++;
        leftRowIdx = 0;
        break;
      }
    } else {
      printf("%72s", " ");
    }

    if (jtCtx.rightBlkReadIdx < taosArrayGetSize(jtCtx.rightBlkList)) {
      while (true) {
        SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.rightBlkList, jtCtx.rightBlkReadIdx);
        if (rightRowIdx < pBlk->info.rows) {
          printInputRowData(pBlk, &rightRowIdx);
          break;
        }
        
        printf("\t--------------------------blk end----------------------------\t");
        jtCtx.rightBlkReadIdx++;
        rightRowIdx = 0;
        break;
      }
    }

    printf("\n");
  }

  jtCtx.leftBlkReadIdx = jtCtx.rightBlkReadIdx = 0;
}

char* getInputStatStr(char* inputStat) {
  if (jtCtx.inputStat & (1 << LEFT_BLK_ID)) {
    strcat(inputStat, "L");
  }
  if (jtCtx.inputStat & (1 << RIGHT_BLK_ID)) {
    strcat(inputStat, "R");
  }
  if (jtCtx.inputStat & (1 << 2)) {
    strcat(inputStat, "E");
  }
  return inputStat;
}

void printBasicInfo(char* caseName) {
  if (!jtCtrl.printTestInfo) {
    return;
  }

  char inputStat[4] = {0};
  printf("\n%dth TEST [%s] START\nBasic Info:\n\t asc:%d\n\t filter:%d\n\t maxRows:left-%d right-%d\n\t "
    "maxGrpRows:left-%d right-%d\n\t blkRows:%d\n\t colCond:%s\n\t joinType:%s\n\t "
    "subType:%s\n\t inputStat:%s\n", jtCtx.loopIdx, caseName, jtCtx.asc, jtCtx.filter, jtCtx.leftMaxRows, jtCtx.rightMaxRows, 
    jtCtx.leftMaxGrpRows, jtCtx.rightMaxGrpRows, jtCtx.blkRows, jtColCondStr[jtCtx.colCond], jtJoinTypeStr[jtCtx.joinType],
    jtSubTypeStr[jtCtx.subType], getInputStatStr(inputStat));
    
  printf("Input Info:\n\t totalBlk:left-%d right-%d\n\t totalRows:left-%d right-%d\n\t "
    "blkRowSize:%d\n\t inputCols:left-%s %s %s %s right-%s %s %s %s\n", 
    (int32_t)taosArrayGetSize(jtCtx.leftBlkList), (int32_t)taosArrayGetSize(jtCtx.rightBlkList), 
    jtCtx.leftTotalRows, jtCtx.rightTotalRows,
    jtCtx.blkRowSize, tDataTypes[jtInputColType[0]].name, tDataTypes[jtInputColType[1]].name,
    tDataTypes[jtInputColType[2]].name, tDataTypes[jtInputColType[3]].name, tDataTypes[jtInputColType[0]].name, 
    tDataTypes[jtInputColType[1]].name, tDataTypes[jtInputColType[2]].name, tDataTypes[jtInputColType[3]].name);

  if (jtCtx.colEqNum) {
    printf("\t colEqNum:%d\n", jtCtx.colEqNum);
    printColList("colEqList", false, jtCtx.colEqList, false, "=");
  }

  if (jtCtx.colOnNum) {
    printf("\t colOnNum:%d\n", jtCtx.colOnNum);
    printColList("colOnList", false, jtCtx.colOnList, false, ">");
  }  

  if (jtCtx.leftFilterNum) {
    printf("\t leftFilterNum:%d\n", jtCtx.leftFilterNum);
    printColList("leftFilterList", true, jtCtx.leftFilterColList, true, ">");
  }

  if (jtCtx.rightFilterNum) {
    printf("\t rightFilterNum:%d\n", jtCtx.rightFilterNum);
    printColList("rightFilterList", false, jtCtx.rightFilterColList, true, ">");
  }

  printf("\t resColSize:%d\n\t resColNum:%d\n\t resColList:", jtCtx.resColSize, jtCtx.resColNum);
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    int32_t s = jtCtx.resColInSlot[i];
    int32_t idx = s >= MAX_SLOT_NUM ? s - MAX_SLOT_NUM : s;
    printf("%sc%d[%s]\t", s >= MAX_SLOT_NUM ? "r" : "l", s, tDataTypes[jtInputColType[idx]].name);
  }

  if (jtCtrl.printInputRow) {
    printInputData();
  }
}

void printOutputInfo() {
  if (!jtCtrl.printTestInfo) {
    return;
  }
  
  printf("\nOutput Info:\n\t expectedRows:%d\n\t ", jtCtx.resRows);
  printf("Actual Result:\n");
}

void printActualResInfo() {
  if (!jtCtrl.printTestInfo) {
    return;
  }

  printf("Actual Result Summary:\n\t blkNum:%d\n\t rowNum:%d%s\n\t leftBlkRead:%d\n\t rightBlkRead:%d\n\t +rows:%d%s\n\t "
    "-rows:%d%s\n\t matchRows:%d%s\n", 
    jtRes.blkNum, jtRes.rowNum, 
    jtRes.rowNum == jtCtx.resRows ? "" : "*",
    jtCtx.leftBlkReadIdx, jtCtx.rightBlkReadIdx,
    jtRes.addRowNum, jtRes.addRowNum ? "*" : "",
    jtRes.subRowNum, jtRes.subRowNum ? "*" : "",
    jtRes.matchNum, jtRes.matchNum == jtCtx.resRows ? "" : "*");
}

void printStatInfo(char* caseName) {
  printf("\n TEST [%s] Stat:\n\t maxResRows:%d\n\t maxResBlkRows:%d\n\t totalResRows:%" PRId64 "\n\t useMSecs:%" PRId64 "\n",
    caseName, jtStat.maxResRows, jtStat.maxResBlkRows, jtStat.totalResRows, jtStat.useMSecs);
  
}

void checkJoinDone(char* caseName) {
  int32_t iter = 0;
  void* p = NULL;
  void* key = NULL;
  while (NULL != (p = tSimpleHashIterate(jtCtx.jtResRows, p, &iter))) {
    key = tSimpleHashGetKey(p, NULL);
    jtRes.succeed = false;
    jtRes.subRowNum += *(int32_t*)p;
    for (int32_t i = 0; i < *(int32_t*)p; ++i) {
      printResRow((char*)key, 0);
    }
  }

  printActualResInfo();
  
  printf("\n%dth TEST [%s] Final Result: %s\n", jtCtx.loopIdx, caseName, jtRes.succeed ? "SUCCEED" : "FAILED");
}

void checkJoinRes(SSDataBlock* pBlock) {
  jtRes.rowNum += pBlock->info.rows;
  if (jtRes.rowNum > jtStat.maxResRows) {
    jtStat.maxResRows = jtRes.rowNum;
  }
  jtRes.blkNum++;
  
  if (pBlock->info.rows > jtStat.maxResBlkRows) {
    jtStat.maxResBlkRows = pBlock->info.rows;
  }

  jtStat.totalResRows += pBlock->info.rows;
  for (int32_t r = 0; r < pBlock->info.rows; ++r) {
    memset(jtCtx.resColBuf, 0, jtCtx.resColSize);
    
    for (int32_t c = 0; c < jtCtx.resColNum; ++c) {
      int32_t slot = jtCtx.resColInSlot[c];
      SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, c);
      switch (jtInputColType[slot % MAX_SLOT_NUM]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          if (colDataIsNull_s(pCol, r)) {
            *(bool*)(jtCtx.resColBuf + slot) = true;
          } else {
            *(int64_t*)(jtCtx.resColBuf + jtCtx.resColOffset[slot]) = *(int64_t*)colDataGetData(pCol, r);
          }
          break;
        case TSDB_DATA_TYPE_INT:
          if (colDataIsNull_s(pCol, r)) {
            *(bool*)(jtCtx.resColBuf + slot) = true;
          } else {
            *(int32_t*)(jtCtx.resColBuf + jtCtx.resColOffset[slot]) = *(int32_t*)colDataGetData(pCol, r);
          }
          break;
        default:
          break;
      }
    }
    
    char* value = (char*)tSimpleHashGet(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
    if (NULL == value) {
      printResRow(jtCtx.resColBuf, 1);
      jtRes.succeed = false;
      jtRes.addRowNum++;
      continue;
    }

    printResRow(jtCtx.resColBuf, 2);
    jtRes.matchNum++;
    rmResRow();
  }
}

void resetForJoinRerun(int32_t dsNum, SSortMergeJoinPhysiNode* pNode, SExecTaskInfo* pTask) {
  jtCtx.leftBlkReadIdx = 0;
  jtCtx.rightBlkReadIdx = 0;
  jtCtx.curKeyOffset = 0;

  memset(&jtRes, 0, sizeof(jtRes));
  jtRes.succeed = true;

  SOperatorInfo* pDownstreams[2];
  createDummyDownstreamOperators(2, pDownstreams);  
  SOperatorInfo* ppDownstreams[] = {pDownstreams[0], pDownstreams[1]};
  jtCtx.pJoinOp = createMergeJoinOperatorInfo(ppDownstreams, 2, pNode, pTask);
  ASSERT_TRUE(NULL != jtCtx.pJoinOp);
}

void handleJoinDone(bool* contLoop) {
  destroyOperator(jtCtx.pJoinOp);
  jtCtx.pJoinOp = NULL;

  if (jtRes.succeed) {
    *contLoop = false;
    return;
  }
  
  if (jtErrorRerun) {
    *contLoop = false;
    return;
  }

  jtInRerun = true;  
}


void jtInitLogFile() {
  const char   *defaultLogFileNamePrefix = "jtlog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  strcpy(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}



void initJoinTest() {
  jtCtx.leftBlkList = taosArrayInit(10, POINTER_BYTES);
  jtCtx.rightBlkList = taosArrayInit(10, POINTER_BYTES);

  jtCtx.jtResRows = tSimpleHashInit(10000000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));

  joinTestReplaceRetrieveFp();

  if (jtCtrl.logHistory) {
    jtStat.pHistory = taosArrayInit(100000, sizeof(SJoinTestHistory));
  }

  jtInitLogFile();
}

void handleTestDone() {
  if (jtCtrl.logHistory) {
    SJoinTestHistory h;
    memcpy(&h.ctx, &jtCtx, sizeof(h.ctx));
    memcpy(&h.res, &jtRes, sizeof(h.res));
    taosArrayPush(jtStat.pHistory, &h);
  }
  
  int32_t blkNum = taosArrayGetSize(jtCtx.leftBlkList);
  for (int32_t i = 0; i < blkNum; ++i) {
    SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.leftBlkList, i);
    blockDataDestroy(pBlk);
  }
  taosArrayClear(jtCtx.leftBlkList);

  blkNum = taosArrayGetSize(jtCtx.rightBlkList);
  for (int32_t i = 0; i < blkNum; ++i) {
    SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.rightBlkList, i);
    blockDataDestroy(pBlk);
  }
  taosArrayClear(jtCtx.rightBlkList);  

  tSimpleHashClear(jtCtx.jtResRows);
  jtCtx.resRows = 0;

  jtCtx.inputStat = 0;
}

void runSingleTest(char* caseName, SJoinTestParam* param) {
  bool contLoop = true;
  
  SSortMergeJoinPhysiNode* pNode = createDummySortMergeJoinPhysiNode(param->joinType, param->subType, param->cond, param->filter, param->asc);    
  createDummyBlkList(10, 10, 10, 10, 3);
  
  while (contLoop) {
    rerunBlockedHere();
    resetForJoinRerun(2, pNode, param->pTask);
    printBasicInfo(caseName);
    printOutputInfo();
  
    while (true) {
      SSDataBlock* pBlock = jtCtx.pJoinOp->fpSet.getNextFn(jtCtx.pJoinOp);
      if (NULL == pBlock) {
        checkJoinDone(caseName);
        break;
      } else {
        checkJoinRes(pBlock);
      }
    }
  
    handleJoinDone(&contLoop);
  }
  
  nodesDestroyNode((SNode*)pNode);
  handleTestDone();
}

void handleCaseEnd() {
  taosMemoryFreeClear(jtCtx.rightFinMatch);
  jtCtx.rightFinMatchNum = 0;
}

}  // namespace

#if 0
#if 1
TEST(leftOuterJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "leftOuterJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_NO_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(leftOuterJoin, eqCondTest) {
  SJoinTestParam param;
  char* caseName = "leftOuterJoin:eqCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }
  
  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(leftOuterJoin, onCondTest) {
  SJoinTestParam param;
  char* caseName = "leftOuterJoin:onCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_ON_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }
  
  printStatInfo(caseName);   
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(leftOuterJoin, fullCondTest) {
  SJoinTestParam param;
  char* caseName = "leftOuterJoin:fullCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }
  
  printStatInfo(caseName);   
  taosMemoryFree(pTask);
}
#endif
#endif

#if 1
TEST(fullOuterJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "fullOuterJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_NO_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(fullOuterJoin, eqCondTest) {
  SJoinTestParam param;
  char* caseName = "fullOuterJoin:eqCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }
  
  printStatInfo(caseName); 
  taosMemoryFree(pTask);
  handleCaseEnd();
}
#endif

#if 1
TEST(fullOuterJoin, onCondTest) {
  SJoinTestParam param;
  char* caseName = "fullOuterJoin:onCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_ON_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }
  
  printStatInfo(caseName);   
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(fullOuterJoin, fullCondTest) {
  SJoinTestParam param;
  char* caseName = "fullOuterJoin:fullCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.filter = false;
    runSingleTest(caseName, &param);

    param.filter = true;
    runSingleTest(caseName, &param);
  }
  
  printStatInfo(caseName);   
  taosMemoryFree(pTask);
}
#endif


int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  initJoinTest();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



#pragma GCC diagnosti
