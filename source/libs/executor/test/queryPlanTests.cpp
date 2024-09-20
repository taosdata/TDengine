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


#ifdef WINDOWS
#define TD_USE_WINSOCK
#endif

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

#define JT_PRINTF (void)printf

#define COL_DISPLAY_WIDTH 18
#define JT_MAX_LOOP       1000

#define LEFT_BLK_ID       0
#define RIGHT_BLK_ID      1
#define RES_BLK_ID        2
#define MAX_SLOT_NUM      4

#define LEFT_TABLE_COLS   0x1
#define RIGHT_TABLE_COLS  0x2
#define ALL_TABLE_COLS    (LEFT_TABLE_COLS | RIGHT_TABLE_COLS)

#define JT_MAX_JLIMIT     20
#define JT_MAX_WINDOW_OFFSET 5
#define JT_KEY_SOLT_ID    (MAX_SLOT_NUM - 1)
#define JT_PRIM_TS_SLOT_ID 0
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
  bool noKeepResRows;
} SJoinTestCtrl;


typedef struct {
  bool    filter;
  bool    asc;
  bool    grpJoin;
  int32_t leftMaxRows;
  int32_t leftMaxGrpRows;
  int32_t rightMaxRows;
  int32_t rightMaxGrpRows;
  int32_t blkRows;
  int32_t colCond;
  int32_t joinType;
  int32_t subType;
  int32_t asofOpType;
  int64_t jLimit;
  int64_t winStartOffset;
  int64_t winEndOffset;
  int64_t inGrpId;

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
  bool       leftColOnly;
  bool       rightColOnly;
  SSHashObj* jtResRows;

  SOperatorInfo* pJoinOp;

  int32_t  loopIdx;

  int32_t  rightFinMatchNum;
  bool*    rightFinMatch;

  int32_t  inColOffset[MAX_SLOT_NUM];
  int32_t  inColSize;
  char*    inColBuf;
  SArray*  leftRowsList;
  SArray*  rightRowsList;
  SArray*  rightFilterOut;

  int64_t  startTsUs;
} SJoinTestCtx;

typedef struct {
  SJoinTestResInfo res;
  SJoinTestCtx     ctx;
} SJoinTestHistory;

typedef struct {
  EJoinType joinType;
  EJoinSubType subType;
  int32_t asofOp;
  int64_t jLimit;
  int32_t cond;
  bool    filter;
  bool    asc;
  bool    grpJoin;
  bool    timetruncate;
  SExecTaskInfo* pTask;
} SJoinTestParam;


SJoinTestCtx jtCtx = {0};
SJoinTestCtrl jtCtrl = {0, 0, 0, 0, 0};
SJoinTestStat jtStat = {0};
SJoinTestResInfo jtRes = {0};



void printResRow(char* value, int32_t type) {
  if (!jtCtrl.printResRow) {
    return;
  }
  
  JT_PRINTF(" ");
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    int32_t slot = jtCtx.resColInSlot[i];
    if (0 == type && ((jtCtx.leftColOnly && slot >= MAX_SLOT_NUM) ||
        (jtCtx.rightColOnly && slot < MAX_SLOT_NUM))) {
      ("%18s", " ");
      continue;
    }
    
    if (*(bool*)(value + slot)) {
      JT_PRINTF("%18s", " NULL");
      continue;
    }
    
    switch (jtInputColType[slot % MAX_SLOT_NUM]) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        JT_PRINTF("%18" PRId64 , *(int64_t*)(value + jtCtx.resColOffset[slot]));
        break;
      case TSDB_DATA_TYPE_INT:
        JT_PRINTF("%18d", *(int32_t*)(value + jtCtx.resColOffset[slot]));
        break;
      case TSDB_DATA_TYPE_BIGINT:
        JT_PRINTF("%18" PRId64, *(int64_t*)(value + jtCtx.resColOffset[slot]));
        break;
    }
  }
  JT_PRINTF("\t %s\n", 0 == type ? "-" : (1 == type ? "+" : ""));
}

void pushResRow(char* buf, int32_t size) {
  jtCtx.resRows++;

  if (!jtCtrl.noKeepResRows) {
    int32_t* rows = (int32_t*)tSimpleHashGet(jtCtx.jtResRows, buf, size);
    if (rows) {
      (*rows)++;
    } else {
      int32_t n = 1;
      assert(0 == tSimpleHashPut(jtCtx.jtResRows, buf, size, &n, sizeof(n)));
    }
  }
}

void rmResRow() {
  int32_t* rows = (int32_t*)tSimpleHashGet(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
  if (rows) {
    (*rows)--;
    if ((*rows) == 0) {
      (void)tSimpleHashRemove(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
    }
  } else {
    assert(0);
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
      assert(0 == nodesListStrictAppendList(pLogic->pParameterList, ((SLogicConditionNode*)(*ppSrc))->pParameterList));
      ((SLogicConditionNode*)(*ppSrc))->pParameterList = NULL;
    } else {
      assert(0 == nodesListStrictAppend(pLogic->pParameterList, *ppSrc));
      *ppSrc = NULL;
    }
    nodesDestroyNode(*ppSrc);
    *ppSrc = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SLogicConditionNode* pLogicCond = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogicCond);
  if (NULL == pLogicCond) {
    return code;
  }
  pLogicCond->node.resType.type = TSDB_DATA_TYPE_BOOL;
  pLogicCond->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
  pLogicCond->condType = LOGIC_COND_TYPE_AND;
  pLogicCond->pParameterList = NULL;
  code = nodesMakeList(&pLogicCond->pParameterList);
  assert(0 == nodesListStrictAppend(pLogicCond->pParameterList, *ppSrc));
  assert(0 == nodesListStrictAppend(pLogicCond->pParameterList, *ppDst));

  *ppDst = (SNode*)pLogicCond;
  *ppSrc = NULL;

  return TSDB_CODE_SUCCESS;
}


void createDummyDownstreamOperators(int32_t num, SOperatorInfo** ppRes) {
  for (int32_t i = 0; i < num; ++i) {
    SOperatorInfo* p = (SOperatorInfo*)taosMemoryCalloc(1, sizeof(SOperatorInfo));
    assert(NULL != p); 
    p->resultDataBlockId = i;
    ppRes[i] = p;
  }
}

void createTargetSlotList(SSortMergeJoinPhysiNode* p) {
  jtCtx.resColNum = 0;
  TAOS_MEMSET(jtCtx.resColList, 0, sizeof(jtCtx.resColList));
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

      STargetNode* pTarget = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget);
      SColumnNode* pCol = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      assert(NULL != pTarget && NULL != pCol);
      pCol->dataBlockId = LEFT_BLK_ID;
      pCol->slotId = i;
      pTarget->dataBlockId = RES_BLK_ID;
      pTarget->slotId = dstIdx++;
      pTarget->pExpr = (SNode*)pCol;
      dstOffset += tDataTypes[jtInputColType[i]].bytes;
      jtCtx.resColSize += tDataTypes[jtInputColType[i]].bytes;
      
      assert(0 == nodesListMakeStrictAppend(&p->pTargets, (SNode*)pTarget));

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

      STargetNode* pTarget = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget);
      SColumnNode* pCol = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      assert(NULL != pTarget && NULL != pCol);
      pCol->dataBlockId = RIGHT_BLK_ID;
      pCol->slotId = i;
      pTarget->dataBlockId = RES_BLK_ID;
      pTarget->slotId = dstIdx++;
      pTarget->pExpr = (SNode*)pCol;
      dstOffset += tDataTypes[jtInputColType[i]].bytes;
      jtCtx.resColSize += tDataTypes[jtInputColType[i]].bytes;
      
      assert(0 == nodesListMakeStrictAppend(&p->pTargets, (SNode*)pTarget));
      jtCtx.resColNum++;
    }
  }  

  jtCtx.resColBuf = (char*)taosMemoryRealloc(jtCtx.resColBuf, jtCtx.resColSize);
  assert(NULL != jtCtx.resColBuf);
}

void createColEqCondStart(SSortMergeJoinPhysiNode* p) {
  jtCtx.colEqNum = 0;
  do {
    jtCtx.colEqNum = taosRand() % MAX_SLOT_NUM; // except TIMESTAMP
  } while (0 == jtCtx.colEqNum);

  int32_t idx = 0;
  TAOS_MEMSET(jtCtx.colEqList, 0, sizeof(jtCtx.colEqList));
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
      SColumnNode* pCol1 = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol1);
      assert(pCol1);
      pCol1->dataBlockId = LEFT_BLK_ID;
      pCol1->slotId = i;
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      assert(0 == nodesListMakeStrictAppend(&p->pEqLeft, (SNode*)pCol1));

      SColumnNode* pCol2 = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol2);
      pCol2->dataBlockId = RIGHT_BLK_ID;
      pCol2->slotId = i;
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      assert(0 == nodesListMakeStrictAppend(&p->pEqRight, (SNode*)pCol2));
    }
  }
}

void createColOnCondStart(SSortMergeJoinPhysiNode* p) {
  jtCtx.colOnNum = 0;
  do {
    jtCtx.colOnNum = taosRand() % (MAX_SLOT_NUM + 1);
  } while (0 == jtCtx.colOnNum || (jtCtx.colOnNum + jtCtx.colEqNum) > MAX_SLOT_NUM);

  int32_t idx = 0;
  TAOS_MEMSET(jtCtx.colOnList, 0, sizeof(jtCtx.colOnList));
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
    int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogic);
    assert(pLogic);
    pLogic->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogic->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colEqList[i]) {
      SColumnNode* pCol1 = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol1);
      assert(pCol1);
      pCol1->dataBlockId = RES_BLK_ID;
      pCol1->slotId = getDstSlotId(i);
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      SColumnNode* pCol2 = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol2);
      assert(pCol2);
      pCol2->dataBlockId = RES_BLK_ID;
      pCol2->slotId = getDstSlotId(MAX_SLOT_NUM + i);
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;

      SOperatorNode* pOp = NULL;
      code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
      assert(pOp);
      pOp->opType = OP_TYPE_EQUAL;
      pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      pOp->pLeft = (SNode*)pCol1;
      pOp->pRight = (SNode*)pCol2;

      if (jtCtx.colEqNum > 1) {
        assert(0 == nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp));
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
    int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogic);
    assert(pLogic);
    pLogic->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogic->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.colOnList[i]) {
      SColumnNode* pCol1 = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol1);
      assert(pCol1);
      pCol1->dataBlockId = RES_BLK_ID;
      pCol1->slotId = getDstSlotId(i);
      pCol1->node.resType.type = jtInputColType[i];
      pCol1->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      SColumnNode* pCol2 = NULL;
      code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol2);
      assert(pCol2);
      pCol2->dataBlockId = RES_BLK_ID;
      pCol2->slotId = getDstSlotId(MAX_SLOT_NUM + i);
      pCol2->node.resType.type = jtInputColType[i];
      pCol2->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      
      SOperatorNode* pOp = NULL;
      code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
      assert(pOp);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      pOp->pLeft = (SNode*)pCol1;
      pOp->pRight = (SNode*)pCol2;

      if (jtCtx.colOnNum > 1) {
        assert(0 == nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp));
      } else {
        p->pColOnCond = (SNode*)pOp;
        break;
      }
    }
  }

  if (jtCtx.colOnNum > 1) {
    p->pColOnCond = (SNode*)pLogic;
  }  

  SNode* pTmp = NULL;
  int32_t code = nodesCloneNode(p->pColOnCond, &pTmp);
  assert(pTmp);
  assert(0 == jtMergeEqCond(&p->pFullOnCond, &pTmp));
}



void createColCond(SSortMergeJoinPhysiNode* p, int32_t cond) {
  jtCtx.colCond = cond;
  switch (cond) {
    case TEST_NO_COND:
      jtCtx.colEqNum = 0;
      jtCtx.colOnNum = 0;
      TAOS_MEMSET(jtCtx.colEqList, 0, sizeof(jtCtx.colEqList));
      TAOS_MEMSET(jtCtx.colOnList, 0, sizeof(jtCtx.colOnList));
      break;
    case TEST_EQ_COND:
      createColEqCondStart(p);
      jtCtx.colOnNum = 0;
      TAOS_MEMSET(jtCtx.colOnList, 0, sizeof(jtCtx.colOnList));
      break;
    case TEST_ON_COND:
      createColOnCondStart(p);
      jtCtx.colEqNum = 0;
      TAOS_MEMSET(jtCtx.colEqList, 0, sizeof(jtCtx.colEqList));
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
    TAOS_MEMSET(jtCtx.leftFilterColList, 0, sizeof(jtCtx.leftFilterColList));
    TAOS_MEMSET(jtCtx.rightFilterColList, 0, sizeof(jtCtx.rightFilterColList));
    return;
  }

  if ((JOIN_STYPE_SEMI == jtCtx.subType || JOIN_STYPE_ANTI == jtCtx.subType) && JOIN_TYPE_LEFT == jtCtx.joinType) {
    jtCtx.rightFilterNum = 0;
    jtCtx.leftFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
    if (0 == jtCtx.leftFilterNum) {
      do {
        jtCtx.leftFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
      } while (0 == jtCtx.leftFilterNum);
    }
  } else if ((JOIN_STYPE_SEMI == jtCtx.subType || JOIN_STYPE_ANTI == jtCtx.subType) && JOIN_TYPE_RIGHT == jtCtx.joinType) {
    jtCtx.leftFilterNum = 0;
    jtCtx.rightFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
    if (0 == jtCtx.rightFilterNum) {
      do {
        jtCtx.rightFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
      } while (0 == jtCtx.rightFilterNum);
    }
  } else {
    jtCtx.leftFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
    if (0 == jtCtx.leftFilterNum) {
      do {
        jtCtx.rightFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
      } while (0 == jtCtx.rightFilterNum);
    } else {
      jtCtx.rightFilterNum = taosRand() % (MAX_SLOT_NUM + 1);
    }
  }

  int32_t idx = 0;
  TAOS_MEMSET(jtCtx.leftFilterColList, 0, sizeof(jtCtx.leftFilterColList));
  TAOS_MEMSET(jtCtx.rightFilterColList, 0, sizeof(jtCtx.rightFilterColList));
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
    int32_t code = nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogic);
    assert(pLogic);
    pLogic->node.resType.type = TSDB_DATA_TYPE_BOOL;
    pLogic->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
    pLogic->condType = LOGIC_COND_TYPE_AND;
  }
  
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.leftFilterColList[i]) {
      SColumnNode* pCol = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_COLUMN,(SNode**)&pCol);
      assert(pCol);
      pCol->dataBlockId = RES_BLK_ID;
      pCol->slotId = getDstSlotId(i);
      pCol->node.resType.type = jtInputColType[i];
      pCol->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      (void)sprintf(pCol->colName, "l%d", i);

      SValueNode* pVal = NULL;
      code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
      assert(pVal);
      pVal->node.resType.type = jtInputColType[i];
      pVal->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      assert(0 == nodesSetValueNodeValue(pVal, getFilterValue(jtInputColType[i])));

      SOperatorNode* pOp = NULL;
      code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
      assert(pOp);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      pOp->pLeft = (SNode*)pCol;
      pOp->pRight = (SNode*)pVal;

      if ((jtCtx.leftFilterNum + jtCtx.rightFilterNum) > 1) {
        assert(0 == nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp));
      } else {
        p->node.pConditions = (SNode*)pOp;
        break;
      }
    }
  }

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (jtCtx.rightFilterColList[i]) {
      SColumnNode* pCol = NULL;
      int32_t code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
      assert(pCol);
      pCol->dataBlockId = RES_BLK_ID;
      pCol->slotId = getDstSlotId(MAX_SLOT_NUM + i);
      pCol->node.resType.type = jtInputColType[i];
      pCol->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      (void)sprintf(pCol->colName, "r%d", i);

      SValueNode* pVal = NULL;
      code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);
      assert(pVal);
      pVal->node.resType.type = jtInputColType[i];
      pVal->node.resType.bytes = tDataTypes[jtInputColType[i]].bytes;
      assert(0 == nodesSetValueNodeValue(pVal, getFilterValue(jtInputColType[i])));

      SOperatorNode* pOp = NULL;
      code = nodesMakeNode(QUERY_NODE_OPERATOR, (SNode**)&pOp);
      assert(pOp);
      pOp->opType = OP_TYPE_GREATER_THAN;
      pOp->node.resType.type = TSDB_DATA_TYPE_BOOL;
      pOp->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BOOL].bytes;
      pOp->pLeft = (SNode*)pCol;
      pOp->pRight = (SNode*)pVal;

      if ((jtCtx.leftFilterNum + jtCtx.rightFilterNum) > 1) {
        assert(0 == nodesListMakeStrictAppend(&pLogic->pParameterList, (SNode*)pOp));
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
  SDataBlockDescNode* pDesc = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, (SNode**)&pDesc);
  assert(pDesc);
  pDesc->dataBlockId = RES_BLK_ID;
  pDesc->totalRowSize = jtCtx.resColSize - MAX_SLOT_NUM * 2 * sizeof(bool);
  pDesc->outputRowSize = pDesc->totalRowSize;
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    SSlotDescNode* pSlot = NULL;
    int32_t code = nodesMakeNode(QUERY_NODE_SLOT_DESC, (SNode**)&pSlot);
    assert(pSlot);
    pSlot->slotId = i;
    int32_t slotIdx = jtCtx.resColInSlot[i] >= MAX_SLOT_NUM ? jtCtx.resColInSlot[i] - MAX_SLOT_NUM : jtCtx.resColInSlot[i];
    pSlot->dataType.type = jtInputColType[slotIdx];
    pSlot->dataType.bytes = tDataTypes[pSlot->dataType.type].bytes;

    assert(0 == nodesListMakeStrictAppend(&pDesc->pSlots, (SNode *)pSlot));
  }

  *ppNode = pDesc;
}

SSortMergeJoinPhysiNode* createDummySortMergeJoinPhysiNode(SJoinTestParam* param) {
  SSortMergeJoinPhysiNode* p = NULL;
  int32_t code = nodesMakeNode(QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, (SNode**)&p);
  assert(p);
  p->joinType = param->joinType;
  p->subType = param->subType;
  p->asofOpType = param->asofOp;
  p->grpJoin = param->grpJoin;
  if (p->subType == JOIN_STYPE_WIN || param->jLimit > 1 || taosRand() % 2) {
    SLimitNode* limitNode = NULL;
    code = nodesMakeNode(QUERY_NODE_LIMIT, (SNode**)&limitNode);
    assert(limitNode);
    limitNode->limit = param->jLimit;
    p->pJLimit = (SNode*)limitNode;
  }
  
  p->leftPrimSlotId = JT_PRIM_TS_SLOT_ID;
  p->rightPrimSlotId = JT_PRIM_TS_SLOT_ID;
  p->node.inputTsOrder = param->asc ? ORDER_ASC : ORDER_DESC;
  if (JOIN_STYPE_WIN == p->subType) {
    SWindowOffsetNode* pOffset = NULL;
    code = nodesMakeNode(QUERY_NODE_WINDOW_OFFSET, (SNode**)&pOffset);
    assert(pOffset);
    SValueNode* pStart = NULL;
    code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pStart);
    assert(pStart);
    SValueNode* pEnd = NULL;
    code = nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pEnd);
    assert(pEnd);
    pStart->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    pStart->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
    pStart->datum.i = (taosRand() % 2) ? (((int32_t)-1) * (int64_t)(taosRand() % JT_MAX_WINDOW_OFFSET)) : (taosRand() % JT_MAX_WINDOW_OFFSET);
    pEnd->node.resType.type = TSDB_DATA_TYPE_BIGINT;
    pEnd->node.resType.bytes = tDataTypes[TSDB_DATA_TYPE_BIGINT].bytes;
    pEnd->datum.i = (taosRand() % 2) ? (((int32_t)-1) * (int64_t)(taosRand() % JT_MAX_WINDOW_OFFSET)) : (taosRand() % JT_MAX_WINDOW_OFFSET);
    if (pStart->datum.i > pEnd->datum.i) {
      TSWAP(pStart->datum.i, pEnd->datum.i);
    }
    pOffset->pStartOffset = (SNode*)pStart;
    pOffset->pEndOffset = (SNode*)pEnd;
    p->pWindowOffset = (SNode*)pOffset;

    jtCtx.winStartOffset = pStart->datum.i;
    jtCtx.winEndOffset = pEnd->datum.i;
  }

  jtCtx.grpJoin = param->grpJoin;
  jtCtx.joinType = param->joinType;
  jtCtx.subType = param->subType;
  jtCtx.asc = param->asc;
  jtCtx.jLimit = param->jLimit;
  jtCtx.asofOpType = param->asofOp;
  jtCtx.leftColOnly = (JOIN_TYPE_LEFT == param->joinType && JOIN_STYPE_SEMI == param->subType);
  jtCtx.rightColOnly = (JOIN_TYPE_RIGHT == param->joinType && JOIN_STYPE_SEMI == param->subType);
  jtCtx.inGrpId = 1;

  createColCond(p, param->cond);
  createFilterStart(p, param->filter);
  createTargetSlotList(p);
  createColEqCondEnd(p);
  createColOnCondEnd(p);
  createFilterEnd(p, param->filter);
  updateColRowInfo();
  createBlockDescNode(&p->node.pOutputDataBlockDesc);

  return p;
}

SExecTaskInfo* createDummyTaskInfo(char* taskId) {
  SExecTaskInfo* p = (SExecTaskInfo*)taosMemoryCalloc(1, sizeof(SExecTaskInfo));
  assert(p);
  p->id.str = taskId;

  return p;
}

SSDataBlock* createDummyBlock(int32_t blkId) {
  SSDataBlock* p = NULL;
  int32_t code = createDataBlock(&p);
  assert(code == 0);

  p->info.id.blockId = blkId;
  p->info.type = STREAM_INVALID;
  p->info.calWin.skey = INT64_MIN;
  p->info.calWin.ekey = INT64_MAX;
  p->info.watermark = INT64_MIN;

  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    SColumnInfoData idata =
        createColumnInfoData(jtInputColType[i], tDataTypes[jtInputColType[i]].bytes, i);

    assert(0 == blockDataAppendColInfo(p, &idata));
  }

  return p;
}

void appendAsofLeftEachResGrps(char* leftInRow, int32_t rightOffset, int32_t rightRows) {
  TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (!jtCtx.resColList[c]) {
      continue;
    }
  
    if (*((bool*)leftInRow + c)) {
      *(char*)(jtCtx.resColBuf + c) = true;
    } else {
      TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[c], leftInRow + jtCtx.inColOffset[c], tDataTypes[jtInputColType[c]].bytes);
    }
  }

  int32_t endIdx = TMIN(rightRows, taosArrayGetSize(jtCtx.rightRowsList) - rightOffset) + rightOffset;
  for (int32_t r = rightOffset; r < endIdx; ++r) {
    bool* rightFilterOut = (bool*)taosArrayGet(jtCtx.rightFilterOut, r);
    if (*rightFilterOut) {
      continue;
    }
    
    char* rightResRows = (char*)taosArrayGet(jtCtx.rightRowsList, r);
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (jtCtx.resColList[MAX_SLOT_NUM + c]) {
        if (*(bool*)(rightResRows + c)) {
          *(bool*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = true;
          TAOS_MEMSET(jtCtx.resColBuf + jtCtx.resColOffset[MAX_SLOT_NUM + c], 0, tDataTypes[jtInputColType[c]].bytes);
        } else {
          *(bool*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = false;
          TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[MAX_SLOT_NUM + c], rightResRows + jtCtx.inColOffset[c], tDataTypes[jtInputColType[c]].bytes);
        }
      }
    }
  
    pushResRow(jtCtx.resColBuf, jtCtx.resColSize);
  }
}

void appendLeftNonMatchGrp(char* leftInRow) {
  if (!jtCtrl.noKeepResRows) {
    TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (!jtCtx.resColList[c]) {
        continue;
      }
    
      if (*((bool*)leftInRow + c)) {
        *(char*)(jtCtx.resColBuf + c) = true;
      } else {
        TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[c], leftInRow + jtCtx.inColOffset[c], tDataTypes[jtInputColType[c]].bytes);
      }
    }
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (jtCtx.resColList[MAX_SLOT_NUM + c]) {
        *(char*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = true;
      }
    }
  }
  
  pushResRow(jtCtx.resColBuf, jtCtx.resColSize);
}

void appendAllAsofResRows() {
  int32_t leftRows = taosArrayGetSize(jtCtx.leftRowsList);
  int32_t rightRows = taosArrayGetSize(jtCtx.rightRowsList);
  if (rightRows <= 0) {
    if (0 == jtCtx.rightFilterNum) {
      for (int32_t i = 0; i < leftRows; ++i) {
        char* leftInRow = (char*)taosArrayGet(jtCtx.leftRowsList, i);
        assert(leftInRow);
        appendLeftNonMatchGrp(leftInRow);
      }      
    }    
  } else {
    assert(rightRows <= jtCtx.jLimit);
    for (int32_t i = 0; i < leftRows; ++i) {
      char* leftInRow = (char*)taosArrayGet(jtCtx.leftRowsList, i);
      assert(leftInRow);
      appendAsofLeftEachResGrps(leftInRow, 0, rightRows);
    }
  }
  taosArrayClear(jtCtx.leftRowsList);
}

void chkAppendAsofForwardGrpResRows(bool forceOut) {
  int32_t rightRows = taosArrayGetSize(jtCtx.rightRowsList);
  if (rightRows < jtCtx.jLimit && !forceOut) {
    return;
  }

  int32_t rightRemains = rightRows;
  int32_t rightOffset = 0;
  int32_t leftRows = taosArrayGetSize(jtCtx.leftRowsList);
  int32_t i = 0;
  for (; i < leftRows; ++i) {
    char* leftRow = (char*)taosArrayGet(jtCtx.leftRowsList, i);
    assert(leftRow);
    int64_t* leftTs = (int64_t*)(leftRow + jtCtx.inColOffset[JT_PRIM_TS_SLOT_ID]);
    bool append = false;
    for (int32_t r = rightOffset; r < rightRows; ++r) {
      char* rightRow = (char*)taosArrayGet(jtCtx.rightRowsList, r);
      assert(rightRow);
      int64_t* rightTs = (int64_t*)(rightRow + jtCtx.inColOffset[JT_PRIM_TS_SLOT_ID]);
      if (((jtCtx.asc && *leftTs > *rightTs) || (!jtCtx.asc && *leftTs < *rightTs)) || (*leftTs == *rightTs && (OP_TYPE_LOWER_THAN == jtCtx.asofOpType || OP_TYPE_GREATER_THAN == jtCtx.asofOpType))) {
        rightOffset++;
        rightRemains--;
        if (rightRemains < jtCtx.jLimit && !forceOut) {
          taosArrayPopFrontBatch(jtCtx.rightRowsList, rightOffset);
          taosArrayPopFrontBatch(jtCtx.rightFilterOut, rightOffset);
          taosArrayPopFrontBatch(jtCtx.leftRowsList, i);
          return;
        }
        
        continue;
      }

      appendAsofLeftEachResGrps(leftRow, rightOffset, jtCtx.jLimit);
      append = true;
      break;
    }

    if (!append) {
      if (!forceOut) {
        break;
      }

      if (0 == jtCtx.rightFilterNum) {
        appendLeftNonMatchGrp(leftRow);
      }
    }
  }

  taosArrayPopFrontBatch(jtCtx.rightRowsList, rightOffset);
  taosArrayPopFrontBatch(jtCtx.rightFilterOut, rightOffset);
  taosArrayPopFrontBatch(jtCtx.leftRowsList, i);
}


void appendWinEachResGrps(char* leftInRow, int32_t rightOffset, int32_t rightRows) {
  if (rightOffset < 0) {
    if (0 == jtCtx.rightFilterNum) {
      appendLeftNonMatchGrp(leftInRow);
    }
    return;
  }
  
  TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    if (!jtCtx.resColList[c]) {
      continue;
    }
  
    if (*((bool*)leftInRow + c)) {
      *(char*)(jtCtx.resColBuf + c) = true;
    } else {
      TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[c], leftInRow + jtCtx.inColOffset[c], tDataTypes[jtInputColType[c]].bytes);
    }
  }

  int32_t endIdx = rightRows + rightOffset;
  int32_t beginIdx = (!jtCtx.asc && rightRows > jtCtx.jLimit) ? (endIdx - jtCtx.jLimit) : rightOffset;
  for (int32_t r = beginIdx; r < endIdx; ++r) {
    bool* rightFilterOut = (bool*)taosArrayGet(jtCtx.rightFilterOut, r);
    if (*rightFilterOut) {
      continue;
    }
    
    char* rightResRows = (char*)taosArrayGet(jtCtx.rightRowsList, r);
    assert(rightResRows);
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (jtCtx.resColList[MAX_SLOT_NUM + c]) {
        if (*(bool*)(rightResRows + c)) {
          *(bool*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = true;
          TAOS_MEMSET(jtCtx.resColBuf + jtCtx.resColOffset[MAX_SLOT_NUM + c], 0, tDataTypes[jtInputColType[c]].bytes);
        } else {
          *(bool*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = false;
          TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[MAX_SLOT_NUM + c], rightResRows + jtCtx.inColOffset[c], tDataTypes[jtInputColType[c]].bytes);
        }
      }
    }
  
    pushResRow(jtCtx.resColBuf, jtCtx.resColSize);
  }
}

void chkAppendWinResRows(bool forceOut) {
  int32_t rightRows = taosArrayGetSize(jtCtx.rightRowsList);
  if (rightRows < jtCtx.jLimit && !forceOut) {
    return;
  }

  int32_t rightRemains = rightRows;
  int32_t rightOffset = 0;
  int32_t leftRows = taosArrayGetSize(jtCtx.leftRowsList);
  int32_t i = 0;
  for (; i < leftRows; ++i) {
    char* leftRow = (char*)taosArrayGet(jtCtx.leftRowsList, i);
    assert(leftRow);
    int64_t* leftTs = (int64_t*)(leftRow + jtCtx.inColOffset[JT_PRIM_TS_SLOT_ID]);
    int64_t winStart = *leftTs + jtCtx.winStartOffset;
    int64_t winEnd = *leftTs + jtCtx.winEndOffset;
    int32_t winBeginIdx = -1;
    bool append = false;
    bool winClosed = false;
    for (int32_t r = rightOffset; r < rightRows; ++r) {
      char* rightRow = (char*)taosArrayGet(jtCtx.rightRowsList, r);
      assert(rightRow);
      int64_t* rightTs = (int64_t*)(rightRow + jtCtx.inColOffset[JT_PRIM_TS_SLOT_ID]);
      if ((jtCtx.asc && *rightTs < winStart) || (!jtCtx.asc && *rightTs > winEnd)) {
        rightOffset++;
        rightRemains--;
        if (rightRemains < jtCtx.jLimit && !forceOut) {
          taosArrayPopFrontBatch(jtCtx.rightRowsList, rightOffset);
          taosArrayPopFrontBatch(jtCtx.rightFilterOut, rightOffset);
          taosArrayPopFrontBatch(jtCtx.leftRowsList, i);
          return;
        }
        
        continue;
      } else if ((jtCtx.asc && *rightTs > winEnd) || (!jtCtx.asc && *rightTs < winStart)) {
        winClosed = true;
        appendWinEachResGrps(leftRow, winBeginIdx, r - winBeginIdx);
        append = true;
        break;
      }

      if (-1 == winBeginIdx) {
        winBeginIdx = r;
      }

      if (jtCtx.asc && (r - winBeginIdx + 1) >= jtCtx.jLimit) {
        appendWinEachResGrps(leftRow, winBeginIdx, jtCtx.jLimit);
        append = true;
        break;
      }
    }

    if (!append) {
      if (!forceOut) {
        break;
      }

      if (winBeginIdx >= 0) {
        appendWinEachResGrps(leftRow, winBeginIdx, rightRows - winBeginIdx);
      } else if (0 == jtCtx.rightFilterNum) {
        appendLeftNonMatchGrp(leftRow);
      }
    }
  }

  taosArrayPopFrontBatch(jtCtx.rightRowsList, rightOffset);
  taosArrayPopFrontBatch(jtCtx.rightFilterOut, rightOffset);
  taosArrayPopFrontBatch(jtCtx.leftRowsList, i);
}


void trimForAsofJlimit() {
  int32_t rowNum = taosArrayGetSize(jtCtx.rightRowsList);
  if (rowNum <= jtCtx.jLimit) {
    return;
  }

  taosArrayPopFrontBatch(jtCtx.rightRowsList, rowNum - jtCtx.jLimit);
  taosArrayPopFrontBatch(jtCtx.rightFilterOut, rowNum - jtCtx.jLimit);
}

void createGrpRows(SSDataBlock** ppBlk, int32_t blkId, int32_t grpRows) {
  if (grpRows <= 0) {
    return;
  }

  if (NULL == *ppBlk) {
    *ppBlk = createDummyBlock((blkId == LEFT_BLK_ID) ? LEFT_BLK_ID : RIGHT_BLK_ID);
    assert(*ppBlk);
    assert(0 == blockDataEnsureCapacity(*ppBlk, jtCtx.blkRows));
    assert(NULL != taosArrayPush((blkId == LEFT_BLK_ID) ? jtCtx.leftBlkList : jtCtx.rightBlkList, ppBlk));
  }

  if (jtCtx.grpJoin) {
    (*ppBlk)->info.id.groupId = jtCtx.inGrpId;
  }

  jtCtx.inputStat |= (1 << blkId);

  SArray* pTableRows = NULL;
  int32_t tableOffset = 0;
  int32_t peerOffset = 0;
  bool keepRes = false;
  bool keepInput = false;
  if (blkId == LEFT_BLK_ID) {
    if ((jtCtx.joinType == JOIN_TYPE_LEFT || jtCtx.joinType == JOIN_TYPE_FULL) && (jtCtx.subType != JOIN_STYPE_SEMI && jtCtx.subType != JOIN_STYPE_ASOF && jtCtx.subType != JOIN_STYPE_WIN)) {
      keepRes = true;
    }
    peerOffset = MAX_SLOT_NUM;
  } else {
    if ((jtCtx.joinType == JOIN_TYPE_RIGHT || jtCtx.joinType == JOIN_TYPE_FULL) && (jtCtx.subType != JOIN_STYPE_SEMI && jtCtx.subType != JOIN_STYPE_ASOF && jtCtx.subType != JOIN_STYPE_WIN)) {
      keepRes = true;
    }
    tableOffset = MAX_SLOT_NUM;
  }

  if (JOIN_STYPE_ASOF == jtCtx.subType) {
    keepInput = jtCtx.asofOpType != OP_TYPE_EQUAL ? true : (blkId == LEFT_BLK_ID);
    pTableRows = (blkId == LEFT_BLK_ID) ? jtCtx.leftRowsList : jtCtx.rightRowsList;
  } else if (JOIN_STYPE_WIN == jtCtx.subType) {
    keepInput = true;
    pTableRows = (blkId == LEFT_BLK_ID) ? jtCtx.leftRowsList : jtCtx.rightRowsList;
  }
  
  int32_t filterNum = (blkId == LEFT_BLK_ID) ? jtCtx.leftFilterNum : jtCtx.rightFilterNum;
  int32_t peerFilterNum = (blkId == LEFT_BLK_ID) ? jtCtx.rightFilterNum : jtCtx.leftFilterNum;
  int32_t* filterCol = (blkId == LEFT_BLK_ID) ? jtCtx.leftFilterColList : jtCtx.rightFilterColList;
  
  char* pData = NULL;
  int32_t tmpInt = 0;
  int64_t tmpBigint = 0;
  bool isNull = false;
  bool filterOut = false;
  bool addToRowList = false;
  int32_t vRange = TMAX(grpRows / 3, 3);
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    jtCtx.grpOffset[c] = c * TMAX(100, grpRows);
  }
  
  for (int32_t i = 0; i < grpRows; ++i) {
    if ((*ppBlk)->info.rows >= (*ppBlk)->info.capacity) {
      *ppBlk = createDummyBlock((blkId == LEFT_BLK_ID) ? LEFT_BLK_ID : RIGHT_BLK_ID);
      assert(*ppBlk);
      assert(0 == blockDataEnsureCapacity(*ppBlk, jtCtx.blkRows));
      assert(NULL != taosArrayPush((blkId == LEFT_BLK_ID) ? jtCtx.leftBlkList : jtCtx.rightBlkList, ppBlk));
      if (jtCtx.grpJoin) {
        (*ppBlk)->info.id.groupId = jtCtx.inGrpId;
      }
    }

    filterOut = (peerFilterNum > 0 && (jtCtx.subType != JOIN_STYPE_ASOF && jtCtx.subType != JOIN_STYPE_WIN)) ? true : false;
    if (!filterOut) {
      TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);
      if (keepInput) {
        TAOS_MEMSET(jtCtx.inColBuf, 0, jtCtx.inColSize);
      }
    }

    addToRowList = true;
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          jtCtx.asc ? ++jtCtx.curTs : --jtCtx.curTs;
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
            if (!filterOut && filterNum && filterCol[c] && tmpInt <= INT_FILTER_VALUE) {
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
          if (!filterOut && filterNum && filterCol[c] && tmpBigint <= BIGINT_FILTER_VALUE) {
            filterOut = true;
          }
          break;
        default:
          break;
      }
      
      SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet((*ppBlk)->pDataBlock, c);
      assert(pCol);
      assert(0 == colDataSetVal(pCol, (*ppBlk)->info.rows, pData, isNull));

      if (keepInput) {
        if (!filterOut || (blkId != LEFT_BLK_ID)) {
          if (isNull) {
            *(char*)(jtCtx.inColBuf + c) = true;
          } else {
            TAOS_MEMCPY(jtCtx.inColBuf + jtCtx.inColOffset[c], pData, tDataTypes[jtInputColType[c]].bytes);
          }
        } else {
          addToRowList = false;
        }
      } else if (keepRes && !filterOut && jtCtx.resColList[tableOffset + c]) {
        if (isNull) {
          *(char*)(jtCtx.resColBuf + tableOffset + c) = true;
        } else {
          TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[tableOffset + c], pData, tDataTypes[jtInputColType[c]].bytes);
        }
      }
    }

    if (keepInput && addToRowList) {
      assert(NULL != taosArrayPush(pTableRows, jtCtx.inColBuf));
      if (blkId == RIGHT_BLK_ID) {
        bool fout = filterOut ? true : false;
        assert(NULL != taosArrayPush(jtCtx.rightFilterOut, &fout));
      }
    } 

    if (keepRes && !filterOut) {
      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        if (jtCtx.resColList[peerOffset + c]) {
          *(char*)(jtCtx.resColBuf + peerOffset + c) = true;
        }
      }
      
      pushResRow(jtCtx.resColBuf, jtCtx.resColSize);
    }
    
    (*ppBlk)->info.rows++;
  }

  if (keepInput) {
    if (JOIN_STYPE_ASOF == jtCtx.subType) {
      if (((jtCtx.asc && (jtCtx.asofOpType == OP_TYPE_GREATER_EQUAL || jtCtx.asofOpType == OP_TYPE_GREATER_THAN)) || (!jtCtx.asc && (jtCtx.asofOpType == OP_TYPE_LOWER_EQUAL || jtCtx.asofOpType == OP_TYPE_LOWER_THAN)) ) || jtCtx.asofOpType == OP_TYPE_EQUAL) {
        if (blkId == LEFT_BLK_ID) {
          appendAllAsofResRows();
        } else {
          trimForAsofJlimit();
        }
      } else {
        chkAppendAsofForwardGrpResRows(false);
      }
    } else {
      chkAppendWinResRows(false);
    }
  }

}

void createRowData(SSDataBlock* pBlk, int64_t tbOffset, int32_t rowIdx, int32_t vRange) {
  int32_t tmpInt = 0;
  int64_t tmpBig = 0;
  
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlk->pDataBlock, c);
    assert(pCol);

    int32_t rv = taosRand() % 10;
    switch (jtInputColType[c]) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        *(int64_t*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + jtCtx.colRowOffset[c]) = jtCtx.curTs;
        assert(0 == colDataSetVal(pCol, pBlk->info.rows, (char*)&jtCtx.curTs, false));
        break;
      case TSDB_DATA_TYPE_INT:
        if (rv) {
          tmpInt = (taosRand() % 2) ? INT_FILTER_VALUE + jtCtx.grpOffset[c] + taosRand() % vRange : INT_FILTER_VALUE - taosRand() % vRange;
          *(int32_t*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + jtCtx.colRowOffset[c]) = tmpInt;
          assert(0 == colDataSetVal(pCol, pBlk->info.rows, (char*)&tmpInt, false));
        } else {
          *(bool*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + c) = true;
          assert(0 == colDataSetVal(pCol, pBlk->info.rows, NULL, true));
        }
        break;
      case TSDB_DATA_TYPE_BIGINT:
        tmpBig = (taosRand() % 2) ? BIGINT_FILTER_VALUE + jtCtx.curKeyOffset++ : BIGINT_FILTER_VALUE - jtCtx.curKeyOffset++;
        *(int64_t*)(jtCtx.colRowDataBuf + tbOffset + rowIdx * jtCtx.blkRowSize + jtCtx.colRowOffset[c]) = tmpBig;
        assert(0 == colDataSetVal(pCol, pBlk->info.rows, (char*)&tmpBig, false));
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
    assert(jtCtx.colRowDataBuf);
  }

  TAOS_MEMSET(jtCtx.colRowDataBuf, 0, totalSize);

  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    jtCtx.grpOffset[c] = c * TMAX(leftGrpRows, rightGrpRows);
  }

  int32_t vRange = TMAX(leftGrpRows / 100, 3);
  for (int32_t i = 0; i < leftGrpRows; ++i) {
    if ((*ppLeft)->info.rows >= (*ppLeft)->info.capacity) {
      *ppLeft = createDummyBlock(LEFT_BLK_ID);
      assert(*ppLeft);
      assert(0 == blockDataEnsureCapacity(*ppLeft, jtCtx.blkRows));
      assert(NULL != taosArrayPush(jtCtx.leftBlkList, ppLeft));
      if (jtCtx.grpJoin) {
        (*ppLeft)->info.id.groupId = jtCtx.inGrpId;
      }
    }

    createRowData(*ppLeft, 0, i, vRange);
  }

  vRange = TMAX(rightGrpRows / 100, 3);
  for (int32_t i = 0; i < rightGrpRows; ++i) {
    if ((*ppRight)->info.rows >= (*ppRight)->info.capacity) {
      *ppRight = createDummyBlock(RIGHT_BLK_ID);
      assert(*ppRight);
      assert(0 == blockDataEnsureCapacity(*ppRight, jtCtx.blkRows));
      assert(NULL != taosArrayPush(jtCtx.rightBlkList, ppRight));
      if (jtCtx.grpJoin) {
        (*ppRight)->info.id.groupId = jtCtx.inGrpId;
      }
    }

    createRowData(*ppRight, rightOffset, i, vRange);
  }

}

void putNMatchRowToRes(char* lrow, int32_t tableOffset, int32_t peerOffset) {
  if (!jtCtrl.noKeepResRows) {
    TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);

    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (jtCtx.resColList[tableOffset + c]) {
        if (*(bool*)(lrow + c)) {
          *(bool*)(jtCtx.resColBuf + tableOffset + c) = true;
        } else {
          TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[tableOffset + c], lrow + jtCtx.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
        }
      }
    }

    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      if (jtCtx.resColList[peerOffset + c]) {
        *(bool*)(jtCtx.resColBuf + peerOffset + c) = true;
      }
    }
  }
  
  pushResRow(jtCtx.resColBuf, jtCtx.resColSize);
}

void putMatchRowToRes(char* lrow, char* rrow, int32_t cols) {
  if (!jtCtrl.noKeepResRows) {
    TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);

    if (cols & LEFT_TABLE_COLS) {
      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        if (jtCtx.resColList[c]) {
          if (*(bool*)(lrow + c)) {
            *(bool*)(jtCtx.resColBuf + c) = true;
          } else {
            TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[c], lrow + jtCtx.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
          }
        }
      }
    }

    if (cols & RIGHT_TABLE_COLS) {
      for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
        if (jtCtx.resColList[MAX_SLOT_NUM + c]) {
          if (*(bool*)(rrow + c)) {
            *(bool*)(jtCtx.resColBuf + MAX_SLOT_NUM + c) = true;
          } else {
            TAOS_MEMCPY(jtCtx.resColBuf + jtCtx.resColOffset[MAX_SLOT_NUM + c], rrow + jtCtx.colRowOffset[c], tDataTypes[jtInputColType[c]].bytes);
          }
        }
      }
    }
  }
  
  pushResRow(jtCtx.resColBuf, jtCtx.resColSize);
}



void innerJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
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
        break;
      }

      if (jtCtx.colOnNum && jtCtx.colOnList[c] && (*(bool*)(lrow + c))) {
        leftMatch = false;
        break;
      }
    }

    if (filterOut || !leftMatch) {
      continue;
    }

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
          break;
        }
      }
      
      if (filterOut || !rightMatch) {
        continue;
      }

      putMatchRowToRes(lrow, rrow, ALL_TABLE_COLS);      
    }
  }
  

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

      if (jtCtx.colOnNum && jtCtx.colOnList[c] && (*(bool*)(lrow + c))) {
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
        putMatchRowToRes(lrow, rrow, ALL_TABLE_COLS);
      }
    }

    if (!leftMatch && 0 == jtCtx.rightFilterNum) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
    }
  }
  

}



void semiJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  bool leftMatch = false, rightMatch = false, filterOut = false;
  void* lValue = NULL, *rValue = NULL, *filterValue = NULL;
  int64_t lBig = 0, rBig = 0, fbig = 0;
  int64_t leftTbOffset = 0;
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;
  char* rrow = NULL;
  
  for (int32_t l = 0; l < leftGrpRows; ++l) {
    char* lrow = jtCtx.colRowDataBuf + leftTbOffset + jtCtx.blkRowSize * l;
    
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
        break;
      }

      if (jtCtx.colOnNum && jtCtx.colOnList[c] && (*(bool*)(lrow + c))) {
        leftMatch = false;
        break;
      }
    }

    if (filterOut || !leftMatch) {
      continue;
    }

    for (int32_t r = 0; r < rightGrpRows; ++r) {
      rrow = jtCtx.colRowDataBuf + rightTbOffset + jtCtx.blkRowSize * r;
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
          break;
        }
      }
      
      if (filterOut || !rightMatch) {
        continue;
      }

      break;
    }

    if (!filterOut && rightMatch) {
      putMatchRowToRes(lrow, rrow, LEFT_TABLE_COLS);      
    }
  }
  

}




void antiJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  bool leftMatch = false, rightMatch = false, filterOut = false;
  void* lValue = NULL, *rValue = NULL, *filterValue = NULL;
  int64_t lBig = 0, rBig = 0, fbig = 0;
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;

  assert(0 == jtCtx.rightFilterNum);
  
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
      
      if (jtCtx.colOnNum && jtCtx.colOnList[c] && (*(bool*)(lrow + c))) {
        leftMatch = false;
      }
    }

    if (filterOut) {
      continue;
    }

    if (false == leftMatch) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
      continue;
    }

    leftMatch = false;
    for (int32_t r = 0; r < rightGrpRows; ++r) {
      char* rrow = jtCtx.colRowDataBuf + rightTbOffset + jtCtx.blkRowSize * r;
      rightMatch = true;

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
      }

      if (rightMatch) {
        leftMatch = true;
        break;
      }
    }

    if (!leftMatch) {
      putNMatchRowToRes(lrow, 0, MAX_SLOT_NUM);
    }
  }
  

}

void addAsofEqInRows(int32_t rowsNum, int64_t tbOffset, bool leftTable) {
  bool filterOut = false;
  void* cvalue = NULL;
  int64_t cbig = 0, fbig = 0;
  int32_t filterNum = leftTable ? jtCtx.leftFilterNum : jtCtx.rightFilterNum;
  int32_t* filterCol = leftTable ? jtCtx.leftFilterColList : jtCtx.rightFilterColList;
  SArray* rowList = leftTable ? jtCtx.leftRowsList : jtCtx.rightRowsList;

  if (!leftTable) {
    rowsNum = TMIN(rowsNum, jtCtx.jLimit);
  }

  for (int32_t l = 0; l < rowsNum; ++l) {
    char* row = jtCtx.colRowDataBuf + tbOffset + jtCtx.blkRowSize * l;
    
    filterOut = false;
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      cvalue = row + jtCtx.colRowOffset[c];
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          fbig = TIMESTAMP_FILTER_VALUE;
          cbig = *(int64_t*)cvalue;
          break;
        case TSDB_DATA_TYPE_INT:
          fbig = INT_FILTER_VALUE;
          cbig = *(int32_t*)cvalue;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          fbig = BIGINT_FILTER_VALUE;
          cbig = *(int64_t*)cvalue;
          break;
        default:
          break;
      }
      
      if (filterNum && filterCol[c] && ((*(bool*)(row + c)) || cbig <= fbig)) {
        filterOut = true;
        break;
      }
    }

    if (filterOut && leftTable) {
      continue;
    }

    assert(NULL != taosArrayPush(rowList, row));
    if (!leftTable) {
      assert(NULL != taosArrayPush(jtCtx.rightFilterOut, &filterOut));
    }
  }

  if (!leftTable && ((jtCtx.asc && (jtCtx.asofOpType == OP_TYPE_GREATER_EQUAL || jtCtx.asofOpType == OP_TYPE_GREATER_THAN)) || (!jtCtx.asc && (jtCtx.asofOpType == OP_TYPE_LOWER_EQUAL || jtCtx.asofOpType == OP_TYPE_LOWER_THAN))) || jtCtx.asofOpType == OP_TYPE_EQUAL) {
    trimForAsofJlimit();
  }
}

void asofJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;

  if (jtCtx.asc) {
    switch (jtCtx.asofOpType) {
      case OP_TYPE_GREATER_THAN:
        addAsofEqInRows(leftGrpRows, 0, true);
        appendAllAsofResRows();
        addAsofEqInRows(rightGrpRows, rightTbOffset, false);
        break;
      case OP_TYPE_GREATER_EQUAL:
        addAsofEqInRows(leftGrpRows, 0, true);
        addAsofEqInRows(rightGrpRows, rightTbOffset, false);
        appendAllAsofResRows();
        break;
      case OP_TYPE_LOWER_THAN:
      case OP_TYPE_LOWER_EQUAL:
        addAsofEqInRows(leftGrpRows, 0, true);
        addAsofEqInRows(rightGrpRows, rightTbOffset, false);
        chkAppendAsofForwardGrpResRows(false);
        break;
      case OP_TYPE_EQUAL:
        taosArrayClear(jtCtx.leftRowsList);
        taosArrayClear(jtCtx.rightRowsList);
        taosArrayClear(jtCtx.rightFilterOut);
        addAsofEqInRows(leftGrpRows, 0, true);
        addAsofEqInRows(rightGrpRows, rightTbOffset, false);
        chkAppendAsofForwardGrpResRows(true);
        taosArrayClear(jtCtx.leftRowsList);
        taosArrayClear(jtCtx.rightRowsList);
        taosArrayClear(jtCtx.rightFilterOut);
        break;
      default:
        return;
    }

    return;
  } 
  
  switch (jtCtx.asofOpType) {
    case OP_TYPE_LOWER_THAN:
      addAsofEqInRows(leftGrpRows, 0, true);
      appendAllAsofResRows();
      addAsofEqInRows(rightGrpRows, rightTbOffset, false);
      break;
    case OP_TYPE_LOWER_EQUAL:
      addAsofEqInRows(leftGrpRows, 0, true);
      addAsofEqInRows(rightGrpRows, rightTbOffset, false);
      appendAllAsofResRows();
      break;
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
      addAsofEqInRows(leftGrpRows, 0, true);
      addAsofEqInRows(rightGrpRows, rightTbOffset, false);
      chkAppendAsofForwardGrpResRows(false);
      break;
    case OP_TYPE_EQUAL:
      taosArrayClear(jtCtx.leftRowsList);
      taosArrayClear(jtCtx.rightRowsList);
      taosArrayClear(jtCtx.rightFilterOut);
      addAsofEqInRows(leftGrpRows, 0, true);
      addAsofEqInRows(rightGrpRows, rightTbOffset, false);
      chkAppendAsofForwardGrpResRows(true);
      taosArrayClear(jtCtx.leftRowsList);
      taosArrayClear(jtCtx.rightRowsList);
      taosArrayClear(jtCtx.rightFilterOut);
      break;
    default:
      return;
  }
}


void addWinEqInRows(int32_t rowsNum, int64_t tbOffset, bool leftTable) {
  bool filterOut = false;
  void* cvalue = NULL;
  int64_t cbig = 0, fbig = 0;
  int32_t filterNum = leftTable ? jtCtx.leftFilterNum : jtCtx.rightFilterNum;
  int32_t* filterCol = leftTable ? jtCtx.leftFilterColList : jtCtx.rightFilterColList;
  SArray* rowList = leftTable ? jtCtx.leftRowsList : jtCtx.rightRowsList;

  for (int32_t l = 0; l < rowsNum; ++l) {
    char* row = jtCtx.colRowDataBuf + tbOffset + jtCtx.blkRowSize * l;
    
    filterOut = false;
    
    for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
      cvalue = row + jtCtx.colRowOffset[c];
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
          fbig = TIMESTAMP_FILTER_VALUE;
          cbig = *(int64_t*)cvalue;
          break;
        case TSDB_DATA_TYPE_INT:
          fbig = INT_FILTER_VALUE;
          cbig = *(int32_t*)cvalue;
          break;
        case TSDB_DATA_TYPE_BIGINT:
          fbig = BIGINT_FILTER_VALUE;
          cbig = *(int64_t*)cvalue;
          break;
        default:
          break;
      }
      
      if (filterNum && filterCol[c] && ((*(bool*)(row + c)) || cbig <= fbig)) {
        filterOut = true;
        break;
      }
    }

    if (filterOut && leftTable) {
      continue;
    }

    assert(NULL != taosArrayPush(rowList, row));
    if (!leftTable) {
      assert(NULL != taosArrayPush(jtCtx.rightFilterOut, &filterOut));
    }
  }
}


void winJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;

  addWinEqInRows(leftGrpRows, 0, true);
  addWinEqInRows(rightGrpRows, rightTbOffset, false);
  chkAppendWinResRows(false);
}



void fullJoinAppendEqGrpRes(int32_t leftGrpRows, int32_t rightGrpRows) {
  bool leftMatch = false, rightMatch = false, lfilterOut = false, rfilterOut = false;
  void* lValue = NULL, *rValue = NULL, *filterValue = NULL;
  int64_t lBig = 0, rBig = 0, fbig = 0;
  int64_t rightTbOffset = jtCtx.blkRowSize * leftGrpRows;

  TAOS_MEMSET(jtCtx.rightFinMatch, 0, rightGrpRows * sizeof(bool));
  
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
        putMatchRowToRes(lrow, rrow, ALL_TABLE_COLS);
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
    case JOIN_TYPE_INNER:
      innerJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
      break;
    case JOIN_TYPE_LEFT: {
      switch (jtCtx.subType) {
        case JOIN_STYPE_OUTER:
          leftJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
          break;
        case JOIN_STYPE_SEMI:
          semiJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
          break;
        case JOIN_STYPE_ANTI:
          antiJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
          break;
        case JOIN_STYPE_ASOF:
          asofJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
          break;
        case JOIN_STYPE_WIN:
          winJoinAppendEqGrpRes(leftGrpRows, rightGrpRows);
          break;
        default:
          break;
      }
      break;
    }
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

  jtCtx.asc ? ++jtCtx.curTs : --jtCtx.curTs;

  if (NULL == *ppLeft && leftGrpRows > 0) {
    *ppLeft = createDummyBlock(LEFT_BLK_ID);
    assert(*ppLeft);
    assert(0 == blockDataEnsureCapacity(*ppLeft, jtCtx.blkRows));
    assert(NULL != taosArrayPush(jtCtx.leftBlkList, ppLeft));
  }

  if (jtCtx.grpJoin) {
    (*ppLeft)->info.id.groupId = jtCtx.inGrpId;
  }

  if (NULL == *ppRight && rightGrpRows > 0) {
    *ppRight = createDummyBlock(RIGHT_BLK_ID);
    assert(*ppRight);
    assert(0 == blockDataEnsureCapacity(*ppRight, jtCtx.blkRows));
    assert(NULL != taosArrayPush(jtCtx.rightBlkList, ppRight));
  }

  if (jtCtx.grpJoin) {
    (*ppRight)->info.id.groupId = jtCtx.inGrpId;
  }


  makeAppendBlkData(ppLeft, ppRight, leftGrpRows, rightGrpRows);

  appendEqGrpRes(leftGrpRows, rightGrpRows);
}

void forceFlushResRows() {
  if (JOIN_STYPE_ASOF == jtCtx.subType && taosArrayGetSize(jtCtx.leftRowsList) > 0) {
    assert((jtCtx.asc && (OP_TYPE_LOWER_EQUAL == jtCtx.asofOpType || OP_TYPE_LOWER_THAN == jtCtx.asofOpType))
         || (!jtCtx.asc && (OP_TYPE_GREATER_EQUAL == jtCtx.asofOpType || OP_TYPE_GREATER_THAN == jtCtx.asofOpType)));
    chkAppendAsofForwardGrpResRows(true);
  } else if (JOIN_STYPE_WIN == jtCtx.subType && taosArrayGetSize(jtCtx.leftRowsList) > 0) {
    chkAppendWinResRows(true);
  }

  taosArrayClear(jtCtx.rightRowsList);
  taosArrayClear(jtCtx.rightFilterOut);
  taosArrayClear(jtCtx.leftRowsList);
  
}

void createBothBlkRowsData(void) {
  SSDataBlock* pLeft = NULL;
  SSDataBlock* pRight = NULL;

  jtCtx.leftTotalRows = taosRand() % jtCtx.leftMaxRows;
  jtCtx.rightTotalRows = taosRand() % jtCtx.rightMaxRows;

  int32_t minTotalRows = TMIN(jtCtx.leftTotalRows, jtCtx.rightTotalRows);
  int32_t maxTotalRows = TMAX(jtCtx.leftTotalRows, jtCtx.rightTotalRows);
  jtCtx.curTs = jtCtx.asc ? (TIMESTAMP_FILTER_VALUE - minTotalRows / 5) : (TIMESTAMP_FILTER_VALUE + 4 * maxTotalRows / 5); 

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

    if (jtCtx.grpJoin && (0 == taosRand() % 3)) {
      forceFlushResRows();
      jtCtx.inGrpId++;
      pLeft = NULL;
      pRight = NULL;
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

  forceFlushResRows();
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
    assert(jtCtx.rightFinMatch);
  }

  taosArrayClear(jtCtx.leftRowsList);
  taosArrayClear(jtCtx.rightRowsList);
  taosArrayClear(jtCtx.rightFilterOut);

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
    for (const auto &f : result) {
      stub.set(f.second, getDummyInputBlock);
    }
#endif
#ifdef LINUX
    AddrAny                       any("libexecutor.so");
    std::map<std::string, void *> result;
    any.get_global_func_addr_dynsym("^getNextBlockFromDownstreamRemain$", result);
    for (const auto &f : result) {
      stub.set(f.second, getDummyInputBlock);
    }
#endif
  }
}

void printColList(char* title, bool left, int32_t* colList, bool filter, char* opStr) {
  bool first = true;
  
  JT_PRINTF("\t %s:", title);
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    if (colList[i]) {
      if (!first) {
        JT_PRINTF(" AND ");
      }
      first = false;
      if (filter) {
        JT_PRINTF("%sc%d%s%" PRId64 , left ? "l" : "r", i, opStr, jtFilterValue[i]);
      } else {
        JT_PRINTF("lc%d%src%d", i, opStr, i);
      }
    }
  }
  JT_PRINTF("\n");
}

void printInputRowData(SSDataBlock* pBlk, int32_t* rowIdx) {
  if (jtCtx.grpJoin) {
    JT_PRINTF("%5" PRIu64, pBlk->info.id.groupId);
  }
  for (int32_t c = 0; c < MAX_SLOT_NUM; ++c) {
    SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlk->pDataBlock, c);
    assert(pCol);
    assert(pCol->info.type == jtInputColType[c]);
    if (colDataIsNull_s(pCol, *rowIdx)) {
      JT_PRINTF("%18s", " NULL");
    } else {
      switch (jtInputColType[c]) {
        case TSDB_DATA_TYPE_TIMESTAMP:
        case TSDB_DATA_TYPE_BIGINT:
          JT_PRINTF("%18" PRId64, *(int64_t*)colDataGetData(pCol, *rowIdx));
          break;
        case TSDB_DATA_TYPE_INT:
          JT_PRINTF("%18d", *(int32_t*)colDataGetData(pCol, *rowIdx));
          break;
        default:
          assert(0);
      }
    }
  }

  (*rowIdx)++;
}

void printInputData() {
  int32_t leftRowIdx = 0, rightRowIdx = 0;
  
  JT_PRINTF("\nInput Data:\n");
  while (jtCtx.leftBlkReadIdx < taosArrayGetSize(jtCtx.leftBlkList) || jtCtx.rightBlkReadIdx < taosArrayGetSize(jtCtx.rightBlkList)) {
    if (jtCtx.leftBlkReadIdx < taosArrayGetSize(jtCtx.leftBlkList)) {
      while (true) {
        SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.leftBlkList, jtCtx.leftBlkReadIdx);
        assert(pBlk);
        if (leftRowIdx < pBlk->info.rows) {
          printInputRowData(pBlk, &leftRowIdx);
          break;
        }

        JT_PRINTF("\t%*s-------------------------blk end-------------------------------", jtCtx.grpJoin ? 6 : 0, " ");
        jtCtx.leftBlkReadIdx++;
        leftRowIdx = 0;
        break;
      }
    } else {
      JT_PRINTF("%*s", jtCtx.grpJoin ? 77 : 72, " ");
    }

    if (jtCtx.rightBlkReadIdx < taosArrayGetSize(jtCtx.rightBlkList)) {
      while (true) {
        SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.rightBlkList, jtCtx.rightBlkReadIdx);
        assert(pBlk);
        if (rightRowIdx < pBlk->info.rows) {
          printInputRowData(pBlk, &rightRowIdx);
          break;
        }
        
        JT_PRINTF("\t%*s--------------------------blk end----------------------------\t", jtCtx.grpJoin ? 6 : 0, " ");
        jtCtx.rightBlkReadIdx++;
        rightRowIdx = 0;
        break;
      }
    }

    JT_PRINTF("\n");
  }

  jtCtx.leftBlkReadIdx = jtCtx.rightBlkReadIdx = 0;
}

char* getInputStatStr(char* inputStat) {
  if (jtCtx.inputStat & (1 << LEFT_BLK_ID)) {
    TAOS_STRCAT(inputStat, "L");
  }
  if (jtCtx.inputStat & (1 << RIGHT_BLK_ID)) {
    TAOS_STRCAT(inputStat, "R");
  }
  if (jtCtx.inputStat & (1 << 2)) {
    TAOS_STRCAT(inputStat, "E");
  }
  return inputStat;
}

char* getAsofOpStr() {
  switch (jtCtx.asofOpType) {
    case OP_TYPE_GREATER_THAN:
      return ">";
    case OP_TYPE_GREATER_EQUAL:
      return ">=";
    case OP_TYPE_LOWER_THAN:
      return "<";
    case OP_TYPE_LOWER_EQUAL:
      return "<=";
    case OP_TYPE_EQUAL:
      return "=";    
    default:
      return "UNKNOWN";
  }
}

void printBasicInfo(char* caseName) {
  if (!jtCtrl.printTestInfo) {
    return;
  }

  char inputStat[4] = {0};
  JT_PRINTF("\n%dth TEST [%s] START\nBasic Info:\n\t asc:%d\n\t filter:%d\n\t maxRows:left-%d right-%d\n\t "
    "maxGrpRows:left-%d right-%d\n\t blkRows:%d\n\t colCond:%s\n\t joinType:%s\n\t "
    "subType:%s\n\t inputStat:%s\n\t groupJoin:%s\n", jtCtx.loopIdx, caseName, jtCtx.asc, jtCtx.filter, jtCtx.leftMaxRows, jtCtx.rightMaxRows, 
    jtCtx.leftMaxGrpRows, jtCtx.rightMaxGrpRows, jtCtx.blkRows, jtColCondStr[jtCtx.colCond], jtJoinTypeStr[jtCtx.joinType],
    jtSubTypeStr[jtCtx.subType], getInputStatStr(inputStat), jtCtx.grpJoin ? "true" : "false");
    
  if (JOIN_STYPE_ASOF == jtCtx.subType) {
    JT_PRINTF("\t asofOp:%s\n\t JLimit:%" PRId64 "\n", getAsofOpStr(), jtCtx.jLimit);
  } else if (JOIN_STYPE_WIN == jtCtx.subType) {
    JT_PRINTF("\t windowOffset:[%" PRId64 ", %" PRId64 "]\n\t JLimit:%" PRId64 "\n", jtCtx.winStartOffset, jtCtx.winEndOffset, jtCtx.jLimit);
  }
  
  JT_PRINTF("Input Info:\n\t totalBlk:left-%d right-%d\n\t totalRows:left-%d right-%d\n\t "
    "blkRowSize:%d\n\t inputCols:left-%s %s %s %s right-%s %s %s %s\n", 
    (int32_t)taosArrayGetSize(jtCtx.leftBlkList), (int32_t)taosArrayGetSize(jtCtx.rightBlkList), 
    jtCtx.leftTotalRows, jtCtx.rightTotalRows,
    jtCtx.blkRowSize, tDataTypes[jtInputColType[0]].name, tDataTypes[jtInputColType[1]].name,
    tDataTypes[jtInputColType[2]].name, tDataTypes[jtInputColType[3]].name, tDataTypes[jtInputColType[0]].name, 
    tDataTypes[jtInputColType[1]].name, tDataTypes[jtInputColType[2]].name, tDataTypes[jtInputColType[3]].name);

  if (jtCtx.colEqNum) {
    JT_PRINTF("\t colEqNum:%d\n", jtCtx.colEqNum);
    printColList("colEqList", false, jtCtx.colEqList, false, "=");
  }

  if (jtCtx.colOnNum) {
    JT_PRINTF("\t colOnNum:%d\n", jtCtx.colOnNum);
    printColList("colOnList", false, jtCtx.colOnList, false, ">");
  }  

  if (jtCtx.leftFilterNum) {
    JT_PRINTF("\t leftFilterNum:%d\n", jtCtx.leftFilterNum);
    printColList("leftFilterList", true, jtCtx.leftFilterColList, true, ">");
  }

  if (jtCtx.rightFilterNum) {
    JT_PRINTF("\t rightFilterNum:%d\n", jtCtx.rightFilterNum);
    printColList("rightFilterList", false, jtCtx.rightFilterColList, true, ">");
  }

  JT_PRINTF("\t resColSize:%d\n\t resColNum:%d\n\t resColList:", jtCtx.resColSize, jtCtx.resColNum);
  for (int32_t i = 0; i < jtCtx.resColNum; ++i) {
    int32_t s = jtCtx.resColInSlot[i];
    int32_t idx = s >= MAX_SLOT_NUM ? s - MAX_SLOT_NUM : s;
    JT_PRINTF("%sc%d[%s]\t", s >= MAX_SLOT_NUM ? "r" : "l", s, tDataTypes[jtInputColType[idx]].name);
  }

  if (jtCtrl.printInputRow) {
    printInputData();
  }
}

void printOutputInfo() {
  if (!jtCtrl.printTestInfo) {
    return;
  }
  
  JT_PRINTF("\nOutput Info:\n\t expectedRows:%d\n\t ", jtCtx.resRows);
  JT_PRINTF("Actual Result:\n");
}

void printActualResInfo() {
  if (!jtCtrl.printTestInfo) {
    return;
  }

  JT_PRINTF("Actual Result Summary:\n\t blkNum:%d\n\t rowNum:%d%s\n\t leftBlkRead:%d\n\t rightBlkRead:%d\n\t +rows:%d%s\n\t "
    "-rows:%d%s\n\t matchRows:%d%s\n\t executionTime:%" PRId64 "us\n", 
    jtRes.blkNum, jtRes.rowNum, 
    jtRes.rowNum == jtCtx.resRows ? "" : "*",
    jtCtx.leftBlkReadIdx, jtCtx.rightBlkReadIdx,
    jtRes.addRowNum, jtRes.addRowNum ? "*" : "",
    jtRes.subRowNum, jtRes.subRowNum ? "*" : "",
    jtRes.matchNum, jtRes.matchNum == jtCtx.resRows ? "" : "*",
    taosGetTimestampUs() - jtCtx.startTsUs);
}

void printStatInfo(char* caseName) {
  JT_PRINTF("\n TEST [%s] Stat:\n\t maxResRows:%d\n\t maxResBlkRows:%d\n\t totalResRows:%" PRId64 "\n\t useMSecs:%" PRId64 "\n",
    caseName, jtStat.maxResRows, jtStat.maxResBlkRows, jtStat.totalResRows, jtStat.useMSecs);
  
}

void checkJoinDone(char* caseName) {
  int32_t iter = 0;
  void* p = NULL;
  void* key = NULL;
  if (!jtCtrl.noKeepResRows) {
    while (NULL != (p = tSimpleHashIterate(jtCtx.jtResRows, p, &iter))) {
      key = tSimpleHashGetKey(p, NULL);
      jtRes.succeed = false;
      jtRes.subRowNum += *(int32_t*)p;
      for (int32_t i = 0; i < *(int32_t*)p; ++i) {
        printResRow((char*)key, 0);
      }
    }
  }

  printActualResInfo();
  
  JT_PRINTF("\n%dth TEST [%s] Final Result: %s\n", jtCtx.loopIdx, caseName, jtRes.succeed ? "SUCCEED" : "FAILED");
}

void putRowToResColBuf(SSDataBlock* pBlock, int32_t r, bool ignoreTbCols) {
  for (int32_t c = 0; c < jtCtx.resColNum; ++c) {
    int32_t slot = jtCtx.resColInSlot[c];
    if (ignoreTbCols && ((jtCtx.leftColOnly && slot >= MAX_SLOT_NUM) ||
        (jtCtx.rightColOnly && slot < MAX_SLOT_NUM))) {
      continue;
    }
    
    SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, c);
    assert(pCol);
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
  if (jtCtrl.noKeepResRows) {
    jtRes.matchNum += pBlock->info.rows;
  } else {
    for (int32_t r = 0; r < pBlock->info.rows; ++r) {
      TAOS_MEMSET(jtCtx.resColBuf, 0, jtCtx.resColSize);

      putRowToResColBuf(pBlock, r, true);
      
      char* value = (char*)tSimpleHashGet(jtCtx.jtResRows, jtCtx.resColBuf, jtCtx.resColSize);
      if (NULL == value) {
        putRowToResColBuf(pBlock, r, false);
        printResRow(jtCtx.resColBuf, 1);
        jtRes.succeed = false;
        jtRes.addRowNum++;
        continue;
      }

      rmResRow();
      
      putRowToResColBuf(pBlock, r, false);
      printResRow(jtCtx.resColBuf, 2);
      jtRes.matchNum++;
    }
  }
}

void resetForJoinRerun(int32_t dsNum, SSortMergeJoinPhysiNode* pNode, SExecTaskInfo* pTask) {
  jtCtx.leftBlkReadIdx = 0;
  jtCtx.rightBlkReadIdx = 0;
  jtCtx.curKeyOffset = 0;

  TAOS_MEMSET(&jtRes, 0, sizeof(jtRes));
  jtRes.succeed = true;

  SOperatorInfo* pDownstreams[2];
  createDummyDownstreamOperators(2, pDownstreams);  
  SOperatorInfo* ppDownstreams[] = {pDownstreams[0], pDownstreams[1]};
  int32_t code = createMergeJoinOperatorInfo(ppDownstreams, 2, pNode, pTask, &jtCtx.pJoinOp);
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
  TAOS_STRCPY(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, false) < 0) {
    JT_PRINTF("failed to open log file in directory:%s\n", tsLogDir);
  }
}



void initJoinTest() {
  jtCtx.leftBlkList = taosArrayInit(10, POINTER_BYTES);
  jtCtx.rightBlkList = taosArrayInit(10, POINTER_BYTES);
  assert(jtCtx.leftBlkList && jtCtx.rightBlkList);
  jtCtx.jtResRows = tSimpleHashInit(10000000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY));
  assert(jtCtx.jtResRows);

  joinTestReplaceRetrieveFp();

  if (jtCtrl.logHistory) {
    jtStat.pHistory = taosArrayInit(100000, sizeof(SJoinTestHistory));
    assert(jtStat.pHistory);
  }

  int32_t offset = MAX_SLOT_NUM * sizeof(bool);
  for (int32_t i = 0; i < MAX_SLOT_NUM; ++i) {
    jtCtx.inColOffset[i] = offset;
    offset += tDataTypes[jtInputColType[i]].bytes;
  }
  jtCtx.inColSize = offset;
  jtCtx.inColBuf = (char*)taosMemoryMalloc(jtCtx.inColSize);
  assert(jtCtx.inColBuf);

  jtCtx.leftRowsList = taosArrayInit(1024, jtCtx.inColSize);
  jtCtx.rightRowsList = taosArrayInit(1024, jtCtx.inColSize);
  jtCtx.rightFilterOut = taosArrayInit(1024, sizeof(bool));
  assert(jtCtx.leftRowsList && jtCtx.rightRowsList && jtCtx.rightFilterOut);

  jtInitLogFile();
}

void handleTestDone() {
  if (jtCtrl.logHistory) {
    SJoinTestHistory h;
    TAOS_MEMCPY(&h.ctx, &jtCtx, sizeof(h.ctx));
    TAOS_MEMCPY(&h.res, &jtRes, sizeof(h.res));
    assert(NULL != taosArrayPush(jtStat.pHistory, &h));
  }
  
  int32_t blkNum = taosArrayGetSize(jtCtx.leftBlkList);
  for (int32_t i = 0; i < blkNum; ++i) {
    SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.leftBlkList, i);
    assert(pBlk);
    (void)blockDataDestroy(pBlk);
  }
  taosArrayClear(jtCtx.leftBlkList);

  blkNum = taosArrayGetSize(jtCtx.rightBlkList);
  for (int32_t i = 0; i < blkNum; ++i) {
    SSDataBlock* pBlk = (SSDataBlock*)taosArrayGetP(jtCtx.rightBlkList, i);
    assert(pBlk);
    (void)blockDataDestroy(pBlk);
  }
  taosArrayClear(jtCtx.rightBlkList);  

  tSimpleHashClear(jtCtx.jtResRows);
  jtCtx.resRows = 0;

  jtCtx.inputStat = 0;
}

void runSingleTest(char* caseName, SJoinTestParam* param) {
  bool contLoop = true;
  
  SSortMergeJoinPhysiNode* pNode = createDummySortMergeJoinPhysiNode(param);    
  assert(pNode);
  createDummyBlkList(1000, 1000, 1000, 1000, 100);
  
  while (contLoop) {
    rerunBlockedHere();
    resetForJoinRerun(2, pNode, param->pTask);
    printBasicInfo(caseName);
    printOutputInfo();

    jtCtx.startTsUs = taosGetTimestampUs();
    while (true) {
      SSDataBlock* pBlock = NULL;
      int32_t code = jtCtx.pJoinOp->fpSet.getNextFn(jtCtx.pJoinOp, &pBlock);
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

#if 1
#if 1
TEST(innerJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "innerJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_INNER;
  param.subType = JOIN_STYPE_NONE;
  param.cond = TEST_NO_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(innerJoin, eqCondTest) {
  SJoinTestParam param;
  char* caseName = "innerJoin:eqCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_INNER;
  param.subType = JOIN_STYPE_NONE;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(innerJoin, onCondTest) {
  SJoinTestParam param;
  char* caseName = "innerJoin:onCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_INNER;
  param.subType = JOIN_STYPE_NONE;
  param.cond = TEST_ON_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(innerJoin, fullCondTest) {
  SJoinTestParam param;
  char* caseName = "innerJoin:fullCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_INNER;
  param.subType = JOIN_STYPE_NONE;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
#if 1
TEST(leftOuterJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "leftOuterJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_NO_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;  
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
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  param.grpJoin = false;  
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_ON_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
#if 1
TEST(fullOuterJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "fullOuterJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_NO_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_ON_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_FULL;
  param.subType = JOIN_STYPE_OUTER;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
#if 1
TEST(leftSemiJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "leftSemiJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_SEMI;
  param.cond = TEST_NO_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(leftSemiJoin, eqCondTest) {
  SJoinTestParam param;
  char* caseName = "leftSemiJoin:eqCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_SEMI;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(leftSemiJoin, onCondTest) {
  SJoinTestParam param;
  char* caseName = "leftSemiJoin:onCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_SEMI;
  param.cond = TEST_ON_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(leftSemiJoin, fullCondTest) {
  SJoinTestParam param;
  char* caseName = "leftSemiJoin:fullCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_SEMI;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
#if 1
TEST(leftAntiJoin, noCondTest) {
  SJoinTestParam param;
  char* caseName = "leftAntiJoin:noCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ANTI;
  param.cond = TEST_NO_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(leftAntiJoin, eqCondTest) {
  SJoinTestParam param;
  char* caseName = "leftAntiJoin:eqCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ANTI;
  param.cond = TEST_EQ_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(leftAntiJoin, onCondTest) {
  SJoinTestParam param;
  char* caseName = "leftAntiJoin:onCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ANTI;
  param.cond = TEST_ON_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
TEST(leftAntiJoin, fullCondTest) {
  SJoinTestParam param;
  char* caseName = "leftAntiJoin:fullCondTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ANTI;
  param.cond = TEST_FULL_COND;
  param.asc = true;
  param.grpJoin = false;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
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
#if 1
TEST(leftAsofJoin, noCondGreaterThanTest) {
  SJoinTestParam param;
  char* caseName = "leftAsofJoin:noCondGreaterThanTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ASOF;
  param.cond = TEST_NO_COND;
  param.asofOp = OP_TYPE_GREATER_THAN;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.jLimit = taosRand() % 2 ? (1 + (taosRand() % JT_MAX_JLIMIT)) : 1;

    param.grpJoin = taosRand() % 2 ? true : false;
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;
    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(leftAsofJoin, noCondGreaterEqTest) {
  SJoinTestParam param;
  char* caseName = "leftAsofJoin:noCondGreaterEqTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ASOF;
  param.cond = TEST_NO_COND;
  param.asofOp = OP_TYPE_GREATER_EQUAL;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.jLimit = taosRand() % 2 ? (1 + (taosRand() % JT_MAX_JLIMIT)) : 1;

    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(leftAsofJoin, noCondEqTest) {
  SJoinTestParam param;
  char* caseName = "leftAsofJoin:noCondEqTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ASOF;
  param.cond = TEST_NO_COND;
  param.asofOp = OP_TYPE_EQUAL;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.jLimit = taosRand() % 2 ? (1 + (taosRand() % JT_MAX_JLIMIT)) : 1;
    
    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#if 1
TEST(leftAsofJoin, noCondLowerThanTest) {
  SJoinTestParam param;
  char* caseName = "leftAsofJoin:noCondLowerThanTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ASOF;
  param.cond = TEST_NO_COND;
  param.asofOp = OP_TYPE_LOWER_THAN;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.jLimit = taosRand() % 2 ? (1 + (taosRand() % JT_MAX_JLIMIT)) : 1;
    
    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif


#if 1
TEST(leftAsofJoin, noCondLowerEqTest) {
  SJoinTestParam param;
  char* caseName = "leftAsofJoin:noCondLowerEqTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_ASOF;
  param.cond = TEST_NO_COND;
  param.asofOp = OP_TYPE_LOWER_EQUAL;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.jLimit = taosRand() % 2 ? (1 + (taosRand() % JT_MAX_JLIMIT)) : 1;
    
    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif

#endif


#if 1
#if 1
TEST(leftWinJoin, noCondProjectionTest) {
  SJoinTestParam param;
  char* caseName = "leftWinJoin:noCondProjectionTest";
  SExecTaskInfo* pTask = createDummyTaskInfo(caseName);
  assert(pTask);

  param.pTask = pTask;
  param.joinType = JOIN_TYPE_LEFT;
  param.subType = JOIN_STYPE_WIN;
  param.cond = TEST_NO_COND;
  param.asc = true;
  
  for (jtCtx.loopIdx = 0; jtCtx.loopIdx < JT_MAX_LOOP; ++jtCtx.loopIdx) {
    param.asc = !param.asc;
    param.jLimit = taosRand() % 2 ? (1 + (taosRand() % JT_MAX_JLIMIT)) : 1;
    
    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = false;
    runSingleTest(caseName, &param);

    param.grpJoin = taosRand() % 2 ? true : false;  
    param.filter = true;
    runSingleTest(caseName, &param);
  }

  printStatInfo(caseName); 
  taosMemoryFree(pTask);
}
#endif


#endif



int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  initJoinTest();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



#pragma GCC diagnosti
