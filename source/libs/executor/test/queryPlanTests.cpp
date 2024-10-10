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
#include "functionMgt.h"

namespace {

#define QPT_MAX_LOOP          100000000
#define QPT_MAX_SUBPLAN_LEVEL 1000
#define QPT_MAX_WHEN_THEN_NUM 10
#define QPT_MAX_NODE_LEVEL    5
#define QPT_MAX_STRING_LEN    1048576
#define QPT_MAX_FUNC_PARAM    5
#define QPT_MAX_LOGIC_PARAM   5
#define QPT_MAX_NODE_LIST_NUM 5
#define QPT_DEFAULT_VNODE_NUM 5
#define QPT_MAX_COLUMN_NUM    8192


int32_t QPT_PHYSIC_NODE_LIST[] = {
  QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_PROJECT,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN,
  QUERY_NODE_PHYSICAL_PLAN_HASH_AGG,
  QUERY_NODE_PHYSICAL_PLAN_EXCHANGE,
  QUERY_NODE_PHYSICAL_PLAN_MERGE,
  QUERY_NODE_PHYSICAL_PLAN_SORT,
  QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT,
  QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL,
  QUERY_NODE_PHYSICAL_PLAN_FILL,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE,
  QUERY_NODE_PHYSICAL_PLAN_PARTITION,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION,
  QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC,
  QUERY_NODE_PHYSICAL_PLAN_DISPATCH,
  QUERY_NODE_PHYSICAL_PLAN_INSERT,
  QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT,
  QUERY_NODE_PHYSICAL_PLAN_DELETE,
  QUERY_NODE_PHYSICAL_SUBPLAN,
  QUERY_NODE_PHYSICAL_PLAN,
  QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT,
  QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN,
  QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE,
  QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL,
  QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT,
  QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL
};

#define QPT_PHYSIC_NODE_NUM() (sizeof(QPT_PHYSIC_NODE_LIST)/sizeof(QPT_PHYSIC_NODE_LIST[0]))
#define QPT_RAND_BOOL_V ((taosRand() % 2) ? true : false)
#define QPT_RAND_ORDER_V (QPT_RAND_BOOL_V ? ORDER_ASC : ORDER_DESC)
#define QPT_RAND_INT_V (taosRand() * (QPT_RAND_BOOL_V ? 1 : -1))
#define QPT_LOW_PROB() ((taosRand() % 11) == 0)
#define QPT_MID_PROB() ((taosRand() % 11) <= 1)
#define QPT_HIGH_PROB() ((taosRand() % 11) <= 7)

#define QPT_CORRECT_HIGH_PROB() (qptCtx.param.correctExpected || QPT_HIGH_PROB())
#define QPT_NCORRECT_LOW_PROB() (!qptCtx.param.correctExpected && QPT_LOW_PROB())

typedef struct {
  ENodeType type;
  void*     param;
} SQPTNodeParam;

typedef struct {
  bool           singlePhysiNode;
  uint64_t       queryId;
  uint64_t       taskId;
  int32_t        subplanMaxLevel;
  int32_t        subplanType[QPT_MAX_SUBPLAN_LEVEL];
  int32_t        physiNodeParamNum;
  SQPTNodeParam* physicNodeParam;
} SQPTPlanParam;

typedef struct {
  uint8_t precision;
  char    dbName[TSDB_DB_NAME_LEN];
} SQPTDbParam;


typedef struct {
  int32_t vnodeNum;
  int32_t vgId;
} SQPTVnodeParam;

typedef struct {
  char    name[TSDB_COL_NAME_LEN];
  int32_t type;
  int32_t len;
  int8_t  inUse;
  bool hasIndex;
  bool isPrimTs;
  bool isPk;
  EColumnType colType;
} SQPTCol;

typedef struct {
  int64_t  uid;
  int64_t  suid;
  int8_t   tblType;
  int32_t  colNum;
  int32_t  tagNum;
  int16_t  pkNum;
  char     tblName[TSDB_TABLE_NAME_LEN];
  char     tblAlias[TSDB_TABLE_NAME_LEN];
  SQPTCol* pCol;
  SQPTCol* pTag;
} SQPTTblParam;


typedef struct {
  bool           correctExpected;
  SQPTPlanParam  plan;
  SQPTDbParam    db;
  SQPTVnodeParam vnode;
  SQPTTblParam   tbl;
} SQPTParam;


typedef struct {
  SPhysiNode*        pCurr;
  int32_t            childrenNum;
  SPhysiNode*        pChild;      // current child
  SPhysiNode*        pLeftChild;
  SPhysiNode*        pRightChild;
  EOrder             currTsOrder;
} SQPTBuildPlanCtx;

typedef struct {
  int32_t nodeLevel;
  bool    onlyTag;
  int16_t nextBlockId;
  SDataBlockDescNode* pInputDataBlockDesc;
} SQPTMakeNodeCtx;

typedef struct {
  int32_t code;
} SQPTExecResult;

typedef struct {
  int32_t          loopIdx;
  SQPTParam        param;
  SQPTBuildPlanCtx buildCtx;
  SQPTMakeNodeCtx  makeCtx;
  SQPTExecResult   result;
  int64_t          startTsUs;
} SQPTCtx;

typedef struct {
  bool printTestInfo;
  bool printInputRow;
  bool printResRow;
  bool logHistory;
  bool noKeepResRows;
} SQPTCtrl;


SQPTCtx qptCtx = {0};
SQPTCtrl qptCtrl = {1, 0, 0, 0, 0};
bool qptErrorRerun = false;
bool qptInRerun = false;

SNode* qptMakeExprNode(SNode** ppNode);


void qptPrintBeginInfo(char* caseName) {
  if (!qptCtrl.printTestInfo) {
    return;
  }

  printf("\n%dth TEST [%s] START\n", qptCtx.loopIdx, caseName);

/*
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
*/  
}

void qptPrintEndInfo(char* caseName) {
  if (!qptCtrl.printTestInfo) {
    return;
  }

  printf("\n\t%dth TEST [%s] END, result - %s%s\n", qptCtx.loopIdx, caseName, 
    (0 == qptCtx.result.code) ? "succeed" : "failed with error:",
    (0 == qptCtx.result.code) ? "" : tstrerror(qptCtx.result.code));
}

void qptPrintStatInfo(char* caseName) {

}


bool qptGetDynamicOp() {
  if (QPT_NCORRECT_LOW_PROB()) {
    return QPT_RAND_BOOL_V;
  }

  if (qptCtx.buildCtx.pChild) {
    return qptCtx.buildCtx.pChild->dynamicOp;
  }

  return QPT_RAND_BOOL_V;
}

EOrder qptGetCurrTsOrder() {
  return QPT_CORRECT_HIGH_PROB() ? qptCtx.buildCtx.currTsOrder : QPT_RAND_ORDER_V;
}

void qptGetRandValue(int16_t* pType, int32_t* pLen, void** ppVal) {
  if (*pType < 0 || QPT_NCORRECT_LOW_PROB()) {
    int32_t typeMax = TSDB_DATA_TYPE_MAX;
    if (QPT_NCORRECT_LOW_PROB()) {
      typeMax++;
    }
    
    *pType = taosRand() % typeMax;
  }
  
  switch (*pType) {
    case TSDB_DATA_TYPE_NULL:
      *pLen = QPT_CORRECT_HIGH_PROB() ? 0 : taosRand();
      if (ppVal) {
        *ppVal = NULL;
      }
      break;
    case TSDB_DATA_TYPE_BOOL:
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(bool*)*ppVal = QPT_RAND_BOOL_V;
      }
      break;
    case TSDB_DATA_TYPE_TINYINT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(int8_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_SMALLINT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(int16_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_INT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(int32_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_BIGINT: 
    case TSDB_DATA_TYPE_TIMESTAMP: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(int64_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_FLOAT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(float*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_DOUBLE: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(double*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: 
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      *pLen = taosRand() % QPT_MAX_STRING_LEN;
      if (ppVal) {
        *ppVal = taosMemoryCalloc(1, *pLen + VARSTR_HEADER_SIZE);
        assert(*ppVal);
        varDataSetLen(*ppVal, *pLen);
        memset((char*)*ppVal + VARSTR_HEADER_SIZE, 'A' + taosRand() % 26, *pLen);
      }
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      *pLen = taosRand() % QPT_MAX_STRING_LEN;
      if (ppVal) {
        char* pTmp = (char*)taosMemoryCalloc(1, *pLen + 1);
        assert(pTmp);
        memset(pTmp, 'A' + taosRand() % 26, *pLen);
        *ppVal = taosMemoryCalloc(1, *pLen * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
        assert(*ppVal);
        assert(taosMbsToUcs4(pTmp, *pLen, (TdUcs4 *)varDataVal(*ppVal), *pLen * TSDB_NCHAR_SIZE, NULL));
        *pLen *= TSDB_NCHAR_SIZE;
        varDataSetLen(*ppVal, *pLen);
        taosMemoryFree(pTmp);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(uint8_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_USMALLINT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(uint16_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_UINT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(uint32_t*)*ppVal = taosRand();
      }
      break;
    case TSDB_DATA_TYPE_UBIGINT: 
      *pLen = QPT_CORRECT_HIGH_PROB() ? tDataTypes[*pType].bytes : taosRand();
      if (ppVal) {
        *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
        assert(*ppVal);
        *(uint64_t*)*ppVal = taosRand();
      }
      break;
    default:
      *pLen = taosRand() % QPT_MAX_STRING_LEN;
      if (ppVal) {
        *ppVal = taosMemoryCalloc(1, *pLen);
        assert(*ppVal);
        memset((char*)*ppVal, 'a' + taosRand() % 26, *pLen);
      }
      break;
  }
}

void qptFreeRandValue(int32_t* pType, void* pVal) {
  switch (*pType) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: 
    case TSDB_DATA_TYPE_SMALLINT: 
    case TSDB_DATA_TYPE_INT: 
    case TSDB_DATA_TYPE_BIGINT: 
    case TSDB_DATA_TYPE_FLOAT: 
    case TSDB_DATA_TYPE_DOUBLE: 
    case TSDB_DATA_TYPE_TIMESTAMP: 
    case TSDB_DATA_TYPE_UTINYINT: 
    case TSDB_DATA_TYPE_USMALLINT: 
    case TSDB_DATA_TYPE_UINT: 
    case TSDB_DATA_TYPE_UBIGINT: 
      taosMemoryFree(pVal);
      break;
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: 
    case TSDB_DATA_TYPE_NCHAR: 
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      break;
    default:
      assert(0);
      break;
  }
}

void qptGetRandRealTableType(int8_t* tableType) {
  while (true) {
    int8_t tType = taosRand() % TSDB_TABLE_MAX;
    switch (tType) {
      case TSDB_SUPER_TABLE:
      case TSDB_CHILD_TABLE:
      case TSDB_NORMAL_TABLE:
      case TSDB_SYSTEM_TABLE:
        *tableType = tType;
        return;
      default:
        break;
    }
  }
}

int32_t qptGetInputSlotId(SDataBlockDescNode* pInput) {
  if (pInput && pInput->pSlots && pInput->pSlots->length > 0 && QPT_CORRECT_HIGH_PROB()) {
    return taosRand() % pInput->pSlots->length;
  }
  
  return taosRand();
}


void qptNodesCalloc(int32_t num, int32_t size, void** pOut) {
  void* p = taosMemoryCalloc(num, size);
  assert(p);
  *(char*)p = 0;
  *pOut = (char*)p + 1;
}

int32_t qptNodesListAppend(SNodeList* pList, SNode* pNode) {
  SListCell* p = NULL;
  qptNodesCalloc(1, sizeof(SListCell), (void**)&p);

  p->pNode = pNode;
  if (NULL == pList->pHead) {
    pList->pHead = p;
  }
  if (NULL != pList->pTail) {
    pList->pTail->pNext = p;
  }
  p->pPrev = pList->pTail;
  pList->pTail = p;
  ++(pList->length);
  return TSDB_CODE_SUCCESS;
}


int32_t qptNodesListStrictAppend(SNodeList* pList, SNode* pNode) {
  int32_t code = qptNodesListAppend(pList, pNode);
  if (TSDB_CODE_SUCCESS != code) {
    nodesDestroyNode(pNode);
  }
  return code;
}

int32_t qptNodesListMakeStrictAppend(SNodeList** pList, SNode* pNode) {
  if (NULL == *pList) {
    int32_t code = nodesMakeList(pList);
    if (NULL == *pList) {
      return code;
    }
  }
  return qptNodesListStrictAppend(*pList, pNode);
}


SNode* qptMakeLimitNode() {
  SNode* pNode = NULL;
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(&pNode);
  }
  
  assert(0 == nodesMakeNode(QUERY_NODE_LIMIT, &pNode));
  assert(pNode);

  SLimitNode* pLimit = (SLimitNode*)pNode;

  if (!qptCtx.param.correctExpected) {
    if (taosRand() % 2) {
      pLimit->limit = taosRand() * ((taosRand() % 2) ? 1 : -1);
    }
    if (taosRand() % 2) {
      pLimit->offset = taosRand() * ((taosRand() % 2) ? 1 : -1);
    }
  } else {
    pLimit->limit = taosRand();
    if (taosRand() % 2) {
      pLimit->offset = taosRand();
    }
  }

  return pNode;
}


SNode* qptMakeWindowOffsetNode(SNode** ppNode) {
  if (QPT_RAND_BOOL_V) {
    return qptMakeRandNode(ppNode);
  }

  SNode* pNode = NULL;
  SWindowOffsetNode* pWinOffset = NULL;

  assert(0 == nodesMakeNode(QUERY_NODE_WINDOW_OFFSET, &pNode));
  assert(pNode);

  SWindowOffsetNode* pWinOffset = (SWindowOffsetNode*)pNode;  
  qptMakeValueNode(TSDB_DATA_TYPE_BIGINT, &pWinOffset->pStartOffset);
  qptMakeValueNode(TSDB_DATA_TYPE_BIGINT, &pWinOffset->pEndOffset);

  return pNode;
}


SNode* qptMakeColumnNodeFromTable(int32_t colIdx, EColumnType colType, SScanPhysiNode* pScanPhysiNode) {
  if (colIdx < 0) {
    return NULL;
  }
  
  SColumnNode* pCol = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  assert(pCol);

  if (QPT_CORRECT_HIGH_PROB()) {
    pCol->node.resType.type = qptCtx.param.tbl.pCol[colIdx].type;
    pCol->node.resType.bytes = qptCtx.param.tbl.pCol[colIdx].len;

    pCol->tableId = qptCtx.param.tbl.uid;  
    pCol->tableType = qptCtx.param.tbl.tblType;  
    pCol->colId = colIdx;
    pCol->projIdx = colIdx;
    pCol->colType = qptCtx.param.tbl.pCol[colIdx].colType;
    pCol->hasIndex = qptCtx.param.tbl.pCol[colIdx].hasIndex;
    pCol->isPrimTs = qptCtx.param.tbl.pCol[colIdx].isPrimTs;
    strcpy(pCol->dbName, qptCtx.param.db.dbName);
    strcpy(pCol->tableName, qptCtx.param.tbl.tblName);
    strcpy(pCol->tableAlias, qptCtx.param.tbl.tblAlias);
    strcpy(pCol->colName, qptCtx.param.tbl.pCol[colIdx].name);
    pCol->dataBlockId = pScanPhysiNode->node.pOutputDataBlockDesc ? pScanPhysiNode->node.pOutputDataBlockDesc->dataBlockId : taosRand();
    pCol->slotId = colIdx;
    pCol->numOfPKs = qptCtx.param.tbl.pkNum;
    pCol->tableHasPk = qptCtx.param.tbl.pkNum > 0;
    pCol->isPk = qptCtx.param.tbl.pCol[colIdx].isPk;
    pCol->projRefIdx = 0;
    pCol->resIdx = 0;
  } else {
    qptGetRandValue(&pCol->node.resType.type, &pCol->node.resType.bytes, NULL);

    pCol->tableId = taosRand();  
    pCol->tableType = taosRand() % TSDB_TABLE_MAX;  
    pCol->colId = QPT_RAND_BOOL_V ? taosRand() : colIdx;
    pCol->projIdx = taosRand();
    pCol->colType = QPT_RAND_BOOL_V ? qptCtx.param.tbl.pCol[colIdx].colType : (EColumnType)(taosRand() % (COLUMN_TYPE_GROUP_KEY + 1));
    pCol->hasIndex = QPT_RAND_BOOL_V;
    pCol->isPrimTs = QPT_RAND_BOOL_V;
    if (QPT_RAND_BOOL_V) {
      pCol->dbName[0] = 0;
    } else {
      strcpy(pCol->dbName, qptCtx.param.db.dbName);
    }
    if (QPT_RAND_BOOL_V) {
      pCol->tableName[0] = 0;
    } else {
      strcpy(pCol->tableName, qptCtx.param.tbl.tblName);
    }
    if (QPT_RAND_BOOL_V) {
      pCol->tableAlias[0] = 0;
    } else {
      strcpy(pCol->tableAlias, qptCtx.param.tbl.tblAlias);
    }
    if (QPT_RAND_BOOL_V) {
      pCol->colName[0] = 0;
    } else {
      strcpy(pCol->colName, qptCtx.param.tbl.pCol[colIdx].name);
    }
    
    pCol->dataBlockId = (QPT_RAND_BOOL_V && pScanPhysiNode->node.pOutputDataBlockDesc) ? pScanPhysiNode->node.pOutputDataBlockDesc->dataBlockId : taosRand();
    pCol->slotId = QPT_RAND_BOOL_V ? taosRand() : colIdx;
    pCol->numOfPKs = QPT_RAND_BOOL_V ? taosRand() : qptCtx.param.tbl.pkNum;
    pCol->tableHasPk = QPT_RAND_BOOL_V ? QPT_RAND_BOOL_V : (qptCtx.param.tbl.pkNum > 0);
    pCol->isPk = QPT_RAND_BOOL_V ? QPT_RAND_BOOL_V : qptCtx.param.tbl.pCol[colIdx].isPk;
    pCol->projRefIdx = taosRand();
    pCol->resIdx = taosRand();
  }

  return (SNode*)pCol;
}


void qptMakeWhenThenNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }
  
  assert(0 == nodesMakeNode(QUERY_NODE_WHEN_THEN, ppNode));
  assert(*ppNode);
  SWhenThenNode* pWhenThen = (SWhenThenNode*)*ppNode;

  qptMakeExprNode(&pWhenThen->pWhen);

  qptMakeExprNode(&pWhenThen->pThen);
}


void qptMakeCaseWhenNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }
  
  assert(0 == nodesMakeNode(QUERY_NODE_CASE_WHEN, ppNode));
  assert(*ppNode);
  
  SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)*ppNode;

  qptCtx.makeCtx.nodeLevel++;  

  if (QPT_RAND_BOOL_V) {
    qptMakeExprNode(&pCaseWhen->pCase);
  }
  
  if (QPT_RAND_BOOL_V) {
    qptMakeExprNode(&pCaseWhen->pElse);
  }
  
  int32_t whenNum = taosRand() % QPT_MAX_WHEN_THEN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < whenNum; ++i) {
    SNode* pNode = NULL;
    qptMakeWhenThenNode(&pNode);
    qptNodesListMakeStrictAppend(&pCaseWhen->pWhenThenList, pNode);
  }
}


void qptMakeOperatorNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  EOperatorType opType = OPERATOR_ARRAY[taosRand() % (sizeof(OPERATOR_ARRAY)/sizeof(OPERATOR_ARRAY[0]))];
  assert(0 == nodesMakeNode(QUERY_NODE_OPERATOR, ppNode));
  
  SOperatorNode* pOp = (SOperatorNode*)*ppNode;
  pOp->opType = QPT_CORRECT_HIGH_PROB() ? opType : (EOperatorType)(opType + 1);

  qptCtx.makeCtx.nodeLevel++;  
  
  switch (pOp->opType) {
    case OP_TYPE_ADD:
    case OP_TYPE_SUB:
    case OP_TYPE_MULTI:
    case OP_TYPE_DIV:
    case OP_TYPE_REM:
    case OP_TYPE_BIT_AND:
    case OP_TYPE_BIT_OR:
    case OP_TYPE_GREATER_THAN:
    case OP_TYPE_GREATER_EQUAL:
    case OP_TYPE_LOWER_THAN:
    case OP_TYPE_LOWER_EQUAL:
    case OP_TYPE_EQUAL:
    case OP_TYPE_NOT_EQUAL:
    case OP_TYPE_LIKE:
    case OP_TYPE_NOT_LIKE:
    case OP_TYPE_MATCH:
    case OP_TYPE_NMATCH:
    case OP_TYPE_IN:
    case OP_TYPE_NOT_IN:
    case OP_TYPE_JSON_GET_VALUE:
    case OP_TYPE_JSON_CONTAINS:
    case OP_TYPE_ASSIGN:
      if (QPT_CORRECT_HIGH_PROB()) {
        qptMakeExprNode(&pOp->pLeft);
        qptMakeExprNode(&pOp->pRight);
      } else {
        if (QPT_RAND_BOOL_V) {
          qptMakeExprNode(&pOp->pLeft);
        }
        if (QPT_RAND_BOOL_V) {
          qptMakeExprNode(&pOp->pRight);
        }
      }
      break;

    case OP_TYPE_IS_NULL:
    case OP_TYPE_IS_NOT_NULL:
    case OP_TYPE_IS_TRUE:
    case OP_TYPE_IS_FALSE:
    case OP_TYPE_IS_UNKNOWN:
    case OP_TYPE_IS_NOT_TRUE:
    case OP_TYPE_IS_NOT_FALSE:
    case OP_TYPE_IS_NOT_UNKNOWN:
    case OP_TYPE_MINUS:
      if (QPT_CORRECT_HIGH_PROB()) {
        qptMakeExprNode(&pOp->pLeft);
      } else {
        if (QPT_RAND_BOOL_V) {
          qptMakeExprNode(&pOp->pLeft);
        }
        if (QPT_RAND_BOOL_V) {
          qptMakeExprNode(&pOp->pRight);
        }
      }
      break;
    default:
      if (QPT_RAND_BOOL_V) {
        qptMakeExprNode(&pOp->pLeft);
      }
      if (QPT_RAND_BOOL_V) {
        qptMakeExprNode(&pOp->pRight);
      }
      break;
  }
}

void qptMakeColumnNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SColumnNode* pCol = NULL;
  nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  SSlotDescNode* pSlot = NULL;

  if (QPT_CORRECT_HIGH_PROB() && qptCtx.makeCtx.pInputDataBlockDesc && qptCtx.makeCtx.pInputDataBlockDesc->pSlots) {
    SNodeList* pColList = qptCtx.makeCtx.pInputDataBlockDesc->pSlots;
    int32_t colIdx = taosRand() % pColList->length;
    SNode* pNode = nodesListGetNode(pColList, colIdx);
    if (pNode && nodeType(pNode) == QUERY_NODE_SLOT_DESC) {
      pSlot = (SSlotDescNode*)pNode;
      pCol->node.resType = pSlot->dataType;
      pCol->dataBlockId = qptCtx.makeCtx.pInputDataBlockDesc->dataBlockId;
      pCol->slotId = pSlot->slotId;
    }
  } 

  if (NULL == pSlot) {
    qptGetRandValue(&pCol->node.resType.type, &pCol->node.resType.bytes, NULL);
    pCol->dataBlockId = taosRand();
    pCol->slotId = taosRand();
  }

  *ppNode = (SNode*)pCol;
}

void qptNodesSetValueNodeValue(SValueNode* pNode, void* value) {
  switch (pNode->node.resType.type) {
    case TSDB_DATA_TYPE_NULL:
      taosMemoryFree(value);
      break;
    case TSDB_DATA_TYPE_BOOL:
      pNode->datum.b = *(bool*)value;
      taosMemoryFree(value);
      *(bool*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.b : QPT_RAND_BOOL_V;
      break;
    case TSDB_DATA_TYPE_TINYINT:
      pNode->datum.i = *(int8_t*)value;
      taosMemoryFree(value);
      *(int8_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.i : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      pNode->datum.i = *(int16_t*)value;
      taosMemoryFree(value);
      *(int16_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.i : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_INT:
      pNode->datum.i = *(int32_t*)value;
      taosMemoryFree(value);
      *(int32_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.i : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_BIGINT:
      pNode->datum.i = *(int64_t*)value;
      taosMemoryFree(value);
      *(int64_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.i : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      pNode->datum.i = *(int64_t*)value;
      taosMemoryFree(value);
      *(int64_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.i : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      pNode->datum.u = *(int8_t*)value;
      taosMemoryFree(value);
      *(int8_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.u : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      pNode->datum.u = *(int16_t*)value;
      taosMemoryFree(value);
      *(int16_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.u : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_UINT:
      pNode->datum.u = *(int32_t*)value;
      taosMemoryFree(value);
      *(int32_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.u : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      pNode->datum.u = *(uint64_t*)value;
      taosMemoryFree(value);
      *(uint64_t*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.u : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_FLOAT:
      pNode->datum.d = *(float*)value;
      taosMemoryFree(value);
      *(float*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.d : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      pNode->datum.d = *(double*)value;
      taosMemoryFree(value);
      *(double*)&pNode->typeData = qptCtx.param.correctExpected ? pNode->datum.d : QPT_RAND_INT_V;
      break;
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_GEOMETRY:
      if (qptCtx.param.correctExpected || QPT_MID_PROB()) {
        pNode->datum.p = (char*)value;
      } else {
      }
      taosMemoryFree(value);
      pNode->datum.p = NULL;
      break;
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      taosMemoryFree(value);
      pNode->datum.p = NULL;
      break;
    default:
      taosMemoryFree(value);
      break;
  }
}


void qptMakeValueNode(int16_t valType, SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SValueNode* pVal = NULL;
  nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);

  int32_t valBytes;
  void* pValue = NULL;
  qptGetRandValue(&valType, &valBytes, &pValue);
  
  pVal->node.resType.type = valType;
  pVal->node.resType.bytes = valBytes;
  
  qptNodesSetValueNodeValue(pVal, pValue);

  *ppNode = (SNode*)pVal;
}

void qptMakeFunctionNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SFunctionNode* pFunc = NULL;
  nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);

  if (qptCtx.param.correctExpected || QPT_HIGH_PROB()) {
    int32_t funcIdx = taosRand() % funcMgtBuiltinsNum;
    char* funcName = fmGetFuncName(funcIdx);
    strcpy(pFunc->functionName, funcName);
    taosMemoryFree(funcName);
    fmGetFuncInfo(pFunc, NULL, 0);
  } else {
    int32_t funcIdx = taosRand();
    if (QPT_RAND_BOOL_V) {
      strcpy(pFunc->functionName, "invalidFuncName");
    } else {
      pFunc->functionName[0] = 0;
    }
    fmGetFuncInfo(pFunc, NULL, 0);
  }
  
  qptCtx.makeCtx.nodeLevel++;  

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  } else {
    int32_t paramNum = taosRand() % QPT_MAX_FUNC_PARAM;
    for (int32_t i = 0; i < paramNum; ++i) {
      SNode* pNode = NULL;
      qptMakeExprNode(&pNode);
      qptNodesListMakeStrictAppend(&pFunc->pParameterList, pNode);
    }
  }

  *ppNode = (SNode*)pFunc;
}




void qptMakeLogicCondNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SLogicConditionNode* pLogic = NULL;
  nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogic);

  if (QPT_CORRECT_HIGH_PROB()) {
    pLogic->condType = (taosRand() % 3) ? ((taosRand() % 2) ? LOGIC_COND_TYPE_AND : LOGIC_COND_TYPE_OR) : LOGIC_COND_TYPE_NOT;
  } else {
    pLogic->condType = (ELogicConditionType)taosRand();
  }
  
  qptCtx.makeCtx.nodeLevel++;  
  
  int32_t paramNum = QPT_CORRECT_HIGH_PROB() ? (taosRand() % QPT_MAX_LOGIC_PARAM + 1) : (taosRand() % QPT_MAX_LOGIC_PARAM);
  for (int32_t i = 0; i < paramNum; ++i) {
    SNode* pNode = NULL;
    qptMakeExprNode(&pNode);
    qptNodesListMakeStrictAppend(&pLogic->pParameterList, pNode);
  }

  *ppNode = (SNode*)pLogic;
}

void qptMakeNodeListNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SNodeListNode* pList = NULL;
  nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&pList);

  qptCtx.makeCtx.nodeLevel++;  
  
  int32_t nodeNum = QPT_CORRECT_HIGH_PROB() ? (taosRand() % QPT_MAX_NODE_LIST_NUM + 1) : (taosRand() % QPT_MAX_NODE_LIST_NUM);
  for (int32_t i = 0; i < nodeNum; ++i) {
    SNode* pNode = NULL;
    qptMakeExprNode(&pNode);
    qptNodesListMakeStrictAppend(&pList->pNodeList, pNode);
  }

  *ppNode = (SNode*)pList; 
}

void qptMakeTempTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  STempTableNode* pTemp = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&pTemp));

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  }

  *ppNode = (SNode*)pTemp;
}

void qptMakeJoinTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SJoinTableNode* pJoin = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_JOIN_TABLE, (SNode**)&pJoin));

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  }

  *ppNode = (SNode*)pJoin;
}

void qptMakeRealTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  SRealTableNode* pReal = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pReal));

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  }

  *ppNode = (SNode*)pReal;
}



void qptMakeNonRealTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(ppNode);
  }

  if (QPT_CORRECT_HIGH_PROB()) {
    if (QPT_RAND_BOOL_V) {
      qptMakeTempTableNode(ppNode);
    } else {
      qptMakeJoinTableNode(ppNode);
    }
  } else {
    qptMakeRealTableNode(ppNode);
  }
}

SNode* qptMakeRandNode(SNode** ppNode) {
  SNode* pNode = NULL;
  nodesMakeNode((ENodeType)taosRand(), ppNode ? ppNode : &pNode);
  return ppNode ? *ppNode : pNode;
}

SNode* qptMakeExprNode(SNode** ppNode) {
  SNode* pNode = NULL;
  if (NULL == ppNode) {
    ppNode = &pNode;
  }

  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }
  
  int32_t nodeTypeMaxValue = 9;
  if (qptCtx.makeCtx.nodeLevel >= QPT_MAX_NODE_LEVEL) {
    nodeTypeMaxValue = 2;
  }

  switch (taosRand() % nodeTypeMaxValue) {
    case 0:
      qptMakeColumnNode(ppNode);
      break;
    case 1:
      qptMakeValueNode(-1, ppNode);
      break;
    case 2:
      qptMakeFunctionNode(ppNode);
      break;
    case 3:
      qptMakeLogicCondNode(ppNode);
      break;
    case 4:
      qptMakeNodeListNode(ppNode);
      break;
    case 5:
      qptMakeOperatorNode(ppNode);
      break;
    case 6:
      qptMakeNonRealTableNode(ppNode);
      break;
    case 7:
      qptMakeCaseWhenNode(ppNode);
      break;
    case 8:
      qptMakeWhenThenNode(ppNode);
      break;
    default:
      assert(0);
      break;
  }

  return *ppNode;
}


void qptResetMakeNodeCtx(SDataBlockDescNode* pInput, bool onlyTag) {
  SQPTMakeNodeCtx* pCtx = &qptCtx.makeCtx;

  pCtx->nodeLevel = 1;
  pCtx->onlyTag = onlyTag;
  pCtx->pInputDataBlockDesc = pInput;
}

SNode* qptMakeConditionNode(bool onlyTag) {
  SNode* pNode = NULL;
  qptResetMakeNodeCtx(qptCtx.buildCtx.pCurr->pOutputDataBlockDesc, onlyTag);
  qptMakeExprNode(&pNode);
  
  return pNode;
}

SNode* qptMakeDataBlockDescNode() {
  if (QPT_NCORRECT_LOW_PROB()) {
    return NULL;
  }

  SDataBlockDescNode* pDesc = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, (SNode**)&pDesc));
  
  pDesc->dataBlockId = qptCtx.param.correctExpected ? qptCtx.makeCtx.nextBlockId++ : QPT_RAND_INT_V;
  pDesc->precision = qptCtx.param.correctExpected ? qptCtx.param.db.precision : QPT_RAND_INT_V;

  return (SNode*)pDesc;
}

SNode* qptMakeSlotDescNode(const char* pName, const SNode* pNode, int16_t slotId, bool output, bool reserve) {
  SSlotDescNode* pSlot = NULL;
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode((SNode**)&pSlot);
  }

  assert(0 == nodesMakeNode(QUERY_NODE_SLOT_DESC, (SNode**)&pSlot));
  
  QPT_RAND_BOOL_V ? (pSlot->name[0] = 0) : snprintf(pSlot->name, sizeof(pSlot->name), "%s", pName);
  pSlot->slotId = QPT_CORRECT_HIGH_PROB() ? slotId : taosRand();
  if (QPT_CORRECT_HIGH_PROB()) {
    pSlot->dataType = ((SExprNode*)pNode)->resType;
  } else {
    qptGetRandValue(&pSlot->dataType.type, &pSlot->dataType.bytes, NULL);
  }
  
  pSlot->reserve = reserve;
  pSlot->output = output;
  return (SNode*)pSlot;
}

void qptMakeTargetNode(SNode* pNode, int16_t dataBlockId, int16_t slotId, SNode** pOutput) {
  if (QPT_NCORRECT_LOW_PROB) {
    return qptMakeRandNode(pOutput);
  }

  STargetNode* pTarget = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget));

  pTarget->dataBlockId = QPT_CORRECT_HIGH_PROB() ? dataBlockId : taosRand();
  pTarget->slotId = QPT_CORRECT_HIGH_PROB() ? slotId : taosRand();
  pTarget->pExpr = QPT_CORRECT_HIGH_PROB() ? pNode : qptMakeRandNode(NULL);

  *pOutput = (SNode*)pTarget;
}

SPhysiNode* qptCreatePhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = NULL;
  assert(0 == nodesMakeNode((ENodeType)nodeType, (SNode**)&pPhysiNode));

  qptCtx.buildCtx.pCurr = pPhysiNode;

  pPhysiNode->pLimit = qptMakeLimitNode();
  pPhysiNode->pSlimit = qptMakeLimitNode();
  pPhysiNode->dynamicOp = qptGetDynamicOp();
  pPhysiNode->inputTsOrder = qptGetCurrTsOrder();

  pPhysiNode->pOutputDataBlockDesc = (SDataBlockDescNode*)qptMakeDataBlockDescNode();

  return pPhysiNode;
}

void qptPostCreatePhysiNode(SPhysiNode* pPhysiNode) {
  pPhysiNode->outputTsOrder = qptGetCurrTsOrder();

  if (QPT_RAND_BOOL_V) {
    pPhysiNode->pConditions = qptMakeConditionNode(false);
  }
  
}

void qptMarkTableInUseCols(int32_t colNum, int32_t totalColNum, SQPTCol* pCol) {
  if (colNum == totalColNum) {
    for (int32_t i = 0; i < colNum; ++i) {
      pCol[i].inUse = 1;
    }
    return;
  }
  
  int32_t colInUse = 0;
  do {
    int32_t colIdx = taosRand() % totalColNum;
    if (pCol[colIdx].inUse) {
      continue;
    }

    pCol[colIdx].inUse = 1;
    colInUse++;
  } while (colInUse < colNum);
}


void qptCreateTableScanColsImpl(       SScanPhysiNode* pScanPhysiNode, SNodeList** ppCols, int32_t totalColNum, SQPTCol* pCol) {
  int32_t colNum = qptCtx.param.correctExpected ? (taosRand() % totalColNum + 1) : (taosRand() % QPT_MAX_COLUMN_NUM + 1);
  int32_t colAdded = 0;
  
  if (qptCtx.param.correctExpected) {
    qptMarkTableInUseCols(colNum, totalColNum, pCol);
    
    for (int32_t i = 0; i < totalColNum && colAdded < colNum; ++i) {
      if (0 == pCol[i].inUse) {
        continue;
      }
      
      assert(0 == qptNodesListMakeStrictAppend(ppCols, qptMakeColumnNodeFromTable(i, pCol[0].colType, pScanPhysiNode)));
    }

    return;
  }
  
  for (int32_t i = 0; i < colNum; ++i) {
    int32_t colIdx = taosRand();
    colIdx = (colIdx >= totalColNum) ? -1 : colIdx;
    
    assert(0 == qptNodesListMakeStrictAppend(ppCols, qptMakeColumnNodeFromTable(colIdx, pCol[0].colType, pScanPhysiNode)));
  }
}


void qptCreateTableScanCols(       SScanPhysiNode* pScanPhysiNode) {
  qptCreateTableScanColsImpl(pScanPhysiNode, &pScanPhysiNode->pScanCols, qptCtx.param.tbl.colNum, qptCtx.param.tbl.pCol);
}

void qptCreateTableScanPseudoCols(       SScanPhysiNode* pScanPhysiNode) {
  qptCreateTableScanColsImpl(pScanPhysiNode, &pScanPhysiNode->pScanPseudoCols, qptCtx.param.tbl.tagNum, qptCtx.param.tbl.pTag);
}


void qptAddDataBlockSlots(SNodeList* pList, SDataBlockDescNode* pDataBlockDesc) {
  if (NULL == pDataBlockDesc) {
    return;
  }
  
  int16_t   nextSlotId = LIST_LENGTH(pDataBlockDesc->pSlots), slotId = 0;
  SNode*    pNode = NULL;
  bool output = QPT_RAND_BOOL_V;
  
  FOREACH(pNode, pList) {
    if (NULL == pNode) {
      continue;
    }
    
    SNode*      pExpr = QUERY_NODE_ORDER_BY_EXPR == nodeType(pNode) ? ((SOrderByExprNode*)pNode)->pExpr : pNode;
    if (QPT_CORRECT_HIGH_PROB()) {
      SNode* pDesc = qptCtx.param.correctExpected ? qptMakeSlotDescNode(NULL, pExpr, nextSlotId, output, QPT_RAND_BOOL_V) : qptMakeExprNode(NULL);
      assert(0 == qptNodesListMakeStrictAppend(&pDataBlockDesc->pSlots, pDesc));
      pDataBlockDesc->totalRowSize += qptCtx.param.correctExpected ? ((SExprNode*)pExpr)->resType.bytes : taosRand();
      if (output && QPT_RAND_BOOL_V) {
        pDataBlockDesc->outputRowSize += qptCtx.param.correctExpected ? ((SExprNode*)pExpr)->resType.bytes : taosRand();
      }
    }
    
    slotId = nextSlotId;
    ++nextSlotId;

    if (QPT_CORRECT_HIGH_PROB()) {
      SNode* pTarget = NULL;
      qptMakeTargetNode(pNode, pDataBlockDesc->dataBlockId, slotId, &pTarget);
      REPLACE_NODE(pTarget);
    }
  }
}


void qptCreateScanPhysiNodeImpl(         SScanPhysiNode* pScanPhysiNode) {
  qptCreateTableScanCols(pScanPhysiNode);

  qptAddDataBlockSlots(pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc);

  if (taosRand() % 2) {
    qptCreateTableScanPseudoCols(pScanPhysiNode);
  }
  
  qptAddDataBlockSlots(pScanPhysiNode->pScanPseudoCols, pScanPhysiNode->node.pOutputDataBlockDesc);
  
  pScanPhysiNode->uid = qptCtx.param.correctExpected ? qptCtx.param.tbl.uid : taosRand();
  pScanPhysiNode->suid = qptCtx.param.correctExpected ? qptCtx.param.tbl.suid : taosRand();
  pScanPhysiNode->tableType = qptCtx.param.correctExpected ? qptCtx.param.tbl.tblType : taosRand();
  pScanPhysiNode->groupOrderScan = (taosRand() % 2) ? true : false;

  SName tblName = {0};
  toName(1, qptCtx.param.db.dbName, qptCtx.param.tbl.tblName, &tblName);
  if (qptCtx.param.correctExpected || QPT_RAND_BOOL_V) {
    memcpy(&pScanPhysiNode->tableName, &tblName, sizeof(SName));
  } else {
    pScanPhysiNode->tableName.acctId = 0;
    pScanPhysiNode->tableName.dbname[0] = 0;
    pScanPhysiNode->tableName.tname[0] = 0;
  }
  
  qptCtx.buildCtx.currTsOrder = (qptCtx.param.correctExpected) ? qptCtx.buildCtx.currTsOrder : QPT_RAND_ORDER_V;
}



SNode* qptCreateTagScanPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  STagScanPhysiNode* pTagScanNode = (STagScanPhysiNode*)pPhysiNode;
  pTagScanNode->onlyMetaCtbIdx = (taosRand() % 2) ? true : false;

  qptCreateScanPhysiNodeImpl(&pTagScanNode->scan);

  qptPostCreatePhysiNode(pPhysiNode);
  
  return (SNode*)pPhysiNode;
}

void qptMakeExprList(SNodeList** ppList) {
  int32_t exprNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < exprNum; ++i) {
    SNode* pNode = NULL;
    qptResetMakeNodeCtx(qptCtx.buildCtx.pChild ? qptCtx.buildCtx.pChild->pOutputDataBlockDesc : NULL, false);
    qptMakeExprNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

void qptMakeColumnList(SNodeList** ppList) {
  int32_t colNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  qptResetMakeNodeCtx(qptCtx.buildCtx.pChild ? qptCtx.buildCtx.pChild->pOutputDataBlockDesc : NULL, false);
  for (int32_t i = 0; i < colNum; ++i) {
    SNode* pNode = NULL;
    qptMakeColumnNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

void qptMakeTargetList(int16_t datablockId, SNodeList** ppList) {
  int32_t tarNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < tarNum; ++i) {
    SNode* pNode = NULL, *pExpr = NULL;
    qptMakeColumnNode(&pExpr);
    qptMakeTargetNode(pExpr, datablockId, i, &pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

SNode* qptCreateProjectPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SProjectPhysiNode* pProject = (SProjectPhysiNode*)pPhysiNode;

  pProject->mergeDataBlock = QPT_RAND_BOOL_V;
  pProject->ignoreGroupId = QPT_RAND_BOOL_V;
  pProject->inputIgnoreGroup = QPT_RAND_BOOL_V;

  qptMakeExprList(&pProject->pProjections);

  qptAddDataBlockSlots(pProject->pProjections, pProject->node.pOutputDataBlockDesc);

  qptPostCreatePhysiNode(pPhysiNode);

  return (SNode*)pPhysiNode;
}


SNode* qptCreateSortMergeJoinPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SSortMergeJoinPhysiNode* pJoin = (SSortMergeJoinPhysiNode*)pPhysiNode;

  pJoin->joinType = taosRand() % JOIN_TYPE_MAX_VALUE + (QPT_CORRECT_HIGH_PROB() ? 0 : 1);
  pJoin->subType = taosRand() % JOIN_STYPE_MAX_VALUE + (QPT_CORRECT_HIGH_PROB() ? 0 : 1);
  qptMakeWindowOffsetNode(&pJoin->pWindowOffset);
  qptMakeLimitNode(&pJoin->pJLimit);
  pJoin->asofOpType = OPERATOR_ARRAY[taosRand() % (sizeof(OPERATOR_ARRAY)/sizeof(OPERATOR_ARRAY[0]))] + (QPT_CORRECT_HIGH_PROB() ? 0 : 1);

  qptMakeExprNode(&pJoin->leftPrimExpr);
  qptMakeExprNode(&pJoin->rightPrimExpr);
  pJoin->leftPrimSlotId = qptGetInputSlotId(qptCtx->buildCtx.pChild ? qptCtx->buildCtx.pChild->pOutputDataBlockDesc : NULL);
  pJoin->rightPrimSlotId = qptGetInputSlotId(qptCtx->buildCtx.pChild ? qptCtx->buildCtx.pChild->pOutputDataBlockDesc : NULL);
  qptMakeColumnList(&pJoin->pEqLeft);
  qptMakeColumnList(&pJoin->pEqRight);
  qptMakeExprNode(&pJoin->pPrimKeyCond);
  qptMakeExprNode(&pJoin->pColEqCond);
  qptMakeExprNode(&pJoin->pColOnCond);
  qptMakeExprNode(&pJoin->pFullOnCond);
  qptMakeTargetList(pPhysiNode->pOutputDataBlockDesc ? pPhysiNode->pOutputDataBlockDesc->dataBlockId, &pJoin->pTargets);
  for (int32_t i = 0; i < 2; i++) {
    pJoin->inputStat[i].inputRowNum = taosRand();
    pJoin->inputStat[i].inputRowSize = taosRand();
  }

  pJoin->seqWinGroup = QPT_RAND_BOOL_V;
  pJoin->grpJoin = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}

SNode* qptCreatePhysicalPlanNode(int32_t nodeType) {
  switch (nodeType) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return (SNode*)qptCreateTagScanPhysiNode(nodeType);
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
      return (SNode*)qptCreateProjectPhysiNode(nodeType);
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      return (SNode*)qptCreateSortMergeJoinPhysiNode(nodeType);
    case QUERY_NODE_PHYSICAL_PLAN_HASH_AGG:
    case QUERY_NODE_PHYSICAL_PLAN_EXCHANGE:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE:
    case QUERY_NODE_PHYSICAL_PLAN_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL:
    case QUERY_NODE_PHYSICAL_PLAN_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE:
    case QUERY_NODE_PHYSICAL_PLAN_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION:
    case QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC:
    case QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC:
    case QUERY_NODE_PHYSICAL_PLAN_DISPATCH:
    case QUERY_NODE_PHYSICAL_PLAN_INSERT:
    case QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT:
    case QUERY_NODE_PHYSICAL_PLAN_DELETE:
    case QUERY_NODE_PHYSICAL_SUBPLAN:
    case QUERY_NODE_PHYSICAL_PLAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT:
    case QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN:
    case QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE:
    case QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL:
    default:
      assert(0);
  }

  return 0;
}

void qptCreateQueryPlan(SNode** ppPlan) {
  
}


void qptRerunBlockedHere() {
  while (qptInRerun) {
    taosSsleep(1);
  }
}

void qptResetForReRun() {
  qptCtx.param.plan.taskId = 1;
  qptCtx.param.vnode.vgId = 1;
  for (int32_t i = 0; i < qptCtx.param.tbl.colNum; ++i) {
    qptCtx.param.tbl.pCol[i].inUse = 0;
  }
  for (int32_t i = 0; i < qptCtx.param.tbl.tagNum; ++i) {
    qptCtx.param.tbl.pTag[i].inUse = 0;
  }
}

void qptSingleTestDone(bool* contLoop) {
/*
  if (jtRes.succeed) {
    *contLoop = false;
    return;
  }
*/

  if (qptErrorRerun) {
    *contLoop = false;
    return;
  }

  qptInRerun = true;  
}


void qptInitLogFile() {
  const char   *defaultLogFileNamePrefix = "queryPlanTestlog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;
  TAOS_STRCPY(tsLogDir, TD_LOG_DIR_PATH);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, false) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}



void qptInitTest() {
  qptInitLogFile();
}

void qptHandleTestEnd() {

}

void qptRunSingleOpTest(char* caseName) {
  SNode* pNode = NULL;
  SReadHandle readHandle = {0};
  SOperatorInfo* pOperator = NULL;
  SExecTaskInfo* pTaskInfo = NULL;
  SStorageAPI    storageAPI = {0};

  if (qptCtx.loopIdx > 0) {
    qptResetForReRun();
  }

  pNode = (SNode*)qptCreatePhysicalPlanNode(qptCtx.param.plan.subplanType[0]);
  
  qptPrintBeginInfo(caseName);

  doCreateTask(qptCtx.param.plan.queryId, qptCtx.param.plan.taskId++, qptCtx.param.vnode.vgId, OPTR_EXEC_MODEL_BATCH, &storageAPI, &pTaskInfo);

  qptCtx.startTsUs = taosGetTimestampUs();
  //qptCtx.result.code = createTagScanOperatorInfo(&readHandle, (STagScanPhysiNode*)pNode, NULL, NULL, NULL, pTaskInfo, &pOperator);
  //qptCtx.result.code = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pNode, pTaskInfo, &pOperator);

  doDestroyTask(pTaskInfo);
  destroyOperator(pOperator);
  nodesDestroyNode((SNode*)pNode);

  qptPrintEndInfo(caseName);

  qptHandleTestEnd();
}

void qptRunSubplanTest(char* caseName) {
  SNode* pNode = NULL;
  SReadHandle readHandle = {0};
  SOperatorInfo* pOperator = NULL;

  if (qptCtx.loopIdx > 0) {
    qptResetForReRun();
  }

  //pNode = (SNode*)qptCreatePhysicalPlanNode(qptCtx.param.plan.subplanType[0]);
  
  qptPrintBeginInfo(caseName);

  qptCtx.startTsUs = taosGetTimestampUs();
  //qptCtx.result.code = createTagScanOperatorInfo(&readHandle, (STagScanPhysiNode*)pNode, NULL, NULL, NULL, NULL, &pOperator);
  //qptCtx.result.code = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pNode, NULL, &pOperator);

  destroyOperator(pOperator);
  nodesDestroyNode((SNode*)pNode);

  qptPrintEndInfo(caseName);

  qptHandleTestEnd();
}


void qptRunPlanTest(char* caseName) {
  if (qptCtx.param.plan.singlePhysiNode) {
    qptRunSingleOpTest(caseName);
  } else {
    qptRunSubplanTest(caseName);
  }
}

SQPTNodeParam* qptInitNodeParam(int32_t nodeType) {
  return NULL;
}

int32_t qptGetColumnRandLen(int32_t colType) {
  switch (colType) {
    case TSDB_DATA_TYPE_NULL:
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT: 
    case TSDB_DATA_TYPE_SMALLINT: 
    case TSDB_DATA_TYPE_INT: 
    case TSDB_DATA_TYPE_BIGINT: 
    case TSDB_DATA_TYPE_TIMESTAMP: 
    case TSDB_DATA_TYPE_FLOAT: 
    case TSDB_DATA_TYPE_DOUBLE: 
    case TSDB_DATA_TYPE_UTINYINT: 
    case TSDB_DATA_TYPE_USMALLINT: 
    case TSDB_DATA_TYPE_UINT: 
    case TSDB_DATA_TYPE_UBIGINT: 
      return tDataTypes[colType].bytes;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: 
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_NCHAR:
      return taosRand() % TSDB_MAX_BINARY_LEN;
    default:
      assert(0);
      break;
  }
}

void qptInitTableCols(SQPTCol* pCol, int32_t colNum, EColumnType colType) {
  int32_t tbnameIdx = -1;
  if (QPT_RAND_BOOL_V && COLUMN_TYPE_TAG == colType) {
    tbnameIdx = taosRand() % colNum;
  }
  
  for (int32_t i = 0; i < colNum; ++i) {
    if (tbnameIdx >= 0 && i == tbnameIdx) {
      strcpy(pCol[i].name, "tbname");
      pCol[i].type = TSDB_DATA_TYPE_VARCHAR;
      pCol[i].len = qptGetColumnRandLen(pCol[i].type);
      pCol[i].inUse = 0;
      pCol[i].hasIndex = QPT_RAND_BOOL_V;
      pCol[i].isPrimTs = QPT_RAND_BOOL_V;
      pCol[i].isPk = QPT_RAND_BOOL_V;
      pCol[i].colType = COLUMN_TYPE_TBNAME;
      continue;
    }
    
    sprintf(pCol[i].name, "col%d", i);
    pCol[i].type = taosRand() % TSDB_DATA_TYPE_MAX;
    pCol[i].len = qptGetColumnRandLen(pCol[i].type);
    pCol[i].inUse = 0;
    pCol[i].hasIndex = QPT_RAND_BOOL_V;
    pCol[i].isPrimTs = QPT_RAND_BOOL_V;
    pCol[i].isPk = QPT_RAND_BOOL_V;
    pCol[i].colType = colType;
  }
}

void qptInitTestCtx(bool correctExpected, bool singleNode, int32_t nodeType, int32_t paramNum, SQPTNodeParam* nodeParam) {
  qptCtx.param.correctExpected = correctExpected;
  qptCtx.param.plan.singlePhysiNode = singleNode;
  if (singleNode) {
    qptCtx.param.plan.subplanMaxLevel = 1;
    qptCtx.param.plan.subplanType[0] = nodeType;
  } else {
    qptCtx.param.plan.subplanMaxLevel = taosRand() % QPT_MAX_SUBPLAN_LEVEL + 1;
    for (int32_t i = 0; i < qptCtx.param.plan.subplanMaxLevel; ++i) {
      qptCtx.param.plan.subplanType[i] = QPT_PHYSIC_NODE_LIST[taosRand() % QPT_PHYSIC_NODE_NUM()];
    }
  }

  if (paramNum > 0) {
    qptCtx.param.plan.physiNodeParamNum = paramNum;
    qptCtx.param.plan.physicNodeParam = nodeParam;
  }

  qptCtx.param.db.precision = TSDB_TIME_PRECISION_MILLI;
  strcpy(qptCtx.param.db.dbName, "qptdb1");

  qptCtx.param.vnode.vnodeNum = QPT_DEFAULT_VNODE_NUM;

  qptCtx.param.tbl.uid = 100;
  qptCtx.param.tbl.suid = 1;
  qptGetRandRealTableType(&qptCtx.param.tbl.tblType);
  qptCtx.param.tbl.colNum = taosRand() % 4098;
  qptCtx.param.tbl.tagNum = taosRand() % 130;
  qptCtx.param.tbl.pkNum = taosRand() % 2;
  strcpy(qptCtx.param.tbl.tblName, "qpttbl1");
  strcpy(qptCtx.param.tbl.tblName, "tbl1");
  qptCtx.param.tbl.pCol = (SQPTCol*)taosMemoryCalloc(qptCtx.param.tbl.colNum, sizeof(*qptCtx.param.tbl.pCol));
  assert(qptCtx.param.tbl.pCol);
  qptInitTableCols(qptCtx.param.tbl.pCol, qptCtx.param.tbl.colNum, COLUMN_TYPE_COLUMN);

  qptCtx.param.tbl.pTag = (SQPTCol*)taosMemoryCalloc(qptCtx.param.tbl.tagNum, sizeof(*qptCtx.param.tbl.pTag));
  assert(qptCtx.param.tbl.pTag);
  qptInitTableCols(qptCtx.param.tbl.pTag, qptCtx.param.tbl.tagNum, COLUMN_TYPE_TAG);
  
}


}  // namespace

#if 1
#if 0
TEST(randSingleNodeTest, tagScan) {
  char* caseName = "randSingleNodeTest:tagScan";

  qptInitTestCtx(false, true, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, 0, NULL);
  
  for (qptCtx.loopIdx = 0; qptCtx.loopIdx < QPT_MAX_LOOP; ++qptCtx.loopIdx) {
    qptRunPlanTest(caseName);
  }

  qptPrintStatInfo(caseName); 
}
#endif
#if 1
TEST(randSingleNodeTest, projection) {
  char* caseName = "randSingleNodeTest:projection";

  qptInitTestCtx(false, true, QUERY_NODE_PHYSICAL_PLAN_PROJECT, 0, NULL);
  
  for (qptCtx.loopIdx = 0; qptCtx.loopIdx < QPT_MAX_LOOP; ++qptCtx.loopIdx) {
    qptRunPlanTest(caseName);
  }

  qptPrintStatInfo(caseName); 
}
#endif




#if 0
TEST(correctSingleNodeTest, tagScan) {
  char* caseName = "correctSingleNodeTest:tagScan";

  qptInitTestCtx(true, true, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, 0, NULL);
  
  for (qptCtx.loopIdx = 0; qptCtx.loopIdx < QPT_MAX_LOOP; ++qptCtx.loopIdx) {
    qptRunPlanTest(caseName);
  }

  qptPrintStatInfo(caseName); 
}
#endif



#endif


int main(int argc, char** argv) {
  taosSeedRand(taosGetTimestampSec());
  qptInitTest();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}



#pragma GCC diagnosti
