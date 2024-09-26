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

#define QPT_MAX_LOOP          1000
#define QPT_MAX_SUBPLAN_LEVEL 1000
#define QPT_MAX_WHEN_THEN_NUM 10
#define QPT_MAX_NODE_LEVEL    5
#define QPT_MAX_STRING_LEN    1048576
#define QPT_MAX_FUNC_PARAM    5
#define QPT_MAX_LOGIC_PARAM   5
#define QPT_MAX_NODE_LIST_NUM 5

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

typedef struct {
  ENodeType type;
  void*     param;
} SQPTNodeParam;

typedef struct {
  bool           singlePhysiNode;
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
  SPhysiNode*        pChild;
  EOrder             currTsOrder;
  SPhysiPlanContext* pCxt;
} SQPTBuildCtx;

typedef struct {
  int32_t nodeLevel;
  bool    onlyTag;
  SDataBlockDescNode* pInputDataBlockDesc;
} SQPTMakeNodeCtx;


typedef struct {
  int32_t         loopIdx;
  SQPTParam       param;
  SQPTBuildCtx    buildCtx;
  SQPTMakeNodeCtx makeCtx;
  int64_t         startTsUs;
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

void qptPrintBeginInfo(char* caseName) {
  if (!qptCtrl.printTestInfo) {
    return;
  }

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

void qptPrintStatInfo(char* caseName) {

}


bool qptGetDynamicOp() {
  if (!qptCtx->param.correctExpected) {
    return (taosRand() % 2) : true : false;
  }

  if (qptCtx->buildCtx.pChild) {
    return qptCtx->buildCtx.pChild->dynamicOp;
  }

  return (taosRand() % 2) : true : false;
}

EOrder qptGetInputTsOrder() {
  return qptCtx->buildCtx.currTsOrder;
}


SNode* qptMakeLimitNode() {
  SNode* pNode = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_LIMIT, &pNode));
  assert(pNode);

  SLimitNode* pLimit = (SLimitNode*)pNode;

  if (!qptCtx->param->correctExpected) {
    if (taosRand() % 2) {
      pLimit->limit = taosRand() * ((taosRand() % 2) : 1 : -1);
    }
    if (taosRand() % 2) {
      pLimit->offset = taosRand() * ((taosRand() % 2) : 1 : -1);
    }
  } else {
    pLimit->limit = taosRand();
    if (taosRand() % 2) {
      pLimit->offset = taosRand();
    }
  }

  return pLimit;
}

SNode* qptMakeColumnNodeFromTable(int32_t colIdx, EColumnType colType, SScanPhysiNode* pScanPhysiNode) {
  SColumnNode* pCol = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  assert(pCol);

  pCol->node.resType.type = qptCtx->param.tbl.pCol[colIdx].type;
  pCol->node.resType.bytes = qptCtx->param.tbl.pCol[colIdx].len;

  pCol->tableId = qptCtx->param.tbl.uid;  
  pCol->tableType = qptCtx->param.tbl.tblType;  
  pCol->colId = colIdx;
  pCol->projIdx = colIdx;
  pCol->colType = qptCtx->param.tbl.pCol[colIdx].colType;
  pCol->hasIndex = qptCtx->param.tbl.pCol[colIdx].hasIndex;
  pCol->isPrimTs = qptCtx->param.tbl.pCol[colIdx].isPrimTs;
  strcpy(pCol->dbName, qptCtx->param.db.dbName);
  strcpy(pCol->tableName, qptCtx->param.tbl.tblName);
  strcpy(pCol->tableAlias, qptCtx->param.tbl.tblAlias);
  strcpy(pCol->colName, qptCtx->param.tbl.pCol[colIdx].name);
  pCol->dataBlockId = pScanPhysiNode->node.pOutputDataBlockDesc->dataBlockId;
  pCol->slotId = colIdx;
  pCol->numOfPKs = qptCtx->param.tbl.pkNum;
  pCol->tableHasPk = qptCtx->param.tbl.pkNum > 0;
  pCol->isPk = qptCtx->param.tbl.pCol[colIdx].isPk;
  pCol->projRefIdx = 0;
  pCol->resIdx = 0;

  return (SNode*)pCol;
}


void qptMakeWhenThenNode(SNode** ppNode) {
  assert(0 == nodesMakeNode(QUERY_NODE_WHEN_THEN, ppNode));
  assert(*ppNode);
  SWhenThenNode* pWhenThen = (SWhenThenNode*)*ppNode;

  qptMakeExprNode(&pWhenThen->pWhen);
  assert(pWhenThen->pWhen);

  qptMakeExprNode(&pWhenThen->pThen);
  assert(pWhenThen->pThen);
}


void qptMakeCaseWhenNode(SNode** ppNode) {
  assert(0 == nodesMakeNode(QUERY_NODE_CASE_WHEN, ppNode));
  assert(*ppNode);
  SCaseWhenNode* pCaseWhen = (SCaseWhenNode*)*ppNode;

  qptCtx->makeCtx.nodeLevel++;  

  qptMakeExprNode(&pCaseWhen->pCase);
  assert(pCaseWhen->pCase);

  qptMakeExprNode(&pCaseWhen->pElse);
  assert(pCaseWhen->pElse);

  int32_t whenNum = taosRand() % QPT_MAX_WHEN_THEN_NUM + 1;
  for (int32_t i = 0; i < whenNum; ++i) {
    SNode* pNode = NULL;
    qptMakeWhenThenNode(&pNode);
    assert(0 == nodesListMakeStrictAppend(&pCaseWhen->pWhenThenList, pNode));
  }
}


void qptMakeOperatorNode(SNode** ppNode) {
  EOperatorType opType;
  opType = OPERATOR_ARRAY[taosRand() % (sizeof(OPERATOR_ARRAY)/sizeof(OPERATOR_ARRAY[0]))];
  assert(0 == nodesMakeNode(QUERY_NODE_OPERATOR, ppNode));
  SOperatorNode* pOp = (SOperatorNode*)*ppNode;
  pOp->opType = opType;
  switch (opType) {
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
      qptMakeOperatorNode(&pOp->pLeft);
      qptMakeOperatorNode(&pOp->pRight);
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
      qptMakeOperatorNode(&pOp->pLeft);
      break;
    default:
      assert(0);
      break;
  }
}

void qptMakeColumnNode(SNode** ppNode) {
  SNodeList* pColList = qptCtx->makeCtx.pInputDataBlockDesc->pSlots;
  int32_t colIdx = taosRand() % pColList->length;
  SNode* pNode = nodesListGetNode(pColList, colIdx);
  assert(nodeType(pNode) == QUERY_NODE_SLOT_DESC);
  SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
  SColumnNode* pCol = NULL;
  nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
  pCol->node.resType = pSlot->dataType;
  pCol->dataBlockId = qptCtx->makeCtx.pInputDataBlockDesc->dataBlockId;
  pCol->slotId = pSlot->slotId;

  *ppNode = (SNode*)pCol;
}

void qptGetRandValue(int32_t* pType, int32_t* pLen, void** ppVal) {
  *pType = taosRand() % TSDB_DATA_TYPE_MAX;
  switch (*pType) {
    case TSDB_DATA_TYPE_NULL:
      *pLen = 0;
      *ppVal = NULL;
      break;
    case TSDB_DATA_TYPE_BOOL:
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(bool*)*ppVal = (taosRand() % 2) : true : false;
      break;
    case TSDB_DATA_TYPE_TINYINT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(int8_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_SMALLINT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(int16_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_INT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(int32_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_BIGINT: 
    case TSDB_DATA_TYPE_TIMESTAMP: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(int64_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_FLOAT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(float*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_DOUBLE: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(double*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_GEOMETRY: 
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      *pLen = taosRand() % QPT_MAX_STRING_LEN;
      *ppVal = taosMemoryCalloc(1, *pLen + VARSTR_HEADER_SIZE);
      assert(*ppVal);
      varDataSetLen(*ppVal, *pLen);
      memset((char*)*ppVal + VARSTR_HEADER_SIZE, 'A' + taosRand() % 26, *pLen);
      break;
    case TSDB_DATA_TYPE_NCHAR: {
      *pLen = taosRand() % QPT_MAX_STRING_LEN;
      char* pTmp = taosMemoryCalloc(1, *pLen + 1);
      assert(pTmp);
      memset(pTmp, 'A' + taosRand() % 26, *pLen);
      *ppVal = taosMemoryCalloc(1, *pLen * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
      assert(*ppVal);
      assert(taosMbsToUcs4(pTmp, *pLen, (TdUcs4 *)varDataVal(*ppVal), *pLen * TSDB_NCHAR_SIZE, NULL));
      *pLen *= TSDB_NCHAR_SIZE;
      varDataSetLen(*ppVal, *pLen);
      taosMemoryFree(pTmp);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(uint8_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_USMALLINT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(uint16_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_UINT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(uint32_t*)*ppVal = taosRand();
      break;
    case TSDB_DATA_TYPE_UBIGINT: 
      *pLen = tDataTypes[*pType].bytes;
      *ppVal = taosMemoryMalloc(tDataTypes[*pType].bytes);
      assert(*ppVal);
      *(uint64_t*)*ppVal = taosRand();
      break;
    default:
      assert(0);
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

void qptMakeValueNode(SNode** ppNode) {
  SValueNode* pVal = NULL;
  nodesMakeNode(QUERY_NODE_VALUE, (SNode**)&pVal);

  int32_t valType, valBytes;
  void* pValue = NULL;
  qptGetRandValue(&valType, &valBytes, &pValue);
  
  pVal->node.resType.type = valType;
  pVal->node.resType.bytes = valBytes;
  nodesSetValueNodeValue(pVal, pValue);

  *ppNode = (SNode*)pVal;
}

void qptMakeFunctionNode(SNode** ppNode) {
  SFunctionNode* pFunc = NULL;
  nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);

  int32_t funcIdx = taosRand() % funcMgtBuiltinsNum;
  strcpy(pFunc->functionName, funcMgtBuiltins[funcIdx].name);
  
  fmGetFuncInfo(pFunc, NULL, 0);

  qptCtx->makeCtx.nodeLevel++;  
  int32_t paramNum = taosRand() % QPT_MAX_FUNC_PARAM + 1;
  for (int32_t i = 0; i < paramNum; ++i) {
    SNode* pNode = NULL;
    qptMakeExprNode(&pNode);
    assert(0 == nodesListMakeStrictAppend(&pFunc->pParameterList, pNode));
  }

  *ppNode = (SNode*)pFunc;
}

void qptMakeLogicCondNode(SNode** ppNode) {
  SLogicConditionNode* pLogic = NULL;
  nodesMakeNode(QUERY_NODE_LOGIC_CONDITION, (SNode**)&pLogic);

  pLogic->condType = (taosRand() % 3) ? ((taosRand() % 2) ? LOGIC_COND_TYPE_AND : LOGIC_COND_TYPE_OR) : LOGIC_COND_TYPE_NOT;

  qptCtx->makeCtx.nodeLevel++;  
  int32_t paramNum = taosRand() % QPT_MAX_LOGIC_PARAM + 1;
  for (int32_t i = 0; i < paramNum; ++i) {
    SNode* pNode = NULL;
    qptMakeExprNode(&pNode);
    assert(0 == nodesListMakeStrictAppend(&pLogic->pParameterList, pNode));
  }

  *ppNode = (SNode*)pLogic;
}

void qptMakeNodeListNode(SNode** ppNode) {
  SNodeListNode* pList = NULL;
  nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&pList);

  qptCtx->makeCtx.nodeLevel++;  
  int32_t nodeNum = taosRand() % QPT_MAX_NODE_LIST_NUM + 1;
  for (int32_t i = 0; i < nodeNum; ++i) {
    SNode* pNode = NULL;
    qptMakeExprNode(&pNode);
    assert(0 == nodesListMakeStrictAppend(&pList->pNodeList, pNode));
  }

  *ppNode = (SNode*)pList; 
}

void qptMakeTempTableNode(SNode** ppNode) {
  STempTableNode* pTemp = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&pTemp));

  *ppNode = (SNode*)pTemp;
}

void qptMakeJoinTableNode(SNode** ppNode) {
  SJoinTableNode* pJoin = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_JOIN_TABLE, (SNode**)&pJoin));

  *ppNode = (SNode*)pJoin;
}


void qptMakeTableNode(SNode** ppNode) {
  if (taosRand() % 2) {
    qptMakeTempTableNode(ppNode);
  } else {
    qptMakeJoinTableNode(ppNode);
  }
}


void qptMakeExprNode(SNode** ppNode) {
  int32_t nodeTypeMaxValue = 9;
  if (qptCtx->makeCtx.nodeLevel >= QPT_MAX_NODE_LEVEL) {
    nodeTypeMaxValue = 2;
  }
  
  switch (taosRand() % 10) {
    case 0:
      qptMakeColumnNode(ppNode);
      break;
    case 1:
      qptMakeValueNode(ppNode);
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
      qptMakeTableNode(ppNode);
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
}

void qptResetMakeNodeCtx(SDataBlockDescNode* pInput, bool onlyTag) {
  SQPTMakeNodeCtx* pCtx = &qptCtx->makeCtx;

  pCtx->nodeLevel = 1;
  pCtx->onlyTag = onlyTag;
  pCtx->pInputDataBlockDesc = pInput;
}

SNode* qptMakeConditionNode(bool onlyTag) {
  SNode* pNode = NULL;
  qptResetMakeNodeCtx(qptCtx->buildCtx.pCurr->pOutputDataBlockDesc, onlyTag);
  qptMakeExprNode(&pNode);
  return pNode;
}


SPhysiNode* qptCreatePhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = NULL;
  int32_t code = nodesMakeNode(nodeType, (SNode**)&pPhysiNode);
  if (NULL == pPhysiNode) {
    assert(0);
  }

  qptCtx.buildCtx.pCurr = pPhysiNode;

  pPhysiNode->pLimit = qptMakeLimitNode();
  pPhysiNode->pSlimit = qptMakeLimitNode();
  pPhysiNode->dynamicOp = qptGetDynamicOp();
  pPhysiNode->inputTsOrder = qptGetInputTsOrder();

  assert(0 == createDataBlockDesc(qptCtx->buildCtx.pCxt, NULL, &pPhysiNode->pOutputDataBlockDesc));
  pPhysiNode->pOutputDataBlockDesc->precision = qptCtx->param.db.precision;

  return pPhysiNode;
}

void qptPostCreatePhysiNode(SPhysiNode* pPhysiNode) {
  pPhysiNode->outputTsOrder = qptGetInputTsOrder();

  if (taosRand() % 2) {
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
  int32_t colNum = qptCtx->pCfg->correctExpected ? (taosRand() % totalColNum + 1) : (taosRand());
  int32_t colAdded = 0;
  
  if (qptCtx->pCfg->correctExpected) {
    qptMarkTableInUseCols(colNum, totalColNum, pCol);
    for (int32_t i = 0; i < totalColNum && colAdded < colNum; ++i) {
      if (0 == pCol[i].inUse) {
        continue;
      }
      
      assert(0 == nodesListMakeStrictAppend(ppCols, qptMakeColumnNodeFromTable(i, pCol[i].colType, pScanPhysiNode)));
    }

    return;
  }
  
  for (int32_t i = 0; i < colNum; ++i) {
    int32_t colIdx = taosRand();
    colIdx = (colIdx >= totalColNum) ? -1 : colIdx;
    
    assert(0 == nodesListMakeStrictAppend(ppCols, qptMakeColumnNodeFromTable(colIdx, pCol[i].colType, pScanPhysiNode)));
  }
}


void qptCreateTableScanCols(       SScanPhysiNode* pScanPhysiNode) {
  qptCreateTableScanColsImpl(pScanPhysiNode, &pScanPhysiNode->pScanCols, qptCtx->param.tbl.colNum, qptCtx.param->tbl.pCol);
}

void qptCreateTableScanPseudoCols(       SScanPhysiNode* pScanPhysiNode) {
  qptCreateTableScanColsImpl(pScanPhysiNode, &pScanPhysiNode->pScanPseudoCols, qptCtx->param.tbl.tagNum, qptCtx->param.tbl.pTag);
}


void qptCreateScanPhysiNodeImpl(         SScanPhysiNode* pScanPhysiNode) {
  qptCreateTableScanCols(pScanPhysiNode);

  assert(0 == addDataBlockSlots(qptCtx->buildCtx.pCxt, pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc));

  if (taosRand() % 2) {
    qptCreateTableScanPseudoCols(pScanPhysiNode);
  }
  
  assert(0 == addDataBlockSlots(qptCtx->buildCtx.pCxt, pScanPhysiNode->pScanPseudoCols, pScanPhysiNode->node.pOutputDataBlockDesc));
  
  pScanPhysiNode->uid = qptCtx->param.tbl.uid;
  pScanPhysiNode->suid = qptCtx->param.tbl.suid;
  pScanPhysiNode->tableType = qptCtx->param.tbl.tblType;
  pScanPhysiNode->groupOrderScan = (taosRand() % 2) : true : false;

  SName tblName = {0};
  toName(1, qptCtx->param.db.dbName, qptCtx->param.tbl.tblName, &tblName);
  memcpy(&pScanPhysiNode->tableName, &tblName, sizeof(SName));
}



STagScanPhysiNode* qptCreateTagScanPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);
  assert(pPhysiNode);

  STagScanPhysiNode* pTagScanNode = (STagScanPhysiNode*)pPhysiNode;
  pTagScanNode->onlyMetaCtbIdx = (taosRand() % 2) : true : false;

  qptCreateScanPhysiNodeImpl(&pTagScanNode->scan);

  qptPostCreatePhysiNode(pPhysiNode);
  
  return pPhysiNode;
}


SSortMergeJoinPhysiNode* qptCreateSortMergeJoinPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);
  assert(pPhysiNode);

  SSortMergeJoinPhysiNode* p = (SSortMergeJoinPhysiNode*)pPhysiNode;

/*
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
*/  

  return pPhysiNode;
}

SPhysiNode* qptCreatePhysicalPlanNode(int32_t nodeType) {
  switch (nodeType) {
    case QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN:
      return qptCreateTagScanPhysiNode(nodeType);
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN:
    case QUERY_NODE_PHYSICAL_PLAN_PROJECT:
    case QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN:
      return qptCreateSortMergeJoinPhysiNode(nodeType);
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

int32_t qptCreateQueryPlan(SNode** ppPlan) {

}


void qptRerunBlockedHere() {
  while (qptInRerun) {
    taosSsleep(1);
  }
}

void qptResetForReRun() {

}

void qptSingleTestDone(bool* contLoop) {
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

void qptRunPlanTest(char* caseName) {
  SNode* pNode = NULL;
  SReadHandle readHandle = {0};
  SOperatorInfo* pOperator = NULL;

  if (qptCtx->param.plan.singlePhysiNode) {
    pNode = (SNode*)qptCreatePhysicalPlanNode(qptCtx->param.plan.subplanType[0]);
  }
  
  qptPrintBeginInfo(caseName);

  qptCtx.startTsUs = taosGetTimestampUs();
  int32_t code = createTagScanOperatorInfo(&readHandle, (STagScanPhysiNode*)pNode, NULL, NULL, NULL, NULL, &pOperator);

  destroyOperator(pOperator);
  nodesDestroyNode((SNode*)pNode);

  qptHandleTestEnd();
}



SQPTNodeParam* qptInitNodeParam(int32_t nodeType) {

}

void qptInitTestCtx(bool correctExpected, bool singleNode, int32_t nodeType, int32_t paramNum, SQPTNodeParam* nodeParam) {
  qptCtx->param.correctExpected = correctExpected;
  qptCtx->param.plan.singlePhysiNode = singleNode;
  if (singleNode) {
    qptCtx->param.plan.subplanMaxLevel = 1;
    qptCtx->param.plan.subplanType[0] = nodeType;
  } else {
    qptCtx->param.plan.subplanMaxLevel = taosRand() % QPT_MAX_SUBPLAN_LEVEL + 1;
    for (int32_t i = 0; i < qptCtx->param.plan.subplanMaxLevel; ++i) {
      qptCtx->param.plan.subplanType[i] = QPT_PHYSIC_NODE_LIST[taosRand() % QPT_PHYSIC_NODE_NUM()];
    }
  }

  if (paramNum > 0) {
    qptCtx->param.plan.physiNodeParamNum = paramNum;
    qptCtx->param.plan.physicNodeParam = nodeParam;
  }
}


}  // namespace

#if 1
#if 1
TEST(correctSingleNodeTest, tagScan) {
  char* caseName = "correctSingleNodeTest:tagScan";

  qptInitTestCtx(true, true, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, 0, NULL);
  
  for (qptCtx.loopIdx = 0; qptCtx.loopIdx < QPT_MAX_LOOP; ++qptCtx.loopIdx) {
    qptRunPlanTest(caseName);
  }

  qptPrintStatInfo(caseName); 
}
#endif

#if 0
TEST(randSingleNodeTest, tagScan) {
  char* caseName = "randSingleNodeTest:tagScan";

  qptInitTestCtx(false, true, QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, 0, NULL);
  
  for (qptCtx.loopIdx = 0; qptCtx.loopIdx < QPT_MAX_LOOP; ++qptCtx.loopIdx) {
    qptRunSingleTest(caseName);
  }

  printStatInfo(caseName); 
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
