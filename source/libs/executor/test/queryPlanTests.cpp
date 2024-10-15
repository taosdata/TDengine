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
#include "ttime.h"

namespace {

#define QPT_MAX_LOOP          10000
#define QPT_MAX_SUBPLAN_LEVEL 1000
#define QPT_MAX_WHEN_THEN_NUM 10
#define QPT_MAX_NODE_LEVEL    5
#define QPT_MAX_STRING_LEN    1048576
#define QPT_MAX_FUNC_PARAM    5
#define QPT_MAX_LOGIC_PARAM   5
#define QPT_MAX_NODE_LIST_NUM 5
#define QPT_DEFAULT_VNODE_NUM 5
#define QPT_MAX_DS_SRC_NUM    10
#define QPT_MAX_ORDER_BY_NUM  10
#define QPT_MAX_COLUMN_NUM    6 //8192

#define QPT_QUERY_NODE_COL    10000

typedef enum {
  QPT_NODE_COLUMN,
  QPT_NODE_EXPR,
  QPT_NODE_FUNCTION,
  QPT_NODE_VALUE,
  QPT_NODE_MAX_VALUE
} QPT_NODE_TYPE;

typedef SNode* (*planBuildFunc)(int32_t);


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
  int32_t        subplanIdx[QPT_MAX_SUBPLAN_LEVEL];
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
  SEpSet  epSet;
} SQPTVnodeParam;

typedef struct {
  int32_t type;
  char    name[TSDB_COL_NAME_LEN];
  int32_t dtype;
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
  SNodeList* pColList;
  SNodeList* pTagList;
  SNodeList* pColTagList;
} SQPTTblParam;


typedef struct {
  bool           correctExpected;
  uint64_t       schedulerId;
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
  int16_t            nextBlockId;
  int32_t            primaryTsSlotId;
  SExecTaskInfo*     pCurrTask;
} SQPTBuildPlanCtx;

typedef struct {
  int32_t nodeLevel;
  bool    fromTable;
  bool    onlyTag;
  bool    onlyCol;
  int16_t inputBlockId;
  SNodeList* pInputList;
} SQPTMakeNodeCtx;

typedef struct {
  int32_t code;
} SQPTExecResult;

typedef struct {
  int32_t          loopIdx;
  char             caseName[128];
  SQPTParam        param;
  SQPTBuildPlanCtx buildCtx;
  SQPTMakeNodeCtx  makeCtx;
  SQPTMakeNodeCtx  makeCtxBak;
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

typedef struct {
  int32_t       type;
  char*         name;
  planBuildFunc buildFunc;
} SQPTPlan;

SNode* qptCreateTagScanPhysiNode(int32_t nodeType);
SNode* qptCreateTableScanPhysiNode(int32_t nodeType);
SNode* qptCreateTableSeqScanPhysiNode(int32_t nodeType);
SNode* qptCreateTableMergeScanPhysiNode(int32_t nodeType);
SNode* qptCreateStreamScanPhysiNode(int32_t nodeType);
SNode* qptCreateSysTableScanPhysiNode(int32_t nodeType);
SNode* qptCreateBlockDistScanPhysiNode(int32_t nodeType);
SNode* qptCreateLastRowScanPhysiNode(int32_t nodeType);
SNode* qptCreateProjectPhysiNode(int32_t nodeType);
SNode* qptCreateMergeJoinPhysiNode(int32_t nodeType);
SNode* qptCreateHashAggPhysiNode(int32_t nodeType);
SNode* qptCreateExchangePhysiNode(int32_t nodeType);
SNode* qptCreateMergePhysiNode(int32_t nodeType);
SNode* qptCreateSortPhysiNode(int32_t nodeType);
SNode* qptCreateGroupSortPhysiNode(int32_t nodeType);
SNode* qptCreateIntervalPhysiNode(int32_t nodeType);
SNode* qptCreateMergeIntervalPhysiNode(int32_t nodeType);
SNode* qptCreateMergeAlignedIntervalPhysiNode(int32_t nodeType);
SNode* qptCreateStreamIntervalPhysiNode(int32_t nodeType);
SNode* qptCreateStreamFinalIntervalPhysiNode(int32_t nodeType);
SNode* qptCreateStreamSemiIntervalPhysiNode(int32_t nodeType);
SNode* qptCreateFillPhysiNode(int32_t nodeType);
SNode* qptCreateStreamFillPhysiNode(int32_t nodeType);
SNode* qptCreateSessionPhysiNode(int32_t nodeType);
SNode* qptCreateStreamSessionPhysiNode(int32_t nodeType);
SNode* qptCreateStreamSemiSessionPhysiNode(int32_t nodeType);
SNode* qptCreateStreamFinalSessionPhysiNode(int32_t nodeType);
SNode* qptCreateStateWindowPhysiNode(int32_t nodeType);
SNode* qptCreateStreamStatePhysiNode(int32_t nodeType);
SNode* qptCreatePartitionPhysiNode(int32_t nodeType);
SNode* qptCreateStreamPartitionPhysiNode(int32_t nodeType);

SQPTPlan qptPlans[] = {
  {QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, "tagScan", qptCreateTagScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, "tableScan", qptCreateTableScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN, "tableSeqScan", qptCreateTableSeqScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, "tableMergeScan", qptCreateTableMergeScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, "streamScan", qptCreateStreamScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN, "sysTableScan", qptCreateSysTableScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN, "blockDistScan", qptCreateBlockDistScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_LAST_ROW_SCAN, "lastRowScan", qptCreateLastRowScanPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_PROJECT, "project", qptCreateProjectPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_JOIN, "mergeJoin", qptCreateMergeJoinPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_HASH_AGG, "hashAgg", qptCreateHashAggPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_EXCHANGE, "exchange", qptCreateExchangePhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE, "merge", qptCreateMergePhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_SORT, "sort", qptCreateSortPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_GROUP_SORT, "groupSort", qptCreateGroupSortPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_HASH_INTERVAL, "interval", qptCreateIntervalPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_INTERVAL, "mergeInterval", qptCreateMergeIntervalPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_ALIGNED_INTERVAL, "mergeAlignedInterval", qptCreateMergeAlignedIntervalPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL, "streamInterval", qptCreateStreamIntervalPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL, "streamFinalInterval", qptCreateStreamFinalIntervalPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL, "streamSemiInterval", qptCreateStreamSemiIntervalPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_FILL, "fill", qptCreateFillPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_FILL, "streamFill", qptCreateStreamFillPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_SESSION, "sessionWindow", qptCreateSessionPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION, "streamSession", qptCreateStreamSessionPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_SESSION, "streamSemiSession", qptCreateStreamSemiSessionPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_SESSION, "streamFinalSession", qptCreateStreamFinalSessionPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_STATE, "stateWindow", qptCreateStateWindowPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE, "streamState", qptCreateStreamStatePhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_PARTITION, "partition", qptCreatePartitionPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_PARTITION, "streamPartition", qptCreateStreamPartitionPhysiNode},
  {QUERY_NODE_PHYSICAL_PLAN_INDEF_ROWS_FUNC, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_INTERP_FUNC, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_DISPATCH, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_INSERT, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_QUERY_INSERT, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_DELETE, "", NULL},
  {QUERY_NODE_PHYSICAL_SUBPLAN, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_EVENT, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_HASH_JOIN, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_GROUP_CACHE, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_DYN_QUERY_CTRL, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_MERGE_COUNT, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT, "", NULL},
  {QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL, "", NULL}
};


#define QPT_PHYSIC_NODE_NUM() (sizeof(qptPlans)/sizeof(qptPlans[0]))
#define QPT_RAND_BOOL_V ((taosRand() % 2) ? true : false)
#define QPT_RAND_ORDER_V (QPT_RAND_BOOL_V ? ORDER_ASC : ORDER_DESC)
#define QPT_RAND_INT_V (taosRand() * (QPT_RAND_BOOL_V ? 1 : -1))
#define QPT_LOW_PROB() ((taosRand() % 11) == 0)
#define QPT_MID_PROB() ((taosRand() % 11) <= 1)
#define QPT_HIGH_PROB() ((taosRand() % 11) <= 7)

#define QPT_CORRECT_HIGH_PROB() (qptCtx.param.correctExpected || QPT_HIGH_PROB())
#define QPT_NCORRECT_LOW_PROB() (!qptCtx.param.correctExpected && QPT_LOW_PROB())



SQPTCtx qptCtx = {0};
SQPTCtrl qptCtrl = {1, 0, 0, 0, 0};
bool qptErrorRerun = false;
bool qptInRerun = false;


SNode* qptMakeExprNode(SNode** ppNode);
void qptMakeNodeList(QPT_NODE_TYPE nodeType, SNodeList** ppList);


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


void qptInitSingleTableCol(SQPTCol* pCol, int32_t idx, EColumnType colType) {
  if (COLUMN_TYPE_COLUMN == colType && 0 == idx) {
    sprintf(pCol->name, "primts%d", idx);
    pCol->dtype = TSDB_DATA_TYPE_TIMESTAMP;
    pCol->len = qptGetColumnRandLen(pCol->dtype);
    pCol->inUse = 0;
    pCol->hasIndex = false;
    pCol->isPrimTs = true;
    pCol->isPk = false;
    pCol->colType = colType;

    return;
  }
  
  sprintf(pCol->name, "%s%d", COLUMN_TYPE_COLUMN == colType ? "col" : "tag", idx);
  pCol->dtype = taosRand() % TSDB_DATA_TYPE_MAX;
  pCol->len = qptGetColumnRandLen(pCol->dtype);
  pCol->inUse = 0;
  pCol->hasIndex = COLUMN_TYPE_COLUMN == colType ? false : QPT_RAND_BOOL_V;
  pCol->isPrimTs = false;
  pCol->isPk = COLUMN_TYPE_COLUMN == colType ? QPT_RAND_BOOL_V : false;;
  pCol->colType = colType;
}


void qptPrintBeginInfo() {
  if (!qptCtrl.printTestInfo) {
    return;
  }

  printf("\n%dth TEST [%s] START\n", qptCtx.loopIdx, qptCtx.caseName);

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

void qptPrintEndInfo() {
  if (!qptCtrl.printTestInfo) {
    return;
  }

  printf("\n\t%dth TEST [%s] END, result - %s%s\n", qptCtx.loopIdx, qptCtx.caseName, 
    (0 == qptCtx.result.code) ? "succeed" : "failed with error:",
    (0 == qptCtx.result.code) ? "" : tstrerror(qptCtx.result.code));
}

void qptPrintStatInfo() {

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

void qptGetRandValue(uint8_t* pType, int32_t* pLen, void** ppVal) {
  if (*pType == (uint8_t)-1 || QPT_NCORRECT_LOW_PROB()) {
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

ENullOrder qptGetRandNullOrder() {
  if (QPT_NCORRECT_LOW_PROB()) {
    return (ENullOrder)taosRand();
  }

  return (ENullOrder)(taosRand() % NULL_ORDER_LAST + 1);
}

int8_t qptGetRandTimestampUnit() {
  static int8_t units[] = {TIME_UNIT_NANOSECOND, TIME_UNIT_MICROSECOND, TIME_UNIT_MILLISECOND, TIME_UNIT_SECOND,
    TIME_UNIT_MINUTE, TIME_UNIT_HOUR, TIME_UNIT_DAY, TIME_UNIT_WEEK, TIME_UNIT_MONTH, TIME_UNIT_YEAR};

  return units[taosRand() % (sizeof(units) / sizeof(units[0]))];
}

int32_t qptGetInputPrimaryTsSlotId() {
  if (QPT_CORRECT_HIGH_PROB()) {
    return qptCtx.buildCtx.primaryTsSlotId;
  }

  return taosRand() % QPT_MAX_COLUMN_NUM;
}

void qptNodesCalloc(int32_t num, int32_t size, void** pOut) {
  void* p = taosMemoryCalloc(num, size);
  assert(p);
  *(char*)p = 0;
  *pOut = (char*)p + 1;
}

void qptNodesFree(void* pNode) {
  void* p = (char*)pNode - 1;
  taosMemoryFree(p);
}

EFillMode qptGetRandFillMode() {
  if (QPT_CORRECT_HIGH_PROB()) {
    return (EFillMode)(taosRand() % FILL_MODE_NEXT + 1);
  }

  return (EFillMode)(taosRand());
}


void qptGetRandTimeWindow(STimeWindow* pWindow) {
  if (QPT_CORRECT_HIGH_PROB()) {
    pWindow->skey = taosRand();
    pWindow->ekey = pWindow->skey + taosRand();
    return;
  }

  pWindow->skey = taosRand();
  pWindow->ekey = taosRand();
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

SNode* qptMakeRandNode(SNode** ppNode) {
  SNode* pNode = NULL;
  nodesMakeNode((ENodeType)taosRand(), ppNode ? ppNode : &pNode);
  return ppNode ? *ppNode : pNode;
}



SNode* qptMakeColumnFromTable(int32_t colIdx) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(NULL);
  }

  if (colIdx < 0) {
    return NULL;
  }
  
  SColumnNode* pCol = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol));
  assert(pCol);

  SQPTCol fakeCol;
  fakeCol.type = QPT_QUERY_NODE_COL;
  qptInitSingleTableCol(&fakeCol, taosRand(), (EColumnType)(taosRand() % COLUMN_TYPE_GROUP_KEY + 1));
  
  SQPTCol* pTbCol = qptCtx.makeCtx.pInputList ? (SQPTCol*)nodesListGetNode(qptCtx.makeCtx.pInputList, colIdx) : &fakeCol;
  
  int16_t blkId = QPT_CORRECT_HIGH_PROB() ? qptCtx.makeCtx.inputBlockId : taosRand();

  if (QPT_CORRECT_HIGH_PROB()) {
    pCol->node.resType.type = pTbCol->dtype;
    pCol->node.resType.bytes = pTbCol->len;

    pCol->tableId = qptCtx.param.tbl.uid;  
    pCol->tableType = qptCtx.param.tbl.tblType;  
    pCol->colId = colIdx;
    pCol->projIdx = colIdx;
    pCol->colType = pTbCol->colType;
    pCol->hasIndex = pTbCol->hasIndex;
    pCol->isPrimTs = pTbCol->isPrimTs;
    strcpy(pCol->dbName, qptCtx.param.db.dbName);
    strcpy(pCol->tableName, qptCtx.param.tbl.tblName);
    strcpy(pCol->tableAlias, qptCtx.param.tbl.tblAlias);
    strcpy(pCol->colName, pTbCol->name);
    pCol->dataBlockId = blkId;
    pCol->slotId = colIdx;
    pCol->numOfPKs = qptCtx.param.tbl.pkNum;
    pCol->tableHasPk = qptCtx.param.tbl.pkNum > 0;
    pCol->isPk = pTbCol->isPk;
    pCol->projRefIdx = 0;
    pCol->resIdx = 0;
  } else {
    qptGetRandValue(&pCol->node.resType.type, &pCol->node.resType.bytes, NULL);

    pCol->tableId = taosRand();  
    pCol->tableType = taosRand() % TSDB_TABLE_MAX;  
    pCol->colId = QPT_RAND_BOOL_V ? taosRand() : colIdx;
    pCol->projIdx = taosRand();
    pCol->colType = QPT_RAND_BOOL_V ? pTbCol->colType : (EColumnType)(taosRand() % (COLUMN_TYPE_GROUP_KEY + 1));
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
      strcpy(pCol->colName, pTbCol->name);
    }
    
    pCol->dataBlockId = blkId;
    pCol->slotId = QPT_RAND_BOOL_V ? taosRand() : colIdx;
    pCol->numOfPKs = QPT_RAND_BOOL_V ? taosRand() : qptCtx.param.tbl.pkNum;
    pCol->tableHasPk = QPT_RAND_BOOL_V ? QPT_RAND_BOOL_V : (qptCtx.param.tbl.pkNum > 0);
    pCol->isPk = QPT_RAND_BOOL_V ? QPT_RAND_BOOL_V : pTbCol->isPk;
    pCol->projRefIdx = taosRand();
    pCol->resIdx = taosRand();
  }

  return (SNode*)pCol;
}


SNode* qptMakeWhenThenNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }
  
  assert(0 == nodesMakeNode(QUERY_NODE_WHEN_THEN, ppNode));
  assert(*ppNode);
  SWhenThenNode* pWhenThen = (SWhenThenNode*)*ppNode;

  qptMakeExprNode(&pWhenThen->pWhen);

  qptMakeExprNode(&pWhenThen->pThen);

  return *ppNode;
}


SNode* qptMakeCaseWhenNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
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

  return *ppNode;  
}


SNode* qptMakeOperatorNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
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

  return *ppNode;
}

SNode* qptMakeColumnNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SColumnNode* pCol = NULL;

  if (QPT_CORRECT_HIGH_PROB() && qptCtx.makeCtx.pInputList) {
    SNodeList* pColList = qptCtx.makeCtx.pInputList;
    int32_t colIdx = taosRand() % pColList->length;
    SQPTCol* pNode = (SQPTCol*)nodesListGetNode(pColList, colIdx);
    if (pNode) {
      switch (pNode->type) {
        case QUERY_NODE_SLOT_DESC: {
          nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
          SSlotDescNode* pSlot = (SSlotDescNode*)pNode;
          pCol->node.resType = pSlot->dataType;
          pCol->dataBlockId = qptCtx.makeCtx.inputBlockId;
          pCol->slotId = pSlot->slotId;
          break;
        }
        case QPT_QUERY_NODE_COL: {
          pCol = (SColumnNode*)qptMakeColumnFromTable(colIdx);
          break;
        }
        default:
          break;
      }
    }
  } 

  if (NULL == pCol) {
    nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pCol);
    qptGetRandValue(&pCol->node.resType.type, &pCol->node.resType.bytes, NULL);
    pCol->dataBlockId = taosRand();
    pCol->slotId = taosRand();
  }

  *ppNode = (SNode*)pCol;

  return *ppNode;
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


SNode* qptMakeValueNode(uint8_t valType, SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
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

  return *ppNode;
}

SNode* qptMakeFunctionNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SFunctionNode* pFunc = NULL;
  nodesMakeNode(QUERY_NODE_FUNCTION, (SNode**)&pFunc);

  if (QPT_CORRECT_HIGH_PROB()) {
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

  return *ppNode;
}




SNode* qptMakeLogicCondNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
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

  return *ppNode;
}

SNode* qptMakeNodeListNode(QPT_NODE_TYPE nodeType, SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SNodeListNode* pList = NULL;
  nodesMakeNode(QUERY_NODE_NODE_LIST, (SNode**)&pList);

  qptCtx.makeCtx.nodeLevel++;  

  qptMakeNodeList(nodeType, &pList->pNodeList);

  *ppNode = (SNode*)pList; 

  return *ppNode;
}

SNode* qptMakeTempTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  STempTableNode* pTemp = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_TEMP_TABLE, (SNode**)&pTemp));

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  }

  *ppNode = (SNode*)pTemp;

  return *ppNode;
}

SNode* qptMakeJoinTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SJoinTableNode* pJoin = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_JOIN_TABLE, (SNode**)&pJoin));

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  }

  *ppNode = (SNode*)pJoin;

  return *ppNode;
}

SNode* qptMakeRealTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SRealTableNode* pReal = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_REAL_TABLE, (SNode**)&pReal));

  if (QPT_CORRECT_HIGH_PROB()) {
    // TODO
  }

  *ppNode = (SNode*)pReal;

  return *ppNode;
}



SNode* qptMakeNonRealTableNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
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

  return *ppNode;
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
      qptMakeNodeListNode(QPT_NODE_EXPR, ppNode);
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


SNode* qptMakeLimitNode(SNode** ppNode) {
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

  *ppNode = pNode;

  return pNode;
}


SNode* qptMakeWindowOffsetNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SNode* pNode = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_WINDOW_OFFSET, &pNode));
  assert(pNode);

  SWindowOffsetNode* pWinOffset = (SWindowOffsetNode*)pNode;  
  qptMakeValueNode(TSDB_DATA_TYPE_BIGINT, &pWinOffset->pStartOffset);
  qptMakeValueNode(TSDB_DATA_TYPE_BIGINT, &pWinOffset->pEndOffset);

  *ppNode = pNode;
  
  return pNode;
}

void qptSaveMakeNodeCtx() {
  qptCtx.makeCtxBak.nodeLevel = qptCtx.makeCtx.nodeLevel;
}

void qptRestoreMakeNodeCtx() {
  qptCtx.makeCtx.nodeLevel = qptCtx.makeCtxBak.nodeLevel;
}


void qptResetTableCols() {
  SNode* pTmp = NULL;
  FOREACH(pTmp, qptCtx.param.tbl.pColList) {
    ((SQPTCol*)pTmp)->inUse = 0;
  }
  FOREACH(pTmp, qptCtx.param.tbl.pTagList) {
    ((SQPTCol*)pTmp)->inUse = 0;
  }
}

void qptResetMakeNodeCtx() {
  SQPTMakeNodeCtx* pCtx = &qptCtx.makeCtx;
  pCtx->nodeLevel = 1;

  if (pCtx->fromTable) {
    qptResetTableCols();
  }
}

void qptInitMakeNodeCtx(bool fromTable, bool onlyTag, bool onlyCol, int16_t inputBlockId, SNodeList* pInputList) {
  SQPTMakeNodeCtx* pCtx = &qptCtx.makeCtx;
    
  pCtx->onlyTag = onlyTag;
  pCtx->fromTable = fromTable;
  pCtx->onlyCol = onlyCol;

  if (NULL == pInputList) {
    if (fromTable) {
      inputBlockId = (qptCtx.buildCtx.pCurr && qptCtx.buildCtx.pCurr->pOutputDataBlockDesc) ? qptCtx.buildCtx.pCurr->pOutputDataBlockDesc->dataBlockId : taosRand();
      pInputList = onlyTag ? qptCtx.param.tbl.pTagList : (onlyCol ? qptCtx.param.tbl.pColList : qptCtx.param.tbl.pColTagList); 
    } else if (qptCtx.buildCtx.pChild && qptCtx.buildCtx.pChild->pOutputDataBlockDesc) {
      inputBlockId = qptCtx.buildCtx.pChild->pOutputDataBlockDesc->dataBlockId;
      pInputList = qptCtx.buildCtx.pChild->pOutputDataBlockDesc->pSlots;
    }
  }

  pCtx->inputBlockId = inputBlockId;
  pCtx->pInputList = pInputList;
  
  qptResetMakeNodeCtx();
}

SNode* qptMakeConditionNode() {
  SNode* pNode = NULL;
  qptMakeExprNode(&pNode);
  
  return pNode;
}

SNode* qptMakeDataBlockDescNode() {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(NULL);
  }

  SDataBlockDescNode* pDesc = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_DATABLOCK_DESC, (SNode**)&pDesc));
  
  pDesc->dataBlockId = QPT_CORRECT_HIGH_PROB() ? qptCtx.buildCtx.nextBlockId++ : QPT_RAND_INT_V;
  pDesc->precision = QPT_CORRECT_HIGH_PROB() ? qptCtx.param.db.precision : QPT_RAND_INT_V;

  return (SNode*)pDesc;
}

SNode* qptMakeSlotDescNode(const char* pName, const SNode* pNode, int16_t slotId, bool output, bool reserve) {
  SSlotDescNode* pSlot = NULL;
  if (QPT_NCORRECT_LOW_PROB()) {
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

SNode* qptMakeTargetNode(SNode* pNode, int16_t dataBlockId, int16_t slotId, SNode** pOutput) {
  if (QPT_NCORRECT_LOW_PROB()) {
    nodesDestroyNode(pNode);
    return qptMakeRandNode(pOutput);
  }

  STargetNode* pTarget = NULL;
  assert(0 == nodesMakeNode(QUERY_NODE_TARGET, (SNode**)&pTarget));

  pTarget->dataBlockId = QPT_CORRECT_HIGH_PROB() ? dataBlockId : taosRand();
  pTarget->slotId = QPT_CORRECT_HIGH_PROB() ? slotId : taosRand();
  pTarget->pExpr = QPT_CORRECT_HIGH_PROB() ? pNode : qptMakeRandNode(NULL);
  if (pTarget->pExpr != pNode) {
    nodesDestroyNode(pNode);
  }
  
  *pOutput = (SNode*)pTarget;

  return *pOutput;
}

SNode* qptMakeDownstreamSrcNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SDownstreamSourceNode* pDs = NULL;
  nodesMakeNode(QUERY_NODE_DOWNSTREAM_SOURCE, (SNode**)&pDs);

  pDs->addr.nodeId = qptCtx.param.vnode.vgId;
  memcpy(&pDs->addr.epSet, &qptCtx.param.vnode.epSet, sizeof(pDs->addr.epSet));
  pDs->taskId = (QPT_CORRECT_HIGH_PROB() && qptCtx.buildCtx.pCurrTask) ? qptCtx.buildCtx.pCurrTask->id.taskId : taosRand();
  pDs->schedId = QPT_CORRECT_HIGH_PROB() ? qptCtx.param.schedulerId : taosRand();
  pDs->execId = taosRand();
  pDs->fetchMsgType = QPT_CORRECT_HIGH_PROB() ? (QPT_RAND_BOOL_V ? TDMT_SCH_FETCH : TDMT_SCH_MERGE_FETCH) : taosRand();
  pDs->localExec = QPT_RAND_BOOL_V;

  *ppNode = (SNode*)pDs;

  return *ppNode;
}

SNode* qptMakeOrderByExprNode(SNode** ppNode) {
  if (QPT_NCORRECT_LOW_PROB()) {
    return qptMakeRandNode(ppNode);
  }

  SOrderByExprNode* pOrder = NULL;
  nodesMakeNode(QUERY_NODE_ORDER_BY_EXPR, (SNode**)&pOrder);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pOrder->pExpr);
  
  pOrder->order = (EOrder)(QPT_CORRECT_HIGH_PROB() ? (QPT_RAND_BOOL_V ? ORDER_ASC : ORDER_DESC) : taosRand());
  pOrder->nullOrder = qptGetRandNullOrder();

  *ppNode = (SNode*)pOrder;

  return *ppNode;
}

SPhysiNode* qptCreatePhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = NULL;
  assert(0 == nodesMakeNode((ENodeType)nodeType, (SNode**)&pPhysiNode));
  assert(pPhysiNode);
  
  qptCtx.buildCtx.pCurr = pPhysiNode;

  qptMakeLimitNode(&pPhysiNode->pLimit);
  qptMakeLimitNode(&pPhysiNode->pSlimit);
  pPhysiNode->dynamicOp = qptGetDynamicOp();
  pPhysiNode->inputTsOrder = qptGetCurrTsOrder();

  pPhysiNode->pOutputDataBlockDesc = (SDataBlockDescNode*)qptMakeDataBlockDescNode();

  return pPhysiNode;
}

void qptPostCreatePhysiNode(SPhysiNode* pPhysiNode) {
  pPhysiNode->outputTsOrder = qptGetCurrTsOrder();

  if (QPT_RAND_BOOL_V) {
    qptInitMakeNodeCtx((QPT_CORRECT_HIGH_PROB() && qptCtx.buildCtx.pChild) ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
    pPhysiNode->pConditions = qptMakeConditionNode();
  }
  
}

void qptMarkTableInUseCols(int32_t colNum, int32_t totalColNum) {
  if (colNum >= totalColNum) {
    for (int32_t i = 0; i < totalColNum; ++i) {
      SQPTCol* pNode = (SQPTCol*)nodesListGetNode(qptCtx.makeCtx.pInputList, i);
      assert(pNode->type == QPT_QUERY_NODE_COL);
      pNode->inUse = 1;
    }
    return;
  }
  
  int32_t colInUse = 0;
  do {
    int32_t colIdx = taosRand() % totalColNum;
    SQPTCol* pNode = (SQPTCol*)nodesListGetNode(qptCtx.makeCtx.pInputList, colIdx);
    assert(pNode->type == QPT_QUERY_NODE_COL);

    if (pNode->inUse) {
      continue;
    }

    pNode->inUse = 1;
    colInUse++;
  } while (colInUse < colNum);
}


void qptMakeTableScanColList(       SNodeList** ppCols) {
  if (QPT_NCORRECT_LOW_PROB()) {
    if (QPT_RAND_BOOL_V) {
      nodesMakeList(ppCols);
    } else {
      *ppCols = NULL;
    }
    
    return;
  }
  
  int32_t colNum = (QPT_CORRECT_HIGH_PROB() && qptCtx.makeCtx.pInputList) ? (taosRand() % qptCtx.makeCtx.pInputList->length + 1) : (taosRand() % QPT_MAX_COLUMN_NUM);
  int32_t colAdded = 0;

  if (qptCtx.makeCtx.pInputList) {
    if (QPT_CORRECT_HIGH_PROB()) {
      qptMarkTableInUseCols(colNum, qptCtx.makeCtx.pInputList->length);
      
      for (int32_t i = 0; colAdded < colNum; ++i) {
        int32_t idx = (i < qptCtx.makeCtx.pInputList->length) ? i : (taosRand() % qptCtx.makeCtx.pInputList->length);
        SQPTCol* pNode = (SQPTCol*)nodesListGetNode(qptCtx.makeCtx.pInputList, idx);
        assert(pNode->type == QPT_QUERY_NODE_COL);
        
        if (0 == pNode->inUse) {
          continue;
        }

        assert(0 == qptNodesListMakeStrictAppend(ppCols, qptMakeColumnFromTable(idx)));
        colAdded++;
      }

      return;
    }
    
    for (int32_t i = 0; i < colNum; ++i) {
      int32_t colIdx = taosRand();
      colIdx = (colIdx >= qptCtx.makeCtx.pInputList->length) ? -1 : colIdx;
      
      assert(0 == qptNodesListMakeStrictAppend(ppCols, qptMakeColumnFromTable(colIdx)));
    }
  } else {
    for (int32_t i = 0; i < colNum; ++i) {
      int32_t colIdx = taosRand();
      assert(0 == qptNodesListMakeStrictAppend(ppCols, qptMakeColumnFromTable(colIdx)));
    }
  }
}


void qptCreateTableScanCols(       int16_t blockId, SNodeList** ppList) {
  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? false : true, QPT_CORRECT_HIGH_PROB() ? true : false, 0, NULL);
  qptMakeTableScanColList(ppList);
}

void qptCreateTableScanPseudoCols(       int16_t blockId, SNodeList** ppList) {
  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? false : true, 0, NULL);
  qptMakeTableScanColList(ppList);
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
      SNode* pDesc = QPT_CORRECT_HIGH_PROB() ? qptMakeSlotDescNode(NULL, pExpr, nextSlotId, output, QPT_RAND_BOOL_V) : qptMakeExprNode(NULL);
      assert(0 == qptNodesListMakeStrictAppend(&pDataBlockDesc->pSlots, pDesc));
      pDataBlockDesc->totalRowSize += QPT_CORRECT_HIGH_PROB() ? ((SExprNode*)pExpr)->resType.bytes : taosRand();
      if (output && QPT_RAND_BOOL_V) {
        pDataBlockDesc->outputRowSize += QPT_CORRECT_HIGH_PROB() ? ((SExprNode*)pExpr)->resType.bytes : taosRand();
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

SNode* qptMakeSpecTypeNode(QPT_NODE_TYPE nodeType, SNode** ppNode) {
  switch (nodeType) {
    case QPT_NODE_COLUMN:
      return qptMakeColumnNode(ppNode);
    case QPT_NODE_FUNCTION:
      return qptMakeFunctionNode(ppNode);
    case QPT_NODE_EXPR:
      return qptMakeExprNode(ppNode);
    case QPT_NODE_VALUE:
      return qptMakeValueNode(-1, ppNode);
    default:
      break;
  }

  return qptMakeRandNode(ppNode);
}


void qptMakeRandNodeList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t exprNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < exprNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeSpecTypeNode((QPT_NODE_TYPE)(taosRand() % (QPT_NODE_MAX_VALUE + 1)), &pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}


void qptMakeExprList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t exprNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < exprNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeExprNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

void qptMakeValueList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t colNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < colNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeValueNode(-1, &pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

void qptMakeColumnList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t colNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < colNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeColumnNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

void qptMakeTargetList(QPT_NODE_TYPE nodeType, int16_t datablockId, SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t tarNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < tarNum; ++i) {
    SNode* pNode = NULL, *pExpr = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeSpecTypeNode(nodeType, &pExpr);
    qptMakeTargetNode(pExpr, datablockId, i, &pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}

void qptMakeFunctionList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t funcNum = taosRand() % QPT_MAX_COLUMN_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < funcNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeFunctionNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}


void qptMakeDownstreamSrcList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t dsNum = taosRand() % QPT_MAX_DS_SRC_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < dsNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeDownstreamSrcNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}


void qptMakeOrerByExprList(SNodeList** ppList) {
  qptSaveMakeNodeCtx();

  int32_t orderNum = taosRand() % QPT_MAX_ORDER_BY_NUM + (QPT_CORRECT_HIGH_PROB() ? 1 : 0);
  for (int32_t i = 0; i < orderNum; ++i) {
    SNode* pNode = NULL;
    qptRestoreMakeNodeCtx();
    qptMakeOrderByExprNode(&pNode);
    qptNodesListMakeStrictAppend(ppList, pNode);
  }
}


void qptMakeSpecTypeNodeList(QPT_NODE_TYPE nodeType, SNodeList** ppList) {
  switch (nodeType) {
    case QPT_NODE_COLUMN:
      return qptMakeColumnList(ppList);
    case QPT_NODE_FUNCTION:
      return qptMakeFunctionList(ppList);
    case QPT_NODE_EXPR:
      return qptMakeExprList(ppList);
    case QPT_NODE_VALUE:
      return qptMakeValueList(ppList);
    default:
      break;
  }

  return qptMakeRandNodeList(ppList);
}


void qptMakeNodeList(QPT_NODE_TYPE nodeType, SNodeList** ppList) {
  qptMakeSpecTypeNodeList(nodeType, ppList);
}



void qptMakeAppendToTargetList(SNodeList* pInputList, int16_t blockId, SNodeList** ppOutList) {
  SNode* pNode = NULL;
  FOREACH(pNode, pInputList) {
    if (QPT_CORRECT_HIGH_PROB()) {
      SNode* pTarget = NULL;
      int16_t slotId = ((*ppOutList) && (*ppOutList)->length) ? (*ppOutList)->length : 0;
      qptMakeTargetNode(pNode, blockId, slotId, &pTarget);
      qptNodesListMakeStrictAppend(ppOutList, pTarget);
    }
  }
}

void qptCreateScanPhysiNodeImpl(         SScanPhysiNode* pScanPhysiNode) {
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pScanPhysiNode->node.pOutputDataBlockDesc) ? pScanPhysiNode->node.pOutputDataBlockDesc->dataBlockId : taosRand();
  qptCreateTableScanCols(blockId, &pScanPhysiNode->pScanCols);

  qptAddDataBlockSlots(pScanPhysiNode->pScanCols, pScanPhysiNode->node.pOutputDataBlockDesc);

  if (taosRand() % 2) {
    blockId = (QPT_CORRECT_HIGH_PROB() && pScanPhysiNode->node.pOutputDataBlockDesc) ? pScanPhysiNode->node.pOutputDataBlockDesc->dataBlockId : taosRand();
    qptCreateTableScanPseudoCols(blockId, &pScanPhysiNode->pScanPseudoCols);
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

SNode* qptCreateTableScanPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pPhysiNode;
  pTableScanNode->scanSeq[0] = taosRand() % 4;
  pTableScanNode->scanSeq[1] = taosRand() % 4;
  pTableScanNode->scanRange.skey = taosRand();
  pTableScanNode->scanRange.ekey = taosRand();
  pTableScanNode->ratio = taosRand();
  pTableScanNode->dataRequired = taosRand();

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeFunctionList(&pTableScanNode->pDynamicScanFuncs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? false : true, 0, NULL);
  qptMakeColumnList(&pTableScanNode->pGroupTags);
  
  pTableScanNode->groupSort = QPT_RAND_BOOL_V;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? false : true, 0, NULL);
  qptMakeExprList(&pTableScanNode->pTags);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pTableScanNode->pSubtable);
  pTableScanNode->interval = taosRand();
  pTableScanNode->offset = taosRand();
  pTableScanNode->sliding = taosRand();
  pTableScanNode->intervalUnit = taosRand();
  pTableScanNode->slidingUnit = taosRand();
  pTableScanNode->triggerType = taosRand();
  pTableScanNode->watermark = taosRand();
  pTableScanNode->igExpired = taosRand();
  pTableScanNode->assignBlockUid = QPT_RAND_BOOL_V;
  pTableScanNode->igCheckUpdate = taosRand();
  pTableScanNode->filesetDelimited = QPT_RAND_BOOL_V;
  pTableScanNode->needCountEmptyTable = QPT_RAND_BOOL_V;
  pTableScanNode->paraTablesSort = QPT_RAND_BOOL_V;
  pTableScanNode->smallDataTsSort = QPT_RAND_BOOL_V;

  qptCreateScanPhysiNodeImpl(&pTableScanNode->scan);

  qptPostCreatePhysiNode(pPhysiNode);
  
  return (SNode*)pPhysiNode;
}


SNode* qptCreateTableSeqScanPhysiNode(int32_t nodeType) {
  return qptCreateTableScanPhysiNode(nodeType);
}

SNode* qptCreateTableMergeScanPhysiNode(int32_t nodeType) {
  return qptCreateTableScanPhysiNode(nodeType);
}

SNode* qptCreateStreamScanPhysiNode(int32_t nodeType) {
  return qptCreateTableScanPhysiNode(nodeType);
}

SNode* qptCreateSysTableScanPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SSystemTableScanPhysiNode* pSysScanNode = (SSystemTableScanPhysiNode*)pPhysiNode;

  memcpy(&pSysScanNode->mgmtEpSet, &qptCtx.param.vnode.epSet, sizeof(pSysScanNode->mgmtEpSet));
  pSysScanNode->showRewrite = QPT_RAND_BOOL_V;
  pSysScanNode->accountId = QPT_CORRECT_HIGH_PROB() ? 1 : taosRand();
  pSysScanNode->sysInfo = QPT_RAND_BOOL_V;

  qptCreateScanPhysiNodeImpl(&pSysScanNode->scan);

  qptPostCreatePhysiNode(pPhysiNode);
  
  return (SNode*)pPhysiNode;
}

SNode* qptCreateBlockDistScanPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SBlockDistScanPhysiNode* pBlkScanNode = (SBlockDistScanPhysiNode*)pPhysiNode;

  qptCreateScanPhysiNodeImpl((SScanPhysiNode*)pBlkScanNode);

  qptPostCreatePhysiNode(pPhysiNode);
  
  return (SNode*)pPhysiNode;
}


SNode* qptCreateLastRowScanPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SLastRowScanPhysiNode* pLRScanNode = (SLastRowScanPhysiNode*)pPhysiNode;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? true : false, QPT_CORRECT_HIGH_PROB() ? false : true, 0, NULL);
  qptMakeColumnList(&pLRScanNode->pGroupTags);
  
  pLRScanNode->groupSort = QPT_RAND_BOOL_V;
  pLRScanNode->ignoreNull = QPT_RAND_BOOL_V;

  if (QPT_CORRECT_HIGH_PROB()) {
    int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();    
    qptMakeAppendToTargetList(pLRScanNode->scan.pScanCols, blockId, &pLRScanNode->pTargets);
  }

  if (QPT_CORRECT_HIGH_PROB()) {
    int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();    
    qptMakeAppendToTargetList(pLRScanNode->scan.pScanPseudoCols, blockId, &pLRScanNode->pTargets);
  }

  if (QPT_RAND_BOOL_V) {
    int32_t funcNum = taosRand() % QPT_MAX_COLUMN_NUM;
    pLRScanNode->pFuncTypes = taosArrayInit(funcNum, sizeof(int32_t));
    assert(pLRScanNode->pFuncTypes);
    for (int32_t i = 0; i < funcNum; ++i) {
      int32_t funcType = taosRand();
      taosArrayPush(pLRScanNode->pFuncTypes, &funcType);
    }
  }

  qptCreateScanPhysiNodeImpl(&pLRScanNode->scan);

  qptPostCreatePhysiNode(pPhysiNode);
  
  return (SNode*)pPhysiNode;
}

SNode* qptCreateProjectPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SProjectPhysiNode* pProject = (SProjectPhysiNode*)pPhysiNode;

  pProject->mergeDataBlock = QPT_RAND_BOOL_V;
  pProject->ignoreGroupId = QPT_RAND_BOOL_V;
  pProject->inputIgnoreGroup = QPT_RAND_BOOL_V;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pProject->pProjections);

  qptAddDataBlockSlots(pProject->pProjections, pProject->node.pOutputDataBlockDesc);

  qptPostCreatePhysiNode(pPhysiNode);

  return (SNode*)pPhysiNode;
}


SNode* qptCreateMergeJoinPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SSortMergeJoinPhysiNode* pJoin = (SSortMergeJoinPhysiNode*)pPhysiNode;

  pJoin->joinType = (EJoinType)(taosRand() % JOIN_TYPE_MAX_VALUE + (QPT_CORRECT_HIGH_PROB() ? 0 : 1));
  pJoin->subType = (EJoinSubType)(taosRand() % JOIN_STYPE_MAX_VALUE + (QPT_CORRECT_HIGH_PROB() ? 0 : 1));
  qptMakeWindowOffsetNode(&pJoin->pWindowOffset);
  qptMakeLimitNode(&pJoin->pJLimit);
  pJoin->asofOpType = OPERATOR_ARRAY[taosRand() % (sizeof(OPERATOR_ARRAY)/sizeof(OPERATOR_ARRAY[0]))] + (QPT_CORRECT_HIGH_PROB() ? 0 : 1);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pJoin->leftPrimExpr);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pJoin->rightPrimExpr);

  pJoin->leftPrimSlotId = qptGetInputSlotId(qptCtx.buildCtx.pChild ? qptCtx.buildCtx.pChild->pOutputDataBlockDesc : NULL);
  pJoin->rightPrimSlotId = qptGetInputSlotId(qptCtx.buildCtx.pChild ? qptCtx.buildCtx.pChild->pOutputDataBlockDesc : NULL);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnList(&pJoin->pEqLeft);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnList(&pJoin->pEqRight);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pJoin->pPrimKeyCond);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pJoin->pColEqCond);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pJoin->pColOnCond);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pJoin->pFullOnCond);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_EXPR, blockId, &pJoin->pTargets);

  for (int32_t i = 0; i < 2; i++) {
    pJoin->inputStat[i].inputRowNum = taosRand();
    pJoin->inputStat[i].inputRowSize = taosRand();
  }

  pJoin->seqWinGroup = QPT_RAND_BOOL_V;
  pJoin->grpJoin = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}

SNode* qptCreateHashAggPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SAggPhysiNode* pAgg = (SAggPhysiNode*)pPhysiNode;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pAgg->pExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_EXPR, blockId, &pAgg->pGroupKeys);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_FUNCTION, blockId, &pAgg->pAggFuncs);
  
  pAgg->mergeDataBlock = QPT_RAND_BOOL_V;
  pAgg->groupKeyOptimized = QPT_RAND_BOOL_V;
  pAgg->hasCountLikeFunc = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}

SNode* qptCreateExchangePhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SExchangePhysiNode* pExc = (SExchangePhysiNode*)pPhysiNode;

  pExc->srcStartGroupId = taosRand();
  pExc->srcEndGroupId = taosRand();
  pExc->singleChannel = QPT_RAND_BOOL_V;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeDownstreamSrcList(&pExc->pSrcEndPoints);

  pExc->seqRecvData = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}

SNode* qptCreateMergePhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SMergePhysiNode* pMerge = (SMergePhysiNode*)pPhysiNode;

  pMerge->type = (EMergeType)(QPT_CORRECT_HIGH_PROB() ? (taosRand() % (MERGE_TYPE_MAX_VALUE - 1) + 1) : taosRand());

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeOrerByExprList(&pMerge->pMergeKeys);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_EXPR, blockId, &pMerge->pTargets);

  pMerge->numOfChannels = taosRand();
  pMerge->numOfSubplans = taosRand();
  pMerge->srcGroupId = taosRand();
  pMerge->srcEndGroupId = taosRand();
  pMerge->groupSort = QPT_RAND_BOOL_V;
  pMerge->ignoreGroupId = QPT_RAND_BOOL_V;
  pMerge->inputWithGroupId = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}


SNode* qptCreateSortPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SSortPhysiNode* pSort = (SSortPhysiNode*)pPhysiNode;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pSort->pExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeOrerByExprList(&pSort->pSortKeys);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_EXPR, blockId, &pSort->pTargets);

  pSort->calcGroupId = QPT_RAND_BOOL_V;
  pSort->excludePkCol = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}

SNode* qptCreateGroupSortPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SGroupSortPhysiNode* pSort = (SGroupSortPhysiNode*)pPhysiNode;

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pSort->pExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeOrerByExprList(&pSort->pSortKeys);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPhysiNode->pOutputDataBlockDesc) ? pPhysiNode->pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_EXPR, blockId, &pSort->pTargets);

  pSort->calcGroupId = QPT_RAND_BOOL_V;
  pSort->excludePkCol = QPT_RAND_BOOL_V;

  return (SNode*)pPhysiNode;
}

void qptCreateWindowPhysiNode(SWindowPhysiNode* pWindow) {
  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pWindow->pExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeFunctionList(&pWindow->pFuncs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnNode(&pWindow->pTspk);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnNode(&pWindow->pTsEnd);

  pWindow->triggerType = taosRand();
  pWindow->watermark = taosRand();
  pWindow->deleteMark = taosRand();
  pWindow->igExpired = taosRand();
  pWindow->destHasPrimayKey = taosRand();
  pWindow->mergeDataBlock = QPT_RAND_BOOL_V;
}

SNode* qptCreateIntervalPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SIntervalPhysiNode* pInterval = (SIntervalPhysiNode*)pPhysiNode;

  qptCreateWindowPhysiNode(&pInterval->window);

  pInterval->interval = taosRand();
  pInterval->offset = taosRand();
  pInterval->sliding = taosRand();
  pInterval->intervalUnit = qptGetRandTimestampUnit();
  pInterval->slidingUnit = qptGetRandTimestampUnit();

  return (SNode*)pPhysiNode;
}

SNode* qptCreateMergeIntervalPhysiNode(int32_t nodeType) {
  return qptCreateIntervalPhysiNode(nodeType);
}

SNode* qptCreateMergeAlignedIntervalPhysiNode(int32_t nodeType) {
  return qptCreateIntervalPhysiNode(nodeType);
}

SNode* qptCreateStreamIntervalPhysiNode(int32_t nodeType) {
  return qptCreateIntervalPhysiNode(nodeType);
}

SNode* qptCreateStreamFinalIntervalPhysiNode(int32_t nodeType) {
  return qptCreateIntervalPhysiNode(nodeType);
}

SNode* qptCreateStreamSemiIntervalPhysiNode(int32_t nodeType) {
  return qptCreateIntervalPhysiNode(nodeType);
}

SNode* qptCreateFillPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SFillPhysiNode* pFill = (SFillPhysiNode*)pPhysiNode;

  pFill->mode = qptGetRandFillMode();

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pFill->pFillExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pFill->pNotFillExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnNode(&pFill->pWStartTs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeNodeListNode(QPT_NODE_VALUE, &pFill->pValues);
  
  qptGetRandTimeWindow(&pFill->timeRange);

  return (SNode*)pPhysiNode;
}

SNode* qptCreateStreamFillPhysiNode(int32_t nodeType) {
  return qptCreateFillPhysiNode(nodeType);
}


SNode* qptCreateSessionPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SSessionWinodwPhysiNode* pSession = (SSessionWinodwPhysiNode*)pPhysiNode;

  qptCreateWindowPhysiNode(&pSession->window);

  pSession->gap = taosRand();

  return (SNode*)pPhysiNode;
}

SNode* qptCreateStreamSessionPhysiNode(int32_t nodeType) {
  return qptCreateSessionPhysiNode(nodeType);
}

SNode* qptCreateStreamSemiSessionPhysiNode(int32_t nodeType) {
  return qptCreateSessionPhysiNode(nodeType);
}

SNode* qptCreateStreamFinalSessionPhysiNode(int32_t nodeType) {
  return qptCreateSessionPhysiNode(nodeType);
}

SNode* qptCreateStateWindowPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SStateWinodwPhysiNode* pState = (SStateWinodwPhysiNode*)pPhysiNode;

  qptCreateWindowPhysiNode(&pState->window);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnNode(&pState->pStateKey);

  return (SNode*)pPhysiNode;
}

SNode* qptCreateStreamStatePhysiNode(int32_t nodeType) {
  return qptCreateStateWindowPhysiNode(nodeType);
}

void qptCreatePartitionPhysiNodeImpl(SPartitionPhysiNode* pPartition) {
  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprList(&pPartition->pExprs);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, pPartition->node.pOutputDataBlockDesc ? pPartition->node.pOutputDataBlockDesc->dataBlockId : taosRand(), pPartition->pExprs);
  qptMakeColumnList(&pPartition->pPartitionKeys);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnList(&pPartition->pPartitionKeys);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  int16_t blockId = (QPT_CORRECT_HIGH_PROB() && pPartition->node.pOutputDataBlockDesc) ? pPartition->node.pOutputDataBlockDesc->dataBlockId : taosRand();
  qptMakeTargetList(QPT_NODE_EXPR, blockId, &pPartition->pTargets);

  pPartition->needBlockOutputTsOrder = QPT_RAND_BOOL_V;
  pPartition->tsSlotId = qptGetInputPrimaryTsSlotId();
}

SNode* qptCreatePartitionPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SPartitionPhysiNode* pPartition = (SPartitionPhysiNode*)pPhysiNode;

  qptCreatePartitionPhysiNodeImpl(pPartition);
  
  return (SNode*)pPhysiNode;
}

SNode* qptCreateStreamPartitionPhysiNode(int32_t nodeType) {
  SPhysiNode* pPhysiNode = qptCreatePhysiNode(nodeType);

  SStreamPartitionPhysiNode* pPartition = (SStreamPartitionPhysiNode*)pPhysiNode;

  qptCreatePartitionPhysiNodeImpl(&pPartition->part);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_CORRECT_HIGH_PROB() ? true : false, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeColumnList(&pPartition->pTags);

  qptInitMakeNodeCtx(QPT_CORRECT_HIGH_PROB() ? false : true, QPT_RAND_BOOL_V, QPT_RAND_BOOL_V, 0, NULL);
  qptMakeExprNode(&pPartition->pSubtable);

  return (SNode*)pPhysiNode;
}



SNode* qptCreatePhysicalPlanNode(int32_t nodeIdx) {
  if (qptPlans[nodeIdx].buildFunc) {
    return (*qptPlans[nodeIdx].buildFunc)(qptPlans[nodeIdx].type);
  }

  return NULL;
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

  qptResetTableCols();
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

void qptRunSingleOpTest() {
  SNode* pNode = NULL;
  SReadHandle readHandle = {0};
  SOperatorInfo* pOperator = NULL;
  SExecTaskInfo* pTaskInfo = NULL;
  SStorageAPI    storageAPI = {0};

  if (qptCtx.loopIdx > 0) {
    qptResetForReRun();
  }
  
  doCreateTask(qptCtx.param.plan.queryId, qptCtx.param.plan.taskId++, qptCtx.param.vnode.vgId, OPTR_EXEC_MODEL_BATCH, &storageAPI, &pTaskInfo);
  qptCtx.buildCtx.pCurrTask = pTaskInfo;

  pNode = (SNode*)qptCreatePhysicalPlanNode(qptCtx.param.plan.subplanIdx[0]);
  
  qptPrintBeginInfo();

  qptCtx.startTsUs = taosGetTimestampUs();
  //qptCtx.result.code = createTagScanOperatorInfo(&readHandle, (STagScanPhysiNode*)pNode, NULL, NULL, NULL, pTaskInfo, &pOperator);
  //qptCtx.result.code = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pNode, pTaskInfo, &pOperator);

  doDestroyTask(pTaskInfo);
  destroyOperator(pOperator);
  nodesDestroyNode((SNode*)pNode);

  qptPrintEndInfo();

  qptHandleTestEnd();
}

void qptRunSubplanTest() {
  SNode* pNode = NULL;
  SReadHandle readHandle = {0};
  SOperatorInfo* pOperator = NULL;

  if (qptCtx.loopIdx > 0) {
    qptResetForReRun();
  }

  //pNode = (SNode*)qptCreatePhysicalPlanNode(qptCtx.param.plan.subplanType[0]);
  
  qptPrintBeginInfo();

  qptCtx.startTsUs = taosGetTimestampUs();
  //qptCtx.result.code = createTagScanOperatorInfo(&readHandle, (STagScanPhysiNode*)pNode, NULL, NULL, NULL, NULL, &pOperator);
  //qptCtx.result.code = createProjectOperatorInfo(NULL, (SProjectPhysiNode*)pNode, NULL, &pOperator);

  destroyOperator(pOperator);
  nodesDestroyNode((SNode*)pNode);

  qptPrintEndInfo();

  qptHandleTestEnd();
}


void qptRunPlanTest() {
  if (qptCtx.param.plan.singlePhysiNode) {
    qptRunSingleOpTest();
  } else {
    qptRunSubplanTest();
  }
}

SQPTNodeParam* qptInitNodeParam(int32_t nodeType) {
  return NULL;
}

void qptInitTableCols(SNodeList** ppList, int32_t colNum, EColumnType colType) {
  SQPTCol* pCol = NULL;
  int32_t tbnameIdx = -1;
  if (QPT_RAND_BOOL_V && COLUMN_TYPE_TAG == colType) {
    tbnameIdx = taosRand() % colNum;
  }
  
  for (int32_t i = 0; i < colNum; ++i) {
    qptNodesCalloc(1, sizeof(SQPTCol), (void**)&pCol);
    pCol->type = QPT_QUERY_NODE_COL;
    
    if (tbnameIdx >= 0 && i == tbnameIdx) {
      strcpy(pCol->name, "tbname");
      pCol->dtype = TSDB_DATA_TYPE_VARCHAR;
      pCol->len = qptGetColumnRandLen(pCol->dtype);
      pCol->inUse = 0;
      pCol->hasIndex = QPT_RAND_BOOL_V;
      pCol->isPrimTs = QPT_RAND_BOOL_V;
      pCol->isPk = QPT_RAND_BOOL_V;
      pCol->colType = COLUMN_TYPE_TBNAME;

      qptNodesListMakeStrictAppend(ppList, (SNode *)pCol);
      continue;
    }

    qptInitSingleTableCol(pCol, i, colType);
    
    qptNodesListMakeStrictAppend(ppList, (SNode *)pCol);
  }
}

void qptInitTestCtx(bool correctExpected, bool singleNode, int32_t nodeType, int32_t nodeIdx, int32_t paramNum, SQPTNodeParam* nodeParam) {
  qptCtx.param.correctExpected = correctExpected;
  qptCtx.param.schedulerId = taosRand();
  qptCtx.param.plan.singlePhysiNode = singleNode;

  if (singleNode) {
    qptCtx.param.plan.subplanMaxLevel = 1;
    qptCtx.param.plan.subplanType[0] = nodeType;
    qptCtx.param.plan.subplanIdx[0] = nodeIdx;
  } else {
    qptCtx.param.plan.subplanMaxLevel = taosRand() % QPT_MAX_SUBPLAN_LEVEL + 1;
    for (int32_t i = 0; i < qptCtx.param.plan.subplanMaxLevel; ++i) {
      nodeIdx = taosRand() % QPT_PHYSIC_NODE_NUM();
      qptCtx.param.plan.subplanType[i] = qptPlans[nodeIdx].type;
      qptCtx.param.plan.subplanIdx[i] = nodeIdx;
    }
  }

  if (paramNum > 0) {
    qptCtx.param.plan.physiNodeParamNum = paramNum;
    qptCtx.param.plan.physicNodeParam = nodeParam;
  }

  qptCtx.param.db.precision = TSDB_TIME_PRECISION_MILLI;
  strcpy(qptCtx.param.db.dbName, "qptdb1");

  qptCtx.param.vnode.vnodeNum = QPT_DEFAULT_VNODE_NUM;
  qptCtx.param.vnode.vgId = 1;
  qptCtx.param.vnode.epSet.numOfEps = 1;
  qptCtx.param.vnode.epSet.inUse = 0;
  strcpy(qptCtx.param.vnode.epSet.eps[0].fqdn, "127.0.0.1");
  qptCtx.param.vnode.epSet.eps[0].port = 6030;

  qptCtx.param.tbl.uid = 100;
  qptCtx.param.tbl.suid = 1;
  qptGetRandRealTableType(&qptCtx.param.tbl.tblType);
  qptCtx.param.tbl.colNum = taosRand() % 4096 + 1;
  qptCtx.param.tbl.tagNum = taosRand() % 128 + 1;
  qptCtx.param.tbl.pkNum = taosRand() % 2;
  strcpy(qptCtx.param.tbl.tblName, "qpttbl1");
  strcpy(qptCtx.param.tbl.tblName, "tbl1");
  
  qptInitTableCols(&qptCtx.param.tbl.pColList, qptCtx.param.tbl.colNum, COLUMN_TYPE_COLUMN);
  qptInitTableCols(&qptCtx.param.tbl.pTagList, qptCtx.param.tbl.tagNum, COLUMN_TYPE_TAG);

  SNode* pTmp = NULL;
  FOREACH(pTmp, qptCtx.param.tbl.pColList) {
    qptNodesListMakeStrictAppend(&qptCtx.param.tbl.pColTagList, pTmp);
  }
  FOREACH(pTmp, qptCtx.param.tbl.pTagList) {
    qptNodesListMakeStrictAppend(&qptCtx.param.tbl.pColTagList, pTmp);
  }
}

void qptDestroyTestCtx() {
  SNode* pTmp = NULL;
  FOREACH(pTmp, qptCtx.param.tbl.pColList) {
    qptNodesFree(pTmp);
  }
  FOREACH(pTmp, qptCtx.param.tbl.pTagList) {
    qptNodesFree(pTmp);
  }
  nodesClearList(qptCtx.param.tbl.pColList);
  nodesClearList(qptCtx.param.tbl.pTagList);
  nodesClearList(qptCtx.param.tbl.pColTagList);

  qptCtx.param.tbl.pColList = NULL;
  qptCtx.param.tbl.pTagList = NULL;
  qptCtx.param.tbl.pColTagList = NULL;
}

}  // namespace

#if 1
#if 1
TEST(singleNodeTest, randPlan) {
  char* caseType = "singleNodeTest:randPlan";

  for (qptCtx.loopIdx = 0; qptCtx.loopIdx < QPT_MAX_LOOP; ++qptCtx.loopIdx) {
    for (int32_t i = 0; i < sizeof(qptPlans)/sizeof(qptPlans[0]); ++i) {
      sprintf(qptCtx.caseName, "%s:%s", caseType, qptPlans[i].name);
      qptInitTestCtx(false, true, qptPlans[i].type, i, 0, NULL);
    
      qptRunPlanTest();

      qptDestroyTestCtx();
    }
  }

  qptPrintStatInfo(); 
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
