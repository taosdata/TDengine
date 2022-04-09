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

#include "tglobal.h"
#include "filter.h"
#include "function.h"
#include "os.h"
#include "querynodes.h"
#include "tname.h"
#include "vnode.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorimpl.h"
#include "query.h"
#include "tcompare.h"
#include "thash.h"
#include "vnode.h"
#include "ttypes.h"

#define SET_REVERSE_SCAN_FLAG(_info) ((_info)->scanFlag = REVERSE_SCAN)
#define SWITCH_ORDER(n)           (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))


void switchCtxOrder(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SWITCH_ORDER(pCtx[i].order);
  }
}

static void setupQueryRangeForReverseScan(STableScanInfo* pTableScanInfo) {
#if 0
  int32_t numOfGroups = (int32_t)(GET_NUM_OF_TABLEGROUP(pRuntimeEnv));
  for(int32_t i = 0; i < numOfGroups; ++i) {
    SArray *group = GET_TABLEGROUP(pRuntimeEnv, i);
    SArray *tableKeyGroup = taosArrayGetP(pQueryAttr->tableGroupInfo.pGroupList, i);

    size_t t = taosArrayGetSize(group);
    for (int32_t j = 0; j < t; ++j) {
      STableQueryInfo *pCheckInfo = taosArrayGetP(group, j);
      updateTableQueryInfoForReverseScan(pCheckInfo);

      // update the last key in tableKeyInfo list, the tableKeyInfo is used to build the tsdbQueryHandle and decide
      // the start check timestamp of tsdbQueryHandle
//      STableKeyInfo *pTableKeyInfo = taosArrayGet(tableKeyGroup, j);
//      pTableKeyInfo->lastKey = pCheckInfo->lastKey;
//
//      assert(pCheckInfo->pTable == pTableKeyInfo->pTable);
    }
  }
#endif
}

int32_t loadDataBlock(SExecTaskInfo* pTaskInfo, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock, uint32_t* status) {
  STaskCostInfo* pCost = &pTaskInfo->cost;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;

  pCost->totalCheckedRows += pBlock->info.rows;
  pCost->loadBlocks += 1;

  *status = BLK_DATA_ALL_NEEDED;

  SArray* pCols = tsdbRetrieveDataBlock(pTableScanInfo->dataReader, NULL);
  if (pCols == NULL) {
    return terrno;
  }

  int32_t numOfCols = pBlock->info.numOfCols;
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* p = taosArrayGet(pCols, i);
    SColMatchInfo*   pColMatchInfo = taosArrayGet(pTableScanInfo->pColMatchInfo, i);
    if (!pColMatchInfo->output) {
      continue;
    }

    ASSERT(pColMatchInfo->colId == p->info.colId);
    taosArraySet(pBlock->pDataBlock, pColMatchInfo->targetSlotId, p);
  }

  doFilter(pTableScanInfo->pFilterNode, pBlock);
  return TSDB_CODE_SUCCESS;
}

static void setupEnvForReverseScan(STableScanInfo* pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  // reverse order time range
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);

  switchCtxOrder(pCtx, numOfOutput);
  SWITCH_ORDER(pTableScanInfo->order);
  setupQueryRangeForReverseScan(pTableScanInfo);

  pTableScanInfo->times = 1;
  pTableScanInfo->current = 0;
  pTableScanInfo->reverseTimes = 0;
}

static SSDataBlock* doTableScanImpl(SOperatorInfo* pOperator, bool* newgroup) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  SSDataBlock*     pBlock = &pTableScanInfo->block;
  STableGroupInfo* pTableGroupInfo = &pOperator->pTaskInfo->tableqinfoGroupInfo;

  *newgroup = false;

  while (tsdbNextDataBlock(pTableScanInfo->dataReader)) {
    if (isTaskKilled(pOperator->pTaskInfo)) {
      longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    pTableScanInfo->numOfBlocks += 1;
    tsdbRetrieveDataBlockInfo(pTableScanInfo->dataReader, &pBlock->info);

    // todo opt
    //    if (pTableGroupInfo->numOfTables > 1 || (pRuntimeEnv->current == NULL && pTableGroupInfo->numOfTables == 1)) {
    //      STableQueryInfo** pTableQueryInfo =
    //          (STableQueryInfo**)taosHashGet(pTableGroupInfo->map, &pBlock->info.uid, sizeof(pBlock->info.uid));
    //      if (pTableQueryInfo == NULL) {
    //        break;
    //      }
    //
    //      doTableQueryInfoTimeWindowCheck(pTaskInfo, *pTableQueryInfo, pTableScanInfo->order);
    //    }

    // this function never returns error?
    uint32_t status = BLK_DATA_ALL_NEEDED;
    int32_t  code = loadDataBlock(pTaskInfo, pTableScanInfo, pBlock, &status);
    //    int32_t  code = loadDataBlockOnDemand(pOperator->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pOperator->pTaskInfo->env, code);
    }

    // current block is ignored according to filter result by block statistics data, continue load the next block
    if (status == BLK_DATA_DISCARD || pBlock->info.rows == 0) {
      continue;
    }

    return pBlock;
  }

  return NULL;
}

static SSDataBlock* doTableScan(SOperatorInfo* pOperator, bool* newgroup) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  // The read handle is not initialized yet, since no qualified tables exists
  if (pTableScanInfo->dataReader == NULL) {
    return NULL;
  }

  SResultRowInfo* pResultRowInfo = pTableScanInfo->pResultRowInfo;
  *newgroup = false;

  while (pTableScanInfo->current < pTableScanInfo->times) {
    SSDataBlock* p = doTableScanImpl(pOperator, newgroup);
    if (p != NULL) {
      return p;
    }

    if (++pTableScanInfo->current >= pTableScanInfo->times) {
      if (pTableScanInfo->reverseTimes <= 0 /* || isTsdbCacheLastRow(pTableScanInfo->pTsdbReadHandle)*/) {
        return NULL;
      } else {
        break;
      }
    }

    // do prepare for the next round table scan operation
    //    STsdbQueryCond cond = createTsdbQueryCond(pQueryAttr, &pQueryAttr->window);
    //    tsdbResetQueryHandle(pTableScanInfo->pTsdbReadHandle, &cond);

    setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
    pTableScanInfo->scanFlag = REPEAT_SCAN;

    if (pResultRowInfo->size > 0) {
      pResultRowInfo->curPos = 0;
    }

    qDebug("%s start to repeat scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           GET_TASKID(pTaskInfo), pTaskInfo->window.skey, pTaskInfo->window.ekey);
  }

  SSDataBlock* p = NULL;
  // todo refactor
  if (pTableScanInfo->reverseTimes > 0) {
    setupEnvForReverseScan(pTableScanInfo, pTableScanInfo->pCtx, pTableScanInfo->numOfOutput);
    //    STsdbQueryCond cond = createTsdbQueryCond(pQueryAttr, &pQueryAttr->window);
    //    tsdbResetQueryHandle(pTableScanInfo->pTsdbReadHandle, &cond);

    qDebug("%s start to reverse scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           GET_TASKID(pTaskInfo), pTaskInfo->window.skey, pTaskInfo->window.ekey);

    if (pResultRowInfo->size > 0) {
      pResultRowInfo->curPos = pResultRowInfo->size - 1;
    }

    p = doTableScanImpl(pOperator, newgroup);
  }

  return p;
}

SOperatorInfo* createTableScanOperatorInfo(void* pTsdbReadHandle, int32_t order, int32_t numOfOutput,
                                           int32_t repeatTime, int32_t reverseTime, SArray* pColMatchInfo,
                                           SNode* pCondition, SExecTaskInfo* pTaskInfo) {
  assert(repeatTime > 0);

  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);

    pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->block.pDataBlock = taosArrayInit(numOfOutput, sizeof(SColumnInfoData));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColumnInfoData idata = {0};
    taosArrayPush(pInfo->block.pDataBlock, &idata);
  }

  pInfo->pFilterNode      = pCondition;
  pInfo->dataReader       = pTsdbReadHandle;
  pInfo->times            = repeatTime;
  pInfo->reverseTimes     = reverseTime;
  pInfo->order            = order;
  pInfo->current          = 0;
  pInfo->scanFlag         = MAIN_SCAN;
  pInfo->pColMatchInfo    = pColMatchInfo;
  pOperator->name         = "TableScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = numOfOutput;
  pOperator->getNextFn    = doTableScan;
  pOperator->pTaskInfo    = pTaskInfo;

  static int32_t cost = 0;
  pOperator->cost.openCost = ++cost;
  pOperator->cost.totalCost = ++cost;
  pOperator->resultInfo.totalRows = ++cost;

  return pOperator;
}

SOperatorInfo* createTableSeqScanOperatorInfo(void* pTsdbReadHandle, STaskRuntimeEnv* pRuntimeEnv) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));

  pInfo->dataReader = pTsdbReadHandle;
  pInfo->times = 1;
  pInfo->reverseTimes = 0;
  pInfo->order = pRuntimeEnv->pQueryAttr->order.order;
  pInfo->current = 0;
  pInfo->prevGroupId = -1;
  pRuntimeEnv->enableGroupData = true;

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->name = "TableSeqScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfOutput = pRuntimeEnv->pQueryAttr->numOfCols;
  pOperator->pRuntimeEnv = pRuntimeEnv;
  pOperator->getNextFn = doTableScanImpl;

  return pOperator;
}

static SSDataBlock* doBlockInfoScan(SOperatorInfo* pOperator, bool* newgroup) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  STableScanInfo* pTableScanInfo = pOperator->info;
  *newgroup = false;

  STableBlockDistInfo tableBlockDist = {0};
  tableBlockDist.numOfTables = 1;  // TODO set the correct number of tables

  int32_t numRowSteps = TSDB_DEFAULT_MAX_ROW_FBLOCK / TSDB_BLOCK_DIST_STEP_ROWS;
  if (TSDB_DEFAULT_MAX_ROW_FBLOCK % TSDB_BLOCK_DIST_STEP_ROWS != 0) {
    ++numRowSteps;
  }

  tableBlockDist.dataBlockInfos  = taosArrayInit(numRowSteps, sizeof(SFileBlockInfo));
  taosArraySetSize(tableBlockDist.dataBlockInfos, numRowSteps);

  tableBlockDist.maxRows = INT_MIN;
  tableBlockDist.minRows = INT_MAX;

  tsdbGetFileBlocksDistInfo(pTableScanInfo->dataReader, &tableBlockDist);
  tableBlockDist.numOfRowsInMemTable = (int32_t) tsdbGetNumOfRowsInMemTable(pTableScanInfo->dataReader);

  SSDataBlock* pBlock = &pTableScanInfo->block;
  pBlock->info.rows   = 1;
  pBlock->info.numOfCols = 1;

//  SBufferWriter bw = tbufInitWriter(NULL, false);
//  blockDistInfoToBinary(&tableBlockDist, &bw);
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, 0);

//  int32_t len = (int32_t) tbufTell(&bw);
//  pColInfo->pData = taosMemoryMalloc(len + sizeof(int32_t));
//  *(int32_t*) pColInfo->pData = len;
//  memcpy(pColInfo->pData + sizeof(int32_t), tbufGetData(&bw, false), len);
//
//  tbufCloseWriter(&bw);

//  SArray* g = GET_TABLEGROUP(pOperator->, 0);
//  pOperator->pRuntimeEnv->current = taosArrayGetP(g, 0);

  pOperator->status = OP_EXEC_DONE;
  return pBlock;
}

SOperatorInfo* createDataBlockInfoScanOperator(void* dataReader, SExecTaskInfo* pTaskInfo) {
  STableScanInfo* pInfo    = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->dataReader       = dataReader;
  pInfo->block.pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type  = TSDB_DATA_TYPE_BINARY;
  infoData.info.bytes = 1024;
  infoData.info.colId = 0;
  taosArrayPush(pInfo->block.pDataBlock, &infoData);

  pOperator->name          = "DataBlockInfoScanOperator";
  //  pOperator->operatorType = OP_TableBlockInfoScan;
  pOperator->blockingOptr  = false;
  pOperator->status        = OP_NOT_OPENED;
  pOperator->_openFn       = operatorDummyOpenFn;
  pOperator->getNextFn     = doBlockInfoScan;

  pOperator->info          = pInfo;
  pOperator->pTaskInfo     = pTaskInfo;

  return pOperator;

  _error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}

static void doClearBufferedBlocks(SStreamBlockScanInfo* pInfo) {
  size_t total = taosArrayGetSize(pInfo->pBlockLists);

  pInfo->validBlockIndex = 0;
  for (int32_t i = 0; i < total; ++i) {
    SSDataBlock* p = taosArrayGetP(pInfo->pBlockLists, i);
    blockDataDestroy(p);
  }
  taosArrayClear(pInfo->pBlockLists);
}

static SSDataBlock* doStreamBlockScan(SOperatorInfo* pOperator, bool* newgroup) {
  // NOTE: this operator does never check if current status is done or not
  SExecTaskInfo*        pTaskInfo = pOperator->pTaskInfo;
  SStreamBlockScanInfo* pInfo = pOperator->info;

  pTaskInfo->code = pOperator->_openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  if (pInfo->blockType == STREAM_DATA_TYPE_SSDATA_BLOCK) {
    size_t total = taosArrayGetSize(pInfo->pBlockLists);
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      return NULL;
    }

    int32_t current = pInfo->validBlockIndex++;
    return taosArrayGetP(pInfo->pBlockLists, current);
  } else {
    SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
    blockDataCleanup(pInfo->pRes);

    while (tqNextDataBlock(pInfo->readerHandle)) {
      pTaskInfo->code = tqRetrieveDataBlockInfo(pInfo->readerHandle, pBlockInfo);
      if (pTaskInfo->code != TSDB_CODE_SUCCESS) {
        terrno = pTaskInfo->code;
        return NULL;
      }

      if (pBlockInfo->rows == 0) {
        return NULL;
      }

      SArray* pCols = tqRetrieveDataBlock(pInfo->readerHandle);

      int32_t numOfCols = pInfo->pRes->info.numOfCols;
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData* p = taosArrayGet(pCols, i);
        SColMatchInfo*   pColMatchInfo = taosArrayGet(pInfo->pColMatchInfo, i);
        if (!pColMatchInfo->output) {
          continue;
        }

        ASSERT(pColMatchInfo->colId == p->info.colId);
        taosArraySet(pInfo->pRes->pDataBlock, pColMatchInfo->targetSlotId, p);
      }

      if (pInfo->pRes->pDataBlock == NULL) {
        // TODO add log
        pTaskInfo->code = terrno;
        return NULL;
      }

      break;
    }

    // record the scan action.
    pInfo->numOfExec++;
    pInfo->numOfRows += pBlockInfo->rows;

    return (pBlockInfo->rows == 0) ? NULL : pInfo->pRes;
  }
}

SOperatorInfo* createStreamScanOperatorInfo(void* streamReadHandle, SSDataBlock* pResBlock, SArray* pColList, SArray* pTableIdList, SExecTaskInfo* pTaskInfo) {
  SStreamBlockScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamBlockScanInfo));
  SOperatorInfo*        pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t numOfOutput = taosArrayGetSize(pColList);

  SArray* pColIds = taosArrayInit(4, sizeof(int16_t));
  for(int32_t i = 0; i < numOfOutput; ++i) {
    int16_t* id = taosArrayGet(pColList, i);
    taosArrayPush(pColIds, id);
  }

  pInfo->pColMatchInfo = pColList;

  // set the extract column id to streamHandle
  tqReadHandleSetColIdList((STqReadHandle*)streamReadHandle, pColIds);
  int32_t code = tqReadHandleSetTbUidList(streamReadHandle, pTableIdList);
  if (code != 0) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);
    return NULL;
  }

  pInfo->pBlockLists = taosArrayInit(4, POINTER_BYTES);
  if (pInfo->pBlockLists == NULL) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);
    return NULL;
  }

  pInfo->readerHandle = streamReadHandle;
  pInfo->pRes = pResBlock;

  pOperator->name         = "StreamBlockScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status       = OP_NOT_OPENED;
  pOperator->info         = pInfo;
  pOperator->numOfOutput  = pResBlock->info.numOfCols;
  pOperator->_openFn      = operatorDummyOpenFn;
  pOperator->getNextFn    = doStreamBlockScan;
  pOperator->closeFn      = operatorDummyCloseFn;
  pOperator->pTaskInfo    = pTaskInfo;

  return pOperator;
}

static void destroySysScanOperator(void* param, int32_t numOfOutput) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  tsem_destroy(&pInfo->ready);
  blockDataDestroy(pInfo->pRes);

  if (pInfo->type == TSDB_MGMT_TABLE_TABLE) {
    metaCloseTbCursor(pInfo->pCur);
  }
}

EDealRes getDBNameFromConditionWalker(SNode* pNode, void* pContext) {
  int32_t   code = TSDB_CODE_SUCCESS;
  ENodeType nType = nodeType(pNode);

  switch (nType) {
    case QUERY_NODE_OPERATOR: {
      SOperatorNode* node = (SOperatorNode*)pNode;

      if (OP_TYPE_EQUAL == node->opType) {
        *(int32_t*)pContext = 1;
        return DEAL_RES_CONTINUE;
      }

      *(int32_t*)pContext = 0;

      return DEAL_RES_IGNORE_CHILD;
    }
    case QUERY_NODE_COLUMN: {
      if (1 != *(int32_t*)pContext) {
        return DEAL_RES_CONTINUE;
      }

      SColumnNode* node = (SColumnNode*)pNode;
      if (TSDB_INS_USER_STABLES_DBNAME_COLID == node->colId) {
        *(int32_t*)pContext = 2;
        return DEAL_RES_CONTINUE;
      }

      *(int32_t*)pContext = 0;
      return DEAL_RES_CONTINUE;
    }
    case QUERY_NODE_VALUE: {
      if (2 != *(int32_t*)pContext) {
        return DEAL_RES_CONTINUE;
      }

      SValueNode* node = (SValueNode*)pNode;
      char*       dbName = nodesGetValueFromNode(node);
      strncpy(pContext, varDataVal(dbName), varDataLen(dbName));
      *((char*)pContext + varDataLen(dbName)) = 0;
      return DEAL_RES_ERROR;  // stop walk
    }
    default:
      break;
  }

  return DEAL_RES_CONTINUE;
}

void getDBNameFromCondition(SNode* pCondition, char* dbName) {
  if (NULL == pCondition) {
    return;
  }

  nodesWalkExpr(pCondition, getDBNameFromConditionWalker, dbName);
}

static int32_t loadSysTableContentCb(void* param, const SDataBuf* pMsg, int32_t code) {
  SOperatorInfo*     operator=(SOperatorInfo*) param;
  SSysTableScanInfo* pScanResInfo = (SSysTableScanInfo*)operator->info;
  if (TSDB_CODE_SUCCESS == code) {
    pScanResInfo->pRsp = pMsg->pData;

    SRetrieveMetaTableRsp* pRsp = pScanResInfo->pRsp;
    pRsp->numOfRows = htonl(pRsp->numOfRows);
    pRsp->useconds  = htobe64(pRsp->useconds);
    pRsp->handle    = htobe64(pRsp->handle);
    pRsp->compLen   = htonl(pRsp->compLen);
  } else {
    operator->pTaskInfo->code = code;
  }

  tsem_post(&pScanResInfo->ready);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doFilterResult(SSysTableScanInfo* pInfo) {
  if (pInfo->pCondition == NULL) {
    return pInfo->pRes->info.rows == 0 ? NULL : pInfo->pRes;
  }

  SFilterInfo* filter = NULL;
  int32_t      code = filterInitFromNode(pInfo->pCondition, &filter, 0);

  SFilterColumnParam param1 = {.numOfCols = pInfo->pRes->info.numOfCols, .pDataBlock = pInfo->pRes->pDataBlock};
  code = filterSetDataFromSlotId(filter, &param1);

  int8_t* rowRes = NULL;
  bool    keep = filterExecute(filter, pInfo->pRes, &rowRes, NULL, param1.numOfCols);
  filterFreeInfo(filter);

  SSDataBlock* px = createOneDataBlock(pInfo->pRes);
  blockDataEnsureCapacity(px, pInfo->pRes->info.rows);

  // TODO refactor
  int32_t numOfRow = 0;
  for (int32_t i = 0; i < pInfo->pRes->info.numOfCols; ++i) {
    SColumnInfoData* pDest = taosArrayGet(px->pDataBlock, i);
    SColumnInfoData* pSrc = taosArrayGet(pInfo->pRes->pDataBlock, i);

    if (keep) {
      colDataAssign(pDest, pSrc, pInfo->pRes->info.rows);
      numOfRow = pInfo->pRes->info.rows;
    } else if (NULL != rowRes) {
      numOfRow = 0;
      for (int32_t j = 0; j < pInfo->pRes->info.rows; ++j) {
        if (rowRes[j] == 0) {
          continue;
        }
      
        colDataAppend(pDest, numOfRow, colDataGetData(pSrc, j), false);
        numOfRow += 1;
      }
    } else {
      numOfRow = 0;
    }
  }

  px->info.rows = numOfRow;
  pInfo->pRes = px;

  return pInfo->pRes->info.rows == 0 ? NULL : pInfo->pRes;
}

static SSDataBlock* doSysTableScan(SOperatorInfo* pOperator, bool* newgroup) {
  // build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  // retrieve local table list info from vnode
  if (pInfo->type == TSDB_MGMT_TABLE_TABLE) {
    if (pInfo->pCur == NULL) {
      pInfo->pCur = metaOpenTbCursor(pInfo->readHandle);
    }

    blockDataCleanup(pInfo->pRes);

    int32_t          tableNameSlotId = 1;
    SColumnInfoData* pTableNameCol = taosArrayGet(pInfo->pRes->pDataBlock, tableNameSlotId);

    char*   name = NULL;
    int32_t numOfRows = 0;

    char n[TSDB_TABLE_NAME_LEN] = {0};
    while ((name = metaTbCursorNext(pInfo->pCur)) != NULL) {
      STR_TO_VARSTR(n, name);
      colDataAppend(pTableNameCol, numOfRows, n, false);
      numOfRows += 1;
      if (numOfRows >= pInfo->capacity) {
        break;
      }

      for (int32_t i = 0; i < pInfo->pRes->info.numOfCols; ++i) {
        if (i == tableNameSlotId) {
          continue;
        }

        SColumnInfoData* pColInfoData = taosArrayGet(pInfo->pRes->pDataBlock, i);
        int64_t          tmp = 0;
        char             t[10] = {0};
        STR_TO_VARSTR(t, "_");  //TODO
        if (IS_VAR_DATA_TYPE(pColInfoData->info.type)) {
          colDataAppend(pColInfoData, numOfRows, t, false);
        } else {
          colDataAppend(pColInfoData, numOfRows, (char*)&tmp, false);
        }
      }
    }

    pInfo->loadInfo.totalRows += numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    //    pInfo->elapsedTime;
    //    pInfo->totalBytes;
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  } else {  // load the meta from mnode of the given epset
    if (pOperator->status == OP_EXEC_DONE) {
      return NULL;
    }

    int64_t startTs = taosGetTimestampUs();

    pInfo->req.type = pInfo->type;
    strncpy(pInfo->req.tb, tNameGetTableName(&pInfo->name), tListLen(pInfo->req.tb));
    if (pInfo->showRewrite) {
      char dbName[TSDB_DB_NAME_LEN] = {0};
      getDBNameFromCondition(pInfo->pCondition, dbName);
      sprintf(pInfo->req.db, "%d.%s", pInfo->accountId, dbName);
    }

    int32_t contLen = tSerializeSRetrieveTableReq(NULL, 0, &pInfo->req);
    char*   buf1 = taosMemoryCalloc(1, contLen);
    tSerializeSRetrieveTableReq(buf1, contLen, &pInfo->req);

    // send the fetch remote task result reques
    SMsgSendInfo* pMsgSendInfo = taosMemoryCalloc(1, sizeof(SMsgSendInfo));
    if (NULL == pMsgSendInfo) {
      qError("%s prepare message %d failed", GET_TASKID(pTaskInfo), (int32_t)sizeof(SMsgSendInfo));
      pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
      return NULL;
    }

    pMsgSendInfo->param = pOperator;
    pMsgSendInfo->msgInfo.pData = buf1;
    pMsgSendInfo->msgInfo.len = contLen;
    pMsgSendInfo->msgType = TDMT_MND_SYSTABLE_RETRIEVE;
    pMsgSendInfo->fp = loadSysTableContentCb;

    int64_t transporterId = 0;
    int32_t code = asyncSendMsgToServer(pInfo->pTransporter, &pInfo->epSet, &transporterId, pMsgSendInfo);
    tsem_wait(&pInfo->ready);

    if (pTaskInfo->code) {
      return NULL;
    }

    SRetrieveMetaTableRsp* pRsp = pInfo->pRsp;
    pInfo->req.showId = pRsp->handle;

    if (pRsp->numOfRows == 0 || pRsp->completed) {
      pOperator->status = OP_EXEC_DONE;
    }

    if (pRsp->numOfRows == 0) {
      //      qDebug("%s vgId:%d, taskID:0x%"PRIx64" %d of total completed, rowsOfSource:%"PRIu64", totalRows:%"PRIu64"
      //      try next",
      //             GET_TASKID(pTaskInfo), pSource->addr.nodeId, pSource->taskId, pExchangeInfo->current + 1,
      //             pDataInfo->totalRows, pExchangeInfo->totalRows);
      return NULL;
    }

    SRetrieveMetaTableRsp* pTableRsp = pInfo->pRsp;
    setSDataBlockFromFetchRsp(pInfo->pRes, &pInfo->loadInfo, pTableRsp->numOfRows, pTableRsp->data, pTableRsp->compLen,
                              pOperator->numOfOutput, startTs, NULL, pInfo->scanCols);

    return doFilterResult(pInfo);
  }

  return NULL;
}

SOperatorInfo* createSysTableScanOperatorInfo(void* pSysTableReadHandle, SSDataBlock* pResBlock, const SName* pName,
                                              SNode* pCondition, SEpSet epset, SArray* colList,
                                              SExecTaskInfo* pTaskInfo, bool showRewrite, int32_t accountId) {
  SSysTableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SSysTableScanInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->accountId = accountId;
  pInfo->showRewrite = showRewrite;
  pInfo->pRes = pResBlock;
  pInfo->capacity = 4096;
  pInfo->pCondition = pCondition;
  pInfo->scanCols = colList;

  // TODO remove it
  int32_t     tableType = 0;
  const char* name = tNameGetTableName(pName);
  if (strncasecmp(name, TSDB_INS_TABLE_USER_DATABASES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_DB;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_USERS, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_USER;
  } else if (strncasecmp(name, TSDB_INS_TABLE_DNODES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_DNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_MNODES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_MNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_MODULES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_MODULE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_QNODES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_QNODE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_FUNCTIONS, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_FUNC;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_INDEXES, tListLen(pName->tname)) == 0) {
    //    tableType = TSDB_MGMT_TABLE_INDEX;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_STABLES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_STB;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_STREAMS, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_STREAMS;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_TABLES, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_TABLE;
  } else if (strncasecmp(name, TSDB_INS_TABLE_VGROUPS, tListLen(pName->tname)) == 0) {
    tableType = TSDB_MGMT_TABLE_VGROUP;
  } else if (strncasecmp(name, TSDB_INS_TABLE_USER_TABLE_DISTRIBUTED, tListLen(pName->tname)) == 0) {
    //    tableType = TSDB_MGMT_TABLE_DIST;
  } else {
    ASSERT(0);
  }

  tNameAssign(&pInfo->name, pName);
  pInfo->type = tableType;
  if (pInfo->type == TSDB_MGMT_TABLE_TABLE) {
    pInfo->readHandle = pSysTableReadHandle;
    blockDataEnsureCapacity(pInfo->pRes, pInfo->capacity);
  } else {
    tsem_init(&pInfo->ready, 0, 0);
    pInfo->epSet = epset;

#if 1
    {  // todo refactor
      SRpcInit rpcInit;
      memset(&rpcInit, 0, sizeof(rpcInit));
      rpcInit.localPort = 0;
      rpcInit.label = "DB-META";
      rpcInit.numOfThreads = 1;
      rpcInit.cfp = qProcessFetchRsp;
      rpcInit.sessions = tsMaxConnections;
      rpcInit.connType = TAOS_CONN_CLIENT;
      rpcInit.user = (char*)"root";
      rpcInit.idleTime = tsShellActivityTimer * 1000;
      rpcInit.ckey = "key";
      rpcInit.spi = 1;
      rpcInit.secret = (char*)"dcc5bed04851fec854c035b2e40263b6";

      pInfo->pTransporter = rpcOpen(&rpcInit);
      if (pInfo->pTransporter == NULL) {
        return NULL;  // todo
      }
    }
#endif
  }

  pOperator->name = "SysTableScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfOutput = pResBlock->info.numOfCols;
  pOperator->getNextFn = doSysTableScan;
  pOperator->closeFn = destroySysScanOperator;
  pOperator->pTaskInfo = pTaskInfo;

  return pOperator;
}
