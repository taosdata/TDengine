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

#include "common/ttime.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "tglobal.h"
#include "tname.h"
#include "systable.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "executorimpl.h"
#include "query.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"
#include "vnode.h"

#define SET_REVERSE_SCAN_FLAG(_info) ((_info)->scanFlag = REVERSE_SCAN)
#define SWITCH_ORDER(n)              (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo);

int32_t buildDbTableInfoBlock(const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta, size_t size, const char* dbName);
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

static void getNextTimeWindow(SInterval* pInterval, STimeWindow* tw, int32_t order) {
  int32_t factor = GET_FORWARD_DIRECTION_FACTOR(order);
  if (pInterval->intervalUnit != 'n' && pInterval->intervalUnit != 'y') {
    tw->skey += pInterval->sliding * factor;
    tw->ekey = tw->skey + pInterval->interval - 1;
    return;
  }

  int64_t key = tw->skey, interval = pInterval->interval;
  // convert key to second
  key = convertTimePrecision(key, pInterval->precision, TSDB_TIME_PRECISION_MILLI) / 1000;

  if (pInterval->intervalUnit == 'y') {
    interval *= 12;
  }

  struct tm tm;
  time_t    t = (time_t)key;
  taosLocalTime(&t, &tm);

  int mon = (int)(tm.tm_year * 12 + tm.tm_mon + interval * factor);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->skey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000L, TSDB_TIME_PRECISION_MILLI, pInterval->precision);

  mon = (int)(mon + interval);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->ekey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000L, TSDB_TIME_PRECISION_MILLI, pInterval->precision);

  tw->ekey -= 1;
}

static bool overlapWithTimeWindow(SInterval* pInterval, SDataBlockInfo* pBlockInfo) {
  STimeWindow w = {0};

  // 0 by default, which means it is not a interval operator of the upstream operator.
  if (pInterval->interval == 0) {
    return false;
  }

  // todo handle the time range case
  TSKEY sk = INT64_MIN;
  TSKEY ek = INT64_MAX;
  //  TSKEY sk = MIN(pQueryAttr->window.skey, pQueryAttr->window.ekey);
  //  TSKEY ek = MAX(pQueryAttr->window.skey, pQueryAttr->window.ekey);

  if (true) {
    getAlignQueryTimeWindow(pInterval, pInterval->precision, pBlockInfo->window.skey, &w);
    assert(w.ekey >= pBlockInfo->window.skey);

    if (w.ekey < pBlockInfo->window.ekey) {
      return true;
    }

    while (1) {  // todo handle the desc order scan case
      getNextTimeWindow(pInterval, &w, TSDB_ORDER_ASC);
      if (w.skey > pBlockInfo->window.ekey) {
        break;
      }

      assert(w.ekey > pBlockInfo->window.ekey);
      if (w.skey <= pBlockInfo->window.ekey && w.skey > pBlockInfo->window.skey) {
        return true;
      }
    }
  } else {
    //    getAlignQueryTimeWindow(pQueryAttr, pBlockInfo->window.ekey, sk, ek, &w);
    //    assert(w.skey <= pBlockInfo->window.ekey);
    //
    //    if (w.skey > pBlockInfo->window.skey) {
    //      return true;
    //    }
    //
    //    while(1) {
    //      getNextTimeWindow(pQueryAttr, &w);
    //      if (w.ekey < pBlockInfo->window.skey) {
    //        break;
    //      }
    //
    //      assert(w.skey < pBlockInfo->window.skey);
    //      if (w.ekey < pBlockInfo->window.ekey && w.ekey >= pBlockInfo->window.skey) {
    //        return true;
    //      }
    //    }
  }

  return false;
}

int32_t loadDataBlock(SOperatorInfo* pOperator, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock, uint32_t* status) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  STableScanInfo* pInfo = pOperator->info;

  STaskCostInfo* pCost = &pTaskInfo->cost;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;

  *status = pInfo->dataBlockLoadFlag;
  if (pTableScanInfo->pFilterNode != NULL || overlapWithTimeWindow(&pTableScanInfo->interval, &pBlock->info)) {
    (*status) = FUNC_DATA_REQUIRED_DATA_LOAD;
  }

  SDataBlockInfo* pBlockInfo = &pBlock->info;
  taosMemoryFreeClear(pBlock->pBlockAgg);

  if (*status == FUNC_DATA_REQUIRED_FILTEROUT) {
    qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->filterOutBlocks += 1;
    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_NOT_LOAD) {
    qDebug("%s data block skipped, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->skipBlocks += 1;
    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_STATIS_LOAD) {
    pCost->loadBlockStatis += 1;

    SColumnDataAgg* pColAgg = NULL;
    tsdbRetrieveDataBlockStatisInfo(pTableScanInfo->dataReader, &pColAgg);

    if (pColAgg != NULL) {
      int32_t numOfCols = pBlock->info.numOfCols;

      // todo create this buffer during creating operator
      pBlock->pBlockAgg = taosMemoryCalloc(numOfCols, sizeof(SColumnDataAgg));
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColMatchInfo* pColMatchInfo = taosArrayGet(pTableScanInfo->pColMatchInfo, i);
        if (!pColMatchInfo->output) {
          continue;
        }
        pBlock->pBlockAgg[pColMatchInfo->targetSlotId] = pColAgg[i];
      }

      return TSDB_CODE_SUCCESS;
    } else {  // failed to load the block sma data, data block statistics does not exist, load data block instead
      *status = FUNC_DATA_REQUIRED_DATA_LOAD;
    }
  }

  ASSERT(*status == FUNC_DATA_REQUIRED_DATA_LOAD);

  // todo filter data block according to the block sma data firstly
#if 0
  if (!doFilterByBlockStatistics(pBlock->pBlockStatis, pTableScanInfo->pCtx, pBlockInfo->rows)) {
    pCost->filterOutBlocks += 1;
    qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo), pBlockInfo->window.skey,
           pBlockInfo->window.ekey, pBlockInfo->rows);
    (*status) = FUNC_DATA_REQUIRED_FILTEROUT;
    return TSDB_CODE_SUCCESS;
  }
#endif

  pCost->totalCheckedRows += pBlock->info.rows;
  pCost->loadBlocks += 1;

  SArray* pCols = tsdbRetrieveDataBlock(pTableScanInfo->dataReader, NULL);
  if (pCols == NULL) {
    return terrno;
  }

  relocateColumnData(pBlock, pTableScanInfo->pColMatchInfo, pCols);
  doFilter(pTableScanInfo->pFilterNode, pBlock);
  if (pBlock->info.rows == 0) {
    pCost->filterOutBlocks += 1;
    qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
  }

  return TSDB_CODE_SUCCESS;
}

static void prepareForDescendingScan(STableScanInfo* pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);

  switchCtxOrder(pCtx, numOfOutput);
  //  setupQueryRangeForReverseScan(pTableScanInfo);

  STimeWindow* pTWindow = &pTableScanInfo->cond.twindow;
  TSWAP(pTWindow->skey, pTWindow->ekey);
  pTableScanInfo->cond.order = TSDB_ORDER_DESC;
}

static SSDataBlock* doTableScanImpl(SOperatorInfo* pOperator, bool* newgroup) {
  STableScanInfo* pTableScanInfo = pOperator->info;

  SSDataBlock* pBlock = pTableScanInfo->pResBlock;
  *newgroup = false;

  while (tsdbNextDataBlock(pTableScanInfo->dataReader)) {
    if (isTaskKilled(pOperator->pTaskInfo)) {
      longjmp(pOperator->pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    pTableScanInfo->numOfBlocks += 1;
    tsdbRetrieveDataBlockInfo(pTableScanInfo->dataReader, &pBlock->info);

    uint32_t status = 0;
    int32_t  code = loadDataBlock(pOperator, pTableScanInfo, pBlock, &status);
    //    int32_t  code = loadDataBlockOnDemand(pOperator->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      longjmp(pOperator->pTaskInfo->env, code);
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
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
  if (pTableScanInfo->dataReader == NULL || pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  *newgroup = false;

  while (pTableScanInfo->current < pTableScanInfo->scanInfo.numOfAsc) {
    SSDataBlock* p = doTableScanImpl(pOperator, newgroup);
    if (p != NULL) {
      return p;
    }

    pTableScanInfo->current += 1;

    if (pTableScanInfo->current < pTableScanInfo->scanInfo.numOfAsc) {
      setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
      pTableScanInfo->scanFlag = REPEAT_SCAN;

      STimeWindow* pWin = &pTableScanInfo->cond.twindow;
      qDebug("%s start to repeat ascending order scan data blocks due to query func required, qrange:%" PRId64
             "-%" PRId64,
             GET_TASKID(pTaskInfo), pWin->skey, pWin->ekey);

      // do prepare for the next round table scan operation
      tsdbResetReadHandle(pTableScanInfo->dataReader, &pTableScanInfo->cond);
    }
  }

  int32_t total = pTableScanInfo->scanInfo.numOfAsc + pTableScanInfo->scanInfo.numOfDesc;
  if (pTableScanInfo->current < total) {
    if (pTableScanInfo->cond.order == TSDB_ORDER_ASC) {
      prepareForDescendingScan(pTableScanInfo, pTableScanInfo->pCtx, pTableScanInfo->numOfOutput);
      tsdbResetReadHandle(pTableScanInfo->dataReader, &pTableScanInfo->cond);
    }

    STimeWindow* pWin = &pTableScanInfo->cond.twindow;
    qDebug("%s start to descending order scan data blocks due to query func required, qrange:%" PRId64 "-%" PRId64,
           GET_TASKID(pTaskInfo), pWin->skey, pWin->ekey);

    while (pTableScanInfo->current < total) {
      SSDataBlock* p = doTableScanImpl(pOperator, newgroup);
      if (p != NULL) {
        return p;
      }

      pTableScanInfo->current += 1;

      if (pTableScanInfo->current < pTableScanInfo->scanInfo.numOfAsc) {
        setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
        pTableScanInfo->scanFlag = REPEAT_SCAN;

        qDebug("%s start to repeat descending order scan data blocks due to query func required, qrange:%" PRId64
               "-%" PRId64,
               GET_TASKID(pTaskInfo), pTaskInfo->window.skey, pTaskInfo->window.ekey);

        // do prepare for the next round table scan operation
        tsdbResetReadHandle(pTableScanInfo->dataReader, &pTableScanInfo->cond);
      }
    }
  }

  setTaskStatus(pTaskInfo, TASK_COMPLETED);
  return NULL;
}

SOperatorInfo* createTableScanOperatorInfo(void* pDataReader, SQueryTableDataCond* pCond, int32_t numOfOutput,
                                           int32_t dataLoadFlag, const uint8_t* scanInfo, SArray* pColMatchInfo,
                                           SSDataBlock* pResBlock, SNode* pCondition, SInterval* pInterval,
                                           double sampleRatio, SExecTaskInfo* pTaskInfo) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    taosMemoryFreeClear(pInfo);
    taosMemoryFreeClear(pOperator);

    pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->cond = *pCond;
  pInfo->scanInfo = (SScanInfo){.numOfAsc = scanInfo[0], .numOfDesc = scanInfo[1]};

  pInfo->interval = *pInterval;
  pInfo->sampleRatio = sampleRatio;
  pInfo->dataBlockLoadFlag = dataLoadFlag;
  pInfo->pResBlock = pResBlock;
  pInfo->pFilterNode = pCondition;
  pInfo->dataReader = pDataReader;
  pInfo->current = 0;
  pInfo->scanFlag = MAIN_SCAN;
  pInfo->pColMatchInfo = pColMatchInfo;

  pOperator->name = "TableScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfOutput = numOfOutput;
  pOperator->fpSet.getNextFn = doTableScan;
  pOperator->pTaskInfo = pTaskInfo;

  static int32_t cost = 0;
  pOperator->cost.openCost = ++cost;
  pOperator->cost.totalCost = ++cost;
  pOperator->resultInfo.totalRows = ++cost;

  return pOperator;
}

SOperatorInfo* createTableSeqScanOperatorInfo(void* pTsdbReadHandle) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));

  pInfo->dataReader = pTsdbReadHandle;
  pInfo->current = 0;
  pInfo->prevGroupId = -1;

  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  pOperator->name = "TableSeqScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->fpSet.getNextFn = doTableScanImpl;

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

  int32_t numRowSteps = TSDB_DEFAULT_MAXROWS_FBLOCK / TSDB_BLOCK_DIST_STEP_ROWS;
  if (TSDB_DEFAULT_MAXROWS_FBLOCK % TSDB_BLOCK_DIST_STEP_ROWS != 0) {
    ++numRowSteps;
  }

  tableBlockDist.dataBlockInfos = taosArrayInit(numRowSteps, sizeof(SFileBlockInfo));
  taosArraySetSize(tableBlockDist.dataBlockInfos, numRowSteps);

  tableBlockDist.maxRows = INT_MIN;
  tableBlockDist.minRows = INT_MAX;

  tsdbGetFileBlocksDistInfo(pTableScanInfo->dataReader, &tableBlockDist);
  tableBlockDist.numOfRowsInMemTable = (int32_t)tsdbGetNumOfRowsInMemTable(pTableScanInfo->dataReader);

  SSDataBlock* pBlock = pTableScanInfo->pResBlock;
  pBlock->info.rows = 1;
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
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->dataReader = dataReader;
  //  pInfo->block.pDataBlock = taosArrayInit(1, sizeof(SColumnInfoData));

  SColumnInfoData infoData = {0};
  infoData.info.type = TSDB_DATA_TYPE_BINARY;
  infoData.info.bytes = 1024;
  infoData.info.colId = 0;
  //  taosArrayPush(pInfo->block.pDataBlock, &infoData);

  pOperator->name = "DataBlockInfoScanOperator";
  //  pOperator->operatorType = OP_TableBlockInfoScan;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->fpSet._openFn = operatorDummyOpenFn;
  pOperator->fpSet.getNextFn = doBlockInfoScan;

  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

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
  int32_t rows = 0;

  pTaskInfo->code = pOperator->fpSet._openFn(pOperator);
  if (pTaskInfo->code != TSDB_CODE_SUCCESS || pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  if (pInfo->blockType == STREAM_DATA_TYPE_SSDATA_BLOCK) {
    size_t total = taosArrayGetSize(pInfo->pBlockLists);
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      pOperator->status = OP_EXEC_DONE;
      return NULL;
    }

    int32_t current = pInfo->validBlockIndex++;
    return taosArrayGetP(pInfo->pBlockLists, current);
  } else {
    SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
    blockDataCleanup(pInfo->pRes);

    while (tqNextDataBlock(pInfo->readerHandle)) {
      SArray*  pCols = NULL;
      uint64_t groupId;
      int32_t  numOfRows;
      int16_t  outputCol;
      int32_t  code = tqRetrieveDataBlock(&pCols, pInfo->readerHandle, &groupId, &numOfRows, &outputCol);

      if (code != TSDB_CODE_SUCCESS || numOfRows == 0) {
        pTaskInfo->code = code;
        return NULL;
      }

      pInfo->pRes->info.groupId = groupId;
      pInfo->pRes->info.rows = numOfRows;

      int32_t numOfCols = pInfo->pRes->info.numOfCols;
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColMatchInfo* pColMatchInfo = taosArrayGet(pInfo->pColMatchInfo, i);
        if (!pColMatchInfo->output) {
          continue;
        }

        bool colExists = false;
        for (int32_t j = 0; j < taosArrayGetSize(pCols); ++j) {
          SColumnInfoData* pResCol = taosArrayGet(pCols, j);
          if (pResCol->info.colId == pColMatchInfo->colId) {
            taosArraySet(pInfo->pRes->pDataBlock, pColMatchInfo->targetSlotId, pResCol);
            colExists = true;
            break;
          }
        }

        // the required column does not exists in submit block, let's set it to be all null value
        if (!colExists) {
          SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->targetSlotId);
          colInfoDataEnsureCapacity(pDst, 0, pBlockInfo->rows);
          colDataAppendNNULL(pDst, 0, pBlockInfo->rows);
        }
      }

      if (pInfo->pRes->pDataBlock == NULL) {
        // TODO add log
        pOperator->status = OP_EXEC_DONE;
        pTaskInfo->code = terrno;
        return NULL;
      }
      rows = pBlockInfo->rows;
      doFilter(pInfo->pCondition, pInfo->pRes);

      break;
    }

    // record the scan action.
    pInfo->numOfExec++;
    pInfo->numOfRows += pBlockInfo->rows;

    if (rows == 0) {
      pOperator->status = OP_EXEC_DONE;
    }

    return (rows == 0) ? NULL : pInfo->pRes;
  }
}

SOperatorInfo* createStreamScanOperatorInfo(void* streamReadHandle, SSDataBlock* pResBlock, SArray* pColList,
                                            SArray* pTableIdList, SExecTaskInfo* pTaskInfo, SNode* pCondition) {
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
  for (int32_t i = 0; i < numOfOutput; ++i) {
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
  pInfo->pCondition = pCondition;

  pOperator->name = "StreamBlockScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->numOfOutput = pResBlock->info.numOfCols;
  pOperator->fpSet._openFn = operatorDummyOpenFn;
  pOperator->fpSet.getNextFn = doStreamBlockScan;
  pOperator->fpSet.closeFn = operatorDummyCloseFn;
  pOperator->pTaskInfo = pTaskInfo;

  return pOperator;
}

static void destroySysScanOperator(void* param, int32_t numOfOutput) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  tsem_destroy(&pInfo->ready);
  blockDataDestroy(pInfo->pRes);

  const char* name = tNameGetTableName(&pInfo->name);
  if (strncasecmp(name, TSDB_INS_TABLE_USER_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
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
      return DEAL_RES_END;  // stop walk
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
    pRsp->useconds = htobe64(pRsp->useconds);
    pRsp->handle = htobe64(pRsp->handle);
    pRsp->compLen = htonl(pRsp->compLen);
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

  SSDataBlock* px = createOneDataBlock(pInfo->pRes, false);
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

        if (colDataIsNull_s(pSrc, j)) {
          colDataAppendNULL(pDest, numOfRow);
        } else {
          colDataAppend(pDest, numOfRow, colDataGetData(pSrc, j), false);
        }

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

static SSDataBlock* buildSysTableMetaBlock() {
  SSDataBlock* pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  pBlock->info.numOfCols = 10;
  pBlock->info.hasVarCol = true;

  pBlock->pDataBlock = taosArrayInit(pBlock->info.numOfCols, sizeof(SColumnInfoData));

  SColumnInfoData colInfoData = {0};
  colInfoData.info.colId = 1;
  colInfoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  colInfoData.info.bytes = (TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 2;
  colInfoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  colInfoData.info.bytes = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 3;
  colInfoData.info.type = TSDB_DATA_TYPE_TIMESTAMP;
  colInfoData.info.bytes = 8;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 4;
  colInfoData.info.type = TSDB_DATA_TYPE_INT;
  colInfoData.info.bytes = 4;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 5;
  colInfoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  colInfoData.info.bytes = (TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 6;
  colInfoData.info.type = TSDB_DATA_TYPE_BIGINT;
  colInfoData.info.bytes = 8;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 7;
  colInfoData.info.type = TSDB_DATA_TYPE_INT;
  colInfoData.info.bytes = 4;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 8;
  colInfoData.info.type = TSDB_DATA_TYPE_INT;
  colInfoData.info.bytes = 4;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 9;
  colInfoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  colInfoData.info.bytes = 512 + VARSTR_HEADER_SIZE;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  colInfoData.info.colId = 10;
  colInfoData.info.type = TSDB_DATA_TYPE_VARCHAR;
  colInfoData.info.bytes = 20 + VARSTR_HEADER_SIZE;
  taosArrayPush(pBlock->pDataBlock, &colInfoData);

  return pBlock;
}

static SSDataBlock* doSysTableScan(SOperatorInfo* pOperator, bool* newgroup) {
  // build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  // retrieve local table list info from vnode
  const char* name = tNameGetTableName(&pInfo->name);
  if (strncasecmp(name, TSDB_INS_TABLE_USER_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
    // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
    if (pInfo->readHandle.mnd != NULL) {
      if (pOperator->status == OP_EXEC_DONE) {
        return NULL;
      }

      buildSysDbTableInfo(pInfo);

      doFilterResult(pInfo);
      pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

      pOperator->status = OP_EXEC_DONE;
      return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
    } else {
      if (pInfo->pCur == NULL) {
        pInfo->pCur = metaOpenTbCursor(pInfo->readHandle.meta);
      }

      blockDataCleanup(pInfo->pRes);

      int32_t numOfRows = 0;

      const char* db = NULL;
      int32_t     vgId = 0;
      vnodeGetInfo(pInfo->readHandle.vnode, &db, &vgId);

      SName sn = {0};
      char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

      tNameGetDbName(&sn, varDataVal(dbname));
      varDataSetLen(dbname, strlen(varDataVal(dbname)));

      SSDataBlock* p = buildSysTableMetaBlock();
      blockDataEnsureCapacity(p, pInfo->capacity);

      char n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      while (metaTbCursorNext(pInfo->pCur) == 0) {
        STR_TO_VARSTR(n, pInfo->pCur->mr.me.name);

        // table name
        SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
        colDataAppend(pColInfoData, numOfRows, n, false);

        // database name
        pColInfoData = taosArrayGet(p->pDataBlock, 1);
        colDataAppend(pColInfoData, numOfRows, dbname, false);

        // vgId
        pColInfoData = taosArrayGet(p->pDataBlock, 6);
        colDataAppend(pColInfoData, numOfRows, (char*)&vgId, false);

        // table comment
        // todo: set the correct comment
        pColInfoData = taosArrayGet(p->pDataBlock, 8);
        colDataAppendNULL(pColInfoData, numOfRows);

        char    str[256] = {0};
        int32_t tableType = pInfo->pCur->mr.me.type;
        if (tableType == TSDB_CHILD_TABLE) {
          // create time
          int64_t ts = pInfo->pCur->mr.me.ctbEntry.ctime;
          pColInfoData = taosArrayGet(p->pDataBlock, 2);
          colDataAppend(pColInfoData, numOfRows, (char*)&ts, false);

          SMetaReader mr = {0};
          metaReaderInit(&mr, pInfo->readHandle.meta, 0);
          metaGetTableEntryByUid(&mr, pInfo->pCur->mr.me.ctbEntry.suid);

          // number of columns
          pColInfoData = taosArrayGet(p->pDataBlock, 3);
          colDataAppend(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schema.nCols, false);

          // super table name
          STR_TO_VARSTR(str, mr.me.name);
          pColInfoData = taosArrayGet(p->pDataBlock, 4);
          colDataAppend(pColInfoData, numOfRows, str, false);
          metaReaderClear(&mr);

          // uid
          pColInfoData = taosArrayGet(p->pDataBlock, 5);
          colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);

          // ttl
          pColInfoData = taosArrayGet(p->pDataBlock, 7);
          colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ctbEntry.ttlDays, false);

          STR_TO_VARSTR(str, "CHILD_TABLE");
        } else if (tableType == TSDB_NORMAL_TABLE) {
          // create time
          pColInfoData = taosArrayGet(p->pDataBlock, 2);
          colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ctime, false);

          // number of columns
          pColInfoData = taosArrayGet(p->pDataBlock, 3);
          colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schema.nCols, false);

          // super table name
          pColInfoData = taosArrayGet(p->pDataBlock, 4);
          colDataAppendNULL(pColInfoData, numOfRows);

          // uid
          pColInfoData = taosArrayGet(p->pDataBlock, 5);
          colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);

          // ttl
          pColInfoData = taosArrayGet(p->pDataBlock, 7);
          colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ttlDays, false);

          STR_TO_VARSTR(str, "NORMAL_TABLE");
        }

        pColInfoData = taosArrayGet(p->pDataBlock, 9);
        colDataAppend(pColInfoData, numOfRows, str, false);

        if (++numOfRows >= pInfo->capacity) {
          break;
        }
      }

      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      relocateColumnData(pInfo->pRes, pInfo->scanCols, p->pDataBlock);
      doFilterResult(pInfo);

      pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
      return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
    }
  } else {  // load the meta from mnode of the given epset
    if (pOperator->status == OP_EXEC_DONE) {
      return NULL;
    }

    while (1) {
      int64_t startTs = taosGetTimestampUs();
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
        qDebug("%s load meta data from mnode failed, totalRows:%" PRIu64 ", code:%s", GET_TASKID(pTaskInfo),
               pInfo->loadInfo.totalRows, tstrerror(pTaskInfo->code));
        return NULL;
      }

      SRetrieveMetaTableRsp* pRsp = pInfo->pRsp;
      pInfo->req.showId = pRsp->handle;

      if (pRsp->numOfRows == 0 || pRsp->completed) {
        pOperator->status = OP_EXEC_DONE;
        qDebug("%s load meta data from mnode completed, rowsOfSource:%d, totalRows:%" PRIu64 " ", GET_TASKID(pTaskInfo),
               pRsp->numOfRows, pInfo->loadInfo.totalRows);

        if (pRsp->numOfRows == 0) {
          return NULL;
        }
      }

      SRetrieveMetaTableRsp* pTableRsp = pInfo->pRsp;
      setSDataBlockFromFetchRsp(pInfo->pRes, &pInfo->loadInfo, pTableRsp->numOfRows, pTableRsp->data,
                                pTableRsp->compLen, pOperator->numOfOutput, startTs, NULL, pInfo->scanCols);

      // todo log the filter info
      doFilterResult(pInfo);
      if (pInfo->pRes->info.rows > 0) {
        return pInfo->pRes;
      }
    }
  }
}

int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo) {
  SSDataBlock* p = buildSysTableMetaBlock();
  blockDataEnsureCapacity(p, pInfo->capacity);

  size_t size = 0;
  const SSysTableMeta* pSysDbTableMeta = NULL;

  getInfosDbMeta(&pSysDbTableMeta, &size);
  p->info.rows = buildDbTableInfoBlock(p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB);

  getPerfDbMeta(&pSysDbTableMeta, &size);
  p->info.rows = buildDbTableInfoBlock(p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB);

  relocateColumnData(pInfo->pRes,  pInfo->scanCols, p->pDataBlock);
//  blockDataDestroy(p);  todo handle memory leak

  pInfo->pRes->info.rows = p->info.rows;
  return p->info.rows;
}

int32_t buildDbTableInfoBlock(const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta, size_t size, const char* dbName) {
  char n[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t numOfRows = p->info.rows;

  for(int32_t i = 0; i < size; ++i) {
    const SSysTableMeta* pm = &pSysDbTableMeta[i];

    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);

    STR_TO_VARSTR(n, pm->name);
    colDataAppend(pColInfoData, numOfRows, n, false);

    // database name
    STR_TO_VARSTR(n, dbName);
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    colDataAppend(pColInfoData, numOfRows, n, false);

    // create time
    pColInfoData = taosArrayGet(p->pDataBlock, 2);
    colDataAppendNULL(pColInfoData, numOfRows);

    // number of columns
    pColInfoData = taosArrayGet(p->pDataBlock, 3);
    colDataAppend(pColInfoData, numOfRows, (char*)&pm->colNum, false);

    for(int32_t j = 4; j <= 8; ++j) {
      pColInfoData = taosArrayGet(p->pDataBlock, j);
      colDataAppendNULL(pColInfoData, numOfRows);
    }

    STR_TO_VARSTR(n, "SYSTEM_TABLE");

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    colDataAppend(pColInfoData, numOfRows, n, false);

    numOfRows += 1;
  }

  return numOfRows;
}

SOperatorInfo* createSysTableScanOperatorInfo(void* readHandle, SSDataBlock* pResBlock, const SName* pName,
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

  tNameAssign(&pInfo->name, pName);
  const char* name = tNameGetTableName(&pInfo->name);
  if (strncasecmp(name, TSDB_INS_TABLE_USER_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
    pInfo->readHandle = *(SReadHandle*) readHandle;
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
  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doSysTableScan, NULL, NULL, destroySysScanOperator,
                                         NULL, NULL, NULL);
  pOperator->pTaskInfo = pTaskInfo;

  return pOperator;
}

static SSDataBlock* doTagScan(SOperatorInfo* pOperator, bool* newgroup) {
#if 0
  SOperatorInfo* pOperator = (SOperatorInfo*) param;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  int32_t maxNumOfTables = (int32_t)pResultInfo->capacity;

  STagScanInfo *pInfo = pOperator->info;
  SSDataBlock  *pRes = pInfo->pRes;
  *newgroup = false;

  int32_t count = 0;
  SArray* pa = GET_TABLEGROUP(pRuntimeEnv, 0);

  int32_t functionId = getExprFunctionId(&pOperator->pExpr[0]);
  if (functionId == FUNCTION_TID_TAG) { // return the tags & table Id
    assert(pQueryAttr->numOfOutput == 1);

    SExprInfo* pExprInfo = &pOperator->pExpr[0];
    int32_t rsize = pExprInfo->base.resSchema.bytes;

    count = 0;

    int16_t bytes = pExprInfo->base.resSchema.bytes;
    int16_t type  = pExprInfo->base.resSchema.type;

    for(int32_t i = 0; i < pQueryAttr->numOfTags; ++i) {
      if (pQueryAttr->tagColList[i].colId == pExprInfo->base.pColumns->info.colId) {
        bytes = pQueryAttr->tagColList[i].bytes;
        type = pQueryAttr->tagColList[i].type;
        break;
      }
    }

    SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, 0);

    while(pInfo->curPos < pInfo->totalTables && count < maxNumOfTables) {
      int32_t i = pInfo->curPos++;
      STableQueryInfo *item = taosArrayGetP(pa, i);

      char *output = pColInfo->pData + count * rsize;
      varDataSetLen(output, rsize - VARSTR_HEADER_SIZE);

      output = varDataVal(output);
      STableId* id = TSDB_TABLEID(item->pTable);

      *(int16_t *)output = 0;
      output += sizeof(int16_t);

      *(int64_t *)output = id->uid;  // memory align problem, todo serialize
      output += sizeof(id->uid);

      *(int32_t *)output = id->tid;
      output += sizeof(id->tid);

      *(int32_t *)output = pQueryAttr->vgId;
      output += sizeof(pQueryAttr->vgId);

      char* data = NULL;
      if (pExprInfo->base.pColumns->info.colId == TSDB_TBNAME_COLUMN_INDEX) {
        data = tsdbGetTableName(item->pTable);
      } else {
        data = tsdbGetTableTagVal(item->pTable, pExprInfo->base.pColumns->info.colId, type, bytes);
      }

      doSetTagValueToResultBuf(output, data, type, bytes);
      count += 1;
    }

    //qDebug("QInfo:0x%"PRIx64" create (tableId, tag) info completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  } else if (functionId == FUNCTION_COUNT) {// handle the "count(tbname)" query
    SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, 0);
    *(int64_t*)pColInfo->pData = pInfo->totalTables;
    count = 1;

    pOperator->status = OP_EXEC_DONE;
    //qDebug("QInfo:0x%"PRIx64" create count(tbname) query, res:%d rows:1", GET_TASKID(pRuntimeEnv), count);
  } else {  // return only the tags|table name etc.
    SExprInfo* pExprInfo = &pOperator->pExpr[0];  // todo use the column list instead of exprinfo

    count = 0;
    while(pInfo->curPos < pInfo->totalTables && count < maxNumOfTables) {
      int32_t i = pInfo->curPos++;

      STableQueryInfo* item = taosArrayGetP(pa, i);

      char *data = NULL, *dst = NULL;
      int16_t type = 0, bytes = 0;
      for(int32_t j = 0; j < pOperator->numOfOutput; ++j) {
        // not assign value in case of user defined constant output column
        if (TSDB_COL_IS_UD_COL(pExprInfo[j].base.pColumns->flag)) {
          continue;
        }

        SColumnInfoData* pColInfo = taosArrayGet(pRes->pDataBlock, j);
        type  = pExprInfo[j].base.resSchema.type;
        bytes = pExprInfo[j].base.resSchema.bytes;

        if (pExprInfo[j].base.pColumns->info.colId == TSDB_TBNAME_COLUMN_INDEX) {
          data = tsdbGetTableName(item->pTable);
        } else {
          data = tsdbGetTableTagVal(item->pTable, pExprInfo[j].base.pColumns->info.colId, type, bytes);
        }

        dst  = pColInfo->pData + count * pExprInfo[j].base.resSchema.bytes;
        doSetTagValueToResultBuf(dst, data, type, bytes);
      }

      count += 1;
    }

    if (pInfo->curPos >= pInfo->totalTables) {
      pOperator->status = OP_EXEC_DONE;
    }

    //qDebug("QInfo:0x%"PRIx64" create tag values results completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  }

  if (pOperator->status == OP_EXEC_DONE) {
    setTaskStatus(pOperator->pRuntimeEnv, TASK_COMPLETED);
  }

  pRes->info.rows = count;
  return (pRes->info.rows == 0)? NULL:pInfo->pRes;

#endif
  return TSDB_CODE_SUCCESS;
}

static void destroyTagScanOperatorInfo(void* param, int32_t numOfOutput) {
  STagScanInfo* pInfo = (STagScanInfo*)param;
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
}

SOperatorInfo* createTagScanOperatorInfo(void* pReaderHandle, SExprInfo* pExpr, int32_t numOfOutput,
                                         SExecTaskInfo* pTaskInfo) {
  STagScanInfo*  pInfo = taosMemoryCalloc(1, sizeof(STagScanInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  pInfo->pReader = pReaderHandle;
  pInfo->curPos = 0;
  pOperator->name = "TagScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN;
  pOperator->blockingOptr = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doTagScan, NULL, NULL, destroyTagScanOperatorInfo, NULL, NULL, NULL);
  pOperator->pExpr = pExpr;
  pOperator->numOfOutput = numOfOutput;
  pOperator->pTaskInfo = pTaskInfo;

  return pOperator;
_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}
