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

#include "executorimpl.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "systable.h"
#include "tname.h"
#include "ttime.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "query.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"
#include "vnode.h"

#define SET_REVERSE_SCAN_FLAG(_info) ((_info)->scanFlag = REVERSE_SCAN)
#define SWITCH_ORDER(n)              (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

static int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity);
static int32_t buildDbTableInfoBlock(bool sysInfo, const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta,
                                     size_t size, const char* dbName);

static bool processBlockWithProbability(const SSampleExecInfo* pInfo);

bool processBlockWithProbability(const SSampleExecInfo* pInfo) {
#if 0
  if (pInfo->sampleRatio == 1) {
    return true;
  }

  uint32_t val = taosRandR((uint32_t*) &pInfo->seed);
  return (val % ((uint32_t)(1/pInfo->sampleRatio))) == 0;
#else
  return true;
#endif
}

static void switchCtxOrder(SqlFunctionCtx* pCtx, int32_t numOfOutput) {
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
  tw->skey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000LL, TSDB_TIME_PRECISION_MILLI, pInterval->precision);

  mon = (int)(mon + interval);
  tm.tm_year = mon / 12;
  tm.tm_mon = mon % 12;
  tw->ekey = convertTimePrecision((int64_t)taosMktime(&tm) * 1000LL, TSDB_TIME_PRECISION_MILLI, pInterval->precision);

  tw->ekey -= 1;
}

static bool overlapWithTimeWindow(SInterval* pInterval, SDataBlockInfo* pBlockInfo, int32_t order) {
  STimeWindow w = {0};

  // 0 by default, which means it is not a interval operator of the upstream operator.
  if (pInterval->interval == 0) {
    return false;
  }

  if (order == TSDB_ORDER_ASC) {
    w = getAlignQueryTimeWindow(pInterval, pInterval->precision, pBlockInfo->window.skey);
    assert(w.ekey >= pBlockInfo->window.skey);

    if (TMAX(w.skey, pBlockInfo->window.skey) <= TMIN(w.ekey, pBlockInfo->window.ekey)) {
      return true;
    }

    while (1) {
      getNextTimeWindow(pInterval, &w, order);
      if (w.skey > pBlockInfo->window.ekey) {
        break;
      }

      assert(w.ekey > pBlockInfo->window.ekey);
      if (TMAX(w.skey, pBlockInfo->window.skey) <= pBlockInfo->window.ekey) {
        return true;
      }
    }
  } else {
    w = getAlignQueryTimeWindow(pInterval, pInterval->precision, pBlockInfo->window.ekey);
    assert(w.skey <= pBlockInfo->window.ekey);

    if (TMAX(w.skey, pBlockInfo->window.skey) <= TMIN(w.ekey, pBlockInfo->window.ekey)) {
      return true;
    }

    while (1) {
      getNextTimeWindow(pInterval, &w, order);
      if (w.ekey < pBlockInfo->window.skey) {
        break;
      }

      assert(w.skey < pBlockInfo->window.skey);
      if (pBlockInfo->window.skey <= TMIN(w.ekey, pBlockInfo->window.ekey)) {
        return true;
      }
    }
  }

  return false;
}

// this function is for table scanner to extract temporary results of upstream aggregate results.
static SResultRow* getTableGroupOutputBuf(SOperatorInfo* pOperator, uint64_t groupId, SFilePage** pPage) {
  if (pOperator->operatorType != QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) {
    return NULL;
  }

  int64_t buf[2] = {0};
  SET_RES_WINDOW_KEY((char*)buf, &groupId, sizeof(groupId), groupId);

  STableScanInfo* pTableScanInfo = pOperator->info;

  SResultRowPosition* p1 = (SResultRowPosition*)tSimpleHashGet(pTableScanInfo->pdInfo.pAggSup->pResultRowHashTable, buf,
                                                               GET_RES_WINDOW_KEY_LEN(sizeof(groupId)));

  if (p1 == NULL) {
    return NULL;
  }

  *pPage = getBufPage(pTableScanInfo->pdInfo.pAggSup->pResultBuf, p1->pageId);
  return (SResultRow*)((char*)(*pPage) + p1->offset);
}

static int32_t doDynamicPruneDataBlock(SOperatorInfo* pOperator, SDataBlockInfo* pBlockInfo, uint32_t* status) {
  STableScanInfo* pTableScanInfo = pOperator->info;

  if (pTableScanInfo->pdInfo.pExprSup == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SExprSupp* pSup1 = pTableScanInfo->pdInfo.pExprSup;

  SFilePage*  pPage = NULL;
  SResultRow* pRow = getTableGroupOutputBuf(pOperator, pBlockInfo->groupId, &pPage);

  if (pRow == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  bool notLoadBlock = true;
  for (int32_t i = 0; i < pSup1->numOfExprs; ++i) {
    int32_t functionId = pSup1->pCtx[i].functionId;

    SResultRowEntryInfo* pEntry = getResultEntryInfo(pRow, i, pTableScanInfo->pdInfo.pExprSup->rowEntryInfoOffset);

    int32_t reqStatus = fmFuncDynDataRequired(functionId, pEntry, &pBlockInfo->window);
    if (reqStatus != FUNC_DATA_REQUIRED_NOT_LOAD) {
      notLoadBlock = false;
      break;
    }
  }

  // release buffer pages
  releaseBufPage(pTableScanInfo->pdInfo.pAggSup->pResultBuf, pPage);

  if (notLoadBlock) {
    *status = FUNC_DATA_REQUIRED_NOT_LOAD;
  }

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool doFilterByBlockSMA(const SNode* pFilterNode, SColumnDataAgg** pColsAgg, int32_t numOfCols,
                                            int32_t numOfRows) {
  if (pColsAgg == NULL || pFilterNode == NULL) {
    return true;
  }

  SFilterInfo* filter = NULL;

  // todo move to the initialization function
  int32_t code = filterInitFromNode((SNode*)pFilterNode, &filter, 0);
  bool    keep = filterRangeExecute(filter, pColsAgg, numOfCols, numOfRows);

  filterFreeInfo(filter);
  return keep;
}

static bool doLoadBlockSMA(STableScanInfo* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  bool             allColumnsHaveAgg = true;
  SColumnDataAgg** pColAgg = NULL;

  int32_t code = tsdbRetrieveDatablockSMA(pTableScanInfo->dataReader, &pColAgg, &allColumnsHaveAgg);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (!allColumnsHaveAgg) {
    return false;
  }

  //  if (allColumnsHaveAgg == true) {
  int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

  // todo create this buffer during creating operator
  if (pBlock->pBlockAgg == NULL) {
    pBlock->pBlockAgg = taosMemoryCalloc(numOfCols, POINTER_BYTES);
    if (pBlock->pBlockAgg == NULL) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_OUT_OF_MEMORY);
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pTableScanInfo->pColMatchInfo); ++i) {
    SColMatchInfo* pColMatchInfo = taosArrayGet(pTableScanInfo->pColMatchInfo, i);
    if (!pColMatchInfo->output) {
      continue;
    }
    pBlock->pBlockAgg[pColMatchInfo->targetSlotId] = pColAgg[i];
  }

  return true;
}

static int32_t loadDataBlock(SOperatorInfo* pOperator, STableScanInfo* pTableScanInfo, SSDataBlock* pBlock,
                             uint32_t* status) {
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  STableScanInfo* pInfo = pOperator->info;

  SFileBlockLoadRecorder* pCost = &pTableScanInfo->readRecorder;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;
  bool loadSMA = false;

  *status = pInfo->dataBlockLoadFlag;
  if (pTableScanInfo->pFilterNode != NULL ||
      overlapWithTimeWindow(&pTableScanInfo->pdInfo.interval, &pBlock->info, pTableScanInfo->cond.order)) {
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
    loadSMA = true;  // mark the operation of load sma;
    bool success = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo);
    if (success) {  // failed to load the block sma data, data block statistics does not exist, load data block instead
      qDebug("%s data block SMA loaded, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
             pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      return TSDB_CODE_SUCCESS;
    } else {
      qDebug("%s failed to load SMA, since not all columns have SMA", GET_TASKID(pTaskInfo));
      *status = FUNC_DATA_REQUIRED_DATA_LOAD;
    }
  }

  ASSERT(*status == FUNC_DATA_REQUIRED_DATA_LOAD);

  // try to filter data block according to sma info
  if (pTableScanInfo->pFilterNode != NULL && (!loadSMA)) {
    bool success = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo);
    if (success) {
      size_t size = taosArrayGetSize(pBlock->pDataBlock);
      bool   keep = doFilterByBlockSMA(pTableScanInfo->pFilterNode, pBlock->pBlockAgg, size, pBlockInfo->rows);
      if (!keep) {
        qDebug("%s data block filter out by block SMA, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
               pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
        pCost->filterOutBlocks += 1;
        (*status) = FUNC_DATA_REQUIRED_FILTEROUT;

        return TSDB_CODE_SUCCESS;
      }
    }
  }

  // free the sma info, since it should not be involved in later computing process.
  taosMemoryFreeClear(pBlock->pBlockAgg);

  // try to filter data block according to current results
  doDynamicPruneDataBlock(pOperator, pBlockInfo, status);
  if (*status == FUNC_DATA_REQUIRED_NOT_LOAD) {
    qDebug("%s data block skipped due to dynamic prune, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->skipBlocks += 1;

    *status = FUNC_DATA_REQUIRED_FILTEROUT;
    return TSDB_CODE_SUCCESS;
  }

  pCost->totalCheckedRows += pBlock->info.rows;
  pCost->loadBlocks += 1;

  SArray* pCols = tsdbRetrieveDataBlock(pTableScanInfo->dataReader, NULL);
  if (pCols == NULL) {
    return terrno;
  }

  relocateColumnData(pBlock, pTableScanInfo->pColMatchInfo, pCols, true);

  // currently only the tbname pseudo column
  if (pTableScanInfo->pseudoSup.numOfExprs > 0) {
    SExprSupp* pSup = &pTableScanInfo->pseudoSup;

    int32_t code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock,
                                          GET_TASKID(pTaskInfo));
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  if (pTableScanInfo->pFilterNode != NULL) {
    int64_t st = taosGetTimestampUs();
    doFilter(pTableScanInfo->pFilterNode, pBlock, pTableScanInfo->pColMatchInfo);

    double el = (taosGetTimestampUs() - st) / 1000.0;
    pTableScanInfo->readRecorder.filterTime += el;

    if (pBlock->info.rows == 0) {
      pCost->filterOutBlocks += 1;
      qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d, elapsed time:%.2f ms",
             GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, el);
    } else {
      qDebug("%s data block filter applied, elapsed time:%.2f ms", GET_TASKID(pTaskInfo), el);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static void prepareForDescendingScan(STableScanInfo* pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);

  switchCtxOrder(pCtx, numOfOutput);
  //  setupQueryRangeForReverseScan(pTableScanInfo);

  pTableScanInfo->cond.order = TSDB_ORDER_DESC;
  STimeWindow* pTWindow = &pTableScanInfo->cond.twindows;
  TSWAP(pTWindow->skey, pTWindow->ekey);
}

int32_t addTagPseudoColumnData(SReadHandle* pHandle, SExprInfo* pPseudoExpr, int32_t numOfPseudoExpr,
                               SSDataBlock* pBlock, const char* idStr) {
  // currently only the tbname pseudo column
  if (numOfPseudoExpr == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SMetaReader mr = {0};
  metaReaderInit(&mr, pHandle->meta, 0);
  int32_t code = metaGetTableEntryByUid(&mr, pBlock->info.uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.uid, tstrerror(terrno), idStr);
    metaReaderClear(&mr);
    return terrno;
  }

  for (int32_t j = 0; j < numOfPseudoExpr; ++j) {
    SExprInfo* pExpr = &pPseudoExpr[j];

    int32_t dstSlotId = pExpr->base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colInfoDataCleanup(pColInfoData, pBlock->info.rows);

    int32_t functionId = pExpr->pExpr->_function.functionId;

    // this is to handle the tbname
    if (fmIsScanPseudoColumnFunc(functionId)) {
      setTbNameColData(pHandle->meta, pBlock, pColInfoData, functionId);
    } else {  // these are tags
      STagVal tagVal = {0};
      tagVal.cid = pExpr->base.pParam[0].pCol->colId;
      const char* p = metaGetTableTagVal(mr.me.ctbEntry.pTags, pColInfoData->info.type, &tagVal);

      char* data = NULL;
      if (pColInfoData->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
        data = tTagValToData((const STagVal*)p, false);
      } else {
        data = (char*)p;
      }

      bool isNullVal = (data == NULL) || (pColInfoData->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
      if (isNullVal) {
        colDataAppendNNULL(pColInfoData, 0, pBlock->info.rows);
      } else if (pColInfoData->info.type != TSDB_DATA_TYPE_JSON) {
        colDataAppendNItems(pColInfoData, 0, data, pBlock->info.rows);
      } else {  // todo opt for json tag
        for (int32_t i = 0; i < pBlock->info.rows; ++i) {
          colDataAppend(pColInfoData, i, data, false);
        }
      }

      if (data && (pColInfoData->info.type != TSDB_DATA_TYPE_JSON) && p != NULL &&
          IS_VAR_DATA_TYPE(((const STagVal*)p)->type)) {
        taosMemoryFree(data);
      }
    }
  }

  metaReaderClear(&mr);
  return TSDB_CODE_SUCCESS;
}

void setTbNameColData(void* pMeta, const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId) {
  struct SScalarFuncExecFuncs fpSet = {0};
  fmGetScalarFuncExecFuncs(functionId, &fpSet);

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_BIGINT, sizeof(uint64_t), 1);
  colInfoDataEnsureCapacity(&infoData, 1);

  colDataAppendInt64(&infoData, 0, (int64_t*)&pBlock->info.uid);
  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .param = pMeta, .columnData = &infoData};

  SScalarParam param = {.columnData = pColInfoData};
  fpSet.process(&srcParam, 1, &param);
  colDataDestroy(&infoData);
}

static SSDataBlock* doTableScanImpl(SOperatorInfo* pOperator) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*    pBlock = pTableScanInfo->pResBlock;

  int64_t st = taosGetTimestampUs();

  while (tsdbNextDataBlock(pTableScanInfo->dataReader)) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    // process this data block based on the probabilities
    bool processThisBlock = processBlockWithProbability(&pTableScanInfo->sample);
    if (!processThisBlock) {
      continue;
    }

    blockDataCleanup(pBlock);

    SDataBlockInfo binfo = pBlock->info;
    tsdbRetrieveDataBlockInfo(pTableScanInfo->dataReader, &binfo);

    binfo.capacity = binfo.rows;
    blockDataEnsureCapacity(pBlock, binfo.rows);
    pBlock->info = binfo;
    ASSERT(binfo.uid != 0);

    uint64_t* groupId = taosHashGet(pTaskInfo->tableqinfoList.map, &pBlock->info.uid, sizeof(int64_t));
    if (groupId) {
      pBlock->info.groupId = *groupId;
    }

    uint32_t status = 0;
    int32_t  code = loadDataBlock(pOperator, pTableScanInfo, pBlock, &status);
    //    int32_t  code = loadDataBlockOnDemand(pOperator->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
      continue;
    }

    pOperator->resultInfo.totalRows = pTableScanInfo->readRecorder.totalRows;
    pTableScanInfo->readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;

    pOperator->cost.totalCost = pTableScanInfo->readRecorder.elapsedTime;

    // todo refactor
    /*pTableScanInfo->lastStatus.uid = pBlock->info.uid;*/
    /*pTableScanInfo->lastStatus.ts = pBlock->info.window.ekey;*/
    pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__SNAPSHOT_DATA;
    pTaskInfo->streamInfo.lastStatus.uid = pBlock->info.uid;
    pTaskInfo->streamInfo.lastStatus.ts = pBlock->info.window.ekey;

    ASSERT(pBlock->info.uid != 0);
    return pBlock;
  }
  return NULL;
}

static SSDataBlock* doTableScanGroup(SOperatorInfo* pOperator) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  // The read handle is not initialized yet, since no qualified tables exists
  if (pTableScanInfo->dataReader == NULL || pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // do the ascending order traverse in the first place.
  while (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
    SSDataBlock* p = doTableScanImpl(pOperator);
    if (p != NULL) {
      ASSERT(p->info.uid != 0);
      return p;
    }

    pTableScanInfo->scanTimes += 1;

    if (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
      setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
      pTableScanInfo->scanFlag = REPEAT_SCAN;
      qDebug(
          "%s start to repeat ascending order scan data SELECT last_row(*),hostname from cpu group by hostname;blocks "
          "due to query func required",
          GET_TASKID(pTaskInfo));

      // do prepare for the next round table scan operation
      tsdbReaderReset(pTableScanInfo->dataReader, &pTableScanInfo->cond);
    }
  }

  int32_t total = pTableScanInfo->scanInfo.numOfAsc + pTableScanInfo->scanInfo.numOfDesc;
  if (pTableScanInfo->scanTimes < total) {
    if (pTableScanInfo->cond.order == TSDB_ORDER_ASC) {
      prepareForDescendingScan(pTableScanInfo, pOperator->exprSupp.pCtx, 0);
      tsdbReaderReset(pTableScanInfo->dataReader, &pTableScanInfo->cond);
      qDebug("%s start to descending order scan data blocks due to query func required", GET_TASKID(pTaskInfo));
    }

    while (pTableScanInfo->scanTimes < total) {
      SSDataBlock* p = doTableScanImpl(pOperator);
      if (p != NULL) {
        return p;
      }

      pTableScanInfo->scanTimes += 1;

      if (pTableScanInfo->scanTimes < total) {
        setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
        pTableScanInfo->scanFlag = REPEAT_SCAN;

        qDebug("%s start to repeat descending order scan data blocks due to query func required",
               GET_TASKID(pTaskInfo));
        tsdbReaderReset(pTableScanInfo->dataReader, &pTableScanInfo->cond);
      }
    }
  }

  return NULL;
}

static SSDataBlock* doTableScan(SOperatorInfo* pOperator) {
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  // if scan table by table
  if (pInfo->scanMode == TABLE_SCAN__TABLE_ORDER) {
    if (pInfo->noTable) return NULL;
    while (1) {
      SSDataBlock* result = doTableScanGroup(pOperator);
      if (result) {
        return result;
      }
      // if no data, switch to next table and continue scan
      pInfo->currentTable++;
      if (pInfo->currentTable >= taosArrayGetSize(pTaskInfo->tableqinfoList.pTableList)) {
        return NULL;
      }
      STableKeyInfo* pTableInfo = taosArrayGet(pTaskInfo->tableqinfoList.pTableList, pInfo->currentTable);
      tsdbSetTableId(pInfo->dataReader, pTableInfo->uid);
      tsdbReaderReset(pInfo->dataReader, &pInfo->cond);
      pInfo->scanTimes = 0;
    }
  }

  if (pInfo->currentGroupId == -1) {
    pInfo->currentGroupId++;
    if (pInfo->currentGroupId >= taosArrayGetSize(pTaskInfo->tableqinfoList.pGroupList)) {
      setTaskStatus(pTaskInfo, TASK_COMPLETED);
      return NULL;
    }

    SArray* tableList = taosArrayGetP(pTaskInfo->tableqinfoList.pGroupList, pInfo->currentGroupId);

    tsdbReaderClose(pInfo->dataReader);

    int32_t code = tsdbReaderOpen(pInfo->readHandle.vnode, &pInfo->cond, tableList, (STsdbReader**)&pInfo->dataReader,
                                  GET_TASKID(pTaskInfo));
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
      return NULL;
    }
  }

  SSDataBlock* result = doTableScanGroup(pOperator);
  if (result) {
    return result;
  }

  pInfo->currentGroupId++;
  if (pInfo->currentGroupId >= taosArrayGetSize(pTaskInfo->tableqinfoList.pGroupList)) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    return NULL;
  }

  SArray* tableList = taosArrayGetP(pTaskInfo->tableqinfoList.pGroupList, pInfo->currentGroupId);
  //  tsdbSetTableList(pInfo->dataReader, tableList);

  tsdbReaderReset(pInfo->dataReader, &pInfo->cond);
  pInfo->scanTimes = 0;

  result = doTableScanGroup(pOperator);
  if (result) {
    return result;
  }

  setTaskStatus(pTaskInfo, TASK_COMPLETED);
  return NULL;
}

static int32_t getTableScannerExecInfo(struct SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SFileBlockLoadRecorder* pRecorder = taosMemoryCalloc(1, sizeof(SFileBlockLoadRecorder));
  STableScanInfo*         pTableScanInfo = pOptr->info;
  *pRecorder = pTableScanInfo->readRecorder;
  *pOptrExplain = pRecorder;
  *len = sizeof(SFileBlockLoadRecorder);
  return 0;
}

static void destroyTableScanOperatorInfo(void* param) {
  STableScanInfo* pTableScanInfo = (STableScanInfo*)param;
  blockDataDestroy(pTableScanInfo->pResBlock);
  cleanupQueryTableDataCond(&pTableScanInfo->cond);

  tsdbReaderClose(pTableScanInfo->dataReader);
  pTableScanInfo->dataReader = NULL;

  if (pTableScanInfo->pColMatchInfo != NULL) {
    taosArrayDestroy(pTableScanInfo->pColMatchInfo);
  }

  cleanupExprSupp(&pTableScanInfo->pseudoSup);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle,
                                           SExecTaskInfo* pTaskInfo) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pTableScanNode->scan.node.pOutputDataBlockDesc;
  int32_t             numOfCols = 0;
  SArray* pColList = extractColMatchInfo(pTableScanNode->scan.pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID);

  int32_t code = initQueryTableDataCond(&pInfo->cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->pseudoSup;
    pSup->pExprInfo = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};
  pInfo->pdInfo.interval = extractIntervalInfo(pTableScanNode);
  pInfo->readHandle = *readHandle;
  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();

  pInfo->dataBlockLoadFlag = pTableScanNode->dataRequired;
  pInfo->pResBlock = createResDataBlock(pDescNode);
  pInfo->pFilterNode = pTableScanNode->scan.node.pConditions;
  pInfo->scanFlag = MAIN_SCAN;
  pInfo->pColMatchInfo = pColList;
  pInfo->currentGroupId = -1;
  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;

  pOperator->name = "TableScanOperator";  // for debug purpose
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doTableScan, NULL, NULL, destroyTableScanOperatorInfo,
                                         NULL, NULL, getTableScannerExecInfo);

  // for non-blocking operator, the open cost is always 0
  pOperator->cost.openCost = 0;
  return pOperator;

_error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);

  pTaskInfo->code = TSDB_CODE_QRY_OUT_OF_MEMORY;
  return NULL;
}

SOperatorInfo* createTableSeqScanOperatorInfo(void* pReadHandle, SExecTaskInfo* pTaskInfo) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  pInfo->dataReader = pReadHandle;
  //  pInfo->prevGroupId       = -1;

  pOperator->name = "TableSeqScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doTableScanImpl, NULL, NULL, NULL, NULL, NULL, NULL);
  return pOperator;
}

static int32_t doGetTableRowSize(void* pMeta, uint64_t uid, int32_t* rowLen, const char* idstr) {
  *rowLen = 0;

  SMetaReader mr = {0};
  metaReaderInit(&mr, pMeta, 0);
  int32_t code = metaGetTableEntryByUid(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", uid, tstrerror(terrno), idstr);
    metaReaderClear(&mr);
    return terrno;
  }

  if (mr.me.type == TSDB_SUPER_TABLE) {
    int32_t numOfCols = mr.me.stbEntry.schemaRow.nCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.stbEntry.schemaRow.pSchema[i].bytes;
    }
  } else if (mr.me.type == TSDB_CHILD_TABLE) {
    uint64_t suid = mr.me.ctbEntry.suid;
    tDecoderClear(&mr.coder);
    code = metaGetTableEntryByUid(&mr, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno), idstr);
      metaReaderClear(&mr);
      return terrno;
    }

    int32_t numOfCols = mr.me.stbEntry.schemaRow.nCols;

    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.stbEntry.schemaRow.pSchema[i].bytes;
    }
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {
    int32_t numOfCols = mr.me.ntbEntry.schemaRow.nCols;
    for (int32_t i = 0; i < numOfCols; ++i) {
      (*rowLen) += mr.me.ntbEntry.schemaRow.pSchema[i].bytes;
    }
  }

  metaReaderClear(&mr);
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* doBlockInfoScan(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SBlockDistInfo* pBlockScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  STableBlockDistInfo blockDistInfo = {.minRows = INT_MAX, .maxRows = INT_MIN};
  int32_t code = doGetTableRowSize(pBlockScanInfo->readHandle.meta, pBlockScanInfo->uid, &blockDistInfo.rowSize,
                                   GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  tsdbGetFileBlocksDistInfo(pBlockScanInfo->pHandle, &blockDistInfo);
  blockDistInfo.numOfInmemRows = (int32_t)tsdbGetNumOfRowsInMemTable(pBlockScanInfo->pHandle);

  SSDataBlock* pBlock = pBlockScanInfo->pResBlock;

  int32_t          slotId = pOperator->exprSupp.pExprInfo->base.resSchema.slotId;
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, slotId);

  int32_t len = tSerializeBlockDistInfo(NULL, 0, &blockDistInfo);
  char*   p = taosMemoryCalloc(1, len + VARSTR_HEADER_SIZE);
  tSerializeBlockDistInfo(varDataVal(p), len, &blockDistInfo);
  varDataSetLen(p, len);

  blockDataEnsureCapacity(pBlock, 1);
  colDataAppend(pColInfo, 0, p, false);
  taosMemoryFree(p);

  pBlock->info.rows = 1;

  pOperator->status = OP_EXEC_DONE;
  return pBlock;
}

static void destroyBlockDistScanOperatorInfo(void* param) {
  SBlockDistInfo* pDistInfo = (SBlockDistInfo*)param;
  blockDataDestroy(pDistInfo->pResBlock);
  tsdbReaderClose(pDistInfo->pHandle);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createDataBlockInfoScanOperator(void* dataReader, SReadHandle* readHandle, uint64_t uid,
                                               SBlockDistScanPhysiNode* pBlockScanNode, SExecTaskInfo* pTaskInfo) {
  SBlockDistInfo* pInfo = taosMemoryCalloc(1, sizeof(SBlockDistInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  pInfo->pHandle = dataReader;
  pInfo->readHandle = *readHandle;
  pInfo->uid = uid;
  pInfo->pResBlock = createResDataBlock(pBlockScanNode->node.pOutputDataBlockDesc);

  int32_t    numOfCols = 0;
  SExprInfo* pExprInfo = createExprInfo(pBlockScanNode->pScanPseudoCols, NULL, &numOfCols);
  int32_t    code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pOperator->name = "DataBlockDistScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_BLOCK_DIST_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doBlockInfoScan, NULL, NULL,
                                         destroyBlockDistScanOperatorInfo, NULL, NULL, NULL);
  return pOperator;

_error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}

static void doClearBufferedBlocks(SStreamScanInfo* pInfo) {
  size_t total = taosArrayGetSize(pInfo->pBlockLists);

  pInfo->validBlockIndex = 0;
  for (int32_t i = 0; i < total; ++i) {
    SSDataBlock* p = taosArrayGetP(pInfo->pBlockLists, i);
    blockDataDestroy(p);
  }
  taosArrayClear(pInfo->pBlockLists);
}

static bool isSessionWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
}

static bool isStateWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE;
}

static bool isIntervalWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL;
}

static bool isSignleIntervalWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL;
}

static bool isSlidingWindow(SStreamScanInfo* pInfo) {
  return isIntervalWindow(pInfo) && pInfo->interval.interval != pInfo->interval.sliding;
}

static void setGroupId(SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t groupColIndex, int32_t rowIndex) {
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, groupColIndex);
  uint64_t*        groupCol = (uint64_t*)pColInfo->pData;
  ASSERT(rowIndex < pBlock->info.rows);
  pInfo->groupId = groupCol[rowIndex];
}

void resetTableScanInfo(STableScanInfo* pTableScanInfo, STimeWindow* pWin) {
  pTableScanInfo->cond.twindows = *pWin;
  pTableScanInfo->scanTimes = 0;
  pTableScanInfo->currentGroupId = -1;
}

static void freeArray(void* array) { taosArrayDestroy(array); }

static void resetTableScanOperator(SOperatorInfo* pTableScanOp) {
  STableScanInfo* pTableScanInfo = pTableScanOp->info;
  pTableScanInfo->cond.startVersion = -1;
  pTableScanInfo->cond.endVersion = -1;
  SArray* gpTbls = pTableScanOp->pTaskInfo->tableqinfoList.pGroupList;
  SArray* allTbls = pTableScanOp->pTaskInfo->tableqinfoList.pTableList;
  taosArrayClearP(gpTbls, freeArray);
  taosArrayPush(gpTbls, &allTbls);
  STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
  resetTableScanInfo(pTableScanOp->info, &win);
}

static SSDataBlock* readPreVersionData(SOperatorInfo* pTableScanOp, uint64_t tbUid, TSKEY startTs, TSKEY endTs,
                                       int64_t maxVersion) {
  SArray* gpTbls = pTableScanOp->pTaskInfo->tableqinfoList.pGroupList;
  taosArrayClear(gpTbls);
  STableKeyInfo tblInfo = {.uid = tbUid, .groupId = 0};
  SArray*       tbls = taosArrayInit(1, sizeof(STableKeyInfo));
  taosArrayPush(tbls, &tblInfo);
  taosArrayPush(gpTbls, &tbls);

  STimeWindow     win = {.skey = startTs, .ekey = endTs};
  STableScanInfo* pTableScanInfo = pTableScanOp->info;
  pTableScanInfo->cond.startVersion = -1;
  pTableScanInfo->cond.endVersion = maxVersion;
  resetTableScanInfo(pTableScanOp->info, &win);
  SSDataBlock* pRes = doTableScan(pTableScanOp);
  resetTableScanOperator(pTableScanOp);
  return pRes;
}

static uint64_t getGroupIdByCol(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion) {
  SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, uid, ts, ts, maxVersion);
  if (!pPreRes || pPreRes->info.rows == 0) {
    return 0;
  }
  ASSERT(pPreRes->info.rows == 1);
  return calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pPreRes, 0);
}

static uint64_t getGroupIdByData(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion) {
  if (pInfo->partitionSup.needCalc) {
    return getGroupIdByCol(pInfo, uid, ts, maxVersion);
  }

  SHashObj* map = pInfo->pTableScanOp->pTaskInfo->tableqinfoList.map;
  uint64_t* groupId = taosHashGet(map, &uid, sizeof(int64_t));
  if (groupId) {
    return *groupId;
  }
  return 0;
}

static bool prepareRangeScan(SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t* pRowIndex) {
  if ((*pRowIndex) == pBlock->info.rows) {
    return false;
  }

  ASSERT(taosArrayGetSize(pBlock->pDataBlock) >= 3);
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startData = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endData = (TSKEY*)pEndTsCol->pData;
  STimeWindow      win = {.skey = startData[*pRowIndex], .ekey = endData[*pRowIndex]};
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpData = (uint64_t*)pGpCol->pData;
  uint64_t         groupId = gpData[*pRowIndex];

  SColumnInfoData* pCalStartTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  TSKEY*           calStartData = (TSKEY*)pCalStartTsCol->pData;
  SColumnInfoData* pCalEndTsCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  TSKEY*           calEndData = (TSKEY*)pCalEndTsCol->pData;

  setGroupId(pInfo, pBlock, GROUPID_COLUMN_INDEX, *pRowIndex);
  if (isSlidingWindow(pInfo)) {
    pInfo->updateWin.skey = calStartData[*pRowIndex];
    pInfo->updateWin.ekey = calEndData[*pRowIndex];
  }
  (*pRowIndex)++;

  for (; *pRowIndex < pBlock->info.rows; (*pRowIndex)++) {
    if (win.skey == startData[*pRowIndex] && groupId == gpData[*pRowIndex]) {
      win.ekey = TMAX(win.ekey, endData[*pRowIndex]);
      continue;
    }
    if (win.skey == endData[*pRowIndex] && groupId == gpData[*pRowIndex]) {
      win.skey = TMIN(win.skey, startData[*pRowIndex]);
      continue;
    }
    ASSERT(!(win.skey > startData[*pRowIndex] && win.ekey < endData[*pRowIndex]) ||
           !(isInTimeWindow(&win, startData[*pRowIndex], 0) || isInTimeWindow(&win, endData[*pRowIndex], 0)));
    break;
  }

  resetTableScanInfo(pInfo->pTableScanOp->info, &win);
  pInfo->pTableScanOp->status = OP_OPENED;
  return true;
}

static STimeWindow getSlidingWindow(TSKEY* startTsCol, TSKEY* endTsCol, SInterval* pInterval,
                                    SDataBlockInfo* pDataBlockInfo, int32_t* pRowIndex, bool hasGroup) {
  SResultRowInfo dumyInfo;
  dumyInfo.cur.pageId = -1;
  STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTsCol[*pRowIndex], pInterval, TSDB_ORDER_ASC);
  STimeWindow endWin = win;
  STimeWindow preWin = win;
  while (1) {
    if (hasGroup) {
      (*pRowIndex) += 1;
    } else {
      (*pRowIndex) += getNumOfRowsInTimeWindow(pDataBlockInfo, startTsCol, *pRowIndex, endWin.ekey, binarySearchForKey,
                                               NULL, TSDB_ORDER_ASC);
    }
    do {
      preWin = endWin;
      getNextTimeWindow(pInterval, &endWin, TSDB_ORDER_ASC);
    } while (endTsCol[(*pRowIndex) - 1] >= endWin.skey);
    endWin = preWin;
    if (win.ekey == endWin.ekey || (*pRowIndex) == pDataBlockInfo->rows) {
      win.ekey = endWin.ekey;
      return win;
    }
    win.ekey = endWin.ekey;
  }
}

static SSDataBlock* doRangeScan(SStreamScanInfo* pInfo, SSDataBlock* pSDB, int32_t tsColIndex, int32_t* pRowIndex) {
  while (1) {
    SSDataBlock* pResult = NULL;
    pResult = doTableScan(pInfo->pTableScanOp);
    if (!pResult && prepareRangeScan(pInfo, pSDB, pRowIndex)) {
      // scan next window data
      pResult = doTableScan(pInfo->pTableScanOp);
    }
    if (!pResult) {
      blockDataCleanup(pSDB);
      *pRowIndex = 0;
      pInfo->updateWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
      STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
      tsdbReaderClose(pTableScanInfo->dataReader);
      pTableScanInfo->dataReader = NULL;
      return NULL;
    }

    doFilter(pInfo->pCondition, pResult, NULL);
    if (pResult->info.rows == 0) {
      continue;
    }

    if (pInfo->partitionSup.needCalc) {
      SSDataBlock* tmpBlock = createOneDataBlock(pResult, true);
      blockDataCleanup(pResult);
      for (int32_t i = 0; i < tmpBlock->info.rows; i++) {
        if (calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, tmpBlock, i) == pInfo->groupId) {
          for (int32_t j = 0; j < pInfo->pTableScanOp->exprSupp.numOfExprs; j++) {
            SColumnInfoData* pSrcCol = taosArrayGet(tmpBlock->pDataBlock, j);
            SColumnInfoData* pDestCol = taosArrayGet(pResult->pDataBlock, j);
            bool             isNull = colDataIsNull(pSrcCol, tmpBlock->info.rows, i, NULL);
            char*            pSrcData = colDataGetData(pSrcCol, i);
            colDataAppend(pDestCol, pResult->info.rows, pSrcData, isNull);
          }
          pResult->info.rows++;
        }
      }
      if (pResult->info.rows > 0) {
        pResult->info.calWin = pInfo->updateWin;
        return pResult;
      }
    } else if (pResult->info.groupId == pInfo->groupId) {
      pResult->info.calWin = pInfo->updateWin;
      return pResult;
    }
  }
}

static int32_t generateSessionScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  if (pSrcBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  blockDataCleanup(pDestBlock);
  int32_t code = blockDataEnsureCapacity(pDestBlock, pSrcBlock->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  ASSERT(taosArrayGetSize(pSrcBlock->pDataBlock) >= 3);
  SColumnInfoData* pStartTsCol = taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startData = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endData = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*        uidCol = (uint64_t*)pUidCol->pData;

  SColumnInfoData* pDestStartCol = taosArrayGet(pDestBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pDestEndCol = taosArrayGet(pDestBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pDestUidCol = taosArrayGet(pDestBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pDestGpCol = taosArrayGet(pDestBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pDestCalStartTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pDestCalEndTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  int32_t          dummy = 0;
  int64_t          version = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t groupId = getGroupIdByData(pInfo, uidCol[i], startData[i], version);
    // gap must be 0.
    SResultWindowInfo* pStartWin =
        getCurSessionWindow(pInfo->windowSup.pStreamAggSup, startData[i], endData[i], groupId, 0, &dummy);
    if (!pStartWin) {
      // window has been closed.
      continue;
    }
    SResultWindowInfo* pEndWin =
        getCurSessionWindow(pInfo->windowSup.pStreamAggSup, endData[i], endData[i], groupId, 0, &dummy);
    ASSERT(pEndWin);
    TSKEY ts = INT64_MIN;
    colDataAppend(pDestStartCol, i, (const char*)&pStartWin->win.skey, false);
    colDataAppend(pDestEndCol, i, (const char*)&pEndWin->win.ekey, false);
    colDataAppendNULL(pDestUidCol, i);
    colDataAppend(pDestGpCol, i, (const char*)&groupId, false);
    colDataAppendNULL(pDestCalStartTsCol, i);
    colDataAppendNULL(pDestCalEndTsCol, i);
    pDestBlock->info.rows++;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateIntervalScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  blockDataCleanup(pDestBlock);
  int32_t rows = pSrcBlock->info.rows;
  if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = blockDataEnsureCapacity(pDestBlock, rows * 2);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData* pSrcStartTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcEndTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*        srcUidData = (uint64_t*)pSrcUidCol->pData;
  SColumnInfoData* pSrcGpCol = taosArrayGet(pSrcBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        srcGp = (uint64_t*)pSrcGpCol->pData;
  ASSERT(pSrcStartTsCol->info.type == TSDB_DATA_TYPE_TIMESTAMP);
  TSKEY*           srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*           srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  SColumnInfoData* pStartTsCol = taosArrayGet(pDestBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pDestBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pDeUidCol = taosArrayGet(pDestBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGpCol = taosArrayGet(pDestBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  int64_t          version = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < rows;) {
    uint64_t srcUid = srcUidData[i];
    uint64_t groupId = getGroupIdByData(pInfo, srcUid, srcStartTsCol[i], version);
    uint64_t srcGpId = srcGp[i];
    TSKEY    calStartTs = srcStartTsCol[i];
    colDataAppend(pCalStartTsCol, pDestBlock->info.rows, (const char*)(&calStartTs), false);
    STimeWindow win = getSlidingWindow(srcStartTsCol, srcEndTsCol, &pInfo->interval, &pSrcBlock->info, &i,
                                       pInfo->partitionSup.needCalc);
    TSKEY       calEndTs = srcStartTsCol[i - 1];
    colDataAppend(pCalEndTsCol, pDestBlock->info.rows, (const char*)(&calEndTs), false);
    colDataAppend(pDeUidCol, pDestBlock->info.rows, (const char*)(&srcUid), false);
    colDataAppend(pStartTsCol, pDestBlock->info.rows, (const char*)(&win.skey), false);
    colDataAppend(pEndTsCol, pDestBlock->info.rows, (const char*)(&win.ekey), false);
    colDataAppend(pGpCol, pDestBlock->info.rows, (const char*)(&groupId), false);
    pDestBlock->info.rows++;
    if (pInfo->partitionSup.needCalc && srcGpId != 0 && groupId != srcGpId) {
      colDataAppend(pCalStartTsCol, pDestBlock->info.rows, (const char*)(&calStartTs), false);
      colDataAppend(pCalEndTsCol, pDestBlock->info.rows, (const char*)(&calEndTs), false);
      colDataAppend(pDeUidCol, pDestBlock->info.rows, (const char*)(&srcUid), false);
      colDataAppend(pStartTsCol, pDestBlock->info.rows, (const char*)(&win.skey), false);
      colDataAppend(pEndTsCol, pDestBlock->info.rows, (const char*)(&win.ekey), false);
      colDataAppend(pGpCol, pDestBlock->info.rows, (const char*)(&srcGpId), false);
      pDestBlock->info.rows++;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateDeleteResultBlock(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  if (pSrcBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  blockDataCleanup(pDestBlock);
  int32_t code = blockDataEnsureCapacity(pDestBlock, pSrcBlock->info.rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  ASSERT(taosArrayGetSize(pSrcBlock->pDataBlock) >= 3);
  SColumnInfoData* pStartTsCol = taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startData = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pEndTsCol = taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  TSKEY*           endData = (TSKEY*)pEndTsCol->pData;
  SColumnInfoData* pUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*        uidCol = (uint64_t*)pUidCol->pData;

  SColumnInfoData* pDestStartCol = taosArrayGet(pDestBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pDestEndCol = taosArrayGet(pDestBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pDestUidCol = taosArrayGet(pDestBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pDestGpCol = taosArrayGet(pDestBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pDestCalStartTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pDestCalEndTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  int32_t          dummy = 0;
  int64_t          version = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t groupId = getGroupIdByData(pInfo, uidCol[i], startData[i], version);
    colDataAppend(pDestStartCol, i, (const char*)(startData + i), false);
    colDataAppend(pDestEndCol, i, (const char*)(endData + i), false);
    colDataAppendNULL(pDestUidCol, i);
    colDataAppend(pDestGpCol, i, (const char*)&groupId, false);
    colDataAppendNULL(pDestCalStartTsCol, i);
    colDataAppendNULL(pDestCalEndTsCol, i);
    pDestBlock->info.rows++;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (isIntervalWindow(pInfo)) {
    code = generateIntervalScanRange(pInfo, pSrcBlock, pDestBlock);
  } else if (isSessionWindow(pInfo) || isStateWindow(pInfo)) {
    code = generateSessionScanRange(pInfo, pSrcBlock, pDestBlock);
  }
  pDestBlock->info.type = STREAM_CLEAR;
  pDestBlock->info.version = pSrcBlock->info.version;
  blockDataUpdateTsWindow(pDestBlock, 0);
  return code;
}

void appendOneRow(SSDataBlock* pBlock, TSKEY* pStartTs, TSKEY* pEndTs, uint64_t* pUid, uint64_t* pGp) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  colDataAppend(pStartTsCol, pBlock->info.rows, (const char*)pStartTs, false);
  colDataAppend(pEndTsCol, pBlock->info.rows, (const char*)pEndTs, false);
  colDataAppend(pUidCol, pBlock->info.rows, (const char*)pUid, false);
  colDataAppend(pGpCol, pBlock->info.rows, (const char*)pGp, false);
  pBlock->info.rows++;
}

static void checkUpdateData(SStreamScanInfo* pInfo, bool invertible, SSDataBlock* pBlock, bool out) {
  if (out) {
    blockDataCleanup(pInfo->pUpdateDataRes);
    blockDataEnsureCapacity(pInfo->pUpdateDataRes, pBlock->info.rows);
  }
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  ASSERT(pColDataInfo->info.type == TSDB_DATA_TYPE_TIMESTAMP);
  TSKEY* tsCol = (TSKEY*)pColDataInfo->pData;
  bool   tableInserted = updateInfoIsTableInserted(pInfo->pUpdateInfo, pBlock->info.uid);
  for (int32_t rowId = 0; rowId < pBlock->info.rows; rowId++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    bool        isClosed = false;
    STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
    if (tableInserted && isOverdue(tsCol[rowId], &pInfo->twAggSup)) {
      win = getActiveTimeWindow(NULL, &dumyInfo, tsCol[rowId], &pInfo->interval, TSDB_ORDER_ASC);
      isClosed = isCloseWindow(&win, &pInfo->twAggSup);
    }
    // must check update info first.
    bool update = updateInfoIsUpdated(pInfo->pUpdateInfo, pBlock->info.uid, tsCol[rowId]);
    bool closedWin = isClosed && isSignleIntervalWindow(pInfo) &&
                     isDeletedWindow(&win, pBlock->info.groupId, pInfo->windowSup.pIntervalAggSup);
    if ((update || closedWin) && out) {
      uint64_t gpId = closedWin && pInfo->partitionSup.needCalc
                          ? calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pBlock, rowId)
                          : 0;
      appendOneRow(pInfo->pUpdateDataRes, tsCol + rowId, tsCol + rowId, &pBlock->info.uid, &gpId);
    }
  }
  if (out && pInfo->pUpdateDataRes->info.rows > 0) {
    pInfo->pUpdateDataRes->info.version = pBlock->info.version;
    blockDataUpdateTsWindow(pInfo->pUpdateDataRes, 0);
    pInfo->pUpdateDataRes->info.type = pInfo->partitionSup.needCalc ? STREAM_DELETE_DATA : STREAM_CLEAR;
  }
}

static int32_t setBlockIntoRes(SStreamScanInfo* pInfo, const SSDataBlock* pBlock) {
  SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
  SOperatorInfo*  pOperator = pInfo->pStreamScanOp;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);

  pInfo->pRes->info.rows = pBlock->info.rows;
  pInfo->pRes->info.uid = pBlock->info.uid;
  pInfo->pRes->info.type = STREAM_NORMAL;
  pInfo->pRes->info.version = pBlock->info.version;

  uint64_t* groupIdPre = taosHashGet(pTaskInfo->tableqinfoList.map, &pBlock->info.uid, sizeof(int64_t));
  if (groupIdPre) {
    pInfo->pRes->info.groupId = *groupIdPre;
  } else {
    pInfo->pRes->info.groupId = 0;
  }

  // todo extract method
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->pColMatchInfo); ++i) {
    SColMatchInfo* pColMatchInfo = taosArrayGet(pInfo->pColMatchInfo, i);
    if (!pColMatchInfo->output) {
      continue;
    }

    bool colExists = false;
    for (int32_t j = 0; j < blockDataGetNumOfCols(pBlock); ++j) {
      SColumnInfoData* pResCol = bdGetColumnInfoData(pBlock, j);
      if (pResCol->info.colId == pColMatchInfo->colId) {
        SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->targetSlotId);
        colDataAssign(pDst, pResCol, pBlock->info.rows, &pInfo->pRes->info);
        colExists = true;
        break;
      }
    }

    // the required column does not exists in submit block, let's set it to be all null value
    if (!colExists) {
      SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->targetSlotId);
      colDataAppendNNULL(pDst, 0, pBlockInfo->rows);
    }
  }

  // currently only the tbname pseudo column
  if (pInfo->numOfPseudoExpr > 0) {
    int32_t code = addTagPseudoColumnData(&pInfo->readHandle, pInfo->pPseudoExpr, pInfo->numOfPseudoExpr, pInfo->pRes,
                                          GET_TASKID(pTaskInfo));
    if (code != TSDB_CODE_SUCCESS) {
      blockDataFreeRes((SSDataBlock*)pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  doFilter(pInfo->pCondition, pInfo->pRes, NULL);
  blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
  blockDataFreeRes((SSDataBlock*)pBlock);
  return 0;
}

static SSDataBlock* doQueueScan(SOperatorInfo* pOperator) {
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;

  qDebug("queue scan called");
  if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    SSDataBlock* pResult = doTableScan(pInfo->pTableScanOp);
    if (pResult && pResult->info.rows > 0) {
      qDebug("queue scan tsdb return %d rows", pResult->info.rows);
      pTaskInfo->streamInfo.returned = 1;
      return pResult;
    } else {
      if (!pTaskInfo->streamInfo.returned) {
        STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
        tsdbReaderClose(pTSInfo->dataReader);
        pTSInfo->dataReader = NULL;
        tqOffsetResetToLog(&pTaskInfo->streamInfo.prepareStatus, pTaskInfo->streamInfo.snapshotVer);
        qDebug("queue scan tsdb over, switch to wal ver %d", pTaskInfo->streamInfo.snapshotVer + 1);
        if (tqSeekVer(pInfo->tqReader, pTaskInfo->streamInfo.snapshotVer + 1) < 0) {
          return NULL;
        }
        ASSERT(pInfo->tqReader->pWalReader->curVersion == pTaskInfo->streamInfo.snapshotVer + 1);
      } else {
        return NULL;
      }
    }
  }

  if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__LOG) {
    while (1) {
      SFetchRet ret = {0};
      tqNextBlock(pInfo->tqReader, &ret);
      if (ret.fetchType == FETCH_TYPE__DATA) {
        blockDataCleanup(pInfo->pRes);
        if (setBlockIntoRes(pInfo, &ret.data) < 0) {
          ASSERT(0);
        }
        // TODO clean data block
        if (pInfo->pRes->info.rows > 0) {
          qDebug("queue scan log return %d rows", pInfo->pRes->info.rows);
          return pInfo->pRes;
        }
      } else if (ret.fetchType == FETCH_TYPE__META) {
        ASSERT(0);
        //        pTaskInfo->streamInfo.lastStatus = ret.offset;
        //        pTaskInfo->streamInfo.metaBlk = ret.meta;
        //        return NULL;
      } else if (ret.fetchType == FETCH_TYPE__NONE) {
        pTaskInfo->streamInfo.lastStatus = ret.offset;
        ASSERT(pTaskInfo->streamInfo.lastStatus.version >= pTaskInfo->streamInfo.prepareStatus.version);
        ASSERT(pTaskInfo->streamInfo.lastStatus.version + 1 == pInfo->tqReader->pWalReader->curVersion);
        char formatBuf[80];
        tFormatOffset(formatBuf, 80, &ret.offset);
        qDebug("queue scan log return null, offset %s", formatBuf);
        return NULL;
      } else {
        ASSERT(0);
      }
    }
  } else if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    SSDataBlock* pResult = doTableScan(pInfo->pTableScanOp);
    if (pResult && pResult->info.rows > 0) {
      qDebug("stream scan tsdb return %d rows", pResult->info.rows);
      return pResult;
    }
    qDebug("stream scan tsdb return null");
    return NULL;
  } else {
    ASSERT(0);
    return NULL;
  }
}

static SSDataBlock* doStreamScan(SOperatorInfo* pOperator) {
  // NOTE: this operator does never check if current status is done or not
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;

  qDebug("stream scan called");
#if 0
  SStreamState* pState = pTaskInfo->streamInfo.pState;
  if (pState) {
    printf(">>>>>>>> stream write backend\n");
    SWinKey key = {
        .ts = 1,
        .groupId = 2,
    };
    char tmp[100] = "abcdefg1";
    if (streamStatePut(pState, &key, &tmp, strlen(tmp) + 1) < 0) {
      ASSERT(0);
    }

    key.ts = 2;
    char tmp2[100] = "abcdefg2";
    if (streamStatePut(pState, &key, &tmp2, strlen(tmp2) + 1) < 0) {
      ASSERT(0);
    }

    key.groupId = 5;
    key.ts = 1;
    char tmp3[100] = "abcdefg3";
    if (streamStatePut(pState, &key, &tmp3, strlen(tmp3) + 1) < 0) {
      ASSERT(0);
    }

    char*   val2 = NULL;
    int32_t sz;
    if (streamStateGet(pState, &key, (void**)&val2, &sz) < 0) {
      ASSERT(0);
    }
    printf("stream read %s %d\n", val2, sz);
    streamFreeVal(val2);
  }
#endif

  if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE) {
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    memcpy(&pTSInfo->cond, &pTaskInfo->streamInfo.tableCond, sizeof(SQueryTableDataCond));
    pTSInfo->scanTimes = 0;
    pTSInfo->currentGroupId = -1;
    pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__SCAN;
  }

  if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__SCAN) {
    SSDataBlock* pBlock = doTableScan(pInfo->pTableScanOp);
    if (pBlock != NULL) {
      return pBlock;
    }
    // TODO fill in bloom filter
    pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__NONE;
    return NULL;
  }

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
  // TODO: refactor
  if (pInfo->blockType == STREAM_INPUT__DATA_BLOCK) {
    if (pInfo->validBlockIndex >= total) {
      /*doClearBufferedBlocks(pInfo);*/
      /*pOperator->status = OP_EXEC_DONE;*/
      return NULL;
    }

    int32_t      current = pInfo->validBlockIndex++;
    SSDataBlock* pBlock = taosArrayGetP(pInfo->pBlockLists, current);
    // TODO move into scan
    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    blockDataUpdateTsWindow(pBlock, 0);
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_GET_ALL:
        return pBlock;
      case STREAM_RETRIEVE: {
        pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RETRIEVE;
        copyDataBlock(pInfo->pUpdateRes, pBlock);
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
        updateInfoAddCloseWindowSBF(pInfo->pUpdateInfo);
      } break;
      case STREAM_DELETE_DATA: {
        printDataBlock(pBlock, "stream scan delete recv");
        if (!isIntervalWindow(pInfo) && !isSessionWindow(pInfo) && !isStateWindow(pInfo)) {
          generateDeleteResultBlock(pInfo, pBlock, pInfo->pDeleteDataRes);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_RESULT;
          printDataBlock(pBlock, "stream scan delete result");
          return pInfo->pDeleteDataRes;
        } else {
          pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
          pInfo->updateResIndex = 0;
          generateScanRange(pInfo, pBlock, pInfo->pUpdateRes);
          prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
          copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
          pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
          printDataBlock(pBlock, "stream scan delete data");
          return pInfo->pDeleteDataRes;
        }
      } break;
      default:
        break;
    }
    // printDataBlock(pBlock, "stream scan recv");
    return pBlock;
  } else if (pInfo->blockType == STREAM_INPUT__DATA_SUBMIT) {
    qDebug("scan mode %d", pInfo->scanMode);
    switch (pInfo->scanMode) {
      case STREAM_SCAN_FROM_RES: {
        blockDataDestroy(pInfo->pUpdateRes);
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
        return pInfo->pRes;
      } break;
      case STREAM_SCAN_FROM_DELETE_DATA: {
        generateScanRange(pInfo, pInfo->pUpdateDataRes, pInfo->pUpdateRes);
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
        copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
        pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
        return pInfo->pDeleteDataRes;
      } break;
      case STREAM_SCAN_FROM_UPDATERES: {
        generateScanRange(pInfo, pInfo->pUpdateDataRes, pInfo->pUpdateRes);
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
        return pInfo->pUpdateRes;
      } break;
      case STREAM_SCAN_FROM_DATAREADER_RANGE:
      case STREAM_SCAN_FROM_DATAREADER_RETRIEVE: {
        SSDataBlock* pSDB = doRangeScan(pInfo, pInfo->pUpdateRes, pInfo->primaryTsIndex, &pInfo->updateResIndex);
        if (pSDB) {
          STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
          uint64_t        version = getReaderMaxVersion(pTableScanInfo->dataReader);
          updateInfoSetScanRange(pInfo->pUpdateInfo, &pTableScanInfo->cond.twindows, pInfo->groupId, version);
          pSDB->info.type = pInfo->scanMode == STREAM_SCAN_FROM_DATAREADER_RANGE ? STREAM_NORMAL : STREAM_PULL_DATA;
          checkUpdateData(pInfo, true, pSDB, false);
          // printDataBlock(pSDB, "stream scan update");
          return pSDB;
        }
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      } break;
      default:
        break;
    }

    SStreamAggSupporter* pSup = pInfo->windowSup.pStreamAggSup;
    if (isStateWindow(pInfo) && pSup->pScanBlock->info.rows > 0) {
      pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
      pInfo->updateResIndex = 0;
      copyDataBlock(pInfo->pUpdateRes, pSup->pScanBlock);
      blockDataCleanup(pSup->pScanBlock);
      prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
      return pInfo->pUpdateRes;
    }

    SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;

    int32_t totBlockNum = taosArrayGetSize(pInfo->pBlockLists);

    while (1) {
      if (pInfo->tqReader->pMsg == NULL) {
        if (pInfo->validBlockIndex >= totBlockNum) {
          return NULL;
        }

        int32_t     current = pInfo->validBlockIndex++;
        SSubmitReq* pSubmit = taosArrayGetP(pInfo->pBlockLists, current);
        if (tqReaderSetDataMsg(pInfo->tqReader, pSubmit, 0) < 0) {
          qError("submit msg messed up when initing stream submit block %p, current %d, total %d", pSubmit, current,
                 totBlockNum);
          pInfo->tqReader->pMsg = NULL;
          continue;
        }
      }

      blockDataCleanup(pInfo->pRes);

      while (tqNextDataBlock(pInfo->tqReader)) {
        SSDataBlock block = {0};

        int32_t code = tqRetrieveDataBlock(&block, pInfo->tqReader);

        if (code != TSDB_CODE_SUCCESS || block.info.rows == 0) {
          continue;
        }

        setBlockIntoRes(pInfo, &block);

        if (updateInfoIgnore(pInfo->pUpdateInfo, &pInfo->pRes->info.window, pInfo->pRes->info.groupId,
                             pInfo->pRes->info.version)) {
          printDataBlock(pInfo->pRes, "stream scan ignore");
          blockDataCleanup(pInfo->pRes);
          continue;
        }

        if (pBlockInfo->rows > 0) {
          break;
        }
      }
      if (pBlockInfo->rows > 0) {
        break;
      } else {
        pInfo->tqReader->pMsg = NULL;
        continue;
      }
      /*blockDataCleanup(pInfo->pRes);*/
    }

    // record the scan action.
    pInfo->numOfExec++;
    pOperator->resultInfo.totalRows += pBlockInfo->rows;
    // printDataBlock(pInfo->pRes, "stream scan");

    if (pBlockInfo->rows == 0) {
      updateInfoDestoryColseWinSBF(pInfo->pUpdateInfo);
      /*pOperator->status = OP_EXEC_DONE;*/
    } else if (pInfo->pUpdateInfo) {
      checkUpdateData(pInfo, true, pInfo->pRes, true);
      pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, pBlockInfo->window.ekey);
      if (pInfo->pUpdateDataRes->info.rows > 0) {
        pInfo->updateResIndex = 0;
        if (pInfo->pUpdateDataRes->info.type == STREAM_CLEAR) {
          pInfo->scanMode = STREAM_SCAN_FROM_UPDATERES;
        } else if (pInfo->pUpdateDataRes->info.type == STREAM_INVERT) {
          pInfo->scanMode = STREAM_SCAN_FROM_RES;
          return pInfo->pUpdateDataRes;
        } else if (pInfo->pUpdateDataRes->info.type == STREAM_DELETE_DATA) {
          pInfo->scanMode = STREAM_SCAN_FROM_DELETE_DATA;
        }
      }
    }

    qDebug("scan rows: %d", pBlockInfo->rows);
    return (pBlockInfo->rows == 0) ? NULL : pInfo->pRes;
  } else {
    ASSERT(0);
    return NULL;
  }
}

static SArray* extractTableIdList(const STableListInfo* pTableGroupInfo) {
  SArray* tableIdList = taosArrayInit(4, sizeof(uint64_t));

  // Transfer the Array of STableKeyInfo into uid list.
  for (int32_t i = 0; i < taosArrayGetSize(pTableGroupInfo->pTableList); ++i) {
    STableKeyInfo* pkeyInfo = taosArrayGet(pTableGroupInfo->pTableList, i);
    taosArrayPush(tableIdList, &pkeyInfo->uid);
  }

  return tableIdList;
}

static SSDataBlock* doRawScan(SOperatorInfo* pOperator) {
  // NOTE: this operator does never check if current status is done or not
  SExecTaskInfo*      pTaskInfo = pOperator->pTaskInfo;
  SStreamRawScanInfo* pInfo = pOperator->info;
  pTaskInfo->streamInfo.metaRsp.metaRspLen = 0;  // use metaRspLen !=0 to judge if data is meta
  pTaskInfo->streamInfo.metaRsp.metaRsp = NULL;

  qDebug("tmqsnap doRawScan called");
  if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    SSDataBlock* pBlock = &pInfo->pRes;

    if (pInfo->dataReader && tsdbNextDataBlock(pInfo->dataReader)) {
      if (isTaskKilled(pTaskInfo)) {
        longjmp(pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
      }

      tsdbRetrieveDataBlockInfo(pInfo->dataReader, &pBlock->info);

      SArray* pCols = tsdbRetrieveDataBlock(pInfo->dataReader, NULL);
      pBlock->pDataBlock = pCols;
      if (pCols == NULL) {
        longjmp(pTaskInfo->env, terrno);
      }

      qDebug("tmqsnap doRawScan get data uid:%ld", pBlock->info.uid);
      pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__SNAPSHOT_DATA;
      pTaskInfo->streamInfo.lastStatus.uid = pBlock->info.uid;
      pTaskInfo->streamInfo.lastStatus.ts = pBlock->info.window.ekey;
      return pBlock;
    }

    SMetaTableInfo mtInfo = getUidfromSnapShot(pInfo->sContext);
    if (mtInfo.uid == 0) {  // read snapshot done, change to get data from wal
      qDebug("tmqsnap read snapshot done, change to get data from wal");
      pTaskInfo->streamInfo.prepareStatus.uid = mtInfo.uid;
      pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__LOG;
      pTaskInfo->streamInfo.lastStatus.version = pInfo->sContext->snapVersion;
    } else {
      pTaskInfo->streamInfo.prepareStatus.uid = mtInfo.uid;
      pTaskInfo->streamInfo.prepareStatus.ts = INT64_MIN;
      qDebug("tmqsnap change get data uid:%ld", mtInfo.uid);
      qStreamPrepareScan(pTaskInfo, &pTaskInfo->streamInfo.prepareStatus, pInfo->sContext->subType);
    }
    qDebug("tmqsnap stream scan tsdb return null");
    return NULL;
  } else if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__SNAPSHOT_META) {
    SSnapContext* sContext = pInfo->sContext;
    void*         data = NULL;
    int32_t       dataLen = 0;
    int16_t       type = 0;
    int64_t       uid = 0;
    if (getMetafromSnapShot(sContext, &data, &dataLen, &type, &uid) < 0) {
      qError("tmqsnap getMetafromSnapShot error");
      taosMemoryFreeClear(data);
      return NULL;
    }

    if (!sContext->queryMetaOrData) {  // change to get data next poll request
      pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__SNAPSHOT_META;
      pTaskInfo->streamInfo.lastStatus.uid = uid;
      pTaskInfo->streamInfo.metaRsp.rspOffset.type = TMQ_OFFSET__SNAPSHOT_DATA;
      pTaskInfo->streamInfo.metaRsp.rspOffset.uid = 0;
      pTaskInfo->streamInfo.metaRsp.rspOffset.ts = INT64_MIN;
    } else {
      pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__SNAPSHOT_META;
      pTaskInfo->streamInfo.lastStatus.uid = uid;
      pTaskInfo->streamInfo.metaRsp.rspOffset = pTaskInfo->streamInfo.lastStatus;
      pTaskInfo->streamInfo.metaRsp.resMsgType = type;
      pTaskInfo->streamInfo.metaRsp.metaRspLen = dataLen;
      pTaskInfo->streamInfo.metaRsp.metaRsp = data;
    }

    return NULL;
  }
  //  else if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__LOG) {
  //    int64_t fetchVer = pTaskInfo->streamInfo.prepareStatus.version + 1;
  //
  //    while(1){
  //      if (tqFetchLog(pInfo->tqReader->pWalReader, pInfo->sContext->withMeta, &fetchVer, &pInfo->pCkHead) < 0) {
  //        qDebug("tmqsnap tmq poll: consumer log end. offset %" PRId64, fetchVer);
  //        pTaskInfo->streamInfo.lastStatus.version = fetchVer;
  //        pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__LOG;
  //        return NULL;
  //      }
  //      SWalCont* pHead = &pInfo->pCkHead->head;
  //      qDebug("tmqsnap tmq poll: consumer log offset %" PRId64 " msgType %d", fetchVer, pHead->msgType);
  //
  //      if (pHead->msgType == TDMT_VND_SUBMIT) {
  //        SSubmitReq* pCont = (SSubmitReq*)&pHead->body;
  //        tqReaderSetDataMsg(pInfo->tqReader, pCont, 0);
  //        SSDataBlock* block = tqLogScanExec(pInfo->sContext->subType, pInfo->tqReader, pInfo->pFilterOutTbUid,
  //        &pInfo->pRes); if(block){
  //          pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__LOG;
  //          pTaskInfo->streamInfo.lastStatus.version = fetchVer;
  //          qDebug("tmqsnap fetch data msg, ver:%" PRId64 ", type:%d", pHead->version, pHead->msgType);
  //          return block;
  //        }else{
  //          fetchVer++;
  //        }
  //      } else{
  //        ASSERT(pInfo->sContext->withMeta);
  //        ASSERT(IS_META_MSG(pHead->msgType));
  //        qDebug("tmqsnap fetch meta msg, ver:%" PRId64 ", type:%d", pHead->version, pHead->msgType);
  //        pTaskInfo->streamInfo.metaRsp.rspOffset.version = fetchVer;
  //        pTaskInfo->streamInfo.metaRsp.rspOffset.type = TMQ_OFFSET__LOG;
  //        pTaskInfo->streamInfo.metaRsp.resMsgType = pHead->msgType;
  //        pTaskInfo->streamInfo.metaRsp.metaRspLen = pHead->bodyLen;
  //        pTaskInfo->streamInfo.metaRsp.metaRsp = taosMemoryMalloc(pHead->bodyLen);
  //        memcpy(pTaskInfo->streamInfo.metaRsp.metaRsp, pHead->body, pHead->bodyLen);
  //        return NULL;
  //      }
  //    }
  return NULL;
}

static void destroyRawScanOperatorInfo(void* param) {
  SStreamRawScanInfo* pRawScan = (SStreamRawScanInfo*)param;
  tsdbReaderClose(pRawScan->dataReader);
  destroySnapContext(pRawScan->sContext);
  taosMemoryFree(pRawScan);
}

// for subscribing db or stb (not including column),
// if this scan is used, meta data can be return
// and schemas are decided when scanning
SOperatorInfo* createRawScanOperatorInfo(SReadHandle* pHandle, SExecTaskInfo* pTaskInfo) {
  // create operator
  // create tb reader
  // create meta reader
  // create tq reader

  SStreamRawScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamRawScanInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return NULL;
  }

  pInfo->vnode = pHandle->vnode;

  pInfo->sContext = pHandle->sContext;
  pOperator->name = "RawStreamScanOperator";
  //  pOperator->blocking = false;
  //  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(NULL, doRawScan, NULL, NULL, destroyRawScanOperatorInfo, NULL, NULL, NULL);
  return pOperator;
}

static void destroyStreamScanOperatorInfo(void* param) {
  SStreamScanInfo* pStreamScan = (SStreamScanInfo*)param;
  if (pStreamScan->pTableScanOp && pStreamScan->pTableScanOp->info) {
    STableScanInfo* pTableScanInfo = pStreamScan->pTableScanOp->info;
    destroyTableScanOperatorInfo(pTableScanInfo);
    taosMemoryFreeClear(pStreamScan->pTableScanOp);
  }
  if (pStreamScan->tqReader) {
    tqCloseReader(pStreamScan->tqReader);
  }
  if (pStreamScan->pColMatchInfo) {
    taosArrayDestroy(pStreamScan->pColMatchInfo);
  }
  if (pStreamScan->pPseudoExpr) {
    destroyExprInfo(pStreamScan->pPseudoExpr, pStreamScan->numOfPseudoExpr);
    taosMemoryFree(pStreamScan->pPseudoExpr);
  }

  updateInfoDestroy(pStreamScan->pUpdateInfo);
  blockDataDestroy(pStreamScan->pRes);
  blockDataDestroy(pStreamScan->pUpdateRes);
  blockDataDestroy(pStreamScan->pPullDataRes);
  blockDataDestroy(pStreamScan->pDeleteDataRes);
  blockDataDestroy(pStreamScan->pUpdateDataRes);
  taosArrayDestroy(pStreamScan->pBlockLists);
  taosMemoryFree(pStreamScan);
}

SOperatorInfo* createStreamScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode, SNode* pTagCond,
                                            SExecTaskInfo* pTaskInfo) {
  SStreamScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamScanInfo));
  SOperatorInfo*   pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto _error;
  }

  SScanPhysiNode*     pScanPhyNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;

  pInfo->pTagCond = pTagCond;
  pInfo->pGroupTags = pTableScanNode->pGroupTags;
  pInfo->twAggSup = (STimeWindowAggSupp){
      .waterMark = pTableScanNode->watermark,
      .calTrigger = pTableScanNode->triggerType,
      .maxTs = INT64_MIN,
  };

  int32_t numOfCols = 0;
  pInfo->pColMatchInfo = extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID);

  int32_t numOfOutput = taosArrayGetSize(pInfo->pColMatchInfo);
  SArray* pColIds = taosArrayInit(numOfOutput, sizeof(int16_t));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColMatchInfo* id = taosArrayGet(pInfo->pColMatchInfo, i);

    int16_t colId = id->colId;
    taosArrayPush(pColIds, &colId);
    if (id->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pInfo->primaryTsIndex = id->targetSlotId;
    }
  }

  pInfo->pBlockLists = taosArrayInit(4, POINTER_BYTES);
  if (pInfo->pBlockLists == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  if (pHandle->vnode) {
    SOperatorInfo*  pTableScanOp = createTableScanOperatorInfo(pTableScanNode, pHandle, pTaskInfo);
    STableScanInfo* pTSInfo = (STableScanInfo*)pTableScanOp->info;
    if (pHandle->version > 0) {
      pTSInfo->cond.endVersion = pHandle->version;
    }

    SArray* tableList = taosArrayGetP(pTaskInfo->tableqinfoList.pGroupList, 0);
    if (pHandle->initTableReader) {
      pTSInfo->scanMode = TABLE_SCAN__TABLE_ORDER;
      pTSInfo->dataReader = NULL;
      if (tsdbReaderOpen(pHandle->vnode, &pTSInfo->cond, tableList, &pTSInfo->dataReader, NULL) < 0) {
        ASSERT(0);
      }
    }

    if (pHandle->initTqReader) {
      ASSERT(pHandle->tqReader == NULL);
      pInfo->tqReader = tqOpenReader(pHandle->vnode);
      ASSERT(pInfo->tqReader);
    } else {
      ASSERT(pHandle->tqReader);
      pInfo->tqReader = pHandle->tqReader;
    }

    pInfo->pUpdateInfo = NULL;
    pInfo->pTableScanOp = pTableScanOp;
    pInfo->interval = pTSInfo->pdInfo.interval;

    pInfo->readHandle = *pHandle;
    pInfo->tableUid = pScanPhyNode->uid;
    pTaskInfo->streamInfo.snapshotVer = pHandle->version;

    // set the extract column id to streamHandle
    tqReaderSetColIdList(pInfo->tqReader, pColIds);
    SArray* tableIdList = extractTableIdList(&pTaskInfo->tableqinfoList);
    int32_t code = tqReaderSetTbUidList(pInfo->tqReader, tableIdList);
    if (code != 0) {
      taosArrayDestroy(tableIdList);
      goto _error;
    }
    taosArrayDestroy(tableIdList);
    memcpy(&pTaskInfo->streamInfo.tableCond, &pTSInfo->cond, sizeof(SQueryTableDataCond));
  } else {
    taosArrayDestroy(pColIds);
  }

  // create the pseduo columns info
  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    pInfo->pPseudoExpr = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pInfo->numOfPseudoExpr);
  }

  pInfo->pRes = createResDataBlock(pDescNode);
  pInfo->pUpdateRes = createSpecialDataBlock(STREAM_CLEAR);
  pInfo->pCondition = pScanPhyNode->node.pConditions;
  pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
  pInfo->windowSup = (SWindowSupporter){.pStreamAggSup = NULL, .gap = -1, .parentType = QUERY_NODE_PHYSICAL_PLAN};
  pInfo->groupId = 0;
  pInfo->pPullDataRes = createSpecialDataBlock(STREAM_RETRIEVE);
  pInfo->pStreamScanOp = pOperator;
  pInfo->deleteDataIndex = 0;
  pInfo->pDeleteDataRes = createSpecialDataBlock(STREAM_DELETE_DATA);
  pInfo->updateWin = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MAX};
  pInfo->pUpdateDataRes = createSpecialDataBlock(STREAM_CLEAR);
  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;
  pInfo->partitionSup.needCalc = false;

  pOperator->name = "StreamScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);
  pOperator->pTaskInfo = pTaskInfo;

  __optr_fn_t nextFn = pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM ? doStreamScan : doQueueScan;
  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, nextFn, NULL, NULL, destroyStreamScanOperatorInfo, NULL, NULL, NULL);

  return pOperator;

_error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
}

static void destroySysScanOperator(void* param) {
  SSysTableScanInfo* pInfo = (SSysTableScanInfo*)param;
  tsem_destroy(&pInfo->ready);
  blockDataDestroy(pInfo->pRes);

  const char* name = tNameGetTableName(&pInfo->name);
  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0 || pInfo->pCur != NULL) {
    metaCloseTbCursor(pInfo->pCur);
    pInfo->pCur = NULL;
  }

  taosArrayDestroy(pInfo->scanCols);
  taosMemoryFreeClear(pInfo->pUser);

  taosMemoryFreeClear(param);
}

static int32_t getSysTableDbNameColId(const char* pTable) {
  // if (0 == strcmp(TSDB_INS_TABLE_INDEXES, pTable)) {
  //   return 1;
  // }
  return TSDB_INS_USER_STABLES_DBNAME_COLID;
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
      if (getSysTableDbNameColId(node->tableName) == node->colId) {
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

static void getDBNameFromCondition(SNode* pCondition, const char* dbName) {
  if (NULL == pCondition) {
    return;
  }
  nodesWalkExpr(pCondition, getDBNameFromConditionWalker, (char*)dbName);
}

static int32_t loadSysTableCallback(void* param, SDataBuf* pMsg, int32_t code) {
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

  doFilter(pInfo->pCondition, pInfo->pRes, NULL);
  return pInfo->pRes->info.rows == 0 ? NULL : pInfo->pRes;
}

static SSDataBlock* buildInfoSchemaTableMetaBlock(char* tableName) {
  size_t               size = 0;
  const SSysTableMeta* pMeta = NULL;
  getInfosDbMeta(&pMeta, &size);

  int32_t index = 0;
  for (int32_t i = 0; i < size; ++i) {
    if (strcmp(pMeta[i].name, tableName) == 0) {
      index = i;
      break;
    }
  }

  SSDataBlock* pBlock = createDataBlock();
  for (int32_t i = 0; i < pMeta[index].colNum; ++i) {
    SColumnInfoData colInfoData =
        createColumnInfoData(pMeta[index].schema[i].type, pMeta[index].schema[i].bytes, i + 1);
    blockDataAppendColInfo(pBlock, &colInfoData);
  }

  return pBlock;
}

int32_t convertTagDataToStr(char* str, int type, void* buf, int32_t bufSize, int32_t* len) {
  int32_t n = 0;

  switch (type) {
    case TSDB_DATA_TYPE_NULL:
      n = sprintf(str, "null");
      break;

    case TSDB_DATA_TYPE_BOOL:
      n = sprintf(str, (*(int8_t*)buf) ? "true" : "false");
      break;

    case TSDB_DATA_TYPE_TINYINT:
      n = sprintf(str, "%d", *(int8_t*)buf);
      break;

    case TSDB_DATA_TYPE_SMALLINT:
      n = sprintf(str, "%d", *(int16_t*)buf);
      break;

    case TSDB_DATA_TYPE_INT:
      n = sprintf(str, "%d", *(int32_t*)buf);
      break;

    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_TIMESTAMP:
      n = sprintf(str, "%" PRId64, *(int64_t*)buf);
      break;

    case TSDB_DATA_TYPE_FLOAT:
      n = sprintf(str, "%.5f", GET_FLOAT_VAL(buf));
      break;

    case TSDB_DATA_TYPE_DOUBLE:
      n = sprintf(str, "%.9f", GET_DOUBLE_VAL(buf));
      break;

    case TSDB_DATA_TYPE_BINARY:
      if (bufSize < 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      memcpy(str, buf, bufSize);
      n = bufSize;
      break;
    case TSDB_DATA_TYPE_NCHAR:
      if (bufSize < 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }

      int32_t length = taosUcs4ToMbs((TdUcs4*)buf, bufSize, str);
      if (length <= 0) {
        return TSDB_CODE_TSC_INVALID_VALUE;
      }
      n = length;
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      n = sprintf(str, "%u", *(uint8_t*)buf);
      break;

    case TSDB_DATA_TYPE_USMALLINT:
      n = sprintf(str, "%u", *(uint16_t*)buf);
      break;

    case TSDB_DATA_TYPE_UINT:
      n = sprintf(str, "%u", *(uint32_t*)buf);
      break;

    case TSDB_DATA_TYPE_UBIGINT:
      n = sprintf(str, "%" PRIu64, *(uint64_t*)buf);
      break;

    default:
      return TSDB_CODE_TSC_INVALID_VALUE;
  }

  if (len) *len = n;

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* sysTableScanUserTags(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

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

  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TAGS);
  blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);

  int32_t ret = 0;
  while ((ret = metaTbCursorNext(pInfo->pCur)) == 0) {
    if (pInfo->pCur->mr.me.type != TSDB_CHILD_TABLE) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

    SMetaReader smr = {0};
    metaReaderInit(&smr, pInfo->readHandle.meta, 0);

    uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
    int32_t  code = metaGetTableEntryByUid(&smr, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno),
             GET_TASKID(pTaskInfo));
      metaReaderClear(&smr);
      metaCloseTbCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    char stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(stableName, smr.me.name);

    int32_t numOfTags = smr.me.stbEntry.schemaTag.nCols;
    for (int32_t i = 0; i < numOfTags; ++i) {
      SColumnInfoData* pColInfoData = NULL;

      // table name
      pColInfoData = taosArrayGet(p->pDataBlock, 0);
      colDataAppend(pColInfoData, numOfRows, tableName, false);

      // database name
      pColInfoData = taosArrayGet(p->pDataBlock, 1);
      colDataAppend(pColInfoData, numOfRows, dbname, false);

      // super table name
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      colDataAppend(pColInfoData, numOfRows, stableName, false);

      // tag name
      char tagName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(tagName, smr.me.stbEntry.schemaTag.pSchema[i].name);
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      colDataAppend(pColInfoData, numOfRows, tagName, false);

      // tag type
      int8_t tagType = smr.me.stbEntry.schemaTag.pSchema[i].type;
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      char tagTypeStr[VARSTR_HEADER_SIZE + 32];
      int  tagTypeLen = sprintf(varDataVal(tagTypeStr), "%s", tDataTypes[tagType].name);
      if (tagType == TSDB_DATA_TYPE_VARCHAR) {
        tagTypeLen += sprintf(varDataVal(tagTypeStr) + tagTypeLen, "(%d)",
                              (int32_t)(smr.me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE));
      } else if (tagType == TSDB_DATA_TYPE_NCHAR) {
        tagTypeLen +=
            sprintf(varDataVal(tagTypeStr) + tagTypeLen, "(%d)",
                    (int32_t)((smr.me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
      }
      varDataSetLen(tagTypeStr, tagTypeLen);
      colDataAppend(pColInfoData, numOfRows, (char*)tagTypeStr, false);

      STagVal tagVal = {0};
      tagVal.cid = smr.me.stbEntry.schemaTag.pSchema[i].colId;
      char*    tagData = NULL;
      uint32_t tagLen = 0;

      if (tagType == TSDB_DATA_TYPE_JSON) {
        tagData = (char*)pInfo->pCur->mr.me.ctbEntry.pTags;
      } else {
        bool exist = tTagGet((STag*)pInfo->pCur->mr.me.ctbEntry.pTags, &tagVal);
        if (exist) {
          if (IS_VAR_DATA_TYPE(tagType)) {
            tagData = (char*)tagVal.pData;
            tagLen = tagVal.nData;
          } else {
            tagData = (char*)&tagVal.i64;
            tagLen = tDataTypes[tagType].bytes;
          }
        }
      }

      char* tagVarChar = NULL;
      if (tagData != NULL) {
        if (tagType == TSDB_DATA_TYPE_JSON) {
          char* tagJson = parseTagDatatoJson(tagData);
          tagVarChar = taosMemoryMalloc(strlen(tagJson) + VARSTR_HEADER_SIZE);
          memcpy(varDataVal(tagVarChar), tagJson, strlen(tagJson));
          varDataSetLen(tagVarChar, strlen(tagJson));
          taosMemoryFree(tagJson);
        } else {
          int32_t bufSize = IS_VAR_DATA_TYPE(tagType) ? (tagLen + VARSTR_HEADER_SIZE)
                                                      : (3 + DBL_MANT_DIG - DBL_MIN_EXP + VARSTR_HEADER_SIZE);
          tagVarChar = taosMemoryMalloc(bufSize);
          int32_t len = -1;
          convertTagDataToStr(varDataVal(tagVarChar), tagType, tagData, tagLen, &len);
          varDataSetLen(tagVarChar, len);
        }
      }
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataAppend(pColInfoData, numOfRows, tagVarChar,
                    (tagData == NULL) || (tagType == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(tagData)));
      taosMemoryFree(tagVarChar);
      ++numOfRows;
    }
    metaReaderClear(&smr);

    if (numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      relocateColumnData(pInfo->pRes, pInfo->scanCols, p->pDataBlock, false);
      doFilterResult(pInfo);

      blockDataCleanup(p);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  }

  if (numOfRows > 0) {
    p->info.rows = numOfRows;
    pInfo->pRes->info.rows = numOfRows;

    relocateColumnData(pInfo->pRes, pInfo->scanCols, p->pDataBlock, false);
    doFilterResult(pInfo);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  blockDataDestroy(p);

  // todo temporarily free the cursor here, the true reason why the free is not valid needs to be found
  if (ret != 0) {
    metaCloseTbCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    doSetOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableScanUserTables(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    buildSysDbTableInfo(pInfo, pOperator->resultInfo.capacity);

    doFilterResult(pInfo);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

    doSetOperatorCompleted(pOperator);
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

    SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
    blockDataEnsureCapacity(p, pOperator->resultInfo.capacity);

    char n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};

    int32_t ret = 0;
    while ((ret = metaTbCursorNext(pInfo->pCur)) == 0) {
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

      int32_t tableType = pInfo->pCur->mr.me.type;
      if (tableType == TSDB_CHILD_TABLE) {
        // create time
        int64_t ts = pInfo->pCur->mr.me.ctbEntry.ctime;
        pColInfoData = taosArrayGet(p->pDataBlock, 2);
        colDataAppend(pColInfoData, numOfRows, (char*)&ts, false);

        SMetaReader mr = {0};
        metaReaderInit(&mr, pInfo->readHandle.meta, 0);

        uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
        int32_t  code = metaGetTableEntryByUid(&mr, suid);
        if (code != TSDB_CODE_SUCCESS) {
          qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
                 suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
          metaReaderClear(&mr);
          metaCloseTbCursor(pInfo->pCur);
          pInfo->pCur = NULL;
          T_LONG_JMP(pTaskInfo->env, terrno);
        }

        // number of columns
        pColInfoData = taosArrayGet(p->pDataBlock, 3);
        colDataAppend(pColInfoData, numOfRows, (char*)&mr.me.stbEntry.schemaRow.nCols, false);

        // super table name
        STR_TO_VARSTR(n, mr.me.name);
        pColInfoData = taosArrayGet(p->pDataBlock, 4);
        colDataAppend(pColInfoData, numOfRows, n, false);
        metaReaderClear(&mr);

        // table comment
        pColInfoData = taosArrayGet(p->pDataBlock, 8);
        if (pInfo->pCur->mr.me.ctbEntry.commentLen > 0) {
          char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
          STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ctbEntry.comment);
          colDataAppend(pColInfoData, numOfRows, comment, false);
        } else if (pInfo->pCur->mr.me.ctbEntry.commentLen == 0) {
          char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
          STR_TO_VARSTR(comment, "");
          colDataAppend(pColInfoData, numOfRows, comment, false);
        } else {
          colDataAppendNULL(pColInfoData, numOfRows);
        }

        // uid
        pColInfoData = taosArrayGet(p->pDataBlock, 5);
        colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);

        // ttl
        pColInfoData = taosArrayGet(p->pDataBlock, 7);
        colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ctbEntry.ttlDays, false);

        STR_TO_VARSTR(n, "CHILD_TABLE");
      } else if (tableType == TSDB_NORMAL_TABLE) {
        // create time
        pColInfoData = taosArrayGet(p->pDataBlock, 2);
        colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ctime, false);

        // number of columns
        pColInfoData = taosArrayGet(p->pDataBlock, 3);
        colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.schemaRow.nCols, false);

        // super table name
        pColInfoData = taosArrayGet(p->pDataBlock, 4);
        colDataAppendNULL(pColInfoData, numOfRows);

        // table comment
        pColInfoData = taosArrayGet(p->pDataBlock, 8);
        if (pInfo->pCur->mr.me.ntbEntry.commentLen > 0) {
          char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
          STR_TO_VARSTR(comment, pInfo->pCur->mr.me.ntbEntry.comment);
          colDataAppend(pColInfoData, numOfRows, comment, false);
        } else if (pInfo->pCur->mr.me.ntbEntry.commentLen == 0) {
          char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
          STR_TO_VARSTR(comment, "");
          colDataAppend(pColInfoData, numOfRows, comment, false);
        } else {
          colDataAppendNULL(pColInfoData, numOfRows);
        }

        // uid
        pColInfoData = taosArrayGet(p->pDataBlock, 5);
        colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.uid, false);

        // ttl
        pColInfoData = taosArrayGet(p->pDataBlock, 7);
        colDataAppend(pColInfoData, numOfRows, (char*)&pInfo->pCur->mr.me.ntbEntry.ttlDays, false);

        STR_TO_VARSTR(n, "NORMAL_TABLE");
      }

      pColInfoData = taosArrayGet(p->pDataBlock, 9);
      colDataAppend(pColInfoData, numOfRows, n, false);

      if (++numOfRows >= pOperator->resultInfo.capacity) {
        p->info.rows = numOfRows;
        pInfo->pRes->info.rows = numOfRows;

        relocateColumnData(pInfo->pRes, pInfo->scanCols, p->pDataBlock, false);
        doFilterResult(pInfo);

        blockDataCleanup(p);
        numOfRows = 0;

        if (pInfo->pRes->info.rows > 0) {
          break;
        }
      }
    }

    if (numOfRows > 0) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      relocateColumnData(pInfo->pRes, pInfo->scanCols, p->pDataBlock, false);
      doFilterResult(pInfo);

      blockDataCleanup(p);
      numOfRows = 0;
    }

    blockDataDestroy(p);

    // todo temporarily free the cursor here, the true reason why the free is not valid needs to be found
    if (ret != 0) {
      metaCloseTbCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      doSetOperatorCompleted(pOperator);
    }

    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  }
}

static SSDataBlock* sysTableScanUserSTables(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  pInfo->pRes->info.rows = 0;
  pOperator->status = OP_EXEC_DONE;

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* doSysTableScan(SOperatorInfo* pOperator) {
  // build message and send to mnode to fetch the content of system tables.
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  char               dbName[TSDB_DB_NAME_LEN] = {0};

  const char* name = tNameGetTableName(&pInfo->name);
  if (pInfo->showRewrite) {
    getDBNameFromCondition(pInfo->pCondition, dbName);
    sprintf(pInfo->req.db, "%d.%s", pInfo->accountId, dbName);
  }

  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0) {
    return sysTableScanUserTables(pOperator);
  } else if (strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
    return sysTableScanUserTags(pOperator);
  } else if (strncasecmp(name, TSDB_INS_TABLE_STABLES, TSDB_TABLE_FNAME_LEN) == 0 && pInfo->showRewrite &&
             IS_SYS_DBNAME(dbName)) {
    return sysTableScanUserSTables(pOperator);
  } else {  // load the meta from mnode of the given epset
    if (pOperator->status == OP_EXEC_DONE) {
      return NULL;
    }

    while (1) {
      int64_t startTs = taosGetTimestampUs();
      strncpy(pInfo->req.tb, tNameGetTableName(&pInfo->name), tListLen(pInfo->req.tb));
      strcpy(pInfo->req.user, pInfo->pUser);

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

      int32_t msgType = (strcasecmp(name, TSDB_INS_TABLE_DNODE_VARIABLES) == 0) ? TDMT_DND_SYSTABLE_RETRIEVE
                                                                                : TDMT_MND_SYSTABLE_RETRIEVE;

      pMsgSendInfo->param = pOperator;
      pMsgSendInfo->msgInfo.pData = buf1;
      pMsgSendInfo->msgInfo.len = contLen;
      pMsgSendInfo->msgType = msgType;
      pMsgSendInfo->fp = loadSysTableCallback;
      pMsgSendInfo->requestId = pTaskInfo->id.queryId;

      int64_t transporterId = 0;
      int32_t code =
          asyncSendMsgToServer(pInfo->readHandle.pMsgCb->clientRpc, &pInfo->epSet, &transporterId, pMsgSendInfo);
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
        qDebug("%s load meta data from mnode completed, rowsOfSource:%d, totalRows:%" PRIu64, GET_TASKID(pTaskInfo),
               pRsp->numOfRows, pInfo->loadInfo.totalRows);

        if (pRsp->numOfRows == 0) {
          taosMemoryFree(pRsp);
          return NULL;
        }
      }

      char* pStart = pRsp->data;
      extractDataBlockFromFetchRsp(pInfo->pRes, pRsp->data, pInfo->scanCols, &pStart);
      updateLoadRemoteInfo(&pInfo->loadInfo, pRsp->numOfRows, pRsp->compLen, startTs, pOperator);

      // todo log the filter info
      doFilterResult(pInfo);
      taosMemoryFree(pRsp);
      if (pInfo->pRes->info.rows > 0) {
        return pInfo->pRes;
      } else if (pOperator->status == OP_EXEC_DONE) {
        return NULL;
      }
    }
  }
}

int32_t buildSysDbTableInfo(const SSysTableScanInfo* pInfo, int32_t capacity) {
  SSDataBlock* p = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TABLES);
  blockDataEnsureCapacity(p, capacity);

  size_t               size = 0;
  const SSysTableMeta* pSysDbTableMeta = NULL;

  getInfosDbMeta(&pSysDbTableMeta, &size);
  p->info.rows = buildDbTableInfoBlock(pInfo->sysInfo, p, pSysDbTableMeta, size, TSDB_INFORMATION_SCHEMA_DB);

  getPerfDbMeta(&pSysDbTableMeta, &size);
  p->info.rows = buildDbTableInfoBlock(pInfo->sysInfo, p, pSysDbTableMeta, size, TSDB_PERFORMANCE_SCHEMA_DB);

  pInfo->pRes->info.rows = p->info.rows;
  relocateColumnData(pInfo->pRes, pInfo->scanCols, p->pDataBlock, false);
  blockDataDestroy(p);

  return pInfo->pRes->info.rows;
}

int32_t buildDbTableInfoBlock(bool sysInfo, const SSDataBlock* p, const SSysTableMeta* pSysDbTableMeta, size_t size,
                              const char* dbName) {
  char    n[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t numOfRows = p->info.rows;

  for (int32_t i = 0; i < size; ++i) {
    const SSysTableMeta* pm = &pSysDbTableMeta[i];
    if (!sysInfo && pm->sysInfo) {
      continue;
    }

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

    for (int32_t j = 4; j <= 8; ++j) {
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

SOperatorInfo* createSysTableScanOperatorInfo(void* readHandle, SSystemTableScanPhysiNode* pScanPhyNode,
                                              const char* pUser, SExecTaskInfo* pTaskInfo) {
  SSysTableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SSysTableScanInfo));
  SOperatorInfo*     pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SScanPhysiNode* pScanNode = &pScanPhyNode->scan;

  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;
  SSDataBlock*        pResBlock = createResDataBlock(pDescNode);

  int32_t num = 0;
  SArray* colList = extractColMatchInfo(pScanNode->pScanCols, pDescNode, &num, COL_MATCH_FROM_COL_ID);

  pInfo->accountId = pScanPhyNode->accountId;
  pInfo->pUser = taosMemoryStrDup((void*)pUser);
  pInfo->sysInfo = pScanPhyNode->sysInfo;
  pInfo->showRewrite = pScanPhyNode->showRewrite;
  pInfo->pRes = pResBlock;
  pInfo->pCondition = pScanNode->node.pConditions;
  pInfo->scanCols = colList;

  initResultSizeInfo(&pOperator->resultInfo, 4096);

  tNameAssign(&pInfo->name, &pScanNode->tableName);
  const char* name = tNameGetTableName(&pInfo->name);

  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
    pInfo->readHandle = *(SReadHandle*)readHandle;
    blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  } else {
    tsem_init(&pInfo->ready, 0, 0);
    pInfo->epSet = pScanPhyNode->mgmtEpSet;
    pInfo->readHandle = *(SReadHandle*)readHandle;
  }

  pOperator->name = "SysTableScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_SYSTABLE_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pResBlock->pDataBlock);
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doSysTableScan, NULL, NULL, destroySysScanOperator, NULL, NULL, NULL);

  return pOperator;

_error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
  return NULL;
}

static SSDataBlock* doTagScan(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  STagScanInfo* pInfo = pOperator->info;
  SExprInfo*    pExprInfo = &pOperator->exprSupp.pExprInfo[0];
  SSDataBlock*  pRes = pInfo->pRes;

  int32_t size = taosArrayGetSize(pInfo->pTableList->pTableList);
  if (size == 0) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    return NULL;
  }

  char        str[512] = {0};
  int32_t     count = 0;
  SMetaReader mr = {0};
  metaReaderInit(&mr, pInfo->readHandle.meta, 0);

  while (pInfo->curPos < size && count < pOperator->resultInfo.capacity) {
    STableKeyInfo* item = taosArrayGet(pInfo->pTableList->pTableList, pInfo->curPos);
    int32_t        code = metaGetTableEntryByUid(&mr, item->uid);
    tDecoderClear(&mr.coder);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", item->uid, tstrerror(terrno),
             GET_TASKID(pTaskInfo));
      metaReaderClear(&mr);
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
      SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);

      // refactor later
      if (fmIsScanPseudoColumnFunc(pExprInfo[j].pExpr->_function.functionId)) {
        STR_TO_VARSTR(str, mr.me.name);
        colDataAppend(pDst, count, str, false);
      } else {  // it is a tag value
        STagVal val = {0};
        val.cid = pExprInfo[j].base.pParam[0].pCol->colId;
        const char* p = metaGetTableTagVal(mr.me.ctbEntry.pTags, pDst->info.type, &val);

        char* data = NULL;
        if (pDst->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
          data = tTagValToData((const STagVal*)p, false);
        } else {
          data = (char*)p;
        }
        colDataAppend(pDst, count, data,
                      (data == NULL) || (pDst->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data)));

        if (pDst->info.type != TSDB_DATA_TYPE_JSON && p != NULL && IS_VAR_DATA_TYPE(((const STagVal*)p)->type) &&
            data != NULL) {
          taosMemoryFree(data);
        }
      }
    }

    count += 1;
    if (++pInfo->curPos >= size) {
      doSetOperatorCompleted(pOperator);
    }
  }

  metaReaderClear(&mr);

  // qDebug("QInfo:0x%"PRIx64" create tag values results completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  if (pOperator->status == OP_EXEC_DONE) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
  }

  pRes->info.rows = count;
  pOperator->resultInfo.totalRows += count;

  return (pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static void destroyTagScanOperatorInfo(void* param) {
  STagScanInfo* pInfo = (STagScanInfo*)param;
  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  taosArrayDestroy(pInfo->pColMatchInfo);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pPhyNode,
                                         STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo) {
  STagScanInfo*  pInfo = taosMemoryCalloc(1, sizeof(STagScanInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pPhyNode->node.pOutputDataBlockDesc;

  int32_t    num = 0;
  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->pScanPseudoCols, NULL, &numOfExprs);
  SArray*    colList = extractColMatchInfo(pPhyNode->pScanPseudoCols, pDescNode, &num, COL_MATCH_FROM_COL_ID);

  int32_t code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pTableList = pTableListInfo;
  pInfo->pColMatchInfo = colList;
  pInfo->pRes = createResDataBlock(pDescNode);
  pInfo->readHandle = *pReadHandle;
  pInfo->curPos = 0;

  pOperator->name = "TagScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN;

  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doTagScan, NULL, NULL, destroyTagScanOperatorInfo, NULL, NULL, NULL);

  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

int32_t createScanTableListInfo(SScanPhysiNode* pScanNode, SNodeList* pGroupTags, bool groupSort, SReadHandle* pHandle,
                                STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond,
                                const char* idStr) {
  int64_t st = taosGetTimestampUs();

  int32_t code = getTableList(pHandle->meta, pHandle->vnode, pScanNode, pTagCond, pTagIndexCond, pTableListInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to getTableList, code: %s", tstrerror(code));
    return code;
  }

  int64_t st1 = taosGetTimestampUs();
  qDebug("generate queried table list completed, elapsed time:%.2f ms %s", (st1 - st) / 1000.0, idStr);

  if (taosArrayGetSize(pTableListInfo->pTableList) == 0) {
    qDebug("no table qualified for query, %s" PRIx64, idStr);
    return TSDB_CODE_SUCCESS;
  }

  pTableListInfo->needSortTableByGroupId = groupSort;
  code = generateGroupIdMap(pTableListInfo, pHandle, pGroupTags);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int64_t st2 = taosGetTimestampUs();
  qDebug("generate group id map completed, elapsed time:%.2f ms %s", (st2 - st1) / 1000.0, idStr);

  return TSDB_CODE_SUCCESS;
}

int32_t createMultipleDataReaders(SQueryTableDataCond* pQueryCond, SReadHandle* pHandle, STableListInfo* pTableListInfo,
                                  int32_t tableStartIdx, int32_t tableEndIdx, SArray* arrayReader, const char* idstr) {
  for (int32_t i = tableStartIdx; i <= tableEndIdx; ++i) {
    SArray* subTableList = taosArrayInit(1, sizeof(STableKeyInfo));
    taosArrayPush(subTableList, taosArrayGet(pTableListInfo->pTableList, i));

    STsdbReader* pReader = NULL;
    tsdbReaderOpen(pHandle->vnode, pQueryCond, subTableList, &pReader, idstr);
    taosArrayPush(arrayReader, &pReader);

    taosArrayDestroy(subTableList);
  }

  return TSDB_CODE_SUCCESS;
}

// todo refactor
static int32_t loadDataBlockFromOneTable(SOperatorInfo* pOperator, STableMergeScanInfo* pTableScanInfo,
                                         int32_t readerIdx, SSDataBlock* pBlock, uint32_t* status) {
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperator->info;

  SFileBlockLoadRecorder* pCost = &pTableScanInfo->readRecorder;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;

  *status = pInfo->dataBlockLoadFlag;
  if (pTableScanInfo->pFilterNode != NULL ||
      overlapWithTimeWindow(&pTableScanInfo->interval, &pBlock->info, pTableScanInfo->cond.order)) {
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

    // clear all data in pBlock that are set when handing the previous block
    for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
      SColumnInfoData* pcol = taosArrayGet(pBlock->pDataBlock, i);
      pcol->pData = NULL;
    }

    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_STATIS_LOAD) {
    pCost->loadBlockStatis += 1;

    bool             allColumnsHaveAgg = true;
    SColumnDataAgg** pColAgg = NULL;
    STsdbReader*     reader = taosArrayGetP(pTableScanInfo->dataReaders, readerIdx);
    tsdbRetrieveDatablockSMA(reader, &pColAgg, &allColumnsHaveAgg);

    if (allColumnsHaveAgg == true) {
      int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

      // todo create this buffer during creating operator
      if (pBlock->pBlockAgg == NULL) {
        pBlock->pBlockAgg = taosMemoryCalloc(numOfCols, POINTER_BYTES);
      }

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
  if (!doFilterByBlockSMA(pBlock->pBlockStatis, pTableScanInfo->pCtx, pBlockInfo->rows)) {
    pCost->filterOutBlocks += 1;
    qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo), pBlockInfo->window.skey,
           pBlockInfo->window.ekey, pBlockInfo->rows);
    (*status) = FUNC_DATA_REQUIRED_FILTEROUT;
    return TSDB_CODE_SUCCESS;
  }
#endif

  pCost->totalCheckedRows += pBlock->info.rows;
  pCost->loadBlocks += 1;

  STsdbReader* reader = taosArrayGetP(pTableScanInfo->dataReaders, readerIdx);
  SArray*      pCols = tsdbRetrieveDataBlock(reader, NULL);
  if (pCols == NULL) {
    return terrno;
  }

  relocateColumnData(pBlock, pTableScanInfo->pColMatchInfo, pCols, true);

  // currently only the tbname pseudo column
  if (pTableScanInfo->pseudoSup.numOfExprs > 0) {
    int32_t code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pTableScanInfo->pseudoSup.pExprInfo,
                                          pTableScanInfo->pseudoSup.numOfExprs, pBlock, GET_TASKID(pTaskInfo));
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  if (pTableScanInfo->pFilterNode != NULL) {
    int64_t st = taosGetTimestampMs();
    doFilter(pTableScanInfo->pFilterNode, pBlock, pTableScanInfo->pColMatchInfo);

    double el = (taosGetTimestampUs() - st) / 1000.0;
    pTableScanInfo->readRecorder.filterTime += el;

    if (pBlock->info.rows == 0) {
      pCost->filterOutBlocks += 1;
      qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d, elapsed time:%.2f ms",
             GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, el);
    } else {
      qDebug("%s data block filter applied, elapsed time:%.2f ms", GET_TASKID(pTaskInfo), el);
    }
  }

  return TSDB_CODE_SUCCESS;
}

typedef struct STableMergeScanSortSourceParam {
  SOperatorInfo* pOperator;
  int32_t        readerIdx;
  SSDataBlock*   inputBlock;
} STableMergeScanSortSourceParam;

static SSDataBlock* getTableDataBlock(void* param) {
  STableMergeScanSortSourceParam* source = param;
  SOperatorInfo*                  pOperator = source->pOperator;
  int32_t                         readerIdx = source->readerIdx;
  SSDataBlock*                    pBlock = source->inputBlock;
  STableMergeScanInfo*            pTableScanInfo = pOperator->info;

  int64_t st = taosGetTimestampUs();

  blockDataCleanup(pBlock);

  STsdbReader* reader = taosArrayGetP(pTableScanInfo->dataReaders, readerIdx);
  while (tsdbNextDataBlock(reader)) {
    if (isTaskKilled(pOperator->pTaskInfo)) {
      T_LONG_JMP(pOperator->pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    // process this data block based on the probabilities
    bool processThisBlock = processBlockWithProbability(&pTableScanInfo->sample);
    if (!processThisBlock) {
      continue;
    }

    blockDataCleanup(pBlock);
    SDataBlockInfo binfo = pBlock->info;
    tsdbRetrieveDataBlockInfo(reader, &binfo);

    blockDataEnsureCapacity(pBlock, binfo.rows);
    pBlock->info.type = binfo.type;
    pBlock->info.uid = binfo.uid;
    pBlock->info.window = binfo.window;
    pBlock->info.rows = binfo.rows;

    uint32_t status = 0;
    int32_t  code = loadDataBlockFromOneTable(pOperator, pTableScanInfo, readerIdx, pBlock, &status);
    //    int32_t  code = loadDataBlockOnDemand(pOperator->pRuntimeEnv, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pOperator->pTaskInfo->env, code);
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
      continue;
    }

    uint64_t* groupId = taosHashGet(pOperator->pTaskInfo->tableqinfoList.map, &pBlock->info.uid, sizeof(int64_t));
    if (groupId) {
      pBlock->info.groupId = *groupId;
    }

    pOperator->resultInfo.totalRows = pTableScanInfo->readRecorder.totalRows;
    pTableScanInfo->readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;

    return pBlock;
  }
  return NULL;
}

SArray* generateSortByTsInfo(SArray* colMatchInfo, int32_t order) {
  int32_t tsTargetSlotId = 0;
  for (int32_t i = 0; i < taosArrayGetSize(colMatchInfo); ++i) {
    SColMatchInfo* colInfo = taosArrayGet(colMatchInfo, i);
    if (colInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      tsTargetSlotId = colInfo->targetSlotId;
    }
  }

  SArray*         pList = taosArrayInit(1, sizeof(SBlockOrderInfo));
  SBlockOrderInfo bi = {0};
  bi.order = order;
  bi.slotId = tsTargetSlotId;
  bi.nullFirst = NULL_ORDER_FIRST;

  taosArrayPush(pList, &bi);

  return pList;
}

int32_t startGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  {
    size_t  tableListSize = taosArrayGetSize(pInfo->tableListInfo->pTableList);
    int32_t i = pInfo->tableStartIndex + 1;
    for (; i < tableListSize; ++i) {
      STableKeyInfo* tableKeyInfo = taosArrayGet(pInfo->tableListInfo->pTableList, i);
      if (tableKeyInfo->groupId != pInfo->groupId) {
        break;
      }
    }
    pInfo->tableEndIndex = i - 1;
  }

  int32_t tableStartIdx = pInfo->tableStartIndex;
  int32_t tableEndIdx = pInfo->tableEndIndex;

  STableListInfo* tableListInfo = pInfo->tableListInfo;
  pInfo->dataReaders = taosArrayInit(64, POINTER_BYTES);
  createMultipleDataReaders(&pInfo->cond, &pInfo->readHandle, tableListInfo, tableStartIdx, tableEndIdx,
                            pInfo->dataReaders, GET_TASKID(pTaskInfo));

  // todo the total available buffer should be determined by total capacity of buffer of this task.
  // the additional one is reserved for merge result
  pInfo->sortBufSize = pInfo->bufPageSize * (tableEndIdx - tableStartIdx + 1 + 1);
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->pSortInputBlock, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, getTableDataBlock, NULL, NULL);

  size_t numReaders = taosArrayGetSize(pInfo->dataReaders);
  for (int32_t i = 0; i < numReaders; ++i) {
    STableMergeScanSortSourceParam param = {0};
    param.readerIdx = i;
    param.pOperator = pOperator;
    param.inputBlock = createOneDataBlock(pInfo->pResBlock, false);
    taosArrayPush(pInfo->sortSourceParams, &param);
  }

  for (int32_t i = 0; i < numReaders; ++i) {
    SSortSource*                    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    STableMergeScanSortSourceParam* param = taosArrayGet(pInfo->sortSourceParams, i);
    ps->param = param;
    tsortAddSource(pInfo->pSortHandle, ps);
  }

  int32_t code = tsortOpen(pInfo->pSortHandle);

  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stopGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  size_t numReaders = taosArrayGetSize(pInfo->dataReaders);

  SSortExecInfo sortExecInfo = tsortGetSortExecInfo(pInfo->pSortHandle);
  pInfo->sortExecInfo.sortMethod = sortExecInfo.sortMethod;
  pInfo->sortExecInfo.sortBuffer = sortExecInfo.sortBuffer;
  pInfo->sortExecInfo.loops += sortExecInfo.loops;
  pInfo->sortExecInfo.readBytes += sortExecInfo.readBytes;
  pInfo->sortExecInfo.writeBytes += sortExecInfo.writeBytes;

  for (int32_t i = 0; i < numReaders; ++i) {
    STableMergeScanSortSourceParam* param = taosArrayGet(pInfo->sortSourceParams, i);
    blockDataDestroy(param->inputBlock);
  }
  taosArrayClear(pInfo->sortSourceParams);

  tsortDestroySortHandle(pInfo->pSortHandle);

  for (int32_t i = 0; i < numReaders; ++i) {
    STsdbReader* reader = taosArrayGetP(pInfo->dataReaders, i);
    tsdbReaderClose(reader);
  }
  taosArrayDestroy(pInfo->dataReaders);
  pInfo->dataReaders = NULL;
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* getSortedTableMergeScanBlockData(SSortHandle* pHandle, SSDataBlock* pResBlock, int32_t capacity,
                                              SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  blockDataCleanup(pResBlock);
  blockDataEnsureCapacity(pResBlock, capacity);

  while (1) {
    STupleHandle* pTupleHandle = tsortNextTuple(pHandle);
    if (pTupleHandle == NULL) {
      break;
    }

    appendOneRowToDataBlock(pResBlock, pTupleHandle);
    if (pResBlock->info.rows >= capacity) {
      break;
    }
  }

  qDebug("%s get sorted row blocks, rows:%d", GET_TASKID(pTaskInfo), pResBlock->info.rows);
  return (pResBlock->info.rows > 0) ? pResBlock : NULL;
}

SSDataBlock* doTableMergeScan(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }
  size_t tableListSize = taosArrayGetSize(pInfo->tableListInfo->pTableList);
  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->tableStartIndex = 0;
    pInfo->groupId = ((STableKeyInfo*)taosArrayGet(pInfo->tableListInfo->pTableList, pInfo->tableStartIndex))->groupId;
    startGroupTableMergeScan(pOperator);
  }
  SSDataBlock* pBlock = NULL;
  while (pInfo->tableStartIndex < tableListSize) {
    pBlock = getSortedTableMergeScanBlockData(pInfo->pSortHandle, pInfo->pResBlock, pOperator->resultInfo.capacity,
                                              pOperator);
    if (pBlock != NULL) {
      pBlock->info.groupId = pInfo->groupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      return pBlock;
    } else {
      stopGroupTableMergeScan(pOperator);
      if (pInfo->tableEndIndex >= tableListSize - 1) {
        doSetOperatorCompleted(pOperator);
        break;
      }
      pInfo->tableStartIndex = pInfo->tableEndIndex + 1;
      pInfo->groupId =
          ((STableKeyInfo*)taosArrayGet(pInfo->tableListInfo->pTableList, pInfo->tableStartIndex))->groupId;
      startGroupTableMergeScan(pOperator);
    }
  }

  return pBlock;
}

void destroyTableMergeScanOperatorInfo(void* param) {
  STableMergeScanInfo* pTableScanInfo = (STableMergeScanInfo*)param;
  cleanupQueryTableDataCond(&pTableScanInfo->cond);
  taosArrayDestroy(pTableScanInfo->sortSourceParams);

  for (int32_t i = 0; i < taosArrayGetSize(pTableScanInfo->dataReaders); ++i) {
    STsdbReader* reader = taosArrayGetP(pTableScanInfo->dataReaders, i);
    tsdbReaderClose(reader);
  }
  taosArrayDestroy(pTableScanInfo->dataReaders);

  if (pTableScanInfo->pColMatchInfo != NULL) {
    taosArrayDestroy(pTableScanInfo->pColMatchInfo);
  }

  pTableScanInfo->pResBlock = blockDataDestroy(pTableScanInfo->pResBlock);
  pTableScanInfo->pSortInputBlock = blockDataDestroy(pTableScanInfo->pSortInputBlock);

  taosArrayDestroy(pTableScanInfo->pSortInfo);
  cleanupExprSupp(&pTableScanInfo->pseudoSup);

  taosMemoryFreeClear(pTableScanInfo->rowEntryInfoOffset);
  taosMemoryFreeClear(param);
}

typedef struct STableMergeScanExecInfo {
  SFileBlockLoadRecorder blockRecorder;
  SSortExecInfo          sortExecInfo;
} STableMergeScanExecInfo;

int32_t getTableMergeScanExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  ASSERT(pOptr != NULL);
  // TODO: merge these two info into one struct
  STableMergeScanExecInfo* execInfo = taosMemoryCalloc(1, sizeof(STableMergeScanExecInfo));
  STableMergeScanInfo*     pInfo = pOptr->info;
  execInfo->blockRecorder = pInfo->readRecorder;
  execInfo->sortExecInfo = pInfo->sortExecInfo;

  *pOptrExplain = execInfo;
  *len = sizeof(STableMergeScanExecInfo);

  return TSDB_CODE_SUCCESS;
}

int32_t compareTableKeyInfoByGid(const void* p1, const void* p2) {
  const STableKeyInfo* info1 = p1;
  const STableKeyInfo* info2 = p2;
  if (info1->groupId < info2->groupId) {
    return -1;
  } else if (info1->groupId > info2->groupId) {
    return 1;
  } else {
    return 0;
  }
}

SOperatorInfo* createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, STableListInfo* pTableListInfo,
                                                SReadHandle* readHandle, SExecTaskInfo* pTaskInfo) {
  STableMergeScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableMergeScanInfo));
  SOperatorInfo*       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }
  if (pTableScanNode->pGroupTags) {
    taosArraySort(pTableListInfo->pTableList, compareTableKeyInfoByGid);
  }

  SDataBlockDescNode* pDescNode = pTableScanNode->scan.node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  SArray* pColList = extractColMatchInfo(pTableScanNode->scan.pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID);

  int32_t code = initQueryTableDataCond(&pInfo->cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->pseudoSup;
    pSup->pExprInfo = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};

  pInfo->readHandle = *readHandle;
  pInfo->interval = extractIntervalInfo(pTableScanNode);
  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();
  pInfo->dataBlockLoadFlag = pTableScanNode->dataRequired;
  pInfo->pFilterNode = pTableScanNode->scan.node.pConditions;
  pInfo->tableListInfo = pTableListInfo;
  pInfo->scanFlag = MAIN_SCAN;
  pInfo->pColMatchInfo = pColList;

  pInfo->pResBlock = createResDataBlock(pDescNode);
  pInfo->sortSourceParams = taosArrayInit(64, sizeof(STableMergeScanSortSourceParam));

  pInfo->pSortInfo = generateSortByTsInfo(pInfo->pColMatchInfo, pInfo->cond.order);
  pInfo->pSortInputBlock = createOneDataBlock(pInfo->pResBlock, false);

  int32_t rowSize = pInfo->pResBlock->info.rowSize;
  pInfo->bufPageSize = getProperSortPageSize(rowSize);

  pOperator->name = "TableMergeScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;
  initResultSizeInfo(&pOperator->resultInfo, 1024);

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doTableMergeScan, NULL, NULL, destroyTableMergeScanOperatorInfo, NULL,
                          NULL, getTableMergeScanExplainExecInfo);
  pOperator->cost.openCost = 0;
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}
