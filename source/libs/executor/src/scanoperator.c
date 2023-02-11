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

#define SET_REVERSE_SCAN_FLAG(_info) ((_info)->scanFlag = REVERSE_SCAN)
#define SWITCH_ORDER(n)              (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))

typedef struct STableMergeScanExecInfo {
  SFileBlockLoadRecorder blockRecorder;
  SSortExecInfo          sortExecInfo;
} STableMergeScanExecInfo;

typedef struct STableMergeScanSortSourceParam {
  SOperatorInfo* pOperator;
  int32_t        readerIdx;
  uint64_t       uid;
  SSDataBlock*   inputBlock;
} STableMergeScanSortSourceParam;

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
    ASSERT(w.ekey >= pBlockInfo->window.skey);

    if (w.ekey < pBlockInfo->window.ekey) {
      return true;
    }

    while (1) {
      getNextTimeWindow(pInterval, &w, order);
      if (w.skey > pBlockInfo->window.ekey) {
        break;
      }

      ASSERT(w.ekey > pBlockInfo->window.ekey);
      if (TMAX(w.skey, pBlockInfo->window.skey) <= pBlockInfo->window.ekey) {
        return true;
      }
    }
  } else {
    w = getAlignQueryTimeWindow(pInterval, pInterval->precision, pBlockInfo->window.ekey);
    ASSERT(w.skey <= pBlockInfo->window.ekey);

    if (w.skey > pBlockInfo->window.skey) {
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

  SResultRowPosition* p1 = (SResultRowPosition*)tSimpleHashGet(pTableScanInfo->base.pdInfo.pAggSup->pResultRowHashTable,
                                                               buf, GET_RES_WINDOW_KEY_LEN(sizeof(groupId)));

  if (p1 == NULL) {
    return NULL;
  }

  *pPage = getBufPage(pTableScanInfo->base.pdInfo.pAggSup->pResultBuf, p1->pageId);
  if (NULL == *pPage) {
    return NULL;
  }

  return (SResultRow*)((char*)(*pPage) + p1->offset);
}

static int32_t doDynamicPruneDataBlock(SOperatorInfo* pOperator, SDataBlockInfo* pBlockInfo, uint32_t* status) {
  STableScanInfo* pTableScanInfo = pOperator->info;

  if (pTableScanInfo->base.pdInfo.pExprSup == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SExprSupp* pSup1 = pTableScanInfo->base.pdInfo.pExprSup;

  SFilePage*  pPage = NULL;
  SResultRow* pRow = getTableGroupOutputBuf(pOperator, pBlockInfo->id.groupId, &pPage);

  if (pRow == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  bool notLoadBlock = true;
  for (int32_t i = 0; i < pSup1->numOfExprs; ++i) {
    int32_t functionId = pSup1->pCtx[i].functionId;

    SResultRowEntryInfo* pEntry = getResultEntryInfo(pRow, i, pTableScanInfo->base.pdInfo.pExprSup->rowEntryInfoOffset);

    int32_t reqStatus = fmFuncDynDataRequired(functionId, pEntry, &pBlockInfo->window);
    if (reqStatus != FUNC_DATA_REQUIRED_NOT_LOAD) {
      notLoadBlock = false;
      break;
    }
  }

  // release buffer pages
  releaseBufPage(pTableScanInfo->base.pdInfo.pAggSup->pResultBuf, pPage);

  if (notLoadBlock) {
    *status = FUNC_DATA_REQUIRED_NOT_LOAD;
  }

  return TSDB_CODE_SUCCESS;
}

static bool doFilterByBlockSMA(SFilterInfo* pFilterInfo, SColumnDataAgg** pColsAgg, int32_t numOfCols,
                               int32_t numOfRows) {
  if (pColsAgg == NULL || pFilterInfo == NULL) {
    return true;
  }

  bool keep = filterRangeExecute(pFilterInfo, pColsAgg, numOfCols, numOfRows);
  return keep;
}

static bool doLoadBlockSMA(STableScanBase* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  bool    allColumnsHaveAgg = true;
  int32_t code = tsdbRetrieveDatablockSMA(pTableScanInfo->dataReader, pBlock, &allColumnsHaveAgg);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (!allColumnsHaveAgg) {
    return false;
  }
  return true;
}

static void doSetTagColumnData(STableScanBase* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo,
                               int32_t rows) {
  if (pTableScanInfo->pseudoSup.numOfExprs > 0) {
    SExprSupp* pSup = &pTableScanInfo->pseudoSup;

    int32_t code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock, rows,
                                          GET_TASKID(pTaskInfo), &pTableScanInfo->metaCache);
    // ignore the table not exists error, since this table may have been dropped during the scan procedure.
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // reset the error code.
    terrno = 0;
  }
}

bool applyLimitOffset(SLimitInfo* pLimitInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo) {
  SLimit*     pLimit = &pLimitInfo->limit;
  const char* id = GET_TASKID(pTaskInfo);

  if (pLimitInfo->remainOffset > 0) {
    if (pLimitInfo->remainOffset >= pBlock->info.rows) {
      pLimitInfo->remainOffset -= pBlock->info.rows;
      blockDataEmpty(pBlock);
      qDebug("current block ignore due to offset, current:%" PRId64 ", %s", pLimitInfo->remainOffset, id);
      return false;
    } else {
      blockDataTrimFirstNRows(pBlock, pLimitInfo->remainOffset);
      pLimitInfo->remainOffset = 0;
    }
  }

  if (pLimit->limit != -1 && pLimit->limit <= (pLimitInfo->numOfOutputRows + pBlock->info.rows)) {
    // limit the output rows
    int32_t keep = (int32_t)(pLimit->limit - pLimitInfo->numOfOutputRows);
    blockDataKeepFirstNRows(pBlock, keep);

    pLimitInfo->numOfOutputRows += pBlock->info.rows;
    qDebug("output limit %" PRId64 " has reached, %s", pLimit->limit, id);
    return true;
  }

  pLimitInfo->numOfOutputRows += pBlock->info.rows;
  return false;
}

static int32_t loadDataBlock(SOperatorInfo* pOperator, STableScanBase* pTableScanInfo, SSDataBlock* pBlock,
                             uint32_t* status) {
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  SFileBlockLoadRecorder* pCost = &pTableScanInfo->readRecorder;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;

  bool loadSMA = false;
  *status = pTableScanInfo->dataBlockLoadFlag;
  if (pOperator->exprSupp.pFilterInfo != NULL ||
      overlapWithTimeWindow(&pTableScanInfo->pdInfo.interval, &pBlock->info, pTableScanInfo->cond.order)) {
    (*status) = FUNC_DATA_REQUIRED_DATA_LOAD;
  }

  SDataBlockInfo* pBlockInfo = &pBlock->info;
  taosMemoryFreeClear(pBlock->pBlockAgg);

  if (*status == FUNC_DATA_REQUIRED_FILTEROUT) {
    qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->filterOutBlocks += 1;
    pCost->totalRows += pBlock->info.rows;
    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_NOT_LOAD) {
    qDebug("%s data block skipped, brange:%" PRId64 "-%" PRId64 ", rows:%d, uid:%"PRIu64, GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, pBlockInfo->id.uid);
    doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, 1);
    pCost->skipBlocks += 1;
    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_SMA_LOAD) {
    pCost->loadBlockStatis += 1;
    loadSMA = true;  // mark the operation of load sma;
    bool success = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo);
    if (success) {  // failed to load the block sma data, data block statistics does not exist, load data block instead
      qDebug("%s data block SMA loaded, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
             pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, 1);
      return TSDB_CODE_SUCCESS;
    } else {
      qDebug("%s failed to load SMA, since not all columns have SMA", GET_TASKID(pTaskInfo));
      *status = FUNC_DATA_REQUIRED_DATA_LOAD;
    }
  }

  ASSERT(*status == FUNC_DATA_REQUIRED_DATA_LOAD);

  // try to filter data block according to sma info
  if (pOperator->exprSupp.pFilterInfo != NULL && (!loadSMA)) {
    bool success = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo);
    if (success) {
      size_t size = taosArrayGetSize(pBlock->pDataBlock);
      bool   keep = doFilterByBlockSMA(pOperator->exprSupp.pFilterInfo, pBlock->pBlockAgg, size, pBlockInfo->rows);
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

  SSDataBlock* p = tsdbRetrieveDataBlock(pTableScanInfo->dataReader, NULL);
  if (p == NULL) {
    return terrno;
  }

  ASSERT(p == pBlock);
  doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);

  // restore the previous value
  pCost->totalRows -= pBlock->info.rows;

  if (pOperator->exprSupp.pFilterInfo != NULL) {
    int64_t st = taosGetTimestampUs();
    doFilter(pBlock, pOperator->exprSupp.pFilterInfo, &pTableScanInfo->matchInfo);

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

  bool limitReached = applyLimitOffset(&pTableScanInfo->limitInfo, pBlock, pTaskInfo);
  if (limitReached) { // set operator flag is done
    setOperatorCompleted(pOperator);
  }

  pCost->totalRows += pBlock->info.rows;
  return TSDB_CODE_SUCCESS;
}

static void prepareForDescendingScan(STableScanBase* pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);

  switchCtxOrder(pCtx, numOfOutput);
  pTableScanInfo->cond.order = TSDB_ORDER_DESC;
  STimeWindow* pTWindow = &pTableScanInfo->cond.twindows;
  TSWAP(pTWindow->skey, pTWindow->ekey);
}

typedef struct STableCachedVal {
  const char* pName;
  STag*       pTags;
} STableCachedVal;

static void freeTableCachedVal(void* param) {
  if (param == NULL) {
    return;
  }

  STableCachedVal* pVal = param;
  taosMemoryFree((void*)pVal->pName);
  taosMemoryFree(pVal->pTags);
  taosMemoryFree(pVal);
}

static STableCachedVal* createTableCacheVal(const SMetaReader* pMetaReader) {
  STableCachedVal* pVal = taosMemoryMalloc(sizeof(STableCachedVal));
  pVal->pName = strdup(pMetaReader->me.name);
  pVal->pTags = NULL;

  // only child table has tag value
  if (pMetaReader->me.type == TSDB_CHILD_TABLE) {
    STag* pTag = (STag*)pMetaReader->me.ctbEntry.pTags;
    pVal->pTags = taosMemoryMalloc(pTag->len);
    memcpy(pVal->pTags, pTag, pTag->len);
  }

  return pVal;
}

// const void *key, size_t keyLen, void *value
static void freeCachedMetaItem(const void* key, size_t keyLen, void* value) { freeTableCachedVal(value); }


static void doSetNullValue(SSDataBlock* pBlock, const SExprInfo* pExpr, int32_t numOfExpr) {
  for (int32_t j = 0; j < numOfExpr; ++j) {
    int32_t dstSlotId = pExpr[j].base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colDataAppendNNULL(pColInfoData, 0, pBlock->info.rows);
  }
}

int32_t addTagPseudoColumnData(SReadHandle* pHandle, const SExprInfo* pExpr, int32_t numOfExpr, SSDataBlock* pBlock,
                               int32_t rows, const char* idStr, STableMetaCacheInfo* pCache) {
  // currently only the tbname pseudo column
  if (numOfExpr <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;

  // backup the rows
  int32_t backupRows = pBlock->info.rows;
  pBlock->info.rows = rows;

  bool            freeReader = false;
  STableCachedVal val = {0};

  SMetaReader mr = {0};
  LRUHandle*  h = NULL;

  // todo refactor: extract method
  // the handling of the null data should be packed in the extracted method

  // 1. check if it is existed in meta cache
  if (pCache == NULL) {
    metaReaderInit(&mr, pHandle->meta, 0);
    code = metaGetTableEntryByUidCache(&mr, pBlock->info.id.uid);
    if (code != TSDB_CODE_SUCCESS) {

      // when encounter the TSDB_CODE_PAR_TABLE_NOT_EXIST error, we proceed.
      if (terrno == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
        qWarn("failed to get table meta, table may have been dropped, uid:0x%" PRIx64 ", code:%s, %s",
              pBlock->info.id.uid, tstrerror(terrno), idStr);

        // append null value before return to caller, since the caller will ignore this error code and proceed
        doSetNullValue(pBlock, pExpr, numOfExpr);
      } else {
        qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.id.uid, tstrerror(terrno),
               idStr);
      }
      metaReaderClear(&mr);
      return terrno;
    }

    metaReaderReleaseLock(&mr);

    val.pName = mr.me.name;
    val.pTags = (STag*)mr.me.ctbEntry.pTags;

    freeReader = true;
  } else {
    pCache->metaFetch += 1;

    h = taosLRUCacheLookup(pCache->pTableMetaEntryCache, &pBlock->info.id.uid, sizeof(pBlock->info.id.uid));
    if (h == NULL) {
      metaReaderInit(&mr, pHandle->meta, 0);
      code = metaGetTableEntryByUidCache(&mr, pBlock->info.id.uid);
      if (code != TSDB_CODE_SUCCESS) {
        if (terrno == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
          qWarn("failed to get table meta, table may have been dropped, uid:0x%" PRIx64 ", code:%s, %s",
                pBlock->info.id.uid, tstrerror(terrno), idStr);
          // append null value before return to caller, since the caller will ignore this error code and proceed
          doSetNullValue(pBlock, pExpr, numOfExpr);
        } else {
          qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.id.uid, tstrerror(terrno),
                 idStr);
        }
        metaReaderClear(&mr);
        return terrno;
      }

      metaReaderReleaseLock(&mr);

      STableCachedVal* pVal = createTableCacheVal(&mr);

      val = *pVal;
      freeReader = true;

      int32_t ret = taosLRUCacheInsert(pCache->pTableMetaEntryCache, &pBlock->info.id.uid, sizeof(uint64_t), pVal,
                                       sizeof(STableCachedVal), freeCachedMetaItem, NULL, TAOS_LRU_PRIORITY_LOW);
      if (ret != TAOS_LRU_STATUS_OK) {
        qError("failed to put meta into lru cache, code:%d, %s", ret, idStr);
        freeTableCachedVal(pVal);
      }
    } else {
      pCache->cacheHit += 1;
      STableCachedVal* pVal = taosLRUCacheValue(pCache->pTableMetaEntryCache, h);
      val = *pVal;

      taosLRUCacheRelease(pCache->pTableMetaEntryCache, h, false);
    }

    qDebug("retrieve table meta from cache:%" PRIu64 ", hit:%" PRIu64 " miss:%" PRIu64 ", %s", pCache->metaFetch,
           pCache->cacheHit, (pCache->metaFetch - pCache->cacheHit), idStr);
  }

  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExpr[j];
    int32_t          dstSlotId = pExpr1->base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colInfoDataCleanup(pColInfoData, pBlock->info.rows);

    int32_t functionId = pExpr1->pExpr->_function.functionId;

    // this is to handle the tbname
    if (fmIsScanPseudoColumnFunc(functionId)) {
      setTbNameColData(pBlock, pColInfoData, functionId, val.pName);
    } else {  // these are tags
      STagVal tagVal = {0};
      tagVal.cid = pExpr1->base.pParam[0].pCol->colId;
      const char* p = metaGetTableTagVal(val.pTags, pColInfoData->info.type, &tagVal);

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
        if (IS_VAR_DATA_TYPE(((const STagVal*)p)->type)) {
          taosMemoryFree(data);
        }
      } else {  // todo opt for json tag
        for (int32_t i = 0; i < pBlock->info.rows; ++i) {
          colDataAppend(pColInfoData, i, data, false);
        }
      }
    }
  }

  // restore the rows
  pBlock->info.rows = backupRows;
  if (freeReader) {
    metaReaderClear(&mr);
  }

  return TSDB_CODE_SUCCESS;
}

void setTbNameColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, const char* name) {
  struct SScalarFuncExecFuncs fpSet = {0};
  fmGetScalarFuncExecFuncs(functionId, &fpSet);

  size_t len = TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE;
  char   buf[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(buf, name)

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, len, 1);

  colInfoDataEnsureCapacity(&infoData, 1, false);
  colDataAppend(&infoData, 0, buf, false);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    fpSet.process(&srcParam, 1, &param);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

  colDataDestroy(&infoData);
}

static SSDataBlock* doTableScanImpl(SOperatorInfo* pOperator) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*    pBlock = pTableScanInfo->pResBlock;

  int64_t st = taosGetTimestampUs();

  while (tsdbNextDataBlock(pTableScanInfo->base.dataReader)) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (pOperator->status == OP_EXEC_DONE) {
      break;
    }

    // process this data block based on the probabilities
    bool processThisBlock = processBlockWithProbability(&pTableScanInfo->sample);
    if (!processThisBlock) {
      continue;
    }

    ASSERT(pBlock->info.id.uid != 0);
    pBlock->info.id.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.id.uid);

    uint32_t status = 0;
    int32_t  code = loadDataBlock(pOperator, &pTableScanInfo->base, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
      continue;
    }

    pOperator->resultInfo.totalRows = pTableScanInfo->base.readRecorder.totalRows;
    pTableScanInfo->base.readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;

    pOperator->cost.totalCost = pTableScanInfo->base.readRecorder.elapsedTime;

    // todo refactor
    /*pTableScanInfo->lastStatus.uid = pBlock->info.id.uid;*/
    /*pTableScanInfo->lastStatus.ts = pBlock->info.window.ekey;*/
    pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__SNAPSHOT_DATA;
    pTaskInfo->streamInfo.lastStatus.uid = pBlock->info.id.uid;
    pTaskInfo->streamInfo.lastStatus.ts = pBlock->info.window.ekey;

    ASSERT(pBlock->info.id.uid != 0);
    return pBlock;
  }
  return NULL;
}

static SSDataBlock* doGroupedTableScan(SOperatorInfo* pOperator) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  // The read handle is not initialized yet, since no qualified tables exists
  if (pTableScanInfo->base.dataReader == NULL || pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // do the ascending order traverse in the first place.
  while (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
    SSDataBlock* p = doTableScanImpl(pOperator);
    if (p != NULL) {
      return p;
    }

    pTableScanInfo->scanTimes += 1;

    if (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
      setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
      pTableScanInfo->base.scanFlag = REPEAT_SCAN;
      qDebug("start to repeat ascending order scan data blocks due to query func required, %s", GET_TASKID(pTaskInfo));

      // do prepare for the next round table scan operation
      tsdbReaderReset(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
    }
  }

  int32_t total = pTableScanInfo->scanInfo.numOfAsc + pTableScanInfo->scanInfo.numOfDesc;
  if (pTableScanInfo->scanTimes < total) {
    if (pTableScanInfo->base.cond.order == TSDB_ORDER_ASC) {
      prepareForDescendingScan(&pTableScanInfo->base, pOperator->exprSupp.pCtx, 0);
      tsdbReaderReset(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
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
        pTableScanInfo->base.scanFlag = REPEAT_SCAN;

        qDebug("%s start to repeat descending order scan data blocks", GET_TASKID(pTaskInfo));
        tsdbReaderReset(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
      }
    }
  }

  return NULL;
}

static SSDataBlock* doTableScan(SOperatorInfo* pOperator) {
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  // scan table one by one sequentially
  if (pInfo->scanMode == TABLE_SCAN__TABLE_ORDER) {
    int32_t numOfTables = tableListGetSize(pTaskInfo->pTableInfoList);

    while (1) {
      SSDataBlock* result = doGroupedTableScan(pOperator);
      if (result) {
        return result;
      }

      // if no data, switch to next table and continue scan
      pInfo->currentTable++;
      if (pInfo->currentTable >= numOfTables) {
        return NULL;
      }

      STableKeyInfo* pTableInfo = tableListGetInfo(pTaskInfo->pTableInfoList, pInfo->currentTable);
      tsdbSetTableList(pInfo->base.dataReader, pTableInfo, 1);
      qDebug("set uid:%" PRIu64 " into scanner, total tables:%d, index:%d %s", pTableInfo->uid, numOfTables,
             pInfo->currentTable, pTaskInfo->id.str);

      tsdbReaderReset(pInfo->base.dataReader, &pInfo->base.cond);
      pInfo->scanTimes = 0;
    }
  } else {  // scan table group by group sequentially
    if (pInfo->currentGroupId == -1) {
      if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pTaskInfo->pTableInfoList)) {
        setOperatorCompleted(pOperator);
        return NULL;
      }

      int32_t        num = 0;
      STableKeyInfo* pList = NULL;
      tableListGetGroupList(pTaskInfo->pTableInfoList, pInfo->currentGroupId, &pList, &num);
      ASSERT(pInfo->base.dataReader == NULL);

      int32_t code = tsdbReaderOpen(pInfo->base.readHandle.vnode, &pInfo->base.cond, pList, num, pInfo->pResBlock,
                                    (STsdbReader**)&pInfo->base.dataReader, GET_TASKID(pTaskInfo));
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }

      if (pInfo->pResBlock->info.capacity > pOperator->resultInfo.capacity) {
        pOperator->resultInfo.capacity = pInfo->pResBlock->info.capacity;
      }
    }

    SSDataBlock* result = doGroupedTableScan(pOperator);
    if (result != NULL) {
      ASSERT(result->info.id.uid != 0);
      return result;
    }

    if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pTaskInfo->pTableInfoList)) {
      setOperatorCompleted(pOperator);
      return NULL;
    }

    // reset value for the next group data output
    pOperator->status = OP_OPENED;
    resetLimitInfoForNextGroup(&pInfo->base.limitInfo);

    int32_t        num = 0;
    STableKeyInfo* pList = NULL;
    tableListGetGroupList(pTaskInfo->pTableInfoList, pInfo->currentGroupId, &pList, &num);

    tsdbSetTableList(pInfo->base.dataReader, pList, num);
    tsdbReaderReset(pInfo->base.dataReader, &pInfo->base.cond);
    pInfo->scanTimes = 0;

    result = doGroupedTableScan(pOperator);
    if (result != NULL) {
      return result;
    }

    setOperatorCompleted(pOperator);
    return NULL;
  }
}

static int32_t getTableScannerExecInfo(struct SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SFileBlockLoadRecorder* pRecorder = taosMemoryCalloc(1, sizeof(SFileBlockLoadRecorder));
  STableScanInfo*         pTableScanInfo = pOptr->info;
  *pRecorder = pTableScanInfo->base.readRecorder;
  *pOptrExplain = pRecorder;
  *len = sizeof(SFileBlockLoadRecorder);
  return 0;
}

static void destroyTableScanOperatorInfo(void* param) {
  STableScanInfo* pTableScanInfo = (STableScanInfo*)param;
  blockDataDestroy(pTableScanInfo->pResBlock);
  cleanupQueryTableDataCond(&pTableScanInfo->base.cond);

  tsdbReaderClose(pTableScanInfo->base.dataReader);
  pTableScanInfo->base.dataReader = NULL;

  if (pTableScanInfo->base.matchInfo.pList != NULL) {
    taosArrayDestroy(pTableScanInfo->base.matchInfo.pList);
  }

  taosLRUCacheCleanup(pTableScanInfo->base.metaCache.pTableMetaEntryCache);
  cleanupExprSupp(&pTableScanInfo->base.pseudoSup);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle,
                                           SExecTaskInfo* pTaskInfo) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SScanPhysiNode*     pScanNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  int32_t code =
      extractColMatchInfo(pScanNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->base.matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initLimitInfo(pScanNode->node.pLimit, pScanNode->node.pSlimit, &pInfo->base.limitInfo);
  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pScanNode->pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->base.pseudoSup;
    pSup->pExprInfo = createExprInfo(pScanNode->pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};

  pInfo->base.scanFlag = MAIN_SCAN;
  pInfo->base.pdInfo.interval = extractIntervalInfo(pTableScanNode);
  pInfo->base.readHandle = *readHandle;
  pInfo->base.dataBlockLoadFlag = pTableScanNode->dataRequired;

  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  pInfo->pResBlock = createDataBlockFromDescNode(pDescNode);
//  blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->currentGroupId = -1;
  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;
  pInfo->hasGroupByTag = pTableScanNode->pGroupTags ? true : false;

  setOperatorInfo(pOperator, "TableScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;

  pInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
  if (pInfo->base.metaCache.pTableMetaEntryCache == NULL) {
    code = terrno;
    goto _error;
  }

  taosLRUCacheSetStrictCapacity(pInfo->base.metaCache.pTableMetaEntryCache, false);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableScan, NULL, destroyTableScanOperatorInfo,
                                         optrDefaultBufFn, getTableScannerExecInfo);

  // for non-blocking operator, the open cost is always 0
  pOperator->cost.openCost = 0;
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroyTableScanOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

SOperatorInfo* createTableSeqScanOperatorInfo(void* pReadHandle, SExecTaskInfo* pTaskInfo) {
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  pInfo->base.dataReader = pReadHandle;
  //  pInfo->prevGroupId       = -1;

  setOperatorInfo(pOperator, "TableSeqScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableScanImpl, NULL, NULL, optrDefaultBufFn, NULL);
  return pOperator;
}

static FORCE_INLINE void doClearBufferedBlocks(SStreamScanInfo* pInfo) {
  taosArrayClear(pInfo->pBlockLists);
  pInfo->validBlockIndex = 0;
}

static bool isSessionWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
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
  pTableScanInfo->base.cond.twindows = *pWin;
  pTableScanInfo->scanTimes = 0;
  pTableScanInfo->currentGroupId = -1;
  tsdbReaderClose(pTableScanInfo->base.dataReader);
  pTableScanInfo->base.dataReader = NULL;
}

static SSDataBlock* readPreVersionData(SOperatorInfo* pTableScanOp, uint64_t tbUid, TSKEY startTs, TSKEY endTs,
                                       int64_t maxVersion) {
  STableKeyInfo tblInfo = {.uid = tbUid, .groupId = 0};

  STableScanInfo*     pTableScanInfo = pTableScanOp->info;
  SQueryTableDataCond cond = pTableScanInfo->base.cond;

  cond.startVersion = -1;
  cond.endVersion = maxVersion;
  cond.twindows = (STimeWindow){.skey = startTs, .ekey = endTs};

  SExecTaskInfo* pTaskInfo = pTableScanOp->pTaskInfo;

  SSDataBlock* pBlock = pTableScanInfo->pResBlock;
  STsdbReader* pReader = NULL;
  int32_t      code = tsdbReaderOpen(pTableScanInfo->base.readHandle.vnode, &cond, &tblInfo, 1, pBlock,
                                     (STsdbReader**)&pReader, GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    T_LONG_JMP(pTaskInfo->env, code);
    return NULL;
  }

  if (tsdbNextDataBlock(pReader)) {
    /*SSDataBlock* p = */ tsdbRetrieveDataBlock(pReader, NULL);
    doSetTagColumnData(&pTableScanInfo->base, pBlock, pTaskInfo, pBlock->info.rows);
    pBlock->info.id.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.id.uid);
  }

  tsdbReaderClose(pReader);
  qDebug("retrieve prev rows:%d, skey:%" PRId64 ", ekey:%" PRId64 " uid:%" PRIu64 ", max ver:%" PRId64
         ", suid:%" PRIu64,
         pBlock->info.rows, startTs, endTs, tbUid, maxVersion, cond.suid);

  return pBlock->info.rows > 0 ? pBlock : NULL;
}

static uint64_t getGroupIdByCol(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion) {
  SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, uid, ts, ts, maxVersion);
  if (!pPreRes || pPreRes->info.rows == 0) {
    return 0;
  }
  ASSERT(pPreRes->info.rows == 1);
  return calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pPreRes, 0);
}

static uint64_t getGroupIdByUid(SStreamScanInfo* pInfo, uint64_t uid) {
  return getTableGroupId(pInfo->pTableScanOp->pTaskInfo->pTableInfoList, uid);
}

static uint64_t getGroupIdByData(SStreamScanInfo* pInfo, uint64_t uid, TSKEY ts, int64_t maxVersion) {
  if (pInfo->partitionSup.needCalc) {
    return getGroupIdByCol(pInfo, uid, ts, maxVersion);
  }

  return getGroupIdByUid(pInfo, uid);
}

static bool prepareRangeScan(SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t* pRowIndex) {
  if (pBlock->info.rows == 0) {
    return false;
  }
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

static STimeWindow getSlidingWindow(TSKEY* startTsCol, TSKEY* endTsCol, uint64_t* gpIdCol, SInterval* pInterval,
                                    SDataBlockInfo* pDataBlockInfo, int32_t* pRowIndex, bool hasGroup) {
  SResultRowInfo dumyInfo = {0};
  dumyInfo.cur.pageId = -1;
  STimeWindow win = getActiveTimeWindow(NULL, &dumyInfo, startTsCol[*pRowIndex], pInterval, TSDB_ORDER_ASC);
  STimeWindow endWin = win;
  STimeWindow preWin = win;
  uint64_t    groupId = gpIdCol[*pRowIndex];

  while (1) {
    if (hasGroup) {
      (*pRowIndex) += 1;
    } else {
      while ((groupId == gpIdCol[(*pRowIndex)] && startTsCol[*pRowIndex] <= endWin.ekey)) {
        (*pRowIndex) += 1;
        if ((*pRowIndex) == pDataBlockInfo->rows) {
          break;
        }
      }
    }

    do {
      preWin = endWin;
      getNextTimeWindow(pInterval, &endWin, TSDB_ORDER_ASC);
    } while (endTsCol[(*pRowIndex) - 1] >= endWin.skey);
    endWin = preWin;
    if (win.ekey == endWin.ekey || (*pRowIndex) == pDataBlockInfo->rows || groupId != gpIdCol[*pRowIndex]) {
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
      tsdbReaderClose(pTableScanInfo->base.dataReader);
      pTableScanInfo->base.dataReader = NULL;
      return NULL;
    }

    doFilter(pResult, pInfo->pTableScanOp->exprSupp.pFilterInfo, NULL);
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

      blockDataDestroy(tmpBlock);

      if (pResult->info.rows > 0) {
        pResult->info.calWin = pInfo->updateWin;
        return pResult;
      }
    } else if (pResult->info.id.groupId == pInfo->groupId) {
      pResult->info.calWin = pInfo->updateWin;
      return pResult;
    }
  }
}

static int32_t generateSessionScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  blockDataCleanup(pDestBlock);
  if (pSrcBlock->info.rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
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
  int64_t          version = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t groupId = getGroupIdByData(pInfo, uidCol[i], startData[i], version);
    // gap must be 0.
    SSessionKey startWin = {0};
    getCurSessionWindow(pInfo->windowSup.pStreamAggSup, startData[i], startData[i], groupId, &startWin);
    if (IS_INVALID_SESSION_WIN_KEY(startWin)) {
      // window has been closed.
      continue;
    }
    SSessionKey endWin = {0};
    getCurSessionWindow(pInfo->windowSup.pStreamAggSup, endData[i], endData[i], groupId, &endWin);
    ASSERT(!IS_INVALID_SESSION_WIN_KEY(endWin));
    colDataAppend(pDestStartCol, i, (const char*)&startWin.win.skey, false);
    colDataAppend(pDestEndCol, i, (const char*)&endWin.win.ekey, false);

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

  SColumnInfoData* pSrcStartTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcEndTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pSrcGpCol = taosArrayGet(pSrcBlock->pDataBlock, GROUPID_COLUMN_INDEX);

  uint64_t* srcUidData = (uint64_t*)pSrcUidCol->pData;
  ASSERT(pSrcStartTsCol->info.type == TSDB_DATA_TYPE_TIMESTAMP);
  TSKEY*  srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*  srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  int64_t version = pSrcBlock->info.version - 1;

  if (pInfo->partitionSup.needCalc && srcStartTsCol[0] != srcEndTsCol[0]) {
    uint64_t     srcUid = srcUidData[0];
    TSKEY        startTs = srcStartTsCol[0];
    TSKEY        endTs = srcEndTsCol[0];
    SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, srcUid, startTs, endTs, version);
    printDataBlock(pPreRes, "pre res");
    blockDataCleanup(pSrcBlock);
    int32_t code = blockDataEnsureCapacity(pSrcBlock, pPreRes->info.rows);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    SColumnInfoData* pTsCol = (SColumnInfoData*)taosArrayGet(pPreRes->pDataBlock, pInfo->primaryTsIndex);
    rows = pPreRes->info.rows;

    for (int32_t i = 0; i < rows; i++) {
      uint64_t groupId = calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pPreRes, i);
      appendOneRowToStreamSpecialBlock(pSrcBlock, ((TSKEY*)pTsCol->pData) + i, ((TSKEY*)pTsCol->pData) + i, &srcUid,
                                       &groupId, NULL);
    }
    printDataBlock(pSrcBlock, "new delete");
  }
  uint64_t* srcGp = (uint64_t*)pSrcGpCol->pData;
  srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  srcUidData = (uint64_t*)pSrcUidCol->pData;

  int32_t code = blockDataEnsureCapacity(pDestBlock, rows);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SColumnInfoData* pStartTsCol = taosArrayGet(pDestBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pDestBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pDeUidCol = taosArrayGet(pDestBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGpCol = taosArrayGet(pDestBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndTsCol = taosArrayGet(pDestBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  for (int32_t i = 0; i < rows;) {
    uint64_t srcUid = srcUidData[i];
    uint64_t groupId = srcGp[i];
    if (groupId == 0) {
      groupId = getGroupIdByData(pInfo, srcUid, srcStartTsCol[i], version);
    }
    TSKEY calStartTs = srcStartTsCol[i];
    colDataAppend(pCalStartTsCol, pDestBlock->info.rows, (const char*)(&calStartTs), false);
    STimeWindow win = getSlidingWindow(srcStartTsCol, srcEndTsCol, srcGp, &pInfo->interval, &pSrcBlock->info, &i,
                                       pInfo->partitionSup.needCalc);
    TSKEY       calEndTs = srcStartTsCol[i - 1];
    colDataAppend(pCalEndTsCol, pDestBlock->info.rows, (const char*)(&calEndTs), false);
    colDataAppend(pDeUidCol, pDestBlock->info.rows, (const char*)(&srcUid), false);
    colDataAppend(pStartTsCol, pDestBlock->info.rows, (const char*)(&win.skey), false);
    colDataAppend(pEndTsCol, pDestBlock->info.rows, (const char*)(&win.ekey), false);
    colDataAppend(pGpCol, pDestBlock->info.rows, (const char*)(&groupId), false);
    pDestBlock->info.rows++;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateDeleteResultBlock(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  blockDataCleanup(pDestBlock);
  int32_t rows = pSrcBlock->info.rows;
  if (rows == 0) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = blockDataEnsureCapacity(pDestBlock, rows);
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
  TSKEY*  srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*  srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  int64_t version = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t srcUid = srcUidData[i];
    uint64_t groupId = srcGp[i];
    char*    tbname[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN] = {0};
    if (groupId == 0) {
      groupId = getGroupIdByData(pInfo, srcUid, srcStartTsCol[i], version);
    }
    if (pInfo->tbnameCalSup.pExprInfo) {
      void* parTbname = NULL;
      streamStateGetParName(pInfo->pStreamScanOp->pTaskInfo->streamInfo.pState, groupId, &parTbname);

      memcpy(varDataVal(tbname), parTbname, TSDB_TABLE_NAME_LEN);
      varDataSetLen(tbname, strlen(varDataVal(tbname)));
      tdbFree(parTbname);
    }
    appendOneRowToStreamSpecialBlock(pDestBlock, srcStartTsCol + i, srcEndTsCol + i, srcUidData + i, &groupId,
                                     tbname[0] == 0 ? NULL : tbname);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (isIntervalWindow(pInfo)) {
    code = generateIntervalScanRange(pInfo, pSrcBlock, pDestBlock);
  } else if (isSessionWindow(pInfo) || isStateWindow(pInfo)) {
    code = generateSessionScanRange(pInfo, pSrcBlock, pDestBlock);
  } else {
    code = generateDeleteResultBlock(pInfo, pSrcBlock, pDestBlock);
  }
  pDestBlock->info.type = STREAM_CLEAR;
  pDestBlock->info.version = pSrcBlock->info.version;
  pDestBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pDestBlock, 0);
  return code;
}

void calBlockTbName(SStreamScanInfo* pInfo, SSDataBlock* pBlock) {
  SExprSupp*    pTbNameCalSup = &pInfo->tbnameCalSup;
  SStreamState* pState = pInfo->pStreamScanOp->pTaskInfo->streamInfo.pState;
  if (pTbNameCalSup == NULL || pTbNameCalSup->numOfExprs == 0) return;
  if (pBlock == NULL || pBlock->info.rows == 0) return;

  void* tbname = NULL;
  if (streamStateGetParName(pInfo->pStreamScanOp->pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, &tbname) < 0) {
    pBlock->info.parTbName[0] = 0;
  } else {
    memcpy(pBlock->info.parTbName, tbname, TSDB_TABLE_NAME_LEN);
  }
  tdbFree(tbname);

  SSDataBlock* pSrcBlock = blockCopyOneRow(pBlock, 0);
  ASSERT(pSrcBlock->info.rows == 1);

  SSDataBlock* pResBlock = createDataBlock();
  pResBlock->info.rowSize = VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN;
  SColumnInfoData data = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, TSDB_TABLE_NAME_LEN, 0);
  taosArrayPush(pResBlock->pDataBlock, &data);
  blockDataEnsureCapacity(pResBlock, 1);

  projectApplyFunctions(pTbNameCalSup->pExprInfo, pResBlock, pSrcBlock, pTbNameCalSup->pCtx, 1, NULL);
  ASSERT(pResBlock->info.rows == 1);
  ASSERT(taosArrayGetSize(pResBlock->pDataBlock) == 1);
  SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, 0);
  ASSERT(pCol->info.type == TSDB_DATA_TYPE_VARCHAR);

  void* pData = colDataGetData(pCol, 0);
  // TODO check tbname validation
  if (pData != (void*)-1 && pData != NULL) {
    memset(pBlock->info.parTbName, 0, TSDB_TABLE_NAME_LEN);
    int32_t len = TMIN(varDataLen(pData), TSDB_TABLE_NAME_LEN - 1);
    memcpy(pBlock->info.parTbName, varDataVal(pData), len);
    /*pBlock->info.parTbName[len + 1] = 0;*/
  } else {
    pBlock->info.parTbName[0] = 0;
  }

  if (pBlock->info.id.groupId && pBlock->info.parTbName[0]) {
    streamStatePutParName(pState, pBlock->info.id.groupId, pBlock->info.parTbName);
  }

  blockDataDestroy(pSrcBlock);
  blockDataDestroy(pResBlock);
}

void appendOneRowToStreamSpecialBlock(SSDataBlock* pBlock, TSKEY* pStartTs, TSKEY* pEndTs, uint64_t* pUid,
                                      uint64_t* pGp, void* pTbName) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);
  colDataAppend(pStartTsCol, pBlock->info.rows, (const char*)pStartTs, false);
  colDataAppend(pEndTsCol, pBlock->info.rows, (const char*)pEndTs, false);
  colDataAppend(pUidCol, pBlock->info.rows, (const char*)pUid, false);
  colDataAppend(pGpCol, pBlock->info.rows, (const char*)pGp, false);
  colDataAppend(pCalStartCol, pBlock->info.rows, (const char*)pStartTs, false);
  colDataAppend(pCalEndCol, pBlock->info.rows, (const char*)pEndTs, false);
  colDataAppend(pTableCol, pBlock->info.rows, (const char*)pTbName, pTbName == NULL);
  pBlock->info.rows++;
}

static void checkUpdateData(SStreamScanInfo* pInfo, bool invertible, SSDataBlock* pBlock, bool out) {
  if (out) {
    blockDataCleanup(pInfo->pUpdateDataRes);
    blockDataEnsureCapacity(pInfo->pUpdateDataRes, pBlock->info.rows * 2);
  }
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  ASSERT(pColDataInfo->info.type == TSDB_DATA_TYPE_TIMESTAMP);
  TSKEY* tsCol = (TSKEY*)pColDataInfo->pData;
  bool   tableInserted = updateInfoIsTableInserted(pInfo->pUpdateInfo, pBlock->info.id.uid);
  for (int32_t rowId = 0; rowId < pBlock->info.rows; rowId++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    bool        isClosed = false;
    STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
    bool overDue = isOverdue(tsCol[rowId], &pInfo->twAggSup);
    if (pInfo->igExpired && overDue) {
      continue;
    }

    if (tableInserted && overDue) {
      win = getActiveTimeWindow(NULL, &dumyInfo, tsCol[rowId], &pInfo->interval, TSDB_ORDER_ASC);
      isClosed = isCloseWindow(&win, &pInfo->twAggSup);
    }
    // must check update info first.
    bool update = updateInfoIsUpdated(pInfo->pUpdateInfo, pBlock->info.id.uid, tsCol[rowId]);
    bool closedWin = isClosed && isSignleIntervalWindow(pInfo) &&
                     isDeletedStreamWindow(&win, pBlock->info.id.groupId,
                                           pInfo->pTableScanOp->pTaskInfo->streamInfo.pState, &pInfo->twAggSup);
    if ((update || closedWin) && out) {
      qDebug("stream update check not pass, update %d, closedWin %d", update, closedWin);
      uint64_t gpId = 0;
      appendOneRowToStreamSpecialBlock(pInfo->pUpdateDataRes, tsCol + rowId, tsCol + rowId, &pBlock->info.id.uid, &gpId,
                                       NULL);
      if (closedWin && pInfo->partitionSup.needCalc) {
        gpId = calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pBlock, rowId);
        appendOneRowToStreamSpecialBlock(pInfo->pUpdateDataRes, tsCol + rowId, tsCol + rowId, &pBlock->info.id.uid,
                                         &gpId, NULL);
      }
    }
  }
  if (out && pInfo->pUpdateDataRes->info.rows > 0) {
    pInfo->pUpdateDataRes->info.version = pBlock->info.version;
    pInfo->pUpdateDataRes->info.dataLoad = 1;
    blockDataUpdateTsWindow(pInfo->pUpdateDataRes, 0);
    pInfo->pUpdateDataRes->info.type = pInfo->partitionSup.needCalc ? STREAM_DELETE_DATA : STREAM_CLEAR;
  }
}

static int32_t setBlockIntoRes(SStreamScanInfo* pInfo, const SSDataBlock* pBlock, bool filter) {
  SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
  SOperatorInfo*  pOperator = pInfo->pStreamScanOp;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;

  blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);

  pInfo->pRes->info.rows = pBlock->info.rows;
  pInfo->pRes->info.id.uid = pBlock->info.id.uid;
  pInfo->pRes->info.type = STREAM_NORMAL;
  pInfo->pRes->info.version = pBlock->info.version;

  pInfo->pRes->info.id.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.id.uid);

  // todo extract method
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->matchInfo.pList); ++i) {
    SColMatchItem* pColMatchInfo = taosArrayGet(pInfo->matchInfo.pList, i);
    if (!pColMatchInfo->needOutput) {
      continue;
    }

    bool colExists = false;
    for (int32_t j = 0; j < blockDataGetNumOfCols(pBlock); ++j) {
      SColumnInfoData* pResCol = bdGetColumnInfoData(pBlock, j);
      if (pResCol->info.colId == pColMatchInfo->colId) {
        SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->dstSlotId);
        colDataAssign(pDst, pResCol, pBlock->info.rows, &pInfo->pRes->info);
        colExists = true;
        break;
      }
    }

    // the required column does not exists in submit block, let's set it to be all null value
    if (!colExists) {
      SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->dstSlotId);
      colDataAppendNNULL(pDst, 0, pBlockInfo->rows);
    }
  }

  // currently only the tbname pseudo column
  if (pInfo->numOfPseudoExpr > 0) {
    int32_t code = addTagPseudoColumnData(&pInfo->readHandle, pInfo->pPseudoExpr, pInfo->numOfPseudoExpr, pInfo->pRes,
                                          pInfo->pRes->info.rows, GET_TASKID(pTaskInfo), NULL);
    // ignore the table not exists error, since this table may have been dropped during the scan procedure.
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      blockDataFreeRes((SSDataBlock*)pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // reset the error code.
    terrno = 0;
  }

  if (filter) {
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
  }

  pInfo->pRes->info.dataLoad = 1;
  blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
  blockDataFreeRes((SSDataBlock*)pBlock);

  calBlockTbName(pInfo, pInfo->pRes);
  return 0;
}

static SSDataBlock* doQueueScan(SOperatorInfo* pOperator) {
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;

  qDebug("queue scan called");

  if (pTaskInfo->streamInfo.pReq != NULL) {
    if (pInfo->tqReader->pMsg == NULL) {
      pInfo->tqReader->pMsg = pTaskInfo->streamInfo.pReq;
      const SSubmitReq* pSubmit = pInfo->tqReader->pMsg;
      if (tqReaderSetDataMsg(pInfo->tqReader, pSubmit, 0) < 0) {
        qError("submit msg messed up when initing stream submit block %p", pSubmit);
        pInfo->tqReader->pMsg = NULL;
        pTaskInfo->streamInfo.pReq = NULL;
        ASSERT(0);
      }
    }

    blockDataCleanup(pInfo->pRes);
    SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;

    while (tqNextDataBlock(pInfo->tqReader)) {
      SSDataBlock block = {0};

      int32_t code = tqRetrieveDataBlock(&block, pInfo->tqReader);

      if (code != TSDB_CODE_SUCCESS || block.info.rows == 0) {
        continue;
      }

      setBlockIntoRes(pInfo, &block, true);

      if (pBlockInfo->rows > 0) {
        return pInfo->pRes;
      }
    }

    pInfo->tqReader->pMsg = NULL;
    pTaskInfo->streamInfo.pReq = NULL;
    return NULL;
  }

  if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    SSDataBlock* pResult = doTableScan(pInfo->pTableScanOp);
    if (pResult && pResult->info.rows > 0) {
      qDebug("queue scan tsdb return %d rows", pResult->info.rows);
      pTaskInfo->streamInfo.returned = 1;
      return pResult;
    } else {
      if (!pTaskInfo->streamInfo.returned) {
        STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
        tsdbReaderClose(pTSInfo->base.dataReader);
        pTSInfo->base.dataReader = NULL;
        tqOffsetResetToLog(&pTaskInfo->streamInfo.prepareStatus, pTaskInfo->streamInfo.snapshotVer);
        qDebug("queue scan tsdb over, switch to wal ver %" PRId64 "", pTaskInfo->streamInfo.snapshotVer + 1);
        if (tqSeekVer(pInfo->tqReader, pTaskInfo->streamInfo.snapshotVer + 1) < 0) {
          tqOffsetResetToLog(&pTaskInfo->streamInfo.lastStatus, pTaskInfo->streamInfo.snapshotVer);
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
        if (setBlockIntoRes(pInfo, &ret.data, true) < 0) {
          ASSERT(0);
        }
        if (pInfo->pRes->info.rows > 0) {
          pOperator->status = OP_EXEC_RECV;
          qDebug("queue scan log return %d rows", pInfo->pRes->info.rows);
          return pInfo->pRes;
        }
      } else if (ret.fetchType == FETCH_TYPE__META) {
        ASSERT(0);
        //        pTaskInfo->streamInfo.lastStatus = ret.offset;
        //        pTaskInfo->streamInfo.metaBlk = ret.meta;
        //        return NULL;
      } else if (ret.fetchType == FETCH_TYPE__NONE ||
                 (ret.fetchType == FETCH_TYPE__SEP && pOperator->status == OP_EXEC_RECV)) {
        pTaskInfo->streamInfo.lastStatus = ret.offset;
        ASSERT(pTaskInfo->streamInfo.lastStatus.version >= pTaskInfo->streamInfo.prepareStatus.version);
        ASSERT(pTaskInfo->streamInfo.lastStatus.version + 1 == pInfo->tqReader->pWalReader->curVersion);
        char formatBuf[80];
        tFormatOffset(formatBuf, 80, &ret.offset);
        qDebug("queue scan log return null, offset %s", formatBuf);
        pOperator->status = OP_OPENED;
        return NULL;
      }
    }
#if 0
    } else if (pTaskInfo->streamInfo.prepareStatus.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    SSDataBlock* pResult = doTableScan(pInfo->pTableScanOp);
    if (pResult && pResult->info.rows > 0) {
      qDebug("stream scan tsdb return %d rows", pResult->info.rows);
      return pResult;
    }
    qDebug("stream scan tsdb return null");
    return NULL;
#endif
  } else {
    ASSERT(0);
    return NULL;
  }
}

static int32_t filterDelBlockByUid(SSDataBlock* pDst, const SSDataBlock* pSrc, SStreamScanInfo* pInfo) {
  STqReader* pReader = pInfo->tqReader;
  int32_t    rows = pSrc->info.rows;
  blockDataEnsureCapacity(pDst, rows);

  SColumnInfoData* pSrcStartCol = taosArrayGet(pSrc->pDataBlock, START_TS_COLUMN_INDEX);
  uint64_t*        startCol = (uint64_t*)pSrcStartCol->pData;
  SColumnInfoData* pSrcEndCol = taosArrayGet(pSrc->pDataBlock, END_TS_COLUMN_INDEX);
  uint64_t*        endCol = (uint64_t*)pSrcEndCol->pData;
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrc->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*        uidCol = (uint64_t*)pSrcUidCol->pData;

  SColumnInfoData* pDstStartCol = taosArrayGet(pDst->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pDstEndCol = taosArrayGet(pDst->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pDstUidCol = taosArrayGet(pDst->pDataBlock, UID_COLUMN_INDEX);
  int32_t          j = 0;
  for (int32_t i = 0; i < rows; i++) {
    if (taosHashGet(pReader->tbIdHash, &uidCol[i], sizeof(uint64_t))) {
      colDataAppend(pDstStartCol, j, (const char*)&startCol[i], false);
      colDataAppend(pDstEndCol, j, (const char*)&endCol[i], false);
      colDataAppend(pDstUidCol, j, (const char*)&uidCol[i], false);

      colDataAppendNULL(taosArrayGet(pDst->pDataBlock, GROUPID_COLUMN_INDEX), j);
      colDataAppendNULL(taosArrayGet(pDst->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX), j);
      colDataAppendNULL(taosArrayGet(pDst->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX), j);
      j++;
    }
  }
  uint32_t cap = pDst->info.capacity;
  pDst->info = pSrc->info;
  pDst->info.rows = j;
  pDst->info.capacity = cap;

  return 0;
}

// for partition by tag
static void setBlockGroupIdByUid(SStreamScanInfo* pInfo, SSDataBlock* pBlock) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  TSKEY*           startTsCol = (TSKEY*)pStartTsCol->pData;
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  uint64_t*        gpCol = (uint64_t*)pGpCol->pData;
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  uint64_t*        uidCol = (uint64_t*)pUidCol->pData;
  int32_t          rows = pBlock->info.rows;
  if (!pInfo->partitionSup.needCalc) {
    for (int32_t i = 0; i < rows; i++) {
      uint64_t groupId = getGroupIdByUid(pInfo, uidCol[i]);
      colDataAppend(pGpCol, i, (const char*)&groupId, false);
    }
  }
}

static SSDataBlock* doStreamScan(SOperatorInfo* pOperator) {
  // NOTE: this operator does never check if current status is done or not
  SExecTaskInfo*   pTaskInfo = pOperator->pTaskInfo;
  SStreamScanInfo* pInfo = pOperator->info;

  qDebug("stream scan called");

  if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE1 ||
      pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE2) {
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    memcpy(&pTSInfo->base.cond, &pTaskInfo->streamInfo.tableCond, sizeof(SQueryTableDataCond));
    if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE1) {
      pTSInfo->base.cond.startVersion = 0;
      pTSInfo->base.cond.endVersion = pTaskInfo->streamInfo.fillHistoryVer1;
      qDebug("stream recover step 1, from %" PRId64 " to %" PRId64, pTSInfo->base.cond.startVersion,
             pTSInfo->base.cond.endVersion);
    } else {
      pTSInfo->base.cond.startVersion = pTaskInfo->streamInfo.fillHistoryVer1 + 1;
      pTSInfo->base.cond.endVersion = pTaskInfo->streamInfo.fillHistoryVer2;
      qDebug("stream recover step 2, from %" PRId64 " to %" PRId64, pTSInfo->base.cond.startVersion,
             pTSInfo->base.cond.endVersion);
    }

    /*resetTableScanInfo(pTSInfo, pWin);*/
    tsdbReaderClose(pTSInfo->base.dataReader);
    pTSInfo->base.dataReader = NULL;
    pInfo->pTableScanOp->status = OP_OPENED;

    pTSInfo->scanTimes = 0;
    pTSInfo->currentGroupId = -1;
    pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__SCAN;
    pTaskInfo->streamInfo.recoverScanFinished = false;
  }

  if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__SCAN) {
    if (pInfo->blockRecoverContiCnt > 100) {
      pInfo->blockRecoverTotCnt += pInfo->blockRecoverContiCnt;
      pInfo->blockRecoverContiCnt = 0;
      return NULL;
    }
    SSDataBlock* pBlock = doTableScan(pInfo->pTableScanOp);
    if (pBlock != NULL) {
      pInfo->blockRecoverContiCnt++;
      calBlockTbName(pInfo, pBlock);
      if (pInfo->pUpdateInfo) {
        TSKEY maxTs = updateInfoFillBlockData(pInfo->pUpdateInfo, pBlock, pInfo->primaryTsIndex);
        pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
      }
      qDebug("stream recover scan get block, rows %d", pBlock->info.rows);
      printDataBlock(pBlock, "scan recover");
      return pBlock;
    }
    pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__NONE;
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    tsdbReaderClose(pTSInfo->base.dataReader);
    pTSInfo->base.dataReader = NULL;

    pTSInfo->base.cond.startVersion = -1;
    pTSInfo->base.cond.endVersion = -1;

    pTaskInfo->streamInfo.recoverScanFinished = true;
    return NULL;
  }

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
// TODO: refactor
FETCH_NEXT_BLOCK:
  if (pInfo->blockType == STREAM_INPUT__DATA_BLOCK) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      /*pOperator->status = OP_EXEC_DONE;*/
      return NULL;
    }

    int32_t      current = pInfo->validBlockIndex++;
    SSDataBlock* pBlock = taosArrayGetP(pInfo->pBlockLists, current);
    if (pBlock->info.id.groupId && pBlock->info.parTbName[0]) {
      streamStatePutParName(pTaskInfo->streamInfo.pState, pBlock->info.id.groupId, pBlock->info.parTbName);
    }
    // TODO move into scan
    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    pBlock->info.dataLoad = 1;
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
        SSDataBlock* pDelBlock = NULL;
        if (pInfo->tqReader) {
          pDelBlock = createSpecialDataBlock(STREAM_DELETE_DATA);
          filterDelBlockByUid(pDelBlock, pBlock, pInfo);
        } else {
          pDelBlock = pBlock;
        }
        setBlockGroupIdByUid(pInfo, pDelBlock);
        printDataBlock(pDelBlock, "stream scan delete recv filtered");
        if (pDelBlock->info.rows == 0) {
          if (pInfo->tqReader) {
            blockDataDestroy(pDelBlock);
          }
          goto FETCH_NEXT_BLOCK;
        }
        if (!isIntervalWindow(pInfo) && !isSessionWindow(pInfo) && !isStateWindow(pInfo)) {
          generateDeleteResultBlock(pInfo, pDelBlock, pInfo->pDeleteDataRes);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_RESULT;
          printDataBlock(pDelBlock, "stream scan delete result");
          blockDataDestroy(pDelBlock);

          if (pInfo->pDeleteDataRes->info.rows > 0) {
            return pInfo->pDeleteDataRes;
          } else {
            goto FETCH_NEXT_BLOCK;
          }
        } else {
          pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
          pInfo->updateResIndex = 0;
          generateScanRange(pInfo, pDelBlock, pInfo->pUpdateRes);
          prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
          copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
          printDataBlock(pDelBlock, "stream scan delete data");
          if (pInfo->tqReader) {
            blockDataDestroy(pDelBlock);
          }
          if (pInfo->pDeleteDataRes->info.rows > 0) {
            pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
            return pInfo->pDeleteDataRes;
          } else {
            goto FETCH_NEXT_BLOCK;
          }
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
          uint64_t        version = getReaderMaxVersion(pTableScanInfo->base.dataReader);
          updateInfoSetScanRange(pInfo->pUpdateInfo, &pTableScanInfo->base.cond.twindows, pInfo->groupId, version);
          pSDB->info.type = pInfo->scanMode == STREAM_SCAN_FROM_DATAREADER_RANGE ? STREAM_NORMAL : STREAM_PULL_DATA;
          checkUpdateData(pInfo, true, pSDB, false);
          // printDataBlock(pSDB, "stream scan update");
          calBlockTbName(pInfo, pSDB);
          return pSDB;
        }
        blockDataCleanup(pInfo->pUpdateDataRes);
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

  NEXT_SUBMIT_BLK:
    while (1) {
      if (pInfo->tqReader->pMsg == NULL) {
        if (pInfo->validBlockIndex >= totBlockNum) {
          updateInfoDestoryColseWinSBF(pInfo->pUpdateInfo);
          doClearBufferedBlocks(pInfo);
          qDebug("stream scan return empty, consume block %d", totBlockNum);
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

        setBlockIntoRes(pInfo, &block, false);

        if (updateInfoIgnore(pInfo->pUpdateInfo, &pInfo->pRes->info.window, pInfo->pRes->info.id.groupId,
                             pInfo->pRes->info.version)) {
          printDataBlock(pInfo->pRes, "stream scan ignore");
          blockDataCleanup(pInfo->pRes);
          continue;
        }

        if (pInfo->pUpdateInfo) {
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

        doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
        pInfo->pRes->info.dataLoad = 1;
        blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);

        if (pBlockInfo->rows > 0 || pInfo->pUpdateDataRes->info.rows > 0) {
          break;
        }
      }
      if (pBlockInfo->rows > 0 || pInfo->pUpdateDataRes->info.rows > 0) {
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

    qDebug("scan rows: %d", pBlockInfo->rows);
    if (pBlockInfo->rows > 0) {
      return pInfo->pRes;
    }

    if (pInfo->pUpdateDataRes->info.rows > 0) {
      goto FETCH_NEXT_BLOCK;
    }

    goto NEXT_SUBMIT_BLK;
  } else {
    ASSERT(0);
    return NULL;
  }
}

static SArray* extractTableIdList(const STableListInfo* pTableListInfo) {
  SArray* tableIdList = taosArrayInit(4, sizeof(uint64_t));

  // Transfer the Array of STableKeyInfo into uid list.
  size_t size = tableListGetSize(pTableListInfo);
  for (int32_t i = 0; i < size; ++i) {
    STableKeyInfo* pkeyInfo = tableListGetInfo(pTableListInfo, i);
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
    if (pInfo->dataReader && tsdbNextDataBlock(pInfo->dataReader)) {
      if (isTaskKilled(pTaskInfo)) {
        longjmp(pTaskInfo->env, pTaskInfo->code);
      }

      SSDataBlock* pBlock = tsdbRetrieveDataBlock(pInfo->dataReader, NULL);
      if (pBlock == NULL) {
        longjmp(pTaskInfo->env, terrno);
      }

      qDebug("tmqsnap doRawScan get data uid:%" PRId64 "", pBlock->info.id.uid);
      pTaskInfo->streamInfo.lastStatus.type = TMQ_OFFSET__SNAPSHOT_DATA;
      pTaskInfo->streamInfo.lastStatus.uid = pBlock->info.id.uid;
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
      qDebug("tmqsnap change get data uid:%" PRId64 "", mtInfo.uid);
      qStreamPrepareScan(pTaskInfo, &pTaskInfo->streamInfo.prepareStatus, pInfo->sContext->subType);
    }
    tDeleteSSchemaWrapper(mtInfo.schema);
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

  int32_t code = TSDB_CODE_SUCCESS;

  SStreamRawScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamRawScanInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  pInfo->vnode = pHandle->vnode;

  pInfo->sContext = pHandle->sContext;
  setOperatorInfo(pOperator, "RawScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(NULL, doRawScan, NULL, destroyRawScanOperatorInfo, optrDefaultBufFn, NULL);
  return pOperator;

_end:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

static void destroyStreamScanOperatorInfo(void* param) {
  SStreamScanInfo* pStreamScan = (SStreamScanInfo*)param;
  if (pStreamScan->pTableScanOp && pStreamScan->pTableScanOp->info) {
    destroyOperatorInfo(pStreamScan->pTableScanOp);
  }
  if (pStreamScan->tqReader) {
    tqCloseReader(pStreamScan->tqReader);
  }
  if (pStreamScan->matchInfo.pList) {
    taosArrayDestroy(pStreamScan->matchInfo.pList);
  }
  if (pStreamScan->pPseudoExpr) {
    destroyExprInfo(pStreamScan->pPseudoExpr, pStreamScan->numOfPseudoExpr);
    taosMemoryFree(pStreamScan->pPseudoExpr);
  }

  cleanupExprSupp(&pStreamScan->tbnameCalSup);

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
  SArray*          pColIds = NULL;
  SStreamScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamScanInfo));
  SOperatorInfo*   pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (pInfo == NULL || pOperator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  SScanPhysiNode*     pScanPhyNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;

  pInfo->pTagCond = pTagCond;
  pInfo->pGroupTags = pTableScanNode->pGroupTags;

  int32_t numOfCols = 0;
  int32_t code =
      extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t numOfOutput = taosArrayGetSize(pInfo->matchInfo.pList);
  pColIds = taosArrayInit(numOfOutput, sizeof(int16_t));
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColMatchItem* id = taosArrayGet(pInfo->matchInfo.pList, i);

    int16_t colId = id->colId;
    taosArrayPush(pColIds, &colId);
    if (id->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pInfo->primaryTsIndex = id->dstSlotId;
    }
  }

  if (pTableScanNode->pSubtable != NULL) {
    SExprInfo* pSubTableExpr = taosMemoryCalloc(1, sizeof(SExprInfo));
    if (pSubTableExpr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    createExprFromOneNode(pSubTableExpr, pTableScanNode->pSubtable, 0);
    if (initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1) != 0) {
      goto _error;
    }
  }

  if (pTableScanNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExprInfo(pTableScanNode->pTags, NULL, &numOfTags);
    if (pTagExpr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
    }
    if (initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _error;
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
      pTSInfo->base.cond.endVersion = pHandle->version;
    }

    STableKeyInfo* pList = NULL;
    int32_t        num = 0;
    tableListGetGroupList(pTaskInfo->pTableInfoList, 0, &pList, &num);

    if (pHandle->initTableReader) {
      pTSInfo->scanMode = TABLE_SCAN__TABLE_ORDER;
      pTSInfo->base.dataReader = NULL;
      code = tsdbReaderOpen(pHandle->vnode, &pTSInfo->base.cond, pList, num, pTSInfo->pResBlock,
                            &pTSInfo->base.dataReader, NULL);
      if (code != 0) {
        terrno = code;
        destroyTableScanOperatorInfo(pTableScanOp);
        goto _error;
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
    if (pInfo->pTableScanOp->pTaskInfo->streamInfo.pState) {
      streamStateSetNumber(pInfo->pTableScanOp->pTaskInfo->streamInfo.pState, -1);
    }

    pInfo->readHandle = *pHandle;
    pInfo->tableUid = pScanPhyNode->uid;
    pTaskInfo->streamInfo.snapshotVer = pHandle->version;

    // set the extract column id to streamHandle
    tqReaderSetColIdList(pInfo->tqReader, pColIds);
    SArray* tableIdList = extractTableIdList(pTaskInfo->pTableInfoList);
    code = tqReaderSetTbUidList(pInfo->tqReader, tableIdList);
    if (code != 0) {
      taosArrayDestroy(tableIdList);
      goto _error;
    }
    taosArrayDestroy(tableIdList);
    memcpy(&pTaskInfo->streamInfo.tableCond, &pTSInfo->base.cond, sizeof(SQueryTableDataCond));
  } else {
    taosArrayDestroy(pColIds);
    pColIds = NULL;
  }

  // create the pseduo columns info
  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    pInfo->pPseudoExpr = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pInfo->numOfPseudoExpr);
  }

  code = filterInitFromNode((SNode*)pScanPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  pInfo->pUpdateRes = createSpecialDataBlock(STREAM_CLEAR);
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
  pInfo->igCheckUpdate = pTableScanNode->igCheckUpdate;
  pInfo->igExpired = pTableScanNode->igExpired;
  pInfo->twAggSup.maxTs = INT64_MIN;

  setOperatorInfo(pOperator, "StreamScanOperator", QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  __optr_fn_t nextFn = pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM ? doStreamScan : doQueueScan;
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, nextFn, NULL, destroyStreamScanOperatorInfo, optrDefaultBufFn, NULL);

  return pOperator;

_error:
  if (pColIds != NULL) {
    taosArrayDestroy(pColIds);
  }

  if (pInfo != NULL) {
    destroyStreamScanOperatorInfo(pInfo);
  }

  taosMemoryFreeClear(pOperator);
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
  blockDataCleanup(pRes);

  int32_t size = tableListGetSize(pTaskInfo->pTableInfoList);
  if (size == 0) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    return NULL;
  }

  char        str[512] = {0};
  int32_t     count = 0;
  SMetaReader mr = {0};
  metaReaderInit(&mr, pInfo->readHandle.meta, 0);

  while (pInfo->curPos < size && count < pOperator->resultInfo.capacity) {
    STableKeyInfo* item = tableListGetInfo(pTaskInfo->pTableInfoList, pInfo->curPos);
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
      setOperatorCompleted(pOperator);
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
  taosArrayDestroy(pInfo->matchInfo.pList);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pPhyNode,
                                         SExecTaskInfo* pTaskInfo) {
  STagScanInfo*  pInfo = taosMemoryCalloc(1, sizeof(STagScanInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pPhyNode->node.pOutputDataBlockDesc;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->pScanPseudoCols, NULL, &numOfExprs);
  int32_t    code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t num = 0;
  code = extractColMatchInfo(pPhyNode->pScanPseudoCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  pInfo->readHandle = *pReadHandle;
  pInfo->curPos = 0;

  setOperatorInfo(pOperator, "TagScanOperator", QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, doTagScan, NULL, destroyTagScanOperatorInfo, optrDefaultBufFn, NULL);

  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

static SSDataBlock* getTableDataBlockImpl(void* param) {
  STableMergeScanSortSourceParam* source = param;
  SOperatorInfo*                  pOperator = source->pOperator;
  STableMergeScanInfo*            pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  int32_t                         readIdx = source->readerIdx;
  SSDataBlock*                    pBlock = source->inputBlock;

  SQueryTableDataCond* pQueryCond = taosArrayGet(pInfo->queryConds, readIdx);

  int64_t      st = taosGetTimestampUs();
  void*        p = tableListGetInfo(pTaskInfo->pTableInfoList, readIdx + pInfo->tableStartIndex);
  SReadHandle* pHandle = &pInfo->base.readHandle;

  int32_t code =
      tsdbReaderOpen(pHandle->vnode, pQueryCond, p, 1, pBlock, &pInfo->base.dataReader, GET_TASKID(pTaskInfo));
  if (code != 0) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  STsdbReader* reader = pInfo->base.dataReader;
  while (tsdbNextDataBlock(reader)) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    // process this data block based on the probabilities
    bool processThisBlock = processBlockWithProbability(&pInfo->sample);
    if (!processThisBlock) {
      continue;
    }

    if (pQueryCond->order == TSDB_ORDER_ASC) {
      pQueryCond->twindows.skey = pBlock->info.window.ekey + 1;
    } else {
      pQueryCond->twindows.ekey = pBlock->info.window.skey - 1;
    }

    uint32_t status = 0;
    code = loadDataBlock(pOperator, &pInfo->base, pBlock, &status);
    //    code = loadDataBlockFromOneTable(pOperator, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
      continue;
    }

    pBlock->info.id.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.id.uid);

    pOperator->resultInfo.totalRows += pBlock->info.rows;
    pInfo->base.readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;

    tsdbReaderClose(pInfo->base.dataReader);
    pInfo->base.dataReader = NULL;
    return pBlock;
  }

  tsdbReaderClose(pInfo->base.dataReader);
  pInfo->base.dataReader = NULL;
  return NULL;
}

SArray* generateSortByTsInfo(SArray* colMatchInfo, int32_t order) {
  int32_t tsTargetSlotId = 0;
  for (int32_t i = 0; i < taosArrayGetSize(colMatchInfo); ++i) {
    SColMatchItem* colInfo = taosArrayGet(colMatchInfo, i);
    if (colInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      tsTargetSlotId = colInfo->dstSlotId;
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

int32_t dumpQueryTableCond(const SQueryTableDataCond* src, SQueryTableDataCond* dst) {
  memcpy((void*)dst, (void*)src, sizeof(SQueryTableDataCond));
  dst->colList = taosMemoryCalloc(src->numOfCols, sizeof(SColumnInfo));
  for (int i = 0; i < src->numOfCols; i++) {
    dst->colList[i] = src->colList[i];
  }
  return 0;
}

int32_t startGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  {
    size_t  numOfTables = tableListGetSize(pTaskInfo->pTableInfoList);
    int32_t i = pInfo->tableStartIndex + 1;
    for (; i < numOfTables; ++i) {
      STableKeyInfo* tableKeyInfo = tableListGetInfo(pTaskInfo->pTableInfoList, i);
      if (tableKeyInfo->groupId != pInfo->groupId) {
        break;
      }
    }
    pInfo->tableEndIndex = i - 1;
  }

  int32_t tableStartIdx = pInfo->tableStartIndex;
  int32_t tableEndIdx = pInfo->tableEndIndex;

  pInfo->base.dataReader = NULL;

  // todo the total available buffer should be determined by total capacity of buffer of this task.
  // the additional one is reserved for merge result
  pInfo->sortBufSize = pInfo->bufPageSize * (tableEndIdx - tableStartIdx + 1 + 1);
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_MULTISOURCE_MERGE, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->pSortInputBlock, pTaskInfo->id.str);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, getTableDataBlockImpl, NULL, NULL);

  // one table has one data block
  int32_t numOfTable = tableEndIdx - tableStartIdx + 1;
  pInfo->queryConds = taosArrayInit(numOfTable, sizeof(SQueryTableDataCond));

  for (int32_t i = 0; i < numOfTable; ++i) {
    STableMergeScanSortSourceParam param = {0};
    param.readerIdx = i;
    param.pOperator = pOperator;
    param.inputBlock = createOneDataBlock(pInfo->pResBlock, false);
    blockDataEnsureCapacity(param.inputBlock, pOperator->resultInfo.capacity);

    taosArrayPush(pInfo->sortSourceParams, &param);

    SQueryTableDataCond cond;
    dumpQueryTableCond(&pInfo->base.cond, &cond);
    taosArrayPush(pInfo->queryConds, &cond);
  }

  for (int32_t i = 0; i < numOfTable; ++i) {
    SSortSource*                    ps = taosMemoryCalloc(1, sizeof(SSortSource));
    STableMergeScanSortSourceParam* param = taosArrayGet(pInfo->sortSourceParams, i);
    ps->param = param;
    ps->onlyRef = true;
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

  int32_t numOfTable = taosArrayGetSize(pInfo->queryConds);

  SSortExecInfo sortExecInfo = tsortGetSortExecInfo(pInfo->pSortHandle);
  pInfo->sortExecInfo.sortMethod = sortExecInfo.sortMethod;
  pInfo->sortExecInfo.sortBuffer = sortExecInfo.sortBuffer;
  pInfo->sortExecInfo.loops += sortExecInfo.loops;
  pInfo->sortExecInfo.readBytes += sortExecInfo.readBytes;
  pInfo->sortExecInfo.writeBytes += sortExecInfo.writeBytes;

  for (int32_t i = 0; i < numOfTable; ++i) {
    STableMergeScanSortSourceParam* param = taosArrayGet(pInfo->sortSourceParams, i);
    blockDataDestroy(param->inputBlock);
  }
  taosArrayClear(pInfo->sortSourceParams);

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->queryConds); i++) {
    SQueryTableDataCond* cond = taosArrayGet(pInfo->queryConds, i);
    taosMemoryFree(cond->colList);
  }
  taosArrayDestroy(pInfo->queryConds);
  pInfo->queryConds = NULL;

  resetLimitInfoForNextGroup(&pInfo->limitInfo);
  return TSDB_CODE_SUCCESS;
}

// all data produced by this function only belongs to one group
// slimit/soffset does not need to be concerned here, since this function only deal with data within one group.
SSDataBlock* getSortedTableMergeScanBlockData(SSortHandle* pHandle, SSDataBlock* pResBlock, int32_t capacity,
                                              SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  blockDataCleanup(pResBlock);

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

  bool limitReached = applyLimitOffset(&pInfo->limitInfo, pResBlock, pTaskInfo);
  qDebug("%s get sorted row block, rows:%d, limit:%"PRId64, GET_TASKID(pTaskInfo), pResBlock->info.rows,
         pInfo->limitInfo.numOfOutputRows);

  if (limitReached) {
    resetLimitInfoForNextGroup(&pInfo->limitInfo);
  }
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

  size_t tableListSize = tableListGetSize(pTaskInfo->pTableInfoList);
  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      setOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->tableStartIndex = 0;
    pInfo->groupId = ((STableKeyInfo*)tableListGetInfo(pTaskInfo->pTableInfoList, pInfo->tableStartIndex))->groupId;
    startGroupTableMergeScan(pOperator);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->tableStartIndex < tableListSize) {
    pBlock = getSortedTableMergeScanBlockData(pInfo->pSortHandle, pInfo->pResBlock, pOperator->resultInfo.capacity,
                                              pOperator);
    if (pBlock != NULL) {
      pBlock->info.id.groupId = pInfo->groupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      return pBlock;
    } else {
      // Data of this group are all dumped, let's try the next group
      stopGroupTableMergeScan(pOperator);
      if (pInfo->tableEndIndex >= tableListSize - 1) {
        setOperatorCompleted(pOperator);
        break;
      }

      pInfo->tableStartIndex = pInfo->tableEndIndex + 1;
      pInfo->groupId = tableListGetInfo(pTaskInfo->pTableInfoList, pInfo->tableStartIndex)->groupId;
      startGroupTableMergeScan(pOperator);
    }
  }

  return pBlock;
}

void destroyTableMergeScanOperatorInfo(void* param) {
  STableMergeScanInfo* pTableScanInfo = (STableMergeScanInfo*)param;
  cleanupQueryTableDataCond(&pTableScanInfo->base.cond);

  int32_t numOfTable = taosArrayGetSize(pTableScanInfo->queryConds);

  for (int32_t i = 0; i < numOfTable; i++) {
    STableMergeScanSortSourceParam* p = taosArrayGet(pTableScanInfo->sortSourceParams, i);
    blockDataDestroy(p->inputBlock);
  }

  taosArrayDestroy(pTableScanInfo->sortSourceParams);
  tsortDestroySortHandle(pTableScanInfo->pSortHandle);
  pTableScanInfo->pSortHandle = NULL;

  tsdbReaderClose(pTableScanInfo->base.dataReader);
  pTableScanInfo->base.dataReader = NULL;

  for (int i = 0; i < taosArrayGetSize(pTableScanInfo->queryConds); i++) {
    SQueryTableDataCond* pCond = taosArrayGet(pTableScanInfo->queryConds, i);
    taosMemoryFree(pCond->colList);
  }
  taosArrayDestroy(pTableScanInfo->queryConds);

  if (pTableScanInfo->base.matchInfo.pList != NULL) {
    taosArrayDestroy(pTableScanInfo->base.matchInfo.pList);
  }

  pTableScanInfo->pResBlock = blockDataDestroy(pTableScanInfo->pResBlock);
  pTableScanInfo->pSortInputBlock = blockDataDestroy(pTableScanInfo->pSortInputBlock);

  taosArrayDestroy(pTableScanInfo->pSortInfo);
  cleanupExprSupp(&pTableScanInfo->base.pseudoSup);

  tsdbReaderClose(pTableScanInfo->base.dataReader);
  pTableScanInfo->base.dataReader = NULL;
  taosLRUCacheCleanup(pTableScanInfo->base.metaCache.pTableMetaEntryCache);

  taosMemoryFreeClear(param);
}

int32_t getTableMergeScanExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  ASSERT(pOptr != NULL);
  // TODO: merge these two info into one struct
  STableMergeScanExecInfo* execInfo = taosMemoryCalloc(1, sizeof(STableMergeScanExecInfo));
  STableMergeScanInfo*     pInfo = pOptr->info;
  execInfo->blockRecorder = pInfo->base.readRecorder;
  execInfo->sortExecInfo = pInfo->sortExecInfo;

  *pOptrExplain = execInfo;
  *len = sizeof(STableMergeScanExecInfo);

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle,
                                                SExecTaskInfo* pTaskInfo) {
  STableMergeScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableMergeScanInfo));
  SOperatorInfo*       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pTableScanNode->scan.node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  int32_t code = extractColMatchInfo(pTableScanNode->scan.pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                                     &pInfo->base.matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pInfo->base.matchInfo.pList);
    goto _error;
  }

  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->base.pseudoSup;
    pSup->pExprInfo = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};

  pInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
  if (pInfo->base.metaCache.pTableMetaEntryCache == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->base.dataBlockLoadFlag = FUNC_DATA_REQUIRED_DATA_LOAD;
  pInfo->base.scanFlag = MAIN_SCAN;
  pInfo->base.readHandle = *readHandle;

  pInfo->base.limitInfo.limit.limit = -1;
  pInfo->base.limitInfo.slimit.limit = -1;

  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  pInfo->pResBlock = createDataBlockFromDescNode(pDescNode);
  blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);

  pInfo->sortSourceParams = taosArrayInit(64, sizeof(STableMergeScanSortSourceParam));

  pInfo->pSortInfo = generateSortByTsInfo(pInfo->base.matchInfo.pList, pInfo->base.cond.order);
  pInfo->pSortInputBlock = createOneDataBlock(pInfo->pResBlock, false);
  initLimitInfo(pTableScanNode->scan.node.pLimit, pTableScanNode->scan.node.pSlimit, &pInfo->limitInfo);

  int32_t  rowSize = pInfo->pResBlock->info.rowSize;
  uint32_t nCols = taosArrayGetSize(pInfo->pResBlock->pDataBlock);
  pInfo->bufPageSize = getProperSortPageSize(rowSize, nCols);

  setOperatorInfo(pOperator, "TableMergeScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableMergeScan, NULL, destroyTableMergeScanOperatorInfo,
                                         optrDefaultBufFn, getTableMergeScanExplainExecInfo);
  pOperator->cost.openCost = 0;
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}

// ====================================================================================================================
// TableCountScanOperator
static SSDataBlock* doTableCountScan(SOperatorInfo* pOperator);
static void         destoryTableCountScanOperator(void* param);
static void         buildVnodeGroupedStbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                                   SSDataBlock* pRes, char* dbName, tb_uid_t stbUid);
static void         buildVnodeGroupedNtbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                                   SSDataBlock* pRes, char* dbName);
static void         buildVnodeFilteredTbCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                              STableCountScanSupp* pSupp, SSDataBlock* pRes, char* dbName);
static void         buildVnodeGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                                STableCountScanSupp* pSupp, SSDataBlock* pRes, int32_t vgId, char* dbName);
static SSDataBlock* buildVnodeDbTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                           STableCountScanSupp* pSupp, SSDataBlock* pRes);
static void         buildSysDbGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                                STableCountScanSupp* pSupp, SSDataBlock* pRes, size_t infodbTableNum,
                                                size_t perfdbTableNum);
static void         buildSysDbFilterTableCount(SOperatorInfo* pOperator, STableCountScanSupp* pSupp, SSDataBlock* pRes,
                                               size_t infodbTableNum, size_t perfdbTableNum);
static const char*  GROUP_TAG_DB_NAME = "db_name";
static const char*  GROUP_TAG_STABLE_NAME = "stable_name";

int32_t tblCountScanGetGroupTagsSlotId(const SNodeList* scanCols, STableCountScanSupp* supp) {
  if (scanCols != NULL) {
    SNode* pNode = NULL;
    FOREACH(pNode, scanCols) {
      if (nodeType(pNode) != QUERY_NODE_TARGET) {
        return TSDB_CODE_QRY_SYS_ERROR;
      }
      STargetNode* targetNode = (STargetNode*)pNode;
      if (nodeType(targetNode->pExpr) != QUERY_NODE_COLUMN) {
        return TSDB_CODE_QRY_SYS_ERROR;
      }
      SColumnNode* colNode = (SColumnNode*)(targetNode->pExpr);
      if (strcmp(colNode->colName, GROUP_TAG_DB_NAME) == 0) {
        supp->dbNameSlotId = targetNode->slotId;
      } else if (strcmp(colNode->colName, GROUP_TAG_STABLE_NAME) == 0) {
        supp->stbNameSlotId = targetNode->slotId;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tblCountScanGetCountSlotId(const SNodeList* pseudoCols, STableCountScanSupp* supp) {
  if (pseudoCols != NULL) {
    SNode* pNode = NULL;
    FOREACH(pNode, pseudoCols) {
      if (nodeType(pNode) != QUERY_NODE_TARGET) {
        return TSDB_CODE_QRY_SYS_ERROR;
      }
      STargetNode* targetNode = (STargetNode*)pNode;
      if (nodeType(targetNode->pExpr) != QUERY_NODE_FUNCTION) {
        return TSDB_CODE_QRY_SYS_ERROR;
      }
      SFunctionNode* funcNode = (SFunctionNode*)(targetNode->pExpr);
      if (funcNode->funcType == FUNCTION_TYPE_TABLE_COUNT) {
        supp->tbCountSlotId = targetNode->slotId;
      }
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tblCountScanGetInputs(SNodeList* groupTags, SName* tableName, STableCountScanSupp* supp) {
  if (groupTags != NULL) {
    SNode* pNode = NULL;
    FOREACH(pNode, groupTags) {
      if (nodeType(pNode) != QUERY_NODE_COLUMN) {
        return TSDB_CODE_QRY_SYS_ERROR;
      }
      SColumnNode* colNode = (SColumnNode*)pNode;
      if (strcmp(colNode->colName, GROUP_TAG_DB_NAME) == 0) {
        supp->groupByDbName = true;
      }
      if (strcmp(colNode->colName, GROUP_TAG_STABLE_NAME) == 0) {
        supp->groupByStbName = true;
      }
    }
  } else {
    strncpy(supp->dbNameFilter, tNameGetDbNameP(tableName), TSDB_DB_NAME_LEN);
    strncpy(supp->stbNameFilter, tNameGetTableName(tableName), TSDB_TABLE_NAME_LEN);
  }
  return TSDB_CODE_SUCCESS;
}

int32_t getTableCountScanSupp(SNodeList* groupTags, SName* tableName, SNodeList* scanCols, SNodeList* pseudoCols,
                              STableCountScanSupp* supp, SExecTaskInfo* taskInfo) {
  int32_t code = 0;
  code = tblCountScanGetInputs(groupTags, tableName, supp);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s get table count scan supp. get inputs error", GET_TASKID(taskInfo));
    return code;
  }
  supp->dbNameSlotId = -1;
  supp->stbNameSlotId = -1;
  supp->tbCountSlotId = -1;

  code = tblCountScanGetGroupTagsSlotId(scanCols, supp);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s get table count scan supp. get group tags slot id error", GET_TASKID(taskInfo));
    return code;
  }
  code = tblCountScanGetCountSlotId(pseudoCols, supp);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s get table count scan supp. get count error", GET_TASKID(taskInfo));
    return code;
  }
  return code;
}

SOperatorInfo* createTableCountScanOperatorInfo(SReadHandle* readHandle, STableCountScanPhysiNode* pTblCountScanNode,
                                                SExecTaskInfo* pTaskInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  SScanPhysiNode*              pScanNode = &pTblCountScanNode->scan;
  STableCountScanOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STableCountScanOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));

  if (!pInfo || !pOperator) {
    goto _error;
  }

  pInfo->readHandle = *readHandle;

  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;
  initResultSizeInfo(&pOperator->resultInfo, 1);
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  getTableCountScanSupp(pTblCountScanNode->pGroupTags, &pTblCountScanNode->scan.tableName,
                        pTblCountScanNode->scan.pScanCols, pTblCountScanNode->scan.pScanPseudoCols, &pInfo->supp,
                        pTaskInfo);

  setOperatorInfo(pOperator, "TableCountScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableCountScan, NULL, destoryTableCountScanOperator,
                                         optrDefaultBufFn, NULL);
  return pOperator;

_error:
  if (pInfo != NULL) {
    destoryTableCountScanOperator(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
  return NULL;
}

void fillTableCountScanDataBlock(STableCountScanSupp* pSupp, char* dbName, char* stbName, int64_t count,
                                 SSDataBlock* pRes) {
  if (pSupp->dbNameSlotId != -1) {
    ASSERT(strlen(dbName));
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->dbNameSlotId);

    char varDbName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    tstrncpy(varDataVal(varDbName), dbName, TSDB_DB_NAME_LEN);

    varDataSetLen(varDbName, strlen(dbName));
    colDataAppend(colInfoData, 0, varDbName, false);
  }

  if (pSupp->stbNameSlotId != -1) {
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->stbNameSlotId);
    if (strlen(stbName) != 0) {
      char varStbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      strncpy(varDataVal(varStbName), stbName, TSDB_TABLE_NAME_LEN);
      varDataSetLen(varStbName, strlen(stbName));
      colDataAppend(colInfoData, 0, varStbName, false);
    } else {
      colDataAppendNULL(colInfoData, 0);
    }
  }

  if (pSupp->tbCountSlotId != -1) {
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->tbCountSlotId);
    colDataAppend(colInfoData, 0, (char*)&count, false);
  }
  pRes->info.rows = 1;
}

static SSDataBlock* buildSysDbTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo) {
  STableCountScanSupp* pSupp = &pInfo->supp;
  SSDataBlock*         pRes = pInfo->pRes;

  size_t infodbTableNum;
  getInfosDbMeta(NULL, &infodbTableNum);
  size_t perfdbTableNum;
  getPerfDbMeta(NULL, &perfdbTableNum);

  if (pSupp->groupByDbName) {
    buildSysDbGroupedTableCount(pOperator, pInfo, pSupp, pRes, infodbTableNum, perfdbTableNum);
    return (pRes->info.rows > 0) ? pRes : NULL;
  } else {
    buildSysDbFilterTableCount(pOperator, pSupp, pRes, infodbTableNum, perfdbTableNum);
    return (pRes->info.rows > 0) ? pRes : NULL;
  }
}

static void buildSysDbFilterTableCount(SOperatorInfo* pOperator, STableCountScanSupp* pSupp, SSDataBlock* pRes,
                                       size_t infodbTableNum, size_t perfdbTableNum) {
  if (strcmp(pSupp->dbNameFilter, TSDB_INFORMATION_SCHEMA_DB) == 0) {
    fillTableCountScanDataBlock(pSupp, TSDB_INFORMATION_SCHEMA_DB, "", infodbTableNum, pRes);
  } else if (strcmp(pSupp->dbNameFilter, TSDB_PERFORMANCE_SCHEMA_DB) == 0) {
    fillTableCountScanDataBlock(pSupp, TSDB_PERFORMANCE_SCHEMA_DB, "", perfdbTableNum, pRes);
  } else if (strlen(pSupp->dbNameFilter) == 0) {
    fillTableCountScanDataBlock(pSupp, "", "", infodbTableNum + perfdbTableNum, pRes);
  }
  setOperatorCompleted(pOperator);
}

static void buildSysDbGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                        STableCountScanSupp* pSupp, SSDataBlock* pRes, size_t infodbTableNum,
                                        size_t perfdbTableNum) {
  if (pInfo->currGrpIdx == 0) {
    uint64_t groupId = calcGroupId(TSDB_INFORMATION_SCHEMA_DB, strlen(TSDB_INFORMATION_SCHEMA_DB));
    pRes->info.id.groupId = groupId;
    fillTableCountScanDataBlock(pSupp, TSDB_INFORMATION_SCHEMA_DB, "", infodbTableNum, pRes);
  } else if (pInfo->currGrpIdx == 1) {
    uint64_t groupId = calcGroupId(TSDB_PERFORMANCE_SCHEMA_DB, strlen(TSDB_PERFORMANCE_SCHEMA_DB));
    pRes->info.id.groupId = groupId;
    fillTableCountScanDataBlock(pSupp, TSDB_PERFORMANCE_SCHEMA_DB, "", perfdbTableNum, pRes);
  } else {
    setOperatorCompleted(pOperator);
  }
  pInfo->currGrpIdx++;
}

static SSDataBlock* doTableCountScan(SOperatorInfo* pOperator) {
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  STableCountScanOperatorInfo* pInfo = pOperator->info;
  STableCountScanSupp*         pSupp = &pInfo->supp;
  SSDataBlock*                 pRes = pInfo->pRes;
  blockDataCleanup(pRes);

  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  if (pInfo->readHandle.mnd != NULL) {
    return buildSysDbTableCount(pOperator, pInfo);
  }

  return buildVnodeDbTableCount(pOperator, pInfo, pSupp, pRes);
}

static SSDataBlock* buildVnodeDbTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                           STableCountScanSupp* pSupp, SSDataBlock* pRes) {
  const char* db = NULL;
  int32_t     vgId = 0;
  char        dbName[TSDB_DB_NAME_LEN] = {0};

  // get dbname
  vnodeGetInfo(pInfo->readHandle.vnode, &db, &vgId);
  SName sn = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  tNameGetDbName(&sn, dbName);

  if (pSupp->groupByDbName) {
    buildVnodeGroupedTableCount(pOperator, pInfo, pSupp, pRes, vgId, dbName);
  } else {
    buildVnodeFilteredTbCount(pOperator, pInfo, pSupp, pRes, dbName);
  }
  return pRes->info.rows > 0 ? pRes : NULL;
}

static void buildVnodeGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                        STableCountScanSupp* pSupp, SSDataBlock* pRes, int32_t vgId, char* dbName) {
  if (pSupp->groupByStbName) {
    if (pInfo->stbUidList == NULL) {
      pInfo->stbUidList = taosArrayInit(16, sizeof(tb_uid_t));
      if (vnodeGetStbIdList(pInfo->readHandle.vnode, 0, pInfo->stbUidList) < 0) {
        qError("vgId:%d, failed to get stb id list error: %s", vgId, terrstr());
      }
    }
    if (pInfo->currGrpIdx < taosArrayGetSize(pInfo->stbUidList)) {
      tb_uid_t stbUid = *(tb_uid_t*)taosArrayGet(pInfo->stbUidList, pInfo->currGrpIdx);
      buildVnodeGroupedStbTableCount(pInfo, pSupp, pRes, dbName, stbUid);

      pInfo->currGrpIdx++;
    } else if (pInfo->currGrpIdx == taosArrayGetSize(pInfo->stbUidList)) {
      buildVnodeGroupedNtbTableCount(pInfo, pSupp, pRes, dbName);

      pInfo->currGrpIdx++;
    } else {
      setOperatorCompleted(pOperator);
    }
  } else {
    uint64_t groupId = calcGroupId(dbName, strlen(dbName));
    pRes->info.id.groupId = groupId;
    int64_t dbTableCount = metaGetTbNum(pInfo->readHandle.meta);
    fillTableCountScanDataBlock(pSupp, dbName, "", dbTableCount, pRes);
    setOperatorCompleted(pOperator);
  }
}

static void buildVnodeFilteredTbCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                      STableCountScanSupp* pSupp, SSDataBlock* pRes, char* dbName) {
  if (strlen(pSupp->dbNameFilter) != 0) {
    if (strlen(pSupp->stbNameFilter) != 0) {
      tb_uid_t      uid = metaGetTableEntryUidByName(pInfo->readHandle.meta, pSupp->stbNameFilter);
      SMetaStbStats stats = {0};
      metaGetStbStats(pInfo->readHandle.meta, uid, &stats);
      int64_t ctbNum = stats.ctbNum;
      fillTableCountScanDataBlock(pSupp, dbName, pSupp->stbNameFilter, ctbNum, pRes);
    } else {
      int64_t tbNumVnode = metaGetTbNum(pInfo->readHandle.meta);
      fillTableCountScanDataBlock(pSupp, dbName, "", tbNumVnode, pRes);
    }
  } else {
    int64_t tbNumVnode = metaGetTbNum(pInfo->readHandle.meta);
    fillTableCountScanDataBlock(pSupp, dbName, "", tbNumVnode, pRes);
  }
  setOperatorCompleted(pOperator);
}

static void buildVnodeGroupedNtbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                           SSDataBlock* pRes, char* dbName) {
  char fullStbName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbName, "");
  uint64_t groupId = calcGroupId(fullStbName, strlen(fullStbName));
  pRes->info.id.groupId = groupId;
  int64_t ntbNum = metaGetNtbNum(pInfo->readHandle.meta);
  if (ntbNum != 0) {
    fillTableCountScanDataBlock(pSupp, dbName, "", ntbNum, pRes);
  }
}

static void buildVnodeGroupedStbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                           SSDataBlock* pRes, char* dbName, tb_uid_t stbUid) {
  char stbName[TSDB_TABLE_NAME_LEN] = {0};
  metaGetTableSzNameByUid(pInfo->readHandle.meta, stbUid, stbName);

  char fullStbName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbName, stbName);
  uint64_t groupId = calcGroupId(fullStbName, strlen(fullStbName));
  pRes->info.id.groupId = groupId;

  SMetaStbStats stats = {0};
  metaGetStbStats(pInfo->readHandle.meta, stbUid, &stats);
  int64_t ctbNum = stats.ctbNum;

  fillTableCountScanDataBlock(pSupp, dbName, stbName, ctbNum, pRes);
}

static void destoryTableCountScanOperator(void* param) {
  STableCountScanOperatorInfo* pTableCountScanInfo = param;
  blockDataDestroy(pTableCountScanInfo->pRes);

  taosArrayDestroy(pTableCountScanInfo->stbUidList);
  taosMemoryFreeClear(param);
}
