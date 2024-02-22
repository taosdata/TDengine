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

#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "systable.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"

#include "query.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"
#include "operator.h"
#include "querytask.h"

#include "storageapi.h"
#include "wal.h"

int32_t scanDebug = 0;

#define MULTI_READER_MAX_TABLE_NUM        5000
#define SET_REVERSE_SCAN_FLAG(_info)      ((_info)->scanFlag = REVERSE_SCAN)
#define SWITCH_ORDER(n)                   (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))
#define STREAM_SCAN_OP_NAME               "StreamScanOperator"
#define STREAM_SCAN_OP_STATE_NAME         "StreamScanFillHistoryState"
#define STREAM_SCAN_OP_CHECKPOINT_NAME    "StreamScanOperator_Checkpoint"

typedef struct STableMergeScanExecInfo {
  SFileBlockLoadRecorder blockRecorder;
  SSortExecInfo          sortExecInfo;
} STableMergeScanExecInfo;

typedef struct STableMergeScanSortSourceParam {
  SOperatorInfo* pOperator;
  int32_t        readerIdx;
  uint64_t       uid;
  STsdbReader*   reader;
} STableMergeScanSortSourceParam;

typedef struct STableCountScanOperatorInfo {
  SReadHandle  readHandle;
  SSDataBlock* pRes;

  STableCountScanSupp supp;

  int32_t currGrpIdx;
  SArray* stbUidList;  // when group by db_name and/or stable_name
} STableCountScanOperatorInfo;

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

static bool overlapWithTimeWindow(SInterval* pInterval, SDataBlockInfo* pBlockInfo, int32_t order) {
  STimeWindow w = {0};

  // 0 by default, which means it is not a interval operator of the upstream operator.
  if (pInterval->interval == 0) {
    return false;
  }

  if (order == TSDB_ORDER_ASC) {
    w = getAlignQueryTimeWindow(pInterval, pBlockInfo->window.skey);
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
    w = getAlignQueryTimeWindow(pInterval, pBlockInfo->window.ekey);
    ASSERT(w.skey <= pBlockInfo->window.ekey);

    if (w.skey > pBlockInfo->window.skey) {
      return true;
    }

    while (1) {
      getNextTimeWindow(pInterval, &w, order);
      if (w.ekey < pBlockInfo->window.skey) {
        break;
      }

      ASSERT(w.skey < pBlockInfo->window.skey);
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

static int32_t insertTableToScanIgnoreList(STableScanInfo* pTableScanInfo, uint64_t uid) {
  if (NULL == pTableScanInfo->pIgnoreTables) {
    int32_t tableNum = taosArrayGetSize(pTableScanInfo->base.pTableListInfo->pTableList);
    pTableScanInfo->pIgnoreTables = taosHashInit(tableNum,  taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (NULL == pTableScanInfo->pIgnoreTables) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  taosHashPut(pTableScanInfo->pIgnoreTables, &uid, sizeof(uid), &pTableScanInfo->scanTimes, sizeof(pTableScanInfo->scanTimes));

  return TSDB_CODE_SUCCESS;
}

static int32_t doDynamicPruneDataBlock(SOperatorInfo* pOperator, SDataBlockInfo* pBlockInfo, uint32_t* status) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  int32_t code = TSDB_CODE_SUCCESS;

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
    code = insertTableToScanIgnoreList(pTableScanInfo, pBlockInfo->id.uid);
  }

  return code;
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
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  bool    allColumnsHaveAgg = true;
  bool    hasNullSMA = false;
  int32_t code = pAPI->tsdReader.tsdReaderRetrieveBlockSMAInfo(pTableScanInfo->dataReader, pBlock, &allColumnsHaveAgg, &hasNullSMA);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (!allColumnsHaveAgg || hasNullSMA) {
    return false;
  }
  return true;
}

static void doSetTagColumnData(STableScanBase* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo,
                               int32_t rows) {
  if (pTableScanInfo->pseudoSup.numOfExprs > 0) {
    SExprSupp* pSup = &pTableScanInfo->pseudoSup;

    int32_t code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock, rows,
                                          pTaskInfo, &pTableScanInfo->metaCache);
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
      blockDataTrimFirstRows(pBlock, pLimitInfo->remainOffset);
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
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

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
    qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64, GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->filterOutBlocks += 1;
    pCost->totalRows += pBlock->info.rows;
    pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_NOT_LOAD) {
    qDebug("%s data block skipped, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64 ", uid:%" PRIu64,
           GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows,
           pBlockInfo->id.uid);
    doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);
    pCost->skipBlocks += 1;
    pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
    return TSDB_CODE_SUCCESS;
  } else if (*status == FUNC_DATA_REQUIRED_SMA_LOAD) {
    pCost->loadBlockStatis += 1;
    loadSMA = true;  // mark the operation of load sma;
    bool success = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo);
    if (success) {  // failed to load the block sma data, data block statistics does not exist, load data block instead
      qDebug("%s data block SMA loaded, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64, GET_TASKID(pTaskInfo),
             pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
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
        qDebug("%s data block filter out by block SMA, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64,
               GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
        pCost->filterOutBlocks += 1;
        (*status) = FUNC_DATA_REQUIRED_FILTEROUT;

        pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  // free the sma info, since it should not be involved in later computing process.
  taosMemoryFreeClear(pBlock->pBlockAgg);

  // try to filter data block according to current results
  doDynamicPruneDataBlock(pOperator, pBlockInfo, status);
  if (*status == FUNC_DATA_REQUIRED_NOT_LOAD) {
    qDebug("%s data block skipped due to dynamic prune, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64,
           GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
    pCost->skipBlocks += 1;
    pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);

    STableScanInfo* p1 = pOperator->info;
    if (taosHashGetSize(p1->pIgnoreTables) == taosArrayGetSize(p1->base.pTableListInfo->pTableList)) {
      *status = FUNC_DATA_REQUIRED_ALL_FILTEROUT;
    } else {
      *status = FUNC_DATA_REQUIRED_FILTEROUT;
    }
    return TSDB_CODE_SUCCESS;
  }

  pCost->totalCheckedRows += pBlock->info.rows;
  pCost->loadBlocks += 1;

  SSDataBlock* p = pAPI->tsdReader.tsdReaderRetrieveDataBlock(pTableScanInfo->dataReader, NULL);
  if (p == NULL) {
    return terrno;
  }

  ASSERT(p == pBlock);
  doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);

  // restore the previous value
  pCost->totalRows -= pBlock->info.rows;

  if (pOperator->exprSupp.pFilterInfo != NULL) {
    int32_t code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, &pTableScanInfo->matchInfo);
    if (code != TSDB_CODE_SUCCESS) return code;

    int64_t st = taosGetTimestampUs();
    double el = (taosGetTimestampUs() - st) / 1000.0;
    pTableScanInfo->readRecorder.filterTime += el;

    if (pBlock->info.rows == 0) {
      pCost->filterOutBlocks += 1;
      qDebug("%s data block filter out, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64 ", elapsed time:%.2f ms",
             GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows, el);
    } else {
      qDebug("%s data block filter applied, elapsed time:%.2f ms", GET_TASKID(pTaskInfo), el);
    }
  }

  bool limitReached = applyLimitOffset(&pTableScanInfo->limitInfo, pBlock, pTaskInfo);
  if (limitReached) {  // set operator flag is done
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
  pVal->pName = taosStrdup(pMetaReader->me.name);
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
static void freeCachedMetaItem(const void* key, size_t keyLen, void* value, void* ud) {
  (void)key;
  (void)keyLen;
  (void)ud;
  freeTableCachedVal(value);
}

static void doSetNullValue(SSDataBlock* pBlock, const SExprInfo* pExpr, int32_t numOfExpr) {
  for (int32_t j = 0; j < numOfExpr; ++j) {
    int32_t dstSlotId = pExpr[j].base.resSchema.slotId;

    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, dstSlotId);
    colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
  }
}

int32_t addTagPseudoColumnData(SReadHandle* pHandle, const SExprInfo* pExpr, int32_t numOfExpr, SSDataBlock* pBlock,
                               int32_t rows, SExecTaskInfo* pTask, STableMetaCacheInfo* pCache) {
  int32_t         code = 0;
  bool            freeReader = false;
  LRUHandle*      h = NULL;
  STableCachedVal val = {0};
  SMetaReader     mr = {0};
  const char*     idStr = pTask->id.str;

  // currently only the tbname pseudo column
  if (numOfExpr <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  // todo: opt if only require the vgId and the vgVer;

  // backup the rows
  int32_t backupRows = pBlock->info.rows;
  pBlock->info.rows = rows;

  // todo refactor: extract method
  // the handling of the null data should be packed in the extracted method

  // 1. check if it is existed in meta cache
  if (pCache == NULL) {
    pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, 0, &pHandle->api.metaFn);
    code = pHandle->api.metaReaderFn.getEntryGetUidCache(&mr, pBlock->info.id.uid);
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
      pHandle->api.metaReaderFn.clearReader(&mr);
      return terrno;
    }

    pHandle->api.metaReaderFn.readerReleaseLock(&mr);

    val.pName = mr.me.name;
    val.pTags = (STag*)mr.me.ctbEntry.pTags;

    freeReader = true;
  } else {
    pCache->metaFetch += 1;

    h = taosLRUCacheLookup(pCache->pTableMetaEntryCache, &pBlock->info.id.uid, sizeof(pBlock->info.id.uid));
    if (h == NULL) {
      pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, 0, &pHandle->api.metaFn);
      code = pHandle->api.metaReaderFn.getEntryGetUidCache(&mr, pBlock->info.id.uid);
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
        pHandle->api.metaReaderFn.clearReader(&mr);
        return terrno;
      }

      pHandle->api.metaReaderFn.readerReleaseLock(&mr);

      STableCachedVal* pVal = createTableCacheVal(&mr);

      val = *pVal;
      freeReader = true;

      int32_t ret = taosLRUCacheInsert(pCache->pTableMetaEntryCache, &pBlock->info.id.uid, sizeof(uint64_t), pVal,
                                       sizeof(STableCachedVal), freeCachedMetaItem, NULL, TAOS_LRU_PRIORITY_LOW, NULL);
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
      int32_t fType = pExpr1->pExpr->_function.functionType;
      if (fType == FUNCTION_TYPE_TBNAME) {
        setTbNameColData(pBlock, pColInfoData, functionId, val.pName);
      } else if (fType == FUNCTION_TYPE_VGID) {
        setVgIdColData(pBlock, pColInfoData, functionId, pTask->id.vgId);
      } else if (fType == FUNCTION_TYPE_VGVER) {
        setVgVerColData(pBlock, pColInfoData, functionId, pBlock->info.version);
      }
    } else {  // these are tags
      STagVal tagVal = {0};
      tagVal.cid = pExpr1->base.pParam[0].pCol->colId;
      const char* p = pHandle->api.metaFn.extractTagVal(val.pTags, pColInfoData->info.type, &tagVal);

      char* data = NULL;
      if (pColInfoData->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
        data = tTagValToData((const STagVal*)p, false);
      } else {
        data = (char*)p;
      }

      bool isNullVal = (data == NULL) || (pColInfoData->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data));
      if (isNullVal) {
        colDataSetNNULL(pColInfoData, 0, pBlock->info.rows);
      } else if (pColInfoData->info.type != TSDB_DATA_TYPE_JSON) {
        code = colDataSetNItems(pColInfoData, 0, data, pBlock->info.rows, false);
        if (IS_VAR_DATA_TYPE(((const STagVal*)p)->type)) {
          taosMemoryFree(data);
        }
        if (code) {
          if (freeReader) {
            pHandle->api.metaReaderFn.clearReader(&mr);
          }
          return code;
        }
      } else {  // todo opt for json tag
        for (int32_t i = 0; i < pBlock->info.rows; ++i) {
          colDataSetVal(pColInfoData, i, data, false);
        }
      }
    }
  }

  // restore the rows
  pBlock->info.rows = backupRows;
  if (freeReader) {
    pHandle->api.metaReaderFn.clearReader(&mr);
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
  colDataSetVal(&infoData, 0, buf, false);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    fpSet.process(&srcParam, 1, &param);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

  colDataDestroy(&infoData);
}

void setVgIdColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, int32_t vgId) {
  struct SScalarFuncExecFuncs fpSet = {0};
  fmGetScalarFuncExecFuncs(functionId, &fpSet);

  SColumnInfoData infoData = createColumnInfoData(pColInfoData->info.type, pColInfoData->info.bytes, 1);

  colInfoDataEnsureCapacity(&infoData, 1, false);
  colDataSetVal(&infoData, 0, (const char*)&vgId, false);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    fpSet.process(&srcParam, 1, &param);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

  colDataDestroy(&infoData);
}

void setVgVerColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, int64_t vgVer) {
  struct SScalarFuncExecFuncs fpSet = {0};
  fmGetScalarFuncExecFuncs(functionId, &fpSet);

  SColumnInfoData infoData = createColumnInfoData(pColInfoData->info.type, pColInfoData->info.bytes, 1);

  colInfoDataEnsureCapacity(&infoData, 1, false);
  colDataSetVal(&infoData, 0, (const char*)&vgVer, false);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    fpSet.process(&srcParam, 1, &param);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

  colDataDestroy(&infoData);
}

static void initNextGroupScan(STableScanInfo* pInfo, STableKeyInfo** pKeyInfo, int32_t* size) {
  tableListGetGroupList(pInfo->base.pTableListInfo, pInfo->currentGroupId, pKeyInfo, size);

  pInfo->tableStartIndex = TARRAY_ELEM_IDX(pInfo->base.pTableListInfo->pTableList, *pKeyInfo);

  pInfo->tableEndIndex = (pInfo->tableStartIndex + (*size) - 1);

  pInfo->pResBlock->info.blankFill = false;

  if (!pInfo->needCountEmptyTable) {
    pInfo->countState = TABLE_COUNT_STATE_END;
  } else {
    pInfo->countState = TABLE_COUNT_STATE_SCAN;
  }
}

void markGroupProcessed(STableScanInfo* pInfo, uint64_t groupId) {
  if (pInfo->countState ==  TABLE_COUNT_STATE_END) {
    return;
  }
  if (pInfo->base.pTableListInfo->oneTableForEachGroup || pInfo->base.pTableListInfo->groupOffset) {
    pInfo->countState = TABLE_COUNT_STATE_PROCESSED;
  } else {
    taosHashRemove(pInfo->base.pTableListInfo->remainGroups, &groupId, sizeof(groupId));
  }
}

static SSDataBlock* getOneRowResultBlock(SExecTaskInfo* pTaskInfo, STableScanBase* pBase, SSDataBlock* pBlock,
                                         const STableKeyInfo* tbInfo) {
  blockDataEmpty(pBlock);
  pBlock->info.rows = 1;
  pBlock->info.id.uid = tbInfo->uid;
  pBlock->info.id.groupId = tbInfo->groupId;
  pBlock->info.blankFill = true;

  // only one row: set all col data to null & hasNull
  int32_t col_num = blockDataGetNumOfCols(pBlock);
  for (int32_t i = 0; i < col_num; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    colDataSetNULL(pColInfoData, 0);
  }

  // set tag/tbname
  doSetTagColumnData(pBase, pBlock, pTaskInfo, 1);
  return pBlock;
}

static SSDataBlock* getBlockForEmptyTable(SOperatorInfo* pOperator, const STableKeyInfo* tbInfo) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SSDataBlock*    pBlock =
      getOneRowResultBlock(pOperator->pTaskInfo, &pTableScanInfo->base, pTableScanInfo->pResBlock, tbInfo);

  pOperator->resultInfo.totalRows++;
  return pBlock;
}

static SSDataBlock* doTableScanImpl(SOperatorInfo* pOperator) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;

  SSDataBlock*    pBlock = pTableScanInfo->pResBlock;
  bool            hasNext = false;
  int32_t         code = TSDB_CODE_SUCCESS;

  int64_t st = taosGetTimestampUs();

  while (true) {
    code = pAPI->tsdReader.tsdNextDataBlock(pTableScanInfo->base.dataReader, &hasNext);
    if (code) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->base.dataReader);
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (!hasNext) {
      break;
    }

    if (isTaskKilled(pTaskInfo)) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->base.dataReader);
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (pOperator->status == OP_EXEC_DONE) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->base.dataReader);
      break;
    }

    // process this data block based on the probabilities
    bool processThisBlock = processBlockWithProbability(&pTableScanInfo->sample);
    if (!processThisBlock) {
      continue;
    }

    if (pBlock->info.id.uid) {
      pBlock->info.id.groupId = tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, pBlock->info.id.uid);
    }

    uint32_t status = 0;
    code = loadDataBlock(pOperator, &pTableScanInfo->base, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    if (status == FUNC_DATA_REQUIRED_ALL_FILTEROUT) {
      break;
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
      continue;
    }

    pOperator->resultInfo.totalRows = pTableScanInfo->base.readRecorder.totalRows;
    pTableScanInfo->base.readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;

    pOperator->cost.totalCost = pTableScanInfo->base.readRecorder.elapsedTime;
    pBlock->info.scanFlag = pTableScanInfo->base.scanFlag;
    return pBlock;
  }
  return NULL;
}

static SSDataBlock* doGroupedTableScan(SOperatorInfo* pOperator) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  // The read handle is not initialized yet, since no qualified tables exists
  if (pTableScanInfo->base.dataReader == NULL || pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // do the ascending order traverse in the first place.
  while (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
    SSDataBlock* p = doTableScanImpl(pOperator);
    if (p != NULL) {
      markGroupProcessed(pTableScanInfo, p->info.id.groupId);
      return p;
    }

    pTableScanInfo->scanTimes += 1;
    taosHashClear(pTableScanInfo->pIgnoreTables);

    if (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
      setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
      pTableScanInfo->base.scanFlag = MAIN_SCAN;
      pTableScanInfo->base.dataBlockLoadFlag = FUNC_DATA_REQUIRED_DATA_LOAD;
      qDebug("start to repeat ascending order scan data blocks due to query func required, %s", GET_TASKID(pTaskInfo));

      // do prepare for the next round table scan operation
      pAPI->tsdReader.tsdReaderResetStatus(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
    }
  }

  int32_t total = pTableScanInfo->scanInfo.numOfAsc + pTableScanInfo->scanInfo.numOfDesc;
  if (pTableScanInfo->scanTimes < total) {
    if (pTableScanInfo->base.cond.order == TSDB_ORDER_ASC) {
      prepareForDescendingScan(&pTableScanInfo->base, pOperator->exprSupp.pCtx, 0);
      pAPI->tsdReader.tsdReaderResetStatus(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
      qDebug("%s start to descending order scan data blocks due to query func required", GET_TASKID(pTaskInfo));
    }

    while (pTableScanInfo->scanTimes < total) {
      SSDataBlock* p = doTableScanImpl(pOperator);
      if (p != NULL) {
        markGroupProcessed(pTableScanInfo, p->info.id.groupId);
        return p;
      }

      pTableScanInfo->scanTimes += 1;
      taosHashClear(pTableScanInfo->pIgnoreTables);

      if (pTableScanInfo->scanTimes < total) {
        setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
        pTableScanInfo->base.scanFlag = MAIN_SCAN;

        qDebug("%s start to repeat descending order scan data blocks", GET_TASKID(pTaskInfo));
        pAPI->tsdReader.tsdReaderResetStatus(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
      }
    }
  }

  if (pTableScanInfo->countState < TABLE_COUNT_STATE_END) {
    STableListInfo* pTableListInfo = pTableScanInfo->base.pTableListInfo;
    if (pTableListInfo->oneTableForEachGroup || pTableListInfo->groupOffset) { // group by tbname, group by tag + sort
      if (pTableScanInfo->countState < TABLE_COUNT_STATE_PROCESSED) {
        pTableScanInfo->countState = TABLE_COUNT_STATE_PROCESSED;
        STableKeyInfo* pStart =
            (STableKeyInfo*)tableListGetInfo(pTableScanInfo->base.pTableListInfo, pTableScanInfo->tableStartIndex);
        return getBlockForEmptyTable(pOperator, pStart);
      }
    } else { // group by tag + no sort
      int32_t numOfTables = tableListGetSize(pTableListInfo);
      if (pTableScanInfo->tableEndIndex + 1 >= numOfTables) {
        // get empty group, mark processed & rm from hash
        void* pIte = taosHashIterate(pTableListInfo->remainGroups, NULL);
        if (pIte != NULL) {
          size_t        keySize = 0;
          uint64_t*     pGroupId = taosHashGetKey(pIte, &keySize);
          STableKeyInfo info = {.uid = *(uint64_t*)pIte, .groupId = *pGroupId};
          taosHashCancelIterate(pTableListInfo->remainGroups, pIte);
          markGroupProcessed(pTableScanInfo, *pGroupId);
          return getBlockForEmptyTable(pOperator, &info);
        }
      }
    }
    pTableScanInfo->countState = TABLE_COUNT_STATE_END;
  }

  return NULL;
}

static int32_t createTableListInfoFromParam(SOperatorInfo* pOperator) {
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;  
  int32_t code = 0;
  STableListInfo* pListInfo = pInfo->base.pTableListInfo;
  STableScanOperatorParam* pParam = (STableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  int32_t num = taosArrayGetSize(pParam->pUidList);
  if (num <= 0) {
    qError("empty table scan uid list");
    return TSDB_CODE_INVALID_PARA;
  }
  
  qError("vgId:%d add total %d dynamic tables to scan, tableSeq:%d, exist num:%" PRId64 ", operator status:%d", 
      pTaskInfo->id.vgId, num, pParam->tableSeq, (int64_t)taosArrayGetSize(pListInfo->pTableList), pOperator->status);

  if (pParam->tableSeq) {
    pListInfo->oneTableForEachGroup = true;
    if (taosArrayGetSize(pListInfo->pTableList) > 0) {
      taosHashClear(pListInfo->map);
      taosArrayClear(pListInfo->pTableList);
      pOperator->status = OP_EXEC_DONE;
    }
  } else {
    pListInfo->oneTableForEachGroup = false;
    pListInfo->numOfOuputGroups = 1;
  }

  STableKeyInfo info = {.groupId = 0};  
  int32_t tableIdx = 0;
  for (int32_t i = 0; i < num; ++i) {
    uint64_t* pUid = taosArrayGet(pParam->pUidList, i);

    if (taosHashPut(pListInfo->map, pUid, sizeof(uint64_t), &tableIdx, sizeof(int32_t))) {
      if (TSDB_CODE_DUP_KEY == terrno) {
        continue;
      }
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    info.uid = *pUid;
    void* p = taosArrayPush(pListInfo->pTableList, &info);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    tableIdx++;
    qError("add dynamic table scan uid:%" PRIu64 ", %s", info.uid, GET_TASKID(pTaskInfo));
  }
  
  return code;
}

static SSDataBlock* startNextGroupScan(SOperatorInfo* pOperator) {
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  int32_t numOfTables = tableListGetSize(pInfo->base.pTableListInfo);
  if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pInfo->base.pTableListInfo)) {
    setOperatorCompleted(pOperator);
    if (pOperator->dynamicTask) {
      taosArrayClear(pInfo->base.pTableListInfo->pTableList);
      taosHashClear(pInfo->base.pTableListInfo->map);
    }
    return NULL;
  }

  // reset value for the next group data output
  pOperator->status = OP_OPENED;
  resetLimitInfoForNextGroup(&pInfo->base.limitInfo);
  
  int32_t        num = 0;
  STableKeyInfo* pList = NULL;
  initNextGroupScan(pInfo, &pList, &num);

  pAPI->tsdReader.tsdSetQueryTableList(pInfo->base.dataReader, pList, num);
  pAPI->tsdReader.tsdReaderResetStatus(pInfo->base.dataReader, &pInfo->base.cond);
  pInfo->scanTimes = 0;
  
  SSDataBlock* result = doGroupedTableScan(pOperator);
  if (result != NULL) {
    if (pOperator->dynamicTask) {
      result->info.id.groupId = result->info.id.uid;
    }
    return result;
  }
  
  return NULL;
}

static SSDataBlock* groupSeqTableScan(SOperatorInfo* pOperator) {
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  int32_t        num = 0;
  STableKeyInfo* pList = NULL;

  if (pInfo->currentGroupId == -1) {
    if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pInfo->base.pTableListInfo)) {
      setOperatorCompleted(pOperator);
      return NULL;
    }

    initNextGroupScan(pInfo, &pList, &num);
    ASSERT(pInfo->base.dataReader == NULL);
  
    int32_t code = pAPI->tsdReader.tsdReaderOpen(pInfo->base.readHandle.vnode, &pInfo->base.cond, pList, num, pInfo->pResBlock,
                                  (void**)&pInfo->base.dataReader, GET_TASKID(pTaskInfo), &pInfo->pIgnoreTables);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (pInfo->filesetDelimited) {
      pAPI->tsdReader.tsdSetFilesetDelimited(pInfo->base.dataReader);
    }
    if (pInfo->pResBlock->info.capacity > pOperator->resultInfo.capacity) {
      pOperator->resultInfo.capacity = pInfo->pResBlock->info.capacity;
    }
  }

  SSDataBlock* result = doGroupedTableScan(pOperator);
  if (result != NULL) {
    if (pOperator->dynamicTask) {
      result->info.id.groupId = result->info.id.uid;
    }
    return result;
  }

  while (true) {
    result = startNextGroupScan(pOperator);
    if (result || pOperator->status == OP_EXEC_DONE) {
      return result;
    }
  }

  return result;
}

static SSDataBlock* doTableScan(SOperatorInfo* pOperator) {
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;

  if (pOperator->pOperatorGetParam) {
    pOperator->dynamicTask = true;
    int32_t code = createTableListInfoFromParam(pOperator);
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
    if (code != TSDB_CODE_SUCCESS) {
      pTaskInfo->code = code;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (pOperator->status == OP_EXEC_DONE) {
      pInfo->currentGroupId = -1;
      pOperator->status = OP_OPENED;
      SSDataBlock* result = NULL;
      while (true) {
        result = startNextGroupScan(pOperator);
        if (result || pOperator->status == OP_EXEC_DONE) {
          return result;
        }
      }
    }
  }

  // scan table one by one sequentially
  if (pInfo->scanMode == TABLE_SCAN__TABLE_ORDER) {
    int32_t       numOfTables = 0;  // tableListGetSize(pTaskInfo->pTableListInfo);
    STableKeyInfo tInfo = {0};
    pInfo->countState = TABLE_COUNT_STATE_END;

    while (1) {
      SSDataBlock* result = doGroupedTableScan(pOperator);
      if (result || (pOperator->status == OP_EXEC_DONE) || isTaskKilled(pTaskInfo)) {
        return result;
      }

      // if no data, switch to next table and continue scan
      pInfo->currentTable++;

      taosRLockLatch(&pTaskInfo->lock);
      numOfTables = tableListGetSize(pInfo->base.pTableListInfo);

      if (pInfo->currentTable >= numOfTables) {
        qDebug("all table checked in table list, total:%d, return NULL, %s", numOfTables, GET_TASKID(pTaskInfo));
        taosRUnLockLatch(&pTaskInfo->lock);
        return NULL;
      }

      tInfo = *(STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->currentTable);
      taosRUnLockLatch(&pTaskInfo->lock);

      pAPI->tsdReader.tsdSetQueryTableList(pInfo->base.dataReader, &tInfo, 1);
      qDebug("set uid:%" PRIu64 " into scanner, total tables:%d, index:%d/%d %s", tInfo.uid, numOfTables,
             pInfo->currentTable, numOfTables, GET_TASKID(pTaskInfo));

      pAPI->tsdReader.tsdReaderResetStatus(pInfo->base.dataReader, &pInfo->base.cond);
      pInfo->scanTimes = 0;
    }
  } else {  // scan table group by group sequentially
    return groupSeqTableScan(pOperator);
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

static void destroyTableScanBase(STableScanBase* pBase, TsdReader* pAPI) {
  cleanupQueryTableDataCond(&pBase->cond);

  pAPI->tsdReaderClose(pBase->dataReader);
  pBase->dataReader = NULL;

  if (pBase->matchInfo.pList != NULL) {
    taosArrayDestroy(pBase->matchInfo.pList);
  }

  tableListDestroy(pBase->pTableListInfo);
  taosLRUCacheCleanup(pBase->metaCache.pTableMetaEntryCache);
  cleanupExprSupp(&pBase->pseudoSup);
}

static void destroyTableScanOperatorInfo(void* param) {
  STableScanInfo* pTableScanInfo = (STableScanInfo*)param;
  blockDataDestroy(pTableScanInfo->pResBlock);
  taosHashCleanup(pTableScanInfo->pIgnoreTables);
  destroyTableScanBase(&pTableScanInfo->base, &pTableScanInfo->base.readerAPI);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle,
                                           STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo) {
  int32_t         code = 0;
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  SScanPhysiNode*     pScanNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  code =
      extractColMatchInfo(pScanNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->base.matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initLimitInfo(pScanNode->node.pLimit, pScanNode->node.pSlimit, &pInfo->base.limitInfo);
  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode, readHandle);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pScanNode->pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->base.pseudoSup;
    pSup->pExprInfo = createExprInfo(pScanNode->pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};
  pInfo->base.scanFlag = (pInfo->scanInfo.numOfAsc > 1) ? PRE_SCAN : MAIN_SCAN;

  pInfo->base.pdInfo.interval = extractIntervalInfo(pTableScanNode);
  pInfo->base.readHandle = *readHandle;
  pInfo->base.dataBlockLoadFlag = pTableScanNode->dataRequired;

  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();

  pInfo->base.readerAPI = pTaskInfo->storageAPI.tsdReader;
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  pInfo->pResBlock = createDataBlockFromDescNode(pDescNode);

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }
  
  pInfo->currentGroupId = -1;

  pInfo->tableEndIndex = -1;
  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;
  pInfo->hasGroupByTag = pTableScanNode->pGroupTags ? true : false;

  setOperatorInfo(pOperator, "TableScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;

  pInfo->needCountEmptyTable = tsCountAlwaysReturnValue && pTableScanNode->needCountEmptyTable;

  pInfo->base.pTableListInfo = pTableListInfo;
  pInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
  if (pInfo->base.metaCache.pTableMetaEntryCache == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->filesetDelimited = pTableScanNode->filesetDelimited;

  taosLRUCacheSetStrictCapacity(pInfo->base.metaCache.pTableMetaEntryCache, false);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableScan, NULL, destroyTableScanOperatorInfo,
                                         optrDefaultBufFn, getTableScannerExecInfo, optrDefaultGetNextExtFn, NULL);

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
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableScanImpl, NULL, NULL, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  return pOperator;
}

FORCE_INLINE void doClearBufferedBlocks(SStreamScanInfo* pInfo) {
  qDebug("clear buff blocks:%d", (int32_t)taosArrayGetSize(pInfo->pBlockLists));
  taosArrayClear(pInfo->pBlockLists);
  pInfo->validBlockIndex = 0;
}

static bool isSessionWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SESSION;
}

static bool isStateWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_STATE ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_EVENT;
}

static bool isIntervalWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_SEMI_INTERVAL ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_MID_INTERVAL ||
         pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_FINAL_INTERVAL;
}

static bool isSignleIntervalWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_INTERVAL;
}

static bool isSlidingWindow(SStreamScanInfo* pInfo) {
  return isIntervalWindow(pInfo) && pInfo->interval.interval != pInfo->interval.sliding;
}

static bool isCountSlidingWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.pStreamAggSup && (pInfo->windowSup.pStreamAggSup->windowCount != pInfo->windowSup.pStreamAggSup->windowSliding);
}

static bool isCountWindow(SStreamScanInfo* pInfo) {
  return pInfo->windowSup.parentType == QUERY_NODE_PHYSICAL_PLAN_STREAM_COUNT;
}

static void setGroupId(SStreamScanInfo* pInfo, SSDataBlock* pBlock, int32_t groupColIndex, int32_t rowIndex) {
  SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, groupColIndex);
  uint64_t*        groupCol = (uint64_t*)pColInfo->pData;
  ASSERT(rowIndex < pBlock->info.rows);
  pInfo->groupId = groupCol[rowIndex];
}

void resetTableScanInfo(STableScanInfo* pTableScanInfo, STimeWindow* pWin, uint64_t ver) {
  pTableScanInfo->base.cond.twindows = *pWin;
  pTableScanInfo->base.cond.startVersion = 0;
  pTableScanInfo->base.cond.endVersion = ver;
  pTableScanInfo->scanTimes = 0;
  pTableScanInfo->currentGroupId = -1;
  pTableScanInfo->tableEndIndex = -1;
  pTableScanInfo->base.readerAPI.tsdReaderClose(pTableScanInfo->base.dataReader);
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
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SSDataBlock* pBlock = pTableScanInfo->pResBlock;
  STsdbReader* pReader = NULL;
  int32_t      code = pAPI->tsdReader.tsdReaderOpen(pTableScanInfo->base.readHandle.vnode, &cond, &tblInfo, 1, pBlock,
                                     (void**)&pReader, GET_TASKID(pTaskInfo), NULL);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    T_LONG_JMP(pTaskInfo->env, code);
    return NULL;
  }

  bool hasNext = false;
  code = pAPI->tsdReader.tsdNextDataBlock(pReader, &hasNext);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    T_LONG_JMP(pTaskInfo->env, code);
    return NULL;
  }

  if (hasNext) {
    /*SSDataBlock* p = */ pAPI->tsdReader.tsdReaderRetrieveDataBlock(pReader, NULL);
    doSetTagColumnData(&pTableScanInfo->base, pBlock, pTaskInfo, pBlock->info.rows);
    pBlock->info.id.groupId = tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, pBlock->info.id.uid);
  }

  pAPI->tsdReader.tsdReaderClose(pReader);
  qDebug("retrieve prev rows:%" PRId64 ", skey:%" PRId64 ", ekey:%" PRId64 " uid:%" PRIu64 ", max ver:%" PRId64
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
  STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
  return tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, uid);
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
  if (isSlidingWindow(pInfo) || isCountSlidingWindow(pInfo)) {
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

  STableScanInfo* pTScanInfo = pInfo->pTableScanOp->info;
  qDebug("prepare range scan start:%" PRId64 ",end:%" PRId64 ",maxVer:%" PRIu64, win.skey, win.ekey, pInfo->pUpdateInfo->maxDataVersion);
  resetTableScanInfo(pInfo->pTableScanOp->info, &win, pInfo->pUpdateInfo->maxDataVersion);
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
  qDebug("do stream range scan. windows index:%d", *pRowIndex);
  bool prepareRes = true;

  while (1) {
    SSDataBlock* pResult = NULL;
    pResult = doTableScan(pInfo->pTableScanOp);
    if (!pResult) {
      prepareRes = prepareRangeScan(pInfo, pSDB, pRowIndex);
      // scan next window data
      pResult = doTableScan(pInfo->pTableScanOp);
    }
    if (!pResult) {
      if (prepareRes) {
        continue;
      }
      blockDataCleanup(pSDB);
      *pRowIndex = 0;
      pInfo->updateWin = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
      STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
      pTableScanInfo->base.readerAPI.tsdReaderClose(pTableScanInfo->base.dataReader);
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
            char*            pSrcData = NULL;
            if (!isNull) pSrcData = colDataGetData(pSrcCol, i);
            colDataSetVal(pDestCol, pResult->info.rows, pSrcData, isNull);
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

static int32_t getPreSessionWindow(SStreamAggSupporter* pAggSup, TSKEY startTs, TSKEY endTs, uint64_t groupId,
                                   SSessionKey* pKey) {
  pKey->win.skey = startTs;
  pKey->win.ekey = endTs;
  pKey->groupId = groupId;

  void* pCur = pAggSup->stateStore.streamStateSessionSeekKeyCurrentPrev(pAggSup->pState, pKey);
  int32_t          code = pAggSup->stateStore.streamStateSessionGetKVByCur(pCur, pKey, NULL, 0);
  if (code != TSDB_CODE_SUCCESS) {
    SET_SESSION_WIN_KEY_INVALID(pKey);
  }

  taosMemoryFree(pCur);
  return code;
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
  int64_t          ver = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t groupId = getGroupIdByData(pInfo, uidCol[i], startData[i], ver);
    // gap must be 0.
    SSessionKey startWin = {0};
    getCurSessionWindow(pInfo->windowSup.pStreamAggSup, startData[i], startData[i], groupId, &startWin);
    if (IS_INVALID_SESSION_WIN_KEY(startWin)) {
      // window has been closed.
      continue;
    }
    SSessionKey endWin = {0};
    getCurSessionWindow(pInfo->windowSup.pStreamAggSup, endData[i], endData[i], groupId, &endWin);
    if (IS_INVALID_SESSION_WIN_KEY(endWin)) {
      getPreSessionWindow(pInfo->windowSup.pStreamAggSup, endData[i], endData[i], groupId, &endWin);
    }
    if (IS_INVALID_SESSION_WIN_KEY(startWin)) {
      // window has been closed.
      qError("generate session scan range failed. rang start:%" PRIx64 ", end:%" PRIx64, startData[i], endData[i]);
      continue;
    }
    colDataSetVal(pDestStartCol, i, (const char*)&startWin.win.skey, false);
    colDataSetVal(pDestEndCol, i, (const char*)&endWin.win.ekey, false);

    colDataSetNULL(pDestUidCol, i);
    colDataSetVal(pDestGpCol, i, (const char*)&groupId, false);
    colDataSetNULL(pDestCalStartTsCol, i);
    colDataSetNULL(pDestCalEndTsCol, i);
    pDestBlock->info.rows++;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateCountScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock, EStreamType mode) {
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
  int64_t          ver = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t groupId = getGroupIdByData(pInfo, uidCol[i], startData[i], ver);
    SSessionKey startWin = {.win.skey = startData[i], .win.ekey = endData[i], .groupId = groupId};
    SSessionKey range = {0};
    getCountWinRange(pInfo->windowSup.pStreamAggSup, &startWin, mode, &range);
    colDataSetVal(pDestStartCol, i, (const char*)&range.win.skey, false);
    colDataSetVal(pDestEndCol, i, (const char*)&range.win.ekey, false);

    colDataSetNULL(pDestUidCol, i);
    colDataSetVal(pDestGpCol, i, (const char*)&groupId, false);
    colDataSetVal(pDestCalStartTsCol, i, (const char*)&range.win.skey, false);
    colDataSetVal(pDestCalEndTsCol, i, (const char*)&range.win.ekey, false);
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
  SExecTaskInfo*   pTaskInfo = pInfo->pStreamScanOp->pTaskInfo;
  SColumnInfoData* pSrcStartTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcEndTsCol = (SColumnInfoData*)taosArrayGet(pSrcBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pSrcUidCol = taosArrayGet(pSrcBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pSrcGpCol = taosArrayGet(pSrcBlock->pDataBlock, GROUPID_COLUMN_INDEX);

  uint64_t* srcUidData = (uint64_t*)pSrcUidCol->pData;
  ASSERT(pSrcStartTsCol->info.type == TSDB_DATA_TYPE_TIMESTAMP);
  TSKEY*  srcStartTsCol = (TSKEY*)pSrcStartTsCol->pData;
  TSKEY*  srcEndTsCol = (TSKEY*)pSrcEndTsCol->pData;
  int64_t ver = pSrcBlock->info.version - 1;

  if (pInfo->partitionSup.needCalc && srcStartTsCol[0] != srcEndTsCol[0]) {
    uint64_t     srcUid = srcUidData[0];
    TSKEY        startTs = srcStartTsCol[0];
    TSKEY        endTs = srcEndTsCol[0];
    SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, srcUid, startTs, endTs, ver);
    printDataBlock(pPreRes, "pre res", GET_TASKID(pTaskInfo));
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
    printDataBlock(pSrcBlock, "new delete", GET_TASKID(pTaskInfo));
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
      groupId = getGroupIdByData(pInfo, srcUid, srcStartTsCol[i], ver);
    }
    TSKEY calStartTs = srcStartTsCol[i];
    colDataSetVal(pCalStartTsCol, pDestBlock->info.rows, (const char*)(&calStartTs), false);
    STimeWindow win = getSlidingWindow(srcStartTsCol, srcEndTsCol, srcGp, &pInfo->interval, &pSrcBlock->info, &i,
                                       pInfo->partitionSup.needCalc);
    TSKEY       calEndTs = srcStartTsCol[i - 1];
    colDataSetVal(pCalEndTsCol, pDestBlock->info.rows, (const char*)(&calEndTs), false);
    colDataSetVal(pDeUidCol, pDestBlock->info.rows, (const char*)(&srcUid), false);
    colDataSetVal(pStartTsCol, pDestBlock->info.rows, (const char*)(&win.skey), false);
    colDataSetVal(pEndTsCol, pDestBlock->info.rows, (const char*)(&win.ekey), false);
    colDataSetVal(pGpCol, pDestBlock->info.rows, (const char*)(&groupId), false);
    pDestBlock->info.rows++;
  }
  return TSDB_CODE_SUCCESS;
}

static void calBlockTbName(SStreamScanInfo* pInfo, SSDataBlock* pBlock) {
  SExprSupp*    pTbNameCalSup = &pInfo->tbnameCalSup;
  blockDataCleanup(pInfo->pCreateTbRes);
  if (pInfo->tbnameCalSup.numOfExprs == 0 && pInfo->tagCalSup.numOfExprs == 0) {
    pBlock->info.parTbName[0] = 0;
  } else {
    appendCreateTableRow(pInfo->pStreamScanOp->pTaskInfo->streamInfo.pState, &pInfo->tbnameCalSup, &pInfo->tagCalSup,
                         pBlock->info.id.groupId, pBlock, 0, pInfo->pCreateTbRes, &pInfo->stateStore);
  }
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
  int64_t ver = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t srcUid = srcUidData[i];
    uint64_t groupId = srcGp[i];
    char     tbname[VARSTR_HEADER_SIZE + TSDB_TABLE_NAME_LEN] = {0};
    if (groupId == 0) {
      groupId = getGroupIdByData(pInfo, srcUid, srcStartTsCol[i], ver);
    }
    if (pInfo->tbnameCalSup.pExprInfo) {
      void* parTbname = NULL;
      code = pInfo->stateStore.streamStateGetParName(pInfo->pStreamScanOp->pTaskInfo->streamInfo.pState, groupId, &parTbname);
      if (code != TSDB_CODE_SUCCESS) {
        SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, srcUid, srcStartTsCol[i], srcStartTsCol[i], ver);
        printDataBlock(pPreRes, "pre res", GET_TASKID(pInfo->pStreamScanOp->pTaskInfo));
        calBlockTbName(pInfo, pPreRes);
        memcpy(varDataVal(tbname), pPreRes->info.parTbName, strlen(pPreRes->info.parTbName));
      } else {
        memcpy(varDataVal(tbname), parTbname, TSDB_TABLE_NAME_LEN);
      }
      varDataSetLen(tbname, strlen(varDataVal(tbname)));
      pInfo->stateStore.streamStateFreeVal(parTbname);
    }
    appendOneRowToStreamSpecialBlock(pDestBlock, srcStartTsCol + i, srcEndTsCol + i, srcUidData + i, &groupId,
                                     tbname[0] == 0 ? NULL : tbname);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t generateScanRange(SStreamScanInfo* pInfo, SSDataBlock* pSrcBlock, SSDataBlock* pDestBlock, EStreamType type) {
  int32_t code = TSDB_CODE_SUCCESS;
  if (isIntervalWindow(pInfo)) {
    code = generateIntervalScanRange(pInfo, pSrcBlock, pDestBlock);
  } else if (isSessionWindow(pInfo) || isStateWindow(pInfo)) {
    code = generateSessionScanRange(pInfo, pSrcBlock, pDestBlock);
  } else if (isCountWindow(pInfo)) {
    code = generateCountScanRange(pInfo, pSrcBlock, pDestBlock, type);
  } else {
    code = generateDeleteResultBlock(pInfo, pSrcBlock, pDestBlock);
  }
  pDestBlock->info.type = STREAM_CLEAR;
  pDestBlock->info.version = pSrcBlock->info.version;
  pDestBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pDestBlock, 0);
  return code;
}

void appendOneRowToStreamSpecialBlock(SSDataBlock* pBlock, TSKEY* pStartTs, TSKEY* pEndTs, uint64_t* pUid,
                                      uint64_t* pGp, void* pTbName) {
  appendAllColumnToStreamSpecialBlock(pBlock, pStartTs, pEndTs, pStartTs, pEndTs, pUid, pGp, pTbName);
}

void appendAllColumnToStreamSpecialBlock(SSDataBlock* pBlock, TSKEY* pStartTs, TSKEY* pEndTs, TSKEY* pCalStartTs,
                                         TSKEY* pCalEndTs, uint64_t* pUid, uint64_t* pGp, void* pTbName) {
  SColumnInfoData* pStartTsCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  SColumnInfoData* pEndTsCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  SColumnInfoData* pUidCol = taosArrayGet(pBlock->pDataBlock, UID_COLUMN_INDEX);
  SColumnInfoData* pGpCol = taosArrayGet(pBlock->pDataBlock, GROUPID_COLUMN_INDEX);
  SColumnInfoData* pCalStartCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX);
  SColumnInfoData* pCalEndCol = taosArrayGet(pBlock->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX);
  SColumnInfoData* pTableCol = taosArrayGet(pBlock->pDataBlock, TABLE_NAME_COLUMN_INDEX);
  colDataSetVal(pStartTsCol, pBlock->info.rows, (const char*)pStartTs, false);
  colDataSetVal(pEndTsCol, pBlock->info.rows, (const char*)pEndTs, false);
  colDataSetVal(pUidCol, pBlock->info.rows, (const char*)pUid, false);
  colDataSetVal(pGpCol, pBlock->info.rows, (const char*)pGp, false);
  colDataSetVal(pCalStartCol, pBlock->info.rows, (const char*)pCalStartTs, false);
  colDataSetVal(pCalEndCol, pBlock->info.rows, (const char*)pCalEndTs, false);
  colDataSetVal(pTableCol, pBlock->info.rows, (const char*)pTbName, pTbName == NULL);
  pBlock->info.rows++;
}

bool checkExpiredData(SStateStore* pAPI, SUpdateInfo* pUpdateInfo, STimeWindowAggSupp* pTwSup, uint64_t tableId, TSKEY ts) {
  bool isExpired = false;
  bool isInc = pAPI->isIncrementalTimeStamp(pUpdateInfo, tableId, ts);
  if (!isInc) {
    isExpired = isOverdue(ts, pTwSup);
  }
  return isExpired;
}

static void checkUpdateData(SStreamScanInfo* pInfo, bool invertible, SSDataBlock* pBlock, bool out) {
  if (out) {
    blockDataCleanup(pInfo->pUpdateDataRes);
    blockDataEnsureCapacity(pInfo->pUpdateDataRes, pBlock->info.rows * 2);
  }
  SColumnInfoData* pColDataInfo = taosArrayGet(pBlock->pDataBlock, pInfo->primaryTsIndex);
  ASSERT(pColDataInfo->info.type == TSDB_DATA_TYPE_TIMESTAMP);
  TSKEY* tsCol = (TSKEY*)pColDataInfo->pData;
  bool   tableInserted = pInfo->stateStore.updateInfoIsTableInserted(pInfo->pUpdateInfo, pBlock->info.id.uid);
  for (int32_t rowId = 0; rowId < pBlock->info.rows; rowId++) {
    SResultRowInfo dumyInfo;
    dumyInfo.cur.pageId = -1;
    bool        isClosed = false;
    STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
    bool        overDue = isOverdue(tsCol[rowId], &pInfo->twAggSup);
    if (pInfo->igExpired && overDue) {
      continue;
    }

    if (tableInserted && overDue) {
      win = getActiveTimeWindow(NULL, &dumyInfo, tsCol[rowId], &pInfo->interval, TSDB_ORDER_ASC);
      isClosed = isCloseWindow(&win, &pInfo->twAggSup);
    }
    // must check update info first.
    bool update = pInfo->stateStore.updateInfoIsUpdated(pInfo->pUpdateInfo, pBlock->info.id.uid, tsCol[rowId]);
    bool closedWin = isClosed && isSignleIntervalWindow(pInfo) &&
                     isDeletedStreamWindow(&win, pBlock->info.id.groupId, pInfo->pState, &pInfo->twAggSup, &pInfo->stateStore);
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

static void doBlockDataWindowFilter(SSDataBlock* pBlock, int32_t tsIndex, STimeWindow* pWindow, const char* id) {
  if (pWindow->skey != INT64_MIN || pWindow->ekey != INT64_MAX) {
    bool* p = taosMemoryCalloc(pBlock->info.rows, sizeof(bool));
    bool  hasUnqualified = false;

    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, tsIndex);

    if (pWindow->skey != INT64_MIN) {
      qDebug("%s filter for additional history window, skey:%" PRId64, id, pWindow->skey);

      ASSERT(pCol->pData != NULL);
      for (int32_t i = 0; i < pBlock->info.rows; ++i) {
        int64_t* ts = (int64_t*)colDataGetData(pCol, i);
        p[i] = (*ts >= pWindow->skey);

        if (!p[i]) {
          hasUnqualified = true;
        }
      }
    } else if (pWindow->ekey != INT64_MAX) {
      qDebug("%s filter for additional history window, ekey:%" PRId64, id, pWindow->ekey);
      for (int32_t i = 0; i < pBlock->info.rows; ++i) {
        int64_t* ts = (int64_t*)colDataGetData(pCol, i);
        p[i] = (*ts <= pWindow->ekey);

        if (!p[i]) {
          hasUnqualified = true;
        }
      }
    }

    if (hasUnqualified) {
      trimDataBlock(pBlock, pBlock->info.rows, p);
    }

    taosMemoryFree(p);
  }
}

// re-build the delete block, ONLY according to the split timestamp
static void rebuildDeleteBlockData(SSDataBlock* pBlock, STimeWindow* pWindow, const char* id) {
  int32_t numOfRows = pBlock->info.rows;
  bool*   p = taosMemoryCalloc(numOfRows, sizeof(bool));
  bool    hasUnqualified = false;
  int64_t skey = pWindow->skey;
  int64_t ekey = pWindow->ekey;

  SColumnInfoData* pSrcStartCol = taosArrayGet(pBlock->pDataBlock, START_TS_COLUMN_INDEX);
  uint64_t*        tsStartCol = (uint64_t*)pSrcStartCol->pData;
  SColumnInfoData* pSrcEndCol = taosArrayGet(pBlock->pDataBlock, END_TS_COLUMN_INDEX);
  uint64_t*        tsEndCol = (uint64_t*)pSrcEndCol->pData;

  if (pWindow->skey != INT64_MIN) {
    for (int32_t i = 0; i < numOfRows; i++) {
      if (tsStartCol[i] < skey) {
        tsStartCol[i] = skey;
      }

      if (tsEndCol[i] >= skey) {
        p[i] = true;
      } else {  // this row should be removed, since it is not in this query time window, which is [skey, INT64_MAX]
        hasUnqualified = true;
      }
    }
  } else if (pWindow->ekey != INT64_MAX) {
    for(int32_t i = 0; i < numOfRows; ++i) {
      if (tsEndCol[i] > ekey) {
        tsEndCol[i] = ekey;
      }

      if (tsStartCol[i] <= ekey) {
        p[i] = true;
      } else {
        hasUnqualified = true;
      }
    }
  }

  if (hasUnqualified) {
    trimDataBlock(pBlock, pBlock->info.rows, p);
    qDebug("%s re-build delete datablock, start key revised to:%"PRId64", rows:%"PRId64, id, skey, pBlock->info.rows);
  } else {
    qDebug("%s not update the delete block", id);
  }

  taosMemoryFree(p);
}

static int32_t setBlockIntoRes(SStreamScanInfo* pInfo, const SSDataBlock* pBlock, STimeWindow* pTimeWindow, bool filter) {
  SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
  SOperatorInfo*  pOperator = pInfo->pStreamScanOp;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  const char*     id = GET_TASKID(pTaskInfo);

  blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);

  pBlockInfo->rows = pBlock->info.rows;
  pBlockInfo->id.uid = pBlock->info.id.uid;
  pBlockInfo->type = STREAM_NORMAL;
  pBlockInfo->version = pBlock->info.version;

  STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
  pBlockInfo->id.groupId = tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, pBlock->info.id.uid);

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
      colDataSetNNULL(pDst, 0, pBlockInfo->rows);
    }
  }

  // currently only the tbname pseudo column
  if (pInfo->numOfPseudoExpr > 0) {
    int32_t code = addTagPseudoColumnData(&pInfo->readHandle, pInfo->pPseudoExpr, pInfo->numOfPseudoExpr, pInfo->pRes,
                                          pBlockInfo->rows, pTaskInfo, &pTableScanInfo->base.metaCache);
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

  // filter the block extracted from WAL files, according to the time window apply additional time window filter
  doBlockDataWindowFilter(pInfo->pRes, pInfo->primaryTsIndex, pTimeWindow, id);
  pInfo->pRes->info.dataLoad = 1;

  blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
  if (pInfo->pRes->info.rows == 0) {
    return 0;
  }

  calBlockTbName(pInfo, pInfo->pRes);
  return 0;
}

static SSDataBlock* doQueueScan(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SStreamScanInfo* pInfo = pOperator->info;
  const char*      id = GET_TASKID(pTaskInfo);

  qDebug("start to exec queue scan, %s", id);

  if (isTaskKilled(pTaskInfo)) {
    return NULL;
  }

  if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    SSDataBlock* pResult = doTableScan(pInfo->pTableScanOp);
    if (pResult && pResult->info.rows > 0) {
      tqOffsetResetToData(&pTaskInfo->streamInfo.currentOffset, pResult->info.id.uid, pResult->info.window.ekey);
      return pResult;
    }

    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    pAPI->tsdReader.tsdReaderClose(pTSInfo->base.dataReader);

    pTSInfo->base.dataReader = NULL;
    int64_t validVer = pTaskInfo->streamInfo.snapshotVer + 1;
    qDebug("queue scan tsdb over, switch to wal ver %" PRId64 "", validVer);
    if (pAPI->tqReaderFn.tqReaderSeek(pInfo->tqReader, validVer, pTaskInfo->id.str) < 0) {
      return NULL;
    }

    tqOffsetResetToLog(&pTaskInfo->streamInfo.currentOffset, validVer);
  }

  if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__LOG) {

    while (1) {
      bool hasResult = pAPI->tqReaderFn.tqReaderNextBlockInWal(pInfo->tqReader, id, pTaskInfo->streamInfo.sourceExcluded);

      SSDataBlock* pRes = pAPI->tqReaderFn.tqGetResultBlock(pInfo->tqReader);
      struct SWalReader* pWalReader = pAPI->tqReaderFn.tqReaderGetWalReader(pInfo->tqReader);

      // curVersion move to next
      tqOffsetResetToLog(&pTaskInfo->streamInfo.currentOffset, pWalReader->curVersion);

      // use ts to pass time when replay, because ts not used if type is log
      pTaskInfo->streamInfo.currentOffset.ts = pAPI->tqReaderFn.tqGetResultBlockTime(pInfo->tqReader);

      if (hasResult) {
        qDebug("doQueueScan get data from log %" PRId64 " rows, version:%" PRId64, pRes->info.rows,
               pTaskInfo->streamInfo.currentOffset.version);
        blockDataCleanup(pInfo->pRes);
        STimeWindow defaultWindow = {.skey = INT64_MIN, .ekey = INT64_MAX};
        setBlockIntoRes(pInfo, pRes, &defaultWindow, true);
        if (pInfo->pRes->info.rows > 0) {
          return pInfo->pRes;
        }
      } else {
        qDebug("doQueueScan get none from log, return, version:%" PRId64, pTaskInfo->streamInfo.currentOffset.version);
        return NULL;
      }
    }
  } else {
    qError("unexpected streamInfo prepare type: %d", pTaskInfo->streamInfo.currentOffset.type);
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

  int32_t j = 0;
  for (int32_t i = 0; i < rows; i++) {
    if (pInfo->readerFn.tqReaderIsQueriedTable(pReader, uidCol[i])) {
      colDataSetVal(pDstStartCol, j, (const char*)&startCol[i], false);
      colDataSetVal(pDstEndCol, j, (const char*)&endCol[i], false);
      colDataSetVal(pDstUidCol, j, (const char*)&uidCol[i], false);

      colDataSetNULL(taosArrayGet(pDst->pDataBlock, GROUPID_COLUMN_INDEX), j);
      colDataSetNULL(taosArrayGet(pDst->pDataBlock, CALCULATE_START_TS_COLUMN_INDEX), j);
      colDataSetNULL(taosArrayGet(pDst->pDataBlock, CALCULATE_END_TS_COLUMN_INDEX), j);
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
      colDataSetVal(pGpCol, i, (const char*)&groupId, false);
    }
  }
}

static void doCheckUpdate(SStreamScanInfo* pInfo, TSKEY endKey, SSDataBlock* pBlock) {
  if (pInfo->pUpdateInfo) {
    pInfo->pUpdateInfo->maxDataVersion = TMAX(pInfo->pUpdateInfo->maxDataVersion, pBlock->info.version);
  }
  if (!pInfo->igCheckUpdate && pInfo->pUpdateInfo) {
    checkUpdateData(pInfo, true, pBlock, true);
    pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, endKey);
    if (pInfo->pUpdateDataRes->info.rows > 0) {
      pInfo->updateResIndex = 0;
      if (pInfo->pUpdateDataRes->info.type == STREAM_CLEAR) {
        pInfo->scanMode = STREAM_SCAN_FROM_UPDATERES;
      } else if (pInfo->pUpdateDataRes->info.type == STREAM_INVERT) {
        pInfo->scanMode = STREAM_SCAN_FROM_RES;
        // return pInfo->pUpdateDataRes;
      } else if (pInfo->pUpdateDataRes->info.type == STREAM_DELETE_DATA) {
        pInfo->scanMode = STREAM_SCAN_FROM_DELETE_DATA;
      }
    }
  }
}

int32_t streamScanOperatorEncode(SStreamScanInfo* pInfo, void** pBuff) {
 int32_t len = pInfo->stateStore.updateInfoSerialize(NULL, 0, pInfo->pUpdateInfo);
 len += encodeSTimeWindowAggSupp(NULL, &pInfo->twAggSup);
 *pBuff = taosMemoryCalloc(1, len);
 void* buf = *pBuff;
 encodeSTimeWindowAggSupp(&buf, &pInfo->twAggSup);
 pInfo->stateStore.updateInfoSerialize(buf, len, pInfo->pUpdateInfo);
 return len;
}

void streamScanOperatorSaveCheckpoint(SStreamScanInfo* pInfo) {
  if (!pInfo->pState) {
    return;
  }
  void* pBuf = NULL;
  int32_t len = streamScanOperatorEncode(pInfo, &pBuf);
  pInfo->stateStore.streamStateSaveInfo(pInfo->pState, STREAM_SCAN_OP_CHECKPOINT_NAME, strlen(STREAM_SCAN_OP_CHECKPOINT_NAME), pBuf, len);
  taosMemoryFree(pBuf);
}

// other properties are recovered from the execution plan
void streamScanOperatorDecode(void* pBuff, int32_t len, SStreamScanInfo* pInfo) {
  if (!pBuff || len == 0) {
    return;
  }
  void* buf = pBuff;
  buf = decodeSTimeWindowAggSupp(buf, &pInfo->twAggSup);
  int32_t tlen = len - encodeSTimeWindowAggSupp(NULL, &pInfo->twAggSup);
  if (tlen == 0) {
    return;
  }

  void* pUpInfo = taosMemoryCalloc(1, sizeof(SUpdateInfo));
  int32_t code = pInfo->stateStore.updateInfoDeserialize(buf, tlen, pUpInfo);
  if (code == TSDB_CODE_SUCCESS) {
    pInfo->stateStore.updateInfoDestroy(pInfo->pUpdateInfo);
    pInfo->pUpdateInfo = pUpInfo;
  } else {
    taosMemoryFree(pUpInfo);
  }
}
static bool hasScanRange(SStreamScanInfo* pInfo) {
  SStreamAggSupporter* pSup = pInfo->windowSup.pStreamAggSup;
  return pSup && pSup->pScanBlock->info.rows > 0 && (isStateWindow(pInfo) || isCountWindow(pInfo));
}

static bool isStreamWindow(SStreamScanInfo* pInfo) {
  return isIntervalWindow(pInfo) || isSessionWindow(pInfo) || isStateWindow(pInfo) || isCountWindow(pInfo);
}

static SSDataBlock* doStreamScan(SOperatorInfo* pOperator) {
  // NOTE: this operator does never check if current status is done or not
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  const char*    id = GET_TASKID(pTaskInfo);

  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  SStreamScanInfo* pInfo = pOperator->info;
  SStreamTaskInfo* pStreamInfo = &pTaskInfo->streamInfo;

  qDebug("stream scan started, %s", id);

  if (pStreamInfo->recoverStep == STREAM_RECOVER_STEP__PREPARE1 || pStreamInfo->recoverStep == STREAM_RECOVER_STEP__PREPARE2) {
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    memcpy(&pTSInfo->base.cond, &pStreamInfo->tableCond, sizeof(SQueryTableDataCond));

    if (pStreamInfo->recoverStep == STREAM_RECOVER_STEP__PREPARE1) {
      pTSInfo->base.cond.startVersion = pStreamInfo->fillHistoryVer.minVer;
      pTSInfo->base.cond.endVersion = pStreamInfo->fillHistoryVer.maxVer;

      pTSInfo->base.cond.twindows = pStreamInfo->fillHistoryWindow;
      qDebug("stream scan step1, verRange:%" PRId64 "-%" PRId64 " window:%"PRId64"-%"PRId64", %s", pTSInfo->base.cond.startVersion,
             pTSInfo->base.cond.endVersion, pTSInfo->base.cond.twindows.skey, pTSInfo->base.cond.twindows.ekey, id);
      pStreamInfo->recoverStep = STREAM_RECOVER_STEP__SCAN1;
      pStreamInfo->recoverScanFinished = false;
    } else {
      pTSInfo->base.cond.startVersion = pStreamInfo->fillHistoryVer.minVer;
      pTSInfo->base.cond.endVersion = pStreamInfo->fillHistoryVer.maxVer;
      pTSInfo->base.cond.twindows = pStreamInfo->fillHistoryWindow;
      qDebug("stream scan step2 (scan wal), verRange:%" PRId64 " - %" PRId64 ", window:%" PRId64 "-%" PRId64 ", %s",
             pTSInfo->base.cond.startVersion, pTSInfo->base.cond.endVersion, pTSInfo->base.cond.twindows.skey,
             pTSInfo->base.cond.twindows.ekey, id);
      pStreamInfo->recoverStep = STREAM_RECOVER_STEP__NONE;
    }

    pAPI->tsdReader.tsdReaderClose(pTSInfo->base.dataReader);

    pTSInfo->base.dataReader = NULL;
    pInfo->pTableScanOp->status = OP_OPENED;

    pTSInfo->scanTimes = 0;
    pTSInfo->currentGroupId = -1;
  }

  if (pStreamInfo->recoverStep == STREAM_RECOVER_STEP__SCAN1) {
    if (isTaskKilled(pTaskInfo)) {
      return NULL;
    }

    switch (pInfo->scanMode) {
      case STREAM_SCAN_FROM_RES: {
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
        printSpecDataBlock(pInfo->pRecoverRes, getStreamOpName(pOperator->operatorType), "recover", GET_TASKID(pTaskInfo));
        return pInfo->pRecoverRes;
      } break;
      default:
        break;
    }

    pInfo->pRecoverRes = doTableScan(pInfo->pTableScanOp);
    if (pInfo->pRecoverRes != NULL) {
      calBlockTbName(pInfo, pInfo->pRecoverRes);
      if (!pInfo->igCheckUpdate && pInfo->pUpdateInfo) {
        TSKEY maxTs = pAPI->stateStore.updateInfoFillBlockData(pInfo->pUpdateInfo, pInfo->pRecoverRes, pInfo->primaryTsIndex);
        pInfo->twAggSup.maxTs = TMAX(pInfo->twAggSup.maxTs, maxTs);
      }
      if (pInfo->pCreateTbRes->info.rows > 0) {
        pInfo->scanMode = STREAM_SCAN_FROM_RES;
        printSpecDataBlock(pInfo->pCreateTbRes, getStreamOpName(pOperator->operatorType), "recover", GET_TASKID(pTaskInfo));
        return pInfo->pCreateTbRes;
      }

      qDebug("stream recover scan get block, rows %" PRId64, pInfo->pRecoverRes->info.rows);
      printSpecDataBlock(pInfo->pRecoverRes, getStreamOpName(pOperator->operatorType), "recover", GET_TASKID(pTaskInfo));
      return pInfo->pRecoverRes;
    }
    pStreamInfo->recoverStep = STREAM_RECOVER_STEP__NONE;
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    pAPI->tsdReader.tsdReaderClose(pTSInfo->base.dataReader);

    pTSInfo->base.dataReader = NULL;

    pTSInfo->base.cond.startVersion = -1;
    pTSInfo->base.cond.endVersion = -1;

    pStreamInfo->recoverScanFinished = true;
    return NULL;
  }

  size_t total = taosArrayGetSize(pInfo->pBlockLists);
// TODO: refactor
FETCH_NEXT_BLOCK:
  if (pInfo->blockType == STREAM_INPUT__DATA_BLOCK) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      return NULL;
    }

    int32_t  current = pInfo->validBlockIndex++;
    qDebug("process %d/%d input data blocks, %s", current, (int32_t) total, id);

    SPackedData* pPacked = taosArrayGet(pInfo->pBlockLists, current);
    SSDataBlock* pBlock = pPacked->pDataBlock;
    if (pBlock->info.parTbName[0]) {
      pAPI->stateStore.streamStatePutParName(pStreamInfo->pState, pBlock->info.id.groupId, pBlock->info.parTbName);
    }

    // TODO move into scan
    pBlock->info.calWin.skey = INT64_MIN;
    pBlock->info.calWin.ekey = INT64_MAX;
    pBlock->info.dataLoad = 1;
    if (pInfo->pUpdateInfo) {
      pInfo->pUpdateInfo->maxDataVersion = TMAX(pInfo->pUpdateInfo->maxDataVersion, pBlock->info.version);
    }

    blockDataUpdateTsWindow(pBlock, 0);
    switch (pBlock->info.type) {
      case STREAM_NORMAL:
      case STREAM_GET_ALL:
        return pBlock;
      case STREAM_RETRIEVE: {
        pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RETRIEVE;
        copyDataBlock(pInfo->pUpdateRes, pBlock);
        pInfo->updateResIndex = 0;
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
        pAPI->stateStore.updateInfoAddCloseWindowSBF(pInfo->pUpdateInfo);
      } break;
      case STREAM_DELETE_DATA: {
        printSpecDataBlock(pBlock, getStreamOpName(pOperator->operatorType), "delete recv", GET_TASKID(pTaskInfo));
        SSDataBlock* pDelBlock = NULL;
        if (pInfo->tqReader) {
          pDelBlock = createSpecialDataBlock(STREAM_DELETE_DATA);
          filterDelBlockByUid(pDelBlock, pBlock, pInfo);
        } else {
          pDelBlock = pBlock;
        }

        setBlockGroupIdByUid(pInfo, pDelBlock);
        rebuildDeleteBlockData(pDelBlock, &pStreamInfo->fillHistoryWindow, id);
        printSpecDataBlock(pDelBlock, getStreamOpName(pOperator->operatorType), "delete recv filtered", GET_TASKID(pTaskInfo));
        if (pDelBlock->info.rows == 0) {
          if (pInfo->tqReader) {
            blockDataDestroy(pDelBlock);
          }
          goto FETCH_NEXT_BLOCK;
        }

        if (!isStreamWindow(pInfo)) {
          generateDeleteResultBlock(pInfo, pDelBlock, pInfo->pDeleteDataRes);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_RESULT;
          printSpecDataBlock(pDelBlock, getStreamOpName(pOperator->operatorType), "delete result", GET_TASKID(pTaskInfo));
          blockDataDestroy(pDelBlock);

          if (pInfo->pDeleteDataRes->info.rows > 0) {
            return pInfo->pDeleteDataRes;
          } else {
            goto FETCH_NEXT_BLOCK;
          }
        } else {
          pInfo->blockType = STREAM_INPUT__DATA_SUBMIT;
          pInfo->updateResIndex = 0;
          generateScanRange(pInfo, pDelBlock, pInfo->pUpdateRes, STREAM_DELETE_DATA);
          prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
          copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
          pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
          printSpecDataBlock(pDelBlock, getStreamOpName(pOperator->operatorType), "delete result", GET_TASKID(pTaskInfo));
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
      case STREAM_CHECKPOINT: {
        qError("stream check point error. msg type: STREAM_INPUT__DATA_BLOCK");
      } break;
      default:
        break;
    }
    printDataBlock(pBlock, getStreamOpName(pOperator->operatorType), GET_TASKID(pTaskInfo));
    return pBlock;
  } else if (pInfo->blockType == STREAM_INPUT__DATA_SUBMIT) {
    qDebug("stream scan mode:%d, %s", pInfo->scanMode, id);
    switch (pInfo->scanMode) {
      case STREAM_SCAN_FROM_RES: {
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
        doCheckUpdate(pInfo, pInfo->pRes->info.window.ekey, pInfo->pRes);
        doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
        pInfo->pRes->info.dataLoad = 1;
        blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
        if (pInfo->pRes->info.rows > 0) {
          return pInfo->pRes;
        }
      } break;
      case STREAM_SCAN_FROM_DELETE_DATA: {
        generateScanRange(pInfo, pInfo->pUpdateDataRes, pInfo->pUpdateRes, STREAM_DELETE_DATA);
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
        copyDataBlock(pInfo->pDeleteDataRes, pInfo->pUpdateRes);
        pInfo->pDeleteDataRes->info.type = STREAM_DELETE_DATA;
        return pInfo->pDeleteDataRes;
      } break;
      case STREAM_SCAN_FROM_UPDATERES: {
        generateScanRange(pInfo, pInfo->pUpdateDataRes, pInfo->pUpdateRes, STREAM_CLEAR);
        prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
        pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
        return pInfo->pUpdateRes;
      } break;
      case STREAM_SCAN_FROM_DATAREADER_RANGE:
      case STREAM_SCAN_FROM_DATAREADER_RETRIEVE: {
        SSDataBlock* pSDB = doRangeScan(pInfo, pInfo->pUpdateRes, pInfo->primaryTsIndex, &pInfo->updateResIndex);
        if (pSDB) {
          STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
          pSDB->info.type = pInfo->scanMode == STREAM_SCAN_FROM_DATAREADER_RANGE ? STREAM_NORMAL : STREAM_PULL_DATA;
          if (!pInfo->igCheckUpdate && pInfo->pUpdateInfo) {
            checkUpdateData(pInfo, true, pSDB, false);
          }
          printSpecDataBlock(pSDB, getStreamOpName(pOperator->operatorType), "update", GET_TASKID(pTaskInfo));
          calBlockTbName(pInfo, pSDB);
          return pSDB;
        }
        blockDataCleanup(pInfo->pUpdateDataRes);
        pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
      } break;
      default:
        break;
    }

    if (hasScanRange(pInfo)) {
      pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
      pInfo->updateResIndex = 0;
      SStreamAggSupporter* pSup = pInfo->windowSup.pStreamAggSup;
      copyDataBlock(pInfo->pUpdateRes, pSup->pScanBlock);
      blockDataCleanup(pSup->pScanBlock);
      prepareRangeScan(pInfo, pInfo->pUpdateRes, &pInfo->updateResIndex);
      pInfo->pUpdateRes->info.type = STREAM_DELETE_DATA;
      printSpecDataBlock(pInfo->pUpdateRes, getStreamOpName(pOperator->operatorType), "rebuild", GET_TASKID(pTaskInfo));
      return pInfo->pUpdateRes;
    }

    SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
    int32_t         totalBlocks = taosArrayGetSize(pInfo->pBlockLists);

  NEXT_SUBMIT_BLK:
    while (1) {
      if (pInfo->readerFn.tqReaderCurrentBlockConsumed(pInfo->tqReader)) {
        if (pInfo->validBlockIndex >= totalBlocks) {
          pAPI->stateStore.updateInfoDestoryColseWinSBF(pInfo->pUpdateInfo);
          doClearBufferedBlocks(pInfo);

          qDebug("stream scan return empty, all %d submit blocks consumed, %s", totalBlocks, id);
          return NULL;
        }

        int32_t      current = pInfo->validBlockIndex++;
        SPackedData* pSubmit = taosArrayGet(pInfo->pBlockLists, current);

        qDebug("set %d/%d as the input submit block, %s", current + 1, totalBlocks, id);
        if (pAPI->tqReaderFn.tqReaderSetSubmitMsg(pInfo->tqReader, pSubmit->msgStr, pSubmit->msgLen, pSubmit->ver) < 0) {
          qError("submit msg messed up when initializing stream submit block %p, current %d/%d, %s", pSubmit, current, totalBlocks, id);
          continue;
        }
      }

      blockDataCleanup(pInfo->pRes);

      while (pAPI->tqReaderFn.tqNextBlockImpl(pInfo->tqReader, id)) {
        SSDataBlock* pRes = NULL;

        int32_t code = pAPI->tqReaderFn.tqRetrieveBlock(pInfo->tqReader, &pRes, id);
        qDebug("retrieve data from submit completed code:%s rows:%" PRId64 " %s", tstrerror(code), pRes->info.rows, id);

        if (code != TSDB_CODE_SUCCESS || pRes->info.rows == 0) {
          qDebug("retrieve data failed, try next block in submit block, %s", id);
          continue;
        }

        setBlockIntoRes(pInfo, pRes, &pStreamInfo->fillHistoryWindow, false);
        if (pInfo->pRes->info.rows == 0) {
          continue;
        }

        if (pInfo->pCreateTbRes->info.rows > 0) {
          pInfo->scanMode = STREAM_SCAN_FROM_RES;
          qDebug("create table res exists, rows:%"PRId64" return from stream scan, %s", pInfo->pCreateTbRes->info.rows, id);
          return pInfo->pCreateTbRes;
        }

        doCheckUpdate(pInfo, pBlockInfo->window.ekey, pInfo->pRes);
        doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
        blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);

        int64_t numOfUpdateRes = pInfo->pUpdateDataRes->info.rows;
        qDebug("%s %" PRId64 " rows in datablock, update res:%" PRId64, id, pBlockInfo->rows, numOfUpdateRes);
        if (pBlockInfo->rows > 0 || numOfUpdateRes > 0) {
          break;
        }
      }

      if (pBlockInfo->rows > 0 || pInfo->pUpdateDataRes->info.rows > 0) {
        break;
      } else {
        continue;
      }
    }

    // record the scan action.
    pInfo->numOfExec++;
    pOperator->resultInfo.totalRows += pBlockInfo->rows;

    qDebug("stream scan completed, and return source rows:%" PRId64", %s", pBlockInfo->rows, id);
    if (pBlockInfo->rows > 0) {
      return pInfo->pRes;
    }

    if (pInfo->pUpdateDataRes->info.rows > 0) {
      goto FETCH_NEXT_BLOCK;
    }

    goto NEXT_SUBMIT_BLK;
  } else if (pInfo->blockType == STREAM_INPUT__CHECKPOINT) {
    if (pInfo->validBlockIndex >= total) {
      doClearBufferedBlocks(pInfo);
      return NULL;
    }

    int32_t  current = pInfo->validBlockIndex++;
    qDebug("process %d/%d input data blocks, %s", current, (int32_t) total, id);

    SPackedData* pData = taosArrayGet(pInfo->pBlockLists, current);
    SSDataBlock* pBlock = taosArrayGet(pData->pDataBlock, 0);

    if (pBlock->info.type == STREAM_CHECKPOINT) {
      streamScanOperatorSaveCheckpoint(pInfo);
    }
    // printDataBlock(pBlock, "stream scan ck");
    return pInfo->pCheckpointRes;
  }

  return NULL;
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
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SStreamRawScanInfo* pInfo = pOperator->info;
  int32_t             code = TSDB_CODE_SUCCESS;
  pTaskInfo->streamInfo.metaRsp.metaRspLen = 0;  // use metaRspLen !=0 to judge if data is meta
  pTaskInfo->streamInfo.metaRsp.metaRsp = NULL;

  qDebug("tmqsnap doRawScan called");
  if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    bool hasNext = false;
    if (pInfo->dataReader && pInfo->sContext->withMeta != ONLY_META) {
      code = pAPI->tsdReader.tsdNextDataBlock(pInfo->dataReader, &hasNext);
      if (code) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->dataReader);
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    if (pInfo->dataReader && hasNext) {
      if (isTaskKilled(pTaskInfo)) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->dataReader);
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }

      SSDataBlock* pBlock = pAPI->tsdReader.tsdReaderRetrieveDataBlock(pInfo->dataReader, NULL);
      if (pBlock == NULL) {
        T_LONG_JMP(pTaskInfo->env, terrno);
      }

      qDebug("tmqsnap doRawScan get data uid:%" PRId64 "", pBlock->info.id.uid);
      tqOffsetResetToData(&pTaskInfo->streamInfo.currentOffset, pBlock->info.id.uid, pBlock->info.window.ekey);
      return pBlock;
    }

    SMetaTableInfo mtInfo = pAPI->snapshotFn.getMetaTableInfoFromSnapshot(pInfo->sContext);
    STqOffsetVal   offset = {0};
    if (mtInfo.uid == 0 || pInfo->sContext->withMeta == ONLY_META) {  // read snapshot done, change to get data from wal
      qDebug("tmqsnap read snapshot done, change to get data from wal");
      tqOffsetResetToLog(&offset, pInfo->sContext->snapVersion + 1);
    } else {
      tqOffsetResetToData(&offset, mtInfo.uid, INT64_MIN);
      qDebug("tmqsnap change get data uid:%" PRId64 "", mtInfo.uid);
    }
    qStreamPrepareScan(pTaskInfo, &offset, pInfo->sContext->subType);
    tDeleteSchemaWrapper(mtInfo.schema);
    return NULL;
  } else if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__SNAPSHOT_META) {
    SSnapContext* sContext = pInfo->sContext;
    void*         data = NULL;
    int32_t       dataLen = 0;
    int16_t       type = 0;
    int64_t       uid = 0;
    if (pAPI->snapshotFn.getTableInfoFromSnapshot(sContext, &data, &dataLen, &type, &uid) < 0) {
      qError("tmqsnap getTableInfoFromSnapshot error");
      taosMemoryFreeClear(data);
      return NULL;
    }

    if (!sContext->queryMeta) {  // change to get data next poll request
      STqOffsetVal offset = {0};
      tqOffsetResetToData(&offset, 0, INT64_MIN);
      qStreamPrepareScan(pTaskInfo, &offset, pInfo->sContext->subType);
    } else {
      tqOffsetResetToMeta(&pTaskInfo->streamInfo.currentOffset, uid);
      pTaskInfo->streamInfo.metaRsp.resMsgType = type;
      pTaskInfo->streamInfo.metaRsp.metaRspLen = dataLen;
      pTaskInfo->streamInfo.metaRsp.metaRsp = data;
    }

    return NULL;
  }
  return NULL;
}

static void destroyRawScanOperatorInfo(void* param) {
  SStreamRawScanInfo* pRawScan = (SStreamRawScanInfo*)param;
  pRawScan->pAPI->tsdReader.tsdReaderClose(pRawScan->dataReader);
  pRawScan->pAPI->snapshotFn.destroySnapshot(pRawScan->sContext);
  tableListDestroy(pRawScan->pTableListInfo);
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

  pInfo->pTableListInfo = tableListCreate();
  pInfo->vnode = pHandle->vnode;
  pInfo->pAPI = &pTaskInfo->storageAPI;

  pInfo->sContext = pHandle->sContext;
  setOperatorInfo(pOperator, "RawScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(NULL, doRawScan, NULL, destroyRawScanOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
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
    destroyOperator(pStreamScan->pTableScanOp);
  }

  if (pStreamScan->tqReader) {
    pStreamScan->readerFn.tqReaderClose(pStreamScan->tqReader);
  }
  if (pStreamScan->matchInfo.pList) {
    taosArrayDestroy(pStreamScan->matchInfo.pList);
  }
  if (pStreamScan->pPseudoExpr) {
    destroyExprInfo(pStreamScan->pPseudoExpr, pStreamScan->numOfPseudoExpr);
    taosMemoryFree(pStreamScan->pPseudoExpr);
  }

  cleanupExprSupp(&pStreamScan->tbnameCalSup);
  cleanupExprSupp(&pStreamScan->tagCalSup);

  pStreamScan->stateStore.updateInfoDestroy(pStreamScan->pUpdateInfo);
  blockDataDestroy(pStreamScan->pRes);
  blockDataDestroy(pStreamScan->pUpdateRes);
  blockDataDestroy(pStreamScan->pDeleteDataRes);
  blockDataDestroy(pStreamScan->pUpdateDataRes);
  blockDataDestroy(pStreamScan->pCreateTbRes);
  taosArrayDestroy(pStreamScan->pBlockLists);
  blockDataDestroy(pStreamScan->pCheckpointRes);

  taosMemoryFree(pStreamScan);
}

void streamScanReleaseState(SOperatorInfo* pOperator) {
  SStreamScanInfo* pInfo = pOperator->info;
  if (!pInfo->pState) {
    return;
  }
  if (!pInfo->pUpdateInfo) {
    return;
  }
  int32_t len = pInfo->stateStore.updateInfoSerialize(NULL, 0, pInfo->pUpdateInfo);
  void* pBuff = taosMemoryCalloc(1, len);
  pInfo->stateStore.updateInfoSerialize(pBuff, len, pInfo->pUpdateInfo);
  pInfo->stateStore.streamStateSaveInfo(pInfo->pState, STREAM_SCAN_OP_STATE_NAME, strlen(STREAM_SCAN_OP_STATE_NAME), pBuff, len);
  taosMemoryFree(pBuff);
}

void streamScanReloadState(SOperatorInfo* pOperator) {
  SStreamScanInfo* pInfo = pOperator->info;
  if (!pInfo->pState) {
    return;
  }
  void*   pBuff = NULL;
  int32_t len = 0;
  pInfo->stateStore.streamStateGetInfo(pInfo->pState, STREAM_SCAN_OP_STATE_NAME, strlen(STREAM_SCAN_OP_STATE_NAME), &pBuff, &len);
  SUpdateInfo* pUpInfo = taosMemoryCalloc(1, sizeof(SUpdateInfo));
  int32_t      code = pInfo->stateStore.updateInfoDeserialize(pBuff, len, pUpInfo);
  taosMemoryFree(pBuff);
  if (code == TSDB_CODE_SUCCESS && pInfo->pUpdateInfo) {
    if (pInfo->pUpdateInfo->minTS < 0) {
      pInfo->stateStore.updateInfoDestroy(pInfo->pUpdateInfo);
      pInfo->pUpdateInfo = pUpInfo;
    } else {
      pInfo->stateStore.windowSBfDelete(pInfo->pUpdateInfo, 1);
      pInfo->stateStore.windowSBfAdd(pInfo->pUpdateInfo, 1);
      ASSERT(pInfo->pUpdateInfo->minTS > pUpInfo->minTS);
      pInfo->pUpdateInfo->maxDataVersion = TMAX(pInfo->pUpdateInfo->maxDataVersion, pUpInfo->maxDataVersion);
      SHashObj* curMap = pInfo->pUpdateInfo->pMap;
      void *pIte = taosHashIterate(curMap, NULL);
      while (pIte != NULL) {
        size_t keySize = 0;
        int64_t* pUid = taosHashGetKey(pIte, &keySize);
        taosHashPut(pUpInfo->pMap, pUid, sizeof(int64_t), pIte, sizeof(TSKEY));
        pIte = taosHashIterate(curMap, pIte);
      }
      taosHashCleanup(curMap);
      pInfo->pUpdateInfo->pMap = pUpInfo->pMap;
      pUpInfo->pMap = NULL;
      pInfo->stateStore.updateInfoDestroy(pUpInfo);
    }
  } else {
    pInfo->stateStore.updateInfoDestroy(pUpInfo);
  }
}

SOperatorInfo* createStreamScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode, SNode* pTagCond,
                                            STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo) {
  SArray*          pColIds = NULL;
  SStreamScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamScanInfo));
  SOperatorInfo*   pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  const char* idstr = pTaskInfo->id.str;

  if (pInfo == NULL || pOperator == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tableListDestroy(pTableListInfo);
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
    tableListDestroy(pTableListInfo);
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
      tableListDestroy(pTableListInfo);
      goto _error;
    }

    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    createExprFromOneNode(pSubTableExpr, pTableScanNode->pSubtable, 0);
    if (initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1, &pTaskInfo->storageAPI.functionStore) != 0) {
      tableListDestroy(pTableListInfo);
      goto _error;
    }
  }

  if (pTableScanNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExpr(pTableScanNode->pTags, &numOfTags);
    if (pTagExpr == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tableListDestroy(pTableListInfo);
      goto _error;
    }
    if (initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags, &pTaskInfo->storageAPI.functionStore) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      tableListDestroy(pTableListInfo);
      goto _error;
    }
  }

  pInfo->pBlockLists = taosArrayInit(4, sizeof(SPackedData));
  if (pInfo->pBlockLists == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    tableListDestroy(pTableListInfo);
    goto _error;
  }

  if (pHandle->vnode) {
    SOperatorInfo*  pTableScanOp = createTableScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo);
    if (pTableScanOp == NULL) {
      qError("createTableScanOperatorInfo error, errorcode: %d", pTaskInfo->code);
      goto _error;
    }
    STableScanInfo* pTSInfo = (STableScanInfo*)pTableScanOp->info;
    if (pHandle->version > 0) {
      pTSInfo->base.cond.endVersion = pHandle->version;
    }

    STableKeyInfo* pList = NULL;
    int32_t        num = 0;
    tableListGetGroupList(pTableListInfo, 0, &pList, &num);

    if (pHandle->initTableReader) {
      pTSInfo->scanMode = TABLE_SCAN__TABLE_ORDER;
      pTSInfo->base.dataReader = NULL;
    }

    if (pHandle->initTqReader) {
      ASSERT(pHandle->tqReader == NULL);
      pInfo->tqReader = pAPI->tqReaderFn.tqReaderOpen(pHandle->vnode);
      ASSERT(pInfo->tqReader);
    } else {
      ASSERT(pHandle->tqReader);
      pInfo->tqReader = pHandle->tqReader;
    }

    pInfo->pUpdateInfo = NULL;
    pInfo->pTableScanOp = pTableScanOp;
    if (pInfo->pTableScanOp->pTaskInfo->streamInfo.pState) {
      pAPI->stateStore.streamStateSetNumber(pInfo->pTableScanOp->pTaskInfo->streamInfo.pState, -1);
    }

    pInfo->readHandle = *pHandle;
    pTaskInfo->streamInfo.snapshotVer = pHandle->version;
    pInfo->pCreateTbRes = buildCreateTableBlock(&pInfo->tbnameCalSup, &pInfo->tagCalSup);
    blockDataEnsureCapacity(pInfo->pCreateTbRes, 8);

    // set the extract column id to streamHandle
    pAPI->tqReaderFn.tqReaderSetColIdList(pInfo->tqReader, pColIds);
    SArray* tableIdList = extractTableIdList(((STableScanInfo*)(pInfo->pTableScanOp->info))->base.pTableListInfo);
    code = pAPI->tqReaderFn.tqReaderSetQueryTableList(pInfo->tqReader, tableIdList, idstr);
    if (code != 0) {
      taosArrayDestroy(tableIdList);
      goto _error;
    }

    taosArrayDestroy(tableIdList);
    memcpy(&pTaskInfo->streamInfo.tableCond, &pTSInfo->base.cond, sizeof(SQueryTableDataCond));
  } else {
    taosArrayDestroy(pColIds);
    tableListDestroy(pTableListInfo);
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
  pInfo->pState = pTaskInfo->streamInfo.pState;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->readerFn = pTaskInfo->storageAPI.tqReaderFn;
  pInfo->pCheckpointRes = createSpecialDataBlock(STREAM_CHECKPOINT);

  // for stream
  if (pTaskInfo->streamInfo.pState) {
    void*   buff = NULL;
    int32_t len = 0;
    int32_t res = pAPI->stateStore.streamStateGetInfo(pTaskInfo->streamInfo.pState, STREAM_SCAN_OP_CHECKPOINT_NAME, strlen(STREAM_SCAN_OP_CHECKPOINT_NAME), &buff, &len);
    if (res == TSDB_CODE_SUCCESS) {
      streamScanOperatorDecode(buff, len, pInfo);
      taosMemoryFree(buff);
    }
  }

  setOperatorInfo(pOperator, STREAM_SCAN_OP_NAME, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  __optr_fn_t nextFn = (pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM) ? doStreamScan : doQueueScan;
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, nextFn, NULL, destroyStreamScanOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  setOperatorStreamStateFn(pOperator, streamScanReleaseState, streamScanReloadState);

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

static void doTagScanOneTable(SOperatorInfo* pOperator, const SSDataBlock* pRes, int32_t count, SMetaReader* mr, SStorageAPI* pAPI) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  STagScanInfo* pInfo = pOperator->info;
  SExprInfo*    pExprInfo = &pOperator->exprSupp.pExprInfo[0];

  STableKeyInfo* item = tableListGetInfo(pInfo->pTableListInfo, pInfo->curPos);
  int32_t        code = pAPI->metaReaderFn.getTableEntryByUid(mr, item->uid);
  tDecoderClear(&(*mr).coder);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", item->uid, tstrerror(terrno),
           GET_TASKID(pTaskInfo));
    pAPI->metaReaderFn.clearReader(mr);
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  char str[512];
  for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
    SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);

    // refactor later
    if (FUNCTION_TYPE_TBNAME == pExprInfo[j].pExpr->_function.functionType) {
      STR_TO_VARSTR(str, (*mr).me.name);
      colDataSetVal(pDst, (count), str, false);
    } else if (FUNCTION_TYPE_TBUID == pExprInfo[j].pExpr->_function.functionType) {
      colDataSetVal(pDst, (count), (char*)&(*mr).me.uid, false);    
    } else if (FUNCTION_TYPE_VGID == pExprInfo[j].pExpr->_function.functionType) {
      colDataSetVal(pDst, (count), (char*)&pTaskInfo->id.vgId, false);    
    } else {  // it is a tag value
      STagVal val = {0};
      val.cid = pExprInfo[j].base.pParam[0].pCol->colId;
      const char* p = pAPI->metaFn.extractTagVal((*mr).me.ctbEntry.pTags, pDst->info.type, &val);

      char* data = NULL;
      if (pDst->info.type != TSDB_DATA_TYPE_JSON && p != NULL) {
        data = tTagValToData((const STagVal*)p, false);
      } else {
        data = (char*)p;
      }
      colDataSetVal(pDst, (count), data,
                    (data == NULL) || (pDst->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data)));

      if (pDst->info.type != TSDB_DATA_TYPE_JSON && p != NULL && IS_VAR_DATA_TYPE(((const STagVal*)p)->type) &&
          data != NULL) {
        taosMemoryFree(data);
      }
    }
  }
}

static void tagScanFreeUidTag(void* p) {
  STUidTagInfo* pInfo = p;
  if (pInfo->pTagVal != NULL) {
    taosMemoryFree(pInfo->pTagVal);
  }
}

static int32_t tagScanCreateResultData(SDataType* pType, int32_t numOfRows, SScalarParam* pParam) {
  SColumnInfoData* pColumnData = taosMemoryCalloc(1, sizeof(SColumnInfoData));
  if (pColumnData == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return terrno;
  }

  pColumnData->info.type = pType->type;
  pColumnData->info.bytes = pType->bytes;
  pColumnData->info.scale = pType->scale;
  pColumnData->info.precision = pType->precision;

  int32_t code = colInfoDataEnsureCapacity(pColumnData, numOfRows, true);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    taosMemoryFree(pColumnData);
    return terrno;
  }

  pParam->columnData = pColumnData;
  pParam->colAlloced = true;
  return TSDB_CODE_SUCCESS;
}

static EDealRes tagScanRewriteTagColumn(SNode** pNode, void* pContext) {
  SColumnNode* pSColumnNode = NULL;
  if (QUERY_NODE_COLUMN == nodeType((*pNode))) {
    pSColumnNode = *(SColumnNode**)pNode;
  } else if (QUERY_NODE_FUNCTION == nodeType((*pNode))) {
    SFunctionNode* pFuncNode = *(SFunctionNode**)(pNode);
    if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
      pSColumnNode = (SColumnNode*)nodesMakeNode(QUERY_NODE_COLUMN);
      if (NULL == pSColumnNode) {
        return DEAL_RES_ERROR;
      }
      pSColumnNode->colId = -1;
      pSColumnNode->colType = COLUMN_TYPE_TBNAME;
      pSColumnNode->node.resType.type = TSDB_DATA_TYPE_VARCHAR;
      pSColumnNode->node.resType.bytes = TSDB_TABLE_FNAME_LEN - 1 + VARSTR_HEADER_SIZE;
      nodesDestroyNode(*pNode);
      *pNode = (SNode*)pSColumnNode;
    } else {
      return DEAL_RES_CONTINUE;
    }
  } else {
    return DEAL_RES_CONTINUE;
  }

  STagScanFilterContext* pCtx = (STagScanFilterContext*)pContext;
  void*            data = taosHashGet(pCtx->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId));
  if (!data) {
    taosHashPut(pCtx->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId), pNode, sizeof((*pNode)));
    pSColumnNode->slotId = pCtx->index++;
    SColumnInfo cInfo = {.colId = pSColumnNode->colId,
                         .type = pSColumnNode->node.resType.type,
                         .bytes = pSColumnNode->node.resType.bytes};
    taosArrayPush(pCtx->cInfoList, &cInfo);
  } else {
    SColumnNode* col = *(SColumnNode**)data;
    pSColumnNode->slotId = col->slotId;
  }

  return DEAL_RES_CONTINUE;
}


static int32_t tagScanFilterByTagCond(SArray* aUidTags, SNode* pTagCond, SArray* aFilterIdxs, void* pVnode, SStorageAPI* pAPI, STagScanInfo* pInfo) {
  int32_t code = 0;
  int32_t numOfTables = taosArrayGetSize(aUidTags);

  SSDataBlock* pResBlock = createTagValBlockForFilter(pInfo->filterCtx.cInfoList, numOfTables, aUidTags, pVnode, pAPI);

  SArray* pBlockList = taosArrayInit(1, POINTER_BYTES);
  taosArrayPush(pBlockList, &pResBlock);
  SDataType type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};

  SScalarParam output = {0};
  code = tagScanCreateResultData(&type, numOfTables, &output);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  code = scalarCalculate(pTagCond, pBlockList, &output);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }

  bool* result = (bool*)output.columnData->pData;
  for (int32_t i = 0 ; i < numOfTables; ++i) {
    if (result[i]) {
      taosArrayPush(aFilterIdxs, &i);
    }
  }

  colDataDestroy(output.columnData);
  taosMemoryFreeClear(output.columnData);

  blockDataDestroy(pResBlock);
  taosArrayDestroy(pBlockList);

  return TSDB_CODE_SUCCESS;
}

static void tagScanFillOneCellWithTag(SOperatorInfo* pOperator, const STUidTagInfo* pUidTagInfo, SExprInfo* pExprInfo, SColumnInfoData* pColInfo, int rowIndex, const SStorageAPI* pAPI, void* pVnode) {
  if (QUERY_NODE_FUNCTION == pExprInfo->pExpr->nodeType) {
    if (FUNCTION_TYPE_TBNAME == pExprInfo->pExpr->_function.functionType) {  // tbname
      char str[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(str, "ctbidx");

      colDataSetVal(pColInfo, rowIndex, str, false);
    } else if (FUNCTION_TYPE_TBUID == pExprInfo->pExpr->_function.functionType) {
      colDataSetVal(pColInfo, rowIndex, (char*)&pUidTagInfo->uid, false);    
    } else if (FUNCTION_TYPE_VGID == pExprInfo->pExpr->_function.functionType) {
      colDataSetVal(pColInfo, rowIndex, (char*)&pOperator->pTaskInfo->id.vgId, false);   
    } 
  } else {
    STagVal tagVal = {0};
    tagVal.cid = pExprInfo->base.pParam[0].pCol->colId;
    if (pUidTagInfo->pTagVal == NULL) {
      colDataSetNULL(pColInfo, rowIndex);
    } else {
      const char* p = pAPI->metaFn.extractTagVal(pUidTagInfo->pTagVal, pColInfo->info.type, &tagVal);

      if (p == NULL || (pColInfo->info.type == TSDB_DATA_TYPE_JSON && ((STag*)p)->nTag == 0)) {
        colDataSetNULL(pColInfo, rowIndex);
      } else if (pColInfo->info.type == TSDB_DATA_TYPE_JSON) {
        colDataSetVal(pColInfo, rowIndex, p, false);
      } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
        char* tmp = taosMemoryMalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1);
        varDataSetLen(tmp, tagVal.nData);
        memcpy(tmp + VARSTR_HEADER_SIZE, tagVal.pData, tagVal.nData);
        colDataSetVal(pColInfo, rowIndex, tmp, false);
        taosMemoryFree(tmp);
      } else {
        colDataSetVal(pColInfo, rowIndex, (const char*)&tagVal.i64, false);
      }
    }
  }
}

static int32_t tagScanFillResultBlock(SOperatorInfo* pOperator, SSDataBlock* pRes, SArray* aUidTags, SArray* aFilterIdxs, bool ignoreFilterIdx,
                                      SStorageAPI* pAPI) {
  STagScanInfo* pInfo = pOperator->info;
  SExprInfo*    pExprInfo = &pOperator->exprSupp.pExprInfo[0];
  if (!ignoreFilterIdx) {
    size_t szTables = taosArrayGetSize(aFilterIdxs);
    for (int i = 0; i < szTables; ++i) {
      int32_t idx = *(int32_t*)taosArrayGet(aFilterIdxs, i);
      STUidTagInfo* pUidTagInfo = taosArrayGet(aUidTags, idx);
      for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);
        tagScanFillOneCellWithTag(pOperator, pUidTagInfo, &pExprInfo[j], pDst, i, pAPI, pInfo->readHandle.vnode);
      }
    }
  } else {
    size_t szTables = taosArrayGetSize(aUidTags);
    for (int i = 0; i < szTables; ++i) {
      STUidTagInfo* pUidTagInfo = taosArrayGet(aUidTags, i);
      for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);
        tagScanFillOneCellWithTag(pOperator, pUidTagInfo, &pExprInfo[j], pDst, i, pAPI, pInfo->readHandle.vnode);
      }
    }
  }
  return 0;
}

static SSDataBlock* doTagScanFromCtbIdx(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  STagScanInfo* pInfo = pOperator->info;
  SSDataBlock*  pRes = pInfo->pRes;
  blockDataCleanup(pRes);

  if (pInfo->pCtbCursor == NULL) {
    pInfo->pCtbCursor = pAPI->metaFn.openCtbCursor(pInfo->readHandle.vnode, pInfo->suid, 1);
  } else {
    pAPI->metaFn.resumeCtbCursor(pInfo->pCtbCursor, 0);
  }

  SArray* aUidTags = pInfo->aUidTags;
  SArray* aFilterIdxs = pInfo->aFilterIdxs;
  int32_t count = 0;
  bool ctbCursorFinished = false;
  while (1) {
    taosArrayClearEx(aUidTags, tagScanFreeUidTag);
    taosArrayClear(aFilterIdxs);

    int32_t numTables = 0;
    while (numTables < pOperator->resultInfo.capacity) {
      SMCtbCursor* pCur = pInfo->pCtbCursor;
      tb_uid_t     uid = pAPI->metaFn.ctbCursorNext(pInfo->pCtbCursor);
      if (uid == 0) {
        ctbCursorFinished = true;
        break;
      }
      STUidTagInfo info = {.uid = uid, .pTagVal = pCur->pVal};
      info.pTagVal = taosMemoryMalloc(pCur->vLen);
      memcpy(info.pTagVal, pCur->pVal, pCur->vLen);
      taosArrayPush(aUidTags, &info);
      ++numTables;
    }

    if (numTables == 0) {
      break;
    }
    bool ignoreFilterIdx = true;
    if (pInfo->pTagCond != NULL) {
      ignoreFilterIdx = false;
      int32_t code = tagScanFilterByTagCond(aUidTags, pInfo->pTagCond, aFilterIdxs, pInfo->readHandle.vnode, pAPI, pInfo);
      if (TSDB_CODE_SUCCESS != code) {
        pOperator->pTaskInfo->code = code;
        T_LONG_JMP(pOperator->pTaskInfo->env, code);
      }
    } else {
      ignoreFilterIdx = true;
    }

    tagScanFillResultBlock(pOperator, pRes, aUidTags, aFilterIdxs, ignoreFilterIdx, pAPI);
    
    count = ignoreFilterIdx ? taosArrayGetSize(aUidTags): taosArrayGetSize(aFilterIdxs);

    if (count != 0) {
      break;
    }
  }

  if (count > 0) {
    pAPI->metaFn.pauseCtbCursor(pInfo->pCtbCursor);
  }
  if (count == 0 || ctbCursorFinished) {
    pAPI->metaFn.closeCtbCursor(pInfo->pCtbCursor);
    pInfo->pCtbCursor = NULL;
    setOperatorCompleted(pOperator);
  }
  pRes->info.rows = count;

  bool bLimitReached = applyLimitOffset(&pInfo->limitInfo, pRes, pTaskInfo);
  if (bLimitReached) {
    setOperatorCompleted(pOperator);
  }
  pOperator->resultInfo.totalRows += pRes->info.rows;
  return (pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* doTagScanFromMetaEntry(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  STagScanInfo* pInfo = pOperator->info;
  SExprInfo*    pExprInfo = &pOperator->exprSupp.pExprInfo[0];
  SSDataBlock*  pRes = pInfo->pRes;
  blockDataCleanup(pRes);

  int32_t size = tableListGetSize(pInfo->pTableListInfo);
  if (size == 0) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    return NULL;
  }

  char        str[512] = {0};
  int32_t     count = 0;
  SMetaReader mr = {0};
  pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, 0, &pAPI->metaFn);

  while (pInfo->curPos < size && count < pOperator->resultInfo.capacity) {
    doTagScanOneTable(pOperator, pRes, count, &mr, &pTaskInfo->storageAPI);
    ++count;
    if (++pInfo->curPos >= size) {
      setOperatorCompleted(pOperator);
    }
  }
  pRes->info.rows = count;

  pAPI->metaReaderFn.clearReader(&mr);
  bool bLimitReached = applyLimitOffset(&pInfo->limitInfo, pRes, pTaskInfo);
  if (bLimitReached) {
    setOperatorCompleted(pOperator);
  }
  // qDebug("QInfo:0x%"PRIx64" create tag values results completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  if (pOperator->status == OP_EXEC_DONE) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
  }

  pOperator->resultInfo.totalRows += pRes->info.rows;

  return (pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static void destroyTagScanOperatorInfo(void* param) {
  STagScanInfo* pInfo = (STagScanInfo*)param;
  if (pInfo->pCtbCursor != NULL) {
    pInfo->pStorageAPI->metaFn.closeCtbCursor(pInfo->pCtbCursor);
  }
  taosHashCleanup(pInfo->filterCtx.colHash);
  taosArrayDestroy(pInfo->filterCtx.cInfoList);
  taosArrayDestroy(pInfo->aFilterIdxs);
  taosArrayDestroyEx(pInfo->aUidTags, tagScanFreeUidTag);

  pInfo->pRes = blockDataDestroy(pInfo->pRes);
  taosArrayDestroy(pInfo->matchInfo.pList);
  pInfo->pTableListInfo = tableListDestroy(pInfo->pTableListInfo);
  taosMemoryFreeClear(param);
}

SOperatorInfo* createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pTagScanNode,
                                         STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond, SExecTaskInfo* pTaskInfo) {
  SScanPhysiNode* pPhyNode = (SScanPhysiNode*)pTagScanNode;
  STagScanInfo*  pInfo = taosMemoryCalloc(1, sizeof(STagScanInfo));
  SOperatorInfo* pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pPhyNode->node.pOutputDataBlockDesc;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->pScanPseudoCols, NULL, &numOfExprs);
  int32_t    code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t num = 0;
  code = extractColMatchInfo(pPhyNode->pScanPseudoCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pTagCond = pTagCond;
  pInfo->pTagIndexCond = pTagIndexCond;
  pInfo->suid = pPhyNode->suid;
  pInfo->pStorageAPI = &pTaskInfo->storageAPI;

  pInfo->pTableListInfo = pTableListInfo;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  pInfo->readHandle = *pReadHandle;
  pInfo->curPos = 0;

  initLimitInfo(pPhyNode->node.pLimit, pPhyNode->node.pSlimit, &pInfo->limitInfo);
  setOperatorInfo(pOperator, "TagScanOperator", QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  if (pTagScanNode->onlyMetaCtbIdx) {
    pInfo->aUidTags = taosArrayInit(pOperator->resultInfo.capacity, sizeof(STUidTagInfo));
    pInfo->aFilterIdxs = taosArrayInit(pOperator->resultInfo.capacity, sizeof(int32_t));
    pInfo->filterCtx.colHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
    pInfo->filterCtx.cInfoList = taosArrayInit(4, sizeof(SColumnInfo));
    if (pInfo->pTagCond != NULL) {
      nodesRewriteExprPostOrder(&pTagCond, tagScanRewriteTagColumn, (void*)&pInfo->filterCtx);
    }
  }
  __optr_fn_t tagScanNextFn = (pTagScanNode->onlyMetaCtbIdx) ? doTagScanFromCtbIdx : doTagScanFromMetaEntry;
  pOperator->fpSet =
      createOperatorFpSet(optrDummyOpenFn, tagScanNextFn, NULL, destroyTagScanOperatorInfo, optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);

  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

// table merge scan operator

// table merge scan operator

static int32_t subTblRowCompareFn(const void* pLeft, const void* pRight, void* param) {
  int32_t left = *(int32_t*)pLeft;
  int32_t right = *(int32_t*)pRight;
  STmsSubTablesMergeInfo* pInfo = (STmsSubTablesMergeInfo*)param;

  int32_t leftIdx = pInfo->aInputs[left].rowIdx;
  int32_t rightIdx = pInfo->aInputs[right].rowIdx;

  if (leftIdx == -1) {
    return 1;
  } else if (rightIdx == -1) {
    return -1;
  }

  int64_t leftTs = pInfo->aInputs[left].aTs[leftIdx];
  int64_t rightTs = pInfo->aInputs[right].aTs[rightIdx];
  int32_t ret = leftTs>rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
  if (pInfo->pOrderInfo->order == TSDB_ORDER_DESC) {
    ret = -1 * ret;
  }
  return ret;
}

int32_t dumpQueryTableCond(const SQueryTableDataCond* src, SQueryTableDataCond* dst) {
  memcpy((void*)dst, (void*)src, sizeof(SQueryTableDataCond));
  dst->colList = taosMemoryCalloc(src->numOfCols, sizeof(SColumnInfo));
  for (int i = 0; i < src->numOfCols; i++) {
    dst->colList[i] = src->colList[i];
  }
  return 0;
}

static int32_t fetchNextSubTableBlockFromReader(SOperatorInfo* pOperator, STmsSubTableInput* pInput, bool* pSubTableHasBlock) {
  int32_t code = 0;

  STableMergeScanInfo* pInfo = pOperator->info;
  SReadHandle* pHandle = &pInfo->base.readHandle;
  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  const SStorageAPI* pAPI= &pTaskInfo->storageAPI;

  blockDataCleanup(pInput->pReaderBlock);
  if (!pInput->bInMemReader) {
    code = pAPI->tsdReader.tsdReaderOpen(pHandle->vnode, &pInput->tblCond, pInput->pKeyInfo, 1, pInput->pReaderBlock,
                                        (void**)&pInput->pReader, GET_TASKID(pTaskInfo), NULL);
    if (code != 0) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  pInfo->base.dataReader = pInput->pReader;

  while (true) {
    bool hasNext = false;
    int32_t code = pAPI->tsdReader.tsdNextDataBlock(pInfo->base.dataReader, &hasNext);
    if (code != 0) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->base.dataReader);
      pInfo->base.dataReader = NULL;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (!hasNext || isTaskKilled(pTaskInfo)) {
      if (isTaskKilled(pTaskInfo)) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->base.dataReader);
        pInfo->base.dataReader = NULL;
        T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
      }
      *pSubTableHasBlock = false;
      break;
    }

   if (pInput->tblCond.order == TSDB_ORDER_ASC) {
      pInput->tblCond.twindows.skey = pInput->pReaderBlock->info.window.ekey + 1;
    } else {
      pInput->tblCond.twindows.ekey = pInput->pReaderBlock->info.window.skey - 1;
    }

    uint32_t status = 0;
    code = loadDataBlock(pOperator, &pInfo->base, pInput->pReaderBlock, &status);
    if (code != 0) {
      pInfo->base.dataReader = NULL;
      T_LONG_JMP(pTaskInfo->env, code);
    }
    if (status == FUNC_DATA_REQUIRED_ALL_FILTEROUT) {
      *pSubTableHasBlock = false;
      break;
    }
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pInput->pReaderBlock->info.rows == 0) {
      continue;
    }

    *pSubTableHasBlock = true;
    break;
  }

  if (*pSubTableHasBlock) {
    pInput->pReaderBlock->info.id.groupId = tableListGetTableGroupId(pInfo->base.pTableListInfo, pInput->pReaderBlock->info.id.uid);
    pOperator->resultInfo.totalRows += pInput->pReaderBlock->info.rows;
  }
  if (!pInput->bInMemReader || !*pSubTableHasBlock) {
    pAPI->tsdReader.tsdReaderClose(pInput->pReader);
    pInput->pReader = NULL;
  }

  pInfo->base.dataReader = NULL;
  return TSDB_CODE_SUCCESS;
}

static void setGroupStartEndIndex(STableMergeScanInfo* pInfo) {
  pInfo->bGroupProcessed = false;

  size_t  numOfTables = tableListGetSize(pInfo->base.pTableListInfo);
  int32_t i = pInfo->tableStartIndex + 1;
  for (; i < numOfTables; ++i) {
    STableKeyInfo* tableKeyInfo = tableListGetInfo(pInfo->base.pTableListInfo, i);
    if (tableKeyInfo->groupId != pInfo->groupId) {
      break;
    }
  }
  pInfo->tableEndIndex = i - 1;
}

static int32_t openSubTablesMergeSort(STmsSubTablesMergeInfo* pSubTblsInfo) {
  for (int32_t i = 0; i < pSubTblsInfo->numSubTables; ++i) {
    STmsSubTableInput * pInput = pSubTblsInfo->aInputs + i;
    if (pInput->rowIdx == -1) {
      continue;
    }
    if (pInput->type == SUB_TABLE_MEM_BLOCK) {
      pInput->rowIdx = 0;
      pInput->pageIdx = -1;
    }
    SSDataBlock* pInputBlock = (pInput->type == SUB_TABLE_MEM_BLOCK) ? pInput->pReaderBlock : pInput->pPageBlock;
    SColumnInfoData* col = taosArrayGet(pInputBlock->pDataBlock, pSubTblsInfo->pOrderInfo->slotId);
    pInput->aTs = (int64_t*)col->pData;
  }
  tMergeTreeCreate(&pSubTblsInfo->pTree, pSubTblsInfo->numSubTables, pSubTblsInfo, subTblRowCompareFn);
  return  TSDB_CODE_SUCCESS;
}

static int32_t initSubTablesMergeInfo(STableMergeScanInfo* pInfo) {
  setGroupStartEndIndex(pInfo);
  STmsSubTablesMergeInfo* pSubTblsInfo = taosMemoryCalloc(1, sizeof(STmsSubTablesMergeInfo));
  if (pSubTblsInfo == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pSubTblsInfo->pOrderInfo = taosArrayGet(pInfo->pSortInfo, 0);
  pSubTblsInfo->numSubTables = pInfo->tableEndIndex - pInfo->tableStartIndex + 1;
  pSubTblsInfo->aInputs = taosMemoryCalloc(pSubTblsInfo->numSubTables, sizeof(STmsSubTableInput));
  if (pSubTblsInfo->aInputs == NULL) {
    taosMemoryFree(pSubTblsInfo);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  int32_t bufPageSize = pInfo->bufPageSize;
  int32_t inMemSize = (pSubTblsInfo->numSubTables - pSubTblsInfo->numTableBlocksInMem) * bufPageSize;
  int32_t code =
      createDiskbasedBuf(&pSubTblsInfo->pBlocksBuf, pInfo->bufPageSize, inMemSize, "blocksExternalBuf", tsTempDir);
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(pSubTblsInfo->aInputs);
    taosMemoryFree(pSubTblsInfo);
    return code;
  }
  pSubTblsInfo->numTableBlocksInMem = pSubTblsInfo->numSubTables;
  pSubTblsInfo->numInMemReaders = pSubTblsInfo->numSubTables;

  pInfo->pSubTablesMergeInfo = pSubTblsInfo;
  return TSDB_CODE_SUCCESS;
} 

static int32_t initSubTableInputs(SOperatorInfo* pOperator, STableMergeScanInfo* pInfo) {
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  SReadHandle* pHandle = &pInfo->base.readHandle;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;

  for (int32_t i = 0; i < pSubTblsInfo->numSubTables; ++i) {
    STmsSubTableInput* pInput =  pSubTblsInfo->aInputs + i;
    pInput->type = SUB_TABLE_MEM_BLOCK;
    dumpQueryTableCond(&pInfo->base.cond, &pInput->tblCond);
    pInput->pReaderBlock = createOneDataBlock(pInfo->pResBlock, false);
    pInput->pPageBlock = createOneDataBlock(pInfo->pResBlock, false);
    STableKeyInfo* keyInfo = tableListGetInfo(pInfo->base.pTableListInfo, i + pInfo->tableStartIndex);
    pInput->pKeyInfo = keyInfo;

    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (i + 1 < pSubTblsInfo->numInMemReaders) {
      pAPI->tsdReader.tsdReaderOpen(pHandle->vnode, &pInput->tblCond, keyInfo, 1, pInput->pReaderBlock,
                                  (void**)&pInput->pReader, GET_TASKID(pTaskInfo), NULL);
      pInput->bInMemReader = true;
    } else {
      pInput->pReader = NULL;
      pInput->bInMemReader = false;
    }
    bool hasNext = true;
    fetchNextSubTableBlockFromReader(pOperator, pInput, &hasNext);
    if (!hasNext) {
      pInput->rowIdx = -1;
      ++pSubTblsInfo->numSubTablesCompleted;
      continue;
    } else {
      pInput->rowIdx = 0;
      pInput->pageIdx = -1;
    }
  }  
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustSubTableFromMemBlock(SOperatorInfo* pOperatorInfo, STmsSubTablesMergeInfo* pSubTblsInfo) {
  STableMergeScanInfo* pInfo = pOperatorInfo->info;
  STmsSubTableInput* pInput = pSubTblsInfo->aInputs + tMergeTreeGetChosenIndex(pSubTblsInfo->pTree);
  bool hasNext = true;
  fetchNextSubTableBlockFromReader(pOperatorInfo, pInput, &hasNext);
  if (!hasNext) {
    pInput->rowIdx = -1;
    ++pSubTblsInfo->numSubTablesCompleted;
  } else {
    pInput->rowIdx = 0;     
  }
  
  return TSDB_CODE_SUCCESS;
}

static int32_t adjustSubTableForNextRow(SOperatorInfo* pOperatorInfo, STmsSubTablesMergeInfo* pSubTblsInfo) {
  STableMergeScanInfo* pInfo = pOperatorInfo->info;
  STmsSubTableInput* pInput = pSubTblsInfo->aInputs + tMergeTreeGetChosenIndex(pSubTblsInfo->pTree);

  SSDataBlock* pInputBlock = (pInput->type == SUB_TABLE_MEM_BLOCK) ? pInput->pReaderBlock : pInput->pPageBlock;
  if (pInput->rowIdx < pInputBlock->info.rows - 1) {
    ++pInput->rowIdx;
  } else if (pInput->rowIdx == pInputBlock->info.rows -1 ) {
    if (pInput->type == SUB_TABLE_MEM_BLOCK) {
      adjustSubTableFromMemBlock(pOperatorInfo, pSubTblsInfo);
    }
    if (pInput->rowIdx != -1) {
      SColumnInfoData* col = taosArrayGet(pInputBlock->pDataBlock, pSubTblsInfo->pOrderInfo->slotId);
      pInput->aTs = (int64_t*)col->pData;
    }
  }

  tMergeTreeAdjust(pSubTblsInfo->pTree, tMergeTreeGetAdjustIndex(pSubTblsInfo->pTree));

  return TSDB_CODE_SUCCESS;
}

static int32_t appendChosenRowToDataBlock(STmsSubTablesMergeInfo* pSubTblsInfo, SSDataBlock* pBlock) {
  STmsSubTableInput* pInput = pSubTblsInfo->aInputs + tMergeTreeGetChosenIndex(pSubTblsInfo->pTree);
  SSDataBlock* pInputBlock = (pInput->type == SUB_TABLE_MEM_BLOCK) ? pInput->pReaderBlock : pInput->pPageBlock;

  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);

    SColumnInfoData* pSrcColInfo = taosArrayGet(pInputBlock->pDataBlock, i);
    bool             isNull = colDataIsNull(pSrcColInfo, pInputBlock->info.rows, pInput->rowIdx, NULL);

    if (isNull) {
      colDataSetVal(pColInfo, pBlock->info.rows, NULL, true);
    } else {
      if (pSrcColInfo->pData != NULL) {
        char* pData = colDataGetData(pSrcColInfo, pInput->rowIdx);
        colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
      }
    }
  }
  pBlock->info.dataLoad = 1;
  pBlock->info.scanFlag = pInputBlock->info.scanFlag;
  pBlock->info.rows += 1;
  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* getSubTablesSortedBlock(SOperatorInfo* pOperator, SSDataBlock* pResBlock, int32_t capacity) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;

  blockDataCleanup(pResBlock);
  bool finished = false;
  while (true) {
    while (1) {
      if (pSubTblsInfo->numSubTablesCompleted >= pSubTblsInfo->numSubTables) {
        finished = true;
        break;
      }

      appendChosenRowToDataBlock(pSubTblsInfo, pResBlock);
      adjustSubTableForNextRow(pOperator, pSubTblsInfo);
      
      if (pResBlock->info.rows >= capacity) {
        break;
      }
    }

    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pOperator->pTaskInfo->env, pTaskInfo->code);
    }

    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pResBlock, pTaskInfo);
    if (finished || limitReached || pResBlock->info.rows > 0) {
      break;
    }
  }
  return (pResBlock->info.rows > 0) ? pResBlock : NULL;
}

static int32_t startSubTablesTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;

  initSubTablesMergeInfo(pInfo);

  initSubTableInputs(pOperator, pInfo);
  
  openSubTablesMergeSort(pInfo->pSubTablesMergeInfo);

  return TSDB_CODE_SUCCESS;
}

static int32_t stopSubTablesTableMergeScan(STableMergeScanInfo* pInfo) {
  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;
  if (pSubTblsInfo != NULL) {
    tMergeTreeDestroy(&pSubTblsInfo->pTree);

    for (int32_t i = 0; i < pSubTblsInfo->numSubTables; ++i) {
      STmsSubTableInput* pInput =  pSubTblsInfo->aInputs + i;
      taosMemoryFree(pInput->tblCond.colList);
      blockDataDestroy(pInput->pReaderBlock);
      blockDataDestroy(pInput->pPageBlock);
      taosArrayDestroy(pInput->aBlockPages);
      pInfo->base.readerAPI.tsdReaderClose(pInput->pReader);
      pInput->pReader = NULL;
    }

    destroyDiskbasedBuf(pSubTblsInfo->pBlocksBuf);
    taosMemoryFree(pSubTblsInfo->aInputs);

    taosMemoryFree(pSubTblsInfo);
    pInfo->pSubTablesMergeInfo = NULL;
  }
  taosMemoryTrim(0);
  return TSDB_CODE_SUCCESS;
}

SSDataBlock* doTableMergeScanParaSubTables(SOperatorInfo* pOperator) {
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperator->info;

  int32_t code = pOperator->fpSet._openFn(pOperator);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  size_t tableListSize = tableListGetSize(pInfo->base.pTableListInfo);
  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      setOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->tableStartIndex = 0;
    pInfo->groupId = ((STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex))->groupId;
    startSubTablesTableMergeScan(pOperator);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->tableStartIndex < tableListSize) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    pBlock = getSubTablesSortedBlock(pOperator, pInfo->pResBlock, pOperator->resultInfo.capacity);
    if (pBlock == NULL && !pInfo->bGroupProcessed && pInfo->needCountEmptyTable) {
      STableKeyInfo* tbInfo = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
      pBlock = getOneRowResultBlock(pTaskInfo, &pInfo->base, pInfo->pResBlock, tbInfo);
    }
    if (pBlock != NULL) {
      pBlock->info.id.groupId = pInfo->groupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      pInfo->bGroupProcessed = true;
      return pBlock;
    } else {
      // Data of this group are all dumped, let's try the next group
      stopSubTablesTableMergeScan(pInfo);
      if (pInfo->tableEndIndex >= tableListSize - 1) {
        setOperatorCompleted(pOperator);
        break;
      }

      pInfo->tableStartIndex = pInfo->tableEndIndex + 1;
      pInfo->groupId = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex)->groupId;
      startSubTablesTableMergeScan(pOperator);
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }
  }

  return pBlock;
}


static void tableMergeScanDoSkipTable(uint64_t uid, void* pTableMergeScanInfo) {
  STableMergeScanInfo* pInfo = pTableMergeScanInfo;
  if (pInfo->mSkipTables == NULL) {
      pInfo->mSkipTables = taosHashInit(pInfo->tableEndIndex - pInfo->tableStartIndex + 1,
                                        taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  }
  int bSkip = 1;
  if (pInfo->mSkipTables != NULL) {
    taosHashPut(pInfo->mSkipTables, &uid, sizeof(uid), &bSkip, sizeof(bSkip));
  }  
}

static void doGetBlockForTableMergeScan(SOperatorInfo* pOperator, bool* pFinished, bool* pSkipped) {
  STableMergeScanInfo*            pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  SSDataBlock*                    pBlock = pInfo->pReaderBlock;
  int32_t                         code = 0;
  bool hasNext = false;
  STsdbReader* reader = pInfo->base.dataReader;

  code = pAPI->tsdReader.tsdNextDataBlock(reader, &hasNext);
  if (code != 0) {
    pAPI->tsdReader.tsdReaderReleaseDataBlock(reader);
    qError("table merge scan fetch next data block error code: %d, %s", code, GET_TASKID(pTaskInfo));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (!hasNext || isTaskKilled(pTaskInfo)) {
    if (isTaskKilled(pTaskInfo)) {
      qInfo("table merge scan fetch next data block found task killed. %s", GET_TASKID(pTaskInfo));
      pAPI->tsdReader.tsdReaderReleaseDataBlock(reader);
    }
    *pFinished = true;
    return;
  }

  uint32_t status = 0;
  code = loadDataBlock(pOperator, &pInfo->base, pBlock, &status);
  if (code != TSDB_CODE_SUCCESS) {
    qInfo("table merge scan load datablock code %d, %s", code, GET_TASKID(pTaskInfo));
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (status == FUNC_DATA_REQUIRED_ALL_FILTEROUT) {
    *pFinished = true;
    return;
  }

  // current block is filter out according to filter condition, continue load the next block
  if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
    *pSkipped = true;
    return;
  }
  return;
}

static SSDataBlock* getBlockForTableMergeScan(void* param) {
  STableMergeScanSortSourceParam* source = param;
  SOperatorInfo*                  pOperator = source->pOperator;
  STableMergeScanInfo*            pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  SSDataBlock*                    pBlock = NULL;
  int32_t                         code = 0;

  int64_t      st = taosGetTimestampUs();
  bool         hasNext = false;

  STsdbReader* reader = pInfo->base.dataReader;
  while (true) {
    if (pInfo->rtnNextDurationBlocks) {
      qDebug("%s table merge scan return already fetched new duration blocks. index %d num of blocks %d", 
              GET_TASKID(pTaskInfo), pInfo->nextDurationBlocksIdx, pInfo->numNextDurationBlocks);
      if (pInfo->nextDurationBlocksIdx < pInfo->numNextDurationBlocks) {
        pBlock = pInfo->nextDurationBlocks[pInfo->nextDurationBlocksIdx];
        ++pInfo->nextDurationBlocksIdx;
      } else {
        for (int32_t i = 0; i < pInfo->numNextDurationBlocks; ++i) {
          blockDataDestroy(pInfo->nextDurationBlocks[i]);
          pInfo->nextDurationBlocks[i] = NULL;
        }
        pInfo->rtnNextDurationBlocks = false;
        pInfo->nextDurationBlocksIdx = 0;
        pInfo->numNextDurationBlocks = 0;
        continue;
      }
    } else {

      bool bFinished = false;
      bool bSkipped = false;
      doGetBlockForTableMergeScan(pOperator, &bFinished, &bSkipped);
      pBlock = pInfo->pReaderBlock;
      qDebug("%s table merge scan fetch block. finished %d skipped %d next-duration-block %d new-fileset %d", 
              GET_TASKID(pTaskInfo), bFinished, bSkipped, pInfo->bNextDurationBlockEvent, pInfo->bNewFilesetEvent);
      if (bFinished) {
        pInfo->bNewFilesetEvent = false;
        break;
      }

      if (pInfo->bNextDurationBlockEvent || pInfo->bNewFilesetEvent) {
        if (!bSkipped) {
          pInfo->nextDurationBlocks[pInfo->numNextDurationBlocks] = createOneDataBlock(pBlock, true);
          ++pInfo->numNextDurationBlocks;
          if (pInfo->numNextDurationBlocks > 2) {
            qError("%s table merge scan prefetch %d next duration blocks. end early.", GET_TASKID(pTaskInfo), pInfo->numNextDurationBlocks);
            pInfo->bNewFilesetEvent = false;
            break;
          }
        }
        if (pInfo->bNewFilesetEvent) {
          pInfo->rtnNextDurationBlocks = true;
          return NULL;
        }
        if (pInfo->bNextDurationBlockEvent) {
          pInfo->bNextDurationBlockEvent = false;
          continue;
        }
      }
      if (bSkipped) continue;
    }
    pBlock->info.id.groupId = tableListGetTableGroupId(pInfo->base.pTableListInfo, pBlock->info.id.uid);

    pOperator->resultInfo.totalRows += pBlock->info.rows;
    pInfo->base.readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;
    return pBlock;
  }

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

void tableMergeScanTsdbNotifyCb(ETsdReaderNotifyType type, STsdReaderNotifyInfo* info, void* param) {
  STableMergeScanInfo* pTmsInfo = param;
  if (type == TSD_READER_NOTIFY_DURATION_START) {
    pTmsInfo->bNewFilesetEvent = true;
  } else if (type == TSD_READER_NOTIFY_NEXT_DURATION_BLOCK) {
    pTmsInfo->bNextDurationBlockEvent = true;
  }
  qDebug("table merge scan receive notification. type %d, fileset %d", type, info->duration.filesetId);

  return;
}

int32_t startDurationForGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t numOfTable = pInfo->tableEndIndex - pInfo->tableStartIndex + 1;

  qDebug("%s table merge scan start duration ", GET_TASKID(pTaskInfo));
  pInfo->bNewFilesetEvent = false;
  pInfo->bNextDurationBlockEvent = false;

  pInfo->sortBufSize = 2048 * pInfo->bufPageSize;
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;
  pInfo->pSortHandle = tsortCreateSortHandle(pInfo->pSortInfo, SORT_BLOCK_TS_MERGE, pInfo->bufPageSize, numOfBufPage,
                                             pInfo->pSortInputBlock, pTaskInfo->id.str, 0, 0, 0);

  tsortSetMergeLimit(pInfo->pSortHandle, pInfo->mergeLimit);
  tsortSetMergeLimitReachedFp(pInfo->pSortHandle, tableMergeScanDoSkipTable, pInfo);
  tsortSetAbortCheckFn(pInfo->pSortHandle, isTaskKilled, pOperator->pTaskInfo);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, getBlockForTableMergeScan, NULL, NULL);

  STableMergeScanSortSourceParam *param = taosMemoryCalloc(1, sizeof(STableMergeScanSortSourceParam));
  param->pOperator = pOperator;

  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  ps->param = param;
  ps->onlyRef = false;
  tsortAddSource(pInfo->pSortHandle, ps);

  if (numOfTable == 1) {
    tsortSetSingleTableMerge(pInfo->pSortHandle);
  } else {
    code = tsortOpen(pInfo->pSortHandle);
  }
  return code;
}

void stopDurationForGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  qDebug("%s table merge scan stop duration ", GET_TASKID(pTaskInfo));

  SSortExecInfo sortExecInfo = tsortGetSortExecInfo(pInfo->pSortHandle);
  pInfo->sortExecInfo.sortMethod = sortExecInfo.sortMethod;
  pInfo->sortExecInfo.sortBuffer = sortExecInfo.sortBuffer;
  pInfo->sortExecInfo.loops += sortExecInfo.loops;
  pInfo->sortExecInfo.readBytes += sortExecInfo.readBytes;
  pInfo->sortExecInfo.writeBytes += sortExecInfo.writeBytes;

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;
}

int32_t startGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  SReadHandle* pHandle = &pInfo->base.readHandle;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;
  qDebug("%s table merge scan start group %"PRIu64, GET_TASKID(pTaskInfo), pInfo->groupId);

  {
    size_t  numOfTables = tableListGetSize(pInfo->base.pTableListInfo);
    int32_t i = pInfo->tableStartIndex + 1;
    for (; i < numOfTables; ++i) {
      STableKeyInfo* tableKeyInfo = tableListGetInfo(pInfo->base.pTableListInfo, i);
      if (tableKeyInfo->groupId != pInfo->groupId) {
        break;
      }
    }
    pInfo->tableEndIndex = i - 1;
  }
  pInfo->bGroupProcessed = false;
  int32_t tableStartIdx = pInfo->tableStartIndex;
  int32_t tableEndIdx = pInfo->tableEndIndex;

  int32_t numOfTable = tableEndIdx - tableStartIdx + 1;
  STableKeyInfo* startKeyInfo = tableListGetInfo(pInfo->base.pTableListInfo, tableStartIdx);
  pAPI->tsdReader.tsdReaderOpen(pHandle->vnode, &pInfo->base.cond, startKeyInfo, numOfTable, pInfo->pReaderBlock,
                                (void**)&pInfo->base.dataReader, GET_TASKID(pTaskInfo), &pInfo->mSkipTables);
  if (pInfo->filesetDelimited) {
    pAPI->tsdReader.tsdSetFilesetDelimited(pInfo->base.dataReader);
  }
  pAPI->tsdReader.tsdSetSetNotifyCb(pInfo->base.dataReader, tableMergeScanTsdbNotifyCb, pInfo);

  int32_t code = startDurationForGroupTableMergeScan(pOperator);

  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, terrno);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t stopGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*         pAPI = &pTaskInfo->storageAPI;

  stopDurationForGroupTableMergeScan(pOperator);

  if (pInfo->base.dataReader != NULL) {
    pAPI->tsdReader.tsdReaderClose(pInfo->base.dataReader);
    pInfo->base.dataReader = NULL;
  }
  for (int32_t i = 0; i < pInfo->numNextDurationBlocks; ++i) {
    if (pInfo->nextDurationBlocks[i]) {
      blockDataDestroy(pInfo->nextDurationBlocks[i]);
      pInfo->nextDurationBlocks[i] = NULL;
    }
    pInfo->numNextDurationBlocks = 0;
    pInfo->nextDurationBlocksIdx = 0;
  }
  resetLimitInfoForNextGroup(&pInfo->limitInfo);
  taosHashCleanup(pInfo->mSkipTables);
  pInfo->mSkipTables = NULL;
  qDebug("%s table merge scan stop group %"PRIu64, GET_TASKID(pTaskInfo), pInfo->groupId);

  return TSDB_CODE_SUCCESS;
}

// all data produced by this function only belongs to one group
// slimit/soffset does not need to be concerned here, since this function only deal with data within one group.
SSDataBlock* getSortedTableMergeScanBlockData(SSortHandle* pHandle, SSDataBlock* pResBlock, int32_t capacity,
                                              SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  blockDataCleanup(pResBlock);
  STupleHandle* pTupleHandle = NULL;
  while (1) {
    while (1) {
      pTupleHandle = tsortNextTuple(pHandle);
      if (pTupleHandle == NULL) {
        break;
      }

      appendOneRowToDataBlock(pResBlock, pTupleHandle);
      if (pResBlock->info.rows >= capacity) {
        break;
      }
    }

    if (tsortIsClosed(pHandle)) {
      terrno = TSDB_CODE_TSC_QUERY_CANCELLED;
      T_LONG_JMP(pOperator->pTaskInfo->env, terrno);
    }
    
    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pResBlock, pTaskInfo);
    qDebug("%s get sorted row block, rows:%" PRId64 ", limit:%" PRId64, GET_TASKID(pTaskInfo), pResBlock->info.rows,
          pInfo->limitInfo.numOfOutputRows);
    if (pTupleHandle == NULL || limitReached || pResBlock->info.rows > 0) {
      break;
    }  
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

  size_t tableListSize = tableListGetSize(pInfo->base.pTableListInfo);
  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      setOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->tableStartIndex = 0;
    pInfo->groupId = ((STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex))->groupId;
    startGroupTableMergeScan(pOperator);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->tableStartIndex < tableListSize) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    pBlock = getSortedTableMergeScanBlockData(pInfo->pSortHandle, pInfo->pResBlock, pOperator->resultInfo.capacity,
                                              pOperator);
    if (pBlock == NULL && !pInfo->bGroupProcessed && pInfo->needCountEmptyTable) {
      STableKeyInfo* tbInfo = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
      pBlock = getOneRowResultBlock(pTaskInfo, &pInfo->base, pInfo->pResBlock, tbInfo);
    }
    if (pBlock != NULL) {
      pBlock->info.id.groupId = pInfo->groupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      pInfo->bGroupProcessed = true;
      return pBlock;
    } else {
      if (pInfo->bNewFilesetEvent) {
        stopDurationForGroupTableMergeScan(pOperator);
        startDurationForGroupTableMergeScan(pOperator);
      } else {
        // Data of this group are all dumped, let's try the next group
        stopGroupTableMergeScan(pOperator);
        if (pInfo->tableEndIndex >= tableListSize - 1) {
          setOperatorCompleted(pOperator);
          break;
        }

        pInfo->tableStartIndex = pInfo->tableEndIndex + 1;
        pInfo->groupId = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex)->groupId;
        startGroupTableMergeScan(pOperator);
        resetLimitInfoForNextGroup(&pInfo->limitInfo);
      }
    }
  }

  return pBlock;
}

void destroyTableMergeScanOperatorInfo(void* param) {
  STableMergeScanInfo* pTableScanInfo = (STableMergeScanInfo*)param;

  // start one reader variable
  pTableScanInfo->base.readerAPI.tsdReaderClose(pTableScanInfo->base.dataReader);
  pTableScanInfo->base.dataReader = NULL;

  for (int32_t i = 0; i < pTableScanInfo->numNextDurationBlocks; ++i) {
    if (pTableScanInfo->nextDurationBlocks[i] != NULL) {
      blockDataDestroy(pTableScanInfo->nextDurationBlocks[i]);
      pTableScanInfo->nextDurationBlocks[i] = NULL;
    }
  }

  tsortDestroySortHandle(pTableScanInfo->pSortHandle);
  pTableScanInfo->pSortHandle = NULL;
  taosHashCleanup(pTableScanInfo->mSkipTables);
  pTableScanInfo->mSkipTables = NULL;
  pTableScanInfo->pSortInputBlock = blockDataDestroy(pTableScanInfo->pSortInputBlock);
  // end one reader variable

  cleanupQueryTableDataCond(&pTableScanInfo->base.cond);
  destroyTableScanBase(&pTableScanInfo->base, &pTableScanInfo->base.readerAPI);

  pTableScanInfo->pResBlock = blockDataDestroy(pTableScanInfo->pResBlock);
  pTableScanInfo->pReaderBlock = blockDataDestroy(pTableScanInfo->pReaderBlock);
  taosArrayDestroy(pTableScanInfo->pSortInfo);

  stopSubTablesTableMergeScan(pTableScanInfo);

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
                                                STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo) {
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

  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode, readHandle);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pInfo->base.matchInfo.pList);
    goto _error;
  }

  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->base.pseudoSup;
    pSup->pExprInfo = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset, &pTaskInfo->storageAPI.functionStore);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};

  pInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
  if (pInfo->base.metaCache.pTableMetaEntryCache == NULL) {
    code = terrno;
    goto _error;
  }

  pInfo->base.readerAPI = pTaskInfo->storageAPI.tsdReader;
  pInfo->base.dataBlockLoadFlag = FUNC_DATA_REQUIRED_DATA_LOAD;
  pInfo->base.scanFlag = MAIN_SCAN;
  pInfo->base.readHandle = *readHandle;

  pInfo->readIdx = -1;

  pInfo->base.limitInfo.limit.limit = -1;
  pInfo->base.limitInfo.slimit.limit = -1;
  pInfo->base.pTableListInfo = pTableListInfo;

  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initLimitInfo(pTableScanNode->scan.node.pLimit, pTableScanNode->scan.node.pSlimit, &pInfo->limitInfo);

  pInfo->mergeLimit = -1;
  bool hasLimit = pInfo->limitInfo.limit.limit != -1 || pInfo->limitInfo.limit.offset != -1;
  if (hasLimit) {
    pInfo->mergeLimit = pInfo->limitInfo.limit.limit + pInfo->limitInfo.limit.offset;
    pInfo->mSkipTables = NULL;
  }
  
  initResultSizeInfo(&pOperator->resultInfo, 1024);
  pInfo->pResBlock = createDataBlockFromDescNode(pDescNode);
  blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);

  pInfo->pSortInfo = generateSortByTsInfo(pInfo->base.matchInfo.pList, pInfo->base.cond.order);
  pInfo->pReaderBlock = createOneDataBlock(pInfo->pResBlock, false);

  pInfo->needCountEmptyTable = tsCountAlwaysReturnValue && pTableScanNode->needCountEmptyTable;

  int32_t  rowSize = pInfo->pResBlock->info.rowSize;
  uint32_t nCols = taosArrayGetSize(pInfo->pResBlock->pDataBlock);
  pInfo->bufPageSize = getProperSortPageSize(rowSize, nCols);

  //start one reader variable
  pInfo->pSortInputBlock = createOneDataBlock(pInfo->pResBlock, false);

  if (!tsExperimental) {
    pInfo->filesetDelimited = false;
  } else {
    pInfo->filesetDelimited = pTableScanNode->filesetDelimited;
  }
  // end one reader variable
  setOperatorInfo(pOperator, "TableMergeScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;

  pOperator->fpSet = createOperatorFpSet(
      optrDummyOpenFn, pTableScanNode->paraTablesSort ? doTableMergeScanParaSubTables : doTableMergeScan, NULL,
      destroyTableMergeScanOperatorInfo, optrDefaultBufFn, getTableMergeScanExplainExecInfo, optrDefaultGetNextExtFn,
      NULL);
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
                                                   SSDataBlock* pRes, char* dbName, tb_uid_t stbUid, SStorageAPI* pAPI);
static void         buildVnodeGroupedNtbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                                   SSDataBlock* pRes, char* dbName, SStorageAPI* pAPI);
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
    tstrncpy(supp->dbNameFilter, tNameGetDbNameP(tableName), TSDB_DB_NAME_LEN);
    tstrncpy(supp->stbNameFilter, tNameGetTableName(tableName), TSDB_TABLE_NAME_LEN);
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
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
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
    colDataSetVal(colInfoData, 0, varDbName, false);
  }

  if (pSupp->stbNameSlotId != -1) {
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->stbNameSlotId);
    if (strlen(stbName) != 0) {
      char varStbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      strncpy(varDataVal(varStbName), stbName, TSDB_TABLE_NAME_LEN);
      varDataSetLen(varStbName, strlen(stbName));
      colDataSetVal(colInfoData, 0, varStbName, false);
    } else {
      colDataSetNULL(colInfoData, 0);
    }
  }

  if (pSupp->tbCountSlotId != -1) {
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->tbCountSlotId);
    colDataSetVal(colInfoData, 0, (char*)&count, false);
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

  if (pSupp->groupByDbName || pSupp->groupByStbName) {
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
    uint64_t groupId = 0;
    if (pSupp->groupByDbName) {
      groupId = calcGroupId(TSDB_INFORMATION_SCHEMA_DB, strlen(TSDB_INFORMATION_SCHEMA_DB));
    } else {
      groupId = calcGroupId("", 0);
    }

    pRes->info.id.groupId = groupId;
    fillTableCountScanDataBlock(pSupp, TSDB_INFORMATION_SCHEMA_DB, "", infodbTableNum, pRes);
  } else if (pInfo->currGrpIdx == 1) {
    uint64_t groupId = 0;
    if (pSupp->groupByDbName) {
      groupId = calcGroupId(TSDB_PERFORMANCE_SCHEMA_DB, strlen(TSDB_PERFORMANCE_SCHEMA_DB));
    } else {
      groupId = calcGroupId("", 0);
    }

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
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  // get dbname
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);
  SName sn = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  tNameGetDbName(&sn, dbName);

  if (pSupp->groupByDbName || pSupp->groupByStbName) {
    buildVnodeGroupedTableCount(pOperator, pInfo, pSupp, pRes, vgId, dbName);
  } else {
    buildVnodeFilteredTbCount(pOperator, pInfo, pSupp, pRes, dbName);
  }
  return pRes->info.rows > 0 ? pRes : NULL;
}

static void buildVnodeGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                        STableCountScanSupp* pSupp, SSDataBlock* pRes, int32_t vgId, char* dbName) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  if (pSupp->groupByStbName) {
    if (pInfo->stbUidList == NULL) {
      pInfo->stbUidList = taosArrayInit(16, sizeof(tb_uid_t));
      if (pAPI->metaFn.storeGetTableList(pInfo->readHandle.vnode, TSDB_SUPER_TABLE, pInfo->stbUidList) < 0) {
        qError("vgId:%d, failed to get stb id list error: %s", vgId, terrstr());
      }
    }
    if (pInfo->currGrpIdx < taosArrayGetSize(pInfo->stbUidList)) {
      tb_uid_t stbUid = *(tb_uid_t*)taosArrayGet(pInfo->stbUidList, pInfo->currGrpIdx);
      buildVnodeGroupedStbTableCount(pInfo, pSupp, pRes, dbName, stbUid, pAPI);

      pInfo->currGrpIdx++;
    } else if (pInfo->currGrpIdx == taosArrayGetSize(pInfo->stbUidList)) {
      buildVnodeGroupedNtbTableCount(pInfo, pSupp, pRes, dbName, pAPI);

      pInfo->currGrpIdx++;
    } else {
      setOperatorCompleted(pOperator);
    }
  } else {
    uint64_t groupId = calcGroupId(dbName, strlen(dbName));
    pRes->info.id.groupId = groupId;

    int64_t dbTableCount = 0;
    pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, &dbTableCount, NULL);
    fillTableCountScanDataBlock(pSupp, dbName, "", dbTableCount, pRes);
    setOperatorCompleted(pOperator);
  }
}

static void buildVnodeFilteredTbCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                      STableCountScanSupp* pSupp, SSDataBlock* pRes, char* dbName) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;

  if (strlen(pSupp->dbNameFilter) != 0) {
    if (strlen(pSupp->stbNameFilter) != 0) {
      uint64_t uid = 0;
      pAPI->metaFn.getTableUidByName(pInfo->readHandle.vnode, pSupp->stbNameFilter, &uid);

      int64_t numOfChildTables = 0;
      pAPI->metaFn.getNumOfChildTables(pInfo->readHandle.vnode, uid, &numOfChildTables, NULL);

      fillTableCountScanDataBlock(pSupp, dbName, pSupp->stbNameFilter, numOfChildTables, pRes);
    } else {
      int64_t tbNumVnode = 0;
      pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, &tbNumVnode, NULL);
      fillTableCountScanDataBlock(pSupp, dbName, "", tbNumVnode, pRes);
    }
  } else {
    int64_t tbNumVnode = 0;
    pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, &tbNumVnode, NULL);
    fillTableCountScanDataBlock(pSupp, dbName, "", tbNumVnode, pRes);
  }

  setOperatorCompleted(pOperator);
}

static void buildVnodeGroupedNtbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                           SSDataBlock* pRes, char* dbName, SStorageAPI* pAPI) {
  char fullStbName[TSDB_TABLE_FNAME_LEN] = {0};
  if (pSupp->groupByDbName) {
    snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbName, "");
  }

  uint64_t groupId = calcGroupId(fullStbName, strlen(fullStbName));
  pRes->info.id.groupId = groupId;

  int64_t numOfTables = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, NULL, &numOfTables);

  if (numOfTables != 0) {
    fillTableCountScanDataBlock(pSupp, dbName, "", numOfTables, pRes);
  }
}

static void buildVnodeGroupedStbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                           SSDataBlock* pRes, char* dbName, tb_uid_t stbUid, SStorageAPI* pAPI) {
  char stbName[TSDB_TABLE_NAME_LEN] = {0};
  pAPI->metaFn.getTableNameByUid(pInfo->readHandle.vnode, stbUid, stbName);

  char fullStbName[TSDB_TABLE_FNAME_LEN] = {0};
  if (pSupp->groupByDbName) {
    snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbName, varDataVal(stbName));
  } else {
    snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s", varDataVal(stbName));
  }

  uint64_t groupId = calcGroupId(fullStbName, strlen(fullStbName));
  pRes->info.id.groupId = groupId;

  int64_t ctbNum = 0;
  int32_t code = pAPI->metaFn.getNumOfChildTables(pInfo->readHandle.vnode, stbUid, &ctbNum, NULL);
  fillTableCountScanDataBlock(pSupp, dbName, varDataVal(stbName), ctbNum, pRes);
}

static void destoryTableCountScanOperator(void* param) {
  STableCountScanOperatorInfo* pTableCountScanInfo = param;
  blockDataDestroy(pTableCountScanInfo->pRes);

  taosArrayDestroy(pTableCountScanInfo->stbUidList);
  taosMemoryFreeClear(param);
}
