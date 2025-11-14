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

#include "executor.h"
#include "executorInt.h"
#include "filter.h"
#include "function.h"
#include "functionMgt.h"
#include "os.h"
#include "querynodes.h"
#include "streamexecutorInt.h"
#include "systable.h"
#include "tname.h"

#include "tdatablock.h"
#include "tmsg.h"
#include "ttime.h"

#include "operator.h"
#include "query.h"
#include "querytask.h"
#include "tcompare.h"
#include "thash.h"
#include "ttypes.h"

#include "function.h"
#include "storageapi.h"
#include "wal.h"

int32_t scanDebug = 0;

#define MULTI_READER_MAX_TABLE_NUM     5000
#define SET_REVERSE_SCAN_FLAG(_info)   ((_info)->scanFlag = REVERSE_SCAN)
#define SWITCH_ORDER(n)                (((n) = ((n) == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC))
#define STREAM_SCAN_OP_NAME            "StreamScanOperator"
#define STREAM_SCAN_OP_STATE_NAME      "StreamScanFillHistoryState"
#define STREAM_SCAN_OP_CHECKPOINT_NAME "StreamScanOperator_Checkpoint"

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

static bool    processBlockWithProbability(const SSampleExecInfo* pInfo);
static int32_t doTableCountScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes);

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

static int32_t overlapWithTimeWindow(SInterval* pInterval, SDataBlockInfo* pBlockInfo, int32_t order, bool* overlap) {
  int32_t     code = TSDB_CODE_SUCCESS;
  STimeWindow w = {0};

  // 0 by default, which means it is not a interval operator of the upstream operator.
  if (pInterval->interval == 0) {
    *overlap = false;
    return code;
  }

  if (order == TSDB_ORDER_ASC) {
    w = getAlignQueryTimeWindow(pInterval, pBlockInfo->window.skey);
    if (w.ekey < pBlockInfo->window.skey) {
      qError("w.ekey:%" PRId64 " < pBlockInfo->window.skey:%" PRId64, w.ekey, pBlockInfo->window.skey);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    if (w.ekey < pBlockInfo->window.ekey) {
      *overlap = true;
      return code;
    }

    while (1) {
      getNextTimeWindow(pInterval, &w, order);
      if (w.skey > pBlockInfo->window.ekey) {
        break;
      }

      if (w.ekey <= pBlockInfo->window.ekey) {
        qError("w.ekey:%" PRId64 " <= pBlockInfo->window.ekey:%" PRId64, w.ekey, pBlockInfo->window.ekey);
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      if (TMAX(w.skey, pBlockInfo->window.skey) <= pBlockInfo->window.ekey) {
        *overlap = true;
        return code;
      }
    }
  } else {
    w = getAlignQueryTimeWindow(pInterval, pBlockInfo->window.ekey);
    if (w.skey > pBlockInfo->window.ekey) {
      qError("w.skey:%" PRId64 " > pBlockInfo->window.skey:%" PRId64, w.skey, pBlockInfo->window.ekey);
      return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
    }

    if (w.skey > pBlockInfo->window.skey) {
      *overlap = true;
      return code;
    }

    while (1) {
      getNextTimeWindow(pInterval, &w, order);
      if (w.ekey < pBlockInfo->window.skey) {
        break;
      }

      if (w.skey >= pBlockInfo->window.skey) {
        qError("w.skey:%" PRId64 " >= pBlockInfo->window.skey:%" PRId64, w.skey, pBlockInfo->window.skey);
        return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
      }
      if (pBlockInfo->window.skey <= TMIN(w.ekey, pBlockInfo->window.ekey)) {
        *overlap = true;
        return code;
      }
    }
  }

  *overlap = false;
  return code;
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
    pTableScanInfo->pIgnoreTables =
        taosHashInit(tableNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (NULL == pTableScanInfo->pIgnoreTables) {
      return terrno;
    }
  }

  int32_t tempRes = taosHashPut(pTableScanInfo->pIgnoreTables, &uid, sizeof(uid), &pTableScanInfo->scanTimes,
                                sizeof(pTableScanInfo->scanTimes));
  if (tempRes != TSDB_CODE_SUCCESS && tempRes != TSDB_CODE_DUP_KEY) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(tempRes));
    return tempRes;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t doDynamicPruneDataBlock(SOperatorInfo* pOperator, SDataBlockInfo* pBlockInfo, uint32_t* status) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  int32_t         code = TSDB_CODE_SUCCESS;

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

    EFuncDataRequired reqStatus = fmFuncDynDataRequired(functionId, pEntry, pBlockInfo);
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

static int32_t doFilterByBlockSMA(SFilterInfo* pFilterInfo, SColumnDataAgg* pColsAgg, int32_t numOfCols,
                                  int32_t numOfRows, bool* keep) {
  if (pColsAgg == NULL || pFilterInfo == NULL) {
    *keep = true;
    return TSDB_CODE_SUCCESS;
  }

  return filterRangeExecute(pFilterInfo, pColsAgg, numOfCols, numOfRows, keep);
}

static int32_t doLoadBlockSMA(STableScanBase* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo,
                              bool* pLoad) {
  SStorageAPI* pAPI = &pTaskInfo->storageAPI;
  bool         allColumnsHaveAgg = true;
  bool         hasNullSMA = false;
  if (pLoad != NULL) {
    *pLoad = false;
  }

  int32_t code = pAPI->tsdReader.tsdReaderRetrieveBlockSMAInfo(pTableScanInfo->dataReader, pBlock, &allColumnsHaveAgg,
                                                               &hasNullSMA);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (!allColumnsHaveAgg || hasNullSMA) {
    *pLoad = false;
  } else {
    *pLoad = true;
  }

  return code;
}

static int32_t doSetTagColumnData(STableScanBase* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo,
                                  int32_t rows) {
  int32_t    code = 0;
  SExprSupp* pSup = &pTableScanInfo->pseudoSup;
  if (pSup->numOfExprs > 0) {
    code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock, rows,
                                  pTaskInfo, &pTableScanInfo->metaCache);
    // ignore the table not exists error, since this table may have been dropped during the scan procedure.
    if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
      if (pTaskInfo->streamInfo.pState) blockDataCleanup(pBlock);
      code = 0;
    }
  }

  return code;
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
      int32_t code = blockDataTrimFirstRows(pBlock, pLimitInfo->remainOffset);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        pTaskInfo->code = code;
        T_LONG_JMP(pTaskInfo->env, code);
      }
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

static bool isDynVtbScan(SOperatorInfo* pOperator) {
  return pOperator->dynamicTask && ((STableScanInfo*)(pOperator->info))->virtualStableScan;
}

static int32_t loadDataBlock(SOperatorInfo* pOperator, STableScanBase* pTableScanInfo, SSDataBlock* pBlock,
                             uint32_t* status) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;
  bool           loadSMA = false;

  SFileBlockLoadRecorder* pCost = &pTableScanInfo->readRecorder;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;
  *status = pTableScanInfo->dataBlockLoadFlag;

  if (pOperator->exprSupp.pFilterInfo != NULL) {
    (*status) = FUNC_DATA_REQUIRED_DATA_LOAD;
  } else {
    bool overlap = false;
    int  ret =
        overlapWithTimeWindow(&pTableScanInfo->pdInfo.interval, &pBlock->info, pTableScanInfo->cond.order, &overlap);
    if (ret != TSDB_CODE_SUCCESS) {
      return ret;
    }
    if (overlap) {
      (*status) = FUNC_DATA_REQUIRED_DATA_LOAD;
    }
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
    code = doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);
    pCost->skipBlocks += 1;
    pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
    return code;
  } else if (*status == FUNC_DATA_REQUIRED_SMA_LOAD) {
    pCost->loadBlockStatis += 1;
    loadSMA = true;  // mark the operation of load sma;
    bool success = true;
    code = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo, &success);
    if (code) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
      qError("%s failed to retrieve sma info", GET_TASKID(pTaskInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (success) {  // failed to load the block sma data, data block statistics does not exist, load data block instead
      qDebug("%s data block SMA loaded, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64, GET_TASKID(pTaskInfo),
             pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
      code = doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
      return code;
    } else {
      qDebug("%s failed to load SMA, since not all columns have SMA", GET_TASKID(pTaskInfo));
      *status = FUNC_DATA_REQUIRED_DATA_LOAD;
    }
  }

  if (*status != FUNC_DATA_REQUIRED_DATA_LOAD) {
    pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
    qError("%s loadDataBlock invalid status:%d", GET_TASKID(pTaskInfo), *status);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  // try to filter data block according to sma info
  if (pOperator->exprSupp.pFilterInfo != NULL && (!loadSMA)) {
    bool success = true;
    code = doLoadBlockSMA(pTableScanInfo, pBlock, pTaskInfo, &success);
    if (code) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
      qError("%s failed to retrieve sma info", GET_TASKID(pTaskInfo));
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (success) {
      size_t size = taosArrayGetSize(pBlock->pDataBlock);
      bool   keep = false;
      code = doFilterByBlockSMA(pOperator->exprSupp.pFilterInfo, pBlock->pBlockAgg, size, pBlockInfo->rows, &keep);
      if (code) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
        qError("%s failed to do filter by block sma, code:%s", GET_TASKID(pTaskInfo), tstrerror(code));
        QUERY_CHECK_CODE(code, lino, _end);
      }

      if (!keep) {
        qDebug("%s data block filter out by block SMA, brange:%" PRId64 "-%" PRId64 ", rows:%" PRId64,
               GET_TASKID(pTaskInfo), pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
        pCost->filterOutBlocks += 1;
        (*status) = FUNC_DATA_REQUIRED_FILTEROUT;
        taosMemoryFreeClear(pBlock->pBlockAgg);

        pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  // free the sma info, since it should not be involved in *later computing process.
  taosMemoryFreeClear(pBlock->pBlockAgg);

  // try to filter data block according to current results
  code = doDynamicPruneDataBlock(pOperator, pBlockInfo, status);
  if (code) {
    pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->dataReader);
    QUERY_CHECK_CODE(code, lino, _end);
  }

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

  SSDataBlock* p = NULL;
  code = pAPI->tsdReader.tsdReaderRetrieveDataBlock(pTableScanInfo->dataReader, &p);
  if (p == NULL || code != TSDB_CODE_SUCCESS || p != pBlock) {
    return code;
  }

  if ((pOperator->operatorType == QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN) &&
      ((STableScanInfo*)pOperator->info)->ignoreTag) {
    // do nothing
  } else {
    // dyn vtb scan do not read tag from origin tables.
    code = doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, pBlock->info.rows);
    if (code) {
      return code;
    }
  }

  // restore the previous value
  pCost->totalRows -= pBlock->info.rows;

  if (pOperator->exprSupp.pFilterInfo != NULL) {
    code = doFilter(pBlock, pOperator->exprSupp.pFilterInfo, &pTableScanInfo->matchInfo, NULL);
    QUERY_CHECK_CODE(code, lino, _end);

    int64_t st = taosGetTimestampUs();
    double  el = (taosGetTimestampUs() - st) / 1000.0;
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void prepareForDescendingScan(STableScanBase* pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);

  switchCtxOrder(pCtx, numOfOutput);
  pTableScanInfo->cond.order = TSDB_ORDER_DESC;
  // STimeWindow* pTWindow = &pTableScanInfo->cond.twindows;
  // TSWAP(pTWindow->skey, pTWindow->ekey);
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

static int32_t createTableCacheVal(const SMetaReader* pMetaReader, STableCachedVal** ppResVal) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  STableCachedVal* pVal = taosMemoryMalloc(sizeof(STableCachedVal));
  QUERY_CHECK_NULL(pVal, code, lino, _end, terrno);

  pVal->pTags = NULL;
  pVal->pName = taosStrdup(pMetaReader->me.name);
  QUERY_CHECK_NULL(pVal->pName, code, lino, _end, terrno);

  // only child table has tag value
  if (pMetaReader->me.type == TSDB_CHILD_TABLE || pMetaReader->me.type == TSDB_VIRTUAL_CHILD_TABLE) {
    STag* pTag = (STag*)pMetaReader->me.ctbEntry.pTags;
    pVal->pTags = taosMemoryMalloc(pTag->len);
    QUERY_CHECK_NULL(pVal->pTags, code, lino, _end, terrno);
    memcpy(pVal->pTags, pTag, pTag->len);
  }

  (*ppResVal) = pVal;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    freeTableCachedVal(pVal);
  }
  return code;
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

static void freeTableCachedValObj(STableCachedVal* pVal) {
  taosMemoryFree((void*)pVal->pName);
  taosMemoryFree(pVal->pTags);
}

int32_t addTagPseudoColumnData(SReadHandle* pHandle, const SExprInfo* pExpr, int32_t numOfExpr, SSDataBlock* pBlock,
                               int32_t rows, SExecTaskInfo* pTask, STableMetaCacheInfo* pCache) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  bool             freeReader = false;
  LRUHandle*       h = NULL;
  STableCachedVal  val = {0};
  SMetaReader      mr = {0};
  const char*      idStr = pTask->id.str;
  int32_t          insertRet = TAOS_LRU_STATUS_OK;
  STableCachedVal* pVal = NULL;

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
  if (pCache == NULL || pCache->pTableMetaEntryCache == NULL) {
    pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
    code = pHandle->api.metaReaderFn.getEntryGetUidCache(&mr, pBlock->info.id.uid);
    if (code != TSDB_CODE_SUCCESS) {
      // when encounter the TSDB_CODE_PAR_TABLE_NOT_EXIST error, we proceed.
      if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
        qWarn("failed to get table meta, table may have been dropped, uid:0x%" PRIx64 ", code:%s, %s",
              pBlock->info.id.uid, tstrerror(code), idStr);

        // append null value before return to caller, since the caller will ignore this error code and proceed
        doSetNullValue(pBlock, pExpr, numOfExpr);
      } else {
        qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.id.uid, tstrerror(code), idStr);
      }
      pHandle->api.metaReaderFn.clearReader(&mr);
      return code;
    }

    pHandle->api.metaReaderFn.readerReleaseLock(&mr);

    val.pName = mr.me.name;
    val.pTags = (STag*)mr.me.ctbEntry.pTags;

    freeReader = true;
  } else {
    pCache->metaFetch += 1;

    h = taosLRUCacheLookup(pCache->pTableMetaEntryCache, &pBlock->info.id.uid, sizeof(pBlock->info.id.uid));
    if (h == NULL) {
      pHandle->api.metaReaderFn.initReader(&mr, pHandle->vnode, META_READER_LOCK, &pHandle->api.metaFn);
      freeReader = true;
      code = pHandle->api.metaReaderFn.getEntryGetUidCache(&mr, pBlock->info.id.uid);
      if (code != TSDB_CODE_SUCCESS) {
        if (code == TSDB_CODE_PAR_TABLE_NOT_EXIST) {
          qWarn("failed to get table meta, table may have been dropped, uid:0x%" PRIx64 ", code:%s, %s",
                pBlock->info.id.uid, tstrerror(code), idStr);
          // append null value before return to caller, since the caller will ignore this error code and proceed
          doSetNullValue(pBlock, pExpr, numOfExpr);
        } else {
          qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.id.uid, tstrerror(code),
                 idStr);
        }
        pHandle->api.metaReaderFn.clearReader(&mr);
        return code;
      }

      pHandle->api.metaReaderFn.readerReleaseLock(&mr);

      code = createTableCacheVal(&mr, &pVal);
      QUERY_CHECK_CODE(code, lino, _end);

      val = *pVal;
    } else {
      pCache->cacheHit += 1;
      STableCachedVal* pValTmp = taosLRUCacheValue(pCache->pTableMetaEntryCache, h);
      val = *pValTmp;

      bool bRes = taosLRUCacheRelease(pCache->pTableMetaEntryCache, h, false);
      qTrace("release LRU cache, res %d", bRes);
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
        code = setTbNameColData(pBlock, pColInfoData, functionId, val.pName);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (fType == FUNCTION_TYPE_VGID) {
        code = setVgIdColData(pBlock, pColInfoData, functionId, pTask->id.vgId);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (fType == FUNCTION_TYPE_VGVER) {
        code = setVgVerColData(pBlock, pColInfoData, functionId, pBlock->info.version);
        QUERY_CHECK_CODE(code, lino, _end);
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
        code = colDataSetNItems(pColInfoData, 0, data, pBlock->info.rows, 1, false);
        if (IS_VAR_DATA_TYPE(((const STagVal*)p)->type)) {
          char* tmp = taosMemoryCalloc(1, varDataLen(data) + 1);
          if (tmp != NULL) {
            memcpy(tmp, varDataVal(data), varDataLen(data));
            qDebug("get tag value:%s, cid:%d, table name:%s, uid%" PRId64, tmp, tagVal.cid, val.pName,
                   pBlock->info.id.uid);
            taosMemoryFree(tmp);
          }
          taosMemoryFree(data);
        }
        QUERY_CHECK_CODE(code, lino, _end);
      } else {  // todo opt for json tag
        for (int32_t i = 0; i < pBlock->info.rows; ++i) {
          code = colDataSetVal(pColInfoData, i, data, false);
          QUERY_CHECK_CODE(code, lino, _end);
        }
      }
    }
  }

  // restore the rows
  pBlock->info.rows = backupRows;

_end:

  if (NULL != pVal) {
    insertRet =
        taosLRUCacheInsert(pCache->pTableMetaEntryCache, &pBlock->info.id.uid, sizeof(uint64_t), pVal,
                           sizeof(STableCachedVal), freeCachedMetaItem, NULL, NULL, TAOS_LRU_PRIORITY_LOW, NULL);
    if (insertRet != TAOS_LRU_STATUS_OK) {
      qWarn("failed to put meta into lru cache, code:%d, %s", insertRet, idStr);
    }
  }

  if (freeReader) {
    pHandle->api.metaReaderFn.clearReader(&mr);
  }
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t setTbNameColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId,
                         const char* name) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  struct SScalarFuncExecFuncs fpSet = {0};
  code = fmGetScalarFuncExecFuncs(functionId, &fpSet);
  QUERY_CHECK_CODE(code, lino, _end);

  size_t len = TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE;
  char   buf[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(buf, name)

  SColumnInfoData infoData = createColumnInfoData(TSDB_DATA_TYPE_VARCHAR, len, 1);

  code = colInfoDataEnsureCapacity(&infoData, 1, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = colDataSetVal(&infoData, 0, buf, false);
  QUERY_CHECK_CODE(code, lino, _end);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    code = fpSet.process(&srcParam, 1, &param);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

  colDataDestroy(&infoData);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t setVgIdColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, int32_t vgId) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  struct SScalarFuncExecFuncs fpSet = {0};
  code = fmGetScalarFuncExecFuncs(functionId, &fpSet);
  QUERY_CHECK_CODE(code, lino, _end);

  SColumnInfoData infoData = createColumnInfoData(pColInfoData->info.type, pColInfoData->info.bytes, 1);

  code = colInfoDataEnsureCapacity(&infoData, 1, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = colDataSetVal(&infoData, 0, (const char*)&vgId, false);
  QUERY_CHECK_CODE(code, lino, _end);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    code = fpSet.process(&srcParam, 1, &param);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

_end:
  colDataDestroy(&infoData);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t setVgVerColData(const SSDataBlock* pBlock, SColumnInfoData* pColInfoData, int32_t functionId, int64_t vgVer) {
  int32_t                     code = TSDB_CODE_SUCCESS;
  int32_t                     lino = 0;
  struct SScalarFuncExecFuncs fpSet = {0};
  code = fmGetScalarFuncExecFuncs(functionId, &fpSet);
  QUERY_CHECK_CODE(code, lino, _end);

  SColumnInfoData infoData = createColumnInfoData(pColInfoData->info.type, pColInfoData->info.bytes, 1);

  code = colInfoDataEnsureCapacity(&infoData, 1, false);
  QUERY_CHECK_CODE(code, lino, _end);

  code = colDataSetVal(&infoData, 0, (const char*)&vgVer, false);
  QUERY_CHECK_CODE(code, lino, _end);

  SScalarParam srcParam = {.numOfRows = pBlock->info.rows, .columnData = &infoData};
  SScalarParam param = {.columnData = pColInfoData};

  if (fpSet.process != NULL) {
    code = fpSet.process(&srcParam, 1, &param);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    qError("failed to get the corresponding callback function, functionId:%d", functionId);
  }

_end:
  colDataDestroy(&infoData);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initNextGroupScan(STableScanInfo* pInfo, STableKeyInfo** pKeyInfo, int32_t* size) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  code = tableListGetGroupList(pInfo->base.pTableListInfo, pInfo->currentGroupId, pKeyInfo, size);
  QUERY_CHECK_CODE(code, lino, _end);

  pInfo->tableStartIndex = TARRAY_ELEM_IDX(pInfo->base.pTableListInfo->pTableList, *pKeyInfo);
  pInfo->tableEndIndex = (pInfo->tableStartIndex + (*size) - 1);
  pInfo->pResBlock->info.blankFill = false;
  taosMemoryFreeClear(pInfo->pResBlock->pBlockAgg);

  if (!pInfo->needCountEmptyTable) {
    pInfo->countState = TABLE_COUNT_STATE_END;
  } else {
    pInfo->countState = TABLE_COUNT_STATE_SCAN;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void markGroupProcessed(STableScanInfo* pInfo, uint64_t groupId) {
  if (pInfo->countState == TABLE_COUNT_STATE_END) {
    return;
  }
  if (pInfo->base.pTableListInfo->groupOffset) {
    pInfo->countState = TABLE_COUNT_STATE_PROCESSED;
  } else {
    int32_t code = taosHashRemove(pInfo->base.pTableListInfo->remainGroups, &groupId, sizeof(groupId));
    if (code != TSDB_CODE_SUCCESS) {
      qDebug("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    }
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
  terrno = doSetTagColumnData(pBase, pBlock, pTaskInfo, 1);

  return pBlock;
}

static SSDataBlock* getBlockForEmptyTable(SOperatorInfo* pOperator, const STableKeyInfo* tbInfo) {
  STableScanInfo* pTableScanInfo = pOperator->info;
  SSDataBlock*    pBlock =
      getOneRowResultBlock(pOperator->pTaskInfo, &pTableScanInfo->base, pTableScanInfo->pResBlock, tbInfo);

  pOperator->resultInfo.totalRows++;
  return pBlock;
}

static int32_t doTableScanImplNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  SSDataBlock*    pBlock = pTableScanInfo->pResBlock;
  bool            hasNext = false;
  int64_t         st = taosGetTimestampUs();

  QRY_PARAM_CHECK(ppRes);
  pBlock->info.dataLoad = false;

  while (true) {
    code = pAPI->tsdReader.tsdNextDataBlock(pTableScanInfo->base.dataReader, &hasNext);
    if (code != TSDB_CODE_SUCCESS) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->base.dataReader);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (!hasNext) {
      break;
    }

    if (isTaskKilled(pTaskInfo)) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pTableScanInfo->base.dataReader);
      code = pTaskInfo->code;
      goto _end;
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
    QUERY_CHECK_CODE(code, lino, _end);

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

    (*ppRes) = pBlock;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static int32_t doGroupedTableScan(SOperatorInfo* pOperator, SSDataBlock** pBlock) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  STableScanInfo* pTableScanInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;

  QRY_PARAM_CHECK(pBlock);

  // The read handle is not initialized yet, since no qualified tables exists
  if (pTableScanInfo->base.dataReader == NULL || pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  // do the ascending order traverse in the first place.
  while (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
    SSDataBlock* p = NULL;
    code = doTableScanImplNext(pOperator, &p);
    QUERY_CHECK_CODE(code, lino, _end);

    if (p != NULL) {
      markGroupProcessed(pTableScanInfo, p->info.id.groupId);
      *pBlock = p;
      return code;
    }

    pTableScanInfo->scanTimes += 1;
    taosHashClear(pTableScanInfo->pIgnoreTables);

    if (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
      setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
      pTableScanInfo->base.scanFlag = MAIN_SCAN;
      pTableScanInfo->base.dataBlockLoadFlag = FUNC_DATA_REQUIRED_DATA_LOAD;
      qDebug("start to repeat ascending order scan data blocks due to query func required, %s", GET_TASKID(pTaskInfo));

      // do prepare for the next round table scan operation
      code = pAPI->tsdReader.tsdReaderResetStatus(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  }

  int32_t total = pTableScanInfo->scanInfo.numOfAsc + pTableScanInfo->scanInfo.numOfDesc;
  if (pTableScanInfo->scanTimes < total) {
    if (pTableScanInfo->base.cond.order == TSDB_ORDER_ASC) {
      prepareForDescendingScan(&pTableScanInfo->base, pOperator->exprSupp.pCtx, 0);
      code = pAPI->tsdReader.tsdReaderResetStatus(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
      QUERY_CHECK_CODE(code, lino, _end);
      qDebug("%s start to descending order scan data blocks due to query func required", GET_TASKID(pTaskInfo));
    }

    while (pTableScanInfo->scanTimes < total) {
      SSDataBlock* p = NULL;
      code = doTableScanImplNext(pOperator, &p);
      QUERY_CHECK_CODE(code, lino, _end);

      if (p != NULL) {
        markGroupProcessed(pTableScanInfo, p->info.id.groupId);
        *pBlock = p;
        return code;
      }

      pTableScanInfo->scanTimes += 1;
      taosHashClear(pTableScanInfo->pIgnoreTables);

      if (pTableScanInfo->scanTimes < total) {
        setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
        pTableScanInfo->base.scanFlag = MAIN_SCAN;

        qDebug("%s start to repeat descending order scan data blocks", GET_TASKID(pTaskInfo));
        code = pAPI->tsdReader.tsdReaderResetStatus(pTableScanInfo->base.dataReader, &pTableScanInfo->base.cond);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

  if (pTableScanInfo->countState < TABLE_COUNT_STATE_END) {
    STableListInfo* pTableListInfo = pTableScanInfo->base.pTableListInfo;
    if (pTableListInfo->groupOffset) {  // group by tbname, group by tag + sort
      if (pTableScanInfo->countState < TABLE_COUNT_STATE_PROCESSED) {
        pTableScanInfo->countState = TABLE_COUNT_STATE_PROCESSED;
        STableKeyInfo* pStart =
            (STableKeyInfo*)tableListGetInfo(pTableScanInfo->base.pTableListInfo, pTableScanInfo->tableStartIndex);

        if (NULL == pStart) {
          return code;
        }

        *pBlock = getBlockForEmptyTable(pOperator, pStart);
        return code;
      }
    } else {  // group by tag + no sort
      int32_t numOfTables = 0;
      code = tableListGetSize(pTableListInfo, &numOfTables);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pTableScanInfo->tableEndIndex + 1 >= numOfTables) {
        // get empty group, mark processed & rm from hash
        void* pIte = taosHashIterate(pTableListInfo->remainGroups, NULL);
        if (pIte != NULL) {
          size_t        keySize = 0;
          uint64_t*     pGroupId = taosHashGetKey(pIte, &keySize);
          STableKeyInfo info = {.uid = *(uint64_t*)pIte, .groupId = *pGroupId};
          taosHashCancelIterate(pTableListInfo->remainGroups, pIte);
          markGroupProcessed(pTableScanInfo, *pGroupId);
          *pBlock = getBlockForEmptyTable(pOperator, &info);

          return code;
        }
      }
    }
    pTableScanInfo->countState = TABLE_COUNT_STATE_END;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }

  return code;
}

static int32_t createTableListInfoFromParam(SOperatorInfo* pOperator) {
  STableScanInfo*          pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  int32_t                  code = 0;
  STableListInfo*          pListInfo = pInfo->base.pTableListInfo;
  STableScanOperatorParam* pParam = (STableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  int32_t                  num = taosArrayGetSize(pParam->pUidList);
  if (num <= 0) {
    qError("empty table scan uid list");
    return TSDB_CODE_INVALID_PARA;
  }

  qDebug("vgId:%d add total %d dynamic tables to scan, tableSeq:%d, exist num:%" PRId64 ", operator status:%d",
         pTaskInfo->id.vgId, num, pParam->tableSeq, (int64_t)taosArrayGetSize(pListInfo->pTableList),
         pOperator->status);

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
  int32_t       tableIdx = 0;
  for (int32_t i = 0; i < num; ++i) {
    uint64_t* pUid = taosArrayGet(pParam->pUidList, i);
    if (!pUid) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }

    if (taosHashPut(pListInfo->map, pUid, sizeof(uint64_t), &tableIdx, sizeof(int32_t))) {
      if (TSDB_CODE_DUP_KEY == terrno) {
        continue;
      }
      return terrno;
    }

    info.uid = *pUid;
    void* p = taosArrayPush(pListInfo->pTableList, &info);
    if (p == NULL) {
      return terrno;
    }

    tableIdx++;
    qDebug("add dynamic table scan uid:%" PRIu64 ", %s", info.uid, GET_TASKID(pTaskInfo));
  }

  return code;
}

static int32_t doInitReader(STableScanInfo* pInfo, SExecTaskInfo* pTaskInfo, SStorageAPI* pAPI, int32_t* pNum,
                            STableKeyInfo** pList) {
  const char* idStr = GET_TASKID(pTaskInfo);
  int32_t     code = initNextGroupScan(pInfo, pList, pNum);
  if (code) {
    qError("%s failed to init groupScan Info, code:%s at line:%d", idStr, tstrerror(code), __LINE__);
    return code;
  }

  if (pInfo->base.dataReader != NULL) {
    qError("%s tsdb reader should be null", idStr);
    return TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
  }

  code = pAPI->tsdReader.tsdReaderOpen(pInfo->base.readHandle.vnode, &pInfo->base.cond, *pList, *pNum, pInfo->pResBlock,
                                       (void**)&pInfo->base.dataReader, idStr, &pInfo->pIgnoreTables);
  if (code) {
    qError("%s failed to open tsdbReader, code:%s at line:%d", idStr, tstrerror(code), __LINE__);
  }

  return code;
}

int compareColIdPair(const void* elem1, const void* elem2) {
  SColIdPair* node1 = (SColIdPair*)elem1;
  SColIdPair* node2 = (SColIdPair*)elem2;

  if (node1->orgColId < node2->orgColId) {
    return -1;
  }

  return node1->orgColId > node2->orgColId;
}

static bool isNewScanParam(STableScanOperatorParam* pParam) {
  return pParam->window.skey == INT64_MAX && pParam->window.ekey == INT64_MIN;
}

static int32_t createVTableScanInfoFromParam(SOperatorInfo* pOperator) {
  int32_t                  code = 0;
  int32_t                  lino = 0;
  STableScanInfo*          pInfo = pOperator->info;
  SExecTaskInfo*           pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*             pAPI = &pTaskInfo->storageAPI;
  STableListInfo*          pListInfo = pInfo->base.pTableListInfo;
  STableScanOperatorParam* pParam = (STableScanOperatorParam*)pOperator->pOperatorGetParam->value;
  SMetaReader              orgTable = {0};
  SMetaReader              superTable = {0};
  SSchemaWrapper*          schema = NULL;
  SArray*                  pColArray = NULL;
  SArray*                  pBlockColArray = NULL;
  int32_t                  num = 0;
  STableKeyInfo*           pList = NULL;

  cleanupQueryTableDataCond(&pInfo->base.cond);

  pAPI->metaReaderFn.initReader(&orgTable, pInfo->base.readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  code = pAPI->metaReaderFn.getTableEntryByName(&orgTable, strstr(pParam->pOrgTbInfo->tbName, ".") + 1);
  pAPI->metaReaderFn.readerReleaseLock(&orgTable);
  qDebug("dynamic vtable scan for origin table:%s, %s", pParam->pOrgTbInfo->tbName, GET_TASKID(pTaskInfo));
  QUERY_CHECK_CODE(code, lino, _return);
  switch (orgTable.me.type) {
    case TSDB_CHILD_TABLE:
      pAPI->metaReaderFn.initReader(&superTable, pInfo->base.readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
      code = pAPI->metaReaderFn.getTableEntryByUid(&superTable, orgTable.me.ctbEntry.suid);
      pAPI->metaReaderFn.readerReleaseLock(&superTable);
      QUERY_CHECK_CODE(code, lino, _return);
      schema = &superTable.me.stbEntry.schemaRow;
      break;
    case TSDB_NORMAL_TABLE:
      schema = &orgTable.me.ntbEntry.schemaRow;
      break;
    default:
      qError("invalid table type:%d", orgTable.me.type);
      return TSDB_CODE_INVALID_PARA;
      break;
  }

  pListInfo->oneTableForEachGroup = true;
  taosHashClear(pListInfo->map);
  taosArrayClear(pListInfo->pTableList);

  uint64_t      pUid = orgTable.me.uid;
  STableKeyInfo info = {.groupId = 0, .uid = pUid};
  int32_t       tableIdx = 0;
  code = taosHashPut(pListInfo->map, &pUid, sizeof(uint64_t), &tableIdx, sizeof(int32_t));
  QUERY_CHECK_CODE(code, lino, _return);
  QUERY_CHECK_NULL(taosArrayPush(pListInfo->pTableList, &info), code, lino, _return, terrno);
  qDebug("add dynamic table scan uid:%" PRIu64 ", %s", info.uid, GET_TASKID(pTaskInfo));

  pColArray = taosArrayInit(schema->nCols, sizeof(SColIdPair));
  QUERY_CHECK_NULL(pColArray, code, lino, _return, terrno);
  pBlockColArray = taosArrayInit(schema->nCols, sizeof(int32_t));
  QUERY_CHECK_NULL(pBlockColArray, code, lino, _return, terrno);

  // virtual table's origin table scan do not has ts column.
  SColIdPair tsPair = {.vtbColId = PRIMARYKEY_TIMESTAMP_COL_ID, .orgColId = PRIMARYKEY_TIMESTAMP_COL_ID};
  QUERY_CHECK_NULL(taosArrayPush(pColArray, &tsPair), code, lino, _return, terrno);

  for (int32_t i = 0; i < taosArrayGetSize(pParam->pOrgTbInfo->colMap); ++i) {
    SColIdNameKV* kv = taosArrayGet(pParam->pOrgTbInfo->colMap, i);
    for (int32_t j = 0; j < schema->nCols; j++) {
      if (strcmp(kv->colName, schema->pSchema[j].name) == 0) {
        SColIdPair pPair = {.vtbColId = kv->colId, .orgColId = (col_id_t)(j + 1)};
        QUERY_CHECK_NULL(taosArrayPush(pColArray, &pPair), code, lino, _return, terrno);
        break;
      }
    }
  }

  for (int32_t i = 0; i < taosArrayGetSize(pColArray); i++) {
    SColIdPair* pPair = (SColIdPair*)taosArrayGet(pColArray, i);
    for (int32_t j = 0; j < taosArrayGetSize(pInfo->base.matchInfo.pList); j++) {
      SColMatchItem* pItem = taosArrayGet(pInfo->base.matchInfo.pList, j);
      if (pItem->colId == pPair->vtbColId) {
        SColIdPair tmpPair = {.orgColId = pPair->orgColId, .vtbColId = pItem->dstSlotId};
        QUERY_CHECK_NULL(taosArrayPush(pBlockColArray, &tmpPair), code, lino, _return, terrno);
        break;
      }
    }
  }

  taosArraySort(pColArray, compareColIdPair);
  taosArraySort(pBlockColArray, compareColIdPair);

  code = initQueryTableDataCondWithColArray(&pInfo->base.cond, &pInfo->base.orgCond, &pInfo->base.readHandle, pColArray);
  QUERY_CHECK_CODE(code, lino, _return);

  if (pInfo->pResBlock) {
    blockDataDestroy(pInfo->pResBlock);
    pInfo->pResBlock = NULL;
  }

  if (pParam->window.ekey > 0) {
    pInfo->base.cond.twindows.skey = pParam->window.ekey + 1;
  } else if (isNewScanParam(pParam)) {
    pInfo->base.cond.twindows.skey = pInfo->base.orgCond.twindows.skey;
    pInfo->base.cond.twindows.ekey = pInfo->base.orgCond.twindows.ekey;
  }
  pInfo->base.cond.suid = orgTable.me.type == TSDB_CHILD_TABLE ? superTable.me.uid : 0;
  pInfo->currentGroupId = 0;
  pInfo->ignoreTag = true;

  code = createOneDataBlockWithColArray(pInfo->pOrgBlock, pBlockColArray, &pInfo->pResBlock);
  QUERY_CHECK_CODE(code, lino, _return);

  void **reader = taosHashGet(pInfo->readerCache, &pUid, sizeof(uint64_t));
  if (reader) {
    if (isNewScanParam(pParam)) {
      pAPI->tsdReader.tsdReaderClose(*reader);
      pInfo->base.dataReader = NULL;

      code = taosHashRemove(pInfo->readerCache, &pUid, sizeof(uint64_t));
      QUERY_CHECK_CODE(code, lino, _return);

      taosRLockLatch(&pTaskInfo->lock);
      code = doInitReader(pInfo, pTaskInfo, pAPI, &num, &pList);
      taosRUnLockLatch(&pTaskInfo->lock);
      QUERY_CHECK_CODE(code, lino, _return);

      code = taosHashPut(pInfo->readerCache, &pUid, sizeof(uint64_t), &pInfo->base.dataReader, POINTER_BYTES);
      QUERY_CHECK_CODE(code, lino, _return);
      pInfo->newReader = true;
    } else {
      pInfo->base.dataReader = *reader;
      code = blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);
      QUERY_CHECK_CODE(code, lino, _return);

      pAPI->tsdReader.tsdReaderSetDatablock(pInfo->base.dataReader, pInfo->pResBlock);
      pInfo->newReader = false;
      pInfo->scanTimes = 0;
    }
  } else {
    pInfo->base.dataReader = NULL;

    taosRLockLatch(&pTaskInfo->lock);
    code = doInitReader(pInfo, pTaskInfo, pAPI, &num, &pList);
    taosRUnLockLatch(&pTaskInfo->lock);
    QUERY_CHECK_CODE(code, lino, _return);

    code = taosHashPut(pInfo->readerCache, &pUid, sizeof(uint64_t), &pInfo->base.dataReader, POINTER_BYTES);
    QUERY_CHECK_CODE(code, lino, _return);
    pInfo->newReader = true;
  }
  pOperator->status = OP_OPENED;
  
  if (pInfo->pResBlock->info.capacity > pOperator->resultInfo.capacity) {
    pOperator->resultInfo.capacity = pInfo->pResBlock->info.capacity;
  }

  pInfo->currentGroupId = -1;
_return:
  if (code) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosArrayDestroy(pColArray);
  taosArrayDestroy(pBlockColArray);
  pAPI->metaReaderFn.clearReader(&superTable);
  pAPI->metaReaderFn.clearReader(&orgTable);
  return code;
}

static int32_t startNextGroupScan(SOperatorInfo* pOperator, SSDataBlock** pResult) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  int32_t         numOfTables = 0;

  QRY_PARAM_CHECK(pResult);

  code = tableListGetSize(pInfo->base.pTableListInfo, &numOfTables);
  QUERY_CHECK_CODE(code, lino, _end);

  if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pInfo->base.pTableListInfo)) {
    setOperatorCompleted(pOperator);
    if (pOperator->dynamicTask) {
      taosArrayClear(pInfo->base.pTableListInfo->pTableList);
      taosHashClear(pInfo->base.pTableListInfo->map);
    }
    return code;
  }

  // reset value for the next group data output
  pOperator->status = OP_OPENED;
  resetLimitInfoForNextGroup(&pInfo->base.limitInfo);

  int32_t        num = 0;
  STableKeyInfo* pList = NULL;
  code = initNextGroupScan(pInfo, &pList, &num);
  QUERY_CHECK_CODE(code, lino, _end);

  code = pAPI->tsdReader.tsdSetQueryTableList(pInfo->base.dataReader, pList, num);
  QUERY_CHECK_CODE(code, lino, _end);

  code = pAPI->tsdReader.tsdReaderResetStatus(pInfo->base.dataReader, &pInfo->base.cond);
  QUERY_CHECK_CODE(code, lino, _end);
  pInfo->scanTimes = 0;

  code = doGroupedTableScan(pOperator, pResult);
  QUERY_CHECK_CODE(code, lino, _end);

  if (*pResult != NULL) {
    if (pOperator->dynamicTask) {
      (*pResult)->info.id.groupId = (*pResult)->info.id.uid;
    }
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static int32_t groupSeqTableScan(SOperatorInfo* pOperator, SSDataBlock** pResBlock) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  int32_t         num = 0;
  STableKeyInfo*  pList = NULL;
  SSDataBlock*    pResult = NULL;
  const char*     idStr = GET_TASKID(pTaskInfo);

  QRY_PARAM_CHECK(pResBlock);

  if (pInfo->currentGroupId == -1) {
    if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pInfo->base.pTableListInfo)) {
      setOperatorCompleted(pOperator);
      return code;
    }

    taosRLockLatch(&pTaskInfo->lock);
    code = doInitReader(pInfo, pTaskInfo, pAPI, &num, &pList);
    taosRUnLockLatch(&pTaskInfo->lock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pInfo->filesetDelimited) {
      pAPI->tsdReader.tsdSetFilesetDelimited(pInfo->base.dataReader);
    }

    if (pInfo->pResBlock->info.capacity > pOperator->resultInfo.capacity) {
      pOperator->resultInfo.capacity = pInfo->pResBlock->info.capacity;
    }
  }

  code = doGroupedTableScan(pOperator, &pResult);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pResult != NULL) {
    if (pOperator->dynamicTask) {
      pResult->info.id.groupId = pResult->info.id.uid;
    }

    *pResBlock = pResult;
    return code;
  }

  while (true) {
    code = startNextGroupScan(pOperator, &pResult);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pResult || pOperator->status == OP_EXEC_DONE) {
      *pResBlock = pResult;
      return code;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", idStr, __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }

  return code;
}

static bool isEmptyQueryTimeWindow(STimeWindow* pWindow) {
  return (pWindow == NULL) || (pWindow->skey > pWindow->ekey);
}

int32_t doTableScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  STableScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*    pAPI = &pTaskInfo->storageAPI;
  QRY_PARAM_CHECK(ppRes);
  qTrace("%s call", __FUNCTION__);
  if (pOperator->pOperatorGetParam) {
    pOperator->dynamicTask = true;
    if (isDynVtbScan(pOperator)) {
      code = createVTableScanInfoFromParam(pOperator);

      freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
      pOperator->pOperatorGetParam = NULL;
      QUERY_CHECK_CODE(code, lino, _end);

      SSDataBlock* result = NULL;
      if (isEmptyQueryTimeWindow(&pInfo->base.cond.twindows) && pInfo->base.cond.type == TIMEWINDOW_RANGE_CONTAINED) {
        (*ppRes) = result;
        return code;
      }

      if (pInfo->newReader) {
        code = startNextGroupScan(pOperator, &result);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        code = doGroupedTableScan(pOperator, &result);
        QUERY_CHECK_CODE(code, lino, _end);
      }

      if (result) {
        SSDataBlock* res = NULL;
        pAPI->tsdReader.tsdReaderSetDatablock(pInfo->base.dataReader, NULL);
        code = createOneDataBlockWithTwoBlock(result, pInfo->pOrgBlock, &res);
        QUERY_CHECK_CODE(code, lino, _end);
        pInfo->pResBlock = res;
        blockDataDestroy(result);
        (*ppRes) = res;
      } else {
        STableKeyInfo *keyInfo = taosArrayGet(pInfo->base.pTableListInfo->pTableList, 0);
        QUERY_CHECK_NULL(keyInfo, code, lino, _end, terrno)

        blockDataDestroy(pInfo->pResBlock);
        pInfo->pResBlock = NULL;

        void** reader = taosHashGet(pInfo->readerCache, &keyInfo->uid, sizeof(uint64_t));
        if (reader) {
          if (*reader == pInfo->base.dataReader) {
            pAPI->tsdReader.tsdReaderSetDatablock(pInfo->base.dataReader, NULL);
            pInfo->base.dataReader = NULL;
          }
          if (pAPI->tsdReader.tsdReaderClose) {
            pAPI->tsdReader.tsdReaderClose(*reader);
          }
        }

        code = taosHashRemove(pInfo->readerCache, &keyInfo->uid, sizeof(uint64_t));
        QUERY_CHECK_CODE(code, lino, _end);
      }
      return code;
    } else {
      code = createTableListInfoFromParam(pOperator);
      freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
      pOperator->pOperatorGetParam = NULL;
      QUERY_CHECK_CODE(code, lino, _end);

      if (pOperator->status == OP_EXEC_DONE) {
        pInfo->currentGroupId = -1;
        pOperator->status = OP_OPENED;
        SSDataBlock* result = NULL;

        while (true) {
          code = startNextGroupScan(pOperator, &result);
          QUERY_CHECK_CODE(code, lino, _end);

          if (result || pOperator->status == OP_EXEC_DONE) {
            (*ppRes) = result;
            return code;
          }
        }
      }
    }
  }

  // scan table one by one sequentially
  if (pInfo->scanMode == TABLE_SCAN__TABLE_ORDER) {
    int32_t       numOfTables = 0;
    STableKeyInfo tInfo = {0};
    pInfo->countState = TABLE_COUNT_STATE_END;

    while (1) {
      SSDataBlock* result = NULL;
      code = doGroupedTableScan(pOperator, &result);
      QUERY_CHECK_CODE(code, lino, _end);

      if (result || (pOperator->status == OP_EXEC_DONE) || isTaskKilled(pTaskInfo)) {
        (*ppRes) = result;
        return code;
      }

      // if no data, switch to next table and continue scan
      pInfo->currentTable++;

      taosRLockLatch(&pTaskInfo->lock);
      numOfTables = 0;
      code = tableListGetSize(pInfo->base.pTableListInfo, &numOfTables);
      if (code != TSDB_CODE_SUCCESS) {
        taosRUnLockLatch(&pTaskInfo->lock);
        TSDB_CHECK_CODE(code, lino, _end);
      }

      if (pInfo->currentTable >= numOfTables) {
        qDebug("all table checked in table list, total:%d, return NULL, %s", numOfTables, GET_TASKID(pTaskInfo));
        taosRUnLockLatch(&pTaskInfo->lock);
        (*ppRes) = NULL;
        return code;
      }

      STableKeyInfo* tmp = (STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->currentTable);
      if (!tmp) {
        taosRUnLockLatch(&pTaskInfo->lock);
        (*ppRes) = NULL;
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      }

      tInfo = *tmp;
      taosRUnLockLatch(&pTaskInfo->lock);

      code = pAPI->tsdReader.tsdSetQueryTableList(pInfo->base.dataReader, &tInfo, 1);
      QUERY_CHECK_CODE(code, lino, _end);
      qDebug("set uid:%" PRIu64 " into scanner, total tables:%d, index:%d/%d %s", tInfo.uid, numOfTables,
             pInfo->currentTable, numOfTables, GET_TASKID(pTaskInfo));

      code = pAPI->tsdReader.tsdReaderResetStatus(pInfo->base.dataReader, &pInfo->base.cond);
      QUERY_CHECK_CODE(code, lino, _end);
      pInfo->scanTimes = 0;
    }
  } else {  // scan table group by group sequentially
    code = groupSeqTableScan(pOperator, ppRes);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s %s failed at line %d since %s", GET_TASKID(pTaskInfo), __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  return code;
}

static int32_t getTableScannerExecInfo(struct SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  SFileBlockLoadRecorder* pRecorder = taosMemoryCalloc(1, sizeof(SFileBlockLoadRecorder));
  if (!pRecorder) {
    return terrno;
  }
  STableScanInfo* pTableScanInfo = pOptr->info;
  *pRecorder = pTableScanInfo->base.readRecorder;
  *pOptrExplain = pRecorder;
  *len = sizeof(SFileBlockLoadRecorder);
  return 0;
}

static void destroyTableScanBase(STableScanBase* pBase, TsdReader* pAPI) {
  cleanupQueryTableDataCond(&pBase->cond);
  cleanupQueryTableDataCond(&pBase->orgCond);

  if (pAPI->tsdReaderClose) {
    pAPI->tsdReaderClose(pBase->dataReader);
  }
  pBase->dataReader = NULL;

  if (pBase->matchInfo.pList != NULL) {
    taosArrayDestroy(pBase->matchInfo.pList);
  }

  tableListDestroy(pBase->pTableListInfo);
  taosLRUCacheCleanup(pBase->metaCache.pTableMetaEntryCache);
  cleanupExprSupp(&pBase->pseudoSup);
}

static void cleanReaderForVTable(STableScanInfo* pInfo){
  if (pInfo != NULL && pInfo->base.readerAPI.tsdReaderClose) {
    void *pIter = taosHashIterate(pInfo->readerCache, NULL);
    while (pIter != NULL) {
      void **reader = pIter;
      if (*reader) {
        if (*reader == pInfo->base.dataReader) {
          pInfo->base.dataReader = NULL;
        }
        pInfo->base.readerAPI.tsdReaderClose(*reader);
      }
      pIter = taosHashIterate(pInfo->readerCache, pIter);
    }
  }
}

static void destroyTableScanOperatorInfo(void* param) {
  STableScanInfo* pTableScanInfo = (STableScanInfo*)param;
  blockDataDestroy(pTableScanInfo->pResBlock);
  blockDataDestroy(pTableScanInfo->pOrgBlock);
  taosHashCleanup(pTableScanInfo->pIgnoreTables);
  if (pTableScanInfo->virtualStableScan && pTableScanInfo->readerCache) {
    cleanReaderForVTable(pTableScanInfo);
    taosHashCleanup(pTableScanInfo->readerCache);
  }
  destroyTableScanBase(&pTableScanInfo->base, &pTableScanInfo->base.readerAPI);
  taosMemoryFreeClear(param);
}

static void resetClolumnReserve(SSDataBlock* pBlock, int32_t dataRequireFlag) {
  if (pBlock && dataRequireFlag == FUNC_DATA_REQUIRED_NOT_LOAD) {
    int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pCol = (SColumnInfoData*)taosArrayGet(pBlock->pDataBlock, i);
      if (pCol) {
        pCol->info.noData = true;
      }
    }
  }
}

static int32_t resetTableScanOperatorState(SOperatorInfo* pOper) {
  int32_t         code = TSDB_CODE_SUCCESS;
  STableScanInfo*   pInfo = pOper->info;
  pOper->status = OP_NOT_OPENED;

  pInfo->scanTimes = 0;
  pInfo->currentGroupId = -1;
  pInfo->tableEndIndex = -1;
  pInfo->tableStartIndex = 0;
  pInfo->currentTable = 0;
  pInfo->scanMode = 0;
  pInfo->countState = 0;
  pInfo->base.scanFlag = (pInfo->scanInfo.numOfAsc > 1) ? PRE_SCAN : MAIN_SCAN;

  if (!pInfo->virtualStableScan) {
  // if (pInfo->virtualStableScan && pInfo->readerCache) {
  //   cleanReaderForVTable(pInfo);
  //   taosHashClear(pInfo->readerCache);
  //   pInfo->readerCache = false;
  // } else {
    if (pInfo->base.readerAPI.tsdReaderClose) {
      pInfo->base.readerAPI.tsdReaderClose(pInfo->base.dataReader);
    }
    pInfo->base.dataReader = NULL;
  }
  tableListDestroy(pInfo->base.pTableListInfo);
  pInfo->base.pTableListInfo = tableListCreate();
  if (!pInfo->base.pTableListInfo) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }
  SExecTaskInfo*         pTaskInfo = pOper->pTaskInfo;

  STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pTaskInfo->pSubplan->pNode;
  if (!pTableScanNode->scan.node.dynamicOp) {
    code = createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort,
                                    &pInfo->base.readHandle, pInfo->base.pTableListInfo, 
                                    pTaskInfo->pSubplan->pTagCond, pTaskInfo->pSubplan->pTagIndexCond, pTaskInfo, NULL);
    if (code) {
      qError("%s failed to createScanTableListInfo, code:%s", __func__, tstrerror(code));
      return code;
    }
  }

  initLimitInfo(pTableScanNode->scan.node.pLimit, pTableScanNode->scan.node.pSlimit, &pInfo->base.limitInfo);
  cleanupQueryTableDataCond(&pInfo->base.cond);
  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode, &pInfo->base.readHandle, true);
  if (code) {
    qError("%s failed to initQueryTableDataCond, code:%s", __func__, tstrerror(code));
    return code;
  }
  if (pTableScanNode->scan.node.dynamicOp && pTableScanNode->scan.virtualStableScan) {
    cleanupQueryTableDataCond(&pInfo->base.orgCond);
    memcpy(&pInfo->base.orgCond, &pInfo->base.cond, sizeof(SQueryTableDataCond));
    memset(&pInfo->base.cond, 0, sizeof(SQueryTableDataCond));
  } 

  pOper->resultInfo.totalRows = 0;
  blockDataEmpty(pInfo->pResBlock);
  blockDataEmpty(pInfo->pOrgBlock);
  taosHashClear(pInfo->pIgnoreTables);
  return code;
}

int32_t createTableScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle,
                                    STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                    SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pTableScanNode;
  SScanPhysiNode*     pScanNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  code =
      extractColMatchInfo(pScanNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->base.matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  initLimitInfo(pScanNode->node.pLimit, pScanNode->node.pSlimit, &pInfo->base.limitInfo);
  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode, readHandle, true);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pScanNode->pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->base.pseudoSup;
    pSup->pExprInfo = NULL;
    code = createExprInfo(pScanNode->pScanPseudoCols, NULL, &pSup->pExprInfo, &pSup->numOfExprs);
    QUERY_CHECK_CODE(code, lino, _error);

    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset,
                                      &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_NULL(pSup->pCtx, code, lino, _error, terrno);
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
  resetClolumnReserve(pInfo->pResBlock, pInfo->base.dataBlockLoadFlag);
  QUERY_CHECK_NULL(pInfo->pResBlock, code, lino, _error, terrno);

  code = prepareDataBlockBuf(pInfo->pResBlock, &pInfo->base.matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->virtualStableScan = pScanNode->virtualStableScan;
  if (pScanNode->node.dynamicOp && pScanNode->virtualStableScan) {
    TSWAP(pInfo->pOrgBlock, pInfo->pResBlock);
    pInfo->pResBlock = NULL;
    memcpy(&pInfo->base.orgCond, &pInfo->base.cond, sizeof(SQueryTableDataCond));
    memset(&pInfo->base.cond, 0, sizeof(SQueryTableDataCond));
  }

  if (pInfo->virtualStableScan) {
    pInfo->readerCache = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    QUERY_CHECK_NULL(pInfo->readerCache, code, lino, _error, terrno);
  }

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->currentGroupId = -1;

  pInfo->tableEndIndex = -1;
  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;
  pInfo->hasGroupByTag = pTableScanNode->pGroupTags ? true : false;

  setOperatorInfo(pOperator, "TableScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = numOfCols;

  pInfo->needCountEmptyTable = tsCountAlwaysReturnValue && pTableScanNode->needCountEmptyTable;
  pInfo->ignoreTag = false;

  pInfo->base.pTableListInfo = pTableListInfo;
  if (readHandle->streamRtInfo == NULL) {
    pInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
    if (pInfo->base.metaCache.pTableMetaEntryCache == NULL) {
      code = terrno;
      QUERY_CHECK_CODE(code, lino, _error);
    }
    taosLRUCacheSetStrictCapacity(pInfo->base.metaCache.pTableMetaEntryCache, false);
  }

  pInfo->filesetDelimited = pTableScanNode->filesetDelimited;

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableScanNext, NULL, destroyTableScanOperatorInfo,
                                         optrDefaultBufFn, getTableScannerExecInfo, optrDefaultGetNextExtFn, NULL);

  setOperatorResetStateFn(pOperator, resetTableScanOperatorState); 
  // for non-blocking operator, the open cost is always 0
  pOperator->cost.openCost = 0;
  *pOptrInfo = pOperator;
  return TSDB_CODE_SUCCESS;

_error:
  if (pInfo != NULL) {
    pInfo->base.pTableListInfo = NULL;  // this attribute will be destroy outside of this function
    destroyTableScanOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}

int32_t createTableSeqScanOperatorInfo(void* pReadHandle, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t         code = 0;
  STableScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _end;
  }

  pInfo->base.dataReader = pReadHandle;
  //  pInfo->prevGroupId       = -1;

  setOperatorInfo(pOperator, "TableSeqScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableScanImplNext, NULL, NULL, optrDefaultBufFn, NULL,
                                         optrDefaultGetNextExtFn, NULL);
  *pOptrInfo = pOperator;
  return code;

_end:
  if (pInfo != NULL) {
    taosMemoryFree(pInfo);
  }

  if (pOperator != NULL) {
    taosMemoryFree(pOperator);
  }

  pTaskInfo->code = code;
  return code;
}

static int32_t doBlockDataPrimaryKeyFilter(SSDataBlock* pBlock, STqOffsetVal* offset) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pBlock->info.window.skey != offset->ts || offset->primaryKey.type == 0) {
    return code;
  }
  bool* p = taosMemoryCalloc(pBlock->info.rows, sizeof(bool));
  QUERY_CHECK_NULL(p, code, lino, _end, terrno);
  bool hasUnqualified = false;

  SColumnInfoData* pColTs = taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData* pColPk = taosArrayGet(pBlock->pDataBlock, 1);

  qDebug("doBlockDataWindowFilter primary key, ts:%" PRId64 " %" PRId64, offset->ts,
         VALUE_GET_TRIVIAL_DATUM(&offset->primaryKey));
  QUERY_CHECK_CONDITION((pColPk->info.type == offset->primaryKey.type), code, lino, _end,
                        TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);

  __compar_fn_t func = getComparFunc(pColPk->info.type, 0);
  QUERY_CHECK_NULL(func, code, lino, _end, terrno);
  for (int32_t i = 0; i < pBlock->info.rows; ++i) {
    int64_t* ts = (int64_t*)colDataGetData(pColTs, i);
    void*    data = colDataGetData(pColPk, i);
    if (IS_VAR_DATA_TYPE(pColPk->info.type)) {
      if (IS_STR_DATA_BLOB(pColPk->info.type)) {
        QUERY_CHECK_CODE(code = TSDB_CODE_BLOB_NOT_SUPPORT_PRIMARY_KEY, lino, _end);
      }
      void* tmq = taosMemoryMalloc(offset->primaryKey.nData + VARSTR_HEADER_SIZE);
      QUERY_CHECK_NULL(tmq, code, lino, _end, terrno);
      memcpy(varDataVal(tmq), offset->primaryKey.pData, offset->primaryKey.nData);
      varDataLen(tmq) = offset->primaryKey.nData;
      p[i] = (*ts > offset->ts) || (func(data, tmq) > 0);
      taosMemoryFree(tmq);
    } else {
      p[i] = (*ts > offset->ts) || (func(data, VALUE_GET_DATUM(&offset->primaryKey, pColPk->info.type)) > 0);
    }

    if (!p[i]) {
      hasUnqualified = true;
    }
  }

  if (hasUnqualified) {
    code = trimDataBlock(pBlock, pBlock->info.rows, p);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  taosMemoryFree(p);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t colIdComparFn(const void* param1, const void* param2) {
  int32_t p1 = *(int32_t*)param1;
  int32_t p2 = *(int32_t*)param2;

  if (p1 == p2) {
    return 0;
  } else {
    return (p1 < p2) ? -1 : 1;
  }
}

static int32_t setBlockIntoRes(SStreamScanInfo* pInfo, const SSDataBlock* pBlock) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SDataBlockInfo* pBlockInfo = &pInfo->pRes->info;
  SOperatorInfo*  pOperator = pInfo->pStreamScanOp;
  SExecTaskInfo*  pTaskInfo = pOperator->pTaskInfo;
  const char*     id = GET_TASKID(pTaskInfo);
  bool            isVtableSourceScan = (pTaskInfo->pSubplan->pVTables != NULL);

  code = blockDataEnsureCapacity(pInfo->pRes, pBlock->info.rows);
  QUERY_CHECK_CODE(code, lino, _end);

  pBlockInfo->rows = pBlock->info.rows;
  pBlockInfo->id.uid = pBlock->info.id.uid;
  pBlockInfo->type = STREAM_NORMAL;
  pBlockInfo->version = pBlock->info.version;

  STableScanInfo* pTableScanInfo = pInfo->pTableScanOp->info;
  if (!isVtableSourceScan) {
    pBlockInfo->id.groupId = tableListGetTableGroupId(pTableScanInfo->base.pTableListInfo, pBlock->info.id.uid);
  } else {
    // use original table uid as groupId for vtable
    pBlockInfo->id.groupId = pBlock->info.id.groupId;
  }

  SArray* pColList = taosArrayInit(4, sizeof(int32_t));
  QUERY_CHECK_NULL(pColList, code, lino, _end, terrno);

  // todo extract method
  for (int32_t i = 0; i < taosArrayGetSize(pInfo->matchInfo.pList); ++i) {
    SColMatchItem* pColMatchInfo = taosArrayGet(pInfo->matchInfo.pList, i);
    if (!pColMatchInfo->needOutput) {
      continue;
    }

    bool colExists = false;
    for (int32_t j = 0; j < blockDataGetNumOfCols(pBlock); ++j) {
      SColumnInfoData* pResCol = NULL;
      code = bdGetColumnInfoData(pBlock, j, &pResCol);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pResCol->info.colId == pColMatchInfo->colId) {
        SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->dstSlotId);
        code = colDataAssign(pDst, pResCol, pBlock->info.rows, &pInfo->pRes->info);
        QUERY_CHECK_CODE(code, lino, _end);

        colExists = true;
        void* tmp = taosArrayPush(pColList, &pColMatchInfo->dstSlotId);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
        break;
      }
    }

    // the required column does not exists in submit block, let's set it to be all null value
    if (!colExists) {
      SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, pColMatchInfo->dstSlotId);
      colDataSetNNULL(pDst, 0, pBlockInfo->rows);
      void* tmp = taosArrayPush(pColList, &pColMatchInfo->dstSlotId);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  // currently only the tbname pseudo column
  if (pInfo->numOfPseudoExpr > 0 && !isVtableSourceScan) {
    code = addTagPseudoColumnData(&pInfo->readHandle, pInfo->pPseudoExpr, pInfo->numOfPseudoExpr, pInfo->pRes,
                                  pBlockInfo->rows, pTaskInfo, &pTableScanInfo->base.metaCache);
    // ignore the table not exists error, since this table may have been dropped during the scan procedure.
    if (code) {
      QUERY_CHECK_CODE(code, lino, _end);
    }

    // reset the error code.
    terrno = 0;

    for (int32_t i = 0; i < pInfo->numOfPseudoExpr; ++i) {
      void* tmp = taosArrayPush(pColList, &pInfo->pPseudoExpr[i].base.resSchema.slotId);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  taosArraySort(pColList, colIdComparFn);

  int32_t i = 0, j = 0;
  while (i < taosArrayGetSize(pColList)) {
    int32_t slot1 = *(int32_t*)taosArrayGet(pColList, i);
    if (slot1 > j) {
      SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, j);
      colDataSetNNULL(pDst, 0, pBlockInfo->rows);
      j += 1;
    } else {
      i += 1;
      j += 1;
    }
  }

  while (j < taosArrayGetSize(pInfo->pRes->pDataBlock)) {
    SColumnInfoData* pDst = taosArrayGet(pInfo->pRes->pDataBlock, j);
    colDataSetNNULL(pDst, 0, pBlockInfo->rows);
    j += 1;
  }

  taosArrayDestroy(pColList);

  code = doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  code = blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
  QUERY_CHECK_CODE(code, lino, _end);
  if (pInfo->pRes->info.rows == 0) {
    return 0;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t processPrimaryKey(SSDataBlock* pBlock, bool hasPrimaryKey, STqOffsetVal* offset) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SValue  val = {0};
  if (hasPrimaryKey) {
    code = doBlockDataPrimaryKeyFilter(pBlock, offset);
    if (code != TSDB_CODE_SUCCESS) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
      return code;
    }
    SColumnInfoData* pColPk = taosArrayGet(pBlock->pDataBlock, 1);

    if (pBlock->info.rows < 1) {
      return code;
    }
    void* tmp = colDataGetData(pColPk, pBlock->info.rows - 1);
    val.type = pColPk->info.type;
    if (IS_VAR_DATA_TYPE(pColPk->info.type)) {
      if (IS_STR_DATA_BLOB(pColPk->info.type)) {
        return TSDB_CODE_BLOB_NOT_SUPPORT_PRIMARY_KEY;
      }
      val.pData = taosMemoryMalloc(varDataLen(tmp));
      QUERY_CHECK_NULL(val.pData, code, lino, _end, terrno);
      val.nData = varDataLen(tmp);
      memcpy(val.pData, varDataVal(tmp), varDataLen(tmp));
    } else {
      valueSetDatum(&val, pColPk->info.type, tmp, pColPk->info.bytes);
    }
  }
  tqOffsetResetToData(offset, pBlock->info.id.uid, pBlock->info.window.ekey, val);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doQueueScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SStreamScanInfo* pInfo = pOperator->info;
  const char*      id = GET_TASKID(pTaskInfo);

  qDebug("start to exec queue scan, %s", id);

  if (isTaskKilled(pTaskInfo)) {
    (*ppRes) = NULL;
    return pTaskInfo->code;
  }

  if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    while (1) {
      SSDataBlock* pResult = NULL;
      code = doTableScanNext(pInfo->pTableScanOp, &pResult);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pResult && pResult->info.rows > 0) {
        bool hasPrimaryKey = pAPI->tqReaderFn.tqGetTablePrimaryKey(pInfo->tqReader);
        code = processPrimaryKey(pResult, hasPrimaryKey, &pTaskInfo->streamInfo.currentOffset);
        QUERY_CHECK_CODE(code, lino, _end);
        qDebug("tmqsnap doQueueScan get data utid:%" PRId64, pResult->info.id.uid);
        if (pResult->info.rows > 0) {
          (*ppRes) = pResult;
          return code;
        }
      } else {
        break;
      }
    }

    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    pAPI->tsdReader.tsdReaderClose(pTSInfo->base.dataReader);

    pTSInfo->base.dataReader = NULL;
    int64_t validVer = pTaskInfo->streamInfo.snapshotVer + 1;
    qDebug("queue scan tsdb over, switch to wal ver %" PRId64, validVer);
    if (pAPI->tqReaderFn.tqReaderSeek(pInfo->tqReader, validVer, pTaskInfo->id.str) < 0) {
      (*ppRes) = NULL;
      return code;
    }

    tqOffsetResetToLog(&pTaskInfo->streamInfo.currentOffset, validVer);
  }

  if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__LOG) {
    while (1) {
      bool hasResult =
          pAPI->tqReaderFn.tqReaderNextBlockInWal(pInfo->tqReader, id, pTaskInfo->streamInfo.sourceExcluded);

      SSDataBlock*       pRes = pAPI->tqReaderFn.tqGetResultBlock(pInfo->tqReader);
      struct SWalReader* pWalReader = pAPI->tqReaderFn.tqReaderGetWalReader(pInfo->tqReader);

      // curVersion move to next
      tqOffsetResetToLog(&pTaskInfo->streamInfo.currentOffset, pWalReader->curVersion);

      // use ts to pass time when replay, because ts not used if type is log
      pTaskInfo->streamInfo.currentOffset.ts = pAPI->tqReaderFn.tqGetResultBlockTime(pInfo->tqReader);

      if (hasResult) {
        qDebug("doQueueScan get data from log %" PRId64 " rows, version:%" PRId64, pRes->info.rows,
               pTaskInfo->streamInfo.currentOffset.version);
        blockDataCleanup(pInfo->pRes);
        code = setBlockIntoRes(pInfo, pRes);
        QUERY_CHECK_CODE(code, lino, _end);
        qDebug("doQueueScan after filter get data from log %" PRId64 " rows, version:%" PRId64, pInfo->pRes->info.rows,
               pTaskInfo->streamInfo.currentOffset.version);
        if (pInfo->pRes->info.rows > 0) {
          (*ppRes) = pInfo->pRes;
          return code;
        }
      } else {
        qDebug("doQueueScan get none from log, return, version:%" PRId64, pTaskInfo->streamInfo.currentOffset.version);
        (*ppRes) = NULL;
        return code;
      }
    }
  } else {
    qError("unexpected streamInfo prepare type: %d", pTaskInfo->streamInfo.currentOffset.type);
    (*ppRes) = NULL;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  (*ppRes) = NULL;
  return code;
}

static SSDataBlock* doQueueScan(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doQueueScanNext(pOperator, &pRes);
  return pRes;
}

int32_t extractTableIdList(const STableListInfo* pTableListInfo, SArray** ppArrayRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* tableIdList = taosArrayInit(4, sizeof(uint64_t));
  QUERY_CHECK_NULL(tableIdList, code, lino, _end, terrno);

  // Transfer the Array of STableKeyInfo into uid list.
  int32_t size = 0;
  code = tableListGetSize(pTableListInfo, &size);
  QUERY_CHECK_CODE(code, lino, _end);
  for (int32_t i = 0; i < size; ++i) {
    STableKeyInfo* pkeyInfo = tableListGetInfo(pTableListInfo, i);
    QUERY_CHECK_NULL(pkeyInfo, code, lino, _end, terrno);
    void* tmp = taosArrayPush(tableIdList, &pkeyInfo->uid);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  (*ppArrayRes) = tableIdList;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doRawScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SStreamRawScanInfo* pInfo = pOperator->info;
  pTaskInfo->streamInfo.btMetaRsp.batchMetaReq = NULL;  // use batchMetaReq != NULL to judge if data is meta
  pTaskInfo->streamInfo.btMetaRsp.batchMetaLen = NULL;

  qDebug("tmqsnap doRawScan called");
  if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__SNAPSHOT_DATA) {
    bool hasNext = false;
    if (pInfo->dataReader && pInfo->sContext->withMeta != ONLY_META) {
      code = pAPI->tsdReader.tsdNextDataBlock(pInfo->dataReader, &hasNext);
      if (code != TSDB_CODE_SUCCESS) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->dataReader);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }

    if (pInfo->dataReader && hasNext) {
      if (isTaskKilled(pTaskInfo)) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->dataReader);
        return code;
      }

      SSDataBlock* pBlock = NULL;
      code = pAPI->tsdReader.tsdReaderRetrieveDataBlock(pInfo->dataReader, &pBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pBlock && pBlock->info.rows > 0) {
        bool hasPrimaryKey = pAPI->snapshotFn.taosXGetTablePrimaryKey(pInfo->sContext);
        code = processPrimaryKey(pBlock, hasPrimaryKey, &pTaskInfo->streamInfo.currentOffset);
        QUERY_CHECK_CODE(code, lino, _end);
        qDebug("tmqsnap doRawScan get data uid:%" PRId64, pBlock->info.id.uid);
        (*ppRes) = pBlock;
        return code;
      }
    }

    SMetaTableInfo mtInfo = {0};
    code = pAPI->snapshotFn.getMetaTableInfoFromSnapshot(pInfo->sContext, &mtInfo);
    QUERY_CHECK_CODE(code, lino, _end);
    if (code != 0) {
      destroyMetaTableInfo(&mtInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    }
    STqOffsetVal offset = {0};
    if (mtInfo.uid == 0 || pInfo->sContext->withMeta == ONLY_META) {  // read snapshot done, change to get data from wal
      qDebug("tmqsnap read snapshot done, change to get data from wal");
      tqOffsetResetToLog(&offset, pInfo->sContext->snapVersion + 1);
    } else {
      SValue val = {0};
      tqOffsetResetToData(&offset, mtInfo.uid, INT64_MIN, val);
      qDebug("tmqsnap change get data uid:%" PRId64, mtInfo.uid);
    }
    destroyMetaTableInfo(&mtInfo);
    code = qStreamPrepareScan(pTaskInfo, &offset, pInfo->sContext->subType);
    QUERY_CHECK_CODE(code, lino, _end);
    (*ppRes) = NULL;
    return code;
  } else if (pTaskInfo->streamInfo.currentOffset.type == TMQ_OFFSET__SNAPSHOT_META) {
    SSnapContext* sContext = pInfo->sContext;
    for (int32_t i = 0; i < tmqRowSize; i++) {
      void*   data = NULL;
      int32_t dataLen = 0;
      int16_t type = 0;
      int64_t uid = 0;
      if (pAPI->snapshotFn.getTableInfoFromSnapshot(sContext, &data, &dataLen, &type, &uid) < 0) {
        qError("tmqsnap getTableInfoFromSnapshot error");
        taosMemoryFreeClear(data);
        break;
      }

      if (!sContext->queryMeta) {  // change to get data next poll request
        STqOffsetVal offset = {0};
        SValue       val = {0};
        tqOffsetResetToData(&offset, 0, INT64_MIN, val);
        code = qStreamPrepareScan(pTaskInfo, &offset, pInfo->sContext->subType);
        QUERY_CHECK_CODE(code, lino, _end);
        break;
      } else {
        tqOffsetResetToMeta(&pTaskInfo->streamInfo.currentOffset, uid);
        SMqMetaRsp tmpMetaRsp = {0};
        tmpMetaRsp.resMsgType = type;
        tmpMetaRsp.metaRspLen = dataLen;
        tmpMetaRsp.metaRsp = data;
        if (!pTaskInfo->streamInfo.btMetaRsp.batchMetaReq) {
          pTaskInfo->streamInfo.btMetaRsp.batchMetaReq = taosArrayInit(4, POINTER_BYTES);
          QUERY_CHECK_NULL(pTaskInfo->streamInfo.btMetaRsp.batchMetaReq, code, lino, _end, terrno);

          pTaskInfo->streamInfo.btMetaRsp.batchMetaLen = taosArrayInit(4, sizeof(int32_t));
          QUERY_CHECK_NULL(pTaskInfo->streamInfo.btMetaRsp.batchMetaLen, code, lino, _end, terrno);
        }
        int32_t  tempRes = TSDB_CODE_SUCCESS;
        uint32_t len = 0;
        tEncodeSize(tEncodeMqMetaRsp, &tmpMetaRsp, len, tempRes);
        if (TSDB_CODE_SUCCESS != tempRes) {
          qError("tmqsnap tEncodeMqMetaRsp error");
          taosMemoryFreeClear(data);
          break;
        }

        int32_t tLen = sizeof(SMqRspHead) + len;
        void*   tBuf = taosMemoryCalloc(1, tLen);
        QUERY_CHECK_NULL(tBuf, code, lino, _end, terrno);

        void*    metaBuff = POINTER_SHIFT(tBuf, sizeof(SMqRspHead));
        SEncoder encoder = {0};
        tEncoderInit(&encoder, metaBuff, len);
        int32_t tempLen = tEncodeMqMetaRsp(&encoder, &tmpMetaRsp);
        if (tempLen < 0) {
          qError("tmqsnap tEncodeMqMetaRsp error");
          tEncoderClear(&encoder);
          taosMemoryFreeClear(tBuf);
          taosMemoryFreeClear(data);
          break;
        }
        taosMemoryFreeClear(data);
        void* tmp = taosArrayPush(pTaskInfo->streamInfo.btMetaRsp.batchMetaReq, &tBuf);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

        tmp = taosArrayPush(pTaskInfo->streamInfo.btMetaRsp.batchMetaLen, &tLen);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      }
    }

    (*ppRes) = NULL;
    return code;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  (*ppRes) = NULL;
  return code;
}

static void destroyTmqRawScanOperatorInfo(void* param) {
  SStreamRawScanInfo* pRawScan = (SStreamRawScanInfo*)param;
  pRawScan->pAPI->tsdReader.tsdReaderClose(pRawScan->dataReader);
  pRawScan->pAPI->snapshotFn.destroySnapshot(pRawScan->sContext);
  tableListDestroy(pRawScan->pTableListInfo);
  taosMemoryFree(pRawScan);
}

// for subscribing db or stb (not including column),
// if this scan is used, meta data can be return
// and schemas are decided when scanning
int32_t createTmqRawScanOperatorInfo(SReadHandle* pHandle, SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  // create operator
  // create tb reader
  // create meta reader
  // create tq reader

  QRY_PARAM_CHECK(pOptrInfo);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  SStreamRawScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamRawScanInfo));
  SOperatorInfo*      pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    lino = __LINE__;
    goto _end;
  }

  pInfo->pTableListInfo = tableListCreate();
  QUERY_CHECK_NULL(pInfo->pTableListInfo, code, lino, _end, terrno);
  pInfo->vnode = pHandle->vnode;
  pInfo->pAPI = &pTaskInfo->storageAPI;

  pInfo->sContext = pHandle->sContext;
  setOperatorInfo(pOperator, "RawScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);

  pOperator->fpSet = createOperatorFpSet(NULL, doRawScanNext, NULL, destroyTmqRawScanOperatorInfo, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);
  *pOptrInfo = pOperator;
  return code;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  pTaskInfo->code = code;
  return code;
}

void destroyTmqScanOperatorInfo(void* param) {
  if (param == NULL) {
    return;
  }

  SStreamScanInfo* pStreamScan = (SStreamScanInfo*)param;
  if (pStreamScan->pTableScanOp && pStreamScan->pTableScanOp->info) {
    destroyOperator(pStreamScan->pTableScanOp);
  }

  if (pStreamScan->tqReader != NULL && pStreamScan->readerFn.tqReaderClose != NULL) {
    pStreamScan->readerFn.tqReaderClose(pStreamScan->tqReader);
  }
  if (pStreamScan->pVtableMergeHandles) {
    taosHashCleanup(pStreamScan->pVtableMergeHandles);
    pStreamScan->pVtableMergeHandles = NULL;
  }
  if (pStreamScan->pVtableMergeBuf) {
    destroyDiskbasedBuf(pStreamScan->pVtableMergeBuf);
    pStreamScan->pVtableMergeBuf = NULL;
  }
  if (pStreamScan->pVtableReadyHandles) {
    taosArrayDestroy(pStreamScan->pVtableReadyHandles);
    pStreamScan->pVtableReadyHandles = NULL;
  }
  if (pStreamScan->pTableListInfo) {
    tableListDestroy(pStreamScan->pTableListInfo);
    pStreamScan->pTableListInfo = NULL;
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

  blockDataDestroy(pStreamScan->pRes);
  blockDataDestroy(pStreamScan->pUpdateRes);
  blockDataDestroy(pStreamScan->pDeleteDataRes);
  blockDataDestroy(pStreamScan->pUpdateDataRes);
  blockDataDestroy(pStreamScan->pCreateTbRes);
  taosArrayDestroy(pStreamScan->pBlockLists);
  blockDataDestroy(pStreamScan->pCheckpointRes);

  taosMemoryFree(pStreamScan);
}

int32_t addPrimaryKeyCol(SSDataBlock* pBlock, uint8_t type, int32_t bytes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  pBlock->info.rowSize += bytes;
  SColumnInfoData infoData = {0};
  infoData.info.type = type;
  infoData.info.bytes = bytes;
  void* tmp = taosArrayPush(pBlock->pDataBlock, &infoData);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* createStreamVtableBlock(SColMatchInfo* pMatchInfo, const char* idstr) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pRes = NULL;

  QUERY_CHECK_NULL(pMatchInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = createDataBlock(&pRes);
  QUERY_CHECK_CODE(code, lino, _end);
  int32_t numOfOutput = taosArrayGetSize(pMatchInfo->pList);
  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColMatchItem* pItem = taosArrayGet(pMatchInfo->pList, i);
    if (!pItem->needOutput) {
      continue;
    }
    SColumnInfoData colInfo = createColumnInfoData(pItem->dataType.type, pItem->dataType.bytes, pItem->colId);
    code = blockDataAppendColInfo(pRes, &colInfo);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s, id: %s", __func__, lino, tstrerror(code), idstr);
    if (pRes != NULL) {
      blockDataDestroy(pRes);
    }
    pRes = NULL;
    terrno = code;
  }
  return pRes;
}

 int32_t createTmqScanOperatorInfo(SReadHandle* pHandle, STableScanPhysiNode* pTableScanNode,
                                                  SNode* pTagCond, STableListInfo* pTableListInfo,
                                                  SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SArray*          pColIds = NULL;
  SStreamScanInfo* pInfo = taosMemoryCalloc(1, sizeof(SStreamScanInfo));
  SOperatorInfo*   pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  SStorageAPI*     pAPI = &pTaskInfo->storageAPI;
  const char*      idstr = pTaskInfo->id.str;
  SSHashObj*       pVtableInfos = pTaskInfo->pSubplan->pVTables;

  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  SScanPhysiNode*     pScanPhyNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanPhyNode->node.pOutputDataBlockDesc;

  pInfo->pTagCond = pTagCond;
  pInfo->pGroupTags = pTableScanNode->pGroupTags;

  int32_t numOfCols = 0;
  code = extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  SDataType pkType = {0};
  pInfo->primaryKeyIndex = -1;
  pInfo->basic.primaryPkIndex = -1;
  int32_t numOfOutput = taosArrayGetSize(pInfo->matchInfo.pList);
  pColIds = taosArrayInit(numOfOutput, sizeof(int16_t));
  QUERY_CHECK_NULL(pColIds, code, lino, _error, terrno);

  for (int32_t i = 0; i < numOfOutput; ++i) {
    SColMatchItem* id = taosArrayGet(pInfo->matchInfo.pList, i);
    QUERY_CHECK_NULL(id, code, lino, _error, terrno);

    int16_t colId = id->colId;
    void*   tmp = taosArrayPush(pColIds, &colId);
    QUERY_CHECK_NULL(tmp, code, lino, _error, terrno);

    if (id->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      pInfo->primaryTsIndex = id->dstSlotId;
    }
    if (id->isPk) {
      pInfo->primaryKeyIndex = id->dstSlotId;
      pInfo->basic.primaryPkIndex = id->dstSlotId;
      pkType = id->dataType;
    }
  }

  pInfo->pPartTbnameSup = NULL;
  if (pTableScanNode->pSubtable != NULL) {
    SExprInfo* pSubTableExpr = taosMemoryCalloc(1, sizeof(SExprInfo));
    if (pSubTableExpr == NULL) {
      code = terrno;
      goto _error;
    }

    pInfo->tbnameCalSup.pExprInfo = pSubTableExpr;
    code = createExprFromOneNode(pSubTableExpr, pTableScanNode->pSubtable, 0);
    QUERY_CHECK_CODE(code, lino, _error);

    if (initExprSupp(&pInfo->tbnameCalSup, pSubTableExpr, 1, &pTaskInfo->storageAPI.functionStore) != 0) {
      goto _error;
    }
  }

  if (pTableScanNode->pTags != NULL) {
    int32_t    numOfTags;
    SExprInfo* pTagExpr = createExpr(pTableScanNode->pTags, &numOfTags);
    if (pTagExpr == NULL) {
      goto _error;
    }
    code = initExprSupp(&pInfo->tagCalSup, pTagExpr, numOfTags, &pTaskInfo->storageAPI.functionStore);
    if (code != 0) {
      goto _error;
    }
  }

  pInfo->pBlockLists = taosArrayInit(4, sizeof(SPackedData));
  TSDB_CHECK_NULL(pInfo->pBlockLists, code, lino, _error, terrno);

  if (pHandle->vnode) {
    SOperatorInfo* pTableScanOp = NULL;
    code = createTableScanOperatorInfo(pTableScanNode, pHandle, pTableListInfo, pTaskInfo, &pTableScanOp);
    if (pTableScanOp == NULL || code != 0) {
      qError("createTableScanOperatorInfo error, code:%d", pTaskInfo->code);
      goto _error;
    }

    STableScanInfo* pTSInfo = (STableScanInfo*)pTableScanOp->info;
    if (pHandle->version > 0) {
      pTSInfo->base.cond.endVersion = pHandle->version;
    }

    STableKeyInfo* pList = NULL;
    int32_t        num = 0;
    code = tableListGetGroupList(pTableListInfo, 0, &pList, &num);
    QUERY_CHECK_CODE(code, lino, _error);

    if (pHandle->initTableReader) {
      pTSInfo->scanMode = TABLE_SCAN__TABLE_ORDER;
      pTSInfo->base.dataReader = NULL;
    }

    if (pHandle->initTqReader) {
      pInfo->tqReader = pAPI->tqReaderFn.tqReaderOpen(pHandle->vnode);
      QUERY_CHECK_NULL(pInfo->tqReader, code, lino, _error, terrno);
    } else {
      pInfo->tqReader = pHandle->tqReader;
      QUERY_CHECK_NULL(pInfo->tqReader, code, lino, _error, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    }

    if (pVtableInfos != NULL) {
      // save vtable info into tqReader for vtable source scan
      SSDataBlock* pResBlock = createStreamVtableBlock(&pInfo->matchInfo, idstr);
      QUERY_CHECK_CODE(code, lino, _error);
      code = pAPI->tqReaderFn.tqReaderSetVtableInfo(pInfo->tqReader, pHandle->vnode, pAPI, pVtableInfos, &pResBlock,
                                                    idstr);
      QUERY_CHECK_CODE(code, lino, _error);
    }

    pInfo->pUpdateInfo = NULL;
    pInfo->pTableScanOp = pTableScanOp;
    if (pInfo->pTableScanOp->pTaskInfo->streamInfo.pState) {
      pAPI->stateStore.streamStateSetNumber(pInfo->pTableScanOp->pTaskInfo->streamInfo.pState, -1,
                                            pInfo->primaryTsIndex);
    }

    pInfo->readHandle = *pHandle;
    pTaskInfo->streamInfo.snapshotVer = pHandle->version;
    pInfo->pCreateTbRes = buildCreateTableBlock(&pInfo->tbnameCalSup, &pInfo->tagCalSup);
    QUERY_CHECK_NULL(pInfo->pCreateTbRes, code, lino, _error, terrno);
    pInfo->hasPart = false;

    code = blockDataEnsureCapacity(pInfo->pCreateTbRes, 8);
    QUERY_CHECK_CODE(code, lino, _error);

    // set the extract column id to streamHandle
    code = pAPI->tqReaderFn.tqReaderSetColIdList(pInfo->tqReader, pColIds, idstr);
    QUERY_CHECK_CODE(code, lino, _error);

    SArray* tableIdList = NULL;
    code = extractTableIdList(((STableScanInfo*)(pInfo->pTableScanOp->info))->base.pTableListInfo, &tableIdList);
    QUERY_CHECK_CODE(code, lino, _error);
    code = pAPI->tqReaderFn.tqReaderSetQueryTableList(pInfo->tqReader, tableIdList, idstr);
    QUERY_CHECK_CODE(code, lino, _error);
    taosArrayDestroy(tableIdList);
    memcpy(&pTaskInfo->streamInfo.tableCond, &pTSInfo->base.cond, sizeof(SQueryTableDataCond));
  } else {
    taosArrayDestroy(pColIds);
    tableListDestroy(pTableListInfo);
  }

  // clear the local variable to avoid repeatly free
  pColIds = NULL;

  // create the pseduo columns info
  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    code = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pInfo->pPseudoExpr, &pInfo->numOfPseudoExpr);
    QUERY_CHECK_CODE(code, lino, _error);
  }

  code = filterInitFromNode((SNode*)pScanPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0, NULL);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);
  code = createSpecialDataBlock(STREAM_CLEAR, &pInfo->pUpdateRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->scanMode = STREAM_SCAN_FROM_READERHANDLE;
  pInfo->windowSup = (SWindowSupporter){.pStreamAggSup = NULL, .gap = -1, .parentType = QUERY_NODE_PHYSICAL_PLAN};
  pInfo->groupId = 0;
  pInfo->igCheckGroupId = false;
  pInfo->pStreamScanOp = pOperator;
  pInfo->deleteDataIndex = 0;
  code = createSpecialDataBlock(STREAM_DELETE_DATA, &pInfo->pDeleteDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->updateWin = (STimeWindow){.skey = INT64_MAX, .ekey = INT64_MAX};
  code = createSpecialDataBlock(STREAM_CLEAR, &pInfo->pUpdateDataRes);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;
  pInfo->partitionSup.needCalc = false;
  pInfo->igCheckUpdate = pTableScanNode->igCheckUpdate;
  pInfo->igExpired = pTableScanNode->igExpired;
  pInfo->twAggSup.maxTs = INT64_MIN;
  pInfo->pState = pTaskInfo->streamInfo.pState;
  pInfo->stateStore = pTaskInfo->storageAPI.stateStore;
  pInfo->readerFn = pTaskInfo->storageAPI.tqReaderFn;
  pInfo->pFillSup = NULL;
  pInfo->useGetResultRange = false;
  pInfo->pRangeScanRes = NULL;

  code = createSpecialDataBlock(STREAM_CHECKPOINT, &pInfo->pCheckpointRes);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, STREAM_SCAN_OP_NAME, QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);

  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doQueueScanNext, NULL, destroyTmqScanOperatorInfo, optrDefaultBufFn,
                                         NULL, optrDefaultGetNextExtFn, NULL);

  *pOptrInfo = pOperator;
  return code;

_error:
  if (pColIds != NULL) {
    taosArrayDestroy(pColIds);
  }

  if (pInfo != NULL && pInfo->pTableScanOp != NULL) {
    STableScanInfo* p = (STableScanInfo*)pInfo->pTableScanOp->info;
    if (p != NULL) {
      p->base.pTableListInfo = NULL;
    }
    destroyTmqScanOperatorInfo(pInfo);
  }

  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}

static int32_t doTagScanOneTable(SOperatorInfo* pOperator, SSDataBlock* pRes, SMetaReader* mr, SStorageAPI* pAPI) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  STagScanInfo*  pInfo = pOperator->info;
  SExprInfo*     pExprInfo = &pOperator->exprSupp.pExprInfo[0];
  int32_t        count = pRes->info.rows;

  STableKeyInfo* item = tableListGetInfo(pInfo->pTableListInfo, pInfo->curPos);
  if (!item) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", item->uid, tstrerror(terrno),
           GET_TASKID(pTaskInfo));
    tDecoderClear(&(*mr).coder);
    goto _end;
  }

  code = pAPI->metaReaderFn.getTableEntryByUid(mr, item->uid);
  tDecoderClear(&(*mr).coder);
  if (code != TSDB_CODE_SUCCESS) {
    qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", item->uid, tstrerror(terrno),
           GET_TASKID(pTaskInfo));
    goto _end;
  }

  char str[512];
  for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
    SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);

    // refactor later
    if (FUNCTION_TYPE_TBNAME == pExprInfo[j].pExpr->_function.functionType) {
      STR_TO_VARSTR(str, (*mr).me.name);
      code = colDataSetVal(pDst, (count), str, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (FUNCTION_TYPE_TBUID == pExprInfo[j].pExpr->_function.functionType) {
      code = colDataSetVal(pDst, (count), (char*)&(*mr).me.uid, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (FUNCTION_TYPE_VGID == pExprInfo[j].pExpr->_function.functionType) {
      code = colDataSetVal(pDst, (count), (char*)&pTaskInfo->id.vgId, false);
      QUERY_CHECK_CODE(code, lino, _end);
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

      code = colDataSetVal(pDst, (count), data,
                           (data == NULL) || (pDst->info.type == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(data)));
      QUERY_CHECK_CODE(code, lino, _end);

      if ((pDst->info.type != TSDB_DATA_TYPE_JSON) && (p != NULL) && IS_VAR_DATA_TYPE(((const STagVal*)p)->type) &&
          (data != NULL)) {
        taosMemoryFree(data);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  } else {
    pRes->info.rows++;
  }

  return code;
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
  int32_t                code = TSDB_CODE_SUCCESS;
  int32_t                lino = 0;
  STagScanFilterContext* pCtx = (STagScanFilterContext*)pContext;
  SColumnNode*           pSColumnNode = NULL;
  if (QUERY_NODE_COLUMN == nodeType((*pNode))) {
    pSColumnNode = *(SColumnNode**)pNode;
  } else if (QUERY_NODE_FUNCTION == nodeType((*pNode))) {
    SFunctionNode* pFuncNode = *(SFunctionNode**)(pNode);
    if (pFuncNode->funcType == FUNCTION_TYPE_TBNAME) {
      pSColumnNode = NULL;
      pCtx->code = nodesMakeNode(QUERY_NODE_COLUMN, (SNode**)&pSColumnNode);
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

  void* data = taosHashGet(pCtx->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId));
  if (!data) {
    code = taosHashPut(pCtx->colHash, &pSColumnNode->colId, sizeof(pSColumnNode->colId), pNode, sizeof((*pNode)));
    if (code == TSDB_CODE_DUP_KEY) {
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);
    pSColumnNode->slotId = pCtx->index++;
    SColumnInfo cInfo = {.colId = pSColumnNode->colId,
                         .type = pSColumnNode->node.resType.type,
                         .bytes = pSColumnNode->node.resType.bytes};
    void*       tmp = taosArrayPush(pCtx->cInfoList, &cInfo);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  } else {
    SColumnNode* col = *(SColumnNode**)data;
    pSColumnNode->slotId = col->slotId;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    return DEAL_RES_ERROR;
  }
  return DEAL_RES_CONTINUE;
}

static int32_t tagScanFilterByTagCond(SArray* aUidTags, SNode* pTagCond, SArray* aFilterIdxs, void* pVnode,
                                      SStorageAPI* pAPI, STagScanInfo* pInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t numOfTables = taosArrayGetSize(aUidTags);
  SArray* pBlockList = NULL;

  SSDataBlock* pResBlock = createTagValBlockForFilter(pInfo->filterCtx.cInfoList, numOfTables, aUidTags, pVnode, pAPI);
  QUERY_CHECK_NULL(pResBlock, code, lino, _end, terrno);

  pBlockList = taosArrayInit(1, POINTER_BYTES);
  QUERY_CHECK_NULL(pBlockList, code, lino, _end, terrno);

  void* tmp = taosArrayPush(pBlockList, &pResBlock);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

  SDataType type = {.type = TSDB_DATA_TYPE_BOOL, .bytes = sizeof(bool)};

  SScalarParam output = {0};
  code = tagScanCreateResultData(&type, numOfTables, &output);
  QUERY_CHECK_CODE(code, lino, _end);

  code = scalarCalculate(pTagCond, pBlockList, &output, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  bool* result = (bool*)output.columnData->pData;
  for (int32_t i = 0; i < numOfTables; ++i) {
    if (result[i]) {
      void* tmp = taosArrayPush(aFilterIdxs, &i);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    }
  }

  colDataDestroy(output.columnData);
  taosMemoryFreeClear(output.columnData);

_end:
  blockDataDestroy(pResBlock);
  taosArrayDestroy(pBlockList);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tagScanFillOneCellWithTag(SOperatorInfo* pOperator, const STUidTagInfo* pUidTagInfo,
                                         SExprInfo* pExprInfo, SColumnInfoData* pColInfo, int rowIndex,
                                         const SStorageAPI* pAPI, void* pVnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (QUERY_NODE_FUNCTION == pExprInfo->pExpr->nodeType) {
    if (FUNCTION_TYPE_TBNAME == pExprInfo->pExpr->_function.functionType) {  // tbname
      char str[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(str, "ctbidx");

      code = colDataSetVal(pColInfo, rowIndex, str, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (FUNCTION_TYPE_TBUID == pExprInfo->pExpr->_function.functionType) {
      code = colDataSetVal(pColInfo, rowIndex, (char*)&pUidTagInfo->uid, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else if (FUNCTION_TYPE_VGID == pExprInfo->pExpr->_function.functionType) {
      code = colDataSetVal(pColInfo, rowIndex, (char*)&pOperator->pTaskInfo->id.vgId, false);
      QUERY_CHECK_CODE(code, lino, _end);
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
        code = colDataSetVal(pColInfo, rowIndex, p, false);
        QUERY_CHECK_CODE(code, lino, _end);
      } else if (IS_VAR_DATA_TYPE(pColInfo->info.type)) {
        if (IS_STR_DATA_BLOB(pColInfo->info.type)) {
          QUERY_CHECK_CODE(code = TSDB_CODE_BLOB_NOT_SUPPORT_TAG, lino, _end);
        }
        char* tmp = taosMemoryMalloc(tagVal.nData + VARSTR_HEADER_SIZE + 1);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);

        varDataSetLen(tmp, tagVal.nData);
        memcpy(tmp + VARSTR_HEADER_SIZE, tagVal.pData, tagVal.nData);
        code = colDataSetVal(pColInfo, rowIndex, tmp, false);
        taosMemoryFree(tmp);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        code = colDataSetVal(pColInfo, rowIndex, (const char*)&tagVal.i64, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tagScanFillResultBlock(SOperatorInfo* pOperator, SSDataBlock* pRes, SArray* aUidTags,
                                      SArray* aFilterIdxs, bool ignoreFilterIdx, SStorageAPI* pAPI) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  STagScanInfo* pInfo = pOperator->info;
  SExprInfo*    pExprInfo = &pOperator->exprSupp.pExprInfo[0];
  if (!ignoreFilterIdx) {
    size_t szTables = taosArrayGetSize(aFilterIdxs);
    for (int i = 0; i < szTables; ++i) {
      int32_t       idx = *(int32_t*)taosArrayGet(aFilterIdxs, i);
      STUidTagInfo* pUidTagInfo = taosArrayGet(aUidTags, idx);
      QUERY_CHECK_NULL(pUidTagInfo, code, lino, _end, terrno);
      for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);
        QUERY_CHECK_NULL(pDst, code, lino, _end, terrno);
        code = tagScanFillOneCellWithTag(pOperator, pUidTagInfo, &pExprInfo[j], pDst, i, pAPI, pInfo->readHandle.vnode);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  } else {
    size_t szTables = taosArrayGetSize(aUidTags);
    for (int i = 0; i < szTables; ++i) {
      STUidTagInfo* pUidTagInfo = taosArrayGet(aUidTags, i);
      QUERY_CHECK_NULL(pUidTagInfo, code, lino, _end, terrno);
      for (int32_t j = 0; j < pOperator->exprSupp.numOfExprs; ++j) {
        SColumnInfoData* pDst = taosArrayGet(pRes->pDataBlock, pExprInfo[j].base.resSchema.slotId);
        QUERY_CHECK_NULL(pDst, code, lino, _end, terrno);
        code = tagScanFillOneCellWithTag(pOperator, pUidTagInfo, &pExprInfo[j], pDst, i, pAPI, pInfo->readHandle.vnode);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doTagScanFromCtbIdxNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  STagScanInfo* pInfo = pOperator->info;
  SSDataBlock*  pRes = pInfo->pRes;

  QRY_PARAM_CHECK(ppRes);

  if (pOperator->status == OP_EXEC_DONE) {
    return TSDB_CODE_SUCCESS;
  }
  blockDataCleanup(pRes);

  if (pInfo->pCtbCursor == NULL) {
    pInfo->pCtbCursor = pAPI->metaFn.openCtbCursor(pInfo->readHandle.vnode, pInfo->suid, 1);
    QUERY_CHECK_NULL(pInfo->pCtbCursor, code, lino, _end, terrno);
  } else {
    code = pAPI->metaFn.resumeCtbCursor(pInfo->pCtbCursor, 0);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SArray* aUidTags = pInfo->aUidTags;
  SArray* aFilterIdxs = pInfo->aFilterIdxs;
  int32_t count = 0;
  bool    ctbCursorFinished = false;
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
      QUERY_CHECK_NULL(info.pTagVal, code, lino, _end, terrno);

      memcpy(info.pTagVal, pCur->pVal, pCur->vLen);
      void* tmp = taosArrayPush(aUidTags, &info);
      QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
      ++numTables;
    }

    if (numTables == 0) {
      break;
    }
    bool ignoreFilterIdx = true;
    if (pInfo->pTagCond != NULL) {
      ignoreFilterIdx = false;
      code = tagScanFilterByTagCond(aUidTags, pInfo->pTagCond, aFilterIdxs, pInfo->readHandle.vnode, pAPI, pInfo);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      ignoreFilterIdx = true;
    }

    code = tagScanFillResultBlock(pOperator, pRes, aUidTags, aFilterIdxs, ignoreFilterIdx, pAPI);
    QUERY_CHECK_CODE(code, lino, _end);

    count = ignoreFilterIdx ? taosArrayGetSize(aUidTags) : taosArrayGetSize(aFilterIdxs);

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
  (*ppRes) = (pRes->info.rows == 0) ? NULL : pInfo->pRes;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }

  return code;
}

static int32_t createTagScanTableListInfoFromParam(SOperatorInfo* pOperator) {
  STagScanInfo*          pInfo = pOperator->info;
  SExecTaskInfo*         pTaskInfo = pOperator->pTaskInfo;
  int32_t                code = 0;
  STableListInfo*        pListInfo = pInfo->pTableListInfo;
  STagScanOperatorParam* pParam = (STagScanOperatorParam*)pOperator->pOperatorGetParam->value;
  tb_uid_t               pUid = pParam->vcUid;

  // qDebug("vgId:%d add total %d dynamic tables to scan, tableSeq:%d, exist num:%" PRId64 ", operator status:%d",
  //        pTaskInfo->id.vgId, num, pParam->tableSeq, (int64_t)taosArrayGetSize(pListInfo->pTableList),
  //        pOperator->status);

  STableKeyInfo info = {.groupId = 0};

  int32_t tableIdx = 0;
  taosHashClear(pListInfo->map);
  taosArrayClear(pListInfo->pTableList);

  if (taosHashPut(pListInfo->map, &pUid, sizeof(uint64_t), &tableIdx, sizeof(int32_t))) {
    if (TSDB_CODE_DUP_KEY == terrno) {
    } else {
      return terrno;
    }
  }

  info.uid = pUid;
  void* p = taosArrayPush(pListInfo->pTableList, &info);
  if (p == NULL) {
    return terrno;
  }

  qDebug("add dynamic table scan uid:%" PRIu64 ", %s", info.uid, GET_TASKID(pTaskInfo));

  return code;
}

static int32_t doTagScanFromMetaEntryNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t       code = TSDB_CODE_SUCCESS;
  int32_t       lino = 0;
  STagScanInfo* pInfo = pOperator->info;
  if (pOperator->pOperatorGetParam) {
    pOperator->resultInfo.totalRows = 0;
    pOperator->dynamicTask = true;
    pInfo->curPos = 0;
    code = createTagScanTableListInfoFromParam(pOperator);
    freeOperatorParam(pOperator->pOperatorGetParam, OP_GET_PARAM);
    pOperator->pOperatorGetParam = NULL;
    QUERY_CHECK_CODE(code, lino, _end);

    if (pOperator->status == OP_EXEC_DONE) {
      pOperator->status = OP_OPENED;
    }
  }

  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return code;
  }

  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  SExprInfo*   pExprInfo = &pOperator->exprSupp.pExprInfo[0];
  SSDataBlock* pRes = pInfo->pRes;
  blockDataCleanup(pRes);

  int32_t size = 0;
  code = tableListGetSize(pInfo->pTableListInfo, &size);
  QUERY_CHECK_CODE(code, lino, _end);

  if (size == 0) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
    (*ppRes) = NULL;
    return code;
  }

  SMetaReader mr = {0};
  pAPI->metaReaderFn.initReader(&mr, pInfo->readHandle.vnode, META_READER_LOCK, &pAPI->metaFn);
  pRes->info.rows = 0;

  while (pInfo->curPos < size && pRes->info.rows < pOperator->resultInfo.capacity) {
    code = doTagScanOneTable(pOperator, pRes, &mr, &pTaskInfo->storageAPI);
    if (code != TSDB_CODE_OUT_OF_MEMORY && code != TSDB_CODE_QRY_REACH_QMEM_THRESHOLD &&
        code != TSDB_CODE_QRY_QUERY_MEM_EXHAUSTED) {
      // ignore other error
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);

    if (++pInfo->curPos >= size) {
      setOperatorCompleted(pOperator);
    }
  }

  pAPI->metaReaderFn.clearReader(&mr);
  bool bLimitReached = applyLimitOffset(&pInfo->limitInfo, pRes, pTaskInfo);
  if (bLimitReached) {
    setOperatorCompleted(pOperator);
  }

  // qDebug("QInfo:0x%" PRIx64 ", create tag values results completed, rows:%d", GET_TASKID(pRuntimeEnv), count);
  if (pOperator->status == OP_EXEC_DONE) {
    setTaskStatus(pTaskInfo, TASK_COMPLETED);
  }

  pOperator->resultInfo.totalRows += pRes->info.rows;

  (*ppRes) = (pRes->info.rows == 0) ? NULL : pInfo->pRes;
_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  return code;
}

static SSDataBlock* doTagScanFromMetaEntry(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doTagScanFromMetaEntryNext(pOperator, &pRes);
  return pRes;
}

static void destroyTagScanOperatorInfo(void* param) {
  STagScanInfo* pInfo = (STagScanInfo*)param;
  if (pInfo->pCtbCursor != NULL && pInfo->pStorageAPI != NULL) {
    pInfo->pStorageAPI->metaFn.closeCtbCursor(pInfo->pCtbCursor);
  }
  taosHashCleanup(pInfo->filterCtx.colHash);
  taosArrayDestroy(pInfo->filterCtx.cInfoList);
  taosArrayDestroy(pInfo->aFilterIdxs);
  taosArrayDestroyEx(pInfo->aUidTags, tagScanFreeUidTag);

  blockDataDestroy(pInfo->pRes);
  taosArrayDestroy(pInfo->matchInfo.pList);
  tableListDestroy(pInfo->pTableListInfo);

  pInfo->pRes = NULL;
  pInfo->pTableListInfo = NULL;
  taosMemoryFreeClear(param);
}

int32_t createTagScanOperatorInfo(SReadHandle* pReadHandle, STagScanPhysiNode* pTagScanNode,
                                  STableListInfo* pTableListInfo, SNode* pTagCond, SNode* pTagIndexCond,
                                  SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SScanPhysiNode* pPhyNode = (SScanPhysiNode*)pTagScanNode;
  STagScanInfo*   pInfo = taosMemoryCalloc(1, sizeof(STagScanInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pPhyNode->node.pOutputDataBlockDesc;

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = NULL;

  code = createExprInfo(pPhyNode->pScanPseudoCols, NULL, &pExprInfo, &numOfExprs);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfExprs, &pTaskInfo->storageAPI.functionStore);
  QUERY_CHECK_CODE(code, lino, _error);

  int32_t num = 0;
  code = extractColMatchInfo(pPhyNode->pScanPseudoCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->pTagCond = pTagCond;
  pInfo->pTagIndexCond = pTagIndexCond;
  pInfo->suid = pPhyNode->suid;
  pInfo->pStorageAPI = &pTaskInfo->storageAPI;

  pInfo->pTableListInfo = pTableListInfo;
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  pInfo->readHandle = *pReadHandle;
  pInfo->curPos = 0;

  initLimitInfo(pPhyNode->node.pLimit, pPhyNode->node.pSlimit, &pInfo->limitInfo);
  setOperatorInfo(pOperator, "TagScanOperator", QUERY_NODE_PHYSICAL_PLAN_TAG_SCAN, false, OP_NOT_OPENED, pInfo,
                  pTaskInfo);
  initResultSizeInfo(&pOperator->resultInfo, 4096);
  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pTagScanNode->onlyMetaCtbIdx) {
    pInfo->aUidTags = taosArrayInit(pOperator->resultInfo.capacity, sizeof(STUidTagInfo));
    QUERY_CHECK_NULL(pInfo->aUidTags, code, lino, _error, terrno);

    pInfo->aFilterIdxs = taosArrayInit(pOperator->resultInfo.capacity, sizeof(int32_t));
    QUERY_CHECK_NULL(pInfo->aFilterIdxs, code, lino, _error, terrno);

    pInfo->filterCtx.colHash =
        taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_SMALLINT), false, HASH_NO_LOCK);
    QUERY_CHECK_NULL(pInfo->filterCtx.colHash, code, lino, _error, terrno);

    pInfo->filterCtx.cInfoList = taosArrayInit(4, sizeof(SColumnInfo));
    QUERY_CHECK_NULL(pInfo->filterCtx.cInfoList, code, lino, _error, terrno);

    if (pInfo->pTagCond != NULL) {
      nodesRewriteExprPostOrder(&pTagCond, tagScanRewriteTagColumn, (void*)&pInfo->filterCtx);
    }
  }
  __optr_fn_t tagScanNextFn = (pTagScanNode->onlyMetaCtbIdx) ? doTagScanFromCtbIdxNext : doTagScanFromMetaEntryNext;
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, tagScanNextFn, NULL, destroyTagScanOperatorInfo,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo) {
    pInfo->pTableListInfo = NULL;
  }

  if (pInfo != NULL) destroyTagScanOperatorInfo(pInfo);
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

// table merge scan operator

static int32_t subTblRowCompareTsFn(const void* pLeft, const void* pRight, void* param) {
  int32_t                 left = *(int32_t*)pLeft;
  int32_t                 right = *(int32_t*)pRight;
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
  int32_t ret = leftTs > rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
  if (pInfo->pTsOrderInfo->order == TSDB_ORDER_DESC) {
    ret = -1 * ret;
  }
  return ret;
}

static int32_t subTblRowCompareTsPkFn(const void* pLeft, const void* pRight, void* param) {
  int32_t                 left = *(int32_t*)pLeft;
  int32_t                 right = *(int32_t*)pRight;
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
  int32_t ret = leftTs > rightTs ? 1 : ((leftTs < rightTs) ? -1 : 0);
  if (pInfo->pTsOrderInfo->order == TSDB_ORDER_DESC) {
    ret = -1 * ret;
  }
  if (ret == 0 && pInfo->pPkOrderInfo) {
    ret = tsortComparBlockCell(pInfo->aInputs[left].pInputBlock, pInfo->aInputs[right].pInputBlock, leftIdx, rightIdx,
                               pInfo->pPkOrderInfo);
  }
  return ret;
}

int32_t dumpQueryTableCond(const SQueryTableDataCond* src, SQueryTableDataCond* dst) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  memcpy((void*)dst, (void*)src, sizeof(SQueryTableDataCond));
  dst->colList = taosMemoryCalloc(src->numOfCols, sizeof(SColumnInfo));
  QUERY_CHECK_NULL(dst->colList, code, lino, _end, terrno);
  for (int i = 0; i < src->numOfCols; i++) {
    dst->colList[i] = src->colList[i];
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t fetchNextSubTableBlockFromReader(SOperatorInfo* pOperator, STmsSubTableInput* pInput,
                                                bool* pSubTableHasBlock) {
  int32_t code = 0;

  STableMergeScanInfo*    pInfo = pOperator->info;
  SReadHandle*            pHandle = &pInfo->base.readHandle;
  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  const SStorageAPI*      pAPI = &pTaskInfo->storageAPI;

  blockDataCleanup(pInput->pReaderBlock);
  if (!pInput->bInMemReader) {
    code = pAPI->tsdReader.tsdReaderOpen(pHandle->vnode, &pInput->tblCond, pInput->pKeyInfo, 1, pInput->pReaderBlock,
                                         (void**)&pInput->pReader, GET_TASKID(pTaskInfo), NULL);
    if (code != 0) {
      return code;
    }
  }

  pInfo->base.dataReader = pInput->pReader;

  while (true) {
    bool hasNext = false;
    code = pAPI->tsdReader.tsdNextDataBlock(pInfo->base.dataReader, &hasNext);
    if (code != 0) {
      pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->base.dataReader);
      pInfo->base.dataReader = NULL;
      return code;
    }

    if (!hasNext || isTaskKilled(pTaskInfo)) {
      if (isTaskKilled(pTaskInfo)) {
        pAPI->tsdReader.tsdReaderReleaseDataBlock(pInfo->base.dataReader);
        pInfo->base.dataReader = NULL;
        return code;
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
      return code;
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
    pInput->pReaderBlock->info.id.groupId =
        tableListGetTableGroupId(pInfo->base.pTableListInfo, pInput->pReaderBlock->info.id.uid);
    pOperator->resultInfo.totalRows += pInput->pReaderBlock->info.rows;
  }
  if (!pInput->bInMemReader || !*pSubTableHasBlock) {
    pAPI->tsdReader.tsdReaderClose(pInput->pReader);
    pInput->pReader = NULL;
  }

  pInfo->base.dataReader = NULL;
  return TSDB_CODE_SUCCESS;
}

static int32_t setGroupStartEndIndex(STableMergeScanInfo* pInfo) {
  pInfo->bGroupProcessed = false;

  int32_t numOfTables = 0;
  int32_t code = tableListGetSize(pInfo->base.pTableListInfo, &numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }

  int32_t i = pInfo->tableStartIndex + 1;
  for (; i < numOfTables; ++i) {
    STableKeyInfo* tableKeyInfo = tableListGetInfo(pInfo->base.pTableListInfo, i);
    if (!tableKeyInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    if (tableKeyInfo->groupId != pInfo->groupId) {
      break;
    }
  }
  pInfo->tableEndIndex = i - 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t openSubTablesMergeSort(STmsSubTablesMergeInfo* pSubTblsInfo) {
  for (int32_t i = 0; i < pSubTblsInfo->numSubTables; ++i) {
    STmsSubTableInput* pInput = pSubTblsInfo->aInputs + i;
    if (pInput->rowIdx == -1) {
      continue;
    }

    if (pInput->type == SUB_TABLE_MEM_BLOCK) {
      pInput->rowIdx = 0;
      pInput->pageIdx = -1;
    }

    pInput->pInputBlock = (pInput->type == SUB_TABLE_MEM_BLOCK) ? pInput->pReaderBlock : pInput->pPageBlock;
    SColumnInfoData* col = taosArrayGet(pInput->pInputBlock->pDataBlock, pSubTblsInfo->pTsOrderInfo->slotId);
    if (!col) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
    pInput->aTs = (int64_t*)col->pData;
  }

  __merge_compare_fn_t mergeCompareFn = (!pSubTblsInfo->pPkOrderInfo) ? subTblRowCompareTsFn : subTblRowCompareTsPkFn;
  return tMergeTreeCreate(&pSubTblsInfo->pTree, pSubTblsInfo->numSubTables, pSubTblsInfo, mergeCompareFn);
}

static int32_t initSubTablesMergeInfo(STableMergeScanInfo* pInfo) {
  int32_t code = setGroupStartEndIndex(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
    return code;
  }
  STmsSubTablesMergeInfo* pSubTblsInfo = taosMemoryCalloc(1, sizeof(STmsSubTablesMergeInfo));
  if (pSubTblsInfo == NULL) {
    return terrno;
  }
  pSubTblsInfo->pTsOrderInfo = taosArrayGet(pInfo->pSortInfo, 0);
  if (!pSubTblsInfo->pTsOrderInfo) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }
  if (taosArrayGetSize(pInfo->pSortInfo) == 2) {
    pSubTblsInfo->pPkOrderInfo = taosArrayGet(pInfo->pSortInfo, 1);
    if (!pSubTblsInfo->pPkOrderInfo) {
      qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
      return terrno;
    }
  } else {
    pSubTblsInfo->pPkOrderInfo = NULL;
  }
  pSubTblsInfo->numSubTables = pInfo->tableEndIndex - pInfo->tableStartIndex + 1;
  pSubTblsInfo->aInputs = taosMemoryCalloc(pSubTblsInfo->numSubTables, sizeof(STmsSubTableInput));
  if (pSubTblsInfo->aInputs == NULL) {
    taosMemoryFree(pSubTblsInfo);
    return terrno;
  }
  int32_t bufPageSize = pInfo->bufPageSize;
  int32_t inMemSize = (pSubTblsInfo->numSubTables - pSubTblsInfo->numTableBlocksInMem) * bufPageSize;
  code = createDiskbasedBuf(&pSubTblsInfo->pBlocksBuf, pInfo->bufPageSize, inMemSize, "blocksExternalBuf", tsTempDir);
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
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SReadHandle*   pHandle = &pInfo->base.readHandle;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;

  for (int32_t i = 0; i < pSubTblsInfo->numSubTables; ++i) {
    STmsSubTableInput* pInput = pSubTblsInfo->aInputs + i;
    pInput->type = SUB_TABLE_MEM_BLOCK;

    code = dumpQueryTableCond(&pInfo->base.cond, &pInput->tblCond);
    QUERY_CHECK_CODE(code, lino, _end);

    code = createOneDataBlock(pInfo->pResBlock, false, &pInput->pReaderBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    code = createOneDataBlock(pInfo->pResBlock, false, &pInput->pPageBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    STableKeyInfo* keyInfo = tableListGetInfo(pInfo->base.pTableListInfo, i + pInfo->tableStartIndex);
    pInput->pKeyInfo = keyInfo;

    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, pTaskInfo->code);
    }

    if (i + 1 < pSubTblsInfo->numInMemReaders) {
      code = pAPI->tsdReader.tsdReaderOpen(pHandle->vnode, &pInput->tblCond, keyInfo, 1, pInput->pReaderBlock,
                                           (void**)&pInput->pReader, GET_TASKID(pTaskInfo), NULL);
      QUERY_CHECK_CODE(code, lino, _end);
      pInput->bInMemReader = true;
    } else {
      pInput->pReader = NULL;
      pInput->bInMemReader = false;
    }
    bool hasNext = true;
    code = fetchNextSubTableBlockFromReader(pOperator, pInput, &hasNext);
    QUERY_CHECK_CODE(code, lino, _end);
    if (!hasNext) {
      pInput->rowIdx = -1;
      ++pSubTblsInfo->numSubTablesCompleted;
      continue;
    } else {
      pInput->rowIdx = 0;
      pInput->pageIdx = -1;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void adjustSubTableFromMemBlock(SOperatorInfo* pOperatorInfo, STmsSubTablesMergeInfo* pSubTblsInfo) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SExecTaskInfo*       pTaskInfo = pOperatorInfo->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperatorInfo->info;
  STmsSubTableInput*   pInput = pSubTblsInfo->aInputs + tMergeTreeGetChosenIndex(pSubTblsInfo->pTree);
  bool                 hasNext = true;
  code = fetchNextSubTableBlockFromReader(pOperatorInfo, pInput, &hasNext);
  QUERY_CHECK_CODE(code, lino, _end);

  if (!hasNext) {
    pInput->rowIdx = -1;
    ++pSubTblsInfo->numSubTablesCompleted;
  } else {
    pInput->rowIdx = 0;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t adjustSubTableForNextRow(SOperatorInfo* pOperatorInfo, STmsSubTablesMergeInfo* pSubTblsInfo) {
  STableMergeScanInfo* pInfo = pOperatorInfo->info;
  STmsSubTableInput*   pInput = pSubTblsInfo->aInputs + tMergeTreeGetChosenIndex(pSubTblsInfo->pTree);

  SSDataBlock* pInputBlock = (pInput->type == SUB_TABLE_MEM_BLOCK) ? pInput->pReaderBlock : pInput->pPageBlock;
  if (pInput->rowIdx < pInputBlock->info.rows - 1) {
    ++pInput->rowIdx;
  } else if (pInput->rowIdx == pInputBlock->info.rows - 1) {
    if (pInput->type == SUB_TABLE_MEM_BLOCK) {
      adjustSubTableFromMemBlock(pOperatorInfo, pSubTblsInfo);
    }
    if (pInput->rowIdx != -1) {
      SColumnInfoData* col = taosArrayGet(pInputBlock->pDataBlock, pSubTblsInfo->pTsOrderInfo->slotId);
      if (!col) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
        return terrno;
      }
      pInput->pInputBlock = pInputBlock;
      pInput->aTs = (int64_t*)col->pData;
    }
  }

  return tMergeTreeAdjust(pSubTblsInfo->pTree, tMergeTreeGetAdjustIndex(pSubTblsInfo->pTree));
}

static int32_t appendChosenRowToDataBlock(STmsSubTablesMergeInfo* pSubTblsInfo, SSDataBlock* pBlock) {
  int32_t            code = TSDB_CODE_SUCCESS;
  int32_t            lino = 0;
  STmsSubTableInput* pInput = pSubTblsInfo->aInputs + tMergeTreeGetChosenIndex(pSubTblsInfo->pTree);
  SSDataBlock*       pInputBlock = (pInput->type == SUB_TABLE_MEM_BLOCK) ? pInput->pReaderBlock : pInput->pPageBlock;

  for (int32_t i = 0; i < taosArrayGetSize(pBlock->pDataBlock); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pBlock->pDataBlock, i);
    QUERY_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    SColumnInfoData* pSrcColInfo = taosArrayGet(pInputBlock->pDataBlock, i);
    QUERY_CHECK_NULL(pSrcColInfo, code, lino, _end, terrno);
    bool isNull = colDataIsNull(pSrcColInfo, pInputBlock->info.rows, pInput->rowIdx, NULL);

    if (isNull) {
      code = colDataSetVal(pColInfo, pBlock->info.rows, NULL, true);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      if (pSrcColInfo->pData != NULL) {
        char* pData = colDataGetData(pSrcColInfo, pInput->rowIdx);
        code = colDataSetVal(pColInfo, pBlock->info.rows, pData, false);
        QUERY_CHECK_CODE(code, lino, _end);
      }
    }
  }
  pBlock->info.dataLoad = 1;
  pBlock->info.scanFlag = pInputBlock->info.scanFlag;
  pBlock->info.rows += 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t getSubTablesSortedBlock(SOperatorInfo* pOperator, SSDataBlock* pBlock, int32_t capacity,
                                       SSDataBlock** pResBlock) {
  int32_t                 code = TSDB_CODE_SUCCESS;
  int32_t                 lino = 0;
  STableMergeScanInfo*    pInfo = pOperator->info;
  SExecTaskInfo*          pTaskInfo = pOperator->pTaskInfo;
  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;
  bool                    finished = false;

  QRY_PARAM_CHECK(pResBlock);

  blockDataCleanup(pBlock);

  while (true) {
    while (true) {
      if (pSubTblsInfo->numSubTablesCompleted >= pSubTblsInfo->numSubTables) {
        finished = true;
        break;
      }

      code = appendChosenRowToDataBlock(pSubTblsInfo, pBlock);
      QUERY_CHECK_CODE(code, lino, _end);

      code = adjustSubTableForNextRow(pOperator, pSubTblsInfo);
      QUERY_CHECK_CODE(code, lino, _end);

      if (pBlock->info.rows >= capacity) {
        break;
      }
    }

    if (isTaskKilled(pTaskInfo)) {
      return pTaskInfo->code;
    }

    bool limitReached = applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo);
    if (finished || limitReached || pBlock->info.rows > 0) {
      break;
    }
  }

  if (pBlock->info.rows > 0) {
    *pResBlock = pBlock;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static int32_t startSubTablesTableMergeScan(SOperatorInfo* pOperator) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  STableMergeScanInfo* pInfo = pOperator->info;

  code = initSubTablesMergeInfo(pInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = initSubTableInputs(pOperator, pInfo);
  QUERY_CHECK_CODE(code, lino, _end);

  code = openSubTablesMergeSort(pInfo->pSubTablesMergeInfo);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void stopSubTablesTableMergeScan(STableMergeScanInfo* pInfo) {
  STmsSubTablesMergeInfo* pSubTblsInfo = pInfo->pSubTablesMergeInfo;
  if (pSubTblsInfo != NULL) {
    tMergeTreeDestroy(&pSubTblsInfo->pTree);

    for (int32_t i = 0; i < pSubTblsInfo->numSubTables; ++i) {
      STmsSubTableInput* pInput = pSubTblsInfo->aInputs + i;
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

    // taosMemoryTrim(0);
  }
}

int32_t doTableMergeScanParaSubTablesNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  QRY_PARAM_CHECK(ppRes);

  int32_t lino = 0;
  int32_t tableListSize = 0;
  int64_t st = taosGetTimestampUs();

  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperator->info;

  if (pOperator->status == OP_EXEC_DONE) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tableListGetSize(pInfo->base.pTableListInfo, &tableListSize);
  QUERY_CHECK_CODE(code, lino, _end);

  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      setOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      return code;
    }

    pInfo->tableStartIndex = 0;
    STableKeyInfo* pTmpGpId = (STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
    QUERY_CHECK_NULL(pTmpGpId, code, lino, _end, terrno);
    pInfo->groupId = pTmpGpId->groupId;
    code = startSubTablesTableMergeScan(pOperator);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->tableStartIndex < tableListSize) {
    if (isTaskKilled(pTaskInfo)) {
      break;
    }

    code = getSubTablesSortedBlock(pOperator, pInfo->pResBlock, pOperator->resultInfo.capacity, &pBlock);
    QUERY_CHECK_CODE(code, lino, _end);

    if (pBlock == NULL && !pInfo->bGroupProcessed && pInfo->needCountEmptyTable) {
      STableKeyInfo* tbInfo = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
      QUERY_CHECK_NULL(tbInfo, code, lino, _end, terrno);

      pBlock = getOneRowResultBlock(pTaskInfo, &pInfo->base, pInfo->pResBlock, tbInfo);
    }

    if (pBlock != NULL) {
      pBlock->info.id.groupId = pInfo->groupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      pInfo->bGroupProcessed = true;
      break;
    } else {
      // Data of this group are all dumped, let's try the next group
      stopSubTablesTableMergeScan(pInfo);
      if (pInfo->tableEndIndex >= tableListSize - 1) {
        setOperatorCompleted(pOperator);
        break;
      }

      pInfo->tableStartIndex = pInfo->tableEndIndex + 1;
      STableKeyInfo* pTmpGpId = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
      QUERY_CHECK_NULL(pTmpGpId, code, lino, _end, terrno);

      pInfo->groupId = pTmpGpId->groupId;
      code = startSubTablesTableMergeScan(pOperator);
      QUERY_CHECK_CODE(code, lino, _end);
      resetLimitInfoForNextGroup(&pInfo->limitInfo);
    }
  }

  pOperator->cost.totalCost += (taosGetTimestampUs() - st) / 1000.0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  } else {
    (*ppRes) = pBlock;
  }

  return code;
}

static void tableMergeScanDoSkipTable(uint64_t uid, void* pTableMergeOpInfo) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SOperatorInfo*       pOperator = (SOperatorInfo*)pTableMergeOpInfo;
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;

  if (pInfo->mSkipTables == NULL) {
    pInfo->mSkipTables = taosHashInit(pInfo->tableEndIndex - pInfo->tableStartIndex + 1,
                                      taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
    QUERY_CHECK_NULL(pInfo->mSkipTables, code, lino, _end, terrno);
  }
  int bSkip = 1;
  if (pInfo->mSkipTables != NULL) {
    code = taosHashPut(pInfo->mSkipTables, &uid, sizeof(uid), &bSkip, sizeof(bSkip));
    if (code == TSDB_CODE_DUP_KEY) {
      code = TSDB_CODE_SUCCESS;
    }
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t doGetBlockForTableMergeScan(SOperatorInfo* pOperator, bool* pFinished, bool* pSkipped) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*         pAPI = &pTaskInfo->storageAPI;
  SSDataBlock*         pBlock = pInfo->pReaderBlock;
  int32_t              code = 0;
  bool                 hasNext = false;
  STsdbReader*         reader = pInfo->base.dataReader;

  code = pAPI->tsdReader.tsdNextDataBlock(reader, &hasNext);
  if (code != 0) {
    pAPI->tsdReader.tsdReaderReleaseDataBlock(reader);
    qError("table merge scan fetch next data block error code: %d, %s", code, GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
    return code;
  }

  if (!hasNext || isTaskKilled(pTaskInfo)) {
    if (isTaskKilled(pTaskInfo)) {
      qInfo("table merge scan fetch next data block found task killed. %s", GET_TASKID(pTaskInfo));
      pAPI->tsdReader.tsdReaderReleaseDataBlock(reader);
    }
    *pFinished = true;
    return code;
  }

  uint32_t status = 0;
  code = loadDataBlock(pOperator, &pInfo->base, pBlock, &status);

  if (code != TSDB_CODE_SUCCESS) {
    qInfo("table merge scan load datablock code %d, %s", code, GET_TASKID(pTaskInfo));
    pTaskInfo->code = code;
    return code;
  }

  if (status == FUNC_DATA_REQUIRED_ALL_FILTEROUT) {
    *pFinished = true;
    return code;
  }

  // current block is filter out according to filter condition, continue load the next block
  if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
    *pSkipped = true;
    return code;
  }

  return code;
}

static int32_t getBlockForTableMergeScan(void* param, SSDataBlock** ppBlock) {
  STableMergeScanSortSourceParam* source = param;

  SOperatorInfo*       pOperator = source->pOperator;
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  SSDataBlock*         pBlock = NULL;
  int64_t              st = taosGetTimestampUs();
  int32_t              code = TSDB_CODE_SUCCESS;

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

      code = doGetBlockForTableMergeScan(pOperator, &bFinished, &bSkipped);
      if (code != 0) {
        return code;
      }

      pBlock = pInfo->pReaderBlock;
      qDebug("%s table merge scan fetch block. finished %d skipped %d next-duration-block %d new-fileset %d",
             GET_TASKID(pTaskInfo), bFinished, bSkipped, pInfo->bNextDurationBlockEvent, pInfo->bNewFilesetEvent);
      if (bFinished) {
        pInfo->bNewFilesetEvent = false;
        break;
      }

      if (pInfo->bNextDurationBlockEvent || pInfo->bNewFilesetEvent) {
        if (!bSkipped) {
          code = createOneDataBlock(pBlock, true, &pInfo->nextDurationBlocks[pInfo->numNextDurationBlocks]);
          if (code) {
            *ppBlock = NULL;
            return code;
          }

          ++pInfo->numNextDurationBlocks;
          if (pInfo->numNextDurationBlocks > 2) {
            qError("%s table merge scan prefetch %d next duration blocks. end early.", GET_TASKID(pTaskInfo),
                   pInfo->numNextDurationBlocks);
            pInfo->bNewFilesetEvent = false;
            break;
          }
        }

        if (pInfo->bNewFilesetEvent) {
          pInfo->rtnNextDurationBlocks = true;
          *ppBlock = NULL;
          return code;
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
    *ppBlock = pBlock;

    return code;
  }

  *ppBlock = NULL;
  return code;
}

int32_t generateSortByTsPkInfo(SArray* colMatchInfo, int32_t order, SArray** ppSortArray) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SArray* pSortInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
  QUERY_CHECK_NULL(pSortInfo, code, lino, _end, terrno);
  SBlockOrderInfo biTs = {0};
  SBlockOrderInfo biPk = {0};

  int32_t tsTargetSlotId = 0;
  int32_t pkTargetSlotId = -1;
  for (int32_t i = 0; i < taosArrayGetSize(colMatchInfo); ++i) {
    SColMatchItem* colInfo = taosArrayGet(colMatchInfo, i);
    QUERY_CHECK_NULL(colInfo, code, lino, _end, terrno);
    if (colInfo->colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
      tsTargetSlotId = colInfo->dstSlotId;
      biTs.order = order;
      biTs.slotId = tsTargetSlotId;
      biTs.nullFirst = (order == TSDB_ORDER_ASC);
      biTs.compFn = getKeyComparFunc(TSDB_DATA_TYPE_TIMESTAMP, order);
    }
    // TODO: order by just ts
    if (colInfo->isPk) {
      pkTargetSlotId = colInfo->dstSlotId;
      biPk.order = order;
      biPk.slotId = pkTargetSlotId;
      biPk.nullFirst = (order == TSDB_ORDER_ASC);
      biPk.compFn = getKeyComparFunc(colInfo->dataType.type, order);
    }
  }

  void* tmp = taosArrayPush(pSortInfo, &biTs);
  QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  if (pkTargetSlotId != -1) {
    tmp = taosArrayPush(pSortInfo, &biPk);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
  }

  (*ppSortArray) = pSortInfo;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t lino = 0;
  int32_t numOfTable = pInfo->tableEndIndex - pInfo->tableStartIndex + 1;

  qDebug("%s table merge scan start duration ", GET_TASKID(pTaskInfo));
  pInfo->bNewFilesetEvent = false;
  pInfo->bNextDurationBlockEvent = false;

  pInfo->sortBufSize = 2048 * pInfo->bufPageSize;
  int32_t numOfBufPage = pInfo->sortBufSize / pInfo->bufPageSize;

  pInfo->pSortHandle = NULL;
  code = tsortCreateSortHandle(pInfo->pSortInfo, SORT_BLOCK_TS_MERGE, pInfo->bufPageSize, numOfBufPage,
                               pInfo->pSortInputBlock, pTaskInfo->id.str, 0, 0, 0, &pInfo->pSortHandle);
  if (code) {
    return code;
  }

  if (pInfo->bSortRowId && numOfTable != 1) {
    int32_t memSize = 512 * 1024 * 1024;
    code = tsortSetSortByRowId(pInfo->pSortHandle, memSize);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  tsortSetMergeLimit(pInfo->pSortHandle, pInfo->mergeLimit);
  tsortSetMergeLimitReachedFp(pInfo->pSortHandle, tableMergeScanDoSkipTable, pOperator);
  tsortSetAbortCheckFn(pInfo->pSortHandle, isTaskKilled, pOperator->pTaskInfo);

  tsortSetFetchRawDataFp(pInfo->pSortHandle, getBlockForTableMergeScan, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);

  STableMergeScanSortSourceParam* param = taosMemoryCalloc(1, sizeof(STableMergeScanSortSourceParam));
  QUERY_CHECK_NULL(param, code, lino, _end, terrno);
  param->pOperator = pOperator;

  SSortSource* ps = taosMemoryCalloc(1, sizeof(SSortSource));
  if (ps == NULL) {
    taosMemoryFree(param);
    QUERY_CHECK_NULL(ps, code, lino, _end, terrno);
  }

  ps->param = param;
  ps->onlyRef = false;
  code = tsortAddSource(pInfo->pSortHandle, ps);
  QUERY_CHECK_CODE(code, lino, _end);

  if (numOfTable == 1) {
    tsortSetSingleTableMerge(pInfo->pSortHandle);
  } else {
    code = tsortOpen(pInfo->pSortHandle);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void stopDurationForGroupTableMergeScan(SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
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

void startGroupTableMergeScan(SOperatorInfo* pOperator) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  SReadHandle*         pHandle = &pInfo->base.readHandle;
  SStorageAPI*         pAPI = &pTaskInfo->storageAPI;
  qDebug("%s table merge scan start group %" PRIu64, GET_TASKID(pTaskInfo), pInfo->groupId);

  {
    int32_t numOfTables = 0;
    code = tableListGetSize(pInfo->base.pTableListInfo, &numOfTables);
    QUERY_CHECK_CODE(code, lino, _end);

    int32_t i = pInfo->tableStartIndex + 1;
    for (; i < numOfTables; ++i) {
      STableKeyInfo* tableKeyInfo = tableListGetInfo(pInfo->base.pTableListInfo, i);
      QUERY_CHECK_NULL(tableKeyInfo, code, lino, _end, terrno);
      if (tableKeyInfo->groupId != pInfo->groupId) {
        break;
      }
    }
    pInfo->tableEndIndex = i - 1;
  }
  pInfo->bGroupProcessed = false;
  int32_t tableStartIdx = pInfo->tableStartIndex;
  int32_t tableEndIdx = pInfo->tableEndIndex;

  int32_t        numOfTable = tableEndIdx - tableStartIdx + 1;
  STableKeyInfo* startKeyInfo = tableListGetInfo(pInfo->base.pTableListInfo, tableStartIdx);
  code = pAPI->tsdReader.tsdReaderOpen(pHandle->vnode, &pInfo->base.cond, startKeyInfo, numOfTable, pInfo->pReaderBlock,
                                       (void**)&pInfo->base.dataReader, GET_TASKID(pTaskInfo), &pInfo->mSkipTables);
  QUERY_CHECK_CODE(code, lino, _end);
  if (pInfo->filesetDelimited) {
    pAPI->tsdReader.tsdSetFilesetDelimited(pInfo->base.dataReader);
  }
  pAPI->tsdReader.tsdSetSetNotifyCb(pInfo->base.dataReader, tableMergeScanTsdbNotifyCb, pInfo);

  code = startDurationForGroupTableMergeScan(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

void stopGroupTableMergeScan(SOperatorInfo* pOperator) {
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
  qDebug("%s table merge scan stop group %" PRIu64, GET_TASKID(pTaskInfo), pInfo->groupId);
}

// all data produced by this function only belongs to one group
// slimit/soffset does not need to be concerned here, since this function only deal with data within one group.
SSDataBlock* getSortedTableMergeScanBlockData(SSortHandle* pHandle, SSDataBlock* pResBlock, int32_t capacity,
                                              SOperatorInfo* pOperator) {
  STableMergeScanInfo* pInfo = pOperator->info;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STupleHandle*        pTupleHandle = NULL;

  blockDataCleanup(pResBlock);

  while (1) {
    while (1) {
      pTupleHandle = NULL;
      int32_t code = tsortNextTuple(pHandle, &pTupleHandle);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        T_LONG_JMP(pOperator->pTaskInfo->env, code);
      }
      if (pTupleHandle == NULL) {
        break;
      }

      code = tsortAppendTupleToBlock(pInfo->pSortHandle, pResBlock, pTupleHandle);
      if (code != TSDB_CODE_SUCCESS) {
        qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(code));
        T_LONG_JMP(pOperator->pTaskInfo->env, terrno);
      }

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

int32_t doTableMergeScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  if (pOperator->status == OP_EXEC_DONE) {
    (*ppRes) = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperator->info;

  code = pOperator->fpSet._openFn(pOperator);
  QUERY_CHECK_CODE(code, lino, _end);

  int64_t st = taosGetTimestampUs();

  int32_t tableListSize = 0;
  code = tableListGetSize(pInfo->base.pTableListInfo, &tableListSize);
  QUERY_CHECK_CODE(code, lino, _end);

  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      setOperatorCompleted(pOperator);
      (*ppRes) = NULL;
      return code;
    }
    pInfo->tableStartIndex = 0;
    STableKeyInfo* tmp = (STableKeyInfo*)tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
    QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
    pInfo->groupId = tmp->groupId;
    startGroupTableMergeScan(pOperator);
  }

  SSDataBlock* pBlock = NULL;
  while (pInfo->tableStartIndex < tableListSize) {
    if (isTaskKilled(pTaskInfo)) {
      goto _end;
    }

    pBlock = getSortedTableMergeScanBlockData(pInfo->pSortHandle, pInfo->pResBlock, pOperator->resultInfo.capacity,
                                              pOperator);
    if (pBlock == NULL && !pInfo->bGroupProcessed && pInfo->needCountEmptyTable) {
      STableKeyInfo* tbInfo = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
      QUERY_CHECK_NULL(tbInfo, code, lino, _end, terrno);
      pBlock = getOneRowResultBlock(pTaskInfo, &pInfo->base, pInfo->pResBlock, tbInfo);
    }
    if (pBlock != NULL) {
      pBlock->info.id.groupId = pInfo->groupId;
      pOperator->resultInfo.totalRows += pBlock->info.rows;
      pInfo->bGroupProcessed = true;
      break;
    } else {
      if (pInfo->bNewFilesetEvent) {
        stopDurationForGroupTableMergeScan(pOperator);
        code = startDurationForGroupTableMergeScan(pOperator);
        QUERY_CHECK_CODE(code, lino, _end);
      } else {
        // Data of this group are all dumped, let's try the next group
        stopGroupTableMergeScan(pOperator);
        if (pInfo->tableEndIndex >= tableListSize - 1) {
          setOperatorCompleted(pOperator);
          break;
        }

        pInfo->tableStartIndex = pInfo->tableEndIndex + 1;
        STableKeyInfo* tmp = tableListGetInfo(pInfo->base.pTableListInfo, pInfo->tableStartIndex);
        QUERY_CHECK_NULL(tmp, code, lino, _end, terrno);
        pInfo->groupId = tmp->groupId;
        startGroupTableMergeScan(pOperator);
        resetLimitInfoForNextGroup(&pInfo->limitInfo);
      }
    }
  }

  pOperator->cost.totalCost += (taosGetTimestampUs() - st) / 1000.0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  } else {
    (*ppRes) = pBlock;
  }

  return code;
}

static SSDataBlock* doTableMergeScan(SOperatorInfo* pOperator) {
  SSDataBlock* pRes = NULL;
  int32_t      code = doTableMergeScanNext(pOperator, &pRes);
  return pRes;
}

void destroyTableMergeScanOperatorInfo(void* param) {
  STableMergeScanInfo* pTableScanInfo = (STableMergeScanInfo*)param;

  // start one reader variable
  if (pTableScanInfo->base.readerAPI.tsdReaderClose != NULL) {
    pTableScanInfo->base.readerAPI.tsdReaderClose(pTableScanInfo->base.dataReader);
    pTableScanInfo->base.dataReader = NULL;
  }

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
  blockDataDestroy(pTableScanInfo->pSortInputBlock);
  pTableScanInfo->pSortInputBlock = NULL;
  // end one reader variable

  destroyTableScanBase(&pTableScanInfo->base, &pTableScanInfo->base.readerAPI);

  blockDataDestroy(pTableScanInfo->pResBlock);
  pTableScanInfo->pResBlock = NULL;

  // remove it from the task->result list
  blockDataDestroy(pTableScanInfo->pReaderBlock);
  pTableScanInfo->pReaderBlock = NULL;
  taosArrayDestroy(pTableScanInfo->pSortInfo);

  stopSubTablesTableMergeScan(pTableScanInfo);

  taosMemoryFreeClear(param);
}

int32_t getTableMergeScanExplainExecInfo(SOperatorInfo* pOptr, void** pOptrExplain, uint32_t* len) {
  if (pOptr == NULL) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(TSDB_CODE_INVALID_PARA));
    return TSDB_CODE_INVALID_PARA;
  }
  // TODO: merge these two info into one struct
  STableMergeScanExecInfo* execInfo = taosMemoryCalloc(1, sizeof(STableMergeScanExecInfo));
  if (!execInfo) {
    return terrno;
  }
  STableMergeScanInfo* pInfo = pOptr->info;
  execInfo->blockRecorder = pInfo->base.readRecorder;
  execInfo->sortExecInfo = pInfo->sortExecInfo;

  *pOptrExplain = execInfo;
  *len = sizeof(STableMergeScanExecInfo);

  return TSDB_CODE_SUCCESS;
}

static int32_t resetTableMergeScanOperatorState(SOperatorInfo* pOper) {
  int32_t         code = TSDB_CODE_SUCCESS;
  STableMergeScanInfo*   pInfo = pOper->info;
  pOper->status = OP_NOT_OPENED;

  pInfo->tableEndIndex = 0;
  pInfo->tableStartIndex = 0;
  pInfo->hasGroupId = false;
  if (pInfo->base.readerAPI.tsdReaderClose) {
    pInfo->base.readerAPI.tsdReaderClose(pInfo->base.dataReader);
  }
  pInfo->base.dataReader = NULL;
  pInfo->base.scanFlag = MAIN_SCAN;

  pInfo->base.limitInfo = (SLimitInfo){0};
  pInfo->base.limitInfo.limit.limit = -1;
  pInfo->base.limitInfo.slimit.limit = -1;

  tableListDestroy(pInfo->base.pTableListInfo);
  pInfo->base.pTableListInfo = tableListCreate();
  if (!pInfo->base.pTableListInfo) {
    qError("%s failed at line %d since %s", __func__, __LINE__, tstrerror(terrno));
    return terrno;
  }
  SExecTaskInfo*         pTaskInfo = pOper->pTaskInfo;

  STableScanPhysiNode* pTableScanNode = (STableScanPhysiNode*)pTaskInfo->pSubplan->pNode;
  code = createScanTableListInfo(&pTableScanNode->scan, pTableScanNode->pGroupTags, pTableScanNode->groupSort,
                                  &pInfo->base.readHandle, pInfo->base.pTableListInfo, 
                                  pTaskInfo->pSubplan->pTagCond, pTaskInfo->pSubplan->pTagIndexCond, pTaskInfo, NULL);
  if (code) {
    qError("%s failed to createScanTableListInfo, code:%s", __func__, tstrerror(code));
    return code;
  }

  initLimitInfo(pTableScanNode->scan.node.pLimit, pTableScanNode->scan.node.pSlimit, &pInfo->limitInfo);
  cleanupQueryTableDataCond(&pInfo->base.cond);
  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode, &pInfo->base.readHandle, false);
  if (code) {
    qError("%s failed to initQueryTableDataCond, code:%s", __func__, tstrerror(code));
    return code;
  }

  tsortDestroySortHandle(pInfo->pSortHandle);
  pInfo->pSortHandle = NULL;

  taosHashCleanup(pInfo->mSkipTables);
  pInfo->mSkipTables = NULL;
  pOper->resultInfo.totalRows = 0;
  
  pInfo->bGroupProcessed = false;
  pInfo->bNewFilesetEvent = false;
  pInfo->bNextDurationBlockEvent = false;
  pInfo->rtnNextDurationBlocks = false;
  pInfo->nextDurationBlocksIdx = 0;
  pInfo->bSortRowId = false;

  for (int32_t i = 0; i < pInfo->numNextDurationBlocks; ++i) {
    if (pInfo->nextDurationBlocks[i] != NULL) {
      blockDataDestroy(pInfo->nextDurationBlocks[i]);
      pInfo->nextDurationBlocks[i] = NULL;
    }
  }
  pInfo->numNextDurationBlocks = 0;

  stopSubTablesTableMergeScan(pInfo);
  return code;
}

int32_t createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, SReadHandle* readHandle,
                                         STableListInfo* pTableListInfo, SExecTaskInfo* pTaskInfo,
                                         SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  STableMergeScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableMergeScanInfo));
  SOperatorInfo*       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    code = terrno;
    goto _error;
  }

  pOperator->pPhyNode = pTableScanNode;
  SDataBlockDescNode* pDescNode = pTableScanNode->scan.node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  code = extractColMatchInfo(pTableScanNode->scan.pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                             &pInfo->base.matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = initQueryTableDataCond(&pInfo->base.cond, pTableScanNode, readHandle, false);
  QUERY_CHECK_CODE(code, lino, _error);

  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->base.pseudoSup;
    code = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pSup->pExprInfo, &pSup->numOfExprs);
    QUERY_CHECK_CODE(code, lino, _error);

    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset,
                                      &pTaskInfo->storageAPI.functionStore);
    QUERY_CHECK_NULL(pSup->pCtx, code, lino, _error, terrno);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};

  if (readHandle->streamRtInfo == NULL) {
    pInfo->base.metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024 * 128, -1, .5);
    QUERY_CHECK_NULL(pInfo->base.metaCache.pTableMetaEntryCache, code, lino, _error, terrno);
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

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0,
                            pTaskInfo->pStreamRuntimeInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  initLimitInfo(pTableScanNode->scan.node.pLimit, pTableScanNode->scan.node.pSlimit, &pInfo->limitInfo);

  pInfo->mergeLimit = -1;
  bool hasLimit = pInfo->limitInfo.limit.limit != -1 || pInfo->limitInfo.limit.offset != -1;
  if (hasLimit) {
    pInfo->mergeLimit = pInfo->limitInfo.limit.offset != -1
                            ? pInfo->limitInfo.limit.limit + pInfo->limitInfo.limit.offset
                            : pInfo->limitInfo.limit.limit;
    pInfo->mSkipTables = NULL;
  }

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  pInfo->pResBlock = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pResBlock, code, lino, _error, terrno);
  code = blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);
  if (!hasLimit && blockDataGetRowSize(pInfo->pResBlock) >= 256 && !pTableScanNode->smallDataTsSort) {
    pInfo->bSortRowId = true;
  } else {
    pInfo->bSortRowId = false;
  }

  code = prepareDataBlockBuf(pInfo->pResBlock, &pInfo->base.matchInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = generateSortByTsPkInfo(pInfo->base.matchInfo.pList, pInfo->base.cond.order, &pInfo->pSortInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  code = createOneDataBlock(pInfo->pResBlock, false, &pInfo->pReaderBlock);
  QUERY_CHECK_CODE(code, lino, _error);

  pInfo->needCountEmptyTable = tsCountAlwaysReturnValue && pTableScanNode->needCountEmptyTable;

  int32_t  rowSize = pInfo->pResBlock->info.rowSize;
  uint32_t nCols = taosArrayGetSize(pInfo->pResBlock->pDataBlock);

  pInfo->bufPageSize = getProperSortPageSize(rowSize, nCols);

  // start one reader variable
  code = createOneDataBlock(pInfo->pResBlock, false, &pInfo->pSortInputBlock);
  QUERY_CHECK_CODE(code, lino, _error);

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
      optrDummyOpenFn, pTableScanNode->paraTablesSort ? doTableMergeScanParaSubTablesNext : doTableMergeScanNext, NULL,
      destroyTableMergeScanOperatorInfo, optrDefaultBufFn, getTableMergeScanExplainExecInfo, optrDefaultGetNextExtFn,
      NULL);
  setOperatorResetStateFn(pOperator, resetTableMergeScanOperatorState);     
  pOperator->cost.openCost = 0;

  *pOptrInfo = pOperator;
  return code;

_error:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  pTaskInfo->code = code;
  if (pInfo != NULL) {
    pInfo->base.pTableListInfo = NULL;
    destroyTableMergeScanOperatorInfo(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  return code;
}

// ====================================================================================================================
// TableCountScanOperator
static void        destoryTableCountScanOperator(void* param);
static int32_t     buildVnodeGroupedStbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                                  SSDataBlock* pRes, char* dbName, tb_uid_t stbUid, SStorageAPI* pAPI);
static int32_t     buildVnodeGroupedNtbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                                  SSDataBlock* pRes, char* dbName, SStorageAPI* pAPI);
static int32_t     buildVnodeFilteredTbCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                             STableCountScanSupp* pSupp, SSDataBlock* pRes, char* dbName);
static int32_t     buildVnodeGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                               STableCountScanSupp* pSupp, SSDataBlock* pRes, int32_t vgId, char* dbName);
static int32_t     buildVnodeDbTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                          STableCountScanSupp* pSupp, SSDataBlock* pRes);
static void        buildSysDbGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                               STableCountScanSupp* pSupp, SSDataBlock* pRes, size_t infodbTableNum,
                                               size_t perfdbTableNum);
static void        buildSysDbFilterTableCount(SOperatorInfo* pOperator, STableCountScanSupp* pSupp, SSDataBlock* pRes,
                                              size_t infodbTableNum, size_t perfdbTableNum);
static const char* GROUP_TAG_DB_NAME = "db_name";
static const char* GROUP_TAG_STABLE_NAME = "stable_name";

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

int32_t createTableCountScanOperatorInfo(SReadHandle* readHandle, STableCountScanPhysiNode* pTblCountScanNode,
                                         SExecTaskInfo* pTaskInfo, SOperatorInfo** pOptrInfo) {
  QRY_PARAM_CHECK(pOptrInfo);

  int32_t                      code = TSDB_CODE_SUCCESS;
  int32_t                      lino = 0;
  SScanPhysiNode*              pScanNode = &pTblCountScanNode->scan;
  STableCountScanOperatorInfo* pInfo = taosMemoryCalloc(1, sizeof(STableCountScanOperatorInfo));
  SOperatorInfo*               pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (!pInfo || !pOperator) {
    code = terrno;
    goto _error;
  }

  pInfo->readHandle = *readHandle;

  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;
  initResultSizeInfo(&pOperator->resultInfo, 1);
  pInfo->pRes = createDataBlockFromDescNode(pDescNode);
  QUERY_CHECK_NULL(pInfo->pRes, code, lino, _error, terrno);

  code = blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);
  QUERY_CHECK_CODE(code, lino, _error);

  code = getTableCountScanSupp(pTblCountScanNode->pGroupTags, &pTblCountScanNode->scan.tableName,
                               pTblCountScanNode->scan.pScanCols, pTblCountScanNode->scan.pScanPseudoCols, &pInfo->supp,
                               pTaskInfo);
  QUERY_CHECK_CODE(code, lino, _error);

  setOperatorInfo(pOperator, "TableCountScanOperator", QUERY_NODE_PHYSICAL_PLAN_TABLE_COUNT_SCAN, false, OP_NOT_OPENED,
                  pInfo, pTaskInfo);
  pOperator->fpSet = createOperatorFpSet(optrDummyOpenFn, doTableCountScanNext, NULL, destoryTableCountScanOperator,
                                         optrDefaultBufFn, NULL, optrDefaultGetNextExtFn, NULL);
  *pOptrInfo = pOperator;
  return code;

_error:
  if (pInfo != NULL) {
    destoryTableCountScanOperator(pInfo);
  }
  if (pOperator != NULL) {
    pOperator->info = NULL;
    destroyOperator(pOperator);
  }
  pTaskInfo->code = code;
  return code;
}

int32_t fillTableCountScanDataBlock(STableCountScanSupp* pSupp, char* dbName, char* stbName, int64_t count,
                                    SSDataBlock* pRes) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  if (pSupp->dbNameSlotId != -1) {
    QUERY_CHECK_CONDITION((strlen(dbName) > 0), code, lino, _end, TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR);
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->dbNameSlotId);
    QUERY_CHECK_NULL(colInfoData, code, lino, _end, terrno);

    char varDbName[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    tstrncpy(varDataVal(varDbName), dbName, TSDB_DB_NAME_LEN);

    varDataSetLen(varDbName, strlen(dbName));
    code = colDataSetVal(colInfoData, 0, varDbName, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }

  if (pSupp->stbNameSlotId != -1) {
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->stbNameSlotId);
    QUERY_CHECK_NULL(colInfoData, code, lino, _end, terrno);
    if (strlen(stbName) != 0) {
      char varStbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
      tstrncpy(varDataVal(varStbName), stbName, TSDB_TABLE_NAME_LEN);
      varDataSetLen(varStbName, strlen(stbName));
      code = colDataSetVal(colInfoData, 0, varStbName, false);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      colDataSetNULL(colInfoData, 0);
    }
  }

  if (pSupp->tbCountSlotId != -1) {
    SColumnInfoData* colInfoData = taosArrayGet(pRes->pDataBlock, pSupp->tbCountSlotId);
    QUERY_CHECK_NULL(colInfoData, code, lino, _end, terrno);
    code = colDataSetVal(colInfoData, 0, (char*)&count, false);
    QUERY_CHECK_CODE(code, lino, _end);
  }
  pRes->info.rows = 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static SSDataBlock* buildSysDbTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo) {
  STableCountScanSupp* pSupp = &pInfo->supp;
  SSDataBlock*         pRes = pInfo->pRes;

  size_t infodbTableNum;
  getInfosDbMeta(NULL, &infodbTableNum);
  infodbTableNum -= 1;
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
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (strcmp(pSupp->dbNameFilter, TSDB_INFORMATION_SCHEMA_DB) == 0) {
    code = fillTableCountScanDataBlock(pSupp, TSDB_INFORMATION_SCHEMA_DB, "", infodbTableNum, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (strcmp(pSupp->dbNameFilter, TSDB_PERFORMANCE_SCHEMA_DB) == 0) {
    code = fillTableCountScanDataBlock(pSupp, TSDB_PERFORMANCE_SCHEMA_DB, "", perfdbTableNum, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (strlen(pSupp->dbNameFilter) == 0) {
    code = fillTableCountScanDataBlock(pSupp, "", "", infodbTableNum + perfdbTableNum, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  setOperatorCompleted(pOperator);
}

static void buildSysDbGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                        STableCountScanSupp* pSupp, SSDataBlock* pRes, size_t infodbTableNum,
                                        size_t perfdbTableNum) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  if (pInfo->currGrpIdx == 0) {
    uint64_t groupId = 0;
    if (pSupp->groupByDbName) {
      groupId = calcGroupId(TSDB_INFORMATION_SCHEMA_DB, strlen(TSDB_INFORMATION_SCHEMA_DB));
    } else {
      groupId = calcGroupId("", 0);
    }

    pRes->info.id.groupId = groupId;
    code = fillTableCountScanDataBlock(pSupp, TSDB_INFORMATION_SCHEMA_DB, "", infodbTableNum, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else if (pInfo->currGrpIdx == 1) {
    uint64_t groupId = 0;
    if (pSupp->groupByDbName) {
      groupId = calcGroupId(TSDB_PERFORMANCE_SCHEMA_DB, strlen(TSDB_PERFORMANCE_SCHEMA_DB));
    } else {
      groupId = calcGroupId("", 0);
    }

    pRes->info.id.groupId = groupId;
    code = fillTableCountScanDataBlock(pSupp, TSDB_PERFORMANCE_SCHEMA_DB, "", perfdbTableNum, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  } else {
    setOperatorCompleted(pOperator);
  }
  pInfo->currGrpIdx++;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
}

static int32_t doTableCountScanNext(SOperatorInfo* pOperator, SSDataBlock** ppRes) {
  int32_t                      code = TSDB_CODE_SUCCESS;
  SExecTaskInfo*               pTaskInfo = pOperator->pTaskInfo;
  STableCountScanOperatorInfo* pInfo = pOperator->info;
  STableCountScanSupp*         pSupp = &pInfo->supp;
  SSDataBlock*                 pRes = pInfo->pRes;

  blockDataCleanup(pRes);
  QRY_PARAM_CHECK(ppRes);

  if (pOperator->status == OP_EXEC_DONE) {
    return code;
  }

  if (pInfo->readHandle.mnd != NULL) {
    (*ppRes) = buildSysDbTableCount(pOperator, pInfo);
    return code;
  }

  code = buildVnodeDbTableCount(pOperator, pInfo, pSupp, pRes);
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed since %s", __func__, tstrerror(code));
    pTaskInfo->code = code;
    T_LONG_JMP(pTaskInfo->env, code);
  }
  if (pRes->info.rows > 0) {
    *ppRes = pRes;
  }

  return code;
}

static int32_t buildVnodeDbTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                      STableCountScanSupp* pSupp, SSDataBlock* pRes) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  const char*    db = NULL;
  int32_t        vgId = 0;
  char           dbName[TSDB_DB_NAME_LEN] = {0};
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  // get dbname
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, &db, &vgId, NULL, NULL);
  SName sn = {0};

  code = tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);
  QUERY_CHECK_CODE(code, lino, _end);

  code = tNameGetDbName(&sn, dbName);
  QUERY_CHECK_CODE(code, lino, _end);

  if (pSupp->groupByDbName || pSupp->groupByStbName) {
    code = buildVnodeGroupedTableCount(pOperator, pInfo, pSupp, pRes, vgId, dbName);
  } else {
    code = buildVnodeFilteredTbCount(pOperator, pInfo, pSupp, pRes, dbName);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }

  return code;
}

static int32_t buildVnodeGroupedTableCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                           STableCountScanSupp* pSupp, SSDataBlock* pRes, int32_t vgId, char* dbName) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  if (pSupp->groupByStbName) {
    if (pInfo->stbUidList == NULL) {
      pInfo->stbUidList = taosArrayInit(16, sizeof(tb_uid_t));
      QUERY_CHECK_NULL(pInfo->stbUidList, code, lino, _end, terrno);
      code = pAPI->metaFn.storeGetTableList(pInfo->readHandle.vnode, TSDB_SUPER_TABLE, pInfo->stbUidList);
      QUERY_CHECK_CODE(code, lino, _end);
    }

    if (pInfo->currGrpIdx < taosArrayGetSize(pInfo->stbUidList)) {
      tb_uid_t stbUid = *(tb_uid_t*)taosArrayGet(pInfo->stbUidList, pInfo->currGrpIdx);
      code = buildVnodeGroupedStbTableCount(pInfo, pSupp, pRes, dbName, stbUid, pAPI);
      QUERY_CHECK_CODE(code, lino, _end);

      pInfo->currGrpIdx++;
    } else if (pInfo->currGrpIdx == taosArrayGetSize(pInfo->stbUidList)) {
      code = buildVnodeGroupedNtbTableCount(pInfo, pSupp, pRes, dbName, pAPI);
      QUERY_CHECK_CODE(code, lino, _end);

      pInfo->currGrpIdx++;
    } else {
      setOperatorCompleted(pOperator);
    }
  } else {
    uint64_t groupId = calcGroupId(dbName, strlen(dbName));
    pRes->info.id.groupId = groupId;

    int64_t dbTableCount = 0;
    pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, &dbTableCount, NULL);
    code = fillTableCountScanDataBlock(pSupp, dbName, "", dbTableCount, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
    setOperatorCompleted(pOperator);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    pTaskInfo->code = code;
  }
  return code;
}

static int32_t buildVnodeFilteredTbCount(SOperatorInfo* pOperator, STableCountScanOperatorInfo* pInfo,
                                         STableCountScanSupp* pSupp, SSDataBlock* pRes, char* dbName) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;
  SStorageAPI*   pAPI = &pTaskInfo->storageAPI;

  if (strlen(pSupp->dbNameFilter) != 0) {
    if (strlen(pSupp->stbNameFilter) != 0) {
      uint64_t uid = 0;
      code = pAPI->metaFn.getTableUidByName(pInfo->readHandle.vnode, pSupp->stbNameFilter, &uid);
      QUERY_CHECK_CODE(code, lino, _end);

      int64_t numOfChildTables = 0;
      code = pAPI->metaFn.getNumOfChildTables(pInfo->readHandle.vnode, uid, &numOfChildTables, NULL, NULL);
      QUERY_CHECK_CODE(code, lino, _end);

      code = fillTableCountScanDataBlock(pSupp, dbName, pSupp->stbNameFilter, numOfChildTables, pRes);
      QUERY_CHECK_CODE(code, lino, _end);
    } else {
      int64_t tbNumVnode = 0;
      pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, &tbNumVnode, NULL);
      code = fillTableCountScanDataBlock(pSupp, dbName, "", tbNumVnode, pRes);
      QUERY_CHECK_CODE(code, lino, _end);
    }
  } else {
    int64_t tbNumVnode = 0;
    pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, &tbNumVnode, NULL);
    code = fillTableCountScanDataBlock(pSupp, dbName, "", tbNumVnode, pRes);
    QUERY_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    pTaskInfo->code = code;
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  setOperatorCompleted(pOperator);
  return code;
}

static int32_t buildVnodeGroupedNtbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                              SSDataBlock* pRes, char* dbName, SStorageAPI* pAPI) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char    fullStbName[TSDB_TABLE_FNAME_LEN] = {0};
  if (pSupp->groupByDbName) {
    snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbName, "");
  }

  uint64_t groupId = calcGroupId(fullStbName, strlen(fullStbName));
  pRes->info.id.groupId = groupId;

  int64_t numOfTables = 0;
  pAPI->metaFn.getBasicInfo(pInfo->readHandle.vnode, NULL, NULL, NULL, &numOfTables);

  if (numOfTables != 0) {
    code = fillTableCountScanDataBlock(pSupp, dbName, "", numOfTables, pRes);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildVnodeGroupedStbTableCount(STableCountScanOperatorInfo* pInfo, STableCountScanSupp* pSupp,
                                              SSDataBlock* pRes, char* dbName, tb_uid_t stbUid, SStorageAPI* pAPI) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  char    stbName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  code = pAPI->metaFn.getTableNameByUid(pInfo->readHandle.vnode, stbUid, stbName);
  QUERY_CHECK_CODE(code, lino, _end);

  char fullStbName[TSDB_TABLE_FNAME_LEN] = {0};
  if (pSupp->groupByDbName) {
    (void)snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s.%s", dbName, varDataVal(stbName));
  } else {
    (void)snprintf(fullStbName, TSDB_TABLE_FNAME_LEN, "%s", varDataVal(stbName));
  }

  uint64_t groupId = calcGroupId(fullStbName, strlen(fullStbName));
  pRes->info.id.groupId = groupId;

  int64_t ctbNum = 0;
  code = pAPI->metaFn.getNumOfChildTables(pInfo->readHandle.vnode, stbUid, &ctbNum, NULL, NULL);
  QUERY_CHECK_CODE(code, lino, _end);
  code = fillTableCountScanDataBlock(pSupp, dbName, varDataVal(stbName), ctbNum, pRes);
  QUERY_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void destoryTableCountScanOperator(void* param) {
  STableCountScanOperatorInfo* pTableCountScanInfo = param;
  blockDataDestroy(pTableCountScanInfo->pRes);

  taosArrayDestroy(pTableCountScanInfo->stbUidList);
  taosMemoryFreeClear(param);
}
