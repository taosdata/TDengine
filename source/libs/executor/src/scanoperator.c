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

#include <vnode.h>
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

static char* SYSTABLE_IDX_COLUMN[] = {"table_name", "db_name",     "create_time",      "columns",
                                      "ttl",        "stable_name", "vgroup_id', 'uid", "type"};

static char* SYSTABLE_SPECIAL_COL[] = {"db_name", "vgroup_id"};

typedef int32_t (*__sys_filte)(void* pMeta, SNode* cond, SArray* result);
typedef int32_t (*__sys_check)(SNode* cond);

typedef struct {
  const char* name;
  __sys_check chkFunc;
  __sys_filte fltFunc;
} SSTabFltFuncDef;

typedef struct {
  void* pMeta;
  void* pVnode;
} SSTabFltArg;

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

static int32_t sysChkFilter__Comm(SNode* pNode);
static int32_t sysChkFilter__DBName(SNode* pNode);
static int32_t sysChkFilter__VgroupId(SNode* pNode);
static int32_t sysChkFilter__TableName(SNode* pNode);
static int32_t sysChkFilter__CreateTime(SNode* pNode);
static int32_t sysChkFilter__Ncolumn(SNode* pNode);
static int32_t sysChkFilter__Ttl(SNode* pNode);
static int32_t sysChkFilter__STableName(SNode* pNode);
static int32_t sysChkFilter__Uid(SNode* pNode);
static int32_t sysChkFilter__Type(SNode* pNode);

static int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__VgroupId(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__TableName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__CreateTime(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Ncolumn(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Ttl(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__STableName(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Uid(void* arg, SNode* pNode, SArray* result);
static int32_t sysFilte__Type(void* arg, SNode* pNode, SArray* result);

const SSTabFltFuncDef filterDict[] = {
    {.name = "table_name", .chkFunc = sysChkFilter__TableName, .fltFunc = sysFilte__TableName},
    {.name = "db_name", .chkFunc = sysChkFilter__DBName, .fltFunc = sysFilte__DbName},
    {.name = "create_time", .chkFunc = sysChkFilter__CreateTime, .fltFunc = sysFilte__CreateTime},
    {.name = "columns", .chkFunc = sysChkFilter__Ncolumn, .fltFunc = sysFilte__Ncolumn},
    {.name = "ttl", .chkFunc = sysChkFilter__Ttl, .fltFunc = sysFilte__Ttl},
    {.name = "stable_name", .chkFunc = sysChkFilter__STableName, .fltFunc = sysFilte__STableName},
    {.name = "vgroup_id", .chkFunc = sysChkFilter__VgroupId, .fltFunc = sysFilte__VgroupId},
    {.name = "uid", .chkFunc = sysChkFilter__Uid, .fltFunc = sysFilte__Uid},
    {.name = "type", .chkFunc = sysChkFilter__Type, .fltFunc = sysFilte__Type}};

#define SYSTAB_FILTER_DICT_SIZE (sizeof(filterDict) / sizeof(filterDict[0]))

static int32_t optSysTabFilte(void* arg, SNode* cond, SArray* result);
static int32_t optSysTabFilteImpl(void* arg, SNode* cond, SArray* result);
static int32_t optSysCheckOper(SNode* pOpear);
static int32_t optSysMergeRslt(SArray* mRslt, SArray* rslt);

static bool processBlockWithProbability(const SSampleExecInfo* pInfo);

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock);

static void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                               SFilterInfo* pFilterInfo);

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

static bool doFilterByBlockSMA(SFilterInfo* pFilterInfo, SColumnDataAgg** pColsAgg, int32_t numOfCols,
                                            int32_t numOfRows) {
  if (pColsAgg == NULL || pFilterInfo == NULL) {
    return true;
  }

  bool keep = filterRangeExecute(pFilterInfo, pColsAgg, numOfCols, numOfRows);
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

  size_t num = taosArrayGetSize(pTableScanInfo->matchInfo.pList);
  for (int32_t i = 0; i < num; ++i) {
    SColMatchItem* pColMatchInfo = taosArrayGet(pTableScanInfo->matchInfo.pList, i);
    if (!pColMatchInfo->needOutput) {
      continue;
    }

    pBlock->pBlockAgg[pColMatchInfo->dstSlotId] = pColAgg[i];
  }

  return true;
}

static void doSetTagColumnData(STableScanInfo* pTableScanInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo,
                               int32_t rows) {
  if (pTableScanInfo->pseudoSup.numOfExprs > 0) {
    SExprSupp* pSup = &pTableScanInfo->pseudoSup;

    int32_t code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock, rows,
                                          GET_TASKID(pTaskInfo), &pTableScanInfo->metaCache);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }
}

// todo handle the slimit info
void applyLimitOffset(SLimitInfo* pLimitInfo, SSDataBlock* pBlock, SExecTaskInfo* pTaskInfo, SOperatorInfo* pOperator) {
  SLimit* pLimit = &pLimitInfo->limit;
  const char* id = GET_TASKID(pTaskInfo);

  if (pLimit->offset > 0 && pLimitInfo->remainOffset > 0) {
    if (pLimitInfo->remainOffset >= pBlock->info.rows) {
      pLimitInfo->remainOffset -= pBlock->info.rows;
      pBlock->info.rows = 0;
      qDebug("current block ignore due to offset, current:%" PRId64 ", %s", pLimitInfo->remainOffset, id);
    } else {
      blockDataTrimFirstNRows(pBlock, pLimitInfo->remainOffset);
      pLimitInfo->remainOffset = 0;
    }
  }

  if (pLimit->limit != -1 && pLimit->limit <= (pLimitInfo->numOfOutputRows + pBlock->info.rows)) {
    // limit the output rows
    int32_t overflowRows = pLimitInfo->numOfOutputRows + pBlock->info.rows - pLimit->limit;
    int32_t keep = pBlock->info.rows - overflowRows;

    blockDataKeepFirstNRows(pBlock, keep);
    qDebug("output limit %" PRId64 " has reached, %s", pLimit->limit, id);
    pOperator->status = OP_EXEC_DONE;
  }
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
    qDebug("%s data block skipped, brange:%" PRId64 "-%" PRId64 ", rows:%d", GET_TASKID(pTaskInfo),
           pBlockInfo->window.skey, pBlockInfo->window.ekey, pBlockInfo->rows);
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

  SArray* pCols = tsdbRetrieveDataBlock(pTableScanInfo->dataReader, NULL);
  if (pCols == NULL) {
    return terrno;
  }

  relocateColumnData(pBlock, pTableScanInfo->matchInfo.pList, pCols, true);
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

  applyLimitOffset(&pInfo->limitInfo, pBlock, pTaskInfo, pOperator);

  pCost->totalRows += pBlock->info.rows;
  pInfo->limitInfo.numOfOutputRows = pCost->totalRows;
  return TSDB_CODE_SUCCESS;
}

static void prepareForDescendingScan(STableScanInfo* pTableScanInfo, SqlFunctionCtx* pCtx, int32_t numOfOutput) {
  SET_REVERSE_SCAN_FLAG(pTableScanInfo);

  switchCtxOrder(pCtx, numOfOutput);
  pTableScanInfo->cond.order = TSDB_ORDER_DESC;
  STimeWindow* pTWindow = &pTableScanInfo->cond.twindows;
  TSWAP(pTWindow->skey, pTWindow->ekey);
}

typedef struct STableCachedVal {
  const char* pName;
  STag* pTags;
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

//const void *key, size_t keyLen, void *value
static void freeCachedMetaItem(const void *key, size_t keyLen, void *value) {
  freeTableCachedVal(value);
}

int32_t addTagPseudoColumnData(SReadHandle* pHandle, const SExprInfo* pExpr, int32_t numOfExpr,
                               SSDataBlock* pBlock, int32_t rows, const char* idStr, STableMetaCacheInfo* pCache) {
  // currently only the tbname pseudo column
  if (numOfExpr <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;

  // backup the rows
  int32_t backupRows = pBlock->info.rows;
  pBlock->info.rows = rows;

  bool freeReader = false;
  STableCachedVal val = {0};

  SMetaReader mr = {0};
  LRUHandle* h = NULL;

  // 1. check if it is existed in meta cache
  if (pCache == NULL) {
    metaReaderInit(&mr, pHandle->meta, 0);
    code = metaGetTableEntryByUid(&mr, pBlock->info.uid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.uid, tstrerror(terrno), idStr);
      metaReaderClear(&mr);
      return terrno;
    }

    metaReaderReleaseLock(&mr);

    val.pName = mr.me.name;
    val.pTags = (STag*)mr.me.ctbEntry.pTags;

    freeReader = true;
  } else {
    pCache->metaFetch += 1;

    h = taosLRUCacheLookup(pCache->pTableMetaEntryCache, &pBlock->info.uid, sizeof(pBlock->info.uid));
    if (h == NULL) {
      metaReaderInit(&mr, pHandle->meta, 0);
      code = metaGetTableEntryByUid(&mr, pBlock->info.uid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get table meta, uid:0x%" PRIx64 ", code:%s, %s", pBlock->info.uid, tstrerror(terrno), idStr);
        metaReaderClear(&mr);
        return terrno;
      }

      metaReaderReleaseLock(&mr);

      STableCachedVal* pVal = taosMemoryMalloc(sizeof(STableCachedVal));
      pVal->pName = strdup(mr.me.name);
      pVal->pTags = NULL;

      // only child table has tag value
      if (mr.me.type == TSDB_CHILD_TABLE) {
        STag* pTag = (STag*)mr.me.ctbEntry.pTags;
        pVal->pTags = taosMemoryMalloc(pTag->len);
        memcpy(pVal->pTags, mr.me.ctbEntry.pTags, pTag->len);
      }

      val = *pVal;
      freeReader = true;

      int32_t ret = taosLRUCacheInsert(pCache->pTableMetaEntryCache, &pBlock->info.uid, sizeof(uint64_t), pVal, sizeof(STableCachedVal), freeCachedMetaItem, NULL, TAOS_LRU_PRIORITY_LOW);
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

    qDebug("retrieve table meta from cache:%"PRIu64", hit:%"PRIu64 " miss:%"PRIu64", %s", pCache->metaFetch, pCache->cacheHit,
        (pCache->metaFetch - pCache->cacheHit), idStr);
  }

  for (int32_t j = 0; j < numOfExpr; ++j) {
    const SExprInfo* pExpr1 = &pExpr[j];
    int32_t    dstSlotId = pExpr1->base.resSchema.slotId;

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
  char buf[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
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
    SDataBlockInfo* pBInfo = &pBlock->info;

    int32_t rows = 0;
    tsdbRetrieveDataBlockInfo(pTableScanInfo->dataReader, &rows, &pBInfo->uid, &pBInfo->window);

    blockDataEnsureCapacity(pBlock, rows);  // todo remove it latter
    pBInfo->rows = rows;

    ASSERT(pBInfo->uid != 0);
    pBlock->info.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.uid);

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

static SSDataBlock* doGroupedTableScan(SOperatorInfo* pOperator) {
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
      return p;
    }

    pTableScanInfo->scanTimes += 1;

    if (pTableScanInfo->scanTimes < pTableScanInfo->scanInfo.numOfAsc) {
      setTaskStatus(pTaskInfo, TASK_NOT_COMPLETED);
      pTableScanInfo->scanFlag = REPEAT_SCAN;
      qDebug("start to repeat ascending order scan data blocks due to query func required, %s", GET_TASKID(pTaskInfo));

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

        qDebug("%s start to repeat descending order scan data blocks", GET_TASKID(pTaskInfo));
        tsdbReaderReset(pTableScanInfo->dataReader, &pTableScanInfo->cond);
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
      tsdbSetTableList(pInfo->dataReader, pTableInfo, 1);
      qDebug("set uid:%" PRIu64 " into scanner, total tables:%d, index:%d %s", pTableInfo->uid, numOfTables,
             pInfo->currentTable, pTaskInfo->id.str);

      tsdbReaderReset(pInfo->dataReader, &pInfo->cond);
      pInfo->scanTimes = 0;
    }
  } else {  // scan table group by group sequentially
    if (pInfo->currentGroupId == -1) {
      if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pTaskInfo->pTableInfoList)) {
        doSetOperatorCompleted(pOperator);
        return NULL;
      }

      int32_t        num = 0;
      STableKeyInfo* pList = NULL;
      tableListGetGroupList(pTaskInfo->pTableInfoList, pInfo->currentGroupId, &pList, &num);
      ASSERT(pInfo->dataReader == NULL);

      int32_t code = tsdbReaderOpen(pInfo->readHandle.vnode, &pInfo->cond, pList, num,
                                    (STsdbReader**)&pInfo->dataReader, GET_TASKID(pTaskInfo));
      if (code != TSDB_CODE_SUCCESS) {
        T_LONG_JMP(pTaskInfo->env, code);
      }
    }

    SSDataBlock* result = doGroupedTableScan(pOperator);
    if (result != NULL) {
      ASSERT(result->info.uid != 0);
      return result;
    }

    if ((++pInfo->currentGroupId) >= tableListGetOutputGroups(pTaskInfo->pTableInfoList)) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }

    // reset value for the next group data output
    pOperator->status = OP_OPENED;
    pInfo->limitInfo.numOfOutputRows = 0;
    pInfo->limitInfo.remainOffset = pInfo->limitInfo.limit.offset;

    int32_t        num = 0;
    STableKeyInfo* pList = NULL;
    tableListGetGroupList(pTaskInfo->pTableInfoList, pInfo->currentGroupId, &pList, &num);

    tsdbSetTableList(pInfo->dataReader, pList, num);
    tsdbReaderReset(pInfo->dataReader, &pInfo->cond);
    pInfo->scanTimes = 0;

    result = doGroupedTableScan(pOperator);
    if (result != NULL) {
      return result;
    }

    doSetOperatorCompleted(pOperator);
    return NULL;
  }
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

  if (pTableScanInfo->matchInfo.pList != NULL) {
    taosArrayDestroy(pTableScanInfo->matchInfo.pList);
  }

  taosLRUCacheCleanup(pTableScanInfo->metaCache.pTableMetaEntryCache);
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

  SScanPhysiNode* pScanNode = &pTableScanNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  int32_t code = extractColMatchInfo(pScanNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                                     &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initLimitInfo(pScanNode->node.pLimit, pScanNode->node.pSlimit, &pInfo->limitInfo);
  code = initQueryTableDataCond(&pInfo->cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  if (pScanNode->pScanPseudoCols != NULL) {
    SExprSupp* pSup = &pInfo->pseudoSup;
    pSup->pExprInfo = createExprInfo(pScanNode->pScanPseudoCols, NULL, &pSup->numOfExprs);
    pSup->pCtx = createSqlFunctionCtx(pSup->pExprInfo, pSup->numOfExprs, &pSup->rowEntryInfoOffset);
  }

  pInfo->scanInfo = (SScanInfo){.numOfAsc = pTableScanNode->scanSeq[0], .numOfDesc = pTableScanNode->scanSeq[1]};
  pInfo->pdInfo.interval = extractIntervalInfo(pTableScanNode);
  pInfo->readHandle = *readHandle;
  pInfo->sample.sampleRatio = pTableScanNode->ratio;
  pInfo->sample.seed = taosGetTimestampSec();

  pInfo->dataBlockLoadFlag = pTableScanNode->dataRequired;

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  pInfo->pResBlock = createResDataBlock(pDescNode);
  blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);

  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->scanFlag = MAIN_SCAN;
  pInfo->currentGroupId = -1;
  pInfo->assignBlockUid = pTableScanNode->assignBlockUid;

  pOperator->name = "TableScanOperator";  // for debug purpose
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pInfo->metaCache.pTableMetaEntryCache = taosLRUCacheInit(1024*128, -1, .5);
  taosLRUCacheSetStrictCapacity(pInfo->metaCache.pTableMetaEntryCache, false);

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doTableScan, NULL, NULL, destroyTableScanOperatorInfo,
                                         getTableScannerExecInfo);

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

  pInfo->dataReader = pReadHandle;
  //  pInfo->prevGroupId       = -1;

  pOperator->name = "TableSeqScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_SEQ_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doTableScanImpl, NULL, NULL, NULL, NULL);
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
  int32_t code = doGetTableRowSize(pBlockScanInfo->readHandle.meta, pBlockScanInfo->uid, (int32_t*)&blockDistInfo.rowSize,
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

static int32_t initTableblockDistQueryCond(uint64_t uid, SQueryTableDataCond* pCond) {
  memset(pCond, 0, sizeof(SQueryTableDataCond));

  pCond->order = TSDB_ORDER_ASC;
  pCond->numOfCols = 1;
  pCond->colList = taosMemoryCalloc(1, sizeof(SColumnInfo));
  if (pCond->colList == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    return terrno;
  }

  pCond->colList->colId = 1;
  pCond->colList->type = TSDB_DATA_TYPE_TIMESTAMP;
  pCond->colList->bytes = sizeof(TSKEY);

  pCond->twindows = (STimeWindow){.skey = INT64_MIN, .ekey = INT64_MAX};
  pCond->suid = uid;
  pCond->type = TIMEWINDOW_RANGE_CONTAINED;
  pCond->startVersion = -1;
  pCond->endVersion = -1;

  return TSDB_CODE_SUCCESS;
}

SOperatorInfo* createDataBlockInfoScanOperator(SReadHandle* readHandle, SBlockDistScanPhysiNode* pBlockScanNode,
                                               SExecTaskInfo* pTaskInfo) {
  SBlockDistInfo* pInfo = taosMemoryCalloc(1, sizeof(SBlockDistInfo));
  SOperatorInfo*  pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
    goto _error;
  }

  {
    SQueryTableDataCond cond = {0};

    int32_t code = initTableblockDistQueryCond(pBlockScanNode->suid, &cond);
    if (code != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    STableListInfo* pTableListInfo = pTaskInfo->pTableInfoList;
    size_t          num = tableListGetSize(pTableListInfo);
    void*           pList = tableListGetInfo(pTableListInfo, 0);

    code = tsdbReaderOpen(readHandle->vnode, &cond, pList, num, &pInfo->pHandle, pTaskInfo->id.str);
    cleanupQueryTableDataCond(&cond);
    if (code != 0) {
      goto _error;
    }
  }

  pInfo->readHandle = *readHandle;
  pInfo->uid = pBlockScanNode->suid;

  pInfo->pResBlock = createResDataBlock(pBlockScanNode->node.pOutputDataBlockDesc);
  blockDataEnsureCapacity(pInfo->pResBlock, 1);

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

  pOperator->fpSet =
      createOperatorFpSet(operatorDummyOpenFn, doBlockInfoScan, NULL, NULL, destroyBlockDistScanOperatorInfo, NULL);
  return pOperator;

_error:
  taosMemoryFreeClear(pInfo);
  taosMemoryFreeClear(pOperator);
  return NULL;
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
  pTableScanInfo->cond.twindows = *pWin;
  pTableScanInfo->scanTimes = 0;
  pTableScanInfo->currentGroupId = -1;
  tsdbReaderClose(pTableScanInfo->dataReader);
  pTableScanInfo->dataReader = NULL;
}

static SSDataBlock* readPreVersionData(SOperatorInfo* pTableScanOp, uint64_t tbUid, TSKEY startTs, TSKEY endTs,
                                       int64_t maxVersion) {
  STableKeyInfo tblInfo = {.uid = tbUid, .groupId = 0};

  STableScanInfo*     pTableScanInfo = pTableScanOp->info;
  SQueryTableDataCond cond = pTableScanInfo->cond;

  cond.startVersion = -1;
  cond.endVersion = maxVersion;
  cond.twindows = (STimeWindow){.skey = startTs, .ekey = endTs};

  SExecTaskInfo* pTaskInfo = pTableScanOp->pTaskInfo;

  SSDataBlock* pBlock = pTableScanInfo->pResBlock;
  blockDataCleanup(pBlock);

  STsdbReader* pReader = NULL;
  int32_t      code = tsdbReaderOpen(pTableScanInfo->readHandle.vnode, &cond, &tblInfo, 1, (STsdbReader**)&pReader,
                                     GET_TASKID(pTaskInfo));
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    T_LONG_JMP(pTaskInfo->env, code);
    return NULL;
  }

  bool hasBlock = tsdbNextDataBlock(pReader);
  if (hasBlock) {
    SDataBlockInfo* pBInfo = &pBlock->info;

    int32_t rows = 0;
    tsdbRetrieveDataBlockInfo(pReader, &rows, &pBInfo->uid, &pBInfo->window);

    SArray* pCols = tsdbRetrieveDataBlock(pReader, NULL);
    blockDataEnsureCapacity(pBlock, rows);
    pBlock->info.rows = rows;

    relocateColumnData(pBlock, pTableScanInfo->matchInfo.pList, pCols, true);
    doSetTagColumnData(pTableScanInfo, pBlock, pTaskInfo, rows);

    pBlock->info.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBInfo->uid);
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
      while ((groupId == gpIdCol[(*pRowIndex)] && startTsCol[*pRowIndex] < endWin.ekey)) {
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
      tsdbReaderClose(pTableScanInfo->dataReader);
      pTableScanInfo->dataReader = NULL;
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
  int64_t          version = pSrcBlock->info.version - 1;
  for (int32_t i = 0; i < pSrcBlock->info.rows; i++) {
    uint64_t groupId = getGroupIdByData(pInfo, uidCol[i], startData[i], version);
    // gap must be 0.
    SSessionKey startWin = {0};
    getCurSessionWindow(pInfo->windowSup.pStreamAggSup, startData[i], endData[i], groupId, &startWin);
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
    if (groupId == 0) {
      groupId = getGroupIdByData(pInfo, srcUid, srcStartTsCol[i], version);
    }
    appendOneRowToStreamSpecialBlock(pDestBlock, srcStartTsCol + i, srcEndTsCol + i, srcUidData + i, &groupId, NULL);
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
  blockDataUpdateTsWindow(pDestBlock, 0);
  return code;
}

static void calBlockTag(SExprSupp* pTagCalSup, SSDataBlock* pBlock, SSDataBlock* pResBlock) {
  if (pTagCalSup == NULL || pTagCalSup->numOfExprs == 0) return;
  if (pBlock == NULL || pBlock->info.rows == 0) return;

  SSDataBlock* pSrcBlock = blockCopyOneRow(pBlock, 0);
  ASSERT(pSrcBlock->info.rows == 1);

  blockDataEnsureCapacity(pResBlock, 1);

  projectApplyFunctions(pTagCalSup->pExprInfo, pResBlock, pSrcBlock, pTagCalSup->pCtx, 1, NULL);
  ASSERT(pResBlock->info.rows == 1);

  // build tagArray
  /*SArray* tagArray = taosArrayInit(0, sizeof(void*));*/
  /*STagVal tagVal = {*/
  /*.cid = 0,*/
  /*.type = 0,*/
  /*};*/
  // build STag
  // set STag

  blockDataDestroy(pSrcBlock);
}

static void calBlockTbName(SExprSupp* pTbNameCalSup, SSDataBlock* pBlock) {
  if (pTbNameCalSup == NULL || pTbNameCalSup->numOfExprs == 0) return;
  if (pBlock == NULL || pBlock->info.rows == 0) return;

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
    memcpy(pBlock->info.parTbName, varDataVal(pData), TMIN(varDataLen(pData), TSDB_TABLE_NAME_LEN));
    pBlock->info.parTbName[TSDB_TABLE_NAME_LEN - 1] = 0;
  } else {
    pBlock->info.parTbName[0] = 0;
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
                     isDeletedStreamWindow(&win, pBlock->info.groupId,
                                           pInfo->pTableScanOp->pTaskInfo->streamInfo.pState, &pInfo->twAggSup);
    if ((update || closedWin) && out) {
      qDebug("stream update check not pass, update %d, closedWin %d", update, closedWin);
      uint64_t gpId = 0;
      appendOneRowToStreamSpecialBlock(pInfo->pUpdateDataRes, tsCol + rowId, tsCol + rowId, &pBlock->info.uid, &gpId,
                                       NULL);
      if (closedWin && pInfo->partitionSup.needCalc) {
        gpId = calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pBlock, rowId);
        appendOneRowToStreamSpecialBlock(pInfo->pUpdateDataRes, tsCol + rowId, tsCol + rowId, &pBlock->info.uid, &gpId,
                                         NULL);
      }
    }
  }
  if (out && pInfo->pUpdateDataRes->info.rows > 0) {
    pInfo->pUpdateDataRes->info.version = pBlock->info.version;
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
  pInfo->pRes->info.uid = pBlock->info.uid;
  pInfo->pRes->info.type = STREAM_NORMAL;
  pInfo->pRes->info.version = pBlock->info.version;

  pInfo->pRes->info.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.uid);

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
    if (code != TSDB_CODE_SUCCESS) {
      blockDataFreeRes((SSDataBlock*)pBlock);
      T_LONG_JMP(pTaskInfo->env, code);
    }
  }

  if (filter) {
    doFilter(pInfo->pRes, pOperator->exprSupp.pFilterInfo, NULL);
  }

  blockDataUpdateTsWindow(pInfo->pRes, pInfo->primaryTsIndex);
  blockDataFreeRes((SSDataBlock*)pBlock);

  calBlockTbName(&pInfo->tbnameCalSup, pInfo->pRes);
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
        tsdbReaderClose(pTSInfo->dataReader);
        pTSInfo->dataReader = NULL;
        tqOffsetResetToLog(&pTaskInfo->streamInfo.prepareStatus, pTaskInfo->streamInfo.snapshotVer);
        qDebug("queue scan tsdb over, switch to wal ver %" PRId64 "", pTaskInfo->streamInfo.snapshotVer + 1);
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
  } else {
    // SSDataBlock* pPreRes = readPreVersionData(pInfo->pTableScanOp, uidCol[i], startTsCol, ts, maxVersion);
    // if (!pPreRes || pPreRes->info.rows == 0) {
    //   return 0;
    // }
    // ASSERT(pPreRes->info.rows == 1);
    // return calGroupIdByData(&pInfo->partitionSup, pInfo->pPartScalarSup, pPreRes, 0);
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

  if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE1 ||
      pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE2) {
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    memcpy(&pTSInfo->cond, &pTaskInfo->streamInfo.tableCond, sizeof(SQueryTableDataCond));
    if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__PREPARE1) {
      pTSInfo->cond.startVersion = 0;
      pTSInfo->cond.endVersion = pTaskInfo->streamInfo.fillHistoryVer1;
      qDebug("stream recover step 1, from %" PRId64 " to %" PRId64, pTSInfo->cond.startVersion,
             pTSInfo->cond.endVersion);
    } else {
      pTSInfo->cond.startVersion = pTaskInfo->streamInfo.fillHistoryVer1 + 1;
      pTSInfo->cond.endVersion = pTaskInfo->streamInfo.fillHistoryVer2;
      qDebug("stream recover step 2, from %" PRId64 " to %" PRId64, pTSInfo->cond.startVersion,
             pTSInfo->cond.endVersion);
    }

    /*resetTableScanInfo(pTSInfo, pWin);*/
    tsdbReaderClose(pTSInfo->dataReader);
    pTSInfo->dataReader = NULL;

    pTSInfo->scanTimes = 0;
    pTSInfo->currentGroupId = -1;
    pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__SCAN;
  }

  if (pTaskInfo->streamInfo.recoverStep == STREAM_RECOVER_STEP__SCAN) {
    SSDataBlock* pBlock = doTableScan(pInfo->pTableScanOp);
    if (pBlock != NULL) {
      calBlockTbName(&pInfo->tbnameCalSup, pBlock);
      updateInfoFillBlockData(pInfo->pUpdateInfo, pBlock, pInfo->primaryTsIndex);
      qDebug("stream recover scan get block, rows %d", pBlock->info.rows);
      return pBlock;
    }
    pTaskInfo->streamInfo.recoverStep = STREAM_RECOVER_STEP__NONE;
    STableScanInfo* pTSInfo = pInfo->pTableScanOp->info;
    tsdbReaderClose(pTSInfo->dataReader);
    pTSInfo->dataReader = NULL;

    pTSInfo->cond.startVersion = -1;
    pTSInfo->cond.endVersion = -1;

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
        SSDataBlock* pDelBlock = NULL;
        if (pInfo->tqReader) {
          pDelBlock = createSpecialDataBlock(STREAM_DELETE_DATA);
          filterDelBlockByUid(pDelBlock, pBlock, pInfo);
        } else {
          pDelBlock = pBlock;
        }
        setBlockGroupIdByUid(pInfo, pDelBlock);
        printDataBlock(pDelBlock, "stream scan delete recv filtered");
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
          pInfo->scanMode = STREAM_SCAN_FROM_DATAREADER_RANGE;
          printDataBlock(pDelBlock, "stream scan delete data");
          if (pInfo->tqReader) {
            blockDataDestroy(pDelBlock);
          }
          if (pInfo->pDeleteDataRes->info.rows > 0) {
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
          uint64_t        version = getReaderMaxVersion(pTableScanInfo->dataReader);
          updateInfoSetScanRange(pInfo->pUpdateInfo, &pTableScanInfo->cond.twindows, pInfo->groupId, version);
          pSDB->info.type = pInfo->scanMode == STREAM_SCAN_FROM_DATAREADER_RANGE ? STREAM_NORMAL : STREAM_PULL_DATA;
          checkUpdateData(pInfo, true, pSDB, false);
          // printDataBlock(pSDB, "stream scan update");
          calBlockTbName(&pInfo->tbnameCalSup, pSDB);
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

        if (updateInfoIgnore(pInfo->pUpdateInfo, &pInfo->pRes->info.window, pInfo->pRes->info.groupId,
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
    SSDataBlock* pBlock = &pInfo->pRes;

    if (pInfo->dataReader && tsdbNextDataBlock(pInfo->dataReader)) {
      if (isTaskKilled(pTaskInfo)) {
        longjmp(pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
      }

      int32_t rows = 0;
      tsdbRetrieveDataBlockInfo(pInfo->dataReader, &rows, &pBlock->info.uid, &pBlock->info.window);
      pBlock->info.rows = rows;

      SArray* pCols = tsdbRetrieveDataBlock(pInfo->dataReader, NULL);
      pBlock->pDataBlock = pCols;
      if (pCols == NULL) {
        longjmp(pTaskInfo->env, terrno);
      }

      qDebug("tmqsnap doRawScan get data uid:%" PRId64 "", pBlock->info.uid);
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
  pOperator->name = "RawScanOperator";
  pOperator->info = pInfo;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(NULL, doRawScan, NULL, NULL, destroyRawScanOperatorInfo, NULL);
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
    STableScanInfo* pTableScanInfo = pStreamScan->pTableScanOp->info;
    destroyTableScanOperatorInfo(pTableScanInfo);
    taosMemoryFreeClear(pStreamScan->pTableScanOp);
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

  int32_t numOfCols = 0;
  int32_t code =
      extractColMatchInfo(pScanPhyNode->pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t numOfOutput = taosArrayGetSize(pInfo->matchInfo.pList);
  SArray* pColIds = taosArrayInit(numOfOutput, sizeof(int16_t));
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
      pTSInfo->cond.endVersion = pHandle->version;
    }

    STableKeyInfo* pList = NULL;
    int32_t        num = 0;
    tableListGetGroupList(pTaskInfo->pTableInfoList, 0, &pList, &num);

    if (pHandle->initTableReader) {
      pTSInfo->scanMode = TABLE_SCAN__TABLE_ORDER;
      pTSInfo->dataReader = NULL;
      code = tsdbReaderOpen(pHandle->vnode, &pTSInfo->cond, pList, num, &pTSInfo->dataReader, NULL);
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
    memcpy(&pTaskInfo->streamInfo.tableCond, &pTSInfo->cond, sizeof(SQueryTableDataCond));
  } else {
    taosArrayDestroy(pColIds);
  }

  // create the pseduo columns info
  if (pTableScanNode->scan.pScanPseudoCols != NULL) {
    pInfo->pPseudoExpr = createExprInfo(pTableScanNode->scan.pScanPseudoCols, NULL, &pInfo->numOfPseudoExpr);
  }

  code = filterInitFromNode((SNode*)pScanPhyNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pRes = createResDataBlock(pDescNode);
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

  pOperator->name = "StreamScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_STREAM_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);
  pOperator->pTaskInfo = pTaskInfo;

  __optr_fn_t nextFn = pTaskInfo->execModel == OPTR_EXEC_MODEL_STREAM ? doStreamScan : doQueueScan;
  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, nextFn, NULL, NULL, destroyStreamScanOperatorInfo, NULL);

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
  if (pInfo->pIdx) {
    taosArrayDestroy(pInfo->pIdx->uids);
    taosMemoryFree(pInfo->pIdx);
    pInfo->pIdx = NULL;
  }

  taosArrayDestroy(pInfo->matchInfo.pList);
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

static SSDataBlock* doFilterResult(SSDataBlock* pDataBlock, SFilterInfo* pFilterInfo) {
  if (pFilterInfo == NULL) {
    return pDataBlock->info.rows == 0 ? NULL : pDataBlock;
  }

  doFilter(pDataBlock, pFilterInfo, NULL);
  return pDataBlock->info.rows == 0 ? NULL : pDataBlock;
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

static bool sysTableIsOperatorCondOnOneTable(SNode* pCond, char* condTable) {
  SOperatorNode* node = (SOperatorNode*)pCond;
  if (node->opType == OP_TYPE_EQUAL) {
    if (nodeType(node->pLeft) == QUERY_NODE_COLUMN &&
        strcasecmp(nodesGetNameFromColumnNode(node->pLeft), "table_name") == 0 &&
        nodeType(node->pRight) == QUERY_NODE_VALUE) {
      SValueNode* pValue = (SValueNode*)node->pRight;
      if (pValue->node.resType.type == TSDB_DATA_TYPE_NCHAR || pValue->node.resType.type == TSDB_DATA_TYPE_VARCHAR ||
          pValue->node.resType.type == TSDB_DATA_TYPE_BINARY) {
        char* value = nodesGetValueFromNode(pValue);
        strncpy(condTable, varDataVal(value), TSDB_TABLE_NAME_LEN);
        return true;
      }
    }
  }
  return false;
}

static bool sysTableIsCondOnOneTable(SNode* pCond, char* condTable) {
  if (pCond == NULL) {
    return false;
  }
  if (nodeType(pCond) == QUERY_NODE_LOGIC_CONDITION) {
    SLogicConditionNode* node = (SLogicConditionNode*)pCond;
    if (LOGIC_COND_TYPE_AND == node->condType) {
      SNode* pChild = NULL;
      FOREACH(pChild, node->pParameterList) {
        if (QUERY_NODE_OPERATOR == nodeType(pChild) && sysTableIsOperatorCondOnOneTable(pChild, condTable)) {
          return true;
        }
      }
    }
  }

  if (QUERY_NODE_OPERATOR == nodeType(pCond)) {
    return sysTableIsOperatorCondOnOneTable(pCond, condTable);
  }

  return false;
}

static SSDataBlock* sysTableScanUserTags(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  SSDataBlock* dataBlock = buildInfoSchemaTableMetaBlock(TSDB_INS_TABLE_TAGS);
  blockDataEnsureCapacity(dataBlock, pOperator->resultInfo.capacity);

  const char* db = NULL;
  int32_t     vgId = 0;
  vnodeGetInfo(pInfo->readHandle.vnode, &db, &vgId);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  char condTableName[TSDB_TABLE_NAME_LEN] = {0};
  // optimize when sql like where table_name='tablename' and xxx.
  if (pInfo->pCondition && sysTableIsCondOnOneTable(pInfo->pCondition, condTableName)) {
    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, condTableName);

    SMetaReader smrChildTable = {0};
    metaReaderInit(&smrChildTable, pInfo->readHandle.meta, 0);
    int32_t code = metaGetTableEntryByName(&smrChildTable, condTableName);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by metaGetTableEntryByName, therefore, return directly
      return NULL;
    }

    if (smrChildTable.me.type != TSDB_CHILD_TABLE) {
      metaReaderClear(&smrChildTable);
      blockDataDestroy(dataBlock);
      pInfo->loadInfo.totalRows = 0;
      return NULL;
    }

    SMetaReader smrSuperTable = {0};
    metaReaderInit(&smrSuperTable, pInfo->readHandle.meta, META_READER_NOLOCK);
    code = metaGetTableEntryByUid(&smrSuperTable, smrChildTable.me.ctbEntry.suid);
    if (code != TSDB_CODE_SUCCESS) {
      // terrno has been set by metaGetTableEntryByUid
      return NULL;
    }

    sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &smrChildTable, dbname, tableName, &numOfRows, dataBlock);
    metaReaderClear(&smrSuperTable);
    metaReaderClear(&smrChildTable);
    if (numOfRows > 0) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
      numOfRows = 0;
    }
    blockDataDestroy(dataBlock);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
    doSetOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  }

  int32_t ret = 0;
  if (pInfo->pCur == NULL) {
    pInfo->pCur = metaOpenTbCursor(pInfo->readHandle.meta);
  }

  while ((ret = metaTbCursorNext(pInfo->pCur)) == 0) {
    if (pInfo->pCur->mr.me.type != TSDB_CHILD_TABLE) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tableName, pInfo->pCur->mr.me.name);

    SMetaReader smrSuperTable = {0};
    metaReaderInit(&smrSuperTable, pInfo->readHandle.meta, 0);
    uint64_t suid = pInfo->pCur->mr.me.ctbEntry.suid;
    int32_t  code = metaGetTableEntryByUid(&smrSuperTable, suid);
    if (code != TSDB_CODE_SUCCESS) {
      qError("failed to get super table meta, uid:0x%" PRIx64 ", code:%s, %s", suid, tstrerror(terrno),
             GET_TASKID(pTaskInfo));
      metaReaderClear(&smrSuperTable);
      metaCloseTbCursor(pInfo->pCur);
      pInfo->pCur = NULL;
      T_LONG_JMP(pTaskInfo->env, terrno);
    }

    sysTableUserTagsFillOneTableTags(pInfo, &smrSuperTable, &pInfo->pCur->mr, dbname, tableName, &numOfRows, dataBlock);

    metaReaderClear(&smrSuperTable);

    if (numOfRows >= pOperator->resultInfo.capacity) {
      relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
      numOfRows = 0;

      if (pInfo->pRes->info.rows > 0) {
        break;
      }
    }
  }

  if (numOfRows > 0) {
    relocateAndFilterSysTagsScanResult(pInfo, numOfRows, dataBlock, pOperator->exprSupp.pFilterInfo);
    numOfRows = 0;
  }

  blockDataDestroy(dataBlock);
  if (ret != 0) {
    metaCloseTbCursor(pInfo->pCur);
    pInfo->pCur = NULL;
    doSetOperatorCompleted(pOperator);
  }

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

void relocateAndFilterSysTagsScanResult(SSysTableScanInfo* pInfo, int32_t numOfRows, SSDataBlock* dataBlock,
                                        SFilterInfo* pFilterInfo) {
  dataBlock->info.rows = numOfRows;
  pInfo->pRes->info.rows = numOfRows;

  relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, dataBlock->pDataBlock, false);
  doFilterResult(pInfo->pRes, pFilterInfo);
  blockDataCleanup(dataBlock);
}

static int32_t sysTableUserTagsFillOneTableTags(const SSysTableScanInfo* pInfo, SMetaReader* smrSuperTable,
                                                SMetaReader* smrChildTable, const char* dbname, const char* tableName,
                                                int32_t* pNumOfRows, const SSDataBlock* dataBlock) {
  char stableName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(stableName, (*smrSuperTable).me.name);

  int32_t numOfRows = *pNumOfRows;

  int32_t numOfTags = (*smrSuperTable).me.stbEntry.schemaTag.nCols;
  for (int32_t i = 0; i < numOfTags; ++i) {
    SColumnInfoData* pColInfoData = NULL;

    // table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 0);
    colDataAppend(pColInfoData, numOfRows, tableName, false);

    // database name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 1);
    colDataAppend(pColInfoData, numOfRows, dbname, false);

    // super table name
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 2);
    colDataAppend(pColInfoData, numOfRows, stableName, false);

    // tag name
    char tagName[TSDB_COL_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(tagName, (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].name);
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 3);
    colDataAppend(pColInfoData, numOfRows, tagName, false);

    // tag type
    int8_t tagType = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].type;
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 4);
    char tagTypeStr[VARSTR_HEADER_SIZE + 32];
    int  tagTypeLen = sprintf(varDataVal(tagTypeStr), "%s", tDataTypes[tagType].name);
    if (tagType == TSDB_DATA_TYPE_VARCHAR) {
      tagTypeLen += sprintf(varDataVal(tagTypeStr) + tagTypeLen, "(%d)",
                            (int32_t)((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE));
    } else if (tagType == TSDB_DATA_TYPE_NCHAR) {
      tagTypeLen += sprintf(
          varDataVal(tagTypeStr) + tagTypeLen, "(%d)",
          (int32_t)(((*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].bytes - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE));
    }
    varDataSetLen(tagTypeStr, tagTypeLen);
    colDataAppend(pColInfoData, numOfRows, (char*)tagTypeStr, false);

    STagVal tagVal = {0};
    tagVal.cid = (*smrSuperTable).me.stbEntry.schemaTag.pSchema[i].colId;
    char*    tagData = NULL;
    uint32_t tagLen = 0;

    if (tagType == TSDB_DATA_TYPE_JSON) {
      tagData = (char*)smrChildTable->me.ctbEntry.pTags;
    } else {
      bool exist = tTagGet((STag*)smrChildTable->me.ctbEntry.pTags, &tagVal);
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
    pColInfoData = taosArrayGet(dataBlock->pDataBlock, 5);
    colDataAppend(pColInfoData, numOfRows, tagVarChar,
                  (tagData == NULL) || (tagType == TSDB_DATA_TYPE_JSON && tTagIsJsonNull(tagData)));
    taosMemoryFree(tagVarChar);
    ++numOfRows;
  }

  *pNumOfRows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

typedef int (*__optSysFilter)(void* a, void* b, int16_t dtype);

int optSysDoCompare(__compar_fn_t func, int8_t comparType, void* a, void* b) {
  int32_t cmp = func(a, b);
  switch (comparType) {
    case OP_TYPE_LOWER_THAN:
      if (cmp < 0) return 0;
      break;
    case OP_TYPE_LOWER_EQUAL: {
      if (cmp <= 0) return 0;
      break;
    }
    case OP_TYPE_GREATER_THAN: {
      if (cmp > 0) return 0;
      break;
    }
    case OP_TYPE_GREATER_EQUAL: {
      if (cmp >= 0) return 0;
      break;
    }
    case OP_TYPE_EQUAL: {
      if (cmp == 0) return 0;
      break;
    }
    default:
      return -1;
  }
  return cmp;
}

static int optSysFilterFuncImpl__LowerThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_LOWER_THAN, a, b);
}
static int optSysFilterFuncImpl__LowerEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_LOWER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__GreaterThan(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_GREATER_THAN, a, b);
}
static int optSysFilterFuncImpl__GreaterEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_GREATER_EQUAL, a, b);
}
static int optSysFilterFuncImpl__Equal(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_EQUAL, a, b);
}

static int optSysFilterFuncImpl__NoEqual(void* a, void* b, int16_t dtype) {
  __compar_fn_t func = getComparFunc(dtype, 0);
  return optSysDoCompare(func, OP_TYPE_NOT_EQUAL, a, b);
}
static __optSysFilter optSysGetFilterFunc(int32_t ctype, bool* reverse) {
  if (ctype == OP_TYPE_LOWER_EQUAL || ctype == OP_TYPE_LOWER_THAN) {
    *reverse = true;
  }
  if (ctype == OP_TYPE_LOWER_THAN)
    return optSysFilterFuncImpl__LowerThan;
  else if (ctype == OP_TYPE_LOWER_EQUAL)
    return optSysFilterFuncImpl__LowerEqual;
  else if (ctype == OP_TYPE_GREATER_THAN)
    return optSysFilterFuncImpl__GreaterThan;
  else if (ctype == OP_TYPE_GREATER_EQUAL)
    return optSysFilterFuncImpl__GreaterEqual;
  else if (ctype == OP_TYPE_EQUAL)
    return optSysFilterFuncImpl__Equal;
  else if (ctype == OP_TYPE_NOT_EQUAL)
    return optSysFilterFuncImpl__NoEqual;
  return NULL;
}
static int32_t sysFilte__DbName(void* arg, SNode* pNode, SArray* result) {
  void* pVnode = ((SSTabFltArg*)arg)->pVnode;

  const char* db = NULL;
  vnodeGetInfo(pVnode, &db, NULL);

  SName sn = {0};
  char  dbname[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
  tNameFromString(&sn, db, T_NAME_ACCT | T_NAME_DB);

  tNameGetDbName(&sn, varDataVal(dbname));
  varDataSetLen(dbname, strlen(varDataVal(dbname)));

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool           reverse = false;
  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;

  int ret = func(dbname, pVal->datum.p, TSDB_DATA_TYPE_VARCHAR);
  if (ret == 0) return 0;

  return -2;
}
static int32_t sysFilte__VgroupId(void* arg, SNode* pNode, SArray* result) {
  void* pVnode = ((SSTabFltArg*)arg)->pVnode;

  int64_t vgId = 0;
  vnodeGetInfo(pVnode, NULL, (int32_t*)&vgId);

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  bool reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;

  int ret = func(&vgId, &pVal->datum.i, TSDB_DATA_TYPE_BIGINT);
  if (ret == 0) return 0;

  return -1;
}
static int32_t sysFilte__TableName(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;

  SMetaFltParam param = {.suid = 0,
                         .cid = 0,
                         .type = TSDB_DATA_TYPE_VARCHAR,
                         .val = pVal->datum.p,
                         .reverse = reverse,
                         .filterFunc = func};
  return -1;
}

static int32_t sysFilte__CreateTime(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;

  SMetaFltParam param = {.suid = 0,
                         .cid = 0,
                         .type = TSDB_DATA_TYPE_BIGINT,
                         .val = &pVal->datum.i,
                         .reverse = reverse,
                         .filterFunc = func};

  int32_t ret = metaFilterCreateTime(pMeta, &param, result);
  return ret;
}
static int32_t sysFilte__Ncolumn(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;
  return -1;
}

static int32_t sysFilte__Ttl(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;
  return -1;
}
static int32_t sysFilte__STableName(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;
  return -1;
}
static int32_t sysFilte__Uid(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;
  return -1;
}
static int32_t sysFilte__Type(void* arg, SNode* pNode, SArray* result) {
  void* pMeta = ((SSTabFltArg*)arg)->pMeta;

  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  bool           reverse = false;

  __optSysFilter func = optSysGetFilterFunc(pOper->opType, &reverse);
  if (func == NULL) return -1;
  return -1;
}
static int32_t sysChkFilter__Comm(SNode* pNode) {
  // impl
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  EOperatorType  opType = pOper->opType;
  if (opType != OP_TYPE_EQUAL && opType != OP_TYPE_LOWER_EQUAL && opType != OP_TYPE_LOWER_THAN &&
      opType != OP_TYPE_GREATER_EQUAL && opType != OP_TYPE_GREATER_THAN) {
    return -1;
  }
  return 0;
}

static int32_t sysChkFilter__DBName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;

  if (pOper->opType != OP_TYPE_EQUAL && pOper->opType != OP_TYPE_NOT_EQUAL) {
    return -1;
  }

  SValueNode* pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }

  return 0;
}
static int32_t sysChkFilter__VgroupId(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__TableName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__CreateTime(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_TIMESTAMP_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}

static int32_t sysChkFilter__Ncolumn(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Ttl(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;

  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__STableName(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_STR_DATA_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Uid(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t sysChkFilter__Type(SNode* pNode) {
  SOperatorNode* pOper = (SOperatorNode*)pNode;
  SValueNode*    pVal = (SValueNode*)pOper->pRight;
  if (!IS_INTEGER_TYPE(pVal->node.resType.type)) {
    return -1;
  }
  return sysChkFilter__Comm(pNode);
}
static int32_t optSysTabFilteImpl(void* arg, SNode* cond, SArray* result) {
  if (optSysCheckOper(cond) != 0) return -1;

  SOperatorNode* pNode = (SOperatorNode*)cond;

  int8_t i = 0;
  for (; i < SYSTAB_FILTER_DICT_SIZE; i++) {
    if (strcmp(filterDict[i].name, ((SColumnNode*)(pNode->pLeft))->colName) == 0) {
      break;
    }
  }
  if (i >= SYSTAB_FILTER_DICT_SIZE) return -1;

  if (filterDict[i].chkFunc(cond) != 0) return -1;

  return filterDict[i].fltFunc(arg, cond, result);
}

static int32_t optSysCheckOper(SNode* pOpear) {
  if (nodeType(pOpear) != QUERY_NODE_OPERATOR) return -1;

  SOperatorNode* pOper = (SOperatorNode*)pOpear;
  if (pOper->opType < OP_TYPE_GREATER_THAN || pOper->opType > OP_TYPE_NOT_EQUAL) {
    return -1;
  }

  if (nodeType(pOper->pLeft) != QUERY_NODE_COLUMN || nodeType(pOper->pRight) != QUERY_NODE_VALUE) {
    return -1;
  }
  return 0;
}

static int tableUidCompare(const void* a, const void* b) {
  int64_t u1 = *(int64_t*)a;
  int64_t u2 = *(int64_t*)b;
  if (u1 == u2) {
    return 0;
  }
  return u1 < u2 ? -1 : 1;
}

typedef struct MergeIndex {
  int idx;
  int len;
} MergeIndex;

static FORCE_INLINE int optSysBinarySearch(SArray* arr, int s, int e, uint64_t k) {
  uint64_t v;
  int32_t  m;
  while (s <= e) {
    m = s + (e - s) / 2;
    v = *(uint64_t*)taosArrayGet(arr, m);
    if (v >= k) {
      e = m - 1;
    } else {
      s = m + 1;
    }
  }
  return s;
}

void optSysIntersection(SArray* in, SArray* out) {
  int32_t sz = (int32_t)taosArrayGetSize(in);
  if (sz <= 0) {
    return;
  }
  MergeIndex* mi = taosMemoryCalloc(sz, sizeof(MergeIndex));
  for (int i = 0; i < sz; i++) {
    SArray* t = taosArrayGetP(in, i);
    mi[i].len = (int32_t)taosArrayGetSize(t);
    mi[i].idx = 0;
  }

  SArray* base = taosArrayGetP(in, 0);
  for (int i = 0; i < taosArrayGetSize(base); i++) {
    uint64_t tgt = *(uint64_t*)taosArrayGet(base, i);
    bool     has = true;
    for (int j = 1; j < taosArrayGetSize(in); j++) {
      SArray* oth = taosArrayGetP(in, j);
      int     mid = optSysBinarySearch(oth, mi[j].idx, mi[j].len - 1, tgt);
      if (mid >= 0 && mid < mi[j].len) {
        uint64_t val = *(uint64_t*)taosArrayGet(oth, mid);
        has = (val == tgt ? true : false);
        mi[j].idx = mid;
      } else {
        has = false;
      }
    }
    if (has == true) {
      taosArrayPush(out, &tgt);
    }
  }
  taosMemoryFreeClear(mi);
}

static int32_t optSysMergeRslt(SArray* mRslt, SArray* rslt) {
  // TODO, find comm mem from mRslt
  for (int i = 0; i < taosArrayGetSize(mRslt); i++) {
    SArray* arslt = taosArrayGetP(mRslt, i);
    taosArraySort(arslt, tableUidCompare);
  }
  optSysIntersection(mRslt, rslt);
  return 0;
}

static int32_t optSysSpecialColumn(SNode* cond) {
  SOperatorNode* pOper = (SOperatorNode*)cond;
  SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
  for (int i = 0; i < sizeof(SYSTABLE_SPECIAL_COL) / sizeof(SYSTABLE_SPECIAL_COL[0]); i++) {
    if (0 == strcmp(pCol->colName, SYSTABLE_SPECIAL_COL[i])) {
      return 1;
    }
  }
  return 0;
}

static int32_t optSysTabFilte(void* arg, SNode* cond, SArray* result) {
  int ret = -1;
  if (nodeType(cond) == QUERY_NODE_OPERATOR) {
    ret = optSysTabFilteImpl(arg, cond, result);
    if (ret == 0) {
      SOperatorNode* pOper = (SOperatorNode*)cond;
      SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
      if (0 == strcmp(pCol->colName, "create_time")) {
        return 0;
      }
      return -1;
    }
    return ret;
  }

  if (nodeType(cond) != QUERY_NODE_LOGIC_CONDITION || ((SLogicConditionNode*)cond)->condType != LOGIC_COND_TYPE_AND) {
    return ret;
  }

  SLogicConditionNode* pNode = (SLogicConditionNode*)cond;
  SNodeList*           pList = (SNodeList*)pNode->pParameterList;

  int32_t len = LIST_LENGTH(pList);

  bool    hasIdx = false;
  bool    hasRslt = true;
  SArray* mRslt = taosArrayInit(len, POINTER_BYTES);

  SListCell* cell = pList->pHead;
  for (int i = 0; i < len; i++) {
    if (cell == NULL) break;

    SArray* aRslt = taosArrayInit(16, sizeof(int64_t));

    ret = optSysTabFilteImpl(arg, cell->pNode, aRslt);
    if (ret == 0) {
      // has index
      hasIdx = true;
      if (optSysSpecialColumn(cell->pNode) == 0) {
        taosArrayPush(mRslt, &aRslt);
      } else {
        // db_name/vgroup not result
        taosArrayDestroy(aRslt);
      }
    } else if (ret == -2) {
      // current vg
      hasIdx = true;
      hasRslt = false;
      taosArrayDestroy(aRslt);
      break;
    } else {
      taosArrayDestroy(aRslt);
    }
    cell = cell->pNext;
  }
  if (hasRslt && hasIdx) {
    optSysMergeRslt(mRslt, result);
  }

  for (int i = 0; i < taosArrayGetSize(mRslt); i++) {
    SArray* aRslt = taosArrayGetP(mRslt, i);
    taosArrayDestroy(aRslt);
  }
  taosArrayDestroy(mRslt);
  if (hasRslt == false) {
    return -2;
  }
  if (hasRslt && hasIdx) {
    cell = pList->pHead;
    for (int i = 0; i < len; i++) {
      if (cell == NULL) break;
      SOperatorNode* pOper = (SOperatorNode*)cell->pNode;
      SColumnNode*   pCol = (SColumnNode*)pOper->pLeft;
      if (0 == strcmp(pCol->colName, "create_time")) {
        return 0;
      }
      cell = cell->pNext;
    }
    return -1;
  }
  return -1;
}

static SSDataBlock* sysTableBuildUserTablesByUids(SOperatorInfo* pOperator) {
  SExecTaskInfo*     pTaskInfo = pOperator->pTaskInfo;
  SSysTableScanInfo* pInfo = pOperator->info;

  SSysTableIndex* pIdx = pInfo->pIdx;
  blockDataCleanup(pInfo->pRes);
  int32_t numOfRows = 0;

  int ret = 0;

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

  char    n[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  int32_t i = pIdx->lastIdx;
  for (; i < taosArrayGetSize(pIdx->uids); i++) {
    tb_uid_t* uid = taosArrayGet(pIdx->uids, i);

    SMetaReader mr = {0};
    metaReaderInit(&mr, pInfo->readHandle.meta, 0);
    ret = metaGetTableEntryByUid(&mr, *uid);
    if (ret < 0) {
      metaReaderClear(&mr);
      continue;
    }
    STR_TO_VARSTR(n, mr.me.name);

    // table name
    SColumnInfoData* pColInfoData = taosArrayGet(p->pDataBlock, 0);
    colDataAppend(pColInfoData, numOfRows, n, false);

    // database name
    pColInfoData = taosArrayGet(p->pDataBlock, 1);
    colDataAppend(pColInfoData, numOfRows, dbname, false);

    // vgId
    pColInfoData = taosArrayGet(p->pDataBlock, 6);
    colDataAppend(pColInfoData, numOfRows, (char*)&vgId, false);

    int32_t tableType = mr.me.type;
    if (tableType == TSDB_CHILD_TABLE) {
      // create time
      int64_t ts = mr.me.ctbEntry.ctime;
      pColInfoData = taosArrayGet(p->pDataBlock, 2);
      colDataAppend(pColInfoData, numOfRows, (char*)&ts, false);

      SMetaReader mr1 = {0};
      metaReaderInit(&mr1, pInfo->readHandle.meta, META_READER_NOLOCK);

      int64_t suid = mr.me.ctbEntry.suid;
      int32_t code = metaGetTableEntryByUid(&mr1, suid);
      if (code != TSDB_CODE_SUCCESS) {
        qError("failed to get super table meta, cname:%s, suid:0x%" PRIx64 ", code:%s, %s", pInfo->pCur->mr.me.name,
               suid, tstrerror(terrno), GET_TASKID(pTaskInfo));
        metaReaderClear(&mr1);
        metaReaderClear(&mr);
        T_LONG_JMP(pTaskInfo->env, terrno);
      }
      pColInfoData = taosArrayGet(p->pDataBlock, 3);
      colDataAppend(pColInfoData, numOfRows, (char*)&mr1.me.stbEntry.schemaRow.nCols, false);

      // super table name
      STR_TO_VARSTR(n, mr1.me.name);
      pColInfoData = taosArrayGet(p->pDataBlock, 4);
      colDataAppend(pColInfoData, numOfRows, n, false);
      metaReaderClear(&mr1);

      // table comment
      pColInfoData = taosArrayGet(p->pDataBlock, 8);
      if (mr.me.ctbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, mr.me.ctbEntry.comment);
        colDataAppend(pColInfoData, numOfRows, comment, false);
      } else if (mr.me.ctbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        colDataAppend(pColInfoData, numOfRows, comment, false);
      } else {
        colDataAppendNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataAppend(pColInfoData, numOfRows, (char*)&mr.me.uid, false);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      colDataAppend(pColInfoData, numOfRows, (char*)&mr.me.ctbEntry.ttlDays, false);

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
      if (mr.me.ntbEntry.commentLen > 0) {
        char comment[TSDB_TB_COMMENT_LEN + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, mr.me.ntbEntry.comment);
        colDataAppend(pColInfoData, numOfRows, comment, false);
      } else if (mr.me.ntbEntry.commentLen == 0) {
        char comment[VARSTR_HEADER_SIZE + VARSTR_HEADER_SIZE] = {0};
        STR_TO_VARSTR(comment, "");
        colDataAppend(pColInfoData, numOfRows, comment, false);
      } else {
        colDataAppendNULL(pColInfoData, numOfRows);
      }

      // uid
      pColInfoData = taosArrayGet(p->pDataBlock, 5);
      colDataAppend(pColInfoData, numOfRows, (char*)&mr.me.uid, false);

      // ttl
      pColInfoData = taosArrayGet(p->pDataBlock, 7);
      colDataAppend(pColInfoData, numOfRows, (char*)&mr.me.ntbEntry.ttlDays, false);

      STR_TO_VARSTR(n, "NORMAL_TABLE");
      // impl later
    }

    metaReaderClear(&mr);

    pColInfoData = taosArrayGet(p->pDataBlock, 9);
    colDataAppend(pColInfoData, numOfRows, n, false);

    if (++numOfRows >= pOperator->resultInfo.capacity) {
      p->info.rows = numOfRows;
      pInfo->pRes->info.rows = numOfRows;

      relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      doFilterResult(pInfo->pRes, pOperator->exprSupp.pFilterInfo);

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

    relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    doFilterResult(pInfo->pRes, pOperator->exprSupp.pFilterInfo);

    blockDataCleanup(p);
    numOfRows = 0;
  }

  if (i >= taosArrayGetSize(pIdx->uids)) {
    doSetOperatorCompleted(pOperator);
  } else {
    pIdx->lastIdx = i + 1;
  }

  blockDataDestroy(p);

  pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;
  return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
}

static SSDataBlock* sysTableBuildUserTables(SOperatorInfo* pOperator) {
  SExecTaskInfo* pTaskInfo = pOperator->pTaskInfo;

  SSysTableScanInfo* pInfo = pOperator->info;
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
      metaReaderInit(&mr, pInfo->readHandle.meta, META_READER_NOLOCK);

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

      relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
      doFilterResult(pInfo->pRes, pOperator->exprSupp.pFilterInfo);

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

    relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
    doFilterResult(pInfo->pRes, pOperator->exprSupp.pFilterInfo);

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

  SNode* pCondition = pInfo->pCondition;
  if (pOperator->status == OP_EXEC_DONE) {
    return NULL;
  }

  // the retrieve is executed on the mnode, so return tables that belongs to the information schema database.
  if (pInfo->readHandle.mnd != NULL) {
    buildSysDbTableInfo(pInfo, pOperator->resultInfo.capacity);
    doFilterResult(pInfo->pRes, pOperator->exprSupp.pFilterInfo);
    pInfo->loadInfo.totalRows += pInfo->pRes->info.rows;

    doSetOperatorCompleted(pOperator);
    return (pInfo->pRes->info.rows == 0) ? NULL : pInfo->pRes;
  } else {
    if (pInfo->showRewrite == false) {
      if (pCondition != NULL && pInfo->pIdx == NULL) {
        SSTabFltArg arg = {.pMeta = pInfo->readHandle.meta, .pVnode = pInfo->readHandle.vnode};

        SSysTableIndex* idx = taosMemoryMalloc(sizeof(SSysTableIndex));
        idx->init = 0;
        idx->uids = taosArrayInit(128, sizeof(int64_t));
        idx->lastIdx = 0;

        pInfo->pIdx = idx;  // set idx arg

        int flt = optSysTabFilte(&arg, pCondition, idx->uids);
        if (flt == 0) {
          pInfo->pIdx->init = 1;
          SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
          return blk;
        } else if (flt == -2) {
          qDebug("%s failed to get sys table info by idx, empty result", GET_TASKID(pTaskInfo));
          return NULL;
        } else if (flt == -1) {
          // not idx
          qDebug("%s failed to get sys table info by idx, scan sys table one by one", GET_TASKID(pTaskInfo));
        }
      } else if (pCondition != NULL && (pInfo->pIdx != NULL && pInfo->pIdx->init == 1)) {
        SSDataBlock* blk = sysTableBuildUserTablesByUids(pOperator);
        return blk;
      }
    }

    return sysTableBuildUserTables(pOperator);
  }
  return NULL;
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
      tstrncpy(pInfo->req.tb, tNameGetTableName(&pInfo->name), tListLen(pInfo->req.tb));
      tstrncpy(pInfo->req.user, pInfo->pUser, tListLen(pInfo->req.user));

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
      extractDataBlockFromFetchRsp(pInfo->pRes, pRsp->data, pInfo->matchInfo.pList, &pStart);
      updateLoadRemoteInfo(&pInfo->loadInfo, pRsp->numOfRows, pRsp->compLen, startTs, pOperator);

      // todo log the filter info
      doFilterResult(pInfo->pRes, pOperator->exprSupp.pFilterInfo);
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
  relocateColumnData(pInfo->pRes, pInfo->matchInfo.pList, p->pDataBlock, false);
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

  SScanPhysiNode*     pScanNode = &pScanPhyNode->scan;
  SDataBlockDescNode* pDescNode = pScanNode->node.pOutputDataBlockDesc;

  int32_t num = 0;
  int32_t code = extractColMatchInfo(pScanNode->pScanCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->accountId = pScanPhyNode->accountId;
  pInfo->pUser = taosMemoryStrDup((void*)pUser);
  pInfo->sysInfo = pScanPhyNode->sysInfo;
  pInfo->showRewrite = pScanPhyNode->showRewrite;
  pInfo->pRes = createResDataBlock(pDescNode);

  pInfo->pCondition = pScanNode->node.pConditions;
  code = filterInitFromNode(pScanNode->node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  initResultSizeInfo(&pOperator->resultInfo, 4096);
  blockDataEnsureCapacity(pInfo->pRes, pOperator->resultInfo.capacity);

  tNameAssign(&pInfo->name, &pScanNode->tableName);
  const char* name = tNameGetTableName(&pInfo->name);

  if (strncasecmp(name, TSDB_INS_TABLE_TABLES, TSDB_TABLE_FNAME_LEN) == 0 ||
      strncasecmp(name, TSDB_INS_TABLE_TAGS, TSDB_TABLE_FNAME_LEN) == 0) {
    pInfo->readHandle = *(SReadHandle*)readHandle;
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
  pOperator->exprSupp.numOfExprs = taosArrayGetSize(pInfo->pRes->pDataBlock);
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doSysTableScan, NULL, NULL, destroySysScanOperator, NULL);
  return pOperator;

_error:
  if (pInfo != NULL) {
    destroySysScanOperator(pInfo);
  }
  taosMemoryFreeClear(pOperator);
  pTaskInfo->code = code;
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
    STableKeyInfo* item = tableListGetInfo(pInfo->pTableList, pInfo->curPos);
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
  taosArrayDestroy(pInfo->matchInfo.pList);
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

  int32_t    numOfExprs = 0;
  SExprInfo* pExprInfo = createExprInfo(pPhyNode->pScanPseudoCols, NULL, &numOfExprs);
  int32_t code = initExprSupp(&pOperator->exprSupp, pExprInfo, numOfExprs);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int32_t num = 0;
  code = extractColMatchInfo(pPhyNode->pScanPseudoCols, pDescNode, &num, COL_MATCH_FROM_COL_ID, &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->pTableList = pTableListInfo;
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

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doTagScan, NULL, NULL, destroyTagScanOperatorInfo, NULL);

  return pOperator;

_error:
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return NULL;
}

// todo refactor
static int32_t loadDataBlockFromOneTable(SOperatorInfo* pOperator, STableMergeScanInfo* pTableScanInfo,
                                         SSDataBlock* pBlock, uint32_t* status) {
  SExecTaskInfo*       pTaskInfo = pOperator->pTaskInfo;
  STableMergeScanInfo* pInfo = pOperator->info;

  SFileBlockLoadRecorder* pCost = &pTableScanInfo->readRecorder;

  pCost->totalBlocks += 1;
  pCost->totalRows += pBlock->info.rows;

  *status = pInfo->dataBlockLoadFlag;
  if (pOperator->exprSupp.pFilterInfo != NULL ||
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
  } else if (*status == FUNC_DATA_REQUIRED_SMA_LOAD) {
    pCost->loadBlockStatis += 1;

    bool             allColumnsHaveAgg = true;
    SColumnDataAgg** pColAgg = NULL;

    if (allColumnsHaveAgg == true) {
      int32_t numOfCols = taosArrayGetSize(pBlock->pDataBlock);

      // todo create this buffer during creating operator
      if (pBlock->pBlockAgg == NULL) {
        pBlock->pBlockAgg = taosMemoryCalloc(numOfCols, POINTER_BYTES);
      }

      for (int32_t i = 0; i < numOfCols; ++i) {
        SColMatchItem* pColMatchInfo = taosArrayGet(pTableScanInfo->matchInfo.pList, i);
        if (!pColMatchInfo->needOutput) {
          continue;
        }
        pBlock->pBlockAgg[pColMatchInfo->dstSlotId] = pColAgg[i];
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

  STsdbReader* reader = pTableScanInfo->pReader;
  SArray*      pCols = tsdbRetrieveDataBlock(reader, NULL);
  if (pCols == NULL) {
    return terrno;
  }

  relocateColumnData(pBlock, pTableScanInfo->matchInfo.pList, pCols, true);

  // currently only the tbname pseudo column
  SExprSupp* pSup = &pTableScanInfo->pseudoSup;

  int32_t code = addTagPseudoColumnData(&pTableScanInfo->readHandle, pSup->pExprInfo, pSup->numOfExprs, pBlock,
                                        pBlock->info.rows, GET_TASKID(pTaskInfo), NULL);
  if (code != TSDB_CODE_SUCCESS) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  if (pOperator->exprSupp.pFilterInfo!= NULL) {
    int64_t st = taosGetTimestampMs();
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

  return TSDB_CODE_SUCCESS;
}

static SSDataBlock* getTableDataBlockImpl(void* param) {
  STableMergeScanSortSourceParam* source = param;
  SOperatorInfo*                  pOperator = source->pOperator;
  STableMergeScanInfo*            pInfo = pOperator->info;
  SExecTaskInfo*                  pTaskInfo = pOperator->pTaskInfo;
  int32_t                         readIdx = source->readerIdx;
  SSDataBlock*                    pBlock = source->inputBlock;
  STableMergeScanInfo*            pTableScanInfo = pOperator->info;

  SQueryTableDataCond* pQueryCond = taosArrayGet(pTableScanInfo->queryConds, readIdx);
  blockDataCleanup(pBlock);

  int64_t st = taosGetTimestampUs();

  void*        p = tableListGetInfo(pInfo->tableListInfo, readIdx + pInfo->tableStartIndex);
  SReadHandle* pHandle = &pInfo->readHandle;

  int32_t code = tsdbReaderOpen(pHandle->vnode, pQueryCond, p, 1, &pInfo->pReader, GET_TASKID(pTaskInfo));
  if (code != 0) {
    T_LONG_JMP(pTaskInfo->env, code);
  }

  STsdbReader* reader = pInfo->pReader;
  while (tsdbNextDataBlock(reader)) {
    if (isTaskKilled(pTaskInfo)) {
      T_LONG_JMP(pTaskInfo->env, TSDB_CODE_TSC_QUERY_CANCELLED);
    }

    // process this data block based on the probabilities
    bool processThisBlock = processBlockWithProbability(&pTableScanInfo->sample);
    if (!processThisBlock) {
      continue;
    }

    blockDataCleanup(pBlock);

    int32_t rows = 0;
    tsdbRetrieveDataBlockInfo(reader, &rows, &pBlock->info.uid, &pBlock->info.window);
    blockDataEnsureCapacity(pBlock, rows);
    pBlock->info.rows = rows;

    if (pQueryCond->order == TSDB_ORDER_ASC) {
      pQueryCond->twindows.skey = pBlock->info.window.ekey + 1;
    } else {
      pQueryCond->twindows.ekey = pBlock->info.window.skey - 1;
    }

    uint32_t status = 0;
    code = loadDataBlockFromOneTable(pOperator, pTableScanInfo, pBlock, &status);
    if (code != TSDB_CODE_SUCCESS) {
      T_LONG_JMP(pTaskInfo->env, code);
    }

    // current block is filter out according to filter condition, continue load the next block
    if (status == FUNC_DATA_REQUIRED_FILTEROUT || pBlock->info.rows == 0) {
      continue;
    }

    pBlock->info.groupId = getTableGroupId(pTaskInfo->pTableInfoList, pBlock->info.uid);

    pOperator->resultInfo.totalRows += pBlock->info.rows;
    pTableScanInfo->readRecorder.elapsedTime += (taosGetTimestampUs() - st) / 1000.0;

    tsdbReaderClose(pInfo->pReader);
    pInfo->pReader = NULL;
    return pBlock;
  }

  tsdbReaderClose(pInfo->pReader);
  pInfo->pReader = NULL;
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

int32_t dumpSQueryTableCond(const SQueryTableDataCond* src, SQueryTableDataCond* dst) {
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
    size_t  numOfTables = tableListGetSize(pInfo->tableListInfo);
    int32_t i = pInfo->tableStartIndex + 1;
    for (; i < numOfTables; ++i) {
      STableKeyInfo* tableKeyInfo = tableListGetInfo(pInfo->tableListInfo, i);
      if (tableKeyInfo->groupId != pInfo->groupId) {
        break;
      }
    }
    pInfo->tableEndIndex = i - 1;
  }

  int32_t tableStartIdx = pInfo->tableStartIndex;
  int32_t tableEndIdx = pInfo->tableEndIndex;

  pInfo->pReader = NULL;

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
    taosArrayPush(pInfo->sortSourceParams, &param);

    SQueryTableDataCond cond;
    dumpSQueryTableCond(&pInfo->cond, &cond);
    taosArrayPush(pInfo->queryConds, &cond);
  }

  for (int32_t i = 0; i < numOfTable; ++i) {
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

  for (int32_t i = 0; i < taosArrayGetSize(pInfo->queryConds); i++) {
    SQueryTableDataCond* cond = taosArrayGet(pInfo->queryConds, i);
    taosMemoryFree(cond->colList);
  }
  taosArrayDestroy(pInfo->queryConds);
  pInfo->queryConds = NULL;

  return TSDB_CODE_SUCCESS;
}

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

  qDebug("%s get sorted row blocks, rows:%d", GET_TASKID(pTaskInfo), pResBlock->info.rows);
  applyLimitOffset(&pInfo->limitInfo, pResBlock, pTaskInfo, pOperator);
  pInfo->limitInfo.numOfOutputRows += pResBlock->info.rows;

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

  size_t tableListSize = tableListGetSize(pInfo->tableListInfo);
  if (!pInfo->hasGroupId) {
    pInfo->hasGroupId = true;

    if (tableListSize == 0) {
      doSetOperatorCompleted(pOperator);
      return NULL;
    }
    pInfo->tableStartIndex = 0;
    pInfo->groupId = ((STableKeyInfo*)tableListGetInfo(pInfo->tableListInfo, pInfo->tableStartIndex))->groupId;
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
      pInfo->groupId = tableListGetInfo(pInfo->tableListInfo, pInfo->tableStartIndex)->groupId;
      startGroupTableMergeScan(pOperator);
    }
  }

  return pBlock;
}

void destroyTableMergeScanOperatorInfo(void* param) {
  STableMergeScanInfo* pTableScanInfo = (STableMergeScanInfo*)param;
  cleanupQueryTableDataCond(&pTableScanInfo->cond);

  int32_t numOfTable = taosArrayGetSize(pTableScanInfo->queryConds);

  for (int32_t i = 0; i < numOfTable; i++) {
    STableMergeScanSortSourceParam* p = taosArrayGet(pTableScanInfo->sortSourceParams, i);
    blockDataDestroy(p->inputBlock);
  }

  taosArrayDestroy(pTableScanInfo->sortSourceParams);

  tsdbReaderClose(pTableScanInfo->pReader);
  pTableScanInfo->pReader = NULL;

  for (int i = 0; i < taosArrayGetSize(pTableScanInfo->queryConds); i++) {
    SQueryTableDataCond* pCond = taosArrayGet(pTableScanInfo->queryConds, i);
    taosMemoryFree(pCond->colList);
  }
  taosArrayDestroy(pTableScanInfo->queryConds);

  if (pTableScanInfo->matchInfo.pList != NULL) {
    taosArrayDestroy(pTableScanInfo->matchInfo.pList);
  }

  pTableScanInfo->pResBlock = blockDataDestroy(pTableScanInfo->pResBlock);
  pTableScanInfo->pSortInputBlock = blockDataDestroy(pTableScanInfo->pSortInputBlock);

  taosArrayDestroy(pTableScanInfo->pSortInfo);
  cleanupExprSupp(&pTableScanInfo->pseudoSup);

  taosMemoryFreeClear(pTableScanInfo->rowEntryInfoOffset);
  taosMemoryFreeClear(param);
}

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

SOperatorInfo* createTableMergeScanOperatorInfo(STableScanPhysiNode* pTableScanNode, STableListInfo* pTableListInfo,
                                                SReadHandle* readHandle, SExecTaskInfo* pTaskInfo) {
  STableMergeScanInfo* pInfo = taosMemoryCalloc(1, sizeof(STableMergeScanInfo));
  SOperatorInfo*       pOperator = taosMemoryCalloc(1, sizeof(SOperatorInfo));
  if (pInfo == NULL || pOperator == NULL) {
    goto _error;
  }

  SDataBlockDescNode* pDescNode = pTableScanNode->scan.node.pOutputDataBlockDesc;

  int32_t numOfCols = 0;
  int32_t code = extractColMatchInfo(pTableScanNode->scan.pScanCols, pDescNode, &numOfCols, COL_MATCH_FROM_COL_ID,
                                     &pInfo->matchInfo);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  code = initQueryTableDataCond(&pInfo->cond, pTableScanNode);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pInfo->matchInfo.pList);
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


  code = filterInitFromNode((SNode*)pTableScanNode->scan.node.pConditions, &pOperator->exprSupp.pFilterInfo, 0);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  pInfo->tableListInfo = pTableListInfo;
  pInfo->scanFlag = MAIN_SCAN;

  initResultSizeInfo(&pOperator->resultInfo, 1024);
  pInfo->pResBlock = createResDataBlock(pDescNode);
  blockDataEnsureCapacity(pInfo->pResBlock, pOperator->resultInfo.capacity);

  pInfo->sortSourceParams = taosArrayInit(64, sizeof(STableMergeScanSortSourceParam));

  pInfo->pSortInfo = generateSortByTsInfo(pInfo->matchInfo.pList, pInfo->cond.order);
  pInfo->pSortInputBlock = createOneDataBlock(pInfo->pResBlock, false);
  initLimitInfo(pTableScanNode->scan.node.pLimit, pTableScanNode->scan.node.pSlimit, &pInfo->limitInfo);

  int32_t rowSize = pInfo->pResBlock->info.rowSize;
  pInfo->bufPageSize = getProperSortPageSize(rowSize);

  pOperator->name = "TableMergeScanOperator";
  pOperator->operatorType = QUERY_NODE_PHYSICAL_PLAN_TABLE_MERGE_SCAN;
  pOperator->blocking = false;
  pOperator->status = OP_NOT_OPENED;
  pOperator->info = pInfo;
  pOperator->exprSupp.numOfExprs = numOfCols;
  pOperator->pTaskInfo = pTaskInfo;

  pOperator->fpSet = createOperatorFpSet(operatorDummyOpenFn, doTableMergeScan, NULL, NULL,
                                         destroyTableMergeScanOperatorInfo, getTableMergeScanExplainExecInfo);
  pOperator->cost.openCost = 0;
  return pOperator;

_error:
  pTaskInfo->code = TSDB_CODE_OUT_OF_MEMORY;
  taosMemoryFree(pInfo);
  taosMemoryFree(pOperator);
  return NULL;
}
