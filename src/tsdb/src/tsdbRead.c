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

#include "os.h"
#include "tulog.h"
#include "talgo.h"
#include "tcompare.h"
#include "exception.h"

#include "../../query/inc/qAst.h"  // todo move to common module
#include "tlosertree.h"
#include "tsdb.h"
#include "tsdbMain.h"

#define EXTRA_BYTES 2
#define ASCENDING_TRAVERSE(o)   (o == TSDB_ORDER_ASC)
#define QH_GET_NUM_OF_COLS(handle) ((size_t)(taosArrayGetSize((handle)->pColumns)))

#define GET_FILE_DATA_BLOCK_INFO(_checkInfo, _block)                                   \
  ((SDataBlockInfo){.window = {.skey = (_block)->keyFirst, .ekey = (_block)->keyLast}, \
                    .numOfCols = (_block)->numOfCols,                                  \
                    .rows = (_block)->numOfRows,                                       \
                    .tid = (_checkInfo)->tableId.tid,                                  \
                    .uid = (_checkInfo)->tableId.uid})


enum {
  TSDB_QUERY_TYPE_ALL      = 1,
  TSDB_QUERY_TYPE_LAST     = 2,
  TSDB_QUERY_TYPE_EXTERNAL = 3,
};

typedef struct SQueryFilePos {
  int32_t fid;
  int32_t slot;
  int32_t pos;
  int64_t lastKey;
  int32_t rows;
  bool    mixBlock;
  bool    blockCompleted;
  STimeWindow win;
} SQueryFilePos;

typedef struct SDataBlockLoadInfo {
  SFileGroup* fileGroup;
  int32_t     slot;
  int32_t     tid;
  SArray*     pLoadedCols;
} SDataBlockLoadInfo;

typedef struct SLoadCompBlockInfo {
  int32_t tid; /* table tid */
  int32_t fileId;
} SLoadCompBlockInfo;

typedef struct STableCheckInfo {
  STableId      tableId;
  TSKEY         lastKey;
  STable*       pTableObj;
  SCompInfo*    pCompInfo;
  int32_t       compSize;
  int32_t       numOfBlocks:29; // number of qualified data blocks not the original blocks
  int8_t        chosen:2;       // indicate which iterator should move forward
  bool          initBuf;        // whether to initialize the in-memory skip list iterator or not
  SSkipListIterator* iter;      // mem buffer skip list iterator
  SSkipListIterator* iiter;     // imem buffer skip list iterator
} STableCheckInfo;

typedef struct STableBlockInfo {
  SCompBlock*        compBlock;
  STableCheckInfo*   pTableCheckInfo;
} STableBlockInfo;

typedef struct SBlockOrderSupporter {
  int32_t             numOfTables;
  STableBlockInfo**   pDataBlockInfo;
  int32_t*            blockIndexArray;
  int32_t*            numOfBlocksPerTable;
} SBlockOrderSupporter;

typedef struct SIOCostSummary {
  int64_t blockLoadTime;
  int64_t statisInfoLoadTime;
  int64_t checkForNextTime;
} SIOCostSummary;

typedef struct STsdbQueryHandle {
  STsdbRepo*     pTsdb;
  SQueryFilePos  cur;              // current position
  int16_t        order;
  STimeWindow    window;           // the primary query time window that applies to all queries
  SDataStatis*   statis;           // query level statistics, only one table block statistics info exists at any time
  int32_t        numOfBlocks;
  SArray*        pColumns;         // column list, SColumnInfoData array list
  bool           locateStart;
  int32_t        outputCapacity;
  int32_t        realNumOfRows;
  SArray*        pTableCheckInfo;  // SArray<STableCheckInfo>
  int32_t        activeIndex;
  bool           checkFiles;       // check file stage
  void*          qinfo;            // query info handle, for debug purpose
  int32_t        type;             // query type: retrieve all data blocks, 2. retrieve only last row, 3. retrieve direct prev|next rows
  SFileGroup*    pFileGroup;
  SFileGroupIter fileIter;
  SRWHelper      rhelper;
  STableBlockInfo* pDataBlockInfo;

  SDataCols     *pDataCols;        // in order to hold current file data block
  int32_t        allocSize;        // allocated data block size
  SMemRef       *pMemRef;
//  SMemTable     *mem;              // mem-table
//  SMemTable     *imem;             // imem-table, acquired from snapshot
  SArray        *defaultLoadColumn;// default load column
  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQuery */

  SIOCostSummary cost;
} STsdbQueryHandle;

typedef struct STableGroupSupporter {
  int32_t    numOfCols;
  SColIndex* pCols;
  STSchema*  pTagSchema;
} STableGroupSupporter;

static STimeWindow changeTableGroupByLastrow(STableGroupInfo *groupList);

static void    changeQueryHandleForInterpQuery(TsdbQueryHandleT pHandle);
static void    doMergeTwoLevelData(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SCompBlock* pBlock);
static int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
static int     tsdbReadRowsFromCache(STableCheckInfo* pCheckInfo, TSKEY maxKey, int maxRowsToRead, STimeWindow* win,
                                     STsdbQueryHandle* pQueryHandle);
static int     tsdbCheckInfoCompar(const void* key1, const void* key2);


static void tsdbInitDataBlockLoadInfo(SDataBlockLoadInfo* pBlockLoadInfo) {
  pBlockLoadInfo->slot = -1;
  pBlockLoadInfo->tid = -1;
  pBlockLoadInfo->fileGroup = NULL;
}

static void tsdbInitCompBlockLoadInfo(SLoadCompBlockInfo* pCompBlockLoadInfo) {
  pCompBlockLoadInfo->tid = -1;
  pCompBlockLoadInfo->fileId = -1;
}

static SArray* getColumnIdList(STsdbQueryHandle* pQueryHandle) {
  size_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);
  assert(numOfCols <= TSDB_MAX_COLUMNS);

  SArray* pIdList = taosArrayInit(numOfCols, sizeof(int16_t));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
    taosArrayPush(pIdList, &pCol->info.colId);
  }

  return pIdList;
}

static SArray* getDefaultLoadColumns(STsdbQueryHandle* pQueryHandle, bool loadTS) {
  SArray* pLocalIdList = getColumnIdList(pQueryHandle);

  // check if the primary time stamp column needs to load
  int16_t colId = *(int16_t*)taosArrayGet(pLocalIdList, 0);

  // the primary timestamp column does not be included in the the specified load column list, add it
  if (loadTS && colId != 0) {
    int16_t columnId = 0;
    taosArrayInsert(pLocalIdList, 0, &columnId);
  }

  return pLocalIdList;
}

static void tsdbMayTakeMemSnapshot(STsdbQueryHandle* pQueryHandle) {
  assert(pQueryHandle != NULL && pQueryHandle->pMemRef != NULL);

  SMemRef* pMemRef = pQueryHandle->pMemRef;
  if (pQueryHandle->pMemRef->ref++ == 0) {
    tsdbTakeMemSnapshot(pQueryHandle->pTsdb, (SMemTable**)&(pMemRef->mem), (SMemTable**)&(pMemRef->imem));
  }
}

static void tsdbMayUnTakeMemSnapshot(STsdbQueryHandle* pQueryHandle) {
  assert(pQueryHandle != NULL && pQueryHandle->pMemRef != NULL);

  SMemRef* pMemRef = pQueryHandle->pMemRef;
  if (--pMemRef->ref == 0) {
    tsdbUnTakeMemSnapShot(pQueryHandle->pTsdb, pMemRef->mem, pMemRef->imem);
    pMemRef->mem = NULL;
    pMemRef->imem = NULL;
  }

  pQueryHandle->pMemRef = NULL;
}
static SArray* createCheckInfoFromTableGroup(STsdbQueryHandle* pQueryHandle, STableGroupInfo* pGroupList, STsdbMeta* pMeta) {
  size_t sizeOfGroup = taosArrayGetSize(pGroupList->pGroupList);
  assert(sizeOfGroup >= 1 && pMeta != NULL);

  // allocate buffer in order to load data blocks from file
  SArray* pTableCheckInfo = taosArrayInit(pGroupList->numOfTables, sizeof(STableCheckInfo));
  if (pTableCheckInfo == NULL) {
    return NULL;
  }

  // todo apply the lastkey of table check to avoid to load header file
  for (int32_t i = 0; i < sizeOfGroup; ++i) {
    SArray* group = *(SArray**) taosArrayGet(pGroupList->pGroupList, i);

    size_t gsize = taosArrayGetSize(group);
    assert(gsize > 0);

    for (int32_t j = 0; j < gsize; ++j) {
      STableKeyInfo* pKeyInfo = (STableKeyInfo*) taosArrayGet(group, j);

      STableCheckInfo info = { .lastKey = pKeyInfo->lastKey, .pTableObj = pKeyInfo->pTable };
      info.tableId = ((STable*)(pKeyInfo->pTable))->tableId;

      assert(info.pTableObj != NULL && (info.pTableObj->type == TSDB_NORMAL_TABLE ||
                                        info.pTableObj->type == TSDB_CHILD_TABLE || info.pTableObj->type == TSDB_STREAM_TABLE));

      info.tableId.tid = info.pTableObj->tableId.tid;
      info.tableId.uid = info.pTableObj->tableId.uid;

      if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
        assert(info.lastKey >= pQueryHandle->window.skey);
      } else {
        assert(info.lastKey <= pQueryHandle->window.skey);
      }

      taosArrayPush(pTableCheckInfo, &info);
      tsdbDebug("%p check table uid:%"PRId64", tid:%d from lastKey:%"PRId64" %p", pQueryHandle, info.tableId.uid,
                info.tableId.tid, info.lastKey, pQueryHandle->qinfo);
    }
  }

  taosArraySort(pTableCheckInfo, tsdbCheckInfoCompar);
  return pTableCheckInfo;
}

static SArray* createCheckInfoFromCheckInfo(SArray* pTableCheckInfo, TSKEY skey) {
  size_t si = taosArrayGetSize(pTableCheckInfo);
  SArray* pNew = taosArrayInit(si, sizeof(STableCheckInfo));
  if (pNew == NULL) {
    return NULL;
  }

  for (int32_t j = 0; j < si; ++j) {
    STableCheckInfo* pCheckInfo = (STableCheckInfo*) taosArrayGet(pTableCheckInfo, j);
    STableCheckInfo info = { .lastKey = skey, .pTableObj = pCheckInfo->pTableObj};

    info.tableId = pCheckInfo->tableId;
    taosArrayPush(pNew, &info);
  }

  // it is ordered already, no need to sort again.
  taosArraySort(pNew, tsdbCheckInfoCompar);
  return pNew;
}

static STsdbQueryHandle* tsdbQueryTablesImpl(TSDB_REPO_T* tsdb, STsdbQueryCond* pCond, void* qinfo, SMemRef* pMemRef) {
  STsdbQueryHandle* pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  if (pQueryHandle == NULL) {
    goto out_of_memory;
  }

  pQueryHandle->order       = pCond->order;
  pQueryHandle->window      = pCond->twindow;
  pQueryHandle->pTsdb       = tsdb;
  pQueryHandle->type        = TSDB_QUERY_TYPE_ALL;
  pQueryHandle->cur.fid     = -1;
  pQueryHandle->cur.win     = TSWINDOW_INITIALIZER;
  pQueryHandle->checkFiles  = true;
  pQueryHandle->activeIndex = 0;   // current active table index
  pQueryHandle->qinfo       = qinfo;
  pQueryHandle->outputCapacity = ((STsdbRepo*)tsdb)->config.maxRowsPerFileBlock;
  pQueryHandle->allocSize   = 0;
  pQueryHandle->locateStart = false;
  pQueryHandle->pMemRef     = pMemRef;

  if (tsdbInitReadHelper(&pQueryHandle->rhelper, (STsdbRepo*) tsdb) != 0) {
    goto out_of_memory;
  }

  tsdbMayTakeMemSnapshot(pQueryHandle);
  assert(pCond != NULL && pCond->numOfCols > 0 && pMemRef != NULL);

  if (ASCENDING_TRAVERSE(pCond->order)) {
    assert(pQueryHandle->window.skey <= pQueryHandle->window.ekey);
  } else {
    assert(pQueryHandle->window.skey >= pQueryHandle->window.ekey);
  }

  // allocate buffer in order to load data blocks from file
  pQueryHandle->statis = calloc(pCond->numOfCols, sizeof(SDataStatis));
  if (pQueryHandle->statis == NULL) {
    goto out_of_memory;
  }

  pQueryHandle->pColumns = taosArrayInit(pCond->numOfCols, sizeof(SColumnInfoData));  // todo: use list instead of array?
  if (pQueryHandle->pColumns == NULL) {
    goto out_of_memory;
  }

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfoData  colInfo = {{0}, 0};

    colInfo.info = pCond->colList[i];
    colInfo.pData = calloc(1, EXTRA_BYTES + pQueryHandle->outputCapacity * pCond->colList[i].bytes);
    if (colInfo.pData == NULL) {
      goto out_of_memory;
    }
    taosArrayPush(pQueryHandle->pColumns, &colInfo);
    pQueryHandle->statis[i].colId = colInfo.info.colId;
  }

  pQueryHandle->defaultLoadColumn = getDefaultLoadColumns(pQueryHandle, true);

  STsdbMeta* pMeta = tsdbGetMeta(tsdb);
  assert(pMeta != NULL);

  pQueryHandle->pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pQueryHandle->pTsdb->config.maxRowsPerFileBlock);
  if (pQueryHandle->pDataCols == NULL) {
    tsdbError("%p failed to malloc buf for pDataCols, %p", pQueryHandle, pQueryHandle->qinfo);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto out_of_memory;
  }

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  return (TsdbQueryHandleT) pQueryHandle;

  out_of_memory:
  tsdbCleanupQueryHandle(pQueryHandle);
  terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  return NULL;
}

TsdbQueryHandleT* tsdbQueryTables(TSDB_REPO_T* tsdb, STsdbQueryCond* pCond, STableGroupInfo* groupList, void* qinfo, SMemRef* pRef) {
  STsdbQueryHandle* pQueryHandle = tsdbQueryTablesImpl(tsdb, pCond, qinfo, pRef);

  STsdbMeta* pMeta = tsdbGetMeta(tsdb);
  assert(pMeta != NULL);

  // todo apply the lastkey of table check to avoid to load header file
  pQueryHandle->pTableCheckInfo = createCheckInfoFromTableGroup(pQueryHandle, groupList, pMeta);
  if (pQueryHandle->pTableCheckInfo == NULL) {
    tsdbCleanupQueryHandle(pQueryHandle);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  tsdbDebug("%p total numOfTable:%" PRIzu " in query, %p", pQueryHandle, taosArrayGetSize(pQueryHandle->pTableCheckInfo), pQueryHandle->qinfo);
  return (TsdbQueryHandleT) pQueryHandle;
}

TsdbQueryHandleT tsdbQueryLastRow(TSDB_REPO_T *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, void* qinfo, SMemRef* pMemRef) {
  pCond->twindow = changeTableGroupByLastrow(groupList);

  // no qualified table
  if (groupList->numOfTables == 0) {
    return NULL;
  }

  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qinfo, pMemRef);

  assert(pCond->order == TSDB_ORDER_ASC && pCond->twindow.skey <= pCond->twindow.ekey);
  return pQueryHandle;
}

SArray* tsdbGetQueriedTableList(TsdbQueryHandleT *pHandle) {
  assert(pHandle != NULL);

  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) pHandle;

  size_t size = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  SArray* res = taosArrayInit(size, POINTER_BYTES);

  for(int32_t i = 0; i < size; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    taosArrayPush(res, &pCheckInfo->pTableObj);
  }

  return res;
}

TsdbQueryHandleT tsdbQueryRowsInExternalWindow(TSDB_REPO_T *tsdb, STsdbQueryCond* pCond, STableGroupInfo *groupList, void* qinfo, SMemRef* pRef) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qinfo, pRef);
  if (pQueryHandle != NULL) {
    pQueryHandle->type = TSDB_QUERY_TYPE_EXTERNAL;
    changeQueryHandleForInterpQuery(pQueryHandle);
  }
  return pQueryHandle;
}

static bool initTableMemIterator(STsdbQueryHandle* pHandle, STableCheckInfo* pCheckInfo) {
  STable* pTable = pCheckInfo->pTableObj;
  assert(pTable != NULL);

  if (pCheckInfo->initBuf) {
    return true;
  }

  pCheckInfo->initBuf = true;
  int32_t order = pHandle->order;

  // no data in buffer, abort
  if (pHandle->pMemRef->mem == NULL && pHandle->pMemRef->imem == NULL) {
    return false;
  }

  assert(pCheckInfo->iter == NULL && pCheckInfo->iiter == NULL);

  STableData* pMem = NULL;
  STableData* pIMem = NULL;

  SMemTable* pMemT = pHandle->pMemRef->mem;
  SMemTable* pIMemT = pHandle->pMemRef->imem;

  if (pMemT && pCheckInfo->tableId.tid < pMemT->maxTables) {
    pMem = pMemT->tData[pCheckInfo->tableId.tid];
    if (pMem != NULL && pMem->uid == pCheckInfo->tableId.uid) { // check uid
      pCheckInfo->iter =
          tSkipListCreateIterFromVal(pMem->pData, (const char*)&pCheckInfo->lastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
    }
  }

  if (pIMemT && pCheckInfo->tableId.tid < pIMemT->maxTables) {
    pIMem = pIMemT->tData[pCheckInfo->tableId.tid];
    if (pIMem != NULL && pIMem->uid == pCheckInfo->tableId.uid) { // check uid
      pCheckInfo->iiter =
          tSkipListCreateIterFromVal(pIMem->pData, (const char*)&pCheckInfo->lastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
    }
  }

  // both iterators are NULL, no data in buffer right now
  if (pCheckInfo->iter == NULL && pCheckInfo->iiter == NULL) {
    return false;
  }

  bool memEmpty  = (pCheckInfo->iter == NULL) || (pCheckInfo->iter != NULL && !tSkipListIterNext(pCheckInfo->iter));
  bool imemEmpty = (pCheckInfo->iiter == NULL) || (pCheckInfo->iiter != NULL && !tSkipListIterNext(pCheckInfo->iiter));
  if (memEmpty && imemEmpty) { // buffer is empty
    return false;
  }

  if (!memEmpty) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    assert(node != NULL);

    SDataRow row = *(SDataRow *)SL_GET_NODE_DATA(node);
    TSKEY key = dataRowKey(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64 ", tid:%d check data in mem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
              "-%" PRId64 ", lastKey:%" PRId64 ", numOfRows:%"PRId64", %p",
              pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, key, order, pMem->keyFirst, pMem->keyLast,
              pCheckInfo->lastKey, pMem->numOfRows, pHandle->qinfo);

    if (ASCENDING_TRAVERSE(order)) {
      assert(pCheckInfo->lastKey <= key);
    } else {
      assert(pCheckInfo->lastKey >= key);
    }

  } else {
    tsdbDebug("%p uid:%"PRId64", tid:%d no data in mem, %p", pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid,
        pHandle->qinfo);
  }

  if (!imemEmpty) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    assert(node != NULL);

    SDataRow row = *(SDataRow *)SL_GET_NODE_DATA(node);
    TSKEY key = dataRowKey(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64 ", tid:%d check data in imem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
              "-%" PRId64 ", lastKey:%" PRId64 ", numOfRows:%"PRId64", %p",
              pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, key, order, pIMem->keyFirst, pIMem->keyLast,
              pCheckInfo->lastKey, pIMem->numOfRows, pHandle->qinfo);

    if (ASCENDING_TRAVERSE(order)) {
      assert(pCheckInfo->lastKey <= key);
    } else {
      assert(pCheckInfo->lastKey >= key);
    }
  } else {
    tsdbDebug("%p uid:%"PRId64", tid:%d no data in imem, %p", pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid,
        pHandle->qinfo);
  }

  return true;
}

static void destroyTableMemIterator(STableCheckInfo* pCheckInfo) {
  tSkipListDestroyIter(pCheckInfo->iter);
  tSkipListDestroyIter(pCheckInfo->iiter);
}

static SDataRow getSDataRowInTableMem(STableCheckInfo* pCheckInfo, int32_t order) {
  SDataRow rmem = NULL, rimem = NULL;
  if (pCheckInfo->iter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    if (node != NULL) {
      rmem = *(SDataRow *)SL_GET_NODE_DATA(node);
    }
  }

  if (pCheckInfo->iiter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    if (node != NULL) {
      rimem = *(SDataRow *)SL_GET_NODE_DATA(node);
    }
  }

  if (rmem == NULL && rimem == NULL) {
    return NULL;
  }

  if (rmem != NULL && rimem == NULL) {
    pCheckInfo->chosen = 0;
    return rmem;
  }

  if (rmem == NULL && rimem != NULL) {
    pCheckInfo->chosen = 1;
    return rimem;
  }

  TSKEY r1 = dataRowKey(rmem);
  TSKEY r2 = dataRowKey(rimem);

  if (r1 == r2) { // data ts are duplicated, ignore the data in mem
    tSkipListIterNext(pCheckInfo->iter);
    pCheckInfo->chosen = 1;
    return rimem;
  } else {
    if (ASCENDING_TRAVERSE(order)) {
      if (r1 < r2) {
        pCheckInfo->chosen = 0;
        return rmem;
      } else {
        pCheckInfo->chosen = 1;
        return rimem;
      }
    } else {
      if (r1 < r2) {
        pCheckInfo->chosen = 1;
        return rimem;
      } else {
        pCheckInfo->chosen = 0;
        return rmem;
      }
    }
  }
}

static bool moveToNextRowInMem(STableCheckInfo* pCheckInfo) {
  bool hasNext = false;
  if (pCheckInfo->chosen == 0) {
    if (pCheckInfo->iter != NULL) {
      hasNext = tSkipListIterNext(pCheckInfo->iter);
    }

    if (hasNext) {
      return hasNext;
    }

    if (pCheckInfo->iiter != NULL) {
      return tSkipListIterGet(pCheckInfo->iiter) != NULL;
    }
  } else { //pCheckInfo->chosen == 1
    if (pCheckInfo->iiter != NULL) {
      hasNext = tSkipListIterNext(pCheckInfo->iiter);
    }

    if (hasNext) {
      return hasNext;
    }

    if (pCheckInfo->iter != NULL) {
      return tSkipListIterGet(pCheckInfo->iter) != NULL;
    }
  }

  return hasNext;
}

static bool hasMoreDataInCache(STsdbQueryHandle* pHandle) {
  size_t size = taosArrayGetSize(pHandle->pTableCheckInfo);
  assert(pHandle->activeIndex < size && pHandle->activeIndex >= 0 && size >= 1);
  pHandle->cur.fid = -1;

  STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);

  STable* pTable = pCheckInfo->pTableObj;
  assert(pTable != NULL);

  if (!pCheckInfo->initBuf) {
    initTableMemIterator(pHandle, pCheckInfo);
  }

  SDataRow row = getSDataRowInTableMem(pCheckInfo, pHandle->order);
  if (row == NULL) {
    return false;
  }

  pCheckInfo->lastKey = dataRowKey(row);  // first timestamp in buffer
  tsdbDebug("%p uid:%" PRId64", tid:%d check data in buffer from skey:%" PRId64 ", order:%d, %p", pHandle,
      pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, pCheckInfo->lastKey, pHandle->order, pHandle->qinfo);

  // all data in mem are checked already.
  if ((pCheckInfo->lastKey > pHandle->window.ekey && ASCENDING_TRAVERSE(pHandle->order)) ||
      (pCheckInfo->lastKey < pHandle->window.ekey && !ASCENDING_TRAVERSE(pHandle->order))) {
    return false;
  }

  int32_t step = ASCENDING_TRAVERSE(pHandle->order)? 1:-1;
  STimeWindow* win = &pHandle->cur.win;
  pHandle->cur.rows = tsdbReadRowsFromCache(pCheckInfo, pHandle->window.ekey, pHandle->outputCapacity, win, pHandle);

  // update the last key value
  pCheckInfo->lastKey = win->ekey + step;
  pHandle->cur.lastKey = win->ekey + step;
  pHandle->cur.mixBlock = true;

  if (!ASCENDING_TRAVERSE(pHandle->order)) {
    SWAP(win->skey, win->ekey, TSKEY);
  }

  return true;
}

static int32_t getFileIdFromKey(TSKEY key, int32_t daysPerFile, int32_t precision) {
  assert(precision >= TSDB_TIME_PRECISION_MICRO || precision <= TSDB_TIME_PRECISION_NANO);
  if (key == TSKEY_INITIAL_VAL) {
    return INT32_MIN;
  }

  int64_t fid = (int64_t)(key / (daysPerFile * tsMsPerDay[precision]));  // set the starting fileId
  if (fid < 0L && llabs(fid) > INT32_MAX) { // data value overflow for INT32
    fid = INT32_MIN;
  }

  if (fid > 0L && fid > INT32_MAX) {
    fid = INT32_MAX;
  }

  return (int32_t)fid;
}

static int32_t binarySearchForBlock(SCompBlock* pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order) {
  int32_t firstSlot = 0;
  int32_t lastSlot = numOfBlocks - 1;

  int32_t midSlot = firstSlot;

  while (1) {
    numOfBlocks = lastSlot - firstSlot + 1;
    midSlot = (firstSlot + (numOfBlocks >> 1));

    if (numOfBlocks == 1) break;

    if (skey > pBlock[midSlot].keyLast) {
      if (numOfBlocks == 2) break;
      if ((order == TSDB_ORDER_DESC) && (skey < pBlock[midSlot + 1].keyFirst)) break;
      firstSlot = midSlot + 1;
    } else if (skey < pBlock[midSlot].keyFirst) {
      if ((order == TSDB_ORDER_ASC) && (skey > pBlock[midSlot - 1].keyLast)) break;
      lastSlot = midSlot - 1;
    } else {
      break;  // got the slot
    }
  }

  return midSlot;
}

static int32_t getFileCompInfo(STsdbQueryHandle* pQueryHandle, int32_t* numOfBlocks) {
  // load all the comp offset value for all tables in this file
  int32_t code = TSDB_CODE_SUCCESS;

  *numOfBlocks = 0;
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    pCheckInfo->numOfBlocks = 0;

    if (tsdbSetHelperTable(&pQueryHandle->rhelper, pCheckInfo->pTableObj, pQueryHandle->pTsdb) != TSDB_CODE_SUCCESS) {
      code = terrno;
      break;
    }

    SCompIdx* compIndex = &pQueryHandle->rhelper.curCompIdx;

    // no data block in this file, try next file
    if (compIndex->len == 0 || compIndex->numOfBlocks == 0 || compIndex->uid != pCheckInfo->tableId.uid) {
      continue; // no data blocks in the file belongs to pCheckInfo->pTable
    }

    if (pCheckInfo->compSize < (int32_t)compIndex->len) {
      assert(compIndex->len > 0);

      char* t = realloc(pCheckInfo->pCompInfo, compIndex->len);
      if (t == NULL) {
        terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
        code = TSDB_CODE_TDB_OUT_OF_MEMORY;
        break;
      }

      pCheckInfo->pCompInfo = (SCompInfo*) t;
      pCheckInfo->compSize = compIndex->len;
    }

    tsdbLoadCompInfo(&(pQueryHandle->rhelper), (void *)(pCheckInfo->pCompInfo));
    SCompInfo* pCompInfo = pCheckInfo->pCompInfo;

    TSKEY s = TSKEY_INITIAL_VAL, e = TSKEY_INITIAL_VAL;

    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      assert(pCheckInfo->lastKey <= pQueryHandle->window.ekey && pQueryHandle->window.skey <= pQueryHandle->window.ekey);
    } else {
      assert(pCheckInfo->lastKey >= pQueryHandle->window.ekey && pQueryHandle->window.skey >= pQueryHandle->window.ekey);
    }

    s = MIN(pCheckInfo->lastKey, pQueryHandle->window.ekey);
    e = MAX(pCheckInfo->lastKey, pQueryHandle->window.ekey);

    // discard the unqualified data block based on the query time window
    int32_t start = binarySearchForBlock(pCompInfo->blocks, compIndex->numOfBlocks, s, TSDB_ORDER_ASC);
    int32_t end = start;

    if (s > pCompInfo->blocks[start].keyLast) {
      continue;
    }

    // todo speedup the procedure of located end block
    while (end < (int32_t)compIndex->numOfBlocks && (pCompInfo->blocks[end].keyFirst <= e)) {
      end += 1;
    }

    pCheckInfo->numOfBlocks = (end - start);

    if (start > 0) {
      memmove(pCompInfo->blocks, &pCompInfo->blocks[start], pCheckInfo->numOfBlocks * sizeof(SCompBlock));
    }

    (*numOfBlocks) += pCheckInfo->numOfBlocks;
  }

  return code;
}

static int32_t doLoadFileDataBlock(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo, int32_t slotIndex) {
  int64_t st = taosGetTimestampUs();

  STSchema *pSchema = tsdbGetTableSchema(pCheckInfo->pTableObj);
  int32_t   code = tdInitDataCols(pQueryHandle->pDataCols, pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for pDataCols, %p", pQueryHandle, pQueryHandle->qinfo);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  code = tdInitDataCols(pQueryHandle->rhelper.pDataCols[0], pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for rhelper.pDataCols[0], %p", pQueryHandle, pQueryHandle->qinfo);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  code = tdInitDataCols(pQueryHandle->rhelper.pDataCols[1], pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for rhelper.pDataCols[1], %p", pQueryHandle, pQueryHandle->qinfo);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  int16_t* colIds = pQueryHandle->defaultLoadColumn->pData;

  int32_t ret = tsdbLoadBlockDataCols(&(pQueryHandle->rhelper), pBlock, pCheckInfo->pCompInfo, colIds, (int)(QH_GET_NUM_OF_COLS(pQueryHandle)));
  if (ret != TSDB_CODE_SUCCESS) {
    int32_t c = terrno;
    assert(c != TSDB_CODE_SUCCESS);
    goto _error;
  }

  SDataBlockLoadInfo* pBlockLoadInfo = &pQueryHandle->dataBlockLoadInfo;

  pBlockLoadInfo->fileGroup = pQueryHandle->pFileGroup;
  pBlockLoadInfo->slot = pQueryHandle->cur.slot;
  pBlockLoadInfo->tid = pCheckInfo->pTableObj->tableId.tid;

  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
  assert(pCols->numOfRows != 0 && pCols->numOfRows <= pBlock->numOfRows);

  pBlock->numOfRows = pCols->numOfRows;

  int64_t elapsedTime = (taosGetTimestampUs() - st);
  pQueryHandle->cost.blockLoadTime += elapsedTime;

  tsdbDebug("%p load file block into buffer, index:%d, brange:%"PRId64"-%"PRId64", rows:%d, elapsed time:%"PRId64 " us, %p",
      pQueryHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, elapsedTime, pQueryHandle->qinfo);
  return TSDB_CODE_SUCCESS;

_error:
  pBlock->numOfRows = 0;

  tsdbError("%p error occurs in loading file block, index:%d, brange:%"PRId64"-%"PRId64", rows:%d, %p",
            pQueryHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, pQueryHandle->qinfo);
  return terrno;
}

static int32_t getEndPosInDataBlock(STsdbQueryHandle* pQueryHandle, SDataBlockInfo* pBlockInfo);
static int32_t doCopyRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows, int32_t start, int32_t end);
static void moveDataToFront(STsdbQueryHandle* pQueryHandle, int32_t numOfRows, int32_t numOfCols);
static void doCheckGeneratedBlockRange(STsdbQueryHandle* pQueryHandle);
static void copyAllRemainRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SDataBlockInfo* pBlockInfo, int32_t endPos);

static int32_t handleDataMergeIfNeeded(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo){
  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataBlockInfo binfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);
  int32_t code = TSDB_CODE_SUCCESS;

  /*bool hasData = */ initTableMemIterator(pQueryHandle, pCheckInfo);
  SDataRow row = getSDataRowInTableMem(pCheckInfo, pQueryHandle->order);

  assert(cur->pos >= 0 && cur->pos <= binfo.rows);

  TSKEY key = (row != NULL)? dataRowKey(row):TSKEY_INITIAL_VAL;
  tsdbDebug("%p key in mem:%"PRId64", %p", pQueryHandle, key, pQueryHandle->qinfo);

  if ((ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key <= binfo.window.ekey)) ||
      (!ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key >= binfo.window.skey))) {

    if ((ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key < binfo.window.skey)) ||
        (!ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key > binfo.window.ekey))) {

      // do not load file block into buffer
      int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order) ? 1 : -1;

      TSKEY maxKey = ASCENDING_TRAVERSE(pQueryHandle->order)? (binfo.window.skey - step):(binfo.window.ekey - step);
      cur->rows = tsdbReadRowsFromCache(pCheckInfo, maxKey, pQueryHandle->outputCapacity, &cur->win, pQueryHandle);
      pQueryHandle->realNumOfRows = cur->rows;

      // update the last key value
      pCheckInfo->lastKey = cur->win.ekey + step;
      if (!ASCENDING_TRAVERSE(pQueryHandle->order)) {
        SWAP(cur->win.skey, cur->win.ekey, TSKEY);
      }

      cur->mixBlock = true;
      cur->blockCompleted = false;
      return code;
    }

    // return error, add test cases
    if ((code = doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    doMergeTwoLevelData(pQueryHandle, pCheckInfo, pBlock);
  } else {
    /*
     * no data in cache, only load data from file
     * during the query processing, data in cache will not be checked anymore.
     *
     * Here the buffer is not enough, so only part of file block can be loaded into memory buffer
     */
    assert(pQueryHandle->outputCapacity >= binfo.rows);
    int32_t endPos = getEndPosInDataBlock(pQueryHandle, &binfo);

    if ((cur->pos == 0 && endPos == binfo.rows -1 && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
        (cur->pos == (binfo.rows - 1) && endPos == 0 && (!ASCENDING_TRAVERSE(pQueryHandle->order)))) {
      pQueryHandle->realNumOfRows = binfo.rows;

      cur->rows = binfo.rows;
      cur->win  = binfo.window;
      cur->mixBlock = false;
      cur->blockCompleted = true;

      if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
        cur->lastKey = binfo.window.ekey + 1;
        cur->pos = binfo.rows;
      } else {
        cur->lastKey = binfo.window.skey - 1;
        cur->pos = -1;
      }
    } else { // partially copy to dest buffer
      copyAllRemainRowsFromFileBlock(pQueryHandle, pCheckInfo, &binfo, endPos);
      cur->mixBlock = true;
    }

    assert(cur->blockCompleted);
    tsdbDebug("create data block from remain file block, brange:%"PRId64"-%"PRId64", rows:%d, lastKey:%"PRId64", %p",
        cur->win.skey, cur->win.ekey, cur->rows, cur->lastKey, pQueryHandle);
  }

  return code;
}

static int32_t loadFileDataBlock(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo, bool* exists) {
  SQueryFilePos* cur = &pQueryHandle->cur;
  int32_t code = TSDB_CODE_SUCCESS;

  if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
    // query ended in/started from current block
    if (pQueryHandle->window.ekey < pBlock->keyLast || pCheckInfo->lastKey > pBlock->keyFirst) {
      if ((code = doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
        *exists = false;
        return code;
      }

      SDataCols* pTSCol = pQueryHandle->rhelper.pDataCols[0];
      assert(pTSCol->cols->type == TSDB_DATA_TYPE_TIMESTAMP && pTSCol->numOfRows == pBlock->numOfRows);

      if (pCheckInfo->lastKey > pBlock->keyFirst) {
        cur->pos =
            binarySearchForKey(pTSCol->cols[0].pData, pBlock->numOfRows, pCheckInfo->lastKey, pQueryHandle->order);
      } else {
        cur->pos = 0;
      }

      assert(pCheckInfo->lastKey <= pBlock->keyLast);
      doMergeTwoLevelData(pQueryHandle, pCheckInfo, pBlock);
    } else {  // the whole block is loaded in to buffer
      cur->pos = ASCENDING_TRAVERSE(pQueryHandle->order)? 0:(pBlock->numOfRows - 1);
      code = handleDataMergeIfNeeded(pQueryHandle, pBlock, pCheckInfo);
    }
  } else {  //desc order, query ended in current block
    if (pQueryHandle->window.ekey > pBlock->keyFirst || pCheckInfo->lastKey < pBlock->keyLast) {
      if ((code = doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
        *exists = false;
        return code;
      }

      SDataCols* pTSCol = pQueryHandle->rhelper.pDataCols[0];
      if (pCheckInfo->lastKey < pBlock->keyLast) {
        cur->pos = binarySearchForKey(pTSCol->cols[0].pData, pBlock->numOfRows, pCheckInfo->lastKey, pQueryHandle->order);
      } else {
        cur->pos = pBlock->numOfRows - 1;
      }

      assert(pCheckInfo->lastKey >= pBlock->keyFirst);
      doMergeTwoLevelData(pQueryHandle, pCheckInfo, pBlock);
    } else {
      cur->pos = ASCENDING_TRAVERSE(pQueryHandle->order)? 0:(pBlock->numOfRows-1);
      code = handleDataMergeIfNeeded(pQueryHandle, pBlock, pCheckInfo);
    }
  }

  *exists = pQueryHandle->realNumOfRows > 0;
  return code;
}

static int doBinarySearchKey(char* pValue, int num, TSKEY key, int order) {
  int    firstPos, lastPos, midPos = -1;
  int    numOfRows;
  TSKEY* keyList;

  assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);

  if (num <= 0) return -1;

  keyList = (TSKEY*)pValue;
  firstPos = 0;
  lastPos = num - 1;

  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger than the key
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

int32_t doCopyRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows, int32_t start, int32_t end) {
  char* pData = NULL;
  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1 : -1;

  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
  TSKEY* tsArray = pCols->cols[0].pData;

  int32_t num = end - start + 1;
  assert(num >= 0);

  if (num == 0) {
    return numOfRows;
  }

  int32_t requiredNumOfCols = (int32_t)taosArrayGetSize(pQueryHandle->pColumns);

  //data in buffer has greater timestamp, copy data in file block
  int32_t i = 0, j = 0;
  while(i < requiredNumOfCols && j < pCols->numOfCols) {
    SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);

    SDataCol* src = &pCols->cols[j];
    if (src->colId < pColInfo->info.colId) {
      j++;
      continue;
    }

    int32_t bytes = pColInfo->info.bytes;

    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = (char*)pColInfo->pData + (capacity - numOfRows - num) * pColInfo->info.bytes;
    }

    if (pColInfo->info.colId == src->colId) {

      if (pColInfo->info.type != TSDB_DATA_TYPE_BINARY && pColInfo->info.type != TSDB_DATA_TYPE_NCHAR) {
        memmove(pData, (char*)src->pData + bytes * start, bytes * num);
      } else {  // handle the var-string
        char* dst = pData;

        // todo refactor, only copy one-by-one
        for (int32_t k = start; k < num + start; ++k) {
          char* p = tdGetColDataOfRow(src, k);
          memcpy(dst, p, varDataTLen(p));
          dst += bytes;
        }
      }

      j++;
      i++;
    } else { // pColInfo->info.colId < src->colId, it is a NULL data
      if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
        char* dst = pData;

        for(int32_t k = start; k < num + start; ++k) {
          setVardataNull(dst, pColInfo->info.type);
          dst += bytes;
        }
      } else {
        setNullN(pData, pColInfo->info.type, pColInfo->info.bytes, num);
      }
      i++;
    }
  }

  while (i < requiredNumOfCols) { // the remain columns are all null data
    SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = (char*)pColInfo->pData + (capacity - numOfRows - num) * pColInfo->info.bytes;
    }

    if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
      char* dst = pData;

      for(int32_t k = start; k < num + start; ++k) {
        setVardataNull(dst, pColInfo->info.type);
        dst += pColInfo->info.bytes;
      }
    } else {
      setNullN(pData, pColInfo->info.type, pColInfo->info.bytes, num);
    }

    i++;
  }

  pQueryHandle->cur.win.ekey = tsArray[end];
  pQueryHandle->cur.lastKey = tsArray[end] + step;

  return numOfRows + num;
}

static void copyOneRowFromMem(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows, SDataRow row,
                              int32_t numOfCols, STable* pTable) {
  char* pData = NULL;

  // the schema version info is embeded in SDataRow
  STSchema* pSchema = tsdbGetTableSchemaByVersion(pTable, dataRowVersion(row));
  int32_t numOfRowCols = schemaNCols(pSchema);

  int32_t i = 0, j = 0;
  while(i < numOfCols && j < numOfRowCols) {
    SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
    if (pSchema->columns[j].colId < pColInfo->info.colId) {
      j++;
      continue;
    }

    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = (char*)pColInfo->pData + (capacity - numOfRows - 1) * pColInfo->info.bytes;
    }

    if (pSchema->columns[j].colId == pColInfo->info.colId) {
      void* value = tdGetRowDataOfCol(row, (int8_t)pColInfo->info.type, TD_DATA_ROW_HEAD_SIZE + pSchema->columns[j].offset);
      if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
        memcpy(pData, value, varDataTLen(value));
      } else {
        memcpy(pData, value, pColInfo->info.bytes);
      }

      j++;
      i++;
    } else { // pColInfo->info.colId < pSchema->columns[j].colId, it is a NULL data
      if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
        setVardataNull(pData, pColInfo->info.type);
      } else {
        setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
      }
      i++;
    }
  }

  while (i < numOfCols) { // the remain columns are all null data
    SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = (char*)pColInfo->pData + (capacity - numOfRows - 1) * pColInfo->info.bytes;
    }

    if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
      setVardataNull(pData, pColInfo->info.type);
    } else {
      setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
    }

    i++;
  }
}

static void moveDataToFront(STsdbQueryHandle* pQueryHandle, int32_t numOfRows, int32_t numOfCols) {
  if (numOfRows == 0 || ASCENDING_TRAVERSE(pQueryHandle->order)) {
    return;
  }

  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  if (numOfRows < pQueryHandle->outputCapacity) {
    int32_t emptySize = pQueryHandle->outputCapacity - numOfRows;
    for(int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      memmove((char*)pColInfo->pData, (char*)pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
    }
  }
}

static void getQualifiedRowsPos(STsdbQueryHandle* pQueryHandle, int32_t startPos, int32_t endPos, int32_t numOfExisted,
                                int32_t* start, int32_t* end) {
  *start = -1;

  if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
    int32_t remain = endPos - startPos + 1;
    if (remain + numOfExisted > pQueryHandle->outputCapacity) {
      *end = (pQueryHandle->outputCapacity - numOfExisted) + startPos - 1;
    } else {
      *end = endPos;
    }

    *start = startPos;
  } else {
    int32_t remain = (startPos - endPos) + 1;
    if (remain + numOfExisted > pQueryHandle->outputCapacity) {
      *end = startPos + 1 - (pQueryHandle->outputCapacity - numOfExisted);
    } else {
      *end = endPos;
    }

    *start = *end;
    *end = startPos;
  }
}

static void updateInfoAfterMerge(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, int32_t numOfRows, int32_t endPos) {
  SQueryFilePos* cur = &pQueryHandle->cur;

  pCheckInfo->lastKey = cur->lastKey;
  pQueryHandle->realNumOfRows = numOfRows;
  cur->rows = numOfRows;
  cur->pos = endPos;
}

static void doCheckGeneratedBlockRange(STsdbQueryHandle* pQueryHandle) {
  SQueryFilePos* cur = &pQueryHandle->cur;

  if (cur->rows > 0) {
    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      assert(cur->win.skey >= pQueryHandle->window.skey && cur->win.ekey <= pQueryHandle->window.ekey);
    } else {
      assert(cur->win.skey >= pQueryHandle->window.ekey && cur->win.ekey <= pQueryHandle->window.skey);
    }

    SColumnInfoData* pColInfoData = taosArrayGet(pQueryHandle->pColumns, 0);
    assert(cur->win.skey == ((TSKEY*)pColInfoData->pData)[0] && cur->win.ekey == ((TSKEY*)pColInfoData->pData)[cur->rows-1]);
  } else {
    cur->win = pQueryHandle->window;

    int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1;
    cur->lastKey = pQueryHandle->window.ekey + step;
  }
}

static void copyAllRemainRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SDataBlockInfo* pBlockInfo, int32_t endPos) {
  SQueryFilePos* cur = &pQueryHandle->cur;

  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
  TSKEY* tsArray = pCols->cols[0].pData;

  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1;
  int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pQueryHandle));

  int32_t pos = cur->pos;

  int32_t start = cur->pos;
  int32_t end = endPos;

  if (!ASCENDING_TRAVERSE(pQueryHandle->order)) {
    SWAP(start, end, int32_t);
  }

  assert(pQueryHandle->outputCapacity >= (end - start + 1));
  int32_t numOfRows = doCopyRowsFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, 0, start, end);

  // the time window should always be ascending order: skey <= ekey
  cur->win = (STimeWindow) {.skey = tsArray[start], .ekey = tsArray[end]};
  cur->mixBlock = (numOfRows != pBlockInfo->rows);
  cur->lastKey = tsArray[endPos] + step;
  cur->blockCompleted = true;

  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  moveDataToFront(pQueryHandle, numOfRows, numOfCols);

  // The value of pos may be -1 or pBlockInfo->rows, and it is invalid in both cases.
  pos = endPos + step;
  updateInfoAfterMerge(pQueryHandle, pCheckInfo, numOfRows, pos);
  doCheckGeneratedBlockRange(pQueryHandle);

  tsdbDebug("%p uid:%" PRIu64",tid:%d data block created, mixblock:%d, brange:%"PRIu64"-%"PRIu64" rows:%d, %p",
            pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, cur->mixBlock, cur->win.skey,
            cur->win.ekey, cur->rows, pQueryHandle->qinfo);
}

int32_t getEndPosInDataBlock(STsdbQueryHandle* pQueryHandle, SDataBlockInfo* pBlockInfo) {
  // NOTE: reverse the order to find the end position in data block
  int32_t endPos = -1;
  int32_t order = ASCENDING_TRAVERSE(pQueryHandle->order)? TSDB_ORDER_DESC : TSDB_ORDER_ASC;

  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];

  if (ASCENDING_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey >= pBlockInfo->window.ekey) {
    endPos = pBlockInfo->rows - 1;
    cur->mixBlock = (cur->pos != 0);
  } else if (!ASCENDING_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey <= pBlockInfo->window.skey) {
    endPos = 0;
    cur->mixBlock = (cur->pos != pBlockInfo->rows - 1);
  } else {
    assert(pCols->numOfRows > 0);
    endPos = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, pQueryHandle->window.ekey, order);
    cur->mixBlock = true;
  }

  return endPos;
}

// only return the qualified data to client in terms of query time window, data rows in the same block but do not
// be included in the query time window will be discarded
static void doMergeTwoLevelData(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SCompBlock* pBlock) {
  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataBlockInfo blockInfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);

  initTableMemIterator(pQueryHandle, pCheckInfo);

  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
  assert(pCols->cols[0].type == TSDB_DATA_TYPE_TIMESTAMP && pCols->cols[0].colId == PRIMARYKEY_TIMESTAMP_COL_INDEX &&
      cur->pos >= 0 && cur->pos < pBlock->numOfRows);

  TSKEY* tsArray = pCols->cols[0].pData;
  assert(pCols->numOfRows == pBlock->numOfRows && tsArray[0] == pBlock->keyFirst && tsArray[pBlock->numOfRows-1] == pBlock->keyLast);

  // for search the endPos, so the order needs to reverse
  int32_t order = (pQueryHandle->order == TSDB_ORDER_ASC)? TSDB_ORDER_DESC:TSDB_ORDER_ASC;

  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1;
  int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pQueryHandle));

  STable* pTable = pCheckInfo->pTableObj;
  int32_t endPos = getEndPosInDataBlock(pQueryHandle, &blockInfo);

  tsdbDebug("%p uid:%" PRIu64",tid:%d start merge data block, file block range:%"PRIu64"-%"PRIu64" rows:%d, start:%d,"
            "end:%d, %p",
            pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, blockInfo.window.skey, blockInfo.window.ekey,
            blockInfo.rows, cur->pos, endPos, pQueryHandle->qinfo);

  // compared with the data from in-memory buffer, to generate the correct timestamp array list
  int32_t numOfRows = 0;

  int32_t pos = cur->pos;
  cur->win = TSWINDOW_INITIALIZER;

  // no data in buffer, load data from file directly
  if (pCheckInfo->iiter == NULL && pCheckInfo->iter == NULL) {
    copyAllRemainRowsFromFileBlock(pQueryHandle, pCheckInfo, &blockInfo, endPos);
    return;
  } else if (pCheckInfo->iter != NULL || pCheckInfo->iiter != NULL) {
    SSkipListNode* node = NULL;
    do {
      SDataRow row = getSDataRowInTableMem(pCheckInfo, pQueryHandle->order);
      if (row == NULL) {
        break;
      }

      TSKEY key = dataRowKey(row);
      if ((key > pQueryHandle->window.ekey && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          (key < pQueryHandle->window.ekey && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        break;
      }

      if (((tsArray[pos] > pQueryHandle->window.ekey || pos > endPos) && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          ((tsArray[pos] < pQueryHandle->window.ekey || pos < endPos) && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        break;
      }

      if ((key < tsArray[pos] && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          (key > tsArray[pos] && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        copyOneRowFromMem(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, row, numOfCols, pTable);
        numOfRows += 1;
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = key;
        }

        cur->win.ekey = key;
        cur->lastKey  = key + step;
        cur->mixBlock = true;

        moveToNextRowInMem(pCheckInfo);
      } else if (key == tsArray[pos]) {  // data in buffer has the same timestamp of data in file block, ignore it
        moveToNextRowInMem(pCheckInfo);
      } else if ((key > tsArray[pos] && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
                  (key < tsArray[pos] && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = tsArray[pos];
        }

        int32_t end = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, key, order);
        assert(end != -1);

        if (tsArray[end] == key) { // the value of key in cache equals to the end timestamp value, ignore it
          moveToNextRowInMem(pCheckInfo);
        }

        int32_t qstart = 0, qend = 0;
        getQualifiedRowsPos(pQueryHandle, pos, end, numOfRows, &qstart, &qend);

        numOfRows = doCopyRowsFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, qstart, qend);
        pos += (qend - qstart + 1) * step;

        cur->win.ekey = ASCENDING_TRAVERSE(pQueryHandle->order)? tsArray[qend]:tsArray[qstart];
        cur->lastKey  = cur->win.ekey + step;
      }
    } while (numOfRows < pQueryHandle->outputCapacity);

    if (numOfRows < pQueryHandle->outputCapacity) {
      /**
       * if cache is empty, load remain file block data. In contrast, if there are remain data in cache, do NOT
       * copy them all to result buffer, since it may be overlapped with file data block.
       */
      if (node == NULL ||
          ((dataRowKey(*(SDataRow *)SL_GET_NODE_DATA(node)) > pQueryHandle->window.ekey) && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          ((dataRowKey(*(SDataRow *)SL_GET_NODE_DATA(node)) < pQueryHandle->window.ekey) && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        // no data in cache or data in cache is greater than the ekey of time window, load data from file block
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = tsArray[pos];
        }

        int32_t start = -1, end = -1;
        getQualifiedRowsPos(pQueryHandle, pos, endPos, numOfRows, &start, &end);

        numOfRows = doCopyRowsFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, start, end);
        pos += (end - start + 1) * step;

        cur->win.ekey = ASCENDING_TRAVERSE(pQueryHandle->order)? tsArray[end]:tsArray[start];
        cur->lastKey  = cur->win.ekey + step;
        cur->mixBlock = true;
      }
    }
  }

  cur->blockCompleted =
      (((pos > endPos || cur->lastKey > pQueryHandle->window.ekey) && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
       ((pos < endPos || cur->lastKey < pQueryHandle->window.ekey) && !ASCENDING_TRAVERSE(pQueryHandle->order)));

  if (!ASCENDING_TRAVERSE(pQueryHandle->order)) {
    SWAP(cur->win.skey, cur->win.ekey, TSKEY);
  }

  moveDataToFront(pQueryHandle, numOfRows, numOfCols);
  updateInfoAfterMerge(pQueryHandle, pCheckInfo, numOfRows, pos);
  doCheckGeneratedBlockRange(pQueryHandle);

  tsdbDebug("%p uid:%" PRIu64",tid:%d data block created, mixblock:%d, brange:%"PRIu64"-%"PRIu64" rows:%d, %p",
      pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, cur->mixBlock, cur->win.skey,
      cur->win.ekey, cur->rows, pQueryHandle->qinfo);
}

int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order) {
  int    firstPos, lastPos, midPos = -1;
  int    numOfRows;
  TSKEY* keyList;

  if (num <= 0) return -1;

  keyList = (TSKEY*)pValue;
  firstPos = 0;
  lastPos = num - 1;

  if (order == TSDB_ORDER_DESC) {
    // find the first position which is smaller than the key
    while (1) {
      if (key >= keyList[lastPos]) return lastPos;
      if (key == keyList[firstPos]) return firstPos;
      if (key < keyList[firstPos]) return firstPos - 1;

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }

  } else {
    // find the first position which is bigger than the key
    while (1) {
      if (key <= keyList[firstPos]) return firstPos;
      if (key == keyList[lastPos]) return lastPos;

      if (key > keyList[lastPos]) {
        lastPos = lastPos + 1;
        if (lastPos >= num)
          return -1;
        else
          return lastPos;
      }

      numOfRows = lastPos - firstPos + 1;
      midPos = (numOfRows >> 1) + firstPos;

      if (key < keyList[midPos]) {
        lastPos = midPos - 1;
      } else if (key > keyList[midPos]) {
        firstPos = midPos + 1;
      } else {
        break;
      }
    }
  }

  return midPos;
}

static void cleanBlockOrderSupporter(SBlockOrderSupporter* pSupporter, int32_t numOfTables) {
  taosTFree(pSupporter->numOfBlocksPerTable);
  taosTFree(pSupporter->blockIndexArray);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableBlockInfo* pBlockInfo = pSupporter->pDataBlockInfo[i];
    taosTFree(pBlockInfo);
  }

  taosTFree(pSupporter->pDataBlockInfo);
}

static int32_t dataBlockOrderCompar(const void* pLeft, const void* pRight, void* param) {
  int32_t leftTableIndex = *(int32_t*)pLeft;
  int32_t rightTableIndex = *(int32_t*)pRight;

  SBlockOrderSupporter* pSupporter = (SBlockOrderSupporter*)param;

  int32_t leftTableBlockIndex = pSupporter->blockIndexArray[leftTableIndex];
  int32_t rightTableBlockIndex = pSupporter->blockIndexArray[rightTableIndex];

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerTable[leftTableIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerTable[rightTableIndex]) {
    /* right block is empty */
    return -1;
  }

  STableBlockInfo* pLeftBlockInfoEx = &pSupporter->pDataBlockInfo[leftTableIndex][leftTableBlockIndex];
  STableBlockInfo* pRightBlockInfoEx = &pSupporter->pDataBlockInfo[rightTableIndex][rightTableBlockIndex];

  //    assert(pLeftBlockInfoEx->compBlock->offset != pRightBlockInfoEx->compBlock->offset);
#if 0	// TODO: temporarily comment off requested by Dr. Liao
  if (pLeftBlockInfoEx->compBlock->offset == pRightBlockInfoEx->compBlock->offset &&
      pLeftBlockInfoEx->compBlock->last == pRightBlockInfoEx->compBlock->last) {
    tsdbError("error in header file, two block with same offset:%" PRId64, (int64_t)pLeftBlockInfoEx->compBlock->offset);
  }
#endif

  return pLeftBlockInfoEx->compBlock->offset > pRightBlockInfoEx->compBlock->offset ? 1 : -1;
}

static int32_t createDataBlocksInfo(STsdbQueryHandle* pQueryHandle, int32_t numOfBlocks, int32_t* numOfAllocBlocks) {
  size_t size = sizeof(STableBlockInfo) * numOfBlocks;

  if (pQueryHandle->allocSize < size) {
    pQueryHandle->allocSize = (int32_t)size;
    char* tmp = realloc(pQueryHandle->pDataBlockInfo, pQueryHandle->allocSize);
    if (tmp == NULL) {
      return TSDB_CODE_TDB_OUT_OF_MEMORY;
    }

    pQueryHandle->pDataBlockInfo = (STableBlockInfo*) tmp;
  }

  memset(pQueryHandle->pDataBlockInfo, 0, size);
  *numOfAllocBlocks = numOfBlocks;

  int32_t numOfTables = (int32_t)taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  SBlockOrderSupporter sup = {0};
  sup.numOfTables = numOfTables;
  sup.numOfBlocksPerTable = calloc(1, sizeof(int32_t) * numOfTables);
  sup.blockIndexArray = calloc(1, sizeof(int32_t) * numOfTables);
  sup.pDataBlockInfo = calloc(1, POINTER_BYTES * numOfTables);

  if (sup.numOfBlocksPerTable == NULL || sup.blockIndexArray == NULL || sup.pDataBlockInfo == NULL) {
    cleanBlockOrderSupporter(&sup, 0);
    return TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  int32_t cnt = 0;
  int32_t numOfQualTables = 0;

  for (int32_t j = 0; j < numOfTables; ++j) {
    STableCheckInfo* pTableCheck = (STableCheckInfo*)taosArrayGet(pQueryHandle->pTableCheckInfo, j);
    if (pTableCheck->numOfBlocks <= 0) {
      continue;
    }

    SCompBlock* pBlock = pTableCheck->pCompInfo->blocks;
    sup.numOfBlocksPerTable[numOfQualTables] = pTableCheck->numOfBlocks;

    char* buf = calloc(1, sizeof(STableBlockInfo) * pTableCheck->numOfBlocks);
    if (buf == NULL) {
      cleanBlockOrderSupporter(&sup, numOfQualTables);
      return TSDB_CODE_TDB_OUT_OF_MEMORY;
    }

    sup.pDataBlockInfo[numOfQualTables] = (STableBlockInfo*)buf;

    for (int32_t k = 0; k < pTableCheck->numOfBlocks; ++k) {
      STableBlockInfo* pBlockInfo = &sup.pDataBlockInfo[numOfQualTables][k];

      pBlockInfo->compBlock = &pBlock[k];
      pBlockInfo->pTableCheckInfo = pTableCheck;
      cnt++;
    }

    numOfQualTables++;
  }

  assert(numOfBlocks == cnt);

  // since there is only one table qualified, blocks are not sorted
  if (numOfQualTables == 1) {
    memcpy(pQueryHandle->pDataBlockInfo, sup.pDataBlockInfo[0], sizeof(STableBlockInfo) * numOfBlocks);
    cleanBlockOrderSupporter(&sup, numOfQualTables);

    tsdbDebug("%p create data blocks info struct completed for 1 table, %d blocks not sorted %p ", pQueryHandle, cnt,
        pQueryHandle->qinfo);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %p", pQueryHandle, cnt,
      numOfQualTables, pQueryHandle->qinfo);

  assert(cnt <= numOfBlocks && numOfQualTables <= numOfTables);  // the pTableQueryInfo[j]->numOfBlocks may be 0
  sup.numOfTables = numOfQualTables;

  SLoserTreeInfo* pTree = NULL;
  uint8_t ret = tLoserTreeCreate(&pTree, sup.numOfTables, &sup, dataBlockOrderCompar);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanBlockOrderSupporter(&sup, numOfTables);
    return TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;

  while (numOfTotal < cnt) {
    int32_t pos = pTree->pNode[0].index;
    int32_t index = sup.blockIndexArray[pos]++;

    STableBlockInfo* pBlocksInfo = sup.pDataBlockInfo[pos];
    pQueryHandle->pDataBlockInfo[numOfTotal++] = pBlocksInfo[index];

    // set data block index overflow, in order to disable the offset comparator
    if (sup.blockIndexArray[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.blockIndexArray[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    tLoserTreeAdjust(pTree, pos + sup.numOfTables);
  }

  /*
   * available when no import exists
   * for(int32_t i = 0; i < cnt - 1; ++i) {
   *   assert((*pDataBlockInfo)[i].compBlock->offset < (*pDataBlockInfo)[i+1].compBlock->offset);
   * }
   */

  tsdbDebug("%p %d data blocks sort completed, %p", pQueryHandle, cnt, pQueryHandle->qinfo);
  cleanBlockOrderSupporter(&sup, numOfTables);
  free(pTree);

  return TSDB_CODE_SUCCESS;
}

static int32_t getDataBlocksInFilesImpl(STsdbQueryHandle* pQueryHandle, bool* exists) {
  pQueryHandle->numOfBlocks = 0;
  SQueryFilePos* cur = &pQueryHandle->cur;

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t numOfBlocks = 0;
  int32_t numOfTables = (int32_t)taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  STsdbCfg* pCfg = &pQueryHandle->pTsdb->config;
  STimeWindow win = TSWINDOW_INITIALIZER;

  while (true) {
    pthread_rwlock_rdlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);

    if ((pQueryHandle->pFileGroup = tsdbGetFileGroupNext(&pQueryHandle->fileIter)) == NULL) {
      pthread_rwlock_unlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);
      break;
    }

    tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, pQueryHandle->pFileGroup->fileId, &win.skey, &win.ekey);

    // current file are not overlapped with query time window, ignore remain files
    if ((ASCENDING_TRAVERSE(pQueryHandle->order) && win.skey > pQueryHandle->window.ekey) ||
        (!ASCENDING_TRAVERSE(pQueryHandle->order) && win.ekey < pQueryHandle->window.ekey)) {
      pthread_rwlock_unlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, %p", pQueryHandle,
                pQueryHandle->window.skey, pQueryHandle->window.ekey, pQueryHandle->qinfo);
      pQueryHandle->pFileGroup = NULL;
      assert(pQueryHandle->numOfBlocks == 0);
      break;
    }

    if (tsdbSetAndOpenHelperFile(&pQueryHandle->rhelper, pQueryHandle->pFileGroup) < 0) {
      pthread_rwlock_unlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);
      code = terrno;
      break;
    }

    pthread_rwlock_unlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);

    if (tsdbLoadCompIdx(&pQueryHandle->rhelper, NULL) < 0) {
      code = terrno;
      break;
    }

    if ((code = getFileCompInfo(pQueryHandle, &numOfBlocks)) != TSDB_CODE_SUCCESS) {
      break;
    }

    tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, %p", pQueryHandle, numOfBlocks, numOfTables,
              pQueryHandle->pFileGroup->fileId, pQueryHandle->qinfo);

    assert(numOfBlocks >= 0);
    if (numOfBlocks == 0) {
      continue;
    }

    // todo return error code to query engine
    if ((code = createDataBlocksInfo(pQueryHandle, numOfBlocks, &pQueryHandle->numOfBlocks)) != TSDB_CODE_SUCCESS) {
      break;
    }

    assert(numOfBlocks >= pQueryHandle->numOfBlocks);
    if (pQueryHandle->numOfBlocks > 0) {
      break;
    }
  }

  // no data in file anymore
  if (pQueryHandle->numOfBlocks <= 0 || code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_SUCCESS) {
      assert(pQueryHandle->pFileGroup == NULL);
    }

    cur->fid = -1;  // denote that there are no data in file anymore
    *exists = false;
    return code;
  }

  assert(pQueryHandle->pFileGroup != NULL && pQueryHandle->numOfBlocks > 0);
  cur->slot = ASCENDING_TRAVERSE(pQueryHandle->order)? 0:pQueryHandle->numOfBlocks-1;
  cur->fid = pQueryHandle->pFileGroup->fileId;

  STableBlockInfo* pBlockInfo = &pQueryHandle->pDataBlockInfo[cur->slot];
  return loadFileDataBlock(pQueryHandle, pBlockInfo->compBlock, pBlockInfo->pTableCheckInfo, exists);
}

static int32_t getDataBlocksInFiles(STsdbQueryHandle* pQueryHandle, bool* exists) {
  STsdbFileH*    pFileHandle = tsdbGetFile(pQueryHandle->pTsdb);
  SQueryFilePos* cur = &pQueryHandle->cur;

  // find the start data block in file
  if (!pQueryHandle->locateStart) {
    pQueryHandle->locateStart = true;
    STsdbCfg* pCfg = &pQueryHandle->pTsdb->config;
    int32_t fid = getFileIdFromKey(pQueryHandle->window.skey, pCfg->daysPerFile, pCfg->precision);

    pthread_rwlock_rdlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);
    tsdbInitFileGroupIter(pFileHandle, &pQueryHandle->fileIter, pQueryHandle->order);
    tsdbSeekFileGroupIter(&pQueryHandle->fileIter, fid);
    pthread_rwlock_unlock(&pQueryHandle->pTsdb->tsdbFileH->fhlock);

    return getDataBlocksInFilesImpl(pQueryHandle, exists);
  } else {
    // check if current file block is all consumed
    STableBlockInfo* pBlockInfo = &pQueryHandle->pDataBlockInfo[cur->slot];
    STableCheckInfo* pCheckInfo = pBlockInfo->pTableCheckInfo;

    // current block is done, try next
    if ((!cur->mixBlock) || cur->blockCompleted) {
      if ((cur->slot == pQueryHandle->numOfBlocks - 1 && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          (cur->slot == 0 && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        // all data blocks in current file has been checked already, try next file if exists
        return getDataBlocksInFilesImpl(pQueryHandle, exists);
      } else {
        // next block of the same file
        int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order) ? 1 : -1;
        cur->slot += step;

        cur->mixBlock = false;
        cur->blockCompleted = false;

        STableBlockInfo* pNext = &pQueryHandle->pDataBlockInfo[cur->slot];
        return loadFileDataBlock(pQueryHandle, pNext->compBlock, pNext->pTableCheckInfo, exists);
      }
    } else {
      tsdbDebug("%p continue in current data block, index:%d, pos:%d, %p", pQueryHandle, cur->slot, cur->pos, pQueryHandle->qinfo);
      int32_t code = handleDataMergeIfNeeded(pQueryHandle, pBlockInfo->compBlock, pCheckInfo);
      *exists = pQueryHandle->realNumOfRows > 0;

      return code;
    }
  }
}

static bool doHasDataInBuffer(STsdbQueryHandle* pQueryHandle) {
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  
  while (pQueryHandle->activeIndex < numOfTables) {
    if (hasMoreDataInCache(pQueryHandle)) {
      return true;
    }

    pQueryHandle->activeIndex += 1;
  }

  return false;
}

static void changeQueryHandleForInterpQuery(TsdbQueryHandleT pHandle) {
  // filter the queried time stamp in the first place
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;
  pQueryHandle->order = TSDB_ORDER_DESC;

  assert(pQueryHandle->window.skey == pQueryHandle->window.ekey);

  // starts from the buffer in case of descending timestamp order check data blocks
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  int32_t i = 0;
  while(i < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);

    // the first qualified table for interpolation query
    if (pQueryHandle->window.skey <= pCheckInfo->pTableObj->lastKey &&
        pCheckInfo->pTableObj->lastKey != TSKEY_INITIAL_VAL) {
      break;
    }

    i++;
  }

  // there are no data in all the tables
  if (i == numOfTables) {
    return;
  }

  STableCheckInfo info = *(STableCheckInfo*) taosArrayGet(pQueryHandle->pTableCheckInfo, i);
  taosArrayClear(pQueryHandle->pTableCheckInfo);

  info.lastKey = pQueryHandle->window.skey;
  taosArrayPush(pQueryHandle->pTableCheckInfo, &info);

  // update the query time window according to the chosen last timestamp
  pQueryHandle->window = (STimeWindow) {info.lastKey, TSKEY_INITIAL_VAL};
}

static int tsdbReadRowsFromCache(STableCheckInfo* pCheckInfo, TSKEY maxKey, int maxRowsToRead, STimeWindow* win,
                                 STsdbQueryHandle* pQueryHandle) {
  int     numOfRows = 0;
  int32_t numOfCols = (int32_t)taosArrayGetSize(pQueryHandle->pColumns);
  win->skey = TSKEY_INITIAL_VAL;

  int64_t st = taosGetTimestampUs();
  STable* pTable = pCheckInfo->pTableObj;

  do {
    SDataRow row = getSDataRowInTableMem(pCheckInfo, pQueryHandle->order);
    if (row == NULL) {
      break;
    }

    TSKEY key = dataRowKey(row);
    if ((key > maxKey && ASCENDING_TRAVERSE(pQueryHandle->order)) || (key < maxKey && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
      tsdbDebug("%p key:%"PRIu64" beyond qrange:%"PRId64" - %"PRId64", no more data in buffer", pQueryHandle, key, pQueryHandle->window.skey,
                pQueryHandle->window.ekey);

      break;
    }

    if (win->skey == INT64_MIN) {
      win->skey = key;
    }

    win->ekey = key;
    copyOneRowFromMem(pQueryHandle, maxRowsToRead, numOfRows, row, numOfCols, pTable);

    if (++numOfRows >= maxRowsToRead) {
      moveToNextRowInMem(pCheckInfo);
      break;
    }

  } while(moveToNextRowInMem(pCheckInfo));

  assert(numOfRows <= maxRowsToRead);

  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  if (!ASCENDING_TRAVERSE(pQueryHandle->order) && numOfRows < maxRowsToRead) {
    int32_t emptySize = maxRowsToRead - numOfRows;

    for(int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      memmove((char*)pColInfo->pData, (char*)pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
    }
  }

  int64_t elapsedTime = taosGetTimestampUs() - st;
  tsdbDebug("%p build data block from cache completed, elapsed time:%"PRId64" us, numOfRows:%d, numOfCols:%d, %p", pQueryHandle,
            elapsedTime, numOfRows, numOfCols, pQueryHandle->qinfo);

  return numOfRows;
}

static int32_t getAllTableList(STable* pSuperTable, SArray* list) {
  SSkipListIterator* iter = tSkipListCreateIter(pSuperTable->pIndex);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* pNode = tSkipListIterGet(iter);

    STable** pTable = (STable**) SL_GET_NODE_DATA((SSkipListNode*) pNode);

    STableKeyInfo info = {.pTable = *pTable, .lastKey = TSKEY_INITIAL_VAL};
    taosArrayPush(list, &info);
  }

  tSkipListDestroyIter(iter);
  return TSDB_CODE_SUCCESS;
}

static void destroyHelper(void* param) {
  if (param == NULL) {
    return;
  }


  tQueryInfo* pInfo = (tQueryInfo*)param;
  if (pInfo->optr != TSDB_RELATION_IN) {
    taosTFree(pInfo->q);
  }

  free(param);
}

// handle data in cache situation
bool tsdbNextDataBlock(TsdbQueryHandleT* pHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;

  int64_t stime = taosGetTimestampUs();
  int64_t elapsedTime = stime;

  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  assert(numOfTables > 0);

  SDataBlockInfo blockInfo = {{0}, 0};
  if (pQueryHandle->type == TSDB_QUERY_TYPE_EXTERNAL) {
    pQueryHandle->type = TSDB_QUERY_TYPE_ALL;
    pQueryHandle->order = TSDB_ORDER_DESC;

    if (!tsdbNextDataBlock(pHandle)) {
      return false;
    }

    tsdbRetrieveDataBlockInfo(pHandle, &blockInfo);
    /*SArray *pDataBlock = */tsdbRetrieveDataBlock(pHandle, pQueryHandle->defaultLoadColumn);
    if (terrno != TSDB_CODE_SUCCESS) {
      return false;
    }

    if (pQueryHandle->cur.win.ekey == pQueryHandle->window.skey) {
      // data already retrieve, discard other data rows and return
      int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pQueryHandle));
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
        memcpy((char*)pCol->pData, (char*)pCol->pData + pCol->info.bytes * (pQueryHandle->cur.rows - 1), pCol->info.bytes);
      }

      pQueryHandle->cur.win  = (STimeWindow){pQueryHandle->window.skey, pQueryHandle->window.skey};
      pQueryHandle->window   = pQueryHandle->cur.win;
      pQueryHandle->cur.rows = 1;
      pQueryHandle->type = TSDB_QUERY_TYPE_ALL;
      return true;
    } else {
      STimeWindow win = (STimeWindow) {pQueryHandle->window.skey, INT64_MAX};
      STsdbQueryCond cond = {
          .order = TSDB_ORDER_ASC,
          .numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pQueryHandle))
        };
      cond.twindow = win;

      cond.colList = calloc(cond.numOfCols, sizeof(SColumnInfo));
      if (cond.colList == NULL) {
        terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
        return false;
      }

      for(int32_t i = 0; i < cond.numOfCols; ++i) {
        SColumnInfoData* pColInfoData = taosArrayGet(pQueryHandle->pColumns, i);
        memcpy(&cond.colList[i], &pColInfoData->info, sizeof(SColumnInfo));
      }

      STsdbQueryHandle* pSecQueryHandle = tsdbQueryTablesImpl(pQueryHandle->pTsdb, &cond, pQueryHandle->qinfo, pQueryHandle->pMemRef);

      taosTFree(cond.colList);

      pSecQueryHandle->pTableCheckInfo = createCheckInfoFromCheckInfo(pQueryHandle->pTableCheckInfo, pSecQueryHandle->window.skey);
      if (pSecQueryHandle->pTableCheckInfo == NULL) {
        tsdbCleanupQueryHandle(pSecQueryHandle);
        return false;
      }

      if (!tsdbNextDataBlock((void*) pSecQueryHandle)) {
        tsdbCleanupQueryHandle(pSecQueryHandle);
        return false;
      }

      tsdbRetrieveDataBlockInfo((void*) pSecQueryHandle, &blockInfo);
      tsdbRetrieveDataBlock((void*) pSecQueryHandle, pSecQueryHandle->defaultLoadColumn);

      int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pSecQueryHandle));
      size_t si = taosArrayGetSize(pSecQueryHandle->pTableCheckInfo);

      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
        memcpy((char*)pCol->pData, (char*)pCol->pData + pCol->info.bytes * (pQueryHandle->cur.rows - 1), pCol->info.bytes);

        SColumnInfoData* pCol1 = taosArrayGet(pSecQueryHandle->pColumns, i);
        assert(pCol->info.colId == pCol1->info.colId);

        memcpy((char*)pCol->pData + pCol->info.bytes, pCol1->pData, pCol1->info.bytes);
      }

      SColumnInfoData* pTSCol = taosArrayGet(pQueryHandle->pColumns, 0);

      // it is ascending order
      pQueryHandle->order    = TSDB_ORDER_DESC;
      pQueryHandle->window   = pQueryHandle->cur.win;
      pQueryHandle->cur.win  = (STimeWindow){((TSKEY*)pTSCol->pData)[0], ((TSKEY*)pTSCol->pData)[1]};
      pQueryHandle->cur.rows = 2;
      pQueryHandle->cur.mixBlock = true;

      int32_t step = -1;// one step for ascending order traverse
      for (int32_t j = 0; j < si; ++j) {
        STableCheckInfo* pCheckInfo = (STableCheckInfo*) taosArrayGet(pQueryHandle->pTableCheckInfo, j);
        pCheckInfo->lastKey = pQueryHandle->cur.win.ekey + step;
      }

      tsdbCleanupQueryHandle(pSecQueryHandle);
    }

    //disable it after retrieve data
    pQueryHandle->type = TSDB_QUERY_TYPE_EXTERNAL;
    pQueryHandle->checkFiles = false;
    return true;
  }

  if (pQueryHandle->checkFiles) {
    // check if the query range overlaps with the file data block
    bool exists = true;

    int32_t code = getDataBlocksInFiles(pQueryHandle, &exists);
    if (code != TSDB_CODE_SUCCESS) {
      pQueryHandle->activeIndex = 0;
      pQueryHandle->checkFiles = false;

      return false;
    }

    if (exists) {
      pQueryHandle->cost.checkForNextTime += (taosGetTimestampUs() - stime);
      return exists;
    }

    pQueryHandle->activeIndex = 0;
    pQueryHandle->checkFiles = false;
  }

  // TODO: opt by consider the scan order
  bool ret = doHasDataInBuffer(pQueryHandle);
  terrno = TSDB_CODE_SUCCESS;

  elapsedTime = taosGetTimestampUs() - stime;
  pQueryHandle->cost.checkForNextTime += elapsedTime;
  return ret;
}

STimeWindow changeTableGroupByLastrow(STableGroupInfo *groupList) {
  STimeWindow window = {INT64_MAX, INT64_MIN};

  int32_t totalNumOfTable = 0;

  // NOTE: starts from the buffer in case of descending timestamp order check data blocks
  size_t numOfGroups = taosArrayGetSize(groupList->pGroupList);
  for(int32_t j = 0; j < numOfGroups; ++j) {
    SArray* pGroup = taosArrayGetP(groupList->pGroupList, j);
    TSKEY   key = TSKEY_INITIAL_VAL;

    STableKeyInfo keyInfo = {0};

    size_t numOfTables = taosArrayGetSize(pGroup);
    for(int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = (STableKeyInfo*) taosArrayGet(pGroup, i);

      // if the lastKey equals to INT64_MIN, there is no data in this table
      TSKEY lastKey = ((STable*)(pKeyInfo->pTable))->lastKey;
      if (key < lastKey) {
        key = lastKey;

        keyInfo.pTable  = pKeyInfo->pTable;
        keyInfo.lastKey = key;
        pKeyInfo->lastKey = key;

        if (key < window.skey) {
          window.skey = key;
        }

        if (key > window.ekey) {
          window.ekey = key;
        }
      }
    }

    // clear current group, unref unused table
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pKeyInfo = (STableKeyInfo*)taosArrayGet(pGroup, i);

      // keyInfo.pTable may be NULL here.
      if (pKeyInfo->pTable != keyInfo.pTable) {
        tsdbUnRefTable(pKeyInfo->pTable);
      }
    }

    taosArrayClear(pGroup);

    // more than one table in each group, only one table left for each group
    if (keyInfo.pTable != NULL) {
      totalNumOfTable++;
      taosArrayPush(pGroup, &keyInfo);
    } else {
      taosArrayDestroy(pGroup);

      taosArrayRemove(groupList->pGroupList, j);
      numOfGroups -= 1;
      j -= 1;
    }
  }

  // window does not being updated, so set the original
  if (window.skey == INT64_MAX && window.ekey == INT64_MIN) {
    window = TSWINDOW_INITIALIZER;
    assert(totalNumOfTable == 0 && taosArrayGetSize(groupList->pGroupList) == 0);
  }

  groupList->numOfTables = totalNumOfTable;
  return window;
}

void tsdbRetrieveDataBlockInfo(TsdbQueryHandleT* pQueryHandle, SDataBlockInfo* pDataBlockInfo) {
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*)pQueryHandle;
  SQueryFilePos* cur = &pHandle->cur;
  STable* pTable = NULL;

  // there are data in file
  if (pHandle->cur.fid >= 0) {
    STableBlockInfo* pBlockInfo = &pHandle->pDataBlockInfo[cur->slot];
    pTable = pBlockInfo->pTableCheckInfo->pTableObj;
  } else {
    STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);
    pTable = pCheckInfo->pTableObj;
  }

  pDataBlockInfo->uid = pTable->tableId.uid;
  pDataBlockInfo->tid = pTable->tableId.tid;
  pDataBlockInfo->rows = cur->rows;
  pDataBlockInfo->window = cur->win;
  pDataBlockInfo->numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pHandle));
}

/*
 * return null for mixed data block, if not a complete file data block, the statistics value will always return NULL
 */
int32_t tsdbRetrieveDataBlockStatisInfo(TsdbQueryHandleT* pQueryHandle, SDataStatis** pBlockStatis) {
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*) pQueryHandle;

  SQueryFilePos* c = &pHandle->cur;
  if (c->mixBlock) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  STableBlockInfo* pBlockInfo = &pHandle->pDataBlockInfo[c->slot];
  assert((c->slot >= 0 && c->slot < pHandle->numOfBlocks) || ((c->slot == pHandle->numOfBlocks) && (c->slot == 0)));

  // file block with sub-blocks has no statistics data
  if (pBlockInfo->compBlock->numOfSubBlocks > 1) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int64_t stime = taosGetTimestampUs();
  tsdbLoadCompData(&pHandle->rhelper, pBlockInfo->compBlock, NULL);

  int16_t* colIds = pHandle->defaultLoadColumn->pData;

  size_t numOfCols = QH_GET_NUM_OF_COLS(pHandle);
  memset(pHandle->statis, 0, numOfCols * sizeof(SDataStatis));
  for(int32_t i = 0; i < numOfCols; ++i) {
    pHandle->statis[i].colId = colIds[i];
  }

  tsdbGetDataStatis(&pHandle->rhelper, pHandle->statis, (int)numOfCols);

  // always load the first primary timestamp column data
  SDataStatis* pPrimaryColStatis = &pHandle->statis[0];
  assert(pPrimaryColStatis->colId == PRIMARYKEY_TIMESTAMP_COL_INDEX);

  pPrimaryColStatis->numOfNull = 0;
  pPrimaryColStatis->min = pBlockInfo->compBlock->keyFirst;
  pPrimaryColStatis->max = pBlockInfo->compBlock->keyLast;

  //update the number of NULL data rows
  for(int32_t i = 1; i < numOfCols; ++i) {
    if (pHandle->statis[i].numOfNull == -1) { // set the column data are all NULL
      pHandle->statis[i].numOfNull = pBlockInfo->compBlock->numOfRows;
    }

    SColumnInfo* pColInfo = taosArrayGet(pHandle->pColumns, i);
    if (pColInfo->type == TSDB_DATA_TYPE_TIMESTAMP) {
      pHandle->statis[i].min = pBlockInfo->compBlock->keyFirst;
      pHandle->statis[i].max = pBlockInfo->compBlock->keyLast;
    }
  }

  int64_t elapsed = taosGetTimestampUs() - stime;
  pHandle->cost.statisInfoLoadTime += elapsed;

  *pBlockStatis = pHandle->statis;
  return TSDB_CODE_SUCCESS;
}

SArray* tsdbRetrieveDataBlock(TsdbQueryHandleT* pQueryHandle, SArray* pIdList) {
  /**
   * In the following two cases, the data has been loaded to SColumnInfoData.
   * 1. data is from cache, 2. data block is not completed qualified to query time range
   */
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*)pQueryHandle;

  if (pHandle->cur.fid < 0) {
    return pHandle->pColumns;
  } else {
    STableBlockInfo* pBlockInfo = &pHandle->pDataBlockInfo[pHandle->cur.slot];
    STableCheckInfo* pCheckInfo = pBlockInfo->pTableCheckInfo;

    if (pHandle->cur.mixBlock) {
      return pHandle->pColumns;
    } else {
      SDataBlockInfo binfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlockInfo->compBlock);
      assert(pHandle->realNumOfRows <= binfo.rows);

      // data block has been loaded, todo extract method
      SDataBlockLoadInfo* pBlockLoadInfo = &pHandle->dataBlockLoadInfo;

      if (pBlockLoadInfo->slot == pHandle->cur.slot && pBlockLoadInfo->fileGroup->fileId == pHandle->cur.fid &&
          pBlockLoadInfo->tid == pCheckInfo->pTableObj->tableId.tid) {
        return pHandle->pColumns;
      } else {  // only load the file block
        SCompBlock* pBlock = pBlockInfo->compBlock;
        if (doLoadFileDataBlock(pHandle, pBlock, pCheckInfo, pHandle->cur.slot) != TSDB_CODE_SUCCESS) {
          return NULL;
        }

        // todo refactor
        int32_t numOfRows = doCopyRowsFromFileBlock(pHandle, pHandle->outputCapacity, 0, 0, pBlock->numOfRows - 1);

        // if the buffer is not full in case of descending order query, move the data in the front of the buffer
        if (!ASCENDING_TRAVERSE(pHandle->order) && numOfRows < pHandle->outputCapacity) {
          int32_t emptySize = pHandle->outputCapacity - numOfRows;
          int32_t reqNumOfCols = (int32_t)taosArrayGetSize(pHandle->pColumns);

          for(int32_t i = 0; i < reqNumOfCols; ++i) {
            SColumnInfoData* pColInfo = taosArrayGet(pHandle->pColumns, i);
            memmove((char*)pColInfo->pData, (char*)pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
          }
        }

        return pHandle->pColumns;
      }
    }
  }
}

void filterPrepare(void* expr, void* param) {
  tExprNode* pExpr = (tExprNode*)expr;
  if (pExpr->_node.info != NULL) {
    return;
  }

  pExpr->_node.info = calloc(1, sizeof(tQueryInfo));

  STSchema*   pTSSchema = (STSchema*) param;
  tQueryInfo* pInfo = pExpr->_node.info;
  tVariant*   pCond = pExpr->_node.pRight->pVal;
  SSchema*    pSchema = pExpr->_node.pLeft->pSchema;

  pInfo->sch      = *pSchema;
  pInfo->optr     = pExpr->_node.optr;
  pInfo->compare  = getComparFunc(pSchema->type, pInfo->optr);
  pInfo->indexed  = pTSSchema->columns->colId == pInfo->sch.colId;

  if (pInfo->optr == TSDB_RELATION_IN) {
    pInfo->q = (char*) pCond->arr;
  } else {
    uint32_t size = pCond->nLen * TSDB_NCHAR_SIZE;
    if (size < (uint32_t)pSchema->bytes) {
      size = pSchema->bytes;
    }
    pInfo->q = calloc(1, size + TSDB_NCHAR_SIZE);   // to make sure tonchar does not cause invalid write, since the '\0' needs at least sizeof(wchar_t) space.
    tVariantDump(pCond, pInfo->q, pSchema->type, true);
  }
}

static int32_t tableGroupComparFn(const void *p1, const void *p2, const void *param) {
  STableGroupSupporter* pTableGroupSupp = (STableGroupSupporter*) param;
  STable* pTable1 = ((STableKeyInfo*) p1)->pTable;
  STable* pTable2 = ((STableKeyInfo*) p2)->pTable;

  for (int32_t i = 0; i < pTableGroupSupp->numOfCols; ++i) {
    SColIndex* pColIndex = &pTableGroupSupp->pCols[i];
    int32_t colIndex = pColIndex->colIndex;

    assert(colIndex >= TSDB_TBNAME_COLUMN_INDEX);

    char *  f1 = NULL;
    char *  f2 = NULL;
    int32_t type = 0;
    int32_t bytes = 0;

    if (colIndex == TSDB_TBNAME_COLUMN_INDEX) {
      f1 = (char*) TABLE_NAME(pTable1);
      f2 = (char*) TABLE_NAME(pTable2);
      type = TSDB_DATA_TYPE_BINARY;
      bytes = tGetTableNameColumnSchema().bytes;
    } else {
      STColumn* pCol = schemaColAt(pTableGroupSupp->pTagSchema, colIndex);
      bytes = pCol->bytes;
      type = pCol->type;
      f1 = tdGetKVRowValOfCol(pTable1->tagVal, pCol->colId);
      f2 = tdGetKVRowValOfCol(pTable2->tagVal, pCol->colId);
    }

    // this tags value may be NULL
    if (f1 == NULL && f2 == NULL) {
      continue;
    }

    if (f1 == NULL) {
      return -1;
    }

    if (f2 == NULL) {
      return 1;
    }

    int32_t ret = doCompare(f1, f2, type, bytes);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

static int tsdbCheckInfoCompar(const void* key1, const void* key2) {
  if (((STableCheckInfo*)key1)->tableId.tid < ((STableCheckInfo*)key2)->tableId.tid) {
    return -1;
  } else if (((STableCheckInfo*)key1)->tableId.tid > ((STableCheckInfo*)key2)->tableId.tid) {
    return 1;
  } else {
    ASSERT(false);
    return 0;
  }
}

void createTableGroupImpl(SArray* pGroups, SArray* pTableList, size_t numOfTables, TSKEY skey,
                          STableGroupSupporter* pSupp, __ext_compar_fn_t compareFn) {
  STable* pTable = taosArrayGetP(pTableList, 0);

  SArray* g = taosArrayInit(16, sizeof(STableKeyInfo));

  STableKeyInfo info = {.pTable = pTable, .lastKey = skey};
  taosArrayPush(g, &info);
  tsdbRefTable(pTable);

  for (int32_t i = 1; i < numOfTables; ++i) {
    STable** prev = taosArrayGet(pTableList, i - 1);
    STable** p = taosArrayGet(pTableList, i);

    int32_t ret = compareFn(prev, p, pSupp);
    assert(ret == 0 || ret == -1);

    tsdbRefTable(*p);
    assert((*p)->type == TSDB_CHILD_TABLE);

    if (ret == 0) {
      STableKeyInfo info1 = {.pTable = *p, .lastKey = skey};
      taosArrayPush(g, &info1);
    } else {
      taosArrayPush(pGroups, &g);  // current group is ended, start a new group
      g = taosArrayInit(16, sizeof(STableKeyInfo));

      STableKeyInfo info1 = {.pTable = *p, .lastKey = skey};
      taosArrayPush(g, &info1);
    }
  }

  taosArrayPush(pGroups, &g);
}

SArray* createTableGroup(SArray* pTableList, STSchema* pTagSchema, SColIndex* pCols, int32_t numOfOrderCols, TSKEY skey) {
  assert(pTableList != NULL);
  SArray* pTableGroup = taosArrayInit(1, POINTER_BYTES);

  size_t size = taosArrayGetSize(pTableList);
  if (size == 0) {
    tsdbDebug("no qualified tables");
    return pTableGroup;
  }

  if (numOfOrderCols == 0 || size == 1) { // no group by tags clause or only one table
    SArray* sa = taosArrayInit(size, sizeof(STableKeyInfo));
    if (sa == NULL) {
      taosArrayDestroy(pTableGroup);
      return NULL;
    }

    for(int32_t i = 0; i < size; ++i) {
      STableKeyInfo *pKeyInfo = taosArrayGet(pTableList, i);
      assert(((STable*)pKeyInfo->pTable)->type == TSDB_CHILD_TABLE);

      tsdbRefTable(pKeyInfo->pTable);

      STableKeyInfo info = {.pTable = pKeyInfo->pTable, .lastKey = skey};
      taosArrayPush(sa, &info);
    }

    taosArrayPush(pTableGroup, &sa);
    tsdbDebug("all %" PRIzu " tables belong to one group", size);
  } else {
    STableGroupSupporter sup = {0};
    sup.numOfCols = numOfOrderCols;
    sup.pTagSchema = pTagSchema;
    sup.pCols = pCols;

    taosqsort(pTableList->pData, size, sizeof(STableKeyInfo), &sup, tableGroupComparFn);
    createTableGroupImpl(pTableGroup, pTableList, size, skey, &sup, tableGroupComparFn);
  }

  return pTableGroup;
}

static bool indexedNodeFilterFp(const void* pNode, void* param) {
  tQueryInfo* pInfo = (tQueryInfo*) param;

  STable* pTable = *(STable**)(SL_GET_NODE_DATA((SSkipListNode*)pNode));

  char*  val = NULL;

  if (pInfo->sch.colId == TSDB_TBNAME_COLUMN_INDEX) {
    val = (char*) TABLE_NAME(pTable);
  } else {
    val = tdGetKVRowValOfCol(pTable->tagVal, pInfo->sch.colId);
  }

  if (pInfo->optr == TSDB_RELATION_ISNULL || pInfo->optr == TSDB_RELATION_NOTNULL) {
    if (pInfo->optr == TSDB_RELATION_ISNULL) {
      return (val == NULL) || isNull(val, pInfo->sch.type);
    } else if (pInfo->optr == TSDB_RELATION_NOTNULL) {
      return (val != NULL) && (!isNull(val, pInfo->sch.type));
    }
  }

  int32_t ret = 0;
  if (val == NULL) { //the val is possible to be null, so check it out carefully
    ret = -1; // val is missing in table tags value pairs
  } else {
    ret = pInfo->compare(val, pInfo->q);
  }

  switch (pInfo->optr) {
    case TSDB_RELATION_EQUAL: {
      return ret == 0;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      return ret != 0;
    }
    case TSDB_RELATION_GREATER_EQUAL: {
      return ret >= 0;
    }
    case TSDB_RELATION_GREATER: {
      return ret > 0;
    }
    case TSDB_RELATION_LESS_EQUAL: {
      return ret <= 0;
    }
    case TSDB_RELATION_LESS: {
      return ret < 0;
    }
    case TSDB_RELATION_LIKE: {
      return ret == 0;
    }
    case TSDB_RELATION_IN: {
      return ret == 1;
    }

    default:
      assert(false);
  }

  return true;
}

static int32_t doQueryTableList(STable* pSTable, SArray* pRes, tExprNode* pExpr) {
  // query according to the expression tree
  SExprTraverseSupp supp = {
      .nodeFilterFn = (__result_filter_fn_t) indexedNodeFilterFp,
      .setupInfoFn = filterPrepare,
      .pExtInfo = pSTable->tagSchema,
      };

  tExprTreeTraverse(pExpr, pSTable->pIndex, pRes, &supp);
  tExprTreeDestroy(&pExpr, destroyHelper);
  return TSDB_CODE_SUCCESS;
}

int32_t tsdbQuerySTableByTagCond(TSDB_REPO_T* tsdb, uint64_t uid, TSKEY skey, const char* pTagCond, size_t len,
                                 int16_t tagNameRelType, const char* tbnameCond, STableGroupInfo* pGroupInfo,
                                 SColIndex* pColIndex, int32_t numOfCols) {
  if (tsdbRLockRepoMeta(tsdb) < 0) goto _error;

  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  if (pTable == NULL) {
    tsdbError("%p failed to get stable, uid:%" PRIu64, tsdb, uid);
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    tsdbUnlockRepoMeta(tsdb);

    goto _error;
  }

  if (pTable->type != TSDB_SUPER_TABLE) {
    tsdbError("%p query normal tag not allowed, uid:%" PRIu64 ", tid:%d, name:%s", tsdb, uid, pTable->tableId.tid,
        pTable->name->data);
    terrno = TSDB_CODE_COM_OPS_NOT_SUPPORT; //basically, this error is caused by invalid sql issued by client

    tsdbUnlockRepoMeta(tsdb);
    goto _error;
  }

  //NOTE: not add ref count for super table
  SArray* res = taosArrayInit(8, sizeof(STableKeyInfo));
  STSchema* pTagSchema = tsdbGetTableTagSchema(pTable);

  // no tags and tbname condition, all child tables of this stable are involved
  if (tbnameCond == NULL && (pTagCond == NULL || len == 0)) {
    int32_t ret = getAllTableList(pTable, res);
    if (ret != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepoMeta(tsdb);
      goto _error;
    }

    pGroupInfo->numOfTables = taosArrayGetSize(res);
    pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols, skey);

    tsdbDebug("%p no table name/tag condition, all tables belong to one group, numOfTables:%" PRIzu "", tsdb, pGroupInfo->numOfTables);
    taosArrayDestroy(res);

    if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;
    return ret;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  tExprNode* expr = NULL;

  TRY(TSDB_MAX_TAG_CONDITIONS) {
    expr = exprTreeFromTableName(tbnameCond);
    if (expr == NULL) {
      expr = exprTreeFromBinary(pTagCond, len);
    } else {
      CLEANUP_PUSH_VOID_PTR_PTR(true, tExprNodeDestroy, expr, NULL);
      tExprNode* tagExpr = exprTreeFromBinary(pTagCond, len);
      if (tagExpr != NULL) {
        CLEANUP_PUSH_VOID_PTR_PTR(true, tExprNodeDestroy, tagExpr, NULL);
        tExprNode* tbnameExpr = expr;
        expr = calloc(1, sizeof(tExprNode));
        if (expr == NULL) {
          THROW( TSDB_CODE_TDB_OUT_OF_MEMORY );
        }
        expr->nodeType = TSQL_NODE_EXPR;
        expr->_node.optr = (uint8_t)tagNameRelType;
        expr->_node.pLeft = tagExpr;
        expr->_node.pRight = tbnameExpr;
      }
    }
    CLEANUP_EXECUTE();

  } CATCH( code ) {
    CLEANUP_EXECUTE();
    terrno = code;
    tsdbUnlockRepoMeta(tsdb);     // unlock tsdb in any cases

    goto _error;
    // TODO: more error handling
  } END_TRY

  doQueryTableList(pTable, res, expr);
  pGroupInfo->numOfTables = taosArrayGetSize(res);
  pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols, skey);

  tsdbDebug("%p stable tid:%d, uid:%"PRIu64" query, numOfTables:%" PRIzu ", belong to %" PRIzu " groups", tsdb, pTable->tableId.tid,
      pTable->tableId.uid, pGroupInfo->numOfTables, taosArrayGetSize(pGroupInfo->pGroupList));

  taosArrayDestroy(res);

  if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;
  return ret;

  _error:
  return terrno;
}

int32_t tsdbGetOneTableGroup(TSDB_REPO_T* tsdb, uint64_t uid, TSKEY startKey, STableGroupInfo* pGroupInfo) {
  if (tsdbRLockRepoMeta(tsdb) < 0) goto _error;

  STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), uid);
  if (pTable == NULL) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    tsdbUnlockRepoMeta(tsdb);
    goto _error;
  }

  assert(pTable->type == TSDB_CHILD_TABLE || pTable->type == TSDB_NORMAL_TABLE || pTable->type == TSDB_STREAM_TABLE);
  tsdbRefTable(pTable);
  if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;

  pGroupInfo->numOfTables = 1;
  pGroupInfo->pGroupList = taosArrayInit(1, POINTER_BYTES);

  SArray* group = taosArrayInit(1, sizeof(STableKeyInfo));

  STableKeyInfo info = {.pTable = pTable, .lastKey = startKey};
  taosArrayPush(group, &info);

  taosArrayPush(pGroupInfo->pGroupList, &group);

  return TSDB_CODE_SUCCESS;

  _error:
  return terrno;
}

int32_t tsdbGetTableGroupFromIdList(TSDB_REPO_T* tsdb, SArray* pTableIdList, STableGroupInfo* pGroupInfo) {
  if (tsdbRLockRepoMeta(tsdb) < 0) {
    return terrno;
  }

  assert(pTableIdList != NULL);
  size_t size = taosArrayGetSize(pTableIdList);
  pGroupInfo->pGroupList = taosArrayInit(1, POINTER_BYTES);
  SArray* group = taosArrayInit(1, sizeof(STableKeyInfo));

  for(int32_t i = 0; i < size; ++i) {
    STableIdInfo *id = taosArrayGet(pTableIdList, i);

    STable* pTable = tsdbGetTableByUid(tsdbGetMeta(tsdb), id->uid);
    if (pTable == NULL) {
      tsdbWarn("table uid:%"PRIu64", tid:%d has been drop already", id->uid, id->tid);
      continue;
    }

    if (pTable->type == TSDB_SUPER_TABLE) {
      tsdbError("direct query on super tale is not allowed, table uid:%"PRIu64", tid:%d", id->uid, id->tid);
      terrno = TSDB_CODE_QRY_INVALID_MSG;
    }

    tsdbRefTable(pTable);

    STableKeyInfo info = {.pTable = pTable, .lastKey = id->key};
    taosArrayPush(group, &info);
  }

  if (tsdbUnlockRepoMeta(tsdb) < 0) {
    taosArrayDestroy(group);
    return terrno;
  }

  pGroupInfo->numOfTables = taosArrayGetSize(group);
  if (pGroupInfo->numOfTables > 0) {
    taosArrayPush(pGroupInfo->pGroupList, &group);
  } else {
    taosArrayDestroy(group);
  }

  return TSDB_CODE_SUCCESS;
}

void tsdbCleanupQueryHandle(TsdbQueryHandleT queryHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*)queryHandle;
  if (pQueryHandle == NULL) {
    return;
  }
  
  if (pQueryHandle->pTableCheckInfo != NULL) {
    size_t size = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
    for (int32_t i = 0; i < size; ++i) {
      STableCheckInfo* pTableCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
      destroyTableMemIterator(pTableCheckInfo);

      taosTFree(pTableCheckInfo->pCompInfo);
    }
    taosArrayDestroy(pQueryHandle->pTableCheckInfo);
  }

  if (pQueryHandle->pColumns != NULL) {
    size_t cols = taosArrayGetSize(pQueryHandle->pColumns);
    for (int32_t i = 0; i < cols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      taosTFree(pColInfo->pData);
    }
    taosArrayDestroy(pQueryHandle->pColumns);
  }

  taosArrayDestroy(pQueryHandle->defaultLoadColumn);
  taosTFree(pQueryHandle->pDataBlockInfo);
  taosTFree(pQueryHandle->statis);

  // todo check error
  tsdbMayUnTakeMemSnapshot(pQueryHandle);

  tsdbDestroyHelper(&pQueryHandle->rhelper);

  tdFreeDataCols(pQueryHandle->pDataCols);
  pQueryHandle->pDataCols = NULL;

  SIOCostSummary* pCost = &pQueryHandle->cost;
  tsdbDebug("%p :io-cost summary: statis-info:%"PRId64" us, datablock:%" PRId64" us, check data:%"PRId64" us, %p",
      pQueryHandle, pCost->statisInfoLoadTime, pCost->blockLoadTime, pCost->checkForNextTime, pQueryHandle->qinfo);

  taosTFree(pQueryHandle);
}

void tsdbDestroyTableGroup(STableGroupInfo *pGroupList) {
  assert(pGroupList != NULL);

  size_t numOfGroup = taosArrayGetSize(pGroupList->pGroupList);

  for(int32_t i = 0; i < numOfGroup; ++i) {
    SArray* p = taosArrayGetP(pGroupList->pGroupList, i);

    size_t numOfTables = taosArrayGetSize(p);
    for(int32_t j = 0; j < numOfTables; ++j) {
      STable* pTable = taosArrayGetP(p, j);
      assert(pTable != NULL);

      tsdbUnRefTable(pTable);
    }

    taosArrayDestroy(p);
  }

  taosArrayDestroy(pGroupList->pGroupList);
  pGroupList->numOfTables = 0;
}
