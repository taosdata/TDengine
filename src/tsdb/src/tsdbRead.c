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
#include "tdataformat.h"
#include "tskiplist.h"
#include "tulog.h"
#include "talgo.h"
#include "tcompare.h"
#include "exception.h"

#include "taosdef.h"
#include "tlosertree.h"
#include "tsdbint.h"
#include "texpr.h"
#include "qFilter.h"
#include "cJSON.h"

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
};

enum {
  TSDB_CACHED_TYPE_NONE    = 0,
  TSDB_CACHED_TYPE_LASTROW = 1,
  TSDB_CACHED_TYPE_LAST    = 2,
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
  SDFileSet*  fileGroup;
  int32_t     slot;
  int32_t     tid;
  SArray*     pLoadedCols;
} SDataBlockLoadInfo;

typedef struct SLoadCompBlockInfo {
  int32_t tid; /* table tid */
  int32_t fileId;
} SLoadCompBlockInfo;

enum {
  CHECKINFO_CHOSEN_MEM  = 0,
  CHECKINFO_CHOSEN_IMEM = 1,
  CHECKINFO_CHOSEN_BOTH = 2    //for update=2(merge case)
};


typedef struct STableCheckInfo {
  STableId      tableId;
  TSKEY         lastKey;
  STable*       pTableObj;
  SBlockInfo*   pCompInfo;
  int32_t       compSize;
  int32_t       numOfBlocks:29; // number of qualified data blocks not the original blocks
  uint8_t        chosen:2;       // indicate which iterator should move forward
  bool          initBuf;        // whether to initialize the in-memory skip list iterator or not
  SSkipListIterator* iter;      // mem buffer skip list iterator
  SSkipListIterator* iiter;     // imem buffer skip list iterator
} STableCheckInfo;

typedef struct STableBlockInfo {
  SBlock          *compBlock;
  STableCheckInfo *pTableCheckInfo;
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
  int64_t headFileLoad;
  int64_t headFileLoadTime;
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
  int8_t         cachelastrow;     // check if last row cached
  bool           loadExternalRow;  // load time window external data rows
  bool           currentLoadExternalRows; // current load external rows
  int32_t        loadType;         // block load type
  uint64_t       qId;              // query info handle, for debug purpose
  int32_t        type;             // query type: retrieve all data blocks, 2. retrieve only last row, 3. retrieve direct prev|next rows
  SDFileSet*     pFileGroup;
  SFSIter        fileIter;
  SReadH         rhelper;
  STableBlockInfo* pDataBlockInfo;
  SDataCols     *pDataCols;        // in order to hold current file data block
  int32_t        allocSize;        // allocated data block size
  SMemRef       *pMemRef;
  SArray        *defaultLoadColumn;// default load column
  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQueryAttr */

  SArray        *prev;             // previous row which is before than time window
  SArray        *next;             // next row which is after the query time window
  SIOCostSummary cost;
} STsdbQueryHandle;

typedef struct STableGroupSupporter {
  int32_t    numOfCols;
  SColIndex* pCols;
  STSchema*  pTagSchema;
} STableGroupSupporter;

static STimeWindow updateLastrowForEachGroup(STableGroupInfo *groupList);
static int32_t checkForCachedLastRow(STsdbQueryHandle* pQueryHandle, STableGroupInfo *groupList);
static int32_t checkForCachedLast(STsdbQueryHandle* pQueryHandle);
static int32_t lazyLoadCacheLast(STsdbQueryHandle* pQueryHandle);
static int32_t tsdbGetCachedLastRow(STable* pTable, SMemRow* pRes, TSKEY* lastKey);

static void    changeQueryHandleForInterpQuery(TsdbQueryHandleT pHandle);
static void    doMergeTwoLevelData(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SBlock* pBlock);
static int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);
static int32_t tsdbReadRowsFromCache(STableCheckInfo* pCheckInfo, TSKEY maxKey, int maxRowsToRead, STimeWindow* win, STsdbQueryHandle* pQueryHandle);
static int32_t tsdbCheckInfoCompar(const void* key1, const void* key2);
static int32_t doGetExternalRow(STsdbQueryHandle* pQueryHandle, int16_t type, SMemRef* pMemRef);
static void*   doFreeColumnInfoData(SArray* pColumnInfoData);
static void*   destroyTableCheckInfo(SArray* pTableCheckInfo);
static bool    tsdbGetExternalRow(TsdbQueryHandleT pHandle);
static int32_t tsdbQueryTableList(STable* pTable, SArray* pRes, void* filterInfo);

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

static void tsdbMayTakeMemSnapshot(STsdbQueryHandle* pQueryHandle, SArray* psTable) {
  assert(pQueryHandle != NULL && pQueryHandle->pMemRef != NULL);

  SMemRef* pMemRef = pQueryHandle->pMemRef;
  if (pQueryHandle->pMemRef->ref++ == 0) {
    tsdbTakeMemSnapshot(pQueryHandle->pTsdb, &(pMemRef->snapshot), psTable);
  }

  taosArrayDestroy(psTable);
}

static void tsdbMayUnTakeMemSnapshot(STsdbQueryHandle* pQueryHandle) {
  assert(pQueryHandle != NULL);
  SMemRef* pMemRef = pQueryHandle->pMemRef;
  if (pMemRef == NULL) { // it has been freed
    return;
  }

  if (--pMemRef->ref == 0) {
    tsdbUnTakeMemSnapShot(pQueryHandle->pTsdb, &(pMemRef->snapshot));
  }

  pQueryHandle->pMemRef = NULL;
}

int64_t tsdbGetNumOfRowsInMemTable(TsdbQueryHandleT* pHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;

  int64_t rows = 0;
  SMemRef* pMemRef = pQueryHandle->pMemRef;
  if (pMemRef == NULL) { return rows; }

  STableData* pMem  = NULL;
  STableData* pIMem = NULL;

  SMemTable* pMemT = pMemRef->snapshot.mem;
  SMemTable* pIMemT = pMemRef->snapshot.imem;

  size_t size = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);

    if (pMemT && pCheckInfo->tableId.tid < pMemT->maxTables) {
      pMem = pMemT->tData[pCheckInfo->tableId.tid];
      rows += (pMem && pMem->uid == pCheckInfo->tableId.uid) ? pMem->numOfRows : 0;
    }
    if (pIMemT && pCheckInfo->tableId.tid < pIMemT->maxTables) {
      pIMem = pIMemT->tData[pCheckInfo->tableId.tid];
      rows += (pIMem && pIMem->uid == pCheckInfo->tableId.uid) ? pIMem->numOfRows : 0;
    }
  }
  return rows;
}

static SArray* createCheckInfoFromTableGroup(STsdbQueryHandle* pQueryHandle, STableGroupInfo* pGroupList, STsdbMeta* pMeta, SArray** psTable) {
  size_t sizeOfGroup = taosArrayGetSize(pGroupList->pGroupList);
  assert(sizeOfGroup >= 1 && pMeta != NULL);

  // allocate buffer in order to load data blocks from file
  SArray* pTableCheckInfo = taosArrayInit(pGroupList->numOfTables, sizeof(STableCheckInfo));
  if (pTableCheckInfo == NULL) {
    return NULL;
  }

  SArray* pTable = taosArrayInit(4, sizeof(STable*));
  if (pTable == NULL) {
    taosArrayDestroy(pTableCheckInfo);
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
      assert(info.pTableObj != NULL && (info.pTableObj->type == TSDB_NORMAL_TABLE ||
                                        info.pTableObj->type == TSDB_CHILD_TABLE || info.pTableObj->type == TSDB_STREAM_TABLE));

      info.tableId.tid = info.pTableObj->tableId.tid;
      info.tableId.uid = info.pTableObj->tableId.uid;

      if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
        if (info.lastKey == INT64_MIN || info.lastKey < pQueryHandle->window.skey) {
          info.lastKey = pQueryHandle->window.skey;
        }

        assert(info.lastKey >= pQueryHandle->window.skey && info.lastKey <= pQueryHandle->window.ekey);
      } else {
        assert(info.lastKey >= pQueryHandle->window.ekey && info.lastKey <= pQueryHandle->window.skey);
      }

      taosArrayPush(pTableCheckInfo, &info);
      tsdbDebug("%p check table uid:%"PRId64", tid:%d from lastKey:%"PRId64" 0x%"PRIx64, pQueryHandle, info.tableId.uid,
                info.tableId.tid, info.lastKey, pQueryHandle->qId);
    }
  }

  taosArraySort(pTableCheckInfo, tsdbCheckInfoCompar);

  size_t gsize = taosArrayGetSize(pTableCheckInfo);

  for (int32_t i = 0; i < gsize; ++i) {
    STableCheckInfo* pInfo = (STableCheckInfo*) taosArrayGet(pTableCheckInfo, i);
    taosArrayPush(pTable, &pInfo->pTableObj);
  }

  *psTable = pTable;
  return pTableCheckInfo;
}

static void resetCheckInfo(STsdbQueryHandle* pQueryHandle) {
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  assert(numOfTables >= 1);

  // todo apply the lastkey of table check to avoid to load header file
  for (int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = (STableCheckInfo*) taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    pCheckInfo->lastKey = pQueryHandle->window.skey;
    pCheckInfo->iter    = tSkipListDestroyIter(pCheckInfo->iter);
    pCheckInfo->iiter   = tSkipListDestroyIter(pCheckInfo->iiter);
    pCheckInfo->initBuf = false;

    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      assert(pCheckInfo->lastKey >= pQueryHandle->window.skey);
    } else {
      assert(pCheckInfo->lastKey <= pQueryHandle->window.skey);
    }
  }
}

// only one table, not need to sort again
static SArray* createCheckInfoFromCheckInfo(STableCheckInfo* pCheckInfo, TSKEY skey, SArray** psTable) {
  SArray* pNew = taosArrayInit(1, sizeof(STableCheckInfo));
  SArray* pTable = taosArrayInit(1, sizeof(STable*));

  STableCheckInfo info = { .lastKey = skey, .pTableObj = pCheckInfo->pTableObj};

  info.tableId = pCheckInfo->tableId;
  taosArrayPush(pNew, &info);
  taosArrayPush(pTable, &pCheckInfo->pTableObj);

  *psTable = pTable;
  return pNew;
}

static bool emptyQueryTimewindow(STsdbQueryHandle* pQueryHandle) {
  assert(pQueryHandle != NULL);

  STimeWindow* w = &pQueryHandle->window;
  bool asc = ASCENDING_TRAVERSE(pQueryHandle->order);

  return ((asc && w->skey > w->ekey) || (!asc && w->ekey > w->skey));
}

// Update the query time window according to the data time to live(TTL) information, in order to avoid to return
// the expired data to client, even it is queried already.
static int64_t getEarliestValidTimestamp(STsdbRepo* pTsdb) {
  STsdbCfg* pCfg = &pTsdb->config;

  int64_t now = taosGetTimestamp(pCfg->precision);
  return now - (tsTickPerDay[pCfg->precision] * pCfg->keep) + 1;  // needs to add one tick
}

static void setQueryTimewindow(STsdbQueryHandle* pQueryHandle, STsdbQueryCond* pCond) {
  pQueryHandle->window = pCond->twindow;

  bool    updateTs = false;
  int64_t startTs = getEarliestValidTimestamp(pQueryHandle->pTsdb);
  if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
    if (startTs > pQueryHandle->window.skey) {
      pQueryHandle->window.skey = startTs;
      pCond->twindow.skey = startTs;
      updateTs = true;
    }
  } else {
    if (startTs > pQueryHandle->window.ekey) {
      pQueryHandle->window.ekey = startTs;
      pCond->twindow.ekey = startTs;
      updateTs = true;
    }
  }

  if (updateTs) {
    tsdbDebug("%p update the query time window, old:%" PRId64 " - %" PRId64 ", new:%" PRId64 " - %" PRId64
              ", 0x%" PRIx64, pQueryHandle, pCond->twindow.skey, pCond->twindow.ekey, pQueryHandle->window.skey,
              pQueryHandle->window.ekey, pQueryHandle->qId);
  }
}

static STsdbQueryHandle* tsdbQueryTablesImpl(STsdbRepo* tsdb, STsdbQueryCond* pCond, uint64_t qId, SMemRef* pMemRef) {
  STsdbQueryHandle* pQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
  if (pQueryHandle == NULL) {
    goto _end;
  }

  pQueryHandle->order       = pCond->order;
  pQueryHandle->pTsdb       = tsdb;
  pQueryHandle->type        = TSDB_QUERY_TYPE_ALL;
  pQueryHandle->cur.fid     = INT32_MIN;
  pQueryHandle->cur.win     = TSWINDOW_INITIALIZER;
  pQueryHandle->checkFiles  = true;
  pQueryHandle->activeIndex = 0;   // current active table index
  pQueryHandle->qId         = qId;
  pQueryHandle->allocSize   = 0;
  pQueryHandle->locateStart = false;
  pQueryHandle->pMemRef     = pMemRef;
  pQueryHandle->loadType    = pCond->type;

  pQueryHandle->outputCapacity  = ((STsdbRepo*)tsdb)->config.maxRowsPerFileBlock;
  pQueryHandle->loadExternalRow = pCond->loadExternalRows;
  pQueryHandle->currentLoadExternalRows = pCond->loadExternalRows;

  if (tsdbInitReadH(&pQueryHandle->rhelper, (STsdbRepo*)tsdb) != 0) {
    goto _end;
  }

  assert(pCond != NULL && pMemRef != NULL);
  setQueryTimewindow(pQueryHandle, pCond);

  if (pCond->numOfCols > 0) {
    // allocate buffer in order to load data blocks from file
    pQueryHandle->statis = calloc(pCond->numOfCols, sizeof(SDataStatis));
    if (pQueryHandle->statis == NULL) {
      goto _end;
    }

    // todo: use list instead of array?
    pQueryHandle->pColumns = taosArrayInit(pCond->numOfCols, sizeof(SColumnInfoData));
    if (pQueryHandle->pColumns == NULL) {
      goto _end;
    }

    for (int32_t i = 0; i < pCond->numOfCols; ++i) {
      SColumnInfoData colInfo = {{0}, 0};

      colInfo.info = pCond->colList[i];
      colInfo.pData = calloc(1, EXTRA_BYTES + pQueryHandle->outputCapacity * pCond->colList[i].bytes);
      if (colInfo.pData == NULL) {
        goto _end;
      }

      taosArrayPush(pQueryHandle->pColumns, &colInfo);
      pQueryHandle->statis[i].colId = colInfo.info.colId;
    }

    pQueryHandle->defaultLoadColumn = getDefaultLoadColumns(pQueryHandle, true);
  }

  STsdbMeta* pMeta = tsdbGetMeta(tsdb);
  assert(pMeta != NULL);

  pQueryHandle->pDataCols = tdNewDataCols(pMeta->maxCols, pQueryHandle->pTsdb->config.maxRowsPerFileBlock);
  if (pQueryHandle->pDataCols == NULL) {
    tsdbError("%p failed to malloc buf for pDataCols, %"PRIu64, pQueryHandle, pQueryHandle->qId);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _end;
  }

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  return (TsdbQueryHandleT) pQueryHandle;

  _end:
  tsdbCleanupQueryHandle(pQueryHandle);
  terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  return NULL;
}

TsdbQueryHandleT* tsdbQueryTables(STsdbRepo* tsdb, STsdbQueryCond* pCond, STableGroupInfo* groupList, uint64_t qId, SMemRef* pRef) {
  STsdbQueryHandle* pQueryHandle = tsdbQueryTablesImpl(tsdb, pCond, qId, pRef);
  if (pQueryHandle == NULL) {
    return NULL;
  }

  if (emptyQueryTimewindow(pQueryHandle)) {
    return (TsdbQueryHandleT*) pQueryHandle;
  }

  STsdbMeta* pMeta = tsdbGetMeta(tsdb);
  assert(pMeta != NULL);

  SArray* psTable = NULL;

  // todo apply the lastkey of table check to avoid to load header file
  pQueryHandle->pTableCheckInfo = createCheckInfoFromTableGroup(pQueryHandle, groupList, pMeta, &psTable);
  if (pQueryHandle->pTableCheckInfo == NULL) {
    tsdbCleanupQueryHandle(pQueryHandle);
    taosArrayDestroy(psTable);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  tsdbMayTakeMemSnapshot(pQueryHandle, psTable);

  tsdbDebug("%p total numOfTable:%" PRIzu " in query, 0x%"PRIx64, pQueryHandle, taosArrayGetSize(pQueryHandle->pTableCheckInfo), pQueryHandle->qId);
  return (TsdbQueryHandleT) pQueryHandle;
}

void tsdbResetQueryHandle(TsdbQueryHandleT queryHandle, STsdbQueryCond *pCond) {
  STsdbQueryHandle* pQueryHandle = queryHandle;

  if (emptyQueryTimewindow(pQueryHandle)) {
    if (pCond->order != pQueryHandle->order) {
      pQueryHandle->order = pCond->order;
      SWAP(pQueryHandle->window.skey, pQueryHandle->window.ekey, int64_t);
    }

    return;
  }

  pQueryHandle->order       = pCond->order;
  pQueryHandle->window      = pCond->twindow;
  pQueryHandle->type        = TSDB_QUERY_TYPE_ALL;
  pQueryHandle->cur.fid     = -1;
  pQueryHandle->cur.win     = TSWINDOW_INITIALIZER;
  pQueryHandle->checkFiles  = true;
  pQueryHandle->activeIndex = 0;   // current active table index
  pQueryHandle->locateStart = false;
  pQueryHandle->loadExternalRow = pCond->loadExternalRows;

  if (ASCENDING_TRAVERSE(pCond->order)) {
    assert(pQueryHandle->window.skey <= pQueryHandle->window.ekey);
  } else {
    assert(pQueryHandle->window.skey >= pQueryHandle->window.ekey);
  }

  // allocate buffer in order to load data blocks from file
  memset(pQueryHandle->statis, 0, sizeof(SDataStatis));

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  resetCheckInfo(pQueryHandle);
}

void tsdbResetQueryHandleForNewTable(TsdbQueryHandleT queryHandle, STsdbQueryCond *pCond, STableGroupInfo* groupList) {
  STsdbQueryHandle* pQueryHandle = queryHandle;

  pQueryHandle->order       = pCond->order;
  pQueryHandle->window      = pCond->twindow;
  pQueryHandle->type        = TSDB_QUERY_TYPE_ALL;
  pQueryHandle->cur.fid     = -1;
  pQueryHandle->cur.win     = TSWINDOW_INITIALIZER;
  pQueryHandle->checkFiles  = true;
  pQueryHandle->activeIndex = 0;   // current active table index
  pQueryHandle->locateStart = false;
  pQueryHandle->loadExternalRow = pCond->loadExternalRows;

  if (ASCENDING_TRAVERSE(pCond->order)) {
    assert(pQueryHandle->window.skey <= pQueryHandle->window.ekey);
  } else {
    assert(pQueryHandle->window.skey >= pQueryHandle->window.ekey);
  }

  // allocate buffer in order to load data blocks from file
  memset(pQueryHandle->statis, 0, sizeof(SDataStatis));

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  SArray* pTable = NULL;
  STsdbMeta* pMeta = tsdbGetMeta(pQueryHandle->pTsdb);

  pQueryHandle->pTableCheckInfo = destroyTableCheckInfo(pQueryHandle->pTableCheckInfo);

  pQueryHandle->pTableCheckInfo = createCheckInfoFromTableGroup(pQueryHandle, groupList, pMeta, &pTable);
  if (pQueryHandle->pTableCheckInfo == NULL) {
    tsdbCleanupQueryHandle(pQueryHandle);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  pQueryHandle->prev = doFreeColumnInfoData(pQueryHandle->prev);
  pQueryHandle->next = doFreeColumnInfoData(pQueryHandle->next);
}

static int32_t lazyLoadCacheLast(STsdbQueryHandle* pQueryHandle) {
  STsdbRepo* pRepo = pQueryHandle->pTsdb;

  size_t  numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  int32_t code = 0;
  for (size_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    STable*          pTable = pCheckInfo->pTableObj;
    if (pTable->cacheLastConfigVersion == pRepo->cacheLastConfigVersion) {
      continue;
    }
    code = tsdbLoadLastCache(pRepo, pTable);
    if (code != 0) {
      tsdbError("%p uid:%" PRId64 ", tid:%d, failed to load last cache since %s", pQueryHandle, pTable->tableId.uid,
                pTable->tableId.tid, tstrerror(terrno));
      break;
    }
  }

  return code;
}

TsdbQueryHandleT tsdbQueryLastRow(STsdbRepo *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, uint64_t qId, SMemRef* pMemRef) {
  pCond->twindow = updateLastrowForEachGroup(groupList);

  // no qualified table
  if (groupList->numOfTables == 0) {
    return NULL;
  }

  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qId, pMemRef);
  if (pQueryHandle == NULL) {
    return NULL;
  }

  lazyLoadCacheLast(pQueryHandle);

  int32_t code = checkForCachedLastRow(pQueryHandle, groupList);
  if (code != TSDB_CODE_SUCCESS) { // set the numOfTables to be 0
    terrno = code;
    return NULL;
  }

  assert(pCond->order == TSDB_ORDER_ASC && pCond->twindow.skey <= pCond->twindow.ekey);
  if (pQueryHandle->cachelastrow) {
    pQueryHandle->type = TSDB_QUERY_TYPE_LAST;
  }
  
  return pQueryHandle;
}

TsdbQueryHandleT tsdbQueryCacheLast(STsdbRepo *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, uint64_t qId, SMemRef* pMemRef) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qId, pMemRef);
  if (pQueryHandle == NULL) {
    return NULL;
  }

  lazyLoadCacheLast(pQueryHandle);

  int32_t code = checkForCachedLast(pQueryHandle);
  if (code != TSDB_CODE_SUCCESS) { // set the numOfTables to be 0
    terrno = code;
    return NULL;
  }

  if (pQueryHandle->cachelastrow) {
    pQueryHandle->type = TSDB_QUERY_TYPE_LAST;
  }
  
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

TsdbQueryHandleT tsdbQueryRowsInExternalWindow(STsdbRepo *tsdb, STsdbQueryCond* pCond, STableGroupInfo *groupList, uint64_t qId, SMemRef* pRef) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qId, pRef);
  //pQueryHandle->loadExternalRow = true;
  //pQueryHandle->currentLoadExternalRows = true;

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
  if (pHandle->pMemRef->snapshot.mem == NULL && pHandle->pMemRef->snapshot.imem == NULL) {
    return false;
  }

  assert(pCheckInfo->iter == NULL && pCheckInfo->iiter == NULL);

  STableData* pMem = NULL;
  STableData* pIMem = NULL;

  SMemTable* pMemT = pHandle->pMemRef->snapshot.mem;
  SMemTable* pIMemT = pHandle->pMemRef->snapshot.imem;

  if (pMemT && pCheckInfo->tableId.tid < pMemT->maxTables) {
    pMem = pMemT->tData[pCheckInfo->tableId.tid];
    if (pMem != NULL && pMem->uid == pCheckInfo->tableId.uid) { // check uid
      TKEY tLastKey = keyToTkey(pCheckInfo->lastKey);
      pCheckInfo->iter =
          tSkipListCreateIterFromVal(pMem->pData, (const char*)&tLastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
    }
  }

  if (pIMemT && pCheckInfo->tableId.tid < pIMemT->maxTables) {
    pIMem = pIMemT->tData[pCheckInfo->tableId.tid];
    if (pIMem != NULL && pIMem->uid == pCheckInfo->tableId.uid) { // check uid
      TKEY tLastKey = keyToTkey(pCheckInfo->lastKey);
      pCheckInfo->iiter =
          tSkipListCreateIterFromVal(pIMem->pData, (const char*)&tLastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
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

    SMemRow row = (SMemRow)SL_GET_NODE_DATA(node);
    TSKEY   key = memRowKey(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64 ", tid:%d check data in mem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
              "-%" PRId64 ", lastKey:%" PRId64 ", numOfRows:%"PRId64", 0x%"PRIx64,
              pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, key, order, pMem->keyFirst, pMem->keyLast,
              pCheckInfo->lastKey, pMem->numOfRows, pHandle->qId);

    if (ASCENDING_TRAVERSE(order)) {
      assert(pCheckInfo->lastKey <= key);
    } else {
      assert(pCheckInfo->lastKey >= key);
    }

  } else {
    tsdbDebug("%p uid:%"PRId64", tid:%d no data in mem, 0x%"PRIx64, pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid,
        pHandle->qId);
  }

  if (!imemEmpty) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    assert(node != NULL);

    SMemRow row = (SMemRow)SL_GET_NODE_DATA(node);
    TSKEY   key = memRowKey(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64 ", tid:%d check data in imem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
              "-%" PRId64 ", lastKey:%" PRId64 ", numOfRows:%"PRId64", 0x%"PRIx64,
              pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, key, order, pIMem->keyFirst, pIMem->keyLast,
              pCheckInfo->lastKey, pIMem->numOfRows, pHandle->qId);

    if (ASCENDING_TRAVERSE(order)) {
      assert(pCheckInfo->lastKey <= key);
    } else {
      assert(pCheckInfo->lastKey >= key);
    }
  } else {
    tsdbDebug("%p uid:%"PRId64", tid:%d no data in imem, 0x%"PRIx64, pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid,
        pHandle->qId);
  }

  return true;
}

static void destroyTableMemIterator(STableCheckInfo* pCheckInfo) {
  tSkipListDestroyIter(pCheckInfo->iter);
  tSkipListDestroyIter(pCheckInfo->iiter);
}

static TSKEY extractFirstTraverseKey(STableCheckInfo* pCheckInfo, int32_t order, int32_t update) {
  SMemRow rmem = NULL, rimem = NULL;
  if (pCheckInfo->iter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    if (node != NULL) {
      rmem = (SMemRow)SL_GET_NODE_DATA(node);
    }
  }

  if (pCheckInfo->iiter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    if (node != NULL) {
      rimem = (SMemRow)SL_GET_NODE_DATA(node);
    }
  }

  if (rmem == NULL && rimem == NULL) {
    return TSKEY_INITIAL_VAL;
  }

  if (rmem != NULL && rimem == NULL) {
    pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
    return memRowKey(rmem);
  }

  if (rmem == NULL && rimem != NULL) {
    pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
    return memRowKey(rimem);
  }

  TSKEY r1 = memRowKey(rmem);
  TSKEY r2 = memRowKey(rimem);

  if (r1 == r2) {
    if(update == TD_ROW_DISCARD_UPDATE){
      pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
      tSkipListIterNext(pCheckInfo->iter);
      return r2;
    }
    else if(update == TD_ROW_OVERWRITE_UPDATE) {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
      tSkipListIterNext(pCheckInfo->iiter);
      return r1;
    } else {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
      return r1;
    }
  } else {
    if (ASCENDING_TRAVERSE(order)) {
      if (r1 < r2) {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
        return r1;
      } else {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
        return r2;
      }
    } else {
      if (r1 < r2) {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
        return r2;
      } else {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
        return r1;
      }
    }
  }
}

static SMemRow getSMemRowInTableMem(STableCheckInfo* pCheckInfo, int32_t order, int32_t update, SMemRow* extraRow) {
  SMemRow rmem = NULL, rimem = NULL;
  if (pCheckInfo->iter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    if (node != NULL) {
      rmem = (SMemRow)SL_GET_NODE_DATA(node);
    }
  }

  if (pCheckInfo->iiter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    if (node != NULL) {
      rimem = (SMemRow)SL_GET_NODE_DATA(node);
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

  TSKEY r1 = memRowKey(rmem);
  TSKEY r2 = memRowKey(rimem);

  if (r1 == r2) {
    if (update == TD_ROW_DISCARD_UPDATE) {
      tSkipListIterNext(pCheckInfo->iter);
      pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
      return rimem;
    } else if(update == TD_ROW_OVERWRITE_UPDATE){
      tSkipListIterNext(pCheckInfo->iiter);
      pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
      return rmem;
    } else {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
      extraRow = rimem;
      return rmem;
    }
  } else {
    if (ASCENDING_TRAVERSE(order)) {
      if (r1 < r2) {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
        return rmem;
      } else {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
        return rimem;
      }
    } else {
      if (r1 < r2) {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
        return rimem;
      } else {
        pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
        return rmem;
      }
    }
  }
}

static bool moveToNextRowInMem(STableCheckInfo* pCheckInfo) {
  bool hasNext = false;
  if (pCheckInfo->chosen == CHECKINFO_CHOSEN_MEM) {
    if (pCheckInfo->iter != NULL) {
      hasNext = tSkipListIterNext(pCheckInfo->iter);
    }

    if (hasNext) {
      return hasNext;
    }

    if (pCheckInfo->iiter != NULL) {
      return tSkipListIterGet(pCheckInfo->iiter) != NULL;
    }
  } else if (pCheckInfo->chosen == CHECKINFO_CHOSEN_IMEM){
    if (pCheckInfo->iiter != NULL) {
      hasNext = tSkipListIterNext(pCheckInfo->iiter);
    }

    if (hasNext) {
      return hasNext;
    }

    if (pCheckInfo->iter != NULL) {
      return tSkipListIterGet(pCheckInfo->iter) != NULL;
    }
  } else {
    if (pCheckInfo->iter != NULL) {
      hasNext = tSkipListIterNext(pCheckInfo->iter);
    }
    if (pCheckInfo->iiter != NULL) {
      hasNext = tSkipListIterNext(pCheckInfo->iiter) || hasNext;
    }
  }

  return hasNext;
}

static bool hasMoreDataInCache(STsdbQueryHandle* pHandle) {
  STsdbCfg *pCfg = &pHandle->pTsdb->config;
  size_t size = taosArrayGetSize(pHandle->pTableCheckInfo);
  assert(pHandle->activeIndex < size && pHandle->activeIndex >= 0 && size >= 1);
  pHandle->cur.fid = INT32_MIN;

  STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);

  STable* pTable = pCheckInfo->pTableObj;
  assert(pTable != NULL);

  if (!pCheckInfo->initBuf) {
    initTableMemIterator(pHandle, pCheckInfo);
  }

  SMemRow row = getSMemRowInTableMem(pCheckInfo, pHandle->order, pCfg->update, NULL);
  if (row == NULL) {
    return false;
  }

  pCheckInfo->lastKey = memRowKey(row);  // first timestamp in buffer
  tsdbDebug("%p uid:%" PRId64", tid:%d check data in buffer from skey:%" PRId64 ", order:%d, 0x%"PRIx64, pHandle,
      pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, pCheckInfo->lastKey, pHandle->order, pHandle->qId);

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

  if (key < 0) {
    key -= (daysPerFile * tsTickPerDay[precision]);
  }
  
  int64_t fid = (int64_t)(key / (daysPerFile * tsTickPerDay[precision]));  // set the starting fileId
  if (fid < 0L && llabs(fid) > INT32_MAX) { // data value overflow for INT32
    fid = INT32_MIN;
  }

  if (fid > 0L && fid > INT32_MAX) {
    fid = INT32_MAX;
  }

  return (int32_t)fid;
}

static int32_t binarySearchForBlock(SBlock* pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order) {
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

static int32_t loadBlockInfo(STsdbQueryHandle * pQueryHandle, int32_t index, int32_t* numOfBlocks) {
  int32_t code = 0;

  STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, index);
  pCheckInfo->numOfBlocks = 0;

  if (tsdbSetReadTable(&pQueryHandle->rhelper, pCheckInfo->pTableObj) != TSDB_CODE_SUCCESS) {
    code = terrno;
    return code;
  }

  SBlockIdx* compIndex = pQueryHandle->rhelper.pBlkIdx;

  // no data block in this file, try next file
  if (compIndex == NULL || compIndex->uid != pCheckInfo->tableId.uid) {
    return 0;  // no data blocks in the file belongs to pCheckInfo->pTable
  }

  assert(compIndex->len > 0);

  if (tsdbLoadBlockInfo(&(pQueryHandle->rhelper), (void**)(&pCheckInfo->pCompInfo),
                        (uint32_t*)(&pCheckInfo->compSize)) < 0) {
    return terrno;
  }
  SBlockInfo* pCompInfo = pCheckInfo->pCompInfo;

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
    return 0;
  }

  // todo speedup the procedure of located end block
  while (end < (int32_t)compIndex->numOfBlocks && (pCompInfo->blocks[end].keyFirst <= e)) {
    end += 1;
  }

  pCheckInfo->numOfBlocks = (end - start);

  if (start > 0) {
    memmove(pCompInfo->blocks, &pCompInfo->blocks[start], pCheckInfo->numOfBlocks * sizeof(SBlock));
  }

  (*numOfBlocks) += pCheckInfo->numOfBlocks;
  return 0;
}

static int32_t getFileCompInfo(STsdbQueryHandle* pQueryHandle, int32_t* numOfBlocks) {
  // load all the comp offset value for all tables in this file
  int32_t code = TSDB_CODE_SUCCESS;
  *numOfBlocks = 0;

  pQueryHandle->cost.headFileLoad += 1;
  int64_t s = taosGetTimestampUs();

  size_t numOfTables = 0;
  if (pQueryHandle->loadType == BLOCK_LOAD_TABLE_SEQ_ORDER) {
    code = loadBlockInfo(pQueryHandle, pQueryHandle->activeIndex, numOfBlocks);
  } else if (pQueryHandle->loadType == BLOCK_LOAD_OFFSET_SEQ_ORDER) {
    numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

    for (int32_t i = 0; i < numOfTables; ++i) {
      code = loadBlockInfo(pQueryHandle, i, numOfBlocks);
      if (code != TSDB_CODE_SUCCESS) {
        int64_t e = taosGetTimestampUs();

        pQueryHandle->cost.headFileLoadTime += (e - s);
        return code;
      }
    }
  } else {
    assert(0);
  }

  int64_t e = taosGetTimestampUs();
  pQueryHandle->cost.headFileLoadTime += (e - s);
  return code;
}

static int32_t doLoadFileDataBlock(STsdbQueryHandle* pQueryHandle, SBlock* pBlock, STableCheckInfo* pCheckInfo, int32_t slotIndex) {
  int64_t st = taosGetTimestampUs();

  STSchema *pSchema = tsdbGetTableSchema(pCheckInfo->pTableObj);
  int32_t   code = tdInitDataCols(pQueryHandle->pDataCols, pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for pDataCols, 0x%"PRIx64, pQueryHandle, pQueryHandle->qId);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  code = tdInitDataCols(pQueryHandle->rhelper.pDCols[0], pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for rhelper.pDataCols[0], 0x%"PRIx64, pQueryHandle, pQueryHandle->qId);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  code = tdInitDataCols(pQueryHandle->rhelper.pDCols[1], pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for rhelper.pDataCols[1], 0x%"PRIx64, pQueryHandle, pQueryHandle->qId);
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

  SDataCols* pCols = pQueryHandle->rhelper.pDCols[0];
  assert(pCols->numOfRows != 0 && pCols->numOfRows <= pBlock->numOfRows);

  pBlock->numOfRows = pCols->numOfRows;

  // Convert from TKEY to TSKEY for primary timestamp column if current block has timestamp before 1970-01-01T00:00:00Z
  if(pBlock->keyFirst < 0 && colIds[0] == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
    int64_t* src = pCols->cols[0].pData;
    for(int32_t i = 0; i < pBlock->numOfRows; ++i) {
      src[i] = tdGetKey(src[i]);
    }
  }

  int64_t elapsedTime = (taosGetTimestampUs() - st);
  pQueryHandle->cost.blockLoadTime += elapsedTime;

  tsdbDebug("%p load file block into buffer, index:%d, brange:%"PRId64"-%"PRId64", rows:%d, elapsed time:%"PRId64 " us, 0x%"PRIx64,
      pQueryHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, elapsedTime, pQueryHandle->qId);
  return TSDB_CODE_SUCCESS;

_error:
  pBlock->numOfRows = 0;

  tsdbError("%p error occurs in loading file block, index:%d, brange:%"PRId64"-%"PRId64", rows:%d, 0x%"PRIx64,
            pQueryHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, pQueryHandle->qId);
  return terrno;
}

static int32_t getEndPosInDataBlock(STsdbQueryHandle* pQueryHandle, SDataBlockInfo* pBlockInfo);
static int32_t doCopyRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows, int32_t start, int32_t end);
static void moveDataToFront(STsdbQueryHandle* pQueryHandle, int32_t numOfRows, int32_t numOfCols);
static void doCheckGeneratedBlockRange(STsdbQueryHandle* pQueryHandle);
static void copyAllRemainRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SDataBlockInfo* pBlockInfo, int32_t endPos);

static int32_t handleDataMergeIfNeeded(STsdbQueryHandle* pQueryHandle, SBlock* pBlock, STableCheckInfo* pCheckInfo){
  SQueryFilePos* cur = &pQueryHandle->cur;
  STsdbCfg*      pCfg = &pQueryHandle->pTsdb->config;
  SDataBlockInfo binfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);
  TSKEY          key;
  int32_t code = TSDB_CODE_SUCCESS;

  /*bool hasData = */ initTableMemIterator(pQueryHandle, pCheckInfo);
  assert(cur->pos >= 0 && cur->pos <= binfo.rows);

  key = extractFirstTraverseKey(pCheckInfo, pQueryHandle->order, pCfg->update);

  if (key != TSKEY_INITIAL_VAL) {
    tsdbDebug("%p key in mem:%"PRId64", 0x%"PRIx64, pQueryHandle, key, pQueryHandle->qId);
  } else {
    tsdbDebug("%p no data in mem, 0x%"PRIx64, pQueryHandle, pQueryHandle->qId);
  }

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
    if (cur->rows == binfo.rows) {
      tsdbDebug("%p whole file block qualified, brange:%"PRId64"-%"PRId64", rows:%d, lastKey:%"PRId64", tid:%d, %"PRIx64,
                pQueryHandle, cur->win.skey, cur->win.ekey, cur->rows, cur->lastKey, binfo.tid, pQueryHandle->qId);
    } else {
      tsdbDebug("%p create data block from remain file block, brange:%"PRId64"-%"PRId64", rows:%d, total:%d, lastKey:%"PRId64", tid:%d, %"PRIx64,
                pQueryHandle, cur->win.skey, cur->win.ekey, cur->rows, binfo.rows, cur->lastKey, binfo.tid, pQueryHandle->qId);
    }

  }

  return code;
}

static int32_t loadFileDataBlock(STsdbQueryHandle* pQueryHandle, SBlock* pBlock, STableCheckInfo* pCheckInfo, bool* exists) {
  SQueryFilePos* cur = &pQueryHandle->cur;
  int32_t code = TSDB_CODE_SUCCESS;
  bool asc = ASCENDING_TRAVERSE(pQueryHandle->order);

  if (asc) {
    // query ended in/started from current block
    if (pQueryHandle->window.ekey < pBlock->keyLast || pCheckInfo->lastKey > pBlock->keyFirst) {
      if ((code = doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
        *exists = false;
        return code;
      }

      SDataCols* pTSCol = pQueryHandle->rhelper.pDCols[0];
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
      cur->pos = asc? 0:(pBlock->numOfRows - 1);
      code = handleDataMergeIfNeeded(pQueryHandle, pBlock, pCheckInfo);
    }
  } else {  //desc order, query ended in current block
    if (pQueryHandle->window.ekey > pBlock->keyFirst || pCheckInfo->lastKey < pBlock->keyLast) {
      if ((code = doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
        *exists = false;
        return code;
      }

      SDataCols* pTsCol = pQueryHandle->rhelper.pDCols[0];
      if (pCheckInfo->lastKey < pBlock->keyLast) {
        cur->pos = binarySearchForKey(pTsCol->cols[0].pData, pBlock->numOfRows, pCheckInfo->lastKey, pQueryHandle->order);
      } else {
        cur->pos = pBlock->numOfRows - 1;
      }

      assert(pCheckInfo->lastKey >= pBlock->keyFirst);
      doMergeTwoLevelData(pQueryHandle, pCheckInfo, pBlock);
    } else {
      cur->pos = asc? 0:(pBlock->numOfRows-1);
      code = handleDataMergeIfNeeded(pQueryHandle, pBlock, pCheckInfo);
    }
  }

  *exists = pQueryHandle->realNumOfRows > 0;
  return code;
}

// search last keyList[ret] < key order asc  and keyList[ret] > key order desc   
static int doBinarySearchKey(TSKEY* keyList, int num, int pos, TSKEY key, int order) {
  // start end posistion
  int s, e;
  s = pos;
  
  // check
  assert(pos >=0 && pos < num);
  assert(num > 0);
 
  if (order == TSDB_ORDER_ASC) {
    // find the first position which is smaller than the key
    e  = num - 1;
    if (key < keyList[pos]) 
      return -1;  
    while (1) {
      // check can return 
      if (key >= keyList[e]) 
        return e;
      if (key <= keyList[s]) 
        return s;
      if (e - s <= 1)
        return s;
      
      // change start or end position
      int mid = s + (e - s + 1)/2;
      if (keyList[mid] > key)
        e = mid;
      else if(keyList[mid] < key)
        s = mid;
      else
        return mid;
    }
  } else { // DESC 
    // find the first position which is bigger than the key
    e  = 0;
    if (key > keyList[pos]) 
        return -1;  
      while (1) {
        // check can return 
        if (key <= keyList[e]) 
          return e;
        if (key >= keyList[s]) 
          return s;
        if (s - e <= 1)
          return s;
        
        // change start or end position
        int mid = s - (s - e + 1)/2;
        if (keyList[mid] < key)
          e = mid;
        else if(keyList[mid] > key)
          s = mid;
        else
          return mid;
      }    
    }
}

static int32_t doCopyRowsFromFileBlock(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows, int32_t start, int32_t end) {
  char* pData = NULL;
  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1 : -1;

  SDataCols* pCols = pQueryHandle->rhelper.pDCols[0];
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

    if (!isAllRowsNull(src) && pColInfo->info.colId == src->colId) {
      if (pColInfo->info.type != TSDB_DATA_TYPE_BINARY && pColInfo->info.type != TSDB_DATA_TYPE_NCHAR) {
        memmove(pData, (char*)src->pData + bytes * start, bytes * num);
      } else {  // handle the var-string
        char* dst = pData;

        // todo refactor, only copy one-by-one
        for (int32_t k = start; k < num + start; ++k) {
          const char* p = tdGetColDataOfRow(src, k);
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

// Note: row1 always has high priority
static void mergeTwoRowFromMem(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows,
                               SMemRow row1, SMemRow row2, int32_t numOfCols, STable* pTable,
                               STSchema* pSchema1, STSchema* pSchema2, bool forceSetNull) {
  char* pData = NULL;
  STSchema* pSchema;
  SMemRow row;
  int16_t colId;
  int16_t offset;

  bool isRow1DataRow = isDataRow(row1);
  bool isRow2DataRow = false;
  bool isChosenRowDataRow;
  int32_t chosen_itr;
  void *value;

  // the schema version info is embeded in SDataRow
  int32_t numOfColsOfRow1 = 0;

  if (pSchema1 == NULL) {
    pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1), (int8_t)memRowType(row1));
  }
  if(isRow1DataRow) {
    numOfColsOfRow1 = schemaNCols(pSchema1);
  } else {
    numOfColsOfRow1 = kvRowNCols(memRowKvBody(row1));
  }

  int32_t numOfColsOfRow2 = 0;
  if(row2) {
    isRow2DataRow = isDataRow(row2);
    if (pSchema2 == NULL) {
      pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2), (int8_t)memRowType(row2));
    }
    if(isRow2DataRow) {
      numOfColsOfRow2 = schemaNCols(pSchema2);
    } else {
      numOfColsOfRow2 = kvRowNCols(memRowKvBody(row2));
    }
  }


  int32_t i = 0, j = 0, k = 0;
  while(i < numOfCols && (j < numOfColsOfRow1 || k < numOfColsOfRow2)) {
    SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, i);

    if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = (char*)pColInfo->pData + (capacity - numOfRows - 1) * pColInfo->info.bytes;
    }

    int32_t colIdOfRow1;
    if(j >= numOfColsOfRow1) {
      colIdOfRow1 = INT32_MAX;
    } else if(isRow1DataRow) {
      colIdOfRow1 = pSchema1->columns[j].colId;
    } else {
      void *rowBody = memRowKvBody(row1);
      SColIdx *pColIdx = kvRowColIdxAt(rowBody, j);
      colIdOfRow1 = pColIdx->colId;
    }

    int32_t colIdOfRow2;
    if(k >= numOfColsOfRow2) {
      colIdOfRow2 = INT32_MAX;
    } else if(isRow2DataRow) {
      colIdOfRow2 = pSchema2->columns[k].colId;
    } else {
      void *rowBody = memRowKvBody(row2);
      SColIdx *pColIdx = kvRowColIdxAt(rowBody, k);
      colIdOfRow2 = pColIdx->colId;
    }

    if(colIdOfRow1 == colIdOfRow2) {
      if(colIdOfRow1 < pColInfo->info.colId) {
        j++;
        k++;
        continue;
      }
      row = row1;
      pSchema = pSchema1;
      isChosenRowDataRow = isRow1DataRow;
      chosen_itr = j;
    } else if(colIdOfRow1 < colIdOfRow2) {
      if(colIdOfRow1 < pColInfo->info.colId) {
        j++;
        continue;
      }
      row = row1;
      pSchema = pSchema1;
      isChosenRowDataRow = isRow1DataRow;
      chosen_itr = j;
    } else {
      if(colIdOfRow2 < pColInfo->info.colId) {
        k++;
        continue;
      }
      row = row2;
      pSchema = pSchema2;
      chosen_itr = k;
      isChosenRowDataRow = isRow2DataRow;
    }
    if(isChosenRowDataRow) {
      colId = pSchema->columns[chosen_itr].colId;
      offset = pSchema->columns[chosen_itr].offset;
      void *rowBody = memRowDataBody(row);
      value = tdGetRowDataOfCol(rowBody, (int8_t)pColInfo->info.type, TD_DATA_ROW_HEAD_SIZE + offset);
    } else {
      void *rowBody = memRowKvBody(row);
      SColIdx *pColIdx = kvRowColIdxAt(rowBody, chosen_itr);
      colId = pColIdx->colId;
      offset = pColIdx->offset;
      value = tdGetKvRowDataOfCol(rowBody, offset);
    }


    if (colId == pColInfo->info.colId) {
      if(forceSetNull || (!isNull(value, (int8_t)pColInfo->info.type))) {
        switch (pColInfo->info.type) {
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR:
            memcpy(pData, value, varDataTLen(value));
            break;
          case TSDB_DATA_TYPE_NULL:
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_UTINYINT:
            *(uint8_t *)pData = *(uint8_t *)value;
            break;
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_USMALLINT:
            *(uint16_t *)pData = *(uint16_t *)value;
            break;
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_UINT:
            *(uint32_t *)pData = *(uint32_t *)value;
            break;
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_UBIGINT:
            *(uint64_t *)pData = *(uint64_t *)value;
            break;
          case TSDB_DATA_TYPE_FLOAT:
            SET_FLOAT_PTR(pData, value);
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            SET_DOUBLE_PTR(pData, value);
            break;
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (pColInfo->info.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
              *(TSKEY *)pData = tdGetKey(*(TKEY *)value);
            } else {
              *(TSKEY *)pData = *(TSKEY *)value;
            }
            break;
          default:
            memcpy(pData, value, pColInfo->info.bytes);
        }
      }
      i++;

      if(row == row1) {
        j++;
      } else {
        k++;
      }
    } else {
      if(forceSetNull) {
        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull(pData, pColInfo->info.type);
        } else {
          setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
        }
      }
      i++;
    }
  }

  if(forceSetNull) {
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

  SDataCols* pCols = pQueryHandle->rhelper.pDCols[0];
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

  tsdbDebug("%p uid:%" PRIu64",tid:%d data block created, mixblock:%d, brange:%"PRIu64"-%"PRIu64" rows:%d, 0x%"PRIx64,
            pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, cur->mixBlock, cur->win.skey,
            cur->win.ekey, cur->rows, pQueryHandle->qId);
}

int32_t getEndPosInDataBlock(STsdbQueryHandle* pQueryHandle, SDataBlockInfo* pBlockInfo) {
  // NOTE: reverse the order to find the end position in data block
  int32_t endPos = -1;

  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataCols* pCols = pQueryHandle->rhelper.pDCols[0];

  if (ASCENDING_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey >= pBlockInfo->window.ekey) {
    endPos = pBlockInfo->rows - 1;
    cur->mixBlock = (cur->pos != 0);
  } else if (!ASCENDING_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey <= pBlockInfo->window.skey) {
    endPos = 0;
    cur->mixBlock = (cur->pos != pBlockInfo->rows - 1);
  } else {
    assert(pCols->numOfRows > 0);
    int pos = ASCENDING_TRAVERSE(pQueryHandle->order)? 0 : pBlockInfo->rows - 1;
    endPos = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, pos, pQueryHandle->window.ekey, pQueryHandle->order);
    assert(endPos != -1);
    cur->mixBlock = true;
  }

  return endPos;
}

// only return the qualified data to client in terms of query time window, data rows in the same block but do not
// be included in the query time window will be discarded
static void doMergeTwoLevelData(STsdbQueryHandle* pQueryHandle, STableCheckInfo* pCheckInfo, SBlock* pBlock) {
  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataBlockInfo blockInfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);
  STsdbCfg*      pCfg = &pQueryHandle->pTsdb->config;

  initTableMemIterator(pQueryHandle, pCheckInfo);

  SDataCols* pCols = pQueryHandle->rhelper.pDCols[0];
  assert(pCols->cols[0].type == TSDB_DATA_TYPE_TIMESTAMP && pCols->cols[0].colId == PRIMARYKEY_TIMESTAMP_COL_INDEX &&
      cur->pos >= 0 && cur->pos < pBlock->numOfRows);

  // key read from file
  TSKEY* keyFile = pCols->cols[0].pData;
  assert(pCols->numOfRows == pBlock->numOfRows && keyFile[0] == pBlock->keyFirst && keyFile[pBlock->numOfRows-1] == pBlock->keyLast);

  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1;
  int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pQueryHandle));

  STable* pTable = pCheckInfo->pTableObj;
  int32_t endPos = getEndPosInDataBlock(pQueryHandle, &blockInfo);
  

  tsdbDebug("%p uid:%" PRIu64",tid:%d start merge data block, file block range:%"PRIu64"-%"PRIu64" rows:%d, start:%d,"
            "end:%d, 0x%"PRIx64,
            pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, blockInfo.window.skey, blockInfo.window.ekey,
            blockInfo.rows, cur->pos, endPos, pQueryHandle->qId);

  // compared with the data from in-memory buffer, to generate the correct timestamp array list
  int32_t numOfRows = 0;

  int16_t rv1 = -1;
  int16_t rv2 = -1;
  STSchema* pSchema1 = NULL;
  STSchema* pSchema2 = NULL;

  // position in file ->fpos
  int32_t pos = cur->pos;
  cur->win = TSWINDOW_INITIALIZER;

  // no data in buffer, load data from file directly
  if (pCheckInfo->iiter == NULL && pCheckInfo->iter == NULL) {
    copyAllRemainRowsFromFileBlock(pQueryHandle, pCheckInfo, &blockInfo, endPos);
    return;
  } else if (pCheckInfo->iter != NULL || pCheckInfo->iiter != NULL) {
    SSkipListNode* node = NULL;
    do {
      SMemRow row2 = NULL;
      SMemRow row1 = getSMemRowInTableMem(pCheckInfo, pQueryHandle->order, pCfg->update, &row2);
      if (row1 == NULL) {
        break;
      }

      TSKEY keyMem = memRowKey(row1);
      if ((keyMem > pQueryHandle->window.ekey && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          (keyMem < pQueryHandle->window.ekey && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        break;
      }

      // break if pos not in this block endPos range. note old code when pos is -1 can crash.
      if(ASCENDING_TRAVERSE(pQueryHandle->order)) { //ASC
        if(pos > endPos || keyFile[pos] > pQueryHandle->window.ekey)
          break;
      } else { //DESC
        if(pos < endPos || keyFile[pos] < pQueryHandle->window.ekey)
          break;
      }

      if ((keyMem < keyFile[pos] && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          (keyMem > keyFile[pos] && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        if (rv1 != memRowVersion(row1)) {
          pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1), (int8_t)memRowType(row1));
          rv1 = memRowVersion(row1);
        }
        if(row2 && rv2 != memRowVersion(row2)) {
          pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2), (int8_t)memRowType(row2));
          rv2 = memRowVersion(row2);
        }
        
        mergeTwoRowFromMem(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, row1, row2, numOfCols, pTable, pSchema1, pSchema2, true);
        numOfRows += 1;
        // record start key with memory key if not
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = keyMem;
        }

        cur->win.ekey = keyMem;
        cur->lastKey  = keyMem + step;
        cur->mixBlock = true;

        moveToNextRowInMem(pCheckInfo);
      // same  select mem key if update is true
      } else if (keyMem == keyFile[pos]) {
        if (pCfg->update) {
          if(pCfg->update == TD_ROW_PARTIAL_UPDATE) {
            doCopyRowsFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, pos, pos);
          }
          if (rv1 != memRowVersion(row1)) {
            pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1), (int8_t)memRowType(row1));
            rv1 = memRowVersion(row1);
          }
          if(row2 && rv2 != memRowVersion(row2)) {
            pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2), (int8_t)memRowType(row2));
            rv2 = memRowVersion(row2);
          }
          
          bool forceSetNull = pCfg->update != TD_ROW_PARTIAL_UPDATE;
          mergeTwoRowFromMem(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, row1, row2, numOfCols, pTable, pSchema1, pSchema2, forceSetNull);
          numOfRows += 1;
          if (cur->win.skey == TSKEY_INITIAL_VAL) {
            cur->win.skey = keyMem;
          }

          cur->win.ekey = keyMem;
          cur->lastKey  = keyMem + step;
          cur->mixBlock = true;
          
          //mem move next
          moveToNextRowInMem(pCheckInfo);
          //file move next, discard file row
          pos += step;
        } else {
          // not update, only mem move to next, discard mem row
          moveToNextRowInMem(pCheckInfo);
        }
      // put file row
      } else if ((keyMem > keyFile[pos] && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
                 (keyMem < keyFile[pos] && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = keyFile[pos];
        }

        int32_t end = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, pos, keyMem, pQueryHandle->order);
        assert(end != -1);

        if (keyFile[end] == keyMem) { // the value of key in cache equals to the end timestamp value, ignore it
          if (pCfg->update == TD_ROW_DISCARD_UPDATE) {
            moveToNextRowInMem(pCheckInfo);
          } else {
            // can update, don't copy then deal on next loop with keyMem == keyFile[pos]
            end -= step;
          }
        }

        int32_t qstart = 0, qend = 0;
        getQualifiedRowsPos(pQueryHandle, pos, end, numOfRows, &qstart, &qend);

        if(qend >= qstart) {
          // copy qend - qstart + 1 rows from file
          numOfRows = doCopyRowsFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, qstart, qend);
          int32_t num = qend - qstart + 1;
          pos += num * step;
        } else {
          // nothing copy from file
          pos += step;
        }
        
        cur->win.ekey = ASCENDING_TRAVERSE(pQueryHandle->order)? keyFile[qend] : keyFile[qstart];
        cur->lastKey  = cur->win.ekey + step;
      }
    } while (numOfRows < pQueryHandle->outputCapacity);

    if (numOfRows < pQueryHandle->outputCapacity) {
      /**
       * if cache is empty, load remain file block data. In contrast, if there are remain data in cache, do NOT
       * copy them all to result buffer, since it may be overlapped with file data block.
       */
      if (node == NULL ||
          ((memRowKey((SMemRow)SL_GET_NODE_DATA(node)) > pQueryHandle->window.ekey) &&
           ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          ((memRowKey((SMemRow)SL_GET_NODE_DATA(node)) < pQueryHandle->window.ekey) &&
           !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        // no data in cache or data in cache is greater than the ekey of time window, load data from file block
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = keyFile[pos];
        }

        int32_t start = -1, end = -1;
        getQualifiedRowsPos(pQueryHandle, pos, endPos, numOfRows, &start, &end);

        numOfRows = doCopyRowsFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, start, end);
        pos += (end - start + 1) * step;

        cur->win.ekey = ASCENDING_TRAVERSE(pQueryHandle->order)? keyFile[end] : keyFile[start];
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

  tsdbDebug("%p uid:%" PRIu64",tid:%d data block created, mixblock:%d, brange:%"PRIu64"-%"PRIu64" rows:%d, 0x%"PRIx64,
      pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, cur->mixBlock, cur->win.skey,
      cur->win.ekey, cur->rows, pQueryHandle->qId);
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
  tfree(pSupporter->numOfBlocksPerTable);
  tfree(pSupporter->blockIndexArray);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableBlockInfo* pBlockInfo = pSupporter->pDataBlockInfo[i];
    tfree(pBlockInfo);
  }

  tfree(pSupporter->pDataBlockInfo);
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

  // access data blocks according to the offset of each block in asc/desc order.
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

    SBlock* pBlock = pTableCheck->pCompInfo->blocks;
    sup.numOfBlocksPerTable[numOfQualTables] = pTableCheck->numOfBlocks;

    char* buf = malloc(sizeof(STableBlockInfo) * pTableCheck->numOfBlocks);
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

    tsdbDebug("%p create data blocks info struct completed for 1 table, %d blocks not sorted 0x%"PRIx64, pQueryHandle, cnt,
        pQueryHandle->qId);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables 0x%"PRIx64, pQueryHandle, cnt,
      numOfQualTables, pQueryHandle->qId);

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

  tsdbDebug("%p %d data blocks sort completed, 0x%"PRIx64, pQueryHandle, cnt, pQueryHandle->qId);
  cleanBlockOrderSupporter(&sup, numOfTables);
  free(pTree);

  return TSDB_CODE_SUCCESS;
}

static int32_t getFirstFileDataBlock(STsdbQueryHandle* pQueryHandle, bool* exists);

static int32_t getDataBlockRv(STsdbQueryHandle* pQueryHandle, STableBlockInfo* pNext, bool *exists) {
  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1 : -1;
  SQueryFilePos* cur = &pQueryHandle->cur;

  while(1) {
    int32_t code = loadFileDataBlock(pQueryHandle, pNext->compBlock, pNext->pTableCheckInfo, exists);
    if (code != TSDB_CODE_SUCCESS || *exists) {
      return code;
    }

    if ((cur->slot == pQueryHandle->numOfBlocks - 1 && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
        (cur->slot == 0 && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
      // all data blocks in current file has been checked already, try next file if exists
      return getFirstFileDataBlock(pQueryHandle, exists);
    } else {  // next block of the same file
      cur->slot += step;
      cur->mixBlock = false;
      cur->blockCompleted = false;
      pNext = &pQueryHandle->pDataBlockInfo[cur->slot];
    }
  }
}

static int32_t getFirstFileDataBlock(STsdbQueryHandle* pQueryHandle, bool* exists) {
  pQueryHandle->numOfBlocks = 0;
  SQueryFilePos* cur = &pQueryHandle->cur;

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t numOfBlocks = 0;
  int32_t numOfTables = (int32_t)taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  STsdbCfg* pCfg = &pQueryHandle->pTsdb->config;
  STimeWindow win = TSWINDOW_INITIALIZER;

  while (true) {
    tsdbRLockFS(REPO_FS(pQueryHandle->pTsdb));

    if ((pQueryHandle->pFileGroup = tsdbFSIterNext(&pQueryHandle->fileIter)) == NULL) {
      tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));
      break;
    }

    tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, pQueryHandle->pFileGroup->fid, &win.skey, &win.ekey);

    // current file are not overlapped with query time window, ignore remain files
    if ((ASCENDING_TRAVERSE(pQueryHandle->order) && win.skey > pQueryHandle->window.ekey) ||
        (!ASCENDING_TRAVERSE(pQueryHandle->order) && win.ekey < pQueryHandle->window.ekey)) {
      tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, 0x%"PRIx64, pQueryHandle,
                pQueryHandle->window.skey, pQueryHandle->window.ekey, pQueryHandle->qId);
      pQueryHandle->pFileGroup = NULL;
      assert(pQueryHandle->numOfBlocks == 0);
      break;
    }

    if (tsdbSetAndOpenReadFSet(&pQueryHandle->rhelper, pQueryHandle->pFileGroup) < 0) {
      tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));
      code = terrno;
      break;
    }

    tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));

    if (tsdbLoadBlockIdx(&pQueryHandle->rhelper) < 0) {
      code = terrno;
      break;
    }

    if ((code = getFileCompInfo(pQueryHandle, &numOfBlocks)) != TSDB_CODE_SUCCESS) {
      break;
    }

    tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, 0x%"PRIx64, pQueryHandle, numOfBlocks, numOfTables,
              pQueryHandle->pFileGroup->fid, pQueryHandle->qId);

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

    cur->fid = INT32_MIN;  // denote that there are no data in file anymore
    *exists = false;
    return code;
  }

  assert(pQueryHandle->pFileGroup != NULL && pQueryHandle->numOfBlocks > 0);
  cur->slot = ASCENDING_TRAVERSE(pQueryHandle->order)? 0:pQueryHandle->numOfBlocks-1;
  cur->fid = pQueryHandle->pFileGroup->fid;

  STableBlockInfo* pBlockInfo = &pQueryHandle->pDataBlockInfo[cur->slot];
  return getDataBlockRv(pQueryHandle, pBlockInfo, exists);
}

static bool isEndFileDataBlock(SQueryFilePos* cur, int32_t numOfBlocks, bool ascTrav) {
  assert(cur != NULL && numOfBlocks > 0);
  return (cur->slot == numOfBlocks - 1 && ascTrav) || (cur->slot == 0 && !ascTrav);
}

static void moveToNextDataBlockInCurrentFile(STsdbQueryHandle* pQueryHandle) {
  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1 : -1;

  SQueryFilePos* cur = &pQueryHandle->cur;
  assert(cur->slot < pQueryHandle->numOfBlocks && cur->slot >= 0);

  cur->slot += step;
  cur->mixBlock       = false;
  cur->blockCompleted = false;
}

int32_t tsdbGetFileBlocksDistInfo(TsdbQueryHandleT* queryHandle, STableBlockDist* pTableBlockInfo) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) queryHandle;

  pTableBlockInfo->totalSize = 0;
  pTableBlockInfo->totalRows = 0;
  STsdbFS* pFileHandle = REPO_FS(pQueryHandle->pTsdb);

  // find the start data block in file
  pQueryHandle->locateStart = true;
  STsdbCfg* pCfg = &pQueryHandle->pTsdb->config;
  int32_t   fid = getFileIdFromKey(pQueryHandle->window.skey, pCfg->daysPerFile, pCfg->precision);

  tsdbRLockFS(pFileHandle);
  tsdbFSIterInit(&pQueryHandle->fileIter, pFileHandle, pQueryHandle->order);
  tsdbFSIterSeek(&pQueryHandle->fileIter, fid);
  tsdbUnLockFS(pFileHandle);

  pTableBlockInfo->numOfFiles += 1;

  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     numOfBlocks = 0;
  int32_t     numOfTables = (int32_t)taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  int         defaultRows = TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);
  STimeWindow win = TSWINDOW_INITIALIZER;

  while (true) {
    numOfBlocks = 0;
    tsdbRLockFS(REPO_FS(pQueryHandle->pTsdb));

    if ((pQueryHandle->pFileGroup = tsdbFSIterNext(&pQueryHandle->fileIter)) == NULL) {
      tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));
      break;
    }

    tsdbGetFidKeyRange(pCfg->daysPerFile, pCfg->precision, pQueryHandle->pFileGroup->fid, &win.skey, &win.ekey);

    // current file are not overlapped with query time window, ignore remain files
    if ((ASCENDING_TRAVERSE(pQueryHandle->order) && win.skey > pQueryHandle->window.ekey) ||
    (!ASCENDING_TRAVERSE(pQueryHandle->order) && win.ekey < pQueryHandle->window.ekey)) {
      tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, 0x%"PRIx64, pQueryHandle,
                pQueryHandle->window.skey, pQueryHandle->window.ekey, pQueryHandle->qId);
      pQueryHandle->pFileGroup = NULL;
      break;
    }

    pTableBlockInfo->numOfFiles += 1;
    if (tsdbSetAndOpenReadFSet(&pQueryHandle->rhelper, pQueryHandle->pFileGroup) < 0) {
      tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));
      code = terrno;
      break;
    }

    tsdbUnLockFS(REPO_FS(pQueryHandle->pTsdb));

    if (tsdbLoadBlockIdx(&pQueryHandle->rhelper) < 0) {
      code = terrno;
      break;
    }

    if ((code = getFileCompInfo(pQueryHandle, &numOfBlocks)) != TSDB_CODE_SUCCESS) {
      break;
    }

    tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, 0x%"PRIx64, pQueryHandle, numOfBlocks, numOfTables,
              pQueryHandle->pFileGroup->fid, pQueryHandle->qId);

    if (numOfBlocks == 0) {
      continue;
    }

    for (int32_t i = 0; i < numOfTables; ++i) {
      STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);

      SBlock* pBlock = pCheckInfo->pCompInfo->blocks;
      for (int32_t j = 0; j < pCheckInfo->numOfBlocks; ++j) {
        pTableBlockInfo->totalSize += pBlock[j].len;

        int32_t numOfRows = pBlock[j].numOfRows;
        pTableBlockInfo->totalRows += numOfRows;
        if (numOfRows > pTableBlockInfo->maxRows) pTableBlockInfo->maxRows = numOfRows;
        if (numOfRows < pTableBlockInfo->minRows) pTableBlockInfo->minRows = numOfRows;
        if (numOfRows < defaultRows) pTableBlockInfo->numOfSmallBlocks+=1;
        int32_t  stepIndex = (numOfRows-1)/TSDB_BLOCK_DIST_STEP_ROWS;
        SFileBlockInfo *blockInfo = (SFileBlockInfo*)taosArrayGet(pTableBlockInfo->dataBlockInfos, stepIndex);
        blockInfo->numBlocksOfStep++;
      }
    }
  }

  return code;
}

static int32_t getDataBlocksInFiles(STsdbQueryHandle* pQueryHandle, bool* exists) {
  STsdbFS*       pFileHandle = REPO_FS(pQueryHandle->pTsdb);
  SQueryFilePos* cur = &pQueryHandle->cur;

  // find the start data block in file
  if (!pQueryHandle->locateStart) {
    pQueryHandle->locateStart = true;
    STsdbCfg* pCfg = &pQueryHandle->pTsdb->config;
    int32_t   fid = getFileIdFromKey(pQueryHandle->window.skey, pCfg->daysPerFile, pCfg->precision);

    tsdbRLockFS(pFileHandle);
    tsdbFSIterInit(&pQueryHandle->fileIter, pFileHandle, pQueryHandle->order);
    tsdbFSIterSeek(&pQueryHandle->fileIter, fid);
    tsdbUnLockFS(pFileHandle);

    return getFirstFileDataBlock(pQueryHandle, exists);
  } else {
    // check if current file block is all consumed
    STableBlockInfo* pBlockInfo = &pQueryHandle->pDataBlockInfo[cur->slot];
    STableCheckInfo* pCheckInfo = pBlockInfo->pTableCheckInfo;

    // current block is done, try next
    if ((!cur->mixBlock) || cur->blockCompleted) {
      // all data blocks in current file has been checked already, try next file if exists
    } else {
      tsdbDebug("%p continue in current data block, index:%d, pos:%d, 0x%"PRIx64, pQueryHandle, cur->slot, cur->pos,
                pQueryHandle->qId);
      int32_t code = handleDataMergeIfNeeded(pQueryHandle, pBlockInfo->compBlock, pCheckInfo);
      *exists = (pQueryHandle->realNumOfRows > 0);

      if (code != TSDB_CODE_SUCCESS || *exists) {
        return code;
      }
    }

    // current block is empty, try next block in file
    // all data blocks in current file has been checked already, try next file if exists
    if (isEndFileDataBlock(cur, pQueryHandle->numOfBlocks, ASCENDING_TRAVERSE(pQueryHandle->order))) {
      return getFirstFileDataBlock(pQueryHandle, exists);
    } else {
      moveToNextDataBlockInCurrentFile(pQueryHandle);
      STableBlockInfo* pNext = &pQueryHandle->pDataBlockInfo[cur->slot];
      return getDataBlockRv(pQueryHandle, pNext, exists);
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

  // no data in memtable or imemtable, decrease the memory reference.
  // TODO !!
//  tsdbMayUnTakeMemSnapshot(pQueryHandle);
  return false;
}

//todo not unref yet, since it is not support multi-group interpolation query
static UNUSED_FUNC void changeQueryHandleForInterpQuery(TsdbQueryHandleT pHandle) {
  // filter the queried time stamp in the first place
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;

  // starts from the buffer in case of descending timestamp order check data blocks
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  int32_t i = 0;
  while(i < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);

    // the first qualified table for interpolation query
    if ((pQueryHandle->window.skey <= pCheckInfo->pTableObj->lastKey) &&
        (pCheckInfo->pTableObj->lastKey != TSKEY_INITIAL_VAL)) {
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
}

static int tsdbReadRowsFromCache(STableCheckInfo* pCheckInfo, TSKEY maxKey, int maxRowsToRead, STimeWindow* win,
                                 STsdbQueryHandle* pQueryHandle) {
  int     numOfRows = 0;
  int32_t numOfCols = (int32_t)taosArrayGetSize(pQueryHandle->pColumns);
  STsdbCfg* pCfg = &pQueryHandle->pTsdb->config;
  win->skey = TSKEY_INITIAL_VAL;

  int64_t st = taosGetTimestampUs();
  STable* pTable = pCheckInfo->pTableObj;
  int16_t rv = -1;
  STSchema* pSchema = NULL;

  do {
    SMemRow row = getSMemRowInTableMem(pCheckInfo, pQueryHandle->order, pCfg->update, NULL);
    if (row == NULL) {
      break;
    }

    TSKEY key = memRowKey(row);
    if ((key > maxKey && ASCENDING_TRAVERSE(pQueryHandle->order)) || (key < maxKey && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
      tsdbDebug("%p key:%"PRIu64" beyond qrange:%"PRId64" - %"PRId64", no more data in buffer", pQueryHandle, key, pQueryHandle->window.skey,
                pQueryHandle->window.ekey);

      break;
    }

    if (win->skey == INT64_MIN) {
      win->skey = key;
    }

    win->ekey = key;
    if (rv != memRowVersion(row)) {
      pSchema = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row), (int8_t)memRowType(row));
      rv = memRowVersion(row);
    }
    mergeTwoRowFromMem(pQueryHandle, maxRowsToRead, numOfRows, row, NULL, numOfCols, pTable, pSchema, NULL, true);

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
  tsdbDebug("%p build data block from cache completed, elapsed time:%"PRId64" us, numOfRows:%d, numOfCols:%d, 0x%"PRIx64, pQueryHandle,
            elapsedTime, numOfRows, numOfCols, pQueryHandle->qId);

  return numOfRows;
}

static int32_t getAllTableList(STable* pSuperTable, SArray* list) {
  STSchema* pTagSchema = tsdbGetTableTagSchema(pSuperTable);
  if(pTagSchema && pTagSchema->numOfCols == 1 && pTagSchema->columns[0].type == TSDB_DATA_TYPE_JSON){
    uint32_t key = TSDB_DATA_JSON_NULL;
    char keyMd5[TSDB_MAX_JSON_KEY_MD5_LEN] = {0};
    jsonKeyMd5(&key, INT_BYTES, keyMd5);
    SArray** tablist = (SArray**)taosHashGet(pSuperTable->jsonKeyMap, keyMd5, TSDB_MAX_JSON_KEY_MD5_LEN);

    for (int i = 0; i < taosArrayGetSize(*tablist); ++i) {
      JsonMapValue* p = taosArrayGet(*tablist, i);
      STableKeyInfo info = {.pTable = p->table, .lastKey = TSKEY_INITIAL_VAL};
      taosArrayPush(list, &info);
    }
  }else{
    SSkipListIterator* iter = tSkipListCreateIter(pSuperTable->pIndex);
    while (tSkipListIterNext(iter)) {
      SSkipListNode* pNode = tSkipListIterGet(iter);

      STable* pTable = (STable*) SL_GET_NODE_DATA((SSkipListNode*) pNode);

      STableKeyInfo info = {.pTable = pTable, .lastKey = TSKEY_INITIAL_VAL};
      taosArrayPush(list, &info);
    }

    tSkipListDestroyIter(iter);
  }
  return TSDB_CODE_SUCCESS;
}

static bool  loadBlockOfActiveTable(STsdbQueryHandle* pQueryHandle) {
  if (pQueryHandle->checkFiles) {
    // check if the query range overlaps with the file data block
    bool exists = true;

    int32_t code = getDataBlocksInFiles(pQueryHandle, &exists);
    if (code != TSDB_CODE_SUCCESS) {
      pQueryHandle->checkFiles = false;
      return false;
    }

    if (exists) {
      tsdbRetrieveDataBlock((TsdbQueryHandleT*) pQueryHandle, NULL);
      if (pQueryHandle->currentLoadExternalRows && pQueryHandle->window.skey == pQueryHandle->window.ekey) {
        SColumnInfoData* pColInfo = taosArrayGet(pQueryHandle->pColumns, 0);
        assert(*(int64_t*)pColInfo->pData == pQueryHandle->window.skey);
      }

      pQueryHandle->currentLoadExternalRows = false; // clear the flag, since the exact matched row is found.
      return exists;
    }

    pQueryHandle->checkFiles = false;
  }

  if (hasMoreDataInCache(pQueryHandle)) {
    pQueryHandle->currentLoadExternalRows = false;
    return true;
  }

  // current result is empty
  if (pQueryHandle->currentLoadExternalRows && pQueryHandle->window.skey == pQueryHandle->window.ekey && pQueryHandle->cur.rows == 0) {
    SMemRef* pMemRef = pQueryHandle->pMemRef;

    doGetExternalRow(pQueryHandle, TSDB_PREV_ROW, pMemRef);
    doGetExternalRow(pQueryHandle, TSDB_NEXT_ROW, pMemRef);

    bool result = tsdbGetExternalRow(pQueryHandle);

    pQueryHandle->prev = doFreeColumnInfoData(pQueryHandle->prev);
    pQueryHandle->next = doFreeColumnInfoData(pQueryHandle->next);
    pQueryHandle->currentLoadExternalRows = false;

    return result;
  }

  return false;
}

static bool loadCachedLastRow(STsdbQueryHandle* pQueryHandle) {
  // the last row is cached in buffer, return it directly.
  // here note that the pQueryHandle->window must be the TS_INITIALIZER
  int32_t numOfCols  = (int32_t)(QH_GET_NUM_OF_COLS(pQueryHandle));
  size_t  numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  assert(numOfTables > 0 && numOfCols > 0);

  SQueryFilePos* cur = &pQueryHandle->cur;

  SMemRow  pRow = NULL;
  TSKEY    key  = TSKEY_INITIAL_VAL;
  int32_t  step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1;

  if (++pQueryHandle->activeIndex < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, pQueryHandle->activeIndex);
    int32_t ret = tsdbGetCachedLastRow(pCheckInfo->pTableObj, &pRow, &key);
    if (ret != TSDB_CODE_SUCCESS) {
      return false;
    }
    mergeTwoRowFromMem(pQueryHandle, pQueryHandle->outputCapacity, 0, pRow, NULL, numOfCols, pCheckInfo->pTableObj, NULL, NULL, true);
    tfree(pRow);

    // update the last key value
    pCheckInfo->lastKey = key + step;

    cur->rows     = 1;  // only one row
    cur->lastKey  = key + step;
    cur->mixBlock = true;
    cur->win.skey = key;
    cur->win.ekey = key;

    return true;
  }

  return false;
}



static bool loadCachedLast(STsdbQueryHandle* pQueryHandle) {
  // the last row is cached in buffer, return it directly.
  // here note that the pQueryHandle->window must be the TS_INITIALIZER
  int32_t tgNumOfCols = (int32_t)QH_GET_NUM_OF_COLS(pQueryHandle);
  size_t  numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  int32_t numOfRows = 0;
  assert(numOfTables > 0 && tgNumOfCols > 0);
  SQueryFilePos* cur = &pQueryHandle->cur;
  TSKEY priKey = TSKEY_INITIAL_VAL;
  int32_t priIdx = -1;
  SColumnInfoData* pColInfo = NULL;

  while (++pQueryHandle->activeIndex < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, pQueryHandle->activeIndex);
    STable* pTable = pCheckInfo->pTableObj;  
    char* pData = NULL;

    int32_t numOfCols = pTable->maxColNum;
    
    if (pTable->lastCols == NULL || pTable->maxColNum <= 0) {
      tsdbWarn("no last cached for table %s, uid:%" PRIu64 ",tid:%d", pTable->name->data, pTable->tableId.uid, pTable->tableId.tid);
      continue;
    }
    
    int32_t i = 0, j = 0;

    // lock pTable->lastCols[i] as it would be released when schema update(tsdbUpdateLastColSchema)
    TSDB_RLOCK_TABLE(pTable);
    while(i < tgNumOfCols && j < numOfCols) {
      pColInfo = taosArrayGet(pQueryHandle->pColumns, i);
      if (pTable->lastCols[j].colId < pColInfo->info.colId) {
        j++;
        continue;
      } else if (pTable->lastCols[j].colId > pColInfo->info.colId) {
        i++;
        continue;
      }
    
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    
      if (pTable->lastCols[j].bytes > 0) {        
        void* value = pTable->lastCols[j].pData;
        switch (pColInfo->info.type) {
          case TSDB_DATA_TYPE_BINARY:
          case TSDB_DATA_TYPE_NCHAR:
            memcpy(pData, value, varDataTLen(value));
            break;
          case TSDB_DATA_TYPE_NULL:
          case TSDB_DATA_TYPE_BOOL:
          case TSDB_DATA_TYPE_TINYINT:
          case TSDB_DATA_TYPE_UTINYINT:  
            *(uint8_t *)pData = *(uint8_t *)value;
            break;
          case TSDB_DATA_TYPE_SMALLINT:
          case TSDB_DATA_TYPE_USMALLINT:
            *(uint16_t *)pData = *(uint16_t *)value;
            break;
          case TSDB_DATA_TYPE_INT:
          case TSDB_DATA_TYPE_UINT:
            *(uint32_t *)pData = *(uint32_t *)value;
            break;
          case TSDB_DATA_TYPE_BIGINT:
          case TSDB_DATA_TYPE_UBIGINT:
            *(uint64_t *)pData = *(uint64_t *)value;
            break;
          case TSDB_DATA_TYPE_FLOAT:
            SET_FLOAT_PTR(pData, value);
            break;
          case TSDB_DATA_TYPE_DOUBLE:
            SET_DOUBLE_PTR(pData, value);
            break;
          case TSDB_DATA_TYPE_TIMESTAMP:
            if (pColInfo->info.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
              priKey = tdGetKey(*(TKEY *)value);
              priIdx = i;

              i++;
              j++;
              continue;
            } else {
              *(TSKEY *)pData = *(TSKEY *)value;
            }
            break;
          default:
            memcpy(pData, value, pColInfo->info.bytes);
        }
    
        for (int32_t n = 0; n < tgNumOfCols; ++n) {
          if (n == i) {
            continue;
          }

          pColInfo = taosArrayGet(pQueryHandle->pColumns, n);
          pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;;

          if (pColInfo->info.colId == PRIMARYKEY_TIMESTAMP_COL_INDEX) {
            *(TSKEY *)pData = pTable->lastCols[j].ts;
            continue;
          }
          
          if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
            setVardataNull(pData, pColInfo->info.type);
          } else {
            setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
          }
        }

        numOfRows++;
        assert(numOfRows < pQueryHandle->outputCapacity);
      } 
    
      i++;
      j++;
    }
    TSDB_RUNLOCK_TABLE(pTable);

    // leave the real ts column as the last row, because last function only (not stable) use the last row as res
    if (priKey != TSKEY_INITIAL_VAL) {
      pColInfo = taosArrayGet(pQueryHandle->pColumns, priIdx);
      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
    
      *(TSKEY *)pData = priKey;

      for (int32_t n = 0; n < tgNumOfCols; ++n) {
        if (n == priIdx) {
          continue;
        }
      
        pColInfo = taosArrayGet(pQueryHandle->pColumns, n);
        pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;;
      
        assert (pColInfo->info.colId != PRIMARYKEY_TIMESTAMP_COL_INDEX);
        
        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
          setVardataNull(pData, pColInfo->info.type);
        } else {
          setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
        }
      }

      numOfRows++;
    }
    
    if (numOfRows > 0) {
      cur->rows     = numOfRows;
      cur->mixBlock = true;
      
      return true;
    }    
  }

  return false;
}

void tsdbSwitchTable(TsdbQueryHandleT queryHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) queryHandle;

  STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, pQueryHandle->activeIndex);
  pCheckInfo->numOfBlocks = 0;
  
  pQueryHandle->locateStart = false;
  pQueryHandle->checkFiles  = true;
  pQueryHandle->cur.rows    = 0;
  pQueryHandle->currentLoadExternalRows = pQueryHandle->loadExternalRow;
  
  terrno = TSDB_CODE_SUCCESS;

  ++pQueryHandle->activeIndex;
}


static bool loadDataBlockFromTableSeq(STsdbQueryHandle* pQueryHandle) {
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  assert(numOfTables > 0);

  int64_t stime = taosGetTimestampUs();

  while(pQueryHandle->activeIndex < numOfTables) {
    if (loadBlockOfActiveTable(pQueryHandle)) {
      return true;
    }

    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, pQueryHandle->activeIndex);
    pCheckInfo->numOfBlocks = 0;

    pQueryHandle->activeIndex += 1;
    pQueryHandle->locateStart = false;
    pQueryHandle->checkFiles  = true;
    pQueryHandle->cur.rows    = 0;
    pQueryHandle->currentLoadExternalRows = pQueryHandle->loadExternalRow;

    terrno = TSDB_CODE_SUCCESS;

    int64_t elapsedTime = taosGetTimestampUs() - stime;
    pQueryHandle->cost.checkForNextTime += elapsedTime;
  }

  return false;
}

// handle data in cache situation
bool tsdbNextDataBlock(TsdbQueryHandleT pHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;
  if (pQueryHandle == NULL) {
    return false;
  }

  if (emptyQueryTimewindow(pQueryHandle)) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, 0x%"PRIx64, pQueryHandle, pQueryHandle->qId);
    return false;
  }

  int64_t stime = taosGetTimestampUs();
  int64_t elapsedTime = stime;

  // TODO refactor: remove "type"
  if (pQueryHandle->type == TSDB_QUERY_TYPE_LAST) {
    if (pQueryHandle->cachelastrow == TSDB_CACHED_TYPE_LASTROW) {
      return loadCachedLastRow(pQueryHandle);
    } else if (pQueryHandle->cachelastrow == TSDB_CACHED_TYPE_LAST) {
      return loadCachedLast(pQueryHandle);
    }
  }

  if (pQueryHandle->loadType == BLOCK_LOAD_TABLE_SEQ_ORDER) {
    return loadDataBlockFromTableSeq(pQueryHandle);
  } else { // loadType == RR and Offset Order
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
}

static int32_t doGetExternalRow(STsdbQueryHandle* pQueryHandle, int16_t type, SMemRef* pMemRef) {
  STsdbQueryHandle* pSecQueryHandle = NULL;

  if (type == TSDB_PREV_ROW && pQueryHandle->prev) {
    return TSDB_CODE_SUCCESS;
  }

  if (type == TSDB_NEXT_ROW && pQueryHandle->next) {
    return TSDB_CODE_SUCCESS;
  }

  // prepare the structure
  int32_t numOfCols = (int32_t) QH_GET_NUM_OF_COLS(pQueryHandle);

  if (type == TSDB_PREV_ROW) {
    pQueryHandle->prev = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
    if (pQueryHandle->prev == NULL) {
      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto out_of_memory;
    }
  } else {
    pQueryHandle->next = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
    if (pQueryHandle->next == NULL) {
      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto out_of_memory;
    }
  }

  SArray* row = (type == TSDB_PREV_ROW)? pQueryHandle->prev : pQueryHandle->next;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);

    SColumnInfoData colInfo = {{0}, 0};
    colInfo.info = pCol->info;
    colInfo.pData = calloc(1, pCol->info.bytes);
    if (colInfo.pData == NULL) {
      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
      goto out_of_memory;
    }

    taosArrayPush(row, &colInfo);
  }

  // load the previous row
  STsdbQueryCond cond = {.numOfCols = numOfCols, .loadExternalRows = false, .type = BLOCK_LOAD_OFFSET_SEQ_ORDER};
  if (type == TSDB_PREV_ROW) {
    cond.order = TSDB_ORDER_DESC;
    cond.twindow = (STimeWindow){pQueryHandle->window.skey, INT64_MIN};
  } else {
    cond.order = TSDB_ORDER_ASC;
    cond.twindow = (STimeWindow){pQueryHandle->window.skey, INT64_MAX};
  }

  cond.colList = calloc(cond.numOfCols, sizeof(SColumnInfo));
  if (cond.colList == NULL) {
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto out_of_memory;
  }

  for (int32_t i = 0; i < cond.numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pQueryHandle->pColumns, i);
    memcpy(&cond.colList[i], &pColInfoData->info, sizeof(SColumnInfo));
  }

  pSecQueryHandle = tsdbQueryTablesImpl(pQueryHandle->pTsdb, &cond, pQueryHandle->qId, pMemRef);
  tfree(cond.colList);
  if (pSecQueryHandle == NULL) {
    goto out_of_memory;
  }

  // current table, only one table
  STableCheckInfo* pCurrent = taosArrayGet(pQueryHandle->pTableCheckInfo, pQueryHandle->activeIndex);

  SArray* psTable = NULL;
  pSecQueryHandle->pTableCheckInfo = createCheckInfoFromCheckInfo(pCurrent, pSecQueryHandle->window.skey, &psTable);
  if (pSecQueryHandle->pTableCheckInfo == NULL) {
    taosArrayDestroy(psTable);
    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
    goto out_of_memory;
  }


  tsdbMayTakeMemSnapshot(pSecQueryHandle, psTable);
  if (!tsdbNextDataBlock((void*)pSecQueryHandle)) {
    // no result in current query, free the corresponding result rows structure
    if (type == TSDB_PREV_ROW) {
      pQueryHandle->prev = doFreeColumnInfoData(pQueryHandle->prev);
    } else {
      pQueryHandle->next = doFreeColumnInfoData(pQueryHandle->next);
    }

    goto out_of_memory;
  }

  SDataBlockInfo blockInfo = {{0}, 0};
  tsdbRetrieveDataBlockInfo((void*)pSecQueryHandle, &blockInfo);
  tsdbRetrieveDataBlock((void*)pSecQueryHandle, pSecQueryHandle->defaultLoadColumn);

  row = (type == TSDB_PREV_ROW)? pQueryHandle->prev:pQueryHandle->next;
  int32_t pos = (type == TSDB_PREV_ROW)?pSecQueryHandle->cur.rows - 1:0;

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(row, i);
    SColumnInfoData* s = taosArrayGet(pSecQueryHandle->pColumns, i);
    memcpy((char*)pCol->pData, (char*)s->pData + s->info.bytes * pos, pCol->info.bytes);
  }

out_of_memory:
  tsdbCleanupQueryHandle(pSecQueryHandle);
  return terrno;
}

bool tsdbGetExternalRow(TsdbQueryHandleT pHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;
  SQueryFilePos* cur = &pQueryHandle->cur;

  cur->fid = INT32_MIN;
  cur->mixBlock = true;
  if (pQueryHandle->prev == NULL || pQueryHandle->next == NULL) {
    cur->rows = 0;
    return false;
  }

  int32_t numOfCols = (int32_t) QH_GET_NUM_OF_COLS(pQueryHandle);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pQueryHandle->pColumns, i);
    SColumnInfoData* first = taosArrayGet(pQueryHandle->prev, i);

    memcpy(pColInfoData->pData, first->pData, pColInfoData->info.bytes);

    SColumnInfoData* sec = taosArrayGet(pQueryHandle->next, i);
    memcpy(((char*)pColInfoData->pData) + pColInfoData->info.bytes, sec->pData, pColInfoData->info.bytes);

    if (i == 0 && pColInfoData->info.type == TSDB_DATA_TYPE_TIMESTAMP) {
      cur->win.skey = *(TSKEY*)pColInfoData->pData;
      cur->win.ekey = *(TSKEY*)(((char*)pColInfoData->pData) + TSDB_KEYSIZE);
    }
  }

  cur->rows = 2;
  return true;
}

/*
 * if lastRow == NULL, return TSDB_CODE_TDB_NO_CACHE_LAST_ROW
 * else set pRes and return TSDB_CODE_SUCCESS and save lastKey
 */
int32_t tsdbGetCachedLastRow(STable* pTable, SMemRow* pRes, TSKEY* lastKey) {
  int32_t code = TSDB_CODE_SUCCESS;

  TSDB_RLOCK_TABLE(pTable);

  if (!pTable->lastRow) {
    code = TSDB_CODE_TDB_NO_CACHE_LAST_ROW;
    goto out;
  }

  if (pRes) {
    *pRes = tdMemRowDup(pTable->lastRow);
    if (*pRes == NULL) {
      code = TSDB_CODE_TDB_OUT_OF_MEMORY;
    }
  }

out:
  TSDB_RUNLOCK_TABLE(pTable);
  return code;
}

bool isTsdbCacheLastRow(TsdbQueryHandleT* pQueryHandle) {
  return ((STsdbQueryHandle *)pQueryHandle)->cachelastrow > TSDB_CACHED_TYPE_NONE;
}

int32_t checkForCachedLastRow(STsdbQueryHandle* pQueryHandle, STableGroupInfo *groupList) {
  assert(pQueryHandle != NULL && groupList != NULL);

  TSKEY    key = TSKEY_INITIAL_VAL;

  SArray* group = taosArrayGetP(groupList->pGroupList, 0);
  assert(group != NULL);

  STableKeyInfo* pInfo = (STableKeyInfo*)taosArrayGet(group, 0);

  int32_t code = 0;
  
  if (((STable*)pInfo->pTable)->lastRow) {
    code = tsdbGetCachedLastRow(pInfo->pTable, NULL, &key);
    if (code != TSDB_CODE_SUCCESS) {
      pQueryHandle->cachelastrow = TSDB_CACHED_TYPE_NONE;
    } else {
      pQueryHandle->cachelastrow = TSDB_CACHED_TYPE_LASTROW;
    }
  }

  // update the tsdb query time range
  if (pQueryHandle->cachelastrow != TSDB_CACHED_TYPE_NONE) {
    pQueryHandle->window      = TSWINDOW_INITIALIZER;
    pQueryHandle->checkFiles  = false;
    pQueryHandle->activeIndex = -1;  // start from -1
  }

  return code;
}

int32_t checkForCachedLast(STsdbQueryHandle* pQueryHandle) {
  assert(pQueryHandle != NULL);

  int32_t code = 0;
  
  STsdbRepo* pRepo = pQueryHandle->pTsdb;

  if (pRepo && CACHE_LAST_NULL_COLUMN(&(pRepo->config))) {
    pQueryHandle->cachelastrow = TSDB_CACHED_TYPE_LAST;
  }

  // update the tsdb query time range
  if (pQueryHandle->cachelastrow) {
    pQueryHandle->checkFiles  = false;
    pQueryHandle->activeIndex = -1;  // start from -1
  }

  return code;
}


STimeWindow updateLastrowForEachGroup(STableGroupInfo *groupList) {
  STimeWindow window = {INT64_MAX, INT64_MIN};

  int32_t totalNumOfTable = 0;
  SArray* emptyGroup = taosArrayInit(16, sizeof(int32_t));

  // NOTE: starts from the buffer in case of descending timestamp order check data blocks
  size_t numOfGroups = taosArrayGetSize(groupList->pGroupList);
  for(int32_t j = 0; j < numOfGroups; ++j) {
    SArray* pGroup = taosArrayGetP(groupList->pGroupList, j);
    TSKEY   key = TSKEY_INITIAL_VAL;

    STableKeyInfo keyInfo = {0};

    size_t numOfTables = taosArrayGetSize(pGroup);
    for(int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pInfo = (STableKeyInfo*) taosArrayGet(pGroup, i);

      // if the lastKey equals to INT64_MIN, there is no data in this table
      TSKEY lastKey = ((STable*)(pInfo->pTable))->lastKey;
      if (key < lastKey) {
        key = lastKey;

        keyInfo.pTable  = pInfo->pTable;
        keyInfo.lastKey = key;
        pInfo->lastKey  = key;

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
      STableKeyInfo* pInfo = (STableKeyInfo*)taosArrayGet(pGroup, i);

      // keyInfo.pTable may be NULL here.
      if (pInfo->pTable != keyInfo.pTable) {
        tsdbUnRefTable(pInfo->pTable);
      }
    }

    // more than one table in each group, only one table left for each group
    if (keyInfo.pTable != NULL) {
      totalNumOfTable++;
      if (taosArrayGetSize(pGroup) == 1) {
        // do nothing
      } else {
        taosArrayClear(pGroup);
        taosArrayPush(pGroup, &keyInfo);
      }
    } else {  // mark all the empty groups, and remove it later
      taosArrayDestroy(pGroup);
      taosArrayPush(emptyGroup, &j);
    }
  }

  // window does not being updated, so set the original
  if (window.skey == INT64_MAX && window.ekey == INT64_MIN) {
    window = TSWINDOW_INITIALIZER;
    assert(totalNumOfTable == 0 && taosArrayGetSize(groupList->pGroupList) == numOfGroups);
  }

  taosArrayRemoveBatch(groupList->pGroupList, TARRAY_GET_START(emptyGroup), (int32_t) taosArrayGetSize(emptyGroup));
  taosArrayDestroy(emptyGroup);

  groupList->numOfTables = totalNumOfTable;
  return window;
}

void tsdbRetrieveDataBlockInfo(TsdbQueryHandleT* pQueryHandle, SDataBlockInfo* pDataBlockInfo) {
  STsdbQueryHandle* pHandle = (STsdbQueryHandle*)pQueryHandle;
  SQueryFilePos* cur = &pHandle->cur;
  STable* pTable = NULL;

  // there are data in file
  if (pHandle->cur.fid != INT32_MIN) {
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
  int     statisStatus = tsdbLoadBlockStatis(&pHandle->rhelper, pBlockInfo->compBlock);
  if (statisStatus < TSDB_STATIS_OK) {
    return terrno;
  } else if (statisStatus > TSDB_STATIS_OK) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  int16_t* colIds = pHandle->defaultLoadColumn->pData;

  size_t numOfCols = QH_GET_NUM_OF_COLS(pHandle);
  memset(pHandle->statis, 0, numOfCols * sizeof(SDataStatis));
  for(int32_t i = 0; i < numOfCols; ++i) {
    pHandle->statis[i].colId = colIds[i];
  }

  tsdbGetBlockStatis(&pHandle->rhelper, pHandle->statis, (int)numOfCols, pBlockInfo->compBlock);

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

  if (pHandle->cur.fid == INT32_MIN) {
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

      if (pBlockLoadInfo->slot == pHandle->cur.slot && pBlockLoadInfo->fileGroup->fid == pHandle->cur.fid &&
          pBlockLoadInfo->tid == pCheckInfo->pTableObj->tableId.tid) {
        return pHandle->pColumns;
      } else {  // only load the file block
        SBlock* pBlock = pBlockInfo->compBlock;
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
  pInfo->compare  = getComparFunc(pInfo->sch.type, pInfo->optr);
  pInfo->indexed  = pTSSchema->columns->colId == pInfo->sch.colId;

  if (pInfo->optr == TSDB_RELATION_IN) {
     int dummy = -1;
     SHashObj *pObj = NULL;
     if (pInfo->sch.colId == TSDB_TBNAME_COLUMN_INDEX) {
        SArray *arr = (SArray *)(pCond->arr);

       size_t size = taosArrayGetSize(arr);
       pObj = taosHashInit(size * 2, taosGetDefaultHashFunction(pInfo->sch.type), true, false);

        for (size_t i = 0; i < size; i++) {
          char* p = taosArrayGetP(arr, i);
          strntolower_s(varDataVal(p), varDataVal(p), varDataLen(p));
          taosHashPut(pObj, varDataVal(p), varDataLen(p), &dummy, sizeof(dummy));
        }
     } else {
       buildFilterSetFromBinary((void **)&pObj, pCond->pz, pCond->nLen);
     }

     pInfo->q = (char *)pObj;
  } else if (pCond != NULL) {
    uint32_t size = pCond->nLen * TSDB_NCHAR_SIZE;
    if (size < (uint32_t)pSchema->bytes) {
      size = pSchema->bytes;
    }

    // to make sure tonchar does not cause invalid write, since the '\0' needs at least sizeof(wchar_t) space.
    pInfo->q = calloc(1, size + TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
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
      bytes = tGetTbnameColumnSchema()->bytes;
    } else {
      if (pTableGroupSupp->pTagSchema && colIndex < pTableGroupSupp->pTagSchema->numOfCols) {
        STColumn* pCol = schemaColAt(pTableGroupSupp->pTagSchema, colIndex);
        bytes = pCol->bytes;
        type = pCol->type;
        if (type == TSDB_DATA_TYPE_JSON){
          f1 = getJsonTagValueElment(pTable1, pColIndex->name, (int32_t)strlen(pColIndex->name), NULL, TSDB_MAX_JSON_TAGS_LEN);
          f2 = getJsonTagValueElment(pTable2, pColIndex->name, (int32_t)strlen(pColIndex->name), NULL, TSDB_MAX_JSON_TAGS_LEN);
        }else{
          f1 = tdGetKVRowValOfCol(pTable1->tagVal, pCol->colId);
          f2 = tdGetKVRowValOfCol(pTable2->tagVal, pCol->colId);
        }
      } 
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

int32_t tsdbQuerySTableByTagCond(STsdbRepo* tsdb, uint64_t uid, TSKEY skey, const char* pTagCond, size_t len,
                                 STableGroupInfo* pGroupInfo, SColIndex* pColIndex, int32_t numOfCols) {
  SArray* res = NULL;
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
  res = taosArrayInit(8, sizeof(STableKeyInfo));
  STSchema* pTagSchema = tsdbGetTableTagSchema(pTable);

  // no tags and tbname condition, all child tables of this stable are involved
  if (pTagCond == NULL || len == 0) {
    int32_t ret = getAllTableList(pTable, res);
    if (ret != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepoMeta(tsdb);
      goto _error;
    }

    pGroupInfo->numOfTables = (uint32_t) taosArrayGetSize(res);
    pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols, skey);

    tsdbDebug("%p no table name/tag condition, all tables qualified, numOfTables:%u, group:%zu", tsdb,
              pGroupInfo->numOfTables, taosArrayGetSize(pGroupInfo->pGroupList));

    taosArrayDestroy(res);
    if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;
    return ret;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  tExprNode* expr = NULL;

  TRY(TSDB_MAX_TAG_CONDITIONS) {
    expr = exprTreeFromBinary(pTagCond, len);
    CLEANUP_EXECUTE();

  } CATCH( code ) {
    CLEANUP_EXECUTE();
    terrno = code;
    tsdbUnlockRepoMeta(tsdb);     // unlock tsdb in any cases

    goto _error;
    // TODO: more error handling
  } END_TRY

  void *filterInfo = calloc(1, sizeof(SFilterInfo));
  ((SFilterInfo*)filterInfo)->pTable = pTable;
  ret = filterInitFromTree(expr, &filterInfo, 0);
  tExprTreeDestroy(expr, NULL);

  if (ret != TSDB_CODE_SUCCESS) {
    terrno = ret;
    tsdbUnlockRepoMeta(tsdb);
    filterFreeInfo(filterInfo);
    goto _error;
  }

  ret = tsdbQueryTableList(pTable, res, filterInfo);
  if (ret != TSDB_CODE_SUCCESS) {
    terrno = ret;
    tsdbUnlockRepoMeta(tsdb);
    filterFreeInfo(filterInfo);
    goto _error;
  }

  filterFreeInfo(filterInfo);

  pGroupInfo->numOfTables = (uint32_t)taosArrayGetSize(res);
  pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols, skey);

  tsdbDebug("%p stable tid:%d, uid:%"PRIu64" query, numOfTables:%u, belong to %" PRIzu " groups", tsdb, pTable->tableId.tid,
      pTable->tableId.uid, pGroupInfo->numOfTables, taosArrayGetSize(pGroupInfo->pGroupList));

  taosArrayDestroy(res);

  if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;
  return ret;

  _error:

  taosArrayDestroy(res);
  return terrno;
}

int32_t tsdbGetOneTableGroup(STsdbRepo* tsdb, uint64_t uid, TSKEY startKey, STableGroupInfo* pGroupInfo) {
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

int32_t tsdbGetTableGroupFromIdList(STsdbRepo* tsdb, SArray* pTableIdList, STableGroupInfo* pGroupInfo) {
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
      tsdbUnlockRepoMeta(tsdb);
      taosArrayDestroy(group);
      return terrno;
    }

    tsdbRefTable(pTable);

    STableKeyInfo info = {.pTable = pTable, .lastKey = id->key};
    taosArrayPush(group, &info);
  }

  if (tsdbUnlockRepoMeta(tsdb) < 0) {
    taosArrayDestroy(group);
    return terrno;
  }

  pGroupInfo->numOfTables = (uint32_t) taosArrayGetSize(group);
  if (pGroupInfo->numOfTables > 0) {
    taosArrayPush(pGroupInfo->pGroupList, &group);
  } else {
    taosArrayDestroy(group);
  }

  return TSDB_CODE_SUCCESS;
}

static void* doFreeColumnInfoData(SArray* pColumnInfoData) {
  if (pColumnInfoData == NULL) {
    return NULL;
  }

  size_t cols = taosArrayGetSize(pColumnInfoData);
  for (int32_t i = 0; i < cols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pColumnInfoData, i);
    tfree(pColInfo->pData);
  }

  taosArrayDestroy(pColumnInfoData);
  return NULL;
}

static void* destroyTableCheckInfo(SArray* pTableCheckInfo) {
  size_t size = taosArrayGetSize(pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    STableCheckInfo* p = taosArrayGet(pTableCheckInfo, i);
    destroyTableMemIterator(p);

    tfree(p->pCompInfo);
  }

  taosArrayDestroy(pTableCheckInfo);
  return NULL;
}

void tsdbCleanupQueryHandle(TsdbQueryHandleT queryHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*)queryHandle;
  if (pQueryHandle == NULL) {
    return;
  }

  pQueryHandle->pColumns = doFreeColumnInfoData(pQueryHandle->pColumns);

  taosArrayDestroy(pQueryHandle->defaultLoadColumn);
  tfree(pQueryHandle->pDataBlockInfo);
  tfree(pQueryHandle->statis);

  if (!emptyQueryTimewindow(pQueryHandle)) {
    tsdbMayUnTakeMemSnapshot(pQueryHandle);
  } else {
    assert(pQueryHandle->pTableCheckInfo == NULL);
  }

  if (pQueryHandle->pTableCheckInfo != NULL) {
    pQueryHandle->pTableCheckInfo = destroyTableCheckInfo(pQueryHandle->pTableCheckInfo);
  }

  tsdbDestroyReadH(&pQueryHandle->rhelper);

  tdFreeDataCols(pQueryHandle->pDataCols);
  pQueryHandle->pDataCols = NULL;

  pQueryHandle->prev = doFreeColumnInfoData(pQueryHandle->prev);
  pQueryHandle->next = doFreeColumnInfoData(pQueryHandle->next);

  SIOCostSummary* pCost = &pQueryHandle->cost;

  tsdbDebug("%p :io-cost summary: head-file read cnt:%"PRIu64", head-file time:%"PRIu64" us, statis-info:%"PRId64" us, datablock:%" PRId64" us, check data:%"PRId64" us, 0x%"PRIx64,
      pQueryHandle, pCost->headFileLoad, pCost->headFileLoadTime, pCost->statisInfoLoadTime, pCost->blockLoadTime, pCost->checkForNextTime, pQueryHandle->qId);

  tfree(pQueryHandle);
}

void tsdbDestroyTableGroup(STableGroupInfo *pGroupList) {
  assert(pGroupList != NULL);

  size_t numOfGroup = taosArrayGetSize(pGroupList->pGroupList);

  for(int32_t i = 0; i < numOfGroup; ++i) {
    SArray* p = taosArrayGetP(pGroupList->pGroupList, i);

    size_t numOfTables = taosArrayGetSize(p);
    for(int32_t j = 0; j < numOfTables; ++j) {
      STable* pTable = taosArrayGetP(p, j);
      if (pTable != NULL) { // in case of handling retrieve data from tsdb
        tsdbUnRefTable(pTable);
      }
      //assert(pTable != NULL);
    }

    taosArrayDestroy(p);
  }

  taosHashCleanup(pGroupList->map);
  taosArrayDestroy(pGroupList->pGroupList);
  pGroupList->numOfTables = 0;
}


static FORCE_INLINE int32_t tsdbGetTagDataFromId(void *param, int32_t id, void **data) {
  STable* pTable = (STable*)(SL_GET_NODE_DATA((SSkipListNode *)param));

  if (id == TSDB_TBNAME_COLUMN_INDEX) {
    *data = TABLE_NAME(pTable);
  } else {
    *data = tdGetKVRowValOfCol(pTable->tagVal, id);
  }

  return TSDB_CODE_SUCCESS;
}



static void queryIndexedColumn(SSkipList* pSkipList, void* filterInfo, SArray* res) {
  SSkipListIterator* iter = NULL;
  char *startVal = NULL;
  int32_t order = 0;
  int32_t inRange = 0;
  int32_t flag = 0;
  bool all = false;
  int8_t *addToResult = NULL;

  filterGetIndexedColumnInfo(filterInfo, &startVal, &order, &flag);

  tsdbDebug("filter index column start, order:%d, flag:%d", order, flag);

  while (order) {
    if (FILTER_GET_FLAG(order, TSDB_ORDER_ASC)) {
      iter = tSkipListCreateIterFromVal(pSkipList, startVal, pSkipList->type, TSDB_ORDER_ASC);
      FILTER_CLR_FLAG(order, TSDB_ORDER_ASC);
    } else {
      iter = tSkipListCreateIterFromVal(pSkipList, startVal, pSkipList->type, TSDB_ORDER_DESC);
      FILTER_CLR_FLAG(order, TSDB_ORDER_DESC);
    }

    while (tSkipListIterNext(iter)) {
      SSkipListNode *pNode = tSkipListIterGet(iter);

      if (inRange == 0 || !FILTER_GET_FLAG(flag, FI_ACTION_NO_NEED)) {
        tsdbDebug("filter index column, filter it");
        filterSetColFieldData(filterInfo, pNode, tsdbGetTagDataFromId);
        all = filterExecute(filterInfo, 1, &addToResult, NULL, 0);
      }

      char *pData = SL_GET_NODE_DATA(pNode);

      tsdbDebug("filter index column, table:%s, result:%d", ((STable *)pData)->name->data, all);

      if (all || (addToResult && *addToResult)) {
        STableKeyInfo info = {.pTable = (void*)pData, .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(res, &info);
        inRange = 1;
      } else if (inRange){
        break;
      }
    }

    inRange = 0;

    tfree(addToResult);
    tSkipListDestroyIter(iter);
  }

  tsdbDebug("filter index column end");
}

static void queryIndexlessColumn(SSkipList* pSkipList, void* filterInfo, SArray* res) {
  SSkipListIterator* iter = tSkipListCreateIter(pSkipList);
  int8_t *addToResult = NULL;

  while (tSkipListIterNext(iter)) {

    SSkipListNode *pNode = tSkipListIterGet(iter);

    filterSetColFieldData(filterInfo, pNode, tsdbGetTagDataFromId);

    char *pData = SL_GET_NODE_DATA(pNode);

    bool all = filterExecute(filterInfo, 1, &addToResult, NULL, 0);

    if (all || (addToResult && *addToResult)) {
      STableKeyInfo info = {.pTable = (void*)pData, .lastKey = TSKEY_INITIAL_VAL};
      taosArrayPush(res, &info);
    }
  }

  tfree(addToResult);

  tSkipListDestroyIter(iter);
}

static FORCE_INLINE int32_t tsdbGetJsonTagDataFromId(void *param, int32_t id, char* name, void **data) {
  JsonMapValue* jsonMapV = (JsonMapValue*)(param);
  STable* pTable = (STable*)(jsonMapV->table);

  if (id == TSDB_TBNAME_COLUMN_INDEX) {
    *data = TABLE_NAME(pTable);
  } else {
    void* jsonData = tsdbGetJsonTagValue(pTable, name, TSDB_MAX_JSON_KEY_MD5_LEN, NULL);
    // jsonData == NULL for ? operation
    // if(jsonData != NULL) jsonData += CHAR_BYTES;   // jump type
    *data = jsonData;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t queryByJsonTag(STable* pTable, void* filterInfo, SArray* res){
  // get all table in fields, and dumplicate it
  SArray* tabList = NULL;
  bool needQueryAll = false;
  SFilterInfo* info = (SFilterInfo*)filterInfo;
  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    SSchema*      sch = fi->desc;
    if (sch->colId == TSDB_TBNAME_COLUMN_INDEX) {
      tabList = taosArrayInit(32, sizeof(JsonMapValue));
      getAllTableList(pTable, tabList);   // query all table
      needQueryAll = true;
      break;
    }
  }
  for (uint16_t i = 0; i < info->unitNum; ++i) {  // is null operation need query all table
    SFilterUnit* unit = &info->units[i];
    if (unit->compare.optr == TSDB_RELATION_ISNULL) {
      tabList = taosArrayInit(32, sizeof(JsonMapValue));
      getAllTableList(pTable, tabList);   // query all table
      needQueryAll = true;
      break;
    }
  }

  for (uint16_t i = 0; i < info->fields[FLD_TYPE_COLUMN].num; ++i) {
    if (needQueryAll) break;    // query all table
    SFilterField* fi = &info->fields[FLD_TYPE_COLUMN].fields[i];
    SSchema*      sch = fi->desc;
    char* key = sch->name;

    SArray** data = (SArray**)taosHashGet(pTable->jsonKeyMap, key, TSDB_MAX_JSON_KEY_MD5_LEN);
    if(data == NULL) continue;
    if(tabList == NULL) {
      tabList = taosArrayDup(*data);
    }else{
      for(int j = 0; j < taosArrayGetSize(*data); j++){
        void* element = taosArrayGet(*data, j);
        void* p = taosArraySearch(tabList, element, tsdbCompareJsonMapValue, TD_EQ);
        if (p == NULL) {
          p = taosArraySearch(tabList, element, tsdbCompareJsonMapValue, TD_GE);
          if(p == NULL){
            taosArrayPush(tabList, element);
          }else{
            taosArrayInsert(tabList, TARRAY_ELEM_IDX(tabList, p), element);
          }
        }
      }
    }
  }
  if(tabList == NULL){
    tsdbError("json key not exist, no candidate table");
    return TSDB_CODE_SUCCESS;
  }
  size_t size = taosArrayGetSize(tabList);
  int8_t *addToResult = NULL;
  for(int i = 0; i < size; i++){
    JsonMapValue* data = taosArrayGet(tabList, i);
    filterSetJsonColFieldData(filterInfo, data, tsdbGetJsonTagDataFromId);
    bool all = filterExecute(filterInfo, 1, &addToResult, NULL, 0);

    if (all || (addToResult && *addToResult)) {
      STableKeyInfo kInfo = {.pTable = (void*)(data->table), .lastKey = TSKEY_INITIAL_VAL};
      taosArrayPush(res, &kInfo);
    }
  }
  tfree(addToResult);
  taosArrayDestroy(tabList);
  return TSDB_CODE_SUCCESS;
}

static int32_t tsdbQueryTableList(STable* pTable, SArray* pRes, void* filterInfo) {
  STSchema*   pTSSchema = pTable->tagSchema;

  if(pTSSchema->columns->type == TSDB_DATA_TYPE_JSON){
    return queryByJsonTag(pTable, filterInfo, pRes);
  }else{
    bool indexQuery = false;
    SSkipList *pSkipList = pTable->pIndex;

    filterIsIndexedColumnQuery(filterInfo, pTSSchema->columns->colId, &indexQuery);

    if (indexQuery) {
      queryIndexedColumn(pSkipList, filterInfo, pRes);
    } else {
      queryIndexlessColumn(pSkipList, filterInfo, pRes);
    }
  }

  return TSDB_CODE_SUCCESS;
}

void* getJsonTagValueElment(void* data, char* key, int32_t keyLen, char* dst, int16_t bytes){
  char keyMd5[TSDB_MAX_JSON_KEY_MD5_LEN] = {0};
  jsonKeyMd5(key, keyLen, keyMd5);

  void* result = tsdbGetJsonTagValue(data, keyMd5, TSDB_MAX_JSON_KEY_MD5_LEN, NULL);
  if (result == NULL){    // json key no result
    if(!dst) return NULL;
    *dst = TSDB_DATA_TYPE_JSON;
    setNull(dst + CHAR_BYTES, TSDB_DATA_TYPE_JSON, 0);
    return dst;
  }

  char* realData = POINTER_SHIFT(result, CHAR_BYTES);
  if(*(char*)result == TSDB_DATA_TYPE_NCHAR || *(char*)result == TSDB_DATA_TYPE_BINARY) {
    assert(varDataTLen(realData) < bytes);
    if(!dst) return result;
    memcpy(dst, result, CHAR_BYTES + varDataTLen(realData));
    return dst;
  }else if (*(char*)result == TSDB_DATA_TYPE_DOUBLE || *(char*)result == TSDB_DATA_TYPE_BIGINT) {
    if(!dst) return result;
    memcpy(dst, result, CHAR_BYTES + LONG_BYTES);
    return dst;
  }else if (*(char*)result == TSDB_DATA_TYPE_BOOL) {
    if(!dst) return result;
    memcpy(dst, result, CHAR_BYTES + CHAR_BYTES);
    return dst;
  }else {
    assert(0);
  }
  return result;
}

void getJsonTagValueAll(void* data, void* dst, int16_t bytes) {
  char* json = parseTagDatatoJson(data);
  char* tagData = POINTER_SHIFT(dst, CHAR_BYTES);
  *(char*)dst = TSDB_DATA_TYPE_JSON;
  if(json == NULL){
    setNull(tagData, TSDB_DATA_TYPE_JSON, 0);
    return;
  }

  int32_t length = 0;
  if(!taosMbsToUcs4(json, strlen(json), varDataVal(tagData), bytes - VARSTR_HEADER_SIZE - CHAR_BYTES, &length)){
    tsdbError("getJsonTagValueAll mbstoucs4 error! length:%d", length);
  }
  varDataSetLen(tagData, length);
  assert(varDataTLen(tagData) <= bytes);
  tfree(json);
}

char* parseTagDatatoJson(void *p){
  char* string = NULL;
  cJSON *json = cJSON_CreateObject();
  if (json == NULL)
  {
    goto end;
  }

  int16_t nCols = kvRowNCols(p);
  ASSERT(nCols%2 == 1);
  char tagJsonKey[TSDB_MAX_JSON_KEY_LEN + 1] = {0};
  for (int j = 0; j < nCols; ++j) {
    SColIdx * pColIdx = kvRowColIdxAt(p, j);
    void* val = (kvRowColVal(p, pColIdx));
    if (j == 0){
      int8_t jsonPlaceHolder = *(int8_t*)val;
      ASSERT(jsonPlaceHolder == TSDB_DATA_JSON_PLACEHOLDER);
      continue;
    }
    if(j == 1){
      uint32_t jsonNULL = *(uint32_t*)(varDataVal(val));
      ASSERT(jsonNULL == TSDB_DATA_JSON_NULL);
      continue;
    }
    if (j == 2){
      if(*(uint32_t*)(varDataVal(val + CHAR_BYTES)) == TSDB_DATA_JSON_NULL) goto end;
      continue;
    }
    if (j%2 == 1) { // json key  encode by binary
      ASSERT(varDataLen(val) <= TSDB_MAX_JSON_KEY_LEN);
      memset(tagJsonKey, 0, sizeof(tagJsonKey));
      memcpy(tagJsonKey, varDataVal(val), varDataLen(val));
    }else{  // json value
      char tagJsonValue[TSDB_MAX_JSON_TAGS_LEN] = {0};
      char* realData = POINTER_SHIFT(val, CHAR_BYTES);
      char type = *(char*)val;
      if(type == TSDB_DATA_TYPE_BINARY) {
        assert(*(uint32_t*)varDataVal(realData) == TSDB_DATA_JSON_null);   // json null value
        assert(varDataLen(realData) == INT_BYTES);
        cJSON* value = cJSON_CreateNull();
        if (value == NULL)
        {
          goto end;
        }
        cJSON_AddItemToObject(json, tagJsonKey, value);
      }else if(type == TSDB_DATA_TYPE_NCHAR) {
        int32_t length = taosUcs4ToMbs(varDataVal(realData), varDataLen(realData), tagJsonValue);
        if (length < 0) {
          tsdbError("charset:%s to %s. val:%s convert json value failed.", DEFAULT_UNICODE_ENCODEC, tsCharset,
                   (char*)val);
          goto end;
        }
        cJSON* value = cJSON_CreateString(tagJsonValue);

        if (value == NULL)
        {
          goto end;
        }
        cJSON_AddItemToObject(json, tagJsonKey, value);
      }else if(type == TSDB_DATA_TYPE_DOUBLE){
        double jsonVd = *(double*)(realData);
        cJSON* value = cJSON_CreateNumber(jsonVd);
        if (value == NULL)
        {
          goto end;
        }
        cJSON_AddItemToObject(json, tagJsonKey, value);
      }else if(type == TSDB_DATA_TYPE_BIGINT){
        int64_t jsonVd = *(int64_t*)(realData);
        cJSON* value = cJSON_CreateNumber((double)jsonVd);
        if (value == NULL)
        {
          goto end;
        }
        cJSON_AddItemToObject(json, tagJsonKey, value);
      }else if (type == TSDB_DATA_TYPE_BOOL) {
        char jsonVd = *(char*)(realData);
        cJSON* value = cJSON_CreateBool(jsonVd);
        if (value == NULL)
        {
          goto end;
        }
        cJSON_AddItemToObject(json, tagJsonKey, value);
      }
      else{
        tsdbError("unsupportted json value");
      }
    }
  }
  string = cJSON_PrintUnformatted(json);
end:
  cJSON_Delete(json);
  return string;
}


