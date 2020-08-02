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
#include "tutil.h"
#include "tcompare.h"
#include "exception.h"

#include "../../query/inc/qAst.h"  // todo move to common module
#include "tlosertree.h"
#include "tsdb.h"
#include "tsdbMain.h"

#define EXTRA_BYTES 2
#define ASCENDING_TRAVERSE(o)   (o == TSDB_ORDER_ASC)
#define QH_GET_NUM_OF_COLS(handle) ((size_t)(taosArrayGetSize((handle)->pColumns)))

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
  int32_t       numOfBlocks;    // number of qualified data blocks not the original blocks
  SDataCols*    pDataCols;
  int32_t       chosen;         // indicate which iterator should move forward
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
  int32_t        allocSize;        // allocated data block size
  SMemTable*     mem;              // mem-table
  SMemTable*     imem;             // imem-table, acquired from snapshot
  SArray*        defaultLoadColumn;// default load column
  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQuery */

  SIOCostSummary cost;
} STsdbQueryHandle;

static void    changeQueryHandleForLastrowQuery(TsdbQueryHandleT pqHandle);
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

TsdbQueryHandleT* tsdbQueryTables(TSDB_REPO_T* tsdb, STsdbQueryCond* pCond, STableGroupInfo* groupList, void* qinfo) {
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

  if (tsdbInitReadHelper(&pQueryHandle->rhelper, (STsdbRepo*) tsdb) != 0) {
    goto out_of_memory;
  }

  tsdbTakeMemSnapshot(pQueryHandle->pTsdb, &pQueryHandle->mem, &pQueryHandle->imem);

  size_t sizeOfGroup = taosArrayGetSize(groupList->pGroupList);
  assert(sizeOfGroup >= 1 && pCond != NULL && pCond->numOfCols > 0);

  // allocate buffer in order to load data blocks from file
  int32_t numOfCols = pCond->numOfCols;
  
  pQueryHandle->statis = calloc(numOfCols, sizeof(SDataStatis));
  if (pQueryHandle->statis == NULL) {
    goto out_of_memory;
  }
  pQueryHandle->pColumns = taosArrayInit(numOfCols, sizeof(SColumnInfoData));  // todo: use list instead of array?
  if (pQueryHandle->pColumns == NULL) {
    goto out_of_memory;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData  colInfo = {{0}, 0};
  
    colInfo.info = pCond->colList[i];
    colInfo.pData = calloc(1, EXTRA_BYTES + pQueryHandle->outputCapacity * pCond->colList[i].bytes);
    if (colInfo.pData == NULL) {
      goto out_of_memory;
    }
    taosArrayPush(pQueryHandle->pColumns, &colInfo);
    pQueryHandle->statis[i].colId = colInfo.info.colId;
  }

  pQueryHandle->pTableCheckInfo = taosArrayInit(groupList->numOfTables, sizeof(STableCheckInfo));
  if (pQueryHandle->pTableCheckInfo == NULL) {
    goto out_of_memory;
  }
  STsdbMeta* pMeta = tsdbGetMeta(tsdb);
  assert(pMeta != NULL);

  for (int32_t i = 0; i < sizeOfGroup; ++i) {
    SArray* group = *(SArray**) taosArrayGet(groupList->pGroupList, i);

    size_t gsize = taosArrayGetSize(group);
    assert(gsize > 0);

    for (int32_t j = 0; j < gsize; ++j) {
      STable* pTable = (STable*) taosArrayGetP(group, j);

      STableCheckInfo info = {
          .lastKey = pQueryHandle->window.skey,
          .tableId = pTable->tableId,
          .pTableObj = pTable,
      };

      assert(info.pTableObj != NULL && (info.pTableObj->type == TSDB_NORMAL_TABLE ||
      info.pTableObj->type == TSDB_CHILD_TABLE || info.pTableObj->type == TSDB_STREAM_TABLE));

      taosArrayPush(pQueryHandle->pTableCheckInfo, &info);
    }
  }
  
  taosArraySort(pQueryHandle->pTableCheckInfo, tsdbCheckInfoCompar);
  pQueryHandle->defaultLoadColumn = getDefaultLoadColumns(pQueryHandle, true);

  tsdbDebug("%p total numOfTable:%zu in query, %p", pQueryHandle, taosArrayGetSize(pQueryHandle->pTableCheckInfo), pQueryHandle->qinfo);

  tsdbInitDataBlockLoadInfo(&pQueryHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pQueryHandle->compBlockLoadInfo);

  return (TsdbQueryHandleT) pQueryHandle;

out_of_memory:
  tsdbCleanupQueryHandle(pQueryHandle);
  terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  return NULL;
}

TsdbQueryHandleT tsdbQueryLastRow(TSDB_REPO_T *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, void* qinfo) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qinfo);
  if (pQueryHandle != NULL) {
    pQueryHandle->type = TSDB_QUERY_TYPE_LAST;
    pQueryHandle->order = TSDB_ORDER_DESC;
    changeQueryHandleForLastrowQuery(pQueryHandle);
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

TsdbQueryHandleT tsdbQueryRowsInExternalWindow(TSDB_REPO_T *tsdb, STsdbQueryCond* pCond, STableGroupInfo *groupList, void* qinfo) {
  STsdbQueryHandle *pQueryHandle = (STsdbQueryHandle*) tsdbQueryTables(tsdb, pCond, groupList, qinfo);
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
  if (pHandle->mem == NULL && pHandle->imem == NULL) {
    return false;
  }

  assert(pCheckInfo->iter == NULL && pCheckInfo->iiter == NULL);

  // TODO: add uid check
  if (pHandle->mem && pCheckInfo->tableId.tid < pHandle->mem->maxTables &&
      pHandle->mem->tData[pCheckInfo->tableId.tid] != NULL) {
    pCheckInfo->iter = tSkipListCreateIterFromVal(pHandle->mem->tData[pCheckInfo->tableId.tid]->pData,
                                                  (const char*)&pCheckInfo->lastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
  }

  if (pHandle->imem && pCheckInfo->tableId.tid < pHandle->imem->maxTables &&
      pHandle->imem->tData[pCheckInfo->tableId.tid] != NULL) {
    pCheckInfo->iiter = tSkipListCreateIterFromVal(pHandle->imem->tData[pCheckInfo->tableId.tid]->pData,
                                                   (const char*)&pCheckInfo->lastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
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

    SDataRow row = SL_GET_NODE_DATA(node);
    TSKEY key = dataRowKey(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64", tid:%d check data in mem from skey:%" PRId64 ", order:%d, %p", pHandle,
           pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, key, order, pHandle->qinfo);
  } else {
    tsdbDebug("%p uid:%"PRId64", tid:%d no data in mem, %p", pHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid,
        pHandle->qinfo);
  }

  if (!imemEmpty) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    assert(node != NULL);

    SDataRow row = SL_GET_NODE_DATA(node);
    TSKEY key = dataRowKey(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64", tid:%d check data in imem from skey:%" PRId64 ", order:%d, %p", pHandle,
           pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, key, order, pHandle->qinfo);
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

SDataRow getSDataRowInTableMem(STableCheckInfo* pCheckInfo, int32_t order) {
  SDataRow rmem = NULL, rimem = NULL;
  if (pCheckInfo->iter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    if (node != NULL) {
      rmem = SL_GET_NODE_DATA(node);
    }
  }

  if (pCheckInfo->iiter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    if (node != NULL) {
      rimem = SL_GET_NODE_DATA(node);
    }
  }

  if (rmem != NULL && rimem != NULL) {
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

  // at least one (rmem or rimem) is absent here
  if (rmem != NULL) {
    pCheckInfo->chosen = 0;
    return rmem;
  }

  if (rimem != NULL) {
    pCheckInfo->chosen = 1;
    return rimem;
  }

  return NULL;
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

  return fid;
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
  *numOfBlocks = 0;
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    pCheckInfo->numOfBlocks = 0;

    tsdbSetHelperTable(&pQueryHandle->rhelper, pCheckInfo->pTableObj, pQueryHandle->pTsdb);

    SCompIdx* compIndex = &pQueryHandle->rhelper.curCompIdx;

    // no data block in this file, try next file
    if (compIndex->len == 0 || compIndex->numOfBlocks == 0 || compIndex->uid != pCheckInfo->tableId.uid) {
      continue; // no data blocks in the file belongs to pCheckInfo->pTable
    }

    if (pCheckInfo->compSize < compIndex->len) {
      assert(compIndex->len > 0);

      char* t = realloc(pCheckInfo->pCompInfo, compIndex->len);
      assert(t != NULL);

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
    while (end < compIndex->numOfBlocks && (pCompInfo->blocks[end].keyFirst <= e)) {
      end += 1;
    }

    pCheckInfo->numOfBlocks = (end - start);

    if (start > 0) {
      memmove(pCompInfo->blocks, &pCompInfo->blocks[start], pCheckInfo->numOfBlocks * sizeof(SCompBlock));
    }

    (*numOfBlocks) += pCheckInfo->numOfBlocks;
  }

  return TSDB_CODE_SUCCESS;
}

#define GET_FILE_DATA_BLOCK_INFO(_checkInfo, _block)                                   \
  ((SDataBlockInfo){.window = {.skey = (_block)->keyFirst, .ekey = (_block)->keyLast}, \
                    .numOfCols = (_block)->numOfCols,                                  \
                    .rows = (_block)->numOfRows,                                       \
                    .tid = (_checkInfo)->tableId.tid,                                  \
                    .uid = (_checkInfo)->tableId.uid})


static bool doLoadFileDataBlock(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo, int32_t slotIndex) {
  STsdbRepo *pRepo = pQueryHandle->pTsdb;
  bool       blockLoaded = false;
  int64_t    st = taosGetTimestampUs();

  if (pCheckInfo->pDataCols == NULL) {
    STsdbMeta* pMeta = tsdbGetMeta(pRepo);
    pCheckInfo->pDataCols = tdNewDataCols(pMeta->maxRowBytes, pMeta->maxCols, pRepo->config.maxRowsPerFileBlock);
  }

  STSchema* pSchema = tsdbGetTableSchema(pCheckInfo->pTableObj);
  tdInitDataCols(pCheckInfo->pDataCols, pSchema);
  tdInitDataCols(pQueryHandle->rhelper.pDataCols[0], pSchema);
  tdInitDataCols(pQueryHandle->rhelper.pDataCols[1], pSchema);

  int16_t* colIds = pQueryHandle->defaultLoadColumn->pData;
  int32_t ret = tsdbLoadBlockDataCols(&(pQueryHandle->rhelper), pBlock, pCheckInfo->pCompInfo, colIds, QH_GET_NUM_OF_COLS(pQueryHandle));
    if (ret == TSDB_CODE_SUCCESS) {
    SDataBlockLoadInfo* pBlockLoadInfo = &pQueryHandle->dataBlockLoadInfo;

    pBlockLoadInfo->fileGroup = pQueryHandle->pFileGroup;
    pBlockLoadInfo->slot = pQueryHandle->cur.slot;
    pBlockLoadInfo->tid = pCheckInfo->pTableObj->tableId.tid;

    blockLoaded = true;
  }

  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
  assert(pCols->numOfRows != 0 && pCols->numOfRows <= pBlock->numOfRows);

  pBlock->numOfRows = pCols->numOfRows;

  int64_t elapsedTime = (taosGetTimestampUs() - st);
  pQueryHandle->cost.blockLoadTime += elapsedTime;

  tsdbDebug("%p load file block into buffer, index:%d, brange:%"PRId64"-%"PRId64" , rows:%d, elapsed time:%"PRId64 " us, %p",
      pQueryHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, elapsedTime, pQueryHandle->qinfo);
  return blockLoaded;
}

static void handleDataMergeIfNeeded(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo){
  SQueryFilePos* cur = &pQueryHandle->cur;
  SDataBlockInfo binfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);

  /*bool hasData = */ initTableMemIterator(pQueryHandle, pCheckInfo);
  SDataRow row = getSDataRowInTableMem(pCheckInfo, pQueryHandle->order);

  TSKEY key = (row != NULL)? dataRowKey(row):TSKEY_INITIAL_VAL;
  cur->pos = ASCENDING_TRAVERSE(pQueryHandle->order)? 0:(binfo.rows-1);

  if ((ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key <= binfo.window.ekey)) ||
      (!ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key >= binfo.window.skey))) {

    if ((ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key < binfo.window.skey)) ||
        (!ASCENDING_TRAVERSE(pQueryHandle->order) && (key != TSKEY_INITIAL_VAL && key > binfo.window.ekey))) {

      // do not load file block into buffer
      int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order) ? 1 : -1;

      cur->rows = tsdbReadRowsFromCache(pCheckInfo, binfo.window.skey - step, pQueryHandle->outputCapacity, &cur->win, pQueryHandle);
      pQueryHandle->realNumOfRows = cur->rows;

      // update the last key value
      pCheckInfo->lastKey = cur->win.ekey + step;
      if (!ASCENDING_TRAVERSE(pQueryHandle->order)) {
        SWAP(cur->win.skey, cur->win.ekey, TSKEY);
      }

      cur->mixBlock = true;
      cur->blockCompleted = false;
      return;
    }

    doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot);
    doMergeTwoLevelData(pQueryHandle, pCheckInfo, pBlock);
  } else {
    /*
     * no data in cache, only load data from file
     * during the query processing, data in cache will not be checked anymore.
     *
     * Here the buffer is not enough, so only part of file block can be loaded into memory buffer
     */
    assert(pQueryHandle->outputCapacity >= binfo.rows);
    pQueryHandle->realNumOfRows = binfo.rows;

    cur->rows = binfo.rows;
    cur->win  = binfo.window;
    cur->mixBlock = false;
    cur->blockCompleted = true;
    cur->lastKey = binfo.window.ekey + (ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1);
    pCheckInfo->lastKey = cur->lastKey;
  }
}

static bool loadFileDataBlock(STsdbQueryHandle* pQueryHandle, SCompBlock* pBlock, STableCheckInfo* pCheckInfo) {
  SQueryFilePos* cur = &pQueryHandle->cur;

  if (ASCENDING_TRAVERSE(pQueryHandle->order)) {
    // query ended in/started from current block
    if (pQueryHandle->window.ekey < pBlock->keyLast || pCheckInfo->lastKey > pBlock->keyFirst) {
      if (!doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) {
        return false;
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
      handleDataMergeIfNeeded(pQueryHandle, pBlock, pCheckInfo);
    }
  } else {  //desc order, query ended in current block
    if (pQueryHandle->window.ekey > pBlock->keyFirst || pCheckInfo->lastKey < pBlock->keyLast) {
      if (!doLoadFileDataBlock(pQueryHandle, pBlock, pCheckInfo, cur->slot)) {
        return false;
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
      handleDataMergeIfNeeded(pQueryHandle, pBlock, pCheckInfo);
    }
  }

  return pQueryHandle->realNumOfRows > 0;
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

static int32_t copyDataFromFileBlock(STsdbQueryHandle* pQueryHandle, int32_t capacity, int32_t numOfRows, int32_t start, int32_t end) {
  char* pData = NULL;
  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1 : -1;

  SDataCols* pCols = pQueryHandle->rhelper.pDataCols[0];
  TSKEY* tsArray = pCols->cols[0].pData;

  int32_t num = end - start + 1;
  assert(num >= 0);

  if (num == 0) {
    return numOfRows;
  }

  int32_t requiredNumOfCols = taosArrayGetSize(pQueryHandle->pColumns);

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
      pData = pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = pColInfo->pData + (capacity - numOfRows - num) * pColInfo->info.bytes;
    }

    if (pColInfo->info.colId == src->colId) {

      if (pColInfo->info.type != TSDB_DATA_TYPE_BINARY && pColInfo->info.type != TSDB_DATA_TYPE_NCHAR) {
        memmove(pData, src->pData + bytes * start, bytes * num);
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
      pData = pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = pColInfo->pData + (capacity - numOfRows - num) * pColInfo->info.bytes;
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
      pData = pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = pColInfo->pData + (capacity - numOfRows - 1) * pColInfo->info.bytes;
    }

    if (pSchema->columns[j].colId == pColInfo->info.colId) {
      void* value = tdGetRowDataOfCol(row, pColInfo->info.type, TD_DATA_ROW_HEAD_SIZE + pSchema->columns[j].offset);
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
      pData = pColInfo->pData + numOfRows * pColInfo->info.bytes;
    } else {
      pData = pColInfo->pData + (capacity - numOfRows - 1) * pColInfo->info.bytes;
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
      memmove(pColInfo->pData, pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
    }
  }
}

static void getQualifiedRowsPos(STsdbQueryHandle* pQueryHandle, int32_t startPos, int32_t endPos,
    int32_t numOfExisted, int32_t *start, int32_t *end) {
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

  // for search the endPos, so the order needs to reverse
  int32_t order = (pQueryHandle->order == TSDB_ORDER_ASC)? TSDB_ORDER_DESC:TSDB_ORDER_ASC;

  int32_t step = ASCENDING_TRAVERSE(pQueryHandle->order)? 1:-1;
  int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);

  STable* pTable = pCheckInfo->pTableObj;
  int32_t endPos = cur->pos;

  if (ASCENDING_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey > blockInfo.window.ekey) {
    endPos = blockInfo.rows - 1;
    cur->mixBlock = (cur->pos != 0);
  } else if (!ASCENDING_TRAVERSE(pQueryHandle->order) && pQueryHandle->window.ekey < blockInfo.window.skey) {
    endPos = 0;
    cur->mixBlock = (cur->pos != blockInfo.rows - 1);
  } else {
    assert(pCols->numOfRows > 0);
    endPos = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, pQueryHandle->window.ekey, order);
    cur->mixBlock = true;
  }

  // compared with the data from in-memory buffer, to generate the correct timestamp array list
  int32_t numOfRows = 0;
  int32_t pos = cur->pos;
  cur->win = TSWINDOW_INITIALIZER;

  // no data in buffer, load data from file directly
  if (pCheckInfo->iiter == NULL && pCheckInfo->iter == NULL) {
    int32_t start = cur->pos;
    int32_t end = endPos;

    if (!ASCENDING_TRAVERSE(pQueryHandle->order)) {
      SWAP(start, end, int32_t);
    }

    numOfRows = copyDataFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, start, end);

    // the time window should always be right order: skey <= ekey
    cur->win = (STimeWindow) {.skey = tsArray[start], .ekey = tsArray[end]};
    cur->lastKey = tsArray[endPos];
    pos += (end - start + 1) * step;

    cur->blockCompleted =
        (((pos >= endPos || cur->lastKey > pQueryHandle->window.ekey) && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
         ((pos <= endPos || cur->lastKey < pQueryHandle->window.ekey) && !ASCENDING_TRAVERSE(pQueryHandle->order)));

    // if the buffer is not full in case of descending order query, move the data in the front of the buffer
    moveDataToFront(pQueryHandle, numOfRows, numOfCols);
    updateInfoAfterMerge(pQueryHandle, pCheckInfo, numOfRows, pos);
    doCheckGeneratedBlockRange(pQueryHandle);
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
        if (tsArray[end] == key) { // the value of key in cache equals to the end timestamp value, ignore it
          moveToNextRowInMem(pCheckInfo);
        }

        int32_t qstart = 0, qend = 0;
        getQualifiedRowsPos(pQueryHandle, pos, end, numOfRows, &qstart, &qend);

        numOfRows = copyDataFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, qstart, qend);
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
          ((dataRowKey(SL_GET_NODE_DATA(node)) > pQueryHandle->window.ekey) && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
          ((dataRowKey(SL_GET_NODE_DATA(node)) < pQueryHandle->window.ekey) && !ASCENDING_TRAVERSE(pQueryHandle->order))) {
        // no data in cache or data in cache is greater than the ekey of time window, load data from file block
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = tsArray[pos];
        }

        int32_t start = -1, end = -1;
        getQualifiedRowsPos(pQueryHandle, pos, endPos, numOfRows, &start, &end);

        numOfRows = copyDataFromFileBlock(pQueryHandle, pQueryHandle->outputCapacity, numOfRows, start, end);
        pos += (end - start + 1) * step;

        cur->win.ekey = ASCENDING_TRAVERSE(pQueryHandle->order)? tsArray[end]:tsArray[start];
        cur->lastKey  = cur->win.ekey + step;
      }
    }
  }

  cur->blockCompleted =
      (((pos >= endPos || cur->lastKey > pQueryHandle->window.ekey) && ASCENDING_TRAVERSE(pQueryHandle->order)) ||
       ((pos <= endPos || cur->lastKey < pQueryHandle->window.ekey) && !ASCENDING_TRAVERSE(pQueryHandle->order)));

  if (!ASCENDING_TRAVERSE(pQueryHandle->order)) {
    SWAP(cur->win.skey, cur->win.ekey, TSKEY);
  }

  moveDataToFront(pQueryHandle, numOfRows, numOfCols);
  updateInfoAfterMerge(pQueryHandle, pCheckInfo, numOfRows, pos);
  doCheckGeneratedBlockRange(pQueryHandle);

  tsdbDebug("%p uid:%" PRIu64",tid:%d data block created, brange:%"PRIu64"-%"PRIu64" rows:%d, %p", pQueryHandle, pCheckInfo->tableId.uid, pCheckInfo->tableId.tid, cur->win.skey,
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
    pQueryHandle->allocSize = size;
    char* tmp = realloc(pQueryHandle->pDataBlockInfo, pQueryHandle->allocSize);
    if (tmp == NULL) {
      return TSDB_CODE_TDB_OUT_OF_MEMORY;
    }

    pQueryHandle->pDataBlockInfo = (STableBlockInfo*) tmp;
  }

  memset(pQueryHandle->pDataBlockInfo, 0, size);
  *numOfAllocBlocks = numOfBlocks;

  int32_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

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
  int32_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

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
  *exists = loadFileDataBlock(pQueryHandle, pBlockInfo->compBlock, pBlockInfo->pTableCheckInfo);

  return TSDB_CODE_SUCCESS;
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
    if (!cur->mixBlock || cur->blockCompleted) {
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
        *exists = loadFileDataBlock(pQueryHandle, pNext->compBlock, pNext->pTableCheckInfo);

        return TSDB_CODE_SUCCESS;
      }
    } else {
      handleDataMergeIfNeeded(pQueryHandle, pBlockInfo->compBlock, pCheckInfo);
      *exists = pQueryHandle->realNumOfRows > 0;

      return TSDB_CODE_SUCCESS;
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

    /*SDataBlockInfo* pBlockInfo =*/ tsdbRetrieveDataBlockInfo(pHandle, &blockInfo);
    /*SArray *pDataBlock = */tsdbRetrieveDataBlock(pHandle, pQueryHandle->defaultLoadColumn);

    if (pQueryHandle->cur.win.ekey == pQueryHandle->window.skey) {
      // data already retrieve, discard other data rows and return
      int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);
      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
        memcpy(pCol->pData, pCol->pData + pCol->info.bytes * (pQueryHandle->cur.rows-1), pCol->info.bytes);
      }

      pQueryHandle->cur.win  = (STimeWindow){pQueryHandle->window.skey, pQueryHandle->window.skey};
      pQueryHandle->window   = pQueryHandle->cur.win;
      pQueryHandle->cur.rows = 1;
      pQueryHandle->type = TSDB_QUERY_TYPE_ALL;
      return true;
    } else {
      STsdbQueryHandle* pSecQueryHandle = calloc(1, sizeof(STsdbQueryHandle));
      pSecQueryHandle->order       = TSDB_ORDER_ASC;
      pSecQueryHandle->window      = (STimeWindow) {pQueryHandle->window.skey, INT64_MAX};
      pSecQueryHandle->pTsdb       = pQueryHandle->pTsdb;
      pSecQueryHandle->type        = TSDB_QUERY_TYPE_ALL;
      pSecQueryHandle->cur.fid     = -1;
      pSecQueryHandle->cur.win     = TSWINDOW_INITIALIZER;
      pSecQueryHandle->checkFiles  = true;
      pSecQueryHandle->activeIndex = 0;
      pSecQueryHandle->outputCapacity = ((STsdbRepo*)pSecQueryHandle->pTsdb)->config.maxRowsPerFileBlock;

      if (tsdbInitReadHelper(&pSecQueryHandle->rhelper, (STsdbRepo*) pSecQueryHandle->pTsdb) != 0) {
        free(pSecQueryHandle);
        return false;
      }

      tsdbTakeMemSnapshot(pSecQueryHandle->pTsdb, &pSecQueryHandle->mem, &pSecQueryHandle->imem);

      // allocate buffer in order to load data blocks from file
      int32_t numOfCols = QH_GET_NUM_OF_COLS(pQueryHandle);

      pSecQueryHandle->statis = calloc(numOfCols, sizeof(SDataStatis));
      pSecQueryHandle->pColumns = taosArrayInit(numOfCols, sizeof(SColumnInfoData));

      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData colInfo = {{0}, 0};
        SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);

        colInfo.info = pCol->info;
        colInfo.pData = calloc(1, EXTRA_BYTES + pQueryHandle->outputCapacity * pCol->info.bytes);
        taosArrayPush(pSecQueryHandle->pColumns, &colInfo);
      }

      size_t si = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
      pSecQueryHandle->pTableCheckInfo = taosArrayInit(si, sizeof(STableCheckInfo));
      STsdbMeta* pMeta = tsdbGetMeta(pQueryHandle->pTsdb);
      assert(pMeta != NULL);

      for (int32_t j = 0; j < si; ++j) {
        STableCheckInfo* pCheckInfo = (STableCheckInfo*) taosArrayGet(pQueryHandle->pTableCheckInfo, j);
        STableCheckInfo info = {
            .lastKey = pSecQueryHandle->window.skey,
            .tableId = pCheckInfo->tableId,
            .pTableObj = pCheckInfo->pTableObj,
        };

        taosArrayPush(pSecQueryHandle->pTableCheckInfo, &info);
      }

      tsdbInitDataBlockLoadInfo(&pSecQueryHandle->dataBlockLoadInfo);
      tsdbInitCompBlockLoadInfo(&pSecQueryHandle->compBlockLoadInfo);
      pSecQueryHandle->defaultLoadColumn = taosArrayClone(pQueryHandle->defaultLoadColumn);

      bool ret = tsdbNextDataBlock((void*) pSecQueryHandle);
      assert(ret);

      tsdbRetrieveDataBlockInfo((void*) pSecQueryHandle, &blockInfo);
      tsdbRetrieveDataBlock((void*) pSecQueryHandle, pSecQueryHandle->defaultLoadColumn);

      for (int32_t i = 0; i < numOfCols; ++i) {
        SColumnInfoData* pCol = taosArrayGet(pQueryHandle->pColumns, i);
        memcpy(pCol->pData, pCol->pData + pCol->info.bytes * (pQueryHandle->cur.rows-1), pCol->info.bytes);

        SColumnInfoData* pCol1 = taosArrayGet(pSecQueryHandle->pColumns, i);
        assert(pCol->info.colId == pCol1->info.colId);

        memcpy(pCol->pData + pCol->info.bytes, pCol1->pData, pCol1->info.bytes);
      }

      SColumnInfoData* pTSCol = taosArrayGet(pQueryHandle->pColumns, 0);

      // it is ascending order
      pQueryHandle->cur.win  = (STimeWindow){((TSKEY*)pTSCol->pData)[0], ((TSKEY*)pTSCol->pData)[1]};
      pQueryHandle->window   = pQueryHandle->cur.win;
      pQueryHandle->cur.rows = 2;
      pQueryHandle->cur.mixBlock = true;
      pQueryHandle->order = TSDB_ORDER_DESC;

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
    bool exists = true;
    int32_t code = getDataBlocksInFiles(pQueryHandle, &exists);
    if (code != TSDB_CODE_SUCCESS) {
      return false;
    }

    if (exists) {
      elapsedTime = taosGetTimestampUs() - stime;
      pQueryHandle->cost.checkForNextTime += elapsedTime;
      return exists;
    }

    pQueryHandle->activeIndex = 0;
    pQueryHandle->checkFiles  = false;
  }

  // TODO: opt by consider the scan order
  bool ret = doHasDataInBuffer(pQueryHandle);
  terrno = TSDB_CODE_SUCCESS;

  elapsedTime = taosGetTimestampUs() - stime;
  pQueryHandle->cost.checkForNextTime += elapsedTime;
  return ret;
}

void changeQueryHandleForLastrowQuery(TsdbQueryHandleT pqHandle) {
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pqHandle;
  assert(!ASCENDING_TRAVERSE(pQueryHandle->order));

  // starts from the buffer in case of descending timestamp order check data blocks

  // todo consider the query time window, current last_row does not apply the query time window
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  TSKEY key = TSKEY_INITIAL_VAL;
  int32_t index = -1;

  for(int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    if (pCheckInfo->pTableObj->lastKey > key) {
      key = pCheckInfo->pTableObj->lastKey;
      index = i;
    }
  }

  if (index == -1) {
    // todo add failure test cases
    return;
  }

  // erase all other elements in array list
  size_t size = taosArrayGetSize(pQueryHandle->pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    if (i == index) {
      continue;
    }

    STableCheckInfo* pTableCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
    tSkipListDestroyIter(pTableCheckInfo->iter);

    if (pTableCheckInfo->pDataCols != NULL) {
      taosTFree(pTableCheckInfo->pDataCols->buf);
    }

    taosTFree(pTableCheckInfo->pDataCols);
    taosTFree(pTableCheckInfo->pCompInfo);
  }

  STableCheckInfo info = *(STableCheckInfo*) taosArrayGet(pQueryHandle->pTableCheckInfo, index);
  taosArrayClear(pQueryHandle->pTableCheckInfo);

  info.lastKey = key;
  taosArrayPush(pQueryHandle->pTableCheckInfo, &info);

  // update the query time window according to the chosen last timestamp
  pQueryHandle->window = (STimeWindow) {key, key};
}

static void changeQueryHandleForInterpQuery(TsdbQueryHandleT pHandle) {
  // filter the queried time stamp in the first place
  STsdbQueryHandle* pQueryHandle = (STsdbQueryHandle*) pHandle;
  pQueryHandle->order = TSDB_ORDER_DESC;

  assert(pQueryHandle->window.skey == pQueryHandle->window.ekey);

  // starts from the buffer in case of descending timestamp order check data blocks
  // todo consider the query time window, current last_row does not apply the query time window
  size_t numOfTables = taosArrayGetSize(pQueryHandle->pTableCheckInfo);

  int32_t i = 0;
  while(i < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pQueryHandle->pTableCheckInfo, i);
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
  int32_t numOfCols = taosArrayGetSize(pQueryHandle->pColumns);
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
      memmove(pColInfo->pData, pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
    }
  }

  int64_t elapsedTime = taosGetTimestampUs() - st;
  tsdbDebug("%p build data block from cache completed, elapsed time:%"PRId64" us, numOfRows:%d, numOfCols:%d, %p", pQueryHandle,
            elapsedTime, numOfRows, numOfCols, pQueryHandle->qinfo);

  return numOfRows;
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
  pDataBlockInfo->numOfCols = QH_GET_NUM_OF_COLS(pHandle);
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

  tsdbGetDataStatis(&pHandle->rhelper, pHandle->statis, numOfCols);

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
        doLoadFileDataBlock(pHandle, pBlock, pCheckInfo, pHandle->cur.slot);

        // todo refactor
        int32_t numOfRows = copyDataFromFileBlock(pHandle, pHandle->outputCapacity, 0, 0, pBlock->numOfRows - 1);

        // if the buffer is not full in case of descending order query, move the data in the front of the buffer
        if (!ASCENDING_TRAVERSE(pHandle->order) && numOfRows < pHandle->outputCapacity) {
          int32_t emptySize = pHandle->outputCapacity - numOfRows;
          int32_t reqNumOfCols = taosArrayGetSize(pHandle->pColumns);

          for(int32_t i = 0; i < reqNumOfCols; ++i) {
            SColumnInfoData* pColInfo = taosArrayGet(pHandle->pColumns, i);
            memmove(pColInfo->pData, pColInfo->pData + emptySize * pColInfo->info.bytes, numOfRows * pColInfo->info.bytes);
          }
        }

        return pHandle->pColumns;
      }
    }
  }
}

static int32_t getAllTableList(STable* pSuperTable, SArray* list) {
  SSkipListIterator* iter = tSkipListCreateIter(pSuperTable->pIndex);
  while (tSkipListIterNext(iter)) {
    SSkipListNode* pNode = tSkipListIterGet(iter);

    STable** pTable = (STable**) SL_GET_NODE_DATA((SSkipListNode*) pNode);
    taosArrayPush(list, pTable);
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

//  tVariantDestroy(&(pInfo->q));
  free(param);
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
  pInfo->param    = pTSSchema;

  if (pInfo->optr == TSDB_RELATION_IN) {
    pInfo->q = (char*) pCond->arr;
  } else {
    pInfo->q = calloc(1, pSchema->bytes);
    tVariantDump(pCond, pInfo->q, pSchema->type, true);
  }
}

typedef struct STableGroupSupporter {
  int32_t    numOfCols;
  SColIndex* pCols;
  STSchema*  pTagSchema;
} STableGroupSupporter;

int32_t tableGroupComparFn(const void *p1, const void *p2, const void *param) {
  STableGroupSupporter* pTableGroupSupp = (STableGroupSupporter*) param;
  STable* pTable1 = *(STable**) p1;
  STable* pTable2 = *(STable**) p2;

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

void createTableGroupImpl(SArray* pGroups, SArray* pTableList, size_t numOfTables, STableGroupSupporter* pSupp,
    __ext_compar_fn_t compareFn) {
  STable* pTable = taosArrayGetP(pTableList, 0);

  SArray* g = taosArrayInit(16, POINTER_BYTES);
  taosArrayPush(g, &pTable);
  tsdbRefTable(pTable);

  for (int32_t i = 1; i < numOfTables; ++i) {
    STable** prev = taosArrayGet(pTableList, i - 1);
    STable** p = taosArrayGet(pTableList, i);

    int32_t ret = compareFn(prev, p, pSupp);
    assert(ret == 0 || ret == -1);

    tsdbRefTable(*p);
    assert((*p)->type == TSDB_CHILD_TABLE);

    if (ret == 0) {
      taosArrayPush(g, p);
    } else {
      taosArrayPush(pGroups, &g);  // current group is ended, start a new group
      g = taosArrayInit(16, POINTER_BYTES);
      taosArrayPush(g, p);
    }
  }

  taosArrayPush(pGroups, &g);
}

SArray* createTableGroup(SArray* pTableList, STSchema* pTagSchema, SColIndex* pCols, int32_t numOfOrderCols) {
  assert(pTableList != NULL);
  SArray* pTableGroup = taosArrayInit(1, POINTER_BYTES);

  size_t size = taosArrayGetSize(pTableList);
  if (size == 0) {
    tsdbDebug("no qualified tables");
    return pTableGroup;
  }

  if (numOfOrderCols == 0 || size == 1) { // no group by tags clause or only one table
    SArray* sa = taosArrayInit(size, POINTER_BYTES);
    for(int32_t i = 0; i < size; ++i) {
      STable** pTable = taosArrayGet(pTableList, i);
      assert((*pTable)->type == TSDB_CHILD_TABLE);

      tsdbRefTable(*pTable);
      taosArrayPush(sa, pTable);
    }

    taosArrayPush(pTableGroup, &sa);
    tsdbDebug("all %zu tables belong to one group", size);
  } else {
    STableGroupSupporter *pSupp = (STableGroupSupporter *) calloc(1, sizeof(STableGroupSupporter));
    pSupp->numOfCols = numOfOrderCols;
    pSupp->pTagSchema = pTagSchema;
    pSupp->pCols = pCols;

    taosqsort(pTableList->pData, size, POINTER_BYTES, pSupp, tableGroupComparFn);
    createTableGroupImpl(pTableGroup, pTableList, size, pSupp, tableGroupComparFn);
    taosTFree(pSupp);
  }

  return pTableGroup;
}

bool indexedNodeFilterFp(const void* pNode, void* param) {
  tQueryInfo* pInfo = (tQueryInfo*) param;

  STable* pTable = *(STable**)(SL_GET_NODE_DATA((SSkipListNode*)pNode));

  char*  val = NULL;

  if (pInfo->sch.colId == TSDB_TBNAME_COLUMN_INDEX) {
    val = (char*) TABLE_NAME(pTable);
  } else {
    val = tdGetKVRowValOfCol(pTable->tagVal, pInfo->sch.colId);
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

int32_t tsdbQuerySTableByTagCond(TSDB_REPO_T* tsdb, uint64_t uid, const char* pTagCond, size_t len,
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
  SArray* res = taosArrayInit(8, POINTER_BYTES);
  STSchema* pTagSchema = tsdbGetTableTagSchema(pTable);

  // no tags and tbname condition, all child tables of this stable are involved
  if (tbnameCond == NULL && (pTagCond == NULL || len == 0)) {
    int32_t ret = getAllTableList(pTable, res);
    if (ret != TSDB_CODE_SUCCESS) {
      tsdbUnlockRepoMeta(tsdb);
      goto _error;
    }

    pGroupInfo->numOfTables = taosArrayGetSize(res);
    pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols);

    tsdbDebug("%p no table name/tag condition, all tables belong to one group, numOfTables:%zu", tsdb, pGroupInfo->numOfTables);
    taosArrayDestroy(res);

    if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;
    return ret;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  tExprNode* expr = NULL;

  TRY(TSDB_MAX_TAGS) {
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
        expr->_node.optr = tagNameRelType;
        expr->_node.pLeft = tagExpr;
        expr->_node.pRight = tbnameExpr;
      }
    }
    CLEANUP_EXECUTE();

  } CATCH( code ) {
    CLEANUP_EXECUTE();
    terrno = code;
    goto _error;
    // TODO: more error handling
  } END_TRY

  doQueryTableList(pTable, res, expr);
  pGroupInfo->numOfTables = taosArrayGetSize(res);
  pGroupInfo->pGroupList  = createTableGroup(res, pTagSchema, pColIndex, numOfCols);

  tsdbDebug("%p stable tid:%d, uid:%"PRIu64" query, numOfTables:%zu, belong to %zu groups", tsdb, pTable->tableId.tid,
      pTable->tableId.uid, pGroupInfo->numOfTables, taosArrayGetSize(pGroupInfo->pGroupList));

  taosArrayDestroy(res);

  if (tsdbUnlockRepoMeta(tsdb) < 0) goto _error;
  return ret;

  _error:
  return terrno;
}

int32_t tsdbGetOneTableGroup(TSDB_REPO_T* tsdb, uint64_t uid, STableGroupInfo* pGroupInfo) {
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

  SArray* group = taosArrayInit(1, POINTER_BYTES);

  taosArrayPush(group, &pTable);
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
  SArray* group = taosArrayInit(1, POINTER_BYTES);

  int32_t i = 0;
  for(; i < size; ++i) {
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
    taosArrayPush(group, &pTable);
  }

  if (tsdbUnlockRepoMeta(tsdb) < 0) {
    taosArrayDestroy(group);
    return terrno;
  }

  pGroupInfo->numOfTables = i;
  taosArrayPush(pGroupInfo->pGroupList, &group);

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

      if (pTableCheckInfo->pDataCols != NULL) {
        taosTFree(pTableCheckInfo->pDataCols->buf);
      }

      taosTFree(pTableCheckInfo->pDataCols);
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
  tsdbUnTakeMemSnapShot(pQueryHandle->pTsdb, pQueryHandle->mem, pQueryHandle->imem);

  tsdbDestroyHelper(&pQueryHandle->rhelper);

  SIOCostSummary* pCost = &pQueryHandle->cost;
  tsdbDebug("%p :io-cost summary: statis-info:%"PRId64"us, datablock:%" PRId64"us, check data:%"PRId64"us, %p",
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