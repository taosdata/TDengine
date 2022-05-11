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

#include "tsdb.h"

#define EXTRA_BYTES                2
#define ASCENDING_TRAVERSE(o)      (o == TSDB_ORDER_ASC)
#define QH_GET_NUM_OF_COLS(handle) ((size_t)(taosArrayGetSize((handle)->pColumns)))

#define GET_FILE_DATA_BLOCK_INFO(_checkInfo, _block)                                   \
  ((SDataBlockInfo){.window = {.skey = (_block)->keyFirst, .ekey = (_block)->keyLast}, \
                    .numOfCols = (_block)->numOfCols,                                  \
                    .rows = (_block)->numOfRows,                                       \
                    .uid = (_checkInfo)->tableId})

enum {
  TSDB_QUERY_TYPE_ALL = 1,
  TSDB_QUERY_TYPE_LAST = 2,
};

enum {
  TSDB_CACHED_TYPE_NONE = 0,
  TSDB_CACHED_TYPE_LASTROW = 1,
  TSDB_CACHED_TYPE_LAST = 2,
};

typedef struct SQueryFilePos {
  int32_t     fid;
  int32_t     slot;
  int32_t     pos;
  int64_t     lastKey;
  int32_t     rows;
  bool        mixBlock;
  bool        blockCompleted;
  STimeWindow win;
} SQueryFilePos;

typedef struct SDataBlockLoadInfo {
  SDFileSet* fileGroup;
  int32_t    slot;
  uint64_t   uid;
  SArray*    pLoadedCols;
} SDataBlockLoadInfo;

typedef struct SLoadCompBlockInfo {
  int32_t tid; /* table tid */
  int32_t fileId;
} SLoadCompBlockInfo;

enum {
  CHECKINFO_CHOSEN_MEM = 0,
  CHECKINFO_CHOSEN_IMEM = 1,
  CHECKINFO_CHOSEN_BOTH = 2  // for update=2(merge case)
};

typedef struct STableCheckInfo {
  uint64_t           tableId;
  TSKEY              lastKey;
  SBlockInfo*        pCompInfo;
  int32_t            compSize;
  int32_t            numOfBlocks : 29;  // number of qualified data blocks not the original blocks
  uint8_t            chosen : 2;        // indicate which iterator should move forward
  bool               initBuf : 1;       // whether to initialize the in-memory skip list iterator or not
  SSkipListIterator* iter;              // mem buffer skip list iterator
  SSkipListIterator* iiter;             // imem buffer skip list iterator
} STableCheckInfo;

typedef struct STableBlockInfo {
  SBlock*          compBlock;
  STableCheckInfo* pTableCheckInfo;
} STableBlockInfo;

typedef struct SBlockOrderSupporter {
  int32_t           numOfTables;
  STableBlockInfo** pDataBlockInfo;
  int32_t*          blockIndexArray;
  int32_t*          numOfBlocksPerTable;
} SBlockOrderSupporter;

typedef struct SIOCostSummary {
  int64_t blockLoadTime;
  int64_t statisInfoLoadTime;
  int64_t checkForNextTime;
  int64_t headFileLoad;
  int64_t headFileLoadTime;
} SIOCostSummary;

typedef struct SBlockLoadSuppInfo {
  SColumnDataAgg*  pstatis;
  SColumnDataAgg** plist;
  SArray*          defaultLoadColumn;  // default load column
  int32_t*         slotIds;            // colId to slotId
} SBlockLoadSuppInfo;

typedef struct STsdbReadHandle {
  STsdb*        pTsdb;
  SQueryFilePos cur;  // current position
  int16_t       order;
  STimeWindow   window;  // the primary query time window that applies to all queries
  //  SColumnDataAgg* statis;  // query level statistics, only one table block statistics info exists at any time
  //  SColumnDataAgg** pstatis;// the ptr array list to return to caller
  int32_t numOfBlocks;
  SArray* pColumns;  // column list, SColumnInfoData array list
  bool    locateStart;
  int32_t outputCapacity;
  int32_t realNumOfRows;
  SArray* pTableCheckInfo;  // SArray<STableCheckInfo>
  int32_t activeIndex;
  bool    checkFiles;               // check file stage
  int8_t  cachelastrow;             // check if last row cached
  bool    loadExternalRow;          // load time window external data rows
  bool    currentLoadExternalRows;  // current load external rows
  int32_t loadType;                 // block load type
  char*   idStr;                    // query info handle, for debug purpose
  int32_t type;  // query type: retrieve all data blocks, 2. retrieve only last row, 3. retrieve direct prev|next rows
  SDFileSet*         pFileGroup;
  SFSIter            fileIter;
  SReadH             rhelper;
  STableBlockInfo*   pDataBlockInfo;
  SDataCols*         pDataCols;         // in order to hold current file data block
  int32_t            allocSize;         // allocated data block size
  SDataBlockLoadInfo dataBlockLoadInfo; /* record current block load information */
  SLoadCompBlockInfo compBlockLoadInfo; /* record current compblock information in SQueryAttr */
  SBlockLoadSuppInfo suppInfo;
  SArray*            prev;  // previous row which is before than time window
  SArray*            next;  // next row which is after the query time window
  SIOCostSummary     cost;
  STSchema*          pSchema;
} STsdbReadHandle;

typedef struct STableGroupSupporter {
  int32_t    numOfCols;
  SColIndex* pCols;
  SSchema*   pTagSchema;
} STableGroupSupporter;

int32_t tsdbQueryTableList(void* pMeta, SArray* pRes, void* filterInfo);

static STimeWindow updateLastrowForEachGroup(STableGroupInfo* groupList);
static int32_t     checkForCachedLastRow(STsdbReadHandle* pTsdbReadHandle, STableGroupInfo* groupList);
static int32_t     checkForCachedLast(STsdbReadHandle* pTsdbReadHandle);
// static int32_t tsdbGetCachedLastRow(STable* pTable, STSRow** pRes, TSKEY* lastKey);

static void    changeQueryHandleForInterpQuery(tsdbReaderT pHandle);
static void    doMergeTwoLevelData(STsdbReadHandle* pTsdbReadHandle, STableCheckInfo* pCheckInfo, SBlock* pBlock);
static int32_t tsdbReadRowsFromCache(STableCheckInfo* pCheckInfo, TSKEY maxKey, int maxRowsToRead, STimeWindow* win,
                                     STsdbReadHandle* pTsdbReadHandle);
static int32_t tsdbCheckInfoCompar(const void* key1, const void* key2);
// static int32_t doGetExternalRow(STsdbReadHandle* pTsdbReadHandle, int16_t type, void* pMemRef);
// static void*   doFreeColumnInfoData(SArray* pColumnInfoData);
// static void*   destroyTableCheckInfo(SArray* pTableCheckInfo);
static bool tsdbGetExternalRow(tsdbReaderT pHandle);

static STsdb* getTsdbByRetentions(SVnode* pVnode, STsdbReadHandle* pReadHandle, TSKEY winSKey, SRetention* retentions);

static void tsdbInitDataBlockLoadInfo(SDataBlockLoadInfo* pBlockLoadInfo) {
  pBlockLoadInfo->slot = -1;
  pBlockLoadInfo->uid = 0;
  pBlockLoadInfo->fileGroup = NULL;
}

static void tsdbInitCompBlockLoadInfo(SLoadCompBlockInfo* pCompBlockLoadInfo) {
  pCompBlockLoadInfo->tid = -1;
  pCompBlockLoadInfo->fileId = -1;
}

static SArray* getColumnIdList(STsdbReadHandle* pTsdbReadHandle) {
  size_t numOfCols = QH_GET_NUM_OF_COLS(pTsdbReadHandle);
  assert(numOfCols <= TSDB_MAX_COLUMNS);

  SArray* pIdList = taosArrayInit(numOfCols, sizeof(int16_t));
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pTsdbReadHandle->pColumns, i);
    taosArrayPush(pIdList, &pCol->info.colId);
  }

  return pIdList;
}

static SArray* getDefaultLoadColumns(STsdbReadHandle* pTsdbReadHandle, bool loadTS) {
  SArray* pLocalIdList = getColumnIdList(pTsdbReadHandle);

  // check if the primary time stamp column needs to load
  int16_t colId = *(int16_t*)taosArrayGet(pLocalIdList, 0);

  // the primary timestamp column does not be included in the the specified load column list, add it
  if (loadTS && colId != PRIMARYKEY_TIMESTAMP_COL_ID) {
    int16_t columnId = PRIMARYKEY_TIMESTAMP_COL_ID;
    taosArrayInsert(pLocalIdList, 0, &columnId);
  }

  return pLocalIdList;
}

int64_t tsdbGetNumOfRowsInMemTable(tsdbReaderT* pHandle) {
  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)pHandle;

  int64_t        rows = 0;
  STsdbMemTable* pMemTable = NULL;  // pTsdbReadHandle->pMemTable;
  if (pMemTable == NULL) {
    return rows;
  }

  //  STableData* pMem  = NULL;
  //  STableData* pIMem = NULL;

  //  SMemTable* pMemT = pMemRef->snapshot.mem;
  //  SMemTable* pIMemT = pMemRef->snapshot.imem;

  size_t size = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);

    //    if (pMemT && pCheckInfo->tableId < pMemT->maxTables) {
    //      pMem = pMemT->tData[pCheckInfo->tableId];
    //      rows += (pMem && pMem->uid == pCheckInfo->tableId) ? pMem->numOfRows : 0;
    //    }
    //    if (pIMemT && pCheckInfo->tableId < pIMemT->maxTables) {
    //      pIMem = pIMemT->tData[pCheckInfo->tableId];
    //      rows += (pIMem && pIMem->uid == pCheckInfo->tableId) ? pIMem->numOfRows : 0;
    //    }
  }
  return rows;
}

static SArray* createCheckInfoFromTableGroup(STsdbReadHandle* pTsdbReadHandle, STableGroupInfo* pGroupList) {
  size_t numOfGroup = taosArrayGetSize(pGroupList->pGroupList);
  assert(numOfGroup >= 1);

  // allocate buffer in order to load data blocks from file
  SArray* pTableCheckInfo = taosArrayInit(pGroupList->numOfTables, sizeof(STableCheckInfo));
  if (pTableCheckInfo == NULL) {
    return NULL;
  }

  // todo apply the lastkey of table check to avoid to load header file
  for (int32_t i = 0; i < numOfGroup; ++i) {
    SArray* group = *(SArray**)taosArrayGet(pGroupList->pGroupList, i);

    size_t gsize = taosArrayGetSize(group);
    assert(gsize > 0);

    for (int32_t j = 0; j < gsize; ++j) {
      STableKeyInfo* pKeyInfo = (STableKeyInfo*)taosArrayGet(group, j);

      STableCheckInfo info = {.lastKey = pKeyInfo->lastKey, .tableId = pKeyInfo->uid};
      if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
        if (info.lastKey == INT64_MIN || info.lastKey < pTsdbReadHandle->window.skey) {
          info.lastKey = pTsdbReadHandle->window.skey;
        }

        assert(info.lastKey >= pTsdbReadHandle->window.skey && info.lastKey <= pTsdbReadHandle->window.ekey);
      } else {
        info.lastKey = pTsdbReadHandle->window.skey;
      }

      taosArrayPush(pTableCheckInfo, &info);
      tsdbDebug("%p check table uid:%" PRId64 " from lastKey:%" PRId64 " %s", pTsdbReadHandle, info.tableId,
                info.lastKey, pTsdbReadHandle->idStr);
    }
  }

  // TODO  group table according to the tag value.
  taosArraySort(pTableCheckInfo, tsdbCheckInfoCompar);
  return pTableCheckInfo;
}

static void resetCheckInfo(STsdbReadHandle* pTsdbReadHandle) {
  size_t numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
  assert(numOfTables >= 1);

  // todo apply the lastkey of table check to avoid to load header file
  for (int32_t i = 0; i < numOfTables; ++i) {
    STableCheckInfo* pCheckInfo = (STableCheckInfo*)taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);
    pCheckInfo->lastKey = pTsdbReadHandle->window.skey;
    pCheckInfo->iter = tSkipListDestroyIter(pCheckInfo->iter);
    pCheckInfo->iiter = tSkipListDestroyIter(pCheckInfo->iiter);
    pCheckInfo->initBuf = false;

    if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
      assert(pCheckInfo->lastKey >= pTsdbReadHandle->window.skey);
    } else {
      assert(pCheckInfo->lastKey <= pTsdbReadHandle->window.skey);
    }
  }
}

// only one table, not need to sort again
static SArray* createCheckInfoFromCheckInfo(STableCheckInfo* pCheckInfo, TSKEY skey, SArray** psTable) {
  SArray* pNew = taosArrayInit(1, sizeof(STableCheckInfo));

  STableCheckInfo info = {.lastKey = skey};

  info.tableId = pCheckInfo->tableId;
  taosArrayPush(pNew, &info);
  return pNew;
}

static bool emptyQueryTimewindow(STsdbReadHandle* pTsdbReadHandle) {
  assert(pTsdbReadHandle != NULL);

  STimeWindow* w = &pTsdbReadHandle->window;
  bool         asc = ASCENDING_TRAVERSE(pTsdbReadHandle->order);

  return ((asc && w->skey > w->ekey) || (!asc && w->ekey > w->skey));
}

// Update the query time window according to the data time to live(TTL) information, in order to avoid to return
// the expired data to client, even it is queried already.
static int64_t getEarliestValidTimestamp(STsdb* pTsdb) {
  STsdbKeepCfg* pCfg = REPO_KEEP_CFG(pTsdb);

  int64_t now = taosGetTimestamp(pCfg->precision);
  return now - (tsTickPerDay[pCfg->precision] * pCfg->keep2) + 1;  // needs to add one tick
}

static void setQueryTimewindow(STsdbReadHandle* pTsdbReadHandle, SQueryTableDataCond* pCond) {
  pTsdbReadHandle->window = pCond->twindow;

  bool    updateTs = false;
  int64_t startTs = getEarliestValidTimestamp(pTsdbReadHandle->pTsdb);
  if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
    if (startTs > pTsdbReadHandle->window.skey) {
      pTsdbReadHandle->window.skey = startTs;
      pCond->twindow.skey = startTs;
      updateTs = true;
    }
  } else {
    if (startTs > pTsdbReadHandle->window.ekey) {
      pTsdbReadHandle->window.ekey = startTs;
      pCond->twindow.ekey = startTs;
      updateTs = true;
    }
  }

  if (updateTs) {
    tsdbDebug("%p update the query time window, old:%" PRId64 " - %" PRId64 ", new:%" PRId64 " - %" PRId64 ", %s",
              pTsdbReadHandle, pCond->twindow.skey, pCond->twindow.ekey, pTsdbReadHandle->window.skey,
              pTsdbReadHandle->window.ekey, pTsdbReadHandle->idStr);
  }
}

static STsdb* getTsdbByRetentions(SVnode* pVnode, STsdbReadHandle* pReadHandle, TSKEY winSKey, SRetention* retentions) {
  if (vnodeIsRollup(pVnode)) {
    int     level = 0;
    int64_t now = taosGetTimestamp(pVnode->config.tsdbCfg.precision);

    for (int i = 0; i < TSDB_RETENTION_MAX; ++i) {
      SRetention* pRetention = retentions + level;
      if (pRetention->keep <= 0) {
        if (level > 0) {
          --level;
        }
        break;
      }
      if ((now - pRetention->keep) <= winSKey) {
        break;
      }
      ++level;
    }

    if (level == TSDB_RETENTION_L0) {
      tsdbDebug("%p rsma level %d is selected to query", pReadHandle, TSDB_RETENTION_L0);
      return VND_RSMA0(pVnode);
    } else if (level == TSDB_RETENTION_L1) {
      tsdbDebug("%p rsma level %d is selected to query", pReadHandle, TSDB_RETENTION_L1);
      return VND_RSMA1(pVnode);
    } else {
      tsdbDebug("%p rsma level %d is selected to query", pReadHandle, TSDB_RETENTION_L2);
      return VND_RSMA2(pVnode);
    }
  }
  return VND_TSDB(pVnode);
}

static STsdbReadHandle* tsdbQueryTablesImpl(SVnode* pVnode, SQueryTableDataCond* pCond, uint64_t qId, uint64_t taskId) {
  STsdbReadHandle* pReadHandle = taosMemoryCalloc(1, sizeof(STsdbReadHandle));
  if (pReadHandle == NULL) {
    goto _end;
  }

  STsdb* pTsdb = getTsdbByRetentions(pVnode, pReadHandle, pCond->twindow.skey, pVnode->config.tsdbCfg.retentions);

  pReadHandle->order = pCond->order;
  pReadHandle->pTsdb = pTsdb;
  pReadHandle->type = TSDB_QUERY_TYPE_ALL;
  pReadHandle->cur.fid = INT32_MIN;
  pReadHandle->cur.win = TSWINDOW_INITIALIZER;
  pReadHandle->checkFiles = true;
  pReadHandle->activeIndex = 0;  // current active table index
  pReadHandle->allocSize = 0;
  pReadHandle->locateStart = false;
  pReadHandle->loadType = pCond->type;

  pReadHandle->outputCapacity = 4096;  //((STsdb*)tsdb)->config.maxRowsPerFileBlock;
  pReadHandle->loadExternalRow = pCond->loadExternalRows;
  pReadHandle->currentLoadExternalRows = pCond->loadExternalRows;

  char buf[128] = {0};
  snprintf(buf, tListLen(buf), "TID:0x%" PRIx64 " QID:0x%" PRIx64, taskId, qId);
  pReadHandle->idStr = strdup(buf);

  if (tsdbInitReadH(&pReadHandle->rhelper, pReadHandle->pTsdb) != 0) {
    goto _end;
  }

  assert(pCond != NULL);
  setQueryTimewindow(pReadHandle, pCond);

  if (pCond->numOfCols > 0) {
    // allocate buffer in order to load data blocks from file
    pReadHandle->suppInfo.pstatis = taosMemoryCalloc(pCond->numOfCols, sizeof(SColumnDataAgg));
    if (pReadHandle->suppInfo.pstatis == NULL) {
      goto _end;
    }

    // todo: use list instead of array?
    pReadHandle->pColumns = taosArrayInit(pCond->numOfCols, sizeof(SColumnInfoData));
    if (pReadHandle->pColumns == NULL) {
      goto _end;
    }

    for (int32_t i = 0; i < pCond->numOfCols; ++i) {
      SColumnInfoData colInfo = {{0}, 0};
      colInfo.info = pCond->colList[i];

      int32_t code = colInfoDataEnsureCapacity(&colInfo, 0, pReadHandle->outputCapacity);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }

      taosArrayPush(pReadHandle->pColumns, &colInfo);
    }

    pReadHandle->suppInfo.defaultLoadColumn = getDefaultLoadColumns(pReadHandle, true);
    pReadHandle->suppInfo.slotIds =
        taosMemoryMalloc(sizeof(int32_t) * taosArrayGetSize(pReadHandle->suppInfo.defaultLoadColumn));
    pReadHandle->suppInfo.plist =
        taosMemoryCalloc(taosArrayGetSize(pReadHandle->suppInfo.defaultLoadColumn), POINTER_BYTES);
  }

  pReadHandle->pDataCols = tdNewDataCols(1000, pVnode->config.tsdbCfg.maxRows);
  if (pReadHandle->pDataCols == NULL) {
    tsdbError("%p failed to malloc buf for pDataCols, %s", pReadHandle, pReadHandle->idStr);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _end;
  }

  tsdbInitDataBlockLoadInfo(&pReadHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pReadHandle->compBlockLoadInfo);

  return (tsdbReaderT)pReadHandle;

_end:
  tsdbCleanupReadHandle(pReadHandle);
  terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  return NULL;
}

tsdbReaderT* tsdbQueryTables(SVnode* pVnode, SQueryTableDataCond* pCond, STableGroupInfo* groupList, uint64_t qId,
                             uint64_t taskId) {
  STsdbReadHandle* pTsdbReadHandle = tsdbQueryTablesImpl(pVnode, pCond, qId, taskId);
  if (pTsdbReadHandle == NULL) {
    return NULL;
  }

  if (emptyQueryTimewindow(pTsdbReadHandle)) {
    return (tsdbReaderT*)pTsdbReadHandle;
  }

  // todo apply the lastkey of table check to avoid to load header file
  pTsdbReadHandle->pTableCheckInfo = createCheckInfoFromTableGroup(pTsdbReadHandle, groupList);
  if (pTsdbReadHandle->pTableCheckInfo == NULL) {
    //    tsdbCleanupReadHandle(pTsdbReadHandle);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    return NULL;
  }

  STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, 0);

  pTsdbReadHandle->pSchema = metaGetTbTSchema(pVnode->pMeta, pCheckInfo->tableId, 0);
  int32_t  numOfCols = taosArrayGetSize(pTsdbReadHandle->suppInfo.defaultLoadColumn);
  int16_t* ids = pTsdbReadHandle->suppInfo.defaultLoadColumn->pData;

  STSchema* pSchema = pTsdbReadHandle->pSchema;

  int32_t i = 0, j = 0;
  while (i < numOfCols && j < pSchema->numOfCols) {
    if (ids[i] == pSchema->columns[j].colId) {
      pTsdbReadHandle->suppInfo.slotIds[i] = j;
      i++;
      j++;
    } else if (ids[i] > pSchema->columns[j].colId) {
      j++;
    } else {
      //    tsdbCleanupReadHandle(pTsdbReadHandle);
      terrno = TSDB_CODE_INVALID_PARA;
      return NULL;
    }
  }

  tsdbDebug("%p total numOfTable:%" PRIzu " in this query, group %" PRIzu " %s", pTsdbReadHandle,
            taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo), taosArrayGetSize(groupList->pGroupList),
            pTsdbReadHandle->idStr);

  return (tsdbReaderT)pTsdbReadHandle;
}

void tsdbResetReadHandle(tsdbReaderT queryHandle, SQueryTableDataCond* pCond) {
  STsdbReadHandle* pTsdbReadHandle = queryHandle;

  if (emptyQueryTimewindow(pTsdbReadHandle)) {
    if (pCond->order != pTsdbReadHandle->order) {
      pTsdbReadHandle->order = pCond->order;
      TSWAP(pTsdbReadHandle->window.skey, pTsdbReadHandle->window.ekey);
    }

    return;
  }

  pTsdbReadHandle->order = pCond->order;
  pTsdbReadHandle->window = pCond->twindow;
  pTsdbReadHandle->type = TSDB_QUERY_TYPE_ALL;
  pTsdbReadHandle->cur.fid = -1;
  pTsdbReadHandle->cur.win = TSWINDOW_INITIALIZER;
  pTsdbReadHandle->checkFiles = true;
  pTsdbReadHandle->activeIndex = 0;  // current active table index
  pTsdbReadHandle->locateStart = false;
  pTsdbReadHandle->loadExternalRow = pCond->loadExternalRows;

  if (ASCENDING_TRAVERSE(pCond->order)) {
    assert(pTsdbReadHandle->window.skey <= pTsdbReadHandle->window.ekey);
  } else {
    assert(pTsdbReadHandle->window.skey >= pTsdbReadHandle->window.ekey);
  }

  // allocate buffer in order to load data blocks from file
  memset(pTsdbReadHandle->suppInfo.pstatis, 0, sizeof(SColumnDataAgg));
  memset(pTsdbReadHandle->suppInfo.plist, 0, POINTER_BYTES);

  tsdbInitDataBlockLoadInfo(&pTsdbReadHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pTsdbReadHandle->compBlockLoadInfo);

  resetCheckInfo(pTsdbReadHandle);
}

void tsdbResetQueryHandleForNewTable(tsdbReaderT queryHandle, SQueryTableDataCond* pCond, STableGroupInfo* groupList) {
  STsdbReadHandle* pTsdbReadHandle = queryHandle;

  pTsdbReadHandle->order = pCond->order;
  pTsdbReadHandle->window = pCond->twindow;
  pTsdbReadHandle->type = TSDB_QUERY_TYPE_ALL;
  pTsdbReadHandle->cur.fid = -1;
  pTsdbReadHandle->cur.win = TSWINDOW_INITIALIZER;
  pTsdbReadHandle->checkFiles = true;
  pTsdbReadHandle->activeIndex = 0;  // current active table index
  pTsdbReadHandle->locateStart = false;
  pTsdbReadHandle->loadExternalRow = pCond->loadExternalRows;

  if (ASCENDING_TRAVERSE(pCond->order)) {
    assert(pTsdbReadHandle->window.skey <= pTsdbReadHandle->window.ekey);
  } else {
    assert(pTsdbReadHandle->window.skey >= pTsdbReadHandle->window.ekey);
  }

  // allocate buffer in order to load data blocks from file
  memset(pTsdbReadHandle->suppInfo.pstatis, 0, sizeof(SColumnDataAgg));
  memset(pTsdbReadHandle->suppInfo.plist, 0, POINTER_BYTES);

  tsdbInitDataBlockLoadInfo(&pTsdbReadHandle->dataBlockLoadInfo);
  tsdbInitCompBlockLoadInfo(&pTsdbReadHandle->compBlockLoadInfo);

  SArray* pTable = NULL;
  //  STsdbMeta* pMeta = tsdbGetMeta(pTsdbReadHandle->pTsdb);

  //  pTsdbReadHandle->pTableCheckInfo = destroyTableCheckInfo(pTsdbReadHandle->pTableCheckInfo);

  pTsdbReadHandle->pTableCheckInfo = NULL;  // createCheckInfoFromTableGroup(pTsdbReadHandle, groupList, pMeta,
                                            // &pTable);
  if (pTsdbReadHandle->pTableCheckInfo == NULL) {
    //    tsdbCleanupReadHandle(pTsdbReadHandle);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  //  pTsdbReadHandle->prev = doFreeColumnInfoData(pTsdbReadHandle->prev);
  //  pTsdbReadHandle->next = doFreeColumnInfoData(pTsdbReadHandle->next);
}

tsdbReaderT tsdbQueryLastRow(SVnode* pVnode, SQueryTableDataCond* pCond, STableGroupInfo* groupList, uint64_t qId,
                             uint64_t taskId) {
  pCond->twindow = updateLastrowForEachGroup(groupList);

  // no qualified table
  if (groupList->numOfTables == 0) {
    return NULL;
  }

  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)tsdbQueryTables(pVnode, pCond, groupList, qId, taskId);
  if (pTsdbReadHandle == NULL) {
    return NULL;
  }

  int32_t code = checkForCachedLastRow(pTsdbReadHandle, groupList);
  if (code != TSDB_CODE_SUCCESS) {  // set the numOfTables to be 0
    terrno = code;
    return NULL;
  }

  assert(pCond->order == TSDB_ORDER_ASC && pCond->twindow.skey <= pCond->twindow.ekey);
  if (pTsdbReadHandle->cachelastrow) {
    pTsdbReadHandle->type = TSDB_QUERY_TYPE_LAST;
  }

  return pTsdbReadHandle;
}

#if 0
tsdbReaderT tsdbQueryCacheLastT(STsdb *tsdb, SQueryTableDataCond *pCond, STableGroupInfo *groupList, uint64_t qId, STsdbMemTable* pMemRef) {
  STsdbReadHandle *pTsdbReadHandle = (STsdbReadHandle*) tsdbQueryTablesT(tsdb, pCond, groupList, qId, pMemRef);
  if (pTsdbReadHandle == NULL) {
    return NULL;
  }

  int32_t code = checkForCachedLast(pTsdbReadHandle);
  if (code != TSDB_CODE_SUCCESS) { // set the numOfTables to be 0
    terrno = code;
    return NULL;
  }

  if (pTsdbReadHandle->cachelastrow) {
    pTsdbReadHandle->type = TSDB_QUERY_TYPE_LAST;
  }
  
  return pTsdbReadHandle;
}

#endif
SArray* tsdbGetQueriedTableList(tsdbReaderT* pHandle) {
  assert(pHandle != NULL);

  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)pHandle;

  size_t  size = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
  SArray* res = taosArrayInit(size, POINTER_BYTES);
  return res;
}

// leave only one table for each group
static STableGroupInfo* trimTableGroup(STimeWindow* window, STableGroupInfo* pGroupList) {
  assert(pGroupList);
  size_t numOfGroup = taosArrayGetSize(pGroupList->pGroupList);

  STableGroupInfo* pNew = taosMemoryCalloc(1, sizeof(STableGroupInfo));
  pNew->pGroupList = taosArrayInit(numOfGroup, POINTER_BYTES);

  for (int32_t i = 0; i < numOfGroup; ++i) {
    SArray* oneGroup = taosArrayGetP(pGroupList->pGroupList, i);
    size_t  numOfTables = taosArrayGetSize(oneGroup);

    SArray* px = taosArrayInit(4, sizeof(STableKeyInfo));
    for (int32_t j = 0; j < numOfTables; ++j) {
      STableKeyInfo* pInfo = (STableKeyInfo*)taosArrayGet(oneGroup, j);
      //      if (window->skey <= pInfo->lastKey && ((STable*)pInfo->pTable)->lastKey != TSKEY_INITIAL_VAL) {
      //        taosArrayPush(px, pInfo);
      //        pNew->numOfTables += 1;
      //        break;
      //      }
    }

    // there are no data in this group
    if (taosArrayGetSize(px) == 0) {
      taosArrayDestroy(px);
    } else {
      taosArrayPush(pNew->pGroupList, &px);
    }
  }

  return pNew;
}

tsdbReaderT tsdbQueryRowsInExternalWindow(SVnode* pVnode, SQueryTableDataCond* pCond, STableGroupInfo* groupList,
                                          uint64_t qId, uint64_t taskId) {
  STableGroupInfo* pNew = trimTableGroup(&pCond->twindow, groupList);

  if (pNew->numOfTables == 0) {
    tsdbDebug("update query time range to invalidate time window");

    assert(taosArrayGetSize(pNew->pGroupList) == 0);
    bool asc = ASCENDING_TRAVERSE(pCond->order);
    if (asc) {
      pCond->twindow.ekey = pCond->twindow.skey - 1;
    } else {
      pCond->twindow.skey = pCond->twindow.ekey - 1;
    }
  }

  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)tsdbQueryTables(pVnode, pCond, pNew, qId, taskId);
  pTsdbReadHandle->loadExternalRow = true;
  pTsdbReadHandle->currentLoadExternalRows = true;

  return pTsdbReadHandle;
}

static bool initTableMemIterator(STsdbReadHandle* pHandle, STableCheckInfo* pCheckInfo) {
  if (pCheckInfo->initBuf) {
    return true;
  }

  pCheckInfo->initBuf = true;
  int32_t order = pHandle->order;

  STbData** pMem = NULL;
  STbData** pIMem = NULL;

  TSKEY tLastKey = keyToTkey(pCheckInfo->lastKey);
  if (pHandle->pTsdb->mem != NULL) {
    pMem = taosHashGet(pHandle->pTsdb->mem->pHashIdx, &pCheckInfo->tableId, sizeof(pCheckInfo->tableId));
    if (pMem != NULL) {
      pCheckInfo->iter =
          tSkipListCreateIterFromVal((*pMem)->pData, (const char*)&tLastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
    }
  }

  if (pHandle->pTsdb->imem != NULL) {
    pIMem = taosHashGet(pHandle->pTsdb->imem->pHashIdx, &pCheckInfo->tableId, sizeof(pCheckInfo->tableId));
    if (pIMem != NULL) {
      pCheckInfo->iiter =
          tSkipListCreateIterFromVal((*pIMem)->pData, (const char*)&tLastKey, TSDB_DATA_TYPE_TIMESTAMP, order);
    }
  }

  // both iterators are NULL, no data in buffer right now
  if (pCheckInfo->iter == NULL && pCheckInfo->iiter == NULL) {
    return false;
  }

  bool memEmpty = (pCheckInfo->iter == NULL) || (pCheckInfo->iter != NULL && !tSkipListIterNext(pCheckInfo->iter));
  bool imemEmpty = (pCheckInfo->iiter == NULL) || (pCheckInfo->iiter != NULL && !tSkipListIterNext(pCheckInfo->iiter));
  if (memEmpty && imemEmpty) {  // buffer is empty
    return false;
  }

  if (!memEmpty) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    assert(node != NULL);

    STSRow* row = (STSRow*)SL_GET_NODE_DATA(node);
    TSKEY   key = TD_ROW_KEY(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64 ", check data in mem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
              "-%" PRId64 ", lastKey:%" PRId64 ", numOfRows:%" PRId64 ", %s",
              pHandle, pCheckInfo->tableId, key, order, (*pMem)->keyMin, (*pMem)->keyMax, pCheckInfo->lastKey,
              (*pMem)->nrows, pHandle->idStr);

    if (ASCENDING_TRAVERSE(order)) {
      assert(pCheckInfo->lastKey <= key);
    } else {
      assert(pCheckInfo->lastKey >= key);
    }

  } else {
    tsdbDebug("%p uid:%" PRId64 ", no data in mem, %s", pHandle, pCheckInfo->tableId, pHandle->idStr);
  }

  if (!imemEmpty) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    assert(node != NULL);

    STSRow* row = (STSRow*)SL_GET_NODE_DATA(node);
    TSKEY   key = TD_ROW_KEY(row);  // first timestamp in buffer
    tsdbDebug("%p uid:%" PRId64 ", check data in imem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
              "-%" PRId64 ", lastKey:%" PRId64 ", numOfRows:%" PRId64 ", %s",
              pHandle, pCheckInfo->tableId, key, order, (*pIMem)->keyMin, (*pIMem)->keyMax, pCheckInfo->lastKey,
              (*pIMem)->nrows, pHandle->idStr);

    if (ASCENDING_TRAVERSE(order)) {
      assert(pCheckInfo->lastKey <= key);
    } else {
      assert(pCheckInfo->lastKey >= key);
    }
  } else {
    tsdbDebug("%p uid:%" PRId64 ", no data in imem, %s", pHandle, pCheckInfo->tableId, pHandle->idStr);
  }

  return true;
}

static void destroyTableMemIterator(STableCheckInfo* pCheckInfo) {
  tSkipListDestroyIter(pCheckInfo->iter);
  tSkipListDestroyIter(pCheckInfo->iiter);
}

static TSKEY extractFirstTraverseKey(STableCheckInfo* pCheckInfo, int32_t order, int32_t update, TDRowVerT maxVer) {
  STSRow *rmem = NULL, *rimem = NULL;
  if (pCheckInfo->iter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    if (node != NULL) {
      rmem = (STSRow*)SL_GET_NODE_DATA(node);
      // TODO: filter max version
      // if (TD_ROW_VER(rmem) > maxVer) {
      //   rmem = NULL;
      // }
    }
  }

  if (pCheckInfo->iiter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    if (node != NULL) {
      rimem = (STSRow*)SL_GET_NODE_DATA(node);
      // TODO: filter max version
      // if (TD_ROW_VER(rimem) > maxVer) {
      //   rimem = NULL;
      // }
    }
  }

  if (rmem == NULL && rimem == NULL) {
    return TSKEY_INITIAL_VAL;
  }

  if (rmem != NULL && rimem == NULL) {
    pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
    return TD_ROW_KEY(rmem);
  }

  if (rmem == NULL && rimem != NULL) {
    pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
    return TD_ROW_KEY(rimem);
  }

  TSKEY r1 = TD_ROW_KEY(rmem);
  TSKEY r2 = TD_ROW_KEY(rimem);

  if (r1 == r2) {
#if 0
    if (update == TD_ROW_DISCARD_UPDATE) {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
      tSkipListIterNext(pCheckInfo->iter);
    } else if (update == TD_ROW_OVERWRITE_UPDATE) {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
      tSkipListIterNext(pCheckInfo->iiter);
    } else {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
    }
#endif
    if (TD_SUPPORT_UPDATE(update)) {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
    } else {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
      tSkipListIterNext(pCheckInfo->iter);
    }
    return r1;
  } else if (r1 < r2 && ASCENDING_TRAVERSE(order)) {
    pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
    return r1;
  } else {
    pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
    return r2;
  }
}

static STSRow* getSRowInTableMem(STableCheckInfo* pCheckInfo, int32_t order, int32_t update, STSRow** extraRow,
                                 TDRowVerT maxVer) {
  STSRow *rmem = NULL, *rimem = NULL;
  if (pCheckInfo->iter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iter);
    if (node != NULL) {
      rmem = (STSRow*)SL_GET_NODE_DATA(node);
#if 0  // TODO: skiplist refactor
      if (TD_ROW_VER(rmem) > maxVer) {
        rmem = NULL;
      }
#endif
    }
  }

  if (pCheckInfo->iiter) {
    SSkipListNode* node = tSkipListIterGet(pCheckInfo->iiter);
    if (node != NULL) {
      rimem = (STSRow*)SL_GET_NODE_DATA(node);
#if 0  // TODO: skiplist refactor
      if (TD_ROW_VER(rimem) > maxVer) {
        rimem = NULL;
      }
#endif
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

  TSKEY r1 = TD_ROW_KEY(rmem);
  TSKEY r2 = TD_ROW_KEY(rimem);

  if (r1 == r2) {
#if 0
    if (update == TD_ROW_DISCARD_UPDATE) {
      tSkipListIterNext(pCheckInfo->iter);
      pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
      return rimem;
    } else if (update == TD_ROW_OVERWRITE_UPDATE) {
      tSkipListIterNext(pCheckInfo->iiter);
      pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
      return rmem;
    } else {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
      *extraRow = rimem;
      return rmem;
    }
#endif
    if (TD_SUPPORT_UPDATE(update)) {
      pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
      *extraRow = rimem;
      return rmem;
    } else {
      tSkipListIterNext(pCheckInfo->iter);
      pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
      return rimem;
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
        pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
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
  } else if (pCheckInfo->chosen == CHECKINFO_CHOSEN_IMEM) {
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

static bool hasMoreDataInCache(STsdbReadHandle* pHandle) {
  STsdbCfg* pCfg = REPO_CFG(pHandle->pTsdb);
  size_t    size = taosArrayGetSize(pHandle->pTableCheckInfo);
  assert(pHandle->activeIndex < size && pHandle->activeIndex >= 0 && size >= 1);
  pHandle->cur.fid = INT32_MIN;

  STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);
  if (!pCheckInfo->initBuf) {
    initTableMemIterator(pHandle, pCheckInfo);
  }

  STSRow* row = getSRowInTableMem(pCheckInfo, pHandle->order, pCfg->update, NULL, TD_VER_MAX);
  if (row == NULL) {
    return false;
  }

  pCheckInfo->lastKey = TD_ROW_KEY(row);  // first timestamp in buffer
  tsdbDebug("%p uid:%" PRId64 ", check data in buffer from skey:%" PRId64 ", order:%d, %s", pHandle,
            pCheckInfo->tableId, pCheckInfo->lastKey, pHandle->order, pHandle->idStr);

  // all data in mem are checked already.
  if ((pCheckInfo->lastKey > pHandle->window.ekey && ASCENDING_TRAVERSE(pHandle->order)) ||
      (pCheckInfo->lastKey < pHandle->window.ekey && !ASCENDING_TRAVERSE(pHandle->order))) {
    return false;
  }

  int32_t      step = ASCENDING_TRAVERSE(pHandle->order) ? 1 : -1;
  STimeWindow* win = &pHandle->cur.win;
  pHandle->cur.rows = tsdbReadRowsFromCache(pCheckInfo, pHandle->window.ekey, pHandle->outputCapacity, win, pHandle);

  // update the last key value
  pCheckInfo->lastKey = win->ekey + step;
  pHandle->cur.lastKey = win->ekey + step;
  pHandle->cur.mixBlock = true;

  if (!ASCENDING_TRAVERSE(pHandle->order)) {
    TSWAP(win->skey, win->ekey);
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
  if (fid < 0L && llabs(fid) > INT32_MAX) {                                // data value overflow for INT32
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

static int32_t loadBlockInfo(STsdbReadHandle* pTsdbReadHandle, int32_t index, int32_t* numOfBlocks) {
  int32_t code = 0;

  STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, index);
  pCheckInfo->numOfBlocks = 0;

  STable table = {.uid = pCheckInfo->tableId, .tid = pCheckInfo->tableId};
  table.pSchema = pTsdbReadHandle->pSchema;

  if (tsdbSetReadTable(&pTsdbReadHandle->rhelper, &table) != TSDB_CODE_SUCCESS) {
    code = terrno;
    return code;
  }

  SBlockIdx* compIndex = pTsdbReadHandle->rhelper.pBlkIdx;

  // no data block in this file, try next file
  if (compIndex == NULL || compIndex->uid != pCheckInfo->tableId) {
    return 0;  // no data blocks in the file belongs to pCheckInfo->pTable
  }

  if (pCheckInfo->compSize < (int32_t)compIndex->len) {
    assert(compIndex->len > 0);

    char* t = taosMemoryRealloc(pCheckInfo->pCompInfo, compIndex->len);
    if (t == NULL) {
      terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
      code = TSDB_CODE_TDB_OUT_OF_MEMORY;
      return code;
    }

    pCheckInfo->pCompInfo = (SBlockInfo*)t;
    pCheckInfo->compSize = compIndex->len;
  }

  if (tsdbLoadBlockInfo(&(pTsdbReadHandle->rhelper), (void*)(pCheckInfo->pCompInfo)) < 0) {
    return terrno;
  }
  SBlockInfo* pCompInfo = pCheckInfo->pCompInfo;

  TSKEY s = TSKEY_INITIAL_VAL, e = TSKEY_INITIAL_VAL;

  if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
    assert(pCheckInfo->lastKey <= pTsdbReadHandle->window.ekey &&
           pTsdbReadHandle->window.skey <= pTsdbReadHandle->window.ekey);
  } else {
    assert(pCheckInfo->lastKey >= pTsdbReadHandle->window.ekey &&
           pTsdbReadHandle->window.skey >= pTsdbReadHandle->window.ekey);
  }

  s = TMIN(pCheckInfo->lastKey, pTsdbReadHandle->window.ekey);
  e = TMAX(pCheckInfo->lastKey, pTsdbReadHandle->window.ekey);

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

static int32_t getFileCompInfo(STsdbReadHandle* pTsdbReadHandle, int32_t* numOfBlocks) {
  // load all the comp offset value for all tables in this file
  int32_t code = TSDB_CODE_SUCCESS;
  *numOfBlocks = 0;

  pTsdbReadHandle->cost.headFileLoad += 1;
  int64_t s = taosGetTimestampUs();

  size_t numOfTables = 0;
  if (pTsdbReadHandle->loadType == BLOCK_LOAD_TABLE_SEQ_ORDER) {
    code = loadBlockInfo(pTsdbReadHandle, pTsdbReadHandle->activeIndex, numOfBlocks);
  } else if (pTsdbReadHandle->loadType == BLOCK_LOAD_OFFSET_SEQ_ORDER) {
    numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);

    for (int32_t i = 0; i < numOfTables; ++i) {
      code = loadBlockInfo(pTsdbReadHandle, i, numOfBlocks);
      if (code != TSDB_CODE_SUCCESS) {
        int64_t e = taosGetTimestampUs();

        pTsdbReadHandle->cost.headFileLoadTime += (e - s);
        return code;
      }
    }
  } else {
    assert(0);
  }

  int64_t e = taosGetTimestampUs();
  pTsdbReadHandle->cost.headFileLoadTime += (e - s);
  return code;
}

static int32_t doLoadFileDataBlock(STsdbReadHandle* pTsdbReadHandle, SBlock* pBlock, STableCheckInfo* pCheckInfo,
                                   int32_t slotIndex) {
  int64_t st = taosGetTimestampUs();

  int32_t code = tdInitDataCols(pTsdbReadHandle->pDataCols, pTsdbReadHandle->pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for pDataCols, %s", pTsdbReadHandle, pTsdbReadHandle->idStr);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  code = tdInitDataCols(pTsdbReadHandle->rhelper.pDCols[0], pTsdbReadHandle->pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for rhelper.pDataCols[0], %s", pTsdbReadHandle, pTsdbReadHandle->idStr);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  code = tdInitDataCols(pTsdbReadHandle->rhelper.pDCols[1], pTsdbReadHandle->pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p failed to malloc buf for rhelper.pDataCols[1], %s", pTsdbReadHandle, pTsdbReadHandle->idStr);
    terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _error;
  }

  int16_t* colIds = pTsdbReadHandle->suppInfo.defaultLoadColumn->pData;

  int32_t ret = tsdbLoadBlockDataCols(&(pTsdbReadHandle->rhelper), pBlock, pCheckInfo->pCompInfo, colIds,
                                      (int)(QH_GET_NUM_OF_COLS(pTsdbReadHandle)), true);
  if (ret != TSDB_CODE_SUCCESS) {
    int32_t c = terrno;
    assert(c != TSDB_CODE_SUCCESS);
    goto _error;
  }

  SDataBlockLoadInfo* pBlockLoadInfo = &pTsdbReadHandle->dataBlockLoadInfo;

  pBlockLoadInfo->fileGroup = pTsdbReadHandle->pFileGroup;
  pBlockLoadInfo->slot = pTsdbReadHandle->cur.slot;
  pBlockLoadInfo->uid = pCheckInfo->tableId;

  SDataCols* pCols = pTsdbReadHandle->rhelper.pDCols[0];
  assert(pCols->numOfRows != 0 && pCols->numOfRows <= pBlock->numOfRows);

  pBlock->numOfRows = pCols->numOfRows;

  // Convert from TKEY to TSKEY for primary timestamp column if current block has timestamp before 1970-01-01T00:00:00Z
  if (pBlock->keyFirst < 0 && colIds[0] == PRIMARYKEY_TIMESTAMP_COL_ID) {
    int64_t* src = pCols->cols[0].pData;
    for (int32_t i = 0; i < pBlock->numOfRows; ++i) {
      src[i] = tdGetKey(src[i]);
    }
  }

  int64_t elapsedTime = (taosGetTimestampUs() - st);
  pTsdbReadHandle->cost.blockLoadTime += elapsedTime;

  tsdbDebug("%p load file block into buffer, index:%d, brange:%" PRId64 "-%" PRId64 ", rows:%d, elapsed time:%" PRId64
            " us, %s",
            pTsdbReadHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, elapsedTime,
            pTsdbReadHandle->idStr);
  return TSDB_CODE_SUCCESS;

_error:
  pBlock->numOfRows = 0;

  tsdbError("%p error occurs in loading file block, index:%d, brange:%" PRId64 "-%" PRId64 ", rows:%d, %s",
            pTsdbReadHandle, slotIndex, pBlock->keyFirst, pBlock->keyLast, pBlock->numOfRows, pTsdbReadHandle->idStr);
  return terrno;
}

static int32_t getEndPosInDataBlock(STsdbReadHandle* pTsdbReadHandle, SDataBlockInfo* pBlockInfo);
static int32_t doCopyRowsFromFileBlock(STsdbReadHandle* pTsdbReadHandle, int32_t capacity, int32_t numOfRows,
                                       int32_t start, int32_t end);
static void    moveDataToFront(STsdbReadHandle* pTsdbReadHandle, int32_t numOfRows, int32_t numOfCols);
static void    doCheckGeneratedBlockRange(STsdbReadHandle* pTsdbReadHandle);
static void    copyAllRemainRowsFromFileBlock(STsdbReadHandle* pTsdbReadHandle, STableCheckInfo* pCheckInfo,
                                              SDataBlockInfo* pBlockInfo, int32_t endPos);

static int32_t handleDataMergeIfNeeded(STsdbReadHandle* pTsdbReadHandle, SBlock* pBlock, STableCheckInfo* pCheckInfo) {
  SQueryFilePos* cur = &pTsdbReadHandle->cur;
  STsdbCfg*      pCfg = REPO_CFG(pTsdbReadHandle->pTsdb);
  SDataBlockInfo binfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);
  TSKEY          key;
  int32_t        code = TSDB_CODE_SUCCESS;

  /*bool hasData = */ initTableMemIterator(pTsdbReadHandle, pCheckInfo);
  assert(cur->pos >= 0 && cur->pos <= binfo.rows);

  key = extractFirstTraverseKey(pCheckInfo, pTsdbReadHandle->order, pCfg->update, TD_VER_MAX);

  if (key != TSKEY_INITIAL_VAL) {
    tsdbDebug("%p key in mem:%" PRId64 ", %s", pTsdbReadHandle, key, pTsdbReadHandle->idStr);
  } else {
    tsdbDebug("%p no data in mem, %s", pTsdbReadHandle, pTsdbReadHandle->idStr);
  }

  bool ascScan = ASCENDING_TRAVERSE(pTsdbReadHandle->order);

  if ((ascScan && (key != TSKEY_INITIAL_VAL && key <= binfo.window.ekey)) ||
      (!ascScan && (key != TSKEY_INITIAL_VAL && key >= binfo.window.skey))) {
    if ((ascScan && (key != TSKEY_INITIAL_VAL && key < binfo.window.skey)) ||
        (!ascScan && (key != TSKEY_INITIAL_VAL && key > binfo.window.ekey))) {
      // do not load file block into buffer
      int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;

      TSKEY maxKey =
          ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? (binfo.window.skey - step) : (binfo.window.ekey - step);
      cur->rows =
          tsdbReadRowsFromCache(pCheckInfo, maxKey, pTsdbReadHandle->outputCapacity, &cur->win, pTsdbReadHandle);
      pTsdbReadHandle->realNumOfRows = cur->rows;

      // update the last key value
      pCheckInfo->lastKey = cur->win.ekey + step;
      if (!ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
        TSWAP(cur->win.skey, cur->win.ekey);
      }

      cur->mixBlock = true;
      cur->blockCompleted = false;
      return code;
    }

    // return error, add test cases
    if ((code = doLoadFileDataBlock(pTsdbReadHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
      return code;
    }

    doMergeTwoLevelData(pTsdbReadHandle, pCheckInfo, pBlock);
  } else {
    /*
     * no data in cache, only load data from file
     * during the query processing, data in cache will not be checked anymore.
     *
     * Here the buffer is not enough, so only part of file block can be loaded into memory buffer
     */
    assert(pTsdbReadHandle->outputCapacity >= binfo.rows);
    int32_t endPos = getEndPosInDataBlock(pTsdbReadHandle, &binfo);

    if ((cur->pos == 0 && endPos == binfo.rows - 1 && ascScan) ||
        (cur->pos == (binfo.rows - 1) && endPos == 0 && (!ascScan))) {
      pTsdbReadHandle->realNumOfRows = binfo.rows;

      cur->rows = binfo.rows;
      cur->win = binfo.window;
      cur->mixBlock = false;
      cur->blockCompleted = true;

      if (ascScan) {
        cur->lastKey = binfo.window.ekey + 1;
        cur->pos = binfo.rows;
      } else {
        cur->lastKey = binfo.window.skey - 1;
        cur->pos = -1;
      }
    } else {  // partially copy to dest buffer
      copyAllRemainRowsFromFileBlock(pTsdbReadHandle, pCheckInfo, &binfo, endPos);
      cur->mixBlock = true;
    }

    assert(cur->blockCompleted);
    if (cur->rows == binfo.rows) {
      tsdbDebug("%p whole file block qualified, brange:%" PRId64 "-%" PRId64 ", rows:%d, lastKey:%" PRId64 ", %s",
                pTsdbReadHandle, cur->win.skey, cur->win.ekey, cur->rows, cur->lastKey, pTsdbReadHandle->idStr);
    } else {
      tsdbDebug("%p create data block from remain file block, brange:%" PRId64 "-%" PRId64
                ", rows:%d, total:%d, lastKey:%" PRId64 ", %s",
                pTsdbReadHandle, cur->win.skey, cur->win.ekey, cur->rows, binfo.rows, cur->lastKey,
                pTsdbReadHandle->idStr);
    }
  }

  return code;
}

static int32_t binarySearchForKey(char* pValue, int num, TSKEY key, int order);

static int32_t loadFileDataBlock(STsdbReadHandle* pTsdbReadHandle, SBlock* pBlock, STableCheckInfo* pCheckInfo,
                                 bool* exists) {
  SQueryFilePos* cur = &pTsdbReadHandle->cur;
  int32_t        code = TSDB_CODE_SUCCESS;
  bool           asc = ASCENDING_TRAVERSE(pTsdbReadHandle->order);

  if (asc) {
    // query ended in/started from current block
    if (pTsdbReadHandle->window.ekey < pBlock->keyLast || pCheckInfo->lastKey > pBlock->keyFirst) {
      if ((code = doLoadFileDataBlock(pTsdbReadHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
        *exists = false;
        return code;
      }

      SDataCols* pTSCol = pTsdbReadHandle->rhelper.pDCols[0];
      assert(pTSCol->cols->type == TSDB_DATA_TYPE_TIMESTAMP && pTSCol->numOfRows == pBlock->numOfRows);

      if (pCheckInfo->lastKey > pBlock->keyFirst) {
        cur->pos =
            binarySearchForKey(pTSCol->cols[0].pData, pBlock->numOfRows, pCheckInfo->lastKey, pTsdbReadHandle->order);
      } else {
        cur->pos = 0;
      }

      assert(pCheckInfo->lastKey <= pBlock->keyLast);
      doMergeTwoLevelData(pTsdbReadHandle, pCheckInfo, pBlock);
    } else {  // the whole block is loaded in to buffer
      cur->pos = asc ? 0 : (pBlock->numOfRows - 1);
      code = handleDataMergeIfNeeded(pTsdbReadHandle, pBlock, pCheckInfo);
    }
  } else {  // desc order, query ended in current block
    if (pTsdbReadHandle->window.ekey > pBlock->keyFirst || pCheckInfo->lastKey < pBlock->keyLast) {
      if ((code = doLoadFileDataBlock(pTsdbReadHandle, pBlock, pCheckInfo, cur->slot)) != TSDB_CODE_SUCCESS) {
        *exists = false;
        return code;
      }

      SDataCols* pTsCol = pTsdbReadHandle->rhelper.pDCols[0];
      if (pCheckInfo->lastKey < pBlock->keyLast) {
        cur->pos =
            binarySearchForKey(pTsCol->cols[0].pData, pBlock->numOfRows, pCheckInfo->lastKey, pTsdbReadHandle->order);
      } else {
        cur->pos = pBlock->numOfRows - 1;
      }

      assert(pCheckInfo->lastKey >= pBlock->keyFirst);
      doMergeTwoLevelData(pTsdbReadHandle, pCheckInfo, pBlock);
    } else {
      cur->pos = asc ? 0 : (pBlock->numOfRows - 1);
      code = handleDataMergeIfNeeded(pTsdbReadHandle, pBlock, pCheckInfo);
    }
  }

  *exists = pTsdbReadHandle->realNumOfRows > 0;
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

static int32_t doCopyRowsFromFileBlock(STsdbReadHandle* pTsdbReadHandle, int32_t capacity, int32_t numOfRows,
                                       int32_t start, int32_t end) {
  SDataCols* pCols = pTsdbReadHandle->rhelper.pDCols[0];
  TSKEY*     tsArray = pCols->cols[0].pData;

  int32_t num = end - start + 1;
  assert(num >= 0);

  if (num == 0) {
    return numOfRows;
  }

  bool    ascScan = ASCENDING_TRAVERSE(pTsdbReadHandle->order);
  int32_t trueStart = ascScan ? start : end;
  int32_t trueEnd = ascScan ? end : start;
  int32_t step = ascScan ? 1 : -1;

  int32_t requiredNumOfCols = (int32_t)taosArrayGetSize(pTsdbReadHandle->pColumns);

  // data in buffer has greater timestamp, copy data in file block
  int32_t i = 0, j = 0;
  while (i < requiredNumOfCols && j < pCols->numOfCols) {
    SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);

    SDataCol* src = &pCols->cols[j];
    if (src->colId < pColInfo->info.colId) {
      j++;
      continue;
    }

    if (!isAllRowsNull(src) && pColInfo->info.colId == src->colId) {
      if (!IS_VAR_DATA_TYPE(pColInfo->info.type)) {  // todo opt performance
        //        memmove(pData, (char*)src->pData + bytes * start, bytes * num);
        int32_t rowIndex = numOfRows;
        for (int32_t k = trueStart; ((ascScan && k <= trueEnd) || (!ascScan && k >= trueEnd)); k += step, ++rowIndex) {
          SCellVal sVal = {0};
          if (tdGetColDataOfRow(&sVal, src, k, pCols->bitmapMode) < 0) {
            TASSERT(0);
          }

          if (sVal.valType == TD_VTYPE_NORM) {
            colDataAppend(pColInfo, rowIndex, sVal.val, false);
          } else {
            colDataAppendNULL(pColInfo, rowIndex);
          }
        }
      } else {  // handle the var-string
        int32_t rowIndex = numOfRows;

        // todo refactor, only copy one-by-one
        for (int32_t k = trueStart; ((ascScan && k <= trueEnd) || (!ascScan && k >= trueEnd)); k += step, ++rowIndex) {
          SCellVal sVal = {0};
          if (tdGetColDataOfRow(&sVal, src, k, pCols->bitmapMode) < 0) {
            TASSERT(0);
          }

          if (sVal.valType == TD_VTYPE_NORM) {
            colDataAppend(pColInfo, rowIndex, sVal.val, false);
          } else {
            colDataAppendNULL(pColInfo, rowIndex);
          }
        }
      }

      j++;
      i++;
    } else {  // pColInfo->info.colId < src->colId, it is a NULL data
      colDataAppendNNULL(pColInfo, numOfRows, num);
      i++;
    }
  }

  while (i < requiredNumOfCols) {  // the remain columns are all null data
    SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);
    colDataAppendNNULL(pColInfo, numOfRows, num);
    i++;
  }

  pTsdbReadHandle->cur.win.ekey = tsArray[trueEnd];
  pTsdbReadHandle->cur.lastKey = tsArray[trueEnd] + step;

  return numOfRows + num;
}

/**
 * @brief  // TODO fix bug for reverse copy data problem
 *        Note: row1 always has high priority
 *
 * @param pTsdbReadHandle
 * @param capacity
 * @param curRow
 * @param row1
 * @param row2
 * @param numOfCols
 * @param uid
 * @param pSchema1
 * @param pSchema2
 * @param update
 * @param lastRowKey
 * @return int32_t The quantity of rows appended
 */
static int32_t mergeTwoRowFromMem(STsdbReadHandle* pTsdbReadHandle, int32_t capacity, int32_t* curRow, STSRow* row1,
                                  STSRow* row2, int32_t numOfCols, uint64_t uid, STSchema* pSchema1, STSchema* pSchema2,
                                  bool update, TSKEY* lastRowKey) {
#if 1
  STSchema* pSchema;
  STSRow*   row;
  int16_t   colId;
  int16_t   offset;

  bool     isRow1DataRow = TD_IS_TP_ROW(row1);
  bool     isRow2DataRow;
  bool     isChosenRowDataRow;
  int32_t  chosen_itr;
  SCellVal sVal = {0};
  TSKEY    rowKey = TSKEY_INITIAL_VAL;
  int32_t  nResult = 0;
  int32_t  mergeOption = 0;  // 0 discard 1 overwrite 2 merge

  // the schema version info is embeded in STSRow
  int32_t numOfColsOfRow1 = 0;

  if (pSchema1 == NULL) {
    // pSchema1 = metaGetTbTSchema(REPO_META(pTsdbReadHandle->pTsdb), uid, TD_ROW_SVER(row1));
    // TODO: use the real schemaVersion
    pSchema1 = metaGetTbTSchema(REPO_META(pTsdbReadHandle->pTsdb), uid, 0);
  }

#ifdef TD_DEBUG_PRINT_ROW
  tdSRowPrint(row1, pSchema1, __func__);
#endif

  if (isRow1DataRow) {
    numOfColsOfRow1 = schemaNCols(pSchema1);
  } else {
    numOfColsOfRow1 = tdRowGetNCols(row1);
  }

  int32_t numOfColsOfRow2 = 0;
  if (row2) {
    isRow2DataRow = TD_IS_TP_ROW(row2);
    if (pSchema2 == NULL) {
      // pSchema2 = metaGetTbTSchema(REPO_META(pTsdbReadHandle->pTsdb), uid, TD_ROW_SVER(row2));
      // TODO: use the real schemaVersion
      pSchema2 = metaGetTbTSchema(REPO_META(pTsdbReadHandle->pTsdb), uid, 0);
    }
    if (isRow2DataRow) {
      numOfColsOfRow2 = schemaNCols(pSchema2);
    } else {
      numOfColsOfRow2 = tdRowGetNCols(row2);
    }
  }

  int32_t i = 0, j = 0, k = 0;
  while (i < numOfCols && (j < numOfColsOfRow1 || k < numOfColsOfRow2)) {
    SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);

    int32_t colIdOfRow1;
    if (j >= numOfColsOfRow1) {
      colIdOfRow1 = INT32_MAX;
    } else if (isRow1DataRow) {
      colIdOfRow1 = pSchema1->columns[j].colId;
    } else {
      colIdOfRow1 = tdKvRowColIdAt(row1, j);
    }

    int32_t colIdOfRow2;
    if (k >= numOfColsOfRow2) {
      colIdOfRow2 = INT32_MAX;
    } else if (isRow2DataRow) {
      colIdOfRow2 = pSchema2->columns[k].colId;
    } else {
      colIdOfRow2 = tdKvRowColIdAt(row2, k);
    }

    if (colIdOfRow1 < colIdOfRow2) {  // the most probability
      if (colIdOfRow1 < pColInfo->info.colId) {
        ++j;
        continue;
      }
      row = row1;
      pSchema = pSchema1;
      isChosenRowDataRow = isRow1DataRow;
      chosen_itr = j;
    } else if (colIdOfRow1 == colIdOfRow2) {
      if (colIdOfRow1 < pColInfo->info.colId) {
        ++j;
        ++k;
        continue;
      }
      row = row1;
      pSchema = pSchema1;
      isChosenRowDataRow = isRow1DataRow;
      chosen_itr = j;
    } else {
      if (colIdOfRow2 < pColInfo->info.colId) {
        ++k;
        continue;
      }
      row = row2;
      pSchema = pSchema2;
      chosen_itr = k;
      isChosenRowDataRow = isRow2DataRow;
    }

    if (isChosenRowDataRow) {
      colId = pSchema->columns[chosen_itr].colId;
      offset = pSchema->columns[chosen_itr].offset;
      // TODO: use STSRowIter
      tdSTpRowGetVal(row, colId, pSchema->columns[chosen_itr].type, pSchema->flen, offset, chosen_itr - 1, &sVal);
      if (colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
        rowKey = *(TSKEY*)sVal.val;
        if (rowKey != *lastRowKey) {
          mergeOption = 1;
          if (*lastRowKey != TSKEY_INITIAL_VAL) {
            ++(*curRow);
          }
          ++nResult;
        } else if (update) {
          mergeOption = 2;
        } else {
          mergeOption = 0;
          break;
        }

        *lastRowKey = rowKey;
      }
    } else {
      // TODO: use STSRowIter
      if (chosen_itr == 0) {
        colId = PRIMARYKEY_TIMESTAMP_COL_ID;
        tdSKvRowGetVal(row, PRIMARYKEY_TIMESTAMP_COL_ID, -1, -1, &sVal);
        rowKey = *(TSKEY*)sVal.val;
        if (rowKey != *lastRowKey) {
          mergeOption = 1;
          if (*lastRowKey != TSKEY_INITIAL_VAL) {
            ++(*curRow);
          }
          ++nResult;
        } else if (update) {
          mergeOption = 2;
        } else {
          mergeOption = 0;
          break;
        }
        *lastRowKey = rowKey;
      } else {
        SKvRowIdx* pColIdx = tdKvRowColIdxAt(row, chosen_itr - 1);
        colId = pColIdx->colId;
        offset = pColIdx->offset;
        tdSKvRowGetVal(row, colId, offset, chosen_itr - 1, &sVal);
      }
    }

    ASSERT(rowKey != TSKEY_INITIAL_VAL);

    if (colId == pColInfo->info.colId) {
      if (tdValTypeIsNorm(sVal.valType)) {
        colDataAppend(pColInfo, *curRow, sVal.val, false);
      } else if (tdValTypeIsNull(sVal.valType)) {
        colDataAppend(pColInfo, *curRow, NULL, true);
      } else if (tdValTypeIsNone(sVal.valType)) {
        // TODO: Set null if nothing append for this row
        if (mergeOption == 1) {
          colDataAppend(pColInfo, *curRow, NULL, true);
        }
      } else {
        ASSERT(0);
      }

      ++i;

      if (row == row1) {
        ++j;
      } else {
        ++k;
      }
    } else {
      if (mergeOption == 1) {
        colDataAppend(pColInfo, *curRow, NULL, true);
      }
      ++i;
    }
  }

  if (mergeOption == 1) {
    while (i < numOfCols) {  // the remain columns are all null data
      SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);
      colDataAppend(pColInfo, *curRow, NULL, true);
      ++i;
    }
  }

  return nResult;
#endif
}

static void moveDataToFront(STsdbReadHandle* pTsdbReadHandle, int32_t numOfRows, int32_t numOfCols) {
  if (numOfRows == 0 || ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
    return;
  }

  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  if (numOfRows < pTsdbReadHandle->outputCapacity) {
    int32_t emptySize = pTsdbReadHandle->outputCapacity - numOfRows;
    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);
      memmove((char*)pColInfo->pData, (char*)pColInfo->pData + emptySize * pColInfo->info.bytes,
              numOfRows * pColInfo->info.bytes);
    }
  }
}

static void getQualifiedRowsPos(STsdbReadHandle* pTsdbReadHandle, int32_t startPos, int32_t endPos,
                                int32_t numOfExisted, int32_t* start, int32_t* end) {
  *start = -1;

  if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
    int32_t remain = endPos - startPos + 1;
    if (remain + numOfExisted > pTsdbReadHandle->outputCapacity) {
      *end = (pTsdbReadHandle->outputCapacity - numOfExisted) + startPos - 1;
    } else {
      *end = endPos;
    }

    *start = startPos;
  } else {
    int32_t remain = (startPos - endPos) + 1;
    if (remain + numOfExisted > pTsdbReadHandle->outputCapacity) {
      *end = startPos + 1 - (pTsdbReadHandle->outputCapacity - numOfExisted);
    } else {
      *end = endPos;
    }

    *start = *end;
    *end = startPos;
  }
}

static void updateInfoAfterMerge(STsdbReadHandle* pTsdbReadHandle, STableCheckInfo* pCheckInfo, int32_t numOfRows,
                                 int32_t endPos) {
  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  pCheckInfo->lastKey = cur->lastKey;
  pTsdbReadHandle->realNumOfRows = numOfRows;
  cur->rows = numOfRows;
  cur->pos = endPos;
}

static void doCheckGeneratedBlockRange(STsdbReadHandle* pTsdbReadHandle) {
  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  if (cur->rows > 0) {
    if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
      assert(cur->win.skey >= pTsdbReadHandle->window.skey && cur->win.ekey <= pTsdbReadHandle->window.ekey);
    } else {
      assert(cur->win.skey >= pTsdbReadHandle->window.ekey && cur->win.ekey <= pTsdbReadHandle->window.skey);
    }

    SColumnInfoData* pColInfoData = taosArrayGet(pTsdbReadHandle->pColumns, 0);
    assert(cur->win.skey == ((TSKEY*)pColInfoData->pData)[0] &&
           cur->win.ekey == ((TSKEY*)pColInfoData->pData)[cur->rows - 1]);
  } else {
    cur->win = pTsdbReadHandle->window;

    int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;
    cur->lastKey = pTsdbReadHandle->window.ekey + step;
  }
}

static void copyAllRemainRowsFromFileBlock(STsdbReadHandle* pTsdbReadHandle, STableCheckInfo* pCheckInfo,
                                           SDataBlockInfo* pBlockInfo, int32_t endPos) {
  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  SDataCols* pCols = pTsdbReadHandle->rhelper.pDCols[0];
  TSKEY*     tsArray = pCols->cols[0].pData;

  int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;
  int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pTsdbReadHandle));

  int32_t pos = cur->pos;

  int32_t start = cur->pos;
  int32_t end = endPos;

  if (!ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
    TSWAP(start, end);
  }

  assert(pTsdbReadHandle->outputCapacity >= (end - start + 1));
  int32_t numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, 0, start, end);

  // the time window should always be ascending order: skey <= ekey
  cur->win = (STimeWindow){.skey = tsArray[start], .ekey = tsArray[end]};
  cur->mixBlock = (numOfRows != pBlockInfo->rows);
  cur->lastKey = tsArray[endPos] + step;
  cur->blockCompleted = true;

  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  moveDataToFront(pTsdbReadHandle, numOfRows, numOfCols);

  // The value of pos may be -1 or pBlockInfo->rows, and it is invalid in both cases.
  pos = endPos + step;
  updateInfoAfterMerge(pTsdbReadHandle, pCheckInfo, numOfRows, pos);
  doCheckGeneratedBlockRange(pTsdbReadHandle);

  tsdbDebug("%p uid:%" PRIu64 ", data block created, mixblock:%d, brange:%" PRIu64 "-%" PRIu64 " rows:%d, %s",
            pTsdbReadHandle, pCheckInfo->tableId, cur->mixBlock, cur->win.skey, cur->win.ekey, cur->rows,
            pTsdbReadHandle->idStr);
}

int32_t getEndPosInDataBlock(STsdbReadHandle* pTsdbReadHandle, SDataBlockInfo* pBlockInfo) {
  // NOTE: reverse the order to find the end position in data block
  int32_t endPos = -1;
  int32_t order = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC;

  SQueryFilePos* cur = &pTsdbReadHandle->cur;
  SDataCols*     pCols = pTsdbReadHandle->rhelper.pDCols[0];

  if (ASCENDING_TRAVERSE(pTsdbReadHandle->order) && pTsdbReadHandle->window.ekey >= pBlockInfo->window.ekey) {
    endPos = pBlockInfo->rows - 1;
    cur->mixBlock = (cur->pos != 0);
  } else if (!ASCENDING_TRAVERSE(pTsdbReadHandle->order) && pTsdbReadHandle->window.ekey <= pBlockInfo->window.skey) {
    endPos = 0;
    cur->mixBlock = (cur->pos != pBlockInfo->rows - 1);
  } else {
    assert(pCols->numOfRows > 0);
    endPos = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, pTsdbReadHandle->window.ekey, order);
    cur->mixBlock = true;
  }

  return endPos;
}

// only return the qualified data to client in terms of query time window, data rows in the same block but do not
// be included in the query time window will be discarded
static void doMergeTwoLevelData(STsdbReadHandle* pTsdbReadHandle, STableCheckInfo* pCheckInfo, SBlock* pBlock) {
  SQueryFilePos* cur = &pTsdbReadHandle->cur;
  SDataBlockInfo blockInfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);
  STsdbCfg*      pCfg = REPO_CFG(pTsdbReadHandle->pTsdb);

  initTableMemIterator(pTsdbReadHandle, pCheckInfo);

  SDataCols* pCols = pTsdbReadHandle->rhelper.pDCols[0];
  assert(pCols->cols[0].type == TSDB_DATA_TYPE_TIMESTAMP && pCols->cols[0].colId == PRIMARYKEY_TIMESTAMP_COL_ID &&
         cur->pos >= 0 && cur->pos < pBlock->numOfRows);

  TSKEY* tsArray = pCols->cols[0].pData;
  assert(pCols->numOfRows == pBlock->numOfRows && tsArray[0] == pBlock->keyFirst &&
         tsArray[pBlock->numOfRows - 1] == pBlock->keyLast);

  // for search the endPos, so the order needs to reverse
  int32_t order = (pTsdbReadHandle->order == TSDB_ORDER_ASC) ? TSDB_ORDER_DESC : TSDB_ORDER_ASC;

  int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;
  int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pTsdbReadHandle));

  STable* pTable = NULL;
  int32_t endPos = getEndPosInDataBlock(pTsdbReadHandle, &blockInfo);

  tsdbDebug("%p uid:%" PRIu64 " start merge data block, file block range:%" PRIu64 "-%" PRIu64
            " rows:%d, start:%d, end:%d, %s",
            pTsdbReadHandle, pCheckInfo->tableId, blockInfo.window.skey, blockInfo.window.ekey, blockInfo.rows,
            cur->pos, endPos, pTsdbReadHandle->idStr);

  // compared with the data from in-memory buffer, to generate the correct timestamp array list
  int32_t numOfRows = 0;
  int32_t curRow = 0;

  int16_t   rv1 = -1;
  int16_t   rv2 = -1;
  STSchema* pSchema1 = NULL;
  STSchema* pSchema2 = NULL;

  int32_t pos = cur->pos;
  cur->win = TSWINDOW_INITIALIZER;

  // no data in buffer, load data from file directly
  if (pCheckInfo->iiter == NULL && pCheckInfo->iter == NULL) {
    copyAllRemainRowsFromFileBlock(pTsdbReadHandle, pCheckInfo, &blockInfo, endPos);
    return;
  } else if (pCheckInfo->iter != NULL || pCheckInfo->iiter != NULL) {
    SSkipListNode* node = NULL;
    TSKEY          lastKeyAppend = TSKEY_INITIAL_VAL;

    do {
      STSRow* row2 = NULL;
      STSRow* row1 = getSRowInTableMem(pCheckInfo, pTsdbReadHandle->order, pCfg->update, &row2, TD_VER_MAX);
      if (row1 == NULL) {
        break;
      }

      TSKEY key = TD_ROW_KEY(row1);
      if ((key > pTsdbReadHandle->window.ekey && ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
          (key < pTsdbReadHandle->window.ekey && !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
        break;
      }

      if (((pos > endPos || tsArray[pos] > pTsdbReadHandle->window.ekey) &&
           ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
          ((pos < endPos || tsArray[pos] < pTsdbReadHandle->window.ekey) &&
           !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
        break;
      }

      if ((key < tsArray[pos] && ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
          (key > tsArray[pos] && !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
        if (rv1 != TD_ROW_SVER(row1)) {
          //          pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1));
          rv1 = TD_ROW_SVER(row1);
        }
        if (row2 && rv2 != TD_ROW_SVER(row2)) {
          //          pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2));
          rv2 = TD_ROW_SVER(row2);
        }

        numOfRows +=
            mergeTwoRowFromMem(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, &curRow, row1, row2, numOfCols,
                               pCheckInfo->tableId, pSchema1, pSchema2, pCfg->update, &lastKeyAppend);
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = key;
        }

        cur->win.ekey = key;
        cur->lastKey = key + step;
        cur->mixBlock = true;
        moveToNextRowInMem(pCheckInfo);
      } else if (key == tsArray[pos]) {  // data in buffer has the same timestamp of data in file block, ignore it
#if 0
        if (pCfg->update) {
          if (pCfg->update == TD_ROW_PARTIAL_UPDATE) {
            doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, numOfRows, pos, pos);
          }
          if (rv1 != TD_ROW_SVER(row1)) {
            //            pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1));
            rv1 = TD_ROW_SVER(row1);
          }
          if (row2 && rv2 != TD_ROW_SVER(row2)) {
            //            pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2));
            rv2 = TD_ROW_SVER(row2);
          }

          bool forceSetNull = pCfg->update != TD_ROW_PARTIAL_UPDATE;
          mergeTwoRowFromMem(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, numOfRows, row1, row2, numOfCols,
                             pCheckInfo->tableId, pSchema1, pSchema2, forceSetNull, &lastRowKey);
          numOfRows += 1;
          if (cur->win.skey == TSKEY_INITIAL_VAL) {
            cur->win.skey = key;
          }

          cur->win.ekey = key;
          cur->lastKey = key + step;
          cur->mixBlock = true;

          moveToNextRowInMem(pCheckInfo);
          pos += step;
        } else {
          moveToNextRowInMem(pCheckInfo);
        }
#endif
        if (TD_SUPPORT_UPDATE(pCfg->update)) {
          if (lastKeyAppend != key) {
            lastKeyAppend = key;
            ++curRow;
          }
          numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, curRow, pos, pos);

          if (rv1 != TD_ROW_SVER(row1)) {
            //            pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1));
            rv1 = TD_ROW_SVER(row1);
          }
          if (row2 && rv2 != TD_ROW_SVER(row2)) {
            //            pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2));
            rv2 = TD_ROW_SVER(row2);
          }
          numOfRows +=
              mergeTwoRowFromMem(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, &curRow, row1, row2, numOfCols,
                                 pCheckInfo->tableId, pSchema1, pSchema2, pCfg->update, &lastKeyAppend);

          if (cur->win.skey == TSKEY_INITIAL_VAL) {
            cur->win.skey = key;
          }

          cur->win.ekey = key;
          cur->lastKey = key + step;
          cur->mixBlock = true;

          moveToNextRowInMem(pCheckInfo);
          pos += step;
        } else {
          moveToNextRowInMem(pCheckInfo);
        }
      } else if ((key > tsArray[pos] && ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
                 (key < tsArray[pos] && !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = tsArray[pos];
        }

        int32_t end = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, key, order);
        assert(end != -1);

        if (tsArray[end] == key) {  // the value of key in cache equals to the end timestamp value, ignore it
#if 0
          if (pCfg->update == TD_ROW_DISCARD_UPDATE) {
            moveToNextRowInMem(pCheckInfo);
          } else {
            end -= step;
          }
#endif
          if (!TD_SUPPORT_UPDATE(pCfg->update)) {
            moveToNextRowInMem(pCheckInfo);
          } else {
            end -= step;
          }
        }

        int32_t qstart = 0, qend = 0;
        getQualifiedRowsPos(pTsdbReadHandle, pos, end, numOfRows, &qstart, &qend);

        if ((lastKeyAppend != TSKEY_INITIAL_VAL) &&
            (lastKeyAppend != (ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? tsArray[qstart] : tsArray[qend]))) {
          ++curRow;
        }
        numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, curRow, qstart, qend);
        pos += (qend - qstart + 1) * step;
        if (numOfRows > 0) {
          curRow = numOfRows - 1;
        }

        cur->win.ekey = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? tsArray[qend] : tsArray[qstart];
        cur->lastKey = cur->win.ekey + step;
        lastKeyAppend = cur->win.ekey;
      }
    } while (numOfRows < pTsdbReadHandle->outputCapacity);

    if (numOfRows < pTsdbReadHandle->outputCapacity) {
      /**
       * if cache is empty, load remain file block data. In contrast, if there are remain data in cache, do NOT
       * copy them all to result buffer, since it may be overlapped with file data block.
       */
      if (node == NULL ||
          ((TD_ROW_KEY((STSRow*)SL_GET_NODE_DATA(node)) > pTsdbReadHandle->window.ekey) &&
           ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
          ((TD_ROW_KEY((STSRow*)SL_GET_NODE_DATA(node)) < pTsdbReadHandle->window.ekey) &&
           !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
        // no data in cache or data in cache is greater than the ekey of time window, load data from file block
        if (cur->win.skey == TSKEY_INITIAL_VAL) {
          cur->win.skey = tsArray[pos];
        }

        int32_t start = -1, end = -1;
        getQualifiedRowsPos(pTsdbReadHandle, pos, endPos, numOfRows, &start, &end);

        numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, numOfRows, start, end);
        pos += (end - start + 1) * step;

        cur->win.ekey = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? tsArray[end] : tsArray[start];
        cur->lastKey = cur->win.ekey + step;
        cur->mixBlock = true;
      }
    }
  }

  cur->blockCompleted =
      (((pos > endPos || cur->lastKey > pTsdbReadHandle->window.ekey) && ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
       ((pos < endPos || cur->lastKey < pTsdbReadHandle->window.ekey) && !ASCENDING_TRAVERSE(pTsdbReadHandle->order)));

  if (!ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
    TSWAP(cur->win.skey, cur->win.ekey);
  }

  moveDataToFront(pTsdbReadHandle, numOfRows, numOfCols);
  updateInfoAfterMerge(pTsdbReadHandle, pCheckInfo, numOfRows, pos);
  doCheckGeneratedBlockRange(pTsdbReadHandle);

  tsdbDebug("%p uid:%" PRIu64 ", data block created, mixblock:%d, brange:%" PRIu64 "-%" PRIu64 " rows:%d, %s",
            pTsdbReadHandle, pCheckInfo->tableId, cur->mixBlock, cur->win.skey, cur->win.ekey, cur->rows,
            pTsdbReadHandle->idStr);
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
  taosMemoryFreeClear(pSupporter->numOfBlocksPerTable);
  taosMemoryFreeClear(pSupporter->blockIndexArray);

  for (int32_t i = 0; i < numOfTables; ++i) {
    STableBlockInfo* pBlockInfo = pSupporter->pDataBlockInfo[i];
    taosMemoryFreeClear(pBlockInfo);
  }

  taosMemoryFreeClear(pSupporter->pDataBlockInfo);
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
#if 0  // TODO: temporarily comment off requested by Dr. Liao
  if (pLeftBlockInfoEx->compBlock->offset == pRightBlockInfoEx->compBlock->offset &&
      pLeftBlockInfoEx->compBlock->last == pRightBlockInfoEx->compBlock->last) {
    tsdbError("error in header file, two block with same offset:%" PRId64, (int64_t)pLeftBlockInfoEx->compBlock->offset);
  }
#endif

  return pLeftBlockInfoEx->compBlock->offset > pRightBlockInfoEx->compBlock->offset ? 1 : -1;
}

static int32_t createDataBlocksInfo(STsdbReadHandle* pTsdbReadHandle, int32_t numOfBlocks, int32_t* numOfAllocBlocks) {
  size_t size = sizeof(STableBlockInfo) * numOfBlocks;

  if (pTsdbReadHandle->allocSize < size) {
    pTsdbReadHandle->allocSize = (int32_t)size;
    char* tmp = taosMemoryRealloc(pTsdbReadHandle->pDataBlockInfo, pTsdbReadHandle->allocSize);
    if (tmp == NULL) {
      return TSDB_CODE_TDB_OUT_OF_MEMORY;
    }

    pTsdbReadHandle->pDataBlockInfo = (STableBlockInfo*)tmp;
  }

  memset(pTsdbReadHandle->pDataBlockInfo, 0, size);
  *numOfAllocBlocks = numOfBlocks;

  // access data blocks according to the offset of each block in asc/desc order.
  int32_t numOfTables = (int32_t)taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);

  SBlockOrderSupporter sup = {0};
  sup.numOfTables = numOfTables;
  sup.numOfBlocksPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  sup.blockIndexArray = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  sup.pDataBlockInfo = taosMemoryCalloc(1, POINTER_BYTES * numOfTables);

  if (sup.numOfBlocksPerTable == NULL || sup.blockIndexArray == NULL || sup.pDataBlockInfo == NULL) {
    cleanBlockOrderSupporter(&sup, 0);
    return TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  int32_t cnt = 0;
  int32_t numOfQualTables = 0;

  for (int32_t j = 0; j < numOfTables; ++j) {
    STableCheckInfo* pTableCheck = (STableCheckInfo*)taosArrayGet(pTsdbReadHandle->pTableCheckInfo, j);
    if (pTableCheck->numOfBlocks <= 0) {
      continue;
    }

    SBlock* pBlock = pTableCheck->pCompInfo->blocks;
    sup.numOfBlocksPerTable[numOfQualTables] = pTableCheck->numOfBlocks;

    char* buf = taosMemoryMalloc(sizeof(STableBlockInfo) * pTableCheck->numOfBlocks);
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
    memcpy(pTsdbReadHandle->pDataBlockInfo, sup.pDataBlockInfo[0], sizeof(STableBlockInfo) * numOfBlocks);
    cleanBlockOrderSupporter(&sup, numOfQualTables);

    tsdbDebug("%p create data blocks info struct completed for 1 table, %d blocks not sorted %s", pTsdbReadHandle, cnt,
              pTsdbReadHandle->idStr);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %s", pTsdbReadHandle, cnt,
            numOfQualTables, pTsdbReadHandle->idStr);

  assert(cnt <= numOfBlocks && numOfQualTables <= numOfTables);  // the pTableQueryInfo[j]->numOfBlocks may be 0
  sup.numOfTables = numOfQualTables;

  SMultiwayMergeTreeInfo* pTree = NULL;
  uint8_t                 ret = tMergeTreeCreate(&pTree, sup.numOfTables, &sup, dataBlockOrderCompar);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanBlockOrderSupporter(&sup, numOfTables);
    return TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;

  while (numOfTotal < cnt) {
    int32_t pos = tMergeTreeGetChosenIndex(pTree);
    int32_t index = sup.blockIndexArray[pos]++;

    STableBlockInfo* pBlocksInfo = sup.pDataBlockInfo[pos];
    pTsdbReadHandle->pDataBlockInfo[numOfTotal++] = pBlocksInfo[index];

    // set data block index overflow, in order to disable the offset comparator
    if (sup.blockIndexArray[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.blockIndexArray[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
  }

  /*
   * available when no import exists
   * for(int32_t i = 0; i < cnt - 1; ++i) {
   *   assert((*pDataBlockInfo)[i].compBlock->offset < (*pDataBlockInfo)[i+1].compBlock->offset);
   * }
   */

  tsdbDebug("%p %d data blocks sort completed, %s", pTsdbReadHandle, cnt, pTsdbReadHandle->idStr);
  cleanBlockOrderSupporter(&sup, numOfTables);
  taosMemoryFree(pTree);

  return TSDB_CODE_SUCCESS;
}

static int32_t getFirstFileDataBlock(STsdbReadHandle* pTsdbReadHandle, bool* exists);

static int32_t getDataBlockRv(STsdbReadHandle* pTsdbReadHandle, STableBlockInfo* pNext, bool* exists) {
  int32_t        step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;
  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  while (1) {
    int32_t code = loadFileDataBlock(pTsdbReadHandle, pNext->compBlock, pNext->pTableCheckInfo, exists);
    if (code != TSDB_CODE_SUCCESS || *exists) {
      return code;
    }

    if ((cur->slot == pTsdbReadHandle->numOfBlocks - 1 && ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
        (cur->slot == 0 && !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
      // all data blocks in current file has been checked already, try next file if exists
      return getFirstFileDataBlock(pTsdbReadHandle, exists);
    } else {  // next block of the same file
      cur->slot += step;
      cur->mixBlock = false;
      cur->blockCompleted = false;
      pNext = &pTsdbReadHandle->pDataBlockInfo[cur->slot];
    }
  }
}

static int32_t getFirstFileDataBlock(STsdbReadHandle* pTsdbReadHandle, bool* exists) {
  pTsdbReadHandle->numOfBlocks = 0;
  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  int32_t code = TSDB_CODE_SUCCESS;

  int32_t numOfBlocks = 0;
  int32_t numOfTables = (int32_t)taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);

  STsdbKeepCfg* pCfg = REPO_KEEP_CFG(pTsdbReadHandle->pTsdb);
  STimeWindow   win = TSWINDOW_INITIALIZER;

  while (true) {
    tsdbRLockFS(REPO_FS(pTsdbReadHandle->pTsdb));

    if ((pTsdbReadHandle->pFileGroup = tsdbFSIterNext(&pTsdbReadHandle->fileIter)) == NULL) {
      tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));
      break;
    }

    tsdbGetFidKeyRange(pCfg->days, pCfg->precision, pTsdbReadHandle->pFileGroup->fid, &win.skey, &win.ekey);

    // current file are not overlapped with query time window, ignore remain files
    if ((ASCENDING_TRAVERSE(pTsdbReadHandle->order) && win.skey > pTsdbReadHandle->window.ekey) ||
        (!ASCENDING_TRAVERSE(pTsdbReadHandle->order) && win.ekey < pTsdbReadHandle->window.ekey)) {
      tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, %s", pTsdbReadHandle,
                pTsdbReadHandle->window.skey, pTsdbReadHandle->window.ekey, pTsdbReadHandle->idStr);
      pTsdbReadHandle->pFileGroup = NULL;
      assert(pTsdbReadHandle->numOfBlocks == 0);
      break;
    }

    if (tsdbSetAndOpenReadFSet(&pTsdbReadHandle->rhelper, pTsdbReadHandle->pFileGroup) < 0) {
      tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));
      code = terrno;
      break;
    }

    tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));

    if (tsdbLoadBlockIdx(&pTsdbReadHandle->rhelper) < 0) {
      code = terrno;
      break;
    }

    if ((code = getFileCompInfo(pTsdbReadHandle, &numOfBlocks)) != TSDB_CODE_SUCCESS) {
      break;
    }

    tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, %s", pTsdbReadHandle, numOfBlocks, numOfTables,
              pTsdbReadHandle->pFileGroup->fid, pTsdbReadHandle->idStr);

    assert(numOfBlocks >= 0);
    if (numOfBlocks == 0) {
      continue;
    }

    // todo return error code to query engine
    if ((code = createDataBlocksInfo(pTsdbReadHandle, numOfBlocks, &pTsdbReadHandle->numOfBlocks)) !=
        TSDB_CODE_SUCCESS) {
      break;
    }

    assert(numOfBlocks >= pTsdbReadHandle->numOfBlocks);
    if (pTsdbReadHandle->numOfBlocks > 0) {
      break;
    }
  }

  // no data in file anymore
  if (pTsdbReadHandle->numOfBlocks <= 0 || code != TSDB_CODE_SUCCESS) {
    if (code == TSDB_CODE_SUCCESS) {
      assert(pTsdbReadHandle->pFileGroup == NULL);
    }

    cur->fid = INT32_MIN;  // denote that there are no data in file anymore
    *exists = false;
    return code;
  }

  assert(pTsdbReadHandle->pFileGroup != NULL && pTsdbReadHandle->numOfBlocks > 0);
  cur->slot = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 0 : pTsdbReadHandle->numOfBlocks - 1;
  cur->fid = pTsdbReadHandle->pFileGroup->fid;

  STableBlockInfo* pBlockInfo = &pTsdbReadHandle->pDataBlockInfo[cur->slot];
  return getDataBlockRv(pTsdbReadHandle, pBlockInfo, exists);
}

static bool isEndFileDataBlock(SQueryFilePos* cur, int32_t numOfBlocks, bool ascTrav) {
  assert(cur != NULL && numOfBlocks > 0);
  return (cur->slot == numOfBlocks - 1 && ascTrav) || (cur->slot == 0 && !ascTrav);
}

static void moveToNextDataBlockInCurrentFile(STsdbReadHandle* pTsdbReadHandle) {
  int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;

  SQueryFilePos* cur = &pTsdbReadHandle->cur;
  assert(cur->slot < pTsdbReadHandle->numOfBlocks && cur->slot >= 0);

  cur->slot += step;
  cur->mixBlock = false;
  cur->blockCompleted = false;
}

int32_t tsdbGetFileBlocksDistInfo(tsdbReaderT* queryHandle, STableBlockDistInfo* pTableBlockInfo) {
  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)queryHandle;

  pTableBlockInfo->totalSize = 0;
  pTableBlockInfo->totalRows = 0;

  STsdbFS* pFileHandle = REPO_FS(pTsdbReadHandle->pTsdb);

  // find the start data block in file
  pTsdbReadHandle->locateStart = true;
  STsdbKeepCfg* pCfg = REPO_KEEP_CFG(pTsdbReadHandle->pTsdb);
  int32_t       fid = getFileIdFromKey(pTsdbReadHandle->window.skey, pCfg->days, pCfg->precision);

  tsdbRLockFS(pFileHandle);
  tsdbFSIterInit(&pTsdbReadHandle->fileIter, pFileHandle, pTsdbReadHandle->order);
  tsdbFSIterSeek(&pTsdbReadHandle->fileIter, fid);
  tsdbUnLockFS(pFileHandle);

  pTableBlockInfo->numOfFiles += 1;

  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     numOfBlocks = 0;
  int32_t     numOfTables = (int32_t)taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
  int         defaultRows = 4096;  // TSDB_DEFAULT_BLOCK_ROWS(pCfg->maxRowsPerFileBlock);
  STimeWindow win = TSWINDOW_INITIALIZER;

  bool ascTraverse = ASCENDING_TRAVERSE(pTsdbReadHandle->order);

  while (true) {
    numOfBlocks = 0;
    tsdbRLockFS(REPO_FS(pTsdbReadHandle->pTsdb));

    if ((pTsdbReadHandle->pFileGroup = tsdbFSIterNext(&pTsdbReadHandle->fileIter)) == NULL) {
      tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));
      break;
    }

    tsdbGetFidKeyRange(pCfg->days, pCfg->precision, pTsdbReadHandle->pFileGroup->fid, &win.skey, &win.ekey);

    // current file are not overlapped with query time window, ignore remain files
    if ((ascTraverse && win.skey > pTsdbReadHandle->window.ekey) ||
        (!ascTraverse && win.ekey < pTsdbReadHandle->window.ekey)) {
      tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, %s", pTsdbReadHandle,
                pTsdbReadHandle->window.skey, pTsdbReadHandle->window.ekey, pTsdbReadHandle->idStr);
      pTsdbReadHandle->pFileGroup = NULL;
      break;
    }

    pTableBlockInfo->numOfFiles += 1;
    if (tsdbSetAndOpenReadFSet(&pTsdbReadHandle->rhelper, pTsdbReadHandle->pFileGroup) < 0) {
      tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));
      code = terrno;
      break;
    }

    tsdbUnLockFS(REPO_FS(pTsdbReadHandle->pTsdb));

    if (tsdbLoadBlockIdx(&pTsdbReadHandle->rhelper) < 0) {
      code = terrno;
      break;
    }

    if ((code = getFileCompInfo(pTsdbReadHandle, &numOfBlocks)) != TSDB_CODE_SUCCESS) {
      break;
    }

    tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, %s", pTsdbReadHandle, numOfBlocks, numOfTables,
              pTsdbReadHandle->pFileGroup->fid, pTsdbReadHandle->idStr);

    if (numOfBlocks == 0) {
      continue;
    }

    for (int32_t i = 0; i < numOfTables; ++i) {
      STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);

      SBlock* pBlock = pCheckInfo->pCompInfo->blocks;
      for (int32_t j = 0; j < pCheckInfo->numOfBlocks; ++j) {
        pTableBlockInfo->totalSize += pBlock[j].len;

        int32_t numOfRows = pBlock[j].numOfRows;
        pTableBlockInfo->totalRows += numOfRows;
        if (numOfRows > pTableBlockInfo->maxRows) {
          pTableBlockInfo->maxRows = numOfRows;
        }

        if (numOfRows < pTableBlockInfo->minRows) {
          pTableBlockInfo->minRows = numOfRows;
        }

        if (numOfRows < defaultRows) {
          pTableBlockInfo->numOfSmallBlocks += 1;
        }
        //        int32_t  stepIndex = (numOfRows-1)/TSDB_BLOCK_DIST_STEP_ROWS;
        //        SFileBlockInfo *blockInfo = (SFileBlockInfo*)taosArrayGet(pTableBlockInfo->dataBlockInfos, stepIndex);
        //        blockInfo->numBlocksOfStep++;
      }
    }
  }

  return code;
}

static int32_t getDataBlocksInFiles(STsdbReadHandle* pTsdbReadHandle, bool* exists) {
  STsdbFS*       pFileHandle = REPO_FS(pTsdbReadHandle->pTsdb);
  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  // find the start data block in file
  if (!pTsdbReadHandle->locateStart) {
    pTsdbReadHandle->locateStart = true;
    STsdbKeepCfg* pCfg = REPO_KEEP_CFG(pTsdbReadHandle->pTsdb);
    int32_t       fid = getFileIdFromKey(pTsdbReadHandle->window.skey, pCfg->days, pCfg->precision);

    tsdbRLockFS(pFileHandle);
    tsdbFSIterInit(&pTsdbReadHandle->fileIter, pFileHandle, pTsdbReadHandle->order);
    tsdbFSIterSeek(&pTsdbReadHandle->fileIter, fid);
    tsdbUnLockFS(pFileHandle);

    return getFirstFileDataBlock(pTsdbReadHandle, exists);
  } else {
    // check if current file block is all consumed
    STableBlockInfo* pBlockInfo = &pTsdbReadHandle->pDataBlockInfo[cur->slot];
    STableCheckInfo* pCheckInfo = pBlockInfo->pTableCheckInfo;

    // current block is done, try next
    if ((!cur->mixBlock) || cur->blockCompleted) {
      // all data blocks in current file has been checked already, try next file if exists
    } else {
      tsdbDebug("%p continue in current data block, index:%d, pos:%d, %s", pTsdbReadHandle, cur->slot, cur->pos,
                pTsdbReadHandle->idStr);
      int32_t code = handleDataMergeIfNeeded(pTsdbReadHandle, pBlockInfo->compBlock, pCheckInfo);
      *exists = (pTsdbReadHandle->realNumOfRows > 0);

      if (code != TSDB_CODE_SUCCESS || *exists) {
        return code;
      }
    }

    // current block is empty, try next block in file
    // all data blocks in current file has been checked already, try next file if exists
    if (isEndFileDataBlock(cur, pTsdbReadHandle->numOfBlocks, ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
      return getFirstFileDataBlock(pTsdbReadHandle, exists);
    } else {
      moveToNextDataBlockInCurrentFile(pTsdbReadHandle);
      STableBlockInfo* pNext = &pTsdbReadHandle->pDataBlockInfo[cur->slot];
      return getDataBlockRv(pTsdbReadHandle, pNext, exists);
    }
  }
}

static bool doHasDataInBuffer(STsdbReadHandle* pTsdbReadHandle) {
  size_t numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);

  while (pTsdbReadHandle->activeIndex < numOfTables) {
    if (hasMoreDataInCache(pTsdbReadHandle)) {
      return true;
    }

    pTsdbReadHandle->activeIndex += 1;
  }

  return false;
}

// todo not unref yet, since it is not support multi-group interpolation query
static UNUSED_FUNC void changeQueryHandleForInterpQuery(tsdbReaderT pHandle) {
  // filter the queried time stamp in the first place
  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)pHandle;

  // starts from the buffer in case of descending timestamp order check data blocks
  size_t numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);

  int32_t i = 0;
  while (i < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);

    // the first qualified table for interpolation query
    //    if ((pTsdbReadHandle->window.skey <= pCheckInfo->pTableObj->lastKey) &&
    //        (pCheckInfo->pTableObj->lastKey != TSKEY_INITIAL_VAL)) {
    //      break;
    //    }

    i++;
  }

  // there are no data in all the tables
  if (i == numOfTables) {
    return;
  }

  STableCheckInfo info = *(STableCheckInfo*)taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);
  taosArrayClear(pTsdbReadHandle->pTableCheckInfo);

  info.lastKey = pTsdbReadHandle->window.skey;
  taosArrayPush(pTsdbReadHandle->pTableCheckInfo, &info);
}

static int tsdbReadRowsFromCache(STableCheckInfo* pCheckInfo, TSKEY maxKey, int maxRowsToRead, STimeWindow* win,
                                 STsdbReadHandle* pTsdbReadHandle) {
  int       numOfRows = 0;
  int       curRows = 0;
  int32_t   numOfCols = (int32_t)taosArrayGetSize(pTsdbReadHandle->pColumns);
  STsdbCfg* pCfg = REPO_CFG(pTsdbReadHandle->pTsdb);
  win->skey = TSKEY_INITIAL_VAL;

  int64_t   st = taosGetTimestampUs();
  int16_t   rv = -1;
  STSchema* pSchema = NULL;
  TSKEY     lastRowKey = TSKEY_INITIAL_VAL;

  do {
    STSRow* row = getSRowInTableMem(pCheckInfo, pTsdbReadHandle->order, pCfg->update, NULL, TD_VER_MAX);
    if (row == NULL) {
      break;
    }

    TSKEY key = TD_ROW_KEY(row);
    if ((key > maxKey && ASCENDING_TRAVERSE(pTsdbReadHandle->order)) ||
        (key < maxKey && !ASCENDING_TRAVERSE(pTsdbReadHandle->order))) {
      tsdbDebug("%p key:%" PRIu64 " beyond qrange:%" PRId64 " - %" PRId64 ", no more data in buffer", pTsdbReadHandle,
                key, pTsdbReadHandle->window.skey, pTsdbReadHandle->window.ekey);

      break;
    }

    if (win->skey == INT64_MIN) {
      win->skey = key;
    }

    win->ekey = key;
    if (rv != TD_ROW_SVER(row)) {
      pSchema = metaGetTbTSchema(REPO_META(pTsdbReadHandle->pTsdb), pCheckInfo->tableId, 0);
      rv = TD_ROW_SVER(row);
    }
    numOfRows += mergeTwoRowFromMem(pTsdbReadHandle, maxRowsToRead, &curRows, row, NULL, numOfCols, pCheckInfo->tableId,
                                    pSchema, NULL, pCfg->update, &lastRowKey);

    if (numOfRows >= maxRowsToRead) {
      moveToNextRowInMem(pCheckInfo);
      break;
    }

  } while (moveToNextRowInMem(pCheckInfo));

  taosMemoryFreeClear(pSchema);  // free the STSChema

  assert(numOfRows <= maxRowsToRead);

  // if the buffer is not full in case of descending order query, move the data in the front of the buffer
  if (!ASCENDING_TRAVERSE(pTsdbReadHandle->order) && numOfRows < maxRowsToRead) {
    int32_t emptySize = maxRowsToRead - numOfRows;

    for (int32_t i = 0; i < numOfCols; ++i) {
      SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);
      memmove((char*)pColInfo->pData, (char*)pColInfo->pData + emptySize * pColInfo->info.bytes,
              numOfRows * pColInfo->info.bytes);
    }
  }

  int64_t elapsedTime = taosGetTimestampUs() - st;
  tsdbDebug("%p build data block from cache completed, elapsed time:%" PRId64 " us, numOfRows:%d, numOfCols:%d, %s",
            pTsdbReadHandle, elapsedTime, numOfRows, numOfCols, pTsdbReadHandle->idStr);

  return numOfRows;
}

static int32_t getAllTableList(SMeta* pMeta, uint64_t uid, SArray* list) {
  SMCtbCursor* pCur = metaOpenCtbCursor(pMeta, uid);

  while (1) {
    tb_uid_t id = metaCtbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    STableKeyInfo info = {.lastKey = TSKEY_INITIAL_VAL, uid = id};
    taosArrayPush(list, &info);
  }

  metaCloseCtbCurosr(pCur);
  return TSDB_CODE_SUCCESS;
}

static void destroyHelper(void* param) {
  if (param == NULL) {
    return;
  }

  //  tQueryInfo* pInfo = (tQueryInfo*)param;
  //  if (pInfo->optr != TSDB_RELATION_IN) {
  //    taosMemoryFreeClear(pInfo->q);
  //  } else {
  //    taosHashCleanup((SHashObj *)(pInfo->q));
  //  }

  taosMemoryFree(param);
}

#define TSDB_PREV_ROW 0x1
#define TSDB_NEXT_ROW 0x2

static bool loadBlockOfActiveTable(STsdbReadHandle* pTsdbReadHandle) {
  if (pTsdbReadHandle->checkFiles) {
    // check if the query range overlaps with the file data block
    bool exists = true;

    int32_t code = getDataBlocksInFiles(pTsdbReadHandle, &exists);
    if (code != TSDB_CODE_SUCCESS) {
      pTsdbReadHandle->checkFiles = false;
      return false;
    }

    if (exists) {
      tsdbRetrieveDataBlock((tsdbReaderT*)pTsdbReadHandle, NULL);
      if (pTsdbReadHandle->currentLoadExternalRows && pTsdbReadHandle->window.skey == pTsdbReadHandle->window.ekey) {
        SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, 0);
        assert(*(int64_t*)pColInfo->pData == pTsdbReadHandle->window.skey);
      }

      pTsdbReadHandle->currentLoadExternalRows = false;  // clear the flag, since the exact matched row is found.
      return exists;
    }

    pTsdbReadHandle->checkFiles = false;
  }

  if (hasMoreDataInCache(pTsdbReadHandle)) {
    pTsdbReadHandle->currentLoadExternalRows = false;
    return true;
  }

  // current result is empty
  if (pTsdbReadHandle->currentLoadExternalRows && pTsdbReadHandle->window.skey == pTsdbReadHandle->window.ekey &&
      pTsdbReadHandle->cur.rows == 0) {
    //    STsdbMemTable* pMemRef = pTsdbReadHandle->pMemTable;

    //    doGetExternalRow(pTsdbReadHandle, TSDB_PREV_ROW, pMemRef);
    //    doGetExternalRow(pTsdbReadHandle, TSDB_NEXT_ROW, pMemRef);

    bool result = tsdbGetExternalRow(pTsdbReadHandle);

    //    pTsdbReadHandle->prev = doFreeColumnInfoData(pTsdbReadHandle->prev);
    //    pTsdbReadHandle->next = doFreeColumnInfoData(pTsdbReadHandle->next);
    pTsdbReadHandle->currentLoadExternalRows = false;

    return result;
  }

  return false;
}

static bool loadCachedLastRow(STsdbReadHandle* pTsdbReadHandle) {
  // the last row is cached in buffer, return it directly.
  // here note that the pTsdbReadHandle->window must be the TS_INITIALIZER
  int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pTsdbReadHandle));
  size_t  numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
  assert(numOfTables > 0 && numOfCols > 0);

  SQueryFilePos* cur = &pTsdbReadHandle->cur;

  STSRow* pRow = NULL;
  TSKEY   key = TSKEY_INITIAL_VAL;
  int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;
  TSKEY   lastRowKey = TSKEY_INITIAL_VAL;
  int32_t curRow = 0;

  if (++pTsdbReadHandle->activeIndex < numOfTables) {
    STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, pTsdbReadHandle->activeIndex);
    //    int32_t ret = tsdbGetCachedLastRow(pCheckInfo->pTableObj, &pRow, &key);
    //    if (ret != TSDB_CODE_SUCCESS) {
    //      return false;
    //    }
    mergeTwoRowFromMem(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, &curRow, pRow, NULL, numOfCols,
                       pCheckInfo->tableId, NULL, NULL, true, &lastRowKey);
    taosMemoryFreeClear(pRow);

    // update the last key value
    pCheckInfo->lastKey = key + step;

    cur->rows = 1;  // only one row
    cur->lastKey = key + step;
    cur->mixBlock = true;
    cur->win.skey = key;
    cur->win.ekey = key;

    return true;
  }

  return false;
}

// static bool loadCachedLast(STsdbReadHandle* pTsdbReadHandle) {
//  // the last row is cached in buffer, return it directly.
//  // here note that the pTsdbReadHandle->window must be the TS_INITIALIZER
//  int32_t tgNumOfCols = (int32_t)QH_GET_NUM_OF_COLS(pTsdbReadHandle);
//  size_t  numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
//  int32_t numOfRows = 0;
//  assert(numOfTables > 0 && tgNumOfCols > 0);
//  SQueryFilePos* cur = &pTsdbReadHandle->cur;
//  TSKEY priKey = TSKEY_INITIAL_VAL;
//  int32_t priIdx = -1;
//  SColumnInfoData* pColInfo = NULL;
//
//  while (++pTsdbReadHandle->activeIndex < numOfTables) {
//    STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, pTsdbReadHandle->activeIndex);
//    STable* pTable = pCheckInfo->pTableObj;
//    char* pData = NULL;
//
//    int32_t numOfCols = pTable->maxColNum;
//
//    if (pTable->lastCols == NULL || pTable->maxColNum <= 0) {
//      tsdbWarn("no last cached for table %s, uid:%" PRIu64 ",tid:%d", pTable->name->data, pTable->uid,
//      pTable->tableId); continue;
//    }
//
//    int32_t i = 0, j = 0;
//    while(i < tgNumOfCols && j < numOfCols) {
//      pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);
//      if (pTable->lastCols[j].colId < pColInfo->info.colId) {
//        j++;
//        continue;
//      } else if (pTable->lastCols[j].colId > pColInfo->info.colId) {
//        i++;
//        continue;
//      }
//
//      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
//
//      if (pTable->lastCols[j].bytes > 0) {
//        void* value = pTable->lastCols[j].pData;
//        switch (pColInfo->info.type) {
//          case TSDB_DATA_TYPE_BINARY:
//          case TSDB_DATA_TYPE_NCHAR:
//            memcpy(pData, value, varDataTLen(value));
//            break;
//          case TSDB_DATA_TYPE_NULL:
//          case TSDB_DATA_TYPE_BOOL:
//          case TSDB_DATA_TYPE_TINYINT:
//          case TSDB_DATA_TYPE_UTINYINT:
//            *(uint8_t *)pData = *(uint8_t *)value;
//            break;
//          case TSDB_DATA_TYPE_SMALLINT:
//          case TSDB_DATA_TYPE_USMALLINT:
//            *(uint16_t *)pData = *(uint16_t *)value;
//            break;
//          case TSDB_DATA_TYPE_INT:
//          case TSDB_DATA_TYPE_UINT:
//            *(uint32_t *)pData = *(uint32_t *)value;
//            break;
//          case TSDB_DATA_TYPE_BIGINT:
//          case TSDB_DATA_TYPE_UBIGINT:
//            *(uint64_t *)pData = *(uint64_t *)value;
//            break;
//          case TSDB_DATA_TYPE_FLOAT:
//            SET_FLOAT_PTR(pData, value);
//            break;
//          case TSDB_DATA_TYPE_DOUBLE:
//            SET_DOUBLE_PTR(pData, value);
//            break;
//          case TSDB_DATA_TYPE_TIMESTAMP:
//            if (pColInfo->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
//              priKey = tdGetKey(*(TKEY *)value);
//              priIdx = i;
//
//              i++;
//              j++;
//              continue;
//            } else {
//              *(TSKEY *)pData = *(TSKEY *)value;
//            }
//            break;
//          default:
//            memcpy(pData, value, pColInfo->info.bytes);
//        }
//
//        for (int32_t n = 0; n < tgNumOfCols; ++n) {
//          if (n == i) {
//            continue;
//          }
//
//          pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, n);
//          pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;;
//
//          if (pColInfo->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
////            *(TSKEY *)pData = pTable->lastCols[j].ts;
//            continue;
//          }
//
//          if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
//            setVardataNull(pData, pColInfo->info.type);
//          } else {
//            setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
//          }
//        }
//
//        numOfRows++;
//        assert(numOfRows < pTsdbReadHandle->outputCapacity);
//      }
//
//      i++;
//      j++;
//    }
//
//    // leave the real ts column as the last row, because last function only (not stable) use the last row as res
//    if (priKey != TSKEY_INITIAL_VAL) {
//      pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, priIdx);
//      pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;
//
//      *(TSKEY *)pData = priKey;
//
//      for (int32_t n = 0; n < tgNumOfCols; ++n) {
//        if (n == priIdx) {
//          continue;
//        }
//
//        pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, n);
//        pData = (char*)pColInfo->pData + numOfRows * pColInfo->info.bytes;;
//
//        assert (pColInfo->info.colId != PRIMARYKEY_TIMESTAMP_COL_ID);
//
//        if (pColInfo->info.type == TSDB_DATA_TYPE_BINARY || pColInfo->info.type == TSDB_DATA_TYPE_NCHAR) {
//          setVardataNull(pData, pColInfo->info.type);
//        } else {
//          setNull(pData, pColInfo->info.type, pColInfo->info.bytes);
//        }
//      }
//
//      numOfRows++;
//    }
//
//    if (numOfRows > 0) {
//      cur->rows     = numOfRows;
//      cur->mixBlock = true;
//
//      return true;
//    }
//  }
//
//  return false;
//}

static bool loadDataBlockFromTableSeq(STsdbReadHandle* pTsdbReadHandle) {
  size_t numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
  assert(numOfTables > 0);

  int64_t stime = taosGetTimestampUs();

  while (pTsdbReadHandle->activeIndex < numOfTables) {
    if (loadBlockOfActiveTable(pTsdbReadHandle)) {
      return true;
    }

    STableCheckInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, pTsdbReadHandle->activeIndex);
    pCheckInfo->numOfBlocks = 0;

    pTsdbReadHandle->activeIndex += 1;
    pTsdbReadHandle->locateStart = false;
    pTsdbReadHandle->checkFiles = true;
    pTsdbReadHandle->cur.rows = 0;
    pTsdbReadHandle->currentLoadExternalRows = pTsdbReadHandle->loadExternalRow;

    terrno = TSDB_CODE_SUCCESS;

    int64_t elapsedTime = taosGetTimestampUs() - stime;
    pTsdbReadHandle->cost.checkForNextTime += elapsedTime;
  }

  return false;
}

// handle data in cache situation
bool tsdbNextDataBlock(tsdbReaderT pHandle) {
  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)pHandle;

  for (int32_t i = 0; i < taosArrayGetSize(pTsdbReadHandle->pColumns); ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pTsdbReadHandle->pColumns, i);
    colInfoDataCleanup(pColInfo, pTsdbReadHandle->outputCapacity);
  }

  if (emptyQueryTimewindow(pTsdbReadHandle)) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, %s", pTsdbReadHandle,
              pTsdbReadHandle->idStr);
    return false;
  }

  int64_t stime = taosGetTimestampUs();
  int64_t elapsedTime = stime;

  // TODO refactor: remove "type"
  if (pTsdbReadHandle->type == TSDB_QUERY_TYPE_LAST) {
    if (pTsdbReadHandle->cachelastrow == TSDB_CACHED_TYPE_LASTROW) {
      //      return loadCachedLastRow(pTsdbReadHandle);
    } else if (pTsdbReadHandle->cachelastrow == TSDB_CACHED_TYPE_LAST) {
      //      return loadCachedLast(pTsdbReadHandle);
    }
  }

  if (pTsdbReadHandle->loadType == BLOCK_LOAD_TABLE_SEQ_ORDER) {
    return loadDataBlockFromTableSeq(pTsdbReadHandle);
  } else {  // loadType == RR and Offset Order
    if (pTsdbReadHandle->checkFiles) {
      // check if the query range overlaps with the file data block
      bool exists = true;

      int32_t code = getDataBlocksInFiles(pTsdbReadHandle, &exists);
      if (code != TSDB_CODE_SUCCESS) {
        pTsdbReadHandle->activeIndex = 0;
        pTsdbReadHandle->checkFiles = false;

        return false;
      }

      if (exists) {
        pTsdbReadHandle->cost.checkForNextTime += (taosGetTimestampUs() - stime);
        return exists;
      }

      pTsdbReadHandle->activeIndex = 0;
      pTsdbReadHandle->checkFiles = false;
    }

    // TODO: opt by consider the scan order
    bool ret = doHasDataInBuffer(pTsdbReadHandle);
    terrno = TSDB_CODE_SUCCESS;

    elapsedTime = taosGetTimestampUs() - stime;
    pTsdbReadHandle->cost.checkForNextTime += elapsedTime;
    return ret;
  }
}

// static int32_t doGetExternalRow(STsdbReadHandle* pTsdbReadHandle, int16_t type, STsdbMemTable* pMemRef) {
//  STsdbReadHandle* pSecQueryHandle = NULL;
//
//  if (type == TSDB_PREV_ROW && pTsdbReadHandle->prev) {
//    return TSDB_CODE_SUCCESS;
//  }
//
//  if (type == TSDB_NEXT_ROW && pTsdbReadHandle->next) {
//    return TSDB_CODE_SUCCESS;
//  }
//
//  // prepare the structure
//  int32_t numOfCols = (int32_t) QH_GET_NUM_OF_COLS(pTsdbReadHandle);
//
//  if (type == TSDB_PREV_ROW) {
//    pTsdbReadHandle->prev = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
//    if (pTsdbReadHandle->prev == NULL) {
//      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
//      goto out_of_memory;
//    }
//  } else {
//    pTsdbReadHandle->next = taosArrayInit(numOfCols, sizeof(SColumnInfoData));
//    if (pTsdbReadHandle->next == NULL) {
//      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
//      goto out_of_memory;
//    }
//  }
//
//  SArray* row = (type == TSDB_PREV_ROW)? pTsdbReadHandle->prev : pTsdbReadHandle->next;
//
//  for (int32_t i = 0; i < numOfCols; ++i) {
//    SColumnInfoData* pCol = taosArrayGet(pTsdbReadHandle->pColumns, i);
//
//    SColumnInfoData colInfo = {{0}, 0};
//    colInfo.info = pCol->info;
//    colInfo.pData = taosMemoryCalloc(1, pCol->info.bytes);
//    if (colInfo.pData == NULL) {
//      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
//      goto out_of_memory;
//    }
//
//    taosArrayPush(row, &colInfo);
//  }
//
//  // load the previous row
//  SQueryTableDataCond cond = {.numOfCols = numOfCols, .loadExternalRows = false, .type = BLOCK_LOAD_OFFSET_SEQ_ORDER};
//  if (type == TSDB_PREV_ROW) {
//    cond.order = TSDB_ORDER_DESC;
//    cond.twindow = (STimeWindow){pTsdbReadHandle->window.skey, INT64_MIN};
//  } else {
//    cond.order = TSDB_ORDER_ASC;
//    cond.twindow = (STimeWindow){pTsdbReadHandle->window.skey, INT64_MAX};
//  }
//
//  cond.colList = taosMemoryCalloc(cond.numOfCols, sizeof(SColumnInfo));
//  if (cond.colList == NULL) {
//    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
//    goto out_of_memory;
//  }
//
//  for (int32_t i = 0; i < cond.numOfCols; ++i) {
//    SColumnInfoData* pColInfoData = taosArrayGet(pTsdbReadHandle->pColumns, i);
//    memcpy(&cond.colList[i], &pColInfoData->info, sizeof(SColumnInfo));
//  }
//
//  pSecQueryHandle = tsdbQueryTablesImpl(pTsdbReadHandle->pTsdb, &cond, pTsdbReadHandle->idStr, pMemRef);
//  taosMemoryFreeClear(cond.colList);
//
//  // current table, only one table
//  STableCheckInfo* pCurrent = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, pTsdbReadHandle->activeIndex);
//
//  SArray* psTable = NULL;
//  pSecQueryHandle->pTableCheckInfo = createCheckInfoFromCheckInfo(pCurrent, pSecQueryHandle->window.skey, &psTable);
//  if (pSecQueryHandle->pTableCheckInfo == NULL) {
//    taosArrayDestroy(psTable);
//    terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
//    goto out_of_memory;
//  }
//
//
//  tsdbMayTakeMemSnapshot(pSecQueryHandle, psTable);
//  if (!tsdbNextDataBlock((void*)pSecQueryHandle)) {
//    // no result in current query, free the corresponding result rows structure
//    if (type == TSDB_PREV_ROW) {
//      pTsdbReadHandle->prev = doFreeColumnInfoData(pTsdbReadHandle->prev);
//    } else {
//      pTsdbReadHandle->next = doFreeColumnInfoData(pTsdbReadHandle->next);
//    }
//
//    goto out_of_memory;
//  }
//
//  SDataBlockInfo blockInfo = {{0}, 0};
//  tsdbRetrieveDataBlockInfo((void*)pSecQueryHandle, &blockInfo);
//  tsdbRetrieveDataBlock((void*)pSecQueryHandle, pSecQueryHandle->defaultLoadColumn);
//
//  row = (type == TSDB_PREV_ROW)? pTsdbReadHandle->prev:pTsdbReadHandle->next;
//  int32_t pos = (type == TSDB_PREV_ROW)?pSecQueryHandle->cur.rows - 1:0;
//
//  for (int32_t i = 0; i < numOfCols; ++i) {
//    SColumnInfoData* pCol = taosArrayGet(row, i);
//    SColumnInfoData* s = taosArrayGet(pSecQueryHandle->pColumns, i);
//    memcpy((char*)pCol->pData, (char*)s->pData + s->info.bytes * pos, pCol->info.bytes);
//  }
//
// out_of_memory:
//  tsdbCleanupReadHandle(pSecQueryHandle);
//  return terrno;
//}

bool tsdbGetExternalRow(tsdbReaderT pHandle) {
  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)pHandle;
  SQueryFilePos*   cur = &pTsdbReadHandle->cur;

  cur->fid = INT32_MIN;
  cur->mixBlock = true;
  if (pTsdbReadHandle->prev == NULL || pTsdbReadHandle->next == NULL) {
    cur->rows = 0;
    return false;
  }

  int32_t numOfCols = (int32_t)QH_GET_NUM_OF_COLS(pTsdbReadHandle);
  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pColInfoData = taosArrayGet(pTsdbReadHandle->pColumns, i);
    SColumnInfoData* first = taosArrayGet(pTsdbReadHandle->prev, i);

    memcpy(pColInfoData->pData, first->pData, pColInfoData->info.bytes);

    SColumnInfoData* sec = taosArrayGet(pTsdbReadHandle->next, i);
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
// int32_t tsdbGetCachedLastRow(STable* pTable, STSRow** pRes, TSKEY* lastKey) {
//  int32_t code = TSDB_CODE_SUCCESS;
//
//  TSDB_RLOCK_TABLE(pTable);
//
//  if (!pTable->lastRow) {
//    code = TSDB_CODE_TDB_NO_CACHE_LAST_ROW;
//    goto out;
//  }
//
//  if (pRes) {
//    *pRes = tdMemRowDup(pTable->lastRow);
//    if (*pRes == NULL) {
//      code = TSDB_CODE_TDB_OUT_OF_MEMORY;
//    }
//  }
//
// out:
//  TSDB_RUNLOCK_TABLE(pTable);
//  return code;
//}

bool isTsdbCacheLastRow(tsdbReaderT* pReader) {
  return ((STsdbReadHandle*)pReader)->cachelastrow > TSDB_CACHED_TYPE_NONE;
}

int32_t checkForCachedLastRow(STsdbReadHandle* pTsdbReadHandle, STableGroupInfo* groupList) {
  assert(pTsdbReadHandle != NULL && groupList != NULL);

  //  TSKEY    key = TSKEY_INITIAL_VAL;
  //
  //  SArray* group = taosArrayGetP(groupList->pGroupList, 0);
  //  assert(group != NULL);
  //
  //  STableKeyInfo* pInfo = (STableKeyInfo*)taosArrayGet(group, 0);
  //
  //  int32_t code = 0;
  //
  //  if (((STable*)pInfo->pTable)->lastRow) {
  //    code = tsdbGetCachedLastRow(pInfo->pTable, NULL, &key);
  //    if (code != TSDB_CODE_SUCCESS) {
  //      pTsdbReadHandle->cachelastrow = TSDB_CACHED_TYPE_NONE;
  //    } else {
  //      pTsdbReadHandle->cachelastrow = TSDB_CACHED_TYPE_LASTROW;
  //    }
  //  }
  //
  //  // update the tsdb query time range
  //  if (pTsdbReadHandle->cachelastrow != TSDB_CACHED_TYPE_NONE) {
  //    pTsdbReadHandle->window      = TSWINDOW_INITIALIZER;
  //    pTsdbReadHandle->checkFiles  = false;
  //    pTsdbReadHandle->activeIndex = -1;  // start from -1
  //  }

  return TSDB_CODE_SUCCESS;
}

int32_t checkForCachedLast(STsdbReadHandle* pTsdbReadHandle) {
  assert(pTsdbReadHandle != NULL);

  int32_t code = 0;
  //  if (pTsdbReadHandle->pTsdb && atomic_load_8(&pTsdbReadHandle->pTsdb->hasCachedLastColumn)){
  //    pTsdbReadHandle->cachelastrow = TSDB_CACHED_TYPE_LAST;
  //  }

  // update the tsdb query time range
  if (pTsdbReadHandle->cachelastrow) {
    pTsdbReadHandle->checkFiles = false;
    pTsdbReadHandle->activeIndex = -1;  // start from -1
  }

  return code;
}

STimeWindow updateLastrowForEachGroup(STableGroupInfo* groupList) {
  STimeWindow window = {INT64_MAX, INT64_MIN};

  int32_t totalNumOfTable = 0;
  SArray* emptyGroup = taosArrayInit(16, sizeof(int32_t));

  // NOTE: starts from the buffer in case of descending timestamp order check data blocks
  size_t numOfGroups = taosArrayGetSize(groupList->pGroupList);
  for (int32_t j = 0; j < numOfGroups; ++j) {
    SArray* pGroup = taosArrayGetP(groupList->pGroupList, j);
    TSKEY   key = TSKEY_INITIAL_VAL;

    STableKeyInfo keyInfo = {0};

    size_t numOfTables = taosArrayGetSize(pGroup);
    for (int32_t i = 0; i < numOfTables; ++i) {
      STableKeyInfo* pInfo = (STableKeyInfo*)taosArrayGet(pGroup, i);

      // if the lastKey equals to INT64_MIN, there is no data in this table
      TSKEY lastKey = 0;  //((STable*)(pInfo->pTable))->lastKey;
      if (key < lastKey) {
        key = lastKey;

        //        keyInfo.pTable  = pInfo->pTable;
        keyInfo.lastKey = key;
        pInfo->lastKey = key;

        if (key < window.skey) {
          window.skey = key;
        }

        if (key > window.ekey) {
          window.ekey = key;
        }
      }
    }

    // more than one table in each group, only one table left for each group
    //    if (keyInfo.pTable != NULL) {
    //      totalNumOfTable++;
    //      if (taosArrayGetSize(pGroup) == 1) {
    //        // do nothing
    //      } else {
    //        taosArrayClear(pGroup);
    //        taosArrayPush(pGroup, &keyInfo);
    //      }
    //    } else {  // mark all the empty groups, and remove it later
    //      taosArrayDestroy(pGroup);
    //      taosArrayPush(emptyGroup, &j);
    //    }
  }

  // window does not being updated, so set the original
  if (window.skey == INT64_MAX && window.ekey == INT64_MIN) {
    window = TSWINDOW_INITIALIZER;
    assert(totalNumOfTable == 0 && taosArrayGetSize(groupList->pGroupList) == numOfGroups);
  }

  taosArrayRemoveBatch(groupList->pGroupList, TARRAY_GET_START(emptyGroup), (int32_t)taosArrayGetSize(emptyGroup));
  taosArrayDestroy(emptyGroup);

  groupList->numOfTables = totalNumOfTable;
  return window;
}

void tsdbRetrieveDataBlockInfo(tsdbReaderT* pTsdbReadHandle, SDataBlockInfo* pDataBlockInfo) {
  STsdbReadHandle* pHandle = (STsdbReadHandle*)pTsdbReadHandle;
  SQueryFilePos*   cur = &pHandle->cur;

  uint64_t uid = 0;

  // there are data in file
  if (pHandle->cur.fid != INT32_MIN) {
    STableBlockInfo* pBlockInfo = &pHandle->pDataBlockInfo[cur->slot];
    uid = pBlockInfo->pTableCheckInfo->tableId;
  } else {
    STableCheckInfo* pCheckInfo = taosArrayGet(pHandle->pTableCheckInfo, pHandle->activeIndex);
    uid = pCheckInfo->tableId;
  }

  tsdbDebug("data block generated, uid:%" PRIu64 " numOfRows:%d, tsrange:%" PRId64 " - %" PRId64 " %s", uid, cur->rows,
            cur->win.skey, cur->win.ekey, pHandle->idStr);

  pDataBlockInfo->uid = uid;

#if 0
  // for multi-group data query processing test purpose
  pDataBlockInfo->groupId = uid;
#endif

  pDataBlockInfo->rows = cur->rows;
  pDataBlockInfo->window = cur->win;
  //  ASSERT(pDataBlockInfo->numOfCols >= (int32_t)(QH_GET_NUM_OF_COLS(pHandle));
}

/*
 * return null for mixed data block, if not a complete file data block, the statistics value will always return NULL
 */
int32_t tsdbRetrieveDataBlockStatisInfo(tsdbReaderT* pTsdbReadHandle, SColumnDataAgg*** pBlockStatis, bool* allHave) {
  STsdbReadHandle* pHandle = (STsdbReadHandle*)pTsdbReadHandle;
  *allHave = false;

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

  tsdbDebug("vgId:%d succeed to load block statis part for uid %" PRIu64, REPO_ID(pHandle->pTsdb),
            TSDB_READ_TABLE_UID(&pHandle->rhelper));

  int16_t* colIds = pHandle->suppInfo.defaultLoadColumn->pData;

  size_t numOfCols = QH_GET_NUM_OF_COLS(pHandle);
  memset(pHandle->suppInfo.plist, 0, numOfCols * POINTER_BYTES);
  memset(pHandle->suppInfo.pstatis, 0, numOfCols * sizeof(SColumnDataAgg));

  for (int32_t i = 0; i < numOfCols; ++i) {
    pHandle->suppInfo.pstatis[i].colId = colIds[i];
  }

  *allHave = true;
  tsdbGetBlockStatis(&pHandle->rhelper, pHandle->suppInfo.pstatis, (int)numOfCols, pBlockInfo->compBlock);

  // always load the first primary timestamp column data
  SColumnDataAgg* pPrimaryColStatis = &pHandle->suppInfo.pstatis[0];
  assert(pPrimaryColStatis->colId == PRIMARYKEY_TIMESTAMP_COL_ID);

  pPrimaryColStatis->numOfNull = 0;
  pPrimaryColStatis->min = pBlockInfo->compBlock->keyFirst;
  pPrimaryColStatis->max = pBlockInfo->compBlock->keyLast;
  pHandle->suppInfo.plist[0] = &pHandle->suppInfo.pstatis[0];

  // update the number of NULL data rows
  int32_t* slotIds = pHandle->suppInfo.slotIds;
  for (int32_t i = 1; i < numOfCols; ++i) {
    ASSERT(colIds[i] == pHandle->pSchema->columns[slotIds[i]].colId);
    if (IS_BSMA_ON(&(pHandle->pSchema->columns[slotIds[i]]))) {
      if (pHandle->suppInfo.pstatis[i].numOfNull == -1) {  // set the column data are all NULL
        pHandle->suppInfo.pstatis[i].numOfNull = pBlockInfo->compBlock->numOfRows;
      } else {
        pHandle->suppInfo.plist[i] = &pHandle->suppInfo.pstatis[i];
      }
    } else {
      *allHave = false;
    }
  }

  int64_t elapsed = taosGetTimestampUs() - stime;
  pHandle->cost.statisInfoLoadTime += elapsed;

  *pBlockStatis = pHandle->suppInfo.plist;
  return TSDB_CODE_SUCCESS;
}

SArray* tsdbRetrieveDataBlock(tsdbReaderT* pTsdbReadHandle, SArray* pIdList) {
  /**
   * In the following two cases, the data has been loaded to SColumnInfoData.
   * 1. data is from cache, 2. data block is not completed qualified to query time range
   */
  STsdbReadHandle* pHandle = (STsdbReadHandle*)pTsdbReadHandle;
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
          pBlockLoadInfo->uid == pCheckInfo->tableId) {
        return pHandle->pColumns;
      } else {  // only load the file block
        SBlock* pBlock = pBlockInfo->compBlock;
        if (doLoadFileDataBlock(pHandle, pBlock, pCheckInfo, pHandle->cur.slot) != TSDB_CODE_SUCCESS) {
          return NULL;
        }

        int32_t numOfRows = doCopyRowsFromFileBlock(pHandle, pHandle->outputCapacity, 0, 0, pBlock->numOfRows - 1);
        return pHandle->pColumns;
      }
    }
  }
}
#if 0
void filterPrepare(void* expr, void* param) {
  tExprNode* pExpr = (tExprNode*)expr;
  if (pExpr->_node.info != NULL) {
    return;
  }

  pExpr->_node.info = taosMemoryCalloc(1, sizeof(tQueryInfo));

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
        pObj = taosHashInit(256, taosGetDefaultHashFunction(pInfo->sch.type), true, false);
        SArray *arr = (SArray *)(pCond->arr);
        for (size_t i = 0; i < taosArrayGetSize(arr); i++) {
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
    // to make sure tonchar does not cause invalid write, since the '\0' needs at least sizeof(TdUcs4) space.
    pInfo->q = taosMemoryCalloc(1, size + TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
    tVariantDump(pCond, pInfo->q, pSchema->type, true);
  }
}

#endif

static int32_t tableGroupComparFn(const void* p1, const void* p2, const void* param) {
#if 0
  STableGroupSupporter* pTableGroupSupp = (STableGroupSupporter*) param;
  STable* pTable1 = ((STableKeyInfo*) p1)->uid;
  STable* pTable2 = ((STableKeyInfo*) p2)->uid;

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
        f1 = tdGetKVRowValOfCol(pTable1->tagVal, pCol->colId);
        f2 = tdGetKVRowValOfCol(pTable2->tagVal, pCol->colId);
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
#endif
  return 0;
}

static int tsdbCheckInfoCompar(const void* key1, const void* key2) {
  if (((STableCheckInfo*)key1)->tableId < ((STableCheckInfo*)key2)->tableId) {
    return -1;
  } else if (((STableCheckInfo*)key1)->tableId > ((STableCheckInfo*)key2)->tableId) {
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

  STableKeyInfo info = {.lastKey = skey};
  taosArrayPush(g, &info);

  for (int32_t i = 1; i < numOfTables; ++i) {
    STable** prev = taosArrayGet(pTableList, i - 1);
    STable** p = taosArrayGet(pTableList, i);

    int32_t ret = compareFn(prev, p, pSupp);
    assert(ret == 0 || ret == -1);

    if (ret == 0) {
      STableKeyInfo info1 = {.lastKey = skey};
      taosArrayPush(g, &info1);
    } else {
      taosArrayPush(pGroups, &g);  // current group is ended, start a new group
      g = taosArrayInit(16, sizeof(STableKeyInfo));

      STableKeyInfo info1 = {.lastKey = skey};
      taosArrayPush(g, &info1);
    }
  }

  taosArrayPush(pGroups, &g);
}

SArray* createTableGroup(SArray* pTableList, SSchemaWrapper* pTagSchema, SColIndex* pCols, int32_t numOfOrderCols,
                         TSKEY skey) {
  assert(pTableList != NULL);
  SArray* pTableGroup = taosArrayInit(1, POINTER_BYTES);

  size_t size = taosArrayGetSize(pTableList);
  if (size == 0) {
    tsdbDebug("no qualified tables");
    return pTableGroup;
  }

  if (numOfOrderCols == 0 || size == 1) {  // no group by tags clause or only one table
    SArray* sa = taosArrayDup(pTableList);
    if (sa == NULL) {
      taosArrayDestroy(pTableGroup);
      return NULL;
    }

    taosArrayPush(pTableGroup, &sa);
    tsdbDebug("all %" PRIzu " tables belong to one group", size);
  } else {
    STableGroupSupporter sup = {0};
    sup.numOfCols = numOfOrderCols;
    sup.pTagSchema = pTagSchema->pSchema;
    sup.pCols = pCols;

    taosqsort(pTableList->pData, size, sizeof(STableKeyInfo), &sup, tableGroupComparFn);
    createTableGroupImpl(pTableGroup, pTableList, size, skey, &sup, tableGroupComparFn);
  }

  return pTableGroup;
}

// static bool tableFilterFp(const void* pNode, void* param) {
//  tQueryInfo* pInfo = (tQueryInfo*) param;
//
//  STable* pTable = (STable*)(SL_GET_NODE_DATA((SSkipListNode*)pNode));
//
//  char* val = NULL;
//  if (pInfo->sch.colId == TSDB_TBNAME_COLUMN_INDEX) {
//    val = (char*) TABLE_NAME(pTable);
//  } else {
//    val = tdGetKVRowValOfCol(pTable->tagVal, pInfo->sch.colId);
//  }
//
//  if (pInfo->optr == TSDB_RELATION_ISNULL || pInfo->optr == TSDB_RELATION_NOTNULL) {
//    if (pInfo->optr == TSDB_RELATION_ISNULL) {
//      return (val == NULL) || isNull(val, pInfo->sch.type);
//    } else if (pInfo->optr == TSDB_RELATION_NOTNULL) {
//      return (val != NULL) && (!isNull(val, pInfo->sch.type));
//    }
//  } else if (pInfo->optr == TSDB_RELATION_IN) {
//     int type = pInfo->sch.type;
//     if (type == TSDB_DATA_TYPE_BOOL || IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_TIMESTAMP) {
//       int64_t v;
//       GET_TYPED_DATA(v, int64_t, pInfo->sch.type, val);
//       return NULL != taosHashGet((SHashObj *)pInfo->q, (char *)&v, sizeof(v));
//     } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
//       uint64_t v;
//       GET_TYPED_DATA(v, uint64_t, pInfo->sch.type, val);
//       return NULL != taosHashGet((SHashObj *)pInfo->q, (char *)&v, sizeof(v));
//     }
//     else if (type == TSDB_DATA_TYPE_DOUBLE || type == TSDB_DATA_TYPE_FLOAT) {
//       double v;
//       GET_TYPED_DATA(v, double, pInfo->sch.type, val);
//       return NULL != taosHashGet((SHashObj *)pInfo->q, (char *)&v, sizeof(v));
//     } else if (type == TSDB_DATA_TYPE_BINARY || type == TSDB_DATA_TYPE_NCHAR){
//       return NULL != taosHashGet((SHashObj *)pInfo->q, varDataVal(val), varDataLen(val));
//     }
//
//  }
//
//  int32_t ret = 0;
//  if (val == NULL) { //the val is possible to be null, so check it out carefully
//    ret = -1; // val is missing in table tags value pairs
//  } else {
//    ret = pInfo->compare(val, pInfo->q);
//  }
//
//  switch (pInfo->optr) {
//    case TSDB_RELATION_EQUAL: {
//      return ret == 0;
//    }
//    case TSDB_RELATION_NOT_EQUAL: {
//      return ret != 0;
//    }
//    case TSDB_RELATION_GREATER_EQUAL: {
//      return ret >= 0;
//    }
//    case TSDB_RELATION_GREATER: {
//      return ret > 0;
//    }
//    case TSDB_RELATION_LESS_EQUAL: {
//      return ret <= 0;
//    }
//    case TSDB_RELATION_LESS: {
//      return ret < 0;
//    }
//    case TSDB_RELATION_LIKE: {
//      return ret == 0;
//    }
//    case TSDB_RELATION_MATCH: {
//      return ret == 0;
//    }
//    case TSDB_RELATION_NMATCH: {
//      return ret == 0;
//    }
//    case TSDB_RELATION_IN: {
//      return ret == 1;
//    }
//
//    default:
//      assert(false);
//  }
//
//  return true;
//}

// static void getTableListfromSkipList(tExprNode *pExpr, SSkipList *pSkipList, SArray *result, SExprTraverseSupp
// *param);

// static int32_t doQueryTableList(STable* pSTable, SArray* pRes, tExprNode* pExpr) {
//  //  // query according to the expression tree
//  SExprTraverseSupp supp = {
//      .nodeFilterFn = (__result_filter_fn_t)tableFilterFp,
//      .setupInfoFn = filterPrepare,
//      .pExtInfo = pSTable->tagSchema,
//  };
//
//  getTableListfromSkipList(pExpr, pSTable->pIndex, pRes, &supp);
//  tExprTreeDestroy(pExpr, destroyHelper);
//  return TSDB_CODE_SUCCESS;
//}

int32_t tsdbQuerySTableByTagCond(void* pMeta, uint64_t uid, TSKEY skey, const char* pTagCond, size_t len,
                                 int16_t tagNameRelType, const char* tbnameCond, STableGroupInfo* pGroupInfo,
                                 SColIndex* pColIndex, int32_t numOfCols, uint64_t reqId, uint64_t taskId) {
  SMetaReader mr = {0};

  metaReaderInit(&mr, (SMeta*)pMeta, 0);

  if (metaGetTableEntryByUid(&mr, uid) < 0) {
    tsdbError("%p failed to get stable, uid:%" PRIu64 ", TID:0x%" PRIx64 " QID:0x%" PRIx64, pMeta, uid, taskId, reqId);
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    goto _error;
  } else {
    tsdbDebug("%p succeed to get stable, uid:%" PRIu64 ", TID:0x%" PRIx64 " QID:0x%" PRIx64, pMeta, uid, taskId, reqId);
  }

  if (mr.me.type != TSDB_SUPER_TABLE) {
    tsdbError("%p query normal tag not allowed, uid:%" PRIu64 ", TID:0x%" PRIx64 " QID:0x%" PRIx64, pMeta, uid, taskId,
              reqId);
    terrno = TSDB_CODE_OPS_NOT_SUPPORT;  // basically, this error is caused by invalid sql issued by client
    goto _error;
  }

  metaReaderClear(&mr);

  // NOTE: not add ref count for super table
  SArray*         res = taosArrayInit(8, sizeof(STableKeyInfo));
  SSchemaWrapper* pTagSchema = metaGetTableSchema(pMeta, uid, 0, true);

  // no tags and tbname condition, all child tables of this stable are involved
  if (tbnameCond == NULL && (pTagCond == NULL || len == 0)) {
    int32_t ret = getAllTableList(pMeta, uid, res);
    if (ret != TSDB_CODE_SUCCESS) {
      goto _error;
    }

    pGroupInfo->numOfTables = (uint32_t)taosArrayGetSize(res);
    pGroupInfo->pGroupList = createTableGroup(res, pTagSchema, pColIndex, numOfCols, skey);

    tsdbDebug("%p no table name/tag condition, all tables qualified, numOfTables:%u, group:%zu, TID:0x%" PRIx64
              " QID:0x%" PRIx64,
              pMeta, pGroupInfo->numOfTables, taosArrayGetSize(pGroupInfo->pGroupList), taskId, reqId);

    taosArrayDestroy(res);
    return ret;
  }

  int32_t ret = TSDB_CODE_SUCCESS;

  SFilterInfo* filterInfo = NULL;
  ret = filterInitFromNode((SNode*)pTagCond, &filterInfo, 0);
  if (ret != TSDB_CODE_SUCCESS) {
    terrno = ret;
    return ret;
  }
  ret = tsdbQueryTableList(pMeta, res, filterInfo);
  pGroupInfo->numOfTables = (uint32_t)taosArrayGetSize(res);
  pGroupInfo->pGroupList = createTableGroup(res, pTagSchema, pColIndex, numOfCols, skey);

  // tsdbDebug("%p stable tid:%d, uid:%" PRIu64 " query, numOfTables:%u, belong to %" PRIzu " groups", tsdb,
  //          pTable->tableId, pTable->uid, pGroupInfo->numOfTables, taosArrayGetSize(pGroupInfo->pGroupList));

  taosArrayDestroy(res);
  return ret;

_error:
  return terrno;
}

int32_t tsdbQueryTableList(void* pMeta, SArray* pRes, void* filterInfo) {
  // impl later

  return TSDB_CODE_SUCCESS;
}
int32_t tsdbGetOneTableGroup(void* pMeta, uint64_t uid, TSKEY startKey, STableGroupInfo* pGroupInfo) {
  SMetaReader mr = {0};

  metaReaderInit(&mr, (SMeta*)pMeta, 0);

  if (metaGetTableEntryByUid(&mr, uid) < 0) {
    terrno = TSDB_CODE_PAR_TABLE_NOT_EXIST;
    goto _error;
  }

  metaReaderClear(&mr);

  pGroupInfo->numOfTables = 1;
  pGroupInfo->pGroupList = taosArrayInit(1, POINTER_BYTES);

  SArray* group = taosArrayInit(1, sizeof(STableKeyInfo));

  STableKeyInfo info = {.lastKey = startKey, .uid = uid};
  taosArrayPush(group, &info);

  taosArrayPush(pGroupInfo->pGroupList, &group);
  return TSDB_CODE_SUCCESS;

_error:
  metaReaderClear(&mr);
  return terrno;
}

#if 0
int32_t tsdbGetTableGroupFromIdListT(STsdb* tsdb, SArray* pTableIdList, STableGroupInfo* pGroupInfo) {
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
#endif
static void* doFreeColumnInfoData(SArray* pColumnInfoData) {
  if (pColumnInfoData == NULL) {
    return NULL;
  }

  size_t cols = taosArrayGetSize(pColumnInfoData);
  for (int32_t i = 0; i < cols; ++i) {
    SColumnInfoData* pColInfo = taosArrayGet(pColumnInfoData, i);
    taosMemoryFreeClear(pColInfo->pData);
  }

  taosArrayDestroy(pColumnInfoData);
  return NULL;
}

static void* destroyTableCheckInfo(SArray* pTableCheckInfo) {
  size_t size = taosArrayGetSize(pTableCheckInfo);
  for (int32_t i = 0; i < size; ++i) {
    STableCheckInfo* p = taosArrayGet(pTableCheckInfo, i);
    destroyTableMemIterator(p);

    taosMemoryFreeClear(p->pCompInfo);
  }

  taosArrayDestroy(pTableCheckInfo);
  return NULL;
}

void tsdbCleanupReadHandle(tsdbReaderT queryHandle) {
  STsdbReadHandle* pTsdbReadHandle = (STsdbReadHandle*)queryHandle;
  if (pTsdbReadHandle == NULL) {
    return;
  }

  pTsdbReadHandle->pColumns = doFreeColumnInfoData(pTsdbReadHandle->pColumns);

  taosArrayDestroy(pTsdbReadHandle->suppInfo.defaultLoadColumn);
  taosMemoryFreeClear(pTsdbReadHandle->pDataBlockInfo);
  taosMemoryFreeClear(pTsdbReadHandle->suppInfo.pstatis);
  taosMemoryFreeClear(pTsdbReadHandle->suppInfo.plist);

  if (!emptyQueryTimewindow(pTsdbReadHandle)) {
    //    tsdbMayUnTakeMemSnapshot(pTsdbReadHandle);
  } else {
    assert(pTsdbReadHandle->pTableCheckInfo == NULL);
  }

  if (pTsdbReadHandle->pTableCheckInfo != NULL) {
    pTsdbReadHandle->pTableCheckInfo = destroyTableCheckInfo(pTsdbReadHandle->pTableCheckInfo);
  }

  tsdbDestroyReadH(&pTsdbReadHandle->rhelper);

  tdFreeDataCols(pTsdbReadHandle->pDataCols);
  pTsdbReadHandle->pDataCols = NULL;

  pTsdbReadHandle->prev = doFreeColumnInfoData(pTsdbReadHandle->prev);
  pTsdbReadHandle->next = doFreeColumnInfoData(pTsdbReadHandle->next);

  SIOCostSummary* pCost = &pTsdbReadHandle->cost;

  tsdbDebug("%p :io-cost summary: head-file read cnt:%" PRIu64 ", head-file time:%" PRIu64 " us, statis-info:%" PRId64
            " us, datablock:%" PRId64 " us, check data:%" PRId64 " us, %s",
            pTsdbReadHandle, pCost->headFileLoad, pCost->headFileLoadTime, pCost->statisInfoLoadTime,
            pCost->blockLoadTime, pCost->checkForNextTime, pTsdbReadHandle->idStr);

  taosMemoryFreeClear(pTsdbReadHandle);
}

#if 0
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

static void applyFilterToSkipListNode(SSkipList *pSkipList, tExprNode *pExpr, SArray *pResult, SExprTraverseSupp *param) {
  SSkipListIterator* iter = tSkipListCreateIter(pSkipList);

  // Scan each node in the skiplist by using iterator
  while (tSkipListIterNext(iter)) {
    SSkipListNode *pNode = tSkipListIterGet(iter);
    if (exprTreeApplyFilter(pExpr, pNode, param)) {
      taosArrayPush(pResult, &(SL_GET_NODE_DATA(pNode)));
    }
  }

  tSkipListDestroyIter(iter);
}

typedef struct {
  char*    v;
  int32_t  optr;
} SEndPoint;

typedef struct {
  SEndPoint* start;
  SEndPoint* end;
} SQueryCond;

// todo check for malloc failure
static int32_t setQueryCond(tQueryInfo *queryColInfo, SQueryCond* pCond) {
  int32_t optr = queryColInfo->optr;

  if (optr == TSDB_RELATION_GREATER || optr == TSDB_RELATION_GREATER_EQUAL ||
      optr == TSDB_RELATION_EQUAL || optr == TSDB_RELATION_NOT_EQUAL) {
    pCond->start       = taosMemoryCalloc(1, sizeof(SEndPoint));
    pCond->start->optr = queryColInfo->optr;
    pCond->start->v    = queryColInfo->q;
  } else if (optr == TSDB_RELATION_LESS || optr == TSDB_RELATION_LESS_EQUAL) {
    pCond->end       = taosMemoryCalloc(1, sizeof(SEndPoint));
    pCond->end->optr = queryColInfo->optr;
    pCond->end->v    = queryColInfo->q;
  } else if (optr == TSDB_RELATION_IN) {
    pCond->start       = taosMemoryCalloc(1, sizeof(SEndPoint));
    pCond->start->optr = queryColInfo->optr;
    pCond->start->v    = queryColInfo->q; 
  } else if (optr == TSDB_RELATION_LIKE) {
    assert(0);
  } else if (optr == TSDB_RELATION_MATCH) {
    assert(0);
  } else if (optr == TSDB_RELATION_NMATCH) {
    assert(0);
  }

  return TSDB_CODE_SUCCESS;
}

static void queryIndexedColumn(SSkipList* pSkipList, tQueryInfo* pQueryInfo, SArray* result) {
  SSkipListIterator* iter = NULL;

  SQueryCond cond = {0};
  if (setQueryCond(pQueryInfo, &cond) != TSDB_CODE_SUCCESS) {
    //todo handle error
  }

  if (cond.start != NULL) {
    iter = tSkipListCreateIterFromVal(pSkipList, (char*) cond.start->v, pSkipList->type, TSDB_ORDER_ASC);
  } else {
    iter = tSkipListCreateIterFromVal(pSkipList, (char*)(cond.end ? cond.end->v: NULL), pSkipList->type, TSDB_ORDER_DESC);
  }

  if (cond.start != NULL) {
    int32_t optr = cond.start->optr;

    if (optr == TSDB_RELATION_EQUAL) {   // equals
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);

        int32_t ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v);
        if (ret != 0) {
          break;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }
    } else if (optr == TSDB_RELATION_GREATER || optr == TSDB_RELATION_GREATER_EQUAL) { // greater equal
      bool comp = true;
      int32_t ret = 0;

      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);

        if (comp) {
          ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v);
          assert(ret >= 0);
        }

        if (ret == 0 && optr == TSDB_RELATION_GREATER) {
          continue;
        } else {
          STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
          taosArrayPush(result, &info);
          comp = false;
        }
      }
    } else if (optr == TSDB_RELATION_NOT_EQUAL) {   // not equal
      bool comp = true;

      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);
        comp = comp && (pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v) == 0);
        if (comp) {
          continue;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }

      tSkipListDestroyIter(iter);

      comp = true;
      iter = tSkipListCreateIterFromVal(pSkipList, (char*) cond.start->v, pSkipList->type, TSDB_ORDER_DESC);
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);
        comp = comp && (pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v) == 0);
        if (comp) {
          continue;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }

    } else if (optr == TSDB_RELATION_IN) {
      while(tSkipListIterNext(iter)) {
        SSkipListNode* pNode = tSkipListIterGet(iter);

        int32_t ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.start->v);
        if (ret != 0) {
          break;
        }

        STableKeyInfo info = {.pTable = (void*)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
        taosArrayPush(result, &info);
      }
      
    } else {
      assert(0);
    }
  } else {
    int32_t optr = cond.end ? cond.end->optr : TSDB_RELATION_INVALID;
    if (optr == TSDB_RELATION_LESS || optr == TSDB_RELATION_LESS_EQUAL) {
      bool    comp = true;
      int32_t ret = 0;

      while (tSkipListIterNext(iter)) {
        SSkipListNode *pNode = tSkipListIterGet(iter);

        if (comp) {
          ret = pQueryInfo->compare(SL_GET_NODE_KEY(pSkipList, pNode), cond.end->v);
          assert(ret <= 0);
        }

        if (ret == 0 && optr == TSDB_RELATION_LESS) {
          continue;
        } else {
          STableKeyInfo info = {.pTable = (void *)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
          taosArrayPush(result, &info);
          comp = false;  // no need to compare anymore
        }
      }
    } else {
      assert(pQueryInfo->optr == TSDB_RELATION_ISNULL || pQueryInfo->optr == TSDB_RELATION_NOTNULL);

      while (tSkipListIterNext(iter)) {
        SSkipListNode *pNode = tSkipListIterGet(iter);

        bool isnull = isNull(SL_GET_NODE_KEY(pSkipList, pNode), pQueryInfo->sch.type);
        if ((pQueryInfo->optr == TSDB_RELATION_ISNULL && isnull) ||
            (pQueryInfo->optr == TSDB_RELATION_NOTNULL && (!isnull))) {
          STableKeyInfo info = {.pTable = (void *)SL_GET_NODE_DATA(pNode), .lastKey = TSKEY_INITIAL_VAL};
          taosArrayPush(result, &info);
        }
      }
    }
  }

  taosMemoryFree(cond.start);
  taosMemoryFree(cond.end);
  tSkipListDestroyIter(iter);
}

static void queryIndexlessColumn(SSkipList* pSkipList, tQueryInfo* pQueryInfo, SArray* res, __result_filter_fn_t filterFp) {
  SSkipListIterator* iter = tSkipListCreateIter(pSkipList);

  while (tSkipListIterNext(iter)) {
    bool addToResult = false;

    SSkipListNode *pNode = tSkipListIterGet(iter);

    char *pData = SL_GET_NODE_DATA(pNode);
    tstr *name = (tstr*) tsdbGetTableName((void*) pData);

    // todo speed up by using hash
    if (pQueryInfo->sch.colId == TSDB_TBNAME_COLUMN_INDEX) {
      if (pQueryInfo->optr == TSDB_RELATION_IN) {
        addToResult = pQueryInfo->compare(name, pQueryInfo->q);
      } else if (pQueryInfo->optr == TSDB_RELATION_LIKE ||
                 pQueryInfo->optr == TSDB_RELATION_MATCH ||
                 pQueryInfo->optr == TSDB_RELATION_NMATCH) {
        addToResult = !pQueryInfo->compare(name, pQueryInfo->q);
      }
    } else {
      addToResult = filterFp(pNode, pQueryInfo);
    }

    if (addToResult) {
      STableKeyInfo info = {.pTable = (void*)pData, .lastKey = TSKEY_INITIAL_VAL};
      taosArrayPush(res, &info);
    }
  }

  tSkipListDestroyIter(iter);
}

// Apply the filter expression to each node in the skiplist to acquire the qualified nodes in skip list
//void getTableListfromSkipList(tExprNode *pExpr, SSkipList *pSkipList, SArray *result, SExprTraverseSupp *param) {
//  if (pExpr == NULL) {
//    return;
//  }
//
//  tExprNode *pLeft  = pExpr->_node.pLeft;
//  tExprNode *pRight = pExpr->_node.pRight;
//
//  // column project
//  if (pLeft->nodeType != TSQL_NODE_EXPR && pRight->nodeType != TSQL_NODE_EXPR) {
//    assert(pLeft->nodeType == TSQL_NODE_COL && (pRight->nodeType == TSQL_NODE_VALUE || pRight->nodeType == TSQL_NODE_DUMMY));
//
//    param->setupInfoFn(pExpr, param->pExtInfo);
//
//    tQueryInfo *pQueryInfo = pExpr->_node.info;
//    if (pQueryInfo->indexed && (pQueryInfo->optr != TSDB_RELATION_LIKE
//                                && pQueryInfo->optr != TSDB_RELATION_MATCH && pQueryInfo->optr != TSDB_RELATION_NMATCH
//                                && pQueryInfo->optr != TSDB_RELATION_IN)) {
//      queryIndexedColumn(pSkipList, pQueryInfo, result);
//    } else {
//      queryIndexlessColumn(pSkipList, pQueryInfo, result, param->nodeFilterFn);
//    }
//
//    return;
//  }
//
//  // The value of hasPK is always 0.
//  uint8_t weight = pLeft->_node.hasPK + pRight->_node.hasPK;
//  assert(weight == 0 && pSkipList != NULL && taosArrayGetSize(result) == 0);
//
//  //apply the hierarchical filter expression to every node in skiplist to find the qualified nodes
//  applyFilterToSkipListNode(pSkipList, pExpr, result, param);
//}
#endif
