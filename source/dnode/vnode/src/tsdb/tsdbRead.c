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
#define ASCENDING_TRAVERSE(o) (o == TSDB_ORDER_ASC)

typedef struct {
  STbDataIter* iter;
  int32_t      index;
  bool         hasVal;
} SIterInfo;

typedef struct STableBlockScanInfo {
  uint64_t  uid;
  TSKEY     lastKey;
  SBlockIdx blockIdx;
  SArray*   pBlockList;  // block data index list
  SIterInfo iter;        // mem buffer skip list iterator
  SIterInfo iiter;       // imem buffer skip list iterator
  SArray*   delSkyline;  // delete info for this table
  int32_t   fileDelIndex;
  bool      iterInit;  // whether to initialize the in-memory skip list iterator or not
} STableBlockScanInfo;

typedef struct SBlockOrderWrapper {
  int64_t uid;
  SBlock* pBlock;
} SBlockOrderWrapper;

typedef struct SBlockOrderSupporter {
  SBlockOrderWrapper** pDataBlockInfo;
  int32_t*             indexPerTable;
  int32_t*             numOfBlocksPerTable;
  int32_t              numOfTables;
} SBlockOrderSupporter;

typedef struct SIOCostSummary {
  int64_t blockLoadTime;
  int64_t smaLoadTime;
  int64_t checkForNextTime;
  int64_t headFileLoad;
  int64_t headFileLoadTime;
} SIOCostSummary;

typedef struct SBlockLoadSuppInfo {
  SArray*          pColAgg;
  SColumnDataAgg   tsColAgg;
  SColumnDataAgg** plist;
  int16_t*         colIds;    // column ids for loading file block data
  int32_t*         slotIds;   // colId to slotId
  char**           buildBuf;  // build string tmp buffer, todo remove it later after all string format being updated.
} SBlockLoadSuppInfo;

typedef struct SFilesetIter {
  int32_t          numOfFiles;  // number of total files
  int32_t          index;       // current accessed index in the list
  SArray*          pFileList;   // data file list
  int32_t          order;
} SFilesetIter;

typedef struct SFileDataBlockInfo {
  int32_t
           tbBlockIdx;  // index position in STableBlockScanInfo in order to check whether neighbor block overlaps with it
  uint64_t uid;
} SFileDataBlockInfo;

typedef struct SDataBlockIter {
  int32_t numOfBlocks;
  int32_t index;
  SArray* blockList;  // SArray<SFileDataBlockInfo>
  int32_t order;
} SDataBlockIter;

typedef struct SFileBlockDumpInfo {
  int32_t totalRows;
  int32_t rowIndex;
  int64_t lastKey;
  bool    allDumped;
} SFileBlockDumpInfo;

typedef struct SVersionRange {
  uint64_t minVer;
  uint64_t maxVer;
} SVersionRange;

typedef struct SReaderStatus {
  bool                 loadFromFile;  // check file stage
  SHashObj*            pTableMap;     // SHash<STableBlockScanInfo>
  STableBlockScanInfo* pTableIter;    // table iterator used in building in-memory buffer data blocks.
  SFileBlockDumpInfo   fBlockDumpInfo;

  SDFileSet*     pCurrentFileset;  // current opened file set
  SBlockData     fileBlockData;
  SFilesetIter   fileIter;
  SDataBlockIter blockIter;
  bool           composedDataBlock;  // the returned data block is a composed block or not
} SReaderStatus;

struct STsdbReader {
  STsdb*             pTsdb;
  uint64_t           suid;
  int16_t            order;
  STimeWindow        window;  // the primary query time window that applies to all queries
  SSDataBlock*       pResBlock;
  int32_t            capacity;
  SReaderStatus      status;
  char*              idStr;  // query info handle, for debug purpose
  int32_t            type;   // query type: 1. retrieve all data blocks, 2. retrieve direct prev|next rows
  SBlockLoadSuppInfo suppInfo;

  SIOCostSummary     cost;
  STSchema*          pSchema;
  SDataFReader*      pFileReader;
  SVersionRange      verRange;
};

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter);
static int      buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                          STsdbReader* pReader);
static TSDBROW* getValidRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader);
static int32_t  doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, STsdbReader* pReader,
                                        SRowMerger* pMerger);
static int32_t  doMergeRowsInBuf(SIterInfo* pIter, int64_t ts, SArray* pDelList, SRowMerger* pMerger,
                                 STsdbReader* pReader);
static int32_t  doAppendOneRow(SSDataBlock* pBlock, STsdbReader* pReader, STSRow* pTSRow);
static void     setComposedBlockFlag(STsdbReader* pReader, bool composed);
static void     updateSchema(TSDBROW* pRow, uint64_t uid, STsdbReader* pReader);
static bool     hasBeenDropped(const SArray* pDelList, int32_t* index, TSDBKEY* pKey, int32_t order);

static void doMergeMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList, STSRow** pTSRow,
                             STsdbReader* pReader);
static void doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader,
                               STSRow** pTSRow);
static int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STbData* pMemTbData,
                                      STbData* piMemTbData);
static STsdb*  getTsdbByRetentions(SVnode* pVnode, TSKEY winSKey, SRetention* retentions, const char* idstr,
                                   int8_t* pLevel);
static SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level);

static int32_t setColumnIdSlotList(STsdbReader* pReader, SSDataBlock* pBlock) {
  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;

  size_t numOfCols = blockDataGetNumOfCols(pBlock);

  pSupInfo->colIds = taosMemoryMalloc(numOfCols * sizeof(int16_t));
  pSupInfo->buildBuf = taosMemoryCalloc(numOfCols, POINTER_BYTES);
  if (pSupInfo->buildBuf == NULL || pSupInfo->colIds == NULL) {
    taosMemoryFree(pSupInfo->colIds);
    taosMemoryFree(pSupInfo->buildBuf);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < numOfCols; ++i) {
    SColumnInfoData* pCol = taosArrayGet(pBlock->pDataBlock, i);
    pSupInfo->colIds[i] = pCol->info.colId;

    if (IS_VAR_DATA_TYPE(pCol->info.type)) {
      pSupInfo->buildBuf[i] = taosMemoryMalloc(pCol->info.bytes);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static SHashObj* createDataBlockScanInfo(STsdbReader* pTsdbReader, const STableKeyInfo* idList, int32_t numOfTables) {
  // allocate buffer in order to load data blocks from file
  // todo use simple hash instead
  SHashObj* pTableMap =
      taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pTableMap == NULL) {
    return NULL;
  }

  // todo apply the lastkey of table check to avoid to load header file
  for (int32_t j = 0; j < numOfTables; ++j) {
    STableBlockScanInfo info = {.lastKey = 0, .uid = idList[j].uid};
    if (ASCENDING_TRAVERSE(pTsdbReader->order)) {
      if (info.lastKey == INT64_MIN || info.lastKey < pTsdbReader->window.skey) {
        info.lastKey = pTsdbReader->window.skey;
      }

      ASSERT(info.lastKey >= pTsdbReader->window.skey && info.lastKey <= pTsdbReader->window.ekey);
    } else {
      info.lastKey = pTsdbReader->window.skey;
    }

    taosHashPut(pTableMap, &info.uid, sizeof(uint64_t), &info, sizeof(info));
    tsdbDebug("%p check table uid:%" PRId64 " from lastKey:%" PRId64 " %s", pTsdbReader, info.uid, info.lastKey,
              pTsdbReader->idStr);
  }

  return pTableMap;
}

static void resetDataBlockScanInfo(SHashObj* pTableMap) {
  STableBlockScanInfo* p = NULL;

  while ((p = taosHashIterate(pTableMap, p)) != NULL) {
    p->iterInit = false;
    p->iiter.hasVal = false;
    if (p->iter.iter != NULL) {
      tsdbTbDataIterDestroy(p->iter.iter);
    }

    taosArrayDestroy(p->delSkyline);
  }
}

static bool isEmptyQueryTimeWindow(STimeWindow* pWindow) {
  ASSERT(pWindow != NULL);
  return pWindow->skey > pWindow->ekey;
}

// Update the query time window according to the data time to live(TTL) information, in order to avoid to return
// the expired data to client, even it is queried already.
static STimeWindow updateQueryTimeWindow(STsdb* pTsdb, STimeWindow* pWindow) {
  STsdbKeepCfg* pCfg = &pTsdb->keepCfg;

  int64_t now = taosGetTimestamp(pCfg->precision);
  int64_t earilyTs = now - (tsTickPerMin[pCfg->precision] * pCfg->keep2) + 1;  // needs to add one tick

  STimeWindow win = *pWindow;
  if (win.skey < earilyTs) {
    win.skey = earilyTs;
  }

  return win;
}

static void limitOutputBufferSize(const SQueryTableDataCond* pCond, int32_t* capacity) {
  int32_t rowLen = 0;
  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    rowLen += pCond->colList[i].bytes;
  }

  // make sure the output SSDataBlock size be less than 2MB.
  const int32_t TWOMB = 2 * 1024 * 1024;
  if ((*capacity) * rowLen > TWOMB) {
    (*capacity) = TWOMB / rowLen;
  }
}

// init file iterator
static int32_t initFilesetIterator(SFilesetIter* pIter, const STsdbFSState* pFState, int32_t order, const char* idstr) {
  size_t numOfFileset = taosArrayGetSize(pFState->aDFileSet);

  pIter->index = ASCENDING_TRAVERSE(order) ? -1 : numOfFileset;
  pIter->order = order;
  pIter->pFileList = taosArrayDup(pFState->aDFileSet);
  pIter->numOfFiles = numOfFileset;

  tsdbDebug("init fileset iterator, total files:%d %s", pIter->numOfFiles, idstr);
  return TSDB_CODE_SUCCESS;
}

static bool filesetIteratorNext(SFilesetIter* pIter, STsdbReader* pReader) {
  bool    asc = ASCENDING_TRAVERSE(pIter->order);
  int32_t step = asc ? 1 : -1;
  pIter->index += step;

  if ((asc && pIter->index >= pIter->numOfFiles) || ((!asc) && pIter->index < 0)) {
    return false;
  }

  // check file the time range of coverage
  STimeWindow win = {0};

  while (1) {
    pReader->status.pCurrentFileset = (SDFileSet*)taosArrayGet(pIter->pFileList, pIter->index);

    int32_t code = tsdbDataFReaderOpen(&pReader->pFileReader, pReader->pTsdb, pReader->status.pCurrentFileset);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    int32_t fid = pReader->status.pCurrentFileset->fid;
    tsdbFidKeyRange(fid, pReader->pTsdb->keepCfg.days, pReader->pTsdb->keepCfg.precision, &win.skey, &win.ekey);

    // current file are no longer overlapped with query time window, ignore remain files
    if ((asc && win.skey > pReader->window.ekey) || (!asc && win.ekey < pReader->window.skey)) {
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, %s", pReader,
                pReader->window.skey, pReader->window.ekey, pReader->idStr);
      return false;
    }

    if ((asc && (win.ekey < pReader->window.skey)) || ((!asc) && (win.skey > pReader->window.ekey))) {
      pIter->index += step;
      continue;
    }

    tsdbDebug("%p file found fid:%d for qrange:%" PRId64 "-%" PRId64 ", ignore, %s", pReader, fid, pReader->window.skey,
              pReader->window.ekey, pReader->idStr);
    return true;
  }

_err:
  return false;
}

static void resetDataBlockIterator(SDataBlockIter* pIter, int32_t order) {
  pIter->order = order;
  pIter->index = -1;
  pIter->numOfBlocks = -1;
  pIter->blockList = taosArrayInit(4, sizeof(SFileDataBlockInfo));
}

static void initReaderStatus(SReaderStatus* pStatus) {
  pStatus->pTableIter = NULL;
  pStatus->loadFromFile = true;
}

static SSDataBlock* createResBlock(SQueryTableDataCond* pCond, int32_t capacity) {
  SSDataBlock* pResBlock = createDataBlock();
  if (pResBlock == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfoData colInfo = {{0}, 0};
    colInfo.info = pCond->colList[i];
    blockDataAppendColInfo(pResBlock, &colInfo);
  }

  int32_t code = blockDataEnsureCapacity(pResBlock, capacity);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    taosMemoryFree(pResBlock);
    return NULL;
  }

  return pResBlock;
}

static int32_t tsdbReaderCreate(SVnode* pVnode, SQueryTableDataCond* pCond, STsdbReader** ppReader, const char* idstr) {
  int32_t      code = 0;
  int8_t       level = 0;
  STsdbReader* pReader = (STsdbReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  initReaderStatus(&pReader->status);

  pReader->pTsdb =
      getTsdbByRetentions(pVnode, pCond->twindows[0].skey, pVnode->config.tsdbCfg.retentions, idstr, &level);
  pReader->suid = pCond->suid;
  pReader->order = pCond->order;
  pReader->capacity = 4096;
  pReader->idStr = (idstr != NULL) ? strdup(idstr) : NULL;
  pReader->verRange = getQueryVerRange(pVnode, pCond, level);
  pReader->type = pCond->type;
  pReader->window = updateQueryTimeWindow(pVnode->pTsdb, pCond->twindows);

  ASSERT(pCond->numOfCols > 0);

  limitOutputBufferSize(pCond, &pReader->capacity);

  // allocate buffer in order to load data blocks from file
  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;
  pSup->pColAgg = taosArrayInit(4, sizeof(SColumnDataAgg));
  pSup->plist = taosMemoryCalloc(pCond->numOfCols, POINTER_BYTES);
  if (pSup->pColAgg == NULL || pSup->plist == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  pSup->tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;

  pReader->pResBlock = createResBlock(pCond, pReader->capacity);
  if (pReader->pResBlock == NULL) {
    code = terrno;
    goto _end;
  }

  setColumnIdSlotList(pReader, pReader->pResBlock);

  *ppReader = pReader;
  return code;

_end:
  tsdbReaderClose(pReader);
  *ppReader = NULL;
  return code;
}

// void tsdbResetQueryHandleForNewTable(STsdbReader* queryHandle, SQueryTableDataCond* pCond, STableListInfo* tableList,
//                                      int32_t tWinIdx) {
//   STsdbReader* pTsdbReadHandle = queryHandle;

//   pTsdbReadHandle->order = pCond->order;
//   pTsdbReadHandle->window = pCond->twindows[tWinIdx];
//   pTsdbReadHandle->type = TSDB_QUERY_TYPE_ALL;
//   pTsdbReadHandle->cur.fid = -1;
//   pTsdbReadHandle->cur.win = TSWINDOW_INITIALIZER;
//   pTsdbReadHandle->checkFiles = true;
//   pTsdbReadHandle->activeIndex = 0;  // current active table index
//   pTsdbReadHandle->locateStart = false;
//   pTsdbReadHandle->loadExternalRow = pCond->loadExternalRows;

//   if (ASCENDING_TRAVERSE(pCond->order)) {
//     assert(pTsdbReadHandle->window.skey <= pTsdbReadHandle->window.ekey);
//   } else {
//     assert(pTsdbReadHandle->window.skey >= pTsdbReadHandle->window.ekey);
//   }

//   // allocate buffer in order to load data blocks from file
//   memset(pTsdbReadHandle->suppInfo.pstatis, 0, sizeof(SColumnDataAgg));
//   memset(pTsdbReadHandle->suppInfo.plist, 0, POINTER_BYTES);

//   tsdbInitDataBlockLoadInfo(&pTsdbReadHandle->dataBlockLoadInfo);
//   tsdbInitCompBlockLoadInfo(&pTsdbReadHandle->compBlockLoadInfo);

//   SArray* pTable = NULL;
//   //  STsdbMeta* pMeta = tsdbGetMeta(pTsdbReadHandle->pTsdb);

//   //  pTsdbReadHandle->pTableCheckInfo = destroyTableCheckInfo(pTsdbReadHandle->pTableCheckInfo);

//   pTsdbReadHandle->pTableCheckInfo = NULL;  // createDataBlockScanInfo(pTsdbReadHandle, groupList, pMeta,
//                                             // &pTable);
//   if (pTsdbReadHandle->pTableCheckInfo == NULL) {
//     //    tsdbReaderClose(pTsdbReadHandle);
//     terrno = TSDB_CODE_TDB_OUT_OF_MEMORY;
//   }

//   //  pTsdbReadHandle->prev = doFreeColumnInfoData(pTsdbReadHandle->prev);
//   //  pTsdbReadHandle->next = doFreeColumnInfoData(pTsdbReadHandle->next);
// }

// SArray* tsdbGetQueriedTableList(STsdbReader** pHandle) {
//   assert(pHandle != NULL);

//   STsdbReader* pTsdbReadHandle = (STsdbReader*)pHandle;

//   size_t  size = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);
//   SArray* res = taosArrayInit(size, POINTER_BYTES);
//   return res;
// }

// static TSKEY extractFirstTraverseKey(STableBlockScanInfo* pCheckInfo, int32_t order, int32_t update, TDRowVerT
// maxVer) {
//   TSDBROW row = {0};
//   STSRow *rmem = NULL, *rimem = NULL;

//   if (pCheckInfo->iter) {
//     if (tsdbTbDataIterGet(pCheckInfo->iter, &row)) {
//       rmem = row.pTSRow;
//     }
//   }

//   if (pCheckInfo->iiter) {
//     if (tsdbTbDataIterGet(pCheckInfo->iiter, &row)) {
//       rimem = row.pTSRow;
//     }
//   }

//   if (rmem == NULL && rimem == NULL) {
//     return TSKEY_INITIAL_VAL;
//   }

//   if (rmem != NULL && rimem == NULL) {
//     pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
//     return TD_ROW_KEY(rmem);
//   }

//   if (rmem == NULL && rimem != NULL) {
//     pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
//     return TD_ROW_KEY(rimem);
//   }

//   TSKEY r1 = TD_ROW_KEY(rmem);
//   TSKEY r2 = TD_ROW_KEY(rimem);

//   if (r1 == r2) {
//     if (TD_SUPPORT_UPDATE(update)) {
//       pCheckInfo->chosen = CHECKINFO_CHOSEN_BOTH;
//     } else {
//       pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
//       tsdbTbDataIterNext(pCheckInfo->iter);
//     }
//     return r1;
//   } else if (r1 < r2 && ASCENDING_TRAVERSE(order)) {
//     pCheckInfo->chosen = CHECKINFO_CHOSEN_MEM;
//     return r1;
//   } else {
//     pCheckInfo->chosen = CHECKINFO_CHOSEN_IMEM;
//     return r2;
//   }
// }

// static bool moveToNextRowInMem(STableBlockScanInfo* pCheckInfo) {
//   bool hasNext = false;
//   if (pCheckInfo->chosen == CHECKINFO_CHOSEN_MEM) {
//     if (pCheckInfo->iter != NULL) {
//       hasNext = tsdbTbDataIterNext(pCheckInfo->iter);
//     }

//     if (hasNext) {
//       return hasNext;
//     }

//     if (pCheckInfo->iiter != NULL) {
//       return tsdbTbDataIterGet(pCheckInfo->iiter, NULL);
//     }
//   } else if (pCheckInfo->chosen == CHECKINFO_CHOSEN_IMEM) {
//     if (pCheckInfo->iiter != NULL) {
//       hasNext = tsdbTbDataIterNext(pCheckInfo->iiter);
//     }

//     if (hasNext) {
//       return hasNext;
//     }

//     if (pCheckInfo->iter != NULL) {
//       return tsdbTbDataIterGet(pCheckInfo->iter, NULL);
//     }
//   } else {
//     if (pCheckInfo->iter != NULL) {
//       hasNext = tsdbTbDataIterNext(pCheckInfo->iter);
//     }
//     if (pCheckInfo->iiter != NULL) {
//       hasNext = tsdbTbDataIterNext(pCheckInfo->iiter) || hasNext;
//     }
//   }

//   return hasNext;
// }

// static int32_t binarySearchForBlock(SBlock* pBlock, int32_t numOfBlocks, TSKEY skey, int32_t order) {
//   int32_t firstSlot = 0;
//   int32_t lastSlot = numOfBlocks - 1;

//   int32_t midSlot = firstSlot;

//   while (1) {
//     numOfBlocks = lastSlot - firstSlot + 1;
//     midSlot = (firstSlot + (numOfBlocks >> 1));

//     if (numOfBlocks == 1) break;

//     if (skey > pBlock[midSlot].maxKey.ts) {
//       if (numOfBlocks == 2) break;
//       if ((order == TSDB_ORDER_DESC) && (skey < pBlock[midSlot + 1].minKey.ts)) break;
//       firstSlot = midSlot + 1;
//     } else if (skey < pBlock[midSlot].minKey.ts) {
//       if ((order == TSDB_ORDER_ASC) && (skey > pBlock[midSlot - 1].maxKey.ts)) break;
//       lastSlot = midSlot - 1;
//     } else {
//       break;  // got the slot
//     }
//   }

//   return midSlot;
// }

static int32_t doLoadBlockIndex(STsdbReader* pReader, SDataFReader* pFileReader, SArray* pIndexList) {
  SArray* aBlockIdx = taosArrayInit(0, sizeof(SBlockIdx));

  int32_t code = tsdbReadBlockIdx(pFileReader, aBlockIdx, NULL);
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }

  if (taosArrayGetSize(aBlockIdx) == 0) {
    taosArrayClear(aBlockIdx);
    return TSDB_CODE_SUCCESS;
  }

  SBlockIdx* pBlockIdx;
  for (int32_t i = 0; i < taosArrayGetSize(aBlockIdx); ++i) {
    pBlockIdx = (SBlockIdx*)taosArrayGet(aBlockIdx, i);

    // uid check
    if (pBlockIdx->suid != pReader->suid) {
      continue;
    }

    // this block belongs to a table that is not queried.
    void* p = taosHashGet(pReader->status.pTableMap, &pBlockIdx->uid, sizeof(uint64_t));
    if (p == NULL) {
      continue;
    }

    // todo: not valid info in bockIndex
    // time range check
    //    if (pBlockIdx->minKey > pReader->window.ekey || pBlockIdx->maxKey < pReader->window.skey) {
    //      continue;
    //    }

    // version check
    //    if (pBlockIdx->minVersion > pReader->verRange.maxVer || pBlockIdx->maxVersion < pReader->verRange.minVer) {
    //      continue;
    //    }

    STableBlockScanInfo* pScanInfo = p;
    if (pScanInfo->pBlockList == NULL) {
      pScanInfo->pBlockList = taosArrayInit(16, sizeof(SBlock));
    }

    pScanInfo->blockIdx = *pBlockIdx;
    taosArrayPush(pIndexList, pBlockIdx);
  }

_end:
  taosArrayDestroy(aBlockIdx);
  return code;
}

static int32_t doLoadFileBlock(STsdbReader* pReader, SArray* pIndexList, uint32_t* numOfValidTables,
                               int32_t* numOfBlocks) {
  size_t numOfTables = taosArrayGetSize(pIndexList);

  *numOfValidTables = 0;

  STableBlockScanInfo* px = NULL;
  while (1) {
    px = taosHashIterate(pReader->status.pTableMap, px);
    if (px == NULL) {
      break;
    }

    taosArrayClear(px->pBlockList);
  }

  for (int32_t i = 0; i < numOfTables; ++i) {
    SBlockIdx* pBlockIdx = taosArrayGet(pIndexList, i);

    SMapData mapData = {0};
    tMapDataReset(&mapData);
    tsdbReadBlock(pReader->pFileReader, pBlockIdx, &mapData, NULL);

    STableBlockScanInfo* pScanInfo = taosHashGet(pReader->status.pTableMap, &pBlockIdx->uid, sizeof(int64_t));
    for (int32_t j = 0; j < mapData.nItem; ++j) {
      SBlock block = {0};

      tMapDataGetItemByIdx(&mapData, j, &block, tGetBlock);

      // 1. time range check
      if (block.minKey.ts > pReader->window.ekey || block.maxKey.ts < pReader->window.skey) {
        continue;
      }

      // 2. version range check
      if (block.minVersion > pReader->verRange.maxVer || block.maxVersion < pReader->verRange.minVer) {
        continue;
      }

      void* p = taosArrayPush(pScanInfo->pBlockList, &block);
      if (p == NULL) {
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      (*numOfBlocks) += 1;
    }

    if (pScanInfo->pBlockList != NULL && taosArrayGetSize(pScanInfo->pBlockList) > 0) {
      (*numOfValidTables) += 1;
    }
  }

  return TSDB_CODE_SUCCESS;
}

// todo remove pblock parameter
static void setBlockAllDumped(SFileBlockDumpInfo* pDumpInfo, SBlock* pBlock, int32_t order) {
  int32_t step = ASCENDING_TRAVERSE(order) ? 1 : -1;

  pDumpInfo->allDumped = true;
  pDumpInfo->lastKey = pBlock->maxKey.ts + step;
}

static void doCopyColVal(SColumnInfoData* pColInfoData, int32_t rowIndex, int32_t colIndex, SColVal* pColVal,
                         SBlockLoadSuppInfo* pSup) {
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    if (pColVal->isNull || pColVal->isNone) {
      colDataAppendNULL(pColInfoData, rowIndex);
    } else {
      varDataSetLen(pSup->buildBuf[colIndex], pColVal->value.nData);
      memcpy(varDataVal(pSup->buildBuf[colIndex]), pColVal->value.pData, pColVal->value.nData);
      colDataAppend(pColInfoData, rowIndex, pSup->buildBuf[colIndex], false);
    }
  } else {
    colDataAppend(pColInfoData, rowIndex, (const char*)&pColVal->value, pColVal->isNull || pColVal->isNone);
  }
}

static int32_t copyBlockDataToSDataBlock(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo) {
  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockIter* pBlockIter = &pStatus->blockIter;

  SBlockData*         pBlockData = &pStatus->fileBlockData;
  SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(pBlockIter);
  SBlock*             pBlock = taosArrayGet(pBlockScanInfo->pBlockList, pFBlock->tbBlockIdx);
  SSDataBlock*        pResBlock = pReader->pResBlock;
  int32_t             numOfCols = blockDataGetNumOfCols(pResBlock);

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  int64_t st = taosGetTimestampUs();

  SColVal cv = {0};
  int32_t colIndex = 0;

  bool    asc = ASCENDING_TRAVERSE(pReader->order);
  int32_t step = asc ? 1 : -1;

  int32_t rowIndex = 0;
  int32_t remain = asc ? (pBlockData->nRow - pDumpInfo->rowIndex) : (pDumpInfo->rowIndex + 1);

  int32_t endIndex = 0;
  if (remain <= pReader->capacity) {
    endIndex = pBlockData->nRow;
  } else {
    endIndex = pDumpInfo->rowIndex + step * pReader->capacity;
    remain = pReader->capacity;
  }

  int32_t          i = 0;
  SColumnInfoData* pColData = taosArrayGet(pResBlock->pDataBlock, i);
  if (pColData->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    for (int32_t j = pDumpInfo->rowIndex; j < endIndex && j >= 0; j += step) {
      colDataAppend(pColData, rowIndex++, (const char*)&pBlockData->aTSKEY[j], false);
    }
    i += 1;
  }

  while (i < numOfCols && colIndex < taosArrayGetSize(pBlockData->aIdx)) {
    rowIndex = 0;
    pColData = taosArrayGet(pResBlock->pDataBlock, i);

    SColData* pData = tBlockDataGetColDataByIdx(pBlockData, colIndex);

    if (pData->cid == pColData->info.colId) {
      for (int32_t j = pDumpInfo->rowIndex; j < endIndex && j >= 0; j += step) {
        tColDataGetValue(pData, j, &cv);
        doCopyColVal(pColData, rowIndex++, i, &cv, pSupInfo);
      }
      colIndex += 1;
    } else {  // the specified column does not exist in file block, fill with null data
      colDataAppendNNULL(pColData, 0, remain);
    }

    ASSERT(rowIndex == remain);
    i += 1;
  }

  while (i < numOfCols) {
    pColData = taosArrayGet(pResBlock->pDataBlock, i);
    colDataAppendNNULL(pColData, 0, remain);
    i += 1;
  }

  pResBlock->info.rows = remain;
  pDumpInfo->rowIndex += step * remain;

  setBlockAllDumped(pDumpInfo, pBlock, pReader->order);

  int64_t elapsedTime = (taosGetTimestampUs() - st);
  pReader->cost.blockLoadTime += elapsedTime;

  int32_t unDumpedRows = asc ? pBlock->nRow - pDumpInfo->rowIndex : pDumpInfo->rowIndex + 1;
  tsdbDebug("%p load file block into buffer, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, remain:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", elapsed time:%" PRId64 " us, %s",
            pReader, pBlockIter->index, pFBlock->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, remain, unDumpedRows,
            pBlock->minVersion, pBlock->maxVersion, elapsedTime, pReader->idStr);

  return TSDB_CODE_SUCCESS;
}

// todo consider the output buffer size
static int32_t doLoadFileBlockData(STsdbReader* pReader, SDataBlockIter* pBlockIter,
                                   STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData) {
  int64_t st = taosGetTimestampUs();

  SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(pBlockIter);
  SBlock*             pBlock = taosArrayGet(pBlockScanInfo->pBlockList, pFBlock->tbBlockIdx);
  SSDataBlock*        pResBlock = pReader->pResBlock;
  int32_t             numOfCols = blockDataGetNumOfCols(pResBlock);

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  uint8_t *pb = NULL, *pb1 = NULL;
  int32_t  code = tsdbReadColData(pReader->pFileReader, &pBlockScanInfo->blockIdx, pBlock, pSupInfo->colIds, numOfCols,
                                 pBlockData, &pb, &pb1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _error;
  }

  int64_t elapsedTime = (taosGetTimestampUs() - st);
  pReader->cost.blockLoadTime += elapsedTime;

  pDumpInfo->allDumped = false;
  tsdbDebug("%p load file block into buffer, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", elapsed time:%" PRId64 " us, %s",
            pReader, pBlockIter->index, pFBlock->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, pBlock->nRow,
            pBlock->minVersion, pBlock->maxVersion, elapsedTime, pReader->idStr);
  return TSDB_CODE_SUCCESS;

_error:
  tsdbError("%p error occurs in loading file block, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, %s",
            pReader, pBlockIter->index, pFBlock->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, pBlock->nRow,
            pReader->idStr);
  return code;
}

// static int doBinarySearchKey(char* pValue, int num, TSKEY key, int order) {
//   int    firstPos, lastPos, midPos = -1;
//   int    numOfRows;
//   TSKEY* keyList;

//   assert(order == TSDB_ORDER_ASC || order == TSDB_ORDER_DESC);

//   if (num <= 0) return -1;

//   keyList = (TSKEY*)pValue;
//   firstPos = 0;
//   lastPos = num - 1;

//   if (order == TSDB_ORDER_DESC) {
//     // find the first position which is smaller than the key
//     while (1) {
//       if (key >= keyList[lastPos]) return lastPos;
//       if (key == keyList[firstPos]) return firstPos;
//       if (key < keyList[firstPos]) return firstPos - 1;

//       numOfRows = lastPos - firstPos + 1;
//       midPos = (numOfRows >> 1) + firstPos;

//       if (key < keyList[midPos]) {
//         lastPos = midPos - 1;
//       } else if (key > keyList[midPos]) {
//         firstPos = midPos + 1;
//       } else {
//         break;
//       }
//     }

//   } else {
//     // find the first position which is bigger than the key
//     while (1) {
//       if (key <= keyList[firstPos]) return firstPos;
//       if (key == keyList[lastPos]) return lastPos;

//       if (key > keyList[lastPos]) {
//         lastPos = lastPos + 1;
//         if (lastPos >= num)
//           return -1;
//         else
//           return lastPos;
//       }

//       numOfRows = lastPos - firstPos + 1;
//       midPos = (numOfRows >> 1) + firstPos;

//       if (key < keyList[midPos]) {
//         lastPos = midPos - 1;
//       } else if (key > keyList[midPos]) {
//         firstPos = midPos + 1;
//       } else {
//         break;
//       }
//     }
//   }

//   return midPos;
// }

// static void doCheckGeneratedBlockRange(STsdbReader* pTsdbReadHandle) {
//   SQueryFilePos* cur = &pTsdbReadHandle->cur;

//   if (cur->rows > 0) {
//     if (ASCENDING_TRAVERSE(pTsdbReadHandle->order)) {
//       assert(cur->win.skey >= pTsdbReadHandle->window.skey && cur->win.ekey <= pTsdbReadHandle->window.ekey);
//     } else {
//       assert(cur->win.skey >= pTsdbReadHandle->window.ekey && cur->win.ekey <= pTsdbReadHandle->window.skey);
//     }

//     SColumnInfoData* pColInfoData = taosArrayGet(pTsdbReadHandle->pColumns, 0);
//     assert(cur->win.skey == ((TSKEY*)pColInfoData->pData)[0] &&
//            cur->win.ekey == ((TSKEY*)pColInfoData->pData)[cur->rows - 1]);
//   } else {
//     cur->win = pTsdbReadHandle->window;

//     int32_t step = ASCENDING_TRAVERSE(pTsdbReadHandle->order) ? 1 : -1;
//     cur->lastKey = pTsdbReadHandle->window.ekey + step;
//   }
// }

// static void copyAllRemainRowsFromFileBlock(STsdbReader* pTsdbReadHandle, STableBlockScanInfo* pCheckInfo,
//                                            SDataBlockInfo* pBlockInfo, int32_t endPos) {
//   SQueryFilePos* cur = &pTsdbReadHandle->cur;

//   SDataCols* pCols = pTsdbReadHandle->rhelper.pDCols[0];
//   TSKEY*     tsArray = pCols->cols[0].pData;

//   bool ascScan = ASCENDING_TRAVERSE(pTsdbReadHandle->order);

//   int32_t step = ascScan ? 1 : -1;

//   int32_t start = cur->pos;
//   int32_t end = endPos;

//   if (!ascScan) {
//     TSWAP(start, end);
//   }

//   assert(pTsdbReadHandle->outputCapacity >= (end - start + 1));
//   int32_t numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, 0, start, end);

//   // the time window should always be ascending order: skey <= ekey
//   cur->win = (STimeWindow){.skey = tsArray[start], .ekey = tsArray[end]};
//   cur->mixBlock = (numOfRows != pBlockInfo->rows);
//   cur->lastKey = tsArray[endPos] + step;
//   cur->blockCompleted = (ascScan ? (endPos == pBlockInfo->rows - 1) : (endPos == 0));

//   // The value of pos may be -1 or pBlockInfo->rows, and it is invalid in both cases.
//   int32_t pos = endPos + step;
//   updateInfoAfterMerge(pTsdbReadHandle, pCheckInfo, numOfRows, pos);
//   doCheckGeneratedBlockRange(pTsdbReadHandle);

//   tsdbDebug("%p uid:%" PRIu64 ", data block created, mixblock:%d, brange:%" PRIu64 "-%" PRIu64 " rows:%d, %s",
//             pTsdbReadHandle, pCheckInfo->tableId, cur->mixBlock, cur->win.skey, cur->win.ekey, cur->rows,
//             pTsdbReadHandle->idStr);
// }

// // only return the qualified data to client in terms of query time window, data rows in the same block but do not
// // be included in the query time window will be discarded
// static void doMergeTwoLevelData(STsdbReader* pTsdbReadHandle, STableBlockScanInfo* pCheckInfo, SBlock* pBlock) {
//   SQueryFilePos* cur = &pTsdbReadHandle->cur;
//   SDataBlockInfo blockInfo = GET_FILE_DATA_BLOCK_INFO(pCheckInfo, pBlock);
//   STsdbCfg*      pCfg = REPO_CFG(pTsdbReadHandle->pTsdb);

//   initTableMemIterator(pTsdbReadHandle, pCheckInfo);

//   SDataCols* pCols = pTsdbReadHandle->rhelper.pDCols[0];
//   assert(pCols->cols[0].type == TSDB_DATA_TYPE_TIMESTAMP && pCols->cols[0].colId == PRIMARYKEY_TIMESTAMP_COL_ID &&
//          cur->pos >= 0 && cur->pos < pBlock->numOfRows);
//   // Even Multi-Version supported, the records with duplicated TSKEY would be merged inside of tsdbLoadData
//   interface. TSKEY* tsArray = pCols->cols[0].pData; assert(pCols->numOfRows == pBlock->numOfRows && tsArray[0] ==
//   pBlock->minKey.ts &&
//          tsArray[pBlock->numOfRows - 1] == pBlock->maxKey.ts);

//   bool    ascScan = ASCENDING_TRAVERSE(pTsdbReadHandle->order);
//   int32_t step = ascScan ? 1 : -1;

//   // for search the endPos, so the order needs to reverse
//   int32_t order = ascScan ? TSDB_ORDER_DESC : TSDB_ORDER_ASC;

//   int32_t numOfCols = (int32_t)(QH_GET_NUM_OF_COLS(pTsdbReadHandle));
//   int32_t endPos = getEndPosInDataBlock(pTsdbReadHandle, &blockInfo);

//   STimeWindow* pWin = &blockInfo.window;
//   tsdbDebug("%p uid:%" PRIu64 " start merge data block, file block range:%" PRIu64 "-%" PRIu64
//             " rows:%d, start:%d, end:%d, %s",
//             pTsdbReadHandle, pCheckInfo->tableId, pWin->skey, pWin->ekey, blockInfo.rows, cur->pos, endPos,
//             pTsdbReadHandle->idStr);

//   // compared with the data from in-memory buffer, to generate the correct timestamp array list
//   int32_t numOfRows = 0;
//   int32_t curRow = 0;

//   int16_t   rv1 = -1;
//   int16_t   rv2 = -1;
//   STSchema* pSchema1 = NULL;
//   STSchema* pSchema2 = NULL;

//   int32_t pos = cur->pos;
//   cur->win = TSWINDOW_INITIALIZER;
//   bool adjustPos = false;

//   // no data in buffer, load data from file directly
//   if (pCheckInfo->iiter == NULL && pCheckInfo->iter == NULL) {
//     copyAllRemainRowsFromFileBlock(pTsdbReadHandle, pCheckInfo, &blockInfo, endPos);
//     return;
//   } else if (pCheckInfo->iter != NULL || pCheckInfo->iiter != NULL) {
//     SSkipListNode* node = NULL;
//     TSKEY          lastKeyAppend = TSKEY_INITIAL_VAL;

//     do {
//       STSRow* row2 = NULL;
//       STSRow* row1 = getSRowInTableMem(pCheckInfo, pTsdbReadHandle->order, pCfg->update, &row2, TD_VER_MAX);
//       if (row1 == NULL) {
//         break;
//       }

//       TSKEY key = TD_ROW_KEY(row1);
//       if ((key > pTsdbReadHandle->window.ekey && ascScan) || (key < pTsdbReadHandle->window.ekey && !ascScan)) {
//         break;
//       }

//       if (adjustPos) {
//         if (key == lastKeyAppend) {
//           pos -= step;
//         }
//         adjustPos = false;
//       }

//       if (((pos > endPos || tsArray[pos] > pTsdbReadHandle->window.ekey) && ascScan) ||
//           ((pos < endPos || tsArray[pos] < pTsdbReadHandle->window.ekey) && !ascScan)) {
//         break;
//       }

//       if ((key < tsArray[pos] && ascScan) || (key > tsArray[pos] && !ascScan)) {
//         if (rv1 != TD_ROW_SVER(row1)) {
//           //          pSchema1 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row1));
//           rv1 = TD_ROW_SVER(row1);
//         }
//         if (row2 && rv2 != TD_ROW_SVER(row2)) {
//           //          pSchema2 = tsdbGetTableSchemaByVersion(pTable, memRowVersion(row2));
//           rv2 = TD_ROW_SVER(row2);
//         }

//         numOfRows +=
//             mergeTwoRowFromMem(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, &curRow, row1, row2, numOfCols,
//                                pCheckInfo->tableId, pSchema1, pSchema2, pCfg->update, &lastKeyAppend);
//         if (cur->win.skey == TSKEY_INITIAL_VAL) {
//           cur->win.skey = key;
//         }

//         cur->win.ekey = key;
//         cur->lastKey = key + step;
//         cur->mixBlock = true;
//         moveToNextRowInMem(pCheckInfo);
//       } else if (key == tsArray[pos]) {  // data in buffer has the same timestamp of data in file block, ignore it
//         if (TD_SUPPORT_UPDATE(pCfg->update)) {
//           if (lastKeyAppend != key) {
//             if (lastKeyAppend != TSKEY_INITIAL_VAL) {
//               ++curRow;
//             }
//             lastKeyAppend = key;
//           }
//           // load data from file firstly
//           numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, curRow, pos, pos);

//           if (rv1 != TD_ROW_SVER(row1)) {
//             rv1 = TD_ROW_SVER(row1);
//           }
//           if (row2 && rv2 != TD_ROW_SVER(row2)) {
//             rv2 = TD_ROW_SVER(row2);
//           }

//           // still assign data into current row
//           numOfRows +=
//               mergeTwoRowFromMem(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, &curRow, row1, row2, numOfCols,
//                                  pCheckInfo->tableId, pSchema1, pSchema2, pCfg->update, &lastKeyAppend);

//           if (cur->win.skey == TSKEY_INITIAL_VAL) {
//             cur->win.skey = key;
//           }

//           cur->win.ekey = key;
//           cur->lastKey = key + step;
//           cur->mixBlock = true;

//           moveToNextRowInMem(pCheckInfo);

//           pos += step;
//           adjustPos = true;
//         } else {
//           // discard the memory record
//           moveToNextRowInMem(pCheckInfo);
//         }
//       } else if ((key > tsArray[pos] && ascScan) || (key < tsArray[pos] && !ascScan)) {
//         if (cur->win.skey == TSKEY_INITIAL_VAL) {
//           cur->win.skey = tsArray[pos];
//         }

//         int32_t end = doBinarySearchKey(pCols->cols[0].pData, pCols->numOfRows, key, order);
//         assert(end != -1);

//         if (tsArray[end] == key) {  // the value of key in cache equals to the end timestamp value, ignore it
// #if 0
//           if (pCfg->update == TD_ROW_DISCARD_UPDATE) {
//             moveToNextRowInMem(pCheckInfo);
//           } else {
//             end -= step;
//           }
// #endif
//           if (!TD_SUPPORT_UPDATE(pCfg->update)) {
//             moveToNextRowInMem(pCheckInfo);
//           } else {
//             end -= step;
//           }
//         }

//         int32_t qstart = 0, qend = 0;
//         getQualifiedRowsPos(pTsdbReadHandle, pos, end, numOfRows, &qstart, &qend);

//         if ((lastKeyAppend != TSKEY_INITIAL_VAL) && (lastKeyAppend != (ascScan ? tsArray[qstart] : tsArray[qend]))) {
//           ++curRow;
//         }

//         numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, curRow, qstart, qend);
//         pos += (qend - qstart + 1) * step;
//         if (numOfRows > 0) {
//           curRow = numOfRows - 1;
//         }

//         cur->win.ekey = ascScan ? tsArray[qend] : tsArray[qstart];
//         cur->lastKey = cur->win.ekey + step;
//         lastKeyAppend = cur->win.ekey;
//       }
//     } while (numOfRows < pTsdbReadHandle->outputCapacity);

//     if (numOfRows < pTsdbReadHandle->outputCapacity) {
//       /**
//        * if cache is empty, load remain file block data. In contrast, if there are remain data in cache, do NOT
//        * copy them all to result buffer, since it may be overlapped with file data block.
//        */
//       if (node == NULL || ((TD_ROW_KEY((STSRow*)SL_GET_NODE_DATA(node)) > pTsdbReadHandle->window.ekey) && ascScan)
//       ||
//           ((TD_ROW_KEY((STSRow*)SL_GET_NODE_DATA(node)) < pTsdbReadHandle->window.ekey) && !ascScan)) {
//         // no data in cache or data in cache is greater than the ekey of time window, load data from file block
//         if (cur->win.skey == TSKEY_INITIAL_VAL) {
//           cur->win.skey = tsArray[pos];
//         }

//         int32_t start = -1, end = -1;
//         getQualifiedRowsPos(pTsdbReadHandle, pos, endPos, numOfRows, &start, &end);

//         numOfRows = doCopyRowsFromFileBlock(pTsdbReadHandle, pTsdbReadHandle->outputCapacity, numOfRows, start, end);
//         pos += (end - start + 1) * step;

//         cur->win.ekey = ascScan ? tsArray[end] : tsArray[start];
//         cur->lastKey = cur->win.ekey + step;
//         cur->mixBlock = true;
//       }
//     }
//   }

//   cur->blockCompleted = (((pos > endPos || cur->lastKey > pTsdbReadHandle->window.ekey) && ascScan) ||
//                          ((pos < endPos || cur->lastKey < pTsdbReadHandle->window.ekey) && !ascScan));

//   if (!ascScan) {
//     TSWAP(cur->win.skey, cur->win.ekey);
//   }

//   updateInfoAfterMerge(pTsdbReadHandle, pCheckInfo, numOfRows, pos);
//   doCheckGeneratedBlockRange(pTsdbReadHandle);

//   tsdbDebug("%p uid:%" PRIu64 ", data block created, mixblock:%d, brange:%" PRIu64 "-%" PRIu64 " rows:%d, %s",
//             pTsdbReadHandle, pCheckInfo->tableId, cur->mixBlock, cur->win.skey, cur->win.ekey, cur->rows,
//             pTsdbReadHandle->idStr);
// }

static void cleanupBlockOrderSupporter(SBlockOrderSupporter* pSup) {
  taosMemoryFreeClear(pSup->numOfBlocksPerTable);
  taosMemoryFreeClear(pSup->indexPerTable);

  for (int32_t i = 0; i < pSup->numOfTables; ++i) {
    SBlockOrderWrapper* pBlockInfo = pSup->pDataBlockInfo[i];
    taosMemoryFreeClear(pBlockInfo);
  }

  taosMemoryFreeClear(pSup->pDataBlockInfo);
}

static int32_t initBlockOrderSupporter(SBlockOrderSupporter* pSup, int32_t numOfTables) {
  ASSERT(numOfTables >= 1);

  pSup->numOfBlocksPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  pSup->indexPerTable = taosMemoryCalloc(1, sizeof(int32_t) * numOfTables);
  pSup->pDataBlockInfo = taosMemoryCalloc(1, POINTER_BYTES * numOfTables);

  if (pSup->numOfBlocksPerTable == NULL || pSup->indexPerTable == NULL || pSup->pDataBlockInfo == NULL) {
    cleanupBlockOrderSupporter(pSup);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fileDataBlockOrderCompar(const void* pLeft, const void* pRight, void* param) {
  int32_t leftIndex = *(int32_t*)pLeft;
  int32_t rightIndex = *(int32_t*)pRight;

  SBlockOrderSupporter* pSupporter = (SBlockOrderSupporter*)param;

  int32_t leftTableBlockIndex = pSupporter->indexPerTable[leftIndex];
  int32_t rightTableBlockIndex = pSupporter->indexPerTable[rightIndex];

  if (leftTableBlockIndex > pSupporter->numOfBlocksPerTable[leftIndex]) {
    /* left block is empty */
    return 1;
  } else if (rightTableBlockIndex > pSupporter->numOfBlocksPerTable[rightIndex]) {
    /* right block is empty */
    return -1;
  }

  SBlockOrderWrapper* pLeftBlock = &pSupporter->pDataBlockInfo[leftIndex][leftTableBlockIndex];
  SBlockOrderWrapper* pRightBlock = &pSupporter->pDataBlockInfo[rightIndex][rightTableBlockIndex];

  return pLeftBlock->pBlock->aSubBlock[0].offset > pRightBlock->pBlock->aSubBlock[0].offset ? 1 : -1;
}

static int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks) {
  bool asc = ASCENDING_TRAVERSE(pReader->order);

  pBlockIter->numOfBlocks = numOfBlocks;
  taosArrayClear(pBlockIter->blockList);

  // access data blocks according to the offset of each block in asc/desc order.
  int32_t numOfTables = (int32_t)taosHashGetSize(pReader->status.pTableMap);

  SBlockOrderSupporter sup = {0};

  int32_t code = initBlockOrderSupporter(&sup, numOfTables);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t cnt = 0;
  void*   ptr = NULL;
  while (1) {
    ptr = taosHashIterate(pReader->status.pTableMap, ptr);
    if (ptr == NULL) {
      break;
    }

    STableBlockScanInfo* pTableScanInfo = (STableBlockScanInfo*)ptr;
    if (pTableScanInfo->pBlockList == NULL || taosArrayGetSize(pTableScanInfo->pBlockList) == 0) {
      continue;
    }

    size_t num = taosArrayGetSize(pTableScanInfo->pBlockList);
    sup.numOfBlocksPerTable[sup.numOfTables] = num;

    char* buf = taosMemoryMalloc(sizeof(SBlockOrderWrapper) * num);
    if (buf == NULL) {
      cleanupBlockOrderSupporter(&sup);
      return TSDB_CODE_TDB_OUT_OF_MEMORY;
    }

    sup.pDataBlockInfo[sup.numOfTables] = (SBlockOrderWrapper*)buf;
    for (int32_t k = 0; k < num; ++k) {
      SBlockOrderWrapper wrapper = {0};
      wrapper.pBlock = (SBlock*)taosArrayGet(pTableScanInfo->pBlockList, k);
      wrapper.uid = pTableScanInfo->uid;

      sup.pDataBlockInfo[sup.numOfTables][k] = wrapper;
      cnt++;
    }

    sup.numOfTables += 1;
  }

  ASSERT(numOfBlocks == cnt);

  // since there is only one table qualified, blocks are not sorted
  if (sup.numOfTables == 1) {
    for (int32_t i = 0; i < numOfBlocks; ++i) {
      SFileDataBlockInfo blockInfo = {.uid = sup.pDataBlockInfo[0][i].uid, .tbBlockIdx = i};
      taosArrayPush(pBlockIter->blockList, &blockInfo);
    }
    tsdbDebug("%p create blocks info struct completed for one table, %d blocks not sorted %s", pReader, cnt,
              pReader->idStr);

    pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %s", pReader, cnt, sup.numOfTables,
            pReader->idStr);

  assert(cnt <= numOfBlocks && sup.numOfTables <= numOfTables);

  SMultiwayMergeTreeInfo* pTree = NULL;
  uint8_t                 ret = tMergeTreeCreate(&pTree, sup.numOfTables, &sup, fileDataBlockOrderCompar);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanupBlockOrderSupporter(&sup);
    return TSDB_CODE_TDB_OUT_OF_MEMORY;
  }

  int32_t numOfTotal = 0;
  while (numOfTotal < cnt) {
    int32_t pos = tMergeTreeGetChosenIndex(pTree);
    int32_t index = sup.indexPerTable[pos]++;

    SFileDataBlockInfo blockInfo = {.uid = sup.pDataBlockInfo[pos][index].uid, .tbBlockIdx = index};
    taosArrayPush(pBlockIter->blockList, &blockInfo);

    // set data block index overflow, in order to disable the offset comparator
    if (sup.indexPerTable[pos] >= sup.numOfBlocksPerTable[pos]) {
      sup.indexPerTable[pos] = sup.numOfBlocksPerTable[pos] + 1;
    }

    numOfTotal += 1;
    tMergeTreeAdjust(pTree, tMergeTreeGetAdjustIndex(pTree));
  }

  tsdbDebug("%p %d data blocks sort completed, %s", pReader, cnt, pReader->idStr);
  cleanupBlockOrderSupporter(&sup);
  taosMemoryFree(pTree);

  pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
  return TSDB_CODE_SUCCESS;
}

static bool blockIteratorNext(SDataBlockIter* pBlockIter) {
  bool asc = ASCENDING_TRAVERSE(pBlockIter->order);

  int32_t step = asc ? 1 : -1;
  if ((pBlockIter->index >= pBlockIter->numOfBlocks - 1 && asc) || (pBlockIter->index <= 0 && (!asc))) {
    return false;
  }

  pBlockIter->index += step;
  return true;
}

/**
 * This is an two rectangles overlap cases.
 */
static int32_t dataBlockPartiallyRequired(STimeWindow* pWindow, SVersionRange* pVerRange, SBlock* pBlock) {
  return (pWindow->ekey < pBlock->maxKey.ts && pWindow->ekey >= pBlock->minKey.ts) ||
         (pWindow->skey > pBlock->minKey.ts && pWindow->skey <= pBlock->maxKey.ts) ||
         (pVerRange->minVer > pBlock->minVersion && pVerRange->minVer <= pBlock->maxVersion) ||
         (pVerRange->maxVer < pBlock->maxVersion && pVerRange->maxVer >= pBlock->minVersion);
}

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter) {
  SFileDataBlockInfo* pFBlockInfo = taosArrayGet(pBlockIter->blockList, pBlockIter->index);
  return pFBlockInfo;
}

static SBlock* getNeighborBlockOfSameTable(SFileDataBlockInfo* pFBlockInfo, STableBlockScanInfo* pTableBlockScanInfo,
                                           int32_t* nextIndex, int32_t order) {
  bool asc = ASCENDING_TRAVERSE(order);
  if (asc && pFBlockInfo->tbBlockIdx >= taosArrayGetSize(pTableBlockScanInfo->pBlockList) - 1) {
    return NULL;
  }

  if (!asc && pFBlockInfo->tbBlockIdx == 0) {
    return NULL;
  }

  int32_t step = asc ? 1 : -1;

  *nextIndex = pFBlockInfo->tbBlockIdx + step;
  SBlock* pNext = taosArrayGet(pTableBlockScanInfo->pBlockList, *nextIndex);
  return pNext;
}

static int32_t findFileBlockInfoIndex(SDataBlockIter* pBlockIter, SFileDataBlockInfo* pFBlockInfo) {
  ASSERT(pBlockIter != NULL && pFBlockInfo != NULL);

  int32_t step = ASCENDING_TRAVERSE(pBlockIter->order) ? 1 : -1;
  int32_t index = pBlockIter->index;

  while (index < pBlockIter->numOfBlocks && index >= 0) {
    SFileDataBlockInfo* pFBlock = taosArrayGet(pBlockIter->blockList, index);
    if (pFBlock->uid == pFBlockInfo->uid && pFBlock->tbBlockIdx == pFBlockInfo->tbBlockIdx) {
      return index;
    }

    index += step;
  }

  ASSERT(0);
  return -1;
}

static int32_t setFileBlockActiveInBlockIter(SDataBlockIter* pBlockIter, int32_t index, int32_t step) {
  if (index < 0 || index >= pBlockIter->numOfBlocks) {
    return -1;
  }

  SFileDataBlockInfo fblock = *(SFileDataBlockInfo*)taosArrayGet(pBlockIter->blockList, index);
  pBlockIter->index += step;

  if (index != pBlockIter->index) {
    taosArrayRemove(pBlockIter->blockList, index);
    taosArrayInsert(pBlockIter->blockList, pBlockIter->index, &fblock);

    SFileDataBlockInfo* pBlockInfo = taosArrayGet(pBlockIter->blockList, pBlockIter->index);
    ASSERT(pBlockInfo->uid == fblock.uid && pBlockInfo->tbBlockIdx == fblock.tbBlockIdx);
  }

  return TSDB_CODE_SUCCESS;
}

static bool overlapWithNeighborBlock(SBlock* pBlock, SBlock* pNeighbor, int32_t order) {
  // it is the last block in current file, no chance to overlap with neighbor blocks.
  if (ASCENDING_TRAVERSE(order)) {
    return pBlock->maxKey.ts == pNeighbor->minKey.ts;
  } else {
    return pBlock->minKey.ts == pNeighbor->maxKey.ts;
  }
}

static bool bufferDataInFileBlockGap(int32_t order, TSDBKEY key, SBlock* pBlock) {
  bool ascScan = ASCENDING_TRAVERSE(order);

  return (ascScan && (key.ts != TSKEY_INITIAL_VAL && key.ts <= pBlock->minKey.ts)) ||
         (!ascScan && (key.ts != TSKEY_INITIAL_VAL && key.ts >= pBlock->maxKey.ts));
}

static bool keyOverlapFileBlock(TSDBKEY key, SBlock* pBlock, SVersionRange* pVerRange) {
  return (key.ts >= pBlock->minKey.ts && key.ts <= pBlock->maxKey.ts) && (pBlock->maxVersion >= pVerRange->minVer) &&
         (pBlock->minVersion <= pVerRange->maxVer);
}

static bool overlapWithDelSkyline(STableBlockScanInfo* pBlockScanInfo, const SBlock* pBlock, int32_t order) {
  if (pBlockScanInfo->delSkyline == NULL) {
    return false;
  }

  TSDBKEY* pFirst = taosArrayGet(pBlockScanInfo->delSkyline, 0);
  TSDBKEY* pLast  = taosArrayGetLast(pBlockScanInfo->delSkyline);

  // ts is not overlap
  if (pBlock->minKey.ts > pLast->ts || pBlock->maxKey.ts < pFirst->ts) {
    return false;
  }

  int32_t step = ASCENDING_TRAVERSE(order)? 1:-1;

  // version is not overlap
  size_t num = taosArrayGetSize(pBlockScanInfo->delSkyline);
  for(int32_t i = pBlockScanInfo->fileDelIndex; i < num; i += step) {
    TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, i);
    if (p->ts >= pBlock->minKey.ts && p->ts <= pBlock->maxKey.ts) {
      if (p->version >= pBlock->minVersion) {
        return true;
      }
    } else if (p->ts > pBlock->maxKey.ts) {
      return false;
    }
  }

  ASSERT(0);
  return false;
}

// 1. the version of all rows should be less than the endVersion
// 2. current block should not overlap with next neighbor block
// 3. current timestamp should not be overlap with each other
// 4. output buffer should be large enough to hold all rows in current block
// 5. delete info should not overlap with current block data
static bool fileBlockShouldLoad(STsdbReader* pReader, SFileDataBlockInfo* pFBlock, SBlock* pBlock,
                                STableBlockScanInfo* pScanInfo, TSDBKEY key) {
  int32_t neighborIndex = 0;
  SBlock* pNeighbor = getNeighborBlockOfSameTable(pFBlock, pScanInfo, &neighborIndex, pReader->order);

  // overlap with neighbor
  bool overlapWithNeighbor = false;
  if (pNeighbor) {
    overlapWithNeighbor = overlapWithNeighborBlock(pBlock, pNeighbor, pReader->order);
  }

  // has duplicated ts of different version in this block
  bool hasDup = (pBlock->nSubBlock == 1)? pBlock->hasDup:true;
  bool overlapWithDel= overlapWithDelSkyline(pScanInfo, pBlock, pReader->order);

  return (overlapWithNeighbor || hasDup || dataBlockPartiallyRequired(&pReader->window, &pReader->verRange, pBlock) ||
          keyOverlapFileBlock(key, pBlock, &pReader->verRange) || (pBlock->nRow > pReader->capacity) || overlapWithDel);
}

static int32_t buildDataBlockFromBuf(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, int64_t endKey) {
  if (!(pBlockScanInfo->iiter.hasVal || pBlockScanInfo->iter.hasVal)) {
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pBlock = pReader->pResBlock;

  int64_t st = taosGetTimestampUs();
  int32_t code = buildDataBlockFromBufImpl(pBlockScanInfo, endKey, pReader->capacity, pReader);

  blockDataUpdateTsWindow(pBlock, 0);
  pBlock->info.uid = pBlockScanInfo->uid;

  setComposedBlockFlag(pReader, true);

  int64_t elapsedTime = taosGetTimestampUs() - st;
  tsdbDebug("%p build data block from cache completed, elapsed time:%" PRId64
            " us, numOfRows:%d, numOfCols:%d, brange: %" PRId64 " - %" PRId64 " %s",
            pReader, elapsedTime, pBlock->info.rows, (int32_t)blockDataGetNumOfCols(pBlock), pBlock->info.window.skey,
            pBlock->info.window.ekey, pReader->idStr);
  return code;
}

static int32_t doMergeBufAndFileRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, TSDBROW* pRow,
                                     STSRow* pTSRow, SIterInfo* pIter, int64_t key) {
  SRowMerger          merge = {0};
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
  SArray* pDelList = pBlockScanInfo->delSkyline;

  // ascending order traverse
  if (ASCENDING_TRAVERSE(pReader->order)) {
    if (key < k.ts) {
      tRowMergerInit(&merge, &fRow, pReader->pSchema);

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
      tRowMergerGetRow(&merge, &pTSRow);
    } else if (k.ts < key) {  // k.ts < key
      doMergeMultiRows(pRow, pBlockScanInfo->uid, pIter, pDelList, &pTSRow, pReader);
    } else {  // k.ts == key, ascending order: file block ----> imem rows -----> mem rows
      tRowMergerInit(&merge, &fRow, pReader->pSchema);
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);

      tRowMerge(&merge, pRow);
      doMergeRowsInBuf(pIter, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);

      tRowMergerGetRow(&merge, &pTSRow);
    }
  } else {  // descending order scan
    if (key < k.ts) {
      doMergeMultiRows(pRow, pBlockScanInfo->uid, pIter, pDelList, &pTSRow, pReader);
    } else if (k.ts < key) {
      tRowMergerInit(&merge, &fRow, pReader->pSchema);

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
      tRowMergerGetRow(&merge, &pTSRow);
    } else {  // descending order: mem rows -----> imem rows ------> file block
      updateSchema(pRow, pBlockScanInfo->uid, pReader);

      tRowMergerInit(&merge, pRow, pReader->pSchema);
      doMergeRowsInBuf(pIter, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);

      tRowMerge(&merge, &fRow);
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);

      tRowMergerGetRow(&merge, &pTSRow);
    }
  }

  tRowMergerClear(&merge);
  doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
  return TSDB_CODE_SUCCESS;
}

static int32_t doMergeThreeLevelRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo) {
  SRowMerger merge = {0};
  STSRow*    pTSRow = NULL;

  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  SArray*             pDelList = pBlockScanInfo->delSkyline;

  TSDBROW* pRow = getValidRow(&pBlockScanInfo->iter, pDelList, pReader);
  TSDBROW* piRow = getValidRow(&pBlockScanInfo->iiter, pDelList, pReader);
  ASSERT(pRow != NULL && piRow != NULL);

  int64_t key = pBlockData->aTSKEY[pDumpInfo->rowIndex];

  uint64_t uid = pBlockScanInfo->uid;

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  if (ASCENDING_TRAVERSE(pReader->order)) {
    // [1&2] key <= [k.ts && ik.ts]
    if (key <= k.ts && key <= ik.ts) {
      TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
      tRowMergerInit(&merge, &fRow, pReader->pSchema);

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);

      if (ik.ts == key) {
        tRowMerge(&merge, piRow);
        doMergeRowsInBuf(&pBlockScanInfo->iiter, key, pBlockScanInfo->delSkyline, &merge, pReader);
      }

      if (k.ts == key) {
        tRowMerge(&merge, pRow);
        doMergeRowsInBuf(&pBlockScanInfo->iter, key, pBlockScanInfo->delSkyline, &merge, pReader);
      }

      tRowMergerGetRow(&merge, &pTSRow);
      doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
      return TSDB_CODE_SUCCESS;
    } else {  // key > ik.ts || key > k.ts
      ASSERT(key != ik.ts);

      // [3] ik.ts < key <= k.ts
      // [4] ik.ts < k.ts <= key
      if (ik.ts < k.ts) {
        doMergeMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, &pTSRow, pReader);
        doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
        return TSDB_CODE_SUCCESS;
      }

      // [5] k.ts < key   <= ik.ts
      // [6] k.ts < ik.ts <= key
      if (k.ts < ik.ts) {
        doMergeMultiRows(pRow, uid, &pBlockScanInfo->iter, pDelList, &pTSRow, pReader);
        doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
        return TSDB_CODE_SUCCESS;
      }

      // [7] k.ts == ik.ts < key
      if (k.ts == ik.ts) {
        ASSERT(key > ik.ts && key > k.ts);

        doMergeMemIMemRows(pRow, piRow, pBlockScanInfo, pReader, &pTSRow);
        doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
        return TSDB_CODE_SUCCESS;
      }
    }
  } else {  // descending order scan
    // [1/2] k.ts >= ik.ts && k.ts >= key
    if (k.ts >= ik.ts && k.ts >= key) {
      updateSchema(pRow, uid, pReader);

      tRowMergerInit(&merge, pRow, pReader->pSchema);
      doMergeRowsInBuf(&pBlockScanInfo->iter, key, pBlockScanInfo->delSkyline, &merge, pReader);

      if (ik.ts == k.ts) {
        tRowMerge(&merge, piRow);
        doMergeRowsInBuf(&pBlockScanInfo->iiter, key, pBlockScanInfo->delSkyline, &merge, pReader);
      }

      if (k.ts == key) {
        TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
        tRowMerge(&merge, &fRow);
        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
      }

      tRowMergerGetRow(&merge, &pTSRow);
      doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
      return TSDB_CODE_SUCCESS;
    } else {
      ASSERT(ik.ts != k.ts);  // this case has been included in the previous if branch

      // [3] ik.ts > k.ts >= Key
      // [4] ik.ts > key >= k.ts
      if (ik.ts > key) {
        doMergeMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, &pTSRow, pReader);
        doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
        return TSDB_CODE_SUCCESS;
      }

      // [5] key > ik.ts > k.ts
      // [6] key > k.ts > ik.ts
      if (key > ik.ts) {
        TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
        tRowMergerInit(&merge, &fRow, pReader->pSchema);

        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
        tRowMergerGetRow(&merge, &pTSRow);
        doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
        return TSDB_CODE_SUCCESS;
      }

      //[7] key = ik.ts > k.ts
      if (key == ik.ts) {
        doMergeMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, &pTSRow, pReader);

        TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
        tRowMerge(&merge, &fRow);
        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
        tRowMergerGetRow(&merge, &pTSRow);
        doAppendOneRow(pReader->pResBlock, pReader, pTSRow);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  ASSERT(0);
}

static bool isValidFileBlockRow(SBlockData* pBlockData, SFileBlockDumpInfo* pDumpInfo,
                                STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader) {
  // check for version and time range
  int64_t ver = pBlockData->aVersion[pDumpInfo->rowIndex];
  if (ver > pReader->verRange.maxVer || ver < pReader->verRange.minVer) {
    return false;
  }

  int64_t ts = pBlockData->aTSKEY[pDumpInfo->rowIndex];
  if (ts > pReader->window.ekey || ts < pReader->window.skey) {
    return false;
  }

  TSDBKEY k = {.ts = ts, .version = ver};
  if (hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->fileDelIndex, &k, pReader->order)) {
    return false;
  }

  return true;
}

static bool outOfTimeWindow(int64_t ts, STimeWindow* pWindow) { return (ts > pWindow->ekey) || (ts < pWindow->skey); }

static int32_t buildComposedDataBlockImpl(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;

  SRowMerger merge = {0};
  STSRow*    pTSRow = NULL;

  int64_t  key = pBlockData->aTSKEY[pDumpInfo->rowIndex];
  TSDBROW* pRow = getValidRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  TSDBROW* piRow = getValidRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);

  if (pBlockScanInfo->iter.hasVal && pBlockScanInfo->iiter.hasVal) {
    return doMergeThreeLevelRows(pReader, pBlockScanInfo);
  } else {
    // imem + file
    if (pBlockScanInfo->iiter.hasVal) {
      return doMergeBufAndFileRows(pReader, pBlockScanInfo, piRow, pTSRow, &pBlockScanInfo->iiter, key);
    }

    // mem + file
    if (pBlockScanInfo->iter.hasVal) {
      return doMergeBufAndFileRows(pReader, pBlockScanInfo, pRow, pTSRow, &pBlockScanInfo->iter, key);
    }

    // imem & mem are all empty, only file exist
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
    tRowMergerInit(&merge, &fRow, pReader->pSchema);
    doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    tRowMergerGetRow(&merge, &pTSRow);
    doAppendOneRow(pReader->pResBlock, pReader, pTSRow);

    return TSDB_CODE_SUCCESS;
  }
}

static int32_t buildComposedDataBlock(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo) {
  SSDataBlock* pResBlock = pReader->pResBlock;

  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  int32_t             step = ASCENDING_TRAVERSE(pReader->order) ? 1 : -1;

  while (1) {
    // todo check the validate of row in file block
    {
      if (!isValidFileBlockRow(pBlockData, pDumpInfo, pBlockScanInfo, pReader)) {
        pDumpInfo->rowIndex += step;

        SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(&pReader->status.blockIter);
        SBlock*             pBlock = taosArrayGet(pBlockScanInfo->pBlockList, pFBlock->tbBlockIdx);

        if (pDumpInfo->rowIndex >= pBlock->nRow || pDumpInfo->rowIndex < 0) {
          setBlockAllDumped(pDumpInfo, pBlock, pReader->order);
          break;
        }

        continue;
      }
    }

    buildComposedDataBlockImpl(pReader, pBlockScanInfo);

    SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(&pReader->status.blockIter);
    SBlock*             pBlock = taosArrayGet(pBlockScanInfo->pBlockList, pFBlock->tbBlockIdx);

    // currently loaded file data block is consumed
    if (pDumpInfo->rowIndex >= pBlock->nRow || pDumpInfo->rowIndex < 0) {
      setBlockAllDumped(pDumpInfo, pBlock, pReader->order);
      break;
    }

    if (pResBlock->info.rows >= pReader->capacity) {
      break;
    }
  }

  pResBlock->info.uid = pBlockScanInfo->uid;
  blockDataUpdateTsWindow(pResBlock, 0);

  setComposedBlockFlag(pReader, true);

  tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64 " rows:%d, %s", pReader,
            pBlockScanInfo->uid, pResBlock->info.window.skey, pResBlock->info.window.ekey, pResBlock->info.rows,
            pReader->idStr);

  return TSDB_CODE_SUCCESS;
}

void setComposedBlockFlag(STsdbReader* pReader, bool composed) { pReader->status.composedDataBlock = composed; }

static int32_t initMemDataIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader) {
  if (pBlockScanInfo->iterInit) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  TSDBKEY startKey = {0};
  if (ASCENDING_TRAVERSE(pReader->order)) {
    startKey = (TSDBKEY){.ts = pReader->window.skey, .version = pReader->verRange.minVer};
  } else {
    startKey = (TSDBKEY){.ts = pReader->window.ekey, .version = pReader->verRange.maxVer};
  }

  int32_t backward = (!ASCENDING_TRAVERSE(pReader->order));

  STbData* d = NULL;
  if (pReader->pTsdb->mem != NULL) {
    tsdbGetTbDataFromMemTable(pReader->pTsdb->mem, pReader->suid, pBlockScanInfo->uid, &d);
    if (d != NULL) {
      code = tsdbTbDataIterCreate(d, &startKey, backward, &pBlockScanInfo->iter.iter);
      if (code == TSDB_CODE_SUCCESS) {
        pBlockScanInfo->iter.hasVal = (tsdbTbDataIterGet(pBlockScanInfo->iter.iter) != NULL);

        tsdbDebug("%p uid:%" PRId64 ", check data in mem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
                  "-%" PRId64 " %s",
                  pReader, pBlockScanInfo->uid, startKey.ts, pReader->order, d->minKey, d->maxKey, pReader->idStr);
      } else {
        tsdbError("%p uid:%" PRId64 ", failed to create iterator for imem, code:%s, %s", pReader, pBlockScanInfo->uid,
                  tstrerror(code), pReader->idStr);
        return code;
      }
    }
  } else {
    tsdbDebug("%p uid:%" PRId64 ", no data in mem, %s", pReader, pBlockScanInfo->uid, pReader->idStr);
  }

  STbData* di = NULL;
  if (pReader->pTsdb->imem != NULL) {
    tsdbGetTbDataFromMemTable(pReader->pTsdb->imem, pReader->suid, pBlockScanInfo->uid, &di);
    if (di != NULL) {
      code = tsdbTbDataIterCreate(di, &startKey, backward, &pBlockScanInfo->iiter.iter);
      if (code == TSDB_CODE_SUCCESS) {
        pBlockScanInfo->iiter.hasVal = (tsdbTbDataIterGet(pBlockScanInfo->iiter.iter) != NULL);

        tsdbDebug("%p uid:%" PRId64 ", check data in imem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
                  "-%" PRId64 " %s",
                  pReader, pBlockScanInfo->uid, startKey.ts, pReader->order, di->minKey, di->maxKey, pReader->idStr);
      } else {
        tsdbError("%p uid:%" PRId64 ", failed to create iterator for mem, code:%s, %s", pReader, pBlockScanInfo->uid,
                  tstrerror(code), pReader->idStr);
        return code;
      }
    }
  } else {
    tsdbDebug("%p uid:%" PRId64 ", no data in imem, %s", pReader, pBlockScanInfo->uid, pReader->idStr);
  }

  initDelSkylineIterator(pBlockScanInfo, pReader, d, di);

  pBlockScanInfo->iterInit = true;
  return TSDB_CODE_SUCCESS;
}

int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STbData* pMemTbData,
                               STbData* piMemTbData) {
  if (pBlockScanInfo->delSkyline != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  STsdb*  pTsdb = pReader->pTsdb;

  SArray* pDelData = taosArrayInit(4, sizeof(SDelData));

  SDelFile* pDelFile = tsdbFSStateGetDelFile(pTsdb->fs->cState);
  if (pDelFile) {
    SDelFReader* pDelFReader = NULL;
    code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb, NULL);
    if (code) {
      goto _err;
    }

    SArray* aDelIdx = taosArrayInit(4, sizeof(SDelIdx));
    if (aDelIdx == NULL) {
      goto _err;
    }

    code = tsdbReadDelIdx(pDelFReader, aDelIdx, NULL);
    if (code) {
      goto _err;
    }

    SDelIdx  idx = {.suid = pReader->suid, .uid = pBlockScanInfo->uid};
    SDelIdx* pIdx = taosArraySearch(aDelIdx, &idx, tCmprDelIdx, TD_EQ);

    code = tsdbReadDelData(pDelFReader, pIdx, pDelData, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }
  }

  SDelData* p = NULL;
  if (pMemTbData != NULL) {
    p = pMemTbData->pHead;
    while (p) {
      taosArrayPush(pDelData, p);
      p = p->pNext;
    }
  }

  if (piMemTbData != NULL) {
    p = piMemTbData->pHead;
    while (p) {
      taosArrayPush(pDelData, p);
      p = p->pNext;
    }
  }

  if (taosArrayGetSize(pDelData) > 0) {
    pBlockScanInfo->delSkyline = taosArrayInit(4, sizeof(TSDBKEY));
    code = tsdbBuildDeleteSkyline(pDelData, 0, (int32_t)(taosArrayGetSize(pDelData) - 1), pBlockScanInfo->delSkyline);
  }

  taosArrayDestroy(pDelData);
  pBlockScanInfo->iter.index =
      ASCENDING_TRAVERSE(pReader->order) ? 0 : taosArrayGetSize(pBlockScanInfo->delSkyline) - 1;
  pBlockScanInfo->iiter.index = pBlockScanInfo->iter.index;
  pBlockScanInfo->fileDelIndex = pBlockScanInfo->iter.index;
  return code;

_err:
  taosArrayDestroy(pDelData);
  return code;
}

static TSDBKEY getCurrentKeyInBuf(SDataBlockIter* pBlockIter, STsdbReader* pReader) {
  TSDBKEY key = {.ts = TSKEY_INITIAL_VAL};

  SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(pBlockIter);
  STableBlockScanInfo* pScanInfo = taosHashGet(pReader->status.pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));

  initMemDataIterator(pScanInfo, pReader);
  TSDBROW* pRow = getValidRow(&pScanInfo->iter, pScanInfo->delSkyline, pReader);
  if (pRow != NULL) {
    key = TSDBROW_KEY(pRow);
  }

  pRow = getValidRow(&pScanInfo->iiter, pScanInfo->delSkyline, pReader);
  if (pRow != NULL) {
    TSDBKEY k = TSDBROW_KEY(pRow);
    if (key.ts > k.ts) {
      key = k;
    }
  }

  return key;
}

static int32_t moveToNextFile(STsdbReader* pReader, int32_t* numOfBlocks) {
  SReaderStatus* pStatus = &pReader->status;

  while (1) {
    bool hasNext = filesetIteratorNext(&pStatus->fileIter, pReader);
    if (!hasNext) {  // no data files on disk
      break;
    }

    SArray* pIndexList = taosArrayInit(4, sizeof(SBlockIdx));
    int32_t code = doLoadBlockIndex(pReader, pReader->pFileReader, pIndexList);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (taosArrayGetSize(pIndexList) > 0) {
      uint32_t numOfValidTable = 0;
      code = doLoadFileBlock(pReader, pIndexList, &numOfValidTable, numOfBlocks);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      if (numOfValidTable > 0) {
        break;
      }
    }

    // no blocks in current file, try next files
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doBuildDataBlock(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;

  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockIter* pBlockIter = &pStatus->blockIter;

  SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(pBlockIter);
  STableBlockScanInfo* pScanInfo = taosHashGet(pStatus->pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));

  SBlock* pBlock = taosArrayGet(pScanInfo->pBlockList, pFBlock->tbBlockIdx);

  TSDBKEY key = getCurrentKeyInBuf(pBlockIter, pReader);
  if (fileBlockShouldLoad(pReader, pFBlock, pBlock, pScanInfo, key)) {
    tBlockDataInit(&pStatus->fileBlockData);
    code = doLoadFileBlockData(pReader, pBlockIter, pScanInfo, &pStatus->fileBlockData);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // build composed data block
    code = buildComposedDataBlock(pReader, pScanInfo);
  } else if (bufferDataInFileBlockGap(pReader->order, key, pBlock)) {
    // data in memory that are earlier than current file block
    // todo rows in buffer should be less than the file block in asc, greater than file block in desc
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->order)) ? pBlock->minKey.ts : pBlock->maxKey.ts;
    code = buildDataBlockFromBuf(pReader, pScanInfo, endKey);
  } else {  // whole block is required, return it directly
    SDataBlockInfo* pInfo = &pReader->pResBlock->info;
    pInfo->rows = pBlock->nRow;
    pInfo->uid = pScanInfo->uid;
    pInfo->window = (STimeWindow){.skey = pBlock->minKey.ts, .ekey = pBlock->maxKey.ts};
    setComposedBlockFlag(pReader, false);
    setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlock, pReader->order);
  }

  return code;
}

static int32_t buildBlockFromBufferSequentially(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;

  while (1) {
    if (pStatus->pTableIter == NULL) {
      pStatus->pTableIter = taosHashIterate(pStatus->pTableMap, NULL);
      if (pStatus->pTableIter == NULL) {
        return TSDB_CODE_SUCCESS;
      }
    }

    STableBlockScanInfo* pBlockScanInfo = pStatus->pTableIter;
    initMemDataIterator(pBlockScanInfo, pReader);

    int64_t endKey = (ASCENDING_TRAVERSE(pReader->order)) ? INT64_MAX : INT64_MIN;
    int32_t code = buildDataBlockFromBuf(pReader, pBlockScanInfo, endKey);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }

    // current table is exhausted, let's try the next table
    pStatus->pTableIter = taosHashIterate(pStatus->pTableMap, pStatus->pTableIter);
    if (pStatus->pTableIter == NULL) {
      return TSDB_CODE_SUCCESS;
    }
  }
}

// set the correct start position in case of the first/last file block, according to the query time window
static void initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(pBlockIter);
  STableBlockScanInfo* pScanInfo = taosHashGet(pReader->status.pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));
  SBlock*              pBlock = taosArrayGet(pScanInfo->pBlockList, pFBlock->tbBlockIdx);

  SReaderStatus* pStatus = &pReader->status;

  SFileBlockDumpInfo* pDumpInfo = &pStatus->fBlockDumpInfo;

  pDumpInfo->totalRows = pBlock->nRow;
  pDumpInfo->allDumped = false;
  pDumpInfo->rowIndex = ASCENDING_TRAVERSE(pReader->order) ? 0 : pBlock->nRow - 1;
}

static int32_t initForFirstBlockInFile(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  int32_t numOfBlocks = 0;
  int32_t code = moveToNextFile(pReader, &numOfBlocks);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // all data files are consumed, try data in buffer
  if (numOfBlocks == 0) {
    pReader->status.loadFromFile = false;
    return code;
  }

  // initialize the block iterator for a new fileset
  code = initBlockIterator(pReader, pBlockIter, numOfBlocks);

  // set the correct start position according to the query time window
  initBlockDumpInfo(pReader, pBlockIter);
  return code;
}

static bool fileBlockPartiallyRead(SFileBlockDumpInfo* pDumpInfo, bool asc) {
  return (!pDumpInfo->allDumped) &&
         ((pDumpInfo->rowIndex > 0 && asc) || (pDumpInfo->rowIndex < (pDumpInfo->totalRows - 1) && (!asc)));
}

static int32_t buildBlockFromFiles(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    asc = ASCENDING_TRAVERSE(pReader->order);

  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  while (1) {
    SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(&pReader->status.blockIter);
    STableBlockScanInfo* pScanInfo = taosHashGet(pReader->status.pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));

    SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

    if (fileBlockPartiallyRead(pDumpInfo, asc)) {  // file data block is partially loaded
      code = buildComposedDataBlock(pReader, pScanInfo);
    } else {
      // current block are exhausted, try the next file block
      if (pDumpInfo->allDumped) {
        // try next data block in current file
        bool hasNext = blockIteratorNext(&pReader->status.blockIter);
        if (hasNext) {  // check for the next block in the block accessed order list
          initBlockDumpInfo(pReader, pBlockIter);
        } else {  // data blocks in current file are exhausted, let's try the next file now
          code = initForFirstBlockInFile(pReader, pBlockIter);

          // error happens or all the data files are completely checked
          if ((code != TSDB_CODE_SUCCESS) || (pReader->status.loadFromFile == false)) {
            return code;
          }
        }
      }

      // current block is not loaded yet, or data in buffer may overlap with the file block.
      code = doBuildDataBlock(pReader);
    }

    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }
  }
}

static STsdb* getTsdbByRetentions(SVnode* pVnode, TSKEY winSKey, SRetention* retentions, const char* idStr,
                                  int8_t* pLevel) {
  if (VND_IS_RSMA(pVnode)) {
    int8_t  level = 0;
    int64_t now = taosGetTimestamp(pVnode->config.tsdbCfg.precision);

    for (int8_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
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

    int32_t     vgId = TD_VID(pVnode);
    const char* str = (idStr != NULL) ? idStr : "";

    if (level == TSDB_RETENTION_L0) {
      *pLevel = TSDB_RETENTION_L0;
      tsdbDebug("vgId:%d, read handle %p rsma level %d is selected to query %s", vgId, TSDB_RETENTION_L0, str);
      return VND_RSMA0(pVnode);
    } else if (level == TSDB_RETENTION_L1) {
      *pLevel = TSDB_RETENTION_L1;
      tsdbDebug("vgId:%d, read handle %p rsma level %d is selected to query %s", vgId, TSDB_RETENTION_L1, str);
      return VND_RSMA1(pVnode);
    } else {
      *pLevel = TSDB_RETENTION_L2;
      tsdbDebug("vgId:%d, read handle %p rsma level %d is selected to query %s", vgId, TSDB_RETENTION_L2, str);
      return VND_RSMA2(pVnode);
    }
  }

  return VND_TSDB(pVnode);
}

SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level) {
  int64_t startVer = (pCond->startVersion == -1)? 0:pCond->startVersion;

  if (VND_IS_RSMA(pVnode)) {
    return (SVersionRange){.minVer = startVer, .maxVer = tdRSmaGetMaxSubmitVer(pVnode->pSma, level)};
  }

  int64_t endVer = 0;
  if (pCond->endVersion == -1) {  // user not specified end version, set current maximum version of vnode as the endVersion
    endVer = pVnode->state.applied;
  } else {
    endVer = (pCond->endVersion > pVnode->state.applied)? pVnode->state.applied:pCond->endVersion;
  }

  return (SVersionRange){.minVer = startVer, .maxVer = endVer};
}

// // todo not unref yet, since it is not support multi-group interpolation query
// static UNUSED_FUNC void changeQueryHandleForInterpQuery(STsdbReader* pHandle) {
//   // filter the queried time stamp in the first place
//   STsdbReader* pTsdbReadHandle = (STsdbReader*)pHandle;

//   // starts from the buffer in case of descending timestamp order check data blocks
//   size_t numOfTables = taosArrayGetSize(pTsdbReadHandle->pTableCheckInfo);

//   int32_t i = 0;
//   while (i < numOfTables) {
//     STableBlockScanInfo* pCheckInfo = taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);

//     // the first qualified table for interpolation query
//     //    if ((pTsdbReadHandle->window.skey <= pCheckInfo->pTableObj->lastKey) &&
//     //        (pCheckInfo->pTableObj->lastKey != TSKEY_INITIAL_VAL)) {
//     //      break;
//     //    }

//     i++;
//   }

//   // there are no data in all the tables
//   if (i == numOfTables) {
//     return;
//   }

//   STableBlockScanInfo info = *(STableBlockScanInfo*)taosArrayGet(pTsdbReadHandle->pTableCheckInfo, i);
//   taosArrayClear(pTsdbReadHandle->pTableCheckInfo);

//   info.lastKey = pTsdbReadHandle->window.skey;
//   taosArrayPush(pTsdbReadHandle->pTableCheckInfo, &info);
// }

bool hasBeenDropped(const SArray* pDelList, int32_t* index, TSDBKEY* pKey, int32_t order) {
  ASSERT(pKey != NULL);
  if (pDelList == NULL) {
    return false;
  }
  size_t num = taosArrayGetSize(pDelList);
  bool asc = ASCENDING_TRAVERSE(order);
  int32_t step = asc? 1:-1;

  if (asc) {
    if (*index >= num - 1) {
      TSDBKEY* last = taosArrayGetLast(pDelList);
      ASSERT(pKey->ts >= last->ts);

      if (pKey->ts > last->ts) {
        return false;
      } else if (pKey->ts == last->ts) {
        TSDBKEY* prev = taosArrayGet(pDelList, num - 2);
        return (prev->version >= pKey->version);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pNext = taosArrayGet(pDelList, (*index) + 1);

      if (pKey->ts < pCurrent->ts) {
        return false;
      }

      if (pCurrent->ts <= pKey->ts && pNext->ts >= pKey->ts && pCurrent->version >= pKey->version) {
        return true;
      }

      while (pNext->ts <= pKey->ts && (*index) < num - 1) {
        (*index) += 1;

        if ((*index) < num - 1) {
          pCurrent = taosArrayGet(pDelList, *index);
          pNext = taosArrayGet(pDelList, (*index) + 1);

          // it is not a consecutive deletion range, ignore it
          if (pCurrent->version == 0 && pNext->version > 0) {
            continue;
          }

          if (pCurrent->ts <= pKey->ts && pNext->ts >= pKey->ts && pCurrent->version >= pKey->version) {
            return true;
          }
        }
      }

      return false;
    }
  } else {
    if (*index <= 0) {
      TSDBKEY* pFirst = taosArrayGet(pDelList, 0);

      if (pKey->ts < pFirst->ts) {
        return false;
      } else if (pKey->ts == pFirst->ts) {
        return pFirst->version >= pKey->version;
      } else {
        ASSERT(0);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pPrev = taosArrayGet(pDelList, (*index) - 1);

      if (pKey->ts > pCurrent->ts) {
        return false;
      }

      if (pPrev->ts <= pKey->ts && pCurrent->ts >= pKey->ts && pPrev->version >= pKey->version) {
        return true;
      }

      while (pPrev->ts >= pKey->ts && (*index) > 1) {
        (*index) += step;

        if ((*index) >= 1) {
          pCurrent = taosArrayGet(pDelList, *index);
          pPrev = taosArrayGet(pDelList, (*index) - 1);

          // it is not a consecutive deletion range, ignore it
          if (pCurrent->version > 0 && pPrev->version == 0) {
            continue;
          }

          if (pPrev->ts <= pKey->ts && pCurrent->ts >= pKey->ts && pPrev->version >= pKey->version) {
            return true;
          }
        }
      }

      return false;
    }
  }

  return false;
}

TSDBROW* getValidRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader) {
  if (!pIter->hasVal) {
    return NULL;
  }

  TSDBROW* pRow = tsdbTbDataIterGet(pIter->iter);
  TSDBKEY  key = TSDBROW_KEY(pRow);
  if (outOfTimeWindow(key.ts, &pReader->window)) {
    pIter->hasVal = false;
    return NULL;
  }

  // it is a valid data version
  if ((key.version <= pReader->verRange.maxVer && key.version >= pReader->verRange.minVer) &&
      (!hasBeenDropped(pDelList, &pIter->index, &key, pReader->order))) {
    return pRow;
  }

  while (1) {
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    if (!pIter->hasVal) {
      return NULL;
    }

    pRow = tsdbTbDataIterGet(pIter->iter);

    key = TSDBROW_KEY(pRow);
    if (outOfTimeWindow(key.ts, &pReader->window)) {
      pIter->hasVal = false;
      return NULL;
    }

    if (key.version <= pReader->verRange.maxVer && key.version >= pReader->verRange.minVer &&
        (!hasBeenDropped(pDelList, &pIter->index, &key, pReader->order))) {
      return pRow;
    }
  }
}

int32_t doMergeRowsInBuf(SIterInfo* pIter, int64_t ts, SArray* pDelList, SRowMerger* pMerger, STsdbReader* pReader) {
  while (1) {
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    if (!pIter->hasVal) {
      break;
    }

    // data exists but not valid
    TSDBROW* pRow = getValidRow(pIter, pDelList, pReader);
    if (pRow == NULL) {
      break;
    }

    // ts is not identical, quit
    TSDBKEY k = TSDBROW_KEY(pRow);
    if (k.ts != ts) {
      break;
    }

    tRowMerge(pMerger, pRow);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doMergeRowsInFileBlockImpl(SBlockData* pBlockData, int32_t rowIndex, int64_t key, SRowMerger* pMerger,
                                          SVersionRange* pVerRange, int32_t step) {
  while (pBlockData->aTSKEY[rowIndex] == key && rowIndex < pBlockData->nRow && rowIndex >= 0) {
    if (pBlockData->aVersion[rowIndex] > pVerRange->maxVer || pBlockData->aVersion[rowIndex] < pVerRange->minVer) {
      continue;
    }

    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, rowIndex);
    tRowMerge(pMerger, &fRow);
    rowIndex += step;
  }

  return rowIndex;
}

typedef enum {
  CHECK_FILEBLOCK_CONT = 0x1,
  CHECK_FILEBLOCK_QUIT = 0x2,
} CHECK_FILEBLOCK_STATE;

static int32_t checkForNeighborFileBlock(STsdbReader* pReader, STableBlockScanInfo* pScanInfo, SBlock* pBlock,
                                         SFileDataBlockInfo* pFBlock, SRowMerger* pMerger, int64_t key,
                                         CHECK_FILEBLOCK_STATE* state) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;

  *state = CHECK_FILEBLOCK_QUIT;
  int32_t step = ASCENDING_TRAVERSE(pReader->order) ? 1 : -1;

  int32_t nextIndex = -1;
  SBlock* pNeighborBlock = getNeighborBlockOfSameTable(pFBlock, pScanInfo, &nextIndex, pReader->order);
  if (pNeighborBlock == NULL) {  // do nothing
    return 0;
  }

  bool overlap = overlapWithNeighborBlock(pBlock, pNeighborBlock, pReader->order);
  if (overlap) {  // load next block
    SReaderStatus*  pStatus = &pReader->status;
    SDataBlockIter* pBlockIter = &pStatus->blockIter;

    // 1. find the next neighbor block in the scan block list
    SFileDataBlockInfo fb = {.uid = pFBlock->uid, .tbBlockIdx = nextIndex};
    int32_t            neighborIndex = findFileBlockInfoIndex(pBlockIter, &fb);

    // 2. remove it from the scan block list
    setFileBlockActiveInBlockIter(pBlockIter, neighborIndex, step);

    // 3. load the neighbor block, and set it to be the currently accessed file data block
    int32_t code = doLoadFileBlockData(pReader, pBlockIter, pScanInfo, &pStatus->fileBlockData);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // 4. check the data values
    initBlockDumpInfo(pReader, pBlockIter);

    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->verRange, step);

    if (pDumpInfo->rowIndex >= pBlock->nRow) {
      *state = CHECK_FILEBLOCK_CONT;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, STsdbReader* pReader,
                                SRowMerger* pMerger) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  bool    asc = ASCENDING_TRAVERSE(pReader->order);
  int64_t key = pBlockData->aTSKEY[pDumpInfo->rowIndex];
  int32_t step = asc ? 1 : -1;

  pDumpInfo->rowIndex += step;
  if (pDumpInfo->rowIndex <= pBlockData->nRow - 1) {
    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->verRange, step);
  }

  // all rows are consumed, let's try next file block
  if ((pDumpInfo->rowIndex >= pBlockData->nRow && asc) || (pDumpInfo->rowIndex < 0 && !asc)) {
    while (1) {
      CHECK_FILEBLOCK_STATE st;

      SFileDataBlockInfo* pFileBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);
      SBlock*             pCurrentBlock = taosArrayGet(pScanInfo->pBlockList, pFileBlockInfo->tbBlockIdx);
      checkForNeighborFileBlock(pReader, pScanInfo, pCurrentBlock, pFileBlockInfo, pMerger, key, &st);
      if (st == CHECK_FILEBLOCK_QUIT) {
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

void updateSchema(TSDBROW* pRow, uint64_t uid, STsdbReader* pReader) {
  int32_t sversion = TSDBROW_SVERSION(pRow);

  if (pReader->pSchema == NULL) {
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, uid, sversion);
  } else if (pReader->pSchema->version != sversion) {
    taosMemoryFreeClear(pReader->pSchema);
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, uid, sversion);
  }
}

void doMergeMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList, STSRow** pTSRow,
                      STsdbReader* pReader) {
  SRowMerger merge = {0};

  TSDBKEY k = TSDBROW_KEY(pRow);
  updateSchema(pRow, uid, pReader);

  tRowMergerInit(&merge, pRow, pReader->pSchema);
  doMergeRowsInBuf(pIter, k.ts, pDelList, &merge, pReader);
  tRowMergerGetRow(&merge, pTSRow);
}

void doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader,
                        STSRow** pTSRow) {
  SRowMerger merge = {0};

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  if (ASCENDING_TRAVERSE(pReader->order)) {  // ascending order imem --> mem
    updateSchema(piRow, pBlockScanInfo->uid, pReader);

    tRowMergerInit(&merge, piRow, pReader->pSchema);
    doMergeRowsInBuf(&pBlockScanInfo->iiter, ik.ts, pBlockScanInfo->delSkyline, &merge, pReader);

    tRowMerge(&merge, pRow);
    doMergeRowsInBuf(&pBlockScanInfo->iter, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
  } else {
    updateSchema(pRow, pBlockScanInfo->uid, pReader);

    tRowMergerInit(&merge, pRow, pReader->pSchema);
    doMergeRowsInBuf(&pBlockScanInfo->iter, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);

    tRowMerge(&merge, piRow);
    doMergeRowsInBuf(&pBlockScanInfo->iiter, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
  }

  tRowMergerGetRow(&merge, pTSRow);
}

int32_t tsdbGetNextRowInMem(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STSRow** pTSRow,
                            int64_t endKey) {
  TSDBROW* pRow = getValidRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  TSDBROW* piRow = getValidRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);
  SArray*  pDelList = pBlockScanInfo->delSkyline;

  // todo refactor
  bool asc = ASCENDING_TRAVERSE(pReader->order);
  if (pBlockScanInfo->iter.hasVal) {
    TSDBKEY k = TSDBROW_KEY(pRow);
    if ((k.ts >= endKey && asc) || (k.ts <= endKey && !asc)) {
      pRow = NULL;
    }
  }

  if (pBlockScanInfo->iiter.hasVal) {
    TSDBKEY k = TSDBROW_KEY(piRow);
    if ((k.ts >= endKey && asc) || (k.ts <= endKey && !asc)) {
      piRow = NULL;
    }
  }

  if (pBlockScanInfo->iter.hasVal && pBlockScanInfo->iiter.hasVal && pRow != NULL && piRow != NULL) {
    TSDBKEY k = TSDBROW_KEY(pRow);
    TSDBKEY ik = TSDBROW_KEY(piRow);

    if (ik.ts < k.ts) {  // ik.ts < k.ts
      doMergeMultiRows(piRow, pBlockScanInfo->uid, &pBlockScanInfo->iiter, pDelList, pTSRow, pReader);
    } else if (k.ts < ik.ts) {
      doMergeMultiRows(pRow, pBlockScanInfo->uid, &pBlockScanInfo->iter, pDelList, pTSRow, pReader);
    } else {  // ik.ts == k.ts
      doMergeMemIMemRows(pRow, piRow, pBlockScanInfo, pReader, pTSRow);
    }

    return TSDB_CODE_SUCCESS;
  }

  if (pBlockScanInfo->iter.hasVal && pRow != NULL) {
    doMergeMultiRows(pRow, pBlockScanInfo->uid, &pBlockScanInfo->iter, pDelList, pTSRow, pReader);
    return TSDB_CODE_SUCCESS;
  }

  if (pBlockScanInfo->iiter.hasVal && piRow != NULL) {
    doMergeMultiRows(piRow, pBlockScanInfo->uid, &pBlockScanInfo->iiter, pDelList, pTSRow, pReader);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doAppendOneRow(SSDataBlock* pBlock, STsdbReader* pReader, STSRow* pTSRow) {
  int32_t numOfRows = pBlock->info.rows;
  int32_t numOfCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  STSchema*           pSchema = pReader->pSchema;

  SColVal colVal = {0};
  int32_t i = 0, j = 0;

  SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
  if (pColInfoData->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    colDataAppend(pColInfoData, numOfRows, (const char*)&pTSRow->ts, false);
    i += 1;
  }

  while (i < numOfCols && j < pSchema->numOfCols) {
    pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    col_id_t colId = pColInfoData->info.colId;

    if (colId == pSchema->columns[j].colId) {
      tTSRowGetVal(pTSRow, pReader->pSchema, j, &colVal);
      doCopyColVal(pColInfoData, numOfRows, i, &colVal, pSupInfo);
      i += 1;
      j += 1;
    } else if (colId < pSchema->columns[j].colId) {
      colDataAppendNULL(pColInfoData, numOfRows);
      i += 1;
    } else if (colId > pSchema->columns[j].colId) {
      j += 1;
    }
  }

  // set null value since current column does not exist in the "pSchema"
  while (i < numOfCols) {
    pColInfoData = taosArrayGet(pBlock->pDataBlock, i);
    colDataAppendNULL(pColInfoData, numOfRows);
    i += 1;
  }

  pBlock->info.rows += 1;
  return TSDB_CODE_SUCCESS;
}

int32_t buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                  STsdbReader* pReader) {
  SSDataBlock* pBlock = pReader->pResBlock;

  do {
    STSRow* pTSRow = NULL;
    tsdbGetNextRowInMem(pBlockScanInfo, pReader, &pTSRow, endKey);
    if (pTSRow == NULL) {
      break;
    }

    doAppendOneRow(pBlock, pReader, pTSRow);

    // no data in buffer, return immediately
    if (!(pBlockScanInfo->iter.hasVal || pBlockScanInfo->iiter.hasVal)) {
      break;
    }

    if (pBlock->info.rows >= capacity) {
      break;
    }
  } while (1);

  ASSERT(pBlock->info.rows <= capacity);
  return TSDB_CODE_SUCCESS;
}

// todo refactor, use arraylist instead
int32_t tsdbSetTableId(STsdbReader* pReader, int64_t uid) {
  ASSERT(pReader != NULL);
  taosHashClear(pReader->status.pTableMap);

  STableBlockScanInfo info = {.lastKey = 0, .uid = uid};
  taosHashPut(pReader->status.pTableMap, &info.uid, sizeof(uint64_t), &info, sizeof(info));
  return TDB_CODE_SUCCESS;
}

void* tsdbGetIdx(SMeta* pMeta) {
  if (pMeta == NULL) {
    return NULL;
  }
  return metaGetIdx(pMeta);
}

void* tsdbGetIvtIdx(SMeta* pMeta) {
  if (pMeta == NULL) {
    return NULL;
  }
  return metaGetIvtIdx(pMeta);
}

/**
 * @brief Get all suids since suid
 *
 * @param pMeta
 * @param suid return all suids in one vnode if suid is 0
 * @param list
 * @return int32_t
 */
int32_t tsdbGetStbIdList(SMeta* pMeta, int64_t suid, SArray* list) {
  SMStbCursor* pCur = metaOpenStbCursor(pMeta, suid);
  if (!pCur) {
    return TSDB_CODE_FAILED;
  }

  while (1) {
    tb_uid_t id = metaStbCursorNext(pCur);
    if (id == 0) {
      break;
    }

    taosArrayPush(list, &id);
  }

  metaCloseStbCursor(pCur);
  return TSDB_CODE_SUCCESS;
}

// ====================================== EXPOSED APIs ======================================
int32_t tsdbReaderOpen(SVnode* pVnode, SQueryTableDataCond* pCond, SArray* pTableList, STsdbReader** ppReader,
                       const char* idstr) {
  int32_t code = tsdbReaderCreate(pVnode, pCond, ppReader, idstr);
  if (code) {
    goto _err;
  }

  if (pCond->suid != 0) {
    (*ppReader)->pSchema = metaGetTbTSchema((*ppReader)->pTsdb->pVnode->pMeta, (*ppReader)->suid, -1);
    ASSERT((*ppReader)->pSchema);
  } else if (taosArrayGetSize(pTableList) > 0) {
    STableKeyInfo* pKey = taosArrayGet(pTableList, 0);
    (*ppReader)->pSchema = metaGetTbTSchema((*ppReader)->pTsdb->pVnode->pMeta, pKey->uid, -1);
  }

  STsdbReader* pReader = *ppReader;
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, %s", pReader, pReader->idStr);
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOfTables = taosArrayGetSize(pTableList);
  pReader->status.pTableMap = createDataBlockScanInfo(pReader, pTableList->pData, numOfTables);
  if (pReader->status.pTableMap == NULL) {
    tsdbReaderClose(pReader);
    *ppReader = NULL;

    code = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  STsdbFSState* pFState = pReader->pTsdb->fs->cState;
  initFilesetIterator(&pReader->status.fileIter, pFState, pReader->order, pReader->idStr);
  resetDataBlockIterator(&pReader->status.blockIter, pReader->order);

  // no data in files, let's try buffer in memory
  if (pReader->status.fileIter.numOfFiles == 0) {
    pReader->status.loadFromFile = false;
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  tsdbDebug("%p total numOfTable:%d in this query %s", pReader, numOfTables, pReader->idStr);
  return code;

_err:
  tsdbError("failed to create data reader, code: %s %s", tstrerror(code), pReader->idStr);
  return code;
}

void tsdbReaderClose(STsdbReader* pReader) {
  if (pReader == NULL) {
    return;
  }

  blockDataDestroy(pReader->pResBlock);
  taosMemoryFreeClear(pReader->suppInfo.plist);

  taosArrayDestroy(pReader->suppInfo.pColAgg);
  taosMemoryFree(pReader->suppInfo.slotIds);

#if 0
//   if (pReader->status.pTableScanInfo != NULL) {
//     pReader->status.pTableScanInfo = destroyTableCheckInfo(pReader->status.pTableScanInfo);
//   }

//   tsdbDestroyReadH(&pReader->rhelper);

//   tdFreeDataCols(pReader->pDataCols);
//   pReader->pDataCols = NULL;
//
//   pReader->prev = doFreeColumnInfoData(pReader->prev);
//   pReader->next = doFreeColumnInfoData(pReader->next);
#endif

  SIOCostSummary* pCost = &pReader->cost;

  tsdbDebug("%p :io-cost summary: head-file read cnt:%" PRIu64 ", head-file time:%" PRIu64 " us, statis-info:%" PRId64
            " us, datablock:%" PRId64 " us, check data:%" PRId64 " us, %s",
            pReader, pCost->headFileLoad, pCost->headFileLoadTime, pCost->smaLoadTime, pCost->blockLoadTime,
            pCost->checkForNextTime, pReader->idStr);

  taosMemoryFree(pReader->idStr);
  taosMemoryFree(pReader->pSchema);
  taosMemoryFreeClear(pReader);
}

bool tsdbNextDataBlock(STsdbReader* pReader) {
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    return false;
  }

  // cleanup the data that belongs to the previous data block
  SSDataBlock* pBlock = pReader->pResBlock;
  blockDataCleanup(pBlock);

  int64_t        stime = taosGetTimestampUs();
  int64_t        elapsedTime = stime;
  SReaderStatus* pStatus = &pReader->status;

  if (pReader->type == BLOCK_LOAD_OFFSET_ORDER) {
    if (pStatus->loadFromFile) {
      int32_t code = buildBlockFromFiles(pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return false;
      }

      if (pBlock->info.rows > 0) {
        return true;
      } else {
        buildBlockFromBufferSequentially(pReader);
        return pBlock->info.rows > 0;
      }
    } else {  // no data in files, let's try the buffer
      buildBlockFromBufferSequentially(pReader);
      return pBlock->info.rows > 0;
    }
  } else if (pReader->type == BLOCK_LOAD_TABLESEQ_ORDER) {
  } else if (pReader->type == BLOCK_LOAD_EXTERN_ORDER) {
  } else {
    ASSERT(0);
  }
  return false;
}

void tsdbRetrieveDataBlockInfo(STsdbReader* pReader, SDataBlockInfo* pDataBlockInfo) {
  ASSERT(pDataBlockInfo != NULL && pReader != NULL);
  pDataBlockInfo->rows = pReader->pResBlock->info.rows;
  pDataBlockInfo->uid = pReader->pResBlock->info.uid;
  pDataBlockInfo->window = pReader->pResBlock->info.window;
}

int32_t tsdbRetrieveDatablockSMA(STsdbReader* pReader, SColumnDataAgg*** pBlockStatis, bool* allHave) {
  int32_t code = 0;
  *allHave = false;

  // there is no statistics data for composed block
  if (pReader->status.composedDataBlock) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(&pReader->status.blockIter);
  STableBlockScanInfo* pBlockScanInfo = taosHashGet(pReader->status.pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));
  SBlock*              pBlock = taosArrayGet(pBlockScanInfo->pBlockList, pFBlock->tbBlockIdx);

  int64_t stime = taosGetTimestampUs();

  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;

  if (tBlockHasSma(pBlock)) {
    code = tsdbReadBlockSma(pReader->pFileReader, pBlock, pSup->pColAgg, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbDebug("vgId:%d, failed to load block SMA for uid %" PRIu64 ", code:%s, %s", 0, pFBlock->uid, tstrerror(code),
                pReader->idStr);
      return code;
    }
  } else {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *allHave = true;

  // always load the first primary timestamp column data
  SColumnDataAgg* pTsAgg = &pSup->tsColAgg;

  pTsAgg->numOfNull = 0;
  pTsAgg->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pTsAgg->min = pReader->pResBlock->info.window.skey;
  pTsAgg->max = pReader->pResBlock->info.window.ekey;
  pSup->plist[0] = pTsAgg;

  // update the number of NULL data rows
  size_t numOfCols = blockDataGetNumOfCols(pReader->pResBlock);

  int32_t i = 0, j = 0;
  while (j < numOfCols && i < taosArrayGetSize(pSup->pColAgg)) {
    SColumnDataAgg* pAgg = taosArrayGet(pSup->pColAgg, i);
    if (pAgg->colId == pSup->colIds[j]) {
      if (IS_BSMA_ON(&(pReader->pSchema->columns[i]))) {
        pSup->plist[j] = pAgg;
      } else {
        *allHave = false;
      }
      i += 1;
      j += 1;
    } else if (pAgg->colId < pSup->colIds[j]) {
      i += 1;
    } else if (pSup->colIds[j] < pAgg->colId) {
      j += 1;
    }
  }

  int64_t elapsed = taosGetTimestampUs() - stime;
  pReader->cost.smaLoadTime += elapsed;

  *pBlockStatis = pSup->plist;

  tsdbDebug("vgId:%d, succeed to load block SMA for uid %" PRIu64 ", elapsed time:%" PRId64 "us, %s", 0, pFBlock->uid,
            elapsed, pReader->idStr);

  return code;
}

SArray* tsdbRetrieveDataBlock(STsdbReader* pReader, SArray* pIdList) {
  SReaderStatus* pStatus = &pReader->status;

  if (pStatus->composedDataBlock) {
    return pReader->pResBlock->pDataBlock;
  }

  SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(&pStatus->blockIter);
  STableBlockScanInfo* pBlockScanInfo = taosHashGet(pStatus->pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));

  int32_t code = tBlockDataInit(&pStatus->fileBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  code = doLoadFileBlockData(pReader, &pStatus->blockIter, pBlockScanInfo, &pStatus->fileBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  copyBlockDataToSDataBlock(pReader, pBlockScanInfo);
  return pReader->pResBlock->pDataBlock;
}

int32_t tsdbReaderReset(STsdbReader* pReader, SQueryTableDataCond* pCond, int32_t tWinIdx) {
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    return TSDB_CODE_SUCCESS;
  }

  pReader->order               = pCond->order;
  pReader->type                = BLOCK_LOAD_OFFSET_ORDER;
  pReader->status.loadFromFile = true;
  pReader->status.pTableIter = NULL;

  pReader->window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows[tWinIdx]);

  // allocate buffer in order to load data blocks from file
  memset(&pReader->suppInfo.tsColAgg, 0, sizeof(SColumnDataAgg));
  memset(pReader->suppInfo.plist, 0, POINTER_BYTES);

  pReader->suppInfo.tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;

  // todo set the correct numOfTables
  int32_t         numOfTables = 1;
  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  STsdbFSState* pFState = pReader->pTsdb->fs->cState;
  initFilesetIterator(&pReader->status.fileIter, pFState, pReader->order, pReader->idStr);
  resetDataBlockIterator(&pReader->status.blockIter, pReader->order);
  resetDataBlockScanInfo(pReader->status.pTableMap);

  int32_t code = 0;
  // no data in files, let's try buffer in memory
  if (pReader->status.fileIter.numOfFiles == 0) {
    pReader->status.loadFromFile = false;
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  tsdbDebug("%p reset reader, suid:%" PRIu64 ", numOfTables:%d, query range:%" PRId64 " - %" PRId64 " in query %s",
            pReader, pReader->suid, numOfTables, pReader->window.skey, pReader->window.ekey, pReader->idStr);
  return code;
}

static int32_t getBucketIndex(int32_t startRow, int32_t bucketRange, int32_t numOfRows) {
  return (numOfRows - startRow) / bucketRange;
}

int32_t tsdbGetFileBlocksDistInfo(STsdbReader* pReader, STableBlockDistInfo* pTableBlockInfo) {
  int32_t code = TSDB_CODE_SUCCESS;
  pTableBlockInfo->totalSize = 0;
  pTableBlockInfo->totalRows = 0;

  // find the start data block in file
  SReaderStatus* pStatus = &pReader->status;

  STsdbCfg* pc = &pReader->pTsdb->pVnode->config.tsdbCfg;
  pTableBlockInfo->defMinRows = pc->minRows;
  pTableBlockInfo->defMaxRows = pc->maxRows;

  int32_t bucketRange = ceil((pc->maxRows - pc->minRows) / 20.0);

  pTableBlockInfo->numOfFiles += 1;

  int32_t numOfTables = (int32_t)taosHashGetSize(pStatus->pTableMap);
  int     defaultRows = 4096;

  SDataBlockIter* pBlockIter = &pStatus->blockIter;
  pTableBlockInfo->numOfFiles += pStatus->fileIter.numOfFiles;
  pTableBlockInfo->numOfBlocks += pBlockIter->numOfBlocks;

  pTableBlockInfo->numOfTables = numOfTables;
  bool hasNext = true;

  while (true) {
    if (hasNext) {
      SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(pBlockIter);
      STableBlockScanInfo* pScanInfo = taosHashGet(pStatus->pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));
      SBlock*              pBlock = taosArrayGet(pScanInfo->pBlockList, pFBlock->tbBlockIdx);

      int32_t numOfRows = pBlock->nRow;
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

      int32_t bucketIndex = getBucketIndex(pTableBlockInfo->defMinRows, bucketRange, numOfRows);
      pTableBlockInfo->blockRowsHisto[bucketIndex]++;

      hasNext = blockIteratorNext(&pStatus->blockIter);

    } else {
      code = initForFirstBlockInFile(pReader, pBlockIter);
      if ((code != TSDB_CODE_SUCCESS) || (pReader->status.loadFromFile == false)) {
        break;
      }

      pTableBlockInfo->numOfBlocks += pBlockIter->numOfBlocks;
    }

/*
    hasNext = blockIteratorNext(&pStatus->blockIter);
*/


//         tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, %s", pReader, numOfBlocks, numOfTables,
//                   pReader->pFileGroup->fid, pReader->idStr);
  }

  return code;
}

int64_t tsdbGetNumOfRowsInMemTable(STsdbReader* pReader) {
  int64_t rows = 0;

  SReaderStatus* pStatus = &pReader->status;
  pStatus->pTableIter = taosHashIterate(pStatus->pTableMap, NULL);

  while (pStatus->pTableIter != NULL) {
    STableBlockScanInfo* pBlockScanInfo = pStatus->pTableIter;

    STbData* d = NULL;
    if (pReader->pTsdb->mem != NULL) {
      tsdbGetTbDataFromMemTable(pReader->pTsdb->mem, pReader->suid, pBlockScanInfo->uid, &d);
      if (d != NULL) {
        rows += tsdbGetNRowsInTbData(d);
      }
    }

    STbData* di = NULL;
    if (pReader->pTsdb->imem != NULL) {
      tsdbGetTbDataFromMemTable(pReader->pTsdb->imem, pReader->suid, pBlockScanInfo->uid, &di);
      if (di != NULL) {
        rows += tsdbGetNRowsInTbData(di);
      }
    }

    // current table is exhausted, let's try the next table
    pStatus->pTableIter = taosHashIterate(pStatus->pTableMap, pStatus->pTableIter);
  }

  return rows;
}
