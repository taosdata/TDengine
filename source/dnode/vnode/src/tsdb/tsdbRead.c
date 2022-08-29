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

#include "osDef.h"
#include "tsdb.h"

#define ASCENDING_TRAVERSE(o)  (o == TSDB_ORDER_ASC)
#define ALL_ROWS_CHECKED_INDEX (INT16_MIN)
#define INITIAL_ROW_INDEX_VAL  (-1)

typedef enum {
  EXTERNAL_ROWS_PREV = 0x1,
  EXTERNAL_ROWS_MAIN = 0x2,
  EXTERNAL_ROWS_NEXT = 0x3,
} EContentData;

typedef struct {
  STbDataIter* iter;
  int32_t      index;
  bool         hasVal;
} SIterInfo;

typedef struct {
  int32_t numOfBlocks;
  int32_t numOfLastBlocks;
} SBlockNumber;

typedef struct STableBlockScanInfo {
  uint64_t  uid;
  TSKEY     lastKey;
  SMapData  mapData;      // block info (compressed)
  SArray*   pBlockList;   // block data index list
  SIterInfo iter;         // mem buffer skip list iterator
  SIterInfo iiter;        // imem buffer skip list iterator
  SArray*   delSkyline;   // delete info for this table
  int32_t   fileDelIndex; // file block delete index
  int32_t   lastBlockDelIndex;// delete index for last block
  bool      iterInit;     // whether to initialize the in-memory skip list iterator or not
  int16_t   indexInBlockL;// row position in last block
} STableBlockScanInfo;

typedef struct SBlockOrderWrapper {
  int64_t uid;
  int64_t offset;
} SBlockOrderWrapper;

typedef struct SBlockOrderSupporter {
  SBlockOrderWrapper** pDataBlockInfo;
  int32_t*             indexPerTable;
  int32_t*             numOfBlocksPerTable;
  int32_t              numOfTables;
} SBlockOrderSupporter;

typedef struct SIOCostSummary {
  int64_t numOfBlocks;
  double  blockLoadTime;
  double  buildmemBlock;
  int64_t headFileLoad;
  double  headFileLoadTime;
  int64_t smaDataLoad;
  double  smaLoadTime;
  int64_t lastBlockLoad;
  double  lastBlockLoadTime;
} SIOCostSummary;

typedef struct SBlockLoadSuppInfo {
  SArray*          pColAgg;
  SColumnDataAgg   tsColAgg;
  SColumnDataAgg** plist;
  int16_t*         colIds;    // column ids for loading file block data
  char**           buildBuf;  // build string tmp buffer, todo remove it later after all string format being updated.
} SBlockLoadSuppInfo;

typedef struct SVersionRange {
  uint64_t minVer;
  uint64_t maxVer;
} SVersionRange;

typedef struct SLastBlockReader {
  SArray*       pBlockL;
  int32_t       currentBlockIndex;
  SBlockData    lastBlockData;
  STimeWindow   window;
  SVersionRange verRange;
  int32_t       order;
  uint64_t      uid;
  int16_t*      rowIndex;         // row index ptr, usually from the STableBlockScanInfo->indexInBlockL
} SLastBlockReader;

typedef struct SFilesetIter {
  int32_t           numOfFiles;    // number of total files
  int32_t           index;         // current accessed index in the list
  SArray*           pFileList;     // data file list
  int32_t           order;
  SLastBlockReader* pLastBlockReader; // last file block reader
} SFilesetIter;

typedef struct SFileDataBlockInfo {
  // index position in STableBlockScanInfo in order to check whether neighbor block overlaps with it
  uint64_t uid;
  int32_t  tbBlockIdx;
} SFileDataBlockInfo;

typedef struct SDataBlockIter {
  int32_t   numOfBlocks;
  int32_t   index;
  SArray*   blockList;      // SArray<SFileDataBlockInfo>
  int32_t   order;
  SBlock    block;          // current SBlock data
  SHashObj* pTableMap;
} SDataBlockIter;

typedef struct SFileBlockDumpInfo {
  int32_t totalRows;
  int32_t rowIndex;
  int64_t lastKey;
  bool    allDumped;
} SFileBlockDumpInfo;

typedef struct SUidOrderCheckInfo {
  uint64_t*            tableUidList;  // access table uid list in uid ascending order list
  int32_t              currentIndex;  // index in table uid list
} SUidOrderCheckInfo;

typedef struct SReaderStatus {
  bool                 loadFromFile;       // check file stage
  bool                 composedDataBlock;  // the returned data block is a composed block or not
  SHashObj*            pTableMap;          // SHash<STableBlockScanInfo>
  STableBlockScanInfo* pTableIter;         // table iterator used in building in-memory buffer data blocks.
  SUidOrderCheckInfo   uidCheckInfo;      // check all table in uid order
  SFileBlockDumpInfo   fBlockDumpInfo;
  SDFileSet*           pCurrentFileset;    // current opened file set
  SBlockData           fileBlockData;
  SFilesetIter         fileIter;
  SDataBlockIter       blockIter;
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
  STsdbReadSnap*     pReadSnap;
  SIOCostSummary     cost;
  STSchema*          pSchema;     // the newest version schema
  STSchema*          pMemSchema;  // the previous schema for in-memory data, to avoid load schema too many times
  SDataFReader*      pFileReader;
  SVersionRange      verRange;

  int32_t      step;
  STsdbReader* innerReader[2];
};

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter);
static int      buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                          STsdbReader* pReader);
static TSDBROW* getValidMemRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader);
static int32_t  doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, STsdbReader* pReader,
                                        SRowMerger* pMerger);
static int32_t  doMergeRowsInLastBlock(SLastBlockReader* pLastBlockReader, STableBlockScanInfo* pScanInfo, int64_t ts, SRowMerger* pMerger);
static int32_t  doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, int64_t ts, SArray* pDelList, SRowMerger* pMerger,
                                 STsdbReader* pReader);
static int32_t  doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, STSRow* pTSRow, uint64_t uid);
static int32_t  doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                     int32_t rowIndex);
static void     setComposedBlockFlag(STsdbReader* pReader, bool composed);
static bool     hasBeenDropped(const SArray* pDelList, int32_t* index, TSDBKEY* pKey, int32_t order);

static void doMergeMemTableMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList, STSRow** pTSRow,
                             STsdbReader* pReader, bool* freeTSRow);
static void doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader,
                               STSRow** pTSRow);
static int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, int64_t key, STsdbReader* pReader);

static int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STbData* pMemTbData,
                                      STbData* piMemTbData);
static STsdb*  getTsdbByRetentions(SVnode* pVnode, TSKEY winSKey, SRetention* retentions, const char* idstr,
                                   int8_t* pLevel);
static SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level);
static int64_t getCurrentKeyInLastBlock(SLastBlockReader* pLastBlockReader);
static bool    hasDataInLastBlock(SLastBlockReader* pLastBlockReader);
static int32_t doBuildDataBlock(STsdbReader* pReader);

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
  // todo use simple hash instead, optimize the memory consumption
  SHashObj* pTableMap =
      taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pTableMap == NULL) {
    return NULL;
  }

  for (int32_t j = 0; j < numOfTables; ++j) {
    STableBlockScanInfo info = {.lastKey = 0, .uid = idList[j].uid, .indexInBlockL = INITIAL_ROW_INDEX_VAL};
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

  tsdbDebug("%p create %d tables scan-info, size:%.2f Kb, %s", pTsdbReader, numOfTables,
            (sizeof(STableBlockScanInfo) * numOfTables) / 1024.0, pTsdbReader->idStr);

  return pTableMap;
}

static void resetDataBlockScanInfo(SHashObj* pTableMap) {
  STableBlockScanInfo* p = NULL;

  while ((p = taosHashIterate(pTableMap, p)) != NULL) {
    p->iterInit = false;
    p->iiter.hasVal = false;
    if (p->iter.iter != NULL) {
      p->iter.iter = tsdbTbDataIterDestroy(p->iter.iter);
    }

    p->fileDelIndex = -1;
    p->delSkyline   = taosArrayDestroy(p->delSkyline);
    p->lastBlockDelIndex = INITIAL_ROW_INDEX_VAL;
  }
}

static void destroyBlockScanInfo(SHashObj* pTableMap) {
  STableBlockScanInfo* p = NULL;

  while ((p = taosHashIterate(pTableMap, p)) != NULL) {
    p->iterInit = false;
    p->iiter.hasVal = false;

    if (p->iter.iter != NULL) {
      p->iter.iter = tsdbTbDataIterDestroy(p->iter.iter);
    }

    if (p->iiter.iter != NULL) {
      p->iiter.iter = tsdbTbDataIterDestroy(p->iiter.iter);
    }

    p->delSkyline = taosArrayDestroy(p->delSkyline);
    p->pBlockList = taosArrayDestroy(p->pBlockList);
    tMapDataClear(&p->mapData);
  }

  taosHashCleanup(pTableMap);
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
static int32_t initFilesetIterator(SFilesetIter* pIter, SArray* aDFileSet, STsdbReader* pReader/*int32_t order, const char* idstr*/) {
  size_t numOfFileset = taosArrayGetSize(aDFileSet);

  pIter->index = ASCENDING_TRAVERSE(pReader->order) ? -1 : numOfFileset;
  pIter->order = pReader->order;
  pIter->pFileList = aDFileSet;
  pIter->numOfFiles = numOfFileset;

  if (pIter->pLastBlockReader == NULL) {
    pIter->pLastBlockReader = taosMemoryCalloc(1, sizeof(struct SLastBlockReader));
    if (pIter->pLastBlockReader == NULL) {
      int32_t code = TSDB_CODE_OUT_OF_MEMORY;
      tsdbError("failed to prepare the last block iterator, code:%d %s", tstrerror(code), pReader->idStr);
      return code;
    }

    SLastBlockReader* pLReader = pIter->pLastBlockReader;
    pLReader->pBlockL = taosArrayInit(4, sizeof(SBlockL));
    pLReader->order   = pReader->order;
    pLReader->window  = pReader->window;
    pLReader->verRange = pReader->verRange;
    pLReader->currentBlockIndex = -1;

    int32_t code = tBlockDataCreate(&pLReader->lastBlockData);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  tsdbDebug("init fileset iterator, total files:%d %s", pIter->numOfFiles, pReader->idStr);
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
    if (pReader->pFileReader != NULL) {
      tsdbDataFReaderClose(&pReader->pFileReader);
    }

    pReader->status.pCurrentFileset = (SDFileSet*)taosArrayGet(pIter->pFileList, pIter->index);

    int32_t code = tsdbDataFReaderOpen(&pReader->pFileReader, pReader->pTsdb, pReader->status.pCurrentFileset);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    pReader->cost.headFileLoad += 1;

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
      if ((asc && pIter->index >= pIter->numOfFiles) || ((!asc) && pIter->index < 0)) {
        return false;
      }
      continue;
    }

    tsdbDebug("%p file found fid:%d for qrange:%" PRId64 "-%" PRId64 ", %s", pReader, fid, pReader->window.skey,
              pReader->window.ekey, pReader->idStr);
    return true;
  }

_err:
  return false;
}

static void resetDataBlockIterator(SDataBlockIter* pIter, int32_t order) {
  pIter->order = order;
  pIter->index = -1;
  pIter->numOfBlocks = 0;
  if (pIter->blockList == NULL) {
    pIter->blockList = taosArrayInit(4, sizeof(SFileDataBlockInfo));
  } else {
    taosArrayClear(pIter->blockList);
  }
}

static void cleanupDataBlockIterator(SDataBlockIter* pIter) { taosArrayDestroy(pIter->blockList); }

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

static int32_t tsdbReaderCreate(SVnode* pVnode, SQueryTableDataCond* pCond, STsdbReader** ppReader, int32_t capacity,
                                const char* idstr) {
  int32_t      code = 0;
  int8_t       level = 0;
  STsdbReader* pReader = (STsdbReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  if (VND_IS_TSMA(pVnode)) {
    tsdbDebug("vgId:%d, tsma is selected to query", TD_VID(pVnode));
  }

  initReaderStatus(&pReader->status);

  pReader->pTsdb = getTsdbByRetentions(pVnode, pCond->twindows.skey, pVnode->config.tsdbCfg.retentions, idstr, &level);
  pReader->suid = pCond->suid;
  pReader->order = pCond->order;
  pReader->capacity = 4096;
  pReader->idStr = (idstr != NULL) ? strdup(idstr) : NULL;
  pReader->verRange = getQueryVerRange(pVnode, pCond, level);
  pReader->type = pCond->type;
  pReader->window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows);

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

  code = tBlockDataCreate(&pReader->status.fileBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _end;
  }

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

static int32_t doLoadBlockIndex(STsdbReader* pReader, SDataFReader* pFileReader, SArray* pIndexList) {
  SArray* aBlockIdx = taosArrayInit(8, sizeof(SBlockIdx));

  int64_t st = taosGetTimestampUs();
  int32_t code = tsdbReadBlockIdx(pFileReader, aBlockIdx);
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }

  size_t num = taosArrayGetSize(aBlockIdx);
  if (num == 0) {
    taosArrayDestroy(aBlockIdx);
    return TSDB_CODE_SUCCESS;
  }

  int64_t et1 = taosGetTimestampUs();

  SBlockIdx* pBlockIdx = NULL;
  for (int32_t i = 0; i < num; ++i) {
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

    STableBlockScanInfo* pScanInfo = p;
    if (pScanInfo->pBlockList == NULL) {
      pScanInfo->pBlockList = taosArrayInit(4, sizeof(int32_t));
    }

    taosArrayPush(pIndexList, pBlockIdx);
  }

  int64_t et2 = taosGetTimestampUs();
  tsdbDebug("load block index for %d tables completed, elapsed time:%.2f ms, set blockIdx:%.2f ms, size:%.2f Kb %s",
            (int32_t)num, (et1 - st) / 1000.0, (et2 - et1) / 1000.0, num * sizeof(SBlockIdx) / 1024.0, pReader->idStr);

  pReader->cost.headFileLoadTime += (et1 - st) / 1000.0;

_end:
  taosArrayDestroy(aBlockIdx);
  return code;
}

static void cleanupTableScanInfo(SHashObj* pTableMap) {
  STableBlockScanInfo* px = NULL;
  while (1) {
    px = taosHashIterate(pTableMap, px);
    if (px == NULL) {
      break;
    }

    // reset the index in last block when handing a new file
    px->indexInBlockL = INITIAL_ROW_INDEX_VAL;
    tMapDataClear(&px->mapData);
    taosArrayClear(px->pBlockList);
  }
}

static int32_t doLoadFileBlock(STsdbReader* pReader, SArray* pIndexList, SArray* pLastBlockIndex,
                               SBlockNumber * pBlockNum, SArray* pQualifiedLastBlock) {
  int32_t numOfQTable = 0;
  size_t  sizeInDisk = 0;
  size_t  numOfTables = taosArrayGetSize(pIndexList);

  int64_t st = taosGetTimestampUs();
  cleanupTableScanInfo(pReader->status.pTableMap);

  for (int32_t i = 0; i < numOfTables; ++i) {
    SBlockIdx* pBlockIdx = taosArrayGet(pIndexList, i);

    STableBlockScanInfo* pScanInfo = taosHashGet(pReader->status.pTableMap, &pBlockIdx->uid, sizeof(int64_t));

    tMapDataReset(&pScanInfo->mapData);
    tsdbReadBlock(pReader->pFileReader, pBlockIdx, &pScanInfo->mapData);

    sizeInDisk += pScanInfo->mapData.nData;
    for (int32_t j = 0; j < pScanInfo->mapData.nItem; ++j) {
      SBlock block = {0};
      tMapDataGetItemByIdx(&pScanInfo->mapData, j, &block, tGetBlock);

      // 1. time range check
      if (block.minKey.ts > pReader->window.ekey || block.maxKey.ts < pReader->window.skey) {
        continue;
      }

      // 2. version range check
      if (block.minVer > pReader->verRange.maxVer || block.maxVer < pReader->verRange.minVer) {
        continue;
      }

      void* p = taosArrayPush(pScanInfo->pBlockList, &j);
      if (p == NULL) {
        tMapDataClear(&pScanInfo->mapData);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pBlockNum->numOfBlocks += 1;
    }

    if (pScanInfo->pBlockList != NULL && taosArrayGetSize(pScanInfo->pBlockList) > 0) {
      numOfQTable += 1;
    }
  }

  size_t numOfLast = taosArrayGetSize(pLastBlockIndex);
  for(int32_t i = 0; i < numOfLast; ++i) {
    SBlockL* pLastBlock = taosArrayGet(pLastBlockIndex, i);
    if (pLastBlock->suid != pReader->suid) {
      continue;
    }

    {
      // 1. time range check
      if (pLastBlock->minKey > pReader->window.ekey || pLastBlock->maxKey < pReader->window.skey) {
        continue;
      }

      // 2. version range check
      if (pLastBlock->minVer > pReader->verRange.maxVer || pLastBlock->maxVer < pReader->verRange.minVer) {
        continue;
      }

      pBlockNum->numOfLastBlocks += 1;
      taosArrayPush(pQualifiedLastBlock, pLastBlock);
    }
  }

  int32_t total = pBlockNum->numOfLastBlocks + pBlockNum->numOfBlocks;

  double el = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug(
      "load block of %d tables completed, blocks:%d in %d tables, lastBlock:%d, block-info-size:%.2f Kb, elapsed "
      "time:%.2f ms %s",
      numOfTables, pBlockNum->numOfBlocks, numOfQTable, pBlockNum->numOfLastBlocks, sizeInDisk / 1000.0, el,
      pReader->idStr);

  pReader->cost.numOfBlocks += total;
  pReader->cost.headFileLoadTime += el;

  return TSDB_CODE_SUCCESS;
}

static void setBlockAllDumped(SFileBlockDumpInfo* pDumpInfo, int64_t maxKey, int32_t order) {
  int32_t step = ASCENDING_TRAVERSE(order) ? 1 : -1;
  pDumpInfo->allDumped = true;
  pDumpInfo->lastKey = maxKey + step;
}

static void doCopyColVal(SColumnInfoData* pColInfoData, int32_t rowIndex, int32_t colIndex, SColVal* pColVal,
                         SBlockLoadSuppInfo* pSup) {
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    if (pColVal->isNull || pColVal->isNone) {
      colDataAppendNULL(pColInfoData, rowIndex);
    } else {
      varDataSetLen(pSup->buildBuf[colIndex], pColVal->value.nData);
      ASSERT(pColVal->value.nData <= pColInfoData->info.bytes);
      memcpy(varDataVal(pSup->buildBuf[colIndex]), pColVal->value.pData, pColVal->value.nData);
      colDataAppend(pColInfoData, rowIndex, pSup->buildBuf[colIndex], false);
    }
  } else {
    colDataAppend(pColInfoData, rowIndex, (const char*)&pColVal->value, pColVal->isNull || pColVal->isNone);
  }
}

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter) {
  if (taosArrayGetSize(pBlockIter->blockList) == 0) {
    ASSERT(pBlockIter->numOfBlocks == taosArrayGetSize(pBlockIter->blockList));
    return NULL;
  }

  SFileDataBlockInfo* pBlockInfo = taosArrayGet(pBlockIter->blockList, pBlockIter->index);
  return pBlockInfo;
}

static SBlock* getCurrentBlock(SDataBlockIter* pBlockIter) { return &pBlockIter->block; }

static int32_t copyBlockDataToSDataBlock(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo) {
  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockIter* pBlockIter = &pStatus->blockIter;

  SBlockData*         pBlockData = &pStatus->fileBlockData;
  SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(pBlockIter);
  SBlock*             pBlock = getCurrentBlock(pBlockIter);
  SSDataBlock*        pResBlock = pReader->pResBlock;
  int32_t             numOfOutputCols = blockDataGetNumOfCols(pResBlock);

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  SColVal cv = {0};
  int64_t st = taosGetTimestampUs();
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

  int32_t colIndex = 0;
  int32_t num = taosArrayGetSize(pBlockData->aIdx);
  while (i < numOfOutputCols && colIndex < num) {
    rowIndex = 0;
    pColData = taosArrayGet(pResBlock->pDataBlock, i);

    SColData* pData = tBlockDataGetColDataByIdx(pBlockData, colIndex);
    if (pData->cid < pColData->info.colId) {
      colIndex += 1;
    } else if (pData->cid == pColData->info.colId) {
      for (int32_t j = pDumpInfo->rowIndex; j < endIndex && j >= 0; j += step) {
        tColDataGetValue(pData, j, &cv);
        doCopyColVal(pColData, rowIndex++, i, &cv, pSupInfo);
      }
      colIndex += 1;
      i += 1;
      ASSERT(rowIndex == remain);
    } else {  // the specified column does not exist in file block, fill with null data
      colDataAppendNNULL(pColData, 0, remain);
      i += 1;
    }
  }

  while (i < numOfOutputCols) {
    pColData = taosArrayGet(pResBlock->pDataBlock, i);
    colDataAppendNNULL(pColData, 0, remain);
    i += 1;
  }

  pResBlock->info.rows = remain;
  pDumpInfo->rowIndex += step * remain;

  setBlockAllDumped(pDumpInfo, pBlock->maxKey.ts, pReader->order);

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  pReader->cost.blockLoadTime += elapsedTime;

  int32_t unDumpedRows = asc ? pBlock->nRow - pDumpInfo->rowIndex : pDumpInfo->rowIndex + 1;
  tsdbDebug("%p copy file block to sdatablock, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, remain:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pFBlock->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, remain, unDumpedRows,
            pBlock->minVer, pBlock->maxVer, elapsedTime, pReader->idStr);

  return TSDB_CODE_SUCCESS;
}

static int32_t doLoadFileBlockData(STsdbReader* pReader, SDataBlockIter* pBlockIter, SBlockData* pBlockData) {
  int64_t st = taosGetTimestampUs();

  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  ASSERT(pBlockInfo != NULL);

  SBlock* pBlock = getCurrentBlock(pBlockIter);
  int32_t code = tsdbReadDataBlock(pReader->pFileReader, pBlock, pBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p error occurs in loading file block, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
              ", rows:%d, code:%s %s",
              pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, pBlock->nRow,
              tstrerror(code), pReader->idStr);
    return code;
  }

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;

  tsdbDebug("%p load file block into buffer, global index:%d, index in table block list:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, pBlock->nRow,
            pBlock->minVer, pBlock->maxVer, elapsedTime, pReader->idStr);

  pReader->cost.blockLoadTime += elapsedTime;
  pDumpInfo->allDumped = false;

  return TSDB_CODE_SUCCESS;
}

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

  return pLeftBlock->offset > pRightBlock->offset ? 1 : -1;
}

static int32_t doSetCurrentBlock(SDataBlockIter* pBlockIter) {
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  if (pBlockInfo != NULL) {
    STableBlockScanInfo* pScanInfo = taosHashGet(pBlockIter->pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
    int32_t* mapDataIndex = taosArrayGet(pScanInfo->pBlockList, pBlockInfo->tbBlockIdx);
    tMapDataGetItemByIdx(&pScanInfo->mapData, *mapDataIndex, &pBlockIter->block, tGetBlock);
  }

#if 0
  qDebug("check file block, table uid:%"PRIu64" index:%d offset:%"PRId64", ", pScanInfo->uid, *mapDataIndex, pBlockIter->block.aSubBlock[0].offset);
#endif

  return TSDB_CODE_SUCCESS;
}

static int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks) {
  bool asc = ASCENDING_TRAVERSE(pReader->order);

  pBlockIter->numOfBlocks = numOfBlocks;
  taosArrayClear(pBlockIter->blockList);
  pBlockIter->pTableMap = pReader->status.pTableMap;

  // access data blocks according to the offset of each block in asc/desc order.
  int32_t numOfTables = (int32_t)taosHashGetSize(pReader->status.pTableMap);

  int64_t st = taosGetTimestampUs();

  SBlockOrderSupporter sup = {0};
  int32_t              code = initBlockOrderSupporter(&sup, numOfTables);
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
    SBlock block = {0};
    for (int32_t k = 0; k < num; ++k) {
      SBlockOrderWrapper wrapper = {0};

      int32_t* mapDataIndex = taosArrayGet(pTableScanInfo->pBlockList, k);
      tMapDataGetItemByIdx(&pTableScanInfo->mapData, *mapDataIndex, &block, tGetBlock);

      wrapper.uid = pTableScanInfo->uid;
      wrapper.offset = block.aSubBlock[0].offset;

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

    int64_t et = taosGetTimestampUs();
    tsdbDebug("%p create blocks info struct completed for one table, %d blocks not sorted, elapsed time:%.2f ms %s",
              pReader, numOfBlocks, (et - st) / 1000.0, pReader->idStr);

    pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
    cleanupBlockOrderSupporter(&sup);
    doSetCurrentBlock(pBlockIter);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %s", pReader, cnt, sup.numOfTables,
            pReader->idStr);

  ASSERT(cnt <= numOfBlocks && sup.numOfTables <= numOfTables);

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

  int64_t et = taosGetTimestampUs();
  tsdbDebug("%p %d data blocks access order completed, elapsed time:%.2f ms %s", pReader, numOfBlocks, (et - st) / 1000.0,
            pReader->idStr);
  cleanupBlockOrderSupporter(&sup);
  taosMemoryFree(pTree);

  pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
  doSetCurrentBlock(pBlockIter);

  return TSDB_CODE_SUCCESS;
}

static bool blockIteratorNext(SDataBlockIter* pBlockIter) {
  bool asc = ASCENDING_TRAVERSE(pBlockIter->order);

  int32_t step = asc ? 1 : -1;
  if ((pBlockIter->index >= pBlockIter->numOfBlocks - 1 && asc) || (pBlockIter->index <= 0 && (!asc))) {
    return false;
  }

  pBlockIter->index += step;
  doSetCurrentBlock(pBlockIter);

  return true;
}

/**
 * This is an two rectangles overlap cases.
 */
static int32_t dataBlockPartiallyRequired(STimeWindow* pWindow, SVersionRange* pVerRange, SBlock* pBlock) {
  return (pWindow->ekey < pBlock->maxKey.ts && pWindow->ekey >= pBlock->minKey.ts) ||
         (pWindow->skey > pBlock->minKey.ts && pWindow->skey <= pBlock->maxKey.ts) ||
         (pVerRange->minVer > pBlock->minVer && pVerRange->minVer <= pBlock->maxVer) ||
         (pVerRange->maxVer < pBlock->maxVer && pVerRange->maxVer >= pBlock->minVer);
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

  SBlock*  pBlock = taosMemoryCalloc(1, sizeof(SBlock));
  int32_t* indexInMapdata = taosArrayGet(pTableBlockScanInfo->pBlockList, *nextIndex);

  tMapDataGetItemByIdx(&pTableBlockScanInfo->mapData, *indexInMapdata, pBlock, tGetBlock);
  return pBlock;
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

  doSetCurrentBlock(pBlockIter);
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
  return (key.ts >= pBlock->minKey.ts && key.ts <= pBlock->maxKey.ts) && (pBlock->maxVer >= pVerRange->minVer) &&
         (pBlock->minVer <= pVerRange->maxVer);
}

static bool doCheckforDatablockOverlap(STableBlockScanInfo* pBlockScanInfo, const SBlock* pBlock) {
  size_t num = taosArrayGetSize(pBlockScanInfo->delSkyline);

  for (int32_t i = pBlockScanInfo->fileDelIndex; i < num; i += 1) {
    TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, i);
    if (p->ts >= pBlock->minKey.ts && p->ts <= pBlock->maxKey.ts) {
      if (p->version >= pBlock->minVer) {
        return true;
      }
    } else if (p->ts < pBlock->minKey.ts) {  // p->ts < pBlock->minKey.ts
      if (p->version >= pBlock->minVer) {
        if (i < num - 1) {
          TSDBKEY* pnext = taosArrayGet(pBlockScanInfo->delSkyline, i + 1);
          if (i + 1 == num - 1) {  // pnext is the last point
            if (pnext->ts >= pBlock->minKey.ts) {
              return true;
            }
          } else {
            if (pnext->ts >= pBlock->minKey.ts && pnext->version >= pBlock->minVer) {
              return true;
            }
          }
        } else {  // it must be the last point
          ASSERT(p->version == 0);
        }
      }
    } else {  // (p->ts > pBlock->maxKey.ts) {
      return false;
    }
  }

  return false;
}

static bool overlapWithDelSkyline(STableBlockScanInfo* pBlockScanInfo, const SBlock* pBlock, int32_t order) {
  if (pBlockScanInfo->delSkyline == NULL) {
    return false;
  }

  // ts is not overlap
  TSDBKEY* pFirst = taosArrayGet(pBlockScanInfo->delSkyline, 0);
  TSDBKEY* pLast = taosArrayGetLast(pBlockScanInfo->delSkyline);
  if (pBlock->minKey.ts > pLast->ts || pBlock->maxKey.ts < pFirst->ts) {
    return false;
  }

  // version is not overlap
  if (ASCENDING_TRAVERSE(order)) {
    return doCheckforDatablockOverlap(pBlockScanInfo, pBlock);
  } else {
    int32_t index = pBlockScanInfo->fileDelIndex;
    while (1) {
      TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, index);
      if (p->ts > pBlock->minKey.ts && index > 0) {
        index -= 1;
      } else {  // find the first point that is smaller than the minKey.ts of dataBlock.
        break;
      }
    }

    return doCheckforDatablockOverlap(pBlockScanInfo, pBlock);
  }
}

// 1. the version of all rows should be less than the endVersion
// 2. current block should not overlap with next neighbor block
// 3. current timestamp should not be overlap with each other
// 4. output buffer should be large enough to hold all rows in current block
// 5. delete info should not overlap with current block data
static bool fileBlockShouldLoad(STsdbReader* pReader, SFileDataBlockInfo* pFBlock, SBlock* pBlock,
                                STableBlockScanInfo* pScanInfo, TSDBKEY key, SLastBlockReader* pLastBlockReader) {
  int32_t neighborIndex = 0;
  SBlock* pNeighbor = getNeighborBlockOfSameTable(pFBlock, pScanInfo, &neighborIndex, pReader->order);

  // overlap with neighbor
  bool overlapWithNeighbor = false;
  if (pNeighbor) {
    overlapWithNeighbor = overlapWithNeighborBlock(pBlock, pNeighbor, pReader->order);
    taosMemoryFree(pNeighbor);
  }

  // has duplicated ts of different version in this block
  bool hasDup = (pBlock->nSubBlock == 1) ? pBlock->hasDup : true;
  bool overlapWithDel = overlapWithDelSkyline(pScanInfo, pBlock, pReader->order);

  // todo here we need to each key in the last files to identify if it is really overlapped with last block
  bool overlapWithlastBlock = false;
  if (taosArrayGetSize(pLastBlockReader->pBlockL) > 0 && (pLastBlockReader->currentBlockIndex != -1)) {
    SBlockL *pBlockL = taosArrayGet(pLastBlockReader->pBlockL, pLastBlockReader->currentBlockIndex);
    overlapWithlastBlock = !(pBlock->maxKey.ts < pBlockL->minKey || pBlock->minKey.ts > pBlockL->maxKey);
  }

  bool moreThanOutputCapacity = pBlock->nRow > pReader->capacity;
  bool partiallyRequired = dataBlockPartiallyRequired(&pReader->window, &pReader->verRange, pBlock);
  bool overlapWithKey = keyOverlapFileBlock(key, pBlock, &pReader->verRange);

  bool loadDataBlock = (overlapWithNeighbor || hasDup || partiallyRequired || overlapWithKey ||
                        moreThanOutputCapacity || overlapWithDel || overlapWithlastBlock);

  // log the reason why load the datablock for profile
  if (loadDataBlock) {
    tsdbDebug("%p uid:%" PRIu64
              " need to load the datablock, overlapwithneighborblock:%d, hasDup:%d, partiallyRequired:%d, "
              "overlapWithKey:%d, greaterThanBuf:%d, overlapWithDel:%d, overlapWithlastBlock:%d, %s",
              pReader, pFBlock->uid, overlapWithNeighbor, hasDup, partiallyRequired, overlapWithKey,
              moreThanOutputCapacity, overlapWithDel, overlapWithlastBlock, pReader->idStr);
  }

  return loadDataBlock;
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

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("%p build data block from cache completed, elapsed time:%.2f ms, numOfRows:%d, brange:%" PRId64
            " - %" PRId64 " %s",
            pReader, elapsedTime, pBlock->info.rows, pBlock->info.window.skey, pBlock->info.window.ekey,
            pReader->idStr);

  pReader->cost.buildmemBlock += elapsedTime;
  return code;
}

static bool tryCopyDistinctRowFromFileBlock(STsdbReader* pReader, SBlockData* pBlockData, int64_t key,
                                            SFileBlockDumpInfo* pDumpInfo) {
  // opt version
  // 1. it is not a border point
  // 2. the direct next point is not an duplicated timestamp
  if ((pDumpInfo->rowIndex < pDumpInfo->totalRows - 1 && pReader->order == TSDB_ORDER_ASC) ||
      (pDumpInfo->rowIndex > 0 && pReader->order == TSDB_ORDER_DESC)) {
    int32_t step = pReader->order == TSDB_ORDER_ASC ? 1 : -1;

    int64_t nextKey = pBlockData->aTSKEY[pDumpInfo->rowIndex + step];
    if (nextKey != key) {  // merge is not needed
      doAppendRowFromFileBlock(pReader->pResBlock, pReader, pBlockData, pDumpInfo->rowIndex);
      pDumpInfo->rowIndex += step;
      return true;
    }
  }

  return false;
}

static FORCE_INLINE STSchema* doGetSchemaForTSRow(int32_t sversion, STsdbReader* pReader, uint64_t uid) {
  // always set the newest schema version in pReader->pSchema
  if (pReader->pSchema == NULL) {
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, uid, -1);
  }

  if (pReader->pSchema && sversion == pReader->pSchema->version) {
    return pReader->pSchema;
  }

  if (pReader->pMemSchema == NULL) {
    int32_t code =
        metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->suid, uid, sversion, &pReader->pMemSchema);
    return pReader->pMemSchema;
  }

  if (pReader->pMemSchema->version == sversion) {
    return pReader->pMemSchema;
  }

  taosMemoryFree(pReader->pMemSchema);
  int32_t code = metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->suid, uid, sversion, &pReader->pMemSchema);
  return pReader->pMemSchema;
}

static int32_t doMergeBufAndFileRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, TSDBROW* pRow,
                                     SIterInfo* pIter, int64_t key, SLastBlockReader* pLastBlockReader) {
  SRowMerger          merge = {0};
  STSRow*             pTSRow = NULL;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  int64_t tsLast = INT64_MIN;
  if ((pLastBlockReader->lastBlockData.nRow > 0) && hasDataInLastBlock(pLastBlockReader)) {
    tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
  }

  TSDBKEY  k = TSDBROW_KEY(pRow);
  TSDBROW  fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

  SBlockData* pLastBlockData = &pLastBlockReader->lastBlockData;

  int64_t minKey = 0;
  if (pReader->order == TSDB_ORDER_ASC) {
    minKey = INT64_MAX;   // chosen the minimum value
    if (minKey > tsLast && hasDataInLastBlock(pLastBlockReader)) {
      minKey = tsLast;
    }

    if (minKey > k.ts) {
      minKey = k.ts;
    }

    if (minKey > key && pBlockData->nRow > 0) {
      minKey = key;
    }
  } else {
    minKey = INT64_MIN;
    if (minKey < tsLast && hasDataInLastBlock(pLastBlockReader)) {
      minKey = tsLast;
    }

    if (minKey < k.ts) {
      minKey = k.ts;
    }

    if (minKey < key && pBlockData->nRow > 0) {
      minKey = key;
    }
  }

  bool init = false;

  // ASC: file block ---> last block -----> imem -----> mem
  //DESC: mem -----> imem -----> last block -----> file block
  if (pReader->order == TSDB_ORDER_ASC) {
    if (minKey == key) {
      init = true;
      tRowMergerInit(&merge, &fRow, pReader->pSchema);
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tsdbRowFromBlockData(pLastBlockData, *pLastBlockReader->rowIndex);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        tRowMergerInit(&merge, &fRow1, pReader->pSchema);
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge);
    }

    if (minKey == k.ts) {
      if (init) {
        tRowMerge(&merge, pRow);
      } else {
        init = true;
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
        tRowMergerInit(&merge, pRow, pSchema);
      }
      doMergeRowsInBuf(pIter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    }
  } else {
    if (minKey == k.ts) {
      init = true;
      STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
      tRowMergerInit(&merge, pRow, pSchema);
      doMergeRowsInBuf(pIter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tsdbRowFromBlockData(pLastBlockData, *pLastBlockReader->rowIndex);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        tRowMergerInit(&merge, &fRow1, pReader->pSchema);
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge);
    }

    if (minKey == key) {
      if (init) {
        tRowMerge(&merge, &fRow);
      } else {
        init = true;
        tRowMergerInit(&merge, &fRow, pReader->pSchema);
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }
  }

  tRowMergerGetRow(&merge, &pTSRow);
  doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo->uid);

  taosMemoryFree(pTSRow);
  tRowMergerClear(&merge);
  return TSDB_CODE_SUCCESS;
}

static int32_t doMergeFileBlockAndLastBlock(SLastBlockReader* pLastBlockReader, STsdbReader* pReader,
                                            STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData,
                                            bool mergeBlockData) {
  SBlockData* pLastBlockData = &pLastBlockReader->lastBlockData;
  int64_t     tsLastBlock = getCurrentKeyInLastBlock(pLastBlockReader);

  STSRow*    pTSRow = NULL;
  SRowMerger merge = {0};

  TSDBROW fRow = tsdbRowFromBlockData(pLastBlockData, *pLastBlockReader->rowIndex);

  tRowMergerInit(&merge, &fRow, pReader->pSchema);
  doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLastBlock, &merge);

  // merge with block data if ts == key
  if (mergeBlockData) {
    doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
  }

  tRowMergerGetRow(&merge, &pTSRow);
  doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo->uid);

  taosMemoryFree(pTSRow);
  tRowMergerClear(&merge);
  return TSDB_CODE_SUCCESS;
}

static int32_t mergeFileBlockAndLastBlock(STsdbReader* pReader, SLastBlockReader* pLastBlockReader, int64_t key,
                                          STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  if (pBlockData->nRow > 0) {
    // no last block available, only data block exists
    if (pLastBlockReader->lastBlockData.nRow == 0 || (!hasDataInLastBlock(pLastBlockReader))) {
      return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
    }

    // row in last file block
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
    int64_t ts = getCurrentKeyInLastBlock(pLastBlockReader);
    ASSERT(ts >= key);

    if (ASCENDING_TRAVERSE(pReader->order)) {
      if (key < ts) { // imem, mem are all empty, file blocks (data blocks and last block) exist
        return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
      } else if (key == ts) {
        STSRow*    pTSRow = NULL;
        SRowMerger merge = {0};

        tRowMergerInit(&merge, &fRow, pReader->pSchema);
        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
        doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, ts, &merge);

        tRowMergerGetRow(&merge, &pTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo->uid);

        taosMemoryFree(pTSRow);
        tRowMergerClear(&merge);
        return TSDB_CODE_SUCCESS;
      } else {
        ASSERT(0);
        return TSDB_CODE_SUCCESS;
      }
    } else {  // desc order
      return doMergeFileBlockAndLastBlock(pLastBlockReader, pReader, pBlockScanInfo, pBlockData, true);
    }
  } else {  // only last block exists
    return doMergeFileBlockAndLastBlock(pLastBlockReader, pReader, pBlockScanInfo, NULL, false);
  }
}

static int32_t doMergeMultiLevelRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData,
                                     SLastBlockReader* pLastBlockReader) {
  SRowMerger merge = {0};
  STSRow*    pTSRow = NULL;

  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SArray*             pDelList = pBlockScanInfo->delSkyline;

  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pDelList, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pDelList, pReader);
  ASSERT(pRow != NULL && piRow != NULL);

  SBlockData* pLastBlockData = &pLastBlockReader->lastBlockData;
  int64_t tsLast = INT64_MIN;
  if (hasDataInLastBlock(pLastBlockReader)) {
    tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
  }

  int64_t key = pBlockData->aTSKEY[pDumpInfo->rowIndex];

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  int64_t minKey = 0;
  if (ASCENDING_TRAVERSE(pReader->order)) {
    minKey = INT64_MAX;  // let's find the minimum
    if (minKey > k.ts) {
      minKey = k.ts;
    }

    if (minKey > ik.ts) {
      minKey = ik.ts;
    }

    if (minKey > key && pBlockData->nRow > 0) {
      minKey = key;
    }

    if (minKey > tsLast && hasDataInLastBlock(pLastBlockReader)) {
      minKey = tsLast;
    }
  } else {
    minKey = INT64_MIN; // let find the maximum ts value
    if (minKey < k.ts) {
      minKey = k.ts;
    }

    if (minKey < ik.ts) {
      minKey = ik.ts;
    }

    if (minKey < key && pBlockData->nRow > 0) {
      minKey = key;
    }

    if (minKey < tsLast && hasDataInLastBlock(pLastBlockReader)) {
      minKey = tsLast;
    }
  }

  bool init = false;

  // ASC: file block -----> last block -----> imem -----> mem
  // DESC: mem -----> imem -----> last block -----> file block
  if (ASCENDING_TRAVERSE(pReader->order)) {
    if (minKey == key) {
      init = true;
      TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
      tRowMergerInit(&merge, &fRow, pReader->pSchema);
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tsdbRowFromBlockData(pLastBlockData, *pLastBlockReader->rowIndex);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        tRowMergerInit(&merge, &fRow1, pReader->pSchema);
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge);
    }

    if (minKey == ik.ts) {
      if (init) {
        tRowMerge(&merge, piRow);
      } else {
        init = true;
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
        tRowMergerInit(&merge, piRow, pSchema);
      }
      doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    }

    if (minKey == k.ts) {
      if (init) {
        tRowMerge(&merge, pRow);
      } else {
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
        tRowMergerInit(&merge, pRow, pSchema);
      }
      doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    }
  } else {
    if (minKey == k.ts) {
      init = true;
      STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
      tRowMergerInit(&merge, pRow, pSchema);
      doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    }

    if (minKey == ik.ts) {
      if (init) {
        tRowMerge(&merge, piRow);
      } else {
        init = true;
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
        tRowMergerInit(&merge, piRow, pSchema);
      }
      doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tsdbRowFromBlockData(pLastBlockData, *pLastBlockReader->rowIndex);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        tRowMergerInit(&merge, &fRow1, pReader->pSchema);
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge);
    }

    if (minKey == key) {
      TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
      if (!init) {
        tRowMergerInit(&merge, &fRow, pReader->pSchema);
      } else {
        tRowMerge(&merge, &fRow);
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }
  }

  tRowMergerGetRow(&merge, &pTSRow);
  doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo->uid);

  taosMemoryFree(pTSRow);
  tRowMergerClear(&merge);
  return TSDB_CODE_SUCCESS;
}

#if 0
static int32_t doMergeThreeLevelRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData) {
  SRowMerger merge = {0};
  STSRow*    pTSRow = NULL;

  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SArray*             pDelList = pBlockScanInfo->delSkyline;

  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pDelList, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pDelList, pReader);
  ASSERT(pRow != NULL && piRow != NULL);

  int64_t key = pBlockData->aTSKEY[pDumpInfo->rowIndex];
  bool    freeTSRow = false;

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
        doMergeRowsInBuf(&pBlockScanInfo->iiter, uid, key, pBlockScanInfo->delSkyline, &merge, pReader);
      }

      if (k.ts == key) {
        tRowMerge(&merge, pRow);
        doMergeRowsInBuf(&pBlockScanInfo->iter, uid, key, pBlockScanInfo->delSkyline, &merge, pReader);
      }

      tRowMergerGetRow(&merge, &pTSRow);
      doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
      return TSDB_CODE_SUCCESS;
    } else {  // key > ik.ts || key > k.ts
      ASSERT(key != ik.ts);

      // [3] ik.ts < key <= k.ts
      // [4] ik.ts < k.ts <= key
      if (ik.ts < k.ts) {
        doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, &pTSRow, pReader, &freeTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
        if (freeTSRow) {
          taosMemoryFree(pTSRow);
        }
        return TSDB_CODE_SUCCESS;
      }

      // [5] k.ts < key   <= ik.ts
      // [6] k.ts < ik.ts <= key
      if (k.ts < ik.ts) {
        doMergeMemTableMultiRows(pRow, uid, &pBlockScanInfo->iter, pDelList, &pTSRow, pReader, &freeTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
        if (freeTSRow) {
          taosMemoryFree(pTSRow);
        }
        return TSDB_CODE_SUCCESS;
      }

      // [7] k.ts == ik.ts < key
      if (k.ts == ik.ts) {
        ASSERT(key > ik.ts && key > k.ts);

        doMergeMemIMemRows(pRow, piRow, pBlockScanInfo, pReader, &pTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
        taosMemoryFree(pTSRow);
        return TSDB_CODE_SUCCESS;
      }
    }
  } else {  // descending order scan
    // [1/2] k.ts >= ik.ts && k.ts >= key
    if (k.ts >= ik.ts && k.ts >= key) {
      STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);

      tRowMergerInit(&merge, pRow, pSchema);
      doMergeRowsInBuf(&pBlockScanInfo->iter, uid, key, pBlockScanInfo->delSkyline, &merge, pReader);

      if (ik.ts == k.ts) {
        tRowMerge(&merge, piRow);
        doMergeRowsInBuf(&pBlockScanInfo->iiter, uid, key, pBlockScanInfo->delSkyline, &merge, pReader);
      }

      if (k.ts == key) {
        TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
        tRowMerge(&merge, &fRow);
        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
      }

      tRowMergerGetRow(&merge, &pTSRow);
      doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
      return TSDB_CODE_SUCCESS;
    } else {
      ASSERT(ik.ts != k.ts);  // this case has been included in the previous if branch

      // [3] ik.ts > k.ts >= Key
      // [4] ik.ts > key >= k.ts
      if (ik.ts > key) {
        doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, &pTSRow, pReader, &freeTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
        if (freeTSRow) {
          taosMemoryFree(pTSRow);
        }
        return TSDB_CODE_SUCCESS;
      }

      // [5] key > ik.ts > k.ts
      // [6] key > k.ts > ik.ts
      if (key > ik.ts) {
        TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
        tRowMergerInit(&merge, &fRow, pReader->pSchema);

        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
        tRowMergerGetRow(&merge, &pTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);
        taosMemoryFree(pTSRow);
        return TSDB_CODE_SUCCESS;
      }

      //[7] key = ik.ts > k.ts
      if (key == ik.ts) {
        doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, &pTSRow, pReader, &freeTSRow);

        TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
        tRowMerge(&merge, &fRow);
        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
        tRowMergerGetRow(&merge, &pTSRow);
        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, uid);

        taosMemoryFree(pTSRow);
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  ASSERT(0);
  return -1;
}
#endif

static bool isValidFileBlockRow(SBlockData* pBlockData, SFileBlockDumpInfo* pDumpInfo,
                                STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader) {
  // it is an multi-table data block
  if (pBlockData->aUid != NULL) {
    uint64_t uid = pBlockData->aUid[pDumpInfo->rowIndex];
    if (uid != pBlockScanInfo->uid) {  // move to next row
      return false;
    }
  }

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

static void initLastBlockReader(SLastBlockReader* pLastBlockReader, uint64_t uid, int16_t* startPos) {
  pLastBlockReader->uid = uid;
  pLastBlockReader->rowIndex = startPos;

  if (*startPos == -1) {
    if (ASCENDING_TRAVERSE(pLastBlockReader->order)) {
      // do nothing
    } else {
      *startPos = pLastBlockReader->lastBlockData.nRow;
    }
  }
}

static void setAllRowsChecked(SLastBlockReader *pLastBlockReader) {
  *pLastBlockReader->rowIndex = ALL_ROWS_CHECKED_INDEX;
}

static bool nextRowInLastBlock(SLastBlockReader *pLastBlockReader, STableBlockScanInfo* pBlockScanInfo) {
  bool asc = ASCENDING_TRAVERSE(pLastBlockReader->order);
  int32_t step = (asc) ? 1 : -1;
  if (*pLastBlockReader->rowIndex == ALL_ROWS_CHECKED_INDEX) {
    return false;
  }

  *(pLastBlockReader->rowIndex) += step;

  SBlockData* pBlockData = &pLastBlockReader->lastBlockData;
  for(int32_t i = *(pLastBlockReader->rowIndex); i < pBlockData->nRow && i >= 0; i += step) {
    if (pBlockData->aUid != NULL) {
      if (asc) {
        if (pBlockData->aUid[i] < pLastBlockReader->uid) {
          continue;
        } else if (pBlockData->aUid[i] > pLastBlockReader->uid) {
          break;
        }
      } else {
        if (pBlockData->aUid[i] > pLastBlockReader->uid) {
          continue;
        } else if (pBlockData->aUid[i] < pLastBlockReader->uid) {
          break;
        }
      }
    }

    int64_t ts = pBlockData->aTSKEY[i];
    if (ts < pLastBlockReader->window.skey) {
      continue;
    }

    int64_t ver = pBlockData->aVersion[i];
    if (ver < pLastBlockReader->verRange.minVer) {
      continue;
    }

    // no data any more, todo opt handle desc case
    if (ts > pLastBlockReader->window.ekey) {
      continue;
    }

    // todo opt handle desc case
    if (ver > pLastBlockReader->verRange.maxVer) {
      continue;
    }

    TSDBKEY k = {.ts = ts, .version = ver};
    if (hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->lastBlockDelIndex, &k, pLastBlockReader->order)) {
      continue;
    }

    *(pLastBlockReader->rowIndex) = i;
    return true;
  }

  // set all data is consumed in last block
  setAllRowsChecked(pLastBlockReader);
  return false;
}

static int64_t getCurrentKeyInLastBlock(SLastBlockReader* pLastBlockReader) {
  SBlockData* pBlockData = &pLastBlockReader->lastBlockData;
  return pBlockData->aTSKEY[*pLastBlockReader->rowIndex];
}

static bool hasDataInLastBlock(SLastBlockReader* pLastBlockReader) {
  if (*pLastBlockReader->rowIndex == ALL_ROWS_CHECKED_INDEX) {
    return false;
  }

  ASSERT(pLastBlockReader->lastBlockData.nRow > 0);
  return true;
}

int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, int64_t key, STsdbReader* pReader) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

  if (tryCopyDistinctRowFromFileBlock(pReader, pBlockData, key, pDumpInfo)) {
    return TSDB_CODE_SUCCESS;
  } else {
    STSRow*    pTSRow = NULL;
    SRowMerger merge = {0};

    tRowMergerInit(&merge, &fRow, pReader->pSchema);
    doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    tRowMergerGetRow(&merge, &pTSRow);
    doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo->uid);

    taosMemoryFree(pTSRow);
    tRowMergerClear(&merge);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t buildComposedDataBlockImpl(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo,
                                          SBlockData* pBlockData, SLastBlockReader* pLastBlockReader) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  int64_t key = (pBlockData->nRow > 0)? pBlockData->aTSKEY[pDumpInfo->rowIndex]:INT64_MIN;
  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);

  if (pBlockScanInfo->iter.hasVal && pBlockScanInfo->iiter.hasVal) {
    return doMergeMultiLevelRows(pReader, pBlockScanInfo, pBlockData, pLastBlockReader);
  } else {
    // imem + file + last block
    if (pBlockScanInfo->iiter.hasVal) {
      return doMergeBufAndFileRows(pReader, pBlockScanInfo, piRow, &pBlockScanInfo->iiter, key, pLastBlockReader);
    }

    // mem + file + last block
    if (pBlockScanInfo->iter.hasVal) {
      return doMergeBufAndFileRows(pReader, pBlockScanInfo, pRow, &pBlockScanInfo->iter, key, pLastBlockReader);
    }

    // files data blocks + last block
    return mergeFileBlockAndLastBlock(pReader, pLastBlockReader, key, pBlockScanInfo, pBlockData);
  }
}

static int32_t buildComposedDataBlock(STsdbReader* pReader) {
  SSDataBlock* pResBlock = pReader->pResBlock;

  SFileDataBlockInfo*  pBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);

  STableBlockScanInfo* pBlockScanInfo = NULL;
  if (pBlockInfo != NULL) {
    pBlockScanInfo = taosHashGet(pReader->status.pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
  } else {
    pBlockScanInfo = pReader->status.pTableIter;
  }

  SLastBlockReader*   pLastBlockReader = pReader->status.fileIter.pLastBlockReader;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  int32_t             step = ASCENDING_TRAVERSE(pReader->order) ? 1 : -1;

  int64_t st = taosGetTimestampUs();

  while (1) {
    // todo check the validate of row in file block
    bool hasBlockData = false;
    {
      while (pBlockData->nRow > 0) {  // find the first qualified row in data block
        if (isValidFileBlockRow(pBlockData, pDumpInfo, pBlockScanInfo, pReader)) {
          hasBlockData = true;
          break;
        }

        pDumpInfo->rowIndex += step;

        SBlock* pBlock = getCurrentBlock(&pReader->status.blockIter);
        if (pDumpInfo->rowIndex >= pBlock->nRow || pDumpInfo->rowIndex < 0) {
          setBlockAllDumped(pDumpInfo, pBlock->maxKey.ts, pReader->order);
          break;
        }
      }
    }

    bool hasBlockLData = hasDataInLastBlock(pLastBlockReader);

    // no data in last block and block, no need to proceed.
    if ((hasBlockData == false) && (hasBlockLData == false)) {
      break;
    }

    buildComposedDataBlockImpl(pReader, pBlockScanInfo, pBlockData, pLastBlockReader);

    // currently loaded file data block is consumed
    if ((pBlockData->nRow > 0) && (pDumpInfo->rowIndex >= pBlockData->nRow || pDumpInfo->rowIndex < 0)) {
      SBlock* pBlock = getCurrentBlock(&pReader->status.blockIter);
      setBlockAllDumped(pDumpInfo, pBlock->maxKey.ts, pReader->order);
      break;
    }

    if (pResBlock->info.rows >= pReader->capacity) {
      break;
    }
  }

  pResBlock->info.uid = pBlockScanInfo->uid;
  blockDataUpdateTsWindow(pResBlock, 0);

  setComposedBlockFlag(pReader, true);
  int64_t et = taosGetTimestampUs();

  if (pResBlock->info.rows > 0) {
    tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64
              " rows:%d, elapsed time:%.2f ms %s",
              pReader, pBlockScanInfo->uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
              pResBlock->info.rows, (et - st) / 1000.0, pReader->idStr);
  }

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
  if (pReader->pReadSnap->pMem != NULL) {
    d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->suid, pBlockScanInfo->uid);
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
  if (pReader->pReadSnap->pIMem != NULL) {
    di = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pIMem, pReader->suid, pBlockScanInfo->uid);
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

  SDelFile* pDelFile = pReader->pReadSnap->fs.pDelFile;
  if (pDelFile) {
    SDelFReader* pDelFReader = NULL;
    code = tsdbDelFReaderOpen(&pDelFReader, pDelFile, pTsdb);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    SArray* aDelIdx = taosArrayInit(4, sizeof(SDelIdx));
    if (aDelIdx == NULL) {
      tsdbDelFReaderClose(&pDelFReader);
      goto _err;
    }

    code = tsdbReadDelIdx(pDelFReader, aDelIdx);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(aDelIdx);
      tsdbDelFReaderClose(&pDelFReader);
      goto _err;
    }

    SDelIdx  idx = {.suid = pReader->suid, .uid = pBlockScanInfo->uid};
    SDelIdx* pIdx = taosArraySearch(aDelIdx, &idx, tCmprDelIdx, TD_EQ);

    if (pIdx != NULL) {
      code = tsdbReadDelData(pDelFReader, pIdx, pDelData);
    }

    taosArrayDestroy(aDelIdx);
    tsdbDelFReaderClose(&pDelFReader);

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
  pBlockScanInfo->lastBlockDelIndex = pBlockScanInfo->iter.index;
  return code;

_err:
  taosArrayDestroy(pDelData);
  return code;
}

static TSDBKEY getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  TSDBKEY key = {.ts = TSKEY_INITIAL_VAL};

  initMemDataIterator(pScanInfo, pReader);
  TSDBROW* pRow = getValidMemRow(&pScanInfo->iter, pScanInfo->delSkyline, pReader);
  if (pRow != NULL) {
    key = TSDBROW_KEY(pRow);
  }

  pRow = getValidMemRow(&pScanInfo->iiter, pScanInfo->delSkyline, pReader);
  if (pRow != NULL) {
    TSDBKEY k = TSDBROW_KEY(pRow);
    if (key.ts > k.ts) {
      key = k;
    }
  }

  return key;
}

static int32_t moveToNextFile(STsdbReader* pReader, SBlockNumber* pBlockNum) {
  SReaderStatus* pStatus = &pReader->status;
  pBlockNum->numOfBlocks = 0;
  pBlockNum->numOfLastBlocks = 0;

  size_t  numOfTables = taosHashGetSize(pReader->status.pTableMap);
  SArray* pIndexList = taosArrayInit(numOfTables, sizeof(SBlockIdx));
  SArray* pLastBlocks = pStatus->fileIter.pLastBlockReader->pBlockL;
  taosArrayClear(pLastBlocks);

  while (1) {
    bool hasNext = filesetIteratorNext(&pStatus->fileIter, pReader);
    if (!hasNext) {  // no data files on disk
      break;
    }

    taosArrayClear(pIndexList);
    int32_t code = doLoadBlockIndex(pReader, pReader->pFileReader, pIndexList);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(pIndexList);
      return code;
    }

    code = tsdbReadBlockL(pReader->pFileReader, pLastBlocks);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(pIndexList);
      return code;
    }

    if (taosArrayGetSize(pIndexList) > 0 || taosArrayGetSize(pLastBlocks) > 0) {
      SArray* pQLastBlock = taosArrayInit(4, sizeof(SBlockL));

      code = doLoadFileBlock(pReader, pIndexList, pLastBlocks, pBlockNum, pQLastBlock);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pIndexList);
        taosArrayDestroy(pQLastBlock);
        return code;
      }

      if (pBlockNum->numOfBlocks + pBlockNum->numOfLastBlocks > 0) {
        ASSERT(taosArrayGetSize(pQLastBlock) == pBlockNum->numOfLastBlocks);
        taosArrayClear(pLastBlocks);
        taosArrayAddAll(pLastBlocks, pQLastBlock);

        taosArrayDestroy(pQLastBlock);
        break;
      }

      taosArrayDestroy(pQLastBlock);
    }

    // no blocks in current file, try next files
  }

  taosArrayDestroy(pIndexList);
  return TSDB_CODE_SUCCESS;
}

static int32_t doLoadRelatedLastBlock(SLastBlockReader* pLastBlockReader, STableBlockScanInfo *pBlockScanInfo, STsdbReader* pReader) {
  SArray*  pBlocks = pLastBlockReader->pBlockL;
  SBlockL* pBlock = NULL;

  uint64_t uid = pBlockScanInfo->uid;
  int32_t totalLastBlocks = (int32_t)taosArrayGetSize(pBlocks);

  initMemDataIterator(pBlockScanInfo, pReader);

  // find the correct SBlockL. todo binary search
  int32_t index = -1;
  for (int32_t i = 0; i < totalLastBlocks; ++i) {
    SBlockL* p = taosArrayGet(pBlocks, i);
    if (p->minUid <= uid && p->maxUid >= uid) {
      index = i;
      pBlock = p;
      break;
    }
  }

  if (index == -1) {
    pLastBlockReader->currentBlockIndex = index;
    tBlockDataReset(&pLastBlockReader->lastBlockData);
    return TSDB_CODE_SUCCESS;
  }

  // the required last datablock has already loaded
  if (index == pLastBlockReader->currentBlockIndex) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t st = taosGetTimestampUs();
  int32_t code = tBlockDataInit(&pLastBlockReader->lastBlockData, pReader->suid, pReader->suid ? 0 : uid, pReader->pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p init block data failed, code:%s %s", pReader, tstrerror(code), pReader->idStr);
    return code;
  }

  code = tsdbReadLastBlock(pReader->pFileReader, pBlock, &pLastBlockReader->lastBlockData);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p error occurs in loading last block into buffer, last block index:%d, total:%d code:%s %s", pReader,
              pLastBlockReader->currentBlockIndex, totalLastBlocks, tstrerror(code), pReader->idStr);
  } else {
    tsdbDebug("%p load last block completed, uid:%" PRIu64
              " last block index:%d, total:%d rows:%d, minVer:%d, maxVer:%d, brange:%" PRId64 "-%" PRId64
              " elapsed time:%.2f ms, %s",
              pReader, uid, index, totalLastBlocks, pBlock->nRow, pBlock->minVer, pBlock->maxVer, pBlock->minKey,
              pBlock->maxKey, el, pReader->idStr);
  }

  pLastBlockReader->currentBlockIndex = index;
  pReader->cost.lastBlockLoad += 1;
  pReader->cost.lastBlockLoadTime += el;

  return TSDB_CODE_SUCCESS;
}

static int32_t uidComparFunc(const void* p1, const void* p2) {
  uint64_t pu1 = *(uint64_t*) p1;
  uint64_t pu2 = *(uint64_t*) p2;
  if (pu1 == pu2) {
    return 0;
  } else {
    return (pu1 < pu2)? -1:1;
  }
}

static void extractOrderedTableUidList(SUidOrderCheckInfo *pOrderCheckInfo, SReaderStatus* pStatus) {
  int32_t index = 0;
  int32_t total = taosHashGetSize(pStatus->pTableMap);

  void* p = taosHashIterate(pStatus->pTableMap, NULL);
  while(p != NULL) {
    STableBlockScanInfo* pScanInfo = p;
    pOrderCheckInfo->tableUidList[index++] = pScanInfo->uid;
    p = taosHashIterate(pStatus->pTableMap, p);
  }

  taosSort(pOrderCheckInfo->tableUidList, total, sizeof(uint64_t), uidComparFunc);
}

static int32_t initOrderCheckInfo(SUidOrderCheckInfo* pOrderCheckInfo, SReaderStatus* pStatus) {
  if (pOrderCheckInfo->tableUidList == NULL) {
    int32_t total = taosHashGetSize(pStatus->pTableMap);

    pOrderCheckInfo->currentIndex = 0;
    pOrderCheckInfo->tableUidList = taosMemoryMalloc(total * sizeof(uint64_t));
    if (pOrderCheckInfo->tableUidList == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    extractOrderedTableUidList(pOrderCheckInfo, pStatus);

    uint64_t uid = pOrderCheckInfo->tableUidList[0];
    pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  } else {
    if (pStatus->pTableIter == NULL) {  // it is the last block of a new file
//      ASSERT(pOrderCheckInfo->currentIndex == taosHashGetSize(pStatus->pTableMap));

      pOrderCheckInfo->currentIndex = 0;
      uint64_t uid = pOrderCheckInfo->tableUidList[pOrderCheckInfo->currentIndex];
      pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));

      // the tableMap has already updated
      if (pStatus->pTableIter == NULL) {
        void* p = taosMemoryRealloc(pOrderCheckInfo->tableUidList, taosHashGetSize(pStatus->pTableMap)*sizeof(uint64_t));
        if (p == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }

        pOrderCheckInfo->tableUidList = p;
        extractOrderedTableUidList(pOrderCheckInfo, pStatus);

        uid = pOrderCheckInfo->tableUidList[0];
        pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool moveToNextTable(SUidOrderCheckInfo *pOrderedCheckInfo, SReaderStatus* pStatus) {
  pOrderedCheckInfo->currentIndex += 1;
  if (pOrderedCheckInfo->currentIndex >= taosHashGetSize(pStatus->pTableMap)) {
    pStatus->pTableIter = NULL;
    return false;
  }

  uint64_t uid = pOrderedCheckInfo->tableUidList[pOrderedCheckInfo->currentIndex];
  pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  ASSERT(pStatus->pTableIter != NULL);
  return true;
}

static int32_t doLoadLastBlockSequentially(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;
  SLastBlockReader* pLastBlockReader = pStatus->fileIter.pLastBlockReader;

  SUidOrderCheckInfo *pOrderedCheckInfo = &pStatus->uidCheckInfo;
  int32_t code = initOrderCheckInfo(pOrderedCheckInfo, pStatus);
  if (code != TSDB_CODE_SUCCESS || (taosHashGetSize(pStatus->pTableMap) == 0)) {
    return code;
  }

  while(1) {
    // load the last data block of current table
    STableBlockScanInfo* pScanInfo = pStatus->pTableIter;
    code = doLoadRelatedLastBlock(pLastBlockReader, pScanInfo, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pLastBlockReader->currentBlockIndex != -1) {
      initLastBlockReader(pLastBlockReader, pScanInfo->uid, &pScanInfo->indexInBlockL);
      int32_t index = pScanInfo->indexInBlockL;

      if (index == INITIAL_ROW_INDEX_VAL || index == pLastBlockReader->lastBlockData.nRow) {
        bool hasData = nextRowInLastBlock(pLastBlockReader, pScanInfo);
        if (!hasData) {  // current table does not have rows in last block, try next table
          bool hasNexTable = moveToNextTable(pOrderedCheckInfo, pStatus);
          if (!hasNexTable) {
            return TSDB_CODE_SUCCESS;
          }
          continue;
        }
      }
    } else {  // no data in last block, try next table
      bool hasNexTable = moveToNextTable(pOrderedCheckInfo, pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }
      continue;
    }

    code = doBuildDataBlock(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTable(pOrderedCheckInfo, pStatus);
    if (!hasNexTable) {
      return TSDB_CODE_SUCCESS;
    }
  }
}

static int32_t doBuildDataBlock(STsdbReader* pReader) {
  TSDBKEY key = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  SBlock* pBlock = NULL;

  SReaderStatus*       pStatus = &pReader->status;
  SDataBlockIter*      pBlockIter = &pStatus->blockIter;
  STableBlockScanInfo* pScanInfo = NULL;
  SFileDataBlockInfo*  pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SLastBlockReader*    pLastBlockReader = pReader->status.fileIter.pLastBlockReader;

  if (pBlockInfo != NULL) {
    pScanInfo = taosHashGet(pReader->status.pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
  } else {
    pScanInfo = pReader->status.pTableIter;
  }

  if (pBlockInfo != NULL) {
    pBlock = getCurrentBlock(pBlockIter);
  }

  {
    key = getCurrentKeyInBuf(pScanInfo, pReader);

    // load the last data block of current table
    code = doLoadRelatedLastBlock(pLastBlockReader, pScanInfo, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // note: the lastblock may be null here
    initLastBlockReader(pLastBlockReader, pScanInfo->uid, &pScanInfo->indexInBlockL);
    if (pScanInfo->indexInBlockL == INITIAL_ROW_INDEX_VAL || pScanInfo->indexInBlockL == pLastBlockReader->lastBlockData.nRow) {
      bool hasData = nextRowInLastBlock(pLastBlockReader, pScanInfo);
    }
  }

  if (pBlockInfo == NULL) {  // build data block from last data file
    ASSERT(pBlockIter->numOfBlocks == 0);
    code = buildComposedDataBlock(pReader);
  } else if (fileBlockShouldLoad(pReader, pBlockInfo, pBlock, pScanInfo, key, pLastBlockReader)) {
    tBlockDataReset(&pStatus->fileBlockData);
    code = tBlockDataInit(&pStatus->fileBlockData, pReader->suid, pScanInfo->uid, pReader->pSchema);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // build composed data block
    code = buildComposedDataBlock(pReader);
  } else if (bufferDataInFileBlockGap(pReader->order, key, pBlock)) {
    // data in memory that are earlier than current file block
    // todo rows in buffer should be less than the file block in asc, greater than file block in desc
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->order)) ? pBlock->minKey.ts : pBlock->maxKey.ts;
    code = buildDataBlockFromBuf(pReader, pScanInfo, endKey);
  } else {
    if (hasDataInLastBlock(pLastBlockReader) && !ASCENDING_TRAVERSE(pReader->order)) {
      // only return the rows in last block
      int64_t tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
      ASSERT (tsLast >= pBlock->maxKey.ts);
      tBlockDataReset(&pReader->status.fileBlockData);

      code = buildComposedDataBlock(pReader);
    } else {   // whole block is required, return it directly
      SDataBlockInfo* pInfo = &pReader->pResBlock->info;
      pInfo->rows = pBlock->nRow;
      pInfo->uid = pScanInfo->uid;
      pInfo->window = (STimeWindow){.skey = pBlock->minKey.ts, .ekey = pBlock->maxKey.ts};
      setComposedBlockFlag(pReader, false);
      setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlock->maxKey.ts, pReader->order);
    }
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
  SBlock* pBlock = getCurrentBlock(pBlockIter);

  SReaderStatus* pStatus = &pReader->status;

  SFileBlockDumpInfo* pDumpInfo = &pStatus->fBlockDumpInfo;

  pDumpInfo->totalRows = pBlock->nRow;
  pDumpInfo->allDumped = false;
  pDumpInfo->rowIndex = ASCENDING_TRAVERSE(pReader->order) ? 0 : pBlock->nRow - 1;
}

static int32_t initForFirstBlockInFile(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  SBlockNumber num = {0};

  int32_t code = moveToNextFile(pReader, &num);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // all data files are consumed, try data in buffer
  if (num.numOfBlocks + num.numOfLastBlocks == 0) {
    pReader->status.loadFromFile = false;
    return code;
  }

  // initialize the block iterator for a new fileset
  if (num.numOfBlocks > 0) {
    code = initBlockIterator(pReader, pBlockIter, num.numOfBlocks);
  } else { // no block data, only last block exists
    tBlockDataReset(&pReader->status.fileBlockData);
    resetDataBlockIterator(pBlockIter, pReader->order);
  }

  SLastBlockReader* pLReader = pReader->status.fileIter.pLastBlockReader;
  pLReader->currentBlockIndex = -1;

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

  if (pBlockIter->numOfBlocks == 0) {
    _begin:
    code = doLoadLastBlockSequentially(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }

    // all data blocks are checked in this last block file, now let's try the next file
    if (pReader->status.pTableIter == NULL) {
      code = initForFirstBlockInFile(pReader, pBlockIter);

      // error happens or all the data files are completely checked
      if ((code != TSDB_CODE_SUCCESS) || (pReader->status.loadFromFile == false)) {
        return code;
      }

      // this file does not have data files, let's start check the last block file if exists
      if (pBlockIter->numOfBlocks == 0) {
        goto _begin;
      }
    }

    code = doBuildDataBlock(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }
  }

  while (1) {
    SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

    if (fileBlockPartiallyRead(pDumpInfo, asc)) {  // file data block is partially loaded
      code = buildComposedDataBlock(pReader);
    } else {
      // current block are exhausted, try the next file block
      if (pDumpInfo->allDumped) {
        // try next data block in current file
        bool hasNext = blockIteratorNext(&pReader->status.blockIter);
        if (hasNext) {  // check for the next block in the block accessed order list
          initBlockDumpInfo(pReader, pBlockIter);
        } else if (taosArrayGetSize(pReader->status.fileIter.pLastBlockReader->pBlockL) > 0) {  // data blocks in current file are exhausted, let's try the next file now
          tBlockDataReset(&pReader->status.fileBlockData);
          resetDataBlockIterator(pBlockIter, pReader->order);
          goto _begin;
        } else {
          code = initForFirstBlockInFile(pReader, pBlockIter);

          // error happens or all the data files are completely checked
          if ((code != TSDB_CODE_SUCCESS) || (pReader->status.loadFromFile == false)) {
            return code;
          }

          // this file does not have blocks, let's start check the last block file
          if (pBlockIter->numOfBlocks == 0) {
            goto _begin;
          }
        }
      }

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

    const char* str = (idStr != NULL) ? idStr : "";

    if (level == TSDB_RETENTION_L0) {
      *pLevel = TSDB_RETENTION_L0;
      tsdbDebug("vgId:%d, rsma level %d is selected to query %s", TD_VID(pVnode), TSDB_RETENTION_L0, str);
      return VND_RSMA0(pVnode);
    } else if (level == TSDB_RETENTION_L1) {
      *pLevel = TSDB_RETENTION_L1;
      tsdbDebug("vgId:%d, rsma level %d is selected to query %s", TD_VID(pVnode), TSDB_RETENTION_L1, str);
      return VND_RSMA1(pVnode);
    } else {
      *pLevel = TSDB_RETENTION_L2;
      tsdbDebug("vgId:%d, rsma level %d is selected to query %s", TD_VID(pVnode), TSDB_RETENTION_L2, str);
      return VND_RSMA2(pVnode);
    }
  }

  return VND_TSDB(pVnode);
}

SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level) {
  int64_t startVer = (pCond->startVersion == -1) ? 0 : pCond->startVersion;

  int64_t endVer = 0;
  if (pCond->endVersion ==
      -1) {  // user not specified end version, set current maximum version of vnode as the endVersion
    endVer = pVnode->state.applied;
  } else {
    endVer = (pCond->endVersion > pVnode->state.applied) ? pVnode->state.applied : pCond->endVersion;
  }

  return (SVersionRange){.minVer = startVer, .maxVer = endVer};
}

bool hasBeenDropped(const SArray* pDelList, int32_t* index, TSDBKEY* pKey, int32_t order) {
  ASSERT(pKey != NULL);
  if (pDelList == NULL) {
    return false;
  }
  size_t  num = taosArrayGetSize(pDelList);
  bool    asc = ASCENDING_TRAVERSE(order);
  int32_t step = asc ? 1 : -1;

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

TSDBROW* getValidMemRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader) {
  if (!pIter->hasVal) {
    return NULL;
  }

  TSDBROW* pRow = tsdbTbDataIterGet(pIter->iter);
  TSDBKEY  key = {.ts = pRow->pTSRow->ts, .version = pRow->version};
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

int32_t doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, int64_t ts, SArray* pDelList, SRowMerger* pMerger,
                         STsdbReader* pReader) {
  while (1) {
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    if (!pIter->hasVal) {
      break;
    }

    // data exists but not valid
    TSDBROW* pRow = getValidMemRow(pIter, pDelList, pReader);
    if (pRow == NULL) {
      break;
    }

    // ts is not identical, quit
    TSDBKEY k = TSDBROW_KEY(pRow);
    if (k.ts != ts) {
      break;
    }

    STSchema* pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, uid);
    tRowMergerAdd(pMerger, pRow, pTSchema);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doMergeRowsInFileBlockImpl(SBlockData* pBlockData, int32_t rowIndex, int64_t key, SRowMerger* pMerger,
                                          SVersionRange* pVerRange, int32_t step) {
  while (pBlockData->aTSKEY[rowIndex] == key && rowIndex < pBlockData->nRow && rowIndex >= 0) {
    if (pBlockData->aVersion[rowIndex] > pVerRange->maxVer || pBlockData->aVersion[rowIndex] < pVerRange->minVer) {
      rowIndex += step;
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
  taosMemoryFree(pNeighborBlock);

  if (overlap) {  // load next block
    SReaderStatus*  pStatus = &pReader->status;
    SDataBlockIter* pBlockIter = &pStatus->blockIter;

    // 1. find the next neighbor block in the scan block list
    SFileDataBlockInfo fb = {.uid = pFBlock->uid, .tbBlockIdx = nextIndex};
    int32_t            neighborIndex = findFileBlockInfoIndex(pBlockIter, &fb);

    // 2. remove it from the scan block list
    setFileBlockActiveInBlockIter(pBlockIter, neighborIndex, step);

    // 3. load the neighbor block, and set it to be the currently accessed file data block
    tBlockDataReset(&pStatus->fileBlockData);
    int32_t code = tBlockDataInit(&pStatus->fileBlockData, pReader->suid, pFBlock->uid, pReader->pSchema);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // 4. check the data values
    initBlockDumpInfo(pReader, pBlockIter);

    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->verRange, step);
    if (pDumpInfo->rowIndex >= pDumpInfo->totalRows) {
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
  if ((pDumpInfo->rowIndex <= pBlockData->nRow - 1 && asc) || (pDumpInfo->rowIndex >= 0 && !asc)) {
    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->verRange, step);
  }

  // all rows are consumed, let's try next file block
  if ((pDumpInfo->rowIndex >= pBlockData->nRow && asc) || (pDumpInfo->rowIndex < 0 && !asc)) {
    while (1) {
      CHECK_FILEBLOCK_STATE st;

      SFileDataBlockInfo* pFileBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);
      SBlock*             pCurrentBlock = getCurrentBlock(&pReader->status.blockIter);
      checkForNeighborFileBlock(pReader, pScanInfo, pCurrentBlock, pFileBlockInfo, pMerger, key, &st);
      if (st == CHECK_FILEBLOCK_QUIT) {
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeRowsInLastBlock(SLastBlockReader* pLastBlockReader, STableBlockScanInfo* pScanInfo, int64_t ts, SRowMerger* pMerger) {
  while(nextRowInLastBlock(pLastBlockReader, pScanInfo)) {
    int64_t next1 = getCurrentKeyInLastBlock(pLastBlockReader);
    if (next1 == ts) {
      TSDBROW fRow1 = tsdbRowFromBlockData(&pLastBlockReader->lastBlockData, *pLastBlockReader->rowIndex);
      tRowMerge(pMerger, &fRow1);
    } else {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void doMergeMemTableMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList, STSRow** pTSRow,
                      STsdbReader* pReader, bool* freeTSRow) {
  TSDBROW* pNextRow = NULL;
  TSDBROW  current = *pRow;

  {  // if the timestamp of the next valid row has a different ts, return current row directly
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);

    if (!pIter->hasVal) {
      *pTSRow = current.pTSRow;
      *freeTSRow = false;
      return;
    } else {  // has next point in mem/imem
      pNextRow = getValidMemRow(pIter, pDelList, pReader);
      if (pNextRow == NULL) {
        *pTSRow = current.pTSRow;
        *freeTSRow = false;
        return;
      }

      if (current.pTSRow->ts != pNextRow->pTSRow->ts) {
        *pTSRow = current.pTSRow;
        *freeTSRow = false;
        return;
      }
    }
  }

  SRowMerger merge = {0};

  // get the correct schema for data in memory
  STSchema* pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(&current), pReader, uid);

  if (pReader->pSchema == NULL) {
    pReader->pSchema = pTSchema;
  }

  tRowMergerInit2(&merge, pReader->pSchema, &current, pTSchema);

  STSchema* pTSchema1 = doGetSchemaForTSRow(TSDBROW_SVERSION(pNextRow), pReader, uid);
  tRowMergerAdd(&merge, pNextRow, pTSchema1);

  doMergeRowsInBuf(pIter, uid, current.pTSRow->ts, pDelList, &merge, pReader);
  tRowMergerGetRow(&merge, pTSRow);
  tRowMergerClear(&merge);

  *freeTSRow = true;
}

void doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader,
                        STSRow** pTSRow) {
  SRowMerger merge = {0};

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  if (ASCENDING_TRAVERSE(pReader->order)) {  // ascending order imem --> mem
    STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);

    tRowMergerInit(&merge, piRow, pSchema);
    doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge, pReader);

    tRowMerge(&merge, pRow);
    doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
  } else {
    STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);

    tRowMergerInit(&merge, pRow, pSchema);
    doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);

    tRowMerge(&merge, piRow);
    doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
  }

  tRowMergerGetRow(&merge, pTSRow);
}

int32_t tsdbGetNextRowInMem(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STSRow** pTSRow, int64_t endKey,
                            bool* freeTSRow) {
  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);
  SArray*  pDelList = pBlockScanInfo->delSkyline;
  uint64_t uid = pBlockScanInfo->uid;

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

    if (ik.ts != k.ts) {
      if (((ik.ts < k.ts) && asc) || ((ik.ts > k.ts) && (!asc))) {  // ik.ts < k.ts
        doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, pTSRow, pReader, freeTSRow);
      } else if (((k.ts < ik.ts) && asc) || ((k.ts > ik.ts) && (!asc))) {
        doMergeMemTableMultiRows(pRow, uid, &pBlockScanInfo->iter, pDelList, pTSRow, pReader, freeTSRow);
      }
    } else {  // ik.ts == k.ts
      doMergeMemIMemRows(pRow, piRow, pBlockScanInfo, pReader, pTSRow);
      *freeTSRow = true;
    }

    return TSDB_CODE_SUCCESS;
  }

  if (pBlockScanInfo->iter.hasVal && pRow != NULL) {
    doMergeMemTableMultiRows(pRow, pBlockScanInfo->uid, &pBlockScanInfo->iter, pDelList, pTSRow, pReader, freeTSRow);
    return TSDB_CODE_SUCCESS;
  }

  if (pBlockScanInfo->iiter.hasVal && piRow != NULL) {
    doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, pTSRow, pReader, freeTSRow);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, STSRow* pTSRow, uint64_t uid) {
  int32_t numOfRows = pBlock->info.rows;
  int32_t numOfCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  STSchema*           pSchema = doGetSchemaForTSRow(pTSRow->sver, pReader, uid);

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
      tTSRowGetVal(pTSRow, pSchema, j, &colVal);
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

int32_t doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData, int32_t rowIndex) {
  int32_t i = 0, j = 0;
  int32_t outputRowIndex = pResBlock->info.rows;

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;

  SColumnInfoData* pColData = taosArrayGet(pResBlock->pDataBlock, i);
  if (pColData->info.colId == PRIMARYKEY_TIMESTAMP_COL_ID) {
    colDataAppendInt64(pColData, outputRowIndex, &pBlockData->aTSKEY[rowIndex]);
    i += 1;
  }

  SColVal cv = {0};
  int32_t numOfInputCols = taosArrayGetSize(pBlockData->aIdx);
  int32_t numOfOutputCols = blockDataGetNumOfCols(pResBlock);

  while (i < numOfOutputCols && j < numOfInputCols) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, i);
    SColData*        pData = tBlockDataGetColDataByIdx(pBlockData, j);

    if (pData->cid == pCol->info.colId) {
      tColDataGetValue(pData, rowIndex, &cv);
      doCopyColVal(pCol, outputRowIndex, i, &cv, pSupInfo);
      j += 1;
    } else {  // the specified column does not exist in file block, fill with null data
      colDataAppendNULL(pCol, outputRowIndex);
    }

    i += 1;
  }

  while (i < numOfOutputCols) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, i);
    colDataAppendNULL(pCol, outputRowIndex);
    i += 1;
  }

  pResBlock->info.rows += 1;
  return TSDB_CODE_SUCCESS;
}

int32_t buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                  STsdbReader* pReader) {
  SSDataBlock* pBlock = pReader->pResBlock;

  do {
    STSRow* pTSRow = NULL;
    bool    freeTSRow = false;
    tsdbGetNextRowInMem(pBlockScanInfo, pReader, &pTSRow, endKey, &freeTSRow);
    if (pTSRow == NULL) {
      break;
    }

    doAppendRowFromTSRow(pBlock, pReader, pTSRow, pBlockScanInfo->uid);
    if (freeTSRow) {
      taosMemoryFree(pTSRow);
    }

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

  STableBlockScanInfo info = {.lastKey = 0, .uid = uid, .indexInBlockL = INITIAL_ROW_INDEX_VAL};
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

uint64_t getReaderMaxVersion(STsdbReader* pReader) { return pReader->verRange.maxVer; }


// ====================================== EXPOSED APIs ======================================
int32_t tsdbReaderOpen(SVnode* pVnode, SQueryTableDataCond* pCond, SArray* pTableList, STsdbReader** ppReader,
                       const char* idstr) {
  int32_t code = tsdbReaderCreate(pVnode, pCond, ppReader, 4096, idstr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  // check for query time window
  STsdbReader* pReader = *ppReader;
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, %s", pReader, pReader->idStr);
    return TSDB_CODE_SUCCESS;
  }

  if (pCond->type == TIMEWINDOW_RANGE_EXTERNAL) {
    // update the SQueryTableDataCond to create inner reader
    STimeWindow w = pCond->twindows;
    int32_t     order = pCond->order;
    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.ekey = pCond->twindows.skey;
      pCond->twindows.skey = INT64_MIN;
      pCond->order = TSDB_ORDER_DESC;
    } else {
      pCond->twindows.skey = pCond->twindows.ekey;
      pCond->twindows.ekey = INT64_MAX;
      pCond->order = TSDB_ORDER_ASC;
    }

    // here we only need one more row, so the capacity is set to be ONE.
    code = tsdbReaderCreate(pVnode, pCond, &pReader->innerReader[0], 1, idstr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.skey = w.ekey;
      pCond->twindows.ekey = INT64_MAX;
    } else {
      pCond->twindows.skey = INT64_MIN;
      pCond->twindows.ekey = w.ekey;
    }
    code = tsdbReaderCreate(pVnode, pCond, &pReader->innerReader[1], 1, idstr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }
  }

  if (pCond->suid != 0) {
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, pReader->suid, pCond->schemaVersion);
  } else if (taosArrayGetSize(pTableList) > 0) {
    STableKeyInfo* pKey = taosArrayGet(pTableList, 0);
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, pKey->uid, pCond->schemaVersion);
  }

  int32_t numOfTables = taosArrayGetSize(pTableList);
  pReader->status.pTableMap = createDataBlockScanInfo(pReader, pTableList->pData, numOfTables);
  if (pReader->status.pTableMap == NULL) {
    tsdbReaderClose(pReader);
    *ppReader = NULL;

    code = TSDB_CODE_TDB_OUT_OF_MEMORY;
    goto _err;
  }

  code = tsdbTakeReadSnap(pReader->pTsdb, &pReader->pReadSnap);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  if (pReader->type == TIMEWINDOW_RANGE_CONTAINED) {
    SDataBlockIter* pBlockIter = &pReader->status.blockIter;

    initFilesetIterator(&pReader->status.fileIter, pReader->pReadSnap->fs.aDFileSet, pReader);
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
  } else {
    STsdbReader*    pPrevReader = pReader->innerReader[0];
    SDataBlockIter* pBlockIter = &pPrevReader->status.blockIter;

    code = tsdbTakeReadSnap(pPrevReader->pTsdb, &pPrevReader->pReadSnap);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    initFilesetIterator(&pPrevReader->status.fileIter, pPrevReader->pReadSnap->fs.aDFileSet, pPrevReader);
    resetDataBlockIterator(&pPrevReader->status.blockIter, pPrevReader->order);

    // no data in files, let's try buffer in memory
    if (pPrevReader->status.fileIter.numOfFiles == 0) {
      pPrevReader->status.loadFromFile = false;
    } else {
      code = initForFirstBlockInFile(pPrevReader, pBlockIter);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  tsdbDebug("%p total numOfTable:%d in this query %s", pReader, numOfTables, pReader->idStr);
  return code;

_err:
  tsdbError("failed to create data reader, code:%s %s", tstrerror(code), pReader->idStr);
  return code;
}

void tsdbReaderClose(STsdbReader* pReader) {
  if (pReader == NULL) {
    return;
  }

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  tsdbUntakeReadSnap(pReader->pTsdb, pReader->pReadSnap);

  taosMemoryFreeClear(pSupInfo->plist);
  taosMemoryFree(pSupInfo->colIds);

  taosArrayDestroy(pSupInfo->pColAgg);
  for (int32_t i = 0; i < blockDataGetNumOfCols(pReader->pResBlock); ++i) {
    if (pSupInfo->buildBuf[i] != NULL) {
      taosMemoryFreeClear(pSupInfo->buildBuf[i]);
    }
  }
  taosMemoryFree(pSupInfo->buildBuf);
  tBlockDataDestroy(&pReader->status.fileBlockData, true);

  cleanupDataBlockIterator(&pReader->status.blockIter);

  size_t numOfTables = taosHashGetSize(pReader->status.pTableMap);
  destroyBlockScanInfo(pReader->status.pTableMap);
  blockDataDestroy(pReader->pResBlock);

  if (pReader->pFileReader != NULL) {
    tsdbDataFReaderClose(&pReader->pFileReader);
  }

  taosMemoryFree(pReader->status.uidCheckInfo.tableUidList);

  SFilesetIter* pFilesetIter = &pReader->status.fileIter;
  if (pFilesetIter->pLastBlockReader != NULL) {
    tBlockDataDestroy(&pFilesetIter->pLastBlockReader->lastBlockData, true);
    taosArrayDestroy(pFilesetIter->pLastBlockReader->pBlockL);
    taosMemoryFree(pFilesetIter->pLastBlockReader);
  }

  SIOCostSummary* pCost = &pReader->cost;

  tsdbDebug("%p :io-cost summary: head-file:%" PRIu64 ", head-file time:%.2f ms, SMA:%" PRId64
            " SMA-time:%.2f ms, fileBlocks:%" PRId64
            ", fileBlocks-time:%.2f ms, "
            "build in-memory-block-time:%.2f ms, lastBlocks:%" PRId64
            ", lastBlocks-time:%.2f ms, STableBlockScanInfo size:%.2f Kb %s",
            pReader, pCost->headFileLoad, pCost->headFileLoadTime, pCost->smaDataLoad, pCost->smaLoadTime,
            pCost->numOfBlocks, pCost->blockLoadTime, pCost->buildmemBlock, pCost->lastBlockLoad,
            pCost->lastBlockLoadTime, numOfTables * sizeof(STableBlockScanInfo) / 1000.0, pReader->idStr);

  taosMemoryFree(pReader->idStr);
  taosMemoryFree(pReader->pSchema);
  if (pReader->pMemSchema != pReader->pSchema) {
    taosMemoryFree(pReader->pMemSchema);
  }
  taosMemoryFreeClear(pReader);
}

static bool doTsdbNextDataBlock(STsdbReader* pReader) {
  // cleanup the data that belongs to the previous data block
  SSDataBlock* pBlock = pReader->pResBlock;
  blockDataCleanup(pBlock);

  SReaderStatus* pStatus = &pReader->status;

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

  return false;
}

bool tsdbNextDataBlock(STsdbReader* pReader) {
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    return false;
  }

  if (pReader->innerReader[0] != NULL) {
    bool ret = doTsdbNextDataBlock(pReader->innerReader[0]);
    if (ret) {
      pReader->step = EXTERNAL_ROWS_PREV;
      return ret;
    }

    tsdbReaderClose(pReader->innerReader[0]);
    pReader->innerReader[0] = NULL;
  }

  pReader->step = EXTERNAL_ROWS_MAIN;
  bool ret = doTsdbNextDataBlock(pReader);
  if (ret) {
    return ret;
  }

  if (pReader->innerReader[1] != NULL) {
    bool ret1 = doTsdbNextDataBlock(pReader->innerReader[1]);
    if (ret1) {
      pReader->step = EXTERNAL_ROWS_NEXT;
      return ret1;
    }

    tsdbReaderClose(pReader->innerReader[1]);
    pReader->innerReader[1] = NULL;
  }

  return false;
}

static void setBlockInfo(STsdbReader* pReader, SDataBlockInfo* pDataBlockInfo) {
  ASSERT(pDataBlockInfo != NULL && pReader != NULL);
  pDataBlockInfo->rows = pReader->pResBlock->info.rows;
  pDataBlockInfo->uid = pReader->pResBlock->info.uid;
  pDataBlockInfo->window = pReader->pResBlock->info.window;
}

void tsdbRetrieveDataBlockInfo(STsdbReader* pReader, SDataBlockInfo* pDataBlockInfo) {
  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    if (pReader->step == EXTERNAL_ROWS_MAIN) {
      setBlockInfo(pReader, pDataBlockInfo);
    } else if (pReader->step == EXTERNAL_ROWS_PREV) {
      setBlockInfo(pReader->innerReader[0], pDataBlockInfo);
    } else {
      setBlockInfo(pReader->innerReader[1], pDataBlockInfo);
    }
  } else {
    setBlockInfo(pReader, pDataBlockInfo);
  }
}

int32_t tsdbRetrieveDatablockSMA(STsdbReader* pReader, SColumnDataAgg*** pBlockStatis, bool* allHave) {
  int32_t code = 0;
  *allHave = false;

  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // there is no statistics data for composed block
  if (pReader->status.composedDataBlock) {
    *pBlockStatis = NULL;
    return TSDB_CODE_SUCCESS;
  }

  SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(&pReader->status.blockIter);

  SBlock* pBlock = getCurrentBlock(&pReader->status.blockIter);
  int64_t stime = taosGetTimestampUs();

  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;

  if (tBlockHasSma(pBlock)) {
    code = tsdbReadBlockSma(pReader->pFileReader, pBlock, pSup->pColAgg);
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

  double elapsed = (taosGetTimestampUs() - stime) / 1000.0;
  pReader->cost.smaLoadTime += elapsed;
  pReader->cost.smaDataLoad += 1;

  *pBlockStatis = pSup->plist;

  tsdbDebug("vgId:%d, succeed to load block SMA for uid %" PRIu64 ", elapsed time:%.2f ms, %s", 0, pFBlock->uid,
            elapsed, pReader->idStr);

  return code;
}

static SArray* doRetrieveDataBlock(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;

  if (pStatus->composedDataBlock) {
    return pReader->pResBlock->pDataBlock;
  }

  SFileDataBlockInfo*  pFBlock = getCurrentBlockInfo(&pStatus->blockIter);
  STableBlockScanInfo* pBlockScanInfo = taosHashGet(pStatus->pTableMap, &pFBlock->uid, sizeof(pFBlock->uid));

  tBlockDataReset(&pStatus->fileBlockData);
  int32_t code = tBlockDataInit(&pStatus->fileBlockData, pReader->suid, pBlockScanInfo->uid, pReader->pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  }

  code = doLoadFileBlockData(pReader, &pStatus->blockIter, &pStatus->fileBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    tBlockDataDestroy(&pStatus->fileBlockData, 1);
    terrno = code;
    return NULL;
  }

  copyBlockDataToSDataBlock(pReader, pBlockScanInfo);
  return pReader->pResBlock->pDataBlock;
}

SArray* tsdbRetrieveDataBlock(STsdbReader* pReader, SArray* pIdList) {
  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    if (pReader->step == EXTERNAL_ROWS_PREV) {
      return doRetrieveDataBlock(pReader->innerReader[0]);
    } else if (pReader->step == EXTERNAL_ROWS_NEXT) {
      return doRetrieveDataBlock(pReader->innerReader[1]);
    }
  }

  return doRetrieveDataBlock(pReader);
}

int32_t tsdbReaderReset(STsdbReader* pReader, SQueryTableDataCond* pCond) {
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    return TSDB_CODE_SUCCESS;
  }

  pReader->order = pCond->order;
  pReader->type = TIMEWINDOW_RANGE_CONTAINED;
  pReader->status.loadFromFile = true;
  pReader->status.pTableIter = NULL;
  pReader->window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows);

  // allocate buffer in order to load data blocks from file
  memset(&pReader->suppInfo.tsColAgg, 0, sizeof(SColumnDataAgg));
  memset(pReader->suppInfo.plist, 0, POINTER_BYTES);

  pReader->suppInfo.tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tsdbDataFReaderClose(&pReader->pFileReader);

  int32_t numOfTables = taosHashGetSize(pReader->status.pTableMap);
  tsdbDataFReaderClose(&pReader->pFileReader);

  initFilesetIterator(&pReader->status.fileIter, pReader->pReadSnap->fs.aDFileSet, pReader);
  resetDataBlockIterator(&pReader->status.blockIter, pReader->order);
  resetDataBlockScanInfo(pReader->status.pTableMap);

  int32_t         code = 0;
  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  // no data in files, let's try buffer in memory
  if (pReader->status.fileIter.numOfFiles == 0) {
    pReader->status.loadFromFile = false;
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("%p reset reader failed, numOfTables:%d, query range:%" PRId64 " - %" PRId64 " in query %s", pReader,
                numOfTables, pReader->window.skey, pReader->window.ekey, pReader->idStr);
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

  if (pBlockIter->numOfBlocks > 0) {
    pTableBlockInfo->numOfBlocks += pBlockIter->numOfBlocks;
  }

  pTableBlockInfo->numOfTables = numOfTables;
  bool hasNext = (pBlockIter->numOfBlocks > 0);

  while (true) {
    if (hasNext) {
      SBlock* pBlock = getCurrentBlock(pBlockIter);

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
      hasNext = (pBlockIter->numOfBlocks > 0);
    }

    //    tsdbDebug("%p %d blocks found in file for %d table(s), fid:%d, %s", pReader, numOfBlocks, numOfTables,
    //              pReader->pFileGroup->fid, pReader->idStr);
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
      d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->suid, pBlockScanInfo->uid);
      if (d != NULL) {
        rows += tsdbGetNRowsInTbData(d);
      }
    }

    STbData* di = NULL;
    if (pReader->pTsdb->imem != NULL) {
      di = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pIMem, pReader->suid, pBlockScanInfo->uid);
      if (di != NULL) {
        rows += tsdbGetNRowsInTbData(di);
      }
    }

    // current table is exhausted, let's try the next table
    pStatus->pTableIter = taosHashIterate(pStatus->pTableMap, pStatus->pTableIter);
  }

  return rows;
}

int32_t tsdbGetTableSchema(SVnode* pVnode, int64_t uid, STSchema** pSchema, int64_t* suid) {
  int32_t sversion = 1;

  SMetaReader mr = {0};
  metaReaderInit(&mr, pVnode->pMeta, 0);
  int32_t code = metaGetTableEntryByUid(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    metaReaderClear(&mr);
    return terrno;
  }

  *suid = 0;

  if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);
    *suid = mr.me.ctbEntry.suid;
    code = metaGetTableEntryByUid(&mr, *suid);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      metaReaderClear(&mr);
      return terrno;
    }
    sversion = mr.me.stbEntry.schemaRow.version;
  } else {
    ASSERT(mr.me.type == TSDB_NORMAL_TABLE);
    sversion = mr.me.ntbEntry.schemaRow.version;
  }

  metaReaderClear(&mr);
  *pSchema = metaGetTbTSchema(pVnode->pMeta, uid, sversion);

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbTakeReadSnap(STsdb* pTsdb, STsdbReadSnap** ppSnap) {
  int32_t code = 0;

  // alloc
  *ppSnap = (STsdbReadSnap*)taosMemoryCalloc(1, sizeof(STsdbReadSnap));
  if (*ppSnap == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // lock
  code = taosThreadRwlockRdlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  // take snapshot
  (*ppSnap)->pMem = pTsdb->mem;
  (*ppSnap)->pIMem = pTsdb->imem;

  if ((*ppSnap)->pMem) {
    tsdbRefMemTable((*ppSnap)->pMem);
  }

  if ((*ppSnap)->pIMem) {
    tsdbRefMemTable((*ppSnap)->pIMem);
  }

  // fs
  code = tsdbFSRef(pTsdb, &(*ppSnap)->fs);
  if (code) {
    taosThreadRwlockUnlock(&pTsdb->rwLock);
    goto _exit;
  }

  // unlock
  code = taosThreadRwlockUnlock(&pTsdb->rwLock);
  if (code) {
    code = TAOS_SYSTEM_ERROR(code);
    goto _exit;
  }

  tsdbTrace("vgId:%d, take read snapshot", TD_VID(pTsdb->pVnode));
_exit:
  return code;
}

void tsdbUntakeReadSnap(STsdb* pTsdb, STsdbReadSnap* pSnap) {
  if (pSnap) {
    if (pSnap->pMem) {
      tsdbUnrefMemTable(pSnap->pMem);
    }

    if (pSnap->pIMem) {
      tsdbUnrefMemTable(pSnap->pIMem);
    }

    tsdbFSUnref(pTsdb, &pSnap->fs);
    taosMemoryFree(pSnap);
  }

  tsdbTrace("vgId:%d, untake read snapshot", TD_VID(pTsdb->pVnode));
}
