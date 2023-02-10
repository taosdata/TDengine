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

#define ASCENDING_TRAVERSE(o) (o == TSDB_ORDER_ASC)

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
  int32_t numOfLastFiles;
} SBlockNumber;

typedef struct SBlockIndex {
  int32_t     ordinalIndex;
  int64_t     inFileOffset;
  STimeWindow window;  // todo replace it with overlap flag.
} SBlockIndex;

typedef struct STableBlockScanInfo {
  uint64_t  uid;
  TSKEY     lastKey;
  SMapData  mapData;            // block info (compressed)
  SArray*   pBlockList;         // block data index list, SArray<SBlockIndex>
  SIterInfo iter;               // mem buffer skip list iterator
  SIterInfo iiter;              // imem buffer skip list iterator
  SArray*   delSkyline;         // delete info for this table
  int32_t   fileDelIndex;       // file block delete index
  int32_t   lastBlockDelIndex;  // delete index for last block
  bool      iterInit;           // whether to initialize the in-memory skip list iterator or not
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
  int64_t composedBlocks;
  double  buildComposedBlockTime;
  double  createScanInfoList;
//  double  getTbFromMemTime;
//  double  getTbFromIMemTime;
  double  initDelSkylineIterTime;
} SIOCostSummary;

typedef struct SBlockLoadSuppInfo {
  SArray*        pColAgg;
  SColumnDataAgg tsColAgg;
  int16_t*       colId;
  int16_t*       slotId;
  int32_t        numOfCols;
  char**         buildBuf;  // build string tmp buffer, todo remove it later after all string format being updated.
  bool           smaValid;  // the sma on all queried columns are activated
} SBlockLoadSuppInfo;

typedef struct SLastBlockReader {
  STimeWindow        window;
  SVersionRange      verRange;
  int32_t            order;
  uint64_t           uid;
  SMergeTree         mergeTree;
  SSttBlockLoadInfo* pInfo;
} SLastBlockReader;

typedef struct SFilesetIter {
  int32_t           numOfFiles;  // number of total files
  int32_t           index;       // current accessed index in the list
  SArray*           pFileList;   // data file list
  int32_t           order;
  SLastBlockReader* pLastBlockReader;  // last file block reader
} SFilesetIter;

typedef struct SFileDataBlockInfo {
  // index position in STableBlockScanInfo in order to check whether neighbor block overlaps with it
  uint64_t uid;
  int32_t  tbBlockIdx;
} SFileDataBlockInfo;

typedef struct SDataBlockIter {
  int32_t   numOfBlocks;
  int32_t   index;
  SArray*   blockList;  // SArray<SFileDataBlockInfo>
  int32_t   order;
  SDataBlk  block;  // current SDataBlk data
  SHashObj* pTableMap;
} SDataBlockIter;

typedef struct SFileBlockDumpInfo {
  int32_t totalRows;
  int32_t rowIndex;
  int64_t lastKey;
  bool    allDumped;
} SFileBlockDumpInfo;

typedef struct SUidOrderCheckInfo {
  uint64_t* tableUidList;  // access table uid list in uid ascending order list
  int32_t   currentIndex;  // index in table uid list
} SUidOrderCheckInfo;

typedef struct SReaderStatus {
  bool                  loadFromFile;       // check file stage
  bool                  composedDataBlock;  // the returned data block is a composed block or not
  SHashObj*             pTableMap;          // SHash<STableBlockScanInfo>
  STableBlockScanInfo** pTableIter;         // table iterator used in building in-memory buffer data blocks.
  SUidOrderCheckInfo    uidCheckInfo;       // check all table in uid order
  SFileBlockDumpInfo    fBlockDumpInfo;
  SDFileSet*            pCurrentFileset;  // current opened file set
  SBlockData            fileBlockData;
  SFilesetIter          fileIter;
  SDataBlockIter        blockIter;
} SReaderStatus;

typedef struct SBlockInfoBuf {
  int32_t currentIndex;
  SArray* pData;
  int32_t numPerBucket;
} SBlockInfoBuf;

struct STsdbReader {
  STsdb*             pTsdb;
  uint64_t           suid;
  int16_t            order;
  bool               freeBlock;
  STimeWindow        window;  // the primary query time window that applies to all queries
  SSDataBlock*       pResBlock;
  int32_t            capacity;
  SReaderStatus      status;
  char*              idStr;  // query info handle, for debug purpose
  int32_t            type;   // query type: 1. retrieve all data blocks, 2. retrieve direct prev|next rows
  SBlockLoadSuppInfo suppInfo;
  STsdbReadSnap*     pReadSnap;
  SIOCostSummary     cost;
  STSchema*          pSchema;      // the newest version schema
  STSchema*          pMemSchema;   // the previous schema for in-memory data, to avoid load schema too many times
  SDataFReader*      pFileReader;  // the file reader
  SDelFReader*       pDelFReader;  // the del file reader
  SArray*            pDelIdx;      // del file block index;
  SVersionRange      verRange;
  SBlockInfoBuf      blockInfoBuf;
  int32_t            step;
  STsdbReader*       innerReader[2];
};

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter);
static int      buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                          STsdbReader* pReader);
static TSDBROW* getValidMemRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader);
static int32_t  doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, STsdbReader* pReader,
                                        SRowMerger* pMerger);
static int32_t  doMergeRowsInLastBlock(SLastBlockReader* pLastBlockReader, STableBlockScanInfo* pScanInfo, int64_t ts,
                                       SRowMerger* pMerger, SVersionRange* pVerRange);
static int32_t  doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, int64_t ts, SArray* pDelList, SRowMerger* pMerger,
                                 STsdbReader* pReader);
static int32_t  doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, STSRow* pTSRow,
                                     STableBlockScanInfo* pScanInfo);
static int32_t  doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                         int32_t rowIndex);
static void     setComposedBlockFlag(STsdbReader* pReader, bool composed);
static bool     hasBeenDropped(const SArray* pDelList, int32_t* index, TSDBKEY* pKey, int32_t order,
                               SVersionRange* pVerRange);

static int32_t doMergeMemTableMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList,
                                        STSRow** pTSRow, STsdbReader* pReader, bool* freeTSRow);
static int32_t doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo,
                                  STsdbReader* pReader, STSRow** pTSRow);
static int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, int64_t key,
                                     STsdbReader* pReader);

static int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STbData* pMemTbData,
                                      STbData* piMemTbData);
static STsdb*  getTsdbByRetentions(SVnode* pVnode, TSKEY winSKey, SRetention* retentions, const char* idstr,
                                   int8_t* pLevel);
static SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level);
static int64_t       getCurrentKeyInLastBlock(SLastBlockReader* pLastBlockReader);
static bool          hasDataInLastBlock(SLastBlockReader* pLastBlockReader);
static int32_t       doBuildDataBlock(STsdbReader* pReader);
static TSDBKEY       getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader);
static bool          hasDataInFileBlock(const SBlockData* pBlockData, const SFileBlockDumpInfo* pDumpInfo);
static void          initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter);
static int32_t       getInitialDelIndex(const SArray* pDelSkyline, int32_t order);

static FORCE_INLINE STSchema* getLatestTableSchema(STsdbReader* pReader, uint64_t uid);

static bool outOfTimeWindow(int64_t ts, STimeWindow* pWindow) { return (ts > pWindow->ekey) || (ts < pWindow->skey); }

static int32_t setColumnIdSlotList(SBlockLoadSuppInfo* pSupInfo, SColumnInfo* pCols, const int32_t* pSlotIdList,
                                   int32_t numOfCols) {
  pSupInfo->smaValid = true;
  pSupInfo->numOfCols = numOfCols;
  pSupInfo->colId = taosMemoryMalloc(numOfCols * (sizeof(int16_t) * 2 + POINTER_BYTES));
  if (pSupInfo->colId == NULL) {
    taosMemoryFree(pSupInfo->colId);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSupInfo->slotId = (int16_t*)((char*)pSupInfo->colId + (sizeof(int16_t) * numOfCols));
  pSupInfo->buildBuf = (char**)((char*)pSupInfo->slotId + (sizeof(int16_t) * numOfCols));
  for (int32_t i = 0; i < numOfCols; ++i) {
    pSupInfo->colId[i] = pCols[i].colId;
    pSupInfo->slotId[i] = pSlotIdList[i];

    if (IS_VAR_DATA_TYPE(pCols[i].type)) {
      pSupInfo->buildBuf[i] = taosMemoryMalloc(pCols[i].bytes);
    } else {
      pSupInfo->buildBuf[i] = NULL;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t updateBlockSMAInfo(STSchema* pSchema, SBlockLoadSuppInfo* pSupInfo) {
  int32_t i = 0, j = 0;

  while (i < pSchema->numOfCols && j < pSupInfo->numOfCols) {
    STColumn* pTCol = &pSchema->columns[i];
    if (pTCol->colId == pSupInfo->colId[j]) {
      if (!IS_BSMA_ON(pTCol)) {
        pSupInfo->smaValid = false;
        return TSDB_CODE_SUCCESS;
      }

      i += 1;
      j += 1;
    } else if (pTCol->colId < pSupInfo->colId[j]) {
      // do nothing
      i += 1;
    } else {
      return TSDB_CODE_INVALID_PARA;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t initBlockScanInfoBuf(SBlockInfoBuf* pBuf, int32_t numOfTables) {
  int32_t num = numOfTables / pBuf->numPerBucket;
  int32_t remainder = numOfTables % pBuf->numPerBucket;
  if (pBuf->pData == NULL) {
    pBuf->pData = taosArrayInit(num + 1, POINTER_BYTES);
  }

  for (int32_t i = 0; i < num; ++i) {
    char* p = taosMemoryCalloc(pBuf->numPerBucket, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    taosArrayPush(pBuf->pData, &p);
  }

  if (remainder > 0) {
    char* p = taosMemoryCalloc(remainder, sizeof(STableBlockScanInfo));
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
    taosArrayPush(pBuf->pData, &p);
  }

  return TSDB_CODE_SUCCESS;
}

static void clearBlockScanInfoBuf(SBlockInfoBuf* pBuf) {
  size_t num = taosArrayGetSize(pBuf->pData);
  for (int32_t i = 0; i < num; ++i) {
    char** p = taosArrayGet(pBuf->pData, i);
    taosMemoryFree(*p);
  }

  taosArrayDestroy(pBuf->pData);
}

static void* getPosInBlockInfoBuf(SBlockInfoBuf* pBuf, int32_t index) {
  int32_t bucketIndex = index / pBuf->numPerBucket;
  char**  pBucket = taosArrayGet(pBuf->pData, bucketIndex);
  return (*pBucket) + (index % pBuf->numPerBucket) * sizeof(STableBlockScanInfo);
}

// NOTE: speedup the whole processing by preparing the buffer for STableBlockScanInfo in batch model
static SHashObj* createDataBlockScanInfo(STsdbReader* pTsdbReader, SBlockInfoBuf* pBuf, const STableKeyInfo* idList,
                                         int32_t numOfTables) {
  // allocate buffer in order to load data blocks from file
  // todo use simple hash instead, optimize the memory consumption
  SHashObj* pTableMap =
      taosHashInit(numOfTables, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pTableMap == NULL) {
    return NULL;
  }

  int64_t st = taosGetTimestampUs();
  initBlockScanInfoBuf(pBuf, numOfTables);

  for (int32_t j = 0; j < numOfTables; ++j) {
    STableBlockScanInfo* pScanInfo = getPosInBlockInfoBuf(pBuf, j);
    pScanInfo->uid = idList[j].uid;
    if (ASCENDING_TRAVERSE(pTsdbReader->order)) {
      int64_t skey = pTsdbReader->window.skey;
      pScanInfo->lastKey = (skey > INT64_MIN) ? (skey - 1) : skey;
    } else {
      int64_t ekey = pTsdbReader->window.ekey;
      pScanInfo->lastKey = (ekey < INT64_MAX) ? (ekey + 1) : ekey;
    }

    taosHashPut(pTableMap, &pScanInfo->uid, sizeof(uint64_t), &pScanInfo, POINTER_BYTES);
    tsdbTrace("%p check table uid:%" PRId64 " from lastKey:%" PRId64 " %s", pTsdbReader, pScanInfo->uid,
              pScanInfo->lastKey, pTsdbReader->idStr);
  }

  pTsdbReader->cost.createScanInfoList = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("%p create %d tables scan-info, size:%.2f Kb, elapsed time:%.2f ms, %s", pTsdbReader, numOfTables,
            (sizeof(STableBlockScanInfo) * numOfTables) / 1024.0, pTsdbReader->cost.createScanInfoList,
            pTsdbReader->idStr);

  return pTableMap;
}

static void resetAllDataBlockScanInfo(SHashObj* pTableMap, int64_t ts) {
  STableBlockScanInfo** p = NULL;
  while ((p = taosHashIterate(pTableMap, p)) != NULL) {
    STableBlockScanInfo* pInfo = *(STableBlockScanInfo**)p;

    pInfo->iterInit = false;
    pInfo->iter.hasVal = false;
    pInfo->iiter.hasVal = false;

    if (pInfo->iter.iter != NULL) {
      pInfo->iter.iter = tsdbTbDataIterDestroy(pInfo->iter.iter);
    }

    if (pInfo->iiter.iter != NULL) {
      pInfo->iiter.iter = tsdbTbDataIterDestroy(pInfo->iiter.iter);
    }

    pInfo->delSkyline = taosArrayDestroy(pInfo->delSkyline);
    pInfo->lastKey = ts;
  }
}

static void clearBlockScanInfo(STableBlockScanInfo* p) {
  p->iterInit = false;

  p->iter.hasVal = false;
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

static void destroyAllBlockScanInfo(SHashObj* pTableMap) {
  void* p = NULL;
  while ((p = taosHashIterate(pTableMap, p)) != NULL) {
    clearBlockScanInfo(*(STableBlockScanInfo**)p);
  }

  taosHashCleanup(pTableMap);
}

static bool isEmptyQueryTimeWindow(STimeWindow* pWindow) { return pWindow->skey > pWindow->ekey; }

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

// init file iterator
static int32_t initFilesetIterator(SFilesetIter* pIter, SArray* aDFileSet, STsdbReader* pReader) {
  size_t numOfFileset = taosArrayGetSize(aDFileSet);

  pIter->index = ASCENDING_TRAVERSE(pReader->order) ? -1 : numOfFileset;
  pIter->order = pReader->order;
  pIter->pFileList = aDFileSet;
  pIter->numOfFiles = numOfFileset;

  if (pIter->pLastBlockReader == NULL) {
    pIter->pLastBlockReader = taosMemoryCalloc(1, sizeof(struct SLastBlockReader));
    if (pIter->pLastBlockReader == NULL) {
      int32_t code = TSDB_CODE_OUT_OF_MEMORY;
      tsdbError("failed to prepare the last block iterator, since:%s %s", tstrerror(code), pReader->idStr);
      return code;
    }
  }

  SLastBlockReader* pLReader = pIter->pLastBlockReader;
  pLReader->order = pReader->order;
  pLReader->window = pReader->window;
  pLReader->verRange = pReader->verRange;

  pLReader->uid = 0;
  tMergeTreeClose(&pLReader->mergeTree);

  if (pLReader->pInfo == NULL) {
    // here we ignore the first column, which is always be the primary timestamp column
    pLReader->pInfo =
        tCreateLastBlockLoadInfo(pReader->pSchema, &pReader->suppInfo.colId[1], pReader->suppInfo.numOfCols - 1);
    if (pLReader->pInfo == NULL) {
      tsdbDebug("init fileset iterator failed, code:%s %s", tstrerror(terrno), pReader->idStr);
      return terrno;
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

  SIOCostSummary* pSum = &pReader->cost;
  getLastBlockLoadInfo(pIter->pLastBlockReader->pInfo, &pSum->lastBlockLoad, &pReader->cost.lastBlockLoadTime);

  pIter->pLastBlockReader->uid = 0;
  tMergeTreeClose(&pIter->pLastBlockReader->mergeTree);
  resetLastBlockLoadInfo(pIter->pLastBlockReader->pInfo);

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
    SColumnInfoData colInfo = {0};
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
                                SSDataBlock* pResBlock, const char* idstr) {
  int32_t      code = 0;
  int8_t       level = 0;
  STsdbReader* pReader = (STsdbReader*)taosMemoryCalloc(1, sizeof(*pReader));
  if (pReader == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  if (VND_IS_TSMA(pVnode)) {
    tsdbDebug("vgId:%d, tsma is selected to query, %s", TD_VID(pVnode), idstr);
  }

  initReaderStatus(&pReader->status);

  pReader->pTsdb = getTsdbByRetentions(pVnode, pCond->twindows.skey, pVnode->config.tsdbCfg.retentions, idstr, &level);
  pReader->suid = pCond->suid;
  pReader->order = pCond->order;
  pReader->capacity = capacity;
  pReader->pResBlock = pResBlock;
  pReader->idStr = (idstr != NULL) ? strdup(idstr) : NULL;
  pReader->verRange = getQueryVerRange(pVnode, pCond, level);
  pReader->type = pCond->type;
  pReader->window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows);
  pReader->blockInfoBuf.numPerBucket = 1000;  // 1000 tables per bucket

  if (pReader->pResBlock == NULL) {
    pReader->freeBlock = true;
    pReader->pResBlock = createResBlock(pCond, pReader->capacity);
    if (pReader->pResBlock == NULL) {
      code = terrno;
      goto _end;
    }
  }

  if (pCond->numOfCols <= 0) {
    tsdbError("vgId:%d, invalid column number %d in query cond, %s", TD_VID(pVnode), pCond->numOfCols, idstr);
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  // allocate buffer in order to load data blocks from file
  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;
  pSup->pColAgg = taosArrayInit(pCond->numOfCols, sizeof(SColumnDataAgg));
  if (pSup->pColAgg == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  pSup->tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;

  code = tBlockDataCreate(&pReader->status.fileBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _end;
  }

  setColumnIdSlotList(&pReader->suppInfo, pCond->colList, pCond->pSlotList, pCond->numOfCols);

  *ppReader = pReader;
  return code;

_end:
  tsdbReaderClose(pReader);
  *ppReader = NULL;
  return code;
}

static int32_t doLoadBlockIndex(STsdbReader* pReader, SDataFReader* pFileReader, SArray* pIndexList) {
  // SArray* aBlockIdx = taosArrayInit(8, sizeof(SBlockIdx));

  int64_t st = taosGetTimestampUs();
  // int32_t code = tsdbReadBlockIdx(pFileReader, aBlockIdx);
  LRUHandle* handle = NULL;
  int32_t    code = tsdbCacheGetBlockIdx(pFileReader->pTsdb->biCache, pFileReader, &handle);
  if (code != TSDB_CODE_SUCCESS || handle == NULL) {
    goto _end;
  }

  SArray* aBlockIdx = (SArray*)taosLRUCacheValue(pFileReader->pTsdb->biCache, handle);
  size_t  num = taosArrayGetSize(aBlockIdx);
  if (num == 0) {
    tsdbBICacheRelease(pFileReader->pTsdb->biCache, handle);
    // taosArrayDestroy(aBlockIdx);
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

    STableBlockScanInfo* pScanInfo = *(STableBlockScanInfo**)p;
    if (pScanInfo->pBlockList == NULL) {
      pScanInfo->pBlockList = taosArrayInit(4, sizeof(SBlockIndex));
    }

    taosArrayPush(pIndexList, pBlockIdx);
  }

  int64_t et2 = taosGetTimestampUs();
  tsdbDebug("load block index for %d tables completed, elapsed time:%.2f ms, set blockIdx:%.2f ms, size:%.2f Kb %s",
            (int32_t)num, (et1 - st) / 1000.0, (et2 - et1) / 1000.0, num * sizeof(SBlockIdx) / 1024.0, pReader->idStr);

  pReader->cost.headFileLoadTime += (et1 - st) / 1000.0;

_end:
  // taosArrayDestroy(aBlockIdx);
  tsdbBICacheRelease(pFileReader->pTsdb->biCache, handle);
  return code;
}

static void cleanupTableScanInfo(SHashObj* pTableMap) {
  STableBlockScanInfo** px = NULL;
  while (1) {
    px = taosHashIterate(pTableMap, px);
    if (px == NULL) {
      break;
    }

    // reset the index in last block when handing a new file
    tMapDataClear(&(*px)->mapData);
    taosArrayClear((*px)->pBlockList);
  }
}

static int32_t doLoadFileBlock(STsdbReader* pReader, SArray* pIndexList, SBlockNumber* pBlockNum) {
  int32_t numOfQTable = 0;
  size_t  sizeInDisk = 0;
  size_t  numOfTables = taosArrayGetSize(pIndexList);

  int64_t st = taosGetTimestampUs();
  cleanupTableScanInfo(pReader->status.pTableMap);

  for (int32_t i = 0; i < numOfTables; ++i) {
    SBlockIdx* pBlockIdx = taosArrayGet(pIndexList, i);

    STableBlockScanInfo* pScanInfo =
        *(STableBlockScanInfo**)taosHashGet(pReader->status.pTableMap, &pBlockIdx->uid, sizeof(int64_t));

    tMapDataReset(&pScanInfo->mapData);
    tsdbReadDataBlk(pReader->pFileReader, pBlockIdx, &pScanInfo->mapData);
    taosArrayEnsureCap(pScanInfo->pBlockList, pScanInfo->mapData.nItem);

    sizeInDisk += pScanInfo->mapData.nData;

    SDataBlk block = {0};
    for (int32_t j = 0; j < pScanInfo->mapData.nItem; ++j) {
      tGetDataBlk(pScanInfo->mapData.pData + pScanInfo->mapData.aOffset[j], &block);

      // 1. time range check
      if (block.minKey.ts > pReader->window.ekey || block.maxKey.ts < pReader->window.skey) {
        continue;
      }

      // 2. version range check
      if (block.minVer > pReader->verRange.maxVer || block.maxVer < pReader->verRange.minVer) {
        continue;
      }

      SBlockIndex bIndex = {.ordinalIndex = j, .inFileOffset = block.aSubBlock->offset};
      bIndex.window = (STimeWindow){.skey = block.minKey.ts, .ekey = block.maxKey.ts};

      void* p1 = taosArrayPush(pScanInfo->pBlockList, &bIndex);
      if (p1 == NULL) {
        tMapDataClear(&pScanInfo->mapData);
        return TSDB_CODE_OUT_OF_MEMORY;
      }

      pBlockNum->numOfBlocks += 1;
    }

    if (pScanInfo->pBlockList != NULL && taosArrayGetSize(pScanInfo->pBlockList) > 0) {
      numOfQTable += 1;
    }
  }

  pBlockNum->numOfLastFiles = pReader->pFileReader->pSet->nSttF;
  int32_t total = pBlockNum->numOfLastFiles + pBlockNum->numOfBlocks;

  double el = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug(
      "load block of %ld tables completed, blocks:%d in %d tables, last-files:%d, block-info-size:%.2f Kb, elapsed "
      "time:%.2f ms %s",
      numOfTables, pBlockNum->numOfBlocks, numOfQTable, pBlockNum->numOfLastFiles, sizeInDisk / 1000.0, el,
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
    if (!COL_VAL_IS_VALUE(pColVal)) {
      colDataAppendNULL(pColInfoData, rowIndex);
    } else {
      varDataSetLen(pSup->buildBuf[colIndex], pColVal->value.nData);
      ASSERT(pColVal->value.nData <= pColInfoData->info.bytes);
      if (pColVal->value.nData > 0) {  // pData may be null, if nData is 0
        memcpy(varDataVal(pSup->buildBuf[colIndex]), pColVal->value.pData, pColVal->value.nData);
      }

      colDataAppend(pColInfoData, rowIndex, pSup->buildBuf[colIndex], false);
    }
  } else {
    colDataAppend(pColInfoData, rowIndex, (const char*)&pColVal->value, !COL_VAL_IS_VALUE(pColVal));
  }
}

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter) {
  size_t num = taosArrayGetSize(pBlockIter->blockList);
  if (num == 0) {
    ASSERT(pBlockIter->numOfBlocks == num);
    return NULL;
  }

  SFileDataBlockInfo* pBlockInfo = taosArrayGet(pBlockIter->blockList, pBlockIter->index);
  return pBlockInfo;
}

static SDataBlk* getCurrentBlock(SDataBlockIter* pBlockIter) { return &pBlockIter->block; }

static int doBinarySearchKey(TSKEY* keyList, int num, int pos, TSKEY key, int order) {
  // start end position
  int s, e;
  s = pos;

  // check
  assert(pos >= 0 && pos < num);
  assert(num > 0);

  if (order == TSDB_ORDER_ASC) {
    // find the first position which is smaller than the key
    e = num - 1;
    if (key < keyList[pos]) return -1;
    while (1) {
      // check can return
      if (key >= keyList[e]) return e;
      if (key <= keyList[s]) return s;
      if (e - s <= 1) return s;

      // change start or end position
      int mid = s + (e - s + 1) / 2;
      if (keyList[mid] > key)
        e = mid;
      else if (keyList[mid] < key)
        s = mid;
      else
        return mid;
    }
  } else {  // DESC
    // find the first position which is bigger than the key
    e = 0;
    if (key > keyList[pos]) return -1;
    while (1) {
      // check can return
      if (key <= keyList[e]) return e;
      if (key >= keyList[s]) return s;
      if (s - e <= 1) return s;

      // change start or end position
      int mid = s - (s - e + 1) / 2;
      if (keyList[mid] < key)
        e = mid;
      else if (keyList[mid] > key)
        s = mid;
      else
        return mid;
    }
  }
}

static int32_t getEndPosInDataBlock(STsdbReader* pReader, SBlockData* pBlockData, SDataBlk* pBlock, int32_t pos) {
  // NOTE: reverse the order to find the end position in data block
  int32_t endPos = -1;
  bool    asc = ASCENDING_TRAVERSE(pReader->order);

  if (asc && pReader->window.ekey >= pBlock->maxKey.ts) {
    endPos = pBlock->nRow - 1;
  } else if (!asc && pReader->window.skey <= pBlock->minKey.ts) {
    endPos = 0;
  } else {
    int64_t key = asc ? pReader->window.ekey : pReader->window.skey;
    endPos = doBinarySearchKey(pBlockData->aTSKEY, pBlock->nRow, pos, key, pReader->order);
  }

  return endPos;
}

static void copyPrimaryTsCol(const SBlockData* pBlockData, SFileBlockDumpInfo* pDumpInfo, SColumnInfoData* pColData,
                             int32_t dumpedRows, bool asc) {
  if (asc) {
    memcpy(pColData->pData, &pBlockData->aTSKEY[pDumpInfo->rowIndex], dumpedRows * sizeof(int64_t));
  } else {
    int32_t startIndex = pDumpInfo->rowIndex - dumpedRows + 1;
    memcpy(pColData->pData, &pBlockData->aTSKEY[startIndex], dumpedRows * sizeof(int64_t));

    // todo: opt perf by extract the loop
    // reverse the array list
    int32_t  mid = dumpedRows >> 1u;
    int64_t* pts = (int64_t*)pColData->pData;
    for (int32_t j = 0; j < mid; ++j) {
      int64_t t = pts[j];
      pts[j] = pts[dumpedRows - j - 1];
      pts[dumpedRows - j - 1] = t;
    }
  }
}

// a faster version of copy procedure.
static void copyNumericCols(const SColData* pData, SFileBlockDumpInfo* pDumpInfo, SColumnInfoData* pColData,
                            int32_t dumpedRows, bool asc) {
  uint8_t* p = NULL;
  if (asc) {
    p = pData->pData + tDataTypes[pData->type].bytes * pDumpInfo->rowIndex;
  } else {
    int32_t startIndex = pDumpInfo->rowIndex - dumpedRows + 1;
    p = pData->pData + tDataTypes[pData->type].bytes * startIndex;
  }

  int32_t step = asc ? 1 : -1;

  // make sure it is aligned to 8bit, the allocated memory address is aligned to 256bit
  //  ASSERT((((uint64_t)pColData->pData) & (0x8 - 1)) == 0);

  // 1. copy data in a batch model
  memcpy(pColData->pData, p, dumpedRows * tDataTypes[pData->type].bytes);

  // 2. reverse the array list in case of descending order scan data block
  if (!asc) {
    switch (pColData->info.type) {
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_DOUBLE:
      case TSDB_DATA_TYPE_BIGINT:
      case TSDB_DATA_TYPE_UBIGINT: {
        int32_t  mid = dumpedRows >> 1u;
        int64_t* pts = (int64_t*)pColData->pData;
        for (int32_t j = 0; j < mid; ++j) {
          int64_t t = pts[j];
          pts[j] = pts[dumpedRows - j - 1];
          pts[dumpedRows - j - 1] = t;
        }
        break;
      }

      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT:
      case TSDB_DATA_TYPE_UTINYINT: {
        int32_t mid = dumpedRows >> 1u;
        int8_t* pts = (int8_t*)pColData->pData;
        for (int32_t j = 0; j < mid; ++j) {
          int8_t t = pts[j];
          pts[j] = pts[dumpedRows - j - 1];
          pts[dumpedRows - j - 1] = t;
        }
        break;
      }

      case TSDB_DATA_TYPE_SMALLINT:
      case TSDB_DATA_TYPE_USMALLINT: {
        int32_t  mid = dumpedRows >> 1u;
        int16_t* pts = (int16_t*)pColData->pData;
        for (int32_t j = 0; j < mid; ++j) {
          int64_t t = pts[j];
          pts[j] = pts[dumpedRows - j - 1];
          pts[dumpedRows - j - 1] = t;
        }
        break;
      }

      case TSDB_DATA_TYPE_FLOAT:
      case TSDB_DATA_TYPE_INT:
      case TSDB_DATA_TYPE_UINT: {
        int32_t  mid = dumpedRows >> 1u;
        int32_t* pts = (int32_t*)pColData->pData;
        for (int32_t j = 0; j < mid; ++j) {
          int32_t t = pts[j];
          pts[j] = pts[dumpedRows - j - 1];
          pts[dumpedRows - j - 1] = t;
        }
        break;
      }
    }
  }

  // 3. if the  null value exists, check items one-by-one
  if (pData->flag != HAS_VALUE) {
    int32_t rowIndex = 0;

    for (int32_t j = pDumpInfo->rowIndex; rowIndex < dumpedRows; j += step, rowIndex++) {
      uint8_t v = tColDataGetBitValue(pData, j);
      if (v == 0 || v == 1) {
        colDataSetNull_f(pColData->nullbitmap, rowIndex);
        pColData->hasNull = true;
      }
    }
  }
}

static int32_t copyBlockDataToSDataBlock(STsdbReader* pReader) {
  SReaderStatus*      pStatus = &pReader->status;
  SDataBlockIter*     pBlockIter = &pStatus->blockIter;
  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  SBlockData*         pBlockData = &pStatus->fileBlockData;
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SDataBlk*           pBlock = getCurrentBlock(pBlockIter);
  SSDataBlock*        pResBlock = pReader->pResBlock;
  int32_t             numOfOutputCols = pSupInfo->numOfCols;

  SColVal cv = {0};
  int64_t st = taosGetTimestampUs();
  bool    asc = ASCENDING_TRAVERSE(pReader->order);
  int32_t step = asc ? 1 : -1;

  // no data exists, return directly.
  if (pBlockData->nRow == 0 || pBlockData->aTSKEY == 0) {
    tsdbWarn("%p no need to copy since no data in blockData, table uid:%" PRIu64 " has been dropped, %s", pReader, pBlockInfo->uid,
              pReader->idStr);
    pResBlock->info.rows = 0;
    return 0;
  }

  if ((pDumpInfo->rowIndex == 0 && asc) || (pDumpInfo->rowIndex == pBlock->nRow - 1 && (!asc))) {
    if (asc && pReader->window.skey <= pBlock->minKey.ts) {
      // pDumpInfo->rowIndex = 0;
    } else if (!asc && pReader->window.ekey >= pBlock->maxKey.ts) {
      // pDumpInfo->rowIndex = pBlock->nRow - 1;
    } else {  // find the appropriate the start position in current block, and set it to be the current rowIndex
      int32_t pos = asc ? pBlock->nRow - 1 : 0;
      int32_t order = asc ? TSDB_ORDER_DESC : TSDB_ORDER_ASC;
      int64_t key = asc ? pReader->window.skey : pReader->window.ekey;
      pDumpInfo->rowIndex = doBinarySearchKey(pBlockData->aTSKEY, pBlock->nRow, pos, key, order);

      if (pDumpInfo->rowIndex < 0) {
        tsdbError(
            "%p failed to locate the start position in current block, global index:%d, table index:%d, brange:%" PRId64
            "-%" PRId64 ", minVer:%" PRId64 ", maxVer:%" PRId64 " %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, pBlock->minVer,
            pBlock->maxVer, pReader->idStr);
        return TSDB_CODE_INVALID_PARA;
      }
    }
  }

  // time window check
  int32_t endIndex = getEndPosInDataBlock(pReader, pBlockData, pBlock, pDumpInfo->rowIndex);
  if (endIndex == -1) {
    setBlockAllDumped(pDumpInfo, pReader->window.ekey, pReader->order);
    return TSDB_CODE_SUCCESS;
  }

  endIndex += step;
  int32_t dumpedRows = asc ? (endIndex - pDumpInfo->rowIndex) : (pDumpInfo->rowIndex - endIndex);
  if (dumpedRows > pReader->capacity) {  // output buffer check
    dumpedRows = pReader->capacity;
  }

  int32_t i = 0;
  int32_t rowIndex = 0;

  SColumnInfoData* pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
  if (pSupInfo->colId[i] == PRIMARYKEY_TIMESTAMP_COL_ID) {
    copyPrimaryTsCol(pBlockData, pDumpInfo, pColData, dumpedRows, asc);
    i += 1;
  }

  int32_t colIndex = 0;
  int32_t num = pBlockData->nColData;
  while (i < numOfOutputCols && colIndex < num) {
    rowIndex = 0;

    SColData* pData = tBlockDataGetColDataByIdx(pBlockData, colIndex);
    if (pData->cid < pSupInfo->colId[i]) {
      colIndex += 1;
    } else if (pData->cid == pSupInfo->colId[i]) {
      pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);

      if (pData->flag == HAS_NONE || pData->flag == HAS_NULL || pData->flag == (HAS_NULL | HAS_NONE)) {
        colDataAppendNNULL(pColData, 0, dumpedRows);
      } else {
        if (IS_MATHABLE_TYPE(pColData->info.type)) {
          copyNumericCols(pData, pDumpInfo, pColData, dumpedRows, asc);
        } else {  // varchar/nchar type
          for (int32_t j = pDumpInfo->rowIndex; rowIndex < dumpedRows; j += step) {
            tColDataGetValue(pData, j, &cv);
            doCopyColVal(pColData, rowIndex++, i, &cv, pSupInfo);
          }
        }
      }

      colIndex += 1;
      i += 1;
    } else {  // the specified column does not exist in file block, fill with null data
      pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
      colDataAppendNNULL(pColData, 0, dumpedRows);
      i += 1;
    }
  }

  // fill the mis-matched columns with null value
  while (i < numOfOutputCols) {
    pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    colDataAppendNNULL(pColData, 0, dumpedRows);
    i += 1;
  }

  pResBlock->info.dataLoad = 1;
  pResBlock->info.rows = dumpedRows;
  pDumpInfo->rowIndex += step * dumpedRows;

  // check if current block are all handled
  if (pDumpInfo->rowIndex >= 0 && pDumpInfo->rowIndex < pBlock->nRow) {
    int64_t ts = pBlockData->aTSKEY[pDumpInfo->rowIndex];
    if (outOfTimeWindow(ts, &pReader->window)) {  // the remain data has out of query time window, ignore current block
      setBlockAllDumped(pDumpInfo, ts, pReader->order);
    }
  } else {
    int64_t ts = asc ? pBlock->maxKey.ts : pBlock->minKey.ts;
    setBlockAllDumped(pDumpInfo, ts, pReader->order);
  }

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  pReader->cost.blockLoadTime += elapsedTime;

  int32_t unDumpedRows = asc ? pBlock->nRow - pDumpInfo->rowIndex : pDumpInfo->rowIndex + 1;
  tsdbDebug("%p copy file block to sdatablock, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, remain:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", uid:%" PRIu64 " elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlock->minKey.ts, pBlock->maxKey.ts, dumpedRows,
            unDumpedRows, pBlock->minVer, pBlock->maxVer, pBlockInfo->uid, elapsedTime, pReader->idStr);

  return TSDB_CODE_SUCCESS;
}

static int32_t doLoadFileBlockData(STsdbReader* pReader, SDataBlockIter* pBlockIter, SBlockData* pBlockData,
                                   uint64_t uid) {
  int32_t code = 0;
  int64_t st = taosGetTimestampUs();

  tBlockDataReset(pBlockData);
  STSchema* pSchema = getLatestTableSchema(pReader, uid);
  if (pSchema == NULL) {
    tsdbDebug("%p table uid:%"PRIu64" has been dropped, no data existed, %s", pReader, uid, pReader->idStr);
    return code;
  }

  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;
  TABLEID tid = {.suid = pReader->suid, .uid = uid};
  code = tBlockDataInit(pBlockData, &tid, pSchema, &pSup->colId[1], pSup->numOfCols - 1);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  SDataBlk* pBlock = getCurrentBlock(pBlockIter);
  code = tsdbReadDataBlock(pReader->pFileReader, pBlock, pBlockData);
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

static int32_t doSetCurrentBlock(SDataBlockIter* pBlockIter, const char* idStr) {
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  if (pBlockInfo != NULL) {
    STableBlockScanInfo** pScanInfo = taosHashGet(pBlockIter->pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
    if (pScanInfo == NULL) {
      tsdbError("failed to locate the uid:%" PRIu64 " in query table uid list, %s", pBlockInfo->uid, idStr);
      return TSDB_CODE_INVALID_PARA;
    }

    SBlockIndex* pIndex = taosArrayGet((*pScanInfo)->pBlockList, pBlockInfo->tbBlockIdx);
    tMapDataGetItemByIdx(&(*pScanInfo)->mapData, pIndex->ordinalIndex, &pBlockIter->block, tGetDataBlk);
  }

#if 0
  qDebug("check file block, table uid:%"PRIu64" index:%d offset:%"PRId64", ", pScanInfo->uid, *mapDataIndex, pBlockIter->block.aSubBlock[0].offset);
#endif

  return TSDB_CODE_SUCCESS;
}

static int32_t initBlockIterator(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t numOfBlocks) {
  bool asc = ASCENDING_TRAVERSE(pReader->order);

  SBlockOrderSupporter sup = {0};
  pBlockIter->numOfBlocks = numOfBlocks;
  taosArrayClear(pBlockIter->blockList);
  pBlockIter->pTableMap = pReader->status.pTableMap;

  // access data blocks according to the offset of each block in asc/desc order.
  int32_t numOfTables = (int32_t)taosHashGetSize(pReader->status.pTableMap);

  int64_t st = taosGetTimestampUs();
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

    STableBlockScanInfo* pTableScanInfo = *(STableBlockScanInfo**)ptr;
    if (pTableScanInfo->pBlockList == NULL || taosArrayGetSize(pTableScanInfo->pBlockList) == 0) {
      continue;
    }

    size_t num = taosArrayGetSize(pTableScanInfo->pBlockList);
    sup.numOfBlocksPerTable[sup.numOfTables] = num;

    char* buf = taosMemoryMalloc(sizeof(SBlockOrderWrapper) * num);
    if (buf == NULL) {
      cleanupBlockOrderSupporter(&sup);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    sup.pDataBlockInfo[sup.numOfTables] = (SBlockOrderWrapper*)buf;

    for (int32_t k = 0; k < num; ++k) {
      SBlockIndex* pIndex = taosArrayGet(pTableScanInfo->pBlockList, k);
      sup.pDataBlockInfo[sup.numOfTables][k] =
          (SBlockOrderWrapper){.uid = pTableScanInfo->uid, .offset = pIndex->inFileOffset};
      cnt++;
    }

    sup.numOfTables += 1;
  }

  if (numOfBlocks != cnt && sup.numOfTables != numOfTables) {
    cleanupBlockOrderSupporter(&sup);
    return TSDB_CODE_INVALID_PARA;
  }

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
    doSetCurrentBlock(pBlockIter, pReader->idStr);
    return TSDB_CODE_SUCCESS;
  }

  tsdbDebug("%p create data blocks info struct completed, %d blocks in %d tables %s", pReader, cnt, sup.numOfTables,
            pReader->idStr);

  SMultiwayMergeTreeInfo* pTree = NULL;

  uint8_t ret = tMergeTreeCreate(&pTree, sup.numOfTables, &sup, fileDataBlockOrderCompar);
  if (ret != TSDB_CODE_SUCCESS) {
    cleanupBlockOrderSupporter(&sup);
    return TSDB_CODE_OUT_OF_MEMORY;
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
  tsdbDebug("%p %d data blocks access order completed, elapsed time:%.2f ms %s", pReader, numOfBlocks,
            (et - st) / 1000.0, pReader->idStr);
  cleanupBlockOrderSupporter(&sup);
  taosMemoryFree(pTree);

  pBlockIter->index = asc ? 0 : (numOfBlocks - 1);
  doSetCurrentBlock(pBlockIter, pReader->idStr);

  return TSDB_CODE_SUCCESS;
}

static bool blockIteratorNext(SDataBlockIter* pBlockIter, const char* idStr) {
  bool asc = ASCENDING_TRAVERSE(pBlockIter->order);

  int32_t step = asc ? 1 : -1;
  if ((pBlockIter->index >= pBlockIter->numOfBlocks - 1 && asc) || (pBlockIter->index <= 0 && (!asc))) {
    return false;
  }

  pBlockIter->index += step;
  doSetCurrentBlock(pBlockIter, idStr);

  return true;
}

/**
 * This is an two rectangles overlap cases.
 */
static int32_t dataBlockPartiallyRequired(STimeWindow* pWindow, SVersionRange* pVerRange, SDataBlk* pBlock) {
  return (pWindow->ekey < pBlock->maxKey.ts && pWindow->ekey >= pBlock->minKey.ts) ||
         (pWindow->skey > pBlock->minKey.ts && pWindow->skey <= pBlock->maxKey.ts) ||
         (pVerRange->minVer > pBlock->minVer && pVerRange->minVer <= pBlock->maxVer) ||
         (pVerRange->maxVer < pBlock->maxVer && pVerRange->maxVer >= pBlock->minVer);
}

static bool getNeighborBlockOfSameTable(SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pTableBlockScanInfo,
                                        int32_t* nextIndex, int32_t order, SBlockIndex* pBlockIndex) {
  bool asc = ASCENDING_TRAVERSE(order);
  if (asc && pBlockInfo->tbBlockIdx >= taosArrayGetSize(pTableBlockScanInfo->pBlockList) - 1) {
    return false;
  }

  if (!asc && pBlockInfo->tbBlockIdx == 0) {
    return false;
  }

  int32_t step = asc ? 1 : -1;
  *nextIndex = pBlockInfo->tbBlockIdx + step;
  *pBlockIndex = *(SBlockIndex*)taosArrayGet(pTableBlockScanInfo->pBlockList, *nextIndex);
  //  tMapDataGetItemByIdx(&pTableBlockScanInfo->mapData, pIndex->ordinalIndex, pBlock, tGetDataBlk);
  return true;
}

static int32_t findFileBlockInfoIndex(SDataBlockIter* pBlockIter, SFileDataBlockInfo* pFBlockInfo) {
  int32_t step = ASCENDING_TRAVERSE(pBlockIter->order) ? 1 : -1;
  int32_t index = pBlockIter->index;

  while (index < pBlockIter->numOfBlocks && index >= 0) {
    SFileDataBlockInfo* pFBlock = taosArrayGet(pBlockIter->blockList, index);
    if (pFBlock->uid == pFBlockInfo->uid && pFBlock->tbBlockIdx == pFBlockInfo->tbBlockIdx) {
      return index;
    }

    index += step;
  }

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

  doSetCurrentBlock(pBlockIter, "");
  return TSDB_CODE_SUCCESS;
}

// todo: this attribute could be acquired during extractin the global ordered block list.
static bool overlapWithNeighborBlock(SDataBlk* pBlock, SBlockIndex* pNeighborBlockIndex, int32_t order) {
  // it is the last block in current file, no chance to overlap with neighbor blocks.
  if (ASCENDING_TRAVERSE(order)) {
    return pBlock->maxKey.ts == pNeighborBlockIndex->window.skey;
  } else {
    return pBlock->minKey.ts == pNeighborBlockIndex->window.ekey;
  }
}

static bool bufferDataInFileBlockGap(int32_t order, TSDBKEY key, SDataBlk* pBlock) {
  bool ascScan = ASCENDING_TRAVERSE(order);

  return (ascScan && (key.ts != TSKEY_INITIAL_VAL && key.ts <= pBlock->minKey.ts)) ||
         (!ascScan && (key.ts != TSKEY_INITIAL_VAL && key.ts >= pBlock->maxKey.ts));
}

static bool keyOverlapFileBlock(TSDBKEY key, SDataBlk* pBlock, SVersionRange* pVerRange) {
  return (key.ts >= pBlock->minKey.ts && key.ts <= pBlock->maxKey.ts) && (pBlock->maxVer >= pVerRange->minVer) &&
         (pBlock->minVer <= pVerRange->maxVer);
}

static bool doCheckforDatablockOverlap(STableBlockScanInfo* pBlockScanInfo, const SDataBlk* pBlock,
                                       int32_t startIndex) {
  size_t num = taosArrayGetSize(pBlockScanInfo->delSkyline);

  for (int32_t i = startIndex; i < num; i += 1) {
    TSDBKEY* p = taosArrayGet(pBlockScanInfo->delSkyline, i);
    if (p->ts >= pBlock->minKey.ts && p->ts <= pBlock->maxKey.ts) {
      if (p->version >= pBlock->minVer) {
        return true;
      }
    } else if (p->ts < pBlock->minKey.ts) {  // p->ts < pBlock->minKey.ts
      if (p->version >= pBlock->minVer) {
        if (i < num - 1) {
          TSDBKEY* pnext = taosArrayGet(pBlockScanInfo->delSkyline, i + 1);
          if (pnext->ts >= pBlock->minKey.ts) {
            return true;
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

static bool overlapWithDelSkyline(STableBlockScanInfo* pBlockScanInfo, const SDataBlk* pBlock, int32_t order) {
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
    return doCheckforDatablockOverlap(pBlockScanInfo, pBlock, pBlockScanInfo->fileDelIndex);
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

    return doCheckforDatablockOverlap(pBlockScanInfo, pBlock, index);
  }
}

typedef struct {
  bool overlapWithNeighborBlock;
  bool hasDupTs;
  bool overlapWithDelInfo;
  bool overlapWithLastBlock;
  bool overlapWithKeyInBuf;
  bool partiallyRequired;
  bool moreThanCapcity;
} SDataBlockToLoadInfo;

static void getBlockToLoadInfo(SDataBlockToLoadInfo* pInfo, SFileDataBlockInfo* pBlockInfo, SDataBlk* pBlock,
                               STableBlockScanInfo* pScanInfo, TSDBKEY keyInBuf, SLastBlockReader* pLastBlockReader,
                               STsdbReader* pReader) {
  int32_t     neighborIndex = 0;
  SBlockIndex bIndex = {0};

  bool hasNeighbor = getNeighborBlockOfSameTable(pBlockInfo, pScanInfo, &neighborIndex, pReader->order, &bIndex);

  // overlap with neighbor
  if (hasNeighbor) {
    pInfo->overlapWithNeighborBlock = overlapWithNeighborBlock(pBlock, &bIndex, pReader->order);
  }

  // has duplicated ts of different version in this block
  pInfo->hasDupTs = (pBlock->nSubBlock == 1) ? pBlock->hasDup : true;
  pInfo->overlapWithDelInfo = overlapWithDelSkyline(pScanInfo, pBlock, pReader->order);

  if (hasDataInLastBlock(pLastBlockReader)) {
    int64_t tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
    pInfo->overlapWithLastBlock = !(pBlock->maxKey.ts < tsLast || pBlock->minKey.ts > tsLast);
  }

  pInfo->moreThanCapcity = pBlock->nRow > pReader->capacity;
  pInfo->partiallyRequired = dataBlockPartiallyRequired(&pReader->window, &pReader->verRange, pBlock);
  pInfo->overlapWithKeyInBuf = keyOverlapFileBlock(keyInBuf, pBlock, &pReader->verRange);
}

// 1. the version of all rows should be less than the endVersion
// 2. current block should not overlap with next neighbor block
// 3. current timestamp should not be overlap with each other
// 4. output buffer should be large enough to hold all rows in current block
// 5. delete info should not overlap with current block data
// 6. current block should not contain the duplicated ts
static bool fileBlockShouldLoad(STsdbReader* pReader, SFileDataBlockInfo* pBlockInfo, SDataBlk* pBlock,
                                STableBlockScanInfo* pScanInfo, TSDBKEY keyInBuf, SLastBlockReader* pLastBlockReader) {
  SDataBlockToLoadInfo info = {0};
  getBlockToLoadInfo(&info, pBlockInfo, pBlock, pScanInfo, keyInBuf, pLastBlockReader, pReader);

  bool loadDataBlock =
      (info.overlapWithNeighborBlock || info.hasDupTs || info.partiallyRequired || info.overlapWithKeyInBuf ||
       info.moreThanCapcity || info.overlapWithDelInfo || info.overlapWithLastBlock);

  // log the reason why load the datablock for profile
  if (loadDataBlock) {
    tsdbDebug("%p uid:%" PRIu64 " need to load the datablock, overlapneighbor:%d, hasDup:%d, partiallyRequired:%d, "
              "overlapWithKey:%d, greaterThanBuf:%d, overlapWithDel:%d, overlapWithlastBlock:%d, %s",
              pReader, pBlockInfo->uid, info.overlapWithNeighborBlock, info.hasDupTs, info.partiallyRequired,
              info.overlapWithKeyInBuf, info.moreThanCapcity, info.overlapWithDelInfo, info.overlapWithLastBlock,
              pReader->idStr);
  }

  return loadDataBlock;
}

static bool isCleanFileDataBlock(STsdbReader* pReader, SFileDataBlockInfo* pBlockInfo, SDataBlk* pBlock,
                                 STableBlockScanInfo* pScanInfo, TSDBKEY keyInBuf, SLastBlockReader* pLastBlockReader) {
  SDataBlockToLoadInfo info = {0};
  getBlockToLoadInfo(&info, pBlockInfo, pBlock, pScanInfo, keyInBuf, pLastBlockReader, pReader);
  bool isCleanFileBlock = !(info.overlapWithNeighborBlock || info.hasDupTs || info.overlapWithKeyInBuf ||
                            info.overlapWithDelInfo || info.overlapWithLastBlock);
  return isCleanFileBlock;
}

static int32_t buildDataBlockFromBuf(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, int64_t endKey) {
  if (!(pBlockScanInfo->iiter.hasVal || pBlockScanInfo->iter.hasVal)) {
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pBlock = pReader->pResBlock;

  int64_t st = taosGetTimestampUs();
  int32_t code = buildDataBlockFromBufImpl(pBlockScanInfo, endKey, pReader->capacity, pReader);

  blockDataUpdateTsWindow(pBlock, pReader->suppInfo.slotId[0]);
  pBlock->info.id.uid = pBlockScanInfo->uid;

  setComposedBlockFlag(pReader, true);

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("%p build data block from cache completed, elapsed time:%.2f ms, numOfRows:%d, brange:%" PRId64
            " - %" PRId64 ", uid:%"PRIu64",  %s",
            pReader, elapsedTime, pBlock->info.rows, pBlock->info.window.skey, pBlock->info.window.ekey,
            pBlockScanInfo->uid, pReader->idStr);

  pReader->cost.buildmemBlock += elapsedTime;
  return code;
}

static bool tryCopyDistinctRowFromFileBlock(STsdbReader* pReader, SBlockData* pBlockData, int64_t key,
                                            SFileBlockDumpInfo* pDumpInfo) {
  // opt version
  // 1. it is not a border point
  // 2. the direct next point is not an duplicated timestamp
  bool asc = (pReader->order == TSDB_ORDER_ASC);
  if ((pDumpInfo->rowIndex < pDumpInfo->totalRows - 1 && asc) || (pDumpInfo->rowIndex > 0 && (!asc))) {
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

static bool nextRowFromLastBlocks(SLastBlockReader* pLastBlockReader, STableBlockScanInfo* pScanInfo,
                                  SVersionRange* pVerRange) {
  int32_t step = ASCENDING_TRAVERSE(pLastBlockReader->order)? 1:-1;

  while (1) {
    bool hasVal = tMergeTreeNext(&pLastBlockReader->mergeTree);
    if (!hasVal) {
      return false;
    }

    TSDBROW row = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
    TSDBKEY k = TSDBROW_KEY(&row);
    if (hasBeenDropped(pScanInfo->delSkyline, &pScanInfo->lastBlockDelIndex, &k, pLastBlockReader->order, pVerRange)) {
      pScanInfo->lastKey = k.ts;
    } else {
      // the qualifed ts may equal to k.ts, only a greater version one.
      // here we need to fallback one step.
      if (pScanInfo->lastKey == k.ts) {
        pScanInfo->lastKey -= step;
      }

      return true;
    }
  }
}

static bool tryCopyDistinctRowFromSttBlock(TSDBROW* fRow, SLastBlockReader* pLastBlockReader,
                                           STableBlockScanInfo* pScanInfo, int64_t ts, STsdbReader* pReader) {
  bool hasVal = nextRowFromLastBlocks(pLastBlockReader, pScanInfo, &pReader->verRange);
  if (hasVal) {
    int64_t next1 = getCurrentKeyInLastBlock(pLastBlockReader);
    if (next1 != ts) {
      doAppendRowFromFileBlock(pReader->pResBlock, pReader, fRow->pBlockData, fRow->iRow);
      return true;
    }
  } else {
    doAppendRowFromFileBlock(pReader->pResBlock, pReader, fRow->pBlockData, fRow->iRow);
    return true;
  }

  return false;
}

static FORCE_INLINE STSchema* getLatestTableSchema(STsdbReader* pReader, uint64_t uid) {
  if (pReader->pSchema != NULL) {
    return pReader->pSchema;
  }

  pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, uid, -1, 1);
  if (pReader->pSchema == NULL)  {
    tsdbError("failed to get table schema, uid:%" PRIu64 ", it may have been dropped, ver:-1, %s", uid, pReader->idStr);
  }

  return  pReader->pSchema;
}

static FORCE_INLINE STSchema* doGetSchemaForTSRow(int32_t sversion, STsdbReader* pReader, uint64_t uid) {
  // always set the newest schema version in pReader->pSchema
  if (pReader->pSchema == NULL) {
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, uid, -1, 1);
  }

  if (pReader->pSchema && sversion == pReader->pSchema->version) {
    return pReader->pSchema;
  }

  if (pReader->pMemSchema == NULL) {
    int32_t code =
        metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->suid, uid, sversion, &pReader->pMemSchema);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    } else {
      return pReader->pMemSchema;
    }
  }

  if (pReader->pMemSchema->version == sversion) {
    return pReader->pMemSchema;
  }

  taosMemoryFreeClear(pReader->pMemSchema);
  int32_t code = metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->suid, uid, sversion, &pReader->pMemSchema);
  if (code != TSDB_CODE_SUCCESS || pReader->pMemSchema == NULL) {
    terrno = code;
    return NULL;
  } else {
    return pReader->pMemSchema;
  }
}

static int32_t doMergeBufAndFileRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, TSDBROW* pRow,
                                     SIterInfo* pIter, int64_t key, SLastBlockReader* pLastBlockReader) {
  SRowMerger          merge = {0};
  STSRow*             pTSRow = NULL;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  int64_t tsLast = INT64_MIN;
  if (hasDataInLastBlock(pLastBlockReader)) {
    tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
  }

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

  int64_t minKey = 0;
  if (pReader->order == TSDB_ORDER_ASC) {
    minKey = INT64_MAX;  // chosen the minimum value
    if (minKey > tsLast && hasDataInLastBlock(pLastBlockReader)) {
      minKey = tsLast;
    }

    if (minKey > k.ts) {
      minKey = k.ts;
    }

    if (minKey > key && hasDataInFileBlock(pBlockData, pDumpInfo)) {
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

    if (minKey < key && hasDataInFileBlock(pBlockData, pDumpInfo)) {
      minKey = key;
    }
  }

  bool init = false;

  // ASC: file block ---> last block -----> imem -----> mem
  // DESC: mem -----> imem -----> last block -----> file block
  if (pReader->order == TSDB_ORDER_ASC) {
    if (minKey == key) {
      init = true;
      int32_t code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        int32_t code = tRowMergerInit(&merge, &fRow1, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge, &pReader->verRange);
    }

    if (minKey == k.ts) {
      STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
      if (pSchema == NULL) {
        return terrno;
      }
      if (init) {
        tRowMergerAdd(&merge, pRow, pSchema);
      } else {
        init = true;
        int32_t code = tRowMergerInit(&merge, pRow, pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      int32_t code = doMergeRowsInBuf(pIter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else {
    if (minKey == k.ts) {
      init = true;
      STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
      int32_t   code = tRowMergerInit(&merge, pRow, pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(pIter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
      if (code != TSDB_CODE_SUCCESS || merge.pTSchema == NULL) {
        return code;
      }
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        int32_t code = tRowMergerInit(&merge, &fRow1, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge, &pReader->verRange);
    }

    if (minKey == key) {
      if (init) {
        tRowMerge(&merge, &fRow);
      } else {
        init = true;
        int32_t code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }
  }

  int32_t code = tRowMergerGetRow(&merge, &pTSRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFree(pTSRow);
  tRowMergerClear(&merge);
  return TSDB_CODE_SUCCESS;
}

static int32_t doMergeFileBlockAndLastBlock(SLastBlockReader* pLastBlockReader, STsdbReader* pReader,
                                            STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData,
                                            bool mergeBlockData) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  int64_t             tsLastBlock = getCurrentKeyInLastBlock(pLastBlockReader);

  STSRow*    pTSRow = NULL;
  SRowMerger merge = {0};
  TSDBROW    fRow = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
  tsdbTrace("fRow ptr:%p, %d, uid:%" PRIu64 ", %s", fRow.pBlockData, fRow.iRow, pLastBlockReader->uid, pReader->idStr);

  // only last block exists
  if ((!mergeBlockData) || (tsLastBlock != pBlockData->aTSKEY[pDumpInfo->rowIndex])) {
    if (tryCopyDistinctRowFromSttBlock(&fRow, pLastBlockReader, pBlockScanInfo, tsLastBlock, pReader)) {
      pBlockScanInfo->lastKey = tsLastBlock;
      return TSDB_CODE_SUCCESS;
    } else {
      int32_t code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
      tRowMerge(&merge, &fRow1);
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLastBlock, &merge, &pReader->verRange);

      code = tRowMergerGetRow(&merge, &pTSRow);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo);

      taosMemoryFree(pTSRow);
      tRowMergerClear(&merge);
    }
  } else {  // not merge block data
    int32_t code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLastBlock, &merge, &pReader->verRange);

    // merge with block data if ts == key
    if (tsLastBlock == pBlockData->aTSKEY[pDumpInfo->rowIndex]) {
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }

    code = tRowMergerGetRow(&merge, &pTSRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo);

    taosMemoryFree(pTSRow);
    tRowMergerClear(&merge);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mergeFileBlockAndLastBlock(STsdbReader* pReader, SLastBlockReader* pLastBlockReader, int64_t key,
                                          STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  if (hasDataInFileBlock(pBlockData, pDumpInfo)) {
    // no last block available, only data block exists
    if (!hasDataInLastBlock(pLastBlockReader)) {
      return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
    }

    // row in last file block
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
    int64_t ts = getCurrentKeyInLastBlock(pLastBlockReader);
    ASSERT(ts >= key);

    if (ASCENDING_TRAVERSE(pReader->order)) {
      if (key < ts) {  // imem, mem are all empty, file blocks (data blocks and last block) exist
        return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
      } else if (key == ts) {
        STSRow*    pTSRow = NULL;
        SRowMerger merge = {0};

        int32_t code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);

        TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
        tRowMerge(&merge, &fRow1);

        doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, ts, &merge, &pReader->verRange);

        code = tRowMergerGetRow(&merge, &pTSRow);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo);

        taosMemoryFree(pTSRow);
        tRowMergerClear(&merge);
        return code;
      } else {
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
  SRowMerger          merge = {0};
  STSRow*             pTSRow = NULL;
  int32_t             code = TSDB_CODE_SUCCESS;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SArray*             pDelList = pBlockScanInfo->delSkyline;

  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pDelList, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pDelList, pReader);

  int64_t tsLast = INT64_MIN;
  if (hasDataInLastBlock(pLastBlockReader)) {
    tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
  }

  int64_t key = hasDataInFileBlock(pBlockData, pDumpInfo) ? pBlockData->aTSKEY[pDumpInfo->rowIndex] : INT64_MIN;

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

    if (minKey > key && hasDataInFileBlock(pBlockData, pDumpInfo)) {
      minKey = key;
    }

    if (minKey > tsLast && hasDataInLastBlock(pLastBlockReader)) {
      minKey = tsLast;
    }
  } else {
    minKey = INT64_MIN;  // let find the maximum ts value
    if (minKey < k.ts) {
      minKey = k.ts;
    }

    if (minKey < ik.ts) {
      minKey = ik.ts;
    }

    if (minKey < key && hasDataInFileBlock(pBlockData, pDumpInfo)) {
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
      code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        code = tRowMergerInit(&merge, &fRow1, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge, &pReader->verRange);
    }

    if (minKey == ik.ts) {
      if (init) {
        tRowMerge(&merge, piRow);
      } else {
        init = true;
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
        if (pSchema == NULL) {
          return code;
        }

        code = tRowMergerInit(&merge, piRow, pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }

      code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge,
                              pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    if (minKey == k.ts) {
      if (init) {
        if (merge.pTSchema == NULL) {
          return code;
        }

        tRowMerge(&merge, pRow);
      } else {
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
        code = tRowMergerInit(&merge, pRow, pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge,
                              pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else {
    if (minKey == k.ts) {
      init = true;
      STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
      code = tRowMergerInit(&merge, pRow, pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge,
                              pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    if (minKey == ik.ts) {
      if (init) {
        tRowMerge(&merge, piRow);
      } else {
        init = true;
        STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
        code = tRowMergerInit(&merge, piRow, pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge,
                              pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    if (minKey == tsLast) {
      TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
      if (init) {
        tRowMerge(&merge, &fRow1);
      } else {
        init = true;
        code = tRowMergerInit(&merge, &fRow1, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      }
      doMergeRowsInLastBlock(pLastBlockReader, pBlockScanInfo, tsLast, &merge, &pReader->verRange);
    }

    if (minKey == key) {
      TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
      if (!init) {
        code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }
      } else {
        if (merge.pTSchema == NULL) {
          return code;
        }
        tRowMerge(&merge, &fRow);
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    }
  }

  if (merge.pTSchema == NULL) {
    return code;
  }

  code = tRowMergerGetRow(&merge, &pTSRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFree(pTSRow);
  tRowMergerClear(&merge);
  return code;
}

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
  int64_t st = 0;

  STbData* d = NULL;
  if (pReader->pReadSnap->pMem != NULL) {
    d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->suid, pBlockScanInfo->uid);
    if (d != NULL) {
      code = tsdbTbDataIterCreate(d, &startKey, backward, &pBlockScanInfo->iter.iter);
      if (code == TSDB_CODE_SUCCESS) {
        pBlockScanInfo->iter.hasVal = (tsdbTbDataIterGet(pBlockScanInfo->iter.iter) != NULL);

        tsdbDebug("%p uid:%" PRIu64 ", check data in mem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
                  "-%" PRId64 " %s",
                  pReader, pBlockScanInfo->uid, startKey.ts, pReader->order, d->minKey, d->maxKey, pReader->idStr);
      } else {
        tsdbError("%p uid:%" PRIu64 ", failed to create iterator for imem, code:%s, %s", pReader, pBlockScanInfo->uid,
                  tstrerror(code), pReader->idStr);
        return code;
      }
    }
  } else {
    tsdbDebug("%p uid:%" PRIu64 ", no data in mem, %s", pReader, pBlockScanInfo->uid, pReader->idStr);
  }

  STbData* di = NULL;
  if (pReader->pReadSnap->pIMem != NULL) {
    di = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pIMem, pReader->suid, pBlockScanInfo->uid);
    if (di != NULL) {
      code = tsdbTbDataIterCreate(di, &startKey, backward, &pBlockScanInfo->iiter.iter);
      if (code == TSDB_CODE_SUCCESS) {
        pBlockScanInfo->iiter.hasVal = (tsdbTbDataIterGet(pBlockScanInfo->iiter.iter) != NULL);

        tsdbDebug("%p uid:%" PRIu64 ", check data in imem from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
                  "-%" PRId64 " %s",
                  pReader, pBlockScanInfo->uid, startKey.ts, pReader->order, di->minKey, di->maxKey, pReader->idStr);
      } else {
        tsdbError("%p uid:%" PRIu64 ", failed to create iterator for mem, code:%s, %s", pReader, pBlockScanInfo->uid,
                  tstrerror(code), pReader->idStr);
        return code;
      }
    }
  } else {
    tsdbDebug("%p uid:%" PRIu64 ", no data in imem, %s", pReader, pBlockScanInfo->uid, pReader->idStr);
  }

  st = taosGetTimestampUs();
  initDelSkylineIterator(pBlockScanInfo, pReader, d, di);
  pReader->cost.initDelSkylineIterTime += (taosGetTimestampUs() - st) / 1000.0;

  pBlockScanInfo->iterInit = true;
  return TSDB_CODE_SUCCESS;
}

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
  if (hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->fileDelIndex, &k, pReader->order,
                     &pReader->verRange)) {
    return false;
  }

  return true;
}

static bool initLastBlockReader(SLastBlockReader* pLBlockReader, STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  // the last block reader has been initialized for this table.
  if (pLBlockReader->uid == pScanInfo->uid) {
    return hasDataInLastBlock(pLBlockReader);
  }

  if (pLBlockReader->uid != 0) {
    tMergeTreeClose(&pLBlockReader->mergeTree);
  }

  initMemDataIterator(pScanInfo, pReader);
  pLBlockReader->uid = pScanInfo->uid;

  int32_t     step = ASCENDING_TRAVERSE(pLBlockReader->order) ? 1 : -1;
  STimeWindow w = pLBlockReader->window;
  if (ASCENDING_TRAVERSE(pLBlockReader->order)) {
    w.skey = pScanInfo->lastKey + step;
  } else {
    w.ekey = pScanInfo->lastKey + step;
  }

  tsdbDebug("init last block reader, window:%"PRId64"-%"PRId64", uid:%"PRIu64", %s", w.skey, w.ekey, pScanInfo->uid, pReader->idStr);
  int32_t code = tMergeTreeOpen(&pLBlockReader->mergeTree, (pLBlockReader->order == TSDB_ORDER_DESC),
                                pReader->pFileReader, pReader->suid, pScanInfo->uid, &w, &pLBlockReader->verRange,
                                pLBlockReader->pInfo, false, pReader->idStr);
  if (code != TSDB_CODE_SUCCESS) {
    return false;
  }

  return nextRowFromLastBlocks(pLBlockReader, pScanInfo, &pReader->verRange);
}

static int64_t getCurrentKeyInLastBlock(SLastBlockReader* pLastBlockReader) {
  TSDBROW row = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
  return TSDBROW_TS(&row);
}

static bool hasDataInLastBlock(SLastBlockReader* pLastBlockReader) { return pLastBlockReader->mergeTree.pIter != NULL; }

bool hasDataInFileBlock(const SBlockData* pBlockData, const SFileBlockDumpInfo* pDumpInfo) {
  if ((pBlockData->nRow > 0) && (pBlockData->nRow != pDumpInfo->totalRows)) {
    return false;  // this is an invalid result.
  }
  return pBlockData->nRow > 0 && (!pDumpInfo->allDumped);
}

int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, int64_t key,
                              STsdbReader* pReader) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  if (tryCopyDistinctRowFromFileBlock(pReader, pBlockData, key, pDumpInfo)) {
    pBlockScanInfo->lastKey = key;
    return TSDB_CODE_SUCCESS;
  } else {
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

    STSRow*    pTSRow = NULL;
    SRowMerger merge = {0};

    int32_t code = tRowMergerInit(&merge, &fRow, pReader->pSchema);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader, &merge);
    code = tRowMergerGetRow(&merge, &pTSRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    doAppendRowFromTSRow(pReader->pResBlock, pReader, pTSRow, pBlockScanInfo);

    taosMemoryFree(pTSRow);
    tRowMergerClear(&merge);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t buildComposedDataBlockImpl(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo,
                                          SBlockData* pBlockData, SLastBlockReader* pLastBlockReader) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  TSDBROW *pRow = NULL, *piRow = NULL;
  int64_t key = (pBlockData->nRow > 0 && (!pDumpInfo->allDumped)) ? pBlockData->aTSKEY[pDumpInfo->rowIndex] : INT64_MIN;
  if (pBlockScanInfo->iter.hasVal) {
    pRow = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  }

  if (pBlockScanInfo->iiter.hasVal) {
    piRow = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);
  }

  // two levels of mem-table does contain the valid rows
  if (pRow != NULL && piRow != NULL) {
    return doMergeMultiLevelRows(pReader, pBlockScanInfo, pBlockData, pLastBlockReader);
  }

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

static int32_t loadNeighborIfOverlap(SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pBlockScanInfo,
                                     STsdbReader* pReader, bool* loadNeighbor) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     step = ASCENDING_TRAVERSE(pReader->order) ? 1 : -1;
  int32_t     nextIndex = -1;
  SBlockIndex nxtBIndex = {0};

  *loadNeighbor = false;
  SDataBlk* pBlock = getCurrentBlock(&pReader->status.blockIter);

  bool hasNeighbor = getNeighborBlockOfSameTable(pBlockInfo, pBlockScanInfo, &nextIndex, pReader->order, &nxtBIndex);
  if (!hasNeighbor) {  // do nothing
    return code;
  }

  if (overlapWithNeighborBlock(pBlock, &nxtBIndex, pReader->order)) {  // load next block
    SReaderStatus*  pStatus = &pReader->status;
    SDataBlockIter* pBlockIter = &pStatus->blockIter;

    // 1. find the next neighbor block in the scan block list
    SFileDataBlockInfo fb = {.uid = pBlockInfo->uid, .tbBlockIdx = nextIndex};
    int32_t            neighborIndex = findFileBlockInfoIndex(pBlockIter, &fb);

    // 2. remove it from the scan block list
    setFileBlockActiveInBlockIter(pBlockIter, neighborIndex, step);

    // 3. load the neighbor block, and set it to be the currently accessed file data block
    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData, pBlockInfo->uid);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // 4. check the data values
    initBlockDumpInfo(pReader, pBlockIter);
    *loadNeighbor = true;
  }

  return code;
}

static void updateComposedBlockInfo(STsdbReader* pReader, double el, STableBlockScanInfo* pBlockScanInfo) {
  SSDataBlock* pResBlock = pReader->pResBlock;

  pResBlock->info.id.uid = (pBlockScanInfo != NULL) ? pBlockScanInfo->uid : 0;
  pResBlock->info.dataLoad = 1;
  blockDataUpdateTsWindow(pResBlock, pReader->suppInfo.slotId[0]);

  setComposedBlockFlag(pReader, true);

  pReader->cost.composedBlocks += 1;
  pReader->cost.buildComposedBlockTime += el;
}

static int32_t buildComposedDataBlock(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;

  SSDataBlock* pResBlock = pReader->pResBlock;

  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);
  SLastBlockReader*   pLastBlockReader = pReader->status.fileIter.pLastBlockReader;

  bool    asc = ASCENDING_TRAVERSE(pReader->order);
  int64_t st = taosGetTimestampUs();
  int32_t step = asc ? 1 : -1;
  double  el = 0;

  STableBlockScanInfo* pBlockScanInfo = NULL;
  if (pBlockInfo != NULL) {
    void* p = taosHashGet(pReader->status.pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
    if (p == NULL) {
      code = TSDB_CODE_INVALID_PARA;
      tsdbError("failed to locate the uid:%" PRIu64 " in query table uid list, total tables:%d, %s", pBlockInfo->uid,
                taosHashGetSize(pReader->status.pTableMap), pReader->idStr);
      goto _end;
    }

    pBlockScanInfo = *(STableBlockScanInfo**)p;

    SDataBlk* pBlock = getCurrentBlock(&pReader->status.blockIter);
    TSDBKEY   keyInBuf = getCurrentKeyInBuf(pBlockScanInfo, pReader);

    // it is a clean block, load it directly
    if (isCleanFileDataBlock(pReader, pBlockInfo, pBlock, pBlockScanInfo, keyInBuf, pLastBlockReader) &&
        pBlock->nRow <= pReader->capacity) {
      if (asc || ((!asc) && (!hasDataInLastBlock(pLastBlockReader)))) {
        copyBlockDataToSDataBlock(pReader);

        // record the last key value
        pBlockScanInfo->lastKey = asc ? pBlock->maxKey.ts : pBlock->minKey.ts;
        goto _end;
      }
    }
  } else {  // file blocks not exist
    pBlockScanInfo = *pReader->status.pTableIter;
  }

  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;

  while (1) {
    bool hasBlockData = false;
    {
      while (pBlockData->nRow > 0) {  // find the first qualified row in data block
        if (isValidFileBlockRow(pBlockData, pDumpInfo, pBlockScanInfo, pReader)) {
          hasBlockData = true;
          break;
        }

        pDumpInfo->rowIndex += step;

        SDataBlk* pBlock = getCurrentBlock(&pReader->status.blockIter);
        if (pDumpInfo->rowIndex >= pBlock->nRow || pDumpInfo->rowIndex < 0) {
          pBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);  // NOTE: get the new block info

          // continue check for the next file block if the last ts in the current block
          // is overlapped with the next neighbor block
          bool loadNeighbor = false;
          code = loadNeighborIfOverlap(pBlockInfo, pBlockScanInfo, pReader, &loadNeighbor);
          if ((!loadNeighbor) || (code != 0)) {
            setBlockAllDumped(pDumpInfo, pBlock->maxKey.ts, pReader->order);
            break;
          }
        }
      }
    }

    // no data in last block and block, no need to proceed.
    if (hasBlockData == false) {
      break;
    }

    buildComposedDataBlockImpl(pReader, pBlockScanInfo, pBlockData, pLastBlockReader);

    // currently loaded file data block is consumed
    if ((pBlockData->nRow > 0) && (pDumpInfo->rowIndex >= pBlockData->nRow || pDumpInfo->rowIndex < 0)) {
      SDataBlk* pBlock = getCurrentBlock(&pReader->status.blockIter);
      setBlockAllDumped(pDumpInfo, pBlock->maxKey.ts, pReader->order);
      break;
    }

    if (pResBlock->info.rows >= pReader->capacity) {
      break;
    }
  }

_end:
  el = (taosGetTimestampUs() - st) / 1000.0;
  updateComposedBlockInfo(pReader, el, pBlockScanInfo);

  if (pResBlock->info.rows > 0) {
    tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64
              " rows:%d, elapsed time:%.2f ms %s",
              pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
              pResBlock->info.rows, el, pReader->idStr);
  }

  return code;
}

void setComposedBlockFlag(STsdbReader* pReader, bool composed) { pReader->status.composedDataBlock = composed; }

int32_t getInitialDelIndex(const SArray* pDelSkyline, int32_t order) {
  if (pDelSkyline == NULL) {
    return 0;
  }

  return ASCENDING_TRAVERSE(order) ? 0 : taosArrayGetSize(pDelSkyline) - 1;
}

int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, STbData* pMemTbData,
                               STbData* piMemTbData) {
  if (pBlockScanInfo->delSkyline != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = 0;
  SArray* pDelData = taosArrayInit(4, sizeof(SDelData));

  SDelFile* pDelFile = pReader->pReadSnap->fs.pDelFile;
  if (pDelFile && taosArrayGetSize(pReader->pDelIdx) > 0) {
    SDelIdx  idx = {.suid = pReader->suid, .uid = pBlockScanInfo->uid};
    SDelIdx* pIdx = taosArraySearch(pReader->pDelIdx, &idx, tCmprDelIdx, TD_EQ);

    if (pIdx != NULL) {
      code = tsdbReadDelData(pReader->pDelFReader, pIdx, pDelData);
    }
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
  int32_t index = getInitialDelIndex(pBlockScanInfo->delSkyline, pReader->order);

  pBlockScanInfo->iter.index = index;
  pBlockScanInfo->iiter.index = index;
  pBlockScanInfo->fileDelIndex = index;
  pBlockScanInfo->lastBlockDelIndex = index;

  return code;

_err:
  taosArrayDestroy(pDelData);
  return code;
}

TSDBKEY getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  bool asc = ASCENDING_TRAVERSE(pReader->order);
//  TSKEY initialVal = asc? TSKEY_MIN:TSKEY_MAX;

  TSDBKEY  key = {.ts = TSKEY_INITIAL_VAL}, ikey = {.ts = TSKEY_INITIAL_VAL};

  bool hasKey = false, hasIKey = false;
  TSDBROW* pRow = getValidMemRow(&pScanInfo->iter, pScanInfo->delSkyline, pReader);
  if (pRow != NULL) {
    hasKey = true;
    key = TSDBROW_KEY(pRow);
  }

  TSDBROW* pIRow = getValidMemRow(&pScanInfo->iiter, pScanInfo->delSkyline, pReader);
  if (pIRow != NULL) {
    hasIKey = true;
    ikey = TSDBROW_KEY(pIRow);
  }

  if (hasKey) {
    if (hasIKey) { // has data in mem & imem
      if (asc) {
        return key.ts <= ikey.ts ? key : ikey;
      } else  {
        return key.ts <= ikey.ts ? ikey: key;
      }
    } else {  // no data in imem
      return key;
    }
  } else {
    // no data in mem & imem, return the initial value
    // only imem has data, return ikey
    return ikey;
  }
}

static int32_t moveToNextFile(STsdbReader* pReader, SBlockNumber* pBlockNum) {
  SReaderStatus* pStatus = &pReader->status;
  pBlockNum->numOfBlocks = 0;
  pBlockNum->numOfLastFiles = 0;

  size_t  numOfTables = taosHashGetSize(pReader->status.pTableMap);
  SArray* pIndexList = taosArrayInit(numOfTables, sizeof(SBlockIdx));

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

    if (taosArrayGetSize(pIndexList) > 0 || pReader->pFileReader->pSet->nSttF > 0) {
      code = doLoadFileBlock(pReader, pIndexList, pBlockNum);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pIndexList);
        return code;
      }

      if (pBlockNum->numOfBlocks + pBlockNum->numOfLastFiles > 0) {
        break;
      }
    }

    // no blocks in current file, try next files
  }

  taosArrayDestroy(pIndexList);

  if (pReader->pReadSnap != NULL) {
    SDelFile* pDelFile = pReader->pReadSnap->fs.pDelFile;
    if (pReader->pDelFReader == NULL && pDelFile != NULL) {
      int32_t code = tsdbDelFReaderOpen(&pReader->pDelFReader, pDelFile, pReader->pTsdb);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      pReader->pDelIdx = taosArrayInit(4, sizeof(SDelIdx));
      if (pReader->pDelIdx == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        return code;
      }

      code = tsdbReadDelIdx(pReader->pDelFReader, pReader->pDelIdx);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pReader->pDelIdx);
        return code;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t uidComparFunc(const void* p1, const void* p2) {
  uint64_t pu1 = *(uint64_t*)p1;
  uint64_t pu2 = *(uint64_t*)p2;
  if (pu1 == pu2) {
    return 0;
  } else {
    return (pu1 < pu2) ? -1 : 1;
  }
}

static void extractOrderedTableUidList(SUidOrderCheckInfo* pOrderCheckInfo, SReaderStatus* pStatus, int32_t order) {
  int32_t index = 0;
  int32_t total = taosHashGetSize(pStatus->pTableMap);

  void* p = taosHashIterate(pStatus->pTableMap, NULL);
  while (p != NULL) {
    STableBlockScanInfo* pScanInfo = *(STableBlockScanInfo**)p;
    pOrderCheckInfo->tableUidList[index++] = pScanInfo->uid;
    p = taosHashIterate(pStatus->pTableMap, p);
  }

  taosSort(pOrderCheckInfo->tableUidList, total, sizeof(uint64_t), uidComparFunc);
}

static int32_t initOrderCheckInfo(SUidOrderCheckInfo* pOrderCheckInfo, STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;

  int32_t total = taosHashGetSize(pStatus->pTableMap);
  if (total == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (pOrderCheckInfo->tableUidList == NULL) {
    pOrderCheckInfo->currentIndex = 0;
    pOrderCheckInfo->tableUidList = taosMemoryMalloc(total * sizeof(uint64_t));
    if (pOrderCheckInfo->tableUidList == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    extractOrderedTableUidList(pOrderCheckInfo, pStatus, pReader->order);
    uint64_t uid = pOrderCheckInfo->tableUidList[0];
    pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  } else {
    if (pStatus->pTableIter == NULL) {  // it is the last block of a new file
      pOrderCheckInfo->currentIndex = 0;
      uint64_t uid = pOrderCheckInfo->tableUidList[pOrderCheckInfo->currentIndex];
      pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));

      // the tableMap has already updated
      if (pStatus->pTableIter == NULL) {
        void* p = taosMemoryRealloc(pOrderCheckInfo->tableUidList, total * sizeof(uint64_t));
        if (p == NULL) {
          return TSDB_CODE_OUT_OF_MEMORY;
        }

        pOrderCheckInfo->tableUidList = p;
        extractOrderedTableUidList(pOrderCheckInfo, pStatus, pReader->order);

        uid = pOrderCheckInfo->tableUidList[0];
        pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

static bool moveToNextTable(SUidOrderCheckInfo* pOrderedCheckInfo, SReaderStatus* pStatus) {
  pOrderedCheckInfo->currentIndex += 1;
  if (pOrderedCheckInfo->currentIndex >= taosHashGetSize(pStatus->pTableMap)) {
    pStatus->pTableIter = NULL;
    return false;
  }

  uint64_t uid = pOrderedCheckInfo->tableUidList[pOrderedCheckInfo->currentIndex];
  pStatus->pTableIter = taosHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  return (pStatus->pTableIter != NULL);
}

static int32_t doLoadLastBlockSequentially(STsdbReader* pReader) {
  SReaderStatus*    pStatus = &pReader->status;
  SLastBlockReader* pLastBlockReader = pStatus->fileIter.pLastBlockReader;

  SUidOrderCheckInfo* pOrderedCheckInfo = &pStatus->uidCheckInfo;
  int32_t             code = initOrderCheckInfo(pOrderedCheckInfo, pReader);
  if (code != TSDB_CODE_SUCCESS || (taosHashGetSize(pStatus->pTableMap) == 0)) {
    return code;
  }

  SSDataBlock* pResBlock = pReader->pResBlock;

  while (1) {
    // load the last data block of current table
    STableBlockScanInfo* pScanInfo = *(STableBlockScanInfo**)pStatus->pTableIter;

    bool hasVal = initLastBlockReader(pLastBlockReader, pScanInfo, pReader);
    if (!hasVal) {
      bool hasNexTable = moveToNextTable(pOrderedCheckInfo, pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    int64_t st = taosGetTimestampUs();
    while (1) {
      bool hasBlockLData = hasDataInLastBlock(pLastBlockReader);

      // no data in last block and block, no need to proceed.
      if (hasBlockLData == false) {
        break;
      }

      buildComposedDataBlockImpl(pReader, pScanInfo, &pReader->status.fileBlockData, pLastBlockReader);
      if (pResBlock->info.rows >= pReader->capacity) {
        break;
      }
    }

    double el = (taosGetTimestampUs() - st) / 1000.0;
    updateComposedBlockInfo(pReader, el, pScanInfo);

    if (pResBlock->info.rows > 0) {
      tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64
                " rows:%d, elapsed time:%.2f ms %s",
                pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
                pResBlock->info.rows, el, pReader->idStr);
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
  int32_t   code = TSDB_CODE_SUCCESS;
  SDataBlk* pBlock = NULL;

  SReaderStatus*       pStatus = &pReader->status;
  SDataBlockIter*      pBlockIter = &pStatus->blockIter;
  STableBlockScanInfo* pScanInfo = NULL;
  SFileDataBlockInfo*  pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SLastBlockReader*    pLastBlockReader = pReader->status.fileIter.pLastBlockReader;

  ASSERT(pBlockInfo != NULL);

  pScanInfo = *(STableBlockScanInfo**)taosHashGet(pReader->status.pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
  if (pScanInfo == NULL) {
    tsdbError("failed to get table scan-info, %s", pReader->idStr);
    code = TSDB_CODE_INVALID_PARA;
    return code;
  }

  pBlock = getCurrentBlock(pBlockIter);

  initLastBlockReader(pLastBlockReader, pScanInfo, pReader);
  TSDBKEY keyInBuf = getCurrentKeyInBuf(pScanInfo, pReader);

  if (fileBlockShouldLoad(pReader, pBlockInfo, pBlock, pScanInfo, keyInBuf, pLastBlockReader)) {
    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData, pScanInfo->uid);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // build composed data block
    code = buildComposedDataBlock(pReader);
  } else if (bufferDataInFileBlockGap(pReader->order, keyInBuf, pBlock)) {
    // data in memory that are earlier than current file block
    // rows in buffer should be less than the file block in asc, greater than file block in desc
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->order)) ? pBlock->minKey.ts : pBlock->maxKey.ts;
    code = buildDataBlockFromBuf(pReader, pScanInfo, endKey);
  } else {
    if (hasDataInLastBlock(pLastBlockReader) && !ASCENDING_TRAVERSE(pReader->order)) {
      // only return the rows in last block
      int64_t tsLast = getCurrentKeyInLastBlock(pLastBlockReader);
      ASSERT(tsLast >= pBlock->maxKey.ts);

      SBlockData* pBData = &pReader->status.fileBlockData;
      tBlockDataReset(pBData);

      SSDataBlock* pResBlock = pReader->pResBlock;
      tsdbDebug("load data in last block firstly, due to desc scan data, %s", pReader->idStr);

      int64_t st = taosGetTimestampUs();

      while (1) {
        bool hasBlockLData = hasDataInLastBlock(pLastBlockReader);

        // no data in last block and block, no need to proceed.
        if (hasBlockLData == false) {
          break;
        }

        buildComposedDataBlockImpl(pReader, pScanInfo, &pReader->status.fileBlockData, pLastBlockReader);
        if (pResBlock->info.rows >= pReader->capacity) {
          break;
        }
      }

      double el = (taosGetTimestampUs() - st) / 1000.0;
      updateComposedBlockInfo(pReader, el, pScanInfo);

      if (pResBlock->info.rows > 0) {
        tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64
                  " rows:%d, elapsed time:%.2f ms %s",
                  pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
                  pResBlock->info.rows, el, pReader->idStr);
      }
    } else {  // whole block is required, return it directly
      SDataBlockInfo* pInfo = &pReader->pResBlock->info;
      pInfo->rows = pBlock->nRow;
      pInfo->id.uid = pScanInfo->uid;
      pInfo->dataLoad = 0;
      pInfo->window = (STimeWindow){.skey = pBlock->minKey.ts, .ekey = pBlock->maxKey.ts};
      setComposedBlockFlag(pReader, false);
      setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlock->maxKey.ts, pReader->order);

      // update the last key for the corresponding table
      pScanInfo->lastKey = ASCENDING_TRAVERSE(pReader->order) ? pInfo->window.ekey : pInfo->window.skey;
      tsdbDebug("%p uid:%" PRIu64 " clean file block retrieved from file, global index:%d, "
                "table index:%d, rows:%d, brange:%" PRId64 "-%" PRId64 ", %s",
                pReader, pScanInfo->uid, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlock->nRow, pBlock->minKey.ts,
                pBlock->maxKey.ts, pReader->idStr);
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

    STableBlockScanInfo** pBlockScanInfo = pStatus->pTableIter;
    initMemDataIterator(*pBlockScanInfo, pReader);

    int64_t endKey = (ASCENDING_TRAVERSE(pReader->order)) ? INT64_MAX : INT64_MIN;
    int32_t code = buildDataBlockFromBuf(pReader, *pBlockScanInfo, endKey);
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
void initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  SDataBlk* pBlock = getCurrentBlock(pBlockIter);

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
  if (num.numOfBlocks + num.numOfLastFiles == 0) {
    pReader->status.loadFromFile = false;
    return code;
  }

  // initialize the block iterator for a new fileset
  if (num.numOfBlocks > 0) {
    code = initBlockIterator(pReader, pBlockIter, num.numOfBlocks);
  } else {  // no block data, only last block exists
    tBlockDataReset(&pReader->status.fileBlockData);
    resetDataBlockIterator(pBlockIter, pReader->order);
  }

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
        bool hasNext = blockIteratorNext(&pReader->status.blockIter, pReader->idStr);
        if (hasNext) {  // check for the next block in the block accessed order list
          initBlockDumpInfo(pReader, pBlockIter);
        } else {
          if (pReader->status.pCurrentFileset->nSttF > 0) {
            // data blocks in current file are exhausted, let's try the next file now
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
    int8_t  precision = pVnode->config.tsdbCfg.precision;
    int64_t now = taosGetTimestamp(precision);
    int64_t offset = tsQueryRsmaTolerance * ((precision == TSDB_TIME_PRECISION_MILLI)   ? 1L
                                             : (precision == TSDB_TIME_PRECISION_MICRO) ? 1000L
                                                                                        : 1000000L);

    for (int8_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
      SRetention* pRetention = retentions + level;
      if (pRetention->keep <= 0) {
        if (level > 0) {
          --level;
        }
        break;
      }
      if ((now - pRetention->keep) <= (winSKey + offset)) {
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

bool hasBeenDropped(const SArray* pDelList, int32_t* index, TSDBKEY* pKey, int32_t order, SVersionRange* pVerRange) {
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
        return (prev->version >= pKey->version && prev->version <= pVerRange->maxVer &&
                prev->version >= pVerRange->minVer);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pNext = taosArrayGet(pDelList, (*index) + 1);

      if (pKey->ts < pCurrent->ts) {
        return false;
      }

      if (pCurrent->ts <= pKey->ts && pNext->ts >= pKey->ts && pCurrent->version >= pKey->version &&
          pVerRange->maxVer >= pCurrent->version) {
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

          if (pCurrent->ts <= pKey->ts && pNext->ts >= pKey->ts && pCurrent->version >= pKey->version &&
              pVerRange->maxVer >= pCurrent->version) {
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
      (!hasBeenDropped(pDelList, &pIter->index, &key, pReader->order, &pReader->verRange))) {
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
        (!hasBeenDropped(pDelList, &pIter->index, &key, pReader->order, &pReader->verRange))) {
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
    if (pTSchema == NULL) {
      return terrno;
    }

    tRowMergerAdd(pMerger, pRow, pTSchema);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t doMergeRowsInFileBlockImpl(SBlockData* pBlockData, int32_t rowIndex, int64_t key, SRowMerger* pMerger,
                                          SVersionRange* pVerRange, int32_t step) {
  while (rowIndex < pBlockData->nRow && rowIndex >= 0 && pBlockData->aTSKEY[rowIndex] == key) {
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

static int32_t checkForNeighborFileBlock(STsdbReader* pReader, STableBlockScanInfo* pScanInfo, SDataBlk* pBlock,
                                         SFileDataBlockInfo* pFBlock, SRowMerger* pMerger, int64_t key,
                                         CHECK_FILEBLOCK_STATE* state) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;

  *state = CHECK_FILEBLOCK_QUIT;
  int32_t step = ASCENDING_TRAVERSE(pReader->order) ? 1 : -1;

  bool    loadNeighbor = true;
  int32_t code = loadNeighborIfOverlap(pFBlock, pScanInfo, pReader, &loadNeighbor);

  if (loadNeighbor && (code == TSDB_CODE_SUCCESS)) {
    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->verRange, step);
    if (pDumpInfo->rowIndex >= pDumpInfo->totalRows) {
      *state = CHECK_FILEBLOCK_CONT;
    }
  }

  return code;
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
      SDataBlk*           pCurrentBlock = getCurrentBlock(&pReader->status.blockIter);
      checkForNeighborFileBlock(pReader, pScanInfo, pCurrentBlock, pFileBlockInfo, pMerger, key, &st);
      if (st == CHECK_FILEBLOCK_QUIT) {
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeRowsInLastBlock(SLastBlockReader* pLastBlockReader, STableBlockScanInfo* pScanInfo, int64_t ts,
                               SRowMerger* pMerger, SVersionRange* pVerRange) {
  while (nextRowFromLastBlocks(pLastBlockReader, pScanInfo, pVerRange)) {
    int64_t next1 = getCurrentKeyInLastBlock(pLastBlockReader);
    if (next1 == ts) {
      TSDBROW fRow1 = tMergeTreeGetRow(&pLastBlockReader->mergeTree);
      tRowMerge(pMerger, &fRow1);
    } else {
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeMemTableMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList, STSRow** pTSRow,
                                 STsdbReader* pReader, bool* freeTSRow) {
  TSDBROW* pNextRow = NULL;
  TSDBROW  current = *pRow;

  {  // if the timestamp of the next valid row has a different ts, return current row directly
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);

    if (!pIter->hasVal) {
      *pTSRow = current.pTSRow;
      *freeTSRow = false;
      return TSDB_CODE_SUCCESS;
    } else {  // has next point in mem/imem
      pNextRow = getValidMemRow(pIter, pDelList, pReader);
      if (pNextRow == NULL) {
        *pTSRow = current.pTSRow;
        *freeTSRow = false;
        return TSDB_CODE_SUCCESS;
      }

      if (current.pTSRow->ts != pNextRow->pTSRow->ts) {
        *pTSRow = current.pTSRow;
        *freeTSRow = false;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  SRowMerger merge = {0};

  // get the correct schema for data in memory
  terrno = 0;
  STSchema* pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(&current), pReader, uid);
  if (pTSchema == NULL) {
    return terrno;
  }

  if (pReader->pSchema == NULL) {
    pReader->pSchema = pTSchema;
  }

  int32_t code = tRowMergerInit2(&merge, pReader->pSchema, &current, pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STSchema* pTSchema1 = doGetSchemaForTSRow(TSDBROW_SVERSION(pNextRow), pReader, uid);
  if (pTSchema1 == NULL) {
    return terrno;
  }

  tRowMergerAdd(&merge, pNextRow, pTSchema1);

  code = doMergeRowsInBuf(pIter, uid, current.pTSRow->ts, pDelList, &merge, pReader);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tRowMergerGetRow(&merge, pTSRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  tRowMergerClear(&merge);
  *freeTSRow = true;
  return TSDB_CODE_SUCCESS;
}

int32_t doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader,
                           STSRow** pTSRow) {
  SRowMerger merge = {0};

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  if (ASCENDING_TRAVERSE(pReader->order)) {  // ascending order imem --> mem
    STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);

    int32_t code = tRowMergerInit(&merge, piRow, pSchema);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge,
                            pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tRowMerge(&merge, pRow);
    code =
        doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

  } else {
    STSchema* pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);

    int32_t code = tRowMergerInit(&merge, pRow, pSchema);
    if (code != TSDB_CODE_SUCCESS || merge.pTSchema == NULL) {
      return code;
    }

    code =
        doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, &merge, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tRowMerge(&merge, piRow);
    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, &merge,
                            pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  int32_t code = tRowMergerGetRow(&merge, pTSRow);
  tRowMergerClear(&merge);

  return code;
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

    int32_t code = TSDB_CODE_SUCCESS;
    if (ik.ts != k.ts) {
      if (((ik.ts < k.ts) && asc) || ((ik.ts > k.ts) && (!asc))) {  // ik.ts < k.ts
        code = doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, pTSRow, pReader, freeTSRow);
      } else if (((k.ts < ik.ts) && asc) || ((k.ts > ik.ts) && (!asc))) {
        code = doMergeMemTableMultiRows(pRow, uid, &pBlockScanInfo->iter, pDelList, pTSRow, pReader, freeTSRow);
      }
    } else {  // ik.ts == k.ts
      *freeTSRow = true;
      code = doMergeMemIMemRows(pRow, piRow, pBlockScanInfo, pReader, pTSRow);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    return code;
  }

  if (pBlockScanInfo->iter.hasVal && pRow != NULL) {
    return doMergeMemTableMultiRows(pRow, pBlockScanInfo->uid, &pBlockScanInfo->iter, pDelList, pTSRow, pReader,
                                    freeTSRow);
  }

  if (pBlockScanInfo->iiter.hasVal && piRow != NULL) {
    return doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, pTSRow, pReader, freeTSRow);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, STSRow* pTSRow,
                             STableBlockScanInfo* pScanInfo) {
  int32_t outputRowIndex = pBlock->info.rows;
  int64_t uid = pScanInfo->uid;

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  STSchema*           pSchema = doGetSchemaForTSRow(pTSRow->sver, pReader, uid);

  SColVal colVal = {0};
  int32_t i = 0, j = 0;

  if (pSupInfo->colId[i] == PRIMARYKEY_TIMESTAMP_COL_ID) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
    ((int64_t*)pColData->pData)[outputRowIndex] = pTSRow->ts;
    i += 1;
  }

  while (i < pSupInfo->numOfCols && j < pSchema->numOfCols) {
    col_id_t colId = pSupInfo->colId[i];

    if (colId == pSchema->columns[j].colId) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);

      tTSRowGetVal(pTSRow, pSchema, j, &colVal);
      doCopyColVal(pColInfoData, outputRowIndex, i, &colVal, pSupInfo);
      i += 1;
      j += 1;
    } else if (colId < pSchema->columns[j].colId) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);

      colDataAppendNULL(pColInfoData, outputRowIndex);
      i += 1;
    } else if (colId > pSchema->columns[j].colId) {
      j += 1;
    }
  }

  // set null value since current column does not exist in the "pSchema"
  while (i < pSupInfo->numOfCols) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
    colDataAppendNULL(pColInfoData, outputRowIndex);
    i += 1;
  }

  pBlock->info.dataLoad = 1;
  pBlock->info.rows += 1;
  pScanInfo->lastKey = pTSRow->ts;
  return TSDB_CODE_SUCCESS;
}

int32_t doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                 int32_t rowIndex) {
  int32_t i = 0, j = 0;
  int32_t outputRowIndex = pResBlock->info.rows;

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  if (pReader->suppInfo.colId[i] == PRIMARYKEY_TIMESTAMP_COL_ID) {
    SColumnInfoData* pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    ((int64_t*)pColData->pData)[outputRowIndex] = pBlockData->aTSKEY[rowIndex];
    i += 1;
  }

  SColVal cv = {0};
  int32_t numOfInputCols = pBlockData->nColData;
  int32_t numOfOutputCols = pSupInfo->numOfCols;

  while (i < numOfOutputCols && j < numOfInputCols) {
    SColData* pData = tBlockDataGetColDataByIdx(pBlockData, j);
    if (pData->cid < pSupInfo->colId[i]) {
      j += 1;
      continue;
    }

    SColumnInfoData* pCol = TARRAY_GET_ELEM(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    if (pData->cid == pSupInfo->colId[i]) {
      tColDataGetValue(pData, rowIndex, &cv);
      doCopyColVal(pCol, outputRowIndex, i, &cv, pSupInfo);
      j += 1;
    } else if (pData->cid > pCol->info.colId) {
      // the specified column does not exist in file block, fill with null data
      colDataAppendNULL(pCol, outputRowIndex);
    }

    i += 1;
  }

  while (i < numOfOutputCols) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    colDataAppendNULL(pCol, outputRowIndex);
    i += 1;
  }

  pResBlock->info.dataLoad = 1;
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

    doAppendRowFromTSRow(pBlock, pReader, pTSRow, pBlockScanInfo);

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

  return TSDB_CODE_SUCCESS;
}

// TODO refactor: with createDataBlockScanInfo
int32_t tsdbSetTableList(STsdbReader* pReader, const void* pTableList, int32_t num) {
  int32_t size = taosHashGetSize(pReader->status.pTableMap);

  STableBlockScanInfo** p = NULL;
  while ((p = taosHashIterate(pReader->status.pTableMap, p)) != NULL) {
    clearBlockScanInfo(*p);
  }

  // todo handle the case where size is less than the value of num
  ASSERT(size >= num);

  taosHashClear(pReader->status.pTableMap);

  STableKeyInfo* pList = (STableKeyInfo*)pTableList;
  for (int32_t i = 0; i < num; ++i) {
    STableBlockScanInfo* pInfo = getPosInBlockInfoBuf(&pReader->blockInfoBuf, i);
    pInfo->uid = pList[i].uid;
    taosHashPut(pReader->status.pTableMap, &pInfo->uid, sizeof(uint64_t), &pInfo, POINTER_BYTES);
  }

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

static int32_t doOpenReaderImpl(STsdbReader* pReader) {
  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  initFilesetIterator(&pReader->status.fileIter, pReader->pReadSnap->fs.aDFileSet, pReader);
  resetDataBlockIterator(&pReader->status.blockIter, pReader->order);

  // no data in files, let's try buffer in memory
  if (pReader->status.fileIter.numOfFiles == 0) {
    pReader->status.loadFromFile = false;
    return TSDB_CODE_SUCCESS;
  } else {
    return initForFirstBlockInFile(pReader, pBlockIter);
  }
}

// ====================================== EXPOSED APIs ======================================
int32_t tsdbReaderOpen(SVnode* pVnode, SQueryTableDataCond* pCond, void* pTableList, int32_t numOfTables,
                       SSDataBlock* pResBlock, STsdbReader** ppReader, const char* idstr) {
  STimeWindow window = pCond->twindows;
  if (pCond->type == TIMEWINDOW_RANGE_EXTERNAL) {
    pCond->twindows.skey += 1;
    pCond->twindows.ekey -= 1;
  }

  int32_t capacity = pVnode->config.tsdbCfg.maxRows;
  if (pResBlock != NULL) {
    blockDataEnsureCapacity(pResBlock, capacity);
  }

  int32_t code = tsdbReaderCreate(pVnode, pCond, ppReader, capacity, pResBlock, idstr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  // check for query time window
  STsdbReader* pReader = *ppReader;
  if (isEmptyQueryTimeWindow(&pReader->window) && pCond->type == TIMEWINDOW_RANGE_CONTAINED) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, %s", pReader, pReader->idStr);
    return TSDB_CODE_SUCCESS;
  }

  if (pCond->type == TIMEWINDOW_RANGE_EXTERNAL) {
    // update the SQueryTableDataCond to create inner reader
    int32_t order = pCond->order;
    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.ekey = window.skey;
      pCond->twindows.skey = INT64_MIN;
      pCond->order = TSDB_ORDER_DESC;
    } else {
      pCond->twindows.skey = window.ekey;
      pCond->twindows.ekey = INT64_MAX;
      pCond->order = TSDB_ORDER_ASC;
    }

    // here we only need one more row, so the capacity is set to be ONE.
    code = tsdbReaderCreate(pVnode, pCond, &pReader->innerReader[0], 1, pResBlock, idstr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.skey = window.ekey;
      pCond->twindows.ekey = INT64_MAX;
    } else {
      pCond->twindows.skey = INT64_MIN;
      pCond->twindows.ekey = window.ekey;
    }
    pCond->order = order;

    code = tsdbReaderCreate(pVnode, pCond, &pReader->innerReader[1], 1, pResBlock, idstr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }
  }

  // NOTE: the endVersion in pCond is the data version not schema version, so pCond->endVersion is not correct here.
  //  no valid error code set in metaGetTbTSchema, so let's set the error code here.
  //  we should proceed in case of tmq processing.
  if (pCond->suid != 0) {
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, pReader->suid, -1, 1);
    if (pReader->pSchema == NULL) {
      tsdbError("failed to get table schema, suid:%" PRIu64 ", ver:-1, %s", pReader->suid, pReader->idStr);
    }
  } else if (numOfTables > 0) {
    STableKeyInfo* pKey = pTableList;
    pReader->pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, pKey->uid, -1, 1);
    if (pReader->pSchema == NULL) {
      tsdbError("failed to get table schema, uid:%" PRIu64 ", ver:-1, %s", pKey->uid, pReader->idStr);
    }
  }

  if (pReader->pSchema != NULL) {
    code = updateBlockSMAInfo(pReader->pSchema, &pReader->suppInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }
  }

  STsdbReader* p = (pReader->innerReader[0] != NULL) ? pReader->innerReader[0] : pReader;
  pReader->status.pTableMap = createDataBlockScanInfo(p, &pReader->blockInfoBuf, pTableList, numOfTables);
  if (pReader->status.pTableMap == NULL) {
    *ppReader = NULL;
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  if (numOfTables > 0) {
    code = tsdbTakeReadSnap(pReader->pTsdb, &pReader->pReadSnap, pReader->idStr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    if (pReader->type == TIMEWINDOW_RANGE_CONTAINED) {
      code = doOpenReaderImpl(pReader);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }
    } else {
      STsdbReader* pPrevReader = pReader->innerReader[0];
      STsdbReader* pNextReader = pReader->innerReader[1];

      // we need only one row
      pPrevReader->capacity = 1;
      pPrevReader->status.pTableMap = pReader->status.pTableMap;
      pPrevReader->pSchema = pReader->pSchema;
      pPrevReader->pMemSchema = pReader->pMemSchema;
      pPrevReader->pReadSnap = pReader->pReadSnap;

      pNextReader->capacity = 1;
      pNextReader->status.pTableMap = pReader->status.pTableMap;
      pNextReader->pSchema = pReader->pSchema;
      pNextReader->pMemSchema = pReader->pMemSchema;
      pNextReader->pReadSnap = pReader->pReadSnap;

      code = doOpenReaderImpl(pPrevReader);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }
    }
  }

  tsdbDebug("%p total numOfTable:%d in this query %s", pReader, numOfTables, pReader->idStr);
  return code;

_err:
  tsdbError("failed to create data reader, code:%s %s", tstrerror(code), idstr);
  tsdbReaderClose(pReader);
  return code;
}

void tsdbReaderClose(STsdbReader* pReader) {
  if (pReader == NULL) {
    return;
  }

  {
    if (pReader->innerReader[0] != NULL || pReader->innerReader[1] != NULL) {
      STsdbReader* p = pReader->innerReader[0];

      p->status.pTableMap = NULL;
      p->pReadSnap = NULL;
      p->pSchema = NULL;
      p->pMemSchema = NULL;

      p = pReader->innerReader[1];

      p->status.pTableMap = NULL;
      p->pReadSnap = NULL;
      p->pSchema = NULL;
      p->pMemSchema = NULL;

      tsdbReaderClose(pReader->innerReader[0]);
      tsdbReaderClose(pReader->innerReader[1]);
    }
  }

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;

  taosArrayDestroy(pSupInfo->pColAgg);
  for (int32_t i = 0; i < pSupInfo->numOfCols; ++i) {
    if (pSupInfo->buildBuf[i] != NULL) {
      taosMemoryFreeClear(pSupInfo->buildBuf[i]);
    }
  }

  if (pReader->freeBlock) {
    pReader->pResBlock = blockDataDestroy(pReader->pResBlock);
  }

  taosMemoryFree(pSupInfo->colId);
  tBlockDataDestroy(&pReader->status.fileBlockData, true);
  cleanupDataBlockIterator(&pReader->status.blockIter);

  size_t numOfTables = taosHashGetSize(pReader->status.pTableMap);
  if (pReader->status.pTableMap != NULL) {
    destroyAllBlockScanInfo(pReader->status.pTableMap);
    clearBlockScanInfoBuf(&pReader->blockInfoBuf);
  }

  if (pReader->pFileReader != NULL) {
    tsdbDataFReaderClose(&pReader->pFileReader);
  }

  if (pReader->pDelFReader != NULL) {
    tsdbDelFReaderClose(&pReader->pDelFReader);
  }

  if (pReader->pDelIdx != NULL) {
    taosArrayDestroy(pReader->pDelIdx);
    pReader->pDelIdx = NULL;
  }

  tsdbUntakeReadSnap(pReader->pTsdb, pReader->pReadSnap, pReader->idStr);

  taosMemoryFree(pReader->status.uidCheckInfo.tableUidList);
  SIOCostSummary* pCost = &pReader->cost;

  SFilesetIter* pFilesetIter = &pReader->status.fileIter;
  if (pFilesetIter->pLastBlockReader != NULL) {
    SLastBlockReader* pLReader = pFilesetIter->pLastBlockReader;
    tMergeTreeClose(&pLReader->mergeTree);

    getLastBlockLoadInfo(pLReader->pInfo, &pCost->lastBlockLoad, &pCost->lastBlockLoadTime);

    pLReader->pInfo = destroyLastBlockLoadInfo(pLReader->pInfo);
    taosMemoryFree(pLReader);
  }

  tsdbDebug(
      "%p :io-cost summary: head-file:%" PRIu64 ", head-file time:%.2f ms, SMA:%" PRId64
      " SMA-time:%.2f ms, fileBlocks:%" PRId64
      ", fileBlocks-load-time:%.2f ms, "
      "build in-memory-block-time:%.2f ms, lastBlocks:%" PRId64 ", lastBlocks-time:%.2f ms, composed-blocks:%" PRId64
      ", composed-blocks-time:%.2fms, STableBlockScanInfo size:%.2f Kb, createTime:%.2f ms,initDelSkylineIterTime:%.2f ms, %s",
      pReader, pCost->headFileLoad, pCost->headFileLoadTime, pCost->smaDataLoad, pCost->smaLoadTime, pCost->numOfBlocks,
      pCost->blockLoadTime, pCost->buildmemBlock, pCost->lastBlockLoad, pCost->lastBlockLoadTime, pCost->composedBlocks,
      pCost->buildComposedBlockTime, numOfTables * sizeof(STableBlockScanInfo) / 1000.0, pCost->createScanInfoList,
      pCost->initDelSkylineIterTime, pReader->idStr);

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
  if (taosHashGetSize(pStatus->pTableMap) == 0) {
    return false;
  }

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
}

bool tsdbNextDataBlock(STsdbReader* pReader) {
  if (isEmptyQueryTimeWindow(&pReader->window)) {
    return false;
  }

  if (pReader->innerReader[0] != NULL && pReader->step == 0) {
    bool ret = doTsdbNextDataBlock(pReader->innerReader[0]);
    pReader->step = EXTERNAL_ROWS_PREV;
    if (ret) {
      return ret;
    }
  }

  if (pReader->step == EXTERNAL_ROWS_PREV) {
    // prepare for the main scan
    int32_t code = doOpenReaderImpl(pReader);
    resetAllDataBlockScanInfo(pReader->status.pTableMap, pReader->innerReader[0]->window.ekey);

    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pReader->step = EXTERNAL_ROWS_MAIN;
  }

  bool ret = doTsdbNextDataBlock(pReader);
  if (ret) {
    return ret;
  }

  if (pReader->innerReader[1] != NULL && pReader->step == EXTERNAL_ROWS_MAIN) {
    // prepare for the next row scan
    int32_t code = doOpenReaderImpl(pReader->innerReader[1]);
    resetAllDataBlockScanInfo(pReader->innerReader[1]->status.pTableMap, pReader->window.ekey);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    bool ret1 = doTsdbNextDataBlock(pReader->innerReader[1]);
    pReader->step = EXTERNAL_ROWS_NEXT;
    if (ret1) {
      return ret1;
    }
  }

  return false;
}

static void doFillNullColSMA(SBlockLoadSuppInfo* pSup, int32_t numOfRows, int32_t numOfCols, SColumnDataAgg* pTsAgg) {
  // do fill all null column value SMA info
  int32_t i = 0, j = 0;
  int32_t size = (int32_t)taosArrayGetSize(pSup->pColAgg);
  taosArrayInsert(pSup->pColAgg, 0, pTsAgg);

  while (j < numOfCols && i < size) {
    SColumnDataAgg* pAgg = taosArrayGet(pSup->pColAgg, i);
    if (pAgg->colId == pSup->colId[j]) {
      i += 1;
      j += 1;
    } else if (pAgg->colId < pSup->colId[j]) {
      i += 1;
    } else if (pSup->colId[j] < pAgg->colId) {
      if (pSup->colId[j] != PRIMARYKEY_TIMESTAMP_COL_ID) {
        SColumnDataAgg nullColAgg = {.colId = pSup->colId[j], .numOfNull = numOfRows};
        taosArrayInsert(pSup->pColAgg, i, &nullColAgg);
      }
      j += 1;
    }
  }
}

int32_t tsdbRetrieveDatablockSMA(STsdbReader* pReader, SSDataBlock* pDataBlock, bool* allHave) {
  SColumnDataAgg*** pBlockSMA = &pDataBlock->pBlockAgg;

  int32_t code = 0;
  *allHave = false;
  *pBlockSMA = NULL;

  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    return TSDB_CODE_SUCCESS;
  }

  // there is no statistics data for composed block
  if (pReader->status.composedDataBlock || (!pReader->suppInfo.smaValid)) {
    return TSDB_CODE_SUCCESS;
  }

  SFileDataBlockInfo* pFBlock = getCurrentBlockInfo(&pReader->status.blockIter);
  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;

  if (pReader->pResBlock->info.id.uid != pFBlock->uid) {
    return TSDB_CODE_SUCCESS;
  }

  SDataBlk* pBlock = getCurrentBlock(&pReader->status.blockIter);
  if (tDataBlkHasSma(pBlock)) {
    code = tsdbReadBlockSma(pReader->pFileReader, pBlock, pSup->pColAgg);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbDebug("vgId:%d, failed to load block SMA for uid %" PRIu64 ", code:%s, %s", 0, pFBlock->uid, tstrerror(code),
                pReader->idStr);
      return code;
    }
  } else {
    *pBlockSMA = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *allHave = true;

  // always load the first primary timestamp column data
  SColumnDataAgg* pTsAgg = &pSup->tsColAgg;

  pTsAgg->numOfNull = 0;
  pTsAgg->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pTsAgg->min = pReader->pResBlock->info.window.skey;
  pTsAgg->max = pReader->pResBlock->info.window.ekey;

  // update the number of NULL data rows
  size_t numOfCols = pSup->numOfCols;

  // ensure capacity
  if (pDataBlock->pDataBlock) {
    size_t colsNum = taosArrayGetSize(pDataBlock->pDataBlock);
    taosArrayEnsureCap(pSup->pColAgg, colsNum);
  }

  SSDataBlock* pResBlock = pReader->pResBlock;
  if (pResBlock->pBlockAgg == NULL) {
    size_t num = taosArrayGetSize(pResBlock->pDataBlock);
    pResBlock->pBlockAgg = taosMemoryCalloc(num, sizeof(SColumnDataAgg));
  }

  // do fill all null column value SMA info
  doFillNullColSMA(pSup, pBlock->nRow, numOfCols, pTsAgg);
  size_t size = taosArrayGetSize(pSup->pColAgg);

  int32_t i = 0, j = 0;
  while (j < numOfCols && i < size) {
    SColumnDataAgg* pAgg = taosArrayGet(pSup->pColAgg, i);
    if (pAgg->colId == pSup->colId[j]) {
      pResBlock->pBlockAgg[pSup->slotId[j]] = pAgg;
      i += 1;
      j += 1;
    } else if (pAgg->colId < pSup->colId[j]) {
      i += 1;
    } else if (pSup->colId[j] < pAgg->colId) {
      // ASSERT(pSup->colId[j] == PRIMARYKEY_TIMESTAMP_COL_ID);
      pResBlock->pBlockAgg[pSup->slotId[j]] = &pSup->tsColAgg;
      j += 1;
    }
  }

  *pBlockSMA = pResBlock->pBlockAgg;
  pReader->cost.smaDataLoad += 1;

  tsdbDebug("vgId:%d, succeed to load block SMA for uid %" PRIu64 ", %s", 0, pFBlock->uid, pReader->idStr);
  return code;
}

static SSDataBlock* doRetrieveDataBlock(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;

  if (pStatus->composedDataBlock) {
    return pReader->pResBlock;
  }

  SFileDataBlockInfo*  pBlockInfo = getCurrentBlockInfo(&pStatus->blockIter);
  STableBlockScanInfo* pBlockScanInfo =
      *(STableBlockScanInfo**)taosHashGet(pStatus->pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
  if (pBlockScanInfo == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    tsdbError("failed to locate the uid:%" PRIu64 " in query table uid list, total tables:%d, %s", pBlockInfo->uid,
              taosHashGetSize(pReader->status.pTableMap), pReader->idStr);
    return NULL;
  }

  int32_t code = doLoadFileBlockData(pReader, &pStatus->blockIter, &pStatus->fileBlockData, pBlockScanInfo->uid);
  if (code != TSDB_CODE_SUCCESS) {
    tBlockDataDestroy(&pStatus->fileBlockData, 1);
    terrno = code;
    return NULL;
  }

  copyBlockDataToSDataBlock(pReader);
  return pReader->pResBlock;
}

SSDataBlock* tsdbRetrieveDataBlock(STsdbReader* pReader, SArray* pIdList) {
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
  if (isEmptyQueryTimeWindow(&pReader->window) || pReader->pReadSnap == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  pReader->order = pCond->order;
  pReader->type = TIMEWINDOW_RANGE_CONTAINED;
  pReader->status.loadFromFile = true;
  pReader->status.pTableIter = NULL;
  pReader->window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows);

  // allocate buffer in order to load data blocks from file
  memset(&pReader->suppInfo.tsColAgg, 0, sizeof(SColumnDataAgg));

  pReader->suppInfo.tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tsdbDataFReaderClose(&pReader->pFileReader);

  int32_t numOfTables = taosHashGetSize(pReader->status.pTableMap);

  initFilesetIterator(&pReader->status.fileIter, pReader->pReadSnap->fs.aDFileSet, pReader);
  resetDataBlockIterator(pBlockIter, pReader->order);

  int64_t ts = ASCENDING_TRAVERSE(pReader->order) ? pReader->window.skey - 1 : pReader->window.ekey + 1;
  resetAllDataBlockScanInfo(pReader->status.pTableMap, ts);

  int32_t code = 0;

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

  tsdbDebug("%p reset reader, suid:%" PRIu64 ", numOfTables:%d, skey:%" PRId64 ", query range:%" PRId64 " - %" PRId64
            " in query %s",
            pReader, pReader->suid, numOfTables, pCond->twindows.skey, pReader->window.skey, pReader->window.ekey,
            pReader->idStr);

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
      SDataBlk* pBlock = getCurrentBlock(pBlockIter);

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

      pTableBlockInfo->totalSize += pBlock->aSubBlock[0].szBlock;

      int32_t bucketIndex = getBucketIndex(pTableBlockInfo->defMinRows, bucketRange, numOfRows);
      pTableBlockInfo->blockRowsHisto[bucketIndex]++;

      hasNext = blockIteratorNext(&pStatus->blockIter, pReader->idStr);
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
    STableBlockScanInfo* pBlockScanInfo = *(STableBlockScanInfo**)pStatus->pTableIter;

    STbData* d = NULL;
    if (pReader->pReadSnap->pMem != NULL) {
      d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->suid, pBlockScanInfo->uid);
      if (d != NULL) {
        rows += tsdbGetNRowsInTbData(d);
      }
    }

    STbData* di = NULL;
    if (pReader->pReadSnap->pIMem != NULL) {
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
  int32_t code = metaGetTableEntryByUidCache(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    metaReaderClear(&mr);
    return terrno;
  }

  *suid = 0;

  if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);
    *suid = mr.me.ctbEntry.suid;
    code = metaGetTableEntryByUidCache(&mr, *suid);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      metaReaderClear(&mr);
      return terrno;
    }
    sversion = mr.me.stbEntry.schemaRow.version;
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {
    sversion = mr.me.ntbEntry.schemaRow.version;
  } else {
    terrno = TSDB_CODE_INVALID_PARA;
    metaReaderClear(&mr);
    return terrno;
  }

  metaReaderClear(&mr);
  *pSchema = metaGetTbTSchema(pVnode->pMeta, uid, sversion, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbTakeReadSnap(STsdb* pTsdb, STsdbReadSnap** ppSnap, const char* idStr) {
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

  tsdbTrace("vgId:%d, take read snapshot, %s", TD_VID(pTsdb->pVnode), idStr);
_exit:
  return code;
}

void tsdbUntakeReadSnap(STsdb* pTsdb, STsdbReadSnap* pSnap, const char* idStr) {
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
  tsdbTrace("vgId:%d, untake read snapshot, %s", TD_VID(pTsdb->pVnode), idStr);
}
