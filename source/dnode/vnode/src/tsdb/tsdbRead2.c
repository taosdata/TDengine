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
#include "tsdbDataFileRW.h"
#include "tsdbFS2.h"
#include "tsdbMerge.h"
#include "tsdbReadUtil.h"
#include "tsdbUtil2.h"
#include "tsimplehash.h"

#define ASCENDING_TRAVERSE(o)       (o == TSDB_ORDER_ASC)
#define getCurrentKeyInSttBlock(_r) ((_r)->currentKey)

typedef struct {
  bool overlapWithNeighborBlock;
  bool hasDupTs;
  bool overlapWithDelInfo;
  bool overlapWithSttBlock;
  bool overlapWithKeyInBuf;
  bool partiallyRequired;
  bool moreThanCapcity;
} SDataBlockToLoadInfo;

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter);
static int32_t  buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                          STsdbReader* pReader);
static TSDBROW* getValidMemRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader);
static int32_t  doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, STsdbReader* pReader);
static int32_t  doMergeRowsInSttBlock(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, int64_t ts,
                                      SRowMerger* pMerger, SVersionRange* pVerRange, const char* id);
static int32_t  doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, int64_t ts, SArray* pDelList, STsdbReader* pReader);
static int32_t  doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, SRow* pTSRow,
                                     STableBlockScanInfo* pScanInfo);
static int32_t  doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                         int32_t rowIndex);
static void     setComposedBlockFlag(STsdbReader* pReader, bool composed);
static bool     hasBeenDropped(const SArray* pDelList, int32_t* index, int64_t key, int64_t ver, int32_t order,
                               SVersionRange* pVerRange);

static int32_t doMergeMemTableMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList,
                                        TSDBROW* pResRow, STsdbReader* pReader, bool* freeTSRow);
static int32_t doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo,
                                  STsdbReader* pReader, SRow** pTSRow);
static int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, int64_t key,
                                     STsdbReader* pReader);
static int32_t mergeRowsInSttBlocks(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pBlockScanInfo,
                                    STsdbReader* pReader);

static int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, int32_t order, SReadCostSummary* pCost);
static STsdb* getTsdbByRetentions(SVnode* pVnode, SQueryTableDataCond* pCond, SRetention* retentions, const char* idstr,
                                  int8_t* pLevel);
static SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level);
static int32_t       doBuildDataBlock(STsdbReader* pReader);
static TSDBKEY       getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader);
static bool          hasDataInFileBlock(const SBlockData* pBlockData, const SFileBlockDumpInfo* pDumpInfo);
static bool          hasDataInSttBlock(STableBlockScanInfo* pInfo);
static void          initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter);
static int32_t       getInitialDelIndex(const SArray* pDelSkyline, int32_t order);
static void          resetTableListIndex(SReaderStatus* pStatus);
static void          getMemTableTimeRange(STsdbReader* pReader, int64_t* pMaxKey, int64_t* pMinKey);
static void          updateComposedBlockInfo(STsdbReader* pReader, double el, STableBlockScanInfo* pBlockScanInfo);
static int32_t       buildFromPreFilesetBuffer(STsdbReader* pReader);

static bool outOfTimeWindow(int64_t ts, STimeWindow* pWindow) { return (ts > pWindow->ekey) || (ts < pWindow->skey); }

static void resetPreFilesetMemTableListIndex(SReaderStatus* pStatus);

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
    } else if (pTCol->colId < pSupInfo->colId[j]) {  // do nothing
      i += 1;
    } else {
      return TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
    }
  }

  return TSDB_CODE_SUCCESS;
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
static int32_t initFilesetIterator(SFilesetIter* pIter, TFileSetArray* pFileSetArray, STsdbReader* pReader) {
  size_t numOfFileset = TARRAY2_SIZE(pFileSetArray);

  pIter->index = ASCENDING_TRAVERSE(pReader->info.order) ? -1 : numOfFileset;
  pIter->order = pReader->info.order;
  pIter->pFilesetList = pFileSetArray;
  pIter->numOfFiles = numOfFileset;

  if (pIter->pSttBlockReader == NULL) {
    pIter->pSttBlockReader = taosMemoryCalloc(1, sizeof(struct SSttBlockReader));
    if (pIter->pSttBlockReader == NULL) {
      int32_t code = TSDB_CODE_OUT_OF_MEMORY;
      tsdbError("failed to prepare the last block iterator, since:%s %s", tstrerror(code), pReader->idStr);
      return code;
    }
  }

  SSttBlockReader* pLReader = pIter->pSttBlockReader;
  pLReader->order = pReader->info.order;
  pLReader->window = pReader->info.window;
  pLReader->verRange = pReader->info.verRange;

  pLReader->uid = 0;
  tMergeTreeClose(&pLReader->mergeTree);
  tsdbDebug("init fileset iterator, total files:%d %s", pIter->numOfFiles, pReader->idStr);
  return TSDB_CODE_SUCCESS;
}

static int32_t filesetIteratorNext(SFilesetIter* pIter, STsdbReader* pReader, bool* hasNext) {
  bool    asc = ASCENDING_TRAVERSE(pIter->order);
  int32_t step = asc ? 1 : -1;
  int32_t code = 0;

  pIter->index += step;
  if ((asc && pIter->index >= pIter->numOfFiles) || ((!asc) && pIter->index < 0)) {
    *hasNext = false;
    return TSDB_CODE_SUCCESS;
  }

  SReadCostSummary* pCost = &pReader->cost;

  pIter->pSttBlockReader->uid = 0;
  tMergeTreeClose(&pIter->pSttBlockReader->mergeTree);
  pReader->status.pLDataIterArray = destroySttBlockReader(pReader->status.pLDataIterArray, &pCost->sttCost);
  pReader->status.pLDataIterArray = taosArrayInit(4, POINTER_BYTES);

  // check file the time range of coverage
  STimeWindow win = {0};

  while (1) {
    if (pReader->pFileReader != NULL) {
      tsdbDataFileReaderClose(&pReader->pFileReader);
    }

    pReader->status.pCurrentFileset = pIter->pFilesetList->data[pIter->index];

    STFileObj** pFileObj = pReader->status.pCurrentFileset->farr;
    if (pFileObj[0] != NULL || pFileObj[3] != NULL) {
      SDataFileReaderConfig conf = {.tsdb = pReader->pTsdb, .szPage = pReader->pTsdb->pVnode->config.tsdbPageSize};

      const char* filesName[4] = {0};

      if (pFileObj[0] != NULL) {
        conf.files[0].file = *pFileObj[0]->f;
        conf.files[0].exist = true;
        filesName[0] = pFileObj[0]->fname;

        conf.files[1].file = *pFileObj[1]->f;
        conf.files[1].exist = true;
        filesName[1] = pFileObj[1]->fname;

        conf.files[2].file = *pFileObj[2]->f;
        conf.files[2].exist = true;
        filesName[2] = pFileObj[2]->fname;
      }

      if (pFileObj[3] != NULL) {
        conf.files[3].exist = true;
        conf.files[3].file = *pFileObj[3]->f;
        filesName[3] = pFileObj[3]->fname;
      }

      code = tsdbDataFileReaderOpen(filesName, &conf, &pReader->pFileReader);
      if (code != TSDB_CODE_SUCCESS) {
        goto _err;
      }

      pReader->cost.headFileLoad += 1;
    }

    int32_t fid = pReader->status.pCurrentFileset->fid;
    tsdbFidKeyRange(fid, pReader->pTsdb->keepCfg.days, pReader->pTsdb->keepCfg.precision, &win.skey, &win.ekey);

    // current file are no longer overlapped with query time window, ignore remain files
    if ((asc && win.skey > pReader->info.window.ekey) || (!asc && win.ekey < pReader->info.window.skey)) {
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, %s", pReader,
                pReader->info.window.skey, pReader->info.window.ekey, pReader->idStr);
      *hasNext = false;
      return TSDB_CODE_SUCCESS;
    }

    if ((asc && (win.ekey < pReader->info.window.skey)) || ((!asc) && (win.skey > pReader->info.window.ekey))) {
      pIter->index += step;
      if ((asc && pIter->index >= pIter->numOfFiles) || ((!asc) && pIter->index < 0)) {
        *hasNext = false;
        return TSDB_CODE_SUCCESS;
      }
      continue;
    }

    tsdbDebug("%p file found fid:%d for qrange:%" PRId64 "-%" PRId64 ", %s", pReader, fid, pReader->info.window.skey,
              pReader->info.window.ekey, pReader->idStr);

    *hasNext = true;
    return TSDB_CODE_SUCCESS;
  }

_err:
  *hasNext = false;
  return code;
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

static int32_t tsdbInitReaderLock(STsdbReader* pReader) {
  int32_t code = taosThreadMutexInit(&pReader->readerMutex, NULL);
  qTrace("tsdb/read: %p, post-init read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  return code;
}

static int32_t tsdbUninitReaderLock(STsdbReader* pReader) {
  int32_t code = -1;
  qTrace("tsdb/read: %p, pre-uninit read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  code = taosThreadMutexDestroy(&pReader->readerMutex);

  qTrace("tsdb/read: %p, post-uninit read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  return code;
}

static int32_t tsdbAcquireReader(STsdbReader* pReader) {
  int32_t code = -1;
  qTrace("tsdb/read: %p, pre-take read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  code = taosThreadMutexLock(&pReader->readerMutex);

  qTrace("tsdb/read: %p, post-take read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  return code;
}

static int32_t tsdbTryAcquireReader(STsdbReader* pReader) {
  int32_t code = taosThreadMutexTryLock(&pReader->readerMutex);
  qTrace("tsdb/read: %p, post-trytake read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  return code;
}

static int32_t tsdbReleaseReader(STsdbReader* pReader) {
  int32_t code = taosThreadMutexUnlock(&pReader->readerMutex);
  qTrace("tsdb/read: %p, post-untake read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);

  return code;
}

void tsdbReleaseDataBlock2(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;
  if (!pStatus->composedDataBlock) {
    tsdbReleaseReader(pReader);
  }
}

static int32_t initResBlockInfo(SResultBlockInfo* pResBlockInfo, int64_t capacity, SSDataBlock* pResBlock,
                                SQueryTableDataCond* pCond) {
  pResBlockInfo->capacity = capacity;
  pResBlockInfo->pResBlock = pResBlock;
  terrno = 0;

  if (pResBlockInfo->pResBlock == NULL) {
    pResBlockInfo->freeBlock = true;
    pResBlockInfo->pResBlock = createResBlock(pCond, pResBlockInfo->capacity);
  } else {
    pResBlockInfo->freeBlock = false;
  }

  return terrno;
}

static int32_t tsdbReaderCreate(SVnode* pVnode, SQueryTableDataCond* pCond, void** ppReader, int32_t capacity,
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

  pReader->pTsdb = getTsdbByRetentions(pVnode, pCond, pVnode->config.tsdbCfg.retentions, idstr, &level);
  pReader->info.suid = pCond->suid;
  pReader->info.order = pCond->order;

  pReader->idStr = (idstr != NULL) ? taosStrdup(idstr) : NULL;
  pReader->info.verRange = getQueryVerRange(pVnode, pCond, level);
  pReader->type = pCond->type;
  pReader->info.window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows);
  pReader->blockInfoBuf.numPerBucket = 1000;  // 1000 tables per bucket

  code = initResBlockInfo(&pReader->resBlockInfo, capacity, pResBlock, pCond);
  if (code != TSDB_CODE_SUCCESS) {
    goto _end;
  }

  if (pCond->numOfCols <= 0) {
    tsdbError("vgId:%d, invalid column number %d in query cond, %s", TD_VID(pVnode), pCond->numOfCols, idstr);
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  // allocate buffer in order to load data blocks from file
  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;
  pSup->tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  setColumnIdSlotList(pSup, pCond->colList, pCond->pSlotList, pCond->numOfCols);

  code = tBlockDataCreate(&pReader->status.fileBlockData);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    goto _end;
  }

  if (pReader->suppInfo.colId[0] != PRIMARYKEY_TIMESTAMP_COL_ID) {
    tsdbError("the first column isn't primary timestamp, %d, %s", pReader->suppInfo.colId[0], pReader->idStr);
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  pReader->status.pPrimaryTsCol = taosArrayGet(pReader->resBlockInfo.pResBlock->pDataBlock, pSup->slotId[0]);
  int32_t type = pReader->status.pPrimaryTsCol->info.type;
  if (type != TSDB_DATA_TYPE_TIMESTAMP) {
    tsdbError("the first column isn't primary timestamp in result block, actual: %s, %s", tDataTypes[type].name,
              pReader->idStr);
    code = TSDB_CODE_INVALID_PARA;
    goto _end;
  }

  pReader->bFilesetDelimited = false;

  tsdbInitReaderLock(pReader);
  tsem_init(&pReader->resumeAfterSuspend, 0, 0);

  *ppReader = pReader;
  return code;

_end:
  tsdbReaderClose2(pReader);
  *ppReader = NULL;
  return code;
}

static int32_t doLoadBlockIndex(STsdbReader* pReader, SDataFileReader* pFileReader, SArray* pIndexList) {
  int64_t st = taosGetTimestampUs();
  int32_t numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  if (pFileReader == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  const TBrinBlkArray* pBlkArray = NULL;

  int32_t code = tsdbDataFileReadBrinBlk(pFileReader, &pBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

#if 0
  LRUHandle* handle = NULL;

  int32_t    code = tsdbCacheGetBlockIdx(pFileReader->pTsdb->biCache, pFileReader, &handle);
  if (code != TSDB_CODE_SUCCESS || handle == NULL) {
    goto _end;
  }


  SArray* aBlockIdx = (SArray*)taosLRUCacheValue(pFileReader->pTsdb->biCache, handle);
  size_t  num = taosArrayGetSize(aBlockIdx);
  if (num == 0) {
    tsdbBICacheRelease(pFileReader->pTsdb->biCache, handle);
    return TSDB_CODE_SUCCESS;
  }
#endif

  // todo binary search to the start position
  int64_t et1 = taosGetTimestampUs();

  SBrinBlk*      pBrinBlk = NULL;
  STableUidList* pList = &pReader->status.uidList;

  int32_t i = 0;

  while (i < TARRAY2_SIZE(pBlkArray)) {
    pBrinBlk = &pBlkArray->data[i];
    if (pBrinBlk->maxTbid.suid < pReader->info.suid) {
      i += 1;
      continue;
    }

    if (pBrinBlk->minTbid.suid > pReader->info.suid) {  // not include the queried table/super table, quit the loop
      break;
    }

    ASSERT(pBrinBlk->minTbid.suid <= pReader->info.suid && pBrinBlk->maxTbid.suid >= pReader->info.suid);
    if (pBrinBlk->maxTbid.suid == pReader->info.suid && pBrinBlk->maxTbid.uid < pList->tableUidList[0]) {
      i += 1;
      continue;
    }

    if (pBrinBlk->minTbid.suid == pReader->info.suid && pBrinBlk->minTbid.uid > pList->tableUidList[numOfTables - 1]) {
      break;
    }

    taosArrayPush(pIndexList, pBrinBlk);
    i += 1;
  }

  int64_t et2 = taosGetTimestampUs();
  tsdbDebug("load block index for %d/%d tables completed, elapsed time:%.2f ms, set BrinBlk:%.2f ms, size:%.2f Kb %s",
            numOfTables, (int32_t)pBlkArray->size, (et1 - st) / 1000.0, (et2 - et1) / 1000.0,
            pBlkArray->size * sizeof(SBrinBlk) / 1024.0, pReader->idStr);

  pReader->cost.headFileLoadTime += (et1 - st) / 1000.0;

_end:
  //  tsdbBICacheRelease(pFileReader->pTsdb->biCache, handle);
  return code;
}

static int32_t doLoadFileBlock(STsdbReader* pReader, SArray* pIndexList, SBlockNumber* pBlockNum,
                               SArray* pTableScanInfoList) {
  size_t  sizeInDisk = 0;
  int64_t st = taosGetTimestampUs();

  // clear info for the new file
  cleanupInfoForNextFileset(pReader->status.pTableMap);

  int32_t      k = 0;
  int32_t      numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  int32_t      step = ASCENDING_TRAVERSE(pReader->info.order) ? 1 : -1;
  STimeWindow  w = pReader->info.window;
  SBrinRecord* pRecord = NULL;

  SBrinRecordIter iter = {0};
  initBrinRecordIter(&iter, pReader->pFileReader, pIndexList);

  while (((pRecord = getNextBrinRecord(&iter)) != NULL)) {
    if (pRecord->suid > pReader->info.suid) {
      break;
    }

    uint64_t uid = pReader->status.uidList.tableUidList[k];
    if (pRecord->suid < pReader->info.suid) {
      continue;
    }

    if (uid < pRecord->uid) {  // forward the table uid index
      while (k < numOfTables && pReader->status.uidList.tableUidList[k] < pRecord->uid) {
        k += 1;
      }

      if (k >= numOfTables) {
        break;
      }

      uid = pReader->status.uidList.tableUidList[k];
    }

    if (pRecord->uid < uid) {
      continue;
    }

    ASSERT(pRecord->suid == pReader->info.suid && uid == pRecord->uid);

    STableBlockScanInfo* pScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, uid, pReader->idStr);
    if (ASCENDING_TRAVERSE(pReader->info.order)) {
      w.skey = pScanInfo->lastProcKey + step;
    } else {
      w.ekey = pScanInfo->lastProcKey + step;
    }

    if (isEmptyQueryTimeWindow(&w)) {
      k += 1;

      if (k >= numOfTables) {
        break;
      } else {
        continue;
      }
    }

    // 1. time range check
    if (pRecord->firstKey > w.ekey || pRecord->lastKey < w.skey) {
      continue;
    }

    // 2. version range check
    if (pRecord->minVer > pReader->info.verRange.maxVer || pRecord->maxVer < pReader->info.verRange.minVer) {
      continue;
    }

    if (pScanInfo->pBlockList == NULL) {
      pScanInfo->pBlockList = taosArrayInit(4, sizeof(SBrinRecord));
    }

    void* p1 = taosArrayPush(pScanInfo->pBlockList, pRecord);
    if (p1 == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    if (pScanInfo->filesetWindow.skey > pRecord->firstKey) {
      pScanInfo->filesetWindow.skey = pRecord->firstKey;
    }
    if (pScanInfo->filesetWindow.ekey < pRecord->lastKey) {
      pScanInfo->filesetWindow.ekey = pRecord->lastKey;
    }

    pBlockNum->numOfBlocks += 1;
    if (taosArrayGetSize(pTableScanInfoList) == 0) {
      taosArrayPush(pTableScanInfoList, &pScanInfo);
    } else {
      STableBlockScanInfo** p = taosArrayGetLast(pTableScanInfoList);
      if ((*p)->uid != uid) {
        taosArrayPush(pTableScanInfoList, &pScanInfo);
      }
    }
  }

  clearBrinBlockIter(&iter);

  pBlockNum->numOfSttFiles = pReader->status.pCurrentFileset->lvlArr->size;
  int32_t total = pBlockNum->numOfSttFiles + pBlockNum->numOfBlocks;

  double el = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug(
      "load block of %d tables completed, blocks:%d in %d tables, stt-files:%d, block-info-size:%.2f Kb, elapsed "
      "time:%.2f ms %s",
      numOfTables, pBlockNum->numOfBlocks, (int32_t)taosArrayGetSize(pTableScanInfoList), pBlockNum->numOfSttFiles,
      sizeInDisk / 1000.0, el, pReader->idStr);

  pReader->cost.numOfBlocks += total;
  pReader->cost.headFileLoadTime += el;

  return TSDB_CODE_SUCCESS;
}

static void setBlockAllDumped(SFileBlockDumpInfo* pDumpInfo, int64_t maxKey, int32_t order) {
  int32_t step = ASCENDING_TRAVERSE(order) ? 1 : -1;
  pDumpInfo->allDumped = true;
  pDumpInfo->lastKey = maxKey + step;
}

static int32_t doCopyColVal(SColumnInfoData* pColInfoData, int32_t rowIndex, int32_t colIndex, SColVal* pColVal,
                            SBlockLoadSuppInfo* pSup) {
  if (IS_VAR_DATA_TYPE(pColVal->type)) {
    if (!COL_VAL_IS_VALUE(pColVal)) {
      colDataSetNULL(pColInfoData, rowIndex);
    } else {
      varDataSetLen(pSup->buildBuf[colIndex], pColVal->value.nData);
      if (pColVal->value.nData > pColInfoData->info.bytes) {
        tsdbWarn("column cid:%d actual data len %d is bigger than schema len %d", pColVal->cid, pColVal->value.nData,
                 pColInfoData->info.bytes);
        return TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
      }
      if (pColVal->value.nData > 0) {  // pData may be null, if nData is 0
        memcpy(varDataVal(pSup->buildBuf[colIndex]), pColVal->value.pData, pColVal->value.nData);
      }

      colDataSetVal(pColInfoData, rowIndex, pSup->buildBuf[colIndex], false);
    }
  } else {
    colDataSetVal(pColInfoData, rowIndex, (const char*)&pColVal->value, !COL_VAL_IS_VALUE(pColVal));
  }

  return TSDB_CODE_SUCCESS;
}

static SFileDataBlockInfo* getCurrentBlockInfo(SDataBlockIter* pBlockIter) {
  if (pBlockIter->blockList == NULL) {
    return NULL;
  }

  size_t num = TARRAY_SIZE(pBlockIter->blockList);
  if (num == 0) {
    ASSERT(pBlockIter->numOfBlocks == num);
    return NULL;
  }

  SFileDataBlockInfo* pBlockInfo = taosArrayGet(pBlockIter->blockList, pBlockIter->index);
  return pBlockInfo;
}

static int doBinarySearchKey(TSKEY* keyList, int num, int pos, TSKEY key, int order) {
  // start end position
  int s, e;
  s = pos;

  // check
  ASSERT(pos >= 0 && pos < num && num > 0);
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

static int32_t getEndPosInDataBlock(STsdbReader* pReader, SBlockData* pBlockData, SBrinRecord* pRecord, int32_t pos) {
  // NOTE: reverse the order to find the end position in data block
  int32_t endPos = -1;
  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);

  if (asc && pReader->info.window.ekey >= pRecord->lastKey) {
    endPos = pRecord->numRow - 1;
  } else if (!asc && pReader->info.window.skey <= pRecord->firstKey) {
    endPos = 0;
  } else {
    int64_t key = asc ? pReader->info.window.ekey : pReader->info.window.skey;
    endPos = doBinarySearchKey(pBlockData->aTSKEY, pRecord->numRow, pos, key, pReader->info.order);
  }

  if ((pReader->info.verRange.maxVer >= pRecord->minVer && pReader->info.verRange.maxVer < pRecord->maxVer) ||
      (pReader->info.verRange.minVer <= pRecord->maxVer && pReader->info.verRange.minVer > pRecord->minVer)) {
    int32_t i = endPos;

    if (asc) {
      for (; i >= 0; --i) {
        if (pBlockData->aVersion[i] <= pReader->info.verRange.maxVer) {
          break;
        }
      }
    } else {
      for (; i < pRecord->numRow; ++i) {
        if (pBlockData->aVersion[i] >= pReader->info.verRange.minVer) {
          break;
        }
      }
    }

    endPos = i;
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

static void blockInfoToRecord(SBrinRecord* record, SFileDataBlockInfo* pBlockInfo){
  record->uid = pBlockInfo->uid;
  record->firstKey = pBlockInfo->firstKey;
  record->lastKey = pBlockInfo->lastKey;
  record->minVer = pBlockInfo->minVer;
  record->maxVer = pBlockInfo->maxVer;
  record->blockOffset = pBlockInfo->blockOffset;
  record->smaOffset = pBlockInfo->smaOffset;
  record->blockSize = pBlockInfo->blockSize;
  record->blockKeySize = pBlockInfo->blockKeySize;
  record->smaSize = pBlockInfo->smaSize;
  record->numRow = pBlockInfo->numRow;
  record->count = pBlockInfo->count;
}

static int32_t copyBlockDataToSDataBlock(STsdbReader* pReader) {
  SReaderStatus*      pStatus = &pReader->status;
  SDataBlockIter*     pBlockIter = &pStatus->blockIter;
  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  SBlockData*         pBlockData = &pStatus->fileBlockData;
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SSDataBlock*        pResBlock = pReader->resBlockInfo.pResBlock;
  int32_t             numOfOutputCols = pSupInfo->numOfCols;
  int32_t             code = TSDB_CODE_SUCCESS;

  SColVal cv = {0};
  int64_t st = taosGetTimestampUs();
  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);
  int32_t step = asc ? 1 : -1;

  SBrinRecord tmp;
  blockInfoToRecord(&tmp, pBlockInfo);
  SBrinRecord* pRecord = &tmp;

  // no data exists, return directly.
  if (pBlockData->nRow == 0 || pBlockData->aTSKEY == 0) {
    tsdbWarn("%p no need to copy since no data in blockData, table uid:%" PRIu64 " has been dropped, %s", pReader,
             pBlockInfo->uid, pReader->idStr);
    pResBlock->info.rows = 0;
    return 0;
  }

  // row index of dump info remain the initial position, let's find the appropriate start position.
  if ((pDumpInfo->rowIndex == 0 && asc) || (pDumpInfo->rowIndex == pRecord->numRow - 1 && (!asc))) {
    if (asc && pReader->info.window.skey <= pRecord->firstKey && pReader->info.verRange.minVer <= pRecord->minVer) {
      // pDumpInfo->rowIndex = 0;
    } else if (!asc && pReader->info.window.ekey >= pRecord->lastKey &&
               pReader->info.verRange.maxVer >= pRecord->maxVer) {
      // pDumpInfo->rowIndex = pRecord->numRow - 1;
    } else {  // find the appropriate the start position in current block, and set it to be the current rowIndex
      int32_t pos = asc ? pRecord->numRow - 1 : 0;
      int32_t order = asc ? TSDB_ORDER_DESC : TSDB_ORDER_ASC;
      int64_t key = asc ? pReader->info.window.skey : pReader->info.window.ekey;
      pDumpInfo->rowIndex = doBinarySearchKey(pBlockData->aTSKEY, pRecord->numRow, pos, key, order);

      if (pDumpInfo->rowIndex < 0) {
        tsdbError(
            "%p failed to locate the start position in current block, global index:%d, table index:%d, brange:%" PRId64
            "-%" PRId64 ", minVer:%" PRId64 ", maxVer:%" PRId64 " %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pRecord->firstKey, pRecord->lastKey, pRecord->minVer,
            pRecord->maxVer, pReader->idStr);
        return TSDB_CODE_INVALID_PARA;
      }

      ASSERT(pReader->info.verRange.minVer <= pRecord->maxVer && pReader->info.verRange.maxVer >= pRecord->minVer);

      // find the appropriate start position that satisfies the version requirement.
      if ((pReader->info.verRange.maxVer >= pRecord->minVer && pReader->info.verRange.maxVer < pRecord->maxVer) ||
          (pReader->info.verRange.minVer <= pRecord->maxVer && pReader->info.verRange.minVer > pRecord->minVer)) {
        int32_t i = pDumpInfo->rowIndex;
        if (asc) {
          for (; i < pRecord->numRow; ++i) {
            if (pBlockData->aVersion[i] >= pReader->info.verRange.minVer) {
              break;
            }
          }
        } else {
          for (; i >= 0; --i) {
            if (pBlockData->aVersion[i] <= pReader->info.verRange.maxVer) {
              break;
            }
          }
        }

        pDumpInfo->rowIndex = i;
      }
    }
  }

  // time window check
  int32_t endIndex = getEndPosInDataBlock(pReader, pBlockData, pRecord, pDumpInfo->rowIndex);
  if (endIndex == -1) {
    setBlockAllDumped(pDumpInfo, pReader->info.window.ekey, pReader->info.order);
    return TSDB_CODE_SUCCESS;
  }

  endIndex += step;
  int32_t dumpedRows = asc ? (endIndex - pDumpInfo->rowIndex) : (pDumpInfo->rowIndex - endIndex);
  if (dumpedRows > pReader->resBlockInfo.capacity) {  // output buffer check
    dumpedRows = pReader->resBlockInfo.capacity;
  } else if (dumpedRows <= 0) {  // no qualified rows in current data block, abort directly.
    setBlockAllDumped(pDumpInfo, pReader->info.window.ekey, pReader->info.order);
    return TSDB_CODE_SUCCESS;
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
        colDataSetNNULL(pColData, 0, dumpedRows);
      } else {
        if (IS_MATHABLE_TYPE(pColData->info.type)) {
          copyNumericCols(pData, pDumpInfo, pColData, dumpedRows, asc);
        } else {  // varchar/nchar type
          for (int32_t j = pDumpInfo->rowIndex; rowIndex < dumpedRows; j += step) {
            tColDataGetValue(pData, j, &cv);
            code = doCopyColVal(pColData, rowIndex++, i, &cv, pSupInfo);
            if (code) {
              return code;
            }
          }
        }
      }

      colIndex += 1;
      i += 1;
    } else {  // the specified column does not exist in file block, fill with null data
      pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
      colDataSetNNULL(pColData, 0, dumpedRows);
      i += 1;
    }
  }

  // fill the mis-matched columns with null value
  while (i < numOfOutputCols) {
    pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    colDataSetNNULL(pColData, 0, dumpedRows);
    i += 1;
  }

  pResBlock->info.dataLoad = 1;
  pResBlock->info.rows = dumpedRows;
  pDumpInfo->rowIndex += step * dumpedRows;

  // check if current block are all handled
  if (pDumpInfo->rowIndex >= 0 && pDumpInfo->rowIndex < pRecord->numRow) {
    int64_t ts = pBlockData->aTSKEY[pDumpInfo->rowIndex];
    if (outOfTimeWindow(ts, &pReader->info.window)) {
      // the remain data has out of query time window, ignore current block
      setBlockAllDumped(pDumpInfo, ts, pReader->info.order);
    }
  } else {
    int64_t ts = asc ? pRecord->lastKey : pRecord->firstKey;
    setBlockAllDumped(pDumpInfo, ts, pReader->info.order);
  }

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  pReader->cost.blockLoadTime += elapsedTime;

  int32_t unDumpedRows = asc ? pRecord->numRow - pDumpInfo->rowIndex : pDumpInfo->rowIndex + 1;
  tsdbDebug("%p copy file block to sdatablock, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, remain:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", uid:%" PRIu64 " elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pRecord->firstKey, pRecord->lastKey, dumpedRows,
            unDumpedRows, pRecord->minVer, pRecord->maxVer, pBlockInfo->uid, elapsedTime, pReader->idStr);

  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE STSchema* getTableSchemaImpl(STsdbReader* pReader, uint64_t uid) {
  ASSERT(pReader->info.pSchema == NULL);

  int32_t code = metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, uid, -1, &pReader->info.pSchema);
  if (code != TSDB_CODE_SUCCESS || pReader->info.pSchema == NULL) {
    terrno = code;
    tsdbError("failed to get table schema, uid:%" PRIu64 ", it may have been dropped, ver:-1, %s", uid, pReader->idStr);
    return NULL;
  }

  code = tsdbRowMergerInit(&pReader->status.merger, pReader->info.pSchema);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    tsdbError("failed to init merger, code:%s, %s", tstrerror(code), pReader->idStr);
    return NULL;
  }

  return pReader->info.pSchema;
}

static int32_t doLoadFileBlockData(STsdbReader* pReader, SDataBlockIter* pBlockIter, SBlockData* pBlockData,
                                   uint64_t uid) {
  int32_t   code = 0;
  STSchema* pSchema = pReader->info.pSchema;
  int64_t   st = taosGetTimestampUs();

  tBlockDataReset(pBlockData);

  if (pReader->info.pSchema == NULL) {
    pSchema = getTableSchemaImpl(pReader, uid);
    if (pSchema == NULL) {
      tsdbDebug("%p table uid:%" PRIu64 " has been dropped, no data existed, %s", pReader, uid, pReader->idStr);
      return code;
    }
  }

  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  SBrinRecord tmp;
  blockInfoToRecord(&tmp, pBlockInfo);
  SBrinRecord* pRecord = &tmp;
  code = tsdbDataFileReadBlockDataByColumn(pReader->pFileReader, pRecord, pBlockData, pSchema, &pSup->colId[1],
                                           pSup->numOfCols - 1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p error occurs in loading file block, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
              ", rows:%d, code:%s %s",
              pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlockInfo->firstKey,
              pBlockInfo->lastKey, pBlockInfo->numRow, tstrerror(code), pReader->idStr);
    return code;
  }

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;

  tsdbDebug("%p load file block into buffer, global index:%d, index in table block list:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pRecord->firstKey, pRecord->lastKey, pRecord->numRow,
            pRecord->minVer, pRecord->maxVer, elapsedTime, pReader->idStr);

  pReader->cost.blockLoadTime += elapsedTime;
  pDumpInfo->allDumped = false;

  return TSDB_CODE_SUCCESS;
}

/**
 * This is an two rectangles overlap cases.
 */
static int32_t dataBlockPartiallyRequired(STimeWindow* pWindow, SVersionRange* pVerRange, SFileDataBlockInfo* pBlock) {
  return (pWindow->ekey < pBlock->lastKey && pWindow->ekey >= pBlock->firstKey) ||
         (pWindow->skey > pBlock->firstKey && pWindow->skey <= pBlock->lastKey) ||
         (pVerRange->minVer > pBlock->minVer && pVerRange->minVer <= pBlock->maxVer) ||
         (pVerRange->maxVer < pBlock->maxVer && pVerRange->maxVer >= pBlock->minVer);
}

static bool getNeighborBlockOfSameTable(SDataBlockIter* pBlockIter, SFileDataBlockInfo* pBlockInfo,
                                        STableBlockScanInfo* pTableBlockScanInfo, int32_t* nextIndex, int32_t order,
                                        SBrinRecord* pRecord) {
  bool asc = ASCENDING_TRAVERSE(order);
  if (asc && pBlockInfo->tbBlockIdx >= taosArrayGetSize(pTableBlockScanInfo->pBlockIdxList) - 1) {
    return false;
  }

  if (!asc && pBlockInfo->tbBlockIdx == 0) {
    return false;
  }

  int32_t             step = asc ? 1 : -1;
  STableDataBlockIdx* pTableDataBlockIdx =
      taosArrayGet(pTableBlockScanInfo->pBlockIdxList, pBlockInfo->tbBlockIdx + step);
  SFileDataBlockInfo* p = taosArrayGet(pBlockIter->blockList, pTableDataBlockIdx->globalIndex);
  blockInfoToRecord(pRecord, p);

  *nextIndex = pBlockInfo->tbBlockIdx + step;
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

static int32_t setFileBlockActiveInBlockIter(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t index,
                                             int32_t step) {
  if (index < 0 || index >= pBlockIter->numOfBlocks) {
    return -1;
  }

  SFileDataBlockInfo fblock = *(SFileDataBlockInfo*)taosArrayGet(pBlockIter->blockList, index);
  pBlockIter->index += step;

  if (index != pBlockIter->index) {
    if (index > pBlockIter->index) {
      for (int32_t i = index - 1; i >= pBlockIter->index; --i) {
        SFileDataBlockInfo* pBlockInfo = taosArrayGet(pBlockIter->blockList, i);

        STableBlockScanInfo* pBlockScanInfo =
            getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, pReader->idStr);
        STableDataBlockIdx* pTableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, pBlockInfo->tbBlockIdx);
        pTableDataBlockIdx->globalIndex = i + 1;

        taosArraySet(pBlockIter->blockList, i + 1, pBlockInfo);
      }
    } else if (index < pBlockIter->index) {
      for (int32_t i = index + 1; i <= pBlockIter->index; ++i) {
        SFileDataBlockInfo* pBlockInfo = taosArrayGet(pBlockIter->blockList, i);

        STableBlockScanInfo* pBlockScanInfo =
            getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, pReader->idStr);
        STableDataBlockIdx* pTableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, pBlockInfo->tbBlockIdx);
        pTableDataBlockIdx->globalIndex = i - 1;

        taosArraySet(pBlockIter->blockList, i - 1, pBlockInfo);
      }
    }

    taosArraySet(pBlockIter->blockList, pBlockIter->index, &fblock);
    STableBlockScanInfo* pBlockScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, fblock.uid, pReader->idStr);
    STableDataBlockIdx*  pTableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, fblock.tbBlockIdx);
    pTableDataBlockIdx->globalIndex = pBlockIter->index;
  }

  return TSDB_CODE_SUCCESS;
}

// todo: this attribute could be acquired during extractin the global ordered block list.
static bool overlapWithNeighborBlock2(SFileDataBlockInfo* pBlock, SBrinRecord* pRec, int32_t order) {
  // it is the last block in current file, no chance to overlap with neighbor blocks.
  if (ASCENDING_TRAVERSE(order)) {
    return pBlock->lastKey == pRec->firstKey;
  } else {
    return pBlock->firstKey == pRec->lastKey;
  }
}

static int64_t getBoarderKeyInFiles(SFileDataBlockInfo* pBlock, STableBlockScanInfo* pScanInfo, int32_t order) {
  bool ascScan = ASCENDING_TRAVERSE(order);

  int64_t key = 0;
  if (pScanInfo->sttKeyInfo.status == STT_FILE_HAS_DATA) {
    int64_t keyInStt = pScanInfo->sttKeyInfo.nextProcKey;
    key = ascScan ? TMIN(pBlock->firstKey, keyInStt) : TMAX(pBlock->lastKey, keyInStt);
  } else {
    key = ascScan ? pBlock->firstKey : pBlock->lastKey;
  }

  return key;
}

static bool bufferDataInFileBlockGap(TSDBKEY keyInBuf, SFileDataBlockInfo* pBlock, STableBlockScanInfo* pScanInfo,
                                     int32_t order) {
  bool    ascScan = ASCENDING_TRAVERSE(order);
  int64_t key = getBoarderKeyInFiles(pBlock, pScanInfo, order);

  return (ascScan && (keyInBuf.ts != TSKEY_INITIAL_VAL && keyInBuf.ts < key)) ||
         (!ascScan && (keyInBuf.ts != TSKEY_INITIAL_VAL && keyInBuf.ts > key));
}

static bool keyOverlapFileBlock(TSDBKEY key, SFileDataBlockInfo* pBlock, SVersionRange* pVerRange) {
  return (key.ts >= pBlock->firstKey && key.ts <= pBlock->lastKey) &&
         (pBlock->maxVer >= pVerRange->minVer) && (pBlock->minVer <= pVerRange->maxVer);
}

static void getBlockToLoadInfo(SDataBlockToLoadInfo* pInfo, SFileDataBlockInfo* pBlockInfo,
                               STableBlockScanInfo* pScanInfo, TSDBKEY keyInBuf, STsdbReader* pReader) {
  SBrinRecord rec = {0};
  int32_t     neighborIndex = 0;

  bool hasNeighbor = getNeighborBlockOfSameTable(&pReader->status.blockIter, pBlockInfo, pScanInfo, &neighborIndex,
                                                 pReader->info.order, &rec);

  // overlap with neighbor
  if (hasNeighbor) {
    pInfo->overlapWithNeighborBlock = overlapWithNeighborBlock2(pBlockInfo, &rec, pReader->info.order);
  }

  SBrinRecord pRecord;
  blockInfoToRecord(&pRecord, pBlockInfo);
  // has duplicated ts of different version in this block
  pInfo->hasDupTs = (pBlockInfo->numRow > pBlockInfo->count) || (pBlockInfo->count <= 0);
  pInfo->overlapWithDelInfo = overlapWithDelSkyline(pScanInfo, &pRecord, pReader->info.order);

  ASSERT(pScanInfo->sttKeyInfo.status != STT_FILE_READER_UNINIT);
  if (pScanInfo->sttKeyInfo.status == STT_FILE_HAS_DATA) {
    int64_t nextProcKeyInStt = pScanInfo->sttKeyInfo.nextProcKey;
    pInfo->overlapWithSttBlock =
        !(pBlockInfo->lastKey < nextProcKeyInStt || pBlockInfo->firstKey > nextProcKeyInStt);
  }

  pInfo->moreThanCapcity = pBlockInfo->numRow > pReader->resBlockInfo.capacity;
  pInfo->partiallyRequired = dataBlockPartiallyRequired(&pReader->info.window, &pReader->info.verRange, pBlockInfo);
  pInfo->overlapWithKeyInBuf = keyOverlapFileBlock(keyInBuf, pBlockInfo, &pReader->info.verRange);
}

// 1. the version of all rows should be less than the endVersion
// 2. current block should not overlap with next neighbor block
// 3. current timestamp should not be overlap with each other
// 4. output buffer should be large enough to hold all rows in current block
// 5. delete info should not overlap with current block data
// 6. current block should not contain the duplicated ts
static bool fileBlockShouldLoad(STsdbReader* pReader, SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pScanInfo,
                                TSDBKEY keyInBuf) {
  SDataBlockToLoadInfo info = {0};
  getBlockToLoadInfo(&info, pBlockInfo, pScanInfo, keyInBuf, pReader);

  bool loadDataBlock =
      (info.overlapWithNeighborBlock || info.hasDupTs || info.partiallyRequired || info.overlapWithKeyInBuf ||
       info.moreThanCapcity || info.overlapWithDelInfo || info.overlapWithSttBlock);

  // log the reason why load the datablock for profile
  if (loadDataBlock) {
    tsdbDebug("%p uid:%" PRIu64
              " need to load the datablock, overlapneighbor:%d, hasDup:%d, partiallyRequired:%d, "
              "overlapWithKey:%d, greaterThanBuf:%d, overlapWithDel:%d, overlapWithSttBlock:%d, %s",
              pReader, pBlockInfo->uid, info.overlapWithNeighborBlock, info.hasDupTs, info.partiallyRequired,
              info.overlapWithKeyInBuf, info.moreThanCapcity, info.overlapWithDelInfo, info.overlapWithSttBlock,
              pReader->idStr);
  }

  return loadDataBlock;
}

static bool isCleanFileDataBlock(STsdbReader* pReader, SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pScanInfo,
                                 TSDBKEY keyInBuf) {
  SDataBlockToLoadInfo info = {0};
  getBlockToLoadInfo(&info, pBlockInfo, pScanInfo, keyInBuf, pReader);
  bool isCleanFileBlock = !(info.overlapWithNeighborBlock || info.hasDupTs || info.overlapWithKeyInBuf ||
                            info.overlapWithDelInfo || info.overlapWithSttBlock);
  return isCleanFileBlock;
}

static int32_t buildDataBlockFromBuf(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, int64_t endKey) {
  if (!(pBlockScanInfo->iiter.hasVal || pBlockScanInfo->iter.hasVal)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t      st = taosGetTimestampUs();
  SSDataBlock* pBlock = pReader->resBlockInfo.pResBlock;
  int32_t      code = buildDataBlockFromBufImpl(pBlockScanInfo, endKey, pReader->resBlockInfo.capacity, pReader);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  updateComposedBlockInfo(pReader, el, pBlockScanInfo);

  tsdbDebug("%p build data block from cache completed, elapsed time:%.2f ms, numOfRows:%" PRId64 ", brange:%" PRId64
            " - %" PRId64 ", uid:%" PRIu64 ",  %s",
            pReader, el, pBlock->info.rows, pBlock->info.window.skey, pBlock->info.window.ekey, pBlockScanInfo->uid,
            pReader->idStr);

  pReader->cost.buildmemBlock += el;
  return code;
}

static bool tryCopyDistinctRowFromFileBlock(STsdbReader* pReader, SBlockData* pBlockData, int64_t key,
                                            SFileBlockDumpInfo* pDumpInfo, bool* copied) {
  // opt version
  // 1. it is not a border point
  // 2. the direct next point is not an duplicated timestamp
  int32_t code = TSDB_CODE_SUCCESS;

  *copied = false;
  bool asc = (pReader->info.order == TSDB_ORDER_ASC);
  if ((pDumpInfo->rowIndex < pDumpInfo->totalRows - 1 && asc) || (pDumpInfo->rowIndex > 0 && (!asc))) {
    int32_t step = pReader->info.order == TSDB_ORDER_ASC ? 1 : -1;

    int64_t nextKey = pBlockData->aTSKEY[pDumpInfo->rowIndex + step];
    if (nextKey != key) {  // merge is not needed
      code = doAppendRowFromFileBlock(pReader->resBlockInfo.pResBlock, pReader, pBlockData, pDumpInfo->rowIndex);
      if (code) {
        return code;
      }
      pDumpInfo->rowIndex += step;
      *copied = true;
    }
  }

  return code;
}

static bool nextRowFromSttBlocks(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo,
                                 SVersionRange* pVerRange) {
  int32_t order = pSttBlockReader->order;
  int32_t step = ASCENDING_TRAVERSE(order) ? 1 : -1;

  while (1) {
    bool hasVal = tMergeTreeNext(&pSttBlockReader->mergeTree);
    if (!hasVal) {  // the next value will be the accessed key in stt
      pScanInfo->sttKeyInfo.status = STT_FILE_NO_DATA;
      pScanInfo->sttKeyInfo.nextProcKey += step;
      return false;
    }

    TSDBROW* pRow = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
    int64_t  key = pRow->pBlockData->aTSKEY[pRow->iRow];
    int64_t  ver = pRow->pBlockData->aVersion[pRow->iRow];

    pSttBlockReader->currentKey = key;
    pScanInfo->sttKeyInfo.nextProcKey = key;

    if (pScanInfo->delSkyline != NULL && TARRAY_SIZE(pScanInfo->delSkyline) > 0) {
      if (!hasBeenDropped(pScanInfo->delSkyline, &pScanInfo->sttBlockDelIndex, key, ver, order, pVerRange)) {
        pScanInfo->sttKeyInfo.status = STT_FILE_HAS_DATA;
        return true;
      }
    } else {
      pScanInfo->sttKeyInfo.status = STT_FILE_HAS_DATA;
      return true;
    }
  }
}

static void doPinSttBlock(SSttBlockReader* pSttBlockReader) { tMergeTreePinSttBlock(&pSttBlockReader->mergeTree); }

static void doUnpinSttBlock(SSttBlockReader* pSttBlockReader) { tMergeTreeUnpinSttBlock(&pSttBlockReader->mergeTree); }

static bool tryCopyDistinctRowFromSttBlock(TSDBROW* fRow, SSttBlockReader* pSttBlockReader,
                                           STableBlockScanInfo* pScanInfo, int64_t ts, STsdbReader* pReader,
                                           bool* copied) {
  int32_t code = TSDB_CODE_SUCCESS;
  *copied = false;

  // avoid the fetch next row replace the referenced stt block in buffer
  doPinSttBlock(pSttBlockReader);
  bool hasVal = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, &pReader->info.verRange);
  doUnpinSttBlock(pSttBlockReader);
  if (hasVal) {
    int64_t next1 = getCurrentKeyInSttBlock(pSttBlockReader);
    if (next1 != ts) {
      code = doAppendRowFromFileBlock(pReader->resBlockInfo.pResBlock, pReader, fRow->pBlockData, fRow->iRow);
      if (code) {
        return code;
      }

      *copied = true;
      return code;
    }
  } else {
    code = doAppendRowFromFileBlock(pReader->resBlockInfo.pResBlock, pReader, fRow->pBlockData, fRow->iRow);
    if (code) {
      return code;
    }

    *copied = true;
    return code;
  }

  return code;
}

static FORCE_INLINE STSchema* doGetSchemaForTSRow(int32_t sversion, STsdbReader* pReader, uint64_t uid) {
  // always set the newest schema version in pReader->info.pSchema
  if (pReader->info.pSchema == NULL) {
    STSchema* ps = getTableSchemaImpl(pReader, uid);
    if (ps == NULL) {
      return NULL;
    }
  }

  if (pReader->info.pSchema && sversion == pReader->info.pSchema->version) {
    return pReader->info.pSchema;
  }

  void** p = tSimpleHashGet(pReader->pSchemaMap, &sversion, sizeof(sversion));
  if (p != NULL) {
    return *(STSchema**)p;
  }

  STSchema* ptr = NULL;
  int32_t   code = metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, uid, sversion, &ptr);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    return NULL;
  } else {
    code = tSimpleHashPut(pReader->pSchemaMap, &sversion, sizeof(sversion), &ptr, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return NULL;
    }
    return ptr;
  }
}

static int32_t doMergeBufAndFileRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, TSDBROW* pRow,
                                     SIterInfo* pIter, int64_t key, SSttBlockReader* pSttBlockReader) {
  SRowMerger*         pMerger = &pReader->status.merger;
  SRow*               pTSRow = NULL;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  int64_t tsLast = INT64_MIN;
  if (hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
    tsLast = getCurrentKeyInSttBlock(pSttBlockReader);
  }

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  if (pMerger->pArray == NULL) {
    ASSERT(pReader->info.pSchema == NULL);
    STSchema* ps = getTableSchemaImpl(pReader, pBlockScanInfo->uid);
    if (ps == NULL) {
      return terrno;
    }
  }

  int64_t minKey = 0;
  if (pReader->info.order == TSDB_ORDER_ASC) {
    minKey = INT64_MAX;  // chosen the minimum value
    if (minKey > tsLast && hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
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
    if (minKey < tsLast && hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
      minKey = tsLast;
    }

    if (minKey < k.ts) {
      minKey = k.ts;
    }

    if (minKey < key && hasDataInFileBlock(pBlockData, pDumpInfo)) {
      minKey = key;
    }
  }

  // ASC: file block ---> last block -----> imem -----> mem
  // DESC: mem -----> imem -----> last block -----> file block
  if (pReader->info.order == TSDB_ORDER_ASC) {
    if (minKey == key) {
      int32_t code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);
    }

    if (minKey == tsLast) {
      TSDBROW* fRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      int32_t  code = tsdbRowMergerAdd(pMerger, fRow1, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLast, pMerger, &pReader->info.verRange, pReader->idStr);
    }

    if (minKey == k.ts) {
      STSchema* pTSchema = NULL;
      if (pRow->type == TSDBROW_ROW_FMT) {
        pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
        if (pTSchema == NULL) {
          return terrno;
        }
      }

      int32_t code = tsdbRowMergerAdd(pMerger, pRow, pTSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(pIter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else {
    if (minKey == k.ts) {
      STSchema* pTSchema = NULL;
      if (pRow->type == TSDBROW_ROW_FMT) {
        pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
        if (pTSchema == NULL) {
          return terrno;
        }
      }

      int32_t code = tsdbRowMergerAdd(pMerger, pRow, pTSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(pIter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, pReader);
      if (code != TSDB_CODE_SUCCESS || pMerger->pTSchema == NULL) {
        return code;
      }
    }

    if (minKey == tsLast) {
      TSDBROW* fRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      int32_t  code = tsdbRowMergerAdd(pMerger, fRow1, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLast, pMerger, &pReader->info.verRange, pReader->idStr);
    }

    if (minKey == key) {
      int32_t code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);
    }
  }

  int32_t code = tsdbRowMergerGetRow(pMerger, &pTSRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFree(pTSRow);
  tsdbRowMergerClear(pMerger);

  return code;
}

static int32_t mergeFileBlockAndSttBlock(STsdbReader* pReader, SSttBlockReader* pSttBlockReader, int64_t key,
                                         STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SRowMerger*         pMerger = &pReader->status.merger;
  int32_t             code = TSDB_CODE_SUCCESS;

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  if (pMerger->pArray == NULL) {
    ASSERT(pReader->info.pSchema == NULL);
    STSchema* ps = getTableSchemaImpl(pReader, pBlockScanInfo->uid);
    if (ps == NULL) {
      return terrno;
    }
  }

  bool dataInDataFile = hasDataInFileBlock(pBlockData, pDumpInfo);
  bool dataInSttFile = hasDataInSttBlock(pBlockScanInfo);
  if (dataInDataFile && (!dataInSttFile)) {
    // no stt file block available, only data block exists
    return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
  } else if ((!dataInDataFile) && dataInSttFile) {
    // no data ile block exists
    return mergeRowsInSttBlocks(pSttBlockReader, pBlockScanInfo, pReader);
  } else if (pBlockScanInfo->cleanSttBlocks && pReader->info.execMode == READER_EXEC_ROWS) {
    // opt model for count data in stt file, which is not overlap with data blocks in files.
    return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
  } else {
    // row in both stt file blocks and data file blocks
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
    int64_t tsLast = getCurrentKeyInSttBlock(pSttBlockReader);
    if (ASCENDING_TRAVERSE(pReader->info.order)) {
      if (key < tsLast) {  // asc
        return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
      } else if (key > tsLast) {
        return mergeRowsInSttBlocks(pSttBlockReader, pBlockScanInfo, pReader);
      }
    } else {  // desc
      if (key > tsLast) {
        return mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, key, pReader);
      } else if (key < tsLast) {
        return mergeRowsInSttBlocks(pSttBlockReader, pBlockScanInfo, pReader);
      }
    }

    // the following for key == tsLast
    // ASC:  file block ------> stt  block
    // DESC: stt  block ------> file block
    SRow* pTSRow = NULL;
    if (ASCENDING_TRAVERSE(pReader->info.order)) {
      code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);

      TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLast, pMerger, &pReader->info.verRange, pReader->idStr);
    } else {
      TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLast, pMerger, &pReader->info.verRange, pReader->idStr);

      code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);
    }

    code = tsdbRowMergerGetRow(pMerger, &pTSRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

    taosMemoryFree(pTSRow);
    tsdbRowMergerClear(pMerger);
    return code;
  }
}

static int32_t doMergeMultiLevelRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData,
                                     SSttBlockReader* pSttBlockReader) {
  SRowMerger*         pMerger = &pReader->status.merger;
  SRow*               pTSRow = NULL;
  int32_t             code = TSDB_CODE_SUCCESS;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SArray*             pDelList = pBlockScanInfo->delSkyline;

  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pDelList, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pDelList, pReader);

  int64_t tsLast = INT64_MIN;
  if (hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
    tsLast = getCurrentKeyInSttBlock(pSttBlockReader);
  }

  int64_t key = hasDataInFileBlock(pBlockData, pDumpInfo) ? pBlockData->aTSKEY[pDumpInfo->rowIndex] : INT64_MIN;

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  STSchema* pSchema = NULL;
  if (pRow->type == TSDBROW_ROW_FMT) {
    pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
    if (pSchema == NULL) {
      return terrno;
    }
  }

  STSchema* piSchema = NULL;
  if (piRow->type == TSDBROW_ROW_FMT) {
    piSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
    if (piSchema == NULL) {
      return code;
    }
  }

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  if (pMerger->pArray == NULL) {
    ASSERT(pReader->info.pSchema == NULL);
    STSchema* ps = getTableSchemaImpl(pReader, pBlockScanInfo->uid);
    if (ps == NULL) {
      return terrno;
    }
  }

  int64_t minKey = 0;
  if (ASCENDING_TRAVERSE(pReader->info.order)) {
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

    if (minKey > tsLast && hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
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

    if (minKey < tsLast && hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
      minKey = tsLast;
    }
  }

  // ASC: file block -----> stt block -----> imem -----> mem
  // DESC: mem -----> imem -----> stt block -----> file block
  if (ASCENDING_TRAVERSE(pReader->info.order)) {
    if (minKey == key) {
      TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
      code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);
    }

    if (minKey == tsLast) {
      TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLast, pMerger, &pReader->info.verRange, pReader->idStr);
    }

    if (minKey == ik.ts) {
      code = tsdbRowMergerAdd(pMerger, piRow, piSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    if (minKey == k.ts) {
      code = tsdbRowMergerAdd(pMerger, pRow, pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  } else {
    if (minKey == k.ts) {
      code = tsdbRowMergerAdd(pMerger, pRow, pSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    if (minKey == ik.ts) {
      code = tsdbRowMergerAdd(pMerger, piRow, piSchema);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    if (minKey == tsLast) {
      TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLast, pMerger, &pReader->info.verRange, pReader->idStr);
    }

    if (minKey == key) {
      TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
      code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }

      doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);
    }
  }

  code = tsdbRowMergerGetRow(pMerger, &pTSRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFree(pTSRow);
  tsdbRowMergerClear(pMerger);
  return code;
}

int32_t doInitMemDataIter(STsdbReader* pReader, STbData** pData, STableBlockScanInfo* pBlockScanInfo, TSDBKEY* pKey,
                          SMemTable* pMem, SIterInfo* pIter, const char* type) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t backward = (!ASCENDING_TRAVERSE(pReader->info.order));
  pIter->hasVal = false;

  if (pMem != NULL) {
    *pData = tsdbGetTbDataFromMemTable(pMem, pReader->info.suid, pBlockScanInfo->uid);

    if ((*pData) != NULL) {
      code = tsdbTbDataIterCreate((*pData), pKey, backward, &pIter->iter);
      if (code == TSDB_CODE_SUCCESS) {
        pIter->hasVal = (tsdbTbDataIterGet(pIter->iter) != NULL);

        tsdbDebug("%p uid:%" PRIu64 ", check data in %s from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
                  "-%" PRId64 " %s",
                  pReader, pBlockScanInfo->uid, type, pKey->ts, pReader->info.order, (*pData)->minKey, (*pData)->maxKey,
                  pReader->idStr);
      } else {
        tsdbError("%p uid:%" PRIu64 ", failed to create iterator for %s, code:%s, %s", pReader, pBlockScanInfo->uid,
                  type, tstrerror(code), pReader->idStr);
        return code;
      }
    }
  } else {
    tsdbDebug("%p uid:%" PRIu64 ", no data in %s, %s", pReader, pBlockScanInfo->uid, type, pReader->idStr);
  }

  return code;
}

static int32_t initMemDataIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader) {
  if (pBlockScanInfo->iterInit) {
    return TSDB_CODE_SUCCESS;
  }

  STbData* d = NULL;
  TSDBKEY  startKey = {0};
  if (ASCENDING_TRAVERSE(pReader->info.order)) {
    startKey = (TSDBKEY){.ts = pBlockScanInfo->lastProcKey + 1, .version = pReader->info.verRange.minVer};
  } else {
    startKey = (TSDBKEY){.ts = pBlockScanInfo->lastProcKey - 1, .version = pReader->info.verRange.maxVer};
  }

  int32_t code =
      doInitMemDataIter(pReader, &d, pBlockScanInfo, &startKey, pReader->pReadSnap->pMem, &pBlockScanInfo->iter, "mem");
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STbData* di = NULL;
  code = doInitMemDataIter(pReader, &di, pBlockScanInfo, &startKey, pReader->pReadSnap->pIMem, &pBlockScanInfo->iiter,
                           "imem");
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  loadMemTombData(&pBlockScanInfo->pMemDelData, d, di, pReader->info.verRange.maxVer);

  pBlockScanInfo->iterInit = true;
  return TSDB_CODE_SUCCESS;
}

static bool isValidFileBlockRow(SBlockData* pBlockData, int32_t rowIndex, STableBlockScanInfo* pBlockScanInfo, bool asc,
                                STsdbReaderInfo* pInfo) {
  // it is an multi-table data block
  if (pBlockData->aUid != NULL) {
    uint64_t uid = pBlockData->aUid[rowIndex];
    if (uid != pBlockScanInfo->uid) {  // move to next row
      return false;
    }
  }

  // check for version and time range
  int64_t ver = pBlockData->aVersion[rowIndex];
  if (ver > pInfo->verRange.maxVer || ver < pInfo->verRange.minVer) {
    return false;
  }

  int64_t ts = pBlockData->aTSKEY[rowIndex];
  if (ts > pInfo->window.ekey || ts < pInfo->window.skey) {
    return false;
  }

  if ((asc && (ts <= pBlockScanInfo->lastProcKey)) || ((!asc) && (ts >= pBlockScanInfo->lastProcKey))) {
    return false;
  }

  if (hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->fileDelIndex, ts, ver, pInfo->order,
                     &pInfo->verRange)) {
    return false;
  }

  if (pBlockScanInfo->delSkyline != NULL && TARRAY_SIZE(pBlockScanInfo->delSkyline) > 0) {
    bool dropped = hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->fileDelIndex, ts, ver,
                                  pInfo->order, &pInfo->verRange);
    if (dropped) {
      return false;
    }
  }

  return true;
}

static bool initSttBlockReader(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  bool hasData = true;

  // the stt block reader has been initialized for this table.
  if (pSttBlockReader->uid == pScanInfo->uid) {
    return hasDataInSttBlock(pScanInfo);
  }

  if (pSttBlockReader->uid != 0) {
    tMergeTreeClose(&pSttBlockReader->mergeTree);
  }

  pSttBlockReader->uid = pScanInfo->uid;

  // second time init stt block reader
  if (pScanInfo->cleanSttBlocks && pReader->info.execMode == READER_EXEC_ROWS) {
    return !pScanInfo->sttBlockReturned;
  }

  STimeWindow w = pSttBlockReader->window;
  if (ASCENDING_TRAVERSE(pSttBlockReader->order)) {
    w.skey = pScanInfo->sttKeyInfo.nextProcKey;
  } else {
    w.ekey = pScanInfo->sttKeyInfo.nextProcKey;
  }

  int64_t st = taosGetTimestampUs();
  tsdbDebug("init stt block reader, window:%" PRId64 "-%" PRId64 ", uid:%" PRIu64 ", %s", w.skey, w.ekey,
            pScanInfo->uid, pReader->idStr);

  SMergeTreeConf conf = {
      .uid = pScanInfo->uid,
      .suid = pReader->info.suid,
      .pTsdb = pReader->pTsdb,
      .timewindow = w,
      .verRange = pSttBlockReader->verRange,
      .strictTimeRange = false,
      .pSchema = pReader->info.pSchema,
      .pCurrentFileset = pReader->status.pCurrentFileset,
      .backward = (pSttBlockReader->order == TSDB_ORDER_DESC),
      .pSttFileBlockIterArray = pReader->status.pLDataIterArray,
      .pCols = pReader->suppInfo.colId,
      .numOfCols = pReader->suppInfo.numOfCols,
      .loadTombFn = loadSttTombDataForAll,
      .pReader = pReader,
      .idstr = pReader->idStr,
      .rspRows = (pReader->info.execMode == READER_EXEC_ROWS),
  };

  SSttDataInfoForTable info = {.pTimeWindowList = taosArrayInit(4, sizeof(STimeWindow))};
  int32_t              code = tMergeTreeOpen2(&pSttBlockReader->mergeTree, &conf, &info);
  if (code != TSDB_CODE_SUCCESS) {
    return false;
  }

  initMemDataIterator(pScanInfo, pReader);
  initDelSkylineIterator(pScanInfo, pReader->info.order, &pReader->cost);

  if (conf.rspRows) {
    pScanInfo->cleanSttBlocks =
        isCleanSttBlock(info.pTimeWindowList, &pReader->info.window, pScanInfo, pReader->info.order);

    if (pScanInfo->cleanSttBlocks) {
      pScanInfo->numOfRowsInStt = info.numOfRows;
      pScanInfo->sttWindow.skey = INT64_MAX;
      pScanInfo->sttWindow.ekey = INT64_MIN;

      // calculate the time window for data in stt files
      for (int32_t i = 0; i < taosArrayGetSize(info.pTimeWindowList); ++i) {
        STimeWindow* pWindow = taosArrayGet(info.pTimeWindowList, i);
        if (pScanInfo->sttWindow.skey > pWindow->skey) {
          pScanInfo->sttWindow.skey = pWindow->skey;
        }

        if (pScanInfo->sttWindow.ekey < pWindow->ekey) {
          pScanInfo->sttWindow.ekey = pWindow->ekey;
        }
      }

      pScanInfo->sttKeyInfo.status = taosArrayGetSize(info.pTimeWindowList) ? STT_FILE_HAS_DATA : STT_FILE_NO_DATA;
      pScanInfo->sttKeyInfo.nextProcKey =
          ASCENDING_TRAVERSE(pReader->info.order) ? pScanInfo->sttWindow.skey : pScanInfo->sttWindow.ekey;
      hasData = true;
    } else {  // not clean stt blocks
      INIT_TIMEWINDOW(&pScanInfo->sttWindow);  //reset the time window
      pScanInfo->sttBlockReturned = false;
      hasData = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, &pReader->info.verRange);
    }
  } else {
    pScanInfo->cleanSttBlocks = false;
    INIT_TIMEWINDOW(&pScanInfo->sttWindow);  //reset the time window
    pScanInfo->sttBlockReturned = false;
    hasData = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, &pReader->info.verRange);
  }

  taosArrayDestroy(info.pTimeWindowList);

  int64_t el = taosGetTimestampUs() - st;
  pReader->cost.initSttBlockReader += (el / 1000.0);

  tsdbDebug("init stt block reader completed, elapsed time:%" PRId64 "us %s", el, pReader->idStr);
  return hasData;
}

static bool hasDataInSttBlock(STableBlockScanInfo* pInfo) { return pInfo->sttKeyInfo.status == STT_FILE_HAS_DATA; }

bool hasDataInFileBlock(const SBlockData* pBlockData, const SFileBlockDumpInfo* pDumpInfo) {
  if ((pBlockData->nRow > 0) && (pBlockData->nRow != pDumpInfo->totalRows)) {
    return false;  // this is an invalid result.
  }
  return pBlockData->nRow > 0 && (!pDumpInfo->allDumped);
}

int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, int64_t key,
                              STsdbReader* pReader) {
  SRowMerger*         pMerger = &pReader->status.merger;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  bool                copied = false;

  int32_t code = tryCopyDistinctRowFromFileBlock(pReader, pBlockData, key, pDumpInfo, &copied);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  if (pMerger->pArray == NULL) {
    ASSERT(pReader->info.pSchema == NULL);
    STSchema* ps = getTableSchemaImpl(pReader, pBlockScanInfo->uid);
    if (ps == NULL) {
      return terrno;
    }
  }

  if (copied) {
    pBlockScanInfo->lastProcKey = key;
    return TSDB_CODE_SUCCESS;
  } else {
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

    SRow* pTSRow = NULL;
    code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pReader);
    code = tsdbRowMergerGetRow(pMerger, &pTSRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

    taosMemoryFree(pTSRow);
    tsdbRowMergerClear(pMerger);
    return code;
  }
}

int32_t mergeRowsInSttBlocks(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pBlockScanInfo,
                             STsdbReader* pReader) {
  bool    copied = false;
  SRow*   pTSRow = NULL;
  int64_t tsLastBlock = getCurrentKeyInSttBlock(pSttBlockReader);

  SRowMerger* pMerger = &pReader->status.merger;
  TSDBROW*    pRow = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
  TSDBROW     fRow = {.iRow = pRow->iRow, .type = TSDBROW_COL_FMT, .pBlockData = pRow->pBlockData};

  tsdbTrace("fRow ptr:%p, %d, uid:%" PRIu64 ", ts:%" PRId64 " %s", pRow->pBlockData, pRow->iRow, pSttBlockReader->uid,
            fRow.pBlockData->aTSKEY[fRow.iRow], pReader->idStr);

  int32_t code = tryCopyDistinctRowFromSttBlock(&fRow, pSttBlockReader, pBlockScanInfo, tsLastBlock, pReader, &copied);
  if (code) {
    return code;
  }

  if (copied) {
    pBlockScanInfo->lastProcKey = tsLastBlock;
    return TSDB_CODE_SUCCESS;
  } else {
    code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
    tsdbRowMergerAdd(pMerger, pRow1, NULL);
    doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, tsLastBlock, pMerger, &pReader->info.verRange,
                          pReader->idStr);

    code = tsdbRowMergerGetRow(pMerger, &pTSRow);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

    taosMemoryFree(pTSRow);
    tsdbRowMergerClear(pMerger);
    return code;
  }
}

static int32_t buildComposedDataBlockImpl(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo,
                                          SBlockData* pBlockData, SSttBlockReader* pSttBlockReader) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  TSDBROW *pRow = NULL, *piRow = NULL;
  int64_t  key = (pBlockData->nRow > 0 && (!pDumpInfo->allDumped))
                     ? pBlockData->aTSKEY[pDumpInfo->rowIndex]
                     : (ASCENDING_TRAVERSE(pReader->info.order) ? INT64_MAX : INT64_MIN);
  if (pBlockScanInfo->iter.hasVal) {
    pRow = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  }

  if (pBlockScanInfo->iiter.hasVal) {
    piRow = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);
  }

  // two levels of mem-table does contain the valid rows
  if (pRow != NULL && piRow != NULL) {
    return doMergeMultiLevelRows(pReader, pBlockScanInfo, pBlockData, pSttBlockReader);
  }

  // imem + file + stt block
  if (pBlockScanInfo->iiter.hasVal) {
    return doMergeBufAndFileRows(pReader, pBlockScanInfo, piRow, &pBlockScanInfo->iiter, key, pSttBlockReader);
  }

  // mem + file + stt block
  if (pBlockScanInfo->iter.hasVal) {
    return doMergeBufAndFileRows(pReader, pBlockScanInfo, pRow, &pBlockScanInfo->iter, key, pSttBlockReader);
  }

  // files data blocks + stt block
  return mergeFileBlockAndSttBlock(pReader, pSttBlockReader, key, pBlockScanInfo, pBlockData);
}

static int32_t loadNeighborIfOverlap(SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pBlockScanInfo,
                                     STsdbReader* pReader, bool* loadNeighbor) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t step = ASCENDING_TRAVERSE(pReader->info.order) ? 1 : -1;
  int32_t nextIndex = -1;

  *loadNeighbor = false;

  SBrinRecord rec = {0};
  bool hasNeighbor = getNeighborBlockOfSameTable(&pReader->status.blockIter, pBlockInfo, pBlockScanInfo, &nextIndex,
                                                 pReader->info.order, &rec);
  if (!hasNeighbor) {  // do nothing
    return code;
  }

  if (overlapWithNeighborBlock2(pBlockInfo, &rec, pReader->info.order)) {  // load next block
    SReaderStatus*  pStatus = &pReader->status;
    SDataBlockIter* pBlockIter = &pStatus->blockIter;

    // 1. find the next neighbor block in the scan block list
    STableDataBlockIdx* tableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, nextIndex);
    int32_t             neighborIndex = tableDataBlockIdx->globalIndex;

    // 2. remove it from the scan block list
    setFileBlockActiveInBlockIter(pReader, pBlockIter, neighborIndex, step);

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

void updateComposedBlockInfo(STsdbReader* pReader, double el, STableBlockScanInfo* pBlockScanInfo) {
  SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;

  pResBlock->info.id.uid = (pBlockScanInfo != NULL) ? pBlockScanInfo->uid : 0;
  pResBlock->info.dataLoad = 1;
  pResBlock->info.version = pReader->info.verRange.maxVer;
  blockDataUpdateTsWindow(pResBlock, pReader->suppInfo.slotId[0]);
  setComposedBlockFlag(pReader, true);

  pReader->cost.composedBlocks += 1;
  pReader->cost.buildComposedBlockTime += el;
}

static int32_t buildComposedDataBlock(STsdbReader* pReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  bool                asc = ASCENDING_TRAVERSE(pReader->info.order);
  int64_t             st = taosGetTimestampUs();
  int32_t             step = asc ? 1 : -1;
  double              el = 0;
  SSDataBlock*        pResBlock = pReader->resBlockInfo.pResBlock;
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);
  SSttBlockReader*    pSttBlockReader = pReader->status.fileIter.pSttBlockReader;
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  STableBlockScanInfo* pBlockScanInfo = NULL;
  if (pBlockInfo == NULL) {
    return 0;
  }

  if (pReader->pIgnoreTables && taosHashGet(*pReader->pIgnoreTables, &pBlockInfo->uid, sizeof(pBlockInfo->uid))) {
    setBlockAllDumped(pDumpInfo, pBlockInfo->lastKey, pReader->info.order);
    return code;
  }

  pBlockScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, pReader->idStr);
  if (pBlockScanInfo == NULL) {
    goto _end;
  }

  TSDBKEY keyInBuf = getCurrentKeyInBuf(pBlockScanInfo, pReader);

  // it is a clean block, load it directly
  int64_t cap = pReader->resBlockInfo.capacity;
  if (isCleanFileDataBlock(pReader, pBlockInfo, pBlockScanInfo, keyInBuf) && (pBlockInfo->numRow <= cap)) {
    if (((asc && (pBlockInfo->firstKey < keyInBuf.ts)) || (!asc && (pBlockInfo->lastKey > keyInBuf.ts))) &&
        (pBlockScanInfo->sttKeyInfo.status == STT_FILE_NO_DATA)) {
      code = copyBlockDataToSDataBlock(pReader);
      if (code) {
        goto _end;
      }

      // record the last key value
      pBlockScanInfo->lastProcKey = asc ? pBlockInfo->lastKey : pBlockInfo->firstKey;
      goto _end;
    }
  }

  SBlockData* pBlockData = &pReader->status.fileBlockData;
  initSttBlockReader(pSttBlockReader, pBlockScanInfo, pReader);

  while (1) {
    bool hasBlockData = false;
    {
      while (pBlockData->nRow > 0 && pBlockData->uid == pBlockScanInfo->uid) {
        // find the first qualified row in data block
        if (isValidFileBlockRow(pBlockData, pDumpInfo->rowIndex, pBlockScanInfo, asc, &pReader->info)) {
          hasBlockData = true;
          break;
        }

        pDumpInfo->rowIndex += step;

        if (pDumpInfo->rowIndex >= pBlockData->nRow || pDumpInfo->rowIndex < 0) {
          pBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);  // NOTE: get the new block info

          // continue check for the next file block if the last ts in the current block
          // is overlapped with the next neighbor block
          bool loadNeighbor = false;
          code = loadNeighborIfOverlap(pBlockInfo, pBlockScanInfo, pReader, &loadNeighbor);
          if ((!loadNeighbor) || (code != 0)) {
            setBlockAllDumped(pDumpInfo, pBlockInfo->lastKey, pReader->info.order);
            break;
          }
        }
      }
    }

    // no data in last block and block, no need to proceed.
    if (hasBlockData == false) {
      break;
    }

    code = buildComposedDataBlockImpl(pReader, pBlockScanInfo, pBlockData, pSttBlockReader);
    if (code) {
      goto _end;
    }

    // currently loaded file data block is consumed
    if ((pBlockData->nRow > 0) && (pDumpInfo->rowIndex >= pBlockData->nRow || pDumpInfo->rowIndex < 0)) {
      setBlockAllDumped(pDumpInfo, pBlockInfo->lastKey, pReader->info.order);
      break;
    }

    if (pResBlock->info.rows >= pReader->resBlockInfo.capacity) {
      break;
    }
  }

_end:
  el = (taosGetTimestampUs() - st) / 1000.0;
  updateComposedBlockInfo(pReader, el, pBlockScanInfo);

  if (pResBlock->info.rows > 0) {
    tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64 " rows:%" PRId64
              ", elapsed time:%.2f ms %s",
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

int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, int32_t order, SReadCostSummary* pCost) {
  int32_t code = 0;
  int32_t newDelDataInFile = taosArrayGetSize(pBlockScanInfo->pFileDelData);
  if (newDelDataInFile == 0 &&
      ((pBlockScanInfo->delSkyline != NULL) || (TARRAY_SIZE(pBlockScanInfo->pMemDelData) == 0))) {
    return code;
  }

  int64_t st = taosGetTimestampUs();

  if (pBlockScanInfo->delSkyline != NULL) {
    taosArrayClear(pBlockScanInfo->delSkyline);
  } else {
    pBlockScanInfo->delSkyline = taosArrayInit(4, sizeof(TSDBKEY));
  }

  SArray* pSource = pBlockScanInfo->pFileDelData;
  if (pSource == NULL) {
    pSource = pBlockScanInfo->pMemDelData;
  } else {
    taosArrayAddAll(pSource, pBlockScanInfo->pMemDelData);
  }

  code = tsdbBuildDeleteSkyline(pSource, 0, taosArrayGetSize(pSource) - 1, pBlockScanInfo->delSkyline);

  taosArrayClear(pBlockScanInfo->pFileDelData);
  int32_t index = getInitialDelIndex(pBlockScanInfo->delSkyline, order);

  pBlockScanInfo->iter.index = index;
  pBlockScanInfo->iiter.index = index;
  pBlockScanInfo->fileDelIndex = index;
  pBlockScanInfo->sttBlockDelIndex = index;

  double el = taosGetTimestampUs() - st;
  pCost->createSkylineIterTime = el / 1000.0;

  return code;
}

TSDBKEY getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);
  TSDBKEY key = {.ts = TSKEY_INITIAL_VAL}, ikey = {.ts = TSKEY_INITIAL_VAL};

  bool     hasKey = false, hasIKey = false;
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
    if (hasIKey) {  // has data in mem & imem
      if (asc) {
        return key.ts <= ikey.ts ? key : ikey;
      } else {
        return key.ts <= ikey.ts ? ikey : key;
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

static void prepareDurationForNextFileSet(STsdbReader* pReader) {
  if (pReader->status.bProcMemFirstFileset) {
    pReader->status.prevFilesetStartKey = INT64_MIN;
    pReader->status.prevFilesetEndKey = INT64_MAX;
    pReader->status.bProcMemFirstFileset = false;
  }

  int32_t     fid = pReader->status.pCurrentFileset->fid;
  STimeWindow winFid = {0};
  tsdbFidKeyRange(fid, pReader->pTsdb->keepCfg.days, pReader->pTsdb->keepCfg.precision, &winFid.skey, &winFid.ekey);

  if (ASCENDING_TRAVERSE(pReader->info.order)) {
    pReader->status.bProcMemPreFileset = !(pReader->status.memTableMaxKey < pReader->status.prevFilesetStartKey ||
                                           (winFid.skey - 1) < pReader->status.memTableMinKey);
  } else {
    pReader->status.bProcMemPreFileset = !(pReader->status.memTableMaxKey < (winFid.ekey + 1) ||
                                           pReader->status.prevFilesetEndKey < pReader->status.memTableMinKey);
  }

  if (pReader->status.bProcMemPreFileset) {
    tsdbDebug("will start pre-fileset %d buffer processing. %s", fid, pReader->idStr);
    pReader->status.procMemUidList.tableUidList = pReader->status.uidList.tableUidList;
    resetPreFilesetMemTableListIndex(&pReader->status);
  }

  if (!pReader->status.bProcMemPreFileset) {
    if (pReader->notifyFn) {
      STsdReaderNotifyInfo info = {0};
      info.duration.filesetId = fid;
      pReader->notifyFn(TSD_READER_NOTIFY_DURATION_START, &info, pReader->notifyParam);
      tsdbDebug("new duration %d start notification when no buffer preceeding fileset, %s", fid, pReader->idStr);
    }
  }

  pReader->status.prevFilesetStartKey = winFid.skey;
  pReader->status.prevFilesetEndKey = winFid.ekey;
}

static int32_t moveToNextFile(STsdbReader* pReader, SBlockNumber* pBlockNum, SArray* pTableList) {
  SReaderStatus* pStatus = &pReader->status;
  pBlockNum->numOfBlocks = 0;
  pBlockNum->numOfSttFiles = 0;

  size_t  numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  SArray* pIndexList = taosArrayInit(numOfTables, sizeof(SBrinBlk));

  while (1) {
    // only check here, since the iterate data in memory is very fast.
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      return pReader->code;
    }

    bool    hasNext = false;
    int32_t code = filesetIteratorNext(&pStatus->fileIter, pReader, &hasNext);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(pIndexList);
      return code;
    }

    if (!hasNext) {  // no data files on disk
      break;
    }

    taosArrayClear(pIndexList);
    code = doLoadBlockIndex(pReader, pReader->pFileReader, pIndexList);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(pIndexList);
      return code;
    }

    if (taosArrayGetSize(pIndexList) > 0 || pReader->status.pCurrentFileset->lvlArr->size > 0) {
      code = doLoadFileBlock(pReader, pIndexList, pBlockNum, pTableList);
      if (code != TSDB_CODE_SUCCESS) {
        taosArrayDestroy(pIndexList);
        return code;
      }

      if (pBlockNum->numOfBlocks + pBlockNum->numOfSttFiles > 0) {
        if (pReader->bFilesetDelimited) {
          prepareDurationForNextFileSet(pReader);
        }
        break;
      }
    }

    // no blocks in current file, try next files
  }

  taosArrayDestroy(pIndexList);
  return loadDataFileTombDataForAll(pReader);
}

static void resetTableListIndex(SReaderStatus* pStatus) {
  STableUidList* pList = &pStatus->uidList;

  pList->currentIndex = 0;
  uint64_t uid = pList->tableUidList[0];
  pStatus->pTableIter = tSimpleHashGet(pStatus->pTableMap, &uid, sizeof(uid));
}

static void resetPreFilesetMemTableListIndex(SReaderStatus* pStatus) {
  STableUidList* pList = &pStatus->procMemUidList;

  pList->currentIndex = 0;
  uint64_t uid = pList->tableUidList[0];
  pStatus->pProcMemTableIter = tSimpleHashGet(pStatus->pTableMap, &uid, sizeof(uid));
}

static bool moveToNextTable(STableUidList* pOrderedCheckInfo, SReaderStatus* pStatus) {
  pOrderedCheckInfo->currentIndex += 1;
  if (pOrderedCheckInfo->currentIndex >= tSimpleHashGetSize(pStatus->pTableMap)) {
    pStatus->pTableIter = NULL;
    return false;
  }

  uint64_t uid = pOrderedCheckInfo->tableUidList[pOrderedCheckInfo->currentIndex];
  pStatus->pTableIter = tSimpleHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  return (pStatus->pTableIter != NULL);
}

static bool moveToNextTableForPreFileSetMem(SReaderStatus* pStatus) {
  STableUidList* pUidList = &pStatus->procMemUidList;
  pUidList->currentIndex += 1;
  if (pUidList->currentIndex >= tSimpleHashGetSize(pStatus->pTableMap)) {
    pStatus->pProcMemTableIter = NULL;
    return false;
  }

  uint64_t uid = pUidList->tableUidList[pUidList->currentIndex];
  pStatus->pProcMemTableIter = tSimpleHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  return (pStatus->pProcMemTableIter != NULL);
}

static void buildCleanBlockFromSttFiles(STsdbReader* pReader, STableBlockScanInfo* pScanInfo) {
  SReaderStatus*   pStatus = &pReader->status;
  SSttBlockReader* pSttBlockReader = pStatus->fileIter.pSttBlockReader;
  SSDataBlock*     pResBlock = pReader->resBlockInfo.pResBlock;

  bool asc = ASCENDING_TRAVERSE(pReader->info.order);

  SDataBlockInfo* pInfo = &pResBlock->info;
  blockDataEnsureCapacity(pResBlock, pScanInfo->numOfRowsInStt);

  pInfo->rows = pScanInfo->numOfRowsInStt;
  pInfo->id.uid = pScanInfo->uid;
  pInfo->dataLoad = 1;
  pInfo->window = pScanInfo->sttWindow;

  setComposedBlockFlag(pReader, true);

  pScanInfo->sttKeyInfo.nextProcKey = asc ? pScanInfo->sttWindow.ekey + 1 : pScanInfo->sttWindow.skey - 1;
  pScanInfo->sttKeyInfo.status = STT_FILE_NO_DATA;
  pScanInfo->lastProcKey = asc ? pScanInfo->sttWindow.ekey : pScanInfo->sttWindow.skey;
  pScanInfo->sttBlockReturned = true;

  pSttBlockReader->mergeTree.pIter = NULL;

  tsdbDebug("%p uid:%" PRId64 " return clean stt block as one, brange:%" PRId64 "-%" PRId64 " rows:%" PRId64 " %s",
            pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
            pResBlock->info.rows, pReader->idStr);
}

static void buildCleanBlockFromDataFiles(STsdbReader* pReader, STableBlockScanInfo* pScanInfo,
                                         SFileDataBlockInfo* pBlockInfo, int32_t blockIndex) {
  // whole block is required, return it directly
  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockInfo* pInfo = &pReader->resBlockInfo.pResBlock->info;
  bool            asc = ASCENDING_TRAVERSE(pReader->info.order);

  pInfo->rows = pBlockInfo->numRow;
  pInfo->id.uid = pScanInfo->uid;
  pInfo->dataLoad = 0;
  pInfo->version = pReader->info.verRange.maxVer;
  pInfo->window = (STimeWindow){.skey = pBlockInfo->firstKey, .ekey = pBlockInfo->lastKey};
  setComposedBlockFlag(pReader, false);
  setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlockInfo->lastKey, pReader->info.order);

  // update the last key for the corresponding table
  pScanInfo->lastProcKey = asc ? pInfo->window.ekey : pInfo->window.skey;
  tsdbDebug("%p uid:%" PRIu64 " clean file block retrieved from file, global index:%d, "
            "table index:%d, rows:%d, brange:%" PRId64 "-%" PRId64 ", %s",
            pReader, pScanInfo->uid, blockIndex, pBlockInfo->tbBlockIdx, pBlockInfo->numRow, pBlockInfo->firstKey,
            pBlockInfo->lastKey, pReader->idStr);
}

static int32_t doLoadSttBlockSequentially(STsdbReader* pReader) {
  SReaderStatus*   pStatus = &pReader->status;
  SSttBlockReader* pSttBlockReader = pStatus->fileIter.pSttBlockReader;
  STableUidList*   pUidList = &pStatus->uidList;
  int32_t          code = TSDB_CODE_SUCCESS;

  if (tSimpleHashGetSize(pStatus->pTableMap) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;

  while (1) {
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      return pReader->code;
    }

    // load the last data block of current table
    STableBlockScanInfo* pScanInfo = *(STableBlockScanInfo**)pStatus->pTableIter;
    if (pScanInfo == NULL) {
      tsdbError("table Iter is null, invalid pScanInfo, try next table %s", pReader->idStr);
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    if (pReader->pIgnoreTables && taosHashGet(*pReader->pIgnoreTables, &pScanInfo->uid, sizeof(pScanInfo->uid))) {
      // reset the index in last block when handing a new file
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    bool hasDataInSttFile = initSttBlockReader(pSttBlockReader, pScanInfo, pReader);
    if (!hasDataInSttFile) {
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }

      continue;
    }

    // if only require the total rows, no need to load data from stt file if it is clean stt blocks
    if (pReader->info.execMode == READER_EXEC_ROWS && pScanInfo->cleanSttBlocks) {
      buildCleanBlockFromSttFiles(pReader, pScanInfo);
      return TSDB_CODE_SUCCESS;
    }

    int64_t st = taosGetTimestampUs();
    while (1) {
      // no data in stt block and block, no need to proceed.
      if (!hasDataInSttBlock(pScanInfo)) {
        break;
      }

      code = buildComposedDataBlockImpl(pReader, pScanInfo, &pReader->status.fileBlockData, pSttBlockReader);
      if (code) {
        return code;
      }

      if (pResBlock->info.rows >= pReader->resBlockInfo.capacity) {
        break;
      }
    }

    double el = (taosGetTimestampUs() - st) / 1000.0;
    updateComposedBlockInfo(pReader, el, pScanInfo);

    if (pResBlock->info.rows > 0) {
      tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64 " rows:%" PRId64
                ", elapsed time:%.2f ms %s",
                pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
                pResBlock->info.rows, el, pReader->idStr);
      return TSDB_CODE_SUCCESS;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTable(pUidList, pStatus);
    if (!hasNexTable) {
      return TSDB_CODE_SUCCESS;
    }
  }
}

// current active data block not overlap with the stt-files/stt-blocks
static bool notOverlapWithFiles(SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pScanInfo, bool asc) {
  ASSERT(pScanInfo->sttKeyInfo.status != STT_FILE_READER_UNINIT);

  if ((!hasDataInSttBlock(pScanInfo)) || (pScanInfo->cleanSttBlocks == true)) {
    return true;
  } else {
    int64_t keyInStt = pScanInfo->sttKeyInfo.nextProcKey;
    return (asc && pBlockInfo->lastKey < keyInStt) || (!asc && pBlockInfo->firstKey > keyInStt);
  }
}

static int32_t doBuildDataBlock(STsdbReader* pReader) {
  SReaderStatus*       pStatus = &pReader->status;
  SDataBlockIter*      pBlockIter = &pStatus->blockIter;
  STableBlockScanInfo* pScanInfo = NULL;
  SFileDataBlockInfo*  pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SSttBlockReader*     pSttBlockReader = pReader->status.fileIter.pSttBlockReader;
  bool                 asc = ASCENDING_TRAVERSE(pReader->info.order);
  int32_t              code = TSDB_CODE_SUCCESS;

  if (pReader->pIgnoreTables && taosHashGet(*pReader->pIgnoreTables, &pBlockInfo->uid, sizeof(pBlockInfo->uid))) {
    setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlockInfo->lastKey, pReader->info.order);
    return code;
  }

  if (pReader->code != TSDB_CODE_SUCCESS) {
    return pReader->code;
  }

  pScanInfo = getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, pReader->idStr);
  if (pScanInfo == NULL) {
    return terrno;
  }

  if (pScanInfo->sttKeyInfo.status == STT_FILE_READER_UNINIT) {
    initSttBlockReader(pSttBlockReader, pScanInfo, pReader);
  }

  TSDBKEY keyInBuf = getCurrentKeyInBuf(pScanInfo, pReader);
  if (fileBlockShouldLoad(pReader, pBlockInfo, pScanInfo, keyInBuf)) {
    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData, pScanInfo->uid);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    // build composed data block
    code = buildComposedDataBlock(pReader);
  } else if (bufferDataInFileBlockGap(keyInBuf, pBlockInfo, pScanInfo, pReader->info.order)) {
    // data in memory that are earlier than current file block and stt blocks
    // rows in buffer should be less than the file block in asc, greater than file block in desc
    int64_t endKey = getBoarderKeyInFiles(pBlockInfo, pScanInfo, pReader->info.order);
    code = buildDataBlockFromBuf(pReader, pScanInfo, endKey);
  } else {
    if (notOverlapWithFiles(pBlockInfo, pScanInfo, asc)) {
      int64_t keyInStt = pScanInfo->sttKeyInfo.nextProcKey;

      if ((!hasDataInSttBlock(pScanInfo)) || (asc && pBlockInfo->lastKey < keyInStt) ||
          (!asc && pBlockInfo->firstKey > keyInStt)) {
        if (pScanInfo->cleanSttBlocks && hasDataInSttBlock(pScanInfo)) {
          if (asc) {  // file block is located before the stt block
            ASSERT(pScanInfo->sttWindow.skey > pBlockInfo->lastKey);
          } else {  // stt block is before the file block
            ASSERT(pScanInfo->sttWindow.ekey < pBlockInfo->firstKey);
          }
        }

        buildCleanBlockFromDataFiles(pReader, pScanInfo, pBlockInfo, pBlockIter->index);
      } else {  // clean stt block
        if (asc) {
          ASSERT(pScanInfo->sttWindow.ekey < pBlockInfo->firstKey);
        } else {
          ASSERT(pScanInfo->sttWindow.skey > pBlockInfo->lastKey);
        }

        // return the stt file block
        ASSERT(pReader->info.execMode == READER_EXEC_ROWS && pSttBlockReader->mergeTree.pIter == NULL);
        buildCleanBlockFromSttFiles(pReader, pScanInfo);
        return TSDB_CODE_SUCCESS;
      }
    } else {
      SBlockData* pBData = &pReader->status.fileBlockData;
      tBlockDataReset(pBData);

      SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;

      tsdbDebug("load data in stt block firstly %s", pReader->idStr);
      int64_t st = taosGetTimestampUs();

      // let's load data from stt files, make sure clear the cleanStt block flag before load the data from stt files
      initSttBlockReader(pSttBlockReader, pScanInfo, pReader);

      // no data in stt block, no need to proceed.
      while (hasDataInSttBlock(pScanInfo)) {
        ASSERT(pScanInfo->sttKeyInfo.status == STT_FILE_HAS_DATA);

        code = buildComposedDataBlockImpl(pReader, pScanInfo, &pReader->status.fileBlockData, pSttBlockReader);
        if (code != TSDB_CODE_SUCCESS) {
          return code;
        }

        if (pResBlock->info.rows >= pReader->resBlockInfo.capacity) {
          break;
        }

        // data in stt now overlaps with current active file data block, need to composed with file data block.
        int64_t lastKeyInStt = getCurrentKeyInSttBlock(pSttBlockReader);
        if ((lastKeyInStt >= pBlockInfo->firstKey && asc) || (lastKeyInStt <= pBlockInfo->lastKey && (!asc))) {
          tsdbDebug("%p lastKeyInStt:%" PRId64 ", overlap with file block, brange:%" PRId64 "-%" PRId64 " %s", pReader,
                    lastKeyInStt, pBlockInfo->firstKey, pBlockInfo->lastKey, pReader->idStr);
          break;
        }
      }

      double el = (taosGetTimestampUs() - st) / 1000.0;
      updateComposedBlockInfo(pReader, el, pScanInfo);

      if (pResBlock->info.rows > 0) {
        tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64 " rows:%" PRId64
                  ", elapsed time:%.2f ms %s",
                  pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
                  pResBlock->info.rows, el, pReader->idStr);
      }
    }
  }

  return (pReader->code != TSDB_CODE_SUCCESS) ? pReader->code : code;
}

static int32_t buildBlockFromBufferSeqForPreFileset(STsdbReader* pReader, int64_t endKey) {
  SReaderStatus* pStatus = &pReader->status;

  tsdbDebug("seq load data blocks from cache that preceeds fileset %d, %s", pReader->status.pCurrentFileset->fid,
            pReader->idStr);

  while (1) {
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      return pReader->code;
    }

    STableBlockScanInfo** pBlockScanInfo = pStatus->pProcMemTableIter;
    if (pReader->pIgnoreTables &&
        taosHashGet(*pReader->pIgnoreTables, &(*pBlockScanInfo)->uid, sizeof((*pBlockScanInfo)->uid))) {
      bool hasNexTable = moveToNextTableForPreFileSetMem(pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }
      continue;
    }

    initMemDataIterator(*pBlockScanInfo, pReader);
    initDelSkylineIterator(*pBlockScanInfo, pReader->info.order, &pReader->cost);

    int32_t code = buildDataBlockFromBuf(pReader, *pBlockScanInfo, endKey);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->resBlockInfo.pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTableForPreFileSetMem(pStatus);
    if (!hasNexTable) {
      return TSDB_CODE_SUCCESS;
    }
  }
}

static int32_t buildBlockFromBufferSequentially(STsdbReader* pReader, int64_t endKey) {
  SReaderStatus* pStatus = &pReader->status;
  STableUidList* pUidList = &pStatus->uidList;

  tsdbDebug("seq load data blocks from cache, %s", pReader->idStr);

  while (1) {
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      return pReader->code;
    }

    STableBlockScanInfo** pBlockScanInfo = pStatus->pTableIter;
    if (pReader->pIgnoreTables &&
        taosHashGet(*pReader->pIgnoreTables, &(*pBlockScanInfo)->uid, sizeof((*pBlockScanInfo)->uid))) {
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        return TSDB_CODE_SUCCESS;
      }
      continue;
    }

    initMemDataIterator(*pBlockScanInfo, pReader);
    initDelSkylineIterator(*pBlockScanInfo, pReader->info.order, &pReader->cost);

    int32_t code = buildDataBlockFromBuf(pReader, *pBlockScanInfo, endKey);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pReader->resBlockInfo.pResBlock->info.rows > 0) {
      return TSDB_CODE_SUCCESS;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTable(pUidList, pStatus);
    if (!hasNexTable) {
      return TSDB_CODE_SUCCESS;
    }
  }
}

// set the correct start position in case of the first/last file block, according to the query time window
static void initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  int64_t             lastKey = ASCENDING_TRAVERSE(pReader->info.order) ? INT64_MIN : INT64_MAX;
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
  SReaderStatus*      pStatus = &pReader->status;
  SFileBlockDumpInfo* pDumpInfo = &pStatus->fBlockDumpInfo;

  if (pBlockInfo) {
    STableBlockScanInfo* pScanInfo = tSimpleHashGet(pBlockIter->pTableMap, &pBlockInfo->uid, sizeof(pBlockInfo->uid));
    if (pScanInfo) {
      lastKey = pScanInfo->lastProcKey;
    }

    pDumpInfo->totalRows = pBlockInfo->numRow;
    pDumpInfo->rowIndex = ASCENDING_TRAVERSE(pReader->info.order) ? 0 : pBlockInfo->numRow - 1;
  } else {
    pDumpInfo->totalRows = 0;
    pDumpInfo->rowIndex = 0;
  }

  pDumpInfo->allDumped = false;
  pDumpInfo->lastKey = lastKey;
}

static int32_t initForFirstBlockInFile(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  SBlockNumber num = {0};
  SArray*      pTableList = taosArrayInit(40, POINTER_BYTES);

  int32_t code = moveToNextFile(pReader, &num, pTableList);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pTableList);
    return code;
  }

  // all data files are consumed, try data in buffer
  if (num.numOfBlocks + num.numOfSttFiles == 0) {
    pReader->status.loadFromFile = false;
    taosArrayDestroy(pTableList);
    return code;
  }

  // initialize the block iterator for a new fileset
  if (num.numOfBlocks > 0) {
    code = initBlockIterator(pReader, pBlockIter, num.numOfBlocks, pTableList);
  } else {  // no block data, only last block exists
    tBlockDataReset(&pReader->status.fileBlockData);
    resetDataBlockIterator(pBlockIter, pReader->info.order);
    resetTableListIndex(&pReader->status);
  }

  // set the correct start position according to the query time window
  initBlockDumpInfo(pReader, pBlockIter);
  taosArrayDestroy(pTableList);
  return code;
}

static bool fileBlockPartiallyRead(SFileBlockDumpInfo* pDumpInfo, bool asc) {
  return (!pDumpInfo->allDumped) &&
         ((pDumpInfo->rowIndex > 0 && asc) || (pDumpInfo->rowIndex < (pDumpInfo->totalRows - 1) && (!asc)));
}

typedef enum {
  TSDB_READ_RETURN = 0x1,
  TSDB_READ_CONTINUE = 0x2,
} ERetrieveType;

static ERetrieveType doReadDataFromSttFiles(STsdbReader* pReader) {
  int32_t         code = TSDB_CODE_SUCCESS;
  SSDataBlock*    pResBlock = pReader->resBlockInfo.pResBlock;
  SDataBlockIter* pBlockIter = &pReader->status.blockIter;

  tsdbDebug("seq load data blocks from stt files %s", pReader->idStr);

  while (1) {
    terrno = 0;

    code = doLoadSttBlockSequentially(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = code;
      return TSDB_READ_RETURN;
    }

    if (pResBlock->info.rows > 0) {
      return TSDB_READ_RETURN;
    }

    // all data blocks are checked in this stt file, now let's try the next file set
    ASSERT(pReader->status.pTableIter == NULL);
    code = initForFirstBlockInFile(pReader, pBlockIter);

    // error happens or all the data files are completely checked
    if ((code != TSDB_CODE_SUCCESS) || (pReader->status.loadFromFile == false)) {
      terrno = code;
      return TSDB_READ_RETURN;
    }

    if (pReader->status.bProcMemPreFileset) {
      code = buildFromPreFilesetBuffer(pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
      if (pResBlock->info.rows > 0) {
        pReader->status.processingMemPreFileSet = true;
        return TSDB_READ_RETURN;
      }
    }

    if (pBlockIter->numOfBlocks > 0) {  // there are data blocks existed.
      return TSDB_READ_CONTINUE;
    } else {  // all blocks in data file are checked, let's check the data in last files
      resetTableListIndex(&pReader->status);
    }
  }
}

static int32_t buildBlockFromFiles(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);

  SDataBlockIter* pBlockIter = &pReader->status.blockIter;
  SSDataBlock*    pResBlock = pReader->resBlockInfo.pResBlock;

  if (pBlockIter->numOfBlocks == 0) {
    // let's try to extract data from stt files.
    ERetrieveType type = doReadDataFromSttFiles(pReader);
    if (type == TSDB_READ_RETURN) {
      return terrno;
    }

    code = doBuildDataBlock(pReader);
    if (code != TSDB_CODE_SUCCESS || pResBlock->info.rows > 0) {
      return code;
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
          // all data blocks in files are checked, let's check the data in last files.
          // data blocks in current file are exhausted, let's try the next file now
          SBlockData* pBlockData = &pReader->status.fileBlockData;
          if (pBlockData->uid != 0) {
            tBlockDataClear(pBlockData);
          }

          tBlockDataReset(pBlockData);
          resetDataBlockIterator(pBlockIter, pReader->info.order);
          resetTableListIndex(&pReader->status);

          ERetrieveType type = doReadDataFromSttFiles(pReader);
          if (type == TSDB_READ_RETURN) {
            return terrno;
          }
        }
      }

      code = doBuildDataBlock(pReader);
    }

    if (code != TSDB_CODE_SUCCESS || pResBlock->info.rows > 0) {
      return code;
    }
  }
}

static STsdb* getTsdbByRetentions(SVnode* pVnode, SQueryTableDataCond* pCond, SRetention* retentions, const char* idStr,
                                  int8_t* pLevel) {
  if (VND_IS_RSMA(pVnode) && !pCond->skipRollup) {
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
      if ((now - pRetention->keep) <= (pCond->twindows.skey + offset)) {
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
  if (pCond->endVersion == -1) {
    // user not specified end version, set current maximum version of vnode as the endVersion
    endVer = pVnode->state.applied;
  } else {
    endVer = (pCond->endVersion > pVnode->state.applied) ? pVnode->state.applied : pCond->endVersion;
  }

  return (SVersionRange){.minVer = startVer, .maxVer = endVer};
}

bool hasBeenDropped(const SArray* pDelList, int32_t* index, int64_t key, int64_t ver, int32_t order,
                    SVersionRange* pVerRange) {
  if (pDelList == NULL || (TARRAY_SIZE(pDelList) == 0)) {
    return false;
  }

  size_t  num = taosArrayGetSize(pDelList);
  bool    asc = ASCENDING_TRAVERSE(order);
  int32_t step = asc ? 1 : -1;

  if (asc) {
    if (*index >= num - 1) {
      TSDBKEY* last = taosArrayGetLast(pDelList);
      ASSERT(key >= last->ts);

      if (key > last->ts) {
        return false;
      } else if (key == last->ts) {
        TSDBKEY* prev = taosArrayGet(pDelList, num - 2);
        return (prev->version >= ver && prev->version <= pVerRange->maxVer && prev->version >= pVerRange->minVer);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pNext = taosArrayGet(pDelList, (*index) + 1);

      if (key < pCurrent->ts) {
        return false;
      }

      if (pCurrent->ts <= key && pNext->ts >= key && pCurrent->version >= ver &&
          pVerRange->maxVer >= pCurrent->version) {
        return true;
      }

      while (pNext->ts <= key && (*index) < num - 1) {
        (*index) += 1;

        if ((*index) < num - 1) {
          pCurrent = taosArrayGet(pDelList, *index);
          pNext = taosArrayGet(pDelList, (*index) + 1);

          // it is not a consecutive deletion range, ignore it
          if (pCurrent->version == 0 && pNext->version > 0) {
            continue;
          }

          if (pCurrent->ts <= key && pNext->ts >= key && pCurrent->version >= ver &&
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

      if (key < pFirst->ts) {
        return false;
      } else if (key == pFirst->ts) {
        return pFirst->version >= ver;
      } else {
        ASSERT(0);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pPrev = taosArrayGet(pDelList, (*index) - 1);

      if (key > pCurrent->ts) {
        return false;
      }

      if (pPrev->ts <= key && pCurrent->ts >= key && pPrev->version >= ver) {
        return true;
      }

      while (pPrev->ts >= key && (*index) > 1) {
        (*index) += step;

        if ((*index) >= 1) {
          pCurrent = taosArrayGet(pDelList, *index);
          pPrev = taosArrayGet(pDelList, (*index) - 1);

          // it is not a consecutive deletion range, ignore it
          if (pCurrent->version > 0 && pPrev->version == 0) {
            continue;
          }

          if (pPrev->ts <= key && pCurrent->ts >= key && pPrev->version >= ver) {
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
  TSDBKEY  key = TSDBROW_KEY(pRow);
  int32_t  order = pReader->info.order;
  if (outOfTimeWindow(key.ts, &pReader->info.window)) {
    pIter->hasVal = false;
    return NULL;
  }

  // it is a valid data version
  if (key.version <= pReader->info.verRange.maxVer && key.version >= pReader->info.verRange.minVer) {
    if (pDelList == NULL || TARRAY_SIZE(pDelList) == 0) {
      return pRow;
    } else {
      bool dropped = hasBeenDropped(pDelList, &pIter->index, key.ts, key.version, order, &pReader->info.verRange);
      if (!dropped) {
        return pRow;
      }
    }
  }

  while (1) {
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    if (!pIter->hasVal) {
      return NULL;
    }

    pRow = tsdbTbDataIterGet(pIter->iter);

    key = TSDBROW_KEY(pRow);
    if (outOfTimeWindow(key.ts, &pReader->info.window)) {
      pIter->hasVal = false;
      return NULL;
    }

    if (key.version <= pReader->info.verRange.maxVer && key.version >= pReader->info.verRange.minVer) {
      if (pDelList == NULL || TARRAY_SIZE(pDelList) == 0) {
        return pRow;
      } else {
        bool dropped = hasBeenDropped(pDelList, &pIter->index, key.ts, key.version, order, &pReader->info.verRange);
        if (!dropped) {
          return pRow;
        }
      }
    }
  }
}

int32_t doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, int64_t ts, SArray* pDelList, STsdbReader* pReader) {
  SRowMerger* pMerger = &pReader->status.merger;

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

    STSchema* pTSchema = NULL;
    if (pRow->type == TSDBROW_ROW_FMT) {
      pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, uid);
      if (pTSchema == NULL) {
        return terrno;
      }
    }

    tsdbRowMergerAdd(pMerger, pRow, pTSchema);
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
    tsdbRowMergerAdd(pMerger, &fRow, NULL);
    rowIndex += step;
  }

  return rowIndex;
}

typedef enum {
  CHECK_FILEBLOCK_CONT = 0x1,
  CHECK_FILEBLOCK_QUIT = 0x2,
} CHECK_FILEBLOCK_STATE;

static int32_t checkForNeighborFileBlock(STsdbReader* pReader, STableBlockScanInfo* pScanInfo,
                                         SFileDataBlockInfo* pFBlock, SRowMerger* pMerger, int64_t key,
                                         CHECK_FILEBLOCK_STATE* state) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;
  SBlockData*         pBlockData = &pReader->status.fileBlockData;
  bool                asc = ASCENDING_TRAVERSE(pReader->info.order);

  *state = CHECK_FILEBLOCK_QUIT;
  int32_t step = ASCENDING_TRAVERSE(pReader->info.order) ? 1 : -1;

  bool    loadNeighbor = true;
  int32_t code = loadNeighborIfOverlap(pFBlock, pScanInfo, pReader, &loadNeighbor);

  if (loadNeighbor && (code == TSDB_CODE_SUCCESS)) {
    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->info.verRange, step);
    if ((pDumpInfo->rowIndex >= pDumpInfo->totalRows && asc) || (pDumpInfo->rowIndex < 0 && !asc)) {
      *state = CHECK_FILEBLOCK_CONT;
    }
  }

  return code;
}

int32_t doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  SFileBlockDumpInfo* pDumpInfo = &pReader->status.fBlockDumpInfo;

  SRowMerger* pMerger = &pReader->status.merger;
  bool        asc = ASCENDING_TRAVERSE(pReader->info.order);
  int64_t     key = pBlockData->aTSKEY[pDumpInfo->rowIndex];
  int32_t     step = asc ? 1 : -1;

  pDumpInfo->rowIndex += step;
  if ((pDumpInfo->rowIndex <= pBlockData->nRow - 1 && asc) || (pDumpInfo->rowIndex >= 0 && !asc)) {
    pDumpInfo->rowIndex =
        doMergeRowsInFileBlockImpl(pBlockData, pDumpInfo->rowIndex, key, pMerger, &pReader->info.verRange, step);
  }

  // all rows are consumed, let's try next file block
  if ((pDumpInfo->rowIndex >= pBlockData->nRow && asc) || (pDumpInfo->rowIndex < 0 && !asc)) {
    while (1) {
      CHECK_FILEBLOCK_STATE st;

      SFileDataBlockInfo* pFileBlockInfo = getCurrentBlockInfo(&pReader->status.blockIter);
      if (pFileBlockInfo == NULL) {
        st = CHECK_FILEBLOCK_QUIT;
        break;
      }

      checkForNeighborFileBlock(pReader, pScanInfo, pFileBlockInfo, pMerger, key, &st);
      if (st == CHECK_FILEBLOCK_QUIT) {
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeRowsInSttBlock(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, int64_t ts,
                              SRowMerger* pMerger, SVersionRange* pVerRange, const char* idStr) {
  while (nextRowFromSttBlocks(pSttBlockReader, pScanInfo, pVerRange)) {
    int64_t next1 = getCurrentKeyInSttBlock(pSttBlockReader);
    if (next1 == ts) {
      TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      tsdbRowMergerAdd(pMerger, pRow1, NULL);
    } else {
      tsdbTrace("uid:%" PRIu64 " last del index:%d, del range:%d, lastKeyInStt:%" PRId64 ", %s", pScanInfo->uid,
                pScanInfo->sttBlockDelIndex, (int32_t)taosArrayGetSize(pScanInfo->delSkyline),
                pScanInfo->sttKeyInfo.nextProcKey, idStr);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeMemTableMultiRows(TSDBROW* pRow, uint64_t uid, SIterInfo* pIter, SArray* pDelList, TSDBROW* pResRow,
                                 STsdbReader* pReader, bool* freeTSRow) {
  TSDBROW* pNextRow = NULL;
  TSDBROW  current = *pRow;

  {  // if the timestamp of the next valid row has a different ts, return current row directly
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);

    if (!pIter->hasVal) {
      *pResRow = *pRow;
      *freeTSRow = false;
      return TSDB_CODE_SUCCESS;
    } else {  // has next point in mem/imem
      pNextRow = getValidMemRow(pIter, pDelList, pReader);
      if (pNextRow == NULL) {
        *pResRow = current;
        *freeTSRow = false;
        return TSDB_CODE_SUCCESS;
      }

      if (TSDBROW_TS(&current) != TSDBROW_TS(pNextRow)) {
        *pResRow = current;
        *freeTSRow = false;
        return TSDB_CODE_SUCCESS;
      }
    }
  }

  terrno = 0;
  int32_t code = 0;

  // start to merge duplicated rows
  STSchema* pTSchema = NULL;
  if (current.type == TSDBROW_ROW_FMT) {  // get the correct schema for row-wise data in memory
    pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(&current), pReader, uid);
    if (pTSchema == NULL) {
      return terrno;
    }
  }

  code = tsdbRowMergerAdd(&pReader->status.merger, &current, pTSchema);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  STSchema* pTSchema1 = NULL;
  if (pNextRow->type == TSDBROW_ROW_FMT) {  // get the correct schema for row-wise data in memory
    pTSchema1 = doGetSchemaForTSRow(TSDBROW_SVERSION(pNextRow), pReader, uid);
    if (pTSchema1 == NULL) {
      return terrno;
    }
  }

  code = tsdbRowMergerAdd(&pReader->status.merger, pNextRow, pTSchema1);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = doMergeRowsInBuf(pIter, uid, TSDBROW_TS(&current), pDelList, pReader);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = tsdbRowMergerGetRow(&pReader->status.merger, &pResRow->pTSRow);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  pResRow->type = TSDBROW_ROW_FMT;
  tsdbRowMergerClear(&pReader->status.merger);
  *freeTSRow = true;

  return TSDB_CODE_SUCCESS;
}

int32_t doMergeMemIMemRows(TSDBROW* pRow, TSDBROW* piRow, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader,
                           SRow** pTSRow) {
  SRowMerger* pMerger = &pReader->status.merger;

  TSDBKEY k = TSDBROW_KEY(pRow);
  TSDBKEY ik = TSDBROW_KEY(piRow);

  STSchema* pSchema = NULL;
  if (pRow->type == TSDBROW_ROW_FMT) {
    pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
    if (pSchema == NULL) {
      return terrno;
    }
  }

  STSchema* piSchema = NULL;
  if (piRow->type == TSDBROW_ROW_FMT) {
    piSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
    if (piSchema == NULL) {
      return terrno;
    }
  }

  if (ASCENDING_TRAVERSE(pReader->info.order)) {  // ascending order imem --> mem
    int32_t code = tsdbRowMergerAdd(&pReader->status.merger, piRow, piSchema);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tsdbRowMergerAdd(&pReader->status.merger, pRow, pSchema);
    code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

  } else {
    int32_t code = tsdbRowMergerAdd(&pReader->status.merger, pRow, pSchema);
    if (code != TSDB_CODE_SUCCESS || pMerger->pTSchema == NULL) {
      return code;
    }

    code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, k.ts, pBlockScanInfo->delSkyline, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tsdbRowMergerAdd(&pReader->status.merger, piRow, piSchema);
    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, ik.ts, pBlockScanInfo->delSkyline, pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  int32_t code = tsdbRowMergerGetRow(pMerger, pTSRow);
  tsdbRowMergerClear(pMerger);
  return code;
}

static int32_t tsdbGetNextRowInMem(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, TSDBROW* pResRow,
                                   int64_t endKey, bool* freeTSRow) {
  TSDBROW* pRow = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader);
  TSDBROW* piRow = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader);
  SArray*  pDelList = pBlockScanInfo->delSkyline;
  uint64_t uid = pBlockScanInfo->uid;

  // todo refactor
  bool asc = ASCENDING_TRAVERSE(pReader->info.order);
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
        code = doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, pResRow, pReader, freeTSRow);
      } else if (((k.ts < ik.ts) && asc) || ((k.ts > ik.ts) && (!asc))) {
        code = doMergeMemTableMultiRows(pRow, uid, &pBlockScanInfo->iter, pDelList, pResRow, pReader, freeTSRow);
      }
    } else {  // ik.ts == k.ts
      *freeTSRow = true;
      pResRow->type = TSDBROW_ROW_FMT;
      code = doMergeMemIMemRows(pRow, piRow, pBlockScanInfo, pReader, &pResRow->pTSRow);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    return code;
  }

  if (pBlockScanInfo->iter.hasVal && pRow != NULL) {
    return doMergeMemTableMultiRows(pRow, pBlockScanInfo->uid, &pBlockScanInfo->iter, pDelList, pResRow, pReader,
                                    freeTSRow);
  }

  if (pBlockScanInfo->iiter.hasVal && piRow != NULL) {
    return doMergeMemTableMultiRows(piRow, uid, &pBlockScanInfo->iiter, pDelList, pResRow, pReader, freeTSRow);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, SRow* pTSRow, STableBlockScanInfo* pScanInfo) {
  int32_t outputRowIndex = pBlock->info.rows;
  int64_t uid = pScanInfo->uid;
  int32_t code = TSDB_CODE_SUCCESS;

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  STSchema*           pSchema = doGetSchemaForTSRow(pTSRow->sver, pReader, uid);
  if (pSchema == NULL) {
    return terrno;
  }

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

      tRowGet(pTSRow, pSchema, j, &colVal);
      code = doCopyColVal(pColInfoData, outputRowIndex, i, &colVal, pSupInfo);
      if (code) {
        return code;
      }
      i += 1;
      j += 1;
    } else if (colId < pSchema->columns[j].colId) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);

      colDataSetNULL(pColInfoData, outputRowIndex);
      i += 1;
    } else if (colId > pSchema->columns[j].colId) {
      j += 1;
    }
  }

  // set null value since current column does not exist in the "pSchema"
  while (i < pSupInfo->numOfCols) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
    colDataSetNULL(pColInfoData, outputRowIndex);
    i += 1;
  }

  pBlock->info.dataLoad = 1;
  pBlock->info.rows += 1;
  pScanInfo->lastProcKey = pTSRow->ts;
  return TSDB_CODE_SUCCESS;
}

int32_t doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                 int32_t rowIndex) {
  int32_t i = 0, j = 0;
  int32_t outputRowIndex = pResBlock->info.rows;
  int32_t code = TSDB_CODE_SUCCESS;

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  ((int64_t*)pReader->status.pPrimaryTsCol->pData)[outputRowIndex] = pBlockData->aTSKEY[rowIndex];
  i += 1;

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
      code = doCopyColVal(pCol, outputRowIndex, i, &cv, pSupInfo);
      if (code) {
        return code;
      }
      j += 1;
    } else if (pData->cid > pCol->info.colId) {
      // the specified column does not exist in file block, fill with null data
      colDataSetNULL(pCol, outputRowIndex);
    }

    i += 1;
  }

  while (i < numOfOutputCols) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    colDataSetNULL(pCol, outputRowIndex);
    i += 1;
  }

  pResBlock->info.dataLoad = 1;
  pResBlock->info.rows += 1;
  return TSDB_CODE_SUCCESS;
}

int32_t buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                  STsdbReader* pReader) {
  SSDataBlock* pBlock = pReader->resBlockInfo.pResBlock;
  int32_t      code = TSDB_CODE_SUCCESS;

  do {
    TSDBROW row = {.type = -1};
    bool    freeTSRow = false;
    tsdbGetNextRowInMem(pBlockScanInfo, pReader, &row, endKey, &freeTSRow);
    if (row.type == -1) {
      break;
    }

    if (row.type == TSDBROW_ROW_FMT) {
      int64_t ts = row.pTSRow->ts;
      code = doAppendRowFromTSRow(pBlock, pReader, row.pTSRow, pBlockScanInfo);

      if (freeTSRow) {
        taosMemoryFree(row.pTSRow);
      }

      if (code) {
        return code;
      }

      pBlockScanInfo->lastProcKey = ts;
    } else {
      code = doAppendRowFromFileBlock(pBlock, pReader, row.pBlockData, row.iRow);
      if (code) {
        break;
      }
      pBlockScanInfo->lastProcKey = row.pBlockData->aTSKEY[row.iRow];
    }

    // no data in buffer, return immediately
    if (!(pBlockScanInfo->iter.hasVal || pBlockScanInfo->iiter.hasVal)) {
      break;
    }

    if (pBlock->info.rows >= capacity) {
      break;
    }
  } while (1);

  return code;
}

// TODO refactor: with createDataBlockScanInfo
int32_t tsdbSetTableList2(STsdbReader* pReader, const void* pTableList, int32_t num) {
  int32_t size = tSimpleHashGetSize(pReader->status.pTableMap);

  STableBlockScanInfo** p = NULL;
  int32_t               iter = 0;

  while ((p = tSimpleHashIterate(pReader->status.pTableMap, p, &iter)) != NULL) {
    clearBlockScanInfo(*p);
  }

  if (size < num) {
    int32_t code = ensureBlockScanInfoBuf(&pReader->blockInfoBuf, num);
    if (code) {
      return code;
    }

    char* p1 = taosMemoryRealloc(pReader->status.uidList.tableUidList, sizeof(uint64_t) * num);
    if (p1 == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    pReader->status.uidList.tableUidList = (uint64_t*)p1;
  }

  tSimpleHashClear(pReader->status.pTableMap);
  STableUidList* pUidList = &pReader->status.uidList;
  pUidList->currentIndex = 0;

  STableKeyInfo* pList = (STableKeyInfo*)pTableList;
  for (int32_t i = 0; i < num; ++i) {
    STableBlockScanInfo* pInfo = getPosInBlockInfoBuf(&pReader->blockInfoBuf, i);
    pInfo->uid = pList[i].uid;
    pUidList->tableUidList[i] = pList[i].uid;

    // todo extract method
    if (ASCENDING_TRAVERSE(pReader->info.order)) {
      int64_t skey = pReader->info.window.skey;
      pInfo->lastProcKey = (skey > INT64_MIN) ? (skey - 1) : skey;
      pInfo->sttKeyInfo.nextProcKey = skey;
    } else {
      int64_t ekey = pReader->info.window.ekey;
      pInfo->lastProcKey = (ekey < INT64_MAX) ? (ekey + 1) : ekey;
      pInfo->sttKeyInfo.nextProcKey = ekey;
    }

    pInfo->sttKeyInfo.status = STT_FILE_READER_UNINIT;
    tSimpleHashPut(pReader->status.pTableMap, &pInfo->uid, sizeof(uint64_t), &pInfo, POINTER_BYTES);
  }

  return TDB_CODE_SUCCESS;
}

void* tsdbGetIdx2(SMeta* pMeta) {
  if (pMeta == NULL) {
    return NULL;
  }
  return metaGetIdx(pMeta);
}

void* tsdbGetIvtIdx2(SMeta* pMeta) {
  if (pMeta == NULL) {
    return NULL;
  }
  return metaGetIvtIdx(pMeta);
}

uint64_t tsdbGetReaderMaxVersion2(STsdbReader* pReader) { return pReader->info.verRange.maxVer; }

static int32_t doOpenReaderImpl(STsdbReader* pReader) {
  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockIter* pBlockIter = &pStatus->blockIter;

  if (pReader->bFilesetDelimited) {
    getMemTableTimeRange(pReader, &pReader->status.memTableMaxKey, &pReader->status.memTableMinKey);
    pReader->status.bProcMemFirstFileset = true;
  }

  initFilesetIterator(&pStatus->fileIter, pReader->pReadSnap->pfSetArray, pReader);
  resetDataBlockIterator(&pStatus->blockIter, pReader->info.order);

  int32_t code = TSDB_CODE_SUCCESS;
  if (pStatus->fileIter.numOfFiles == 0) {
    pStatus->loadFromFile = false;
    //  } else if (READER_EXEC_DATA == pReader->info.readMode) {
    // DO NOTHING
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
  }

  if (!pStatus->loadFromFile) {
    resetTableListIndex(pStatus);
  }

  return code;
}

static void freeSchemaFunc(void* param) {
  void** p = (void**)param;
  taosMemoryFreeClear(*p);
}

static void clearSharedPtr(STsdbReader* p) {
  p->status.pTableMap = NULL;
  p->status.uidList.tableUidList = NULL;
  p->info.pSchema = NULL;
  p->pReadSnap = NULL;
  p->pSchemaMap = NULL;
}

static void setSharedPtr(STsdbReader* pDst, const STsdbReader* pSrc) {
  pDst->status.pTableMap = pSrc->status.pTableMap;
  pDst->status.uidList = pSrc->status.uidList;
  pDst->info.pSchema = pSrc->info.pSchema;
  pDst->pSchemaMap = pSrc->pSchemaMap;
  pDst->pReadSnap = pSrc->pReadSnap;
  pDst->pReadSnap->pfSetArray = pSrc->pReadSnap->pfSetArray;

  if (pDst->info.pSchema) {
    tsdbRowMergerInit(&pDst->status.merger, pDst->info.pSchema);
  }
}

// ====================================== EXPOSED APIs ======================================
int32_t tsdbReaderOpen2(void* pVnode, SQueryTableDataCond* pCond, void* pTableList, int32_t numOfTables,
                        SSDataBlock* pResBlock, void** ppReader, const char* idstr, SHashObj** pIgnoreTables) {
  STimeWindow window = pCond->twindows;
  SVnodeCfg*  pConf = &(((SVnode*)pVnode)->config);

  int32_t capacity = pConf->tsdbCfg.maxRows;
  if (pResBlock != NULL) {
    blockDataEnsureCapacity(pResBlock, capacity);
  }

  int32_t code = tsdbReaderCreate(pVnode, pCond, ppReader, capacity, pResBlock, idstr);
  if (code != TSDB_CODE_SUCCESS) {
    goto _err;
  }

  // check for query time window
  STsdbReader* pReader = *ppReader;
  if (isEmptyQueryTimeWindow(&pReader->info.window) && pCond->type == TIMEWINDOW_RANGE_CONTAINED) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, %s", pReader, pReader->idStr);
    return TSDB_CODE_SUCCESS;
  }

  if (pCond->type == TIMEWINDOW_RANGE_EXTERNAL) {
    // update the SQueryTableDataCond to create inner reader
    int32_t order = pCond->order;
    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.ekey = window.skey - 1;
      pCond->twindows.skey = INT64_MIN;
      pCond->order = TSDB_ORDER_DESC;
    } else {
      pCond->twindows.skey = window.ekey + 1;
      pCond->twindows.ekey = INT64_MAX;
      pCond->order = TSDB_ORDER_ASC;
    }

    // here we only need one more row, so the capacity is set to be ONE.
    code = tsdbReaderCreate(pVnode, pCond, (void**)&((STsdbReader*)pReader)->innerReader[0], 1, pResBlock, idstr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.skey = window.ekey + 1;
      pCond->twindows.ekey = INT64_MAX;
    } else {
      pCond->twindows.skey = INT64_MIN;
      pCond->twindows.ekey = window.ekey - 1;
    }
    pCond->order = order;

    code = tsdbReaderCreate(pVnode, pCond, (void**)&((STsdbReader*)pReader)->innerReader[1], 1, pResBlock, idstr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }
  }

  // NOTE: the endVersion in pCond is the data version not schema version, so pCond->endVersion is not correct here.
  //  no valid error code set in metaGetTbTSchema, so let's set the error code here.
  //  we should proceed in case of tmq processing.
  if (pCond->suid != 0) {
    pReader->info.pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, -1, 1);
    if (pReader->info.pSchema == NULL) {
      tsdbError("failed to get table schema, suid:%" PRIu64 ", ver:-1, %s", pReader->info.suid, pReader->idStr);
    }
  } else if (numOfTables > 0) {
    STableKeyInfo* pKey = pTableList;
    pReader->info.pSchema = metaGetTbTSchema(pReader->pTsdb->pVnode->pMeta, pKey->uid, -1, 1);
    if (pReader->info.pSchema == NULL) {
      tsdbError("failed to get table schema, uid:%" PRIu64 ", ver:-1, %s", pKey->uid, pReader->idStr);
    }
  }

  if (pReader->info.pSchema != NULL) {
    tsdbRowMergerInit(&pReader->status.merger, pReader->info.pSchema);
  }

  pReader->pSchemaMap = tSimpleHashInit(8, taosFastHash);
  if (pReader->pSchemaMap == NULL) {
    tsdbError("failed init schema hash for reader %s", pReader->idStr);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tSimpleHashSetFreeFp(pReader->pSchemaMap, freeSchemaFunc);
  if (pReader->info.pSchema != NULL) {
    code = updateBlockSMAInfo(pReader->info.pSchema, &pReader->suppInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }
  }

  STsdbReader* p = (pReader->innerReader[0] != NULL) ? pReader->innerReader[0] : pReader;
  pReader->status.pTableMap =
      createDataBlockScanInfo(p, &pReader->blockInfoBuf, pTableList, &pReader->status.uidList, numOfTables);
  if (pReader->status.pTableMap == NULL) {
    *ppReader = NULL;
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pReader->status.pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
  if (pReader->status.pLDataIterArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  pReader->flag = READER_STATUS_SUSPEND;
  pReader->info.execMode = pCond->notLoadData ? READER_EXEC_ROWS : READER_EXEC_DATA;

  pReader->pIgnoreTables = pIgnoreTables;
  tsdbDebug("%p total numOfTable:%d, window:%" PRId64 " - %" PRId64 ", verRange:%" PRId64 " - %" PRId64
            " in this query %s",
            pReader, numOfTables, pReader->info.window.skey, pReader->info.window.ekey, pReader->info.verRange.minVer,
            pReader->info.verRange.maxVer, pReader->idStr);

  return code;

_err:
  tsdbError("failed to create data reader, code:%s %s", tstrerror(code), idstr);
  tsdbReaderClose2(*ppReader);
  *ppReader = NULL;  // reset the pointer value.
  return code;
}

void tsdbReaderClose2(STsdbReader* pReader) {
  if (pReader == NULL) {
    return;
  }

  tsdbAcquireReader(pReader);

  {
    if (pReader->innerReader[0] != NULL || pReader->innerReader[1] != NULL) {
      STsdbReader* p = pReader->innerReader[0];
      clearSharedPtr(p);

      p = pReader->innerReader[1];
      clearSharedPtr(p);

      tsdbReaderClose2(pReader->innerReader[0]);
      tsdbReaderClose2(pReader->innerReader[1]);
    }
  }

  SBlockLoadSuppInfo* pSupInfo = &pReader->suppInfo;
  TARRAY2_DESTROY(&pSupInfo->colAggArray, NULL);
  for (int32_t i = 0; i < pSupInfo->numOfCols; ++i) {
    if (pSupInfo->buildBuf[i] != NULL) {
      taosMemoryFreeClear(pSupInfo->buildBuf[i]);
    }
  }

  if (pReader->resBlockInfo.freeBlock) {
    pReader->resBlockInfo.pResBlock = blockDataDestroy(pReader->resBlockInfo.pResBlock);
  }

  taosMemoryFree(pSupInfo->colId);
  tBlockDataDestroy(&pReader->status.fileBlockData);
  cleanupDataBlockIterator(&pReader->status.blockIter);

  size_t numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  if (pReader->status.pTableMap != NULL) {
    destroyAllBlockScanInfo(pReader->status.pTableMap);
    pReader->status.pTableMap = NULL;
  }
  clearBlockScanInfoBuf(&pReader->blockInfoBuf);

  if (pReader->pFileReader != NULL) {
    tsdbDataFileReaderClose(&pReader->pFileReader);
  }

  SReadCostSummary* pCost = &pReader->cost;
  SFilesetIter*     pFilesetIter = &pReader->status.fileIter;
  if (pFilesetIter->pSttBlockReader != NULL) {
    SSttBlockReader* pLReader = pFilesetIter->pSttBlockReader;
    tMergeTreeClose(&pLReader->mergeTree);
    taosMemoryFree(pLReader);
  }

  destroySttBlockReader(pReader->status.pLDataIterArray, &pCost->sttCost);
  taosMemoryFreeClear(pReader->status.uidList.tableUidList);

  qTrace("tsdb/reader-close: %p, untake snapshot", pReader);
  void* p = pReader->pReadSnap;
  if ((p == atomic_val_compare_exchange_ptr((void**)&pReader->pReadSnap, p, NULL)) && (p != NULL)) {
    tsdbUntakeReadSnap2(pReader, p, true);
  }

  tsem_destroy(&pReader->resumeAfterSuspend);
  tsdbReleaseReader(pReader);
  tsdbUninitReaderLock(pReader);

  tsdbDebug(
      "%p :io-cost summary: head-file:%" PRIu64 ", head-file time:%.2f ms, SMA:%" PRId64
      " SMA-time:%.2f ms, fileBlocks:%" PRId64
      ", fileBlocks-load-time:%.2f ms, "
      "build in-memory-block-time:%.2f ms, sttBlocks:%" PRId64 ", sttBlocks-time:%.2f ms, sttStatisBlock:%" PRId64
      ", stt-statis-Block-time:%.2f ms, composed-blocks:%" PRId64
      ", composed-blocks-time:%.2fms, STableBlockScanInfo size:%.2f Kb, createTime:%.2f ms,createSkylineIterTime:%.2f "
      "ms, initSttBlockReader:%.2fms, %s",
      pReader, pCost->headFileLoad, pCost->headFileLoadTime, pCost->smaDataLoad, pCost->smaLoadTime, pCost->numOfBlocks,
      pCost->blockLoadTime, pCost->buildmemBlock, pCost->sttCost.loadBlocks, pCost->sttCost.blockElapsedTime,
      pCost->sttCost.loadStatisBlocks, pCost->sttCost.statisElapsedTime, pCost->composedBlocks,
      pCost->buildComposedBlockTime, numOfTables * sizeof(STableBlockScanInfo) / 1000.0, pCost->createScanInfoList,
      pCost->createSkylineIterTime, pCost->initSttBlockReader, pReader->idStr);

  taosMemoryFree(pReader->idStr);

  tsdbRowMergerCleanup(&pReader->status.merger);
  taosMemoryFree(pReader->info.pSchema);

  tSimpleHashCleanup(pReader->pSchemaMap);
  taosMemoryFreeClear(pReader);
}

static int32_t doSuspendCurrentReader(STsdbReader* pCurrentReader) {
  SReaderStatus* pStatus = &pCurrentReader->status;

  if (pStatus->loadFromFile) {
    tsdbDataFileReaderClose(&pCurrentReader->pFileReader);

    SReadCostSummary* pCost = &pCurrentReader->cost;
    pStatus->pLDataIterArray = destroySttBlockReader(pStatus->pLDataIterArray, &pCost->sttCost);
    pStatus->pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
  }

  // resetDataBlockScanInfo excluding lastKey
  STableBlockScanInfo** p = NULL;

  int32_t step = ASCENDING_TRAVERSE(pCurrentReader->info.order) ? 1 : -1;
  int32_t iter = 0;
  while ((p = tSimpleHashIterate(pStatus->pTableMap, p, &iter)) != NULL) {
    STableBlockScanInfo* pInfo = *(STableBlockScanInfo**)p;
    clearBlockScanInfo(pInfo);
    pInfo->sttKeyInfo.nextProcKey = pInfo->lastProcKey + step;
  }

  pStatus->uidList.currentIndex = 0;
  initReaderStatus(pStatus);

  return TSDB_CODE_SUCCESS;
}

int32_t tsdbReaderSuspend2(STsdbReader* pReader) {
  // save reader's base state & reset top state to be reconstructed from base state
  int32_t code = 0;
  pReader->status.suspendInvoked = true;  // record the suspend status

  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    if (pReader->step == EXTERNAL_ROWS_PREV) {
      doSuspendCurrentReader(pReader->innerReader[0]);
    } else if (pReader->step == EXTERNAL_ROWS_MAIN) {
      doSuspendCurrentReader(pReader);
    } else {
      doSuspendCurrentReader(pReader->innerReader[1]);
    }
  } else {
    doSuspendCurrentReader(pReader);
  }

  // make sure only release once
  void* p = pReader->pReadSnap;
  if ((p == atomic_val_compare_exchange_ptr((void**)&pReader->pReadSnap, p, NULL)) && (p != NULL)) {
    tsdbUntakeReadSnap2(pReader, p, false);
  }

  if (pReader->bFilesetDelimited) {
    pReader->status.memTableMinKey = INT64_MAX;
    pReader->status.memTableMaxKey = INT64_MIN;
  }
  pReader->flag = READER_STATUS_SUSPEND;

  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    clearSharedPtr(pReader->innerReader[0]);
    clearSharedPtr(pReader->innerReader[1]);
  }

#if SUSPEND_RESUME_TEST
  tsem_post(&pReader->resumeAfterSuspend);
#endif

  tsdbDebug("reader: %p suspended in this query %s, step:%d", pReader, pReader->idStr, pReader->step);
  return code;
}

static int32_t tsdbSetQueryReseek(void* pQHandle) {
  int32_t      code = 0;
  STsdbReader* pReader = pQHandle;

  code = tsdbTryAcquireReader(pReader);
  if (code == 0) {
    if (pReader->flag == READER_STATUS_SUSPEND) {
      tsdbReleaseReader(pReader);
      return code;
    }

    tsdbReaderSuspend2(pReader);
    tsdbReleaseReader(pReader);

    return code;
  } else if (code == EBUSY) {
    return TSDB_CODE_VND_QUERY_BUSY;
  } else {
    terrno = TAOS_SYSTEM_ERROR(code);
    return TSDB_CODE_FAILED;
  }
}

int32_t tsdbReaderResume2(STsdbReader* pReader) {
  int32_t               code = 0;
  STableBlockScanInfo** pBlockScanInfo = pReader->status.pTableIter;

  //  restore reader's state, task snapshot
  int32_t numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  if (numOfTables > 0) {
    qTrace("tsdb/reader: %p, take snapshot", pReader);
    code = tsdbTakeReadSnap2(pReader, tsdbSetQueryReseek, &pReader->pReadSnap);
    if (code != TSDB_CODE_SUCCESS) {
      goto _err;
    }

    if (pReader->type == TIMEWINDOW_RANGE_CONTAINED) {
      code = doOpenReaderImpl(pReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    } else {
      STsdbReader* pPrevReader = pReader->innerReader[0];
      STsdbReader* pNextReader = pReader->innerReader[1];

      // we need only one row
      pPrevReader->resBlockInfo.capacity = 1;
      setSharedPtr(pPrevReader, pReader);

      pNextReader->resBlockInfo.capacity = 1;
      setSharedPtr(pNextReader, pReader);

      if (pReader->step == 0 || pReader->step == EXTERNAL_ROWS_PREV) {
        code = doOpenReaderImpl(pPrevReader);
      } else if (pReader->step == EXTERNAL_ROWS_MAIN) {
        code = doOpenReaderImpl(pReader);
      } else {
        code = doOpenReaderImpl(pNextReader);
      }

      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  pReader->flag = READER_STATUS_NORMAL;
  tsdbDebug("reader: %p resumed uid %" PRIu64 ", numOfTable:%" PRId32 ", in this query %s", pReader,
            pBlockScanInfo ? (*pBlockScanInfo)->uid : 0, numOfTables, pReader->idStr);
  return code;

_err:
  tsdbError("failed to resume data reader, code:%s %s", tstrerror(code), pReader->idStr);
  return code;
}

static int32_t buildFromPreFilesetBuffer(STsdbReader* pReader) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SReaderStatus* pStatus = &pReader->status;

  SSDataBlock* pBlock = pReader->resBlockInfo.pResBlock;

  int32_t     fid = pReader->status.pCurrentFileset->fid;
  STimeWindow win = {0};
  tsdbFidKeyRange(fid, pReader->pTsdb->keepCfg.days, pReader->pTsdb->keepCfg.precision, &win.skey, &win.ekey);

  int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? win.skey : win.ekey;
  code = buildBlockFromBufferSeqForPreFileset(pReader, endKey);
  if (code != TSDB_CODE_SUCCESS || pBlock->info.rows > 0) {
    return code;
  } else {
    tsdbDebug("finished pre-fileset %d buffer processing. %s", fid, pReader->idStr);
    pStatus->bProcMemPreFileset = false;
    pStatus->processingMemPreFileSet = false;
    if (pReader->notifyFn) {
      STsdReaderNotifyInfo info = {0};
      info.duration.filesetId = fid;
      pReader->notifyFn(TSD_READER_NOTIFY_DURATION_START, &info, pReader->notifyParam);
      tsdbDebug("new duration %d start notification when buffer pre-fileset, %s", fid, pReader->idStr);
    }
  }
  return code;
}

static int32_t doTsdbNextDataBlockFilesetDelimited(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;
  int32_t        code = TSDB_CODE_SUCCESS;
  SSDataBlock*   pBlock = pReader->resBlockInfo.pResBlock;

  if (pStatus->loadFromFile) {
    if (pStatus->bProcMemPreFileset) {
      code = buildFromPreFilesetBuffer(pReader);
      if (code != TSDB_CODE_SUCCESS || pBlock->info.rows > 0) {
        return code;
      }
    }

    code = buildBlockFromFiles(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    tsdbTrace("block from file rows: %" PRId64 ", will process pre-file set buffer: %d. %s", pBlock->info.rows,
              pStatus->bProcMemFirstFileset, pReader->idStr);
    if (pStatus->bProcMemPreFileset) {
      if (pBlock->info.rows > 0) {
        if (pReader->notifyFn && !pReader->status.processingMemPreFileSet) {
          int32_t              fid = pReader->status.pCurrentFileset->fid;
          STsdReaderNotifyInfo info = {0};
          info.duration.filesetId = fid;
          pReader->notifyFn(TSD_READER_NOTIFY_NEXT_DURATION_BLOCK, &info, pReader->notifyParam);
        }
      } else {
        pStatus->bProcMemPreFileset = false;
      }
    }

    if (pBlock->info.rows <= 0) {
      resetTableListIndex(&pReader->status);
      int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
      code = buildBlockFromBufferSequentially(pReader, endKey);
    }
  } else {  // no data in files, let's try the buffer
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
    code = buildBlockFromBufferSequentially(pReader, endKey);
  }
  return code;
}

static int32_t doTsdbNextDataBlockFilesFirst(STsdbReader* pReader) {
  SReaderStatus* pStatus = &pReader->status;
  int32_t        code = TSDB_CODE_SUCCESS;
  SSDataBlock*   pBlock = pReader->resBlockInfo.pResBlock;

  if (pStatus->loadFromFile) {
    code = buildBlockFromFiles(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    if (pBlock->info.rows <= 0) {
      resetTableListIndex(&pReader->status);
      int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
      code = buildBlockFromBufferSequentially(pReader, endKey);
    }
  } else {  // no data in files, let's try the buffer
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
    code = buildBlockFromBufferSequentially(pReader, endKey);
  }
  return code;
}

static int32_t doTsdbNextDataBlock2(STsdbReader* pReader, bool* hasNext) {
  int32_t code = TSDB_CODE_SUCCESS;

  // cleanup the data that belongs to the previous data block
  SSDataBlock* pBlock = pReader->resBlockInfo.pResBlock;
  blockDataCleanup(pBlock);

  *hasNext = false;

  SReaderStatus* pStatus = &pReader->status;
  if (tSimpleHashGetSize(pStatus->pTableMap) == 0) {
    return code;
  }

  if (!pReader->bFilesetDelimited) {
    code = doTsdbNextDataBlockFilesFirst(pReader);
  } else {
    code = doTsdbNextDataBlockFilesetDelimited(pReader);
  }

  *hasNext = pBlock->info.rows > 0;

  return code;
}

int32_t tsdbNextDataBlock2(STsdbReader* pReader, bool* hasNext) {
  int32_t code = TSDB_CODE_SUCCESS;

  *hasNext = false;

  if (isEmptyQueryTimeWindow(&pReader->info.window) || pReader->step == EXTERNAL_ROWS_NEXT ||
      pReader->code != TSDB_CODE_SUCCESS) {
    return (pReader->code != TSDB_CODE_SUCCESS) ? pReader->code : code;
  }

  SReaderStatus* pStatus = &pReader->status;

  // NOTE: the following codes is used to perform test for suspend/resume for tsdbReader when it blocks the commit
  // the data should be ingested in round-robin and all the child tables should be createted before ingesting data
  // the version range of query will be used to identify the correctness of suspend/resume functions.
  // this function will blocked before loading the SECOND block from vnode-buffer, and restart itself from sst-files
#if SUSPEND_RESUME_TEST
  if (!pReader->status.suspendInvoked && !pReader->status.loadFromFile) {
    tsem_wait(&pReader->resumeAfterSuspend);
  }
#endif

  code = tsdbAcquireReader(pReader);
  qTrace("tsdb/read: %p, take read mutex, code: %d", pReader, code);

  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbReleaseReader(pReader);
      return code;
    }
  }

  if (pReader->innerReader[0] != NULL && pReader->step == 0) {
    code = doTsdbNextDataBlock2(pReader->innerReader[0], hasNext);
    if (code) {
      tsdbReleaseReader(pReader);
      return code;
    }

    pReader->step = EXTERNAL_ROWS_PREV;
    if (*hasNext) {
      pStatus = &pReader->innerReader[0]->status;
      if (pStatus->composedDataBlock) {
        qTrace("tsdb/read: %p, unlock read mutex", pReader);
        tsdbReleaseReader(pReader);
      }

      return code;
    }
  }

  if (pReader->step == EXTERNAL_ROWS_PREV) {
    // prepare for the main scan
    code = doOpenReaderImpl(pReader);
    int32_t step = 1;
    resetAllDataBlockScanInfo(pReader->status.pTableMap, pReader->innerReader[0]->info.window.ekey, step);

    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    pReader->step = EXTERNAL_ROWS_MAIN;
  }

  code = doTsdbNextDataBlock2(pReader, hasNext);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbReleaseReader(pReader);
    return code;
  }

  if (*hasNext) {
    if (pStatus->composedDataBlock) {
      qTrace("tsdb/read: %p, unlock read mutex", pReader);
      tsdbReleaseReader(pReader);
    }

    return code;
  }

  if (pReader->step == EXTERNAL_ROWS_MAIN && pReader->innerReader[1] != NULL) {
    // prepare for the next row scan
    int32_t step = -1;
    code = doOpenReaderImpl(pReader->innerReader[1]);
    resetAllDataBlockScanInfo(pReader->innerReader[1]->status.pTableMap, pReader->info.window.ekey, step);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = doTsdbNextDataBlock2(pReader->innerReader[1], hasNext);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbReleaseReader(pReader);
      return code;
    }

    pReader->step = EXTERNAL_ROWS_NEXT;
    if (*hasNext) {
      pStatus = &pReader->innerReader[1]->status;
      if (pStatus->composedDataBlock) {
        qTrace("tsdb/read: %p, unlock read mutex", pReader);
        tsdbReleaseReader(pReader);
      }

      return code;
    }
  }

  qTrace("tsdb/read: %p, unlock read mutex", pReader);
  tsdbReleaseReader(pReader);

  return code;
}

static void doFillNullColSMA(SBlockLoadSuppInfo* pSup, int32_t numOfRows, int32_t numOfCols, SColumnDataAgg* pTsAgg) {
  // do fill all null column value SMA info
  int32_t i = 0, j = 0;
  int32_t size = (int32_t)TARRAY2_SIZE(&pSup->colAggArray);
  int32_t code = TARRAY2_INSERT_PTR(&pSup->colAggArray, 0, pTsAgg);
  if (code != TSDB_CODE_SUCCESS) {
    return;
  }

  size++;

  while (j < numOfCols && i < size) {
    SColumnDataAgg* pAgg = &pSup->colAggArray.data[i];
    if (pAgg->colId == pSup->colId[j]) {
      i += 1;
      j += 1;
    } else if (pAgg->colId < pSup->colId[j]) {
      i += 1;
    } else if (pSup->colId[j] < pAgg->colId) {
      if (pSup->colId[j] != PRIMARYKEY_TIMESTAMP_COL_ID) {
        SColumnDataAgg nullColAgg = {.colId = pSup->colId[j], .numOfNull = numOfRows};
        code = TARRAY2_INSERT_PTR(&pSup->colAggArray, i, &nullColAgg);
        if (code != TSDB_CODE_SUCCESS) {
          return;
        }

        i += 1;
        size++;
      }
      j += 1;
    }
  }

  while (j < numOfCols) {
    if (pSup->colId[j] != PRIMARYKEY_TIMESTAMP_COL_ID) {
      SColumnDataAgg nullColAgg = {.colId = pSup->colId[j], .numOfNull = numOfRows};
      code = TARRAY2_INSERT_PTR(&pSup->colAggArray, i, &nullColAgg);
      if (code != TSDB_CODE_SUCCESS) {
        return;
      }

      i += 1;
    }
    j++;
  }
}

int32_t tsdbRetrieveDatablockSMA2(STsdbReader* pReader, SSDataBlock* pDataBlock, bool* allHave, bool* hasNullSMA) {
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

  SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;
  if (pResBlock->info.id.uid != pFBlock->uid) {
    return TSDB_CODE_SUCCESS;
  }

  //  int64_t st = taosGetTimestampUs();
  TARRAY2_CLEAR(&pSup->colAggArray, 0);

  SBrinRecord pRecord;
  blockInfoToRecord(&pRecord, pFBlock);
  code = tsdbDataFileReadBlockSma(pReader->pFileReader, &pRecord, &pSup->colAggArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbDebug("vgId:%d, failed to load block SMA for uid %" PRIu64 ", code:%s, %s", 0, pFBlock->uid, tstrerror(code),
              pReader->idStr);
    return code;
  }

  if (pSup->colAggArray.size > 0) {
    *allHave = true;
  } else {
    *pBlockSMA = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // always load the first primary timestamp column data
  SColumnDataAgg* pTsAgg = &pSup->tsColAgg;

  pTsAgg->numOfNull = 0;
  pTsAgg->colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  pTsAgg->min = pResBlock->info.window.skey;
  pTsAgg->max = pResBlock->info.window.ekey;

  // update the number of NULL data rows
  size_t numOfCols = pSup->numOfCols;

  if (pResBlock->pBlockAgg == NULL) {
    size_t num = taosArrayGetSize(pResBlock->pDataBlock);
    pResBlock->pBlockAgg = taosMemoryCalloc(num, POINTER_BYTES);
  }

  // do fill all null column value SMA info
  doFillNullColSMA(pSup, pFBlock->numRow, numOfCols, pTsAgg);

  size_t size = pSup->colAggArray.size;

  int32_t i = 0, j = 0;
  while (j < numOfCols && i < size) {
    SColumnDataAgg* pAgg = &pSup->colAggArray.data[i];
    if (pAgg->colId == pSup->colId[j]) {
      pResBlock->pBlockAgg[pSup->slotId[j]] = pAgg;
      i += 1;
      j += 1;
    } else if (pAgg->colId < pSup->colId[j]) {
      i += 1;
    } else if (pSup->colId[j] < pAgg->colId) {
      pResBlock->pBlockAgg[pSup->slotId[j]] = NULL;
      *allHave = false;
      j += 1;
    }
  }

  *pBlockSMA = pResBlock->pBlockAgg;
  pReader->cost.smaDataLoad += 1;

  //  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  pReader->cost.smaLoadTime += 0;  // elapsedTime;

  tsdbDebug("vgId:%d, succeed to load block SMA for uid %" PRIu64 ", %s", 0, pFBlock->uid, pReader->idStr);
  return code;
}

static SSDataBlock* doRetrieveDataBlock(STsdbReader* pReader) {
  SReaderStatus*      pStatus = &pReader->status;
  int32_t             code = TSDB_CODE_SUCCESS;
  SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(&pStatus->blockIter);

  if (pReader->code != TSDB_CODE_SUCCESS) {
    return NULL;
  }

  STableBlockScanInfo* pBlockScanInfo = getTableBlockScanInfo(pStatus->pTableMap, pBlockInfo->uid, pReader->idStr);
  if (pBlockScanInfo == NULL) {
    return NULL;
  }

  code = doLoadFileBlockData(pReader, &pStatus->blockIter, &pStatus->fileBlockData, pBlockScanInfo->uid);
  if (code != TSDB_CODE_SUCCESS) {
    tBlockDataReset(&pStatus->fileBlockData);
    terrno = code;
    return NULL;
  }

  code = copyBlockDataToSDataBlock(pReader);
  if (code != TSDB_CODE_SUCCESS) {
    tBlockDataReset(&pStatus->fileBlockData);
    terrno = code;
    return NULL;
  }

  return pReader->resBlockInfo.pResBlock;
}

SSDataBlock* tsdbRetrieveDataBlock2(STsdbReader* pReader, SArray* pIdList) {
  STsdbReader* pTReader = pReader;
  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    if (pReader->step == EXTERNAL_ROWS_PREV) {
      pTReader = pReader->innerReader[0];
    } else if (pReader->step == EXTERNAL_ROWS_NEXT) {
      pTReader = pReader->innerReader[1];
    }
  }

  SReaderStatus* pStatus = &pTReader->status;
  if (pStatus->composedDataBlock || pReader->info.execMode == READER_EXEC_ROWS) {
    return pTReader->resBlockInfo.pResBlock;
  }

  SSDataBlock* ret = doRetrieveDataBlock(pTReader);

  qTrace("tsdb/read-retrieve: %p, unlock read mutex", pReader);
  tsdbReleaseReader(pReader);

  return ret;
}

int32_t tsdbReaderReset2(STsdbReader* pReader, SQueryTableDataCond* pCond) {
  int32_t code = TSDB_CODE_SUCCESS;

  qTrace("tsdb/reader-reset: %p, take read mutex", pReader);
  tsdbAcquireReader(pReader);

  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbReleaseReader(pReader);
      return code;
    }
  }

  if (isEmptyQueryTimeWindow(&pReader->info.window) || pReader->pReadSnap == NULL) {
    tsdbDebug("tsdb reader reset return %p, %s", pReader->pReadSnap, pReader->idStr);
    tsdbReleaseReader(pReader);
    return TSDB_CODE_SUCCESS;
  }

  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockIter* pBlockIter = &pStatus->blockIter;

  pReader->info.order = pCond->order;
  pReader->type = TIMEWINDOW_RANGE_CONTAINED;
  pReader->info.window = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows);
  pStatus->loadFromFile = true;
  pStatus->pTableIter = NULL;

  // allocate buffer in order to load data blocks from file
  memset(&pReader->suppInfo.tsColAgg, 0, sizeof(SColumnDataAgg));

  pReader->suppInfo.tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tsdbDataFileReaderClose(&pReader->pFileReader);

  int32_t numOfTables = tSimpleHashGetSize(pStatus->pTableMap);

  initFilesetIterator(&pStatus->fileIter, pReader->pReadSnap->pfSetArray, pReader);
  resetDataBlockIterator(pBlockIter, pReader->info.order);
  resetTableListIndex(&pReader->status);

  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);
  int32_t step = asc ? 1 : -1;
  int64_t ts = asc ? pReader->info.window.skey - 1 : pReader->info.window.ekey + 1;
  resetAllDataBlockScanInfo(pStatus->pTableMap, ts, step);

  // no data in files, let's try buffer in memory
  if (pStatus->fileIter.numOfFiles == 0) {
    pStatus->loadFromFile = false;
    resetTableListIndex(pStatus);
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("%p reset reader failed, numOfTables:%d, query range:%" PRId64 " - %" PRId64 " in query %s", pReader,
                numOfTables, pReader->info.window.skey, pReader->info.window.ekey, pReader->idStr);

      tsdbReleaseReader(pReader);
      return code;
    }
  }

  tsdbDebug("%p reset reader, suid:%" PRIu64 ", numOfTables:%d, skey:%" PRId64 ", query range:%" PRId64 " - %" PRId64
            " in query %s",
            pReader, pReader->info.suid, numOfTables, pCond->twindows.skey, pReader->info.window.skey,
            pReader->info.window.ekey, pReader->idStr);

  tsdbReleaseReader(pReader);

  return code;
}

static int32_t getBucketIndex(int32_t startRow, int32_t bucketRange, int32_t numOfRows, int32_t numOfBucket) {
  if (numOfRows < startRow) {
    return 0;
  }
  int32_t bucketIndex = ((numOfRows - startRow) / bucketRange);
  if (bucketIndex == numOfBucket) {
    bucketIndex -= 1;
  }
  return bucketIndex;
}

int32_t tsdbGetFileBlocksDistInfo2(STsdbReader* pReader, STableBlockDistInfo* pTableBlockInfo) {
  int32_t       code = TSDB_CODE_SUCCESS;
  const int32_t numOfBuckets = 20.0;

  pTableBlockInfo->totalSize = 0;
  pTableBlockInfo->totalRows = 0;
  pTableBlockInfo->numOfVgroups = 1;

  // find the start data block in file
  tsdbAcquireReader(pReader);
  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbReleaseReader(pReader);
      return code;
    }
  }

  SMergeTreeConf conf = {
      .pReader = pReader,
      .pSchema = pReader->info.pSchema,
      .pCols = pReader->suppInfo.colId,
      .numOfCols = pReader->suppInfo.numOfCols,
      .suid = pReader->info.suid,
  };

  SReaderStatus* pStatus = &pReader->status;
  if (pStatus->pCurrentFileset != NULL) {
    pTableBlockInfo->numOfSttRows += tsdbGetRowsInSttFiles(pStatus->pCurrentFileset, pStatus->pLDataIterArray,
                                                           pReader->pTsdb, &conf, pReader->idStr);
  }

  STsdbCfg* pc = &pReader->pTsdb->pVnode->config.tsdbCfg;
  pTableBlockInfo->defMinRows = pc->minRows;
  pTableBlockInfo->defMaxRows = pc->maxRows;

  int32_t bucketRange = ceil(((double)(pc->maxRows - pc->minRows)) / numOfBuckets);

  pTableBlockInfo->numOfFiles += 1;

  int32_t numOfTables = (int32_t)tSimpleHashGetSize(pStatus->pTableMap);

  SDataBlockIter* pBlockIter = &pStatus->blockIter;
  pTableBlockInfo->numOfFiles += pStatus->fileIter.numOfFiles;

  if (pBlockIter->numOfBlocks > 0) {
    pTableBlockInfo->numOfBlocks += pBlockIter->numOfBlocks;
  }

  pTableBlockInfo->numOfTables = numOfTables;
  bool hasNext = (pBlockIter->numOfBlocks > 0);

  while (true) {
    if (hasNext) {
      SFileDataBlockInfo* pBlockInfo = getCurrentBlockInfo(pBlockIter);
      int32_t             numOfRows = pBlockInfo->numRow;

      pTableBlockInfo->totalRows += numOfRows;

      if (numOfRows > pTableBlockInfo->maxRows) {
        pTableBlockInfo->maxRows = numOfRows;
      }

      if (numOfRows < pTableBlockInfo->minRows) {
        pTableBlockInfo->minRows = numOfRows;
      }

      pTableBlockInfo->totalSize += pBlockInfo->blockSize;

      int32_t bucketIndex = getBucketIndex(pTableBlockInfo->defMinRows, bucketRange, numOfRows, numOfBuckets);
      pTableBlockInfo->blockRowsHisto[bucketIndex]++;

      hasNext = blockIteratorNext(&pStatus->blockIter, pReader->idStr);
    } else {
      code = initForFirstBlockInFile(pReader, pBlockIter);
      if ((code != TSDB_CODE_SUCCESS) || (pStatus->loadFromFile == false)) {
        break;
      }

      // add the data in stt files of new fileset
      if (pStatus->pCurrentFileset != NULL) {
        pTableBlockInfo->numOfSttRows += tsdbGetRowsInSttFiles(pStatus->pCurrentFileset, pStatus->pLDataIterArray,
                                                               pReader->pTsdb, &conf, pReader->idStr);
      }

      pTableBlockInfo->numOfBlocks += pBlockIter->numOfBlocks;
      hasNext = (pBlockIter->numOfBlocks > 0);
    }
  }

  // record the data in stt files
  tsdbReleaseReader(pReader);
  return code;
}

static void getMemTableTimeRange(STsdbReader* pReader, int64_t* pMaxKey, int64_t* pMinKey) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t rows = 0;

  SReaderStatus* pStatus = &pReader->status;

  int32_t iter = 0;
  int64_t maxKey = INT64_MIN;
  int64_t minKey = INT64_MAX;

  void* pHashIter = tSimpleHashIterate(pStatus->pTableMap, NULL, &iter);
  while (pHashIter != NULL) {
    STableBlockScanInfo* pBlockScanInfo = *(STableBlockScanInfo**)pHashIter;

    STbData* d = NULL;
    if (pReader->pReadSnap->pMem != NULL) {
      d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->info.suid, pBlockScanInfo->uid);
      if (d != NULL) {
        if (d->maxKey > maxKey) {
          maxKey = d->maxKey;
        }
        if (d->minKey < minKey) {
          minKey = d->minKey;
        }
      }
    }

    STbData* di = NULL;
    if (pReader->pReadSnap->pIMem != NULL) {
      di = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pIMem, pReader->info.suid, pBlockScanInfo->uid);
      if (di != NULL) {
        if (di->maxKey > maxKey) {
          maxKey = di->maxKey;
        }
        if (di->minKey < minKey) {
          minKey = di->minKey;
        }
      }
    }

    // current table is exhausted, let's try the next table
    pHashIter = tSimpleHashIterate(pStatus->pTableMap, pHashIter, &iter);
  }

  *pMaxKey = maxKey;
  *pMinKey = minKey;
}

int64_t tsdbGetNumOfRowsInMemTable2(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t rows = 0;

  SReaderStatus* pStatus = &pReader->status;
  tsdbAcquireReader(pReader);
  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbReleaseReader(pReader);
      return code;
    }
  }

  int32_t iter = 0;
  pStatus->pTableIter = tSimpleHashIterate(pStatus->pTableMap, NULL, &iter);

  while (pStatus->pTableIter != NULL) {
    STableBlockScanInfo* pBlockScanInfo = *(STableBlockScanInfo**)pStatus->pTableIter;

    STbData* d = NULL;
    if (pReader->pReadSnap->pMem != NULL) {
      d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->info.suid, pBlockScanInfo->uid);
      if (d != NULL) {
        rows += tsdbGetNRowsInTbData(d);
      }
    }

    STbData* di = NULL;
    if (pReader->pReadSnap->pIMem != NULL) {
      di = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pIMem, pReader->info.suid, pBlockScanInfo->uid);
      if (di != NULL) {
        rows += tsdbGetNRowsInTbData(di);
      }
    }

    // current table is exhausted, let's try the next table
    pStatus->pTableIter = tSimpleHashIterate(pStatus->pTableMap, pStatus->pTableIter, &iter);
  }

  tsdbReleaseReader(pReader);

  return rows;
}

int32_t tsdbGetTableSchema(SMeta* pMeta, int64_t uid, STSchema** pSchema, int64_t* suid) {
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pMeta, 0);
  int32_t code = metaReaderGetTableEntryByUidCache(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
    metaReaderClear(&mr);
    return terrno;
  }

  *suid = 0;

  // only child table and ordinary table is allowed, super table is not allowed.
  if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);
    *suid = mr.me.ctbEntry.suid;
    code = metaReaderGetTableEntryByUidCache(&mr, *suid);
    if (code != TSDB_CODE_SUCCESS) {
      terrno = TSDB_CODE_TDB_INVALID_TABLE_ID;
      metaReaderClear(&mr);
      return terrno;
    }
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {  // do nothing
  } else {
    terrno = TSDB_CODE_INVALID_PARA;
    metaReaderClear(&mr);
    return terrno;
  }

  metaReaderClear(&mr);

  // get the newest table schema version
  code = metaGetTbTSchemaEx(pMeta, *suid, uid, -1, pSchema);
  return code;
}

int32_t tsdbTakeReadSnap2(STsdbReader* pReader, _query_reseek_func_t reseek, STsdbReadSnap** ppSnap) {
  int32_t        code = 0;
  STsdb*         pTsdb = pReader->pTsdb;
  SVersionRange* pRange = &pReader->info.verRange;

  // lock
  taosThreadMutexLock(&pTsdb->mutex);

  // alloc
  STsdbReadSnap* pSnap = (STsdbReadSnap*)taosMemoryCalloc(1, sizeof(STsdbReadSnap));
  if (pSnap == NULL) {
    taosThreadMutexUnlock(&pTsdb->mutex);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  // take snapshot
  if (pTsdb->mem && (pRange->minVer <= pTsdb->mem->maxVer && pRange->maxVer >= pTsdb->mem->minVer)) {
    pSnap->pMem = pTsdb->mem;
    pSnap->pNode = taosMemoryMalloc(sizeof(*pSnap->pNode));
    if (pSnap->pNode == NULL) {
      taosThreadMutexUnlock(&pTsdb->mutex);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    pSnap->pNode->pQHandle = pReader;
    pSnap->pNode->reseek = reseek;

    tsdbRefMemTable(pTsdb->mem, pSnap->pNode);
  }

  if (pTsdb->imem && (pRange->minVer <= pTsdb->imem->maxVer && pRange->maxVer >= pTsdb->imem->minVer)) {
    pSnap->pIMem = pTsdb->imem;
    pSnap->pINode = taosMemoryMalloc(sizeof(*pSnap->pINode));
    if (pSnap->pINode == NULL) {
      taosThreadMutexUnlock(&pTsdb->mutex);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }

    pSnap->pINode->pQHandle = pReader;
    pSnap->pINode->reseek = reseek;

    tsdbRefMemTable(pTsdb->imem, pSnap->pINode);
  }

  // fs
  code = tsdbFSCreateRefSnapshotWithoutLock(pTsdb->pFS, &pSnap->pfSetArray);

  // unlock
  taosThreadMutexUnlock(&pTsdb->mutex);

  if (code == TSDB_CODE_SUCCESS) {
    tsdbTrace("vgId:%d, take read snapshot", TD_VID(pTsdb->pVnode));
  }

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("vgId:%d take read snapshot failed, code:%s", TD_VID(pTsdb->pVnode), tstrerror(code));

    *ppSnap = NULL;
    if (pSnap) {
      if (pSnap->pNode) taosMemoryFree(pSnap->pNode);
      if (pSnap->pINode) taosMemoryFree(pSnap->pINode);
      taosMemoryFree(pSnap);
    }
  } else {
    *ppSnap = pSnap;
  }

  return code;
}

void tsdbUntakeReadSnap2(STsdbReader* pReader, STsdbReadSnap* pSnap, bool proactive) {
  STsdb* pTsdb = pReader->pTsdb;

  if (pSnap) {
    if (pSnap->pMem) {
      tsdbUnrefMemTable(pSnap->pMem, pSnap->pNode, proactive);
    }

    if (pSnap->pIMem) {
      tsdbUnrefMemTable(pSnap->pIMem, pSnap->pINode, proactive);
    }

    if (pSnap->pNode) taosMemoryFree(pSnap->pNode);
    if (pSnap->pINode) taosMemoryFree(pSnap->pINode);

    tsdbFSDestroyRefSnapshot(&pSnap->pfSetArray);

    taosMemoryFree(pSnap);
  }
  tsdbTrace("vgId:%d, untake read snapshot", TD_VID(pTsdb->pVnode));
}

// if failed, do nothing
void tsdbReaderSetId2(STsdbReader* pReader, const char* idstr) {
  taosMemoryFreeClear(pReader->idStr);
  pReader->idStr = taosStrdup(idstr);
  pReader->status.fileIter.pSttBlockReader->mergeTree.idStr = pReader->idStr;
}

void tsdbReaderSetCloseFlag(STsdbReader* pReader) { /*pReader->code = TSDB_CODE_TSC_QUERY_CANCELLED;*/
}

void tsdbSetFilesetDelimited(STsdbReader* pReader) { pReader->bFilesetDelimited = true; }

void tsdbReaderSetNotifyCb(STsdbReader* pReader, TsdReaderNotifyCbFn notifyFn, void* param) {
  pReader->notifyFn = notifyFn;
  pReader->notifyParam = param;
}
