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
#define getCurrentKeyInSttBlock(_r) (&((_r)->currentKey))
#define tColRowGetKeyDeepCopy(_pBlock, _irow, _slotId, _pKey)             \
  do {                                                                    \
    (_pKey)->ts = (_pBlock)->aTSKEY[(_irow)];                             \
    (_pKey)->numOfPKs = 0;                                                \
    if ((_slotId) != -1) {                                                \
      code = tColRowGetPriamyKeyDeepCopy(_pBlock, _irow, _slotId, _pKey); \
      TSDB_CHECK_CODE(code, lino, _end);                                  \
    }                                                                     \
  } while (0)

#define outOfTimeWindow(_ts, _window) (((_ts) > (_window)->ekey) || ((_ts) < (_window)->skey))

typedef struct {
  bool overlapWithNeighborBlock;
  bool hasDupTs;
  bool overlapWithDelInfo;
  bool overlapWithSttBlock;
  bool overlapWithKeyInBuf;
  bool partiallyRequired;
  bool moreThanCapcity;
} SDataBlockToLoadInfo;

static int32_t getCurrentBlockInfo(SDataBlockIter* pBlockIter, SFileDataBlockInfo** pInfo, const char* idStr);
static int32_t buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                         STsdbReader* pReader);
static int32_t getValidMemRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader, TSDBROW** pRes);
static int32_t doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, SRowKey* pKey,
                                       STsdbReader* pReader);
static int32_t doMergeRowsInSttBlock(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo,
                                     SRowMerger* pMerger, int32_t pkSrcSlot, SVersionRange* pVerRange, const char* id);
static int32_t doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, SRowKey* pCurKey, SArray* pDelList,
                                STsdbReader* pReader);
static int32_t doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, SRow* pTSRow,
                                    STableBlockScanInfo* pScanInfo);
static int32_t doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                        int32_t rowIndex);
static void    setComposedBlockFlag(STsdbReader* pReader, bool composed);
static int32_t hasBeenDropped(const SArray* pDelList, int32_t* index, int64_t key, int64_t ver, int32_t order,
                              SVersionRange* pVerRange, bool hasPk, bool* dropped);

static int32_t doMergeMemTableMultiRows(TSDBROW* pRow, SRowKey* pKey, uint64_t uid, SIterInfo* pIter, SArray* pDelList,
                                        TSDBROW* pResRow, STsdbReader* pReader, bool* freeTSRow);
static int32_t doMergeMemIMemRows(TSDBROW* pRow, SRowKey* pRowKey, TSDBROW* piRow, SRowKey* piRowKey,
                                  STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, SRow** pTSRow);
static int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, SRowKey* pKey,
                                     STsdbReader* pReader);
static int32_t mergeRowsInSttBlocks(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo,
                                    STsdbReader* pReader);

static int32_t initDelSkylineIterator(STableBlockScanInfo* pBlockScanInfo, int32_t order, SReadCostSummary* pCost);
static void getTsdbByRetentions(SVnode* pVnode, SQueryTableDataCond* pCond, SRetention* retentions, const char* idstr,
                                int8_t* pLevel, STsdb** pTsdb);
static SVersionRange getQueryVerRange(SVnode* pVnode, SQueryTableDataCond* pCond, int8_t level);
static int32_t       doBuildDataBlock(STsdbReader* pReader);
static int32_t       getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader, TSDBKEY* key);
static bool          hasDataInFileBlock(const SBlockData* pBlockData, const SFileBlockDumpInfo* pDumpInfo);
static bool          hasDataInSttBlock(STableBlockScanInfo* pInfo);
static int32_t       initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter);
static int32_t       getInitialDelIndex(const SArray* pDelSkyline, int32_t order);
static int32_t       resetTableListIndex(SReaderStatus* pStatus, const char* id);
static void          getMemTableTimeRange(STsdbReader* pReader, int64_t* pMaxKey, int64_t* pMinKey);
static void          updateComposedBlockInfo(STsdbReader* pReader, double el, STableBlockScanInfo* pBlockScanInfo);
static int32_t       buildFromPreFilesetBuffer(STsdbReader* pReader);

static void resetPreFilesetMemTableListIndex(SReaderStatus* pStatus);

FORCE_INLINE int32_t pkCompEx(SRowKey* p1, SRowKey* p2) {
  if (p2 == NULL) {
    return 1;
  }

  if (p1 == NULL) {
    return -1;
  }

  if (p1->ts < p2->ts) {
    return -1;
  } else if (p1->ts > p2->ts) {
    return 1;
  }

  if (p1->numOfPKs == 0) {
    return 0;
  } else {
    return tRowKeyCompare(p1, p2);
  }
}

static int32_t tColRowGetPriamyKeyDeepCopy(SBlockData* pBlock, int32_t irow, int32_t slotId, SRowKey* pKey) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  SColData* pColData = NULL;
  SColVal   cv;

  TSDB_CHECK_CONDITION((pBlock != NULL) && (pBlock->aColData != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pColData = &pBlock->aColData[slotId];

  tColDataGetValue(pColData, irow, &cv);

  pKey->numOfPKs = 1;
  pKey->pks[0].type = cv.value.type;

  if (IS_NUMERIC_TYPE(cv.value.type)) {
    pKey->pks[0].val = cv.value.val;
  } else {
    pKey->pks[0].nData = cv.value.nData;
    TAOS_MEMCPY(pKey->pks[0].pData, cv.value.pData, cv.value.nData);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// for test purpose, todo remove it
static int32_t tGetPrimaryKeyIndex(uint8_t* p, SPrimaryKeyIndex* index) {
  int32_t n = 0;
  n += tGetI8(p + n, &index->type);
  n += tGetU32v(p + n, &index->offset);
  return n;
}

static int32_t tRowGetPrimaryKeyDeepCopy(SRow* pRow, SRowKey* pKey) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SPrimaryKeyIndex indices[TD_MAX_PK_COLS];
  uint8_t*         data = NULL;
  int32_t          len = 0;

  TSDB_CHECK_NULL(pRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (pRow->numOfPKs > 0) {
    TSDB_CHECK_NULL(pRow->data, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  data = pRow->data;
  for (int32_t i = 0; i < pRow->numOfPKs; i++) {
    data += tGetPrimaryKeyIndex(data, &indices[i]);
  }

  // primary keys
  for (int32_t i = 0; i < pRow->numOfPKs; i++) {
    pKey->pks[i].type = indices[i].type;

    uint8_t* tdata = data + indices[i].offset;
    if (pRow->flag >> 4) {
      tdata += tGetI16v(tdata, NULL);
    }

    if (IS_VAR_DATA_TYPE(indices[i].type)) {
      tdata += tGetU32v(tdata, &pKey->pks[i].nData);
      TAOS_MEMCPY(pKey->pks[i].pData, tdata, pKey->pks[i].nData);
    } else {
      TAOS_MEMCPY(&pKey->pks[i].val, data + indices[i].offset, tDataTypes[pKey->pks[i].type].bytes);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t setColumnIdSlotList(SBlockLoadSuppInfo* pSupInfo, SColumnInfo* pCols, const int32_t* pSlotIdList,
                                   int32_t numOfCols) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pSupInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (numOfCols > 0) {
    TSDB_CHECK_NULL(pSlotIdList, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(pCols, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  pSupInfo->pk.pk = 0;
  pSupInfo->numOfPks = 0;
  pSupInfo->pkSrcSlot = -1;
  pSupInfo->pkDstSlot = -1;
  pSupInfo->smaValid = true;
  pSupInfo->numOfCols = numOfCols;

  pSupInfo->colId = taosMemoryMalloc(numOfCols * (sizeof(int16_t) * 2 + POINTER_BYTES));
  TSDB_CHECK_NULL(pSupInfo->colId, code, lino, _end, terrno);

  pSupInfo->slotId = (int16_t*)((char*)pSupInfo->colId + (sizeof(int16_t) * numOfCols));
  pSupInfo->buildBuf = (char**)((char*)pSupInfo->slotId + (sizeof(int16_t) * numOfCols));
  for (int32_t i = 0; i < numOfCols; ++i) {
    pSupInfo->colId[i] = pCols[i].colId;
    pSupInfo->slotId[i] = pSlotIdList[i];

    if (pCols[i].pk) {
      pSupInfo->pk = pCols[i];
      pSupInfo->pkSrcSlot = i - 1;
      pSupInfo->pkDstSlot = pSlotIdList[i];
      pSupInfo->numOfPks += 1;
    }

    if (IS_VAR_DATA_TYPE(pCols[i].type)) {
      pSupInfo->buildBuf[i] = taosMemoryMalloc(pCols[i].bytes);
      if (pSupInfo->buildBuf[i] == NULL) {
        tsdbError("failed to prepare memory for set columnId slot list, size:%d, code: %s", pCols[i].bytes,
                  tstrerror(terrno));
      }
      TSDB_CHECK_NULL(pSupInfo->buildBuf[i], code, lino, _end, terrno);
    } else {
      pSupInfo->buildBuf[i] = NULL;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t updateBlockSMAInfo(STSchema* pSchema, SBlockLoadSuppInfo* pSupInfo) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  int32_t   i = 0, j = 0;
  STColumn* pTCol = NULL;

  TSDB_CHECK_NULL(pSchema, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pSupInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  while (i < pSchema->numOfCols && j < pSupInfo->numOfCols) {
    pTCol = &pSchema->columns[i];
    if (pTCol->colId == pSupInfo->colId[j]) {
      if (!IS_BSMA_ON(pTCol) && (PRIMARYKEY_TIMESTAMP_COL_ID != pTCol->colId)) {
        pSupInfo->smaValid = false;
        goto _end;
      }

      i += 1;
      j += 1;
    } else if (pTCol->colId < pSupInfo->colId[j]) {  // do nothing
      i += 1;
    } else {
      code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static bool isEmptyQueryTimeWindow(STimeWindow* pWindow) {
  return (pWindow == NULL) || (pWindow->skey > pWindow->ekey);
}

// Update the query time window according to the data time to live(TTL) information, in order to avoid to return
// the expired data to client, even it is queried already.
static int32_t updateQueryTimeWindow(STsdb* pTsdb, STimeWindow* pWindow, STimeWindow* out) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int64_t earlyTs = 0;

  TSDB_CHECK_NULL(pTsdb, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pWindow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(out, code, lino, _end, TSDB_CODE_INVALID_PARA);

  earlyTs = tsdbGetEarliestTs(pTsdb);
  *out = *pWindow;
  if (out->skey < earlyTs) {
    out->skey = earlyTs;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// init file iterator
static int32_t initFilesetIterator(SFilesetIter* pIter, TFileSetArray* pFileSetArray, STsdbReader* pReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SBlockLoadSuppInfo* pInfo = NULL;
  SSttBlockReader*    pSttReader = NULL;
  size_t              numOfFileset = 0;
  bool                asc = false;

  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pFileSetArray, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pInfo = &pReader->suppInfo;
  numOfFileset = TARRAY2_SIZE(pFileSetArray);
  asc = ASCENDING_TRAVERSE(pReader->info.order);

  pIter->index = asc ? -1 : numOfFileset;
  pIter->order = pReader->info.order;
  pIter->pFilesetList = pFileSetArray;
  pIter->numOfFiles = numOfFileset;

  if (pIter->pSttBlockReader == NULL) {
    pIter->pSttBlockReader = taosMemoryCalloc(1, sizeof(struct SSttBlockReader));
    if (pIter->pSttBlockReader == NULL) {
      tsdbError("failed to prepare the last block iterator, since:%s %s", tstrerror(terrno), pReader->idStr);
    }
    TSDB_CHECK_NULL(pIter->pSttBlockReader, code, lino, _end, terrno);
  }

  pSttReader = pIter->pSttBlockReader;
  pSttReader->order = pReader->info.order;
  pSttReader->window = pReader->info.window;
  pSttReader->verRange = pReader->info.verRange;
  pSttReader->numOfPks = pReader->suppInfo.numOfPks;
  pSttReader->uid = 0;

  tMergeTreeClose(&pSttReader->mergeTree);
  code = initRowKey(&pSttReader->currentKey, INT64_MIN, pInfo->numOfPks, pInfo->pk.type, pInfo->pk.bytes, asc);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("failed init row key, %s", pReader->idStr);
  } else {
    tsdbDebug("init fileset iterator, total files:%d %s", pIter->numOfFiles, pReader->idStr);
  }
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t filesetIteratorNext(SFilesetIter* pIter, STsdbReader* pReader, bool* hasNext) {
  int32_t           code = TSDB_CODE_SUCCESS;
  int32_t           lino = 0;
  bool              asc = false;
  int32_t           step = 0;
  SReadCostSummary* pCost = NULL;
  STFileObj**       pFileObj = NULL;

  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(hasNext, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pIter->order);
  step = asc ? 1 : -1;
  *hasNext = false;

  pIter->index += step;
  if ((asc && pIter->index >= pIter->numOfFiles) || ((!asc) && pIter->index < 0)) {
    *hasNext = false;
    goto _end;
  }

  pCost = &pReader->cost;

  TSDB_CHECK_NULL(pIter->pSttBlockReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  pIter->pSttBlockReader->uid = 0;
  tMergeTreeClose(&pIter->pSttBlockReader->mergeTree);
  destroySttBlockReader(pReader->status.pLDataIterArray, &pCost->sttCost);

  pReader->status.pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
  TSDB_CHECK_NULL(pReader->status.pLDataIterArray, code, lino, _end, terrno);

  // check file the time range of coverage
  STimeWindow win = {0};

  while (1) {
    if (pReader->pFileReader != NULL) {
      tsdbDataFileReaderClose(&pReader->pFileReader);
    }

    TSDB_CHECK_CONDITION(pIter->index < pIter->pFilesetList->size, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
    pReader->status.pCurrentFileset = pIter->pFilesetList->data[pIter->index];

    pFileObj = pReader->status.pCurrentFileset->farr;
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
      TSDB_CHECK_CODE(code, lino, _end);

      pReader->cost.headFileLoad += 1;
    }

    int32_t fid = pReader->status.pCurrentFileset->fid;
    tsdbFidKeyRange(fid, pReader->pTsdb->keepCfg.days, pReader->pTsdb->keepCfg.precision, &win.skey, &win.ekey);

    // current file are no longer overlapped with query time window, ignore remain files
    if ((asc && win.skey > pReader->info.window.ekey) || (!asc && win.ekey < pReader->info.window.skey)) {
      tsdbDebug("%p remain files are not qualified for qrange:%" PRId64 "-%" PRId64 ", ignore, %s", pReader,
                pReader->info.window.skey, pReader->info.window.ekey, pReader->idStr);
      *hasNext = false;
      break;
    }

    if ((asc && (win.ekey < pReader->info.window.skey)) || ((!asc) && (win.skey > pReader->info.window.ekey))) {
      pIter->index += step;
      if ((asc && pIter->index >= pIter->numOfFiles) || ((!asc) && pIter->index < 0)) {
        *hasNext = false;
        break;
      }
      continue;
    }

    tsdbDebug("%p file found fid:%d for qrange:%" PRId64 "-%" PRId64 ", %s", pReader, fid, pReader->info.window.skey,
              pReader->info.window.ekey, pReader->idStr);

    *hasNext = true;
    break;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

bool shouldFreePkBuf(SBlockLoadSuppInfo* pSupp) {
  return (pSupp != NULL) && (pSupp->numOfPks > 0) && IS_VAR_DATA_TYPE(pSupp->pk.type);
}

int32_t resetDataBlockIterator(SDataBlockIter* pIter, int32_t order, bool needFree, const char* id) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pIter->order = order;
  pIter->index = -1;
  pIter->numOfBlocks = 0;

  if (pIter->blockList == NULL) {
    pIter->blockList = taosArrayInit(4, sizeof(SFileDataBlockInfo));
    if (pIter->blockList == NULL) {
      tsdbError("%s failed to reset block iter, func:%s at line:%d code:%s", id, __func__, __LINE__, tstrerror(terrno));
    }
    TSDB_CHECK_NULL(pIter->blockList, code, lino, _end, terrno);
  } else {
    clearDataBlockIterator(pIter, needFree);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void initReaderStatus(SReaderStatus* pStatus) {
  if (pStatus == NULL) {
    return;
  }
  pStatus->pTableIter = NULL;
  pStatus->loadFromFile = true;
}

static int32_t createResBlock(SQueryTableDataCond* pCond, int32_t capacity, SSDataBlock** pResBlock) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;

  TSDB_CHECK_NULL(pCond, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(capacity >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pResBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = createDataBlock(&pBlock);
  TSDB_CHECK_CODE(code, lino, _end);

  for (int32_t i = 0; i < pCond->numOfCols; ++i) {
    SColumnInfoData colInfo = {0};
    colInfo.info = pCond->colList[i];
    code = blockDataAppendColInfo(pBlock, &colInfo);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  code = blockDataEnsureCapacity(pBlock, capacity);
  TSDB_CHECK_CODE(code, lino, _end);

  *pResBlock = pBlock;
  pBlock = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pBlock) {
    taosArrayDestroy(pBlock->pDataBlock);
    taosMemoryFreeClear(pBlock);
  }
  return code;
}

static int32_t tsdbInitReaderLock(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = taosThreadMutexInit(&pReader->readerMutex, NULL);
  tsdbTrace("tsdb/read: %p, post-init read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbUninitReaderLock(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  tsdbTrace("tsdb/read: %p, pre-uninit read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);
  code = taosThreadMutexDestroy(&pReader->readerMutex);
  tsdbTrace("tsdb/read: %p, post-uninit read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbAcquireReader(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_CONDITION((pReader != NULL) && (pReader->idStr != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);

  tsdbTrace("tsdb/read: %s, pre-take read mutex: %p, code: %d", pReader->idStr, &pReader->readerMutex, code);
  code = taosThreadMutexLock(&pReader->readerMutex);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("tsdb/read:%p, failed to lock reader mutex, code:%s", pReader->idStr, tstrerror(code));
  } else {
    tsdbTrace("tsdb/read: %s, post-take read mutex: %p, code: %d", pReader->idStr, &pReader->readerMutex, code);
  }
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbTryAcquireReader(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = taosThreadMutexTryLock(&pReader->readerMutex);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("tsdb/read: %p, post-trytake read mutex: %p, code: %d", pReader, &pReader->readerMutex, code);
  } else {
    tsdbTrace("tsdb/read: %p, post-trytask read mutex: %p", pReader, &pReader->readerMutex);
  }
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbReleaseReader(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = taosThreadMutexUnlock(&pReader->readerMutex);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("tsdb/read: %p post-untake read mutex:%p failed, code:%d", pReader, &pReader->readerMutex, code);
  } else {
    tsdbTrace("tsdb/read: %p, post-untake read mutex: %p", pReader, &pReader->readerMutex);
  }
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void tsdbReleaseDataBlock2(STsdbReader* pReader) {
  if (pReader == NULL) return;

  SReaderStatus* pStatus = &pReader->status;
  if (!pStatus->composedDataBlock) {
    (void) tsdbReleaseReader(pReader);
  }
}

static int32_t initResBlockInfo(SResultBlockInfo* pResBlockInfo, int64_t capacity, SSDataBlock* pResBlock,
                                SQueryTableDataCond* pCond, SBlockLoadSuppInfo* pSup) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* p = NULL;

  TSDB_CHECK_NULL(pResBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION((pResBlockInfo->pResBlock != NULL) || (pSup != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);

  pResBlockInfo->capacity = capacity;
  pResBlockInfo->pResBlock = pResBlock;

  if (pResBlockInfo->pResBlock == NULL) {
    pResBlockInfo->freeBlock = true;
    pResBlockInfo->pResBlock = NULL;

    code = createResBlock(pCond, pResBlockInfo->capacity, &pResBlockInfo->pResBlock);
    TSDB_CHECK_CODE(code, lino, _end);

    if (pSup->numOfPks > 0) {
      p = pResBlockInfo->pResBlock;
      p->info.pks[0].type = pSup->pk.type;
      p->info.pks[1].type = pSup->pk.type;

      if (IS_VAR_DATA_TYPE(pSup->pk.type)) {
        p->info.pks[0].pData = taosMemoryCalloc(1, pSup->pk.bytes);
        TSDB_CHECK_NULL(p->info.pks[0].pData, code, lino, _end, terrno);

        p->info.pks[1].pData = taosMemoryCalloc(1, pSup->pk.bytes);
        TSDB_CHECK_NULL(p->info.pks[0].pData, code, lino, _end, terrno);

        p->info.pks[0].nData = pSup->pk.bytes;
        p->info.pks[1].nData = pSup->pk.bytes;
      }
    }
  } else {
    pResBlockInfo->freeBlock = false;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbReaderCreate(SVnode* pVnode, SQueryTableDataCond* pCond, void** ppReader, int32_t capacity,
                                SSDataBlock* pResBlock, const char* idstr) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  int8_t              level = 0;
  STsdbReader*        pReader = NULL;
  SBlockLoadSuppInfo* pSup = NULL;

  TSDB_CHECK_NULL(pVnode, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pCond, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(ppReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *ppReader = NULL;
  pReader = (STsdbReader*)taosMemoryCalloc(1, sizeof(*pReader));
  TSDB_CHECK_NULL(pReader, code, lino, _end, terrno);

  if (VND_IS_TSMA(pVnode)) {
    tsdbDebug("vgId:%d, tsma is selected to query, %s", TD_VID(pVnode), idstr);
  }

  initReaderStatus(&pReader->status);
  getTsdbByRetentions(pVnode, pCond, pVnode->config.tsdbCfg.retentions, idstr, &level, &pReader->pTsdb);

  pReader->info.suid = pCond->suid;
  pReader->info.order = pCond->order;
  pReader->info.verRange = getQueryVerRange(pVnode, pCond, level);
  code = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows, &pReader->info.window);
  TSDB_CHECK_CODE(code, lino, _end);

  if (idstr == NULL) {
    idstr = "";
  }
  pReader->idStr = taosStrdup(idstr);
  TSDB_CHECK_NULL(pReader->idStr, code, lino, _end, terrno);

  pReader->type = pCond->type;
  pReader->bFilesetDelimited = false;
  pReader->blockInfoBuf.numPerBucket = 1000;  // 1000 tables per bucket

  if (pCond->numOfCols <= 0) {
    tsdbError("vgId:%d, invalid column number %d in query cond, %s", TD_VID(pVnode), pCond->numOfCols, idstr);
    TSDB_CHECK_CONDITION(pCond->numOfCols > 0, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  // allocate buffer in order to load data blocks from file
  pSup = &pReader->suppInfo;
  pSup->tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  code = setColumnIdSlotList(pSup, pCond->colList, pCond->pSlotList, pCond->numOfCols);
  TSDB_CHECK_CODE(code, lino, _end);

  code = initResBlockInfo(&pReader->resBlockInfo, capacity, pResBlock, pCond, pSup);
  TSDB_CHECK_CODE(code, lino, _end);

  code = tBlockDataCreate(&pReader->status.fileBlockData);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pReader->suppInfo.colId[0] != PRIMARYKEY_TIMESTAMP_COL_ID) {
    tsdbError("the first column isn't primary timestamp, %d, %s", pReader->suppInfo.colId[0], pReader->idStr);
    TSDB_CHECK_CONDITION(pReader->suppInfo.colId[0] == PRIMARYKEY_TIMESTAMP_COL_ID, code, lino, _end,
                         TSDB_CODE_INVALID_PARA);
  }

  pReader->status.pPrimaryTsCol = taosArrayGet(pReader->resBlockInfo.pResBlock->pDataBlock, pSup->slotId[0]);
  TSDB_CHECK_NULL(pReader->status.pPrimaryTsCol, code, lino, _end, terrno);

  int32_t type = pReader->status.pPrimaryTsCol->info.type;
  if (type != TSDB_DATA_TYPE_TIMESTAMP) {
    tsdbError("the first column isn't primary timestamp in result block, actual: %s, %s", tDataTypes[type].name,
              pReader->idStr);
    TSDB_CHECK_CONDITION(type == TSDB_DATA_TYPE_TIMESTAMP, code, lino, _end, TSDB_CODE_INVALID_PARA);
  }

  code = tsdbInitReaderLock(pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  code = tsem_init(&pReader->resumeAfterSuspend, 0, 0);
  TSDB_CHECK_CODE(code, lino, _end);

  *ppReader = pReader;
  pReader = NULL;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pReader) {
    tsdbReaderClose2(pReader);
  }
  return code;
}

static int32_t doLoadBlockIndex(STsdbReader* pReader, SDataFileReader* pFileReader, SArray* pIndexList) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  int32_t              st = 0;
  int32_t              et1 = 0;
  int32_t              et2 = 0;
  int32_t              numOfTables = 0;
  const TBrinBlkArray* pBlkArray = NULL;
  STableUidList*       pList = NULL;
  SBrinBlk*            pBrinBlk = NULL;

  if (pFileReader == NULL) {
    goto _end;
  }

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pIndexList, code, lino, _end, TSDB_CODE_INVALID_PARA);

  st = taosGetTimestampUs();
  numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);

  code = tsdbDataFileReadBrinBlk(pFileReader, &pBlkArray);
  TSDB_CHECK_CODE(code, lino, _end);

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
  et1 = taosGetTimestampUs();

  pList = &pReader->status.uidList;

  int32_t i = 0;
  int32_t j = 0;
  while (i < TARRAY2_SIZE(pBlkArray)) {
    pBrinBlk = &pBlkArray->data[i];
    if (pBrinBlk->maxTbid.suid < pReader->info.suid) {
      i += 1;
      continue;
    }

    if (pBrinBlk->minTbid.suid > pReader->info.suid) {  // not include the queried table/super table, quit the loop
      break;
    }

    TSDB_CHECK_CONDITION(
        (pBrinBlk->minTbid.suid <= pReader->info.suid) && (pBrinBlk->maxTbid.suid >= pReader->info.suid), code, lino,
        _end, TSDB_CODE_INTERNAL_ERROR);

    if (pBrinBlk->maxTbid.suid == pReader->info.suid && pBrinBlk->maxTbid.uid < pList->tableUidList[j]) {
      i += 1;
      continue;
    }

    if (pBrinBlk->minTbid.suid == pReader->info.suid && pBrinBlk->minTbid.uid > pList->tableUidList[numOfTables - 1]) {
      break;
    }

    const void* p1 = taosArrayPush(pIndexList, pBrinBlk);
    TSDB_CHECK_NULL(p1, code, lino, _end, terrno);

    i += 1;
    if (pBrinBlk->maxTbid.suid == pReader->info.suid) {
      while (j < numOfTables && pList->tableUidList[j] < pBrinBlk->maxTbid.uid) {
        j++;
      }
      if (j >= numOfTables) {
        break;
      }
    }
  }

  et2 = taosGetTimestampUs();
  tsdbDebug("load block index for %d/%d tables completed, elapsed time:%.2f ms, set BrinBlk:%.2f ms, size:%.2f Kb %s",
            numOfTables, (int32_t)pBlkArray->size, (et1 - st) / 1000.0, (et2 - et1) / 1000.0,
            pBlkArray->size * sizeof(SBrinBlk) / 1024.0, pReader->idStr);

  pReader->cost.headFileLoadTime += (et1 - st) / 1000.0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t loadFileBlockBrinInfo(STsdbReader* pReader, SArray* pIndexList, SBlockNumber* pBlockNum,
                                     SArray* pTableScanInfoList) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  int64_t         st = 0;
  bool            asc = false;
  STimeWindow     w = {0};
  SBrinRecordIter iter = {0};
  int32_t         numOfTables = 0;
  SBrinRecord*    pRecord = NULL;
  int32_t         k = 0;
  size_t          sizeInDisk = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockNum, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTableScanInfoList, code, lino, _end, TSDB_CODE_INVALID_PARA);

  st = taosGetTimestampUs();
  asc = ASCENDING_TRAVERSE(pReader->info.order);
  w = pReader->info.window;
  numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);

  // clear info for the new file
  cleanupInfoForNextFileset(pReader->status.pTableMap);
  initBrinRecordIter(&iter, pReader->pFileReader, pIndexList);

  while (1) {
    code = getNextBrinRecord(&iter, &pRecord);
    TSDB_CHECK_CODE(code, lino, _end);

    if (pRecord == NULL) {
      break;
    }

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

    TSDB_CHECK_CONDITION((pRecord->suid == pReader->info.suid) && (uid == pRecord->uid), code, lino, _end,
                         TSDB_CODE_INTERNAL_ERROR);

    STableBlockScanInfo* pScanInfo = NULL;
    code = getTableBlockScanInfo(pReader->status.pTableMap, uid, &pScanInfo, pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);

    // here we should find the first timestamp that is greater than the lastProcKey
    // the window is an open interval NOW.
    if (asc) {
      w.skey = pScanInfo->lastProcKey.ts;
    } else {
      w.ekey = pScanInfo->lastProcKey.ts;
    }

    // NOTE: specialized for open interval
    if (((w.skey < INT64_MAX) && ((w.skey + 1) > w.ekey)) || (w.skey == INT64_MAX)) {
      k += 1;
      if (k >= numOfTables) {
        break;
      } else {
        continue;
      }
    }

    // 1. time range check
    if (pRecord->firstKey.key.ts > w.ekey || pRecord->lastKey.key.ts < w.skey) {
      continue;
    }

    if (asc) {
      if (pkCompEx(&pRecord->lastKey.key, &pScanInfo->lastProcKey) <= 0) {
        continue;
      }
    } else {
      if (pkCompEx(&pRecord->firstKey.key, &pScanInfo->lastProcKey) >= 0) {
        continue;
      }
    }

    // 2. version range check, version range is an CLOSED interval
    if (pRecord->minVer > pReader->info.verRange.maxVer || pRecord->maxVer < pReader->info.verRange.minVer) {
      continue;
    }

    if (pScanInfo->pBlockList == NULL) {
      pScanInfo->pBlockList = taosArrayInit(4, sizeof(SFileDataBlockInfo));
      TSDB_CHECK_NULL(pScanInfo->pBlockList, code, lino, _end, terrno);
    }

    if (pScanInfo->pBlockIdxList == NULL) {
      pScanInfo->pBlockIdxList = taosArrayInit(4, sizeof(STableDataBlockIdx));
      TSDB_CHECK_NULL(pScanInfo->pBlockIdxList, code, lino, _end, terrno);
    }

    SFileDataBlockInfo blockInfo = {.tbBlockIdx = TARRAY_SIZE(pScanInfo->pBlockList)};
    code = recordToBlockInfo(&blockInfo, pRecord);
    TSDB_CHECK_CODE(code, lino, _end);
    sizeInDisk += blockInfo.blockSize;

    const void* p1 = taosArrayPush(pScanInfo->pBlockList, &blockInfo);
    TSDB_CHECK_NULL(p1, code, lino, _end, terrno);

    // todo: refactor to record the fileset skey/ekey
    if (pScanInfo->filesetWindow.skey > pRecord->firstKey.key.ts) {
      pScanInfo->filesetWindow.skey = pRecord->firstKey.key.ts;
    }

    if (pScanInfo->filesetWindow.ekey < pRecord->lastKey.key.ts) {
      pScanInfo->filesetWindow.ekey = pRecord->lastKey.key.ts;
    }

    pBlockNum->numOfBlocks += 1;
    if (taosArrayGetSize(pTableScanInfoList) == 0) {
      p1 = taosArrayPush(pTableScanInfoList, &pScanInfo);
      TSDB_CHECK_NULL(p1, code, lino, _end, terrno);
    } else {
      STableBlockScanInfo** p = taosArrayGetLast(pTableScanInfoList);
      TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);

      if ((*p)->uid != uid) {
        p1 = taosArrayPush(pTableScanInfoList, &pScanInfo);
        TSDB_CHECK_NULL(p1, code, lino, _end, terrno);
      }
    }
  }

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  clearBrinBlockIter(&iter);
  return code;
}

static void setBlockAllDumped(SFileBlockDumpInfo* pDumpInfo, int64_t maxKey, int32_t order) {
  if (pDumpInfo != NULL) {
    pDumpInfo->allDumped = true;
  }
}

static int32_t updateLastKeyInfo(SRowKey* pKey, SFileDataBlockInfo* pBlockInfo, SDataBlockInfo* pInfo, int32_t numOfPks,
                                 bool asc) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pKey->ts = asc ? pInfo->window.ekey : pInfo->window.skey;
  pKey->numOfPKs = numOfPks;
  if (pKey->numOfPKs <= 0) {
    goto _end;
  }

  TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (IS_NUMERIC_TYPE(pKey->pks[0].type)) {
    pKey->pks[0].val = asc ? pBlockInfo->lastPk.val : pBlockInfo->firstPk.val;
  } else {
    uint8_t* p = asc ? pBlockInfo->lastPk.pData : pBlockInfo->firstPk.pData;
    pKey->pks[0].nData = asc ? varDataLen(pBlockInfo->lastPk.pData) : varDataLen(pBlockInfo->firstPk.pData);
    TAOS_MEMCPY(pKey->pks[0].pData, p, pKey->pks[0].nData);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doCopyColVal(SColumnInfoData* pColInfoData, int32_t rowIndex, int32_t colIndex, SColVal* pColVal,
                            SBlockLoadSuppInfo* pSup) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pColVal, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (IS_VAR_DATA_TYPE(pColVal->value.type)) {
    if (!COL_VAL_IS_VALUE(pColVal)) {
      colDataSetNULL(pColInfoData, rowIndex);
    } else {
      TSDB_CHECK_NULL(pSup, code, lino, _end, TSDB_CODE_INVALID_PARA);
      varDataSetLen(pSup->buildBuf[colIndex], pColVal->value.nData);
      if ((pColVal->value.nData + VARSTR_HEADER_SIZE) > pColInfoData->info.bytes) {
        tsdbWarn("column cid:%d actual data len %d is bigger than schema len %d", pColVal->cid, pColVal->value.nData,
                 pColInfoData->info.bytes);
        code = TSDB_CODE_TDB_INVALID_TABLE_SCHEMA_VER;
        TSDB_CHECK_CODE(code, lino, _end);
      }

      if (pColVal->value.nData > 0) {  // pData may be null, if nData is 0
        (void)memcpy(varDataVal(pSup->buildBuf[colIndex]), pColVal->value.pData, pColVal->value.nData);
      }

      code = colDataSetVal(pColInfoData, rowIndex, pSup->buildBuf[colIndex], false);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  } else {
    code = colDataSetVal(pColInfoData, rowIndex, (const char*)&pColVal->value.val, !COL_VAL_IS_VALUE(pColVal));
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t getCurrentBlockInfo(SDataBlockIter* pBlockIter, SFileDataBlockInfo** pInfo, const char* id) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pBlockIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockIter->blockList, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pInfo = NULL;

  size_t num = TARRAY_SIZE(pBlockIter->blockList);
  TSDB_CHECK_CONDITION(num != 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pInfo = taosArrayGet(pBlockIter->blockList, pBlockIter->index);
  TSDB_CHECK_NULL(*pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doBinarySearchKey(const TSKEY* keyList, int num, int pos, TSKEY key, int order) {
  // start end position
  int s, e;
  s = pos;

  // check
  if (!(keyList != NULL && pos >= 0 && pos < num && num > 0)) {
    return -1;
  }
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

// handle the repeat ts cases.
static int32_t findFirstPos(const int64_t* pTsList, int32_t num, int32_t startPos, bool asc) {
  int32_t i = startPos;
  int64_t startTs = pTsList[startPos];
  if (asc) {
    while (i < num && (pTsList[i] == startTs)) {
      i++;
    }
    return i - 1;
  } else {
    while (i >= 0 && (pTsList[i] == startTs)) {
      i--;
    }
    return i + 1;
  }
}

static int32_t getEndPosInDataBlock(STsdbReader* pReader, SBlockData* pBlockData, SBrinRecord* pRecord, int32_t pos) {
  // NOTE: reverse the order to find the end position in data block
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t endPos = -1;
  bool    asc = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRecord, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pReader->info.order);

  if (asc && pReader->info.window.ekey >= pRecord->lastKey.key.ts) {
    endPos = pRecord->numRow - 1;
  } else if (!asc && pReader->info.window.skey <= pRecord->firstKey.key.ts) {
    endPos = 0;
  } else {
    int64_t key = asc ? pReader->info.window.ekey : pReader->info.window.skey;
    endPos = doBinarySearchKey(pBlockData->aTSKEY, pRecord->numRow, pos, key, pReader->info.order);
    if (endPos == -1) {
      goto _end;
    }

    endPos = findFirstPos(pBlockData->aTSKEY, pRecord->numRow, endPos, asc);
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return endPos;
}

static int32_t copyPrimaryTsCol(const SBlockData* pBlockData, SFileBlockDumpInfo* pDumpInfo, SColumnInfoData* pColData,
                                int32_t dumpedRows, bool asc) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_CONDITION((pBlockData != NULL) && (pBlockData->aTSKEY != NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pDumpInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(dumpedRows >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (asc) {
    TAOS_MEMCPY(pColData->pData, &pBlockData->aTSKEY[pDumpInfo->rowIndex], dumpedRows * sizeof(int64_t));
  } else {
    int32_t startIndex = pDumpInfo->rowIndex - dumpedRows + 1;
    TAOS_MEMCPY(pColData->pData, &pBlockData->aTSKEY[startIndex], dumpedRows * sizeof(int64_t));

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// a faster version of copy procedure.
static int32_t copyNumericCols(const SColData* pData, SFileBlockDumpInfo* pDumpInfo, SColumnInfoData* pColData,
                               int32_t dumpedRows, bool asc) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  uint8_t* p = NULL;
  int32_t  step = asc ? 1 : -1;

  TSDB_CHECK_NULL(pData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pDumpInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION(dumpedRows >= 0, code, lino, _end, TSDB_CODE_INVALID_PARA);

  TSDB_CHECK_CONDITION(pData->type < TSDB_DATA_TYPE_MAX, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if (asc) {
    p = pData->pData + tDataTypes[pData->type].bytes * pDumpInfo->rowIndex;
  } else {
    int32_t startIndex = pDumpInfo->rowIndex - dumpedRows + 1;
    p = pData->pData + tDataTypes[pData->type].bytes * startIndex;
  }

  // make sure it is aligned to 8bit, the allocated memory address is aligned to 256bit

  // 1. copy data in a batch model
  TAOS_MEMCPY(pColData->pData, p, dumpedRows * tDataTypes[pData->type].bytes);

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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void blockInfoToRecord(SBrinRecord* record, SFileDataBlockInfo* pBlockInfo, SBlockLoadSuppInfo* pSupp) {
  record->uid = pBlockInfo->uid;
  record->firstKey = (STsdbRowKey){.key = {.ts = pBlockInfo->firstKey, .numOfPKs = pSupp->numOfPks}};
  record->lastKey = (STsdbRowKey){.key = {.ts = pBlockInfo->lastKey, .numOfPKs = pSupp->numOfPks}};

  if (pSupp->numOfPks > 0) {
    SValue* pFirst = &record->firstKey.key.pks[0];
    SValue* pLast = &record->lastKey.key.pks[0];

    pFirst->type = pSupp->pk.type;
    pLast->type = pSupp->pk.type;

    if (IS_VAR_DATA_TYPE(pFirst->type)) {
      pFirst->pData = (uint8_t*)varDataVal(pBlockInfo->firstPk.pData);
      pFirst->nData = varDataLen(pBlockInfo->firstPk.pData);

      pLast->pData = (uint8_t*)varDataVal(pBlockInfo->lastPk.pData);
      pLast->nData = varDataLen(pBlockInfo->lastPk.pData);
    } else {
      pFirst->val = pBlockInfo->firstPk.val;
      pLast->val = pBlockInfo->lastPk.val;
    }
  }

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

static int32_t copyBlockDataToSDataBlock(STsdbReader* pReader, SRowKey* pLastProcKey) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SReaderStatus*      pStatus = NULL;
  SDataBlockIter*     pBlockIter = NULL;
  SBlockLoadSuppInfo* pSupInfo = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SBlockData*         pBlockData = NULL;
  SFileDataBlockInfo* pBlockInfo = NULL;
  SSDataBlock*        pResBlock = NULL;
  int32_t             numOfOutputCols = 0;
  int64_t             st = 0;
  bool                asc = false;
  int32_t             step = 0;
  SColVal             cv = {0};
  SBrinRecord         tmp;
  SBrinRecord*        pRecord = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlockIter = &pStatus->blockIter;
  pSupInfo = &pReader->suppInfo;
  pDumpInfo = &pReader->status.fBlockDumpInfo;

  pBlockData = &pStatus->fileBlockData;
  pResBlock = pReader->resBlockInfo.pResBlock;
  numOfOutputCols = pSupInfo->numOfCols;
  st = taosGetTimestampUs();
  asc = ASCENDING_TRAVERSE(pReader->info.order);
  step = asc ? 1 : -1;

  code = getCurrentBlockInfo(pBlockIter, &pBlockInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  blockInfoToRecord(&tmp, pBlockInfo, pSupInfo);
  pRecord = &tmp;

  // no data exists, return directly.
  if (pBlockData->nRow == 0 || pBlockData->aTSKEY == 0) {
    tsdbWarn("%p no need to copy since no data in blockData, table uid:%" PRIu64 " has been dropped, %s", pReader,
             pBlockInfo->uid, pReader->idStr);
    pResBlock->info.rows = 0;
    goto _end;
  }

  TSDB_CHECK_CONDITION((pDumpInfo->rowIndex >= 0) && (pDumpInfo->rowIndex < pRecord->numRow), code, lino, _end,
                       TSDB_CODE_INVALID_PARA);

  // row index of dump info remain the initial position, let's find the appropriate start position.
  if (((pDumpInfo->rowIndex == 0) && asc) || ((pDumpInfo->rowIndex == (pRecord->numRow - 1)) && (!asc))) {
    if (asc && pReader->info.window.skey <= pRecord->firstKey.key.ts &&
        pReader->info.verRange.minVer <= pRecord->minVer) {
      // pDumpInfo->rowIndex = 0;
    } else if (!asc && pReader->info.window.ekey >= pRecord->lastKey.key.ts &&
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
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pRecord->firstKey.key.ts, pRecord->lastKey.key.ts,
            pRecord->minVer, pRecord->maxVer, pReader->idStr);
        code = TSDB_CODE_INVALID_PARA;
        TSDB_CHECK_CODE(code, lino, _end);
      }

      pDumpInfo->rowIndex = findFirstPos(pBlockData->aTSKEY, pRecord->numRow, pDumpInfo->rowIndex, (!asc));

      TSDB_CHECK_CONDITION(
          (pReader->info.verRange.minVer <= pRecord->maxVer && pReader->info.verRange.maxVer >= pRecord->minVer), code,
          lino, _end, TSDB_CODE_INVALID_PARA);

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
    goto _end;
  }

  endIndex += step;
  int32_t dumpedRows = asc ? (endIndex - pDumpInfo->rowIndex) : (pDumpInfo->rowIndex - endIndex);
  if (dumpedRows > pReader->resBlockInfo.capacity) {  // output buffer check
    dumpedRows = pReader->resBlockInfo.capacity;
  } else if (dumpedRows <= 0) {  // no qualified rows in current data block, quit directly.
    setBlockAllDumped(pDumpInfo, pReader->info.window.ekey, pReader->info.order);
    goto _end;
  }

  int32_t i = 0;
  int32_t rowIndex = 0;

  SColumnInfoData* pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
  TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (pSupInfo->colId[i] == PRIMARYKEY_TIMESTAMP_COL_ID) {
    code = copyPrimaryTsCol(pBlockData, pDumpInfo, pColData, dumpedRows, asc);
    TSDB_CHECK_CODE(code, lino, _end);
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
      TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);

      if (pData->flag == HAS_NONE || pData->flag == HAS_NULL || pData->flag == (HAS_NULL | HAS_NONE)) {
        colDataSetNNULL(pColData, 0, dumpedRows);
      } else {
        if (IS_MATHABLE_TYPE(pColData->info.type)) {
          code = copyNumericCols(pData, pDumpInfo, pColData, dumpedRows, asc);
          TSDB_CHECK_CODE(code, lino, _end);
        } else {  // varchar/nchar type
          for (int32_t j = pDumpInfo->rowIndex; rowIndex < dumpedRows; j += step) {
            tColDataGetValue(pData, j, &cv);
            code = doCopyColVal(pColData, rowIndex++, i, &cv, pSupInfo);
            TSDB_CHECK_CODE(code, lino, _end);
          }
        }
      }

      colIndex += 1;
      i += 1;
    } else {  // the specified column does not exist in file block, fill with null data
      pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
      TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);

      colDataSetNNULL(pColData, 0, dumpedRows);
      i += 1;
    }
  }

  // fill the mis-matched columns with null value
  while (i < numOfOutputCols) {
    pColData = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);

    colDataSetNNULL(pColData, 0, dumpedRows);
    i += 1;
  }

  pResBlock->info.dataLoad = 1;
  pResBlock->info.rows = dumpedRows;
  pDumpInfo->rowIndex += step * dumpedRows;

  tColRowGetKeyDeepCopy(pBlockData, pDumpInfo->rowIndex - step, pSupInfo->pkSrcSlot, pLastProcKey);

  // check if current block are all handled
  if (pDumpInfo->rowIndex >= 0 && pDumpInfo->rowIndex < pRecord->numRow) {
    int64_t ts = pBlockData->aTSKEY[pDumpInfo->rowIndex];

    // the remain data has out of query time window, ignore current block
    if (outOfTimeWindow(ts, &pReader->info.window)) {
      setBlockAllDumped(pDumpInfo, ts, pReader->info.order);
    }
  } else {
    int64_t ts = asc ? pRecord->lastKey.key.ts : pRecord->firstKey.key.ts;
    setBlockAllDumped(pDumpInfo, ts, pReader->info.order);
  }

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  pReader->cost.blockLoadTime += elapsedTime;

  int32_t unDumpedRows = asc ? pRecord->numRow - pDumpInfo->rowIndex : pDumpInfo->rowIndex + 1;
  tsdbDebug("%p copy file block to sdatablock, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, remain:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", uid:%" PRIu64 " elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pRecord->firstKey.key.ts, pRecord->lastKey.key.ts,
            dumpedRows, unDumpedRows, pRecord->minVer, pRecord->maxVer, pBlockInfo->uid, elapsedTime, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static FORCE_INLINE STSchema* getTableSchemaImpl(STsdbReader* pReader, uint64_t uid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_CONDITION((pReader != NULL) && (pReader->info.pSchema == NULL), code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, uid, -1, &pReader->info.pSchema);
  if (code != TSDB_CODE_SUCCESS || pReader->info.pSchema == NULL) {
    terrno = code;
    tsdbError("failed to get table schema, uid:%" PRIu64 ", it may have been dropped, ver:-1, %s", uid, pReader->idStr);
  }
  TSDB_CHECK_CODE(code, lino, _end);
  TSDB_CHECK_NULL(pReader->info.pSchema, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

  code = tsdbRowMergerInit(&pReader->status.merger, pReader->info.pSchema);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    terrno = code;
    return NULL;
  }
  return pReader->info.pSchema;
}

static int32_t doLoadFileBlockData(STsdbReader* pReader, SDataBlockIter* pBlockIter, SBlockData* pBlockData,
                                   uint64_t uid) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  STSchema*           pSchema = NULL;
  SFileDataBlockInfo* pBlockInfo = NULL;
  SBlockLoadSuppInfo* pSup = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  int64_t             st = 0;
  SBrinRecord         tmp;
  SBrinRecord*        pRecord = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pSchema = pReader->info.pSchema;
  st = taosGetTimestampUs();
  pSup = &pReader->suppInfo;

  tBlockDataReset(pBlockData);

  if (pReader->info.pSchema == NULL) {
    pSchema = getTableSchemaImpl(pReader, uid);
    if (pSchema == NULL) {
      tsdbError("%p table uid:%" PRIu64 " failed to get tableschema, code:%s, %s", pReader, uid, tstrerror(code),
                pReader->idStr);
      TSDB_CHECK_NULL(pSchema, code, lino, _end, terrno);
    }
  }

  code = getCurrentBlockInfo(pBlockIter, &pBlockInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  pDumpInfo = &pReader->status.fBlockDumpInfo;

  blockInfoToRecord(&tmp, pBlockInfo, pSup);
  pRecord = &tmp;
  code = tsdbDataFileReadBlockDataByColumn(pReader->pFileReader, pRecord, pBlockData, pSchema, &pSup->colId[1],
                                           pSup->numOfCols - 1);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%p error occurs in loading file block, global index:%d, table index:%d, brange:%" PRId64 "-%" PRId64
              ", rows:%d, code:%s %s",
              pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pBlockInfo->firstKey, pBlockInfo->lastKey,
              pBlockInfo->numRow, tstrerror(code), pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;

  tsdbDebug("%p load file block into buffer, global index:%d, index in table block list:%d, brange:%" PRId64 "-%" PRId64
            ", rows:%d, minVer:%" PRId64 ", maxVer:%" PRId64 ", elapsed time:%.2f ms, %s",
            pReader, pBlockIter->index, pBlockInfo->tbBlockIdx, pRecord->firstKey.key.ts, pRecord->lastKey.key.ts,
            pRecord->numRow, pRecord->minVer, pRecord->maxVer, elapsedTime, pReader->idStr);

  pReader->cost.blockLoadTime += elapsedTime;
  pDumpInfo->allDumped = false;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

static int32_t getNeighborBlockOfTable(SDataBlockIter* pBlockIter, SFileDataBlockInfo* pBlockInfo,
                                       STableBlockScanInfo* pScanInfo, int32_t* nextIndex, int32_t order,
                                       SBrinRecord* pRecord, SBlockLoadSuppInfo* pSupInfo, bool* res) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  bool                asc = false;
  int32_t             step = 0;
  STableDataBlockIdx* pTableDataBlockIdx = NULL;
  SFileDataBlockInfo* p = NULL;

  TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(res, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *res = false;
  asc = ASCENDING_TRAVERSE(order);
  step = asc ? 1 : -1;

  if (asc && pBlockInfo->tbBlockIdx >= taosArrayGetSize(pScanInfo->pBlockIdxList) - 1) {
    *res = false;
  } else if (!asc && pBlockInfo->tbBlockIdx == 0) {
    *res = false;
  } else {
    TSDB_CHECK_NULL(pBlockIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(nextIndex, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(pRecord, code, lino, _end, TSDB_CODE_INVALID_PARA);
    TSDB_CHECK_NULL(pSupInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

    pTableDataBlockIdx = taosArrayGet(pScanInfo->pBlockIdxList, pBlockInfo->tbBlockIdx + step);
    TSDB_CHECK_NULL(pTableDataBlockIdx, code, lino, _end, TSDB_CODE_INVALID_PARA);

    p = taosArrayGet(pBlockIter->blockList, pTableDataBlockIdx->globalIndex);
    TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);

    blockInfoToRecord(pRecord, p, pSupInfo);

    *nextIndex = pBlockInfo->tbBlockIdx + step;
    *res = true;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t setFileBlockActiveInBlockIter(STsdbReader* pReader, SDataBlockIter* pBlockIter, int32_t index,
                                             int32_t step) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  const void*          p = NULL;
  SFileDataBlockInfo   fblock;
  SFileDataBlockInfo*  pBlockInfo = NULL;
  STableBlockScanInfo* pBlockScanInfo = NULL;
  STableDataBlockIdx*  pTableDataBlockIdx = NULL;

  TSDB_CHECK_NULL(pBlockIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_CONDITION((index >= 0) && (index < pBlockIter->numOfBlocks), code, lino, _end, TSDB_CODE_INVALID_PARA);

  p = taosArrayGet(pBlockIter->blockList, index);
  TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);

  fblock = *(SFileDataBlockInfo*)p;
  pBlockIter->index += step;

  if (index != pBlockIter->index) {
    if (index > pBlockIter->index) {
      for (int32_t i = index - 1; i >= pBlockIter->index; --i) {
        pBlockInfo = taosArrayGet(pBlockIter->blockList, i);
        TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

        code = getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, &pBlockScanInfo, pReader->idStr);
        TSDB_CHECK_CODE(code, lino, _end);

        pTableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, pBlockInfo->tbBlockIdx);
        TSDB_CHECK_NULL(pTableDataBlockIdx, code, lino, _end, TSDB_CODE_INVALID_PARA);

        pTableDataBlockIdx->globalIndex = i + 1;

        taosArraySet(pBlockIter->blockList, i + 1, pBlockInfo);
      }
    } else if (index < pBlockIter->index) {
      for (int32_t i = index + 1; i <= pBlockIter->index; ++i) {
        pBlockInfo = taosArrayGet(pBlockIter->blockList, i);
        TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

        code = getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, &pBlockScanInfo, pReader->idStr);
        TSDB_CHECK_CODE(code, lino, _end);

        pTableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, pBlockInfo->tbBlockIdx);
        TSDB_CHECK_NULL(pTableDataBlockIdx, code, lino, _end, TSDB_CODE_INVALID_PARA);

        pTableDataBlockIdx->globalIndex = i - 1;
        taosArraySet(pBlockIter->blockList, i - 1, pBlockInfo);
      }
    }

    taosArraySet(pBlockIter->blockList, pBlockIter->index, &fblock);
    pBlockScanInfo = NULL;
    code = getTableBlockScanInfo(pReader->status.pTableMap, fblock.uid, &pBlockScanInfo, pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);

    pTableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, fblock.tbBlockIdx);
    TSDB_CHECK_NULL(pTableDataBlockIdx, code, lino, _end, TSDB_CODE_INVALID_PARA);

    pTableDataBlockIdx->globalIndex = pBlockIter->index;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// todo: this attribute could be acquired during extractin the global ordered block list.
static bool overlapWithNeighborBlock2(SFileDataBlockInfo* pBlock, SBrinRecord* pRec, int32_t order, int32_t pkType,
                                      int32_t numOfPk) {
  // it is the last block in current file, no chance to overlap with neighbor blocks.
  if (ASCENDING_TRAVERSE(order)) {
    if (pBlock->lastKey == pRec->firstKey.key.ts) {
      if (numOfPk > 0) {
        SValue v1 = {.type = pkType};
        if (IS_VAR_DATA_TYPE(pkType)) {
          v1.pData = (uint8_t*)varDataVal(pBlock->lastPk.pData), v1.nData = varDataLen(pBlock->lastPk.pData);
        } else {
          v1.val = pBlock->lastPk.val;
        }
        return (tValueCompare(&v1, &pRec->firstKey.key.pks[0]) == 0);
      } else {  // no pk
        return true;
      }
    } else {
      return false;
    }
  } else {
    if (pBlock->firstKey == pRec->lastKey.key.ts) {
      if (numOfPk > 0) {
        SValue v1 = {.type = pkType};
        if (IS_VAR_DATA_TYPE(pkType)) {
          v1.pData = (uint8_t*)varDataVal(pBlock->firstPk.pData), v1.nData = varDataLen(pBlock->firstPk.pData);
        } else {
          v1.val = pBlock->firstPk.val;
        }
        return (tValueCompare(&v1, &pRec->lastKey.key.pks[0]) == 0);
      } else {  // no pk
        return true;
      }
    } else {
      return false;
    }
  }
}

static int64_t getBoarderKeyInFiles(SFileDataBlockInfo* pBlock, STableBlockScanInfo* pScanInfo, int32_t order) {
  bool ascScan = ASCENDING_TRAVERSE(order);

  int64_t key = 0;
  if (pScanInfo->sttKeyInfo.status == STT_FILE_HAS_DATA) {
    int64_t keyInStt = pScanInfo->sttKeyInfo.nextProcKey.ts;
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
  return (key.ts >= pBlock->firstKey && key.ts <= pBlock->lastKey) && (pBlock->maxVer >= pVerRange->minVer) &&
         (pBlock->minVer <= pVerRange->maxVer);
}

static int32_t getBlockToLoadInfo(SDataBlockToLoadInfo* pInfo, SFileDataBlockInfo* pBlockInfo,
                                  STableBlockScanInfo* pScanInfo, TSDBKEY keyInBuf, STsdbReader* pReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SBrinRecord         rec = {0};
  int32_t             neighborIndex = 0;
  int32_t             order = 0;
  SBlockLoadSuppInfo* pSupInfo = NULL;
  SBrinRecord         pRecord;
  bool                hasNeighbor = false;

  TSDB_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  order = pReader->info.order;
  pSupInfo = &pReader->suppInfo;

  code = getNeighborBlockOfTable(&pReader->status.blockIter, pBlockInfo, pScanInfo, &neighborIndex, order, &rec,
                                 pSupInfo, &hasNeighbor);
  TSDB_CHECK_CODE(code, lino, _end);

  // overlap with neighbor
  if (hasNeighbor) {
    pInfo->overlapWithNeighborBlock =
        overlapWithNeighborBlock2(pBlockInfo, &rec, order, pSupInfo->pk.type, pSupInfo->numOfPks);
  }

  blockInfoToRecord(&pRecord, pBlockInfo, pSupInfo);

  // has duplicated ts of different version in this block
  pInfo->hasDupTs = (pBlockInfo->numRow > pBlockInfo->count) || (pBlockInfo->count <= 0);
  pInfo->overlapWithDelInfo = overlapWithDelSkyline(pScanInfo, &pRecord, order);

  // todo handle the primary key overlap case
  if (pScanInfo->sttKeyInfo.status == STT_FILE_HAS_DATA) {
    int64_t nextProcKeyInStt = pScanInfo->sttKeyInfo.nextProcKey.ts;
    pInfo->overlapWithSttBlock = !(pBlockInfo->lastKey < nextProcKeyInStt || pBlockInfo->firstKey > nextProcKeyInStt);
  }

  pInfo->moreThanCapcity = pBlockInfo->numRow > pReader->resBlockInfo.capacity;
  pInfo->partiallyRequired = dataBlockPartiallyRequired(&pReader->info.window, &pReader->info.verRange, pBlockInfo);
  pInfo->overlapWithKeyInBuf = keyOverlapFileBlock(keyInBuf, pBlockInfo, &pReader->info.verRange);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// 1. the version of all rows should be less than the endVersion
// 2. current block should not overlap with next neighbor block
// 3. current timestamp should not be overlap with each other
// 4. output buffer should be large enough to hold all rows in current block
// 5. delete info should not overlap with current block data
// 6. current block should not contain the duplicated ts
static int32_t fileBlockShouldLoad(STsdbReader* pReader, SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pScanInfo,
                                   TSDBKEY keyInBuf, bool* load) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SDataBlockToLoadInfo info = {0};

  TSDB_CHECK_NULL(load, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *load = false;
  code = getBlockToLoadInfo(&info, pBlockInfo, pScanInfo, keyInBuf, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  *load = (info.overlapWithNeighborBlock || info.hasDupTs || info.partiallyRequired || info.overlapWithKeyInBuf ||
           info.moreThanCapcity || info.overlapWithDelInfo || info.overlapWithSttBlock);

  // log the reason why load the datablock for profile
  if (*load) {
    tsdbDebug("%p uid:%" PRIu64
              " need to load the datablock, overlapneighbor:%d, hasDup:%d, partiallyRequired:%d, "
              "overlapWithKey:%d, greaterThanBuf:%d, overlapWithDel:%d, overlapWithSttBlock:%d, %s",
              pReader, pBlockInfo->uid, info.overlapWithNeighborBlock, info.hasDupTs, info.partiallyRequired,
              info.overlapWithKeyInBuf, info.moreThanCapcity, info.overlapWithDelInfo, info.overlapWithSttBlock,
              pReader->idStr);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t isCleanFileDataBlock(STsdbReader* pReader, SFileDataBlockInfo* pBlockInfo,
                                    STableBlockScanInfo* pScanInfo, TSDBKEY keyInBuf, bool* res) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SDataBlockToLoadInfo info = {0};

  TSDB_CHECK_NULL(res, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *res = false;
  code = getBlockToLoadInfo(&info, pBlockInfo, pScanInfo, keyInBuf, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  *res = !(info.overlapWithNeighborBlock || info.hasDupTs || info.overlapWithKeyInBuf || info.overlapWithDelInfo ||
           info.overlapWithSttBlock);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initRowMergeIfNeeded(STsdbReader* pReader, int64_t uid) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SRowMerger* pMerger = NULL;
  STSchema*   ps = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;

  if (pMerger->pArray == NULL) {
    ps = getTableSchemaImpl(pReader, uid);
    TSDB_CHECK_NULL(ps, code, lino, _end, terrno);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildDataBlockFromBuf(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, int64_t endKey) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  int64_t      st = 0;
  SSDataBlock* pBlock = NULL;

  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  if (!(pBlockScanInfo->iiter.hasVal || pBlockScanInfo->iter.hasVal)) {
    goto _end;
  }

  code = initRowMergeIfNeeded(pReader, pBlockScanInfo->uid);
  TSDB_CHECK_CODE(code, lino, _end);

  st = taosGetTimestampUs();
  pBlock = pReader->resBlockInfo.pResBlock;
  code = buildDataBlockFromBufImpl(pBlockScanInfo, endKey, pReader->resBlockInfo.capacity, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  updateComposedBlockInfo(pReader, el, pBlockScanInfo);

  tsdbDebug("%p build data block from cache completed, elapsed time:%.2f ms, numOfRows:%" PRId64 ", brange:%" PRId64
            " - %" PRId64 ", uid:%" PRIu64 ",  %s",
            pReader, el, pBlock->info.rows, pBlock->info.window.skey, pBlock->info.window.ekey, pBlockScanInfo->uid,
            pReader->idStr);

  pReader->cost.buildmemBlock += el;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tryCopyDistinctRowFromFileBlock(STsdbReader* pReader, SBlockData* pBlockData, SRowKey* pKey,
                                               SFileBlockDumpInfo* pDumpInfo, bool* copied) {
  // opt version
  // 1. it is not a border point
  // 2. the direct next point is not an duplicated timestamp
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    asc = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pDumpInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(copied, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *copied = false;
  asc = (pReader->info.order == TSDB_ORDER_ASC);
  if ((pDumpInfo->rowIndex < pDumpInfo->totalRows - 1 && asc) || (pDumpInfo->rowIndex > 0 && (!asc))) {
    int32_t step = ASCENDING_TRAVERSE(pReader->info.order) ? 1 : -1;

    SRowKey nextRowKey;
    TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);
    tColRowGetKey(pBlockData, pDumpInfo->rowIndex + step, &nextRowKey);

    if (pkCompEx(pKey, &nextRowKey) != 0) {  // merge is not needed
      code = doAppendRowFromFileBlock(pReader->resBlockInfo.pResBlock, pReader, pBlockData, pDumpInfo->rowIndex);
      TSDB_CHECK_CODE(code, lino, _end);
      pDumpInfo->rowIndex += step;
      *copied = true;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t nextRowFromSttBlocks(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, int32_t pkSrcSlot,
                                    SVersionRange* pVerRange) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  order = 0;
  int32_t  step = 0;
  SRowKey* pNextProc = NULL;

  TSDB_CHECK_NULL(pSttBlockReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  order = pSttBlockReader->order;
  step = ASCENDING_TRAVERSE(order) ? 1 : -1;
  pNextProc = &pScanInfo->sttKeyInfo.nextProcKey;

  while (1) {
    bool hasVal = false;
    code = tMergeTreeNext(&pSttBlockReader->mergeTree, &hasVal);
    if (code) {
      tsdbError("failed to iter the next row in stt-file merge tree, code:%s, %s", tstrerror(code),
                pSttBlockReader->mergeTree.idStr);
      TSDB_CHECK_CODE(code, lino, _end);
    }

    if (!hasVal) {  // the next value will be the accessed key in stt
      pScanInfo->sttKeyInfo.status = STT_FILE_NO_DATA;

      // next file, the timestamps in the next file must be greater than those in current
      pNextProc->ts += step;
      if (pSttBlockReader->numOfPks > 0) {
        if (IS_NUMERIC_TYPE(pNextProc->pks[0].type)) {
          pNextProc->pks[0].val = INT64_MIN;
        } else {
          memset(pNextProc->pks[0].pData, 0, pNextProc->pks[0].nData);
        }
      }
      goto _end;
    }

    TSDBROW* pRow = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
    int64_t  key = pRow->pBlockData->aTSKEY[pRow->iRow];
    int64_t  ver = pRow->pBlockData->aVersion[pRow->iRow];

    if (pSttBlockReader->numOfPks == 0) {
      pSttBlockReader->currentKey.ts = key;
    } else {
      tColRowGetKeyDeepCopy(pRow->pBlockData, pRow->iRow, pkSrcSlot, &pSttBlockReader->currentKey);
    }

    tColRowGetKeyDeepCopy(pRow->pBlockData, pRow->iRow, pkSrcSlot, pNextProc);

    if (pScanInfo->delSkyline != NULL && TARRAY_SIZE(pScanInfo->delSkyline) > 0) {
      bool dropped = false;
      code = hasBeenDropped(pScanInfo->delSkyline, &pScanInfo->sttBlockDelIndex, key, ver, order, pVerRange,
                            pSttBlockReader->numOfPks > 0, &dropped);
      TSDB_CHECK_CODE(code, lino, _end);
      if (!dropped) {
        pScanInfo->sttKeyInfo.status = STT_FILE_HAS_DATA;
        goto _end;
      }
    } else {
      pScanInfo->sttKeyInfo.status = STT_FILE_HAS_DATA;
      goto _end;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void doPinSttBlock(SSttBlockReader* pSttBlockReader) { tMergeTreePinSttBlock(&pSttBlockReader->mergeTree); }

static void doUnpinSttBlock(SSttBlockReader* pSttBlockReader) { tMergeTreeUnpinSttBlock(&pSttBlockReader->mergeTree); }

static int32_t tryCopyDistinctRowFromSttBlock(TSDBROW* fRow, SSttBlockReader* pSttBlockReader,
                                              STableBlockScanInfo* pScanInfo, SRowKey* pSttKey, STsdbReader* pReader,
                                              bool* copied) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SRowKey* pNext = NULL;

  TSDB_CHECK_NULL(pSttBlockReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(copied, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *copied = false;

  // avoid the fetch next row replace the referenced stt block in buffer
  doPinSttBlock(pSttBlockReader);
  code = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, pReader->suppInfo.pkSrcSlot, &pReader->info.verRange);
  doUnpinSttBlock(pSttBlockReader);
  TSDB_CHECK_CODE(code, lino, _end);

  if (hasDataInSttBlock(pScanInfo)) {
    pNext = getCurrentKeyInSttBlock(pSttBlockReader);
    if (pkCompEx(pSttKey, pNext) != 0) {
      code = doAppendRowFromFileBlock(pReader->resBlockInfo.pResBlock, pReader, fRow->pBlockData, fRow->iRow);
      *copied = (code == TSDB_CODE_SUCCESS);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  } else {
    code = doAppendRowFromFileBlock(pReader->resBlockInfo.pResBlock, pReader, fRow->pBlockData, fRow->iRow);
    *copied = (code == TSDB_CODE_SUCCESS);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static FORCE_INLINE STSchema* doGetSchemaForTSRow(int32_t sversion, STsdbReader* pReader, uint64_t uid) {
  int32_t   code = TSDB_CODE_SUCCESS;
  int32_t   lino = 0;
  STSchema* ps = NULL;
  void**    p = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // always set the newest schema version in pReader->info.pSchema
  if (pReader->info.pSchema == NULL) {
    ps = getTableSchemaImpl(pReader, uid);
    TSDB_CHECK_NULL(ps, code, lino, _end, terrno);
  }

  if (pReader->info.pSchema && sversion == pReader->info.pSchema->version) {
    ps = pReader->info.pSchema;
    goto _end;
  }

  p = tSimpleHashGet(pReader->pSchemaMap, &sversion, sizeof(sversion));
  if (p != NULL) {
    ps = *(STSchema**)p;
    goto _end;
  }

  code = metaGetTbTSchemaEx(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, uid, sversion, &ps);
  TSDB_CHECK_CODE(code, lino, _end);

  code = tSimpleHashPut(pReader->pSchemaMap, &sversion, sizeof(sversion), &ps, POINTER_BYTES);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    terrno = code;
    return NULL;
  }
  return ps;
}

static int32_t doMergeBufAndFileRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, TSDBROW* pRow,
                                     SIterInfo* pIter, SSttBlockReader* pSttBlockReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SRowMerger*         pMerger = NULL;
  SRow*               pTSRow = NULL;
  SBlockData*         pBlockData = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SRowKey*            pSttKey = NULL;
  int32_t             pkSrcSlot = 0;
  SRowKey             k = {0};
  STSchema*           pSchema = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRow, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;
  pBlockData = &pReader->status.fileBlockData;
  pDumpInfo = &pReader->status.fBlockDumpInfo;
  pkSrcSlot = pReader->suppInfo.pkSrcSlot;

  if (hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
    pSttKey = getCurrentKeyInSttBlock(pSttBlockReader);
  }

  tRowGetKeyEx(pRow, &k);

  if (pRow->type == TSDBROW_ROW_FMT) {
    pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
    TSDB_CHECK_NULL(pSchema, code, lino, _end, terrno);
  }

  SRowKey* pfKey = &(SRowKey){0};
  if (hasDataInFileBlock(pBlockData, pDumpInfo)) {
    tColRowGetKey(pBlockData, pDumpInfo->rowIndex, pfKey);
  } else {
    pfKey = NULL;
  }

  TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  code = initRowMergeIfNeeded(pReader, pBlockScanInfo->uid);
  TSDB_CHECK_CODE(code, lino, _end);

  SRowKey minKey = k;
  if (pReader->info.order == TSDB_ORDER_ASC) {
    if (pfKey != NULL && pkCompEx(pfKey, &minKey) < 0) {
      minKey = *pfKey;
    }

    if (pSttKey != NULL && pkCompEx(pSttKey, &minKey) < 0) {
      minKey = *pSttKey;
    }
  } else {
    if (pfKey != NULL && pkCompEx(pfKey, &minKey) > 0) {
      minKey = *pfKey;
    }

    if (pSttKey != NULL && pkCompEx(pSttKey, &minKey) > 0) {
      minKey = *pSttKey;
    }
  }

  // copy the last key before the time of stt reader loading the next stt block, in which the underlying data block may
  // be changed, resulting in the corresponding changing of the value of sttRowKey
  tRowKeyAssign(&pBlockScanInfo->lastProcKey, &minKey);

  // file block ---> stt block -----> mem
  if (pkCompEx(&minKey, pfKey) == 0) {
    code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pfKey, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pkCompEx(&minKey, pSttKey) == 0) {
    TSDBROW* fRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
    code = tsdbRowMergerAdd(pMerger, fRow1, NULL);
    TSDB_CHECK_CODE(code, lino, _end);
    code = doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, pMerger, pkSrcSlot, &pReader->info.verRange,
                                 pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pkCompEx(&minKey, &k) == 0) {
    code = tsdbRowMergerAdd(pMerger, pRow, pSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(pIter, pBlockScanInfo->uid, &k, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  code = tsdbRowMergerGetRow(pMerger, &pTSRow);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFreeClear(pTSRow);
  tsdbRowMergerClear(pMerger);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t mergeFileBlockAndSttBlock(STsdbReader* pReader, SSttBlockReader* pSttBlockReader, SRowKey* pKey,
                                         STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SRowMerger*         pMerger = NULL;
  SRow*               pTSRow = NULL;
  int32_t             pkSrcSlot = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pDumpInfo = &pReader->status.fBlockDumpInfo;
  pMerger = &pReader->status.merger;
  pkSrcSlot = pReader->suppInfo.pkSrcSlot;

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  code = initRowMergeIfNeeded(pReader, pBlockScanInfo->uid);
  TSDB_CHECK_CODE(code, lino, _end);

  bool dataInDataFile = hasDataInFileBlock(pBlockData, pDumpInfo);
  bool dataInSttFile = hasDataInSttBlock(pBlockScanInfo);

  if (dataInDataFile && (!dataInSttFile)) {
    // no stt file block available, only data block exists
    code = mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pKey, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else if ((!dataInDataFile) && dataInSttFile) {
    // no data in data file exists
    code = mergeRowsInSttBlocks(pSttBlockReader, pBlockScanInfo, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else if (pBlockScanInfo->cleanSttBlocks && pReader->info.execMode == READER_EXEC_ROWS) {
    // opt model for count data in stt file, which is not overlap with data blocks in files.
    code = mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pKey, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else {
    TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
    // row in both stt file blocks and data file blocks
    TSDBROW  fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
    SRowKey* pSttKey = getCurrentKeyInSttBlock(pSttBlockReader);

    int32_t ret = pkCompEx(pKey, pSttKey);

    if (ASCENDING_TRAVERSE(pReader->info.order)) {
      if (ret < 0) {  // asc
        code = mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pKey, pReader);
        TSDB_CHECK_CODE(code, lino, _end);
      } else if (ret > 0) {
        code = mergeRowsInSttBlocks(pSttBlockReader, pBlockScanInfo, pReader);
        TSDB_CHECK_CODE(code, lino, _end);
      }
    } else {  // desc
      if (ret > 0) {
        code = mergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pKey, pReader);
        TSDB_CHECK_CODE(code, lino, _end);
      } else if (ret < 0) {
        code = mergeRowsInSttBlocks(pSttBlockReader, pBlockScanInfo, pReader);
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
    if (ret != 0) {
      goto _end;
    }

    // pKey == pSttKey
    tRowKeyAssign(&pBlockScanInfo->lastProcKey, pKey);

    // the following for key == sttKey->key.ts
    // file block ------> stt  block

    code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pKey, pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
    code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
    TSDB_CHECK_CODE(code, lino, _end);

    // pSttKey will be changed when sttBlockReader iterates to the next row, so use pKey instead.
    code = doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, pMerger, pkSrcSlot, &pReader->info.verRange,
                                 pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);

    code = tsdbRowMergerGetRow(pMerger, &pTSRow);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

    taosMemoryFreeClear(pTSRow);
    tsdbRowMergerClear(pMerger);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doMergeMultiLevelRows(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo, SBlockData* pBlockData,
                                     SSttBlockReader* pSttBlockReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SRowMerger*         pMerger = NULL;
  SRow*               pTSRow = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SArray*             pDelList = NULL;
  int32_t             pkSrcSlot = 0;
  TSDBROW*            pRow = NULL;
  TSDBROW*            piRow = NULL;
  SRowKey*            pSttKey = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;
  pDumpInfo = &pReader->status.fBlockDumpInfo;
  pDelList = pBlockScanInfo->delSkyline;
  pkSrcSlot = pReader->suppInfo.pkSrcSlot;

  code = getValidMemRow(&pBlockScanInfo->iter, pDelList, pReader, &pRow);
  TSDB_CHECK_CODE(code, lino, _end);
  code = getValidMemRow(&pBlockScanInfo->iiter, pDelList, pReader, &piRow);
  TSDB_CHECK_CODE(code, lino, _end);

  if (hasDataInSttBlock(pBlockScanInfo) && (!pBlockScanInfo->cleanSttBlocks)) {
    pSttKey = getCurrentKeyInSttBlock(pSttBlockReader);
  }

  SRowKey* pfKey = &(SRowKey){0};
  if (hasDataInFileBlock(pBlockData, pDumpInfo)) {
    tColRowGetKey(pBlockData, pDumpInfo->rowIndex, pfKey);
  } else {
    pfKey = NULL;
  }

  SRowKey k = {0}, ik = {0};
  tRowGetKeyEx(pRow, &k);
  tRowGetKeyEx(piRow, &ik);

  STSchema* pSchema = NULL;
  if (pRow->type == TSDBROW_ROW_FMT) {
    pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
    TSDB_CHECK_NULL(pSchema, code, lino, _end, terrno);
  }

  STSchema* piSchema = NULL;
  if (piRow->type == TSDBROW_ROW_FMT) {
    piSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
    TSDB_CHECK_NULL(pSchema, code, lino, _end, terrno);
  }

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  code = initRowMergeIfNeeded(pReader, pBlockScanInfo->uid);
  TSDB_CHECK_CODE(code, lino, _end);

  SRowKey minKey = k;
  if (ASCENDING_TRAVERSE(pReader->info.order)) {
    if (pkCompEx(&ik, &minKey) < 0) {  // minKey > ik.key.ts) {
      minKey = ik;
    }

    if ((pfKey != NULL) && (pkCompEx(pfKey, &minKey) < 0)) {
      minKey = *pfKey;
    }

    if ((pSttKey != NULL) && (pkCompEx(pSttKey, &minKey) < 0)) {
      minKey = *pSttKey;
    }
  } else {
    if (pkCompEx(&ik, &minKey) > 0) {
      minKey = ik;
    }

    if ((pfKey != NULL) && (pkCompEx(pfKey, &minKey) > 0)) {
      minKey = *pfKey;
    }

    if ((pSttKey != NULL) && (pkCompEx(pSttKey, &minKey) > 0)) {
      minKey = *pSttKey;
    }
  }

  tRowKeyAssign(&pBlockScanInfo->lastProcKey, &minKey);

  // file block -----> stt block -----> imem -----> mem
  if (pkCompEx(&minKey, pfKey) == 0) {
    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);
    code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pfKey, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pkCompEx(&minKey, pSttKey) == 0) {
    TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
    code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInSttBlock(pSttBlockReader, pBlockScanInfo, pMerger, pkSrcSlot, &pReader->info.verRange,
                                 pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pkCompEx(&minKey, &ik) == 0) {
    code = tsdbRowMergerAdd(pMerger, piRow, piSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, &ik, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pkCompEx(&minKey, &k) == 0) {
    code = tsdbRowMergerAdd(pMerger, pRow, pSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, &k, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  code = tsdbRowMergerGetRow(pMerger, &pTSRow);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFreeClear(pTSRow);
  tsdbRowMergerClear(pMerger);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doInitMemDataIter(STsdbReader* pReader, STbData** pData, STableBlockScanInfo* pBlockScanInfo, STsdbRowKey* pKey,
                          SMemTable* pMem, SIterInfo* pIter, const char* type) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t backward = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(type, code, lino, _end, TSDB_CODE_INVALID_PARA);

  backward = (!ASCENDING_TRAVERSE(pReader->info.order));
  pIter->hasVal = false;

  if (pMem != NULL) {
    TSDB_CHECK_NULL(pData, code, lino, _end, TSDB_CODE_INVALID_PARA);
    *pData = tsdbGetTbDataFromMemTable(pMem, pReader->info.suid, pBlockScanInfo->uid);

    if ((*pData) != NULL) {
      code = tsdbTbDataIterCreate((*pData), pKey, backward, &pIter->iter);
      if (code == TSDB_CODE_SUCCESS) {
        pIter->hasVal = (tsdbTbDataIterGet(pIter->iter) != NULL);

        tsdbDebug("%p uid:%" PRIu64 ", check data in %s from skey:%" PRId64 ", order:%d, ts range in buf:%" PRId64
                  "-%" PRId64 " %s",
                  pReader, pBlockScanInfo->uid, type, pKey->key.ts, pReader->info.order, (*pData)->minKey,
                  (*pData)->maxKey, pReader->idStr);
      } else {
        tsdbError("%p uid:%" PRIu64 ", failed to create iterator for %s, code:%s, %s", pReader, pBlockScanInfo->uid,
                  type, tstrerror(code), pReader->idStr);
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
  } else {
    tsdbDebug("%p uid:%" PRIu64 ", no data in %s, %s", pReader, pBlockScanInfo->uid, type, pReader->idStr);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doForwardDataIter(SRowKey* pKey, SIterInfo* pIter, STableBlockScanInfo* pBlockScanInfo,
                                 STsdbReader* pReader) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SRowKey  rowKey = {0};
  TSDBROW* pRow = NULL;

  while (1) {
    code = getValidMemRow(pIter, pBlockScanInfo->delSkyline, pReader, &pRow);
    TSDB_CHECK_CODE(code, lino, _end);
    if (!pIter->hasVal) {
      break;
    }

    tRowGetKeyEx(pRow, &rowKey);
    int32_t ret = pkCompEx(pKey, &rowKey);
    if (ret == 0) {
      pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    } else {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// handle the open interval issue. Find the first row key that is greater than the given one.
static int32_t forwardDataIter(SRowKey* pKey, STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = doForwardDataIter(pKey, &pBlockScanInfo->iter, pBlockScanInfo, pReader);
  TSDB_CHECK_CODE(code, lino, _end);
  code = doForwardDataIter(pKey, &pBlockScanInfo->iiter, pBlockScanInfo, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initMemDataIterator(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  STbData*       d = NULL;
  STbData*       di = NULL;
  bool           asc = false;
  bool           forward = true;
  STsdbReadSnap* pSnap = NULL;
  STimeWindow*   pWindow = NULL;
  STsdbRowKey    startKey;

  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pReader->info.order);
  pSnap = pReader->pReadSnap;
  pWindow = &pReader->info.window;

  if (pBlockScanInfo->iterInit) {
    goto _end;
  }

  startKey.key = pBlockScanInfo->lastProcKey;
  startKey.version = asc ? pReader->info.verRange.minVer : pReader->info.verRange.maxVer;
  if ((asc && (startKey.key.ts < pWindow->skey)) || ((!asc) && startKey.key.ts > pWindow->ekey)) {
    startKey.key.ts = asc ? pWindow->skey : pWindow->ekey;
    forward = false;
  }

  code = doInitMemDataIter(pReader, &d, pBlockScanInfo, &startKey, pSnap->pMem, &pBlockScanInfo->iter, "mem");
  TSDB_CHECK_CODE(code, lino, _end);

  code = doInitMemDataIter(pReader, &di, pBlockScanInfo, &startKey, pSnap->pIMem, &pBlockScanInfo->iiter, "imem");
  TSDB_CHECK_CODE(code, lino, _end);

  code = loadMemTombData(&pBlockScanInfo->pMemDelData, d, di, pReader->info.verRange.maxVer);
  TSDB_CHECK_CODE(code, lino, _end);

  if (forward) {
    code = forwardDataIter(&startKey.key, pBlockScanInfo, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  pBlockScanInfo->iterInit = true;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t isValidFileBlockRow(SBlockData* pBlockData, int32_t rowIndex, STableBlockScanInfo* pBlockScanInfo,
                                   bool asc, STsdbReaderInfo* pInfo, STsdbReader* pReader, bool* valid) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(valid, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *valid = false;
  // it is an multi-table data block
  if (pBlockData->aUid != NULL) {
    uint64_t uid = pBlockData->aUid[rowIndex];
    if (uid != pBlockScanInfo->uid) {  // move to next row
      *valid = false;
      goto _end;
    }
  }

  TSDB_CHECK_NULL(pInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  // check for version and time range
  int64_t ver = pBlockData->aVersion[rowIndex];
  if (ver > pInfo->verRange.maxVer || ver < pInfo->verRange.minVer) {
    *valid = false;
    goto _end;
  }

  int64_t ts = pBlockData->aTSKEY[rowIndex];
  if (ts > pInfo->window.ekey || ts < pInfo->window.skey) {
    *valid = false;
    goto _end;
  }

  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if ((asc && (ts < pBlockScanInfo->lastProcKey.ts)) || ((!asc) && (ts > pBlockScanInfo->lastProcKey.ts))) {
    *valid = false;
    goto _end;
  }

  if (ts == pBlockScanInfo->lastProcKey.ts) {  // todo opt perf
    SRowKey nextRowKey;                        // lazy eval
    tColRowGetKey(pBlockData, rowIndex, &nextRowKey);
    if (pkCompEx(&pBlockScanInfo->lastProcKey, &nextRowKey) == 0) {
      *valid = false;
      goto _end;
    }
  }

  if (pBlockScanInfo->delSkyline != NULL && TARRAY_SIZE(pBlockScanInfo->delSkyline) > 0) {
    bool dropped = false;
    code = hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->fileDelIndex, ts, ver, pInfo->order,
                          &pInfo->verRange, pReader->suppInfo.numOfPks > 0, &dropped);
    TSDB_CHECK_CODE(code, lino, _end);
    if (dropped) {
      *valid = false;
      goto _end;
    }
  }

  *valid = true;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void initSttBlockReader(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  int32_t              order = 0;
  bool                 asc = false;
  int64_t              st = 0;
  SSttDataInfoForTable info = (SSttDataInfoForTable){0};

  TSDB_CHECK_NULL(pSttBlockReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  order = pReader->info.order;
  asc = ASCENDING_TRAVERSE(order);

  // the stt block reader has been initialized for this table.
  if (pSttBlockReader->uid == pScanInfo->uid) {
    goto _end;
  }

  if (pSttBlockReader->uid != 0) {
    tMergeTreeClose(&pSttBlockReader->mergeTree);
  }

  pSttBlockReader->uid = pScanInfo->uid;

  // second or third time init stt block reader
  if (pScanInfo->cleanSttBlocks && (pReader->info.execMode == READER_EXEC_ROWS)) {
    // only allowed to retrieve clean stt blocks for count once
    if (pScanInfo->sttBlockReturned) {
      pScanInfo->sttKeyInfo.status = STT_FILE_NO_DATA;
      tsdbDebug("uid:%" PRIu64 " set no stt-file data after stt-block retrieved, %s", pScanInfo->uid, pReader->idStr);
    }
    goto _end;
  }

  STimeWindow w = pSttBlockReader->window;
  if (asc) {
    w.skey = pScanInfo->sttKeyInfo.nextProcKey.ts;
  } else {
    w.ekey = pScanInfo->sttKeyInfo.nextProcKey.ts;
  }

  st = taosGetTimestampUs();
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
      .pCurRowKey = &pScanInfo->sttKeyInfo.nextProcKey,
      .pReader = pReader,
      .idstr = pReader->idStr,
      .rspRows = (pReader->info.execMode == READER_EXEC_ROWS),
  };

  info.pKeyRangeList = taosArrayInit(4, sizeof(SSttKeyRange));
  TSDB_CHECK_NULL(info.pKeyRangeList, code, lino, _end, terrno);

  code = tMergeTreeOpen2(&pSttBlockReader->mergeTree, &conf, &info);
  TSDB_CHECK_CODE(code, lino, _end);

  code = initMemDataIterator(pScanInfo, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  code = initDelSkylineIterator(pScanInfo, pReader->info.order, &pReader->cost);
  TSDB_CHECK_CODE(code, lino, _end);

  if (conf.rspRows) {
    pScanInfo->cleanSttBlocks = isCleanSttBlock(info.pKeyRangeList, &pReader->info.window, pScanInfo, order);
    if (pScanInfo->cleanSttBlocks) {
      pScanInfo->numOfRowsInStt = info.numOfRows;

      // calculate the time window for data in stt files
      for (int32_t i = 0; i < taosArrayGetSize(info.pKeyRangeList); ++i) {
        SSttKeyRange* pKeyRange = taosArrayGet(info.pKeyRangeList, i);
        if (pKeyRange == NULL) {
          continue;
        }

        if (pkCompEx(&pScanInfo->sttRange.skey, &pKeyRange->skey) > 0) {
          tRowKeyAssign(&pScanInfo->sttRange.skey, &pKeyRange->skey);
        }

        if (pkCompEx(&pScanInfo->sttRange.ekey, &pKeyRange->ekey) < 0) {
          tRowKeyAssign(&pScanInfo->sttRange.ekey, &pKeyRange->ekey);
        }
      }

      pScanInfo->sttKeyInfo.status = taosArrayGetSize(info.pKeyRangeList) ? STT_FILE_HAS_DATA : STT_FILE_NO_DATA;

      SRowKey* p = asc ? &pScanInfo->sttRange.skey : &pScanInfo->sttRange.ekey;
      tRowKeyAssign(&pScanInfo->sttKeyInfo.nextProcKey, p);
    } else {                                // not clean stt blocks
      INIT_KEYRANGE(&pScanInfo->sttRange);  // reset the time window
      code = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, pReader->suppInfo.pkSrcSlot, &pReader->info.verRange);
      if (code != TSDB_CODE_SUCCESS) {
        pScanInfo->sttBlockReturned = false;
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
  } else {
    pScanInfo->cleanSttBlocks = false;
    INIT_KEYRANGE(&pScanInfo->sttRange);  // reset the time window
    code = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, pReader->suppInfo.pkSrcSlot, &pReader->info.verRange);
    if (code != TSDB_CODE_SUCCESS) {
      pScanInfo->sttBlockReturned = false;
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

  pScanInfo->sttBlockReturned = false;
  int64_t el = taosGetTimestampUs() - st;
  pReader->cost.initSttBlockReader += (el / 1000.0);

  tsdbDebug("init stt block reader completed, elapsed time:%" PRId64 "us %s", el, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (pReader) {
      pReader->code = code;
    }
  }
  taosArrayDestroy(info.pKeyRangeList);
}

static bool hasDataInSttBlock(STableBlockScanInfo* pInfo) { return pInfo->sttKeyInfo.status == STT_FILE_HAS_DATA; }

bool hasDataInFileBlock(const SBlockData* pBlockData, const SFileBlockDumpInfo* pDumpInfo) {
  if ((pBlockData->nRow > 0) && (pBlockData->nRow != pDumpInfo->totalRows)) {
    return false;  // this is an invalid result.
  }
  return pBlockData->nRow > 0 && (!pDumpInfo->allDumped);
}

int32_t mergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pBlockScanInfo, SRowKey* pKey,
                              STsdbReader* pReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SRow*               pTSRow = NULL;
  SRowMerger*         pMerger = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  bool                copied = false;

  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pKey, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;
  pDumpInfo = &pReader->status.fBlockDumpInfo;

  code = tryCopyDistinctRowFromFileBlock(pReader, pBlockData, pKey, pDumpInfo, &copied);
  TSDB_CHECK_CODE(code, lino, _end);

  // merge is not initialized yet, due to the fact that the pReader->info.pSchema is not initialized
  code = initRowMergeIfNeeded(pReader, pBlockScanInfo->uid);
  TSDB_CHECK_CODE(code, lino, _end);

  tRowKeyAssign(&pBlockScanInfo->lastProcKey, pKey);

  if (copied) {
    goto _end;
  }

  TSDBROW fRow = tsdbRowFromBlockData(pBlockData, pDumpInfo->rowIndex);

  code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doMergeRowsInFileBlocks(pBlockData, pBlockScanInfo, pKey, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  code = tsdbRowMergerGetRow(pMerger, &pTSRow);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pBlockScanInfo);

  taosMemoryFreeClear(pTSRow);
  tsdbRowMergerClear(pMerger);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t mergeRowsInSttBlocks(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, STsdbReader* pReader) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SRow*       pTSRow = NULL;
  int32_t     pkSrcSlot = 0;
  SRowMerger* pMerger = NULL;
  bool        copied = false;

  TSDB_CHECK_NULL(pSttBlockReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pkSrcSlot = pReader->suppInfo.pkSrcSlot;
  pMerger = &pReader->status.merger;

  // let's record the last processed key
  tRowKeyAssign(&pScanInfo->lastProcKey, getCurrentKeyInSttBlock(pSttBlockReader));

  TSDBROW* pRow = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
  TSDBROW  fRow = {.iRow = pRow->iRow, .type = TSDBROW_COL_FMT, .pBlockData = pRow->pBlockData};

  if (IS_VAR_DATA_TYPE(pScanInfo->lastProcKey.pks[0].type)) {
    tsdbTrace("fRow ptr:%p, %d, uid:%" PRIu64 ", ts:%" PRId64 " pk:%s %s", pRow->pBlockData, pRow->iRow,
              pSttBlockReader->uid, fRow.pBlockData->aTSKEY[fRow.iRow], pScanInfo->lastProcKey.pks[0].pData,
              pReader->idStr);
  }

  code = tryCopyDistinctRowFromSttBlock(&fRow, pSttBlockReader, pScanInfo, &pScanInfo->lastProcKey, pReader, &copied);
  TSDB_CHECK_CODE(code, lino, _end);

  if (copied) {
    goto _end;
  }

  code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
  TSDB_CHECK_CODE(code, lino, _end);

  TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
  code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doMergeRowsInSttBlock(pSttBlockReader, pScanInfo, pMerger, pkSrcSlot, &pReader->info.verRange, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  code = tsdbRowMergerGetRow(pMerger, &pTSRow);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doAppendRowFromTSRow(pReader->resBlockInfo.pResBlock, pReader, pTSRow, pScanInfo);

  taosMemoryFreeClear(pTSRow);
  tsdbRowMergerClear(pMerger);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildComposedDataBlockImpl(STsdbReader* pReader, STableBlockScanInfo* pBlockScanInfo,
                                          SBlockData* pBlockData, SSttBlockReader* pSttBlockReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  TSDBROW*            pRow = NULL;
  TSDBROW*            piRow = NULL;
  SRowKey*            pKey = &(SRowKey){0};

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pDumpInfo = &pReader->status.fBlockDumpInfo;

  if (hasDataInFileBlock(pBlockData, pDumpInfo)) {
    tColRowGetKey(pBlockData, pDumpInfo->rowIndex, pKey);
  } else {
    pKey = NULL;
  }

  if (pBlockScanInfo->iter.hasVal) {
    code = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader, &pRow);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pBlockScanInfo->iiter.hasVal) {
    code = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader, &piRow);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pRow != NULL && piRow != NULL) {
    // two levels of mem-table does contain the valid rows
    code = doMergeMultiLevelRows(pReader, pBlockScanInfo, pBlockData, pSttBlockReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else if (pBlockScanInfo->iiter.hasVal) {
    // imem + file + stt block
    code = doMergeBufAndFileRows(pReader, pBlockScanInfo, piRow, &pBlockScanInfo->iiter, pSttBlockReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else if (pBlockScanInfo->iter.hasVal) {
    // mem + file + stt block
    code = doMergeBufAndFileRows(pReader, pBlockScanInfo, pRow, &pBlockScanInfo->iter, pSttBlockReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else {
    // files data blocks + stt block
    code = mergeFileBlockAndSttBlock(pReader, pSttBlockReader, pKey, pBlockScanInfo, pBlockData);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t loadNeighborIfOverlap(SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pBlockScanInfo,
                                     STsdbReader* pReader, bool* loadNeighbor) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  int32_t             order = 0;
  SDataBlockIter*     pIter = NULL;
  SBlockLoadSuppInfo* pSupInfo = NULL;
  int32_t             step = 0;
  int32_t             nextIndex = -1;
  SBrinRecord         rec = {0};
  bool                hasNeighbor = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(loadNeighbor, code, lino, _end, TSDB_CODE_INVALID_PARA);

  order = pReader->info.order;
  pIter = &pReader->status.blockIter;
  pSupInfo = &pReader->suppInfo;
  step = ASCENDING_TRAVERSE(order) ? 1 : -1;

  *loadNeighbor = false;
  code = getNeighborBlockOfTable(pIter, pBlockInfo, pBlockScanInfo, &nextIndex, order, &rec, pSupInfo, &hasNeighbor);
  TSDB_CHECK_CODE(code, lino, _end);
  if (!hasNeighbor) {  // do nothing
    goto _end;
  }

  // load next block
  if (overlapWithNeighborBlock2(pBlockInfo, &rec, order, pReader->suppInfo.pk.type, pReader->suppInfo.numOfPks)) {
    SReaderStatus*  pStatus = &pReader->status;
    SDataBlockIter* pBlockIter = &pStatus->blockIter;

    // 1. find the next neighbor block in the scan block list
    STableDataBlockIdx* tableDataBlockIdx = taosArrayGet(pBlockScanInfo->pBlockIdxList, nextIndex);
    TSDB_CHECK_NULL(tableDataBlockIdx, code, lino, _end, TSDB_CODE_INVALID_PARA);

    // 2. remove it from the scan block list
    int32_t neighborIndex = tableDataBlockIdx->globalIndex;
    code = setFileBlockActiveInBlockIter(pReader, pBlockIter, neighborIndex, step);
    TSDB_CHECK_CODE(code, lino, _end);

    // 3. load the neighbor block, and set it to be the currently accessed file data block
    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData, pBlockInfo->uid);
    TSDB_CHECK_CODE(code, lino, _end);

    // 4. check the data values
    code = initBlockDumpInfo(pReader, pBlockIter);
    TSDB_CHECK_CODE(code, lino, _end);
    *loadNeighbor = true;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void updateComposedBlockInfo(STsdbReader* pReader, double el, STableBlockScanInfo* pBlockScanInfo) {
  SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;

  pResBlock->info.id.uid = (pBlockScanInfo != NULL) ? pBlockScanInfo->uid : 0;
  pResBlock->info.dataLoad = 1;
  pResBlock->info.version = pReader->info.verRange.maxVer;

  int32_t code = blockDataUpdateTsWindow(pResBlock, pReader->suppInfo.slotId[0]);
  code = blockDataUpdatePkRange(pResBlock, pReader->suppInfo.pkDstSlot, ASCENDING_TRAVERSE(pReader->info.order));
  setComposedBlockFlag(pReader, true);

  pReader->cost.composedBlocks += 1;
  pReader->cost.buildComposedBlockTime += el;
}

static int32_t buildComposedDataBlock(STsdbReader* pReader) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  bool                 asc = false;
  int64_t              st = 0;
  double               el = 0;
  int32_t              step = 0;
  SSDataBlock*         pResBlock = NULL;
  SFileDataBlockInfo*  pBlockInfo = NULL;
  SSttBlockReader*     pSttBlockReader = NULL;
  SFileBlockDumpInfo*  pDumpInfo = NULL;
  STableBlockScanInfo* pBlockScanInfo = NULL;
  TSDBKEY              keyInBuf;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pReader->info.order);
  st = taosGetTimestampUs();
  step = asc ? 1 : -1;
  pResBlock = pReader->resBlockInfo.pResBlock;
  pSttBlockReader = pReader->status.fileIter.pSttBlockReader;
  pDumpInfo = &pReader->status.fBlockDumpInfo;

  code = getCurrentBlockInfo(&pReader->status.blockIter, &pBlockInfo, pReader->idStr);
  if (code != TSDB_CODE_SUCCESS) {
    return 0;
  }

  if (pReader->pIgnoreTables && taosHashGet(*pReader->pIgnoreTables, &pBlockInfo->uid, sizeof(pBlockInfo->uid))) {
    setBlockAllDumped(pDumpInfo, pBlockInfo->lastKey, pReader->info.order);
    return code;
  }

  code = getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, &pBlockScanInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  code = getCurrentKeyInBuf(pBlockScanInfo, pReader, &keyInBuf);
  TSDB_CHECK_CODE(code, lino, _end);

  // it is a clean block, load it directly
  int64_t cap = pReader->resBlockInfo.capacity;
  bool    isClean = false;
  code = isCleanFileDataBlock(pReader, pBlockInfo, pBlockScanInfo, keyInBuf, &isClean);
  TSDB_CHECK_CODE(code, lino, _end);
  bool directCopy = isClean && (pBlockInfo->numRow <= cap) && (pBlockScanInfo->sttKeyInfo.status == STT_FILE_NO_DATA) &&
                    ((asc && ((pBlockInfo->lastKey < keyInBuf.ts) || (keyInBuf.ts == INT64_MIN))) ||
                     (!asc && (pBlockInfo->lastKey > keyInBuf.ts)));
  if (directCopy) {
    code = copyBlockDataToSDataBlock(pReader, &pBlockScanInfo->lastProcKey);
    TSDB_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  SBlockData* pBlockData = &pReader->status.fileBlockData;
  initSttBlockReader(pSttBlockReader, pBlockScanInfo, pReader);
  code = pReader->code;
  TSDB_CHECK_CODE(code, lino, _end);

  while (1) {
    bool hasBlockData = false;
    {
      while (pBlockData->nRow > 0 && pBlockData->uid == pBlockScanInfo->uid) {
        // find the first qualified row in data block
        bool valid = false;
        code =
            isValidFileBlockRow(pBlockData, pDumpInfo->rowIndex, pBlockScanInfo, asc, &pReader->info, pReader, &valid);
        TSDB_CHECK_CODE(code, lino, _end);
        if (valid) {
          hasBlockData = true;
          break;
        }

        pDumpInfo->rowIndex += step;

        if (pDumpInfo->rowIndex >= pBlockData->nRow || pDumpInfo->rowIndex < 0) {
          // NOTE: get the new block info
          code = getCurrentBlockInfo(&pReader->status.blockIter, &pBlockInfo, pReader->idStr);
          TSDB_CHECK_CODE(code, lino, _end);

          // continue check for the next file block if the last ts in the current block
          // is overlapped with the next neighbor block
          bool loadNeighbor = false;
          code = loadNeighborIfOverlap(pBlockInfo, pBlockScanInfo, pReader, &loadNeighbor);
          if ((!loadNeighbor) || (code != 0)) {
            lino = __LINE__;
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
    TSDB_CHECK_CODE(code, lino, _end);

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
  if (pReader) {
    el = (taosGetTimestampUs() - st) / 1000.0;
    updateComposedBlockInfo(pReader, el, pBlockScanInfo);
  }

  if (pResBlock && pResBlock->info.rows > 0) {
    tsdbDebug("%p uid:%" PRIu64 ", composed data block created, brange:%" PRIu64 "-%" PRIu64 " rows:%" PRId64
              ", elapsed time:%.2f ms %s",
              pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
              pResBlock->info.rows, el, pReader->idStr);
  }

  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
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
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t newDelDataInFile = 0;
  int64_t st = 0;
  SArray* pSource = NULL;

  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  newDelDataInFile = taosArrayGetSize(pBlockScanInfo->pFileDelData);
  if (newDelDataInFile == 0 &&
      ((pBlockScanInfo->delSkyline != NULL) || (TARRAY_SIZE(pBlockScanInfo->pMemDelData) == 0))) {
    goto _end;
  }

  st = taosGetTimestampUs();

  if (pBlockScanInfo->delSkyline != NULL) {
    taosArrayClear(pBlockScanInfo->delSkyline);
  } else {
    pBlockScanInfo->delSkyline = taosArrayInit(4, sizeof(TSDBKEY));
    TSDB_CHECK_NULL(pBlockScanInfo->delSkyline, code, lino, _end, terrno);
  }

  pSource = pBlockScanInfo->pFileDelData;
  if (pSource == NULL) {
    pSource = pBlockScanInfo->pMemDelData;
  } else {
    const void* p1 = taosArrayAddAll(pSource, pBlockScanInfo->pMemDelData);
    TSDB_CHECK_NULL(p1, code, lino, _end, terrno);
  }

  code = tsdbBuildDeleteSkyline(pSource, 0, taosArrayGetSize(pSource) - 1, pBlockScanInfo->delSkyline);

  taosArrayClear(pBlockScanInfo->pFileDelData);
  int32_t index = getInitialDelIndex(pBlockScanInfo->delSkyline, order);

  pBlockScanInfo->iter.index = index;
  pBlockScanInfo->iiter.index = index;
  pBlockScanInfo->fileDelIndex = index;
  pBlockScanInfo->sttBlockDelIndex = index;
  TSDB_CHECK_CODE(code, lino, _end);

  TSDB_CHECK_NULL(pCost, code, lino, _end, TSDB_CODE_INVALID_PARA);
  double el = taosGetTimestampUs() - st;
  pCost->createSkylineIterTime = el / 1000.0;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t getCurrentKeyInBuf(STableBlockScanInfo* pScanInfo, STsdbReader* pReader, TSDBKEY* pkey) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  bool     asc = false;
  bool     hasKey = false;
  bool     hasIKey = false;
  TSDBROW* pRow = NULL;
  TSDBROW* pIRow = NULL;
  TSDBKEY  key = {.ts = TSKEY_INITIAL_VAL};
  TSDBKEY  ikey = {.ts = TSKEY_INITIAL_VAL};

  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pkey, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pReader->info.order);

  code = getValidMemRow(&pScanInfo->iter, pScanInfo->delSkyline, pReader, &pRow);
  TSDB_CHECK_CODE(code, lino, _end);
  if (pRow != NULL) {
    hasKey = true;
    key = TSDBROW_KEY(pRow);
  }

  code = getValidMemRow(&pScanInfo->iiter, pScanInfo->delSkyline, pReader, &pIRow);
  TSDB_CHECK_CODE(code, lino, _end);
  if (pIRow != NULL) {
    hasIKey = true;
    ikey = TSDBROW_KEY(pIRow);
  }

  if (hasKey) {
    if (hasIKey) {  // has data in mem & imem
      if (asc) {
        *pkey = key.ts <= ikey.ts ? key : ikey;
      } else {
        *pkey = key.ts <= ikey.ts ? ikey : key;
      }
    } else {  // no data in imem
      *pkey = key;
    }
  } else {
    // no data in mem & imem, return the initial value
    // only imem has data, return ikey
    *pkey = ikey;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  SArray*        pIndexList = NULL;
  size_t         numOfTables = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockNum, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlockNum->numOfBlocks = 0;
  pBlockNum->numOfSttFiles = 0;

  numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  pIndexList = taosArrayInit(numOfTables, sizeof(SBrinBlk));
  TSDB_CHECK_NULL(pIndexList, code, lino, _end, terrno);

  while (1) {
    // only check here, since the iterate data in memory is very fast.
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      code = pReader->code;
      TSDB_CHECK_CODE(code, lino, _end);
    }

    bool hasNext = false;
    code = filesetIteratorNext(&pStatus->fileIter, pReader, &hasNext);
    TSDB_CHECK_CODE(code, lino, _end);

    if (!hasNext) {  // no data files on disk
      break;
    }

    taosArrayClear(pIndexList);
    code = doLoadBlockIndex(pReader, pReader->pFileReader, pIndexList);
    TSDB_CHECK_CODE(code, lino, _end);

    if (taosArrayGetSize(pIndexList) > 0 || pReader->status.pCurrentFileset->lvlArr->size > 0) {
      code = loadFileBlockBrinInfo(pReader, pIndexList, pBlockNum, pTableList);
      TSDB_CHECK_CODE(code, lino, _end);

      if (pBlockNum->numOfBlocks + pBlockNum->numOfSttFiles > 0) {
        if (pReader->bFilesetDelimited) {
          prepareDurationForNextFileSet(pReader);
        }
        break;
      }
    }

    // no blocks in current file, try next files
  }

  code = loadDataFileTombDataForAll(pReader);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pIndexList) {
    taosArrayDestroy(pIndexList);
  }
  return code;
}

// pTableIter can be NULL, no need to handle the return value
static int32_t resetTableListIndex(SReaderStatus* pStatus, const char* id) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  STableUidList* pList = NULL;

  TSDB_CHECK_NULL(pStatus, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pList = &pStatus->uidList;

  pList->currentIndex = 0;
  uint64_t uid = pList->tableUidList[0];
  pStatus->pTableIter = tSimpleHashGet(pStatus->pTableMap, &uid, sizeof(uid));
  if (pStatus->pTableIter == NULL) {
    tsdbError("%s failed to load tableBlockScanInfo for uid:%" PRId64 ", code: internal error", id, uid);
    TSDB_CHECK_NULL(pStatus->pTableIter, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
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

static int32_t buildCleanBlockFromSttFiles(STsdbReader* pReader, STableBlockScanInfo* pScanInfo) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SReaderStatus*   pStatus = NULL;
  SSttBlockReader* pSttBlockReader = NULL;
  SSDataBlock*     pResBlock = NULL;
  SDataBlockInfo*  pInfo = NULL;
  bool             asc = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pSttBlockReader = pStatus->fileIter.pSttBlockReader;
  pResBlock = pReader->resBlockInfo.pResBlock;

  asc = ASCENDING_TRAVERSE(pReader->info.order);
  code = blockDataEnsureCapacity(pResBlock, pScanInfo->numOfRowsInStt);
  TSDB_CHECK_CODE(code, lino, _end);

  pInfo = &pResBlock->info;
  pInfo->rows = pScanInfo->numOfRowsInStt;
  pInfo->id.uid = pScanInfo->uid;
  pInfo->dataLoad = 1;
  pInfo->window.skey = pScanInfo->sttRange.skey.ts;
  pInfo->window.ekey = pScanInfo->sttRange.ekey.ts;

  setComposedBlockFlag(pReader, true);

  pScanInfo->sttKeyInfo.nextProcKey.ts = asc ? pScanInfo->sttRange.ekey.ts + 1 : pScanInfo->sttRange.skey.ts - 1;
  pScanInfo->sttKeyInfo.status = STT_FILE_NO_DATA;

  if (asc) {
    tRowKeyAssign(&pScanInfo->lastProcKey, &pScanInfo->sttRange.ekey);
  } else {
    tRowKeyAssign(&pScanInfo->lastProcKey, &pScanInfo->sttRange.skey);
  }

  pScanInfo->sttBlockReturned = true;
  pSttBlockReader->mergeTree.pIter = NULL;

  tsdbDebug("%p uid:%" PRId64 " return clean stt block as one, brange:%" PRId64 "-%" PRId64 " rows:%" PRId64 " %s",
            pReader, pResBlock->info.id.uid, pResBlock->info.window.skey, pResBlock->info.window.ekey,
            pResBlock->info.rows, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildCleanBlockFromDataFiles(STsdbReader* pReader, STableBlockScanInfo* pScanInfo,
                                            SFileDataBlockInfo* pBlockInfo, int32_t blockIndex) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SReaderStatus*  pStatus = NULL;
  SDataBlockInfo* pInfo = NULL;
  bool            asc = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // whole data block is required, return it directly
  pStatus = &pReader->status;
  pInfo = &pReader->resBlockInfo.pResBlock->info;
  asc = ASCENDING_TRAVERSE(pReader->info.order);

  pInfo->rows = pBlockInfo->numRow;
  pInfo->id.uid = pScanInfo->uid;
  pInfo->dataLoad = 0;
  pInfo->version = pReader->info.verRange.maxVer;
  pInfo->window = (STimeWindow){.skey = pBlockInfo->firstKey, .ekey = pBlockInfo->lastKey};

  if (pReader->suppInfo.numOfPks > 0) {
    if (IS_NUMERIC_TYPE(pReader->suppInfo.pk.type)) {
      pInfo->pks[0].val = pBlockInfo->firstPk.val;
      pInfo->pks[1].val = pBlockInfo->lastPk.val;
    } else {
      (void)memcpy(pInfo->pks[0].pData, varDataVal(pBlockInfo->firstPk.pData), varDataLen(pBlockInfo->firstPk.pData));
      (void)memcpy(pInfo->pks[1].pData, varDataVal(pBlockInfo->lastPk.pData), varDataLen(pBlockInfo->lastPk.pData));

      pInfo->pks[0].nData = varDataLen(pBlockInfo->firstPk.pData);
      pInfo->pks[1].nData = varDataLen(pBlockInfo->lastPk.pData);
    }
  }

  // update the last key for the corresponding table
  setComposedBlockFlag(pReader, false);
  setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlockInfo->lastKey, pReader->info.order);
  code = updateLastKeyInfo(&pScanInfo->lastProcKey, pBlockInfo, pInfo, pReader->suppInfo.numOfPks, asc);
  TSDB_CHECK_CODE(code, lino, _end);

  tsdbDebug("%p uid:%" PRIu64
            " clean file block retrieved from file, global index:%d, "
            "table index:%d, rows:%d, brange:%" PRId64 "-%" PRId64 ", %s",
            pReader, pScanInfo->uid, blockIndex, pBlockInfo->tbBlockIdx, pBlockInfo->numRow, pBlockInfo->firstKey,
            pBlockInfo->lastKey, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doLoadSttBlockSequentially(STsdbReader* pReader) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SReaderStatus*       pStatus = NULL;
  SSttBlockReader*     pSttBlockReader = NULL;
  STableUidList*       pUidList = NULL;
  SSDataBlock*         pResBlock = NULL;
  STableBlockScanInfo* pScanInfo = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pSttBlockReader = pStatus->fileIter.pSttBlockReader;
  pUidList = &pStatus->uidList;

  if (tSimpleHashGetSize(pStatus->pTableMap) == 0) {
    goto _end;
  }

  pResBlock = pReader->resBlockInfo.pResBlock;

  while (1) {
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      code = pReader->code;
      TSDB_CHECK_CODE(code, lino, _end);
    }

    // load the last data block of current table
    if (pStatus->pTableIter == NULL) {
      tsdbError("table Iter is null, invalid pScanInfo, try next table %s", pReader->idStr);
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        break;
      }

      continue;
    } else {
      pScanInfo = *(STableBlockScanInfo**)pStatus->pTableIter;
    }

    if (pReader->pIgnoreTables && taosHashGet(*pReader->pIgnoreTables, &pScanInfo->uid, sizeof(pScanInfo->uid))) {
      // reset the index in last block when handing a new file
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        break;
      }

      continue;
    }

    initSttBlockReader(pSttBlockReader, pScanInfo, pReader);
    code = pReader->code;
    TSDB_CHECK_CODE(code, lino, _end);

    if (!hasDataInSttBlock(pScanInfo)) {
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        break;
      }

      continue;
    }

    // if only require the total rows, no need to load data from stt file if it is clean stt blocks
    if (pReader->info.execMode == READER_EXEC_ROWS && pScanInfo->cleanSttBlocks) {
      code = buildCleanBlockFromSttFiles(pReader, pScanInfo);
      TSDB_CHECK_CODE(code, lino, _end);
      break;
    }

    int64_t st = taosGetTimestampUs();
    while (1) {
      // no data in stt block and block, no need to proceed.
      if (!hasDataInSttBlock(pScanInfo)) {
        break;
      }

      code = buildComposedDataBlockImpl(pReader, pScanInfo, &pReader->status.fileBlockData, pSttBlockReader);
      TSDB_CHECK_CODE(code, lino, _end);

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
      break;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTable(pUidList, pStatus);
    if (!hasNexTable) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// current active data block not overlap with the stt-files/stt-blocks
static bool notOverlapWithFiles(SFileDataBlockInfo* pBlockInfo, STableBlockScanInfo* pScanInfo, bool asc) {
  if ((!hasDataInSttBlock(pScanInfo)) || (pScanInfo->cleanSttBlocks == true)) {
    return true;
  } else {
    int64_t keyInStt = pScanInfo->sttKeyInfo.nextProcKey.ts;
    return (asc && pBlockInfo->lastKey < keyInStt) || (!asc && pBlockInfo->firstKey > keyInStt);
  }
}

static int32_t doBuildDataBlock(STsdbReader* pReader) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SReaderStatus*       pStatus = NULL;
  SDataBlockIter*      pBlockIter = NULL;
  STableBlockScanInfo* pScanInfo = NULL;
  SFileDataBlockInfo*  pBlockInfo = NULL;
  SSttBlockReader*     pSttBlockReader = NULL;
  TSDBKEY              keyInBuf;
  bool                 asc = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlockIter = &pStatus->blockIter;
  pSttBlockReader = pReader->status.fileIter.pSttBlockReader;
  asc = ASCENDING_TRAVERSE(pReader->info.order);

  code = getCurrentBlockInfo(pBlockIter, &pBlockInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pReader->pIgnoreTables && taosHashGet(*pReader->pIgnoreTables, &pBlockInfo->uid, sizeof(pBlockInfo->uid))) {
    setBlockAllDumped(&pStatus->fBlockDumpInfo, pBlockInfo->lastKey, pReader->info.order);
    goto _end;
  }

  code = pReader->code;
  TSDB_CHECK_CODE(code, lino, _end);

  code = getTableBlockScanInfo(pReader->status.pTableMap, pBlockInfo->uid, &pScanInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pScanInfo->sttKeyInfo.status == STT_FILE_READER_UNINIT) {
    initSttBlockReader(pSttBlockReader, pScanInfo, pReader);
    code = pReader->code;
    TSDB_CHECK_CODE(code, lino, _end);
  }

  code = getCurrentKeyInBuf(pScanInfo, pReader, &keyInBuf);
  TSDB_CHECK_CODE(code, lino, _end);
  bool load = false;
  code = fileBlockShouldLoad(pReader, pBlockInfo, pScanInfo, keyInBuf, &load);
  TSDB_CHECK_CODE(code, lino, _end);
  if (load) {
    code = doLoadFileBlockData(pReader, pBlockIter, &pStatus->fileBlockData, pScanInfo->uid);
    TSDB_CHECK_CODE(code, lino, _end);

    // build composed data block
    code = buildComposedDataBlock(pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else if (bufferDataInFileBlockGap(keyInBuf, pBlockInfo, pScanInfo, pReader->info.order)) {
    // data in memory that are earlier than current file block and stt blocks
    // rows in buffer should be less than the file block in asc, greater than file block in desc
    int64_t endKey = getBoarderKeyInFiles(pBlockInfo, pScanInfo, pReader->info.order);
    code = buildDataBlockFromBuf(pReader, pScanInfo, endKey);
    TSDB_CHECK_CODE(code, lino, _end);
  } else {
    if (notOverlapWithFiles(pBlockInfo, pScanInfo, asc)) {
      int64_t keyInStt = pScanInfo->sttKeyInfo.nextProcKey.ts;

      if ((!hasDataInSttBlock(pScanInfo)) || (asc && pBlockInfo->lastKey < keyInStt) ||
          (!asc && pBlockInfo->firstKey > keyInStt)) {
        // the stt blocks may located in the gap of different data block, but the whole sttRange may overlap with the
        // data block, so the overlap check is invalid actually.
        code = buildCleanBlockFromDataFiles(pReader, pScanInfo, pBlockInfo, pBlockIter->index);
        TSDB_CHECK_CODE(code, lino, _end);
      } else {  // clean stt block
        TSDB_CHECK_CONDITION((pReader->info.execMode == READER_EXEC_ROWS) && (pSttBlockReader->mergeTree.pIter == NULL),
                             code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
        code = buildCleanBlockFromSttFiles(pReader, pScanInfo);
        TSDB_CHECK_CODE(code, lino, _end);
      }
    } else {
      SBlockData* pBData = &pReader->status.fileBlockData;
      tBlockDataReset(pBData);

      SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;

      tsdbDebug("load data in stt block firstly %s", pReader->idStr);
      int64_t st = taosGetTimestampUs();

      // let's load data from stt files, make sure clear the cleanStt block flag before load the data from stt files
      initSttBlockReader(pSttBlockReader, pScanInfo, pReader);
      code = pReader->code;
      TSDB_CHECK_CODE(code, lino, _end);

      // no data in stt block, no need to proceed.
      while (hasDataInSttBlock(pScanInfo)) {
        TSDB_CHECK_CONDITION(pScanInfo->sttKeyInfo.status == STT_FILE_HAS_DATA, code, lino, _end,
                             TSDB_CODE_INTERNAL_ERROR);

        code = buildComposedDataBlockImpl(pReader, pScanInfo, &pReader->status.fileBlockData, pSttBlockReader);
        TSDB_CHECK_CODE(code, lino, _end);

        if (pResBlock->info.rows >= pReader->resBlockInfo.capacity) {
          break;
        }

        // data in stt now overlaps with current active file data block, need to composed with file data block.
        SRowKey* pSttKey = getCurrentKeyInSttBlock(pSttBlockReader);
        if ((pSttKey->ts >= pBlockInfo->firstKey && asc) || (pSttKey->ts <= pBlockInfo->lastKey && (!asc))) {
          tsdbDebug("%p lastKeyInStt:%" PRId64 ", overlap with file block, brange:%" PRId64 "-%" PRId64 " %s", pReader,
                    pSttKey->ts, pBlockInfo->firstKey, pBlockInfo->lastKey, pReader->idStr);
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

  code = pReader->code;
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildBlockFromBufferSeqForPreFileset(STsdbReader* pReader, int64_t endKey) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;

  tsdbDebug("seq load data blocks from cache that preceeds fileset %d, %s", pReader->status.pCurrentFileset->fid,
            pReader->idStr);

  while (1) {
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      code = pReader->code;
      TSDB_CHECK_CODE(code, lino, _end);
    }

    STableBlockScanInfo** pBlockScanInfo = pStatus->pProcMemTableIter;
    if (pReader->pIgnoreTables &&
        taosHashGet(*pReader->pIgnoreTables, &(*pBlockScanInfo)->uid, sizeof((*pBlockScanInfo)->uid))) {
      bool hasNexTable = moveToNextTableForPreFileSetMem(pStatus);
      if (!hasNexTable) {
        break;
      }
      continue;
    }

    code = initMemDataIterator(*pBlockScanInfo, pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    code = initDelSkylineIterator(*pBlockScanInfo, pReader->info.order, &pReader->cost);
    TSDB_CHECK_CODE(code, lino, _end);

    code = buildDataBlockFromBuf(pReader, *pBlockScanInfo, endKey);
    TSDB_CHECK_CODE(code, lino, _end);

    if (pReader->resBlockInfo.pResBlock->info.rows > 0) {
      break;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTableForPreFileSetMem(pStatus);
    if (!hasNexTable) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildBlockFromBufferSequentially(STsdbReader* pReader, int64_t endKey) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  STableUidList* pUidList = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pUidList = &pStatus->uidList;

  tsdbDebug("seq load data blocks from cache, %s", pReader->idStr);

  while (1) {
    if (pReader->code != TSDB_CODE_SUCCESS) {
      tsdbWarn("tsdb reader is stopped ASAP, code:%s, %s", tstrerror(pReader->code), pReader->idStr);
      code = pReader->code;
      TSDB_CHECK_CODE(code, lino, _end);
    }

    STableBlockScanInfo** pBlockScanInfo = pStatus->pTableIter;
    if (pBlockScanInfo == NULL || *pBlockScanInfo == NULL) {
      break;
    }

    if (pReader->pIgnoreTables &&
        taosHashGet(*pReader->pIgnoreTables, &(*pBlockScanInfo)->uid, sizeof((*pBlockScanInfo)->uid))) {
      bool hasNexTable = moveToNextTable(pUidList, pStatus);
      if (!hasNexTable) {
        break;
      }
      continue;
    }

    code = initMemDataIterator(*pBlockScanInfo, pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    code = initDelSkylineIterator(*pBlockScanInfo, pReader->info.order, &pReader->cost);
    TSDB_CHECK_CODE(code, lino, _end);

    code = buildDataBlockFromBuf(pReader, *pBlockScanInfo, endKey);
    TSDB_CHECK_CODE(code, lino, _end);

    if (pReader->resBlockInfo.pResBlock->info.rows > 0) {
      break;
    }

    // current table is exhausted, let's try next table
    bool hasNexTable = moveToNextTable(pUidList, pStatus);
    if (!hasNexTable) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// set the correct start position in case of the first/last file block, according to the query time window
static int32_t initBlockDumpInfo(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SFileDataBlockInfo* pBlockInfo = NULL;
  SReaderStatus*      pStatus = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pDumpInfo = &pStatus->fBlockDumpInfo;

  code = getCurrentBlockInfo(pBlockIter, &pBlockInfo, pReader->idStr);
  if (code == TSDB_CODE_SUCCESS) {
    pDumpInfo->totalRows = pBlockInfo->numRow;
    pDumpInfo->rowIndex = ASCENDING_TRAVERSE(pReader->info.order) ? 0 : pBlockInfo->numRow - 1;
  } else {
    pDumpInfo->totalRows = 0;
    pDumpInfo->rowIndex = 0;
    code = TSDB_CODE_SUCCESS;
  }

  pDumpInfo->allDumped = false;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t initForFirstBlockInFile(STsdbReader* pReader, SDataBlockIter* pBlockIter) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SBlockNumber num = {0};
  SArray*      pTableList = NULL;

  pTableList = taosArrayInit(40, POINTER_BYTES);
  TSDB_CHECK_NULL(pTableList, code, lino, _end, terrno);

  code = moveToNextFile(pReader, &num, pTableList);
  TSDB_CHECK_CODE(code, lino, _end);

  // all data files are consumed, try data in buffer
  if (num.numOfBlocks + num.numOfSttFiles == 0) {
    pReader->status.loadFromFile = false;
    goto _end;
  }

  // initialize the block iterator for a new fileset
  if (num.numOfBlocks > 0) {
    code = initBlockIterator(pReader, pBlockIter, num.numOfBlocks, pTableList);
    TSDB_CHECK_CODE(code, lino, _end);
  } else {  // no block data, only last block exists
    tBlockDataReset(&pReader->status.fileBlockData);
    code = resetDataBlockIterator(pBlockIter, pReader->info.order, shouldFreePkBuf(&pReader->suppInfo), pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);

    code = resetTableListIndex(&pReader->status, pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  code = initBlockDumpInfo(pReader, pBlockIter);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (pTableList) {
    taosArrayDestroy(pTableList);
  }
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

static int32_t doReadDataFromSttFiles(STsdbReader* pReader, ERetrieveType* pReturnType) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SSDataBlock*    pResBlock = NULL;
  SDataBlockIter* pBlockIter = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReturnType, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pResBlock = pReader->resBlockInfo.pResBlock;
  pBlockIter = &pReader->status.blockIter;

  *pReturnType = TSDB_READ_RETURN;

  tsdbDebug("seq load data blocks from stt files %s", pReader->idStr);

  while (1) {
    code = doLoadSttBlockSequentially(pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    if (pResBlock->info.rows > 0) {
      goto _end;
    }

    // all data blocks are checked in this stt file, now let's try the next file set
    TSDB_CHECK_CONDITION(pReader->status.pTableIter == NULL, code, lino, _end, TSDB_CODE_INTERNAL_ERROR);

    code = initForFirstBlockInFile(pReader, pBlockIter);
    TSDB_CHECK_CODE(code, lino, _end);

    // all the data files are completely checked
    if (pReader->status.loadFromFile == false) {
      goto _end;
    }

    if (pReader->status.bProcMemPreFileset) {
      code = buildFromPreFilesetBuffer(pReader);
      TSDB_CHECK_CODE(code, lino, _end);
      if (pResBlock->info.rows > 0) {
        pReader->status.processingMemPreFileSet = true;
        goto _end;
      }
    }

    if (pBlockIter->numOfBlocks > 0) {  // there are data blocks existed.
      *pReturnType = TSDB_READ_CONTINUE;
      goto _end;
    } else {  // all blocks in data file are checked, let's check the data in stt-files
      code = resetTableListIndex(&pReader->status, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t buildBlockFromFiles(STsdbReader* pReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SDataBlockIter*     pBlockIter = NULL;
  SSDataBlock*        pResBlock = NULL;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SBlockData*         pBlockData = NULL;
  const char*         id = NULL;
  bool                asc = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  asc = ASCENDING_TRAVERSE(pReader->info.order);

  pBlockIter = &pReader->status.blockIter;
  pResBlock = pReader->resBlockInfo.pResBlock;
  pDumpInfo = &pReader->status.fBlockDumpInfo;
  pBlockData = &pReader->status.fileBlockData;
  id = pReader->idStr;

  if (pBlockIter->numOfBlocks == 0) {
    // let's try to extract data from stt files.
    ERetrieveType type = 0;
    code = doReadDataFromSttFiles(pReader, &type);
    TSDB_CHECK_CODE(code, lino, _end);
    if (type == TSDB_READ_RETURN) {
      goto _end;
    }

    code = doBuildDataBlock(pReader);
    TSDB_CHECK_CODE(code, lino, _end);
    if (pResBlock->info.rows > 0) {
      goto _end;
    }
  }

  while (1) {
    if (fileBlockPartiallyRead(pDumpInfo, asc)) {  // file data block is partially loaded
      code = buildComposedDataBlock(pReader);
      TSDB_CHECK_CODE(code, lino, _end);
    } else {
      // current block are exhausted, try the next file block
      if (pDumpInfo->allDumped) {
        // try next data block in current file
        bool hasNext = blockIteratorNext(&pReader->status.blockIter);
        if (hasNext) {  // check for the next block in the block accessed order list
          code = initBlockDumpInfo(pReader, pBlockIter);
          TSDB_CHECK_CODE(code, lino, _end);
        } else {
          // all data blocks in files are checked, let's check the data in last files.
          // data blocks in current file are exhausted, let's try the next file now
          if (pBlockData->uid != 0) {
            tBlockDataClear(pBlockData);
          }

          tBlockDataReset(pBlockData);
          code = resetDataBlockIterator(pBlockIter, pReader->info.order, shouldFreePkBuf(&pReader->suppInfo), id);
          TSDB_CHECK_CODE(code, lino, _end);

          code = resetTableListIndex(&pReader->status, id);
          TSDB_CHECK_CODE(code, lino, _end);

          ERetrieveType type = 0;
          code = doReadDataFromSttFiles(pReader, &type);
          TSDB_CHECK_CODE(code, lino, _end);
          if (type == TSDB_READ_RETURN) {
            break;
          }
        }
      }

      code = doBuildDataBlock(pReader);
      TSDB_CHECK_CODE(code, lino, _end);
    }

    if (pResBlock->info.rows > 0) {
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void getTsdbByRetentions(SVnode* pVnode, SQueryTableDataCond* pCond, SRetention* retentions, const char* idStr,
                                int8_t* pLevel, STsdb** pTsdb) {
  if (pTsdb == NULL) {
    return;
  }

  *pTsdb = NULL;
  if (VND_IS_RSMA(pVnode) && !pCond->skipRollup) {
    int8_t  level = 0;
    int8_t  precision = pVnode->config.tsdbCfg.precision;
    int64_t now = taosGetTimestamp(precision);
    int64_t offset = tsQueryRsmaTolerance * ((precision == TSDB_TIME_PRECISION_MILLI)   ? 1L
                                             : (precision == TSDB_TIME_PRECISION_MICRO) ? 1000L
                                                                                        : 1000000L);

    for (int32_t i = 0; i < TSDB_RETENTION_MAX; ++i) {
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
      *pTsdb = VND_RSMA0(pVnode);
      return;
    } else if (level == TSDB_RETENTION_L1) {
      *pLevel = TSDB_RETENTION_L1;
      tsdbDebug("vgId:%d, rsma level %d is selected to query %s", TD_VID(pVnode), TSDB_RETENTION_L1, str);
      *pTsdb = VND_RSMA1(pVnode);
      return;
    } else {
      *pLevel = TSDB_RETENTION_L2;
      tsdbDebug("vgId:%d, rsma level %d is selected to query %s", TD_VID(pVnode), TSDB_RETENTION_L2, str);
      *pTsdb = VND_RSMA2(pVnode);
      return;
    }
  }

  *pTsdb = VND_TSDB(pVnode);
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

static int32_t reverseSearchStartPos(const SArray* pDelList, int32_t index, int64_t key, bool asc, int32_t* start) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  num = 0;

  num = taosArrayGetSize(pDelList);
  *start = index;

  if (asc) {
    if (*start >= num - 1) {
      *start = num - 1;
    }

    TSDBKEY* p = taosArrayGet(pDelList, *start);
    TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);

    while (p->ts >= key && *start > 0) {
      *start -= 1;
    }
  } else {
    if (index <= 0) {
      *start = 0;
    }

    TSDBKEY* p = taosArrayGet(pDelList, *start);
    TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);

    while (p->ts <= key && *start < num - 1) {
      *start += 1;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t hasBeenDropped(const SArray* pDelList, int32_t* index, int64_t key, int64_t ver, int32_t order,
                       SVersionRange* pVerRange, bool hasPk, bool* dropped) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  size_t  num = 0;
  int32_t step = 0;
  bool    asc = false;

  *dropped = false;

  if (pDelList == NULL || (TARRAY_SIZE(pDelList) == 0)) {
    goto _end;
  }

  num = taosArrayGetSize(pDelList);
  asc = ASCENDING_TRAVERSE(order);
  step = asc ? 1 : -1;

  if (hasPk) {  // handle the case where duplicated timestamps existed.
    code = reverseSearchStartPos(pDelList, *index, key, asc, index);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (asc) {
    if (*index >= num - 1) {
      TSDBKEY* last = taosArrayGetLast(pDelList);
      if (last == NULL) {
        goto _end;
      }

      if (key > last->ts) {
        goto _end;
      } else if (key == last->ts) {
        TSDBKEY* prev = taosArrayGet(pDelList, num - 2);
        if (prev == NULL) {
          goto _end;
        }

        *dropped = (prev->version >= ver && prev->version <= pVerRange->maxVer && prev->version >= pVerRange->minVer);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pNext = taosArrayGet(pDelList, (*index) + 1);
      if (pCurrent == NULL || pNext == NULL) {
        goto _end;
      }

      if (key < pCurrent->ts) {
        goto _end;
      }

      if (pCurrent->ts <= key && pNext->ts >= key && pCurrent->version >= ver &&
          pVerRange->maxVer >= pCurrent->version) {
        *dropped = true;
        goto _end;
      }

      while (pNext->ts <= key && (*index) < num - 1) {
        (*index) += 1;

        if ((*index) < num - 1) {
          pCurrent = taosArrayGet(pDelList, *index);
          pNext = taosArrayGet(pDelList, (*index) + 1);
          if (pCurrent == NULL || pNext == NULL) {
            break;
          }

          // it is not a consecutive deletion range, ignore it
          if (pCurrent->version == 0 && pNext->version > 0) {
            continue;
          }

          if (pCurrent->ts <= key && pNext->ts >= key && pCurrent->version >= ver &&
              pVerRange->maxVer >= pCurrent->version) {
            *dropped = true;
            break;
          }
        }
      }
    }
  } else {
    if (*index <= 0) {
      TSDBKEY* pFirst = taosArrayGet(pDelList, 0);
      if (pFirst == NULL) {
        goto _end;
      }

      if (key < pFirst->ts) {
        goto _end;
      } else if (key == pFirst->ts) {
        *dropped = pFirst->version >= ver;
      } else {
        tsdbError("unexpected error, key:%" PRId64 ", first:%" PRId64, key, pFirst->ts);
      }
    } else {
      TSDBKEY* pCurrent = taosArrayGet(pDelList, *index);
      TSDBKEY* pPrev = taosArrayGet(pDelList, (*index) - 1);
      if (pCurrent == NULL || pPrev == NULL) {
        goto _end;
      }

      if (key > pCurrent->ts) {
        goto _end;
      }

      if (pPrev->ts <= key && pCurrent->ts >= key && pPrev->version >= ver) {
        *dropped = true;
        goto _end;
      }

      while (pPrev->ts >= key && (*index) > 1) {
        (*index) += step;

        if ((*index) >= 1) {
          pCurrent = taosArrayGet(pDelList, *index);
          pPrev = taosArrayGet(pDelList, (*index) - 1);
          if (pCurrent == NULL || pPrev == NULL) {
            break;
          }

          // it is not a consecutive deletion range, ignore it
          if (pCurrent->version > 0 && pPrev->version == 0) {
            continue;
          }

          if (pPrev->ts <= key && pCurrent->ts >= key && pPrev->version >= ver) {
            *dropped = true;
            break;
          }
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

FORCE_INLINE int32_t getValidMemRow(SIterInfo* pIter, const SArray* pDelList, STsdbReader* pReader, TSDBROW** pRes) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  int32_t  order = 0;
  TSDBROW* pRow = NULL;
  TSDBKEY  key;

  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pRes, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pRes = NULL;

  if (!pIter->hasVal) {
    goto _end;
  }

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  order = pReader->info.order;
  pRow = tsdbTbDataIterGet(pIter->iter);

  TSDBROW_INIT_KEY(pRow, key);
  if (outOfTimeWindow(key.ts, &pReader->info.window)) {
    pIter->hasVal = false;
    goto _end;
  }

  // it is a valid data version
  if (key.version <= pReader->info.verRange.maxVer && key.version >= pReader->info.verRange.minVer) {
    if (pDelList == NULL || TARRAY_SIZE(pDelList) == 0) {
      *pRes = pRow;
      goto _end;
    } else {
      bool dropped = false;
      code = hasBeenDropped(pDelList, &pIter->index, key.ts, key.version, order, &pReader->info.verRange,
                            pReader->suppInfo.numOfPks > 0, &dropped);
      TSDB_CHECK_CODE(code, lino, _end);
      if (!dropped) {
        *pRes = pRow;
        goto _end;
      }
    }
  }

  while (1) {
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    if (!pIter->hasVal) {
      goto _end;
    }

    pRow = tsdbTbDataIterGet(pIter->iter);

    TSDBROW_INIT_KEY(pRow, key);
    if (outOfTimeWindow(key.ts, &pReader->info.window)) {
      pIter->hasVal = false;
      goto _end;
    }

    if (key.version <= pReader->info.verRange.maxVer && key.version >= pReader->info.verRange.minVer) {
      if (pDelList == NULL || TARRAY_SIZE(pDelList) == 0) {
        *pRes = pRow;
        goto _end;
      } else {
        bool dropped = false;
        code = hasBeenDropped(pDelList, &pIter->index, key.ts, key.version, order, &pReader->info.verRange,
                              pReader->suppInfo.numOfPks > 0, &dropped);
        TSDB_CHECK_CODE(code, lino, _end);
        if (!dropped) {
          *pRes = pRow;
          goto _end;
        }
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doMergeRowsInBuf(SIterInfo* pIter, uint64_t uid, SRowKey* pCurKey, SArray* pDelList, STsdbReader* pReader) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SRowMerger* pMerger = NULL;

  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;

  while (1) {
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);
    if (!pIter->hasVal) {
      break;
    }

    // data exists but not valid
    TSDBROW* pRow = NULL;
    code = getValidMemRow(pIter, pDelList, pReader, &pRow);
    TSDB_CHECK_CODE(code, lino, _end);
    if (pRow == NULL) {
      break;
    }

    // ts is not identical, quit
    if (TSDBROW_TS(pRow) != pCurKey->ts) {
      break;
    }

    if (pCurKey->numOfPKs > 0) {
      SRowKey nextKey = {0};
      tRowGetKeyEx(pRow, &nextKey);
      if (pkCompEx(pCurKey, &nextKey) != 0) {
        break;
      }
    }

    STSchema* pTSchema = NULL;
    if (pRow->type == TSDBROW_ROW_FMT) {
      pTSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, uid);
      TSDB_CHECK_NULL(pTSchema, code, lino, _end, terrno);
    }

    code = tsdbRowMergerAdd(pMerger, pRow, pTSchema);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doMergeRowsInFileBlockImpl(SBlockData* pBlockData, int32_t* rowIndex, SRowKey* pKey, SRowMerger* pMerger,
                                          SVersionRange* pVerRange, int32_t step) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(rowIndex, code, lino, _end, TSDB_CODE_INVALID_PARA);

  while ((*rowIndex) < pBlockData->nRow && (*rowIndex) >= 0) {
    SRowKey cur;
    tColRowGetKey(pBlockData, (*rowIndex), &cur);
    if (pkCompEx(&cur, pKey) != 0) {
      break;
    }

    if (pBlockData->aVersion[(*rowIndex)] > pVerRange->maxVer ||
        pBlockData->aVersion[(*rowIndex)] < pVerRange->minVer) {
      (*rowIndex) += step;
      continue;
    }

    TSDBROW fRow = tsdbRowFromBlockData(pBlockData, (*rowIndex));
    code = tsdbRowMergerAdd(pMerger, &fRow, NULL);
    TSDB_CHECK_CODE(code, lino, _end);
    (*rowIndex) += step;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

typedef enum {
  CHECK_FILEBLOCK_CONT = 0x1,
  CHECK_FILEBLOCK_QUIT = 0x2,
} CHECK_FILEBLOCK_STATE;

static int32_t checkForNeighborFileBlock(STsdbReader* pReader, STableBlockScanInfo* pScanInfo,
                                         SFileDataBlockInfo* pFBlock, SRowMerger* pMerger, SRowKey* pKey,
                                         CHECK_FILEBLOCK_STATE* state) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SBlockData*         pBlockData = NULL;
  bool                asc = false;
  SVersionRange*      pVerRange = NULL;
  bool                loadNeighbor = true;
  int32_t             step = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(state, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pDumpInfo = &pReader->status.fBlockDumpInfo;
  pBlockData = &pReader->status.fileBlockData;
  asc = ASCENDING_TRAVERSE(pReader->info.order);
  pVerRange = &pReader->info.verRange;
  ASCENDING_TRAVERSE(pReader->info.order) ? 1 : -1;

  *state = CHECK_FILEBLOCK_QUIT;
  code = loadNeighborIfOverlap(pFBlock, pScanInfo, pReader, &loadNeighbor);
  TSDB_CHECK_CODE(code, lino, _end);

  if (loadNeighbor) {
    code = doMergeRowsInFileBlockImpl(pBlockData, &pDumpInfo->rowIndex, pKey, pMerger, pVerRange, step);
    TSDB_CHECK_CODE(code, lino, _end);
    if ((pDumpInfo->rowIndex >= pDumpInfo->totalRows && asc) || (pDumpInfo->rowIndex < 0 && !asc)) {
      *state = CHECK_FILEBLOCK_CONT;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doMergeRowsInFileBlocks(SBlockData* pBlockData, STableBlockScanInfo* pScanInfo, SRowKey* pKey,
                                STsdbReader* pReader) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SFileBlockDumpInfo* pDumpInfo = NULL;
  SRowMerger*         pMerger = NULL;
  bool                asc = false;
  int32_t             step = 0;
  SVersionRange*      pRange = NULL;

  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pDumpInfo = &pReader->status.fBlockDumpInfo;
  pMerger = &pReader->status.merger;
  asc = ASCENDING_TRAVERSE(pReader->info.order);
  step = asc ? 1 : -1;
  pRange = &pReader->info.verRange;

  pDumpInfo->rowIndex += step;
  if ((pDumpInfo->rowIndex <= pBlockData->nRow - 1 && asc) || (pDumpInfo->rowIndex >= 0 && !asc)) {
    code = doMergeRowsInFileBlockImpl(pBlockData, &pDumpInfo->rowIndex, pKey, pMerger, pRange, step);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // all rows are consumed, let's try next file block
  if ((pDumpInfo->rowIndex >= pBlockData->nRow && asc) || (pDumpInfo->rowIndex < 0 && !asc)) {
    while (1) {
      SFileDataBlockInfo* pFileBlockInfo = NULL;
      code = getCurrentBlockInfo(&pReader->status.blockIter, &pFileBlockInfo, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);

      if (pFileBlockInfo == NULL) {
        break;
      }

      CHECK_FILEBLOCK_STATE st = CHECK_FILEBLOCK_QUIT;
      code = checkForNeighborFileBlock(pReader, pScanInfo, pFileBlockInfo, pMerger, pKey, &st);
      TSDB_CHECK_CODE(code, lino, _end);
      if (st == CHECK_FILEBLOCK_QUIT) {
        break;
      }
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doMergeRowsInSttBlock(SSttBlockReader* pSttBlockReader, STableBlockScanInfo* pScanInfo, SRowMerger* pMerger,
                              int32_t pkSrcSlot, SVersionRange* pVerRange, const char* idStr) {
  int32_t  code = TSDB_CODE_SUCCESS;
  int32_t  lino = 0;
  SRowKey* pRowKey = NULL;
  SRowKey* pNextKey = NULL;

  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pRowKey = &pScanInfo->lastProcKey;

  while (1) {
    code = nextRowFromSttBlocks(pSttBlockReader, pScanInfo, pkSrcSlot, pVerRange);
    TSDB_CHECK_CODE(code, lino, _end);
    if (!hasDataInSttBlock(pScanInfo)) {
      break;
    }

    pNextKey = getCurrentKeyInSttBlock(pSttBlockReader);

    int32_t ret = pkCompEx(pRowKey, pNextKey);
    if (ret == 0) {
      TSDBROW* pRow1 = tMergeTreeGetRow(&pSttBlockReader->mergeTree);
      code = tsdbRowMergerAdd(pMerger, pRow1, NULL);
      TSDB_CHECK_CODE(code, lino, _end);
    } else {
      tsdbTrace("uid:%" PRIu64 " last del index:%d, del range:%d, lastKeyInStt:%" PRId64 ", %s", pScanInfo->uid,
                pScanInfo->sttBlockDelIndex, (int32_t)taosArrayGetSize(pScanInfo->delSkyline),
                pScanInfo->sttKeyInfo.nextProcKey.ts, idStr);
      break;
    }
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doMergeMemTableMultiRows(TSDBROW* pRow, SRowKey* pKey, uint64_t uid, SIterInfo* pIter, SArray* pDelList,
                                 TSDBROW* pResRow, STsdbReader* pReader, bool* freeTSRow) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SRowMerger* pMerger = NULL;
  TSDBROW*    pNextRow = NULL;
  STSchema*   pTSchema = NULL;
  STSchema*   pTSchema1 = NULL;
  TSDBROW     current;

  TSDB_CHECK_NULL(pRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pIter, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pResRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(freeTSRow, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;
  current = *pRow;

  {  // if the timestamp of the next valid row has a different ts, return current row directly
    pIter->hasVal = tsdbTbDataIterNext(pIter->iter);

    if (!pIter->hasVal) {
      *pResRow = *pRow;
      *freeTSRow = false;
      goto _end;
    } else {  // has next point in mem/imem
      code = getValidMemRow(pIter, pDelList, pReader, &pNextRow);
      TSDB_CHECK_CODE(code, lino, _end);
      if (pNextRow == NULL) {
        *pResRow = current;
        *freeTSRow = false;
        goto _end;
      }

      if (TSDBROW_TS(&current) != TSDBROW_TS(pNextRow)) {
        *pResRow = current;
        *freeTSRow = false;
        goto _end;
      }

      if (pKey->numOfPKs > 0) {
        SRowKey nextRowKey = {0};
        tRowGetKeyEx(pNextRow, &nextRowKey);
        if (pkCompEx(pKey, &nextRowKey) != 0) {
          *pResRow = current;
          *freeTSRow = false;
          goto _end;
        }
      }
    }
  }

  // start to merge duplicated rows
  if (current.type == TSDBROW_ROW_FMT) {  // get the correct schema for row-wise data in memory
    pTSchema = doGetSchemaForTSRow(current.pTSRow->sver, pReader, uid);
    TSDB_CHECK_NULL(pTSchema, code, lino, _end, terrno);
  }

  code = tsdbRowMergerAdd(pMerger, &current, pTSchema);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pNextRow->type == TSDBROW_ROW_FMT) {  // get the correct schema for row-wise data in memory
    pTSchema1 = doGetSchemaForTSRow(pNextRow->pTSRow->sver, pReader, uid);
    TSDB_CHECK_NULL(pTSchema1, code, lino, _end, terrno);
  }

  code = tsdbRowMergerAdd(pMerger, pNextRow, pTSchema1);
  TSDB_CHECK_CODE(code, lino, _end);

  code = doMergeRowsInBuf(pIter, uid, pKey, pDelList, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  code = tsdbRowMergerGetRow(pMerger, &pResRow->pTSRow);
  TSDB_CHECK_CODE(code, lino, _end);

  pResRow->type = TSDBROW_ROW_FMT;
  tsdbRowMergerClear(pMerger);
  *freeTSRow = true;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doMergeMemIMemRows(TSDBROW* pRow, SRowKey* pRowKey, TSDBROW* piRow, SRowKey* piRowKey,
                           STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, SRow** pTSRow) {
  int32_t     code = TSDB_CODE_SUCCESS;
  int32_t     lino = 0;
  SRowMerger* pMerger = NULL;
  STSchema*   pSchema = NULL;
  STSchema*   piSchema = NULL;

  TSDB_CHECK_NULL(pRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(piRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pMerger = &pReader->status.merger;

  if (pRow->type == TSDBROW_ROW_FMT) {
    pSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(pRow), pReader, pBlockScanInfo->uid);
    TSDB_CHECK_NULL(pSchema, code, lino, _end, terrno);
  }

  if (piRow->type == TSDBROW_ROW_FMT) {
    piSchema = doGetSchemaForTSRow(TSDBROW_SVERSION(piRow), pReader, pBlockScanInfo->uid);
    TSDB_CHECK_NULL(piSchema, code, lino, _end, terrno);
  }

  if (ASCENDING_TRAVERSE(pReader->info.order)) {  // ascending order imem --> mem
    code = tsdbRowMergerAdd(pMerger, piRow, piSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, piRowKey, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    code = tsdbRowMergerAdd(pMerger, pRow, pSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, pRowKey, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  } else {
    code = tsdbRowMergerAdd(pMerger, pRow, pSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(&pBlockScanInfo->iter, pBlockScanInfo->uid, pRowKey, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    code = tsdbRowMergerAdd(pMerger, piRow, piSchema);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doMergeRowsInBuf(&pBlockScanInfo->iiter, pBlockScanInfo->uid, piRowKey, pBlockScanInfo->delSkyline, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  tRowKeyAssign(&pBlockScanInfo->lastProcKey, pRowKey);

  code = tsdbRowMergerGetRow(pMerger, pTSRow);
  tsdbRowMergerClear(pMerger);
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbGetNextRowInMem(STableBlockScanInfo* pBlockScanInfo, STsdbReader* pReader, TSDBROW* pResRow,
                                   int64_t endKey, bool* freeTSRow) {
  int32_t    code = TSDB_CODE_SUCCESS;
  int32_t    lino = 0;
  TSDBROW*   pRow = NULL;
  TSDBROW*   piRow = NULL;
  SArray*    pDelList = NULL;
  uint64_t   uid = 0;
  SIterInfo* piter = NULL;
  SIterInfo* piiter = NULL;
  SRowKey    rowKey = {0};
  SRowKey    irowKey = {0};
  bool       asc = false;

  TSDB_CHECK_NULL(pBlockScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pResRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(freeTSRow, code, lino, _end, TSDB_CODE_INVALID_PARA);

  code = getValidMemRow(&pBlockScanInfo->iter, pBlockScanInfo->delSkyline, pReader, &pRow);
  TSDB_CHECK_CODE(code, lino, _end);
  code = getValidMemRow(&pBlockScanInfo->iiter, pBlockScanInfo->delSkyline, pReader, &piRow);
  TSDB_CHECK_CODE(code, lino, _end);

  pDelList = pBlockScanInfo->delSkyline;
  uid = pBlockScanInfo->uid;
  piter = &pBlockScanInfo->iter;
  piiter = &pBlockScanInfo->iiter;

  // todo refactor
  asc = ASCENDING_TRAVERSE(pReader->info.order);
  if (piter->hasVal) {
    tRowGetKeyEx(pRow, &rowKey);
    if ((rowKey.ts >= endKey && asc) || (rowKey.ts <= endKey && !asc)) {
      pRow = NULL;
    }
  }

  if (piiter->hasVal) {
    tRowGetKeyEx(piRow, &irowKey);
    if ((irowKey.ts >= endKey && asc) || (irowKey.ts <= endKey && !asc)) {
      piRow = NULL;
    }
  }

  if (pRow != NULL && piRow != NULL) {
    if (rowKey.numOfPKs == 0) {
      if ((rowKey.ts > irowKey.ts && asc) || (rowKey.ts < irowKey.ts && (!asc))) {  // ik.ts < k.ts
        code = doMergeMemTableMultiRows(piRow, &irowKey, uid, piiter, pDelList, pResRow, pReader, freeTSRow);
        TSDB_CHECK_CODE(code, lino, _end);
      } else if ((rowKey.ts < irowKey.ts && asc) || (rowKey.ts > irowKey.ts && (!asc))) {
        code = doMergeMemTableMultiRows(pRow, &rowKey, uid, piter, pDelList, pResRow, pReader, freeTSRow);
        TSDB_CHECK_CODE(code, lino, _end);
      } else {  // ik.ts == k.ts
        pResRow->type = TSDBROW_ROW_FMT;
        code = doMergeMemIMemRows(pRow, &rowKey, piRow, &irowKey, pBlockScanInfo, pReader, &pResRow->pTSRow);
        TSDB_CHECK_CODE(code, lino, _end);
        *freeTSRow = true;
      }
    } else {
      int32_t ret = pkCompEx(&rowKey, &irowKey);
      if (ret != 0) {
        if ((ret > 0 && asc) || (ret < 0 && (!asc))) {  // ik.ts < k.ts
          code = doMergeMemTableMultiRows(piRow, &irowKey, uid, piiter, pDelList, pResRow, pReader, freeTSRow);
          TSDB_CHECK_CODE(code, lino, _end);
        } else if ((ret < 0 && asc) || (ret > 0 && (!asc))) {
          code = doMergeMemTableMultiRows(pRow, &rowKey, uid, piter, pDelList, pResRow, pReader, freeTSRow);
          TSDB_CHECK_CODE(code, lino, _end);
        }
      } else {  // ik.ts == k.ts
        pResRow->type = TSDBROW_ROW_FMT;
        code = doMergeMemIMemRows(pRow, &rowKey, piRow, &irowKey, pBlockScanInfo, pReader, &pResRow->pTSRow);
        TSDB_CHECK_CODE(code, lino, _end);
        *freeTSRow = true;
      }
    }
  } else if (piter->hasVal && pRow != NULL) {
    code = doMergeMemTableMultiRows(pRow, &rowKey, uid, piter, pDelList, pResRow, pReader, freeTSRow);
    TSDB_CHECK_CODE(code, lino, _end);
  } else if (piiter->hasVal && piRow != NULL) {
    code = doMergeMemTableMultiRows(piRow, &irowKey, uid, piiter, pDelList, pResRow, pReader, freeTSRow);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doAppendRowFromTSRow(SSDataBlock* pBlock, STsdbReader* pReader, SRow* pTSRow, STableBlockScanInfo* pScanInfo) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  int32_t             outputRowIndex = 0;
  int64_t             uid = 0;
  SBlockLoadSuppInfo* pSupInfo = NULL;
  STSchema*           pSchema = NULL;
  SColVal             colVal = {0};
  int32_t             i = 0, j = 0;

  TSDB_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pTSRow, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pScanInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  outputRowIndex = pBlock->info.rows;
  uid = pScanInfo->uid;

  pSupInfo = &pReader->suppInfo;
  pSchema = doGetSchemaForTSRow(pTSRow->sver, pReader, uid);
  TSDB_CHECK_NULL(pSchema, code, lino, _end, terrno);

  if (pSupInfo->colId[i] == PRIMARYKEY_TIMESTAMP_COL_ID) {
    SColumnInfoData* pColData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
    TSDB_CHECK_NULL(pColData, code, lino, _end, TSDB_CODE_INVALID_PARA);

    ((int64_t*)pColData->pData)[outputRowIndex] = pTSRow->ts;
    i += 1;
  }

  while (i < pSupInfo->numOfCols && j < pSchema->numOfCols) {
    col_id_t colId = pSupInfo->colId[i];

    if (colId == pSchema->columns[j].colId) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
      TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);

      code = tRowGet(pTSRow, pSchema, j, &colVal);
      TSDB_CHECK_CODE(code, lino, _end);

      code = doCopyColVal(pColInfoData, outputRowIndex, i, &colVal, pSupInfo);
      TSDB_CHECK_CODE(code, lino, _end);
      i += 1;
      j += 1;
    } else if (colId < pSchema->columns[j].colId) {
      SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
      TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);

      colDataSetNULL(pColInfoData, outputRowIndex);
      i += 1;
    } else if (colId > pSchema->columns[j].colId) {
      j += 1;
    }
  }

  // set null value since current column does not exist in the "pSchema"
  while (i < pSupInfo->numOfCols) {
    SColumnInfoData* pColInfoData = taosArrayGet(pBlock->pDataBlock, pSupInfo->slotId[i]);
    TSDB_CHECK_NULL(pColInfoData, code, lino, _end, TSDB_CODE_INVALID_PARA);

    colDataSetNULL(pColInfoData, outputRowIndex);
    i += 1;
  }

  pBlock->info.dataLoad = 1;
  pBlock->info.rows += 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t doAppendRowFromFileBlock(SSDataBlock* pResBlock, STsdbReader* pReader, SBlockData* pBlockData,
                                 int32_t rowIndex) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  int32_t             i = 0, j = 0;
  int32_t             outputRowIndex = 0;
  SBlockLoadSuppInfo* pSupInfo = NULL;
  SColVal             cv = {0};
  int32_t             numOfInputCols = 0;
  int32_t             numOfOutputCols = 0;

  TSDB_CHECK_NULL(pResBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlockData, code, lino, _end, TSDB_CODE_INVALID_PARA);

  outputRowIndex = pResBlock->info.rows;

  pSupInfo = &pReader->suppInfo;
  ((int64_t*)pReader->status.pPrimaryTsCol->pData)[outputRowIndex] = pBlockData->aTSKEY[rowIndex];
  i += 1;

  numOfInputCols = pBlockData->nColData;
  numOfOutputCols = pSupInfo->numOfCols;

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
      TSDB_CHECK_CODE(code, lino, _end);
      j += 1;
    } else if (pData->cid > pCol->info.colId) {
      // the specified column does not exist in file block, fill with null data
      colDataSetNULL(pCol, outputRowIndex);
    }

    i += 1;
  }

  while (i < numOfOutputCols) {
    SColumnInfoData* pCol = taosArrayGet(pResBlock->pDataBlock, pSupInfo->slotId[i]);
    TSDB_CHECK_NULL(pCol, code, lino, _end, TSDB_CODE_INVALID_PARA);

    colDataSetNULL(pCol, outputRowIndex);
    i += 1;
  }

  pResBlock->info.dataLoad = 1;
  pResBlock->info.rows += 1;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t buildDataBlockFromBufImpl(STableBlockScanInfo* pBlockScanInfo, int64_t endKey, int32_t capacity,
                                  STsdbReader* pReader) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pBlock = pReader->resBlockInfo.pResBlock;

  do {
    TSDBROW row = {.type = -1};
    bool    freeTSRow = false;
    code = tsdbGetNextRowInMem(pBlockScanInfo, pReader, &row, endKey, &freeTSRow);
    TSDB_CHECK_CODE(code, lino, _end);

    if (row.type == -1) {
      break;
    }

    if (row.type == TSDBROW_ROW_FMT) {
      code = doAppendRowFromTSRow(pBlock, pReader, row.pTSRow, pBlockScanInfo);
      if (code != TSDB_CODE_SUCCESS) {
        if (freeTSRow) {
          taosMemoryFreeClear(row.pTSRow);
        }
        TSDB_CHECK_CODE(code, lino, _end);
      }
      pBlockScanInfo->lastProcKey.ts = row.pTSRow->ts;
      pBlockScanInfo->lastProcKey.numOfPKs = row.pTSRow->numOfPKs;
      if (row.pTSRow->numOfPKs > 0) {
        code = tRowGetPrimaryKeyDeepCopy(row.pTSRow, &pBlockScanInfo->lastProcKey);
        if (code != TSDB_CODE_SUCCESS) {
          if (freeTSRow) {
            taosMemoryFreeClear(row.pTSRow);
          }
          TSDB_CHECK_CODE(code, lino, _end);
        }
      }

      if (freeTSRow) {
        taosMemoryFreeClear(row.pTSRow);
      }
    } else {
      code = doAppendRowFromFileBlock(pBlock, pReader, row.pBlockData, row.iRow);
      TSDB_CHECK_CODE(code, lino, _end);

      tColRowGetKeyDeepCopy(row.pBlockData, row.iRow, pReader->suppInfo.pkSrcSlot, &pBlockScanInfo->lastProcKey);
    }

    // no data in buffer, return immediately
    if (!(pBlockScanInfo->iter.hasVal || pBlockScanInfo->iiter.hasVal)) {
      break;
    }

    if (pBlock->info.rows >= capacity) {
      break;
    }
  } while (1);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

// TODO refactor: with createDataBlockScanInfo
int32_t tsdbSetTableList2(STsdbReader* pReader, const void* pTableList, int32_t num) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  int32_t               size = 0;
  STableBlockScanInfo** p = NULL;
  STableUidList*        pUidList = NULL;
  int32_t               iter = 0;
  bool                  acquired = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  size = tSimpleHashGetSize(pReader->status.pTableMap);

  code = tsdbAcquireReader(pReader);
  TSDB_CHECK_CODE(code, lino, _end);
  acquired = true;

  while ((p = tSimpleHashIterate(pReader->status.pTableMap, p, &iter)) != NULL) {
    clearBlockScanInfo(*p);
  }

  tSimpleHashClear(pReader->status.pTableMap);

  if (size < num) {
    code = ensureBlockScanInfoBuf(&pReader->blockInfoBuf, num);
    TSDB_CHECK_CODE(code, lino, _end);

    char* p1 = taosMemoryRealloc(pReader->status.uidList.tableUidList, sizeof(uint64_t) * num);
    TSDB_CHECK_NULL(p1, code, lino, _end, terrno);

    pReader->status.uidList.tableUidList = (uint64_t*)p1;
  }

  pUidList = &pReader->status.uidList;
  pUidList->currentIndex = 0;

  STableKeyInfo* pList = (STableKeyInfo*)pTableList;
  for (int32_t i = 0; i < num; ++i) {
    pUidList->tableUidList[i] = pList[i].uid;

    STableBlockScanInfo* pInfo = NULL;
    code = getPosInBlockInfoBuf(&pReader->blockInfoBuf, i, &pInfo);
    TSDB_CHECK_CODE(code, lino, _end);

    code = initTableBlockScanInfo(pInfo, pList[i].uid, pReader->status.pTableMap, pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (acquired) {
    (void)tsdbReleaseReader(pReader);
  }
  return code;
}

uint64_t tsdbGetReaderMaxVersion2(STsdbReader* pReader) { return pReader->info.verRange.maxVer; }

static int32_t doOpenReaderImpl(STsdbReader* pReader) {
  int32_t         code = TSDB_CODE_SUCCESS;
  int32_t         lino = 0;
  SReaderStatus*  pStatus = NULL;
  SDataBlockIter* pBlockIter = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlockIter = &pStatus->blockIter;

  if (pReader->bFilesetDelimited) {
    getMemTableTimeRange(pReader, &pReader->status.memTableMaxKey, &pReader->status.memTableMinKey);
    pReader->status.bProcMemFirstFileset = true;
  }

  code = initFilesetIterator(&pStatus->fileIter, pReader->pReadSnap->pfSetArray, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  code = resetDataBlockIterator(&pStatus->blockIter, pReader->info.order, shouldFreePkBuf(&pReader->suppInfo),
                                pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pStatus->fileIter.numOfFiles == 0) {
    pStatus->loadFromFile = false;
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
  }

  if (!pStatus->loadFromFile) {
    code = resetTableListIndex(pStatus, pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static void freeSchemaFunc(void* param) {
  void** p = (void**)param;
  taosMemoryFreeClear(*p);
}

static void clearSharedPtr(STsdbReader* p) {
  if (p) {
    p->status.pTableMap = NULL;
    p->status.uidList.tableUidList = NULL;
    p->info.pSchema = NULL;
    p->pReadSnap = NULL;
    p->pSchemaMap = NULL;
  }
}

static int32_t setSharedPtr(STsdbReader* pDst, const STsdbReader* pSrc) {
  pDst->status.pTableMap = pSrc->status.pTableMap;
  pDst->status.uidList = pSrc->status.uidList;
  pDst->info.pSchema = pSrc->info.pSchema;
  pDst->pSchemaMap = pSrc->pSchemaMap;
  pDst->pReadSnap = pSrc->pReadSnap;
  pDst->pReadSnap->pfSetArray = pSrc->pReadSnap->pfSetArray;

  if (pDst->info.pSchema) {
    return tsdbRowMergerInit(&pDst->status.merger, pDst->info.pSchema);
  }

  return TSDB_CODE_SUCCESS;
}

// ====================================== EXPOSED APIs ======================================
int32_t tsdbReaderOpen2(void* pVnode, SQueryTableDataCond* pCond, void* pTableList, int32_t numOfTables,
                        SSDataBlock* pResBlock, void** ppReader, const char* idstr, SHashObj** pIgnoreTables) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  STimeWindow  window = {0};
  SVnodeCfg*   pConf = NULL;
  STsdbReader* pReader = NULL;
  int32_t      capacity = 0;

  window = pCond->twindows;
  pConf = &(((SVnode*)pVnode)->config);

  capacity = pConf->tsdbCfg.maxRows;
  if (pResBlock != NULL) {
    code = blockDataEnsureCapacity(pResBlock, capacity);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  code = tsdbReaderCreate(pVnode, pCond, ppReader, capacity, pResBlock, idstr);
  TSDB_CHECK_CODE(code, lino, _end);

  // check for query time window
  pReader = *ppReader;
  if (isEmptyQueryTimeWindow(&pReader->info.window) && pCond->type == TIMEWINDOW_RANGE_CONTAINED) {
    tsdbDebug("%p query window not overlaps with the data set, no result returned, %s", pReader, pReader->idStr);
    goto _end;
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
    TSDB_CHECK_CODE(code, lino, _end);

    if (order == TSDB_ORDER_ASC) {
      pCond->twindows.skey = window.ekey + 1;
      pCond->twindows.ekey = INT64_MAX;
    } else {
      pCond->twindows.skey = INT64_MIN;
      pCond->twindows.ekey = window.ekey - 1;
    }
    pCond->order = order;

    code = tsdbReaderCreate(pVnode, pCond, (void**)&((STsdbReader*)pReader)->innerReader[1], 1, pResBlock, idstr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // NOTE: the endVersion in pCond is the data version not schema version, so pCond->endVersion is not correct here.
  //  no valid error code set in metaGetTbTSchema, so let's set the error code here.
  //  we should proceed in case of tmq processing.
  if (pCond->suid != 0) {
    code = metaGetTbTSchemaMaybeNull(pReader->pTsdb->pVnode->pMeta, pReader->info.suid, -1, 1, &pReader->info.pSchema);
    TSDB_CHECK_CODE(code, lino, _end);
    if (pReader->info.pSchema == NULL) {
      tsdbWarn("failed to get table schema, suid:%" PRIu64 ", ver:-1, %s", pReader->info.suid, pReader->idStr);
    }
  } else if (numOfTables > 0) {
    STableKeyInfo* pKey = pTableList;
    code = metaGetTbTSchemaMaybeNull(pReader->pTsdb->pVnode->pMeta, pKey->uid, -1, 1, &pReader->info.pSchema);
    TSDB_CHECK_CODE(code, lino, _end);
    if (pReader->info.pSchema == NULL) {
      tsdbWarn("failed to get table schema, uid:%" PRIu64 ", ver:-1, %s", pKey->uid, pReader->idStr);
    }
  }

  if (pReader->info.pSchema != NULL) {
    code = tsdbRowMergerInit(&pReader->status.merger, pReader->info.pSchema);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  pReader->pSchemaMap = tSimpleHashInit(8, taosFastHash);
  if (pReader->pSchemaMap == NULL) {
    tsdbError("failed init schema hash for reader %s", pReader->idStr);
    TSDB_CHECK_NULL(pReader->pSchemaMap, code, lino, _end, terrno);
  }

  tSimpleHashSetFreeFp(pReader->pSchemaMap, freeSchemaFunc);
  if (pReader->info.pSchema != NULL) {
    code = updateBlockSMAInfo(pReader->info.pSchema, &pReader->suppInfo);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  STsdbReader* p = (pReader->innerReader[0] != NULL) ? pReader->innerReader[0] : pReader;

  code = createDataBlockScanInfo(p, &pReader->blockInfoBuf, pTableList, &pReader->status.uidList, numOfTables,
                                 &pReader->status.pTableMap);
  TSDB_CHECK_CODE(code, lino, _end);

  pReader->status.pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
  TSDB_CHECK_NULL(pReader->status.pLDataIterArray, code, lino, _end, terrno);

  pReader->flag = READER_STATUS_SUSPEND;
  pReader->info.execMode = pCond->notLoadData ? READER_EXEC_ROWS : READER_EXEC_DATA;

  pReader->pIgnoreTables = pIgnoreTables;
  tsdbDebug("%p total numOfTable:%d, window:%" PRId64 " - %" PRId64 ", verRange:%" PRId64 " - %" PRId64
            " in this query %s",
            pReader, numOfTables, pReader->info.window.skey, pReader->info.window.ekey, pReader->info.verRange.minVer,
            pReader->info.verRange.maxVer, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s, %s", __func__, lino, tstrerror(code), idstr);
    tsdbReaderClose2(*ppReader);
    *ppReader = NULL;  // reset the pointer value.
  }
  return code;
}

void tsdbReaderClose2(STsdbReader* pReader) {
  if (pReader == NULL) {
    return;
  }

  int32_t code = tsdbAcquireReader(pReader);
  if (code) {
    return;
  }

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

  if (pSupInfo->buildBuf) {
    for (int32_t i = 0; i < pSupInfo->numOfCols; ++i) {
      if (pSupInfo->buildBuf[i] != NULL) {
        taosMemoryFreeClear(pSupInfo->buildBuf[i]);
      }
    }
  }

  if (pReader->resBlockInfo.freeBlock) {
    blockDataDestroy(pReader->resBlockInfo.pResBlock);
    pReader->resBlockInfo.pResBlock = NULL;
  }

  taosMemoryFree(pSupInfo->colId);
  tBlockDataDestroy(&pReader->status.fileBlockData);
  cleanupDataBlockIterator(&pReader->status.blockIter, shouldFreePkBuf(&pReader->suppInfo));

  size_t numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  if (pReader->status.pTableMap != NULL) {
    destroyAllBlockScanInfo(&pReader->status.pTableMap);
  }
  clearBlockScanInfoBuf(&pReader->blockInfoBuf);

  if (pReader->pFileReader != NULL) {
    tsdbDataFileReaderClose(&pReader->pFileReader);
  }

  SReadCostSummary* pCost = &pReader->cost;
  SFilesetIter*     pFilesetIter = &pReader->status.fileIter;
  if (pFilesetIter->pSttBlockReader != NULL) {
    SSttBlockReader* pSttBlockReader = pFilesetIter->pSttBlockReader;
    tMergeTreeClose(&pSttBlockReader->mergeTree);

    clearRowKey(&pSttBlockReader->currentKey);
    taosMemoryFree(pSttBlockReader);
  }

  destroySttBlockReader(pReader->status.pLDataIterArray, &pCost->sttCost);
  pReader->status.pLDataIterArray = NULL;
  taosMemoryFreeClear(pReader->status.uidList.tableUidList);

  tsdbTrace("tsdb/reader-close: %p, untake snapshot", pReader);
  void* p = pReader->pReadSnap;
  if ((p == atomic_val_compare_exchange_ptr((void**)&pReader->pReadSnap, p, NULL)) && (p != NULL)) {
    tsdbUntakeReadSnap2(pReader, p, true);
    pReader->pReadSnap = NULL;
  }

  (void) tsem_destroy(&pReader->resumeAfterSuspend);
  (void) tsdbReleaseReader(pReader);
  (void) tsdbUninitReaderLock(pReader);

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
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  SReaderStatus*        pStatus = NULL;
  STableBlockScanInfo** p = NULL;

  TSDB_CHECK_NULL(pCurrentReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pCurrentReader->status;

  if (pStatus->loadFromFile) {
    tsdbDataFileReaderClose(&pCurrentReader->pFileReader);

    SReadCostSummary* pCost = &pCurrentReader->cost;
    destroySttBlockReader(pStatus->pLDataIterArray, &pCost->sttCost);
    pStatus->pLDataIterArray = taosArrayInit(4, POINTER_BYTES);
    TSDB_CHECK_NULL(pStatus->pLDataIterArray, code, lino, _end, terrno);
  }

  // resetDataBlockScanInfo excluding lastKey
  int32_t step = ASCENDING_TRAVERSE(pCurrentReader->info.order) ? 1 : -1;
  int32_t iter = 0;
  while ((p = tSimpleHashIterate(pStatus->pTableMap, p, &iter)) != NULL) {
    STableBlockScanInfo* pInfo = *(STableBlockScanInfo**)p;
    clearBlockScanInfo(pInfo);
    //    pInfo->sttKeyInfo.nextProcKey = pInfo->lastProcKey.ts + step;
    //    pInfo->sttKeyInfo.nextProcKey = pInfo->lastProcKey + step;
  }

  pStatus->uidList.currentIndex = 0;
  initReaderStatus(pStatus);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbReaderSuspend2(STsdbReader* pReader) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // save reader's base state & reset top state to be reconstructed from base state
  pReader->status.suspendInvoked = true;  // record the suspend status

  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    if (pReader->step == EXTERNAL_ROWS_PREV) {
      code = doSuspendCurrentReader(pReader->innerReader[0]);
    } else if (pReader->step == EXTERNAL_ROWS_MAIN) {
      code = doSuspendCurrentReader(pReader);
    } else {
      code = doSuspendCurrentReader(pReader->innerReader[1]);
    }
  } else {
    code = doSuspendCurrentReader(pReader);
  }

  // make sure only release once
  void* p = pReader->pReadSnap;
  TSDB_CHECK_NULL(p, code, lino, _end, TSDB_CODE_INVALID_PARA);
  if ((p == atomic_val_compare_exchange_ptr((void**)&pReader->pReadSnap, p, NULL)) && (p != NULL)) {
    tsdbUntakeReadSnap2(pReader, p, false);
    pReader->pReadSnap = NULL;
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t tsdbSetQueryReseek(void* pQHandle) {
  int32_t      code = 0;
  STsdbReader* pReader = pQHandle;

  code = tsdbTryAcquireReader(pReader);
  if (code == 0) {
    if (pReader->flag == READER_STATUS_SUSPEND) {
      code = tsdbReleaseReader(pReader);
      return code;
    }

    code = tsdbReaderSuspend2(pReader);
    (void)tsdbReleaseReader(pReader);
    return code;
  } else if (code == EBUSY) {
    return TSDB_CODE_VND_QUERY_BUSY;
  } else {
    terrno = TAOS_SYSTEM_ERROR(code);
    return TSDB_CODE_FAILED;
  }
}

int32_t tsdbReaderResume2(STsdbReader* pReader) {
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               lino = 0;
  STableBlockScanInfo** pBlockScanInfo = NULL;
  int32_t               numOfTables = 0;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pBlockScanInfo = pReader->status.pTableIter;

  //  restore reader's state, task snapshot
  numOfTables = tSimpleHashGetSize(pReader->status.pTableMap);
  if (numOfTables > 0) {
    tsdbTrace("tsdb/reader: %p, take snapshot", pReader);
    code = tsdbTakeReadSnap2(pReader, tsdbSetQueryReseek, &pReader->pReadSnap, pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);

    // open reader failure may cause the flag still to be READER_STATUS_SUSPEND, which may cause suspend reader failure.
    // So we need to set it A.S.A.P
    pReader->flag = READER_STATUS_NORMAL;

    if (pReader->type == TIMEWINDOW_RANGE_CONTAINED) {
      code = doOpenReaderImpl(pReader);
      TSDB_CHECK_CODE(code, lino, _end);
    } else {
      STsdbReader* pPrevReader = pReader->innerReader[0];
      STsdbReader* pNextReader = pReader->innerReader[1];

      // we need only one row
      pPrevReader->resBlockInfo.capacity = 1;
      code = setSharedPtr(pPrevReader, pReader);
      TSDB_CHECK_CODE(code, lino, _end);

      pNextReader->resBlockInfo.capacity = 1;
      code = setSharedPtr(pNextReader, pReader);
      TSDB_CHECK_CODE(code, lino, _end);

      if (pReader->step == 0 || pReader->step == EXTERNAL_ROWS_PREV) {
        code = doOpenReaderImpl(pPrevReader);
        TSDB_CHECK_CODE(code, lino, _end);
      } else if (pReader->step == EXTERNAL_ROWS_MAIN) {
        code = doOpenReaderImpl(pReader);
        TSDB_CHECK_CODE(code, lino, _end);
      } else {
        code = doOpenReaderImpl(pNextReader);
        TSDB_CHECK_CODE(code, lino, _end);
      }
    }
  }

  tsdbDebug("reader: %p resumed uid %" PRIu64 ", numOfTable:%" PRId32 ", in this query %s", pReader,
            pBlockScanInfo ? (*pBlockScanInfo)->uid : 0, numOfTables, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s, %s", __func__, lino, tstrerror(code),
              (pReader && pReader->idStr) ? pReader->idStr : "");
  }
  return code;
}

static int32_t buildFromPreFilesetBuffer(STsdbReader* pReader) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  SSDataBlock*   pBlock = NULL;
  int32_t        fid = 0;
  STimeWindow    win = {0};

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlock = pReader->resBlockInfo.pResBlock;
  fid = pReader->status.pCurrentFileset->fid;
  tsdbFidKeyRange(fid, pReader->pTsdb->keepCfg.days, pReader->pTsdb->keepCfg.precision, &win.skey, &win.ekey);

  int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? win.skey : win.ekey;
  code = buildBlockFromBufferSeqForPreFileset(pReader, endKey);
  TSDB_CHECK_CODE(code, lino, _end);
  if (pBlock->info.rows <= 0) {
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

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doTsdbNextDataBlockFilesetDelimited(STsdbReader* pReader) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  SSDataBlock*   pBlock = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlock = pReader->resBlockInfo.pResBlock;

  if (pStatus->loadFromFile) {
    if (pStatus->bProcMemPreFileset) {
      code = buildFromPreFilesetBuffer(pReader);
      TSDB_CHECK_CODE(code, lino, _end);
      if (pBlock->info.rows > 0) {
        goto _end;
      }
    }

    code = buildBlockFromFiles(pReader);
    TSDB_CHECK_CODE(code, lino, _end);

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
      code = resetTableListIndex(&pReader->status, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);

      int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
      code = buildBlockFromBufferSequentially(pReader, endKey);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  } else {  // no data in files, let's try the buffer
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
    code = buildBlockFromBufferSequentially(pReader, endKey);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doTsdbNextDataBlockFilesFirst(STsdbReader* pReader) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  SSDataBlock*   pBlock = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  pBlock = pReader->resBlockInfo.pResBlock;

  if (pStatus->loadFromFile) {
    code = buildBlockFromFiles(pReader);
    TSDB_CHECK_CODE(code, lino, _end);

    if (pBlock->info.rows <= 0) {
      code = resetTableListIndex(&pReader->status, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);

      int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
      code = buildBlockFromBufferSequentially(pReader, endKey);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  } else {  // no data in files, let's try the buffer
    int64_t endKey = (ASCENDING_TRAVERSE(pReader->info.order)) ? INT64_MAX : INT64_MIN;
    code = buildBlockFromBufferSequentially(pReader, endKey);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doTsdbNextDataBlock2(STsdbReader* pReader, bool* hasNext) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  SSDataBlock* pBlock = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  // cleanup the data that belongs to the previous data block
  pBlock = pReader->resBlockInfo.pResBlock;
  blockDataCleanup(pBlock);

  *hasNext = false;

  SReaderStatus* pStatus = &pReader->status;
  if (tSimpleHashGetSize(pStatus->pTableMap) == 0) {
    goto _end;
  }

  if (!pReader->bFilesetDelimited) {
    code = doTsdbNextDataBlockFilesFirst(pReader);
  } else {
    code = doTsdbNextDataBlockFilesetDelimited(pReader);
  }

  *hasNext = pBlock->info.rows > 0;
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbNextDataBlock2(STsdbReader* pReader, bool* hasNext) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  bool           acquired = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(hasNext, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *hasNext = false;

  code = pReader->code;
  TSDB_CHECK_CODE(code, lino, _end);

  if (isEmptyQueryTimeWindow(&pReader->info.window) || pReader->step == EXTERNAL_ROWS_NEXT) {
    goto _end;
  }

  pStatus = &pReader->status;

  // NOTE: the following codes is used to perform test for suspend/resume for tsdbReader when it blocks the commit
  // the data should be ingested in round-robin and all the child tables should be createted before ingesting data
  // the version range of query will be used to identify the correctness of suspend/resume functions.
  // this function will be blocked before loading the SECOND block from vnode-buffer, and restart itself from sst-files
#if SUSPEND_RESUME_TEST
  if (!pReader->status.suspendInvoked && !pReader->status.loadFromFile) {
    tsem_wait(&pReader->resumeAfterSuspend);
  }
#endif

  code = tsdbAcquireReader(pReader);
  TSDB_CHECK_CODE(code, lino, _end);
  acquired = true;

  tsdbTrace("tsdb/read: %p, take read mutex, code: %d", pReader, code);

  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pReader->innerReader[0] != NULL && pReader->step == 0) {
    code = doTsdbNextDataBlock2(pReader->innerReader[0], hasNext);
    TSDB_CHECK_CODE(code, lino, _end);

    pReader->step = EXTERNAL_ROWS_PREV;
    if (*hasNext) {
      pStatus = &pReader->innerReader[0]->status;
      if (pStatus->composedDataBlock) {
        tsdbTrace("tsdb/read: %p, unlock read mutex", pReader);
        code = tsdbReleaseReader(pReader);
        acquired = false;
        TSDB_CHECK_CODE(code, lino, _end);
      }

      goto _end;
    }
  }

  if (pReader->step == EXTERNAL_ROWS_PREV) {
    // prepare for the main scan
    if (tSimpleHashGetSize(pReader->status.pTableMap) > 0) {
      code = doOpenReaderImpl(pReader);
    }

    int32_t step = 1;
    resetAllDataBlockScanInfo(pReader->status.pTableMap, pReader->innerReader[0]->info.window.ekey, step);
    TSDB_CHECK_CODE(code, lino, _end);

    pReader->step = EXTERNAL_ROWS_MAIN;
  }

  code = doTsdbNextDataBlock2(pReader, hasNext);
  TSDB_CHECK_CODE(code, lino, _end);

  if (*hasNext) {
    if (pStatus->composedDataBlock) {
      tsdbTrace("tsdb/read: %p, unlock read mutex", pReader);
      code = tsdbReleaseReader(pReader);
      acquired = false;
      TSDB_CHECK_CODE(code, lino, _end);
    }
    goto _end;
  }

  if (pReader->step == EXTERNAL_ROWS_MAIN && pReader->innerReader[1] != NULL) {
    // prepare for the next row scan
    if (tSimpleHashGetSize(pReader->status.pTableMap) > 0) {
      code = doOpenReaderImpl(pReader->innerReader[1]);
    }

    int32_t step = -1;
    resetAllDataBlockScanInfo(pReader->innerReader[1]->status.pTableMap, pReader->info.window.ekey, step);
    TSDB_CHECK_CODE(code, lino, _end);

    code = doTsdbNextDataBlock2(pReader->innerReader[1], hasNext);
    TSDB_CHECK_CODE(code, lino, _end);

    pReader->step = EXTERNAL_ROWS_NEXT;
    if (*hasNext) {
      pStatus = &pReader->innerReader[1]->status;
      if (pStatus->composedDataBlock) {
        tsdbTrace("tsdb/read: %p, unlock read mutex", pReader);
        code = tsdbReleaseReader(pReader);
        acquired = false;
        TSDB_CHECK_CODE(code, lino, _end);
      }

      goto _end;
    }
  }

  tsdbTrace("tsdb/read: %p, unlock read mutex", pReader);
  code = tsdbReleaseReader(pReader);
  acquired = false;
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (acquired) {
    tsdbTrace("tsdb/read: %p, unlock read mutex", pReader);
    (void)tsdbReleaseReader(pReader);
  }
  return code;
}

static int32_t doFillNullColSMA(SBlockLoadSuppInfo* pSup, int32_t numOfRows, int32_t numOfCols,
                                SColumnDataAgg* pTsAgg) {
  // do fill all null column value SMA info
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  int32_t i = 0, j = 0;
  int32_t size = 0;

  TSDB_CHECK_NULL(pSup, code, lino, _end, TSDB_CODE_INVALID_PARA);

  size = (int32_t)TARRAY2_SIZE(&pSup->colAggArray);
  code = TARRAY2_INSERT_PTR(&pSup->colAggArray, 0, pTsAgg);
  TSDB_CHECK_CODE(code, lino, _end);

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
        TSDB_CHECK_CODE(code, lino, _end);

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
      TSDB_CHECK_CODE(code, lino, _end);

      i += 1;
    }
    j++;
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbRetrieveDatablockSMA2(STsdbReader* pReader, SSDataBlock* pDataBlock, bool* allHave, bool* hasNullSMA) {
  int32_t             code = TSDB_CODE_SUCCESS;
  int32_t             lino = 0;
  SColumnDataAgg**    pBlockSMA = NULL;
  SFileDataBlockInfo* pBlockInfo = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pDataBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(allHave, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pBlockSMA = &pDataBlock->pBlockAgg;
  *allHave = false;
  *pBlockSMA = NULL;

  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    goto _end;
  }

  // there is no statistics data for composed block
  if (pReader->status.composedDataBlock || (!pReader->suppInfo.smaValid)) {
    goto _end;
  }

  code = getCurrentBlockInfo(&pReader->status.blockIter, &pBlockInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  SBlockLoadSuppInfo* pSup = &pReader->suppInfo;

  SSDataBlock* pResBlock = pReader->resBlockInfo.pResBlock;
  if (pResBlock->info.id.uid != pBlockInfo->uid) {
    goto _end;
  }

  //  int64_t st = taosGetTimestampUs();
  TARRAY2_CLEAR(&pSup->colAggArray, 0);

  SBrinRecord pRecord;
  blockInfoToRecord(&pRecord, pBlockInfo, pSup);
  code = tsdbDataFileReadBlockSma(pReader->pFileReader, &pRecord, &pSup->colAggArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbDebug("vgId:%d, failed to load block SMA for uid %" PRIu64 ", code:%s, %s", 0, pBlockInfo->uid, tstrerror(code),
              pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (pSup->colAggArray.size > 0) {
    *allHave = true;
  } else {
    *pBlockSMA = NULL;
    goto _end;
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
    pResBlock->pBlockAgg = taosMemoryCalloc(num, sizeof(SColumnDataAgg));
    TSDB_CHECK_NULL(pResBlock->pBlockAgg, code, lino, _end, terrno);
    for (int i = 0; i < num; ++i) {
      pResBlock->pBlockAgg[i].colId = -1;
    }
  }

  // do fill all null column value SMA info
  code = doFillNullColSMA(pSup, pBlockInfo->numRow, numOfCols, pTsAgg);
  TSDB_CHECK_CODE(code, lino, _end);

  size_t size = pSup->colAggArray.size;

  int32_t i = 0, j = 0;
  while (j < numOfCols && i < size) {
    SColumnDataAgg* pAgg = &pSup->colAggArray.data[i];
    if (pAgg->colId == pSup->colId[j]) {
      pResBlock->pBlockAgg[pSup->slotId[j]] = *pAgg;
      i += 1;
      j += 1;
    } else if (pAgg->colId < pSup->colId[j]) {
      i += 1;
    } else if (pSup->colId[j] < pAgg->colId) {
      pResBlock->pBlockAgg[pSup->slotId[j]].colId = -1;
      *allHave = false;
      j += 1;
    }
  }

  *pBlockSMA = pResBlock->pBlockAgg;
  pReader->cost.smaDataLoad += 1;

  //  double elapsedTime = (taosGetTimestampUs() - st) / 1000.0;
  pReader->cost.smaLoadTime += 0;  // elapsedTime;

  tsdbDebug("vgId:%d, succeed to load block SMA for uid %" PRIu64 ", %s", 0, pBlockInfo->uid, pReader->idStr);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doRetrieveDataBlock(STsdbReader* pReader, SSDataBlock** pBlock) {
  int32_t              code = TSDB_CODE_SUCCESS;
  int32_t              lino = 0;
  SReaderStatus*       pStatus = NULL;
  SFileDataBlockInfo*  pBlockInfo = NULL;
  STableBlockScanInfo* pBlockScanInfo = NULL;
  bool                 reset = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pStatus = &pReader->status;
  *pBlock = NULL;

  code = getCurrentBlockInfo(&pStatus->blockIter, &pBlockInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  code = pReader->code;
  TSDB_CHECK_CODE(code, lino, _end);

  code = getTableBlockScanInfo(pStatus->pTableMap, pBlockInfo->uid, &pBlockScanInfo, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  reset = true;
  code = doLoadFileBlockData(pReader, &pStatus->blockIter, &pStatus->fileBlockData, pBlockScanInfo->uid);
  TSDB_CHECK_CODE(code, lino, _end);

  code = copyBlockDataToSDataBlock(pReader, &pBlockScanInfo->lastProcKey);
  TSDB_CHECK_CODE(code, lino, _end);

  *pBlock = pReader->resBlockInfo.pResBlock;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    if (reset) {
      tBlockDataReset(&pStatus->fileBlockData);
    }
  }
  return code;
}

int32_t tsdbRetrieveDataBlock2(STsdbReader* pReader, SSDataBlock** pBlock, SArray* pIdList) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  STsdbReader* pTReader = NULL;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(pBlock, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *pBlock = NULL;

  pTReader = pReader;
  if (pReader->type == TIMEWINDOW_RANGE_EXTERNAL) {
    if (pReader->step == EXTERNAL_ROWS_PREV) {
      pTReader = pReader->innerReader[0];
    } else if (pReader->step == EXTERNAL_ROWS_NEXT) {
      pTReader = pReader->innerReader[1];
    }
  }

  SReaderStatus* pStatus = &pTReader->status;
  if (pStatus->composedDataBlock || pReader->info.execMode == READER_EXEC_ROWS) {
    //    tsdbReaderSuspend2(pReader);
    //    tsdbReaderResume2(pReader);
    *pBlock = pTReader->resBlockInfo.pResBlock;
    goto _end;
  }

  code = doRetrieveDataBlock(pTReader, pBlock);

  tsdbTrace("tsdb/read-retrieve: %p, unlock read mutex", pReader);
  (void) tsdbReleaseReader(pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  //  tsdbReaderSuspend2(pReader);
  //  tsdbReaderResume2(pReader);
_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t tsdbReaderReset2(STsdbReader* pReader, SQueryTableDataCond* pCond) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  bool    acquired = false;

  tsdbTrace("tsdb/reader-reset: %p, take read mutex", pReader);
  code = tsdbAcquireReader(pReader);
  TSDB_CHECK_CODE(code, lino, _end);
  acquired = true;

  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  if (isEmptyQueryTimeWindow(&pReader->info.window) || pReader->pReadSnap == NULL) {
    tsdbDebug("tsdb reader reset return %p, %s", pReader->pReadSnap, pReader->idStr);
    code = tsdbReleaseReader(pReader);
    acquired = false;
    TSDB_CHECK_CODE(code, lino, _end);
    goto _end;
  }

  SReaderStatus*  pStatus = &pReader->status;
  SDataBlockIter* pBlockIter = &pStatus->blockIter;

  TSDB_CHECK_NULL(pCond, code, lino, _end, TSDB_CODE_INVALID_PARA);
  pReader->info.order = pCond->order;
  pReader->type = TIMEWINDOW_RANGE_CONTAINED;
  code = updateQueryTimeWindow(pReader->pTsdb, &pCond->twindows, &pReader->info.window);
  TSDB_CHECK_CODE(code, lino, _end);
  pStatus->loadFromFile = true;
  pStatus->pTableIter = NULL;

  // allocate buffer in order to load data blocks from file
  memset(&pReader->suppInfo.tsColAgg, 0, sizeof(SColumnDataAgg));

  pReader->suppInfo.tsColAgg.colId = PRIMARYKEY_TIMESTAMP_COL_ID;
  tsdbDataFileReaderClose(&pReader->pFileReader);

  int32_t numOfTables = tSimpleHashGetSize(pStatus->pTableMap);

  code = initFilesetIterator(&pStatus->fileIter, pReader->pReadSnap->pfSetArray, pReader);
  TSDB_CHECK_CODE(code, lino, _end);

  code = resetDataBlockIterator(pBlockIter, pReader->info.order, shouldFreePkBuf(&pReader->suppInfo), pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  code = resetTableListIndex(&pReader->status, pReader->idStr);
  TSDB_CHECK_CODE(code, lino, _end);

  bool    asc = ASCENDING_TRAVERSE(pReader->info.order);
  int32_t step = asc ? 1 : -1;
  int64_t ts = 0;
  if (asc) {
    ts = (pReader->info.window.skey > INT64_MIN) ? pReader->info.window.skey - 1 : pReader->info.window.skey;
  } else {
    ts = (pReader->info.window.ekey < INT64_MAX) ? pReader->info.window.ekey + 1 : pReader->info.window.ekey;
  }
  resetAllDataBlockScanInfo(pStatus->pTableMap, ts, step);

  // no data in files, let's try buffer in memory
  if (pStatus->fileIter.numOfFiles == 0) {
    pStatus->loadFromFile = false;
    code = resetTableListIndex(pStatus, pReader->idStr);
    TSDB_CHECK_CODE(code, lino, _end);
  } else {
    code = initForFirstBlockInFile(pReader, pBlockIter);
    if (code != TSDB_CODE_SUCCESS) {
      tsdbError("%p reset reader failed, numOfTables:%d, query range:%" PRId64 " - %" PRId64 " in query %s", pReader,
                numOfTables, pReader->info.window.skey, pReader->info.window.ekey, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);
    }
  }

  tsdbDebug("%p reset reader, suid:%" PRIu64 ", numOfTables:%d, skey:%" PRId64 ", query range:%" PRId64 " - %" PRId64
            " in query %s",
            pReader, pReader->info.suid, numOfTables, pCond->twindows.skey, pReader->info.window.skey,
            pReader->info.window.ekey, pReader->idStr);

  code = tsdbReleaseReader(pReader);
  acquired = false;
  TSDB_CHECK_CODE(code, lino, _end);

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (acquired) {
    (void)tsdbReleaseReader(pReader);
  }
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
  int32_t       lino = 0;
  const int32_t numOfBuckets = 20.0;
  bool          acquired = false;

  TSDB_CHECK_NULL(pTableBlockInfo, code, lino, _end, TSDB_CODE_INVALID_PARA);

  pTableBlockInfo->totalSize = 0;
  pTableBlockInfo->totalRows = 0;
  pTableBlockInfo->numOfVgroups = 1;

  // find the start data block in file
  code = tsdbAcquireReader(pReader);
  TSDB_CHECK_CODE(code, lino, _end);
  acquired = true;

  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    TSDB_CHECK_CODE(code, lino, _end);
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
      SFileDataBlockInfo* pBlockInfo = NULL;
      code = getCurrentBlockInfo(pBlockIter, &pBlockInfo, pReader->idStr);
      TSDB_CHECK_CODE(code, lino, _end);

      int32_t numOfRows = pBlockInfo->numRow;

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

      hasNext = blockIteratorNext(&pStatus->blockIter);
    } else {
      code = initForFirstBlockInFile(pReader, pBlockIter);
      TSDB_CHECK_CODE(code, lino, _end);
      if (pStatus->loadFromFile == false) {
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
_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (acquired) {
    (void)tsdbReleaseReader(pReader);
  }
  return code;
}

static void getMemTableTimeRange(STsdbReader* pReader, int64_t* pMaxKey, int64_t* pMinKey) {
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

int64_t tsdbGetNumOfRowsInMemTable2(STsdbReader* pReader, uint32_t* rows) {
  int32_t        code = TSDB_CODE_SUCCESS;
  int32_t        lino = 0;
  SReaderStatus* pStatus = NULL;
  bool           acquired = false;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);
  TSDB_CHECK_NULL(rows, code, lino, _end, TSDB_CODE_INVALID_PARA);

  *rows = 0;
  pStatus = &pReader->status;
  code = tsdbAcquireReader(pReader);
  TSDB_CHECK_CODE(code, lino, _end);
  acquired = true;

  if (pReader->flag == READER_STATUS_SUSPEND) {
    code = tsdbReaderResume2(pReader);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  int32_t iter = 0;
  pStatus->pTableIter = tSimpleHashIterate(pStatus->pTableMap, NULL, &iter);

  while (pStatus->pTableIter != NULL) {
    STableBlockScanInfo* pBlockScanInfo = *(STableBlockScanInfo**)pStatus->pTableIter;

    STbData* d = NULL;
    if (pReader->pReadSnap->pMem != NULL) {
      d = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pMem, pReader->info.suid, pBlockScanInfo->uid);
      if (d != NULL) {
        *rows += tsdbGetNRowsInTbData(d);
      }
    }

    STbData* di = NULL;
    if (pReader->pReadSnap->pIMem != NULL) {
      di = tsdbGetTbDataFromMemTable(pReader->pReadSnap->pIMem, pReader->info.suid, pBlockScanInfo->uid);
      if (di != NULL) {
        *rows += tsdbGetNRowsInTbData(di);
      }
    }

    // current table is exhausted, let's try the next table
    pStatus->pTableIter = tSimpleHashIterate(pStatus->pTableMap, pStatus->pTableIter, &iter);
  }

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  if (acquired) {
    (void)tsdbReleaseReader(pReader);
  }
  return code;
}

int32_t tsdbGetTableSchema(SMeta* pMeta, int64_t uid, STSchema** pSchema, int64_t* suid) {
  SMetaReader mr = {0};
  metaReaderDoInit(&mr, pMeta, META_READER_LOCK);
  int32_t code = metaReaderGetTableEntryByUidCache(&mr, uid);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_TDB_INVALID_TABLE_ID;
    metaReaderClear(&mr);
    return code;
  }

  *suid = 0;

  // only child table and ordinary table is allowed, super table is not allowed.
  if (mr.me.type == TSDB_CHILD_TABLE) {
    tDecoderClear(&mr.coder);
    *suid = mr.me.ctbEntry.suid;
    code = metaReaderGetTableEntryByUidCache(&mr, *suid);
    if (code != TSDB_CODE_SUCCESS) {
      code = TSDB_CODE_TDB_INVALID_TABLE_ID;
      metaReaderClear(&mr);
      return code;
    }
  } else if (mr.me.type == TSDB_NORMAL_TABLE) {  // do nothing
  } else {
    code = TSDB_CODE_INVALID_PARA;
    tsdbError("invalid mr.me.type:%d, code:%s", mr.me.type, tstrerror(code));
    metaReaderClear(&mr);
    return code;
  }

  metaReaderClear(&mr);

  // get the newest table schema version
  code = metaGetTbTSchemaEx(pMeta, *suid, uid, -1, pSchema);
  return code;
}

int32_t tsdbTakeReadSnap2(STsdbReader* pReader, _query_reseek_func_t reseek, STsdbReadSnap** ppSnap, const char* id) {
  int32_t        code = 0;
  STsdb*         pTsdb = pReader->pTsdb;
  SVersionRange* pRange = &pReader->info.verRange;
  int32_t        lino = 0;
  *ppSnap = NULL;

  // lock
  code = taosThreadMutexLock(&pTsdb->mutex);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed to lock tsdb, code:%s", id, tstrerror(code));
    return code;
  }

  // alloc
  STsdbReadSnap* pSnap = (STsdbReadSnap*)taosMemoryCalloc(1, sizeof(STsdbReadSnap));
  if (pSnap == NULL) {
    (void) taosThreadMutexUnlock(&pTsdb->mutex);
    TSDB_CHECK_NULL(pSnap, code, lino, _exit, terrno);
  }

  // take snapshot
  if (pTsdb->mem && (pRange->minVer <= pTsdb->mem->maxVer && pRange->maxVer >= pTsdb->mem->minVer)) {
    pSnap->pMem = pTsdb->mem;
    pSnap->pNode = taosMemoryMalloc(sizeof(*pSnap->pNode));
    if (pSnap->pNode == NULL) {
      (void) taosThreadMutexUnlock(&pTsdb->mutex);
      TSDB_CHECK_NULL(pSnap->pNode, code, lino, _exit, terrno);
    }

    pSnap->pNode->pQHandle = pReader;
    pSnap->pNode->reseek = reseek;

    code = tsdbRefMemTable(pTsdb->mem, pSnap->pNode);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pTsdb->imem && (pRange->minVer <= pTsdb->imem->maxVer && pRange->maxVer >= pTsdb->imem->minVer)) {
    pSnap->pIMem = pTsdb->imem;
    pSnap->pINode = taosMemoryMalloc(sizeof(*pSnap->pINode));
    if (pSnap->pINode == NULL) {
      code = terrno;

      if (pTsdb->mem && pSnap->pNode) {
        tsdbUnrefMemTable(pTsdb->mem, pSnap->pNode, true);  // unref the previous refed mem
      }

      (void) taosThreadMutexUnlock(&pTsdb->mutex);
      goto _exit;
    }

    pSnap->pINode->pQHandle = pReader;
    pSnap->pINode->reseek = reseek;

    code = tsdbRefMemTable(pTsdb->imem, pSnap->pINode);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // fs
  code = tsdbFSCreateRefSnapshotWithoutLock(pTsdb->pFS, &pSnap->pfSetArray);
  if (code) {
    if (pSnap->pNode) {
      tsdbUnrefMemTable(pTsdb->mem, pSnap->pNode, true);  // unref the previous refed mem
    }

    if (pSnap->pINode) {
      tsdbUnrefMemTable(pTsdb->imem, pSnap->pINode, true);
    }

    (void) taosThreadMutexUnlock(&pTsdb->mutex);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // unlock
  (void) taosThreadMutexUnlock(&pTsdb->mutex);
  *ppSnap = pSnap;

  tsdbTrace("%s vgId:%d, take read snapshot", id, TD_VID(pTsdb->pVnode));
  return code;

_exit:
  tsdbError("%s vgId:%d take read snapshot failed, line:%d code:%s", id, TD_VID(pTsdb->pVnode), lino, tstrerror(code));

  if (pSnap) {
    if (pSnap->pNode) taosMemoryFree(pSnap->pNode);
    if (pSnap->pINode) taosMemoryFree(pSnap->pINode);
    taosMemoryFree(pSnap);
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
int32_t tsdbReaderSetId(void* p, const char* idstr) {
  int32_t      code = TSDB_CODE_SUCCESS;
  int32_t      lino = 0;
  STsdbReader* pReader = (STsdbReader*)p;

  TSDB_CHECK_NULL(pReader, code, lino, _end, TSDB_CODE_INVALID_PARA);

  taosMemoryFreeClear(pReader->idStr);
  pReader->idStr = taosStrdup(idstr);
  TSDB_CHECK_NULL(pReader->idStr, code, lino, _end, terrno);

  pReader->status.fileIter.pSttBlockReader->mergeTree.idStr = pReader->idStr;

_end:
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

void tsdbReaderSetCloseFlag(STsdbReader* pReader) { /*pReader->code = TSDB_CODE_TSC_QUERY_CANCELLED;*/ }

void tsdbSetFilesetDelimited(STsdbReader* pReader) { pReader->bFilesetDelimited = true; }

void tsdbReaderSetNotifyCb(STsdbReader* pReader, TsdReaderNotifyCbFn notifyFn, void* param) {
  pReader->notifyFn = notifyFn;
  pReader->notifyParam = param;
}
