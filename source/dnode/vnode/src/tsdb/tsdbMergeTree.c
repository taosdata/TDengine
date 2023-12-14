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
#include "tsdbFSet2.h"
#include "tsdbUtil2.h"
#include "tsdbMerge.h"
#include "tsdbReadUtil.h"
#include "tsdbSttFileRW.h"

static void tLDataIterClose2(SLDataIter *pIter);

// SLDataIter =================================================
SSttBlockLoadInfo *tCreateSttBlockLoadInfo(STSchema *pSchema, int16_t *colList, int32_t numOfCols) {
  SSttBlockLoadInfo *pLoadInfo = taosMemoryCalloc(1, sizeof(SSttBlockLoadInfo));
  if (pLoadInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pLoadInfo->blockData[0].sttBlockIndex = -1;
  pLoadInfo->blockData[1].sttBlockIndex = -1;

  pLoadInfo->currentLoadBlockIndex = 1;

  int32_t code = tBlockDataCreate(&pLoadInfo->blockData[0].data);
  if (code) {
    terrno = code;
  }

  code = tBlockDataCreate(&pLoadInfo->blockData[1].data);
  if (code) {
    terrno = code;
  }

  pLoadInfo->aSttBlk = taosArrayInit(4, sizeof(SSttBlk));
  pLoadInfo->pSchema = pSchema;
  pLoadInfo->colIds = colList;
  pLoadInfo->numOfCols = numOfCols;

  return pLoadInfo;
}

void *destroySttBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo) {
  if (pLoadInfo == NULL) {
    return NULL;
  }

  pLoadInfo->currentLoadBlockIndex = 1;

  SBlockDataInfo* pInfo = &pLoadInfo->blockData[0];
  tBlockDataDestroy(&pInfo->data);
  pInfo->sttBlockIndex = -1;
  pInfo->pin = false;

  pInfo = &pLoadInfo->blockData[1];
  tBlockDataDestroy(&pInfo->data);
  pInfo->sttBlockIndex = -1;
  pInfo->pin = false;

  if (pLoadInfo->info.pCount != NULL) {
    taosArrayDestroy(pLoadInfo->info.pUid);
    taosArrayDestroy(pLoadInfo->info.pFirstKey);
    taosArrayDestroy(pLoadInfo->info.pLastKey);
    taosArrayDestroy(pLoadInfo->info.pCount);
  }

  taosArrayDestroy(pLoadInfo->aSttBlk);
  taosMemoryFree(pLoadInfo);
  return NULL;
}

void destroyLDataIter(SLDataIter *pIter) {
  tLDataIterClose2(pIter);
  destroySttBlockLoadInfo(pIter->pBlockLoadInfo);
  taosMemoryFree(pIter);
}

void *destroySttBlockReader(SArray *pLDataIterArray, SSttBlockLoadCostInfo* pLoadCost) {
  if (pLDataIterArray == NULL) {
    return NULL;
  }

  int32_t numOfLevel = taosArrayGetSize(pLDataIterArray);
  for (int32_t i = 0; i < numOfLevel; ++i) {
    SArray *pList = taosArrayGetP(pLDataIterArray, i);
    for (int32_t j = 0; j < taosArrayGetSize(pList); ++j) {
      SLDataIter *pIter = taosArrayGetP(pList, j);
      if (pLoadCost != NULL) {
        pLoadCost->loadBlocks += pIter->pBlockLoadInfo->cost.loadBlocks;
        pLoadCost->loadStatisBlocks += pIter->pBlockLoadInfo->cost.loadStatisBlocks;
        pLoadCost->blockElapsedTime += pIter->pBlockLoadInfo->cost.blockElapsedTime;
        pLoadCost->statisElapsedTime += pIter->pBlockLoadInfo->cost.statisElapsedTime;
      }

      destroyLDataIter(pIter);
    }
    taosArrayDestroy(pList);
  }

  taosArrayDestroy(pLDataIterArray);
  return NULL;
}

// choose the unpinned slot to load next data block
static void updateBlockLoadSlot(SSttBlockLoadInfo* pLoadInfo) {
  int32_t nextSlotIndex = pLoadInfo->currentLoadBlockIndex ^ 1;
  if (pLoadInfo->blockData[nextSlotIndex].pin) {
    nextSlotIndex = nextSlotIndex ^ 1;
  }

  pLoadInfo->currentLoadBlockIndex = nextSlotIndex;
}

static SBlockData *loadLastBlock(SLDataIter *pIter, const char *idStr) {
  int32_t code = 0;

  SSttBlockLoadInfo *pInfo = pIter->pBlockLoadInfo;
  if (pInfo->blockData[0].sttBlockIndex == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 0) {
      tsdbDebug("current load index is set to 0, block index:%d, fileVer:%" PRId64 ", due to uid:%" PRIu64
                ", load data, %s",
                pIter->iSttBlk, pIter->cid, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 0;
    }
    return &pInfo->blockData[0].data;
  }

  if (pInfo->blockData[1].sttBlockIndex == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 1) {
      tsdbDebug("current load index is set to 1, block index:%d, fileVer:%" PRId64 ", due to uid:%" PRIu64
                ", load data, %s",
                pIter->iSttBlk, pIter->cid, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 1;
    }
    return &pInfo->blockData[1].data;
  }

  if (pIter->pSttBlk == NULL || pInfo->pSchema == NULL) {
    return NULL;
  }

  updateBlockLoadSlot(pInfo);
  int64_t st = taosGetTimestampUs();

  SBlockData *pBlock = &pInfo->blockData[pInfo->currentLoadBlockIndex].data;
  code = tsdbSttFileReadBlockDataByColumn(pIter->pReader, pIter->pSttBlk, pBlock, pInfo->pSchema, &pInfo->colIds[1],
                                          pInfo->numOfCols - 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  double el = (taosGetTimestampUs() - st) / 1000.0;
  pInfo->cost.blockElapsedTime += el;
  pInfo->cost.loadBlocks += 1;

  tsdbDebug("read stt block, total load:%" PRId64 ", trigger by uid:%" PRIu64 ", stt-fileVer:%" PRId64
            ", last block index:%d, entry:%d, rows:%d, uidRange:%" PRId64 "-%" PRId64 " tsRange:%" PRId64 "-%" PRId64
            " %p, elapsed time:%.2f ms, %s",
            pInfo->cost.loadBlocks, pIter->uid, pIter->cid, pIter->iSttBlk, pInfo->currentLoadBlockIndex, pBlock->nRow,
            pIter->pSttBlk->minUid, pIter->pSttBlk->maxUid, pIter->pSttBlk->minKey, pIter->pSttBlk->maxKey, pBlock, el,
            idStr);

  pInfo->blockData[pInfo->currentLoadBlockIndex].sttBlockIndex = pIter->iSttBlk;
  pIter->iRow = (pIter->backward) ? pInfo->blockData[pInfo->currentLoadBlockIndex].data.nRow : -1;

  tsdbDebug("last block index list:%d, %d, rowIndex:%d %s", pInfo->blockData[0].sttBlockIndex,
            pInfo->blockData[1].sttBlockIndex, pIter->iRow, idStr);
  return &pInfo->blockData[pInfo->currentLoadBlockIndex].data;

  _exit:
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
  }

  return NULL;
}

// find the earliest block that contains the required records
static FORCE_INLINE int32_t findEarliestIndex(int32_t index, uint64_t uid, const SSttBlk *pBlockList, int32_t num,
                                              int32_t backward) {
  int32_t i = index;
  int32_t step = backward ? 1 : -1;
  while (i >= 0 && i < num && uid >= pBlockList[i].minUid && uid <= pBlockList[i].maxUid) {
    i += step;
  }
  return i - step;
}

static int32_t binarySearchForStartBlock(SSttBlk *pBlockList, int32_t num, uint64_t uid, int32_t backward) {
  int32_t midPos = -1;
  if (num <= 0) {
    return -1;
  }

  int32_t firstPos = 0;
  int32_t lastPos = num - 1;

  // find the first position which is bigger than the key
  if ((uid > pBlockList[lastPos].maxUid) || (uid < pBlockList[firstPos].minUid)) {
    return -1;
  }

  while (1) {
    if (uid >= pBlockList[firstPos].minUid && uid <= pBlockList[firstPos].maxUid) {
      return findEarliestIndex(firstPos, uid, pBlockList, num, backward);
    }

    if (uid > pBlockList[lastPos].maxUid || uid < pBlockList[firstPos].minUid) {
      return -1;
    }

    int32_t numOfRows = lastPos - firstPos + 1;
    midPos = (numOfRows >> 1u) + firstPos;

    if (uid < pBlockList[midPos].minUid) {
      lastPos = midPos - 1;
    } else if (uid > pBlockList[midPos].maxUid) {
      firstPos = midPos + 1;
    } else {
      return findEarliestIndex(midPos, uid, pBlockList, num, backward);
    }
  }
}

static FORCE_INLINE int32_t findEarliestRow(int32_t index, uint64_t uid, const uint64_t *uidList, int32_t num,
                                            int32_t backward) {
  int32_t i = index;
  int32_t step = backward ? 1 : -1;
  while (i >= 0 && i < num && uid == uidList[i]) {
    i += step;
  }
  return i - step;
}

static int32_t binarySearchForStartRowIndex(uint64_t *uidList, int32_t num, uint64_t uid, int32_t backward) {
  int32_t firstPos = 0;
  int32_t lastPos = num - 1;

  // find the first position which is bigger than the key
  if ((uid > uidList[lastPos]) || (uid < uidList[firstPos])) {
    return -1;
  }

  while (1) {
    if (uid == uidList[firstPos]) {
      return findEarliestRow(firstPos, uid, uidList, num, backward);
    }

    if (uid > uidList[lastPos] || uid < uidList[firstPos]) {
      return -1;
    }

    int32_t numOfRows = lastPos - firstPos + 1;
    int32_t midPos = (numOfRows >> 1u) + firstPos;

    if (uid < uidList[midPos]) {
      lastPos = midPos - 1;
    } else if (uid > uidList[midPos]) {
      firstPos = midPos + 1;
    } else {
      return findEarliestRow(midPos, uid, uidList, num, backward);
    }
  }
}

static int32_t extractSttBlockInfo(SLDataIter *pIter, const TSttBlkArray *pArray, SSttBlockLoadInfo *pBlockLoadInfo,
                                   uint64_t suid) {
  if (TARRAY2_SIZE(pArray) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSttBlk *pStart = &pArray->data[0];
  SSttBlk *pEnd = &pArray->data[TARRAY2_SIZE(pArray) - 1];

  // all identical
  if (pStart->suid == pEnd->suid) {
    if (pStart->suid != suid) {  // no qualified stt block existed
      taosArrayClear(pBlockLoadInfo->aSttBlk);
      pIter->iSttBlk = -1;
      return TSDB_CODE_SUCCESS;
    } else {  // all blocks are qualified
      taosArrayClear(pBlockLoadInfo->aSttBlk);
      taosArrayAddBatch(pBlockLoadInfo->aSttBlk, pArray->data, pArray->size);
    }
  } else {
    SArray *pTmp = taosArrayInit(TARRAY2_SIZE(pArray), sizeof(SSttBlk));
    for (int32_t i = 0; i < TARRAY2_SIZE(pArray); ++i) {
      SSttBlk *p = &pArray->data[i];
      if (p->suid < suid) {
        continue;
      }

      if (p->suid == suid) {
        taosArrayPush(pTmp, p);
      } else if (p->suid > suid) {
        break;
      }
    }

    taosArrayDestroy(pBlockLoadInfo->aSttBlk);
    pBlockLoadInfo->aSttBlk = pTmp;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t loadSttStatisticsBlockData(SSttFileReader *pSttFileReader, SSttBlockLoadInfo *pBlockLoadInfo,
                                          TStatisBlkArray *pStatisBlkArray, uint64_t suid, const char *id) {
  int32_t numOfBlocks = TARRAY2_SIZE(pStatisBlkArray);
  if (numOfBlocks <= 0) {
    return 0;
  }

  int32_t startIndex = 0;
  while((startIndex < numOfBlocks) && (pStatisBlkArray->data[startIndex].maxTbid.suid < suid)) {
    ++startIndex;
  }

  if (startIndex >= numOfBlocks || pStatisBlkArray->data[startIndex].minTbid.suid > suid) {
    return 0;
  }

  int32_t endIndex = startIndex;
  while(endIndex < numOfBlocks && pStatisBlkArray->data[endIndex].minTbid.suid <= suid) {
    ++endIndex;
  }

  int32_t num = endIndex - startIndex;
  pBlockLoadInfo->cost.loadStatisBlocks += num;

  STbStatisBlock block;
  tStatisBlockInit(&block);

  int64_t st = taosGetTimestampUs();

  for(int32_t k = startIndex; k < endIndex; ++k) {
    tsdbSttFileReadStatisBlock(pSttFileReader, &pStatisBlkArray->data[k], &block);

    int32_t i = 0;
    int32_t rows = TARRAY2_SIZE(block.suid);
    while (i < rows && block.suid->data[i] != suid) {
      ++i;
    }

    // existed
    if (i < rows) {
      if (pBlockLoadInfo->info.pUid == NULL) {
        pBlockLoadInfo->info.pUid = taosArrayInit(rows, sizeof(int64_t));
        pBlockLoadInfo->info.pFirstKey = taosArrayInit(rows, sizeof(int64_t));
        pBlockLoadInfo->info.pLastKey = taosArrayInit(rows, sizeof(int64_t));
        pBlockLoadInfo->info.pCount = taosArrayInit(rows, sizeof(int64_t));
      }

      if (pStatisBlkArray->data[k].maxTbid.suid == suid) {
        taosArrayAddBatch(pBlockLoadInfo->info.pUid, &block.uid->data[i], rows - i);
        taosArrayAddBatch(pBlockLoadInfo->info.pFirstKey, &block.firstKey->data[i], rows - i);
        taosArrayAddBatch(pBlockLoadInfo->info.pLastKey, &block.lastKey->data[i], rows - i);
        taosArrayAddBatch(pBlockLoadInfo->info.pCount, &block.count->data[i], rows - i);
      } else {
        while (i < rows && block.suid->data[i] == suid) {
          taosArrayPush(pBlockLoadInfo->info.pUid, &block.uid->data[i]);
          taosArrayPush(pBlockLoadInfo->info.pFirstKey, &block.firstKey->data[i]);
          taosArrayPush(pBlockLoadInfo->info.pLastKey, &block.lastKey->data[i]);
          taosArrayPush(pBlockLoadInfo->info.pCount, &block.count->data[i]);
          i += 1;
        }
      }
    }
  }

  tStatisBlockDestroy(&block);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  pBlockLoadInfo->cost.statisElapsedTime += el;

  tsdbDebug("%s load %d statis blocks into buf, elapsed time:%.2fms", id, num, el);
  return TSDB_CODE_SUCCESS;
}

static int32_t doLoadSttFilesBlk(SSttBlockLoadInfo *pBlockLoadInfo, SLDataIter *pIter, int64_t suid,
                                 _load_tomb_fn loadTombFn, void *pReader1, const char *idStr) {
  int64_t st = taosGetTimestampUs();

  const TSttBlkArray *pSttBlkArray = NULL;
  pBlockLoadInfo->sttBlockLoaded = true;

  // load the stt block info for each stt-block
  int32_t code = tsdbSttFileReadSttBlk(pIter->pReader, &pSttBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("load stt blk failed, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load the stt block info for each stt file block
  code = extractSttBlockInfo(pIter, pSttBlkArray, pBlockLoadInfo, suid);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("load stt block info failed, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load stt statistics block for all stt-blocks, to decide if the data of queried table exists in current stt file
  TStatisBlkArray *pStatisBlkArray = NULL;
  code = tsdbSttFileReadStatisBlk(pIter->pReader, (const TStatisBlkArray **)&pStatisBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("failed to load stt block statistics, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load statistics block for all tables in current stt file
  code = loadSttStatisticsBlockData(pIter->pReader, pIter->pBlockLoadInfo, pStatisBlkArray, suid, idStr);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("failed to load stt statistics block data, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  code = loadTombFn(pReader1, pIter->pReader, pIter->pBlockLoadInfo);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("load the stt file info completed, elapsed time:%.2fms, %s", el, idStr);
  return code;
}

static int32_t uidComparFn(const void* p1, const void* p2) {
  const uint64_t *pFirst = p1;
  const uint64_t *pVal = p2;

  if (*pFirst == *pVal) {
    return 0;
  } else {
    return *pFirst < *pVal? -1:1;
  }
}

static void setSttInfoForCurrentTable(SSttBlockLoadInfo *pLoadInfo, uint64_t uid, STimeWindow *pTimeWindow,
                                      int64_t *numOfRows) {
  if (pTimeWindow == NULL || taosArrayGetSize(pLoadInfo->info.pUid) == 0) {
    return;
  }

  int32_t index = taosArraySearchIdx(pLoadInfo->info.pUid, &uid, uidComparFn, TD_EQ);
  if (index >= 0) {
    pTimeWindow->skey = *(int64_t *)taosArrayGet(pLoadInfo->info.pFirstKey, index);
    pTimeWindow->ekey = *(int64_t *)taosArrayGet(pLoadInfo->info.pLastKey, index);

    *numOfRows += *(int64_t*) taosArrayGet(pLoadInfo->info.pCount, index);
  }
}

int32_t tLDataIterOpen2(SLDataIter *pIter, SSttFileReader *pSttFileReader, int32_t cid, int8_t backward,
                        SMergeTreeConf *pConf, SSttBlockLoadInfo *pBlockLoadInfo, STimeWindow *pTimeWindow,
                        int64_t *numOfRows, const char *idStr) {
  int32_t code = TSDB_CODE_SUCCESS;

  pIter->uid = pConf->uid;
  pIter->cid = cid;
  pIter->backward = backward;
  pIter->verRange.minVer = pConf->verRange.minVer;
  pIter->verRange.maxVer = pConf->verRange.maxVer;
  pIter->timeWindow.skey = pConf->timewindow.skey;
  pIter->timeWindow.ekey = pConf->timewindow.ekey;
  pIter->pReader = pSttFileReader;
  pIter->pBlockLoadInfo = pBlockLoadInfo;

  // open stt file failed, ignore and continue
  if (pIter->pReader == NULL) {
    tsdbError("stt file reader is null, %s", idStr);
    pIter->pSttBlk = NULL;
    pIter->iSttBlk = -1;
    return TSDB_CODE_SUCCESS;
  }

  if (!pBlockLoadInfo->sttBlockLoaded) {
    code = doLoadSttFilesBlk(pBlockLoadInfo, pIter, pConf->suid, pConf->loadTombFn, pConf->pReader, idStr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

  setSttInfoForCurrentTable(pBlockLoadInfo, pConf->uid, pTimeWindow, numOfRows);

  // find the start block, actually we could load the position to avoid repeatly searching for the start position when
  // the skey is updated.
  size_t size = taosArrayGetSize(pBlockLoadInfo->aSttBlk);
  pIter->iSttBlk = binarySearchForStartBlock(pBlockLoadInfo->aSttBlk->pData, size, pConf->uid, backward);
  if (pIter->iSttBlk != -1) {
    pIter->pSttBlk = taosArrayGet(pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
    pIter->iRow = (pIter->backward) ? pIter->pSttBlk->nRow : -1;

    if ((!backward) && ((pConf->strictTimeRange && pIter->pSttBlk->minKey >= pIter->timeWindow.ekey) ||
                        (!pConf->strictTimeRange && pIter->pSttBlk->minKey > pIter->timeWindow.ekey))) {
      pIter->pSttBlk = NULL;
    }

    if (backward && ((pConf->strictTimeRange && pIter->pSttBlk->maxKey <= pIter->timeWindow.skey) ||
                     (!pConf->strictTimeRange && pIter->pSttBlk->maxKey < pIter->timeWindow.skey))) {
      pIter->pSttBlk = NULL;
      pIter->ignoreEarlierTs = true;
    }
  }

  return code;
}

void tLDataIterClose2(SLDataIter *pIter) {
  tsdbSttFileReaderClose(&pIter->pReader);
  pIter->pReader = NULL;
}

void tLDataIterNextBlock(SLDataIter *pIter, const char *idStr) {
  int32_t step = pIter->backward ? -1 : 1;
  int32_t oldIndex = pIter->iSttBlk;

  pIter->iSttBlk += step;

  int32_t index = -1;
  size_t  size = pIter->pBlockLoadInfo->aSttBlk->size;
  for (int32_t i = pIter->iSttBlk; i < size && i >= 0; i += step) {
    SSttBlk *p = taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, i);
    if ((!pIter->backward) && p->minUid > pIter->uid) {
      break;
    }

    if (pIter->backward && p->maxUid < pIter->uid) {
      break;
    }

    // check uid firstly
    if (p->minUid <= pIter->uid && p->maxUid >= pIter->uid) {
      if ((!pIter->backward) && p->minKey > pIter->timeWindow.ekey) {
        break;
      }

      if (pIter->backward && p->maxKey < pIter->timeWindow.skey) {
        break;
      }

      // check time range secondly
      if (p->minKey <= pIter->timeWindow.ekey && p->maxKey >= pIter->timeWindow.skey) {
        if ((!pIter->backward) && p->minVer > pIter->verRange.maxVer) {
          break;
        }

        if (pIter->backward && p->maxVer < pIter->verRange.minVer) {
          break;
        }

        if (p->minVer <= pIter->verRange.maxVer && p->maxVer >= pIter->verRange.minVer) {
          index = i;
          break;
        }
      }
    }
  }

  pIter->pSttBlk = NULL;
  if (index != -1) {
    SSttBlk *p = taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, index);

    pIter->iSttBlk = index;
    pIter->pSttBlk = (SSttBlk *)taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
    tsdbDebug("try next stt-file block:%d from %d, trigger by uid:%" PRIu64 ", stt-fileVer:%" PRId64
              ", uidRange:%" PRId64 "-%" PRId64 " %s",
              pIter->iSttBlk, oldIndex, pIter->uid, pIter->cid, p->minUid, p->maxUid, idStr);
  } else {
    tsdbDebug("no more last block qualified, uid:%" PRIu64 ", stt-file block:%d, %s", pIter->uid, oldIndex, idStr);
  }
}

static void findNextValidRow(SLDataIter *pIter, const char *idStr) {
  bool    hasVal = false;
  int32_t step = pIter->backward ? -1 : 1;
  int32_t i = pIter->iRow;

  SBlockData *pData = loadLastBlock(pIter, idStr);

  // mostly we only need to find the start position for a given table
  if ((((i == 0) && (!pIter->backward)) || (i == pData->nRow - 1 && pIter->backward)) && pData->aUid != NULL) {
    i = binarySearchForStartRowIndex((uint64_t *)pData->aUid, pData->nRow, pIter->uid, pIter->backward);
    if (i == -1) {
      tsdbDebug("failed to find the data in pBlockData, uid:%" PRIu64 " , %s", pIter->uid, idStr);
      pIter->iRow = -1;
      return;
    }
  }

  for (; i < pData->nRow && i >= 0; i += step) {
    if (pData->aUid != NULL) {
      if (!pIter->backward) {
        if (pData->aUid[i] > pIter->uid) {
          break;
        }
      } else {
        if (pData->aUid[i] < pIter->uid) {
          break;
        }
      }
    }

    int64_t ts = pData->aTSKEY[i];
    if (!pIter->backward) {               // asc
      if (ts > pIter->timeWindow.ekey) {  // no more data
        break;
      } else if (ts < pIter->timeWindow.skey) {
        continue;
      }
    } else {
      if (ts < pIter->timeWindow.skey) {
        break;
      } else if (ts > pIter->timeWindow.ekey) {
        continue;
      }
    }

    int64_t ver = pData->aVersion[i];
    if (ver < pIter->verRange.minVer) {
      continue;
    }

    // todo opt handle desc case
    if (ver > pIter->verRange.maxVer) {
      continue;
    }

    hasVal = true;
    break;
  }

  pIter->iRow = (hasVal) ? i : -1;
}

bool tLDataIterNextRow(SLDataIter *pIter, const char *idStr) {
  int32_t step = pIter->backward ? -1 : 1;
  terrno = TSDB_CODE_SUCCESS;

  // no qualified last file block in current file, no need to fetch row
  if (pIter->pSttBlk == NULL) {
    return false;
  }

  int32_t     iBlockL = pIter->iSttBlk;
  SBlockData *pBlockData = loadLastBlock(pIter, idStr);
  if (pBlockData == NULL || terrno != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  pIter->iRow += step;

  while (1) {
    bool skipBlock = false;
    findNextValidRow(pIter, idStr);

    if (pIter->pBlockLoadInfo->checkRemainingRow) {
      skipBlock = true;
      int16_t *aCols = pIter->pBlockLoadInfo->colIds;
      int      nCols = pIter->pBlockLoadInfo->numOfCols;
      bool     isLast = pIter->pBlockLoadInfo->isLast;
      for (int inputColIndex = 0; inputColIndex < nCols; ++inputColIndex) {
        for (int colIndex = 0; colIndex < pBlockData->nColData; ++colIndex) {
          SColData *pColData = &pBlockData->aColData[colIndex];
          int16_t   cid = pColData->cid;

          if (cid == aCols[inputColIndex]) {
            if (isLast && (pColData->flag & HAS_VALUE)) {
              skipBlock = false;
              break;
            } else if (pColData->flag & (HAS_VALUE | HAS_NULL)) {
              skipBlock = false;
              break;
            }
          }
        }
      }
    }

    if (skipBlock || pIter->iRow >= pBlockData->nRow || pIter->iRow < 0) {
      tLDataIterNextBlock(pIter, idStr);
      if (pIter->pSttBlk == NULL) {  // no more data
        goto _exit;
      }
    } else {
      break;
    }

    if (iBlockL != pIter->iSttBlk) {
      pBlockData = loadLastBlock(pIter, idStr);
      if (pBlockData == NULL) {
        goto _exit;
      }

      // set start row index
      pIter->iRow = pIter->backward ? pBlockData->nRow - 1 : 0;
    }
  }

  pIter->rInfo.suid = pBlockData->suid;
  pIter->rInfo.uid = pBlockData->uid;
  pIter->rInfo.row = tsdbRowFromBlockData(pBlockData, pIter->iRow);

  _exit:
  return (terrno == TSDB_CODE_SUCCESS) && (pIter->pSttBlk != NULL) && (pBlockData != NULL);
}

// SMergeTree =================================================
static FORCE_INLINE int32_t tLDataIterCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  SLDataIter *pIter1 = (SLDataIter *)(((uint8_t *)p1) - offsetof(SLDataIter, node));
  SLDataIter *pIter2 = (SLDataIter *)(((uint8_t *)p2) - offsetof(SLDataIter, node));

  TSDBKEY key1 = TSDBROW_KEY(&pIter1->rInfo.row);
  TSDBKEY key2 = TSDBROW_KEY(&pIter2->rInfo.row);

  if (key1.ts < key2.ts) {
    return -1;
  } else if (key1.ts > key2.ts) {
    return 1;
  } else {
    if (key1.version < key2.version) {
      return -1;
    } else if (key1.version > key2.version) {
      return 1;
    } else {
      return 0;
    }
  }
}

static FORCE_INLINE int32_t tLDataIterDescCmprFn(const SRBTreeNode *p1, const SRBTreeNode *p2) {
  return -1 * tLDataIterCmprFn(p1, p2);
}

int32_t tMergeTreeOpen2(SMergeTree *pMTree, SMergeTreeConf *pConf, SSttDataInfoForTable* pSttDataInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  pMTree->pIter = NULL;
  pMTree->backward = pConf->backward;
  pMTree->idStr = pConf->idstr;

  if (!pMTree->backward) {  // asc
    tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
  } else {  // desc
    tRBTreeCreate(&pMTree->rbt, tLDataIterDescCmprFn);
  }

  pMTree->ignoreEarlierTs = false;

  // no data exists, go to end
  int32_t numOfLevels = ((STFileSet *)pConf->pCurrentFileset)->lvlArr->size;
  if (numOfLevels == 0) {
    goto _end;
  }

  adjustSttDataIters(pConf->pSttFileBlockIterArray, pConf->pCurrentFileset);

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SSttLvl *pSttLevel = ((STFileSet *)pConf->pCurrentFileset)->lvlArr->data[j];
    SArray * pList = taosArrayGetP(pConf->pSttFileBlockIterArray, j);

    for (int32_t i = 0; i < TARRAY2_SIZE(pSttLevel->fobjArr); ++i) {  // open all last file
      SLDataIter *pIter = taosArrayGetP(pList, i);

      SSttFileReader *   pSttFileReader = pIter->pReader;
      SSttBlockLoadInfo *pLoadInfo = pIter->pBlockLoadInfo;

      // open stt file reader if not opened yet
      // if failed to open this stt file, ignore the error and try next one
      if (pSttFileReader == NULL) {
        SSttFileReaderConfig conf = {.tsdb = pConf->pTsdb, .szPage = pConf->pTsdb->pVnode->config.tsdbPageSize};
        conf.file[0] = *pSttLevel->fobjArr->data[i]->f;

        code = tsdbSttFileReaderOpen(pSttLevel->fobjArr->data[i]->fname, &conf, &pSttFileReader);
        if (code != TSDB_CODE_SUCCESS) {
          tsdbError("open stt file reader error. file name %s, code %s, %s", pSttLevel->fobjArr->data[i]->fname,
                    tstrerror(code), pMTree->idStr);
        }
      }

      if (pLoadInfo == NULL) {
        pLoadInfo = tCreateSttBlockLoadInfo(pConf->pSchema, pConf->pCols, pConf->numOfCols);
      }

      memset(pIter, 0, sizeof(SLDataIter));

      STimeWindow w = {0};
      int64_t     numOfRows = 0;

      int64_t cid = pSttLevel->fobjArr->data[i]->f->cid;
      code = tLDataIterOpen2(pIter, pSttFileReader, cid, pMTree->backward, pConf, pLoadInfo, &w, &numOfRows, pMTree->idStr);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }

      bool hasVal = tLDataIterNextRow(pIter, pMTree->idStr);
      if (hasVal) {
        tMergeTreeAddIter(pMTree, pIter);

        // let's record the time window for current table of uid in the stt files
        if (pSttDataInfo != NULL) {
          taosArrayPush(pSttDataInfo->pTimeWindowList, &w);
          pSttDataInfo->numOfRows += numOfRows;
        }
      } else {
        if (!pMTree->ignoreEarlierTs) {
          pMTree->ignoreEarlierTs = pIter->ignoreEarlierTs;
        }
      }
    }
  }

  return code;

  _end:
  tMergeTreeClose(pMTree);
  return code;
}

void tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter) { tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pIter); }

bool tMergeTreeIgnoreEarlierTs(SMergeTree *pMTree) { return pMTree->ignoreEarlierTs; }

static void tLDataIterPinSttBlock(SLDataIter* pIter, const char* id) {
  SSttBlockLoadInfo* pInfo = pIter->pBlockLoadInfo;

  if (pInfo->blockData[0].sttBlockIndex == pIter->iSttBlk) {
    pInfo->blockData[0].pin = true;
    ASSERT(!pInfo->blockData[1].pin);
    tsdbTrace("pin stt-block, blockIndex:%d, stt-fileVer:%" PRId64 " %s", pIter->iSttBlk, pIter->cid, id);
    return;
  }

  if (pInfo->blockData[1].sttBlockIndex == pIter->iSttBlk) {
    pInfo->blockData[1].pin = true;
    ASSERT(!pInfo->blockData[0].pin);
    tsdbTrace("pin stt-block, blockIndex:%d, stt-fileVer:%"PRId64" %s", pIter->iSttBlk, pIter->cid, id);
    return;
  }

  tsdbError("failed to pin any stt block, sttBlock:%d stt-fileVer:%"PRId64" %s", pIter->iSttBlk, pIter->cid, id);
}

static void tLDataIterUnpinSttBlock(SLDataIter* pIter, const char* id) {
  SSttBlockLoadInfo* pInfo = pIter->pBlockLoadInfo;
  if (pInfo->blockData[0].pin) {
    ASSERT(!pInfo->blockData[1].pin);
    pInfo->blockData[0].pin = false;
    tsdbTrace("unpin stt-block:%d, stt-fileVer:%" PRId64 " %s", pInfo->blockData[0].sttBlockIndex, pIter->cid, id);
    return;
  }

  if (pInfo->blockData[1].pin) {
    ASSERT(!pInfo->blockData[0].pin);
    pInfo->blockData[1].pin = false;
    tsdbTrace("unpin stt-block:%d, stt-fileVer:%" PRId64 " %s", pInfo->blockData[1].sttBlockIndex, pIter->cid, id);
    return;
  }

  tsdbError("failed to unpin any stt block, sttBlock:%d stt-fileVer:%" PRId64 " %s", pIter->iSttBlk, pIter->cid, id);
}

void tMergeTreePinSttBlock(SMergeTree *pMTree) {
  if (pMTree->pIter == NULL) {
    return;
  }

  SLDataIter *pIter = pMTree->pIter;
  pMTree->pPinnedBlockIter = pIter;
  tLDataIterPinSttBlock(pIter, pMTree->idStr);
}

void tMergeTreeUnpinSttBlock(SMergeTree *pMTree) {
  if (pMTree->pPinnedBlockIter == NULL) {
    return;
  }

  SLDataIter* pIter = pMTree->pPinnedBlockIter;
  pMTree->pPinnedBlockIter = NULL;
  tLDataIterUnpinSttBlock(pIter, pMTree->idStr);
}

bool tMergeTreeNext(SMergeTree *pMTree) {
  if (pMTree->pIter) {
    SLDataIter *pIter = pMTree->pIter;

    bool hasVal = tLDataIterNextRow(pIter, pMTree->idStr);
    if (!hasVal) {
      pMTree->pIter = NULL;
    }

    // compare with min in RB Tree
    pIter = (SLDataIter *)tRBTreeMin(&pMTree->rbt);
    if (pMTree->pIter && pIter) {
      int32_t c = pMTree->rbt.cmprFn(&pMTree->pIter->node, &pIter->node);
      if (c > 0) {
        tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pMTree->pIter);
        pMTree->pIter = NULL;
      } else {
        ASSERT(c);
      }
    }
  }

  if (pMTree->pIter == NULL) {
    pMTree->pIter = (SLDataIter *)tRBTreeMin(&pMTree->rbt);
    if (pMTree->pIter) {
      tRBTreeDrop(&pMTree->rbt, (SRBTreeNode *)pMTree->pIter);
    }
  }

  return pMTree->pIter != NULL;
}

void tMergeTreeClose(SMergeTree *pMTree) {
  pMTree->pIter = NULL;
  pMTree->pPinnedBlockIter = NULL;
}
