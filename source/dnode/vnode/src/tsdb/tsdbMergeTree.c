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
#include "tsdbMerge.h"
#include "tsdbReadUtil.h"
#include "tsdbSttFileRW.h"

static void tLDataIterClose2(SLDataIter *pIter);

// SLDataIter =================================================
SSttBlockLoadInfo *tCreateOneLastBlockLoadInfo(STSchema *pSchema, int16_t *colList, int32_t numOfCols) {
  SSttBlockLoadInfo *pLoadInfo = taosMemoryCalloc(1, sizeof(SSttBlockLoadInfo));
  if (pLoadInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pLoadInfo->blockIndex[0] = -1;
  pLoadInfo->blockIndex[1] = -1;
  pLoadInfo->currentLoadBlockIndex = 1;

  int32_t code = tBlockDataCreate(&pLoadInfo->blockData[0]);
  if (code) {
    terrno = code;
  }

  code = tBlockDataCreate(&pLoadInfo->blockData[1]);
  if (code) {
    terrno = code;
  }

  pLoadInfo->aSttBlk = taosArrayInit(4, sizeof(SSttBlk));
  pLoadInfo->pSchema = pSchema;
  pLoadInfo->colIds = colList;
  pLoadInfo->numOfCols = numOfCols;

  return pLoadInfo;
}

void getSttBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo, SSttBlockLoadCostInfo* pLoadCost) {
  for (int32_t i = 0; i < 1; ++i) {
    pLoadCost->blockElapsedTime += pLoadInfo[i].cost.blockElapsedTime;
    pLoadCost->loadBlocks += pLoadInfo[i].cost.loadBlocks;
    pLoadCost->loadStatisBlocks += pLoadInfo[i].cost.loadStatisBlocks;
    pLoadCost->statisElapsedTime += pLoadInfo[i].cost.statisElapsedTime;
  }
}

void *destroyLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo) {
  if (pLoadInfo == NULL) {
    return NULL;
  }

  pLoadInfo->currentLoadBlockIndex = 1;
  pLoadInfo->blockIndex[0] = -1;
  pLoadInfo->blockIndex[1] = -1;

  tBlockDataDestroy(&pLoadInfo->blockData[0]);
  tBlockDataDestroy(&pLoadInfo->blockData[1]);

  taosArrayDestroy(pLoadInfo->aSttBlk);
  taosMemoryFree(pLoadInfo);
  return NULL;
}

static void destroyLDataIter(SLDataIter *pIter) {
  tLDataIterClose2(pIter);
  destroyLastBlockLoadInfo(pIter->pBlockLoadInfo);
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

static SBlockData *loadLastBlock(SLDataIter *pIter, const char *idStr) {
  int32_t code = 0;

  SSttBlockLoadInfo *pInfo = pIter->pBlockLoadInfo;
  if (pInfo->blockIndex[0] == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 0) {
      tsdbDebug("current load index is set to 0, block index:%d, fileVer:%" PRId64 ", due to uid:%" PRIu64
                ", load data, %s",
                pIter->iSttBlk, pIter->cid, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 0;
    }
    return &pInfo->blockData[0];
  }

  if (pInfo->blockIndex[1] == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 1) {
      tsdbDebug("current load index is set to 1, block index:%d, fileVer:%" PRId64 ", due to uid:%" PRIu64
                ", load data, %s",
                pIter->iSttBlk, pIter->cid, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 1;
    }
    return &pInfo->blockData[1];
  }

  if (pIter->pSttBlk == NULL || pInfo->pSchema == NULL) {
    return NULL;
  }

  // current block not loaded yet
  pInfo->currentLoadBlockIndex ^= 1;
  int64_t st = taosGetTimestampUs();

  SBlockData *pBlock = &pInfo->blockData[pInfo->currentLoadBlockIndex];
  code = tsdbSttFileReadBlockDataByColumn(pIter->pReader, pIter->pSttBlk, pBlock, pInfo->pSchema, &pInfo->colIds[1],
                                          pInfo->numOfCols - 1);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  double el = (taosGetTimestampUs() - st) / 1000.0;
  pInfo->cost.blockElapsedTime += el;
  pInfo->cost.loadBlocks += 1;

  tsdbDebug("read last block, total load:%" PRId64 ", trigger by uid:%" PRIu64 ", stt-fileVer:%" PRId64
            ", last block index:%d, entry:%d, rows:%d, uidRange:%" PRId64 "-%" PRId64 " tsRange:%" PRId64 "-%" PRId64
            " %p, elapsed time:%.2f ms, %s",
            pInfo->cost.loadBlocks, pIter->uid, pIter->cid, pIter->iSttBlk, pInfo->currentLoadBlockIndex, pBlock->nRow,
            pIter->pSttBlk->minUid, pIter->pSttBlk->maxUid, pIter->pSttBlk->minKey, pIter->pSttBlk->maxKey, pBlock, el,
            idStr);

  pInfo->blockIndex[pInfo->currentLoadBlockIndex] = pIter->iSttBlk;
  pIter->iRow = (pIter->backward) ? pInfo->blockData[pInfo->currentLoadBlockIndex].nRow : -1;

  tsdbDebug("last block index list:%d, %d, rowIndex:%d %s", pInfo->blockIndex[0], pInfo->blockIndex[1], pIter->iRow,
            idStr);
  return &pInfo->blockData[pInfo->currentLoadBlockIndex];

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

static int32_t suidComparFn(const void *target, const void *p2) {
  const uint64_t *targetUid = target;
  const uint64_t *uid2 = p2;
  if (*uid2 == (*targetUid)) {
    return 0;
  } else {
    return (*targetUid) < (*uid2) ? -1 : 1;
  }
}

static bool existsFromSttBlkStatis(SSttBlockLoadInfo *pBlockLoadInfo, uint64_t suid, uint64_t uid,
                                   SSttFileReader *pReader) {
  const TStatisBlkArray *pStatisBlkArray = pBlockLoadInfo->pSttStatisBlkArray;
  if (TARRAY2_SIZE(pStatisBlkArray) <= 0) {
    return true;
  }

  int32_t i = 0;
  for (i = 0; i < TARRAY2_SIZE(pStatisBlkArray); ++i) {
    SStatisBlk *p = &pStatisBlkArray->data[i];
    if (p->minTbid.suid <= suid && p->maxTbid.suid >= suid) {
      break;
    }
  }

  if (i >= TARRAY2_SIZE(pStatisBlkArray)) {
    return false;
  }

  while (i < TARRAY2_SIZE(pStatisBlkArray)) {
    SStatisBlk *p = &pStatisBlkArray->data[i];
    if (p->minTbid.suid > suid) {
      return false;
    }

//    if (pBlockLoadInfo->statisBlock == NULL) {
//      pBlockLoadInfo->statisBlock = taosMemoryCalloc(1, sizeof(STbStatisBlock));
//
//      int64_t st = taosGetTimestampMs();
//      tsdbSttFileReadStatisBlock(pReader, p, pBlockLoadInfo->statisBlock);
//      pBlockLoadInfo->statisBlockIndex = i;
//
//      double el = (taosGetTimestampMs() - st) / 1000.0;
//      pBlockLoadInfo->cost.loadStatisBlocks += 1;
//      pBlockLoadInfo->cost.statisElapsedTime += el;
//    } else if (pBlockLoadInfo->statisBlockIndex != i) {
//      tStatisBlockDestroy(pBlockLoadInfo->statisBlock);
//
//      int64_t st = taosGetTimestampMs();
//      tsdbSttFileReadStatisBlock(pReader, p, pBlockLoadInfo->statisBlock);
//      pBlockLoadInfo->statisBlockIndex = i;
//
//      double el = (taosGetTimestampMs() - st) / 1000.0;
//      pBlockLoadInfo->cost.loadStatisBlocks += 1;
//      pBlockLoadInfo->cost.statisElapsedTime += el;
//    }

    STbStatisBlock* pBlock = pBlockLoadInfo->statisBlock;
    int32_t index = tarray2SearchIdx(pBlock->suid, &suid, sizeof(int64_t), suidComparFn, TD_EQ);
    if (index == -1) {
      return false;
    }

    int32_t j = index;
    if (pBlock->uid->data[j] == uid) {
      return true;
    } else if (pBlock->uid->data[j] > uid) {
      while (j >= 0 && pBlock->suid->data[j] == suid) {
        if (pBlock->uid->data[j] == uid) {
          return true;
        } else {
          j -= 1;
        }
      }
    } else {
      j = index + 1;
      while (j < pBlock->suid->size && pBlock->suid->data[j] == suid) {
        if (pBlock->uid->data[j] == uid) {
          return true;
        } else {
          j += 1;
        }
      }
    }

    i += 1;
  }

  return false;
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

  code = extractSttBlockInfo(pIter, pSttBlkArray, pBlockLoadInfo, suid);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("load stt block info failed, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  // load stt blocks statis for all stt-blocks, to decide if the data of queried table exists in current stt file
  code = tsdbSttFileReadStatisBlk(pIter->pReader, (const TStatisBlkArray **)&pBlockLoadInfo->pSttStatisBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    tsdbError("failed to load stt block statistics, code:%s, %s", tstrerror(code), idStr);
    return code;
  }

  code = loadTombFn(pReader1, pIter->pReader, pIter->pBlockLoadInfo);

  double el = (taosGetTimestampUs() - st) / 1000.0;
  tsdbDebug("load the stt file info completed, elapsed time:%.2fms, %s", el, idStr);
  return code;
}

int32_t tLDataIterOpen2(SLDataIter *pIter, SSttFileReader *pSttFileReader, int32_t cid, int8_t backward,
                        uint64_t suid, uint64_t uid, STimeWindow *pTimeWindow, SVersionRange *pRange,
                        SSttBlockLoadInfo *pBlockLoadInfo, const char *idStr, bool strictTimeRange,
                        _load_tomb_fn loadTombFn, void *pReader1) {
  int32_t code = TSDB_CODE_SUCCESS;

  pIter->uid = uid;
  pIter->cid = cid;
  pIter->backward = backward;
  pIter->verRange.minVer = pRange->minVer;
  pIter->verRange.maxVer = pRange->maxVer;
  pIter->timeWindow.skey = pTimeWindow->skey;
  pIter->timeWindow.ekey = pTimeWindow->ekey;
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
    code = doLoadSttFilesBlk(pBlockLoadInfo, pIter, suid, loadTombFn, pReader1, idStr);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }

//  bool exists = existsFromSttBlkStatis(pBlockLoadInfo, suid, uid, pIter->pReader);
//  if (!exists) {
//    pIter->iSttBlk = -1;
//    pIter->pSttBlk = NULL;
//    return TSDB_CODE_SUCCESS;
//   }

  // find the start block, actually we could load the position to avoid repeatly searching for the start position when
  // the skey is updated.
  size_t size = taosArrayGetSize(pBlockLoadInfo->aSttBlk);
  pIter->iSttBlk = binarySearchForStartBlock(pBlockLoadInfo->aSttBlk->pData, size, uid, backward);
  if (pIter->iSttBlk != -1) {
    pIter->pSttBlk = taosArrayGet(pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
    pIter->iRow = (pIter->backward) ? pIter->pSttBlk->nRow : -1;

    if ((!backward) && ((strictTimeRange && pIter->pSttBlk->minKey >= pIter->timeWindow.ekey) ||
                        (!strictTimeRange && pIter->pSttBlk->minKey > pIter->timeWindow.ekey))) {
      pIter->pSttBlk = NULL;
    }

    if (backward && ((strictTimeRange && pIter->pSttBlk->maxKey <= pIter->timeWindow.skey) ||
                     (!strictTimeRange && pIter->pSttBlk->maxKey < pIter->timeWindow.skey))) {
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

SRowInfo *tLDataIterGet(SLDataIter *pIter) { return &pIter->rInfo; }

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

static void adjustValidLDataIters(SArray *pLDIterList, int32_t numOfFileObj) {
  int32_t size = taosArrayGetSize(pLDIterList);

  if (size < numOfFileObj) {
    int32_t inc = numOfFileObj - size;
    for (int32_t k = 0; k < inc; ++k) {
      SLDataIter *pIter = taosMemoryCalloc(1, sizeof(SLDataIter));
      taosArrayPush(pLDIterList, &pIter);
    }
  } else if (size > numOfFileObj) {  // remove unused LDataIter
    int32_t inc = size - numOfFileObj;

    for (int i = 0; i < inc; ++i) {
      SLDataIter *pIter = taosArrayPop(pLDIterList);
      destroyLDataIter(pIter);
    }
  }
}

int32_t tMergeTreeOpen2(SMergeTree *pMTree, SMergeTreeConf *pConf) {
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

  // add the list/iter placeholder
  while (taosArrayGetSize(pConf->pSttFileBlockIterArray) < numOfLevels) {
    SArray *pList = taosArrayInit(4, POINTER_BYTES);
    taosArrayPush(pConf->pSttFileBlockIterArray, &pList);
  }

  for (int32_t j = 0; j < numOfLevels; ++j) {
    SSttLvl *pSttLevel = ((STFileSet *)pConf->pCurrentFileset)->lvlArr->data[j];
    SArray  *pList = taosArrayGetP(pConf->pSttFileBlockIterArray, j);

    int32_t numOfFileObj = TARRAY2_SIZE(pSttLevel->fobjArr);
    adjustValidLDataIters(pList, numOfFileObj);

    for (int32_t i = 0; i < numOfFileObj; ++i) {  // open all last file
      SLDataIter *pIter = taosArrayGetP(pList, i);

      SSttFileReader    *pSttFileReader = pIter->pReader;
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
        pLoadInfo = tCreateOneLastBlockLoadInfo(pConf->pSchema, pConf->pCols, pConf->numOfCols);
      }

      memset(pIter, 0, sizeof(SLDataIter));

      int64_t cid = pSttLevel->fobjArr->data[i]->f->cid;
      code = tLDataIterOpen2(pIter, pSttFileReader, cid, pMTree->backward, pConf->suid, pConf->uid, &pConf->timewindow,
                             &pConf->verRange, pLoadInfo, pMTree->idStr, pConf->strictTimeRange, pConf->loadTombFn,
                             pConf->pReader);
      if (code != TSDB_CODE_SUCCESS) {
        goto _end;
      }
      
      bool hasVal = tLDataIterNextRow(pIter, pMTree->idStr);
      if (hasVal) {
        tMergeTreeAddIter(pMTree, pIter);
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

bool tMergeTreeNext(SMergeTree *pMTree) {
  int32_t code = TSDB_CODE_SUCCESS;
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
  if (pMTree->destroyLoadInfo) {
    pMTree->pLoadInfo = destroyLastBlockLoadInfo(pMTree->pLoadInfo);
    pMTree->destroyLoadInfo = false;
  }
}
