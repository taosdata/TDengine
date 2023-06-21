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
#include "tsdbSttFileRW.h"

static void tLDataIterClose2(SLDataIter *pIter);

// SLDataIter =================================================
SSttBlockLoadInfo *tCreateLastBlockLoadInfo(STSchema *pSchema, int16_t *colList, int32_t numOfCols,
                                            int32_t numOfSttTrigger) {
  SSttBlockLoadInfo *pLoadInfo = taosMemoryCalloc(numOfSttTrigger, sizeof(SSttBlockLoadInfo));
  if (pLoadInfo == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pLoadInfo->numOfStt = numOfSttTrigger;

  for (int32_t i = 0; i < numOfSttTrigger; ++i) {
    pLoadInfo[i].blockIndex[0] = -1;
    pLoadInfo[i].blockIndex[1] = -1;
    pLoadInfo[i].currentLoadBlockIndex = 1;

    int32_t code = tBlockDataCreate(&pLoadInfo[i].blockData[0]);
    if (code) {
      terrno = code;
    }

    code = tBlockDataCreate(&pLoadInfo[i].blockData[1]);
    if (code) {
      terrno = code;
    }

    pLoadInfo[i].aSttBlk = taosArrayInit(4, sizeof(SSttBlk));
    pLoadInfo[i].pSchema = pSchema;
    pLoadInfo[i].colIds = colList;
    pLoadInfo[i].numOfCols = numOfCols;
  }

  return pLoadInfo;
}

void resetLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo) {
  for (int32_t i = 0; i < pLoadInfo->numOfStt; ++i) {
    pLoadInfo[i].currentLoadBlockIndex = 1;
    pLoadInfo[i].blockIndex[0] = -1;
    pLoadInfo[i].blockIndex[1] = -1;

    taosArrayClear(pLoadInfo[i].aSttBlk);

    pLoadInfo[i].elapsedTime = 0;
    pLoadInfo[i].loadBlocks = 0;
    pLoadInfo[i].sttBlockLoaded = false;
  }
}

void getLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo, int64_t *blocks, double *el) {
  for (int32_t i = 0; i < pLoadInfo->numOfStt; ++i) {
    *el += pLoadInfo[i].elapsedTime;
    *blocks += pLoadInfo[i].loadBlocks;
  }
}

void *destroyLastBlockLoadInfo(SSttBlockLoadInfo *pLoadInfo) {
  if (pLoadInfo == NULL) {
    return NULL;
  }

  for (int32_t i = 0; i < pLoadInfo->numOfStt; ++i) {
    pLoadInfo[i].currentLoadBlockIndex = 1;
    pLoadInfo[i].blockIndex[0] = -1;
    pLoadInfo[i].blockIndex[1] = -1;

    tBlockDataDestroy(&pLoadInfo[i].blockData[0]);
    tBlockDataDestroy(&pLoadInfo[i].blockData[1]);

    taosArrayDestroy(pLoadInfo[i].aSttBlk);
  }

  taosMemoryFree(pLoadInfo);
  return NULL;
}

void destroySttBlockReader(SLDataIter* pLDataIter, int32_t numOfIter) {
  for(int32_t i = 0; i < numOfIter; ++i) {
    tLDataIterClose2(&pLDataIter[i]);
  }
}

static SBlockData *loadLastBlock(SLDataIter *pIter, const char *idStr) {
  int32_t code = 0;

  SSttBlockLoadInfo *pInfo = pIter->pBlockLoadInfo;
  if (pInfo->blockIndex[0] == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 0) {
      tsdbDebug("current load index is set to 0, block index:%d, file index:%d, due to uid:%" PRIu64 ", load data, %s",
                pIter->iSttBlk, pIter->iStt, pIter->uid, idStr);
      pInfo->currentLoadBlockIndex = 0;
    }
    return &pInfo->blockData[0];
  }

  if (pInfo->blockIndex[1] == pIter->iSttBlk) {
    if (pInfo->currentLoadBlockIndex != 1) {
      tsdbDebug("current load index is set to 1, block index:%d, file index:%d, due to uid:%" PRIu64 ", load data, %s",
                pIter->iSttBlk, pIter->iStt, pIter->uid, idStr);
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

  TABLEID id = {0};
  if (pIter->pSttBlk->suid != 0) {
    id.suid = pIter->pSttBlk->suid;
  } else {
    id.uid = pIter->uid;
  }

  code = tBlockDataInit(pBlock, &id, pInfo->pSchema, pInfo->colIds, pInfo->numOfCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  code = tsdbSttFileReadBlockData(pIter->pReader, pIter->pSttBlk, pBlock);
//  code = tsdbReadSttBlock(pIter->pReader, pIter->iStt, pIter->pSttBlk, pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  double el = (taosGetTimestampUs() - st) / 1000.0;
  pInfo->elapsedTime += el;
  pInfo->loadBlocks += 1;

  tsdbDebug("read last block, total load:%d, trigger by uid:%" PRIu64
            ", last file index:%d, last block index:%d, entry:%d, rows:%d, %p, elapsed time:%.2f ms, %s",
            pInfo->loadBlocks, pIter->uid, pIter->iStt, pIter->iSttBlk, pInfo->currentLoadBlockIndex, pBlock->nRow,
            pBlock, el, idStr);

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

int32_t tLDataIterOpen(struct SLDataIter *pIter, SDataFReader *pReader, int32_t iStt, int8_t backward, uint64_t suid,
                        uint64_t uid, STimeWindow *pTimeWindow, SVersionRange *pRange, SSttBlockLoadInfo *pBlockLoadInfo,
                        const char *idStr, bool strictTimeRange) {
  return 0;
}

static int32_t loadSttBlockInfo(SLDataIter *pIter, SSttBlockLoadInfo* pBlockLoadInfo, uint64_t suid) {
  TSttBlkArray* pArray = pBlockLoadInfo->pBlockArray;
  if (TARRAY2_SIZE(pArray) <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSttBlk *pStart = &pArray->data[0];
  SSttBlk *pEnd = &pArray->data[TARRAY2_SIZE(pArray) - 1];

  // all identical
  if (pStart->suid == pEnd->suid) {
    if (pStart->suid != suid) { // no qualified stt block existed
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
      SSttBlk* p = &pArray->data[i];
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

static int32_t loadSttTombBlockData(SSttFileReader* pSttFileReader, uint64_t suid, SSttBlockLoadInfo* pLoadInfo) {
  if (pLoadInfo->pTombBlockArray == NULL) {
    pLoadInfo->pTombBlockArray = taosArrayInit(4, POINTER_BYTES);
  }

  const TTombBlkArray* pBlkArray = NULL;
  int32_t code = tsdbSttFileReadTombBlk(pSttFileReader, &pBlkArray);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  for(int32_t j = 0; j < pBlkArray->size; ++j) {
    STombBlk* pTombBlk = &pBlkArray->data[j];
    if (pTombBlk->maxTbid.suid < suid) {
      continue;  // todo use binary search instead
    }

    if (pTombBlk->minTbid.suid > suid) {
      break;
    }

    STombBlock* pTombBlock = taosMemoryCalloc(1, sizeof(STombBlock));
    code = tsdbSttFileReadTombBlock(pSttFileReader, pTombBlk, pTombBlock);
    if (code != TSDB_CODE_SUCCESS) {
      // todo handle error
    }

    void* p = taosArrayPush(pLoadInfo->pTombBlockArray, &pTombBlock);
    if (p == NULL) {
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tLDataIterOpen2(struct SLDataIter *pIter, SSttFileReader *pReader, int32_t iStt, int8_t backward, uint64_t suid,
                       uint64_t uid, STimeWindow *pTimeWindow, SVersionRange *pRange, SSttBlockLoadInfo *pBlockLoadInfo,
                       const char *idStr, bool strictTimeRange) {
  int32_t code = TSDB_CODE_SUCCESS;

  pIter->uid = uid;
  pIter->iStt = iStt;
  pIter->backward = backward;
  pIter->verRange.minVer = pRange->minVer;
  pIter->verRange.maxVer = pRange->maxVer;
  pIter->timeWindow.skey = pTimeWindow->skey;
  pIter->timeWindow.ekey = pTimeWindow->ekey;
  pIter->pReader = pReader;

  pIter->pBlockLoadInfo = pBlockLoadInfo;

  if (!pBlockLoadInfo->sttBlockLoaded) {
    int64_t st = taosGetTimestampUs();
    pBlockLoadInfo->sttBlockLoaded = true;

    code = tsdbSttFileReadSttBlk(pIter->pReader, (const TSttBlkArray **)&pBlockLoadInfo->pBlockArray);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    code = loadSttBlockInfo(pIter, pBlockLoadInfo, suid);

    code = loadSttTombBlockData(pReader, suid, pBlockLoadInfo);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    double el = (taosGetTimestampUs() - st) / 1000.0;
    tsdbDebug("load the last file info completed, elapsed time:%.2fms, %s", el, idStr);
  }

  // find the start block
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
    pIter->iSttBlk = index;
    pIter->pSttBlk = (SSttBlk *)taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
    tsdbDebug("try next last file block:%d from %d, trigger by uid:%" PRIu64 ", file index:%d, %s", pIter->iSttBlk,
              oldIndex, pIter->uid, pIter->iStt, idStr);
  } else {
    tsdbDebug("no more last block qualified, uid:%" PRIu64 ", file index:%d, %s", pIter->uid, oldIndex, idStr);
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

int32_t tMergeTreeOpen(SMergeTree *pMTree, int8_t backward, SDataFReader *pFReader, uint64_t suid, uint64_t uid,
                       STimeWindow *pTimeWindow, SVersionRange *pVerRange, SSttBlockLoadInfo *pBlockLoadInfo,
                       bool destroyLoadInfo, const char *idStr, bool strictTimeRange, SLDataIter* pLDataIter) {
  int32_t code = TSDB_CODE_SUCCESS;

  pMTree->backward = backward;
  pMTree->pIter = NULL;
  pMTree->idStr = idStr;

  if (!pMTree->backward) {  // asc
    tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
  } else {  // desc
    tRBTreeCreate(&pMTree->rbt, tLDataIterDescCmprFn);
  }

  pMTree->pLoadInfo = pBlockLoadInfo;
  pMTree->destroyLoadInfo = destroyLoadInfo;
  pMTree->ignoreEarlierTs = false;

  for (int32_t i = 0; i < pFReader->pSet->nSttF; ++i) {  // open all last file
    memset(&pLDataIter[i], 0, sizeof(SLDataIter));
    code = tLDataIterOpen(&pLDataIter[i], pFReader, i, pMTree->backward, suid, uid, pTimeWindow, pVerRange,
                          &pMTree->pLoadInfo[i], pMTree->idStr, strictTimeRange);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }

    bool hasVal = tLDataIterNextRow(&pLDataIter[i], pMTree->idStr);
    if (hasVal) {
      tMergeTreeAddIter(pMTree, &pLDataIter[i]);
    } else {
      if (!pMTree->ignoreEarlierTs) {
        pMTree->ignoreEarlierTs = pLDataIter[i].ignoreEarlierTs;
      }
    }
  }

  return code;

_end:
  tMergeTreeClose(pMTree);
  return code;
}

int32_t tMergeTreeOpen2(SMergeTree *pMTree, int8_t backward, STsdb *pTsdb, uint64_t suid, uint64_t uid,
                        STimeWindow *pTimeWindow, SVersionRange *pVerRange, SSttBlockLoadInfo *pBlockLoadInfo,
                        bool destroyLoadInfo, const char *idStr, bool strictTimeRange, SLDataIter *pLDataIter,
                        void *pCurrentFileSet) {
  int32_t code = TSDB_CODE_SUCCESS;

  pMTree->backward = backward;
  pMTree->pIter = NULL;
  pMTree->idStr = idStr;

  if (!pMTree->backward) {  // asc
    tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
  } else {  // desc
    tRBTreeCreate(&pMTree->rbt, tLDataIterDescCmprFn);
  }

  pMTree->pLoadInfo = pBlockLoadInfo;
  pMTree->destroyLoadInfo = destroyLoadInfo;
  pMTree->ignoreEarlierTs = false;

  // todo handle other level of stt files, here only deal with the first level stt
  int32_t size = ((STFileSet *)pCurrentFileSet)->lvlArr[0].size;
  if (size == 0) {
    goto _end;
  }

  SSttLvl *pSttLevel = ((STFileSet *)pCurrentFileSet)->lvlArr[0].data[0];
  ASSERT(pSttLevel->level == 0);

  for (int32_t i = 0; i < pSttLevel->fobjArr[0].size; ++i) {  // open all last file
    SSttFileReader* pSttFileReader = pLDataIter[i].pReader;
    memset(&pLDataIter[i], 0, sizeof(SLDataIter));

    if (pSttFileReader == NULL) {
      SSttFileReaderConfig conf = {.tsdb = pTsdb, .szPage = pTsdb->pVnode->config.szPage};
      conf.file[0] = *pSttLevel->fobjArr[0].data[i]->f;

      code = tsdbSttFileReaderOpen(pSttLevel->fobjArr[0].data[i]->fname, &conf, &pSttFileReader);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }

    code = tLDataIterOpen2(&pLDataIter[i], pSttFileReader, i, pMTree->backward, suid, uid, pTimeWindow, pVerRange,
                           &pMTree->pLoadInfo[i], pMTree->idStr, strictTimeRange);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }

    bool hasVal = tLDataIterNextRow(&pLDataIter[i], pMTree->idStr);
    if (hasVal) {
      tMergeTreeAddIter(pMTree, &pLDataIter[i]);
    } else {
      if (!pMTree->ignoreEarlierTs) {
        pMTree->ignoreEarlierTs = pLDataIter[i].ignoreEarlierTs;
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
