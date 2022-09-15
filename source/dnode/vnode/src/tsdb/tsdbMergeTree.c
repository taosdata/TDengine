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

// SLDataIter =================================================
struct SLDataIter {
  SRBTreeNode   node;
  SSttBlk      *pSttBlk;
  SDataFReader *pReader;
  int32_t       iStt;
  int8_t        backward;
  int32_t       iSttBlk;
  int32_t       iRow;
  SRowInfo      rInfo;
  uint64_t      uid;
  STimeWindow   timeWindow;
  SVersionRange verRange;

  SSttBlockLoadInfo* pBlockLoadInfo;
};

SSttBlockLoadInfo* tCreateLastBlockLoadInfo() {
  SSttBlockLoadInfo* pLoadInfo = taosMemoryCalloc(TSDB_DEFAULT_STT_FILE, sizeof(SSttBlockLoadInfo));
  if (pLoadInfo == NULL) {
    terrno =  TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  for(int32_t i = 0; i < TSDB_DEFAULT_STT_FILE; ++i) {
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
  }

  return pLoadInfo;
}

void resetLastBlockLoadInfo(SSttBlockLoadInfo* pLoadInfo) {
  for(int32_t i = 0; i < TSDB_DEFAULT_STT_FILE; ++i) {
    pLoadInfo[i].currentLoadBlockIndex = 1;
    pLoadInfo[i].blockIndex[0] = -1;
    pLoadInfo[i].blockIndex[1] = -1;

    taosArrayClear(pLoadInfo[i].aSttBlk);

    pLoadInfo[i].elapsedTime = 0;
    pLoadInfo[i].loadBlocks = 0;
  }
}

void getLastBlockLoadInfo(SSttBlockLoadInfo* pLoadInfo, int64_t* blocks, double* el) {
  for(int32_t i = 0; i < TSDB_DEFAULT_STT_FILE; ++i) {
    *el += pLoadInfo[i].elapsedTime;
    *blocks += pLoadInfo[i].loadBlocks;
  }
}

void* destroyLastBlockLoadInfo(SSttBlockLoadInfo* pLoadInfo) {
  for(int32_t i = 0; i < TSDB_DEFAULT_STT_FILE; ++i) {
    pLoadInfo[i].currentLoadBlockIndex = 1;
    pLoadInfo[i].blockIndex[0] = -1;
    pLoadInfo[i].blockIndex[1] = -1;

    tBlockDataDestroy(&pLoadInfo[i].blockData[0], true);
    tBlockDataDestroy(&pLoadInfo[i].blockData[1], true);

    taosArrayDestroy(pLoadInfo[i].aSttBlk);
  }

  taosMemoryFree(pLoadInfo);
  return NULL;
}

static SBlockData* loadLastBlock(SLDataIter *pIter, const char* idStr) {
  int32_t code = 0;

  SSttBlockLoadInfo* pInfo = pIter->pBlockLoadInfo;
  if (pInfo->blockIndex[0]  == pIter->iSttBlk) {
    return &pInfo->blockData[0];
  }

  if (pInfo->blockIndex[1] == pIter->iSttBlk) {
    return &pInfo->blockData[1];
  }

  pInfo->currentLoadBlockIndex ^= 1;
  if (pIter->pSttBlk != NULL) {  // current block not loaded yet
    int64_t st = taosGetTimestampUs();
    code = tsdbReadSttBlock(pIter->pReader, pIter->iStt, pIter->pSttBlk, &pInfo->blockData[pInfo->currentLoadBlockIndex]);
    double el = (taosGetTimestampUs() - st)/ 1000.0;
    pInfo->elapsedTime += el;
    pInfo->loadBlocks += 1;

    tsdbDebug("read last block, index:%d, last file index:%d, elapsed time:%.2f ms, %s", pIter->iSttBlk, pIter->iStt, el, idStr);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    pInfo->blockIndex[pInfo->currentLoadBlockIndex] = pIter->iSttBlk;
    pIter->iRow = (pIter->backward) ? pInfo->blockData[pInfo->currentLoadBlockIndex].nRow : -1;
  }

  return &pInfo->blockData[pInfo->currentLoadBlockIndex];

  _exit:
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
  }

  return NULL;
}

// find the earliest block that contains the required records
static FORCE_INLINE int32_t findEarliestIndex(int32_t index, uint64_t uid, const SSttBlk* pBlockList, int32_t num, int32_t backward) {
  int32_t i = index;
  int32_t step = backward? 1:-1;
  while (i >= 0 && i < num && uid >= pBlockList[i].minUid && uid <= pBlockList[i].maxUid) {
    i += step;
  }
  return i - step;
}

static int32_t binarySearchForStartBlock(SSttBlk*pBlockList, int32_t num, uint64_t uid, int32_t backward) {
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

static FORCE_INLINE int32_t findEarliestRow(int32_t index, uint64_t uid, const uint64_t* uidList, int32_t num, int32_t backward) {
  int32_t i = index;
  int32_t step = backward? 1:-1;
  while (i >= 0 && i < num && uid == uidList[i]) {
    i += step;
  }
  return i - step;
}

static int32_t binarySearchForStartRowIndex(uint64_t* uidList, int32_t num, uint64_t uid, int32_t backward) {
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

int32_t tLDataIterOpen(struct SLDataIter **pIter, SDataFReader *pReader, int32_t iStt, int8_t backward, uint64_t suid,
                       uint64_t uid, STimeWindow *pTimeWindow, SVersionRange *pRange, SSttBlockLoadInfo* pBlockLoadInfo) {
  int32_t code = 0;
  *pIter = taosMemoryCalloc(1, sizeof(SLDataIter));
  if (*pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  (*pIter)->uid = uid;
  (*pIter)->pReader = pReader;
  (*pIter)->iStt = iStt;
  (*pIter)->backward = backward;
  (*pIter)->verRange = *pRange;
  (*pIter)->timeWindow = *pTimeWindow;

  (*pIter)->pBlockLoadInfo = pBlockLoadInfo;
  if (taosArrayGetSize(pBlockLoadInfo->aSttBlk) == 0) {
    code = tsdbReadSttBlk(pReader, iStt, pBlockLoadInfo->aSttBlk);
    if (code) {
      goto _exit;
    } else {
      size_t size = taosArrayGetSize(pBlockLoadInfo->aSttBlk);
      SArray* pTmp = taosArrayInit(size, sizeof(SSttBlk));
      for(int32_t i = 0; i < size; ++i) {
        SSttBlk* p = taosArrayGet(pBlockLoadInfo->aSttBlk, i);
        if (p->suid == suid) {
          taosArrayPush(pTmp, p);
        }
      }

      taosArrayDestroy(pBlockLoadInfo->aSttBlk);
      pBlockLoadInfo->aSttBlk = pTmp;
    }
  }

  size_t size = taosArrayGetSize(pBlockLoadInfo->aSttBlk);

  // find the start block
  (*pIter)->iSttBlk = binarySearchForStartBlock(pBlockLoadInfo->aSttBlk->pData, size, uid, backward);
  if ((*pIter)->iSttBlk != -1) {
    (*pIter)->pSttBlk = taosArrayGet(pBlockLoadInfo->aSttBlk, (*pIter)->iSttBlk);
    (*pIter)->iRow = ((*pIter)->backward) ? (*pIter)->pSttBlk->nRow : -1;
  }

_exit:
  return code;
}

void tLDataIterClose(SLDataIter *pIter) {
  taosMemoryFree(pIter);
}

void tLDataIterNextBlock(SLDataIter *pIter) {
  int32_t step = pIter->backward ? -1 : 1;
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
    pIter->pSttBlk = (SSttBlk *)taosArrayGet(pIter->pBlockLoadInfo->aSttBlk, pIter->iSttBlk);
  }
}

static void findNextValidRow(SLDataIter *pIter, const char* idStr) {
  int32_t step = pIter->backward ? -1 : 1;

  bool        hasVal = false;
  int32_t     i = pIter->iRow;

  SBlockData *pBlockData = loadLastBlock(pIter, idStr);

  // mostly we only need to find the start position for a given table
  if ((((i == 0) && (!pIter->backward)) || (i == pBlockData->nRow - 1 && pIter->backward)) && pBlockData->aUid != NULL) {
    i = binarySearchForStartRowIndex((uint64_t*)pBlockData->aUid, pBlockData->nRow, pIter->uid, pIter->backward);
    if (i == -1) {
      pIter->iRow = -1;
      return;
    }
  }

  for (; i < pBlockData->nRow && i >= 0; i += step) {
    if (pBlockData->aUid != NULL) {
      if (!pIter->backward) {
        /*if (pBlockData->aUid[i] < pIter->uid) {
          continue;
        } else */if (pBlockData->aUid[i] > pIter->uid) {
          break;
        }
      } else {
        /*if (pBlockData->aUid[i] > pIter->uid) {
          continue;
        } else */if (pBlockData->aUid[i] < pIter->uid) {
          break;
        }
      }
    }

    int64_t ts = pBlockData->aTSKEY[i];
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

    int64_t ver = pBlockData->aVersion[i];
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

bool tLDataIterNextRow(SLDataIter *pIter, const char* idStr) {
  int32_t code = 0;
  int32_t step = pIter->backward ? -1 : 1;

  // no qualified last file block in current file, no need to fetch row
  if (pIter->pSttBlk == NULL) {
    return false;
  }

  int32_t iBlockL = pIter->iSttBlk;
  SBlockData *pBlockData = loadLastBlock(pIter, idStr);
  pIter->iRow += step;

  while (1) {
    findNextValidRow(pIter, idStr);

    if (pIter->iRow >= pBlockData->nRow || pIter->iRow < 0) {
      tLDataIterNextBlock(pIter);
      if (pIter->pSttBlk == NULL) {  // no more data
        goto _exit;
      }
    } else {
      break;
    }

    if (iBlockL != pIter->iSttBlk) {
      pBlockData = loadLastBlock(pIter, idStr);
      pIter->iRow += step;
    }
  }

  pIter->rInfo.suid = pBlockData->suid;
  pIter->rInfo.uid = pBlockData->uid;
  pIter->rInfo.row = tsdbRowFromBlockData(pBlockData, pIter->iRow);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
  }

  return (code == TSDB_CODE_SUCCESS) && (pIter->pSttBlk != NULL);
}

SRowInfo *tLDataIterGet(SLDataIter *pIter) { return &pIter->rInfo; }

// SMergeTree =================================================
static FORCE_INLINE int32_t tLDataIterCmprFn(const void *p1, const void *p2) {
  SLDataIter *pIter1 = (SLDataIter *)(((uint8_t *)p1) - sizeof(SRBTreeNode));
  SLDataIter *pIter2 = (SLDataIter *)(((uint8_t *)p2) - sizeof(SRBTreeNode));

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

int32_t tMergeTreeOpen(SMergeTree *pMTree, int8_t backward, SDataFReader *pFReader, uint64_t suid, uint64_t uid,
                       STimeWindow *pTimeWindow, SVersionRange *pVerRange, void* pBlockLoadInfo, const char* idStr) {
  pMTree->backward = backward;
  pMTree->pIter = NULL;
  pMTree->pIterList = taosArrayInit(4, POINTER_BYTES);
  if (pMTree->pIterList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMTree->idStr = idStr;

  tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
  int32_t code = TSDB_CODE_SUCCESS;

  SSttBlockLoadInfo* pLoadInfo = NULL;
  if (pBlockLoadInfo == NULL) {
    if (pMTree->pLoadInfo == NULL) {
      pMTree->destroyLoadInfo = true;
      pMTree->pLoadInfo = tCreateLastBlockLoadInfo();
    }

    pLoadInfo = pMTree->pLoadInfo;
  } else {
    pLoadInfo = pBlockLoadInfo;
  }

  for (int32_t i = 0; i < pFReader->pSet->nSttF; ++i) {  // open all last file
    struct SLDataIter* pIter = NULL;
    code = tLDataIterOpen(&pIter, pFReader, i, pMTree->backward, suid, uid, pTimeWindow, pVerRange, &pLoadInfo[i]);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }

    bool hasVal = tLDataIterNextRow(pIter, pMTree->idStr);
    if (hasVal) {
      taosArrayPush(pMTree->pIterList, &pIter);
      tMergeTreeAddIter(pMTree, pIter);
    } else {
      tLDataIterClose(pIter);
    }
  }

  return code;

_end:
  tMergeTreeClose(pMTree);
  return code;
}

void tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter) { tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pIter); }

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
      int32_t c = pMTree->rbt.cmprFn(RBTREE_NODE_PAYLOAD(&pMTree->pIter->node), RBTREE_NODE_PAYLOAD(&pIter->node));
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

TSDBROW tMergeTreeGetRow(SMergeTree *pMTree) { return pMTree->pIter->rInfo.row; }

void tMergeTreeClose(SMergeTree *pMTree) {
  size_t size = taosArrayGetSize(pMTree->pIterList);
  for (int32_t i = 0; i < size; ++i) {
    SLDataIter *pIter = taosArrayGetP(pMTree->pIterList, i);
    tLDataIterClose(pIter);
  }

  pMTree->pIterList = taosArrayDestroy(pMTree->pIterList);
  pMTree->pIter = NULL;

  if (pMTree->destroyLoadInfo) {
    pMTree->pLoadInfo = destroyLastBlockLoadInfo(pMTree->pLoadInfo);
    pMTree->destroyLoadInfo = false;
  }
}
