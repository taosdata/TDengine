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
typedef struct SLDataIter {
  SRBTreeNode   node;
  SSstBlk      *pSstBlk;
  SDataFReader *pReader;
  int32_t       iSst;
  int8_t        backward;
  SArray       *aSstBlk;
  int32_t       iSstBlk;
  SBlockData    bData[2];
  int32_t       loadIndex;
  int32_t       iRow;
  SRowInfo      rInfo;
  uint64_t      uid;
  STimeWindow   timeWindow;
  SVersionRange verRange;
} SLDataIter;

static SBlockData* getCurrentBlock(SLDataIter* pIter) {
  return &pIter->bData[pIter->loadIndex];
}

static SBlockData* getNextBlock(SLDataIter* pIter) {
  pIter->loadIndex ^= 1;
  return getCurrentBlock(pIter);
}

int32_t tLDataIterOpen(struct SLDataIter **pIter, SDataFReader *pReader, int32_t iSst, int8_t backward, uint64_t uid,
                       STimeWindow *pTimeWindow, SVersionRange *pRange) {
  int32_t code = 0;
  *pIter = taosMemoryCalloc(1, sizeof(SLDataIter));
  if (*pIter == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  (*pIter)->uid = uid;
  (*pIter)->timeWindow = *pTimeWindow;
  (*pIter)->verRange = *pRange;
  (*pIter)->pReader = pReader;
  (*pIter)->iSst = iSst;
  (*pIter)->backward = backward;
  (*pIter)->aSstBlk = taosArrayInit(0, sizeof(SSstBlk));
  if ((*pIter)->aSstBlk == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataCreate(&(*pIter)->bData[0]);
  if (code) {
    goto _exit;
  }

  code = tBlockDataCreate(&(*pIter)->bData[1]);
  if (code) {
    goto _exit;
  }

  code = tsdbReadSstBlk(pReader, iSst, (*pIter)->aSstBlk);
  if (code) {
    goto _exit;
  }

  size_t size = taosArrayGetSize((*pIter)->aSstBlk);

  // find the start block
  int32_t index = -1;
  if (!backward) {  // asc
    for (int32_t i = 0; i < size; ++i) {
      SSstBlk *p = taosArrayGet((*pIter)->aSstBlk, i);
      if (p->minUid <= uid && p->maxUid >= uid) {
        index = i;
        break;
      }
    }
  } else {  // desc
    for (int32_t i = size - 1; i >= 0; --i) {
      SSstBlk *p = taosArrayGet((*pIter)->aSstBlk, i);
      if (p->minUid <= uid && p->maxUid >= uid) {
        index = i;
        break;
      }
    }
  }

  (*pIter)->iSstBlk = index;
  if (index != -1) {
    (*pIter)->pSstBlk = taosArrayGet((*pIter)->aSstBlk, (*pIter)->iSstBlk);
  }

_exit:
  return code;
}

void tLDataIterClose(SLDataIter *pIter) {
  tBlockDataDestroy(&pIter->bData[0], 1);
  tBlockDataDestroy(&pIter->bData[1], 1);
  taosArrayDestroy(pIter->aSstBlk);
  taosMemoryFree(pIter);
}

extern int32_t tsdbReadSstBlockEx(SDataFReader *pReader, int32_t iSst, SSstBlk *pSstBlk, SBlockData *pBlockData);

void tLDataIterNextBlock(SLDataIter *pIter) {
  int32_t step = pIter->backward ? -1 : 1;
  pIter->iSstBlk += step;

  int32_t index = -1;
  size_t  size = taosArrayGetSize(pIter->aSstBlk);
  for (int32_t i = pIter->iSstBlk; i < size && i >= 0; i += step) {
    SSstBlk *p = taosArrayGet(pIter->aSstBlk, i);
    if ((!pIter->backward) && p->minUid > pIter->uid) {
      break;
    }

    if (pIter->backward && p->maxUid < pIter->uid) {
      break;
    }

    if (p->minUid <= pIter->uid && p->maxUid >= pIter->uid) {
      index = i;
      break;
    }
  }

  if (index == -1) {
    pIter->pSstBlk = NULL;
  } else {
    pIter->pSstBlk = (SSstBlk *)taosArrayGet(pIter->aSstBlk, pIter->iSstBlk);
  }
}

static void findNextValidRow(SLDataIter *pIter) {
  int32_t step = pIter->backward ? -1 : 1;

  bool    hasVal = false;
  int32_t i = pIter->iRow;
  SBlockData* pBlockData = getCurrentBlock(pIter);

  for (; i < pBlockData->nRow && i >= 0; i += step) {
    if (pBlockData->aUid != NULL) {
      if (!pIter->backward) {
        if (pBlockData->aUid[i] < pIter->uid) {
          continue;
        } else if (pBlockData->aUid[i] > pIter->uid) {
          break;
        }
      } else {
        if (pBlockData->aUid[i] > pIter->uid) {
          continue;
        } else if (pBlockData->aUid[i] < pIter->uid) {
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

    //  todo handle delete soon
#if 0
    TSDBKEY k = {.ts = ts, .version = ver};
        if (hasBeenDropped(pBlockScanInfo->delSkyline, &pBlockScanInfo->lastBlockDelIndex, &k, pLastBlockReader->order)) {
          continue;
        }
#endif

    hasVal = true;
    break;
  }

  pIter->iRow = (hasVal) ? i : -1;
}

bool tLDataIterNextRow(SLDataIter *pIter) {
  int32_t code = 0;
  int32_t step = pIter->backward ? -1 : 1;

  // no qualified last file block in current file, no need to fetch row
  if (pIter->pSstBlk == NULL) {
    return false;
  }

  int32_t iBlockL = pIter->iSstBlk;
  SBlockData* pBlockData = getCurrentBlock(pIter);

  if (pBlockData->nRow == 0 && pIter->pSstBlk != NULL) {  // current block not loaded yet
    pBlockData = getNextBlock(pIter);
    code = tsdbReadSstBlockEx(pIter->pReader, pIter->iSst, pIter->pSstBlk, pBlockData);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    pIter->iRow = (pIter->backward) ? pBlockData->nRow : -1;
  }

  pIter->iRow += step;

  while (1) {
    findNextValidRow(pIter);

    if (pIter->iRow >= pBlockData->nRow || pIter->iRow < 0) {
      tLDataIterNextBlock(pIter);
      if (pIter->pSstBlk == NULL) {  // no more data
        goto _exit;
      }
    } else {
      break;
    }

    if (iBlockL != pIter->iSstBlk) {
      pBlockData = getNextBlock(pIter);
      code = tsdbReadSstBlockEx(pIter->pReader, pIter->iSst, pIter->pSstBlk, pBlockData);
      if (code) {
        goto _exit;
      }
      pIter->iRow = pIter->backward ? (pBlockData->nRow - 1) : 0;
    }
  }

  pIter->rInfo.suid = pBlockData->suid;
  pIter->rInfo.uid = pBlockData->uid;
  pIter->rInfo.row = tsdbRowFromBlockData(pBlockData, pIter->iRow);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
  }

  return (code == TSDB_CODE_SUCCESS) && (pIter->pSstBlk != NULL);
}

SRowInfo *tLDataIterGet(SLDataIter *pIter) { return &pIter->rInfo; }

// SMergeTree =================================================
static FORCE_INLINE int32_t tLDataIterCmprFn(const void *p1, const void *p2) {
  SLDataIter *pIter1 = (SLDataIter *)(p1 - sizeof(SRBTreeNode));
  SLDataIter *pIter2 = (SLDataIter *)(p2 - sizeof(SRBTreeNode));

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

int32_t tMergeTreeOpen(SMergeTree *pMTree, int8_t backward, SDataFReader* pFReader, uint64_t uid, STimeWindow* pTimeWindow, SVersionRange* pVerRange) {
  pMTree->backward = backward;
  pMTree->pIter = NULL;
  pMTree->pIterList = taosArrayInit(4, POINTER_BYTES);
  if (pMTree->pIterList == NULL) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
  int32_t code = TSDB_CODE_OUT_OF_MEMORY;

  struct SLDataIter* pIterList[TSDB_DEFAULT_LAST_FILE] = {0};
  for(int32_t i = 0; i < pFReader->pSet->nSstF; ++i) { // open all last file
    code = tLDataIterOpen(&pIterList[i], pFReader, i, pMTree->backward, uid, pTimeWindow, pVerRange);
    if (code != TSDB_CODE_SUCCESS) {
      goto _end;
    }

    bool hasVal = tLDataIterNextRow(pIterList[i]);
    if (hasVal) {
      taosArrayPush(pMTree->pIterList, &pIterList[i]);
      tMergeTreeAddIter(pMTree, pIterList[i]);
    } else {
      tLDataIterClose(pIterList[i]);
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

    bool hasVal = tLDataIterNextRow(pIter);
    if (!hasVal) {
      pMTree->pIter = NULL;
    }

    // compare with min in RB Tree
    pIter = (SLDataIter *)tRBTreeMin(&pMTree->rbt);
    if (pMTree->pIter && pIter) {
      int32_t c = pMTree->rbt.cmprFn(pMTree->pIter->node.payload, pIter->node.payload);
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
}
