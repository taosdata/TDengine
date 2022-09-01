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
  SBlockL      *pBlockL;
  SDataFReader *pReader;
  int32_t       iLast;
  int8_t        backward;
  SArray       *aBlockL;
  int32_t       iBlockL;
  SBlockData    bData;
  int32_t       iRow;
  SRowInfo      rInfo;
  uint64_t      uid;
  STimeWindow   timeWindow;
  SVersionRange verRange;
} SLDataIter;

int32_t tLDataIterOpen(struct SLDataIter **pIter, SDataFReader *pReader, int32_t iLast, int8_t backward, uint64_t uid,
                       STimeWindow *pTimeWindow, SVersionRange *pRange) {
  int32_t code = 0;
  *pIter = taosMemoryCalloc(1, sizeof(SLDataIter));

  (*pIter)->uid = uid;
  (*pIter)->timeWindow = *pTimeWindow;
  (*pIter)->verRange = *pRange;
  (*pIter)->pReader = pReader;
  (*pIter)->iLast = iLast;
  (*pIter)->backward = backward;
  (*pIter)->aBlockL = taosArrayInit(0, sizeof(SBlockL));
  if ((*pIter)->aBlockL == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataCreate(&(*pIter)->bData);
  if (code) {
    goto _exit;
  }

  code = tsdbReadBlockL(pReader, iLast, (*pIter)->aBlockL);
  if (code) {
    goto _exit;
  }

  size_t size = taosArrayGetSize((*pIter)->aBlockL);

  // find the start block
  int32_t index = -1;
  if (!backward) {  // asc
    for (int32_t i = 0; i < size; ++i) {
      SBlockL *p = taosArrayGet((*pIter)->aBlockL, i);
      if (p->minUid <= uid && p->maxUid >= uid) {
        index = i;
        break;
      }
    }
  } else {  // desc
    for (int32_t i = size - 1; i >= 0; --i) {
      SBlockL *p = taosArrayGet((*pIter)->aBlockL, i);
      if (p->minUid <= uid && p->maxUid >= uid) {
        index = i;
        break;
      }
    }
  }

  (*pIter)->iBlockL = index;
  if (index != -1) {
    (*pIter)->pBlockL = taosArrayGet((*pIter)->aBlockL, (*pIter)->iBlockL);
  }

_exit:
  return code;
}

void tLDataIterClose(SLDataIter *pIter) {
  tBlockDataDestroy(&pIter->bData, 1);
  taosArrayDestroy(pIter->aBlockL);
}

extern int32_t tsdbReadLastBlockEx(SDataFReader *pReader, int32_t iLast, SBlockL *pBlockL, SBlockData *pBlockData);

void tLDataIterNextBlock(SLDataIter *pIter) {
  int32_t step = pIter->backward? -1:1;
  pIter->iBlockL += step;

  int32_t index = -1;
  size_t size = taosArrayGetSize(pIter->aBlockL);
  for(int32_t i = pIter->iBlockL; i < size && i >= 0; i += step) {
    SBlockL *p = taosArrayGet(pIter->aBlockL, i);
    if (p->minUid <= pIter->uid && p->maxUid >= pIter->uid) {
      index = i;
      break;
    }

    if (p->minUid > pIter->uid) {
      break;
    }
  }

  if (index == -1) {
    pIter->pBlockL = NULL;
  }  else {
    pIter->pBlockL = (SBlockL *)taosArrayGet(pIter->aBlockL, pIter->iBlockL);
  }
}

static void findNextValidRow(SLDataIter* pIter) {
  int32_t step = pIter->backward? -1:1;

  bool hasVal = false;
  int32_t i = pIter->iRow;
  for (; i < pIter->bData.nRow && i >= 0; i += step) {
    if (pIter->bData.aUid != NULL) {
      if (!pIter->backward) {
        if (pIter->bData.aUid[i] < pIter->uid) {
          continue;
        } else if (pIter->bData.aUid[i] > pIter->uid) {
          break;
        }
      } else {
        if (pIter->bData.aUid[i] > pIter->uid) {
          continue;
        } else if (pIter->bData.aUid[i] < pIter->uid) {
          break;
        }
      }
    }

    int64_t ts = pIter->bData.aTSKEY[i];
    if (ts < pIter->timeWindow.skey) {
      continue;
    }

    int64_t ver = pIter->bData.aVersion[i];
    if (ver < pIter->verRange.minVer) {
      continue;
    }

    // no data any more, todo opt handle desc case
    if (ts > pIter->timeWindow.ekey) {
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

  pIter->iRow = (hasVal)? i:-1;
}

bool tLDataIterNextRow(SLDataIter *pIter) {
  int32_t code = 0;
  int32_t step = pIter->backward? -1:1;

  // no qualified last file block in current file, no need to fetch row
  if (pIter->pBlockL == NULL) {
    return false;
  }

  int32_t iBlockL = pIter->iBlockL;
  if (pIter->bData.nRow == 0 && pIter->pBlockL != NULL) {  // current block not loaded yet
    code = tsdbReadLastBlockEx(pIter->pReader, pIter->iLast, pIter->pBlockL, &pIter->bData);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    pIter->iRow = (pIter->backward)? pIter->bData.nRow:-1;
  }

  pIter->iRow += step;
  findNextValidRow(pIter);

  if (pIter->iRow >= pIter->bData.nRow || pIter->iRow < 0) {
    tLDataIterNextBlock(pIter);
    if (pIter->pBlockL == NULL) {  // no more data
      goto _exit;
    }
  }

  if (iBlockL != pIter->iBlockL) {
    if (pIter->pBlockL) {
      code = tsdbReadLastBlockEx(pIter->pReader, pIter->iLast, pIter->pBlockL, &pIter->bData);
      if (code) {
        goto _exit;
      }

      pIter->iRow = pIter->backward? (pIter->bData.nRow-1):0;
      findNextValidRow(pIter);
      if (pIter->iRow >= pIter->bData.nRow || pIter->iRow < 0) {
        // todo try next block
      }
    } else {
      // no more data
      goto _exit;
    }
  }

  pIter->rInfo.suid = pIter->bData.suid;
  pIter->rInfo.uid = pIter->bData.uid;
  pIter->rInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);

_exit:
  if  (code != TSDB_CODE_SUCCESS) {
    return false;
  } else {
    return pIter->pBlockL != NULL;
  }
}

SRowInfo *tLDataIterGet(SLDataIter *pIter) {
  // TODO
  return NULL;
}

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

void tMergeTreeOpen(SMergeTree *pMTree, int8_t backward, SDataFReader* pFReader, uint64_t uid, STimeWindow* pTimeWindow, SVersionRange* pVerRange) {
  pMTree->backward = backward;
  pMTree->pNode = NULL;
  pMTree->pIter = NULL;
  tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);

  struct SLDataIter* pIterList[TSDB_DEFAULT_LAST_FILE] = {0};
  for(int32_t i = 0; i < pFReader->pSet->nLastF; ++i) { // open all last file
    /*int32_t code = */tLDataIterOpen(&pIterList[i], pFReader, i, pMTree->backward, uid, pTimeWindow, pVerRange);
    bool hasVal = tLDataIterNextRow(pIterList[i]);
    if (hasVal) {
      tMergeTreeAddIter(pMTree, pIterList[i]);
    }
  }
}

void tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter) { tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pIter); }

bool tMergeTreeNext(SMergeTree* pMTree) {
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
      int32_t c = pMTree->rbt.cmprFn(pMTree->pIter->node.payload, &pIter->node.payload);
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

TSDBROW tMergeTreeGetRow(SMergeTree* pMTree) {
  return pMTree->pIter->rInfo.row;
}

void tMergeTreeClose(SMergeTree* pMTree) {

}
