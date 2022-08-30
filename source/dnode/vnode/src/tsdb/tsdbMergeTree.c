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
typedef struct {
  SRBTreeNode node;
  SBlockL    *pBlockL;
  SRowInfo   *pRowInfo;

  SDataFReader *pReader;
  int32_t       iLast;
  int8_t        backward;
  SArray       *aBlockL;
  int32_t       iBlockL;
  SBlockData    bData;
  int32_t       iRow;
  SRowInfo      rInfo;
} SLDataIter;

int32_t tLDataIterOpen(SLDataIter *pIter, SDataFReader *pReader, int32_t iLast, int8_t backward) {
  int32_t code = 0;

  pIter->pReader = pReader;
  pIter->iLast = iLast;
  pIter->backward = backward;

  pIter->aBlockL = taosArrayInit(0, sizeof(SBlockL));
  if (pIter->aBlockL == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  code = tBlockDataCreate(&pIter->bData);
  if (code) goto _exit;

  code = tsdbReadBlockL(pReader, iLast, pIter->aBlockL);
  if (code) goto _exit;

  if (backward) {
    pIter->iBlockL = taosArrayGetSize(pIter->aBlockL) - 1;
  } else {
    pIter->iBlockL = 0;
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
  if (pIter->backward) {
    pIter->iBlockL--;
  } else {
    pIter->iBlockL++;
  }

  if (pIter->iBlockL >= 0 && pIter->iBlockL < taosArrayGetSize(pIter->aBlockL)) {
    pIter->pBlockL = (SBlockL *)taosArrayGet(pIter->aBlockL, pIter->iBlockL);
  } else {
    pIter->pBlockL = NULL;
  }
}

int32_t tLDataIterNextRow(SLDataIter *pIter) {
  int32_t code = 0;

  int32_t iBlockL = pIter->iBlockL;
  if (pIter->backward) {
    pIter->iRow--;
    if (pIter->iRow < 0) {
      tLDataIterNextBlock(pIter);
    }
  } else {
    pIter->iRow++;
    if (pIter->iRow >= pIter->bData.nRow) {
      pIter->iBlockL++;
      tLDataIterNextBlock(pIter);
    }
  }

  if (iBlockL != pIter->iBlockL) {
    if (pIter->pBlockL) {
      code = tsdbReadLastBlockEx(pIter->pReader, pIter->iLast, pIter->pBlockL, &pIter->bData);
      if (code) goto _exit;
      pIter->iRow = 0;
    } else {
      // no more data
      goto _exit;
    }
  }

  pIter->rInfo.suid = pIter->bData.suid;
  pIter->rInfo.uid = pIter->bData.uid;
  pIter->rInfo.row = tsdbRowFromBlockData(&pIter->bData, pIter->iRow);

_exit:
  return code;
}

SRowInfo *tLDataIterGet(SLDataIter *pIter) {
  // TODO
  return NULL;
}

// SMergeTree =================================================
typedef struct {
  int8_t       backward;
  SRBTreeNode *pNode;
  SRBTree      rbt;
} SMergeTree;

static FORCE_INLINE int32_t tLDataIterCmprFn(const void *p1, const void *p2) {
  SLDataIter *pIter1 = (SLDataIter *)p1;
  SLDataIter *pIter2 = (SLDataIter *)p2;

  // TODO
  ASSERT(0);

  return 0;
}

void tMergeTreeOpen(SMergeTree *pMTree, int8_t backward) {
  pMTree->backward = backward;
  pMTree->pNode = NULL;
  tRBTreeCreate(&pMTree->rbt, tLDataIterCmprFn);
}

void tMergeTreeAddIter(SMergeTree *pMTree, SLDataIter *pIter) { tRBTreePut(&pMTree->rbt, (SRBTreeNode *)pIter); }