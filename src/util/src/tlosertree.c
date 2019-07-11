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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "taosmsg.h"
#include "tlog.h"
#include "tlosertree.h"

// set initial value for loser tree
void tLoserTreeInit(SLoserTreeInfo* pTree) {
  assert((pTree->totalEntries & 0x01) == 0 && (pTree->numOfEntries << 1 == pTree->totalEntries));

  for (int32_t i = 0; i < pTree->totalEntries; ++i) {
    if (i < pTree->numOfEntries) {
      pTree->pNode[i].index = -1;
    } else {
      pTree->pNode[i].index = i - pTree->numOfEntries;
    }
  }
}

/*
 * display whole loser tree on screen for debug purpose only.
 */
void tLoserTreeDisplay(SLoserTreeInfo* pTree) {
  printf("the value of loser tree:\t");
  for (int32_t i = 0; i < pTree->totalEntries; ++i) printf("%d\t", pTree->pNode[i].index);
  printf("\n");
}

uint8_t tLoserTreeCreate(SLoserTreeInfo** pTree, int32_t numOfEntries, void* param, __merge_compare_fn_t compareFn) {
  int32_t totalEntries = numOfEntries << 1;

  *pTree = (SLoserTreeInfo*)calloc(1, sizeof(SLoserTreeInfo) + sizeof(SLoserTreeNode) * totalEntries);
  if ((*pTree) == NULL) {
    pError("allocate memory for losertree failed. out of memory");
    return TSDB_CODE_CLI_OUT_OF_MEMORY;
  }

  (*pTree)->pNode = (SLoserTreeNode*)(((char*)(*pTree)) + sizeof(SLoserTreeInfo));

  (*pTree)->numOfEntries = numOfEntries;
  (*pTree)->totalEntries = totalEntries;
  (*pTree)->param = param;
  (*pTree)->comparaFn = compareFn;

  // set initial value for loser tree
  tLoserTreeInit(*pTree);

#ifdef _DEBUG_VIEW
  printf("the initial value of loser tree:\n");
  tLoserTreeDisplay(*pTree);
#endif

  for (int32_t i = totalEntries - 1; i >= numOfEntries; i--) {
    tLoserTreeAdjust(*pTree, i);
  }

#if defined(_DEBUG_VIEW)
  printf("after adjust:\n");
  tLoserTreeDisplay(*pTree);
  printf("initialize local reducer completed!\n");
#endif

  return TSDB_CODE_SUCCESS;
}

void tLoserTreeAdjust(SLoserTreeInfo* pTree, int32_t idx) {
  assert(idx <= pTree->totalEntries - 1 && idx >= pTree->numOfEntries && pTree->totalEntries >= 2);

  if (pTree->totalEntries == 2) {
    pTree->pNode[0].index = 0;
    pTree->pNode[1].index = 0;
    return;
  }

  int32_t        parentId = idx >> 1;
  SLoserTreeNode kLeaf = pTree->pNode[idx];

  while (parentId > 0) {
    if (pTree->pNode[parentId].index == -1) {
      pTree->pNode[parentId] = kLeaf;
      return;
    }

    int32_t ret = pTree->comparaFn(&pTree->pNode[parentId], &kLeaf, pTree->param);
    if (ret < 0) {
      SLoserTreeNode t = pTree->pNode[parentId];
      pTree->pNode[parentId] = kLeaf;
      kLeaf = t;
    }

    parentId = parentId >> 1;
  }

  if (memcmp(&kLeaf, &pTree->pNode[1], sizeof(kLeaf)) != 0) {
    // winner cannot be identical to the loser, which is pTreeNode[1]
    pTree->pNode[0] = kLeaf;
  }
}

void tLoserTreeRebuild(SLoserTreeInfo* pTree) {
  assert((pTree->totalEntries & 0x1) == 0);

  tLoserTreeInit(pTree);
  for (int32_t i = pTree->totalEntries - 1; i >= pTree->numOfEntries; i--) {
    tLoserTreeAdjust(pTree, i);
  }
}
