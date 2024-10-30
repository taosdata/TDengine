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

#define _DEFAULT_SOURCE
#include "tlosertree.h"
#include "taoserror.h"
#include "tlog.h"

// Set the initial value of the multiway merge tree.
static int32_t tMergeTreeInit(SMultiwayMergeTreeInfo* pTree) {
  if (!((pTree->totalSources & 0x01) == 0 && (pTree->numOfSources << 1 == pTree->totalSources))) {
    uError("losertree failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_INVALID_PARA;
  }

  for (int32_t i = 0; i < pTree->totalSources; ++i) {
    if (i < pTree->numOfSources) {
      pTree->pNode[i].index = -1;
    } else {
      pTree->pNode[i].index = i - pTree->numOfSources;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t tMergeTreeCreate(SMultiwayMergeTreeInfo** pTree, uint32_t numOfSources, void* param,
                         __merge_compare_fn_t compareFn) {
  int32_t totalEntries = numOfSources << 1u;

  SMultiwayMergeTreeInfo* pTreeInfo =
      (SMultiwayMergeTreeInfo*)taosMemoryCalloc(1, sizeof(SMultiwayMergeTreeInfo) + sizeof(STreeNode) * totalEntries);
  if (pTreeInfo == NULL) {
    uError("allocate memory for loser-tree failed. reason:%s", strerror(errno));
    return terrno;
  }

  pTreeInfo->pNode = (STreeNode*)(((char*)pTreeInfo) + sizeof(SMultiwayMergeTreeInfo));

  pTreeInfo->numOfSources = numOfSources;
  pTreeInfo->totalSources = totalEntries;
  pTreeInfo->param = param;
  pTreeInfo->comparFn = compareFn;

  // set initial value for loser tree
  int32_t code = tMergeTreeInit(pTreeInfo);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFree(pTreeInfo);
    return code;
  }

#ifdef _DEBUG_VIEW
  printf("the initial value of loser tree:\n");
  tLoserTreeDisplaypTreeInfo;
#endif

  for (int32_t i = totalEntries - 1; i >= numOfSources; i--) {
    code = tMergeTreeAdjust(pTreeInfo, i);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFree(pTreeInfo);
      return code;
    }
  }

#if defined(_DEBUG_VIEW)
  printf("after adjust:\n");
  tLoserTreeDisplaypTreeInfo;
  printf("initialize local reducer completed!\n");
#endif

  *pTree = pTreeInfo;
  return 0;
}

void tMergeTreeDestroy(SMultiwayMergeTreeInfo** pTree) {
  if (pTree == NULL || *pTree == NULL) {
    return;
  }

  taosMemoryFreeClear(*pTree);
}

int32_t tMergeTreeAdjust(SMultiwayMergeTreeInfo* pTree, int32_t idx) {
  int32_t code = 0;
  if (!(idx <= pTree->totalSources - 1 && idx >= pTree->numOfSources && pTree->totalSources >= 2)) {
    uError("losertree failed at: %s:%d", __func__, __LINE__);
    return TSDB_CODE_INVALID_PARA;
  }

  if (pTree->totalSources == 2) {
    pTree->pNode[0].index = 0;
    pTree->pNode[1].index = 0;
    return code;
  }

  int32_t   parentId = idx >> 1;
  STreeNode kLeaf = pTree->pNode[idx];

  while (parentId > 0) {
    STreeNode* pCur = &pTree->pNode[parentId];
    if (pCur->index == -1) {
      pTree->pNode[parentId] = kLeaf;
      return code;
    }

    int32_t ret = pTree->comparFn(pCur, &kLeaf, pTree->param);
    if (ret < 0) {
      STreeNode t = pTree->pNode[parentId];
      pTree->pNode[parentId] = kLeaf;
      kLeaf = t;
    }

    parentId = parentId >> 1;
  }

  if (memcmp(&kLeaf, &pTree->pNode[1], sizeof(kLeaf)) != 0) {
    // winner cannot be identical to the loser, which is pTreeNode[1]
    pTree->pNode[0] = kLeaf;
  }
  return code;
}

int32_t tMergeTreeRebuild(SMultiwayMergeTreeInfo* pTree) {
  int32_t code = tMergeTreeInit(pTree);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  for (int32_t i = pTree->totalSources - 1; i >= pTree->numOfSources; i--) {
    code = tMergeTreeAdjust(pTree, i);
    if (TSDB_CODE_SUCCESS != code) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

/*
 * display whole loser tree on screen for debug purpose only.
 */
void tMergeTreePrint(const SMultiwayMergeTreeInfo* pTree) {
  (void)printf("the value of loser tree:\t");
  for (int32_t i = 0; i < pTree->totalSources; ++i) {
    (void)printf("%d\t", pTree->pNode[i].index);
  }

  (void)printf("\n");
}
