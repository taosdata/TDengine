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

#ifndef _TD_UTIL_LOSERTREE_H_
#define _TD_UTIL_LOSERTREE_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int32_t (*__merge_compare_fn_t)(const void *, const void *, void *param);

typedef struct STreeNode {
  int32_t index;
  void   *pData;  // TODO remove it?
} STreeNode;

typedef struct SMultiwayMergeTreeInfo {
  int32_t              numOfSources;
  int32_t              totalSources;
  __merge_compare_fn_t comparFn;
  void                *param;
  struct STreeNode    *pNode;
} SMultiwayMergeTreeInfo;

#define tMergeTreeGetChosenIndex(t_) ((t_)->pNode[0].index)
#define tMergeTreeGetAdjustIndex(t_) (tMergeTreeGetChosenIndex(t_) + (t_)->numOfSources)

int32_t tMergeTreeCreate(SMultiwayMergeTreeInfo **pTree, uint32_t numOfEntries, void *param,
                         __merge_compare_fn_t compareFn);

void tMergeTreeDestroy(SMultiwayMergeTreeInfo **pTree);

void tMergeTreeAdjust(SMultiwayMergeTreeInfo *pTree, int32_t idx);

void tMergeTreeRebuild(SMultiwayMergeTreeInfo *pTree);

void tMergeTreePrint(const SMultiwayMergeTreeInfo *pTree);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_LOSERTREE_H_*/
