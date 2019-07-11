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

#ifndef TDENGINE_TLOSERTREE_H
#define TDENGINE_TLOSERTREE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef int (*__merge_compare_fn_t)(const void *, const void *, void *param);

typedef struct SLoserTreeNode {
  int32_t index;
  void *  pData;
} SLoserTreeNode;

typedef struct SLoserTreeInfo {
  int32_t              numOfEntries;
  int32_t              totalEntries;
  __merge_compare_fn_t comparaFn;
  void *               param;

  SLoserTreeNode *pNode;
} SLoserTreeInfo;

uint8_t tLoserTreeCreate(SLoserTreeInfo **pTree, int32_t numOfEntries, void *param, __merge_compare_fn_t compareFn);

void tLoserTreeInit(SLoserTreeInfo *pTree);

void tLoserTreeAdjust(SLoserTreeInfo *pTree, int32_t idx);

void tLoserTreeRebuild(SLoserTreeInfo *pTree);

void tLoserTreeDisplay(SLoserTreeInfo *pTree);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TLOSERTREE_H
