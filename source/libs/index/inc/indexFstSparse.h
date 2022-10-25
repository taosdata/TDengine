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

#ifndef _TD_INDEX_FST_SPARSE_H_
#define _TD_INDEX_FST_SPARSE_H_

#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FstSparseSet {
  int32_t *dense;
  int32_t *sparse;
  int32_t  size;
  int32_t  cap;
} FstSparseSet;

FstSparseSet *sparSetCreate(int32_t sz);
void          sparSetDestroy(FstSparseSet *s);
uint32_t      sparSetLen(FstSparseSet *ss);
bool          sparSetAdd(FstSparseSet *ss, int32_t ip, int32_t *val);
bool          sparSetGet(FstSparseSet *ss, int32_t i, int32_t *val);
bool          sparSetContains(FstSparseSet *ss, int32_t ip);
void          sparSetClear(FstSparseSet *ss);

#ifdef __cplusplus
}
#endif

#endif
