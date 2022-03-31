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

#include "indexFstSparse.h"

FstSparseSet *sparSetCreate(int32_t sz) {
  FstSparseSet *ss = taosMemoryCalloc(1, sizeof(FstSparseSet));
  if (ss = NULL) {
    return NULL;
  }

  ss->dense = taosArrayInit(sz, sizeof(uint32_t));
  ss->sparse = taosArrayInit(sz, sizeof(uint32_t));
  ss->size = sz;
  return ss;
}
void sparSetDestroy(FstSparseSet *ss) {
  if (ss == NULL) {
    return;
  }
  taosArrayDestroy(ss->dense);
  taosArrayDestroy(ss->sparse);
  taosMemoryFree(ss);
}
uint32_t sparSetLen(FstSparseSet *ss) { return ss == NULL ? 0 : ss->size; }
uint32_t sparSetAdd(FstSparseSet *ss, uint32_t ip) {
  if (ss == NULL) {
    return 0;
  }
  uint32_t i = ss->size;
  taosArraySet(ss->dense, i, &ip);
  taosArraySet(ss->sparse, ip, &i);
  ss->size += 1;
  return i;
}
uint32_t sparSetGet(FstSparseSet *ss, uint32_t i) {
  if (i >= taosArrayGetSize(ss->dense)) {
    return 0;
  }
  uint32_t *v = taosArrayGet(ss->dense, i);
  return *v;
}
bool sparSetContains(FstSparseSet *ss, uint32_t ip) {
  if (ip >= taosArrayGetSize(ss->sparse)) {
    return false;
  }
  uint32_t i = *(uint32_t *)taosArrayGet(ss->sparse, ip);
  if (i >= taosArrayGetSize(ss->dense)) {
    return false;
  }
  uint32_t v = *(uint32_t *)taosArrayGet(ss->dense, i);
  return v == ip;
}
void sparSetClear(FstSparseSet *ss) {
  if (ss == NULL) {
    return;
  }
  ss->size = 0;
}
