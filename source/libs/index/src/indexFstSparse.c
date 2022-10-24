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

static FORCE_INLINE void sparSetInitBuf(int32_t *buf, int32_t cap) {
  for (int32_t i = 0; i < cap; i++) {
    buf[i] = -1;
  }
}
FstSparseSet *sparSetCreate(int32_t sz) {
  FstSparseSet *ss = taosMemoryCalloc(1, sizeof(FstSparseSet));
  if (ss == NULL) {
    return NULL;
  }

  ss->dense = (int32_t *)taosMemoryMalloc(sz * sizeof(int32_t));
  ss->sparse = (int32_t *)taosMemoryMalloc(sz * sizeof(int32_t));
  sparSetInitBuf(ss->dense, sz);
  sparSetInitBuf(ss->sparse, sz);

  ss->cap = sz;

  ss->size = 0;
  return ss;
}
void sparSetDestroy(FstSparseSet *ss) {
  if (ss == NULL) {
    return;
  }
  taosMemoryFree(ss->dense);
  taosMemoryFree(ss->sparse);
  taosMemoryFree(ss);
}
uint32_t sparSetLen(FstSparseSet *ss) {
  // Get occupied size
  return ss == NULL ? 0 : ss->size;
}
bool sparSetAdd(FstSparseSet *ss, int32_t ip, int32_t *idx) {
  if (ss == NULL) {
    return false;
  }
  if (ip >= ss->cap || ip < 0) {
    return false;
  }
  uint32_t i = ss->size;
  ss->dense[i] = ip;
  ss->sparse[ip] = i;
  ss->size += 1;

  if (idx != NULL) *idx = i;

  return true;
}
bool sparSetGet(FstSparseSet *ss, int32_t idx, int32_t *ip) {
  if (idx >= ss->cap || idx >= ss->size || idx < 0) {
    return false;
  }
  int32_t val = ss->dense[idx];
  if (ip != NULL) {
    *ip = val;
  }
  return val == -1 ? false : true;
}
bool sparSetContains(FstSparseSet *ss, int32_t ip) {
  if (ip >= ss->cap || ip < 0) {
    return false;
  }

  int32_t i = ss->sparse[ip];
  if (i >= 0 && i < ss->cap && i < ss->size && ss->dense[i] == ip) {
    return true;
  } else {
    return false;
  }
}
void sparSetClear(FstSparseSet *ss) {
  if (ss == NULL) {
    return;
  }
  sparSetInitBuf(ss->dense, ss->cap);
  sparSetInitBuf(ss->sparse, ss->cap);
  ss->size = 0;
}
