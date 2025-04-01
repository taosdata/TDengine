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

#include "bseCache.h"

int32_t tableCacheOpen(int32_t cap, STableCache **p) {
  int32_t      code = 0;
  STableCache *pCache = taosMemoryCalloc(1, sizeof(STableCache));
  if (pCache == NULL) {
    return terrno;
  }
  pCache->cap = cap;

  *p = pCache;
  return code;
}
int32_t tableCacheGet(STableCache *pMgt, char *key, STableReader **pReader) {
  int32_t code = 0;
  return code;
}

void tableCacheClose(STableCache *pCache) {
  taosMemFree(pCache->pCache);
  taosMemFree(pCache);
}

int32_t blockCacheOpen(int32_t cap, SBlockCache **p) {
  int32_t code = 0;

  SBlockCache *pCache = taosMemoryCalloc(1, sizeof(SBlockCache));
  if (pCache == NULL) {
    return terrno;
  }
  pCache->cap = cap;

  *p = pCache;
  return code;
}
int32_t blockCacheGet(SBlockCache *pgt, char *key, SBlock **pBlock) {
  int32_t code = 0;
  return 0;
}
void blockCacheClose(SBlockCache *p) {
  taosMemFree(p->pCache);
  taosMemFree(p);
}