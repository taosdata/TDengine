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
#include <stdlib.h>

#include "tsdbCache.h"

STsdbCache *tsdbCreateCache(int32_t numOfBlocks) {
  STsdbCache *pCacheHandle = (STsdbCache *)malloc(sizeof(STsdbCache));
  if (pCacheHandle == NULL) {
    // TODO : deal with the error
    return NULL;
  }

  return pCacheHandle;
}

int32_t tsdbFreeCache(STsdbCache *pHandle) { return 0; }

void *tsdbAllocFromCache(STsdbCache *pCache, int64_t bytes) {
  // TODO: implement here
  void *ptr = malloc(bytes);
  if (ptr == NULL) return NULL;

  return ptr;
}