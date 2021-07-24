/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#ifndef TDENGINE_CACHELRU_H
#define TDENGINE_CACHELRU_H

#include "cache.h"
#include "cacheMutex.h"
#include "cacheTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cacheSlabLruClass {
  cacheItem*    tail;   /* tail of lru item list */
  uint32_t      num;    /* number of lru list items */
  uint64_t      bytes;  /* total size of lru list items */
  int           id;     /* lru id */
  cacheMutex    mutex;
} cacheSlabLruClass;

void  cacheLruUnlinkItem(cache_t*, cacheItem*, bool lock);
void  cacheLruLinkItem(cache_t*, cacheItem*, bool lock);

void  cacheLruMoveToHead(cache_t*, cacheItem*, bool lock);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHELRU_H */