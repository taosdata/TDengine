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

#ifndef TDENGINE_CACHE_TYPES_H
#define TDENGINE_CACHE_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

struct cache_t;
typedef struct cache_t cache_t;

struct cacheOption;
typedef struct cacheOption cacheOption;

struct cacheMutex;
typedef struct cacheMutex cacheMutex;

struct cacheSlabClass;
typedef struct cacheSlabClass cacheSlabClass;

struct cacheSlabLruClass;
typedef struct cacheSlabLruClass cacheSlabLruClass;

typedef enum cacheLockFlag {
  CACHE_LOCK_HASH = 1,
  CACHE_LOCK_SLAB = 2,
  CACHE_LOCK_LRU  = 4,
} cacheLockFlag;

#define IS_CACHE_LOCK_HASH(flag) (flag & CACHE_LOCK_HASH)
#define IS_CACHE_LOCK_SLAB(flag) (flag & CACHE_LOCK_SLAB)
#define IS_CACHE_LOCK_LRU(flag)  (flag & CACHE_LOCK_LRU)

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_TYPES_H */