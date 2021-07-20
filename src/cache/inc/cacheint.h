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

#ifndef TDENGINE_CACHEINT_H
#define TDENGINE_CACHEINT_H

#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include "cache.h"
#include "cacheTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t (*hash_func)(const void *key, size_t length);

// cache statistics data
typedef struct cache_stat_t {
  int32_t outMemory;
} cache_stat_t;

typedef struct cache_lru_class_t {
  cache_item_t* tail;   // tail of lru item list
  uint32_t      num;    // number of lru list items
  uint64_t      bytes;  // total size of lru list items
} cache_lru_class_t;

typedef uint32_t (*cache_hash_func_t)(const void *key, size_t length);

struct cache_t {  
  cache_option_t options;

  cache_slab_class_t** slabs;   /* array of slab pointers */

  cache_lru_class_t*  lruArray; /* LRU item list array */

  cache_hash_func_t hash;

  size_t alloced;               /* allocated memory size */

  int power_largest;

  cache_hashtable_t* table;

  cache_stat_t stat;
    
  cache_t* next;                /* cache list link in manager */
};

// the global cache manager
typedef struct cache_manager_t {

} cache_manager_t;

typedef struct cache_key_t {
  const char* key;
  uint8_t nkey;
} cache_key_t;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHEINT_H