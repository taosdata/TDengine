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
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t (*hash_func)(const void *key, size_t length);

struct cache_context_t {
  // cache options
  cache_option_t options;

  // array of slab pointers
  slab_t** slabs;

  size_t alloced;

  int power_largest;

  hashtable_t* table;
  // list link pointer in manager
  struct cache_context_t* next;
};

// the global cache manager
typedef struct cache_manager_t {

} cache_manager_t;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHEINT_H