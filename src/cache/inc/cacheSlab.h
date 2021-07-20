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

#ifndef TDENGINE_CACHE_SLAB_H
#define TDENGINE_CACHE_SLAB_H

#include <stdlib.h> // for size_t
#include "cacheTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

struct cache_slab_class_t {
  unsigned int size;        /* sizes of items */
  unsigned int perSlab;     /* how many items per slab */

  cache_item_t *freeItem;   /* list of free item ptrs */
  unsigned int nFree;       /* free item count */
  unsigned int nAllocSlabs; /* how many slabs were allocated for this class */

  void **slabArray;         /* array of slab pointers */
  unsigned int nArray;      /* size of slab array */
};

cache_code_t slab_init(cache_t *);

unsigned int slabClsId(cache_t *cache, size_t size);

cache_item_t* slab_alloc_item(cache_t *cache, size_t ntotal);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHE_SLAB_H