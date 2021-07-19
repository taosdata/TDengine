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

#include "cacheint.h"
#include "cacheItem.h"
#include "cacheDefine.h"
#include "cacheSlab.h"

static cache_item_t* do_slab_alloc(cache_t *cache, size_t size, unsigned int id);
static int  do_slab_newslab(cache_t *cache, unsigned int id);
static bool reach_cache_limit(cache_t *cache, int len);
static int  grow_slab_array(cache_t *cache, unsigned int id);
static void *alloc_memory(cache_t *cache, size_t size);
static void split_slab_page_into_freelist(cache_t *cache,char *ptr, const unsigned int id);
static void do_slabs_free(cache_t *cache, void *ptr, unsigned int id);

cache_code_t slab_init(cache_t *cache) {
  cache->slabs = NULL;

  cache->slabs = calloc(1, sizeof(cache_slabcls_t) * MAX_NUMBER_OF_SLAB_CLASSES);
  if (cache->slabs == NULL) {
    goto error;
  }

  int i = 0;
  size_t size = sizeof(cache_item_t) + CHUNK_SIZE;
  while (i < MAX_NUMBER_OF_SLAB_CLASSES) {
    cache_slabcls_t *slab = calloc(1, sizeof(cache_slabcls_t));
    if (slab == NULL) {
      goto error;
    }

    if (size % CHUNK_ALIGN_BYTES) {
      size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
    }
    slab->size = size;
    slab->perSlab = SLAB_PAGE_SIZE / size;

    cache->slabs[i] = slab;

    size *= cache->options.factor;
  }
  cache->power_largest = i;

  return CACHE_OK;

error:
  if (cache->slabs != NULL) {
    while (i < MAX_NUMBER_OF_SLAB_CLASSES) {
      if (cache->slabs[i] == NULL) {
        continue;
      }
      free(cache->slabs[i]);
    }
  }
  return CACHE_FAIL;
}

unsigned int slabClsId(cache_t *cache, size_t size) {
  int i = 0;
  while (size > cache->slabs[i]->size) {
    if (i++ > cache->power_largest) {
      return cache->power_largest;
    }
  }

  return i;
}

cache_item_t* slab_alloc_item(cache_t *cache, size_t ntotal) {
  unsigned int id = slabClsId(cache, ntotal);
  cache_item_t *item = NULL;
  int i;

  for (i = 0; i < 10; ++i) {
    item = do_slab_alloc(cache, ntotal, id);
    if (item) {
      return item;
    }
  }
  return NULL;
}

static bool reach_cache_limit(cache_t *cache, int len) {
  if (cache->reachLimit) {
    return true;
  }

  if (cache->alloced + len >= cache->options.limit) {
    return true;
  }

  return false;
}

static void do_slabs_free(cache_t *cache, void *ptr, unsigned int id) {
  cache_slabcls_t *p = cache->slabs[id];
  cache_item_t *item = (cache_item_t *)ptr;

  if (!(item->flags & ITEM_CHUNKED)) {
    item->flags = ITEM_SLABBED;
    item->slabClsId = id;
    item->prev = NULL;
    item->next = p->freeItem;
    if (item->next) item->next->prev = item;
    p->freeItem = item;
    p->nFree++;
  } else {

  }
}

static void *alloc_memory(cache_t *cache, size_t size) {
  cache->alloced += size;
  return malloc(size);
}

static int grow_slab_array(cache_t *cache, unsigned int id) {
  cache_slabcls_t *p = cache->slabs[id];
  if (p->nAllocSlabs == p->nArray) {
    size_t new_size =  (p->nArray != 0) ? p->nArray * 2 : 16;
    void *new_array = realloc(p->slabArray, new_size * sizeof(void *));
    if (new_array == NULL) return 0;
    p->nArray = new_size;
    p->slabArray = new_array;
  }

  return 1;
}

static void split_slab_page_into_freelist(cache_t *cache, char *ptr, const unsigned int id) {
  cache_slabcls_t *p = cache->slabs[id];
  int i = 0;
  for (i = 0; i < p->perSlab; i++) {
    do_slabs_free(cache, ptr, id);
    ptr += p->size;
  }
}

static int do_slab_newslab(cache_t *cache, unsigned int id) {
  cache_slabcls_t *p = cache->slabs[id];
  char *ptr;
  int len = p->size * p->perSlab;

  if (reach_cache_limit(cache, len)) { 
    return CACHE_REACH_LIMIT;
  }

  if (grow_slab_array(cache, id) == 0 || (ptr = alloc_memory(cache, len)) == NULL) {
    return CACHE_ALLOC_FAIL;
  }

  memset(ptr, 0, (size_t)len);
  split_slab_page_into_freelist(cache, ptr, id);

  p->slabArray[p->nArray++] = ptr;

  return CACHE_OK;
}

static cache_item_t* do_slab_alloc(cache_t *cache, size_t size, unsigned int id) {
  cache_slabcls_t *p = cache->slabs[id];
  cache_item_t *item = NULL;

  if (p->nFree == 0) {
    do_slab_newslab(cache, id);
  }

  if (p->nFree > 0) {
    item = p->freeItem;
    p->freeItem = item->next;
    if (item->next) item->next->prev = NULL;
    item->flags &= ~ITEM_SLABBED;
    p->nFree -= 1;
  }

  return item;
}