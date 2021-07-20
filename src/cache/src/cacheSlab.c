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
#include "osTime.h"

typedef enum cache_lru_list_t {
  CACHE_LRU_HOT   = 0,
  CACHE_LRU_WARM  = 64,
  CACHE_LRU_COLD  = 128,
} cache_lru_list_t;

static cache_item_t* do_slab_alloc(cache_t *cache, size_t size, unsigned int id);
static int  do_slab_newslab(cache_t *cache, unsigned int id);
static bool reach_cache_limit(cache_t *cache, int len);
static int  grow_slab_array(cache_t *cache, unsigned int id);
static void *alloc_memory(cache_t *cache, size_t size);
static void split_slab_page_into_freelist(cache_t *cache,char *ptr, const unsigned int id);
static void do_slabs_free(cache_t *cache, void *ptr, unsigned int id);
static int move_item_from_lru(cache_t *cache, int id, int curLru, uint64_t totalBytes, 
                                uint32_t* moveToLru,  cache_item_t* search, cache_item_t** pItem);
static int lru_pull(cache_t *cache, int origId, int curLru, uint64_t total_bytes);

cache_code_t slab_init(cache_t *cache) {
  cache->slabs = NULL;

  cache->slabs = calloc(1, sizeof(cache_slab_class_t) * MAX_NUMBER_OF_SLAB_CLASSES);
  if (cache->slabs == NULL) {
    goto error;
  }

  int i = 0;
  size_t size = sizeof(cache_item_t) + CHUNK_SIZE;
  while (i < MAX_NUMBER_OF_SLAB_CLASSES) {
    cache_slab_class_t *slab = calloc(1, sizeof(cache_slab_class_t));
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
      break;
    }

    if (lru_pull(cache, id, CACHE_LRU_COLD, 0) <= 0) {  /* try to pull item fom cold list */
      /* pull item from cold list failed, try to pull item from hot list */
      if (lru_pull(cache, id, CACHE_LRU_HOT, 0) <= 0) {
        break;
      }
    }
  }

  return item;
}

static bool reach_cache_limit(cache_t *cache, int len) {
  if (cache->alloced + len >= cache->options.limit) {
    return true;
  }

  return false;
}

static void do_slabs_free(cache_t *cache, void *ptr, unsigned int id) {
  cache_slab_class_t *p = cache->slabs[id];
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
  cache_slab_class_t *p = cache->slabs[id];
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
  cache_slab_class_t *p = cache->slabs[id];
  int i = 0;
  for (i = 0; i < p->perSlab; i++) {
    do_slabs_free(cache, ptr, id);
    ptr += p->size;
  }
}

static int do_slab_newslab(cache_t *cache, unsigned int id) {
  cache_slab_class_t *p = cache->slabs[id];
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
  cache_slab_class_t *p = cache->slabs[id];
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

static int move_item_from_lru(cache_t *cache, int id, int curLru, uint64_t totalBytes, 
                                uint32_t* moveToLru,  cache_item_t* search, cache_item_t** pItem) {
  int removed = 0;
  uint64_t limit = 0;
  cache_option_t* opt = &(cache->options);
  cache_lru_class_t* lruCls = cache->lruArray;

  switch (curLru) {
    case CACHE_LRU_HOT:
      limit = totalBytes * opt->hotPercent / 100;
      // no break here, go through to next case
    case CACHE_LRU_WARM:
      if (limit == 0) {
        limit = totalBytes * opt->warmPercent / 100;
      }
      if (item_is_active(search)) {
        item_unactive(search);
        removed++;
        if (curLru == CACHE_LRU_WARM) {
          item_move_to_lru_head(cache, search);
          item_remove(cache, search);
        } else {
          *moveToLru = CACHE_LRU_WARM;
          item_unlink_from_lru(cache, search);
          *pItem = search;
        }
      } else if (lruCls[id].bytes > limit) {
        *moveToLru = CACHE_LRU_COLD;
        item_unlink_from_lru(cache, search);
        *pItem = search;
        removed++;
      } else {
        *pItem = search;
      }
      break;
    case CACHE_LRU_COLD:
      *pItem = search;
      break;
  }

  return removed;
}

static int lru_pull(cache_t *cache, int origId, int curLru, uint64_t totalBytes) {
  cache_item_t* item = NULL;
  cache_item_t* search;
  cache_item_t* next;
  cache_option_t* opt = &(cache->options);
  int id = origId;
  int removed = 0;
  int tries = 5;
  uint32_t moveToLru = 0;
  uint64_t now = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);

  id |= curLru;
  search = cache->lruArray[id].tail;
  for (; tries > 0 && search != NULL; tries--, search = next) {
    next = search->prev;
    uint32_t hv = cache->hash(item_key(search), search->nkey);

    // is item expired?
    if (now - search->lastTime >= opt->expireTime) {
      item_unlink_nolock(cache, search, hv);
      item_remove(cache, search);
      removed++;
      continue;
    }

    removed += move_item_from_lru(cache, id, curLru, totalBytes, &moveToLru, search, &item);

    if (item != NULL) {
      break;
    }   
  }

  if (item != NULL) {
    if (moveToLru) {
      item->slabClsId = item_clsid(item);
      item->slabClsId|= moveToLru;
      item_link_to_lru(cache, item);
    }
  }

  return removed;
}