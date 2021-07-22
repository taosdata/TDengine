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

#ifndef TDENGINE_CACHE_ITEM_H
#define TDENGINE_CACHE_ITEM_H

#include <stdint.h>
#include <string.h>
#include "cacheint.h"
#include "cacheTypes.h"
#include "osAtomic.h"
#include "osDef.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum cache_item_flag_t {
  ITEM_LINKED   = 1,
  ITEM_SLABBED  = 2,  /* item in slab free list */  
  ITEM_FETCHED  = 4,  /* Item was fetched at least once in its lifetime */
  ITEM_ACTIVE   = 8,  /* Appended on fetch, removed on LRU shuffling */
  ITEM_CHUNKED  = 16, /* item in chunked mode */
} cache_item_flag_t;

struct cacheItem {
  /* Protected by LRU locks */
  struct cacheItem*  next;
  struct cacheItem*  prev;

  struct cacheItem* h_next;

  cacheTable*     pTable;         /* owner cache table */
  uint16_t        flags;          /* item flags above */

  uint8_t         slabClsId;      /* which slab class we're in */

  uint64_t        expireTime;     /* expire time, 0 means no expire time */
  uint64_t        lastTime;       /* last access time */
  uint8_t         nkey;           /* key length */
  uint8_t         refCount;       /* reference count */
  uint32_t        nbytes;         /* size of data */

  char            data[];
};

/* item flags macros */
#define item_is_linked(item)    ((item)->flags & ITEM_LINKED)
#define item_is_slabbed(item)   ((item)->flags & ITEM_SLABBED)
#define item_is_active(item)    ((item)->flags & ITEM_ACTIVE)
#define item_is_chunked(item)   ((item)->flags & ITEM_CHUNKED)

#define item_set_slabbed(item)   (item)->flags &= ITEM_SLABBED

#define item_unlink(item)       (item)->flags &= ~ITEM_LINKED
#define item_unactive(item)     (item)->flags &= ~ITEM_ACTIVE
#define item_unslabbed(item)    (item)->flags &= ~ITEM_SLABBED

#define item_clsid(item)        ((item)->slabClsId & ~(3<<6))
#define item_lruid(item)        ((item)->slabClsId & (3<<6))

size_t cacheItemTotalBytes(uint8_t nkey, uint32_t nbytes);

cacheItem* cacheAllocItem(cache_t*, uint8_t nkey, uint32_t nbytes, uint64_t expireTime);

void    cacheItemFree(cache_t*, cacheItem*);
void    cacheItemUnlinkNolock(cacheTable* pTable, cacheItem* item);
void   cacheItemUnlinkFromLru(cache_t*, cacheItem*, bool lock);
void   cacheItemLinkToLru(cache_t*, cacheItem*, bool lock);
void    cacheItemMoveToLruHead(cache_t*, cacheItem*);
void    cacheItemRemove(cache_t*, cacheItem*);

#define item_key(item)  (((char*)&((item)->data)) + sizeof(unsigned int))

#define item_data(item) (((char*)&((item)->data)) + sizeof(unsigned int) + (item)->nkey + 1)

#define item_len(item) ((item)->nbytes)

#define key_from_item(item) (cache_key_t) {.key = item_key(item), .nkey = (item)->nkey};

static FORCE_INLINE uint8_t itemIncrRef(cacheItem* pItem) { 
  return atomic_add_fetch_32(&(pItem->refCount), 1);
}

static FORCE_INLINE uint8_t itemDecrRef(cacheItem* pItem) { 
  return atomic_sub_fetch_32(&(pItem->refCount), 1);
}

static FORCE_INLINE bool cacheItemNeverExpired(cacheItem* pItem) {
  return pItem->expireTime == 0;
}

static FORCE_INLINE bool key_equal(cache_key_t key1, cache_key_t key2) {
  if (key1.nkey != key2.nkey) {
    return false;
  }

  return memcmp(key1.key, key2.key, key1.nkey) == 0;
}

static FORCE_INLINE bool item_equal_key(cacheItem* item, const char* key, uint8_t nkey) {
  if (item->nkey != nkey) {
    return false;
  }

  return memcmp(item_key(item), key, nkey) == 0;
}

static FORCE_INLINE bool item_key_equal(cacheItem* item1, cacheItem* item2) {
  cache_key_t key1 = key_from_item(item1);
  cache_key_t key2 = key_from_item(item2);
  return key_equal(key1, key2);
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHE_ITEM_H