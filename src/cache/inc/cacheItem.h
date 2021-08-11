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
#include "cachePriv.h"
#include "cacheTable.h"
#include "cacheTypes.h"
#include "osAtomic.h"
#include "osDef.h"

#ifdef __cplusplus
extern "C" {
#endif

enum cacheItemFlag {
  /* item in use state: */
  ITEM_IN_USE   = 1,  /* item in use, 0 means item is free */
  ITEM_FETCHED  = 2,  /* item in use and was fetched at least once in its lifetime */
  ITEM_ACTIVE   = 4,  /* Appended on fetch, removed on LRU shuffling */
  ITEM_CHUNKED  = 8,  /* item in use and in chunked mode */
};

struct cacheItem {
  /* Protected by slab/slab-LRU mutex */
  struct cacheItem* next;
  struct cacheItem* prev;

  struct cacheItem* h_next;       /* hash tabel next item, protected by table mutex */

  cacheTable*       pTable;       /* owner cache table */
  uint16_t          flags;        /* item flags above */

  uint8_t           slabLruId;    /* which slab lru class we're in */

  uint64_t          expireTime;   /* expire time, 0 means no expire time */
  uint64_t          lastTime;     /* last access time */

  uint8_t           refCnt;

  uint8_t           nkey;         /* key length */
  
  uint32_t          nbytes;       /* size of data */

  char              data[];
};

/* item flags macros */
#define item_is_free(item)       ((item)->flags == 0)
#define item_set_free(item)      (item)->flags = 0
#define item_unset_free(item)    item_set_used(item)

#define item_is_used(item)        ((item)->flags & ITEM_IN_USE)
#define item_set_used(item)       (item)->flags |= ITEM_IN_USE

#define item_set_fetched(item)    (item)->flags |= ITEM_FETCHED
#define item_is_fetched(item)     ((item)->flags & ITEM_FETCHED)

#define item_is_active(item)      ((item)->flags & ITEM_ACTIVE)
#define item_set_actived(item)    (item)->flags |= ITEM_ACTIVE
#define item_unset_active(item)   (item)->flags &= ~ITEM_ACTIVE

#define item_is_chunked(item)     ((item)->flags & ITEM_CHUNKED)
#define item_set_chunked(item)    (item)->flags |= ITEM_CHUNKED

/* return slab class id in [0,MAX_NUMBER_OF_SLAB_CLASSES-1] */
#define item_slab_id(item)       ((item)->slabLruId & ~(3<<6))

/* return the lru list type id:hot,warm,cold */
#define item_lru_id(item)       ((item)->slabLruId & (3<<6))

/* return the slab lru list array index in [0,MAX_NUMBER_OF_SLAB_LRU] */
#define item_slablru_id(item)   ((item)->slabLruId)

#define item_data(item)  (char*)&((item)->data)

#define item_key(item) (((char*)&((item)->data) + (item)->nbytes + 1))

#define item_len(item) ((item)->nbytes)

/* item totalBytes = sizeof(cacheItem) + key size + data size + 1(for '\0' between data and key) */
static size_t FORCE_INLINE cacheItemTotalBytes(uint8_t nkey, uint32_t nbytes) {
  return sizeof(cacheItem) + nkey + nbytes + 1;
}

/* item in user part ref count */
static FORCE_INLINE int32_t itemUserDataRef(cacheItem* pItem) {
  return (pItem->pTable->option.refOffset <= 0) ? 0 : *((int32_t*)(item_data(pItem) + pItem->pTable->option.refOffset));
}

static FORCE_INLINE uint8_t* itemRefPtr(cacheItem* pItem) {
  return &(pItem->refCnt);
}

static FORCE_INLINE uint8_t itemRef(cacheItem* pItem) {
  return *itemRefPtr(pItem);
}

static FORCE_INLINE void setItemRef(cacheItem* pItem, uint8_t cnt) {
  *itemRefPtr(pItem) = cnt;
}

static FORCE_INLINE uint8_t itemIncrRef(cacheItem* pItem) { 
  return atomic_add_fetch_32(itemRefPtr(pItem), 1);
}

static FORCE_INLINE uint8_t itemDecrRef(cacheItem* pItem) { 
  return atomic_sub_fetch_32(itemRefPtr(pItem), 1);
}

static FORCE_INLINE bool cacheItemIsNeverExpired(cacheItem* pItem) {
  return pItem->expireTime == 0;
}

static bool FORCE_INLINE cacheItemIsExpired(cacheItem* pItem, uint64_t now) {
  return (pItem->expireTime != 0) && (now - pItem->lastTime >= pItem->expireTime);
}

static FORCE_INLINE bool item_equal_key(cacheItem* item, const void* key, uint8_t nkey) {
  if (item->nkey != nkey) {
    return false;
  }

  return memcmp(item_key(item), key, nkey) == 0;
}

void cacheItemUnlink(cacheTable* pTable, cacheItem* pItem, cacheLockFlag flag);
void cacheItemRemove(cache_t*, cacheItem*);
void cacheItemBump(cacheTable* pTable, cacheItem* pItem, uint64_t now);

cacheItem* cacheAllocItem(cacheTable*, cacheMutex* pMutex, uint8_t nkey, uint32_t nbytes, uint64_t expireTime);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHE_ITEM_H