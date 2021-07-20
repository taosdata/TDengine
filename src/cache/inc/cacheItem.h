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

struct cache_item_t {
  /* Protected by LRU locks */
  struct cache_item_t*  next;
  struct cache_item_t*  prev;

  struct cache_item_t*  h_next;   /* hash chain next */

  uint16_t        flags;          /* item flags above */

  uint8_t         slabClsId;      /* which slab class we're in */

  uint64_t        lastTime;       /* last access time */
  uint8_t         nkey;           /* key length */

  int             nbytes;         /* size of data */

  char            data[];
};

#define item_is_linked(item)    ((item)->flags & ITEM_LINKED)
#define item_is_slabbed(item)   ((item)->flags & ITEM_SLABBED)
#define item_is_active(item)    ((item)->flags & ITEM_ACTIVE)

#define item_unlink(item)       ((item)->flags &= ~ITEM_LINKED)
#define item_unactive(item)     ((item)->flags &= ~ITEM_ACTIVE)

#define item_clsid(item)        ((item)->slabClsId & ~(3<<6))
#define item_lruid(item)        ((item)->slabClsId & (3<<6))

size_t item_size(uint8_t nkey, int nbytes);
cache_item_t* item_alloc(cache_t*, uint8_t nkey, int nbytes);
void    item_free(cache_t*, cache_item_t*);
void   item_unlink_nolock(cache_t*, cache_item_t*, uint32_t hv);
void   item_unlink_from_lru(cache_t*, cache_item_t*);
void   item_link_to_lru(cache_t*, cache_item_t*);
void    item_move_to_lru_head(cache_t*, cache_item_t*);
void    item_remove(cache_t*, cache_item_t*);

#define item_key(item)  (((char*)&((item)->data)) + sizeof(unsigned int))

#define item_data(item) (((char*)&((item)->data)) + sizeof(unsigned int) + (item)->nkey + 1)

#define item_len(item) ((item)->nbytes)

#define key_from_item(item) (cache_key_t) {.key = item_key(item), .nkey = (item)->nkey};

static FORCE_INLINE bool key_equal(cache_key_t key1, cache_key_t key2) {
  if (key1.nkey != key2.nkey) {
    return false;
  }

  return memcmp(key1.key, key2.key, key1.nkey) == 0;
}

static FORCE_INLINE bool item_key_equal(cache_item_t* item1, cache_item_t* item2) {
  cache_key_t key1 = key_from_item(item1);
  cache_key_t key2 = key_from_item(item2);
  return key_equal(key1, key2);
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHE_ITEM_H