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

#ifndef TDENGINE_ITEM_H
#define TDENGINE_ITEM_H

#include <stdint.h>
#include <string.h>
#include "cacheint.h"
#include "item.h"
#include "types.h"
#include "osDef.h"

#ifdef __cplusplus
extern "C" {
#endif

struct item_t {
  /* Protected by LRU locks */
  struct item_t*  next;
  struct item_t*  prev;

  struct item_t*  h_next;       /* hash chain next */

  uint8_t         slabs_clsid;    /* which slab class we're in */
  uint8_t         nkey;           /* key length */

  int             nbytes;         /* size of data */

  char            data[];
};

size_t item_size(uint8_t nkey, int nbytes);
item_t* item_alloc(cache_context_t*, size_t ntotal, int id);
void    item_free(cache_context_t*, item_t*);

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

static FORCE_INLINE bool item_key_equal(item_t* item1, item_t* item2) {
  cache_key_t key1 = key_from_item(item1);
  cache_key_t key2 = key_from_item(item2);
  return key_equal(key1, key2);
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_ITEM_H