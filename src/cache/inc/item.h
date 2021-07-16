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

static FORCE_INLINE bool item_key_equal(const item_t* item1, const item_t* item2) {
  if (item1->nkey != item2->nkey) {
    return false;
  }

  return memcmp(item_key(item1), item_key(item2), item1->nkey) == 0;
}

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_ITEM_H