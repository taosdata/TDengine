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

#include <assert.h>
#include "cacheHashtable.h"
#include "cacheint.h"
#include "cacheItem.h"

size_t item_size(uint8_t nkey, int nbytes) {
  return sizeof(cache_item_t) + sizeof(unsigned int) + (nkey + 1) + nbytes;
}

cache_item_t* item_alloc(cache_t* cache, uint8_t nkey, int nbytes) {
  /*
  size_t ntotal = item_size(nkey, nbytes);
  unsigned int id = slabClsId(cache, ntotal);
  cache_item_t* item = NULL;

  if (ntotal > 10240) {

  } else {

  }
  */
  return NULL;
}

void item_free(cache_t* cache, cache_item_t* item) {

}

void item_move_to_lru_head(cache_t* cache, cache_item_t* item) {
  item_unlink_from_lru(cache, item);
  item_link_to_lru(cache, item);
}

void   item_link_to_lru(cache_t* cache, cache_item_t* item) {

}

void item_unlink_from_lru(cache_t* cache, cache_item_t* item) {
  cache_lru_class_t* lru = &(cache->lruArray[item->slabClsId]);
  cache_item_t* tail = lru->tail;

  if (tail == item) {
    tail = tail->next;
  }

  if (item->next) item->next->prev = item->prev;
  if (item->prev) item->prev->next = item->next;

  lru->num -= 1;
  lru->bytes -= item_size(item->nkey, item->nbytes);
}

void item_unlink_nolock(cache_t* cache, cache_item_t* item, uint32_t hv) {
  if (item_is_linked(item)) {
    item_unlink(item);
    hash_remove(cache, item_key(item), item->nkey, hv);    
    item_unlink_from_lru(cache, item);
    item_remove(cache, item);
  }
}

void item_remove(cache_t* cache, cache_item_t* item) {
  assert(!item_is_slabbed(item));

  item_free(cache, item);
}