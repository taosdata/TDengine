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

size_t item_size(uint8_t nkey, int nbytes) {
  return sizeof(cache_item_t) + sizeof(unsigned int) + (nkey + 1) + nbytes;
}

cache_item_t* item_alloc(cache_context_t* context, uint8_t nkey, int nbytes) {
  /*
  size_t ntotal = item_size(nkey, nbytes);
  unsigned int id = slab_class_id(context, ntotal);
  cache_item_t* item = NULL;

  if (ntotal > 10240) {

  } else {

  }
  */
  return NULL;
}

void item_free(cache_context_t* context, cache_item_t* item) {

}