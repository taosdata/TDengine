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

#ifndef TDENGINE_CACHE_HASHTABLE_H
#define TDENGINE_CACHE_HASHTABLE_H

#include <stdbool.h>
#include <stdint.h>
#include "cacheTypes.h"

#ifdef __cplusplus
extern "C" {
#endif

struct cache_hashtable_t {
  cache_item_t** primary_hashtable;

  cache_item_t** old_hashtable;
  
  int   hashpower;
  bool    expanding;
};

cache_code_t hash_init(cache_t* cache);
cache_code_t hash_put(cache_t* cache, cache_item_t* item);
cache_item_t* hash_get(cache_t* cache, const char* key, uint8_t nkey);
void          hash_remove(cache_t* cache, const char* key, uint8_t nkey, uint32_t hv);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_HASHTABLE_H */
