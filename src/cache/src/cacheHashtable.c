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

#include <stdlib.h>
#include "cacheHashtable.h"
#include "cacheint.h"
#include "cacheItem.h"

#define hashsize(n) ((int32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

extern uint32_t jenkins_hash(const void *key, size_t length);
extern uint32_t MurmurHash3_x86_32(const void *key, size_t length);

cache_code_t hash_init(cache_t* cache) {
  cache_hashtable_t* table = calloc(1, sizeof(cache_hashtable_t));
  if (table == NULL) {
    return CACHE_OOM;
  }

  table->hashpower = cache->options.hashPowerInit;
  table->primary_hashtable = calloc(hashsize(table->hashpower), sizeof(void *));
  if (table->primary_hashtable == NULL) {
    free(table);
    return CACHE_OOM;
  }
  table->hash = MurmurHash3_x86_32;
  table->expanding = false;

  cache->table = table;
  return CACHE_OK;
}

cache_code_t hash_put(cache_t* cache, cache_item_t* item) {
  cache_hashtable_t* table = cache->table;
  cache_item_t* hash_head = NULL, *current = NULL, *hash_last = NULL;
  const char* key = item_key(item);
  uint32_t hash = table->hash(key, item->nkey);
  
  if (table->expanding) {

  } else {
    hash_head = table->primary_hashtable[hash & hashmask(table->hashpower)];
  }

  // check if the same key exists in the table?
  for (current = hash_head; current != NULL; current = current->h_next) {
    if (!item_key_equal(current, item)) {
      hash_last = current;
      continue;
    }
    if (hash_last) {
      hash_last->h_next = current->h_next;
    } else {
      // first item
    }
    item_free(cache, current);
    break;
  }

  // insert it
  item->h_next = hash_head;

  return CACHE_OK;
}

cache_item_t* hash_get(cache_t* cache, const char* key, uint8_t nkey) {
  cache_hashtable_t* table = cache->table;
  uint32_t hash = table->hash(key, nkey);
  cache_item_t* hash_head = table->primary_hashtable[hash & hashmask(table->hashpower)], *current = NULL;
  cache_key_t find_key = (cache_key_t){.key = key, .nkey = nkey};

  for (current = hash_head; current != NULL; current = current->h_next) {
    cache_key_t current_key = key_from_item(current);
    if (!key_equal(current_key, find_key)) {
      continue;
    }
    
    return current;
  }

  return NULL;
}