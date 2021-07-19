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

#include <string.h>
#include "cacheHashtable.h"
#include "cacheint.h"
#include "cacheLog.h"
#include "cacheItem.h"
#include "cacheSlab.h"

static cache_code_t check_cache_options(cache_option_t* options);
static cache_code_t do_cache_put(cache_t* cache, const char* key, uint8_t nkey, const char* value, int nbytes, cache_item_t** ppItem);

//static cache_manager_t cache_manager

cache_t* cache_create(cache_option_t* options) {
  if (check_cache_options(options) != CACHE_OK) {
    cacheError("check_cache_options fail");
    return NULL;
  }

  cache_t* cache = calloc(1, sizeof(cache_t));
  if (cache == NULL) {
    cacheError("calloc cache_t fail");
    return NULL;
  }

  cache->options = *options;
  if (hash_init(cache) != CACHE_OK) {
    goto error;
  }
  if (slab_init(cache) != CACHE_OK) {
    goto error;
  }

  return cache;

error:
  if (cache != NULL) {
    free(cache);
  }

  return NULL;
}

void  cache_destroy(cache_t* cache) {

}

cache_code_t cache_put(cache_t* cache, const char* key, uint8_t nkey, const char* value, int nbytes) {
  return do_cache_put(cache,key,nkey,value,nbytes,NULL);
}

cache_code_t cache_get(cache_t* cache, const char* key, uint8_t nkey, char** value, int *len) {
  cache_item_t* item = hash_get(cache, key, nkey);
  if (item) {
    *value = item_data(item);
    *len = item->nbytes;
    return CACHE_OK;
  }

  char *loadValue;
  size_t loadLen = 0;
  if (cache->options.loadFunc(cache->options.userData, key, nkey, &loadValue, &loadLen) != CACHE_OK) {
    return CACHE_KEY_NOT_FOUND;
  }

  int ret = do_cache_put(cache,key,nkey,loadValue,loadLen,&item);
  if (ret != CACHE_OK) {
    return ret;
  }
  *value = item_data(item);
  *len   = item_len(item);

  return CACHE_OK;
}

static cache_code_t do_cache_put(cache_t* cache, const char* key, uint8_t nkey, const char* value, int nbytes, cache_item_t** ppItem) {
  cache_item_t* item = item_alloc(cache, nkey, nbytes);
  if (item == NULL) {
    return CACHE_OOM;
  }

  memcpy(item_key(item), key, nkey);
  memcpy(item_data(item), value, nbytes);

  if (ppItem) {
    *ppItem = item;
  }
  return CACHE_OK;
}

static cache_code_t check_cache_options(cache_option_t* options) {
  return CACHE_OK;
}