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
#include "assoc.h"
#include "cacheint.h"
#include "cacheLog.h"
#include "item.h"
#include "slab.h"

static cache_code_t check_cache_options(cache_option_t* options);
static cache_code_t do_cache_put(cache_context_t* context, const char* key, uint8_t nkey, const char* value, int nbytes, item_t** ppItem);

//static cache_manager_t cache_manager

cache_context_t* cache_create(cache_option_t* options) {
  if (check_cache_options(options) != CACHE_OK) {
    cacheError("check_cache_options fail");
    return NULL;
  }

  cache_context_t* context = calloc(1, sizeof(cache_context_t));
  if (context == NULL) {
    cacheError("calloc cache_context_t fail");
    return NULL;
  }

  context->options = *options;
  if (hash_init(context) != CACHE_OK) {
    goto error;
  }
  if (slab_init(context) != CACHE_OK) {
    goto error;
  }

  return context;

error:
  if (context != NULL) {
    free(context);
  }

  return NULL;
}

void  cache_destroy(cache_context_t* context) {

}

cache_code_t cache_put(cache_context_t* context, const char* key, uint8_t nkey, const char* value, int nbytes) {
  return do_cache_put(context,key,nkey,value,nbytes,NULL);
}

cache_code_t cache_get(cache_context_t* context, const char* key, uint8_t nkey, char** value, int *len) {
  item_t* item = hash_get(context, key, nkey);
  if (item) {
    *value = item_data(item);
    *len = item->nbytes;
    return CACHE_OK;
  }

  char *loadValue;
  size_t loadLen = 0;
  if (context->options.loadFunc(context->options.userData, key, nkey, &loadValue, &loadLen) != CACHE_OK) {
    return CACHE_KEY_NOT_FOUND;
  }

  int ret = do_cache_put(context,key,nkey,loadValue,loadLen,&item);
  if (ret != CACHE_OK) {
    return ret;
  }
  *value = item_data(item);
  *len   = item_len(item);

  return CACHE_OK;
}

static cache_code_t do_cache_put(cache_context_t* context, const char* key, uint8_t nkey, const char* value, int nbytes, item_t** ppItem) {
  size_t ntotal = item_size(nkey, nbytes);
  unsigned int id = slabs_clsid(context, ntotal);

  item_t* item = item_alloc(context, ntotal, id);
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