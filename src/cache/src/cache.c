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
  size_t ntotal = item_size(nkey, nbytes);
  unsigned int id = slabs_clsid(context, ntotal);
  item_t* item;

  item = item_alloc(context, ntotal, id);
  if (item == NULL) {
    return CACHE_OOM;
  }

  memcpy(item_key(item), key, nkey);
  memcpy(item_data(item), value, nbytes);

  return CACHE_OK;
}

cache_code_t cache_get(cache_context_t* context, const char* key, char** value, size_t *len) {
  return CACHE_OK;
}

static cache_code_t check_cache_options(cache_option_t* options) {
  return CACHE_OK;
}