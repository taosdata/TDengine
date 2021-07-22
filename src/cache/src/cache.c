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

static int check_cache_options(cacheOption* options);
static int cachePutDataIntoCache(cacheTable* pTable, const char* key, uint8_t nkey, const char* value, uint32_t nbytes, cacheItem** ppItem);

//static cache_manager_t cache_manager

cache_t* cacheCreate(cacheOption* options) {
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

  if (cacheSlabInit(cache) != CACHE_OK) {
    goto error;
  }

  return cache;

error:
  if (cache != NULL) {
    free(cache);
  }

  return NULL;
}

void  cacheDestroy(cache_t* cache) {

}

int cachePut(cacheTable* pTable, const char* key, uint8_t nkey, const char* value, uint32_t nbytes, uint64_t expire) {
  return cachePutDataIntoCache(pTable,key,nkey,value,nbytes,NULL);
}

cacheItem* cacheGet(cacheTable* pTable, const char* key, uint8_t nkey) {
  // first find the key in the cache table
  cacheItem* item = cacheTableGet(pTable, key, nkey);
  if (item) {
    itemIncrRef(item);
    return item;
  }

  // try to load the data from user defined function
  if (pTable->option.loadFunc == NULL) {
    return NULL;
  }
  char *loadValue;
  size_t loadLen = 0;
  if (pTable->option.loadFunc(pTable->option.userData, key, nkey, &loadValue, &loadLen) != CACHE_OK) {
    return NULL;
  }

  // TODO: save in the cache if access only one time?
  int ret = cachePutDataIntoCache(pTable,key,nkey,loadValue,loadLen,&item);
  free(loadValue);
  if (ret != CACHE_OK) {
    return NULL;
  }

  itemIncrRef(item);
  return item;
}

void cacheItemData(cacheItem* pItem, char** data, int* nbytes) {
  *data = item_data(pItem);
  *nbytes = pItem->nbytes;
}

static int cachePutDataIntoCache(cacheTable* pTable, const char* key, uint8_t nkey, const char* value, uint32_t nbytes, cacheItem** ppItem) {
  cacheItem* item = cacheAllocItem(pTable->pCache, nkey, nbytes, 3600 * 1000);
  if (item == NULL) {
    return CACHE_OOM;
  }

  item->nkey = nkey;
  item->nbytes = nbytes;
  memcpy(item_key(item), key, nkey);
  memcpy(item_data(item), value, nbytes);

  cacheTablePut(pTable, item);

  if (ppItem) {
    *ppItem = item;
  }
  return CACHE_OK;
}

static int check_cache_options(cacheOption* options) {
  return CACHE_OK;
}