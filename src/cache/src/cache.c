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
#include "cacheint.h"
#include "cacheLog.h"
#include "cacheItem.h"
#include "cacheSlab.h"
#include "cacheTable.h"

static int check_cache_options(cacheOption* options);
static int cachePutDataIntoCache(cacheTable* pTable, const char* key, uint8_t nkey, 
                                const char* value, uint32_t nbytes, cacheItem** ppItem, uint64_t expire);

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

  if (cacheLruInit(cache) != CACHE_OK) {
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
  cacheSlabDestroy(cache);
  cacheLruDestroy(cache);

  cacheAllocMemoryCookie *pCookie = cache->cookieHead;
  while (pCookie != NULL) {
    cacheAllocMemoryCookie *next = pCookie->next;
    free(pCookie->buffer);
    free(pCookie);
    pCookie = next;
  }

  cacheTable *pTable = cache->tableHead;
  while (pTable != NULL) {
    cacheTable* pNextTable = pTable->next;
    cacheTableDestroy(pTable);
    pTable = pNextTable;
  }

  free(cache);
}

int cachePut(cacheTable* pTable, const char* key, uint8_t nkey, const char* value, uint32_t nbytes, uint64_t expire) {
  cacheMutex* pMutex = cacheGetTableBucketMutexByKey(pTable, key, nkey);
  cacheMutexLock(pMutex);

  int ret = cachePutDataIntoCache(pTable,key,nkey,value,nbytes,NULL, expire);

  cacheMutexUnlock(pMutex);

  return ret;
}

int cacheGet(cacheTable* pTable, const char* key, uint8_t nkey, char** data, int* nbytes) {
  cacheMutex *pMutex = cacheGetTableBucketMutexByKey(pTable, key, nkey);
  cacheMutexLock(pMutex);

  *data = NULL;
  *nbytes = 0;
  /* first find the key in the cache table */
  cacheItem* pItem = cacheTableGet(pTable, key, nkey);
  if (pItem) {
    itemIncrRef(pItem);
    uint64_t now = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);
    if (cacheItemIsExpired(pItem, now)) { /* is item expired? */
      /* cacheItemUnlink make ref == 1 */
      cacheItemUnlink(pTable, pItem, CACHE_LOCK_LRU);
      /* cacheItemRemove make ref == 0 then free item */
      cacheItemRemove(pTable->pCache, pItem);
      pItem = NULL;
    } else if (cacheItemIsNeverExpired(pItem)) {      
      /* never expired item refCount == 1 */
      assert(pItem->refCount == 2);
      pItem->lastTime = now;    
    } else {
      cacheItemBump(pTable, pItem, now);      
    }
    goto out;
  }

  /* try to load the data from user defined function */
  if (pTable->option.loadFunc == NULL) {
    goto out;
  }
  char *loadValue;
  size_t loadLen = 0;
  uint64_t expire;
  if (pTable->option.loadFunc(pTable->option.userData, key, nkey, &loadValue, &loadLen, &expire) != CACHE_OK) {
    goto out;
  }

  /* TODO: save in the cache if access only one time? */
  int ret = cachePutDataIntoCache(pTable,key,nkey,loadValue,loadLen,&pItem, expire);
  free(loadValue);
  if (ret != CACHE_OK) {
    goto out;
  }
  itemIncrRef(pItem);

out:
  if (pItem == NULL) {
    cacheMutexUnlock(pMutex);
    return CACHE_KEY_NOT_FOUND;
  }
  *data = malloc(pItem->nbytes);
  if (*data == NULL) {
    cacheMutexUnlock(pMutex);
    return CACHE_OOM;
  }
  memcpy(*data, item_data(pItem), pItem->nbytes);
  *nbytes = pItem->nbytes;
  itemDecrRef(pItem);

  cacheMutexUnlock(pMutex);

  return CACHE_OK;
}

void cacheRemove(cacheTable* pTable, const char* key, uint8_t nkey) {
  cacheMutex* mutex = cacheGetTableBucketMutexByKey(pTable, key, nkey);
  cacheMutexLock(mutex);

  cacheTableRemove(pTable, key, nkey);

  cacheMutexUnlock(mutex);
}

static int cachePutDataIntoCache(cacheTable* pTable, const char* key, uint8_t nkey, const char* value,
                                uint32_t nbytes, cacheItem** ppItem, uint64_t expire) {
  cacheItem* pItem = cacheAllocItem(pTable->pCache, nkey, nbytes, expire);
  if (pItem == NULL) {
    return CACHE_OOM;
  }

  pItem->nkey = nkey;
  pItem->nbytes = nbytes;
  memcpy(item_key(pItem), key, nkey);
  memcpy(item_data(pItem), value, nbytes);
  pItem->lastTime = taosGetTimestamp(TSDB_TIME_PRECISION_MILLI);
  cacheTablePut(pTable, pItem);

  if (ppItem) {
    *ppItem = pItem;
  }
  return CACHE_OK;
}

static int check_cache_options(cacheOption* options) {
  return CACHE_OK;
}