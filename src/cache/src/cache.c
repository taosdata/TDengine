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
#include "cache_priv.h"
#include "cacheLog.h"
#include "cacheItem.h"
#include "cacheSlab.h"
#include "cacheTable.h"

static int check_cache_options(cacheOption* options);
static int cachePutDataIntoCache(cacheTable* pTable, const char* key, uint8_t nkey, 
                                const char* value, uint32_t nbytes, cacheItem** ppItem, uint64_t expire);
static int cacheInitItemMutex(cache_t*);
static void cacheFreeChunkItems(cache_t*);
static void cacheFreeMemory(cache_t*);
static void cacheFreeTable(cache_t*);
static void cacheDestroyItemMutex(cache_t*);

cache_t* cacheCreate(cacheOption* options) {
  if (check_cache_options(options) != CACHE_OK) {
    cacheError("check_cache_options fail");
    return NULL;
  }

  cache_t* pCache = calloc(1, sizeof(cache_t));
  if (pCache == NULL) {
    cacheError("calloc cache_t fail");
    return NULL;
  }

  taosInitRWLatch(&(pCache->latch));
  pCache->options = *options;

  if (cacheSlabInit(pCache) != CACHE_OK) {
    goto error;
  }

  if (cacheLruInit(pCache) != CACHE_OK) {
    goto error;
  }

  if (cacheInitItemMutex(pCache) != CACHE_OK) {
    goto error;
  }

  return pCache;

error:
  if (pCache != NULL) {
    free(pCache);
  }

  return NULL;
}

void  cacheDestroy(cache_t* pCache) {
  cacheSlabDestroy(pCache);
  cacheLruDestroy(pCache);
  cacheFreeChunkItems(pCache);
  cacheFreeMemory(pCache);
  cacheFreeTable(pCache);
  cacheDestroyItemMutex(pCache);

  free(pCache);
}

int cachePut(cacheTable* pTable, const char* key, uint8_t nkey, const char* value, uint32_t nbytes, uint64_t expire) {
  cacheMutex* pMutex = getItemMutexByKey(pTable, key, nkey);
  cacheMutexLock(pMutex);

  int ret = cachePutDataIntoCache(pTable,key,nkey,value,nbytes,NULL, expire);

  cacheMutexUnlock(pMutex);

  return ret;
}

int cacheGet(cacheTable* pTable, const char* key, uint8_t nkey, char** data, int* nbytes) {
  cacheMutex *pMutex = getItemMutexByKey(pTable, key, nkey);
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
  if (pTable->option.loadFp == NULL) {
    goto out;
  }
  char *loadValue;
  size_t loadLen = 0;
  uint64_t expire;
  if (pTable->option.loadFp(pTable->option.userData, key, nkey, &loadValue, &loadLen, &expire) != CACHE_OK) {
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
  cacheMutex* mutex = getItemMutexByKey(pTable, key, nkey);
  cacheMutexLock(mutex);

  cacheTableRemove(pTable, key, nkey, true);

  if (pTable->option.delFp) {
    pTable->option.delFp(pTable->option.userData, key, nkey);
  }

  cacheMutexUnlock(mutex);
}

void cacheDestroyTable(cacheTable* pTable) {

}

void *allocMemory(cache_t *cache, size_t size, bool chunked) {  
  void* ptr = malloc(size);
  if (ptr == NULL) {
    return NULL;
  }

  cacheAllocMemoryCookie *pCookie = malloc(sizeof(cacheAllocMemoryCookie));
  if (pCookie == NULL) {
    free(ptr);
    return NULL;
  }
  pCookie->buffer = ptr;

  taosWLockLatch(&(cache->latch));
  cache->alloced += size;
  if (!chunked) {
    pCookie->next = cache->cookieHead;
    cache->cookieHead = pCookie;
  } else {
    cacheItem* pItem = (cacheItem*)ptr;
    pItem->next = pItem->prev = NULL;
    pItem->next = cache->chunkItemHead;
    if (cache->chunkItemHead) cache->chunkItemHead->prev = pItem;
    cache->chunkItemHead = pItem;
    item_set_chunked(pItem);
  }
  taosWUnLockLatch(&(cache->latch));

  return ptr;
}

FORCE_INLINE cacheMutex* getItemMutexByIndex(cacheTable* pTable, uint32_t hash) {
  return &(pTable->pCache->itemMutex[hash & hashmask(ITEM_MUTEX_HASH_POWER)]);
}

FORCE_INLINE cacheMutex* getItemMutexByKey(cacheTable* pTable, const char* key, uint8_t nkey) {
  return &(pTable->pCache->itemMutex[pTable->hashFp(key, nkey) & hashmask(ITEM_MUTEX_HASH_POWER)]);
}

FORCE_INLINE cacheMutex* getItemMutexByItem(cacheItem* pItem) {
  return getItemMutexByKey(pItem->pTable, item_key(pItem), pItem->nkey);
}

static int cacheInitItemMutex(cache_t* pCache) {
  int i = 0;
  int count = hashsize(ITEM_MUTEX_HASH_POWER);
  pCache->itemMutex = calloc(count, sizeof(cacheMutex));
  for (; i < hashsize(ITEM_MUTEX_HASH_POWER); i++) {
    if (cacheMutexInit(&(pCache->itemMutex[i]))) {
      goto err;
    }
  }
  return 0;
err:
  return -1;
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

static void cacheFreeChunkItems(cache_t* pCache) {
  cacheItem* pItem = pCache->chunkItemHead;

  while (pItem) {
    cacheItem* next = pItem->next;
    free(pItem);
    pItem = next;
  }
}

static void cacheFreeMemory(cache_t* pCache) {
  cacheAllocMemoryCookie *pCookie = pCache->cookieHead;

  while (pCookie != NULL) {
    cacheAllocMemoryCookie *next = pCookie->next;
    free(pCookie->buffer);
    free(pCookie);
    pCookie = next;
  }
}

static void cacheFreeTable(cache_t* pCache) {
  cacheTable *pTable = pCache->tableHead;

  while (pTable != NULL) {
    cacheTable* pNextTable = pTable->next;
    cacheTableDestroy(pTable);
    pTable = pNextTable;
  }
}

static void cacheDestroyItemMutex(cache_t* pCache) {
  int i = 0;
  for (; i < hashsize(ITEM_MUTEX_HASH_POWER); i++) {
    cacheMutexDestroy(&(pCache->itemMutex[i]));  
  }
  free(pCache->itemMutex);
}

static int check_cache_options(cacheOption* options) {
  return CACHE_OK;
}