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
#include <stdlib.h>
#include <unistd.h>
#include "cacheLog.h"
#include "cacheTable.h"
#include "cache_priv.h"
#include "cacheItem.h"
#include "cacheSlab.h"
#include "cacheTypes.h"
#include "hash.h"

typedef struct cacheIterator {
  cacheTable* pTable;
  cacheItem* pItem;
  cacheItem* pNext;
  uint32_t bucketIndex;
  bool bucketLocked;
} cacheIterator;

static int        initTableBucket(cacheTableBucket* pBucket);
static cacheItem* findItemByKey(cacheTable* pTable, const char* key, uint8_t nkey, cacheItem **ppPrev);
static void       removeTableItem(cache_t* pCache, cacheTable* pTable, cacheTableBucket* pBucket, 
                                cacheItem* pItem, cacheItem* pPrev, bool freeItem);
static cacheTableBucket* getTableBucketByKey(cacheTable* pTable, const char* key, uint8_t nkey);
static void       checkExpandTable(cacheTable* pTable);
static void*      expandTableThread(void *arg);
static void       expandCacheTable(cacheTable* pTable);

cacheTable* cacheCreateTable(cache_t* cache, cacheTableOption* option) {
  cacheTable* pTable = calloc(1, sizeof(cacheTable));
  if (pTable == NULL) {
    goto error;
  }

  // init hash power CANNOT less than ITEM_MUTEX_HASH_POWER
  if (option->initHashPower < ITEM_MUTEX_HASH_POWER) {
    option->initHashPower = ITEM_MUTEX_HASH_POWER;
  }

  if (cacheMutexInit(&(pTable->mutex)) != 0) {
    goto error;
  }

  pTable->capacity = hashsize(option->initHashPower);
  pTable->pBucket = malloc(sizeof(cacheTableBucket) * pTable->capacity);
  if (pTable->pBucket == NULL) {
    goto error;
  }
  memset(pTable->pBucket, 0, sizeof(cacheTableBucket) * pTable->capacity);
  int i = 0;
  for (i = 0; i < pTable->capacity; i++) {
    initTableBucket(&(pTable->pBucket[i]));
  }

  pTable->hashFp = taosGetDefaultHashFunction(option->keyType);
  pTable->option = *option;
  pTable->pCache = cache;
  pTable->pOldBucket = NULL;
  pTable->expanding = false;
  pTable->expandIndex = 0;
  pTable->nOverflow = 0;
  taosInitRWLatch(&(pTable->latch));

  taosWLockLatch(&(cache->latch));
  if (cache->tableHead == NULL) {
    cache->tableHead = pTable;
    pTable->next = NULL;
  } else {
    pTable->next = cache->tableHead;
    cache->tableHead = pTable;
  }
  taosWUnLockLatch(&(cache->latch));

  return pTable;

error:
  if (pTable) {
    free(pTable);
  }

  return NULL;
}

void cacheTableDestroy(cacheTable *pTable) {
  free(pTable->pBucket);
  free(pTable);
}

static int initTableBucket(cacheTableBucket* pBucket) {
  pBucket->head = NULL;
  pBucket->overflow = false;
  return 0;
}

static cacheItem* findItemByKey(cacheTable* pTable, const char* key, uint8_t nkey, cacheItem **ppPrev) {
  cacheItem* pItem = getTableBucketByKey(pTable, key, nkey)->head;
  
  if (ppPrev) *ppPrev = NULL;

  while (pItem != NULL) {
    if (!item_equal_key(pItem, key, nkey)) {
      if (ppPrev) {
        *ppPrev = pItem;
      }
      pItem = pItem->h_next;
      continue;
    }
    return pItem;
  }

  return NULL;
}

static cacheTableBucket* getTableBucketByKey(cacheTable* pTable, const char* key, uint8_t nkey) {
  assert(pTable != NULL);
  uint32_t hash = pTable->hashFp(key, nkey);
  uint32_t index = 0;
  cacheTableBucket* pBucket = NULL;

  taosWLockLatch(&(pTable->latch));
  if (pTable->expanding && (index = hash % pTable->capacity) >= pTable->expandIndex) {
    assert(pTable->pOldBucket != NULL);
    pBucket = &(pTable->pOldBucket[index]);
  } else {
    pBucket = &(pTable->pBucket[hash & hashmask(pTable->option.initHashPower)]);
  }
  taosWUnLockLatch(&(pTable->latch));

  return pBucket;
}

static void checkExpandTable(cacheTable* pTable) {
  taosWLockLatch(&(pTable->latch));
  if (pTable->expanding) {
    taosWUnLockLatch(&(pTable->latch));
    return;
  }

  if (pTable->nOverflow * 1.0 / pTable->capacity < 0.33) {
    taosWUnLockLatch(&(pTable->latch));
    return;
  }

  taosWUnLockLatch(&(pTable->latch));
  cacheMutexLock(&(pTable->mutex));
  pthread_t thread;
  pthread_create(&thread, NULL, expandTableThread, pTable);
}

static void removeTableItem(cache_t* pCache, cacheTable* pTable, cacheTableBucket* pBucket, 
                            cacheItem* pItem, cacheItem* pPrev, bool freeItem) {
  if (pItem != NULL) {
    if (pPrev != NULL) {
      pPrev->h_next = pItem->h_next;
    }
    if (pBucket->head == pItem) {
      pBucket->head = pItem->h_next;
    }
    assert(pBucket->num > 0);
    pBucket->num -= 1;
    if (pBucket->num < ITEM_OVERFLOW_PER_BUCKET && pBucket->overflow) {
      assert(pTable->nOverflow > 0);
      pBucket->overflow = false;
      pTable->nOverflow -= 1;
    }
    if (freeItem) cacheItemUnlink(pItem->pTable, pItem, CACHE_LOCK_LRU);   
  }
}

int cacheTablePut(cacheTable* pTable, cacheItem* pItem) {
  cacheTableBucket* pBucket = getTableBucketByKey(pTable, item_key(pItem), pItem->nkey);

  cacheItem *pOldItem, *pPrev;
  pOldItem = findItemByKey(pTable, item_key(pItem), pItem->nkey, &pPrev);

  removeTableItem(pTable->pCache, pTable, pBucket, pOldItem, pPrev, true);
  
  pItem->h_next = pBucket->head;
  pBucket->head = pItem;
  pBucket->num += 1;
  if (pBucket->num >= ITEM_OVERFLOW_PER_BUCKET && !pBucket->overflow) {
    pBucket->overflow = true;
    pTable->nOverflow += 1;
  }
  pItem->pTable = pTable;

  checkExpandTable(pTable);

  return CACHE_OK;
}

cacheItem* cacheTableGet(cacheTable* pTable, const char* key, uint8_t nkey) {  
  return findItemByKey(pTable, key, nkey, NULL);
}

void cacheTableRemove(cacheTable* pTable, const char* key, uint8_t nkey, bool freeItem) {
  uint32_t index = pTable->hashFp(key, nkey) % pTable->capacity;
  cacheTableBucket* pBucket = &(pTable->pBucket[index]);

  cacheItem *pItem, *pPrev;
  pItem = findItemByKey(pTable, key, nkey, &pPrev);

  removeTableItem(pTable->pCache, pTable, pBucket, pItem, pPrev, freeItem);
}

static void* expandTableThread(void *arg) {
  cacheTable* pTable = (cacheTable*)arg;
  expandCacheTable(pTable);
  return NULL;
}

static void expandCacheTable(cacheTable* pTable) {
  assert(pTable->expanding == false);
  assert(pTable->expandIndex == 0);
  assert(pTable->pOldBucket == NULL);
  
  uint32_t capacity;
  cacheMutex* pMutex;
  uint32_t i, newIndex;
  cacheItem *pItem, *h_next;
  cacheTableBucket* pBucket;

  pTable->option.initHashPower += 1;
  capacity = hashsize(pTable->option.initHashPower);
  
  cacheInfo("start expandCacheTable");
  //cacheInfo("expandCacheTable, old capacity: %ld, new capacity: %ld", pTable->capacity, capacity);

  pBucket = malloc(sizeof(cacheTableBucket) * capacity);
  if (pBucket == NULL) {
    cacheError("expanding bucket oom");
    cacheMutexUnlock(&(pTable->mutex));
    return;
  }

  memset(pBucket, 0, sizeof(cacheTableBucket) * capacity);
  
  for (i = 0; i < capacity; i++) {
    initTableBucket(&(pBucket[i]));
  }

  // swap old and new bucket array
  taosWLockLatch(&(pTable->latch));
  pTable->pOldBucket = pTable->pBucket;
  pTable->pBucket = pBucket;
  pTable->expanding = true;
  pTable->expandIndex = 0;
  taosWUnLockLatch(&(pTable->latch));

  pTable->nOverflow = 0;
  while (pTable->expandIndex < pTable->capacity) {
    pMutex = getItemMutexByIndex(pTable, pTable->expandIndex);
    if (cacheMutexTryLock(pMutex) != 0) {
      // sleep and try again
      usleep(10*1000);
      continue;
    }

    // move old bucket item to new hash table
    taosWLockLatch(&(pTable->latch));
    i = pTable->expandIndex;
    taosWUnLockLatch(&(pTable->latch));
    for (pItem = pTable->pOldBucket[i].head; pItem; pItem = h_next) {
      h_next = pItem->h_next;
      newIndex = pTable->hashFp(item_key(pItem), pItem->nkey) % capacity;
      pBucket = &(pTable->pBucket[newIndex]);
      pItem->h_next = pBucket->head;
      pBucket->head = pItem;
      pBucket->num += 1;
      if (pBucket->num >= ITEM_OVERFLOW_PER_BUCKET && !pBucket->overflow) {
        pBucket->overflow = true;
        pTable->nOverflow += 1;
      }
    }
    taosWLockLatch(&(pTable->latch));
    pTable->expandIndex += 1;
    pTable->pOldBucket[i].head = NULL;
    taosWUnLockLatch(&(pTable->latch));

    cacheMutexUnlock(pMutex);
  }
  
  cacheMutexUnlock(&(pTable->mutex));

  taosWLockLatch(&(pTable->latch));
  free(pTable->pOldBucket);
  pTable->expandIndex = 0;  
  pTable->capacity = capacity;
  pTable->expanding = false;
  pTable->pOldBucket = NULL;
  taosWUnLockLatch(&(pTable->latch));

  cacheInfo("end expandCacheTable");
  //cacheInfo("end expandCacheTable, old capacity: %ld, new capacity: %ld", pTable->capacity, capacity);
}

cacheIterator* cacheTableGetIterator(cacheTable* pTable) {
  cacheIterator* pIter = calloc(1, sizeof(cacheIterator));
  if (pIter == NULL) {
    return NULL;
  }

  cacheMutexLock(&(pTable->mutex));
  pIter->pTable = pTable;
  pIter->pItem  = pIter->pNext = NULL;
  pIter->bucketIndex = 0;
  pIter->bucketLocked = false;
  return pIter;
}

bool cacheTableIterateNext(cacheIterator* pIter) {
  cacheTable* pTable = pIter->pTable;
  cacheMutex* pMutex = NULL;
  cacheTableBucket* pBucket = NULL;

  if (pIter->pItem) {
    itemDecrRef(pIter->pItem);
    pIter->pItem = NULL;
  }
  
  while (pIter->bucketIndex < pTable->capacity && pIter->pItem == NULL) {
    pBucket = &(pTable->pBucket[pIter->bucketIndex]);

    if (pIter->bucketLocked) {
      // if the bucket has been locked, it means item is not the first item in the bucket
      if (pIter->pNext != NULL) {
        pIter->pItem = pIter->pNext;        
        pIter->pNext = pIter->pItem->h_next;
      } else {
        // the bucket has been traversed, move to the next bucket
        pMutex = getItemMutexByIndex(pTable, pIter->bucketIndex);
        cacheMutexUnlock(pMutex);
        pIter->bucketLocked = false;
        pIter->bucketIndex += 1;
      }

      continue;
    }

    pMutex = getItemMutexByIndex(pTable, pIter->bucketIndex);
    cacheMutexLock(pMutex);
    pIter->bucketLocked = true;
    
    // empty bucket, move to the next bucket
    if (pBucket->head == NULL) {
      cacheMutexUnlock(pMutex);
      pIter->bucketLocked = false;
      pIter->bucketIndex += 1;
      continue;
    }

    pIter->pItem = pBucket->head;
    pIter->pNext = pIter->pItem->h_next;
  }

  if (pIter->pItem) {
    itemIncrRef(pIter->pItem);
    return true;
  }

  return false;
}

void cacheTableIteratorFinal(cacheIterator* pIter) {
  cacheTable* pTable = pIter->pTable;

  if (pIter->pItem) {
    itemIncrRef(pIter->pItem);
  }

  if (pIter->bucketLocked) {    
    assert(pIter->bucketIndex == pTable->capacity);
    cacheMutex* pMutex = getItemMutexByIndex(pTable, pIter->bucketIndex - 1);
    cacheMutexUnlock(pMutex);
  }

  cacheMutexUnlock(&(pTable->mutex));
  free(pIter);
}
