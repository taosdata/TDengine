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
#include "cacheTable.h"
#include "cache_priv.h"
#include "cacheItem.h"
#include "cacheSlab.h"
#include "cacheTypes.h"
#include "hash.h"

static int        initTableBucket(cacheTableBucket* pBucket, uint32_t id);
static cacheItem* findItemByKey(cacheTable* pTable, const char* key, uint8_t nkey, cacheItem **ppPrev);
static void       removeTableItem(cache_t* pCache, cacheTableBucket* pBucket, cacheItem* pItem,
                                cacheItem* pPrev, bool freeItem);

cacheTable* cacheCreateTable(cache_t* cache, cacheTableOption* option) {
  cacheTable* pTable = calloc(1, sizeof(cacheTable));
  if (pTable == NULL) {
    goto error;
  }

  if (cacheMutexInit(&pTable->mutex) != 0) {
    goto error;
  }

  pTable->capacity = option->initNum;
  pTable->pBucket = malloc(sizeof(cacheTableBucket) * pTable->capacity);
  if (pTable->pBucket == NULL) {
    goto error;
  }
  memset(pTable->pBucket, 0, sizeof(cacheTableBucket) * pTable->capacity);
  int i = 0;
  for (i = 0; i < pTable->capacity; i++) {
    initTableBucket(&(pTable->pBucket[i]), i);
  }

  pTable->hashFp = taosGetDefaultHashFunction(option->keyType);
  pTable->option = *option;
  pTable->pCache = cache;

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
  cacheMutexDestroy(&(pTable->mutex));

  free(pTable->pBucket);
  free(pTable);
}

static int initTableBucket(cacheTableBucket* pBucket, uint32_t id) {
  pBucket->hash = id;
  pBucket->head = NULL;
  return 0;
}

static cacheItem* findItemByKey(cacheTable* pTable, const char* key, uint8_t nkey, cacheItem **ppPrev) {
  cacheItem* pItem = NULL;
  uint32_t index = pTable->hashFp(key, nkey) % pTable->capacity;

  pItem = pTable->pBucket[index].head;
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

static void removeTableItem(cache_t* pCache, cacheTableBucket* pBucket, cacheItem* pItem,
                            cacheItem* pPrev, bool freeItem) {
  if (pItem != NULL) {
    if (pPrev != NULL) {
      pPrev->h_next = pItem->h_next;
    }
    if (pBucket->head == pItem) {
      pBucket->head = pItem->h_next;
    }
    
    if (freeItem) cacheItemUnlink(pItem->pTable, pItem, CACHE_LOCK_LRU);   
  }
}

int cacheTablePut(cacheTable* pTable, cacheItem* pItem) {
  uint32_t index = pTable->hashFp(item_key(pItem), pItem->nkey) % pTable->capacity;
  cacheTableBucket* pBucket = &(pTable->pBucket[index]);

  cacheItem *pOldItem, *pPrev;
  pOldItem = findItemByKey(pTable, item_key(pItem), pItem->nkey, &pPrev);

  removeTableItem(pTable->pCache, pBucket, pOldItem, pPrev, true);
  
  pItem->h_next = pBucket->head;
  pBucket->head = pItem;
  pItem->pTable = pTable;

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

  removeTableItem(pTable->pCache, pBucket, pItem, pPrev, freeItem);
}