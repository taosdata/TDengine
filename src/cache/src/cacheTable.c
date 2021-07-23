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
#include "cacheint.h"
#include "cacheItem.h"
#include "hash.h"

static cacheItem* cacheFindItemByKey(cacheTable* pTable, const char* key, uint8_t nkey, cacheItem **ppPrev);
static void       cacheRemoveTableItem(cache_t* pCache, cacheItem* pItem, cacheItem* pPrev);

cacheTable* cacheCreateTable(cache_t* cache, cacheTableOption* option) {
  cacheTable* pTable = calloc(1, sizeof(cacheTable));
  if (pTable == NULL) {
    goto error;
  }

  if (cacheMutexInit(&pTable->mutex) != 0) {
    goto error;
  }

  pTable->capacity = option->initNum;
  pTable->ppItems = malloc(sizeof(cacheItem*) * pTable->capacity);
  if (pTable->ppItems == NULL) {
    goto error;
  }
  memset(pTable->ppItems, 0, sizeof(cacheItem*) * pTable->capacity);

  pTable->hashFp = taosGetDefaultHashFunction(option->keyType);
  pTable->option = *option;
  pTable->pCache = cache;

  if (cache->tableHead == NULL) {
    cache->tableHead = pTable;
    pTable->next = NULL;
  } else {
    pTable->next = cache->tableHead;
    cache->tableHead = pTable;
  }

  return pTable;

error:
  if (pTable) {
    free(pTable);
  }

  return NULL;
}

static cacheItem* cacheFindItemByKey(cacheTable* pTable, const char* key, uint8_t nkey, cacheItem **ppPrev) {
  cacheItem* item = NULL;
  uint32_t index = pTable->hashFp(key, nkey) % pTable->capacity;

  item = pTable->ppItems[index];
  if (ppPrev) *ppPrev = NULL;

  while (item != NULL) {
    if (!item_equal_key(item, key, nkey)) {
      if (ppPrev) {
        *ppPrev = item;
      }
      item = item->h_next;
      continue;
    }
    return item;
  }

  return NULL;
}

static void cacheRemoveTableItem(cache_t* pCache, cacheItem* pItem, cacheItem* pPrev) {
  if (pItem != NULL) {
    if (pPrev != NULL) {
      pPrev->h_next = pItem->h_next;
    }
    cacheItemRemove(pCache, pItem);
  }
}

int cacheTablePut(cacheTable* pTable, cacheItem* item) {
  cacheMutexLock(&(pTable->mutex));

  cacheItem *pOldItem, *pPrev;
  pOldItem = cacheFindItemByKey(pTable, item_key(item), item->nkey, &pPrev);

  cacheRemoveTableItem(pTable->pCache, pOldItem, pPrev);

  uint32_t index = pTable->hashFp(item_key(item), item->nkey) % pTable->capacity;
  item->h_next = pTable->ppItems[index];
  pTable->ppItems[index] = item;

  cacheMutexUnlock(&(pTable->mutex));
  return CACHE_OK;
}

cacheItem* cacheTableGet(cacheTable* pTable, const char* key, uint8_t nkey) {
  cacheMutexLock(&(pTable->mutex));
  
  cacheItem *pItem = cacheFindItemByKey(pTable, key, nkey, NULL);

  cacheMutexUnlock(&(pTable->mutex));
  return pItem;
}

void cacheTableRemove(cacheTable* pTable, const char* key, uint8_t nkey) {
  cacheMutexLock(&(pTable->mutex));
  
  cacheItem *pItem, *pPrev;
  pItem = cacheFindItemByKey(pTable, key, nkey, &pPrev);

  cacheRemoveTableItem(pTable->pCache, pItem, pPrev);

  cacheMutexUnlock(&(pTable->mutex));
}