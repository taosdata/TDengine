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
#include "hash.h"

cacheTable* cacheCreateTable(cache_t* cache, cacheTableOption* option) {
  cacheTable* pTable = calloc(1, sizeof(cacheTable));
  if (pTable == NULL) {
    goto error;
  }

  if (cacheMutexInit(&pTable->mutex) != 0) {
    goto error;
  }

  pTable->pHandle = taosHashInit(option->initNum, taosGetDefaultHashFunction(option->keyType), true, HASH_NO_LOCK);
  if (pTable->pHandle == NULL) {
    goto error;
  }
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

int cacheTablePut(cacheTable* pTable, cacheItem* item) {
  cacheMutexLock(&(pTable->mutex));
  taosHashPut(pTable->pHandle, item_key(item), item->nkey, item, sizeof(cacheItem*));
  cacheMutexUnlock(&(pTable->mutex));
  return CACHE_OK;
}

cacheItem* cacheTableGet(cacheTable* pTable, const char* key, uint8_t nkey) {
  cacheMutexLock(&(pTable->mutex));
  cacheItem* item = (cacheItem*)taosHashGet(pTable->pHandle, key, nkey);
  cacheMutexUnlock(&(pTable->mutex));
  return item;
}

void cacheTableRemove(cacheTable* pTable, const char* key, uint8_t nkey) {
  cacheMutexLock(&(pTable->mutex));
  taosHashRemove(pTable->pHandle,key, nkey);
  cacheMutexUnlock(&(pTable->mutex));
}