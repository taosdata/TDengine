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

#define hashsize(n) ((int32_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

extern uint32_t jenkins_hash(const void *key, size_t length);
extern uint32_t MurmurHash3_x86_32(const void *key, size_t length);

cacheTable* cacheCreateTable(cache_t* cache, cacheTableOption* option) {
  cacheTable* pTable = calloc(1, sizeof(cacheTable));
  if (pTable == NULL) {
    goto error;
  }

  if (cacheMutexInit(&pTable->mutex) != 0) {
    goto error;
  }

  pTable->pHandle = taosHashInit(option->initNum, option->hashFp, true, HASH_ENTRY_LOCK);
  if (pTable->pHandle == NULL) {
    goto error;
  }
  pTable->option = *option;
  pTable->pCache = cache;

  if (cache->tables == NULL) {
    cache->table = pTable;
    pTable->next = NULL;
  } else {
    pTable->next = cache->table;
    cache->table = pTable;
  }

  return pTable;

error:
  if (pTable) {
    free(pTable);
  }

  return NULL;
}

cache_code_t cacheTablePut(cacheTable* pTable, cacheItem* item) {
  cacheMutexLock(&(pTable->mutex));
  //taosHashPut(pTable->pHandle, item_key(item), item->nkey);
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