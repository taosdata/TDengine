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

#ifndef TDENGINE_CACHE_HASHTABLE_H
#define TDENGINE_CACHE_HASHTABLE_H

#include <stdbool.h>
#include <stdint.h>
#include "cache.h"
#include "cacheMutex.h"
#include "cacheTypes.h"
#include "hashfunc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct cacheTable { 
  cache_t* pCache;
  cacheMutex mutex;
  cacheTable* next;
  cacheTableOption option;

  _hash_fn_t hashFp;

  uint32_t capacity;

  cacheItem** ppItems;
};

int cacheTablePut(cacheTable *pTable, cacheItem* item);
cacheItem* cacheTableGet(cacheTable* pTable, const char* key, uint8_t nkey);
void          cacheTableRemove(cacheTable* pTable, const char* key, uint8_t nkey);

#ifdef __cplusplus
}
#endif

#endif /* TDENGINE_CACHE_HASHTABLE_H */
