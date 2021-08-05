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

#ifndef TDENGINE_CACHE_H
#define TDENGINE_CACHE_H

#include <stddef.h> // for size_t
#include <stdint.h> // for uint8_t
#include "hashfunc.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*cache_load_func_t)(void*, const char* key, uint8_t nkey, char** value, size_t *len, uint64_t *pExpire);
typedef int (*cache_del_func_t)(void*, const char* key, uint8_t nkey);

typedef struct cacheOption {
  size_t limit;               /* size limit */

  double factor;              /* slab growth factor */

  int hotPercent;             /* percentage of slab space for CACHE_LRU_HOT */

  int warmPercent;            /* percentage of slab space for CACHE_LRU_WARM */
} cacheOption;

typedef struct cacheTableOption {
  cache_load_func_t loadFp; /* user defined load data function */

  cache_del_func_t  delFp;  /* user defined delete data function */

  int initHashPower;          /* table initial hash power,in [10,32] */

  void* userData;             /* user data */
  int32_t keyType;
} cacheTableOption;

enum {
  CACHE_OK            = 0,
  CACHE_FAIL          = -1,
  CACHE_OOM           = -2,
  CACHE_KEY_NOT_FOUND = -3,
  CACHE_REACH_LIMIT   = -4,
  CACHE_ALLOC_FAIL    = -5,
};

struct cache_t;
typedef struct cache_t cache_t;

struct cacheTable;
typedef struct cacheTable cacheTable;

struct cacheItem;
typedef struct cacheItem cacheItem;

struct cacheIterator;
typedef struct cacheIterator cacheIterator;

cache_t* cacheCreate(cacheOption* options);

void  cacheDestroy(cache_t*);

cacheTable* cacheCreateTable(cache_t* cache, cacheTableOption* options);
void cacheDestroyTable(cacheTable*);

int cachePut(cacheTable*, const char* key, uint8_t nkey, const char* value, uint32_t nbytes, uint64_t expire);

/* data MUST be free by user */
int cacheGet(cacheTable*, const char* key, uint8_t nkey, char** data, int* nbytes);

void cacheRemove(cacheTable* pTable, const char* key, uint8_t nkey);

cacheIterator* cacheTableGetIterator(cacheTable*);
bool cacheTableIterateNext(cacheIterator*);

void cacheTableIteratorFinal(cacheIterator*);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_CACHE_H