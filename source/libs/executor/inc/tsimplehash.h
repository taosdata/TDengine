/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifndef TDENGINE_TSIMPLEHASH_H
#define TDENGINE_TSIMPLEHASH_H

#include "tarray.h"
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t (*_hash_fn_t)(const char *, uint32_t);
typedef int32_t (*_equal_fn_t)(const void *, const void *, size_t len);
typedef void (*_hash_free_fn_t)(void *);

typedef struct SSHashObj SSHashObj;

/**
 * init the hash table
 *
 * @param capacity    initial capacity of the hash table
 * @param fn          hash function to generate the hash value
 * @return
 */
SSHashObj *tSimpleHashInit(size_t capacity, _hash_fn_t fn, size_t keyLen, size_t dataLen);

/**
 * return the size of hash table
 * @param pHashObj
 * @return
 */
int32_t tSimpleHashGetSize(const SSHashObj *pHashObj);

/**
 * put element into hash table, if the element with the same key exists, update it
 * @param pHashObj
 * @param key
 * @param data
 * @return
 */
int32_t tSimpleHashPut(SSHashObj *pHashObj, const void *key, const void *data);

/**
 * return the payload data with the specified key
 *
 * @param pHashObj
 * @param key
 * @return
 */
void *tSimpleHashGet(SSHashObj *pHashObj, const void *key);

/**
 * remove item with the specified key
 * @param pHashObj
 * @param key
 * @param keyLen
 */
int32_t tSimpleHashRemove(SSHashObj *pHashObj, const void *key);

/**
 * Clear the hash table.
 * @param pHashObj
 */
void tSimpleHashClear(SSHashObj *pHashObj);

/**
 * Clean up hash table and release all allocated resources.
 * @param handle
 */
void tSimpleHashCleanup(SSHashObj *pHashObj);

/**
 * Get the hash table size
 * @param pHashObj
 * @return
 */
size_t tSimpleHashGetMemSize(const SSHashObj *pHashObj);

/**
 * Get the corresponding key information for a given data in hash table
 * @param data
 * @param keyLen
 * @return
 */
void *tSimpleHashGetKey(const SSHashObj* pHashObj, void *data, size_t* keyLen);

#ifdef __cplusplus
}
#endif
#endif  // TDENGINE_TSIMPLEHASH_H
