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

#ifndef _TD_UTIL_HASH_H_
#define _TD_UTIL_HASH_H_

#include "tarray.h"
#include "tlockfree.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t (*_hash_fn_t)(const char *, uint32_t);
typedef int32_t (*_equal_fn_t)(const void *, const void *, size_t len);
typedef void (*_hash_before_fn_t)(void *);
typedef void (*_hash_free_fn_t)(void *);

#define HASH_KEY_ALREADY_EXISTS (-2)
#define HASH_NODE_EXIST(code)   (code == HASH_KEY_ALREADY_EXISTS)

/**
 * murmur hash algorithm
 * @key  usually string
 * @len  key length
 * @seed hash seed
 * @out  an int32 value
 */
uint32_t MurmurHash3_32(const char *key, uint32_t len);

uint64_t MurmurHash3_64(const char *key, uint32_t len);
/**
 *
 * @param key
 * @param len
 * @return
 */
uint32_t taosIntHash_32(const char *key, uint32_t len);
uint32_t taosIntHash_64(const char *key, uint32_t len);

uint32_t taosFastHash(const char *key, uint32_t len);
uint32_t taosDJB2Hash(const char *key, uint32_t len);

_hash_fn_t  taosGetDefaultHashFunction(int32_t type);
_equal_fn_t taosGetDefaultEqualFunction(int32_t type);

typedef enum SHashLockTypeE {
  HASH_NO_LOCK = 0,
  HASH_ENTRY_LOCK = 1,
} SHashLockTypeE;

typedef struct SHashNode SHashNode;
typedef struct SHashObj  SHashObj;

/**
 * init the hash table
 *
 * @param capacity    initial capacity of the hash table
 * @param fn          hash function to generate the hash value
 * @param threadsafe  thread safe or not
 * @return
 */
SHashObj *taosHashInit(size_t capacity, _hash_fn_t fn, bool update, SHashLockTypeE type);

/**
 * return the size of hash table
 * @param pHashObj
 * @return
 */
int32_t taosHashGetSize(const SHashObj *pHashObj);

/**
 * put element into hash table, if the element with the same key exists, update it
 * @param pHashObj
 * @param key
 * @param keyLen
 * @param data
 * @param size
 * @return
 */
int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, const void *data, size_t size);

/**
 * return the payload data with the specified key
 *
 * @param pHashObj
 * @param key
 * @param keyLen
 * @return
 */
void *taosHashGet(SHashObj *pHashObj, const void *key, size_t keyLen);

/**
 * Clone the result to destination buffer
 * @param pHashObj
 * @param key
 * @param keyLen
 * @param destBuf
 * @return
 */
int32_t taosHashGetDup(SHashObj *pHashObj, const void *key, size_t keyLen, void *destBuf);

/**
 *
 * @param pHashObj
 * @param key
 * @param keyLen
 * @param destBuf
 * @param size
 * @return
 */
int32_t taosHashGetDup_m(SHashObj *pHashObj, const void *key, size_t keyLen, void **destBuf, int32_t *size);

/**
 * remove item with the specified key
 * @param pHashObj
 * @param key
 * @param keyLen
 */
int32_t taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen);

/**
 * Clear the hash table.
 * @param pHashObj
 */
void taosHashClear(SHashObj *pHashObj);

/**
 * Clean up hash table and release all allocated resources.
 * @param handle
 */
void taosHashCleanup(SHashObj *pHashObj);

/**
 * Get the max overflow link list length
 * @param pHashObj
 * @return
 */
int32_t taosHashGetMaxOverflowLinkLength(const SHashObj *pHashObj);

/**
 * Get the hash table size
 * @param pHashObj
 * @return
 */
size_t taosHashGetMemSize(const SHashObj *pHashObj);

/**
 * Create the hash table iterator
 * @param pHashObj
 * @param p
 * @return
 */
void *taosHashIterate(SHashObj *pHashObj, void *p);

/**
 * Cancel the hash table iterator
 * @param pHashObj
 * @param p
 */
void taosHashCancelIterate(SHashObj *pHashObj, void *p);

/**
 * Get the corresponding key information for a given data in hash table
 * @param data
 * @param keyLen
 * @return
 */
void *taosHashGetKey(void *data, size_t *keyLen);

/**
 * return the payload data with the specified key(reference number added)
 *
 * @param pHashObj
 * @param key
 * @param keyLen
 * @return
 */
void *taosHashAcquire(SHashObj *pHashObj, const void *key, size_t keyLen);

/**
 * release the previous acquired obj
 *
 * @param pHashObj
 * @param data
 * @return
 */
void taosHashRelease(SHashObj *pHashObj, void *p);

/**
 *
 * @param pHashObj
 * @param fp
 */
void taosHashSetEqualFp(SHashObj *pHashObj, _equal_fn_t fp);

/**
 *
 * @param pHashObj
 * @param fp
 */
void taosHashSetFreeFp(SHashObj *pHashObj, _hash_free_fn_t fp);

int64_t taosHashGetCompTimes(SHashObj *pHashObj);

#ifdef __cplusplus
}
#endif

#endif  // _TD_UTIL_HASH_H_
