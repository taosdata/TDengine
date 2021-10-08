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

#ifndef TDENGINE_HASH_H
#define TDENGINE_HASH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "tarray.h"
#include "tlockfree.h"

typedef uint32_t (*_hash_fn_t)(const char *, uint32_t);
typedef int32_t  (*_equal_fn_t)(const void*, const void*, uint32_t len);
typedef void (*_hash_before_fn_t)(void *);
typedef void (*_hash_free_fn_t)(void *);

#define HASH_MAX_CAPACITY (1024 * 1024 * 16)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)

#define HASH_INDEX(v, c) ((v) & ((c)-1))

/**
 * murmur hash algorithm
 * @key  usually string
 * @len  key length
 * @seed hash seed
 * @out  an int32 value
 */
uint32_t MurmurHash3_32(const char *key, uint32_t len);

/**
 *
 * @param key
 * @param len
 * @return
 */
uint32_t taosIntHash_32(const char *key, uint32_t len);
uint32_t taosIntHash_64(const char *key, uint32_t len);

_hash_fn_t taosGetDefaultHashFunction(int32_t type);

typedef struct SHashNode {
  struct SHashNode *next;
  uint32_t          hashVal;     // the hash value of key
  uint32_t          dataLen;     // length of data
  uint32_t          keyLen;      // length of the key
  uint16_t          count;       // reference count
  int8_t            removed;     // flag to indicate removed
  char              data[];
} SHashNode;

#define GET_HASH_NODE_KEY(_n)  ((char*)(_n) + sizeof(SHashNode) + (_n)->dataLen)
#define GET_HASH_NODE_DATA(_n) ((char*)(_n) + sizeof(SHashNode))
#define GET_HASH_PNODE(_n) ((SHashNode *)((char*)(_n) - sizeof(SHashNode)))

typedef enum SHashLockTypeE {
  HASH_NO_LOCK     = 0,
  HASH_ENTRY_LOCK  = 1,
} SHashLockTypeE;

typedef struct SHashEntry {
  int32_t    num;      // number of elements in current entry
  SRWLatch   latch;    // entry latch
  SHashNode *next;
} SHashEntry;

typedef struct SHashObj {
  SHashEntry    **hashList;
  uint32_t        capacity;     // number of slots
  uint32_t        size;         // number of elements in hash table

  _hash_fn_t      hashFp;       // hash function
  _hash_free_fn_t freeFp;       // hash node free callback function
  _equal_fn_t     equalFp;      // equal function
  _hash_before_fn_t callbackFp; // function invoked before return the value to caller

  SRWLatch        lock;         // read-write spin lock
  SHashLockTypeE  type;         // lock type
  bool            enableUpdate; // enable update
  SArray         *pMemBlock;    // memory block allocated for SHashEntry
} SHashObj;

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
int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t size);

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
void *taosHashGetClone(SHashObj *pHashObj, const void *key, size_t keyLen, void* destBuf);

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
void  taosHashCancelIterate(SHashObj *pHashObj, void *p);

/**
 * Get the corresponding key information for a given data in hash table
 * @param pHashObj
 * @param data
 * @return
 */
int32_t taosHashGetKey(SHashObj *pHashObj, void *data, void** key, size_t* keyLen);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASH_H
