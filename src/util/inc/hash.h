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
#include "hashfunc.h"
#include "tlockfree.h"

#define HASH_MAX_CAPACITY (1024 * 1024 * 16)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c) ((v) & ((c)-1))

typedef void (*_hash_free_fn_t)(void *param);

typedef struct SHashNode {
  struct SHashNode *next;
  uint32_t          hashVal;     // the hash value of key
  uint32_t          dataLen;     // length of data
  uint32_t          keyLen;      // length of the key
  int8_t            removed;     // flag to indicate removed
  int8_t            count;       // reference count
  char              data[];
} SHashNode;

#define GET_HASH_NODE_KEY(_n)  ((char*)(_n) + sizeof(SHashNode) + (_n)->dataLen)
#define GET_HASH_NODE_DATA(_n) ((char*)(_n) + sizeof(SHashNode))
#define GET_HASH_PNODE(_n) ((char*)(_n) - sizeof(SHashNode));

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
  size_t          capacity;     // number of slots
  size_t          size;         // number of elements in hash table
  _hash_fn_t      hashFp;       // hash function
  _hash_free_fn_t freeFp;       // hash node free callback function

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
 * apply the udf before return the result
 * @param pHashObj
 * @param key
 * @param keyLen
 * @param fp
 * @param d
 * @param dsize
 * @return
 */
void* taosHashGetClone(SHashObj *pHashObj, const void *key, size_t keyLen, void (*fp)(void *), void* d, size_t dsize);

/**
 * remove item with the specified key
 * @param pHashObj
 * @param key
 * @param keyLen
 */
int32_t taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen);

int32_t taosHashRemoveWithData(SHashObj *pHashObj, const void *key, size_t keyLen, void* data, size_t dsize);

int32_t taosHashCondTraverse(SHashObj *pHashObj, bool (*fp)(void *, void *), void *param);

void taosHashEmpty(SHashObj *pHashObj);

/**
 * clean up hash table
 * @param handle
 */
void taosHashCleanup(SHashObj *pHashObj);

/**
 *
 * @param pHashObj
 * @return
 */
int32_t taosHashGetMaxOverflowLinkLength(const SHashObj *pHashObj);

size_t taosHashGetMemSize(const SHashObj *pHashObj);

void *taosHashIterate(SHashObj *pHashObj, void *p);
void  taosHashCancelIterate(SHashObj *pHashObj, void *p);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASH_H
