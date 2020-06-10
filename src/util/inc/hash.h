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

#include "hashfunc.h"

#define HASH_MAX_CAPACITY (1024 * 1024 * 16)
#define HASH_DEFAULT_LOAD_FACTOR (0.75)
#define HASH_INDEX(v, c) ((v) & ((c)-1))

typedef void (*_hash_free_fn_t)(void *param);

typedef struct SHashNode {
  char *key;
//  union {
    struct SHashNode * prev;
//    struct SHashEntry *prev1;
//  };
//
  struct SHashNode *next;
  uint32_t          hashVal;  // the hash value of key, if hashVal == HASH_VALUE_IN_TRASH, this node is moved to trash
  uint32_t          keyLen;   // length of the key
  char              data[];
} SHashNode;

typedef struct SHashObj {
  SHashNode     **hashList;
  size_t          capacity;  // number of slots
  size_t          size;      // number of elements in hash table
  _hash_fn_t      hashFp;    // hash function
  _hash_free_fn_t freeFp;    // hash node free callback function

#if defined(LINUX)
  pthread_rwlock_t *lock;
#else
  pthread_mutex_t *lock;
#endif
} SHashObj;

typedef struct SHashMutableIterator {
  SHashObj * pHashObj;
  int32_t    entryIndex;
  SHashNode *pCur;
  SHashNode *pNext;  // current node can be deleted for mutable iterator, so keep the next one before return current
  int32_t    num;    // already check number of elements in hash table
} SHashMutableIterator;

/**
 * init the hash table
 *
 * @param capacity    initial capacity of the hash table
 * @param fn          hash function to generate the hash value
 * @param threadsafe  thread safe or not
 * @return
 */
SHashObj *taosHashInit(size_t capacity, _hash_fn_t fn, bool threadsafe);

/**
 * return the size of hash table
 * @param pHashObj
 * @return
 */
size_t taosHashGetSize(const SHashObj *pHashObj);

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
 * remove item with the specified key
 * @param pHashObj
 * @param key
 * @param keyLen
 */
void taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen);

/**
 * clean up hash table
 * @param handle
 */
void taosHashCleanup(SHashObj *pHashObj);

/**
 * Set the free callback function
 * This function if set will be invoked right before freeing each hash node
 * @param pHashObj
 */
void taosHashSetFreecb(SHashObj *pHashObj, _hash_free_fn_t freeFp);

/**
 *
 * @param pHashObj
 * @return
 */
SHashMutableIterator* taosHashCreateIter(SHashObj *pHashObj);

/**
 *
 * @param iter
 * @return
 */
bool taosHashIterNext(SHashMutableIterator *iter);

/**
 *
 * @param iter
 * @return
 */
void *taosHashIterGet(SHashMutableIterator *iter);

/**
 *
 * @param iter
 * @return
 */
void* taosHashDestroyIter(SHashMutableIterator* iter);

/**
 *
 * @param pHashObj
 * @return
 */
int32_t taosHashGetMaxOverflowLinkLength(const SHashObj *pHashObj);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASH_H
