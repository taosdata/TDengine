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

// TODO: SHashNode is an internal implementation and should not
// be in the public header file.
typedef struct SHashNode {
  struct SHashNode *next;
  uint32_t          hashVal;     // the hash value of key
  uint32_t          dataLen;     // length of data
  uint32_t          keyLen;      // length of the key
  int8_t            removed;     // flag to indicate removed
  int32_t           refCount;    // reference count
  char              data[];
} SHashNode;
  
#define GET_HASH_PNODE(_n) ((SHashNode *)((char*)(_n) - sizeof(SHashNode)))

typedef enum SHashLockTypeE {
  HASH_NO_LOCK     = 0,
  HASH_ENTRY_LOCK  = 1,
} SHashLockTypeE;

typedef struct SHashObj SHashObj;

/**
 * initialize a hash table
 *
 * @param capacity   initial capacity of the hash table
 * @param fn         hash function
 * @param update     whether the hash table allows in place update
 * @param type       whether the hash table has per entry lock
 * @return           hash table object
 */
SHashObj *taosHashInit(size_t capacity, _hash_fn_t fn, bool update, SHashLockTypeE type);

/**
 * set equal func of the hash table
 *
 * @param pHashObj
 * @param equalFp
 * @return
 */
void taosHashSetEqualFp(SHashObj *pHashObj, _equal_fn_t fp);

void taosHashSetFreeFp(SHashObj *pHashObj, _hash_free_fn_t fp);

/**
 * return the size of hash table
 *
 * @param pHashObj
 * @return
 */
int32_t taosHashGetSize(const SHashObj *pHashObj);

/**
 * put element into hash table, if the element with the same key exists, update it
 *
 * @param pHashObj   hash table object
 * @param key        key
 * @param keyLen     length of key
 * @param data       data
 * @param size       size of data
 * @return           0 if success, -1 otherwise
 */
int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t size);

/**
 * return the payload data with the specified key
 *
 * @param pHashObj   hash table object
 * @param key        key
 * @param keyLen     length of key
 * @return           pointer to data
 */
void *taosHashGet(SHashObj *pHashObj, const void *key, size_t keyLen);

/**
 * Get the data associated with "key". Note that caller needs to make sure
 * "d" has enough capacity to accomodate the data.
 *
 * @param pHashObj   hash table object
 * @param key        key
 * @param keyLen     length of key
 * @param fp         function to be called on hash node when the data is found
 * @param d          buffer
 * @return           pointer to data
 */
void* taosHashGetClone(SHashObj *pHashObj, const void *key, size_t keyLen, void (*fp)(void *), void* d);

/**
 * Get the data associated with "key". Note that caller needs to take ownership
 * of the data "d" and make sure it is deallocated.
 *
 * @param pHashObj   hash table object
 * @param key        key
 * @param keyLen     length of key
 * @param fp         function to be called on hash node when the data is found
 * @param d          buffer
 * @param sz         size of the data buffer
 * @return           pointer to data
 */
void* taosHashGetCloneExt(SHashObj *pHashObj, const void *key, size_t keyLen, void (*fp)(void *), void** d, size_t *sz);

/**
 * remove item with the specified key
 *
 * @param pHashObj   hash table object
 * @param key        key
 * @param keyLen     length of key
 * @return           0 if success, -1 otherwise
 */
int32_t taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen);

/**
 * remove item with the specified key
 *
 * @param pHashObj   hash table object
 * @param key        key
 * @param keyLen     length of key
 * @param data       buffer for data
 * @param dsize      size of data buffer
 * @return           0 if success, -1 otherwise
 */
int32_t taosHashRemoveWithData(SHashObj *pHashObj, const void *key, size_t keyLen, void* data, size_t dsize);

/**
 * traverse through all objects in the hash table and apply "fp" on each node.
 * If "fp" returns false when applied on top of a node, the node will also be
 * removed from table.
 *
 * @param pHashObj   hash table object
 * @param fp         function pointer applied on each node
 * @param param      parameter fed into "fp"
 */
void taosHashCondTraverse(SHashObj *pHashObj, bool (*fp)(void *, void *), void *param);

/**
 * clear the contents of the hash table
 *
 * @param pHashObj   hash table object
 */
void taosHashClear(SHashObj *pHashObj);

/**
 * clean up hash table
 *
 * @param pHashObj   hash table object
 */
void taosHashCleanup(SHashObj *pHashObj);

/**
 * return the number of collisions in the hash table
 *
 * @param pHashObj   hash table object
 * @return           maximum number of collisions
 */
int32_t taosHashGetMaxOverflowLinkLength(SHashObj *pHashObj);

/**
 * return the consumed memory of the hash table
 *
 * @param pHashObj   hash table object
 * @return           consumed memory of the hash table
 */
size_t taosHashGetMemSize(const SHashObj *pHashObj);

void *taosHashIterate(SHashObj *pHashObj, void *p);

void  taosHashCancelIterate(SHashObj *pHashObj, void *p);

void *taosHashGetDataKey(SHashObj *pHashObj, void *data);

uint32_t taosHashGetDataKeyLen(SHashObj *pHashObj, void *data);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_HASH_H
