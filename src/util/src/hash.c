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

#include "os.h"

#include "hash.h"
#include "tulog.h"
#include "ttime.h"
#include "tutil.h"

static FORCE_INLINE void __wr_lock(void *lock) {
  if (lock == NULL) {
    return;
  }

#if defined(LINUX)
  pthread_rwlock_wrlock(lock);
#else
  pthread_mutex_lock(lock);
#endif
}

static FORCE_INLINE void __rd_lock(void *lock) {
  if (lock == NULL) {
    return;
  }

#if defined(LINUX)
  pthread_rwlock_rdlock(lock);
#else
  pthread_mutex_lock(lock);
#endif
}

static FORCE_INLINE void __unlock(void *lock) {
  if (lock == NULL) {
    return;
  }

#if defined(LINUX)
  pthread_rwlock_unlock(lock);
#else
  pthread_mutex_unlock(lock);
#endif
}

static FORCE_INLINE int32_t __lock_init(void *lock) {
  if (lock == NULL) {
    return 0;
  }

#if defined(LINUX)
  return pthread_rwlock_init(lock, NULL);
#else
  return pthread_mutex_init(lock, NULL);
#endif
}

static FORCE_INLINE void __lock_destroy(void *lock) {
  if (lock == NULL) {
    return;
  }

#if defined(LINUX)
  pthread_rwlock_destroy(lock);
#else
  pthread_mutex_destroy(lock);
#endif
}

static FORCE_INLINE int32_t taosHashCapacity(int32_t length) {
  int32_t len = MIN(length, HASH_MAX_CAPACITY);

  uint32_t i = 4;
  while (i < len) i = (i << 1U);
  return i;
}

/**
 * inplace update node in hash table
 * @param pHashObj      hash table object
 * @param pNode     data node
 */
static void doUpdateHashTable(SHashObj *pHashObj, SHashNode *pNode) {
  if (pNode->prev1) {
    pNode->prev1->next = pNode;
  }

  if (pNode->next) {
    (pNode->next)->prev = pNode;
  }
}

/**
 * get SHashNode from hashlist, nodes from trash are not included.
 * @param pHashObj  Cache objection
 * @param key       key for hash
 * @param keyLen    key length
 * @return
 */
static SHashNode *doGetNodeFromHashTable(SHashObj *pHashObj, const char *key, uint32_t keyLen, uint32_t *hashVal) {
  uint32_t hash = (*pHashObj->hashFp)(key, keyLen);

  int32_t     slot = HASH_INDEX(hash, pHashObj->capacity);
  SHashEntry *pEntry = pHashObj->hashList[slot];

  SHashNode *pNode = pEntry->next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(pNode->key, key, keyLen) == 0)) {
      break;
    }

    pNode = pNode->next;
  }

  if (pNode) {
    assert(HASH_INDEX(pNode->hashVal, pHashObj->capacity) == slot);
  }

  // return the calculated hash value, to avoid calculating it again in other functions
  if (hashVal != NULL) {
    *hashVal = hash;
  }

  return pNode;
}

/**
 * resize the hash list if the threshold is reached
 *
 * @param pHashObj
 */
static void taosHashTableResize(SHashObj *pHashObj) {
  if (pHashObj->size < pHashObj->capacity * HASH_DEFAULT_LOAD_FACTOR) {
    return;
  }

  // double the original capacity
  SHashNode *pNode = NULL;
  SHashNode *pNext = NULL;

  int32_t newSize = pHashObj->capacity << 1u;
  if (newSize > HASH_MAX_CAPACITY) {
//    uTrace("current capacity:%d, maximum capacity:%d, no resize applied due to limitation is reached",
//           pHashObj->capacity, HASH_MAX_CAPACITY);
    return;
  }

//  int64_t st = taosGetTimestampUs();

  SHashEntry **pNewEntry = realloc(pHashObj->hashList, sizeof(SHashEntry *) * newSize);
  if (pNewEntry == NULL) {
//    uTrace("cache resize failed due to out of memory, capacity remain:%d", pHashObj->capacity);
    return;
  }

  pHashObj->hashList = pNewEntry;
  for (int32_t i = pHashObj->capacity; i < newSize; ++i) {
    pHashObj->hashList[i] = calloc(1, sizeof(SHashEntry));
  }

  pHashObj->capacity = newSize;

  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    SHashEntry *pEntry = pHashObj->hashList[i];

    pNode = pEntry->next;
    if (pNode != NULL) {
      assert(pNode->prev1 == pEntry && pEntry->num > 0);
    }

    while (pNode) {
      int32_t j = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
      if (j == i) {  // this key resides in the same slot, no need to relocate it
        pNode = pNode->next;
      } else {
        pNext = pNode->next;

        // remove from current slot
        assert(pNode->prev1 != NULL);

        if (pNode->prev1 == pEntry) {  // first node of the overflow linked list
          pEntry->next = pNode->next;
        } else {
          pNode->prev->next = pNode->next;
        }

        pEntry->num--;
        assert(pEntry->num >= 0);

        if (pNode->next != NULL) {
          (pNode->next)->prev = pNode->prev;
        }

        // added into new slot
        pNode->next = NULL;
        pNode->prev1 = NULL;

        SHashEntry *pNewIndexEntry = pHashObj->hashList[j];

        if (pNewIndexEntry->next != NULL) {
          assert(pNewIndexEntry->next->prev1 == pNewIndexEntry);

          pNewIndexEntry->next->prev = pNode;
        }

        pNode->next = pNewIndexEntry->next;
        pNode->prev1 = pNewIndexEntry;

        pNewIndexEntry->next = pNode;
        pNewIndexEntry->num++;

        // continue
        pNode = pNext;
      }
    }
  }

//  int64_t et = taosGetTimestampUs();
//  uTrace("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", pHashObj->capacity,
//         ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

/**
 * @param capacity maximum slots available for hash elements
 * @param fn       hash function
 * @return
 */
SHashObj *taosHashInit(size_t capacity, _hash_fn_t fn, bool threadsafe) {
  if (capacity == 0 || fn == NULL) {
    return NULL;
  }

  SHashObj *pHashObj = (SHashObj *)calloc(1, sizeof(SHashObj));
  if (pHashObj == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  // the max slots is not defined by user
  pHashObj->capacity = taosHashCapacity(capacity);
  assert((pHashObj->capacity & (pHashObj->capacity - 1)) == 0);

  pHashObj->hashFp = fn;

  pHashObj->hashList = (SHashEntry **)calloc(pHashObj->capacity, sizeof(SHashEntry *));
  if (pHashObj->hashList == NULL) {
    free(pHashObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    pHashObj->hashList[i] = calloc(1, sizeof(SHashEntry));
  }

  if (threadsafe) {
#if defined(LINUX)
    pHashObj->lock = calloc(1, sizeof(pthread_rwlock_t));
#else
    pHashObj->lock = calloc(1, sizeof(pthread_mutex_t));
#endif
  }

  if (__lock_init(pHashObj->lock) != 0) {
    free(pHashObj->hashList);
    free(pHashObj);

    uError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  return pHashObj;
}

/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. required a consecutive memory block, no pointer is allowed
 *                 in pData. Pointer copy causes memory access error.
 * @param size     size of block
 * @return         SHashNode
 */
static SHashNode *doCreateHashNode(const char *key, size_t keyLen, const char *pData, size_t dataSize,
                                   uint32_t hashVal) {
  size_t totalSize = dataSize + sizeof(SHashNode) + keyLen + 1;  // one extra byte for null

  SHashNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNewNode->data, pData, dataSize);

  pNewNode->key = pNewNode->data + dataSize;
  memcpy(pNewNode->key, key, keyLen);
  pNewNode->keyLen = keyLen;

  pNewNode->hashVal = hashVal;

  return pNewNode;
}

static SHashNode *doUpdateHashNode(SHashNode *pNode, const char *key, size_t keyLen, const char *pData,
                                   size_t dataSize) {
  size_t size = dataSize + sizeof(SHashNode) + keyLen;

  SHashNode *pNewNode = (SHashNode *)realloc(pNode, size);
  if (pNewNode == NULL) {
    return NULL;
  }

  memcpy(pNewNode->data, pData, dataSize);

  pNewNode->key = pNewNode->data + dataSize;

  assert(memcmp(pNewNode->key, key, keyLen) == 0 && keyLen == pNewNode->keyLen);

  memcpy(pNewNode->key, key, keyLen);
  return pNewNode;
}

/**
 * insert the hash node at the front of the linked list
 *
 * @param pHashObj
 * @param pNode
 */
static void doAddToHashTable(SHashObj *pHashObj, SHashNode *pNode) {
  assert(pNode != NULL);

  int32_t     index = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
  SHashEntry *pEntry = pHashObj->hashList[index];

  pNode->next = pEntry->next;

  if (pEntry->next) {
    pEntry->next->prev = pNode;
  }

  pEntry->next = pNode;
  pNode->prev1 = pEntry;

  pEntry->num++;
  pHashObj->size++;
}

size_t taosHashGetSize(const SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }

  return pHashObj->size;
}

/**
 * add data node into hash table
 * @param pHashObj    hash object
 * @param pNode   hash node
 */
int32_t taosHashPut(SHashObj *pHashObj, const char *key, size_t keyLen, void *data, size_t size) {
  __wr_lock(pHashObj->lock);

  uint32_t   hashVal = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pHashObj, key, keyLen, &hashVal);

  if (pNode == NULL) {  // no data in hash table with the specified key, add it into hash table
    taosHashTableResize(pHashObj);

    SHashNode *pNewNode = doCreateHashNode(key, keyLen, data, size, hashVal);
    if (pNewNode == NULL) {
      __unlock(pHashObj->lock);

      return -1;
    }

    doAddToHashTable(pHashObj, pNewNode);
  } else {
    SHashNode *pNewNode = doUpdateHashNode(pNode, key, keyLen, data, size);
    if (pNewNode == NULL) {
      __unlock(pHashObj->lock);
      return -1;
    }

    doUpdateHashTable(pHashObj, pNewNode);
  }

  __unlock(pHashObj->lock);
  return 0;
}

void *taosHashGet(SHashObj *pHashObj, const char *key, size_t keyLen) {
  __rd_lock(pHashObj->lock);

  uint32_t   hashVal = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pHashObj, key, keyLen, &hashVal);

  __unlock(pHashObj->lock);

  if (pNode != NULL) {
    assert(pNode->hashVal == hashVal);

    return pNode->data;
  } else {
    return NULL;
  }
}

/**
 * remove node in hash list
 * @param pHashObj
 * @param pNode
 */
void taosHashRemove(SHashObj *pHashObj, const char *key, size_t keyLen) {
  __wr_lock(pHashObj->lock);

  uint32_t   val = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pHashObj, key, keyLen, &val);
  if (pNode == NULL) {
    __unlock(pHashObj->lock);
    return;
  }

  SHashNode *pNext = pNode->next;
  if (pNode->prev != NULL) {
    int32_t slot = HASH_INDEX(val, pHashObj->capacity);
    if (pHashObj->hashList[slot]->next == pNode) {
      pHashObj->hashList[slot]->next = pNext;
    } else {
      pNode->prev->next = pNext;
    }
  }

  if (pNext != NULL) {
    pNext->prev = pNode->prev;
  }

  uint32_t index = HASH_INDEX(pNode->hashVal, pHashObj->capacity);

  SHashEntry *pEntry = pHashObj->hashList[index];
  pEntry->num--;

  pHashObj->size--;

  pNode->next = NULL;
  pNode->prev = NULL;

  tfree(pNode);
  __unlock(pHashObj->lock);
}

void taosHashCleanup(SHashObj *pHashObj) {
  if (pHashObj == NULL || pHashObj->capacity <= 0) {
    return;
  }

  SHashNode *pNode, *pNext;

  __wr_lock(pHashObj->lock);

  if (pHashObj->hashList) {
    for (int32_t i = 0; i < pHashObj->capacity; ++i) {
      SHashEntry *pEntry = pHashObj->hashList[i];
      pNode = pEntry->next;

      while (pNode) {
        pNext = pNode->next;
        if (pHashObj->freeFp) {
          pHashObj->freeFp(pNode->data);
        }

        free(pNode);
        pNode = pNext;
      }

      tfree(pEntry);
    }

    free(pHashObj->hashList);
  }

  __unlock(pHashObj->lock);
  __lock_destroy(pHashObj->lock);

  tfree(pHashObj->lock);
  memset(pHashObj, 0, sizeof(SHashObj));
  free(pHashObj);
}

void taosHashSetFreecb(SHashObj *pHashObj, _hash_free_fn_t freeFp) {
  if (pHashObj == NULL || freeFp == NULL) {
    return;
  }

  pHashObj->freeFp = freeFp;
}

SHashMutableIterator *taosHashCreateIter(SHashObj *pHashObj) {
  SHashMutableIterator *pIter = calloc(1, sizeof(SHashMutableIterator));
  if (pIter == NULL) {
    return NULL;
  }

  pIter->pHashObj = pHashObj;
  return pIter;
}

static SHashNode *getNextHashNode(SHashMutableIterator *pIter) {
  assert(pIter != NULL);

  while (pIter->entryIndex < pIter->pHashObj->capacity) {
    SHashEntry *pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
    if (pEntry->next == NULL) {
      pIter->entryIndex++;
      continue;
    }

    return pEntry->next;
  }

  return NULL;
}

bool taosHashIterNext(SHashMutableIterator *pIter) {
  if (pIter == NULL) {
    return false;
  }

  size_t size = taosHashGetSize(pIter->pHashObj);
  if (size == 0 || pIter->num >= size) {
    return false;
  }

  // check the first one
  if (pIter->num == 0) {
    assert(pIter->pCur == NULL && pIter->pNext == NULL);

    while (1) {
      SHashEntry *pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
      if (pEntry->next == NULL) {
        pIter->entryIndex++;
        continue;
      }

      pIter->pCur = pEntry->next;

      if (pIter->pCur->next) {
        pIter->pNext = pIter->pCur->next;
      } else {
        pIter->pNext = getNextHashNode(pIter);
      }

      break;
    }

    pIter->num++;
    return true;
  } else {
    assert(pIter->pCur != NULL);
    if (pIter->pNext) {
      pIter->pCur = pIter->pNext;
    } else {  // no more data in the hash list
      return false;
    }

    pIter->num++;

    if (pIter->pCur->next) {
      pIter->pNext = pIter->pCur->next;
    } else {
      pIter->pNext = getNextHashNode(pIter);
    }

    return true;
  }
}

void *taosHashIterGet(SHashMutableIterator *iter) { return (iter == NULL) ? NULL : iter->pCur->data; }

void *taosHashDestroyIter(SHashMutableIterator *iter) {
  if (iter == NULL) {
    return NULL;
  }

  free(iter);
  return NULL;
}

// for profile only
int32_t taosHashGetMaxOverflowLinkLength(const SHashObj *pHashObj) {
  if (pHashObj == NULL || pHashObj->size == 0) {
    return 0;
  }

  int32_t num = 0;

  for (int32_t i = 0; i < pHashObj->size; ++i) {
    SHashEntry *pEntry = pHashObj->hashList[i];
    if (num < pEntry->num) {
      num = pEntry->num;
    }
  }

  return num;
}
