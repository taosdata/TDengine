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
  while (i < len) i = (i << 1u);
  return i;
}

/**
 * Get SHashNode from hashlist, nodes from trash are not included.
 * @param pHashObj  Cache objection
 * @param key       key for hash
 * @param keyLen    key length
 * @param hashVal   hash value by hash function
 * @return
 */
FORCE_INLINE SHashNode *doGetNodeFromHashTable(SHashObj *pHashObj, const void *key, uint32_t keyLen, uint32_t *hashVal) {
  uint32_t hash = (*pHashObj->hashFp)(key, keyLen);
  
  int32_t    slot = HASH_INDEX(hash, pHashObj->capacity);
  SHashNode *pNode = pHashObj->hashList[slot];
  
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
 * Resize the hash list if the threshold is reached
 *
 * @param pHashObj
 */
static void taosHashTableResize(SHashObj *pHashObj);

/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. Requires a consecutive memory block, no pointer is allowed in pData.
 *                 Pointer copy causes memory access error.
 * @param dsize    size of data
 * @return         SHashNode
 */
static SHashNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal);

/**
 * Update the hash node
 *
 * @param pNode   hash node
 * @param key     key for generate hash value
 * @param keyLen  key length
 * @param pData   actual data
 * @param dsize   size of actual data
 * @return        hash node
 */
static SHashNode *doUpdateHashNode(SHashNode *pNode, const void *key, size_t keyLen, const void *pData, size_t dsize);

/**
 * insert the hash node at the front of the linked list
 *
 * @param pHashObj
 * @param pNode
 */
static void doAddToHashTable(SHashObj *pHashObj, SHashNode *pNode);

/**
 * Get the next element in hash table for iterator
 * @param pIter
 * @return
 */
static SHashNode *getNextHashNode(SHashMutableIterator *pIter);

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

  pHashObj->hashList = (SHashNode **)calloc(pHashObj->capacity, POINTER_BYTES);
  if (pHashObj->hashList == NULL) {
    free(pHashObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
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

size_t taosHashGetSize(const SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }

  return pHashObj->size;
}

int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t size) {
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

    if (pNewNode->prev) {
      pNewNode->prev->next = pNewNode;
    } else {
      int32_t slot = HASH_INDEX(pNewNode->hashVal, pHashObj->capacity);
      
      assert(pHashObj->hashList[slot] == pNode);
      pHashObj->hashList[slot] = pNewNode;
    }
  
    if (pNewNode->next) {
      (pNewNode->next)->prev = pNewNode;
    }
  }

  __unlock(pHashObj->lock);
  return 0;
}

void *taosHashGet(SHashObj *pHashObj, const void *key, size_t keyLen) {
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

void taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen) {
  __wr_lock(pHashObj->lock);

  uint32_t   val = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pHashObj, key, keyLen, &val);
  if (pNode == NULL) {
    __unlock(pHashObj->lock);
    return;
  }

  SHashNode *pNext = pNode->next;
  if (pNode->prev == NULL) {
    int32_t slot = HASH_INDEX(val, pHashObj->capacity);
    assert(pHashObj->hashList[slot] == pNode);
    
    pHashObj->hashList[slot] = pNext;
  } else {
    pNode->prev->next = pNext;
  }
  
  if (pNext != NULL) {
    pNext->prev = pNode->prev;
  }

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
      pNode = pHashObj->hashList[i];

      while (pNode) {
        pNext = pNode->next;
        if (pHashObj->freeFp) {
          pHashObj->freeFp(pNode->data);
        }

        free(pNode);
        pNode = pNext;
      }
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

bool taosHashIterNext(SHashMutableIterator *pIter) {
  if (pIter == NULL) {
    return false;
  }

  size_t size = taosHashGetSize(pIter->pHashObj);
  if (size == 0) {
    return false;
  }

  // check the first one
  if (pIter->num == 0) {
    assert(pIter->pCur == NULL && pIter->pNext == NULL);

    while (1) {
      SHashNode *pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
      if (pEntry == NULL) {
        pIter->entryIndex++;
        continue;
      }

      pIter->pCur = pEntry;

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
    SHashNode *pEntry = pHashObj->hashList[i];
    if (pEntry == NULL) {
      continue;
    }
    
    int32_t j = 0;
    while(pEntry != NULL) {
      pEntry = pEntry->next;
      j++;
    }
    
    if (num < j) {
      num = j;
    }
  }

  return num;
}

void taosHashTableResize(SHashObj *pHashObj) {
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

  void *pNewEntry = realloc(pHashObj->hashList, POINTER_BYTES * newSize);
  if (pNewEntry == NULL) {// todo handle error
//    uTrace("cache resize failed due to out of memory, capacity remain:%d", pHashObj->capacity);
    return;
  }
  
  pHashObj->hashList = pNewEntry;
  memset(&pHashObj->hashList[pHashObj->capacity], 0, POINTER_BYTES * (newSize - pHashObj->capacity));
  
  pHashObj->capacity = newSize;
  
  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    pNode = pHashObj->hashList[i];
    if (pNode != NULL) {
      assert(pNode->prev == NULL);
    }
    
    while (pNode) {
      int32_t j = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
      if (j == i) {  // this key locates in the same slot, no need to relocate it
        pNode = pNode->next;
      } else {
        pNext = pNode->next;
        
        if (pNode->prev == NULL) {  // first node of the overflow linked list
          pHashObj->hashList[i] = pNext;
        } else {
          pNode->prev->next = pNext;
        }
        
        if (pNext != NULL) {
          pNext->prev = pNode->prev;
        }
        
        // clear pointer
        pNode->next = NULL;
        pNode->prev = NULL;
        
        // added into new slot
        SHashNode *pNew = pHashObj->hashList[j];
        if (pNew != NULL) {
          assert(pNew->prev == NULL);
          pNew->prev = pNode;
        }
        
        pNode->next = pNew;
        pHashObj->hashList[j] = pNode;
        
        // continue
        pNode = pNext;
      }
    }
  }

//  uTrace("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", pHashObj->capacity,
//         ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

SHashNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal) {
  size_t totalSize = dsize + sizeof(SHashNode) + keyLen;
  
  SHashNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  memcpy(pNewNode->data, pData, dsize);
  
  pNewNode->key = pNewNode->data + dsize;
  memcpy(pNewNode->key, key, keyLen);
  pNewNode->keyLen = keyLen;
  
  pNewNode->hashVal = hashVal;
  return pNewNode;
}

SHashNode *doUpdateHashNode(SHashNode *pNode, const void *key, size_t keyLen, const void *pData, size_t dsize) {
  size_t size = dsize + sizeof(SHashNode) + keyLen;
  
  SHashNode *pNewNode = (SHashNode *)realloc(pNode, size);
  if (pNewNode == NULL) {
    return NULL;
  }
  
  memcpy(pNewNode->data, pData, dsize);
  
  pNewNode->key = pNewNode->data + dsize;
  assert(memcmp(pNewNode->key, key, keyLen) == 0 && keyLen == pNewNode->keyLen);
  
  memcpy(pNewNode->key, key, keyLen);
  return pNewNode;
}

void doAddToHashTable(SHashObj *pHashObj, SHashNode *pNode) {
  assert(pNode != NULL);
  
  int32_t index = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
  
  SHashNode* pEntry = pHashObj->hashList[index];
  if (pEntry != NULL) {
    pEntry->prev = pNode;
    
    pNode->next = pEntry;
    pNode->prev = NULL;
  }
  
  pHashObj->hashList[index] = pNode;
  pHashObj->size++;
}

SHashNode *getNextHashNode(SHashMutableIterator *pIter) {
  assert(pIter != NULL);
  
  pIter->entryIndex++;
  while (pIter->entryIndex < pIter->pHashObj->capacity) {
    SHashNode *pNode = pIter->pHashObj->hashList[pIter->entryIndex];
    if (pNode == NULL) {
      pIter->entryIndex++;
      continue;
    }
    
    return pNode;
  }
  
  return NULL;
}
