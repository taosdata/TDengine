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
#include "tlog.h"
#include "ttime.h"
#include "tutil.h"

static FORCE_INLINE void __wr_lock(void *lock) {
#if defined              LINUX
  pthread_rwlock_wrlock(lock);
#else
  pthread_mutex_lock(lock);
#endif
}

static FORCE_INLINE void __rd_lock(void *lock) {
#if defined              LINUX
  pthread_rwlock_rdlock(lock);
#else
  pthread_mutex_lock(lock);
#endif
}

static FORCE_INLINE void __unlock(void *lock) {
#if defined              LINUX
  pthread_rwlock_unlock(lock);
#else
  pthread_mutex_unlock(lock);
#endif
}

static FORCE_INLINE int32_t __lock_init(void *lock) {
#if defined                 LINUX
  return pthread_rwlock_init(lock, NULL);
#else
  return pthread_mutex_init(lock, NULL);
#endif
}

static FORCE_INLINE void __lock_destroy(void *lock) {
#if defined              LINUX
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
 * hash key function
 *
 * @param key    key string
 * @param len    length of key
 * @return       hash value
 */
static FORCE_INLINE uint32_t taosHashKey(const char *key, uint32_t len) { return MurmurHash3_32(key, len); }

/**
 * inplace update node in hash table
 * @param pObj      hash table object
 * @param pNode     data node
 */
static void doUpdateHashTable(HashObj *pObj, SHashNode *pNode) {
  if (pNode->prev1) {
    pNode->prev1->next = pNode;
  }

  if (pNode->next) {
    (pNode->next)->prev = pNode;
  }

  pTrace("key:%s %p update hash table", pNode->key, pNode);
}

/**
 * get SHashNode from hashlist, nodes from trash are not included.
 * @param pObj      Cache objection
 * @param key       key for hash
 * @param keyLen    key length
 * @return
 */
static SHashNode *doGetNodeFromHashTable(HashObj *pObj, const char *key, uint32_t keyLen, uint32_t *hashVal) {
  uint32_t hash = (*pObj->hashFp)(key, keyLen);

  int32_t     slot = HASH_INDEX(hash, pObj->capacity);
  SHashEntry *pEntry = pObj->hashList[slot];

  SHashNode *pNode = pEntry->next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(pNode->key, key, keyLen) == 0)) {
      break;
    }

    pNode = pNode->next;
  }

  if (pNode) {
    assert(HASH_INDEX(pNode->hashVal, pObj->capacity) == slot);
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
 * @param pObj
 */
static void taosHashTableResize(HashObj *pObj) {
  if (pObj->size < pObj->capacity * HASH_DEFAULT_LOAD_FACTOR) {
    return;
  }

  // double the original capacity
  SHashNode *pNode = NULL;
  SHashNode *pNext = NULL;

  int32_t newSize = pObj->capacity << 1U;
  if (newSize > HASH_MAX_CAPACITY) {
    pTrace("current capacity:%d, maximum capacity:%d, no resize applied due to limitation is reached", pObj->capacity,
           HASH_MAX_CAPACITY);
    return;
  }

  int64_t st = taosGetTimestampUs();

  SHashEntry **pNewEntry = realloc(pObj->hashList, sizeof(SHashEntry*) * newSize);
  if (pNewEntry == NULL) {
    pTrace("cache resize failed due to out of memory, capacity remain:%d", pObj->capacity);
    return;
  }

  pObj->hashList = pNewEntry;
  for(int32_t i = pObj->capacity; i < newSize; ++i) {
    pObj->hashList[i] = calloc(1, sizeof(SHashEntry));
  }
  
  pObj->capacity = newSize;

  for (int32_t i = 0; i < pObj->capacity; ++i) {
    SHashEntry *pEntry = pObj->hashList[i];

    pNode = pEntry->next;
    if (pNode != NULL) {
      assert(pNode->prev1 == pEntry && pEntry->num > 0);
    }
    
    while (pNode) {
      int32_t j = HASH_INDEX(pNode->hashVal, pObj->capacity);
      if (j == i) {  // this key resides in the same slot, no need to relocate it
        pNode = pNode->next;
      } else {
        pNext = pNode->next;

        // remove from current slot
        assert(pNode->prev1 != NULL);
        
        if (pNode->prev1 == pEntry) { // first node of the overflow linked list
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

        SHashEntry *pNewIndexEntry = pObj->hashList[j];

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

  int64_t et = taosGetTimestampUs();

  pTrace("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", pObj->capacity,
         ((double)pObj->size) / pObj->capacity, (et - st) / 1000.0);
}

/**
 * @param capacity maximum slots available for hash elements
 * @param fn       hash function
 * @return
 */
void *taosInitHashTable(uint32_t capacity, _hash_fn_t fn, bool multithreadSafe) {
  if (capacity == 0 || fn == NULL) {
    return NULL;
  }

  HashObj *pObj = (HashObj *)calloc(1, sizeof(HashObj));
  if (pObj == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  // the max slots is not defined by user
  pObj->capacity = taosHashCapacity(capacity);
  assert((pObj->capacity & (pObj->capacity - 1)) == 0);

  pObj->hashFp = fn;

  pObj->hashList = (SHashEntry **)calloc(pObj->capacity, sizeof(SHashEntry*));
  if (pObj->hashList == NULL) {
    free(pObj);
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }
  
  for(int32_t i = 0; i < pObj->capacity; ++i) {
    pObj->hashList[i] = calloc(1, sizeof(SHashEntry));
  }

  if (multithreadSafe && (__lock_init(pObj) != 0)) {
    free(pObj->hashList);
    free(pObj);

    pError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  return (void *)pObj;
}

/**
 * @param key      key of object for hash, usually a null-terminated string
 * @param keyLen   length of key
 * @param pData    actually data. required a consecutive memory block, no pointer is allowed
 *                 in pData. Pointer copy causes memory access error.
 * @param size     size of block
 * @return         SHashNode
 */
static SHashNode *doCreateHashNode(const char *key, uint32_t keyLen, const char *pData, size_t dataSize,
                                   uint32_t hashVal) {
  size_t totalSize = dataSize + sizeof(SHashNode) + keyLen;

  SHashNode *pNewNode = calloc(1, totalSize);
  if (pNewNode == NULL) {
    pError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  memcpy(pNewNode->data, pData, dataSize);

  pNewNode->key = pNewNode->data + dataSize;
  memcpy(pNewNode->key, key, keyLen);
  pNewNode->keyLen = keyLen;

  pNewNode->hashVal = hashVal;

  return pNewNode;
}

static SHashNode *doUpdateHashNode(SHashNode *pNode, const char *key, uint32_t keyLen, const char *pData,
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
 * @param pObj
 * @param pNode
 */
static void doAddToHashTable(HashObj *pObj, SHashNode *pNode) {
  assert(pNode != NULL);

  int32_t     index = HASH_INDEX(pNode->hashVal, pObj->capacity);
  SHashEntry *pEntry = pObj->hashList[index];
  
  pNode->next = pEntry->next;

  if (pEntry->next) {
    pEntry->next->prev = pNode;
  }

  pEntry->next = pNode;
  pNode->prev1 = pEntry;
  
  pEntry->num++;
  pObj->size++;
}

int32_t taosNumElemsInHashTable(HashObj *pObj) {
  if (pObj == NULL) {
    return 0;
  }
  
  return pObj->size;
}

/**
 * add data node into hash table
 * @param pObj    hash object
 * @param pNode   hash node
 */
int32_t taosAddToHashTable(HashObj *pObj, const char *key, uint32_t keyLen, void *data, uint32_t size) {
  if (pObj->multithreadSafe) {
    __wr_lock(&pObj->lock);
  }

  uint32_t   hashVal = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pObj, key, keyLen, &hashVal);

  if (pNode == NULL) {  // no data in hash table with the specified key, add it into hash table
    taosHashTableResize(pObj);

    SHashNode *pNewNode = doCreateHashNode(key, keyLen, data, size, hashVal);
    if (pNewNode == NULL) {
      if (pObj->multithreadSafe) {
        __unlock(&pObj->lock);
      }

      return -1;
    }

    doAddToHashTable(pObj, pNewNode);
  } else {
    SHashNode *pNewNode = doUpdateHashNode(pNode, key, keyLen, data, size);
    if (pNewNode == NULL) {
      if (pObj->multithreadSafe) {
        __unlock(&pObj->lock);
      }

      return -1;
    }

    doUpdateHashTable(pObj, pNewNode);
  }

  if (pObj->multithreadSafe) {
    __unlock(&pObj->lock);
  }

  return 0;
}

char *taosGetDataFromHashTable(HashObj *pObj, const char *key, uint32_t keyLen) {
  if (pObj->multithreadSafe) {
    __rd_lock(&pObj->lock);
  }

  uint32_t   hashVal = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pObj, key, keyLen, &hashVal);

  if (pObj->multithreadSafe) {
    __unlock(&pObj->lock);
  }

  if (pNode != NULL) {
    assert(pNode->hashVal == hashVal);

    return pNode->data;
  } else {
    return NULL;
  }
}

/**
 * remove node in hash list
 * @param pObj
 * @param pNode
 */
void taosDeleteFromHashTable(HashObj *pObj, const char *key, uint32_t keyLen) {
  if (pObj->multithreadSafe) {
    __wr_lock(&pObj->lock);
  }

  uint32_t val = 0;
  SHashNode *pNode = doGetNodeFromHashTable(pObj, key, keyLen, &val);
  if (pNode == NULL) {
    if (pObj->multithreadSafe) {
      __unlock(&pObj->lock);
    }

    return;
  }

  SHashNode *pNext = pNode->next;
  if (pNode->prev != NULL) {
    int32_t slot = HASH_INDEX(val, pObj->capacity);
    if (pObj->hashList[slot]->next == pNode) {
      pObj->hashList[slot]->next = pNext;
    } else {
      pNode->prev->next = pNext;
    }
  }
  
  if (pNext != NULL) {
    pNext->prev = pNode->prev;
  }

  uint32_t    index = HASH_INDEX(pNode->hashVal, pObj->capacity);
  SHashEntry *pEntry = pObj->hashList[index];
  pEntry->num--;

  pObj->size--;

  pNode->next = NULL;
  pNode->prev = NULL;

  pTrace("key:%s %p remove from hash table", pNode->key, pNode);
  tfree(pNode);

  if (pObj->multithreadSafe) {
    __unlock(&pObj->lock);
  }
}

void taosCleanUpHashTable(void *handle) {
  HashObj *pObj = (HashObj *)handle;
  if (pObj == NULL || pObj->capacity <= 0) return;

  SHashNode *pNode, *pNext;

  if (pObj->multithreadSafe) {
    __wr_lock(&pObj->lock);
  }

  if (pObj->hashList) {
    for (int32_t i = 0; i < pObj->capacity; ++i) {
      SHashEntry *pEntry = pObj->hashList[i];
      pNode = pEntry->next;

      while (pNode) {
        pNext = pNode->next;
        free(pNode);
        pNode = pNext;
      }
      
      tfree(pEntry);
    }

    free(pObj->hashList);
  }

  if (pObj->multithreadSafe) {
    __unlock(&pObj->lock);
    __lock_destroy(&pObj->lock);
  }

  memset(pObj, 0, sizeof(HashObj));
  free(pObj);
}

// for profile only
int32_t taosGetHashMaxOverflowLength(HashObj* pObj) {
  if (pObj == NULL || pObj->size == 0) {
    return 0;
  }
  
  int32_t num = 0;
  
  for(int32_t i = 0; i < pObj->size; ++i) {
    SHashEntry *pEntry = pObj->hashList[i];
    if (num < pEntry->num) {
      num = pEntry->num;
    }
  }
  
  return num;
}
