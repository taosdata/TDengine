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

#define HASH_NEED_RESIZE(_h)  ((_h)->size >= (_h)->capacity * HASH_DEFAULT_LOAD_FACTOR)

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
FORCE_INLINE SHashNode *doGetNodeFromHashTable(SHashObj *pHashObj, const void *key, uint32_t keyLen, uint32_t hashVal) {
  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);

  SHashEntry *pe = pHashObj->hashList[slot];

  // no data, return directly
  int32_t num = atomic_load_32(&pe->num);
  if (num == 0) {
    return NULL;
  }

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    taosRLockLatch(&pe->latch);
  }

  SHashNode* pNode = pe->head.next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(pNode->key, key, keyLen) == 0)) {
      assert(pNode->hashVal == hashVal);
      break;
    }
    
    pNode = pNode->next;
  }

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    taosRUnLockLatch(&pe->latch);
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
static FORCE_INLINE SHashNode *doUpdateHashNode(SHashNode *pNode, SHashNode *pNewNode) {
  assert(pNode->keyLen == pNewNode->keyLen);
  SWAP(pNode->key, pNewNode->key, void*);
  SWAP(pNode->data, pNewNode->data, void*);

  return pNewNode;
}

/**
 * insert the hash node at the front of the linked list
 *
 * @param pHashObj
 * @param pNode
 */
static void pushfrontNode(SHashEntry* pEntry, SHashNode *pNode);

/**
 * Get the next element in hash table for iterator
 * @param pIter
 * @return
 */
static SHashNode *getNextHashNode(SHashMutableIterator *pIter);

SHashObj *taosHashInit(size_t capacity, _hash_fn_t fn, bool update, SHashLockTypeE type) {
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
  pHashObj->lockType = type;
  pHashObj->enableUpdate = update;

  pHashObj->hashList = (SHashEntry **)calloc(pHashObj->capacity, sizeof(void*));
  if (pHashObj->hashList == NULL) {
    free(pHashObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  } else {

    pHashObj->pMemBlock = taosArrayInit(8, sizeof(void*));

    void* p = calloc(pHashObj->capacity, sizeof(SHashEntry));
    for(int32_t i = 0; i < pHashObj->capacity; ++i) {
      pHashObj->hashList[i] = p + i * sizeof(SHashEntry);
    }

    taosArrayPush(pHashObj->pMemBlock, &p);
  }

  if (pHashObj->lockType != HASH_NO_LOCK) {
#if defined(LINUX)
    pHashObj->lock.lock = calloc(1, sizeof(pthread_rwlock_t));
#else
    pHashObj->lock.lock = calloc(1, sizeof(pthread_mutex_t));
#endif
  }

  if (__lock_init(pHashObj->lock.lock) != 0) {
    free(pHashObj->hashList);
    free(pHashObj);

    uError("failed to init lock, reason:%s", strerror(errno));
    return NULL;
  }

  return pHashObj;
}

size_t taosHashGetSize(const SHashObj *pHashObj) {
  return (pHashObj == NULL)? 0:pHashObj->size;
}

int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t size) {
  uint32_t   hashVal = (*pHashObj->hashFp)(key, keyLen);
  SHashNode *pNewNode = doCreateHashNode(key, keyLen, data, size, hashVal);
  if (pNewNode == NULL) {
    return -1;
  }

  // need the resize process, write lock applied
  if (HASH_NEED_RESIZE(pHashObj)) {
    __wr_lock(pHashObj->lock.lock);
    taosHashTableResize(pHashObj);
    __unlock(pHashObj->lock.lock);
  }

  __rd_lock(pHashObj->lock.lock);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    taosWLockLatch(&pe->latch);
  }

  SHashNode* pNode = pe->head.next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(pNode->key, key, keyLen) == 0)) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    pNode = pNode->next;
  }

  if (pNode == NULL) {
    // no data in hash table with the specified key, add it into hash table
    pushfrontNode(pe, pNewNode);

    if (pHashObj->lockType == HASH_ENTRY_LOCK) {
      taosWUnLockLatch(&pe->latch);
    }

    // enable resize
    __unlock(pHashObj->lock.lock);
    atomic_add_fetch_64(&pHashObj->size, 1);

    return 0;
  } else {
    // not support the update operation, return error
    if (pHashObj->enableUpdate) {
      doUpdateHashNode(pNode, pNewNode);
    }

    if (pHashObj->lockType == HASH_ENTRY_LOCK) {
      taosWUnLockLatch(&pe->latch);
    }

    // enable resize
    __unlock(pHashObj->lock.lock);

    tfree(pNewNode->data)
    tfree(pNewNode);

    return pHashObj->enableUpdate? 0:-1;
  }
}

void *taosHashGet(SHashObj *pHashObj, const void *key, size_t keyLen) {
  if (pHashObj->size <= 0 || keyLen == 0 || key == NULL) {
    return NULL;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, keyLen);

  // only add the read lock to disable the resize process
  __rd_lock(pHashObj->lock.lock);

  SHashNode *pNode = doGetNodeFromHashTable(pHashObj, key, keyLen, hashVal);

  __unlock(pHashObj->lock.lock);

  if (pNode) {
    assert(pNode->hashVal == hashVal);
    return pNode->data;
  } else {
    return NULL;
  }
}

int32_t taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen) {
  if (pHashObj->size <= 0) {
    return -1;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, keyLen);

  // disable the resize process
  __rd_lock(pHashObj->lock.lock);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  // no data, return directly
  if (pe->num == 0) {
    __unlock(pHashObj->lock.lock);
    return -1;
  }

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    taosWLockLatch(&pe->latch);
  }

  SHashNode* pNode = pe->head.next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(pNode->key, key, keyLen) == 0)) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    pNode = pNode->next;
  }

  if (pNode != NULL) {
    assert(pNode->prev != NULL);

    SHashNode *pNext = pNode->next;
    pNode->prev->next = pNext;

    if (pNext != NULL) {
      pNext->prev = pNode->prev;
    }

    pe->num -= 1;
  }

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    taosWUnLockLatch(&pe->latch);
  }

  __unlock(pHashObj->lock.lock);

  if (pNode != NULL) {
    atomic_sub_fetch_64(&pHashObj->size, 1);

    pNode->next = NULL;
    pNode->prev = NULL;

    tfree(pNode->data);
    tfree(pNode);

    return 0;
  } else {
    return -1;
  }
}

int32_t taosHashRemoveNode(SHashObj *pHashObj, const void *key, size_t keyLen, void* data, size_t dsize) {
  if (pHashObj->size <= 0) {
    return -1;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, keyLen);

  // disable the resize process
  __rd_lock(pHashObj->lock.lock);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  // no data, return directly
  if (pe->num == 0) {
    __unlock(pHashObj->lock.lock);
    return -1;
  }

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    taosWLockLatch(&pe->latch);
  }

  SHashNode* pNode = pe->head.next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(pNode->key, key, keyLen) == 0)) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    pNode = pNode->next;
  }

  if (pNode != NULL) {
    assert(pNode->prev != NULL);

    SHashNode *pNext = pNode->next;
    pNode->prev->next = pNext;

    if (pNext != NULL) {
      pNext->prev = pNode->prev;
    }
  }

  if (pHashObj->lockType == HASH_ENTRY_LOCK) {
    pe->num -= 1;
    taosWUnLockLatch(&pe->latch);
  }

  __unlock(pHashObj->lock.lock);

  atomic_sub_fetch_64(&pHashObj->size, 1);

  if (data != NULL) {
    memcpy(data, pNode->data, dsize);
  }

  if (pNode != NULL) {
    pNode->next = NULL;
    pNode->prev = NULL;

    tfree(pNode->data);
    tfree(pNode);

    return 0;
  } else {
    return -1;
  }
}

void taosHashCleanup(SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return;
  }

  SHashNode *pNode, *pNext;

  __wr_lock(pHashObj->lock.lock);

  if (pHashObj->hashList) {
    for (int32_t i = 0; i < pHashObj->capacity; ++i) {
      SHashEntry* pEntry = pHashObj->hashList[i];
      if (pEntry->num == 0) {
        assert(pEntry->head.next == 0);
        continue;
      }

      pNode = pEntry->head.next;
      while (pNode) {
        pNext = pNode->next;
        if (pHashObj->freeFp) {
          pHashObj->freeFp(pNode->data);
        }

        free(pNode->data);
        free(pNode);
        pNode = pNext;
      }
    }

    free(pHashObj->hashList);
  }

  __unlock(pHashObj->lock.lock);
  __lock_destroy(pHashObj->lock.lock);

  tfree(pHashObj->lock.lock);

  // destroy mem block
  size_t memBlock = taosArrayGetSize(pHashObj->pMemBlock);
  for(int32_t i = 0; i < memBlock; ++i) {
    void* p = taosArrayGetP(pHashObj->pMemBlock, i);
    tfree(p);
  }

  taosArrayDestroy(pHashObj->pMemBlock);

  memset(pHashObj, 0, sizeof(SHashObj));
  free(pHashObj);
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
      SHashEntry *pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
      if (pEntry->num == 0) {
        pIter->entryIndex++;
        continue;
      }

      pIter->pCur = pEntry->head.next;

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

void taosHashTableResize(SHashObj *pHashObj) {
  if (!HASH_NEED_RESIZE(pHashObj)) {
    return;
  }
  
  // double the original capacity
  SHashNode *pNode = NULL;
  SHashNode *pNext = NULL;
  
  int32_t newSize = pHashObj->capacity << 1u;
  if (newSize > HASH_MAX_CAPACITY) {
//    uDebug("current capacity:%d, maximum capacity:%d, no resize applied due to limitation is reached",
//           pHashObj->capacity, HASH_MAX_CAPACITY);
    return;
  }

  void *pNewEntryList = realloc(pHashObj->hashList, sizeof(SHashEntry) * newSize);
  if (pNewEntryList == NULL) {// todo handle error
//    uDebug("cache resize failed due to out of memory, capacity remain:%d", pHashObj->capacity);
    return;
  }
  
  pHashObj->hashList = pNewEntryList;

  size_t inc = newSize - pHashObj->capacity;
  void* p = calloc(inc, sizeof(SHashEntry));

  for(int32_t i = 0; i < inc; ++i) {
    pHashObj->hashList[i + pHashObj->capacity] = p + i * sizeof(SHashEntry);
  }

  taosArrayPush(pHashObj->pMemBlock, &p);

  pHashObj->capacity = newSize;
  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    SHashEntry* pe = pHashObj->hashList[i];
    if (pe->num == 0) {
      assert(pe->head.next == NULL);
      continue;
    }

    pNode = pe->head.next;
    while (pNode) {
      int32_t j = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
      if (j == i) {  // this key locates in the same slot, no need to relocate it
        pNode = pNode->next;
        assert(pNode == NULL || pNode->next != pNode);
      } else {
        pNext = pNode->next;
        assert(pNode != pNext && (pNext == NULL || pNext->prev == pNode) && pNode->prev->next == pNode);

        assert(pNode->prev != NULL);
        pNode->prev->next = pNext;
        if (pNext != NULL) {
          pNext->prev = pNode->prev;
        }
        
        // clear pointer
        pNode->next = NULL;
        pNode->prev = NULL;
        pe->num -= 1;
        
        // added into new slot
        SHashEntry *pNewEntry = pHashObj->hashList[j];
        pushfrontNode(pNewEntry, pNode);

        // continue
        pNode = pNext;
      }
    }
  }

//  uDebug("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", pHashObj->capacity,
//         ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

SHashNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal) {
  SHashNode *pNewNode = calloc(1, sizeof(SHashNode));
  if (pNewNode == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pNewNode->data = malloc(dsize + keyLen);
  memcpy(pNewNode->data, pData, dsize);
  
  pNewNode->key = pNewNode->data + dsize;
  memcpy(pNewNode->key, key, keyLen);

  pNewNode->keyLen = keyLen;
  pNewNode->hashVal = hashVal;
  return pNewNode;
}

void pushfrontNode(SHashEntry* pEntry, SHashNode *pNode) {
  assert(pNode != NULL && pEntry != NULL);
  
  SHashNode* pNext = pEntry->head.next;
  if (pNext != NULL) {
    pNext->prev = pNode;
  }

  pNode->next = pNext;
  pNode->prev = &pEntry->head;
  pEntry->head.next = pNode;

  pEntry->num += 1;
}

SHashNode *getNextHashNode(SHashMutableIterator *pIter) {
  assert(pIter != NULL);
  
  pIter->entryIndex++;
  while (pIter->entryIndex < pIter->pHashObj->capacity) {
    SHashEntry*pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
    if (pEntry->num == 0) {
      pIter->entryIndex++;
      continue;
    }
    
    return pEntry->head.next;
  }
  
  return NULL;
}
