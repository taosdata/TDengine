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
#include "taosdef.h"

#define HASH_NEED_RESIZE(_h) ((_h)->size >= (_h)->capacity * HASH_DEFAULT_LOAD_FACTOR)

#define DO_FREE_HASH_NODE(_n) \
  do {                        \
    tfree(_n);            \
  } while (0)

#define FREE_HASH_NODE(_h, _n)  \
  do {                          \
    if ((_h)->freeFp) {         \
      (_h)->freeFp(GET_HASH_NODE_DATA(_n)); \
    }                           \
                                \
    DO_FREE_HASH_NODE(_n);      \
  } while (0);

static FORCE_INLINE void __wr_lock(void *lock, int32_t type) {
  if (type == HASH_NO_LOCK) {
    return;
  }
  taosWLockLatch(lock);
}

static FORCE_INLINE void __rd_lock(void *lock, int32_t type) {
  if (type == HASH_NO_LOCK) {
    return;
  }

  taosRLockLatch(lock);
}

static FORCE_INLINE void __rd_unlock(void *lock, int32_t type) {
  if (type == HASH_NO_LOCK) {
    return;
  }

  taosRUnLockLatch(lock);
}

static FORCE_INLINE void __wr_unlock(void *lock, int32_t type) {
  if (type == HASH_NO_LOCK) {
    return;
  }

  taosWUnLockLatch(lock);
}

static FORCE_INLINE int32_t taosHashCapacity(int32_t length) {
  int32_t len = MIN(length, HASH_MAX_CAPACITY);

  int32_t i = 4;
  while (i < len) i = (i << 1u);
  return i;
}

static FORCE_INLINE SHashNode *doSearchInEntryList(SHashEntry *pe, const void *key, size_t keyLen, uint32_t hashVal) {
  SHashNode *pNode = pe->next;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(GET_HASH_NODE_KEY(pNode), key, keyLen) == 0)) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    pNode = pNode->next;
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
static FORCE_INLINE SHashNode *doUpdateHashNode(SHashEntry* pe, SHashNode* prev, SHashNode *pNode, SHashNode *pNewNode) {
  assert(pNode->keyLen == pNewNode->keyLen);
  if (prev != NULL) {
    prev->next = pNewNode;
  } else {
    pe->next = pNewNode;
  }

  pNewNode->next = pNode->next;
  return pNewNode;
}

/**
 * insert the hash node at the front of the linked list
 *
 * @param pHashObj
 * @param pNode
 */
static void pushfrontNodeInEntryList(SHashEntry *pEntry, SHashNode *pNode);

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
  pHashObj->capacity = taosHashCapacity((int32_t)capacity);
  assert((pHashObj->capacity & (pHashObj->capacity - 1)) == 0);

  pHashObj->hashFp = fn;
  pHashObj->type = type;
  pHashObj->enableUpdate = update;

  pHashObj->hashList = (SHashEntry **)calloc(pHashObj->capacity, sizeof(void *));
  if (pHashObj->hashList == NULL) {
    free(pHashObj);
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  } else {
    pHashObj->pMemBlock = taosArrayInit(8, sizeof(void *));

    void *p = calloc(pHashObj->capacity, sizeof(SHashEntry));
    for (int32_t i = 0; i < pHashObj->capacity; ++i) {
      pHashObj->hashList[i] = (void *)((char *)p + i * sizeof(SHashEntry));
    }

    taosArrayPush(pHashObj->pMemBlock, &p);
  }

  return pHashObj;
}

size_t taosHashGetSize(const SHashObj *pHashObj) { return (pHashObj == NULL) ? 0 : pHashObj->size; }

int32_t taosHashPut(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t size) {
  uint32_t   hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);
  SHashNode *pNewNode = doCreateHashNode(key, keyLen, data, size, hashVal);
  if (pNewNode == NULL) {
    return -1;
  }

  // need the resize process, write lock applied
  if (HASH_NEED_RESIZE(pHashObj)) {
    __wr_lock(&pHashObj->lock, pHashObj->type);
    taosHashTableResize(pHashObj);
    __wr_unlock(&pHashObj->lock, pHashObj->type);
  }

  __rd_lock(&pHashObj->lock, pHashObj->type);

  int32_t     slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  if (pHashObj->type == HASH_ENTRY_LOCK) {
    taosWLockLatch(&pe->latch);
  }

  SHashNode *pNode = pe->next;
  if (pe->num > 0) {
    assert(pNode != NULL);
  } else {
    assert(pNode == NULL);
  }

  SHashNode* prev = NULL;
  while (pNode) {
    if ((pNode->keyLen == keyLen) && (memcmp(GET_HASH_NODE_KEY(pNode), key, keyLen) == 0)) {
      assert(pNode->hashVal == hashVal);
      break;
    }

    prev = pNode;
    pNode = pNode->next;
  }

  if (pNode == NULL) {
    // no data in hash table with the specified key, add it into hash table
    pushfrontNodeInEntryList(pe, pNewNode);

    if (pe->num == 0) {
      assert(pe->next == NULL);
    } else {
      assert(pe->next != NULL);
    }

    if (pHashObj->type == HASH_ENTRY_LOCK) {
      taosWUnLockLatch(&pe->latch);
    }

    // enable resize
    __rd_unlock(&pHashObj->lock, pHashObj->type);
    atomic_add_fetch_64(&pHashObj->size, 1);

    return 0;
  } else {
    // not support the update operation, return error
    if (pHashObj->enableUpdate) {
      doUpdateHashNode(pe, prev, pNode, pNewNode);
      DO_FREE_HASH_NODE(pNode);
    } else {
      DO_FREE_HASH_NODE(pNewNode);
    }

    if (pHashObj->type == HASH_ENTRY_LOCK) {
      taosWUnLockLatch(&pe->latch);
    }

    // enable resize
    __rd_unlock(&pHashObj->lock, pHashObj->type);

    return pHashObj->enableUpdate ? 0 : -1;
  }
}

void *taosHashGet(SHashObj *pHashObj, const void *key, size_t keyLen) {
  return taosHashGetCB(pHashObj, key, keyLen, NULL, NULL, 0);
}

void* taosHashGetCB(SHashObj *pHashObj, const void *key, size_t keyLen, void (*fp)(void *), void* d, size_t dsize) {
  if (pHashObj->size <= 0 || keyLen == 0 || key == NULL) {
    return NULL;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  // only add the read lock to disable the resize process
  __rd_lock(&pHashObj->lock, pHashObj->type);

  int32_t     slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  // no data, return directly
  if (atomic_load_32(&pe->num) == 0) {
    __rd_unlock(&pHashObj->lock, pHashObj->type);
    return NULL;
  }

  char *data = NULL;

  // lock entry
  if (pHashObj->type == HASH_ENTRY_LOCK) {
    taosRLockLatch(&pe->latch);
  }

  if (pe->num > 0) {
    assert(pe->next != NULL);
  } else {
    assert(pe->next == NULL);
  }

  SHashNode *pNode = doSearchInEntryList(pe, key, keyLen, hashVal);
  if (pNode != NULL) {
    if (fp != NULL) {
      fp(GET_HASH_NODE_DATA(pNode));
    }

    if (d != NULL) {
      memcpy(d, GET_HASH_NODE_DATA(pNode), dsize);
    } else {
      data = GET_HASH_NODE_DATA(pNode);
    }
  }

  if (pHashObj->type == HASH_ENTRY_LOCK) {
    taosRUnLockLatch(&pe->latch);
  }

  __rd_unlock(&pHashObj->lock, pHashObj->type);
  return data;
}

int32_t taosHashRemove(SHashObj *pHashObj, const void *key, size_t keyLen) {
  return taosHashRemoveWithData(pHashObj, key, keyLen, NULL, 0);
}

int32_t taosHashRemoveWithData(SHashObj *pHashObj, const void *key, size_t keyLen, void *data, size_t dsize) {
  if (pHashObj == NULL || pHashObj->size <= 0) {
    return -1;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  // disable the resize process
  __rd_lock(&pHashObj->lock, pHashObj->type);

  int32_t     slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHashEntry *pe = pHashObj->hashList[slot];

  // no data, return directly
  if (pe->num == 0) {
    __rd_unlock(&pHashObj->lock, pHashObj->type);
    return -1;
  }

  if (pHashObj->type == HASH_ENTRY_LOCK) {
    taosWLockLatch(&pe->latch);
  }

  if (pe->num == 0) {
    assert(pe->next == NULL);
  } else {
    assert(pe->next != NULL);
  }

  // double check after locked
  if (pe->num == 0) {
    assert(pe->next == NULL);
    taosWUnLockLatch(&pe->latch);

    __rd_unlock(&pHashObj->lock, pHashObj->type);
    return -1;
  }

  SHashNode *pNode = pe->next;
  SHashNode *pRes = NULL;

  // remove it
  if ((pNode->keyLen == keyLen) && (memcmp(GET_HASH_NODE_KEY(pNode), key, keyLen) == 0)) {
    pe->num -= 1;
    pRes = pNode;
    pe->next = pNode->next;
  } else {
    while (pNode->next != NULL) {
      if (((pNode->next)->keyLen == keyLen) && (memcmp(GET_HASH_NODE_KEY((pNode->next)), key, keyLen) == 0)) {
        assert((pNode->next)->hashVal == hashVal);
        break;
      }

      pNode = pNode->next;
    }


    if (pNode->next != NULL) {
      pe->num -= 1;
      pRes = pNode->next;
      pNode->next = pNode->next->next;
    }
  }

  if (pe->num == 0) {
    assert(pe->next == NULL);
  } else {
    assert(pe->next != NULL);
  }

  if (pHashObj->type == HASH_ENTRY_LOCK) {
    taosWUnLockLatch(&pe->latch);
  }

  __rd_unlock(&pHashObj->lock, pHashObj->type);

  if (data != NULL && pRes != NULL) {
    memcpy(data, GET_HASH_NODE_DATA(pRes), dsize);
  }

  if (pRes != NULL) {
    atomic_sub_fetch_64(&pHashObj->size, 1);
    FREE_HASH_NODE(pHashObj, pRes);
    return 0;
  } else {
    return -1;
  }
}

int32_t taosHashCondTraverse(SHashObj *pHashObj, bool (*fp)(void *, void *), void *param) {
  if (pHashObj == NULL || pHashObj->size == 0) {
    return 0;
  }

  // disable the resize process
  __rd_lock(&pHashObj->lock, pHashObj->type);

  int32_t numOfEntries = (int32_t)pHashObj->capacity;
  for (int32_t i = 0; i < numOfEntries; ++i) {
    SHashEntry *pEntry = pHashObj->hashList[i];
    if (pEntry->num == 0) {
      continue;
    }

    if (pHashObj->type == HASH_ENTRY_LOCK) {
      taosWLockLatch(&pEntry->latch);
    }

    // todo remove the first node
    SHashNode *pNode = NULL;
    while((pNode = pEntry->next) != NULL) {
      if (fp && (!fp(param, GET_HASH_NODE_DATA(pNode)))) {
        pEntry->num -= 1;
        atomic_sub_fetch_64(&pHashObj->size, 1);

        pEntry->next = pNode->next;

        if (pEntry->num == 0) {
          assert(pEntry->next == NULL);
        } else {
          assert(pEntry->next != NULL);
        }

        FREE_HASH_NODE(pHashObj, pNode);
      } else {
        break;
      }
    }

    // handle the following node
    if (pNode != NULL) {
      assert(pNode == pEntry->next);
      SHashNode *pNext = NULL;

      while ((pNext = pNode->next) != NULL) {
        // not qualified, remove it
        if (fp && (!fp(param, GET_HASH_NODE_DATA(pNext)))) {
          pNode->next = pNext->next;
          pEntry->num -= 1;
          atomic_sub_fetch_64(&pHashObj->size, 1);

          if (pEntry->num == 0) {
            assert(pEntry->next == NULL);
          } else {
            assert(pEntry->next != NULL);
          }

          FREE_HASH_NODE(pHashObj, pNext);
        } else {
          pNode = pNext;
        }
      }
    }

    if (pHashObj->type == HASH_ENTRY_LOCK) {
      taosWUnLockLatch(&pEntry->latch);
    }
  }

  __rd_unlock(&pHashObj->lock, pHashObj->type);
  return 0;
}

void taosHashCleanup(SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return;
  }

  SHashNode *pNode, *pNext;

  __wr_lock(&pHashObj->lock, pHashObj->type);

  if (pHashObj->hashList) {
    for (int32_t i = 0; i < pHashObj->capacity; ++i) {
      SHashEntry *pEntry = pHashObj->hashList[i];
      if (pEntry->num == 0) {
        assert(pEntry->next == 0);
        continue;
      }

      pNode = pEntry->next;
      assert(pNode != NULL);

      while (pNode) {
        pNext = pNode->next;
        FREE_HASH_NODE(pHashObj, pNode);

        pNode = pNext;
      }
    }

    free(pHashObj->hashList);
  }

  __wr_unlock(&pHashObj->lock, pHashObj->type);

  // destroy mem block
  size_t memBlock = taosArrayGetSize(pHashObj->pMemBlock);
  for (int32_t i = 0; i < memBlock; ++i) {
    void *p = taosArrayGetP(pHashObj->pMemBlock, i);
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

  // keep it in local variable, in case the resize operation expand the size
  pIter->numOfEntries = pHashObj->capacity;
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
  if (pIter->numOfChecked == 0) {
    assert(pIter->pCur == NULL && pIter->pNext == NULL);

    while (1) {
      SHashEntry *pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
      if (pEntry->num == 0) {
        assert(pEntry->next == NULL);

        pIter->entryIndex++;
        continue;
      }

      if (pIter->pHashObj->type == HASH_ENTRY_LOCK) {
        taosRLockLatch(&pEntry->latch);
      }

      pIter->pCur = pEntry->next;

      if (pIter->pCur->next) {
        pIter->pNext = pIter->pCur->next;

        if (pIter->pHashObj->type == HASH_ENTRY_LOCK) {
          taosRUnLockLatch(&pEntry->latch);
        }
      } else {
        if (pIter->pHashObj->type == HASH_ENTRY_LOCK) {
          taosRUnLockLatch(&pEntry->latch);
        }

        pIter->pNext = getNextHashNode(pIter);
      }

      break;
    }

    pIter->numOfChecked++;
    return true;
  } else {
    assert(pIter->pCur != NULL);
    if (pIter->pNext) {
      pIter->pCur = pIter->pNext;
    } else {  // no more data in the hash list
      return false;
    }

    pIter->numOfChecked++;

    if (pIter->pCur->next) {
      pIter->pNext = pIter->pCur->next;
    } else {
      pIter->pNext = getNextHashNode(pIter);
    }

    return true;
  }
}

void *taosHashIterGet(SHashMutableIterator *iter) { return (iter == NULL) ? NULL : GET_HASH_NODE_DATA(iter->pCur); }

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

  int32_t newSize = (int32_t)(pHashObj->capacity << 1u);
  if (newSize > HASH_MAX_CAPACITY) {
    //    uDebug("current capacity:%d, maximum capacity:%d, no resize applied due to limitation is reached",
    //           pHashObj->capacity, HASH_MAX_CAPACITY);
    return;
  }

  int64_t st = taosGetTimestampUs();
  void *pNewEntryList = realloc(pHashObj->hashList, sizeof(void *) * newSize);
  if (pNewEntryList == NULL) {  // todo handle error
    //    uDebug("cache resize failed due to out of memory, capacity remain:%d", pHashObj->capacity);
    return;
  }

  pHashObj->hashList = pNewEntryList;

  size_t inc = newSize - pHashObj->capacity;
  void * p = calloc(inc, sizeof(SHashEntry));

  for (int32_t i = 0; i < inc; ++i) {
    pHashObj->hashList[i + pHashObj->capacity] = (void *)((char *)p + i * sizeof(SHashEntry));
  }

  taosArrayPush(pHashObj->pMemBlock, &p);

  pHashObj->capacity = newSize;
  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    SHashEntry *pe = pHashObj->hashList[i];

    if (pe->num == 0) {
      assert(pe->next == NULL);
    } else {
      assert(pe->next != NULL);
    }

    if (pe->num == 0) {
      assert(pe->next == NULL);
      continue;
    }

    while ((pNode = pe->next) != NULL) {
      int32_t j = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
      if (j != i) {
        pe->num -= 1;
        pe->next = pNode->next;

        if (pe->num == 0) {
          assert(pe->next == NULL);
        } else {
          assert(pe->next != NULL);
        }

        SHashEntry *pNewEntry = pHashObj->hashList[j];
        pushfrontNodeInEntryList(pNewEntry, pNode);
      } else {
        break;
      }
    }

    if (pNode != NULL) {
      while ((pNext = pNode->next) != NULL) {
        int32_t j = HASH_INDEX(pNext->hashVal, pHashObj->capacity);
        if (j != i) {
          pe->num -= 1;

          pNode->next = pNext->next;
          pNext->next = NULL;

          // added into new slot
          SHashEntry *pNewEntry = pHashObj->hashList[j];

          if (pNewEntry->num == 0) {
            assert(pNewEntry->next == NULL);
          } else {
            assert(pNewEntry->next != NULL);
          }

          pushfrontNodeInEntryList(pNewEntry, pNext);
        } else {
          pNode = pNext;
        }
      }

      if (pe->num == 0) {
        assert(pe->next == NULL);
      } else {
        assert(pe->next != NULL);
      }

    }

  }

  int64_t et = taosGetTimestampUs();

  uDebug("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", (int32_t)pHashObj->capacity,
           ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

SHashNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal) {
  SHashNode *pNewNode = calloc(1, sizeof(SHashNode) + keyLen + dsize);

  if (pNewNode == NULL) {
    uError("failed to allocate memory, reason:%s", strerror(errno));
    return NULL;
  }

  pNewNode->keyLen = (uint32_t)keyLen;
  pNewNode->hashVal = hashVal;

  memcpy(GET_HASH_NODE_DATA(pNewNode), pData, dsize);
  memcpy(GET_HASH_NODE_KEY(pNewNode), key, keyLen);

  return pNewNode;
}

void pushfrontNodeInEntryList(SHashEntry *pEntry, SHashNode *pNode) {
  assert(pNode != NULL && pEntry != NULL);

  pNode->next = pEntry->next;
  pEntry->next = pNode;

  pEntry->num += 1;
}

SHashNode *getNextHashNode(SHashMutableIterator *pIter) {
  assert(pIter != NULL);

  pIter->entryIndex++;
  SHashNode *p = NULL;

  while (pIter->entryIndex < pIter->numOfEntries) {
    SHashEntry *pEntry = pIter->pHashObj->hashList[pIter->entryIndex];
    if (pEntry->num == 0) {
      pIter->entryIndex++;
      continue;
    }

    if (pIter->pHashObj->type == HASH_ENTRY_LOCK) {
      taosRLockLatch(&pEntry->latch);
    }

    p = pEntry->next;

    if (pIter->pHashObj->type == HASH_ENTRY_LOCK) {
      taosRUnLockLatch(&pEntry->latch);
    }

    return p;
  }

  return NULL;
}

size_t taosHashGetMemSize(const SHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }

  return (pHashObj->capacity * (sizeof(SHashEntry) + POINTER_BYTES)) + sizeof(SHashNode) * taosHashGetSize(pHashObj) + sizeof(SHashObj);
}
