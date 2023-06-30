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

#include "tsimplehash.h"
#include "taoserror.h"
#include "tlog.h"
#include "tdef.h"

#define DEFAULT_BUF_PAGE_SIZE     1024
#define SHASH_DEFAULT_LOAD_FACTOR 0.75
#define HASH_MAX_CAPACITY         (1024 * 1024 * 16L)
#define SHASH_NEED_RESIZE(_h)     ((_h)->size >= (_h)->capacity * SHASH_DEFAULT_LOAD_FACTOR)

#define GET_SHASH_NODE_DATA(_n)     (((SHNode*)_n)->data)
#define GET_SHASH_NODE_KEY(_n, _dl) ((char*)GET_SHASH_NODE_DATA(_n) + (_dl))

#define HASH_INDEX(v, c) ((v) & ((c)-1))

#define FREE_HASH_NODE(_n, fp) \
  do {                         \
    if (fp) {                  \
      fp((_n)->data);          \
    }                          \
    taosMemoryFreeClear(_n);   \
  } while (0);

struct SSHashObj {
  SHNode        **hashList;
  size_t          capacity;  // number of slots
  int64_t         size;      // number of elements in hash table
  _hash_fn_t      hashFp;    // hash function
  _equal_fn_t     equalFp;   // equal function
  _hash_free_fn_t freeFp;    // free function
  SArray         *pHashNodeBuf;  // hash node allocation buffer, 1k size of each page by default
  int32_t         offset;        // allocation offset in current page
};

static FORCE_INLINE int32_t taosHashCapacity(int32_t length) {
  int32_t len = (length < HASH_MAX_CAPACITY ? length : HASH_MAX_CAPACITY);

  int32_t i = 4;
  while (i < len) i = (i << 1u);
  return i;
}

SSHashObj *tSimpleHashInit(size_t capacity, _hash_fn_t fn) {
  if (fn == NULL) {
    return NULL;
  }

  if (capacity == 0) {
    capacity = 4;
  }

  SSHashObj *pHashObj = (SSHashObj *)taosMemoryMalloc(sizeof(SSHashObj));
  if (!pHashObj) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  // the max slots is not defined by user
  pHashObj->hashFp = fn;
  pHashObj->capacity = taosHashCapacity((int32_t)capacity);
  pHashObj->equalFp = memcmp;

  pHashObj->freeFp = NULL;
  pHashObj->offset = 0;
  pHashObj->size = 0;
  
  pHashObj->hashList = (SHNode **)taosMemoryCalloc(pHashObj->capacity, sizeof(void *));
  if (!pHashObj->hashList) {
    taosMemoryFree(pHashObj);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return pHashObj;
}

int32_t tSimpleHashGetSize(const SSHashObj *pHashObj) {
  if (!pHashObj) {
    return 0;
  }
  return (int32_t) pHashObj->size;
}

void tSimpleHashSetFreeFp(SSHashObj* pHashObj, _hash_free_fn_t freeFp) {
  pHashObj->freeFp = freeFp;
}

static void* doInternalAlloc(SSHashObj* pHashObj, int32_t size) {
#if 0
  void** p = taosArrayGetLast(pHashObj->pHashNodeBuf);
  if (p == NULL || (pHashObj->offset + size) > DEFAULT_BUF_PAGE_SIZE) {
    // let's allocate one new page
    int32_t allocSize = TMAX(size, DEFAULT_BUF_PAGE_SIZE);
    void* pNewPage = taosMemoryMalloc(allocSize);
    if (pNewPage == NULL) {
      return NULL;
    }

    // if the allocate the buffer page is greater than the DFFAULT_BUF_PAGE_SIZE,
    // pHashObj->offset will always be greater than DEFAULT_BUF_PAGE_SIZE, which means that
    // current buffer page is full. And a new buffer page needs to be allocated.
    pHashObj->offset = size;
    taosArrayPush(pHashObj->pHashNodeBuf, &pNewPage);
    return pNewPage;
  } else {
    void* pPos = (char*)(*p) + pHashObj->offset;
    pHashObj->offset += size;
    return pPos;
  }
#else
  return taosMemoryMalloc(size);
#endif
}

static SHNode *doCreateHashNode(SSHashObj *pHashObj, const void *key, size_t keyLen, const void *data, size_t dataLen,
                                uint32_t hashVal) {
  SHNode *pNewNode = doInternalAlloc(pHashObj, sizeof(SHNode) + keyLen + dataLen);
  if (!pNewNode) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pNewNode->keyLen = keyLen;
  pNewNode->dataLen = dataLen;
  pNewNode->next = NULL;
  pNewNode->hashVal = hashVal;

  if (data) {
    memcpy(GET_SHASH_NODE_DATA(pNewNode), data, dataLen);
  }

  memcpy(GET_SHASH_NODE_KEY(pNewNode, dataLen), key, keyLen);
  return pNewNode;
}

static void tSimpleHashTableResize(SSHashObj *pHashObj) {
  if (!SHASH_NEED_RESIZE(pHashObj)) {
    return;
  }

  int32_t newCapacity = (int32_t)(pHashObj->capacity << 1u);
  if (newCapacity > HASH_MAX_CAPACITY) {
    uDebug("current capacity:%" PRIzu ", maximum capacity:%" PRId32 ", no resize applied due to limitation is reached",
           pHashObj->capacity, (int32_t)HASH_MAX_CAPACITY);
    return;
  }

//  int64_t st = taosGetTimestampUs();
  void   *pNewEntryList = taosMemoryRealloc(pHashObj->hashList, POINTER_BYTES * newCapacity);
  if (!pNewEntryList) {
    uWarn("hash resize failed due to out of memory, capacity remain:%zu", pHashObj->capacity);
    return;
  }

  size_t inc = newCapacity - pHashObj->capacity;
  memset((char *)pNewEntryList + pHashObj->capacity * POINTER_BYTES, 0, inc * sizeof(void *));

  pHashObj->hashList = pNewEntryList;
  pHashObj->capacity = newCapacity;

  for (int32_t idx = 0; idx < pHashObj->capacity; ++idx) {
    SHNode *pNode = pHashObj->hashList[idx];
    if (!pNode) {
      continue;
    }

    SHNode *pNext = NULL;
    SHNode *pPrev = NULL;

    while (pNode != NULL) {
      int32_t newIdx = HASH_INDEX(pNode->hashVal, pHashObj->capacity);
      pNext = pNode->next;
      if (newIdx != idx) {
        if (!pPrev) {
          pHashObj->hashList[idx] = pNext;
        } else {
          pPrev->next = pNext;
        }

        pNode->next = pHashObj->hashList[newIdx];
        pHashObj->hashList[newIdx] = pNode;
      } else {
        pPrev = pNode;
      }

      pNode = pNext;
    }
  }

//  int64_t et = taosGetTimestampUs();
  //  uDebug("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms",
  //  (int32_t)pHashObj->capacity,
  //         ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

int32_t tSimpleHashPut(SSHashObj *pHashObj, const void *key, size_t keyLen, const void *data, size_t dataLen) {
  if (!pHashObj || !key) {
    return -1;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  // need the resize process, write lock applied
  if (SHASH_NEED_RESIZE(pHashObj)) {
    tSimpleHashTableResize(pHashObj);
  }

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);

  SHNode *pNode = pHashObj->hashList[slot];
  if (!pNode) {
    SHNode *pNewNode = doCreateHashNode(pHashObj, key, keyLen, data, dataLen, hashVal);
    if (!pNewNode) {
      return -1;
    }

    pHashObj->hashList[slot] = pNewNode;
    pHashObj->size += 1;
    return 0;
  }

  while (pNode) {
    if ((keyLen == pNode->keyLen) && (*(pHashObj->equalFp))(GET_SHASH_NODE_KEY(pNode, pNode->dataLen), key, keyLen) == 0) {
      break;
    }
    pNode = pNode->next;
  }

  if (!pNode) {
    SHNode *pNewNode = doCreateHashNode(pHashObj, key, keyLen, data, dataLen, hashVal);
    if (!pNewNode) {
      return -1;
    }
    pNewNode->next = pHashObj->hashList[slot];
    pHashObj->hashList[slot] = pNewNode;
    pHashObj->size += 1;
  } else if (data) {  // update data
    memcpy(GET_SHASH_NODE_DATA(pNode), data, dataLen);
  }

  return 0;
}

static FORCE_INLINE SHNode *doSearchInEntryList(SSHashObj *pHashObj, const void *key, size_t keyLen, int32_t index) {
  SHNode *pNode = pHashObj->hashList[index];
  while (pNode) {
    const char* p = GET_SHASH_NODE_KEY(pNode, pNode->dataLen);
    ASSERT(keyLen > 0);

    if (pNode->keyLen == keyLen && ((*(pHashObj->equalFp))(p, key, keyLen) == 0)) {
      break;
    }
    pNode = pNode->next;
  }

  return pNode;
}

static FORCE_INLINE bool taosHashTableEmpty(const SSHashObj *pHashObj) { return tSimpleHashGetSize(pHashObj) == 0; }

void *tSimpleHashGet(SSHashObj *pHashObj, const void *key, size_t keyLen) {
  if (!pHashObj || taosHashTableEmpty(pHashObj) || !key) {
    return NULL;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHNode *pNode = pHashObj->hashList[slot];
  if (!pNode) {
    return NULL;
  }

  char *data = NULL;
  pNode = doSearchInEntryList(pHashObj, key, keyLen, slot);
  if (pNode != NULL) {
    data = GET_SHASH_NODE_DATA(pNode);
  }

  return data;
}

int32_t tSimpleHashRemove(SSHashObj *pHashObj, const void *key, size_t keyLen) {
  int32_t code = TSDB_CODE_FAILED;
  if (!pHashObj || !key) {
    return code;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);

  SHNode *pNode = pHashObj->hashList[slot];
  SHNode *pPrev = NULL;
  while (pNode) {
    if ((*(pHashObj->equalFp))(GET_SHASH_NODE_KEY(pNode, pNode->dataLen), key, keyLen) == 0) {
      if (!pPrev) {
        pHashObj->hashList[slot] = pNode->next;
      } else {
        pPrev->next = pNode->next;
      }

      FREE_HASH_NODE(pNode, pHashObj->freeFp);
      pHashObj->size -= 1;
      code = TSDB_CODE_SUCCESS;
      break;
    }
    pPrev = pNode;
    pNode = pNode->next;
  }

  return code;
}

int32_t tSimpleHashIterateRemove(SSHashObj *pHashObj, const void *key, size_t keyLen, void **pIter, int32_t *iter) {
  if (!pHashObj || !key) {
    return TSDB_CODE_FAILED;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)keyLen);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);

  SHNode *pNode = pHashObj->hashList[slot];
  SHNode *pPrev = NULL;
  while (pNode) {
    if ((*(pHashObj->equalFp))(GET_SHASH_NODE_KEY(pNode, pNode->dataLen), key, keyLen) == 0) {
      if (!pPrev) {
        pHashObj->hashList[slot] = pNode->next;
      } else {
        pPrev->next = pNode->next;
      }

      if (*pIter == (void *)GET_SHASH_NODE_DATA(pNode)) {
        *pIter = pPrev ? GET_SHASH_NODE_DATA(pPrev) : NULL;
      }

      FREE_HASH_NODE(pNode, pHashObj->freeFp);
      pHashObj->size -= 1;
      break;
    }
    pPrev = pNode;
    pNode = pNode->next;
  }

  return TSDB_CODE_SUCCESS;
}

void tSimpleHashClear(SSHashObj *pHashObj) {
  if (!pHashObj || taosHashTableEmpty(pHashObj)) {
    return;
  }

  SHNode *pNode = NULL, *pNext = NULL;
  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    pNode = pHashObj->hashList[i];
    if (!pNode) {
      continue;
    }

    while (pNode) {
      pNext = pNode->next;
      FREE_HASH_NODE(pNode, pHashObj->freeFp);
      pNode = pNext;
    }

    pHashObj->hashList[i] = NULL;
  }

  pHashObj->offset = 0;
  pHashObj->size = 0;
}

void tSimpleHashCleanup(SSHashObj *pHashObj) {
  if (!pHashObj) {
    return;
  }

  tSimpleHashClear(pHashObj);
  taosMemoryFreeClear(pHashObj->hashList);
  taosMemoryFree(pHashObj);
}

size_t tSimpleHashGetMemSize(const SSHashObj *pHashObj) {
  if (!pHashObj) {
    return 0;
  }

  return (pHashObj->capacity * sizeof(void *)) + sizeof(SHNode) * tSimpleHashGetSize(pHashObj) + sizeof(SSHashObj);
}

void *tSimpleHashIterate(const SSHashObj *pHashObj, void *data, int32_t *iter) {
  if (!pHashObj) {
    return NULL;
  }

  SHNode *pNode = NULL;

  if (!data) {
    for (int32_t i = *iter; i < pHashObj->capacity; ++i) {
      pNode = pHashObj->hashList[i];
      if (!pNode) {
        continue;
      }
      *iter = i;
      return GET_SHASH_NODE_DATA(pNode);
    }
    return NULL;
  }

  pNode = (SHNode *)((char *)data - offsetof(SHNode, data));

  if (pNode->next) {
    return GET_SHASH_NODE_DATA(pNode->next);
  }

  ++(*iter);
  for (int32_t i = *iter; i < pHashObj->capacity; ++i) {
    pNode = pHashObj->hashList[i];
    if (!pNode) {
      continue;
    }
    *iter = i;
    return GET_SHASH_NODE_DATA(pNode);
  }

  return NULL;
}
