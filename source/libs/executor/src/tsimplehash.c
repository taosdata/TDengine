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
#include "tsimplehash.h"
#include "taoserror.h"

#define SHASH_DEFAULT_LOAD_FACTOR 0.75
#define HASH_MAX_CAPACITY         (1024*1024*16)
#define SHASH_NEED_RESIZE(_h)     ((_h)->size >= (_h)->capacity * SHASH_DEFAULT_LOAD_FACTOR)

#define GET_SHASH_NODE_KEY(_n, _dl)  ((char*)(_n) + sizeof(SHNode) + (_dl))
#define GET_SHASH_NODE_DATA(_n)      ((char*)(_n) + sizeof(SHNode))

#define HASH_INDEX(v, c)         ((v) & ((c)-1))
#define HASH_NEED_RESIZE(_h)     ((_h)->size >= (_h)->capacity * SHASH_DEFAULT_LOAD_FACTOR)

#define FREE_HASH_NODE(_n) \
  do {                     \
    taosMemoryFreeClear(_n);             \
  } while (0);

typedef struct SHNode {
  struct SHNode   *next;
  char             data[];
} SHNode;

typedef struct SSHashObj {
  SHNode         **hashList;
  size_t           capacity;     // number of slots
  int64_t          size;         // number of elements in hash table
  _hash_fn_t       hashFp;       // hash function
  _equal_fn_t      equalFp;      // equal function
  int32_t          keyLen;
  int32_t          dataLen;
} SSHashObj;

static FORCE_INLINE int32_t taosHashCapacity(int32_t length) {
  int32_t len = MIN(length, HASH_MAX_CAPACITY);

  int32_t i = 4;
  while (i < len) i = (i << 1u);
  return i;
}

SSHashObj *tSimpleHashInit(size_t capacity, _hash_fn_t fn, size_t keyLen, size_t dataLen) {
  ASSERT(fn != NULL);

  if (capacity == 0) {
    capacity = 4;
  }

  SSHashObj* pHashObj = (SSHashObj*) taosMemoryCalloc(1, sizeof(SSHashObj));
  if (pHashObj == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  // the max slots is not defined by user
  pHashObj->capacity = taosHashCapacity((int32_t)capacity);

  pHashObj->equalFp = memcmp;
  pHashObj->hashFp  = fn;
  ASSERT((pHashObj->capacity & (pHashObj->capacity - 1)) == 0);

  pHashObj->keyLen = keyLen;
  pHashObj->dataLen = dataLen;

  pHashObj->hashList = (SHNode **)taosMemoryCalloc(pHashObj->capacity, sizeof(void *));
  if (pHashObj->hashList == NULL) {
    taosMemoryFree(pHashObj);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  return pHashObj;
}

int32_t tSimpleHashGetSize(const SSHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }
  return (int32_t)atomic_load_64((int64_t*)&pHashObj->size);
}

static SHNode *doCreateHashNode(const void *key, size_t keyLen, const void *pData, size_t dsize, uint32_t hashVal) {
  SHNode *pNewNode = taosMemoryMalloc(sizeof(SHNode) + keyLen + dsize);
  if (pNewNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pNewNode->next = NULL;
  memcpy(GET_SHASH_NODE_DATA(pNewNode), pData, dsize);
  memcpy(GET_SHASH_NODE_KEY(pNewNode, dsize), key, keyLen);
  return pNewNode;
}

void taosHashTableResize(SSHashObj *pHashObj) {
  if (!HASH_NEED_RESIZE(pHashObj)) {
    return;
  }

  int32_t newCapacity = (int32_t)(pHashObj->capacity << 1u);
  if (newCapacity > HASH_MAX_CAPACITY) {
//    uDebug("current capacity:%zu, maximum capacity:%d, no resize applied due to limitation is reached",
//           pHashObj->capacity, HASH_MAX_CAPACITY);
    return;
  }

  int64_t st = taosGetTimestampUs();
  void *pNewEntryList = taosMemoryRealloc(pHashObj->hashList, sizeof(void *) * newCapacity);
  if (pNewEntryList == NULL) {
//    qWarn("hash resize failed due to out of memory, capacity remain:%zu", pHashObj->capacity);
    return;
  }

  size_t inc = newCapacity - pHashObj->capacity;
  memset(pNewEntryList + pHashObj->capacity * sizeof(void*), 0, inc);

  pHashObj->hashList = pNewEntryList;
  pHashObj->capacity = newCapacity;

  for (int32_t idx = 0; idx < pHashObj->capacity; ++idx) {
    SHNode* pNode = pHashObj->hashList[idx];
    SHNode *pNext;
    SHNode *pPrev = NULL;

    if (pNode == NULL) {
      continue;
    }

    while (pNode != NULL) {
      void* key = GET_SHASH_NODE_KEY(pNode, pHashObj->dataLen);
      uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)pHashObj->dataLen);

      int32_t newIdx = HASH_INDEX(hashVal, pHashObj->capacity);
      pNext = pNode->next;
      if (newIdx != idx) {
        if (pPrev == NULL) {
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

  int64_t et = taosGetTimestampUs();

//  uDebug("hash table resize completed, new capacity:%d, load factor:%f, elapsed time:%fms", (int32_t)pHashObj->capacity,
//         ((double)pHashObj->size) / pHashObj->capacity, (et - st) / 1000.0);
}

int32_t tSimpleHashPut(SSHashObj *pHashObj, const void *key, const void *data) {
  if (pHashObj == NULL || key == NULL) {
    return -1;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)pHashObj->keyLen);

  // need the resize process, write lock applied
  if (SHASH_NEED_RESIZE(pHashObj)) {
    taosHashTableResize(pHashObj);
  }

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);

  SHNode *pNode = pHashObj->hashList[slot];
  if (pNode == NULL) {
    SHNode *pNewNode = doCreateHashNode(key, pHashObj->keyLen, data, pHashObj->size, hashVal);
    if (pNewNode == NULL) {
      return -1;
    }

    pHashObj->hashList[slot] = pNewNode;
    return 0;
  }

  while (pNode) {
    if ((*(pHashObj->equalFp))(GET_SHASH_NODE_KEY(pNode, pHashObj->dataLen), key, pHashObj->keyLen) == 0) {
      break;
    }
    pNode = pNode->next;
  }

  if (pNode == NULL) {
    SHNode *pNewNode = doCreateHashNode(key, pHashObj->keyLen, data, pHashObj->size, hashVal);
    if (pNewNode == NULL) {
      return -1;
    }
    pNewNode->next = pHashObj->hashList[slot];
    pHashObj->hashList[slot] = pNewNode;
    atomic_add_fetch_64(&pHashObj->size, 1);
  } else {  //update data
    memcpy(GET_SHASH_NODE_DATA(pNode), data, pHashObj->dataLen);
  }

  return 0;
}

static FORCE_INLINE SHNode *doSearchInEntryList(SSHashObj *pHashObj, const void *key, int32_t index) {
  SHNode *pNode = pHashObj->hashList[index];
  while (pNode) {
    if ((*(pHashObj->equalFp))(GET_SHASH_NODE_KEY(pNode, pHashObj->dataLen), key, pHashObj->keyLen) == 0) {
      break;
    }

    pNode = pNode->next;
  }

  return pNode;
}

static FORCE_INLINE bool taosHashTableEmpty(const SSHashObj *pHashObj) {
  return tSimpleHashGetSize(pHashObj) == 0;
}

void *tSimpleHashGet(SSHashObj *pHashObj, const void *key) {
  if (pHashObj == NULL || taosHashTableEmpty(pHashObj) || key == NULL) {
    return NULL;
  }

  uint32_t hashVal = (*pHashObj->hashFp)(key, (uint32_t)pHashObj->keyLen);

  int32_t slot = HASH_INDEX(hashVal, pHashObj->capacity);
  SHNode *pNode = pHashObj->hashList[slot];
  if (pNode == NULL) {
    return NULL;
  }

  char *data = NULL;
  pNode = doSearchInEntryList(pHashObj, key, slot);
  if (pNode != NULL) {
    data = GET_SHASH_NODE_DATA(pNode);
  }

  return data;
}

int32_t tSimpleHashRemove(SSHashObj *pHashObj, const void *key) {
  // todo
}

void tSimpleHashClear(SSHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return;
  }

  SHNode *pNode, *pNext;
  for (int32_t i = 0; i < pHashObj->capacity; ++i) {
    pNode = pHashObj->hashList[i];
    if (pNode == NULL) {
      continue;
    }

    while (pNode) {
      pNext = pNode->next;
      FREE_HASH_NODE(pNode);
      pNode = pNext;
    }
  }
  pHashObj->size = 0;
}

void tSimpleHashCleanup(SSHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return;
  }

  tSimpleHashClear(pHashObj);
  taosMemoryFreeClear(pHashObj->hashList);
}

size_t tSimpleHashGetMemSize(const SSHashObj *pHashObj) {
  if (pHashObj == NULL) {
    return 0;
  }

  return (pHashObj->capacity * sizeof(void *)) + sizeof(SHNode) * tSimpleHashGetSize(pHashObj) + sizeof(SSHashObj);
}

void *tSimpleHashGetKey(const SSHashObj* pHashObj, void *data, size_t* keyLen) {
  int32_t offset = offsetof(SHNode, data);
  SHNode *node = data - offset;
  if (keyLen != NULL) {
    *keyLen = pHashObj->keyLen;
  }

  return GET_SHASH_NODE_KEY(node, pHashObj->dataLen);
}