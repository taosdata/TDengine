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

#include "bseCache.h"

typedef struct {
  int32_t cap;
  int32_t size;

  SHashObj      *pCache;
  SList         *lruList;
  TdThreadRwlock rwlock;
} SCacheLRU;

typedef struct {
  SListNode *pNode;

} SCacheItem;

static int32_t cacheLRUCreate(int32_t cap, int32_t keySize, int32_t elemSize, SCacheLRU **pCache);
static void    cacheLRUFree(SCacheLRU *pCache);

int32_t cacheLRUCreate(int32_t cap, int32_t keySize, int32_t elemSize, SCacheLRU **pCache) {
  int32_t code = 0;
  int32_t lino = 0;

  SCacheLRU *p = taosMemoryCalloc(1, sizeof(SCacheLRU));
  if (p == NULL) {
    return terrno;
  }
  p->cap = cap;
  p->lruList = tdListNew(elemSize);
  if (p->lruList == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }
  p->pCache = taosHashInit(16, MurmurHash3_32, true, HASH_NO_LOCK);
  if (p->pCache == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  taosThreadRwlockInit(&p->rwlock, NULL);

  *pCache = p;

_error:
  if (code != 0) {
    cacheLRUFree(p);
    bseError("failed to create cache lru at line %d since %d", lino, tstrerror(code));
  }
  return code;
}

int32_t cacheLRUGet(SCacheLRU *pCache, char *key, int32_t keyLen, void **pElem) {
  int32_t code = 0;
  taosThreadRwlockRdlock(&pCache->rwlock);

  SListNode *pNode = taosHashGet(pCache->pCache, key, keyLen);
  if (pNode == NULL) {
    code = TSDB_CODE_NOT_FOUND;
    goto _error;
  }

  *pElem = pNode->data;
_error:
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t cacheLRUPut(SCacheLRU *pCache, char *key, int32_t keyLen, void *pElem) {
  int32_t code = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  SListNode *pNode = taosHashGet(pCache->pCache, key, keyLen);
  if (pNode != NULL) {
    tdListPopNode(pCache->lruList, pNode);
    tdListPrependNode(pCache->lruList, pNode);
  }

  if (pCache->size >= pCache->cap) {
    SListNode *pTail = tdListPopTail(pCache->lruList);
    if (pTail != NULL) {
      // taosHashRemove(pCache->pCache, pTail->key, pTail->keyLen);
      taosMemoryFree(pTail);
      pCache->size--;
    }
  }

  // SListNode *pNewNode = taosMemoryCalloc(1, sizeof(SListNode));
  // if (pNewNode == NULL) {
  //   code = terrno;
  //   goto _error;
  // }
  // pNewNode->data = pElem;
  // pNewNode->key = key;
  // pNewNode->keyLen = keyLen;

  // taosHashPut(pCache->pCache, key, keyLen, pNewNode, sizeof(SListNode));
  // taosListAddHead(pCache->lruList, pNewNode);
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

void cacheLRUFree(SCacheLRU *pCache) {
  taosHashCleanup(pCache->pCache);
  taosThreadRwlockDestroy(&pCache->rwlock);
  taosMemFree(pCache->lruList);
  taosMemFree(pCache);
}
typedef struct {
  STableReader *pReader;
} STableCacheItem;

int32_t tableCacheOpen(int32_t cap, STableCache **p) {
  int32_t      code = 0;
  STableCache *pCache = taosMemoryCalloc(1, sizeof(STableCache));
  if (pCache == NULL) {
    return terrno;
  }
  pCache->cap = cap;

  taosThreadRwlockInit(&pCache->rwlock, NULL);

  *p = pCache;
  return code;
}
int32_t tableCacheGet(STableCache *pCache, SSeqRange *key, STableReader **pReader) {
  int32_t code = 0;
  taosThreadRwlockRdlock(&pCache->rwlock);
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t tableCachePut(STableCache *pCache, SSeqRange *key, STableReader *pReader) {
  int32_t code = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

void tableCacheClose(STableCache *pCache) {
  taosMemFree(pCache->pCache);
  taosThreadRwlockDestroy(&pCache->rwlock);
  taosMemFree(pCache);
}

int32_t blockCacheOpen(int32_t cap, SBlockCache **pCache) {
  int32_t code = 0;

  SBlockCache *p = taosMemoryCalloc(1, sizeof(SBlockCache));
  if (p == NULL) {
    return terrno;
  }
  p->cap = cap;
  taosThreadRwlockInit(&p->rwlock, NULL);

  *pCache = p;
  return code;
}
int32_t blockCacheGet(SBlockCache *pCache, char *key, SBlock **pBlock) {
  int32_t code = 0;

  taosThreadRwlockRdlock(&pCache->rwlock);

  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t blockCachePut(SBlockCache *pCache, char *key, SBlock *pBlock) {
  int32_t code = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}
void blockCacheClose(SBlockCache *p) {
  taosMemFree(p->pCache);
  taosThreadRwlockDestroy(&p->rwlock);
  taosMemFree(p);
}