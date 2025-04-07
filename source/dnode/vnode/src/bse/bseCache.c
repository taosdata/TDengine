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
#include "tdef.h"

typedef void (*CacheElemFn)(void *p);
typedef struct {
  int32_t cap;
  int32_t size;

  SHashObj      *pCache;
  SList         *lruList;
  TdThreadRwlock rwlock;

  CacheElemFn freeElemFunc;
} SCacheLRU;

typedef struct {
  void      *pItem;
  SSeqRange  pKey;
  int32_t    ref;
  SListNode *pNode;
} SCacheItem;

static int32_t cacheLRUCreate(int32_t cap, int32_t keySize, CacheElemFn freeElemFunc, SCacheLRU **pCache);
static int32_t cacheGet(SCacheLRU *pCache, SSeqRange *key, int32_t keyLen, void **pElem);
static int32_t cachePut(SCacheLRU *pCache, SSeqRange *key, int32_t keyLen, void *pElem);
static int32_t cacheRmove(SCacheLRU *pCache, SSeqRange *key, int32_t keyLen);
static void    cacheLRUFree(SCacheLRU *pCache);

static void freeItemInListNode(SListNode *pItem, CacheFreeFn fn);
static void freeItemInListNode(SListNode *pItem, CacheFreeFn fn) {
  if (pItem == NULL || fn == NULL) return;
  SCacheItem *pCacheItem = (SCacheItem *)pItem->data;
  if (pCacheItem->pItem != NULL) {
    fn(pCacheItem->pItem);
  }
}

int32_t cacheLRUCreate(int32_t cap, int32_t keySize, CacheElemFn freeElemFunc, SCacheLRU **pCache) {
  int32_t code = 0;
  int32_t lino = 0;

  SCacheLRU *p = taosMemoryCalloc(1, sizeof(SCacheLRU));
  if (p == NULL) {
    return terrno;
  }
  p->cap = cap;
  p->lruList = tdListNew(sizeof(SCacheItem));
  if (p->lruList == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }
  p->pCache = taosHashInit(16, MurmurHash3_32, true, HASH_NO_LOCK);
  if (p->pCache == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }

  p->freeElemFunc = freeElemFunc;

  taosThreadRwlockInit(&p->rwlock, NULL);

  *pCache = p;

_error:
  if (code != 0) {
    cacheLRUFree(p);
    bseError("failed to create cache lru at line %d since %d", lino, tstrerror(code));
  }
  return code;
}

int32_t cacheLRUGet(SCacheLRU *pCache, SSeqRange *key, int32_t keyLen, void **pElem) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockRdlock(&pCache->rwlock);

  SCacheItem *pItem = taosHashGet(pCache->pCache, key, keyLen);
  if (pItem == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _error);
  }
  pItem->pNode = tdListPopNode(pCache->lruList, pItem->pNode);
  tdListPrependNode(pCache->lruList, pItem->pNode);

  *pElem = pItem->pItem;

_error:
  if (code != 0) {
    bseError("failed to get cache lru at line %d since %d", lino, tstrerror(code));
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t cacheLRUPut(SCacheLRU *pCache, SSeqRange *key, int32_t keyLen, void *pElem) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  SCacheItem *pItem = taosHashGet(pCache->pCache, key, keyLen);
  if (pItem != NULL) {
    SListNode *t = tdListPopNode(pCache->lruList, pItem->pNode);
    freeItemInListNode(t, pCache->freeElemFunc);

    taosMemFreeClear(t);

    (void)taosHashRemove(pCache->pCache, key, keyLen);
    pCache->size--;
  }

  while (pCache->size >= pCache->cap) {
    SListNode *pNode = tdListPopTail(pCache->lruList);
    if (pNode != NULL) {
      SCacheItem *pCacheItem = (SCacheItem *)pNode->data;
      (void)taosHashRemove(pCache->pCache, &pCacheItem->pKey, sizeof(pCacheItem->pKey));

      freeItemInListNode(pNode, pCache->freeElemFunc);
      taosMemFreeClear(pNode);
      pCache->size--;
    }
  }

  SCacheItem item = {.pItem = pElem, .pKey = *(SSeqRange *)key, .ref = 1};
  SListNode *pListNode = tdListAdd(pCache->lruList, &item);
  if (pListNode == NULL) {
    TSDB_CHECK_CODE(terrno, lino, _error);
  }
  item.pNode = pListNode;

  code = taosHashPut(pCache->pCache, key, keyLen, &item, sizeof(SCacheItem));
  if (code != 0) {
    TSDB_CHECK_CODE(code, lino, _error);
  }

_error:
  if (code != 0) {
    bseError("failed to put cache lru at line %d since %d", __LINE__, tstrerror(code));
  } else {
    pCache->size++;
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}
int32_t cacheLRURemove(SCacheLRU *pCache, SSeqRange *key, int32_t keyLen) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  SCacheItem *pItem = taosHashGet(pCache->pCache, key, keyLen);
  if (pItem == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _error);
  }

  taosHashRemove(pCache->pCache, key, keyLen);

  SListNode *pNode = tdListPopNode(pCache->lruList, pItem->pNode);
  freeItemInListNode(pNode, pCache->freeElemFunc);

  taosMemFreeClear(pNode);
_error:
  if (code != 0) {
    bseError("failed to remove cache lru at line %d since %d", __LINE__, tstrerror(code));
  } else {
    pCache->size--;
  }

  taosThreadRwlockUnlock(&pCache->rwlock);

  return code;
}

void cacheLRUFree(SCacheLRU *pCache) {
  taosHashCleanup(pCache->pCache);
  taosThreadRwlockDestroy(&pCache->rwlock);

  while (isListEmpty(pCache->lruList) == 0) {
    SListNode *pNode = tdListPopTail(pCache->lruList);
    freeItemInListNode(pNode, pCache->freeElemFunc);
    taosMemFreeClear(pNode);
  }
  tdListFree(pCache->lruList);
  pCache->lruList = NULL;
  taosMemFree(pCache);
}

int32_t tableCacheOpen(int32_t cap, CacheFreeFn fn, STableCache **p) {
  int32_t      code = 0;
  STableCache *pCache = taosMemoryCalloc(1, sizeof(STableCache));
  if (pCache == NULL) {
    return terrno;
  }

  code = cacheLRUCreate(cap, sizeof(SSeqRange), (CacheElemFn)fn, (SCacheLRU **)&pCache->pCache);
  if (code != 0) {
  }
  pCache->size = 0;
  pCache->cap = cap;

  taosThreadRwlockInit(&pCache->rwlock, NULL);

  *p = pCache;
_error:
  if (code != 0) {
    bseError("failed to create table cache at line %d since %d", __LINE__, tstrerror(code));
  }
  return code;
}

void tableCacheClose(STableCache *p) {
  int32_t code = 0;
_error:
  if (code != 0) {
    bseError("failed to close table cache at line %d since %d", __LINE__, tstrerror(code));
  }
  cacheLRUFree((SCacheLRU *)p->pCache);
  taosThreadRwlockDestroy(&p->rwlock);
  taosMemFree(p);
}
int32_t tableCacheGet(STableCache *pCache, SSeqRange *key, STableReader **pReader) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockRdlock(&pCache->rwlock);

  void *pElem = NULL;
  code = cacheLRUGet(pCache->pCache, key, sizeof(*key), &pElem);
  TSDB_CHECK_CODE(code, lino, _error);

  *pReader = pElem;
_error:
  if (code != 0) {
    bseError("failed to get table cache at line %d since %d", lino, tstrerror(code));
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t tableCachePut(STableCache *pCache, SSeqRange *key, STableReader *pReader) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  code = cacheLRUPut(pCache->pCache, key, sizeof(*key), pReader);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to put table cache at line %d since %d", lino, tstrerror(code));
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}
int32_t tableCacheRemove(STableCache *pCache, SSeqRange *key) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  code = cacheLRURemove(pCache->pCache, key, sizeof(*key));
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to remove table cache at line %d since %d", lino, tstrerror(code));
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t blockCacheOpen(int32_t cap, CacheElemFn freeFn, SBlockCache **pCache) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockCache *p = taosMemoryCalloc(1, sizeof(SBlockCache));
  if (p == NULL) {
    return terrno;
  }
  code = cacheLRUCreate(cap, sizeof(char *), freeFn, (SCacheLRU **)&p->pCache);
  if (code != 0) {
    TSDB_CHECK_CODE(code, lino, _error);
  }

  p->cap = cap;
  taosThreadRwlockInit(&p->rwlock, NULL);

  *pCache = p;
_error:
  if (code != 0) {
    blockCacheClose(p);
    bseError("failed to create block cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}
int32_t blockCacheGet(SBlockCache *pCache, SSeqRange *key, SBlock **pBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadRwlockRdlock(&pCache->rwlock);

  code = cacheLRUGet(pCache->pCache, (SSeqRange *)key, sizeof(SSeqRange), (void **)pBlock);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to get block cache at line %d since %d", lino, tstrerror(code));
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}

int32_t blockCachePut(SBlockCache *pCache, SSeqRange *key, SBlock *pBlock) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  code = cacheLRUPut(pCache->pCache, key, sizeof(SSeqRange), pBlock);
  TSDB_CHECK_CODE(code, lino, _error);

  taosThreadRwlockUnlock(&pCache->rwlock);

_error:
  if (code != 0) {
    bseError("failed to put block cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}

int32_t blockCacheRemove(SBlockCache *pCache, SSeqRange *key) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadRwlockWrlock(&pCache->rwlock);

  code = cacheLRURemove(pCache->pCache, key, sizeof(SSeqRange));
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to remove block cache at line %d since %d", lino, tstrerror(code));
  }
  taosThreadRwlockUnlock(&pCache->rwlock);
  return code;
}
void blockCacheClose(SBlockCache *p) {
  cacheLRUFree((SCacheLRU *)p->pCache);
  taosThreadRwlockDestroy(&p->rwlock);
  taosMemFree(p);
}