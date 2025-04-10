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

  SHashObj *pCache;
  SList    *lruList;

  CacheElemFn   freeElemFunc;
  TdThreadMutex mutex;
} SLruCache;

typedef struct {
  void      *pItem;
  SSeqRange  pKey;
  int32_t    ref;
  SListNode *pNode;
} SCacheItem;

static int32_t lruCacheCreate(int32_t cap, int32_t keySize, CacheElemFn freeElemFunc, SLruCache **pCache);
static int32_t lruCacheGet(SLruCache *pCache, SSeqRange *key, int32_t keyLen, void **pElem);
static int32_t lruCachePut(SLruCache *pCache, SSeqRange *key, int32_t keyLen, void *pElem);
static int32_t lruCacheRemove(SLruCache *pCache, SSeqRange *key, int32_t keyLen);
static int32_t lruCacheRemoveNolock(SLruCache *pCache, SSeqRange *key, int32_t keyLen);
static void    lruCacheFree(SLruCache *pCache);
static void    freeItemInListNode(SListNode *pItem, CacheFreeFn fn);

void freeItemInListNode(SListNode *pItem, CacheFreeFn fn) {
  if (pItem == NULL || fn == NULL) return;
  SCacheItem *pCacheItem = (SCacheItem *)pItem->data;
  if (pCacheItem->pItem != NULL) {
    fn(pCacheItem->pItem);
  }
}

int32_t lruCacheCreate(int32_t cap, int32_t keySize, CacheElemFn freeElemFunc, SLruCache **pCache) {
  int32_t code = 0;
  int32_t lino = 0;

  SLruCache *p = taosMemoryCalloc(1, sizeof(SLruCache));
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

  taosThreadMutexInit(&p->mutex, NULL);
  *pCache = p;

_error:
  if (code != 0) {
    lruCacheFree(p);
    bseError("failed to create cache lru at line %d since %d", lino, tstrerror(code));
  }
  return code;
}

int32_t lruCacheGet(SLruCache *pCache, SSeqRange *key, int32_t keyLen, void **pElem) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexLock(&pCache->mutex);
  SCacheItem *pItem = taosHashGet(pCache->pCache, key, keyLen);
  if (pItem == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _error);
  }
  pItem->pNode = tdListPopNode(pCache->lruList, pItem->pNode);
  tdListPrependNode(pCache->lruList, pItem->pNode);

  *pElem = pItem->pItem;

_error:
  if (code != 0) {
    bseError("failed to get cache lru at line %d since %s", lino, tstrerror(code));
  }
  taosThreadMutexUnlock(&pCache->mutex);
  return code;
}

int32_t cacheLRUPut(SLruCache *pCache, SSeqRange *key, int32_t keyLen, void *pElem) {
  int32_t code = 0;
  int32_t lino = 0;

  taosThreadMutexLock(&pCache->mutex);
  SCacheItem *pItem = taosHashGet(pCache->pCache, key, keyLen);
  if (pItem != NULL) {
    SListNode *t = tdListPopNode(pCache->lruList, pItem->pNode);
    freeItemInListNode(t, pCache->freeElemFunc);

    taosMemFreeClear(t);

    (void)taosHashRemove(pCache->pCache, key, keyLen);
    pCache->size--;
  }

  while (pCache->size >= pCache->cap) {
    SListNode *pNode = tdListGetTail(pCache->lruList);
    if (pNode != NULL) {
      SCacheItem *pCacheItem = (SCacheItem *)pNode->data;
      code = lruCacheRemoveNolock(pCache, &pCacheItem->pKey, sizeof(pCacheItem->pKey));
      TSDB_CHECK_CODE(code, lino, _error);
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
  taosThreadMutexUnlock(&pCache->mutex);
  return code;
}
int32_t lruCacheRemoveNolock(SLruCache *pCache, SSeqRange *key, int32_t keyLen) {
  int32_t code = 0;
  int32_t lino = 0;

  SCacheItem *pItem = taosHashGet(pCache->pCache, key, keyLen);
  if (pItem == NULL) {
    TSDB_CHECK_CODE(code = TSDB_CODE_NOT_FOUND, lino, _error);
  }

  SListNode *pNode = tdListPopNode(pCache->lruList, pItem->pNode);
  freeItemInListNode(pNode, pCache->freeElemFunc);

  code = taosHashRemove(pCache->pCache, key, keyLen);
  TSDB_CHECK_CODE(code, lino, _error);

  taosMemFreeClear(pNode);
_error:
  if (code != 0) {
    bseError("failed to remove cache lru at line %d since %d", lino, tstrerror(code));
  } else {
    pCache->size--;
  }
  return code;
}
int32_t lruCacheRemove(SLruCache *pCache, SSeqRange *key, int32_t keyLen) {
  int32_t code = 0;
  int32_t lino = 0;
  taosThreadMutexLock(&pCache->mutex);
  code = lruCacheRemoveNolock(pCache, key, keyLen);
  taosThreadMutexUnlock(&pCache->mutex);

  return code;
}

void lruCacheFree(SLruCache *pCache) {
  taosHashCleanup(pCache->pCache);

  while (isListEmpty(pCache->lruList) == 0) {
    SListNode *pNode = tdListPopTail(pCache->lruList);
    freeItemInListNode(pNode, pCache->freeElemFunc);
    taosMemFreeClear(pNode);
  }
  tdListFree(pCache->lruList);
  pCache->lruList = NULL;

  taosThreadMutexDestroy(&pCache->mutex);
  taosMemoryFree(pCache);
}

int32_t tableCacheOpen(int32_t cap, CacheFreeFn fn, STableCache **p) {
  int32_t      code = 0;
  int32_t      line = 0;
  STableCache *pCache = taosMemoryCalloc(1, sizeof(STableCache));
  if (pCache == NULL) {
    return terrno;
  }

  code = lruCacheCreate(cap, sizeof(SSeqRange), (CacheElemFn)fn, (SLruCache **)&pCache->pCache);
  if (code != 0) {
    TSDB_CHECK_CODE(code, line, _error);
  }

  pCache->size = 0;
  pCache->cap = cap;

  *p = pCache;
_error:
  if (code != 0) {
    bseError("failed to create table cache at line %d since %d", __LINE__, tstrerror(code));
  }
  return code;
}

void tableCacheClose(STableCache *p) {
  if (p == NULL) return;

  lruCacheFree((SLruCache *)p->pCache);
  taosMemoryFree(p);
}
int32_t tableCacheGet(STableCache *pCache, SSeqRange *key, STableReader **pReader) {
  int32_t code = 0;
  int32_t lino = 0;

  void *pElem = NULL;
  code = lruCacheGet(pCache->pCache, key, sizeof(*key), &pElem);
  TSDB_CHECK_CODE(code, lino, _error);

  *pReader = pElem;
_error:
  if (code != 0) {
    bseWarn("failed to get table cache at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t tableCachePut(STableCache *pCache, SSeqRange *key, STableReader *pReader) {
  int32_t code = 0;
  int32_t lino = 0;

  code = cacheLRUPut(pCache->pCache, key, sizeof(*key), pReader);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to put table cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}
int32_t tableCacheRemove(STableCache *pCache, SSeqRange *key) {
  int32_t code = 0;
  int32_t lino = 0;

  code = lruCacheRemove(pCache->pCache, key, sizeof(*key));
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to remove table cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}

int32_t blockCacheOpen(int32_t cap, CacheElemFn freeFn, SBlockCache **pCache) {
  int32_t code = 0;
  int32_t lino = 0;

  SBlockCache *p = taosMemoryCalloc(1, sizeof(SBlockCache));
  if (p == NULL) {
    return terrno;
  }
  code = lruCacheCreate(cap, sizeof(char *), freeFn, (SLruCache **)&p->pCache);
  TSDB_CHECK_CODE(code, lino, _error);

  p->size = 0;
  p->cap = cap;

  *pCache = p;
_error:
  if (code != 0) {
    blockCacheClose(p);
    bseError("failed to create block cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}
int32_t blockCacheGet(SBlockCache *pCache, SSeqRange *key, void **pBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = lruCacheGet(pCache->pCache, (SSeqRange *)key, sizeof(SSeqRange), (void **)pBlock);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseWarn("failed to get block cache at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

int32_t blockCachePut(SBlockCache *pCache, SSeqRange *key, void *pBlock) {
  int32_t code = 0;
  int32_t lino = 0;

  code = cacheLRUPut(pCache->pCache, key, sizeof(SSeqRange), pBlock);
  TSDB_CHECK_CODE(code, lino, _error);

_error:
  if (code != 0) {
    bseError("failed to put block cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}

int32_t blockCacheRemove(SBlockCache *pCache, SSeqRange *key) {
  int32_t code = 0;
  int32_t lino = 0;

  code = lruCacheRemove(pCache->pCache, key, sizeof(SSeqRange));
  TSDB_CHECK_CODE(code, lino, _error);
_error:
  if (code != 0) {
    bseError("failed to remove block cache at line %d since %d", lino, tstrerror(code));
  }
  return code;
}
void blockCacheClose(SBlockCache *p) {
  if (p == NULL) return;

  lruCacheFree((SLruCache *)p->pCache);
  taosMemoryFree(p);
}