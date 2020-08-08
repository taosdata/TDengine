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

#ifndef TDENGINE_TCACHE_H
#define TDENGINE_TCACHE_H

#ifdef __cplusplus
extern "C" {
#endif

#define WITH_FLF

#ifdef WITH_FLF
#define DECL_FLF    const char *cfile, int cline, const char *cfunc
#define ARGS_FLF    __FILE__, __LINE__, __FUNCTION__
#define DARG_FLF    basename((char*)cfile), cline, cfunc, basename((char*)__FILE__), __LINE__, __FUNCTION__

#define DUMP(m, fmt, ...) m("%s[%d]#%s=>%s[%d]#%s: "fmt, DARG_FLF, ##__VA_ARGS__)
#define DUMP_NODE(m, pCacheObj, pNode, fmt, ...) m("%s[%d]#%s=>%s[%d]#%s: cache %s:[%p], node:[%p], ref:[%d], trash:[%s]"fmt, \
                                 DARG_FLF, \
                                 pCacheObj->name, pCacheObj, pNode, T_REF_VAL_GET(pNode), \
                                 pNode->inTrashCan ? "T": "F" \
                                 ##__VA_ARGS__)

#define DECL_FUNC1(rettype, funcname, a1) rettype do_##funcname(a1, DECL_FLF)
#define DECL_FUNC2(rettype, funcname, a1,a2) rettype do_##funcname(a1,a2, DECL_FLF)
#define DECL_FUNC3(rettype, funcname, a1,a2,a3) rettype do_##funcname(a1,a2,a3, DECL_FLF)
#define DECL_FUNC4(rettype, funcname, a1,a2,a3,a4) rettype do_##funcname(a1,a2,a3,a4, DECL_FLF)
#define DECL_FUNC5(rettype, funcname, a1,a2,a3,a4,a5) rettype do_##funcname(a1,a2,a3,a4,a5, DECL_FLF)
#define DECL_FUNC6(rettype, funcname, a1,a2,a3,a4,a5,a6) rettype do_##funcname(a1,a2,a3,a4,a5,a6, DECL_FLF)
#define DECL_FUNC7(rettype, funcname, a1,a2,a3,a4,a5,a6,a7) rettype do_##funcname(a1,a2,a3,a4,a5,a6,a7, DECL_FLF)
#define DECL_FUNC8(rettype, funcname, a1,a2,a3,a4,a5,a6,a7,a8) rettype do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8, DECL_FLF)
#define DECL_FUNC9(rettype, funcname, a1,a2,a3,a4,a5,a6,a7,a8,a9) rettype do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8,a9, DECL_FLF)
#define CALL_FUNC1(funcname, a1) do_##funcname(a1, ARGS_FLF)
#define CALL_FUNC2(funcname, a1,a2) do_##funcname(a1,a2, ARGS_FLF)
#define CALL_FUNC3(funcname, a1,a2,a3) do_##funcname(a1,a2,a3, ARGS_FLF)
#define CALL_FUNC4(funcname, a1,a2,a3,a4) do_##funcname(a1,a2,a3,a4, ARGS_FLF)
#define CALL_FUNC5(funcname, a1,a2,a3,a4,a5) do_##funcname(a1,a2,a3,a4,a5, ARGS_FLF)
#define CALL_FUNC6(funcname, a1,a2,a3,a4,a5,a6) do_##funcname(a1,a2,a3,a4,a5,a6, ARGS_FLF)
#define CALL_FUNC7(funcname, a1,a2,a3,a4,a5,a6,a7) do_##funcname(a1,a2,a3,a4,a5,a6,a7, ARGS_FLF)
#define CALL_FUNC8(funcname, a1,a2,a3,a4,a5,a6,a7,a8) do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8, ARGS_FLF)
#define CALL_FUNC9(funcname, a1,a2,a3,a4,a5,a6,a7,a8,a9) do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8,a9, ARGS_FLF)

#else // WITH_FLF

#define DUMP(m, fmt, ...) m(fmt, ##__VA_ARGS__)

#define DECL_FUNC1(rettype, funcname, a1) rettype do_##funcname(a1)
#define DECL_FUNC2(rettype, funcname, a1,a2) rettype do_##funcname(a1,a2)
#define DECL_FUNC3(rettype, funcname, a1,a2,a3) rettype do_##funcname(a1,a2,a3)
#define DECL_FUNC4(rettype, funcname, a1,a2,a3,a4) rettype do_##funcname(a1,a2,a3,a4)
#define DECL_FUNC5(rettype, funcname, a1,a2,a3,a4,a5) rettype do_##funcname(a1,a2,a3,a4,a5)
#define DECL_FUNC6(rettype, funcname, a1,a2,a3,a4,a5,a6) rettype do_##funcname(a1,a2,a3,a4,a5,a6)
#define DECL_FUNC7(rettype, funcname, a1,a2,a3,a4,a5,a6,a7) rettype do_##funcname(a1,a2,a3,a4,a5,a6,a7)
#define DECL_FUNC8(rettype, funcname, a1,a2,a3,a4,a5,a6,a7,a8) rettype do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8)
#define DECL_FUNC9(rettype, funcname, a1,a2,a3,a4,a5,a6,a7,a8,a9) rettype do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8,a9)
#define CALL_FUNC1(funcname, a1) do_##funcname(a1)
#define CALL_FUNC2(funcname, a1,a2) do_##funcname(a1,a2)
#define CALL_FUNC3(funcname, a1,a2,a3) do_##funcname(a1,a2,a3)
#define CALL_FUNC4(funcname, a1,a2,a3,a4) do_##funcname(a1,a2,a3,a4)
#define CALL_FUNC5(funcname, a1,a2,a3,a4,a5) do_##funcname(a1,a2,a3,a4,a5)
#define CALL_FUNC6(funcname, a1,a2,a3,a4,a5,a6) do_##funcname(a1,a2,a3,a4,a5,a6)
#define CALL_FUNC7(funcname, a1,a2,a3,a4,a5,a6,a7) do_##funcname(a1,a2,a3,a4,a5,a6,a7)
#define CALL_FUNC8(funcname, a1,a2,a3,a4,a5,a6,a7,a8) do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8)
#define CALL_FUNC9(funcname, a1,a2,a3,a4,a5,a6,a7,a8,a9) do_##funcname(a1,a2,a3,a4,a5,a6,a7,a8,a9)
#endif // WITH_FLF

#include "os.h"
#include "tlockfree.h"
#include "hash.h"

typedef void (*__cache_free_fn_t)(void*);

typedef struct SCacheStatis {
  int64_t missCount;
  int64_t hitCount;
  int64_t totalAccess;
  int64_t refreshCount;
} SCacheStatis;

typedef struct SCacheDataNode           SCacheDataNode;
typedef struct SCacheObj                SCacheObj;

struct SCacheDataNode {
  uint64_t           addedTime;    // the added time when this element is added or updated into cache
  uint64_t           lifespan;     // life duration when this element should be remove from cache
  uint64_t           expireTime;   // expire time
  uint64_t           signature;
  uint16_t           keySize: 15;  // max key size: 32kb
  bool               inTrashCan: 1;// denote if it is in trash or not
  uint32_t           size;         // allocated size for current SCacheDataNode
  T_REF_DECLARE()

  // double-linked list of nodes in hash set or in trash
  SCacheDataNode    *next;
  SCacheDataNode    *prev;
  // denotes whether or not it's in hash set or in trash
  SCacheObj         *owner;

  // denotes if it's or not being callback'd from where
  pthread_t         *cb_thread;

  char              *key;
  char               data[];
};

/*
 * to accommodate the old data which has the same key value of new one in hashList
 * when an new node is put into cache, if an existed one with the same key:
 * 1. if the old one does not be referenced, update it.
 * 2. otherwise, move the old one to pTrash, addedTime the new one.
 *
 * when the node in pTrash does not be referenced, it will be release at the expired expiredTime
 */
struct SCacheObj {
  int64_t         totalSize;          // total allocated buffer in this hash table, SCacheObj is not included.
  int64_t         refreshTime;
  char*           name;
  SCacheStatis    statistics;
  SHashObj *      pHashTable;
  __cache_free_fn_t freeFp;
  uint32_t        numOfElemsInTrash;  // number of element in trash
  volatile uint8_t deleting;           // set the deleting flag to stop refreshing ASAP.
  pthread_t       refreshWorker;
  bool            extendLifespan;     // auto extend life span when one item is accessed.

  int             efd;

  SCacheDataNode *hash_head;
  SCacheDataNode *hash_tail;

  // trade space for speed
  SCacheDataNode *trash_head;
  SCacheDataNode *trash_tail;

#if defined(LINUX)
  pthread_rwlock_t lock;
#else
  pthread_mutex_t  lock;
#endif
};

/**
 * initialize the cache object
 * @param keyType              key type
 * @param refreshTimeInSeconds refresh operation interval time, the maximum survival time when one element is expired
 *                             and not referenced by other objects
 * @param extendLifespan       auto extend lifespan, if accessed
 * @param fn                   free resource callback function
 * @return
 */
// SCacheObj *taosCacheInit(int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_free_fn_t fn, const char *cacheName);
DECL_FUNC5(SCacheObj *, taosCacheInit, int32_t keyType, int64_t refreshTimeInSeconds, bool extendLifespan, __cache_free_fn_t fn, const char *cacheName);
#define taosCacheInit(a1,a2,a3,a4,a5) CALL_FUNC5(taosCacheInit, a1,a2,a3,a4,a5)


/**
 * add data into cache
 *
 * @param handle        cache object
 * @param key           key
 * @param pData         cached data
 * @param dataSize      data size
 * @param keepTime      survival time in second
 * @return              cached element
 */
// void *taosCachePut(SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize, int keepTimeInSeconds);
DECL_FUNC6(void *, taosCachePut, SCacheObj *pCacheObj, const void *key, size_t keyLen, const void *pData, size_t dataSize, int keepTimeInSeconds);
#define taosCachePut(a1,a2,a3,a4,a5,a6) CALL_FUNC6(taosCachePut, a1,a2,a3,a4,a5,a6)

/**
 * get data from cache
 * @param pCacheObj     cache object
 * @param key           key
 * @return              cached data or NULL
 */
// void *taosCacheAcquireByKey(SCacheObj *pCacheObj, const void *key, size_t keyLen);
DECL_FUNC3(void *, taosCacheAcquireByKey, SCacheObj *pCacheObj, const void *key, size_t keyLen);
#define taosCacheAcquireByKey(a1,a2,a3) CALL_FUNC3(taosCacheAcquireByKey, a1,a2,a3)

/**
 * update the expire time of data in cache 
 * @param pCacheObj     cache object
 * @param key           key
 * @param keyLen        keyLen
 * @param expireTime    new expire time of data
 * @return
 */ 
// void* taosCacheUpdateExpireTimeByName(SCacheObj *pCacheObj, void *key, size_t keyLen, uint64_t expireTime);
DECL_FUNC4(void*, taosCacheUpdateExpireTimeByName, SCacheObj *pCacheObj, void *key, size_t keyLen, uint64_t expireTime);
#define taosCacheUpdateExpireTimeByName(a1,a2,a3,a4) CALL_FUNC4(taosCacheUpdateExpireTimeByName, a1,a2,a3,a4)

/**
 * Add one reference count for the exist data, and assign this data for a new owner.
 * The new owner needs to invoke the taosCacheRelease when it does not need this data anymore.
 * This procedure is a faster version of taosCacheAcquireByKey function, which avoids the sideeffect of the problem of
 * the data is moved to trash, and taosCacheAcquireByKey will fail to retrieve it again.
 *
 * @param handle
 * @param data
 * @return
 */
// void *taosCacheAcquireByData(SCacheObj *pCacheObj, void *data);
DECL_FUNC2(void *, taosCacheAcquireByData, SCacheObj *pCacheObj, void *data);
#define taosCacheAcquireByData(a1,a2) CALL_FUNC2(taosCacheAcquireByData, a1,a2)

/**
 * transfer the ownership of data in cache to another object without increasing reference count.
 * @param handle
 * @param data
 * @return
 */
// void *taosCacheTransfer(SCacheObj *pCacheObj, void **data);
DECL_FUNC2(void *, taosCacheTransfer, SCacheObj *pCacheObj, void **data);
#define taosCacheTransfer(a1,a2) CALL_FUNC2(taosCacheTransfer, a1,a2)

/**
 * remove data in cache, the data will not be removed immediately.
 * if it is referenced by other object, it will be remain in cache
 * @param handle    cache object
 * @param data      not the key, actually referenced data
 * @param _remove   force model, reduce the ref count and move the data into pTrash
 */
// void taosCacheRelease(SCacheObj *pCacheObj, void **data, bool _remove);
DECL_FUNC3(void, taosCacheRelease, SCacheObj *pCacheObj, void **data, bool _remove);
#define taosCacheRelease(a1,a2,a3) CALL_FUNC3(taosCacheRelease, a1,a2,a3)

/**
 *  move all data node into trash, clear node in trash can if it is not referenced by any clients
 * @param handle
 */
// void taosCacheEmpty(SCacheObj *pCacheObj);
DECL_FUNC1(void, taosCacheEmpty, SCacheObj *pCacheObj);
#define taosCacheEmpty(a1) CALL_FUNC1(taosCacheEmpty, a1)

/**
 * release all allocated memory and destroy the cache object.
 *
 * This function only set the deleting flag, and the specific work of clean up cache is delegated to
 * taosCacheRefresh function, which will executed every SCacheObj->refreshTime sec.
 *
 * If the value of SCacheObj->refreshTime is too large, the taosCacheRefresh function may not be invoked
 * before the main thread terminated, in which case all allocated resources are simply recycled by OS.
 *
 * @param handle
 */
// void taosCacheCleanup(SCacheObj *pCacheObj);
DECL_FUNC1(void, taosCacheCleanup, SCacheObj *pCacheObj);
#define taosCacheCleanup(a1) CALL_FUNC1(taosCacheCleanup, a1)

/**
 *
 * @param pCacheObj
 * @param fp
 * @return
 */
// void taosCacheRefresh(SCacheObj *pCacheObj, __cache_free_fn_t fp);
DECL_FUNC2(void, taosCacheRefresh, SCacheObj *pCacheObj, __cache_free_fn_t fp);
#define taosCacheRefresh(a1,a2) CALL_FUNC2(taosCacheRefresh, a1,a2)

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TCACHE_H
