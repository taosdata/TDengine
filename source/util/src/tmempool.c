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

#define _DEFAULT_SOURCE
#include "tmempool.h"
#include "tlog.h"
#include "tutil.h"

typedef struct {
  int32_t       numOfFree;  /* number of free slots */
  int32_t       first;      /* the first free slot  */
  int32_t       numOfBlock; /* the number of blocks */
  int32_t       blockSize;  /* block size in bytes  */
  int32_t      *freeList;   /* the index list       */
  char         *pool;       /* the actual mem block */
  TdThreadMutex mutex;
} pool_t;

mpool_h taosMemPoolInit(int32_t numOfBlock, int32_t blockSize) {
  int32_t i;
  pool_t *pool_p;

  if (numOfBlock <= 1 || blockSize <= 1) {
    uError("invalid parameter in memPoolInit\n");
    return NULL;
  }

  pool_p = (pool_t *)taosMemoryMalloc(sizeof(pool_t));
  if (pool_p == NULL) {
    uError("mempool malloc failed\n");
    return NULL;
  } else {
    memset(pool_p, 0, sizeof(pool_t));
  }

  pool_p->blockSize = blockSize;
  pool_p->numOfBlock = numOfBlock;
  pool_p->pool = (char *)taosMemoryMalloc((size_t)(blockSize * numOfBlock));
  pool_p->freeList = (int32_t *)taosMemoryMalloc(sizeof(int32_t) * (size_t)numOfBlock);

  if (pool_p->pool == NULL || pool_p->freeList == NULL) {
    uError("failed to allocate memory\n");
    taosMemoryFreeClear(pool_p->freeList);
    taosMemoryFreeClear(pool_p->pool);
    taosMemoryFreeClear(pool_p);
    return NULL;
  }

  taosThreadMutexInit(&(pool_p->mutex), NULL);

  memset(pool_p->pool, 0, (size_t)(blockSize * numOfBlock));
  for (i = 0; i < pool_p->numOfBlock; ++i) pool_p->freeList[i] = i;

  pool_p->first = 0;
  pool_p->numOfFree = pool_p->numOfBlock;

  return (mpool_h)pool_p;
}

char *taosMemPoolMalloc(mpool_h handle) {
  char   *pos = NULL;
  pool_t *pool_p = (pool_t *)handle;

  taosThreadMutexLock(&(pool_p->mutex));

  if (pool_p->numOfFree > 0) {
    pos = pool_p->pool + pool_p->blockSize * (pool_p->freeList[pool_p->first]);
    pool_p->first++;
    pool_p->first = pool_p->first % pool_p->numOfBlock;
    pool_p->numOfFree--;
  }

  taosThreadMutexUnlock(&(pool_p->mutex));

  if (pos == NULL) uDebug("mempool: out of memory");
  return pos;
}

void taosMemPoolFree(mpool_h handle, char *pMem) {
  int32_t index;
  pool_t *pool_p = (pool_t *)handle;

  if (pMem == NULL) return;

  index = (int32_t)(pMem - pool_p->pool) % pool_p->blockSize;
  if (index != 0) {
    uError("invalid free address:%p\n", pMem);
    return;
  }

  index = (int32_t)((pMem - pool_p->pool) / pool_p->blockSize);
  if (index < 0 || index >= pool_p->numOfBlock) {
    uError("mempool: error, invalid address:%p", pMem);
    return;
  }

  memset(pMem, 0, (size_t)pool_p->blockSize);

  taosThreadMutexLock(&pool_p->mutex);

  pool_p->freeList[(pool_p->first + pool_p->numOfFree) % pool_p->numOfBlock] = index;
  pool_p->numOfFree++;

  taosThreadMutexUnlock(&pool_p->mutex);
}

void taosMemPoolCleanUp(mpool_h handle) {
  pool_t *pool_p = (pool_t *)handle;

  taosThreadMutexDestroy(&pool_p->mutex);
  if (pool_p->pool) taosMemoryFree(pool_p->pool);
  if (pool_p->freeList) taosMemoryFree(pool_p->freeList);
  memset(pool_p, 0, sizeof(*pool_p));
  taosMemoryFree(pool_p);
}
