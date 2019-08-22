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

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tlog.h"
#include "tmempool.h"
#include "tutil.h"

typedef struct {
  int             numOfFree;  /* number of free slots */
  int             first;      /* the first free slot  */
  int             numOfBlock; /* the number of blocks */
  int             blockSize;  /* block size in bytes  */
  int *           freeList;   /* the index list       */
  char *          pool;       /* the actual mem block */
  pthread_mutex_t mutex;
} pool_t;

mpool_h taosMemPoolInit(int numOfBlock, int blockSize) {
  int     i;
  pool_t *pool_p;

  if (numOfBlock <= 1 || blockSize <= 1) {
    pError("invalid parameter in memPoolInit\n");
    return NULL;
  }

  pool_p = (pool_t *)malloc(sizeof(pool_t));
  if (pool_p == NULL) {
    pError("mempool malloc failed\n");
    return NULL;
  } else {
    memset(pool_p, 0, sizeof(pool_t));
  }

  pool_p->blockSize = blockSize;
  pool_p->numOfBlock = numOfBlock;
  pool_p->pool = (char *)malloc((size_t)(blockSize * numOfBlock));
  pool_p->freeList = (int *)malloc(sizeof(int) * (size_t)numOfBlock);

  if (pool_p->pool == NULL || pool_p->freeList == NULL) {
    pError("failed to allocate memory\n");
    free(pool_p->freeList);
    free(pool_p->pool);
    free(pool_p);
    return NULL;
  }

  pthread_mutex_init(&(pool_p->mutex), NULL);

  for (i = 0; i < pool_p->numOfBlock; ++i) pool_p->freeList[i] = i;

  pool_p->first = 0;
  pool_p->numOfFree = pool_p->numOfBlock;

  return (mpool_h)pool_p;
}

char *taosMemPoolMalloc(mpool_h handle) {
  char *  pos = NULL;
  pool_t *pool_p = (pool_t *)handle;

  pthread_mutex_lock(&(pool_p->mutex));

  if (pool_p->numOfFree <= 0) {
    pTrace("mempool: out of memory");
  } else {
    pos = pool_p->pool + pool_p->blockSize * (pool_p->freeList[pool_p->first]);
    pool_p->first++;
    pool_p->first = pool_p->first % pool_p->numOfBlock;
    pool_p->numOfFree--;
  }

  pthread_mutex_unlock(&(pool_p->mutex));
  if (pos != NULL) memset(pos, 0, (size_t)pool_p->blockSize);
  return pos;
}

void taosMemPoolFree(mpool_h handle, char *pMem) {
  int     index;
  pool_t *pool_p = (pool_t *)handle;

  if (pMem == NULL) return;

  pthread_mutex_lock(&pool_p->mutex);

  index = (int)(pMem - pool_p->pool) % pool_p->blockSize;

  if (index != 0) {
    pError("invalid free address:%p\n", pMem);
  } else {
    index = (int)((pMem - pool_p->pool) / pool_p->blockSize);

    if (index < 0 || index >= pool_p->numOfBlock) {
      pError("mempool: error, invalid address:%p\n", pMem);
    } else {
      pool_p->freeList[(pool_p->first + pool_p->numOfFree) % pool_p->numOfBlock] = index;
      pool_p->numOfFree++;
      memset(pMem, 0, (size_t)pool_p->blockSize);
    }
  }

  pthread_mutex_unlock(&pool_p->mutex);
}

void taosMemPoolCleanUp(mpool_h handle) {
  pool_t *pool_p = (pool_t *)handle;

  pthread_mutex_destroy(&pool_p->mutex);
  if (pool_p->pool) free(pool_p->pool);
  if (pool_p->freeList) free(pool_p->freeList);
  memset(&pool_p, 0, sizeof(pool_p));
  free(pool_p);
}
