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
#include "tlog.h"
#include "tmempool.h"

typedef struct _id_hash_t {
  int32_t            id;
  struct _id_hash_t *prev;
  struct _id_hash_t *next;
  void *             data;
} SIdHash;

typedef struct {
  SIdHash **idHashList;
  mpool_h   idHashMemPool;
  int64_t  *lockedBy;
  int       maxSessions;
} SHashObj;

static void taosLockHash(SHashObj *pObj, int hash);
static void taosUnlockHash(SHashObj *pObj, int hash);

int taosHashId(void *handle, uint32_t id) {
  SHashObj *pObj = (SHashObj *)handle;
  int       hash = 0;

  hash = hash % pObj->maxSessions;

  return hash;
}

void *taosAddIdHash(void *handle, void *data, int32_t id) {
  int       hash;
  SIdHash  *pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = taosHashId(pObj, id);
  pNode = (SIdHash *)taosMemPoolMalloc(pObj->idHashMemPool);
  pNode->id = id;
  pNode->data = data;
  pNode->prev = 0;

  taosLockHash(pObj, hash);
  pNode->next = pObj->idHashList[hash];

  if (pObj->idHashList[hash] != 0) (pObj->idHashList[hash])->prev = pNode;
  pObj->idHashList[hash] = pNode;
  taosUnlockHash(pObj, hash);

  return pObj;
}

void taosDeleteIdHash(void *handle, int32_t id) {
  int       hash;
  SIdHash  *pNode;
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  hash = taosHashId(pObj, id);

  taosLockHash(pObj, hash);

  pNode = pObj->idHashList[hash];
  while (pNode) {
    if (pNode->id == id) break;

    pNode = pNode->next;
  }

  if (pNode) {
    if (pNode->prev) {
      pNode->prev->next = pNode->next;
    } else {
      pObj->idHashList[hash] = pNode->next;
    }

    if (pNode->next) {
      pNode->next->prev = pNode->prev;
    }

    taosMemPoolFree(pObj->idHashMemPool, (char *)pNode);
  }

  taosUnlockHash(pObj, hash);
}

void *taosGetIdHash(void *handle, uint32_t id) {
  int       hash;
  SIdHash  *pNode;
  SHashObj *pObj;
  void     *data = NULL;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return NULL;

  hash = taosHashId(pObj, id);

  taosLockHash(pObj, hash);
  pNode = pObj->idHashList[hash];

  while (pNode) {
    if (pNode->id == id) {
      break;
    }
    pNode = pNode->next;
  }

  if (pNode) 
    data = pNode->data;
   
  taosUnlockHash(pObj, hash);

  return data;
}

void *taosOpenIdHash(int maxSessions) {
  SIdHash **idHashList;
  mpool_h   idHashMemPool;
  SHashObj *pObj;

  idHashMemPool = taosMemPoolInit(maxSessions, sizeof(SIdHash));
  if (idHashMemPool == 0) return NULL;

  idHashList = calloc(sizeof(SIdHash *), (size_t)maxSessions);
  if (idHashList == 0) {
    taosMemPoolCleanUp(idHashMemPool);
    return NULL;
  }

  pObj = malloc(sizeof(SHashObj));
  if (pObj == NULL) {
    taosMemPoolCleanUp(idHashMemPool);
    free(idHashList);
    return NULL;
  }

  pObj->maxSessions = maxSessions;
  pObj->idHashMemPool = idHashMemPool;
  pObj->idHashList = idHashList;

  pObj->lockedBy = calloc(sizeof(int64_t), maxSessions);

  return pObj;
}

void taosCloseIdHash(void *handle) {
  SHashObj *pObj;

  pObj = (SHashObj *)handle;
  if (pObj == NULL || pObj->maxSessions == 0) return;

  if (pObj->idHashMemPool) taosMemPoolCleanUp(pObj->idHashMemPool);

  if (pObj->idHashList) free(pObj->idHashList);

  memset(pObj, 0, sizeof(SHashObj));
  free(pObj);
}

static void taosLockHash(SHashObj *pObj, int hash) {
  int64_t tid = taosGetPthreadId();
  int     i = 0;
  while (atomic_val_compare_exchange_64(&(pObj->lockedBy[hash]), 0, tid) != 0) {
    if (++i % 1000 == 0) {
      sched_yield();
    }
  }
}

static void taosUnlockHash(SHashObj *pObj, int hash) {
  int64_t tid = taosGetPthreadId();
  if (atomic_val_compare_exchange_64(&(pObj->lockedBy[hash]), tid, 0) != tid) {
    assert(false);
  }
}

