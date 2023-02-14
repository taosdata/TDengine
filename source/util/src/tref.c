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
#include "tref.h"
#include "taoserror.h"
#include "tlog.h"
#include "tutil.h"

#define TSDB_REF_OBJECTS       50
#define TSDB_REF_STATE_EMPTY   0
#define TSDB_REF_STATE_ACTIVE  1
#define TSDB_REF_STATE_DELETED 2

typedef struct SRefNode {
  struct SRefNode *prev;     // previous node
  struct SRefNode *next;     // next node
  void            *p;        // pointer to resource protected,
  int64_t          rid;      // reference ID
  int32_t          count;    // number of references
  int32_t          removed;  // 1: removed
} SRefNode;

typedef struct {
  SRefNode **nodeList;  // array of SRefNode linked list
  int32_t    state;     // 0: empty, 1: active;  2: deleted
  int32_t    rsetId;    // refSet ID, global unique
  int64_t    rid;       // increase by one for each new reference
  int32_t    max;       // mod
  int32_t    count;     // total number of SRefNodes in this set
  int64_t   *lockedBy;
  void (*fp)(void *);
} SRefSet;

static SRefSet       tsRefSetList[TSDB_REF_OBJECTS];
static TdThreadOnce  tsRefModuleInit = PTHREAD_ONCE_INIT;
static TdThreadMutex tsRefMutex;
static int32_t       tsRefSetNum = 0;
static int32_t       tsNextId = 0;

static void    taosInitRefModule(void);
static void    taosLockList(int64_t *lockedBy);
static void    taosUnlockList(int64_t *lockedBy);
static void    taosIncRsetCount(SRefSet *pSet);
static void    taosDecRsetCount(SRefSet *pSet);
static int32_t taosDecRefCount(int32_t rsetId, int64_t rid, int32_t remove);

int32_t taosOpenRef(int32_t max, RefFp fp) {
  SRefNode **nodeList;
  SRefSet   *pSet;
  int64_t   *lockedBy;
  int32_t    i, rsetId;

  taosThreadOnce(&tsRefModuleInit, taosInitRefModule);

  nodeList = taosMemoryCalloc(sizeof(SRefNode *), (size_t)max);
  if (nodeList == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  lockedBy = taosMemoryCalloc(sizeof(int64_t), (size_t)max);
  if (lockedBy == NULL) {
    taosMemoryFree(nodeList);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  taosThreadMutexLock(&tsRefMutex);

  for (i = 0; i < TSDB_REF_OBJECTS; ++i) {
    tsNextId = (tsNextId + 1) % TSDB_REF_OBJECTS;
    if (tsNextId == 0) tsNextId = 1;  // dont use 0 as rsetId
    if (tsRefSetList[tsNextId].state == TSDB_REF_STATE_EMPTY) break;
  }

  if (i < TSDB_REF_OBJECTS) {
    rsetId = tsNextId;
    pSet = tsRefSetList + rsetId;
    pSet->max = max;
    pSet->nodeList = nodeList;
    pSet->lockedBy = lockedBy;
    pSet->fp = fp;
    pSet->rid = 1;
    pSet->rsetId = rsetId;
    pSet->state = TSDB_REF_STATE_ACTIVE;
    taosIncRsetCount(pSet);

    tsRefSetNum++;
    uTrace("rsetId:%d is opened, max:%d, fp:%p refSetNum:%d", rsetId, max, fp, tsRefSetNum);
  } else {
    rsetId = TSDB_CODE_REF_FULL;
    taosMemoryFree(nodeList);
    taosMemoryFree(lockedBy);
    uTrace("run out of Ref ID, maximum:%d refSetNum:%d", TSDB_REF_OBJECTS, tsRefSetNum);
  }

  taosThreadMutexUnlock(&tsRefMutex);

  return rsetId;
}

int32_t taosCloseRef(int32_t rsetId) {
  SRefSet *pSet;
  int32_t  deleted = 0;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d is invalid, out of range", rsetId);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return -1;
  }

  pSet = tsRefSetList + rsetId;

  taosThreadMutexLock(&tsRefMutex);

  if (pSet->state == TSDB_REF_STATE_ACTIVE) {
    pSet->state = TSDB_REF_STATE_DELETED;
    deleted = 1;
    uTrace("rsetId:%d is closed, count:%d", rsetId, pSet->count);
  } else {
    uTrace("rsetId:%d is already closed, count:%d", rsetId, pSet->count);
  }

  taosThreadMutexUnlock(&tsRefMutex);

  if (deleted) taosDecRsetCount(pSet);

  return 0;
}

int64_t taosAddRef(int32_t rsetId, void *p) {
  int32_t   hash;
  SRefNode *pNode;
  SRefSet  *pSet;
  int64_t   rid = 0;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d p:%p failed to add, rsetId not valid", rsetId, p);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return -1;
  }

  pSet = tsRefSetList + rsetId;
  taosIncRsetCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    taosDecRsetCount(pSet);
    uTrace("rsetId:%d p:%p failed to add, not active", rsetId, p);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    return -1;
  }

  pNode = taosMemoryCalloc(sizeof(SRefNode), 1);
  if (pNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  rid = atomic_add_fetch_64(&pSet->rid, 1);
  hash = rid % pSet->max;
  taosLockList(pSet->lockedBy + hash);

  pNode->p = p;
  pNode->rid = rid;
  pNode->count = 1;

  pNode->prev = NULL;
  pNode->next = pSet->nodeList[hash];
  if (pSet->nodeList[hash]) pSet->nodeList[hash]->prev = pNode;
  pSet->nodeList[hash] = pNode;

  uTrace("rsetId:%d p:%p rid:%" PRId64 " is added, count:%d", rsetId, p, rid, pSet->count);

  taosUnlockList(pSet->lockedBy + hash);

  return rid;
}

int32_t taosRemoveRef(int32_t rsetId, int64_t rid) { return taosDecRefCount(rsetId, rid, 1); }

// if rid is 0, return the first p in hash list, otherwise, return the next after current rid
void *taosAcquireRef(int32_t rsetId, int64_t rid) {
  int32_t   hash;
  SRefNode *pNode;
  SRefSet  *pSet;
  void     *p = NULL;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    // uTrace("rsetId:%d rid:%" PRId64 " failed to acquire, rsetId not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return NULL;
  }

  if (rid <= 0) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to acquire, rid not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_NOT_EXIST;
    return NULL;
  }

  pSet = tsRefSetList + rsetId;
  taosIncRsetCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to acquire, not active", rsetId, rid);
    taosDecRsetCount(pSet);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    return NULL;
  }

  hash = rid % pSet->max;
  taosLockList(pSet->lockedBy + hash);

  pNode = pSet->nodeList[hash];

  while (pNode) {
    if (pNode->rid == rid) {
      break;
    }

    pNode = pNode->next;
  }

  if (pNode) {
    if (pNode->removed == 0) {
      pNode->count++;
      p = pNode->p;
      uTrace("rsetId:%d p:%p rid:%" PRId64 " is acquired", rsetId, pNode->p, rid);
    } else {
      terrno = TSDB_CODE_REF_NOT_EXIST;
      uTrace("rsetId:%d p:%p rid:%" PRId64 " is already removed, failed to acquire", rsetId, pNode->p, rid);
    }
  } else {
    terrno = TSDB_CODE_REF_NOT_EXIST;
    uTrace("rsetId:%d rid:%" PRId64 " is not there, failed to acquire", rsetId, rid);
  }

  taosUnlockList(pSet->lockedBy + hash);

  taosDecRsetCount(pSet);

  return p;
}

int32_t taosReleaseRef(int32_t rsetId, int64_t rid) { return taosDecRefCount(rsetId, rid, 0); }

// if rid is 0, return the first p in hash list, otherwise, return the next after current rid
void *taosIterateRef(int32_t rsetId, int64_t rid) {
  SRefNode *pNode = NULL;
  SRefSet  *pSet;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to iterate, rsetId not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return NULL;
  }

  if (rid < 0) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to iterate, rid not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_NOT_EXIST;
    return NULL;
  }

  void *newP = NULL;
  pSet = tsRefSetList + rsetId;
  taosIncRsetCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to iterate, rset not active", rsetId, rid);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    taosDecRsetCount(pSet);
    return NULL;
  }

  do {
    newP = NULL;
    int32_t hash = 0;
    if (rid > 0) {
      hash = rid % pSet->max;
      taosLockList(pSet->lockedBy + hash);

      pNode = pSet->nodeList[hash];
      while (pNode) {
        if (pNode->rid == rid) break;
        pNode = pNode->next;
      }

      if (pNode == NULL) {
        uError("rsetId:%d rid:%" PRId64 " not there, quit", rsetId, rid);
        terrno = TSDB_CODE_REF_NOT_EXIST;
        taosUnlockList(pSet->lockedBy + hash);
        taosDecRsetCount(pSet);
        return NULL;
      }

      // rid is there
      pNode = pNode->next;
      // check first place
      while (pNode) {
        if (!pNode->removed) break;
        pNode = pNode->next;
      }
      if (pNode == NULL) {
        taosUnlockList(pSet->lockedBy + hash);
        hash++;
      }
    }

    if (pNode == NULL) {
      for (; hash < pSet->max; ++hash) {
        taosLockList(pSet->lockedBy + hash);
        pNode = pSet->nodeList[hash];
        if (pNode) {
          // check first place
          while (pNode) {
            if (!pNode->removed) break;
            pNode = pNode->next;
          }
          if (pNode) break;
        }
        taosUnlockList(pSet->lockedBy + hash);
      }
    }

    if (pNode) {
      pNode->count++;  // acquire it
      newP = pNode->p;
      taosUnlockList(pSet->lockedBy + hash);
      uTrace("rsetId:%d p:%p rid:%" PRId64 " is returned", rsetId, newP, rid);
    } else {
      uTrace("rsetId:%d the list is over", rsetId);
    }

    if (rid > 0) taosReleaseRef(rsetId, rid);  // release the current one
    if (pNode) rid = pNode->rid;
  } while (newP && pNode->removed);

  taosDecRsetCount(pSet);

  return newP;
}

int32_t taosListRef() {
  SRefSet  *pSet;
  SRefNode *pNode;
  int32_t   num = 0;

  taosThreadMutexLock(&tsRefMutex);

  for (int32_t i = 0; i < TSDB_REF_OBJECTS; ++i) {
    pSet = tsRefSetList + i;

    if (pSet->state == TSDB_REF_STATE_EMPTY) continue;

    uInfo("rsetId:%d state:%d count:%d", i, pSet->state, pSet->count);

    for (int32_t j = 0; j < pSet->max; ++j) {
      pNode = pSet->nodeList[j];

      while (pNode) {
        uInfo("rsetId:%d p:%p rid:%" PRId64 "count:%d", i, pNode->p, pNode->rid, pNode->count);
        pNode = pNode->next;
        num++;
      }
    }
  }

  taosThreadMutexUnlock(&tsRefMutex);

  return num;
}

static int32_t taosDecRefCount(int32_t rsetId, int64_t rid, int32_t remove) {
  int32_t   hash;
  SRefSet  *pSet;
  SRefNode *pNode;
  int32_t   released = 0;
  int32_t   code = 0;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to remove, rsetId not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return -1;
  }

  if (rid <= 0) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to remove, rid not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_NOT_EXIST;
    return -1;
  }

  pSet = tsRefSetList + rsetId;
  if (pSet->state == TSDB_REF_STATE_EMPTY) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to remove, cleaned", rsetId, rid);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    return -1;
  }

  hash = rid % pSet->max;
  taosLockList(pSet->lockedBy + hash);

  pNode = pSet->nodeList[hash];
  while (pNode) {
    if (pNode->rid == rid) break;

    pNode = pNode->next;
  }

  if (pNode) {
    pNode->count--;
    if (remove) pNode->removed = 1;

    if (pNode->count <= 0) {
      if (pNode->prev) {
        pNode->prev->next = pNode->next;
      } else {
        pSet->nodeList[hash] = pNode->next;
      }

      if (pNode->next) {
        pNode->next->prev = pNode->prev;
      }
      released = 1;
    } else {
      uTrace("rsetId:%d p:%p rid:%" PRId64 " is released, remain count %d", rsetId, pNode->p, rid, pNode->count);
    }
  } else {
    uTrace("rsetId:%d rid:%" PRId64 " is not there, failed to release/remove", rsetId, rid);
    terrno = TSDB_CODE_REF_NOT_EXIST;
    code = -1;
  }

  taosUnlockList(pSet->lockedBy + hash);

  if (released) {
    uTrace("rsetId:%d p:%p rid:%" PRId64 " is removed, count:%d, free mem: %p", rsetId, pNode->p, rid, pSet->count,
           pNode);
    (*pSet->fp)(pNode->p);
    taosMemoryFree(pNode);

    taosDecRsetCount(pSet);
  }

  return code;
}

static void taosLockList(int64_t *lockedBy) {
  int64_t tid = taosGetSelfPthreadId();
  int32_t i = 0;
  while (atomic_val_compare_exchange_64(lockedBy, 0, tid) != 0) {
    if (++i % 100 == 0) {
      sched_yield();
    }
  }
}

static void taosUnlockList(int64_t *lockedBy) {
  int64_t tid = taosGetSelfPthreadId();
  if (atomic_val_compare_exchange_64(lockedBy, tid, 0) != tid) {
    ASSERTS(false, "atomic_val_compare_exchange_64 tid failed");
  }
}

static void taosInitRefModule(void) { taosThreadMutexInit(&tsRefMutex, NULL); }

static void taosIncRsetCount(SRefSet *pSet) {
  atomic_add_fetch_32(&pSet->count, 1);
  // uTrace("rsetId:%d inc count:%d", pSet->rsetId, count);
}

static void taosDecRsetCount(SRefSet *pSet) {
  int32_t count = atomic_sub_fetch_32(&pSet->count, 1);
  // uTrace("rsetId:%d dec count:%d", pSet->rsetId, count);

  if (count > 0) return;

  taosThreadMutexLock(&tsRefMutex);

  if (pSet->state != TSDB_REF_STATE_EMPTY) {
    pSet->state = TSDB_REF_STATE_EMPTY;
    pSet->max = 0;
    pSet->fp = NULL;

    taosMemoryFreeClear(pSet->nodeList);
    taosMemoryFreeClear(pSet->lockedBy);

    tsRefSetNum--;
    uTrace("rsetId:%d is cleaned, refSetNum:%d count:%d", pSet->rsetId, tsRefSetNum, pSet->count);
  }

  taosThreadMutexUnlock(&tsRefMutex);
}
