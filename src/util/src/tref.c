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
#include "taoserror.h"
#include "tulog.h"
#include "tutil.h"

#define TSDB_REF_OBJECTS       50
#define TSDB_REF_STATE_EMPTY   0
#define TSDB_REF_STATE_ACTIVE  1
#define TSDB_REF_STATE_DELETED 2

typedef struct SRefNode {
  struct SRefNode  *prev;  // previous node 
  struct SRefNode  *next;  // next node
  void             *p;     // pointer to resource protected, 
  int64_t           rid;   // reference ID
  int32_t           count; // number of references
} SRefNode;
	
typedef struct {
  SRefNode **nodeList; // array of SRefNode linked list
  int        state;    // 0: empty, 1: active;  2: deleted
  int        rsetId;   // refSet ID, global unique 
  int64_t    rid;      // increase by one for each new reference
  int        max;      // mod 
  int32_t    count;    // total number of SRefNodes in this set
  int64_t   *lockedBy; 
  void     (*fp)(void *);
} SRefSet;

static SRefSet         tsRefSetList[TSDB_REF_OBJECTS];
static pthread_once_t  tsRefModuleInit = PTHREAD_ONCE_INIT;
static pthread_mutex_t tsRefMutex;
static int             tsRefSetNum = 0;
static int             tsNextId = 0;

static void taosInitRefModule(void);
static void taosLockList(int64_t *lockedBy);
static void taosUnlockList(int64_t *lockedBy);
static void taosIncRefCount(SRefSet *pSet);
static void taosDecRefCount(SRefSet *pSet);

int taosOpenRef(int max, void (*fp)(void *))
{
  SRefNode **nodeList;
  SRefSet   *pSet;
  int64_t   *lockedBy;
  int        i, rsetId;
  
  pthread_once(&tsRefModuleInit, taosInitRefModule);
   
  nodeList = calloc(sizeof(SRefNode *), (size_t)max);
  if (nodeList == NULL)  {
    terrno = TSDB_CODE_REF_NO_MEMORY;
    return -1;
  }

  lockedBy = calloc(sizeof(int64_t), (size_t)max);
  if (lockedBy == NULL) {
    free(nodeList);
    terrno = TSDB_CODE_REF_NO_MEMORY;
    return -1;
  }

  pthread_mutex_lock(&tsRefMutex);
 
  for (i = 0; i < TSDB_REF_OBJECTS; ++i) {
    tsNextId = (tsNextId + 1) % TSDB_REF_OBJECTS;
    if (tsRefSetList[tsNextId].state == TSDB_REF_STATE_EMPTY) break;
  } 

  if (i < TSDB_REF_OBJECTS) {
    rsetId = tsNextId;
    pSet = tsRefSetList + rsetId;
    taosIncRefCount(pSet);
    pSet->max = max;
    pSet->nodeList = nodeList;
    pSet->lockedBy = lockedBy;
    pSet->fp = fp;
    pSet->rid = 1;
    pSet->rsetId = rsetId;
    pSet->state = TSDB_REF_STATE_ACTIVE;

    tsRefSetNum++;
    uTrace("rsetId:%d is opened, max:%d, fp:%p refSetNum:%d", rsetId, max, fp, tsRefSetNum);
  } else {
    rsetId = TSDB_CODE_REF_FULL;
    free (nodeList);
    free (lockedBy);
    uTrace("run out of Ref ID, maximum:%d refSetNum:%d", TSDB_REF_OBJECTS, tsRefSetNum);
  } 

  pthread_mutex_unlock(&tsRefMutex);

  return rsetId;
}

int taosCloseRef(int rsetId)
{
  SRefSet  *pSet;
  int       deleted = 0;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d is invalid, out of range", rsetId);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return -1;
  }

  pSet = tsRefSetList + rsetId;

  pthread_mutex_lock(&tsRefMutex);

  if (pSet->state == TSDB_REF_STATE_ACTIVE) { 
    pSet->state = TSDB_REF_STATE_DELETED;
    deleted = 1;
    uTrace("rsetId:%d is closed, count:%d", rsetId, pSet->count);
  } else {
    uTrace("rsetId:%d is already closed, count:%d", rsetId, pSet->count);
  }

  pthread_mutex_unlock(&tsRefMutex);

  if (deleted) taosDecRefCount(pSet);

  return 0;
}

int64_t taosAddRef(int rsetId, void *p) 
{
  int       hash;
  SRefNode *pNode;
  SRefSet  *pSet;
  int64_t   rid = 0;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d p:%p failed to add, rsetId not valid", rsetId, p);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return -1;
  }

  pSet = tsRefSetList + rsetId;
  taosIncRefCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    taosDecRefCount(pSet);
    uTrace("rsetId:%d p:%p failed to add, not active", rsetId, p);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    return -1;
  }
  
  pNode = calloc(sizeof(SRefNode), 1);
  if (pNode == NULL) {
    terrno = TSDB_CODE_REF_NO_MEMORY;
    return -1;
  }

  rid = atomic_add_fetch_64(&pSet->rid, 1);
  hash = rid % pSet->max;
  taosLockList(pSet->lockedBy+hash);
  
  pNode->p = p;
  pNode->rid = rid;
  pNode->count = 1;

  pNode->prev = NULL;
  pNode->next = pSet->nodeList[hash];
  if (pSet->nodeList[hash]) pSet->nodeList[hash]->prev = pNode;
  pSet->nodeList[hash] = pNode;

  uTrace("rsetId:%d p:%p rid:%" PRId64 " is added, count:%d", rsetId, p, rid, pSet->count);

  taosUnlockList(pSet->lockedBy+hash);

  return rid; 
}

void *taosAcquireRef(int rsetId, int64_t rid) 
{
  int       hash;
  SRefNode *pNode;
  SRefSet  *pSet;
  void     *p = NULL;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to acquire, rsetId not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return NULL;
  }

  pSet = tsRefSetList + rsetId;
  taosIncRefCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to acquire, not active", rsetId, rid);
    taosDecRefCount(pSet);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    return NULL;
  }
  
  hash = rid % pSet->max;
  taosLockList(pSet->lockedBy+hash);

  pNode = pSet->nodeList[hash];

  while (pNode) {
    if (pNode->rid == rid) {
      break;
    }
      
    pNode = pNode->next;   
  }

  if (pNode) {
    pNode->count++;
    p = pNode->p;
    uTrace("rsetId:%d p:%p rid:%" PRId64 " is acquired", rsetId, pNode->p, rid);
  } else {
    terrno = TSDB_CODE_REF_NOT_EXIST;
    uTrace("rsetId:%d rid:%" PRId64 " is not there, failed to acquire", rsetId, rid);
  }

  taosUnlockList(pSet->lockedBy+hash);

  taosDecRefCount(pSet);

  return p;
}

int taosReleaseRef(int rsetId, int64_t rid) 
{
  int       hash;
  SRefNode *pNode;
  SRefSet  *pSet;
  int       released = 0;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to release, rsetId not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return -1;
  }

  pSet = tsRefSetList + rsetId;
  if (pSet->state == TSDB_REF_STATE_EMPTY) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to release, cleaned", rsetId, rid);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    return -1;
  }
  
  terrno = 0;
  hash = rid % pSet->max;
  taosLockList(pSet->lockedBy+hash);
  
  pNode = pSet->nodeList[hash];
  while (pNode) {
    if (pNode->rid == rid)
      break;

    pNode = pNode->next;  
  }

  if (pNode) {
    pNode->count--;

    if (pNode->count == 0) {
      if ( pNode->prev ) {
        pNode->prev->next = pNode->next;
      } else {
        pSet->nodeList[hash] = pNode->next;  
      }
		
      if ( pNode->next ) {
        pNode->next->prev = pNode->prev; 
      } 
		
      (*pSet->fp)(pNode->p); 

      uTrace("rsetId:%d p:%p rid:%" PRId64 "is removed, count:%d, free mem: %p", rsetId, pNode->p, rid, pSet->count, pNode);
      free(pNode);
      released = 1;
    } else {
      uTrace("rsetId:%d p:%p rid:%" PRId64 "is released", rsetId, pNode->p, rid);
    }
  } else {
    uTrace("rsetId:%d rid:%" PRId64 " is not there, failed to release", rsetId, rid);
    terrno = TSDB_CODE_REF_NOT_EXIST;
  }

  taosUnlockList(pSet->lockedBy+hash);

  if (released) taosDecRefCount(pSet);

  return terrno;
}

// if rid is 0, return the first p in hash list, otherwise, return the next after current rid
void *taosIterateRef(int rsetId, int64_t rid) {
  SRefNode *pNode = NULL;
  SRefSet  *pSet;

  if (rsetId < 0 || rsetId >= TSDB_REF_OBJECTS) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to iterate, rsetId not valid", rsetId, rid);
    terrno = TSDB_CODE_REF_INVALID_ID;
    return NULL;
  }

  pSet = tsRefSetList + rsetId;
  taosIncRefCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    uTrace("rsetId:%d rid:%" PRId64 " failed to iterate, rset not active", rsetId, rid);
    terrno = TSDB_CODE_REF_ID_REMOVED;
    taosDecRefCount(pSet);
    return NULL;
  }

  int hash = 0;
  if (rid > 0) {
    hash = rid % pSet->max;
    taosLockList(pSet->lockedBy+hash);

    pNode = pSet->nodeList[hash];
    while (pNode) {
      if (pNode->rid == rid) break;
      pNode = pNode->next;   
    }

    if (pNode == NULL) {
      uError("rsetId:%d rid:%" PRId64 " not there, quit", rsetId, rid);
      terrno = TSDB_CODE_REF_NOT_EXIST;
      taosUnlockList(pSet->lockedBy+hash);
      return NULL;
    }

    // rid is there
    pNode = pNode->next;
    if (pNode == NULL) { 
      taosUnlockList(pSet->lockedBy+hash);
      hash++;
    }
  }

  if (pNode == NULL) {
    for (; hash < pSet->max; ++hash) {
      taosLockList(pSet->lockedBy+hash);
      pNode = pSet->nodeList[hash];
      if (pNode) break; 
      taosUnlockList(pSet->lockedBy+hash);
    }
  } 

  void *newP = NULL;
  if (pNode) {
    pNode->count++;  // acquire it
    newP = pNode->p;  
    taosUnlockList(pSet->lockedBy+hash);
    uTrace("rsetId:%d p:%p rid:%" PRId64 " is returned", rsetId, newP, rid);
  } else {
    uTrace("rsetId:%d the list is over", rsetId);
  }

  if (rid > 0) taosReleaseRef(rsetId, rid);  // release the current one

  taosDecRefCount(pSet);

  return newP;
}

int taosListRef() {
  SRefSet  *pSet;
  SRefNode *pNode;
  int       num = 0;

  pthread_mutex_lock(&tsRefMutex);

  for (int i = 0; i < TSDB_REF_OBJECTS; ++i) {
    pSet = tsRefSetList + i;
    
    if (pSet->state == TSDB_REF_STATE_EMPTY) 
      continue;

    uInfo("rsetId:%d state:%d count::%d", i, pSet->state, pSet->count);

    for (int j=0; j < pSet->max; ++j) {
      pNode = pSet->nodeList[j];
     
      while (pNode) {
        uInfo("rsetId:%d p:%p rid:%" PRId64 "count:%d", i, pNode->p, pNode->rid, pNode->count);
        pNode = pNode->next;
        num++;
      }
    }  
  } 

  pthread_mutex_unlock(&tsRefMutex);

  return num;
}

static void taosLockList(int64_t *lockedBy) {
  int64_t tid = taosGetPthreadId();
  int     i = 0;
  while (atomic_val_compare_exchange_64(lockedBy, 0, tid) != 0) {
    if (++i % 100 == 0) {
      sched_yield();
    }
  }
}

static void taosUnlockList(int64_t *lockedBy) {
  int64_t tid = taosGetPthreadId();
  if (atomic_val_compare_exchange_64(lockedBy, tid, 0) != tid) {
    assert(false);
  }
}

static void taosInitRefModule(void) {
  pthread_mutex_init(&tsRefMutex, NULL);
}

static void taosIncRefCount(SRefSet *pSet) {
  atomic_add_fetch_32(&pSet->count, 1);
  uTrace("rsetId:%d inc count:%d", pSet->rsetId, pSet->count);
}

static void taosDecRefCount(SRefSet *pSet) {
  int32_t count = atomic_sub_fetch_32(&pSet->count, 1);
  uTrace("rsetId:%d dec count:%d", pSet->rsetId, pSet->count);

  if (count > 0) return;

  pthread_mutex_lock(&tsRefMutex);

  if (pSet->state != TSDB_REF_STATE_EMPTY) {
    pSet->state = TSDB_REF_STATE_EMPTY;
    pSet->max = 0;
    pSet->fp = NULL;

    taosTFree(pSet->nodeList);
    taosTFree(pSet->lockedBy);

    tsRefSetNum--;
    uTrace("rsetId:%d is cleaned, refSetNum:%d count:%d", pSet->rsetId, tsRefSetNum, pSet->count);
  }

  pthread_mutex_unlock(&tsRefMutex);
} 

