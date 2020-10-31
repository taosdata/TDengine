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
  struct SRefNode  *prev;
  struct SRefNode  *next;
  void             *p;
  int32_t           count;
} SRefNode;
	
typedef struct {
  SRefNode **nodeList;
  int        state;  // 0: empty, 1: active;  2: deleted
  int        refId;
  int        max;
  int32_t    count;  // total number of SRefNodes in this set
  int64_t   *lockedBy;
  void     (*fp)(void *);
} SRefSet;

static SRefSet         tsRefSetList[TSDB_REF_OBJECTS];
static pthread_once_t  tsRefModuleInit = PTHREAD_ONCE_INIT;
static pthread_mutex_t tsRefMutex;
static int             tsRefSetNum = 0;
static int             tsNextId = 0;

static void taosInitRefModule(void);
static int  taosHashRef(SRefSet *pSet, void *p); 
static void taosLockList(int64_t *lockedBy);
static void taosUnlockList(int64_t *lockedBy);
static void taosIncRefCount(SRefSet *pSet);
static void taosDecRefCount(SRefSet *pSet);

int taosOpenRef(int max, void (*fp)(void *))
{
  SRefNode **nodeList;
  SRefSet   *pSet;
  int64_t   *lockedBy;
  int        i, refId;
  
  pthread_once(&tsRefModuleInit, taosInitRefModule);
   
  nodeList = calloc(sizeof(SRefNode *), (size_t)max);
  if (nodeList == NULL) {
    return TSDB_CODE_REF_NO_MEMORY;
  }

  lockedBy = calloc(sizeof(int64_t), (size_t)max);
  if (lockedBy == NULL) {
    free(nodeList);
    return TSDB_CODE_REF_NO_MEMORY;
  }

  pthread_mutex_lock(&tsRefMutex);
 
  for (i = 0; i < TSDB_REF_OBJECTS; ++i) {
    tsNextId = (tsNextId + 1) % TSDB_REF_OBJECTS;
    if (tsRefSetList[tsNextId].state == TSDB_REF_STATE_EMPTY) break;
  } 

  if (i < TSDB_REF_OBJECTS) {
    refId = tsNextId;
    pSet = tsRefSetList + refId;
    taosIncRefCount(pSet);
    pSet->max = max;
    pSet->nodeList = nodeList;
    pSet->lockedBy = lockedBy;
    pSet->fp = fp;
    pSet->state = TSDB_REF_STATE_ACTIVE;
    pSet->refId = refId;

    tsRefSetNum++;
    uTrace("refId:%d is opened, max:%d, fp:%p refSetNum:%d", refId, max, fp, tsRefSetNum);
  } else {
    refId = TSDB_CODE_REF_FULL;
    free (nodeList);
    free (lockedBy);
    uTrace("run out of Ref ID, maximum:%d refSetNum:%d", TSDB_REF_OBJECTS, tsRefSetNum);
  } 

  pthread_mutex_unlock(&tsRefMutex);

  return refId;
}

void taosCloseRef(int refId)
{
  SRefSet  *pSet;
  int       deleted = 0;

  if (refId < 0 || refId >= TSDB_REF_OBJECTS) {
    uTrace("refId:%d is invalid, out of range", refId);
    return;
  }

  pSet = tsRefSetList + refId;

  pthread_mutex_lock(&tsRefMutex);

  if (pSet->state == TSDB_REF_STATE_ACTIVE) { 
    pSet->state = TSDB_REF_STATE_DELETED;
    deleted = 1;
    uTrace("refId:%d is closed, count:%d", refId, pSet->count);
  } else {
    uTrace("refId:%d is already closed, count:%d", refId, pSet->count);
  }

  pthread_mutex_unlock(&tsRefMutex);

  if (deleted) taosDecRefCount(pSet);
}

int taosAddRef(int refId, void *p) 
{
  int       hash;
  SRefNode *pNode;
  SRefSet  *pSet;

  if (refId < 0 || refId >= TSDB_REF_OBJECTS) {
    uTrace("refId:%d p:%p failed to add, refId not valid", refId, p);
    return TSDB_CODE_REF_INVALID_ID;
  }

  pSet = tsRefSetList + refId;
  taosIncRefCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    taosDecRefCount(pSet);
    uTrace("refId:%d p:%p failed to add, not active", refId, p);
    return TSDB_CODE_REF_ID_REMOVED;
  }
  
  int code = 0;
  hash = taosHashRef(pSet, p);

  taosLockList(pSet->lockedBy+hash);
  
  pNode = pSet->nodeList[hash];
  while (pNode) {
    if (pNode->p == p)
      break;

    pNode = pNode->next;  
  }

  if (pNode) {
    code = TSDB_CODE_REF_ALREADY_EXIST;
    uTrace("refId:%d p:%p is already there, faild to add", refId, p);
  } else {
    pNode = calloc(sizeof(SRefNode), 1);
    if (pNode) {
      pNode->p = p;
      pNode->count = 1;
      pNode->prev = 0;
      pNode->next = pSet->nodeList[hash];
      if (pSet->nodeList[hash]) pSet->nodeList[hash]->prev = pNode;
      pSet->nodeList[hash] = pNode;
      uTrace("refId:%d p:%p is added, count:%d  malloc mem: %p", refId, p, pSet->count, pNode);
    } else {
      code = TSDB_CODE_REF_NO_MEMORY;
      uTrace("refId:%d p:%p is not added, since no memory", refId, p);
    }
  }

  if (code < 0) taosDecRefCount(pSet);

  taosUnlockList(pSet->lockedBy+hash);

  return code; 
}

int taosAcquireRef(int refId, void *p) 
{
  int       hash, code = 0;
  SRefNode *pNode;
  SRefSet  *pSet;

  if (refId < 0 || refId >= TSDB_REF_OBJECTS) {
    uTrace("refId:%d p:%p failed to acquire, refId not valid", refId, p);
    return TSDB_CODE_REF_INVALID_ID;
  }

  pSet = tsRefSetList + refId;
  taosIncRefCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    uTrace("refId:%d p:%p failed to acquire, not active", refId, p);
    taosDecRefCount(pSet);
    return TSDB_CODE_REF_ID_REMOVED;
  }
  
  hash = taosHashRef(pSet, p);

  taosLockList(pSet->lockedBy+hash);

  pNode = pSet->nodeList[hash];

  while (pNode) {
    if (pNode->p == p) {
      break;
    }
      
    pNode = pNode->next;   
  }

  if (pNode) {
    pNode->count++;
    uTrace("refId:%d p:%p is acquired", refId, p);
  } else {
    code = TSDB_CODE_REF_NOT_EXIST;
    uTrace("refId:%d p:%p is not there, failed to acquire", refId, p);
  }

  taosUnlockList(pSet->lockedBy+hash);

  taosDecRefCount(pSet);

  return code;
}

void taosReleaseRef(int refId, void *p) 
{
  int       hash;
  SRefNode *pNode;
  SRefSet  *pSet;
  int       released = 0;

  if (refId < 0 || refId >= TSDB_REF_OBJECTS) {
    uTrace("refId:%d p:%p failed to release, refId not valid", refId, p);
    return;
  }

  pSet = tsRefSetList + refId;
  if (pSet->state == TSDB_REF_STATE_EMPTY) {
    uTrace("refId:%d p:%p failed to release, cleaned", refId, p);
    return;
  }
  
  hash = taosHashRef(pSet, p);

  taosLockList(pSet->lockedBy+hash);
  
  pNode = pSet->nodeList[hash];
  while (pNode) {
    if (pNode->p == p)
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

      free(pNode);
      released = 1;
      uTrace("refId:%d p:%p is removed, count:%d, free mem: %p", refId, p, pSet->count, pNode);
    } else {
      uTrace("refId:%d p:%p is released", refId, p);
    }
  } else {
    uTrace("refId:%d p:%p is not there, failed to release", refId, p);
  }

  taosUnlockList(pSet->lockedBy+hash);

  if (released) taosDecRefCount(pSet);
}

// if p is NULL, return the first p in hash list, otherwise, return the next after p
void *taosIterateRef(int refId, void *p) {
  SRefNode *pNode = NULL;
  SRefSet  *pSet;

  if (refId < 0 || refId >= TSDB_REF_OBJECTS) {
    uTrace("refId:%d p:%p failed to iterate, refId not valid", refId, p);
    return NULL;
  }

  pSet = tsRefSetList + refId;
  taosIncRefCount(pSet);
  if (pSet->state != TSDB_REF_STATE_ACTIVE) {
    uTrace("refId:%d p:%p failed to iterate, not active", refId, p);
    taosDecRefCount(pSet);
    return NULL;
  }

  int hash = 0;
  if (p) {
    hash = taosHashRef(pSet, p);
    taosLockList(pSet->lockedBy+hash);

    pNode = pSet->nodeList[hash];
    while (pNode) {
      if (pNode->p == p) break;
      pNode = pNode->next;   
    }

    if (pNode == NULL) {
      uError("refId:%d p:%p not there, quit", refId, p);
      taosUnlockList(pSet->lockedBy+hash);
      return NULL;
    }

    // p is there
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
    uTrace("refId:%d p:%p is returned", refId, p);
  } else {
    uTrace("refId:%d p:%p the list is over", refId, p);
  }

  if (p) taosReleaseRef(refId, p);  // release the current one

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

    uInfo("refId:%d state:%d count::%d", i, pSet->state, pSet->count);

    for (int j=0; j < pSet->max; ++j) {
      pNode = pSet->nodeList[j];
     
      while (pNode) {
        uInfo("refId:%d p:%p count:%d", i, pNode->p, pNode->count);
        pNode = pNode->next;
        num++;
      }
    }  
  } 

  pthread_mutex_unlock(&tsRefMutex);

  return num;
}

static int taosHashRef(SRefSet *pSet, void *p) 
{
  int     hash = 0;
  int64_t v = (int64_t)p;
  
  for (int i = 0; i < sizeof(v); ++i) {
    hash += (int)(v & 0xFFFF);
    v = v >> 16;
    i = i + 2;
  }

  hash = hash % pSet->max;

  return hash;
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
  uTrace("refId:%d inc count:%d", pSet->refId, pSet->count);
}

static void taosDecRefCount(SRefSet *pSet) {
  int32_t count = atomic_sub_fetch_32(&pSet->count, 1);
  uTrace("refId:%d dec count:%d", pSet->refId, pSet->count);

  if (count > 0) return;

  pthread_mutex_lock(&tsRefMutex);

  if (pSet->state != TSDB_REF_STATE_EMPTY) {
    pSet->state = TSDB_REF_STATE_EMPTY;
    pSet->max = 0;
    pSet->fp = NULL;

    taosTFree(pSet->nodeList);
    taosTFree(pSet->lockedBy);

    tsRefSetNum--;
    uTrace("refId:%d is cleaned, refSetNum:%d count:%d", pSet->refId, tsRefSetNum, pSet->count);
  }

  pthread_mutex_unlock(&tsRefMutex);
} 

