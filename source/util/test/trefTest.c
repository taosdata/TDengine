#include "tref.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "os.h"
#include "taoserror.h"
#include "tglobal.h"
#include "tlog.h"

typedef struct {
  int      refNum;
  int      steps;
  int      rsetId;
  int64_t *rid;
  void   **p;
} SRefSpace;

void iterateRefs(int rsetId) {
  int count = 0;

  void *p = taosIterateRef(rsetId, 0);
  while (p) {
    // process P
    count++;
    p = taosIterateRef(rsetId, (int64_t)p);
  }

  printf(" %d ", count);
}

void *addRef(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int        id;

  for (int i = 0; i < pSpace->steps; ++i) {
    printf("a");
    id = taosRand() % pSpace->refNum;
    if (pSpace->rid[id] <= 0) {
      pSpace->p[id] = taosMemoryMalloc(128);
      pSpace->rid[id] = taosAddRef(pSpace->rsetId, pSpace->p[id]);
    }
    taosUsleep(100);
  }

  return NULL;
}

void *removeRef(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int        id, code;

  for (int i = 0; i < pSpace->steps; ++i) {
    printf("d");
    id = taosRand() % pSpace->refNum;
    if (pSpace->rid[id] > 0) {
      code = taosRemoveRef(pSpace->rsetId, pSpace->rid[id]);
      if (code == 0) pSpace->rid[id] = 0;
    }

    taosUsleep(100);
  }

  return NULL;
}

void *acquireRelease(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int        id;

  for (int i = 0; i < pSpace->steps; ++i) {
    printf("a");

    id = taosRand() % pSpace->refNum;
    void *p = taosAcquireRef(pSpace->rsetId, (int64_t)pSpace->p[id]);
    if (p) {
      taosUsleep(id % 5 + 1);
      taosReleaseRef(pSpace->rsetId, (int64_t)pSpace->p[id]);
    }
  }

  return NULL;
}

void myfree(void *p) { taosMemoryFree(p); }

void *openRefSpace(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;

  printf("c");
  pSpace->rsetId = taosOpenRef(50, myfree);

  if (pSpace->rsetId < 0) {
    printf("failed to open ref, reason:%s\n", tstrerror(pSpace->rsetId));
    return NULL;
  }

  pSpace->p = (void **)taosMemoryCalloc(sizeof(void *), pSpace->refNum);
  pSpace->rid = taosMemoryCalloc(pSpace->refNum, sizeof(int64_t));

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  TdThread thread1, thread2, thread3;
  taosThreadCreate(&(thread1), &thattr, addRef, (void *)(pSpace));
  taosThreadCreate(&(thread2), &thattr, removeRef, (void *)(pSpace));
  taosThreadCreate(&(thread3), &thattr, acquireRelease, (void *)(pSpace));

  taosThreadJoin(thread1, NULL);
  taosThreadJoin(thread2, NULL);
  taosThreadJoin(thread3, NULL);

  for (int i = 0; i < pSpace->refNum; ++i) {
    taosRemoveRef(pSpace->rsetId, pSpace->rid[i]);
  }

  taosCloseRef(pSpace->rsetId);

  uInfo("rsetId:%d main thread exit", pSpace->rsetId);
  taosMemoryFree(pSpace->p);
  pSpace->p = NULL;

  return NULL;
}

int main(int argc, char *argv[]) {
  int refNum = 100;
  int threads = 10;
  int steps = 10000;
  int loops = 1;

  uDebugFlag = 143;

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "-n") == 0 && i < argc - 1) {
      refNum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s") == 0 && i < argc - 1) {
      steps = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0 && i < argc - 1) {
      threads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-l") == 0 && i < argc - 1) {
      loops = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d") == 0 && i < argc - 1) {
      uDebugFlag = atoi(argv[i]);
    } else {
      printf("\nusage: %s [options] \n", argv[0]);
      printf("  [-n]: number of references, default: %d\n", refNum);
      printf("  [-s]: steps to run for each reference, default: %d\n", steps);
      printf("  [-t]: number of rsetIds running in parallel, default: %d\n", threads);
      printf("  [-l]: number of loops, default: %d\n", loops);
      printf("  [-d]: debugFlag, default: %d\n", uDebugFlag);
      exit(0);
    }
  }

  taosInitLog("tref.log", 10);

  SRefSpace *pSpaceList = (SRefSpace *)taosMemoryCalloc(sizeof(SRefSpace), threads);
  TdThread  *pThreadList = (TdThread *)taosMemoryCalloc(sizeof(TdThread), threads);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i = 0; i < loops; ++i) {
    printf("\nloop: %d\n", i);
    for (int j = 0; j < threads; ++j) {
      pSpaceList[j].steps = steps;
      pSpaceList[j].refNum = refNum;
      taosThreadCreate(&(pThreadList[j]), &thattr, openRefSpace, (void *)(pSpaceList + j));
    }

    for (int j = 0; j < threads; ++j) {
      taosThreadJoin(pThreadList[j], NULL);
    }
  }

  int num = taosListRef();
  printf("\nnumber of references:%d\n", num);

  taosMemoryFree(pSpaceList);
  taosMemoryFree(pThreadList);

  taosCloseLog();

  return num;
}
