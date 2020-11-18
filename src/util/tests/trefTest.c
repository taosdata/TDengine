#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include "os.h"
#include "tref.h"
#include "tlog.h"
#include "tglobal.h"
#include "taoserror.h"
#include "tulog.h"

typedef struct {
  int     refNum;
  int     steps;
  int     rsetId;
  int64_t rid;
  void  **p;
} SRefSpace;

void iterateRefs(int rsetId) {
  int  count = 0;

  void *p = taosIterateRef(rsetId, NULL);
  while (p) {
    // process P
    count++;
    p = taosIterateRef(rsetId, p);
  }    

  printf(" %d ", count); 
}

void *addRef(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int id;
  int64_t rid;

  for (int i=0; i < pSpace->steps; ++i) {
    printf("a");
    id = random() % pSpace->refNum; 
    if (pSpace->rid[id] <= 0) {
      pSpace->p[id] = malloc(128);
      pSpace->rid[id] = taosAddRef(pSpace->rsetId, pSpace->p[id]);
    }
    usleep(100);
  }  

  return NULL;
}
       
void *removeRef(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int id;
  int64_t rid;

  for (int i=0; i < pSpace->steps; ++i) {
    printf("d");
    id = random() % pSpace->refNum; 
    if (pSpace->rid[id] > 0) {
      code = taosRemoveRef(pSpace->rsetId, pSpace->rid[id]);
      if (code == 0) pSpace->rid[id] = 0;
    }

    usleep(100);
  }  

  return NULL;
}
       
void *acquireRelease(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int id;
  int64_t rid;

  for (int i=0; i < pSpace->steps; ++i) {
    printf("a");
    
    id = random() % pSpace->refNum; 
    void *p = taosAcquireRef(pSpace->rsetId, pSpace->p[id]);
    if (p) {
      usleep(id % 5 + 1);
      taosReleaseRef(pSpace->rsetId, pSpace->p[id]);
    }
  }  

  return NULL;
}
       
void myfree(void *p) {
  free(p);
}

void *openRefSpace(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;

  printf("c");
  pSpace->rsetId = taosOpenRef(50, myfree);

  if (pSpace->rsetId < 0) {
    printf("failed to open ref, reson:%s\n", tstrerror(pSpace->rsetId));
    return NULL;
  } 

  pSpace->p = (void **) calloc(sizeof(void *), pSpace->refNum);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_t thread1, thread2, thread3;
  pthread_create(&(thread1), &thattr, addRef, (void *)(pSpace));
  pthread_create(&(thread2), &thattr, removeRef, (void *)(pSpace));
  pthread_create(&(thread3), &thattr, acquireRelease, (void *)(pSpace));

  pthread_join(thread1, NULL);
  pthread_join(thread2, NULL);
  pthread_join(thread3, NULL);

  for (int i=0; i<pSpace->refNum; ++i) {
    taosRemoveRef(pSpace->rsetId, pSpace->rid[i]);
  }

  taosCloseRef(pSpace->rsetId);

  uInfo("rsetId:%d main thread exit", pSpace->rsetId);
  free(pSpace->p);
  pSpace->p = NULL;

  return NULL;
}

int main(int argc, char *argv[]) {
  int refNum = 100;
  int threads = 10;
  int steps = 10000;  
  int loops = 1;

  uDebugFlag = 143;

  for (int i=1; i<argc; ++i) {
    if (strcmp(argv[i], "-n")==0 && i < argc-1) {
      refNum = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-s")==0 && i < argc-1) {
      steps = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-t")==0 && i < argc-1) {
      threads = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-l")==0 && i < argc-1) {
      loops = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-d")==0 && i < argc-1) {
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

  taosInitLog("tref.log", 5000000, 10);

  SRefSpace *pSpaceList = (SRefSpace *) calloc(sizeof(SRefSpace), threads);
  pthread_t *pThreadList = (pthread_t *) calloc(sizeof(pthread_t), threads);

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i=0; i<loops; ++i) {
    printf("\nloop: %d\n", i);
    for (int j=0; j<threads; ++j) {
      pSpaceList[j].steps = steps;
      pSpaceList[j].refNum = refNum;
      pthread_create(&(pThreadList[j]), &thattr, openRefSpace, (void *)(pSpaceList+j));
    }

    for (int j=0; j<threads; ++j) {
      pthread_join(pThreadList[j], NULL);
    }
  }

  int num = taosListRef();
  printf("\nnumber of references:%d\n", num);

  free(pSpaceList);
  free(pThreadList);

  taosCloseLog();

  return num;
}

