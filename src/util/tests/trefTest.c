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
  int  refNum;
  int  steps;
  int  refId;
  void **p;
} SRefSpace;

void iterateRefs(int refId) {
  int  count = 0;

  void *p = taosIterateRef(refId, NULL);
  while (p) {
    // process P
    count++;
    p = taosIterateRef(refId, p);
  }    

  printf(" %d ", count); 
}

void *takeRefActions(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;
  int code, id;

  for (int i=0; i < pSpace->steps; ++i) {
    printf("s");
    id = random() % pSpace->refNum; 
    code = taosAddRef(pSpace->refId, pSpace->p[id]);
    usleep(1);
    
    id = random() % pSpace->refNum; 
    code = taosAcquireRef(pSpace->refId, pSpace->p[id]);
    if (code >= 0) {
      usleep(id % 5 + 1);
      taosReleaseRef(pSpace->refId, pSpace->p[id]);
    }

    id = random() % pSpace->refNum; 
    taosRemoveRef(pSpace->refId, pSpace->p[id]);
    usleep(id %5 + 1);

    id = random() % pSpace->refNum; 
    code = taosAcquireRef(pSpace->refId, pSpace->p[id]);
    if (code >= 0) {
      usleep(id % 5 + 1);
      taosReleaseRef(pSpace->refId, pSpace->p[id]);
    }

    id = random() % pSpace->refNum; 
    iterateRefs(id);
  }  

  for (int i=0; i < pSpace->refNum; ++i) {
    taosRemoveRef(pSpace->refId, pSpace->p[i]);
  }
  
  //uInfo("refId:%d thread exits", pSpace->refId);

  return NULL;
}
       
void myfree(void *p) {
  return;
}

void *openRefSpace(void *param) {
  SRefSpace *pSpace = (SRefSpace *)param;

  printf("c");
  pSpace->refId = taosOpenRef(50, myfree);

  if (pSpace->refId < 0) {
    printf("failed to open ref, reson:%s\n", tstrerror(pSpace->refId));
    return NULL;
  } 

  pSpace->p = (void **) calloc(sizeof(void *), pSpace->refNum);
  for (int i=0; i<pSpace->refNum; ++i) {
    pSpace->p[i] = (void *) malloc(128);
  }

  pthread_attr_t thattr;
  pthread_attr_init(&thattr);
  pthread_attr_setdetachstate(&thattr, PTHREAD_CREATE_JOINABLE);

  pthread_t thread1, thread2, thread3;
  pthread_create(&(thread1), &thattr, takeRefActions, (void *)(pSpace));
  pthread_create(&(thread2), &thattr, takeRefActions, (void *)(pSpace));
  pthread_create(&(thread3), &thattr, takeRefActions, (void *)(pSpace));

  pthread_join(thread1, NULL);
  pthread_join(thread2, NULL);
  pthread_join(thread3, NULL);

  taosCloseRef(pSpace->refId);

  for (int i=0; i<pSpace->refNum; ++i) {
    free(pSpace->p[i]);
  }

  uInfo("refId:%d main thread exit", pSpace->refId);
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
      printf("  [-t]: number of refIds running in parallel, default: %d\n", threads);
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

