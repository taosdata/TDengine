#include <gtest/gtest.h>
#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

static void syncFreeObj(void *param);
int32_t     init();
void        cleanup();
int64_t     start();
void        stop(int64_t rid);

static int32_t tsNodeRefId = -1;
int            g = 100;

typedef struct SyncObj {
  int64_t rid;
  void   *data;
  char    name[32];
  int     counter;
} SyncObj;

static void syncFreeObj(void *param) {
  SyncObj *pObj = (SyncObj *)param;
  printf("syncFreeObj name:%s rid:%" PRId64 " \n", pObj->name, pObj->rid);
  taosMemoryFree(pObj);
}

int32_t init() {
  tsNodeRefId = taosOpenRef(200, syncFreeObj);
  if (tsNodeRefId < 0) {
    sError("failed to init node ref");
    cleanup();
    return -1;
  }
  return 0;
}

void cleanup() {
  if (tsNodeRefId != -1) {
    taosCloseRef(tsNodeRefId);
    tsNodeRefId = -1;
  }
}

int64_t start() {
  SyncObj *pObj = (SyncObj *)taosMemoryMalloc(sizeof(SyncObj));
  assert(pObj != NULL);

  pObj->data = &g;
  snprintf(pObj->name, sizeof(pObj->name), "%s", "hello");

  pObj->rid = taosAddRef(tsNodeRefId, pObj);
  if (pObj->rid < 0) {
    syncFreeObj(pObj);
    return -1;
  }

  printf("start name:%s rid:%" PRId64 " \n", pObj->name, pObj->rid);
  return pObj->rid;
}

void stop(int64_t rid) {
  SyncObj *pObj = (SyncObj *)taosAcquireRef(tsNodeRefId, rid);
  if (pObj == NULL) return;

  printf("stop name:%s rid:%" PRId64 " \n", pObj->name, pObj->rid);
  pObj->data = NULL;

  taosReleaseRef(tsNodeRefId, pObj->rid);
  taosRemoveRef(tsNodeRefId, rid);
}

void *func(void *param) {
  int64_t rid = (int64_t)param;

  int32_t ms = taosRand() % 10000;
  taosMsleep(ms);

  SyncObj *pObj = (SyncObj *)taosAcquireRef(tsNodeRefId, rid);
  if (pObj != NULL) {
    printf("taosAcquireRef sleep:%d, name:%s, rid:%" PRId64 " \n", ms, pObj->name, pObj->rid);
  } else {
    printf("taosAcquireRef sleep:%d, NULL! \n", ms);
  }

  taosReleaseRef(tsNodeRefId, rid);
  return NULL;
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  logTest();

  taosSeedRand(taosGetTimestampSec());

  int32_t ret;

  ret = init();
  assert(ret == 0);

  int64_t rid = start();
  assert(rid > 0);

  for (int i = 0; i < 20; ++i) {
    TdThread tid;
    taosThreadCreate(&tid, NULL, func, (void *)rid);
  }

  int32_t ms = taosRand() % 10000;
  taosMsleep(ms);
  printf("main sleep %d, stop and clean ", ms);

  stop(rid);
  cleanup();

  while (1) {
    taosMsleep(1000);
    printf("sleep 1 ... \n");
  }

  return 0;
}
