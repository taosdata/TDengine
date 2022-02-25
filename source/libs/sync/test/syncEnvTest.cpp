#include "syncEnv.h"
#include <stdio.h>
#include "syncIO.h"
#include "syncInt.h"
#include "syncRaftStore.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

int main() {
  taosInitLog((char*)"syncEnvTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  int32_t ret = syncEnvStart();
  assert(ret == 0);

  ret = syncIOStart();
  assert(ret == 0);

  SSyncInfo syncInfo;
  syncInfo.vgId = 1;

  SSyncNode* pSyncNode = syncNodeStart(&syncInfo);
  assert(pSyncNode != NULL);

  while (1) {
    taosMsleep(1000);
  }

  return 0;
}
