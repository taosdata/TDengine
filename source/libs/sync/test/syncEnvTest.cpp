#include "syncTest.h"

void logTest() {
  sTrace("--- sync log test: trace");
  sDebug("--- sync log test: debug");
  sInfo("--- sync log test: info");
  sWarn("--- sync log test: warn");
  sError("--- sync log test: error");
  sFatal("--- sync log test: fatal");
}

int main() {
  // taosInitLog((char*)"syncEnvTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  int32_t ret;

  logTest();

  ret = syncInit();
  assert(ret == 0);

  for (int i = 0; i < 5; ++i) {
    // ret = syncEnvStartTimer();
    assert(ret == 0);

    taosMsleep(5000);

    // ret = syncEnvStopTimer();
    assert(ret == 0);

    taosMsleep(5000);
  }

  syncCleanUp();
  return 0;
}
