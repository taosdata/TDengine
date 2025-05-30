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

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;

  logTest();

  int32_t ret;

  ret = syncIOStart((char*)"127.0.0.1", 7010);
  TD_ALWAYS_ASSERT(ret == 0);

  for (int i = 0; i < 3; ++i) {
    ret = syncIOPingTimerStart();
    TD_ALWAYS_ASSERT(ret == 0);
    taosMsleep(5000);

    ret = syncIOPingTimerStop();
    TD_ALWAYS_ASSERT(ret == 0);
    taosMsleep(5000);
  }

  while (1) {
    taosSsleep(1);
  }
  return 0;
}
