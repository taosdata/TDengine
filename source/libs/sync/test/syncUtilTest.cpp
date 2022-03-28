#include "syncUtil.h"
//#include <gtest/gtest.h>
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

void electRandomMSTest() {
  for (int i = 0; i < 10; ++i) {
    int32_t ms = syncUtilElectRandomMS(150, 300);
    printf("syncUtilElectRandomMS: %d \n", ms);
  }
}

int main() {
  // taosInitLog((char *)"syncTest.log", 100000, 10);
  tsAsyncLog = 0;
  sDebugFlag = 143 + 64;
  logTest();

  electRandomMSTest();

  return 0;
}
