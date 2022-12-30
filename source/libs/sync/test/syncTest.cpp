#include "syncTest.h"
// #include <gtest/gtest.h>

/*
typedef enum {
  DEBUG_FATAL = 1,
  DEBUG_ERROR = 1,
  DEBUG_WARN = 2,
  DEBUG_INFO = 2,
  DEBUG_DEBUG = 4,
  DEBUG_TRACE = 8,
  DEBUG_DUMP = 16,
  DEBUG_SCREEN = 64,
  DEBUG_FILE = 128
} ELogLevel;
*/

void logTest(char* s) {
  sFatal("==%s== sync log test: fatal", s);
  sError("==%s== sync log test: error", s);

  sWarn("==%s== sync log test: warn", s);
  sInfo("==%s== sync log test: info", s);

  sDebug("==%s== sync log test: debug", s);

  sTrace("==%s== sync log test: trace", s);
}

void test1() {
  sDebugFlag = DEBUG_TRACE + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

void test2() {
  sDebugFlag = DEBUG_DEBUG + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

void test3() {
  sDebugFlag = DEBUG_INFO + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

void test4() {
  sDebugFlag = DEBUG_ERROR + DEBUG_SCREEN + DEBUG_FILE;
  logTest((char*)__FUNCTION__);
}

int main(int argc, char** argv) {
  taosInitLog("/tmp/syncTest.log", 100);
  tsAsyncLog = 0;
  sDebugFlag = DEBUG_SCREEN + DEBUG_FILE + DEBUG_TRACE + DEBUG_INFO + DEBUG_ERROR;

  test1();
  test2();
  test3();
  test4();

  /*
  if (argc == 2) {
    bool bTaosDirExist = taosDirExist(argv[1]);
    printf("%s bTaosDirExist:%d \n", argv[1], bTaosDirExist);

    bool bTaosCheckExistFile = taosCheckExistFile(argv[1]);
    printf("%s bTaosCheckExistFile:%d \n", argv[1], bTaosCheckExistFile);
  }
  */

  taosCloseLog();
  return 0;
}
