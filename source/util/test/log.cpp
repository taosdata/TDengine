#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <random>
#include <tlog.h> 
#include <iostream> 

using namespace std;


TEST(log, check_log_refactor) {
  const char   *logDir = "/tmp";
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10000;
  tsAsyncLog = 0;
  // idxDebugFlag = 143;
  strcpy(tsLogDir, (char *)logDir);
  taosInitLog(tsLogDir, 10, false);
  tsAsyncLog = 0;
  uDebugFlag = 143;

  std::string str;
  str.push_back('a');
  
  for (int i = 0;  i < 10000; i += 2) {	 
    str.push_back('a');
    uError("write to file %s", str.c_str());
  }
  str.clear();
  for (int i = 0;  i < 10000; i += 2) {	 
    str.push_back('a');
    uDebug("write to file %s", str.c_str());
  }

  for (int i = 0;  i < 10000; i += 2) {	 
    str.push_back('a');
    uInfo("write to file %s", str.c_str());
  }
  str.clear();

  for (int i = 0;  i < 10000; i += 2) {	 
    str.push_back('a');
    uTrace("write to file %s", str.c_str());
  }
  taosCloseLog();
}
