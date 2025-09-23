#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <random>
#include <tdef.h>
#include <tlog.h> 
#include <tlogInt.h> 
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

extern char *tsLogOutput;
static void *taosLogCrashMockFunc(void *param) {
  printf("%s:%d entry\n", __func__, __LINE__);
  setThreadName("logCrashMockThread");
  writeCrashLogToFile(0, nullptr, (char *)"unit_test_diff_thread", 0, 0);
  printf("%s:%d end\n", __func__, __LINE__);
  return NULL;
}
static void *taosLogCrashReportFunc(void *param) {
  printf("%s:%d entry\n", __func__, __LINE__);
  setThreadName("logCrashReportThread");
  taosSsleep(2); // wait for logCrashMockFunc ready
  checkAndPrepareCrashInfo();
  printf("%s:%d end\n", __func__, __LINE__);
  return NULL;
}
TEST(log, misc) {
  // taosInitLog
  const char *path = TD_TMP_DIR_PATH "td";
  taosRemoveDir(path);
  taosMkDir(path);
  tstrncpy(tsLogDir, path, PATH_MAX);
  EXPECT_EQ(taosInitLog("taoslog", 1, true), 0);

  taosOpenNewSlowLogFile();
  taosLogObjSetToday(0);
  taosPrintSlowLog("slow log test");

  // test taosInitLogOutput
  const char *pLogName = NULL;
  tsLogOutput = (char *)taosMemCalloc(1, TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), TSDB_CODE_INVALID_CFG);
  tstrncpy(tsLogOutput, "stdout", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tstrncpy(tsLogOutput, "stderr", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tstrncpy(tsLogOutput, "/dev/null", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tsLogOutput[0] = '#';
  EXPECT_EQ(taosInitLogOutput(&pLogName), TSDB_CODE_INVALID_CFG);
  tstrncpy(tsLogOutput, "/", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tstrncpy(tsLogOutput, "\\", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tstrncpy(tsLogOutput, "testLogOutput", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tstrncpy(tsLogOutput, "testLogOutputDir/testLogOutput", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), 0);
  tstrncpy(tsLogOutput, ".", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), TSDB_CODE_INVALID_CFG);
  tstrncpy(tsLogOutput, "/..", TSDB_FILENAME_LEN);
  EXPECT_EQ(taosInitLogOutput(&pLogName), TSDB_CODE_INVALID_CFG);
  tsLogOutput[0] = 0;

  // test taosAssertDebug
  tsAssert = false;
  taosAssertDebug(true, __FILE__, __LINE__, 0, "test_assert_true_without_core");
  taosAssertDebug(false, __FILE__, __LINE__, 0, "test_assert_false_with_core");
  tsAssert = true;

#ifdef USE_REPORT
  // test taosLogCrashInfo, taosReadCrashInfo and taosReleaseCrashLogFile
  char  nodeType[16] = "nodeType";
  char *pCrashMsg = (char *)taosMemoryCalloc(1, 16);
  EXPECT_NE(pCrashMsg, nullptr);
  tstrncpy(pCrashMsg, "crashMsg", 16);

#if !defined(_TD_DARWIN_64) && !defined(WINDOWS)
  pid_t pid = taosGetPId();
  EXPECT_EQ(pid > 0, true);
  siginfo_t sigInfo = {0};
  sigInfo.si_pid = pid;
  taosLogCrashInfo(nodeType, pCrashMsg, strlen(pCrashMsg), 0, &sigInfo);
#else
  taosLogCrashInfo(nodeType, pCrashMsg, strlen(pCrashMsg), 0, nullptr);
#endif

  char crashInfo[PATH_MAX] = {0};
  snprintf(crashInfo, sizeof(crashInfo), "%s%s.%sCrashLog", tsLogDir, TD_DIRSEP, nodeType);

  char     *pReadMsg = NULL;
  int64_t   readMsgLen = 0;
  TdFilePtr pFile = NULL;
  taosReadCrashInfo(crashInfo, &pReadMsg, &readMsgLen, &pFile);
  EXPECT_NE(pReadMsg, nullptr);
  EXPECT_NE(pFile, nullptr);
  EXPECT_EQ(strncasecmp(pReadMsg, "crashMsg", strlen("crashMsg")), 0);
  EXPECT_EQ(taosCloseFile(&pFile), 0);
  taosMemoryFreeClear(pReadMsg);

  pFile = taosOpenFile(crashInfo, TD_FILE_WRITE);
  EXPECT_NE(pFile, nullptr);
  EXPECT_EQ(taosWriteFile(pFile, "00000", 1), 1);
  EXPECT_EQ(taosCloseFile(&pFile), 0);

  taosReadCrashInfo(crashInfo, &pReadMsg, &readMsgLen, &pFile);
  EXPECT_EQ(pReadMsg, nullptr);
  EXPECT_EQ(pFile, nullptr);

  pFile = taosOpenFile(crashInfo, TD_FILE_WRITE);
  EXPECT_NE(pFile, nullptr);
  taosReleaseCrashLogFile(pFile, true);

  // test initCrashLogWriter/writeCrashLogToFile
  EXPECT_EQ(initCrashLogWriter(), 0);
  writeCrashLogToFile(0, nullptr, (char *)"unit_test_same_thread", 0, 0);

  TdThread     mockThread, reportThread;
  TdThreadAttr threadAttr;
  EXPECT_EQ(taosThreadAttrInit(&threadAttr), 0);
  EXPECT_EQ(taosThreadCreate(&mockThread, &threadAttr, taosLogCrashMockFunc, NULL), 0);
  EXPECT_EQ(taosThreadCreate(&reportThread, &threadAttr, taosLogCrashReportFunc, NULL), 0);
  EXPECT_EQ(taosThreadJoin(mockThread, NULL), 0);
  EXPECT_EQ(taosThreadJoin(reportThread, NULL), 0);
  EXPECT_EQ(taosThreadAttrDestroy(&threadAttr), 0);
#endif
  // clean up
  taosRemoveDir(path);

  taosCloseLog();
}

TEST(log, test_u64toa) {
  char buf[64] = {0};
  char *p = buf;

  p = u64toaFastLut(0, buf);
  EXPECT_EQ(p, buf + 1);
  EXPECT_EQ(strcmp(buf, "0"), 0);

  p = u64toaFastLut(1, buf);
  EXPECT_EQ(p, buf + 1);
  EXPECT_EQ(strcmp(buf, "1"), 0);

  p = u64toaFastLut(12, buf);
  EXPECT_EQ(p, buf + 2);
  EXPECT_EQ(strcmp(buf, "12"), 0);

  p = u64toaFastLut(12345, buf);
  EXPECT_EQ(p, buf + 5);
  EXPECT_EQ(strcmp(buf, "12345"), 0);

  p = u64toaFastLut(1234567890, buf);
  EXPECT_EQ(p, buf + 10);
  EXPECT_EQ(strcmp(buf, "1234567890"), 0);
}
