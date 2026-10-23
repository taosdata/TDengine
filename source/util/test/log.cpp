/*
 * Copyright (c) 2024 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <gtest/gtest.h>
#ifdef LINUX
#include <errno.h>
#include <stddef.h>
#include <linux/filter.h>
#include <linux/seccomp.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#endif
#include <stdlib.h>
#include <time.h>
#include <random>
#include <os.h>
#include <taoserror.h>
#include <tdef.h>
#include <tlog.h> 
#include <tlogInt.h> 
#include <iostream> 

using namespace std;

#if defined(LINUX) && GTEST_HAS_DEATH_TEST
class LogChmodDeathTest : public ::testing::TestWithParam<bool> {};

TEST_P(LogChmodDeathTest, chmod_failure_is_nonfatal) {
  bool useSymlink = GetParam();
  char logDir[PATH_MAX] = {0};
  taosGetTmpfilePath(TD_TMP_DIR_PATH, "logChmodTest", logDir);
  ASSERT_EQ(taosMkDir(logDir), 0) << terrstr();

  char logFile[PATH_MAX + 32] = {0};
  snprintf(logFile, sizeof(logFile), "%s/chmodTest.0", logDir);
  if (useSymlink) {
    ASSERT_EQ(taosSymLink(logFile, logFile), 0) << terrstr();
  } else {
    TdFilePtr pFile = taosOpenFile(logFile, TD_FILE_CREATE | TD_FILE_WRITE);
    ASSERT_NE(pFile, nullptr) << terrstr();
    ASSERT_EQ(taosCloseFile(&pFile), 0);
  }

  // Confine the injected fchmod failure and logger state to the child process.
  EXPECT_EXIT(
      ([&]() {
        struct sock_filter instructions[] = {
            BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
            BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, SYS_fchmod, 0, 1),
            BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM),
            BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
        };
        struct sock_fprog filter = {
            static_cast<unsigned short>(sizeof(instructions) / sizeof(instructions[0])),
            instructions,
        };
        EXPECT_EQ(prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0), 0);
        if (::testing::Test::HasFailure()) _exit(1);
        EXPECT_EQ(prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &filter), 0);
        if (::testing::Test::HasFailure()) _exit(1);

        tstrncpy(tsLogDir, logDir, PATH_MAX);
        tsAsyncLog = 0;
        terrno = 0;
        EXPECT_EQ(taosInitLog("chmodTest", 1, true), 0) << terrstr();
        if (::testing::Test::HasFailure()) _exit(1);
        // The missing .1 file or successful symlink retry leaves this error context.
        EXPECT_EQ(terrno, TAOS_SYSTEM_ERROR(useSymlink ? ELOOP : ENOENT));
        taosPrintLog("UTL INFO ", DEBUG_INFO, DEBUG_FILE,
                     "normal log remains writable after fchmod fails");
        taosPrintSlowLog("log remains writable after fchmod fails");
        taosCloseLog();

        snprintf(tsLogDir, PATH_MAX, "%s/missing", logDir);
        EXPECT_EQ(taosInitLog("chmodTest", 1, false),
                  TAOS_SYSTEM_ERROR(ENOENT));
        taosCloseLog();
        _exit(::testing::Test::HasFailure() ? 1 : 0);
      }()),
      ::testing::ExitedWithCode(0),
      "WARNING: failed to set log file permissions");

  TdFilePtr pFile = taosOpenFile(logFile, TD_FILE_READ);
  EXPECT_NE(pFile, nullptr) << terrstr();
  if (pFile != nullptr) {
    char contents[1024] = {0};
    EXPECT_GT(taosReadFile(pFile, contents, sizeof(contents) - 1), 0);
    EXPECT_NE(strstr(contents, "normal log remains writable after fchmod fails"), nullptr);
    EXPECT_EQ(taosCloseFile(&pFile), 0);
  }
  taosRemoveDir(logDir);
}

INSTANTIATE_TEST_SUITE_P(log, LogChmodDeathTest, ::testing::Values(false, true));
#endif


TEST(log, check_log_refactor) {
  char logDir[PATH_MAX] = {0};
  taosGetTmpfilePath(TD_TMP_DIR_PATH, "logTest", logDir);
  ASSERT_EQ(taosMkDir(logDir), 0) << terrstr();
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10000;
  tsAsyncLog = 0;
  // idxDebugFlag = 143;
  tstrncpy(tsLogDir, logDir, PATH_MAX);
  ASSERT_EQ(taosInitLog("taoslog", 10, false), 0) << terrstr();
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
  taosRemoveDir(logDir);
}

TEST(log, rotate_by_real_lines) {
  // RAII guard: this test mutates process-global log state (tsNumOfLogLines / tsLogDir / the initialized
  // log handle). If any ASSERT/EXPECT aborts the test early, the globals must still be restored so later
  // cases (e.g. TEST(log, misc)) are not polluted by a leftover tsNumOfLogLines=100 or a stray log dir.
  struct LogStateGuard {
    char    savedLogDir[PATH_MAX] = {0};
    int32_t savedLines = 0;
    bool    savedAsync = false;
    bool    savedDebugFile = false;
    char    logDir[PATH_MAX] = {0};
    ~LogStateGuard() {
      taosCloseLog();
      tsNumOfLogLines = savedLines;
      tsAsyncLog = savedAsync;
      tstrncpy(tsLogDir, savedLogDir, PATH_MAX);
      taosRemoveDir(logDir);
    }
  } guard;

  taosGetTmpfilePath(TD_TMP_DIR_PATH, "logLinesTest", guard.logDir);
  tstrncpy(guard.savedLogDir, tsLogDir, PATH_MAX);
  guard.savedLines = tsNumOfLogLines;
  guard.savedAsync = tsAsyncLog;
  ASSERT_EQ(taosMkDir(guard.logDir), 0) << terrstr();
  tstrncpy(tsLogDir, guard.logDir, PATH_MAX);
  tsAsyncLog = 0;
  int32_t oldLines = tsNumOfLogLines;
  tsNumOfLogLines = 100;
  ASSERT_EQ(taosInitLog("linesTest", 1, false), 0) << terrstr();
  uDebugFlag = 143;

  // the active file may be .0 or .1 depending on the global log state; rotation makes both exist
  char file0[PATH_MAX + 32] = {0}, file1[PATH_MAX + 32] = {0};
  snprintf(file0, sizeof(file0), "%s/linesTest.0", guard.logDir);
  snprintf(file1, sizeof(file1), "%s/linesTest.1", guard.logDir);
  auto bothExist = [&]() { return taosCheckExistFile(file0) && taosCheckExistFile(file1); };
  ASSERT_FALSE(bothExist());

  // few calls, but each call is long (~3.9KB, estimated as ~65 lines): must still trigger rotation
  std::string multi;
  for (int i = 0; i < 60; ++i) multi += std::string(64, 'a') + "\n";
  uInfoL("multi lines:\n%s", multi.c_str());
  EXPECT_FALSE(bothExist());
  uInfoL("multi lines:\n%s", multi.c_str());
  EXPECT_TRUE(bothExist());
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
  char path[PATH_MAX] = {0};
  taosGetTmpfilePath(TD_TMP_DIR_PATH, "logTest", path);
  ASSERT_EQ(taosMkDir(path), 0) << terrstr();
  tstrncpy(tsLogDir, path, PATH_MAX);
  ASSERT_EQ(taosInitLog("taoslog", 1, true), 0) << terrstr();

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
  ASSERT_NE(pCrashMsg, nullptr);
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
  ASSERT_NE(pReadMsg, nullptr) << "failed to read " << crashInfo;
  ASSERT_NE(pFile, nullptr) << "failed to open " << crashInfo;
  EXPECT_EQ(readMsgLen, strlen("crashMsg"));
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
  taosCloseLog();
  taosRemoveDir(path);
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
