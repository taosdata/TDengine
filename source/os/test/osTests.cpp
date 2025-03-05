/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include <inttypes.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "tlog.h"

#ifdef WINDOWS
#include <windows.h>
#else

#include <arpa/inet.h>

TEST(osTest, locale) {
  char *ret = taosCharsetReplace(NULL);
  EXPECT_EQ(ret, nullptr);

  ret = taosCharsetReplace("utf8");
  EXPECT_NE(ret, nullptr);

  ret = taosCharsetReplace("utf-8");
  EXPECT_NE(ret, nullptr);

  taosGetSystemLocale(NULL, "");
  taosGetSystemLocale("", NULL);
}

TEST(osTest, memory) {
  int32_t ret = taosMemoryDbgInitRestore();
  EXPECT_EQ(ret, 0);

  int64_t ret64 = taosMemSize(NULL);
  EXPECT_EQ(ret64, 0);
}

TEST(osTest, rand2) {
  char str[128] = {0};
  taosRandStr2(str, 100);
}

TEST(osTest, socket2) {
  int32_t ret32 = taosCloseSocket(NULL);
  EXPECT_NE(ret32, 0);

  ret32 = taosSetSockOpt(NULL, 0, 0, NULL, 0);
  EXPECT_NE(ret32, 0);

#if defined(LINUX)
  struct in_addr ipInt;
  ipInt.s_addr = htonl(0x7F000001);
  char buf[128] = {0};
  taosInetNtop(ipInt, buf, 32);
#endif

  ret32 = taosGetIpv4FromFqdn("localhost", NULL);
  EXPECT_NE(ret32, 0);
  uint32_t ip = 0;
  ret32 = taosGetIpv4FromFqdn(NULL, &ip);
  EXPECT_NE(ret32, 0);

  taosInetNtoa(NULL, ip);
  ret32 = taosInetAddr(NULL);
  EXPECT_EQ(ret32, 0);

  ret32 = taosWinSocketInit();
  EXPECT_EQ(ret32, 0);
}

TEST(osTest, time2) {
  taosGetLocalTimezoneOffset();

  char  buf[12] = {0};
  char  fmt[12] = {0};
  void *retptr = taosStrpTime(NULL, fmt, NULL);
  EXPECT_EQ(retptr, nullptr);
  retptr = taosStrpTime(buf, NULL, NULL);
  EXPECT_EQ(retptr, nullptr);

  size_t ret = taosStrfTime(NULL, 0, fmt, NULL);
  EXPECT_EQ(ret, 0);
  ret = taosStrfTime(buf, 0, NULL, NULL);
  EXPECT_EQ(ret, 0);

  time_t     tp = {0};
  struct tm *retptr2 = taosGmTimeR(&tp, NULL);
  EXPECT_EQ(retptr2, nullptr);
  retptr2 = taosGmTimeR(NULL, NULL);
  EXPECT_EQ(retptr2, nullptr);

  time_t rett = taosTimeGm(NULL);
  EXPECT_EQ(rett, -1);

  timezone_t tz = {0};
  retptr2 = taosLocalTime(&tp, NULL, NULL, 0, tz);
  EXPECT_EQ(retptr2, nullptr);
  retptr2 = taosLocalTime(NULL, NULL, NULL, 0, tz);
  EXPECT_EQ(retptr2, nullptr);
}

TEST(osTest, system) {
#if defined(LINUX)
  taosSetConsoleEcho(false);
  taosSetConsoleEcho(true);

  taosGetOldTerminalMode();
  taosCloseCmd(NULL);

  TdCmdPtr ptr = taosOpenCmd(NULL);
  EXPECT_EQ(ptr, nullptr);
  taosCloseCmd(&ptr);

  ptr = taosOpenCmd("echo 'hello world'");
  ASSERT_NE(ptr, nullptr);

  char    buf[256] = {0};
  int64_t ret64 = taosGetsCmd(NULL, 0, NULL);
  EXPECT_LE(ret64, 0);
  ret64 = taosGetsCmd(ptr, 0, NULL);
  EXPECT_LE(ret64, 0);
  ret64 = taosGetsCmd(ptr, 255, buf);
  EXPECT_GT(ret64, 0);
  taosCloseCmd(&ptr);

  ptr = taosOpenCmd("echoxxx 'hello world'");
  ASSERT_NE(ptr, nullptr);
  ret64 = taosGetsCmd(ptr, 255, buf);
  EXPECT_LE(ret64, 0);
  taosCloseCmd(&ptr);

  ret64 = taosGetLineCmd(NULL, NULL);
  EXPECT_LE(ret64, 0);
  ret64 = taosGetLineCmd(ptr, NULL);
  EXPECT_LE(ret64, 0);

  ptr = taosOpenCmd("echo 'hello world'");
  ASSERT_NE(ptr, nullptr);
  char *ptrBuf = NULL;
  ret64 = taosGetLineCmd(ptr, &ptrBuf);
  EXPECT_GE(ret64, 0);
  taosCloseCmd(&ptr);

  ptr = taosOpenCmd("echoxxx 'hello world'");
  ASSERT_NE(ptr, nullptr);
  ret64 = taosGetLineCmd(ptr, &ptrBuf);
  EXPECT_LE(ret64, 0);
  taosCloseCmd(&ptr);

  int32_t ret32 = taosEOFCmd(NULL);
  EXPECT_EQ(ret32, 0);

#endif
}

TEST(osTest, sysinfo) {
#if defined(LINUX)

  int32_t ret32 = 0;

  ret32 = taosGetEmail(NULL, 0);
  EXPECT_NE(ret32, 0);

  ret32 = taosGetOsReleaseName(NULL, NULL, NULL, 0);
  EXPECT_NE(ret32, 0);

  char  buf[128] = {0};
  float numOfCores = 0;
  ret32 = taosGetCpuInfo(buf, 0, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetCpuInfo(NULL, 0, &numOfCores);
  EXPECT_NE(ret32, 0);

  
  ret32 = taosGetCpuCores(NULL, false);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetTotalMemory(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcMemory(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetSysMemory(NULL);
  EXPECT_NE(ret32, 0);

  ret32 = taosGetDiskSize(buf, NULL);
  EXPECT_NE(ret32, 0);
  SDiskSize disksize = {0};
  ret32 = taosGetDiskSize(NULL, &disksize);
  EXPECT_NE(ret32, 0);

  int64_t tmp = 0;
  ret32 = taosGetProcIO(NULL, NULL, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcIO(&tmp, NULL, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcIO(&tmp, &tmp, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcIO(&tmp, &tmp, &tmp, NULL);
  EXPECT_NE(ret32, 0);

  ret32 = taosGetProcIODelta(NULL, NULL, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcIODelta(&tmp, NULL, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcIODelta(&tmp, &tmp, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetProcIODelta(&tmp, &tmp, &tmp, NULL);
  EXPECT_NE(ret32, 0);

  taosGetProcIODelta(NULL, NULL, NULL, NULL);
  taosGetProcIODelta(&tmp, &tmp, &tmp, &tmp);

  ret32 = taosGetCardInfo(NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetCardInfo(&tmp, NULL);
  EXPECT_NE(ret32, 0);

  ret32 = taosGetCardInfoDelta(NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetCardInfoDelta(&tmp, NULL);
  EXPECT_NE(ret32, 0);

  taosGetCardInfoDelta(NULL, NULL);
  taosGetCardInfoDelta(&tmp, &tmp);

  ret32 = taosGetSystemUUIDLimit36(NULL, 0);
  EXPECT_NE(ret32, 0);

  ret32 = taosGetSystemUUIDLen(NULL, 0);
  EXPECT_NE(ret32, 0);
  ret32 = taosGetSystemUUIDLen(buf, -1);
  EXPECT_NE(ret32, 0);

  taosSetCoreDump(false);

  ret32 = taosGetlocalhostname(NULL, 0);
  EXPECT_NE(ret32, 0);
#endif
}

TEST(osTest, osFQDNSuccess) {
  char     fqdn[TD_FQDN_LEN];
  char     ipString[INET_ADDRSTRLEN];
  int      code = taosGetFqdn(fqdn);
  uint32_t ipv4 = 0;
  code = taosGetIpv4FromFqdn(fqdn, &ipv4);
  ASSERT_NE(ipv4, 0xffffffff);

  struct in_addr addr;
  addr.s_addr = htonl(ipv4);
  (void)snprintf(ipString, INET_ADDRSTRLEN, "%u.%u.%u.%u", (unsigned int)(addr.s_addr >> 24) & 0xFF,
                 (unsigned int)(addr.s_addr >> 16) & 0xFF, (unsigned int)(addr.s_addr >> 8) & 0xFF,
                 (unsigned int)(addr.s_addr) & 0xFF);
  (void)printf("fqdn:%s  ip:%s\n", fqdn, ipString);
}

TEST(osTest, osFQDNFailed) {
  char     fqdn[1024] = "fqdn_test_not_found";
  char     ipString[24];
  uint32_t ipv4 = 0;
  int32_t  code = taosGetIpv4FromFqdn(fqdn, &ipv4);
  ASSERT_NE(code, 0);

  terrno = TSDB_CODE_RPC_FQDN_ERROR;
  (void)printf("fqdn:%s transfer to ip failed!\n", fqdn);
}

#endif  //  WINDOWS

TEST(osTest, osSystem) {
  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;  // tsLogEmbedded ? 255 : uDebugFlag
  taosPrintTrace(flags, level, dflag, 0);

  const int sysLen = 64;
  char      osSysName[sysLen];
  int       ret = taosGetOsReleaseName(osSysName, NULL, NULL, sysLen);
  (void)printf("os system name:%s\n", osSysName);
  ASSERT_EQ(ret, 0);
}

void fileOperateOnFree(void *param) {
  char     *fname = (char *)param;
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
  (void)printf("On free thread open file\n");
  ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  (void)printf("On free thread lock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  ret = taosUnLockFile(pFile);
  (void)printf("On free thread unlock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  ret = taosCloseFile(&pFile);
  ASSERT_EQ(ret, 0);
  (void)printf("On free thread close file ret:%d\n", ret);
}
void *fileOperateOnFreeThread(void *param) {
  fileOperateOnFree(param);
  return NULL;
}
void fileOperateOnBusy(void *param) {
  char     *fname = (char *)param;
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
  (void)printf("On busy thread open file\n");
  if (pFile == NULL) return;
  // ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  (void)printf("On busy thread lock file ret:%d\n", ret);
  ASSERT_NE(ret, 0);

  ret = taosUnLockFile(pFile);
  (void)printf("On busy thread unlock file ret:%d\n", ret);

  ret = taosCloseFile(&pFile);
  (void)printf("On busy thread close file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);
}
void *fileOperateOnBusyThread(void *param) {
  fileOperateOnBusy(param);
  return NULL;
}

TEST(osTest, osFile) {
  char *fname = "./osfiletest1.txt";

  TdFilePtr pOutFD = taosCreateFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  ASSERT_NE(pOutFD, nullptr);
  (void)printf("create file success\n");
  (void)taosCloseFile(&pOutFD);

  (void)taosCloseFile(&pOutFD);

  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
  (void)printf("open file\n");
  ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  (void)printf("lock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  TdThreadAttr thattr;
  (void)taosThreadAttrInit(&thattr);

  TdThread thread1, thread2;
  (void)taosThreadCreate(&(thread1), &thattr, fileOperateOnBusyThread, (void *)fname);
  (void)taosThreadAttrDestroy(&thattr);

  (void)taosThreadJoin(thread1, NULL);
  taosThreadClear(&thread1);

  ret = taosUnLockFile(pFile);
  (void)printf("unlock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  ret = taosCloseFile(&pFile);
  (void)printf("close file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  (void)taosThreadCreate(&(thread2), &thattr, fileOperateOnFreeThread, (void *)fname);
  (void)taosThreadAttrDestroy(&thattr);

  (void)taosThreadJoin(thread2, NULL);
  taosThreadClear(&thread2);

  // int ret = taosRemoveFile(fname);
  // ASSERT_EQ(ret, 0);
  // printf("remove file success");
}

#ifndef OSFILE_PERFORMANCE_TEST

#define MAX_WORDS          100
#define MAX_WORD_LENGTH    20
#define MAX_TEST_FILE_SIZE 100000
#define TESTTIMES          1000

char *getRandomWord() {
  static char words[][MAX_WORD_LENGTH] = {"Lorem",
                                          "ipsum",
                                          "dolor",
                                          "sit",
                                          "amet",
                                          "consectetur",
                                          "adipiscing",
                                          "elit",
                                          "sed",
                                          "do",
                                          "eiusmod",
                                          "tempor",
                                          "incididunt",
                                          "ut",
                                          "labore",
                                          "et",
                                          "dolore",
                                          "magna",
                                          "aliqua",
                                          "Ut",
                                          "enim",
                                          "ad",
                                          "minim",
                                          "veniam",
                                          "quis",
                                          "nostrud",
                                          "exercitation",
                                          "ullamco",
                                          "Why",
                                          "do",
                                          "programmers",
                                          "prefer",
                                          "using",
                                          "dark",
                                          "mode?",
                                          "Because",
                                          "light",
                                          "attracts",
                                          "bugs",
                                          "and",
                                          "they",
                                          "want",
                                          "to",
                                          "code",
                                          "in",
                                          "peace,",
                                          "like",
                                          "a",
                                          "ninja",
                                          "in",
                                          "the",
                                          "shadows."
                                          "aliqua",
                                          "Ut",
                                          "enim",
                                          "ad",
                                          "minim",
                                          "veniam",
                                          "quis",
                                          "nostrud",
                                          "exercitation",
                                          "ullamco",
                                          "laboris",
                                          "nisi",
                                          "ut",
                                          "aliquip",
                                          "ex",
                                          "ea",
                                          "commodo",
                                          "consequat",
                                          "Duis",
                                          "aute",
                                          "irure",
                                          "dolor",
                                          "in",
                                          "reprehenderit",
                                          "in",
                                          "voluptate",
                                          "velit",
                                          "esse",
                                          "cillum",
                                          "dolore",
                                          "eu",
                                          "fugiat",
                                          "nulla",
                                          "pariatur",
                                          "Excepteur",
                                          "sint",
                                          "occaecat",
                                          "cupidatat",
                                          "non",
                                          "proident",
                                          "sunt",
                                          "in",
                                          "culpa",
                                          "qui",
                                          "officia",
                                          "deserunt",
                                          "mollit",
                                          "anim",
                                          "id",
                                          "est",
                                          "laborum"};

  return words[taosRand() % MAX_WORDS];
}

int64_t fillBufferWithRandomWords(char *buffer, int64_t maxBufferSize) {
  int64_t len = 0;
  while (len < maxBufferSize) {
    char  *word = getRandomWord();
    size_t wordLen = strlen(word);

    if (len + wordLen + 1 < maxBufferSize) {
      (void)strcat(buffer, word);
      (void)strcat(buffer, " ");
      len += wordLen + 1;
    } else {
      break;
    }
  }
  return len;
}

int64_t calculateAverage(int64_t arr[], int size) {
  int64_t sum = 0;
  for (int i = 0; i < size; i++) {
    sum += arr[i];
  }
  return sum / size;
}

int64_t calculateMax(int64_t arr[], int size) {
  int64_t max = arr[0];
  for (int i = 1; i < size; i++) {
    if (arr[i] > max) {
      max = arr[i];
    }
  }
  return max;
}

int64_t calculateMin(int64_t arr[], int size) {
  int64_t min = arr[0];
  for (int i = 1; i < size; i++) {
    if (arr[i] < min) {
      min = arr[i];
    }
  }
  return min;
}

TEST(osTest, osFilePerformance) {
  (void)printf("os file performance testting...\n");
  int64_t WriteFileCost;
  int64_t ReadFileCost;
  int64_t OpenForWriteCloseFileCost;
  int64_t OpenForReadCloseFileCost;

  char   *buffer;
  char   *writeBuffer = (char *)taosMemoryCalloc(1, MAX_TEST_FILE_SIZE);
  char   *readBuffer = (char *)taosMemoryCalloc(1, MAX_TEST_FILE_SIZE);
  int64_t size = fillBufferWithRandomWords(writeBuffer, MAX_TEST_FILE_SIZE);
  char   *fname = "./osFilePerformanceTest.txt";

  TdFilePtr pOutFD = taosCreateFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  ASSERT_NE(pOutFD, nullptr);
  (void)taosCloseFile(&pOutFD);

  (void)printf("os file performance start write...\n");
  int64_t t1 = taosGetTimestampUs();
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_WRITE_THROUGH);
    ASSERT_NE(pFile, nullptr);
    (void)taosWriteFile(pFile, writeBuffer, size);
    (void)taosFsyncFile(pFile);
    (void)taosCloseFile(&pFile);
  }

  int64_t t2 = taosGetTimestampUs();
  WriteFileCost = t2 - t1;

  (void)printf("os file performance start read...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ);
    ASSERT_NE(pFile, nullptr);
    (void)taosReadFile(pFile, readBuffer, size);
    (void)taosCloseFile(&pFile);
    int readLine = strlen(readBuffer);
    ASSERT_EQ(size, readLine);
  }
  int64_t t3 = taosGetTimestampUs();
  ReadFileCost = t3 - t2;

  (void)printf("os file performance start open1...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
    ASSERT_NE(pFile, nullptr);
    (void)taosCloseFile(&pFile);
  }
  int64_t t4 = taosGetTimestampUs();
  OpenForWriteCloseFileCost = t4 - t3;

  (void)printf("os file performance start open2...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_READ);
    ASSERT_NE(pFile, nullptr);
    (void)taosCloseFile(&pFile);
  }
  int64_t t5 = taosGetTimestampUs();
  OpenForReadCloseFileCost = t5 - t4;

#ifdef WINDOWS
  printf("os file performance start window native...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    HANDLE hFile = CreateFile(fname,                    // 文件名
                              GENERIC_WRITE,            // 写权限
                              FILE_SHARE_READ,          // 不共享
                              NULL,                     // 默认安全描述符
                              OPEN_ALWAYS,              // 打开已存在的文件
                              FILE_FLAG_WRITE_THROUGH,  // 文件标志，可以根据实际需求调整
                              NULL                      // 模板文件句柄，对于创建新文件不需要
    );

    if (hFile == INVALID_HANDLE_VALUE) {
      printf("Error opening file\n");
      break;
    }

    // 写入数据
    DWORD bytesWritten;
    if (!WriteFile(hFile, writeBuffer, size, &bytesWritten, NULL)) {
      // 处理错误
      printf("Error writing to file\n");
      CloseHandle(hFile);
      break;
    }
    // 关闭文件
    CloseHandle(hFile);
  }
  int64_t t6 = taosGetTimestampUs();
  int64_t nativeWritCost = t6 - t5;

  printf("Test Write file using native API %d times, cost: %" PRId64 "us\n", TESTTIMES, nativeWritCost);
#endif  // WINDOWS

  taosMemoryFree(writeBuffer);
  taosMemoryFree(readBuffer);

  (void)printf("Test Write file %d times, cost: %" PRId64 "us\n", TESTTIMES, WriteFileCost);
  (void)printf("Test Read file %d times, cost: %" PRId64 "us\n", TESTTIMES, ReadFileCost);
  (void)printf("Test OpenForWrite & Close file %d times, cost: %" PRId64 "us\n", TESTTIMES, OpenForWriteCloseFileCost);
  (void)printf("Test OpenForRead & Close file %d times, cost: %" PRId64 "us\n", TESTTIMES, OpenForReadCloseFileCost);
}

#endif  // OSFILE_PERFORMANCE_TEST

#pragma GCC diagnostic pop
