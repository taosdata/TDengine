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
#include <iostream>
#include <inttypes.h>

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

TEST(osTest, osFQDNSuccess) {
  char     fqdn[1024];
  char     ipString[INET_ADDRSTRLEN];
  int      code = taosGetFqdn(fqdn);
  uint32_t ipv4 = taosGetIpv4FromFqdn(fqdn);
  ASSERT_NE(ipv4, 0xffffffff);

  struct in_addr addr;
  addr.s_addr = htonl(ipv4);
  snprintf(ipString, INET_ADDRSTRLEN, "%u.%u.%u.%u", (unsigned int)(addr.s_addr >> 24) & 0xFF,
           (unsigned int)(addr.s_addr >> 16) & 0xFF, (unsigned int)(addr.s_addr >> 8) & 0xFF,
           (unsigned int)(addr.s_addr) & 0xFF);
  printf("fqdn:%s  ip:%s\n", fqdn, ipString);
}

TEST(osTest, osFQDNFailed) {
  char     fqdn[1024] = "fqdn_test_not_found";
  char     ipString[24];
  uint32_t ipv4 = taosGetIpv4FromFqdn(fqdn);
  ASSERT_EQ(ipv4, 0xffffffff);

  terrno = TSDB_CODE_RPC_FQDN_ERROR;
  printf("fqdn:%s transfer to ip failed!\n", fqdn);
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
  printf("os system name:%s\n", osSysName);
  ASSERT_EQ(ret, 0);
}

void fileOperateOnFree(void *param) {
  char *    fname = (char *)param;
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
  printf("On free thread open file\n");
  ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  printf("On free thread lock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  ret = taosUnLockFile(pFile);
  printf("On free thread unlock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  ret = taosCloseFile(&pFile);
  ASSERT_EQ(ret, 0);
  printf("On free thread close file ret:%d\n", ret);
}
void *fileOperateOnFreeThread(void *param) {
  fileOperateOnFree(param);
  return NULL;
}
void fileOperateOnBusy(void *param) {
  char *    fname = (char *)param;
  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
  printf("On busy thread open file\n");
  if (pFile == NULL) return;
  // ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  printf("On busy thread lock file ret:%d\n", ret);
  ASSERT_NE(ret, 0);

  ret = taosUnLockFile(pFile);
  printf("On busy thread unlock file ret:%d\n", ret);

  ret = taosCloseFile(&pFile);
  printf("On busy thread close file ret:%d\n", ret);
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
  printf("create file success\n");
  taosCloseFile(&pOutFD);

  taosCloseFile(&pOutFD);

  TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
  printf("open file\n");
  ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  printf("lock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);

  TdThread thread1, thread2;
  taosThreadCreate(&(thread1), &thattr, fileOperateOnBusyThread, (void *)fname);
  taosThreadAttrDestroy(&thattr);

  taosThreadJoin(thread1, NULL);
  taosThreadClear(&thread1);

  ret = taosUnLockFile(pFile);
  printf("unlock file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  ret = taosCloseFile(&pFile);
  printf("close file ret:%d\n", ret);
  ASSERT_EQ(ret, 0);

  taosThreadCreate(&(thread2), &thattr, fileOperateOnFreeThread, (void *)fname);
  taosThreadAttrDestroy(&thattr);

  taosThreadJoin(thread2, NULL);
  taosThreadClear(&thread2);

  //int ret = taosRemoveFile(fname);
  //ASSERT_EQ(ret, 0);
  //printf("remove file success");
}

#ifndef OSFILE_PERFORMANCE_TEST

#define MAX_WORDS          100
#define MAX_WORD_LENGTH    20
#define MAX_TEST_FILE_SIZE 100000
#define TESTTIMES          1000

char *getRandomWord() {
  static char words[][MAX_WORD_LENGTH] = {
        "Lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
        "sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna",
        "aliqua", "Ut", "enim", "ad", "minim", "veniam", "quis", "nostrud", "exercitation", "ullamco",
        "Why", "do", "programmers", "prefer", "using", "dark", "mode?", "Because", "light", "attracts",
        "bugs", "and", "they", "want", "to", "code", "in", "peace,", "like", "a", "ninja", "in", "the", "shadows."
        "aliqua", "Ut", "enim", "ad", "minim", "veniam", "quis", "nostrud", "exercitation", "ullamco",
        "laboris", "nisi", "ut", "aliquip", "ex", "ea", "commodo", "consequat", "Duis", "aute", "irure",
        "dolor", "in", "reprehenderit", "in", "voluptate", "velit", "esse", "cillum", "dolore", "eu",
        "fugiat", "nulla", "pariatur", "Excepteur", "sint", "occaecat", "cupidatat", "non", "proident",
        "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit", "anim", "id", "est", "laborum"
    };

  return words[taosRand() % MAX_WORDS];
}

int64_t fillBufferWithRandomWords(char *buffer, int64_t maxBufferSize) {
  int64_t len = 0;
  while (len < maxBufferSize) {
    char * word = getRandomWord();
    size_t wordLen = strlen(word);

    if (len + wordLen + 1 < maxBufferSize) {
      strcat(buffer, word);
      strcat(buffer, " ");
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
  printf("os file performance testting...\n");
  int64_t WriteFileCost;
  int64_t ReadFileCost;
  int64_t OpenForWriteCloseFileCost;
  int64_t OpenForReadCloseFileCost;

  char *  buffer;
  char *  writeBuffer = (char *)taosMemoryCalloc(1, MAX_TEST_FILE_SIZE);
  char *  readBuffer = (char *)taosMemoryCalloc(1, MAX_TEST_FILE_SIZE);
  int64_t size = fillBufferWithRandomWords(writeBuffer, MAX_TEST_FILE_SIZE);
  char *  fname = "./osFilePerformanceTest.txt";

  TdFilePtr pOutFD = taosCreateFile(fname, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
  ASSERT_NE(pOutFD, nullptr);
  taosCloseFile(&pOutFD);

  printf("os file performance start write...\n");
  int64_t t1 = taosGetTimestampUs();
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_WRITE_THROUGH);
    ASSERT_NE(pFile, nullptr);
    taosWriteFile(pFile, writeBuffer, size);
    taosFsyncFile(pFile);
    taosCloseFile(&pFile);
  }

  int64_t t2 = taosGetTimestampUs();
  WriteFileCost = t2 - t1;

  printf("os file performance start read...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_READ);
    ASSERT_NE(pFile, nullptr);
    taosReadFile(pFile, readBuffer, size);
    taosCloseFile(&pFile);
    int readLine = strlen(readBuffer);
    ASSERT_EQ(size, readLine);
  }
  int64_t t3 = taosGetTimestampUs();
  ReadFileCost = t3 - t2;

  printf("os file performance start open1...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_WRITE);
    ASSERT_NE(pFile, nullptr);
    taosCloseFile(&pFile);
  }
  int64_t t4 = taosGetTimestampUs();
  OpenForWriteCloseFileCost = t4 - t3;

  printf("os file performance start open2...\n");
  for (int i = 0; i < TESTTIMES; ++i) {
    TdFilePtr pFile = taosOpenFile(fname, TD_FILE_CREATE | TD_FILE_READ);
    ASSERT_NE(pFile, nullptr);
    taosCloseFile(&pFile);
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

  printf("Test Write file %d times, cost: %" PRId64 "us\n", TESTTIMES, WriteFileCost);
  printf("Test Read file %d times, cost: %" PRId64 "us\n", TESTTIMES, ReadFileCost);
  printf("Test OpenForWrite & Close file %d times, cost: %" PRId64 "us\n", TESTTIMES, OpenForWriteCloseFileCost);
  printf("Test OpenForRead & Close file %d times, cost: %" PRId64 "us\n", TESTTIMES, OpenForReadCloseFileCost);
}

#endif // OSFILE_PERFORMANCE_TEST

#pragma GCC diagnostic pop
