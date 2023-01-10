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

TEST(osTest, osSystem) {
  const char *flags = "UTL FATAL ";
  ELogLevel   level = DEBUG_FATAL;
  int32_t     dflag = 255;  // tsLogEmbedded ? 255 : uDebugFlag
  taosPrintTrace(flags, level, dflag, 0);
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
  ASSERT_NE(pFile, nullptr);

  int ret = taosLockFile(pFile);
  printf("On busy thread lock file ret:%d\n", ret);
  ASSERT_NE(ret, 0);

  ret = taosUnLockFile(pFile);
  printf("On busy thread unlock file ret:%d\n", ret);
#ifdef _TD_DARWIN_64
  ASSERT_EQ(ret, 0);
#else
  ASSERT_NE(ret, 0);
#endif

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

#pragma GCC diagnostic pop
