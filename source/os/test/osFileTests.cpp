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

TEST(osFileTests, taosGetTmpfilePath) {
  char inputTmpDir[100] = "/tmp";
  char fileNamePrefix[100] = "txt";
  char dstPath[100] = {0};

  taosGetTmpfilePath(NULL, fileNamePrefix, dstPath);
  taosGetTmpfilePath(inputTmpDir, NULL, dstPath);
  taosGetTmpfilePath(inputTmpDir, fileNamePrefix, dstPath);

  int32_t ret = taosRemoveFile(NULL);
  EXPECT_NE(ret, 0);

  ret = taosCloseFile(NULL);
  EXPECT_EQ(ret, 0);

  ret = taosRenameFile(NULL, "");
  EXPECT_NE(ret, 0);
  ret = taosRenameFile("", NULL);
  EXPECT_NE(ret, 0);

  int64_t stDev = 0;
  int64_t stIno = 0;
  ret = taosDevInoFile(NULL, &stDev, &stIno);
  EXPECT_NE(ret, 0);
}

TEST(osFileTests, taosCopyFile) {
  char    from[100] = {0};
  char    to[100] = {0};
  int64_t ret = taosCopyFile(from, NULL);
  EXPECT_EQ(ret, -1);

  ret = taosCopyFile(NULL, to);
  EXPECT_EQ(ret, -1);

  ret = taosCopyFile(from, to);
  EXPECT_EQ(ret, -1);

  tstrncpy(from, "/tmp/tdengine-test-file", sizeof(from));
  TdFilePtr testFilePtr = taosCreateFile(from, TD_FILE_CREATE);
  taosWriteFile(testFilePtr, "abcdefg", 9);

  int64_t ret64 = taosReadFile(testFilePtr, NULL, 0);
  EXPECT_NE(ret64, 0);
  ret64 = taosReadFile(NULL, to, 100);
  EXPECT_NE(ret64, 0);
  ret64 = taosWriteFile(testFilePtr, NULL, 0);
  EXPECT_EQ(ret64, 0);
  ret64 = taosWriteFile(NULL, to, 100);
  EXPECT_EQ(ret64, 0);
  ret64 = taosPWriteFile(testFilePtr, NULL, 0, 0);
  EXPECT_EQ(ret64, 0);
  ret64 = taosPWriteFile(NULL, to, 100, 0);
  EXPECT_EQ(ret64, 0);
  ret64 = taosLSeekFile(NULL, 0, 0);
  EXPECT_EQ(ret64, -1);

  ret64 = taosPReadFile(NULL, NULL, 0, 0);
  EXPECT_EQ(ret64, -1);

  bool retb = taosValidFile(testFilePtr);
  EXPECT_TRUE(retb);
  retb = taosValidFile(NULL);
  EXPECT_FALSE(retb);

  retb = taosCheckAccessFile(NULL, 0);
  EXPECT_FALSE(retb);

  int32_t ret32 = taosFStatFile(NULL, NULL, NULL);
  EXPECT_NE(ret32, 0);

  ret32 = taosLockFile(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosUnLockFile(NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosFtruncateFile(NULL, 0);
  EXPECT_NE(ret32, 0);
  ret64 = taosFSendFile(NULL, testFilePtr, NULL, 0);
  EXPECT_NE(ret64, 0);
  ret64 = taosFSendFile(testFilePtr, NULL, NULL, 0);
  EXPECT_NE(ret64, 0);

  char buf[100] = {0};
  ret64 = taosGetLineFile(NULL, (char**)&buf);
  EXPECT_EQ(ret64, -1);
  ret64 = taosGetLineFile(testFilePtr, NULL);
  EXPECT_EQ(ret64, -1);

  ret64 = taosGetsFile(testFilePtr, 0, NULL);
  EXPECT_NE(ret64, -1);
  ret64 = taosGetsFile(NULL, 0, buf);
  EXPECT_NE(ret64, -1);

  ret32 = taosEOFFile(NULL);
  EXPECT_NE(ret64, -1);

  taosCloseFile(&testFilePtr);
  ret32 = taosFStatFile(testFilePtr, NULL, NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosLockFile(testFilePtr);
  EXPECT_NE(ret32, 0);
  ret32 = taosUnLockFile(testFilePtr);
  EXPECT_NE(ret32, 0);
  ret32 = taosFtruncateFile(testFilePtr, 0);
  EXPECT_NE(ret32, 0);
  ret64 = taosFSendFile(testFilePtr, testFilePtr, NULL, 0);
  EXPECT_NE(ret64, 0);
  ret64 = taosGetLineFile(testFilePtr, NULL);
  EXPECT_EQ(ret64, -1);
  ret64 = taosGetsFile(testFilePtr, 0, NULL);
  EXPECT_NE(ret64, -1);
  ret32 = taosEOFFile(testFilePtr);
  EXPECT_NE(ret64, -1);

  retb = taosValidFile(testFilePtr);
  EXPECT_FALSE(retb);

  ret = taosCopyFile(from, to);
  EXPECT_EQ(ret, -1);

  int64_t size = 0;
  int64_t mtime = 0;
  int64_t atime = 0;
  ret = taosStatFile(NULL, &size, &mtime, &atime);
  EXPECT_NE(ret, 0);

  ret = taosStatFile(from, &size, &mtime, NULL);
  EXPECT_EQ(ret, 0);

  int64_t diskid = 0;
  ret = taosGetFileDiskID(NULL, &diskid);
  EXPECT_NE(ret, 0);

  ret = taosGetFileDiskID("", &diskid);
  EXPECT_NE(ret, 0);

  ret = taosGetFileDiskID(from, NULL);
  EXPECT_EQ(ret, 0);

  ret32 = taosCompressFile(NULL, "");
  EXPECT_NE(ret32, 0);
  ret32 = taosCompressFile("", NULL);
  EXPECT_NE(ret32, 0);
  ret32 = taosCompressFile("", "");
  EXPECT_NE(ret32, 0);
  ret32 = taosCompressFile("/tmp/tdengine-test-file", "");
  EXPECT_NE(ret32, 0);

  ret32 = taosLinkFile("", "");
  EXPECT_NE(ret32, 0);

  char  mod[8] = {0};
  FILE* retptr = taosOpenCFile(NULL, "");
  EXPECT_EQ(retptr, nullptr);
  retptr = taosOpenCFile("", NULL);
  EXPECT_EQ(retptr, nullptr);
  retptr = taosOpenCFile("", mod);
  EXPECT_EQ(retptr, nullptr);

  ret32 = taosSeekCFile(NULL, 0, 0);
  EXPECT_NE(ret32, 0);

  size_t retsize = taosReadFromCFile(buf, 0, 0, NULL);
  EXPECT_EQ(retsize, 0);
  retsize = taosReadFromCFile(NULL, 0, 0, NULL);
  EXPECT_EQ(retsize, 0);

  taosRemoveFile(from);
}

TEST(osFileTests, taosCreateFile) {
  char    path[100] = {0};
  int32_t tdFileOptions = 0;

  TdFilePtr ret = taosCreateFile(NULL, 0);
  EXPECT_EQ(ret, nullptr);

  ret = taosCreateFile(path, 0);
  EXPECT_EQ(ret, nullptr);

  FILE* retptr = taosOpenFileForStream(NULL, 0);
  EXPECT_EQ(retptr, nullptr);

  TdFilePtr retptr2 = taosOpenFile(NULL, 0);
  EXPECT_EQ(retptr2, nullptr);
}