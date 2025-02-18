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

TEST(osDirTests, taosRemoveDir) {
  int32_t ret = 0;

  const char* testDir = "/tmp/tdengine-test-dir";
  if (taosDirExist(testDir)) {
    taosRemoveDir(testDir);
  }

  taosRemoveDir(testDir);

  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  const char* testFile = "/tmp/tdengine-test-dir/test-file";
  TdFilePtr   testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  const char* testDir2 = "/tmp/tdengine-test-dir/test-dir2";
  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  const char* testFile2 = "/tmp/tdengine-test-dir/test-dir2/test-file2";
  TdFilePtr   testFilePtr2 = taosCreateFile(testFile2, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr2, nullptr);
  ret = taosCloseFile(&testFilePtr2);
  EXPECT_EQ(ret, 0);

  taosRemoveDir(testDir);

  bool exist = taosDirExist(testDir);
  EXPECT_EQ(exist, false);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosDirExist) {
  const char* dir1 = NULL;
  bool        exist = taosDirExist(dir1);
  EXPECT_EQ(exist, false);

  char dir2[2048] = {0};
  for (int32_t i = 0; i < 2047; ++i) {
    dir2[i] = 1;
  }
  exist = taosDirExist(dir2);
  EXPECT_EQ(exist, false);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosMulMkDir) {
  int32_t ret = 0;

  const char* dir1 = NULL;
  ret = taosMulMkDir(dir1);
  EXPECT_EQ(ret, -1);

  char dir2[2048] = {0};
  for (int32_t i = 0; i < 2047; ++i) {
    dir2[i] = '1';
  }
  ret = taosMulMkDir(dir2);
  EXPECT_EQ(ret, -1);

  const char* dir3 = "/tmp/tdengine-test-dir/1/2/3/4";
  taosRemoveDir(dir3);
  ret = taosMulMkDir(dir3);
  EXPECT_EQ(ret, 0);
  taosRemoveDir(dir3);

  const char* dir4 = "./tdengine-test-dir/1/2/3/4";
  taosRemoveDir(dir4);
  ret = taosMulMkDir(dir4);
  EXPECT_EQ(ret, 0);
  taosRemoveDir(dir4);

  const char* dir5 = "tdengine-test-dir/1/2/3/4";
  taosRemoveDir(dir5);
  ret = taosMulMkDir(dir5);
  EXPECT_EQ(ret, 0);
  ret = taosMulMkDir(dir5);
  EXPECT_EQ(ret, 0);
  taosRemoveDir(dir5);

  const char* testFile = "/tmp/tdengine-test-dir/test-file";
  TdFilePtr   testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);
  char dir6[2048] = {0};
  strcpy(dir6, "/tmp/tdengine-test-dir/test-file/1/2/3/4");
  ret = taosMulMkDir(dir6);
  EXPECT_NE(ret, 0);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosMulModeMkDir) {
  int32_t ret = 0;

  const char* dir1 = NULL;
  ret = taosMulModeMkDir(dir1, 777, true);
  EXPECT_NE(ret, 0);

  char dir2[2048] = {0};
  for (int32_t i = 0; i < 2047; ++i) {
    dir2[i] = '1';
  }
  ret = taosMulModeMkDir(dir2, 777, true);
  EXPECT_NE(ret, 0);

  const char* dir3 = "/tmp/tdengine-test-dir/1/2/3/4";
  taosRemoveDir(dir3);
  ret = taosMulMkDir(dir3);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir3, 777, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir3, 777, false);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir3, 999, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir3, 999, false);
  EXPECT_EQ(ret, 0);
  taosRemoveDir(dir3);

  const char* dir4 = "./tdengine-test-dir/1/2/3/4";
  taosRemoveDir(dir4);
  ret = taosMulModeMkDir(dir4, 777, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir4, 777, false);
  EXPECT_EQ(ret, 0);
  taosRemoveDir(dir4);

  const char* dir5 = "tdengine-test-dir/1/2/3/4";
  taosRemoveDir(dir5);
  ret = taosMulModeMkDir(dir5, 777, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir5, 777, false);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir5, 777, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir5, 777, false);
  EXPECT_EQ(ret, 0);
  taosRemoveDir(dir5);

  const char* testFile = "/tmp/tdengine-test-dir/test-file";
  TdFilePtr   testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);
  char dir6[2048] = {0};
  strcpy(dir6, "/tmp/tdengine-test-dir/test-file/1/2/3/4");
  ret = taosMulModeMkDir(dir6, 777, true);
  EXPECT_NE(ret, 0);

  const char* dir7 = "tdengine-test-dir/1/2/3/5";
  taosRemoveDir(dir7);
  ret = taosMulModeMkDir(dir7, 999, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir7, 999, false);
  EXPECT_EQ(ret, 0);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosRemoveOldFiles) {
  int32_t     ret = 0;
  const char* testDir = "/tmp/tdengine-test-dir";
  if (taosDirExist(testDir)) {
    taosRemoveDir(testDir);
  }
  taosRemoveDir(testDir);
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  const char* testFile = "/tmp/tdengine-test-dir/test-file";
  TdFilePtr   testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  taosRemoveOldFiles(testFile, 10);

  const char* testDir2 = "/tmp/tdengine-test-dir/test-dir2";
  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  const char* testFile3 = "/tmp/tdengine-test-dir/log.1433726073.gz";
  TdFilePtr   testFilePtr3 = taosCreateFile(testFile3, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr3, nullptr);
  ret = taosCloseFile(&testFilePtr3);
  EXPECT_EQ(ret, 0);

  const char* testFile4 = "/tmp/tdengine-test-dir/log.80.gz";
  TdFilePtr   testFilePtr4 = taosCreateFile(testFile4, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr4, nullptr);
  ret = taosCloseFile(&testFilePtr4);
  EXPECT_EQ(ret, 0);

  char testFile5[1024];
  snprintf(testFile5, 1024, "/tmp/tdengine-test-dir/log.%d.gz", taosGetTimestampSec());
  TdFilePtr testFilePtr5 = taosCreateFile(testFile5, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr5, nullptr);
  ret = taosCloseFile(&testFilePtr5);
  EXPECT_EQ(ret, 0);

  const char* testFile6 = "/tmp/tdengine-test-dir/log.1433726073.gz";
  TdFilePtr   testFilePtr6 = taosCreateFile(testFile6, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr6, nullptr);
  ret = taosCloseFile(&testFilePtr6);
  EXPECT_EQ(ret, 0);

  taosRemoveOldFiles(testDir, 10);

  bool exist = taosDirExist(testDir);
  EXPECT_EQ(exist, true);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosExpandDir) {
  int32_t     ret = 0;
  const char* testDir = "/tmp/tdengine-test-dir";
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  char fullpath[1024] = {0};
  ret = taosExpandDir(testDir, NULL, 1024);
  EXPECT_NE(ret, 0);
  ret = taosExpandDir(NULL, fullpath, 1024);
  EXPECT_NE(ret, 0);
  ret = taosExpandDir(testDir, fullpath, 1024);
  EXPECT_EQ(ret, 0);

  ret = taosExpandDir("/x123", fullpath, 1024);
  EXPECT_EQ(ret, 0);

  ret = taosExpandDir("", fullpath, 1024);
  EXPECT_EQ(ret, 0);

  char dir2[2048] = {0};
  for (int32_t i = 0; i < 2047; ++i) {
    dir2[i] = '1';
  }
  ret = taosExpandDir(dir2, fullpath, 1024);
  EXPECT_EQ(ret, 0);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosRealPath) {
  int32_t ret = 0;
  char    testDir[PATH_MAX * 2] = "/tmp/tdengine-test-dir";
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  char fullpath[PATH_MAX * 2] = {0};

  ret = taosRealPath(testDir, NULL, PATH_MAX * 2);
  EXPECT_EQ(ret, 0);

  ret = taosRealPath(NULL, fullpath, PATH_MAX * 2);
  EXPECT_NE(ret, 0);

  ret = taosRealPath(testDir, fullpath, PATH_MAX * 2);
  EXPECT_EQ(ret, 0);

  ret = taosRealPath(testDir, fullpath, 12);
  EXPECT_NE(ret, 0);

  ret = taosRealPath("/c/d", fullpath, 1024);
  EXPECT_NE(ret, 0);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosIsDir) {
  bool ret = taosIsDir("/c/d");
  EXPECT_EQ(ret, false);
}

TEST(osDirTests, taosDirName) {
  char* ret = taosDirName(NULL);
  EXPECT_EQ(ret, nullptr);

  char name1[24] = "xyz";
  ret = taosDirName(name1);
  EXPECT_NE(ret, nullptr);
  EXPECT_EQ(name1[0], 0);

  char name2[24] = "/root/xyz";
  ret = taosDirName(name2);
  EXPECT_NE(ret, nullptr);
  EXPECT_STREQ(ret, "/root");
}

TEST(osDirTests, taosDirEntryBaseName) {
  char* ret = taosDirEntryBaseName(NULL);
  EXPECT_EQ(ret, nullptr);

  char name1[12] = "/";
  ret = taosDirEntryBaseName(name1);
  EXPECT_STREQ(ret, "/");

  char name2[12] = "/root/";
  ret = taosDirEntryBaseName(name2);
  EXPECT_STREQ(ret, "root");

  char name3[12] = "/root";
  ret = taosDirEntryBaseName(name3);
  EXPECT_STREQ(ret, "root");

  char name4[12] = "root";
  ret = taosDirEntryBaseName(name4);
  EXPECT_STREQ(ret, "root");
}

TEST(osDirTests, taosOpenDir) {
  TdDirPtr ret = taosOpenDir(NULL);
  EXPECT_EQ(ret, nullptr);
}

TEST(osDirTests, taosReadDir) {
  TdDirEntryPtr rddir = taosReadDir(NULL);
  EXPECT_EQ(rddir, nullptr);

  int32_t     ret = 0;
  const char* testDir = "/tmp/tdengine-test-dir";
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  const char* testFile = "/tmp/tdengine-test-dir/test-file";
  TdFilePtr   testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  TdDirPtr dir = taosOpenDir(testFile);
  EXPECT_EQ(dir, nullptr);

  const char* testDir2 = "/tmp/tdengine-test-dir/test-dir2";
  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  const char* testFile2 = "/tmp/tdengine-test-dir/test-dir2/test-file2";
  TdFilePtr   testFilePtr2 = taosCreateFile(testFile2, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr2, nullptr);
  ret = taosCloseFile(&testFilePtr2);
  EXPECT_EQ(ret, 0);

  dir = taosOpenDir(testFile);
  EXPECT_EQ(dir, nullptr);

  rddir = taosReadDir(dir);
  EXPECT_EQ(rddir, nullptr);

  dir = taosOpenDir(testDir);
  EXPECT_NE(dir, nullptr);

  rddir = taosReadDir(dir);
  EXPECT_NE(rddir, nullptr);

  bool entry = taosDirEntryIsDir(NULL);
  EXPECT_EQ(entry, false);

  char* entryname = taosGetDirEntryName(NULL);
  EXPECT_EQ(entryname, nullptr);

  entryname = taosGetDirEntryName(rddir);
  EXPECT_NE(entryname, nullptr);

  int32_t code = taosCloseDir(NULL);
  EXPECT_NE(code, 0);

  code = taosCloseDir(&dir);
  EXPECT_EQ(code, 0);

  taosRemoveDir("/tmp/tdengine-test-dir");
}

TEST(osDirTests, taosGetDirSize) {
  TdDirEntryPtr rddir = taosReadDir(NULL);
  EXPECT_EQ(rddir, nullptr);

  int32_t     ret = 0;
  const char* testDir = "/tmp/tdengine-test-dir";
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  const char* testFile = "/tmp/tdengine-test-dir/test-file";
  TdFilePtr   testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  TdDirPtr dir = taosOpenDir(testFile);
  EXPECT_EQ(dir, nullptr);

  const char* testDir2 = "/tmp/tdengine-test-dir/test-dir2";
  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  const char* testFile2 = "/tmp/tdengine-test-dir/test-dir2/test-file2";
  TdFilePtr   testFilePtr2 = taosCreateFile(testFile2, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr2, nullptr);
  ret = taosCloseFile(&testFilePtr2);
  EXPECT_EQ(ret, 0);

  int64_t size = -1;
  ret = taosGetDirSize(testFile, &size);
  EXPECT_NE(ret, 0);

  ret = taosGetDirSize(testDir, &size);
  EXPECT_EQ(ret, 0);

  taosRemoveDir("/tmp/tdengine-test-dir");

  taosRemoveDir("./tdengine-test-dir/1/2/3/5");
  taosRemoveDir("./tdengine-test-dir/1/2/3/4");
  taosRemoveDir("./tdengine-test-dir/1/2/3");
  taosRemoveDir("./tdengine-test-dir/1/2");
  taosRemoveDir("./tdengine-test-dir/1");
  taosRemoveDir("./tdengine-test-dir/");
  taosRemoveDir("tdengine-test-dir/1/2/3/5");
  taosRemoveDir("tdengine-test-dir/1/2/3/4");
  taosRemoveDir("tdengine-test-dir/1/2/3");
  taosRemoveDir("tdengine-test-dir/1/2");
  taosRemoveDir("tdengine-test-dir/1");
  taosRemoveDir("tdengine-test-dir/");
  taosRemoveDir("tdengine-test-dir/");
}
