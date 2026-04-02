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

// Helper functions to build platform-specific test paths
static void buildTestPath(char* buf, size_t bufSize, const char* relativePath) {
  snprintf(buf, bufSize, "%stdengine-test-dir%s%s", TD_TMP_DIR_PATH, 
           relativePath[0] ? TD_DIRSEP : "", relativePath);
}

TEST(osDirTests, taosRemoveDir) {
  int32_t ret = 0;
  char testDir[PATH_MAX];
  char testFile[PATH_MAX];
  char testDir2[PATH_MAX];
  char testFile2[PATH_MAX];

  buildTestPath(testDir, sizeof(testDir), "");
  buildTestPath(testFile, sizeof(testFile), "test-file");
  buildTestPath(testDir2, sizeof(testDir2), "test-dir2");
  buildTestPath(testFile2, sizeof(testFile2), "test-dir2/test-file2");
  buildTestPath(testDir, sizeof(testDir), "");
  buildTestPath(testFile, sizeof(testFile), "test-file");
  buildTestPath(testDir2, sizeof(testDir2), "test-dir2");
  buildTestPath(testFile2, sizeof(testFile2), "test-dir2/test-file2");

  if (taosDirExist(testDir)) {
    taosRemoveDir(testDir);
  }

  taosRemoveDir(testDir);

  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr2 = taosCreateFile(testFile2, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr2, nullptr);
  ret = taosCloseFile(&testFilePtr2);
  EXPECT_EQ(ret, 0);

  taosRemoveDir(testDir);

  bool exist = taosDirExist(testDir);
  EXPECT_EQ(exist, false);

  taosRemoveDir(testDir);
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

  char testDir[PATH_MAX];
  buildTestPath(testDir, sizeof(testDir), "");
  taosRemoveDir(testDir);
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

  char dir3[PATH_MAX];
  buildTestPath(dir3, sizeof(dir3), "1/2/3/4");
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

  char testFile[PATH_MAX];
  char dir6[PATH_MAX];
  buildTestPath(testFile, sizeof(testFile), "test-file");
  TdFilePtr testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);
  buildTestPath(dir6, sizeof(dir6), "test-file/1/2/3/4");
  ret = taosMulMkDir(dir6);
  EXPECT_NE(ret, 0);

  char testDir[PATH_MAX];
  buildTestPath(testDir, sizeof(testDir), "");
  taosRemoveDir(testDir);
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

  char dir3[PATH_MAX];
  buildTestPath(dir3, sizeof(dir3), "1/2/3/4");
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

  char testFile[PATH_MAX];
  char dir6[PATH_MAX];
  buildTestPath(testFile, sizeof(testFile), "test-file");
  TdFilePtr testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);
  buildTestPath(dir6, sizeof(dir6), "test-file/1/2/3/4");
  ret = taosMulModeMkDir(dir6, 777, true);
  EXPECT_NE(ret, 0);

  const char* dir7 = "tdengine-test-dir/1/2/3/5";
  taosRemoveDir(dir7);
  ret = taosMulModeMkDir(dir7, 999, true);
  EXPECT_EQ(ret, 0);
  ret = taosMulModeMkDir(dir7, 999, false);
  EXPECT_EQ(ret, 0);

  char testDir[PATH_MAX];
  buildTestPath(testDir, sizeof(testDir), "");
  taosRemoveDir(testDir);
}

TEST(osDirTests, taosRemoveOldFiles) {
  int32_t ret = 0;
  char testDir[PATH_MAX];
  char testFile[PATH_MAX];
  char testDir2[PATH_MAX];
  char testFile3[PATH_MAX];
  char testFile4[PATH_MAX];
  char testFile5[PATH_MAX];
  char testFile6[PATH_MAX];
  
  buildTestPath(testDir, sizeof(testDir), "");
  buildTestPath(testFile, sizeof(testFile), "test-file");
  buildTestPath(testDir2, sizeof(testDir2), "test-dir2");
  buildTestPath(testFile3, sizeof(testFile3), "log.1433726073.gz");
  buildTestPath(testFile4, sizeof(testFile4), "log.80.gz");
  snprintf(testFile5, sizeof(testFile5), "%stdengine-test-dir%slog.%d.gz", TD_TMP_DIR_PATH, TD_DIRSEP, taosGetTimestampSec());
  buildTestPath(testFile6, sizeof(testFile6), "log.1433726073.gz");
  
  if (taosDirExist(testDir)) {
    taosRemoveDir(testDir);
  }
  taosRemoveDir(testDir);
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  taosRemoveOldFiles(testFile, 10);

  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr3 = taosCreateFile(testFile3, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr3, nullptr);
  ret = taosCloseFile(&testFilePtr3);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr4 = taosCreateFile(testFile4, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr4, nullptr);
  ret = taosCloseFile(&testFilePtr4);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr5 = taosCreateFile(testFile5, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr5, nullptr);
  ret = taosCloseFile(&testFilePtr5);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr6 = taosCreateFile(testFile6, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr6, nullptr);
  ret = taosCloseFile(&testFilePtr6);
  EXPECT_EQ(ret, 0);

  taosRemoveOldFiles(testDir, 10);

  bool exist = taosDirExist(testDir);
  EXPECT_EQ(exist, true);

  taosRemoveDir(testDir);
}

TEST(osDirTests, taosExpandDir) {
  int32_t ret = 0;
  char testDir[PATH_MAX];
  buildTestPath(testDir, sizeof(testDir), "");
  
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

  taosRemoveDir(testDir);
}

TEST(osDirTests, taosRealPath) {
  int32_t ret = 0;
  char testDir[PATH_MAX * 2];
  buildTestPath(testDir, sizeof(testDir), "");
  
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

  taosRemoveDir(testDir);
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

  int32_t ret = 0;
  char testDir[PATH_MAX];
  char testFile[PATH_MAX];
  char testDir2[PATH_MAX];
  char testFile2[PATH_MAX];
  
  buildTestPath(testDir, sizeof(testDir), "");
  buildTestPath(testFile, sizeof(testFile), "test-file");
  buildTestPath(testDir2, sizeof(testDir2), "test-dir2");
  buildTestPath(testFile2, sizeof(testFile2), "test-dir2/test-file2");
  
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  TdDirPtr dir = taosOpenDir(testFile);
  EXPECT_EQ(dir, nullptr);

  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr2 = taosCreateFile(testFile2, TD_FILE_CREATE);
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

  taosRemoveDir(testDir);
}

TEST(osDirTests, taosGetDirSize) {
  TdDirEntryPtr rddir = taosReadDir(NULL);
  EXPECT_EQ(rddir, nullptr);

  int32_t ret = 0;
  char testDir[PATH_MAX];
  char testFile[PATH_MAX];
  char testDir2[PATH_MAX];
  char testFile2[PATH_MAX];
  
  buildTestPath(testDir, sizeof(testDir), "");
  buildTestPath(testFile, sizeof(testFile), "test-file");
  buildTestPath(testDir2, sizeof(testDir2), "test-dir2");
  buildTestPath(testFile2, sizeof(testFile2), "test-dir2/test-file2");
  
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr = taosCreateFile(testFile, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr, nullptr);
  ret = taosCloseFile(&testFilePtr);
  EXPECT_EQ(ret, 0);

  TdDirPtr dir = taosOpenDir(testFile);
  EXPECT_EQ(dir, nullptr);

  ret = taosMkDir(testDir2);
  EXPECT_EQ(ret, 0);

  TdFilePtr testFilePtr2 = taosCreateFile(testFile2, TD_FILE_CREATE);
  EXPECT_NE(testFilePtr2, nullptr);
  ret = taosCloseFile(&testFilePtr2);
  EXPECT_EQ(ret, 0);

  int64_t size = -1;
  ret = taosGetDirSize(testFile, &size);
  EXPECT_NE(ret, 0);

  ret = taosGetDirSize(testDir, &size);
  EXPECT_EQ(ret, 0);

  taosRemoveDir(testDir);

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

TEST(osDirTests, taosGetCwd) {
  char cwdBuf[1024] = {0};
  taosGetCwd(cwdBuf, sizeof(cwdBuf));
  EXPECT_GT(strlen(cwdBuf), 0);
}

TEST(osDirTests, taosAppPath) {
  char appPath[PATH_MAX] = {0};
  int32_t ret = taosAppPath(appPath, sizeof(appPath));
  EXPECT_EQ(ret, 0);
  EXPECT_GT(strlen(appPath), 0);

  // Test with small buffer
  char smallBuf[10] = {0};
  ret = taosAppPath(smallBuf, sizeof(smallBuf));
  // Should not crash, behavior may vary
}

TEST(osDirTests, taosDllOperations) {
  // Test loading a system library (libc on Linux)
#ifndef WINDOWS
  void* handle = taosLoadDll("libc.so.6");
  if (handle != NULL) {
    // Load a function from libc
    void* funcPtr = taosLoadDllFunc(handle, "printf");
    EXPECT_NE(funcPtr, nullptr);

    // Try loading a non-existent function
    void* invalidFunc = taosLoadDllFunc(handle, "this_function_does_not_exist_xyz123");
    EXPECT_EQ(invalidFunc, nullptr);

    // Close the DLL
    taosCloseDll(handle);
  }
#endif

  // Test loading a non-existent library
  void* invalidHandle = taosLoadDll("/invalid/path/to/nonexistent.so");
  EXPECT_EQ(invalidHandle, nullptr);

  // Test NULL handle operations
  taosCloseDll(NULL);
  void* nullFunc = taosLoadDllFunc(NULL, "some func");
  EXPECT_EQ(nullFunc, nullptr);
}

TEST(osDirTests, taosMkDirExistingDir) {
  char testDir[PATH_MAX];
  buildTestPath(testDir, sizeof(testDir), "mkdirexist");
  
  // Clean up first
  taosRemoveDir(testDir);
  
  // Ensure parent directory exists
  char parentDir[PATH_MAX];
  buildTestPath(parentDir, sizeof(parentDir), "");
  taosMulMkDir(parentDir);
  
  // Create directory
  int32_t ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);
  
  // Try creating the same directory again (should succeed or return 0)
  ret = taosMkDir(testDir);
  EXPECT_EQ(ret, 0);
  
  // Clean up
  taosRemoveDir(testDir);
  taosRemoveDir(parentDir);
}

TEST(osDirTests, taosCloseDirWithNullPointer) {
  TdDirPtr nullPtr = NULL;
  int32_t code = taosCloseDir(&nullPtr);
  EXPECT_NE(code, 0);
}

TEST(osDirTests, taosDirEntryIsDirWithNull) {
  bool isDir = taosDirEntryIsDir(NULL);
  EXPECT_EQ(isDir, false);
}

TEST(osDirTests, taosExpandDirErrorCases) {
  int32_t ret = 0;
  char fullpath[1024] = {0};
  
  // Test with very long directory name that might cause expansion issues
  char longDir[PATH_MAX * 2];
  memset(longDir, 'a', sizeof(longDir) - 1);
  longDir[sizeof(longDir) - 1] = '\0';
  
  // This might trigger error paths in wordexp
  ret = taosExpandDir(longDir, fullpath, sizeof(fullpath));
  // Result may vary, just ensure it doesn't crash
}
