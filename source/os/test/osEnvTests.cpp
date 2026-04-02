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

// Helper function to get platform-specific temp directory
static const char* getPlatformTempDir() {
#ifdef WINDOWS
  const char *tmpDir = getenv("TEMP");
  if (tmpDir == NULL) tmpDir = getenv("TMP");
  if (tmpDir == NULL) tmpDir = "C:\\Windows\\Temp";
  return tmpDir;
#elif defined(_TD_DARWIN_64)
  const char *tmpDir = getenv("TMPDIR");
  if (tmpDir == NULL) tmpDir = "/tmp";
  return tmpDir;
#else
  return "/tmp";
#endif
}

TEST(osEnvTests, osDefaultInit) {
  int32_t ret = 0;

  strcpy(tsTimezoneStr, "");
  strcpy(tsTempDir, "");
  tsNumOfCores = 0;
  ret = osDefaultInit();
  EXPECT_EQ(ret, 0);

  // Use platform-specific temp directory
  strcpy(tsTempDir, getPlatformTempDir());
  ret = osDefaultInit();
  EXPECT_EQ(ret, 0);

  // Test tsNumOfCores < 2 branch
  // Save original value and restore after test
  float originalCores = tsNumOfCores;
  tsNumOfCores = 1;
  ret = osDefaultInit();
  EXPECT_EQ(ret, 0);
  // After osDefaultInit, if cores < 2, it should be set to 2
  EXPECT_GE(tsNumOfCores, 2);
  tsNumOfCores = originalCores;

  osCleanup();
}

TEST(osEnvTests, osUpdate) {
  int32_t ret = 0;

  strcpy(tsLogDir, "");
  strcpy(tsDataDir, "");
  strcpy(tsTempDir, "");

  ret = osUpdate();
  EXPECT_EQ(ret, 0);

  // Test with valid directories to cover taosGetDiskSize calls
  const char *tmpDir = getPlatformTempDir();
  strcpy(tsLogDir, tmpDir);
  strcpy(tsDataDir, tmpDir);
  strcpy(tsTempDir, tmpDir);

  ret = osUpdate();
  EXPECT_EQ(ret, 0);
}

TEST(osEnvTests, osSufficient) {
  bool ret = 0;

  tsLogSpace.size.avail = 10000;
  tsDataSpace.size.avail = 10000;
  tsTempSpace.size.avail = 10000;
  tsLogSpace.reserved = 2000;
  tsDataSpace.reserved = 2000;
  tsDataSpace.reserved = 2000;

  ret = osLogSpaceAvailable();
  EXPECT_EQ(ret, true);

  ret = osTempSpaceAvailable();
  EXPECT_EQ(ret, true);

  ret = osDataSpaceAvailable();
  EXPECT_EQ(ret, true);

  ret = osLogSpaceSufficient();
  EXPECT_EQ(ret, true);

  ret = osDataSpaceSufficient();
  EXPECT_EQ(ret, true);

  ret = osTempSpaceSufficient();
  EXPECT_EQ(ret, true);

  taosSetSystemLocale(NULL);
  taosSetSystemLocale("1");

  osSetProcPath(1, NULL);
  osSetProcPath(0, (char **)&ret);

  // Test osSetProcPath with valid arguments (platform-specific)
#ifdef WINDOWS
  char *argv[] = {(char *)"C:\\Program Files\\TDengine\\taosd.exe", NULL};
#elif defined(_TD_DARWIN_64)
  char *argv[] = {(char *)"/usr/local/bin/taosd", NULL};
#else
  char *argv[] = {(char *)"/usr/bin/taosd", NULL};
#endif
  osSetProcPath(1, argv);
  EXPECT_EQ(tsProcPath, argv[0]);
}

TEST(osEnvTests, osSetTimezone) {
  int32_t ret = 0;

  // Test osSetTimezone function
  ret = osSetTimezone("UTC");
  EXPECT_EQ(ret, 0);

  ret = osSetTimezone("Asia/Shanghai");
  EXPECT_EQ(ret, 0);
}

TEST(osEnvTests, osSpaceEdgeCases) {
  bool ret = false;

  // Test edge case: avail = 0
  tsLogSpace.size.avail = 0;
  tsDataSpace.size.avail = 0;
  tsTempSpace.size.avail = 0;

  ret = osLogSpaceAvailable();
  EXPECT_EQ(ret, false);

  ret = osTempSpaceAvailable();
  EXPECT_EQ(ret, false);

  ret = osDataSpaceAvailable();
  EXPECT_EQ(ret, false);

  // Test edge case: avail <= reserved
  tsLogSpace.size.avail = 1000;
  tsDataSpace.size.avail = 1000;
  tsTempSpace.size.avail = 1000;
  tsLogSpace.reserved = 1000;
  tsDataSpace.reserved = 1000;
  tsTempSpace.reserved = 1000;

  ret = osLogSpaceSufficient();
  EXPECT_EQ(ret, false);

  ret = osDataSpaceSufficient();
  EXPECT_EQ(ret, false);

  ret = osTempSpaceSufficient();
  EXPECT_EQ(ret, false);
}