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

TEST(osEnvTests, osDefaultInit) {
  int32_t ret = 0;

  strcpy(tsTimezoneStr, "");
  strcpy(tsTempDir, "");
  tsNumOfCores = 0;
  ret = osDefaultInit();
  EXPECT_EQ(ret, 0);

  strcpy(tsTempDir, "/tmp");
  ret = osDefaultInit();
  EXPECT_EQ(ret, 0);

  osCleanup();
}

TEST(osEnvTests, osUpdate) {
  int32_t ret = 0;

  strcpy(tsLogDir, "");
  strcpy(tsDataDir, "");
  strcpy(tsTempDir, "");

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
}