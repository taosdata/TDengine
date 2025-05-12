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

TEST(osSystemTest, osSystem1) {
  char tmp[4096] = "test";
#ifdef _TD_DARWIN_64
  taosLogTraceToBuf(tmp, sizeof(tmp), 4);
#elif !defined(WINDOWS)
  taosLogTraceToBuf(tmp, sizeof(tmp), 3);
#else
  taosLogTraceToBuf(tmp, sizeof(tmp), 8);
#endif
  double  cpu_engine;
  double  cpu_system;
  float   cpu_cores;
  int64_t mem_engine;     // KB
  int64_t mem_system;     // KB

  taosGetCpuUsage(&cpu_system, &cpu_engine);
  (void)taosGetCpuCores(&cpu_cores, false);

  (void)taosGetProcMemory(&mem_engine);
  (void)taosGetSysMemory(&mem_system);
  (void)printf("cpu_engine: %f  cpu_system: %f\n", cpu_engine, cpu_system);
  (void)printf("cpu_cores: %f\n", cpu_cores);
  ASSERT_GT(cpu_cores, 0);
  ASSERT_GE(mem_engine, 0);
  ASSERT_GE(mem_system, 0);

  float numOfCores = 0;
  int32_t res = taosGetCpuInfo(tmp, 4096, &numOfCores);
  (void)printf("cpu info: %s\n", tmp);
  ASSERT_EQ(res, 0);
}


TEST(osSystemTest, systemUUIDTest) {
  char uuid1[38];
  memset(uuid1, 0, sizeof(uuid1));
  taosGetSystemUUIDLimit36(uuid1, sizeof(uuid1));
  ASSERT_EQ(strlen(uuid1), 36);

  char uuid2[34];
  memset(uuid2, 0, sizeof(uuid2));
  taosGetSystemUUIDLimit36(uuid2, sizeof(uuid2));
  ASSERT_EQ(strlen(uuid2), 33);

  char uuid3[36];
  memset(uuid3, 0, sizeof(uuid3));
  taosGetSystemUUIDLimit36(uuid3, sizeof(uuid3));
  ASSERT_EQ(strlen(uuid3), 35);

  char uuid4[2];
  memset(uuid4, 0, sizeof(uuid4));
  taosGetSystemUUIDLimit36(uuid4, sizeof(uuid4));
  ASSERT_EQ(strlen(uuid4), 1);

  char uuid5[36];
  memset( uuid5, 0, sizeof(uuid5));
  taosGetSystemUUIDLimit36(uuid5, sizeof(uuid5));
  ASSERT_EQ(strlen(uuid5), 35);

  char uuid6[37];
  memset( uuid6, 0, sizeof(uuid6));
  taosGetSystemUUIDLimit36(uuid6, sizeof(uuid6));
  ASSERT_EQ(strlen(uuid6), 36);

  char uuid7[1];
  memset(uuid7, 0, sizeof(uuid7));
  taosGetSystemUUIDLimit36(uuid7, sizeof(uuid7));
  ASSERT_EQ(strlen(uuid7), 0);
}

TEST(osSystemTest, systemUUIDTest2) {
  char uuid1[38];
  memset(uuid1, 0, sizeof(uuid1));
  taosGetSystemUUIDLen(uuid1, sizeof(uuid1));
  ASSERT_EQ(strlen(uuid1), sizeof(uuid1) - 1);

  char uuid2[34];
  memset(uuid2, 0, sizeof(uuid2));
  taosGetSystemUUIDLen(uuid2, sizeof(uuid2));
  ASSERT_EQ(strlen(uuid2), sizeof(uuid2) - 1);

  char uuid3[36];
  memset(uuid3, 0, sizeof(uuid3));
  taosGetSystemUUIDLen(uuid3, sizeof(uuid3));
  ASSERT_EQ(strlen(uuid3), sizeof(uuid3) - 1);

  char uuid4[2];
  memset(uuid4, 0, sizeof(uuid4));
  taosGetSystemUUIDLen(uuid4, sizeof(uuid4));
  ASSERT_EQ(strlen(uuid4), sizeof(uuid4) - 1);

  char uuid5[36];
  memset( uuid5, 0, sizeof(uuid5));
  taosGetSystemUUIDLen(uuid5, sizeof(uuid5));
  ASSERT_EQ(strlen(uuid5), sizeof(uuid5) - 1);

  char uuid6[37];
  memset( uuid6, 0, sizeof(uuid6));
  taosGetSystemUUIDLen(uuid6, sizeof(uuid6));
  ASSERT_EQ(strlen(uuid6), sizeof(uuid6) - 1);

  char uuid7[1];
  memset(uuid7, 0, sizeof(uuid7));
  taosGetSystemUUIDLen(uuid7, sizeof(uuid7));
  ASSERT_EQ(strlen(uuid7), sizeof(uuid7) - 1);

  char uuid8[40];
  memset(uuid8, 0, sizeof(uuid8));
  taosGetSystemUUIDLen(uuid8, sizeof(uuid8));
  ASSERT_EQ(strlen(uuid8), sizeof(uuid8) - 1);

  char uuid9[73];
  memset(uuid9, 0, sizeof(uuid9));
  taosGetSystemUUIDLen(uuid9, sizeof(uuid9));
  ASSERT_EQ(strlen(uuid9), sizeof(uuid9) - 1);
}
