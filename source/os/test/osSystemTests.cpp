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
  taosGetCpuCores(&cpu_cores, false);

  taosGetProcMemory(&mem_engine);
  taosGetSysMemory(&mem_system);
  printf("cpu_engine: %f  cpu_system: %f\n", cpu_engine, cpu_system);
  printf("cpu_cores: %f\n", cpu_cores);
  ASSERT_GT(cpu_cores, 0);
  ASSERT_GE(mem_engine, 0);
  ASSERT_GE(mem_system, 0);

  float numOfCores = 0;
  int32_t res = taosGetCpuInfo(tmp, 4096, &numOfCores);
  printf("cpu info: %s\n", tmp);
  ASSERT_EQ(res, 0);
}
