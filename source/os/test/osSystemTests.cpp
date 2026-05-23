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
  int64_t mem_free; // KB
  int64_t mem_cacheBuffer; // KB

  taosGetCpuUsage(&cpu_system, &cpu_engine);
  (void)taosGetCpuCores(&cpu_cores, false);

  (void)taosGetProcMemory(&mem_engine);
  (void)taosGetSysMemory(&mem_system, &mem_free, &mem_cacheBuffer);
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

// Tests for cgroup-aware resource detection (K8s/container environments)
TEST(osSystemTest, cgroupCpuCoresTest) {
  // taosGetCpuCores with physical=false should return cgroup-aware value
  float cores = 0;
  int32_t ret = taosGetCpuCores(&cores, false);
  ASSERT_EQ(ret, 0);
  ASSERT_GT(cores, 0);
  (void)printf("cgroup-aware cpu cores: %f\n", cores);

  // physical=true should always return sysconf value
  float physCores = 0;
  ret = taosGetCpuCores(&physCores, true);
  ASSERT_EQ(ret, 0);
  ASSERT_GT(physCores, 0);
  (void)printf("physical cpu cores: %f\n", physCores);

  // cgroup-aware cores should be <= physical cores
  ASSERT_LE(cores, physCores);
}

TEST(osSystemTest, cgroupTotalMemoryTest) {
  // Total memory should be positive and cgroup-aware
  int64_t totalKB = 0;
  int32_t ret = taosGetTotalMemory(&totalKB);
  ASSERT_EQ(ret, 0);
  ASSERT_GT(totalKB, 0);
  (void)printf("total memory (cgroup-aware): %" PRId64 " KB\n", totalKB);
}

TEST(osSystemTest, cgroupSysAvailMemoryTest) {
  int64_t availSize = 0;
  int32_t ret = taosGetSysAvailMemory(&availSize);
  ASSERT_EQ(ret, 0);
  ASSERT_GE(availSize, 0);
  (void)printf("available memory: %" PRId64 " bytes\n", availSize);
}

TEST(osSystemTest, cgroupSysMemoryTest) {
  int64_t usedKB = 0, freeKB = 0, cacheBufferKB = 0;
  int32_t ret = taosGetSysMemory(&usedKB, &freeKB, &cacheBufferKB);
  ASSERT_EQ(ret, 0);
  ASSERT_GE(usedKB, 0);
  ASSERT_GE(freeKB, 0);
  ASSERT_GE(cacheBufferKB, 0);
  (void)printf("sys memory - used: %" PRId64 " KB, free: %" PRId64 " KB, cache: %" PRId64 " KB\n",
               usedKB, freeKB, cacheBufferKB);
}

TEST(osSystemTest, cgroupCpuUsageTest) {
  double cpu_system = 0, cpu_engine = 0;

  // First call to initialize baselines
  int32_t ret = taosGetCpuUsage(&cpu_system, &cpu_engine);
  ASSERT_EQ(ret, 0);

  // Second call should compute meaningful deltas
  ret = taosGetCpuUsage(&cpu_system, &cpu_engine);
  ASSERT_EQ(ret, 0);
  ASSERT_GE(cpu_system, 0.0);
  ASSERT_GE(cpu_engine, 0.0);
  ASSERT_LE(cpu_system, 100.0);
  (void)printf("cpu usage - system: %f%%, engine: %f%%\n", cpu_system, cpu_engine);
}

TEST(osSystemTest, cgroupConsistencyTest) {
  // Verify basic sanity of memory values across all environments
  int64_t totalKB = 0;
  ASSERT_EQ(taosGetTotalMemory(&totalKB), 0);
  ASSERT_GT(totalKB, 0);

  int64_t usedKB = 0, freeKB = 0, cacheBufferKB = 0;
  ASSERT_EQ(taosGetSysMemory(&usedKB, &freeKB, &cacheBufferKB), 0);

  (void)printf("consistency check: total=%" PRId64 " KB, used=%" PRId64 " KB, free=%" PRId64 " KB, cache=%" PRId64 " KB\n",
               totalKB, usedKB, freeKB, cacheBufferKB);

  // These invariants hold regardless of cgroup vs /proc/meminfo source
  ASSERT_GE(usedKB, 0);
  ASSERT_GE(freeKB, 0);
  ASSERT_GE(cacheBufferKB, 0);
  ASSERT_LE(usedKB, totalKB);
  ASSERT_LE(freeKB, totalKB);
}

TEST(osSystemTests, taosDllOperations) {
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
