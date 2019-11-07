#include <iostream>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <stdint-gcc.h>
#include <limits.h>

#include "taos.h"
#include "ihash.h"
#include "ttime.h"

TEST(testCase, hashTest) {
  char dd[10] = {0};
  void* handle = taosInitIntHash(1024, sizeof(dd), taosHashInt);

  for(int32_t i = -100000; i < 100000; ++i) {
    taosAddIntHash(handle, (uint64_t) i, dd);
  }

  int64_t s = taosGetTimestampUs();
  for(int32_t i = -100000; i < 100000; ++i) {
    char* d = taosGetIntHashData(handle, (uint64_t) i);
    EXPECT_TRUE(d != NULL);
  }

  int64_t e = taosGetTimestampUs();
  printf("elapsed time for retrieving data from hash table:%lldus, avg:%fus\n", (e-s), (e-s)/200000.0);

  // test the too large data
  taosAddIntHash(handle, INT64_MAX, dd);
  char* res = taosGetIntHashData(handle, INT64_MAX);
  EXPECT_TRUE(res != NULL);

  taosCleanUpIntHash(handle);
}