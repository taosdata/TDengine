#include "gtest/gtest.h"

#include "tfreelist.h"

TEST(TD_UTIL_FREELIST_TEST, simple_test) {
  SFreeList fl;

  tFreeListInit(&fl);

  for (size_t i = 0; i < 1000; i++) {
    void *ptr = TFL_MALLOC(1024, &fl);
    GTEST_ASSERT_NE(ptr, nullptr);
  }

  tFreeListClear(&fl);
}