#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <random>

#include "tarray.h"

namespace {

static void remove_batch_test() {
  SArray *pa = (SArray*) taosArrayInit(4, sizeof(int32_t));

  for(int32_t i = 0; i < 20; ++i) {
    int32_t a = i;
    taosArrayPush(pa, &a);
  }

  SArray* delList = (SArray*)taosArrayInit(4, sizeof(int32_t));
  taosArrayRemoveBatch(pa, (const int32_t*) TARRAY_GET_START(delList), taosArrayGetSize(delList));
  EXPECT_EQ(taosArrayGetSize(pa), 20);

  int32_t a = 5;
  taosArrayPush(delList, &a);

  taosArrayRemoveBatch(pa, (const int32_t*) TARRAY_GET_START(delList), taosArrayGetSize(delList));
  EXPECT_EQ(taosArrayGetSize(pa), 19);
  EXPECT_EQ(*(int*)taosArrayGet(pa, 5), 6);

  taosArrayInsert(pa, 5, &a);
  EXPECT_EQ(taosArrayGetSize(pa), 20);
  EXPECT_EQ(*(int*)taosArrayGet(pa, 5), 5);

  taosArrayClear(delList);

  a = 6;
  taosArrayPush(delList, &a);

  a = 9;
  taosArrayPush(delList, &a);

  a = 14;
  taosArrayPush(delList, &a);
  taosArrayRemoveBatch(pa, (const int32_t*) TARRAY_GET_START(delList), taosArrayGetSize(delList));
  EXPECT_EQ(taosArrayGetSize(pa), 17);
}
}  // namespace

TEST(arrayTest, array_list_test) {
  remove_batch_test();
}
