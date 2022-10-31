#include <gtest/gtest.h>
#include <stdlib.h>
#include <time.h>
#include <random>

#include "tarray.h"
#include "tcompare.h"
/*
namespace {

static void remove_batch_test() {
SArray* pa = (SArray*)taosArrayInit(4, sizeof(int32_t));

for (int32_t i = 0; i < 20; ++i) {
  int32_t a = i;
  taosArrayPush(pa, &a);
}

SArray* delList = (SArray*)taosArrayInit(4, sizeof(int32_t));
taosArrayRemoveBatch(pa, (const int32_t*)TARRAY_GET_START(delList), taosArrayGetSize(delList));
EXPECT_EQ(taosArrayGetSize(pa), 20);

int32_t a = 5;
taosArrayPush(delList, &a);

taosArrayRemoveBatch(pa, (const int32_t*)TARRAY_GET_START(delList), taosArrayGetSize(delList));
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
taosArrayRemoveBatch(pa, (const int32_t*)TARRAY_GET_START(delList), taosArrayGetSize(delList));
EXPECT_EQ(taosArrayGetSize(pa), 17);

taosArrayDestroy(pa);
taosArrayDestroy(delList);
}
}  // namespace

TEST(arrayTest, array_list_test) { remove_batch_test(); }
*/
TEST(arrayTest, array_search_test) {
  SArray* pa = (SArray*)taosArrayInit(4, sizeof(int32_t));

  for (int32_t i = 10; i < 20; ++i) {
    int32_t a = i;
    taosArrayPush(pa, &a);
  }

  for (int i = 0; i < 30; i++) {
    int32_t  k = i;
    int32_t* pRet = (int32_t*)taosArraySearch(pa, &k, compareInt32Val, TD_GE);
    int32_t  idx = taosArraySearchIdx(pa, &k, compareInt32Val, TD_GE);

    if (pRet == NULL) {
      ASSERT_EQ(idx, -1);
    } else {
      ASSERT_EQ(taosArrayGet(pa, idx), pRet);
    }

    pRet = (int32_t*)taosArraySearch(pa, &k, compareInt32Val, TD_LE);
    idx = taosArraySearchIdx(pa, &k, compareInt32Val, TD_LE);

    if (pRet == NULL) {
      ASSERT_EQ(idx, -1);
    } else {
      ASSERT_EQ(taosArrayGet(pa, idx), pRet);
    }
  }

  taosArrayDestroy(pa);
}
