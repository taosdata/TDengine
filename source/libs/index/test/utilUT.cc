#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "index.h"
#include "indexCache.h"
#include "indexComm.h"
#include "indexFst.h"
#include "indexFstCommon.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexTfile.h"
#include "indexUtil.h"
#include "tcoding.h"
#include "tglobal.h"
#include "tskiplist.h"
#include "tutil.h"

class UtilEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    src = (SArray *)taosArrayInit(2, sizeof(void *));
    for (int i = 0; i < 3; i++) {
      SArray *m = taosArrayInit(10, sizeof(uint64_t));
      taosArrayPush(src, &m);
    }

    rslt = (SArray *)taosArrayInit(10, sizeof(uint64_t));
  }
  virtual void TearDown() {
    for (int i = 0; i < taosArrayGetSize(src); i++) {
      SArray *m = (SArray *)taosArrayGetP(src, i);
      taosArrayDestroy(m);
    }
    taosArrayDestroy(src);
  }

  SArray *src;
  SArray *rslt;
};

static void clearSourceArray(SArray *p) {
  for (int i = 0; i < taosArrayGetSize(p); i++) {
    SArray *m = (SArray *)taosArrayGetP(p, i);
    taosArrayClear(m);
  }
}
static void clearFinalArray(SArray *p) { taosArrayClear(p); }
TEST_F(UtilEnv, intersectionSimpleResult) {
  SArray *f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < 10; i++) {
    uint64_t val = i;
    taosArrayPush(f, &val);
  }
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < 10; i++) {
    uint64_t val = i;
    taosArrayPush(f, &val);
  }
  f = (SArray *)taosArrayGetP(src, 2);
  for (int i = 0; i < 10; i++) {
    uint64_t val = i;
    taosArrayPush(f, &val);
  }
  iIntersection(src, rslt);
  assert(taosArrayGetSize(rslt) == 10);

  clearSourceArray(src);
  clearFinalArray(rslt);
}
TEST_F(UtilEnv, intersectMultiEmptyResult) {
  SArray *f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 10; i < 20; i++) {
    uint64_t val = i;
    taosArrayPush(f, &val);
  }
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < 10; i++) {
    uint64_t val = i;
    taosArrayPush(f, &val);
  }
  f = (SArray *)taosArrayGetP(src, 2);
  for (int i = 20; i < 30; i++) {
    uint64_t val = i;
    taosArrayPush(f, &val);
  }
  // empty source
  iIntersection(src, rslt);
  assert(taosArrayGetSize(rslt) == 0);
  clearSourceArray(src);
  clearFinalArray(rslt);
}
TEST_F(UtilEnv, intersectSimpleEmpty) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  iIntersection(src, rslt);
  assert(taosArrayGetSize(rslt) == 0);
}
TEST_F(UtilEnv, intersect01) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr1[] = {2, 3, 4, 5};
  SArray * f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
    taosArrayPush(f, &arr1[i]);
  }

  uint64_t arr2[] = {1, 2, 3, 5};
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < sizeof(arr2) / sizeof(arr2[0]); i++) {
    taosArrayPush(f, &arr2[i]);
  }

  uint64_t arr3[] = {3, 5, 10, 11};
  f = (SArray *)taosArrayGetP(src, 2);
  for (int i = 0; i < sizeof(arr3) / sizeof(arr3[0]); i++) {
    taosArrayPush(f, &arr3[i]);
  }

  iIntersection(src, rslt);
  assert(taosArrayGetSize(rslt) == 2);
}
TEST_F(UtilEnv, intersect02) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr1[] = {13, 14, 15};
  SArray * f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
    taosArrayPush(f, &arr1[i]);
  }

  uint64_t arr2[] = {8, 10, 12, 13};
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < sizeof(arr2) / sizeof(arr2[0]); i++) {
    taosArrayPush(f, &arr2[i]);
  }

  uint64_t arr3[] = {9, 10, 11};
  f = (SArray *)taosArrayGetP(src, 2);
  for (int i = 0; i < sizeof(arr3) / sizeof(arr3[0]); i++) {
    taosArrayPush(f, &arr3[i]);
  }

  iIntersection(src, rslt);
  assert(taosArrayGetSize(rslt) == 0);
}
TEST_F(UtilEnv, 01union) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr1[] = {13, 14, 15};
  SArray * f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
    taosArrayPush(f, &arr1[i]);
  }
  iUnion(src, rslt);
  assert(taosArrayGetSize(rslt) == 3);
}
TEST_F(UtilEnv, 02union) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr1[] = {13, 14, 15};
  SArray * f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
    taosArrayPush(f, &arr1[i]);
  }

  uint64_t arr2[] = {13, 14, 15};
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < sizeof(arr2) / sizeof(arr2[0]); i++) {
    taosArrayPush(f, &arr2[i]);
  }
  iUnion(src, rslt);
  assert(taosArrayGetSize(rslt) == 3);
}
TEST_F(UtilEnv, 03union) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr1[] = {13, 16, 18, 20};
  SArray * f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
    taosArrayPush(f, &arr1[i]);
  }

  uint64_t arr2[] = {0, 12, 13, 20, 23};
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < sizeof(arr2) / sizeof(arr2[0]); i++) {
    taosArrayPush(f, &arr2[i]);
  }
  uint64_t arr3[] = {1, 12, 13, 16, 17};
  f = (SArray *)taosArrayGetP(src, 2);
  for (int i = 0; i < sizeof(arr3) / sizeof(arr3[0]); i++) {
    taosArrayPush(f, &arr3[i]);
  }
  iUnion(src, rslt);
  assert(taosArrayGetSize(rslt) == 9);
}
TEST_F(UtilEnv, 04union) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr1[] = {1, 4, 5, 6};
  SArray * f = (SArray *)taosArrayGetP(src, 0);
  for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
    taosArrayPush(f, &arr1[i]);
  }

  uint64_t arr2[] = {7, 8, 10};
  f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < sizeof(arr2) / sizeof(arr2[0]); i++) {
    taosArrayPush(f, &arr2[i]);
  }

  uint64_t arr3[] = {20, 21, 30, 100, 120};
  f = (SArray *)taosArrayGetP(src, 2);
  for (int i = 0; i < sizeof(arr3) / sizeof(arr3[0]); i++) {
    taosArrayPush(f, &arr3[i]);
  }
  iUnion(src, rslt);
  assert(taosArrayGetSize(rslt) == 12);
}
TEST_F(UtilEnv, 05unionExcept) {
  clearSourceArray(src);
  clearFinalArray(rslt);

  uint64_t arr2[] = {7};
  SArray * f = (SArray *)taosArrayGetP(src, 1);
  for (int i = 0; i < sizeof(arr2) / sizeof(arr2[0]); i++) {
    taosArrayPush(f, &arr2[i]);
  }

  iUnion(src, rslt);

  SArray *ept = taosArrayInit(0, sizeof(uint64_t));
  iExcept(rslt, ept);
  EXPECT_EQ(taosArrayGetSize(rslt), 1);
}
TEST_F(UtilEnv, 01Except) {
  SArray *total = taosArrayInit(4, sizeof(uint64_t));
  {
    uint64_t arr1[] = {1, 4, 5, 6};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(total, &arr1[i]);
    }
  }

  SArray *except = taosArrayInit(4, sizeof(uint64_t));
  {
    uint64_t arr1[] = {1, 4, 5, 6};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(except, &arr1[i]);
    }
  }
  iExcept(total, except);
  ASSERT_EQ(taosArrayGetSize(total), 0);

  taosArrayClear(total);
  taosArrayClear(except);

  {
    uint64_t arr1[] = {1, 4, 5, 6, 7, 8};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(total, &arr1[i]);
    }
  }

  {
    uint64_t arr1[] = {2, 4, 5, 6};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(except, &arr1[i]);
    }
  }
  iExcept(total, except);
  ASSERT_EQ(taosArrayGetSize(total), 3);

  taosArrayClear(total);
  taosArrayClear(except);
  {
    uint64_t arr1[] = {1, 4, 5, 6, 7, 8, 10, 100};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(total, &arr1[i]);
    }
  }

  {
    uint64_t arr1[] = {2, 4, 5, 6};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(except, &arr1[i]);
    }
  }
  iExcept(total, except);
  ASSERT_EQ(taosArrayGetSize(total), 5);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 0), 1);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 1), 7);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 2), 8);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 3), 10);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 4), 100);

  taosArrayClear(total);
  taosArrayClear(except);
  {
    uint64_t arr1[] = {1, 100};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(total, &arr1[i]);
    }
  }

  {
    uint64_t arr1[] = {2, 4, 5, 6};
    for (int i = 0; i < sizeof(arr1) / sizeof(arr1[0]); i++) {
      taosArrayPush(except, &arr1[i]);
    }
  }
  iExcept(total, except);
  ASSERT_EQ(taosArrayGetSize(total), 2);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 0), 1);
  ASSERT_EQ(*(uint64_t *)taosArrayGet(total, 1), 100);
}
TEST_F(UtilEnv, testFill) {
  for (int i = 0; i < 1000000; i++) {
    int64_t val = i;
    char    buf[65] = {0};
    idxInt2str(val, buf, 1);
    EXPECT_EQ(val, taosStr2int64(buf));
  }
  for (int i = 0; i < 1000000; i++) {
    int64_t val = 0 - i;
    char    buf[65] = {0};
    idxInt2str(val, buf, -1);
    EXPECT_EQ(val, taosStr2int64(buf));
  }
}
TEST_F(UtilEnv, TempResult) {
  SIdxTRslt *relt = idxTRsltCreate();

  SArray *f = taosArrayInit(0, sizeof(uint64_t));

  uint64_t val = UINT64_MAX - 1;
  taosArrayPush(relt->add, &val);
  idxTRsltMergeTo(relt, f);
  EXPECT_EQ(taosArrayGetSize(f), 1);
}
TEST_F(UtilEnv, TempResultExcept) {
  SIdxTRslt *relt = idxTRsltCreate();

  SArray *f = taosArrayInit(0, sizeof(uint64_t));

  uint64_t val = UINT64_MAX;
  taosArrayPush(relt->add, &val);
  idxTRsltMergeTo(relt, f);
  EXPECT_EQ(taosArrayGetSize(f), 1);
}

TEST_F(UtilEnv, testDictComm) {
  int32_t count = COMMON_INPUTS_LEN;
  for (int i = 0; i < 256; i++) {
    uint8_t v = COMMON_INPUTS_INV[i];
    EXPECT_EQ(COMMON_INPUTS[v], i);
  }
}
