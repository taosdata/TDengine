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

// call taosArrayResize
TEST(arrayTest, array_data_correct) {
  SArray* pa = (SArray*)taosArrayInit(1, sizeof(int32_t));
  SArray* pa1 = (SArray*)taosArrayInit(1, sizeof(int32_t));
  size_t cnt = 1000;


  for (int32_t i = 0; i < cnt; ++i) {
    taosArrayPush(pa, &i);
    int32_t v = cnt + i;
    taosArrayPush(pa1, &v);
  }
  ASSERT_EQ(taosArrayGetSize(pa), cnt);
  ASSERT_EQ(taosArrayAddBatch(pa, NULL, 0), nullptr);
  ASSERT_NE(taosArrayAddBatch(pa, taosArrayGet(pa1, 0), cnt), nullptr);

  int32_t* pv = NULL;
  for (int32_t i = 0; i < cnt*2; i++) {
    pv = (int32_t*)taosArrayGet(pa, i);
    ASSERT_EQ(*pv, i);
  }

  taosArrayDestroy(pa);
  taosArrayDestroy(pa1);
}

// free
static void arrayFree(void *param) {
  void *pItem = *(void **)param;
  if (pItem != NULL) {
    taosMemoryFree(pItem);
  }
}

// string compare
static int32_t strCompare(const void *a, const void *b) {
  const char *x = *(const char **)a;
  const char *y = *(const char **)b;

  return strcmp(x, y);
}

// int32 compare
static int int32Compare(const void* a, const void* b) {
  int32_t l = *(int32_t*)a;
  int32_t r = *(int32_t*)b;
  return l - r;
}

// no need free
TEST(arrayTest, check_duplicate_nofree) {
  // no need free item
  int32_t count = 5;
  SArray* pa = taosArrayInit(1, sizeof(int32_t));
  for (int32_t i = 1; i <= count; i++) {
    for (int32_t j = 0; j < i; j++) {
      taosArrayPush(pa, &i);
      //printf(" nofree put i=%d v=%d\n",i, i);
    }
  }

  taosArrayRemoveDuplicate(pa, int32Compare, NULL);
  printf("nofree taosArrayRemoveDuplicate size=%d\n", (int32_t)taosArrayGetSize(pa));
  ASSERT_EQ(taosArrayGetSize(pa), count);
  for (int32_t i = 1; i <= count; i++) {
    int32_t v = *(int32_t*)taosArrayGet(pa, i-1);
    //printf(" nofree get i=%d v=%d\n",i, v);
    ASSERT_EQ(v, i);
  }

  taosArrayDestroy(pa);
}

// need free
TEST(arrayTest, check_duplicate_needfree) {
  // no need free item
  int32_t count = 5;
  const char* format="hello-word-%d";
  SArray *pa = taosArrayInit(1, sizeof(char *));
  for (int32_t i = 1; i <= count; i++) {
    for (int32_t j = 0; j < i; j++) {
      char *v = (char *)taosMemoryCalloc(100, sizeof(char));
      sprintf(v, format, i);
      //printf(" needfree put i=%d v=%s\n", i, v);
      taosArrayPush(pa, &v);
    }
  }

  taosArrayRemoveDuplicate(pa, strCompare, arrayFree);
  printf("needfree taosArrayRemoveDuplicate size=%d\n", (int32_t)taosArrayGetSize(pa));
  ASSERT_EQ(taosArrayGetSize(pa), count);
  char value[100];
  for (int32_t i = 1; i <= count; i++) {
    sprintf(value, format, i);
    char * v = (char *)taosArrayGetP(pa, i - 1);
    //printf(" needfree get i=%d v=%s\n", i, v);
    ASSERT_STREQ(v, value);
  }

  taosArrayClearP(pa, taosMemoryFree);
  taosArrayDestroyP(pa, taosMemoryFree);
}

// over all
TEST(arrayTest, check_overall) {
  
  ASSERT_EQ(taosArrayInit(1, 0), nullptr);
  ASSERT_EQ(taosArrayGet(NULL, 1), nullptr);
  ASSERT_EQ(taosArrayGetP(NULL, 1), nullptr);
  ASSERT_EQ(taosArrayInsert(NULL, 1, NULL), nullptr);

  //ASSERT_EQ(taosArrayInit(0x10000000, 10000), nullptr);
  //ASSERT_EQ(taosArrayInit_s(10000,0x10000000-1), nullptr);

  SArray* pa = taosArrayInit(1, sizeof(uint64_t));
  ASSERT_EQ(taosArrayGet(pa, 10), nullptr);
  ASSERT_EQ(taosArrayGetLast(pa), nullptr);

  uint64_t v = 100;
  taosArrayPush(pa, &v);
  taosArrayPopFrontBatch(pa, 100);
  FDelete fnNull = NULL;
  taosArrayClearEx(pa, fnNull);
  taosArrayDestroyEx(pa, fnNull);

  int32_t count = 5;
  uint64_t list[5]= {1,2,3,4,5};

  SArray* pb = taosArrayFromList(list, count, sizeof(uint64_t));
  for (int32_t i=0; i < count; i++) {
     ASSERT_EQ(*(uint64_t*)taosArrayGet(pb, i), list[i]);
  }  
  taosArrayDestroy(pb);
}