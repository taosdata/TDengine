#include <gtest/gtest.h>
#include <limits.h>
#include <iostream>

#include "os.h"
#include "taos.h"
#include "taosdef.h"
#include "thash.h"
#include "tlog.h"

namespace {

typedef struct TESTSTRUCT {
  char* p;
} TESTSTRUCT;

// the simple test code for basic operations
void simpleTest() {
  SHashObj* hashTable =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);

  // put 400 elements in the hash table
  for (int32_t i = -200; i < 200; ++i) {
    taosHashPut(hashTable, (const char*)&i, sizeof(int32_t), (char*)&i, sizeof(int32_t));
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 400);

  for (int32_t i = 0; i < 200; ++i) {
    char* p = (char*)taosHashGet(hashTable, (const char*)&i, sizeof(int32_t));
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }

  for (int32_t i = 1000; i < 2000; ++i) {
    taosHashRemove(hashTable, (const char*)&i, sizeof(int32_t));
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 400);

  for (int32_t i = 0; i < 100; ++i) {
    taosHashRemove(hashTable, (const char*)&i, sizeof(int32_t));
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 300);

  for (int32_t i = 100; i < 150; ++i) {
    taosHashRemove(hashTable, (const char*)&i, sizeof(int32_t));
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 250);
  taosHashCleanup(hashTable);
}

void stringKeyTest() {
  auto* hashTable =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);

  char key[128] = {0};

  // put 200 elements in the hash table
  for (int32_t i = 0; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    taosHashPut(hashTable, key, len, (char*)&i, sizeof(int32_t));
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 1000);

  for (int32_t i = 0; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);

    char* p = (char*)taosHashGet(hashTable, key, len);
    ASSERT_TRUE(p != nullptr);

    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }

  for (int32_t i = 500; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);

    taosHashRemove(hashTable, key, len);
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 500);

  for (int32_t i = 0; i < 499; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);

    taosHashRemove(hashTable, key, len);
  }

  ASSERT_EQ(taosHashGetSize(hashTable), 1);

  taosHashCleanup(hashTable);
}

void functionTest() {}

/**
 * evaluate the performance issue, by add 10million elements in to hash table in
 * a single threads situation
 */
void noLockPerformanceTest() {
  auto* hashTable =
      (SHashObj*)taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);

  char    key[128] = {0};
  int32_t num = 5000;

  int64_t st = taosGetTimestampUs();

  // put 10M elements in the hash table
  for (int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    taosHashPut(hashTable, key, len, (char*)&i, sizeof(int32_t));
  }

  ASSERT_EQ(taosHashGetSize(hashTable), num);

  int64_t et = taosGetTimestampUs();
  printf("Elpased time:%" PRId64 " us to add %d elements, avg cost:%lf us\n", et - st, num, (et - st) / (double)num);

  st = taosGetTimestampUs();
  for (int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    char*   p = (char*)taosHashGet(hashTable, key, len);
    ASSERT_TRUE(p != nullptr);

    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }

  et = taosGetTimestampUs();
  printf("Elpased time:%" PRId64 " us to fetch all %d elements, avg cost:%lf us\n", et - st, num,
         (et - st) / (double)num);

  printf("The maximum length of overflow linklist in hash table is:%d\n", taosHashGetMaxOverflowLinkLength(hashTable));
  taosHashCleanup(hashTable);
}

void multithreadsTest() {
  // todo
}

// check the function robustness
void invalidOperationTest() {}

void acquireRleaseTest() {
  SHashObj* hashTable =
      (SHashObj*)taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);

  int32_t     key = 2;
  int32_t     code = 0;
  int32_t     num = 0;
  TESTSTRUCT  data = {0};
  const char* str1 = "abcdefg";
  const char* str2 = "aaaaaaa";
  const char* str3 = "123456789";

  data.p = (char*)taosMemoryMalloc(10);
  strcpy(data.p, str1);

  code = taosHashPut(hashTable, &key, sizeof(key), &data, sizeof(data));
  ASSERT_EQ(code, 0);

  TESTSTRUCT* pdata = (TESTSTRUCT*)taosHashAcquire(hashTable, &key, sizeof(key));
  ASSERT_TRUE(pdata != nullptr);
  ASSERT_TRUE(strcmp(pdata->p, str1) == 0);

  code = taosHashRemove(hashTable, &key, sizeof(key));
  ASSERT_EQ(code, 0);
  ASSERT_TRUE(strcmp(pdata->p, str1) == 0);

  num = taosHashGetSize(hashTable);
  ASSERT_EQ(num, 1);

  strcpy(pdata->p, str3);

  data.p = (char*)taosMemoryMalloc(10);
  strcpy(data.p, str2);
  code = taosHashPut(hashTable, &key, sizeof(key), &data, sizeof(data));
  ASSERT_EQ(code, 0);
  num = taosHashGetSize(hashTable);
  ASSERT_EQ(num, 2);

  printf("%s,expect:%s", pdata->p, str3);
  ASSERT_TRUE(strcmp(pdata->p, str3) == 0);

  taosMemoryFreeClear(pdata->p);

  taosHashRelease(hashTable, pdata);
  num = taosHashGetSize(hashTable);
  ASSERT_EQ(num, 1);

  taosHashCleanup(hashTable);
  taosMemoryFreeClear(data.p);
}

void perfTest() {
  SHashObj* hash1h =
      (SHashObj*)taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* hash1s =
      (SHashObj*)taosHashInit(1000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* hash10s =
      (SHashObj*)taosHashInit(10000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* hash100s =
      (SHashObj*)taosHashInit(100000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* hash1m =
      (SHashObj*)taosHashInit(1000000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* hash10m =
      (SHashObj*)taosHashInit(10000000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  SHashObj* hash100m =
      (SHashObj*)taosHashInit(100000000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);

  char* name = (char*)taosMemoryCalloc(50000000, 9);
  for (int64_t i = 0; i < 50000000; ++i) {
    sprintf(name + i * 9, "t%08" PRId64, i);
  }

  for (int64_t i = 0; i < 50; ++i) {
    taosHashPut(hash1h, name + i * 9, 9, &i, sizeof(i));
  }

  for (int64_t i = 0; i < 500; ++i) {
    taosHashPut(hash1s, name + i * 9, 9, &i, sizeof(i));
  }

  for (int64_t i = 0; i < 5000; ++i) {
    taosHashPut(hash10s, name + i * 9, 9, &i, sizeof(i));
  }

  for (int64_t i = 0; i < 50000; ++i) {
    taosHashPut(hash100s, name + i * 9, 9, &i, sizeof(i));
  }

  for (int64_t i = 0; i < 500000; ++i) {
    taosHashPut(hash1m, name + i * 9, 9, &i, sizeof(i));
  }

  for (int64_t i = 0; i < 5000000; ++i) {
    taosHashPut(hash10m, name + i * 9, 9, &i, sizeof(i));
  }

  for (int64_t i = 0; i < 50000000; ++i) {
    taosHashPut(hash100m, name + i * 9, 9, &i, sizeof(i));
  }

  int64_t start1h = taosGetTimestampMs();
  int64_t start1hCt = taosHashGetCompTimes(hash1h);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash1h, name + (i % 50) * 9, 9));
  }
  int64_t end1h = taosGetTimestampMs();
  int64_t end1hCt = taosHashGetCompTimes(hash1h);

  int64_t start1s = taosGetTimestampMs();
  int64_t start1sCt = taosHashGetCompTimes(hash1s);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash1s, name + (i % 500) * 9, 9));
  }
  int64_t end1s = taosGetTimestampMs();
  int64_t end1sCt = taosHashGetCompTimes(hash1s);

  int64_t start10s = taosGetTimestampMs();
  int64_t start10sCt = taosHashGetCompTimes(hash10s);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash10s, name + (i % 5000) * 9, 9));
  }
  int64_t end10s = taosGetTimestampMs();
  int64_t end10sCt = taosHashGetCompTimes(hash10s);

  int64_t start100s = taosGetTimestampMs();
  int64_t start100sCt = taosHashGetCompTimes(hash100s);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash100s, name + (i % 50000) * 9, 9));
  }
  int64_t end100s = taosGetTimestampMs();
  int64_t end100sCt = taosHashGetCompTimes(hash100s);

  int64_t start1m = taosGetTimestampMs();
  int64_t start1mCt = taosHashGetCompTimes(hash1m);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash1m, name + (i % 500000) * 9, 9));
  }
  int64_t end1m = taosGetTimestampMs();
  int64_t end1mCt = taosHashGetCompTimes(hash1m);

  int64_t start10m = taosGetTimestampMs();
  int64_t start10mCt = taosHashGetCompTimes(hash10m);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash10m, name + (i % 5000000) * 9, 9));
  }
  int64_t end10m = taosGetTimestampMs();
  int64_t end10mCt = taosHashGetCompTimes(hash10m);

  int64_t start100m = taosGetTimestampMs();
  int64_t start100mCt = taosHashGetCompTimes(hash100m);
  for (int64_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(hash100m, name + (i % 50000000) * 9, 9));
  }
  int64_t end100m = taosGetTimestampMs();
  int64_t end100mCt = taosHashGetCompTimes(hash100m);

  SArray* sArray[1000] = {0};
  for (int64_t i = 0; i < 1000; ++i) {
    sArray[i] = taosArrayInit(100000, 9);
  }
  int64_t cap = 4;
  while (cap < 100000000) cap = (cap << 1u);

  _hash_fn_t hashFp = taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY);
  int32_t    slotR = cap / 1000 + 1;
  for (int64_t i = 0; i < 10000000; ++i) {
    char*    p = name + (i % 50000000) * 9;
    uint32_t v = (*hashFp)(p, 9);
    taosArrayPush(sArray[(v % cap) / slotR], p);
  }
  SArray* slArray = taosArrayInit(100000000, 9);
  for (int64_t i = 0; i < 1000; ++i) {
    int32_t num = taosArrayGetSize(sArray[i]);
    SArray* pArray = sArray[i];
    for (int64_t m = 0; m < num; ++m) {
      char* p = (char*)taosArrayGet(pArray, m);
      ASSERT(taosArrayPush(slArray, p));
    }
  }
  int64_t start100mS = taosGetTimestampMs();
  int64_t start100mSCt = taosHashGetCompTimes(hash100m);
  int32_t num = taosArrayGetSize(slArray);
  for (int64_t i = 0; i < num; ++i) {
    ASSERT(taosHashGet(hash100m, (char*)TARRAY_GET_ELEM(slArray, i), 9));
  }
  int64_t end100mS = taosGetTimestampMs();
  int64_t end100mSCt = taosHashGetCompTimes(hash100m);
  for (int64_t i = 0; i < 1000; ++i) {
    taosArrayDestroy(sArray[i]);
  }
  taosArrayDestroy(slArray);

  printf("1h \t %" PRId64 "ms,%" PRId64 "\n", end1h - start1h, end1hCt - start1hCt);
  printf("1s \t %" PRId64 "ms,%" PRId64 "\n", end1s - start1s, end1sCt - start1sCt);
  printf("10s \t %" PRId64 "ms,%" PRId64 "\n", end10s - start10s, end10sCt - start10sCt);
  printf("100s \t %" PRId64 "ms,%" PRId64 "\n", end100s - start100s, end100sCt - start100sCt);
  printf("1m \t %" PRId64 "ms,%" PRId64 "\n", end1m - start1m, end1mCt - start1mCt);
  printf("10m \t %" PRId64 "ms,%" PRId64 "\n", end10m - start10m, end10mCt - start10mCt);
  printf("100m \t %" PRId64 "ms,%" PRId64 "\n", end100m - start100m, end100mCt - start100mCt);
  printf("100mS \t %" PRId64 "ms,%" PRId64 "\n", end100mS - start100mS, end100mSCt - start100mSCt);

  taosHashCleanup(hash1h);
  taosHashCleanup(hash1s);
  taosHashCleanup(hash10s);
  taosHashCleanup(hash100s);
  taosHashCleanup(hash1m);
  taosHashCleanup(hash10m);
  taosHashCleanup(hash100m);

  SHashObj* mhash[1000] = {0};
  for (int64_t i = 0; i < 1000; ++i) {
    mhash[i] = (SHashObj*)taosHashInit(100000, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  }

  for (int64_t i = 0; i < 50000000; ++i) {
#if 0
    taosHashPut(mhash[i%1000], name + i * 9, 9, &i, sizeof(i));
#else
    taosHashPut(mhash[i / 50000], name + i * 9, 9, &i, sizeof(i));
#endif
  }

  int64_t startMhashCt = 0;
  for (int64_t i = 0; i < 1000; ++i) {
    startMhashCt += taosHashGetCompTimes(mhash[i]);
  }

  int64_t startMhash = taosGetTimestampMs();
#if 0
  for (int32_t i = 0; i < 10000000; ++i) {
    ASSERT(taosHashGet(mhash[i%1000], name + i * 9, 9));
  }
#else
  //  for (int64_t i = 0; i < 10000000; ++i) {
  for (int64_t i = 0; i < 50000000; i += 5) {
    ASSERT(taosHashGet(mhash[i / 50000], name + i * 9, 9));
  }
#endif
  int64_t endMhash = taosGetTimestampMs();
  int64_t endMhashCt = 0;
  for (int64_t i = 0; i < 1000; ++i) {
    printf(" %" PRId64, taosHashGetCompTimes(mhash[i]));
    endMhashCt += taosHashGetCompTimes(mhash[i]);
  }
  printf("\n100m \t %" PRId64 "ms,%" PRId64 "\n", endMhash - startMhash, endMhashCt - startMhashCt);

  for (int64_t i = 0; i < 1000; ++i) {
    taosHashCleanup(mhash[i]);
  }
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, hashTest) {
  simpleTest();
  stringKeyTest();
  noLockPerformanceTest();
  multithreadsTest();
  acquireRleaseTest();
  // perfTest();
}
