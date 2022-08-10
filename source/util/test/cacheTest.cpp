#include <gtest/gtest.h>
#include <iostream>

#include "os.h"
#include "taos.h"
#include "tcache.h"

// test cache
TEST(cacheTest, client_cache_test) {
  const int32_t REFRESH_TIME_IN_SEC = 2;
  SCacheObj*    tscMetaCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, REFRESH_TIME_IN_SEC * 1000, 0, NULL, "test");

  const char* key1 = "test1";
  char        data1[] = "test11";

  char* cachedObj = (char*)taosCachePut(tscMetaCache, key1, strlen(key1), data1, strlen(data1) + 1, 1);
  taosSsleep(REFRESH_TIME_IN_SEC + 1);

  printf("obj is still valid: %s\n", cachedObj);

  char data2[] = "test22";
  taosCacheRelease(tscMetaCache, (void**)&cachedObj, false);

  /* the object is cleared by cache clean operation */
  cachedObj = (char*)taosCachePut(tscMetaCache, key1, strlen(key1), data2, strlen(data2) + 1, 20);
  printf("after updated: %s\n", cachedObj);

  printf("start to remove data from cache\n");
  taosCacheRelease(tscMetaCache, (void**)&cachedObj, false);
  printf("end of removing data from cache\n");

  const char* key3 = "test2";
  const char* data3 = "kkkkkkk";

  char* cachedObj2 = (char*)taosCachePut(tscMetaCache, key3, strlen(key3), data3, strlen(data3) + 1, 1);
  printf("%s\n", cachedObj2);

  taosCacheRelease(tscMetaCache, (void**)&cachedObj2, false);

  taosSsleep(3);
  char* d = (char*)taosCacheAcquireByKey(tscMetaCache, key3, strlen(key3));
  assert(d == NULL);

  char key5[] = "test5";
  char data5[] = "data5kkkkk";
  cachedObj2 = (char*)taosCachePut(tscMetaCache, key5, strlen(key5), data5, strlen(data5) + 1, 20);

  const char* data6 = "new Data after updated";
  taosCacheRelease(tscMetaCache, (void**)&cachedObj2, false);

  cachedObj2 = (char*)taosCachePut(tscMetaCache, key5, strlen(key5), data6, strlen(data6) + 1, 20);
  printf("%s\n", cachedObj2);

  taosCacheRelease(tscMetaCache, (void**)&cachedObj2, true);

  const char* data7 = "add call update procedure";
  cachedObj2 = (char*)taosCachePut(tscMetaCache, key5, strlen(key5), data7, strlen(data7) + 1, 20);
  printf("%s\n=======================================\n\n", cachedObj2);

  char* cc = (char*)taosCacheAcquireByKey(tscMetaCache, key5, strlen(key5));

  taosCacheRelease(tscMetaCache, (void**)&cachedObj2, true);
  taosCacheRelease(tscMetaCache, (void**)&cc, false);

  const char* data8 = "ttft";
  const char* key6 = "key6";

  char* ft = (char*)taosCachePut(tscMetaCache, key6, strlen(key6), data8, strlen(data8), 20);
  taosCacheRelease(tscMetaCache, (void**)&ft, false);

  /**
   * 140ns
   */
  uint64_t startTime = taosGetTimestampUs();
  printf("Cache Performance Test\nstart time:%" PRIu64 "\n", startTime);
  for (int32_t i = 0; i < 1000; ++i) {
    char* dd = (char*)taosCacheAcquireByKey(tscMetaCache, key6, strlen(key6));
    if (dd != NULL) {
      //      printf("get the data\n");
    } else {
      printf("data has been released\n");
    }

    taosCacheRelease(tscMetaCache, (void**)&dd, false);
  }

  uint64_t endTime = taosGetTimestampUs();
  int64_t  el = endTime - startTime;

  printf("End of Test, %" PRIu64 "\nTotal Elapsed Time:%" PRIu64 " us.avg:%f us\n", endTime, el, el / 1000.0);

  taosCacheCleanup(tscMetaCache);
}

TEST(cacheTest, cache_iter_test) {
  const int32_t REFRESH_TIME_IN_SEC = 2;
  auto*         pCache = taosCacheInit(TSDB_DATA_TYPE_BINARY, REFRESH_TIME_IN_SEC * 1000, false, NULL, "test");

  char key[256] = {0};
  char data[1024] = "abcdefghijk";
  //  int32_t len = strlen(data);

  uint64_t startTime = taosGetTimestampUs();
  int32_t  num = 10000;

  for (int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "abc_%7d", i);
    taosCachePut(pCache, key, strlen(key), data, len, 3600);
  }

  uint64_t endTime = taosGetTimestampUs();

  printf("add %d object cost:%" PRIu64 " us, avg:%f us\n", num, endTime - startTime,
         (endTime - startTime) / (double)num);

  startTime = taosGetTimestampUs();
  for (int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "abc_%7d", i);
    void*   k = taosCacheAcquireByKey(pCache, key, len);
    assert(k != 0);
  }
  endTime = taosGetTimestampUs();
  printf("retrieve %d object cost:%" PRIu64 " us,avg:%f\n", num, endTime - startTime,
         (endTime - startTime) / (double)num);

  int32_t     count = 0;
  SCacheIter* pIter = taosCacheCreateIter(pCache);
  while (taosCacheIterNext(pIter)) {
    size_t keyLen = 0;
    size_t dataLen = 0;

    char* key1 = static_cast<char*>(taosCacheIterGetKey(pIter, &keyLen));
    char* data1 = static_cast<char*>(taosCacheIterGetData(pIter, &dataLen));

    //    char d[256] = {0};
    //    memcpy(d, data1, dataLen);
    //    char k[256] = {0};
    //    memcpy(k, key1, keyLen);
  }

  ASSERT_EQ(count, num);

  taosCacheCleanup(pCache);
}
