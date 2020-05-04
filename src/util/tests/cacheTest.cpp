#include <iostream>
#include <gtest/gtest.h>
#include <sys/time.h>

#include "taos.h"
//#include "tsdb.h"

//#include "testCommon.h"
#include "tstoken.h"
#include "tutil.h"
#include "tcache.h"
#include "ttimer.h"
#include "ttime.h"

namespace {
int32_t tsMaxMgmtConnections = 10000;
int32_t tsMaxMeterConnections = 200;
}
// test cache
TEST(testCase, client_cache_test) {
  const int32_t REFRESH_TIME_IN_SEC = 2;
  void* tscTmr = taosTmrInit (tsMaxMgmtConnections*2, 200, 6000, "TSC");
  SCacheObj* tscCacheHandle = taosCacheInit(tscTmr, REFRESH_TIME_IN_SEC);

  const char* key1 = "test1";
  char data1[] = "test11";

  char* cachedObj = (char*) taosCachePut(tscCacheHandle, key1, data1, strlen(data1)+1, 1);
  sleep(REFRESH_TIME_IN_SEC+1);

  printf("obj is still valid: %s\n", cachedObj);

  char data2[] = "test22";
  taosCacheRelease(tscCacheHandle, (void**) &cachedObj, false);

  /* the object is cleared by cache clean operation */
  cachedObj = (char*) taosCachePut(tscCacheHandle, key1, data2, strlen(data2)+1, 20);
  printf("after updated: %s\n", cachedObj);

  printf("start to remove data from cache\n");
  taosCacheRelease(tscCacheHandle, (void**) &cachedObj, false);
  printf("end of removing data from cache\n");

  const char* key3 = "test2";
  const char* data3 = "kkkkkkk";

  char* cachedObj2 = (char*) taosCachePut(tscCacheHandle, key3, data3, strlen(data3) + 1, 1);
  printf("%s\n", cachedObj2);

  taosCacheRelease(tscCacheHandle, (void**) &cachedObj2, false);

  sleep(3);
  char* d = (char*) taosCacheAcquireByName(tscCacheHandle, key3);
//    assert(d == NULL);

  char key5[] = "test5";
  char data5[] = "data5kkkkk";
  cachedObj2 = (char*) taosCachePut(tscCacheHandle, key5, data5, strlen(data5) + 1, 20);

  const char* data6= "new Data after updated";
  taosCacheRelease(tscCacheHandle, (void**) &cachedObj2, false);

  cachedObj2 = (char*) taosCachePut(tscCacheHandle, key5, data6, strlen(data6) + 1, 20);
  printf("%s\n", cachedObj2);

  taosCacheRelease(tscCacheHandle, (void**) &cachedObj2, true);

  const char* data7 = "add call update procedure";
  cachedObj2 = (char*) taosCachePut(tscCacheHandle, key5, data7, strlen(data7) + 1, 20);
  printf("%s\n=======================================\n\n", cachedObj2);

  char* cc = (char*) taosCacheAcquireByName(tscCacheHandle, key5);

  taosCacheRelease(tscCacheHandle, (void**) &cachedObj2, true);
  taosCacheRelease(tscCacheHandle, (void**) &cc, false);

  const char* data8 = "ttft";
  const char* key6 = "key6";

  char* ft = (char*) taosCachePut(tscCacheHandle, key6, data8, strlen(data8), 20);
  taosCacheRelease(tscCacheHandle, (void**) &ft, false);

  /**
   * 140ns
   */
  uint64_t startTime = taosGetTimestampUs();
  printf("Cache Performance Test\nstart time:%" PRIu64 "\n", startTime);
  for(int32_t i=0; i<1000; ++i) {
    char* dd = (char*) taosCacheAcquireByName(tscCacheHandle, key6);
    if (dd != NULL) {
//      printf("get the data\n");
    } else {
      printf("data has been released\n");
    }

    taosCacheRelease(tscCacheHandle, (void**) &dd, false);
  }

  uint64_t endTime = taosGetTimestampUs();
  int64_t el = endTime - startTime;

  printf("End of Test, %" PRIu64 "\nTotal Elapsed Time:%" PRIu64 " us.avg:%f us\n", endTime, el, el/1000.0);

  taosCacheCleanup(tscCacheHandle);
}

TEST(testCase, cache_resize_test) {
  const int32_t REFRESH_TIME_IN_SEC = 2;
  void* tscTmr = taosTmrInit (1000*2, 200, 6000, "TSC");

  auto* pCache = taosCacheInit(tscTmr, REFRESH_TIME_IN_SEC);

  char key[256] = {0};
  char data[1024] = "abcdefghijk";
  int32_t len = strlen(data);

  uint64_t startTime = taosGetTimestampUs();
  int32_t num = 10000;

  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "abc_%7d", i);
    taosCachePut(pCache, key, data, len, 3600);
  }
  uint64_t endTime = taosGetTimestampUs();

  printf("add %d object cost:%" PRIu64 " us, avg:%f us\n", num, endTime - startTime, (endTime-startTime)/(double)num);

  startTime = taosGetTimestampUs();
  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "abc_%7d", i);
    void* k = taosCacheAcquireByName(pCache, key);
    assert(k != 0);
  }
  endTime = taosGetTimestampUs();
  printf("retrieve %d object cost:%" PRIu64 " us,avg:%f\n", num, endTime - startTime, (endTime - startTime)/(double)num);

  taosCacheCleanup(pCache);
}