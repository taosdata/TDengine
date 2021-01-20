#include <iostream>
#include <gtest/gtest.h>
#include <sys/time.h>

#include "os.h"
#include "taos.h"
#include "taosdef.h"
#include "testCommon.h"
#include "tstoken.h"
#include "tutil.h"
#include "tcache.h"
#include "ttimer.h"

// test cache
TEST(testCase, client_cache_test) {
  const int32_t REFRESH_TIME_IN_SEC = 2;
  void* tscTmr = taosTmrInit (tsMaxMgmtConnections*2, 200, 6000, "TSC");
  void* tscMetaCache = taosInitDataCache(tsMaxMeterConnections, tscTmr, REFRESH_TIME_IN_SEC);

  char* key1 = "test1";
  char* data1 = "test11";

  char* cachedObj = (char*) taosAddDataIntoCache(tscMetaCache, key1, data1, strlen(data1), 1);
  sleep(REFRESH_TIME_IN_SEC+1);

  printf("obj is still valid: %s\n", cachedObj);

  char* data2 = "test22";
  taosRemoveDataFromCache(tscMetaCache, (void**) &cachedObj, false);

  /* the object is cleared by cache clean operation */
  cachedObj = (char*) taosUpdateDataFromCache(tscMetaCache, key1, data2, strlen(data2), 20);
  printf("after updated: %s\n", cachedObj);

  printf("start to remove data from cache\n");
  taosRemoveDataFromCache(tscMetaCache, (void**) &cachedObj, false);
  printf("end of removing data from cache\n");

  getchar();

  char* key3 = "test2";
  char* data3 = "kkkkkkk";

  char* cachedObj2 = (char*) taosAddDataIntoCache(tscMetaCache, key3, data3, strlen(data3), 1);
  printf("%s\n", cachedObj2);

  taosRemoveDataFromCache(tscMetaCache, (void**) &cachedObj2, false);

  sleep(3);
  char* d = (char*) taosGetDataFromCache(tscMetaCache, key3);
//    assert(d == NULL);

  char* key5 = "test5";
  char* data5 = "data5kkkkk";
  cachedObj2 = (char*) taosAddDataIntoCache(tscMetaCache, key5, data5, strlen(data5), 20);

  char* data6= "new Data after updated";
  taosRemoveDataFromCache(tscMetaCache, (void**) &cachedObj2, false);

  cachedObj2 = (char*) taosUpdateDataFromCache(tscMetaCache, key5, data6, strlen(data6), 20);
  printf("%s\n", cachedObj2);

  taosRemoveDataFromCache(tscMetaCache, (void**) &cachedObj2, true);

  char* data7 = "add call update procedure";
  cachedObj2 = (char*) taosAddDataIntoCache(tscMetaCache, key5, data7, strlen(data7), 20);
  printf("%s\n=======================================\n\n", cachedObj2);

  char* cc = (char*) taosGetDataFromCache(tscMetaCache, key5);

  taosRemoveDataFromCache(tscMetaCache, (void**) &cachedObj2, true);
  taosRemoveDataFromCache(tscMetaCache, (void**) &cc, false);

  char* data8 = "ttft";
  char* key6 = "key6";

  char* ft = (char*) taosAddDataIntoCache(tscMetaCache, key6, data8, strlen(data8), 20);
  taosRemoveDataFromCache(tscMetaCache, (void**) &ft, false);

  /**
   * 140ns
   */
  uint64_t startTime = taosGetTimestampUs();
  printf("Cache Performance Test\nstart time:%lld\n", startTime);
  for(int32_t i=0; i<1000; ++i) {
    char* dd = (char*) taosGetDataFromCache(tscMetaCache, key6);
    if (dd != NULL) {
//      printf("get the data\n");
    } else {
      printf("data has been released\n");
    }

    taosRemoveDataFromCache(tscMetaCache, (void**) &dd, false);
  }

  uint64_t endTime = taosGetTimestampUs();
  int64_t el = endTime - startTime;

  printf("End of Test, %lld\nTotal Elapsed Time:%lld us.avg:%f us\n", endTime, el, el/1000.0);

  taosCleanUpDataCache(tscMetaCache);
}

TEST(testCase, cache_resize_test) {
  const int32_t REFRESH_TIME_IN_SEC = 2;
  void* tscTmr = taosTmrInit (1000*2, 200, 6000, "TSC");

  void* pCache = taosInitDataCache(4096, tscTmr, REFRESH_TIME_IN_SEC);

  char key[256] = {0};
  char data[1024] = "abcdefghijk";
  int32_t len = strlen(data);

  uint64_t startTime = taosGetTimestampUs();
  int32_t num = 10000;

  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "abc_%7d", i);
    taosAddDataIntoCache(pCache, key, data, len, 3600);
  }
  uint64_t endTime = taosGetTimestampUs();

  printf("add 10,000,000 object cost:%lld us, avg:%f us\n", endTime - startTime, (endTime-startTime)/(double)num);

  startTime = taosGetTimestampUs();
  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "abc_%7d", i);
    void* k = taosGetDataFromCache(pCache, key);
    assert(k != 0);
  }
  endTime = taosGetTimestampUs();
  printf("retrieve 10,000,000 object cost:%lld us,avg:%f\n", endTime - startTime, (endTime - startTime)/(double)num);

  taosCleanUpDataCache(pCache);
  taosMsleep(20000);
  getchar();
}