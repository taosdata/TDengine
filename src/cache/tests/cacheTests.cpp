#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/time.h>

#include "cache.h"

TEST(cacheTest, testInsert) {
  cacheOption options = (cacheOption) {
    .limit = 1024 * 1024,
    .factor = 1.2,
    .hotPercent = 30,
    .warmPercent = 30,
  };

  cache_t* cache = cacheCreate(&options);

  cacheTableOption tableOptions = (cacheTableOption) {
    .loadFp = NULL,
    .initHashPower  = 10,
    .userData = NULL,
    .keyType   = TSDB_DATA_TYPE_BINARY,
  };

  cacheTable* pTable = cacheCreateTable(cache, &tableOptions);

  char *pData;
  int nBytes;
  int i = 0;

  for (i = 0; i < 5000; ++i) {
    char buf[20] = {0};
    snprintf(buf, sizeof(buf), "0123456789_%d", i);
    size_t nkey = strlen(buf);
    int err = cachePut(pTable, buf, nkey, buf, nkey, 3600);

    printf("\nhas push key %s %s\n", buf, err == CACHE_OK ? "success" : "fail");

    int ret = cacheGet(pTable, buf, nkey, &pData, &nBytes);
    ASSERT(ret == CACHE_OK);
    
    ASSERT_EQ(nkey, nBytes);
    ASSERT(memcmp(pData, buf, nBytes) == 0);
    free(pData);

    //cacheRemove(pTable, buf, nkey);
    //ASSERT(cacheGet(pTable, buf, nkey, &pData, &nBytes) == CACHE_KEY_NOT_FOUND);
    /*
    for (int j = 10; j >= 0 && i - j >= 0; j--) {
      snprintf(buf, sizeof(buf), "0123456789_%d", i - j);
      pItem = cacheGet(pTable, buf, strlen(buf));
      printf("cache item %s %s\n", buf, pItem == NULL ? "not found" : "found");
    }
    */
  }
  cacheDestroy(cache);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
