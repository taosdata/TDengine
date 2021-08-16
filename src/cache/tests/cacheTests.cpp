#include <gtest/gtest.h>
#include <stdlib.h>
#include <sys/time.h>

#include "cache.h"

typedef struct testData {
  char value[20];
  uint16_t refCnt;
} testData;

TEST(cacheTest, testInsert) {
  cacheOption options = (cacheOption) {
    .limit = 1024 * 1024,
    .factor = 1.2,
    .hotPercent = 30,
    .warmPercent = 30,
  };

  cache_t* cache = cacheCreate(&options);

  testData data, *value;
  cacheTableOption tableOptions = (cacheTableOption) {
    .loadFp = NULL,
    .initHashPower  = 10,
    .refOffset = (int32_t)((char *)&(data.refCnt) - (char *)&data),
    .userData = NULL,
    .keyType   = TSDB_DATA_TYPE_BINARY,    
    //tsChildTableUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type);
  };

  cacheTable* pTable = cacheCreateTable(cache, &tableOptions);

  int nBytes;
  int i = 0;

  for (i = 0; i < 15000; ++i) {
    char buf[20] = {0};
    
    snprintf(buf, sizeof(buf), "0123456789_%d", i);
    size_t nkey = strlen(buf);
    
    memcpy(data.value, buf, nkey);
    data.refCnt = 0;

    int err = cachePut(pTable, buf, nkey, (char*)&data, sizeof(testData), false, 3600);

    printf("\nhas push key %s %s\n", buf, err == CACHE_OK ? "success" : "fail");

    value = (testData*)cacheGet(pTable, buf, nkey, &nBytes);
    ASSERT(value != NULL);
    
    ASSERT_EQ(sizeof(testData), nBytes);
    ASSERT(memcmp(&data.value, value->value, sizeof(char) * 20) == 0);

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
