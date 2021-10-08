#include "os.h"
#include <gtest/gtest.h>
#include <limits.h>
#include <taosdef.h>
#include <iostream>

#include "hash.h"
#include "taos.h"

namespace {
// the simple test code for basic operations
void simpleTest() {
  SHashObj* hashTable = (SHashObj*) taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);
  
  // put 400 elements in the hash table
  for(int32_t i = -200; i < 200; ++i) {
    taosHashPut(hashTable, (const char*) &i, sizeof(int32_t), (char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 400);
  
  for(int32_t i = 0; i < 200; ++i) {
    char* p = (char*) taosHashGet(hashTable, (const char*) &i, sizeof(int32_t));
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }
  
  for(int32_t i = 1000; i < 2000; ++i) {
    taosHashRemove(hashTable, (const char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 400);
  
  for(int32_t i = 0; i < 100; ++i) {
    taosHashRemove(hashTable, (const char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 300);
  
  for(int32_t i = 100; i < 150; ++i) {
    taosHashRemove(hashTable, (const char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 250);
  taosHashCleanup(hashTable);
}

void stringKeyTest() {
  auto* hashTable = (SHashObj*) taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);
  
  char key[128] = {0};
  
  // put 200 elements in the hash table
  for(int32_t i = 0; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    taosHashPut(hashTable, key, len, (char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 1000);
  
  for(int32_t i = 0; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    
    char* p = (char*) taosHashGet(hashTable, key, len);
    ASSERT_TRUE(p != nullptr);
    
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }
  
  for(int32_t i = 500; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    
    taosHashRemove(hashTable, key, len);
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 500);
  
  for(int32_t i = 0; i < 499; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
  
    taosHashRemove(hashTable, key, len);
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), 1);
  
  taosHashCleanup(hashTable);
}

void functionTest() {

}

/**
 * evaluate the performance issue, by add 10million elements in to hash table in
 * a single threads situation
 */
void noLockPerformanceTest() {
  auto* hashTable = (SHashObj*) taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  ASSERT_EQ(taosHashGetSize(hashTable), 0);
  
  char key[128] = {0};
  int32_t num = 5000000;
  
  int64_t st = taosGetTimestampUs();
  
  // put 10M elements in the hash table
  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    taosHashPut(hashTable, key, len, (char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosHashGetSize(hashTable), num);
  
  int64_t et = taosGetTimestampUs();
  printf("Elpased time:%" PRId64 " us to add %d elements, avg cost:%lf us\n", et - st, num, (et - st)/(double) num);
  
  st = taosGetTimestampUs();
  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    char* p = (char*) taosHashGet(hashTable, key, len);
    ASSERT_TRUE(p != nullptr);
    
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }
  
  et = taosGetTimestampUs();
  printf("Elpased time:%" PRId64 " us to fetch all %d elements, avg cost:%lf us\n", et - st, num, (et - st)/(double) num);
  
  printf("The maximum length of overflow linklist in hash table is:%d\n", taosHashGetMaxOverflowLinkLength(hashTable));
  taosHashCleanup(hashTable);
}

void multithreadsTest() {
  //todo
}

// check the function robustness
void invalidOperationTest() {

}

}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, hashTest) {
  simpleTest();
  stringKeyTest();
  noLockPerformanceTest();
  multithreadsTest();
}
