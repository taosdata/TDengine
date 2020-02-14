#include <iostream>
#include <gtest/gtest.h>
#include <limits.h>

#include "taos.h"
#include "hash.h"
#include "ttime.h"

namespace {
// the simple test code for basic operations
void simpleTest() {
  auto* hashTable = (HashObj*) taosInitHashTable(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 0);
  
  // put 400 elements in the hash table
  for(int32_t i = -200; i < 200; ++i) {
    taosAddToHashTable(hashTable, (const char*) &i, sizeof(int32_t), (char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 400);
  
  for(int32_t i = 0; i < 200; ++i) {
    char* p = taosGetDataFromHashTable(hashTable, (const char*) &i, sizeof(int32_t));
    ASSERT_TRUE(p != nullptr);
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }
  
  for(int32_t i = 1000; i < 2000; ++i) {
    taosDeleteFromHashTable(hashTable, (const char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 400);
  
  for(int32_t i = 0; i < 100; ++i) {
    taosDeleteFromHashTable(hashTable, (const char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 300);
  
  for(int32_t i = 100; i < 150; ++i) {
    taosDeleteFromHashTable(hashTable, (const char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 250);
  taosCleanUpHashTable(hashTable);
}

void stringKeyTest() {
  auto* hashTable = (HashObj*) taosInitHashTable(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false);
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 0);
  
  char key[128] = {0};
  
  // put 200 elements in the hash table
  for(int32_t i = 0; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    taosAddToHashTable(hashTable, key, len, (char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 1000);
  
  for(int32_t i = 0; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    
    char* p = taosGetDataFromHashTable(hashTable, key, len);
    ASSERT_TRUE(p != nullptr);
    
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }
  
  for(int32_t i = 500; i < 1000; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    
    taosDeleteFromHashTable(hashTable, key, len);
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 500);
  
  for(int32_t i = 0; i < 499; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
  
    taosDeleteFromHashTable(hashTable, key, len);
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 1);
  
  taosCleanUpHashTable(hashTable);
}

void functionTest() {

}

/**
 * evaluate the performance issue, by add 10million elements in to hash table in
 * a single threads situation
 */
void noLockPerformanceTest() {
  auto* hashTable = (HashObj*) taosInitHashTable(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false);
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), 0);
  
  char key[128] = {0};
  int32_t num = 5000000;
  
  int64_t st = taosGetTimestampUs();
  
  // put 10M elements in the hash table
  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    taosAddToHashTable(hashTable, key, len, (char*) &i, sizeof(int32_t));
  }
  
  ASSERT_EQ(taosNumElemsInHashTable(hashTable), num);
  
  int64_t et = taosGetTimestampUs();
  printf("Elpased time:%" PRId64 " us to add %d elements, avg cost:%lf us\n", et - st, num, (et - st)/(double) num);
  
  st = taosGetTimestampUs();
  for(int32_t i = 0; i < num; ++i) {
    int32_t len = sprintf(key, "%d_1_%dabcefg_", i, i + 10);
    char* p = taosGetDataFromHashTable(hashTable, key, len);
    ASSERT_TRUE(p != nullptr);
    
    ASSERT_EQ(*reinterpret_cast<int32_t*>(p), i);
  }
  
  et = taosGetTimestampUs();
  printf("Elpased time:%" PRId64 " us to fetch all %d elements, avg cost:%lf us\n", et - st, num, (et - st)/(double) num);
  
  printf("The maximum length of overflow linklist in hash table is:%d\n", taosGetHashMaxOverflowLength(hashTable));
  taosCleanUpHashTable(hashTable);
}

void multithreadsTest() {
  //todo
}

// check the function robustness
void invalidOperationTest() {

}

}
TEST(testCase, hashTest) {
  simpleTest();
  stringKeyTest();
  noLockPerformanceTest();
  multithreadsTest();
}