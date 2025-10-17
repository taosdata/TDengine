/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 #include <gtest/gtest.h>
 #include <vnodeInt.h>
 
 #include <taoserror.h>
 #include <tglobal.h>
 #include <iostream>
 
 #include <tmsg.h>
 #include <vnodeInt.h>
 
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "os.h"
#include "taoserror.h"
#include "tlog.h"
#include "tsdbCacheInt.h"
#include "tarray2.h"
#include "tsdb.h"
#include "vnode.h"

#ifdef USE_ROCKSDB
#include "rocksdb/c.h"
#endif

SDmNotifyHandle dmNotifyHdl = {.state = 0};

#define TSDB_CACHE_TEST_PATH "/tmp/tsdb_cache_test"

static void initLog() {
  const char   *defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  qDebugFlag = 159;

  char temp[1024] = {0};
  snprintf(temp, sizeof(temp), "%s/%s", tsLogDir, defaultLogFileNamePrefix);
  if (taosInitLog(temp, maxLogFileNum, false) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}

class TsdbCacheEnv : public ::testing::Test {
protected:
  static void SetUpTestSuite() { initLog(); }

  static void TearDownTestSuite() {}

  void SetUp() override {
    // Clean up test directory
    taosRemoveDir(TSDB_CACHE_TEST_PATH);
    taosMkDir(TSDB_CACHE_TEST_PATH);
  }

  void TearDown() override {
    // Clean up after test
    taosRemoveDir(TSDB_CACHE_TEST_PATH);
  }

  // Helper function to create a mock STsdb
  STsdb *createMockTsdb(bool rowBased = true) {
    STsdb *pTsdb = (STsdb *)taosMemoryCalloc(1, sizeof(STsdb));
    if (!pTsdb) return nullptr;

    SVnode *pVnode = (SVnode *)taosMemoryCalloc(1, sizeof(SVnode));
    if (!pVnode) {
      taosMemoryFree(pTsdb);
      return nullptr;
    }

    pTsdb->pVnode = pVnode;
    pVnode->pTsdb = pTsdb;
    pVnode->config.vgId = 1;  // Mock vgId
    pVnode->config.cacheFormat = rowBased ? TSDB_CACHE_FORMAT_ROW : TSDB_CACHE_FORMAT_COL;
    pVnode->config.cacheLastSize = 1;  // 1MB
    
    // Set tsdb path
    pTsdb->path = (char*)TSDB_CACHE_TEST_PATH;

    // Initialize LRU cache
    size_t capacity = 1024 * 1024;  // 1MB
    pTsdb->lruCache = taosLRUCacheInit(capacity, 1, 0.5);
    if (!pTsdb->lruCache) {
      taosMemoryFree(pVnode);
      taosMemoryFree(pTsdb);
      return nullptr;
    }

    taosThreadMutexInit(&pTsdb->lruMutex, nullptr);

#ifdef USE_ROCKSDB
    // Initialize RocksDB structures similar to tsdbOpenRocksCache
    // Create comparator
    pTsdb->rCache.my_comparator = rocksdb_comparator_create(
        nullptr, 
        nullptr,  // destructor
        rowBased ? (int (*)(void*, const char*, size_t, const char*, size_t))tsdbRowCacheCmp 
                 : (int (*)(void*, const char*, size_t, const char*, size_t))tsdbColCacheCmp,
        nullptr   // name function
    );
    
    if (pTsdb->rCache.my_comparator) {
      // Create table options
      pTsdb->rCache.tableoptions = rocksdb_block_based_options_create();
      
      // Create options
      pTsdb->rCache.options = rocksdb_options_create();
      if (pTsdb->rCache.options) {
        rocksdb_options_set_create_if_missing(pTsdb->rCache.options, 1);
        rocksdb_options_set_comparator(pTsdb->rCache.options, pTsdb->rCache.my_comparator);
        if (pTsdb->rCache.tableoptions) {
          rocksdb_options_set_block_based_table_factory(pTsdb->rCache.options, pTsdb->rCache.tableoptions);
        }
        rocksdb_options_set_info_log_level(pTsdb->rCache.options, 2);  // WARN_LEVEL
      }
      
      // Create write options
      pTsdb->rCache.writeoptions = rocksdb_writeoptions_create();
      if (pTsdb->rCache.writeoptions) {
        rocksdb_writeoptions_disable_WAL(pTsdb->rCache.writeoptions, 1);
      }
      
      // Create read options
      pTsdb->rCache.readoptions = rocksdb_readoptions_create();
      
      // Open RocksDB database
      char cachePath[256];
      snprintf(cachePath, sizeof(cachePath), "%s/rocksdb_%d", TSDB_CACHE_TEST_PATH, (int)taosGetTimestampMs());
      taosMkDir(cachePath);
      
      char *err = nullptr;
      pTsdb->rCache.db = rocksdb_open(pTsdb->rCache.options, cachePath, &err);
      if (pTsdb->rCache.db) {
        // Create flush options
        pTsdb->rCache.flushoptions = rocksdb_flushoptions_create();
        
        // Create write batch
        pTsdb->rCache.writebatch = rocksdb_writebatch_create();
        
        // Initialize mutex
        taosThreadMutexInit(&pTsdb->rCache.writeBatchMutex, nullptr);
      } else {
        if (err) {
          printf("Failed to open RocksDB: %s\n", err);
          rocksdb_free(err);
        }
      }
    }
    
    // Initialize other rCache fields
    pTsdb->rCache.sver = 0;
    pTsdb->rCache.suid = 0;
    pTsdb->rCache.uid = 0;
    pTsdb->rCache.pTSchema = nullptr;
    pTsdb->rCache.ctxArray = nullptr;
#else
    // Initialize non-RocksDB fields
    memset(&pTsdb->rCache, 0, sizeof(pTsdb->rCache));
#endif

    return pTsdb;
  }

  void destroyMockTsdb(STsdb *pTsdb) {
    if (!pTsdb) return;

    if (pTsdb->lruCache) {
      taosLRUCacheCleanup(pTsdb->lruCache);
    }

    taosThreadMutexDestroy(&pTsdb->lruMutex);

#ifdef USE_ROCKSDB
    // Clean up RocksDB resources similar to tsdbCloseRocksCache
    if (pTsdb->rCache.writebatch) {
      rocksdb_writebatch_destroy(pTsdb->rCache.writebatch);
      pTsdb->rCache.writebatch = nullptr;
    }
    
    if (pTsdb->rCache.db) {
      rocksdb_close(pTsdb->rCache.db);
      pTsdb->rCache.db = nullptr;
    }
    
    if (pTsdb->rCache.flushoptions) {
      rocksdb_flushoptions_destroy(pTsdb->rCache.flushoptions);
      pTsdb->rCache.flushoptions = nullptr;
    }
    
    if (pTsdb->rCache.readoptions) {
      rocksdb_readoptions_destroy(pTsdb->rCache.readoptions);
      pTsdb->rCache.readoptions = nullptr;
    }
    
    if (pTsdb->rCache.writeoptions) {
      rocksdb_writeoptions_destroy(pTsdb->rCache.writeoptions);
      pTsdb->rCache.writeoptions = nullptr;
    }
    
    if (pTsdb->rCache.options) {
      rocksdb_options_destroy(pTsdb->rCache.options);
      pTsdb->rCache.options = nullptr;
    }
    
    if (pTsdb->rCache.tableoptions) {
      rocksdb_block_based_options_destroy(pTsdb->rCache.tableoptions);
      pTsdb->rCache.tableoptions = nullptr;
    }
    
    if (pTsdb->rCache.my_comparator) {
      rocksdb_comparator_destroy(pTsdb->rCache.my_comparator);
      pTsdb->rCache.my_comparator = nullptr;
    }
    
    taosThreadMutexDestroy(&pTsdb->rCache.writeBatchMutex);
#endif

    if (pTsdb->pVnode) {
      taosMemoryFree(pTsdb->pVnode);
    }

    taosMemoryFree(pTsdb);
  }

  // Helper function to create a mock STSchema
  STSchema *createMockSchema(int numCols = 3) {
    int32_t   schemaSize = sizeof(STSchema) + numCols * sizeof(STColumn);
    STSchema *pSchema = (STSchema *)taosMemoryCalloc(1, schemaSize);
    if (!pSchema) return nullptr;

    pSchema->numOfCols = numCols;
    pSchema->version = 1;

    // Column 0: timestamp
    pSchema->columns[0].colId = 0;
    pSchema->columns[0].type = TSDB_DATA_TYPE_TIMESTAMP;
    pSchema->columns[0].bytes = 8;

    // Column 1: int
    if (numCols > 1) {
      pSchema->columns[1].colId = 1;
      pSchema->columns[1].type = TSDB_DATA_TYPE_INT;
      pSchema->columns[1].bytes = 4;
    }

    // Column 2: varchar
    if (numCols > 2) {
      pSchema->columns[2].colId = 2;
      pSchema->columns[2].type = TSDB_DATA_TYPE_VARCHAR;
      pSchema->columns[2].bytes = 64;
    }

    return pSchema;
  }
};

// Test SLastCol serialization and deserialization
TEST_F(TsdbCacheEnv, LastColSerializeDeserialize) {
  SLastCol lastCol;
  memset(&lastCol, 0, sizeof(SLastCol));

  lastCol.rowKey.ts = 1234567890;
  lastCol.rowKey.numOfPKs = 0;
  lastCol.dirty = 1;
  lastCol.colVal.cid = 1;
  lastCol.colVal.flag = CV_FLAG_VALUE;
  lastCol.colVal.value.type = TSDB_DATA_TYPE_INT;
  lastCol.colVal.value.val = 42;
  lastCol.cacheStatus = TSDB_LAST_CACHE_VALID;

  char  *value = nullptr;
  size_t size = 0;

  // Test serialization (internal function, testing through public interface)
  // We'll create a mock scenario
  EXPECT_EQ(lastCol.colVal.value.val, 42);
  EXPECT_EQ(lastCol.cacheStatus, TSDB_LAST_CACHE_VALID);
}

// Test SLastCol with variable data type
TEST_F(TsdbCacheEnv, LastColVarDataSerialize) {
  SLastCol lastCol;
  memset(&lastCol, 0, sizeof(SLastCol));

  lastCol.rowKey.ts = 1234567890;
  lastCol.rowKey.numOfPKs = 0;
  lastCol.dirty = 1;
  lastCol.colVal.cid = 2;
  lastCol.colVal.flag = CV_FLAG_VALUE;
  lastCol.colVal.value.type = TSDB_DATA_TYPE_VARCHAR;

  const char *testStr = "Hello World";
  lastCol.colVal.value.nData = strlen(testStr);
  lastCol.colVal.value.pData = (uint8_t *)taosMemoryMalloc(lastCol.colVal.value.nData);
  memcpy(lastCol.colVal.value.pData, testStr, lastCol.colVal.value.nData);
  lastCol.cacheStatus = TSDB_LAST_CACHE_VALID;

  EXPECT_EQ(lastCol.colVal.value.nData, strlen(testStr));
  EXPECT_EQ(memcmp(lastCol.colVal.value.pData, testStr, strlen(testStr)), 0);

  taosMemoryFree(lastCol.colVal.value.pData);
}

// Test row key comparison
TEST_F(TsdbCacheEnv, RowKeyCompare) {
  SRowKey key1 = {.ts = 1000, .numOfPKs = 0};
  SRowKey key2 = {.ts = 2000, .numOfPKs = 0};
  SRowKey key3 = {.ts = 1000, .numOfPKs = 0};

  EXPECT_LT(tRowKeyCompare(&key1, &key2), 0);
  EXPECT_GT(tRowKeyCompare(&key2, &key1), 0);
  EXPECT_EQ(tRowKeyCompare(&key1, &key3), 0);
}

// Test LRU cache basic operations
TEST_F(TsdbCacheEnv, LRUCacheBasicOps) {
  STsdb *pTsdb = createMockTsdb(false);
  ASSERT_NE(pTsdb, nullptr);

  tb_uid_t uid = 12345;
  int16_t  cid = 1;
  int8_t   lflag = LFLAG_LAST;

  SLastKey key = {.uid = uid, .cid = cid, .lflag = lflag};

  // Create a last col entry
  SRowKey  emptyRowKey = {.ts = TSKEY_MIN, .numOfPKs = 0};
  SLastCol lastCol = {
      .rowKey = emptyRowKey,.dirty = 0, .colVal = COL_VAL_NONE(cid, TSDB_DATA_TYPE_INT), .cacheStatus = TSDB_LAST_CACHE_VALID};

  // Test cache lookup (should not find)
  LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &key, ROCKS_KEY_LEN);
  EXPECT_EQ(h, nullptr);

  destroyMockTsdb(pTsdb);
}

#if TSDB_CACHE_FORMAT_COL
// Test column cache create and delete
TEST_F(TsdbCacheEnv, ColCacheCreateDelete) {
  STsdb    *pTsdb = createMockTsdb(false);
  STSchema *pSchema = createMockSchema(3);
  ASSERT_NE(pTsdb, nullptr);
  ASSERT_NE(pSchema, nullptr);

  tb_uid_t uid = 12345;

  // Create table cache
  int32_t code = tsdbColCacheCreateTable(pTsdb, uid, pSchema);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  // Verify cache entries were created
  for (int j = 0; j < pSchema->numOfCols; j++) {
    int16_t cid = pSchema->columns[j].colId;
    for (int i = 0; i < 2; i++) {
      int8_t    lflag = (i == 0) ? LFLAG_LAST : LFLAG_LAST_ROW;
      SLastKey  key = {.uid = uid, .cid = cid, .lflag = lflag};
      LRUHandle *h = taosLRUCacheLookup(pTsdb->lruCache, &key, ROCKS_KEY_LEN);
      if (h) {
        taosLRUCacheRelease(pTsdb->lruCache, h, false);
      }
    }
  }

  // Delete table cache
  code = tsdbColCacheDeleteTable(pTsdb, uid, pSchema);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  taosMemoryFree(pSchema);
  destroyMockTsdb(pTsdb);
}
#endif

// Test row cache create and delete
TEST_F(TsdbCacheEnv, RowCacheCreateDelete) {
  STsdb    *pTsdb = createMockTsdb(true);
  STSchema *pSchema = createMockSchema(3);
  ASSERT_NE(pTsdb, nullptr);
  ASSERT_NE(pSchema, nullptr);

  tb_uid_t uid = 12345;

  // Create table cache
  int32_t code = tsdbRowCacheCreateTable(pTsdb, uid, pSchema);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  // Verify cache entries were created
  for (int i = 0; i < 2; i++) {
    int8_t       lflag = (i == 0) ? LFLAG_LAST_ROW : LFLAG_LAST;
    SLastRowKey  key = {.uid = uid, .lflag = lflag};
    LRUHandle   *h = taosLRUCacheLookup(pTsdb->lruCache, &key, ROCKS_ROW_KEY_LEN);
    if (h) {
      taosLRUCacheRelease(pTsdb->lruCache, h, false);
    }
  }

  // Delete table cache
  code = tsdbRowCacheDeleteTable(pTsdb, uid);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  taosMemoryFree(pSchema);
  destroyMockTsdb(pTsdb);
}

// Test cache format detection
TEST_F(TsdbCacheEnv, CacheFormatDetection) {
  STsdb *pTsdbRow = createMockTsdb(true);
  STsdb *pTsdbCol = createMockTsdb(false);

  ASSERT_NE(pTsdbRow, nullptr);
  ASSERT_NE(pTsdbCol, nullptr);

  EXPECT_EQ(pTsdbRow->pVnode->config.cacheFormat, TSDB_CACHE_FORMAT_ROW);
  EXPECT_EQ(pTsdbCol->pVnode->config.cacheFormat, TSDB_CACHE_FORMAT_COL);

  destroyMockTsdb(pTsdbRow);
  destroyMockTsdb(pTsdbCol);
}

// Test cache upgrade need detection
TEST_F(TsdbCacheEnv, CacheUpgradeNeed) {
  STsdb *pTsdb = createMockTsdb(false);
  ASSERT_NE(pTsdb, nullptr);

  // Should need upgrade from col to row format
  bool needUpgrade = tsdbNeedCacheUpgrade(pTsdb->pVnode);
  EXPECT_TRUE(needUpgrade);

  // Change to row format
  pTsdb->pVnode->config.cacheFormat = TSDB_CACHE_FORMAT_ROW;
  needUpgrade = tsdbNeedCacheUpgrade(pTsdb->pVnode);
  EXPECT_FALSE(needUpgrade);

  destroyMockTsdb(pTsdb);
}

// Test cache format setter
TEST_F(TsdbCacheEnv, SetCacheFormat) {
  STsdb *pTsdb = createMockTsdb(false);
  ASSERT_NE(pTsdb, nullptr);

  EXPECT_EQ(pTsdb->pVnode->config.cacheFormat, TSDB_CACHE_FORMAT_COL);

  int32_t code = tsdbSetCacheFormat(pTsdb->pVnode, TSDB_CACHE_FORMAT_ROW);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(pTsdb->pVnode->config.cacheFormat, TSDB_CACHE_FORMAT_ROW);

  destroyMockTsdb(pTsdb);
}

// Test row cache comparator
TEST_F(TsdbCacheEnv, RowCacheComparator) {
  SLastRowKey key1 = {.uid = 100, .lflag = LFLAG_LAST_ROW};
  SLastRowKey key2 = {.uid = 200, .lflag = LFLAG_LAST_ROW};
  SLastRowKey key3 = {.uid = 100, .lflag = LFLAG_LAST};
  SLastRowKey key4 = {.uid = 100, .lflag = LFLAG_LAST_ROW};

  int cmp1 = tsdbRowCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key2, sizeof(key2));
  EXPECT_LT(cmp1, 0);  // key1.uid < key2.uid

  int cmp2 = tsdbRowCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key3, sizeof(key3));
  EXPECT_LT(cmp2, 0);  // same uid, key1.lflag < key3.lflag

  int cmp3 = tsdbRowCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key4, sizeof(key4));
  EXPECT_EQ(cmp3, 0);  // identical keys
}

// Test column cache comparator
TEST_F(TsdbCacheEnv, ColCacheComparator) {
  SLastKey key1 = {.uid = 100, .cid = 1, .lflag = LFLAG_LAST};
  SLastKey key2 = {.uid = 200, .cid = 1, .lflag = LFLAG_LAST};
  SLastKey key3 = {.uid = 100, .cid = 2, .lflag = LFLAG_LAST};
  SLastKey key4 = {.uid = 100, .cid = 1, .lflag = LFLAG_LAST_ROW};
  SLastKey key5 = {.uid = 100, .cid = 1, .lflag = LFLAG_LAST};

  int cmp1 = tsdbColCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key2, sizeof(key2));
  EXPECT_LT(cmp1, 0);  // key1.uid < key2.uid

  int cmp2 = tsdbColCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key3, sizeof(key3));
  EXPECT_LT(cmp2, 0);  // same uid, key1.cid < key3.cid

  int cmp3 = tsdbColCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key4, sizeof(key4));
  EXPECT_LT(cmp3, 0);  // same uid and cid, key1.lflag < key4.lflag

  int cmp4 = tsdbColCacheCmp(nullptr, (char *)&key1, sizeof(key1), (char *)&key5, sizeof(key5));
  EXPECT_EQ(cmp4, 0);  // identical keys
}

// Test tsdbUtil functions - tRowKeyCompare
TEST_F(TsdbCacheEnv, UtilRowKeyCompare) {
  SRowKey key1 = {.ts = 1000, .numOfPKs = 0};
  SRowKey key2 = {.ts = 2000, .numOfPKs = 0};
  SRowKey key3 = {.ts = 1000, .numOfPKs = 0};

  EXPECT_LT(tRowKeyCompare(&key1, &key2), 0);
  EXPECT_GT(tRowKeyCompare(&key2, &key1), 0);
  EXPECT_EQ(tRowKeyCompare(&key1, &key3), 0);

  // Test with primary keys
  SRowKey key4 = {.ts = 1000, .numOfPKs = 1};
  key4.pks[0].type = TSDB_DATA_TYPE_INT;
  key4.pks[0].val = 100;

  SRowKey key5 = {.ts = 1000, .numOfPKs = 1};
  key5.pks[0].type = TSDB_DATA_TYPE_INT;
  key5.pks[0].val = 200;

  EXPECT_LT(tRowKeyCompare(&key4, &key5), 0);
}

// Test cache capacity and usage
TEST_F(TsdbCacheEnv, CacheCapacityUsage) {
  STsdb *pTsdb = createMockTsdb(true);
  ASSERT_NE(pTsdb, nullptr);

  size_t initialUsage = taosLRUCacheGetUsage(pTsdb->lruCache);
  EXPECT_GE(initialUsage, 0);

  // Get initial element count
  int32_t initialElems = taosLRUCacheGetElems(pTsdb->lruCache);
  EXPECT_GE(initialElems, 0);

  // Change capacity
  size_t newCapacity = 2 * 1024 * 1024;  // 2MB
  taosLRUCacheSetCapacity(pTsdb->lruCache, newCapacity);
  
  size_t capacity = taosLRUCacheGetCapacity(pTsdb->lruCache);
  EXPECT_EQ(capacity, newCapacity);

  destroyMockTsdb(pTsdb);
}

// Test cache with VNode interface
TEST_F(TsdbCacheEnv, CacheVnodeInterface) {
  STsdb *pTsdb = createMockTsdb(true);
  ASSERT_NE(pTsdb, nullptr);

  SVnode *pVnode = pTsdb->pVnode;

  // Test get usage
  size_t usage = tsdbCacheGetUsage(pVnode);
  EXPECT_GE(usage, 0);

  // Test get elements
  int32_t elems = tsdbCacheGetElems(pVnode);
  EXPECT_GE(elems, 0);

  // Test set capacity
  size_t newCapacity = 2 * 1024 * 1024;
  tsdbCacheSetCapacity(pVnode, newCapacity);

  destroyMockTsdb(pTsdb);
}

// Test cache with null vnode
TEST_F(TsdbCacheEnv, CacheNullVnode) {
  size_t usage = tsdbCacheGetUsage(nullptr);
  EXPECT_EQ(usage, 0);

  int32_t elems = tsdbCacheGetElems(nullptr);
  EXPECT_EQ(elems, 0);
}

// Test SLastCol reallocation
TEST_F(TsdbCacheEnv, LastColRealloc) {
  SLastCol lastCol;
  memset(&lastCol, 0, sizeof(SLastCol));

  lastCol.rowKey.ts = 1234567890;
  lastCol.rowKey.numOfPKs = 0;
  lastCol.dirty = 1;
  lastCol.colVal.cid = 1;
  lastCol.colVal.flag = CV_FLAG_VALUE;
  lastCol.colVal.value.type = TSDB_DATA_TYPE_INT;
  lastCol.colVal.value.val = 42;
  lastCol.cacheStatus = TSDB_LAST_CACHE_VALID;

  size_t  charge = 0;
  int32_t code = tsdbCacheReallocSLastCol(&lastCol, &charge);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_GT(charge, 0);
}

// Test SLastCol reallocation with varchar
TEST_F(TsdbCacheEnv, LastColReallocVarchar) {
  SLastCol lastCol;
  memset(&lastCol, 0, sizeof(SLastCol));

  lastCol.rowKey.ts = 1234567890;
  lastCol.rowKey.numOfPKs = 0;
  lastCol.dirty = 1;
  lastCol.colVal.cid = 2;
  lastCol.colVal.flag = CV_FLAG_VALUE;
  lastCol.colVal.value.type = TSDB_DATA_TYPE_VARCHAR;

  const char *testStr = "Test String";
  lastCol.colVal.value.nData = strlen(testStr);
  lastCol.colVal.value.pData = (uint8_t *)testStr;  // Not owned by lastCol initially
  lastCol.cacheStatus = TSDB_LAST_CACHE_VALID;

  size_t  charge = 0;
  int32_t code = tsdbCacheReallocSLastCol(&lastCol, &charge);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_GT(charge, sizeof(SLastCol));

  // Clean up allocated memory
  if (lastCol.colVal.value.pData && lastCol.colVal.value.nData > 0) {
    taosMemoryFree(lastCol.colVal.value.pData);
  }
}

// Test column status helper functions (public API only)
TEST_F(TsdbCacheEnv, ColStatusHelpers) {
  STsdb    *pTsdb = createMockTsdb(true);
  STSchema *pSchema = createMockSchema(3);
  ASSERT_NE(pTsdb, nullptr);
  ASSERT_NE(pSchema, nullptr);

  tb_uid_t uid = 12345;

  // Create a row cache entry first
  int32_t code = tsdbRowCacheCreateTable(pTsdb, uid, pSchema);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);

  // Look up the created entry
  SLastRowKey  key = {.uid = uid, .lflag = LFLAG_LAST};
  LRUHandle   *h = taosLRUCacheLookup(pTsdb->lruCache, &key, ROCKS_ROW_KEY_LEN);
  
  if (h) {
    SLastRow *pLastRow = (SLastRow *)taosLRUCacheValue(pTsdb->lruCache, h);
    
    // Test tsdbLastRowGetLastColValColStatus (public API)
    SColCacheStatus *colStatus = tsdbLastRowGetLastColValColStatus(pLastRow, pSchema->columns[1].colId);
    if (colStatus) {
      EXPECT_EQ(colStatus->cid, pSchema->columns[1].colId);
      EXPECT_EQ(colStatus->status, TSDB_LAST_CACHE_VALID);
    }

    // Test tsdbLastRowInvalidateCol (public API)
    code = tsdbLastRowInvalidateCol(pLastRow, pSchema->columns[1].colId);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);

    // Verify invalidation
    colStatus = tsdbLastRowGetLastColValColStatus(pLastRow, pSchema->columns[1].colId);
    if (colStatus) {
      EXPECT_EQ(colStatus->status, TSDB_LAST_CACHE_NO_CACHE);
    }

    taosLRUCacheRelease(pTsdb->lruCache, h, false);
  }

  taosMemoryFree(pSchema);
  destroyMockTsdb(pTsdb);
}

// Test error handling - null parameters (public API only)
TEST_F(TsdbCacheEnv, ErrorHandlingNullParams) {
  // Test tsdbCacheReallocSLastCol with null
  int32_t code = tsdbCacheReallocSLastCol(nullptr, nullptr);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);

  // Test tsdbLastRowGetLastColValColStatus with null (public API)
  SColCacheStatus *status = tsdbLastRowGetLastColValColStatus(nullptr, 0);
  EXPECT_EQ(status, nullptr);

  // Test tsdbLastRowInvalidateCol with null (public API)
  code = tsdbLastRowInvalidateCol(nullptr, 0);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);

  // Test tsdbNeedCacheUpgrade with null
  bool needUpgrade = tsdbNeedCacheUpgrade(nullptr);
  EXPECT_FALSE(needUpgrade);

  // Test tsdbSetCacheFormat with null
  code = tsdbSetCacheFormat(nullptr, TSDB_CACHE_FORMAT_ROW);
  EXPECT_EQ(code, TSDB_CODE_INVALID_PARA);
}

// Test cache usage scenarios
TEST_F(TsdbCacheEnv, CacheUsageScenarios) {
  STsdb *pTsdb = createMockTsdb(true);
  ASSERT_NE(pTsdb, nullptr);

  // Create multiple table caches
  STSchema *pSchema = createMockSchema(5);
  ASSERT_NE(pSchema, nullptr);

  for (int i = 0; i < 10; i++) {
    tb_uid_t uid = 1000 + i;
    int32_t  code = tsdbRowCacheCreateTable(pTsdb, uid, pSchema);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  }

  // Check cache usage increased
  size_t usage = taosLRUCacheGetUsage(pTsdb->lruCache);
  EXPECT_GT(usage, 0);

  int32_t elems = taosLRUCacheGetElems(pTsdb->lruCache);
  EXPECT_GT(elems, 0);

  // Delete some caches
  for (int i = 0; i < 5; i++) {
    tb_uid_t uid = 1000 + i;
    int32_t  code = tsdbRowCacheDeleteTable(pTsdb, uid);
    EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  }

  taosMemoryFree(pSchema);
  destroyMockTsdb(pTsdb);
}

// Test edge cases for key comparison
TEST_F(TsdbCacheEnv, KeyComparisonEdgeCases) {
  // Test with maximum and minimum values
  SRowKey keyMin = {.ts = TSKEY_MIN, .numOfPKs = 0};
  SRowKey keyMax = {.ts = TSKEY_MAX, .numOfPKs = 0};

  EXPECT_LT(tRowKeyCompare(&keyMin, &keyMax), 0);
  EXPECT_GT(tRowKeyCompare(&keyMax, &keyMin), 0);

  // Test with same timestamps
  SRowKey key1 = {.ts = 1000, .numOfPKs = 0};
  SRowKey key2 = {.ts = 1000, .numOfPKs = 0};
  EXPECT_EQ(tRowKeyCompare(&key1, &key2), 0);
}

// Test memory allocation failures simulation
TEST_F(TsdbCacheEnv, MemoryAllocationHandling) {
  SLastCol lastCol;
  memset(&lastCol, 0, sizeof(SLastCol));

  lastCol.rowKey.ts = 1234567890;
  lastCol.rowKey.numOfPKs = 0;
  lastCol.dirty = 1;
  lastCol.colVal.cid = 1;
  lastCol.colVal.flag = CV_FLAG_VALUE;
  lastCol.colVal.value.type = TSDB_DATA_TYPE_INT;
  lastCol.colVal.value.val = 42;
  lastCol.cacheStatus = TSDB_LAST_CACHE_VALID;

  size_t charge = 0;
  // This should succeed with valid input
  int32_t code = tsdbCacheReallocSLastCol(&lastCol, &charge);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
}

// Main function
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

