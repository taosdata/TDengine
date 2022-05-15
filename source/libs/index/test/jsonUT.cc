#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "index.h"
#include "indexCache.h"
#include "indexFst.h"
#include "indexFstCountingWriter.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexTfile.h"
#include "indexUtil.h"
#include "tglobal.h"
#include "tskiplist.h"
#include "tutil.h"

static std::string dir = "/tmp/json";
static std::string logDir = "/tmp/log";

static void initLog() {
  const char*   defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  sDebugFlag = 143;
  strcpy(tsLogDir, logDir.c_str());
  taosRemoveDir(tsLogDir);
  taosMkDir(tsLogDir);

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum) < 0) {
    printf("failed to open log file in directory:%s\n", tsLogDir);
  }
}
class JsonEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    taosRemoveDir(logDir.c_str());
    taosMkDir(logDir.c_str());
    taosRemoveDir(dir.c_str());
    taosMkDir(dir.c_str());
    printf("set up\n");

    initLog();
    opts = indexOptsCreate();
    int ret = tIndexJsonOpen(opts, dir.c_str(), &index);
    assert(ret == 0);
  }
  virtual void TearDown() {
    tIndexJsonClose(index);
    indexOptsDestroy(opts);
    printf("destory\n");
  }
  SIndexJsonOpts* opts;
  SIndexJson*     index;
};

TEST_F(JsonEnv, testWrite) {
  {
    std::string colName("test");
    std::string colVal("ab");
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       colVal.c_str(), colVal.size());

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 100; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("voltage");
    std::string colVal("ab1");
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       colVal.c_str(), colVal.size());

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 100; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("voltage");
    std::string colVal("123");
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       colVal.c_str(), colVal.size());

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 100; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test");
    std::string colVal("ab");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                    colVal.c_str(), colVal.size());

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    tIndexJsonSearch(index, mq, result);
    assert(100 == taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
}
TEST_F(JsonEnv, testWriteMillonData) {
  {
    std::string colName("test");
    std::string colVal("ab");
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       colVal.c_str(), colVal.size());

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 100; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("voltagefdadfa");
    std::string colVal("abxxxxxxxxxxxx");
    for (int i = 0; i < 1000; i++) {
      colVal[i % colVal.size()] = '0' + i % 128;
      SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                         colVal.c_str(), colVal.size());

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      for (size_t i = 0; i < 1000; i++) {
        tIndexJsonPut(index, terms, i);
      }
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("voltagefdadfa");
    std::string colVal("abxxxxxxxxxxxx");
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       colVal.c_str(), colVal.size());

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test");
    std::string colVal("ab");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                    colVal.c_str(), colVal.size());

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    tIndexJsonSearch(index, mq, result);
    assert(100 == taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    {
      std::string colName("test");
      std::string colVal("ab");

      SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
      SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                      colVal.c_str(), colVal.size());

      SArray* result = taosArrayInit(1, sizeof(uint64_t));
      indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
      tIndexJsonSearch(index, mq, result);
      assert(0 == taosArrayGetSize(result));
      indexMultiTermQueryDestroy(mq);
    }
    {
      {
        std::string colName("test");
        std::string colVal("ab");

        SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
        SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                        colVal.c_str(), colVal.size());

        SArray* result = taosArrayInit(1, sizeof(uint64_t));
        indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
        tIndexJsonSearch(index, mq, result);
        assert(100 == taosArrayGetSize(result));
        indexMultiTermQueryDestroy(mq);
      }
    }
  }
}
TEST_F(JsonEnv, testWriteJsonNumberData) {
  {
    std::string colName("test");
    // std::string colVal("10");
    int         val = 10;
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                       (const char*)&val, sizeof(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test2");
    int         val = 20;
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                       (const char*)&val, sizeof(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test");
    int         val = 15;
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                       (const char*)&val, sizeof(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test2");
    const char* val = "test";
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       (const char*)val, strlen(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string           colName("test");
    int                   val = 15;
    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test");
    int         val = 15;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test");
    int         val = 10;
    ;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(int));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test");
    int         val = 10;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_THAN);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test");
    int         val = 10;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_EQUAL);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
}

TEST_F(JsonEnv, testWriteJsonTfileAndCache) {
  {
    std::string colName("test1");
    int         val = 10;
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                       (const char*)&val, sizeof(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test");
    std::string colVal("xxxxxxxxxxxxxxxxxxx");
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                       colVal.c_str(), colVal.size());

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 100000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(int));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test1");
    // std::string colVal("10");
    int val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_EQUAL);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("other_column");
    int         val = 100;

    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                       (const char*)&val, sizeof(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test1");
    int         val = 10;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_THAN);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
  {
    std::string colName("test1");
    int         val = 15;
    SIndexTerm* term = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                       (const char*)&val, sizeof(val));

    SIndexMultiTerm* terms = indexMultiTermCreate();
    indexMultiTermAdd(terms, term);
    for (size_t i = 0; i < 1000; i++) {
      tIndexJsonPut(index, terms, i);
    }
    indexMultiTermDestroy(terms);
  }
  {
    std::string colName("test1");
    int         val = 8;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreate(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                    (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
    tIndexJsonSearch(index, mq, result);
    EXPECT_EQ(2000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
  }
}
