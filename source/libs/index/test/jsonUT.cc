#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "index.h"
#include "indexCache.h"
#include "indexFst.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexTfile.h"
#include "indexUtil.h"
#include "tglobal.h"
#include "tskiplist.h"
#include "tutil.h"

static std::string dir = TD_TMP_DIR_PATH "json";
static std::string logDir = TD_TMP_DIR_PATH "log";

SIndexTerm* indexTermCreateT(int64_t suid, SIndexOperOnColumn oper, uint8_t colType, const char* colName,
                             int32_t nColName, const char* colVal, int32_t nColVal) {
  char    buf[256] = {0};
  int16_t sz = nColVal;
  memcpy(buf, (uint16_t*)&sz, 2);
  memcpy(buf + 2, colVal, nColVal);
  if (colType == TSDB_DATA_TYPE_BINARY || colType == TSDB_DATA_TYPE_GEOMETRY) {
    return indexTermCreate(suid, oper, colType, colName, nColName, buf, sizeof(buf));
  } else {
    return indexTermCreate(suid, oper, colType, colName, nColName, colVal, nColVal);
  }
}
static void initLog() {
  const char*   defaultLogFileNamePrefix = "taoslog";
  const int32_t maxLogFileNum = 10;

  tsAsyncLog = 0;
  idxDebugFlag = 143;
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
    opts = indexOptsCreate(1024 * 1024 * 4);
    int ret = indexJsonOpen(opts, dir.c_str(), &index);
    assert(ret == 0);
  }
  virtual void TearDown() {
    indexJsonClose(index);
    indexOptsDestroy(opts);
    printf("destory\n");
    taosMsleep(1000);
  }
  SIndexJsonOpts* opts;
  SIndexJson*     index;
};

static void WriteData(SIndexJson* index, const std::string& colName, int8_t dtype, void* data, int dlen, int tableId,
                      int8_t operType = ADD_VALUE) {
  SIndexTerm*      term = indexTermCreateT(1, (SIndexOperOnColumn)operType, dtype, colName.c_str(), colName.size(),
                                           (const char*)data, dlen);
  SIndexMultiTerm* terms = indexMultiTermCreate();
  indexMultiTermAdd(terms, term);
  indexJsonPut(index, terms, (int64_t)tableId);

  indexMultiTermDestroy(terms);
}

static void delData(SIndexJson* index, const std::string& colName, int8_t dtype, void* data, int dlen, int tableId,
                    int8_t operType = DEL_VALUE) {
  SIndexTerm*      term = indexTermCreateT(1, (SIndexOperOnColumn)operType, dtype, colName.c_str(), colName.size(),
                                           (const char*)data, dlen);
  SIndexMultiTerm* terms = indexMultiTermCreate();
  indexMultiTermAdd(terms, term);
  indexJsonPut(index, terms, (int64_t)tableId);

  indexMultiTermDestroy(terms);
}
static void Search(SIndexJson* index, const std::string& colNam, int8_t dtype, void* data, int dlen, int8_t filterType,
                   SArray** result) {
  std::string colName(colNam);

  SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
  SIndexTerm* q = indexTermCreateT(1, ADD_VALUE, dtype, colName.c_str(), colName.size(), (const char*)data, dlen);

  SArray* res = taosArrayInit(1, sizeof(uint64_t));
  indexMultiTermQueryAdd(mq, q, (EIndexQueryType)filterType);
  indexJsonSearch(index, mq, res);
  indexMultiTermQueryDestroy(mq);
  *result = res;
}
TEST_F(JsonEnv, testWrite) {
  {
    std::string colName("test");
    std::string colVal("ab");
    for (int i = 0; i < 100; i++) {
      SIndexTerm*      term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                               colVal.c_str(), colVal.size());
      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("voltage");
    std::string colVal("ab1");
    for (int i = 0; i < 100; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                          colVal.c_str(), colVal.size());

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("voltage");
    std::string colVal("123");
    for (size_t i = 0; i < 100; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                          colVal.c_str(), colVal.size());

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test");
    std::string colVal("ab");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                               colVal.c_str(), colVal.size());

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(100, taosArrayGetSize(result));
    taosArrayDestroy(result);
    indexMultiTermQueryDestroy(mq);
  }
}
TEST_F(JsonEnv, testWriteMillonData) {
  {
    std::string colName("test");
    std::string colVal("ab");
    for (size_t i = 0; i < 10; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                          colVal.c_str(), colVal.size());

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("voltagefdadfa");
    std::string colVal("abxxxxxxxxxxxx");
    for (int i = 0; i < 10000; i++) {
      colVal[i % colVal.size()] = '0' + i % 128;
      for (size_t i = 0; i < 10; i++) {
        SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                            colVal.c_str(), colVal.size());

        SIndexMultiTerm* terms = indexMultiTermCreate();
        indexMultiTermAdd(terms, term);
        indexJsonPut(index, terms, i);
        indexMultiTermDestroy(terms);
      }
    }
  }
  {
    std::string colName("voltagefdadfa");
    std::string colVal("abxxxxxxxxxxxx");
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                          colVal.c_str(), colVal.size());

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test");
    std::string colVal("ab");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                               colVal.c_str(), colVal.size());

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(10, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    {
      std::string colName("test");
      std::string colVal("ab");

      SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
      SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                                 colVal.c_str(), colVal.size());

      SArray* result = taosArrayInit(1, sizeof(uint64_t));
      indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
      indexJsonSearch(index, mq, result);
      EXPECT_EQ(0, taosArrayGetSize(result));
      indexMultiTermQueryDestroy(mq);
      taosArrayDestroy(result);
    }
    {
      {
        std::string colName("test");
        std::string colVal("ab");

        SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
        SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                                   colVal.c_str(), colVal.size());

        SArray* result = taosArrayInit(1, sizeof(uint64_t));
        indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
        indexJsonSearch(index, mq, result);
        EXPECT_EQ(10, taosArrayGetSize(result));
        indexMultiTermQueryDestroy(mq);
        taosArrayDestroy(result);
      }
    }
  }
}
TEST_F(JsonEnv, testWriteJsonNumberData) {
  {
    std::string colName("test");
    // std::string colVal("10");
    int val = 10;
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                          (const char*)&val, sizeof(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test2");
    int         val = 20;
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                          (const char*)&val, sizeof(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test");
    int         val = 15;
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                          (const char*)&val, sizeof(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test2");
    const char* val = "test";
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                          (const char*)val, strlen(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string           colName("test");
    int                   val = 15;
    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test");
    int         val = 15;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test");
    int         val = 10;
    ;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(int));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test");
    int         val = 10;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_THAN);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test");
    int         val = 10;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_EQUAL);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
}

TEST_F(JsonEnv, testWriteJsonTfileAndCache_INT) {
  {
    std::string colName("test1");
    int         val = 10;
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                          (const char*)&val, sizeof(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test");
    std::string colVal("xxxxxxxxxxxxxxxxxxx");
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_BINARY, colName.c_str(), colName.size(),
                                          colVal.c_str(), colVal.size());

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_TERM);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(int));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test1");
    // std::string colVal("10");
    int val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_THAN);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test1");
    int         val = 10;

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_EQUAL);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(1000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("other_column");
    int         val = 100;

    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                          (const char*)&val, sizeof(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test1");
    int         val = 10;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_LESS_THAN);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(0, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
  {
    std::string colName("test1");
    int         val = 15;
    for (size_t i = 0; i < 1000; i++) {
      SIndexTerm* term = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                          (const char*)&val, sizeof(val));

      SIndexMultiTerm* terms = indexMultiTermCreate();
      indexMultiTermAdd(terms, term);
      indexJsonPut(index, terms, i + 1000);
      indexMultiTermDestroy(terms);
    }
  }
  {
    std::string colName("test1");
    int         val = 8;
    // std::string colVal("10");

    SIndexMultiTermQuery* mq = indexMultiTermQueryCreate(MUST);
    SIndexTerm*           q = indexTermCreateT(1, ADD_VALUE, TSDB_DATA_TYPE_INT, colName.c_str(), colName.size(),
                                               (const char*)&val, sizeof(val));

    SArray* result = taosArrayInit(1, sizeof(uint64_t));
    indexMultiTermQueryAdd(mq, q, QUERY_GREATER_EQUAL);
    indexJsonSearch(index, mq, result);
    EXPECT_EQ(2000, taosArrayGetSize(result));
    indexMultiTermQueryDestroy(mq);
    taosArrayDestroy(result);
  }
}
TEST_F(JsonEnv, testWriteJsonTfileAndCache_INT2) {
  {
    int         val = 10;
    std::string colName("test1");
    for (int i = 0; i < 1000; i++) {
      val += 1;
      WriteData(index, colName, TSDB_DATA_TYPE_INT, &val, sizeof(val), i);
    }
  }
  {
    int         val = 10;
    std::string colName("test2xxx");
    std::string colVal("xxxxxxxxxxxxxxx");
    for (int i = 0; i < 1000; i++) {
      val += 1;
      WriteData(index, colName, TSDB_DATA_TYPE_BINARY, (void*)(colVal.c_str()), colVal.size(), i);
    }
  }
  {
    SArray*     res = NULL;
    std::string colName("test1");
    int         val = 9;
    Search(index, colName, TSDB_DATA_TYPE_INT, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    SArray*     res = NULL;
    std::string colName("test2xxx");
    std::string colVal("xxxxxxxxxxxxxxx");
    Search(index, colName, TSDB_DATA_TYPE_BINARY, (void*)(colVal.c_str()), colVal.size(), QUERY_TERM, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
}
TEST_F(JsonEnv, testWriteJsonTfileAndCache_FLOAT) {
  {
    float       val = 10.0;
    std::string colName("test1");
    for (int i = 0; i < 1000; i++) {
      WriteData(index, colName, TSDB_DATA_TYPE_FLOAT, &val, sizeof(val), i);
    }
  }
  {
    float       val = 2.0;
    std::string colName("test1");
    for (int i = 0; i < 1000; i++) {
      WriteData(index, colName, TSDB_DATA_TYPE_FLOAT, &val, sizeof(val), i + 1000);
    }
  }
  {
    SArray*     res = NULL;
    std::string colName("test1");
    float       val = 1.9;
    Search(index, colName, TSDB_DATA_TYPE_FLOAT, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(2000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    SArray*     res = NULL;
    std::string colName("test1");
    float       val = 2.1;
    Search(index, colName, TSDB_DATA_TYPE_FLOAT, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    std::string colName("test1");
    SArray*     res = NULL;
    float       val = 2.1;
    Search(index, colName, TSDB_DATA_TYPE_FLOAT, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
}
TEST_F(JsonEnv, testWriteJsonTfileAndCache_DOUBLE) {
  {
    double val = 10.0;
    for (int i = 0; i < 1000; i++) {
      WriteData(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), i);
    }
  }
  {
    double val = 2.0;
    for (int i = 0; i < 1000; i++) {
      WriteData(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), i + 1000);
    }
  }
  {
    SArray*     res = NULL;
    std::string colName("test1");
    double      val = 1.9;
    Search(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(2000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    SArray* res = NULL;
    double  val = 2.1;
    Search(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    SArray* res = NULL;
    double  val = 2.1;
    Search(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), QUERY_GREATER_EQUAL, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    SArray* res = NULL;
    double  val = 10.0;
    Search(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), QUERY_LESS_EQUAL, &res);
    EXPECT_EQ(2000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
  {
    SArray* res = NULL;
    double  val = 10.0;
    Search(index, "test1", TSDB_DATA_TYPE_DOUBLE, &val, sizeof(val), QUERY_LESS_THAN, &res);
    EXPECT_EQ(1000, taosArrayGetSize(res));
    taosArrayDestroy(res);
  }
}
