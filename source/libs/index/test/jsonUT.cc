/*
 * Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
#include <algorithm>
#include <iostream>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>
#ifdef LINUX
#include <atomic>
#include "stub.h"
#include "indexComm.h"
#endif
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
  if (colType == TSDB_DATA_TYPE_BINARY || colType == TSDB_DATA_TYPE_GEOMETRY || colType == TSDB_DATA_TYPE_NCHAR) {
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

  if (taosInitLog(defaultLogFileNamePrefix, maxLogFileNum, 0) < 0) {
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
    TD_ALWAYS_ASSERT(ret == 0);
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

TEST_F(JsonEnv, Ucs4KeysSurviveFlushAndPreserveCodePointOrder) {
  // Include embedded zero bytes, opposite little-endian byte ordering,
  // a supplementary character and the empty string.
  TdUcs4 values[] = {0x00ff, 0x0100, 0x4e2d, 0x1f600};
  for (int i = 0; i < 4; ++i) {
    WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &values[i], sizeof(TdUcs4), i + 1);
  }
  char empty = 0;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &empty, 0, 5);
  for (int pass = 0; pass < 2; ++pass) {
    SArray *result = nullptr;
    Search(index, "city", TSDB_DATA_TYPE_NCHAR, &values[1], sizeof(TdUcs4), QUERY_TERM, &result);
    ASSERT_EQ(1, taosArrayGetSize(result));
    EXPECT_EQ(2, *(uint64_t *)taosArrayGet(result, 0));
    taosArrayDestroy(result);
    Search(index, "city", TSDB_DATA_TYPE_NCHAR, &values[1], sizeof(TdUcs4), QUERY_LESS_THAN, &result);
    EXPECT_EQ(2, taosArrayGetSize(result));  // Empty and U+00FF.
    taosArrayDestroy(result);
    ASSERT_EQ(0, indexCloseChecked(index));
    index = nullptr;
    ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  }
  delData(index, "city", TSDB_DATA_TYPE_NCHAR, &values[1], sizeof(TdUcs4), 2);
  ASSERT_EQ(0, indexCloseChecked(index));
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  SArray *result = nullptr;
  Search(index, "city", TSDB_DATA_TYPE_NCHAR, &values[1], sizeof(TdUcs4), QUERY_TERM, &result);
  EXPECT_EQ(0, taosArrayGetSize(result));
  taosArrayDestroy(result);
}

TEST_F(JsonEnv, PublishAndQuarantinePopulatedDirectory) {
  std::string staging = dir + "/building", published = dir + "/final", quarantine = dir + "/corrupt";
  ASSERT_EQ(0, taosMkDir(staging.c_str()));
  std::string entry = staging + "/entry.tindex";
  TdFilePtr file = taosCreateFile(entry.c_str(), TD_FILE_CREATE | TD_FILE_WRITE);
  ASSERT_NE(nullptr, file);
  ASSERT_EQ(0, taosCloseFile(&file));
  ASSERT_EQ(0, taosRenameDir(staging.c_str(), published.c_str()));
  ASSERT_EQ(0, taosRenameDir(published.c_str(), quarantine.c_str()));
  EXPECT_TRUE(taosCheckExistFile((quarantine + "/entry.tindex").c_str()));
  EXPECT_FALSE(taosCheckExistFile(staging.c_str()));
  EXPECT_FALSE(taosCheckExistFile(published.c_str()));
}

#ifdef LINUX
static char* failJsonPack(SIndexTerm*) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST(JsonCache, PackingFailurePreservesReferenceCount) {
  SIndexTerm term = {};
  term.colType = TSDB_DATA_TYPE_NCHAR;
  IDX_TYPE_ADD_EXTERN_TYPE(term.colType, TSDB_DATA_TYPE_JSON);
  IndexCache* cache = idxCacheCreate(nullptr, 1, "city", term.colType);
  ASSERT_NE(nullptr, cache);
  int refs = T_REF_VAL_GET(cache);
  Stub stub;
  stub.set(idxPackJsonData, failJsonPack);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, idxCachePut(cache, &term, 1));
  EXPECT_EQ(refs, T_REF_VAL_GET(cache));
  idxCacheUnRef(cache);
}

static SSkipListNode* failCacheInsert(SSkipList*, void*) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST(JsonCache, FailedInsertDoesNotChargeCache) {
  char key[] = "city";
  SIndexTerm term = {};
  term.colType = TSDB_DATA_TYPE_BINARY;
  term.colVal = key;
  term.nColVal = sizeof(key) - 1;
  IndexCache* cache = idxCacheCreate(nullptr, 1, "tag", term.colType);
  ASSERT_NE(nullptr, cache);
  int refs = T_REF_VAL_GET(cache);
  Stub stub;
  stub.set(tSkipListPut, failCacheInsert);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, idxCachePut(cache, &term, 1));
  stub.reset(tSkipListPut);
  EXPECT_EQ(0, cache->occupiedMem);
  EXPECT_EQ(refs, T_REF_VAL_GET(cache));
  EXPECT_EQ(0, idxCachePut(cache, &term, 1));
  idxCacheUnRef(cache);
}

static void* failUidAppend(SArray*, const void*, int32_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

static void* failNodeAlloc(int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

static SArray* failTransitionArray(size_t, size_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST(JsonFst, EmptyNodeReportsAllocationAndStackFailures) {
  FstUnFinishedNodes* nodes = fstUnFinishedNodesCreate();
  ASSERT_NE(nullptr, nodes);
  Stub stub;
  stub.set(taosMemMalloc, failNodeAlloc);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, fstUnFinishedNodesPushEmpty(nodes, true));
  stub.reset(taosMemMalloc);
  stub.set(taosArrayInit, failTransitionArray);
  EXPECT_EQ(nullptr, fstUnFinishedNodesCreate());
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, terrno);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, fstUnFinishedNodesPushEmpty(nodes, true));
  stub.reset(taosArrayInit);
  stub.set(taosArrayAddBatch, failUidAppend);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, fstUnFinishedNodesPushEmpty(nodes, true));
  EXPECT_EQ(nullptr, fstBuilderCreate(nullptr, 0));
  stub.reset(taosArrayAddBatch);
  EXPECT_EQ(1, taosArrayGetSize(nodes->stack));
  fstUnFinishedNodesDestroy(nodes);
}

TEST(JsonFst, InsertReportsChildStackFailure) {
  uint8_t data[] = {'a', 'b'};
  for (size_t length : {1, 2}) {
    IFileCtx* ctx = idxFileCtxCreate(TMEMORY, nullptr, false, 4096);
    ASSERT_NE(nullptr, ctx);
    FstBuilder* builder = fstBuilderCreate(ctx, 0);
    ASSERT_NE(nullptr, builder);
    FstSlice key = fstSliceCreate(data, length);
    Stub stub;
    stub.set(taosArrayAddBatch, failUidAppend);
    EXPECT_FALSE(fstBuilderInsert(builder, key, 1));
    EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, terrno);
    stub.reset(taosArrayAddBatch);
    fstSliceDestroy(&key);
    fstBuilderDestroy(builder);
    idxFileCtxDestroy(ctx, false);
  }
}

static void* failStackGrowth(void*, int64_t) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST(JsonFst, InsertReportsStackGrowthFailureAfterPartialSuffix) {
  IFileCtx* ctx = idxFileCtxCreate(TMEMORY, nullptr, false, 4096);
  ASSERT_NE(nullptr, ctx);
  FstBuilder* builder = fstBuilderCreate(ctx, 0);
  ASSERT_NE(nullptr, builder);
  SArray* stack = builder->unfinished->stack;
  const size_t capacity = stack->capacity;
  ASSERT_GT(capacity, 1u);
  std::vector<uint8_t> bytes(capacity, 'a');
  FstSlice key = fstSliceCreate(bytes.data(), bytes.size());

  Stub stub;
  stub.set(taosMemRealloc, failStackGrowth);
  EXPECT_FALSE(fstBuilderInsert(builder, key, 1));
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, terrno);
  stub.reset(taosMemRealloc);
  EXPECT_EQ(capacity, taosArrayGetSize(stack));
  fstSliceDestroy(&key);
  fstBuilderDestroy(builder);
  idxFileCtxDestroy(ctx, false);
}

static FstLastTransition* failTransition(uint8_t, Output) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  return nullptr;
}

TEST(JsonFst, FinalTransitionGrowthIsChecked) {
  for (bool injectFailure : {false, true}) {
    IFileCtx* ctx = idxFileCtxCreate(TMEMORY, nullptr, false, 4096);
    ASSERT_NE(nullptr, ctx);
    FstBuilder* builder = fstBuilderCreate(ctx, 0);
    ASSERT_NE(nullptr, builder);
    for (uint8_t i = 1; i <= 17; ++i) {
      FstSlice key = fstSliceCreate(&i, 1);
      ASSERT_TRUE(fstBuilderInsert(builder, key, i));
      fstSliceDestroy(&key);
    }

    Stub stub;
    if (injectFailure) stub.set(taosArrayAddBatch, failUidAppend);
    int32_t code = fstBuilderFinish(builder);
    if (injectFailure) stub.reset(taosArrayAddBatch);
    fstBuilderDestroy(builder);
    EXPECT_EQ(injectFailure ? TSDB_CODE_OUT_OF_MEMORY : 0, code);
    if (!injectFailure) {
      FstSlice bytes = fstSliceCreate((uint8_t*)ctx->mem.buf, ctx->offset);
      Fst* fst = fstCreate(&bytes);
      ASSERT_NE(nullptr, fst);
      for (uint8_t i = 1; i <= 17; ++i) {
        FstSlice key = fstSliceCreate(&i, 1);
        uint64_t value = 0;
        EXPECT_TRUE(fstGet(fst, &key, &value));
        EXPECT_EQ(i, value);
        fstSliceDestroy(&key);
      }
      fstDestroy(fst);
      fstSliceDestroy(&bytes);
    }
    idxFileCtxDestroy(ctx, false);
  }
}

TEST(JsonCache, IteratorReportsUidAppendFailure) {
  char key[] = "city";
  SIndexTerm term = {};
  term.colType = TSDB_DATA_TYPE_BINARY;
  term.colVal = key;
  term.nColVal = sizeof(key) - 1;
  IndexCache* cache = idxCacheCreate(nullptr, 1, "tag", term.colType);
  ASSERT_NE(nullptr, cache);
  ASSERT_EQ(0, idxCachePut(cache, &term, 1));
  std::swap(cache->imm, cache->mem);
  Iterate* iter = nullptr;
  ASSERT_EQ(0, idxCacheIteratorCreate(cache, &iter));
  ASSERT_NE(nullptr, iter);
  Stub stub;
  stub.set(taosArrayAddBatch, failUidAppend);
  bool advanced = iter->next(iter);
  stub.reset(taosArrayAddBatch);
  EXPECT_FALSE(advanced);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, iter->error);
  EXPECT_STREQ(key, iter->val.colVal);
  EXPECT_EQ(0, taosArrayGetSize(iter->val.val));
  idxCacheIteratorDestroy(iter);
  idxCacheUnRef(cache);
}

static char* failSecondJsonKeyCopy(const char* key) {
  if (strncmp(key, "zCity", 5) == 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  size_t size = strlen(key) + 1;
  char* copy = (char*)taosMemoryMalloc(size);
  if (copy != nullptr) memcpy(copy, key, size);
  return copy;
}

TEST_F(JsonEnv, CheckedCloseReportsIteratorAdvanceFailure) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "aCity", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  WriteData(index, "zCity", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 2);
  Stub stub;
  stub.set(taosStrdupi, failSecondJsonKeyCopy);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, indexCloseChecked(index));
  index = nullptr;
  stub.reset(taosStrdupi);
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
}

TEST_F(JsonEnv, CheckedCloseReportsSuffixTransitionFailure) {
  TdUcs4 saved = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &saved, sizeof(saved), 1);
  ASSERT_EQ(0, indexCloseChecked(index));
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));

  TdUcs4 added = 0x5317;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &added, sizeof(added), 2);
  Stub stub;
  stub.set(fstLastTransitionCreate, failTransition);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, indexCloseChecked(index));
  index = nullptr;
  stub.reset(fstLastTransitionCreate);

  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  SArray* result = nullptr;
  Search(index, "city", TSDB_DATA_TYPE_NCHAR, &saved, sizeof(saved), QUERY_TERM, &result);
  ASSERT_EQ(1, taosArrayGetSize(result));
  EXPECT_EQ(1, *(uint64_t*)taosArrayGet(result, 0));
  taosArrayDestroy(result);
  Search(index, "city", TSDB_DATA_TYPE_NCHAR, &added, sizeof(added), QUERY_TERM, &result);
  EXPECT_EQ(0, taosArrayGetSize(result));
  taosArrayDestroy(result);
}

static int32_t failFstFinish(FstBuilder*) { return TSDB_CODE_OUT_OF_MEMORY; }

TEST_F(JsonEnv, CheckedCloseReportsFstFinishFailure) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  Stub stub;
  stub.set(fstBuilderFinish, failFstFinish);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, indexCloseChecked(index));
  index = nullptr;
  stub.reset(fstBuilderFinish);
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
}

static FStmStRslt* failDiskStreamResult(FstSlice*, FstOutput, void*) { return nullptr; }

TEST_F(JsonEnv, CheckedCloseReportsDiskIteratorAdvanceFailure) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  ASSERT_EQ(0, indexCloseChecked(index));
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 2);
  Stub stub;
  stub.set(swsResultCreate, failDiskStreamResult);
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, indexCloseChecked(index));
  stub.reset(swsResultCreate);
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  SArray* result = taosArrayInit(1, sizeof(uint64_t));
  ASSERT_NE(nullptr, result);
  Search(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), QUERY_TERM, &result);
  ASSERT_EQ(1, taosArrayGetSize(result));
  EXPECT_EQ(1, *(uint64_t*)taosArrayGet(result, 0));
  taosArrayDestroy(result);
}

static int64_t (*readPostingBlock)(IFileCtx*, uint8_t*, int, int32_t);
static int readsUntilFailure;
static bool shortRead;

static int64_t failPostingBlock(IFileCtx* ctx, uint8_t* buf, int len, int32_t offset) {
  if (--readsUntilFailure == 0) return shortRead ? len - 1 : static_cast<int32_t>(TAOS_SYSTEM_ERROR(EIO));
  return readPostingBlock(ctx, buf, len, offset);
}

TEST_F(JsonEnv, PostingReadErrorsAreDistinctFromShortReads) {
  TdUcs4 value = 0x4e2d;
  for (int i = 1; i <= 513; ++i) WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), i);
  ASSERT_EQ(0, indexCloseChecked(index));
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  TFileReader* reader = tfileGetReaderByCol((IndexTFile*)index->tindex, 1, JSON_COLUMN);
  ASSERT_NE(nullptr, reader);
  Iterate* healthy = tfileIteratorCreate(reader);
  ASSERT_NE(nullptr, healthy);
  ASSERT_TRUE(healthy->next(healthy));
  EXPECT_EQ(513, taosArrayGetSize(healthy->val.val));
  EXPECT_EQ(513, *(uint64_t*)taosArrayGetLast(healthy->val.val));
  tfileIteratorDestroy(healthy);
  readPostingBlock = reader->ctx->readFrom;
  for (int read : {1, 2, 3}) {
    for (bool truncated : {false, true}) {
      Iterate* iter = tfileIteratorCreate(reader);
      ASSERT_NE(nullptr, iter);
      readsUntilFailure = read;
      shortRead = truncated;
      reader->ctx->readFrom = failPostingBlock;
      EXPECT_FALSE(iter->next(iter));
      reader->ctx->readFrom = readPostingBlock;
      EXPECT_EQ(truncated ? TSDB_CODE_INDEX_INVALID_FILE : static_cast<int32_t>(TAOS_SYSTEM_ERROR(EIO)), iter->error);
      tfileIteratorDestroy(iter);
    }
  }
  tfileReaderUnRef(reader);
}

static std::atomic<bool> failNextIterator{false};

static SSkipListIterator* failFirstIterator(SSkipList* list) {
  if (failNextIterator.exchange(false)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return nullptr;
  }
  return tSkipListCreateIterFromVal(list, nullptr, TSDB_DATA_TYPE_BINARY, TSDB_ORDER_ASC);
}

TEST_F(JsonEnv, CheckedCloseReportsIteratorAllocationFailure) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  Stub stub;
  stub.set(tSkipListCreateIter, failFirstIterator);
  failNextIterator = true;
  EXPECT_EQ(TSDB_CODE_OUT_OF_MEMORY, indexCloseChecked(index));
  index = nullptr;
  EXPECT_FALSE(failNextIterator.load());
  stub.reset(tSkipListCreateIter);
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
}
#endif

TEST_F(JsonEnv, CheckedCloseReportsUnwritableIndex) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  // Force a real file-open failure during asynchronous flush, not an injected
  // return code. The migration must not publish an index after this failure.
  taosMemoryFree(index->path);
  index->path = taosStrdup((dir + "/missing/subdirectory").c_str());
  ASSERT_NE(nullptr, index->path);
  EXPECT_NE(0, indexCloseChecked(index));
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
}

static std::string firstJsonIndexFile() {
  TdDirPtr directory = taosOpenDir(dir.c_str());
  if (directory == nullptr) return "";
  std::string result;
  TdDirEntryPtr entry;
  while ((entry = taosReadDir(directory)) != nullptr) {
    std::string name = taosGetDirEntryName(entry);
    if (name.size() >= 7 && name.substr(name.size() - 7) == ".tindex") {
      result = dir + "/" + name;
      break;
    }
  }
  (void)taosCloseDir(&directory);
  return result;
}

TEST_F(JsonEnv, RejectsCorruptPublishedFiles) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  ASSERT_EQ(0, indexCloseChecked(index));
  index = nullptr;
  std::string file = firstJsonIndexFile();
  ASSERT_FALSE(file.empty());
  std::string original;
  {
    std::ifstream input(file, std::ios::binary);
    original.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
  }
  ASSERT_GT(original.size(), sizeof(TFileHeader) + 44);
  TFileHeader header;
  memcpy(&header, original.data(), sizeof(header));
  for (int damage = 0; damage < 5; ++damage) {
    std::string bytes = original;
    if (damage == 0) bytes.back() ^= 1;  // Invalid footer magic.
    if (damage == 1) bytes.resize(sizeof(TFileHeader) - 1);  // Truncated header.
    if (damage == 2) bytes[header.fstOffset + 16] ^= 1;  // FST checksum mismatch.
    if (damage == 3) memset(&bytes[offsetof(TFileHeader, fstOffset)], 0xff, sizeof(int32_t));
    if (damage == 4) memset(&bytes[offsetof(TFileHeader, colName)], 'x', sizeof(header.colName));
    {
      std::ofstream output(file, std::ios::binary | std::ios::trunc);
      output.write(bytes.data(), bytes.size());
    }
    // A stale OOM must not turn confirmed corruption into an allocation error.
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    EXPECT_EQ(TSDB_CODE_INDEX_INVALID_FILE, indexJsonOpen(opts, dir.c_str(), &index)) << damage;
    EXPECT_EQ(nullptr, index);
    if (index != nullptr) { indexJsonClose(index); index = nullptr; }
  }
  {
    std::ofstream output(file, std::ios::binary | std::ios::trunc);
    output.write(original.data(), original.size());
  }
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
}

static int64_t (*jsonOriginalRead)(IFileCtx*, uint8_t*, int, int32_t);
static int jsonReadCall, jsonFailCall;
static int32_t jsonReadError;
static int64_t jsonFailRead(IFileCtx* ctx, uint8_t* buf, int len, int32_t offset) {
  if (++jsonReadCall == jsonFailCall) return jsonReadError;
  return jsonOriginalRead(ctx, buf, len, offset);
}

TEST_F(JsonEnv, ReaderPreservesIoAndAllocationErrors) {
  TdUcs4 value = 0x4e2d;
  WriteData(index, "city", TSDB_DATA_TYPE_NCHAR, &value, sizeof(value), 1);
  ASSERT_EQ(0, indexCloseChecked(index));
  index = nullptr;
  ASSERT_EQ(0, indexJsonOpen(opts, dir.c_str(), &index));
  std::string file = firstJsonIndexFile();
  ASSERT_FALSE(file.empty());
  // Exercise footer, header and FST reads independently, including allocation
  // failure from the block-cache reader. Reader creation owns ctx on failure.
  int32_t errors[] = {static_cast<int32_t>(TAOS_SYSTEM_ERROR(EIO)), static_cast<int32_t>(TAOS_SYSTEM_ERROR(EACCES)), TSDB_CODE_OUT_OF_MEMORY};
  for (int32_t error : errors) {
    for (int call = 1; call <= 3; ++call) {
      IFileCtx* ctx = idxFileCtxCreate(TFILE, file.c_str(), true, 1024);
      ASSERT_NE(nullptr, ctx);
      ctx->lru = index->lru;
      jsonOriginalRead = ctx->readFrom;
      jsonReadCall = 0;
      jsonFailCall = call;
      jsonReadError = error;
      ctx->readFrom = jsonFailRead;
      TFileReader* reader = nullptr;
      EXPECT_EQ(error, tfileReaderCreate(ctx, &reader));
      EXPECT_EQ(nullptr, reader);
    }
  }
  // A real OS read failure must also survive the IFileCtx layer.
  IFileCtx* ctx = idxFileCtxCreate(TFILE, file.c_str(), true, 1024);
  ASSERT_NE(nullptr, ctx);
  ctx->lru = taosLRUCacheInit(1024 * 1024, -1, .5);
  ASSERT_NE(nullptr, ctx->lru);
  (void)taosCloseFile(&ctx->file.pFile);
  uint8_t byte;
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, ctx->readFrom(ctx, &byte, 1, 0));
  taosLRUCacheCleanup(ctx->lru);
  idxFileCtxDestroy(ctx, false);
}
