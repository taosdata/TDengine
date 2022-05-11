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

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

class ParserInitialCTest : public ParserDdlTest {};

// todo compact

TEST_F(ParserInitialCTest, createAccount) {
  useDb("root", "test");

  run("create account ac_wxy pass '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT);
}

TEST_F(ParserInitialCTest, createBnode) {
  useDb("root", "test");

  run("create bnode on dnode 1");
}

TEST_F(ParserInitialCTest, createDatabase) {
  useDb("root", "test");

  run("create database wxy_db");

  run("create database if not exists wxy_db "
      "cachelast 2 "
      "comp 1 "
      "days 100 "
      "fsync 100 "
      "maxrows 1000 "
      "minrows 100 "
      "keep 1440 "
      "precision 'ms' "
      "replica 3 "
      "wal 2 "
      "vgroups 100 "
      "single_stable 0 "
      "retentions 15s:7d,1m:21d,15m:5y");

  run("create database if not exists wxy_db "
      "days 100m "
      "keep 1440m,300h,400d ");
}

TEST_F(ParserInitialCTest, createDnode) {
  useDb("root", "test");

  run("create dnode abc1 port 7000");

  run("create dnode 1.1.1.1 port 9000");
}

// todo create function

TEST_F(ParserInitialCTest, createIndexSma) {
  useDb("root", "test");

  run("create sma index index1 on t1 function(max(c1), min(c3 + 10), sum(c4)) INTERVAL(10s)");
}

TEST_F(ParserInitialCTest, createMnode) {
  useDb("root", "test");

  run("create mnode on dnode 1");
}

TEST_F(ParserInitialCTest, createQnode) {
  useDb("root", "test");

  run("create qnode on dnode 1");
}

TEST_F(ParserInitialCTest, createSnode) {
  useDb("root", "test");

  run("create snode on dnode 1");
}

TEST_F(ParserInitialCTest, createStable) {
  useDb("root", "test");

  SMCreateStbReq expect = {0};

  auto setCreateStbReqFunc = [&](const char* pTbname, int8_t igExists = 0,
                                 float   xFilesFactor = TSDB_DEFAULT_ROLLUP_FILE_FACTOR,
                                 int32_t delay = TSDB_DEFAULT_ROLLUP_DELAY, int32_t ttl = TSDB_DEFAULT_TABLE_TTL,
                                 const char* pComment = nullptr) {
    memset(&expect, 0, sizeof(SMCreateStbReq));
    int32_t len = snprintf(expect.name, sizeof(expect.name), "0.test.%s", pTbname);
    expect.name[len] = '\0';
    expect.igExists = igExists;
    expect.xFilesFactor = xFilesFactor;
    expect.delay = delay;
    expect.ttl = ttl;
    if (nullptr != pComment) {
      expect.comment = strdup(pComment);
      expect.commentLen = strlen(pComment) + 1;
    }
  };

  auto addFieldToCreateStbReqFunc = [&](bool col, const char* pFieldName, uint8_t type, int32_t bytes = 0,
                                        int8_t flags = COL_SMA_ON) {
    SField field = {0};
    strcpy(field.name, pFieldName);
    field.type = type;
    field.bytes = bytes > 0 ? bytes : tDataTypes[type].bytes;
    field.flags = flags;

    if (col) {
      if (NULL == expect.pColumns) {
        expect.pColumns = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SField));
      }
      taosArrayPush(expect.pColumns, &field);
      expect.numOfColumns += 1;
    } else {
      if (NULL == expect.pTags) {
        expect.pTags = taosArrayInit(TARRAY_MIN_SIZE, sizeof(SField));
      }
      taosArrayPush(expect.pTags, &field);
      expect.numOfTags += 1;
    }
  };

  setCheckDdlFunc([&](const SQuery* pQuery, ParserStage stage) {
    ASSERT_EQ(nodeType(pQuery->pRoot), QUERY_NODE_CREATE_TABLE_STMT);
    SMCreateStbReq req = {0};
    ASSERT_TRUE(TSDB_CODE_SUCCESS == tDeserializeSMCreateStbReq(pQuery->pCmdMsg->pMsg, pQuery->pCmdMsg->msgLen, &req));

    ASSERT_EQ(std::string(req.name), std::string(expect.name));
    ASSERT_EQ(req.igExists, expect.igExists);
    ASSERT_EQ(req.xFilesFactor, expect.xFilesFactor);
    ASSERT_EQ(req.delay, expect.delay);
    ASSERT_EQ(req.ttl, expect.ttl);
    ASSERT_EQ(req.numOfColumns, expect.numOfColumns);
    ASSERT_EQ(req.numOfTags, expect.numOfTags);
    ASSERT_EQ(req.commentLen, expect.commentLen);
    ASSERT_EQ(req.ast1Len, expect.ast1Len);
    ASSERT_EQ(req.ast2Len, expect.ast2Len);

    if (expect.numOfColumns > 0) {
      ASSERT_EQ(taosArrayGetSize(req.pColumns), expect.numOfColumns);
      ASSERT_EQ(taosArrayGetSize(req.pColumns), taosArrayGetSize(expect.pColumns));
      for (int32_t i = 0; i < expect.numOfColumns; ++i) {
        SField* pField = (SField*)taosArrayGet(req.pColumns, i);
        SField* pExpectField = (SField*)taosArrayGet(expect.pColumns, i);
        ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
        ASSERT_EQ(pField->type, pExpectField->type);
        ASSERT_EQ(pField->bytes, pExpectField->bytes);
        ASSERT_EQ(pField->flags, pExpectField->flags);
      }
    }
    if (expect.numOfTags > 0) {
      ASSERT_EQ(taosArrayGetSize(req.pTags), expect.numOfTags);
      ASSERT_EQ(taosArrayGetSize(req.pTags), taosArrayGetSize(expect.pTags));
      for (int32_t i = 0; i < expect.numOfTags; ++i) {
        SField* pField = (SField*)taosArrayGet(req.pTags, i);
        SField* pExpectField = (SField*)taosArrayGet(expect.pTags, i);
        ASSERT_EQ(std::string(pField->name), std::string(pExpectField->name));
        ASSERT_EQ(pField->type, pExpectField->type);
        ASSERT_EQ(pField->bytes, pExpectField->bytes);
        ASSERT_EQ(pField->flags, pExpectField->flags);
      }
    }
    if (expect.commentLen > 0) {
      ASSERT_EQ(std::string(req.comment), std::string(expect.comment));
    }
    if (expect.ast1Len > 0) {
      ASSERT_EQ(std::string(req.pAst1), std::string(expect.pAst1));
    }
    if (expect.ast2Len > 0) {
      ASSERT_EQ(std::string(req.pAst2), std::string(expect.pAst2));
    }
  });

  setCreateStbReqFunc("t1");
  addFieldToCreateStbReqFunc(true, "ts", TSDB_DATA_TYPE_TIMESTAMP);
  addFieldToCreateStbReqFunc(true, "c1", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(false, "id", TSDB_DATA_TYPE_INT);
  run("create stable t1(ts timestamp, c1 int) TAGS(id int)");

  setCreateStbReqFunc("t1", 1, 0.1, 2, 100, "test create table");
  addFieldToCreateStbReqFunc(true, "ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0);
  addFieldToCreateStbReqFunc(true, "c1", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(true, "c2", TSDB_DATA_TYPE_UINT);
  addFieldToCreateStbReqFunc(true, "c3", TSDB_DATA_TYPE_BIGINT);
  addFieldToCreateStbReqFunc(true, "c4", TSDB_DATA_TYPE_UBIGINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c5", TSDB_DATA_TYPE_FLOAT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c6", TSDB_DATA_TYPE_DOUBLE, 0, 0);
  addFieldToCreateStbReqFunc(true, "c7", TSDB_DATA_TYPE_BINARY, 20 + VARSTR_HEADER_SIZE, 0);
  addFieldToCreateStbReqFunc(true, "c8", TSDB_DATA_TYPE_SMALLINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c9", TSDB_DATA_TYPE_USMALLINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c10", TSDB_DATA_TYPE_TINYINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c11", TSDB_DATA_TYPE_UTINYINT, 0, 0);
  addFieldToCreateStbReqFunc(true, "c12", TSDB_DATA_TYPE_BOOL, 0, 0);
  addFieldToCreateStbReqFunc(true, "c13", TSDB_DATA_TYPE_NCHAR, 30 * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE, 0);
  addFieldToCreateStbReqFunc(true, "c14", TSDB_DATA_TYPE_VARCHAR, 50 + VARSTR_HEADER_SIZE, 0);
  addFieldToCreateStbReqFunc(false, "a1", TSDB_DATA_TYPE_TIMESTAMP);
  addFieldToCreateStbReqFunc(false, "a2", TSDB_DATA_TYPE_INT);
  addFieldToCreateStbReqFunc(false, "a3", TSDB_DATA_TYPE_UINT);
  addFieldToCreateStbReqFunc(false, "a4", TSDB_DATA_TYPE_BIGINT);
  addFieldToCreateStbReqFunc(false, "a5", TSDB_DATA_TYPE_UBIGINT);
  addFieldToCreateStbReqFunc(false, "a6", TSDB_DATA_TYPE_FLOAT);
  addFieldToCreateStbReqFunc(false, "a7", TSDB_DATA_TYPE_DOUBLE);
  addFieldToCreateStbReqFunc(false, "a8", TSDB_DATA_TYPE_BINARY, 20 + VARSTR_HEADER_SIZE);
  addFieldToCreateStbReqFunc(false, "a9", TSDB_DATA_TYPE_SMALLINT);
  addFieldToCreateStbReqFunc(false, "a10", TSDB_DATA_TYPE_USMALLINT);
  addFieldToCreateStbReqFunc(false, "a11", TSDB_DATA_TYPE_TINYINT);
  addFieldToCreateStbReqFunc(false, "a12", TSDB_DATA_TYPE_UTINYINT);
  addFieldToCreateStbReqFunc(false, "a13", TSDB_DATA_TYPE_BOOL);
  addFieldToCreateStbReqFunc(false, "a14", TSDB_DATA_TYPE_NCHAR, 30 * TSDB_NCHAR_SIZE + VARSTR_HEADER_SIZE);
  addFieldToCreateStbReqFunc(false, "a15", TSDB_DATA_TYPE_VARCHAR, 50 + VARSTR_HEADER_SIZE);
  run("create stable if not exists test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), "
      "c8 SMALLINT, c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, "
      "c13 NCHAR(30), c14 VARCHAR(50)) "
      "TAGS (a1 TIMESTAMP, a2 INT, a3 INT UNSIGNED, a4 BIGINT, a5 BIGINT UNSIGNED, a6 FLOAT, a7 DOUBLE, "
      "a8 BINARY(20), a9 SMALLINT, a10 SMALLINT UNSIGNED COMMENT 'test column comment', a11 TINYINT, "
      "a12 TINYINT UNSIGNED, a13 BOOL, a14 NCHAR(30), a15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3) ROLLUP (min) FILE_FACTOR 0.1 DELAY 2");
}

TEST_F(ParserInitialCTest, createStream) {
  useDb("root", "test");

  run("create stream s1 as select * from t1");

  run("create stream if not exists s1 as select * from t1");

  run("create stream s1 into st1 as select * from t1");

  run("create stream if not exists s1 trigger window_close watermark 10s into st1 as select * from t1");
}

TEST_F(ParserInitialCTest, createTable) {
  useDb("root", "test");

  run("create table t1(ts timestamp, c1 int)");

  run("create table if not exists test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), c8 "
      "SMALLINT, "
      "c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, c13 "
      "NCHAR(30), "
      "c15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create table' SMA(c1, c2, c3)");

  run("create table if not exists test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), c8 "
      "SMALLINT, "
      "c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, c13 "
      "NCHAR(30), "
      "c15 VARCHAR(50)) "
      "TAGS (tsa TIMESTAMP, a1 INT, a2 INT UNSIGNED, a3 BIGINT, a4 BIGINT UNSIGNED, "
      "a5 FLOAT, a6 DOUBLE, a7 "
      "BINARY(20), a8 SMALLINT, "
      "a9 SMALLINT UNSIGNED COMMENT 'test column comment', a10 "
      "TINYINT, a11 TINYINT UNSIGNED, a12 BOOL, a13 NCHAR(30), "
      "a15 VARCHAR(50)) "
      "TTL 100 COMMENT 'test create "
      "table' SMA(c1, c2, c3) ROLLUP (min) FILE_FACTOR 0.1 DELAY 2");

  run("create table if not exists t1 using st1 tags(1, 'wxy')");

  run("create table "
      "if not exists test.t1 using test.st1 (tag1, tag2) tags(1, 'abc') "
      "if not exists test.t2 using test.st1 (tag1, tag2) tags(2, 'abc') "
      "if not exists test.t3 using test.st1 (tag1, tag2) tags(3, 'abc') ");
}

TEST_F(ParserInitialCTest, createTopic) {
  useDb("root", "test");

  run("create topic tp1 as select * from t1");

  run("create topic if not exists tp1 as select * from t1");

  run("create topic tp1 as test");

  run("create topic if not exists tp1 as test");
}

TEST_F(ParserInitialCTest, createUser) {
  useDb("root", "test");

  run("create user wxy pass '123456'");
}

}  // namespace ParserTest
