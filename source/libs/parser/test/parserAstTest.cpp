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

#include <algorithm>
#include <string>

#include <gtest/gtest.h>

#include "parInt.h"

using namespace std;
using namespace testing;

class ParserTest : public Test {
protected:
  void setDatabase(const string& acctId, const string& db) {
    acctId_ = acctId;
    db_ = db;
  }

  void bind(const char* sql) {
    reset();
    cxt_.acctId = atoi(acctId_.c_str());
    cxt_.db = db_.c_str();
    sqlBuf_ = string(sql);
    transform(sqlBuf_.begin(), sqlBuf_.end(), sqlBuf_.begin(), ::tolower);
    cxt_.sqlLen = strlen(sql);
    cxt_.pSql = sqlBuf_.c_str();
  }

  bool run(int32_t parseCode = TSDB_CODE_SUCCESS, int32_t translateCode = TSDB_CODE_SUCCESS) {
    query_ = nullptr;
    bool res = runImpl(parseCode, translateCode);
    qDestroyQuery(query_);
    return res;
  }

private:
  static const int max_err_len = 1024;

  bool runImpl(int32_t parseCode, int32_t translateCode) {
    int32_t code = doParse(&cxt_, &query_);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] parser code:" << tstrerror(code) << ", msg:" << errMagBuf_ << endl;
      return (TSDB_CODE_SUCCESS != parseCode);
    }
    if (TSDB_CODE_SUCCESS != parseCode) {
      return false;
    }
    string parserStr = toString(query_->pRoot);
    code = doTranslate(&cxt_, query_);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] translate code:" << code << ", " << translateCode << ", msg:" << errMagBuf_ << endl;
      return (code == translateCode);
    }
    cout << "input sql : [" << cxt_.pSql << "]" << endl;
    cout << "parser output: " << endl;
    cout << parserStr << endl;
    cout << "translate output: " << endl;
    cout << toString(query_->pRoot) << endl;
    return (TSDB_CODE_SUCCESS == translateCode);
  }

  string toString(const SNode* pRoot, bool format = false) {
    char* pStr = NULL;
    int32_t len = 0;
    int32_t code = nodesNodeToString(pRoot, format, &pStr, &len);
    if (code != TSDB_CODE_SUCCESS) {
      cout << "sql:[" << cxt_.pSql << "] toString code:" << code << ", strerror:" << tstrerror(code) << endl;
      throw "nodesNodeToString failed!";
    }
    string str(pStr);
    tfree(pStr);
    return str;
  }

  void reset() {
    memset(&cxt_, 0, sizeof(cxt_));
    memset(errMagBuf_, 0, max_err_len);
    cxt_.pMsg = errMagBuf_;
    cxt_.msgLen = max_err_len;
  }

  string acctId_;
  string db_;
  char errMagBuf_[max_err_len];
  string sqlBuf_;
  SParseContext cxt_;
  SQuery* query_;
};

TEST_F(ParserTest, selectSimple) {
  setDatabase("root", "test");

  bind("SELECT * FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT * FROM test.t1");
  ASSERT_TRUE(run());

  bind("SELECT ts, c1 FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT ts, t.c1 FROM (SELECT * FROM t1) t");
  ASSERT_TRUE(run());

  bind("SELECT * FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, selectConstant) {
  setDatabase("root", "test");

  bind("SELECT 123, 20.4, 'abc', \"wxy\", TIMESTAMP '2022-02-09 17:30:20', true, false, 10s FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT 1234567890123456789012345678901234567890, 20.1234567890123456789012345678901234567890, 'abc', \"wxy\", TIMESTAMP '2022-02-09 17:30:20', true, false, 15s FROM t1");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, selectExpression) {
  setDatabase("root", "test");

  bind("SELECT ts + 10s, c1 + 10, concat(c2, 'abc') FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT ts > 0, c1 < 20 AND c2 = 'qaz' FROM t1");
  ASSERT_TRUE(run());

  bind("SELECT ts > 0, c1 BETWEEN 10 AND 20 AND c2 = 'qaz' FROM t1");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, selectClause) {
  setDatabase("root", "test");

  // GROUP BY clause
  bind("SELECT count(*) cnt FROM t1 WHERE c1 > 0");
  ASSERT_TRUE(run());

  bind("SELECT count(*), c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c2");
  ASSERT_TRUE(run());

  bind("SELECT count(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 HAVING count(c1) > 10");
  ASSERT_TRUE(run());

  bind("SELECT count(*), c1, c2 + 10, c1 + c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c2, c1");
  ASSERT_TRUE(run());

  bind("SELECT count(*), c1 + 10, c2 cnt FROM t1 WHERE c1 > 0 GROUP BY c1 + 10, c2");
  ASSERT_TRUE(run());

  // ORDER BY clause
  bind("SELECT count(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 ORDER BY cnt");
  ASSERT_TRUE(run());

  bind("SELECT count(*) cnt FROM t1 WHERE c1 > 0 GROUP BY c2 ORDER BY 1");
  ASSERT_TRUE(run());

  // DISTINCT clause
  bind("SELECT DISTINCT c1, c2 FROM t1 WHERE c1 > 0 ORDER BY c1");
  ASSERT_TRUE(run());

  bind("SELECT DISTINCT c1 + 10, c2 FROM t1 WHERE c1 > 0 ORDER BY c1 + 10, c2");
  ASSERT_TRUE(run());

  bind("SELECT DISTINCT c1 + 10 cc1, c2 cc2 FROM t1 WHERE c1 > 0 ORDER BY cc1, c2");
  ASSERT_TRUE(run());

  bind("SELECT DISTINCT count(c2) FROM t1 WHERE c1 > 0 GROUP BY c1 ORDER BY count(c2)");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, selectWindow) {
  setDatabase("root", "test");

  bind("SELECT count(*) FROM t1 interval(10s)");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, selectSyntaxError) {
  setDatabase("root", "test");

  bind("SELECTT * FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));

  bind("SELECT *");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));

  bind("SELECT *, * FROM test.t1");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));

  bind("SELECT * FROM test.t1 t WHER");
  ASSERT_TRUE(run(TSDB_CODE_FAILED));
}

TEST_F(ParserTest, selectSemanticError) {
  setDatabase("root", "test");

  // TSDB_CODE_PAR_INVALID_COLUMN
  bind("SELECT c1, cc1 FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_INVALID_COLUMN));

  bind("SELECT t1.c1, t1.cc1 FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_INVALID_COLUMN));

  // TSDB_CODE_PAR_TABLE_NOT_EXIST
  bind("SELECT * FROM t10");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_TABLE_NOT_EXIST));

  bind("SELECT * FROM test.t10");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_TABLE_NOT_EXIST));

  bind("SELECT t2.c1 FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_TABLE_NOT_EXIST));

  // TSDB_CODE_PAR_AMBIGUOUS_COLUMN
  bind("SELECT c2 FROM t1 tt1, t1 tt2 WHERE tt1.c1 = tt2.c1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_AMBIGUOUS_COLUMN));

  // TSDB_CODE_PAR_WRONG_VALUE_TYPE
  bind("SELECT 10n FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_WRONG_VALUE_TYPE));

  bind("SELECT TIMESTAMP '2010' FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_WRONG_VALUE_TYPE));

  // TSDB_CODE_PAR_INVALID_FUNTION
  bind("SELECT cnt(*) FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_INVALID_FUNTION));

  // TSDB_CODE_PAR_FUNTION_PARA_NUM
  // TSDB_CODE_PAR_FUNTION_PARA_TYPE

  // TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION
  bind("SELECT c2 FROM t1 tt1 JOIN t1 tt2 ON count(*) > 0");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION));

  bind("SELECT c2 FROM t1 where count(*) > 0");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION));

  bind("SELECT c2 FROM t1 GROUP BY count(*)");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_ILLEGAL_USE_AGG_FUNCTION));

  // TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT
  bind("SELECT c2 FROM t1 ORDER BY 0");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT));

  bind("SELECT c2 FROM t1 ORDER BY 2");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_WRONG_NUMBER_OF_SELECT));

  // TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION
  bind("SELECT count(*) cnt FROM t1 HAVING c1 > 0");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION));

  bind("SELECT count(*) cnt FROM t1 GROUP BY c2 HAVING c1 > 0");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION));

  bind("SELECT count(*), c1 cnt FROM t1 GROUP BY c2 HAVING c2 > 0");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION));

  bind("SELECT count(*) cnt FROM t1 GROUP BY c2 HAVING c2 > 0 ORDER BY c1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_GROUPBY_LACK_EXPRESSION));

  // TSDB_CODE_PAR_NOT_SINGLE_GROUP
  bind("SELECT count(*), c1 FROM t1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_NOT_SINGLE_GROUP));

  bind("SELECT count(*) FROM t1 ORDER BY c1");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_NOT_SINGLE_GROUP));

  bind("SELECT c1 FROM t1 ORDER BY count(*)");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_NOT_SINGLE_GROUP));

  // TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION
  bind("SELECT DISTINCT c1, c2 FROM t1 WHERE c1 > 0 ORDER BY ts");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION));

  bind("SELECT DISTINCT c1 FROM t1 WHERE c1 > 0 ORDER BY count(c2)");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION));

  bind("SELECT DISTINCT c2 FROM t1 WHERE c1 > 0 ORDER BY count(c2)");
  ASSERT_TRUE(run(TSDB_CODE_SUCCESS, TSDB_CODE_PAR_NOT_SELECTED_EXPRESSION));
}

TEST_F(ParserTest, createUser) {
  setDatabase("root", "test");

  bind("create user wxy pass '123456'");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, createDnode) {
  setDatabase("root", "test");

  bind("create dnode abc1 port 7000");
  ASSERT_TRUE(run());

  bind("create dnode 1.1.1.1 port 9000");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, createDatabase) {
  setDatabase("root", "test");

  bind("create database wxy_db");
  ASSERT_TRUE(run());

  bind("create database if not exists wxy_db "
       "BLOCKS 100 "
       "CACHE 100 "
       "CACHELAST 2 "
       "COMP 1 "
       "DAYS 100 "
       "FSYNC 100 "
       "MAXROWS 1000 "
       "MINROWS 100 "
       "KEEP 100 "
       "PRECISION 'ms' "
       "QUORUM 1 "
       "REPLICA 3 "
       "TTL 100 "
       "WAL 2 "
       "VGROUPS 100 "
       "SINGLE_STABLE 0 "
       "STREAM_MODE 1 "
      );
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, showDatabase) {
  setDatabase("root", "test");

  bind("show databases");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, useDatabase) {
  setDatabase("root", "test");

  bind("use wxy_db");
  ASSERT_TRUE(run());
}

TEST_F(ParserTest, createTable) {
  setDatabase("root", "test");

  bind("create table t1(ts timestamp, c1 int)");
  ASSERT_TRUE(run());

  bind("create table if not exists test.t1("
       "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), c8 SMALLINT, "
       "c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, c13 NCHAR(30), c14 JSON, c15 VARCHAR(50)) "
       "KEEP 100 TTL 100 COMMENT 'test create table' SMA(c1, c2, c3)"
      );
  ASSERT_TRUE(run());
  
  bind("create table if not exists test.t1("
       "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), c8 SMALLINT, "
       "c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, c13 NCHAR(30), c14 JSON, c15 VARCHAR(50)) "
       "TAGS (tsa TIMESTAMP, a1 INT, a2 INT UNSIGNED, a3 BIGINT, a4 BIGINT UNSIGNED, a5 FLOAT, a6 DOUBLE, a7 BINARY(20), a8 SMALLINT, "
       "a9 SMALLINT UNSIGNED COMMENT 'test column comment', a10 TINYINT, a11 TINYINT UNSIGNED, a12 BOOL, a13 NCHAR(30), a14 JSON, a15 VARCHAR(50)) "
       "KEEP 100 TTL 100 COMMENT 'test create table' SMA(c1, c2, c3)"
      );
  ASSERT_TRUE(run());
  
  bind("create table if not exists t1 using st1 tags(1, 'wxy')");
  ASSERT_TRUE(run());

  bind("create table "
       "if not exists test.t1 using test.st1 (tag1, tag2) tags(1, 'abc') "
       "if not exists test.t2 using test.st1 (tag1, tag2) tags(2, 'abc') "
       "if not exists test.t3 using test.st1 (tag1, tag2) tags(3, 'abc')"
      );
  ASSERT_TRUE(run());

  bind("create stable t1(ts timestamp, c1 int) TAGS(id int)");
  ASSERT_TRUE(run());

  bind("create stable if not exists test.t1("
       "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), c8 SMALLINT, "
       "c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, c13 NCHAR(30), c14 JSON, c15 VARCHAR(50)) "
       "TAGS (tsa TIMESTAMP, a1 INT, a2 INT UNSIGNED, a3 BIGINT, a4 BIGINT UNSIGNED, a5 FLOAT, a6 DOUBLE, a7 BINARY(20), a8 SMALLINT, "
       "a9 SMALLINT UNSIGNED COMMENT 'test column comment', a10 TINYINT, a11 TINYINT UNSIGNED, a12 BOOL, a13 NCHAR(30), a14 JSON, a15 VARCHAR(50)) "
       "KEEP 100 TTL 100 COMMENT 'test create table' SMA(c1, c2, c3)"
      );
  ASSERT_TRUE(run());
}
