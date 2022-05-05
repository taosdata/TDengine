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

class ParserInitialCTest : public ParserTestBase {};

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

  run("create stable t1(ts timestamp, c1 int) TAGS(id int)");

  run("create stable if not exists test.t1("
      "ts TIMESTAMP, c1 INT, c2 INT UNSIGNED, c3 BIGINT, c4 BIGINT UNSIGNED, c5 FLOAT, c6 DOUBLE, c7 BINARY(20), c8 "
      "SMALLINT, "
      "c9 SMALLINT UNSIGNED COMMENT 'test column comment', c10 TINYINT, c11 TINYINT UNSIGNED, c12 BOOL, c13 NCHAR(30), "
      "c15 VARCHAR(50)) "
      "TAGS (tsa TIMESTAMP, a1 INT, a2 INT UNSIGNED, a3 BIGINT, a4 BIGINT UNSIGNED, a5 FLOAT, a6 DOUBLE, a7 "
      "BINARY(20), a8 SMALLINT, "
      "a9 SMALLINT UNSIGNED COMMENT 'test column comment', a10 TINYINT, a11 TINYINT UNSIGNED, a12 BOOL, a13 NCHAR(30), "
      "a15 VARCHAR(50)) "
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
