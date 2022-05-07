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

class ParserShowToUseTest : public ParserTestBase {};

// todo SHOW accounts
// todo SHOW apps
// todo SHOW connections
// todo SHOW create database
// todo SHOW create stable
// todo SHOW create table

TEST_F(ParserShowToUseTest, showDatabases) {
  useDb("root", "test");

  run("SHOW databases");
}

TEST_F(ParserShowToUseTest, showDnodes) {
  useDb("root", "test");

  run("SHOW dnodes");
}

TEST_F(ParserShowToUseTest, showFunctions) {
  useDb("root", "test");

  run("SHOW functions");
}

// todo SHOW licence

TEST_F(ParserShowToUseTest, showIndexes) {
  useDb("root", "test");

  run("SHOW indexes from t1");

  run("SHOW indexes from t1 from test");
}

TEST_F(ParserShowToUseTest, showMnodes) {
  useDb("root", "test");

  run("SHOW mnodes");
}

TEST_F(ParserShowToUseTest, showModules) {
  useDb("root", "test");

  run("SHOW modules");
}

TEST_F(ParserShowToUseTest, showQnodes) {
  useDb("root", "test");

  run("SHOW qnodes");
}

// todo SHOW queries
// todo SHOW scores

TEST_F(ParserShowToUseTest, showStables) {
  useDb("root", "test");

  run("SHOW stables");

  run("SHOW test.stables");

  run("SHOW stables like 'c%'");

  run("SHOW test.stables like 'c%'");
}

TEST_F(ParserShowToUseTest, showStreams) {
  useDb("root", "test");

  run("SHOW streams");
}

TEST_F(ParserShowToUseTest, showTransactions) {
  useDb("root", "test");

  run("SHOW TRANSACTIONS");
}

TEST_F(ParserShowToUseTest, showTables) {
  useDb("root", "test");

  run("SHOW tables");

  run("SHOW test.tables");

  run("SHOW tables like 'c%'");

  run("SHOW test.tables like 'c%'");
}

// todo SHOW topics

TEST_F(ParserShowToUseTest, showUsers) {
  useDb("root", "test");

  run("SHOW users");
}

// todo SHOW variables

TEST_F(ParserShowToUseTest, showVgroups) {
  useDb("root", "test");

  run("SHOW vgroups");

  run("SHOW test.vgroups");
}

// todo SHOW vnodes

// todo split vgroup

TEST_F(ParserShowToUseTest, useDatabase) {
  useDb("root", "test");

  run("use wxy_db");
}

}  // namespace ParserTest
