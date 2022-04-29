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

// todo show accounts
// todo show apps
// todo show connections
// todo show create database
// todo show create stable
// todo show create table

TEST_F(ParserShowToUseTest, showDatabases) {
  useDb("root", "test");

  run("show databases");
}

TEST_F(ParserShowToUseTest, showDnodes) {
  useDb("root", "test");

  run("show dnodes");
}

TEST_F(ParserShowToUseTest, showFunctions) {
  useDb("root", "test");

  run("show functions");
}

// todo show licence

TEST_F(ParserShowToUseTest, showIndexes) {
  useDb("root", "test");

  run("show indexes from t1");

  run("show indexes from t1 from test");
}

TEST_F(ParserShowToUseTest, showMnodes) {
  useDb("root", "test");

  run("show mnodes");
}

TEST_F(ParserShowToUseTest, showModules) {
  useDb("root", "test");

  run("show modules");
}

TEST_F(ParserShowToUseTest, showQnodes) {
  useDb("root", "test");

  run("show qnodes");
}

// todo show queries
// todo show scores

TEST_F(ParserShowToUseTest, showStables) {
  useDb("root", "test");

  run("show stables");

  run("show test.stables");

  run("show stables like 'c%'");

  run("show test.stables like 'c%'");
}

TEST_F(ParserShowToUseTest, showStreams) {
  useDb("root", "test");

  run("show streams");
}

TEST_F(ParserShowToUseTest, showTables) {
  useDb("root", "test");

  run("show tables");

  run("show test.tables");

  run("show tables like 'c%'");

  run("show test.tables like 'c%'");
}

// todo show topics

TEST_F(ParserShowToUseTest, showUsers) {
  useDb("root", "test");

  run("show users");
}

// todo show variables

TEST_F(ParserShowToUseTest, showVgroups) {
  useDb("root", "test");

  run("show vgroups");

  run("show test.vgroups");
}

// todo show vnodes

// todo split vgroup

TEST_F(ParserShowToUseTest, useDatabase) {
  useDb("root", "test");

  run("use wxy_db");
}

}  // namespace ParserTest
