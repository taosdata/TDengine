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

class ParserInitialDTest : public ParserTestBase {};

// todo delete
// todo desc
// todo describe
// todo drop account

TEST_F(ParserInitialDTest, dropBnode) {
  useDb("root", "test");

  run("drop bnode on dnode 1");
}

// todo drop database
// todo drop dnode
// todo drop function

TEST_F(ParserInitialDTest, dropIndex) {
  useDb("root", "test");

  run("drop index index1 on t1");
}

TEST_F(ParserInitialDTest, dropMnode) {
  useDb("root", "test");

  run("drop mnode on dnode 1");
}

TEST_F(ParserInitialDTest, dropQnode) {
  useDb("root", "test");

  run("drop qnode on dnode 1");
}

TEST_F(ParserInitialDTest, dropSnode) {
  useDb("root", "test");

  run("drop snode on dnode 1");
}

// todo drop stable
// todo drop stream
// todo drop table

TEST_F(ParserInitialDTest, dropTopic) {
  useDb("root", "test");

  run("drop topic tp1");

  run("drop topic if exists tp1");
}

TEST_F(ParserInitialDTest, dropUser) {
  useDb("root", "test");

  run("drop user wxy");
}

}  // namespace ParserTest
