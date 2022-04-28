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

class ParserInitialATest : public ParserTestBase {};

TEST_F(ParserInitialATest, alterAccount) {
  useDb("root", "test");

  run("alter account ac_wxy pass '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT);
}

TEST_F(ParserInitialATest, alterDnode) {
  useDb("root", "test");

  run("alter dnode 1 'resetLog'");

  run("alter dnode 1 'debugFlag' '134'");
}

TEST_F(ParserInitialATest, alterDatabase) {
  useDb("root", "test");

  run("alter database wxy_db cachelast 1 fsync 200 keep 1440 wal 1");
}

// todo alter local
// todo alter stable
// todo alter table

TEST_F(ParserInitialATest, alterUser) {
  useDb("root", "test");

  run("alter user wxy pass '123456'");

  run("alter user wxy privilege 'write'");
}

}  // namespace ParserTest