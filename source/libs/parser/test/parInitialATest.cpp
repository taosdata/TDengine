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

  run("ALTER ACCOUNT ac_wxy PASS '123456'", TSDB_CODE_PAR_EXPRIE_STATEMENT);
}

TEST_F(ParserInitialATest, alterDnode) {
  useDb("root", "test");

  run("ALTER DNODE 1 'resetLog'");

  run("ALTER DNODE 1 'debugFlag' '134'");
}

TEST_F(ParserInitialATest, alterDatabase) {
  useDb("root", "test");

  run("ALTER DATABASE wxy_db CACHELAST 1 FSYNC 200 WAL 1");
}

// todo ALTER local
// todo ALTER stable

/*
 * ALTER TABLE [db_name.]tb_name alter_table_clause
 *
 * alter_table_clause: {
 *     alter_table_options
 *   | ADD COLUMN col_name column_type
 *   | DROP COLUMN col_name
 *   | MODIFY COLUMN col_name column_type
 *   | RENAME COLUMN old_col_name new_col_name
 *   | ADD TAG tag_name tag_type
 *   | DROP TAG tag_name
 *   | MODIFY TAG tag_name tag_type
 *   | RENAME TAG old_tag_name new_tag_name
 *   | SET TAG tag_name = new_tag_value
 *   | ADD {FULLTEXT | SMA} INDEX index_name (col_name [, col_name] ...) [index_option]
 * }
 *
 * alter_table_options:
 *     alter_table_option ...
 *
 * alter_table_option: {
 *     TTL value
 *   | COMMENT 'string_value'
 * }
 */
TEST_F(ParserInitialATest, alterTable) {
  useDb("root", "test");

  // run("ALTER TABLE t1 TTL 10");
  // run("ALTER TABLE t1 COMMENT 'test'");
  run("ALTER TABLE t1 ADD COLUMN cc1 BIGINT");
  run("ALTER TABLE t1 DROP COLUMN c1");
  run("ALTER TABLE t1 MODIFY COLUMN c1 VARCHAR(20)");
  run("ALTER TABLE t1 RENAME COLUMN c1 cc1");

  run("ALTER TABLE st1 ADD TAG tag11 BIGINT");
  run("ALTER TABLE st1 DROP TAG tag1");
  run("ALTER TABLE st1 MODIFY TAG tag1 VARCHAR(20)");
  run("ALTER TABLE st1 RENAME TAG tag1 tag11");

  // run("ALTER TABLE st1s1 SET TAG tag1=10");

  // todo
  // ADD {FULLTEXT | SMA} INDEX index_name (col_name [, col_name] ...) [index_option]
}

TEST_F(ParserInitialATest, alterUser) {
  useDb("root", "test");

  run("ALTER user wxy PASS '123456'");

  run("ALTER user wxy privilege 'write'");
}

TEST_F(ParserInitialATest, bug001) {
  useDb("root", "test");

  run("ALTER DATABASE db WAL 0     # td-14436", TSDB_CODE_PAR_SYNTAX_ERROR);
}

}  // namespace ParserTest