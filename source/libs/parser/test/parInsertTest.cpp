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

#include <gtest/gtest.h>

#include "parTestUtil.h"

using namespace std;

namespace ParserTest {

// syntax:
// INSERT INTO
//   tb_name
//       [USING stb_name [(tag1_name, ...)] TAGS (tag1_value, ...)]
//       [(field1_name, ...)]
//       VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
//   [...];
class ParserInsertTest : public ParserTestBase {};

// INSERT INTO tb_name [(field1_name, ...)] VALUES (field1_value, ...)
TEST_F(ParserInsertTest, singleTableSingleRowTest) {
  useDb("root", "test");

  run("INSERT INTO t1 VALUES (now, 1, 'beijing', 3, 4, 5)");

  run("INSERT INTO t1 (ts, c1, c2, c3, c4, c5) VALUES (now, 1, 'beijing', 3, 4, 5)");
}

// INSERT INTO tb_name VALUES (field1_value, ...)(field1_value, ...)
TEST_F(ParserInsertTest, singleTableMultiRowTest) {
  useDb("root", "test");

  run("INSERT INTO t1 VALUES (now, 1, 'beijing', 3, 4, 5)"
      "(now+1s, 2, 'shanghai', 6, 7, 8)"
      "(now+2s, 3, 'guangzhou', 9, 10, 11)");
}

// INSERT INTO tb1_name VALUES (field1_value, ...) tb2_name VALUES (field1_value, ...)
TEST_F(ParserInsertTest, multiTableSingleRowTest) {
  useDb("root", "test");

  run("INSERT INTO st1s1 VALUES (now, 1, 'beijing') st1s2 VALUES (now, 10, '131028')");
}

// INSERT INTO tb1_name VALUES (field1_value, ...) tb2_name VALUES (field1_value, ...)
TEST_F(ParserInsertTest, multiTableMultiRowTest) {
  useDb("root", "test");

  run("INSERT INTO "
      "st1s1 VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou') "
      "st1s2 VALUES (now, 10, '131028')(now+1s, 20, '132028')");
}

// INSERT INTO
//    tb1_name USING st1_name [(tag1_name, ...)] TAGS (tag1_value, ...) VALUES (field1_value, ...)
//    tb2_name USING st2_name [(tag1_name, ...)] TAGS (tag1_value, ...) VALUES (field1_value, ...)
TEST_F(ParserInsertTest, autoCreateTableTest) {
  useDb("root", "test");

  run("INSERT INTO st1s1 USING st1 TAGS(1, 'wxy', now) "
      "VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou')");

  run("INSERT INTO st1s1 USING st1 (tag1, tag2) TAGS(1, 'wxy') (ts, c1, c2) "
      "VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou')");

  run("INSERT INTO st1s1 (ts, c1, c2) USING st1 (tag1, tag2) TAGS(1, 'wxy') "
      "VALUES (now, 1, 'beijing')(now+1s, 2, 'shanghai')(now+2s, 3, 'guangzhou')");

  run("INSERT INTO "
      "st1s1 USING st1 (tag1, tag2) TAGS(1, 'wxy') (ts, c1, c2) VALUES (now, 1, 'beijing') "
      "st1s2 (ts, c1, c2) USING st1 TAGS(2, 'abc', now) VALUES (now+1s, 2, 'shanghai')");
}

}  // namespace ParserTest
