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
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "taos.h"

class taoscTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    printf("start test setup.\n");
    TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
    ASSERT_TRUE(taos != nullptr);

    TAOS_RES* res = taos_query(taos, "drop database IF EXISTS taosc_test_db;");
    if (taos_errno(res) != 0) {
      printf("error in drop database taosc_test_db, reason:%s\n", taos_errstr(res));
      return;
    }
    taosSsleep(5);
    taosSsleep(3);
    taos_free_result(res);
    printf("drop database taosc_test_db,finished.\n");

    res = taos_query(taos, "create database taosc_test_db;");
    if (taos_errno(res) != 0) {
      printf("error in create database taosc_test_db, reason:%s\n", taos_errstr(res));
      return;
    }
    taosSsleep(5);
    taos_free_result(res);
    printf("create database taosc_test_db,finished.\n");

    taos_close(taos);
  }

  static void TearDownTestCase() {}

  void SetUp() override {}

  void TearDown() override {}
};

tsem_t query_sem;
int    getRecordCounts = 0;

void fetchCallback(void* param, void* res, int32_t numOfRow) {
  ASSERT_TRUE(numOfRow >= 0);
  if (numOfRow == 0) {
    printf("completed\n");
    taos_free_result(res);
    tsem_post(&query_sem);
    return;
  }

  printf("numOfRow = %d \n", numOfRow);
  int         numFields = taos_num_fields(res);
  TAOS_FIELD* fields = taos_fetch_fields(res);

  for (int i = 0; i < numOfRow; ++i) {
    TAOS_ROW row = taos_fetch_row(res);
    char     temp[256] = {0};
    taos_print_row(temp, row, fields, numFields);
    // printf("%s\n", temp);
  }

  getRecordCounts += numOfRow;
  taos_fetch_raw_block_a(res, fetchCallback, param);
}

void queryCallback(void* param, void* res, int32_t code) {
  ASSERT_TRUE(code == 0);
  ASSERT_TRUE(param == NULL);
  taos_fetch_raw_block_a(res, fetchCallback, param);
}

TEST_F(taoscTest, Connect) {
  char    sql[1024] = {0};
  int32_t code = 0;
  TAOS*   taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != nullptr);

  TAOS_RES* res = taos_query(taos, "create table taosc_test_db.taosc_test_table (ts TIMESTAMP, val INT)");
  if (taos_errno(res) != 0) {
    printf("error in table database taosc_test_table, reason:%s\n", taos_errstr(res));
  }
  ASSERT_TRUE(taos_errno(res) == 0);
  taos_free_result(res);
  taosSsleep(2);

  int insertCounts = 10000;
  for (int i = 0; i < insertCounts; i++) {
    char sql[128];
    sprintf(sql, "insert into taosc_test_db.taosc_test_table values(now() + %ds, %d)", i, i);
    res = taos_query(taos, sql);
    ASSERT_TRUE(taos_errno(res) == 0);
    taos_free_result(res);
  }

  tsem_init(&query_sem, 0, 0);
  taos_query_a(taos, "select * from taosc_test_db.taosc_test_table;", queryCallback, NULL);
  tsem_wait(&query_sem);

  ASSERT_EQ(getRecordCounts, insertCounts);
  taos_close(taos);

  printf("Connect test finished.\n");
}
