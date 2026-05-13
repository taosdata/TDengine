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

#include "gtest/gtest.h"
#include <iostream>
#include "taoserror.h"
#include "thash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "taos.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}

TEST(clientBICase, select_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(pConn, nullptr);

  taos_set_conn_mode(pConn, TAOS_CONN_MODE_BI, 1);

  TAOS_RES* pRes = taos_query(pConn, "drop database if exists bi_test");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database bi_test");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use bi_test");
  while(taos_errno(pRes) == TSDB_CODE_MND_DB_IN_CREATING || taos_errno(pRes) == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    pRes = taos_query(pConn, "use bi_test");
  }
  taos_free_result(pRes);

  // create super table and sub table
  pRes = taos_query(pConn, "create table super_t (ts timestamp, flag int) tags (t1 VARCHAR(10))");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table sub_t0 using super_t tags('t1')");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  // create super virtual table and sub virtual table
  pRes = taos_query(pConn, "create stable v_super_t (ts timestamp, flag int) tags (t1 VARCHAR(10)) virtual 1");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create vtable v_sub_t0 (sub_t0.flag) using v_super_t TAGS (1)");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  // create normal virtual table
  pRes = taos_query(pConn, "CREATE VTABLE v_normal_t (ts timestamp,flag int from sub_t0.flag)");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  taos_free_result(pRes);

  // insert some data to sub table
  for(int i = 0; i < 10; i++) {
    char str[512] = {0};
    sprintf(str, "insert into sub_t0 values(now+%da, %d)", i, i);
    pRes = taos_query(pConn, str);
    ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
    taos_free_result(pRes);
  }

  // select data use virtual table
  pRes = taos_query(pConn, "select * from v_sub_t0");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);

  TAOS_ROW  pRow = NULL;
  int  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 10);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select first(*) from v_sub_t0");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 1);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select last(*) from v_sub_t0");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 1);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select last_row(*) from v_sub_t0");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 1);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select * from v_normal_t");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 10);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select first(*) from v_normal_t");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 1);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select last(*) from v_normal_t");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 1);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select last_row(*) from v_normal_t");
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  pRow = NULL;
  rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, 1);
  taos_free_result(pRes);

  taos_close(pConn);
}
#pragma GCC diagnostic pop
