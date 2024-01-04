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
int    insertCounts = 100;
void*  pUserParam = NULL;

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
  ASSERT_TRUE(param == pUserParam);
  taos_fetch_raw_block_a(res, fetchCallback, param);
}

TEST_F(taoscTest, taos_query_a_test) {
  char    sql[1024] = {0};
  int32_t code = 0;
  TAOS*   taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != nullptr);

  const char* table_name = "taosc_test_table1";
  sprintf(sql, "create table taosc_test_db.%s (ts TIMESTAMP, val INT)", table_name);
  TAOS_RES* res = taos_query(taos, sql);
  if (taos_errno(res) != 0) {
    printf("error in table database %s, reason:%s\n", table_name, taos_errstr(res));
  }
  ASSERT_TRUE(taos_errno(res) == 0);
  taos_free_result(res);
  taosSsleep(2);

  for (int i = 0; i < insertCounts; i++) {
    char sql[128];
    sprintf(sql, "insert into taosc_test_db.%s values(now() + %ds, %d)", table_name, i, i);
    res = taos_query(taos, sql);
    ASSERT_TRUE(taos_errno(res) == 0);
    taos_free_result(res);
  }

  pUserParam = NULL;
  void* tmpParam = pUserParam;
  tsem_init(&query_sem, 0, 0);
  sprintf(sql, "select * from taosc_test_db.%s;", table_name);
  taos_query_a(taos, sql, queryCallback, pUserParam);
  tsem_wait(&query_sem);
  ASSERT_TRUE(pUserParam == tmpParam);

  ASSERT_EQ(getRecordCounts, insertCounts);
  taos_close(taos);

  printf("taos_query_a test finished.\n");
}

TEST_F(taoscTest, taos_query_a_param_test) {
  char    sql[1024] = {0};
  int32_t code = 0;
  TAOS*   taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != nullptr);

  int pArray[2] = {0, 1};
  pUserParam = NULL;
  void* tmpParam = pUserParam;
  getRecordCounts = 0;
  const char* table_name = "taosc_test_table1";
  tsem_init(&query_sem, 0, 0);
  sprintf(sql, "select * from taosc_test_db.%s;", table_name);
  taos_query_a(taos, sql, queryCallback, pUserParam);
  tsem_wait(&query_sem);
  ASSERT_TRUE(pUserParam == tmpParam);
  ASSERT_EQ(pArray[0], 0);
  ASSERT_EQ(pArray[1], 1);

  ASSERT_EQ(getRecordCounts, insertCounts);
  taos_close(taos);

  printf("taos_query_a_param test finished.\n");
}

TEST_F(taoscTest, taos_query_test) {
  char    sql[1024] = {0};
  int32_t code = 0;
  TAOS*   taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != nullptr);

  const char* table_name = "taosc_test_table1";
  sprintf(sql, "select * from taosc_test_db.%s;", table_name);
  TAOS_RES* res = taos_query(taos, sql);
  ASSERT_TRUE(res != nullptr);

  getRecordCounts = 0;
  TAOS_ROW row;
  while ((row = taos_fetch_row(res))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, insertCounts);
  taos_free_result(res);
  taos_close(taos);

  printf("taos_query test finished.\n");
}

void queryCallback2(void* param, void* res, int32_t code) {
  ASSERT_TRUE(code == 0);
  ASSERT_TRUE(param == pUserParam);
  // After using taos_query_a to query, using taos_fetch_row in the callback will cause blocking. 
  // Reason: schProcessOnCbBegin SCH_LOCK_TASK(pTask)
  TAOS_ROW row;
  row = taos_fetch_row(res);
  ASSERT_TRUE(row == NULL);
  int* errCode = taosGetErrno();
  ASSERT_TRUE(*errCode = TSDB_CODE_TSC_INVALID_OPERATION);

  tsem_post(&query_sem);
  taos_free_result(res);
}

TEST_F(taoscTest, taos_query_a_t2) {
  char    sql[1024] = {0};
  int32_t code = 0;
  TAOS*   taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != nullptr);

  getRecordCounts = 0;

  const char* table_name = "taosc_test_table1";
  sprintf(sql, "select * from taosc_test_db.%s;", table_name);

  pUserParam = NULL;
  tsem_init(&query_sem, 0, 0);
  taos_query_a(taos, sql, queryCallback2, pUserParam);
  tsem_timewait(&query_sem, 10 * 1000);

  ASSERT_NE(getRecordCounts, insertCounts);
  taos_close(taos);

  printf("taos_query_a_t2 test finished.\n");
}

void queryCallbackStartFetchThread(void* param, void* res, int32_t code) {
  printf("queryCallbackStartFetchThread start...\n");
  ASSERT_TRUE(code == 0);
  void** tmp = (void**)param;
  *tmp = res;
  printf("queryCallbackStartFetchThread end. res:%p\n", res);
  tsem_post(&query_sem);
}

TEST_F(taoscTest, taos_query_a_fetch_row) {
  char    sql[1024] = {0};
  int32_t code = 0;
  TAOS*   taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_TRUE(taos != nullptr);

  getRecordCounts = 0;

  const char* table_name = "taosc_test_table1";
  sprintf(sql, "select * from taosc_test_db.%s;", table_name);

  tsem_init(&query_sem, 0, 0);
  printf("taos_query_a_fetch_row  query start...\n");
  TAOS_RES *res;
  TAOS_RES **pres = &res;
  taos_query_a(taos, sql, queryCallbackStartFetchThread, pres);
  tsem_wait(&query_sem);

  getRecordCounts = 0;
  TAOS_ROW row;
  printf("taos_query_a_fetch_row  taos_fetch_row start...\n");

  while ((row = taos_fetch_row(*pres))) {
     getRecordCounts++;
  }
  printf("taos_query_a_fetch_row  taos_fetch_row end. %p record count:%d.\n", *pres, getRecordCounts);
  taos_free_result(*pres);

  ASSERT_EQ(getRecordCounts, insertCounts);
  taos_close(taos);

  printf("taos_query_a_fetch_row test finished.\n");
}

