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
//    printf("start test setup.\n");
//    TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
//    ASSERT_TRUE(taos != nullptr);
//
//    TAOS_RES* res = taos_query(taos, "drop database IF EXISTS taosc_test_db;");
//    if (taos_errno(res) != 0) {
//      printf("error in drop database taosc_test_db, reason:%s\n", taos_errstr(res));
//      return;
//    }
//    taosSsleep(5);
//    taos_free_result(res);
//    printf("drop database taosc_test_db,finished.\n");
//
//    res = taos_query(taos, "create database taosc_test_db;");
//    if (taos_errno(res) != 0) {
//      printf("error in create database taosc_test_db, reason:%s\n", taos_errstr(res));
//      return;
//    }
//    taosSsleep(5);
//    taos_free_result(res);
//    printf("create database taosc_test_db,finished.\n");
//
//    taos_close(taos);
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

/**
 * @brief execute sql only.
 *
 * @param taos
 * @param sql
 */
void executeSQL(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int       code = taos_errno(res);
  if (code != 0) {
    printf("%s\n", taos_errstr(res));
    taos_free_result(res);
    taos_close(taos);
    exit(EXIT_FAILURE);
  }
  taos_free_result(res);
}

/**
 * @brief check return status and exit program when error occur.
 *
 * @param stmt
 * @param code
 * @param msg
 */
void checkErrorCode(TAOS_STMT *stmt, int code, const char* msg) {
  if (code != 0) {
    printf("%s. error: %s\n", msg, taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
    exit(EXIT_FAILURE);
  }
}

typedef struct {
    int64_t ts;
    float current;
    int voltage;
    float phase;
} Row;


/**
 * @brief insert data using stmt API
 *
 * @param taos
 */
void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  // prepare
//  const char *sql = "INSERT INTO ?.d1001 USING meters TAGS(?, ?) values(?, ?, ?, ?)";
//  const char *sql = "INSERT INTO ?.? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
//  const char *sql = "INSERT INTO power.? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
//  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
//  const char *sql = "INSERT INTO ? USING meters TAGS(?, ?) values(?, ?, ?, ?)";
  const char *sql = "insert into huawei USING meters TAGS(?, ?) values(?, ?, ?, ?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_prepare");
  // bind table name and tags
  TAOS_MULTI_BIND tags[2];
  char     *location = "California.SanFrancisco";
  int       groupId = 2;
  tags[0].buffer_type = TSDB_DATA_TYPE_BINARY;
  tags[0].buffer_length = strlen(location);
  tags[0].length = (int32_t *)&tags[0].buffer_length;
  tags[0].buffer = location;
  tags[0].is_null = NULL;

  tags[1].buffer_type = TSDB_DATA_TYPE_INT;
  tags[1].buffer_length = sizeof(int);
  tags[1].length = (int32_t *)&tags[1].buffer_length;
  tags[1].buffer = &groupId;
  tags[1].is_null = NULL;

//  code = taos_stmt_set_tbname_tags(stmt, "duck", tags);
//  checkErrorCode(stmt, code, "failed to execute taos_stmt_set_dbname_tbname_tags");

  // insert two rows with multi binds
  TAOS_MULTI_BIND params[4];
  // values to bind
  int64_t ts[] = {1648432611250, 1648432611778};
  float   current[] = {10.3, 12.6};
  int     voltage[] = {219, 218};
  float   phase[] = {0.31, 0.33};
  // is_null array
  char is_null[2] = {0};
  // length array
  int32_t int64Len[2] = {sizeof(int64_t)};
  int32_t floatLen[2] = {sizeof(float)};
  int32_t intLen[2] = {sizeof(int)};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(int64_t);
  params[0].buffer = ts;
  params[0].length = int64Len;
  params[0].is_null = is_null;
  params[0].num = 2;

  params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[1].buffer_length = sizeof(float);
  params[1].buffer = current;
  params[1].length = floatLen;
  params[1].is_null = is_null;
  params[1].num = 2;

  params[2].buffer_type = TSDB_DATA_TYPE_INT;
  params[2].buffer_length = sizeof(int);
  params[2].buffer = voltage;
  params[2].length = intLen;
  params[2].is_null = is_null;
  params[2].num = 2;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(float);
  params[3].buffer = phase;
  params[3].length = floatLen;
  params[3].is_null = is_null;
  params[3].num = 2;

  code = taos_stmt_bind_param_batch(stmt, params); // bind batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_bind_param_batch");
  code = taos_stmt_add_batch(stmt);  // add batch
  checkErrorCode(stmt, code, "failed to execute taos_stmt_add_batch");
  // execute
  code = taos_stmt_execute(stmt);
  checkErrorCode(stmt, code, "failed to execute taos_stmt_execute");
  int affectedRows = taos_stmt_affected_rows(stmt);
  printf("successfully inserted %d rows\n", affectedRows);

  // close
  taos_stmt_close(stmt);
}

TEST_F(taoscTest, taos_stmt_test) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 6030);
  if (taos == NULL) {
    printf("failed to connect to server");
    exit(EXIT_FAILURE);
  }
//  executeSQL(taos, "drop database if exists power");
//  executeSQL(taos, "create database power");
  executeSQL(taos, "use power");
//  executeSQL(taos, "create stable meters (ts timestamp, current float, voltage int, phase float) tags (location binary(64), groupId int)");
  insertData(taos);
  taos_close(taos);
  taos_cleanup();
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

