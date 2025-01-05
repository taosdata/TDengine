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
#include <string.h>
#include "clientInt.h"
#include "osSemaphore.h"
#include "taoserror.h"
#include "tglobal.h"
#include "thash.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "../inc/clientStmt.h"
#include "../inc/clientStmt2.h"
#include "executor.h"
#include "taos.h"

namespace {
void do_query(TAOS *taos, const char *sql) {
  TAOS_RES *result = taos_query(taos, sql);
  int       code = taos_errno(result);
  ASSERT_EQ(code, 0);

  taos_free_result(result);
}

void checkRows(TAOS *pConn, const char *sql, int32_t expectedRows) {
  TAOS_RES *pRes = taos_query(pConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  TAOS_ROW pRow = NULL;
  int      rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, expectedRows);
  taos_free_result(pRes);
}

typedef struct {
  int64_t ts;
  float   current;
  int     voltage;
  float   phase;
} Row;

int CTB_NUMS = 3;
int ROW_NUMS = 3;
int CYC_NUMS = 3;

void insertData(TAOS *taos) {
  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);
  const char *sql = "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)";
  int         code = taos_stmt_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);
  int total_affected = 0;

  for (int k = 0; k < CYC_NUMS; k++) {
    for (int i = 1; i <= CTB_NUMS; i++) {
      char *table_name = (char *)taosMemoryMalloc(20);
      sprintf(table_name, "d_bind_%d", i);
      char *location = (char *)taosMemoryMalloc(20);
      sprintf(location, "location_%d", i);

      // set table name and tags
      TAOS_MULTI_BIND tags[2];
      // groupId
      tags[0].buffer_type = TSDB_DATA_TYPE_INT;
      tags[0].buffer_length = sizeof(int);
      tags[0].length = (int32_t *)&tags[0].buffer_length;
      tags[0].buffer = &i;
      tags[0].is_null = NULL;
      tags[0].num = 1;
      // location
      tags[1].buffer_type = TSDB_DATA_TYPE_BINARY;
      tags[1].buffer_length = strlen(location);
      tags[1].length = (int32_t *)&tags[1].buffer_length;
      tags[1].buffer = location;
      tags[1].is_null = NULL;
      tags[1].num = 1;
      if (k % 2 == 0) {
        code = taos_stmt_set_tbname_tags(stmt, table_name, tags);
        ASSERT_EQ(code, 0);

      } else {
        code = taos_stmt_set_tbname(stmt, table_name);
        ASSERT_EQ(code, 0);
        code = taos_stmt_set_tags(stmt, tags);
        ASSERT_EQ(code, 0);
      }

      // insert rows
      TAOS_MULTI_BIND params[4];
      // ts
      params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
      params[0].buffer_length = sizeof(int64_t);
      params[0].length = (int32_t *)&params[0].buffer_length;
      params[0].is_null = NULL;
      params[0].num = 1;
      // current
      params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
      params[1].buffer_length = sizeof(float);
      params[1].length = (int32_t *)&params[1].buffer_length;
      params[1].is_null = NULL;
      params[1].num = 1;
      // voltage
      params[2].buffer_type = TSDB_DATA_TYPE_INT;
      params[2].buffer_length = sizeof(int);
      params[2].length = (int32_t *)&params[2].buffer_length;
      params[2].is_null = NULL;
      params[2].num = 1;
      // phase
      params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
      params[3].buffer_length = sizeof(float);
      params[3].length = (int32_t *)&params[3].buffer_length;
      params[3].is_null = NULL;
      params[3].num = 1;

      for (int j = 0; j < ROW_NUMS; j++) {
        struct timeval tv;
        (&tv, NULL);
        long long milliseconds = tv.tv_sec * 1000LL + tv.tv_usec / 1000;  // current timestamp in milliseconds
        int64_t   ts = milliseconds + j + k * 10000;
        float     current = (float)0.0001f * j;
        int       voltage = j;
        float     phase = (float)0.0001f * j;
        params[0].buffer = &ts;
        params[1].buffer = &current;
        params[2].buffer = &voltage;
        params[3].buffer = &phase;
        // bind param
        code = taos_stmt_bind_param(stmt, params);
        ASSERT_EQ(code, 0);
      }
      // add batch
      code = taos_stmt_add_batch(stmt);
      ASSERT_EQ(code, 0);
      // execute batch
      code = taos_stmt_execute(stmt);
      ASSERT_EQ(code, 0);
      // get affected rows
      int affected = taos_stmt_affected_rows_once(stmt);
      total_affected += affected;
    }
  }
  ASSERT_EQ(total_affected, CTB_NUMS * ROW_NUMS * CYC_NUMS);
  checkRows(taos, "select * from meters", CTB_NUMS * ROW_NUMS * CYC_NUMS);

  taos_stmt_close(stmt);
}
}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(stmtCase, normal_insert) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  // create database and table
  do_query(taos, "DROP DATABASE IF EXISTS power");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  do_query(taos, "USE power");
  do_query(taos,
           "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
           "(groupId INT, location BINARY(24))");

  insertData(taos);
  taos_close(taos);

  taos_cleanup();
}

#pragma GCC diagnostic pop