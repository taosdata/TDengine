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

void insertData(TAOS *taos, TAOS_STMT_OPTIONS *option, const char *sql, int CTB_NUMS, int ROW_NUMS, int CYC_NUMS,
                bool isCreateTable) {
  // create database and table
  do_query(taos, "DROP DATABASE IF EXISTS power");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  do_query(taos,
           "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
           "(groupId INT, location BINARY(24))");
  do_query(taos, "USE power");

  // init
  TAOS_STMT *stmt;
  if (option == nullptr) {
    stmt = taos_stmt_init(taos);
  } else {
    stmt = taos_stmt_init_with_options(taos, option);
  }
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);
  int total_affected = 0;

  for (int k = 0; k < CYC_NUMS; k++) {
    for (int i = 1; i <= CTB_NUMS; i++) {
      char           *table_name = (char *)taosMemoryMalloc(20);
      TAOS_MULTI_BIND tags[2];

      sprintf(table_name, "d_bind_%d", i);
      if (isCreateTable && k == 0) {
        char *tmp = (char *)taosMemoryMalloc(100);
        sprintf(tmp, "CREATE TABLE %s using meters TAGS (1, 'abc')", table_name);
        do_query(taos, tmp);
        taosMemFree(tmp);
      } else {
        char *location = (char *)taosMemoryMalloc(20);
        sprintf(location, "location_%d", i);

        // set table name and tags
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
      }

      if (!isCreateTable) {
        if (k % 2 == 0) {
          code = taos_stmt_set_tbname_tags(stmt, table_name, tags);
          ASSERT_EQ(code, 0);

        } else {
          if (i % 2 == 0) {
            code = taos_stmt_set_tbname(stmt, table_name);
            ASSERT_EQ(code, 0);
          } else {
            code = taos_stmt_set_sub_tbname(stmt, table_name);
            ASSERT_EQ(code, 0);
          }

          code = taos_stmt_set_tags(stmt, tags);
          ASSERT_EQ(code, 0);
        }
      } else {
        code = taos_stmt_set_tbname(stmt, table_name);
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
        int64_t ts = 1591060628000 + j + k * 100;
        float   current = (float)0.0001f * j;
        int     voltage = j;
        float   phase = (float)0.0001f * j;
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

void getFields(TAOS *taos, const char *sql, int expectedALLFieldNum, TAOS_FIELD_E *expectedTagFields,
               int expectedTagFieldNum, TAOS_FIELD_E *expectedColFields, int expectedColFieldNum) {
  // create database and table
  do_query(taos, "DROP DATABASE IF EXISTS power");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS power");
  do_query(taos, "USE power");
  do_query(taos,
           "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
           "(groupId INT, location BINARY(24))");

  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);
  code = taos_stmt_set_tbname(stmt, "ctb_1");
  ASSERT_EQ(code, 0);

  int           fieldNum = 0;
  TAOS_FIELD_E *pFields = NULL;
  code = stmtGetParamNum(stmt, &fieldNum);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(fieldNum, expectedColFieldNum);

  code = taos_stmt_get_tag_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(fieldNum, expectedTagFieldNum);
  for (int i = 0; i < fieldNum; i++) {
    ASSERT_STREQ(pFields[i].name, expectedTagFields[i].name);
    ASSERT_EQ(pFields[i].type, expectedTagFields[i].type);
    ASSERT_EQ(pFields[i].precision, expectedTagFields[i].precision);
    // ASSERT_EQ(pFields[i].bytes, expectedTagFields[i].bytes);
    ASSERT_EQ(pFields[i].scale, expectedTagFields[i].scale);
  }
  taosMemoryFree(pFields);

  int type;
  int bytes;
  code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(fieldNum, expectedColFieldNum);
  for (int i = 0; i < fieldNum; i++) {
    taos_stmt_get_param(stmt, i, &type, &bytes);
    ASSERT_EQ(type, pFields[i].type);
    ASSERT_EQ(bytes, pFields[i].bytes);

    ASSERT_STREQ(pFields[i].name, expectedColFields[i].name);
    ASSERT_EQ(pFields[i].type, expectedColFields[i].type);
    ASSERT_EQ(pFields[i].precision, expectedColFields[i].precision);
    // ASSERT_EQ(pFields[i].bytes, expectedColFields[i].bytes);
    ASSERT_EQ(pFields[i].scale, expectedColFields[i].scale);
  }
  taosMemoryFree(pFields);

  taos_stmt_close(stmt);
}

}  // namespace

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(stmtCase, stb_insert) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  // interlace = 0
  { insertData(taos, nullptr, "INSERT INTO power.? USING meters TAGS(?,?) VALUES (?,?,?,?)", 1, 1, 1, false); }

  { insertData(taos, nullptr, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 3, 3, 3, false); }

  { insertData(taos, nullptr, "INSERT INTO ? VALUES (?,?,?,?)", 3, 3, 3, true); }

  // interlace = 1
  {
    TAOS_STMT_OPTIONS options = {0, true, true};
    insertData(taos, &options, "INSERT INTO ? VALUES (?,?,?,?)", 3, 3, 3, true);
  }

  taos_close(taos);
}

TEST(stmtCase, get_fields) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  {
    TAOS_FIELD_E tagFields[2] = {{"groupid", TSDB_DATA_TYPE_INT, 0, 0, sizeof(int)},
                                 {"location", TSDB_DATA_TYPE_BINARY, 0, 0, 24}};
    TAOS_FIELD_E colFields[4] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, sizeof(int64_t)},
                                 {"current", TSDB_DATA_TYPE_FLOAT, 0, 0, sizeof(float)},
                                 {"voltage", TSDB_DATA_TYPE_INT, 0, 0, sizeof(int)},
                                 {"phase", TSDB_DATA_TYPE_FLOAT, 0, 0, sizeof(float)}};
    getFields(taos, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 7, &tagFields[0], 2, &colFields[0], 4);
  }
  taos_close(taos);
}

TEST(stmtCase, all_type) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "use db");
  do_query(taos,
           "create stable db.stb(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 smallint, c7 "
           "tinyint, c8 bool, c9 nchar(8))TAGS(tts timestamp, t1 int, t2 bigint, t3 float, t4 double, t5 binary(8), t6 "
           "smallint, t7 "
           "tinyint, t8 bool, t9 nchar(8))");

  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);

  uintptr_t c10len = 0;
  struct {
    int64_t       c1;
    int32_t       c2;
    int64_t       c3;
    float         c4;
    double        c5;
    unsigned char c6[8];
    int16_t       c7;
    int8_t        c8;
    int8_t        c9;
    char          c10[32];
  } v = {0};
  TAOS_MULTI_BIND params[11];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.c1);
  params[0].buffer = &v.c1;
  params[0].length = (int32_t *)&params[0].buffer_length;
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer_length = sizeof(v.c2);
  params[1].buffer = &v.c2;
  params[1].length = (int32_t *)&params[1].buffer_length;
  params[1].is_null = NULL;
  params[1].num = 1;

  params[2].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[2].buffer_length = sizeof(v.c3);
  params[2].buffer = &v.c3;
  params[2].length = (int32_t *)&params[2].buffer_length;
  params[2].is_null = NULL;
  params[2].num = 1;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer_length = sizeof(v.c4);
  params[3].buffer = &v.c4;
  params[3].length = (int32_t *)&params[3].buffer_length;
  params[3].is_null = NULL;
  params[3].num = 1;

  params[4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[4].buffer_length = sizeof(v.c5);
  params[4].buffer = &v.c5;
  params[4].length = (int32_t *)&params[4].buffer_length;
  params[4].is_null = NULL;
  params[4].num = 1;

  params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[5].buffer_length = sizeof(v.c6);
  params[5].buffer = &v.c6;
  params[5].length = (int32_t *)&params[5].buffer_length;
  params[5].is_null = NULL;
  params[5].num = 1;

  params[6].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[6].buffer_length = sizeof(v.c7);
  params[6].buffer = &v.c7;
  params[6].length = (int32_t *)&params[6].buffer_length;
  params[6].is_null = NULL;
  params[6].num = 1;

  params[7].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[7].buffer_length = sizeof(v.c8);
  params[7].buffer = &v.c8;
  params[7].length = (int32_t *)&params[7].buffer_length;
  params[7].is_null = NULL;
  params[7].num = 1;

  params[8].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[8].buffer_length = sizeof(v.c9);
  params[8].buffer = &v.c9;
  params[8].length = (int32_t *)&params[8].buffer_length;
  params[8].is_null = NULL;
  params[8].num = 1;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer_length = sizeof(v.c10);
  params[9].buffer = &v.c10;
  params[9].length = (int32_t *)&c10len;
  params[9].is_null = NULL;
  params[9].num = 1;

  char *stmt_sql = "insert into ? using stb tags(?,?,?,?,?,?,?,?,?,?)values (?,?,?,?,?,?,?,?,?,?)";
  int   code = taos_stmt_prepare(stmt, stmt_sql, 0);
  ASSERT_EQ(code, 0);

  code = taos_stmt_set_tbname(stmt, "ntb");
  ASSERT_EQ(code, 0);

  code = taos_stmt_set_tags(stmt, params);
  ASSERT_EQ(code, 0);

  v.c1 = (int64_t)1591060628000;
  v.c2 = (int32_t)2147483647;
  v.c3 = (int64_t)2147483648;
  v.c4 = (float)0.1;
  v.c5 = (double)0.000000001;
  for (int j = 0; j < sizeof(v.c6); j++) {
    v.c6[j] = (char)('a');
  }
  v.c7 = 32767;
  v.c8 = 127;
  v.c9 = 1;
  strcpy(v.c10, "一二三四五六七八");
  c10len = strlen(v.c10);
  taos_stmt_bind_param(stmt, params);
  taos_stmt_add_batch(stmt);

  code = taos_stmt_execute(stmt);
  ASSERT_EQ(code, 0);

  taos_stmt_close(stmt);
  taos_close(taos);
}

#pragma GCC diagnostic pop