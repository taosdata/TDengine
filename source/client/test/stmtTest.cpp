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
#include "geosWrapper.h"
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

void checkError(TAOS_STMT *stmt, int code) {
  if (code != TSDB_CODE_SUCCESS) {
    STscStmt *pStmt = (STscStmt *)stmt;
    if (pStmt == nullptr || pStmt->sql.sqlStr == nullptr) {
      printf("stmt api error\n  stats : %d\n  errstr : %s\n", pStmt->sql.status, taos_stmt_errstr(stmt));
    } else {
      printf("stmt api error\n  sql : %s\n  stats : %d\n  errstr : %s\n", pStmt->sql.sqlStr, pStmt->sql.status,
             taos_stmt_errstr(stmt));
    }
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
}

void do_query(TAOS *taos, const char *sql) {
  TAOS_RES *result = taos_query(taos, sql);
  // printf("sql: %s\n", sql);
  int code = taos_errno(result);
  while (code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    result = taos_query(taos, sql);
    code = taos_errno(result);
  }
  if (code != TSDB_CODE_SUCCESS) {
    printf("query failen  sql : %s\n  errstr : %s\n", sql, taos_errstr(result));
    ASSERT_EQ(taos_errno(result), TSDB_CODE_SUCCESS);
  }
  taos_free_result(result);
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
  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_2");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_2");
  do_query(
      taos,
      "CREATE STABLE IF NOT EXISTS stmt_testdb_2.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
      "(groupId INT, location BINARY(24))");
  do_query(taos, "USE stmt_testdb_2");

  // init
  TAOS_STMT *stmt;
  if (option == nullptr) {
    stmt = taos_stmt_init(taos);
  } else {
    stmt = taos_stmt_init_with_options(taos, option);
  }
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt_prepare(stmt, sql, 0);
  checkError(stmt, code);
  int total_affected = 0;

  for (int k = 0; k < CYC_NUMS; k++) {
    for (int i = 1; i <= CTB_NUMS; i++) {
      char *table_name = (char *)taosMemoryMalloc(20);
      char *location = (char *)taosMemoryMalloc(20);

      TAOS_MULTI_BIND tags[2];

      sprintf(table_name, "d_bind_%d", i);
      if (isCreateTable && k == 0) {
        char *tmp = (char *)taosMemoryMalloc(100);
        sprintf(tmp, "CREATE TABLE %s using meters TAGS (1, 'abc')", table_name);
        do_query(taos, tmp);
        taosMemoryFree(tmp);
      } else {
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
          checkError(stmt, code);

        } else {
          if (i % 2 == 0) {
            code = taos_stmt_set_tbname(stmt, table_name);
            checkError(stmt, code);
          } else {
            code = taos_stmt_set_sub_tbname(stmt, table_name);
            checkError(stmt, code);
          }

          code = taos_stmt_set_tags(stmt, tags);
          checkError(stmt, code);
        }
      } else {
        code = taos_stmt_set_tbname(stmt, table_name);
        checkError(stmt, code);
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
        int64_t ts = 1591060628000 + j + k * 100000;
        float   current = (float)0.0001f * j;
        int     voltage = j;
        float   phase = (float)0.0001f * j;
        params[0].buffer = &ts;
        params[1].buffer = &current;
        params[2].buffer = &voltage;
        params[3].buffer = &phase;
        // bind param
        code = taos_stmt_bind_param(stmt, params);
        checkError(stmt, code);
      }
      // add batch
      code = taos_stmt_add_batch(stmt);
      checkError(stmt, code);
      // execute batch
      code = taos_stmt_execute(stmt);
      checkError(stmt, code);
      // get affected rows
      int affected = taos_stmt_affected_rows_once(stmt);
      total_affected += affected;

      taosMemoryFree(table_name);
      taosMemoryFree(location);
    }
  }
  ASSERT_EQ(total_affected, CTB_NUMS * ROW_NUMS * CYC_NUMS);

  taos_stmt_close(stmt);
}

void getFields(TAOS *taos, const char *sql, int expectedALLFieldNum, TAOS_FIELD_E *expectedTagFields,
               int expectedTagFieldNum, TAOS_FIELD_E *expectedColFields, int expectedColFieldNum) {
  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt_prepare(stmt, sql, 0);
  checkError(stmt, code);
  code = taos_stmt_set_tbname(stmt, "ctb_1");
  checkError(stmt, code);

  int           fieldNum = 0;
  TAOS_FIELD_E *pFields = NULL;
  code = stmtGetParamNum(stmt, &fieldNum);
  checkError(stmt, code);
  ASSERT_EQ(fieldNum, expectedColFieldNum);

  code = taos_stmt_get_tag_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code);
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
  checkError(stmt, code);
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

void getFieldsError(TAOS *taos, const char *sql, int expectedErrocode) {
  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);
  STscStmt *pStmt = (STscStmt *)stmt;

  int code = taos_stmt_prepare(stmt, sql, 0);

  int           fieldNum = 0;
  TAOS_FIELD_E *pFields = NULL;
  code = taos_stmt_get_tag_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, expectedErrocode);
  ASSERT_EQ(pStmt->errCode, TSDB_CODE_SUCCESS);

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
  { insertData(taos, nullptr, "INSERT INTO stmt_testdb_2.? USING meters TAGS(?,?) VALUES (?,?,?,?)", 1, 1, 1, false); }

  { insertData(taos, nullptr, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 3, 3, 3, false); }

  { insertData(taos, nullptr, "INSERT INTO ? VALUES (?,?,?,?)", 3, 3, 3, true); }

  // interlace = 1
  {
    TAOS_STMT_OPTIONS options = {0, true, true};
    insertData(taos, &options, "INSERT INTO ? VALUES (?,?,?,?)", 3, 3, 3, true);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_2");
  taos_close(taos);
}

TEST(stmtCase, get_fields) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  // create database and table
  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_3");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_3");
  do_query(taos, "USE stmt_testdb_3");
  do_query(
      taos,
      "CREATE STABLE IF NOT EXISTS stmt_testdb_3.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
      "(groupId INT, location BINARY(24))");
  // nomarl test
  {
    TAOS_FIELD_E tagFields[2] = {{"groupid", TSDB_DATA_TYPE_INT, 0, 0, sizeof(int)},
                                 {"location", TSDB_DATA_TYPE_BINARY, 0, 0, 24}};
    TAOS_FIELD_E colFields[4] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, sizeof(int64_t)},
                                 {"current", TSDB_DATA_TYPE_FLOAT, 0, 0, sizeof(float)},
                                 {"voltage", TSDB_DATA_TYPE_INT, 0, 0, sizeof(int)},
                                 {"phase", TSDB_DATA_TYPE_FLOAT, 0, 0, sizeof(float)}};
    getFields(taos, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", 7, &tagFields[0], 2, &colFields[0], 4);
  }
  // error case [TD-33570]
  { getFieldsError(taos, "INSERT INTO ? VALUES (?,?,?,?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR); }

  { getFieldsError(taos, "INSERT INTO ? USING meters TAGS(?,?) VALUES (?,?,?,?)", TSDB_CODE_TSC_STMT_TBNAME_ERROR); }


  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_3");
  taos_close(taos);
}

TEST(stmtCase, all_type) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_1");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_1");
  do_query(
      taos,
      "CREATE STABLE stmt_testdb_1.stb1(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 "
      "smallint, c7 "
      "tinyint, c8 bool, c9 nchar(8), c10 geometry(100))TAGS(tts timestamp, t1 int, t2 bigint, t3 float, t4 double, t5 "
      "binary(8), t6 smallint, t7 tinyint, t8 bool, t9 nchar(8), t10 geometry(100))");

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
  } v = {1591060628000, 1, 2, 3.0, 4.0, "abcdef", 5, 6, 7, "ijnop"};

  struct {
    int32_t c1;
    int32_t c2;
    int32_t c3;
    int32_t c4;
    int32_t c5;
    int32_t c6;
    int32_t c7;
    int32_t c8;
    int32_t c9;
    int32_t c10;
  } v_len = {sizeof(int64_t), sizeof(int32_t),
             sizeof(int64_t), sizeof(float),
             sizeof(double),  8,
             sizeof(int16_t), sizeof(int8_t),
             sizeof(int8_t),  8};
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

  size_t size;
  int    code = initCtxGeomFromText();
  checkError(stmt, code);

  unsigned char *outputGeom1;
  const char    *wkt = "LINESTRING(1.0 1.0, 2.0 2.0)";
  code = doGeomFromText(wkt, &outputGeom1, &size);
  checkError(stmt, code);
  params[10].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[10].buffer = outputGeom1;
  params[9].buffer_length = size;
  params[10].length = (int32_t *)&size;
  params[10].is_null = NULL;
  params[10].num = 1;

  char *stmt_sql = "insert into stmt_testdb_1.? using stb1 tags(?,?,?,?,?,?,?,?,?,?,?)values (?,?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code);

  code = taos_stmt_set_tbname(stmt, "ntb");
  checkError(stmt, code);

  code = taos_stmt_set_tags(stmt, params);
  checkError(stmt, code);

  code = taos_stmt_bind_param(stmt, params);
  checkError(stmt, code);

  code = taos_stmt_add_batch(stmt);
  checkError(stmt, code);

  code = taos_stmt_execute(stmt);
  checkError(stmt, code);

  taos_stmt_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_1");
  taos_close(taos);
}

TEST(stmtCase, geometry) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_5");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_5");
  do_query(taos, "CREATE TABLE stmt_testdb_5.tb1(ts timestamp,c1 geometry(256))");

  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);
  unsigned char wkb1[3][61] = {
      {
          0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
      },
      {0x01, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f},
      {0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}};

  int64_t  ts[3] = {1591060628000, 1591060628001, 1591060628002};
  int32_t *t64_len = (int32_t *)taosMemoryMalloc(sizeof(int32_t) * 3);
  int32_t *wkb_len = (int32_t *)taosMemoryMalloc(sizeof(int32_t) * 3);

  for (int i = 0; i < 3; i++) {
    t64_len[i] = sizeof(int64_t);
  }
  wkb_len[0] = 21;
  wkb_len[1] = 61;
  wkb_len[2] = 41;

  TAOS_MULTI_BIND params[2];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(int64_t);
  params[0].buffer = &ts[0];
  params[0].length = t64_len;
  params[0].is_null = NULL;
  params[0].num = 3;

  params[1].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[1].buffer_length = 61;
  params[1].buffer = wkb1;
  params[1].length = wkb_len;
  params[1].is_null = NULL;
  params[1].num = 3;

  char *stmt_sql = "insert into stmt_testdb_5.tb1 (ts,c1)values(?,?)";
  int   code = taos_stmt_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code);

  code = taos_stmt_bind_param_batch(stmt, params);
  checkError(stmt, code);

  code = taos_stmt_add_batch(stmt);
  checkError(stmt, code);

  code = taos_stmt_execute(stmt);
  checkError(stmt, code);

  //test wrong wkb input
  unsigned char wkb2[3][61] = {
      {
          0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
          0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
      },
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f},
      {0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40}};
  params[1].buffer = wkb2;
  code = taos_stmt_bind_param_batch(stmt, params);
  ASSERT_EQ(code, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);

  taosMemoryFree(t64_len);
  taosMemoryFree(wkb_len);
  taos_stmt_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_5");
  taos_close(taos);
}
//TD-33582
TEST(stmtCase, errcode) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_4");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_4");
  do_query(taos, "USE stmt_testdb_4");
  do_query(
      taos,
      "CREATE STABLE IF NOT EXISTS stmt_testdb_4.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
      "(groupId INT, location BINARY(24))");

  TAOS_STMT *stmt = taos_stmt_init(taos);
  ASSERT_NE(stmt, nullptr);
  char *sql = "select * from t where ts > ? and name = ? foo = ?";
  int   code = taos_stmt_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int fieldNum = 0;
  TAOS_FIELD_E *pFields = NULL;
  code = stmtGetParamNum(stmt, &fieldNum);
  ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);

  code = taos_stmt_get_tag_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);
  // get fail dont influence the next stmt prepare
  sql = "nsert into ? (ts, name) values (?, ?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  checkError(stmt, code);

  taos_stmt_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_4");
  taos_close(taos);
}

// TS-6870
TEST(stmtCase, update) {
  int   code = 0;
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_6");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_6 precision 'us'");
  do_query(taos, "USE stmt_testdb_6");
  do_query(taos,
           "create table stmt_testdb_6.devices(ts timestamp,device_id int,status binary(10),temperature float,humidity "
           "float);");
  do_query(taos,
           "insert into stmt_testdb_6.devices (ts,device_id,status,temperature,humidity) "
           "values(1591060628000000,1,'abc',1.0,1.1);");

  float   temperature = 10.0;
  float   humidity = 10.1;
  int32_t device_id = 2;
  int64_t ts = 1591060628000000;

  int32_t c1_len = sizeof(float);
  int32_t c2_len = sizeof(float);
  int32_t c3_len = sizeof(int32_t);
  int32_t c4_len = sizeof(int64_t);

  TAOS_MULTI_BIND params[5];
  params[0].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[0].buffer = &temperature;
  params[0].buffer_length = sizeof(float);
  params[0].length = &c1_len;
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[1].buffer = &humidity;
  params[0].buffer_length = sizeof(float);
  params[1].length = &c2_len;
  params[1].is_null = NULL;
  params[1].num = 1;

  params[2].buffer_type = TSDB_DATA_TYPE_INT;
  params[2].buffer = &device_id;
  params[2].buffer_length = sizeof(int32_t);
  params[2].length = &c3_len;
  params[2].is_null = NULL;
  params[2].num = 1;

  params[3].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[3].buffer = &ts;
  params[3].buffer_length = sizeof(int64_t);
  params[3].length = &c4_len;
  params[3].is_null = NULL;
  params[3].num = 1;

  params[4].buffer_type = TSDB_DATA_TYPE_INT;
  params[4].buffer = &device_id;
  params[4].buffer_length = sizeof(int32_t);
  params[4].length = &c3_len;
  params[4].is_null = NULL;
  params[4].num = 1;
  // standard case
  {
    printf("case 1 : standard case\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.devices set temperature = ?,humidity = ?,device_id=? where ts=? and device_id=? and "
        "status IS NOT NULL";
    int code = taos_stmt_prepare(stmt, sql, 0);
    checkError(stmt, code);

    int           fieldNum = 0;
    TAOS_FIELD_E *pFields = NULL;

    code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code);
    ASSERT_EQ(fieldNum, 5);
    ASSERT_STREQ(pFields[0].name, "temperature");
    ASSERT_STREQ(pFields[1].name, "humidity");
    ASSERT_STREQ(pFields[2].name, "device_id");
    ASSERT_STREQ(pFields[3].name, "ts");
    ASSERT_EQ(pFields[3].precision, 1);
    ASSERT_STREQ(pFields[4].name, "device_id");

    taosMemoryFree(pFields);

    code = taos_stmt_bind_param_batch(stmt, params);
    checkError(stmt, code);

    code = taos_stmt_add_batch(stmt);
    checkError(stmt, code);

    code = taos_stmt_execute(stmt);
    checkError(stmt, code);

    TAOS_RES *result =
        taos_query(taos, "select temperature,humidity,device_id from stmt_testdb_6.devices where ts=1591060628000000");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), 0);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);

    int32_t actual_temperature = *(float *)row[0];
    ASSERT_EQ(actual_temperature, 10);

    float actual_humidity = *(float *)row[1];
    ASSERT_EQ(static_cast<int>(actual_humidity * 10), static_cast<int>(10.1 * 10));

    int32_t actual_device_id = *(int32_t *)row[2];
    ASSERT_EQ(actual_device_id, 2);

    taos_free_result(result);

    sql =
        "update stmt_testdb_6.devices set temperature = ?,humidity = ?,device_id=? where (ts=? and device_id=? and "
        "status IS NOT NULL)";
    code = taos_stmt_prepare(stmt, sql, 0);
    checkError(stmt, code);

    code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code);
    ASSERT_EQ(fieldNum, 5);
    ASSERT_STREQ(pFields[0].name, "temperature");
    ASSERT_STREQ(pFields[1].name, "humidity");
    ASSERT_STREQ(pFields[2].name, "device_id");
    ASSERT_STREQ(pFields[3].name, "ts");
    ASSERT_STREQ(pFields[4].name, "device_id");

    taosMemoryFree(pFields);

    code = taos_stmt_bind_param_batch(stmt, params);
    checkError(stmt, code);

    code = taos_stmt_add_batch(stmt);
    checkError(stmt, code);

    params[4].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
    params[4].buffer = &ts;
    params[4].buffer_length = sizeof(int64_t);
    params[4].length = &c4_len;
    params[4].is_null = NULL;
    params[4].num = 1;

    params[3].buffer_type = TSDB_DATA_TYPE_INT;
    params[3].buffer = &device_id;
    params[3].buffer_length = sizeof(int32_t);
    params[3].length = &c3_len;
    params[3].is_null = NULL;
    params[3].num = 1;

    sql =
        "UPDATE stmt_testdb_6.devices SET temperature=?, humidity=?, device_id=? WHERE device_id=? AND ts=? AND  "
        "status "
        "IN ('active', 'online') )";
    code = taos_stmt_prepare(stmt, sql, 0);
    checkError(stmt, code);
    code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code);
    ASSERT_EQ(fieldNum, 5);
    ASSERT_STREQ(pFields[0].name, "temperature");
    ASSERT_STREQ(pFields[1].name, "humidity");
    ASSERT_STREQ(pFields[2].name, "device_id");
    ASSERT_STREQ(pFields[3].name, "device_id");
    ASSERT_STREQ(pFields[4].name, "ts");

    taosMemoryFree(pFields);

    code = taos_stmt_bind_param_batch(stmt, params);
    checkError(stmt, code);

    code = taos_stmt_add_batch(stmt);
    checkError(stmt, code);

    sql =
        "UPDATE stmt_testdb_6.devices SET temperature=?, humidity=?, device_id=? WHERE device_id=? AND ts=? AND EXISTS "
        "(SELECT 1 FROM test.device_logs WHERE device_id = test.devices.device_id)";
    code = taos_stmt_prepare(stmt, sql, 0);
    checkError(stmt, code);

    code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code);
    ASSERT_EQ(fieldNum, 5);
    ASSERT_STREQ(pFields[0].name, "temperature");
    ASSERT_STREQ(pFields[1].name, "humidity");
    ASSERT_STREQ(pFields[2].name, "device_id");
    ASSERT_STREQ(pFields[3].name, "device_id");
    ASSERT_STREQ(pFields[4].name, "ts");

    taosMemoryFree(pFields);

    code = taos_stmt_bind_param_batch(stmt, params);
    checkError(stmt, code);

    code = taos_stmt_add_batch(stmt);
    checkError(stmt, code);

    taos_stmt_close(stmt);
  }

  // pk error
  {
    printf("case 2 : pk error\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.devices set ts = ?,humidity = ?,device_id=? where ts=? and device_id=? and "
        "status IS NOT NULL";
    int code = taos_stmt_prepare(stmt, sql, 0);
    ASSERT_EQ(TSDB_CODE_PAR_SYNTAX_ERROR, code);
    ASSERT_STREQ("Cannot update primary key column 'ts'", taos_stmt_errstr(stmt));
    taos_stmt_close(stmt);
  }
  // sql error
  {
    printf("case 3 : sql error\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.devices temperature = ?,humidity = ?,device_id=? where ts=? and device_id=? and "
        "status IS not null";
    code = taos_stmt_prepare(stmt, sql, 0);
    code = taos_stmt_bind_param_batch(stmt, params);
    ASSERT_EQ(TSDB_CODE_PAR_SYNTAX_ERROR, code);
    ASSERT_STREQ(
        "syntax error near \"stmt_testdb_6.devices temperature = ?,humidity = ?,device_id=? where ts=? and device_id=? "
        "and status is not null\"",
        taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }
  // no where pk
  {
    printf("case 4 : no where pk\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql = "update stmt_testdb_6.devices set temperature = ?,humidity = ?,device_id=?";
    code = taos_stmt_prepare(stmt, sql, 0);
    code = taos_stmt_bind_param_batch(stmt, params);
    ASSERT_EQ(TSDB_CODE_TSC_INVALID_OPERATION, code);
    ASSERT_STREQ("Primary timestamp column should not be null", taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }
  // table not exist
  {
    printf("case 5 : tb not exist\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.t1 set temperature = ?,humidity = ?,device_id=? where ts=? and device_id=? and "
        "status IS NOT NULL";
    code = taos_stmt_prepare(stmt, sql, 0);
    ASSERT_EQ(TSDB_CODE_PAR_TABLE_NOT_EXIST, code);
    ASSERT_STREQ("Table does not exist", taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }
  // wrong tbname
  {
    printf("case 6 : wrong tbname\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.中文表名 set temperature = ?,humidity = ?,device_id=? where ts=? and device_id=? and "
        "status IS NOT NULL";
    code = taos_stmt_prepare(stmt, sql, 0);
    ASSERT_EQ(TSDB_CODE_TSC_STMT_TBNAME_ERROR, code);
    ASSERT_STREQ("Invalid table name", taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }

  // wrong sql
  {
    printf("case 7 : wrong sql\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.devices temperature = ?,humidity = ?,device_id=? set ts=? where device_id=? and "
        "status IS NOT NULL";
    code = taos_stmt_prepare(stmt, sql, 0);
    ASSERT_EQ(TSDB_CODE_PAR_SYNTAX_ERROR, code);
    ASSERT_STREQ("Expected SET keyword", taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }

  // not ?
  {
    printf("case 8 : not ?\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.devices set temperature=2.1,humidity=2.2,device_id=3 where ts=? and device_id=? and "
        "status IS NOT NULL";
    code = taos_stmt_prepare(stmt, sql, 0);
    ASSERT_EQ(TSDB_CODE_PAR_SYNTAX_ERROR, code);
    ASSERT_STREQ("Expected '?' placeholder", taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }

  TAOS_MULTI_BIND params2[4];
  params2[0].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params2[0].buffer = &temperature;
  params2[0].buffer_length = sizeof(float);
  params2[0].length = &c1_len;
  params2[0].is_null = NULL;
  params2[0].num = 1;

  params2[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params2[1].buffer = &humidity;
  params2[1].buffer_length = sizeof(float);
  params2[1].length = &c2_len;
  params2[1].is_null = NULL;
  params2[1].num = 1;

  params2[2].buffer_type = TSDB_DATA_TYPE_INT;
  params2[2].buffer = &device_id;
  params2[2].buffer_length = sizeof(int32_t);
  params2[2].length = &c3_len;
  params2[2].is_null = NULL;
  params2[2].num = 1;

  params2[3].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params2[3].buffer = &ts;
  params2[3].buffer_length = sizeof(int64_t);
  params2[3].length = &c4_len;
  params2[3].is_null = NULL;
  params2[3].num = 1;

  // composite key
  do_query(taos,
           "create table stmt_testdb_6.devices2(ts timestamp,device_id int COMPOSITE KEY,status binary(10),temperature "
           "float,humidity float);");
  do_query(taos,
           "insert into stmt_testdb_6.devices2 (ts,device_id,status,temperature,humidity) "
           "values(1591060628000000,1,'abc',1.0,1.1);");
  {
    printf("case 9 : composite key\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql =
        "update stmt_testdb_6.devices2 set temperature = ?,humidity = ? where device_id=? and ts=?"
        " and status IS NOT NULL";
    int code = taos_stmt_prepare(stmt, sql, 0);
    checkError(stmt, code);

    int           fieldNum = 0;
    TAOS_FIELD_E *pFields = NULL;

    code = taos_stmt_get_col_fields(stmt, &fieldNum, &pFields);
    checkError(stmt, code);
    ASSERT_EQ(fieldNum, 4);
    ASSERT_STREQ(pFields[0].name, "temperature");
    ASSERT_STREQ(pFields[1].name, "humidity");
    ASSERT_STREQ(pFields[2].name, "device_id");
    ASSERT_STREQ(pFields[3].name, "ts");
    taosMemoryFree(pFields);

    device_id = 1;
    code = taos_stmt_bind_param_batch(stmt, params2);
    checkError(stmt, code);

    code = taos_stmt_add_batch(stmt);
    checkError(stmt, code);

    code = taos_stmt_execute(stmt);
    checkError(stmt, code);

    TAOS_RES *result =
        taos_query(taos, "select temperature,humidity,device_id from stmt_testdb_6.devices2 where ts=1591060628000000");
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(taos_errno(result), 0);

    TAOS_ROW row = taos_fetch_row(result);
    ASSERT_NE(row, nullptr);

    int32_t actual_temperature = *(float *)row[0];
    ASSERT_EQ(actual_temperature, 10);

    float actual_humidity = *(float *)row[1];
    ASSERT_EQ(static_cast<int>(actual_humidity * 10), static_cast<int>(10.1 * 10));

    int32_t actual_device_id = *(int32_t *)row[2];
    ASSERT_EQ(actual_device_id, 1);

    taos_free_result(result);

    taos_stmt_close(stmt);
  }

  // no where composite pk
  {
    printf("case 10 : no where composite pk\n");
    TAOS_STMT *stmt = taos_stmt_init(taos);
    ASSERT_NE(stmt, nullptr);
    char *sql = "update stmt_testdb_6.devices2 set temperature = ?,humidity = ?,device_id=? where ts=?";
    code = taos_stmt_prepare(stmt, sql, 0);
    ASSERT_EQ(TSDB_CODE_PAR_SYNTAX_ERROR, code);
    ASSERT_STREQ("Cannot update primary key column 'device_id'", taos_stmt_errstr(stmt));

    taos_stmt_close(stmt);
  }

  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_6");
  taos_close(taos);
}

#pragma GCC diagnostic pop