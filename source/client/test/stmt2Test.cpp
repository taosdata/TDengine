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
void checkError(TAOS_STMT2* stmt, int code) {
  if (code != TSDB_CODE_SUCCESS) {
    STscStmt2* pStmt = (STscStmt2*)stmt;
    if (pStmt == nullptr || pStmt->sql.sqlStr == nullptr) {
      printf("stmt api error\n  stats : %d\n  errstr : %s\n", pStmt->sql.status, taos_stmt_errstr(stmt));
    } else {
      printf("stmt api error\n  sql : %s\n  stats : %d\n  errstr : %s\n", pStmt->sql.sqlStr, pStmt->sql.status,
             taos_stmt_errstr(stmt));
    }
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }
}

void stmtAsyncQueryCb(void* param, TAOS_RES* pRes, int code) {
  int affected_rows = taos_affected_rows(pRes);
  return;
}

void getFieldsSuccess(TAOS* taos, const char* sql, TAOS_FIELD_ALL* expectedFields, int expectedFieldNum) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  checkError(stmt, code);
  ASSERT_EQ(fieldNum, expectedFieldNum);

  for (int i = 0; i < fieldNum; i++) {
    ASSERT_STREQ(pFields[i].name, expectedFields[i].name);
    ASSERT_EQ(pFields[i].type, expectedFields[i].type);
    ASSERT_EQ(pFields[i].field_type, expectedFields[i].field_type);
    ASSERT_EQ(pFields[i].precision, expectedFields[i].precision);
    ASSERT_EQ(pFields[i].bytes, expectedFields[i].bytes);
    ASSERT_EQ(pFields[i].scale, expectedFields[i].scale);
  }
  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void getFieldsError(TAOS* taos, const char* sql, int errorCode) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, errorCode);
  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void getQueryFields(TAOS* taos, const char* sql, int expectedFieldNum) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
  checkError(stmt, code);
  ASSERT_EQ(fieldNum, expectedFieldNum);
  taos_stmt2_free_fields(stmt, NULL);
  taos_stmt2_close(stmt);
}

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
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

void do_stmt(TAOS* taos, TAOS_STMT2_OPTION* option, const char* sql, int CTB_NUMS, int ROW_NUMS, int CYC_NUMS,
             bool hastags, bool createTable) {
  printf("test sql : %s\n", sql);
  do_query(taos, "drop database if exists stmt2_testdb_1");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_1");
  do_query(taos, "create stable stmt2_testdb_1.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");

  TAOS_STMT2* stmt = taos_stmt2_init(taos, option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);
  int total_affected = 0;

  // tbname
  char** tbs = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)taosMemoryMalloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    if (createTable) {
      char* tmp = (char*)taosMemoryMalloc(sizeof(char) * 100);
      sprintf(tmp, "create table stmt2_testdb_1.%s using stmt2_testdb_1.stb tags(0, 'after')", tbs[i]);
      do_query(taos, tmp);
    }
  }
  for (int r = 0; r < CYC_NUMS; r++) {
    // col params
    int64_t** ts = (int64_t**)taosMemoryMalloc(CTB_NUMS * sizeof(int64_t*));
    char**    b = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
    int*      ts_len = (int*)taosMemoryMalloc(ROW_NUMS * sizeof(int));
    int*      b_len = (int*)taosMemoryMalloc(ROW_NUMS * sizeof(int));
    for (int i = 0; i < ROW_NUMS; i++) {
      ts_len[i] = sizeof(int64_t);
      b_len[i] = 1;
    }
    for (int i = 0; i < CTB_NUMS; i++) {
      ts[i] = (int64_t*)taosMemoryMalloc(ROW_NUMS * sizeof(int64_t));
      b[i] = (char*)taosMemoryMalloc(ROW_NUMS * sizeof(char));
      for (int j = 0; j < ROW_NUMS; j++) {
        ts[i][j] = 1591060628000 + r * 100000 + j;
        b[i][j] = 'a' + j;
      }
    }
    // tag params
    int t1 = 0;
    int t1len = sizeof(int);
    int t2len = 3;
    //   TAOS_STMT2_BIND* tagv[2] = {&tags[0][0], &tags[1][0]};

    // bind params
    TAOS_STMT2_BIND** paramv = (TAOS_STMT2_BIND**)taosMemoryMalloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
    TAOS_STMT2_BIND** tags = NULL;
    if (hastags) {
      tags = (TAOS_STMT2_BIND**)taosMemoryMalloc(CTB_NUMS * sizeof(TAOS_STMT2_BIND*));
      for (int i = 0; i < CTB_NUMS; i++) {
        // create tags
        tags[i] = (TAOS_STMT2_BIND*)taosMemoryMalloc(2 * sizeof(TAOS_STMT2_BIND));
        tags[i][0] = {TSDB_DATA_TYPE_INT, &t1, &t1len, NULL, 0};
        tags[i][1] = {TSDB_DATA_TYPE_BINARY, (void*)"after", &t2len, NULL, 0};
      }
    }

    for (int i = 0; i < CTB_NUMS; i++) {
      // create col params
      paramv[i] = (TAOS_STMT2_BIND*)taosMemoryMalloc(2 * sizeof(TAOS_STMT2_BIND));
      paramv[i][0] = {TSDB_DATA_TYPE_TIMESTAMP, &ts[i][0], &ts_len[0], NULL, ROW_NUMS};
      paramv[i][1] = {TSDB_DATA_TYPE_BINARY, &b[i][0], &b_len[0], NULL, ROW_NUMS};
    }
    // bind
    TAOS_STMT2_BINDV bindv = {CTB_NUMS, tbs, tags, paramv};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code);

    // exec
    int affected = 0;
    code = taos_stmt2_exec(stmt, &affected);
    total_affected += affected;
    checkError(stmt, code);

    for (int i = 0; i < CTB_NUMS; i++) {
      if (hastags) {
        taosMemoryFree(tags[i]);
      }
      taosMemoryFree(paramv[i]);
      taosMemoryFree(ts[i]);
      taosMemoryFree(b[i]);
    }
    taosMemoryFree(ts);
    taosMemoryFree(b);
    taosMemoryFree(ts_len);
    taosMemoryFree(b_len);
    taosMemoryFree(paramv);
    if (hastags) {
      taosMemoryFree(tags);
    }
  }
  if (option->asyncExecFn == NULL) {
    ASSERT_EQ(total_affected, CYC_NUMS * ROW_NUMS * CTB_NUMS);
  }
  for (int i = 0; i < CTB_NUMS; i++) {
    taosMemoryFree(tbs[i]);
  }
  taosMemoryFree(tbs);

  taos_stmt2_close(stmt);
}

}  // namespace

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(stmt2Case, stmt2_test_limit) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_7");
  do_query(taos, "create stable stmt2_testdb_7.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
  "insert into stmt2_testdb_7.tb2 using stmt2_testdb_7.stb tags(2,'xyz') values(1591060628000, "
  "'abc'),(1591060628001,'def'),(1591060628004, 'hij')");
  do_query(taos, "use stmt2_testdb_7");


  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};


  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);


  const char* sql = "select * from stmt2_testdb_7.tb2 where ts > ? and ts < ? limit ?";
  int code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);


  int t64_len[1] = {sizeof(int64_t)};
  int b_len[1] = {3};
  int x = 2;
  int x_len = sizeof(int);
  int64_t ts[2] = {1591060627000, 1591060628005};
  TAOS_STMT2_BIND params[3] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts[0], t64_len, NULL, 1},
  {TSDB_DATA_TYPE_TIMESTAMP, &ts[1], t64_len, NULL, 1},
  {TSDB_DATA_TYPE_INT, &x, &x_len, NULL, 1}};
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code);


  taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code);


  TAOS_RES* pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);


  int getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
  getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  taos_close(taos);
}


TEST(stmt2Case, insert_stb_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_2");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_2 PRECISION 'ns'");
  do_query(taos,
           "create stable stmt2_testdb_2.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create stable if not exists stmt2_testdb_2.all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 "
      "bigint, "
      "v6 tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");
  printf("support case \n");

  // case 1 : test super table
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  {
    // case 2 : no tag
    const char*    sql = "insert into stmt2_testdb_2.stb(ts,b,tbname) values(?,?,?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 3 : random order
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(tbname,ts,t2,b,t1) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 4 : random order 2
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(ts,tbname,b,t2,t1) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG}};
    printf("case 4 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 5 : 'db'.'stb'
  {
    const char*    sql = "insert into 'stmt2_testdb_2'.'stb'(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 5 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 6 : use db
  {
    do_query(taos, "use stmt2_testdb_2");
    const char*    sql = "insert into stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 6 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 7 : less param
  {
    const char*    sql = "insert into stmt2_testdb_2.stb(ts,tbname) values(?,?)";
    TAOS_FIELD_ALL expectedFields[2] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 7 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 2);
  }

  // case 8 : test all types
  {
    const char* sql =
        "insert into "
        "all_stb(tbname,tts,tv1,tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9,tv10,tv11,tv12,tv13,tv14,tv15,ts,v1,v2,v3,v4,v5,v6,v7,"
        "v8,v9,v10,"
        "v11,v12,v13,v14,v15) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[33] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                         {"tts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_TAG},
                                         {"tv1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_TAG},
                                         {"tv15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_TAG},
                                         {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                         {"v1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_COL},
                                         {"v15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_COL}};
    printf("case 8 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 33);
  }

  // not support case
  printf("not support case \n");

  // case 1 : add in main TD-33353
  {
    const char* sql = "insert into stmt2_testdb_2.stb(t1,t2,ts,b,tbname) values(1,?,?,'abc',?)";
    printf("case 1dif : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  // case 2 : no pk
  {
    const char* sql = "insert into stmt2_testdb_2.stb(b,tbname) values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 3 : no tbname and tag(not support bind)
  {
    const char* sql = "insert into stmt2_testdb_2.stb(ts,b) values(?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 4 : no col and tag(not support bind)
  {
    const char* sql = "insert into stmt2_testdb_2.stb(tbname) values(?)";
    printf("case 4 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 5 : no field name
  {
    const char* sql = "insert into stmt2_testdb_2.stb(?,?,?,?,?)";
    printf("case 5 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 6 :  test super table not exist
  {
    const char* sql = "insert into stmt2_testdb_2.nstb(?,?,?,?,?)";
    printf("case 6 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 7 :  no col
  {
    const char* sql = "insert into stmt2_testdb_2.stb(t1,t2,tbname) values(?,?,?)";
    printf("case 7 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 8 :   wrong para nums
  {
    const char* sql = "insert into stmt2_testdb_2.stb(ts,b,tbname) values(?,?,?,?,?)";
    printf("case 8 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  // case 9 :   wrong simbol
  {
    const char* sql = "insert into stmt2_testdb_2.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)";
    printf("case 9 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  do_query(taos, "drop database if exists stmt2_testdb_2");
  taos_close(taos);
}

TEST(stmt2Case, insert_ctb_using_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_3");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_3 PRECISION 'ns'");
  do_query(taos,
           "create stable stmt2_testdb_3.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create stable if not exists stmt2_testdb_3.all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 "
      "bigint, "
      "v6 tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");
  do_query(taos, "CREATE TABLE stmt2_testdb_3.t0 USING stmt2_testdb_3.stb (t1,t2) TAGS (7,'Cali');");

  printf("support case \n");
  // case 1 : test child table already exist
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.t0(ts,b)using stmt2_testdb_3.stb (t1,t2) TAGS(?,?) VALUES (?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 2 : insert clause
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.stb (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 3 : insert child table not exist
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.d1 using stmt2_testdb_3.stb (t1,t2)TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 4 : random order
  {
    const char*    sql = "INSERT INTO stmt2_testdb_3.? using stmt2_testdb_3.stb (t2,t1)TAGS(?,?) (b,ts)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 4 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 5 : less para
  {
    const char*    sql = "insert into stmt2_testdb_3.? using stmt2_testdb_3.stb (t2)tags(?) (ts)values(?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 5 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 6 : insert into db.? using db.stb tags(?, ?) values(?,?)
  // no field name
  {
    const char*    sql = "insert into stmt2_testdb_3.? using stmt2_testdb_3.stb tags(?, ?) values(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 6 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 7 : insert into db.d0 (ts)values(?)
  //  less para
  {
    const char*    sql = "insert into stmt2_testdb_3.t0 (ts)values(?)";
    TAOS_FIELD_ALL expectedFields[1] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 7 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 1);
  }

  // case 8 : 'db' 'stb'
  {
    const char* sql = "INSERT INTO 'stmt2_testdb_3'.? using 'stmt2_testdb_3'.'stb' (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 8 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 9 : use db
  {
    do_query(taos, "use stmt2_testdb_3");
    const char*    sql = "INSERT INTO ? using stb (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 9 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 5);
  }

  // case 10 : test all types
  {
    const char* sql =
        "insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[33] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                         {"tts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_TAG},
                                         {"tv1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_TAG},
                                         {"tv7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_TAG},
                                         {"tv8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_TAG},
                                         {"tv11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_TAG},
                                         {"tv12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_TAG},
                                         {"tv14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_TAG},
                                         {"tv15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_TAG},
                                         {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                         {"v1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_COL},
                                         {"v15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_COL}};
    printf("case 10 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 33);
  }
  printf("not support case \n");

  // case 1 : test super table not exist
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.?(ts,b)using stmt2_testdb_3.nstb (t1,t2) TAGS(?,?) VALUES (?,?)";
    printf("case 1 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 2 : no pk
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.?(ts,b)using stmt2_testdb_3.nstb (t1,t2) TAGS(?,?) (n)VALUES (?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 3 : less param and no filed name
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.?(ts,b)using stmt2_testdb_3.stb TAGS(?)VALUES (?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 4 :  none para for ctbname
  {
    const char* sql = "INSERT INTO stmt2_testdb_3.d0 using stmt2_testdb_3.stb values(?,?)";
    printf("case 4 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
  }

  // case 5 :  none para for ctbname
  {
    const char* sql = "insert into ! using stmt2_testdb_3.stb tags(?, ?) values(?,?)";
    printf("case 5 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
  }

  do_query(taos, "drop database if exists stmt2_testdb_3");
  taos_close(taos);
}

TEST(stmt2Case, insert_ntb_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_4");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_4 PRECISION 'ms'");
  do_query(taos, "CREATE TABLE stmt2_testdb_4.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
  do_query(
      taos,
      "create table if not exists stmt2_testdb_4.all_ntb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 "
      "bigint, v6 tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 "
      "double, v12 binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20));");

  printf("support case \n");

  // case 1 : test normal table no field name
  {
    const char*    sql = "INSERT INTO stmt2_testdb_4.ntb VALUES(?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL},
                                        {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
                                        {"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 2 : test random order
  {
    const char*    sql = "INSERT INTO stmt2_testdb_4.ntb (ni,nb,nvc,nts)VALUES(?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                        {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
                                        {"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 3 : less param
  {
    const char*    sql = "INSERT INTO stmt2_testdb_4.ntb (nts)VALUES(?)";
    TAOS_FIELD_ALL expectedFields[1] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 1);
  }

  // case 4 : test all types
  {
    const char*    sql = "insert into stmt2_testdb_4.all_ntb values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[16] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v1", TSDB_DATA_TYPE_BOOL, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v2", TSDB_DATA_TYPE_TINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v3", TSDB_DATA_TYPE_SMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v4", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v5", TSDB_DATA_TYPE_BIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v6", TSDB_DATA_TYPE_UTINYINT, 0, 0, 1, TAOS_FIELD_COL},
                                         {"v7", TSDB_DATA_TYPE_USMALLINT, 0, 0, 2, TAOS_FIELD_COL},
                                         {"v8", TSDB_DATA_TYPE_UINT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v9", TSDB_DATA_TYPE_UBIGINT, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v10", TSDB_DATA_TYPE_FLOAT, 0, 0, 4, TAOS_FIELD_COL},
                                         {"v11", TSDB_DATA_TYPE_DOUBLE, 0, 0, 8, TAOS_FIELD_COL},
                                         {"v12", TSDB_DATA_TYPE_VARCHAR, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v13", TSDB_DATA_TYPE_VARBINARY, 0, 0, 22, TAOS_FIELD_COL},
                                         {"v14", TSDB_DATA_TYPE_GEOMETRY, 0, 0, 102, TAOS_FIELD_COL},
                                         {"v15", TSDB_DATA_TYPE_NCHAR, 0, 0, 82, TAOS_FIELD_COL}};
    printf("case 4 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 16);
  }

  printf("not support case \n");

  // case 1 :  wrong db
  {
    const char* sql = "insert into ntb values(?,?,?,?)";
    printf("case 1 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 2 :  normal table must have tbnam
  {
    const char* sql = "insert into stmt2_testdb_4.? values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_STMT_TBNAME_ERROR);
  }

  // case 3 :  wrong para nums
  {
    const char* sql = "insert into stmt2_testdb_4.ntb(nts,ni) values(?,?,?,?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  do_query(taos, "drop database if exists stmt2_testdb_4");
  taos_close(taos);
}

TEST(stmt2Case, select_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_5");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_5 PRECISION 'ns'");
  do_query(taos, "use stmt2_testdb_5");
  do_query(taos, "CREATE TABLE stmt2_testdb_5.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
  {
    // case 1 :
    const char* sql = "select * from ntb where ts = ?";
    printf("case 1 : %s\n", sql);
    getQueryFields(taos, sql, 1);
  }

  {
    // case 2 :
    const char* sql = "select * from ntb where ts = ? and b = ?";
    printf("case 2 : %s\n", sql);
    getQueryFields(taos, sql, 2);
  }

  {
    // case 3 :
    const char* sql = "select * from ? where ts = ?";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  do_query(taos, "drop database if exists stmt2_testdb_5");
  taos_close(taos);
}

TEST(stmt2Case, get_fields_error_Test) {
  // case 1 :
  {
    printf("case 1 : NULL param \n");
    int code = taos_stmt2_get_fields(NULL, NULL, NULL);
    ASSERT_EQ(code, TSDB_CODE_INVALID_PARA);
  }
}

TEST(stmt2Case, stmt2_init_prepare_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  {
    (void)taos_stmt2_init(NULL, NULL);
    ASSERT_EQ(terrno, TSDB_CODE_INVALID_PARA);
    terrno = 0;
  }

  {
    (void)taos_stmt2_prepare(NULL, NULL, 0);
    ASSERT_EQ(terrno, TSDB_CODE_INVALID_PARA);
    terrno = 0;
  }

  {
    TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_EQ(terrno, 0);
    ASSERT_NE(stmt, nullptr);
    int code = taos_stmt2_prepare(stmt, "wrong sql", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_EQ(((STscStmt2*)stmt)->db, nullptr);

    code = taos_stmt2_prepare(stmt, "insert into 'stmt2_testdb_5'.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_STREQ(((STscStmt2*)stmt)->db, "stmt2_testdb_5");  // add in main TD-33332
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2_OPTION option = {0, true, false, NULL, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    taos_stmt2_close(stmt);
  }

  {
    TAOS_STMT2_OPTION option = {0, true, true, stmtAsyncQueryCb, NULL};
    TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    taos_stmt2_close(stmt);
  }
  taos_close(taos);
}

TEST(stmt2Case, stmt2_stb_insert) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  // normal
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  {
    do_stmt(taos, &option, "insert into `stmt2_testdb_1`.`stb` (tbname,ts,b,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true,
            true);
  }
  {
    do_stmt(taos, &option, "insert into `stmt2_testdb_1`.? using `stmt2_testdb_1`.`stb` tags(?,?) values(?,?)", 3, 3, 3,
            true, true);
  }

  // async
  option = {0, true, true, stmtAsyncQueryCb, NULL};
  {
    do_stmt(taos, &option, "insert into stmt2_testdb_1.stb (ts,b,tbname,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true, true);
  }
  {
    do_stmt(taos, &option, "insert into stmt2_testdb_1.? using stmt2_testdb_1.stb (t1,t2)tags(?,?) (ts,b)values(?,?)",
            3, 3, 3, true, true);
  }
  // { do_stmt(taos, &option, "insert into db.? values(?,?)", 3, 3, 3, false, true); }

  // interlace = 0 & use db]
  do_query(taos, "use stmt2_testdb_1");
  option = {0, false, false, NULL, NULL};
  { do_stmt(taos, &option, "insert into stb (tbname,ts,b) values(?,?,?)", 3, 3, 3, false, true); }
  { do_stmt(taos, &option, "insert into ? using stb (t1,t2)tags(?,?) (ts,b)values(?,?)", 3, 3, 3, true, true); }
  { do_stmt(taos, &option, "insert into ? values(?,?)", 3, 3, 3, false, true); }

  // interlace = 1
  option = {0, true, true, stmtAsyncQueryCb, NULL};
  { do_stmt(taos, &option, "insert into ? values(?,?)", 3, 3, 3, false, true); }
  option = {0, true, true, NULL, NULL};
  { do_stmt(taos, &option, "insert into ? values(?,?)", 3, 3, 3, false, true); }

  do_query(taos, "drop database if exists stmt2_testdb_1");
  taos_close(taos);
}

// TD-33417
TEST(stmt2Case, stmt2_insert_non_statndard) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_6");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_6");
  do_query(taos,
           "create stable stmt2_testdb_6.stb1  (ts timestamp, int_col int,long_col bigint,double_col "
           "double,bool_col bool,binary_col binary(20),nchar_col nchar(20),varbinary_col varbinary(20),geometry_col "
           "geometry(200)) tags(int_tag int,long_tag bigint,double_tag double,bool_tag bool,binary_tag "
           "binary(20),nchar_tag nchar(20),varbinary_tag varbinary(20),geometry_tag geometry(200));");

  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

  // less cols and tags
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "INSERT INTO stmt2_testdb_6.stb1 (ts,int_tag,tbname)  VALUES (?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code);
    int total_affect_rows = 0;

    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int64_t ts[2] = {1591060628000, 1591060628100};
    for (int i = 0; i < 3; i++) {
      ts[0] += 1000;
      ts[1] += 1000;

      TAOS_STMT2_BIND tags1 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
      TAOS_STMT2_BIND tags2 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
      TAOS_STMT2_BIND params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};
      TAOS_STMT2_BIND params2 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};

      TAOS_STMT2_BIND* tagv[2] = {&tags1, &tags2};
      TAOS_STMT2_BIND* paramv[2] = {&params1, &params2};
      char*            tbname[2] = {"tb1", "tb2"};
      TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      total_affect_rows += affected_rows;

      checkError(stmt, code);
    }

    ASSERT_EQ(total_affect_rows, 12);
    taos_stmt2_close(stmt);
  }

  // disorder cols and tags
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "INSERT INTO stmt2_testdb_6.stb1 (binary_tag,int_col,tbname,ts,int_tag)  VALUES (?,?,?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    checkError(stmt, code);

    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int     tag_bl = 3;
    int64_t ts[2] = {1591060628000, 1591060628100};
    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     coli[2] = {1, 2};
    int     ilen[2] = {sizeof(int), sizeof(int)};
    int     total_affect_rows = 0;
    for (int i = 0; i < 3; i++) {
      ts[0] += 1000;
      ts[1] += 1000;

      TAOS_STMT2_BIND tags1[2] = {{TSDB_DATA_TYPE_BINARY, (void*)"abc", &tag_bl, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1}};
      TAOS_STMT2_BIND tags2[2] = {{TSDB_DATA_TYPE_BINARY, (void*)"abc", &tag_bl, NULL, 1},
                                  {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1}};
      TAOS_STMT2_BIND params1[2] = {{TSDB_DATA_TYPE_INT, &coli, &ilen[0], NULL, 2},
                                    {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2}};
      TAOS_STMT2_BIND params2[2] = {{TSDB_DATA_TYPE_INT, &coli, &ilen[0], NULL, 2},
                                    {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2}};

      TAOS_STMT2_BIND* tagv[2] = {&tags1[0], &tags2[0]};
      TAOS_STMT2_BIND* paramv[2] = {&params1[0], &params2[0]};
      char*            tbname[2] = {"tb3", "tb4"};
      TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
      code = taos_stmt2_bind_param(stmt, &bindv, -1);
      checkError(stmt, code);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      total_affect_rows += affected_rows;
      checkError(stmt, code);
    }
    ASSERT_EQ(total_affect_rows, 12);
    taos_stmt2_close(stmt);
  }

  do_query(taos, "drop database if exists stmt2_testdb_6");
  taos_close(taos);
}

// TD-33419
TEST(stmt2Case, stmt2_insert_db) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_12");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_12");
  do_query(taos,
           "create stable `stmt2_testdb_12`.`stb1`  (ts timestamp, int_col int,long_col bigint,double_col "
           "double,bool_col bool,binary_col binary(20),nchar_col nchar(20),varbinary_col varbinary(20),geometry_col "
           "geometry(200)) tags(int_tag int,long_tag bigint,double_tag double,bool_tag bool,binary_tag "
           "binary(20),nchar_tag nchar(20),varbinary_tag varbinary(20),geometry_tag geometry(200));");

  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  const char* sql = "INSERT INTO `stmt2_testdb_12`.`stb1` (ts,int_tag,tbname)  VALUES (?,?,?)";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
  int     tag_i = 0;
  int     tag_l = sizeof(int);
  int64_t ts[2] = {1591060628000, 1591060628100};
  int     total_affect_rows = 0;
  for (int i = 0; i < 3; i++) {
    ts[0] += 1000;
    ts[1] += 1000;

    TAOS_STMT2_BIND tags1 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
    TAOS_STMT2_BIND tags2 = {TSDB_DATA_TYPE_INT, &tag_i, &tag_l, NULL, 1};
    TAOS_STMT2_BIND params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};
    TAOS_STMT2_BIND params2 = {TSDB_DATA_TYPE_TIMESTAMP, &ts, &t64_len[0], NULL, 2};

    TAOS_STMT2_BIND* tagv[2] = {&tags1, &tags2};
    TAOS_STMT2_BIND* paramv[2] = {&params1, &params2};
    char*            tbname[2] = {"tb1", "tb2"};
    TAOS_STMT2_BINDV bindv = {2, &tbname[0], &tagv[0], &paramv[0]};
    code = taos_stmt2_bind_param(stmt, &bindv, -1);
    checkError(stmt, code);

    int affected_rows;
    taos_stmt2_exec(stmt, &affected_rows);
    total_affect_rows += affected_rows;
    checkError(stmt, code);
  }

  ASSERT_EQ(total_affect_rows, 12);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_12");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_query) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_7");
  do_query(taos, "create stable stmt2_testdb_7.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into stmt2_testdb_7.tb1 using stmt2_testdb_7.stb tags(1,'abc') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628002, 'hij')");
  do_query(taos,
           "insert into stmt2_testdb_7.tb2 using stmt2_testdb_7.stb tags(2,'xyz') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628004, 'hij')");
  do_query(taos, "use stmt2_testdb_7");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql = "select * from stmt2_testdb_7.stb where ts = ?";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int              t64_len[1] = {sizeof(int64_t)};
  int              b_len[1] = {3};
  int64_t          ts = 1591060628000;
  TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, t64_len, NULL, 1};
  TAOS_STMT2_BIND* paramv = &params;
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code);

  taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code);

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);

  int getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 2);
  // test 1 result
  ts = 1591060628004;
  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts, t64_len, NULL, 1};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code);

  taos_stmt2_exec(stmt, NULL);
  checkError(stmt, code);

  pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);

  getRecordCounts = 0;
  while ((taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 1);
  // taos_free_result(pRes);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_7");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_ntb_insert) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  do_query(taos, "drop database if exists stmt2_testdb_8");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_8");
  do_query(taos, "create table stmt2_testdb_8.ntb(ts timestamp, b binary(10))");
  do_query(taos, "use stmt2_testdb_8");
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  int total_affected_rows = 0;

  const char* sql = "insert into stmt2_testdb_8.ntb values(?,?)";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);
  for (int i = 0; i < 3; i++) {
    int64_t ts[3] = {1591060628000 + i * 3, 1591060628001 + i * 3, 1591060628002 + i * 3};
    int     t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};
    int     b_len[3] = {5, 5, 5};

    TAOS_STMT2_BIND  params1 = {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 3};
    TAOS_STMT2_BIND  params2 = {TSDB_DATA_TYPE_BINARY, (void*)"abcdefghijklmnopqrstuvwxyz", &b_len[0], NULL, 3};
    TAOS_STMT2_BIND* paramv1 = &params1;
    TAOS_STMT2_BIND* paramv2 = &params2;

    TAOS_STMT2_BINDV bindv1 = {1, NULL, NULL, &paramv1};
    TAOS_STMT2_BINDV bindv2 = {1, NULL, NULL, &paramv2};

    code = taos_stmt2_bind_param(stmt, &bindv1, 0);
    code = taos_stmt2_bind_param(stmt, &bindv2, 1);
    checkError(stmt, code);

    int affected_rows;
    code = taos_stmt2_exec(stmt, &affected_rows);
    total_affected_rows += affected_rows;
    checkError(stmt, code);
  }
  ASSERT_EQ(total_affected_rows, 9);

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_8");
  taos_close(taos);
}

TEST(stmt2Case, stmt2_status_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);

  int64_t ts[3] = {1591060628000, 1591060628001, 1591060628002};
  int     t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};

  TAOS_STMT2_BIND  params = {TSDB_DATA_TYPE_TIMESTAMP, &ts[0], &t64_len[0], NULL, 3};
  TAOS_STMT2_BIND* paramv = &params;
  TAOS_STMT2_BINDV bindv1 = {1, NULL, NULL, &paramv};

  int code = taos_stmt2_bind_param(stmt, &bindv1, 0);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_BIND_NUMBER_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "bind number out of range or not match");

  code = taos_stmt2_exec(stmt, NULL);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "Stmt API usage error");

  const char* sql = "insert into stmt2_testdb_9.ntb values(?,?)";
  code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "Stmt API usage error");

  taos_stmt2_close(stmt);
  taos_close(taos);
}

TEST(stmt2Case, stmt2_nchar) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  do_query(taos, "drop database if exists stmt2_testdb_10;");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_10;");
  do_query(taos, "use stmt2_testdb_10;");
  do_query(taos,
           "create table m1 (ts timestamp, blob2 nchar(10), blob nchar(10),blob3 nchar(10),blob4 nchar(10),blob5 "
           "nchar(10))");

  // insert 10 records
  struct {
    int64_t ts[10];
    char    blob[10][1];
    char    blob2[10][1];
    char    blob3[10][1];
    char    blob4[10][1];
    char    blob5[10][1];

  } v;

  int32_t* t64_len = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len2 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len3 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len4 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);
  int32_t* blob_len5 = (int32_t*)taosMemMalloc(sizeof(int32_t) * 10);

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  TAOS_STMT2_BIND params[10];
  char            is_null[10] = {0};

  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  // params[0].buffer_length = sizeof(v.ts[0]);
  params[0].buffer = v.ts;
  params[0].length = t64_len;
  params[0].is_null = is_null;
  params[0].num = 10;

  params[1].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[8].buffer_length = sizeof(v.blob2[0]);
  params[1].buffer = v.blob2;
  params[1].length = blob_len2;
  params[1].is_null = is_null;
  params[1].num = 10;

  params[2].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[9].buffer_length = sizeof(v.blob[0]);
  params[2].buffer = v.blob3;
  params[2].length = blob_len;
  params[2].is_null = is_null;
  params[2].num = 10;

  params[3].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[9].buffer_length = sizeof(v.blob[0]);
  params[3].buffer = v.blob4;
  params[3].length = blob_len;
  params[3].is_null = is_null;
  params[3].num = 10;

  params[4].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[9].buffer_length = sizeof(v.blob[0]);
  params[4].buffer = v.blob;
  params[4].length = blob_len;
  params[4].is_null = is_null;
  params[4].num = 10;

  params[5].buffer_type = TSDB_DATA_TYPE_NCHAR;
  // params[9].buffer_length = sizeof(v.blob[0]);
  params[5].buffer = v.blob5;
  params[5].length = blob_len;
  params[5].is_null = is_null;
  params[5].num = 10;

  int code = taos_stmt2_prepare(stmt, "insert into ? (ts, blob2, blob, blob3, blob4, blob5) values(?,?,?,?,?,?)", 0);
  checkError(stmt, code);

  int64_t ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    is_null[i] = 0;

    v.ts[i] = ts++;

    v.blob[i][0] = 'a' + i;
    v.blob2[i][0] = 'f' + i;
    v.blob3[i][0] = 't' + i;
    v.blob4[i][0] = 'A' + i;
    v.blob5[i][0] = 'G' + i;

    blob_len[i] = sizeof(char);
    blob_len2[i] = sizeof(char);
    blob_len3[i] = sizeof(char);
    blob_len4[i] = sizeof(char);
    blob_len5[i] = sizeof(char);
  }

  char*            tbname = "m1";
  TAOS_STMT2_BIND* bind_cols[1] = {&params[0]};
  TAOS_STMT2_BINDV bindv = {1, &tbname, NULL, &bind_cols[0]};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code);

  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code);
  ASSERT_EQ(affected_rows, 10);

  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_10;");
  taos_close(taos);
  taosMemoryFree(blob_len);
  taosMemoryFree(blob_len2);
  taosMemoryFree(blob_len5);
  taosMemoryFree(blob_len3);
  taosMemoryFree(blob_len4);
}

TEST(stmt2Case, all_type) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists stmt2_testdb_11");
  do_query(taos, "create database IF NOT EXISTS stmt2_testdb_11");
  do_query(
      taos,
      "create stable stmt2_testdb_11.stb(ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 binary(8), c6 "
      "smallint, c7 "
      "tinyint, c8 bool, c9 nchar(8), c10 geometry(256))TAGS(tts timestamp, t1 int, t2 bigint, t3 float, t4 double, t5 "
      "binary(8), t6 smallint, t7 tinyint, t8 bool, t9 nchar(8), t10 geometry(256))");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  int       code = 0;
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
  TAOS_STMT2_BIND params[11];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].length = (int32_t*)&v_len.c1;
  params[0].buffer = &v.c1;
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_INT;
  params[1].buffer = &v.c2;
  params[1].length = (int32_t*)&v_len.c2;
  params[1].is_null = NULL;
  params[1].num = 1;

  params[2].buffer_type = TSDB_DATA_TYPE_BIGINT;
  params[2].buffer = &v.c3;
  params[2].length = (int32_t*)&v_len.c3;
  params[2].is_null = NULL;
  params[2].num = 1;

  params[3].buffer_type = TSDB_DATA_TYPE_FLOAT;
  params[3].buffer = &v.c4;
  params[3].length = (int32_t*)&v_len.c4;
  params[3].is_null = NULL;
  params[3].num = 1;

  params[4].buffer_type = TSDB_DATA_TYPE_DOUBLE;
  params[4].buffer = &v.c5;
  params[4].length = (int32_t*)&v_len.c5;
  params[4].is_null = NULL;
  params[4].num = 1;

  params[5].buffer_type = TSDB_DATA_TYPE_BINARY;
  params[5].buffer = &v.c6;
  params[5].length = (int32_t*)&v_len.c6;
  params[5].is_null = NULL;
  params[5].num = 1;

  params[6].buffer_type = TSDB_DATA_TYPE_SMALLINT;
  params[6].buffer = &v.c7;
  params[6].length = (int32_t*)&v_len.c7;
  params[6].is_null = NULL;
  params[6].num = 1;

  params[7].buffer_type = TSDB_DATA_TYPE_TINYINT;
  params[7].buffer = &v.c8;
  params[7].length = (int32_t*)&v_len.c8;
  params[7].is_null = NULL;
  params[7].num = 1;

  params[8].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[8].buffer = &v.c9;
  params[8].length = (int32_t*)&v_len.c9;
  params[8].is_null = NULL;
  params[8].num = 1;

  params[9].buffer_type = TSDB_DATA_TYPE_NCHAR;
  params[9].buffer = &v.c10;
  params[9].length = (int32_t*)&v_len.c10;
  params[9].is_null = NULL;
  params[9].num = 1;

  unsigned char* outputGeom1;
  size_t         size1;
  initCtxMakePoint();
  code = doMakePoint(1.000, 2.000, &outputGeom1, &size1);
  checkError(stmt, code);
  params[10].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[10].buffer = outputGeom1;
  params[10].length = (int32_t*)&size1;
  params[10].is_null = NULL;
  params[10].num = 1;

  char* stmt_sql = "insert into stmt2_testdb_11.? using stb tags(?,?,?,?,?,?,?,?,?,?,?)values (?,?,?,?,?,?,?,?,?,?,?)";
  code = taos_stmt2_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code);

  char*            tbname[1] = {"tb1"};
  TAOS_STMT2_BIND* tags = &params[0];
  TAOS_STMT2_BIND* cols = &params[0];
  TAOS_STMT2_BINDV bindv = {1, &tbname[0], &tags, &cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code);

  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code);
  ASSERT_EQ(affected_rows, 1);

  geosFreeBuffer(outputGeom1);
  taos_stmt2_close(stmt);
  do_query(taos, "drop database if exists stmt2_testdb_11");
  taos_close(taos);
}

TEST(stmt2Case, geometry) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_13");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_13");
  do_query(taos, "CREATE TABLE stmt2_testdb_13.tb1(ts timestamp,c1 geometry(256))");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  unsigned char wkb1[] = {
      // 1
      0x01,                                            // 字节顺序：小端字节序
      0x01, 0x00, 0x00, 0x00,                          // 几何类型：Point (1)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,  // p1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,  // p2
                                                       // 2
      0x01, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0xf0, 0x3f,
      // 3
      0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x40};

  // unsigned char* wkb_all[3]{&wkb1[0], &wkb2[0], &wkb3[0]};
  int32_t wkb_len[3] = {21, 61, 41};

  int64_t ts[3] = {1591060628000, 1591060628001, 1591060628002};
  int32_t t64_len[3] = {sizeof(int64_t), sizeof(int64_t), sizeof(int64_t)};

  TAOS_STMT2_BIND params[2];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer = &ts[0];
  params[0].length = &t64_len[0];
  params[0].is_null = NULL;
  params[0].num = 3;

  params[1].buffer_type = TSDB_DATA_TYPE_GEOMETRY;
  params[1].buffer = &wkb1[0];
  params[1].length = &wkb_len[0];
  params[1].is_null = NULL;
  params[1].num = 3;

  char* stmt_sql = "insert into stmt2_testdb_13.tb1 (ts,c1)values(?,?)";
  int   code = taos_stmt2_prepare(stmt, stmt_sql, 0);
  checkError(stmt, code);

  TAOS_STMT2_BIND* cols = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &cols};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  checkError(stmt, code);

  int affected_rows;
  code = taos_stmt2_exec(stmt, &affected_rows);
  checkError(stmt, code);
  ASSERT_EQ(affected_rows, 3);

  // test wrong wkb input
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
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  ASSERT_EQ(code, TSDB_CODE_FUNC_FUNTION_PARA_VALUE);

  taos_stmt2_close(stmt);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_13");
  taos_close(taos);
}

// TD-33582
TEST(stmt2Case, errcode) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "DROP DATABASE IF EXISTS stmt2_testdb_14");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt2_testdb_14");
  do_query(taos, "use stmt2_testdb_14");

  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);
  char* sql = "select * from t where ts > ? and name = ? foo = ?";
  int   code = taos_stmt2_prepare(stmt, sql, 0);
  checkError(stmt, code);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, TSDB_CODE_PAR_SYNTAX_ERROR);

  // get fail dont influence the next stmt prepare
  sql = "nsert into ? (ts, name) values (?, ?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  checkError(stmt, code);
}
#pragma GCC diagnostic pop
