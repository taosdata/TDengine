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

void checkRows(TAOS* pConn, const char* sql, int32_t expectedRows) {
  TAOS_RES* pRes = taos_query(pConn, sql);
  ASSERT_EQ(taos_errno(pRes), TSDB_CODE_SUCCESS);
  TAOS_ROW pRow = NULL;
  int      rows = 0;
  while ((pRow = taos_fetch_row(pRes)) != NULL) {
    rows++;
  }
  ASSERT_EQ(rows, expectedRows);
  taos_free_result(pRes);
}

void stmtAsyncQueryCb(void* param, TAOS_RES* pRes, int code) {
  int affected_rows = taos_affected_rows(pRes);
  return;
}

void getFieldsSuccess(TAOS* taos, const char* sql, TAOS_FIELD_ALL* expectedFields, int expectedFieldNum) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  int               code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  ASSERT_EQ(code, 0);
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
  int               code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);

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
  int               code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);

  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, NULL);
  ASSERT_EQ(code, 0);
  ASSERT_EQ(fieldNum, expectedFieldNum);
  taos_stmt2_free_fields(stmt, NULL);
  taos_stmt2_close(stmt);
}

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  ASSERT_EQ(code, 0);

  taos_free_result(result);
}

void do_stmt(TAOS* taos, TAOS_STMT2_OPTION* option, const char* sql, int CTB_NUMS, int ROW_NUMS, int CYC_NUMS,
             bool hastags, bool createTable) {
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos, "use db");

  TAOS_STMT2* stmt = taos_stmt2_init(taos, option);
  ASSERT_NE(stmt, nullptr);
  int code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);

  // tbname
  char** tbs = (char**)taosMemoryMalloc(CTB_NUMS * sizeof(char*));
  for (int i = 0; i < CTB_NUMS; i++) {
    tbs[i] = (char*)taosMemoryMalloc(sizeof(char) * 20);
    sprintf(tbs[i], "ctb_%d", i);
    if (createTable) {
      char* tmp = (char*)taosMemoryMalloc(sizeof(char) * 100);
      sprintf(tmp, "create table db.%s using db.stb tags(0, 'after')", tbs[i]);
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
    ASSERT_EQ(code, 0);

    // exec
    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, 0);

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

  checkRows(taos, "select * from db.stb", CYC_NUMS * ROW_NUMS * CTB_NUMS);
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

TEST(clientCase, driverInit_Test) {
  // taosInitGlobalCfg();
  //  taos_init();
}

TEST(stmt2Case, insert_stb_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db PRECISION 'ns'");
  do_query(taos,
           "create table db.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create table if not exists db.all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 "
      "tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");
  printf("support case \n");

  // case 1 : test super table
  {
    const char*    sql = "insert into db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
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
    const char*    sql = "insert into db.stb(ts,b,tbname) values(?,?,?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 3 : random order
  {
    const char*    sql = "insert into db.stb(tbname,ts,t2,b,t1) values(?,?,?,?,?)";
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
    const char*    sql = "insert into db.stb(ts,tbname,b,t2,t1) values(?,?,?,?,?)";
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
    const char*    sql = "insert into 'db'.'stb'(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
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
    do_query(taos, "use db");
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
    const char*    sql = "insert into db.stb(ts,tbname) values(?,?)";
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
    const char* sql = "insert into db.stb(t1,t2,ts,b,tbname) values(1,?,?,'abc',?)";
    printf("case 1dif : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  // case 2 : no pk
  {
    const char* sql = "insert into db.stb(b,tbname) values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 3 : no tbname and tag(not support bind)
  {
    const char* sql = "insert into db.stb(ts,b) values(?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 4 : no col and tag(not support bind)
  {
    const char* sql = "insert into db.stb(tbname) values(?)";
    printf("case 4 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 5 : no field name
  {
    const char* sql = "insert into db.stb(?,?,?,?,?)";
    printf("case 5 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 6 :  test super table not exist
  {
    const char* sql = "insert into db.nstb(?,?,?,?,?)";
    printf("case 6 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 7 :  no col
  {
    const char* sql = "insert into db.stb(t1,t2,tbname) values(?,?,?)";
    printf("case 7 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_INVALID_OPERATION);
  }

  // case 8 :   wrong para nums
  {
    const char* sql = "insert into db.stb(ts,b,tbname) values(?,?,?,?,?)";
    printf("case 8 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  // case 9 :   wrong simbol
  {
    const char* sql = "insert into db.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)";
    printf("case 9 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }

  taos_close(taos);
}

TEST(stmt2Case, insert_ctb_using_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db PRECISION 'ns'");
  do_query(taos,
           "create table db.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create table if not exists db.all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 "
      "tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");
  do_query(taos, "CREATE TABLE db.t0 USING db.stb (t1,t2) TAGS (7,'Cali');");

  printf("support case \n");
  // case 1 : test child table already exist
  {
    const char*    sql = "INSERT INTO db.t0(ts,b)using db.stb (t1,t2) TAGS(?,?) VALUES (?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 2 : insert clause
  {
    const char*    sql = "INSERT INTO db.? using db.stb (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
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
    const char*    sql = "INSERT INTO db.d1 using db.stb (t1,t2)TAGS(?,?) (ts,b)VALUES(?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 4 : random order
  {
    const char*    sql = "INSERT INTO db.? using db.stb (t2,t1)TAGS(?,?) (b,ts)VALUES(?,?)";
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
    const char*    sql = "insert into db.? using db.stb (t2)tags(?) (ts)values(?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
                                        {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 5 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 6 : insert into db.? using db.stb tags(?, ?) values(?,?)
  // no field name
  {
    const char*    sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
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
    const char*    sql = "insert into db.t0 (ts)values(?)";
    TAOS_FIELD_ALL expectedFields[1] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
    printf("case 7 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 1);
  }

  // case 8 : 'db' 'stb'
  {
    const char*    sql = "INSERT INTO 'db'.? using 'db'.'stb' (t1,t2) TAGS(?,?) (ts,b)VALUES(?,?)";
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
    do_query(taos, "use db");
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
    const char* sql = "INSERT INTO db.?(ts,b)using db.nstb (t1,t2) TAGS(?,?) VALUES (?,?)";
    printf("case 1 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 2 : no pk
  {
    const char* sql = "INSERT INTO db.?(ts,b)using db.nstb (t1,t2) TAGS(?,?) (n)VALUES (?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 3 : less param and no filed name
  {
    const char* sql = "INSERT INTO db.?(ts,b)using db.stb TAGS(?)VALUES (?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_SYNTAX_ERROR);
  }

  // case 4 :  none para for ctbname
  {
    const char* sql = "INSERT INTO db.d0 using db.stb values(?,?)";
    printf("case 4 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
  }

  // case 5 :  none para for ctbname
  {
    const char* sql = "insert into ! using db.stb tags(?, ?) values(?,?)";
    printf("case 5 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_TSC_SQL_SYNTAX_ERROR);
  }
  taos_close(taos);
}

TEST(stmt2Case, insert_ntb_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);

  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db PRECISION 'ms'");
  do_query(taos, "CREATE TABLE db.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
  do_query(
      taos,
      "create table if not exists db.all_ntb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 "
      "tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20));");

  printf("support case \n");

  // case 1 : test normal table no field name
  {
    const char*    sql = "INSERT INTO db.ntb VALUES(?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL},
                                        {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
                                        {"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL}};
    printf("case 1 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 2 : test random order
  {
    const char*    sql = "INSERT INTO db.ntb (ni,nb,nvc,nts)VALUES(?,?,?,?)";
    TAOS_FIELD_ALL expectedFields[4] = {{"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL},
                                        {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
                                        {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
                                        {"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 4);
  }

  // case 3 : less param
  {
    const char*    sql = "INSERT INTO db.ntb (nts)VALUES(?)";
    TAOS_FIELD_ALL expectedFields[1] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 0, 0, 8, TAOS_FIELD_COL}};
    printf("case 3 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 1);
  }

  // case 4 : test all types
  {
    const char*    sql = "insert into db.all_ntb values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
    const char* sql = "insert into db.? values(?,?)";
    printf("case 2 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_TABLE_NOT_EXIST);
  }

  // case 3 :  wrong para nums
  {
    const char* sql = "insert into db.ntb(nts,ni) values(?,?,?,?,?)";
    printf("case 3 : %s\n", sql);
    getFieldsError(taos, sql, TSDB_CODE_PAR_INVALID_COLUMNS_NUM);
  }
}

TEST(stmt2Case, select_get_fields_Test) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db PRECISION 'ns'");
  do_query(taos, "use db");
  do_query(taos, "CREATE TABLE db.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");
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

    code = taos_stmt2_prepare(stmt, "insert into 'db'.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)", 0);
    ASSERT_NE(stmt, nullptr);
    ASSERT_STREQ(((STscStmt2*)stmt)->db, "db");  // add in main TD-33332
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
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  { do_stmt(taos, &option, "insert into db.stb (tbname,ts,b,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true, true); }
  option = {0, true, true, stmtAsyncQueryCb, NULL};
  { do_stmt(taos, &option, "insert into db.? using db.stb tags(?,?) values(?,?)", 3, 3, 3, true, true); }
  option = {0, false, false, NULL, NULL};
  { do_stmt(taos, &option, "insert into stb (tbname,ts,b,t1,t2) values(?,?,?,?,?)", 3, 3, 3, true, false); }
  option = {0, true, true, NULL, NULL};
  { do_stmt(taos, &option, "insert into db.stb (tbname,ts,b) values(?,?,?)", 3, 3, 3, false, true); }

  taos_close(taos);
}

// TD-33417
TEST(stmt2Case, stmt2_insert_non_statndard) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists example_all_type_stmt1");
  do_query(taos, "create database example_all_type_stmt1");
  do_query(taos,
           "create table example_all_type_stmt1.stb1  (ts timestamp, int_col int,long_col bigint,double_col "
           "double,bool_col bool,binary_col binary(20),nchar_col nchar(20),varbinary_col varbinary(20),geometry_col "
           "geometry(200)) tags(int_tag int,long_tag bigint,double_tag double,bool_tag bool,binary_tag "
           "binary(20),nchar_tag nchar(20),varbinary_tag varbinary(20),geometry_tag geometry(200));");

  TAOS_STMT2_OPTION option = {0, false, false, NULL, NULL};

  // less cols and tags
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql = "INSERT INTO example_all_type_stmt1.stb1 (ts,int_tag,tbname)  VALUES (?,?,?)";
    int         code = taos_stmt2_prepare(stmt, sql, 0);
    ASSERT_EQ(code, 0);

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
      ASSERT_EQ(code, 0);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      ASSERT_EQ(code, 0);
    }

    checkRows(taos, "select * from example_all_type_stmt1.tb1", 6);
    checkRows(taos, "select * from example_all_type_stmt1.tb2", 6);
    checkRows(taos, "select * from example_all_type_stmt1.stb1", 12);
    taos_stmt2_close(stmt);
  }

  // disorder cols and tags
  {
    TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
    ASSERT_NE(stmt, nullptr);
    const char* sql =
        "INSERT INTO example_all_type_stmt1.stb1 (binary_tag,int_col,tbname,ts,int_tag)  VALUES (?,?,?,?,?)";
    int code = taos_stmt2_prepare(stmt, sql, 0);
    ASSERT_EQ(code, 0);

    int     tag_i = 0;
    int     tag_l = sizeof(int);
    int     tag_bl = 3;
    int64_t ts[2] = {1591060628000, 1591060628100};
    int     t64_len[2] = {sizeof(int64_t), sizeof(int64_t)};
    int     coli[2] = {1, 2};
    int     ilen[2] = {sizeof(int), sizeof(int)};
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
      ASSERT_EQ(code, 0);

      int affected_rows;
      taos_stmt2_exec(stmt, &affected_rows);
      ASSERT_EQ(code, 0);
    }

    checkRows(taos, "select * from example_all_type_stmt1.tb3", 6);
    checkRows(taos, "select * from example_all_type_stmt1.tb4", 6);
    checkRows(taos, "select * from example_all_type_stmt1.stb1", 24);
    taos_stmt2_close(stmt);
  }

  taos_close(taos);
}

TEST(stmt2Case, stmt2_query) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.stb (ts timestamp, b binary(10)) tags(t1 int, t2 binary(10))");
  do_query(taos,
           "insert into db.tb1 using db.stb tags(1,'abc') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628002, 'hij')");
  do_query(taos,
           "insert into db.tb2 using db.stb tags(2,'xyz') values(1591060628000, "
           "'abc'),(1591060628001,'def'),(1591060628002, 'hij')");
  do_query(taos, "use db");

  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql = "select * from db.stb where ts = ? and tbname = ?";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);

  int              t64_len[1] = {sizeof(int64_t)};
  int              b_len[1] = {3};
  int64_t          ts = 1591060628000;
  TAOS_STMT2_BIND  params[2] = {{TSDB_DATA_TYPE_TIMESTAMP, &ts, t64_len, NULL, 1},
                                {TSDB_DATA_TYPE_BINARY, (void*)"tb1", b_len, NULL, 1}};
  TAOS_STMT2_BIND* paramv = &params[0];
  TAOS_STMT2_BINDV bindv = {1, NULL, NULL, &paramv};
  code = taos_stmt2_bind_param(stmt, &bindv, -1);
  ASSERT_EQ(code, 0);

  taos_stmt2_exec(stmt, NULL);
  ASSERT_EQ(code, 0);

  TAOS_RES* pRes = taos_stmt2_result(stmt);
  ASSERT_NE(pRes, nullptr);

  int      getRecordCounts = 0;
  TAOS_ROW row;
  while ((row = taos_fetch_row(pRes))) {
    getRecordCounts++;
  }
  ASSERT_EQ(getRecordCounts, 1);
  // taos_free_result(pRes);

  taos_stmt2_close(stmt);
  taos_close(taos);
}

TEST(stmt2Case, stmt2_ntb_insert) {
  TAOS* taos = taos_connect("localhost", "root", "taosdata", "", 0);
  ASSERT_NE(taos, nullptr);
  TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
  do_query(taos, "drop database if exists db");
  do_query(taos, "create database db");
  do_query(taos, "create table db.ntb(ts timestamp, b binary(10))");
  do_query(taos, "use db");
  TAOS_STMT2* stmt = taos_stmt2_init(taos, &option);
  ASSERT_NE(stmt, nullptr);

  const char* sql = "insert into db.ntb values(?,?)";
  int         code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, 0);
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
    ASSERT_EQ(code, 0);

    code = taos_stmt2_exec(stmt, NULL);
    ASSERT_EQ(code, 0);
  }
  checkRows(taos, "select * from db.ntb", 9);

  taos_stmt2_close(stmt);
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

  const char* sql = "insert into db.ntb values(?,?)";
  code = taos_stmt2_prepare(stmt, sql, 0);
  ASSERT_EQ(code, TSDB_CODE_TSC_STMT_API_ERROR);
  ASSERT_STREQ(taos_stmt2_error(stmt), "Stmt API usage error");

  taos_stmt2_close(stmt);
  taos_close(taos);
}

#pragma GCC diagnostic pop
