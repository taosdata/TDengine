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

#include "executor.h"
#include "taos.h"

namespace {
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

void getQueryFields(TAOS* taos, const char* sql) {
  TAOS_STMT2_OPTION option = {0};
  TAOS_STMT2*       stmt = taos_stmt2_init(taos, &option);
  int               code = taos_stmt2_prepare(stmt, sql, 0);
  if (code != 0) {
    printf("failed to execute taos_stmt2_prepare. error:%s\n", taos_stmt2_error(stmt));
    taos_stmt2_close(stmt);
    return;
  }
  int             fieldNum = 0;
  TAOS_FIELD_ALL* pFields = NULL;
  code = taos_stmt2_get_fields(stmt, &fieldNum, &pFields);
  if (code != 0) {
    printf("failed get col,ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_stmt2_error(stmt));
  } else {
    printf("bind nums:%d\n", fieldNum);
  }
  printf("====================================\n");
  taos_stmt2_free_fields(stmt, pFields);
  taos_stmt2_close(stmt);
}

void do_query(TAOS* taos, const char* sql) {
  TAOS_RES* result = taos_query(taos, sql);
  int       code = taos_errno(result);
  ASSERT_EQ(code, 0);

  taos_free_result(result);
}

// void do_stmt(TAOS* taos) {

//   printf("=================error test===================\n");

//   // case 14 :  INSERT INTO db.d0 using db.stb values(?,?)
//   // none para for ctbname
//   sql = "INSERT INTO db.d0 using db.stb values(?,?)";
//   printf("case 14 (no tags error): %s\n", sql);
//   getFields(taos, sql);

//   // case 15 :  insert into db.stb(t1,t2,tbname) values(?,?,?)
//   // no value
//   sql = "insert into db.stb(t1,t2,tbname) values(?,?,?)";
//   printf("case 15 (no PK error): %s\n", sql);
//   getFields(taos, sql);

//   // case 16 : insert into db.stb(ts,b,tbname) values(?,?,?,?,?)
//   //  wrong para nums
//   sql = "insert into db.stb(ts,b,tbname) values(?,?,?,?,?)";
//   printf("case 16 (wrong para nums): %s\n", sql);
//   getFields(taos, sql);

//   // case 17 : insert into db.? values(?,?)
//   // normal table must have tbnam
//   sql = "insert into db.? values(?,?)";
//   printf("case 17 (normal table must have tbname): %s\n", sql);
//   getFields(taos, sql);

//   // case 18 :  INSERT INTO db.stb(t1,t2,ts,b) values(?,?,?,?)
//   // no tbname error
//   sql = "INSERT INTO db.stb(t1,t2,ts,b) values(?,?,?,?)";
//   printf("case 18 (no tbname error): %s\n", sql);
//   getFields(taos, sql);

//   // case 19 : insert into db.ntb(nts,ni) values(?,?,?,?,?)
//   //  wrong para nums
//   sql = "insert into ntb(nts,ni) values(?,?,?,?,?)";
//   printf("case 19 : %s\n", sql);
//   getFields(taos, sql);

//   // case 20 : insert into db.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)
//   // wrong simbol
//   sql = "insert into db.stb(t1,t2,ts,b,tbname) values(*,*,*,*,*)";
//   printf("=================normal test===================\n");
//   printf("case 20 : %s\n", sql);
//   getFields(taos, sql);

//   // case 21 : INSERT INTO ! using db.stb TAGS(?,?) VALUES(?,?)
//   // wrong simbol
//   sql = "insert into ! using db.stb tags(?, ?) values(?,?)";
//   printf("case 21 : %s\n", sql);
//   getFields(taos, sql);

//   // case 22 : INSERT INTO ! using db.stb TAGS(?,?) VALUES(?,?)
//   // wrong tbname
//   sql = "insert into db.stb values(?,?)";
//   printf("case 22 : %s\n", sql);
//   getFields(taos, sql);

//   // case 23 : select * from ? where ts = ?
//   // wrong query type
//   sql = "select * from ? where ts = ?";
//   printf("case 23 : %s\n", sql);
//   getQueryFields(taos, sql);
// }

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
  do_query(taos, "use db");
  do_query(taos,
           "create table db.stb (ts timestamp, b binary(10)) tags(t1 "
           "int, t2 binary(10))");
  do_query(
      taos,
      "create table if not exists all_stb(ts timestamp, v1 bool, v2 tinyint, v3 smallint, v4 int, v5 bigint, v6 "
      "tinyint unsigned, v7 smallint unsigned, v8 int unsigned, v9 bigint unsigned, v10 float, v11 double, v12 "
      "binary(20), v13 varbinary(20), v14 geometry(100), v15 nchar(20))tags(tts timestamp, tv1 bool, tv2 tinyint, tv3 "
      "smallint, tv4 int, tv5 bigint, tv6 tinyint unsigned, tv7 smallint unsigned, tv8 int unsigned, tv9 bigint "
      "unsigned, tv10 float, tv11 double, tv12 binary(20), tv13 varbinary(20), tv14 geometry(100), tv15 nchar(20));");

  // // case 1 : test super table
  // {
  //   const char*    sql = "insert into db.stb(t1,t2,ts,b,tbname) values(?,?,?,?,?)";
  //   TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
  //                                       {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
  //   printf("case 1 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 5);
  // }

  // case 2 : no tbname
  {
    const char*    sql = "insert into db.stb(t1,t2,ts,b,tbname) values(1,?,?,'abc',?)";
    TAOS_FIELD_ALL expectedFields[3] = {{"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
                                        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
                                        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
    printf("case 2 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  {
    // case 3 :  insert into db.stb(ts,b,tbname) values(?,?,?)
    // no tag
    const char*    sql = "insert into db.stb(ts,b,tbname) values(?,?,?)";
    TAOS_FIELD_ALL expectedFields[3] = {
        {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
        {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
        {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
    };
    printf("case 5 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 3);
  }

  // case 12 : insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  // values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  // test all types
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
    printf("case 12 : %s\n", sql);
    getFieldsSuccess(taos, sql, expectedFields, 33);
  }

  taos_close(taos);
  taos_cleanup();
}

TEST(stmt2Case, insert_ctb_using_get_fields_Test) {
  // do_query(taos, "CREATE TABLE db.d0 USING db.stb (t1,t2) TAGS (7,'Cali');");

  // // case 2 : INSERT INTO db.d0 VALUES (?,?)
  // // test child table
  // {
  //   const char*    sql = "INSERT INTO db.d0(ts,b) VALUES (?,?)";
  //   TAOS_FIELD_ALL expectedFields[2] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
  //   printf("case 2 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 2);
  // }

  //  // case 6 : INSERT INTO db.? using db.stb (t1,t2)TAGS(?,?) (ts,b)VALUES(?,?)
  // // normal insert clause
  // {
  //   const char*    sql = "INSERT INTO db.? using db.stb (t1,t2)TAGS(?,?) (ts,b)VALUES(?,?)";
  //   TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
  //                                       {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
  //   printf("case 6 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 5);
  // }

  // // case 7 : insert into db.? using db.stb(t2,t1) tags(?, ?) (b,ts)values(?,?)
  // // disordered
  // {
  //   const char*    sql = "insert into db.? using db.stb(t2,t1) tags(?, ?) (b,ts)values(?,?)";
  //   TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
  //   printf("case 7 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 5);
  // }

  // // case 8 : insert into db.? using db.stb tags(?, ?) values(?,?)
  // // no field name
  // {
  //   const char*    sql = "insert into db.? using db.stb tags(?, ?) values(?,?)";
  //   TAOS_FIELD_ALL expectedFields[5] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
  //                                       {"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
  //   printf("case 8 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 5);
  // }

  // // case 9 : insert into db.? using db.stb (t2)tags(?) (ts)values(?)
  // //  less para
  // {
  //   const char*    sql = "insert into db.? using db.stb (t2)tags(?) (ts)values(?)";
  //   TAOS_FIELD_ALL expectedFields[3] = {{"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
  //   printf("case 9 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 3);
  // }

  // // case 10 : insert into db.d0 (ts)values(?)
  // //  less para
  // {
  //   const char*    sql = "insert into db.d0 (ts)values(?)";
  //   TAOS_FIELD_ALL expectedFields[1] = {{"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL}};
  //   printf("case 10 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 1);
  // }
  // // case 11 : insert into abc using stb tags(?, ?) values(?,?)
  // // insert create table
  // {
  //   const char*    sql = "insert into abc using stb tags(?, ?) values(?,?)";
  //   TAOS_FIELD_ALL expectedFields[4] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
  //   printf("case 11 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 4);
  // }

  // // // case 13 : insert into ? using all_stb tags(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  // // values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  // // // test all types
  // // sql =
  // //     "insert into all_stb "
  // //
  // "(tbname,tts,tv1,tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9,tv10,tv11,tv12,tv13,tv14,tv15,ts,v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,"
  // //     "v11,v12,v13,v14,v15) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
  // // printf("case 13 : %s\n", sql);
  // // getFields(taos, sql);
}

TEST(stmt2Case, insert_ntb_get_fields_Test) {
  //   do_query(taos, "CREATE TABLE db.ntb(nts timestamp, nb binary(10),nvc varchar(16),ni int);");

  // // // case 3 : INSERT INTO db.ntb VALUES(?,?,?,?)
  // // // test normal table
  // {
  //   const char*    sql = "INSERT INTO db.ntb VALUES(?,?,?,?)";
  //   TAOS_FIELD_ALL expectedFields[4] = {{"nts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"nb", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL},
  //                                       {"nvc", TSDB_DATA_TYPE_BINARY, 0, 0, 18, TAOS_FIELD_COL},
  //                                       {"ni", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_COL}};
  //   printf("case 3 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 4);
  // }

  // {
  //   // case 4 : insert into db.stb(t1,tbname,ts,t2,b) values(?,?,?,?,?)
  //   // test random order
  //   const char*    sql = "insert into db.stb(t1,tbname,ts,t2,b) values(?,?,?,?,?)";
  //   TAOS_FIELD_ALL expectedFields[5] = {{"t1", TSDB_DATA_TYPE_INT, 0, 0, 4, TAOS_FIELD_TAG},
  //                                       {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"b", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_COL}};
  //   printf("case 4 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 5);
  // }
}

TEST(stmt2Case, select_get_fields_Test) {
  // // case 14 : select * from ntb where ts = ?
  // // query type
  // sql = "select * from ntb where ts = ?";
  // printf("case 14 : %s\n", sql);
  // getQueryFields(taos, sql);

  // // case 15 : select * from ntb where ts = ? and b = ?
  // // query type
  // sql = "select * from ntb where ts = ? and b = ?";
  // printf("case 15 : %s\n", sql);
  // getQueryFields(taos, sql);
}

TEST(stmt2Case, db_get_fields_Test) {}

TEST(stmt2Case, get_fields_error_Test) {
  //   {
  //   const char*    sql = "insert into db.stb(t1,t2,ts,b,tbname) values(1,?,?,'abc',?)";
  //   TAOS_FIELD_ALL expectedFields[3] = {{"t2", TSDB_DATA_TYPE_BINARY, 0, 0, 12, TAOS_FIELD_TAG},
  //                                       {"ts", TSDB_DATA_TYPE_TIMESTAMP, 2, 0, 8, TAOS_FIELD_COL},
  //                                       {"tbname", TSDB_DATA_TYPE_BINARY, 0, 0, 271, TAOS_FIELD_TBNAME}};
  //   printf("case 2 : %s\n", sql);
  //   getFieldsSuccess(taos, sql, expectedFields, 3);
  // }
}

#pragma GCC diagnostic pop
