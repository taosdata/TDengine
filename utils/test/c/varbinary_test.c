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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"
#include "types.h"
#include "tlog.h"

#define GET_ROW_NUM \
  numRows = 0;\
  while(1){\
    row = taos_fetch_row(pRes);\
    if (row != NULL){\
      numRows++;\
    }else{\
      break;\
    }\
  }
void varbinary_sql_test() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists varbinary_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists varbinary_db");
  taos_free_result(pRes);

  pRes = taos_query(taos, "use varbinary_db");
  int code = taos_errno(pRes);
  taos_free_result(pRes);
  ASSERT(code == 0);

  pRes = taos_query(taos, "create stable stb (ts timestamp, c1 nchar(32), c2 varbinary(16), c3 float) tags (t1 int, t2 binary(8), t3 varbinary(8))");
  taos_free_result(pRes);

  pRes = taos_query(taos, "desc stb");

  TAOS_ROW row = NULL;
  int32_t  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
    char* type  = row[1];
    int32_t length = *(int32_t *)row[2];

    if (rowIndex == 2) {
      ASSERT(strncmp(type, "VARBINARY", sizeof("VARBINARY") - 1) == 0);
      ASSERT(length == 16);
    }

    if (rowIndex == 6) {
      ASSERT(strncmp(type, "VARBINARY", sizeof("VARBINARY") - 1) == 0);
      ASSERT(length == 8);
    }
    rowIndex++;
  }
  taos_free_result(pRes);

  pRes = taos_query(taos, "insert into tb1 using stb tags (1, 'tb1_bin1', 'vart1') values (now, 'nchar1', 'varc1', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb1 values (now + 1s, 'nchar2', null, 0.4);");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb3 using stb tags (3, 'tb3_bin1', '\\x7f8290') values (now + 2s, 'nchar1', '\\x7f8290', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb3 values (now + 3s, 'nchar1', '\\x7f829000', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb2 using stb tags (2, 'tb2_bin1', '\\x') values (now + 4s, 'nchar1', '\\x', 0.3)");
  taos_free_result(pRes);
  pRes = taos_query(taos, "insert into tb2 values (now + 5s, 'nchar1', '\\x00000000', 0.3)");
  taos_free_result(pRes);

  // test insert
  pRes = taos_query(taos, "insert into tb2 using stb tags (2, 'tb2_bin1', 093) values (now + 2s, 'nchar1', 892, 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(taos, "insert into tb3 using stb tags (3, 'tb3_bin1', 0x7f829) values (now + 3s, 'nchar1', 0x7f829, 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(taos, "insert into tb3 using stb tags (3, 'tb3_bin1', '\\x7f829') values (now + 3s, 'nchar1', '\\x7f829', 0.3)");
  ASSERT(taos_errno(pRes) != 0);

  pRes = taos_query(taos, "insert into tb4 using stb tags (4, 'tb4_bin1', 0b100000010) values (now + 4s, 'nchar1', 0b110000001, 0.3)");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  // test error
  pRes = taos_query(taos, "select * from tb1 where c2 >= 0x8de6");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from tb1 where c2 >= 0");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 > '\\x7F82900'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2+2 from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2|2 from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 not like 's%'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 like 's%'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 match 'ssd'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 nmatch 'ssd'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2->'ssd' = 1");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 contains 'ssd'");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where sum(c2) = 2");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);


  // math function test, not support
  pRes = taos_query(taos, "select abs(c2) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select floor(c2) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  // string function test, not support
  pRes = taos_query(taos, "select ltrim(c2) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select upper(c2) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);


  pRes = taos_query(taos, "select to_json(c2) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select TO_UNIXTIMESTAMP(c2) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select cast(c2 as varchar(16)) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select cast(c3 as varbinary(16)) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select cast(c1 as varbinary(16)) from stb");
  ASSERT(taos_errno(pRes) != 0);
  taos_free_result(pRes);

  // support first/last/last_row/count/hyperloglog/sample/tail/mode/length
  pRes = taos_query(taos, "select first(c2) from stb");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select count(c2) from stb");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select sample(c2,2) from stb");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select mode(c2) from stb");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select length(c2) from stb where c2 = '\\x7F8290'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select cast(t2 as varbinary(16)) from stb order by ts");
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[0], length[0], &data, &size) < 0){
      ASSERT(0);
    }

    ASSERT(memcmp(data, "\\x7462315F62696E31", size) == 0);
    taosMemoryFree(data);
    break;
  }
  taos_free_result(pRes);

  int numRows = 0;

  pRes = taos_query(taos, "select * from stb where c2 > 'varc1'");
  GET_ROW_NUM
  ASSERT(numRows == 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 > '\\x7F8290'");
  GET_ROW_NUM
  ASSERT(numRows == 1);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 in ('\\x7F829000','\\x00000000')");
  GET_ROW_NUM
  ASSERT(numRows == 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 not in ('\\x00000000')");
  GET_ROW_NUM
  ASSERT(numRows == 4);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 is null");
  GET_ROW_NUM
  ASSERT(numRows == 1);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 is not null");
  GET_ROW_NUM
  ASSERT(numRows == 5);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 between '\\x3e' and '\\x7F8290'");
  GET_ROW_NUM
  ASSERT(numRows == 2);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select * from stb where c2 not between '\\x3e' and '\\x7F8290'");
  GET_ROW_NUM
  ASSERT(numRows == 3);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select cast('1' as varbinary(8))");
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[0], length[0], &data, &size) < 0){
      ASSERT(0);
    }

    ASSERT(memcmp(data, "\\x31", size) == 0);
    taosMemoryFree(data);
  }
  taos_free_result(pRes);

  pRes = taos_query(taos, "select ts,c2 from stb order by c2");
  rowIndex = 0;
  while ((row = taos_fetch_row(pRes)) != NULL) {
//    int64_t ts = *(int64_t *)row[0];
    if (rowIndex == 0) {
      ASSERT(row[1] == NULL);
      rowIndex++;
      continue;
    }
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[1], length[1], &data, &size) < 0){
      ASSERT(0);
    }

    if (rowIndex == 1) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x", size) == 0);
    }
    if (rowIndex == 2) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x00000000", size) == 0);
    }
    if (rowIndex == 3) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7661726331", size) == 0);
    }
    if (rowIndex == 4) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7F8290", size) == 0);
    }
    if (rowIndex == 5) {
//      ASSERT(ts == 1661943960000);
      ASSERT(memcmp(data, "\\x7F829000", size) == 0);
    }
    taosMemoryFree(data);

    rowIndex++;
  }
  printf("%s result %s\n", __FUNCTION__, taos_errstr(pRes));
  taos_free_result(pRes);

  // test insert string value '\x'
  pRes = taos_query(taos, "insert into tb5 using stb tags (5, 'tb5_bin1', '\\\\xg') values (now + 4s, 'nchar1', '\\\\xg', 0.3)");
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2,t3 from stb where t3 = '\\x5C7867'");
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[0], length[0], &data, &size) < 0){
      ASSERT(0);
    }

    ASSERT(memcmp(data, "\\x5C7867", size) == 0);
    taosMemoryFree(data);

    if(taosAscii2Hex(row[1], length[1], &data, &size) < 0){
      ASSERT(0);
    }

    ASSERT(memcmp(data, "\\x5C7867", size) == 0);
    taosMemoryFree(data);
  }
  taos_free_result(pRes);

  // test insert
  char tmp [65517*2+3] = {0};
  tmp[0] = '\\';
  tmp[1] = 'x';
  memset(tmp + 2, 48, 65517*2);

  char sql[65517*2+3 + 256] = {0};

  pRes = taos_query(taos, "create stable stb1 (ts timestamp, c2 varbinary(65517)) tags (t1 int, t2 binary(8), t3 varbinary(8))");
  taos_free_result(pRes);

  sprintf(sql, "insert into tb6 using stb1 tags (6, 'tb6_bin1', '\\\\xg') values (now + 4s, '%s')", tmp);
  pRes = taos_query(taos, sql);
  taos_free_result(pRes);

  pRes = taos_query(taos, "select c2 from tb6");
  while ((row = taos_fetch_row(pRes)) != NULL) {
    int32_t*    length = taos_fetch_lengths(pRes);
    void*         data = NULL;
    uint32_t      size = 0;
    if(taosAscii2Hex(row[0], length[0], &data, &size) < 0){
      ASSERT(0);
    }

    ASSERT(memcmp(data, tmp, size) == 0);
    taosMemoryFree(data);
  }
  taos_free_result(pRes);

  taos_close(taos);
}

void varbinary_stmt_test(){
  TAOS     *taos;
  TAOS_RES *result;
  int      code;
  TAOS_STMT *stmt;

  taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (taos == NULL) {
    printf("failed to connect to db, reason:%s\n", taos_errstr(taos));
    ASSERT(0);
  }

  result = taos_query(taos, "drop database demo");
  taos_free_result(result);

  result = taos_query(taos, "create database demo");
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create database, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    ASSERT(0);
  }
  taos_free_result(result);

  result = taos_query(taos, "use demo");
  taos_free_result(result);

  // create table
  const char* sql = "create table m1 (ts timestamp, b bool, varbin varbinary(16))";
  result = taos_query(taos, sql);
  code = taos_errno(result);
  if (code != 0) {
    printf("failed to create table, reason:%s\n", taos_errstr(result));
    taos_free_result(result);
    ASSERT(0);
  }
  taos_free_result(result);
  struct {
    int64_t ts;
    int8_t b;
    int8_t varbin[16];
  } v = {0};

  int32_t boolLen = sizeof(int8_t);
  int32_t bintLen = sizeof(int64_t);
  int32_t vbinLen = 5;

  stmt = taos_stmt_init(taos);
  TAOS_MULTI_BIND params[3];
  params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
  params[0].buffer_length = sizeof(v.ts);
  params[0].buffer = &v.ts;
  params[0].length = &bintLen;
  params[0].is_null = NULL;
  params[0].num = 1;

  params[1].buffer_type = TSDB_DATA_TYPE_BOOL;
  params[1].buffer_length = sizeof(v.b);
  params[1].buffer = &v.b;
  params[1].length = &boolLen;
  params[1].is_null = NULL;
  params[1].num = 1;

  params[2].buffer_type = TSDB_DATA_TYPE_VARBINARY;
  params[2].buffer_length = sizeof(v.varbin);
  params[2].buffer = v.varbin;
  params[2].length = &vbinLen;
  params[2].is_null = NULL;
  params[2].num = 1;

  char is_null = 1;

  sql = "insert into m1 values(?,?,?)";
  code = taos_stmt_prepare(stmt, sql, 0);
  if (code != 0){
    printf("failed to execute taos_stmt_prepare. code:0x%x\n", code);
  }
  v.ts = 1591060628000;
  for (int i = 0; i < 10; ++i) {
    v.ts += 1;
    for (int j = 1; j < 3; ++j) {
      params[j].is_null = ((i == j) ? &is_null : 0);
    }
    v.b = (int8_t)i % 2;

    for (int j = 0; j < vbinLen; ++j) {
      v.varbin[j] = i + j;
    }

    taos_stmt_bind_param(stmt, params);
    taos_stmt_add_batch(stmt);
  }
  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute insert statement.\n");
    ASSERT(0);
  }
  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT varbin FROM m1 WHERE varbin = ?", 0);
  for (int j = 0; j < vbinLen; ++j) {
    v.varbin[j] = j;
  }
  taos_stmt_bind_param(stmt, params + 2);
  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute select statement.\n");
    ASSERT(0);
  }

  result = taos_stmt_use_result(stmt);

  TAOS_ROW    row;
  int         rows = 0;
  int         num_fields = taos_num_fields(result);
  TAOS_FIELD *fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    ASSERT(strcmp(temp, "\\x0001020304") == 0);
  }
  ASSERT (rows == 1);

//  taos_free_result(result);
  taos_stmt_close(stmt);

  // query the records
  stmt = taos_stmt_init(taos);
  taos_stmt_prepare(stmt, "SELECT varbin FROM m1 WHERE varbin = ?", 0);

  char tmp[16] = "\\x090a0b0c0d";
  vbinLen = strlen(tmp);
  params[2].buffer_type = TSDB_DATA_TYPE_VARCHAR;
  params[2].buffer_length = sizeof(v.varbin);
  params[2].buffer = tmp;
  params[2].length = &vbinLen;
  taos_stmt_bind_param(stmt, params + 2);
  if (taos_stmt_execute(stmt) != 0) {
    printf("failed to execute select statement.\n");
    ASSERT(0);
  }

  result = taos_stmt_use_result(stmt);

  rows = 0;
  num_fields = taos_num_fields(result);
  fields = taos_fetch_fields(result);

  // fetch the records row by row
  while ((row = taos_fetch_row(result))) {
    char temp[256] = {0};
    rows++;
    taos_print_row(temp, row, fields, num_fields);
    ASSERT(strcmp(temp, "\\x090A0B0C0D") == 0);
  }
  ASSERT (rows == 1);
//  taos_free_result(result);
  taos_stmt_close(stmt);
  taos_close(taos);
  printf("%s result success\n", __FUNCTION__);
}

tmq_t* build_consumer() {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

void varbinary_tmq_test(){

  // build database

  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  TAOS_RES *pRes = taos_query(pConn, "drop database if exists abc");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists abc vgroups 1 wal_retention_period 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create stable if not exists st1 (ts timestamp, c2 varbinary(16)) tags(t1 int, t2 varbinary(8))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000, '\\x3f89')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 values(1626006833400, 'hello')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 modify column c2 varbinary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add tag t3 varbinary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct0 set tag t2='894'");
  if (taos_errno(pRes) != 0) {
    printf("failed to slter child table ct3, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tb1 (ts timestamp, c1 varbinary(8))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table tb1 add column c2 varbinary(8)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  // create topic
  pRes = taos_query(pConn, "create topic topic_db with meta as database abc");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_db, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);

  // build consumer
  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_db");

  int32_t code = tmq_subscribe(tmq, topic_list);
  ASSERT(code == 0);

  int32_t cnt = 0;
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 1000);
    if (tmqmessage) {
      if (tmq_get_res_type(tmqmessage) == TMQ_RES_TABLE_META || tmq_get_res_type(tmqmessage) == TMQ_RES_METADATA) {
        char* result = tmq_get_json_meta(tmqmessage);
//        if (result) {
//          printf("meta result: %s\n", result);
//        }
        switch (cnt) {
          case 0:
            ASSERT(strcmp(result, "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9},{\"name\":\"c2\",\"type\":16,\"length\":16}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t2\",\"type\":16,\"length\":8}]}") == 0);
            break;
          case 1:
            ASSERT(strcmp(result, "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":2,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t2\",\"type\":16,\"value\":\"\\\"\\\\x3F89\\\"\"}],\"createList\":[]}") == 0);
            break;
          case 2:
            ASSERT(strcmp(result, "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":7,\"colName\":\"c2\",\"colType\":16,\"colLength\":64}") == 0);
            break;
          case 3:
            ASSERT(strcmp(result, "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":1,\"colName\":\"t3\",\"colType\":16,\"colLength\":64}") == 0);
            break;
          case 4:
            ASSERT(strcmp(result, "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"alterType\":4,\"colName\":\"t2\",\"colValue\":\"\\\"\\\\x383934\\\"\",\"colValueNull\":false}") == 0);
            break;
          case 5:
            ASSERT(strcmp(result, "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\",\"type\":9},{\"name\":\"c1\",\"type\":16,\"length\":8}],\"tags\":[]}") == 0);
            break;
          case 6:
            ASSERT(strcmp(result, "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"alterType\":5,\"colName\":\"c2\",\"colType\":16,\"colLength\":8}") == 0);
            break;
          default:
            break;
        }
        cnt++;
        tmq_free_json_meta(result);
      }
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  code = tmq_consumer_close(tmq);
  ASSERT(code == 0);

  tmq_list_destroy(topic_list);

  pRes = taos_query(pConn, "drop topic if exists topic_db");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topic, reason:%s\n", taos_errstr(pRes));
    ASSERT(0);
  }
  taos_free_result(pRes);
  taos_close(pConn);
  printf("%s result success\n", __FUNCTION__);
}

int main(int argc, char *argv[]) {
  int ret = 0;

  varbinary_tmq_test();
  varbinary_stmt_test();
  varbinary_sql_test();
  return ret;
}
