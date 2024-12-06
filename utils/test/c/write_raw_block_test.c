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
#include <time.h>
#include "taos.h"
#include "types.h"

TAOS* pConn = NULL;
void  action(char* sql) {
  TAOS_RES* pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);
}

int32_t test_write_raw_block(char* query, char* dst) {
  TAOS_RES* pRes = taos_query(pConn, query);
  ASSERT(taos_errno(pRes) == 0);
  void*   data = NULL;
  int32_t numOfRows = 0;
  int     error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  ASSERT(error_code == 0);
  error_code = taos_write_raw_block(pConn, numOfRows, data, dst);
  taos_free_result(pRes);
  return error_code;
}

int32_t test_write_raw_block_with_fields(char* query, char* dst) {
  TAOS_RES* pRes = taos_query(pConn, query);
  ASSERT(taos_errno(pRes) == 0);
  void*   data = NULL;
  int32_t numOfRows = 0;
  int     error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  ASSERT(error_code == 0);

  int         numFields = taos_num_fields(pRes);
  TAOS_FIELD* fields = taos_fetch_fields(pRes);
  error_code = taos_write_raw_block_with_fields(pConn, numOfRows, data, dst, fields, numFields);
  taos_free_result(pRes);
  return error_code;
}

void init_env() {
  pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn);

  action("drop database if exists db_raw");
  action("create database if not exists db_raw vgroups 2");
  action("use db_raw");

  action(
      "CREATE STABLE `meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` FLOAT) TAGS (`groupid` INT, "
      "`location` VARCHAR(16))");
  action("create table d0 using meters tags(1, 'San Francisco')");
  action("create table d1 using meters tags(2, 'San Francisco')");
  action("create table d2 using meters tags(3, 'San Francisco')");
  action("insert into d0 (ts, current) values (now, 120)");

  action("create table ntba(ts timestamp, addr binary(32))");
  action("create table ntbb(ts timestamp, addr binary(8))");
  action("create table ntbc(ts timestamp, addr binary(8), c2 int)");

  action("insert into ntba values(now,'123456789abcdefg123456789')");
  action("insert into ntbb values(now + 1s,'hello')");
  action("insert into ntbc values(now + 13s, 'sdf', 123)");
}

int main(int argc, char* argv[]) {
  printf("test write_raw_block start.\n");
  init_env();
  ASSERT(test_write_raw_block("select * from d0", "d1") == 0);                     // test schema same
  ASSERT(test_write_raw_block("select * from ntbb", "ntba") == 0);                 // test schema compatible
  ASSERT(test_write_raw_block("select * from ntbb", "ntbc") == 0);                 // test schema small
  ASSERT(test_write_raw_block("select * from ntbc", "ntbb") == 0);                 // test schema bigger
  ASSERT(test_write_raw_block("select * from ntba", "ntbb") != 0);                 // test schema mismatch
  ASSERT(test_write_raw_block("select * from ntba", "no-exist-table") != 0);       // test no exist table
  ASSERT(test_write_raw_block("select addr from ntba", "ntbb") != 0);              // test without ts
  ASSERT(test_write_raw_block_with_fields("select ts,phase from d0", "d2") == 0);  // test with fields

  printf("test write_raw_block end.\n");
  return 0;
}