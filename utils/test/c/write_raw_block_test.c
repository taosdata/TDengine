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
//  TAOS_RES* pRes = taos_query(pConn, query);
//  ASSERT(taos_errno(pRes) == 0);
//  void*   data = NULL;
//  int32_t numOfRows = 0;
//  int     error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
//  ASSERT(error_code == 0);
//
//  int         numFields = taos_num_fields(pRes);
//  TAOS_FIELD* fields = taos_fetch_fields(pRes);


  TdFilePtr pFile = taosOpenFile("../../../utils/test/c/data.bin", TD_FILE_READ);
  ASSERT(pFile != NULL);
  int32_t size = 5735607;
  void*   pMemBuf = taosMemoryCalloc(1, size);
  ASSERT (pMemBuf != NULL);
  int32_t readSize = taosReadFile(pFile, pMemBuf, size);
  ASSERT (readSize == size);

  taosCloseFile(&pFile);
  tmq_raw_data raw = {
    .raw = pMemBuf,
    .raw_len = size,
    .raw_type = 2
  };
  int code = tmq_write_raw(pConn, raw);
//  taos_free_result(pRes);
  taosMemoryFree(pMemBuf);

  return 0;
}

void init_env() {
  pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn);

  action("drop database if exists db_raw");
  action("create database if not exists db_raw vgroups 2");
  action("use db_raw");

  system("taos -f ../../../utils/test/c/stb1_sql");
  system("taos -f ../../../utils/test/c/tb1_sql");
//  action("CREATE STABLE `rt_logging` (`time` TIMESTAMP, `depth` DOUBLE, `tvb` DOUBLE, `lagdepth` DOUBLE, `bitdep` DOUBLE, `blockpos` DOUBLE, `timedist_inst` DOUBLE, `hkload` DOUBLE, `wob` DOUBLE, `spp` DOUBLE, `cup` DOUBLE, `rpm` DOUBLE, `torque` DOUBLE, `tdrpm` DOUBLE, `tdtorque` DOUBLE, `spm1` DOUBLE, `spm2` DOUBLE, `spm3` DOUBLE, `zbc` DOUBLE, `pit1` DOUBLE, `pit2` DOUBLE, `pit3` DOUBLE, `pit4` DOUBLE, `pit5` DOUBLE, `pit6` DOUBLE, `tpit` DOUBLE, `mwi` DOUBLE, `mwo` DOUBLE, `mci` DOUBLE, `mco` DOUBLE, `mti` DOUBLE, `mto` DOUBLE, `mfi` DOUBLE, `flowout` DOUBLE, `gas` DOUBLE, `c1` DOUBLE, `c2` DOUBLE, `c3` DOUBLE, `ic4` DOUBLE, `nc4` DOUBLE, `ic5` DOUBLE, `nc5` DOUBLE, `co2` DOUBLE, `h2` DOUBLE, `h2s1` DOUBLE, `h2s2` DOUBLE, `h2s3` DOUBLE, `h2s4` DOUBLE, `dcexp` DOUBLE, `dexp` DOUBLE, `gainlosstot` DOUBLE, `bitspeed` DOUBLE) TAGS (`jth` VARCHAR(80), `bzjh` VARCHAR(20), `etloilfield` VARCHAR(20))");
//  action("CREATE TABLE `t_lj_JS2025012601` USING `rt_logging` (`jth`, `bzjh`, `etloilfield`) TAGS (\"JS2025012601\", \"天105斜\", \"207\")");
//  action("insert into `t_lj_JS2025012601`(time, depth) values(now,1)");
}

int main(int argc, char* argv[]) {
  printf("test write_raw_block start.\n");
  init_env();
//  ASSERT(test_write_raw_block("select * from d0", "d1") == 0);                     // test schema same
//  ASSERT(test_write_raw_block("select * from ntbb", "ntba") == 0);                 // test schema compatible
//  ASSERT(test_write_raw_block("select * from ntbb", "ntbc") == 0);                 // test schema small
//  ASSERT(test_write_raw_block("select * from ntbc", "ntbb") == 0);                 // test schema bigger
//  ASSERT(test_write_raw_block("select * from ntba", "ntbb") != 0);                 // test schema mismatch
//  ASSERT(test_write_raw_block("select * from ntba", "no-exist-table") != 0);       // test no exist table
//  ASSERT(test_write_raw_block("select addr from ntba", "ntbb") != 0);              // test without ts
  ASSERT(test_write_raw_block_with_fields("select * from `t_lj_JS2025012601`", "t_lj_JS2025012601") == 0);  // test with fields

  printf("test write_raw_block end.\n");
  return 0;
}
