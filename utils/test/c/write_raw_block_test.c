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

int buildStable(TAOS* pConn) {
  TAOS_RES* pRes = taos_query(pConn,
                    "CREATE STABLE `meters` (`ts` TIMESTAMP, `current` INT, `voltage` INT, `phase` FLOAT) TAGS "
                    "(`groupid` INT, `location` VARCHAR(16))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table meters, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d0 using meters tags(1, 'San Francisco')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table d0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into d0 (ts, current) values (now, 120)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into table d0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1 using meters tags(2, 'San Francisco')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table d1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d2 using meters tags(3, 'San Francisco')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table d2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table ntba(ts timestamp, addr binary(32))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create ntba, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table ntbb(ts timestamp, addr binary(8))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create ntbb, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ntba values(now,'123456789abcdefg123456789')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert table ntba, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ntba values(now,'hello')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert table ntba, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  return 0;
}

int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }
  int32_t ret = -1;

  TAOS_RES* pRes = taos_query(pConn, "drop database if exists db_raw");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists db_raw vgroups 2");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use db_raw");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  buildStable(pConn);

  pRes = taos_query(pConn, "select * from d0");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  void   *data = NULL;
  int32_t numOfRows = 0;
  int     error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  if(error_code !=0 ){
    printf("error fetch raw block, reason:%s\n", taos_errstr(pRes));
    goto END;
  }

  taos_write_raw_block(pConn, numOfRows, data, "d1");
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select ts,phase from d0");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  if(error_code !=0 ){
    printf("error fetch raw block, reason:%s\n", taos_errstr(pRes));
    goto END;
  }

  int         numFields = taos_num_fields(pRes);
  TAOS_FIELD *fields = taos_fetch_fields(pRes);
  taos_write_raw_block_with_fields(pConn, numOfRows, data, "d2", fields, numFields);
  taos_free_result(pRes);

  // check error msg
  pRes = taos_query(pConn, "select * from ntba");
  if (taos_errno(pRes) != 0) {
    printf("error select * from ntba, reason:%s\n", taos_errstr(pRes));
    goto END;
  }

  data = NULL;
  numOfRows = 0;
  error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  if(error_code !=0 ){
    printf("error fetch select * from ntba, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  error_code = taos_write_raw_block(pConn, numOfRows, data, "ntbb");
  if(error_code == 0) {
      printf(" taos_write_raw_block to ntbb expect failed , but success!\n");
      goto END;
  }

  // pass NULL return last error code describe
  const char* err = taos_errstr(NULL);
  printf("write_raw_block return code =0x%x err=%s\n", error_code, err);
  if(strcmp(err, "success") == 0) {
      printf("expect failed , but error string is success! err=%s\n", err);
      goto END;
  }

  // no exist table
  error_code = taos_write_raw_block(pConn, numOfRows, data, "no-exist-table");
  if(error_code == 0) {
      printf(" taos_write_raw_block to no-exist-table expect failed , but success!\n");
      goto END;
  }

  err = taos_errstr(NULL);
  printf("write_raw_block no exist table return code =0x%x err=%s\n", error_code, err);
  if(strcmp(err, "success") == 0) {
      printf("expect failed write no exist table, but error string is success! err=%s\n", err);
      goto END;
  }

  // success
  ret  = 0;

END:
  // free
  if(pRes) taos_free_result(pRes);
  if(pConn) taos_close(pConn);
  return ret;
}

int main(int argc, char* argv[]) {
  printf("test write_raw_block...\n");
  int ret = init_env();
  if (ret < 0) {
    printf("test write_raw_block failed.\n");
    return ret;
  }
  printf("test write_raw_block ok.\n");
  return 0;
}