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

int buildStable(TAOS* pConn, TAOS_RES* pRes) {
  pRes = taos_query(pConn,
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

  return 0;
}

int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  TAOS_RES* pRes = taos_query(pConn, "drop database if exists db_raw");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists db_raw vgroups 2");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use db_raw");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  buildStable(pConn, pRes);

  pRes = taos_query(pConn, "select * from d0");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  void   *data = NULL;
  int32_t numOfRows = 0;
  int     error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  ASSERT(error_code == 0);
  ASSERT(numOfRows == 1);

  taos_write_raw_block(pConn, numOfRows, data, "d1");
  taos_free_result(pRes);

  pRes = taos_query(pConn, "select ts,phase from d0");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  error_code = taos_fetch_raw_block(pRes, &numOfRows, &data);
  ASSERT(error_code == 0);
  ASSERT(numOfRows == 1);

  int         numFields = taos_num_fields(pRes);
  TAOS_FIELD *fields = taos_fetch_fields(pRes);
  taos_write_raw_block_with_fields(pConn, numOfRows, data, "d2", fields, numFields);
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

int main(int argc, char* argv[]) {
  if (init_env() < 0) {
    return -1;
  }
}
