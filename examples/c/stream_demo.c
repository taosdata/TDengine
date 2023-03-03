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

// clang-format off
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "taos.h"

int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  TAOS_RES* pRes = taos_query(pConn, "create database if not exists abc1 vgroups 2");
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

#if 0
  pRes = taos_query(pConn, "create database if not exists abc2 vgroups 20");
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
#endif

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create stable if not exists st1 (ts timestamp, k int, j varchar(20)) tags(a varchar(20))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tu1 using st1 tags('c1')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tu2 using st1 tags('c2')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tu3 using st1 tags('c3')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  return 0;
}

int32_t create_stream() {
  printf("create stream\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    /*"create stream stream1 trigger at_once watermark 10s into outstb as select _wstart start, avg(k) from st1 partition by tbname interval(10s)");*/
                    "create stream stream2 into outstb subtable(concat(concat(concat('prefix_', tname), '_suffix_'), cast(k1 as varchar(20)))) as select _wstart wstart, avg(k) from st1 partition by tbname tname, a k1 interval(10s);");
  if (taos_errno(pRes) != 0) {
    printf("failed to create stream stream1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  taos_close(pConn);
  return 0;
}

int main(int argc, char* argv[]) {
  if (argc > 1) {
    printf("env init\n");
    int code = init_env();
    if (code) {
       return code;
    }

  }
  create_stream();
}
