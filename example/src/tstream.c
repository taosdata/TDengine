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
#include <string.h>
#include <time.h>
#include "taos.h"

int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  TAOS_RES* pRes = taos_query(pConn, "create database if not exists abc1 vgroups 1");
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create stable if not exists st1 (ts timestamp, k int) tags(a int)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tu1 using st1 tags(1)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists tu2 using st1 tags(2)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu2, reason:%s\n", taos_errstr(pRes));
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

  /*const char* sql = "select min(k), max(k), sum(k) from tu1";*/
  const char* sql = "select min(k), max(k), sum(k) as sum_of_k from st1";
  /*const char* sql = "select sum(k) from tu1 interval(10m)";*/
  pRes = tmq_create_stream(pConn, "stream1", "out1", sql);
  if (taos_errno(pRes) != 0) {
    printf("failed to create stream out1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  taos_close(pConn);
  return 0;
}

int main(int argc, char* argv[]) {
  int code;
  if (argc > 1) {
    printf("env init\n");
    code = init_env();
  }
  create_stream();
#if 0
  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  /*perf_loop(tmq, topic_list);*/
  /*basic_consume_loop(tmq, topic_list);*/
  sync_consume_loop(tmq, topic_list);
#endif
}
