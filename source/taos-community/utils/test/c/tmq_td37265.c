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
#include "cJSON.h"
#include "taos.h"
#include "tmsg.h"
#include "types.h"

void do_query(TAOS *taos, const char *sql) {
  TAOS_RES *result = taos_query(taos, sql);
  int code = taos_errno(result);
  while (code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING) {
    taosMsleep(2000);
    result = taos_query(taos, sql);
    code = taos_errno(result);
  }
  assert(code==0);
  taos_free_result(result);
}

void insertData(TAOS *taos, const char *sql, int CTB_NUMS, int ROW_NUMS, int CYC_NUMS) {
  // create database and table
  do_query(taos, "DROP DATABASE IF EXISTS stmt_testdb_2");
  do_query(taos, "CREATE DATABASE IF NOT EXISTS stmt_testdb_2");
  do_query(
      taos,
      "CREATE STABLE IF NOT EXISTS stmt_testdb_2.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (groupId INT, location BINARY(24))");
  do_query(taos, "USE stmt_testdb_2");
  do_query(taos, "create table if not exists stmt_testdb_2.t1 using stmt_testdb_2.meters tags (1, 'abc')");
  do_query(taos, "create topic topic_db with meta as database stmt_testdb_2");

  // init
  TAOS_STMT *stmt = taos_stmt_init(taos);
  assert(stmt != NULL);
  int code = taos_stmt_prepare(stmt, sql, 0);
  assert(code == 0);
  int total_affected = 0;

  for (int k = 0; k < CYC_NUMS; k++) {
    for (int i = 1; i <= CTB_NUMS; i++) {

      // insert rows
      TAOS_MULTI_BIND params[2];
      // ts
      params[0].buffer_type = TSDB_DATA_TYPE_TIMESTAMP;
      params[0].buffer_length = sizeof(int64_t);
      params[0].length = (int32_t *)&params[0].buffer_length;
      params[0].is_null = NULL;
      params[0].num = 1;
      // current
      params[1].buffer_type = TSDB_DATA_TYPE_FLOAT;
      params[1].buffer_length = sizeof(float);
      params[1].length = (int32_t *)&params[1].buffer_length;
      params[1].is_null = NULL;
      params[1].num = 1;

      for (int j = 0; j < ROW_NUMS; j++) {
        int64_t ts = 1591060628000 + j + k * 100000;
        float   current = (float)0.0001f * j;
        int     voltage = j;
        float   phase = (float)0.0001f * j;
        params[0].buffer = &ts;
        params[1].buffer = &current;
        // bind param
        code = taos_stmt_bind_param(stmt, params);
        assert(code == 0);
      }
      // add batch
      code = taos_stmt_add_batch(stmt);
      assert(code == 0);

      // execute batch
      code = taos_stmt_execute(stmt);
      assert(code == 0);
      // get affected rows
      int affected = taos_stmt_affected_rows_once(stmt);
      total_affected += affected;
    }
  }
  assert(total_affected == CTB_NUMS * ROW_NUMS * CYC_NUMS);

  taos_stmt_close(stmt);
}

tmq_t* build_consumer() {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");

  tmq_conf_set_auto_commit_cb(conf, NULL, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_db");
  return topic_list;
}

void basic_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  int32_t code = tmq_subscribe(tmq, topics);
  assert(code == 0);

  int32_t cnt = 0;
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 5000);
    if (tmqmessage) {
      cnt++;
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  assert(cnt > 0);
  code = tmq_consumer_close(tmq);
  assert(code == 0);
}

void init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  assert (pConn != NULL);
  insertData(pConn, "INSERT INTO stmt_testdb_2.t1(ts,phase) VALUES (?,?)", 1, 1, 1);
  taos_close(pConn);
}


int main(int argc, char* argv[]) {
  init_env();
  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  tmq_list_destroy(topic_list);
}