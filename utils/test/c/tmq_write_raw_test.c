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

static TAOS* use_db() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return NULL;
  }

  TAOS_RES* pRes = taos_query(pConn, "use db_taosx");
  if (taos_errno(pRes) != 0) {
    printf("error in use db_taosx, reason:%s\n", taos_errstr(pRes));
    return NULL;
  }
  taos_free_result(pRes);
  return pConn;
}

static void checkData() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);
  TAOS_RES* pRes = taos_query(pConn, "select * from db_taosx.ct0");
  ASSERT (taos_errno(pRes) == 0);
  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(pRes))) {
    int         numFields = taos_num_fields(pRes);
    TAOS_FIELD* fields = taos_fetch_fields(pRes);

    for (int i = 0; i < numFields; ++i) {
      if (IS_DECIMAL_TYPE(fields[i].type)) {
        ASSERT(strcmp(row[i], "23.230") == 0 || strcmp(row[i], "43.530") == 0);
      }
    }
  }
  taos_free_result(pRes);
  taos_close(pConn);
}

static void msg_process(TAOS_RES* msg) {
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  if (strcmp(tmq_get_db_name(msg), "db_query") == 0){
    TAOS_ROW row = NULL;
    while ((row = taos_fetch_row(msg))) {

      int         numFields = taos_num_fields(msg);
      TAOS_FIELD *fields = taos_fetch_fields(msg);

      for (int i = 0; i < numFields; ++i) {
        if (IS_DECIMAL_TYPE(fields[i].type)) {
          ASSERT(strcmp(row[i], "3.122") == 0);
        }
      }
    }
    return;
  }

  TAOS* pConn = use_db();
  if (tmq_get_res_type(msg) == TMQ_RES_TABLE_META || tmq_get_res_type(msg) == TMQ_RES_METADATA) {
    char* result = tmq_get_json_meta(msg);
    printf("meta result: %s\n", result);
    tmq_free_json_meta(result);
  }

  tmq_raw_data raw = {0};
  tmq_get_raw(msg, &raw);
  printf("write raw data type: %d\n", raw.raw_type);
  int32_t ret = tmq_write_raw(pConn, raw);
  printf("write raw data: %s\n", tmq_err2str(ret));
  ASSERT(ret == 0);

  tmq_free_raw(raw);
  taos_close(pConn);
}

int buildDatabase(TAOS* pConn, TAOS_RES* pRes) {
  pRes = taos_query(pConn,
                    "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16), c4 decimal(10,3)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000, \"ttt\", true)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a', 3.2)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct1 using st1(t1) tags(2000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table ct1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct2 using st1(t1) tags(NULL)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table ct2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 using st1(t1) tags(2000) values(1626006833600, 3, 4, 'b', 4.32)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct3 using st1(t1) tags(3000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a', 23.23)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 using st1(t1) tags(2000) values(1626006833600, 3, 4, 'b', 43.53)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 using st1(t1) tags(3000) values(1626006833600, 5, 6, 'c', 43.53) ct1 using st1(t1) tags(2000) values(1626006833601, 2, 3, 'sds', 43.53) (1626006833602, 4, 5, "
      "'ddd', 43.53) ct0 using st1 tags(1000, \"ttt\", true) values(1626006833603, 4, 3, 'hwj', 43.53) ct1 using st1(t1) tags(2000) values(now+5s, 23, 32, 's21ds', 43.53)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct0 set tag t4=false");
  if (taos_errno(pRes) != 0) {
    printf("alter table ct0 set tag t4=false, reason:%s\n", taos_errstr(pRes));
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

  TAOS_RES* pRes = taos_query(pConn, "drop database if exists db_taosx");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists db_taosx vgroups 1 wal_retention_period 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop topic if exists topic_db");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topic, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists abc1 vgroups 1 wal_retention_period 3600");
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

  buildDatabase(pConn, pRes);

  pRes = taos_query(pConn, "drop topic if exists topic_query");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topic, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists db_query");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists db_query vgroups 1 wal_retention_period 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  // create and insert another stable
  pRes = taos_query(pConn,
                    "create stable if not exists db_query.st11 (ts timestamp, c1 int, c2 float, c3 binary(16), c4 decimal(9,3)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into db_query.ct10 using db_query.st11 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a', 3.122)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  taos_close(pConn);
  return 0;
}

int32_t create_topic() {
  printf("create topic\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  pRes = taos_query(pConn, "create topic topic_db with meta as database abc1");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic topic_query as select * from db_query.st11");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_query, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("commit %d tmq %p param %p\n", code, tmq, param);
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
  tmq_conf_set(conf, "msg.consume.excluded", "1");
  tmq_conf_set(conf, "fetch.max.wait.ms", "1");
  assert(tmq_conf_set(conf, "fetch.max.wait.ms", "100000000000") == TMQ_CONF_INVALID);
  assert(tmq_conf_set(conf, "fetch.max.wait.ms", "-100000000000") == TMQ_CONF_INVALID);
  assert(tmq_conf_set(conf, "fetch.max.wait.ms", "0") == TMQ_CONF_INVALID);
  assert(tmq_conf_set(conf, "fetch.max.wait.ms", "1000") == TMQ_CONF_OK);
  assert(tmq_conf_set(conf, "min.poll.rows", "100000000000") == TMQ_CONF_INVALID);
  assert(tmq_conf_set(conf, "min.poll.rows", "-1") == TMQ_CONF_INVALID);
  assert(tmq_conf_set(conf, "min.poll.rows", "0") == TMQ_CONF_INVALID);
  assert(tmq_conf_set(conf, "min.poll.rows", "1") == TMQ_CONF_OK);
//  tmq_conf_set(conf, "max.poll.interval.ms", "20000");

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_db");
  tmq_list_append(topic_list, "topic_query");
  return topic_list;
}

void basic_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  int32_t code;

  if ((code = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(code));
    printf("subscribe err\n");
    return;
  }
  int32_t cnt = 0;
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 5000);
    if (tmqmessage) {
      cnt++;
      msg_process(tmqmessage);
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  code = tmq_consumer_close(tmq);
  if (code)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(code));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

int main(int argc, char* argv[]) {
  if (init_env() < 0) {
    return -1;
  }
  create_topic();

  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  tmq_list_destroy(topic_list);
  checkData();
}
