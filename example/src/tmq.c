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

static int  running = 1;
static void msg_process(TAOS_RES* msg) {
  char buf[1024];
  /*memset(buf, 0, 1024);*/
  printf("topic: %s\n", tmq_get_topic_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;
    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    taos_print_row(buf, row, fields, numOfFields);
    printf("%s\n", buf);

    const char* tbName = tmq_get_table_name(msg);
    if (tbName) {
      printf("from tb: %s\n", tbName);
    }
  }
}

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

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes =
      taos_query(pConn, "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct1 using st1 tags(2000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }

  pRes = taos_query(pConn, "create table if not exists ct3 using st1 tags(3000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }

  taos_free_result(pRes);
  return 0;
}

int32_t create_topic() {
  printf("create topic\n");
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

  pRes = taos_query(pConn, "create topic topic_ctb_column as abc1");
  /*pRes = taos_query(pConn, "create topic topic_ctb_column as select ts, c1, c2, c3 from st1");*/
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_ctb_column, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

#if 0
  pRes = taos_query(pConn, "insert into tu1 values(now, 1, 1.0, 'bi1')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  pRes = taos_query(pConn, "insert into tu1 values(now+1d, 1, 1.0, 'bi1')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  pRes = taos_query(pConn, "insert into tu2 values(now, 2, 2.0, 'bi2')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  pRes = taos_query(pConn, "insert into tu2 values(now+1d, 2, 2.0, 'bi2')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
#endif

  taos_close(pConn);
  return 0;
}

void tmq_commit_cb_print(tmq_t* tmq, tmq_resp_err_t resp, tmq_topic_vgroup_list_t* offsets, void* param) {
  printf("commit %d tmq %p offsets %p param %p\n", resp, tmq, offsets, param);
}

tmq_t* build_consumer() {
#if 0
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);
#endif

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  /*tmq_conf_set(conf, "td.connect.db", "abc1");*/
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_ctb_column");
  /*tmq_list_append(topic_list, "tmq_test_db_multi_insert_topic");*/
  return topic_list;
}

void basic_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  tmq_resp_err_t err;

  if ((err = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(err));
    printf("subscribe err\n");
    return;
  }
  int32_t cnt = 0;
  /*clock_t startTime = clock();*/
  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 0);
    if (tmqmessage) {
      cnt++;
      /*printf("get data\n");*/
      /*msg_process(tmqmessage);*/
      taos_free_result(tmqmessage);
      /*} else {*/
      /*break;*/
    }
  }
  /*clock_t endTime = clock();*/
  /*printf("log cnt: %d %f s\n", cnt, (double)(endTime - startTime) / CLOCKS_PER_SEC);*/

  err = tmq_consumer_close(tmq);
  if (err)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(err));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

void sync_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  static const int MIN_COMMIT_COUNT = 1;

  int            msg_count = 0;
  tmq_resp_err_t err;

  if ((err = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(err));
    return;
  }

  tmq_list_t* subList = NULL;
  tmq_subscription(tmq, &subList);
  char**  subTopics = tmq_list_to_c_array(subList);
  int32_t sz = tmq_list_get_size(subList);
  printf("subscribed topics: ");
  for (int32_t i = 0; i < sz; i++) {
    printf("%s, ", subTopics[i]);
  }
  printf("\n");
  tmq_list_destroy(subList);

  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 1000);
    if (tmqmessage) {
      msg_process(tmqmessage);
      taos_free_result(tmqmessage);

      tmq_commit(tmq, NULL, 1);
      /*if ((++msg_count % MIN_COMMIT_COUNT) == 0) tmq_commit(tmq, NULL, 0);*/
    }
  }

  err = tmq_consumer_close(tmq);
  if (err)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(err));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

void perf_loop(tmq_t* tmq, tmq_list_t* topics) {
  tmq_resp_err_t err;

  if ((err = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(err));
    printf("subscribe err\n");
    return;
  }
  int32_t batchCnt = 0;
  int32_t skipLogNum = 0;
  clock_t startTime = clock();
  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 500);
    if (tmqmessage) {
      batchCnt++;
      /*skipLogNum += tmqGetSkipLogNum(tmqmessage);*/
      /*msg_process(tmqmessage);*/
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }
  clock_t endTime = clock();
  printf("log batch cnt: %d, skip log cnt: %d, time used:%f s\n", batchCnt, skipLogNum,
         (double)(endTime - startTime) / CLOCKS_PER_SEC);

  err = tmq_consumer_close(tmq);
  if (err)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(err));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

int main(int argc, char* argv[]) {
  if (argc > 1) {
    printf("env init\n");
    if (init_env() < 0) {
      return -1;
    }
    create_topic();
  }
  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  /*perf_loop(tmq, topic_list);*/
  /*basic_consume_loop(tmq, topic_list);*/
  sync_consume_loop(tmq, topic_list);
}
