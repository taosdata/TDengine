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
#include <time.h>
#include "taos.h"
#include "types.h"

tmq_t* build_consumer() {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "g1");
  tmq_conf_set(conf, "client.id", "c1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "enable.replay", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

void test_vgroup_error(TAOS* pConn){
  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists d1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists d1 vgroups 2 wal_retention_period 3600");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "CREATE STABLE d1.s1 (ts TIMESTAMP, c1 INT) TAGS (t1 INT)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic t1 as select * from d1.s1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  tmq_list_t* topic_list = tmq_list_new();

  tmq_list_append(topic_list, "t1");
  tmq_t*      tmq = build_consumer();
  ASSERT(tmq_subscribe(tmq, topic_list) != 0);
  tmq_list_destroy(topic_list);
  tmq_consumer_close(tmq);
}

void test_stable_db_error(TAOS* pConn){
  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists d1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists d1 vgroups 1 wal_retention_period 3600");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "CREATE STABLE d1.s1 (ts TIMESTAMP, c1 INT) TAGS (t1 INT)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic t1 as stable d1.s1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  tmq_list_t* topic_list = tmq_list_new();

  tmq_list_append(topic_list, "t1");
  tmq_t*      tmq = build_consumer();
  ASSERT(tmq_subscribe(tmq, topic_list) != 0);
  tmq_list_destroy(topic_list);
  tmq_consumer_close(tmq);

  pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic t1 as database d1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  topic_list = tmq_list_new();
  tmq_list_append(topic_list, "t1");
  tmq = build_consumer();
  ASSERT(tmq_subscribe(tmq, topic_list) != 0);
  tmq_list_destroy(topic_list);
  tmq_consumer_close(tmq);
}

void insert_with_sleep(TAOS* pConn, int32_t* interval, int32_t len){
  for(int i = 0; i < len; i++){
    TAOS_RES* pRes = taos_query(pConn, "insert into d1.table1 (ts, c1) values (now, 1)");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);

    taosMsleep(interval[i]);
  }
}

void insert_with_sleep_multi(TAOS* pConn, int32_t* interval, int32_t len){
  for(int i = 0; i < len; i++){
    TAOS_RES* pRes = taos_query(pConn, "insert into d1.table1 (ts, c1) values (now, 1) (now+1s, 2) d1.table2 (ts, c1) values (now, 1) (now+1s, 2)");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);

    taosMsleep(interval[i]);
  }
}

void test_case1(TAOS* pConn, int32_t* interval, int32_t len){
  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists d1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists d1 vgroups 2 wal_retention_period 3600");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "CREATE STABLE d1.s1 (ts TIMESTAMP, c1 INT) TAGS (t1 INT)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1.table1 using d1.s1 tags(1)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  insert_with_sleep(pConn, interval, len);

  pRes = taos_query(pConn, "create topic t1 as select * from d1.table1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  tmq_list_t* topic_list = tmq_list_new();

  tmq_list_append(topic_list, "t1");
  tmq_t*      tmq = build_consumer();
  // 启动订阅
  tmq_subscribe(tmq, topic_list);
  tmq_list_destroy(topic_list);

  int32_t     timeout = 5000;

  int64_t t = 0;
  int32_t totalRows = 0;
  char    buf[1024] = {0};
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, timeout);
    if (tmqmessage) {
      if(t != 0){
        ASSERT(taosGetTimestampMs() - t >= interval[totalRows - 1]);
      }
      t = taosGetTimestampMs();

      TAOS_ROW row = taos_fetch_row(tmqmessage);
      if (row == NULL) {
        break;
      }

      TAOS_FIELD* fields = taos_fetch_fields(tmqmessage);
      int32_t     numOfFields = taos_field_count(tmqmessage);
      taos_print_row(buf, row, fields, numOfFields);

      printf("time:%" PRId64 " rows[%d]: %s\n", t, totalRows, buf);
      totalRows++;
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  ASSERT(totalRows == len);
  tmq_consumer_close(tmq);
}

void test_case2(TAOS* pConn, int32_t* interval, int32_t len, tsem_t* sem){
  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists d1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists d1 vgroups 1 wal_retention_period 3600");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "CREATE STABLE d1.s1 (ts TIMESTAMP, c1 INT) TAGS (t1 INT)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1.table1 using d1.s1 tags(1)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1.table2 using d1.s1 tags(2)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  insert_with_sleep_multi(pConn, interval, len);

  pRes = taos_query(pConn, "create topic t1 as select * from d1.s1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  tmq_list_t* topic_list = tmq_list_new();

  tmq_list_append(topic_list, "t1");
  tmq_t*      tmq = build_consumer();
  // 启动订阅
  tmq_subscribe(tmq, topic_list);
  tmq_list_destroy(topic_list);

  int32_t     timeout = 5000;

  int64_t t = 0;
  int32_t totalRows = 0;
  char    buf[1024] = {0};
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, timeout);
    if (tmqmessage) {
      if(t != 0 && totalRows % 4 == 0){
        ASSERT(taosGetTimestampMs() - t >= interval[totalRows/4 - 1]);
      }
      t = taosGetTimestampMs();

      while(1){
        TAOS_ROW row = taos_fetch_row(tmqmessage);
        if (row == NULL) {
          break;
        }

        TAOS_FIELD* fields = taos_fetch_fields(tmqmessage);
        int32_t     numOfFields = taos_field_count(tmqmessage);
        taos_print_row(buf, row, fields, numOfFields);

        printf("time:%" PRId64 " rows[%d]: %s\n", t, totalRows, buf);
        totalRows++;
      }

      taos_free_result(tmqmessage);

      if(totalRows == len * 4){
        taosSsleep(1);
        tsem_post(sem);
      }
    } else {
      break;
    }
  }

  ASSERT(totalRows == len * 4 + 1);
  tmq_consumer_close(tmq);
}

void test_case3(TAOS* pConn, int32_t* interval, int32_t len){
  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists d1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists d1 vgroups 1 wal_retention_period 3600");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "CREATE STABLE d1.s1 (ts TIMESTAMP, c1 INT) TAGS (t1 INT)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1.table1 using d1.s1 tags(1)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1.table2 using d1.s1 tags(2)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  insert_with_sleep_multi(pConn, interval, len);

  pRes = taos_query(pConn, "create topic t1 as select * from d1.s1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  tmq_list_t* topic_list = tmq_list_new();

  tmq_list_append(topic_list, "t1");
  tmq_t*      tmq = build_consumer();
  // 启动订阅
  tmq_subscribe(tmq, topic_list);

  int32_t     timeout = 5000;

  TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, timeout);
  taos_free_result(tmqmessage);

  tmq_consumer_close(tmq);

  tmq = build_consumer();
  // 启动订阅
  tmq_subscribe(tmq, topic_list);

  int64_t t = 0;
  int32_t totalRows = 0;
  char    buf[1024] = {0};
  while (1) {
    tmqmessage = tmq_consumer_poll(tmq, timeout);
    if (tmqmessage) {
      if(t != 0 && totalRows % 4 == 0){
        ASSERT(taosGetTimestampMs() - t >= interval[totalRows/4 - 1]);
      }
      t = taosGetTimestampMs();

      while(1){
        TAOS_ROW row = taos_fetch_row(tmqmessage);
        if (row == NULL) {
          break;
        }

        TAOS_FIELD* fields = taos_fetch_fields(tmqmessage);
        int32_t     numOfFields = taos_field_count(tmqmessage);
        taos_print_row(buf, row, fields, numOfFields);

        printf("time:%" PRId64 " rows[%d]: %s\n", t, totalRows, buf);
        totalRows++;
      }

      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  ASSERT(totalRows == len * 4);

  tmq_consumer_close(tmq);
  tmq_list_destroy(topic_list);
}

void* insertThreadFunc(void* param) {
  tsem_t* sem = (tsem_t*)param;
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);

  tsem_wait(sem);

  TAOS_RES* pRes = taos_query(pConn, "insert into d1.table1 (ts, c1) values (now, 11)");
  ASSERT(taos_errno(pRes) == 0);
  printf("insert data again\n");
  taos_free_result(pRes);
  taos_close(pConn);
  return NULL;
}

int main(int argc, char* argv[]) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  test_vgroup_error(pConn);
  test_stable_db_error(pConn);

  tsem_t sem;
  tsem_init(&sem, 0, 0);
  TdThread thread;
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  // pthread_create one thread to consume
  taosThreadCreate(&thread, &thattr, insertThreadFunc, (void*)(&sem));

  int32_t interval[5] = {1000, 200, 3000, 40, 500};
  test_case1(pConn, interval, sizeof(interval)/sizeof(int32_t));
  printf("test_case1 success\n");
  test_case2(pConn, interval, sizeof(interval)/sizeof(int32_t), &sem);
  printf("test_case2 success\n");
  test_case3(pConn, interval, sizeof(interval)/sizeof(int32_t));
  taos_close(pConn);

  taosThreadJoin(thread, NULL);
  taosThreadClear(&thread);
  tsem_destroy(&sem);
  return 0;
}
