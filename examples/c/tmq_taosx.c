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
#include "taos.h"

static int  running = 1;

static TAOS* use_db(){
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

static void msg_process(TAOS_RES* msg) {
  /*memset(buf, 0, 1024);*/
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  TAOS *pConn = use_db();
  if (tmq_get_res_type(msg) == TMQ_RES_TABLE_META) {
    char* result = tmq_get_json_meta(msg);
    if (result) {
      printf("meta result: %s\n", result);
    }
    tmq_free_json_meta(result);
  }

  tmq_raw_data raw = {0};
  tmq_get_raw(msg, &raw);
  int32_t ret = tmq_write_raw(pConn, raw);
  printf("write raw data: %s\n", tmq_err2str(ret));

//  else{
//    while(1){
//      int numOfRows = 0;
//      void *pData = NULL;
//      taos_fetch_raw_block(msg, &numOfRows, &pData);
//      if(numOfRows == 0) break;
//      printf("write data: tbname:%s, numOfRows:%d\n", tmq_get_table_name(msg), numOfRows);
//      int ret = taos_write_raw_block(pConn, numOfRows, pData, tmq_get_table_name(msg));
//      printf("write raw data: %s\n", tmq_err2str(ret));
//    }
//  }

  taos_close(pConn);
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

  pRes = taos_query(pConn, "create database if not exists db_taosx vgroups 4");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists abc1 vgroups 3");
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

  pRes = taos_query(pConn,
                    "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
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

  pRes = taos_query(pConn, "insert into ct0 values(1626006833600, 1, 2, 'a')");
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

  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
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

  pRes = taos_query(pConn, "insert into ct3 values(1626006833600, 5, 6, 'c') ct1 values(1626006833601, 2, 3, 'sds') (1626006833602, 4, 5, 'ddd') ct0 values(1626006833602, 4, 3, 'hwj') ct1 values(now+5s, 23, 32, 's21ds')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add column c4 bigint");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 modify column c3 binary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct3 values(1626006833605, 53, 63, 'cffffffffffffffffffffffffffff', 8989898899999) (1626006833609, 51, 62, 'c333', 940)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct3 select * from ct1");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add tag t2 binary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct3 set tag t1=5000");
  if (taos_errno(pRes) != 0) {
    printf("failed to slter child table ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "delete from abc1   .ct3 where ts < 1626006833606");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop table ct3 ct1");
  if (taos_errno(pRes) != 0) {
    printf("failed to drop child table ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop table st1");
  if (taos_errno(pRes) != 0) {
    printf("failed to drop super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists n1(ts timestamp, c1 int, c2 nchar(4))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 add column c3 bigint");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 modify column c2 nchar(8)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 rename column c3 cc3");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 comment 'hello'");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 drop column c1");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into n1 values(now, 'eeee', 8989898899999) (now+9s, 'c333', 940)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop table n1");
  if (taos_errno(pRes) != 0) {
    printf("failed to drop normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt(ts timestamp, i int) tags(t json)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt1 using jt tags('{\"k1\":1, \"k2\":\"hello\"}')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt2 using jt tags('')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop table st1");
  if (taos_errno(pRes) != 0) {
    printf("failed to drop super table st1, reason:%s\n", taos_errstr(pRes));
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

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic topic_ctb_column with meta as database abc1");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_ctb_column, reason:%s\n", taos_errstr(pRes));
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
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");

  /*tmq_conf_set(conf, "experimental.snapshot.enable", "true");*/

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_ctb_column");
  /*tmq_list_append(topic_list, "tmq_test_db_multi_insert_topic");*/
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
  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, -1);
    if (tmqmessage) {
      cnt++;
      msg_process(tmqmessage);
      /*if (cnt >= 2) break;*/
      /*printf("get data\n");*/
      taos_free_result(tmqmessage);
      /*} else {*/
      /*break;*/
      /*tmq_commit_sync(tmq, NULL);*/
    }
  }

  code = tmq_consumer_close(tmq);
  if (code)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(code));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

void sync_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  static const int MIN_COMMIT_COUNT = 1;

  int     msg_count = 0;
  int32_t code;

  if ((code = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(code));
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

      /*tmq_commit_sync(tmq, NULL);*/
      /*if ((++msg_count % MIN_COMMIT_COUNT) == 0) tmq_commit(tmq, NULL, 0);*/
    }
  }

  code = tmq_consumer_close(tmq);
  if (code)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(code));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

int main(int argc, char* argv[]) {
  printf("env init\n");
  if (init_env() < 0) {
    return -1;
  }
  create_topic();

  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  /*sync_consume_loop(tmq, topic_list);*/
}
