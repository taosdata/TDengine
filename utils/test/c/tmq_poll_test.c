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

TAOS_RES* pRes = NULL;
TAOS* pConn = NULL;
TAOS_RES* tmqmessage = NULL;

#define EXEC_SQL(sql) \
  pRes = taos_query(pConn,sql);\
  ASSERT(taos_errno(pRes) == 0);\
  taos_free_result(pRes)

void init_env() {
  EXEC_SQL("drop topic if exists topic_db");
  EXEC_SQL("drop database if exists db_src");
  EXEC_SQL("create database if not exists db_src vgroups 1 wal_retention_period 3600");
  EXEC_SQL("use db_src");
  EXEC_SQL("create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 nchar(8), t4 bool)");
  EXEC_SQL("insert into ct3 using st1(t1) tags(3000) values(1626006833600, 5, 6, 'c')");

  EXEC_SQL("create topic topic_db as database db_src");
}



tmq_t* build_consumer(bool testLongHeartBeat) {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");

  if (testLongHeartBeat){
    ASSERT(tmq_conf_set(conf, "session.timeout.ms", "8000") == TMQ_CONF_OK);
    ASSERT(tmq_conf_set(conf, "heartbeat.interval.ms", "100000") == TMQ_CONF_OK);
  }

  tmq_conf_set_auto_commit_cb(conf, NULL, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  ASSERT(tmq != NULL);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_db");
  return topic_list;
}

void test_poll_continuity(tmq_t* tmq, tmq_list_t* topics) {
  ASSERT ((tmq_subscribe(tmq, topics)) == 0);
  tmqmessage = tmq_consumer_poll(tmq, 500);
  ASSERT (tmqmessage != NULL);
  taos_free_result(tmqmessage);

  ASSERT (tmq_unsubscribe(tmq) == 0);
  printf("unsubscribe success\n");

  ASSERT (tmq_subscribe(tmq, topics) == 0);
  printf("subscribe success\n");

  tmqmessage = tmq_consumer_poll(tmq, 500);
  ASSERT (tmqmessage == NULL);
  taos_free_result(tmqmessage);

  EXEC_SQL("insert into ct1 using st1(t1) tags(3000) values(1626006833600, 5, 6, 'c')");
  printf("insert into ct1\n");

  tmqmessage = tmq_consumer_poll(tmq, 500);
  ASSERT (tmqmessage != NULL);
  taos_free_result(tmqmessage);
}


void test_consumer_offline(tmq_t* tmq, tmq_list_t* topics) {
  ASSERT ((tmq_subscribe(tmq, topics)) == 0);
  tmqmessage = tmq_consumer_poll(tmq, 500);
  ASSERT (tmqmessage != NULL);
  taos_free_result(tmqmessage);

  taosSsleep(15);

  tmqmessage = tmq_consumer_poll(tmq, 500);
  ASSERT (tmqmessage == NULL);
  ASSERT (taos_errno(NULL) == TSDB_CODE_MND_CONSUMER_NOT_EXIST);
  taos_free_result(tmqmessage);
}

int main(int argc, char* argv[]) {
  pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT (pConn != NULL);
  printf("test poll continuity\n");
  for (int i = 0; i < 10; i++){
    printf("-------run times:%d start---------\n", i);
    init_env();
    tmq_t*      tmq = build_consumer(false);
    tmq_list_t* topic_list = build_topic_list();
    test_poll_continuity(tmq, topic_list);
    ASSERT(tmq_consumer_close(tmq) == 0);
    tmq_list_destroy(topic_list);
    printf("-------run times:%d end---------\n\n", i);
  }

//  printf("\n\n\ntest consumer offline\n");
//  init_env();
//  tmq_t*      tmq = build_consumer(true);
//  tmq_list_t* topic_list = build_topic_list();
//  test_consumer_offline(tmq, topic_list);
//  ASSERT(tmq_consumer_close(tmq) == 0);
//  tmq_list_destroy(topic_list);

  taos_close(pConn);

}
