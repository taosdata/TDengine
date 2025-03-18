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
#include "types.h"

const char* topic_name = "t1";
bool  pollStart = false;

int32_t create_topic() {
  printf("create topic\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic t1 as database ts6115");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

void* consumeThreadFunc(void* param) {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "enable.auto.commit", "false");
  tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  tmq_conf_set(conf, "group.id", "group_1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.with.table.name", "false");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  tmq_list_append(topicList, topic_name);

  // 启动订阅
  tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  int32_t     timeout = 200;
  while (1) {
    printf("start to poll\n");

    TAOS_RES *pRes = tmq_consumer_poll(tmq, timeout);
    pollStart = true;
    if (pRes == NULL) {
      printf("pRes is NULL, reason:%s\n", taos_errstr(NULL));
      break;
    }
    taos_free_result(pRes);
    taosMsleep(200);
  }
  tmq_consumer_close(tmq);
  return NULL;
}

void* dropTopicThreadFunc(void* param) {
  printf("drop topic\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  while(!pollStart) {
    taosSsleep(taosRand()%5);
  }
  pRes = taos_query(pConn, "drop topic force t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "show consumers");
  ASSERT(taos_errno(pRes) == 0);
  ASSERT(taos_affected_rows(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
  return NULL;
}

void* dropCGroupThreadFunc(void* param) {
  printf("drop topic\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  while(!pollStart) {
    taosSsleep(taosRand()%5);
  }

  pRes = taos_query(pConn, "drop consumer group force group_1 on t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "show consumers");
  ASSERT(taos_errno(pRes) == 0);
  ASSERT(taos_affected_rows(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
  return NULL;
}

int main(int argc, char* argv[]) {
  printf("test start.........\n");

  int32_t runTimes = 1;
  TdThread thread1, thread2;
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  for (int i = 0; i < runTimes; i++){
    printf("test drop topic times:%d\n", i);

    pollStart = false;
    create_topic();
    taosThreadCreate(&(thread1), &thattr, dropTopicThreadFunc, NULL);
    taosThreadCreate(&(thread2), &thattr, consumeThreadFunc, NULL);

    taosThreadJoin(thread1, NULL);
    taosThreadClear(&thread1);

    taosThreadJoin(thread2, NULL);
    taosThreadClear(&thread2);
  }

  create_topic();

  for (int i = 0; i < runTimes; i++){
    printf("test drop consumer group times:%d\n", i);

    pollStart = false;
    taosThreadCreate(&(thread1), &thattr, dropCGroupThreadFunc, NULL);
    taosThreadCreate(&(thread2), &thattr, consumeThreadFunc, NULL);

    taosThreadJoin(thread1, NULL);
    taosThreadClear(&thread1);

    taosThreadJoin(thread2, NULL);
    taosThreadClear(&thread2);
  }

  printf("test end.........\n");
  return 0;
}
