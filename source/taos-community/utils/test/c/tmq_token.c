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
char* token = NULL;
tsem2_t sem;
tsem2_t sem1;


void create_topic() {
  printf("create topic\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  pRes = taos_query(pConn, "drop topic if exists t1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic t1 as database tmq_token");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create token token1 from user root enable 0 ttl 1");
  ASSERT(taos_errno(pRes) == 0);
  TAOS_ROW row = taos_fetch_row(pRes);
  if (row != NULL && *row != NULL) {
    printf("token: %s\n", (char *)(*row));
    token = strdup((char *)(*row));
    ASSERT(token != NULL);
  }
  taos_free_result(pRes);

  taos_close(pConn);
}

void alter_token_enable() {
  printf("alter token enable\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  pRes = taos_query(pConn, "alter token token1 enable 1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

void alter_token_disable() {
  printf("alter token disable\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  pRes = taos_query(pConn, "alter token token1 enable 0");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

void drop_token() {
  printf("drop token\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  pRes = taos_query(pConn, "drop token token1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

void* consumeThreadFunc(void* param) {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "enable.auto.commit", "false");
  tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  tmq_conf_set(conf, "group.id", "group_1");
  tmq_conf_set(conf, "td.connect.token", token);
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.with.table.name", "false");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  ASSERT(tmq == NULL);
  ASSERT(taos_errno(NULL) == TSDB_CODE_MND_TOKEN_DISABLED);

  ASSERT(tsem2_post(&sem1) == 0);
  ASSERT(tsem2_wait(&sem) == 0);
  tmq = tmq_consumer_new(conf, NULL, 0);
  ASSERT(tmq != NULL);
  tmq_conf_destroy(conf);

  // create topic list
  tmq_list_t* topicList = tmq_list_new();
  ASSERT(tmq_list_append(topicList, topic_name) == 0);

  // start subscription
  ASSERT(tmq_subscribe(tmq, topicList) == 0);
  tmq_list_destroy(topicList);

  while (1) {
    printf("start to poll\n");

    TAOS_RES *pRes = tmq_consumer_poll(tmq, 1000);
    printf("pRes is %p, reason:%s\n", pRes, taos_errstr(NULL));

    if (pRes == NULL && taos_errno(NULL) == TSDB_CODE_MND_TOKEN_NOT_EXIST) {
      printf("break, pRes is NULL, reason:%s\n", taos_errstr(NULL));
      break;
    }
    taos_free_result(pRes);
    taosMsleep(200);
  }
  tmq_consumer_close(tmq);
  return NULL;
}

void* alterTokenThreadFunc(void* param) {
  ASSERT(tsem2_wait(&sem1) == 0);
  alter_token_enable();
  ASSERT(tsem2_post(&sem) == 0);
  taosSsleep(5);
  alter_token_disable();
  taosSsleep(5);
  alter_token_enable();
  taosSsleep(5);
  drop_token();
  return NULL;
}

int main(int argc, char* argv[]) {
  printf("test start.........\n");

  int32_t runTimes = 1;
  TdThread thread1, thread2;
  TdThreadAttr thattr;
  taosThreadAttrInit(&thattr);
  taosThreadAttrSetDetachState(&thattr, PTHREAD_CREATE_JOINABLE);

  int32_t code = tsem2_init(&sem, 0, 0);
  ASSERT(code == 0);
  code = tsem2_init(&sem1, 0, 0);
  ASSERT(code == 0);
  for (int i = 0; i < runTimes; i++){
    create_topic();
    taosThreadCreate(&(thread1), &thattr, alterTokenThreadFunc, NULL);
    taosThreadCreate(&(thread2), &thattr, consumeThreadFunc, NULL);

    taosThreadJoin(thread1, NULL);
    taosThreadClear(&thread1);

    taosThreadJoin(thread2, NULL);
    taosThreadClear(&thread2);
  }
  code = tsem2_destroy(&sem);
  ASSERT(code == 0);
  code = tsem2_destroy(&sem1);
  ASSERT(code == 0);
  taosMemFree(token);
  printf("test end.........\n");
  return 0;
}
