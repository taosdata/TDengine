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

#include <gtest/gtest.h>

#include <taoserror.h>
#include <tglobal.h>
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"

#include "../inc/clientInt.h"
#include "taos.h"

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(testCase, driverInit_Test) {
  // taosInitGlobalCfg();
  //  taos_init();
}

TEST(testCase, create_topic_ctb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == nullptr);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);

  taos_free_result(pRes);

  // char* sql = "select * from tu";
  // pRes = tmq_create_topic(pConn, "test_ctb_topic_1", sql, strlen(sql));
  pRes = taos_query(pConn, "create test_ctb_topic_1 as select * from tu");
  taos_free_result(pRes);
  taos_close(pConn);
}

TEST(testCase, create_topic_stb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  // taos_free_result(pRes);

  TAOS_FIELD* pFields = taos_fetch_fields(pRes);
  ASSERT_TRUE(pFields == nullptr);

  int32_t numOfFields = taos_num_fields(pRes);
  ASSERT_EQ(numOfFields, 0);

  taos_free_result(pRes);

  // char* sql = "select * from st1";
  // pRes = tmq_create_topic(pConn, "test_stb_topic_1", sql, strlen(sql));
  pRes = taos_query(pConn, "create test_ctb_topic_1 as select * from st1");
  taos_free_result(pRes);
  taos_close(pConn);
}

#if 0
TEST(testCase, tmq_subscribe_ctb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg1");
  tmq_t* tmq = tmq_consumer_new(pConn, conf, NULL, 0);

  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "test_ctb_topic_1");
  tmq_subscribe(tmq, topic_list);

  while (1) {
    tmq_message_t* msg = tmq_consumer_poll(tmq, 1000);
    taos_free_result(msg);
    //printf("get msg\n");
    //if (msg == NULL) break;
  }
}

TEST(testCase, tmq_subscribe_stb_Test) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  assert(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_t* tmq = tmq_consumer_new(pConn, conf, NULL, 0);

  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "test_stb_topic_1");
  tmq_subscribe(tmq, topic_list);
  
  int cnt = 1;
  while (1) {
    tmq_message_t* msg = tmq_consumer_poll(tmq, 1000);
    if (msg == NULL) continue;
    tmqShowMsg(msg);
    if (cnt++ % 10 == 0){
      tmq_commit(tmq, NULL, 0);
    }
    //tmq_commit(tmq, NULL, 0);
    taos_free_result(msg);
    //printf("get msg\n");
  }
}

TEST(testCase, tmq_consume_Test) {
}

TEST(testCase, tmq_commit_Test) {
}

#endif
