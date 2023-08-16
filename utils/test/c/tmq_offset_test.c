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

int buildData(TAOS* pConn){
  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists tp");
  if (taos_errno(pRes) != 0) {
    printf("error in drop tp, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists db_ts3756");
  if (taos_errno(pRes) != 0) {
    printf("error in drop db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create database if not exists db_ts3756 vgroups 2 wal_retention_period 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create db_taosx, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "use db_ts3756");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn,"CREATE TABLE `t1` (`ts` TIMESTAMP, `voltage` INT)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create table meters, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into t1 values(now, 1)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into t1 values(now + 1s, 2)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic tp as select * from t1");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic tp, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  return 0;
}

void test_offset(TAOS* pConn){
  if(buildData(pConn) != 0){
    ASSERT(0);
  }
  tmq_conf_t* conf = tmq_conf_new();

  tmq_conf_set(conf, "enable.auto.commit", "false");
  tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  tmq_conf_set(conf, "group.id", "group_id_2");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.with.table.name", "false");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  tmq_list_append(topicList, "tp");

  // 启动订阅
  tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  int32_t     timeout = 200;

  tmq_topic_assignment* pAssign1 = NULL;
  int32_t numOfAssign1 = 0;

  tmq_topic_assignment* pAssign2 = NULL;
  int32_t numOfAssign2 = 0;

  tmq_topic_assignment* pAssign3 = NULL;
  int32_t numOfAssign3 = 0;

  int32_t code = tmq_get_topic_assignment(tmq, "tp", &pAssign1, &numOfAssign1);
  if (code != 0) {
    printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign1);
    tmq_consumer_close(tmq);
    ASSERT(0);
  }

  code = tmq_get_topic_assignment(tmq, "tp", &pAssign2, &numOfAssign2);
  if (code != 0) {
    printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign2);
    tmq_consumer_close(tmq);
    ASSERT(0);
  }

  code = tmq_get_topic_assignment(tmq, "tp", &pAssign3, &numOfAssign3);
  if (code != 0) {
    printf("error occurs:%s\n", tmq_err2str(code));
    tmq_free_assignment(pAssign3);
    tmq_consumer_close(tmq);
    ASSERT(0);
    return;
  }

  ASSERT(numOfAssign1 == 2);
  ASSERT(numOfAssign1 == numOfAssign2);
  ASSERT(numOfAssign1 == numOfAssign3);

  for(int i = 0; i < numOfAssign1; i++){
    int j = 0;
    int k = 0;
    for(; j < numOfAssign2; j++){
      if(pAssign1[i].vgId == pAssign2[j].vgId){
        break;
      }
    }
    for(; k < numOfAssign3; k++){
      if(pAssign1[i].vgId == pAssign3[k].vgId){
        break;
      }
    }

    ASSERT(pAssign1[i].currentOffset == pAssign2[j].currentOffset);
    ASSERT(pAssign1[i].currentOffset == pAssign3[k].currentOffset);

    ASSERT(pAssign1[i].begin == pAssign2[j].begin);
    ASSERT(pAssign1[i].begin == pAssign3[k].begin);

    ASSERT(pAssign1[i].end == pAssign2[j].end);
    ASSERT(pAssign1[i].end == pAssign3[k].end);
  }
  tmq_free_assignment(pAssign1);
  tmq_free_assignment(pAssign2);
  tmq_free_assignment(pAssign3);

  int cnt = 0;
  int offset1 = -1;
  int offset2 = -1;
  while (cnt++ < 10) {
    printf("start to poll:%d\n", cnt);
    TAOS_RES* pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      tmq_topic_assignment* pAssign = NULL;
      int32_t numOfAssign = 0;

      code = tmq_get_topic_assignment(tmq, "tp", &pAssign, &numOfAssign);
      if (code != 0) {
        printf("error occurs:%s\n", tmq_err2str(code));
        tmq_free_assignment(pAssign);
        tmq_consumer_close(tmq);
        ASSERT(0);
      }

      for(int i = 0; i < numOfAssign; i++){
        int64_t position = tmq_position(tmq, "tp", pAssign[i].vgId);
        if(position == 0) continue;

        printf("position = %d\n", (int)position);
        tmq_commit_offset_sync(tmq, "tp", pAssign[i].vgId, position);
        int64_t committed = tmq_committed(tmq, "tp", pAssign[i].vgId);
        ASSERT(position == committed);
      }

      tmq_offset_seek(tmq, "tp", pAssign[0].vgId, pAssign[0].currentOffset);
      tmq_offset_seek(tmq, "tp", pAssign[1].vgId, pAssign[1].currentOffset);

      if(offset1 != -1){
        ASSERT(offset1 == pAssign[0].currentOffset);
      }
      if(offset2 != -1){
        ASSERT(offset2 == pAssign[1].currentOffset);
      }

      offset1 = pAssign[0].currentOffset;
      offset2 = pAssign[1].currentOffset;

      tmq_free_assignment(pAssign);

      taos_free_result(pRes);
    }
  }

  tmq_consumer_close(tmq);
}

// run taosBenchmark first
void test_ts3756(TAOS* pConn){
  TAOS_RES*pRes = taos_query(pConn, "use test");
  if (taos_errno(pRes) != 0) {
    ASSERT(0);
  }
  taos_free_result(pRes);
  pRes = taos_query(pConn, "drop topic if exists t1");
  if (taos_errno(pRes) != 0) {
    ASSERT(0);
  }
  taos_free_result(pRes);
  pRes = taos_query(pConn, "create topic t1 as select * from meters");
  if (taos_errno(pRes) != 0) {
    ASSERT(0);
  }
  taos_free_result(pRes);
  tmq_conf_t* conf = tmq_conf_new();

  tmq_conf_set(conf, "enable.auto.commit", "false");
  tmq_conf_set(conf, "auto.commit.interval.ms", "2000");
  tmq_conf_set(conf, "group.id", "group_id_2");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "auto.offset.reset", "latest");
  tmq_conf_set(conf, "msg.with.table.name", "false");

  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  tmq_conf_destroy(conf);

  // 创建订阅 topics 列表
  tmq_list_t* topicList = tmq_list_new();
  tmq_list_append(topicList, "t1");

  // 启动订阅
  tmq_subscribe(tmq, topicList);
  tmq_list_destroy(topicList);

  int32_t     timeout = 200;

  tmq_topic_assignment* pAssign = NULL;
  int32_t numOfAssign = 0;

  while (1) {
//    printf("start to poll\n");

    pRes = tmq_consumer_poll(tmq, timeout);
    if (pRes) {
      tmq_topic_assignment* pAssignTmp = NULL;
      int32_t numOfAssignTmp = 0;

      int32_t code = tmq_get_topic_assignment(tmq, "t1", &pAssignTmp, &numOfAssignTmp);
      if (code != 0) {
        printf("error occurs:%s\n", tmq_err2str(code));
        tmq_free_assignment(pAssign);
        tmq_consumer_close(tmq);
        ASSERT(0);
      }

      if(numOfAssign != 0){
        int i = 0;
        for(; i < numOfAssign; i++){
          if(pAssign[i].currentOffset != pAssignTmp[i].currentOffset){
            break;
          }
        }
        if(i == numOfAssign){
          ASSERT(0);
        }
        tmq_free_assignment(pAssign);
      }
      numOfAssign = numOfAssignTmp;
      pAssign = pAssignTmp;
      taos_free_result(pRes);
    }
  }

  tmq_free_assignment(pAssign);
}

int main(int argc, char* argv[]) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  test_offset(pConn);
  test_ts3756(pConn);
  taos_close(pConn);
  return 0;
}
