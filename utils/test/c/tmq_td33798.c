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

bool batchMeta = false;
int32_t consumeIndex = 0;
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

void checkBatchMeta(TAOS_RES* msg){
  char* result = tmq_get_json_meta(msg);
  printf("meta result: %s\n", result);
  switch (consumeIndex) {
    case 0:
      ASSERT(strcmp(result, "{\"tmq_meta_version\":\"1.0\",\"metas\":[{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"delta-d\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":1}]},{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]},{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]},{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct11\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]},{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct10\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]},{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[{\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]}]}]}") == 0);
      break;
    default:
      ASSERT(0);
      break;
  }

  tmq_free_json_meta(result);
}

void checkNonBatchMeta(TAOS_RES* msg){
  char* result = tmq_get_json_meta(msg);
  printf("meta result: %s\n", result);
  switch (consumeIndex) {
    case 0:
      ASSERT(strcmp(result, "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"delta-d\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":1}]}") == 0);
      break;
    case 1:
      ASSERT(strcmp(result, "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}") == 0);
      break;
    case 2:
      ASSERT(strcmp(result, "{\"tmq_meta_version\":\"1.0\",\"metas\":[{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}]}") == 0);
      break;
    case 3:
      ASSERT(strcmp(result, "{\"tmq_meta_version\":\"1.0\",\"metas\":[{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct11\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}]}") == 0);
      break;
    case 4:
      ASSERT(strcmp(result, "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct10\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}") == 0);
      break;
    case 5:
      ASSERT(strcmp(result, "{\"tmq_meta_version\":\"1.0\",\"metas\":[{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[{\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]}]}]}") == 0);
      break;
    default:
      ASSERT(0);
      break;
  }

  tmq_free_json_meta(result);
}

static void msg_process(TAOS_RES* msg) {
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  TAOS* pConn = use_db();
  ASSERT (tmq_get_res_type(msg) == TMQ_RES_TABLE_META);
  if (batchMeta){
    checkBatchMeta(msg);
  } else {
    checkNonBatchMeta(msg);
  }
  taos_close(pConn);
}

int buildDatabase(TAOS* pConn, TAOS_RES* pRes) {
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

  pRes = taos_query(pConn, "insert into ct1 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct11 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct10 using st1 tags(1000, \"ttt\", true)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table tu1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  taosSsleep(1);
  pRes = taos_query(pConn, "insert into ct1 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a') ct2 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a') ct3 using st1 tags(1000, \"ttt\", true) values(1626006833400, 1, 2, 'a')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);


  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct1, reason:%s\n", taos_errstr(pRes));
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

  pRes = taos_query(pConn, "create topic topic_db only meta as database abc1");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_db, reason:%s\n", taos_errstr(pRes));
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
  tmq_conf_set(conf, "group.id", batchMeta ? "batch" : "nonbatch");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.consume.excluded", "1");
  if (batchMeta) {
    tmq_conf_set(conf, "msg.enable.batchmeta", "1");
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
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
  int32_t code;

  if ((code = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(code));
    printf("subscribe err\n");
    return;
  }
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 5000);
    if (tmqmessage) {
      msg_process(tmqmessage);
      consumeIndex++;
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

  tmq_list_t* topic_list = build_topic_list();
  tmq_t*      tmq = build_consumer();
  basic_consume_loop(tmq, topic_list);

  batchMeta = true;
  consumeIndex = 0;
  tmq = build_consumer();
  basic_consume_loop(tmq, topic_list);

  tmq_list_destroy(topic_list);
}
