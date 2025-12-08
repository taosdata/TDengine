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

char* meta_json_result[10] = {
  "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"columns\":[{\"name\":\"_ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"uid\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"},{\"name\":\"deviceId\",\"type\":8,\"length\":2,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[{\"name\":\"tag\",\"type\":10,\"length\":16}]}",
  "",
  "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"alterType\":7,\"colName\":\"uid\",\"colType\":8,\"colLength\":32}",
  "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"alterType\":5,\"colName\":\"alarmId\",\"colType\":8,\"colLength\":8}",
  "",
  "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"alterType\":1,\"colName\":\"t\",\"colType\":10,\"colLength\":4}",
  "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"alterType\":8,\"colName\":\"tag\",\"colType\":10,\"colLength\":32}",
  "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"alterType\":7,\"colName\":\"deviceId\",\"colType\":8,\"colLength\":16}",
  "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"alarm_record\",\"alterType\":5,\"colName\":\"numId\",\"colType\":8,\"colLength\":8}",
  ""
};
static int idx = 0;

static void msg_process(TAOS_RES* msg) {
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  if (tmq_get_res_type(msg) == TMQ_RES_TABLE_META || tmq_get_res_type(msg) == TMQ_RES_METADATA) {
    char* result = tmq_get_json_meta(msg);
    printf("meta result: %s\n", result);
    if (idx == 0 || idx == 2 || idx == 3 || idx == 5 || idx == 6 || idx == 7 || idx == 8) {
      if (strcmp(result, meta_json_result[idx]) != 0) {
        printf("meta json not match, expect:%s\n", meta_json_result[idx]);
        assert(0);
      }
    }
    idx++;
    tmq_free_json_meta(result);
  }
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("commit %d tmq %p param %p\n", code, tmq, param);
}

tmq_t* build_consumer() {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg233");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  // tmq_conf_set(conf, "msg.enable.batchmeta", "1");
  
  // tmq_conf_set(conf, "msg.consume.excluded", "1");
  // tmq_conf_set(conf, "experimental.snapshot.enable", "true");

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "t_get_meta_json");
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

void insertData() {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);

  TAOS_RES *pRes = taos_query(taos, "drop database if exists get_meta_json");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create database if not exists get_meta_json vgroups 1");
  taos_free_result(pRes);

  pRes = taos_query(taos, "create topic if not exists t_get_meta_json only meta as database get_meta_json");
  taos_free_result(pRes);

  // check column name duplication
  const char *sql[] = {
      "alarm_record,tag=alarm_record uid=\"3+8001+c939604c\",deviceId=\"3\" 1732527117484",
  };
  pRes = taos_query(taos, "use get_meta_json");
  taos_free_result(pRes);
  pRes = taos_schemaless_insert(taos, (char **)sql, sizeof(sql) / sizeof(sql[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  int code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  ASSERT(code == 0);
  taos_free_result(pRes);

  // check tag name duplication
  const char *sql1[] = {
      "alarm_record,tag=alarm_record1 uid=\"2+100012+303fe9b5\",deviceId=\"2\",alarmId=\"100012\" 1732527119493",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql1, sizeof(sql1) / sizeof(sql1[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  ASSERT(code == 0);
  taos_free_result(pRes);

  // check tag name duplication
  const char *sql2[] = {
    "alarm_record,t=34t,tag=alarm_recordr3331 uid=\"2+100012+303fe9b5\",deviceId=\"2iiiiiii\",numId=\"100012\" 1732527119497",
  };
  pRes = taos_schemaless_insert(taos, (char **)sql2, sizeof(sql2) / sizeof(sql2[0]), TSDB_SML_LINE_PROTOCOL,
                                TSDB_SML_TIMESTAMP_MILLI_SECONDS);
  code = taos_errno(pRes);
  printf("%s result0:%s\n", __FUNCTION__, taos_errstr(pRes));
  ASSERT(code == 0);
  taos_free_result(pRes);

  taos_close(taos);
}

int main(int argc, char* argv[]) {
  insertData();
  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  tmq_list_destroy(topic_list);
}