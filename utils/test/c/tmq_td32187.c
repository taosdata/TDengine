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


static TAOS_RES* tmqmessage = NULL;
static char* topic = "topic_test";
static int32_t vgroupId = 0;
static int64_t offset = 0;

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("commit %d tmq %p param %p\n", code, tmq, param);
}

tmq_t* build_consumer() {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.consume.excluded", "1");
//  tmq_conf_set(conf, "experimental.snapshot.enable", "true");

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, topic);
  return topic_list;
}

static void callFunc(int i, tmq_t* tmq, tmq_list_t* topics) {
  printf("call %d\n", i);
  switch (i) {
    case 0:
      tmq_subscribe(tmq, topics);
      break;
    case 1:
      tmq_unsubscribe(tmq);
      break;
    case 2:{
      tmq_list_t* t = NULL;
      tmq_subscription(tmq, &t);
      tmq_list_destroy(t);
      break;
    }
    case 3:
      taos_free_result(tmqmessage);
      tmqmessage = tmq_consumer_poll(tmq, 5000);
      break;
    case 4:
//      tmq_consumer_close(tmq);
      break;
    case 5:
      tmq_commit_sync(tmq, NULL);
      break;
    case 6:
      tmq_commit_async(tmq, NULL, NULL, NULL);
      break;
    case 7:
      tmq_commit_offset_sync(tmq, topic,  vgroupId, offset);
      break;
    case 8:
      tmq_commit_offset_async(tmq, topic, vgroupId, offset, NULL, NULL);
      break;
    case 9:
      tmq_get_topic_assignment(tmq, topic, NULL, NULL);
      break;
    case 10:
      tmq_free_assignment(NULL);
      break;
    case 11:
      tmq_offset_seek(tmq, topic, vgroupId, offset);
      break;
    case 12:
      tmq_position(tmq, topic, vgroupId);
      break;
    case 13:
      tmq_committed(tmq, topic, vgroupId);
      break;
    case 14:
      tmq_get_connect(tmq);
      break;
    case 15:
      tmq_get_table_name(tmqmessage);
      break;
    case 16:
      vgroupId = tmq_get_vgroup_id(tmqmessage);
      break;
    case 17:
      offset = tmq_get_vgroup_offset(tmqmessage);
      break;
    case 18:
      tmq_get_res_type(tmqmessage);
      break;
    case 19:
      tmq_get_topic_name(tmqmessage);
      break;
    case 20:
      tmq_get_db_name(tmqmessage);
      break;
    default:
      break;
  }
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
    tmqmessage = tmq_consumer_poll(tmq, 5000);
    if (tmqmessage) {
      printf("poll message\n");
      while(cnt < 100){
        uint32_t i = taosRand()%21;
        callFunc(i, tmq, topics);
        callFunc(i, tmq, topics);
        cnt++;
      }
      while(cnt < 300){
        uint32_t i = taosRand()%21;
        callFunc(i, tmq, topics);
        cnt++;
      }
      taos_free_result(tmqmessage);
    }
    break;
  }

  code = tmq_consumer_close(tmq);
  if (code)
    fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(code));
  else
    fprintf(stderr, "%% Consumer closed\n");
}

int main(int argc, char* argv[]) {
  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  tmq_list_destroy(topic_list);
}