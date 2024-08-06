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
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "taos.h"

volatile int thread_stop = 0;
static int   running = 1;
const char*  topic_name = "topic_meters";

void* prepare_data(void* arg) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return NULL;
  }

  TAOS_RES* pRes;
  int       i = 1;

  while (!thread_stop) {
    char buf[200] = {0};
    i++;
    snprintf(
        buf, sizeof(buf),
        "INSERT INTO power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') VALUES (NOW + %da, 10.30000, "
        "219, 0.31000)",
        i);

    pRes = taos_query(pConn, buf);
    if (taos_errno(pRes) != 0) {
      printf("error in insert data to power.meters, reason:%s\n", taos_errstr(pRes));
    }
    taos_free_result(pRes);
    sleep(1);
  }
  printf("prepare data thread exit\n");
  return NULL;
}

// ANCHOR: msg_process
static int32_t msg_process(TAOS_RES* msg) {
  char        buf[1024];  // buf to store the row content
  int32_t     rows = 0;
  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName = tmq_get_db_name(msg);
  int32_t     vgroupId = tmq_get_vgroup_id(msg);

  printf("topic: %s\n", topicName);
  printf("db: %s\n", dbName);
  printf("vgroup id: %d\n", vgroupId);

  while (1) {
    // get one row data from message
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;

    // get the field information
    TAOS_FIELD* fields = taos_fetch_fields(msg);
    // get the number of fields
    int32_t numOfFields = taos_field_count(msg);
    // get the precision of the result
    int32_t precision = taos_result_precision(msg);
    rows++;
    // print the row content
    if (taos_print_row(buf, row, fields, numOfFields) < 0) {
      printf("failed to print row\n");
      break;
    }
    // print the precision and row content to the console
    printf("precision: %d, row content: %s\n", precision, buf);
  }

  return rows;
}
// ANCHOR_END: msg_process

static int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  TAOS_RES* pRes;
  // drop database if exists
  printf("create database\n");
  pRes = taos_query(pConn, "drop topic if exists topic_meters");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topic_meters, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists power");
  if (taos_errno(pRes) != 0) {
    printf("error in drop power, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  // create database
  pRes = taos_query(pConn, "create database power precision 'ms' WAL_RETENTION_PERIOD 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create tmqdb, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  // create super table
  printf("create super table\n");
  pRes = taos_query(
      pConn,
      "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
      "(groupId INT, location BINARY(24))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table meters, reason:%s\n", taos_errstr(pRes));
    goto END;
  }

  taos_free_result(pRes);
  taos_close(pConn);
  return 0;

END:
  taos_free_result(pRes);
  taos_close(pConn);
  return -1;
}

int32_t create_topic() {
  printf("create topic\n");
  TAOS_RES* pRes;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  pRes = taos_query(pConn, "use power");
  if (taos_errno(pRes) != 0) {
    printf("error in use tmqdb, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topic_meters, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("tmq_commit_cb_print() code: %d, tmq: %p, param: %p\n", code, tmq, param);
}

// ANCHOR: create_consumer_1
tmq_t* build_consumer() {
  tmq_conf_res_t code;
  tmq_t*         tmq = NULL;

  // create a configuration object
  tmq_conf_t* conf = tmq_conf_new();

  // set the configuration parameters
  code = tmq_conf_set(conf, "enable.auto.commit", "true");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "group.id", "group1");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "client.id", "client1");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.user", "root");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.pass", "taosdata");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "auto.offset.reset", "latest");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  // set the callback function for auto commit
  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  // create a consumer object
  tmq = tmq_consumer_new(conf, NULL, 0);

_end:
  // destroy the configuration object
  tmq_conf_destroy(conf);
  return tmq;
}
// ANCHOR_END: create_consumer_1

// ANCHOR: build_topic_list
// build a topic list used to subscribe
tmq_list_t* build_topic_list() {
  // create a empty topic list
  tmq_list_t* topicList = tmq_list_new();
  const char* topic_name = "topic_meters";

  // append topic name to the list
  int32_t code = tmq_list_append(topicList, topic_name);
  if (code) {
    // if failed, destroy the list and return NULL
    tmq_list_destroy(topicList);
    return NULL;
  }
  // if success, return the list
  return topicList;
}
// ANCHOR_END: build_topic_list

// ANCHOR: basic_consume_loop
void basic_consume_loop(tmq_t* tmq) {
  int32_t totalRows = 0;   // total rows consumed
  int32_t msgCnt = 0;      // total messages consumed
  int32_t timeout = 5000;  // poll timeout

  while (running) {
    // poll message from TDengine
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;
      // process the message
      totalRows += msg_process(tmqmsg);
      // free the message
      taos_free_result(tmqmsg);
    }
    if (msgCnt > 50) {
      // consume 50 messages and break
      break;
    }
  }

  // print the result: total messages and total rows consumed
  fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}
// ANCHOR_END: basic_consume_loop

// ANCHOR: consume_repeatly
void consume_repeatly(tmq_t* tmq) {
  int32_t               numOfAssignment = 0;
  tmq_topic_assignment* pAssign = NULL;

  // get the topic assignment
  int32_t code = tmq_get_topic_assignment(tmq, topic_name, &pAssign, &numOfAssignment);
  if (code != 0 || pAssign == NULL || numOfAssignment == 0) {
    fprintf(stderr, "failed to get assignment, reason:%s", tmq_err2str(code));
    return;
  }

  // seek to the earliest offset
  for (int32_t i = 0; i < numOfAssignment; ++i) {
    tmq_topic_assignment* p = &pAssign[i];

    code = tmq_offset_seek(tmq, topic_name, p->vgId, p->begin);
    if (code != 0) {
      fprintf(stderr, "failed to seek to %d, reason:%s", (int)p->begin, tmq_err2str(code));
    }
  }

  // free the assignment array
  tmq_free_assignment(pAssign);

  // let's consume the messages again
  basic_consume_loop(tmq);
}
// ANCHOR_END: consume_repeatly

// ANCHOR: manual_commit
void manual_commit(tmq_t* tmq) {
  int32_t totalRows = 0;   // total rows consumed
  int32_t msgCnt = 0;      // total messages consumed
  int32_t timeout = 5000;  // poll timeout

  while (running) {
    // poll message from TDengine
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;
      // process the message
      totalRows += msg_process(tmqmsg);
      // commit the message
      int32_t code = tmq_commit_sync(tmq, tmqmsg);

      if (code) {
        fprintf(stderr, "Failed to commit message: %s\n", tmq_err2str(code));
        // free the message
        taos_free_result(tmqmsg);
        break;
      }
      // free the message
      taos_free_result(tmqmsg);
    }
    if (msgCnt > 50) {
      // consume 50 messages and break
      break;
    }
  }

  // print the result: total messages and total rows consumed
  fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}
// ANCHOR_END: manual_commit

int main(int argc, char* argv[]) {
  int32_t   code;
  pthread_t thread_id;

  if (init_env() < 0) {
    return -1;
  }

  if (create_topic() < 0) {
    return -1;
  }

  if (pthread_create(&thread_id, NULL, &prepare_data, NULL)) {
    fprintf(stderr, "create thread failed\n");
    return 1;
  }

  // ANCHOR: create_consumer_2
  tmq_t* tmq = build_consumer();
  if (NULL == tmq) {
    fprintf(stderr, "build consumer to localhost fail!\n");
    return -1;
  }
  printf("build consumer to localhost successfully \n");

  // ANCHOR_END: create_consumer_2

  // ANCHOR: subscribe_3
  tmq_list_t* topic_list = build_topic_list();
  if (NULL == topic_list) {
    return -1;
  }

  if ((code = tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr, "Failed to tmq_subscribe(): %s\n", tmq_err2str(code));
  }

  tmq_list_destroy(topic_list);

  basic_consume_loop(tmq);
  // ANCHOR_END: subscribe_3

  consume_repeatly(tmq);

  manual_commit(tmq);

  // ANCHOR: unsubscribe_and_close
  // unsubscribe the topic
  code = tmq_unsubscribe(tmq);
  if (code) {
    fprintf(stderr, "Failed to tmq_unsubscribe(): %s\n", tmq_err2str(code));
  }
  fprintf(stderr, "Unsubscribed consumer successfully.\n");
  // close the consumer
  code = tmq_consumer_close(tmq);
  if (code) {
    fprintf(stderr, "Failed to close consumer: %s\n", tmq_err2str(code));
  } else {
    fprintf(stderr, "Consumer closed successfully.\n");
  }
  // ANCHOR_END: unsubscribe_and_close

  thread_stop = 1;
  pthread_join(thread_id, NULL);

  return 0;
}
