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

// to compile: gcc -o tmq_demo tmq_demo.c -ltaos -lpthread

#include <assert.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "taosws.h"

volatile int thread_stop = 0;
static int   running = 1;
static int   count = 0;
const char*  topic_name = "topic_meters";

typedef struct {
  const char* enable_auto_commit;
  const char* auto_commit_interval_ms;
  const char* group_id;
  const char* client_id;
  const char* td_connect_host;
  const char* td_connect_port;
  const char* td_connect_user;
  const char* td_connect_pass;
  const char* auto_offset_reset;
} ConsumerConfig;

ConsumerConfig config = {.enable_auto_commit = "true",
                         .auto_commit_interval_ms = "1000",
                         .group_id = "group1",
                         .client_id = "client1",
                         .td_connect_host = "localhost",
                         .td_connect_port = "6030",
                         .td_connect_user = "root",
                         .td_connect_pass = "taosdata",
                         .auto_offset_reset = "latest"};

void* prepare_data(void* arg) {
  int      code = 0;
  char*    dsn = "ws://localhost:6041";
  WS_TAOS* pConn = ws_connect(dsn);
  if (pConn == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return NULL;
  }

  WS_RES* pRes;
  int     i = 1;

  while (!thread_stop) {
    char buf[200] = {0};
    i++;
    snprintf(
        buf, sizeof(buf),
        "INSERT INTO power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') VALUES (NOW + %da, 10.30000, "
        "219, 0.31000)",
        i);

    pRes = ws_query(pConn, buf);
    code = ws_errno(pRes);
    if (code != 0) {
      fprintf(stderr, "Failed to insert data to power.meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    }
    ws_free_result(pRes);
    sleep(1);
  }
  fprintf(stdout, "Prepare data thread exit\n");
  return NULL;
}

// ANCHOR: msg_process
int32_t msg_process(WS_RES* msg) {
  int32_t     rows = 0;
  const char* topicName = ws_tmq_get_topic_name(msg);
  const char* dbName = ws_tmq_get_db_name(msg);
  int32_t     vgroupId = ws_tmq_get_vgroup_id(msg);

  while (true) {
    // get one row data from message
    WS_ROW row = ws_fetch_row(msg);
    if (row == NULL) break;

    // Add your data processing logic here

    rows++;
  }

  return rows;
}
// ANCHOR_END: msg_process

WS_TAOS* init_env() {
  int      code = 0;
  char*    dsn = "ws://localhost:6041";
  WS_TAOS* pConn = ws_connect(dsn);
  if (pConn == NULL) {
    fprintf(stderr, "Failed to connect to %s, ErrCode: 0x%x, ErrMessage: %s.\n", dsn, ws_errno(NULL), ws_errstr(NULL));
    return NULL;
  }

  WS_RES* pRes;
  // drop database if exists
  pRes = ws_query(pConn, "DROP TOPIC IF EXISTS topic_meters");
  code = ws_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "DROP DATABASE IF EXISTS power");
  code = ws_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  // create database
  pRes = ws_query(pConn, "CREATE DATABASE power PRECISION 'ms' WAL_RETENTION_PERIOD 3600");
  code = ws_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create power, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  // create super table
  pRes =
      ws_query(pConn,
               "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS "
               "(groupId INT, location BINARY(24))");
  code = ws_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create super table meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  return pConn;

END:
  ws_free_result(pRes);
  ws_close(pConn);
  return NULL;
}

void deinit_env(WS_TAOS* pConn) {
  if (pConn) ws_close(pConn);
}

int32_t create_topic(WS_TAOS* pConn) {
  WS_RES* pRes;
  int     code = 0;

  if (!pConn) {
    fprintf(stderr, "Invalid input parameter.\n");
    return -1;
  }

  pRes = ws_query(pConn, "USE power");
  code = ws_errno(pRes);
  if (ws_errno(pRes) != 0) {
    fprintf(stderr, "Failed to use power, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    return -1;
  }
  ws_free_result(pRes);

  pRes = ws_query(
      pConn,
      "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, current, voltage, phase, groupid, location FROM meters");
  code = ws_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create topic topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    return -1;
  }
  ws_free_result(pRes);
  return 0;
}

int32_t drop_topic(WS_TAOS* pConn) {
  WS_RES* pRes;
  int     code = 0;

  if (!pConn) {
    fprintf(stderr, "Invalid input parameter.\n");
    return -1;
  }

  pRes = ws_query(pConn, "USE power");
  code = ws_errno(pRes);
  if (ws_errno(pRes) != 0) {
    fprintf(stderr, "Failed to use power, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    return -1;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "DROP TOPIC IF EXISTS topic_meters");
  code = ws_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop topic topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, ws_errstr(pRes));
    return -1;
  }
  ws_free_result(pRes);
  return 0;
}

void tmq_commit_cb_print(ws_tmq_t* tmq, int32_t code, void* param) {
  count += 1;
  fprintf(stdout, "tmq_commit_cb_print() code: %d, tmq: %p, param: %p, count: %d.\n", code, tmq, param, count);
}

// ANCHOR: create_consumer_1
ws_tmq_t* build_consumer(const ConsumerConfig* config) {
  ws_tmq_conf_res_t code;
  ws_tmq_t*         tmq = NULL;

  // create a configuration object
  ws_tmq_conf_t* conf = ws_tmq_conf_new();

  // set the configuration parameters
  code = ws_tmq_conf_set(conf, "enable.auto.commit", config->enable_auto_commit);
  if (WS_TMQ_CONF_OK != code) {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }
  code = ws_tmq_conf_set(conf, "auto.commit.interval.ms", config->auto_commit_interval_ms);
  if (WS_TMQ_CONF_OK != code) {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }
  code = ws_tmq_conf_set(conf, "group.id", config->group_id);
  if (WS_TMQ_CONF_OK != code) {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }
  code = ws_tmq_conf_set(conf, "client.id", config->client_id);
  if (WS_TMQ_CONF_OK != code) {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }

  code = ws_tmq_conf_set(conf, "auto.offset.reset", config->auto_offset_reset);
  if (WS_TMQ_CONF_OK != code) {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }

  // create a consumer object
  tmq = ws_tmq_consumer_new(conf, "taos://localhost:6041", NULL, 0);

_end:
  // destroy the configuration object
  ws_tmq_conf_destroy(conf);
  return tmq;
}
// ANCHOR_END: create_consumer_1

// ANCHOR: build_topic_list
// build a topic list used to subscribe
ws_tmq_list_t* build_topic_list() {
  // create a empty topic list
  ws_tmq_list_t* topicList = ws_tmq_list_new();

  // append topic name to the list
  int32_t code = ws_tmq_list_append(topicList, topic_name);
  if (code) {
    // if failed, destroy the list and return NULL
    ws_tmq_list_destroy(topicList);
    fprintf(stderr,
            "Failed to create topic_list, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, ws_tmq_errstr(NULL));
    return NULL;
  }
  // if success, return the list
  return topicList;
}
// ANCHOR_END: build_topic_list

// ANCHOR: basic_consume_loop
void basic_consume_loop(ws_tmq_t* tmq) {
  int32_t totalRows = 0;   // total rows consumed
  int32_t msgCnt = 0;      // total messages consumed
  int32_t timeout = 5000;  // poll timeout

  while (running) {
    // poll message from TDengine
    WS_RES* tmqmsg = ws_tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;

      // Add your data processing logic here
      totalRows += msg_process(tmqmsg);

      // free the message
      ws_free_result(tmqmsg);
    }
    if (msgCnt > 50) {
      // consume 50 messages and break
      break;
    }
  }

  // print the result: total messages and total rows consumed
  fprintf(stdout, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}
// ANCHOR_END: basic_consume_loop

// ANCHOR: consume_repeatly
void consume_repeatly(ws_tmq_t* tmq) {
  int32_t                  numOfAssignment = 0;
  ws_tmq_topic_assignment* pAssign = NULL;

  // get the topic assignment
  int32_t code = ws_tmq_get_topic_assignment(tmq, topic_name, &pAssign, &numOfAssignment);
  if (code != 0 || pAssign == NULL || numOfAssignment == 0) {
    fprintf(stderr, "Failed to get assignment, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, ws_tmq_errstr(tmq));
    return;
  }

  // seek to the earliest offset
  for (int32_t i = 0; i < numOfAssignment; ++i) {
    ws_tmq_topic_assignment* p = &pAssign[i];

    code = ws_tmq_offset_seek(tmq, topic_name, p->vgId, p->begin);
    if (code != 0) {
      fprintf(stderr,
              "Failed to seek offset, topic: %s, groupId: %s, clientId: %s, vgId: %d, ErrCode: 0x%x, ErrMessage: %s.\n",
              topic_name, config.group_id, config.client_id, p->vgId, code, ws_tmq_errstr(tmq));
      break;
    }
  }
  if (code == 0) fprintf(stdout, "Assignment seek to beginning successfully.\n");

  // free the assignment array
  ws_tmq_free_assignment(pAssign, numOfAssignment);

  // let's consume the messages again
  basic_consume_loop(tmq);
}
// ANCHOR_END: consume_repeatly

// ANCHOR: manual_commit
void manual_commit(ws_tmq_t* tmq) {
  int32_t totalRows = 0;   // total rows consumed
  int32_t msgCnt = 0;      // total messages consumed
  int32_t timeout = 5000;  // poll timeout

  while (running) {
    // poll message from TDengine
    WS_RES* tmqmsg = ws_tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;
      // process the message
      totalRows += msg_process(tmqmsg);
      // commit the message
      int32_t code = ws_tmq_commit_sync(tmq, tmqmsg);
      if (code) {
        fprintf(stderr,
                "Failed to commit offset, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
                topic_name, config.group_id, config.client_id, code, ws_tmq_errstr(tmq));
        // free the message
        ws_free_result(tmqmsg);
        break;
      } else {
        fprintf(stdout, "Commit offset manually successfully.\n");
      }
      // free the message
      ws_free_result(tmqmsg);
    }
    if (msgCnt > 50) {
      // consume 50 messages and break
      break;
    }
  }

  // print the result: total messages and total rows consumed
  fprintf(stdout, "%d msg consumed, include %d rows.\n", msgCnt, totalRows);
}
// ANCHOR_END: manual_commit

int main(int argc, char* argv[]) {
  int32_t   code;
  pthread_t thread_id;

  WS_TAOS* pConn = init_env();
  if (pConn == NULL) {
    fprintf(stderr, "Failed to init env.\n");
    return -1;
  }

  if (create_topic(pConn) < 0) {
    fprintf(stderr, "Failed to create topic.\n");
    return -1;
  }

  if (pthread_create(&thread_id, NULL, &prepare_data, NULL)) {
    fprintf(stderr, "Failed to create thread.\n");
    return -1;
  }

  // ANCHOR: create_consumer_2
  ws_tmq_t* tmq = build_consumer(&config);
  if (NULL == tmq) {
    fprintf(stderr, "Failed to create native consumer, host: %s, groupId: %s, , clientId: %s.\n",
            config.td_connect_host, config.group_id, config.client_id);
    return -1;
  } else {
    fprintf(stdout, "Create consumer successfully, host: %s, groupId: %s, clientId: %s.\n", config.td_connect_host,
            config.group_id, config.client_id);
  }

  // ANCHOR_END: create_consumer_2

  // ANCHOR: subscribe_3
  ws_tmq_list_t* topic_list = build_topic_list();
  if (NULL == topic_list) {
    fprintf(stderr, "Failed to create topic_list, topic: %s, groupId: %s, clientId: %s.\n", topic_name, config.group_id,
            config.client_id);
    return -1;
  }

  if ((code = ws_tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr,
            "Failed to subscribe topic_list, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, ws_tmq_errstr(tmq));
  } else {
    fprintf(stdout, "Subscribe topics successfully.\n");
  }

  ws_tmq_list_destroy(topic_list);

  basic_consume_loop(tmq);
  // ANCHOR_END: subscribe_3

  consume_repeatly(tmq);

  manual_commit(tmq);

  // ANCHOR: unsubscribe_and_close
  // unsubscribe the topic
  code = ws_tmq_unsubscribe(tmq);
  if (code) {
    fprintf(stderr,
            "Failed to unsubscribe consumer, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, ws_tmq_errstr(tmq));
  } else {
    fprintf(stdout, "Consumer unsubscribed successfully.\n");
  }

  // close the consumer
  code = ws_tmq_consumer_close(tmq);
  if (code) {
    fprintf(stderr, "Failed to close consumer, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, ws_tmq_errstr(tmq));
  } else {
    fprintf(stdout, "Consumer closed successfully.\n");
  }
  // ANCHOR_END: unsubscribe_and_close

  thread_stop = 1;
  pthread_join(thread_id, NULL);

  if (drop_topic(pConn) < 0) {
    fprintf(stderr, "Failed to drop topic.\n");
    return -1;
  }

  deinit_env(pConn);
  return 0;
}
