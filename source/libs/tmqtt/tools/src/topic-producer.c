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
#ifndef WINDOWS
#include <unistd.h>
#endif
#include "cJSON.h"
#include "taos.h"

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

#define SQL_CREATE_DB  "CREATE DATABASE power PRECISION 'us' WAL_RETENTION_PERIOD 3600"
#define SQL_DROP_DB    "DROP DATABASE IF EXISTS power"
#define SQL_USE_DB     "USE power"
#define SQL_DROP_TOPIC "DROP TOPIC IF EXISTS topic_meters"
/*#define SQL_CREATE_TOPIC                                                                                             \
  "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, tbname, current, voltage, phase, groupid, location, deci, " \
  "deci64,"                                                                                                          \
  "bin, vc, nc, tbname "                                                                                             \
  "FROM meters"
#define SQL_CREATE_STABLE                                                                                             \
  "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT, deci DECIMAL(8, " \
  "6), deci64 decimal(20, 10), bin binary(20), vc varchar(20), nc nchar(20)) TAGS (groupId "                          \
  "INT, location BINARY(24)) "
#define SQL_INSERT                                                                                                  \
  "INSERT INTO power.d1001 USING power.meters TAGS(2,'California.SanFrancisco') VALUES (NOW + %da, 10.30000, 219, " \
  "0.31000, 1.23, 2.381, 'abc', 'def', 'ghi')"
*/
// clang-format off
#define SQL_CREATE_TOPIC  \
  "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT ts, tbname, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19 FROM meters"
#define SQL_CREATE_STABLE  \
  "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, c1 BOOL, c2 TINYINT, c3 SMALLINT, c4 INT, c5 BIGINT, c6 FLOAT, c7 DOUBLE, c8 binary(255), c9 TIMESTAMP, c10 NCHAR(255), c11 TINYINT UNSIGNED, c12 SMALLINT UNSIGNED, c13 INT UNSIGNED, c14 BIGINT UNSIGNED, c15 VARBINARY(255), c16 DECIMAL(38, 10), c17 VARCHAR(255), c18 GEOMETRY(10240), c19 DECIMAL(18, 4)) tags(t1 JSON)"
#define SQL_INSERT  \
  "INSERT INTO power.d1001 USING power.meters TAGS('{\"k1\": \"v1\"}') VALUES (NOW + %du, true, -79, 25761, -83885, 7865351, 3848271.756357, 92575.506626, '8.0742e+19', 752424273771827, '3.082946351e+18', 57, 21219, 627629871, 84394301683266985, '-2.653889251096953e+18', -262609547807621769.7285797, '-7.694200485148515e+19', 'POINT(1.0 1.0)', 57823334285922.827)"
// clang-format on

static void* prep_data(void* arg) {
  const char* host = "localhost";
  const char* user = "root";
  const char* password = "taosdata";
  uint16_t    port = 6030;
  int         code = 0;

  fprintf(stdout, "start to connect taosd\n");

  TAOS* pConn = taos_connect(host, user, password, NULL, port);
  if (pConn == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return NULL;
  }

  TAOS_RES* pRes;
  int       i = 1;

  while (!thread_stop) {
    char buf[4096] = {0};
    i++;
    snprintf(buf, sizeof(buf), SQL_INSERT, i);

    fprintf(stdout, "start to insert: %s\n", buf);

    pRes = taos_query(pConn, buf);
    code = taos_errno(pRes);
    if (code != 0) {
      fprintf(stderr, "Failed to insert data to power.meters, ErrCode: 0x%x, ErrMessage: %s.\n", code,
              taos_errstr(pRes));
    }
    taos_free_result(pRes);
#ifdef WINDOWS
    Sleep(1000);
#else
    usleep(1);
#endif
  }
  fprintf(stdout, "Prepare data thread exit\n");
  taos_close(pConn);
  return NULL;
}

int32_t msg_process(TAOS_RES* msg) {
  int32_t     rows = 0;
  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName = tmq_get_db_name(msg);
  int32_t     vgroupId = tmq_get_vgroup_id(msg);

  char str[512] = {0};
  while (true) {
    // get one row data from message
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;

    // Add your data processing logic here
    int32_t numOfFields = taos_num_fields(msg);
    if (!numOfFields) {
      return 0;
    }
    TAOS_FIELD* pFields = taos_fetch_fields(msg);
    if (!pFields) {
      return 0;
    }
    int32_t code = taos_print_row(str, row, pFields, numOfFields);
#if 0
    (void)printf("row: %s\n", str);
#endif
    rows++;
  }

  return rows;
}

static TAOS* init_env() {
  const char* host = "localhost";
  const char* user = "root";
  const char* password = "taosdata";
  uint16_t    port = 6030;
  int         code = 0;

  fprintf(stdout, "start to connect taosd.\n");

  TAOS* pConn = taos_connect(host, user, password, NULL, port);
  if (pConn == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return NULL;
  }

  TAOS_RES* pRes;
  // drop database if exists
  fprintf(stdout, "start to drop topic: topic_meters.\n");

  pRes = taos_query(pConn, SQL_DROP_TOPIC);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  fprintf(stdout, "start to drop db power.\n");

  pRes = taos_query(pConn, SQL_DROP_DB);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop database power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  // create database
  fprintf(stdout, "start to create db power.\n");

  pRes = taos_query(pConn, SQL_CREATE_DB);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  // create super table
  fprintf(stdout, "start to create super table power.meters\n");

  pRes = taos_query(pConn, SQL_CREATE_STABLE);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create super table meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  return pConn;

END:
  taos_free_result(pRes);
  taos_close(pConn);
  return NULL;
}

void deinit_env(TAOS* pConn) {
  if (pConn) taos_close(pConn);
}

int32_t create_topics(TAOS* pConn) {
  TAOS_RES* pRes;
  int       code = 0;

  if (!pConn) {
    fprintf(stderr, "Invalid input parameter.\n");
    return -1;
  }

  pRes = taos_query(pConn, SQL_USE_DB);
  code = taos_errno(pRes);
  if (taos_errno(pRes) != 0) {
    fprintf(stderr, "Failed to use power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, SQL_CREATE_TOPIC);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create topic topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  return 0;
}

int32_t drop_topic_with_connect(void) {
  // 1, connect
  // 2, drop topic
  // 3, disconnect

  const char* host = "localhost";
  const char* user = "root";
  const char* password = "taosdata";
  uint16_t    port = 6030;
  int         code = 0;
  TAOS*       pConn = taos_connect(host, user, password, NULL, port);
  if (pConn == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  TAOS_RES* pRes;
  // drop database if exists
  pRes = taos_query(pConn, SQL_DROP_TOPIC);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

END:
  if (pConn) taos_close(pConn);

  return code;
}

int32_t drop_topic_without_connect(TAOS* pConn) {
  TAOS_RES* pRes;
  int       code = 0;

  if (!pConn) {
    fprintf(stderr, "Invalid input parameter.\n");
    return -1;
  }

  pRes = taos_query(pConn, SQL_USE_DB);
  code = taos_errno(pRes);
  if (taos_errno(pRes) != 0) {
    fprintf(stderr, "Failed to use power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, SQL_DROP_TOPIC);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop topic topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  return 0;
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  count += 1;
  fprintf(stdout, "tmq_commit_cb_print() code: %d, tmq: %p, param: %p, count: %d.\n", code, tmq, param, count);
}

tmq_t* build_consumer(const ConsumerConfig* config) {
  tmq_conf_res_t code;
  tmq_t*         tmq = NULL;

  // create a configuration object
  tmq_conf_t* conf = tmq_conf_new();

  // set the configuration parameters
  code = tmq_conf_set(conf, "enable.auto.commit", config->enable_auto_commit);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "auto.commit.interval.ms", config->auto_commit_interval_ms);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "group.id", config->group_id);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "client.id", config->client_id);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.ip", config->td_connect_host);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.port", config->td_connect_port);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.user", config->td_connect_user);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "td.connect.pass", config->td_connect_pass);
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "auto.offset.reset", config->auto_offset_reset);
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

// build a topic list used to subscribe
tmq_list_t* build_topic_list() {
  // create a empty topic list
  tmq_list_t* topicList = tmq_list_new();

  // append topic name to the list
  int32_t code = tmq_list_append(topicList, topic_name);
  if (code) {
    // if failed, destroy the list and return NULL
    tmq_list_destroy(topicList);
    fprintf(stderr,
            "Failed to create topic_list, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, tmq_err2str(code));
    return NULL;
  }
  // if success, return the list
  return topicList;
}

void basic_consume_loop(tmq_t* tmq) {
  int32_t totalRows = 0;   // total rows consumed
  int32_t msgCnt = 0;      // total messages consumed
  int32_t timeout = 5000;  // poll timeout

  while (running) {
    // poll message from TDengine
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;

      // Add your data processing logic here
      totalRows += msg_process(tmqmsg);

      // free the message
      taos_free_result(tmqmsg);
    }
    if (totalRows > 999999) {
      // consume 50 messages and break
      break;
    }
  }

  // print the result: total messages and total rows consumed
  fprintf(stdout, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

void consume_repeatly(tmq_t* tmq) {
  int32_t               numOfAssignment = 0;
  tmq_topic_assignment* pAssign = NULL;

  // get the topic assignment
  int32_t code = tmq_get_topic_assignment(tmq, topic_name, &pAssign, &numOfAssignment);
  if (code != 0 || pAssign == NULL || numOfAssignment == 0) {
    fprintf(stderr, "Failed to get assignment, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, tmq_err2str(code));
    return;
  }

  // seek to the earliest offset
  for (int32_t i = 0; i < numOfAssignment; ++i) {
    tmq_topic_assignment* p = &pAssign[i];

    code = tmq_offset_seek(tmq, topic_name, p->vgId, p->begin);
    if (code != 0) {
      fprintf(stderr,
              "Failed to seek offset, topic: %s, groupId: %s, clientId: %s, vgId: %d, ErrCode: 0x%x, ErrMessage: %s.\n",
              topic_name, config.group_id, config.client_id, p->vgId, code, tmq_err2str(code));
      break;
    }
  }
  if (code == 0) fprintf(stdout, "Assignment seek to beginning successfully.\n");

  // free the assignment array
  tmq_free_assignment(pAssign);

  // let's consume the messages again
  basic_consume_loop(tmq);
}

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
        fprintf(stderr,
                "Failed to commit offset, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
                topic_name, config.group_id, config.client_id, code, tmq_err2str(code));
        // free the message
        taos_free_result(tmqmsg);
        break;
      } else {
        fprintf(stdout, "Commit offset manually successfully.\n");
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
  fprintf(stdout, "%d msg consumed, include %d rows.\n", msgCnt, totalRows);
}

int topic_create(void) {
  TAOS* pConn = init_env();
  if (pConn == NULL) {
    fprintf(stderr, "Failed to init env.\n");
    return -1;
  }

  fprintf(stdout, "start to create topic.\n");

  if (create_topics(pConn) < 0) {
    fprintf(stderr, "Failed to create topic.\n");
    return -1;
  }

  fprintf(stdout, "created topic.\n");

  deinit_env(pConn);

  return 0;
}

int topic_drop(void) {
  fprintf(stdout, "start to drop topic.\n");

  if (drop_topic_with_connect() < 0) {
    fprintf(stderr, "Failed to drop topic.\n");
    return -1;
  }

  fprintf(stdout, "dropped topic.\n");

  return 0;
}

int topic_prep(void) {
  pthread_t thread_id;

  if (pthread_create(&thread_id, NULL, &prep_data, NULL)) {
    fprintf(stderr, "Failed to create thread.\n");
    return -1;
  }

  // thread_stop = 1;
  pthread_join(thread_id, NULL);

  return 0;
}

#define SQL_USE_DB_DEFAULT     "USE test"
#define SQL_DROP_TOPIC_DEFAULT "DROP TOPIC IF EXISTS topic_meters"

int32_t drop_topic_without_connect_default(TAOS* pConn) {
  TAOS_RES* pRes;
  int       code = 0;

  if (!pConn) {
    fprintf(stderr, "Invalid input parameter.\n");
    return -1;
  }

  pRes = taos_query(pConn, SQL_USE_DB_DEFAULT);
  code = taos_errno(pRes);
  if (taos_errno(pRes) != 0) {
    fprintf(stderr, "Failed to use power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, SQL_DROP_TOPIC_DEFAULT);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to drop topic topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
  return 0;
}

tmq_list_t* build_topiclist(const char* topic_name) {
  // create a empty topic list
  tmq_list_t* topicList = tmq_list_new();

  // append topic name to the list
  int32_t code = tmq_list_append(topicList, topic_name);
  if (code) {
    // if failed, destroy the list and return NULL
    tmq_list_destroy(topicList);
    fprintf(stderr,
            "Failed to create topic_list, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, code, tmq_err2str(code));
    return NULL;
  }
  // if success, return the list
  return topicList;
}

static cJSON* tmq_ctx_do_topic(const char* topic_name, const char* db_name, const char* tbl_name, int32_t vid,
                               cJSON** ajson) {
  const char* json_topic_name_key = "topic";
  const char* json_db_name_key = "db";
  const char* json_tbl_name_key = "tbl";
  const char* json_vid_name_key = "vid";
  const char* json_rows_key = "rows";

  cJSON* json = cJSON_CreateObject();
  if (!json) {
    printf("json msg: out of memory.");
    return NULL;
  }

  if (NULL == cJSON_AddStringToObject(json, json_topic_name_key, topic_name)) {
    printf("json msg: out of memory.");
    cJSON_Delete(json);
    return NULL;
  }

  if (db_name) {
    if (NULL == cJSON_AddStringToObject(json, json_db_name_key, db_name)) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return NULL;
    }
  }

  if (tbl_name) {
    if (NULL == cJSON_AddStringToObject(json, json_tbl_name_key, tbl_name)) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return NULL;
    }
  }

  if (NULL == cJSON_AddNumberToObject(json, json_vid_name_key, vid)) {
    printf("json msg: out of memory.");
    cJSON_Delete(json);
    return NULL;
  }

  *ajson = cJSON_AddArrayToObject(json, json_rows_key);
  if (!ajson) {
    printf("json msg: out of memory.");
    cJSON_Delete(json);
    return NULL;
  }

  return json;
}

typedef uint16_t VarDataLenT;

#define TSDB_NCHAR_SIZE    sizeof(int32_t)
#define VARSTR_HEADER_SIZE sizeof(VarDataLenT)

#define GET_FLOAT_VAL(x)  (*(float*)(x))
#define GET_DOUBLE_VAL(x) (*(double*)(x))
#define varDataLen(v)     ((VarDataLenT*)(v))[0]

#define HEX_PREFIX_LEN 2  // \x

static char valueOf(uint8_t symbol) {
  switch (symbol) {
    case 0:
      return '0';
    case 1:
      return '1';
    case 2:
      return '2';
    case 3:
      return '3';
    case 4:
      return '4';
    case 5:
      return '5';
    case 6:
      return '6';
    case 7:
      return '7';
    case 8:
      return '8';
    case 9:
      return '9';
    case 10:
      return 'A';
    case 11:
      return 'B';
    case 12:
      return 'C';
    case 13:
      return 'D';
    case 14:
      return 'E';
    case 15:
      return 'F';
    default: {
      return -1;
    }
  }
}

static int32_t taosAscii2Hex(const char* z, uint32_t n, void** data, uint32_t* size) {
  *size = n * 2 + HEX_PREFIX_LEN;
  uint8_t* tmp = (uint8_t*)calloc(*size + 1, 1);
  if (tmp == NULL) {
    return -1;
  }

  *data = tmp;
  *(tmp++) = '\\';
  *(tmp++) = 'x';
  for (int i = 0; i < n; i++) {
    uint8_t val = z[i];
    tmp[i * 2] = valueOf(val >> 4);
    tmp[i * 2 + 1] = valueOf(val & 0x0F);
  }

  return 0;
}

static int tmq_ctx_do_fields(cJSON* item, TAOS_FIELD* fields, int32_t field_count, TAOS_ROW row) {
  int rc = 0;

  for (int i = 0; i < field_count; ++i) {
    if (!row[i]) {
      if (NULL == cJSON_AddNullToObject(item, fields[i].name)) {
        return -1;
      }

      continue;
    }

    switch (fields[i].type) {
      case TSDB_DATA_TYPE_BOOL:
        if (NULL == cJSON_AddBoolToObject(item, fields[i].name, *((int8_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_TINYINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int8_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_UTINYINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint8_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int16_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_USMALLINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint16_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_INT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int32_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_UINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint32_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_TIMESTAMP:
      case TSDB_DATA_TYPE_BIGINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((int64_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_UBIGINT:
        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, *((uint64_t*)row[i]))) {
          return -1;
        }
        break;
      case TSDB_DATA_TYPE_FLOAT: {
        float fv = GET_FLOAT_VAL(row[i]);

        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, fv)) {
          return -1;
        }
      } break;
      case TSDB_DATA_TYPE_DOUBLE: {
        double dv = GET_DOUBLE_VAL(row[i]);

        if (NULL == cJSON_AddNumberToObject(item, fields[i].name, dv)) {
          return -1;
        }
      } break;
      case TSDB_DATA_TYPE_VARBINARY: {
        void*    data = NULL;
        uint32_t tmp = 0;
        int32_t  charLen = varDataLen((char*)row[i] - VARSTR_HEADER_SIZE);
        if (taosAscii2Hex(row[i], charLen, &data, &tmp) < 0) {
          break;
        }

        if (NULL == cJSON_AddStringToObject(item, fields[i].name, data)) {
          free(data);
          return -1;
        }

        free(data);
      } break;
      case TSDB_DATA_TYPE_BINARY:  // TSDB_DATA_TYPE_VARCHAR
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t charLen = varDataLen((char*)row[i] - VARSTR_HEADER_SIZE);
        if (fields[i].type == TSDB_DATA_TYPE_BINARY || fields[i].type == TSDB_DATA_TYPE_VARBINARY ||
            fields[i].type == TSDB_DATA_TYPE_GEOMETRY) {
          if (charLen > fields[i].bytes || charLen < 0) {
            printf("json msg error: binary. charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        } else {
          if (charLen > fields[i].bytes * TSDB_NCHAR_SIZE || charLen < 0) {
            printf("json msg error: charLen:%d, fields[i].bytes:%d", charLen, fields[i].bytes);
            break;
          }
        }

        char* data = calloc(1, charLen + 1);
        if (!data) {
          return -1;
        }
        (void)memcpy(data, row[i], charLen);

        // if (NULL == cJSON_AddStringToObject(item, fields[i].name, row[i])) {
        if (NULL == cJSON_AddStringToObject(item, fields[i].name, data)) {
          free(data);
          return -1;
        }
        free(data);

      } break;
      case TSDB_DATA_TYPE_GEOMETRY: {
#if 0  // ignore geometry data type
        char* outputWKT = NULL;

        rc = initCtxAsText();
        if (rc != 0) {
          return -1;
        }

        rc = doAsText(row[i], fields[i].bytes, &outputWKT);
        if (rc != 0) {
          return -1;
        }

        if (NULL == cJSON_AddStringToObject(item, fields[i].name, outputWKT)) {
          geosFreeBuffer(outputWKT);
          return -1;
        }

        geosFreeBuffer(outputWKT);
#endif
      } break;
      case TSDB_DATA_TYPE_DECIMAL64:
      case TSDB_DATA_TYPE_DECIMAL: {
        if (NULL == cJSON_AddStringToObject(item, fields[i].name, row[i])) {
          return -1;
        }
      } break;
      default:
        printf("json do_fields: invalid data type: %hhd", fields[i].type);

        // ignore invalid data types rc = TTQ_ERR_INVAL;
        break;
    }
  }

  return rc;
}

static int32_t json_msg_process(TAOS_RES* msg) {
  char*       msg_buf = NULL;
  int         msg_len = -1;
  int32_t     rows = 0;
  int         rc;
  const char* topic_name = tmq_get_topic_name(msg);

  cJSON*      ajson = NULL;
  const char* db_name = tmq_get_db_name(msg);
  const char* tb_name = tmq_get_table_name(msg);
  int32_t     vgroup_id = tmq_get_vgroup_id(msg);

  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName = tmq_get_db_name(msg);
  int32_t     vgroupId = tmq_get_vgroup_id(msg);

  cJSON* json = tmq_ctx_do_topic(topic_name, db_name, tb_name, vgroup_id, &ajson);
  if (!json) {
    printf("json msg: out of memory.");
    return rows;
  }

  TAOS_ROW row = NULL;
  while ((row = taos_fetch_row(msg)) != NULL) {
    int32_t field_count = taos_num_fields(msg);
    if (!field_count) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return rows;
    }

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    if (!fields) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return rows;
    }

    cJSON* item = cJSON_CreateObject();
    if (!item) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return rows;
    }
    if (!cJSON_AddItemToArray(ajson, item)) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return rows;
    }

    int rc = tmq_ctx_do_fields(item, fields, field_count, row);
    if (rc) {
      printf("json msg: out of memory.");
      cJSON_Delete(json);
      return rows;
    }

    rows++;
  }

  char* data = cJSON_PrintUnformatted(json);
  if (!data) {
    printf("json msg: out of memory.");
    cJSON_Delete(json);
    return rows;
  }
  cJSON_Delete(json);

  json = cJSON_Parse(data);
  cJSON_Delete(json);

  return rows;
}

static void json_consume_loop(tmq_t* tmq) {
  int32_t totalRows = 0;   // total rows consumed
  int32_t msgCnt = 0;      // total messages consumed
  int32_t timeout = 5000;  // poll timeout

  while (running) {
    // poll message from TDengine
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;

      // Add your data processing logic here
      totalRows += json_msg_process(tmqmsg);

      // free the message
      taos_free_result(tmqmsg);
    }
    if (totalRows > 999999) {
      // consume 50 messages and break
      break;
    }
  }

  // print the result: total messages and total rows consumed
  fprintf(stdout, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

int topic_consume(void) {
  const char* host = "localhost";
  const char* user = "root";
  const char* password = "taosdata";
  uint16_t    port = 6030;
  int         code = 0;
  int         rc;

  fprintf(stdout, "start to connect taosd.\n");

  TAOS* pConn = taos_connect(host, user, password, NULL, port);
  if (pConn == NULL) {
    fprintf(stderr, "Failed to connect to %s:%hu, ErrCode: 0x%x, ErrMessage: %s.\n", host, port, taos_errno(NULL),
            taos_errstr(NULL));
    taos_cleanup();
    return -1;
  }

  TAOS_RES* pRes = taos_query(pConn, SQL_USE_DB_DEFAULT);
  code = taos_errno(pRes);
  if (taos_errno(pRes) != 0) {
    fprintf(stderr, "Failed to use power, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

#define SQL_CREATE_TOPIC_DEFAULT "CREATE TOPIC IF NOT EXISTS topic_meters AS SELECT * FROM meters"
#if 0
  pRes = taos_query(pConn, SQL_CREATE_TOPIC_DEFAULT);
  code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "Failed to create topic topic_meters, ErrCode: 0x%x, ErrMessage: %s.\n", code, taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
#endif

  config.auto_offset_reset = "earliest";
  config.group_id = "group3";

  tmq_t* tmq = build_consumer(&config);
  if (NULL == tmq) {
    fprintf(stderr, "Failed to create native consumer, host: %s, groupId: %s, , clientId: %s.\n",
            config.td_connect_host, config.group_id, config.client_id);
    return -1;
  } else {
    fprintf(stdout, "Create consumer successfully, host: %s, groupId: %s, clientId: %s.\n", config.td_connect_host,
            config.group_id, config.client_id);
  }

  tmq_list_t* topic_list = build_topiclist("topic_meters");
  if (NULL == topic_list) {
    fprintf(stderr, "Failed to create topic_list, topic: %s, groupId: %s, clientId: %s.\n", topic_name, config.group_id,
            config.client_id);
    return -1;
  }

  if ((rc = tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr,
            "Failed to subscribe topic_list, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, rc, tmq_err2str(rc));
  } else {
    fprintf(stdout, "Subscribe topics successfully.\n");
  }

  tmq_list_destroy(topic_list);

  clock_t start_time = clock();
#if 0
  basic_consume_loop(tmq);
#endif
  json_consume_loop(tmq);

  clock_t end_time = clock();
  double  elapsed_time = (double)(end_time - start_time) / CLOCKS_PER_SEC;
  fprintf(stdout, "elapsed_time: %f.\n", elapsed_time);

  //  ANCHOR_END: subscribe_3
#if 0
   consume_repeatly(tmq);

   manual_commit(tmq);
#endif
  // unsubscribe the topic
  rc = tmq_unsubscribe(tmq);
  if (rc) {
    fprintf(stderr,
            "Failed to unsubscribe consumer, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, rc, tmq_err2str(rc));
  } else {
    fprintf(stdout, "Consumer unsubscribed successfully.\n");
  }

  // close the consumer
  rc = tmq_consumer_close(tmq);
  if (rc) {
    fprintf(stderr, "Failed to close consumer, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, rc, tmq_err2str(rc));
  } else {
    fprintf(stdout, "Consumer closed successfully.\n");
  }

  thread_stop = 1;
#if 0
  if (drop_topic_without_connect_default(pConn) < 0) {
    fprintf(stderr, "Failed to drop topic.\n");
    return -1;
  }
#endif
  deinit_env(pConn);

  return 0;
}

int main(int argc, char* argv[]) {
  int rc;

  if (argc >= 2) {
    if (!strcmp(argv[1], "create")) {
      return topic_create();
    } else if (!strcmp(argv[1], "drop")) {
      return topic_drop();
    } else if (!strcmp(argv[1], "consume")) {
      return topic_consume();
    } else if (!strcmp(argv[1], "prep")) {
      return topic_prep();
    }
  }

  pthread_t thread_id;

  TAOS* pConn = init_env();
  if (pConn == NULL) {
    fprintf(stderr, "Failed to init env.\n");
    return -1;
  }

  if (create_topics(pConn) < 0) {
    fprintf(stderr, "Failed to create topic.\n");
    return -1;
  }

  if (pthread_create(&thread_id, NULL, &prep_data, NULL)) {
    fprintf(stderr, "Failed to create thread.\n");
    return -1;
  }

  tmq_t* tmq = build_consumer(&config);
  if (NULL == tmq) {
    fprintf(stderr, "Failed to create native consumer, host: %s, groupId: %s, , clientId: %s.\n",
            config.td_connect_host, config.group_id, config.client_id);
    return -1;
  } else {
    fprintf(stdout, "Create consumer successfully, host: %s, groupId: %s, clientId: %s.\n", config.td_connect_host,
            config.group_id, config.client_id);
  }

  tmq_list_t* topic_list = build_topic_list();
  if (NULL == topic_list) {
    fprintf(stderr, "Failed to create topic_list, topic: %s, groupId: %s, clientId: %s.\n", topic_name, config.group_id,
            config.client_id);
    return -1;
  }

  if ((rc = tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr,
            "Failed to subscribe topic_list, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, rc, tmq_err2str(rc));
  } else {
    fprintf(stdout, "Subscribe topics successfully.\n");
  }

  tmq_list_destroy(topic_list);

  basic_consume_loop(tmq);
  //  ANCHOR_END: subscribe_3

  // consume_repeatly(tmq);

  // manual_commit(tmq);

  // unsubscribe the topic
  rc = tmq_unsubscribe(tmq);
  if (rc) {
    fprintf(stderr,
            "Failed to unsubscribe consumer, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, rc, tmq_err2str(rc));
  } else {
    fprintf(stdout, "Consumer unsubscribed successfully.\n");
  }

  // close the consumer
  rc = tmq_consumer_close(tmq);
  if (rc) {
    fprintf(stderr, "Failed to close consumer, topic: %s, groupId: %s, clientId: %s, ErrCode: 0x%x, ErrMessage: %s.\n",
            topic_name, config.group_id, config.client_id, rc, tmq_err2str(rc));
  } else {
    fprintf(stdout, "Consumer closed successfully.\n");
  }

  thread_stop = 1;
  pthread_join(thread_id, NULL);

  if (drop_topic_without_connect(pConn) < 0) {
    fprintf(stderr, "Failed to drop topic.\n");
    return -1;
  }

  deinit_env(pConn);

  return rc;
}
