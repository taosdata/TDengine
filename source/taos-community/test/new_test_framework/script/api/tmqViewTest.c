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

static int  running = 1;
const char* topic_name = "topicname";

static int32_t msg_process(TAOS_RES* msg) {
  char    buf[1024];
  int32_t rows = 0;

  const char* topicName = tmq_get_topic_name(msg);
  const char* dbName = tmq_get_db_name(msg);
  int32_t     vgroupId = tmq_get_vgroup_id(msg);

  printf("topic: %s\n", topicName);
  printf("db: %s\n", dbName);
  printf("vgroup id: %d\n", vgroupId);

  while (1) {
    TAOS_ROW row = taos_fetch_row(msg);
    if (row == NULL) break;

    TAOS_FIELD* fields = taos_fetch_fields(msg);
    int32_t     numOfFields = taos_field_count(msg);
    // int32_t*    length = taos_fetch_lengths(msg);
    int32_t precision = taos_result_precision(msg);
    rows++;
    taos_print_row(buf, row, fields, numOfFields);
    printf("precision: %d, row content: %s\n", precision, buf);
  }

  return rows;
}

static int32_t init_env() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (pConn == NULL) {
    return -1;
  }

  TAOS_RES* pRes;
  // drop database if exists
  printf("create database\n");
  pRes = taos_query(pConn, "drop topic topicname");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topicname, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists tmqdb");
  if (taos_errno(pRes) != 0) {
    printf("error in drop tmqdb, reason:%s\n", taos_errstr(pRes));
  }
  taos_free_result(pRes);

  // create database
  pRes = taos_query(pConn, "create database tmqdb precision 'ns' WAL_RETENTION_PERIOD 3600");
  if (taos_errno(pRes) != 0) {
    printf("error in create tmqdb, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  // create super table
  printf("create super table\n");
  pRes = taos_query(
      pConn, "create table tmqdb.stb (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table stb, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  // create sub tables
  printf("create sub tables\n");
  pRes = taos_query(pConn, "create table tmqdb.ctb0 using tmqdb.stb tags(0, 'subtable0')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb0, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tmqdb.ctb1 using tmqdb.stb tags(1, 'subtable1')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb1, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tmqdb.ctb2 using tmqdb.stb tags(2, 'subtable2')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb2, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table tmqdb.ctb3 using tmqdb.stb tags(3, 'subtable3')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table ctb3, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  // insert data
  printf("insert data into sub tables\n");
  pRes = taos_query(pConn, "insert into tmqdb.ctb0 values(now, 0, 0, 'a0')(now+1s, 0, 0, 'a00')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tmqdb.ctb1 values(now, 1, 1, 'a1')(now+1s, 11, 11, 'a11')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tmqdb.ctb2 values(now, 2, 2, 'a1')(now+1s, 22, 22, 'a22')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tmqdb.ctb3 values(now, 3, 3, 'a1')(now+1s, 33, 33, 'a33')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ctb0, reason:%s\n", taos_errstr(pRes));
    goto END;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create or replace view tmqdb.view1 as select ts, c1, t1 from tmqdb.stb");
  if (taos_errno(pRes) != 0) {
    printf("failed to create view, reason:%s\n", taos_errstr(pRes));
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

  pRes = taos_query(pConn, "use tmqdb");
  if (taos_errno(pRes) != 0) {
    printf("error in use tmqdb, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create topic topicname as select * from tmqdb.view1 where c1 > 1 and c1 <= 22");
  if (taos_errno(pRes) != 0) {
    printf("failed to create topic topicname, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  taos_close(pConn);
  return 0;
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("tmq_commit_cb_print() code: %d, tmq: %p, param: %p\n", code, tmq, param);
}

tmq_t* build_consumer() {
  tmq_conf_res_t code;
  tmq_t*         tmq = NULL;

  tmq_conf_t* conf = tmq_conf_new();
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
  code = tmq_conf_set(conf, "group.id", "cgrpName");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }
  code = tmq_conf_set(conf, "client.id", "user defined name");
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
  code = tmq_conf_set(conf, "auto.offset.reset", "earliest");
  if (TMQ_CONF_OK != code) {
    tmq_conf_destroy(conf);
    return NULL;
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq = tmq_consumer_new(conf, NULL, 0);

_end:
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topicList = tmq_list_new();
  int32_t     code = tmq_list_append(topicList, topic_name);
  if (code) {
    tmq_list_destroy(topicList);
    return NULL;
  }
  return topicList;
}

void basic_consume_loop(tmq_t* tmq) {
  int32_t totalRows = 0;
  int32_t msgCnt = 0;
  int32_t timeout = 5000;
  while (running) {
    TAOS_RES* tmqmsg = tmq_consumer_poll(tmq, timeout);
    if (tmqmsg) {
      msgCnt++;
      totalRows += msg_process(tmqmsg);
      taos_free_result(tmqmsg);
    } else {
      break;
    }
  }

  fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

void consume_repeatly(tmq_t* tmq) {
  int32_t               numOfAssignment = 0;
  tmq_topic_assignment* pAssign = NULL;

  int32_t code = tmq_get_topic_assignment(tmq, topic_name, &pAssign, &numOfAssignment);
  if (code != 0) {
    fprintf(stderr, "failed to get assignment, reason:%s", tmq_err2str(code));
  }

  // seek to the earliest offset
  for(int32_t i = 0; i < numOfAssignment; ++i) {
    tmq_topic_assignment* p = &pAssign[i];

    code = tmq_offset_seek(tmq, topic_name, p->vgId, p->begin);
    if (code != 0) {
      fprintf(stderr, "failed to seek to %d, reason:%s", (int)p->begin, tmq_err2str(code));
    }
  }

  tmq_free_assignment(pAssign);

  // let's do it again
  basic_consume_loop(tmq);
}

int main(int argc, char* argv[]) {
  int32_t code;

  if (init_env() < 0) {
    return -1;
  }

  if (create_topic() < 0) {
    return -1;
  }

  tmq_t* tmq = build_consumer();
  if (NULL == tmq) {
    fprintf(stderr, "build_consumer() fail!\n");
    return -1;
  }

  tmq_list_t* topic_list = build_topic_list();
  if (NULL == topic_list) {
    return -1;
  }

  if ((code = tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr, "Failed to tmq_subscribe(): %s\n", tmq_err2str(code));
  }

  tmq_list_destroy(topic_list);

  basic_consume_loop(tmq);

  consume_repeatly(tmq);

  code = tmq_consumer_close(tmq);
  if (code) {
    fprintf(stderr, "Failed to close consumer: %s\n", tmq_err2str(code));
  } else {
    fprintf(stderr, "Consumer closed\n");
  }

  return 0;
}
