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
#include "taosws.h"

static int running = 1;
const char *topic_name = "topicname";

static int32_t msg_process(WS_RES *msg)
{
  char buf[1024];
  int32_t rows = 0;

  const char *topicName = ws_tmq_get_topic_name(msg);
  const char *dbName = ws_tmq_get_db_name(msg);
  int32_t vgroupId = ws_tmq_get_vgroup_id(msg);

  printf("topic: %s\n", topicName);
  printf("db: %s\n", dbName);
  printf("vgroup id: %d\n", vgroupId);

  while (1)
  {
    WS_ROW row = ws_fetch_row(msg);
    if (row == NULL)
      break;

    const WS_FIELD *fields = ws_fetch_fields(msg);
    int32_t numOfFields = ws_field_count(msg);
    // int32_t*    length = taos_fetch_lengths(msg);
    int32_t precision = ws_result_precision(msg);
    rows++;
    ws_print_row(buf, sizeof(buf), row, fields, numOfFields);
    printf("precision: %d, row content: %s\n", precision, buf);
  }

  return rows;
}

static int32_t init_env()
{
  WS_TAOS *pConn = ws_connect("ws://localhost:6041");
  if (pConn == NULL)
  {
    return -1;
  }

  WS_RES *pRes;
  // drop database if exists
  printf("create database\n");
  pRes = ws_query(pConn, "drop topic if exists topicname");
  if (ws_errno(pRes) != 0)
  {
    printf("error in drop topicname, reason:%s\n", ws_errstr(pRes));
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "drop database if exists tmqdb");
  if (ws_errno(pRes) != 0)
  {
    printf("error in drop tmqdb, reason:%s\n", ws_errstr(pRes));
  }
  ws_free_result(pRes);

  // create database
  pRes = ws_query(pConn, "create database tmqdb precision 'ns' WAL_RETENTION_PERIOD 3600");
  if (ws_errno(pRes) != 0)
  {
    printf("error in create tmqdb, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  // create super table
  printf("create super table\n");
  pRes = ws_query(
      pConn, "create table tmqdb.stb (ts timestamp, c1 int, c2 float, c3 varchar(16)) tags(t1 int, t3 varchar(16))");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to create super table stb, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  // create sub tables
  printf("create sub tables\n");
  pRes = ws_query(pConn, "create table tmqdb.ctb0 using tmqdb.stb tags(0, 'subtable0')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to create super table ctb0, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "create table tmqdb.ctb1 using tmqdb.stb tags(1, 'subtable1')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to create super table ctb1, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "create table tmqdb.ctb2 using tmqdb.stb tags(2, 'subtable2')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to create super table ctb2, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "create table tmqdb.ctb3 using tmqdb.stb tags(3, 'subtable3')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to create super table ctb3, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  // insert data
  printf("insert data into sub tables\n");
  pRes = ws_query(pConn, "insert into tmqdb.ctb0 values(now, 0, 0, 'a0')(now+1s, 0, 0, 'a00')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to insert into ctb0, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "insert into tmqdb.ctb1 values(now, 1, 1, 'a1')(now+1s, 11, 11, 'a11')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to insert into ctb0, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "insert into tmqdb.ctb2 values(now, 2, 2, 'a1')(now+1s, 22, 22, 'a22')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to insert into ctb0, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "insert into tmqdb.ctb3 values(now, 3, 3, 'a1')(now+1s, 33, 33, 'a33')");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to insert into ctb0, reason:%s\n", ws_errstr(pRes));
    goto END;
  }
  ws_free_result(pRes);
  ws_close(pConn);
  return 0;

END:
  ws_free_result(pRes);
  ws_close(pConn);
  return -1;
}

int32_t create_topic()
{
  printf("create topic\n");
  WS_RES *pRes;
  WS_TAOS *pConn = ws_connect("ws://localhost:6041");
  if (pConn == NULL)
  {
    return -1;
  }

  pRes = ws_query(pConn, "use tmqdb");
  if (ws_errno(pRes) != 0)
  {
    printf("error in use tmqdb, reason:%s\n", ws_errstr(pRes));
    return -1;
  }
  ws_free_result(pRes);

  pRes = ws_query(pConn, "create topic topicname as select ts, c1, c2, c3, tbname from tmqdb.stb where c1 > 1");
  if (ws_errno(pRes) != 0)
  {
    printf("failed to create topic topicname, reason:%s\n", ws_errstr(pRes));
    return -1;
  }
  ws_free_result(pRes);

  ws_close(pConn);
  return 0;
}

ws_tmq_t *build_consumer()
{
  ws_tmq_conf_res_t code;
  ws_tmq_t *tmq = NULL;
  char err_str[200] = {0};

  ws_tmq_conf_t *conf = ws_tmq_conf_new();
  code = ws_tmq_conf_set(conf, "enable.auto.commit", "true");
  if (WS_TMQ_CONF_OK != code)
  {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }
  code = ws_tmq_conf_set(conf, "auto.commit.interval.ms", "1000");
  if (WS_TMQ_CONF_OK != code)
  {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }
  code = ws_tmq_conf_set(conf, "group.id", "cgrpName");
  if (WS_TMQ_CONF_OK != code)
  {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }
  code = ws_tmq_conf_set(conf, "client.id", "user defined name");
  if (WS_TMQ_CONF_OK != code)
  {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }

  code = ws_tmq_conf_set(conf, "auto.offset.reset", "earliest");
  if (WS_TMQ_CONF_OK != code)
  {
    ws_tmq_conf_destroy(conf);
    return NULL;
  }

  tmq = ws_tmq_consumer_new(
      conf,
      "taos://localhost:6041",
      err_str,
      200);

_end:
  ws_tmq_conf_destroy(conf);
  return tmq;
}

ws_tmq_list_t *build_topic_list()
{
  ws_tmq_list_t *topicList = ws_tmq_list_new();
  int32_t code = ws_tmq_list_append(topicList, topic_name);
  if (code)
  {
    ws_tmq_list_destroy(topicList);
    return NULL;
  }
  return topicList;
}

void basic_consume_loop(ws_tmq_t *tmq)
{
  int32_t totalRows = 0;
  int32_t msgCnt = 0;
  int32_t timeout = 5000;
  while (running)
  {
    WS_RES *tmqmsg = ws_tmq_consumer_poll(tmq, timeout);
    if (tmqmsg)
    {
      msgCnt++;
      totalRows += msg_process(tmqmsg);
      ws_free_result(tmqmsg);
    }
    else
    {
      break;
    }
  }

  fprintf(stderr, "%d msg consumed, include %d rows\n", msgCnt, totalRows);
}

void consume_repeatly(ws_tmq_t *tmq)
{
  int32_t numOfAssignment = 0;
  ws_tmq_topic_assignment *pAssign = NULL;

  int32_t code = ws_tmq_get_topic_assignment(tmq, topic_name, &pAssign, &numOfAssignment);
  if (code != 0)
  {
    fprintf(stderr, "failed to get assignment, reason:%s", ws_tmq_errstr(tmq));
  }

  // seek to the earliest offset
  for (int32_t i = 0; i < numOfAssignment; ++i)
  {
    ws_tmq_topic_assignment *p = &pAssign[i];

    code = ws_tmq_offset_seek(tmq, topic_name, p->vgId, p->begin);
    if (code != 0)
    {
      fprintf(stderr, "failed to seek to %d, reason:%s", (int)p->begin, ws_tmq_errstr(tmq));
    }
  }

  ws_tmq_free_assignment(pAssign, numOfAssignment);

  // let's do it again
  basic_consume_loop(tmq);
}

int main(int argc, char *argv[])
{
  int32_t code;

  ws_enable_log("debug");

  if (init_env() < 0)
  {
    return -1;
  }

  if (create_topic() < 0)
  {
    return -1;
  }

  int i = 0;
  while (i < 3)
  {
    i++;
    ws_tmq_t *tmq = build_consumer();
    if (NULL == tmq)
    {
      fprintf(stderr, "build_consumer() fail!\n");
      return -1;
    }

    ws_tmq_list_t *topic_list = build_topic_list();
    if (NULL == topic_list)
    {
      return -1;
    }

    if ((code = ws_tmq_subscribe(tmq, topic_list)))
    {
      fprintf(stderr, "Failed to tmq_subscribe(): %s\n", ws_tmq_errstr(tmq));
    }

    ws_tmq_list_destroy(topic_list);

    basic_consume_loop(tmq);

    consume_repeatly(tmq);

    code = ws_tmq_consumer_close(tmq);
    if (code)
    {
      fprintf(stderr, "Failed to close consumer: %s\n", ws_tmq_errstr(tmq));
    }
    else
    {
      fprintf(stderr, "Consumer closed\n");
    }
  }
  return 0;
}
