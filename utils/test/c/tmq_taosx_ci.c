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

static int running = 1;
TdFilePtr  g_fp = NULL;
typedef struct {
  bool snapShot;
  bool dropTable;
  bool subTable;
  bool filterTable;
  int  onlyMeta;
  int  srcVgroups;
  int  dstVgroups;
  char dir[256];
  bool btMeta;
  bool rawData;
} Config;

Config g_conf = {0};
tmq_t*      tmq = NULL;

static void use_db(TAOS** ppConn) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "use db_dst");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);
  *ppConn = pConn;
}

static void msg_process(TAOS_RES* msg) {
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  TAOS* pConn = NULL;
  use_db(&pConn);
  if (tmq_get_res_type(msg) == TMQ_RES_TABLE_META || tmq_get_res_type(msg) == TMQ_RES_METADATA) {
    char* result = tmq_get_json_meta(msg);
    if (result) {
      printf("meta result: %s\n", result);
      if (g_fp && strcmp(result, "") != 0) {
        // RES_TYPE__TMQ_BATCH_META
        if ((*(int8_t*)msg) == 5) {
          cJSON*  pJson = cJSON_Parse(result);
          cJSON*  pJsonArray = cJSON_GetObjectItem(pJson, "metas");
          int32_t num = cJSON_GetArraySize(pJsonArray);
          for (int32_t i = 0; i < num; i++) {
            cJSON* pJsonItem = cJSON_GetArrayItem(pJsonArray, i);
            char*  itemStr = cJSON_PrintUnformatted(pJsonItem);
            taosFprintfFile(g_fp, itemStr);
            tmq_free_json_meta(itemStr);
            taosFprintfFile(g_fp, "\n");
          }
          cJSON_Delete(pJson);
        } else {
          taosFprintfFile(g_fp, result);
          taosFprintfFile(g_fp, "\n");
          taosFsyncFile(g_fp);
        }
      }
    }
    tmq_free_json_meta(result);
  }

  tmq_raw_data raw = {0};
  tmq_get_raw(msg, &raw);
  printf("write raw data type: %d\n", raw.raw_type);
  int32_t ret = tmq_write_raw(pConn, raw);
  printf("write raw data: %s\n", tmq_err2str(ret));
  ASSERT(ret == 0);

  tmq_free_raw(raw);
  taos_close(pConn);
}

void buildDatabase() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", "db_src", 0);
  ASSERT(pConn != NULL);
  /* test for TD-20612  start*/
  TAOS_RES* pRes = taos_query(pConn, "create table tb1 (ts timestamp, c1 int, c2 int)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tb1 (ts, c1) values(1669092069069, 0)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tb1 (ts, c2) values(1669092069069, 1)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  /* test for TD-20612  end*/

  pRes = taos_query(pConn,
                    "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000, \"ttt\", true)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 values(1626006833400, 1, 2, 'a')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct1 using st1(t1) tags(2000)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct2 using st1(t1) tags(NULL)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct3 using st1(t1) tags(3000)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 values(1626006833600, 5, 6, 'c') ct1 values(1626006833601, 2, 3, 'sds') (1626006833602, 4, 5, "
      "'ddd') ct0 values(1626006833603, 4, 3, 'hwj') ct1 values(1626006833703, 23, 32, 's21ds')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add column c4 bigint");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1736006813600, -32222, 43, 'ewb', 99)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 drop column c4");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1736006833600, -4223, 344, 'bfs')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add column c4 bigint");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1766006833600, -4432, 4433, 'e23wb', 9349)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 modify column c3 binary(64)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "insert into ct3 values(1626006833605, 53, 63, 'cffffffffffffffffffffffffffff', 8989898899999) "
                    "(1626006833609, 51, 62, 'c333', 940)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct3 select * from ct1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add tag t2 binary(64)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct3 set tag t1=5000");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "delete from db_src   .ct3 where ts < 1626006833606");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  if (g_conf.dropTable) {
    pRes = taos_query(pConn, "drop table ct3, ct1");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);

    pRes = taos_query(pConn, "drop table st1");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);
  }

  pRes = taos_query(pConn, "create table if not exists n1(ts timestamp, c1 int, c2 nchar(4))");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 add column c3 bigint");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 modify column c2 nchar(8)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 rename column c3 cc3");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 comment 'hello'");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 drop column c1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into n1 values(now, 'eeee', 8989898899999) (now+9s, 'c333', 940)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  if (g_conf.dropTable) {
    pRes = taos_query(pConn, "drop table n1");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);
  }

  pRes = taos_query(pConn, "create table jt(ts timestamp, i int) tags(t json)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt1 using jt tags('{\"k1\":1, \"k2\":\"hello\"}')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt2 using jt tags('')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into jt1 values(now, 1)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into jt2 values(now, 11)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  if (g_conf.dropTable) {
    pRes = taos_query(pConn,
                      "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                      "nchar(8), t4 bool)");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);

    pRes = taos_query(pConn, "drop table st1");
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);
  }

  pRes = taos_query(pConn,
                    "create stable if not exists stt (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "create stable if not exists sttb (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "create table if not exists stt1 using stt tags(2, \"stt1\", true) sttb1 using sttb tags(4, \"sttb1\", true) "
      "stt2 using stt tags(43, \"stt2\", false) sttb2 using sttb tags(54, \"sttb2\", true)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes =
      taos_query(pConn,
                 "insert into stt1 values(now + 322s, 3, 2, 'stt1')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes =
      taos_query(pConn,
                 "insert into stt1 values(now + 2s, 3, 2, 'stt1') stt3 using stt tags(23, \"stt3\", true) values(now + "
                 "1s, 1, 2, 'stt3') sttb3 using sttb tags(4, \"sttb3\", true) values(now + 2s, 13, 22, 'sttb3') "
                 "stt4 using stt tags(433, \"stt4\", false) values(now + 3s, 21, 21, 'stt4') sttb4 using sttb "
                 "tags(543, \"sttb4\", true) values(now + 4s, 16, 25, 'sttb4')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes =
      taos_query(pConn,
                 "insert into stt1 values(now + 442s, 3, 2, 'stt1')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

void buildStable() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", "db_src", 0);
  ASSERT(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn,
                    "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "create stable if not exists stt1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000, \"ttt\", true)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 values(1626006833400, 1, 2, 'a')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct1 using st1(t1) tags(2000)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct2 using st1(t1) tags(4000) values(1626006833600, 3, 4, 'b')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct3 using st1(t1) tags(3000) if not exists ctt3 using stt1(t1) tags(3000)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 values(1626006833600, 5, 6, 'c') ctt3 values(1626006833601, 2, 3, 'sds') (1626006833602, 4, 5, "
      "'ddd') ct0 values(1626006833603, 4, 3, 'hwj') ct1 values(1626006833703, 23, 32, 's21ds')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct0 set tag t1 = 9000 ct3 set tag t1 = 1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 values(1626006833610, 5, 6, 'c') ct0 values(1626006833613, 4, 3, 'hwj')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "alter table using st1 set tag t1 = 3 where t1 = 4000");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct2 values(1626006833610, 5, 6, 'c')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop table ct3, ct1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

void create_db() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", NULL, 0);
  ASSERT(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn, "drop topic if exists topic_db");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop topic if exists topic_stb");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists db_dst");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "drop database if exists db_src");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  char sql[128] = {0};
  snprintf(sql, 128, "create database if not exists db_dst vgroups %d wal_retention_period 3600", g_conf.dstVgroups);
  pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  snprintf(sql, 128, "create database if not exists db_src vgroups %d wal_retention_period 3600", g_conf.srcVgroups);
  pRes = taos_query(pConn, sql);
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  taos_close(pConn);
}

void create_topic() {
  printf("create topic\n");
  TAOS_RES* pRes = NULL;
  TAOS*     pConn = taos_connect("localhost", "root", "taosdata", "db_src", 0);
  ASSERT(pConn != NULL);

  if (g_conf.subTable) {
    if (g_conf.filterTable) {
      char topic[128] = {0};
      sprintf(topic, "create topic topic_stb %s as stable st1 where t1 > 1000", g_conf.onlyMeta == 0 ? "with meta" : "only meta");
      pRes = taos_query(pConn, topic);
      ASSERT(taos_errno(pRes) == 0);
    } else {
      char topic[128] = {0};
      sprintf(topic, "create topic topic_stb %s as stable st1", g_conf.onlyMeta == 0 ? "with meta" : "only meta");
      pRes = taos_query(pConn, topic);
      ASSERT(taos_errno(pRes) == 0);
    }
  } else {
    char topic[128] = {0};
    sprintf(topic, "create topic topic_db %s as database db_src", g_conf.onlyMeta == 0 ? "with meta" : "only meta");
    pRes = taos_query(pConn, topic);
    ASSERT(taos_errno(pRes) == 0);
  }

  taos_free_result(pRes);
  taos_close(pConn);
}

void tmq_commit_cb_print(tmq_t* tmq, int32_t code, void* param) {
  printf("commit %d tmq %p param %p\n", code, tmq, param);
}

void build_consumer(tmq_t** ptmq) {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.consume.excluded", "1");
  if (g_conf.rawData) {
    tmq_conf_set(conf, "msg.consume.rawdata", "1");
  }
  //  tmq_conf_set(conf, "session.timeout.ms", "1000000");
  //  tmq_conf_set(conf, "max.poll.interval.ms", "20000");

  if (g_conf.snapShot) {
    tmq_conf_set(conf, "experimental.snapshot.enable", "true");
  }

  if (g_conf.btMeta) {
    tmq_conf_set(conf, "msg.enable.batchmeta", "true");
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  *ptmq = tmq;
}

void build_topic_list(tmq_list_t** ptopic_list) {
  tmq_list_t* topic_list = tmq_list_new();
  if (g_conf.subTable) {
    tmq_list_append(topic_list, "topic_stb");
  } else {
    tmq_list_append(topic_list, "topic_db");
  }
  *ptopic_list = topic_list;
}

void basic_consume_loop(tmq_t* tmq, tmq_list_t* topics) {
  int32_t code;

  code = tmq_subscribe(tmq, topics);
  ASSERT(code == 0);
  int32_t cnt = 0;
  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 5000);
    if (tmqmessage) {
      cnt++;
      printf("cnt:%d\n", cnt);
      msg_process(tmqmessage);
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }

  code = tmq_consumer_close(tmq);
  ASSERT(code == 0);
}

void initLogFile() {
  char f1[1024] = {0};
  char f2[1024] = {0};

  if (g_conf.snapShot) {
    sprintf(f1, "%s/../log/tmq_taosx_tmp_snapshot.source", g_conf.dir);
    sprintf(f2, "%s/../log/tmq_taosx_tmp_snapshot.result", g_conf.dir);
  } else {
    sprintf(f1, "%s/../log/tmq_taosx_tmp.source", g_conf.dir);
    sprintf(f2, "%s/../log/tmq_taosx_tmp.result", g_conf.dir);
  }

  TdFilePtr pFile = taosOpenFile(f1, TD_FILE_TEXT | TD_FILE_TRUNC | TD_FILE_STREAM);
  ASSERT(NULL != pFile);
  g_fp = pFile;

  TdFilePtr pFile2 = taosOpenFile(f2, TD_FILE_TEXT | TD_FILE_TRUNC | TD_FILE_STREAM);
  ASSERT(NULL != pFile2);

  if (g_conf.snapShot) {
    if (g_conf.subTable) {
      char* result[] = {
        "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":1}]}",
        "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":9000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
        "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":3}],\"createList\":[]}"
      };
      for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
        taosFprintfFile(pFile2, result[i]);
        taosFprintfFile(pFile2, "\n");
      }
    } else {
      char* result[] = {
          "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\","
          "\"type\":"
          "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c1\","
          "\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
          "\"name\":\"c2\",\"type\":"
          "4,"
          "\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"}],\"tags\":[]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\","
          "\"type\":9,"
          "\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
          ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
          "\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\","
          "\"level\":\"medium\"}"
          ",{"
          "\"name\":\"c3\",\"type\":8,\"length\":64,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":"
          "\"zstd\",\"level\":\"medium\"},{"
          "\"name\":\"c4\",\"type\":5,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
          "\"medium\"}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":"
          "\"t3\","
          "\"type\":10,\"length\":"
          "8},{\"name\":\"t4\",\"type\":1},{\"name\":\"t2\",\"type\":8,\"length\":64}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":4,"
          "\"tags\":["
          "{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":4,"
          "\"tags\":["
          "{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":4,"
          "\"tags\":["
          "],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":4,"
          "\"tags\":["
          "{\"name\":\"t1\",\"type\":4,\"value\":5000}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"columns\":[{\"name\":\"ts\","
          "\"type\":9,"
          "\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\","
          "\"type\":10,\"length\":8,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":"
          "\"medium\"},{\"name\":\"cc3\",\"type\":5,"
          "\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"}],\"tags\":[]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"jt\",\"columns\":[{\"name\":\"ts\","
          "\"type\":9,"
          "\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},"
          "{\"name\":\"i\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
          "\"medium\"}],\"tags\":[{\"name\":\"t\",\"type\":15}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt1\",\"using\":\"jt\",\"tagNum\":1,"
          "\"tags\":[{"
          "\"name\":\"t\",\"type\":15,\"value\":\"{\\\"k1\\\":1,\\\"k2\\\":\\\"hello\\\"}\"}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt2\",\"using\":\"jt\",\"tagNum\":1,"
          "\"tags\":[]"
          ",\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"stt\",\"columns\":[{\"name\":\"ts\","
          "\"type\":9,"
          "\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
          ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\","
          "\"level\":"
          "\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":"
          "false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
          "\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":"
          "\"zstd\",\"level\":\"medium\"}],"
          "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
          "\"type\":"
          "1}]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"sttb\",\"columns\":[{\"name\":\"ts\","
          "\"type\":"
          "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
          "\"c1\","
          "\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
          "\"name\":\"c2\",\"type\":6,"
          "\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
          "\"c3\","
          "\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":"
          "\"medium\"}],"
          "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
          "\"type\":"
          "1}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt1\",\"using\":\"stt\",\"tagNum\":3,"
          "\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":2},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt1\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb1\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":"
          "\"\\\"sttb1\\\"\"}"
          ",{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt2\",\"using\":\"stt\",\"tagNum\":3,"
          "\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":43},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt2\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":0}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb2\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":54},{\"name\":\"t3\",\"type\":10,\"value\":"
          "\"\\\"sttb2\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt3\",\"using\":\"stt\",\"tagNum\":3,"
          "\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":23},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt3\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb3\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":"
          "\"\\\"sttb3\\\"\"}"
          ",{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt4\",\"using\":\"stt\",\"tagNum\":3,"
          "\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":433},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt4\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":0}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb4\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":543},{\"name\":\"t3\",\"type\":10,\"value\":"
          "\"\\\"sttb4\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}"};
      for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
        taosFprintfFile(pFile2, result[i]);
        taosFprintfFile(pFile2, "\n");
      }
    }
  } else {
    if (g_conf.onlyMeta) {
      if (g_conf.subTable) {
      } else {
        char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\","
            "\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
            "\"c1\",\"type\":4,\"isPrimarykey\":false,"
            "\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\",\"type\":4,"
            "\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"}],\"tags\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
            ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\","
            "\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"bss\","
            "\"compress\":\"lz4\",\"level\":\"medium\"},{"
            "\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":"
            "\"zstd\",\"level\":\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":3000}],\"createList\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":5,\"colName\":\"c4\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":6,\"colName\":\"c4\"}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":5,\"colName\":\"c4\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":7,\"colName\":\"c3\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":1,\"colName\":\"t2\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"\",\"alterType\":19,\"tables\":[{\"tableName\":\"ct3\",\"tags\":[{\"colName\":\"t1\",\"colValue\":\"5000\",\"colValueNull\":false}]}]}",
            "{\"type\":\"drop\",\"tableNameList\":[\"ct3\",\"ct1\"]}",
            "{\"type\":\"drop\",\"tableType\":\"super\",\"tableName\":\"st1\"}",
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
            "\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"},{\"name\":\"c2\",\"type\":10,\"length\":4,"
            "\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":5,\"colName\":\"c3\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":7,\"colName\":\"c2\","
            "\"colType\":10,\"colLength\":8}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":10,\"colName\":\"c3\","
            "\"colNewName\":\"cc3\"}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":9}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":6,\"colName\":\"c1\"}",
            "{\"type\":\"drop\",\"tableNameList\":[\"n1\"]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"jt\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},"
            "{\"name\":\"i\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"}],\"tags\":[{\"name\":\"t\",\"type\":15}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt1\",\"using\":\"jt\",\"tagNum\":1,\"tags\":"
            "[{"
            "\"name\":\"t\",\"type\":15,\"value\":\"{\\\"k1\\\":1,\\\"k2\\\":\\\"hello\\\"}\"}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt2\",\"using\":\"jt\",\"tagNum\":1,\"tags\":"
            "[]"
            ",\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},"
            "{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":"
            "false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\",\"type\":8,"
            "\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":1}]}",
            "{\"type\":\"drop\",\"tableType\":\"super\",\"tableName\":\"st1\"}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"stt\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
            ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\","
            "\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":"
            "false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
            "\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":"
            "\"zstd\",\"level\":\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"sttb\",\"columns\":[{\"name\":\"ts\","
            "\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
            "\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"},{\"name\":\"c2\",\"type\":6,"
            "\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\","
            "\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":"
            "\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt1\",\"using\":\"stt\",\"tagNum\":3,"
            "\"tags\":"
            "[{\"name\":\"t1\",\"type\":4,\"value\":2},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt1\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[{\"tableName\":\"stt1\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":2},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt1\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"sttb1\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb1\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"stt2\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":43},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt2\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":0}]},{\"tableName\":\"sttb2\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":54},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb2\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt3\",\"using\":\"stt\",\"tagNum\":3,"
            "\"tags\":"
            "[{\"name\":\"t1\",\"type\":4,\"value\":23},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt3\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[{\"tableName\":\"stt3\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":23},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt3\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"sttb3\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb3\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"stt4\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":433},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt4\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":0}]},{\"tableName\":\"sttb4\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":543},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb4\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]}]}"};

        for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
          taosFprintfFile(pFile2, result[i]);
          taosFprintfFile(pFile2, "\n");
        }
      }
    } else {
      if (g_conf.subTable) {
        if (g_conf.filterTable) {
          char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":3000}],\"createList\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"\",\"alterType\":19,\"tables\":[{\"tableName\":\"ct0\",\"tags\":[{\"colName\":\"t1\",\"colValue\":\"9000\",\"colValueNull\":false}]}]}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":20,\"tags\":[{\"colName\":\"t1\",\"colValue\":\"3\",\"colValueNull\":false}],\"where\":\"`t1` = 4000\"}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":20,\"tags\":[{\"colName\":\"t1\",\"colValue\":\"3333\",\"colValueNull\":false}],\"where\":\"`t1` = 3\"}",
            "{\"type\":\"drop\",\"tableNameList\":[\"ct1\"]}"};

          for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
            taosFprintfFile(pFile2, result[i]);
            taosFprintfFile(pFile2, "\n");
          }
        } else {
          char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":3000}],\"createList\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"\",\"alterType\":19,\"tables\":[{\"tableName\":\"ct0\",\"tags\":[{\"colName\":\"t1\",\"colValue\":\"9000\",\"colValueNull\":false}]}]}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":20,\"tags\":[{\"colName\":\"t1\",\"colValue\":\"3\",\"colValueNull\":false}],\"where\":\"`t1` = 4000\"}",
            "{\"type\":\"drop\",\"tableNameList\":[\"ct3\",\"ct1\"]}"};
          for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
            taosFprintfFile(pFile2, result[i]);
            taosFprintfFile(pFile2, "\n");
          }
        }
      } else {
        char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\","
            "\"type\":9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
            "\"name\":\"c1\",\"type\":4,"
            "\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
            "\"c2\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"}],\"tags\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
            ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\","
            "\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":"
            "false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
            "\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":"
            "\"zstd\",\"level\":\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,"
            "\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":3000}],\"createList\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":5,\"colName\":\"c4\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":6,\"colName\":\"c4\"}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":5,\"colName\":\"c4\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":7,\"colName\":\"c3\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":1,\"colName\":\"t2\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"\",\"alterType\":19,\"tables\":[{\"tableName\":\"ct3\",\"tags\":[{\"colName\":\"t1\",\"colValue\":\"5000\",\"colValueNull\":false}]}]}",
            "{\"type\":\"delete\",\"sql\":\"delete from `ct3` where `ts` >= 1626006833600 and `ts` <= 1626006833605\"}",
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
            ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\","
            "\"level\":\"medium\"},{\"name\":\"c2\",\"type\":10,\"length\":4,"
            "\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":\"medium\"}],\"tags\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":5,\"colName\":\"c3\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":7,\"colName\":\"c2\","
            "\"colType\":10,\"colLength\":8}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":10,\"colName\":\"c3\","
            "\"colNewName\":\"cc3\"}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":9}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":6,\"colName\":\"c1\"}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"jt\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},"
            "{\"name\":\"i\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"}],\"tags\":[{\"name\":\"t\",\"type\":15}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt1\",\"using\":\"jt\",\"tagNum\":1,\"tags\":"
            "[{"
            "\"name\":\"t\",\"type\":15,\"value\":\"{\\\"k1\\\":1,\\\"k2\\\":\\\"hello\\\"}\"}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt2\",\"using\":\"jt\",\"tagNum\":1,\"tags\":"
            "[]"
            ",\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"stt\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"}"
            ",{\"name\":\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\","
            "\"level\":\"medium\"},{\"name\":\"c2\",\"type\":6,\"isPrimarykey\":"
            "false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{"
            "\"name\":\"c3\",\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":"
            "\"zstd\",\"level\":\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"sttb\",\"columns\":[{\"name\":\"ts\","
            "\"type\":"
            "9,\"isPrimarykey\":false,\"encode\":\"delta-i\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":"
            "\"c1\",\"type\":4,\"isPrimarykey\":false,\"encode\":\"simple8b\",\"compress\":\"lz4\",\"level\":"
            "\"medium\"},{\"name\":\"c2\",\"type\":6,"
            "\"isPrimarykey\":false,\"encode\":\"bss\",\"compress\":\"lz4\",\"level\":\"medium\"},{\"name\":\"c3\","
            "\"type\":8,\"length\":16,\"isPrimarykey\":false,\"encode\":\"disabled\",\"compress\":\"zstd\",\"level\":"
            "\"medium\"}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\","
            "\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt1\",\"using\":\"stt\",\"tagNum\":3,"
            "\"tags\":"
            "[{\"name\":\"t1\",\"type\":4,\"value\":2},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt1\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[{\"tableName\":\"stt1\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":2},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt1\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"sttb1\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb1\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"stt2\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":43},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt2\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":0}]},{\"tableName\":\"sttb2\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":54},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb2\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt3\",\"using\":\"stt\",\"tagNum\":3,"
            "\"tags\":"
            "[{\"name\":\"t1\",\"type\":4,\"value\":23},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt3\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[{\"tableName\":\"stt3\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":23},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt3\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"sttb3\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb3\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]},{\"tableName\":\"stt4\",\"using\":\"stt\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":433},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"stt4\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":0}]},{\"tableName\":\"sttb4\",\"using\":\"sttb\","
            "\"tagNum\":3,\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":543},{\"name\":\"t3\",\"type\":10,\"value\":"
            "\"\\\"sttb4\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}]}]}"};

        for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
          taosFprintfFile(pFile2, result[i]);
          taosFprintfFile(pFile2, "\n");
        }
      }
    }
  }

  taosCloseFile(&pFile2);
}

void testConsumeExcluded(int topic_type) {
  TAOS*     pConn = NULL;
  use_db(&pConn);
  TAOS_RES* pRes = NULL;

  if (topic_type == 1) {
    char* topic = "create topic topic_excluded with meta as database db_dst";
    pRes = taos_query(pConn, topic);
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);
  } else if (topic_type == 2) {
    char* topic = "create topic topic_excluded as select * from stt";
    pRes = taos_query(pConn, topic);
    ASSERT(taos_errno(pRes) == 0);
    taos_free_result(pRes);
  }
  taos_close(pConn);

  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", "tg2");
  tmq_conf_set(conf, "client.id", "my app 1");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.consume.excluded", "1");

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);

  tmq_list_t* topic_list = tmq_list_new();
  tmq_list_append(topic_list, "topic_excluded");

  int32_t code = 0;

  code = tmq_subscribe(tmq, topic_list);
  ASSERT(code == 0);
  while (running) {
    TAOS_RES* msg = tmq_consumer_poll(tmq, 1000);
    if (msg) {
      tmq_raw_data raw = {0};
      tmq_get_raw(msg, &raw);
      if (topic_type == 1) {
        assert(raw.raw_type != 2 && raw.raw_type != 4 && raw.raw_type != TDMT_VND_CREATE_STB &&
               raw.raw_type != TDMT_VND_ALTER_STB && raw.raw_type != TDMT_VND_CREATE_TABLE &&
               raw.raw_type != TDMT_VND_ALTER_TABLE && raw.raw_type != TDMT_VND_DELETE);
        assert(raw.raw_type == TDMT_VND_DROP_STB || raw.raw_type == TDMT_VND_DROP_TABLE || raw.raw_type == 5);
      } else if (topic_type == 2) {
        assert(0);
      }
      //      printf("write raw data type: %d\n", raw.raw_type);
      tmq_free_raw(raw);

      taos_free_result(msg);
    } else {
      break;
    }
  }

  tmq_consumer_close(tmq);
  tmq_list_destroy(topic_list);

  pConn = NULL;
  use_db(&pConn);
  pRes = taos_query(pConn, "drop topic if exists topic_excluded");
  ASSERT(taos_errno(pRes) == 0);
  taos_close(pConn);
  taos_free_result(pRes);
}

void testDetailError() {
  tmq_raw_data raw = {0};
  raw.raw_type = 2;
  int32_t code = tmq_write_raw((TAOS*)1, raw);
  ASSERT(code);
  const char* err = tmq_err2str(code);
  char*       tmp = strstr(err, "Invalid parameters,detail:taos:");
  ASSERT(tmp != NULL);
}

void consume() {
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 5000);
    if (tmqmessage) {
      msg_process(tmqmessage);
      taos_free_result(tmqmessage);
    } else {
      break;
    }
  }
}
void buildStableFilter() {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", "db_src", 0);
  ASSERT(pConn != NULL);

  TAOS_RES* pRes = taos_query(pConn,
                    "create stable if not exists stt1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct0 using st1 tags(1000, \"ttt\", true)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct0 values(1626006833400, 1, 2, 'a')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct1 using st1(t1) tags(2000)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct2 using st1(t1) tags(4000) values(1626006833600, 3, 4, 'b')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct3 using st1(t1) tags(3000) if not exists ctt3 using stt1(t1) tags(3000)");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 values(1626006833600, 5, 6, 'c') ctt3 values(1626006833601, 2, 3, 'sds') (1626006833602, 4, 5, "
      "'ddd') ct0 values(1626006833603, 4, 3, 'hwj') ct1 values(1626006833703, 23, 32, 's21ds')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  consume();

  pRes = taos_query(pConn, "alter table ct0 set tag t1 = 9000 ct3 set tag t1 = 1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 values(1626006833610, 5, 6, 'c') ct1 values(1626006833613, 4, 3, 'hwj')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  consume();

  pRes = taos_query(
      pConn,
      "alter table using st1 set tag t1 = 3 where t1 = 4000");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct2 values(1626006833610, 5, 6, 'c')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  consume();

  pRes = taos_query(
      pConn,
      "alter table using st1 set tag t1 = 3333 where t1 = 3");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct2 values(1626006833611, 5, 6, 'c')");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  consume();

  pRes = taos_query(pConn, "drop table ct3, ct1");
  ASSERT(taos_errno(pRes) == 0);
  taos_free_result(pRes);

  consume();

  taos_close(pConn);
}

void testFilterTable(tmq_t* tmq, tmq_list_t* topics) {
  int32_t code;

  code = tmq_subscribe(tmq, topics);
  ASSERT(code == 0);
  
  buildStableFilter();
  code = tmq_consumer_close(tmq);
  ASSERT(code == 0);
}

int main(int argc, char* argv[]) {
  for (int32_t i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-c") == 0) {
      tstrncpy(g_conf.dir, argv[++i], sizeof(g_conf.dir));
    } else if (strcmp(argv[i], "-s") == 0) {
      g_conf.snapShot = true;
    } else if (strcmp(argv[i], "-d") == 0) {
      g_conf.dropTable = true;
    } else if (strcmp(argv[i], "-sv") == 0) {
      g_conf.srcVgroups = atol(argv[++i]);
    } else if (strcmp(argv[i], "-dv") == 0) {
      g_conf.dstVgroups = atol(argv[++i]);
    } else if (strcmp(argv[i], "-t") == 0) {
      g_conf.subTable = true;
    } else if (strcmp(argv[i], "-f") == 0) {
      g_conf.filterTable = true;
    } else if (strcmp(argv[i], "-onlymeta") == 0) {
      g_conf.onlyMeta = 1;
    } else if (strcmp(argv[i], "-bt") == 0) {
      g_conf.btMeta = true;
    } else if (strcmp(argv[i], "-raw") == 0) {
      g_conf.rawData = true;
    }
  }

  printf("env init\n");
  if (strlen(g_conf.dir) != 0) {
    initLogFile();
  }

  create_db();
  if (g_conf.subTable) {
    if (g_conf.filterTable) {
      TAOS* pConn = taos_connect("localhost", "root", "taosdata", "db_src", 0);
      ASSERT(pConn != NULL);

      TAOS_RES* pRes = taos_query(pConn,
                        "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                        "nchar(8), t4 bool)");
      ASSERT(taos_errno(pRes) == 0);
      taos_free_result(pRes);
      taos_close(pConn);
    } else {
      buildStable();
    }
  } else {
    buildDatabase();
  }
  create_topic();

  build_consumer(&tmq);
  tmq_list_t* topic_list = NULL;
  build_topic_list(&topic_list);
  if (g_conf.filterTable) {
    testFilterTable(tmq, topic_list);
  } else {
    basic_consume_loop(tmq, topic_list);
  }
  tmq_list_destroy(topic_list);

  if (!g_conf.subTable) {
    testConsumeExcluded(1);
    testConsumeExcluded(2);
    testDetailError();
  }
  
  taosCloseFile(&g_fp);
}
