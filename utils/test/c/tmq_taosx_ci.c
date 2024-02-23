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
#include "tmsg.h"

static int running = 1;
TdFilePtr  g_fp = NULL;
typedef struct {
  bool snapShot;
  bool dropTable;
  bool subTable;
  int  meta;
  int  srcVgroups;
  int  dstVgroups;
  char dir[256];
} Config;

Config g_conf = {0};

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

static void msg_process(TAOS_RES* msg) {
  printf("-----------topic-------------: %s\n", tmq_get_topic_name(msg));
  printf("db: %s\n", tmq_get_db_name(msg));
  printf("vg: %d\n", tmq_get_vgroup_id(msg));
  TAOS* pConn = use_db();
  if (tmq_get_res_type(msg) == TMQ_RES_TABLE_META || tmq_get_res_type(msg) == TMQ_RES_METADATA) {
    char* result = tmq_get_json_meta(msg);
    if (result) {
      printf("meta result: %s\n", result);
      if (g_fp && strcmp(result, "") != 0) {
        taosFprintfFile(g_fp, result);
        taosFprintfFile(g_fp, "\n");
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

int buildDatabase(TAOS* pConn, TAOS_RES* pRes) {
  /* test for TD-20612  start*/
  pRes = taos_query(pConn, "create table tb1 (ts timestamp, c1 int, c2 int)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tb1 (ts, c1) values(1669092069069, 0)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into tb1 (ts, c2) values(1669092069069, 1)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  /* test for TD-20612  end*/

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

  pRes = taos_query(pConn, "insert into ct0 values(1626006833400, 1, 2, 'a')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct1 using st1(t1) tags(2000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table ct1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct2 using st1(t1) tags(NULL)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table ct2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct1 values(1626006833600, 3, 4, 'b')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table if not exists ct3 using st1(t1) tags(3000)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "insert into ct3 values(1626006833600, 5, 6, 'c') ct1 values(1626006833601, 2, 3, 'sds') (1626006833602, 4, 5, "
      "'ddd') ct0 values(1626006833603, 4, 3, 'hwj') ct1 values(now+5s, 23, 32, 's21ds')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add column c4 bigint");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 modify column c3 binary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "insert into ct3 values(1626006833605, 53, 63, 'cffffffffffffffffffffffffffff', 8989898899999) "
                    "(1626006833609, 51, 62, 'c333', 940)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into ct3 select * from ct1");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table st1 add tag t2 binary(64)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter super table st1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table ct3 set tag t1=5000");
  if (taos_errno(pRes) != 0) {
    printf("failed to slter child table ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "delete from abc1   .ct3 where ts < 1626006833606");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into ct3, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  if (g_conf.dropTable) {
    pRes = taos_query(pConn, "drop table ct3, ct1");
    if (taos_errno(pRes) != 0) {
      printf("failed to drop child table ct3, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);

    pRes = taos_query(pConn, "drop table st1");
    if (taos_errno(pRes) != 0) {
      printf("failed to drop super table st1, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);
  }

  pRes = taos_query(pConn, "create table if not exists n1(ts timestamp, c1 int, c2 nchar(4))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 add column c3 bigint");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 modify column c2 nchar(8)");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 rename column c3 cc3");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 comment 'hello'");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "alter table n1 drop column c1");
  if (taos_errno(pRes) != 0) {
    printf("failed to alter normal table n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into n1 values(now, 'eeee', 8989898899999) (now+9s, 'c333', 940)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into n1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  if (g_conf.dropTable) {
    pRes = taos_query(pConn, "drop table n1");
    if (taos_errno(pRes) != 0) {
      printf("failed to drop normal table n1, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);
  }

  pRes = taos_query(pConn, "create table jt(ts timestamp, i int) tags(t json)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt1 using jt tags('{\"k1\":1, \"k2\":\"hello\"}')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table jt2 using jt tags('')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into jt1 values(now, 1)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into jt2 values(now, 11)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table jt2, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  if (g_conf.dropTable) {
    pRes = taos_query(pConn,
                      "create stable if not exists st1 (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                      "nchar(8), t4 bool)");
    if (taos_errno(pRes) != 0) {
      printf("failed to create super table st1, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);

    pRes = taos_query(pConn, "drop table st1");
    if (taos_errno(pRes) != 0) {
      printf("failed to drop super table st1, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);
  }

  pRes = taos_query(pConn,
                    "create stable if not exists stt (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table stt, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "create stable if not exists sttb (ts timestamp, c1 int, c2 float, c3 binary(16)) tags(t1 int, t3 "
                    "nchar(8), t4 bool)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table sttb, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(
      pConn,
      "create table if not exists stt1 using stt tags(2, \"stt1\", true) sttb1 using sttb tags(4, \"sttb1\", true) "
      "stt2 using stt tags(43, \"stt2\", false) sttb2 using sttb tags(54, \"sttb2\", true)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table stt1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes =
      taos_query(pConn,
                 "insert into stt1 values(now + 2s, 3, 2, 'stt1') stt3 using stt tags(23, \"stt3\", true) values(now + "
                 "1s, 1, 2, 'stt3') sttb3 using sttb tags(4, \"sttb3\", true) values(now + 2s, 13, 22, 'sttb3') "
                 "stt4 using stt tags(433, \"stt4\", false) values(now + 3s, 21, 21, 'stt4') sttb4 using sttb "
                 "tags(543, \"sttb4\", true) values(now + 4s, 16, 25, 'sttb4')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table stt1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  return 0;
}

int buildStable(TAOS* pConn, TAOS_RES* pRes) {
  pRes = taos_query(pConn,
                    "CREATE STABLE `meters` (`ts` TIMESTAMP, `current` FLOAT, `voltage` INT, `phase` FLOAT) TAGS "
                    "(`groupid` INT, `location` VARCHAR(16))");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table meters, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d0 using meters tags(1, 'San Francisco')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table d0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "create table d1 using meters tags(2, 'Beijing')");
  if (taos_errno(pRes) != 0) {
    printf("failed to create child table d1, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

#ifdef WINDOWS
  pRes = taos_query(pConn,
                    "CREATE STABLE `meters_summary` (`_wstart` TIMESTAMP, `current` FLOAT, `groupid` INT, `location` VARCHAR(16)) TAGS (`group_id` BIGINT UNSIGNED)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table meters_summary, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn,
                    "  CREATE TABLE `t_d2a450ee819dcf7576f0282d9ac22dbc` USING `meters_summary` (`group_id`) TAGS (13135550082773579308)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table meters_summary, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  pRes = taos_query(pConn, "insert into t_d2a450ee819dcf7576f0282d9ac22dbc values (now, 120, 1, 'San Francisco')");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into table d0, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
#else
  pRes = taos_query(pConn,
                    "create stream meters_summary_s trigger at_once IGNORE EXPIRED 0 into meters_summary as select _wstart, max(current) as current, "
                    "groupid, location from meters partition by groupid, location interval(10m)");
  if (taos_errno(pRes) != 0) {
    printf("failed to create super table meters_summary, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);
#endif

  pRes = taos_query(pConn, "insert into d0 (ts, current) values (now, 120)");
  if (taos_errno(pRes) != 0) {
    printf("failed to insert into table d0, reason:%s\n", taos_errstr(pRes));
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

  char sql[128] = {0};
  snprintf(sql, 128, "create database if not exists db_taosx vgroups %d wal_retention_period 3600", g_conf.dstVgroups);
  pRes = taos_query(pConn, sql);
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

  pRes = taos_query(pConn, "drop topic if exists meters_summary_t1");
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

  snprintf(sql, 128, "create database if not exists abc1 vgroups %d wal_retention_period 3600", g_conf.srcVgroups);
  pRes = taos_query(pConn, sql);
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

  if (g_conf.subTable) {
    buildStable(pConn, pRes);
  } else {
    buildDatabase(pConn, pRes);
  }

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

  pRes = taos_query(pConn, "use abc1");
  if (taos_errno(pRes) != 0) {
    printf("error in use db, reason:%s\n", taos_errstr(pRes));
    return -1;
  }
  taos_free_result(pRes);

  if (g_conf.subTable) {
    char topic[128] = {0};
    sprintf(topic, "create topic meters_summary_t1 %s as stable meters_summary", g_conf.meta == 0 ? "with meta" : "only meta");
    pRes = taos_query(pConn, topic);
    if (taos_errno(pRes) != 0) {
      printf("failed to create topic meters_summary_t1, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);
  } else {
    char topic[128] = {0};
    sprintf(topic, "create topic topic_db %s as database abc1", g_conf.meta == 0 ? "with meta" : "only meta");
    pRes = taos_query(pConn, topic);
    if (taos_errno(pRes) != 0) {
      printf("failed to create topic topic_db, reason:%s\n", taos_errstr(pRes));
      return -1;
    }
    taos_free_result(pRes);
  }

  taos_close(pConn);
  return 0;
}

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

  if (g_conf.snapShot) {
    tmq_conf_set(conf, "experimental.snapshot.enable", "true");
  }

  tmq_conf_set_auto_commit_cb(conf, tmq_commit_cb_print, NULL);
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  assert(tmq);
  tmq_conf_destroy(conf);
  return tmq;
}

tmq_list_t* build_topic_list() {
  tmq_list_t* topic_list = tmq_list_new();
  if (g_conf.subTable) {
    tmq_list_append(topic_list, "meters_summary_t1");
  } else {
    tmq_list_append(topic_list, "topic_db");
  }
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
  while (running) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 1000);
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
  if (NULL == pFile) {
    fprintf(stderr, "Failed to open %s for save result\n", f1);
    exit(-1);
  }
  g_fp = pFile;

  TdFilePtr pFile2 = taosOpenFile(f2, TD_FILE_TEXT | TD_FILE_TRUNC | TD_FILE_STREAM);
  if (NULL == pFile2) {
    fprintf(stderr, "Failed to open %s for save result\n", f2);
    exit(-1);
  }

  if (g_conf.snapShot) {
    if (g_conf.subTable) {
      char* result[] = {
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"meters_summary\",\"columns\":[{\"name\":\"_"
          "wstart\",\"type\":9},{\"name\":\"current\",\"type\":6},{\"name\":\"groupid\",\"type\":4},{\"name\":"
          "\"location\",\"type\":8,\"length\":16}],\"tags\":[{\"name\":\"group_id\",\"type\":14}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"t_d2a450ee819dcf7576f0282d9ac22dbc\",\"using\":"
          "\"meters_summary\",\"tagNum\":1,\"tags\":[{\"name\":\"group_id\",\"type\":14,\"value\":1.313555008277358e+"
          "19}],\"createList\":[]}"};
      for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
        taosFprintfFile(pFile2, result[i]);
        taosFprintfFile(pFile2, "\n");
      }
    } else {
      char* result[] = {
          "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\",\"type\":"
          "9},{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":4}],\"tags\":[]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
          ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":64},{"
          "\"name\":\"c4\",\"type\":5}],\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":"
          "8},{\"name\":\"t4\",\"type\":1},{\"name\":\"t2\",\"type\":8,\"length\":64}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":4,\"tags\":["
          "{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":4,\"tags\":["
          "{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":4,\"tags\":["
          "],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":4,\"tags\":["
          "{\"name\":\"t1\",\"type\":4,\"value\":5000}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
          ",{\"name\":\"c2\",\"type\":10,\"length\":8},{\"name\":\"cc3\",\"type\":5}],\"tags\":[]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"jt\",\"columns\":[{\"name\":\"ts\",\"type\":9},"
          "{\"name\":\"i\",\"type\":4}],\"tags\":[{\"name\":\"t\",\"type\":15}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt1\",\"using\":\"jt\",\"tagNum\":1,\"tags\":[{"
          "\"name\":\"t\",\"type\":15,\"value\":\"{\\\"k1\\\":1,\\\"k2\\\":\\\"hello\\\"}\"}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt2\",\"using\":\"jt\",\"tagNum\":1,\"tags\":[]"
          ",\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"stt\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
          ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
          "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
          "1}]}",
          "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"sttb\",\"columns\":[{\"name\":\"ts\",\"type\":"
          "9},{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
          "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
          "1}]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt1\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":2},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt1\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb1\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"sttb1\\\"\"}"
          ",{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt2\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":43},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt2\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":0}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb2\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":54},{\"name\":\"t3\",\"type\":10,\"value\":"
          "\"\\\"sttb2\\\"\"},{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt3\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
          "[{\"name\":\"t1\",\"type\":4,\"value\":23},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"stt3\\\"\"},{"
          "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"sttb3\",\"using\":\"sttb\",\"tagNum\":3,"
          "\"tags\":[{\"name\":\"t1\",\"type\":4,\"value\":4},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"sttb3\\\"\"}"
          ",{\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
          "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt4\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
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
    if (g_conf.meta) {
      if (g_conf.subTable){

      }else{
        char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9},{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":4}],\"tags\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
            ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":3000}],\"createList\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":5,\"colName\":\"c4\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":7,\"colName\":\"c3\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":1,\"colName\":\"t2\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"alterType\":4,\"colName\":\"t1\","
            "\"colValue\":\"5000\",\"colValueNull\":false}",
            "{\"type\":\"drop\",\"tableNameList\":[\"ct3\",\"ct1\"]}",
            "{\"type\":\"drop\",\"tableType\":\"super\",\"tableName\":\"st1\"}",
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
            ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":10,\"length\":4}],\"tags\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":5,\"colName\":\"c3\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":7,\"colName\":\"c2\","
            "\"colType\":10,\"colLength\":8}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":10,\"colName\":\"c3\","
            "\"colNewName\":\"cc3\"}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":9}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":6,\"colName\":\"c1\"}",
            "{\"type\":\"drop\",\"tableNameList\":[\"n1\"]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"jt\",\"columns\":[{\"name\":\"ts\",\"type\":9},"
            "{\"name\":\"i\",\"type\":4}],\"tags\":[{\"name\":\"t\",\"type\":15}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt1\",\"using\":\"jt\",\"tagNum\":1,\"tags\":[{"
            "\"name\":\"t\",\"type\":15,\"value\":\"{\\\"k1\\\":1,\\\"k2\\\":\\\"hello\\\"}\"}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt2\",\"using\":\"jt\",\"tagNum\":1,\"tags\":[]"
            ",\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9},"
            "{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":1}]}",
            "{\"type\":\"drop\",\"tableType\":\"super\",\"tableName\":\"st1\"}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"stt\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
            ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"sttb\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9},{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt1\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
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
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt3\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
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
        char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"meters_summary\",\"columns\":[{\"name\":\"_"
            "wstart\",\"type\":9},{\"name\":\"current\",\"type\":6},{\"name\":\"groupid\",\"type\":4},{\"name\":"
            "\"location\",\"type\":8,\"length\":16}],\"tags\":[{\"name\":\"group_id\",\"type\":14}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"t_d2a450ee819dcf7576f0282d9ac22dbc\",\"using\":"
            "\"meters_summary\",\"tagNum\":1,\"tags\":[{\"name\":\"group_id\",\"type\":14,\"value\":1.313555008277358e+"
            "19}],\"createList\":[]}"};

        for (int i = 0; i < sizeof(result) / sizeof(result[0]); i++) {
          taosFprintfFile(pFile2, result[i]);
          taosFprintfFile(pFile2, "\n");
        }
      }
      else {
        char* result[] = {
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"tb1\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9},{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":4}],\"tags\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"st1\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
            ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct0\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":1000},{\"name\":\"t3\",\"type\":10,\"value\":\"\\\"ttt\\\"\"},{"
            "\"name\":\"t4\",\"type\":1,\"value\":1}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct1\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":2000}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct2\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"using\":\"st1\",\"tagNum\":3,\"tags\":["
            "{\"name\":\"t1\",\"type\":4,\"value\":3000}],\"createList\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":5,\"colName\":\"c4\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":7,\"colName\":\"c3\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"super\",\"tableName\":\"st1\",\"alterType\":1,\"colName\":\"t2\","
            "\"colType\":8,\"colLength\":64}",
            "{\"type\":\"alter\",\"tableType\":\"child\",\"tableName\":\"ct3\",\"alterType\":4,\"colName\":\"t1\","
            "\"colValue\":\"5000\",\"colValueNull\":false}",
            "{\"type\":\"delete\",\"sql\":\"delete from `ct3` where `ts` >= 1626006833600 and `ts` <= 1626006833605\"}",
            "{\"type\":\"create\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
            ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":10,\"length\":4}],\"tags\":[]}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":5,\"colName\":\"c3\","
            "\"colType\":5}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":7,\"colName\":\"c2\","
            "\"colType\":10,\"colLength\":8}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":10,\"colName\":\"c3\","
            "\"colNewName\":\"cc3\"}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":9}",
            "{\"type\":\"alter\",\"tableType\":\"normal\",\"tableName\":\"n1\",\"alterType\":6,\"colName\":\"c1\"}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"jt\",\"columns\":[{\"name\":\"ts\",\"type\":9},"
            "{\"name\":\"i\",\"type\":4}],\"tags\":[{\"name\":\"t\",\"type\":15}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt1\",\"using\":\"jt\",\"tagNum\":1,\"tags\":[{"
            "\"name\":\"t\",\"type\":15,\"value\":\"{\\\"k1\\\":1,\\\"k2\\\":\\\"hello\\\"}\"}],\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"jt2\",\"using\":\"jt\",\"tagNum\":1,\"tags\":[]"
            ",\"createList\":[]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"stt\",\"columns\":[{\"name\":\"ts\",\"type\":9}"
            ",{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"super\",\"tableName\":\"sttb\",\"columns\":[{\"name\":\"ts\",\"type\":"
            "9},{\"name\":\"c1\",\"type\":4},{\"name\":\"c2\",\"type\":6},{\"name\":\"c3\",\"type\":8,\"length\":16}],"
            "\"tags\":[{\"name\":\"t1\",\"type\":4},{\"name\":\"t3\",\"type\":10,\"length\":8},{\"name\":\"t4\",\"type\":"
            "1}]}",
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt1\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
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
            "{\"type\":\"create\",\"tableType\":\"child\",\"tableName\":\"stt3\",\"using\":\"stt\",\"tagNum\":3,\"tags\":"
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

void testConsumeExcluded(int topic_type){
  TAOS* pConn = use_db();
  TAOS_RES *pRes = NULL;

  if(topic_type == 1){
    char *topic = "create topic topic_excluded with meta as database db_taosx";
    pRes = taos_query(pConn, topic);
    if (taos_errno(pRes) != 0) {
      printf("failed to create topic topic_excluded, reason:%s\n", taos_errstr(pRes));
      taos_close(pConn);
      return;
    }
    taos_free_result(pRes);
  }else if(topic_type == 2){
    char *topic = "create topic topic_excluded as select * from stt";
    pRes = taos_query(pConn, topic);
    if (taos_errno(pRes) != 0) {
      printf("failed to create topic topic_excluded, reason:%s\n", taos_errstr(pRes));
      taos_close(pConn);
      return;
    }
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

  if ((code = tmq_subscribe(tmq, topic_list))) {
    fprintf(stderr, "%% Failed to start consuming topics: %s\n", tmq_err2str(code));
    printf("subscribe err\n");
    return;
  }
  while (running) {
    TAOS_RES* msg = tmq_consumer_poll(tmq, 1000);
    if (msg) {
      tmq_raw_data raw = {0};
      tmq_get_raw(msg, &raw);
      if(topic_type == 1){
        assert(raw.raw_type != 2 && raw.raw_type != 4 &&
               raw.raw_type != TDMT_VND_CREATE_STB &&
               raw.raw_type != TDMT_VND_ALTER_STB &&
               raw.raw_type != TDMT_VND_CREATE_TABLE &&
               raw.raw_type != TDMT_VND_ALTER_TABLE &&
               raw.raw_type != TDMT_VND_DELETE);
        assert(raw.raw_type == TDMT_VND_DROP_STB ||
               raw.raw_type == TDMT_VND_DROP_TABLE);
      }else if(topic_type == 2){
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

  pConn = use_db();
  pRes = taos_query(pConn, "drop topic if exists topic_excluded");
  if (taos_errno(pRes) != 0) {
    printf("error in drop topic, reason:%s\n", taos_errstr(pRes));
    taos_close(pConn);
    return;
  }
  taos_free_result(pRes);
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
    } else if (strcmp(argv[i], "-onlymeta") == 0) {
      g_conf.meta = 1;
    }
  }

  printf("env init\n");
  if (strlen(g_conf.dir) != 0) {
    initLogFile();
  }

  if (init_env() < 0) {
    return -1;
  }
  create_topic();

  tmq_t*      tmq = build_consumer();
  tmq_list_t* topic_list = build_topic_list();
  basic_consume_loop(tmq, topic_list);
  tmq_list_destroy(topic_list);

  testConsumeExcluded(1);
  testConsumeExcluded(2);
  taosCloseFile(&g_fp);
}
