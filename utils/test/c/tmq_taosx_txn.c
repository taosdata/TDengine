/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * §35 taosX replication: target-side STB transaction test binary.
 *
 * Simulates the taosX replication flow for batch metadata transactions:
 *   1. Source DB: BEGIN → CREATE STB + child tables → COMMIT/ROLLBACK
 *   2. TMQ subscription on source VNode WAL (with_meta)
 *   3. tmq_get_raw → tmq_write_raw on target DB
 *   4. Verification of target state
 *
 * Usage: tmq_taosx_txn <scenario>
 *   scenario 1: CREATE STB + child tables → COMMIT → verify target has STB + tables
 *   scenario 2: CREATE STB + child tables → ROLLBACK → verify target has nothing
 *   scenario 3: CREATE STB → ALTER STB → COMMIT → verify target has altered STB
 *   scenario 4: CREATE STB → DROP STB → COMMIT → verify target has no STB
 *   scenario 5: Idempotent COMMIT (replay COMMIT twice)
 *   scenario 6: CREATE CTBs + ALTER child tag + COMMIT
 *   scenario 7: CREATE CTBs + DROP child + COMMIT
 *   scenario 8: CREATE normal table + ALTER + COMMIT
 *   scenario 9: CREATE normal table + DROP + COMMIT
 *   scenario 10: Mixed STB + CTB + normal table + COMMIT
 *   scenario 11: Multi-VGroup (2 VGs): STB + 10 CTBs + 2 NTBs + COMMIT
 *   scenario 12: Low-watermark replay (crash recovery, double consume)
 *   scenario 13: Pre-existing STB → BEGIN → ALTER STB → COMMIT (first MNode DDL = ALTER)
 *   scenario 14: Pre-existing STB → BEGIN → DROP STB → COMMIT (first MNode DDL = DROP)
 *   scenario 15: Pre-existing STB → BEGIN → ALTER STB → ROLLBACK (first MNode DDL = ALTER)
 *   scenario 16: Pre-existing STB → BEGIN → DROP STB → ROLLBACK (first MNode DDL = DROP)
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "taos.h"

#define SRC_DB "src_txn_db"
#define DST_DB "dst_txn_db"
#define TOPIC_NAME "topic_taosx_txn"

static void do_query(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int code = taos_errno(res);
  if (code != 0) {
    printf("FAILED: %s — %s (0x%x)\n", sql, taos_errstr(res), code);
    taos_free_result(res);
    exit(1);
  }
  taos_free_result(res);
}

static int do_query_ok(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  int code = taos_errno(res);
  taos_free_result(res);
  return code;
}

static int query_count(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  if (taos_errno(res) != 0) {
    printf("query_count FAILED: %s — %s\n", sql, taos_errstr(res));
    taos_free_result(res);
    return -1;
  }
  TAOS_ROW row = taos_fetch_row(res);
  int count = 0;
  if (row && row[0]) {
    count = *(int64_t *)row[0];
  }
  taos_free_result(res);
  return count;
}

static int query_rows(TAOS *taos, const char *sql) {
  TAOS_RES *res = taos_query(taos, sql);
  if (taos_errno(res) != 0) {
    printf("query_rows FAILED: %s — %s\n", sql, taos_errstr(res));
    taos_free_result(res);
    return -1;
  }
  int rows = 0;
  while (taos_fetch_row(res)) rows++;
  taos_free_result(res);
  return rows;
}

static TAOS *connect_db(const char *db) {
  TAOS *taos = taos_connect("localhost", "root", "taosdata", NULL, 0);
  if (!taos) {
    printf("FAILED: Cannot connect to TDengine\n");
    exit(1);
  }
  if (db) {
    char sql[256];
    snprintf(sql, sizeof(sql), "use %s", db);
    do_query(taos, sql);
  }
  return taos;
}

static tmq_t *create_consumer_ex(const char *group_id);

static void setup_source(int scenario) {
  TAOS *taos = connect_db(NULL);

  do_query(taos, "drop topic if exists " TOPIC_NAME);
  do_query(taos, "drop database if exists " SRC_DB);
  do_query(taos, "drop database if exists " DST_DB);
  int  vgroups = (scenario == 11) ? 2 : 1;
  char sql[256];
  snprintf(sql, sizeof(sql), "create database " SRC_DB " vgroups %d wal_retention_period 3600", vgroups);
  do_query(taos, sql);
  snprintf(sql, sizeof(sql), "create database " DST_DB " vgroups %d", vgroups);
  do_query(taos, sql);
  do_query(taos, "use " SRC_DB);

  // Create TMQ topic before any DDL (to capture all WAL entries)
  do_query(taos, "create topic " TOPIC_NAME " with meta as database " SRC_DB);

  switch (scenario) {
    case 1: {
      // CREATE STB + child tables → COMMIT
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "COMMIT");
      break;
    }
    case 2: {
      // CREATE STB + child tables → ROLLBACK
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "ROLLBACK");
      break;
    }
    case 3: {
      // CREATE STB → ALTER STB → COMMIT
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "alter table stb1 add column c2 float");
      do_query(taos, "COMMIT");
      break;
    }
    case 4: {
      // CREATE STB → DROP STB → COMMIT
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "drop table stb1");
      do_query(taos, "COMMIT");
      break;
    }
    case 5: {
      // Same as scenario 1 (for idempotent COMMIT test — Python handles replay)
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "COMMIT");
      break;
    }
    case 6: {
      // CREATE STB + child tables → ALTER child table tag → COMMIT
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "BEGIN");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "alter table ct1 set tag t1=100");
      do_query(taos, "COMMIT");
      break;
    }
    case 7: {
      // CREATE STB + child tables → DROP child table → COMMIT
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "BEGIN");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "drop table ct1");
      do_query(taos, "COMMIT");
      break;
    }
    case 8: {
      // CREATE normal table → ALTER add column → COMMIT
      do_query(taos, "BEGIN");
      do_query(taos, "create table ntb1 (ts timestamp, v int)");
      do_query(taos, "alter table ntb1 add column c2 float");
      do_query(taos, "COMMIT");
      break;
    }
    case 9: {
      // CREATE normal table → DROP → COMMIT
      do_query(taos, "BEGIN");
      do_query(taos, "create table ntb1 (ts timestamp, v int)");
      do_query(taos, "drop table ntb1");
      do_query(taos, "COMMIT");
      break;
    }
    case 10: {
      // Mixed: CREATE STB + CTBs + normal table → COMMIT
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ntb1 (ts timestamp, v int)");
      do_query(taos, "COMMIT");
      break;
    }
    case 11: {
      // Multi-VGroup: create enough tables to spread across vgroups=2
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      for (int i = 0; i < 10; i++) {
        char ddl[256];
        snprintf(ddl, sizeof(ddl), "create table ct%d using stb1 tags(%d)", i, i);
        do_query(taos, ddl);
      }
      do_query(taos, "create table ntb1 (ts timestamp, v int)");
      do_query(taos, "create table ntb2 (ts timestamp, v int)");
      do_query(taos, "COMMIT");
      break;
    }
    case 12: {
      // Low-watermark replay: same as s1, Python wrapper handles double replay
      do_query(taos, "BEGIN");
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "create table ntb1 (ts timestamp, v int)");
      do_query(taos, "COMMIT");
      break;
    }
    case 13: {
      // Pre-existing STB → BEGIN → ALTER STB add column → COMMIT
      // ALTER STB is the FIRST MNode DDL in the txn (no preceding CREATE STB)
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "BEGIN");
      do_query(taos, "alter table stb1 add column c2 float");
      do_query(taos, "COMMIT");
      break;
    }
    case 14: {
      // Pre-existing STB + child tables → BEGIN → DROP STB → COMMIT
      // DROP STB is the FIRST MNode DDL in the txn (no preceding CREATE STB)
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "insert into ct1 values(now, 1)");
      do_query(taos, "BEGIN");
      do_query(taos, "drop table stb1");
      do_query(taos, "COMMIT");
      break;
    }
    case 15: {
      // Pre-existing STB → BEGIN → ALTER STB add column → ROLLBACK
      // ALTER STB is the FIRST MNode DDL in the txn; rollback should undo alter
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "BEGIN");
      do_query(taos, "alter table stb1 add column c2 float");
      do_query(taos, "ROLLBACK");
      break;
    }
    case 16: {
      // Pre-existing STB + child tables → BEGIN → DROP STB → ROLLBACK
      // DROP STB is the FIRST MNode DDL in the txn; rollback should restore STB/CTBs
      do_query(taos, "create table stb1 (ts timestamp, v int) tags (t1 int)");
      do_query(taos, "create table ct1 using stb1 tags(1)");
      do_query(taos, "create table ct2 using stb1 tags(2)");
      do_query(taos, "insert into ct1 values(now, 1)");
      do_query(taos, "BEGIN");
      do_query(taos, "drop table stb1");
      do_query(taos, "ROLLBACK");
      break;
    }
    default:
      printf("Unknown scenario: %d\n", scenario);
      exit(1);
  }

  taos_close(taos);
  printf("Source DB setup complete for scenario %d\n", scenario);
}

static tmq_t *create_consumer(void) { return create_consumer_ex("taosx_txn_test"); }

static tmq_t *create_consumer_ex(const char *group_id) {
  tmq_conf_t *conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", group_id);
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");

  int32_t code = 0;
  char errstr[256] = {0};
  tmq_t  *consumer = tmq_consumer_new(conf, errstr, sizeof(errstr));
  tmq_conf_destroy(conf);
  if (!consumer) {
    printf("FAILED: Cannot create TMQ consumer: %s\n", errstr);
    exit(1);
  }

  tmq_list_t *topics = tmq_list_new();
  tmq_list_append(topics, TOPIC_NAME);
  code = tmq_subscribe(consumer, topics);
  tmq_list_destroy(topics);
  if (code != 0) {
    printf("FAILED: Cannot subscribe, code: %d\n", code);
    exit(1);
  }
  return consumer;
}

static void replicate_to_target(void) {
  tmq_t *consumer = create_consumer();
  TAOS  *dst = connect_db(DST_DB);
  int    msg_count = 0;
  int    empty_polls = 0;
  int    max_empty = 10;

  printf("Starting replication...\n");
  while (empty_polls < max_empty) {
    TAOS_RES *msg = tmq_consumer_poll(consumer, 1000);
    if (!msg) {
      empty_polls++;
      continue;
    }
    empty_polls = 0;
    msg_count++;

    int16_t msg_type = tmq_get_res_type(msg);
    printf("  msg %d: type=%d, vg=%d\n", msg_count, msg_type, tmq_get_vgroup_id(msg));

    tmq_raw_data raw = {0};
    int32_t code = tmq_get_raw(msg, &raw);
    if (code == 0) {
      printf("  raw type=%d, len=%d\n", raw.raw_type, raw.raw_len);
      int32_t ret = tmq_write_raw(dst, raw);
      printf("  write_raw result: %s (%d)\n", tmq_err2str(ret), ret);
      // Don't assert — some messages may fail (e.g. table already exists)
      tmq_free_raw(raw);
    }
    taos_free_result(msg);
  }

  printf("Replication finished: %d messages\n", msg_count);
  tmq_consumer_close(consumer);
  taos_close(dst);
}

static void replicate_with_group(const char *group_id) {
  tmq_t *consumer = create_consumer_ex(group_id);
  TAOS  *dst = connect_db(DST_DB);
  int    msg_count = 0;
  int    empty_polls = 0;
  int    max_empty = 10;

  printf("Starting replication (group=%s)...\n", group_id);
  while (empty_polls < max_empty) {
    TAOS_RES *msg = tmq_consumer_poll(consumer, 1000);
    if (!msg) {
      empty_polls++;
      continue;
    }
    empty_polls = 0;
    msg_count++;

    tmq_raw_data raw = {0};
    int32_t      code = tmq_get_raw(msg, &raw);
    if (code == 0) {
      int32_t ret = tmq_write_raw(dst, raw);
      printf("  replay msg %d: raw_type=%d, result=%s (%d)\n", msg_count, raw.raw_type, tmq_err2str(ret), ret);
      tmq_free_raw(raw);
    }
    taos_free_result(msg);
  }

  printf("Replay finished (group=%s): %d messages\n", group_id, msg_count);
  tmq_consumer_close(consumer);
  taos_close(dst);
}

static int verify_scenario(int scenario) {
  TAOS *dst = connect_db(DST_DB);
  int   pass = 1;

  switch (scenario) {
    case 1: {
      // Expect: STB stb1 + child tables ct1, ct2
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s1: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s1: tables=%d (expected 2)\n", tbl_count);
      if (tbl_count != 2) pass = 0;
      break;
    }
    case 2: {
      // Expect: no STB (rolled back)
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s2: stables=%d (expected 0)\n", stb_count);
      if (stb_count != 0) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s2: tables=%d (expected 0)\n", tbl_count);
      if (tbl_count != 0) pass = 0;
      break;
    }
    case 3: {
      // Expect: STB stb1 with 3 columns (ts, v, c2)
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s3: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int col_count = query_rows(dst, "describe " DST_DB ".stb1");
      printf("verify s3: columns+tags=%d (expected 4: ts,v,c2 + t1)\n", col_count);
      if (col_count != 4) pass = 0;
      break;
    }
    case 4: {
      // Expect: no STB (created then dropped within same txn)
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s4: stables=%d (expected 0)\n", stb_count);
      if (stb_count != 0) pass = 0;
      break;
    }
    case 5: {
      // Same as scenario 1 (idempotent — replay succeeds even on second run)
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s5: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s5: tables=%d (expected 2)\n", tbl_count);
      if (tbl_count != 2) pass = 0;
      break;
    }
    case 6: {
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s6: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s6: tables=%d (expected 2)\n", tbl_count);
      if (tbl_count != 2) pass = 0;
      break;
    }
    case 7: {
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s7: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s7: tables=%d (expected 1)\n", tbl_count);
      if (tbl_count != 1) pass = 0;
      break;
    }
    case 8: {
      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s8: tables=%d (expected 1)\n", tbl_count);
      if (tbl_count != 1) pass = 0;

      int col_count = query_rows(dst, "describe " DST_DB ".ntb1");
      printf("verify s8: columns=%d (expected 3)\n", col_count);
      if (col_count != 3) pass = 0;
      break;
    }
    case 9: {
      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s9: tables=%d (expected 0)\n", tbl_count);
      if (tbl_count != 0) pass = 0;
      break;
    }
    case 10: {
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s10: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s10: tables=%d (expected 2)\n", tbl_count);
      if (tbl_count != 2) pass = 0;
      break;
    }
    case 11: {
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s11: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s11: tables=%d (expected 12)\n", tbl_count);
      if (tbl_count != 12) pass = 0;
      break;
    }
    case 12: {
      // Low-watermark replay: STB + 2 CTBs + 1 NTB after double replay
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s12: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s12: tables=%d (expected 3)\n", tbl_count);
      if (tbl_count != 3) pass = 0;
      break;
    }
    case 13: {
      // Pre-existing STB altered: STB stb1 with 4 cols (ts, v, c2 + tag t1), 1 child table
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s13: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int col_count = query_rows(dst, "describe " DST_DB ".stb1");
      printf("verify s13: columns+tags=%d (expected 4: ts,v,c2 + t1)\n", col_count);
      if (col_count != 4) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s13: tables=%d (expected 1)\n", tbl_count);
      if (tbl_count != 1) pass = 0;
      break;
    }
    case 14: {
      // Pre-existing STB dropped: no STBs, no child tables
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s14: stables=%d (expected 0)\n", stb_count);
      if (stb_count != 0) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s14: tables=%d (expected 0)\n", tbl_count);
      if (tbl_count != 0) pass = 0;
      break;
    }
    case 15: {
      // Rollback alter: STB unchanged (ts,v + tag t1), and child table remains
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s15: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int col_count = query_rows(dst, "describe " DST_DB ".stb1");
      printf("verify s15: columns+tags=%d (expected 3: ts,v + t1)\n", col_count);
      if (col_count != 3) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s15: tables=%d (expected 1)\n", tbl_count);
      if (tbl_count != 1) pass = 0;
      break;
    }
    case 16: {
      // Rollback drop: STB and both child tables should remain
      int stb_count = query_rows(dst, "show " DST_DB ".stables");
      printf("verify s16: stables=%d (expected 1)\n", stb_count);
      if (stb_count != 1) pass = 0;

      int tbl_count = query_rows(dst, "show " DST_DB ".tables");
      printf("verify s16: tables=%d (expected 2)\n", tbl_count);
      if (tbl_count != 2) pass = 0;

      int cnt = query_count(dst, "select count(*) from " DST_DB ".ct1");
      printf("verify s16: ct1 rows=%d (expected 1)\n", cnt);
      if (cnt != 1) pass = 0;
      break;
    }
    default:
      printf("Unknown scenario: %d\n", scenario);
      pass = 0;
  }

  taos_close(dst);
  return pass;
}

static void cleanup(void) {
  TAOS *taos = connect_db(NULL);
  do_query_ok(taos, "drop topic if exists " TOPIC_NAME);
  do_query_ok(taos, "drop database if exists " SRC_DB);
  do_query_ok(taos, "drop database if exists " DST_DB);
  taos_close(taos);
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Usage: %s <scenario>\n", argv[0]);
    printf("  1: CREATE STB + child tables + COMMIT\n");
    printf("  2: CREATE STB + child tables + ROLLBACK\n");
    printf("  3: CREATE STB + ALTER STB + COMMIT\n");
    printf("  4: CREATE STB + DROP STB + COMMIT\n");
    printf("  5: Idempotent COMMIT (replay)\n");
    printf("  6: CREATE CTBs + ALTER child tag + COMMIT\n");
    printf("  7: CREATE CTBs + DROP child + COMMIT\n");
    printf("  8: CREATE normal table + ALTER + COMMIT\n");
    printf("  9: CREATE normal table + DROP + COMMIT\n");
    printf("  10: Mixed STB + CTB + normal table + COMMIT\n");
    printf("  11: Multi-VGroup: STB + 10 CTBs + 2 NTBs + COMMIT\n");
    printf("  12: Low-watermark replay: double replay for idempotent recovery\n");
    printf("  13: Pre-existing STB → ALTER STB → COMMIT (first MNode DDL = ALTER)\n");
    printf("  14: Pre-existing STB → DROP STB → COMMIT (first MNode DDL = DROP)\n");
    printf("  15: Pre-existing STB → ALTER STB → ROLLBACK (first MNode DDL = ALTER)\n");
    printf("  16: Pre-existing STB → DROP STB → ROLLBACK (first MNode DDL = DROP)\n");
    return 1;
  }

  int scenario = atoi(argv[1]);
  printf("=== tmq_taosx_txn scenario %d ===\n", scenario);

  // Phase 1: Setup source with transaction
  setup_source(scenario);

  // Phase 2: Replicate via TMQ
  replicate_to_target();

  // Phase 2b: For scenario 12, replay again with a new consumer group
  // Simulates crash recovery with low-watermark offset reset
  if (scenario == 12) {
    printf("=== s12: Replay #2 (simulating crash recovery from beginning) ===\n");
    replicate_with_group("taosx_txn_replay");
  }

  // Phase 3: Verify target
  int pass = verify_scenario(scenario);

  // Phase 4: Cleanup
  cleanup();

  if (pass) {
    printf("=== PASS ===\n");
    return 0;
  } else {
    printf("=== FAIL ===\n");
    return 1;
  }
}
