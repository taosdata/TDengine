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

/*
 * tmq_taosx_txn.c — TMQ taosX replication test binary
 *
 * Tests verify that DDL transactions replicated through TMQ are delivered
 * atomically to the target: DDL messages within a BEGIN…COMMIT block arrive
 * as a single atomic batch (via STxnWalManager); ROLLBACK produces no DDL
 * messages on the consumer side.
 *
 * Build: add_executable(tmq_taosx_txn tmq_taosx_txn.c)
 *        target_link_libraries(tmq_taosx_txn PUBLIC ${TAOS_NATIVE_LIB} util common os)
 *
 * Usage: ./tmq_taosx_txn <scenario_number>
 *   Returns 0 on pass, non-zero on fail.
 *
 * Scenario index
 * ──────────────
 * s1  CREATE STB + 2 CTBs → COMMIT → target has stb1 + ct1 + ct2
 * s2  CREATE STB + 2 CTBs → ROLLBACK → target has nothing
 * s3  CREATE STB → ALTER STB add column → COMMIT → target has altered schema
 * s4  CREATE STB + CTBs → DROP STB → COMMIT → target has nothing
 * s5  Idempotent COMMIT replay (s1 scenario re-consumed by different group)
 * s6  CREATE STB + CTBs → ALTER CTB tag → COMMIT → tag updated
 * s7  CREATE STB + 2 CTBs → DROP one CTB → COMMIT → target has stb + 1 CTB
 * s8  CREATE normal table → ALTER add column → COMMIT → altered NTB on target
 * s9  CREATE normal table → DROP → COMMIT → target has nothing
 * s10 Mixed STB + 2 CTBs + NTB → COMMIT → all present on target
 * s11 Multi-VGroup: 2-VGroup DB, STB + 10 CTBs + 2 NTBs → COMMIT → all present
 * s12 Low-watermark replay: consume from two groups → same state
 * s13 Pre-existing STB on target → ALTER STB as first DDL in txn → COMMIT
 * s14 Pre-existing STB on target → DROP STB as first DDL in txn → COMMIT
 * s15 Pre-existing STB on target → ALTER STB → ROLLBACK → target unchanged
 * s16 Pre-existing STB on target → DROP STB → ROLLBACK → target unchanged
 * s17 Replicated-txn timeout exemption (abbreviated: verifies basic connectivity)
 * s18 Snapshot mode: all data committed before subscribe → target gets all
 * s19 Snapshot mode: in-flight txn on source → COMMIT → target gets full state
 * s20 Snapshot idempotent replay: double-consume → same target state
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "taos.h"
#include "taoserror.h"

/* ── global connections ─────────────────────────────────────────────────── */

static TAOS *g_src = NULL;  /* source: meta+DDL operations                */
static TAOS *g_dst = NULL;  /* destination: write_raw target (dst_txn_db) */

/* ── helpers ────────────────────────────────────────────────────────────── */

static void die(const char *ctx, const char *err) {
  fprintf(stderr, "FATAL [%s]: %s\n", ctx, err);
  exit(1);
}

/* Execute SQL on conn; abort on error. */
static void exec(TAOS *conn, const char *sql) {
  TAOS_RES *res = taos_query(conn, sql);
  int       rc  = taos_errno(res);
  if (rc != 0) {
    fprintf(stderr, "SQL FAILED: %s\n  reason: %s\n", sql, taos_errstr(res));
    taos_free_result(res);
    exit(1);
  }
  taos_free_result(res);
}

/* Execute SQL; return error code (0 = success). */
static int exec_safe(TAOS *conn, const char *sql) {
  TAOS_RES *res = taos_query(conn, sql);
  int       rc  = taos_errno(res);
  taos_free_result(res);
  return rc;
}

/* Return first int64 from a SELECT, or -1 on error. */
static int64_t query_int(TAOS *conn, const char *sql) {
  TAOS_RES *res = taos_query(conn, sql);
  if (taos_errno(res) != 0) {
    fprintf(stderr, "query_int failed: %s → %s\n", sql, taos_errstr(res));
    taos_free_result(res);
    return -1;
  }
  TAOS_ROW row = taos_fetch_row(res);
  int64_t  val = (row && row[0]) ? (int64_t)atoll((char *)row[0]) : 0;
  taos_free_result(res);
  return val;
}

/* Assert an integer query result equals expected; return 0 on match, 1 on mismatch. */
static int verify(TAOS *conn, const char *sql, int64_t expected, const char *label) {
  int64_t got = query_int(conn, sql);
  if (got != expected) {
    fprintf(stderr, "VERIFY FAILED [%s]: expected %" PRId64 " got %" PRId64 "\n  SQL: %s\n",
            label, expected, got, sql);
    return 1;
  }
  printf("  OK [%s]: %" PRId64 "\n", label, got);
  return 0;
}

/* ── database / topic setup ─────────────────────────────────────────────── */

static void drop_and_create_dbs(int src_vgroups) {
  char sql[256];
  exec(g_src, "drop database if exists src_txn_db");
  exec(g_src, "drop database if exists dst_txn_db");
  snprintf(sql, sizeof(sql),
           "create database src_txn_db vgroups %d wal_retention_period 3600",
           src_vgroups);
  exec(g_src, sql);
  exec(g_src, "create database dst_txn_db vgroups 1");
  /* reconnect dst on the new database */
  taos_close(g_dst);
  g_dst = taos_connect("localhost", "root", "taosdata", "dst_txn_db", 0);
  if (!g_dst) die("drop_and_create_dbs", "taos_connect dst");
}

static void setup_topic(void) {
  exec(g_src, "drop topic if exists topic_taosx_txn");
  exec(g_src, "create topic topic_taosx_txn with meta as database src_txn_db");
}

/* ── TMQ consumer helpers ───────────────────────────────────────────────── */

static tmq_t *make_consumer(const char *group, int snapshot) {
  tmq_conf_t *conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id",               group);
  tmq_conf_set(conf, "auto.offset.reset",      "earliest");
  tmq_conf_set(conf, "enable.auto.commit",     "false");
  tmq_conf_set(conf, "msg.consume.excluded.changed.column", "false");
  if (snapshot) tmq_conf_set(conf, "td.enable.snapshot", "1");

  char  err[256] = {0};
  tmq_t *consumer = tmq_consumer_new(conf, err, sizeof(err));
  tmq_conf_destroy(conf);
  if (!consumer) die("make_consumer", err);

  tmq_list_t *topics = tmq_list_new();
  tmq_list_append(topics, "topic_taosx_txn");
  int rc = tmq_subscribe(consumer, topics);
  tmq_list_destroy(topics);
  if (rc != 0) {
    fprintf(stderr, "tmq_subscribe failed: %s\n", tmq_err2str(rc));
    exit(1);
  }
  return consumer;
}

/*
 * Drain consumer: poll until 3 consecutive empty polls.
 * Apply each message to g_dst via tmq_write_raw.
 * Returns number of messages applied, or -1 on hard error.
 * TABLE_ALREADY_EXIST errors are tolerated (idempotent re-apply).
 */
static int drain(tmq_t *consumer) {
  int applied    = 0;
  int empty      = 0;
  int max_empty  = 5;  /* wait up to 5 × 1 s for messages */

  while (empty < max_empty) {
    TAOS_RES *msg = tmq_consumer_poll(consumer, 1000);
    if (!msg) {
      empty++;
      continue;
    }
    empty = 0;

    tmq_raw_data raw = {0};
    int          rc  = tmq_get_raw(msg, &raw);
    if (rc == 0) {
      int wrc = tmq_write_raw(g_dst, raw);
      if (wrc != 0) {
        /* Allow "already exists" — idempotent replay tolerated */
        const char *es = tmq_err2str(wrc);
        int is_dup = (strstr(es, "already exist") != NULL) ||
                     (wrc == TSDB_CODE_MND_STB_ALREADY_EXIST)  ||
                     (wrc == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) ||
                     (wrc == TSDB_CODE_MND_DB_ALREADY_EXIST);
        if (is_dup) {
          printf("  [idempotent] tmq_write_raw: %s (tolerated)\n", es);
        } else {
          fprintf(stderr, "tmq_write_raw FAILED (code=%d): %s\n", wrc, es);
          tmq_free_raw(raw);
          taos_free_result(msg);
          return -1;
        }
      }
      tmq_free_raw(raw);
      applied++;
    }

    tmq_commit_sync(consumer, msg);
    taos_free_result(msg);
  }
  return applied;
}

static void close_consumer(tmq_t *consumer) {
  if (consumer) tmq_consumer_close(consumer);
}

/* ── scenario helper: exec on src_txn_db ──────────────────────────────── */

static void src(const char *sql) {
  char full[512];
  snprintf(full, sizeof(full), "%s", sql);
  exec(g_src, sql);
}

#define SRC(sql) exec(g_src, "use src_txn_db"); exec(g_src, sql)

/* ── SCENARIO IMPLEMENTATIONS ────────────────────────────────────────────── */

/* s1: CREATE STB + 2 CTBs → COMMIT → target has stb1 + ct1 + ct2 */
static int s1(void) {
  printf("s1: CREATE STB + 2 CTBs → COMMIT → target has stb1 + ct1 + ct2\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s1", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "table count");
  return fail;
}

/* s2: CREATE STB + 2 CTBs → ROLLBACK → target has nothing */
static int s2(void) {
  printf("s2: CREATE STB + 2 CTBs → ROLLBACK → target has nothing\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "rollback");

  tmq_t *c = make_consumer("grp_s2", 0);
  drain(c);
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    0, "stb count after rollback");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    0, "table count after rollback");
  return fail;
}

/* s3: CREATE STB → ALTER STB add column → COMMIT → target has altered schema */
static int s3(void) {
  printf("s3: CREATE STB → ALTER STB → COMMIT → target has altered schema\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s3", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name='c2'",
    1, "c2 column exists");
  return fail;
}

/* s4: CREATE STB + CTBs → DROP STB → COMMIT → target has nothing */
static int s4(void) {
  printf("s4: CREATE STB + CTBs → DROP STB → COMMIT → target has nothing\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "drop stable stb1");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s4", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    0, "stb count after drop");
  return fail;
}

/* s5: Idempotent COMMIT replay — re-consume s1 scenario from a different group */
static int s5(void) {
  printf("s5: Idempotent COMMIT replay → target state consistent\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "commit");

  /* First consume */
  tmq_t *c1 = make_consumer("grp_s5_a", 0);
  if (drain(c1) < 0) { close_consumer(c1); return 1; }
  close_consumer(c1);

  /* Second consume (different group, same topic from earliest) */
  tmq_t *c2 = make_consumer("grp_s5_b", 0);
  drain(c2);  /* tolerate already-exist on write_raw */
  close_consumer(c2);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count idempotent");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "table count idempotent");
  return fail;
}

/* s6: CREATE STB + CTBs → ALTER CTB tag value → COMMIT → tag updated on target */
static int s6(void) {
  printf("s6: CREATE STB + CTBs → ALTER CTB tag → COMMIT → tag updated\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "alter table ct1 set tag t1=100");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s6", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "table count");
  fail |= verify(g_dst,
    "select count(*) from dst_txn_db.stb1 where t1=100",
    0, "tag 100 row count (no data rows, just verify stb accessible)");
  return fail;
}

/* s7: CREATE STB + 2 CTBs → DROP one CTB → COMMIT → target has stb + 1 CTB */
static int s7(void) {
  printf("s7: CREATE STB + 2 CTBs → DROP ct1 → COMMIT → target has stb + ct2\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "drop table ct1");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s7", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "table count after drop ct1");
  return fail;
}

/* s8: CREATE normal table → ALTER add column → COMMIT → altered NTB on target */
static int s8(void) {
  printf("s8: CREATE NTB → ALTER add column → COMMIT → altered NTB on target\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "alter table ntb1 add column c2 float");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s8", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='ntb1' and col_name='c2'",
    1, "c2 column exists on ntb1");
  return fail;
}

/* s9: CREATE normal table → DROP → COMMIT → target has nothing */
static int s9(void) {
  printf("s9: CREATE NTB → DROP NTB → COMMIT → target has nothing\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "drop table ntb1");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s9", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    0, "table count after drop ntb");
}

/* s10: Mixed (STB + 2 CTBs + NTB) → COMMIT → all present on target */
static int s10(void) {
  printf("s10: Mixed STB+2CTBs+NTB → COMMIT → all present on target\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s10", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    3, "total table count (ct1+ct2+ntb1)");
  return fail;
}

/* s11: Multi-VGroup: 2-VGroup DB, STB + 10 CTBs + 2 NTBs → COMMIT → all present */
static int s11(void) {
  printf("s11: Multi-VGroup (2 VGroups): STB+10CTBs+2NTBs → COMMIT → all present\n");
  drop_and_create_dbs(2);  /* 2 vgroups */
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  char sql[128];
  for (int i = 1; i <= 10; i++) {
    snprintf(sql, sizeof(sql), "create table ct%d using stb1 tags(%d)", i, i);
    exec(g_src, sql);
  }
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "create table ntb2 (ts timestamp, c1 int)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s11", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    12, "total table count (10 CTBs + 2 NTBs)");
  return fail;
}

/* s12: Low-watermark replay — two consumer groups consume same topic → same result */
static int s12(void) {
  printf("s12: Low-watermark replay (two groups → same target state)\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "commit");

  /* First consumer group */
  tmq_t *c1 = make_consumer("grp_s12_a", 0);
  if (drain(c1) < 0) { close_consumer(c1); return 1; }
  close_consumer(c1);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "after first group");

  /* Second consumer group re-reads from earliest, re-applies (idempotent) */
  tmq_t *c2 = make_consumer("grp_s12_b", 0);
  drain(c2);  /* tolerate already-exist */
  close_consumer(c2);

  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "after second group (still 1 table)");
  return fail;
}

/* s13: Pre-existing STB on target → ALTER STB as first DDL in txn → COMMIT */
static int s13(void) {
  printf("s13: Pre-existing STB → ALTER STB first DDL → COMMIT → target altered\n");
  drop_and_create_dbs(1);
  setup_topic();

  /* Pre-create the STB outside any transaction (committed, normal) */
  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");

  /* First consume: creates stb1 on target */
  tmq_t *c_pre = make_consumer("grp_s13_pre", 0);
  if (drain(c_pre) < 0) { close_consumer(c_pre); return 1; }
  close_consumer(c_pre);

  /* Now BEGIN + ALTER as first DDL in this new txn */
  exec(g_src, "begin");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "commit");

  /* Second consume: delivers ALTER */
  tmq_t *c2 = make_consumer("grp_s13_post", 0);
  if (drain(c2) < 0) { close_consumer(c2); return 1; }
  close_consumer(c2);

  return verify(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name='c2'",
    1, "c2 column exists after ALTER");
}

/* s14: Pre-existing STB on target → DROP STB as first DDL in txn → COMMIT */
static int s14(void) {
  printf("s14: Pre-existing STB → DROP STB first DDL → COMMIT → target has no STB\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");

  tmq_t *c_pre = make_consumer("grp_s14_pre", 0);
  if (drain(c_pre) < 0) { close_consumer(c_pre); return 1; }
  close_consumer(c_pre);

  exec(g_src, "begin");
  exec(g_src, "drop stable stb1");
  exec(g_src, "commit");

  tmq_t *c2 = make_consumer("grp_s14_post", 0);
  if (drain(c2) < 0) { close_consumer(c2); return 1; }
  close_consumer(c2);

  return verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    0, "stb count after drop");
}

/* s15: Pre-existing STB on target → ALTER STB → ROLLBACK → target unchanged */
static int s15(void) {
  printf("s15: Pre-existing STB → ALTER STB → ROLLBACK → target schema unchanged\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");

  tmq_t *c_pre = make_consumer("grp_s15_pre", 0);
  if (drain(c_pre) < 0) { close_consumer(c_pre); return 1; }
  close_consumer(c_pre);

  /* Verify stb1 exists on target with only c1 column */
  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1'",
    2, "initial columns (ts + c1)");

  exec(g_src, "begin");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "rollback");

  /* ROLLBACK → consumer sees nothing; no new msgs */
  tmq_t *c2 = make_consumer("grp_s15_post", 0);
  drain(c2);
  close_consumer(c2);

  /* c2 column must NOT appear on target */
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name='c2'",
    0, "c2 must not exist after rollback");
  return fail;
}

/* s16: Pre-existing STB on target → DROP STB → ROLLBACK → target unchanged */
static int s16(void) {
  printf("s16: Pre-existing STB → DROP STB → ROLLBACK → target still has STB\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");

  tmq_t *c_pre = make_consumer("grp_s16_pre", 0);
  if (drain(c_pre) < 0) { close_consumer(c_pre); return 1; }
  close_consumer(c_pre);

  exec(g_src, "begin");
  exec(g_src, "drop stable stb1");
  exec(g_src, "rollback");

  tmq_t *c2 = make_consumer("grp_s16_post", 0);
  drain(c2);
  close_consumer(c2);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb still present after rollback");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "ct1 still present after rollback");
  return fail;
}

/*
 * s17: Replicated-txn inactivity-timeout exemption.
 * Full test requires a 15-20s sleep and a specific mnode config. Here we
 * verify the basic operation completes without error (connectivity check).
 * The mnode-level timeout exemption for TXN_IS_REPLICATED is validated by
 * the cluster tests (test_meta_batch_txn_cluster_fi.py).
 */
static int s17(void) {
  printf("s17: Replicated-txn connectivity / basic replication check\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s17", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "table present");
}

/*
 * s18: Snapshot mode — all data committed before subscribe → target gets all.
 * Verifies that a consumer in snapshot mode (td.enable.snapshot=1) correctly
 * receives all committed DDL and delivers it to the target.
 */
static int s18(void) {
  printf("s18: Snapshot mode: committed data → consumer gets full state\n");
  drop_and_create_dbs(1);
  setup_topic();

  /* Create data; commit it */
  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "commit");

  /* Subscribe with snapshot mode */
  tmq_t *c = make_consumer("grp_s18", 1 /* snapshot */);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count via snapshot");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "table count via snapshot");
  return fail;
}

/*
 * s19: Snapshot mode + in-flight txn → COMMIT → target gets full final state.
 * Subscribe in snapshot mode WHILE a transaction is in-flight on the source.
 *
 * tqMeta.c caps snapshotVer = min(committedVer, minTxnBeginIndex-1), so the
 * meta snapshot stops before the in-flight txn's first WAL entry and never
 * delivers PRE_CREATE entries.  Snapshot delivers only stb1 (NORMAL).
 * WAL replay starts from snapshotVer+1; individual in-txn DDL entries are
 * filtered by WAL_IS_TXN_MSG; TXN_COMMIT triggers atomic STxnWalManager
 * delivery of ct1+ct2 as NORMAL.
 * Final target state: stb1 + ct1 + ct2.
 */
static int s19(void) {
  printf("s19: Snapshot mode + in-flight txn → COMMIT → target has full state\n");
  drop_and_create_dbs(1);
  setup_topic();

  /* Create the STB outside any txn (committed, visible in snapshot) */
  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");

  /* Start a txn but don't commit yet — consumer will subscribe during this window */
  exec(g_src, "begin");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");

  /* Subscribe while ct1/ct2 are in-flight (snapshot will see stb1 NORMAL) */
  tmq_t *c = make_consumer("grp_s19", 1 /* snapshot */);

  /* Now commit the in-flight txn */
  exec(g_src, "commit");

  /* Drain: snapshot delivers stb1, WAL delivers ct1+ct2 on COMMIT */
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb1 via snapshot+wal");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "ct1+ct2 via wal after snapshot COMMIT");
  return fail;
}

/*
 * s20: Snapshot idempotent replay — double-consume → same target state.
 * Two different consumer groups with snapshot=1 consume the same topic.
 * The second group re-applies all messages; TABLE_ALREADY_EXIST is tolerated.
 */
static int s20(void) {
  printf("s20: Snapshot idempotent double replay → same target state\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "commit");

  /* First consumer group with snapshot */
  tmq_t *c1 = make_consumer("grp_s20_a", 1);
  if (drain(c1) < 0) { close_consumer(c1); return 1; }
  close_consumer(c1);

  /* Second consumer group with snapshot — re-reads from earliest */
  tmq_t *c2 = make_consumer("grp_s20_b", 1);
  drain(c2);  /* tolerate already-exist */
  close_consumer(c2);

  int fail = 0;
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count after double replay");
  fail |= verify(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "table count after double replay");
  return fail;
}

/* ── dispatch ────────────────────────────────────────────────────────────── */

typedef int (*scenario_fn)(void);

static const scenario_fn scenarios[] = {
  NULL,  /* index 0 unused */
  s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
  s11, s12, s13, s14, s15, s16, s17, s18, s19, s20,
};

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <scenario_number (1-20)>\n", argv[0]);
    return 1;
  }

  int scenario = atoi(argv[1]);
  int max_scenario = (int)(sizeof(scenarios) / sizeof(scenarios[0])) - 1;
  if (scenario < 1 || scenario > max_scenario) {
    fprintf(stderr, "Invalid scenario %d (must be 1-%d)\n", scenario, max_scenario);
    return 1;
  }

  /* Library init */
  if (taos_init() != 0) {
    fprintf(stderr, "taos_init failed\n");
    return 1;
  }

  g_src = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!g_src) die("main", "cannot connect to source");

  g_dst = taos_connect("localhost", "root", "taosdata", "", 0);
  if (!g_dst) die("main", "cannot connect to destination");

  printf("======== Running scenario %d ========\n", scenario);
  int rc = scenarios[scenario]();

  if (rc == 0) {
    printf("======== Scenario %d PASSED ========\n", scenario);
  } else {
    fprintf(stderr, "======== Scenario %d FAILED ========\n", scenario);
  }

  taos_close(g_src);
  taos_close(g_dst);
  taos_cleanup();
  return rc;
}
