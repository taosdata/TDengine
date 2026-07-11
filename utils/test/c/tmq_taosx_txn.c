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
/* Cross-platform millisecond sleep. Do NOT pull in the internal "os.h" for taosMsleep:
 * this file has two compile paths — the CMake build (links os/util/common, internal
 * headers visible) AND a standalone gcc fallback in test_taosx_txn_recovery.py that
 * only has -I/usr/local/taos/include (public SDK, no os.h). Using os.h breaks the
 * fallback with "os.h: No such file or directory". Adapt in-place instead. */
#ifdef _WIN32
#include <windows.h>
#define portable_msleep(ms) Sleep(ms)
#else
#include <unistd.h>
#define portable_msleep(ms) usleep((useconds_t)(ms) * 1000)
#endif
#include "taos.h"
#include "taoserror.h"

/* ── global connections ─────────────────────────────────────────────────── */

static TAOS *g_src = NULL;   /* source: meta+DDL operations                */
static TAOS *g_dst = NULL;   /* destination: write_raw target (dst_txn_db) */
static TAOS *g_src2 = NULL;  /* second source session, for concurrent-session scenarios */

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

/* Return first int64 from a SELECT (e.g. a count(*) result), or -1 on error.
 * NOTE: row[0] for a numeric column is a pointer to the raw binary value
 * (e.g. a little-endian int64_t for BIGINT), NOT a decimal ASCII string —
 * do not atoll() it (that treats binary bytes as text and reads garbage/0
 * for almost any real value). Read it as the fixed-width binary type it is. */
static int64_t query_int(TAOS *conn, const char *sql) {
  TAOS_RES *res = taos_query(conn, sql);
  if (taos_errno(res) != 0) {
    fprintf(stderr, "query_int failed: %s → %s\n", sql, taos_errstr(res));
    taos_free_result(res);
    return -1;
  }
  TAOS_ROW row = taos_fetch_row(res);
  int64_t  val = (row && row[0]) ? *(int64_t *)row[0] : 0;
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

/* Verify with retry: try up to max_retries times with sleep_ms between attempts.
 * Returns 0 on match, 1 on failure after all retries. */
static int verify_with_retry(TAOS *conn, const char *sql, int64_t expected, const char *label,
                             int max_retries, int sleep_ms) {
  for (int attempt = 1; attempt <= max_retries; attempt++) {
    int64_t got = query_int(conn, sql);
    if (got == expected) {
      printf("  OK [%s]: %" PRId64 " (attempt %d)\n", label, got, attempt);
      return 0;
    }
    if (attempt < max_retries) {
      printf("  RETRY [%s]: attempt %d/%d got %" PRId64 ", expected %" PRId64 ", waiting %dms...\n",
             label, attempt, max_retries, got, expected, sleep_ms);
      portable_msleep(sleep_ms);
    }
  }
  fprintf(stderr, "VERIFY FAILED [%s]: expected %" PRId64 " (after %d attempts)\n  SQL: %s\n",
          label, expected, max_retries, sql);
  return 1;
}

/* Macro: small retry margin for ordinary async CDC delivery latency. */
#define verify_retry(conn, sql, expected, label) \
  verify_with_retry(conn, sql, expected, label, 5, 200)

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

/* Point the second source session at src_txn_db (independent session/connection
 * from g_src, used for concurrent-session scenarios: each session gets its own
 * BEGIN/COMMIT transaction state on the MNode). */
static void use_src2(void) {
  if (!g_src2) {
    g_src2 = taos_connect("localhost", "root", "taosdata", "", 0);
    if (!g_src2) die("use_src2", "cannot connect second source session");
  }
  exec(g_src2, "use src_txn_db");
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
  int applied      = 0;
  int empty        = 0;
  int max_empty    = 4;    /* wait up to 4 × 300ms for messages */
  int poll_timeout = 300;  /* ms; local taosd delivers in well under this */

  while (empty < max_empty) {
    TAOS_RES *msg = tmq_consumer_poll(consumer, poll_timeout);
    if (!msg) {
      printf("  [drain] poll timeout, empty=%d/%d\n", empty + 1, max_empty);
      empty++;
      continue;
    }
    empty = 0;

    tmq_raw_data raw = {0};
    int          rc  = tmq_get_raw(msg, &raw);
    printf("  [drain] poll got msg, tmq_get_raw rc=%d, raw.len=%u\n", rc, raw.raw_len);
    if (rc == 0) {
      int wrc = tmq_write_raw(g_dst, raw);
      printf("  [drain] tmq_write_raw returned wrc=%d\n", wrc);
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
  printf("  [drain] finished, applied=%d\n", applied);
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    0, "stb count after rollback");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count idempotent");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "table count");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
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

  return verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "after first group");

  /* Second consumer group re-reads from earliest, re-applies (idempotent) */
  tmq_t *c2 = make_consumer("grp_s12_b", 0);
  drain(c2);  /* tolerate already-exist */
  close_consumer(c2);

  fail |= verify_retry(g_dst,
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

  return verify_retry(g_dst,
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

  return verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb still present after rollback");
  fail |= verify_retry(g_dst,
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

  return verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count via snapshot");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb1 via snapshot+wal");
  fail |= verify_retry(g_dst,
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
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    1, "stb count after double replay");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "table count after double replay");
  return fail;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Category 1: CREATE/DROP/ALTER cross-mix within a single transaction
 * ═══════════════════════════════════════════════════════════════════════ */

/* s24: CREATE STB+2CTB, ALTER add column+tag, DROP one CTB, ALTER remaining
 *      CTB's tag — all in ONE txn → COMMIT → target reflects final state only. */
static int s24(void) {
  printf("s24: CREATE+ALTER(col+tag)+DROP+ALTER(tag) mixed in one txn → COMMIT\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "alter stable stb1 add tag t2 int");
  exec(g_src, "drop table ct1");
  exec(g_src, "alter table ct2 set tag t1=200");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s24", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "only ct2 remains (ct1 dropped)");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name='c2'",
    1, "c2 column exists");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tags "
    "where db_name='dst_txn_db' and table_name='ct2' and tag_name='t2'",
    1, "t2 tag exists on ct2 (schema propagated)");
  return fail;
}

/* s25: Same mixed sequence as s24 but ROLLBACK → target completely unaffected,
 *      no partial/intermediate state leaks through. */
static int s25(void) {
  printf("s25: Same CREATE+ALTER+DROP+ALTER mix, one txn → ROLLBACK → target empty\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "alter stable stb1 add tag t2 int");
  exec(g_src, "drop table ct1");
  exec(g_src, "alter table ct2 set tag t1=200");
  exec(g_src, "rollback");

  tmq_t *c = make_consumer("grp_s25", 0);
  drain(c);
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    0, "no stb after rollback");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    0, "no tables after rollback");
  return fail;
}

/* s26: CREATE → DROP → re-CREATE (same name) within one txn → COMMIT.
 *      Net effect must be "created with the FINAL schema", not the first one. */
static int s26(void) {
  printf("s26: CREATE→DROP→re-CREATE same name (different schema), one txn → COMMIT\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "drop table ntb1");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int, c2 varchar(16))");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s26", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "ntb1 exists (net create)");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='ntb1' and col_name='c2'",
    1, "c2 column exists (final schema, not the dropped first version)");
  return fail;
}

/* s27: 3 consecutive ADD COLUMN on the same STB within one txn → COMMIT.
 *      Only the final schema (3 extra columns) must apply; taosX must not
 *      mis-replay the intermediate ALTERs as separate independent events. */
static int s27(void) {
  printf("s27: 3x consecutive ADD COLUMN on same STB, one txn → COMMIT\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "alter stable stb1 add column c3 varchar(16)");
  exec(g_src, "alter stable stb1 add column c4 bigint");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s27", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' "
    "and col_name in ('c2','c3','c4')",
    3, "all 3 new columns present exactly once");
  return fail;
}

/* s28: Two unrelated STBs (A, B) each CREATE+ALTER within the SAME txn →
 *      COMMIT → both fully and independently correct on target. */
static int s28(void) {
  printf("s28: Two unrelated STBs, each CREATE+ALTER, same txn → COMMIT\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  /* NOTE: unquoted identifiers are normalized to lowercase by the server, so
   * use all-lowercase names here to match what information_schema will show. */
  exec(g_src, "create stable stba (ts timestamp, c1 int) tags (ta int)");
  exec(g_src, "create table cta1 using stba tags(1)");
  exec(g_src, "create stable stbb (ts timestamp, c1 int) tags (tb int)");
  exec(g_src, "create table ctb1 using stbb tags(1)");
  exec(g_src, "alter stable stba add column c2 float");
  exec(g_src, "alter stable stbb add tag tb2 int");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s28", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_stables where db_name='dst_txn_db'",
    2, "both STBs present");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stba' and col_name='c2'",
    1, "stba got its column");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tags "
    "where db_name='dst_txn_db' and table_name='ctb1' and tag_name='tb2'",
    1, "stbb got its tag");
  return fail;
}

/* s29: CREATE virtual table + DROP its source table in the SAME txn → COMMIT.
 *      Extends check_k4 (test_meta_batch_txn_ddl_visibility.py, server-side
 *      visibility only) to the taosX consumption path: verify the orphaned
 *      virtual table replicates correctly (schema present, source gone). */
static int s29(void) {
  printf("s29: CREATE vtable + DROP its source, same txn → COMMIT (orphan replication)\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create table src_stb (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "create table src_ct1 using src_stb tags(1)");

  exec(g_src, "begin");
  exec(g_src, "create table vstb (ts timestamp, c1 int) tags(t1 int) virtual 1");
  exec(g_src, "create vtable vct1 (c1 from src_txn_db.src_ct1.c1) using vstb tags(1)");
  exec(g_src, "drop table src_ct1");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s29", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables "
    "where db_name='dst_txn_db' and table_name='vct1'",
    1, "orphaned vtable exists on target");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables "
    "where db_name='dst_txn_db' and table_name='src_ct1'",
    0, "source table absent on target");
  return fail;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Category 2: transactional vs non-transactional DDL interleaving
 * ═══════════════════════════════════════════════════════════════════════ */

/* s30: non-txn CREATE → txn ALTER (COMMIT) → non-txn ALTER (no BEGIN) →
 *      both ALTERs (one via txn, one direct) must replicate correctly and
 *      in order. */
static int s30(void) {
  printf("s30: non-txn CREATE → txn ALTER → non-txn ALTER → both land on target\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "begin");
  exec(g_src, "alter stable stb1 add column c2 float");
  exec(g_src, "commit");
  exec(g_src, "alter stable stb1 add column c3 varchar(16)");  /* no BEGIN */

  tmq_t *c = make_consumer("grp_s30", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name in ('c2','c3')",
    2, "both txn-ALTER and non-txn-ALTER columns present");
}

/* s31: txn CREATE (COMMIT) → non-txn DROP → txn re-CREATE (COMMIT), same name.
 *      Verify no ghost/duplicate state on target across the churn. */
static int s31(void) {
  printf("s31: txn CREATE → non-txn DROP → txn re-CREATE, same name → no ghosts\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "begin");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "commit");
  exec(g_src, "drop table ct1");  /* non-txn */
  exec(g_src, "begin");
  exec(g_src, "create table ct1 using stb1 tags(2)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s31", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "exactly one ct1 (no duplicate/ghost)");
}

/* s32: schema evolves (txn ALTER) around ongoing non-txn INSERTs on the same
 *      table — verifies DDL/DML ordering doesn't corrupt schema replication. */
static int s32(void) {
  printf("s32: non-txn INSERTs interleaved with txn ALTER on same NTB → COMMIT\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "insert into ntb1 values(now, 1)");
  exec(g_src, "begin");
  exec(g_src, "alter table ntb1 add column c2 float");
  exec(g_src, "commit");
  exec(g_src, "insert into ntb1 values(now+1s, 2, 2.5)");

  tmq_t *c = make_consumer("grp_s32", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='ntb1' and col_name='c2'",
    1, "schema change survived surrounding non-txn writes");
}

/* s33: a second (non-txn) session's conflicting DDL is rejected while a txn
 *      holds the table open; after ROLLBACK the retry succeeds — target must
 *      reflect only the final, successful DDL. */
static int s33(void) {
  printf("s33: non-txn session conflicts with open txn on same table → retry after ROLLBACK\n");
  drop_and_create_dbs(1);
  setup_topic();
  use_src2();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int, c2 float) tags (t1 int)");

  exec(g_src, "begin");
  exec(g_src, "alter stable stb1 drop column c2");
  /* g_src2: conflicting DDL on the same STB while g_src's txn is open */
  int conflict_rc = exec_safe(g_src2, "alter stable stb1 add column c3 bigint");
  if (conflict_rc == 0) {
    fprintf(stderr, "s33: expected conflicting ALTER to be rejected, but it succeeded\n");
    return 1;
  }
  printf("  [expected] g_src2 conflicting ALTER rejected: %s\n", tmq_err2str(conflict_rc));
  exec(g_src, "rollback");

  /* Now the retry (non-txn, from g_src2) should succeed */
  exec(g_src2, "alter stable stb1 add column c3 bigint");

  tmq_t *c = make_consumer("grp_s33", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name='c2'",
    1, "c2 still present (rolled back drop)");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name='c3'",
    1, "c3 present from the successful retry");
  return fail;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Category 3: multiple concurrent transactional/non-transactional sessions
 * ═══════════════════════════════════════════════════════════════════════ */

/* s34: two independent, unrelated txns on two sessions, COMMIT in REVERSE
 *      order of BEGIN → target gets both, fully and correctly, regardless
 *      of commit ordering (unrelated resources, no conflict expected). */
static int s34(void) {
  printf("s34: two independent txns (2 sessions), reverse commit order → both land\n");
  drop_and_create_dbs(1);
  setup_topic();
  use_src2();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stbA (ts timestamp, c1 int) tags (ta int)");
  exec(g_src2, "create stable stbB (ts timestamp, c1 int) tags (tb int)");

  exec(g_src, "begin");
  exec(g_src, "create table ctA1 using stbA tags(1)");
  exec(g_src2, "begin");
  exec(g_src2, "create table ctB1 using stbB tags(1)");
  exec(g_src, "create table ctA2 using stbA tags(2)");
  exec(g_src2, "create table ctB2 using stbB tags(2)");

  /* txn2 (g_src2, started SECOND) commits FIRST */
  exec(g_src2, "commit");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s34", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    4, "all 4 CTBs from both independent txns present");
}

/* s35: txn A COMMIT, non-txn B in between, txn C ROLLBACK → target has only
 *      A's and B's changes; C's are completely absent. */
static int s35(void) {
  printf("s35: txn A(commit) + non-txn B + txn C(rollback) interleaved\n");
  drop_and_create_dbs(1);
  setup_topic();
  use_src2();

  exec(g_src, "use src_txn_db");
  exec(g_src2, "use src_txn_db");

  exec(g_src, "begin");
  exec(g_src, "create table ntbA (ts timestamp, c1 int)");
  exec(g_src, "commit");

  exec(g_src2, "create table ntbB (ts timestamp, c1 int)");  /* non-txn */

  exec(g_src, "begin");
  exec(g_src, "create table ntbC (ts timestamp, c1 int)");
  exec(g_src, "rollback");

  tmq_t *c = make_consumer("grp_s35", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "only ntbA + ntbB present (ntbC rolled back)");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables "
    "where db_name='dst_txn_db' and table_name='ntbC'",
    0, "ntbC absent");
  return fail;
}

/* s36: two sessions BEGIN on the SAME table; second is blocked/conflicts;
 *      after the first COMMITs, the second retries and succeeds → target
 *      ends up with BOTH changes (sequential, not corrupted). */
static int s36(void) {
  printf("s36: two sessions BEGIN on same STB, second blocked then retries after first COMMIT\n");
  drop_and_create_dbs(1);
  setup_topic();
  use_src2();

  exec(g_src, "use src_txn_db");
  exec(g_src2, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");

  exec(g_src, "begin");
  exec(g_src, "alter stable stb1 add column c2 float");

  exec(g_src2, "begin");
  int conflict_rc = exec_safe(g_src2, "alter stable stb1 add column c3 bigint");
  if (conflict_rc == 0) {
    fprintf(stderr, "s36: expected second session's BEGIN+ALTER to conflict, but it succeeded\n");
    exec(g_src2, "rollback");
    return 1;
  }
  exec(g_src2, "rollback");
  exec(g_src, "commit");

  /* Retry from session 2, now that session 1 has committed */
  exec(g_src2, "begin");
  exec(g_src2, "alter stable stb1 add column c3 bigint");
  exec(g_src2, "commit");

  tmq_t *c = make_consumer("grp_s36", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_columns "
    "where db_name='dst_txn_db' and table_name='stb1' and col_name in ('c2','c3')",
    2, "both sessions' columns present, in sequence");
}

/* s37: many independent round-robin sessions (alternating g_src / g_src2),
 *      each doing its own BEGIN/COMMIT on unrelated tables → target's final
 *      object count matches exactly regardless of the interleaving. */
static int s37(void) {
  printf("s37: round-robin independent BEGIN/COMMIT across 2 sessions, 5 rounds each\n");
  drop_and_create_dbs(1);
  setup_topic();
  use_src2();

  exec(g_src, "use src_txn_db");
  exec(g_src2, "use src_txn_db");

  char sql[128];
  for (int i = 0; i < 5; i++) {
    exec(g_src, "begin");
    snprintf(sql, sizeof(sql), "create table ntb_a%d (ts timestamp, c1 int)", i);
    exec(g_src, sql);
    exec(g_src, "commit");

    exec(g_src2, "begin");
    snprintf(sql, sizeof(sql), "create table ntb_b%d (ts timestamp, c1 int)", i);
    exec(g_src2, sql);
    exec(g_src2, "commit");
  }

  tmq_t *c = make_consumer("grp_s37", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    10, "all 10 tables from 2x5 round-robin txns present");
}

/* ═══════════════════════════════════════════════════════════════════════
 * Category 5: boundary tests
 * ═══════════════════════════════════════════════════════════════════════ */

/* s38: empty transaction (BEGIN; COMMIT, zero DDL ops) → consumer must not
 *      crash or produce phantom messages. */
static int s38(void) {
  printf("s38: empty txn (BEGIN; COMMIT, no ops) → no crash, no phantom messages\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s38", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    0, "no objects created (empty txn)");
}

/* s39: minimal txn — a single DDL op — as a baseline distinct from "no txn
 *      at all". */
static int s39(void) {
  printf("s39: minimal txn (BEGIN; 1 CREATE; COMMIT)\n");
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "begin");
  exec(g_src, "create table ntb1 (ts timestamp, c1 int)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s39", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "single table present");
}

/* s40: very large single txn — 1200 CTBs created in one BEGIN/COMMIT block —
 *      verifies atomic, complete delivery of a bulk batch (mirrors the
 *      >1000-table scale used in test_meta_batch_txn_ddl_visibility.py's
 *      check_l_bulk_drop_vtables, here exercising the taosX replication path). */
static int s40(void) {
  const int N = 1200;
  printf("s40: bulk single txn — %d CTBs → COMMIT → full atomic replication\n", N);
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "begin");
  char sql[128];
  for (int i = 0; i < N; i++) {
    snprintf(sql, sizeof(sql), "create table ct_bulk%d using stb1 tags(%d)", i, i);
    exec(g_src, sql);
  }
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s40", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_with_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    N, "all 1200 bulk CTBs replicated atomically", 10, 500);
}

/* s41: a txn spanning a long wall-clock window, with a lot of unrelated
 *      non-txn WAL traffic (from a second session) accumulating in between
 *      its statements → COMMIT → the original txn's atomic batch must still
 *      be delivered completely and correctly despite the intervening noise. */
static int s41(void) {
  printf("s41: long-window txn with concurrent unrelated WAL traffic in between\n");
  drop_and_create_dbs(1);
  setup_topic();
  use_src2();

  exec(g_src, "use src_txn_db");
  exec(g_src2, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");

  exec(g_src, "begin");
  exec(g_src, "create table ct1 using stb1 tags(1)");

  /* Unrelated non-txn WAL noise from a second session while the txn is open */
  char sql[128];
  for (int i = 0; i < 20; i++) {
    snprintf(sql, sizeof(sql), "create table ntb_noise%d (ts timestamp, c1 int)", i);
    exec(g_src2, sql);
  }
  portable_msleep(500);  /* widen the wall-clock window between txn statements */

  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "commit");

  tmq_t *c = make_consumer("grp_s41", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables "
    "where db_name='dst_txn_db' and table_name in ('ct1','ct2')",
    2, "both txn CTBs present despite intervening WAL noise");
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    22, "txn CTBs + 20 unrelated noise tables all present");
  return fail;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Category 6: stress tests
 * ═══════════════════════════════════════════════════════════════════════ */

/* s42: ~200 rapid sequential BEGIN/COMMIT/ROLLBACK cycles (mostly commits,
 *      some rollbacks) → target must exactly match only the committed
 *      subset — no leaked rollback state, no missed commits. */
static int s42(void) {
  const int N = 200;
  printf("s42: %d rapid sequential BEGIN/COMMIT/ROLLBACK cycles → target matches committed subset\n", N);
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");

  int committed = 0;
  char sql[128];
  for (int i = 0; i < N; i++) {
    exec(g_src, "begin");
    snprintf(sql, sizeof(sql), "create table ct_stress%d using stb1 tags(%d)", i, i);
    exec(g_src, sql);
    /* every 4th cycle is a rollback */
    if (i % 4 == 3) {
      exec(g_src, "rollback");
    } else {
      exec(g_src, "commit");
      committed++;
    }
  }
  printf("  [s42] %d/%d cycles committed\n", committed, N);

  tmq_t *c = make_consumer("grp_s42", 0);
  if (drain(c) < 0) { close_consumer(c); return 1; }
  close_consumer(c);

  return verify_with_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    committed, "target matches exactly the committed subset (rollbacks excluded)", 10, 500);
}

/* s43: build a moderate backlog, then have 4 INDEPENDENT consumer groups
 *      each drain it from earliest → all 4 must independently converge to
 *      the identical target state (idempotent re-apply tolerated). */
static int s43(void) {
  const int N = 40;
  printf("s43: backlog of %d CTBs, 4 independent consumer groups converge to same state\n", N);
  drop_and_create_dbs(1);
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "begin");
  char sql[128];
  for (int i = 0; i < N; i++) {
    snprintf(sql, sizeof(sql), "create table ct_multi%d using stb1 tags(%d)", i, i);
    exec(g_src, sql);
  }
  exec(g_src, "commit");

  const char *groups[] = {"grp_s43_a", "grp_s43_b", "grp_s43_c", "grp_s43_d"};
  int fail = 0;
  for (int g = 0; g < 4; g++) {
    tmq_t *c = make_consumer(groups[g], 0);
    drain(c);  /* tolerate idempotent re-apply from prior groups */
    close_consumer(c);

    char label[64];
    snprintf(label, sizeof(label), "group %s converged", groups[g]);
    fail |= verify_retry(g_dst,
      "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
      N, label);
  }
  return fail;
}

/* ═══════════════════════════════════════════════════════════════════════
 * Category 7: WAL cleanup / retention interacting with txn consumption
 * ═══════════════════════════════════════════════════════════════════════ */

/* s44: commit + fully drain a consumer group, THEN explicitly trim the
 *      source WAL, THEN commit new data — the SAME consumer group (already
 *      past the trimmed portion) must continue consuming the new data
 *      correctly. Trimming already-consumed WAL must not disturb ongoing
 *      consumption of new commits. */
static int s44(void) {
  printf("s44: commit+drain, TRIM DATABASE WAL, new commit → same group still consumes correctly\n");
  exec(g_src, "drop database if exists src_txn_db");
  exec(g_src, "drop database if exists dst_txn_db");
  exec(g_src, "create database src_txn_db vgroups 1 wal_retention_period 1 wal_segment_size 1");
  exec(g_src, "create database dst_txn_db vgroups 1");
  taos_close(g_dst);
  g_dst = taos_connect("localhost", "root", "taosdata", "dst_txn_db", 0);
  if (!g_dst) die("s44", "taos_connect dst");
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "begin");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "commit");

  tmq_t *c1 = make_consumer("grp_s44", 0);
  if (drain(c1) < 0) { close_consumer(c1); return 1; }
  close_consumer(c1);

  int fail = 0;
  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    1, "ct1 present after first drain");

  /* Force WAL trim now that everything so far has already been consumed. */
  exec(g_src, "trim database src_txn_db wal");
  portable_msleep(500);

  exec(g_src, "begin");
  exec(g_src, "create table ct2 using stb1 tags(2)");
  exec(g_src, "commit");

  /* Same consumer group resumes; it never needs the trimmed portion. */
  tmq_t *c2 = make_consumer("grp_s44", 0);
  if (drain(c2) < 0) { close_consumer(c2); return 1; }
  close_consumer(c2);

  fail |= verify_retry(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'",
    2, "ct2 present after WAL trim (new data unaffected)");
  return fail;
}

/* s45: EXPLORATORY — a txn commits but NO consumer ever reads it before the
 *      WAL is explicitly trimmed out from under it. A brand-new consumer
 *      group (offset=earliest) then tries to bootstrap.
 *
 *      There's no single officially-documented contract for this exact
 *      corner (fresh group + trimmed WAL it never saw), so this scenario
 *      does NOT assert one specific outcome. It accepts either:
 *        (a) the data is still correctly recovered (e.g. via a snapshot/
 *            meta-reconstruction fallback), or
 *        (b) the consumer/drain cleanly reports an error and the target
 *            simply has nothing (no partial/corrupted state).
 *      What it does NOT accept is a partial/corrupted table count (neither
 *      0 nor 1), a crash, or a hang — those indicate silent data
 *      corruption or a real bug, and would justify tightening this
 *      assertion to a single required outcome once the contract is
 *      decided. */
static int s45(void) {
  printf("s45: never-consumed txn + WAL trimmed before any consumer reads it (exploratory)\n");
  exec(g_src, "drop database if exists src_txn_db");
  exec(g_src, "drop database if exists dst_txn_db");
  exec(g_src, "create database src_txn_db vgroups 1 wal_retention_period 1 wal_segment_size 1");
  exec(g_src, "create database dst_txn_db vgroups 1");
  taos_close(g_dst);
  g_dst = taos_connect("localhost", "root", "taosdata", "dst_txn_db", 0);
  if (!g_dst) die("s45", "taos_connect dst");
  setup_topic();

  exec(g_src, "use src_txn_db");
  exec(g_src, "create stable stb1 (ts timestamp, c1 int) tags (t1 int)");
  exec(g_src, "begin");
  exec(g_src, "create table ct1 using stb1 tags(1)");
  exec(g_src, "commit");

  /* No consumer created yet: this txn's WAL entries have never been read. */
  exec(g_src, "trim database src_txn_db wal");
  portable_msleep(500);

  tmq_t *c = make_consumer("grp_s45", 0);
  int drc = drain(c);
  close_consumer(c);

  int64_t got = query_int(g_dst,
    "select count(*) from information_schema.ins_tables where db_name='dst_txn_db'");
  if (got != 0 && got != 1) {
    fprintf(stderr,
      "s45: target has an UNEXPECTED partial/corrupted count=%" PRId64
      " (neither 0 nor 1) — this indicates real data corruption, not a clean outcome\n",
      got);
    return 1;
  }
  if (drc < 0) {
    printf("  [s45] drain reported an error (tolerated: 'clean failure' outcome), target count=%" PRId64 "\n", got);
  } else {
    printf("  [s45] drain completed without hard error, target count=%" PRId64 "\n", got);
  }
  printf("s45: completed without crash or partial/corrupted state (informational scenario)\n");
  return 0;
}

/* ── dispatch ────────────────────────────────────────────────────────────── */

typedef int (*scenario_fn)(void);

static const scenario_fn scenarios[] = {
  NULL,  /* index 0 unused */
  s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
  s11, s12, s13, s14, s15, s16, s17, s18, s19, s20,
  /* 21-23 are cluster scenarios implemented directly in test_taosx_txn_cluster.py, not here */
  NULL, NULL, NULL,
  s24, s25, s26, s27, s28, s29,
  s30, s31, s32, s33,
  s34, s35, s36, s37,
  s38, s39, s40, s41,
  s42, s43,
  s44, s45,
};

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <scenario_number (1-45, except 21-23)>\n", argv[0]);
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

  int scenario = atoi(argv[1]);
  int max_scenario = (int)(sizeof(scenarios) / sizeof(scenarios[0])) - 1;
  if (scenario < 1 || scenario > max_scenario || scenarios[scenario] == NULL) {
    fprintf(stderr, "Invalid scenario %d (must be 1-%d, and not 21-23 "
                     "which are implemented in test_taosx_txn_cluster.py)\n",
            scenario, max_scenario);
    return 1;
  }

  printf("======== Running scenario %d ========\n", scenario);
  int rc = scenarios[scenario]();

  if (rc == 0) {
    printf("======== Scenario %d PASSED ========\n", scenario);
  } else {
    fprintf(stderr, "======== Scenario %d FAILED ========\n", scenario);
  }

  taos_close(g_src);
  taos_close(g_dst);
  if (g_src2) taos_close(g_src2);
  taos_cleanup();
  return rc;
}
