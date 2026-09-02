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
 * Full-chain end-to-end subscription test for VST inheritance (BASE ON).
 *
 * Verifies the complete TMQ path that connector unit tests cannot reach, across
 * every delivery mode a production consumer actually hits:
 *
 *   Scenario A — realtime DB topic   (snapshot=false, "with meta as database")
 *       Incremental WAL meta path: clientRawBlockJson.c builds the json meta from
 *       SVCreateStbReq/SMAlterStbReq WAL entries.
 *   Scenario B — snapshot DB topic   (snapshot=true,  "with meta as database")
 *       Bootstrap path: metaSnapshot.c getTableInfoFromSnapshot rebuilds create-stb
 *       meta from the persisted SMetaEntry. Exercises a DIFFERENT code path than A,
 *       and verifies parent-before-child emit order so cross-cluster replay resolves.
 *   Scenario C — snapshot STABLE topic ("with meta as stable leaf_child")
 *       Stable-scoped: only the child stable's meta is delivered (parents are not),
 *       so the target must already have the parents. Confirms the child still carries
 *       baseOn/ownColStart/ownTagStart and replays against pre-existing parents.
 *
 *   All scenarios use a MULTI-VGROUP source (vgroups 3) so meta that fans out across
 *   vnodes is handled. Each consumed meta is validated for the frozen contract AND
 *   replayed via tmq_write_raw into a fresh target db; inheritance is then verified
 *   server-side (ins_vstable_inherits + SHOW CREATE ... BASE ON).
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

#define VGROUPS "2"

// --- per-scenario assertion counters over the consumed meta stream ----------
static int g_seen_create_child_baseon = 0;  // create "leaf_child" with baseOn (2 parents)
static int g_seen_create_standalone = 0;    // create "standalone" without baseOn
static int g_seen_alter_add_baseon = 0;     // alterType 22
static int g_seen_alter_drop_baseon = 0;    // alterType 23

static void reset_counters(void) {
  g_seen_create_child_baseon = 0;
  g_seen_create_standalone = 0;
  g_seen_alter_add_baseon = 0;
  g_seen_alter_drop_baseon = 0;
}

// NOTE: this binary is compiled -DNDEBUG (Release), so the standard C assert() is a
// no-op. Every failure path below must therefore exit(1) explicitly — never rely on
// assert() to fail the test, or it will silently "pass".
static void fail(const char* msg, const char* detail) {
  fprintf(stderr, "FAIL: %s : %s\n", msg, detail ? detail : "");
  exit(1);
}

// Assert the cJSON "baseOn" array contains exactly the expected bare parent names.
static void check_baseon_contains(cJSON* meta, const char** expected, int nExpected) {
  cJSON* baseOn = cJSON_GetObjectItem(meta, "baseOn");
  if (baseOn == NULL || !cJSON_IsArray(baseOn)) {
    fail("baseOn missing or not array", cJSON_PrintUnformatted(meta));
  }
  int n = cJSON_GetArraySize(baseOn);
  if (n != nExpected) {
    fprintf(stderr, "baseOn size %d != expected %d, meta=%s\n", n, nExpected, cJSON_PrintUnformatted(meta));
    exit(1);
  }
  for (int e = 0; e < nExpected; ++e) {
    int found = 0;
    for (int i = 0; i < n; ++i) {
      cJSON* item = cJSON_GetArrayItem(baseOn, i);
      if (cJSON_IsString(item) && strcmp(item->valuestring, expected[e]) == 0) {
        found = 1;
        break;
      }
    }
    if (!found) fail("expected parent not in baseOn", expected[e]);
  }
}

// Inspect one parsed meta object and tick the assertion counters.
static void inspect_meta(cJSON* meta) {
  cJSON* type = cJSON_GetObjectItem(meta, "type");
  cJSON* tableName = cJSON_GetObjectItem(meta, "tableName");
  cJSON* tableType = cJSON_GetObjectItem(meta, "tableType");
  if (!cJSON_IsString(type) || !cJSON_IsString(tableName)) return;

  const char* tn = tableName->valuestring;

  if (strcmp(type->valuestring, "create") == 0 && cJSON_IsString(tableType) &&
      strcmp(tableType->valuestring, "super") == 0) {
    if (strcmp(tn, "leaf_child") == 0) {
      // inherits from p_device + p_metric; own cols/tags follow inherited ones
      const char* parents[] = {"p_device", "p_metric"};
      check_baseon_contains(meta, parents, 2);
      cJSON* ocs = cJSON_GetObjectItem(meta, "ownColStart");
      cJSON* ots = cJSON_GetObjectItem(meta, "ownTagStart");
      if (!cJSON_IsNumber(ocs) || !cJSON_IsNumber(ots)) {
        fail("leaf_child missing ownColStart/ownTagStart", cJSON_PrintUnformatted(meta));
      }
      if (ocs->valueint <= 0 || ots->valueint <= 0) {
        fail("leaf_child ownColStart/ownTagStart should be > 0 (inherited cols precede own)",
             cJSON_PrintUnformatted(meta));
      }
      g_seen_create_child_baseon++;
    } else if (strcmp(tn, "standalone") == 0) {
      // non-inherited stable: baseOn key must be ABSENT (old-consumer compat)
      if (cJSON_GetObjectItem(meta, "baseOn") != NULL) {
        fail("standalone must NOT carry baseOn", cJSON_PrintUnformatted(meta));
      }
      g_seen_create_standalone++;
    }
  } else if (strcmp(type->valuestring, "alter") == 0) {
    cJSON* alterType = cJSON_GetObjectItem(meta, "alterType");
    if (!cJSON_IsNumber(alterType)) return;
    if (alterType->valueint == TSDB_ALTER_TABLE_ADD_BASE_ON) {  // 22
      const char* parents[] = {"p_metric"};
      check_baseon_contains(meta, parents, 1);
      g_seen_alter_add_baseon++;
    } else if (alterType->valueint == TSDB_ALTER_TABLE_DROP_BASE_ON) {  // 23
      const char* parents[] = {"p_metric"};
      check_baseon_contains(meta, parents, 1);
      g_seen_alter_drop_baseon++;
    }
  }
}

// Parse a json meta string: either a single object, or a batch-meta envelope
// {"tmq_meta_version":"1.0","metas":[...]}.
static void parse_and_inspect(const char* json) {
  if (json == NULL) return;
  cJSON* root = cJSON_Parse(json);
  if (root == NULL) return;
  cJSON* metas = cJSON_GetObjectItem(root, "metas");
  if (metas != NULL && cJSON_IsArray(metas)) {
    int n = cJSON_GetArraySize(metas);
    for (int i = 0; i < n; ++i) {
      inspect_meta(cJSON_GetArrayItem(metas, i));
    }
  } else {
    inspect_meta(root);
  }
  cJSON_Delete(root);
}

static TAOS* connect_or_die(const char* db) {
  TAOS* pConn = taos_connect("localhost", "root", "taosdata", db, 0);
  if (pConn == NULL) {
    fail("taos_connect failed", db ? db : "(no db)");
  }
  return pConn;
}

static void exec_or_die(TAOS* pConn, const char* sql) {
  TAOS_RES* pRes = taos_query(pConn, sql);
  int32_t code = taos_errno(pRes);
  if (code != 0) {
    fprintf(stderr, "query failed: %s, reason: %s\n", sql, taos_errstr(pRes));
    taos_free_result(pRes);
    exit(1);
  }
  taos_free_result(pRes);
}

// Process one TMQ message: validate json meta and replay it into the target db.
// A fresh connection is opened and closed per message (mirrors tmq_write_raw_test):
// reusing one long-lived connection across many tmq_write_raw calls plus a final
// taos_close races the shared sync-request lifecycle.
static void msg_process(TAOS_RES* msg, const char* dstDb) {
  if (tmq_get_res_type(msg) != TMQ_RES_TABLE_META && tmq_get_res_type(msg) != TMQ_RES_METADATA) {
    return;
  }
  char* result = tmq_get_json_meta(msg);
  if (result != NULL) {
    fprintf(stderr, "meta result: %s\n", result);
    parse_and_inspect(result);
    tmq_free_json_meta(result);
  }

  // Replay the raw meta into the target db (cross-db, simulating cross-cluster).
  tmq_raw_data raw = {0};
  if (tmq_get_raw(msg, &raw) == 0) {
    TAOS* pDst = connect_or_die(dstDb);
    int32_t ret = tmq_write_raw(pDst, raw);
    if (ret != 0) {
      fprintf(stderr, "tmq_write_raw failed (raw_type=%d): %s\n", raw.raw_type, tmq_err2str(ret));
      exit(1);
    }
    tmq_free_raw(raw);
    taos_close(pDst);
  }
}

// Create the parent VSTs + inherited child + standalone (with ALTER add/drop)
// inside the given (already-created, in-use) database connection.
static void build_schema(TAOS* pConn, const char* db) {
  char sql[512];

  snprintf(sql, sizeof(sql),
           "create stable %s.p_device (ts timestamp, status int, temp float) "
           "tags (region int, site binary(32)) virtual 1", db);
  exec_or_die(pConn, sql);
  snprintf(sql, sizeof(sql),
           "create stable %s.p_metric (ts timestamp, val double) tags (unit nchar(8)) virtual 1", db);
  exec_or_die(pConn, sql);

  // Child inheriting from BOTH parents at create time, with its own col + tag.
  snprintf(sql, sizeof(sql),
           "create stable %s.leaf_child (ts timestamp, accuracy int) tags (sensor_id int) "
           "base on %s.p_device, %s.p_metric virtual 1", db, db, db);
  exec_or_die(pConn, sql);

  // Standalone (no inheritance) — must NOT emit baseOn in meta.
  snprintf(sql, sizeof(sql),
           "create stable %s.standalone (ts timestamp, own_col int) tags (own_tag int) virtual 1", db);
  exec_or_die(pConn, sql);

  // ALTER ADD/DROP BASE ON — exercises alterType 22 then 23.
  snprintf(sql, sizeof(sql), "alter stable %s.standalone add base on %s.p_metric", db, db);
  exec_or_die(pConn, sql);
  snprintf(sql, sizeof(sql), "alter stable %s.standalone drop base on %s.p_metric", db, db);
  exec_or_die(pConn, sql);
}

static tmq_t* build_consumer(const char* group, const char* snapshot) {
  tmq_conf_t* conf = tmq_conf_new();
  tmq_conf_set(conf, "group.id", group);
  tmq_conf_set(conf, "client.id", "base_on_app");
  tmq_conf_set(conf, "td.connect.user", "root");
  tmq_conf_set(conf, "td.connect.pass", "taosdata");
  tmq_conf_set(conf, "msg.with.table.name", "true");
  tmq_conf_set(conf, "enable.auto.commit", "true");
  tmq_conf_set(conf, "auto.offset.reset", "earliest");
  tmq_conf_set(conf, "experimental.snapshot.enable", snapshot);  // "true" = bootstrap from snapshot
  tmq_t* tmq = tmq_consumer_new(conf, NULL, 0);
  if (tmq == NULL) fail("tmq_consumer_new returned NULL", NULL);
  tmq_conf_destroy(conf);
  return tmq;
}

static void consume_loop(tmq_t* tmq, const char* topicName, const char* dstDb) {
  tmq_list_t* topics = tmq_list_new();
  tmq_list_append(topics, topicName);
  int32_t code;
  if ((code = tmq_subscribe(tmq, topics))) {
    fprintf(stderr, "%% Failed to subscribe %s: %s\n", topicName, tmq_err2str(code));
    exit(1);
  }
  int32_t empty = 0;
  while (1) {
    TAOS_RES* tmqmessage = tmq_consumer_poll(tmq, 3000);
    if (tmqmessage) {
      empty = 0;
      msg_process(tmqmessage, dstDb);
      taos_free_result(tmqmessage);
    } else {
      // Snapshot bootstrap can need a couple of empty polls before data flows.
      if (++empty >= 3) break;
    }
  }
  code = tmq_consumer_close(tmq);
  if (code) fprintf(stderr, "%% Failed to close consumer: %s\n", tmq_err2str(code));
  tmq_list_destroy(topics);
}

static int64_t scalar_count(TAOS* pConn, const char* sql) {
  TAOS_RES* pRes = taos_query(pConn, sql);
  if (taos_errno(pRes) != 0) {
    fprintf(stderr, "count query failed: %s : %s\n", sql, taos_errstr(pRes));
    taos_free_result(pRes);
    exit(1);
  }
  TAOS_ROW row = taos_fetch_row(pRes);
  int64_t v = (row && row[0]) ? *(int64_t*)row[0] : -1;
  taos_free_result(pRes);
  return v;
}

// Confirm inheritance was reconstructed for leaf_child in the target db.
static void verify_child_inherits(TAOS* pConn, const char* db) {
  char sql[256];
  snprintf(sql, sizeof(sql),
           "select count(*) from information_schema.ins_vstable_inherits "
           "where db_name='%s' and child_stable_name='leaf_child'", db);
  int64_t cnt = scalar_count(pConn, sql);
  if (cnt != 2) {
    fprintf(stderr, "[%s] leaf_child inherit rows = %lld, expected 2\n", db, (long long)cnt);
    exit(1);
  }

  snprintf(sql, sizeof(sql), "show create stable %s.leaf_child", db);
  TAOS_RES* pRes = taos_query(pConn, sql);
  if (taos_errno(pRes) != 0) fail("show create stable failed", taos_errstr(pRes));
  TAOS_ROW row = taos_fetch_row(pRes);
  if (row == NULL) fail("show create stable returned no row", db);
  const char* createStmt = (const char*)row[1];
  if (createStmt == NULL || strstr(createStmt, "BASE ON") == NULL) {
    fail("target SHOW CREATE leaf_child missing BASE ON", createStmt);
  }
  fprintf(stderr, "[%s] SHOW CREATE leaf_child: %s\n", db, createStmt);
  taos_free_result(pRes);
}

// ---------------------------------------------------------------------------
// Scenario A: realtime DB topic (incremental WAL meta path).
// ---------------------------------------------------------------------------
static void scenario_realtime_db(TAOS* pAdmin) {
  fprintf(stderr, "\n==== Scenario A: realtime DB topic (snapshot=false) ====\n");
  reset_counters();

  exec_or_die(pAdmin, "drop topic if exists topic_rt_db");
  exec_or_die(pAdmin, "drop database if exists baseon_rt_src");
  exec_or_die(pAdmin, "drop database if exists baseon_rt_dst");
  exec_or_die(pAdmin, "create database baseon_rt_src vgroups " VGROUPS " wal_retention_period 3600");
  exec_or_die(pAdmin, "create database baseon_rt_dst vgroups " VGROUPS " wal_retention_period 3600");

  exec_or_die(pAdmin, "create topic topic_rt_db with meta as database baseon_rt_src");
  build_schema(pAdmin, "baseon_rt_src");

  tmq_t* tmq = build_consumer("g_rt_db", "false");
  consume_loop(tmq, "topic_rt_db", "baseon_rt_dst");

  if (g_seen_create_child_baseon < 1) fail("A: never saw create leaf_child with baseOn", NULL);
  if (g_seen_create_standalone < 1) fail("A: never saw create standalone (no baseOn)", NULL);
  if (g_seen_alter_add_baseon < 1) fail("A: never saw alter ADD BASE ON (22)", NULL);
  if (g_seen_alter_drop_baseon < 1) fail("A: never saw alter DROP BASE ON (23)", NULL);
  verify_child_inherits(pAdmin, "baseon_rt_dst");
  fprintf(stderr, "==== Scenario A PASSED ====\n");
}

// ---------------------------------------------------------------------------
// Scenario B: snapshot DB topic (bootstrap from persisted SMetaEntry).
// Schema is built BEFORE the topic, then flushed so it lives only in the
// snapshot (not the live WAL tail) — forcing the metaSnapshot.c path.
// ---------------------------------------------------------------------------
static void scenario_snapshot_db(TAOS* pAdmin) {
  fprintf(stderr, "\n==== Scenario B: snapshot DB topic (snapshot=true) ====\n");
  reset_counters();

  exec_or_die(pAdmin, "drop topic if exists topic_snap_db");
  exec_or_die(pAdmin, "drop database if exists baseon_snap_src");
  exec_or_die(pAdmin, "drop database if exists baseon_snap_dst");
  exec_or_die(pAdmin, "create database baseon_snap_src vgroups " VGROUPS " wal_retention_period 3600");
  exec_or_die(pAdmin, "create database baseon_snap_dst vgroups " VGROUPS " wal_retention_period 3600");

  build_schema(pAdmin, "baseon_snap_src");
  exec_or_die(pAdmin, "flush database baseon_snap_src");  // push meta into the snapshot
  exec_or_die(pAdmin, "create topic topic_snap_db with meta as database baseon_snap_src");

  tmq_t* tmq = build_consumer("g_snap_db", "true");
  consume_loop(tmq, "topic_snap_db", "baseon_snap_dst");

  if (g_seen_create_child_baseon < 1) fail("B: snapshot never delivered leaf_child with baseOn", NULL);
  if (g_seen_create_standalone < 1) fail("B: snapshot never delivered standalone", NULL);
  verify_child_inherits(pAdmin, "baseon_snap_dst");
  fprintf(stderr, "==== Scenario B PASSED ====\n");
}

// ---------------------------------------------------------------------------
// Scenario C: snapshot STABLE topic. Only leaf_child's meta is delivered, so
// the target must already have the parent stables. Verify the child still
// carries baseOn and replays correctly against pre-existing parents.
// ---------------------------------------------------------------------------
static void scenario_snapshot_stable(TAOS* pAdmin) {
  fprintf(stderr, "\n==== Scenario C: snapshot STABLE topic (scoped) ====\n");
  reset_counters();

  exec_or_die(pAdmin, "drop topic if exists topic_snap_stb");
  exec_or_die(pAdmin, "drop database if exists baseon_stb_src");
  exec_or_die(pAdmin, "drop database if exists baseon_stb_dst");
  exec_or_die(pAdmin, "create database baseon_stb_src vgroups " VGROUPS " wal_retention_period 3600");
  exec_or_die(pAdmin, "create database baseon_stb_dst vgroups " VGROUPS " wal_retention_period 3600");

  build_schema(pAdmin, "baseon_stb_src");
  exec_or_die(pAdmin, "flush database baseon_stb_src");
  exec_or_die(pAdmin, "create topic topic_snap_stb with meta as stable baseon_stb_src.leaf_child");

  // Stable-scoped topic does NOT carry the parents; pre-create them in the target
  // so the replayed child's BASE ON can resolve.
  exec_or_die(pAdmin,
              "create stable baseon_stb_dst.p_device (ts timestamp, status int, temp float) "
              "tags (region int, site binary(32)) virtual 1");
  exec_or_die(pAdmin,
              "create stable baseon_stb_dst.p_metric (ts timestamp, val double) "
              "tags (unit nchar(8)) virtual 1");

  tmq_t* tmq = build_consumer("g_snap_stb", "true");
  consume_loop(tmq, "topic_snap_stb", "baseon_stb_dst");

  if (g_seen_create_child_baseon < 1) fail("C: stable topic never delivered leaf_child with baseOn", NULL);
  verify_child_inherits(pAdmin, "baseon_stb_dst");
  fprintf(stderr, "==== Scenario C PASSED ====\n");
}

// ---------------------------------------------------------------------------
// Scenario D: negative (replay into a parent-less target must FAIL, not corrupt)
// + data-plane (a VCT on the leaf carries inherited-column data, proving the
// inherited schema is functionally complete, not just a DDL shell).
// ---------------------------------------------------------------------------
static int g_neg_replay_rejected = 0;

// Replay leaf_child's create into a target that has NO parents. The create-stb
// replay re-resolves parents by name on the target mnode, so it MUST fail rather
// than silently create a broken inheritance.
static void msg_process_expect_reject(TAOS_RES* msg, TAOS* pDst) {
  if (tmq_get_res_type(msg) != TMQ_RES_TABLE_META && tmq_get_res_type(msg) != TMQ_RES_METADATA) {
    return;
  }
  char* result = tmq_get_json_meta(msg);
  int isLeafCreate = 0;
  if (result != NULL) {
    if (strstr(result, "\"tableName\":\"leaf_child\"") && strstr(result, "\"baseOn\"")) {
      isLeafCreate = 1;
    }
    tmq_free_json_meta(result);
  }
  tmq_raw_data raw = {0};
  if (tmq_get_raw(msg, &raw) == 0) {
    int32_t ret = tmq_write_raw(pDst, raw);
    if (isLeafCreate && ret != 0) {
      fprintf(stderr, "negative: leaf_child replay correctly rejected: %s\n", tmq_err2str(ret));
      g_neg_replay_rejected = 1;
    }
    tmq_free_raw(raw);
  }
}

static void scenario_negative_and_dataplane(TAOS* pAdmin) {
  fprintf(stderr, "\n==== Scenario D: negative replay + data-plane ====\n");
  reset_counters();
  g_neg_replay_rejected = 0;

  exec_or_die(pAdmin, "drop topic if exists topic_neg");
  exec_or_die(pAdmin, "drop database if exists baseon_neg_src");
  exec_or_die(pAdmin, "drop database if exists baseon_neg_dst");
  exec_or_die(pAdmin, "create database baseon_neg_src vgroups " VGROUPS " wal_retention_period 3600");
  // Target deliberately has NO parent stables. A STABLE-scoped topic delivers only
  // leaf_child (not its parents), so its replay must fail here — unlike a db topic,
  // which would carry the parents in the same stream.
  exec_or_die(pAdmin, "create database baseon_neg_dst vgroups 1 wal_retention_period 3600");

  build_schema(pAdmin, "baseon_neg_src");
  exec_or_die(pAdmin, "flush database baseon_neg_src");
  exec_or_die(pAdmin, "create topic topic_neg with meta as stable baseon_neg_src.leaf_child");

  // --- Negative: replay leaf_child create into the parent-less target. ---
  TAOS* pDst = connect_or_die("baseon_neg_dst");
  tmq_t* tmq = build_consumer("g_neg", "true");
  {
    tmq_list_t* topics = tmq_list_new();
    tmq_list_append(topics, "topic_neg");
    if (tmq_subscribe(tmq, topics)) fail("D: subscribe topic_neg failed", NULL);
    int empty = 0;
    while (1) {
      TAOS_RES* m = tmq_consumer_poll(tmq, 3000);
      if (m) { empty = 0; msg_process_expect_reject(m, pDst); taos_free_result(m); }
      else if (++empty >= 3) break;
    }
    tmq_consumer_close(tmq);
    tmq_list_destroy(topics);
  }
  taos_close(pDst);
  if (!g_neg_replay_rejected) {
    fail("D: leaf_child replay into parent-less target should have been rejected", NULL);
  }

  // --- Data-plane: real source data + a VCT on the leaf; confirm an INHERITED
  // column carries the referenced data (not just present in the schema). ---
  exec_or_die(pAdmin,
              "create table baseon_neg_src.src_tb (ts timestamp, m1 int, m2 float, m3 int)");
  exec_or_die(pAdmin, "insert into baseon_neg_src.src_tb values (now, 11, 1.5, 22)");
  exec_or_die(pAdmin, "insert into baseon_neg_src.src_tb values (now+1s, 33, 3.5, 44)");
  // VCT on leaf_child mapping inherited parent col (status, from p_device) +
  // own col (accuracy). 'status' is an inherited column, so non-null data through
  // it proves the merged inherited schema is functional. leaf_child's merged tag
  // set is [region, site, unit (inherited)] + [sensor_id (own)] = 4 tags.
  exec_or_die(pAdmin,
              "create vtable baseon_neg_src.vct_leaf "
              "(status from baseon_neg_src.src_tb.m1, "
              " temp from baseon_neg_src.src_tb.m2, "
              " accuracy from baseon_neg_src.src_tb.m3) "
              "using baseon_neg_src.leaf_child tags (1, 'beijing', 'kpa', 7)");

  int64_t rows = scalar_count(pAdmin, "select count(*) from baseon_neg_src.leaf_child");
  if (rows != 2) {
    fprintf(stderr, "D: leaf_child data rows = %lld, expected 2\n", (long long)rows);
    exit(1);
  }
  int64_t inheritedNonNull =
      scalar_count(pAdmin, "select count(status) from baseon_neg_src.leaf_child");
  if (inheritedNonNull != 2) {
    fprintf(stderr, "D: inherited column 'status' non-null rows = %lld, expected 2\n",
            (long long)inheritedNonNull);
    exit(1);
  }
  fprintf(stderr, "==== Scenario D PASSED (negative rejected; inherited column carries data) ====\n");
}

int main(int argc, char* argv[]) {
  TAOS* pAdmin = connect_or_die(NULL);

  scenario_realtime_db(pAdmin);
  scenario_snapshot_db(pAdmin);
  scenario_snapshot_stable(pAdmin);
  scenario_negative_and_dataplane(pAdmin);

  taos_close(pAdmin);
  printf("tmq_base_on_test: ALL SCENARIOS PASSED "
         "(realtime-db, snapshot-db, snapshot-stable, negative+dataplane; vgroups=" VGROUPS ")\n");
  return 0;
}
