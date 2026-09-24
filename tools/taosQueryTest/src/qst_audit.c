/* Read-only result-store aggregation for endurance evidence. */

#define _POSIX_C_SOURCE 200809L

#include "qst_audit.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "qst_native.h"
#include "qst_result_store.h"
#include "toolscJson.h"

enum {
  QST_AUDIT_JSON_BYTES = 1024U * 1024U,
  QST_AUDIT_TARGET_TD_BIT = 1U << 0,
  QST_AUDIT_TARGET_MYSQL_BIT = 1U << 1,
  QST_AUDIT_TARGET_PG_BIT = 1U << 2,
  QST_AUDIT_TARGET_INFLUX_BIT = 1U << 3,
  QST_AUDIT_TARGET_ALL_BITS = QST_AUDIT_TARGET_TD_BIT |
                             QST_AUDIT_TARGET_MYSQL_BIT |
                             QST_AUDIT_TARGET_PG_BIT |
                             QST_AUDIT_TARGET_INFLUX_BIT,
  QST_AUDIT_SQL_FINGERPRINT_LIMIT = 20U,
  QST_AUDIT_SQL_SAMPLE_LIMIT = 20U,
};

typedef struct {
  const char *name;
  uint16_t bit;
} SQstAuditShape;

static const SQstAuditShape kAuditShapes[] = {
    {"explain", 1U << 0},
    {"select", 1U << 1},
    {"from", 1U << 2},
    {"where", 1U << 3},
    {"group_by", 1U << 4},
    {"having", 1U << 5},
    {"order_by", 1U << 6},
    {"limit", 1U << 7},
    {"join", 1U << 8},
    {"subquery", 1U << 9},
    {"aggregate", 1U << 10},
    {"window", 1U << 11},
    {"external", 1U << 12},
};

static int32_t qstAuditParseUint64(const char *text, uint64_t *out_value) {
  char *end = NULL;
  unsigned long long value = 0;

  if (NULL == text || '\0' == text[0] || NULL == out_value || '-' == text[0]) {
    return 0;
  }
  errno = 0;
  value = strtoull(text, &end, 10);
  if (errno != 0 || end == text || '\0' != *end || value == 0) {
    return 0;
  }
  *out_value = (uint64_t)value;
  return 1;
}

static int32_t qstAuditParseTargets(const char *text, uint32_t *out_bits) {
  char copy[64] = {0};
  char *save = NULL;
  char *token = NULL;
  uint32_t bits = 0;

  if (NULL == text || NULL == out_bits || '\0' == text[0] ||
      strlen(text) >= sizeof(copy) || NULL != strstr(text, ",,")) {
    return 0;
  }
  memcpy(copy, text, strlen(text) + 1U);
  for (token = strtok_r(copy, ",", &save); NULL != token;
       token = strtok_r(NULL, ",", &save)) {
    uint32_t bit = 0;

    if (0 == strcmp(token, "td")) {
      bit = QST_AUDIT_TARGET_TD_BIT;
    } else if (0 == strcmp(token, "mysql")) {
      bit = QST_AUDIT_TARGET_MYSQL_BIT;
    } else if (0 == strcmp(token, "pg")) {
      bit = QST_AUDIT_TARGET_PG_BIT;
    } else if (0 == strcmp(token, "influx")) {
      bit = QST_AUDIT_TARGET_INFLUX_BIT;
    } else {
      return 0;
    }
    if ((bits & bit) != 0) {
      return 0;
    }
    bits |= bit;
  }
  if (bits == 0 || (bits & ~QST_AUDIT_TARGET_ALL_BITS) != 0) {
    return 0;
  }
  *out_bits = bits;
  return 1;
}

static const char *qstAuditTargetName(uint32_t target_bit) {
  switch (target_bit) {
    case QST_AUDIT_TARGET_TD_BIT:
      return "td";
    case QST_AUDIT_TARGET_MYSQL_BIT:
      return "mysql";
    case QST_AUDIT_TARGET_PG_BIT:
      return "pg";
    case QST_AUDIT_TARGET_INFLUX_BIT:
      return "influx";
    default:
      return NULL;
  }
}

static const char *qstAuditTargetSourcePrefix(const char *target_name) {
  if (target_name == NULL) {
    return NULL;
  }
  if (0 == strcmp(target_name, "mysql")) {
    return "mysql";
  }
  if (0 == strcmp(target_name, "pg")) {
    return "pg";
  }
  if (0 == strcmp(target_name, "influx")) {
    return "influx";
  }
  return NULL;
}

static EQstCode qstAuditAddQuery(tools_cJSON *object, const char *name,
                                 SQstResultStore *store, const char *sql,
                                 int32_t *out_failed) {
  SQstQueryResult result = {0};
  tools_cJSON *entry = NULL;
  tools_cJSON *raw = NULL;
  EQstCode code = QST_OK;

  if (NULL == object || NULL == name || NULL == store ||
      NULL == store->native_taos || NULL == sql || NULL == out_failed) {
    return QST_E_INVALID_ARGUMENT;
  }
  entry = tools_cJSON_CreateObject();
  if (NULL == entry) {
    return QST_E_RESOURCE;
  }
  code = qst_native_query_result((SQstNativeClient *)store->native_taos, sql,
                                 &result);
  if (code == QST_OK && (NULL == result.data || result.bytes == 0)) {
    code = QST_E_RESULT_STORE;
  }
  tools_cJSON_AddBoolToObject(entry, "ok", code == QST_OK);
  if (code == QST_OK) {
    raw = tools_cJSON_CreateRaw((const char *)result.data);
    if (NULL == raw) {
      tools_cJSON_Delete(entry);
      qst_result_reset(&result);
      return QST_E_RESOURCE;
    }
    tools_cJSON_AddItemToObject(entry, "result", raw);
  } else {
    char code_text[32] = {0};

    (void)snprintf(code_text, sizeof(code_text), "%d", code);
    tools_cJSON_AddStringToObject(entry, "error_code", code_text);
    (void)fprintf(stderr, "taosQueryTest: audit query failed name=%s code=%d\n",
                  name, code);
    *out_failed = 1;
  }
  tools_cJSON_AddItemToObject(object, name, entry);
  qst_result_reset(&result);
  return QST_OK;
}

static int32_t qstAuditColumnIndex(const tools_cJSON *columns, const char *name,
                                   int32_t *out_index) {
  if (columns == NULL || name == NULL || out_index == NULL ||
      !tools_cJSON_IsArray(columns)) {
    return 0;
  }
  for (int32_t index = 0; index < tools_cJSON_GetArraySize(columns); ++index) {
    const tools_cJSON *column = tools_cJSON_GetArrayItem(columns, index);
    const tools_cJSON *column_name =
        column == NULL ? NULL : tools_cJSON_GetObjectItemCaseSensitive(column, "name");
    if (column_name != NULL && tools_cJSON_IsString(column_name) &&
        column_name->valuestring != NULL &&
        strcmp(column_name->valuestring, name) == 0) {
      *out_index = index;
      return 1;
    }
  }
  return 0;
}

static int32_t qstAuditReadInt64(const tools_cJSON *cell, int64_t *out_value,
                                 int32_t *out_is_null) {
  const tools_cJSON *value = NULL;
  char *end = NULL;
  long long parsed = 0;

  if (cell == NULL || out_value == NULL || out_is_null == NULL) {
    return 0;
  }
  *out_value = 0;
  *out_is_null = tools_cJSON_IsNull(cell);
  if (*out_is_null != 0) {
    return 1;
  }
  value = tools_cJSON_GetObjectItemCaseSensitive(cell, "value");
  if (value == NULL || !tools_cJSON_IsString(value) || value->valuestring == NULL ||
      value->valuestring[0] == '\0') {
    return 0;
  }
  errno = 0;
  parsed = strtoll(value->valuestring, &end, 10);
  if (errno != 0 || end == value->valuestring || *end != '\0' ||
      parsed < INT64_MIN || parsed > INT64_MAX) {
    return 0;
  }
  *out_value = (int64_t)parsed;
  return 1;
}

static EQstCode qstAuditAddSyntheticCount(tools_cJSON *shapes, const char *name,
                                          uint64_t count) {
  tools_cJSON *entry = NULL;
  tools_cJSON *result = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *column = NULL;
  tools_cJSON *rows = NULL;
  tools_cJSON *row = NULL;
  tools_cJSON *cell = NULL;
  char count_text[32] = {0};

  if (shapes == NULL || name == NULL ||
      snprintf(count_text, sizeof(count_text), "%llu",
               (unsigned long long)count) < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  entry = tools_cJSON_CreateObject();
  result = tools_cJSON_CreateObject();
  columns = tools_cJSON_CreateArray();
  column = tools_cJSON_CreateObject();
  rows = tools_cJSON_CreateArray();
  row = tools_cJSON_CreateArray();
  cell = tools_cJSON_CreateObject();
  if (entry == NULL || result == NULL || columns == NULL || column == NULL ||
      rows == NULL || row == NULL || cell == NULL) {
    tools_cJSON_Delete(entry);
    tools_cJSON_Delete(result);
    tools_cJSON_Delete(columns);
    tools_cJSON_Delete(column);
    tools_cJSON_Delete(rows);
    tools_cJSON_Delete(row);
    tools_cJSON_Delete(cell);
    return QST_E_RESOURCE;
  }
  tools_cJSON_AddBoolToObject(entry, "ok", 1);
  tools_cJSON_AddStringToObject(result, "format", "query-result-v1");
  tools_cJSON_AddStringToObject(column, "name", "count(*)");
  tools_cJSON_AddStringToObject(column, "type", "int64");
  tools_cJSON_AddNumberToObject(column, "bytes", 8);
  tools_cJSON_AddItemToArray(columns, column);
  tools_cJSON_AddStringToObject(cell, "kind", "int64");
  tools_cJSON_AddStringToObject(cell, "value", count_text);
  tools_cJSON_AddItemToArray(row, cell);
  tools_cJSON_AddItemToArray(rows, row);
  tools_cJSON_AddItemToObject(result, "columns", columns);
  tools_cJSON_AddItemToObject(result, "rows", rows);
  tools_cJSON_AddItemToObject(entry, "result", result);
  tools_cJSON_AddItemToObject(shapes, name, entry);
  return QST_OK;
}

static void qstAuditAddFailedShape(tools_cJSON *shapes, const char *name,
                                   EQstCode code) {
  tools_cJSON *entry = tools_cJSON_CreateObject();
  char code_text[32] = {0};

  if (entry == NULL ||
      snprintf(code_text, sizeof(code_text), "%d", code) < 0) {
    tools_cJSON_Delete(entry);
    return;
  }
  tools_cJSON_AddBoolToObject(entry, "ok", 0);
  tools_cJSON_AddStringToObject(entry, "error_code", code_text);
  tools_cJSON_AddItemToObject(shapes, name, entry);
}

static EQstCode qstAuditAddSqlShapes(tools_cJSON *shapes, SQstResultStore *store,
                                     const char *target_name,
                                     int32_t *out_failed) {
  SQstQueryResult result = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *rows = NULL;
  int32_t mask_index = -1;
  int32_t count_index = -1;
  uint64_t counts[sizeof(kAuditShapes) / sizeof(kAuditShapes[0])] = {0};
  EQstCode code = QST_OK;

  if (shapes == NULL || store == NULL || target_name == NULL || out_failed == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  {
    char sql[512] = {0};
    if (snprintf(sql, sizeof(sql),
                 "SELECT sql_shape_mask,COUNT(*) FROM res_db.statements "
                 "WHERE run_id=%llu AND target='%s' GROUP BY sql_shape_mask",
                 (unsigned long long)store->run_id, target_name) < 0 ||
        strlen(sql) >= sizeof(sql)) {
      return QST_E_RESOURCE;
    }
    code = qst_native_query_result((SQstNativeClient *)store->native_taos, sql,
                                   &result);
  }
  if (code != QST_OK || result.data == NULL || result.bytes == 0) {
    *out_failed = 1;
    for (uint32_t index = 0; index < sizeof(kAuditShapes) / sizeof(kAuditShapes[0]);
         ++index) {
      qstAuditAddFailedShape(shapes, kAuditShapes[index].name,
                             code == QST_OK ? QST_E_RESULT_STORE : code);
    }
    qst_result_reset(&result);
    return QST_OK;
  }
  root = tools_cJSON_ParseWithOpts((const char *)result.data, NULL, 1);
  columns = tools_cJSON_GetObjectItemCaseSensitive(root, "columns");
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  if (!tools_cJSON_IsObject(root) || !qstAuditColumnIndex(columns, "sql_shape_mask",
                                                           &mask_index) ||
      !qstAuditColumnIndex(columns, "count(*)", &count_index) ||
      !tools_cJSON_IsArray(rows)) {
    *out_failed = 1;
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  for (int32_t row_index = 0; row_index < tools_cJSON_GetArraySize(rows); ++row_index) {
    const tools_cJSON *row = tools_cJSON_GetArrayItem(rows, row_index);
    int64_t mask = 0;
    int64_t count = 0;
    int32_t is_null = 0;
    if (row == NULL || !tools_cJSON_IsArray(row) ||
        !qstAuditReadInt64(tools_cJSON_GetArrayItem(row, mask_index), &mask,
                           &is_null) ||
        !qstAuditReadInt64(tools_cJSON_GetArrayItem(row, count_index), &count,
                           &(int32_t){0}) ||
        (is_null == 0 && (mask < 0 || mask > UINT16_MAX)) || count <= 0) {
      *out_failed = 1;
      code = QST_E_RESULT_STORE;
      goto _return;
    }
    if (is_null != 0) {
      continue;
    }
    for (uint32_t shape_index = 0;
         shape_index < sizeof(kAuditShapes) / sizeof(kAuditShapes[0]); ++shape_index) {
      if ((mask & kAuditShapes[shape_index].bit) != 0) {
        if (counts[shape_index] > UINT64_MAX - (uint64_t)count) {
          *out_failed = 1;
          code = QST_E_RESULT_STORE;
          goto _return;
        }
        counts[shape_index] += (uint64_t)count;
      }
    }
  }
  for (uint32_t index = 0; index < sizeof(kAuditShapes) / sizeof(kAuditShapes[0]);
       ++index) {
    code = qstAuditAddSyntheticCount(shapes, kAuditShapes[index].name,
                                     counts[index]);
    if (code != QST_OK) {
      *out_failed = 1;
      goto _return;
    }
  }
_return:
  if (code != QST_OK) {
    for (uint32_t index = 0; index < sizeof(kAuditShapes) / sizeof(kAuditShapes[0]);
         ++index) {
      if (tools_cJSON_GetObjectItemCaseSensitive(shapes, kAuditShapes[index].name) ==
          NULL) {
        qstAuditAddFailedShape(shapes, kAuditShapes[index].name, code);
      }
    }
  }
  tools_cJSON_Delete(root);
  qst_result_reset(&result);
  return QST_OK;
}

static EQstCode qstAuditAddTarget(tools_cJSON *targets, SQstResultStore *store,
                                  const char *target_name, int32_t *out_failed) {
  tools_cJSON *target = NULL;
  tools_cJSON *shapes = NULL;
  const char *source_prefix = NULL;
  char sql[1024] = {0};

  if (NULL == targets || NULL == store || NULL == target_name ||
      NULL == out_failed) {
    return QST_E_INVALID_ARGUMENT;
  }
  target = tools_cJSON_CreateObject();
  shapes = tools_cJSON_CreateObject();
  if (NULL == target || NULL == shapes) {
    tools_cJSON_Delete(target);
    tools_cJSON_Delete(shapes);
    return QST_E_RESOURCE;
  }
  source_prefix = qstAuditTargetSourcePrefix(target_name);
  if (snprintf(sql, sizeof(sql),
               "SELECT COUNT(*) FROM res_db.statements WHERE run_id=%llu "
               "AND target='%s'",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "statement_count", store, sql, out_failed) != QST_OK ||
      (source_prefix != NULL &&
       (snprintf(sql, sizeof(sql),
                 "SELECT COUNT(*) FROM res_db.statements WHERE run_id=%llu "
                 "AND target='%s' AND sql_text LIKE '%%src_fq_%s_%%'",
                 (unsigned long long)store->run_id, target_name, source_prefix) < 0 ||
        strlen(sql) >= sizeof(sql) ||
        qstAuditAddQuery(target, "source_reference_count", store, sql,
                         out_failed) != QST_OK)) ||
      snprintf(sql, sizeof(sql),
               "SELECT COUNT(*) FROM res_db.results WHERE run_id=%llu "
               "AND target='%s'",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "result_count", store, sql, out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT COUNT(*) FROM (SELECT sql_sha256 FROM res_db.statements "
               "WHERE run_id=%llu AND target='%s' GROUP BY sql_sha256)",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "unique_sql_sha256_count", store, sql, out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT sql_sha256,COUNT(*) FROM res_db.statements WHERE run_id=%llu "
               "AND target='%s' GROUP BY sql_sha256 ORDER BY COUNT(*) DESC LIMIT %u",
               (unsigned long long)store->run_id, target_name,
               QST_AUDIT_SQL_FINGERPRINT_LIMIT) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "sql_fingerprint_top", store, sql,
                       out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT sql_sha256,sql_text FROM res_db.statements WHERE run_id=%llu "
               "AND target='%s' ORDER BY ts LIMIT %u",
               (unsigned long long)store->run_id, target_name,
               QST_AUDIT_SQL_SAMPLE_LIMIT) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "sql_samples", store, sql,
                       out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT execution_status,COUNT(*) FROM res_db.statements "
               "WHERE run_id=%llu AND target='%s' GROUP BY execution_status",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "execution_status", store, sql, out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT dialect,generation_abnormal,generation_unsupported_fallback,"
               "execution_status,COUNT(*) FROM res_db.statements WHERE run_id=%llu "
               "AND target='%s' GROUP BY dialect,generation_abnormal,"
               "generation_unsupported_fallback,execution_status",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "generation_metadata", store, sql,
                       out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT error_code,COUNT(*) FROM res_db.statements WHERE run_id=%llu "
               "AND target='%s' AND error_code IS NOT NULL GROUP BY error_code "
               "ORDER BY COUNT(*) DESC LIMIT 20",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "error_code_top", store, sql, out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT verdict,COUNT(*) FROM res_db.results WHERE run_id=%llu "
               "AND target='%s' GROUP BY verdict",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "verdict", store, sql, out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT validator,COUNT(*) FROM res_db.results WHERE run_id=%llu "
               "AND target='%s' GROUP BY validator ORDER BY COUNT(*) DESC LIMIT 20",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "validator", store, sql, out_failed) != QST_OK ||
      snprintf(sql, sizeof(sql),
               "SELECT result_storage_status,COUNT(*) FROM res_db.results "
               "WHERE run_id=%llu AND target='%s' GROUP BY result_storage_status",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(sql) >= sizeof(sql) ||
      qstAuditAddQuery(target, "result_storage_status", store, sql, out_failed) != QST_OK) {
    tools_cJSON_Delete(target);
    tools_cJSON_Delete(shapes);
    return QST_E_RESOURCE;
  }
  if (qstAuditAddSqlShapes(shapes, store, target_name, out_failed) != QST_OK) {
    tools_cJSON_Delete(target);
    tools_cJSON_Delete(shapes);
    return QST_E_RESOURCE;
  }
  tools_cJSON_AddItemToObject(target, "sql_shape", shapes);
  tools_cJSON_AddItemToObject(targets, target_name, target);
  return QST_OK;
}

EQstCode qst_audit_run_cli(int32_t argc, char *const argv[]) {
  SQstResultStoreConfig config = {0};
  SQstResultStore store = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *targets = NULL;
  char *printed = NULL;
  char run_id_text[32] = {0};
  uint64_t run_id = 0;
  uint32_t target_bits = 0;
  int32_t query_failed = 0;
  EQstCode code = QST_OK;

  if (argc != 5 || NULL == argv || 0 != strcmp(argv[1], "--audit-run") ||
      !qstAuditParseUint64(argv[2], &run_id) ||
      0 != strcmp(argv[3], "--audit-targets") ||
      !qstAuditParseTargets(argv[4], &target_bits)) {
    (void)fprintf(stderr,
                  "Usage: %s --audit-run RUN_ID --audit-targets td[,mysql,pg,influx]\n",
                  argc > 0 && NULL != argv[0] ? argv[0] : "taosQueryTest");
    return QST_E_INVALID_ARGUMENT;
  }
  config.run_id = run_id;
  config.endpoint = getenv("QST_RESULT_STORE_ENDPOINT");
  config.config_dir = getenv("QST_RESULT_STORE_CONFIG_DIR");
  config.user = getenv("QST_RESULT_STORE_USER");
  config.password = getenv("QST_RESULT_STORE_PASSWORD");
  code = qst_result_store_open(&config, &store);
  if (code != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: audit result-store open failed code=%d\n", code);
    return code;
  }
  root = tools_cJSON_CreateObject();
  targets = tools_cJSON_CreateObject();
  if (NULL == root || NULL == targets) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  if (snprintf(run_id_text, sizeof(run_id_text), "%llu",
               (unsigned long long)run_id) < 0 ||
      strlen(run_id_text) >= sizeof(run_id_text)) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  tools_cJSON_AddStringToObject(root, "run_id", run_id_text);
  tools_cJSON_AddBoolToObject(root, "available", 1);
  {
    char sql[512] = {0};

    (void)snprintf(sql, sizeof(sql),
                   "SELECT COUNT(*) FROM res_db.runs WHERE run_id=%llu",
                   (unsigned long long)run_id);
    code = qstAuditAddQuery(root, "run_record_count", &store, sql, &query_failed);
    if (code != QST_OK) {
      goto _return;
    }
  }
  for (uint32_t bit = QST_AUDIT_TARGET_TD_BIT; bit <= QST_AUDIT_TARGET_INFLUX_BIT;
       bit <<= 1U) {
    if ((target_bits & bit) == 0) {
      continue;
    }
    code = qstAuditAddTarget(targets, &store, qstAuditTargetName(bit),
                              &query_failed);
    if (code != QST_OK) {
      goto _return;
    }
  }
  tools_cJSON_AddItemToObject(root, "targets", targets);
  targets = NULL;
  tools_cJSON_AddBoolToObject(root, "complete", query_failed == 0);
  printed = tools_cJSON_PrintUnformatted(root);
  if (NULL == printed) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  if (strlen(printed) >= QST_AUDIT_JSON_BYTES || fprintf(stdout, "%s\n", printed) < 0) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  code = query_failed == 0 ? QST_OK : QST_E_RESULT_STORE;

_return:
  tools_cJSON_free(printed);
  tools_cJSON_Delete(targets);
  tools_cJSON_Delete(root);
  if (qst_result_store_close(&store) != QST_OK && code == QST_OK) {
    code = QST_E_RESULT_STORE;
  }
  return code;
}

EQstCode qst_audit_reset_result_store_cli(int32_t argc, char *const argv[]) {
  SQstResultStoreConfig config = {0};
  SQstResultStore store = {0};
  int64_t run_count = 0;
  uint64_t run_id = 0;
  EQstCode code = QST_OK;
  char sql[512] = {0};

  if (argc != 3 || argv == NULL || strcmp(argv[1], "--reset-result-store") != 0 ||
      !qstAuditParseUint64(argv[2], &run_id)) {
    (void)fprintf(stderr, "Usage: %s --reset-result-store RUN_ID\n",
                  argc > 0 && argv != NULL && argv[0] != NULL ? argv[0] :
                                                                "taosQueryTest");
    return QST_E_INVALID_ARGUMENT;
  }
  if (getenv("QST_RESULT_STORE_ALLOW_RESET") == NULL ||
      strcmp(getenv("QST_RESULT_STORE_ALLOW_RESET"), "1") != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: result-store reset requires "
                  "QST_RESULT_STORE_ALLOW_RESET=1\n");
    return QST_E_INVALID_ARGUMENT;
  }
  config.run_id = run_id;
  config.endpoint = getenv("QST_RESULT_STORE_ENDPOINT");
  config.config_dir = getenv("QST_RESULT_STORE_CONFIG_DIR");
  config.user = getenv("QST_RESULT_STORE_USER");
  config.password = getenv("QST_RESULT_STORE_PASSWORD");
  code = qst_result_store_open(&config, &store);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: result-store reset open failed code=%d\n", code);
    return code;
  }
  if (snprintf(sql, sizeof(sql),
               "SELECT COUNT(*) FROM res_db.runs WHERE run_id=%llu",
               (unsigned long long)run_id) < 0 || strlen(sql) >= sizeof(sql)) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  code = qst_native_count((SQstNativeClient *)store.native_taos, sql, &run_count);
  if (code != QST_OK || run_count != 1) {
    (void)fprintf(stderr,
                  "taosQueryTest: result-store reset run verification failed "
                  "run_id=%llu count=%lld code=%d\n",
                  (unsigned long long)run_id, (long long)run_count, code);
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  code = qst_native_execute((SQstNativeClient *)store.native_taos,
                            "DROP DATABASE res_db");
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: result-store reset drop failed run_id=%llu "
                  "code=%d\n",
                  (unsigned long long)run_id, code);
    goto _return;
  }
  if (fprintf(stdout, "{\"run_id\":\"%llu\",\"reset\":true,"
                      "\"complete\":true}\n",
              (unsigned long long)run_id) < 0) {
    code = QST_E_RESOURCE;
  }

_return:
  if (qst_result_store_close(&store) != QST_OK && code == QST_OK) {
    code = QST_E_RESULT_STORE;
  }
  return code;
}
