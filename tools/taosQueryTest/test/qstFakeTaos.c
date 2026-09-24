/* Minimal native TDengine ABI fake used only by taosQueryTest unit tests. */

#define _POSIX_C_SOURCE 200809L

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
  char name[65];
  int8_t type;
  int32_t bytes;
} SQstFakeField;

typedef struct {
  int32_t code;
  int64_t count;
  void *row[16];
  SQstFakeField fields[16];
  int32_t lengths[16];
  int32_t field_count;
  int32_t result_precision;
  uint32_t row_count;
  uint32_t next_row;
  int64_t ids[2];
  char labels[2][16];
  uint32_t historical_result;
  char historical_content[8192];
  char historical_validator[32];
  char server_version[64];
  char variable_name[64];
  char variable_value[16];
  uint32_t result_table_result;
  int64_t result_ts[2];
  int64_t result_gid[2];
  int32_t result_verdict[2];
  int32_t result_error_code[2];
  char result_validator[2][32];
  char result_diff_summary[2][128];
  char result_error_message[2][128];
  uint8_t result_content[2][2048];
  uint32_t result_content_bytes[2];
  char result_storage_status[2][24];
  char result_analysis_content[2][128];
  char result_ds_fingerprint[2][65];
  int64_t statement_ts;
  int64_t statement_ended_at;
  int64_t statement_gid;
  int64_t statement_sid;
  int32_t statement_status;
  int64_t statement_row_count;
  int32_t statement_error_code;
  char statement_sql[256];
  char statement_sha256[65];
  int16_t shape_masks[2];
  int64_t shape_counts[2];
  uint32_t shape_aggregate_result;
  char described_statement_columns[4][64];
  uint32_t described_statement_column_count;
  uint32_t describe_statement_result;
  uint8_t raw_block[256];
  uint32_t raw_block_bytes;
  uint32_t raw_block_returned;
} SQstFakeResult;

typedef struct {
  void *connection;
  char *sql;
  void (*callback)(void *, void *, int32_t);
  void *param;
  uint32_t delay_milliseconds;
} SQstFakeAsyncRequest;

enum {
  QST_FAKE_TYPE_SMALLINT = 3,
  QST_FAKE_TYPE_BIGINT = 5,
  QST_FAKE_TYPE_INT = 4,
  QST_FAKE_TYPE_TIMESTAMP = 9,
  QST_FAKE_TYPE_VARCHAR = 8,
  QST_FAKE_TYPE_BLOB = 18,
  QST_FAKE_TIMESTAMP_MILLISECOND = 0,
};

static int32_t qstFakeQnodeReady = 0;
static int32_t qstFakeAddedSqlShapeMask = 0;

static int32_t qstFakeCount(const char *sql, int64_t *out_count) {
  const char *bad_count = getenv("QST_FAKE_TAOS_BAD_COUNT");

  if (NULL != bad_count && bad_count[0] != '\0') {
    *out_count = 3;
    return 0;
  }
  if (NULL != strstr(sql, "information_schema.ins_qnodes")) {
    *out_count = qstFakeQnodeReady;
    return 0;
  }
  if (NULL != strstr(sql, "fq_local_series_empty")) {
    *out_count = 0;
    return 0;
  }
  if (NULL != strstr(sql, "fq_local_precision")) {
    *out_count = 2;
    return 0;
  }
  if (NULL != strstr(sql, "frag_all_types") ||
      NULL != strstr(sql, "frag_plain")) {
    *out_count = 4;
    return 0;
  }
  if (NULL != strstr(sql, "fq_local_device")) {
    *out_count = 4;
    return 0;
  }
  if (NULL != strstr(sql, "fq_local_series")) {
    *out_count = 4;
    return 0;
  }
  if (NULL != strstr(sql, "res_db.")) {
    *out_count = 1;
    return 0;
  }
  return -1;
}

static int32_t qstFakeContainsC4Literal(const char *sql) {
  static const uint8_t expected[] = {'\'', '%', '_', '\\', '\\', '\\', '\'', '\''};
  const uint8_t *bytes = (const uint8_t *)sql;
  const uint64_t sql_bytes = strlen(sql);

  for (uint64_t index = 0; index + sizeof(expected) <= sql_bytes; ++index) {
    if (memcmp(bytes + index, expected, sizeof(expected)) == 0) {
      return 1;
    }
  }
  return 0;
}

/* Cross-process failure injection for product recovery tests. The counter is
 * updated under an advisory lock so forked result-store workers consume an
 * exact number of matching failures. */
static int32_t qstFakeConsumeFailure(const char *sql) {
  const char *pattern = getenv("QST_FAKE_TAOS_FAIL_COUNT_PATTERN");
  const char *limit_text = getenv("QST_FAKE_TAOS_FAIL_COUNT");
  const char *path = getenv("QST_FAKE_TAOS_FAIL_COUNT_FILE");
  char text[32] = {0};
  char *end = NULL;
  unsigned long limit = 0;
  unsigned long count = 0;
  int32_t fd = -1;
  ssize_t bytes = 0;
  int32_t fail = 0;

  if (pattern == NULL || pattern[0] == '\0' || limit_text == NULL ||
      limit_text[0] == '\0' || path == NULL || path[0] == '\0' ||
      strstr(sql, pattern) == NULL) {
    return 0;
  }
  errno = 0;
  limit = strtoul(limit_text, &end, 10);
  if (errno != 0 || end == limit_text || *end != '\0' ||
      limit == 0 || limit > INT_MAX) {
    return 0;
  }
  fd = open(path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  if (fd < 0 || flock(fd, LOCK_EX) != 0) {
    if (fd >= 0) {
      (void)close(fd);
    }
    return 0;
  }
  bytes = read(fd, text, sizeof(text) - 1U);
  if (bytes > 0) {
    errno = 0;
    count = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0') {
      count = limit;
    }
  }
  if (count < limit) {
    const int32_t written = snprintf(text, sizeof(text), "%lu", count + 1U);

    if (written > 0 && (uint64_t)written < sizeof(text) && lseek(fd, 0, SEEK_SET) == 0 &&
        ftruncate(fd, 0) == 0 && write(fd, text, (size_t)written) == written &&
        fsync(fd) == 0) {
      fail = 1;
    }
  }
  (void)flock(fd, LOCK_UN);
  (void)close(fd);
  return fail;
}

/* Test-only server-crash injection. A PID is consumed at most once so all
 * callbacks in one async batch cannot repeatedly signal the same fake taosd.
 * The optional server limit lets a test reproduce the crash once in the batch
 * and once again during ordered single-statement replay. */
static void qstFakeMaybeCrashTaosd(const char *sql) {
  const char *pattern = getenv("QST_FAKE_TAOS_CRASH_PATTERN");
  const char *pid_text = getenv("QST_FAKE_TAOSD_PID");
  const char *cores_dir = getenv("QST_FAKE_TAOSD_CORES_DIR");
  const char *state_path = getenv("QST_FAKE_TAOS_CRASH_PID_FILE");
  const char *limit_text = getenv("QST_FAKE_TAOS_CRASH_SERVER_LIMIT");
  char text[4096] = {0};
  char marker[64] = {0};
  char core_path[4096] = {0};
  char *end = NULL;
  long pid_value = 0;
  unsigned long limit = 0;
  unsigned long count = 0;
  int32_t fd = -1;
  ssize_t bytes = 0;
  int32_t should_crash = 0;

  if (pattern == NULL || pattern[0] == '\0' || pid_text == NULL ||
      pid_text[0] == '\0' || cores_dir == NULL || cores_dir[0] == '\0' ||
      state_path == NULL || state_path[0] == '\0' || strstr(sql, pattern) == NULL) {
    return;
  }
  errno = 0;
  pid_value = strtol(pid_text, &end, 10);
  if (errno != 0 || end == pid_text || *end != '\0' || pid_value <= 0 ||
      pid_value > INT_MAX ||
      snprintf(marker, sizeof(marker), "pid=%ld\n", pid_value) < 0) {
    return;
  }
  if (limit_text != NULL && limit_text[0] != '\0') {
    errno = 0;
    limit = strtoul(limit_text, &end, 10);
    if (errno != 0 || end == limit_text || *end != '\0' || limit == 0 ||
        limit > INT_MAX) {
      return;
    }
  }
  fd = open(state_path, O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  if (fd < 0 || flock(fd, LOCK_EX) != 0) {
    if (fd >= 0) {
      (void)close(fd);
    }
    return;
  }
  bytes = read(fd, text, sizeof(text) - 1U);
  if (bytes > 0 && strstr(text, marker) != NULL) {
    (void)flock(fd, LOCK_UN);
    (void)close(fd);
    return;
  }
  for (char *cursor = text; (cursor = strstr(cursor, "pid=")) != NULL;
       cursor += strlen("pid=")) {
    ++count;
  }
  if (limit == 0 || count < limit) {
    const size_t marker_bytes = strlen(marker);

    if ((uint64_t)(bytes > 0 ? bytes : 0) + marker_bytes < sizeof(text) &&
        lseek(fd, 0, SEEK_END) >= 0 &&
        write(fd, marker, marker_bytes) == (ssize_t)marker_bytes) {
      should_crash = 1;
    }
  }
  (void)flock(fd, LOCK_UN);
  (void)close(fd);
  if (should_crash == 0 ||
      snprintf(core_path, sizeof(core_path), "%s/fake-core-%ld", cores_dir,
               pid_value) < 0) {
    return;
  }
  fd = open(core_path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (fd >= 0) {
    (void)write(fd, "fake core\n", strlen("fake core\n"));
    (void)close(fd);
  }
  (void)kill((pid_t)pid_value, SIGSEGV);
}

static int32_t qstFakeContainsC3Json(const char *sql) {
  static const uint8_t expected[] = {'{', '"', 'k', '"', ':', '"',
                                     0xe4, 0xb8, 0xad, '"', ',', '"',
                                     'n', '"', ':', '3', '}'};
  const uint8_t *bytes = (const uint8_t *)sql;
  const uint64_t sql_bytes = strlen(sql);

  for (uint64_t index = 0; index + sizeof(expected) <= sql_bytes; ++index) {
    if (memcmp(bytes + index, expected, sizeof(expected)) == 0) {
      return 1;
    }
  }
  return 0;
}

static void qstFakeConfigureStoredResults(SQstFakeResult *result) {
  static const char kFingerprint[] =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  static const char kResultContent[] =
      "{\"format\":\"query-result-v1\",\"columns\":[],\"rows\":[]}";
  static const char *const kNames[] = {
      "ts", "gid", "verdict", "validator", "diff_summary", "error_code",
      "error_message", "result_content", "result_storage_status",
      "analysis_content", "ds_fingerprint"};
  static const int8_t kTypes[] = {
      QST_FAKE_TYPE_TIMESTAMP, QST_FAKE_TYPE_BIGINT, QST_FAKE_TYPE_INT,
      QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_INT,
      QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_BLOB, QST_FAKE_TYPE_VARCHAR,
      QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_VARCHAR};
  static const int32_t kWidths[] = {8, 8, 4, 32, 128, 4, 128, 2048, 24, 128, 65};

  if (NULL == result) {
    return;
  }
  result->result_table_result = 1;
  result->row_count = 2;
  result->field_count = 11;
  result->result_ts[0] = INT64_C(1704067200000);
  result->result_ts[1] = INT64_C(1704067201000);
  result->result_gid[0] = 11;
  result->result_gid[1] = 12;
  result->result_verdict[0] = 2;
  result->result_verdict[1] = 0;
  (void)snprintf(result->result_validator[0], sizeof(result->result_validator[0]),
                 "manual");
  (void)snprintf(result->result_validator[1], sizeof(result->result_validator[1]),
                 "historical");
  (void)snprintf(result->result_diff_summary[0], sizeof(result->result_diff_summary[0]),
                 "needs AI review");
  (void)snprintf(result->result_diff_summary[1], sizeof(result->result_diff_summary[1]),
                 "already passed");
  memcpy(result->result_content[0], kResultContent, sizeof(kResultContent) - 1U);
  result->result_content_bytes[0] = sizeof(kResultContent) - 1U;
  (void)snprintf(result->result_storage_status[0],
                 sizeof(result->result_storage_status[0]), "memory");
  (void)snprintf(result->result_storage_status[1],
                 sizeof(result->result_storage_status[1]), "unavailable");
  (void)snprintf(result->result_ds_fingerprint[0],
                 sizeof(result->result_ds_fingerprint[0]), "%s", kFingerprint);
  (void)snprintf(result->result_ds_fingerprint[1],
                 sizeof(result->result_ds_fingerprint[1]), "%s", kFingerprint);
  for (uint32_t index = 0; index < result->field_count; ++index) {
    (void)snprintf(result->fields[index].name, sizeof(result->fields[index].name),
                   "%s", kNames[index]);
    result->fields[index].type = kTypes[index];
    result->fields[index].bytes = kWidths[index];
  }
}

void *taos_connect(const char *host, const char *user, const char *password,
                   const char *database, uint16_t port) {
  const char *connect_log = getenv("QST_FAKE_TAOS_CONNECT_LOG");

  if (connect_log != NULL && connect_log[0] != '\0') {
    FILE *file = fopen(connect_log, "a");

    if (file == NULL) {
      return NULL;
    }
    if (fprintf(file, "host=%s port=%u\n", host == NULL ? "" : host,
                (unsigned int)port) < 0) {
      (void)fclose(file);
      return NULL;
    }
    if (fclose(file) != 0) {
      return NULL;
    }
  }
  (void)user;
  (void)password;
  (void)database;
  (void)port;
  return (void *)(uintptr_t)1;
}

void taos_close(void *connection) {
  (void)connection;
}

int32_t taos_options(int32_t option, const void *value) {
  (void)option;
  return value == NULL ? -1 : 0;
}

const char *taos_errstr(void *result) {
  SQstFakeResult *fake = (SQstFakeResult *)result;
  return NULL != fake && fake->code != 0 ? "fake taos error" : "success";
}

void *taos_query(void *connection, const char *sql) {
  const char *failure = getenv("QST_FAKE_TAOS_FAIL");
  const char *historical = getenv("QST_FAKE_TAOS_HISTORICAL_RESULT");
  const char *historical_validator = getenv("QST_FAKE_TAOS_HISTORICAL_VALIDATOR");
  const char *verify = getenv("QST_FAKE_TAOS_VERIFY_FIXTURE");
  const char *sql_log = getenv("QST_FAKE_TAOS_SQL_LOG");
  SQstFakeResult *result = NULL;

  if (NULL == connection || NULL == sql) {
    return NULL;
  }
  if (sql_log != NULL && sql_log[0] != '\0') {
    FILE *file = fopen(sql_log, "a");
    if (file != NULL) {
      (void)fputs(sql, file);
      (void)fputc('\n', file);
      (void)fclose(file);
    }
  }
  qstFakeMaybeCrashTaosd(sql);
  result = (SQstFakeResult *)calloc(1, sizeof(*result));
  if (NULL == result) {
    return NULL;
  }
  if (NULL != failure && failure[0] != '\0' && NULL != strstr(sql, failure)) {
    result->code = -1;
    return result;
  }
  if (qstFakeConsumeFailure(sql) != 0) {
    result->code = -1;
    return result;
  }
  if (0 == strcmp(sql, "CREATE QNODE ON DNODE 1")) {
    qstFakeQnodeReady = 1;
  }
  if (NULL != strstr(sql,
                     "ALTER STABLE res_db.statements ADD COLUMN sql_shape_mask")) {
    qstFakeAddedSqlShapeMask = 1;
  }
  if (NULL != verify && verify[0] != '\0' &&
      NULL != strstr(sql, "INSERT INTO fq_local_series") &&
      (NULL == strstr(sql, "\xE4\xB8\xAD\xE6\x96\x87") ||
       !qstFakeContainsC3Json(sql) ||
       !qstFakeContainsC4Literal(sql))) {
    result->code = -1;
    return result;
  }
  if (0 == strncmp(sql, "SELECT COUNT(*)", strlen("SELECT COUNT(*)"))) {
    result->code = qstFakeCount(sql, &result->count);
    result->row[0] = &result->count;
    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "count(*)");
    result->fields[0].type = QST_FAKE_TYPE_BIGINT;
    result->fields[0].bytes = (int32_t)sizeof(result->count);
    result->field_count = 1;
    result->row_count = 1;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
  }
  if (NULL != strstr(sql, "SELECT sql_shape_mask,COUNT(*)")) {
    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "sql_shape_mask");
    result->fields[0].type = QST_FAKE_TYPE_SMALLINT;
    result->fields[0].bytes = (int32_t)sizeof(result->shape_masks[0]);
    (void)snprintf(result->fields[1].name, sizeof(result->fields[1].name),
                   "count(*)");
    result->fields[1].type = QST_FAKE_TYPE_BIGINT;
    result->fields[1].bytes = (int32_t)sizeof(result->shape_counts[0]);
    result->field_count = 2;
    result->row_count = 2;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
    result->shape_masks[0] = (int16_t)0x1ffe;
    result->shape_masks[1] = 1;
    result->shape_counts[0] = 2;
    result->shape_counts[1] = 1;
    result->shape_aggregate_result = 1;
  }
  if (NULL != strstr(sql, "INSERT INTO res_db.stmt_") &&
      NULL == strstr(sql, "(ts,gid,ended_at,sid,sql_text,sql_sha256,sql_shape_mask,")) {
    result->code = -1;
    return result;
  }
  if (0 == strcmp(sql, "DESCRIBE res_db.statements")) {
    static const char *const names[] = {
        "dialect", "generation_abnormal", "generation_unsupported_fallback",
        "sql_shape_mask"};
    const char *missing = getenv("QST_FAKE_TAOS_MISSING_STATEMENT_COLUMN");

    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "field");
    result->fields[0].type = QST_FAKE_TYPE_VARCHAR;
    result->fields[0].bytes =
        (int32_t)sizeof(result->described_statement_columns[0]);
    result->field_count = 1;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
    for (uint32_t index = 0; index < sizeof(names) / sizeof(names[0]); ++index) {
      if (missing != NULL && strcmp(missing, names[index]) == 0 &&
          strcmp(names[index], "sql_shape_mask") == 0 &&
          qstFakeAddedSqlShapeMask == 0) {
        continue;
      }
      (void)snprintf(result->described_statement_columns[
                         result->described_statement_column_count],
                     sizeof(result->described_statement_columns[0]), "%s", names[index]);
      ++result->described_statement_column_count;
    }
    result->row_count = result->described_statement_column_count;
    result->describe_statement_result = 1;
  }
  if (NULL != strstr(sql, "server_version()")) {
    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "server_version()");
    result->fields[0].type = QST_FAKE_TYPE_VARCHAR;
    result->fields[0].bytes = (int32_t)sizeof(result->server_version);
    (void)snprintf(result->server_version, sizeof(result->server_version),
                   "3.4.3.0.fake");
    result->row[0] = result->server_version;
    result->field_count = 1;
    result->row_count = 1;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
  }
  if (NULL != strstr(sql, "federatedQueryEnable")) {
    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "name");
    (void)snprintf(result->fields[1].name, sizeof(result->fields[1].name),
                   "value");
    result->fields[0].type = QST_FAKE_TYPE_VARCHAR;
    result->fields[1].type = QST_FAKE_TYPE_VARCHAR;
    result->fields[0].bytes = (int32_t)sizeof(result->variable_name);
    result->fields[1].bytes = (int32_t)sizeof(result->variable_value);
    (void)snprintf(result->variable_name, sizeof(result->variable_name),
                   "federatedQueryEnable");
    (void)snprintf(result->variable_value, sizeof(result->variable_value), "1");
    result->row[0] = result->variable_name;
    result->row[1] = result->variable_value;
    result->field_count = 2;
    result->row_count = 1;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
  }
  if (0 == strncmp(sql, "SELECT result_content,validator FROM res_db.verdict_",
                   strlen("SELECT result_content,validator FROM res_db.verdict_"))) {
    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "result_content");
    result->fields[0].type = QST_FAKE_TYPE_BLOB;
    result->fields[0].bytes = (int32_t)sizeof(result->historical_content);
    (void)snprintf(result->fields[1].name, sizeof(result->fields[1].name),
                   "validator");
    result->fields[1].type = QST_FAKE_TYPE_VARCHAR;
    result->fields[1].bytes = (int32_t)sizeof(result->historical_validator);
    result->field_count = 2;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
    if (historical != NULL && historical[0] != '\0' &&
        strlen(historical) < sizeof(result->historical_content)) {
      result->historical_result = 1;
      result->row_count = 1;
      (void)snprintf(result->historical_content,
                     sizeof(result->historical_content), "%s", historical);
      (void)snprintf(result->historical_validator,
                     sizeof(result->historical_validator), "%s",
                     historical_validator != NULL && historical_validator[0] != '\0' ?
                         historical_validator : "historical");
    }
  }
  if (NULL != strstr(sql, "qst_query_result")) {
    (void)snprintf(result->fields[0].name, sizeof(result->fields[0].name),
                   "id");
    result->fields[0].type = QST_FAKE_TYPE_BIGINT;
    result->fields[0].bytes = (int32_t)sizeof(result->ids[0]);
    (void)snprintf(result->fields[1].name, sizeof(result->fields[1].name),
                   "label");
    result->fields[1].type = QST_FAKE_TYPE_VARCHAR;
    result->fields[1].bytes = (int32_t)sizeof(result->labels[0]);
    result->field_count = 2;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
    result->row_count = 2;
    result->ids[0] = 42;
    result->ids[1] = 7;
    (void)snprintf(result->labels[0], sizeof(result->labels[0]), "alpha");
    (void)snprintf(result->labels[1], sizeof(result->labels[1]), "beta");
  }
  if (NULL != strstr(sql, "SELECT ts,gid,verdict,validator,diff_summary,error_code")) {
    qstFakeConfigureStoredResults(result);
  }
  if (result->shape_aggregate_result == 0 &&
      (NULL != strstr(sql, "FROM res_db.stmt_") ||
       NULL != strstr(sql, "FROM res_db.statements WHERE"))) {
    static const char *const names[] = {
        "ts", "gid", "ended_at", "sid", "sql_text", "sql_sha256",
        "taosd_pid", "taosc_pid", "execution_status", "result_row_count",
        "error_code", "error_message", "taosd_log_ranges", "taosc_log_ranges",
        "core_relative_path"};
    static const int8_t types[] = {
        QST_FAKE_TYPE_TIMESTAMP, QST_FAKE_TYPE_BIGINT, QST_FAKE_TYPE_TIMESTAMP,
        QST_FAKE_TYPE_BIGINT, QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_VARCHAR,
        QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_INT,
        QST_FAKE_TYPE_BIGINT, QST_FAKE_TYPE_INT, QST_FAKE_TYPE_VARCHAR,
        QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_VARCHAR, QST_FAKE_TYPE_VARCHAR};
    result->field_count = 15;
    result->row_count = 1;
    result->result_precision = QST_FAKE_TIMESTAMP_MILLISECOND;
    result->statement_ts = INT64_C(1704067200000);
    result->statement_ended_at = INT64_C(1704067201000);
    result->statement_gid = 11;
    result->statement_sid = 22;
    result->statement_status = 0;
    result->statement_row_count = 3;
    (void)snprintf(result->statement_sql, sizeof(result->statement_sql),
                   "select * from t");
    (void)snprintf(result->statement_sha256, sizeof(result->statement_sha256),
                   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    for (uint32_t index = 0; index < result->field_count; ++index) {
      (void)snprintf(result->fields[index].name, sizeof(result->fields[index].name),
                     "%s", names[index]);
      result->fields[index].type = types[index];
      result->fields[index].bytes = index == 0 || index == 2 || index == 1 || index == 3 ||
                                            index == 8 || index == 9 || index == 10
                                        ? 8
                                        : 256;
    }
  }
  return result;
}

int32_t taos_errno(void *result) {
  return NULL == result ? -1 : ((SQstFakeResult *)result)->code;
}

void taos_kill_query(void *connection) {
  const char *eventfd_text = getenv("QST_FAKE_TAOS_KILL_EVENTFD");
  const char *path = getenv("QST_FAKE_TAOS_KILL_LOG");

  (void)connection;
  if (eventfd_text != NULL && eventfd_text[0] != '\0') {
    char *end = NULL;
    const unsigned long descriptor = strtoul(eventfd_text, &end, 10);

    if (end != eventfd_text && *end == '\0' && descriptor <= INT32_MAX) {
      const uint64_t event_count = 1;

      (void)write((int32_t)descriptor, &event_count, sizeof(event_count));
    }
  }
  if (path != NULL && path[0] != '\0') {
    FILE *file = fopen(path, "a");
    if (file != NULL) {
      (void)fputs("kill\n", file);
      (void)fclose(file);
    }
  }
}

static void *qstFakeAsyncCallback(void *arg) {
  SQstFakeAsyncRequest *request = (SQstFakeAsyncRequest *)arg;
  struct timespec pause = {0};
  void *result = NULL;
  int32_t code = -1;

  if (request == NULL) {
    return NULL;
  }
  pause.tv_sec = request->delay_milliseconds / 1000U;
  pause.tv_nsec = (long)(request->delay_milliseconds % 1000U) * 1000000L;
  (void)nanosleep(&pause, NULL);
  result = taos_query(request->connection, request->sql);
  code = result == NULL ? -1 : taos_errno(result);
  request->callback(request->param, result, code);
  free(request->sql);
  free(request);
  return NULL;
}

void taos_query_a(void *connection, const char *sql,
                  void (*callback)(void *, void *, int32_t), void *param) {
  const char *no_callback = getenv("QST_FAKE_TAOS_NO_CALLBACK");
  const char *delay_text = getenv("QST_FAKE_TAOS_ASYNC_DELAY_MS");
  unsigned long delay = 0;

  if (callback == NULL || (no_callback != NULL && no_callback[0] != '\0')) {
    return;
  }
  if (delay_text != NULL && delay_text[0] != '\0') {
    char *end = NULL;
    SQstFakeAsyncRequest *request = NULL;
    pthread_t thread = 0;

    delay = strtoul(delay_text, &end, 10);
    if (end != delay_text && *end == '\0' && delay <= UINT32_MAX) {
      request = (SQstFakeAsyncRequest *)calloc(1, sizeof(*request));
      if (request != NULL) {
        request->sql = strdup(sql);
        request->connection = connection;
        request->callback = callback;
        request->param = param;
        request->delay_milliseconds = (uint32_t)delay;
        if (request->sql != NULL && pthread_create(&thread, NULL,
                                                   qstFakeAsyncCallback,
                                                   request) == 0) {
          (void)pthread_detach(thread);
          return;
        }
        free(request->sql);
        free(request);
      }
    }
  }
  {
    void *result = taos_query(connection, sql);
    const int32_t code = NULL == result ? -1 : taos_errno(result);

    callback(param, result, code);
  }
}

void taos_fetch_rows_a(void *result,
                       void (*callback)(void *, void *, int32_t),
                       void *param) {
  SQstFakeResult *fake = (SQstFakeResult *)result;
  int32_t row_count = -1;

  if (fake != NULL && fake->code == 0) {
    row_count = (int32_t)(fake->row_count - fake->next_row);
  }
  if (callback != NULL) {
    callback(param, result, row_count);
  }
}

static void qstFakeRawWriteU16(uint8_t *data, uint32_t offset, uint16_t value) {
  memcpy(data + offset, &value, sizeof(value));
}

static void qstFakeRawWriteU32(uint8_t *data, uint32_t offset, uint32_t value) {
  memcpy(data + offset, &value, sizeof(value));
}

static void qstFakeRawWriteI32(uint8_t *data, uint32_t offset, int32_t value) {
  memcpy(data + offset, &value, sizeof(value));
}

static void qstFakeRawWriteI64(uint8_t *data, uint32_t offset, int64_t value) {
  memcpy(data + offset, &value, sizeof(value));
}

static int32_t qstFakeBuildQueryResultRawBlock(SQstFakeResult *fake) {
  enum {
    kHeaderBytes = 28U,
    kFieldBytes = 5U,
    kColumnBytes = 4U,
    kRows = 2U,
    kColumns = 2U,
    kPayloadOffset = kHeaderBytes + kColumns * kFieldBytes +
                     kColumns * kColumnBytes,
    kFixedBitmapBytes = 1U,
    kFixedPayloadBytes = 16U,
    kVariableOffsetsBytes = 8U,
    kVariablePayloadBytes = 13U,
    kBlankFillBytes = 1U,
    kTotalBytes = kPayloadOffset + kFixedBitmapBytes + kFixedPayloadBytes +
                  kVariableOffsetsBytes + kVariablePayloadBytes +
                  kBlankFillBytes,
  };
  uint32_t offset = kPayloadOffset;

  if (fake == NULL || fake->field_count != 2 || fake->row_count != kRows ||
      fake->fields[0].type != QST_FAKE_TYPE_BIGINT ||
      fake->fields[1].type != QST_FAKE_TYPE_VARCHAR ||
      kTotalBytes > sizeof(fake->raw_block)) {
    return -1;
  }
  memset(fake->raw_block, 0, sizeof(fake->raw_block));
  qstFakeRawWriteI32(fake->raw_block, 0, 2);
  qstFakeRawWriteI32(fake->raw_block, 4, kTotalBytes);
  qstFakeRawWriteI32(fake->raw_block, 8, kRows);
  qstFakeRawWriteI32(fake->raw_block, 12, kColumns);
  fake->raw_block[kHeaderBytes] = QST_FAKE_TYPE_BIGINT;
  qstFakeRawWriteI32(fake->raw_block, kHeaderBytes + 1U,
                      (int32_t)sizeof(fake->ids[0]));
  fake->raw_block[kHeaderBytes + kFieldBytes] = QST_FAKE_TYPE_VARCHAR;
  qstFakeRawWriteI32(fake->raw_block, kHeaderBytes + kFieldBytes + 1U,
                      (int32_t)sizeof(fake->labels[0]));
  qstFakeRawWriteU32(fake->raw_block, kHeaderBytes + kColumns * kFieldBytes,
                      kFixedPayloadBytes);
  qstFakeRawWriteU32(fake->raw_block,
                      kHeaderBytes + kColumns * kFieldBytes + kColumnBytes,
                      kVariablePayloadBytes);
  ++offset;  /* Fixed-column NULL bitmap: both rows are non-NULL. */
  qstFakeRawWriteI64(fake->raw_block, offset, fake->ids[0]);
  offset += sizeof(fake->ids[0]);
  qstFakeRawWriteI64(fake->raw_block, offset, fake->ids[1]);
  offset += sizeof(fake->ids[1]);
  qstFakeRawWriteU32(fake->raw_block, offset, 0);
  offset += sizeof(uint32_t);
  qstFakeRawWriteU32(fake->raw_block, offset, 7);
  offset += sizeof(uint32_t);
  qstFakeRawWriteU16(fake->raw_block, offset, 5);
  offset += sizeof(uint16_t);
  memcpy(fake->raw_block + offset, "alpha", 5);
  offset += 5U;
  qstFakeRawWriteU16(fake->raw_block, offset, 4);
  offset += sizeof(uint16_t);
  memcpy(fake->raw_block + offset, "beta", 4);
  offset += 4U;
  if (offset + kBlankFillBytes != kTotalBytes) {
    return -1;
  }
  fake->raw_block_bytes = kTotalBytes;
  return 0;
}

int32_t taos_fetch_raw_block(void *result, int32_t *row_count, void **data) {
  SQstFakeResult *fake = (SQstFakeResult *)result;

  if (fake == NULL || row_count == NULL || data == NULL || fake->code != 0) {
    return -1;
  }
  *row_count = 0;
  *data = NULL;
  if (fake->raw_block_returned != 0) {
    return 0;
  }
  if (qstFakeBuildQueryResultRawBlock(fake) != 0) {
    return -1;
  }
  fake->raw_block_returned = 1;
  *row_count = (int32_t)fake->row_count;
  *data = fake->raw_block;
  return 0;
}

void taos_free_result(void *result) {
  free(result);
}

void **taos_fetch_row(void *result) {
  SQstFakeResult *fake = (SQstFakeResult *)result;

  if (NULL == fake || fake->code != 0) {
    return NULL;
  }
  if (fake->field_count != 0) {
    if (fake->next_row >= fake->row_count) {
      return NULL;
    }
    if (fake->result_table_result != 0) {
      const uint32_t row_index = fake->next_row;
      fake->row[0] = &fake->result_ts[row_index];
      fake->row[1] = &fake->result_gid[row_index];
      fake->row[2] = &fake->result_verdict[row_index];
      fake->row[3] = fake->result_validator[row_index];
      fake->row[4] = fake->result_diff_summary[row_index];
      fake->row[5] = row_index == 0 ? &fake->result_error_code[row_index] : NULL;
      fake->row[6] = fake->result_error_message[row_index][0] == '\0' ? NULL :
                     fake->result_error_message[row_index];
      fake->row[7] = row_index == 0 ? fake->result_content[row_index] : NULL;
      fake->row[8] = fake->result_storage_status[row_index];
      fake->row[9] = fake->result_analysis_content[row_index][0] == '\0' ? NULL :
                     fake->result_analysis_content[row_index];
      fake->row[10] = fake->result_ds_fingerprint[row_index];
      fake->lengths[0] = (int32_t)sizeof(fake->result_ts[row_index]);
      fake->lengths[1] = (int32_t)sizeof(fake->result_gid[row_index]);
      fake->lengths[2] = (int32_t)sizeof(fake->result_verdict[row_index]);
      fake->lengths[3] = (int32_t)strlen(fake->result_validator[row_index]);
      fake->lengths[4] = (int32_t)strlen(fake->result_diff_summary[row_index]);
      fake->lengths[5] = row_index == 0 ? (int32_t)sizeof(fake->result_error_code[row_index]) : 0;
      fake->lengths[6] = fake->row[6] == NULL ? 0 :
                         (int32_t)strlen(fake->result_error_message[row_index]);
      fake->lengths[7] = row_index == 0 ? (int32_t)fake->result_content_bytes[row_index] : 0;
      fake->lengths[8] = (int32_t)strlen(fake->result_storage_status[row_index]);
      fake->lengths[9] = fake->row[9] == NULL ? 0 :
                         (int32_t)strlen(fake->result_analysis_content[row_index]);
      fake->lengths[10] = (int32_t)strlen(fake->result_ds_fingerprint[row_index]);
      ++fake->next_row;
      return fake->row;
    }
    if (fake->shape_aggregate_result != 0) {
      const uint32_t row_index = fake->next_row;

      fake->row[0] = &fake->shape_masks[row_index];
      fake->row[1] = &fake->shape_counts[row_index];
      fake->lengths[0] = (int32_t)sizeof(fake->shape_masks[row_index]);
      fake->lengths[1] = (int32_t)sizeof(fake->shape_counts[row_index]);
      ++fake->next_row;
      return fake->row;
    }
    if (fake->describe_statement_result != 0) {
      const uint32_t row_index = fake->next_row;

      fake->row[0] = fake->described_statement_columns[row_index];
      fake->lengths[0] =
          (int32_t)strlen(fake->described_statement_columns[row_index]);
      ++fake->next_row;
      return fake->row;
    }
    if (fake->statement_sql[0] != '\0') {
      fake->row[0] = &fake->statement_ts;
      fake->row[1] = &fake->statement_gid;
      fake->row[2] = &fake->statement_ended_at;
      fake->row[3] = &fake->statement_sid;
      fake->row[4] = fake->statement_sql;
      fake->row[5] = fake->statement_sha256;
      fake->row[6] = NULL;
      fake->row[7] = NULL;
      fake->row[8] = &fake->statement_status;
      fake->row[9] = &fake->statement_row_count;
      fake->row[10] = NULL;
      for (uint32_t index = 11; index < 15; ++index) fake->row[index] = NULL;
      fake->lengths[0] = sizeof(fake->statement_ts);
      fake->lengths[1] = sizeof(fake->statement_gid);
      fake->lengths[2] = sizeof(fake->statement_ended_at);
      fake->lengths[3] = sizeof(fake->statement_sid);
      fake->lengths[4] = strlen(fake->statement_sql);
      fake->lengths[5] = strlen(fake->statement_sha256);
      fake->lengths[8] = sizeof(fake->statement_status);
      fake->lengths[9] = sizeof(fake->statement_row_count);
      ++fake->next_row;
      return fake->row;
    }
    if (strcmp(fake->fields[0].name, "count(*)") == 0) {
      fake->row[0] = &fake->count;
      fake->lengths[0] = (int32_t)sizeof(fake->count);
      ++fake->next_row;
      return fake->row;
    }
    if (strcmp(fake->fields[0].name, "server_version()") == 0) {
      fake->row[0] = fake->server_version;
      fake->lengths[0] = (int32_t)strlen(fake->server_version);
      ++fake->next_row;
      return fake->row;
    }
    if (strcmp(fake->fields[0].name, "name") == 0 &&
        strcmp(fake->fields[1].name, "value") == 0) {
      fake->row[0] = fake->variable_name;
      fake->row[1] = fake->variable_value;
      fake->lengths[0] = (int32_t)strlen(fake->variable_name);
      fake->lengths[1] = (int32_t)strlen(fake->variable_value);
      ++fake->next_row;
      return fake->row;
    }
    if (fake->historical_result != 0) {
      fake->row[0] = fake->historical_content;
      fake->row[1] = fake->historical_validator;
      fake->lengths[0] = (int32_t)strlen(fake->historical_content);
      fake->lengths[1] = (int32_t)strlen(fake->historical_validator);
    } else {
      fake->row[0] = &fake->ids[fake->next_row];
      fake->row[1] = fake->labels[fake->next_row];
      fake->lengths[0] = (int32_t)sizeof(fake->ids[fake->next_row]);
      fake->lengths[1] = (int32_t)strlen(fake->labels[fake->next_row]);
    }
    ++fake->next_row;
    return fake->row;
  }
  if (NULL == fake->row[0]) {
    return NULL;
  }
  return fake->row;
}

int32_t taos_num_fields(void *result) {
  return NULL == result ? -1 : ((SQstFakeResult *)result)->field_count;
}

void *taos_fetch_fields(void *result) {
  SQstFakeResult *fake = (SQstFakeResult *)result;

  return NULL == fake || fake->field_count == 0 ? NULL : fake->fields;
}

int32_t *taos_fetch_lengths(void *result) {
  SQstFakeResult *fake = (SQstFakeResult *)result;

  return NULL == fake || fake->field_count == 0 ? NULL : fake->lengths;
}

int32_t taos_result_precision(void *result) {
  return NULL == result ? -1 : ((SQstFakeResult *)result)->result_precision;
}
