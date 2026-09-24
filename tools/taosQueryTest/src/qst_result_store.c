/* Native result-store boundary and local pending binary stream writer. */

#define _POSIX_C_SOURCE 200809L

#include "qst_result_store.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <time.h>
#include <unistd.h>

#include "taos.h"
#include "qst_block.h"
#include "qst_generator.h"
#include "qst_native.h"
#include "qst_sha256.h"
#include "qst_task.h"
#include "toolscJson.h"

enum {
  QST_PENDING_FORMAT_VERSION = 1,
  QST_PENDING_STREAM_MAGIC = UINT32_C(0x51535450),
  QST_PENDING_STREAM_VERSION = 1,
  QST_PENDING_DIGEST_HEX_BYTES = 64,
  QST_PENDING_PATH_BYTES = 4096,
  QST_RESULT_BLOB_MAX_BYTES = 4U * 1024U * 1024U,
  QST_RESULT_STORE_RECOVERY_ATTEMPTS = 3,
  QST_RESULT_STORE_RECOVERY_WAIT_MILLISECONDS = 100,
  QST_PENDING_STREAM_APPEND_BYTES = 1U * 1024U * 1024U,
};

enum {
  QST_SQL_SHAPE_EXPLAIN = 1U << 0,
  QST_SQL_SHAPE_SELECT = 1U << 1,
  QST_SQL_SHAPE_FROM = 1U << 2,
  QST_SQL_SHAPE_WHERE = 1U << 3,
  QST_SQL_SHAPE_GROUP_BY = 1U << 4,
  QST_SQL_SHAPE_HAVING = 1U << 5,
  QST_SQL_SHAPE_ORDER_BY = 1U << 6,
  QST_SQL_SHAPE_LIMIT = 1U << 7,
  QST_SQL_SHAPE_JOIN = 1U << 8,
  QST_SQL_SHAPE_SUBQUERY = 1U << 9,
  QST_SQL_SHAPE_AGGREGATE = 1U << 10,
  QST_SQL_SHAPE_WINDOW = 1U << 11,
  QST_SQL_SHAPE_EXTERNAL = 1U << 12,
};

typedef struct {
  uint32_t magic;
  uint32_t version;
  uint32_t persist_kind;
  uint32_t target;
  uint64_t run_id;
  uint64_t gid;
  uint64_t record_seq;
  int64_t created_at_ns;
  uint64_t payload_bytes;
} SQstPendingStreamRecordHeader;

static EQstCode qstPendingReadFile(const char *path, char **out_text);
static int32_t qstResultStoreHexValue(char value, uint8_t *out_value);
static EQstCode qstResultStorePersistTimestampMs(const SQstShmMapping *mapping,
                                                 uint64_t record_seq,
                                                 int64_t *out_timestamp);
static int32_t qstPendingHexSegmentIsValid(const char *text, uint64_t bytes);
static EQstCode qstResultStoreExecuteWithRecovery(
    SQstShmMapping *mapping, SQstResultStore *store, const char *sql,
    uint64_t record_seq);
static EQstCode qstResultStoreOpenNative(SQstResultStore *store);
static EQstCode qstResultStoreCopyTypedNumber(const tools_cJSON *item,
                                              int64_t *out_value,
                                              int32_t *out_is_null);

typedef struct {
  const char *name;
  const char *type;
  uint32_t bit;
} SQstResultStoreStatementColumn;

static const SQstResultStoreStatementColumn kResultStoreStatementColumns[] = {
    {"dialect", "TINYINT", 1U << 0},
    {"generation_abnormal", "TINYINT", 1U << 1},
    {"generation_unsupported_fallback", "TINYINT", 1U << 2},
    {"sql_shape_mask", "SMALLINT", 1U << 3},
};

static int32_t qstResultStoreShapeBoundary(unsigned char value) {
  return !((value >= 'A' && value <= 'Z') ||
           (value >= 'a' && value <= 'z') ||
           (value >= '0' && value <= '9') || value == '_');
}

static int32_t qstResultStoreShapeEqual(unsigned char left, unsigned char right) {
  if (left >= 'a' && left <= 'z') {
    left = (unsigned char)(left - ('a' - 'A'));
  }
  if (right >= 'a' && right <= 'z') {
    right = (unsigned char)(right - ('a' - 'A'));
  }
  return left == right;
}

static int32_t qstResultStoreShapeTokenAt(const char *text, uint64_t length,
                                          uint64_t offset, const char *token,
                                          uint64_t *out_end) {
  const uint64_t token_length = token == NULL ? 0 : strlen(token);

  if (text == NULL || token == NULL || out_end == NULL || token_length == 0 ||
      offset >= length || token_length > length - offset ||
      (offset > 0 && !qstResultStoreShapeBoundary((unsigned char)text[offset - 1]))) {
    return 0;
  }
  for (uint64_t index = 0; index < token_length; ++index) {
    if (!qstResultStoreShapeEqual((unsigned char)text[offset + index],
                                  (unsigned char)token[index])) {
      return 0;
    }
  }
  *out_end = offset + token_length;
  return *out_end == length ||
         qstResultStoreShapeBoundary((unsigned char)text[*out_end]);
}

/* A federated source is recorded as src_fq_<provider>_<name>.<table>. The
 * source name must begin on an identifier boundary and contain a nonempty
 * identifier suffix before its table separator. */
static int32_t qstResultStoreShapeExternalSourceAt(const char *text,
                                                   uint64_t length,
                                                   uint64_t offset) {
  static const char kSourcePrefix[] = "SRC_FQ_";
  const uint64_t prefix_length = sizeof(kSourcePrefix) - 1U;
  uint64_t source_end = 0;

  if (text == NULL || offset >= length || prefix_length > length - offset ||
      (offset > 0 &&
       !qstResultStoreShapeBoundary((unsigned char)text[offset - 1]))) {
    return 0;
  }
  for (uint64_t index = 0; index < prefix_length; ++index) {
    if (!qstResultStoreShapeEqual((unsigned char)text[offset + index],
                                  (unsigned char)kSourcePrefix[index])) {
      return 0;
    }
  }
  source_end = offset + prefix_length;
  while (source_end < length &&
         !qstResultStoreShapeBoundary((unsigned char)text[source_end])) {
    ++source_end;
  }
  return source_end > offset + prefix_length && source_end < length &&
         text[source_end] == '.';
}

static int32_t qstResultStoreShapeSpace(unsigned char value) {
  return value == ' ' || value == '\t' || value == '\n' || value == '\r' ||
         value == '\v' || value == '\f';
}

static uint16_t qstResultStoreSqlShapeMask(const char *text) {
  uint16_t mask = 0;
  uint64_t length = text == NULL ? 0 : strlen(text);

  for (uint64_t offset = 0; offset < length; ++offset) {
    unsigned char initial = (unsigned char)text[offset];
    uint64_t end = 0;
    uint16_t function_mask = 0;

    if (initial >= 'a' && initial <= 'z') {
      initial = (unsigned char)(initial - ('a' - 'A'));
    }
    switch (initial) {
      case 'C':
        if (qstResultStoreShapeTokenAt(text, length, offset, "COUNT", &end)) {
          function_mask = QST_SQL_SHAPE_AGGREGATE;
        }
        break;
      case 'E':
        if (qstResultStoreShapeTokenAt(text, length, offset, "EXPLAIN", &end)) {
          mask |= QST_SQL_SHAPE_EXPLAIN;
        }
        if (qstResultStoreShapeTokenAt(text, length, offset, "EXTERNAL", &end)) {
          mask |= QST_SQL_SHAPE_EXTERNAL;
        }
        break;
      case 'F':
        if (qstResultStoreShapeTokenAt(text, length, offset, "FROM", &end)) {
          mask |= QST_SQL_SHAPE_FROM;
        }
        break;
      case 'G':
        if (qstResultStoreShapeTokenAt(text, length, offset, "GROUP", &end)) {
          uint64_t pair = end;

          while (pair < length &&
                 qstResultStoreShapeSpace((unsigned char)text[pair])) {
            ++pair;
          }
          if (qstResultStoreShapeTokenAt(text, length, pair, "BY", &end)) {
            mask |= QST_SQL_SHAPE_GROUP_BY;
          }
        }
        break;
      case 'H':
        if (qstResultStoreShapeTokenAt(text, length, offset, "HAVING", &end)) {
          mask |= QST_SQL_SHAPE_HAVING;
        }
        break;
      case 'J':
        if (qstResultStoreShapeTokenAt(text, length, offset, "JOIN", &end)) {
          mask |= QST_SQL_SHAPE_JOIN;
        }
        break;
      case 'L':
        if (qstResultStoreShapeTokenAt(text, length, offset, "LIMIT", &end)) {
          mask |= QST_SQL_SHAPE_LIMIT;
        }
        break;
      case 'O':
        if (qstResultStoreShapeTokenAt(text, length, offset, "ORDER", &end)) {
          uint64_t pair = end;

          while (pair < length &&
                 qstResultStoreShapeSpace((unsigned char)text[pair])) {
            ++pair;
          }
          if (qstResultStoreShapeTokenAt(text, length, pair, "BY", &end)) {
            mask |= QST_SQL_SHAPE_ORDER_BY;
          }
        }
        if (qstResultStoreShapeTokenAt(text, length, offset, "OVER", &end)) {
          function_mask = QST_SQL_SHAPE_WINDOW;
        }
        break;
      case 'S':
        if (qstResultStoreShapeTokenAt(text, length, offset, "SELECT", &end)) {
          mask |= QST_SQL_SHAPE_SELECT;
        }
        if (qstResultStoreShapeExternalSourceAt(text, length, offset)) {
          mask |= QST_SQL_SHAPE_EXTERNAL;
        }
        break;
      case 'W':
        if (qstResultStoreShapeTokenAt(text, length, offset, "WHERE", &end)) {
          mask |= QST_SQL_SHAPE_WHERE;
        }
        break;
      case '(':
        {
          uint64_t nested = offset + 1U;

          while (nested < length &&
                 qstResultStoreShapeSpace((unsigned char)text[nested])) {
            ++nested;
          }
          if (qstResultStoreShapeTokenAt(text, length, nested, "SELECT", &end)) {
            mask |= QST_SQL_SHAPE_SUBQUERY;
          }
        }
        break;
      default:
        break;
    }
    if (function_mask != 0) {
      uint64_t function_end = end;

      while (function_end < length &&
             qstResultStoreShapeSpace((unsigned char)text[function_end])) {
        ++function_end;
      }
      if (function_end < length && text[function_end] == '(') {
        mask |= function_mask;
      }
    }
  }
  return mask;
}

/* An explicitly configured result-store endpoint is host:port. The empty
 * form remains for in-process unit tests that intentionally use native
 * defaults, while packaged runs always supply a concrete endpoint. */
static EQstCode qstResultStoreParseEndpoint(const char *endpoint,
                                            char *out_host,
                                            uint64_t host_bytes,
                                            uint16_t *out_port) {
  const char *separator = NULL;
  char *end = NULL;
  unsigned long parsed_port = 0;
  uint64_t host_length = 0;

  if (NULL == out_host || host_bytes == 0 || NULL == out_port) {
    return QST_E_INVALID_ARGUMENT;
  }
  out_host[0] = '\0';
  *out_port = 0;
  if (NULL == endpoint || endpoint[0] == '\0') {
    return QST_OK;
  }
  separator = strrchr(endpoint, ':');
  if (NULL == separator || separator == endpoint || separator[1] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  host_length = (uint64_t)(separator - endpoint);
  if (host_length >= host_bytes || strchr(separator + 1, ':') != NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  errno = 0;
  parsed_port = strtoul(separator + 1, &end, 10);
  if (errno != 0 || end == separator + 1 || *end != '\0' ||
      parsed_port == 0 || parsed_port > UINT16_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  memcpy(out_host, endpoint, (size_t)host_length);
  out_host[host_length] = '\0';
  *out_port = (uint16_t)parsed_port;
  return QST_OK;
}

static void qstPendingHex(const uint8_t *input, uint64_t input_bytes,
                          char *output) {
  static const char kHex[] = "0123456789abcdef";

  for (uint64_t index = 0; index < input_bytes; ++index) {
    output[index * 2U] = kHex[input[index] >> 4U];
    output[index * 2U + 1U] = kHex[input[index] & 0x0fU];
  }
  output[input_bytes * 2U] = '\0';
}

/* Uses a per-run logical timestamp key because TDengine child-table rows are
 * keyed by timestamp. Concurrent completions commonly share a wall-clock
 * millisecond, while record_seq is unique and stable across a retry. */
static EQstCode qstResultStorePersistTimestampMs(const SQstShmMapping *mapping,
                                                 uint64_t record_seq,
                                                 int64_t *out_timestamp) {
  uint64_t run_milliseconds = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == out_timestamp ||
      record_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  run_milliseconds = mapping->header->run_id / UINT64_C(1000000);
  if (run_milliseconds > (uint64_t)INT64_MAX ||
      record_seq > (uint64_t)INT64_MAX - run_milliseconds) {
    return QST_E_RESOURCE;
  }
  *out_timestamp = (int64_t)(run_milliseconds + record_seq);
  return QST_OK;
}

static const char *qstPendingKindName(uint32_t persist_kind) {
  switch (persist_kind) {
    case QST_PERSIST_RUN_UPDATE:
      return "runs";
    case QST_PERSIST_DATASET:
      return "datasets";
    case QST_PERSIST_STATEMENT:
      return "statements";
    case QST_PERSIST_INITIAL_RESULT:
      return "results";
    case QST_PERSIST_MANUAL_AI_RESULT:
      return "results";
    case QST_PERSIST_HISTORICAL_VERDICT:
      return "historical_verdicts";
    default:
      return NULL;
  }
}

static int64_t qstPendingNowNs(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > INT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (int64_t)((uint64_t)now.tv_sec * UINT64_C(1000000000) +
                   (uint64_t)now.tv_nsec);
}

static EQstCode qstPendingWriteAll(int32_t fd, const uint8_t *bytes,
                                   uint64_t byte_count) {
  uint64_t written = 0;

  if (fd < 0 || (byte_count != 0 && NULL == bytes)) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (written < byte_count) {
    const uint64_t remaining = byte_count - written;
    const size_t request = remaining > (uint64_t)SSIZE_MAX ?
                               (size_t)SSIZE_MAX : (size_t)remaining;
    const ssize_t result = write(fd, bytes + written, request);

    if (result < 0) {
      if (errno == EINTR) {
        continue;
      }
      return QST_E_PENDING_IO;
    }
    if (result == 0) {
      return QST_E_PENDING_IO;
    }
    written += (uint64_t)result;
  }
  return QST_OK;
}

static EQstCode qstPendingStreamPath(char *out_path, uint64_t path_bytes,
                                     const char *directory) {
  int32_t result = 0;

  if (NULL == out_path || NULL == directory || directory[0] == '\0' ||
      path_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  result = snprintf(out_path, (size_t)path_bytes, "%s/pending.stream", directory);
  if (result < 0 || (uint64_t)result >= path_bytes) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

/* Appends one accumulated binary segment. This is intentionally only a write
 * coalescing boundary: it has no fsync, rename, or all-or-nothing semantics. */
static EQstCode qstPendingStreamWrite(SQstPendingBatch *stream) {
  int32_t locked = 0;
  EQstCode code = QST_OK;

  if (NULL == stream || stream->fd < 0 || NULL == stream->buffer ||
      stream->byte_count == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (flock(stream->fd, LOCK_EX) != 0) {
    return QST_E_PENDING_IO;
  }
  locked = 1;
  code = qstPendingWriteAll(stream->fd, stream->buffer, stream->byte_count);
  if (locked != 0 && flock(stream->fd, LOCK_UN) != 0 && code == QST_OK) {
    code = QST_E_PENDING_IO;
  }
  if (code == QST_OK) {
    stream->byte_count = 0;
  }
  return code;
}

/* Counts all regular pending objects so quota applies to the complete directory. */
static EQstCode qstPendingDirectoryBytes(const char *directory,
                                         uint64_t *out_bytes) {
  DIR *stream = NULL;
  struct dirent *entry = NULL;
  uint64_t total = 0;

  if (NULL == directory || NULL == out_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  stream = opendir(directory);
  if (NULL == stream) {
    return QST_E_PENDING_IO;
  }
  while ((entry = readdir(stream)) != NULL) {
    char path[QST_PENDING_PATH_BYTES] = {0};
    struct stat status = {0};
    int32_t formatted = 0;

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }
    formatted = snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name);
    if (formatted < 0 || (uint64_t)formatted >= sizeof(path)) {
      (void)closedir(stream);
      return QST_E_PENDING_IO;
    }
    if (lstat(path, &status) != 0) {
      /* Replay can consume an entry after readdir() exposed it but before
       * this scan reaches lstat(). */
      if (errno == ENOENT) {
        continue;
      }
      (void)closedir(stream);
      return QST_E_PENDING_IO;
    }
    if (!S_ISREG(status.st_mode)) {
      continue;
    }
    if (status.st_size < 0 || (uint64_t)status.st_size > UINT64_MAX - total) {
      (void)closedir(stream);
      return QST_E_PENDING_IO;
    }
    total += (uint64_t)status.st_size;
  }
  if (closedir(stream) != 0) {
    return QST_E_PENDING_IO;
  }
  *out_bytes = total;
  return QST_OK;
}

/* Reserves pending-file capacity once per record without rescanning a shared
 * directory. The first writer reconstructs the counter from durable files. */
static EQstCode qstPendingReserveQuota(SQstShmMapping *mapping,
                                       const SQstPendingStore *store,
                                       uint64_t bytes, int32_t *out_reserved) {
  SQstGenerationState *state = NULL;
  uint64_t used = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == out_reserved) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_reserved = 0;
  if (store->quota_bytes == 0) {
    return QST_OK;
  }
  if (NULL == mapping || qst_generator_state_get(mapping, &state) != QST_OK) {
    code = qstPendingDirectoryBytes(store->pending_dir, &used);
    if (code != QST_OK || used > store->quota_bytes ||
        bytes > store->quota_bytes - used) {
      return QST_E_PENDING_IO;
    }
    return QST_OK;
  }
  for (;;) {
    used = atomic_load_explicit(&state->pending_used_bytes,
                                memory_order_acquire);
    if (used == UINT64_MAX) {
      uint64_t scanned = 0;
      uint64_t expected = UINT64_MAX;

      if (!atomic_compare_exchange_weak_explicit(
              &state->pending_used_bytes, &expected, UINT64_MAX - 1U,
              memory_order_acq_rel, memory_order_acquire)) {
        continue;
      }
      code = qstPendingDirectoryBytes(store->pending_dir, &scanned);
      if (code != QST_OK || scanned > store->quota_bytes) {
        atomic_store_explicit(&state->pending_used_bytes, UINT64_MAX,
                              memory_order_release);
        return QST_E_PENDING_IO;
      }
      atomic_store_explicit(&state->pending_used_bytes, scanned,
                            memory_order_release);
      continue;
    }
    if (used == UINT64_MAX - 1U) {
      (void)sched_yield();
      continue;
    }
    if (used > store->quota_bytes || bytes > store->quota_bytes - used) {
      return QST_E_PENDING_IO;
    }
    if (atomic_compare_exchange_weak_explicit(
            &state->pending_used_bytes, &used, used + bytes,
            memory_order_acq_rel, memory_order_acquire)) {
      *out_reserved = 1;
      return QST_OK;
    }
  }
}

static void qstPendingReleaseQuota(SQstShmMapping *mapping, uint64_t bytes,
                                   int32_t reserved) {
  SQstGenerationState *state = NULL;

  if (reserved == 0 || NULL == mapping ||
      qst_generator_state_get(mapping, &state) != QST_OK) {
    return;
  }
  (void)atomic_fetch_sub_explicit(&state->pending_used_bytes, bytes,
                                  memory_order_acq_rel);
}

/* Read the stable's metadata rather than probing a data projection. A stable
 * projection fans out to every child table even with LIMIT 1, which is
 * prohibitively expensive for a long-lived result store. */
static EQstCode qstResultStoreStatementColumnMask(SQstNativeClient *client,
                                                  uint32_t *out_mask) {
  SQstQueryResult query = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *rows = NULL;
  int32_t field_index = -1;
  char *query_text = NULL;
  EQstCode code = QST_OK;

  if (client == NULL || out_mask == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_mask = 0;
  code = qst_native_query_result(client, "DESCRIBE res_db.statements", &query);
  if (code != QST_OK || query.data == NULL || query.bytes == 0 ||
      query.bytes >= SIZE_MAX) {
    qst_result_reset(&query);
    return code == QST_OK ? QST_E_RESULT_STORE : code;
  }
  query_text = (char *)calloc((size_t)query.bytes + 1U, 1);
  if (query_text == NULL) {
    qst_result_reset(&query);
    return QST_E_RESOURCE;
  }
  memcpy(query_text, query.data, (size_t)query.bytes);
  root = tools_cJSON_ParseWithOpts(query_text, NULL, 1);
  columns = tools_cJSON_GetObjectItemCaseSensitive(root, "columns");
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsArray(columns) ||
      !tools_cJSON_IsArray(rows)) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  for (int32_t index = 0; index < tools_cJSON_GetArraySize(columns); ++index) {
    const tools_cJSON *column = tools_cJSON_GetArrayItem(columns, index);
    const tools_cJSON *name =
        column == NULL ? NULL : tools_cJSON_GetObjectItemCaseSensitive(column, "name");
    if (name != NULL && tools_cJSON_IsString(name) && name->valuestring != NULL &&
        strcmp(name->valuestring, "field") == 0) {
      field_index = index;
      break;
    }
  }
  if (field_index < 0) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  for (int32_t row_index = 0; row_index < tools_cJSON_GetArraySize(rows); ++row_index) {
    const tools_cJSON *row = tools_cJSON_GetArrayItem(rows, row_index);
    const tools_cJSON *cell =
        row == NULL ? NULL : tools_cJSON_GetArrayItem(row, field_index);
    const tools_cJSON *value =
        cell == NULL ? NULL : tools_cJSON_GetObjectItemCaseSensitive(cell, "value");

    if (!tools_cJSON_IsArray(row) || cell == NULL || !tools_cJSON_IsObject(cell) ||
        value == NULL || !tools_cJSON_IsString(value) || value->valuestring == NULL) {
      code = QST_E_RESULT_STORE;
      goto _return;
    }
    for (uint32_t column_index = 0;
         column_index < sizeof(kResultStoreStatementColumns) /
                            sizeof(kResultStoreStatementColumns[0]);
         ++column_index) {
      if (strcmp(value->valuestring,
                 kResultStoreStatementColumns[column_index].name) == 0) {
        *out_mask |= kResultStoreStatementColumns[column_index].bit;
      }
    }
  }

_return:
  free(query_text);
  tools_cJSON_Delete(root);
  qst_result_reset(&query);
  return code;
}

static EQstCode qstResultStoreEnsureStatementColumns(SQstNativeClient *client) {
  char alter[256] = {0};
  uint32_t present_mask = 0;
  uint32_t required_mask = 0;
  int32_t changed = 0;
  EQstCode code = QST_OK;

  if (client == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0;
       index < sizeof(kResultStoreStatementColumns) /
                   sizeof(kResultStoreStatementColumns[0]);
       ++index) {
    required_mask |= kResultStoreStatementColumns[index].bit;
  }
  code = qstResultStoreStatementColumnMask(client, &present_mask);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0;
       index < sizeof(kResultStoreStatementColumns) /
                   sizeof(kResultStoreStatementColumns[0]);
       ++index) {
    const SQstResultStoreStatementColumn *column =
        &kResultStoreStatementColumns[index];
    int32_t written = 0;

    if ((present_mask & column->bit) != 0) {
      continue;
    }
    written = snprintf(alter, sizeof(alter),
                       "ALTER STABLE res_db.statements ADD COLUMN %s %s",
                       column->name, column->type);
    if (written < 0 || (uint64_t)written >= sizeof(alter)) {
      return QST_E_RESOURCE;
    }
    if (qst_native_execute(client, alter) != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: result statement schema migration failed "
                    "column=%s\n",
                    column->name);
      return QST_E_RESULT_STORE;
    }
    changed = 1;
  }
  if (changed != 0) {
    code = qstResultStoreStatementColumnMask(client, &present_mask);
    if (code != QST_OK) {
      return code;
    }
  }
  if (present_mask != required_mask) {
    (void)fprintf(stderr,
                  "taosQueryTest: result statement schema verification failed "
                  "present_mask=%u required_mask=%u\n",
                  present_mask, required_mask);
    return QST_E_RESULT_STORE;
  }
  return QST_OK;
}

static EQstCode qstResultStoreEnsureSchema(SQstNativeClient *client) {
  static const char *const kSchemaStatements[] = {
      "CREATE DATABASE IF NOT EXISTS res_db",
      "CREATE TABLE IF NOT EXISTS res_db.datasets "
      "(ts TIMESTAMP, ds_fingerprint BINARY(64), config_json BINARY(8192), "
      "fingerprint_format BINARY(1024))",
      "CREATE TABLE IF NOT EXISTS res_db.runs "
      "(begin_ts TIMESTAMP, run_id BIGINT UNSIGNED, end_ts TIMESTAMP, status TINYINT, "
      "random_seed BIGINT UNSIGNED, mode BINARY(16), focus BINARY(16), "
      "target_set BINARY(64), ds_fingerprint BINARY(64), tool_version BINARY(64), "
      "stop_reason BINARY(32), parallel_test_info BINARY(8192), "
      "planned_statement_count BIGINT, executed_statement_count BIGINT, "
      "succeeded_statement_count BIGINT, execution_error_statement_count BIGINT, "
      "timeout_statement_count BIGINT, cancelled_statement_count BIGINT, "
      "passed_statement_count BIGINT, failed_statement_count BIGINT, "
      "unverified_statement_count BIGINT, error_code INT, error_message BINARY(1024))",
      "CREATE STABLE IF NOT EXISTS res_db.statements "
      "(ts TIMESTAMP, gid BIGINT, ended_at TIMESTAMP, sid BIGINT, "
      "sql_text BINARY(8192), sql_sha256 BINARY(64), sql_shape_mask SMALLINT, "
      "taosd_pid BINARY(1024), "
      "taosc_pid BINARY(1024), execution_status TINYINT, result_row_count BIGINT, "
      "error_code INT, error_message BINARY(1024), taosd_log_ranges BINARY(1024), "
      "taosc_log_ranges BINARY(1024), core_relative_path BINARY(512), "
      "dialect TINYINT, generation_abnormal TINYINT, "
      "generation_unsupported_fallback TINYINT) "
      "TAGS (run_id BIGINT UNSIGNED, target BINARY(16), ds_fingerprint BINARY(64))",
      "CREATE STABLE IF NOT EXISTS res_db.results "
      "(ts TIMESTAMP, gid BIGINT, verdict TINYINT, validator BINARY(32), "
      "diff_summary BINARY(4096), error_code INT, error_message BINARY(1024), "
      "result_content BLOB, result_storage_status BINARY(24), "
      "analysis_content BINARY(8192)) "
      "TAGS (run_id BIGINT UNSIGNED, target BINARY(16), ds_fingerprint BINARY(64))",
      "CREATE STABLE IF NOT EXISTS res_db.historical_verdicts "
      "(ts TIMESTAMP, sql_sha256 BINARY(64), sql_text BINARY(8192), "
      "result_row_count BIGINT, result_content BLOB, validator BINARY(32), "
      "first_pass_version BINARY(64)) "
      "TAGS (ds_fingerprint BINARY(64), target BINARY(16))",
      /* Compatibility ledger remains until all persistence payloads carry every
       * business field needed by the five FS-defined object writers. */
      "CREATE TABLE IF NOT EXISTS res_db.qst_pending_records "
      "(ts TIMESTAMP, run_id BIGINT UNSIGNED, record_seq BIGINT UNSIGNED, persist_kind INT, "
      "target INT, gid BIGINT UNSIGNED, stable_key BINARY(512), payload_sha256 "
      "BINARY(64), payload_hex BINARY(16384))",
  };

  if (NULL == client) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0;
       index < sizeof(kSchemaStatements) / sizeof(kSchemaStatements[0]); ++index) {
    const EQstCode code = qst_native_execute(client, kSchemaStatements[index]);

    if (code != QST_OK) {
      (void)fprintf(stderr, "taosQueryTest: result schema statement index=%u code=%d\n",
                    index, code);
      return code;
    }
  }
  if (qstResultStoreEnsureStatementColumns(client) != QST_OK) {
    return QST_E_RESULT_STORE;
  }
  return QST_OK;
}

static const char *qstResultStoreTargetName(uint32_t target_bit) {
  switch (target_bit) {
    case 1U << 0:
      return "td";
    case 1U << 1:
      return "mysql";
    case 1U << 2:
      return "pg";
    case 1U << 3:
      return "influx";
    default:
      return NULL;
  }
}

static const char *qstResultStoreTargetNameById(uint32_t target) {
  return target >= QST_TARGET_TD && target <= QST_TARGET_INFLUX ?
             qstResultStoreTargetName(1U << (target - 1U)) : NULL;
}

/* Slot zero keeps the original table name for compatibility. Every other
 * judge owns a child table so concurrent statement batches do not serialize
 * on one TDengine child-table write path. */
static EQstCode qstResultStoreTaskTableName(char *out_table,
                                            uint64_t out_table_bytes,
                                            const char *kind,
                                            uint64_t run_id,
                                            const char *target_name,
                                            uint32_t worker_slot) {
  int32_t written = 0;

  if (NULL == out_table || out_table_bytes == 0 || NULL == kind ||
      NULL == target_name || run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  written = worker_slot == 0 ?
      snprintf(out_table, (size_t)out_table_bytes, "res_db.%s_%llu_%s", kind,
               (unsigned long long)run_id, target_name) :
      snprintf(out_table, (size_t)out_table_bytes, "res_db.%s_%llu_%s_w%u", kind,
               (unsigned long long)run_id, target_name, worker_slot);
  return written < 0 || (uint64_t)written >= out_table_bytes ? QST_E_RESOURCE :
                                                                QST_OK;
}

/* Validates the suffix after a task-table prefix in a pending DML and returns
 * the number of tables that must exist before replay. Pending files contain
 * complete DML, so this is the only durable routing metadata available after
 * a judge exits. */
static EQstCode qstResultStorePendingTaskTableWorkerCount(
    const char *suffix, uint32_t *out_worker_count) {
  static const char kValuesPrefix[] = " VALUES (";
  uint64_t worker_slot = 0;
  uint32_t digits = 0;

  if (NULL == suffix || NULL == out_worker_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (strncmp(suffix, kValuesPrefix, sizeof(kValuesPrefix) - 1U) == 0) {
    *out_worker_count = 1U;
    return QST_OK;
  }
  if (suffix[0] != '_' || suffix[1] != 'w') {
    return QST_E_SHM_CORRUPT;
  }
  suffix += 2;
  while (*suffix >= '0' && *suffix <= '9') {
    if (worker_slot > (UINT32_MAX - (uint32_t)(*suffix - '0')) / 10U) {
      return QST_E_SHM_CORRUPT;
    }
    worker_slot = worker_slot * 10U + (uint32_t)(*suffix - '0');
    ++suffix;
    ++digits;
  }
  if (digits == 0 || worker_slot == 0 || worker_slot >= UINT32_MAX ||
      strncmp(suffix, kValuesPrefix, sizeof(kValuesPrefix) - 1U) != 0) {
    return QST_E_SHM_CORRUPT;
  }
  *out_worker_count = (uint32_t)worker_slot + 1U;
  return QST_OK;
}

static EQstCode qstResultStoreDigestFromHex(const char *text,
                                            SQstDigest256 *out_digest) {
  if (NULL == text || NULL == out_digest || strlen(text) != 64U) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < sizeof(out_digest->bytes); ++index) {
    uint8_t high = 0;
    uint8_t low = 0;
    if (!qstResultStoreHexValue(text[index * 2U], &high) ||
        !qstResultStoreHexValue(text[index * 2U + 1U], &low)) {
      return QST_E_INVALID_ARGUMENT;
    }
    out_digest->bytes[index] = (uint8_t)((high << 4U) | low);
  }
  return QST_OK;
}

EQstCode qst_result_store_prepare_run(SQstResultStore *store,
                                      uint32_t target_selection_bits,
                                      SQstDigest256 ds_fingerprint,
                                      uint32_t worker_count) {
  SQstNativeClient *client = NULL;
  char fingerprint_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};

  if (NULL == store || NULL == store->native_taos || store->run_id == 0 ||
      target_selection_bits == 0 || worker_count == 0 ||
      (target_selection_bits & ~UINT32_C(0x0f)) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  client = (SQstNativeClient *)store->native_taos;
  if (qstResultStoreEnsureSchema(client) != QST_OK) {
    (void)fprintf(stderr, "taosQueryTest: result schema preparation failed\n");
    return QST_E_RESULT_STORE;
  }
  qstPendingHex(ds_fingerprint.bytes, sizeof(ds_fingerprint.bytes), fingerprint_hex);
  for (uint32_t target_bit = 1; target_bit <= (1U << 3); target_bit <<= 1U) {
    const char *target_name = NULL;

    if ((target_selection_bits & target_bit) == 0) {
      continue;
    }
    target_name = qstResultStoreTargetName(target_bit);
    if (NULL == target_name) {
      return QST_E_INVALID_ARGUMENT;
    }
    for (uint32_t worker_slot = 0; worker_slot < worker_count; ++worker_slot) {
      char statement[1024] = {0};
      char statement_table[256] = {0};
      char result_table[256] = {0};
      int32_t written = 0;

      if (qstResultStoreTaskTableName(statement_table, sizeof(statement_table),
                                      "stmt", store->run_id, target_name,
                                      worker_slot) != QST_OK ||
          qstResultStoreTaskTableName(result_table, sizeof(result_table),
                                      "result", store->run_id, target_name,
                                      worker_slot) != QST_OK) {
        return QST_E_RESOURCE;
      }
      written = snprintf(statement, sizeof(statement),
                         "CREATE TABLE IF NOT EXISTS %s USING res_db.statements "
                         "TAGS (%llu,'%s','%s')",
                         statement_table, (unsigned long long)store->run_id,
                         target_name, fingerprint_hex);
      if (written < 0 || (uint64_t)written >= sizeof(statement) ||
          qst_native_execute(client, statement) != QST_OK) {
        return QST_E_RESULT_STORE;
      }
      written = snprintf(statement, sizeof(statement),
                         "CREATE TABLE IF NOT EXISTS %s USING res_db.results "
                         "TAGS (%llu,'%s','%s')",
                         result_table, (unsigned long long)store->run_id,
                         target_name, fingerprint_hex);
      if (written < 0 || (uint64_t)written >= sizeof(statement) ||
          qst_native_execute(client, statement) != QST_OK) {
        return QST_E_RESULT_STORE;
      }
    }
    {
      char statement[1024] = {0};
      const int32_t written = snprintf(
          statement, sizeof(statement),
          "CREATE TABLE IF NOT EXISTS res_db.verdict_%s_%s USING "
          "res_db.historical_verdicts TAGS ('%s','%s')",
          fingerprint_hex, target_name, fingerprint_hex, target_name);
      if (written < 0 || (uint64_t)written >= sizeof(statement) ||
          qst_native_execute(client, statement) != QST_OK) {
        return QST_E_RESULT_STORE;
      }
    }
  }
  return QST_OK;
}

static EQstCode qstResultStoreCopyHex(const SQstShmMapping *mapping,
                                      SQstRef ref, uint64_t owner_gid,
                                      char **out_hex) {
  uint8_t *bytes = NULL;
  char *hex = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_hex || qstRefIsEmpty(ref) ||
      ref.length > UINT32_MAX / 2U || ref.length > SIZE_MAX / 2U) {
    return QST_E_INVALID_ARGUMENT;
  }
  bytes = (uint8_t *)calloc(ref.length, 1);
  hex = (char *)calloc((size_t)ref.length * 2U + 1U, 1);
  if (NULL == bytes || NULL == hex) {
    free(bytes);
    free(hex);
    return QST_E_RESOURCE;
  }
  code = qst_block_copy(mapping, ref, owner_gid, 0, bytes, ref.length);
  if (code == QST_OK) {
    qstPendingHex(bytes, ref.length, hex);
    *out_hex = hex;
  } else {
    free(hex);
  }
  free(bytes);
  return code;
}

static EQstCode qstResultStoreCopyText(const SQstShmMapping *mapping, SQstRef ref,
                                       uint64_t owner_gid, char **out_text) {
  char *text = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_text || qstRefIsEmpty(ref) ||
      ref.length >= SIZE_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  text = (char *)calloc((size_t)ref.length + 1U, 1);
  if (NULL == text) {
    return QST_E_RESOURCE;
  }
  code = qst_block_copy(mapping, ref, owner_gid, 0, text, ref.length);
  if (code != QST_OK || memchr(text, '\0', ref.length) != NULL) {
    free(text);
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  *out_text = text;
  return QST_OK;
}

static char *qstResultStoreEscapeText(const char *text) {
  uint64_t bytes = 0;
  uint64_t escaped_bytes = 0;
  char *escaped = NULL;

  if (NULL == text) {
    return NULL;
  }
  bytes = strlen(text);
  if (bytes > (SIZE_MAX - 1U) / 2U) {
    return NULL;
  }
  for (uint64_t index = 0; index < bytes; ++index) {
    const uint64_t output_bytes =
        text[index] == '\'' || text[index] == '\\' ? 2U : 1U;

    if (escaped_bytes > UINT64_MAX - output_bytes) {
      return NULL;
    }
    escaped_bytes += output_bytes;
  }
  if (escaped_bytes >= SIZE_MAX) {
    return NULL;
  }
  escaped = (char *)calloc((size_t)escaped_bytes + 1U, 1);
  if (NULL == escaped) {
    return NULL;
  }
  for (uint64_t source = 0, target = 0; source < bytes; ++source) {
    escaped[target++] = text[source];
    if (text[source] == '\'' || text[source] == '\\') {
      escaped[target++] = text[source];
    }
  }
  return escaped;
}

/* Appends one complete overflow object under an advisory process lock. */
EQstCode qst_result_store_write_overflow(SQstResultStore *store,
                                         const char *table, const char *column,
                                         const char *encoding,
                                         const uint8_t *content,
                                         uint64_t content_bytes,
                                         char *out_reference,
                                         uint64_t reference_bytes) {
  char *escaped = NULL;
  char *line = NULL;
  uint64_t escaped_bytes = 0;
  uint64_t line_bytes = 0;
  uint64_t line_number = 1;
  int32_t fd = -1;
  EQstCode code = QST_OK;

  if (NULL == store || store->overflow_path[0] == '\0' || NULL == table ||
      NULL == column || NULL == encoding || NULL == content || NULL == out_reference ||
      reference_bytes == 0 || content_bytes > (SIZE_MAX - 1U) / 6U) {
    return QST_E_INVALID_ARGUMENT;
  }
  escaped = (char *)calloc((size_t)content_bytes * 6U + 1U, 1);
  if (NULL == escaped) {
    return QST_E_RESOURCE;
  }
  for (uint64_t index = 0; index < content_bytes; ++index) {
    const uint8_t value = content[index];
    if (value == '"' || value == '\\') {
      escaped[escaped_bytes++] = '\\';
      escaped[escaped_bytes++] = (char)value;
    } else if (value >= 0x20U) {
      escaped[escaped_bytes++] = (char)value;
    } else {
      (void)snprintf(escaped + escaped_bytes, 7U, "\\u%04x", value);
      escaped_bytes += 6U;
    }
  }
  if (escaped_bytes > UINT64_MAX - strlen(table) - strlen(column) -
                          strlen(encoding) - 64U ||
      escaped_bytes + strlen(table) + strlen(column) + strlen(encoding) + 64U >= SIZE_MAX) {
    free(escaped);
    return QST_E_RESOURCE;
  }
  line_bytes = escaped_bytes + strlen(table) + strlen(column) + strlen(encoding) + 64U;
  line = (char *)calloc((size_t)line_bytes, 1);
  if (NULL == line) {
    free(escaped);
    return QST_E_RESOURCE;
  }
  fd = open(store->overflow_path, O_RDWR | O_CREAT | O_APPEND, 0640);
  if (fd < 0 || flock(fd, LOCK_EX) != 0) {
    free(line);
    free(escaped);
    if (fd >= 0) {
      (void)close(fd);
    }
    return QST_E_PENDING_IO;
  }
  {
    char buffer[4096] = {0};
    ssize_t read_bytes = 0;
    if (lseek(fd, 0, SEEK_SET) < 0) {
      code = QST_E_PENDING_IO;
    }
    while (code == QST_OK && (read_bytes = read(fd, buffer, sizeof(buffer))) > 0) {
      for (ssize_t index = 0; index < read_bytes; ++index) {
        if (buffer[index] == '\n') {
          ++line_number;
        }
      }
    }
    if (read_bytes < 0) {
      code = QST_E_PENDING_IO;
    }
  }
  if (code == QST_OK && snprintf(line, (size_t)line_bytes,
                                 "{\"table\":\"%s\",\"column\":\"%s\",\"encoding\":\"%s\",\"content\":\"%s\"}\n",
                                 table, column, encoding, escaped) < 0) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qstPendingWriteAll(fd, (const uint8_t *)line, strlen(line));
  }
  if (flock(fd, LOCK_UN) != 0 && code == QST_OK) {
    code = QST_E_PENDING_IO;
  }
  if (close(fd) != 0 && code == QST_OK) {
    code = QST_E_PENDING_IO;
  }
  if (code == QST_OK) {
    const int32_t written = snprintf(out_reference, (size_t)reference_bytes,
                                     "@overflow:%llu",
                                     (unsigned long long)line_number);

    if (written < 0 || (uint64_t)written >= reference_bytes) {
      code = QST_E_RESOURCE;
    }
  }
  free(line);
  free(escaped);
  return code;
}

/* Builds a quoted result-store literal, preserving oversized content in the
 * shared overflow journal instead of truncating it for a BINARY column. */
static EQstCode qstResultStoreMakeTextLiteral(SQstResultStore *store,
                                              const char *table,
                                              const char *column,
                                              const char *text,
                                              uint64_t column_bytes,
                                              char **out_literal) {
  const char *stored_text = text;
  char overflow_reference[64] = {0};
  char *escaped = NULL;
  char *literal = NULL;
  uint64_t literal_bytes = 0;

  if (NULL == store || NULL == table || NULL == column || NULL == out_literal ||
      column_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_literal = NULL;
  if (NULL == text) {
    literal = strdup("NULL");
    if (NULL == literal) {
      return QST_E_RESOURCE;
    }
    *out_literal = literal;
    return QST_OK;
  }
  if (strlen(text) > column_bytes) {
    const EQstCode code = qst_result_store_write_overflow(
        store, table, column, "utf8", (const uint8_t *)text, strlen(text),
        overflow_reference, sizeof(overflow_reference));

    if (code != QST_OK) {
      return code;
    }
    stored_text = overflow_reference;
  }
  escaped = qstResultStoreEscapeText(stored_text);
  if (NULL == escaped || strlen(escaped) > UINT64_MAX - 3U ||
      strlen(escaped) + 3U >= SIZE_MAX) {
    free(escaped);
    return QST_E_RESOURCE;
  }
  literal_bytes = strlen(escaped) + 3U;
  literal = (char *)calloc((size_t)literal_bytes, 1);
  if (NULL == literal) {
    free(escaped);
    return QST_E_RESOURCE;
  }
  if (snprintf(literal, (size_t)literal_bytes, "'%s'", escaped) < 0) {
    free(literal);
    free(escaped);
    return QST_E_RESOURCE;
  }
  free(escaped);
  *out_literal = literal;
  return QST_OK;
}

static EQstCode qstResultStoreBuildDatasetDml(SQstResultStore *store,
                                              const SQstDatasetRecord *record,
                                              char **out_dml) {
  char *fingerprint = NULL;
  char *config_json = NULL;
  char *format = NULL;
  char *dml = NULL;
  uint64_t dml_bytes = 0;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == record || NULL == record->ds_fingerprint ||
      record->defined_ts_ns <= 0 || strlen(record->ds_fingerprint) != 64U ||
      NULL == record->config_json || NULL == record->fingerprint_format ||
      NULL == out_dml) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_dml = NULL;
  code = qstResultStoreMakeTextLiteral(store, "datasets", "ds_fingerprint",
                                       record->ds_fingerprint, 64U, &fingerprint);
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "datasets", "config_json",
                                         record->config_json, 8192U, &config_json);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "datasets", "fingerprint_format",
                                         record->fingerprint_format, 1024U, &format);
  }
  if (code != QST_OK || strlen(fingerprint) > UINT64_MAX - strlen(config_json) -
                                             strlen(format) - 128U ||
      strlen(fingerprint) + strlen(config_json) + strlen(format) + 128U >= SIZE_MAX) {
    free(format);
    free(config_json);
    free(fingerprint);
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  dml_bytes = strlen(fingerprint) + strlen(config_json) + strlen(format) + 128U;
  dml = (char *)calloc((size_t)dml_bytes, 1);
  if (NULL == dml) {
    free(format);
    free(config_json);
    free(fingerprint);
    return QST_E_RESOURCE;
  }
  written = snprintf(dml, (size_t)dml_bytes,
                     "INSERT INTO res_db.datasets VALUES (%lld,%s,%s,%s)",
                     (long long)(record->defined_ts_ns / INT64_C(1000000)),
                     fingerprint, config_json, format);
  free(format);
  free(config_json);
  free(fingerprint);
  if (written < 0 || (uint64_t)written >= dml_bytes) {
    free(dml);
    return QST_E_RESOURCE;
  }
  *out_dml = dml;
  return QST_OK;
}

static EQstCode qstResultStoreBuildRunDml(SQstResultStore *store,
                                          const SQstRunRecord *record,
                                          char **out_dml) {
  char *mode = NULL;
  char *focus = NULL;
  char *target_set = NULL;
  char *fingerprint = NULL;
  char *tool_version = NULL;
  char *stop_reason = NULL;
  char *parallel_info = NULL;
  char *error_message = NULL;
  char *dml = NULL;
  char random_seed[32] = {0};
  char end_ts[32] = {0};
  char error_code[32] = {0};
  char planned_count[32] = {0};
  uint64_t dml_bytes = 0;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == record || NULL == record->mode ||
      NULL == record->focus || NULL == record->target_set ||
      NULL == record->tool_version || record->begin_ts_ns <= 0 ||
      record->status > 3U || record->planned_statement_count < -1 ||
      record->executed_statement_count < 0 ||
      record->succeeded_statement_count < 0 ||
      record->execution_error_statement_count < 0 ||
      record->timeout_statement_count < 0 ||
      record->cancelled_statement_count < 0 || record->passed_statement_count < 0 ||
      record->failed_statement_count < 0 || record->unverified_statement_count < 0 ||
      NULL == out_dml) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != record->ds_fingerprint && strlen(record->ds_fingerprint) != 64U) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_dml = NULL;
  if (record->end_ts_ns < 0 ||
      (record->end_ts_ns != 0 && record->end_ts_ns < record->begin_ts_ns)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstResultStoreMakeTextLiteral(store, "runs", "mode", record->mode, 16U,
                                       &mode);
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "focus", record->focus,
                                         16U, &focus);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "target_set",
                                         record->target_set, 64U, &target_set);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "ds_fingerprint",
                                         record->ds_fingerprint, 64U, &fingerprint);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "tool_version",
                                         record->tool_version, 64U, &tool_version);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "stop_reason",
                                         record->stop_reason, 32U, &stop_reason);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "parallel_test_info",
                                         record->parallel_test_info, 8192U,
                                         &parallel_info);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "runs", "error_message",
                                         record->error_message, 1024U,
                                         &error_message);
  }
  if (code != QST_OK) {
    goto _return;
  }
  (void)snprintf(random_seed, sizeof(random_seed), "%s%llu",
                 record->has_random_seed != 0 ? "" : "NULL",
                 (unsigned long long)record->random_seed);
  if (record->has_random_seed == 0) {
    (void)snprintf(random_seed, sizeof(random_seed), "NULL");
  }
  if (record->end_ts_ns == 0) {
    (void)snprintf(end_ts, sizeof(end_ts), "NULL");
  } else {
    (void)snprintf(end_ts, sizeof(end_ts), "%lld",
                   (long long)(record->end_ts_ns / INT64_C(1000000)));
  }
  if (record->has_error == 0) {
    (void)snprintf(error_code, sizeof(error_code), "NULL");
  } else {
    (void)snprintf(error_code, sizeof(error_code), "%d", record->error_code);
  }
  if (record->planned_statement_count < 0) {
    (void)snprintf(planned_count, sizeof(planned_count), "NULL");
  } else {
    (void)snprintf(planned_count, sizeof(planned_count), "%lld",
                   (long long)record->planned_statement_count);
  }
  if (strlen(mode) > UINT64_MAX - strlen(focus) - strlen(target_set) -
                         strlen(fingerprint) - strlen(tool_version) -
                         strlen(stop_reason) - strlen(parallel_info) -
                         strlen(error_message) - 512U ||
      strlen(mode) + strlen(focus) + strlen(target_set) + strlen(fingerprint) +
              strlen(tool_version) + strlen(stop_reason) + strlen(parallel_info) +
              strlen(error_message) + 512U >= SIZE_MAX) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  dml_bytes = strlen(mode) + strlen(focus) + strlen(target_set) +
      strlen(fingerprint) + strlen(tool_version) + strlen(stop_reason) +
      strlen(parallel_info) + strlen(error_message) + 512U;
  dml = (char *)calloc((size_t)dml_bytes, 1);
  if (NULL == dml) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  written = snprintf(
      dml, (size_t)dml_bytes,
      "INSERT INTO res_db.runs VALUES (%lld,%llu,%s,%u,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
      "%lld,%lld,%lld,%lld,%lld,%lld,%lld,%lld,%s,%s)",
      (long long)(record->begin_ts_ns / INT64_C(1000000)),
      (unsigned long long)store->run_id, end_ts, record->status, random_seed,
      mode, focus, target_set, fingerprint, tool_version, stop_reason,
      parallel_info, planned_count, (long long)record->executed_statement_count,
      (long long)record->succeeded_statement_count,
      (long long)record->execution_error_statement_count,
      (long long)record->timeout_statement_count,
      (long long)record->cancelled_statement_count,
      (long long)record->passed_statement_count,
      (long long)record->failed_statement_count,
      (long long)record->unverified_statement_count, error_code, error_message);
  if (written < 0 || (uint64_t)written >= dml_bytes) {
    free(dml);
    dml = NULL;
    code = QST_E_RESOURCE;
    goto _return;
  }
  *out_dml = dml;
  dml = NULL;
_return:
  free(dml);
  free(error_message);
  free(parallel_info);
  free(stop_reason);
  free(tool_version);
  free(fingerprint);
  free(target_set);
  free(focus);
  free(mode);
  return code;
}

static EQstCode qstResultStoreWriteObjectRecord(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, uint32_t persist_kind,
    const char *ds_fingerprint, const char *stable_key, uint64_t record_seq,
    char *dml, uint32_t *out_durable_source) {
  SQstPendingRecord pending_record = {0};
  SQstRef pending_file_ref = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == store ||
      NULL == stable_key || NULL == dml || NULL == out_durable_source ||
      record_seq == 0 || (persist_kind != QST_PERSIST_RUN_UPDATE &&
                          persist_kind != QST_PERSIST_DATASET)) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_durable_source = 0;
  if (qstResultStoreExecuteWithRecovery(mapping, store, dml, record_seq) == QST_OK) {
    *out_durable_source = QST_DURABLE_RESULT_STORE;
    return QST_OK;
  }
  if (NULL == pending_store || NULL == pending_store->pending_dir ||
      pending_store->pending_dir[0] == '\0') {
    return QST_E_RESULT_STORE;
  }
  pending_record.persist_kind = persist_kind;
  pending_record.run_id = store->run_id;
  pending_record.record_seq = record_seq;
  pending_record.ds_fingerprint = ds_fingerprint;
  pending_record.stable_key = stable_key;
  pending_record.payload = dml;
  pending_record.payload_bytes = strlen(dml);
  code = qst_pending_write(mapping, pending_store, &pending_record, &pending_file_ref);
  if (code != QST_OK) {
    return code;
  }
  code = qst_block_release(mapping, pending_file_ref, 0);
  if (code != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  *out_durable_source = QST_DURABLE_PENDING_FILE;
  return QST_OK;
}

EQstCode qst_result_store_write_dataset_record(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, const SQstDatasetRecord *record,
    uint64_t record_seq, uint32_t *out_durable_source) {
  char stable_key[96] = {0};
  char *dml = NULL;
  EQstCode code = QST_OK;

  if (NULL == record || NULL == record->ds_fingerprint ||
      strlen(record->ds_fingerprint) != 64U) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(stable_key, sizeof(stable_key), "datasets|%s",
               record->ds_fingerprint) < 0 || strlen(stable_key) >= sizeof(stable_key)) {
    return QST_E_RESOURCE;
  }
  code = qstResultStoreBuildDatasetDml(store, record, &dml);
  if (code == QST_OK) {
    code = qstResultStoreWriteObjectRecord(mapping, store, pending_store,
                                           QST_PERSIST_DATASET,
                                           record->ds_fingerprint, stable_key,
                                           record_seq, dml, out_durable_source);
  }
  free(dml);
  return code;
}

EQstCode qst_result_store_write_run_record(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, const SQstRunRecord *record,
    uint64_t record_seq, uint32_t *out_durable_source) {
  char stable_key[96] = {0};
  char *dml = NULL;
  EQstCode code = QST_OK;

  if (NULL == store || store->run_id == 0 || NULL == record || record_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(stable_key, sizeof(stable_key), "runs|%llu|%llu",
               (unsigned long long)store->run_id,
               (unsigned long long)record_seq) < 0 ||
      strlen(stable_key) >= sizeof(stable_key)) {
    return QST_E_RESOURCE;
  }
  code = qstResultStoreBuildRunDml(store, record, &dml);
  if (code == QST_OK) {
    code = qstResultStoreWriteObjectRecord(mapping, store, pending_store,
                                           QST_PERSIST_RUN_UPDATE,
                                           record->ds_fingerprint, stable_key,
                                           record_seq, dml, out_durable_source);
  }
  free(dml);
  return code;
}

static int32_t qstResultStoreHexValue(char value, uint8_t *out_value) {
  if (NULL == out_value) {
    return 0;
  }
  if (value >= '0' && value <= '9') {
    *out_value = (uint8_t)(value - '0');
    return 1;
  }
  if (value >= 'a' && value <= 'f') {
    *out_value = (uint8_t)(value - 'a' + 10);
    return 1;
  }
  return 0;
}

static EQstCode qstResultStoreDecodeHexResult(const char *hex,
                                              SQstQueryResult *out_result) {
  uint64_t hex_bytes = 0;
  uint8_t *data = NULL;

  if (NULL == hex || NULL == out_result) {
    return QST_E_INVALID_ARGUMENT;
  }
  hex_bytes = strlen(hex);
  if (hex_bytes == 0 || (hex_bytes & 1U) != 0 || hex_bytes / 2U >= SIZE_MAX) {
    return QST_E_SHM_CORRUPT;
  }
  data = (uint8_t *)calloc((size_t)(hex_bytes / 2U) + 1U, 1);
  if (NULL == data) {
    return QST_E_RESOURCE;
  }
  for (uint64_t index = 0; index < hex_bytes; index += 2U) {
    uint8_t high = 0;
    uint8_t low = 0;

    if (!qstResultStoreHexValue(hex[index], &high) ||
        !qstResultStoreHexValue(hex[index + 1U], &low)) {
      free(data);
      return QST_E_SHM_CORRUPT;
    }
    data[index / 2U] = (uint8_t)((high << 4U) | low);
  }
  if (memchr(data, '\0', (size_t)(hex_bytes / 2U)) != NULL) {
    free(data);
    return QST_E_SHM_CORRUPT;
  }
  qst_result_reset(out_result);
  out_result->data = data;
  out_result->bytes = hex_bytes / 2U;
  return QST_OK;
}

EQstCode qst_result_store_lookup_historical(
    SQstResultStore *store, const char *ds_fingerprint, uint32_t target,
    SQstDigest256 sql_sha256, SQstQueryResult *out_result,
    char *out_validator, uint64_t validator_bytes, int32_t *out_found) {
  SQstQueryResult query = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *rows = NULL;
  tools_cJSON *row = NULL;
  tools_cJSON *content = NULL;
  tools_cJSON *validator = NULL;
  tools_cJSON *content_kind = NULL;
  tools_cJSON *content_value = NULL;
  tools_cJSON *validator_kind = NULL;
  tools_cJSON *validator_value = NULL;
  const char *target_name = NULL;
  char fingerprint_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};
  char sql_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};
  char statement[512] = {0};
  char *query_text = NULL;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == store->native_taos || NULL == ds_fingerprint ||
      strlen(ds_fingerprint) != QST_PENDING_DIGEST_HEX_BYTES || NULL == out_result ||
      NULL == out_validator || validator_bytes == 0 || NULL == out_found) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < QST_PENDING_DIGEST_HEX_BYTES; ++index) {
    uint8_t ignored = 0;

    if (!qstResultStoreHexValue(ds_fingerprint[index], &ignored)) {
      return QST_E_INVALID_ARGUMENT;
    }
  }
  target_name = qstResultStoreTargetNameById(target);
  if (NULL == target_name) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < sizeof(sql_sha256.bytes); ++index) {
    const uint8_t high = sql_sha256.bytes[index] >> 4U;
    const uint8_t low = sql_sha256.bytes[index] & 0x0fU;
    static const char kHex[] = "0123456789abcdef";

    sql_hex[index * 2U] = kHex[high];
    sql_hex[index * 2U + 1U] = kHex[low];
  }
  memcpy(fingerprint_hex, ds_fingerprint, sizeof(fingerprint_hex) - 1U);
  *out_found = 0;
  out_validator[0] = '\0';
  qst_result_reset(out_result);
  written = snprintf(
      statement, sizeof(statement),
      "SELECT result_content,validator FROM res_db.verdict_%s_%s "
      "WHERE sql_sha256='%s' LIMIT 1",
      fingerprint_hex, target_name, sql_hex);
  if (written < 0 || (uint64_t)written >= sizeof(statement)) {
    return QST_E_RESOURCE;
  }
  code = qst_native_query_result((SQstNativeClient *)store->native_taos,
                                 statement, &query);
  if (code != QST_OK || query.bytes >= SIZE_MAX) {
    qst_result_reset(&query);
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  query_text = (char *)calloc((size_t)query.bytes + 1U, 1);
  if (NULL == query_text) {
    qst_result_reset(&query);
    return QST_E_RESOURCE;
  }
  memcpy(query_text, query.data, (size_t)query.bytes);
  root = tools_cJSON_ParseWithOpts(query_text, NULL, 1);
  columns = tools_cJSON_GetObjectItemCaseSensitive(root, "columns");
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsArray(columns) ||
      !tools_cJSON_IsArray(rows) || tools_cJSON_GetArraySize(columns) != 2) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  if (tools_cJSON_GetArraySize(rows) == 0) {
    code = QST_OK;
    goto _return;
  }
  if (tools_cJSON_GetArraySize(rows) != 1 ||
      NULL == (row = tools_cJSON_GetArrayItem(rows, 0)) ||
      !tools_cJSON_IsArray(row) || tools_cJSON_GetArraySize(row) != 2 ||
      NULL == (content = tools_cJSON_GetArrayItem(row, 0)) ||
      NULL == (validator = tools_cJSON_GetArrayItem(row, 1)) ||
      !tools_cJSON_IsObject(content) || !tools_cJSON_IsObject(validator)) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  content_kind = tools_cJSON_GetObjectItemCaseSensitive(content, "kind");
  content_value = tools_cJSON_GetObjectItemCaseSensitive(content, "value");
  validator_kind = tools_cJSON_GetObjectItemCaseSensitive(validator, "kind");
  validator_value = tools_cJSON_GetObjectItemCaseSensitive(validator, "value");
  if (!tools_cJSON_IsString(content_kind) || !tools_cJSON_IsString(content_value) ||
      !tools_cJSON_IsString(validator_kind) || !tools_cJSON_IsString(validator_value) ||
      strcmp(content_kind->valuestring, "hex") != 0 ||
      (strcmp(validator_kind->valuestring, "varchar") != 0 &&
       strcmp(validator_kind->valuestring, "nchar") != 0) ||
      (strcmp(validator_value->valuestring, "historical") != 0 &&
       strcmp(validator_value->valuestring, "third_party") != 0) ||
      strlen(validator_value->valuestring) >= validator_bytes) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  code = qstResultStoreDecodeHexResult(content_value->valuestring, out_result);
  if (code == QST_OK) {
    memcpy(out_validator, validator_value->valuestring,
           strlen(validator_value->valuestring) + 1U);
    *out_found = 1;
  }
_return:
  free(query_text);
  tools_cJSON_Delete(root);
  qst_result_reset(&query);
  if (code != QST_OK) {
    qst_result_reset(out_result);
    out_validator[0] = '\0';
  }
  return code;
}

EQstCode qst_result_store_historical_is_empty(
    SQstResultStore *store, const char *ds_fingerprint, uint32_t target,
    int32_t *out_empty) {
  SQstQueryResult query = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *rows = NULL;
  tools_cJSON *row = NULL;
  tools_cJSON *count_cell = NULL;
  char *query_text = NULL;
  char statement[256] = {0};
  const char *target_name = NULL;
  uint32_t target_bit = 0;
  int64_t count = 0;
  int32_t is_null = 0;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == store->native_taos || NULL == ds_fingerprint ||
      strlen(ds_fingerprint) != QST_PENDING_DIGEST_HEX_BYTES ||
      NULL == out_empty) {
    return QST_E_INVALID_ARGUMENT;
  }
  target_name = qstResultStoreTargetNameById(target);
  if (NULL == target_name) {
    return QST_E_INVALID_ARGUMENT;
  }
  target_bit = 1U << (target - 1U);
  if (strcmp(store->historical_fingerprint, ds_fingerprint) != 0) {
    if (snprintf(store->historical_fingerprint,
                 sizeof(store->historical_fingerprint), "%s",
                 ds_fingerprint) < 0) {
      return QST_E_RESOURCE;
    }
    store->historical_empty_known_mask = 0;
    store->historical_empty_mask = 0;
  }
  if ((store->historical_empty_known_mask & target_bit) != 0) {
    *out_empty = (store->historical_empty_mask & target_bit) != 0;
    return QST_OK;
  }
  written = snprintf(statement, sizeof(statement),
                     "SELECT COUNT(*) FROM res_db.verdict_%s_%s",
                     ds_fingerprint, target_name);
  if (written < 0 || (uint64_t)written >= sizeof(statement)) {
    return QST_E_RESOURCE;
  }
  code = qst_native_query_result((SQstNativeClient *)store->native_taos,
                                 statement, &query);
  if (code != QST_OK || query.bytes >= SIZE_MAX) {
    qst_result_reset(&query);
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  query_text = (char *)calloc((size_t)query.bytes + 1U, 1);
  if (NULL == query_text) {
    qst_result_reset(&query);
    return QST_E_RESOURCE;
  }
  memcpy(query_text, query.data, (size_t)query.bytes);
  root = tools_cJSON_ParseWithOpts(query_text, NULL, 1);
  columns = tools_cJSON_GetObjectItemCaseSensitive(root, "columns");
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsArray(columns) ||
      !tools_cJSON_IsArray(rows) || tools_cJSON_GetArraySize(rows) != 1 ||
      NULL == (row = tools_cJSON_GetArrayItem(rows, 0)) ||
      !tools_cJSON_IsArray(row) || tools_cJSON_GetArraySize(row) != 1 ||
      NULL == (count_cell = tools_cJSON_GetArrayItem(row, 0))) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  code = qstResultStoreCopyTypedNumber(count_cell, &count, &is_null);
  if (code != QST_OK || is_null != 0 || count < 0) {
    code = code != QST_OK ? code : QST_E_RESULT_STORE;
    goto _return;
  }
  store->historical_empty_known_mask |= target_bit;
  if (count == 0) {
    store->historical_empty_mask |= target_bit;
  } else {
    store->historical_empty_mask &= ~target_bit;
  }
  *out_empty = count == 0;

_return:
  free(query_text);
  tools_cJSON_Delete(root);
  qst_result_reset(&query);
  return code;
}

static tools_cJSON *qstResultStoreColumn(const tools_cJSON *columns,
                                         const char *name, int32_t *out_index) {
  if (NULL == columns || NULL == name || NULL == out_index ||
      !tools_cJSON_IsArray(columns)) {
    return NULL;
  }
  for (int32_t index = 0; index < tools_cJSON_GetArraySize(columns); ++index) {
    tools_cJSON *column = tools_cJSON_GetArrayItem(columns, index);
    tools_cJSON *column_name = tools_cJSON_GetObjectItemCaseSensitive(column, "name");
    if (tools_cJSON_IsString(column_name) &&
        strcmp(column_name->valuestring, name) == 0) {
      *out_index = index;
      return column;
    }
  }
  return NULL;
}

static EQstCode qstResultStoreCopyTypedText(const tools_cJSON *item,
                                            char **out_text) {
  tools_cJSON *kind = NULL;
  tools_cJSON *value = NULL;
  char *text = NULL;

  if (NULL == item || NULL == out_text) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_text = NULL;
  if (tools_cJSON_IsNull(item)) {
    return QST_OK;
  }
  if (!tools_cJSON_IsObject(item) ||
      NULL == (kind = tools_cJSON_GetObjectItemCaseSensitive(item, "kind")) ||
      NULL == (value = tools_cJSON_GetObjectItemCaseSensitive(item, "value")) ||
      !tools_cJSON_IsString(kind) || !tools_cJSON_IsString(value) ||
      strcmp(kind->valuestring, "hex") == 0) {
    return QST_E_RESULT_STORE;
  }
  text = strdup(value->valuestring);
  if (NULL == text) {
    return QST_E_RESOURCE;
  }
  *out_text = text;
  return QST_OK;
}

static EQstCode qstResultStoreCopyTypedHex(const tools_cJSON *item,
                                           SQstQueryResult *out_result) {
  tools_cJSON *kind = NULL;
  tools_cJSON *value = NULL;

  if (NULL == item || NULL == out_result) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_result_reset(out_result);
  if (tools_cJSON_IsNull(item)) {
    return QST_OK;
  }
  if (!tools_cJSON_IsObject(item) ||
      NULL == (kind = tools_cJSON_GetObjectItemCaseSensitive(item, "kind")) ||
      NULL == (value = tools_cJSON_GetObjectItemCaseSensitive(item, "value")) ||
      !tools_cJSON_IsString(kind) || !tools_cJSON_IsString(value) ||
      strcmp(kind->valuestring, "hex") != 0) {
    return QST_E_RESULT_STORE;
  }
  return qstResultStoreDecodeHexResult(value->valuestring, out_result);
}

static EQstCode qstResultStoreCopyTypedNumber(const tools_cJSON *item,
                                              int64_t *out_value,
                                              int32_t *out_is_null) {
  tools_cJSON *kind = NULL;
  tools_cJSON *value = NULL;
  char *end = NULL;
  long long parsed = 0;

  if (NULL == item || NULL == out_value || NULL == out_is_null) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_value = 0;
  *out_is_null = 0;
  if (tools_cJSON_IsNull(item)) {
    *out_is_null = 1;
    return QST_OK;
  }
  if (!tools_cJSON_IsObject(item) ||
      NULL == (kind = tools_cJSON_GetObjectItemCaseSensitive(item, "kind")) ||
      NULL == (value = tools_cJSON_GetObjectItemCaseSensitive(item, "value")) ||
      !tools_cJSON_IsString(kind) || !tools_cJSON_IsString(value)) {
    return QST_E_RESULT_STORE;
  }
  errno = 0;
  parsed = strtoll(value->valuestring, &end, 10);
  if (errno != 0 || end == value->valuestring || *end != '\0' ||
      parsed < INT64_MIN || parsed > INT64_MAX) {
    return QST_E_RESULT_STORE;
  }
  *out_value = (int64_t)parsed;
  return QST_OK;
}

static int32_t qstResultStoreIsLeapYear(int32_t year) {
  return (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
}

static int64_t qstResultStoreDaysFromCivil(int32_t year, uint32_t month,
                                           uint32_t day) {
  const int32_t adjusted_year = year - (month <= 2U ? 1 : 0);
  const int32_t era = (adjusted_year >= 0 ? adjusted_year : adjusted_year - 399) / 400;
  const uint32_t year_of_era = (uint32_t)(adjusted_year - era * 400);
  const int32_t month_prime = (int32_t)month + (month > 2U ? -3 : 9);
  const uint32_t day_of_year =
      (uint32_t)((153 * month_prime + 2) / 5) + day - 1U;
  const uint32_t day_of_era = year_of_era * 365U + year_of_era / 4U -
                              year_of_era / 100U + day_of_year;
  return (int64_t)era * INT64_C(146097) + (int64_t)day_of_era - INT64_C(719468);
}

static EQstCode qstResultStoreCopyTypedTimestamp(const tools_cJSON *item,
                                                  int64_t *out_ts_ns) {
  tools_cJSON *kind = NULL;
  tools_cJSON *value = NULL;
  int32_t year = 0;
  int32_t month = 0;
  int32_t day = 0;
  int32_t hour = 0;
  int32_t minute = 0;
  int32_t second = 0;
  int32_t fraction = 0;
  int32_t consumed = 0;
  int32_t parsed = 0;
  int64_t days = 0;
  int64_t seconds = 0;
  int64_t nanos = 0;

  if (NULL == item || NULL == out_ts_ns) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_ts_ns = 0;
  if (!tools_cJSON_IsObject(item) ||
      NULL == (kind = tools_cJSON_GetObjectItemCaseSensitive(item, "kind")) ||
      NULL == (value = tools_cJSON_GetObjectItemCaseSensitive(item, "value")) ||
      !tools_cJSON_IsString(kind) || !tools_cJSON_IsString(value) ||
      strcmp(kind->valuestring, "timestamp") != 0) {
    return QST_E_RESULT_STORE;
  }
  parsed = sscanf(value->valuestring,
                  "%4d-%2d-%2dT%2d:%2d:%2d.%9dZ%n", &year, &month, &day,
                  &hour, &minute, &second, &fraction, &consumed);
  if (parsed != 7 || year < 1 || month < 1 || month > 12 || day < 1 ||
      hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 ||
      second > 59 || fraction < 0 || fraction > 999999999 || consumed <= 0 ||
      value->valuestring[consumed] != '\0') {
    return QST_E_RESULT_STORE;
  }
  {
    static const uint8_t month_days[] =
        {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    const int32_t max_day = month_days[month - 1] +
        (month == 2 && qstResultStoreIsLeapYear(year) ? 1 : 0);
    if (day > max_day) {
      return QST_E_RESULT_STORE;
    }
  }
  days = qstResultStoreDaysFromCivil(year, (uint32_t)month, (uint32_t)day);
  if (days > INT64_MAX / INT64_C(86400) ||
      days < INT64_MIN / INT64_C(86400)) {
    return QST_E_RESOURCE;
  }
  seconds = days * INT64_C(86400) + hour * INT64_C(3600) +
            minute * INT64_C(60) + second;
  if (seconds > INT64_MAX / INT64_C(1000000000) ||
      seconds < INT64_MIN / INT64_C(1000000000)) {
    return QST_E_RESOURCE;
  }
  nanos = seconds * INT64_C(1000000000) + fraction;
  *out_ts_ns = nanos;
  return QST_OK;
}

static EQstCode qstResultStoreParseStoredRow(const tools_cJSON *row,
                                             const tools_cJSON *columns,
                                             uint32_t target,
                                             SQstStoredResult *out_record) {
  const char *const names[] = {"gid", "verdict", "error_code"};
  int32_t indexes[3] = {-1, -1, -1};
  int64_t number = 0;
  int32_t is_null = 0;
  tools_cJSON *item = NULL;
  EQstCode code = QST_OK;

  if (NULL == row || NULL == columns || NULL == out_record ||
      !tools_cJSON_IsArray(row)) {
    return QST_E_RESULT_STORE;
  }
  for (uint32_t index = 0; index < sizeof(names) / sizeof(names[0]); ++index) {
    if (NULL == qstResultStoreColumn(columns, names[index], &indexes[index])) {
      return QST_E_RESULT_STORE;
    }
  }
  memset(out_record, 0, sizeof(*out_record));
  out_record->target = target;
  item = tools_cJSON_GetArrayItem(row, indexes[0]);
  code = qstResultStoreCopyTypedNumber(item, &number, &is_null);
  if (code != QST_OK || is_null != 0 || number <= 0) {
    return QST_E_RESULT_STORE;
  }
  out_record->gid = (uint64_t)number;
  item = tools_cJSON_GetArrayItem(row, indexes[1]);
  code = qstResultStoreCopyTypedNumber(item, &number, &is_null);
  if (code != QST_OK || is_null != 0 || number < 0 || number > 2) {
    return QST_E_RESULT_STORE;
  }
  out_record->verdict = (int32_t)number;
  item = tools_cJSON_GetArrayItem(row, indexes[2]);
  code = qstResultStoreCopyTypedNumber(item, &number, &is_null);
  if (code != QST_OK) {
    return code;
  }
  out_record->has_error_code = is_null == 0;
  out_record->error_code = is_null == 0 ? (int32_t)number : 0;
  {
    int32_t timestamp_index = -1;
    if (NULL == qstResultStoreColumn(columns, "ts", &timestamp_index) ||
        qstResultStoreCopyTypedTimestamp(
            tools_cJSON_GetArrayItem(row, timestamp_index), &out_record->ts_ns) !=
            QST_OK ||
        out_record->ts_ns == 0) {
      return QST_E_RESULT_STORE;
    }
  }
  {
    int32_t text_index = -1;
    const char *const text_names[] = {"validator", "diff_summary",
                                      "error_message", "analysis_content",
                                      "ds_fingerprint"};
    char **text_outputs[] = {&out_record->validator, &out_record->diff_summary,
                             &out_record->error_message,
                             &out_record->analysis_content,
                             &out_record->ds_fingerprint};
    for (uint32_t index = 0; index < sizeof(text_names) / sizeof(text_names[0]); ++index) {
      if (NULL == qstResultStoreColumn(columns, text_names[index], &text_index)) {
        if (index == 0) {
          return QST_E_RESULT_STORE;
        }
        continue;
      }
      code = qstResultStoreCopyTypedText(
          tools_cJSON_GetArrayItem(row, text_index), text_outputs[index]);
      if (code != QST_OK) {
        return code;
      }
    }
  }
  {
    int32_t result_index = -1;
    if (NULL == qstResultStoreColumn(columns, "result_content", &result_index)) {
      return QST_E_RESULT_STORE;
    }
    code = qstResultStoreCopyTypedHex(tools_cJSON_GetArrayItem(row, result_index),
                                      &out_record->result_content);
    if (code != QST_OK) {
      return code;
    }
  }
  {
    int32_t storage_index = -1;
    if (qstResultStoreColumn(columns, "result_storage_status", &storage_index) != NULL) {
      char *storage = NULL;
      code = qstResultStoreCopyTypedText(tools_cJSON_GetArrayItem(row, storage_index),
                                         &storage);
      if (code != QST_OK) {
        return code;
      }
      if (storage != NULL) {
        if (strcmp(storage, "memory") == 0) out_record->result_storage_status = QST_RESULT_MEMORY;
        else if (strcmp(storage, "disk") == 0) out_record->result_storage_status = QST_RESULT_DISK;
        else if (strcmp(storage, "pending") == 0) out_record->result_storage_status = QST_RESULT_PENDING;
        else if (strcmp(storage, "unavailable") == 0) out_record->result_storage_status = QST_RESULT_UNAVAILABLE;
        else out_record->result_storage_status = QST_RESULT_NOT_REQUESTED;
      }
      free(storage);
    }
  }
  return QST_OK;
}

EQstCode qst_result_store_read_results(SQstResultStore *store, uint32_t target,
                                       SQstStoredResult **out_records,
                                       uint32_t *out_count) {
  SQstQueryResult query = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *rows = NULL;
  SQstStoredResult *records = NULL;
  const char *target_name = qstResultStoreTargetNameById(target);
  char statement[1024] = {0};
  char *query_text = NULL;
  int32_t written = 0;
  int32_t row_count = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == store->native_taos || NULL == out_records ||
      NULL == out_count || NULL == target_name || store->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_records = NULL;
  *out_count = 0;
  written = snprintf(statement, sizeof(statement),
                     "SELECT ts,gid,verdict,validator,diff_summary,error_code,"
                     "error_message,result_content,result_storage_status,analysis_content,"
                     "ds_fingerprint FROM res_db.results WHERE run_id=%llu "
                     "AND target='%s' ORDER BY ts DESC",
                     (unsigned long long)store->run_id, target_name);
  if (written < 0 || (uint64_t)written >= sizeof(statement)) {
    return QST_E_RESOURCE;
  }
  code = qst_native_query_result((SQstNativeClient *)store->native_taos,
                                 statement, &query);
  if (code != QST_OK) {
    return code;
  }
  query_text = (char *)calloc((size_t)query.bytes + 1U, 1);
  if (NULL == query_text) {
    qst_result_reset(&query);
    return QST_E_RESOURCE;
  }
  memcpy(query_text, query.data, (size_t)query.bytes);
  root = tools_cJSON_ParseWithOpts(query_text, NULL, 1);
  columns = tools_cJSON_GetObjectItemCaseSensitive(root, "columns");
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  row_count = tools_cJSON_IsArray(rows) ? tools_cJSON_GetArraySize(rows) : -1;
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsArray(columns) || row_count < 0) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  if (row_count > 0) {
    records = (SQstStoredResult *)calloc((size_t)row_count, sizeof(*records));
    if (NULL == records) {
      code = QST_E_RESOURCE;
      goto _return;
    }
    for (int32_t index = 0; index < row_count; ++index) {
      code = qstResultStoreParseStoredRow(tools_cJSON_GetArrayItem(rows, index),
                                          columns, target, &records[index]);
      if (code != QST_OK) {
        qst_result_store_free_results(records, (uint32_t)row_count);
        records = NULL;
        goto _return;
      }
    }
  }
  *out_records = records;
  *out_count = (uint32_t)row_count;
  records = NULL;
_return:
  free(records);
  free(query_text);
  tools_cJSON_Delete(root);
  qst_result_reset(&query);
  return code;
}

void qst_result_store_free_results(SQstStoredResult *records,
                                   uint32_t record_count) {
  if (NULL == records) {
    return;
  }
  for (uint32_t index = 0; index < record_count; ++index) {
    free(records[index].validator);
    free(records[index].diff_summary);
    free(records[index].error_message);
    free(records[index].analysis_content);
    free(records[index].ds_fingerprint);
    qst_result_reset(&records[index].result_content);
  }
  free(records);
}

static EQstCode qstResultStoreParseStoredStatement(
    const tools_cJSON *row, const tools_cJSON *columns, uint32_t target,
    SQstStoredStatement *out_record) {
  const char *const required_text[] = {"sql_text", "sql_sha256"};
  char **required_output[] = {&out_record->sql_text, &out_record->sql_sha256};
  const char *const optional_text[] = {
      "taosd_pid", "taosc_pid", "error_message", "taosd_log_ranges",
      "taosc_log_ranges", "core_relative_path"};
  char **optional_output[] = {
      &out_record->taosd_pid, &out_record->taosc_pid, &out_record->error_message,
      &out_record->taosd_log_ranges, &out_record->taosc_log_ranges,
      &out_record->core_relative_path};
  int32_t index = -1;
  int64_t number = 0;
  int32_t is_null = 0;
  EQstCode code = QST_OK;

  if (NULL == row || NULL == columns || NULL == out_record ||
      !tools_cJSON_IsArray(row)) {
    return QST_E_RESULT_STORE;
  }
  memset(out_record, 0, sizeof(*out_record));
  out_record->target = target;
  if (qstResultStoreColumn(columns, "ts", &index) == NULL ||
      qstResultStoreCopyTypedTimestamp(tools_cJSON_GetArrayItem(row, index),
                                       &out_record->ts_ns) != QST_OK ||
      out_record->ts_ns == 0) {
    return QST_E_RESULT_STORE;
  }
  if (qstResultStoreColumn(columns, "gid", &index) == NULL ||
      qstResultStoreCopyTypedNumber(tools_cJSON_GetArrayItem(row, index),
                                    &number, &is_null) != QST_OK ||
      is_null != 0 || number <= 0) {
    return QST_E_RESULT_STORE;
  }
  out_record->gid = (uint64_t)number;
  if (qstResultStoreColumn(columns, "ended_at", &index) == NULL) {
    return QST_E_RESULT_STORE;
  }
  if (tools_cJSON_IsNull(tools_cJSON_GetArrayItem(row, index))) {
    out_record->ended_at_ns = 0;
  } else if (qstResultStoreCopyTypedTimestamp(
                 tools_cJSON_GetArrayItem(row, index),
                 &out_record->ended_at_ns) != QST_OK) {
    return QST_E_RESULT_STORE;
  }
  if (qstResultStoreColumn(columns, "sid", &index) == NULL ||
      qstResultStoreCopyTypedNumber(tools_cJSON_GetArrayItem(row, index),
                                    &number, &is_null) != QST_OK ||
      is_null != 0 || number < 0) {
    return QST_E_RESULT_STORE;
  }
  out_record->sid = (uint64_t)number;
  for (uint32_t text_index = 0;
       text_index < sizeof(required_text) / sizeof(required_text[0]);
       ++text_index) {
    if (qstResultStoreColumn(columns, required_text[text_index], &index) == NULL ||
        qstResultStoreCopyTypedText(tools_cJSON_GetArrayItem(row, index),
                                    required_output[text_index]) != QST_OK ||
        *required_output[text_index] == NULL) {
      return QST_E_RESULT_STORE;
    }
  }
  if (strlen(out_record->sql_sha256) != QST_PENDING_DIGEST_HEX_BYTES ||
      !qstPendingHexSegmentIsValid(out_record->sql_sha256,
                                   QST_PENDING_DIGEST_HEX_BYTES / 2U)) {
    return QST_E_RESULT_STORE;
  }
  if (qstResultStoreColumn(columns, "execution_status", &index) == NULL ||
      qstResultStoreCopyTypedNumber(tools_cJSON_GetArrayItem(row, index),
                                    &number, &is_null) != QST_OK ||
      is_null != 0 || number < QST_EXEC_SUCCESS || number > QST_EXEC_CANCELLED) {
    return QST_E_RESULT_STORE;
  }
  out_record->execution_status = (uint32_t)number;
  if (qstResultStoreColumn(columns, "result_row_count", &index) == NULL ||
      qstResultStoreCopyTypedNumber(tools_cJSON_GetArrayItem(row, index),
                                    &number, &is_null) != QST_OK ||
      is_null != 0 || number < -1) {
    return QST_E_RESULT_STORE;
  }
  out_record->result_row_count = number;
  if (qstResultStoreColumn(columns, "error_code", &index) == NULL) {
    return QST_E_RESULT_STORE;
  }
  code = qstResultStoreCopyTypedNumber(tools_cJSON_GetArrayItem(row, index),
                                       &number, &is_null);
  if (code != QST_OK) {
    return code;
  }
  out_record->has_error_code = is_null == 0;
  if (is_null == 0) {
    if (number < INT32_MIN || number > INT32_MAX) {
      return QST_E_RESULT_STORE;
    }
    out_record->error_code = (int32_t)number;
  }
  for (uint32_t text_index = 0;
       text_index < sizeof(optional_text) / sizeof(optional_text[0]);
       ++text_index) {
    if (qstResultStoreColumn(columns, optional_text[text_index], &index) == NULL) {
      continue;
    }
    code = qstResultStoreCopyTypedText(tools_cJSON_GetArrayItem(row, index),
                                       optional_output[text_index]);
    if (code != QST_OK) {
      return code;
    }
  }
  return QST_OK;
}

EQstCode qst_result_store_read_statements(
    SQstResultStore *store, uint32_t target, SQstStoredStatement **out_records,
    uint32_t *out_count) {
  SQstQueryResult query = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *columns = NULL;
  tools_cJSON *rows = NULL;
  SQstStoredStatement *records = NULL;
  const char *target_name = qstResultStoreTargetNameById(target);
  char statement[1536] = {0};
  char *query_text = NULL;
  int32_t row_count = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == store->native_taos || NULL == out_records ||
      NULL == out_count || NULL == target_name || store->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_records = NULL;
  *out_count = 0;
  if (snprintf(statement, sizeof(statement),
               "SELECT ts,gid,ended_at,sid,sql_text,sql_sha256,taosd_pid,"
               "taosc_pid,execution_status,result_row_count,error_code,"
               "error_message,taosd_log_ranges,taosc_log_ranges,core_relative_path "
               "FROM res_db.statements WHERE run_id=%llu AND target='%s' "
               "ORDER BY ts DESC",
               (unsigned long long)store->run_id, target_name) < 0 ||
      strlen(statement) >= sizeof(statement)) {
    return QST_E_RESOURCE;
  }
  code = qst_native_query_result((SQstNativeClient *)store->native_taos,
                                 statement, &query);
  if (code != QST_OK || query.bytes >= SIZE_MAX) {
    qst_result_reset(&query);
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  query_text = (char *)calloc((size_t)query.bytes + 1U, 1);
  if (NULL == query_text) {
    qst_result_reset(&query);
    return QST_E_RESOURCE;
  }
  memcpy(query_text, query.data, (size_t)query.bytes);
  root = tools_cJSON_ParseWithOpts(query_text, NULL, 1);
  columns = tools_cJSON_GetObjectItemCaseSensitive(root, "columns");
  rows = tools_cJSON_GetObjectItemCaseSensitive(root, "rows");
  row_count = tools_cJSON_IsArray(rows) ? tools_cJSON_GetArraySize(rows) : -1;
  if (!tools_cJSON_IsObject(root) || !tools_cJSON_IsArray(columns) || row_count < 0) {
    code = QST_E_RESULT_STORE;
    goto _return;
  }
  if (row_count > 0) {
    records = (SQstStoredStatement *)calloc((size_t)row_count, sizeof(*records));
    if (records == NULL) {
      code = QST_E_RESOURCE;
      goto _return;
    }
    for (int32_t index = 0; index < row_count; ++index) {
      code = qstResultStoreParseStoredStatement(
          tools_cJSON_GetArrayItem(rows, index), columns, target, &records[index]);
      if (code != QST_OK) {
        qst_result_store_free_statements(records, (uint32_t)row_count);
        records = NULL;
        goto _return;
      }
    }
  }
  *out_records = records;
  *out_count = (uint32_t)row_count;
  records = NULL;
_return:
  qst_result_store_free_statements(records, row_count > 0 ? (uint32_t)row_count : 0);
  free(query_text);
  tools_cJSON_Delete(root);
  qst_result_reset(&query);
  return code;
}

void qst_result_store_free_statements(SQstStoredStatement *records,
                                      uint32_t record_count) {
  if (records == NULL) {
    return;
  }
  for (uint32_t index = 0; index < record_count; ++index) {
    free(records[index].sql_text);
    free(records[index].sql_sha256);
    free(records[index].taosd_pid);
    free(records[index].taosc_pid);
    free(records[index].error_message);
    free(records[index].taosd_log_ranges);
    free(records[index].taosc_log_ranges);
    free(records[index].core_relative_path);
  }
  free(records);
}

static EQstCode qstResultStoreBuildManualAiDml(
    SQstResultStore *store, const SQstManualAiResult *record, uint64_t ts_ms,
    char **out_dml) {
  char *diff = NULL;
  char *error_message = NULL;
  char *analysis = NULL;
  char *result_hex = NULL;
  char *result_literal = NULL;
  char *dml = NULL;
  char error_code[32] = {0};
  char *storage_literal = NULL;
  const char *storage = "not_requested";
  uint64_t capacity = 0;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == record || NULL == out_dml || record->gid == 0 ||
      record->target < QST_TARGET_TD || record->target > QST_TARGET_INFLUX ||
      record->verdict < 0 || record->verdict > 2 ||
      NULL == record->ds_fingerprint || strlen(record->ds_fingerprint) != 64U ||
      ts_ms == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_dml = NULL;
  code = qstResultStoreMakeTextLiteral(store, "results", "diff_summary",
                                       record->diff_summary == NULL ? "" :
                                           record->diff_summary,
                                       4096U, &diff);
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "results", "error_message",
                                         record->error_message, 1024U,
                                         &error_message);
  }
  if (code == QST_OK) {
    code = qstResultStoreMakeTextLiteral(store, "results", "analysis_content",
                                         record->analysis_content == NULL ? "" :
                                             record->analysis_content,
                                         8192U, &analysis);
  }
  if (code != QST_OK) {
    goto _return;
  }
  if (record->has_error_code == 0) {
    (void)snprintf(error_code, sizeof(error_code), "NULL");
  } else {
    (void)snprintf(error_code, sizeof(error_code), "%d", record->error_code);
  }
  if (record->verdict == 2 && record->result_content != NULL &&
      record->result_content->data != NULL && record->result_content->bytes != 0) {
    if (record->result_content->bytes > QST_RESULT_BLOB_MAX_BYTES) {
      char reference[64] = {0};
      char *overflow_hex = NULL;
      if (record->result_content->bytes > SIZE_MAX / 2U) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      overflow_hex = (char *)calloc(
          (size_t)record->result_content->bytes * 2U + 1U, 1);
      if (NULL == overflow_hex) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      qstPendingHex(record->result_content->data, record->result_content->bytes,
                    overflow_hex);
      code = qst_result_store_write_overflow(
          store, "results", "result_content", "hex",
          (const uint8_t *)overflow_hex, strlen(overflow_hex),
          reference, sizeof(reference));
      free(overflow_hex);
      if (code != QST_OK) {
        goto _return;
      }
      result_literal = (char *)calloc(strlen(reference) + 3U, 1);
      if (NULL == result_literal) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      (void)snprintf(result_literal, strlen(reference) + 3U, "'%s'", reference);
      storage = "disk";
    } else {
      result_hex = (char *)calloc((size_t)record->result_content->bytes * 2U + 1U, 1);
      if (NULL == result_hex) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      qstPendingHex(record->result_content->data, record->result_content->bytes,
                    result_hex);
      result_literal = (char *)calloc(strlen(result_hex) + 5U, 1);
      if (NULL == result_literal) {
        code = QST_E_RESOURCE;
        goto _return;
      }
      (void)snprintf(result_literal, strlen(result_hex) + 5U, "'\\x%s'", result_hex);
      storage = "memory";
    }
  } else if (record->verdict == 2) {
    storage = "unavailable";
  }
  code = qstResultStoreMakeTextLiteral(store, "results", "result_storage_status",
                                       storage, 24U, &storage_literal);
  if (code != QST_OK) {
    goto _return;
  }
  if (strlen(diff) > UINT64_MAX - strlen(error_message) - strlen(analysis) -
                           (result_literal == NULL ? 0U : strlen(result_literal)) - 256U ||
      strlen(diff) + strlen(error_message) + strlen(analysis) +
              (result_literal == NULL ? 0U : strlen(result_literal)) + 256U >= SIZE_MAX) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  capacity = strlen(diff) + strlen(error_message) + strlen(analysis) +
      (result_literal == NULL ? 0U : strlen(result_literal)) + strlen(storage_literal) + 256U;
  dml = (char *)calloc((size_t)capacity, 1);
  if (NULL == dml) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  written = snprintf(dml, (size_t)capacity,
                     "INSERT INTO res_db.result_%llu_%s VALUES (%llu,%llu,%d,'ai',%s,%s,%s,%s,%s,%s)",
                     (unsigned long long)store->run_id,
                     qstResultStoreTargetNameById(record->target),
                     (unsigned long long)ts_ms, (unsigned long long)record->gid,
                     record->verdict, diff, error_code,
                     error_message == NULL ? "NULL" : error_message,
                     result_literal == NULL ? "NULL" : result_literal,
                     storage_literal, analysis);
  if (written < 0 || (uint64_t)written >= capacity) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  *out_dml = dml;
  dml = NULL;
_return:
  free(dml);
  free(storage_literal);
  free(result_literal);
  free(result_hex);
  free(analysis);
  free(error_message);
  free(diff);
  return code;
}

EQstCode qst_result_store_write_manual_ai_result(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, const SQstManualAiResult *record,
    uint64_t record_seq, uint32_t *out_durable_source) {
  SQstPendingRecord pending = {0};
  SQstRef pending_ref = {0};
  char stable_key[160] = {0};
  char *dml = NULL;
  SQstDigest256 ds_digest = {0};
  uint64_t ts_ms = 0;
  int32_t native_ready = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == record || NULL == out_durable_source ||
      record_seq == 0 || store->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_durable_source = 0;
  ts_ms = (uint64_t)(qstPendingNowNs() / INT64_C(1000000));
  if (ts_ms == 0 || snprintf(stable_key, sizeof(stable_key),
                             "results|%llu|%u|%llu|%016llx",
                             (unsigned long long)store->run_id, record->target,
                             (unsigned long long)record->gid,
                             (unsigned long long)(record->action_id_hash != 0 ?
                                                      record->action_id_hash : ts_ms)) < 0 ||
      strlen(stable_key) >= sizeof(stable_key)) {
    return QST_E_RESOURCE;
  }
  code = qstResultStoreDigestFromHex(record->ds_fingerprint, &ds_digest);
  if (code != QST_OK) {
    return code;
  }
  native_ready = store->native_taos != NULL;
  if (native_ready != 0) {
    code = qst_result_store_prepare_run(
        store, UINT32_C(1) << (record->target - QST_TARGET_TD), ds_digest, 1U);
    if (code != QST_OK) {
      native_ready = 0;
    }
  }
  code = qstResultStoreBuildManualAiDml(store, record, ts_ms, &dml);
  if (code != QST_OK) {
    return code;
  }
  if (native_ready != 0) {
    code = qstResultStoreEnsureSchema((SQstNativeClient *)store->native_taos);
    if (code != QST_OK) {
      native_ready = 0;
    } else {
      code = qst_native_execute((SQstNativeClient *)store->native_taos, dml);
      if (code != QST_OK) {
        native_ready = 0;
      }
    }
  }
  if (native_ready == 0 && code == QST_OK) {
    code = QST_E_RESULT_STORE;
  }
  if (code == QST_OK) {
    *out_durable_source = QST_DURABLE_RESULT_STORE;
    free(dml);
    return QST_OK;
  }
  if (NULL == pending_store || pending_store->pending_dir == NULL ||
      pending_store->pending_dir[0] == '\0') {
    free(dml);
    return code;
  }
  pending.persist_kind = QST_PERSIST_MANUAL_AI_RESULT;
  pending.target = record->target;
  pending.run_id = store->run_id;
  pending.gid = record->gid;
  pending.record_seq = record_seq;
  pending.ds_fingerprint = record->ds_fingerprint;
  pending.stable_key = stable_key;
  pending.payload = dml;
  pending.payload_bytes = strlen(dml);
  code = qst_pending_write(mapping, pending_store, &pending, &pending_ref);
  if (code == QST_OK && mapping != NULL) {
    code = qst_block_release(mapping, pending_ref, 0);
  }
  free(dml);
  if (code == QST_OK) {
    *out_durable_source = QST_DURABLE_PENDING_FILE;
  }
  return code;
}

static const char *qstResultStoreStorageName(uint32_t storage) {
  switch (storage) {
    case QST_RESULT_NOT_REQUESTED:
      return "not_requested";
    case QST_RESULT_MEMORY:
      return "memory";
    case QST_RESULT_DISK:
      return "disk";
    case QST_RESULT_PENDING:
      return "pending";
    case QST_RESULT_UNAVAILABLE:
      return "unavailable";
    default:
      return "unavailable";
  }
}

static int32_t qstResultStoreAddStatementCapacity(uint64_t *capacity,
                                                  size_t addend) {
  if (NULL == capacity || addend > UINT64_MAX ||
      *capacity > UINT64_MAX - (uint64_t)addend) {
    return 0;
  }
  *capacity += (uint64_t)addend;
  return 1;
}

EQstCode qst_result_store_build_task_dml(SQstShmMapping *mapping,
                                         SQstResultStore *store,
                                         const SQstPersistPayload *payload,
                                         char **out_dml) {
  SQstTaskPersistSnapshot snapshot = {0};
  const char *target_name = NULL;
  char sql_sha256_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};
  char *sql_text = NULL;
  char *escaped_sql = NULL;
  char *result_hex = NULL;
  char *result_literal = NULL;
  char *payload_text = NULL;
  char *escaped_validator = NULL;
  char *escaped_diff_summary = NULL;
  char *core_path = NULL;
  char *escaped_core_path = NULL;
  char *core_literal = NULL;
  char *statement = NULL;
  const char *result_storage_name = NULL;
  char task_table[256] = {0};
  char error_code_literal[32] = {0};
  char sql_overflow_reference[64] = {0};
  char result_overflow_reference[64] = {0};
  uint64_t statement_capacity = 0;
  int64_t persist_timestamp_ms = 0;
  uint16_t sql_shape_mask = 0;
  int32_t written = 0;
  int32_t verdict = 2;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == store || NULL == payload ||
      NULL == out_dml ||
      (payload->persist_kind != QST_PERSIST_STATEMENT &&
       payload->persist_kind != QST_PERSIST_INITIAL_RESULT &&
       payload->persist_kind != QST_PERSIST_HISTORICAL_VERDICT)) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_dml = NULL;
  code = qstResultStorePersistTimestampMs(mapping, payload->record_seq,
                                          &persist_timestamp_ms);
  if (code != QST_OK) {
    return code;
  }
  code = qst_task_get_persist_snapshot(mapping, payload->gid,
                                       payload->dispatch_epoch,
                                       payload->transition_seq, &snapshot);
  if (code != QST_OK || snapshot.target != payload->target) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  target_name = qstResultStoreTargetNameById(snapshot.target);
  if (NULL == target_name) {
    return QST_E_RESULT_STORE;
  }
  if ((payload->persist_kind == QST_PERSIST_STATEMENT ||
       payload->persist_kind == QST_PERSIST_INITIAL_RESULT) &&
      qstResultStoreTaskTableName(
          task_table, sizeof(task_table),
          payload->persist_kind == QST_PERSIST_STATEMENT ? "stmt" : "result",
          mapping->header->run_id, target_name, payload->reserved0) != QST_OK) {
    return QST_E_RESOURCE;
  }
  /* Initial-result rows do not carry SQL text. Avoid copying and escaping the
   * task SQL a second time; statement and historical rows still require it. */
  if (payload->persist_kind != QST_PERSIST_INITIAL_RESULT) {
    if (qstResultStoreCopyText(mapping, snapshot.sql_ref, snapshot.gid,
                               &sql_text) != QST_OK) {
      free(sql_text);
      return QST_E_RESULT_STORE;
    }
    sql_shape_mask = qstResultStoreSqlShapeMask(sql_text);
    if (strlen(sql_text) > 8192U) {
      code = qst_result_store_write_overflow(
          store, "statements", "sql_text", "utf8", (const uint8_t *)sql_text,
          strlen(sql_text), sql_overflow_reference,
          sizeof(sql_overflow_reference));
      if (code != QST_OK) {
        free(sql_text);
        return code;
      }
      free(sql_text);
      sql_text = strdup(sql_overflow_reference);
      if (NULL == sql_text) {
        return QST_E_RESOURCE;
      }
    }
    escaped_sql = qstResultStoreEscapeText(sql_text);
    if (NULL == escaped_sql) {
      free(sql_text);
      return QST_E_RESOURCE;
    }
  }
  qstPendingHex(snapshot.sql_sha256.bytes, sizeof(snapshot.sql_sha256.bytes),
                sql_sha256_hex);
  if ((payload->persist_kind == QST_PERSIST_INITIAL_RESULT ||
       payload->persist_kind == QST_PERSIST_HISTORICAL_VERDICT) &&
      snapshot.focus == QST_FOCUS_CORRECTNESS &&
      snapshot.result_storage_status == QST_RESULT_MEMORY &&
      !qstRefIsEmpty(snapshot.final_payload.result_ref) &&
      snapshot.final_payload.result_ref.length <= QST_RESULT_BLOB_MAX_BYTES) {
    code = qstResultStoreCopyHex(mapping, snapshot.final_payload.result_ref,
                                 snapshot.gid, &result_hex);
    if (code != QST_OK) {
      free(escaped_sql);
      free(sql_text);
      return code;
    }
  }
  if (payload->persist_kind == QST_PERSIST_INITIAL_RESULT &&
      snapshot.focus == QST_FOCUS_CORRECTNESS &&
      snapshot.result_storage_status == QST_RESULT_MEMORY &&
      !qstRefIsEmpty(snapshot.final_payload.result_ref) &&
      snapshot.final_payload.result_ref.length > QST_RESULT_BLOB_MAX_BYTES) {
    code = qstResultStoreCopyHex(mapping, snapshot.final_payload.result_ref,
                                 snapshot.gid, &result_hex);
    if (code == QST_OK) {
      code = qst_result_store_write_overflow(
          store, "results", "result_content", "hex",
          (const uint8_t *)result_hex, strlen(result_hex),
          result_overflow_reference, sizeof(result_overflow_reference));
    }
    free(result_hex);
    result_hex = NULL;
    if (code != QST_OK) {
      free(escaped_sql);
      free(sql_text);
      return code;
    }
  }
  result_storage_name =
      payload->persist_kind == QST_PERSIST_INITIAL_RESULT &&
              snapshot.focus == QST_FOCUS_CORRECTNESS &&
              snapshot.result_storage_status == QST_RESULT_MEMORY &&
              !qstRefIsEmpty(snapshot.final_payload.result_ref) &&
              snapshot.final_payload.result_ref.length > QST_RESULT_BLOB_MAX_BYTES ?
          "disk" : qstResultStoreStorageName(snapshot.result_storage_status);
  if (result_hex != NULL) {
    /* TDengine SQL accepts binary payloads as a string beginning with \\x.
     * The MySQL-style X'...' spelling is rejected by the native parser. */
    result_literal = (char *)calloc(strlen(result_hex) + 5U, 1);
    if (NULL == result_literal) {
      free(result_hex);
      free(escaped_sql);
      free(sql_text);
      return QST_E_RESOURCE;
    }
    (void)snprintf(result_literal, strlen(result_hex) + 5U, "'\\x%s'",
                   result_hex);
  } else if (result_overflow_reference[0] != '\0') {
    result_literal = (char *)calloc(strlen(result_overflow_reference) + 3U, 1);
    if (NULL == result_literal) {
      free(escaped_sql);
      free(sql_text);
      return QST_E_RESOURCE;
    }
    (void)snprintf(result_literal, strlen(result_overflow_reference) + 3U,
                   "'%s'", result_overflow_reference);
  }
  if (payload->persist_kind == QST_PERSIST_INITIAL_RESULT) {
    tools_cJSON *root = NULL;
    tools_cJSON *kind = NULL;
    tools_cJSON *verdict_json = NULL;
    tools_cJSON *validator = NULL;
    tools_cJSON *diff_summary = NULL;

    code = qstResultStoreCopyText(mapping, payload->payload_ref, snapshot.gid,
                                  &payload_text);
    if (code != QST_OK || NULL == (root = tools_cJSON_Parse(payload_text)) ||
        NULL == (kind = tools_cJSON_GetObjectItemCaseSensitive(root, "kind")) ||
        NULL == (verdict_json = tools_cJSON_GetObjectItemCaseSensitive(root, "verdict")) ||
        NULL == (validator = tools_cJSON_GetObjectItemCaseSensitive(root, "validator")) ||
        NULL == (diff_summary = tools_cJSON_GetObjectItemCaseSensitive(root, "diff_summary")) ||
        !tools_cJSON_IsString(kind) || strcmp(kind->valuestring, "result") != 0 ||
        !tools_cJSON_IsNumber(verdict_json) || verdict_json->valueint < 0 ||
        verdict_json->valueint > 2 || !tools_cJSON_IsString(validator) ||
        !tools_cJSON_IsString(diff_summary)) {
      tools_cJSON_Delete(root);
      free(payload_text);
      free(result_literal);
      free(result_hex);
      free(escaped_sql);
      free(sql_text);
      return QST_E_SHM_CORRUPT;
    }
    verdict = verdict_json->valueint;
    escaped_validator = qstResultStoreEscapeText(validator->valuestring);
    escaped_diff_summary = qstResultStoreEscapeText(diff_summary->valuestring);
    tools_cJSON_Delete(root);
    free(payload_text);
    payload_text = NULL;
    if (NULL == escaped_validator || NULL == escaped_diff_summary) {
      free(escaped_validator);
      free(escaped_diff_summary);
      free(result_literal);
      free(result_hex);
      free(escaped_sql);
      free(sql_text);
      return QST_E_RESOURCE;
    }
  }
  if (payload->persist_kind == QST_PERSIST_STATEMENT &&
      !qstRefIsEmpty(snapshot.final_payload.core_path_ref)) {
    code = qstResultStoreCopyText(mapping, snapshot.final_payload.core_path_ref,
                                  snapshot.gid, &core_path);
    if (code != QST_OK || NULL == (escaped_core_path = qstResultStoreEscapeText(core_path))) {
      free(core_path);
      free(escaped_core_path);
      free(result_literal);
      free(result_hex);
      free(escaped_validator);
      free(escaped_diff_summary);
      free(escaped_sql);
      free(sql_text);
      return code != QST_OK ? code : QST_E_RESOURCE;
    }
    core_literal = (char *)calloc(strlen(escaped_core_path) + 3U, 1);
    if (NULL == core_literal) {
      free(core_path);
      free(escaped_core_path);
      free(result_literal);
      free(result_hex);
      free(escaped_validator);
      free(escaped_diff_summary);
      free(escaped_sql);
      free(sql_text);
      return QST_E_RESOURCE;
    }
    (void)snprintf(core_literal, strlen(escaped_core_path) + 3U, "'%s'",
                   escaped_core_path);
  }
  statement_capacity = 1024U;
  if (qstResultStoreAddStatementCapacity(&statement_capacity,
                                         escaped_sql == NULL ? 0U :
                                                              strlen(escaped_sql)) == 0 ||
      qstResultStoreAddStatementCapacity(
          &statement_capacity,
          result_literal == NULL ? 4U : strlen(result_literal)) == 0 ||
      qstResultStoreAddStatementCapacity(
          &statement_capacity,
          core_literal == NULL ? 0U : strlen(core_literal)) == 0 ||
      qstResultStoreAddStatementCapacity(
          &statement_capacity,
          escaped_validator == NULL ? 0U : strlen(escaped_validator)) == 0 ||
      qstResultStoreAddStatementCapacity(
          &statement_capacity,
          escaped_diff_summary == NULL ? 0U : strlen(escaped_diff_summary)) == 0 ||
      statement_capacity >= SIZE_MAX) {
    free(core_literal);
    free(escaped_core_path);
    free(core_path);
    free(result_literal);
    free(result_hex);
    free(escaped_validator);
    free(escaped_diff_summary);
    free(escaped_sql);
    free(sql_text);
    return QST_E_RESOURCE;
  }
  statement = (char *)calloc((size_t)statement_capacity, 1);
  if (NULL == statement) {
    free(core_literal);
    free(escaped_core_path);
    free(core_path);
    free(result_hex);
    free(result_literal);
    free(escaped_validator);
    free(escaped_diff_summary);
    free(escaped_sql);
    free(sql_text);
    return QST_E_RESOURCE;
  }
  if (payload->persist_kind == QST_PERSIST_STATEMENT) {
    if (snapshot.final_payload.execution_status == QST_EXEC_SUCCESS) {
      (void)snprintf(error_code_literal, sizeof(error_code_literal), "NULL");
    } else if (snprintf(error_code_literal, sizeof(error_code_literal), "%d",
                        snapshot.final_payload.execution_error_code) < 0) {
      free(statement);
      free(core_literal);
      free(escaped_core_path);
      free(core_path);
      free(result_literal);
      free(result_hex);
      free(escaped_validator);
      free(escaped_diff_summary);
      free(escaped_sql);
      free(sql_text);
      return QST_E_RESOURCE;
    }
    written = snprintf(
        statement, (size_t)statement_capacity,
        "INSERT INTO %s (ts,gid,ended_at,sid,sql_text,sql_sha256,"
        "sql_shape_mask,taosd_pid,taosc_pid,execution_status,result_row_count,"
        "error_code,error_message,taosd_log_ranges,taosc_log_ranges,core_relative_path,"
        "dialect,generation_abnormal,generation_unsupported_fallback) VALUES "
        "(%lld,%llu,%lld,%llu,'%s','%s',%u,'%d','%d',%u,%lld,%s,NULL,NULL,NULL,%s,"
        "%u,%u,%u)",
        task_table,
        (long long)persist_timestamp_ms,
        (unsigned long long)snapshot.gid,
        (long long)(snapshot.final_payload.ended_ts_ns / INT64_C(1000000)),
        (unsigned long long)snapshot.final_payload.sid, escaped_sql, sql_sha256_hex,
        (unsigned)sql_shape_mask, snapshot.final_payload.taosd_pid,
        snapshot.final_payload.taosc_pid,
        snapshot.final_payload.execution_status,
        (long long)snapshot.final_payload.result_row_count,
        error_code_literal, core_literal == NULL ? "NULL" : core_literal,
        snapshot.dialect, snapshot.generation_contains_abnormal,
        snapshot.generation_contains_unsupported_fallback);
  } else if (payload->persist_kind == QST_PERSIST_INITIAL_RESULT) {
    written = snprintf(
        statement, (size_t)statement_capacity,
        "INSERT INTO %s VALUES (%lld,%llu,%d,'%s',"
        "'%s',NULL,NULL,%s,'%s',NULL)",
        task_table,
        (long long)persist_timestamp_ms,
        (unsigned long long)snapshot.gid,
        verdict, escaped_validator, escaped_diff_summary,
        result_literal == NULL ? "NULL" : result_literal, result_storage_name);
  } else {
    SQstGenerationState *generation_state = NULL;
    char fingerprint[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};

    if (snapshot.focus != QST_FOCUS_CORRECTNESS || NULL == result_hex ||
        qst_generator_state_get(mapping, &generation_state) != QST_OK) {
      free(statement);
      free(result_literal);
      free(escaped_validator);
      free(escaped_diff_summary);
      free(result_hex);
      free(escaped_sql);
      free(sql_text);
      return QST_E_SHM_CORRUPT;
    }
    qstPendingHex(generation_state->dataset_digest.bytes,
                  sizeof(generation_state->dataset_digest.bytes), fingerprint);
    written = snprintf(
        statement, (size_t)statement_capacity,
        "INSERT INTO res_db.verdict_%s_%s VALUES (%lld,'%s','%s',%lld,'\\x%s',"
        "'historical','taosQueryTest')",
        fingerprint, target_name, (long long)persist_timestamp_ms,
        sql_sha256_hex, escaped_sql,
        (long long)snapshot.final_payload.result_row_count, result_hex);
  }
  code = written < 0 || (uint64_t)written >= statement_capacity ? QST_E_RESOURCE :
      QST_OK;
  if (code == QST_OK) {
    *out_dml = statement;
    statement = NULL;
  }
  free(statement);
  free(core_literal);
  free(escaped_core_path);
  free(core_path);
  free(result_literal);
  free(escaped_validator);
  free(escaped_diff_summary);
  free(result_hex);
  free(escaped_sql);
  free(sql_text);
  return code;
}

/* Writes one already hex-encoded logical pending object through the native API. */
static EQstCode qstResultStoreWriteHex(SQstShmMapping *mapping,
                                       SQstResultStore *store,
                                       uint32_t persist_kind,
                                       uint32_t target, uint64_t gid,
                                       uint64_t record_seq,
                                       const char *stable_key_hex,
                                       const char *payload_sha256_hex,
                                       const char *payload_hex) {
  char *sql = NULL;
  uint64_t sql_bytes = 0;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == store || NULL == store->native_taos || record_seq == 0 ||
      NULL == stable_key_hex || NULL == payload_sha256_hex ||
      NULL == payload_hex) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (strlen(stable_key_hex) > UINT64_MAX - strlen(payload_hex) - 512U) {
    return QST_E_RESOURCE;
  }
  sql_bytes = strlen(stable_key_hex) + strlen(payload_hex) + 512U;
  sql = (char *)calloc((size_t)sql_bytes, 1);
  if (NULL == sql) {
    return QST_E_RESOURCE;
  }
  written = snprintf(
      sql, (size_t)sql_bytes,
      "INSERT INTO res_db.qst_pending_records VALUES (NOW,%llu,%llu,%u,%u,"
      "%llu,'%s','%s','%s')",
      (unsigned long long)store->run_id, (unsigned long long)record_seq,
      persist_kind, target, (unsigned long long)gid, stable_key_hex,
      payload_sha256_hex, payload_hex);
  if (written < 0 || (uint64_t)written >= sql_bytes) {
    free(sql);
    return QST_E_RESOURCE;
  }
  code = qstResultStoreExecuteWithRecovery(mapping, store, sql, record_seq);
  free(sql);
  return code;
}

EQstCode qst_result_store_configure_connection(
    const SQstResultStoreConfig *config, SQstResultStore *out_store) {
  EQstCode code = QST_OK;

  if (NULL == config || NULL == out_store || config->run_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if ((NULL != config->pending_dir &&
       strlen(config->pending_dir) >= sizeof(out_store->pending_dir)) ||
      (NULL != config->quarantine_dir &&
       strlen(config->quarantine_dir) >= sizeof(out_store->quarantine_dir)) ||
      (NULL != config->overflow_path &&
       strlen(config->overflow_path) >= sizeof(out_store->overflow_path)) ||
      (NULL != config->config_dir &&
       strlen(config->config_dir) >= sizeof(out_store->config_dir)) ||
      (NULL != config->user && strlen(config->user) >= sizeof(out_store->user)) ||
      (NULL != config->password &&
       strlen(config->password) >= sizeof(out_store->password))) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_store, 0, sizeof(*out_store));
  out_store->run_id = config->run_id;
  out_store->endpoint_digest = config->endpoint_digest;
  code = qstResultStoreParseEndpoint(config->endpoint, out_store->endpoint_host,
                                     sizeof(out_store->endpoint_host),
                                     &out_store->endpoint_port);
  if (code != QST_OK) {
    memset(out_store, 0, sizeof(*out_store));
    return code;
  }
  if (NULL != config->config_dir) {
    memcpy(out_store->config_dir, config->config_dir,
           strlen(config->config_dir) + 1U);
  }
  if (NULL != config->user) {
    memcpy(out_store->user, config->user, strlen(config->user) + 1U);
  }
  if (NULL != config->password) {
    memcpy(out_store->password, config->password, strlen(config->password) + 1U);
  }
  return QST_OK;
}

EQstCode qst_result_store_open(const SQstResultStoreConfig *config,
                               SQstResultStore *out_store) {
  EQstCode code = QST_OK;

  code = qst_result_store_configure_connection(config, out_store);
  if (code != QST_OK) {
    return code;
  }
  if (NULL != config->pending_dir) {
    memcpy(out_store->pending_dir, config->pending_dir,
           strlen(config->pending_dir) + 1U);
  }
  if (NULL != config->quarantine_dir) {
    memcpy(out_store->quarantine_dir, config->quarantine_dir,
           strlen(config->quarantine_dir) + 1U);
  }
  if (NULL != config->overflow_path) {
    memcpy(out_store->overflow_path, config->overflow_path,
           strlen(config->overflow_path) + 1U);
  }
  code = qstResultStoreOpenNative(out_store);
  if (code != QST_OK) {
    memset(out_store, 0, sizeof(*out_store));
    return code;
  }
  return QST_OK;
}

EQstCode qst_result_store_close(SQstResultStore *store) {
  SQstNativeClient *client = NULL;
  EQstCode code = QST_OK;

  if (NULL == store) {
    return QST_E_INVALID_ARGUMENT;
  }
  client = (SQstNativeClient *)store->native_taos;
  if (NULL != client) {
    code = qst_native_close(client);
    free(client);
  }
  memset(store, 0, sizeof(*store));
  return code;
}

EQstCode qst_result_store_readiness_json(const SQstResultStore *store,
                                         char *out_json,
                                         uint64_t out_bytes) {
  static const char kUnavailable[] =
      "{\"available\":false,\"server_version\":\"\","
      "\"federated_query_enable\":\"\","
      "\"external_source_ddl\":\"not_probed\","
      "\"error\":\"result store unavailable\","
      "\"capabilities\":{\"native_query\":false,"
      "\"result_oracle_v1\":false,\"external_source_ddl\":false}}";
  int32_t written = 0;

  if (NULL == out_json || out_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL == store || NULL == store->native_taos) {
    written = snprintf(out_json, (size_t)out_bytes, "%s", kUnavailable);
    return written > 0 && (uint64_t)written < out_bytes ? QST_OK :
                                                         QST_E_RESOURCE;
  }
  return qst_native_readiness_json((SQstNativeClient *)store->native_taos,
                                   out_json, out_bytes);
}

EQstCode qst_result_store_write(SQstShmMapping *mapping, SQstResultStore *store,
                                const SQstPersistPayload *payload,
                                int32_t *out_taos_code) {
  SQstNativeClient *client = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == payload ||
      NULL == out_taos_code || NULL == store->native_taos ||
      store->run_id != mapping->header->run_id || payload->record_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstRefIsEmpty(payload->stable_key_ref) &&
      qst_block_validate(mapping, payload->stable_key_ref, payload->gid) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  if (!qstRefIsEmpty(payload->payload_ref) &&
      qst_block_validate(mapping, payload->payload_ref, payload->gid) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  client = (SQstNativeClient *)store->native_taos;
  if (NULL != client &&
      (payload->persist_kind == QST_PERSIST_STATEMENT ||
       payload->persist_kind == QST_PERSIST_INITIAL_RESULT ||
       payload->persist_kind == QST_PERSIST_HISTORICAL_VERDICT)) {
    char *business_dml = NULL;

    code = qst_result_store_build_task_dml(mapping, store, payload, &business_dml);
    if (code == QST_OK) {
      code = qstResultStoreExecuteWithRecovery(
          mapping, store, business_dml, payload->record_seq);
    }
    free(business_dml);
  } else if (NULL != client) {
    char *stable_key_hex = NULL;
    char *payload_hex = NULL;
    char payload_sha256_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};

    code = qstResultStoreCopyHex(mapping, payload->stable_key_ref,
                                 payload->gid, &stable_key_hex);
    if (code == QST_OK) {
      code = qstResultStoreCopyHex(mapping, payload->payload_ref, payload->gid,
                                   &payload_hex);
    }
    if (code == QST_OK) {
      qstPendingHex(payload->payload_sha256.bytes,
                    sizeof(payload->payload_sha256.bytes), payload_sha256_hex);
      code = qstResultStoreWriteHex(
          mapping, store, payload->persist_kind, payload->target, payload->gid,
          payload->record_seq, stable_key_hex, payload_sha256_hex, payload_hex);
    }
    free(payload_hex);
    free(stable_key_hex);
  }
  *out_taos_code = code == QST_OK ? 0 : code;
  return code;
}

/* Keeps native batch assembly linear when a judge accumulates many records. */
static EQstCode qstResultStoreReserveBatchDml(char **inout_dml,
                                              uint64_t *inout_capacity,
                                              uint64_t required_bytes) {
  uint64_t capacity = 0;
  char *next_dml = NULL;

  if (NULL == inout_dml || NULL == inout_capacity || required_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  capacity = *inout_capacity;
  if (required_bytes <= capacity) {
    return QST_OK;
  }
  capacity = capacity == 0 ? 4096U : capacity;
  while (capacity < required_bytes) {
    if (capacity > UINT64_MAX / 2U) {
      capacity = required_bytes;
      break;
    }
    capacity *= 2U;
  }
  if (capacity < required_bytes || capacity > SIZE_MAX) {
    return QST_E_RESOURCE;
  }
  next_dml = realloc(*inout_dml, (size_t)capacity);
  if (NULL == next_dml) {
    return QST_E_RESOURCE;
  }
  *inout_dml = next_dml;
  *inout_capacity = capacity;
  return QST_OK;
}

enum {
  QST_RESULT_STORE_STATEMENT_BIND_COUNT = 19,
  QST_RESULT_STORE_STATEMENT_SQL_BYTES = 8192,
};

typedef struct {
  int64_t timestamp_ms;
  int64_t gid;
  int64_t ended_at_ms;
  int64_t sid;
  int16_t sql_shape_mask;
  int32_t error_code;
  int64_t result_row_count;
  int8_t execution_status;
  int8_t dialect;
  int8_t generation_abnormal;
  int8_t generation_unsupported_fallback;
  char sql_sha256[QST_PENDING_DIGEST_HEX_BYTES + 1U];
  char taosd_pid[32];
  char taosc_pid[32];
  char *sql_text;
  int32_t lengths[QST_RESULT_STORE_STATEMENT_BIND_COUNT];
  char is_null[QST_RESULT_STORE_STATEMENT_BIND_COUNT];
  TAOS_MULTI_BIND binds[QST_RESULT_STORE_STATEMENT_BIND_COUNT];
} SQstResultStoreStatementBindRow;

static void qstResultStoreStatementBind(TAOS_MULTI_BIND *bind,
                                        int32_t type, void *buffer,
                                        uintptr_t buffer_length,
                                        int32_t *length, char *is_null) {
  *bind = (TAOS_MULTI_BIND){.buffer_type = type,
                            .buffer = buffer,
                            .buffer_length = buffer_length,
                            .length = length,
                            .is_null = is_null,
                            .num = 1};
}

static EQstCode qstResultStoreStatementBindRowInitialize(
    SQstResultStoreStatementBindRow *row) {
  uint32_t index = 0;

  if (NULL == row || NULL == row->sql_text) {
    return QST_E_INVALID_ARGUMENT;
  }
#define QST_RESULT_STORE_BIND_VALUE(type, value)                              \
  do {                                                                         \
    const uint32_t bind_index = index++;                                       \
    row->lengths[bind_index] = sizeof(value);                                  \
    qstResultStoreStatementBind(&row->binds[bind_index], type, &(value),      \
                                sizeof(value), &row->lengths[bind_index],     \
                                &row->is_null[bind_index]);                   \
  } while (0)
#define QST_RESULT_STORE_BIND_TEXT(value)                                      \
  do {                                                                         \
    const uint32_t bind_index = index++;                                       \
    row->lengths[bind_index] = (int32_t)strlen(value);                         \
    qstResultStoreStatementBind(&row->binds[bind_index], TSDB_DATA_TYPE_BINARY, \
                                value, (uintptr_t)row->lengths[bind_index],   \
                                &row->lengths[bind_index],                    \
                                &row->is_null[bind_index]);                   \
  } while (0)
#define QST_RESULT_STORE_BIND_NULL(type)                                       \
  do {                                                                         \
    const uint32_t bind_index = index++;                                       \
    row->lengths[bind_index] = 0;                                              \
    row->is_null[bind_index] = 1;                                              \
    qstResultStoreStatementBind(&row->binds[bind_index], type, NULL, 0,       \
                                &row->lengths[bind_index],                    \
                                &row->is_null[bind_index]);                   \
  } while (0)
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_TIMESTAMP, row->timestamp_ms);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_BIGINT, row->gid);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_TIMESTAMP, row->ended_at_ms);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_BIGINT, row->sid);
  QST_RESULT_STORE_BIND_TEXT(row->sql_text);
  QST_RESULT_STORE_BIND_TEXT(row->sql_sha256);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_SMALLINT, row->sql_shape_mask);
  QST_RESULT_STORE_BIND_TEXT(row->taosd_pid);
  QST_RESULT_STORE_BIND_TEXT(row->taosc_pid);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_TINYINT, row->execution_status);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_BIGINT, row->result_row_count);
  if (row->execution_status == QST_EXEC_SUCCESS) {
    QST_RESULT_STORE_BIND_NULL(TSDB_DATA_TYPE_INT);
  } else {
    QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_INT, row->error_code);
  }
  QST_RESULT_STORE_BIND_NULL(TSDB_DATA_TYPE_BINARY);
  QST_RESULT_STORE_BIND_NULL(TSDB_DATA_TYPE_BINARY);
  QST_RESULT_STORE_BIND_NULL(TSDB_DATA_TYPE_BINARY);
  QST_RESULT_STORE_BIND_NULL(TSDB_DATA_TYPE_BINARY);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_TINYINT, row->dialect);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_TINYINT,
                              row->generation_abnormal);
  QST_RESULT_STORE_BIND_VALUE(TSDB_DATA_TYPE_TINYINT,
                              row->generation_unsupported_fallback);
#undef QST_RESULT_STORE_BIND_NULL
#undef QST_RESULT_STORE_BIND_TEXT
#undef QST_RESULT_STORE_BIND_VALUE
  return index == QST_RESULT_STORE_STATEMENT_BIND_COUNT ? QST_OK :
                                                            QST_E_INTERNAL;
}

static void qstResultStoreStatementBindRowsDestroy(
    SQstResultStoreStatementBindRow *rows, uint32_t row_count) {
  if (NULL == rows) {
    return;
  }
  for (uint32_t index = 0; index < row_count; ++index) {
    free(rows[index].sql_text);
  }
  free(rows);
}

/* The bind arrays live inside their owning rows, so they must be copied into
 * a contiguous array before qst_native_execute_stmt_batch advances by row. */
static EQstCode qstResultStoreFlattenStatementBinds(
    const SQstResultStoreStatementBindRow *rows, uint32_t row_count,
    TAOS_MULTI_BIND **out_binds) {
  TAOS_MULTI_BIND *binds = NULL;
  uint64_t bind_count = 0;

  if (NULL == rows || row_count == 0 || NULL == out_binds) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_binds = NULL;
  bind_count = (uint64_t)row_count * QST_RESULT_STORE_STATEMENT_BIND_COUNT;
  if (bind_count > SIZE_MAX / sizeof(*binds)) {
    return QST_E_RESOURCE;
  }
  binds = calloc((size_t)bind_count, sizeof(*binds));
  if (NULL == binds) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < row_count; ++index) {
    memcpy(binds + (uint64_t)index * QST_RESULT_STORE_STATEMENT_BIND_COUNT,
           rows[index].binds, sizeof(rows[index].binds));
  }
  *out_binds = binds;
  return QST_OK;
}

/* Prepares the normal statement-result batch without SQL quoting. Caller
 * falls back to the exact text DML whenever this returns before execute. */
static EQstCode qstResultStoreBuildStatementBindBatch(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPersistPayload *payloads, uint32_t payload_count,
    SQstResultStoreStatementBindRow **out_rows, char *out_statement,
    uint64_t statement_bytes) {
  SQstResultStoreStatementBindRow *rows = NULL;
  const char *target_name = NULL;
  char task_table[256] = {0};
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == store ||
      NULL == payloads || payload_count == 0 || NULL == out_rows ||
      NULL == out_statement || statement_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_rows = NULL;
  target_name = qstResultStoreTargetNameById(payloads[0].target);
  if (NULL == target_name) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstResultStoreTaskTableName(task_table, sizeof(task_table), "stmt",
                                  mapping->header->run_id, target_name,
                                  payloads[0].reserved0) != QST_OK) {
    return QST_E_RESOURCE;
  }
  written = snprintf(
      out_statement, (size_t)statement_bytes,
      "INSERT INTO %s (ts,gid,ended_at,sid,sql_text,sql_sha256,"
      "sql_shape_mask,taosd_pid,taosc_pid,execution_status,result_row_count,"
      "error_code,error_message,taosd_log_ranges,taosc_log_ranges,core_relative_path,"
      "dialect,generation_abnormal,generation_unsupported_fallback) VALUES "
      "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
      task_table);
  if (written < 0 || (uint64_t)written >= statement_bytes) {
    return QST_E_RESOURCE;
  }
  rows = calloc(payload_count, sizeof(*rows));
  if (NULL == rows) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < payload_count; ++index) {
    const SQstPersistPayload *payload = &payloads[index];
    SQstTaskPersistSnapshot snapshot = {0};
    SQstResultStoreStatementBindRow *row = &rows[index];

    if (payload->persist_kind != QST_PERSIST_STATEMENT ||
        payload->target != payloads[0].target ||
        payload->reserved0 != payloads[0].reserved0 || payload->record_seq == 0 ||
        qstResultStorePersistTimestampMs(mapping, payload->record_seq,
                                         &row->timestamp_ms) != QST_OK ||
        qst_task_get_persist_snapshot(mapping, payload->gid,
                                      payload->dispatch_epoch,
                                      payload->transition_seq, &snapshot) !=
            QST_OK ||
        snapshot.target != payload->target ||
        !qstRefIsEmpty(snapshot.final_payload.core_path_ref)) {
      code = QST_E_INVALID_ARGUMENT;
      break;
    }
    code = qstResultStoreCopyText(mapping, snapshot.sql_ref, snapshot.gid,
                                  &row->sql_text);
    if (code != QST_OK || strlen(row->sql_text) >
                              QST_RESULT_STORE_STATEMENT_SQL_BYTES) {
      code = code == QST_OK ? QST_E_INVALID_ARGUMENT : code;
      break;
    }
    qstPendingHex(snapshot.sql_sha256.bytes, sizeof(snapshot.sql_sha256.bytes),
                  row->sql_sha256);
    row->gid = (int64_t)snapshot.gid;
    row->ended_at_ms =
        (int64_t)(snapshot.final_payload.ended_ts_ns / INT64_C(1000000));
    row->sid = (int64_t)snapshot.final_payload.sid;
    row->sql_shape_mask = (int16_t)qstResultStoreSqlShapeMask(row->sql_text);
    row->execution_status = (int8_t)snapshot.final_payload.execution_status;
    row->result_row_count = snapshot.final_payload.result_row_count;
    row->error_code = snapshot.final_payload.execution_error_code;
    row->dialect = (int8_t)snapshot.dialect;
    row->generation_abnormal = (int8_t)snapshot.generation_contains_abnormal;
    row->generation_unsupported_fallback =
        (int8_t)snapshot.generation_contains_unsupported_fallback;
    written = snprintf(row->taosd_pid, sizeof(row->taosd_pid), "%d",
                       snapshot.final_payload.taosd_pid);
    if (written < 0 || (uint64_t)written >= sizeof(row->taosd_pid)) {
      code = QST_E_RESOURCE;
      break;
    }
    written = snprintf(row->taosc_pid, sizeof(row->taosc_pid), "%d",
                       snapshot.final_payload.taosc_pid);
    if (written < 0 || (uint64_t)written >= sizeof(row->taosc_pid)) {
      code = QST_E_RESOURCE;
      break;
    }
    code = qstResultStoreStatementBindRowInitialize(row);
    if (code != QST_OK) {
      break;
    }
  }
  if (code != QST_OK) {
    qstResultStoreStatementBindRowsDestroy(rows, payload_count);
    return code;
  }
  *out_rows = rows;
  return QST_OK;
}

EQstCode qst_result_store_write_batch(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPersistPayload *payloads, uint32_t payload_count,
    int32_t *out_taos_code) {
  static const char kValuesPrefix[] = " VALUES ";
  char *batch_dml = NULL;
  uint64_t batch_bytes = 0;
  uint64_t batch_capacity = 0;
  uint64_t prefix_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == store ||
      NULL == store->native_taos || NULL == payloads || payload_count == 0 ||
      NULL == out_taos_code || store->run_id != mapping->header->run_id) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_taos_code = QST_E_RESOURCE;
  if (payloads[0].persist_kind == QST_PERSIST_STATEMENT) {
    SQstResultStoreStatementBindRow *rows = NULL;
    TAOS_MULTI_BIND *binds = NULL;
    char statement[512] = {0};
    int32_t execute_attempted = 0;

    code = qstResultStoreBuildStatementBindBatch(
        mapping, store, payloads, payload_count, &rows, statement,
        sizeof(statement));
    if (code == QST_OK) {
      code = qstResultStoreFlattenStatementBinds(rows, payload_count, &binds);
    }
    if (code == QST_OK) {
      code = qst_native_execute_stmt_batch(
          (SQstNativeClient *)store->native_taos, statement, binds,
          payload_count, QST_RESULT_STORE_STATEMENT_BIND_COUNT,
          &execute_attempted);
    }
    free(binds);
    qstResultStoreStatementBindRowsDestroy(rows, payload_count);
    if (code == QST_OK) {
      *out_taos_code = 0;
      return QST_OK;
    }
    if (execute_attempted != 0) {
      *out_taos_code = code;
      return code;
    }
    /* No statement execute reached the server, so preserve the established
     * SQL batch path for old clients and unsupported row shapes. */
    code = QST_OK;
  }
  for (uint32_t index = 0; index < payload_count; ++index) {
    const SQstPersistPayload *payload = &payloads[index];
    char *dml = NULL;
    const char *values = NULL;
    uint64_t dml_bytes = 0;
    uint64_t row_bytes = 0;
    uint64_t next_bytes = 0;

    if (payload->record_seq == 0 ||
        (payload->persist_kind != QST_PERSIST_STATEMENT &&
         payload->persist_kind != QST_PERSIST_INITIAL_RESULT &&
         payload->persist_kind != QST_PERSIST_HISTORICAL_VERDICT) ||
        payload->persist_kind != payloads[0].persist_kind ||
        payload->target != payloads[0].target ||
        payload->reserved0 != payloads[0].reserved0) {
      code = QST_E_INVALID_ARGUMENT;
      break;
    }
    code = qst_result_store_build_task_dml(mapping, store, payload, &dml);
    values = code == QST_OK ? strstr(dml, kValuesPrefix) : NULL;
    if (code != QST_OK || NULL == values) {
      free(dml);
      code = code == QST_OK ? QST_E_RESULT_STORE : code;
      break;
    }
    dml_bytes = strlen(dml);
    prefix_bytes = index == 0 ?
        (uint64_t)(values - dml) + strlen(kValuesPrefix) : prefix_bytes;
    row_bytes = dml_bytes - ((uint64_t)(values - dml) + strlen(kValuesPrefix));
    if (index != 0 && (dml_bytes < prefix_bytes ||
                       memcmp(batch_dml, dml, (size_t)prefix_bytes) != 0)) {
      free(dml);
      code = QST_E_RESULT_STORE;
      break;
    }
    if (index == 0) {
      if (prefix_bytes > UINT64_MAX - row_bytes - 1U) {
        free(dml);
        code = QST_E_RESOURCE;
        break;
      }
      next_bytes = prefix_bytes + row_bytes + 1U;
    } else if (batch_bytes > UINT64_MAX - row_bytes - 1U) {
      free(dml);
      code = QST_E_RESOURCE;
      break;
    }
    if (index != 0) {
      next_bytes = batch_bytes + row_bytes + 1U;
    }
    code = qstResultStoreReserveBatchDml(&batch_dml, &batch_capacity,
                                         next_bytes);
    if (code != QST_OK) {
      free(dml);
      break;
    }
    if (index == 0) {
      memcpy(batch_dml, dml, (size_t)prefix_bytes);
      batch_bytes = prefix_bytes;
    }
    memcpy(batch_dml + batch_bytes, values + strlen(kValuesPrefix),
           (size_t)row_bytes);
    batch_bytes += row_bytes;
    batch_dml[batch_bytes] = '\0';
    free(dml);
  }
  if (code == QST_OK) {
    code = qstResultStoreExecuteWithRecovery(
        mapping, store, batch_dml, payloads[payload_count - 1U].record_seq);
  }
  free(batch_dml);
  *out_taos_code = code == QST_OK ? 0 : code;
  return code;
}

/* Closes only the process-local connector. The immutable run and local
 * pending paths survive a failed reconnect so callers can continue staging. */
static void qstResultStoreCloseNative(SQstResultStore *store) {
  SQstNativeClient *client = NULL;

  if (NULL == store || NULL == store->native_taos) {
    return;
  }
  client = (SQstNativeClient *)store->native_taos;
  (void)qst_native_close(client);
  free(client);
  store->native_taos = NULL;
}

static EQstCode qstResultStoreOpenNative(SQstResultStore *store) {
  SQstNativeClient *client = NULL;
  EQstCode code = QST_OK;

  if (NULL == store || store->run_id == 0 || store->native_taos != NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  client = (SQstNativeClient *)calloc(1, sizeof(*client));
  if (NULL == client) {
    return QST_E_RESOURCE;
  }
  code = qst_native_open_with_endpoint_credentials(
      client, store->endpoint_host[0] == '\0' ? NULL : store->endpoint_host,
      store->endpoint_port,
      store->config_dir[0] == '\0' ? NULL : store->config_dir,
      store->user[0] == '\0' ? NULL : store->user,
      store->password[0] == '\0' ? NULL : store->password);
  if (code != QST_OK) {
    free(client);
    return code;
  }
  store->native_taos = client;
  return QST_OK;
}

static int32_t qstResultStoreReadinessIsAvailable(const char *readiness) {
  tools_cJSON *root = NULL;
  tools_cJSON *available = NULL;
  int32_t is_available = 0;

  if (NULL == readiness || readiness[0] == '\0') {
    return 0;
  }
  root = tools_cJSON_ParseWithOpts(readiness, NULL, 1);
  available = root == NULL ? NULL :
      tools_cJSON_GetObjectItemCaseSensitive(root, "available");
  is_available = available != NULL && tools_cJSON_IsTrue(available);
  tools_cJSON_Delete(root);
  return is_available;
}

/* The result DB is pre-deployed. Recovery is limited to reconnecting the
 * native client and waiting for a healthy endpoint; it never starts or stops
 * a service owned by the test machine. */
EQstCode qst_result_store_recover(SQstShmMapping *mapping,
                                  SQstResultStore *store, uint64_t run_id,
                                  uint32_t recovery_seq) {
  EQstCode code = QST_E_RESULT_STORE;

  if (NULL == mapping || NULL == mapping->header || NULL == store ||
      run_id == 0 || run_id != store->run_id ||
      run_id != mapping->header->run_id || recovery_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t attempt = 0; attempt < QST_RESULT_STORE_RECOVERY_ATTEMPTS;
       ++attempt) {
    SQstNativeClient *client = NULL;
    char readiness[1024] = {0};

    if (store->native_taos == NULL) {
      code = qstResultStoreOpenNative(store);
      if (code != QST_OK) {
        goto _retry;
      }
    }
    client = (SQstNativeClient *)store->native_taos;
    code = qst_native_readiness_json(client, readiness, sizeof(readiness));
    if (code == QST_OK && qstResultStoreReadinessIsAvailable(readiness) != 0) {
      code = qstResultStoreEnsureSchema(client);
      if (code == QST_OK) {
        return QST_OK;
      }
    } else if (code == QST_OK) {
      code = QST_E_RESULT_STORE;
    }
    qstResultStoreCloseNative(store);

_retry:
    if (attempt + 1U < QST_RESULT_STORE_RECOVERY_ATTEMPTS &&
        poll(NULL, 0, QST_RESULT_STORE_RECOVERY_WAIT_MILLISECONDS) < 0 &&
        errno != EINTR) {
      break;
    }
  }
  qstResultStoreCloseNative(store);
  (void)fprintf(stderr,
                "taosQueryTest: result store recovery exhausted run_id=%llu sequence=%u code=%d\n",
                (unsigned long long)run_id, recovery_seq, code);
  return code == QST_OK ? QST_E_RESULT_STORE : code;
}

/* A failed DML gets one complete recovery cycle before its caller chooses the
 * pending-results fallback. A healthy per-worker connector writes directly:
 * recovering before every record serializes readiness/schema probes under
 * high judge-worker concurrency and can expire the execute-queue lease. */
static EQstCode qstResultStoreExecuteWithRecovery(
    SQstShmMapping *mapping, SQstResultStore *store, const char *sql,
    uint64_t record_seq) {
  uint32_t recovery_seq = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == sql || record_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  recovery_seq = record_seq > UINT32_MAX ? UINT32_MAX : (uint32_t)record_seq;
  if (NULL != store->native_taos) {
    code = qst_native_execute((SQstNativeClient *)store->native_taos, sql);
  } else {
    code = QST_E_RESULT_STORE;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: result store DML recovery run_id=%llu "
                  "record_seq=%llu code=%d\n",
                  (unsigned long long)store->run_id,
                  (unsigned long long)record_seq, code);
    code = qst_result_store_recover(mapping, store, store->run_id, recovery_seq);
    if (code == QST_OK) {
      code = qst_native_execute((SQstNativeClient *)store->native_taos, sql);
    }
  }
  return code;
}

EQstCode qst_pending_write(SQstShmMapping *mapping,
                           const SQstPendingStore *store,
                           const SQstPendingRecord *record,
                           SQstRef *out_pending_file_ref) {
  SQstPendingBatch stream = {.fd = -1};
  SQstRef path_ref = {0};
  EQstCode code = QST_OK;

  if (NULL == store || NULL == record ||
      NULL == out_pending_file_ref || NULL == store->pending_dir ||
      NULL == record->stable_key || record->stable_key[0] == '\0' ||
      record->record_seq == 0 ||
      (mapping != NULL && (mapping->header == NULL ||
                           record->run_id != mapping->header->run_id)) ||
      (mapping == NULL && record->run_id != store->run_id) ||
      (record->payload_bytes != 0 && NULL == record->payload)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_pending_batch_begin(mapping, store, &stream);
  if (code == QST_OK) {
    code = qst_pending_batch_append(&stream, record);
  }
  if (code == QST_OK) {
    code = qst_pending_batch_flush(&stream);
  }
  if (code != QST_OK) {
    qst_pending_batch_abort(&stream);
    return code;
  }
  if (mapping != NULL) {
    code = qst_block_alloc(mapping, record->gid, 1, strlen(stream.final_path),
                           &path_ref);
    if (code != QST_OK) {
      qst_pending_batch_abort(&stream);
      return code;
    }
    code = qst_block_write_seal(mapping, &path_ref, stream.final_path,
                                strlen(stream.final_path));
    if (code != QST_OK) {
      (void)qst_block_abort(mapping, path_ref, record->gid);
      qst_pending_batch_abort(&stream);
      return code;
    }
  }
  qst_pending_batch_abort(&stream);
  *out_pending_file_ref = path_ref;
  return QST_OK;
}

EQstCode qst_pending_batch_begin(SQstShmMapping *mapping,
                                 const SQstPendingStore *store,
                                 SQstPendingBatch *out_batch) {
  if (NULL == store || NULL == store->pending_dir || NULL == out_batch ||
      (NULL != mapping && (NULL == mapping->header ||
                           mapping->header->run_id != store->run_id))) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_batch, 0, sizeof(*out_batch));
  out_batch->mapping = mapping;
  out_batch->store = store;
  if (qstPendingStreamPath(out_batch->final_path, sizeof(out_batch->final_path),
                           store->pending_dir) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  out_batch->fd = open(out_batch->final_path,
                       O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0640);
  return out_batch->fd >= 0 ? QST_OK : QST_E_PENDING_IO;
}

EQstCode qst_pending_batch_append(SQstPendingBatch *batch,
                                  const SQstPendingRecord *record) {
  SQstPendingStreamRecordHeader header = {0};
  uint64_t required_bytes = 0;
  uint64_t next_capacity = 0;
  int64_t created_at_ns = 0;
  int32_t quota_reserved = 0;
  EQstCode code = QST_OK;

  if (NULL == batch || NULL == batch->store || NULL == record ||
      record->run_id != batch->store->run_id || batch->fd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (record->record_seq == 0 || record->payload_bytes == 0 ||
      NULL == record->payload || qstPendingKindName(record->persist_kind) == NULL ||
      record->payload_bytes > UINT64_MAX - sizeof(header) ||
      record->payload_bytes > SIZE_MAX - sizeof(header)) {
    return QST_E_INVALID_ARGUMENT;
  }
  created_at_ns = record->created_at_ns == 0 ? qstPendingNowNs() :
                                               record->created_at_ns;
  if (created_at_ns <= 0) {
    return QST_E_RESOURCE;
  }
  header.magic = QST_PENDING_STREAM_MAGIC;
  header.version = QST_PENDING_STREAM_VERSION;
  header.persist_kind = record->persist_kind;
  header.target = record->target;
  header.run_id = record->run_id;
  header.gid = record->gid;
  header.record_seq = record->record_seq;
  header.created_at_ns = created_at_ns;
  header.payload_bytes = record->payload_bytes;
  required_bytes = batch->byte_count + sizeof(header) + record->payload_bytes;
  if (required_bytes < batch->byte_count) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    code = qstPendingReserveQuota(batch->mapping, batch->store,
                                  sizeof(header) + record->payload_bytes,
                                  &quota_reserved);
  }
  if (code == QST_OK && required_bytes > batch->buffer_capacity) {
    next_capacity = batch->buffer_capacity == 0 ? UINT64_C(65536) :
                                                 batch->buffer_capacity;
    while (next_capacity < required_bytes) {
      if (next_capacity > UINT64_MAX / 2U) {
        next_capacity = required_bytes;
        break;
      }
      next_capacity *= 2U;
    }
    if (next_capacity > SIZE_MAX) {
      code = QST_E_RESOURCE;
    } else {
      uint8_t *next_buffer =
          (uint8_t *)realloc(batch->buffer, (size_t)next_capacity);

      if (NULL == next_buffer) {
        code = QST_E_RESOURCE;
      } else {
        batch->buffer = next_buffer;
        batch->buffer_capacity = next_capacity;
      }
    }
  }
  if (code == QST_OK) {
    memcpy(batch->buffer + batch->byte_count, &header, sizeof(header));
    memcpy(batch->buffer + batch->byte_count + sizeof(header), record->payload,
           (size_t)record->payload_bytes);
    batch->byte_count = required_bytes;
  }
  if (code != QST_OK) {
    qstPendingReleaseQuota(batch->mapping,
                           sizeof(header) + record->payload_bytes,
                           quota_reserved);
    return code;
  }
  if (batch->record_count == 0) {
    batch->first_record_seq = record->record_seq;
  }
  batch->last_record_seq = record->record_seq;
  ++batch->record_count;
  batch->quota_reserved = 1;
  return QST_OK;
}

int32_t qst_pending_batch_ready(const SQstPendingBatch *batch) {
  return NULL != batch && batch->byte_count >= QST_PENDING_STREAM_APPEND_BYTES;
}

EQstCode qst_pending_batch_flush(SQstPendingBatch *batch) {
  if (NULL == batch || NULL == batch->store || batch->fd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (batch->byte_count != 0) {
    return qstPendingStreamWrite(batch);
  }
  batch->quota_reserved = 0;
  return QST_OK;
}

void qst_pending_batch_abort(SQstPendingBatch *batch) {
  if (NULL == batch) {
    return;
  }
  if (batch->fd >= 0) {
    (void)close(batch->fd);
    batch->fd = -1;
  }
  qstPendingReleaseQuota(batch->mapping, batch->byte_count,
                         batch->quota_reserved);
  free(batch->buffer);
  batch->buffer = NULL;
  memset(batch, 0, sizeof(*batch));
  batch->fd = -1;
}

typedef struct {
  uint64_t record_seq;
  int32_t is_batch;
  char name[QST_PENDING_PATH_BYTES];
} SQstPendingFile;

static int32_t qstPendingHexIsValid(const char *text, uint64_t expected_bytes) {
  uint64_t length = 0;

  if (NULL == text || expected_bytes > (UINT64_MAX - 1U) / 2U) {
    return 0;
  }
  length = strlen(text);
  if (length != expected_bytes * 2U) {
    return 0;
  }
  for (uint64_t index = 0; index < length; ++index) {
    if (!((text[index] >= '0' && text[index] <= '9') ||
          (text[index] >= 'a' && text[index] <= 'f'))) {
      return 0;
    }
  }
  return 1;
}

/* Decodes lower-case protocol hex into a NUL-terminated byte buffer. */
static EQstCode qstPendingDecodeHex(const char *hex, char **out_bytes,
                                    uint64_t *out_byte_count) {
  uint64_t hex_bytes = 0;
  char *bytes = NULL;

  if (NULL == hex || NULL == out_bytes || NULL == out_byte_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_bytes = NULL;
  *out_byte_count = 0;
  hex_bytes = strlen(hex);
  if ((hex_bytes & 1U) != 0 || hex_bytes / 2U >= SIZE_MAX ||
      !qstPendingHexIsValid(hex, hex_bytes / 2U)) {
    return QST_E_SHM_CORRUPT;
  }
  bytes = (char *)calloc((size_t)(hex_bytes / 2U) + 1U, 1);
  if (NULL == bytes) {
    return QST_E_RESOURCE;
  }
  for (uint64_t index = 0; index < hex_bytes; index += 2U) {
    const uint8_t high = (uint8_t)(hex[index] <= '9' ? hex[index] - '0' :
                                                        hex[index] - 'a' + 10);
    const uint8_t low = (uint8_t)(hex[index + 1U] <= '9' ?
                                   hex[index + 1U] - '0' : hex[index + 1U] - 'a' + 10);

    bytes[index / 2U] = (char)((high << 4U) | low);
  }
  if (memchr(bytes, '\0', (size_t)(hex_bytes / 2U)) != NULL) {
    free(bytes);
    return QST_E_SHM_CORRUPT;
  }
  *out_bytes = bytes;
  *out_byte_count = hex_bytes / 2U;
  return QST_OK;
}

static int32_t qstPendingHexSegmentIsValid(const char *text, uint64_t bytes) {
  if (NULL == text) {
    return 0;
  }
  for (uint64_t index = 0; index < bytes; ++index) {
    if (!((text[index] >= '0' && text[index] <= '9') ||
          (text[index] >= 'a' && text[index] <= 'f'))) {
      return 0;
    }
  }
  return 1;
}

static int32_t qstPendingParseFileName(const char *name, uint64_t *out_record_seq,
                                       int32_t *out_is_temporary) {
  const char *cursor = NULL;
  const char *digest = NULL;
  char *end = NULL;
  uint64_t record_seq = 0;

  if (NULL == name || NULL == out_record_seq || NULL == out_is_temporary ||
      strncmp(name, "p.", 2) != 0) {
    return 0;
  }
  errno = 0;
  cursor = name + 2;
  record_seq = strtoull(cursor, &end, 10);
  if (errno != 0 || end == cursor || record_seq == 0 || end == NULL ||
      *end != '.') {
    return 0;
  }
  digest = end + 1;
  if (strlen(digest) < QST_PENDING_DIGEST_HEX_BYTES + strlen(".json")) {
    return 0;
  }
  if (!qstPendingHexSegmentIsValid(digest, QST_PENDING_DIGEST_HEX_BYTES)) {
    return 0;
  }
  if (strcmp(digest + QST_PENDING_DIGEST_HEX_BYTES, ".json") == 0) {
    *out_is_temporary = 0;
  } else if (strcmp(digest + QST_PENDING_DIGEST_HEX_BYTES, ".json.tmp") == 0) {
    *out_is_temporary = 1;
  } else {
    return 0;
  }
  *out_record_seq = record_seq;
  return 1;
}

static int32_t qstPendingParseBatchFileName(const char *name,
                                            uint64_t *out_first_record_seq,
                                            uint64_t *out_last_record_seq,
                                            int32_t *out_is_temporary) {
  const char *cursor = NULL;
  char *end = NULL;
  uint64_t first_record_seq = 0;
  uint64_t last_record_seq = 0;

  if (NULL == name || NULL == out_first_record_seq ||
      NULL == out_last_record_seq || NULL == out_is_temporary ||
      strncmp(name, "b.", 2) != 0) {
    return 0;
  }
  errno = 0;
  cursor = name + 2;
  first_record_seq = strtoull(cursor, &end, 10);
  if (errno != 0 || end == cursor || first_record_seq == 0 || *end != '.') {
    return 0;
  }
  cursor = end + 1;
  errno = 0;
  last_record_seq = strtoull(cursor, &end, 10);
  if (errno != 0 || end == cursor || last_record_seq < first_record_seq) {
    return 0;
  }
  if (strcmp(end, ".jsonl") == 0) {
    *out_is_temporary = 0;
  } else if (strcmp(end, ".jsonl.tmp") == 0) {
    *out_is_temporary = 1;
  } else {
    return 0;
  }
  *out_first_record_seq = first_record_seq;
  *out_last_record_seq = last_record_seq;
  return 1;
}

static int qstPendingFileCompare(const void *left, const void *right) {
  const SQstPendingFile *left_file = (const SQstPendingFile *)left;
  const SQstPendingFile *right_file = (const SQstPendingFile *)right;

  if (left_file->record_seq < right_file->record_seq) {
    return -1;
  }
  if (left_file->record_seq > right_file->record_seq) {
    return 1;
  }
  return strcmp(left_file->name, right_file->name);
}

static EQstCode qstPendingDiscard(const SQstResultStore *store,
                                  const char *name) {
  char source[QST_PENDING_PATH_BYTES] = {0};
  int32_t written = 0;

  if (NULL == store || NULL == name || store->pending_dir[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  written = snprintf(source, sizeof(source), "%s/%s", store->pending_dir, name);
  if (written < 0 || (uint64_t)written >= sizeof(source)) {
    return QST_E_PENDING_IO;
  }
  return unlink(source) == 0 ? QST_OK : QST_E_PENDING_IO;
}

static EQstCode qstPendingReadFile(const char *path, char **out_text) {
  struct stat status = {0};
  char *text = NULL;
  int32_t fd = -1;
  uint64_t read_bytes = 0;

  if (NULL == path || NULL == out_text || stat(path, &status) != 0 ||
      !S_ISREG(status.st_mode) || status.st_size <= 0 ||
      (uint64_t)status.st_size >= SIZE_MAX) {
    return QST_E_PENDING_IO;
  }
  text = (char *)calloc((size_t)status.st_size + 1U, 1);
  if (NULL == text) {
    return QST_E_RESOURCE;
  }
  fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    free(text);
    return QST_E_PENDING_IO;
  }
  while (read_bytes < (uint64_t)status.st_size) {
    const ssize_t bytes = read(fd, text + read_bytes,
                               (size_t)((uint64_t)status.st_size - read_bytes));

    if (bytes < 0 && errno == EINTR) {
      continue;
    }
    if (bytes <= 0) {
      (void)close(fd);
      free(text);
      return QST_E_PENDING_IO;
    }
    read_bytes += (uint64_t)bytes;
  }
  if (close(fd) != 0) {
    free(text);
    return QST_E_PENDING_IO;
  }
  *out_text = text;
  return QST_OK;
}

static tools_cJSON *qstPendingRequiredItem(const tools_cJSON *root,
                                            const char *name, int32_t is_string) {
  tools_cJSON *item = NULL;

  if (NULL == root || NULL == name) {
    return NULL;
  }
  item = tools_cJSON_GetObjectItemCaseSensitive(root, name);
  if ((is_string != 0 && !tools_cJSON_IsString(item)) ||
      (is_string == 0 && !tools_cJSON_IsNumber(item))) {
    return NULL;
  }
  return item;
}

/* Replays one JSON record. Direct objects provide name and are removed after a
 * successful replay; records from a published batch provide text and leave
 * deletion to the batch owner after every line succeeds. */
static EQstCode qstPendingReplayOne(SQstShmMapping *mapping,
                                    SQstResultStore *store,
                                    uint64_t run_id, const char *name,
                                    const char *input_text,
                                    uint64_t batch_record_seq,
                                    int32_t remove_on_success) {
  SQstSha256 sha = {0};
  SQstDigest256 key_digest = {0};
  SQstDigest256 payload_digest = {0};
  tools_cJSON *root = NULL;
  tools_cJSON *format = NULL;
  tools_cJSON *record_kind = NULL;
  tools_cJSON *stored_run_id = NULL;
  tools_cJSON *target = NULL;
  tools_cJSON *ds_fingerprint = NULL;
  tools_cJSON *gid = NULL;
  tools_cJSON *record_seq = NULL;
  tools_cJSON *stable_key = NULL;
  tools_cJSON *payload_sha256 = NULL;
  tools_cJSON *payload = NULL;
  tools_cJSON *created_at = NULL;
  char path[QST_PENDING_PATH_BYTES] = {0};
  char key_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};
  char payload_hex[QST_PENDING_DIGEST_HEX_BYTES + 1] = {0};
  char *stable_key_hex = NULL;
  char *raw_payload = NULL;
  char *text = NULL;
  uint64_t expected_record_seq = 0;
  uint32_t persist_kind = 0;
  int32_t is_temporary = 0;
  int32_t owns_text = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store ||
      ((NULL == name ||
        qstPendingParseFileName(name, &expected_record_seq, &is_temporary) == 0 ||
        is_temporary != 0) &&
       (NULL == input_text || batch_record_seq == 0))) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != input_text) {
    expected_record_seq = batch_record_seq;
    text = (char *)input_text;
  } else {
    if (snprintf(path, sizeof(path), "%s/%s", store->pending_dir, name) < 0 ||
        strlen(path) >= sizeof(path)) {
      return QST_E_PENDING_IO;
    }
    code = qstPendingReadFile(path, &text);
    if (code != QST_OK) {
      return code;
    }
    owns_text = 1;
  }
  root = tools_cJSON_ParseWithOpts(text, NULL, 1);
  format = qstPendingRequiredItem(root, "format_version", 0);
  record_kind = qstPendingRequiredItem(root, "record_kind", 1);
  stored_run_id = qstPendingRequiredItem(root, "run_id", 0);
  target = qstPendingRequiredItem(root, "target", 0);
  ds_fingerprint = qstPendingRequiredItem(root, "ds_fingerprint", 1);
  gid = qstPendingRequiredItem(root, "gid", 0);
  record_seq = qstPendingRequiredItem(root, "record_seq", 0);
  stable_key = qstPendingRequiredItem(root, "stable_key", 1);
  payload_sha256 = qstPendingRequiredItem(root, "payload_sha256", 1);
  payload = qstPendingRequiredItem(root, "payload", 1);
  created_at = qstPendingRequiredItem(root, "created_at", 0);
  if (NULL == root || !tools_cJSON_IsObject(root) || NULL == format ||
      NULL == record_kind || NULL == stored_run_id || NULL == target ||
      NULL == ds_fingerprint ||
      NULL == gid || NULL == record_seq || NULL == stable_key ||
      NULL == payload_sha256 || NULL == payload || NULL == created_at ||
      format->valueint != QST_PENDING_FORMAT_VERSION ||
      stored_run_id->valueint < 0 || (uint64_t)stored_run_id->valueint != run_id ||
      record_seq->valueint <= 0 ||
      (uint64_t)record_seq->valueint != expected_record_seq ||
      created_at->valueint <= 0 || target->valueint < 0 || gid->valueint < 0 ||
      (persist_kind = record_kind->valuestring != NULL &&
                              strcmp(record_kind->valuestring, "runs") == 0 ?
                          QST_PERSIST_RUN_UPDATE :
                      record_kind->valuestring != NULL &&
                              strcmp(record_kind->valuestring, "datasets") == 0 ?
                          QST_PERSIST_DATASET :
                      record_kind->valuestring != NULL &&
                              strcmp(record_kind->valuestring, "statements") == 0 ?
                          QST_PERSIST_STATEMENT :
                      record_kind->valuestring != NULL &&
                              strcmp(record_kind->valuestring, "results") == 0 ?
                          QST_PERSIST_INITIAL_RESULT :
                      record_kind->valuestring != NULL &&
                              strcmp(record_kind->valuestring, "historical_verdicts") == 0 ?
                          QST_PERSIST_HISTORICAL_VERDICT : 0) == 0 ||
      !qstPendingHexIsValid(payload_sha256->valuestring,
                            QST_PENDING_DIGEST_HEX_BYTES / 2U) ||
      !qstPendingHexIsValid(payload->valuestring, strlen(payload->valuestring) / 2U)) {
    code = QST_E_SHM_CORRUPT;
    goto _return;
  }
  qst_sha256_init(&sha);
  qst_sha256_update(&sha, (const uint8_t *)stable_key->valuestring,
                    strlen(stable_key->valuestring));
  qst_sha256_final(&sha, key_digest.bytes);
  qstPendingHex(key_digest.bytes, sizeof(key_digest.bytes), key_hex);
  if (NULL != name) {
    const char *digest = strchr(name + 2, '.');

    if (NULL == digest || strncmp(digest + 1, key_hex,
                                  QST_PENDING_DIGEST_HEX_BYTES) != 0) {
      code = QST_E_SHM_CORRUPT;
      goto _return;
    }
  }
  qst_sha256_init(&sha);
  qst_sha256_update(&sha, (const uint8_t *)payload->valuestring,
                    strlen(payload->valuestring));
  qst_sha256_final(&sha, payload_digest.bytes);
  qstPendingHex(payload_digest.bytes, sizeof(payload_digest.bytes), payload_hex);
  if (strcmp(payload_hex, payload_sha256->valuestring) != 0) {
    code = QST_E_SHM_CORRUPT;
    goto _return;
  }
  if (persist_kind == QST_PERSIST_INITIAL_RESULT) {
    char *kind_probe = NULL;
    uint64_t kind_probe_bytes = 0;

    if (qstPendingDecodeHex(payload->valuestring, &kind_probe,
                            &kind_probe_bytes) == QST_OK) {
      if (strstr(kind_probe, ",'ai',") != NULL) {
        persist_kind = QST_PERSIST_MANUAL_AI_RESULT;
      }
      free(kind_probe);
    }
  }
  if (persist_kind == QST_PERSIST_STATEMENT ||
      persist_kind == QST_PERSIST_INITIAL_RESULT ||
      persist_kind == QST_PERSIST_MANUAL_AI_RESULT) {
    SQstDigest256 parsed_fingerprint = {0};
    const char *target_name = qstResultStoreTargetNameById((uint32_t)target->valueint);
    char expected_prefix[128] = {0};
    uint32_t worker_count = 0;
    int32_t prefix_bytes = 0;

    code = qstPendingDecodeHex(payload->valuestring, &raw_payload, &(uint64_t){0});
    if (code != QST_OK) {
      goto _return;
    }
    if (strncmp(raw_payload, "INSERT INTO res_db.",
                strlen("INSERT INTO res_db.")) != 0) {
      free(raw_payload);
      raw_payload = NULL;
    } else if (NULL == target_name ||
        !qstPendingHexIsValid(ds_fingerprint->valuestring,
                              sizeof(parsed_fingerprint.bytes))) {
      code = QST_E_SHM_CORRUPT;
      goto _return;
    } else {
      for (uint32_t index = 0; index < sizeof(parsed_fingerprint.bytes); ++index) {
        const char high = ds_fingerprint->valuestring[index * 2U];
        const char low = ds_fingerprint->valuestring[index * 2U + 1U];
        const uint8_t high_value =
            (uint8_t)(high <= '9' ? high - '0' : high - 'a' + 10);
        const uint8_t low_value =
            (uint8_t)(low <= '9' ? low - '0' : low - 'a' + 10);

        parsed_fingerprint.bytes[index] =
            (uint8_t)((high_value << 4U) | low_value);
      }
      prefix_bytes = snprintf(
          expected_prefix, sizeof(expected_prefix),
          "INSERT INTO res_db.%s_%llu_%s",
          persist_kind == QST_PERSIST_STATEMENT ? "stmt" : "result",
          (unsigned long long)run_id, target_name);
      if (prefix_bytes < 0 || (uint64_t)prefix_bytes >= sizeof(expected_prefix) ||
          strncmp(raw_payload, expected_prefix, (size_t)prefix_bytes) != 0 ||
          qstResultStorePendingTaskTableWorkerCount(
              raw_payload + prefix_bytes, &worker_count) != QST_OK ||
          qst_result_store_prepare_run(
              store, UINT32_C(1) << ((uint32_t)target->valueint - 1U),
              parsed_fingerprint, worker_count) != QST_OK) {
        code = QST_E_RESULT_STORE;
        goto _return;
      }
      if (persist_kind == QST_PERSIST_MANUAL_AI_RESULT) {
        char expected_key_prefix[128] = {0};
        const int32_t key_prefix_bytes = snprintf(
            expected_key_prefix, sizeof(expected_key_prefix),
            "results|%llu|%u|%llu|",
            (unsigned long long)run_id, (unsigned)target->valueint,
            (unsigned long long)gid->valueint);
        if (key_prefix_bytes < 0 ||
            (uint64_t)key_prefix_bytes >= sizeof(expected_key_prefix) ||
            strncmp(stable_key->valuestring, expected_key_prefix,
                    (size_t)key_prefix_bytes) != 0 ||
            strstr(raw_payload, ",'ai',") == NULL) {
          code = QST_E_SHM_CORRUPT;
          goto _return;
        }
      }
      code = qstResultStoreExecuteWithRecovery(
          mapping, store, raw_payload, (uint64_t)record_seq->valueint);
    }
  } else if (persist_kind == QST_PERSIST_RUN_UPDATE ||
             persist_kind == QST_PERSIST_DATASET) {
    const char *expected_prefix = persist_kind == QST_PERSIST_RUN_UPDATE ?
                                      "INSERT INTO res_db.runs VALUES (" :
                                      "INSERT INTO res_db.datasets VALUES (";
    char expected_key[96] = {0};
    int32_t expected_key_bytes = 0;

    code = qstPendingDecodeHex(payload->valuestring, &raw_payload, &(uint64_t){0});
    if (code != QST_OK) {
      goto _return;
    }
    if (target->valueint != 0 || gid->valueint != 0 ||
        (persist_kind == QST_PERSIST_RUN_UPDATE &&
         ((ds_fingerprint->valuestring[0] != '\0' &&
           !qstPendingHexIsValid(ds_fingerprint->valuestring,
                                 QST_PENDING_DIGEST_HEX_BYTES / 2U)) ||
          (expected_key_bytes = snprintf(expected_key, sizeof(expected_key),
                                         "runs|%llu|%llu",
                                         (unsigned long long)run_id,
                                         (unsigned long long)record_seq->valueint)) < 0 ||
          (uint64_t)expected_key_bytes >= sizeof(expected_key))) ||
        (persist_kind == QST_PERSIST_DATASET &&
         (!qstPendingHexIsValid(ds_fingerprint->valuestring,
                                QST_PENDING_DIGEST_HEX_BYTES / 2U) ||
          (expected_key_bytes = snprintf(expected_key, sizeof(expected_key),
                                         "datasets|%s",
                                         ds_fingerprint->valuestring)) < 0 ||
          (uint64_t)expected_key_bytes >= sizeof(expected_key))) ||
        strcmp(stable_key->valuestring, expected_key) != 0 ||
        strncmp(raw_payload, expected_prefix, strlen(expected_prefix)) != 0) {
      code = QST_E_SHM_CORRUPT;
      goto _return;
    }
    code = qstResultStoreEnsureSchema((SQstNativeClient *)store->native_taos);
    if (code == QST_OK) {
      code = qstResultStoreExecuteWithRecovery(
          mapping, store, raw_payload, (uint64_t)record_seq->valueint);
    }
  }
  if (raw_payload == NULL) {
    stable_key_hex = (char *)calloc(strlen(stable_key->valuestring) * 2U + 1U, 1);
    if (NULL == stable_key_hex) {
      code = QST_E_RESOURCE;
      goto _return;
    }
    qstPendingHex((const uint8_t *)stable_key->valuestring,
                  strlen(stable_key->valuestring), stable_key_hex);
    code = qstResultStoreWriteHex(
        mapping, store, persist_kind, (uint32_t)target->valueint,
        (uint64_t)gid->valueint,
        (uint64_t)record_seq->valueint, stable_key_hex,
        payload_sha256->valuestring, payload->valuestring);
  }
  if (code == QST_OK && remove_on_success != 0 && unlink(path) != 0) {
    code = QST_E_PENDING_IO;
  }
_return:
  free(raw_payload);
  free(stable_key_hex);
  tools_cJSON_Delete(root);
  if (owns_text != 0) {
    free(text);
  }
  return code;
}

static EQstCode qstPendingReplayBatch(SQstShmMapping *mapping,
                                      SQstResultStore *store,
                                      uint64_t run_id, const char *name,
                                      uint64_t first_record_seq,
                                      uint64_t last_record_seq,
                                      uint64_t *out_replayed_count) {
  char path[QST_PENDING_PATH_BYTES] = {0};
  char *text = NULL;
  char *cursor = NULL;
  uint64_t line_count = 0;
  uint64_t last_line_record_seq = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == name ||
      NULL == out_replayed_count || first_record_seq == 0 ||
      last_record_seq < first_record_seq) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (snprintf(path, sizeof(path), "%s/%s", store->pending_dir, name) < 0 ||
      strlen(path) >= sizeof(path)) {
    return QST_E_PENDING_IO;
  }
  code = qstPendingReadFile(path, &text);
  if (code != QST_OK) {
    return code;
  }
  cursor = text;
  while (*cursor != '\0') {
    char *line_end = strchr(cursor, '\n');
    tools_cJSON *root = NULL;
    tools_cJSON *record_seq = NULL;
    uint64_t line_record_seq = 0;

    if (NULL == line_end || line_end == cursor) {
      code = QST_E_SHM_CORRUPT;
      break;
    }
    *line_end = '\0';
    root = tools_cJSON_ParseWithOpts(cursor, NULL, 1);
    record_seq = qstPendingRequiredItem(root, "record_seq", 0);
    if (NULL == record_seq || record_seq->valueint <= 0) {
      tools_cJSON_Delete(root);
      code = QST_E_SHM_CORRUPT;
      break;
    }
    line_record_seq = (uint64_t)record_seq->valueint;
    tools_cJSON_Delete(root);
    if ((line_count == 0 && line_record_seq != first_record_seq) ||
        (line_record_seq < first_record_seq || line_record_seq > last_record_seq)) {
      code = QST_E_SHM_CORRUPT;
      break;
    }
    code = qstPendingReplayOne(mapping, store, run_id, NULL, cursor,
                               line_record_seq, 0);
    if (code != QST_OK) {
      break;
    }
    last_line_record_seq = line_record_seq;
    ++line_count;
    cursor = line_end + 1;
  }
  if (code == QST_OK && (line_count == 0 ||
                         last_line_record_seq != last_record_seq)) {
    code = QST_E_SHM_CORRUPT;
  }
  if (code == QST_OK && unlink(path) != 0) {
    code = QST_E_PENDING_IO;
  }
  if (code == QST_OK) {
    *out_replayed_count += line_count;
  }
  free(text);
  return code;
}

/* Replays the single append-only stream while holding the same advisory lock
 * used by writers, so replay cannot interleave with a new binary record. */
static EQstCode qstPendingReplayStream(SQstShmMapping *mapping,
                                       SQstResultStore *store,
                                       uint64_t run_id,
                                       uint64_t *out_replayed_count) {
  char path[QST_PENDING_PATH_BYTES] = {0};
  uint64_t replayed_count = 0;
  int32_t fd = -1;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == out_replayed_count ||
      qstPendingStreamPath(path, sizeof(path), store->pending_dir) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  fd = open(path, O_RDWR | O_CLOEXEC);
  if (fd < 0 || flock(fd, LOCK_EX) != 0) {
    if (fd >= 0) {
      (void)close(fd);
    }
    return QST_E_PENDING_IO;
  }
  if (lseek(fd, 0, SEEK_SET) < 0) {
    code = QST_E_PENDING_IO;
  }
  while (code == QST_OK) {
    SQstPendingStreamRecordHeader header = {0};
    uint64_t header_bytes = 0;
    char *payload = NULL;

    while (header_bytes < sizeof(header)) {
      const ssize_t read_bytes = read(
          fd, (uint8_t *)&header + header_bytes,
          sizeof(header) - (size_t)header_bytes);

      if (read_bytes < 0 && errno == EINTR) {
        continue;
      }
      if (read_bytes < 0) {
        code = QST_E_PENDING_IO;
        break;
      }
      if (read_bytes == 0) {
        code = header_bytes == 0 ? QST_E_QUEUE_EMPTY : QST_E_SHM_CORRUPT;
        break;
      }
      header_bytes += (uint64_t)read_bytes;
    }
    if (code == QST_E_QUEUE_EMPTY) {
      code = QST_OK;
      break;
    }
    if (code != QST_OK) {
      break;
    }
    if (header.magic != QST_PENDING_STREAM_MAGIC ||
        header.version != QST_PENDING_STREAM_VERSION ||
        header.run_id != run_id || header.record_seq == 0 ||
        header.payload_bytes == 0 || header.payload_bytes >= SIZE_MAX ||
        qstPendingKindName(header.persist_kind) == NULL) {
      code = QST_E_SHM_CORRUPT;
      break;
    }
    payload = (char *)calloc((size_t)header.payload_bytes + 1U, 1);
    if (NULL == payload) {
      code = QST_E_RESOURCE;
      break;
    }
    for (uint64_t payload_bytes = 0; payload_bytes < header.payload_bytes;) {
      const ssize_t read_bytes = read(
          fd, payload + payload_bytes,
          (size_t)(header.payload_bytes - payload_bytes));

      if (read_bytes < 0 && errno == EINTR) {
        continue;
      }
      if (read_bytes <= 0) {
        code = read_bytes == 0 ? QST_E_SHM_CORRUPT : QST_E_PENDING_IO;
        break;
      }
      payload_bytes += (uint64_t)read_bytes;
    }
    if (code == QST_OK) {
      code = qstResultStoreExecuteWithRecovery(mapping, store, payload,
                                               header.record_seq);
    }
    free(payload);
    if (code != QST_OK) {
      break;
    }
    ++replayed_count;
  }
  if (code == QST_OK && unlink(path) != 0) {
    code = QST_E_PENDING_IO;
  }
  if (flock(fd, LOCK_UN) != 0 && code == QST_OK) {
    code = QST_E_PENDING_IO;
  }
  if (close(fd) != 0 && code == QST_OK) {
    code = QST_E_PENDING_IO;
  }
  if (code == QST_OK) {
    *out_replayed_count += replayed_count;
  }
  return code;
}

EQstCode qst_pending_replay(SQstShmMapping *mapping, SQstResultStore *store,
                            uint64_t run_id, uint64_t *out_replayed_count) {
  SQstPendingFile *files = NULL;
  DIR *directory = NULL;
  struct dirent *entry = NULL;
  uint32_t file_count = 0;
  uint32_t file_capacity = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == store || NULL == out_replayed_count ||
      run_id == 0 || run_id != mapping->header->run_id ||
      run_id != store->run_id ||
      store->pending_dir[0] == '\0') {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_replayed_count = 0;
  code = qst_result_store_recover(mapping, store, run_id, 1);
  if (code != QST_OK) {
    return code;
  }
  directory = opendir(store->pending_dir);
  if (NULL == directory) {
    return QST_E_PENDING_IO;
  }
  while ((entry = readdir(directory)) != NULL) {
    uint64_t record_seq = 0;
    uint64_t last_record_seq = 0;
    int32_t temporary = 0;
    int32_t is_batch = 0;
    char path[QST_PENDING_PATH_BYTES] = {0};
    struct stat status = {0};

    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
        strcmp(entry->d_name, "quarantine") == 0) {
      continue;
    }
    if (strcmp(entry->d_name, "pending.stream") == 0) {
      is_batch = 2;
    } else if (qstPendingParseFileName(entry->d_name, &record_seq, &temporary) == 0) {
      if (qstPendingParseBatchFileName(entry->d_name, &record_seq,
                                       &last_record_seq, &temporary) != 0) {
        is_batch = 1;
      } else {
        code = qstPendingDiscard(store, entry->d_name);
        if (code != QST_OK) {
          break;
        }
        continue;
      }
    }
    if (temporary != 0 && is_batch == 1) {
      if (snprintf(path, sizeof(path), "%s/%s", store->pending_dir,
                   entry->d_name) < 0 || unlink(path) != 0) {
        code = QST_E_PENDING_IO;
        break;
      }
      continue;
    }
    if (temporary != 0) {
      if (snprintf(path, sizeof(path), "%s/%s", store->pending_dir,
                   entry->d_name) < 0 || stat(path, &status) != 0) {
        code = QST_E_PENDING_IO;
        break;
      }
      if (time(NULL) - status.st_mtime > 1800) {
        code = qstPendingDiscard(store, entry->d_name);
        if (code != QST_OK) {
          break;
        }
      }
      continue;
    }
    if (file_count == file_capacity) {
      const uint32_t next_capacity = file_capacity == 0 ? 16U : file_capacity * 2U;
      SQstPendingFile *next_files = NULL;

      if (next_capacity < file_capacity ||
          next_capacity > SIZE_MAX / sizeof(*files)) {
        code = QST_E_RESOURCE;
        break;
      }
      next_files = (SQstPendingFile *)realloc(files,
                                               next_capacity * sizeof(*files));
      if (NULL == next_files) {
        code = QST_E_RESOURCE;
        break;
      }
      files = next_files;
      file_capacity = next_capacity;
    }
    files[file_count].record_seq = record_seq;
    files[file_count].is_batch = is_batch;
    if (snprintf(files[file_count].name, sizeof(files[file_count].name), "%s",
                 entry->d_name) < 0) {
      code = QST_E_RESOURCE;
      break;
    }
    ++file_count;
  }
  if (closedir(directory) != 0 && code == QST_OK) {
    code = QST_E_PENDING_IO;
  }
  if (code == QST_OK) {
    qsort(files, file_count, sizeof(*files), qstPendingFileCompare);
    for (uint32_t index = 0; index < file_count; ++index) {
      uint64_t first_record_seq = 0;
      uint64_t last_record_seq = 0;
      int32_t temporary = 0;

      if (files[index].is_batch == 2) {
        code = qstPendingReplayStream(mapping, store, run_id,
                                      out_replayed_count);
      } else if (files[index].is_batch != 0) {
        if (qstPendingParseBatchFileName(files[index].name, &first_record_seq,
                                         &last_record_seq, &temporary) == 0) {
          code = QST_E_SHM_CORRUPT;
        } else {
          code = qstPendingReplayBatch(mapping, store, run_id, files[index].name,
                                       first_record_seq, last_record_seq,
                                       out_replayed_count);
        }
      } else {
        code = qstPendingReplayOne(mapping, store, run_id, files[index].name,
                                   NULL, 0, 1);
      }
      if (code == QST_E_SHM_CORRUPT) {
        code = qstPendingDiscard(store, files[index].name);
      } else if (code == QST_OK) {
        if (files[index].is_batch == 0) {
          *out_replayed_count += 1U;
        }
      }
      if (code != QST_OK) {
        break;
      }
    }
  }
  free(files);
  return code;
}
