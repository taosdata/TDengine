/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

/* query-result-v1 serialization and comparison for native query results. */

#define _POSIX_C_SOURCE 200809L

#include "qst_result.h"

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "taos.h"
#include "toolscJson.h"

#include "qst_native.h"

enum {
  QST_RESULT_TIMESTAMP_MILLISECOND = 0,
  QST_RESULT_TIMESTAMP_MICROSECOND = 1,
  QST_RESULT_TIMESTAMP_NANOSECOND = 2,
};

enum { QST_RESULT_ERROR_MESSAGE_BYTES = 1024 };
enum { QST_RESULT_TIMESTAMP_CACHE_ENTRIES = 32 };
enum { QST_RESULT_CAPTURE_CACHE_ENTRIES = 16 };
/* Typical correctness documents are about 2 KiB. Starting at 4 KiB avoids
 * repeated realloc/copy cycles on the pair hot path while bounding each
 * in-flight capture to a small fixed initial allocation. */
enum { QST_RESULT_JSON_INITIAL_CAPACITY = 4096 };

typedef struct {
  int64_t raw_value;
  int32_t precision;
  char text[40];
  uint8_t valid;
} SQstResultTimestampCacheEntry;

static _Thread_local SQstResultTimestampCacheEntry
    gQstResultTimestampCache[QST_RESULT_TIMESTAMP_CACHE_ENTRIES];

static const char *qstResultTypeName(int8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return "bool";
    case TSDB_DATA_TYPE_TINYINT:
      return "int8";
    case TSDB_DATA_TYPE_SMALLINT:
      return "int16";
    case TSDB_DATA_TYPE_INT:
      return "int32";
    case TSDB_DATA_TYPE_BIGINT:
      return "int64";
    case TSDB_DATA_TYPE_FLOAT:
      return "float32";
    case TSDB_DATA_TYPE_DOUBLE:
      return "float64";
    case TSDB_DATA_TYPE_TIMESTAMP:
      return "timestamp";
    case TSDB_DATA_TYPE_NCHAR:
      return "nchar";
    case TSDB_DATA_TYPE_VARCHAR:
      return "varchar";
    case TSDB_DATA_TYPE_UTINYINT:
      return "uint8";
    case TSDB_DATA_TYPE_USMALLINT:
      return "uint16";
    case TSDB_DATA_TYPE_UINT:
      return "uint32";
    case TSDB_DATA_TYPE_UBIGINT:
      return "uint64";
    case TSDB_DATA_TYPE_JSON:
      return "json";
    case TSDB_DATA_TYPE_VARBINARY:
      return "varbinary";
    case TSDB_DATA_TYPE_DECIMAL:
      return "decimal";
    case TSDB_DATA_TYPE_BLOB:
      return "blob";
    case TSDB_DATA_TYPE_MEDIUMBLOB:
      return "mediumblob";
    case TSDB_DATA_TYPE_GEOMETRY:
      return "geometry";
    case TSDB_DATA_TYPE_DECIMAL64:
      return "decimal64";
    default:
      return NULL;
  }
}

static int32_t qstResultFixedWidth(int8_t type) {
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
    case TSDB_DATA_TYPE_TINYINT:
    case TSDB_DATA_TYPE_UTINYINT:
      return 1;
    case TSDB_DATA_TYPE_SMALLINT:
    case TSDB_DATA_TYPE_USMALLINT:
      return 2;
    case TSDB_DATA_TYPE_INT:
    case TSDB_DATA_TYPE_UINT:
    case TSDB_DATA_TYPE_FLOAT:
      return 4;
    case TSDB_DATA_TYPE_BIGINT:
    case TSDB_DATA_TYPE_UBIGINT:
    case TSDB_DATA_TYPE_DOUBLE:
    case TSDB_DATA_TYPE_TIMESTAMP:
      return 8;
    default:
      return 0;
  }
}

static void qstResultSetDiff(SQstResultDiff *diff, EQstResultDiffKind kind,
                             uint32_t column_index, uint64_t row_index,
                             const char *summary) {
  if (NULL == diff) {
    return;
  }
  memset(diff, 0, sizeof(*diff));
  diff->kind = (uint32_t)kind;
  diff->column_index = column_index;
  diff->row_index = row_index;
  if (NULL != summary) {
    (void)snprintf(diff->summary, sizeof(diff->summary), "%s", summary);
  }
}

static tools_cJSON *qstResultTypedString(const char *kind, const char *value) {
  tools_cJSON *object = NULL;

  if (NULL == kind || NULL == value) {
    return NULL;
  }
  object = tools_cJSON_CreateObject();
  if (NULL == object) {
    return NULL;
  }
  tools_cJSON_AddStringToObject(object, "kind", kind);
  tools_cJSON_AddStringToObject(object, "value", value);
  return object;
}

static char *qstResultHex(const uint8_t *bytes, uint64_t byte_count) {
  static const char kHex[] = "0123456789abcdef";
  char *text = NULL;

  if ((byte_count != 0 && NULL == bytes) || byte_count > SIZE_MAX / 2U) {
    return NULL;
  }
  text = (char *)calloc((size_t)(byte_count * 2U) + 1U, 1);
  if (NULL == text) {
    return NULL;
  }
  for (uint64_t index = 0; index < byte_count; ++index) {
    text[index * 2U] = kHex[bytes[index] >> 4U];
    text[index * 2U + 1U] = kHex[bytes[index] & 0x0fU];
  }
  return text;
}

static int32_t qstResultHasNul(const uint8_t *bytes, uint64_t byte_count) {
  return byte_count != 0 && NULL != memchr(bytes, '\0', (size_t)byte_count);
}

static tools_cJSON *qstResultTextValue(const uint8_t *bytes,
                                       uint64_t byte_count, const char *kind) {
  char *copy = NULL;
  char *hex = NULL;
  tools_cJSON *value = NULL;

  if (NULL == bytes && byte_count != 0) {
    return NULL;
  }
  if (qstResultHasNul(bytes, byte_count) != 0) {
    hex = qstResultHex(bytes, byte_count);
    if (NULL == hex) {
      return NULL;
    }
    value = qstResultTypedString("hex", hex);
    free(hex);
    return value;
  }
  if (byte_count >= SIZE_MAX) {
    return NULL;
  }
  copy = (char *)calloc((size_t)byte_count + 1U, 1);
  if (NULL == copy) {
    return NULL;
  }
  if (byte_count != 0) {
    memcpy(copy, bytes, (size_t)byte_count);
  }
  value = qstResultTypedString(kind, copy);
  free(copy);
  return value;
}

static EQstCode qstResultTimestampText(int64_t raw_value, int32_t precision,
                                       char out_text[40]) {
  int64_t nanoseconds = 0;
  int64_t seconds = 0;
  int64_t remainder = 0;
  time_t epoch_seconds = 0;
  struct tm utc = {0};
  int32_t written = 0;

  if (NULL == out_text) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (precision == QST_RESULT_TIMESTAMP_MILLISECOND) {
    if (raw_value > INT64_MAX / 1000000LL || raw_value < INT64_MIN / 1000000LL) {
      return QST_E_RESOURCE;
    }
    nanoseconds = raw_value * 1000000LL;
  } else if (precision == QST_RESULT_TIMESTAMP_MICROSECOND) {
    if (raw_value > INT64_MAX / 1000LL || raw_value < INT64_MIN / 1000LL) {
      return QST_E_RESOURCE;
    }
    nanoseconds = raw_value * 1000LL;
  } else if (precision == QST_RESULT_TIMESTAMP_NANOSECOND) {
    nanoseconds = raw_value;
  } else {
    return QST_E_VERSION_CAPABILITY;
  }
  seconds = nanoseconds / 1000000000LL;
  remainder = nanoseconds % 1000000000LL;
  if (remainder < 0) {
    remainder += 1000000000LL;
    --seconds;
  }
  epoch_seconds = (time_t)seconds;
  if ((int64_t)epoch_seconds != seconds || NULL == gmtime_r(&epoch_seconds, &utc)) {
    return QST_E_RESOURCE;
  }
  written = snprintf(out_text, 40, "%04d-%02d-%02dT%02d:%02d:%02d.%09lldZ",
                     utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday,
                     utc.tm_hour, utc.tm_min, utc.tm_sec,
                     (long long)remainder);
  return written > 0 && written < 40 ? QST_OK : QST_E_RESOURCE;
}

static EQstCode qstResultTimestampTextCached(int64_t raw_value,
                                             int32_t precision,
                                             char out_text[40]) {
  const uint64_t mixed = (uint64_t)raw_value ^ ((uint64_t)precision << 59U);
  SQstResultTimestampCacheEntry *entry =
      &gQstResultTimestampCache[mixed % QST_RESULT_TIMESTAMP_CACHE_ENTRIES];
  EQstCode code = QST_OK;

  if (out_text == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (entry->valid != 0 && entry->raw_value == raw_value &&
      entry->precision == precision) {
    memcpy(out_text, entry->text, sizeof(entry->text));
    return QST_OK;
  }
  code = qstResultTimestampText(raw_value, precision, out_text);
  if (code == QST_OK) {
    entry->raw_value = raw_value;
    entry->precision = precision;
    memcpy(entry->text, out_text, sizeof(entry->text));
    entry->valid = 1;
  }
  return code;
}

static tools_cJSON *qstResultAddValue(const void *data, int32_t length,
                                      int8_t type, int32_t precision) {
  char text[80] = {0};
  char timestamp[40] = {0};
  char *hex = NULL;
  int32_t written = 0;

  if (NULL == data) {
    return tools_cJSON_CreateNull();
  }
  if (length < 0) {
    return NULL;
  }
  if (qstResultFixedWidth(type) > length) {
    return NULL;
  }
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return tools_cJSON_CreateBool(*(const int8_t *)data != 0);
    case TSDB_DATA_TYPE_TINYINT:
      written = snprintf(text, sizeof(text), "%d", *(const int8_t *)data);
      return written > 0 ? qstResultTypedString("int8", text) : NULL;
    case TSDB_DATA_TYPE_SMALLINT:
      written = snprintf(text, sizeof(text), "%d", *(const int16_t *)data);
      return written > 0 ? qstResultTypedString("int16", text) : NULL;
    case TSDB_DATA_TYPE_INT:
      written = snprintf(text, sizeof(text), "%d", *(const int32_t *)data);
      return written > 0 ? qstResultTypedString("int32", text) : NULL;
    case TSDB_DATA_TYPE_BIGINT:
      written = snprintf(text, sizeof(text), "%lld", (long long)*(const int64_t *)data);
      return written > 0 ? qstResultTypedString("int64", text) : NULL;
    case TSDB_DATA_TYPE_UTINYINT:
      written = snprintf(text, sizeof(text), "%u", *(const uint8_t *)data);
      return written > 0 ? qstResultTypedString("uint8", text) : NULL;
    case TSDB_DATA_TYPE_USMALLINT:
      written = snprintf(text, sizeof(text), "%u", *(const uint16_t *)data);
      return written > 0 ? qstResultTypedString("uint16", text) : NULL;
    case TSDB_DATA_TYPE_UINT:
      written = snprintf(text, sizeof(text), "%u", *(const uint32_t *)data);
      return written > 0 ? qstResultTypedString("uint32", text) : NULL;
    case TSDB_DATA_TYPE_UBIGINT:
      written = snprintf(text, sizeof(text), "%llu", (unsigned long long)*(const uint64_t *)data);
      return written > 0 ? qstResultTypedString("uint64", text) : NULL;
    case TSDB_DATA_TYPE_FLOAT:
      written = snprintf(text, sizeof(text), "%a", (double)*(const float *)data);
      return written > 0 ? qstResultTypedString("float32", text) : NULL;
    case TSDB_DATA_TYPE_DOUBLE:
      written = snprintf(text, sizeof(text), "%a", *(const double *)data);
      return written > 0 ? qstResultTypedString("float64", text) : NULL;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (qstResultTimestampTextCached(*(const int64_t *)data, precision,
                                       timestamp) != QST_OK) {
        return NULL;
      }
      return qstResultTypedString("timestamp", timestamp);
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_DECIMAL64:
      return qstResultTextValue((const uint8_t *)data, (uint64_t)length,
                                qstResultTypeName(type));
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
      hex = qstResultHex((const uint8_t *)data, (uint64_t)length);
      if (NULL == hex) {
        return NULL;
      }
      {
        tools_cJSON *value = qstResultTypedString("hex", hex);
        free(hex);
        return value;
      }
    default:
      return NULL;
  }
}

/* Result capture is on the hot pair path. Building a cJSON tree for every
 * small query result dominates execution time, so emit the unchanged v1 JSON
 * representation directly. The comparison side still parses this format. */
typedef struct {
  char *data;
  uint64_t bytes;
  uint64_t capacity;
} SQstResultJsonBuffer;

typedef struct {
  uint8_t *data;
  uint64_t bytes;
  uint64_t capacity;
  uint64_t rows_offset;
} SQstResultRawBuffer;

typedef struct {
  SQstDigest256 key;
  uint8_t       valid;
  uint8_t       reserved[7];
  SQstResultRawBuffer raw;
  SQstQueryResult     normalized;
} SQstResultCaptureCacheEntry;

typedef struct {
  SQstResultCaptureCacheEntry entries[QST_RESULT_CAPTURE_CACHE_ENTRIES];
  uint64_t                    hit_count;
  uint64_t                    miss_count;
} SQstResultCaptureCacheState;

static EQstCode qstResultJsonAppend(SQstResultJsonBuffer *buffer,
                                    const char *text, uint64_t bytes);

static EQstCode qstResultJsonReserve(SQstResultJsonBuffer *buffer,
                                     uint64_t additional_bytes) {
  uint64_t required = 0;
  uint64_t capacity = 0;
  char *grown = NULL;

  if (buffer == NULL || additional_bytes > UINT64_MAX - buffer->bytes - 1U) {
    return QST_E_INVALID_ARGUMENT;
  }
  required = buffer->bytes + additional_bytes + 1U;
  if (required <= buffer->capacity) {
    return QST_OK;
  }
  capacity = buffer->capacity == 0 ? QST_RESULT_JSON_INITIAL_CAPACITY :
                                     buffer->capacity;
  while (capacity < required) {
    if (capacity > SIZE_MAX / 2U) {
      capacity = required;
      break;
    }
    capacity *= 2U;
  }
  if (capacity > SIZE_MAX) {
    return QST_E_RESOURCE;
  }
  grown = (char *)realloc(buffer->data, (size_t)capacity);
  if (grown == NULL) {
    return QST_E_RESOURCE;
  }
  buffer->data = grown;
  buffer->capacity = capacity;
  return QST_OK;
}

static void qstResultJsonBufferReset(SQstResultJsonBuffer *buffer) {
  if (buffer != NULL) {
    free(buffer->data);
    *buffer = (SQstResultJsonBuffer){0};
  }
}

static EQstCode qstResultJsonAppend(SQstResultJsonBuffer *buffer,
                                    const char *text, uint64_t bytes) {
  if (buffer == NULL || (text == NULL && bytes != 0) ||
      bytes > UINT64_MAX - buffer->bytes - 1U) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstResultJsonReserve(buffer, bytes) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (bytes != 0) {
    memcpy(buffer->data + buffer->bytes, text, (size_t)bytes);
  }
  buffer->bytes += bytes;
  buffer->data[buffer->bytes] = '\0';
  return QST_OK;
}

static EQstCode qstResultJsonAppendText(SQstResultJsonBuffer *buffer,
                                        const char *text) {
  return text == NULL ? QST_E_INVALID_ARGUMENT :
                        qstResultJsonAppend(buffer, text, strlen(text));
}

static void qstResultRawBufferReset(SQstResultRawBuffer *buffer) {
  if (buffer != NULL) {
    free(buffer->data);
    *buffer = (SQstResultRawBuffer){0};
  }
}

static EQstCode qstResultRawAppend(SQstResultRawBuffer *buffer,
                                   const void *data, uint64_t bytes) {
  uint64_t required = 0;
  uint64_t capacity = 0;
  uint8_t *grown = NULL;

  if (buffer == NULL || (data == NULL && bytes != 0) ||
      bytes > UINT64_MAX - buffer->bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  required = buffer->bytes + bytes;
  if (required > buffer->capacity) {
    capacity = buffer->capacity == 0 ? 256U : buffer->capacity;
    while (capacity < required) {
      if (capacity > SIZE_MAX / 2U) {
        capacity = required;
        break;
      }
      capacity *= 2U;
    }
    if (capacity > SIZE_MAX) {
      return QST_E_RESOURCE;
    }
    grown = (uint8_t *)realloc(buffer->data, (size_t)capacity);
    if (grown == NULL) {
      return QST_E_RESOURCE;
    }
    buffer->data = grown;
    buffer->capacity = capacity;
  }
  if (bytes != 0) {
    memcpy(buffer->data + buffer->bytes, data, (size_t)bytes);
  }
  buffer->bytes = required;
  return QST_OK;
}

static EQstCode qstResultRawAppendU32(SQstResultRawBuffer *buffer,
                                      uint32_t value) {
  return qstResultRawAppend(buffer, &value, sizeof(value));
}

static EQstCode qstResultJsonAppendEscaped(SQstResultJsonBuffer *buffer,
                                           const uint8_t *text,
                                           uint64_t bytes) {
  static const char kHex[] = "0123456789abcdef";
  uint64_t output_bytes = 0;
  uint64_t write_offset = 0;

  if (buffer == NULL || (text == NULL && bytes != 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint64_t index = 0; index < bytes; ++index) {
    const uint8_t value = text[index];
    const uint64_t increment =
        value == '"' || value == '\\' || value == '\b' || value == '\f' ||
                value == '\n' || value == '\r' || value == '\t'
            ? 2U
            : value < 0x20U ? 6U : 1U;

    if (output_bytes > UINT64_MAX - increment) {
      return QST_E_RESOURCE;
    }
    output_bytes += increment;
  }
  if (qstResultJsonReserve(buffer, output_bytes) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (output_bytes == bytes) {
    memcpy(buffer->data + buffer->bytes, text, (size_t)bytes);
    buffer->bytes += bytes;
    buffer->data[buffer->bytes] = '\0';
    return QST_OK;
  }
  for (uint64_t index = 0; index < bytes; ++index) {
    const uint8_t value = text[index];

    if (value == '"' || value == '\\') {
      buffer->data[buffer->bytes + write_offset++] = '\\';
      buffer->data[buffer->bytes + write_offset++] = (char)value;
    } else if (value == '\b' || value == '\f' || value == '\n' ||
               value == '\r' || value == '\t') {
      static const char kShortEscape[] = "bfnrt";
      const uint32_t short_index = value == '\b' ? 0U : value == '\f' ? 1U :
                                    value == '\n' ? 2U : value == '\r' ? 3U : 4U;
      buffer->data[buffer->bytes + write_offset++] = '\\';
      buffer->data[buffer->bytes + write_offset++] = kShortEscape[short_index];
    } else if (value < 0x20U) {
      buffer->data[buffer->bytes + write_offset++] = '\\';
      buffer->data[buffer->bytes + write_offset++] = 'u';
      buffer->data[buffer->bytes + write_offset++] = '0';
      buffer->data[buffer->bytes + write_offset++] = '0';
      buffer->data[buffer->bytes + write_offset++] = kHex[value >> 4U];
      buffer->data[buffer->bytes + write_offset++] = kHex[value & 0x0fU];
    } else {
      buffer->data[buffer->bytes + write_offset++] = (char)value;
    }
  }
  buffer->bytes += write_offset;
  buffer->data[buffer->bytes] = '\0';
  return QST_OK;
}

static EQstCode qstResultJsonAppendTyped(SQstResultJsonBuffer *buffer,
                                         const char *kind,
                                         const uint8_t *value,
                                         uint64_t value_bytes) {
  EQstCode code = QST_OK;

  if (buffer == NULL || kind == NULL || (value == NULL && value_bytes != 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstResultJsonAppendText(buffer, "{\"kind\":\"");
  if (code == QST_OK) {
    code = qstResultJsonAppendText(buffer, kind);
  }
  if (code == QST_OK) {
    code = qstResultJsonAppendText(buffer, "\",\"value\":\"");
  }
  if (code == QST_OK) {
    code = qstResultJsonAppendEscaped(buffer, value, value_bytes);
  }
  return code == QST_OK ? qstResultJsonAppendText(buffer, "\"}") : code;
}

static EQstCode qstResultJsonAppendHex(SQstResultJsonBuffer *buffer,
                                       const uint8_t *value,
                                       uint64_t value_bytes) {
  static const char kHex[] = "0123456789abcdef";
  char hex[256] = {0};
  uint64_t index = 0;

  if (buffer == NULL || (value == NULL && value_bytes != 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstResultJsonAppendText(buffer, "{\"kind\":\"hex\",\"value\":\"") !=
      QST_OK) {
    return QST_E_RESOURCE;
  }
  while (index < value_bytes) {
    const uint64_t count = value_bytes - index > sizeof(hex) / 2U ?
                               sizeof(hex) / 2U : value_bytes - index;

    for (uint64_t offset = 0; offset < count; ++offset) {
      hex[offset * 2U] = kHex[value[index + offset] >> 4U];
      hex[offset * 2U + 1U] = kHex[value[index + offset] & 0x0fU];
    }
    if (qstResultJsonAppend(buffer, hex, count * 2U) != QST_OK) {
      return QST_E_RESOURCE;
    }
    index += count;
  }
  return qstResultJsonAppendText(buffer, "\"}");
}

static EQstCode qstResultJsonAppendValueUncached(
    SQstResultJsonBuffer *buffer, const void *data, int32_t length,
    int8_t type, int32_t precision) {
  char text[80] = {0};
  char timestamp[40] = {0};
  const char *kind = qstResultTypeName(type);
  int32_t written = 0;

  if (buffer == NULL || length < 0 || (data != NULL &&
      qstResultFixedWidth(type) > length)) {
    return QST_E_VERSION_CAPABILITY;
  }
  if (data == NULL) {
    return qstResultJsonAppendText(buffer, "null");
  }
  switch (type) {
    case TSDB_DATA_TYPE_BOOL:
      return qstResultJsonAppendText(buffer,
                                     *(const int8_t *)data != 0 ? "true" : "false");
    case TSDB_DATA_TYPE_TINYINT:
      written = snprintf(text, sizeof(text), "%d", *(const int8_t *)data);
      break;
    case TSDB_DATA_TYPE_SMALLINT:
      written = snprintf(text, sizeof(text), "%d", *(const int16_t *)data);
      break;
    case TSDB_DATA_TYPE_INT:
      written = snprintf(text, sizeof(text), "%d", *(const int32_t *)data);
      break;
    case TSDB_DATA_TYPE_BIGINT:
      written = snprintf(text, sizeof(text), "%lld", (long long)*(const int64_t *)data);
      break;
    case TSDB_DATA_TYPE_UTINYINT:
      written = snprintf(text, sizeof(text), "%u", *(const uint8_t *)data);
      break;
    case TSDB_DATA_TYPE_USMALLINT:
      written = snprintf(text, sizeof(text), "%u", *(const uint16_t *)data);
      break;
    case TSDB_DATA_TYPE_UINT:
      written = snprintf(text, sizeof(text), "%u", *(const uint32_t *)data);
      break;
    case TSDB_DATA_TYPE_UBIGINT:
      written = snprintf(text, sizeof(text), "%llu",
                         (unsigned long long)*(const uint64_t *)data);
      break;
    case TSDB_DATA_TYPE_FLOAT:
      written = snprintf(text, sizeof(text), "%a", (double)*(const float *)data);
      break;
    case TSDB_DATA_TYPE_DOUBLE:
      written = snprintf(text, sizeof(text), "%a", *(const double *)data);
      break;
    case TSDB_DATA_TYPE_TIMESTAMP:
      if (qstResultTimestampText(*(const int64_t *)data, precision, timestamp) !=
          QST_OK) {
        return QST_E_VERSION_CAPABILITY;
      }
      return qstResultJsonAppendTyped(buffer, "timestamp",
                                      (const uint8_t *)timestamp,
                                      strlen(timestamp));
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_NCHAR:
    case TSDB_DATA_TYPE_JSON:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_DECIMAL64:
      if (qstResultHasNul((const uint8_t *)data, (uint64_t)length) != 0) {
        return qstResultJsonAppendHex(buffer, (const uint8_t *)data,
                                      (uint64_t)length);
      }
      return qstResultJsonAppendTyped(buffer, kind, (const uint8_t *)data,
                                      (uint64_t)length);
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
      return qstResultJsonAppendHex(buffer, (const uint8_t *)data,
                                    (uint64_t)length);
    default:
      return QST_E_VERSION_CAPABILITY;
  }
  if (written <= 0 || (uint64_t)written >= sizeof(text) || kind == NULL) {
    return QST_E_VERSION_CAPABILITY;
  }
  return qstResultJsonAppendTyped(buffer, kind, (const uint8_t *)text,
                                  (uint64_t)written);
}

static EQstCode qstResultJsonAppendValue(SQstResultJsonBuffer *buffer,
                                         const void *data, int32_t length,
                                         int8_t type, int32_t precision) {
  return qstResultJsonAppendValueUncached(buffer, data, length, type,
                                          precision);
}

static EQstCode qstResultJsonAppendColumns(SQstResultJsonBuffer *buffer,
                                           const TAOS_FIELD *fields,
                                           int32_t column_count,
                                           int32_t precision) {
  for (int32_t index = 0; index < column_count; ++index) {
    const char *type_name = qstResultTypeName(fields[index].type);
    const char *unit = NULL;
    char bytes[32] = {0};
    int32_t written = 0;

    if (type_name == NULL || fields[index].bytes < 0 ||
        memchr(fields[index].name, '\0', sizeof(fields[index].name)) == NULL) {
      return QST_E_VERSION_CAPABILITY;
    }
    if (fields[index].type == TSDB_DATA_TYPE_TIMESTAMP) {
      unit = precision == QST_RESULT_TIMESTAMP_MILLISECOND ? "ms" :
             precision == QST_RESULT_TIMESTAMP_MICROSECOND ? "us" :
             precision == QST_RESULT_TIMESTAMP_NANOSECOND ? "ns" : NULL;
      if (unit == NULL) {
        return QST_E_VERSION_CAPABILITY;
      }
    }
    if (index != 0 && qstResultJsonAppendText(buffer, ",") != QST_OK) {
      return QST_E_RESOURCE;
    }
    written = snprintf(bytes, sizeof(bytes), "%d", fields[index].bytes);
    if (written <= 0 || (uint64_t)written >= sizeof(bytes) ||
        qstResultJsonAppendText(buffer, "{\"name\":\"") != QST_OK ||
        qstResultJsonAppendEscaped(buffer, (const uint8_t *)fields[index].name,
                                   strlen(fields[index].name)) != QST_OK ||
        qstResultJsonAppendText(buffer, "\",\"type\":\"") != QST_OK ||
        qstResultJsonAppendText(buffer, type_name) != QST_OK ||
        qstResultJsonAppendText(buffer, "\",\"bytes\":") != QST_OK ||
        qstResultJsonAppendText(buffer, bytes) != QST_OK) {
      return QST_E_RESOURCE;
    }
    if (unit != NULL && (qstResultJsonAppendText(buffer, ",\"unit\":\"") !=
                         QST_OK || qstResultJsonAppendText(buffer, unit) != QST_OK ||
                         qstResultJsonAppendText(buffer, "\"") != QST_OK)) {
      return QST_E_RESOURCE;
    }
    if (qstResultJsonAppendText(buffer, "}") != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  return QST_OK;
}

struct SQstResultAsyncCapture {
  SQstResultJsonBuffer output;
  TAOS_FIELD *fields;
  int32_t column_count;
  int32_t precision;
  int64_t row_count;
};

static EQstCode qstResultCaptureAppendValues(
    SQstResultAsyncCapture *capture, void **row, const int32_t *lengths);

static EQstCode qstResultRawAppendSchema(SQstResultRawBuffer *buffer,
                                         const SQstResultAsyncCapture *capture) {
  if (buffer == NULL || capture == NULL || capture->column_count < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstResultRawAppendU32(buffer, (uint32_t)capture->column_count) != QST_OK ||
      qstResultRawAppend(buffer, &capture->precision,
                         sizeof(capture->precision)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  for (int32_t column = 0; column < capture->column_count; ++column) {
    const TAOS_FIELD *field = &capture->fields[column];
    const uint32_t name_bytes = (uint32_t)strnlen(
        field->name, sizeof(field->name));

    if (name_bytes == sizeof(field->name) ||
        qstResultRawAppend(buffer, &field->type, sizeof(field->type)) != QST_OK ||
        qstResultRawAppend(buffer, &field->bytes, sizeof(field->bytes)) != QST_OK ||
        qstResultRawAppendU32(buffer, name_bytes) != QST_OK ||
        qstResultRawAppend(buffer, field->name, name_bytes) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  buffer->rows_offset = buffer->bytes;
  return QST_OK;
}

static EQstCode qstResultRawAppendRow(SQstResultRawBuffer *buffer,
                                      const SQstResultAsyncCapture *capture,
                                      void **row, const int32_t *lengths) {
  if (buffer == NULL || capture == NULL || row == NULL || lengths == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (int32_t column = 0; column < capture->column_count; ++column) {
    const uint32_t bytes = row[column] == NULL ? UINT32_MAX :
                           lengths[column] < 0 ? UINT32_MAX - 1U :
                           (uint32_t)lengths[column];

    if (bytes == UINT32_MAX - 1U ||
        qstResultRawAppendU32(buffer, bytes) != QST_OK ||
        (bytes != UINT32_MAX &&
         qstResultRawAppend(buffer, row[column], bytes) != QST_OK)) {
      return QST_E_RESULT_STORE;
    }
  }
  return QST_OK;
}

EQstCode qst_result_async_capture_begin(SQstNativeClient *client, void *result,
                                         SQstResultAsyncCapture **out_capture) {
  SQstResultAsyncCapture *capture = NULL;
  EQstCode code = QST_OK;

  if (client == NULL || result == NULL || out_capture == NULL ||
      client->errno_fn == NULL || client->num_fields == NULL ||
      client->fetch_fields == NULL || client->fetch_row == NULL ||
      client->fetch_lengths == NULL || client->result_precision == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_capture = NULL;
  if (client->errno_fn(result) != 0) {
    return QST_E_RESULT_STORE;
  }
  capture = (SQstResultAsyncCapture *)calloc(1, sizeof(*capture));
  if (capture == NULL) {
    return QST_E_RESOURCE;
  }
  capture->column_count = client->num_fields(result);
  capture->precision = client->result_precision(result);
  if (capture->column_count < 0 ||
      (capture->column_count != 0 &&
       (capture->fields = (TAOS_FIELD *)client->fetch_fields(result)) == NULL)) {
    free(capture);
    return QST_E_RESULT_STORE;
  }
  code = qstResultJsonAppendText(
      &capture->output, "{\"format\":\"query-result-v1\",\"columns\":[");
  if (code == QST_OK) {
    code = qstResultJsonAppendColumns(
        &capture->output, capture->fields, capture->column_count,
        capture->precision);
  }
  if (code == QST_OK) {
    code = qstResultJsonAppendText(&capture->output, "],\"rows\":[");
  }
  if (code != QST_OK) {
    qst_result_async_capture_abort(capture);
    return code;
  }
  *out_capture = capture;
  return QST_OK;
}

EQstCode qst_result_async_capture_row(
    SQstNativeClient *client, void *result, void **row,
    SQstResultAsyncCapture *capture) {
  int32_t *lengths = NULL;

  if (client == NULL || result == NULL || row == NULL || capture == NULL ||
      client->fetch_lengths == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (capture->row_count == INT64_MAX ||
      (lengths = client->fetch_lengths(result)) == NULL) {
    return QST_E_RESULT_STORE;
  }
  return qstResultCaptureAppendValues(capture, row, lengths);
}

static EQstCode qstResultCaptureAppendValues(
    SQstResultAsyncCapture *capture, void **row, const int32_t *lengths) {
  EQstCode code = QST_OK;

  if (capture == NULL || row == NULL || lengths == NULL ||
      capture->row_count == INT64_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (capture->row_count != 0) {
    code = qstResultJsonAppendText(&capture->output, ",");
  }
  if (code == QST_OK) {
    code = qstResultJsonAppendText(&capture->output, "[");
  }
  for (int32_t column = 0; column < capture->column_count; ++column) {
    if (lengths[column] < 0) {
      code = QST_E_RESULT_STORE;
      break;
    }
    if (column != 0 && code == QST_OK) {
      code = qstResultJsonAppendText(&capture->output, ",");
    }
    if (code == QST_OK) {
      code = qstResultJsonAppendValue(
          &capture->output, row[column],
          row[column] == NULL ? 0 : lengths[column],
          capture->fields[column].type, capture->precision);
    }
  }
  if (code != QST_OK) {
    return code;
  }
  code = qstResultJsonAppendText(&capture->output, "]");
  if (code == QST_OK) {
    ++capture->row_count;
  }
  return code;
}

EQstCode qst_result_async_capture_rows(SQstNativeClient *client, void *result,
                                        int32_t row_count,
                                        SQstResultAsyncCapture *capture) {
  if (client == NULL || result == NULL || capture == NULL || row_count < 0 ||
      client->fetch_row == NULL || client->fetch_lengths == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (int32_t row_index = 0; row_index < row_count; ++row_index) {
    void **row = client->fetch_row(result);

    if (row == NULL) {
      return QST_E_RESULT_STORE;
    }
    const EQstCode code =
        qst_result_async_capture_row(client, result, row, capture);
    if (code != QST_OK) {
      return code;
    }
  }
  return QST_OK;
}

EQstCode qst_result_async_capture_finish(SQstResultAsyncCapture *capture,
                                          SQstQueryResult *out_result) {
  EQstCode code = QST_OK;

  if (capture == NULL || out_result == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstResultJsonAppendText(&capture->output, "]}");
  if (code != QST_OK || capture->output.data == NULL) {
    qst_result_async_capture_abort(capture);
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  qst_result_reset(out_result);
  out_result->data = (uint8_t *)capture->output.data;
  out_result->bytes = capture->output.bytes;
  out_result->row_count = capture->row_count;
  capture->output.data = NULL;
  qst_result_async_capture_abort(capture);
  return QST_OK;
}

void qst_result_async_capture_abort(SQstResultAsyncCapture *capture) {
  if (capture == NULL) {
    return;
  }
  qstResultJsonBufferReset(&capture->output);
  free(capture);
}

enum {
  QST_RESULT_RAW_HEADER_BYTES = 28U,
  QST_RESULT_RAW_FIELD_BYTES = 5U,
  QST_RESULT_RAW_COLUMN_BYTES = 4U,
  QST_RESULT_RAW_MAX_COLUMNS = 128U,
  QST_RESULT_RAW_MAX_BLOCK_BYTES = 512U * 1024U * 1024U,
};

typedef struct {
  const uint8_t *head;
  const uint8_t *payload;
  uint32_t       payload_bytes;
  int32_t        fixed_width;
  int8_t         type;
} SQstRawResultColumn;

static int32_t qstResultRawTypeSupported(int8_t type) {
  if (qstResultFixedWidth(type) > 0) {
    return 1;
  }
  switch (type) {
    case TSDB_DATA_TYPE_VARCHAR:
    case TSDB_DATA_TYPE_VARBINARY:
    case TSDB_DATA_TYPE_BLOB:
    case TSDB_DATA_TYPE_MEDIUMBLOB:
    case TSDB_DATA_TYPE_GEOMETRY:
    case TSDB_DATA_TYPE_DECIMAL:
    case TSDB_DATA_TYPE_DECIMAL64:
      return 1;
    default:
      /* NCHAR and JSON are converted by the public row API. Fall back before
       * consuming a block so their normal representation remains unchanged. */
      return 0;
  }
}

static uint32_t qstResultRawVarHeaderBytes(int8_t type) {
  return type == TSDB_DATA_TYPE_BLOB || type == TSDB_DATA_TYPE_MEDIUMBLOB ?
             sizeof(uint32_t) :
             sizeof(uint16_t);
}

static int32_t qstResultRawReadU32(const uint8_t *data, uint32_t bytes,
                                   uint32_t offset, uint32_t *out_value) {
  if (data == NULL || out_value == NULL || offset > bytes ||
      bytes - offset < sizeof(*out_value)) {
    return 0;
  }
  memcpy(out_value, data + offset, sizeof(*out_value));
  return 1;
}

static int32_t qstResultRawReadI32(const uint8_t *data, uint32_t bytes,
                                   uint32_t offset, int32_t *out_value) {
  if (data == NULL || out_value == NULL || offset > bytes ||
      bytes - offset < sizeof(*out_value)) {
    return 0;
  }
  memcpy(out_value, data + offset, sizeof(*out_value));
  return 1;
}

static int32_t qstResultRawReadU16(const uint8_t *data, uint32_t bytes,
                                   uint32_t offset, uint16_t *out_value) {
  if (data == NULL || out_value == NULL || offset > bytes ||
      bytes - offset < sizeof(*out_value)) {
    return 0;
  }
  memcpy(out_value, data + offset, sizeof(*out_value));
  return 1;
}

static int32_t qstResultRawAddU32(uint32_t left, uint32_t right,
                                  uint32_t *out_sum) {
  if (out_sum == NULL || left > UINT32_MAX - right) {
    return 0;
  }
  *out_sum = left + right;
  return 1;
}

static EQstCode qstResultRawBlockPrepare(
    const SQstResultAsyncCapture *capture, const void *raw_data,
    int32_t fetched_rows, SQstRawResultColumn columns[QST_RESULT_RAW_MAX_COLUMNS]) {
  const uint8_t *data = (const uint8_t *)raw_data;
  int32_t version = 0;
  int32_t block_bytes_signed = 0;
  int32_t rows = 0;
  int32_t column_count = 0;
  uint32_t block_bytes = 0;
  uint32_t column_bytes_offset = 0;
  uint32_t payload_offset = 0;

  if (capture == NULL || raw_data == NULL || columns == NULL ||
      fetched_rows <= 0 || capture->column_count <= 0 ||
      capture->column_count > QST_RESULT_RAW_MAX_COLUMNS ||
      !qstResultRawReadI32(data, QST_RESULT_RAW_HEADER_BYTES, 0, &version) ||
      !qstResultRawReadI32(data, QST_RESULT_RAW_HEADER_BYTES, 4,
                           &block_bytes_signed) ||
      !qstResultRawReadI32(data, QST_RESULT_RAW_HEADER_BYTES, 8, &rows) ||
      !qstResultRawReadI32(data, QST_RESULT_RAW_HEADER_BYTES, 12,
                           &column_count) ||
      (version != 1 && version != 2) || block_bytes_signed < 0 ||
      rows != fetched_rows || column_count != capture->column_count) {
    return QST_E_RESULT_STORE;
  }
  block_bytes = (uint32_t)block_bytes_signed;
  if (block_bytes < QST_RESULT_RAW_HEADER_BYTES ||
      block_bytes > QST_RESULT_RAW_MAX_BLOCK_BYTES ||
      (uint32_t)column_count >
          (UINT32_MAX - QST_RESULT_RAW_HEADER_BYTES) /
              QST_RESULT_RAW_FIELD_BYTES ||
      !qstResultRawAddU32(
          QST_RESULT_RAW_HEADER_BYTES,
          (uint32_t)column_count * QST_RESULT_RAW_FIELD_BYTES,
          &column_bytes_offset) ||
      (uint32_t)column_count >
          (UINT32_MAX - column_bytes_offset) / QST_RESULT_RAW_COLUMN_BYTES ||
      !qstResultRawAddU32(
          column_bytes_offset,
          (uint32_t)column_count * QST_RESULT_RAW_COLUMN_BYTES,
          &payload_offset) ||
      payload_offset > block_bytes) {
    return QST_E_RESULT_STORE;
  }
  for (int32_t index = 0; index < column_count; ++index) {
    const uint32_t field_offset = QST_RESULT_RAW_HEADER_BYTES +
                                  (uint32_t)index * QST_RESULT_RAW_FIELD_BYTES;
    const uint32_t bytes_offset = column_bytes_offset +
                                  (uint32_t)index * QST_RESULT_RAW_COLUMN_BYTES;
    uint32_t payload_bytes = 0;
    uint32_t head_bytes = 0;
    uint32_t next_payload_offset = 0;
    int32_t fixed_width = 0;

    if (field_offset >= block_bytes ||
        capture->fields[index].type != (int8_t)data[field_offset] ||
        !qstResultRawReadU32(data, block_bytes, bytes_offset, &payload_bytes)) {
      return QST_E_RESULT_STORE;
    }
    fixed_width = qstResultFixedWidth((int8_t)data[field_offset]);
    if (fixed_width > 0) {
      head_bytes = ((uint32_t)rows + 7U) / 8U;
      if ((uint32_t)fixed_width > UINT32_MAX / (uint32_t)rows ||
          payload_bytes != (uint32_t)fixed_width * (uint32_t)rows) {
        return QST_E_RESULT_STORE;
      }
    } else {
      if ((uint32_t)rows > UINT32_MAX / sizeof(uint32_t)) {
        return QST_E_RESULT_STORE;
      }
      head_bytes = (uint32_t)rows * sizeof(uint32_t);
    }
    if (!qstResultRawAddU32(payload_offset, head_bytes, &next_payload_offset) ||
        !qstResultRawAddU32(next_payload_offset, payload_bytes,
                            &next_payload_offset) ||
        next_payload_offset > block_bytes) {
      return QST_E_RESULT_STORE;
    }
    columns[index] = (SQstRawResultColumn){
        .head = data + payload_offset,
        .payload = data + payload_offset + head_bytes,
        .payload_bytes = payload_bytes,
        .fixed_width = fixed_width,
        .type = (int8_t)data[field_offset],
    };
    payload_offset = next_payload_offset;
  }
  /* The native raw block keeps one trailing blankFill byte after its column
   * payloads. It is not part of any cell but is part of blockSize. */
  return payload_offset == block_bytes - sizeof(uint8_t) ? QST_OK :
                                                     QST_E_RESULT_STORE;
}

static EQstCode qstResultCaptureRawBlock(
    SQstResultAsyncCapture *capture, const void *raw_data, int32_t fetched_rows) {
  SQstRawResultColumn columns[QST_RESULT_RAW_MAX_COLUMNS] = {{0}};
  void *row[QST_RESULT_RAW_MAX_COLUMNS] = {0};
  int32_t lengths[QST_RESULT_RAW_MAX_COLUMNS] = {0};
  EQstCode code = qstResultRawBlockPrepare(capture, raw_data, fetched_rows,
                                           columns);

  if (code != QST_OK) {
    return code;
  }
  for (int32_t row_index = 0; row_index < fetched_rows; ++row_index) {
    for (int32_t column = 0; column < capture->column_count; ++column) {
      const SQstRawResultColumn *raw_column = &columns[column];

      row[column] = NULL;
      lengths[column] = 0;
      if (raw_column->fixed_width > 0) {
        const uint8_t bitmap = raw_column->head[(uint32_t)row_index / 8U];

        if ((bitmap & (UINT8_C(1) << (7U - ((uint32_t)row_index & 7U)))) == 0) {
          row[column] = (void *)(raw_column->payload +
                                  (uint64_t)row_index * raw_column->fixed_width);
          lengths[column] = raw_column->fixed_width;
        }
      } else {
        uint32_t value_offset = 0;
        uint32_t value_bytes = 0;
        const uint32_t value_header_bytes =
            qstResultRawVarHeaderBytes(raw_column->type);

        if (!qstResultRawReadU32(raw_column->head,
                                 (uint32_t)fetched_rows * sizeof(uint32_t),
                                 (uint32_t)row_index * sizeof(uint32_t),
                                 &value_offset)) {
          return QST_E_RESULT_STORE;
        }
        if (value_offset == UINT32_MAX) {
          continue;
        }
        if (value_offset > raw_column->payload_bytes ||
            raw_column->payload_bytes - value_offset < value_header_bytes) {
          return QST_E_RESULT_STORE;
        }
        if (value_header_bytes == sizeof(uint16_t)) {
          uint16_t short_value_bytes = 0;

          if (!qstResultRawReadU16(raw_column->payload,
                                   raw_column->payload_bytes, value_offset,
                                   &short_value_bytes)) {
            return QST_E_RESULT_STORE;
          }
          value_bytes = short_value_bytes;
        } else if (!qstResultRawReadU32(raw_column->payload,
                                        raw_column->payload_bytes,
                                        value_offset, &value_bytes)) {
          return QST_E_RESULT_STORE;
        }
        if (value_bytes > raw_column->payload_bytes - value_offset -
                              value_header_bytes ||
            value_bytes > INT32_MAX) {
          return QST_E_RESULT_STORE;
        }
        row[column] = (void *)(raw_column->payload + value_offset +
                               value_header_bytes);
        lengths[column] = (int32_t)value_bytes;
      }
    }
    code = qstResultCaptureAppendValues(capture, row, lengths);
    if (code != QST_OK) {
      return code;
    }
  }
  return QST_OK;
}

EQstCode qst_result_capture_raw_blocks(SQstNativeClient *client, void *result,
                                       SQstQueryResult *out_result) {
  SQstResultAsyncCapture *capture = NULL;
  TAOS_FIELD *fields = NULL;
  int32_t column_count = 0;
  EQstCode code = QST_OK;

  if (client == NULL || result == NULL || out_result == NULL ||
      client->fetch_raw_block == NULL || client->errno_fn == NULL ||
      client->num_fields == NULL || client->fetch_fields == NULL) {
    return QST_E_VERSION_CAPABILITY;
  }
  if (client->errno_fn(result) != 0) {
    return QST_E_RESULT_STORE;
  }
  column_count = client->num_fields(result);
  if (column_count < 0 || column_count > QST_RESULT_RAW_MAX_COLUMNS ||
      (column_count != 0 && (fields = client->fetch_fields(result)) == NULL)) {
    return QST_E_VERSION_CAPABILITY;
  }
  for (int32_t index = 0; index < column_count; ++index) {
    if (qstResultRawTypeSupported(fields[index].type) == 0) {
      return QST_E_VERSION_CAPABILITY;
    }
  }
  code = qst_result_async_capture_begin(client, result, &capture);
  if (code != QST_OK) {
    return code;
  }
  if (column_count != 0) {
    for (;;) {
      void *raw_data = NULL;
      int32_t fetched_rows = 0;
      const int32_t native_code =
          client->fetch_raw_block(result, &fetched_rows, &raw_data);

      if (native_code != 0 || fetched_rows < 0) {
        code = QST_E_RESULT_STORE;
        break;
      }
      if (fetched_rows == 0) {
        break;
      }
      if (raw_data == NULL) {
        code = QST_E_RESULT_STORE;
        break;
      }
      code = qstResultCaptureRawBlock(capture, raw_data, fetched_rows);
      if (code != QST_OK) {
        break;
      }
    }
  }
  if (code != QST_OK) {
    qst_result_async_capture_abort(capture);
    return code;
  }
  return qst_result_async_capture_finish(capture, out_result);
}

EQstCode qst_native_capture_raw_result(SQstNativeClient *client, void *result,
                                       SQstQueryResult *out_result) {
  return qst_result_capture_raw_blocks(client, result, out_result);
}

static EQstCode qstResultBuildColumns(const TAOS_FIELD *fields,
                                      int32_t column_count, int32_t precision,
                                      char **out_columns,
                                      uint64_t *out_bytes) {
  tools_cJSON *columns = NULL;
  char *printed = NULL;

  if ((NULL == fields && column_count != 0) || column_count < 0 ||
      NULL == out_columns || NULL == out_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  columns = tools_cJSON_CreateArray();
  if (NULL == columns) {
    return QST_E_RESOURCE;
  }
  for (int32_t index = 0; index < column_count; ++index) {
    const char *type_name = qstResultTypeName(fields[index].type);
    tools_cJSON *column = NULL;

    if (NULL == type_name || NULL == memchr(fields[index].name, '\0',
                                           sizeof(fields[index].name)) ||
        fields[index].bytes < 0) {
      tools_cJSON_Delete(columns);
      return QST_E_VERSION_CAPABILITY;
    }
    column = tools_cJSON_CreateObject();
    if (NULL == column) {
      tools_cJSON_Delete(columns);
      return QST_E_RESOURCE;
    }
    tools_cJSON_AddStringToObject(column, "name", fields[index].name);
    tools_cJSON_AddStringToObject(column, "type", type_name);
    tools_cJSON_AddNumberToObject(column, "bytes", fields[index].bytes);
    if (fields[index].type == TSDB_DATA_TYPE_TIMESTAMP) {
      const char *unit = precision == QST_RESULT_TIMESTAMP_MILLISECOND ? "ms" :
          (precision == QST_RESULT_TIMESTAMP_MICROSECOND ? "us" :
           (precision == QST_RESULT_TIMESTAMP_NANOSECOND ? "ns" : NULL));
      if (NULL == unit) {
        tools_cJSON_Delete(column);
        tools_cJSON_Delete(columns);
        return QST_E_VERSION_CAPABILITY;
      }
      tools_cJSON_AddStringToObject(column, "unit", unit);
    }
    tools_cJSON_AddItemToArray(columns, column);
  }
  printed = tools_cJSON_PrintUnformatted(columns);
  tools_cJSON_Delete(columns);
  if (NULL == printed) {
    return QST_E_RESOURCE;
  }
  *out_columns = printed;
  *out_bytes = strlen(printed);
  return QST_OK;
}

void qst_result_reset(SQstQueryResult *result) {
  if (NULL == result) {
    return;
  }
  free(result->data);
  memset(result, 0, sizeof(*result));
}

EQstCode qst_result_build_error(int32_t error_code, const char *message,
                                SQstQueryResult *out_result) {
  tools_cJSON *root = NULL;
  tools_cJSON *error = NULL;
  char *printed = NULL;
  char normalized[QST_RESULT_ERROR_MESSAGE_BYTES] = {0};
  size_t used = 0;
  int32_t in_space = 0;

  if (message == NULL || out_result == NULL) return QST_E_INVALID_ARGUMENT;
  for (const unsigned char *cursor = (const unsigned char *)message;
       *cursor != '\0' && used + 1U < sizeof(normalized); ++cursor) {
    if (*cursor == ' ' || *cursor == '\t' || *cursor == '\r' || *cursor == '\n') {
      in_space = 1;
      continue;
    }
    if (in_space != 0 && used != 0 && used + 1U < sizeof(normalized)) {
      normalized[used++] = ' ';
    }
    in_space = 0;
    normalized[used++] = (char)*cursor;
  }
  normalized[used] = '\0';
  root = tools_cJSON_CreateObject();
  error = tools_cJSON_CreateObject();
  if (root == NULL || error == NULL) {
    tools_cJSON_Delete(root);
    tools_cJSON_Delete(error);
    return QST_E_RESOURCE;
  }
  tools_cJSON_AddStringToObject(root, "format", "query-result-v1");
  tools_cJSON_AddNumberToObject(error, "code", error_code);
  tools_cJSON_AddStringToObject(error, "message", normalized);
  tools_cJSON_AddItemToObject(root, "error", error);
  tools_cJSON_AddItemToObject(root, "columns", tools_cJSON_CreateArray());
  tools_cJSON_AddItemToObject(root, "rows", tools_cJSON_CreateArray());
  printed = tools_cJSON_PrintUnformatted(root);
  tools_cJSON_Delete(root);
  if (printed == NULL) return QST_E_RESOURCE;
  qst_result_reset(out_result);
  out_result->data = (uint8_t *)printed;
  out_result->bytes = strlen(printed);
  /* Error documents contain an empty rows array. Keep the normalized result
   * storable by treating that array as a zero-row result; the error object,
   * rather than a negative row count, carries the execution failure. */
  out_result->row_count = 0;
  return QST_OK;
}

EQstCode qst_native_capture_result(SQstNativeClient *client, void *result,
                                   SQstQueryResult *out_result) {
  SQstResultAsyncCapture *capture = NULL;
  EQstCode code = QST_OK;

  if (client == NULL || result == NULL || out_result == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_result_async_capture_begin(client, result, &capture);
  if (code != QST_OK) {
    return code;
  }
  for (;;) {
    void **row = client->fetch_row == NULL ? NULL : client->fetch_row(result);

    if (NULL == row) {
      break;
    }
    code = qst_result_async_capture_row(client, result, row, capture);
    if (code != QST_OK) {
      break;
    }
  }
  if (code != QST_OK) {
    qst_result_async_capture_abort(capture);
    return code;
  }
  return qst_result_async_capture_finish(capture, out_result);
}

static EQstCode qstResultCloneQueryResult(const SQstQueryResult *source,
                                          SQstQueryResult *out_result) {
  uint8_t *copy = NULL;

  if (source == NULL || out_result == NULL || source->data == NULL ||
      source->bytes == 0 || source->bytes > SIZE_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  copy = (uint8_t *)malloc((size_t)source->bytes);
  if (copy == NULL) {
    return QST_E_RESOURCE;
  }
  memcpy(copy, source->data, (size_t)source->bytes);
  qst_result_reset(out_result);
  out_result->data = copy;
  out_result->bytes = source->bytes;
  out_result->row_count = source->row_count;
  return QST_OK;
}

static SQstResultCaptureCacheEntry *qstResultCaptureCacheFind(
    SQstResultCaptureCacheState *state, const SQstDigest256 *key) {
  if (state == NULL || key == NULL) {
    return NULL;
  }
  for (uint32_t index = 0; index < QST_RESULT_CAPTURE_CACHE_ENTRIES; ++index) {
    SQstResultCaptureCacheEntry *entry = &state->entries[index];

    if (entry->valid != 0 &&
        memcmp(entry->key.bytes, key->bytes, sizeof(key->bytes)) == 0) {
      return entry;
    }
  }
  return NULL;
}

static SQstResultCaptureCacheEntry *qstResultCaptureCacheSelect(
    SQstResultCaptureCacheState *state, const SQstDigest256 *key) {
  uint32_t hash = 0;

  if (state == NULL || key == NULL) {
    return NULL;
  }
  memcpy(&hash, key->bytes, sizeof(hash));
  return &state->entries[hash % QST_RESULT_CAPTURE_CACHE_ENTRIES];
}

static EQstCode qstResultCaptureRawRows(SQstResultAsyncCapture *capture,
                                        const SQstResultRawBuffer *raw) {
  void **row = NULL;
  int32_t *lengths = NULL;
  uint64_t offset = 0;
  EQstCode code = QST_OK;

  if (capture == NULL || raw == NULL || raw->data == NULL ||
      raw->rows_offset > raw->bytes || capture->column_count < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (capture->column_count != 0) {
    row = (void **)calloc((size_t)capture->column_count, sizeof(*row));
    lengths = (int32_t *)calloc((size_t)capture->column_count,
                                sizeof(*lengths));
    if (row == NULL || lengths == NULL) {
      free(lengths);
      free(row);
      return QST_E_RESOURCE;
    }
  }
  offset = raw->rows_offset;
  while (offset < raw->bytes) {
    for (int32_t column = 0; column < capture->column_count; ++column) {
      uint32_t bytes = 0;

      if (raw->bytes - offset < sizeof(bytes)) {
        code = QST_E_RESULT_STORE;
        goto _return;
      }
      memcpy(&bytes, raw->data + offset, sizeof(bytes));
      offset += sizeof(bytes);
      if (bytes == UINT32_MAX) {
        row[column] = NULL;
        lengths[column] = 0;
        continue;
      }
      if (bytes > INT32_MAX || bytes > raw->bytes - offset) {
        code = QST_E_RESULT_STORE;
        goto _return;
      }
      row[column] = raw->data + offset;
      lengths[column] = (int32_t)bytes;
      offset += bytes;
    }
    code = qstResultCaptureAppendValues(capture, row, lengths);
    if (code != QST_OK) {
      goto _return;
    }
  }

_return:
  free(lengths);
  free(row);
  return code;
}

static EQstCode qstResultCaptureCacheReplace(
    SQstResultCaptureCacheEntry *entry, const SQstDigest256 *key,
    const SQstResultRawBuffer *raw, const SQstQueryResult *normalized) {
  SQstResultRawBuffer raw_copy = {0};
  SQstQueryResult normalized_copy = {0};
  EQstCode code = QST_OK;

  if (entry == NULL || key == NULL || raw == NULL || normalized == NULL ||
      raw->data == NULL || raw->bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstResultRawAppend(&raw_copy, raw->data, raw->bytes);
  if (code == QST_OK) {
    raw_copy.rows_offset = raw->rows_offset;
    code = qstResultCloneQueryResult(normalized, &normalized_copy);
  }
  if (code != QST_OK) {
    qst_result_reset(&normalized_copy);
    qstResultRawBufferReset(&raw_copy);
    return code;
  }
  qst_result_reset(&entry->normalized);
  qstResultRawBufferReset(&entry->raw);
  entry->key = *key;
  entry->raw = raw_copy;
  entry->normalized = normalized_copy;
  entry->valid = 1;
  return QST_OK;
}

EQstCode qst_result_capture_verified_cached(
    SQstNativeClient *client, void *result, const SQstDigest256 *sql_sha256,
    SQstResultCaptureCache *cache, SQstQueryResult *out_result) {
  SQstResultCaptureCacheState *state = NULL;
  SQstResultCaptureCacheEntry *entry = NULL;
  SQstResultAsyncCapture *capture = NULL;
  SQstResultRawBuffer raw = {0};
  EQstCode code = QST_OK;

  if (cache == NULL || sql_sha256 == NULL) {
    return qst_native_capture_result(client, result, out_result);
  }
  if (client == NULL || result == NULL || out_result == NULL ||
      client->fetch_row == NULL || client->fetch_lengths == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (cache->private_state == NULL) {
    cache->private_state = calloc(1, sizeof(SQstResultCaptureCacheState));
    if (cache->private_state == NULL) {
      return QST_E_RESOURCE;
    }
  }
  state = (SQstResultCaptureCacheState *)cache->private_state;
  entry = qstResultCaptureCacheFind(state, sql_sha256);
  code = qst_result_async_capture_begin(client, result, &capture);
  if (code != QST_OK) {
    return code;
  }
  code = qstResultRawAppendSchema(&raw, capture);
  while (code == QST_OK) {
    void **row = client->fetch_row(result);
    int32_t *lengths = NULL;

    if (row == NULL) {
      break;
    }
    lengths = client->fetch_lengths(result);
    if (lengths == NULL) {
      code = QST_E_RESULT_STORE;
      break;
    }
    code = qstResultRawAppendRow(&raw, capture, row, lengths);
  }
  if (code != QST_OK) {
    qstResultRawBufferReset(&raw);
    qst_result_async_capture_abort(capture);
    return code;
  }
  if (entry != NULL && entry->raw.bytes == raw.bytes &&
      memcmp(entry->raw.data, raw.data, (size_t)raw.bytes) == 0) {
    ++state->hit_count;
    code = qstResultCloneQueryResult(&entry->normalized, out_result);
    qstResultRawBufferReset(&raw);
    qst_result_async_capture_abort(capture);
    return code;
  }
  ++state->miss_count;
  code = qstResultCaptureRawRows(capture, &raw);
  if (code == QST_OK) {
    code = qst_result_async_capture_finish(capture, out_result);
    capture = NULL;
  }
  if (code == QST_OK) {
    entry = qstResultCaptureCacheSelect(state, sql_sha256);
    code = qstResultCaptureCacheReplace(entry, sql_sha256, &raw, out_result);
  }
  qstResultRawBufferReset(&raw);
  qst_result_async_capture_abort(capture);
  return code;
}

void qst_result_capture_cache_destroy(SQstResultCaptureCache *cache) {
  SQstResultCaptureCacheState *state = NULL;

  if (cache == NULL || cache->private_state == NULL) {
    return;
  }
  state = (SQstResultCaptureCacheState *)cache->private_state;
  if (getenv("QST_PROFILE_PHASES") != NULL) {
    (void)fprintf(stderr,
                  "taosQueryTest: result_capture_cache hits=%llu misses=%llu\n",
                  (unsigned long long)state->hit_count,
                  (unsigned long long)state->miss_count);
  }
  for (uint32_t index = 0; index < QST_RESULT_CAPTURE_CACHE_ENTRIES; ++index) {
    qst_result_reset(&state->entries[index].normalized);
    qstResultRawBufferReset(&state->entries[index].raw);
  }
  free(state);
  cache->private_state = NULL;
}

EQstCode qst_native_capture_result_stream(
    SQstNativeClient *client, void *result, void *file,
    uint64_t max_row_hash_bytes, SQstResultStreamSummary *out_summary) {
  TAOS_FIELD *fields = NULL;
  char *columns = NULL;
  uint64_t columns_bytes = 0;
  int32_t column_count = 0;
  int32_t precision = 0;
  SQstResultStream stream = {0};
  EQstCode code = QST_OK;

  if (NULL == client || NULL == result || NULL == file || NULL == out_summary ||
      NULL == client->errno_fn || NULL == client->fetch_row ||
      NULL == client->num_fields || NULL == client->fetch_fields ||
      NULL == client->fetch_lengths || NULL == client->result_precision) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (client->errno_fn(result) != 0) {
    return QST_E_RESULT_STORE;
  }
  column_count = client->num_fields(result);
  precision = client->result_precision(result);
  if (column_count < 0 || (column_count != 0 &&
      NULL == (fields = client->fetch_fields(result)))) {
    return QST_E_RESULT_STORE;
  }
  code = qstResultBuildColumns(fields, column_count, precision, &columns,
                               &columns_bytes);
  if (code != QST_OK) {
    return code;
  }
  code = qst_result_stream_begin(&stream, file, columns, columns_bytes,
                                 max_row_hash_bytes);
  free(columns);
  if (code != QST_OK) {
    return code;
  }
  for (;;) {
    void **row = client->fetch_row(result);
    int32_t *lengths = NULL;
    tools_cJSON *encoded_row = NULL;
    char *printed_row = NULL;

    if (NULL == row) {
      break;
    }
    lengths = client->fetch_lengths(result);
    encoded_row = tools_cJSON_CreateArray();
    if (NULL == lengths || NULL == encoded_row) {
      tools_cJSON_Delete(encoded_row);
      qst_result_stream_abort(&stream);
      return NULL == lengths ? QST_E_RESULT_STORE : QST_E_RESOURCE;
    }
    for (int32_t index = 0; index < column_count; ++index) {
      tools_cJSON *value = qstResultAddValue(row[index], lengths[index],
                                             fields[index].type, precision);
      if (NULL == value) {
        tools_cJSON_Delete(encoded_row);
        qst_result_stream_abort(&stream);
        return QST_E_VERSION_CAPABILITY;
      }
      tools_cJSON_AddItemToArray(encoded_row, value);
    }
    printed_row = tools_cJSON_PrintUnformatted(encoded_row);
    tools_cJSON_Delete(encoded_row);
    if (NULL == printed_row) {
      qst_result_stream_abort(&stream);
      return QST_E_RESOURCE;
    }
    code = qst_result_stream_append_row(&stream, printed_row,
                                        strlen(printed_row));
    tools_cJSON_free(printed_row);
    if (code != QST_OK) {
      qst_result_stream_abort(&stream);
      return code;
    }
  }
  return qst_result_stream_finish(&stream, out_summary);
}

static tools_cJSON *qstResultParse(const SQstQueryResult *result) {
  char *text = NULL;
  tools_cJSON *parsed = NULL;

  if (NULL == result || NULL == result->data || result->bytes == 0 ||
      result->bytes >= SIZE_MAX) {
    return NULL;
  }
  text = (char *)calloc((size_t)result->bytes + 1U, 1);
  if (NULL == text) {
    return NULL;
  }
  memcpy(text, result->data, (size_t)result->bytes);
  parsed = tools_cJSON_ParseWithOpts(text, NULL, 1);
  free(text);
  return parsed;
}

static int32_t qstResultHasError(const tools_cJSON *root,
                                 const tools_cJSON **out_error) {
  const tools_cJSON *error = NULL;

  if (root == NULL || out_error == NULL) return -1;
  error = tools_cJSON_GetObjectItemCaseSensitive(root, "error");
  *out_error = error;
  return error == NULL ? 0 : (tools_cJSON_IsObject(error) ? 1 : -1);
}

static int qstResultStringCompare(const void *left, const void *right) {
  const char *const *left_text = (const char *const *)left;
  const char *const *right_text = (const char *const *)right;

  return strcmp(*left_text, *right_text);
}

static EQstCode qstResultCompareUnorderedRows(const tools_cJSON *actual_rows,
                                              const tools_cJSON *expected_rows,
                                              SQstResultDiff *out_diff) {
  const int32_t row_count = tools_cJSON_GetArraySize(actual_rows);
  char **actual_text = NULL;
  char **expected_text = NULL;
  EQstCode code = QST_OK;

  if (row_count != tools_cJSON_GetArraySize(expected_rows)) {
    qstResultSetDiff(out_diff, QST_RESULT_DIFF_ROW_COUNT, 0, 0,
                     "row count differs");
    return QST_OK;
  }
  if (row_count == 0) {
    return QST_OK;
  }
  actual_text = (char **)calloc((size_t)row_count, sizeof(*actual_text));
  expected_text = (char **)calloc((size_t)row_count, sizeof(*expected_text));
  if (NULL == actual_text || NULL == expected_text) {
    code = QST_E_RESOURCE;
    goto _return;
  }
  for (int32_t index = 0; index < row_count; ++index) {
    actual_text[index] = tools_cJSON_PrintUnformatted(
        tools_cJSON_GetArrayItem(actual_rows, index));
    expected_text[index] = tools_cJSON_PrintUnformatted(
        tools_cJSON_GetArrayItem(expected_rows, index));
    if (NULL == actual_text[index] || NULL == expected_text[index]) {
      code = QST_E_RESOURCE;
      goto _return;
    }
  }
  qsort(actual_text, (size_t)row_count, sizeof(*actual_text), qstResultStringCompare);
  qsort(expected_text, (size_t)row_count, sizeof(*expected_text), qstResultStringCompare);
  for (int32_t index = 0; index < row_count; ++index) {
    if (strcmp(actual_text[index], expected_text[index]) != 0) {
      qstResultSetDiff(out_diff, QST_RESULT_DIFF_UNORDERED_ROW, 0,
                       (uint64_t)index, "row multiset differs");
      break;
    }
  }

_return:
  if (NULL != actual_text) {
    for (int32_t index = 0; index < row_count; ++index) {
      tools_cJSON_free(actual_text[index]);
    }
  }
  if (NULL != expected_text) {
    for (int32_t index = 0; index < row_count; ++index) {
      tools_cJSON_free(expected_text[index]);
    }
  }
  free(actual_text);
  free(expected_text);
  return code;
}

EQstCode qst_result_compare(const SQstQueryResult *actual,
                            const SQstQueryResult *expected,
                            int32_t rows_ordered,
                            SQstResultDiff *out_diff) {
  tools_cJSON *actual_root = NULL;
  tools_cJSON *expected_root = NULL;
  tools_cJSON *actual_columns = NULL;
  tools_cJSON *expected_columns = NULL;
  tools_cJSON *actual_rows = NULL;
  tools_cJSON *expected_rows = NULL;
  tools_cJSON *actual_format = NULL;
  tools_cJSON *expected_format = NULL;
  const tools_cJSON *actual_error = NULL;
  const tools_cJSON *expected_error = NULL;
  EQstCode code = QST_OK;

  if (NULL == actual || NULL == expected || NULL == out_diff) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstResultSetDiff(out_diff, QST_RESULT_DIFF_NONE, 0, 0, "equal");
  actual_root = qstResultParse(actual);
  expected_root = qstResultParse(expected);
  if (NULL == actual_root || NULL == expected_root ||
      !tools_cJSON_IsObject(actual_root) || !tools_cJSON_IsObject(expected_root)) {
    qstResultSetDiff(out_diff, QST_RESULT_DIFF_FORMAT, 0, 0,
                     "invalid query-result-v1 JSON");
    code = QST_E_INVALID_ARGUMENT;
    goto _return;
  }
  actual_format = tools_cJSON_GetObjectItemCaseSensitive(actual_root, "format");
  expected_format = tools_cJSON_GetObjectItemCaseSensitive(expected_root, "format");
  if (!tools_cJSON_IsString(actual_format) ||
      !tools_cJSON_IsString(expected_format) ||
      strcmp(actual_format->valuestring, "query-result-v1") != 0 ||
      strcmp(expected_format->valuestring, "query-result-v1") != 0) {
    qstResultSetDiff(out_diff, QST_RESULT_DIFF_FORMAT, 0, 0,
                     "unsupported result format");
    code = QST_E_INVALID_ARGUMENT;
    goto _return;
  }
  {
    const int32_t actual_error_state = qstResultHasError(actual_root, &actual_error);
    const int32_t expected_error_state = qstResultHasError(expected_root, &expected_error);
    if (actual_error_state < 0 || expected_error_state < 0) {
      qstResultSetDiff(out_diff, QST_RESULT_DIFF_FORMAT, 0, 0,
                       "invalid error result");
      code = QST_E_INVALID_ARGUMENT;
      goto _return;
    }
    if (actual_error_state != expected_error_state) {
      qstResultSetDiff(out_diff, QST_RESULT_DIFF_ERROR, 0, 0,
                       "success and error results differ");
      goto _return;
    }
    if (actual_error_state == 1) {
      const tools_cJSON *actual_code = tools_cJSON_GetObjectItemCaseSensitive(
          actual_error, "code");
      const tools_cJSON *expected_code = tools_cJSON_GetObjectItemCaseSensitive(
          expected_error, "code");
      const tools_cJSON *actual_message = tools_cJSON_GetObjectItemCaseSensitive(
          actual_error, "message");
      const tools_cJSON *expected_message = tools_cJSON_GetObjectItemCaseSensitive(
          expected_error, "message");
      if (!tools_cJSON_IsNumber(actual_code) || !tools_cJSON_IsNumber(expected_code) ||
          actual_code->valuedouble != expected_code->valuedouble) {
        qstResultSetDiff(out_diff, QST_RESULT_DIFF_ERROR_CODE, 0, 0,
                         "error code differs");
        goto _return;
      }
      if (!tools_cJSON_IsString(actual_message) ||
          !tools_cJSON_IsString(expected_message) ||
          strcmp(actual_message->valuestring, expected_message->valuestring) != 0) {
        qstResultSetDiff(out_diff, QST_RESULT_DIFF_ERROR_MESSAGE, 0, 0,
                         "error message differs");
      }
      goto _return;
    }
  }
  actual_columns = tools_cJSON_GetObjectItemCaseSensitive(actual_root, "columns");
  expected_columns = tools_cJSON_GetObjectItemCaseSensitive(expected_root, "columns");
  actual_rows = tools_cJSON_GetObjectItemCaseSensitive(actual_root, "rows");
  expected_rows = tools_cJSON_GetObjectItemCaseSensitive(expected_root, "rows");
  if (!tools_cJSON_IsArray(actual_columns) || !tools_cJSON_IsArray(expected_columns) ||
      !tools_cJSON_IsArray(actual_rows) || !tools_cJSON_IsArray(expected_rows)) {
    qstResultSetDiff(out_diff, QST_RESULT_DIFF_FORMAT, 0, 0,
                     "query-result-v1 lacks columns or rows");
    code = QST_E_INVALID_ARGUMENT;
    goto _return;
  }
  if (tools_cJSON_GetArraySize(actual_columns) !=
      tools_cJSON_GetArraySize(expected_columns)) {
    qstResultSetDiff(out_diff, QST_RESULT_DIFF_COLUMN_COUNT, 0, 0,
                     "column count differs");
    goto _return;
  }
  for (int32_t index = 0; index < tools_cJSON_GetArraySize(actual_columns); ++index) {
    if (!tools_cJSON_Compare(tools_cJSON_GetArrayItem(actual_columns, index),
                             tools_cJSON_GetArrayItem(expected_columns, index), 1)) {
      qstResultSetDiff(out_diff, QST_RESULT_DIFF_COLUMN_DEFINITION,
                       (uint32_t)index, 0, "column definition differs");
      goto _return;
    }
  }
  if (rows_ordered != 0) {
    if (tools_cJSON_GetArraySize(actual_rows) != tools_cJSON_GetArraySize(expected_rows)) {
      qstResultSetDiff(out_diff, QST_RESULT_DIFF_ROW_COUNT, 0, 0,
                       "row count differs");
      goto _return;
    }
    for (int32_t index = 0; index < tools_cJSON_GetArraySize(actual_rows); ++index) {
      if (!tools_cJSON_Compare(tools_cJSON_GetArrayItem(actual_rows, index),
                               tools_cJSON_GetArrayItem(expected_rows, index), 1)) {
        qstResultSetDiff(out_diff, QST_RESULT_DIFF_ORDERED_ROW, 0,
                         (uint64_t)index, "ordered row differs");
        goto _return;
      }
    }
  } else {
    code = qstResultCompareUnorderedRows(actual_rows, expected_rows, out_diff);
  }

_return:
  tools_cJSON_Delete(actual_root);
  tools_cJSON_Delete(expected_root);
  return code;
}
