/*
 * Copyright (c) 2026 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#define _POSIX_C_SOURCE 200809L

#include "qst_result.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "qst_sha256.h"
#include "toolscJson.h"

enum {
  QST_RESULT_STREAM_DEFAULT_HASH_BYTES = 64U * 1024U * 1024U,
  QST_RESULT_STREAM_ROW_HASH_BYTES = 32U,
};

static const char kQstResultStreamPrefix[] =
    "{\"format\":\"query-result-v1\",\"columns\":";
static const char kQstResultStreamRowsPrefix[] = ",\"rows\":[";

typedef struct {
  FILE *file;
  SQstSha256 ordered;
  SQstSha256 schema;
  uint8_t *row_hashes;
  uint64_t row_count;
  uint64_t row_hash_capacity;
  uint64_t logical_bytes;
  uint64_t max_row_hash_bytes;
  int32_t finished;
} SQstResultStreamState;

static void qstResultStreamSetDiff(SQstResultDiff *diff,
                                   EQstResultDiffKind kind,
                                   const char *summary) {
  if (NULL == diff) {
    return;
  }
  memset(diff, 0, sizeof(*diff));
  diff->kind = (uint32_t)kind;
  if (NULL != summary) {
    (void)snprintf(diff->summary, sizeof(diff->summary), "%s", summary);
  }
}

static EQstCode qstResultStreamWrite(SQstResultStreamState *state,
                                     const uint8_t *bytes, uint64_t length) {
  if (NULL == state || NULL == state->file || (NULL == bytes && length != 0) ||
      length > UINT64_MAX - state->logical_bytes || length > SIZE_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (length != 0 && fwrite(bytes, 1, (size_t)length, state->file) != length) {
    return QST_E_PENDING_IO;
  }
  qst_sha256_update(&state->ordered, bytes, length);
  state->logical_bytes += length;
  return QST_OK;
}

static int qstResultStreamHashCompare(const void *left, const void *right) {
  return memcmp(left, right, QST_RESULT_STREAM_ROW_HASH_BYTES);
}

static EQstCode qstResultStreamCanonicalJson(const char *json, uint64_t bytes,
                                             int32_t require_array,
                                             char **out_text,
                                             uint64_t *out_bytes) {
  char *input = NULL;
  char *printed = NULL;
  tools_cJSON *value = NULL;

  if (NULL == json || NULL == out_text || NULL == out_bytes || bytes == 0 ||
      bytes >= SIZE_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  input = (char *)calloc((size_t)bytes + 1U, 1);
  if (NULL == input) {
    return QST_E_RESOURCE;
  }
  memcpy(input, json, (size_t)bytes);
  value = tools_cJSON_ParseWithOpts(input, NULL, 1);
  free(input);
  if (NULL == value || (require_array != 0 && !tools_cJSON_IsArray(value))) {
    tools_cJSON_Delete(value);
    return QST_E_INVALID_ARGUMENT;
  }
  printed = tools_cJSON_PrintUnformatted(value);
  tools_cJSON_Delete(value);
  if (NULL == printed) {
    return QST_E_RESOURCE;
  }
  *out_text = printed;
  *out_bytes = strlen(printed);
  return QST_OK;
}

EQstCode qst_result_stream_begin(SQstResultStream *stream, void *file,
                                 const char *columns_json,
                                 uint64_t columns_bytes,
                                 uint64_t max_row_hash_bytes) {
  SQstResultStreamState *state = NULL;
  char *columns = NULL;
  uint64_t canonical_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == stream || NULL == file || NULL == columns_json ||
      columns_bytes == 0 || stream->private_state != NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(stream, 0, sizeof(*stream));
  code = qstResultStreamCanonicalJson(columns_json, columns_bytes, 1,
                                      &columns, &canonical_bytes);
  if (code != QST_OK) {
    return code;
  }
  state = (SQstResultStreamState *)calloc(1, sizeof(*state));
  if (NULL == state) {
    free(columns);
    return QST_E_RESOURCE;
  }
  state->file = (FILE *)file;
  state->max_row_hash_bytes = max_row_hash_bytes == 0 ?
      QST_RESULT_STREAM_DEFAULT_HASH_BYTES : max_row_hash_bytes;
  qst_sha256_init(&state->ordered);
  qst_sha256_init(&state->schema);
  qst_sha256_update(&state->schema, (const uint8_t *)columns, canonical_bytes);
  code = qstResultStreamWrite(state, (const uint8_t *)kQstResultStreamPrefix,
                              sizeof(kQstResultStreamPrefix) - 1U);
  if (code == QST_OK) {
    code = qstResultStreamWrite(state, (const uint8_t *)columns,
                                canonical_bytes);
  }
  if (code == QST_OK) {
    code = qstResultStreamWrite(state,
                                (const uint8_t *)kQstResultStreamRowsPrefix,
                                sizeof(kQstResultStreamRowsPrefix) - 1U);
  }
  free(columns);
  if (code != QST_OK) {
    free(state);
    return code;
  }
  stream->private_state = state;
  return QST_OK;
}

EQstCode qst_result_stream_append_row(SQstResultStream *stream,
                                      const char *row_json,
                                      uint64_t row_bytes) {
  SQstResultStreamState *state = NULL;
  char *row = NULL;
  uint64_t canonical_bytes = 0;
  uint8_t row_hash[QST_RESULT_STREAM_ROW_HASH_BYTES] = {0};
  SQstSha256 row_digest = {0};
  EQstCode code = QST_OK;

  if (NULL == stream || NULL == (state = stream->private_state) ||
      state->finished != 0 || NULL == row_json) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstResultStreamCanonicalJson(row_json, row_bytes, 1, &row,
                                      &canonical_bytes);
  if (code != QST_OK) {
    return code;
  }
  if (state->row_count != 0) {
    code = qstResultStreamWrite(state, (const uint8_t *)",", 1U);
  }
  if (code == QST_OK) {
    code = qstResultStreamWrite(state, (const uint8_t *)row, canonical_bytes);
  }
  if (code == QST_OK) {
    qst_sha256_init(&row_digest);
    qst_sha256_update(&row_digest, (const uint8_t *)row, canonical_bytes);
    qst_sha256_final(&row_digest, row_hash);
    if (state->row_count == UINT64_MAX ||
        state->row_count + 1U > state->max_row_hash_bytes /
            QST_RESULT_STREAM_ROW_HASH_BYTES) {
      code = QST_E_RESOURCE;
    }
  }
  if (code == QST_OK) {
    if (state->row_count == state->row_hash_capacity) {
      uint64_t capacity = state->row_hash_capacity == 0 ? 256U :
          state->row_hash_capacity * 2U;
      uint8_t *grown = NULL;
      if (capacity < state->row_count + 1U ||
          capacity > SIZE_MAX / QST_RESULT_STREAM_ROW_HASH_BYTES) {
        code = QST_E_RESOURCE;
      } else {
        grown = (uint8_t *)realloc(state->row_hashes,
                                   (size_t)(capacity *
                                            QST_RESULT_STREAM_ROW_HASH_BYTES));
        if (NULL == grown) {
          code = QST_E_RESOURCE;
        } else {
          state->row_hashes = grown;
          state->row_hash_capacity = capacity;
        }
      }
    }
  }
  if (code == QST_OK) {
    memcpy(state->row_hashes + state->row_count *
               QST_RESULT_STREAM_ROW_HASH_BYTES,
           row_hash, sizeof(row_hash));
    ++state->row_count;
  }
  free(row);
  return code;
}

EQstCode qst_result_stream_finish(SQstResultStream *stream,
                                  SQstResultStreamSummary *out_summary) {
  SQstResultStreamState *state = NULL;
  SQstSha256 unordered = {0};
  SQstResultStreamSummary summary = {0};
  EQstCode code = QST_OK;

  if (NULL == stream || NULL == (state = stream->private_state) ||
      NULL == out_summary || state->finished != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstResultStreamWrite(state, (const uint8_t *)"]}", 2U);
  if (code != QST_OK) {
    qst_result_stream_abort(stream);
    return code;
  }
  if (state->row_count > SIZE_MAX ||
      state->row_count > UINT64_MAX / QST_RESULT_STREAM_ROW_HASH_BYTES) {
    qst_result_stream_abort(stream);
    return QST_E_RESOURCE;
  }
  if (state->row_count > 1U) {
    qsort(state->row_hashes, (size_t)state->row_count,
          QST_RESULT_STREAM_ROW_HASH_BYTES, qstResultStreamHashCompare);
  }
  qst_sha256_init(&unordered);
  qst_sha256_update(&unordered, state->row_hashes,
                    state->row_count * QST_RESULT_STREAM_ROW_HASH_BYTES);
  summary.comparator_version = QST_RESULT_STREAM_COMPARATOR_VERSION;
  summary.logical_bytes = state->logical_bytes;
  summary.row_count = state->row_count;
  qst_sha256_final(&state->schema, summary.schema_sha256);
  qst_sha256_final(&state->ordered, summary.ordered_sha256);
  qst_sha256_final(&unordered, summary.unordered_sha256);
  *out_summary = summary;
  state->finished = 1;
  free(state->row_hashes);
  free(state);
  stream->private_state = NULL;
  return QST_OK;
}

void qst_result_stream_abort(SQstResultStream *stream) {
  SQstResultStreamState *state = NULL;

  if (NULL == stream || NULL == (state = stream->private_state)) {
    return;
  }
  free(state->row_hashes);
  free(state);
  stream->private_state = NULL;
}

EQstCode qst_result_stream_compare(const SQstResultStreamSummary *actual,
                                   const SQstResultStreamSummary *expected,
                                   int32_t rows_ordered,
                                   SQstResultDiff *out_diff) {
  const uint8_t *actual_digest = NULL;
  const uint8_t *expected_digest = NULL;

  if (NULL == actual || NULL == expected || NULL == out_diff ||
      (rows_ordered != 0 && rows_ordered != 1)) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstResultStreamSetDiff(out_diff, QST_RESULT_DIFF_NONE, "equal");
  if (actual->comparator_version != expected->comparator_version) {
    qstResultStreamSetDiff(out_diff, QST_RESULT_DIFF_FORMAT,
                           "stream comparator version differs");
    return QST_OK;
  }
  if (memcmp(actual->schema_sha256, expected->schema_sha256,
             sizeof(actual->schema_sha256)) != 0) {
    qstResultStreamSetDiff(out_diff, QST_RESULT_DIFF_COLUMN_DEFINITION,
                           "stream schema digest differs");
    return QST_OK;
  }
  if (actual->row_count != expected->row_count) {
    qstResultStreamSetDiff(out_diff, QST_RESULT_DIFF_ROW_COUNT,
                           "stream row count differs");
    return QST_OK;
  }
  if (actual->logical_bytes != expected->logical_bytes) {
    qstResultStreamSetDiff(out_diff, QST_RESULT_DIFF_ROW_COUNT,
                           "stream logical byte count differs");
    return QST_OK;
  }
  actual_digest = rows_ordered != 0 ? actual->ordered_sha256 :
      actual->unordered_sha256;
  expected_digest = rows_ordered != 0 ? expected->ordered_sha256 :
      expected->unordered_sha256;
  if (memcmp(actual_digest, expected_digest, sizeof(actual->ordered_sha256)) != 0) {
    qstResultStreamSetDiff(out_diff, rows_ordered != 0 ?
        QST_RESULT_DIFF_ORDERED_ROW : QST_RESULT_DIFF_UNORDERED_ROW,
        rows_ordered != 0 ? "stream ordered digest differs" :
                            "stream row multiset digest differs");
  }
  return QST_OK;
}
