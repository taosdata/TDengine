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

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RESULT_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RESULT_H_

#include <stdint.h>

#include "qst_types.h"

typedef struct SQstNativeClient SQstNativeClient;

#define QST_RESULT_STREAM_COMPARATOR_VERSION 1U

typedef struct {
  uint8_t *data;
  uint64_t bytes;
  int64_t row_count;
} SQstQueryResult;

typedef enum {
  QST_RESULT_DIFF_NONE = 0,
  QST_RESULT_DIFF_FORMAT = 1,
  QST_RESULT_DIFF_COLUMN_COUNT = 2,
  QST_RESULT_DIFF_COLUMN_DEFINITION = 3,
  QST_RESULT_DIFF_ROW_COUNT = 4,
  QST_RESULT_DIFF_ORDERED_ROW = 5,
  QST_RESULT_DIFF_UNORDERED_ROW = 6,
  QST_RESULT_DIFF_ERROR = 7,
  QST_RESULT_DIFF_ERROR_CODE = 8,
  QST_RESULT_DIFF_ERROR_MESSAGE = 9,
} EQstResultDiffKind;

typedef struct {
  uint32_t kind;
  uint32_t column_index;
  uint64_t row_index;
  char summary[256];
} SQstResultDiff;

/* Bounded metadata produced while a query-result-v1 document is streamed. */
typedef struct {
  uint32_t comparator_version;
  uint64_t logical_bytes;
  uint64_t row_count;
  uint8_t schema_sha256[32];
  uint8_t ordered_sha256[32];
  uint8_t unordered_sha256[32];
} SQstResultStreamSummary;

/* Opaque writer state; the output stream remains owned by the caller. */
typedef struct {
  void *private_state;
} SQstResultStream;

/* Opaque asynchronous result normalizer. The native connector invokes its
 * row callback on a worker thread; callers must keep this state until the
 * zero-row completion callback and then call finish or abort exactly once. */
typedef struct SQstResultAsyncCapture SQstResultAsyncCapture;

/* Pair-local cache state for verified normalized result documents. It is never
 * shared across processes and does not substitute for fetching a live result. */
typedef struct {
  void *private_state;
} SQstResultCaptureCache;

/* Releases result bytes allocated by qst_native_capture_result. */
void qst_result_reset(SQstQueryResult *result);

/* Starts, appends one fetch_rows_a block, and finishes a query-result-v1
 * document without waiting synchronously for later transport blocks. */
EQstCode qst_result_async_capture_begin(SQstNativeClient *client, void *result,
                                         SQstResultAsyncCapture **out_capture);
EQstCode qst_result_async_capture_rows(SQstNativeClient *client, void *result,
                                        int32_t row_count,
                                        SQstResultAsyncCapture *capture);
/* Appends a row already returned by taos_fetch_row; a NULL row is an end
 * marker owned by the caller and is not passed to this function. */
EQstCode qst_result_async_capture_row(SQstNativeClient *client, void *result,
                                       void **row,
                                       SQstResultAsyncCapture *capture);
EQstCode qst_result_async_capture_finish(SQstResultAsyncCapture *capture,
                                          SQstQueryResult *out_result);
void qst_result_async_capture_abort(SQstResultAsyncCapture *capture);

/* Produces query-result-v1 from the documented native raw-block response.
 * Results containing client-side converted types report QST_E_VERSION_CAPABILITY
 * before fetching, so callers may safely use the ordinary row API instead. */
EQstCode qst_result_capture_raw_blocks(SQstNativeClient *client, void *result,
                                       SQstQueryResult *out_result);

/* Fetches every current result cell into a lossless private snapshot. A cached
 * document is reused only when that snapshot is byte-for-byte identical for
 * the supplied SQL digest; otherwise the current snapshot is serialized. */
EQstCode qst_result_capture_verified_cached(
    SQstNativeClient *client, void *result, const SQstDigest256 *sql_sha256,
    SQstResultCaptureCache *cache, SQstQueryResult *out_result);
void qst_result_capture_cache_destroy(SQstResultCaptureCache *cache);

/* Builds a normalized error result without exposing the full provider text. */
EQstCode qst_result_build_error(int32_t error_code, const char *message,
                                SQstQueryResult *out_result);

/*
 * Compares two query-result-v1 documents. Columns always compare in returned
 * order. Rows compare in returned order when rows_ordered is nonzero, and as
 * a multiset otherwise. The caller owns both input buffers and out_diff.
 */
EQstCode qst_result_compare(const SQstQueryResult *actual,
                            const SQstQueryResult *expected,
                            int32_t rows_ordered,
                            SQstResultDiff *out_diff);

/*
 * Starts a canonical query-result-v1 stream on a caller-owned FILE pointer.
 * columns_json must be one JSON array in returned column order. The writer
 * retains no caller buffer after this call. max_row_hash_bytes bounds the
 * memory used for unordered-row summaries; zero selects the module default.
 */
EQstCode qst_result_stream_begin(SQstResultStream *stream, void *file,
                                 const char *columns_json,
                                 uint64_t columns_bytes,
                                 uint64_t max_row_hash_bytes);
/* Appends one JSON row array; only the canonicalized row is retained briefly. */
EQstCode qst_result_stream_append_row(SQstResultStream *stream,
                                      const char *row_json,
                                      uint64_t row_bytes);
/* Finishes the document and returns bounded comparison metadata. */
EQstCode qst_result_stream_finish(SQstResultStream *stream,
                                  SQstResultStreamSummary *out_summary);
/* Aborts a stream and releases writer-owned memory without closing FILE. */
void qst_result_stream_abort(SQstResultStream *stream);
/* Compares summaries without exposing complete rows or values in the diff. */
EQstCode qst_result_stream_compare(const SQstResultStreamSummary *actual,
                                   const SQstResultStreamSummary *expected,
                                   int32_t rows_ordered,
                                   SQstResultDiff *out_diff);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RESULT_H_
