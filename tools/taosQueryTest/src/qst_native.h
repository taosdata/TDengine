/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

/* Native TDengine C connector boundary used by dataset and result modules. */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_NATIVE_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_NATIVE_H_

#include <stdint.h>

#include "qst_result.h"
#include "qst_types.h"

typedef void (*QstNativeAsyncCallback)(void *param, void *result,
                                       int32_t code);

typedef struct SQstNativeClient {
  void *library;
  void *connection;
  void *(*connect)(const char *, const char *, const char *, const char *, uint16_t);
  int32_t (*options)(int32_t, const void *);
  void (*close)(void *);
  void *(*query)(void *, const char *);
  void (*query_a)(void *, const char *, QstNativeAsyncCallback, void *);
  void (*fetch_rows_a)(void *, QstNativeAsyncCallback, void *);
  void (*kill_query)(void *);
  int32_t (*errno_fn)(void *);
  const char *(*errstr)(void *);
  void (*free_result)(void *);
  void **(*fetch_row)(void *);
  int32_t (*fetch_raw_block)(void *, int32_t *, void **);
  int32_t (*num_fields)(void *);
  void *(*fetch_fields)(void *);
  int32_t *(*fetch_lengths)(void *);
  int32_t (*result_precision)(void *);
  void *(*stmt_init)(void *);
  int32_t (*stmt_prepare)(void *, const char *, unsigned long);
  int32_t (*stmt_bind_param)(void *, void *);
  int32_t (*stmt_add_batch)(void *);
  int32_t (*stmt_execute)(void *);
  int32_t (*stmt_close)(void *);
  char *(*stmt_errstr)(void *);
} SQstNativeClient;

EQstCode qst_native_open(SQstNativeClient *out_client);
/* Opens one process-local native client for the explicitly assigned endpoint. */
EQstCode qst_native_open_with_endpoint(SQstNativeClient *out_client,
                                       const char *host, uint16_t port,
                                       const char *config_dir);
/* Opens one process-local native client for an explicitly assigned endpoint
 * and optional native credentials. Empty credentials retain TDengine defaults. */
EQstCode qst_native_open_with_endpoint_credentials(
    SQstNativeClient *out_client, const char *host, uint16_t port,
    const char *config_dir, const char *user, const char *password);
EQstCode qst_native_execute(SQstNativeClient *client, const char *sql);
/* Executes one native statement without emitting its server error. The raw
 * native code remains available to callers that implement bounded retries. */
EQstCode qst_native_execute_silent(SQstNativeClient *client, const char *sql,
                                   int32_t *out_native_code);
/* Executes one homogeneous prepared-statement batch. When execute_attempted
 * is zero on failure no data-bearing request reached the server. */
EQstCode qst_native_execute_stmt_batch(SQstNativeClient *client,
                                       const char *sql,
                                       void *row_binds,
                                       uint32_t row_count,
                                       uint32_t field_count,
                                       int32_t *out_execute_attempted);
/* Submits one query through the native asynchronous TDengine API. */
EQstCode qst_native_execute_async(SQstNativeClient *client, const char *sql,
                                  QstNativeAsyncCallback callback,
                                  void *callback_param);
/* Cancels the active query on this exact native connector connection. */
EQstCode qst_native_kill_query(SQstNativeClient *client);
EQstCode qst_native_count(SQstNativeClient *client, const char *sql,
                          int64_t *out_count);
/* Executes one read-only native query and transfers its owned normalized
 * query-result-v1 document to out_result. */
EQstCode qst_native_query_result(SQstNativeClient *client, const char *sql,
                                 SQstQueryResult *out_result);
/* Collects bounded, redacted server version and capability metadata. The
 * returned JSON is owned by the caller's buffer and never contains credentials. */
EQstCode qst_native_readiness_json(SQstNativeClient *client, char *out_json,
                                   uint64_t out_bytes);
/* Converts one successful native result into an owned query-result-v1 JSON. */
EQstCode qst_native_capture_result(SQstNativeClient *client, void *result,
                                   SQstQueryResult *out_result);
/* Captures a query result through the public raw-block API. This is intended
 * only for pair-local experiments; callers must retain the row API fallback
 * when the returned capability code says that a result cannot be decoded
 * losslessly by this public representation. */
EQstCode qst_native_capture_raw_result(SQstNativeClient *client, void *result,
                                       SQstQueryResult *out_result);
/* Streams one successful native result as query-result-v1 without building a
 * complete rows array; FILE remains owned by the caller. */
EQstCode qst_native_capture_result_stream(
    SQstNativeClient *client, void *result, void *file,
    uint64_t max_row_hash_bytes, SQstResultStreamSummary *out_summary);
EQstCode qst_native_close(SQstNativeClient *client);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_SRC_QST_NATIVE_H_
