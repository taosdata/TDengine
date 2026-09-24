/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RESULT_STORE_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RESULT_STORE_H_

#include "qst_queue.h"
#include "qst_result.h"

/* Local-only result-store connection configuration. */
typedef struct {
  uint64_t run_id;
  SQstDigest256 endpoint_digest;
  const char *endpoint;
  const char *config_dir;
  const char *user;
  const char *password;
  const char *pending_dir;
  const char *quarantine_dir;
  const char *overflow_path;
} SQstResultStoreConfig;

/* Local-only native TDengine C connector handle. */
typedef struct {
  void *native_taos;
  uint64_t run_id;
  SQstDigest256 endpoint_digest;
  uint16_t endpoint_port;
  char endpoint_host[256];
  char config_dir[4096];
  char user[256];
  char password[256];
  char pending_dir[4096];
  char quarantine_dir[4096];
  char overflow_path[4096];
  /* Per-worker cache for the immutable historical table of this run. */
  char historical_fingerprint[65];
  uint32_t historical_empty_known_mask;
  uint32_t historical_empty_mask;
} SQstResultStore;

typedef struct {
  uint64_t run_id;
  const char *pending_dir;
  const char *quarantine_dir;
  uint64_t quota_bytes;
} SQstPendingStore;

typedef struct {
  uint32_t persist_kind;
  uint32_t target;
  uint64_t run_id;
  uint64_t gid;
  uint64_t record_seq;
  const char *ds_fingerprint;
  const char *stable_key;
  const void *payload;
  uint64_t payload_bytes;
  int64_t created_at_ns;
} SQstPendingRecord;

/* Process-local writer for the run's continuous binary pending stream. Each
 * append becomes visible after write() succeeds; no filesystem sync or batch
 * publication barrier is applied. */
typedef struct {
  SQstShmMapping *mapping;
  const SQstPendingStore *store;
  uint64_t first_record_seq;
  uint64_t last_record_seq;
  uint64_t byte_count;
  uint64_t record_count;
  int32_t fd;
  int32_t quota_reserved;
  uint8_t *buffer;
  uint64_t buffer_capacity;
  char final_path[4096];
} SQstPendingBatch;

/* Immutable logical datasets row. Strings are UTF-8 and are stored in the
 * overflow journal when they exceed their result-table column capacity. */
typedef struct {
  int64_t defined_ts_ns;
  const char *ds_fingerprint;
  const char *config_json;
  const char *fingerprint_format;
} SQstDatasetRecord;

/* One complete replacement image of the run summary identified by run_id.
 * A negative statement count means that the corresponding value is unknown. */
typedef struct {
  int64_t begin_ts_ns;
  int64_t end_ts_ns;
  uint32_t status;
  int32_t error_code;
  int32_t has_random_seed;
  int32_t has_error;
  uint64_t random_seed;
  int64_t planned_statement_count;
  int64_t executed_statement_count;
  int64_t succeeded_statement_count;
  int64_t execution_error_statement_count;
  int64_t timeout_statement_count;
  int64_t cancelled_statement_count;
  int64_t passed_statement_count;
  int64_t failed_statement_count;
  int64_t unverified_statement_count;
  const char *mode;
  const char *focus;
  const char *target_set;
  const char *ds_fingerprint;
  const char *tool_version;
  const char *stop_reason;
  const char *parallel_test_info;
  const char *error_message;
} SQstRunRecord;

/* One result row read from a run-specific results child table. Dynamic text
 * and result_content are owned by the record and released by the matching
 * free helper. The row is immutable input for standalone AI actions. */
typedef struct {
  int64_t ts_ns;
  uint64_t gid;
  uint32_t target;
  int32_t verdict;
  int32_t has_error_code;
  int32_t error_code;
  uint32_t result_storage_status;
  char *validator;
  char *diff_summary;
  char *error_message;
  char *analysis_content;
  char *ds_fingerprint;
  SQstQueryResult result_content;
} SQstStoredResult;

/* One statement row read from a run-specific statements child table. All
 * strings are caller-owned and preserve the exact SQL/SHA mapping. */
typedef struct {
  int64_t ts_ns;
  int64_t ended_at_ns;
  uint64_t gid;
  uint64_t sid;
  uint32_t target;
  uint32_t execution_status;
  int64_t result_row_count;
  int32_t has_error_code;
  int32_t error_code;
  char *sql_text;
  char *sql_sha256;
  char *taosd_pid;
  char *taosc_pid;
  char *error_message;
  char *taosd_log_ranges;
  char *taosc_log_ranges;
  char *core_relative_path;
} SQstStoredStatement;

/* A standalone AI result appended after an initial result record. */
typedef struct {
  uint64_t gid;
  uint32_t target;
  int32_t verdict;
  int32_t has_error_code;
  int32_t error_code;
  const char *ds_fingerprint;
  const SQstQueryResult *result_content;
  const char *validator;
  const char *diff_summary;
  const char *error_message;
  const char *analysis_content;
  uint64_t action_id_hash;
} SQstManualAiResult;

EQstCode qst_result_store_open(const SQstResultStoreConfig *config,
                               SQstResultStore *out_store);
/* Copies one validated connection identity without opening a socket. Pending
 * fallback callers use this to ensure a later recovery cannot change target. */
EQstCode qst_result_store_configure_connection(
    const SQstResultStoreConfig *config, SQstResultStore *out_store);
/* Releases the process-local native connector owned by this store. */
EQstCode qst_result_store_close(SQstResultStore *store);
/* Captures bounded, credential-free readiness from the already-open result
 * store connection. An unavailable local connector is represented by a
 * stable unavailable snapshot so startup provenance remains complete. */
EQstCode qst_result_store_readiness_json(const SQstResultStore *store,
                                         char *out_json,
                                         uint64_t out_bytes);
/* Main-process-only DDL preparation for all selected target-specific child tables.
 * worker_count includes the legacy slot zero table and creates one independent
 * statement/result table for every additional judge slot. */
EQstCode qst_result_store_prepare_run(SQstResultStore *store,
                                      uint32_t target_selection_bits,
                                      SQstDigest256 ds_fingerprint,
                                      uint32_t worker_count);
/* Appends one complete oversized field and returns its durable line reference. */
EQstCode qst_result_store_write_overflow(SQstResultStore *store,
                                         const char *table, const char *column,
                                         const char *encoding,
                                         const uint8_t *content,
                                         uint64_t content_bytes,
                                         char *out_reference,
                                         uint64_t reference_bytes);
/* Writes one runs replacement image, or atomically stages it when res_db is
 * unavailable. record_seq is the run-local, monotonically increasing order. */
EQstCode qst_result_store_write_run_record(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, const SQstRunRecord *record,
    uint64_t record_seq, uint32_t *out_durable_source);
/* Writes a datasets definition once per fingerprint, or atomically stages it
 * when res_db is unavailable. Existing matching definitions are idempotent. */
EQstCode qst_result_store_write_dataset_record(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, const SQstDatasetRecord *record,
    uint64_t record_seq, uint32_t *out_durable_source);
/* Looks up the first deterministic passing historical result for one exact
 * dataset, target and SQL digest. out_result is owned by the caller on hit. */
EQstCode qst_result_store_lookup_historical(
    SQstResultStore *store, const char *ds_fingerprint, uint32_t target,
    SQstDigest256 sql_sha256, SQstQueryResult *out_result,
    char *out_validator, uint64_t validator_bytes, int32_t *out_found);
/* Probes one run/target historical table once and caches whether it has rows. */
EQstCode qst_result_store_historical_is_empty(
    SQstResultStore *store, const char *ds_fingerprint, uint32_t target,
    int32_t *out_empty);
/* Reads all result rows for one selected target through the native connector. */
EQstCode qst_result_store_read_results(SQstResultStore *store, uint32_t target,
                                       SQstStoredResult **out_records,
                                       uint32_t *out_count);
void qst_result_store_free_results(SQstStoredResult *records,
                                   uint32_t record_count);
/* Reads statements and preserves the exact SQL-to-SHA mapping for one target. */
EQstCode qst_result_store_read_statements(
    SQstResultStore *store, uint32_t target,
    SQstStoredStatement **out_records, uint32_t *out_count);
void qst_result_store_free_statements(SQstStoredStatement *records,
                                      uint32_t record_count);
/* Appends one validator=ai result, using pending-results when native DML fails. */
EQstCode qst_result_store_write_manual_ai_result(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPendingStore *pending_store, const SQstManualAiResult *record,
    uint64_t record_seq, uint32_t *out_durable_source);
/* Allocates the exact business DML for one immutable task persistence object.
 * The caller owns *out_dml and must free it with free(). */
EQstCode qst_result_store_build_task_dml(SQstShmMapping *mapping,
                                         SQstResultStore *store,
                                         const SQstPersistPayload *payload,
                                         char **out_dml);
EQstCode qst_result_store_write(SQstShmMapping *mapping, SQstResultStore *store,
                                const SQstPersistPayload *payload,
                                int32_t *out_taos_code);
/* Writes a same-kind, same-target task group in one native INSERT statement.
 * The batch either reaches the result store as a whole or returns an error;
 * callers must not mark any member durable until this function succeeds. */
EQstCode qst_result_store_write_batch(
    SQstShmMapping *mapping, SQstResultStore *store,
    const SQstPersistPayload *payloads, uint32_t payload_count,
    int32_t *out_taos_code);
/* Reconnects and confirms the pre-deployed result DB is ready for idempotent
 * schema/DML work. It never starts or stops a test-machine service. */
EQstCode qst_result_store_recover(SQstShmMapping *mapping,
                                  SQstResultStore *store, uint64_t run_id,
                                  uint32_t recovery_seq);
EQstCode qst_pending_write(SQstShmMapping *mapping,
                           const SQstPendingStore *store,
                           const SQstPendingRecord *record,
                           SQstRef *out_pending_file_ref);
/* Opens the run's pending binary stream for continuous append. */
EQstCode qst_pending_batch_begin(SQstShmMapping *mapping,
                                 const SQstPendingStore *store,
                                 SQstPendingBatch *out_batch);
/* Appends one complete binary record. The caller retains ownership of record. */
EQstCode qst_pending_batch_append(SQstPendingBatch *batch,
                                  const SQstPendingRecord *record);
/* Returns nonzero once buffered bytes reach the write-coalescing threshold. */
int32_t qst_pending_batch_ready(const SQstPendingBatch *batch);
/* Flushes buffered frames to the append stream. No filesystem sync is performed. */
EQstCode qst_pending_batch_flush(SQstPendingBatch *batch);
/* Closes the append stream without deleting already written records. */
void qst_pending_batch_abort(SQstPendingBatch *batch);
EQstCode qst_pending_replay(SQstShmMapping *mapping, SQstResultStore *store,
                            uint64_t run_id, uint64_t *out_replayed_count);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RESULT_STORE_H_
