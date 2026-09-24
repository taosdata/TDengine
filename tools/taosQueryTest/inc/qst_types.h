/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_TYPES_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_TYPES_H_

#include <stdatomic.h>
#include <stdint.h>

#define QST_SHM_FORMAT_VERSION 18U
#define QST_ABI_ENDIAN_TAG 0x01020304U
#define QST_CACHELINE_BYTES 64U
#define QST_GENERATOR_TARGET_COUNT 4U
#define QST_GENERATION_STATE_FORMAT_VERSION 8U

typedef int32_t EQstCode;

enum {
  QST_OK = 0,
  QST_E_INVALID_ARGUMENT = -1,
  QST_E_STOPPING = -2,
  QST_E_RESOURCE = -3,
  QST_E_SHM_ABI = -4,
  QST_E_SHM_FORMAT = -5,
  QST_E_SHM_CORRUPT = -6,
  QST_E_QUEUE_FULL = -7,
  QST_E_QUEUE_EMPTY = -8,
  QST_E_QUEUE_LEASE_EXPIRED = -9,
  QST_E_STALE_OWNER = -10,
  QST_E_BLOCK_NO_SPACE = -11,
  QST_E_BLOCK_INVALID = -12,
  QST_E_TASK_STATE = -13,
  QST_E_VERSION_CAPABILITY = -14,
  QST_E_RESULT_STORE = -15,
  QST_E_PENDING_IO = -16,
  QST_E_INTERNAL = -17,
  QST_E_DEADLINE = -18,
  /* Generator-local fixed SQL buffer reached its defined capacity. */
  QST_E_SQL_LIMIT = -19,
};

typedef enum {
  QST_ROLE_LAUNCHER = 1,
  QST_ROLE_MAIN = 2,
  QST_ROLE_GENERATOR = 3,
  QST_ROLE_EXEC_MANAGER = 4,
  QST_ROLE_PAIR_SUPERVISOR = 5,
  QST_ROLE_TAOSD = 6,
  QST_ROLE_TAOSC = 7,
  QST_ROLE_JUDGE_MANAGER = 8,
  QST_ROLE_JUDGE_WORKER = 9,
  QST_ROLE_AI_WORKER = 10,
} EQstRole;

typedef enum {
  QST_TARGET_TD = 1,
  QST_TARGET_MYSQL = 2,
  QST_TARGET_PG = 3,
  QST_TARGET_INFLUX = 4,
} EQstTarget;

typedef enum {
  QST_MODE_FULL = 1,
  QST_MODE_RANDOM = 2,
} EQstMode;

typedef enum {
  QST_FOCUS_COVERAGE = 1,
  QST_FOCUS_CRASH = 2,
  QST_FOCUS_CORRECTNESS = 3,
} EQstFocus;

typedef enum {
  QST_DIALECT_TDENGINE = 1,
  QST_DIALECT_MYSQL = 2,
  QST_DIALECT_POSTGRESQL = 3,
  QST_DIALECT_INFLUXQL = 4,
} EQstDialect;

typedef enum {
  QST_STOP_NONE = 0,
  QST_STOP_FULL_COMPLETE = 1,
  QST_STOP_STATEMENT_LIMIT = 2,
  QST_STOP_DURATION_LIMIT = 3,
  QST_STOP_USER_REQUEST = 4,
  QST_STOP_PREPARE_FAILED = 5,
  QST_STOP_INFRASTRUCTURE_FAILED = 6,
  QST_STOP_SHM_CORRUPT = 7,
} EQstStopReason;

typedef enum {
  QST_PROCESS_RUNNING = 1,
  QST_PROCESS_STOP_REQUESTED = 2,
  QST_PROCESS_STOP_ACKED = 3,
  QST_PROCESS_EXITED = 4,
  QST_PROCESS_REAPED = 5,
  QST_PROCESS_KILLED = 6,
  QST_PROCESS_REGISTERING = 7,
} EQstProcessExitState;

/* A flow confirmation identifies the core-loop phase it has validated. */
typedef enum {
  QST_FLOW_STAGE_STARTUP = 1,
  QST_FLOW_STAGE_IDLE = 2,
  QST_FLOW_STAGE_DRAINING = 3,
  QST_FLOW_STAGE_STOPPING = 4,
  QST_FLOW_STAGE_RECOVERING = 5,
} EQstFlowStage;

/* The source represented by SQstProcessSlot.flow_observed_state. */
typedef enum {
  QST_FLOW_STATE_CONTROL_QUEUE = 1,
  QST_FLOW_STATE_INPUT_QUEUE = 2,
  QST_FLOW_STATE_OUTPUT_QUEUE = 3,
  QST_FLOW_STATE_DIRECT_CHILDREN = 4,
} EQstFlowStateKind;

typedef enum {
  QST_RUN_PREPARING = 1,
  QST_RUN_RUNNING = 2,
  QST_RUN_DRAINING = 3,
  QST_RUN_COMPLETED = 4,
  QST_RUN_FAILED = 5,
  QST_RUN_CANCELLED = 6,
} EQstRunState;

typedef enum {
  QST_EXEC_SUCCESS = 0,
  QST_EXEC_ERROR = 1,
  QST_EXEC_TIMEOUT = 2,
  QST_EXEC_CANCELLED = 3,
} EQstExecutionStatus;

typedef enum {
  QST_RESULT_NOT_REQUESTED = 1,
  QST_RESULT_MEMORY = 2,
  QST_RESULT_DISK = 3,
  QST_RESULT_PENDING = 4,
  QST_RESULT_UNAVAILABLE = 5,
} EQstResultStorageStatus;

typedef enum {
  QST_PERSIST_STATEMENT = 1,
  QST_PERSIST_INITIAL_RESULT = 2,
  QST_PERSIST_HISTORICAL_VERDICT = 3,
  QST_PERSIST_MANUAL_AI_RESULT = 4,
  QST_PERSIST_RUN_UPDATE = 5,
  QST_PERSIST_DATASET = 6,
} EQstPersistKind;

typedef enum {
  QST_DURABLE_RESULT_STORE = 1,
  QST_DURABLE_PENDING_FILE = 2,
} EQstDurableSource;

typedef enum {
  QST_QUEUE_CONTROL = 1,
  QST_QUEUE_GENERATE = 2,
  QST_QUEUE_EXECUTE = 3,
  QST_QUEUE_PERSIST = 4,
} EQstQueueId;

typedef enum {
  QST_SLOT_FREE = 0,
  QST_SLOT_VISIBLE = 1,
  QST_SLOT_CLAIMED = 2,
  QST_SLOT_ACKED = 3,
  QST_SLOT_ABANDONED = 4,
} EQstSlotState;

typedef enum {
  QST_TASK_GENERATED = 1,
  QST_TASK_EXECUTING = 2,
  QST_TASK_FINAL = 3,
  QST_TASK_FINAL_PUBLISHED = 4,
  QST_TASK_PERSISTING = 5,
  QST_TASK_PERSISTED = 6,
  QST_TASK_ISOLATED = 7,
  QST_TASK_REGISTERING = 8,
} EQstTaskPhase;

typedef enum {
  QST_MSG_CONTROL = 1,
  QST_MSG_GENERATE_WORK = 2,
  QST_MSG_EXECUTE_FINAL = 3,
  QST_MSG_PERSIST_CONFIRM = 4,
} EQstMessageType;

typedef enum {
  QST_OBJECT_SOURCE = 1,
  QST_OBJECT_DATABASE_OR_SCHEMA = 2,
  QST_OBJECT_TABLE_OR_MEASUREMENT = 3,
  QST_OBJECT_COLUMN_OR_TAG = 4,
} EQstObjectKind;

typedef enum {
  QST_AI_ENTRY_AUTO_VERDICT = 1,
  QST_AI_ENTRY_MANUAL_UNVERIFIED = 2,
  QST_AI_ENTRY_MANUAL_ALL = 3,
} EQstAiEntry;

typedef enum {
  QST_AI_PRECHECKED = 1,
  QST_AI_RUNNING = 2,
  QST_AI_SUCCEEDED = 3,
  QST_AI_FAILED = 4,
  QST_AI_ABANDONED = 5,
  QST_AI_REGISTERING = 6,
} EQstAiActionState;

typedef enum {
  QST_BLOCK_FREE = 0,
  QST_BLOCK_WRITING = 1,
  QST_BLOCK_SEALED = 2,
  QST_BLOCK_RECLAIMING = 3,
} EQstBlockState;

/* SQstRef uses an offset from the shared-memory mapping base. */
typedef struct {
  uint64_t offset;
  uint32_t length;
  uint32_t crc32c;
} SQstRef;

typedef struct {
  uint8_t bytes[32];
} SQstDigest256;

/*
 * Shared FULL traversal cursor for one fixed target. The decision path itself
 * is reconstructed in the generator process; it is never copied to shared
 * memory, because a valid catalog path has no fixed occurrence bound.
 */
typedef struct {
  uint64_t next_path_ordinal;
  uint32_t exhausted;
  uint32_t reserved0;
} SQstGeneratorCursor;

/* Runtime generation state; one instance occupies generation_state_region. */
typedef struct {
  uint32_t format_version;
  uint32_t catalog_version;
  SQstDigest256 catalog_digest;
  _Atomic uint64_t publish_seq;
  _Atomic uint64_t random_generation_seq;
  _Atomic uint32_t full_target_index;
  _Atomic uint32_t full_target_done_mask;
  uint32_t mode;
  uint32_t focus;
  uint32_t target_selection_bits;
  uint32_t random_builder_count;
  uint64_t random_seed;
  uint64_t statement_limit;
  uint64_t duration_limit_minutes;
  _Atomic uint64_t random_activated_count;
  _Atomic uint64_t random_duration_deadline_mono_ns;
  /* Completed FULL traversal rounds, including the terminal round. */
  _Atomic uint64_t full_coverage_round_count;
  /* Generator-owned, durable progress through the verified FULL seed stratum.
   * A seed must be committed before the unrestricted DFS begins for its target. */
  _Atomic uint32_t full_coverage_seed_next[QST_GENERATOR_TARGET_COUNT];
  SQstDigest256 dataset_digest;
  SQstDigest256 fixture_manifest_digest;
  SQstRef object_directory_ref;
  SQstRef fixture_manifest_ref;
  _Atomic uint32_t dataset_state;
  uint32_t reserved1;
  SQstGeneratorCursor full_cursor[QST_GENERATOR_TARGET_COUNT];
  SQstDigest256 dataset_snapshot_sha256;
  /* Frozen launcher capacities shared with every pending/log writer. */
  uint64_t pending_quota_bytes;
  uint64_t log_rotation_bytes;
  /* UINT64_MAX means the local pending directory has not been counted yet. */
  _Atomic uint64_t pending_used_bytes;
  uint8_t reserved2[80];
} SQstGenerationState;

static inline int32_t qstRefIsEmpty(SQstRef ref) {
  return ref.offset == 0 && ref.length == 0 && ref.crc32c == 0;
}

_Static_assert(sizeof(uint32_t) == 4, "qst requires 32-bit uint32_t");
_Static_assert(sizeof(uint64_t) == 8, "qst requires 64-bit uint64_t");
_Static_assert(sizeof(int64_t) == 8, "qst requires 64-bit int64_t");

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_TYPES_H_
