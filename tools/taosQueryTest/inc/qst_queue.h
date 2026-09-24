/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_QUEUE_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_QUEUE_H_

#include <stdatomic.h>
#include <stdint.h>

#include "qst_shm.h"

typedef enum {
  QST_CONTROL_START = 1,
  QST_CONTROL_STOP_REQUEST = 2,
  QST_CONTROL_STOP_ACK = 3,
  QST_CONTROL_FREEZE = 4,
  QST_CONTROL_RESTART = 5,
  QST_CONTROL_FATAL = 6,
} EQstControlOpcode;

typedef enum {
  QST_ACK_CONTROL_RESPONSE = 1,
  QST_ACK_TASK_DISPATCHED = 2,
  QST_ACK_PERSIST_WORK_ACCEPTED = 3,
  QST_ACK_DURABILITY_APPLIED = 4,
} EQstAckProofKind;

/* reserved0 marks one persist confirmation covering every record required by
 * the task. The individual records have already reached the durable store. */
#define QST_PERSIST_CONFIRM_BATCH UINT32_C(0x01)

enum {
  QST_STAT_DELTA_GENERATED = UINT64_C(1) << 0,
  QST_STAT_DELTA_EXEC_SUCCESS = UINT64_C(1) << 1,
  QST_STAT_DELTA_EXEC_ERROR = UINT64_C(1) << 2,
  QST_STAT_DELTA_EXEC_TIMEOUT = UINT64_C(1) << 3,
  QST_STAT_DELTA_EXEC_CANCELLED = UINT64_C(1) << 4,
  QST_STAT_DELTA_VERDICT_PASS = UINT64_C(1) << 5,
  QST_STAT_DELTA_VERDICT_FAIL = UINT64_C(1) << 6,
  QST_STAT_DELTA_VERDICT_UNVERIFIED = UINT64_C(1) << 7,
  QST_STAT_DELTA_TASK_ISOLATED = UINT64_C(1) << 8,
  QST_STAT_DELTA_PENDING_WRITTEN = UINT64_C(1) << 9,
  QST_STAT_DELTA_PENDING_REPLAYED = UINT64_C(1) << 10,
};

typedef struct {
  uint32_t opcode;
  int32_t reason_code;
  uint32_t target_role;
  uint32_t target_slot_id;
  uint32_t target_generation;
  uint64_t requested_stop_generation;
  uint64_t related_gid;
  SQstRef diagnostic_ref;
} SQstControlPayload;

typedef struct {
  uint32_t target;
  uint32_t mode;
  uint32_t focus;
  uint32_t dialect;
  uint8_t generation_contains_abnormal;
  uint8_t generation_contains_unsupported_fallback;
  uint8_t reserved0[2];
  uint64_t generation_seq;
  uint64_t weight_profile_version;
  SQstDigest256 sql_sha256;
  SQstDigest256 dataset_digest;
  SQstRef sql_ref;
  SQstRef generation_record_ref;
  SQstDigest256 generation_record_sha256;
} SQstGeneratePayload;

typedef struct {
  int64_t statement_ts_ns;
  int64_t ended_ts_ns;
  uint64_t sid;
  uint32_t execution_status;
  int32_t execution_error_code;
  int64_t result_row_count;
  int32_t taosd_pid;
  int32_t taosc_pid;
  SQstRef result_ref;
  SQstRef error_ref;
  SQstRef log_range_ref;
  SQstRef core_path_ref;
} SQstExecutePayload;

typedef struct {
  uint32_t persist_kind;
  uint32_t durable_source;
  uint32_t target;
  uint32_t reserved0;
  uint64_t gid;
  uint64_t dispatch_epoch;
  uint64_t transition_seq;
  uint64_t record_seq;
  uint64_t stats_delta_mask;
  SQstDigest256 payload_sha256;
  SQstRef stable_key_ref;
  SQstRef payload_ref;
  SQstRef pending_file_ref;
} SQstPersistPayload;

typedef struct {
  uint64_t message_id;
  uint32_t message_type;
  uint32_t producer_role;
  uint32_t producer_slot_id;
  uint32_t producer_generation;
  uint32_t target;
  uint64_t run_id;
  uint64_t gid;
  uint64_t dispatch_epoch;
  uint64_t transition_seq;
  union {
    SQstControlPayload control;
    SQstGeneratePayload generate;
    SQstExecutePayload execute;
    SQstPersistPayload persist;
  } body;
  uint32_t message_crc32c;
  uint32_t reserved;
} SQstMessage;

typedef struct {
  _Atomic uint64_t sequence;
  _Atomic uint32_t state;
  _Atomic uint32_t owner_role;
  _Atomic uint32_t owner_slot_id;
  _Atomic uint32_t owner_generation;
  _Atomic uint32_t claim_generation;
  _Atomic uint64_t claim_seq;
  _Atomic uint64_t lease_deadline_mono_ns;
  SQstMessage message;
  uint8_t reserved[92];
} SQstQueueSlot;

_Static_assert(sizeof(SQstQueueSlot) % QST_CACHELINE_BYTES == 0,
               "queue slots must start on cache-line boundaries");
_Static_assert(sizeof(SQstQueueSlot) == 384,
               "unexpected queue-slot ABI size");

typedef struct {
  uint32_t queue_id;
  uint32_t slot_bytes;
  uint64_t slot_offset;
  uint64_t capacity;
  _Atomic uint64_t enqueue_pos;
  _Atomic uint64_t dequeue_pos;
  _Atomic uint64_t progress_epoch;
  _Atomic uint32_t futex_seq;
  uint32_t eventfd_owner_role;
} SQstQueueDescriptor;

typedef struct SQstClaim {
  uint32_t queue_id;
  uint32_t slot_index;
  uint32_t owner_role;
  uint32_t owner_slot_id;
  uint32_t owner_generation;
  uint64_t queue_position;
  uint64_t claim_seq;
  uint64_t lease_deadline_mono_ns;
  SQstMessage message;
} SQstClaim;

/* A coherent read-only queue observation for private supervision state. */
typedef struct {
  uint64_t enqueue_pos;
  uint64_t dequeue_pos;
  uint64_t progress_epoch;
} SQstQueueSnapshot;

EQstCode qst_queue_publish(SQstShmMapping *mapping, EQstQueueId queue_id,
                           const SQstMessage *message,
                           uint64_t deadline_mono_ns);
EQstCode qst_queue_claim(SQstShmMapping *mapping, EQstQueueId queue_id,
                         EQstRole role, uint32_t generation,
                         uint64_t deadline_mono_ns, SQstClaim *out_claim);
EQstCode qst_queue_claim_instance(SQstShmMapping *mapping,
                                  EQstQueueId queue_id, EQstRole role,
                                  uint32_t slot_id, uint32_t generation,
                                  uint64_t deadline_mono_ns,
                                  SQstClaim *out_claim);
EQstCode qst_queue_ack(SQstShmMapping *mapping, const SQstClaim *claim,
                       EQstAckProofKind proof_kind, uint64_t proof_id);
EQstCode qst_queue_abandon(SQstShmMapping *mapping, const SQstClaim *claim,
                           EQstCode reason);
EQstCode qst_queue_reclaim_claim(SQstShmMapping *mapping,
                                 const SQstClaim *claim,
                                 EQstRole parent_role,
                                 uint32_t parent_slot_id,
                                 uint32_t parent_generation,
                                 EQstCode reason);
EQstCode qst_queue_requeue_abandoned(SQstShmMapping *mapping,
                                     EQstQueueId queue_id,
                                     uint64_t message_id, EQstRole role,
                                     uint32_t generation,
                                     uint64_t deadline_mono_ns);
/* Reads queue state without changing queue ownership or progress counters. */
EQstCode qst_queue_snapshot(const SQstShmMapping *mapping,
                            EQstQueueId queue_id,
                            SQstQueueSnapshot *out_snapshot);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_QUEUE_H_
