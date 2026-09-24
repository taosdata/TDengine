/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_TASK_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_TASK_H_

#include <stdatomic.h>
#include <stdint.h>

#include "qst_queue.h"

enum {
  /* A pair owns one native client per in-flight statement. The release
   * profile uses a bounded 16-pair x 128-client window. */
  QST_PAIR_MAX_INFLIGHT = 128U,
  QST_PERSIST_MASK_STATEMENT = 0x01U,
  QST_PERSIST_MASK_INITIAL_RESULT = 0x02U,
  QST_PERSIST_MASK_HISTORICAL_VERDICT = 0x04U,
  QST_PERSIST_MASK_MANUAL_AI_RESULT = 0x08U,
  QST_PERSIST_MASK_ALL = QST_PERSIST_MASK_STATEMENT |
                         QST_PERSIST_MASK_INITIAL_RESULT |
                         QST_PERSIST_MASK_HISTORICAL_VERDICT |
                         QST_PERSIST_MASK_MANUAL_AI_RESULT,
};

/* One fixed task-ledger entry. Gid zero marks an unused entry. */
typedef struct {
  uint64_t gid;
  uint32_t target;
  uint32_t mode;
  uint32_t focus;
  uint32_t dialect;
  _Atomic uint32_t phase;
  _Atomic uint64_t dispatch_epoch;
  _Atomic uint64_t transition_seq;
  _Atomic uint32_t owner_role;
  _Atomic uint32_t owner_slot_id;
  _Atomic uint32_t owner_generation;
  _Atomic uint32_t timeout_attempt;
  _Atomic uint32_t required_persist_mask;
  _Atomic uint32_t durable_persist_mask;
  _Atomic uint32_t result_storage_status;
  SQstExecutePayload final_payload;
  int64_t verdict_ts_ns;
  int32_t final_verdict;
  uint8_t generation_contains_abnormal;
  uint8_t generation_contains_unsupported_fallback;
  uint8_t reserved0[2];
  uint64_t generation_seq;
  uint64_t weight_profile_version;
  uint64_t record_seq;
  SQstRef sql_ref;
  SQstDigest256 sql_sha256;
  SQstDigest256 dataset_digest;
  SQstRef persist_manifest_ref;
  SQstRef generation_record_ref;
  SQstDigest256 generation_record_sha256;
  uint8_t reserved[8];
} SQstTaskEntry;

/* Immutable execution input copied from one pair-supervisor-owned task. */
typedef struct {
  uint64_t gid;
  uint64_t dispatch_epoch;
  uint64_t transition_seq;
  uint32_t target;
  uint32_t dialect;
  uint32_t focus;
  uint32_t reserved0;
  SQstRef sql_ref;
  SQstDigest256 sql_sha256;
} SQstTaskDispatch;

/* Immutable evidence required to persist one FINAL_PUBLISHED/PERSISTING task. */
typedef struct {
  uint64_t gid;
  uint64_t dispatch_epoch;
  uint64_t transition_seq;
  uint32_t target;
  uint32_t focus;
  uint32_t result_storage_status;
  uint32_t dialect;
  uint8_t generation_contains_abnormal;
  uint8_t generation_contains_unsupported_fallback;
  uint8_t reserved0[2];
  SQstDigest256 sql_sha256;
  SQstRef sql_ref;
  SQstExecutePayload final_payload;
} SQstTaskPersistSnapshot;

typedef struct {
  SQstDigest256 action_id;
  uint64_t gid;
  uint32_t entry;
  _Atomic uint32_t state;
  uint32_t owner_generation;
  int32_t action_code;
  int64_t created_ts_ns;
  int64_t completed_ts_ns;
  SQstDigest256 prompt_sha256;
  SQstDigest256 material_manifest_sha256;
  SQstRef material_manifest_ref;
  SQstRef analysis_ref;
  SQstRef remote_issue_ref;
  uint8_t reserved[72];
} SQstAiActionEntry;

_Static_assert(sizeof(SQstTaskEntry) % QST_CACHELINE_BYTES == 0,
               "task entries must start on cache-line boundaries");
_Static_assert(sizeof(SQstAiActionEntry) % QST_CACHELINE_BYTES == 0,
               "AI entries must start on cache-line boundaries");
_Static_assert(sizeof(SQstTaskEntry) == 384,
               "unexpected task-entry ABI size");
_Static_assert(sizeof(SQstAiActionEntry) == 256,
               "unexpected AI-action ABI size");

EQstCode qst_task_reserve_gid(SQstShmMapping *mapping, EQstRole role,
                              uint32_t generation, uint64_t *out_gid);
EQstCode qst_task_create(SQstShmMapping *mapping, uint64_t gid,
                         EQstRole role, uint32_t generation,
                         const SQstGeneratePayload *input,
                         uint64_t *out_dispatch_epoch);
/* Creates a task for a gid returned by qst_task_reserve_gid() in this
 * generator generation. The reservation is globally unique, so this path
 * retains block and digest validation while avoiding a redundant ledger-wide
 * duplicate scan. Callers must not use it for recovered or caller-supplied
 * gids. */
EQstCode qst_task_create_reserved(SQstShmMapping *mapping, uint64_t gid,
                                  EQstRole role, uint32_t generation,
                                  const SQstGeneratePayload *input,
                                  uint64_t *out_dispatch_epoch);
/* Creates a task whose immutable generation-record digest was verified while
 * the generator prepared its sealed blocks; block ownership/CRC validation is
 * still performed at commit time. */
EQstCode qst_task_create_prepared(SQstShmMapping *mapping, uint64_t gid,
                                  EQstRole role, uint32_t generation,
                                  const SQstGeneratePayload *input,
                                  uint64_t *out_dispatch_epoch);
/* Reports whether the fixed shared task ledger has one unclaimed entry. */
EQstCode qst_task_has_capacity(const SQstShmMapping *mapping,
                               int32_t *out_has_capacity);
/* Counts unclaimed entries in the fixed shared task ledger. */
EQstCode qst_task_free_count(const SQstShmMapping *mapping,
                             uint32_t *out_free_count);
/* Reports whether no task can still require a pair supervisor. Callers may
 * stop pair runtimes only after the publisher has made its terminal decision
 * and this helper reports a drained ledger. */
EQstCode qst_task_execution_inputs_drained(const SQstShmMapping *mapping,
                                           int32_t *out_drained);
/* Reports whether no generated input remains unassigned to an execution pair.
 * Executing/final/persistence phases remain assigned while they drain. */
EQstCode qst_task_execution_inputs_assigned(const SQstShmMapping *mapping,
                                            int32_t *out_assigned);
/* Reports whether one current pair has no pair-owned execution/final work
 * after the publisher's terminal decision. The exec manager must keep at
 * least one non-retiring pair while generated inputs remain unassigned. */
EQstCode qst_task_pair_retire_eligible(const SQstShmMapping *mapping,
                                       uint32_t exec_generation,
                                       uint32_t pair_slot_id,
                                       uint32_t pair_generation,
                                       int32_t allow_before_stop,
                                       int32_t *out_eligible);
/* Reverts a generated task that was never published to generate_queue. */
EQstCode qst_task_abort_generated(SQstShmMapping *mapping, uint64_t gid,
                                  EQstRole role, uint32_t generation);
EQstCode qst_task_claim_dispatch(SQstShmMapping *mapping, uint64_t gid,
                                 EQstRole role, uint32_t generation,
                                 uint64_t dispatch_epoch,
                                 const SQstGeneratePayload *expected_payload,
                                 uint64_t *out_transition_seq);
/* Claims generated work for one current pair-supervisor queue consumer. */
EQstCode qst_task_claim_dispatch_pair(
    SQstShmMapping *mapping, uint64_t gid, uint32_t pair_slot_id,
    uint32_t pair_generation, uint64_t dispatch_epoch,
    const SQstGeneratePayload *expected_payload,
    SQstTaskDispatch *out_dispatch);
/* Rebuilds the original generate payload after a manager-owned rollback. */
EQstCode qst_task_get_generate_payload(SQstShmMapping *mapping, uint64_t gid,
                                       uint64_t dispatch_epoch, EQstRole role,
                                       uint32_t generation,
                                       SQstGeneratePayload *out_payload);
/* Transfers an executing task to one current direct pair-supervisor child. */
EQstCode qst_task_assign_pair(SQstShmMapping *mapping, uint64_t gid,
                              EQstRole role, uint32_t generation,
                              uint64_t dispatch_epoch,
                              uint64_t transition_seq,
                              uint32_t pair_slot_id,
                              uint32_t pair_generation);
/* Reverts a dispatch claim before execution starts, preserving the task SQL. */
EQstCode qst_task_abort_dispatch(SQstShmMapping *mapping, uint64_t gid,
                                 uint64_t dispatch_epoch,
                                 uint64_t transition_seq, EQstRole owner_role,
                                 uint32_t owner_slot_id,
                                 uint32_t owner_generation);
/* Finds one currently executing task assigned to the calling pair. */
EQstCode qst_task_find_assigned_pair(SQstShmMapping *mapping,
                                     uint32_t pair_slot_id,
                                     uint32_t pair_generation,
                                     SQstTaskDispatch *out_dispatch);
/* Finds one assigned task that is not already owned by the pair's local worker
 * set. excluded_gids is process-local state and is never persisted. */
EQstCode qst_task_find_assigned_pair_excluding(
    SQstShmMapping *mapping, uint32_t pair_slot_id, uint32_t pair_generation,
    const uint64_t *excluded_gids, uint32_t excluded_count,
    SQstTaskDispatch *out_dispatch);
/* Checks whether one current direct pair-supervisor child has a free native
 * connection for another executing task. */
EQstCode qst_task_pair_is_idle(const SQstShmMapping *mapping,
                               uint32_t exec_generation,
                               uint32_t pair_slot_id,
                               uint32_t pair_generation,
                               int32_t *out_is_idle);
EQstCode qst_task_publish_final(SQstShmMapping *mapping, uint64_t gid,
                                uint64_t dispatch_epoch,
                                uint64_t transition_seq, EQstRole role,
                                uint32_t slot_id,
                                uint32_t generation,
                                const SQstExecutePayload *final_payload);
/* Records how the current execution pair retained this task's full result. */
EQstCode qst_task_set_result_storage(SQstShmMapping *mapping, uint64_t gid,
                                     uint64_t dispatch_epoch,
                                     uint64_t transition_seq,
                                     uint32_t pair_slot_id,
                                     uint32_t pair_generation,
                                     uint32_t result_storage_status);
/* Converts one completed timeout to a new manager-owned execution epoch.
 * QST_E_DEADLINE means the timeout budget is exhausted and leaves FINAL intact. */
EQstCode qst_task_retry_timeout(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint64_t transition_seq, uint32_t exec_generation, uint32_t retry_count,
    SQstTaskDispatch *out_dispatch, int32_t *out_recycle_required,
    uint32_t *out_previous_pair_slot, uint32_t *out_previous_pair_generation);
/* Reclaims an EXECUTING task after its owning pair has failed without a final
 * callback. The new dispatch epoch does not consume the SQL timeout budget. */
EQstCode qst_task_recover_pair_failure(
    SQstShmMapping *mapping, uint32_t exec_generation,
    uint32_t failed_pair_slot, uint32_t failed_pair_generation,
    SQstTaskDispatch *out_dispatch);
/* Copies immutable FINAL evidence for the current execution manager generation. */
EQstCode qst_task_get_final(SQstShmMapping *mapping, uint64_t gid,
                            uint64_t dispatch_epoch,
                            uint64_t transition_seq, EQstRole role,
                            uint32_t generation,
                            SQstExecutePayload *out_payload,
                            uint32_t *out_target);
/* Copies FINAL evidence owned by one current pair-supervisor instance. */
EQstCode qst_task_get_final_pair(SQstShmMapping *mapping, uint64_t gid,
                                 uint64_t dispatch_epoch,
                                 uint64_t transition_seq,
                                 uint32_t pair_slot_id,
                                 uint32_t pair_generation,
                                 SQstExecutePayload *out_payload,
                                 uint32_t *out_target);
/* Returns immutable execution evidence while the task is ready for persistence. */
EQstCode qst_task_get_persist_snapshot(const SQstShmMapping *mapping,
                                       uint64_t gid,
                                       uint64_t dispatch_epoch,
                                       uint64_t transition_seq,
                                       SQstTaskPersistSnapshot *out_snapshot);
/* Finds one FINAL or ISOLATED task that still requires execute_queue publication. */
EQstCode qst_task_find_final_for_exec(SQstShmMapping *mapping,
                                      uint32_t generation,
                                      uint64_t *out_gid,
                                      uint64_t *out_dispatch_epoch,
                                      uint64_t *out_transition_seq);
/* Finds FINAL evidence stranded by one exited pair before it reached
 * execute_queue. This recovery-only scan never selects a live pair's work. */
EQstCode qst_task_find_final_for_pair_recovery(
    SQstShmMapping *mapping, uint32_t exec_generation,
    uint32_t failed_pair_slot, uint32_t failed_pair_generation,
    uint64_t *out_gid, uint64_t *out_dispatch_epoch,
    uint64_t *out_transition_seq);
/* Moves a task from FINAL only after execute_queue accepted its final evidence. */
EQstCode qst_task_confirm_final_published(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint64_t transition_seq, EQstRole role, uint32_t generation,
    const SQstExecutePayload *expected_payload);
/* Confirms execute-queue publication by the pair that owns FINAL evidence. */
EQstCode qst_task_confirm_final_published_pair(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint64_t transition_seq, uint32_t pair_slot_id, uint32_t pair_generation,
    const SQstExecutePayload *expected_payload);
EQstCode qst_task_begin_persist(SQstShmMapping *mapping, uint64_t gid,
                                uint64_t dispatch_epoch,
                                uint64_t transition_seq, EQstRole role,
                                uint32_t slot_id, uint32_t generation,
                                int32_t final_verdict,
                                uint32_t required_persist_mask);
EQstCode qst_task_mark_durable(SQstShmMapping *mapping, uint64_t gid,
                               uint64_t dispatch_epoch, uint32_t persist_mask,
                               uint32_t durable_source, uint64_t record_seq,
                               EQstRole role, uint32_t generation);
/* Releases every sealed task block after the main process reaches PERSISTED. */
EQstCode qst_task_release_persisted_blocks(SQstShmMapping *mapping,
                                           uint64_t gid, EQstRole role,
                                           uint32_t generation);
/* Returns a block-free PERSISTED ledger slot to the fixed shared work set. */
EQstCode qst_task_reclaim_persisted(SQstShmMapping *mapping, uint64_t gid,
                                    EQstRole role, uint32_t generation);
/* The sole judge may complete an already durable batch without routing a
 * redundant confirmation through main. All record kinds in persist_mask must
 * have reached the result store or pending stream before this call. */
EQstCode qst_task_complete_persisted_by_judge(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint32_t persist_mask, uint32_t durable_source, uint64_t record_seq,
    uint32_t judge_slot, uint32_t judge_generation);
/* Records a manager-owned execution failure that cannot be safely retried.
 * The supplied payload must be an ERROR final fact for reason and has no
 * callback-owned result references. The task remains eligible for the normal
 * execute_queue and durable-statement persistence flow. */
EQstCode qst_task_isolate(SQstShmMapping *mapping, uint64_t gid,
                          EQstCode reason, EQstRole role,
                          uint32_t generation,
                          const SQstExecutePayload *final_payload);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_TASK_H_
