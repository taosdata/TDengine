/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#include "qst_task.h"

#include <sched.h>
#include <stddef.h>
#include <string.h>

#include "qst_block.h"
#include "qst_process.h"

/* Cache only the process-slot location. Every call still validates the slot's
 * role, slot id, generation, and exit state so stop and slot reuse remain
 * visible to the caller. */
typedef struct {
  const SQstShmMapping *mapping;
  uint32_t role;
  uint32_t slot_id;
  uint32_t generation;
  uint32_t slot_index;
  uint32_t valid;
} SQstTaskCallerCache;

static _Thread_local SQstTaskCallerCache gQstTaskCallerCache;

/* The persistence path resolves the same live gid several times in one
 * worker. This cache records only its physical ledger slot; phase and gid are
 * revalidated on every hit, so it never becomes a process-local source of
 * task state and slot reclamation remains visible. */
typedef struct {
  const SQstShmMapping *mapping;
  uint64_t run_id;
  uint64_t gid;
  uint32_t slot_index;
  uint32_t valid;
} SQstTaskLookupCache;

static _Thread_local SQstTaskLookupCache gQstTaskLookupCache;

static int32_t qstIsTarget(uint32_t target) {
  return target >= QST_TARGET_TD && target <= QST_TARGET_INFLUX;
}

static int32_t qstRefEqual(SQstRef left, SQstRef right) {
  return left.offset == right.offset && left.length == right.length &&
         left.crc32c == right.crc32c;
}

static int32_t qstExecutePayloadEqual(const SQstExecutePayload *left,
                                      const SQstExecutePayload *right) {
  return NULL != left && NULL != right &&
         left->statement_ts_ns == right->statement_ts_ns &&
         left->ended_ts_ns == right->ended_ts_ns && left->sid == right->sid &&
         left->execution_status == right->execution_status &&
         left->execution_error_code == right->execution_error_code &&
         left->result_row_count == right->result_row_count &&
         left->taosd_pid == right->taosd_pid &&
         left->taosc_pid == right->taosc_pid &&
         qstRefEqual(left->result_ref, right->result_ref) &&
         qstRefEqual(left->error_ref, right->error_ref) &&
         qstRefEqual(left->log_range_ref, right->log_range_ref) &&
         qstRefEqual(left->core_path_ref, right->core_path_ref);
}

static int32_t qstGeneratePayloadMatchesTask(const SQstTaskEntry *entry,
                                              const SQstGeneratePayload *payload) {
  return NULL != entry && NULL != payload &&
         entry->target == payload->target && entry->mode == payload->mode &&
         entry->focus == payload->focus && entry->dialect == payload->dialect &&
         entry->generation_contains_abnormal ==
             payload->generation_contains_abnormal &&
         entry->generation_contains_unsupported_fallback ==
             payload->generation_contains_unsupported_fallback &&
         entry->generation_seq == payload->generation_seq &&
         entry->weight_profile_version == payload->weight_profile_version &&
         qstRefEqual(entry->sql_ref, payload->sql_ref) &&
         qstRefEqual(entry->generation_record_ref,
                     payload->generation_record_ref) &&
         memcmp(entry->sql_sha256.bytes, payload->sql_sha256.bytes,
                sizeof(entry->sql_sha256.bytes)) == 0 &&
         memcmp(entry->dataset_digest.bytes, payload->dataset_digest.bytes,
                sizeof(entry->dataset_digest.bytes)) == 0 &&
         memcmp(entry->generation_record_sha256.bytes,
                payload->generation_record_sha256.bytes,
         sizeof(entry->generation_record_sha256.bytes)) == 0;
}

/* Moves immutable terminal facts into shared cumulative counters before reuse. */
static void qstTaskRecordPersistedStats(SQstShmMapping *mapping,
                                        const SQstTaskEntry *entry) {
  if (NULL == mapping || NULL == mapping->header || NULL == entry) {
    return;
  }
  atomic_fetch_add_explicit(&mapping->header->persisted_task_count, 1,
                            memory_order_release);
  switch (entry->final_payload.execution_status) {
    case QST_EXEC_SUCCESS:
      atomic_fetch_add_explicit(&mapping->header->execution_success_count, 1,
                                memory_order_release);
      break;
    case QST_EXEC_ERROR:
      atomic_fetch_add_explicit(&mapping->header->execution_error_count, 1,
                                memory_order_release);
      break;
    case QST_EXEC_TIMEOUT:
      atomic_fetch_add_explicit(&mapping->header->execution_timeout_count, 1,
                                memory_order_release);
      break;
    case QST_EXEC_CANCELLED:
      atomic_fetch_add_explicit(&mapping->header->execution_cancelled_count, 1,
                                memory_order_release);
      break;
    default:
      break;
  }
  if (atomic_load_explicit(&entry->result_storage_status,
                           memory_order_acquire) == QST_RESULT_MEMORY) {
    atomic_fetch_add_explicit(&mapping->header->result_memory_count, 1,
                              memory_order_release);
  } else if (atomic_load_explicit(&entry->result_storage_status,
                                  memory_order_acquire) ==
             QST_RESULT_UNAVAILABLE) {
    atomic_fetch_add_explicit(&mapping->header->result_unavailable_count, 1,
                              memory_order_release);
  }
  if ((atomic_load_explicit(&entry->required_persist_mask,
                            memory_order_acquire) &
       QST_PERSIST_MASK_INITIAL_RESULT) == 0) {
    return;
  }
  switch (entry->final_verdict) {
    case 0:
      atomic_fetch_add_explicit(&mapping->header->verdict_passed_count, 1,
                                memory_order_release);
      break;
    case 1:
      atomic_fetch_add_explicit(&mapping->header->verdict_failed_count, 1,
                                memory_order_release);
      break;
    case 2:
      atomic_fetch_add_explicit(&mapping->header->verdict_unverified_count, 1,
                                memory_order_release);
      break;
    default:
      break;
  }
}

static EQstCode qstGetTaskRegion(const SQstShmMapping *mapping,
                                 SQstTaskEntry **out_entries,
                                 uint32_t *out_count) {
  const SQstRegion *region = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == out_entries || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->task_region;
  if (region->element_bytes != sizeof(SQstTaskEntry) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  *out_entries = (SQstTaskEntry *)((uint8_t *)mapping->base +
                                   region->region_offset);
  *out_count = region->element_count;
  return QST_OK;
}

static int32_t qstTaskCallerIsCurrent(const SQstShmMapping *mapping,
                                      EQstRole role, uint32_t slot_id,
                                      uint32_t generation) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      generation == 0) {
    return 0;
  }
  if (role == QST_ROLE_LAUNCHER) {
    return generation == mapping->header->launcher_generation;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return 0;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  if (gQstTaskCallerCache.valid != 0 &&
      gQstTaskCallerCache.mapping == mapping &&
      gQstTaskCallerCache.role == (uint32_t)role &&
      gQstTaskCallerCache.slot_id == slot_id &&
      gQstTaskCallerCache.generation == generation &&
      gQstTaskCallerCache.slot_index < region->element_count) {
    SQstProcessSlot *cached = &slots[gQstTaskCallerCache.slot_index];
    const uint32_t exit_state =
        atomic_load_explicit(&cached->exit_state, memory_order_acquire);

    if (atomic_load_explicit(&cached->role, memory_order_acquire) == role &&
        cached->slot_id == slot_id &&
        atomic_load_explicit(&cached->generation, memory_order_acquire) ==
            generation &&
        (exit_state == QST_PROCESS_RUNNING ||
         (role == QST_ROLE_MAIN && slot_id == 0 &&
          exit_state == QST_PROCESS_STOP_REQUESTED))) {
      return 1;
    }
    gQstTaskCallerCache.valid = 0;
  }
  for (uint32_t index = 0; index < region->element_count; ++index) {
    const uint32_t exit_state =
        atomic_load_explicit(&slots[index].exit_state, memory_order_acquire);

    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) == role &&
        slots[index].slot_id == slot_id &&
        atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
            generation &&
        (exit_state == QST_PROCESS_RUNNING ||
        (role == QST_ROLE_MAIN && slot_id == 0 &&
         exit_state == QST_PROCESS_STOP_REQUESTED))) {
      gQstTaskCallerCache.mapping = mapping;
      gQstTaskCallerCache.role = (uint32_t)role;
      gQstTaskCallerCache.slot_id = slot_id;
      gQstTaskCallerCache.generation = generation;
      gQstTaskCallerCache.slot_index = index;
      gQstTaskCallerCache.valid = 1;
      /* Main remains the durability owner while its parent waits for its
       * ordered cancellation drain. No child role may mutate work after stop. */
      return 1;
    }
  }
  return 0;
}

static int32_t qstPairSupervisorBelongsTo(const SQstShmMapping *mapping,
                                          uint32_t slot_id, uint32_t generation,
                                          uint32_t exec_generation) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header) {
    return 0;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return 0;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) ==
            QST_ROLE_PAIR_SUPERVISOR &&
        slots[index].slot_id == slot_id &&
        atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
            generation &&
        (atomic_load_explicit(&slots[index].exit_state, memory_order_acquire) ==
             QST_PROCESS_RUNNING ||
         atomic_load_explicit(&slots[index].exit_state, memory_order_acquire) ==
             QST_PROCESS_EXITED) &&
        slots[index].parent_role == QST_ROLE_EXEC_MANAGER &&
        slots[index].parent_generation == exec_generation) {
      return 1;
    }
  }
  return 0;
}

static EQstCode qstFindTask(const SQstShmMapping *mapping, uint64_t gid,
                            SQstTaskEntry **out_entry) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = qstGetTaskRegion(mapping, &entries, &count);

  if (code != QST_OK || gid == 0 || NULL == out_entry) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (gQstTaskLookupCache.valid != 0 &&
      gQstTaskLookupCache.mapping == mapping &&
      gQstTaskLookupCache.run_id == mapping->header->run_id &&
      gQstTaskLookupCache.gid == gid &&
      gQstTaskLookupCache.slot_index < count) {
    SQstTaskEntry *cached = &entries[gQstTaskLookupCache.slot_index];
    const uint32_t phase = atomic_load_explicit(&cached->phase,
                                                memory_order_acquire);

    if (phase != 0 && phase != QST_TASK_REGISTERING && cached->gid == gid) {
      *out_entry = cached;
      return QST_OK;
    }
    gQstTaskLookupCache.valid = 0;
  }
  {
    const uint32_t primary_index = (uint32_t)(gid % count);
    const uint32_t phase = atomic_load_explicit(&entries[primary_index].phase,
                                                memory_order_acquire);

    if (phase != 0 && phase != QST_TASK_REGISTERING &&
        entries[primary_index].gid == gid) {
      gQstTaskLookupCache.mapping = mapping;
      gQstTaskLookupCache.run_id = mapping->header->run_id;
      gQstTaskLookupCache.gid = gid;
      gQstTaskLookupCache.slot_index = primary_index;
      gQstTaskLookupCache.valid = 1;
      *out_entry = &entries[primary_index];
      return QST_OK;
    }
  }
  /* A consumed gid can leave its modulo slot occupied by older live work.
   * Allocation then falls back to another free slot, and lookup must retain
   * this bounded recovery path rather than treating the modulo rule as an
   * ownership invariant. */
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t phase = atomic_load_explicit(&entries[index].phase,
                                                memory_order_acquire);
    if (phase != 0 && phase != QST_TASK_REGISTERING &&
        entries[index].gid == gid) {
      gQstTaskLookupCache.mapping = mapping;
      gQstTaskLookupCache.run_id = mapping->header->run_id;
      gQstTaskLookupCache.gid = gid;
      gQstTaskLookupCache.slot_index = index;
      gQstTaskLookupCache.valid = 1;
      *out_entry = &entries[index];
      return QST_OK;
    }
  }
  return QST_E_TASK_STATE;
}

EQstCode qst_task_reserve_gid(SQstShmMapping *mapping, EQstRole role,
                              uint32_t generation, uint64_t *out_gid) {
  uint64_t gid = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == out_gid ||
      role != QST_ROLE_GENERATOR || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  gid = atomic_fetch_add_explicit(&mapping->header->next_gid, 1,
                                  memory_order_relaxed);
  if (gid == 0) {
    gid = atomic_fetch_add_explicit(&mapping->header->next_gid, 1,
                                    memory_order_relaxed);
  }
  if (gid == 0) {
    return QST_E_RESOURCE;
  }
  *out_gid = gid;
  return QST_OK;
}

static EQstCode qstTaskCreateInternal(SQstShmMapping *mapping, uint64_t gid,
                                       EQstRole role, uint32_t generation,
                                       const SQstGeneratePayload *input,
                                       uint64_t *out_dispatch_epoch,
                                       int32_t verify_generation_record_digest,
                                       int32_t validate_input_refs,
                                       int32_t skip_duplicate_scan) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  uint32_t start_index = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == input || NULL == out_dispatch_epoch ||
      gid == 0 || role != QST_ROLE_GENERATOR || generation == 0 ||
      !qstIsTarget(input->target) ||
      input->mode < QST_MODE_FULL || input->mode > QST_MODE_RANDOM ||
      input->focus < QST_FOCUS_COVERAGE || input->focus > QST_FOCUS_CORRECTNESS ||
      input->dialect < QST_DIALECT_TDENGINE ||
      input->dialect > QST_DIALECT_INFLUXQL || qstRefIsEmpty(input->sql_ref) ||
      input->generation_contains_abnormal > 1 ||
      input->generation_contains_unsupported_fallback > 1 ||
      qstRefIsEmpty(input->generation_record_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (validate_input_refs != 0) {
    if (qst_block_validate(mapping, input->sql_ref, gid) != QST_OK ||
        qst_block_validate(mapping, input->generation_record_ref, gid) !=
            QST_OK) {
      return QST_E_BLOCK_INVALID;
    }
  }
  if (verify_generation_record_digest != 0) {
    SQstDigest256 calculated = {0};

    if (qst_block_sha256(mapping, input->generation_record_ref, gid,
                         &calculated) != QST_OK ||
        memcmp(calculated.bytes, input->generation_record_sha256.bytes,
               sizeof(calculated.bytes)) != 0) {
      return QST_E_BLOCK_INVALID;
    }
  }
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  if (skip_duplicate_scan == 0) {
    SQstTaskEntry *existing = NULL;
    code = qstFindTask(mapping, gid, &existing);
    if (code == QST_OK) {
      return QST_E_TASK_STATE;
    }
    if (code != QST_E_TASK_STATE) {
      return code;
    }
  }

  if (count == 0) {
    return QST_E_RESOURCE;
  }
  start_index = skip_duplicate_scan != 0 ?
                    (uint32_t)(gid % count) : 0;
  for (uint32_t visited = 0; visited < count; ++visited) {
    const uint32_t index = (start_index + visited) % count;
    uint32_t expected_phase = 0;
    if (!atomic_compare_exchange_strong_explicit(
            &entries[index].phase, &expected_phase, QST_TASK_REGISTERING,
            memory_order_acq_rel, memory_order_acquire)) {
      continue;
    }
    (void)atomic_fetch_sub_explicit(&mapping->header->free_task_count, 1,
                                    memory_order_acq_rel);
    entries[index].gid = gid;
    entries[index].target = input->target;
    entries[index].mode = input->mode;
    entries[index].focus = input->focus;
    entries[index].dialect = input->dialect;
    entries[index].generation_contains_abnormal =
        input->generation_contains_abnormal;
    entries[index].generation_contains_unsupported_fallback =
        input->generation_contains_unsupported_fallback;
    entries[index].generation_seq = input->generation_seq;
    entries[index].weight_profile_version = input->weight_profile_version;
    entries[index].sql_ref = input->sql_ref;
    entries[index].sql_sha256 = input->sql_sha256;
    entries[index].dataset_digest = input->dataset_digest;
    entries[index].generation_record_ref = input->generation_record_ref;
    entries[index].generation_record_sha256 = input->generation_record_sha256;
    entries[index].final_verdict = -1;
    atomic_store_explicit(&entries[index].owner_role, 0, memory_order_relaxed);
    atomic_store_explicit(&entries[index].owner_slot_id, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].owner_generation, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].timeout_attempt, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].required_persist_mask, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].durable_persist_mask, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].result_storage_status,
                          QST_RESULT_NOT_REQUESTED,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].dispatch_epoch, 1,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].transition_seq, 1,
                          memory_order_relaxed);
    atomic_store_explicit(&entries[index].phase, QST_TASK_GENERATED,
                          memory_order_release);
    *out_dispatch_epoch = 1;
    return QST_OK;
  }
  return QST_E_RESOURCE;
}

EQstCode qst_task_create(SQstShmMapping *mapping, uint64_t gid,
                         EQstRole role, uint32_t generation,
                         const SQstGeneratePayload *input,
                         uint64_t *out_dispatch_epoch) {
  return qstTaskCreateInternal(mapping, gid, role, generation, input,
                                out_dispatch_epoch, 1, 1, 0);
}

EQstCode qst_task_create_reserved(SQstShmMapping *mapping, uint64_t gid,
                                  EQstRole role, uint32_t generation,
                                  const SQstGeneratePayload *input,
                                  uint64_t *out_dispatch_epoch) {
  return qstTaskCreateInternal(mapping, gid, role, generation, input,
                                out_dispatch_epoch, 1, 1, 1);
}

EQstCode qst_task_create_prepared(SQstShmMapping *mapping, uint64_t gid,
                                  EQstRole role, uint32_t generation,
                                  const SQstGeneratePayload *input,
                                  uint64_t *out_dispatch_epoch) {
  return qstTaskCreateInternal(mapping, gid, role, generation, input,
                                out_dispatch_epoch, 0, 0, 1);
}

EQstCode qst_task_has_capacity(const SQstShmMapping *mapping,
                               int32_t *out_has_capacity) {
  const SQstRegion *region = NULL;
  uint32_t free_count = 0;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_has_capacity) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_has_capacity = 0;
  region = &mapping->header->task_region;
  if (region->element_bytes != sizeof(SQstTaskEntry) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  free_count = atomic_load_explicit(&mapping->header->free_task_count,
                                    memory_order_acquire);
  if (free_count > region->element_count) {
    return QST_E_SHM_CORRUPT;
  }
  *out_has_capacity = free_count != 0;
  return QST_OK;
}

EQstCode qst_task_free_count(const SQstShmMapping *mapping,
                             uint32_t *out_free_count) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == out_free_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_free_count = 0;
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  *out_free_count = atomic_load_explicit(&mapping->header->free_task_count,
                                         memory_order_acquire);
  if (*out_free_count > count) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

EQstCode qst_task_execution_inputs_drained(const SQstShmMapping *mapping,
                                           int32_t *out_drained) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == out_drained) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_drained = 0;
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t phase = atomic_load_explicit(&entries[index].phase,
                                                memory_order_acquire);

    switch (phase) {
      case 0:
      case QST_TASK_FINAL_PUBLISHED:
      case QST_TASK_PERSISTING:
      case QST_TASK_PERSISTED:
        break;
      case QST_TASK_GENERATED:
      case QST_TASK_EXECUTING:
      case QST_TASK_FINAL:
      case QST_TASK_ISOLATED:
      case QST_TASK_REGISTERING:
        return QST_OK;
      default:
        return QST_E_SHM_CORRUPT;
    }
  }
  *out_drained = 1;
  return QST_OK;
}

EQstCode qst_task_execution_inputs_assigned(const SQstShmMapping *mapping,
                                            int32_t *out_assigned) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == out_assigned) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_assigned = 0;
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  *out_assigned = 1;
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t phase = atomic_load_explicit(&entries[index].phase,
                                                memory_order_acquire);

    switch (phase) {
      case 0:
      case QST_TASK_EXECUTING:
      case QST_TASK_FINAL:
      case QST_TASK_FINAL_PUBLISHED:
      case QST_TASK_PERSISTING:
      case QST_TASK_PERSISTED:
        break;
      case QST_TASK_GENERATED:
      case QST_TASK_ISOLATED:
      case QST_TASK_REGISTERING:
        *out_assigned = 0;
        break;
      default:
        return QST_E_SHM_CORRUPT;
    }
  }
  return QST_OK;
}

EQstCode qst_task_pair_retire_eligible(const SQstShmMapping *mapping,
                                       uint32_t exec_generation,
                                       uint32_t pair_slot_id,
                                       uint32_t pair_generation,
                                       int32_t allow_before_stop,
                                       int32_t *out_eligible) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == out_eligible ||
      exec_generation == 0 || pair_generation == 0 ||
      (allow_before_stop != 0 && allow_before_stop != 1)) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_eligible = 0;
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_EXEC_MANAGER, 0,
                              exec_generation) ||
      !qstPairSupervisorBelongsTo(mapping, pair_slot_id, pair_generation,
                                  exec_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (allow_before_stop == 0 &&
      atomic_load_explicit(&mapping->header->stop_reason,
                           memory_order_acquire) == QST_STOP_NONE) {
    return QST_OK;
  }
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t phase = atomic_load_explicit(&entries[index].phase,
                                                memory_order_acquire);

    switch (phase) {
      case 0:
      case QST_TASK_FINAL_PUBLISHED:
      case QST_TASK_PERSISTING:
      case QST_TASK_PERSISTED:
        break;
      case QST_TASK_REGISTERING:
      case QST_TASK_ISOLATED:
        return QST_OK;
      case QST_TASK_GENERATED:
        /* Once the stop reason is published, the remaining generated tasks
         * can be assigned to another non-retiring pair. The exec manager
         * excludes this pair from future dispatches. */
        break;
      case QST_TASK_EXECUTING:
        if (atomic_load_explicit(&entries[index].owner_role,
                                 memory_order_acquire) ==
            QST_ROLE_EXEC_MANAGER) {
          return QST_OK;
        }
        if (atomic_load_explicit(&entries[index].owner_role,
                                 memory_order_acquire) !=
            QST_ROLE_PAIR_SUPERVISOR) {
          /* The phase and owner fields are published separately. A transient
           * owner value during a task transition means this pair is not yet
           * safe to retire, rather than indicating corrupted shared memory. */
          return QST_OK;
        }
        if (atomic_load_explicit(&entries[index].owner_slot_id,
                                 memory_order_acquire) == pair_slot_id &&
            atomic_load_explicit(&entries[index].owner_generation,
                                 memory_order_acquire) == pair_generation) {
          return QST_OK;
        }
        break;
      case QST_TASK_FINAL:
        if (atomic_load_explicit(&entries[index].owner_role,
                                 memory_order_acquire) ==
            QST_ROLE_EXEC_MANAGER) {
          /* A timeout/recovery transition may temporarily retain terminal
           * evidence under the exec manager. It is live work, not corruption,
           * and the pair must remain available until the manager forwards it. */
          return QST_OK;
        }
        if (atomic_load_explicit(&entries[index].owner_role,
                                 memory_order_acquire) !=
            QST_ROLE_PAIR_SUPERVISOR) {
          /* See the EXECUTING case above: keep the pair alive across the
           * phase/owner publication window and retry retirement later. */
          return QST_OK;
        }
        if (atomic_load_explicit(&entries[index].owner_slot_id,
                                 memory_order_acquire) == pair_slot_id &&
            atomic_load_explicit(&entries[index].owner_generation,
                                 memory_order_acquire) == pair_generation) {
          return QST_OK;
        }
        break;
      default:
        return QST_E_SHM_CORRUPT;
    }
  }
  *out_eligible = 1;
  return QST_OK;
}

EQstCode qst_task_abort_generated(SQstShmMapping *mapping, uint64_t gid,
                                  EQstRole role, uint32_t generation) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_GENERATED;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || role != QST_ROLE_GENERATOR || generation == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_REGISTERING,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  entry->gid = 0;
  entry->target = 0;
  entry->mode = 0;
  entry->focus = 0;
  entry->dialect = 0;
  memset(&entry->final_payload, 0, sizeof(entry->final_payload));
  entry->verdict_ts_ns = 0;
  entry->final_verdict = -1;
  entry->generation_contains_abnormal = 0;
  entry->generation_contains_unsupported_fallback = 0;
  memset(entry->reserved0, 0, sizeof(entry->reserved0));
  entry->generation_seq = 0;
  entry->weight_profile_version = 0;
  entry->record_seq = 0;
  memset(&entry->sql_ref, 0, sizeof(entry->sql_ref));
  memset(&entry->sql_sha256, 0, sizeof(entry->sql_sha256));
  memset(&entry->dataset_digest, 0, sizeof(entry->dataset_digest));
  memset(&entry->persist_manifest_ref, 0, sizeof(entry->persist_manifest_ref));
  memset(&entry->generation_record_ref, 0, sizeof(entry->generation_record_ref));
  memset(&entry->generation_record_sha256, 0,
         sizeof(entry->generation_record_sha256));
  atomic_store_explicit(&entry->dispatch_epoch, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->transition_seq, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_role, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->timeout_attempt, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->required_persist_mask, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->durable_persist_mask, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->result_storage_status, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->phase, 0, memory_order_release);
  (void)atomic_fetch_add_explicit(&mapping->header->free_task_count, 1,
                                  memory_order_release);
  return QST_OK;
}

EQstCode qst_task_claim_dispatch(SQstShmMapping *mapping, uint64_t gid,
                                 EQstRole role, uint32_t generation,
                                 uint64_t dispatch_epoch,
                                 const SQstGeneratePayload *expected_payload,
                                 uint64_t *out_transition_seq) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_GENERATED;
  uint64_t transition_seq = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == expected_payload || NULL == out_transition_seq ||
      role != QST_ROLE_EXEC_MANAGER || generation == 0 || dispatch_epoch == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
      dispatch_epoch) {
    return QST_E_STALE_OWNER;
  }
  if (!qstGeneratePayloadMatchesTask(entry, expected_payload)) {
    return QST_E_SHM_CORRUPT;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_EXECUTING,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  transition_seq = atomic_fetch_add_explicit(&entry->transition_seq, 1,
                                             memory_order_relaxed) + 1;
  atomic_store_explicit(&entry->owner_role, role, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, generation,
                        memory_order_release);
  *out_transition_seq = transition_seq;
  return QST_OK;
}

EQstCode qst_task_claim_dispatch_pair(
    SQstShmMapping *mapping, uint64_t gid, uint32_t pair_slot_id,
    uint32_t pair_generation, uint64_t dispatch_epoch,
    const SQstGeneratePayload *expected_payload,
    SQstTaskDispatch *out_dispatch) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_GENERATED;
  uint64_t transition_seq = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == expected_payload || NULL == out_dispatch ||
      pair_generation == 0 || dispatch_epoch == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_PAIR_SUPERVISOR,
                              pair_slot_id, pair_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
      dispatch_epoch) {
    return QST_E_STALE_OWNER;
  }
  if (!qstGeneratePayloadMatchesTask(entry, expected_payload)) {
    return QST_E_SHM_CORRUPT;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_EXECUTING,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  transition_seq = atomic_fetch_add_explicit(&entry->transition_seq, 1,
                                             memory_order_relaxed) + 1;
  atomic_store_explicit(&entry->owner_role, QST_ROLE_PAIR_SUPERVISOR,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, pair_slot_id,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, pair_generation,
                        memory_order_release);
  memset(out_dispatch, 0, sizeof(*out_dispatch));
  out_dispatch->gid = gid;
  out_dispatch->dispatch_epoch = dispatch_epoch;
  out_dispatch->transition_seq = transition_seq;
  out_dispatch->target = entry->target;
  out_dispatch->dialect = entry->dialect;
  out_dispatch->focus = entry->focus;
  out_dispatch->sql_ref = entry->sql_ref;
  out_dispatch->sql_sha256 = entry->sql_sha256;
  return QST_OK;
}

EQstCode qst_task_get_generate_payload(SQstShmMapping *mapping, uint64_t gid,
                                       uint64_t dispatch_epoch, EQstRole role,
                                       uint32_t generation,
                                       SQstGeneratePayload *out_payload) {
  SQstTaskEntry *entry = NULL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == out_payload ||
      role != QST_ROLE_EXEC_MANAGER || generation == 0 ||
      dispatch_epoch == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
          QST_TASK_GENERATED ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      entry->gid == 0 || qstRefIsEmpty(entry->sql_ref) ||
      qst_block_validate(mapping, entry->sql_ref, gid) != QST_OK) {
    return QST_E_STALE_OWNER;
  }
  memset(out_payload, 0, sizeof(*out_payload));
  out_payload->target = entry->target;
  out_payload->mode = entry->mode;
  out_payload->focus = entry->focus;
  out_payload->dialect = entry->dialect;
  out_payload->generation_contains_abnormal =
      entry->generation_contains_abnormal;
  out_payload->generation_contains_unsupported_fallback =
      entry->generation_contains_unsupported_fallback;
  out_payload->generation_seq = entry->generation_seq;
  out_payload->weight_profile_version = entry->weight_profile_version;
  out_payload->sql_sha256 = entry->sql_sha256;
  out_payload->dataset_digest = entry->dataset_digest;
  out_payload->sql_ref = entry->sql_ref;
  out_payload->generation_record_ref = entry->generation_record_ref;
  out_payload->generation_record_sha256 = entry->generation_record_sha256;
  return QST_OK;
}

EQstCode qst_task_assign_pair(SQstShmMapping *mapping, uint64_t gid,
                              EQstRole role, uint32_t generation,
                              uint64_t dispatch_epoch,
                              uint64_t transition_seq,
                              uint32_t pair_slot_id,
                              uint32_t pair_generation) {
  SQstTaskEntry *entry = NULL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || role != QST_ROLE_EXEC_MANAGER || generation == 0 ||
      dispatch_epoch == 0 || transition_seq == 0 || pair_generation == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (!qstPairSupervisorBelongsTo(mapping, pair_slot_id, pair_generation,
                                  generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
          QST_TASK_EXECUTING ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) != role ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) != 0 ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          generation) {
    return QST_E_STALE_OWNER;
  }
  atomic_store_explicit(&entry->owner_generation, pair_generation,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, pair_slot_id,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_role, QST_ROLE_PAIR_SUPERVISOR,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_task_abort_dispatch(SQstShmMapping *mapping, uint64_t gid,
                                 uint64_t dispatch_epoch,
                                 uint64_t transition_seq, EQstRole owner_role,
                                 uint32_t owner_slot_id,
                                 uint32_t owner_generation) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_EXECUTING;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || dispatch_epoch == 0 || transition_seq == 0 ||
      owner_generation == 0 ||
      (owner_role != QST_ROLE_EXEC_MANAGER &&
       owner_role != QST_ROLE_PAIR_SUPERVISOR)) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (owner_role == QST_ROLE_EXEC_MANAGER) {
    if (!qstTaskCallerIsCurrent(mapping, owner_role, 0, owner_generation)) {
      return QST_E_STALE_OWNER;
    }
  } else if (!qstTaskCallerIsCurrent(mapping, owner_role, owner_slot_id,
                                     owner_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
          owner_role ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
          owner_slot_id ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          owner_generation) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_GENERATED,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  atomic_store_explicit(&entry->owner_role, QST_ROLE_GENERATOR,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, 0, memory_order_release);
  return QST_OK;
}

static int32_t qstTaskGidExcluded(const uint64_t *excluded_gids,
                                  uint32_t excluded_count, uint64_t gid) {
  for (uint32_t index = 0; index < excluded_count; ++index) {
    if (excluded_gids[index] == gid) {
      return 1;
    }
  }
  return 0;
}

EQstCode qst_task_find_assigned_pair_excluding(
    SQstShmMapping *mapping, uint32_t pair_slot_id, uint32_t pair_generation,
    const uint64_t *excluded_gids, uint32_t excluded_count,
    SQstTaskDispatch *out_dispatch) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || pair_generation == 0 || NULL == out_dispatch ||
      excluded_count > QST_PAIR_MAX_INFLIGHT ||
      (excluded_count != 0 && NULL == excluded_gids)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_PAIR_SUPERVISOR,
                              pair_slot_id, pair_generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    SQstTaskEntry *entry = &entries[index];

    if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
            QST_TASK_EXECUTING ||
        atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
            QST_ROLE_PAIR_SUPERVISOR ||
        atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
            pair_slot_id ||
        atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
            pair_generation) {
      continue;
    }
    if (qstTaskGidExcluded(excluded_gids, excluded_count, entry->gid) != 0) {
      continue;
    }
    if (entry->gid == 0 || qstRefIsEmpty(entry->sql_ref) ||
        qst_block_validate(mapping, entry->sql_ref, entry->gid) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    memset(out_dispatch, 0, sizeof(*out_dispatch));
    out_dispatch->gid = entry->gid;
    out_dispatch->dispatch_epoch = atomic_load_explicit(
        &entry->dispatch_epoch, memory_order_acquire);
    out_dispatch->transition_seq = atomic_load_explicit(
        &entry->transition_seq, memory_order_acquire);
    out_dispatch->target = entry->target;
    out_dispatch->dialect = entry->dialect;
    out_dispatch->focus = entry->focus;
    out_dispatch->sql_ref = entry->sql_ref;
    out_dispatch->sql_sha256 = entry->sql_sha256;
    if (out_dispatch->dispatch_epoch == 0 || out_dispatch->transition_seq == 0) {
      return QST_E_SHM_CORRUPT;
    }
    if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
            QST_TASK_EXECUTING ||
        atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
            QST_ROLE_PAIR_SUPERVISOR ||
        atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
            pair_slot_id ||
        atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
            pair_generation ||
        atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
            out_dispatch->dispatch_epoch ||
        atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
            out_dispatch->transition_seq) {
      return QST_E_STALE_OWNER;
    }
    return QST_OK;
  }
  return QST_E_QUEUE_EMPTY;
}

EQstCode qst_task_find_assigned_pair(SQstShmMapping *mapping,
                                     uint32_t pair_slot_id,
                                     uint32_t pair_generation,
                                     SQstTaskDispatch *out_dispatch) {
  return qst_task_find_assigned_pair_excluding(
      mapping, pair_slot_id, pair_generation, NULL, 0, out_dispatch);
}

EQstCode qst_task_pair_is_idle(const SQstShmMapping *mapping,
                               uint32_t exec_generation,
                               uint32_t pair_slot_id,
                               uint32_t pair_generation,
                               int32_t *out_is_idle) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_is_idle || exec_generation == 0 ||
      pair_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_EXEC_MANAGER, 0,
                              exec_generation) ||
      !qstPairSupervisorBelongsTo(mapping, pair_slot_id, pair_generation,
                                  exec_generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  uint32_t in_flight = 0;

  for (uint32_t index = 0; index < count; ++index) {
    if (atomic_load_explicit(&entries[index].phase, memory_order_acquire) ==
            QST_TASK_EXECUTING &&
        atomic_load_explicit(&entries[index].owner_role,
                             memory_order_acquire) ==
            QST_ROLE_PAIR_SUPERVISOR &&
        atomic_load_explicit(&entries[index].owner_slot_id,
                             memory_order_acquire) == pair_slot_id &&
        atomic_load_explicit(&entries[index].owner_generation,
                             memory_order_acquire) == pair_generation) {
      ++in_flight;
      if (in_flight >= QST_PAIR_MAX_INFLIGHT) {
        *out_is_idle = 0;
        return QST_OK;
      }
    }
  }
  *out_is_idle = 1;
  return QST_OK;
}

EQstCode qst_task_publish_final(SQstShmMapping *mapping, uint64_t gid,
                                uint64_t dispatch_epoch,
                                uint64_t transition_seq, EQstRole role,
                                uint32_t slot_id,
                                uint32_t generation,
                                const SQstExecutePayload *final_payload) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_EXECUTING;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == final_payload ||
      role != QST_ROLE_PAIR_SUPERVISOR || generation == 0 ||
      dispatch_epoch == 0 || transition_seq == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, slot_id, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->owner_role, memory_order_acquire) != role ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
          slot_id ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          generation) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      qst_block_validate(mapping, entry->sql_ref, gid) != QST_OK ||
      (!qstRefIsEmpty(final_payload->result_ref) &&
       qst_block_validate(mapping, final_payload->result_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(final_payload->error_ref) &&
       qst_block_validate(mapping, final_payload->error_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(final_payload->log_range_ref) &&
       qst_block_validate(mapping, final_payload->log_range_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(final_payload->core_path_ref) &&
       qst_block_validate(mapping, final_payload->core_path_ref, gid) != QST_OK)) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_REGISTERING,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  entry->final_payload = *final_payload;
  atomic_store_explicit(&entry->owner_role, role, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, slot_id, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, generation,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->phase, QST_TASK_FINAL, memory_order_release);
  return QST_OK;
}

EQstCode qst_task_set_result_storage(SQstShmMapping *mapping, uint64_t gid,
                                     uint64_t dispatch_epoch,
                                     uint64_t transition_seq,
                                     uint32_t pair_slot_id,
                                     uint32_t pair_generation,
                                     uint32_t result_storage_status) {
  SQstTaskEntry *entry = NULL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || pair_generation == 0 || dispatch_epoch == 0 ||
      transition_seq == 0 || result_storage_status < QST_RESULT_NOT_REQUESTED ||
      result_storage_status > QST_RESULT_UNAVAILABLE) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_PAIR_SUPERVISOR,
                              pair_slot_id, pair_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
          QST_TASK_EXECUTING ||
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
          QST_ROLE_PAIR_SUPERVISOR ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
          pair_slot_id ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          pair_generation ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq) {
    return QST_E_STALE_OWNER;
  }
  atomic_store_explicit(&entry->result_storage_status, result_storage_status,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_task_retry_timeout(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint64_t transition_seq, uint32_t exec_generation, uint32_t retry_count,
    SQstTaskDispatch *out_dispatch, int32_t *out_recycle_required,
    uint32_t *out_previous_pair_slot, uint32_t *out_previous_pair_generation) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_FINAL;
  uint32_t timeout_attempt = 0;
  uint64_t max_attempts = 0;
  uint64_t next_dispatch_epoch = 0;
  uint64_t next_transition_seq = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == out_dispatch || NULL == out_recycle_required ||
      NULL == out_previous_pair_slot || NULL == out_previous_pair_generation ||
      dispatch_epoch == 0 || transition_seq == 0 || exec_generation == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_EXEC_MANAGER, 0,
                              exec_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (retry_count == UINT32_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  max_attempts = (uint64_t)retry_count + 1U;
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
          QST_TASK_FINAL ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      entry->final_payload.execution_status != QST_EXEC_TIMEOUT ||
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
          QST_ROLE_PAIR_SUPERVISOR ||
      qstRefIsEmpty(entry->sql_ref) ||
      qst_block_validate(mapping, entry->sql_ref, gid) != QST_OK) {
    return QST_E_STALE_OWNER;
  }
  timeout_attempt = atomic_load_explicit(&entry->timeout_attempt,
                                         memory_order_acquire);
  if (timeout_attempt == UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  timeout_attempt += 1U;
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_REGISTERING,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  atomic_store_explicit(&entry->timeout_attempt, timeout_attempt,
                        memory_order_relaxed);
  if ((uint64_t)timeout_attempt >= max_attempts) {
    atomic_store_explicit(&entry->phase, QST_TASK_FINAL, memory_order_release);
    return QST_E_DEADLINE;
  }
  next_dispatch_epoch = dispatch_epoch + 1U;
  next_transition_seq = transition_seq + 1U;
  if (next_dispatch_epoch == 0 || next_transition_seq == 0) {
    atomic_store_explicit(&entry->phase, QST_TASK_FINAL, memory_order_release);
    return QST_E_RESOURCE;
  }
  *out_previous_pair_slot = atomic_load_explicit(&entry->owner_slot_id,
                                                  memory_order_acquire);
  *out_previous_pair_generation = atomic_load_explicit(
      &entry->owner_generation, memory_order_acquire);
  *out_recycle_required =
      (uint64_t)timeout_attempt + 1U == max_attempts ? 1 : 0;
  memset(out_dispatch, 0, sizeof(*out_dispatch));
  out_dispatch->gid = gid;
  out_dispatch->dispatch_epoch = next_dispatch_epoch;
  out_dispatch->transition_seq = next_transition_seq;
    out_dispatch->target = entry->target;
    out_dispatch->dialect = entry->dialect;
    out_dispatch->focus = entry->focus;
  out_dispatch->sql_ref = entry->sql_ref;
  out_dispatch->sql_sha256 = entry->sql_sha256;
  memset(&entry->final_payload, 0, sizeof(entry->final_payload));
  atomic_store_explicit(&entry->dispatch_epoch, next_dispatch_epoch,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->transition_seq, next_transition_seq,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_role, QST_ROLE_EXEC_MANAGER,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, exec_generation,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->phase, QST_TASK_EXECUTING,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_task_recover_pair_failure(
    SQstShmMapping *mapping, uint32_t exec_generation,
    uint32_t failed_pair_slot, uint32_t failed_pair_generation,
    SQstTaskDispatch *out_dispatch) {
  SQstTaskEntry *entries = NULL;
  uint32_t entry_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_dispatch || exec_generation == 0 ||
      failed_pair_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_EXEC_MANAGER, 0,
                              exec_generation) ||
      !qstPairSupervisorBelongsTo(mapping, failed_pair_slot,
                                  failed_pair_generation, exec_generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetTaskRegion(mapping, &entries, &entry_count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < entry_count; ++index) {
    SQstTaskEntry *entry = &entries[index];
    uint32_t expected_phase = QST_TASK_EXECUTING;
    uint64_t dispatch_epoch = 0;
    uint64_t transition_seq = 0;

    if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
            QST_TASK_EXECUTING ||
        atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
            QST_ROLE_PAIR_SUPERVISOR ||
        atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
            failed_pair_slot ||
        atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
            failed_pair_generation) {
      continue;
    }
    if (entry->gid == 0 || qstRefIsEmpty(entry->sql_ref) ||
        qst_block_validate(mapping, entry->sql_ref, entry->gid) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    dispatch_epoch = atomic_load_explicit(&entry->dispatch_epoch,
                                          memory_order_acquire);
    transition_seq = atomic_load_explicit(&entry->transition_seq,
                                          memory_order_acquire);
    if (dispatch_epoch == UINT64_MAX || transition_seq == UINT64_MAX ||
        !atomic_compare_exchange_strong_explicit(
            &entry->phase, &expected_phase, QST_TASK_REGISTERING,
            memory_order_acq_rel, memory_order_acquire)) {
      return QST_E_STALE_OWNER;
    }
    memset(out_dispatch, 0, sizeof(*out_dispatch));
    out_dispatch->gid = entry->gid;
    out_dispatch->dispatch_epoch = dispatch_epoch + 1U;
    out_dispatch->transition_seq = transition_seq + 1U;
    out_dispatch->target = entry->target;
    out_dispatch->dialect = entry->dialect;
    out_dispatch->focus = entry->focus;
    out_dispatch->sql_ref = entry->sql_ref;
    out_dispatch->sql_sha256 = entry->sql_sha256;
    memset(&entry->final_payload, 0, sizeof(entry->final_payload));
    atomic_store_explicit(&entry->result_storage_status,
                          QST_RESULT_NOT_REQUESTED, memory_order_relaxed);
    atomic_store_explicit(&entry->dispatch_epoch, out_dispatch->dispatch_epoch,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->transition_seq, out_dispatch->transition_seq,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->owner_role, QST_ROLE_EXEC_MANAGER,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
    atomic_store_explicit(&entry->owner_generation, exec_generation,
                          memory_order_relaxed);
    atomic_store_explicit(&entry->phase, QST_TASK_EXECUTING,
                          memory_order_release);
    return QST_OK;
  }
  return QST_E_QUEUE_EMPTY;
}

EQstCode qst_task_get_final(SQstShmMapping *mapping, uint64_t gid,
                            uint64_t dispatch_epoch,
                            uint64_t transition_seq, EQstRole role,
                            uint32_t generation,
                            SQstExecutePayload *out_payload,
                            uint32_t *out_target) {
  SQstTaskEntry *entry = NULL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == out_payload || NULL == out_target ||
      role != QST_ROLE_EXEC_MANAGER || generation == 0 ||
      dispatch_epoch == 0 || transition_seq == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if ((atomic_load_explicit(&entry->phase, memory_order_acquire) !=
           QST_TASK_FINAL &&
       atomic_load_explicit(&entry->phase, memory_order_acquire) !=
           QST_TASK_ISOLATED) ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      entry->target == 0 ||
      qst_block_validate(mapping, entry->sql_ref, gid) != QST_OK) {
    return QST_E_STALE_OWNER;
  }
  *out_payload = entry->final_payload;
  *out_target = entry->target;
  if ((!qstRefIsEmpty(out_payload->result_ref) &&
       qst_block_validate(mapping, out_payload->result_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(out_payload->error_ref) &&
       qst_block_validate(mapping, out_payload->error_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(out_payload->log_range_ref) &&
       qst_block_validate(mapping, out_payload->log_range_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(out_payload->core_path_ref) &&
       qst_block_validate(mapping, out_payload->core_path_ref, gid) != QST_OK)) {
    return QST_E_SHM_CORRUPT;
  }
  if ((atomic_load_explicit(&entry->phase, memory_order_acquire) !=
           QST_TASK_FINAL &&
       atomic_load_explicit(&entry->phase, memory_order_acquire) !=
           QST_TASK_ISOLATED) ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq) {
    return QST_E_STALE_OWNER;
  }
  return QST_OK;
}

EQstCode qst_task_get_final_pair(SQstShmMapping *mapping, uint64_t gid,
                                 uint64_t dispatch_epoch,
                                 uint64_t transition_seq,
                                 uint32_t pair_slot_id,
                                 uint32_t pair_generation,
                                 SQstExecutePayload *out_payload,
                                 uint32_t *out_target) {
  SQstTaskEntry *entry = NULL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == out_payload || NULL == out_target ||
      pair_generation == 0 || dispatch_epoch == 0 || transition_seq == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_PAIR_SUPERVISOR,
                              pair_slot_id, pair_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
          QST_TASK_FINAL ||
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
          QST_ROLE_PAIR_SUPERVISOR ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
          pair_slot_id ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          pair_generation ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      entry->target == 0 || qst_block_validate(mapping, entry->sql_ref, gid) !=
          QST_OK) {
    return QST_E_STALE_OWNER;
  }
  *out_payload = entry->final_payload;
  *out_target = entry->target;
  if ((!qstRefIsEmpty(out_payload->result_ref) &&
       qst_block_validate(mapping, out_payload->result_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(out_payload->error_ref) &&
       qst_block_validate(mapping, out_payload->error_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(out_payload->log_range_ref) &&
       qst_block_validate(mapping, out_payload->log_range_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(out_payload->core_path_ref) &&
       qst_block_validate(mapping, out_payload->core_path_ref, gid) != QST_OK)) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

EQstCode qst_task_get_persist_snapshot(const SQstShmMapping *mapping,
                                       uint64_t gid,
                                       uint64_t dispatch_epoch,
                                       uint64_t transition_seq,
                                       SQstTaskPersistSnapshot *out_snapshot) {
  SQstTaskEntry *entry = NULL;
  uint32_t phase = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == out_snapshot || dispatch_epoch == 0 ||
      transition_seq == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  phase = atomic_load_explicit(&entry->phase, memory_order_acquire);
  if ((phase != QST_TASK_FINAL_PUBLISHED && phase != QST_TASK_PERSISTING) ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      qst_block_validate(mapping, entry->sql_ref, gid) != QST_OK ||
      (!qstRefIsEmpty(entry->final_payload.result_ref) &&
       qst_block_validate(mapping, entry->final_payload.result_ref, gid) != QST_OK) ||
      (!qstRefIsEmpty(entry->final_payload.error_ref) &&
       qst_block_validate(mapping, entry->final_payload.error_ref, gid) != QST_OK)) {
    return QST_E_STALE_OWNER;
  }
  memset(out_snapshot, 0, sizeof(*out_snapshot));
  out_snapshot->gid = entry->gid;
  out_snapshot->dispatch_epoch = dispatch_epoch;
  out_snapshot->transition_seq = transition_seq;
  out_snapshot->target = entry->target;
  out_snapshot->focus = entry->focus;
  out_snapshot->dialect = entry->dialect;
  out_snapshot->generation_contains_abnormal =
      entry->generation_contains_abnormal;
  out_snapshot->generation_contains_unsupported_fallback =
      entry->generation_contains_unsupported_fallback;
  out_snapshot->result_storage_status = atomic_load_explicit(
      &entry->result_storage_status, memory_order_acquire);
  out_snapshot->sql_sha256 = entry->sql_sha256;
  out_snapshot->sql_ref = entry->sql_ref;
  out_snapshot->final_payload = entry->final_payload;
  return QST_OK;
}

EQstCode qst_task_find_final_for_exec(SQstShmMapping *mapping,
                                      uint32_t generation,
                                      uint64_t *out_gid,
                                      uint64_t *out_dispatch_epoch,
                                      uint64_t *out_transition_seq) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || generation == 0 || NULL == out_gid ||
      NULL == out_dispatch_epoch || NULL == out_transition_seq) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_EXEC_MANAGER, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    SQstTaskEntry *entry = &entries[index];
    uint64_t dispatch_epoch = 0;
    uint64_t transition_seq = 0;

    const uint32_t phase = atomic_load_explicit(&entry->phase,
                                                memory_order_acquire);

    if ((phase != QST_TASK_FINAL && phase != QST_TASK_ISOLATED) ||
        entry->gid == 0) {
      continue;
    }
    /* Normal pair completion publishes directly to execute_queue. The
     * manager scans only terminal evidence that needs recovery handling. */
    if (phase == QST_TASK_FINAL &&
        entry->final_payload.execution_status != QST_EXEC_TIMEOUT) {
      continue;
    }
    dispatch_epoch = atomic_load_explicit(&entry->dispatch_epoch,
                                          memory_order_acquire);
    transition_seq = atomic_load_explicit(&entry->transition_seq,
                                          memory_order_acquire);
    if (dispatch_epoch == 0 || transition_seq == 0) {
      return QST_E_SHM_CORRUPT;
    }
    *out_gid = entry->gid;
    *out_dispatch_epoch = dispatch_epoch;
    *out_transition_seq = transition_seq;
    return QST_OK;
  }
  return QST_E_QUEUE_EMPTY;
}

EQstCode qst_task_find_final_for_pair_recovery(
    SQstShmMapping *mapping, uint32_t exec_generation,
    uint32_t failed_pair_slot, uint32_t failed_pair_generation,
    uint64_t *out_gid, uint64_t *out_dispatch_epoch,
    uint64_t *out_transition_seq) {
  SQstTaskEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || exec_generation == 0 ||
      failed_pair_generation == 0 || NULL == out_gid ||
      NULL == out_dispatch_epoch || NULL == out_transition_seq) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_EXEC_MANAGER, 0,
                              exec_generation) ||
      !qstPairSupervisorBelongsTo(mapping, failed_pair_slot,
                                  failed_pair_generation, exec_generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetTaskRegion(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    SQstTaskEntry *entry = &entries[index];

    if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
            QST_TASK_FINAL ||
        atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
            QST_ROLE_PAIR_SUPERVISOR ||
        atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
            failed_pair_slot ||
        atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
            failed_pair_generation ||
        entry->gid == 0) {
      continue;
    }
    *out_gid = entry->gid;
    *out_dispatch_epoch = atomic_load_explicit(&entry->dispatch_epoch,
                                                memory_order_acquire);
    *out_transition_seq = atomic_load_explicit(&entry->transition_seq,
                                                memory_order_acquire);
    return *out_dispatch_epoch == 0 || *out_transition_seq == 0 ?
               QST_E_SHM_CORRUPT :
               QST_OK;
  }
  return QST_E_QUEUE_EMPTY;
}

EQstCode qst_task_confirm_final_published(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint64_t transition_seq, EQstRole role, uint32_t generation,
    const SQstExecutePayload *expected_payload) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == expected_payload ||
      role != QST_ROLE_EXEC_MANAGER || generation == 0 ||
      dispatch_epoch == 0 || transition_seq == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      !qstExecutePayloadEqual(&entry->final_payload, expected_payload)) {
    return QST_E_STALE_OWNER;
  }
  expected_phase = atomic_load_explicit(&entry->phase, memory_order_acquire);
  if (expected_phase != QST_TASK_FINAL && expected_phase != QST_TASK_ISOLATED) {
    return QST_E_TASK_STATE;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_FINAL_PUBLISHED,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  atomic_store_explicit(&entry->owner_role, role, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, generation,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_task_confirm_final_published_pair(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint64_t transition_seq, uint32_t pair_slot_id, uint32_t pair_generation,
    const SQstExecutePayload *expected_payload) {
  SQstTaskEntry *entry = NULL;
  uint32_t expected_phase = QST_TASK_FINAL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == expected_payload || pair_generation == 0 ||
      dispatch_epoch == 0 || transition_seq == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, QST_ROLE_PAIR_SUPERVISOR,
                              pair_slot_id, pair_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->owner_role, memory_order_acquire) !=
          QST_ROLE_PAIR_SUPERVISOR ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
          pair_slot_id ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          pair_generation ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq ||
      !qstExecutePayloadEqual(&entry->final_payload, expected_payload) ||
      !atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_FINAL_PUBLISHED,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  atomic_store_explicit(&entry->owner_role, QST_ROLE_PAIR_SUPERVISOR,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, pair_slot_id,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, pair_generation,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_task_begin_persist(SQstShmMapping *mapping, uint64_t gid,
                                uint64_t dispatch_epoch,
                                uint64_t transition_seq, EQstRole role,
                                uint32_t slot_id, uint32_t generation,
                                int32_t final_verdict,
                                uint32_t required_persist_mask) {
  SQstTaskEntry *entry = NULL;
  uint32_t phase = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || role != QST_ROLE_JUDGE_WORKER || generation == 0 ||
      required_persist_mask == 0 ||
      (required_persist_mask & ~QST_PERSIST_MASK_ALL) != 0 ||
      (((required_persist_mask & QST_PERSIST_MASK_INITIAL_RESULT) != 0 &&
        (final_verdict < 0 || final_verdict > 2)) ||
       ((required_persist_mask & QST_PERSIST_MASK_INITIAL_RESULT) == 0 &&
        final_verdict != -1))) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, slot_id, generation)) {
    return QST_E_STALE_OWNER;
  }
  phase = atomic_load_explicit(&entry->phase, memory_order_acquire);
  /* execute_queue publication precedes FINAL_PUBLISHED so a crashed manager
   * leaves recoverable FINAL evidence. A fast worker may observe that narrow
   * interval after claiming the message; yield only for that publication. */
  for (uint32_t attempt = 0; phase == QST_TASK_FINAL && attempt < 1024U;
       ++attempt) {
    (void)sched_yield();
    phase = atomic_load_explicit(&entry->phase, memory_order_acquire);
  }
  if (phase == QST_TASK_PERSISTING) {
    if (atomic_load_explicit(&entry->owner_role, memory_order_acquire) != role ||
        atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
            slot_id ||
        atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
            generation ||
        atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
            dispatch_epoch ||
        atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
            transition_seq) {
      return QST_E_STALE_OWNER;
    }
    return atomic_load_explicit(&entry->required_persist_mask,
                                memory_order_acquire) == required_persist_mask &&
                   entry->final_verdict == final_verdict ?
               QST_OK : QST_E_TASK_STATE;
  }
  if (phase != QST_TASK_FINAL_PUBLISHED && phase != QST_TASK_ISOLATED) {
    return QST_E_TASK_STATE;
  }
  if (atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch ||
      atomic_load_explicit(&entry->transition_seq, memory_order_acquire) !=
          transition_seq) {
    return QST_E_STALE_OWNER;
  }
  atomic_store_explicit(&entry->required_persist_mask, required_persist_mask,
                        memory_order_relaxed);
  entry->final_verdict = final_verdict;
  atomic_store_explicit(&entry->owner_role, role, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, slot_id, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, generation,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->phase, QST_TASK_PERSISTING,
                        memory_order_release);
  return QST_OK;
}

static EQstCode qstTaskMarkDurable(SQstShmMapping *mapping, uint64_t gid,
                                   uint64_t dispatch_epoch,
                                   uint32_t persist_mask,
                                   uint32_t durable_source,
                                   uint64_t record_seq, EQstRole role,
                                   uint32_t slot_id, uint32_t generation,
                                   SQstTaskEntry *entry) {
  uint32_t required_mask = 0;
  uint32_t durable_mask = 0;
  EQstCode code = QST_OK;

  if (entry == NULL) {
    code = qstFindTask(mapping, gid, &entry);
  }

  if (code != QST_OK ||
      (role != QST_ROLE_MAIN && role != QST_ROLE_JUDGE_WORKER) ||
      (role == QST_ROLE_MAIN && slot_id != 0) || generation == 0 ||
      record_seq == 0 || (durable_source != QST_DURABLE_RESULT_STORE &&
                          durable_source != QST_DURABLE_PENDING_FILE)) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (entry->gid != gid) {
    return QST_E_TASK_STATE;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, slot_id, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
          QST_TASK_PERSISTING ||
      atomic_load_explicit(&entry->dispatch_epoch, memory_order_acquire) !=
          dispatch_epoch) {
    return QST_E_STALE_OWNER;
  }
  required_mask = atomic_load_explicit(&entry->required_persist_mask,
                                       memory_order_acquire);
  if (persist_mask == 0 || (persist_mask & ~required_mask) != 0) {
    return QST_E_TASK_STATE;
  }
  durable_mask = atomic_fetch_or_explicit(&entry->durable_persist_mask,
                                          persist_mask, memory_order_release) |
                 persist_mask;
  entry->record_seq = record_seq;
  atomic_store_explicit(&entry->owner_role, role, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, slot_id, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, generation,
                        memory_order_release);
  if ((durable_mask & required_mask) == required_mask) {
    qstTaskRecordPersistedStats(mapping, entry);
    atomic_store_explicit(&entry->phase, QST_TASK_PERSISTED,
                          memory_order_release);
  }
  return QST_OK;
}

EQstCode qst_task_mark_durable(SQstShmMapping *mapping, uint64_t gid,
                               uint64_t dispatch_epoch, uint32_t persist_mask,
                               uint32_t durable_source, uint64_t record_seq,
                               EQstRole role, uint32_t generation) {
  return qstTaskMarkDurable(mapping, gid, dispatch_epoch, persist_mask,
                            durable_source, record_seq, role, 0, generation,
                            NULL);
}

static EQstCode qstTaskReleasePersistedBlocks(SQstShmMapping *mapping,
                                              uint64_t gid, EQstRole role,
                                              uint32_t slot_id,
                                              uint32_t generation,
                                              SQstTaskEntry *entry) {
  SQstRef refs[7] = {{0}};
  EQstCode code = QST_OK;

  if (entry == NULL) {
    code = qstFindTask(mapping, gid, &entry);
  }

  if (code != QST_OK ||
      (role != QST_ROLE_MAIN && role != QST_ROLE_JUDGE_WORKER) ||
      (role == QST_ROLE_MAIN && slot_id != 0) || generation == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (entry->gid != gid) {
    return QST_E_TASK_STATE;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, slot_id, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&entry->phase, memory_order_acquire) !=
      QST_TASK_PERSISTED) {
    return QST_E_TASK_STATE;
  }
  if (
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) != role ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) !=
          slot_id ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          generation) {
    return QST_E_STALE_OWNER;
  }
  refs[0] = entry->sql_ref;
  refs[1] = entry->generation_record_ref;
  refs[2] = entry->persist_manifest_ref;
  refs[3] = entry->final_payload.result_ref;
  refs[4] = entry->final_payload.error_ref;
  refs[5] = entry->final_payload.log_range_ref;
  refs[6] = entry->final_payload.core_path_ref;
  if (qst_block_release_many(mapping, refs, sizeof(refs) / sizeof(refs[0]),
                             gid) != QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  memset(&entry->sql_ref, 0, sizeof(entry->sql_ref));
  memset(&entry->generation_record_ref, 0, sizeof(entry->generation_record_ref));
  memset(&entry->persist_manifest_ref, 0, sizeof(entry->persist_manifest_ref));
  memset(&entry->final_payload.result_ref, 0,
         sizeof(entry->final_payload.result_ref));
  memset(&entry->final_payload.error_ref, 0,
         sizeof(entry->final_payload.error_ref));
  memset(&entry->final_payload.log_range_ref, 0,
         sizeof(entry->final_payload.log_range_ref));
  memset(&entry->final_payload.core_path_ref, 0,
         sizeof(entry->final_payload.core_path_ref));
  return QST_OK;
}

EQstCode qst_task_release_persisted_blocks(SQstShmMapping *mapping,
                                           uint64_t gid, EQstRole role,
                                           uint32_t generation) {
  return qstTaskReleasePersistedBlocks(mapping, gid, role, 0, generation,
                                       NULL);
}

static EQstCode qstTaskReclaimPersisted(SQstShmMapping *mapping, uint64_t gid,
                                        EQstRole role, uint32_t slot_id,
                                        uint32_t generation,
                                        SQstTaskEntry *entry) {
  uint32_t expected_phase = QST_TASK_PERSISTED;
  EQstCode code = QST_OK;

  if (entry == NULL) {
    code = qstFindTask(mapping, gid, &entry);
  }

  if (code != QST_OK ||
      (role != QST_ROLE_MAIN && role != QST_ROLE_JUDGE_WORKER) ||
      (role == QST_ROLE_MAIN && slot_id != 0) || generation == 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (entry->gid != gid) {
    return QST_E_TASK_STATE;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, slot_id, generation)) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &entry->phase, &expected_phase, QST_TASK_REGISTERING,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_TASK_STATE;
  }
  if (!qstRefIsEmpty(entry->sql_ref) ||
      !qstRefIsEmpty(entry->generation_record_ref) ||
      !qstRefIsEmpty(entry->persist_manifest_ref) ||
      !qstRefIsEmpty(entry->final_payload.result_ref) ||
      !qstRefIsEmpty(entry->final_payload.error_ref) ||
      !qstRefIsEmpty(entry->final_payload.log_range_ref) ||
      !qstRefIsEmpty(entry->final_payload.core_path_ref)) {
    atomic_store_explicit(&entry->phase, QST_TASK_PERSISTED,
                          memory_order_release);
    return QST_E_TASK_STATE;
  }
  entry->gid = 0;
  entry->target = 0;
  entry->mode = 0;
  entry->focus = 0;
  entry->dialect = 0;
  memset(&entry->final_payload, 0, sizeof(entry->final_payload));
  entry->verdict_ts_ns = 0;
  entry->final_verdict = -1;
  entry->generation_contains_abnormal = 0;
  entry->generation_contains_unsupported_fallback = 0;
  memset(entry->reserved0, 0, sizeof(entry->reserved0));
  entry->generation_seq = 0;
  entry->weight_profile_version = 0;
  entry->record_seq = 0;
  memset(&entry->sql_ref, 0, sizeof(entry->sql_ref));
  memset(&entry->sql_sha256, 0, sizeof(entry->sql_sha256));
  memset(&entry->dataset_digest, 0, sizeof(entry->dataset_digest));
  memset(&entry->persist_manifest_ref, 0, sizeof(entry->persist_manifest_ref));
  memset(&entry->generation_record_ref, 0, sizeof(entry->generation_record_ref));
  memset(&entry->generation_record_sha256, 0,
         sizeof(entry->generation_record_sha256));
  memset(entry->reserved, 0, sizeof(entry->reserved));
  atomic_store_explicit(&entry->dispatch_epoch, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->transition_seq, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_role, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->timeout_attempt, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->required_persist_mask, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->durable_persist_mask, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->result_storage_status, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->phase, 0, memory_order_release);
  (void)atomic_fetch_add_explicit(&mapping->header->free_task_count, 1,
                                  memory_order_release);
  return QST_OK;
}

EQstCode qst_task_reclaim_persisted(SQstShmMapping *mapping, uint64_t gid,
                                    EQstRole role, uint32_t generation) {
  return qstTaskReclaimPersisted(mapping, gid, role, 0, generation, NULL);
}

EQstCode qst_task_complete_persisted_by_judge(
    SQstShmMapping *mapping, uint64_t gid, uint64_t dispatch_epoch,
    uint32_t persist_mask, uint32_t durable_source, uint64_t record_seq,
    uint32_t judge_slot, uint32_t judge_generation) {
  SQstTaskEntry *entry = NULL;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK) {
    return code;
  }
  code = qstTaskMarkDurable(
      mapping, gid, dispatch_epoch, persist_mask, durable_source, record_seq,
      QST_ROLE_JUDGE_WORKER, judge_slot, judge_generation, entry);

  if (code != QST_OK) {
    return code;
  }
  code = qstTaskReleasePersistedBlocks(mapping, gid, QST_ROLE_JUDGE_WORKER,
                                       judge_slot, judge_generation, entry);
  if (code != QST_OK) {
    return code;
  }
  return qstTaskReclaimPersisted(mapping, gid, QST_ROLE_JUDGE_WORKER,
                                 judge_slot, judge_generation, entry);
}

EQstCode qst_task_isolate(SQstShmMapping *mapping, uint64_t gid,
                          EQstCode reason, EQstRole role,
                          uint32_t generation,
                          const SQstExecutePayload *final_payload) {
  SQstTaskEntry *entry = NULL;
  uint32_t phase = 0;
  EQstCode code = qstFindTask(mapping, gid, &entry);

  if (code != QST_OK || NULL == final_payload || reason == QST_OK ||
      role != QST_ROLE_EXEC_MANAGER || generation == 0 ||
      final_payload->statement_ts_ns <= 0 || final_payload->ended_ts_ns <= 0 ||
      final_payload->ended_ts_ns < final_payload->statement_ts_ns ||
      final_payload->execution_status != QST_EXEC_ERROR ||
      final_payload->execution_error_code != reason ||
      !qstRefIsEmpty(final_payload->result_ref) ||
      !qstRefIsEmpty(final_payload->error_ref) ||
      !qstRefIsEmpty(final_payload->log_range_ref) ||
      !qstRefIsEmpty(final_payload->core_path_ref)) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (!qstTaskCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  phase = atomic_load_explicit(&entry->phase, memory_order_acquire);
  if (phase != QST_TASK_EXECUTING ||
      atomic_load_explicit(&entry->owner_role, memory_order_acquire) != role ||
      atomic_load_explicit(&entry->owner_slot_id, memory_order_acquire) != 0 ||
      atomic_load_explicit(&entry->owner_generation, memory_order_acquire) !=
          generation) {
    return QST_E_TASK_STATE;
  }
  atomic_store_explicit(&entry->required_persist_mask,
                        atomic_load_explicit(&entry->required_persist_mask,
                                             memory_order_acquire) |
                            QST_PERSIST_MASK_STATEMENT |
                            QST_PERSIST_MASK_INITIAL_RESULT,
                        memory_order_relaxed);
  entry->final_payload = *final_payload;
  atomic_store_explicit(&entry->result_storage_status,
                        QST_RESULT_UNAVAILABLE, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_role, role, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&entry->owner_generation, generation,
                        memory_order_relaxed);
  atomic_store_explicit(&entry->phase, QST_TASK_ISOLATED,
                        memory_order_release);
  atomic_fetch_add_explicit(&mapping->header->isolated_task_count, 1,
                            memory_order_release);
  return QST_OK;
}
