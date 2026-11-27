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

#include "qst_process.h"

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Caches only a process-ledger location. Every hit revalidates the complete
 * identity, so process stop and slot reuse remain visible to the caller. */
typedef struct {
  const SQstShmMapping *mapping;
  uint64_t run_id;
  uint32_t role;
  uint32_t slot_id;
  uint32_t generation;
  uint32_t slot_index;
  uint32_t valid;
} SQstProcessLookupCache;

static _Thread_local SQstProcessLookupCache gQstProcessLookupCache;

static EQstCode qstGetProcessRegion(const SQstShmMapping *mapping,
                                    SQstProcessSlot **out_slots,
                                    uint32_t *out_count) {
  const SQstRegion *region = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == out_slots || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    (void)fprintf(stderr,
                  "taosQueryTest: process region validation failed "
                  "element_bytes=%u expected=%zu element_count=%u "
                  "offset=%llu bytes=%llu mapping_bytes=%llu\n",
                  region->element_bytes, sizeof(SQstProcessSlot),
                  region->element_count,
                  (unsigned long long)region->region_offset,
                  (unsigned long long)region->region_bytes,
                  (unsigned long long)mapping->bytes);
    return QST_E_SHM_CORRUPT;
  }
  *out_slots = (SQstProcessSlot *)((uint8_t *)mapping->base +
                                    region->region_offset);
  *out_count = region->element_count;
  return QST_OK;
}

static EQstCode qstFindProcess(const SQstShmMapping *mapping, EQstRole role,
                               uint32_t slot_id, uint32_t generation,
                               SQstProcessSlot **out_slot) {
  SQstProcessSlot *slots = NULL;
  uint32_t count = 0;
  EQstCode code = qstGetProcessRegion(mapping, &slots, &count);

  if (code != QST_OK || role < QST_ROLE_LAUNCHER ||
      role > QST_ROLE_AI_WORKER || generation == 0 || NULL == out_slot) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  if (gQstProcessLookupCache.valid != 0 &&
      gQstProcessLookupCache.mapping == mapping &&
      gQstProcessLookupCache.run_id == mapping->header->run_id &&
      gQstProcessLookupCache.role == (uint32_t)role &&
      gQstProcessLookupCache.slot_id == slot_id &&
      gQstProcessLookupCache.generation == generation &&
      gQstProcessLookupCache.slot_index < count) {
    SQstProcessSlot *cached = &slots[gQstProcessLookupCache.slot_index];

    if (atomic_load_explicit(&cached->role, memory_order_acquire) == role &&
        cached->slot_id == slot_id &&
        atomic_load_explicit(&cached->generation, memory_order_acquire) ==
            generation) {
      *out_slot = cached;
      return QST_OK;
    }
    gQstProcessLookupCache.valid = 0;
  }
  for (uint32_t index = 0; index < count; ++index) {
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) == role &&
        slots[index].slot_id == slot_id &&
        atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
            generation) {
      gQstProcessLookupCache.mapping = mapping;
      gQstProcessLookupCache.run_id = mapping->header->run_id;
      gQstProcessLookupCache.role = (uint32_t)role;
      gQstProcessLookupCache.slot_id = slot_id;
      gQstProcessLookupCache.generation = generation;
      gQstProcessLookupCache.slot_index = index;
      gQstProcessLookupCache.valid = 1;
      *out_slot = &slots[index];
      return QST_OK;
    }
  }
  return QST_E_STALE_OWNER;
}

static int32_t qstParentIsCurrent(const SQstShmMapping *mapping,
                                  EQstRole parent_role,
                                  uint32_t parent_slot_id,
                                  uint32_t parent_generation) {
  SQstProcessSlot *parent = NULL;

  if (parent_role == QST_ROLE_LAUNCHER) {
    return parent_generation == mapping->header->launcher_generation;
  }
  if (qstFindProcess(mapping, parent_role, parent_slot_id, parent_generation,
                     &parent) != QST_OK) {
    return 0;
  }
  return atomic_load_explicit(&parent->exit_state, memory_order_acquire) !=
         QST_PROCESS_REAPED;
}

static uint64_t qstMonotonicNowNs(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

EQstCode qst_process_register_instance_ex(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t requested_slot_id,
    int32_t pid, int32_t pgid, uint32_t *out_slot_id,
    uint32_t *out_generation) {
  SQstProcessSlot *slots = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_slot_id || NULL == out_generation ||
      parent_role < QST_ROLE_LAUNCHER || parent_role > QST_ROLE_AI_WORKER ||
      parent_generation == 0 || role < QST_ROLE_MAIN ||
      role > QST_ROLE_AI_WORKER || pid <= 0 || pgid <= 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstParentIsCurrent(mapping, parent_role, parent_slot_id,
                          parent_generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetProcessRegion(mapping, &slots, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    if (requested_slot_id != QST_PROCESS_SLOT_AUTO &&
        atomic_load_explicit(&slots[index].role, memory_order_acquire) == role &&
        slots[index].slot_id == requested_slot_id &&
        atomic_load_explicit(&slots[index].exit_state, memory_order_acquire) !=
            QST_PROCESS_REAPED) {
      return QST_E_RESOURCE;
    }
  }
  for (uint32_t index = 0; index < count; ++index) {
    uint32_t expected_state = 0;
    uint32_t generation = 0;
    const uint32_t existing_role = atomic_load_explicit(&slots[index].role,
                                                        memory_order_acquire);

    if (existing_role == 0) {
      if (!atomic_compare_exchange_strong_explicit(
              &slots[index].exit_state, &expected_state,
              QST_PROCESS_REGISTERING, memory_order_acq_rel,
              memory_order_acquire)) {
        continue;
      }
    } else if (((requested_slot_id != QST_PROCESS_SLOT_AUTO &&
                 slots[index].slot_id == requested_slot_id) ||
                requested_slot_id == QST_PROCESS_SLOT_AUTO) &&
               existing_role == role) {
      expected_state = QST_PROCESS_REAPED;
      if (!atomic_compare_exchange_strong_explicit(
              &slots[index].exit_state, &expected_state,
              QST_PROCESS_REGISTERING, memory_order_acq_rel,
              memory_order_acquire)) {
        continue;
      }
    } else {
      continue;
    }
    slots[index].pid = 0;
    slots[index].ppid = 0;
    slots[index].pgid = 0;
    slots[index].slot_id = 0;
    slots[index].parent_role = 0;
    slots[index].parent_slot_id = 0;
    slots[index].parent_generation = 0;
    slots[index].exit_code = 0;
    atomic_store_explicit(&slots[index].last_error_code, QST_OK,
                          memory_order_relaxed);
    memset(&slots[index].last_error_ref, 0, sizeof(slots[index].last_error_ref));
    atomic_store_explicit(&slots[index].flow_seq, 0, memory_order_relaxed);
    atomic_store_explicit(&slots[index].flow_mono_ns, 0, memory_order_relaxed);
    atomic_store_explicit(&slots[index].business_seq, 0, memory_order_relaxed);
    atomic_store_explicit(&slots[index].business_mono_ns, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&slots[index].current_gid, 0, memory_order_relaxed);
    atomic_store_explicit(&slots[index].deadline_mono_ns, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&slots[index].stop_generation, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&slots[index].flow_observed_state, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&slots[index].flow_stage, 0, memory_order_relaxed);
    atomic_store_explicit(&slots[index].flow_state_kind, 0,
                          memory_order_relaxed);
    generation = atomic_fetch_add_explicit(&slots[index].generation, 1,
                                           memory_order_relaxed) + 1;
    if (generation == 0) {
      atomic_store_explicit(&slots[index].exit_state, 0, memory_order_release);
      return QST_E_RESOURCE;
    }
    slots[index].pid = pid;
    slots[index].ppid = (int32_t)getpid();
    slots[index].pgid = pgid;
    slots[index].slot_id = requested_slot_id == QST_PROCESS_SLOT_AUTO ? index :
                                                                     requested_slot_id;
    slots[index].parent_role = parent_role;
    slots[index].parent_slot_id = parent_slot_id;
    slots[index].parent_generation = parent_generation;
    atomic_store_explicit(&slots[index].role, role, memory_order_release);
    if (role == QST_ROLE_MAIN) {
      atomic_store_explicit(&mapping->header->main_generation, generation,
                            memory_order_release);
    }
    atomic_store_explicit(&slots[index].exit_state, QST_PROCESS_RUNNING,
                          memory_order_release);
    *out_slot_id = slots[index].slot_id;
    *out_generation = generation;
    return QST_OK;
  }
  return QST_E_RESOURCE;
}

EQstCode qst_process_register_instance(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t slot_id, int32_t pid,
    int32_t pgid, uint32_t *out_generation) {
  uint32_t ignored_slot_id = 0;

  return qst_process_register_instance_ex(
      mapping, parent_role, parent_slot_id, parent_generation, role, slot_id,
      pid, pgid, &ignored_slot_id, out_generation);
}

EQstCode qst_process_register(SQstShmMapping *mapping, EQstRole parent_role,
                               uint32_t parent_generation, EQstRole role,
                               int32_t pid, int32_t pgid,
                               uint32_t *out_generation) {
  return qst_process_register_instance(mapping, parent_role, 0,
                                       parent_generation, role, 0, pid, pgid,
                                       out_generation);
}

EQstCode qst_process_confirm_flow_instance(
    SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation,
                                  EQstFlowStage flow_stage,
                                  EQstFlowStateKind flow_state_kind,
                                  uint64_t flow_observed_state,
                                  uint64_t current_gid,
                                  uint64_t deadline_mono_ns) {
  SQstProcessSlot *slot = NULL;
  uint64_t now_ns = 0;
  uint32_t exit_state = 0;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (flow_stage < QST_FLOW_STAGE_STARTUP ||
      flow_stage > QST_FLOW_STAGE_RECOVERING ||
      flow_state_kind < QST_FLOW_STATE_CONTROL_QUEUE ||
      flow_state_kind > QST_FLOW_STATE_DIRECT_CHILDREN ||
      (current_gid != 0 && deadline_mono_ns == 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  exit_state = atomic_load_explicit(&slot->exit_state, memory_order_acquire);
  if (exit_state != QST_PROCESS_RUNNING &&
      !(exit_state == QST_PROCESS_STOP_REQUESTED &&
        flow_stage == QST_FLOW_STAGE_STOPPING)) {
    return QST_E_STALE_OWNER;
  }
  now_ns = qstMonotonicNowNs();
  if (now_ns == 0) {
    return QST_E_RESOURCE;
  }
  atomic_store_explicit(&slot->current_gid, current_gid, memory_order_relaxed);
  atomic_store_explicit(&slot->deadline_mono_ns, deadline_mono_ns,
                        memory_order_relaxed);
  atomic_store_explicit(&slot->flow_observed_state, flow_observed_state,
                        memory_order_relaxed);
  atomic_store_explicit(&slot->flow_stage, flow_stage, memory_order_relaxed);
  atomic_store_explicit(&slot->flow_state_kind, flow_state_kind,
                        memory_order_relaxed);
  atomic_store_explicit(&slot->flow_mono_ns, now_ns, memory_order_relaxed);
  atomic_fetch_add_explicit(&slot->flow_seq, 1, memory_order_release);
  return QST_OK;
}

EQstCode qst_process_confirm_flow(SQstShmMapping *mapping, EQstRole role,
                                  uint32_t generation,
                                  EQstFlowStage flow_stage,
                                  EQstFlowStateKind flow_state_kind,
                                  uint64_t flow_observed_state,
                                  uint64_t current_gid,
                                  uint64_t deadline_mono_ns) {
  return qst_process_confirm_flow_instance(
      mapping, role, 0, generation, flow_stage, flow_state_kind,
      flow_observed_state, current_gid, deadline_mono_ns);
}

EQstCode qst_process_record_business_instance(
    SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, uint64_t current_gid, uint64_t deadline_mono_ns) {
  SQstProcessSlot *slot = NULL;
  uint64_t now_ns = 0;
  uint32_t exit_state = 0;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  exit_state = atomic_load_explicit(&slot->exit_state, memory_order_acquire);
  if ((current_gid != 0 && deadline_mono_ns == 0) ||
      (exit_state != QST_PROCESS_RUNNING &&
       !(role == QST_ROLE_MAIN && slot_id == 0 &&
         exit_state == QST_PROCESS_STOP_REQUESTED))) {
    return QST_E_INVALID_ARGUMENT;
  }
  now_ns = qstMonotonicNowNs();
  if (now_ns == 0) {
    return QST_E_RESOURCE;
  }
  atomic_store_explicit(&slot->current_gid, current_gid, memory_order_relaxed);
  atomic_store_explicit(&slot->deadline_mono_ns, deadline_mono_ns,
                        memory_order_relaxed);
  atomic_store_explicit(&slot->business_mono_ns, now_ns, memory_order_relaxed);
  atomic_fetch_add_explicit(&slot->business_seq, 1, memory_order_release);
  return QST_OK;
}

EQstCode qst_process_record_business(SQstShmMapping *mapping, EQstRole role,
                                     uint32_t generation, uint64_t current_gid,
                                     uint64_t deadline_mono_ns) {
  return qst_process_record_business_instance(mapping, role, 0, generation,
                                              current_gid, deadline_mono_ns);
}

EQstCode qst_process_request_stop_instance(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t slot_id,
    uint32_t generation, uint32_t stop_generation) {
  SQstProcessSlot *slot = NULL;
  uint32_t expected_stop_generation = 0;
  uint32_t expected = QST_PROCESS_RUNNING;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (stop_generation == 0 || slot->parent_role != parent_role ||
      slot->parent_slot_id != parent_slot_id ||
      slot->parent_generation != parent_generation ||
      !qstParentIsCurrent(mapping, parent_role, parent_slot_id,
                          parent_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &slot->stop_generation, &expected_stop_generation, stop_generation,
          memory_order_release, memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &slot->exit_state, &expected, QST_PROCESS_STOP_REQUESTED,
          memory_order_acq_rel, memory_order_acquire)) {
    expected_stop_generation = stop_generation;
    (void)atomic_compare_exchange_strong_explicit(
        &slot->stop_generation, &expected_stop_generation, 0,
        memory_order_release, memory_order_acquire);
    return QST_E_STALE_OWNER;
  }
  return QST_OK;
}

EQstCode qst_process_request_stop(SQstShmMapping *mapping,
                                  EQstRole parent_role,
                                  uint32_t parent_generation, EQstRole role,
                                  uint32_t generation,
                                  uint32_t stop_generation) {
  return qst_process_request_stop_instance(
      mapping, parent_role, 0, parent_generation, role, 0, generation,
      stop_generation);
}

EQstCode qst_process_ack_stop_instance(SQstShmMapping *mapping,
                                       EQstRole role, uint32_t slot_id,
                                       uint32_t generation,
                                       uint32_t stop_generation) {
  SQstProcessSlot *slot = NULL;
  uint32_t expected = QST_PROCESS_STOP_REQUESTED;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (stop_generation == 0 ||
      atomic_load_explicit(&slot->stop_generation, memory_order_acquire) !=
          stop_generation) {
    return QST_E_STALE_OWNER;
  }
  if (atomic_load_explicit(&slot->exit_state, memory_order_acquire) ==
      QST_PROCESS_STOP_ACKED) {
    return QST_OK;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &slot->exit_state, &expected, QST_PROCESS_STOP_ACKED,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  return QST_OK;
}

EQstCode qst_process_ack_stop(SQstShmMapping *mapping, EQstRole role,
                              uint32_t generation,
                              uint32_t stop_generation) {
  return qst_process_ack_stop_instance(mapping, role, 0, generation,
                                       stop_generation);
}

EQstCode qst_process_get_stop_generation_instance(
    const SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, uint32_t *out_stop_generation,
    uint32_t *out_exit_state) {
  SQstProcessSlot *slot = NULL;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (NULL == out_stop_generation || NULL == out_exit_state) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_exit_state = atomic_load_explicit(&slot->exit_state,
                                         memory_order_acquire);
  /* A stop publisher stores its generation before releasing STOP_REQUESTED.
   * Read in that order so a caller cannot combine an old generation with the
   * newly published state and incorrectly treat its owner as stale. */
  *out_stop_generation = atomic_load_explicit(&slot->stop_generation,
                                              memory_order_acquire);
  return QST_OK;
}

EQstCode qst_process_get_stop_generation(const SQstShmMapping *mapping,
                                         EQstRole role, uint32_t generation,
                                         uint32_t *out_stop_generation,
                                         uint32_t *out_exit_state) {
  return qst_process_get_stop_generation_instance(
      mapping, role, 0, generation, out_stop_generation, out_exit_state);
}

EQstCode qst_process_record_error_instance(
    SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, int32_t error_code) {
  SQstProcessSlot *slot = NULL;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK || error_code == QST_OK) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  atomic_store_explicit(&slot->last_error_code, error_code,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_process_get_error_instance(const SQstShmMapping *mapping,
                                        EQstRole role, uint32_t slot_id,
                                        uint32_t generation,
                                        int32_t *out_error_code) {
  SQstProcessSlot *slot = NULL;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK || NULL == out_error_code) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  *out_error_code = atomic_load_explicit(&slot->last_error_code,
                                         memory_order_acquire);
  return QST_OK;
}

EQstCode qst_process_find_generation(const SQstShmMapping *mapping,
                                     EQstRole role, int32_t pid,
                                     uint32_t *out_generation) {
  SQstProcessSlot *slots = NULL;
  uint32_t count = 0;
  EQstCode code = qstGetProcessRegion(mapping, &slots, &count);

  if (code != QST_OK) {
    return code;
  }
  if (role < QST_ROLE_MAIN || role > QST_ROLE_AI_WORKER || pid <= 0 ||
      NULL == out_generation) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                memory_order_acquire);
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) == role &&
        slots[index].pid == pid && state != QST_PROCESS_REAPED) {
      *out_generation = atomic_load_explicit(&slots[index].generation,
                                             memory_order_acquire);
      return *out_generation == 0 ? QST_E_SHM_CORRUPT : QST_OK;
    }
  }
  return QST_E_STALE_OWNER;
}

EQstCode qst_process_list_direct_children(
    const SQstShmMapping *mapping, EQstRole parent_role,
    uint32_t parent_generation, SQstProcessIdentity *out_children,
    uint32_t child_capacity, uint32_t *out_child_count) {
  SQstProcessSlot *slots = NULL;
  uint32_t count = 0;
  uint32_t child_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_child_count ||
      (child_capacity != 0 && NULL == out_children) ||
      parent_role < QST_ROLE_LAUNCHER || parent_role > QST_ROLE_AI_WORKER ||
      parent_generation == 0 ||
      !qstParentIsCurrent(mapping, parent_role, 0, parent_generation)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstGetProcessRegion(mapping, &slots, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                memory_order_acquire);
    if (slots[index].parent_role == parent_role &&
        slots[index].parent_slot_id == 0 &&
        slots[index].parent_generation == parent_generation &&
        atomic_load_explicit(&slots[index].role, memory_order_acquire) != 0 &&
        state != QST_PROCESS_REAPED) {
      ++child_count;
    }
  }
  if (child_count > child_capacity) {
    *out_child_count = child_count;
    return QST_E_RESOURCE;
  }
  child_count = 0;
  for (uint32_t index = 0; index < count; ++index) {
    SQstProcessIdentity *child = NULL;
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                memory_order_acquire);

    if (slots[index].parent_role != parent_role ||
        slots[index].parent_slot_id != 0 ||
        slots[index].parent_generation != parent_generation ||
        atomic_load_explicit(&slots[index].role, memory_order_acquire) == 0 ||
        state == QST_PROCESS_REAPED) {
      continue;
    }
    child = &out_children[child_count++];
    child->slot_id = slots[index].slot_id;
    child->role = atomic_load_explicit(&slots[index].role, memory_order_acquire);
    child->generation = atomic_load_explicit(&slots[index].generation,
                                             memory_order_acquire);
    child->exit_state = state;
    child->pid = slots[index].pid;
    child->ppid = slots[index].ppid;
    child->pgid = slots[index].pgid;
    child->exit_code = slots[index].exit_code;
  }
  *out_child_count = child_count;
  return QST_OK;
}

static void qstCopyProcessIdentity(const SQstProcessSlot *slot,
                                   SQstProcessIdentity *out_identity) {
  out_identity->slot_id = slot->slot_id;
  out_identity->role = atomic_load_explicit(&slot->role, memory_order_acquire);
  out_identity->generation = atomic_load_explicit(&slot->generation,
                                                  memory_order_acquire);
  out_identity->exit_state = atomic_load_explicit(&slot->exit_state,
                                                  memory_order_acquire);
  out_identity->pid = slot->pid;
  out_identity->ppid = slot->ppid;
  out_identity->pgid = slot->pgid;
  out_identity->exit_code = slot->exit_code;
}

static EQstCode qstCollectDescendantsPostorder(
    const SQstProcessSlot *slots, uint32_t count, EQstRole parent_role,
    uint32_t parent_slot_id, uint32_t parent_generation, uint8_t *visit_state,
    SQstProcessIdentity *out_processes, uint32_t process_capacity,
    uint32_t *inout_process_count) {
  if (NULL == slots || NULL == visit_state || NULL == inout_process_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < count; ++index) {
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                memory_order_acquire);
    EQstCode code = QST_OK;

    if (slots[index].parent_role != parent_role ||
        slots[index].parent_slot_id != parent_slot_id ||
        slots[index].parent_generation != parent_generation ||
        atomic_load_explicit(&slots[index].role, memory_order_acquire) == 0) {
      continue;
    }
    if (visit_state[index] != 0) {
      return QST_E_SHM_CORRUPT;
    }
    visit_state[index] = 1;
    code = qstCollectDescendantsPostorder(
        slots, count,
        (EQstRole)atomic_load_explicit(&slots[index].role,
                                       memory_order_acquire),
        slots[index].slot_id,
        atomic_load_explicit(&slots[index].generation, memory_order_acquire),
        visit_state, out_processes, process_capacity, inout_process_count);
    if (code != QST_OK) {
      return code;
    }
    visit_state[index] = 2;
    /* Reaped parents remain traversable so launcher adoption can reach a
     * non-reaped grandchild that survived its supervisor. */
    if (state != QST_PROCESS_REAPED) {
      if (NULL != out_processes) {
        if (*inout_process_count >= process_capacity) {
          return QST_E_RESOURCE;
        }
        qstCopyProcessIdentity(&slots[index],
                               &out_processes[*inout_process_count]);
      }
      if (*inout_process_count == UINT32_MAX) {
        return QST_E_RESOURCE;
      }
      ++*inout_process_count;
    }
  }
  return QST_OK;
}

EQstCode qst_process_list_descendants_postorder(
    const SQstShmMapping *mapping, EQstRole root_role,
    uint32_t root_generation, SQstProcessIdentity *out_processes,
    uint32_t process_capacity, uint32_t *out_process_count) {
  SQstProcessSlot *slots = NULL;
  uint8_t *visit_state = NULL;
  uint32_t count = 0;
  uint32_t required_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_process_count ||
      (process_capacity != 0 && NULL == out_processes) ||
      root_role < QST_ROLE_LAUNCHER || root_role > QST_ROLE_AI_WORKER ||
      root_generation == 0 ||
      !qstParentIsCurrent(mapping, root_role, 0, root_generation)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstGetProcessRegion(mapping, &slots, &count);
  if (code != QST_OK) {
    return code;
  }
  visit_state = (uint8_t *)calloc(count, sizeof(*visit_state));
  if (NULL == visit_state) {
    return QST_E_RESOURCE;
  }
  code = qstCollectDescendantsPostorder(
      slots, count, root_role, 0, root_generation, visit_state, NULL, 0,
      &required_count);
  free(visit_state);
  if (code != QST_OK) {
    return code;
  }
  *out_process_count = required_count;
  if (required_count > process_capacity) {
    return QST_E_RESOURCE;
  }
  if (required_count == 0) {
    return QST_OK;
  }
  visit_state = (uint8_t *)calloc(count, sizeof(*visit_state));
  if (NULL == visit_state) {
    return QST_E_RESOURCE;
  }
  required_count = 0;
  code = qstCollectDescendantsPostorder(
      slots, count, root_role, 0, root_generation, visit_state, out_processes,
      process_capacity, &required_count);
  free(visit_state);
  if (code == QST_OK) {
    *out_process_count = required_count;
  }
  return code;
}

EQstCode qst_process_mark_exited_instance(SQstShmMapping *mapping,
                                          EQstRole role, uint32_t slot_id,
                                          uint32_t generation,
                                          int32_t exit_code) {
  SQstProcessSlot *slot = NULL;
  uint32_t expected_state = QST_PROCESS_RUNNING;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &slot->exit_state, &expected_state, QST_PROCESS_EXITED,
          memory_order_acq_rel, memory_order_acquire)) {
    expected_state = QST_PROCESS_STOP_REQUESTED;
    if (atomic_compare_exchange_strong_explicit(
            &slot->exit_state, &expected_state, QST_PROCESS_EXITED,
            memory_order_acq_rel, memory_order_acquire)) {
      slot->exit_code = exit_code;
      return QST_OK;
    }
    expected_state = QST_PROCESS_STOP_ACKED;
    if (!atomic_compare_exchange_strong_explicit(
            &slot->exit_state, &expected_state, QST_PROCESS_EXITED,
            memory_order_acq_rel, memory_order_acquire)) {
      return QST_E_STALE_OWNER;
    }
  }
  slot->exit_code = exit_code;
  return QST_OK;
}

EQstCode qst_process_mark_exited(SQstShmMapping *mapping, EQstRole role,
                                 uint32_t generation, int32_t exit_code) {
  return qst_process_mark_exited_instance(mapping, role, 0, generation,
                                          exit_code);
}

EQstCode qst_process_reap_generation_instance(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t slot_id,
    uint32_t generation, int32_t exit_code) {
  SQstProcessSlot *slot = NULL;
  uint32_t expected_state = 0;
  EQstCode code = qstFindProcess(mapping, role, slot_id, generation, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (slot->parent_role != parent_role ||
      slot->parent_slot_id != parent_slot_id ||
      slot->parent_generation != parent_generation ||
      !qstParentIsCurrent(mapping, parent_role, parent_slot_id,
                          parent_generation)) {
    return QST_E_STALE_OWNER;
  }
  expected_state = atomic_load_explicit(&slot->exit_state, memory_order_acquire);
  while (expected_state != QST_PROCESS_REAPED) {
    if (atomic_compare_exchange_weak_explicit(
            &slot->exit_state, &expected_state, QST_PROCESS_REAPED,
            memory_order_acq_rel, memory_order_acquire)) {
      slot->exit_code = exit_code;
      return QST_OK;
    }
  }
  return QST_E_STALE_OWNER;
}

EQstCode qst_process_reap_generation(SQstShmMapping *mapping,
                                     EQstRole parent_role,
                                     uint32_t parent_generation,
                                     EQstRole role, uint32_t generation,
                                     int32_t exit_code) {
  return qst_process_reap_generation_instance(
      mapping, parent_role, 0, parent_generation, role, 0, generation,
      exit_code);
}

EQstCode qst_process_reap_orphaned_generation_instance(
    SQstShmMapping *mapping, EQstRole adopter_role,
    uint32_t adopter_generation, EQstRole role, uint32_t slot_id,
    uint32_t generation, int32_t exit_code) {
  SQstProcessSlot *slot = NULL;
  uint32_t expected_state = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || adopter_role != QST_ROLE_LAUNCHER ||
      adopter_generation != mapping->header->launcher_generation ||
      role < QST_ROLE_MAIN || role > QST_ROLE_AI_WORKER || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstFindProcess(mapping, role, slot_id, generation, &slot);
  if (code != QST_OK || slot->parent_role == QST_ROLE_LAUNCHER) {
    return code != QST_OK ? code : QST_E_STALE_OWNER;
  }
  {
    SQstProcessSlot *slots = NULL;
    uint32_t count = 0;

    code = qstGetProcessRegion(mapping, &slots, &count);
    if (code != QST_OK) {
      return code;
    }
    for (uint32_t index = 0; index < count; ++index) {
      if (slots[index].parent_role == role &&
          slots[index].parent_slot_id == slot_id &&
          slots[index].parent_generation == generation &&
          atomic_load_explicit(&slots[index].role, memory_order_acquire) != 0 &&
          atomic_load_explicit(&slots[index].exit_state, memory_order_acquire) !=
              QST_PROCESS_REAPED) {
        return QST_E_STALE_OWNER;
      }
    }
  }
  expected_state = atomic_load_explicit(&slot->exit_state, memory_order_acquire);
  while (expected_state != QST_PROCESS_REAPED) {
    if (atomic_compare_exchange_weak_explicit(
            &slot->exit_state, &expected_state, QST_PROCESS_REAPED,
            memory_order_acq_rel, memory_order_acquire)) {
      slot->exit_code = exit_code;
      return QST_OK;
    }
  }
  return QST_E_STALE_OWNER;
}

EQstCode qst_process_reap_orphaned_generation(
    SQstShmMapping *mapping, EQstRole adopter_role,
    uint32_t adopter_generation, EQstRole role, uint32_t generation,
    int32_t exit_code) {
  return qst_process_reap_orphaned_generation_instance(
      mapping, adopter_role, adopter_generation, role, 0, generation,
      exit_code);
}
