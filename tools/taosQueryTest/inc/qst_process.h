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

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROCESS_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROCESS_H_

#include <stdatomic.h>
#include <stdint.h>

#include "qst_shm.h"

#define QST_PROCESS_SLOT_AUTO UINT32_MAX

/* One fixed process-ledger slot. Role zero marks an unused slot. */
typedef struct {
  _Atomic uint32_t role;
  _Atomic uint32_t generation;
  int32_t pid;
  int32_t ppid;
  int32_t pgid;
  uint32_t slot_id;
  uint32_t parent_role;
  uint32_t parent_slot_id;
  uint32_t parent_generation;
  _Atomic uint64_t flow_seq;
  _Atomic uint64_t flow_mono_ns;
  _Atomic uint64_t business_seq;
  _Atomic uint64_t business_mono_ns;
  _Atomic uint64_t current_gid;
  _Atomic uint64_t deadline_mono_ns;
  _Atomic uint32_t stop_generation;
  _Atomic uint32_t exit_state;
  int32_t exit_code;
  _Atomic int32_t last_error_code;
  SQstRef last_error_ref;
  _Atomic uint64_t flow_observed_state;
  _Atomic uint32_t flow_stage;
  _Atomic uint32_t flow_state_kind;
  uint8_t reserved[56];
} SQstProcessSlot;

/* Immutable identity snapshot of one non-reaped direct child process. */
typedef struct {
  uint32_t slot_id;
  uint32_t role;
  uint32_t generation;
  uint32_t exit_state;
  int32_t pid;
  int32_t ppid;
  int32_t pgid;
  int32_t exit_code;
} SQstProcessIdentity;

_Static_assert(sizeof(SQstProcessSlot) % QST_CACHELINE_BYTES == 0,
               "process slots must start on cache-line boundaries");
_Static_assert(sizeof(SQstProcessSlot) == 192,
               "unexpected process-slot ABI size");

EQstCode qst_process_register(SQstShmMapping *mapping, EQstRole parent_role,
                               uint32_t parent_generation, EQstRole role,
                               int32_t pid, int32_t pgid,
                               uint32_t *out_generation);
/* Registers one stable role-local instance; singletons use slot_id zero. */
EQstCode qst_process_register_instance(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t slot_id, int32_t pid,
    int32_t pgid, uint32_t *out_generation);
/* Registers an instance and returns the concrete slot selected for AUTO. */
EQstCode qst_process_register_instance_ex(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t requested_slot_id,
    int32_t pid, int32_t pgid, uint32_t *out_slot_id,
    uint32_t *out_generation);
EQstCode qst_process_confirm_flow(SQstShmMapping *mapping, EQstRole role,
                                  uint32_t generation, EQstFlowStage flow_stage,
                                  EQstFlowStateKind flow_state_kind,
                                  uint64_t flow_observed_state,
                                  uint64_t current_gid,
                                  uint64_t deadline_mono_ns);
EQstCode qst_process_confirm_flow_instance(
    SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, EQstFlowStage flow_stage,
    EQstFlowStateKind flow_state_kind, uint64_t flow_observed_state,
    uint64_t current_gid, uint64_t deadline_mono_ns);
/* Records a real state transition without extending the active-task deadline. */
EQstCode qst_process_record_business(SQstShmMapping *mapping, EQstRole role,
                                     uint32_t generation, uint64_t current_gid,
                                     uint64_t deadline_mono_ns);
EQstCode qst_process_record_business_instance(
    SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, uint64_t current_gid, uint64_t deadline_mono_ns);
/* A direct parent requests a monotonically newer cooperative stop. */
EQstCode qst_process_request_stop(SQstShmMapping *mapping,
                                  EQstRole parent_role,
                                  uint32_t parent_generation, EQstRole role,
                                  uint32_t generation,
                                  uint32_t stop_generation);
EQstCode qst_process_request_stop_instance(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t slot_id,
    uint32_t generation, uint32_t stop_generation);
/* The target process acknowledges the exact stop generation it received. */
EQstCode qst_process_ack_stop(SQstShmMapping *mapping, EQstRole role,
                              uint32_t generation,
                              uint32_t stop_generation);
EQstCode qst_process_ack_stop_instance(SQstShmMapping *mapping,
                                       EQstRole role, uint32_t slot_id,
                                       uint32_t generation,
                                       uint32_t stop_generation);
/* Reads the caller-owned cooperative-stop state without exposing a slot. */
EQstCode qst_process_get_stop_generation(const SQstShmMapping *mapping,
                                         EQstRole role, uint32_t generation,
                                         uint32_t *out_stop_generation,
                                         uint32_t *out_exit_state);
EQstCode qst_process_get_stop_generation_instance(
    const SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, uint32_t *out_stop_generation,
    uint32_t *out_exit_state);
/* Publishes the reason for a child-owned abnormal exit before it terminates. */
EQstCode qst_process_record_error_instance(
    SQstShmMapping *mapping, EQstRole role, uint32_t slot_id,
    uint32_t generation, int32_t error_code);
/* Reads the abnormal-exit reason published for one live process generation. */
EQstCode qst_process_get_error_instance(const SQstShmMapping *mapping,
                                        EQstRole role, uint32_t slot_id,
                                        uint32_t generation,
                                        int32_t *out_error_code);
/* Finds a live process generation by the registered role and OS process ID. */
EQstCode qst_process_find_generation(const SQstShmMapping *mapping,
                                     EQstRole role, int32_t pid,
                                     uint32_t *out_generation);
/* Lists the current direct children in stable process-slot order. */
EQstCode qst_process_list_direct_children(
    const SQstShmMapping *mapping, EQstRole parent_role,
    uint32_t parent_generation, SQstProcessIdentity *out_children,
    uint32_t child_capacity, uint32_t *out_child_count);
/* Lists non-reaped descendants in postorder so callers stop leaves first. */
EQstCode qst_process_list_descendants_postorder(
    const SQstShmMapping *mapping, EQstRole root_role,
    uint32_t root_generation, SQstProcessIdentity *out_processes,
    uint32_t process_capacity, uint32_t *out_process_count);
EQstCode qst_process_mark_exited(SQstShmMapping *mapping, EQstRole role,
                                 uint32_t generation, int32_t exit_code);
EQstCode qst_process_mark_exited_instance(SQstShmMapping *mapping,
                                          EQstRole role, uint32_t slot_id,
                                          uint32_t generation,
                                          int32_t exit_code);
EQstCode qst_process_reap_generation(SQstShmMapping *mapping,
                                     EQstRole parent_role,
                                     uint32_t parent_generation,
                                     EQstRole role, uint32_t generation,
                                     int32_t exit_code);
EQstCode qst_process_reap_generation_instance(
    SQstShmMapping *mapping, EQstRole parent_role, uint32_t parent_slot_id,
    uint32_t parent_generation, EQstRole role, uint32_t slot_id,
    uint32_t generation, int32_t exit_code);
/* The launcher reaps an adopted child after it has been stopped and waited. */
EQstCode qst_process_reap_orphaned_generation(
    SQstShmMapping *mapping, EQstRole adopter_role,
    uint32_t adopter_generation, EQstRole role, uint32_t generation,
    int32_t exit_code);
/* Reaps one launcher-adopted non-singleton process instance after its children. */
EQstCode qst_process_reap_orphaned_generation_instance(
    SQstShmMapping *mapping, EQstRole adopter_role,
    uint32_t adopter_generation, EQstRole role, uint32_t slot_id,
    uint32_t generation, int32_t exit_code);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_PROCESS_H_
