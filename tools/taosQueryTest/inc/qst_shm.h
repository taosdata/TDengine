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

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SHM_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SHM_H_

#include <stdatomic.h>
#include <stdint.h>

#include "qst_types.h"

typedef struct {
  uint64_t region_offset;
  uint64_t region_bytes;
  uint32_t element_bytes;
  uint32_t element_count;
} SQstRegion;

typedef struct {
  uint8_t magic[8];
  uint32_t format_version;
  uint32_t header_bytes;
  uint32_t endian_tag;
  uint32_t page_bytes;
  uint64_t total_bytes;
  uint64_t run_id;
  _Atomic uint32_t run_state;
  _Atomic uint32_t stop_generation;
  _Atomic uint32_t stop_reason;
  /* Exact number of blocks currently published as FREE. */
  _Atomic uint32_t free_block_count;
  /* Exact number of task-ledger entries currently published as free. */
  _Atomic uint32_t free_task_count;
  _Atomic uint64_t next_message_id;
  _Atomic uint64_t next_gid;
  _Atomic uint64_t next_record_seq;
  uint32_t launcher_generation;
  _Atomic uint32_t main_generation;
  _Atomic uint64_t generated_task_count;
  _Atomic uint64_t persisted_task_count;
  _Atomic uint64_t isolated_task_count;
  _Atomic uint64_t execution_success_count;
  _Atomic uint64_t execution_error_count;
  _Atomic uint64_t execution_timeout_count;
  _Atomic uint64_t execution_cancelled_count;
  _Atomic uint64_t result_memory_count;
  _Atomic uint64_t result_unavailable_count;
  _Atomic uint64_t verdict_passed_count;
  _Atomic uint64_t verdict_failed_count;
  _Atomic uint64_t verdict_unverified_count;
  uint32_t process_slot_bytes;
  uint32_t task_entry_bytes;
  uint32_t ai_action_entry_bytes;
  uint32_t queue_slot_bytes;
  uint32_t block_header_bytes;
  SQstDigest256 resource_profile_digest;
  SQstDigest256 dimension_catalog_digest;
  SQstDigest256 dependency_lock_digest;
  SQstDigest256 tdengine_version_digest;
  SQstRegion process_region;
  SQstRegion task_region;
  SQstRegion ai_action_region;
  SQstRegion queue_region[4];
  SQstRegion block_region;
  SQstRegion generation_state_region;
  uint32_t dimension_catalog_version;
  int32_t launcher_pid;
  uint32_t execution_pair_count;
  uint32_t pair_async_window;
  uint32_t judge_worker_count;
  uint32_t statement_timeout_seconds;
  uint32_t statement_retry_count;
  uint32_t header_crc32c;
  uint8_t reserved0[48];
} SQstShmHeader;

_Static_assert(sizeof(SQstShmHeader) % QST_CACHELINE_BYTES == 0,
               "shared-memory header must end on a cache-line boundary");
_Static_assert(sizeof(SQstShmHeader) == 640,
               "unexpected shared-memory header ABI size");
_Static_assert(sizeof(SQstGenerationState) % QST_CACHELINE_BYTES == 0,
               "generation state must be cache-line aligned");

typedef struct SQstRunConfig SQstRunConfig;

typedef struct {
  int32_t shm_fd;
  int32_t queue_eventfd[4];
  void *base;
  uint64_t bytes;
  SQstShmHeader *header;
} SQstShmMapping;

/* Creates one unlinked disk-backed shared mapping and its initial ABI layout. */
EQstCode qst_shm_create(const SQstRunConfig *config, SQstShmMapping *out_mapping);

/* Maps and validates an inherited mapping descriptor, duplicating it locally. */
EQstCode qst_shm_map_validate(int32_t shm_fd, uint64_t run_id,
                              SQstShmMapping *out_mapping);

/* Validates that a non-empty reference stays inside the block region. */
EQstCode qst_shm_validate_ref(const SQstShmMapping *mapping, SQstRef ref);

/* Publishes the immutable compiled SQL catalog identity before child startup. */
EQstCode qst_shm_set_dimension_catalog(SQstShmMapping *mapping,
                                       uint32_t catalog_version,
                                       SQstDigest256 catalog_digest);
/* Freezes the startup TDengine readiness digest before manager processes fork. */
EQstCode qst_shm_set_tdengine_version_digest(SQstShmMapping *mapping,
                                             SQstDigest256 version_digest);

/* Releases the mapping and its owned shared-mapping file descriptor. */
EQstCode qst_shm_unmap(SQstShmMapping *mapping);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_SHM_H_
