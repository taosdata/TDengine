/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RUNNER_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RUNNER_H_

#include <stdint.h>

#include "qst_shm.h"

/* Local-only parsed configuration. It must never be copied into shared memory. */
struct SQstRunConfig {
  uint64_t run_id;
  uint64_t random_seed;
  uint32_t mode;
  uint32_t focus;
  uint32_t target_selection_bits;
  uint32_t statement_timeout_seconds;
  uint32_t statement_retry_count;
  /* Explicit execution caps; zero keeps the resource-derived value. */
  uint32_t execution_pair_count_override;
  uint32_t judge_worker_count_override;
  uint32_t queue_capacity_override;
  uint32_t pair_async_window_cap;
  uint32_t random_builder_count_override;
  uint64_t statement_limit;
  uint64_t duration_limit_minutes;
  uint64_t shm_bytes;
  uint32_t process_slot_count;
  uint32_t task_entry_count;
  uint32_t ai_action_entry_count;
  uint32_t queue_capacity;
  uint32_t block_bytes;
  uint32_t block_count;
  uint32_t execution_pair_count;
  uint32_t pair_async_window;
  uint32_t judge_worker_count;
  uint64_t pending_quota_bytes;
  uint64_t log_rotation_bytes;
  SQstDigest256 config_digest;
  SQstDigest256 resource_profile_digest;
  SQstDigest256 dependency_lock_digest;
  SQstDigest256 tdengine_version_digest;
  const char *tool_root;
  const char *result_dir;
  const char *generate_output_path;
};

EQstCode qst_launcher_run(const SQstRunConfig *config);
EQstCode qst_main_run(SQstShmMapping *mapping, const SQstRunConfig *config);
EQstCode qst_generator_run(SQstShmMapping *mapping, uint32_t generation);
EQstCode qst_exec_manager_run(SQstShmMapping *mapping, uint32_t generation);
EQstCode qst_pair_supervisor_run(SQstShmMapping *mapping,
                                 uint32_t generation, uint32_t pair_slot);
EQstCode qst_judge_manager_run(SQstShmMapping *mapping,
                               uint32_t generation);
EQstCode qst_judge_worker_run(SQstShmMapping *mapping, uint32_t generation,
                              uint32_t worker_slot);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_RUNNER_H_
