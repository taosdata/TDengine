/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_AI_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_AI_H_

#include "qst_task.h"

EQstCode qst_ai_precheck(SQstShmMapping *mapping, uint64_t gid,
                         EQstAiEntry entry, SQstRef prompt_ref,
                         SQstRef material_manifest_ref,
                         SQstDigest256 *out_action_id);
EQstCode qst_ai_run_action(SQstShmMapping *mapping, SQstDigest256 action_id,
                           uint32_t worker_generation);
EQstCode qst_ai_publish_result(SQstShmMapping *mapping,
                               SQstDigest256 action_id, SQstRef analysis_ref,
                               EQstCode action_code,
                               uint32_t worker_generation);
/* Returns the owner gid frozen in one action-ledger entry. */
EQstCode qst_ai_get_action_gid(const SQstShmMapping *mapping,
                               SQstDigest256 action_id, uint64_t *out_gid);
/* Copies a completed action's analysis block into a caller-owned buffer. */
EQstCode qst_ai_copy_action_analysis(const SQstShmMapping *mapping,
                                     SQstDigest256 action_id,
                                     uint8_t **out_bytes,
                                     uint32_t *out_length);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_AI_H_
