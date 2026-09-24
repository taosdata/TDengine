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

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_BLOCK_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_BLOCK_H_

#include <stdatomic.h>
#include <stdint.h>

#include "qst_shm.h"

typedef struct {
  _Atomic uint32_t state;
  uint32_t payload_capacity;
  uint64_t owner_gid;
  uint32_t owner_generation;
  uint32_t payload_length;
  uint64_t next_block_offset;
  uint32_t payload_crc32c;
  uint32_t header_crc32c;
} SQstBlockHeader;

_Static_assert(sizeof(SQstBlockHeader) == 40,
               "unexpected block-header ABI size");

/* Allocates a writing block chain owned by one task or by the run (gid zero). */
EQstCode qst_block_alloc(SQstShmMapping *mapping, uint64_t owner_gid,
                         uint32_t owner_generation, uint64_t bytes,
                         SQstRef *out_ref);

/* Writes and seals a previously allocated chain, updating its CRC in place. */
EQstCode qst_block_write_seal(SQstShmMapping *mapping, SQstRef *inout_ref,
                              const void *source, uint64_t bytes);

/* Aborts a complete, unsealed chain that is still owned by the caller. */
EQstCode qst_block_abort(SQstShmMapping *mapping, SQstRef ref,
                         uint64_t owner_gid);

/* Checks ownership, chain integrity, payload CRCs, and the reference CRC. */
EQstCode qst_block_validate(const SQstShmMapping *mapping, SQstRef ref,
                            uint64_t expected_owner_gid);

/* Copies a logical byte range from a validated sealed chain without pointers. */
EQstCode qst_block_copy(const SQstShmMapping *mapping, SQstRef ref,
                        uint64_t expected_owner_gid, uint64_t source_offset,
                        void *destination, uint64_t bytes);

/* Calculates SHA-256 of one validated sealed block chain without local copies. */
EQstCode qst_block_sha256(const SQstShmMapping *mapping, SQstRef ref,
                          uint64_t expected_owner_gid,
                          SQstDigest256 *out_digest);

/* Releases a sealed chain after its owner has reached a durable terminal state. */
EQstCode qst_block_release(SQstShmMapping *mapping, SQstRef ref,
                           uint64_t owner_gid);

/* Validates all non-empty sealed chains, then releases them as one owner-only batch. */
EQstCode qst_block_release_many(SQstShmMapping *mapping, const SQstRef *refs,
                                uint32_t ref_count, uint64_t owner_gid);

/* Counts free blocks in the current shared block region for flow control. */
EQstCode qst_block_free_count(const SQstShmMapping *mapping,
                              uint32_t *out_free_count);

/* Reclaims run-owned blocks left by a terminated older generation. */
EQstCode qst_block_reclaim_run_owned(SQstShmMapping *mapping,
                                     uint32_t current_generation,
                                     uint32_t *out_reclaimed_count);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_BLOCK_H_
