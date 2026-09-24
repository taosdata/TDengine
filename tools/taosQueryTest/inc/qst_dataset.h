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

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_DATASET_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_DATASET_H_

#include <stdint.h>

#include "qst_runner.h"

enum {
  QST_FIXTURE_PLANNED = 1,
  QST_FIXTURE_READY = 2,
};

enum {
  QST_TYPE_ROLE_COLUMN = 1U << 0,
  QST_TYPE_ROLE_TAG = 1U << 1,
  QST_TYPE_ROLE_PSEUDO = 1U << 2,
  QST_TYPE_ROLE_ALIAS = 1U << 3,
};

enum {
  QST_OBJECT_FLAG_PRIMARY_TIMESTAMP = 1U << 0,
  QST_OBJECT_FLAG_TAG = 1U << 1,
};

typedef struct {
  uint32_t type_id;
  uint32_t roles;
  const char *canonical_name;
  const char *ddl_type;
} SQstLocalTypeDescriptor;

typedef struct {
  uint64_t run_id;
  SQstDigest256 ds_fingerprint;
  SQstDigest256 fixture_manifest_digest;
  SQstRef object_directory_ref;
  SQstRef fixture_manifest_ref;
} SQstDatasetInfo;

/* Immutable header for QSTOBJ10 and QSTFIX10 shared block payloads. */
typedef struct {
  uint8_t magic[8];
  uint32_t format_version;
  uint32_t header_bytes;
  uint32_t entry_bytes;
  uint32_t entry_count;
  SQstDigest256 content_sha256;
  uint32_t crc32c;
  uint8_t reserved[20];
} SQstManifestHeader;

typedef struct {
  uint64_t object_id;
  uint64_t parent_object_id;
  uint32_t target;
  uint32_t object_kind;
  uint32_t ordinal;
  uint32_t flags;
  SQstRef qualified_name_ref;
  SQstRef schema_ref;
  SQstDigest256 descriptor_sha256;
  uint8_t reserved[16];
} SQstObjectDescriptor;

typedef struct {
  uint64_t registry_id;
  uint32_t target;
  uint32_t state;
  SQstRef object_ref;
  SQstRef expected_schema_ref;
  SQstRef verification_ref;
  SQstDigest256 fixture_sha256;
  uint8_t reserved[16];
} SQstFixtureDescriptor;

_Static_assert(sizeof(SQstManifestHeader) == 80,
               "unexpected manifest-header ABI size");
_Static_assert(sizeof(SQstObjectDescriptor) == 112,
               "unexpected object-descriptor ABI size");
_Static_assert(sizeof(SQstFixtureDescriptor) == 112,
               "unexpected fixture-descriptor ABI size");

EQstCode qst_dataset_prepare(SQstShmMapping *mapping,
                             const SQstRunConfig *config,
                             SQstDatasetInfo *out_dataset);
/* Parses an external provider TCP port as a strict decimal value in 1..65535. */
EQstCode qst_dataset_parse_external_port(const char *port,
                                         uint16_t *out_port);
/* Prepares the immutable fixture for an already-owned main generation. */
EQstCode qst_dataset_prepare_for_generation(SQstShmMapping *mapping,
                                            const SQstRunConfig *config,
                                            uint32_t generation,
                                            SQstDatasetInfo *out_dataset);
/* Recovers run-owned dataset blocks before a new main generation prepares. */
EQstCode qst_dataset_recover_stale_generation(SQstShmMapping *mapping,
                                              uint32_t generation);
/* Removes only this run's local fixture and releases its run-owned blocks. */
EQstCode qst_dataset_cleanup(SQstShmMapping *mapping,
                             const SQstRunConfig *config,
                             SQstDatasetInfo *inout_dataset);
EQstCode qst_dataset_get_object(const SQstShmMapping *mapping,
                                const SQstDatasetInfo *dataset,
                                uint32_t target, uint32_t object_kind,
                                uint32_t object_index, SQstRef *out_object_ref);
/*
 * Resolves one immutable object descriptor by its globally unique object ID.
 * The directory is validated before the descriptor is returned.
 */
EQstCode qst_dataset_get_object_by_id(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint64_t object_id,
                                      SQstObjectDescriptor *out_object);
/*
 * Returns one object-directory entry in its immutable traversal order.  Callers
 * use this to derive role-specific candidate sets without assuming object IDs
 * are dense or ordered.
 */
EQstCode qst_dataset_get_object_at(const SQstShmMapping *mapping,
                                   const SQstDatasetInfo *dataset,
                                   uint32_t directory_index,
                                   SQstObjectDescriptor *out_object);
/* Returns the validated immutable object-directory entry count. */
EQstCode qst_dataset_get_object_count(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint32_t *out_count);
/* Copies the complete validated object directory into caller-owned storage. */
EQstCode qst_dataset_copy_object_directory(
    const SQstShmMapping *mapping, const SQstDatasetInfo *dataset,
    SQstObjectDescriptor *out_objects, uint32_t object_capacity,
    uint32_t *out_count);
/*
 * Resolves an object within its parent namespace. Column and tag callers must
 * provide the parent table ID to avoid ordinal ambiguity across tables.
 */
EQstCode qst_dataset_get_child_object(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint32_t target,
                                      uint64_t parent_object_id,
                                      uint32_t object_kind,
                                      uint32_t object_index,
                                      SQstObjectDescriptor *out_object);
/* Copies a validated object name from its run-owned immutable block chain. */
EQstCode qst_dataset_copy_object_name(const SQstShmMapping *mapping,
                                      const SQstObjectDescriptor *object,
                                      char *out_name, uint64_t out_bytes);
EQstCode qst_dataset_validate_binding(const SQstShmMapping *mapping,
                                      const SQstDatasetInfo *dataset,
                                      uint64_t gid, uint32_t target,
                                      SQstRef binding_ref);
/* Returns the immutable registry derived from the build-time taos.h ABI. */
EQstCode qst_dataset_local_type_registry(const SQstLocalTypeDescriptor **out_types,
                                         uint32_t *out_type_count);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_DATASET_H_
