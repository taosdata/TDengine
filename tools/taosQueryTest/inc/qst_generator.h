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

#ifndef TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_GENERATOR_H_
#define TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_GENERATOR_H_

#include <stdint.h>

#include "qst_task.h"

enum {
  QST_GENERATOR_SQL_BYTES = 65536,
  /* Keep generated-path evidence in the large buffer, but do not submit a
   * deeply composed malformed statement that exceeds this execution budget. */
  QST_GENERATOR_EXECUTABLE_SQL_BYTES = 4096,
  QST_GENERATOR_RANDOM_ARRAY_SIZE = 100,
  QST_GENERATOR_DEFAULT_QUERY_DEPTH = 3,
  QST_GENERATOR_DEFAULT_EXPRESSION_DEPTH = 10,
  QST_GENERATOR_MAX_RECURSION_DEPTH = 10,
  QST_GENERATOR_MAX_LOOP_ITERATIONS = 10,
  /* RANDOM publication preserves sequence order across this bounded width. */
  QST_GENERATOR_MAX_RANDOM_BUILDERS = 16U,
  QST_GENERATOR_SQL_DIMENSION_LOOP_ITERATIONS = 10,
  QST_GENERATOR_MAX_SCOPE_DEPTH = QST_GENERATOR_MAX_RECURSION_DEPTH + 2,
  QST_GENERATOR_MAX_SCOPE_TABLES = 64,
  QST_GENERATOR_MAX_ALIAS_BYTES = 193,
  /* Each selected target publishes one verified runtime-table query before its
   * unrestricted deterministic DFS begins. */
  QST_GENERATOR_FULL_COVERAGE_SEED_COUNT = 1,
  QST_GENERATION_RECORD_VERSION = 3,
  QST_GENERATOR_CLASS_NORMAL = 1,
  QST_GENERATOR_CLASS_ABNORMAL = 2,
  /* DS 4.4.3 default fixed-point target profile; weight_scale is 1,000,000. */
  QST_GENERATOR_TARGET_WEIGHT_TD = 250000,
  QST_GENERATOR_TARGET_WEIGHT_MYSQL = 750000,
  QST_GENERATOR_TARGET_WEIGHT_POSTGRESQL = 750000,
  QST_GENERATOR_TARGET_WEIGHT_INFLUX = 750000,
  QST_GENERATOR_DIALECT_MASK_TDENGINE = 1U << 0,
  QST_GENERATOR_DIALECT_MASK_MYSQL = 1U << 1,
  QST_GENERATOR_DIALECT_MASK_POSTGRESQL = 1U << 2,
  QST_GENERATOR_DIALECT_MASK_INFLUXQL = 1U << 3,
  QST_GENERATOR_DIALECT_MASK_ALL =
      QST_GENERATOR_DIALECT_MASK_TDENGINE |
      QST_GENERATOR_DIALECT_MASK_MYSQL |
      QST_GENERATOR_DIALECT_MASK_POSTGRESQL |
      QST_GENERATOR_DIALECT_MASK_INFLUXQL,
};

typedef enum {
  QST_GENERATOR_VALUE_ATOMIC = 1,
  QST_GENERATOR_VALUE_FIXED_TOKEN = 2,
  QST_GENERATOR_VALUE_CALL_DIMENSION = 3,
  QST_GENERATOR_VALUE_CALL_SELF = 4,
  QST_GENERATOR_VALUE_COMPOSE = 5,
} EQstGeneratorValueKind;

typedef enum {
  QST_GENERATOR_CALL_REQUIRED = 1,
  QST_GENERATOR_CALL_OPTIONAL = 2,
  QST_GENERATOR_CALL_LOOP = 3,
} EQstGeneratorCallKind;

/* A child call can delimit a query block or bind the table it just named. */
typedef enum {
  QST_GENERATOR_SCOPE_NONE = 0,
  QST_GENERATOR_SCOPE_QUERY_BLOCK = 1,
  QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS = 2,
} EQstGeneratorScopeAction;

typedef enum {
  QST_GENERATOR_RETURN_NON_EMPTY = 1,
  QST_GENERATOR_RETURN_EMPTY_RESULT = 2,
  QST_GENERATOR_RETURN_STOP_CURRENT = 3,
} EQstGeneratorReturnState;

/* Explains the non-terminal RANDOM publication decision for phase profiling. */
typedef enum {
  QST_RANDOM_PUBLISH_GATE_READY = 1,
  QST_RANDOM_PUBLISH_GATE_TASK_LEDGER_EMPTY = 2,
  QST_RANDOM_PUBLISH_GATE_ACTIVATION_WINDOW_FULL = 3,
  QST_RANDOM_PUBLISH_GATE_STOP = 4,
} EQstRandomPublishGate;

/* Immutable description of one direct child invocation in a catalog value. */
typedef struct {
  uint32_t fragment_id;
  uint32_t call_kind;
  uint32_t recursion_cost;
  uint32_t min_iterations;
  uint32_t typical_iterations;
  uint32_t max_iterations;
  uint32_t scope_action;
} SQstGeneratorChildCall;

typedef enum {
  QST_GENERATOR_STEP_CALL = 1,
  QST_GENERATOR_STEP_INLINE_VALUE = 2,
} EQstGeneratorStepKind;

/* Runtime bindings are resolved from the immutable dataset object directory. */
typedef enum {
  QST_RUNTIME_OBJECT_NONE = 0,
  QST_RUNTIME_OBJECT_SOURCE = 1,
  QST_RUNTIME_OBJECT_DATABASE = 2,
  QST_RUNTIME_OBJECT_TABLE = 3,
  QST_RUNTIME_OBJECT_COLUMN = 4,
  QST_RUNTIME_OBJECT_TABLE_REFERENCE = 5,
  QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX = 6,
  QST_RUNTIME_OBJECT_COLUMN_REFERENCE = 7,
  QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK = 8,
  QST_RUNTIME_OBJECT_QUALIFIED_TBNAME_PSEUDOCOLUMN = 9,
  QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER = 10,
} EQstRuntimeObjectKind;

/* One immutable SQL-generation step in a composed value. */
typedef struct {
  uint32_t kind;
  uint32_t value_id;
  const char *text;
  SQstGeneratorChildCall child_call;
} SQstGeneratorStep;

typedef struct {
  uint32_t value_id;
  uint32_t kind;
  uint32_t generation_class;
  uint32_t return_state;
  uint64_t weight;
  uint32_t child_count;
  uint32_t runtime_object_kind;
  const SQstGeneratorChildCall *children;
  const char *text;
  uint32_t step_count;
  const SQstGeneratorStep *steps;
  /* Zero means every ABI-supported dialect; nonzero is a four-dialect mask. */
  uint32_t dialect_mask;
} SQstGeneratorValue;

typedef struct {
  uint32_t fragment_id;
  uint64_t weight;
  uint32_t value_count;
  const SQstGeneratorValue *values;
} SQstGeneratorFragment;

typedef struct {
  uint32_t version;
  uint32_t root_fragment_id;
  uint32_t fragment_count;
  const SQstGeneratorFragment *fragments;
  SQstDigest256 digest;
} SQstGeneratorCatalog;

typedef struct {
  uint64_t state;
  uint64_t random_array[QST_GENERATOR_RANDOM_ARRAY_SIZE];
  uint32_t random_index;
  uint64_t loop_round;
  uint64_t lazy_seed;
  uint64_t lazy_array_state;
  uint32_t lazy_initialized;
} SQstGeneratorRandomState;

/*
 * Read-only object-directory snapshot used while resolving RUNTIME_OBJECT
 * values.  No pointer into shared memory is retained by a generated SQL.
 */
typedef struct {
  const SQstShmMapping *mapping;
  SQstRef object_directory_ref;
  SQstRef fixture_manifest_ref;
  uint32_t target;
  uint32_t dialect;
  const void *objects;
  uint32_t object_count;
} SQstGeneratorRuntimeContext;

/* One generator-local occurrence in the current depth-first construction. */
typedef struct {
  uint32_t occurrence_id;
  uint32_t parent_occurrence_id;
  uint32_t fragment_id;
  uint32_t value_id;
  uint32_t child_index;
  uint32_t loop_iteration;
  uint32_t depth;
  uint32_t return_state;
  uint32_t runtime_object_kind;
  uint32_t runtime_object_ordinal;
  uint64_t runtime_object_id;
  uint32_t text_offset;
  uint32_t text_length;
} SQstGenerationOccurrence;

/* Fixed prefix of a GenerationRecord block payload containing SQL only. */
typedef struct {
  uint32_t format_version;
  uint32_t header_bytes;
  uint32_t target;
  uint32_t dialect;
  uint64_t generation_seq;
  uint64_t weight_profile_version;
  uint32_t sql_length;
  uint8_t contains_abnormal;
  uint8_t contains_unsupported_fallback;
  uint8_t reserved0[2];
  SQstDigest256 catalog_digest;
  SQstDigest256 dataset_digest;
  SQstDigest256 sql_sha256;
} SQstGenerationRecordHeader;

typedef struct {
  uint32_t choice_count;
  uint32_t occurrence_count;
  uint32_t choice_capacity;
  uint32_t occurrence_capacity;
  uint32_t *choice_cardinalities;
  uint32_t *selected_value_ids;
  SQstGenerationOccurrence *occurrences;
  uint32_t sql_length;
  uint8_t contains_abnormal;
  uint8_t contains_unsupported_fallback;
  uint8_t reserved0[2];
  char sql[QST_GENERATOR_SQL_BYTES];
  SQstDigest256 sql_sha256;
} SQstGeneratedSql;

typedef struct {
  uint32_t target;
  uint32_t mode;
  uint32_t focus;
  uint32_t dialect;
  uint64_t generation_seq;
  uint64_t weight_profile_version;
  uint32_t catalog_version;
  uint32_t reserved0;
  SQstDigest256 catalog_digest;
  SQstDigest256 dataset_digest;
} SQstGeneratorPublishInput;

/* One process-local RANDOM SQL prepared for a specific uncommitted sequence. */
typedef struct {
  SQstGeneratorPublishInput input;
  SQstGeneratedSql sql;
  /* Shared resources prepared by a RANDOM builder before ordered publish. */
  uint64_t prepared_gid;
  SQstRef prepared_sql_ref;
  SQstRef prepared_generation_record_ref;
  SQstDigest256 prepared_generation_record_sha256;
  uint32_t resources_prepared;
  uint32_t reserved0;
} SQstGeneratorRandomPending;

/*
 * Generator-private FULL state retained while shared queues apply backpressure.
 * The choice path is process-local acceleration state only: shared cursors remain
 * authoritative and a new generator can reconstruct the path after a restart.
 */
typedef struct {
  SQstGeneratedSql sql;
  uint64_t cursor_next_path_ordinal;
  uint64_t full_path_next_ordinal;
  /* Counts path replays needed to rebuild this process-local cache. */
  uint64_t full_path_restore_count;
  uint32_t target_index;
  uint32_t valid;
  uint32_t full_path_target_index;
  uint32_t full_path_ready;
  uint32_t full_path_exhausted;
  uint32_t full_path_choice_count;
  uint32_t full_path_choice_capacity;
  uint32_t *full_path_choices;
  const SQstGeneratorCatalog *validated_catalog;
  SQstDigest256 validated_catalog_digest;
  const SQstShmMapping *runtime_mapping;
  SQstRef runtime_object_directory_ref;
  SQstRef runtime_fixture_manifest_ref;
  void *runtime_objects;
  uint32_t runtime_object_count;
  /* Opaque process-local catalog and runtime lookup cache. It is never
   * written to shared memory and is discarded when a pending path is freed. */
  void *build_runtime;
} SQstGeneratorFullPending;

/*
 * Generator-private DFS state for one unpublished FULL successor. It holds
 * only the choices needed to construct that successor; it never owns shared
 * cursor state, task-ledger resources, or a runtime lookup cache.
 */
typedef struct {
  uint64_t next_path_ordinal;
  uint32_t target_index;
  uint32_t exhausted;
  uint32_t choice_count;
  uint32_t choice_capacity;
  uint32_t *choice_cardinalities;
  uint32_t full_path_choice_count;
  uint32_t full_path_choice_capacity;
  uint32_t *full_path_choices;
} SQstGeneratorFullPathSeed;

/*
 * Generator-process-local immutable state for RANDOM publication.  The first
 * use validates the catalog and copies the sealed object directory; later SQL
 * builds reuse that verified copy until the published snapshot identity changes.
 */
typedef struct {
  const SQstGeneratorCatalog *validated_catalog;
  SQstDigest256 validated_catalog_digest;
  const SQstShmMapping *runtime_mapping;
  SQstDigest256 runtime_dataset_digest;
  SQstDigest256 runtime_fixture_manifest_digest;
  SQstRef runtime_object_directory_ref;
  SQstRef runtime_fixture_manifest_ref;
  void *runtime_objects;
  uint32_t runtime_object_count;
  /* Opaque, process-local index over the immutable copied object directory. */
  void *runtime_object_index;
  /* Opaque sorted catalog indexes shared by every RANDOM build in this process. */
  void *catalog_value_index;
  uint32_t catalog_value_count;
  uint32_t catalog_has_runtime_values;
  uint32_t catalog_runtime_values_prepared;
  void *catalog_fragment_index;
  /* One byte per sorted catalog value and supported recursion depth. */
  void *catalog_value_eligibility;
  /* Per-prefetch-slot mutable buildability state.  It remains private to one
   * generator thread and is invalidated by an epoch on each new SQL path. */
  void *buildable_fragments;
  void *buildable_fragment_epochs;
  void *buildable_values;
  void *buildable_value_epochs;
  /* Opaque immutable-catalog lookup cache owned by this private runtime. */
  void *buildable_lookup_cache;
  /* Per-scope candidate arrays are reusable builder-local capacity. Their
   * contents are invalidated with the current buildability epoch. */
  void *runtime_candidate_lists[QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER + 1U];
  uint32_t runtime_candidate_capacities[
      QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER + 1U];
  /* Builder-thread-local plan frames reused across deterministic SQL paths. */
  void *plan_scratch;
  /* Builder-thread-local eligible values reused by dynamic FRAG selection. */
  void *plan_selection_scratch;
  uint32_t plan_selection_capacity;
  uint32_t buildability_epoch;
  /* RANDOM publication persists SQL and its digest only. The pipeline can
   * skip FULL-enumeration metadata while preserving the generated SQL path. */
  uint32_t disable_path_metadata;
  /* Test-only mode: execute the production RANDOM walker but retain choices
   * and runtime validation without materializing SQL text or its digest. */
  uint32_t selection_only;
  uint8_t *selection_fragments;
  uint32_t selection_fragment_count;
  uint8_t *selection_values;
  uint32_t selection_value_count;
  uint64_t selection_root_normal_count;
  uint64_t selection_root_abnormal_count;
  /* Optional, process-local RANDOM build attribution. These counters are
   * enabled only by the end-to-end phase profiler and never affect choices. */
  int32_t profile_enabled;
  uint64_t profile_runtime_initialize_count;
  uint64_t profile_runtime_initialize_ns;
  uint64_t profile_plan_walk_count;
  uint64_t profile_plan_walk_ns;
  uint64_t profile_plan_select_count;
  uint64_t profile_plan_select_ns;
  uint64_t profile_plan_select_value_examined;
  uint64_t profile_runtime_candidate_enumeration_count;
  uint64_t profile_runtime_candidate_enumeration_ns;
  uint64_t profile_runtime_append_count;
  uint64_t profile_runtime_append_ns;
} SQstGeneratorRandomRuntime;

/*
 * One startup-compiled FRAG entry.  The ordered array is the only catalog
 * traversal order used by the RANDOM execution stage; child calls refer back
 * to entries by index instead of resolving a fragment id while building SQL.
 */
typedef struct {
  const SQstGeneratorFragment *fragment;
  uint32_t fragment_id;
  uint32_t ordered_index;
  uint32_t first_value_index;
  uint32_t value_count;
  /* A runtime object can change eligibility with the active scope. */
  uint32_t has_runtime_value;
} SQstGeneratorFragPlanEntry;

/* A VAL is flattened once during preparation and never looked up by id. */
typedef struct {
  const SQstGeneratorValue *value;
  uint32_t fragment_index;
  uint32_t value_index;
  uint32_t first_operation_index;
  uint32_t operation_count;
  /* Cached only for immutable literal rendering in the RANDOM fast path. */
  uint32_t text_length;
  /* Bit N is set when the value is structurally usable at recursion depth N. */
  uint32_t eligible_depth_mask;
} SQstGeneratorFragPlanValue;

/* One direct child or inline operation in a flattened VAL. */
typedef struct {
  const SQstGeneratorStep *step;
  SQstGeneratorChildCall child_call;
  uint32_t child_fragment_index;
  uint32_t operation_kind;
  uint32_t operation_index;
  /* Cached only when operation_kind is QST_GENERATOR_STEP_INLINE_VALUE. */
  uint32_t text_length;
} SQstGeneratorFragPlanOperation;

/* Immutable weighted-selection data for one depth, dialect and FRAG. */
typedef struct {
  const uint64_t *prefix_weights;
  uint64_t total_weight;
  uint64_t reciprocal;
  uint32_t eligible_count;
  /* The only eligible VAL when eligible_count is one, otherwise UINT32_MAX. */
  uint32_t sole_value_index;
} SQstGeneratorFragPlanStaticSelection;

/*
 * Immutable, process-local RANDOM plan. Preparation owns all flattened and
 * static-selection arrays. Runtime builders only walk these arrays and
 * consume their random stream.
 */
typedef struct {
  const SQstGeneratorCatalog *catalog;
  SQstDigest256 catalog_digest;
  SQstGeneratorFragPlanEntry *fragments;
  SQstGeneratorFragPlanValue *values;
  SQstGeneratorFragPlanOperation *operations;
  /* Prefix weights and selection entries for static fragments. */
  uint64_t *static_selection_prefix_weights;
  SQstGeneratorFragPlanStaticSelection *static_selections;
  /* One row per depth/dialect slot, each indexed directly by FRAG index. */
  SQstGeneratorFragPlanStaticSelection **static_selection_rows;
  uint32_t fragment_count;
  uint32_t value_count;
  uint32_t operation_count;
  uint32_t root_fragment_index;
  uint32_t has_runtime_values;
} SQstGeneratorFragPlan;

/*
 * A catalog value emits text then runs direct child invocations in order.
 * Recursion and optional/loop behavior belong to each child invocation.
 */
/* Validates ID namespaces, references, finite recursion endpoints and digest. */
EQstCode qst_generator_catalog_validate(const SQstGeneratorCatalog *catalog);
/* Calculates the catalog digest from immutable catalog fields. */
EQstCode qst_generator_catalog_digest(const SQstGeneratorCatalog *catalog,
                                      SQstDigest256 *out_digest);
/* Initializes a deterministic random stream for one generation sequence. */
EQstCode qst_generator_random_init(SQstGeneratorRandomState *state,
                                   uint64_t random_seed,
                                   uint64_t generation_seq);
/* Equivalent stream initialization that materializes random_array lazily. */
EQstCode qst_generator_random_init_lazy(SQstGeneratorRandomState *state,
                                        uint64_t random_seed,
                                        uint64_t generation_seq);
/* Consumes one deterministic random value, including array wrap accounting. */
EQstCode qst_generator_random_next(SQstGeneratorRandomState *state,
                                   uint64_t *out_value);
/* Releases generator-local path and occurrence storage owned by one result. */
void qst_generator_generated_sql_destroy(SQstGeneratedSql *sql);
/* Releases an unpublished generator-private FULL SQL retained for retry. */
void qst_generator_full_pending_destroy(SQstGeneratorFullPending *pending);
/* Releases one generator-private FULL successor seed. */
void qst_generator_full_path_seed_destroy(SQstGeneratorFullPathSeed *seed);
/* Releases the generator-process-local immutable RANDOM runtime snapshot. */
void qst_generator_random_runtime_destroy(SQstGeneratorRandomRuntime *runtime);
/* Builds every FRAG, VAL and child-operation index before RANDOM execution. */
EQstCode qst_generator_frag_plan_prepare(SQstGeneratorFragPlan *plan,
                                         const SQstGeneratorCatalog *catalog);
/* Releases the process-local arrays owned by a prepared FRAG plan. */
void qst_generator_frag_plan_destroy(SQstGeneratorFragPlan *plan);
/* Validates one immutable catalog and prepares reusable RANDOM lookup indexes. */
EQstCode qst_generator_random_runtime_prepare(
    SQstGeneratorRandomRuntime *runtime, const SQstGeneratorCatalog *catalog);
/* Builds exactly one RANDOM path. Reinitializing with identical input repeats it. */
EQstCode qst_generator_random_build(const SQstGeneratorCatalog *catalog,
                                    SQstGeneratorRandomState *state,
                                    uint32_t depth,
                                    SQstGeneratedSql *out_sql);
/* Builds one RANDOM path and resolves every runtime object from context. */
EQstCode qst_generator_random_build_with_runtime(
    const SQstGeneratorCatalog *catalog, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratedSql *out_sql);
/* Builds RANDOM SQL with an already prepared catalog runtime and its private
 * per-slot buildability scratch state. */
EQstCode qst_generator_random_build_prepared_with_runtime(
    const SQstGeneratorCatalog *catalog, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *prepared_runtime,
    SQstGeneratedSql *out_sql);
/* Builds one RANDOM path through the prepared FRAG plan used by production. */
EQstCode qst_generator_random_plan_build_with_runtime(
    const SQstGeneratorFragPlan *plan, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *prepared_runtime,
    SQstGeneratedSql *out_sql);
/* Builds the current FULL path without mutating the shared ordinal cursor. */
EQstCode qst_generator_full_peek(const SQstGeneratorCatalog *catalog,
                                 const SQstGeneratorCursor *cursor,
                                 uint32_t depth,
                                 SQstGeneratedSql *out_sql);
/* Builds the current FULL path and resolves every runtime object from context. */
EQstCode qst_generator_full_peek_with_runtime(
    const SQstGeneratorCatalog *catalog, const SQstGeneratorCursor *cursor,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratedSql *out_sql);
/* Advances a FULL cursor after its previously peeked SQL has been published. */
EQstCode qst_generator_full_commit(SQstGeneratorCursor *cursor,
                                   const SQstGeneratedSql *sql);
/*
 * Atomically publishes a complete SQL through block storage, task ledger and
 * generate_queue. On every failure it preserves caller generation state and
 * releases all unpublished task resources.
 */
EQstCode qst_generator_publish(SQstShmMapping *mapping, EQstRole role,
                               uint32_t generation,
                               const SQstGeneratorPublishInput *input,
                               const SQstGeneratedSql *sql,
                               uint64_t queue_deadline_mono_ns,
                               uint64_t *out_gid);
/* Initializes the shared, immutable catalog identity and generation cursors. */
EQstCode qst_generator_state_initialize(SQstShmMapping *mapping,
                                        const SQstGeneratorCatalog *catalog,
                                        uint32_t mode, uint32_t focus,
                                        uint32_t target_selection_bits,
                                        uint64_t random_seed,
                                        uint64_t statement_limit,
                                        uint64_t duration_limit_minutes);
/* Returns the validated mutable generation state for the current run. */
EQstCode qst_generator_state_get(SQstShmMapping *mapping,
                                 SQstGenerationState **out_state);
/* Freezes the RANDOM pipeline builder width for this run to a supported
 * power of two. */
EQstCode qst_generator_state_set_random_builder_count(
    SQstShmMapping *mapping, uint32_t main_generation, uint32_t builder_count);
/* Publishes the immutable prepared-dataset snapshot before generator startup. */
EQstCode qst_generator_state_publish_dataset(
    SQstShmMapping *mapping, uint32_t main_generation,
    SQstDigest256 dataset_digest, SQstDigest256 fixture_manifest_digest,
    SQstRef object_directory_ref, SQstRef fixture_manifest_ref);
/* Copies the prepared-dataset snapshot after its release-published state. */
EQstCode qst_generator_state_get_dataset(
    SQstShmMapping *mapping, SQstDigest256 *out_dataset_digest,
    SQstDigest256 *out_fixture_manifest_digest,
    SQstRef *out_object_directory_ref, SQstRef *out_fixture_manifest_ref);
/* Checks whether the next RANDOM path may be published at now_mono_ns. */
EQstCode qst_generator_random_can_publish(SQstShmMapping *mapping,
                                          uint64_t now_mono_ns,
                                          int32_t *out_can_publish,
                                          EQstStopReason *out_stop_reason,
                                          EQstRandomPublishGate *out_gate);
/* Checks the FULL run deadline before another statement is published. */
EQstCode qst_generator_full_duration_expired(uint64_t deadline_mono_ns,
                                             uint64_t now_mono_ns,
                                             int32_t *out_expired);
/* Publishes one durable FULL coverage seed for target_index when it remains.
 * The seed resolves a table from the published dataset directory; it does not
 * advance or alter the target's unrestricted DFS cursor. */
EQstCode qst_generator_full_publish_coverage_seed(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint32_t target_index, uint64_t weight_profile_version,
    uint64_t queue_deadline_mono_ns, uint64_t *out_gid);
/* Records the first ACTIVE transition for one RANDOM generation sequence. */
EQstCode qst_generator_random_activate(SQstShmMapping *mapping,
                                       uint64_t generation_seq,
                                       uint64_t now_mono_ns);
/* Records one exhausted FULL traversal and optionally starts its next round. */
EQstCode qst_generator_full_complete_round(SQstShmMapping *mapping,
                                           int32_t restart);
/* Publishes one FULL path and advances its shared cursor only after success. */
EQstCode qst_generator_full_publish_next(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    uint64_t *out_gid);
/* Retries one locally retained FULL path without rebuilding it after backpressure. */
EQstCode qst_generator_full_publish_pending(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorFullPending *pending, uint64_t *out_gid);
/* Publishes one FULL path using an independent process-local cache per target. */
EQstCode qst_generator_full_publish_pending_by_target(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorFullPending pending_by_target[QST_GENERATOR_TARGET_COUNT],
    uint64_t *out_gid);

/* Builds one target's current FULL DFS path without modifying shared state.
 * target_index is zero based. Callers publish the retained pending value with
 * qst_generator_full_publish_pending_by_target(), which remains the sole
 * owner of cursor, publication sequence, ledger, and queue mutations. */
EQstCode qst_generator_full_prepare_pending_for_target(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint32_t target_index, SQstGeneratorFullPending *pending);
/* Captures the immediate process-local successor of a prepared FULL path.
 * QST_E_QUEUE_EMPTY means the prepared path is the target's last path and
 * seed->exhausted is set. */
EQstCode qst_generator_full_pending_capture_successor(
    const SQstGeneratorFullPending *pending,
    SQstGeneratorFullPathSeed *seed);
/* Builds a captured successor without updating shared cursor state. The
 * publisher remains the only owner of cursor, task ledger, and queue writes. */
EQstCode qst_generator_full_prepare_pending_from_seed(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    const SQstGeneratorFullPathSeed *seed, SQstGeneratorFullPending *pending);
/* Publishes one RANDOM path and increments its shared sequence only on success. */
EQstCode qst_generator_random_publish_next(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorRandomRuntime *runtime, uint64_t *out_gid);
/* Builds, but does not reserve shared resources or advance the sequence. */
EQstCode qst_generator_random_build_sequence(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t generation_seq,
    SQstGeneratorRandomRuntime *runtime, SQstGeneratorRandomPending *out_pending);
/* Builds one prepared RANDOM sequence by walking a startup-compiled FRAG plan. */
EQstCode qst_generator_random_plan_build_sequence(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorFragPlan *plan, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t generation_seq,
    SQstGeneratorRandomRuntime *runtime, SQstGeneratorRandomPending *out_pending);
/* Consumes a producer-created deterministic stream for one prepared sequence. */
EQstCode qst_generator_random_plan_build_from_state(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorFragPlan *plan, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t generation_seq,
    SQstGeneratorRandomState *random_state,
    SQstGeneratorRandomRuntime *runtime, SQstGeneratorRandomPending *out_pending);
/* Publishes one prepared RANDOM SQL and advances its sequence only on success. */
EQstCode qst_generator_random_publish_prepared(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    SQstGeneratorRandomPending *pending, uint64_t queue_deadline_mono_ns,
    uint64_t *out_gid);
/* Prepares shared SQL/generation-record blocks without publishing a task. */
EQstCode qst_generator_random_prepare_resources(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    SQstGeneratorRandomPending *pending);
/* Releases shared resources retained by an unpublished RANDOM pending item. */
EQstCode qst_generator_random_release_resources(
    SQstShmMapping *mapping, SQstGeneratorRandomPending *pending);
/* Clears one published RANDOM SQL while retaining its private allocation cache. */
void qst_generator_random_pending_reset(SQstGeneratorRandomPending *pending);
/* Releases an uncommitted process-local RANDOM SQL. */
void qst_generator_random_pending_destroy(SQstGeneratorRandomPending *pending);
/*
 * Builds the next FULL DFS path and advances cursor only after a complete SQL
 * has been constructed. A caller that did not publish the SQL can retain the
 * original cursor and reproduce the same path after a process restart.
 */
EQstCode qst_generator_full_next(const SQstGeneratorCatalog *catalog,
                                 SQstGeneratorCursor *cursor, uint32_t depth,
                                 SQstGeneratedSql *out_sql);
/* Enumerates the FULL path space incrementally without cursor replay. */
EQstCode qst_generator_full_exhaustive_count(
    const SQstGeneratorCatalog *catalog, uint32_t depth,
    const SQstGeneratorRuntimeContext *runtime, uint64_t *out_path_count,
    uint64_t *out_fallback_count);
/*
 * Generates JSONL SQL evidence without publishing tasks or executing generated
 * SQL. The caller must publish a validated immutable dataset snapshot first.
 */
EQstCode qst_generator_write_only(SQstShmMapping *mapping,
                                  uint32_t main_generation,
                                  const char *output_path,
                                  uint64_t *out_sql_count);

#endif  // TDENGINE_TOOLS_TAOSQUERYTEST_INC_QST_GENERATOR_H_
