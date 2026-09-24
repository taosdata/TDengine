/* Deterministic FRAG/VAL traversal shared by FULL and RANDOM generation. */

#include "qst_generator.h"

#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "qst_sha256.h"
#include "qst_block.h"
#include "qst_dataset.h"
#include "qst_process.h"
#include "qst_queue.h"
#include "qst_sql_dimension_config.h"

typedef struct {
  SQstObjectDescriptor table;
  char alias[QST_GENERATOR_MAX_ALIAS_BYTES + 1U];
} SQstGeneratorScopeTable;

enum { QST_GENERATOR_DEBUG_OUTPUT_PATH_BYTES = 4096U };

typedef struct {
  SQstObjectDescriptor source;
  SQstObjectDescriptor last_table;
  SQstGeneratorScopeTable tables[QST_GENERATOR_MAX_SCOPE_TABLES];
  uint32_t has_source;
  uint32_t has_last_table;
  uint32_t default_table_seeded;
  uint32_t table_count;
} SQstGeneratorScope;

typedef enum {
  QST_RUNTIME_RENDER_NAME = 1,
  QST_RUNTIME_RENDER_QUALIFIED_TABLE = 2,
  QST_RUNTIME_RENDER_BARE_COLUMN = 3,
  QST_RUNTIME_RENDER_TABLE_COLUMN = 4,
  QST_RUNTIME_RENDER_ALIAS_COLUMN = 5,
  QST_RUNTIME_RENDER_TABLE_ASTERISK = 6,
  QST_RUNTIME_RENDER_ALIAS_ASTERISK = 7,
  QST_RUNTIME_RENDER_TABLE_TBNAME = 8,
  QST_RUNTIME_RENDER_ALIAS_TBNAME = 9,
  QST_RUNTIME_RENDER_ROLLUP_TAG = 10,
} EQstGeneratorRuntimeRender;

typedef struct {
  SQstObjectDescriptor object;
  SQstObjectDescriptor table;
  uint32_t table_index;
  uint32_t render;
} SQstGeneratorRuntimeCandidate;

typedef struct {
  uint64_t object_id;
  uint32_t object_index;
} SQstGeneratorRuntimeObjectIdIndex;

typedef struct {
  uint64_t parent_object_id;
  uint32_t object_index;
} SQstGeneratorRuntimeColumnIndex;

typedef struct {
  uint64_t parent_object_id;
  uint32_t first_index;
  uint32_t object_count;
} SQstGeneratorRuntimeColumnRange;

/*
 * RANDOM retains this index beside its copied, sealed object directory. The
 * index lists retain directory order within every object kind and table, so it
 * changes lookup cost without changing candidate ordinal or SQL rendering.
 */
typedef struct {
  uint32_t object_count;
  uint32_t kind_offsets[QST_OBJECT_COLUMN_OR_TAG + 2U];
  uint32_t *kind_object_indices;
  SQstGeneratorRuntimeObjectIdIndex *object_ids;
  SQstGeneratorRuntimeColumnIndex *columns;
  SQstGeneratorRuntimeColumnRange *column_ranges;
  uint32_t column_count;
  uint32_t column_range_count;
  uint32_t *name_offsets;
  char *names;
  uint32_t names_bytes;
} SQstGeneratorRuntimeObjectIndex;

typedef struct {
  uint32_t value_id;
  const SQstGeneratorValue *value;
} SQstGeneratorBuildableValue;

typedef struct {
  uint32_t value_index;
  uint64_t prefix_weight;
} SQstGeneratorPlanSelectionEntry;

typedef struct {
  uint32_t fragment_id;
  uint32_t fragment_index;
} SQstGeneratorBuildableFragment;

enum { QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE = 1024 };
enum { QST_GENERATOR_NAME_COMPARE_BYTES = 256 };
enum { QST_GENERATOR_PLAN_DIALECT_SLOT_COUNT = 5 };
enum {
  QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT =
      (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) *
      QST_GENERATOR_PLAN_DIALECT_SLOT_COUNT,
};

/* Private direct-mapped indexes preserve immutable catalog lookup results.
 * They never participate in SQL choice selection or shared-state publication. */
typedef struct {
  uint32_t fragment_ids[QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE];
  uint32_t fragment_indices[QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE];
  const SQstGeneratorValue *values[QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE];
  uint32_t value_indices[QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE];
} SQstGeneratorBuildLookupCache;

/* Catalog ordering is immutable for the lifetime of a generator process. */
static const SQstGeneratorCatalog *g_buildable_index_catalog = NULL;
static SQstGeneratorBuildableValue *g_buildable_value_index = NULL;
static SQstGeneratorBuildableFragment *g_buildable_fragment_index = NULL;
static uint32_t g_buildable_value_count = 0;
static uint32_t g_buildable_index_catalog_version = 0;
static uint32_t g_buildable_index_fragment_count = 0;
static SQstDigest256 g_buildable_index_catalog_digest = {{0}};
/* This cache is immutable after its first publication, but multiple generator
 * builders can attempt that first publication concurrently. */
static pthread_mutex_t g_buildable_index_mutex = PTHREAD_MUTEX_INITIALIZER;

static uint32_t qstGeneratorPlanDialectSlot(uint32_t dialect_mask) {
  switch (dialect_mask) {
    case 0:
      return 0;
    case QST_GENERATOR_DIALECT_MASK_TDENGINE:
      return 1;
    case QST_GENERATOR_DIALECT_MASK_MYSQL:
      return 2;
    case QST_GENERATOR_DIALECT_MASK_POSTGRESQL:
      return 3;
    case QST_GENERATOR_DIALECT_MASK_INFLUXQL:
      return 4;
    default:
      return UINT32_MAX;
  }
}

static uint64_t qstGeneratorPlanSelectionSlot(uint32_t depth,
                                               uint32_t dialect_slot) {
  return (uint64_t)depth * QST_GENERATOR_PLAN_DIALECT_SLOT_COUNT +
         dialect_slot;
}

/* Computes value % divisor without the hot-path hardware division. The
 * reciprocal is prepared once for each immutable static selection slot. */
static uint64_t qstGeneratorPlanFastModulo(uint64_t value, uint64_t divisor,
                                           uint64_t reciprocal) {
  uint64_t quotient = 0;
  uint64_t remainder = 0;

  if (divisor == 1U) {
    return 0;
  }
  quotient = (uint64_t)(((__uint128_t)value * reciprocal) >> 64U);
  remainder = value - quotient * divisor;
  if (remainder >= divisor) {
    remainder -= divisor;
  }
  return remainder;
}

typedef struct {
  const SQstGeneratorCatalog *catalog;
  const uint32_t *full_choices;
  uint32_t full_choice_count;
  SQstGeneratorRandomState *random_state;
  SQstGeneratedSql *output;
  uint8_t *buildable_fragments;
  uint32_t *buildable_fragment_epochs;
  const SQstGeneratorBuildableFragment *buildable_fragment_index;
  uint8_t *buildable_values;
  uint32_t *buildable_value_epochs;
  const SQstGeneratorBuildableValue *buildable_value_index;
  uint32_t buildable_value_count;
  const uint8_t *buildable_value_eligibility;
  SQstGeneratorBuildLookupCache *lookup_cache;
  const SQstGeneratorRuntimeContext *runtime;
  SQstDatasetInfo dataset;
  const SQstObjectDescriptor *runtime_objects;
  uint32_t runtime_object_count;
  const SQstGeneratorRuntimeObjectIndex *runtime_object_index;
  uint32_t runtime_objects_owned;
  uint32_t buildable_indexes_owned;
  uint32_t buildability_state_owned;
  uint32_t lookup_cache_owned;
  uint32_t *buildability_epoch_owner;
  SQstGeneratorScope scopes[QST_GENERATOR_MAX_SCOPE_DEPTH];
  uint32_t runtime_candidate_count[QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER +
                                   1U];
  uint8_t runtime_candidate_cached[QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER +
                                   1U];
  SQstGeneratorRuntimeCandidate **runtime_candidate_lists;
  uint32_t *runtime_candidate_capacities;
  void *plan_selection_scratch;
  uint32_t plan_selection_capacity;
  uint32_t scope_depth;
  uint32_t buildability_epoch;
  uint32_t dialect_mask;
  uint32_t choice_index;
  uint32_t is_random;
  uint8_t contains_abnormal;
  uint32_t disable_path_metadata;
  uint32_t disable_occurrence_metadata;
  uint32_t selection_only;
  /* One immutable selection row per recursion depth for this SQL dialect. */
  const SQstGeneratorFragPlanStaticSelection *static_selection_rows[
      QST_GENERATOR_MAX_RECURSION_DEPTH + 1U];
  SQstGeneratorRandomRuntime *profile_runtime;
} SQstGeneratorBuild;

/* A RANDOM build keeps one dialect for its complete lifetime. Bind its static
 * rows once so every FRAG selection avoids recomputing the same dialect slot
 * and two-dimensional row address. */
static void qstGeneratorPlanBindStaticSelectionRows(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan) {
  const uint32_t dialect_slot =
      build != NULL ? qstGeneratorPlanDialectSlot(build->dialect_mask) :
                      UINT32_MAX;

  if (NULL == build || NULL == plan ||
      plan->static_selection_prefix_weights == NULL ||
      plan->static_selection_rows == NULL || dialect_slot == UINT32_MAX) {
    return;
  }
  for (uint32_t depth = 0; depth <= QST_GENERATOR_MAX_RECURSION_DEPTH;
       ++depth) {
    build->static_selection_rows[depth] = plan->static_selection_rows[
        qstGeneratorPlanSelectionSlot(depth, dialect_slot)];
  }
}

static inline uint64_t qstGeneratorProfileNowNs(
    const SQstGeneratorRandomRuntime *runtime) {
  struct timespec now = {0};

  if (NULL == runtime || runtime->profile_enabled == 0 ||
      clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 ||
      now.tv_nsec < 0) {
    return 0;
  }
  if ((uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (uint64_t)now.tv_sec * UINT64_C(1000000000) +
         (uint64_t)now.tv_nsec;
}

static inline void qstGeneratorProfileAddElapsed(
    SQstGeneratorRandomRuntime *runtime, uint64_t started_ns,
    uint64_t *total_ns) {
  const uint64_t finished_ns = qstGeneratorProfileNowNs(runtime);
  const uint64_t elapsed_ns = finished_ns >= started_ns ?
                                  finished_ns - started_ns :
                                  0;

  if (NULL == runtime || NULL == total_ns || started_ns == 0 ||
      finished_ns == 0) {
    return;
  }
  *total_ns = *total_ns > UINT64_MAX - elapsed_ns ? UINT64_MAX :
                                                     *total_ns + elapsed_ns;
}

typedef struct {
  uint32_t choice_count;
  uint32_t choice_capacity;
  uint32_t *choices;
} SQstGeneratorFullPath;

enum {
  QST_GENERATION_DATASET_UNPUBLISHED = 0,
  QST_GENERATION_DATASET_PUBLISHED = 1,
  /* DS 3.4.1 reserves SQL, binding, result, and diagnostic blocks per worker. */
  QST_GENERATOR_DOWNSTREAM_BLOCKS_PER_WORKER = 4,
};

static int32_t qstGeneratorIsChildCallValid(
    const SQstGeneratorChildCall *child_call);
static int32_t qstGeneratorIsStepValid(const SQstGeneratorStep *step);
static int32_t qstGeneratorCatalogHasRuntimeObject(
    const SQstGeneratorCatalog *catalog);
static void qstGeneratorRuntimeObjectIndexDestroy(
    SQstGeneratorRuntimeObjectIndex *index);
static int32_t qstGeneratorIsValueEligible(const SQstGeneratorValue *value,
                                           uint32_t depth);
static int32_t qstGeneratorValueCanBuild(SQstGeneratorBuild *build,
                                         const SQstGeneratorValue *value,
                                         uint32_t depth);
static EQstCode qstGeneratorScopePush(SQstGeneratorBuild *build);
static void qstGeneratorSelectionMark(
    const SQstGeneratorBuild *build, uint32_t fragment_id, uint32_t value_id,
    uint32_t is_abnormal);
static EQstCode qstGeneratorRuntimeCandidateCount(
    SQstGeneratorBuild *build, uint32_t runtime_object_kind,
    uint32_t *out_count);
static EQstCode qstGeneratorValueRuntimeCandidateCount(
    SQstGeneratorBuild *build, const SQstGeneratorValue *value,
    uint32_t *out_count);
static EQstCode qstGeneratorAppend(SQstGeneratedSql *output,
                                   const char *text);
static inline __attribute__((always_inline)) EQstCode qstGeneratorAppendBytes(
    SQstGeneratedSql *output, const char *text, uint32_t text_length);
static inline __attribute__((always_inline)) EQstCode
qstGeneratorAppendBytesFast(SQstGeneratedSql *output, const char *text,
                            uint32_t text_length);
static EQstCode qstGeneratorScopeAddTable(
    SQstGeneratorBuild *build, const SQstObjectDescriptor *table);
static EQstCode qstGeneratorScopeSeedDefaultObjects(SQstGeneratorBuild *build);

static void qstGeneratorBuildabilityReset(SQstGeneratorBuild *build) {
  if (NULL == build || NULL == build->catalog ||
      NULL == build->buildable_fragments ||
      NULL == build->buildable_fragment_epochs ||
      NULL == build->buildable_values ||
      NULL == build->buildable_value_epochs) {
    return;
  }
  if (build->buildability_epoch == UINT32_MAX) {
    const uint64_t fragment_state_count =
        (uint64_t)build->catalog->fragment_count *
        (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U);
    const uint64_t value_state_count =
        (uint64_t)build->buildable_value_count *
        (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U);

    memset(build->buildable_fragment_epochs, 0,
           (size_t)fragment_state_count * sizeof(*build->buildable_fragment_epochs));
    memset(build->buildable_value_epochs, 0,
           (size_t)value_state_count * sizeof(*build->buildable_value_epochs));
    build->buildability_epoch = 1U;
  } else {
    ++build->buildability_epoch;
  }
  memset(build->runtime_candidate_cached, 0,
         sizeof(build->runtime_candidate_cached));
}

static void qstGeneratorGeneratedSqlReset(SQstGeneratedSql *sql) {
  if (NULL == sql) {
    return;
  }
  sql->occurrence_count = 0;
  sql->choice_count = 0;
  sql->sql_length = 0;
  sql->contains_abnormal = 0;
  sql->contains_unsupported_fallback = 0;
  memset(sql->sql_sha256.bytes, 0, sizeof(sql->sql_sha256.bytes));
  sql->sql[0] = '\0';
}

static void qstGeneratorSelectionMark(const SQstGeneratorBuild *build,
                                      uint32_t fragment_id,
                                      uint32_t value_id,
                                      uint32_t is_abnormal) {
  if (build == NULL || build->profile_runtime == NULL ||
      build->profile_runtime->selection_only == 0) {
    return;
  }
  if (build->profile_runtime->selection_fragments != NULL &&
      fragment_id < build->profile_runtime->selection_fragment_count) {
    build->profile_runtime->selection_fragments[fragment_id] = 1;
  }
  if (build->profile_runtime->selection_values != NULL && value_id != 0 &&
      value_id < build->profile_runtime->selection_value_count) {
    build->profile_runtime->selection_values[value_id] = 1;
  }
  if (build->catalog != NULL && fragment_id == build->catalog->root_fragment_id) {
    if (is_abnormal != 0) {
      ++build->profile_runtime->selection_root_abnormal_count;
    } else {
      ++build->profile_runtime->selection_root_normal_count;
    }
  }
}

static void qstGeneratorBuildDestroy(SQstGeneratorBuild *build) {
  if (NULL != build) {
    if (build->buildability_epoch_owner != NULL) {
      *build->buildability_epoch_owner = build->buildability_epoch;
    }
    if (build->buildability_state_owned != 0) {
      free(build->buildable_fragments);
      free(build->buildable_fragment_epochs);
      free(build->buildable_values);
      free(build->buildable_value_epochs);
    }
    if (build->lookup_cache_owned != 0) {
      free(build->lookup_cache);
    }
    if (build->buildable_indexes_owned != 0) {
      free((void *)build->buildable_value_index);
      free((void *)build->buildable_fragment_index);
    }
    if (build->runtime_objects_owned != 0) {
      free((void *)build->runtime_objects);
    }
    build->buildable_fragments = NULL;
    build->buildable_fragment_epochs = NULL;
    build->runtime_objects = NULL;
    build->runtime_object_count = 0;
    build->runtime_objects_owned = 0;
    build->buildable_values = NULL;
    build->buildable_value_epochs = NULL;
    build->buildable_value_index = NULL;
    build->buildable_value_count = 0;
    build->buildable_fragment_index = NULL;
    build->buildable_indexes_owned = 0;
    build->buildability_state_owned = 0;
    build->lookup_cache = NULL;
    build->lookup_cache_owned = 0;
    build->buildability_epoch_owner = NULL;
  }
}
static EQstCode qstGeneratorBuildFragment(SQstGeneratorBuild *build,
                                          uint32_t fragment_id,
                                          uint32_t depth,
                                          uint32_t parent_occurrence_id,
                                          uint32_t child_index,
                                          uint32_t loop_iteration,
                                          uint32_t *out_return_state);

void qst_generator_generated_sql_destroy(SQstGeneratedSql *sql) {
  if (NULL == sql) {
    return;
  }
  free(sql->occurrences);
  free(sql->selected_value_ids);
  free(sql->choice_cardinalities);
  sql->occurrences = NULL;
  sql->occurrence_capacity = 0;
  sql->selected_value_ids = NULL;
  sql->choice_cardinalities = NULL;
  sql->choice_capacity = 0;
  /* The SQL tail is overwritten from offset zero on the next build. Clearing
   * all 64 KiB here made every completed path pay a cache-bandwidth penalty. */
  qstGeneratorGeneratedSqlReset(sql);
}

void qst_generator_full_pending_destroy(SQstGeneratorFullPending *pending) {
  if (NULL == pending) {
    return;
  }
  qst_generator_generated_sql_destroy(&pending->sql);
  free(pending->full_path_choices);
  free(pending->runtime_objects);
  if (NULL != pending->build_runtime) {
    qst_generator_random_runtime_destroy(
        (SQstGeneratorRandomRuntime *)pending->build_runtime);
    free(pending->build_runtime);
  }
  memset(pending, 0, sizeof(*pending));
}

void qst_generator_full_path_seed_destroy(SQstGeneratorFullPathSeed *seed) {
  if (NULL == seed) {
    return;
  }
  free(seed->choice_cardinalities);
  free(seed->full_path_choices);
  memset(seed, 0, sizeof(*seed));
}

void qst_generator_random_runtime_destroy(
    SQstGeneratorRandomRuntime *runtime) {
  if (NULL == runtime) {
    return;
  }
  qstGeneratorRuntimeObjectIndexDestroy(
      (SQstGeneratorRuntimeObjectIndex *)runtime->runtime_object_index);
  free(runtime->runtime_objects);
  free(runtime->catalog_value_index);
  free(runtime->catalog_fragment_index);
  free(runtime->catalog_value_eligibility);
  free(runtime->buildable_fragments);
  free(runtime->buildable_fragment_epochs);
  free(runtime->buildable_values);
  free(runtime->buildable_value_epochs);
  free(runtime->buildable_lookup_cache);
  for (uint32_t kind = QST_RUNTIME_OBJECT_SOURCE;
       kind <= QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER; ++kind) {
    free(runtime->runtime_candidate_lists[kind]);
  }
  free(runtime->plan_scratch);
  free(runtime->plan_selection_scratch);
  memset(runtime, 0, sizeof(*runtime));
}

static void qstGeneratorFullPendingClearSql(SQstGeneratorFullPending *pending) {
  if (NULL != pending) {
    /* The next FULL path is built by the same target-local generator. Retain
     * its output capacity so every successful publication does not allocate
     * and free path metadata again. */
    qstGeneratorGeneratedSqlReset(&pending->sql);
    pending->cursor_next_path_ordinal = 0;
    pending->target_index = 0;
    pending->valid = 0;
  }
}

static EQstCode qstGeneratorReserveArray(void **array, uint32_t *capacity,
                                         uint32_t required,
                                         uint64_t element_bytes) {
  uint32_t next_capacity = 0;
  void *next_array = NULL;

  if (NULL == array || NULL == capacity || element_bytes == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (required <= *capacity) {
    return QST_OK;
  }
  next_capacity = *capacity == 0 ? 16U : *capacity;
  while (next_capacity < required) {
    if (next_capacity > UINT32_MAX / 2U) {
      next_capacity = required;
      break;
    }
    next_capacity *= 2U;
  }
  if ((uint64_t)next_capacity > SIZE_MAX / element_bytes) {
    return QST_E_RESOURCE;
  }
  next_array = realloc(*array, (size_t)next_capacity * (size_t)element_bytes);
  if (NULL == next_array) {
    return QST_E_RESOURCE;
  }
  *array = next_array;
  *capacity = next_capacity;
  return QST_OK;
}

static EQstCode qstGeneratorReserveChoice(SQstGeneratedSql *output,
                                          uint32_t index) {
  uint32_t next_capacity = 0;
  uint32_t *next_cardinalities = NULL;
  uint32_t *next_value_ids = NULL;

  if (NULL == output || index == UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  if (index < output->choice_capacity) {
    return QST_OK;
  }
  next_capacity = output->choice_capacity == 0 ? 16U : output->choice_capacity;
  while (next_capacity <= index) {
    if (next_capacity > UINT32_MAX / 2U) {
      next_capacity = index + 1U;
      break;
    }
    next_capacity *= 2U;
  }
  if ((uint64_t)next_capacity > SIZE_MAX / sizeof(*next_cardinalities)) {
    return QST_E_RESOURCE;
  }
  next_cardinalities = realloc(output->choice_cardinalities,
                               (size_t)next_capacity * sizeof(*next_cardinalities));
  if (NULL == next_cardinalities) {
    return QST_E_RESOURCE;
  }
  output->choice_cardinalities = next_cardinalities;
  next_value_ids = realloc(output->selected_value_ids,
                           (size_t)next_capacity * sizeof(*next_value_ids));
  if (NULL == next_value_ids) {
    return QST_E_RESOURCE;
  }
  output->selected_value_ids = next_value_ids;
  output->choice_capacity = next_capacity;
  return QST_OK;
}

static EQstCode qstGeneratorReserveOccurrence(SQstGeneratedSql *output,
                                              uint32_t index) {
  if (NULL == output || index == UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  return qstGeneratorReserveArray((void **)&output->occurrences,
                                  &output->occurrence_capacity, index + 1U,
                                  sizeof(*output->occurrences));
}

static void qstGeneratorFullPathDestroy(SQstGeneratorFullPath *path) {
  if (NULL != path) {
    free(path->choices);
    memset(path, 0, sizeof(*path));
  }
}

static void qstGeneratorFullPendingPathDestroy(
    SQstGeneratorFullPending *pending) {
  if (NULL != pending) {
    free(pending->full_path_choices);
    pending->full_path_choices = NULL;
    pending->full_path_choice_count = 0;
    pending->full_path_choice_capacity = 0;
    pending->full_path_next_ordinal = 0;
    pending->full_path_target_index = 0;
    pending->full_path_ready = 0;
    pending->full_path_exhausted = 0;
  }
}

static SQstGeneratorFullPath qstGeneratorFullPendingPath(
    SQstGeneratorFullPending *pending) {
  SQstGeneratorFullPath path = {0};

  if (NULL != pending) {
    path.choices = pending->full_path_choices;
    path.choice_count = pending->full_path_choice_count;
    path.choice_capacity = pending->full_path_choice_capacity;
  }
  return path;
}

static void qstGeneratorFullPendingSavePath(
    SQstGeneratorFullPending *pending, const SQstGeneratorFullPath *path) {
  if (NULL != pending && NULL != path) {
    pending->full_path_choices = path->choices;
    pending->full_path_choice_count = path->choice_count;
    pending->full_path_choice_capacity = path->choice_capacity;
  }
}

static int32_t qstGeneratorDigestIsZero(const SQstDigest256 *digest) {
  static const SQstDigest256 kZeroDigest = {{0}};

  return NULL != digest &&
         memcmp(digest->bytes, kZeroDigest.bytes, sizeof(digest->bytes)) == 0;
}

static void qstGeneratorDatasetSnapshotDigest(
    const SQstDigest256 *dataset_digest,
    const SQstDigest256 *fixture_manifest_digest,
    const SQstRef *object_directory_ref, const SQstRef *fixture_manifest_ref,
    SQstDigest256 *out_digest) {
  SQstSha256 digest = {0};

  qst_sha256_init(&digest);
  qst_sha256_update(&digest, dataset_digest->bytes,
                    sizeof(dataset_digest->bytes));
  qst_sha256_update(&digest, fixture_manifest_digest->bytes,
                    sizeof(fixture_manifest_digest->bytes));
  qst_sha256_update(&digest, (const uint8_t *)object_directory_ref,
                    sizeof(*object_directory_ref));
  qst_sha256_update(&digest, (const uint8_t *)fixture_manifest_ref,
                    sizeof(*fixture_manifest_ref));
  qst_sha256_final(&digest, out_digest->bytes);
}

static uint64_t qstGeneratorSplitMix64Next(uint64_t *state) {
  uint64_t z = 0;

  *state += UINT64_C(0x9E3779B97F4A7C15);
  z = *state;
  z = (z ^ (z >> 30U)) * UINT64_C(0xBF58476D1CE4E5B9);
  z = (z ^ (z >> 27U)) * UINT64_C(0x94D049BB133111EB);
  return z ^ (z >> 31U);
}

static EQstCode qstGeneratorRandomEnsureLazyValue(
    SQstGeneratorRandomState *state) {
  if (state == NULL || state->random_index >= QST_GENERATOR_RANDOM_ARRAY_SIZE) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (state->lazy_initialized <= state->random_index) {
    if (state->lazy_initialized == QST_GENERATOR_RANDOM_ARRAY_SIZE) {
      return QST_E_RESOURCE;
    }
    state->random_array[state->lazy_initialized] =
        qstGeneratorSplitMix64Next(&state->lazy_array_state);
    ++state->lazy_initialized;
  }
  return QST_OK;
}

static const SQstGeneratorFragment *qstGeneratorFindFragment(
    const SQstGeneratorCatalog *catalog, uint32_t fragment_id) {
  if (NULL == catalog || NULL == catalog->fragments) {
    return NULL;
  }
  for (uint32_t index = 0; index < catalog->fragment_count; ++index) {
    if (catalog->fragments[index].fragment_id == fragment_id) {
      return &catalog->fragments[index];
    }
  }
  return NULL;
}

static int32_t qstGeneratorFindFragmentIndex(const SQstGeneratorCatalog *catalog,
                                             uint32_t fragment_id,
                                             uint32_t *out_index) {
  if (NULL == catalog || NULL == out_index || NULL == catalog->fragments) {
    return 0;
  }
  for (uint32_t index = 0; index < catalog->fragment_count; ++index) {
    if (catalog->fragments[index].fragment_id == fragment_id) {
      *out_index = index;
      return 1;
    }
  }
  return 0;
}

static int32_t qstGeneratorRuntimeKindIsValid(uint32_t runtime_object_kind) {
  return runtime_object_kind >= QST_RUNTIME_OBJECT_SOURCE &&
         runtime_object_kind <= QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER;
}

static uint32_t qstGeneratorDialectMask(uint32_t dialect) {
  if (dialect < QST_DIALECT_TDENGINE || dialect > QST_DIALECT_INFLUXQL) {
    return 0;
  }
  return UINT32_C(1) << (dialect - QST_DIALECT_TDENGINE);
}

static int32_t qstGeneratorRuntimeObjectIdCompare(const void *left,
                                                  const void *right) {
  const SQstGeneratorRuntimeObjectIdIndex *lhs =
      (const SQstGeneratorRuntimeObjectIdIndex *)left;
  const SQstGeneratorRuntimeObjectIdIndex *rhs =
      (const SQstGeneratorRuntimeObjectIdIndex *)right;

  if (lhs->object_id < rhs->object_id) return -1;
  if (lhs->object_id > rhs->object_id) return 1;
  return lhs->object_index < rhs->object_index ? -1 :
         (lhs->object_index > rhs->object_index ? 1 : 0);
}

static int32_t qstGeneratorRuntimeColumnCompare(const void *left,
                                                const void *right) {
  const SQstGeneratorRuntimeColumnIndex *lhs =
      (const SQstGeneratorRuntimeColumnIndex *)left;
  const SQstGeneratorRuntimeColumnIndex *rhs =
      (const SQstGeneratorRuntimeColumnIndex *)right;

  if (lhs->parent_object_id < rhs->parent_object_id) return -1;
  if (lhs->parent_object_id > rhs->parent_object_id) return 1;
  return lhs->object_index < rhs->object_index ? -1 :
         (lhs->object_index > rhs->object_index ? 1 : 0);
}

static void qstGeneratorRuntimeObjectIndexDestroy(
    SQstGeneratorRuntimeObjectIndex *index) {
  if (NULL == index) {
    return;
  }
  free(index->kind_object_indices);
  free(index->object_ids);
  free(index->columns);
  free(index->column_ranges);
  free(index->name_offsets);
  free(index->names);
  free(index);
}

static EQstCode qstGeneratorRuntimeObjectIndexPrepareNames(
    SQstGeneratorRuntimeObjectIndex *index,
    const SQstShmMapping *mapping, const SQstObjectDescriptor *objects) {
  uint64_t names_bytes = 0;

  if (NULL == index || NULL == mapping || NULL == objects ||
      index->object_count == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  index->name_offsets = calloc((size_t)index->object_count + 1U,
                               sizeof(*index->name_offsets));
  if (NULL == index->name_offsets) {
    return QST_E_RESOURCE;
  }
  for (uint32_t object_index = 0; object_index < index->object_count;
       ++object_index) {
    const SQstObjectDescriptor *object = &objects[object_index];

    if (qstRefIsEmpty(object->qualified_name_ref) ||
        names_bytes > UINT32_MAX - object->qualified_name_ref.length - 1U) {
      return QST_E_SHM_CORRUPT;
    }
    index->name_offsets[object_index] = (uint32_t)names_bytes;
    names_bytes += (uint64_t)object->qualified_name_ref.length + 1U;
  }
  index->name_offsets[index->object_count] = (uint32_t)names_bytes;
  index->names = calloc((size_t)names_bytes, sizeof(*index->names));
  if (NULL == index->names) {
    return QST_E_RESOURCE;
  }
  for (uint32_t object_index = 0; object_index < index->object_count;
       ++object_index) {
    const SQstObjectDescriptor *object = &objects[object_index];
    const uint32_t offset = index->name_offsets[object_index];

    if (qst_block_copy(mapping, object->qualified_name_ref, 0, 0,
                       index->names + offset,
                       object->qualified_name_ref.length) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
  }
  index->names_bytes = (uint32_t)names_bytes;
  return QST_OK;
}

static EQstCode qstGeneratorRuntimeObjectIndexCreate(
    const SQstObjectDescriptor *objects, uint32_t object_count,
    SQstGeneratorRuntimeObjectIndex **out_index) {
  SQstGeneratorRuntimeObjectIndex *index = NULL;
  uint32_t kind_counts[QST_OBJECT_COLUMN_OR_TAG + 1U] = {0};
  uint32_t kind_next[QST_OBJECT_COLUMN_OR_TAG + 1U] = {0};
  uint32_t column_count = 0;

  if (NULL == objects || object_count == 0 || NULL == out_index) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_index = NULL;
  if ((uint64_t)object_count >
          SIZE_MAX / sizeof(*index->kind_object_indices) ||
      (uint64_t)object_count > SIZE_MAX / sizeof(*index->object_ids)) {
    return QST_E_RESOURCE;
  }
  index = (SQstGeneratorRuntimeObjectIndex *)calloc(1, sizeof(*index));
  if (NULL == index) {
    return QST_E_RESOURCE;
  }
  index->kind_object_indices = calloc(object_count,
                                      sizeof(*index->kind_object_indices));
  index->object_ids = calloc(object_count, sizeof(*index->object_ids));
  if (NULL == index->kind_object_indices || NULL == index->object_ids) {
    qstGeneratorRuntimeObjectIndexDestroy(index);
    return QST_E_RESOURCE;
  }
  for (uint32_t object_index = 0; object_index < object_count; ++object_index) {
    const SQstObjectDescriptor *object = &objects[object_index];

    if (object->object_kind == 0 ||
        object->object_kind > QST_OBJECT_COLUMN_OR_TAG || object->object_id == 0) {
      qstGeneratorRuntimeObjectIndexDestroy(index);
      return QST_E_SHM_CORRUPT;
    }
    ++kind_counts[object->object_kind];
    if (object->object_kind == QST_OBJECT_COLUMN_OR_TAG) {
      if (column_count == UINT32_MAX) {
        qstGeneratorRuntimeObjectIndexDestroy(index);
        return QST_E_RESOURCE;
      }
      ++column_count;
    }
    index->object_ids[object_index] =
        (SQstGeneratorRuntimeObjectIdIndex){.object_id = object->object_id,
                                             .object_index = object_index};
  }
  for (uint32_t kind = 0; kind <= QST_OBJECT_COLUMN_OR_TAG; ++kind) {
    index->kind_offsets[kind + 1U] = index->kind_offsets[kind] + kind_counts[kind];
    kind_next[kind] = index->kind_offsets[kind];
  }
  for (uint32_t object_index = 0; object_index < object_count; ++object_index) {
    const uint32_t kind = objects[object_index].object_kind;

    index->kind_object_indices[kind_next[kind]++] = object_index;
  }
  qsort(index->object_ids, object_count, sizeof(*index->object_ids),
        qstGeneratorRuntimeObjectIdCompare);
  if (column_count != 0) {
    uint32_t range_count = 0;

    index->columns = calloc(column_count, sizeof(*index->columns));
    index->column_ranges = calloc(column_count, sizeof(*index->column_ranges));
    if (NULL == index->columns || NULL == index->column_ranges) {
      qstGeneratorRuntimeObjectIndexDestroy(index);
      return QST_E_RESOURCE;
    }
    for (uint32_t column_index = 0; column_index < column_count; ++column_index) {
      const uint32_t object_index =
          index->kind_object_indices[index->kind_offsets[QST_OBJECT_COLUMN_OR_TAG] +
                                      column_index];

      index->columns[column_index] =
          (SQstGeneratorRuntimeColumnIndex){
              .parent_object_id = objects[object_index].parent_object_id,
              .object_index = object_index};
    }
    qsort(index->columns, column_count, sizeof(*index->columns),
          qstGeneratorRuntimeColumnCompare);
    for (uint32_t column_index = 0; column_index < column_count;) {
      const uint32_t first_index = column_index;
      const uint64_t parent_object_id =
          index->columns[column_index].parent_object_id;

      do {
        ++column_index;
      } while (column_index < column_count &&
               index->columns[column_index].parent_object_id == parent_object_id);
      index->column_ranges[range_count++] =
          (SQstGeneratorRuntimeColumnRange){
              .parent_object_id = parent_object_id,
              .first_index = first_index,
              .object_count = column_index - first_index};
    }
    index->column_count = column_count;
    index->column_range_count = range_count;
  }
  index->object_count = object_count;
  *out_index = index;
  return QST_OK;
}

static const uint32_t *qstGeneratorRuntimeObjectKindRange(
    const SQstGeneratorBuild *build, uint32_t object_kind,
    uint32_t *out_count) {
  const SQstGeneratorRuntimeObjectIndex *index = NULL;

  if (NULL == build || NULL == out_count || object_kind == 0 ||
      object_kind > QST_OBJECT_COLUMN_OR_TAG) {
    return NULL;
  }
  index = build->runtime_object_index;
  if (NULL == index || index->object_count != build->runtime_object_count ||
      NULL == index->kind_object_indices) {
    return NULL;
  }
  *out_count = index->kind_offsets[object_kind + 1U] -
               index->kind_offsets[object_kind];
  return index->kind_object_indices + index->kind_offsets[object_kind];
}

static const SQstGeneratorRuntimeColumnIndex *qstGeneratorRuntimeColumnRange(
    const SQstGeneratorBuild *build, uint64_t parent_object_id,
    uint32_t *out_count) {
  const SQstGeneratorRuntimeObjectIndex *index = NULL;
  uint32_t lower = 0;
  uint32_t upper = 0;

  if (NULL == build || NULL == out_count || parent_object_id == 0) {
    return NULL;
  }
  index = build->runtime_object_index;
  if (NULL == index || index->object_count != build->runtime_object_count ||
      (index->column_count != 0 &&
       (NULL == index->columns || NULL == index->column_ranges))) {
    return NULL;
  }
  upper = index->column_range_count;
  while (lower < upper) {
    const uint32_t middle = lower + (upper - lower) / 2U;
    const uint64_t current = index->column_ranges[middle].parent_object_id;

    if (parent_object_id < current) {
      upper = middle;
    } else if (parent_object_id > current) {
      lower = middle + 1U;
    } else {
      *out_count = index->column_ranges[middle].object_count;
      return &index->columns[index->column_ranges[middle].first_index];
    }
  }
  *out_count = 0;
  return index->columns;
}

static EQstCode qstGeneratorRuntimeObjectById(
    const SQstGeneratorBuild *build, uint64_t object_id,
    SQstObjectDescriptor *out_object) {
  const SQstGeneratorRuntimeObjectIndex *index = NULL;
  uint32_t lower = 0;
  uint32_t upper = 0;

  if (NULL == build || NULL == out_object || object_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  index = build->runtime_object_index;
  if (NULL == index || index->object_count != build->runtime_object_count ||
      NULL == index->object_ids) {
    return QST_E_INVALID_ARGUMENT;
  }
  upper = index->object_count;
  while (lower < upper) {
    const uint32_t middle = lower + (upper - lower) / 2U;
    const uint64_t current = index->object_ids[middle].object_id;

    if (object_id < current) {
      upper = middle;
    } else if (object_id > current) {
      lower = middle + 1U;
    } else if (index->object_ids[middle].object_index <
               build->runtime_object_count) {
      *out_object = build->runtime_objects[index->object_ids[middle].object_index];
      return QST_OK;
    } else {
      return QST_E_SHM_CORRUPT;
    }
  }
  return QST_E_INVALID_ARGUMENT;
}

static const char *qstGeneratorRuntimeObjectName(
    const SQstGeneratorBuild *build, const SQstObjectDescriptor *object,
    uint32_t *out_length) {
  const SQstGeneratorRuntimeObjectIndex *index = NULL;
  uint32_t lower = 0;
  uint32_t upper = 0;

  if (NULL == build || NULL == object || NULL == out_length ||
      object->object_id == 0 || NULL == build->runtime_object_index) {
    return NULL;
  }
  index = build->runtime_object_index;
  if (index->object_count != build->runtime_object_count ||
      NULL == index->object_ids || NULL == index->name_offsets ||
      NULL == index->names) {
    return NULL;
  }
  upper = index->object_count;
  while (lower < upper) {
    const uint32_t middle = lower + (upper - lower) / 2U;
    const SQstGeneratorRuntimeObjectIdIndex *entry = &index->object_ids[middle];

    if (object->object_id < entry->object_id) {
      upper = middle;
    } else if (object->object_id > entry->object_id) {
      lower = middle + 1U;
    } else if (entry->object_index < index->object_count) {
      const uint32_t begin = index->name_offsets[entry->object_index];
      const uint32_t end = index->name_offsets[entry->object_index + 1U];

      if (end <= begin || end > index->names_bytes) {
        return NULL;
      }
      *out_length = end - begin - 1U;
      return index->names + begin;
    } else {
      return NULL;
    }
  }
  return NULL;
}

static EQstCode qstGeneratorRuntimeInitialize(
    SQstGeneratorBuild *build, const SQstGeneratorRuntimeContext *runtime) {
  uint32_t object_count = 0;
  EQstCode code = QST_OK;

  if (NULL == build) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL == runtime) {
    return QST_OK;
  }
  if (NULL == runtime->mapping || qstRefIsEmpty(runtime->object_directory_ref) ||
      qstRefIsEmpty(runtime->fixture_manifest_ref) ||
      runtime->target < QST_TARGET_TD || runtime->target > QST_TARGET_INFLUX ||
      runtime->dialect < QST_DIALECT_TDENGINE ||
      runtime->dialect > QST_DIALECT_INFLUXQL) {
    return QST_E_INVALID_ARGUMENT;
  }
  build->runtime = runtime;
  build->dataset.object_directory_ref = runtime->object_directory_ref;
  build->dataset.fixture_manifest_ref = runtime->fixture_manifest_ref;
  if (NULL != runtime->objects || runtime->object_count != 0) {
    if (NULL == runtime->objects || runtime->object_count == 0) {
      return QST_E_INVALID_ARGUMENT;
    }
    build->runtime_objects = runtime->objects;
    build->runtime_object_count = runtime->object_count;
    return qstGeneratorScopePush(build);
  }
  code = qst_dataset_get_object_count(runtime->mapping, &build->dataset,
                                      &object_count);
  if (code != QST_OK || object_count == 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  if ((uint64_t)object_count > SIZE_MAX / sizeof(*build->runtime_objects)) {
    return QST_E_RESOURCE;
  }
  build->runtime_objects = calloc(object_count, sizeof(*build->runtime_objects));
  if (NULL == build->runtime_objects) {
    return QST_E_RESOURCE;
  }
  build->runtime_objects_owned = 1;
  code = qst_dataset_copy_object_directory(
      runtime->mapping, &build->dataset,
      (SQstObjectDescriptor *)build->runtime_objects, object_count,
      &build->runtime_object_count);
  if (code != QST_OK || build->runtime_object_count != object_count) {
    free((void *)build->runtime_objects);
    build->runtime_objects = NULL;
    build->runtime_objects_owned = 0;
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  return qstGeneratorScopePush(build);
}

static EQstCode qstGeneratorRuntimeGetParent(
    const SQstGeneratorBuild *build, const SQstObjectDescriptor *object,
    SQstObjectDescriptor *out_parent) {
  if (NULL == build || NULL == build->runtime || NULL == object ||
      NULL == out_parent || object->parent_object_id == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (build->runtime_object_index != NULL) {
    return qstGeneratorRuntimeObjectById(build, object->parent_object_id,
                                         out_parent);
  }
  for (uint32_t index = 0; index < build->runtime_object_count; ++index) {
    if (build->runtime_objects[index].object_id == object->parent_object_id) {
      *out_parent = build->runtime_objects[index];
      return QST_OK;
    }
  }
  return QST_E_INVALID_ARGUMENT;
}

static EQstCode qstGeneratorRuntimeGetSource(
    const SQstGeneratorBuild *build, const SQstObjectDescriptor *object,
    SQstObjectDescriptor *out_source) {
  SQstObjectDescriptor current = {0};

  if (NULL == build || NULL == object || NULL == out_source) {
    return QST_E_INVALID_ARGUMENT;
  }
  current = *object;
  while (current.object_kind != QST_OBJECT_SOURCE) {
    SQstObjectDescriptor parent = {0};
    EQstCode code = qstGeneratorRuntimeGetParent(build, &current, &parent);

    if (code != QST_OK) {
      return code;
    }
    current = parent;
  }
  *out_source = current;
  return QST_OK;
}

static SQstGeneratorScope *qstGeneratorCurrentScope(
    SQstGeneratorBuild *build) {
  if (NULL == build || build->scope_depth == 0 ||
      build->scope_depth > QST_GENERATOR_MAX_SCOPE_DEPTH) {
    return NULL;
  }
  return &build->scopes[build->scope_depth - 1U];
}

static EQstCode qstGeneratorScopePush(SQstGeneratorBuild *build) {
  SQstGeneratorScope *scope = NULL;
  EQstCode code = QST_OK;

  if (NULL == build || build->scope_depth >= QST_GENERATOR_MAX_SCOPE_DEPTH) {
    return QST_E_RESOURCE;
  }
  /* A scope table is live only below table_count. Reset those scalars and
   * clear an alias when its slot is reused instead of writing the whole
   * 20 KiB scope on every nested query block. */
  scope = &build->scopes[build->scope_depth];
  scope->has_source = 0;
  scope->has_last_table = 0;
  scope->default_table_seeded = 0;
  scope->table_count = 0;
  ++build->scope_depth;
  code = qstGeneratorScopeSeedDefaultObjects(build);
  if (code != QST_OK) {
    --build->scope_depth;
  } else {
    qstGeneratorBuildabilityReset(build);
  }
  return code;
}

static void qstGeneratorScopePop(SQstGeneratorBuild *build) {
  if (NULL != build && build->scope_depth > 1U) {
    --build->scope_depth;
    qstGeneratorBuildabilityReset(build);
  }
}

static EQstCode qstGeneratorScopeAddTable(SQstGeneratorBuild *build,
                                          const SQstObjectDescriptor *table) {
  SQstGeneratorScope *scope = qstGeneratorCurrentScope(build);

  if (NULL == scope || NULL == table ||
      table->object_kind != QST_OBJECT_TABLE_OR_MEASUREMENT ||
      scope->table_count >= QST_GENERATOR_MAX_SCOPE_TABLES) {
    return QST_E_RESOURCE;
  }
  scope->tables[scope->table_count].table = *table;
  scope->tables[scope->table_count].alias[0] = '\0';
  ++scope->table_count;
  scope->last_table = *table;
  scope->has_last_table = 1;
  return QST_OK;
}

/* Each query block starts with one target-local binding for SELECT-before-FROM. */
static EQstCode qstGeneratorScopeSeedDefaultObjects(SQstGeneratorBuild *build) {
  SQstGeneratorScope *scope = qstGeneratorCurrentScope(build);
  SQstObjectDescriptor source = {0};
  const SQstObjectDescriptor *table = NULL;
  const uint32_t *table_indices = NULL;
  uint32_t table_count = 0;

  if (NULL == build || NULL == scope || NULL == build->runtime ||
      build->selection_only != 0) {
    return QST_OK;
  }
  table_indices = qstGeneratorRuntimeObjectKindRange(
      build, QST_OBJECT_TABLE_OR_MEASUREMENT, &table_count);
  for (uint32_t index = 0;
       index < (table_indices != NULL ? table_count : build->runtime_object_count);
       ++index) {
    const SQstObjectDescriptor *candidate =
        &build->runtime_objects[table_indices != NULL ? table_indices[index] : index];

    if (candidate->object_kind == QST_OBJECT_TABLE_OR_MEASUREMENT &&
        candidate->target == build->runtime->target) {
      table = candidate;
      break;
    }
  }
  if (NULL == table) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (build->runtime->target != QST_TARGET_TD) {
    if (qstGeneratorRuntimeGetSource(build, table, &source) != QST_OK ||
        source.target != build->runtime->target) {
      return QST_E_INVALID_ARGUMENT;
    }
    scope->source = source;
    scope->has_source = 1;
  }
  if (qstGeneratorScopeAddTable(build, table) != QST_OK) {
    return QST_E_RESOURCE;
  }
  scope->default_table_seeded = 1;
  return QST_OK;
}

static EQstCode qstGeneratorScopeBindAlias(SQstGeneratorBuild *build,
                                           const char *text,
                                           uint32_t text_length) {
  SQstGeneratorScope *scope = qstGeneratorCurrentScope(build);
  uint32_t alias_begin = 0;
  uint32_t alias_end = text_length;

  if (NULL == scope || NULL == text) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (scope->table_count == 0 || text_length == 0) {
    return QST_OK;
  }
  while (alias_end > 0 &&
         (text[alias_end - 1U] == ' ' || text[alias_end - 1U] == '\t' ||
          text[alias_end - 1U] == '\n' || text[alias_end - 1U] == '\r')) {
    --alias_end;
  }
  alias_begin = alias_end;
  while (alias_begin > 0 && text[alias_begin - 1U] != ' ' &&
         text[alias_begin - 1U] != '\t' && text[alias_begin - 1U] != '\n' &&
         text[alias_begin - 1U] != '\r') {
    --alias_begin;
  }
  if (alias_begin == alias_end) {
    return QST_OK;
  }
  if (alias_end - alias_begin >=
      sizeof(scope->tables[scope->table_count - 1U].alias)) {
    return QST_E_INVALID_ARGUMENT;
  }
  memcpy(scope->tables[scope->table_count - 1U].alias, text + alias_begin,
         alias_end - alias_begin);
  scope->tables[scope->table_count - 1U].alias[alias_end - alias_begin] = '\0';
  qstGeneratorBuildabilityReset(build);
  return QST_OK;
}

static int32_t qstGeneratorRuntimeTableIsInScope(
    const SQstGeneratorScope *scope, uint64_t table_id, uint32_t *out_index) {
  if (NULL == scope) {
    return 0;
  }
  for (uint32_t index = 0; index < scope->table_count; ++index) {
    if (scope->tables[index].table.object_id == table_id) {
      if (NULL != out_index) {
        *out_index = index;
      }
      return 1;
    }
  }
  return 0;
}

static int32_t qstGeneratorRuntimeNameIsUniqueInScope(
    const SQstGeneratorBuild *build, const SQstGeneratorScope *scope,
    const SQstObjectDescriptor *column) {
  uint8_t column_bytes[QST_GENERATOR_NAME_COMPARE_BYTES] = {0};
  uint8_t candidate_bytes[QST_GENERATOR_NAME_COMPARE_BYTES] = {0};
  const uint32_t *column_indices = NULL;
  uint32_t column_count = 0;

  if (NULL == build || NULL == build->runtime || NULL == scope ||
      NULL == column || qstRefIsEmpty(column->qualified_name_ref) ||
      column->qualified_name_ref.length == 0) {
    return 0;
  }
  column_indices = qstGeneratorRuntimeObjectKindRange(
      build, QST_OBJECT_COLUMN_OR_TAG, &column_count);
  for (uint32_t object_index = 0;
       object_index < (column_indices != NULL ? column_count :
                                               build->runtime_object_count);
       ++object_index) {
    const SQstObjectDescriptor *candidate = &build->runtime_objects[
        column_indices != NULL ? column_indices[object_index] : object_index];
    uint64_t name_offset = 0;

    if (candidate->object_kind != QST_OBJECT_COLUMN_OR_TAG ||
        candidate->object_id == column->object_id ||
        !qstGeneratorRuntimeTableIsInScope(scope, candidate->parent_object_id,
                                           NULL)) {
      continue;
    }
    if (qstRefIsEmpty(candidate->qualified_name_ref) ||
        candidate->qualified_name_ref.length != column->qualified_name_ref.length) {
      continue;
    }
    /* A sealed reference carries the content CRC. Different CRCs prove the
     * names differ; matching CRCs still use the exact byte comparison below
     * so a collision cannot change the bare-column candidate set. */
    if (candidate->qualified_name_ref.crc32c !=
        column->qualified_name_ref.crc32c) {
      continue;
    }
    while (name_offset < column->qualified_name_ref.length) {
      uint64_t compare_bytes = column->qualified_name_ref.length - name_offset;

      if (compare_bytes > sizeof(column_bytes)) {
        compare_bytes = sizeof(column_bytes);
      }
      if (qst_block_copy(build->runtime->mapping, column->qualified_name_ref, 0,
                         name_offset, column_bytes, compare_bytes) != QST_OK ||
          qst_block_copy(build->runtime->mapping, candidate->qualified_name_ref,
                         0, name_offset, candidate_bytes, compare_bytes) !=
              QST_OK) {
        return 0;
      }
      if (memcmp(column_bytes, candidate_bytes, (size_t)compare_bytes) != 0) {
        break;
      }
      name_offset += compare_bytes;
    }
    if (name_offset == column->qualified_name_ref.length) {
      return 0;
    }
  }
  return 1;
}

/* Slow path: enumerate the current scope and refresh its candidate cache. */
static EQstCode qstGeneratorRuntimeCandidateEnumerate(
    SQstGeneratorBuild *build, uint32_t runtime_object_kind,
    uint32_t candidate_ordinal, SQstGeneratorRuntimeCandidate *out_candidate,
    uint32_t *out_count) {
  const SQstGeneratorScope *scope = NULL;
  uint32_t candidate_count = 0;
  uint64_t started_ns = 0;

  if (NULL == build || NULL == build->runtime ||
      !qstGeneratorRuntimeKindIsValid(runtime_object_kind) ||
      (NULL == out_candidate && NULL == out_count)) {
    return QST_E_INVALID_ARGUMENT;
  }
  scope = qstGeneratorCurrentScope(build);
  if (NULL == scope) {
    return QST_E_INTERNAL;
  }
  if (NULL != build->profile_runtime) {
    started_ns = qstGeneratorProfileNowNs(build->profile_runtime);
    if (build->profile_runtime->profile_runtime_candidate_enumeration_count !=
        UINT64_MAX) {
      ++build->profile_runtime->profile_runtime_candidate_enumeration_count;
    }
  }
#define QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate)                    \
  do {                                                                         \
    if (candidate_count == UINT32_MAX) {                                       \
      return QST_E_RESOURCE;                                                   \
    }                                                                          \
    if (build->runtime_candidate_lists != NULL &&                              \
        build->runtime_candidate_capacities != NULL) {                         \
      const uint32_t required = candidate_count + 1U;                          \
      EQstCode reserve_code = qstGeneratorReserveArray(                        \
          (void **)&build->runtime_candidate_lists[runtime_object_kind],       \
          &build->runtime_candidate_capacities[runtime_object_kind], required, \
          sizeof(SQstGeneratorRuntimeCandidate));                              \
                                                                               \
      if (reserve_code != QST_OK) {                                            \
        return reserve_code;                                                    \
      }                                                                         \
      build->runtime_candidate_lists[runtime_object_kind][candidate_count] =  \
          (candidate);                                                          \
    }                                                                           \
    if (candidate_count == candidate_ordinal && NULL != out_candidate) {      \
      *out_candidate = (candidate);                                            \
    }                                                                           \
    ++candidate_count;                                                         \
  } while (0)
  if (runtime_object_kind == QST_RUNTIME_OBJECT_COLUMN_REFERENCE ||
      runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK ||
      runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_TBNAME_PSEUDOCOLUMN) {
    for (uint32_t table_index = 0; table_index < scope->table_count;
         ++table_index) {
      const SQstGeneratorScopeTable *scope_table = &scope->tables[table_index];

      if (runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK ||
          runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_TBNAME_PSEUDOCOLUMN) {
        SQstGeneratorRuntimeCandidate candidate = {0};

        if (runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_TBNAME_PSEUDOCOLUMN &&
            scope_table->table.target != QST_TARGET_TD) {
          continue;
        }
        candidate.object = scope_table->table;
        candidate.table = scope_table->table;
        candidate.table_index = table_index;
        candidate.render = runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK ?
                               QST_RUNTIME_RENDER_TABLE_ASTERISK :
                               QST_RUNTIME_RENDER_TABLE_TBNAME;
        QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate);
        if (scope_table->alias[0] != '\0') {
          candidate.render = runtime_object_kind == QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK ?
                                 QST_RUNTIME_RENDER_ALIAS_ASTERISK :
                                 QST_RUNTIME_RENDER_ALIAS_TBNAME;
          QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate);
        }
        continue;
      }
      {
        const SQstGeneratorRuntimeColumnIndex *columns = NULL;
        uint32_t column_count = 0;

        columns = qstGeneratorRuntimeColumnRange(
            build, scope_table->table.object_id, &column_count);
        for (uint32_t object_index = 0;
             object_index < (columns != NULL ? column_count :
                                               build->runtime_object_count);
             ++object_index) {
          const SQstObjectDescriptor *object = &build->runtime_objects[
              columns != NULL ? columns[object_index].object_index : object_index];
        SQstGeneratorRuntimeCandidate candidate = {0};

        if (object->object_kind != QST_OBJECT_COLUMN_OR_TAG ||
            object->parent_object_id != scope_table->table.object_id) {
          continue;
        }
        candidate.object = *object;
        candidate.table = scope_table->table;
        candidate.table_index = table_index;
        if (qstGeneratorRuntimeNameIsUniqueInScope(build, scope, object)) {
          candidate.render = QST_RUNTIME_RENDER_BARE_COLUMN;
          QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate);
        }
        candidate.render = QST_RUNTIME_RENDER_TABLE_COLUMN;
        QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate);
        if (scope_table->alias[0] != '\0') {
          candidate.render = QST_RUNTIME_RENDER_ALIAS_COLUMN;
          QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate);
        }
        }
      }
    }
  } else {
    const uint32_t *object_indices = NULL;
    uint32_t object_count = 0;
    uint32_t object_kind = 0;

    switch (runtime_object_kind) {
      case QST_RUNTIME_OBJECT_SOURCE:
        object_kind = QST_OBJECT_SOURCE;
        break;
      case QST_RUNTIME_OBJECT_DATABASE:
        object_kind = QST_OBJECT_DATABASE_OR_SCHEMA;
        break;
      case QST_RUNTIME_OBJECT_TABLE:
      case QST_RUNTIME_OBJECT_TABLE_REFERENCE:
      case QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX:
        object_kind = QST_OBJECT_TABLE_OR_MEASUREMENT;
        break;
      case QST_RUNTIME_OBJECT_COLUMN:
      case QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER:
        object_kind = QST_OBJECT_COLUMN_OR_TAG;
        break;
      default:
        return QST_E_INVALID_ARGUMENT;
    }
    object_indices = qstGeneratorRuntimeObjectKindRange(build, object_kind,
                                                         &object_count);
    for (uint32_t object_index = 0;
         object_index < (object_indices != NULL ? object_count :
                                                 build->runtime_object_count);
         ++object_index) {
      const SQstObjectDescriptor *object = &build->runtime_objects[
          object_indices != NULL ? object_indices[object_index] : object_index];
      SQstObjectDescriptor source = {0};
      SQstGeneratorRuntimeCandidate candidate = {0};
      int32_t matches = 0;

      switch (runtime_object_kind) {
        case QST_RUNTIME_OBJECT_SOURCE:
          matches = object->object_kind == QST_OBJECT_SOURCE &&
                    build->runtime->target != QST_TARGET_TD &&
                    object->target == build->runtime->target;
          break;
        case QST_RUNTIME_OBJECT_DATABASE:
          matches = object->object_kind == QST_OBJECT_DATABASE_OR_SCHEMA &&
                    object->target == build->runtime->target;
          break;
        case QST_RUNTIME_OBJECT_TABLE:
          if (object->object_kind == QST_OBJECT_TABLE_OR_MEASUREMENT) {
            matches = scope->has_source == 0 ?
                          object->target == build->runtime->target :
                      qstGeneratorRuntimeGetSource(build, object, &source) == QST_OK &&
                          source.object_id == scope->source.object_id;
          }
          break;
        case QST_RUNTIME_OBJECT_COLUMN:
        case QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER:
          matches = object->object_kind == QST_OBJECT_COLUMN_OR_TAG &&
                    scope->has_last_table != 0 &&
                    object->parent_object_id == scope->last_table.object_id &&
                    (runtime_object_kind != QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER ||
                     (object->flags & QST_OBJECT_FLAG_TAG) != 0);
          if (runtime_object_kind == QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER) {
            candidate.render = QST_RUNTIME_RENDER_ROLLUP_TAG;
          }
          break;
        case QST_RUNTIME_OBJECT_TABLE_REFERENCE:
          matches = object->object_kind == QST_OBJECT_TABLE_OR_MEASUREMENT &&
                    object->target == build->runtime->target;
          candidate.render = QST_RUNTIME_RENDER_QUALIFIED_TABLE;
          break;
        case QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX:
          matches = object->object_kind == QST_OBJECT_TABLE_OR_MEASUREMENT &&
                    scope->has_source != 0 &&
                    qstGeneratorRuntimeGetSource(build, object, &source) == QST_OK &&
                    source.object_id == scope->source.object_id;
          candidate.render = QST_RUNTIME_RENDER_QUALIFIED_TABLE;
          break;
        default:
          break;
      }
      if (matches == 0) {
        continue;
      }
      candidate.object = *object;
      if (candidate.render == 0) {
        candidate.render = QST_RUNTIME_RENDER_NAME;
      }
      QST_GENERATOR_SELECT_RUNTIME_CANDIDATE(candidate);
    }
  }
#undef QST_GENERATOR_SELECT_RUNTIME_CANDIDATE
  if (NULL != out_count) {
    *out_count = candidate_count;
  }
  if (build->runtime_candidate_lists != NULL &&
      build->runtime_candidate_lists[runtime_object_kind] != NULL) {
    build->runtime_candidate_count[runtime_object_kind] = candidate_count;
    build->runtime_candidate_cached[runtime_object_kind] = 1;
  }
  if (NULL != build->profile_runtime) {
    qstGeneratorProfileAddElapsed(
        build->profile_runtime, started_ns,
        &build->profile_runtime->profile_runtime_candidate_enumeration_ns);
  }
  return candidate_count > candidate_ordinal || NULL == out_candidate ?
             QST_OK : QST_E_INVALID_ARGUMENT;
}

/* Hot path: a valid scope cache needs no scanner-local stack storage. */
static EQstCode qstGeneratorRuntimeCandidateAt(
    SQstGeneratorBuild *build, uint32_t runtime_object_kind,
    uint32_t candidate_ordinal, SQstGeneratorRuntimeCandidate *out_candidate,
    uint32_t *out_count) {
  const uint32_t cached_count =
      build != NULL && qstGeneratorRuntimeKindIsValid(runtime_object_kind) ?
          build->runtime_candidate_count[runtime_object_kind] :
          0;

  if (NULL == build || NULL == build->runtime ||
      !qstGeneratorRuntimeKindIsValid(runtime_object_kind) ||
      (NULL == out_candidate && NULL == out_count)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL == qstGeneratorCurrentScope(build)) {
    return QST_E_INTERNAL;
  }
  if (build->runtime_candidate_cached[runtime_object_kind] != 0 &&
      build->runtime_candidate_lists != NULL &&
      build->runtime_candidate_lists[runtime_object_kind] != NULL) {
    if (NULL != out_count) {
      *out_count = cached_count;
    }
    if (NULL != out_candidate) {
      if (candidate_ordinal >= cached_count) {
        return QST_E_INVALID_ARGUMENT;
      }
      *out_candidate =
          build->runtime_candidate_lists[runtime_object_kind][candidate_ordinal];
    }
    return QST_OK;
  }
  return qstGeneratorRuntimeCandidateEnumerate(
      build, runtime_object_kind, candidate_ordinal, out_candidate, out_count);
}

static EQstCode qstGeneratorRuntimeCandidateCount(
    SQstGeneratorBuild *build, uint32_t runtime_object_kind,
    uint32_t *out_count) {
  if (NULL == build || NULL == out_count ||
      !qstGeneratorRuntimeKindIsValid(runtime_object_kind)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL == build->runtime) {
    /* A runtime value cannot be rendered without an immutable object snapshot.
     * Report no candidate so random and FULL selection never choose a path
     * that would fail later in qstGeneratorRuntimeAppendObject(). */
    *out_count = 0;
    return QST_OK;
  }
  if (runtime_object_kind > QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (build->runtime_candidate_cached[runtime_object_kind] != 0) {
    *out_count = build->runtime_candidate_count[runtime_object_kind];
    return QST_OK;
  }
  {
    EQstCode code = qstGeneratorRuntimeCandidateAt(
        build, runtime_object_kind, 0, NULL, out_count);

    if (code == QST_OK) {
      build->runtime_candidate_count[runtime_object_kind] = *out_count;
      build->runtime_candidate_cached[runtime_object_kind] = 1;
    }
    return code;
  }
}

/* A table descriptor can carry an already qualified local name.  Prefix it
 * only when the descriptor is a bare child of the selected namespace. */
static int32_t qstGeneratorRuntimeNameHasParentPrefix(const char *object_name,
                                                      const char *parent_name) {
  const uint64_t parent_length = parent_name != NULL ? strlen(parent_name) : 0;

  return object_name != NULL && parent_length != 0 &&
         strncmp(object_name, parent_name, parent_length) == 0 &&
         object_name[parent_length] == '.';
}

static EQstCode qstGeneratorRuntimeAppendObject(
    SQstGeneratorBuild *build, uint32_t runtime_object_kind,
    uint32_t candidate_ordinal, SQstGenerationOccurrence *occurrence) {
  SQstGeneratorRuntimeCandidate candidate = {0};
  SQstGeneratorScope *scope = NULL;
  SQstObjectDescriptor parent = {0};
  /* Each buffer is initialized by its only consuming branch: the immutable
   * object-name copier writes a terminator and snprintf writes text. Avoid
   * clearing four 4 KiB buffers for every rendered runtime object. */
  char object_name[QST_GENERATOR_EXECUTABLE_SQL_BYTES];
  char table_name[QST_GENERATOR_EXECUTABLE_SQL_BYTES];
  char parent_name[QST_GENERATOR_EXECUTABLE_SQL_BYTES];
  char text[QST_GENERATOR_EXECUTABLE_SQL_BYTES];
  const char *append_text = text;
  const char *object_name_ptr = NULL;
  const char *table_name_ptr = NULL;
  const char *parent_name_ptr = NULL;
  uint32_t object_name_length = 0;
  uint32_t cached_name_length = 0;
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == build || NULL == build->output || NULL == build->runtime) {
    return QST_E_INVALID_ARGUMENT;
  }
  scope = qstGeneratorCurrentScope(build);
  code = qstGeneratorRuntimeCandidateAt(build, runtime_object_kind,
                                        candidate_ordinal, &candidate, NULL);
  if (NULL == scope || code != QST_OK ||
      candidate.object.qualified_name_ref.length >= sizeof(object_name)) {
    return code != QST_OK ? code : QST_E_SQL_LIMIT;
  }
  object_name_ptr = qstGeneratorRuntimeObjectName(
      build, &candidate.object, &object_name_length);
  if (object_name_ptr == NULL) {
    if (qst_dataset_copy_object_name(build->runtime->mapping, &candidate.object,
                                     object_name, sizeof(object_name)) != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    object_name_ptr = object_name;
    object_name_length = candidate.object.qualified_name_ref.length;
  }
  if (candidate.render == QST_RUNTIME_RENDER_QUALIFIED_TABLE) {
    code = qstGeneratorRuntimeGetParent(build, &candidate.object, &parent);
    if (code != QST_OK ||
        parent.qualified_name_ref.length >= sizeof(parent_name)) {
      return code != QST_OK ? code : QST_E_SQL_LIMIT;
    }
    parent_name_ptr = qstGeneratorRuntimeObjectName(build, &parent,
                                                    &cached_name_length);
    if (parent_name_ptr == NULL) {
      if (qst_dataset_copy_object_name(build->runtime->mapping, &parent,
                                       parent_name, sizeof(parent_name)) != QST_OK) {
        return QST_E_SHM_CORRUPT;
      }
      parent_name_ptr = parent_name;
    }
  }
  if (candidate.render == QST_RUNTIME_RENDER_TABLE_COLUMN ||
      candidate.render == QST_RUNTIME_RENDER_TABLE_ASTERISK ||
      candidate.render == QST_RUNTIME_RENDER_TABLE_TBNAME) {
    if (candidate.table.qualified_name_ref.length >= sizeof(table_name)) {
      return QST_E_SQL_LIMIT;
    }
    table_name_ptr = qstGeneratorRuntimeObjectName(
        build, &candidate.table, &cached_name_length);
    if (table_name_ptr == NULL) {
      if (qst_dataset_copy_object_name(build->runtime->mapping, &candidate.table,
                                       table_name, sizeof(table_name)) != QST_OK) {
        return QST_E_SHM_CORRUPT;
      }
      table_name_ptr = table_name;
    }
  }
  switch (candidate.render) {
    case QST_RUNTIME_RENDER_NAME:
    case QST_RUNTIME_RENDER_BARE_COLUMN:
      append_text = object_name_ptr;
      written = (int32_t)object_name_length;
      break;
    case QST_RUNTIME_RENDER_QUALIFIED_TABLE:
      written = qstGeneratorRuntimeNameHasParentPrefix(object_name_ptr,
                                                        parent_name_ptr) ?
                    snprintf(text, sizeof(text), "%s", object_name_ptr) :
                    snprintf(text, sizeof(text), "%s.%s", parent_name_ptr,
                             object_name_ptr);
      break;
    case QST_RUNTIME_RENDER_TABLE_COLUMN:
      written = snprintf(text, sizeof(text), "%s.%s", table_name_ptr,
                         object_name_ptr);
      break;
    case QST_RUNTIME_RENDER_ALIAS_COLUMN:
      written = snprintf(text, sizeof(text), "%s.%s",
                         scope->tables[candidate.table_index].alias,
                         object_name_ptr);
      break;
    case QST_RUNTIME_RENDER_TABLE_ASTERISK:
      written = snprintf(text, sizeof(text), "%s.*", table_name_ptr);
      break;
    case QST_RUNTIME_RENDER_ALIAS_ASTERISK:
      written = snprintf(text, sizeof(text), "%s.*",
                         scope->tables[candidate.table_index].alias);
      break;
    case QST_RUNTIME_RENDER_TABLE_TBNAME:
      written = snprintf(text, sizeof(text), "%s.tbname", table_name_ptr);
      break;
    case QST_RUNTIME_RENDER_ALIAS_TBNAME:
      written = snprintf(text, sizeof(text), "%s.tbname",
                         scope->tables[candidate.table_index].alias);
      break;
    case QST_RUNTIME_RENDER_ROLLUP_TAG:
      written = snprintf(text, sizeof(text), "%%%%%s", object_name_ptr);
      break;
    default:
      return QST_E_INVALID_ARGUMENT;
  }
  if (written < 0 || (uint64_t)written >= sizeof(text)) {
    return QST_E_SQL_LIMIT;
  }
  code = qstGeneratorAppendBytes(build->output, append_text, (uint32_t)written);
  if (code != QST_OK) {
    return code;
  }
  if (runtime_object_kind == QST_RUNTIME_OBJECT_SOURCE) {
    scope->source = candidate.object;
    scope->has_source = 1;
    qstGeneratorBuildabilityReset(build);
  } else if (runtime_object_kind == QST_RUNTIME_OBJECT_TABLE ||
             runtime_object_kind == QST_RUNTIME_OBJECT_TABLE_REFERENCE ||
             runtime_object_kind == QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX) {
    if (scope->default_table_seeded != 0) {
      scope->table_count = 0;
      scope->has_last_table = 0;
      scope->default_table_seeded = 0;
    }
    scope->last_table = candidate.object;
    scope->has_last_table = 1;
    if (qstGeneratorScopeAddTable(build, &candidate.object) != QST_OK) {
      return QST_E_RESOURCE;
    }
    qstGeneratorBuildabilityReset(build);
  }
  if (NULL != occurrence) {
    occurrence->runtime_object_ordinal = candidate.object.ordinal;
    occurrence->runtime_object_id = candidate.object.object_id;
  }
  return QST_OK;
}

static int32_t qstGeneratorIsValueEligible(const SQstGeneratorValue *value,
                                           uint32_t depth) {
  if (NULL == value || value->kind < QST_GENERATOR_VALUE_ATOMIC ||
      value->kind > QST_GENERATOR_VALUE_COMPOSE ||
      value->generation_class < QST_GENERATOR_CLASS_NORMAL ||
      value->generation_class > QST_GENERATOR_CLASS_ABNORMAL ||
      value->return_state < QST_GENERATOR_RETURN_NON_EMPTY ||
      value->return_state > QST_GENERATOR_RETURN_STOP_CURRENT ||
      (value->dialect_mask & ~QST_GENERATOR_DIALECT_MASK_ALL) != 0 ||
      value->step_count > UINT32_MAX - value->child_count ||
      (value->child_count != 0 && NULL == value->children) ||
      (value->step_count != 0 && NULL == value->steps) ||
      (value->child_count != 0 && value->step_count != 0) ||
      ((value->kind == QST_GENERATOR_VALUE_ATOMIC ||
        value->kind == QST_GENERATOR_VALUE_FIXED_TOKEN ||
        value->kind == QST_GENERATOR_VALUE_CALL_SELF) &&
       (value->child_count != 0 || value->step_count != 0)) ||
      (value->kind == QST_GENERATOR_VALUE_CALL_DIMENSION &&
       (value->child_count != 1 || value->step_count != 0))) {
    return 0;
  }
  if (depth == 0 && value->kind == QST_GENERATOR_VALUE_CALL_SELF) {
    return 0;
  }
  for (uint32_t child = 0; child < value->child_count; ++child) {
    if (!qstGeneratorIsChildCallValid(&value->children[child]) ||
        (depth == 0 && value->children[child].recursion_cost != 0 &&
         !(value->children[child].call_kind == QST_GENERATOR_CALL_LOOP &&
           value->children[child].min_iterations == 0))) {
      return 0;
    }
  }
  for (uint32_t step = 0; step < value->step_count; ++step) {
    if (!qstGeneratorIsStepValid(&value->steps[step]) ||
        (value->steps[step].kind == QST_GENERATOR_STEP_CALL &&
         depth == 0 && value->steps[step].child_call.recursion_cost != 0 &&
         !(value->steps[step].child_call.call_kind == QST_GENERATOR_CALL_LOOP &&
           value->steps[step].child_call.min_iterations == 0))) {
      return 0;
    }
  }
  return 1;
}

static int32_t qstGeneratorIsChildCallValid(
    const SQstGeneratorChildCall *child_call) {
  if (NULL == child_call || child_call->fragment_id == 0 ||
      child_call->call_kind < QST_GENERATOR_CALL_REQUIRED ||
      child_call->call_kind > QST_GENERATOR_CALL_LOOP ||
      child_call->recursion_cost > 1 ||
      child_call->scope_action > QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS ||
      (child_call->scope_action != QST_GENERATOR_SCOPE_NONE &&
       child_call->call_kind == QST_GENERATOR_CALL_LOOP)) {
    return 0;
  }
  if (child_call->call_kind != QST_GENERATOR_CALL_LOOP) {
    return child_call->min_iterations == 0 &&
           child_call->typical_iterations == 0 &&
           child_call->max_iterations == 0;
  }
  return child_call->max_iterations != 0 &&
         child_call->max_iterations <= QST_GENERATOR_MAX_LOOP_ITERATIONS &&
         child_call->min_iterations <= child_call->typical_iterations &&
         child_call->typical_iterations <= child_call->max_iterations;
}

static int32_t qstGeneratorIsStepValid(const SQstGeneratorStep *step) {
  if (NULL == step || step->kind < QST_GENERATOR_STEP_CALL ||
      step->kind > QST_GENERATOR_STEP_INLINE_VALUE) {
    return 0;
  }
  if (step->kind == QST_GENERATOR_STEP_CALL) {
    return step->value_id == 0 && step->text == NULL &&
           qstGeneratorIsChildCallValid(&step->child_call);
  }
  return step->value_id != 0 && step->text != NULL &&
         step->child_call.fragment_id == 0 && step->child_call.call_kind == 0 &&
         step->child_call.recursion_cost == 0 &&
         step->child_call.min_iterations == 0 &&
         step->child_call.typical_iterations == 0 &&
         step->child_call.max_iterations == 0;
}

void qst_generator_frag_plan_destroy(SQstGeneratorFragPlan *plan) {
  if (NULL == plan) {
    return;
  }
  free(plan->static_selection_rows);
  free(plan->static_selections);
  free(plan->static_selection_prefix_weights);
  free(plan->operations);
  free(plan->values);
  free(plan->fragments);
  memset(plan, 0, sizeof(*plan));
}

/*
 * Compile catalog pointers and all child targets once.  This is deliberately
 * separate from RANDOM execution: the execution stage must never search a
 * fragment id, inspect a VAL child array, or recompute depth eligibility.
 */
EQstCode qst_generator_frag_plan_prepare(SQstGeneratorFragPlan *plan,
                                         const SQstGeneratorCatalog *catalog) {
  uint64_t total_values = 0;
  uint64_t total_operations = 0;
  uint64_t selection_prefix_count = 0;
  uint64_t selection_fragment_count = 0;
  uint32_t value_cursor = 0;
  uint32_t operation_cursor = 0;
  EQstCode code = QST_OK;

  if (NULL == plan || NULL == catalog) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_generator_frag_plan_destroy(plan);
  code = qst_generator_catalog_validate(catalog);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];

    if (UINT64_MAX - total_values < fragment->value_count) {
      return QST_E_RESOURCE;
    }
    total_values += fragment->value_count;
    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      const SQstGeneratorValue *value = &fragment->values[value_index];

      if (UINT64_MAX - total_operations < value->child_count + value->step_count) {
        return QST_E_RESOURCE;
      }
      total_operations += value->child_count + value->step_count;
    }
  }
  if (total_values == 0 || total_values > UINT32_MAX ||
      total_operations > UINT32_MAX ||
      total_values > UINT64_MAX / QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT ||
      (uint64_t)catalog->fragment_count >
          UINT64_MAX / QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT ||
      (uint64_t)catalog->fragment_count > SIZE_MAX / sizeof(*plan->fragments) ||
      total_values > SIZE_MAX / sizeof(*plan->values) ||
      (total_operations != 0 &&
       total_operations > SIZE_MAX / sizeof(*plan->operations))) {
    return QST_E_RESOURCE;
  }
  selection_prefix_count = total_values * QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT;
  selection_fragment_count =
      (uint64_t)catalog->fragment_count * QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT;
  if (selection_prefix_count >
          SIZE_MAX / sizeof(*plan->static_selection_prefix_weights) ||
      selection_fragment_count >
          SIZE_MAX / sizeof(*plan->static_selections) ||
      selection_fragment_count >
          SIZE_MAX / sizeof(*plan->static_selection_rows)) {
    return QST_E_RESOURCE;
  }
  plan->fragments = calloc(catalog->fragment_count, sizeof(*plan->fragments));
  plan->values = calloc((size_t)total_values, sizeof(*plan->values));
  plan->static_selection_prefix_weights = calloc(
      (size_t)selection_prefix_count,
      sizeof(*plan->static_selection_prefix_weights));
  plan->static_selections = calloc(
      (size_t)selection_fragment_count,
      sizeof(*plan->static_selections));
  plan->static_selection_rows = calloc(
      QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT,
      sizeof(*plan->static_selection_rows));
  if (total_operations != 0) {
    plan->operations = calloc((size_t)total_operations,
                              sizeof(*plan->operations));
  }
  if (NULL == plan->fragments || NULL == plan->values ||
      NULL == plan->static_selection_prefix_weights ||
      NULL == plan->static_selections ||
      NULL == plan->static_selection_rows ||
      (total_operations != 0 && NULL == plan->operations)) {
    qst_generator_frag_plan_destroy(plan);
    return QST_E_RESOURCE;
  }
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];
    SQstGeneratorFragPlanEntry *entry = &plan->fragments[fragment_index];

    entry->fragment = fragment;
    entry->fragment_id = fragment->fragment_id;
    entry->ordered_index = fragment_index;
    entry->first_value_index = value_cursor;
    entry->value_count = fragment->value_count;
    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      const SQstGeneratorValue *value = &fragment->values[value_index];
      SQstGeneratorFragPlanValue *planned_value = &plan->values[value_cursor++];

      planned_value->value = value;
      planned_value->fragment_index = fragment_index;
      planned_value->value_index = value_index;
      planned_value->first_operation_index = operation_cursor;
      planned_value->operation_count = value->child_count + value->step_count;
      if (value->text != NULL) {
        const size_t text_length = strlen(value->text);

        if (text_length > UINT32_MAX) {
          qst_generator_frag_plan_destroy(plan);
          return QST_E_RESOURCE;
        }
        planned_value->text_length = (uint32_t)text_length;
      }
      if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
        entry->has_runtime_value = 1;
        plan->has_runtime_values = 1;
      }
      for (uint32_t depth = 0; depth <= QST_GENERATOR_MAX_RECURSION_DEPTH;
           ++depth) {
        if (qstGeneratorIsValueEligible(value, depth)) {
          planned_value->eligible_depth_mask |= UINT32_C(1) << depth;
        }
      }
      for (uint32_t child_index = 0; child_index < value->child_count;
           ++child_index) {
        SQstGeneratorFragPlanOperation *operation =
            &plan->operations[operation_cursor++];

        operation->child_call = value->children[child_index];
        operation->operation_kind = QST_GENERATOR_STEP_CALL;
        operation->operation_index = child_index;
        if (!qstGeneratorFindFragmentIndex(catalog,
                                           operation->child_call.fragment_id,
                                           &operation->child_fragment_index)) {
          qst_generator_frag_plan_destroy(plan);
          return QST_E_INVALID_ARGUMENT;
        }
      }
      for (uint32_t step_index = 0; step_index < value->step_count;
           ++step_index) {
        const SQstGeneratorStep *step = &value->steps[step_index];
        SQstGeneratorFragPlanOperation *operation =
            &plan->operations[operation_cursor++];

        operation->step = step;
        operation->operation_kind = step->kind;
        operation->operation_index = step_index;
        if (step->kind == QST_GENERATOR_STEP_INLINE_VALUE) {
          const size_t text_length = strlen(step->text);

          if (text_length > UINT32_MAX) {
            qst_generator_frag_plan_destroy(plan);
            return QST_E_RESOURCE;
          }
          operation->text_length = (uint32_t)text_length;
        }
        if (step->kind == QST_GENERATOR_STEP_CALL) {
          operation->child_call = step->child_call;
          if (!qstGeneratorFindFragmentIndex(catalog,
                                             operation->child_call.fragment_id,
                                             &operation->child_fragment_index)) {
            qst_generator_frag_plan_destroy(plan);
            return QST_E_INVALID_ARGUMENT;
          }
        }
      }
    }
  }
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragPlanEntry *entry = &plan->fragments[fragment_index];

    for (uint32_t depth = 0; depth <= QST_GENERATOR_MAX_RECURSION_DEPTH;
         ++depth) {
      for (uint32_t dialect_slot = 0;
           dialect_slot < QST_GENERATOR_PLAN_DIALECT_SLOT_COUNT;
           ++dialect_slot) {
        const uint64_t selection_slot =
            qstGeneratorPlanSelectionSlot(depth, dialect_slot);
        const uint64_t fragment_slot =
            selection_slot * catalog->fragment_count + fragment_index;
        SQstGeneratorFragPlanStaticSelection *selection =
            &plan->static_selections[fragment_slot];
        uint64_t total_weight = 0;
        uint32_t eligible_count = 0;
        uint32_t sole_value_index = UINT32_MAX;

        for (uint32_t value_index = 0; value_index < entry->value_count;
             ++value_index) {
          const uint32_t planned_index = entry->first_value_index + value_index;
          const SQstGeneratorFragPlanValue *value = &plan->values[planned_index];
          uint64_t *prefix =
              &plan->static_selection_prefix_weights[
                  selection_slot * total_values + planned_index];

          if (value->value->weight != 0) {
            if (UINT64_MAX - total_weight < value->value->weight) {
              qst_generator_frag_plan_destroy(plan);
              return QST_E_RESOURCE;
            }
            total_weight += value->value->weight;
            ++eligible_count;
            sole_value_index = planned_index;
          }
          *prefix = total_weight;
        }
        selection->prefix_weights =
            &plan->static_selection_prefix_weights[
                selection_slot * total_values + entry->first_value_index];
        selection->total_weight = total_weight;
        if (total_weight > 1U) {
          selection->reciprocal =
              (uint64_t)(((__uint128_t)1U << 64U) / total_weight);
        }
        selection->eligible_count = eligible_count;
        selection->sole_value_index =
            eligible_count == 1U ? sole_value_index : UINT32_MAX;
      }
    }
  }
  for (uint32_t selection_slot = 0;
       selection_slot < QST_GENERATOR_PLAN_SELECTION_SLOT_COUNT;
       ++selection_slot) {
    plan->static_selection_rows[selection_slot] =
        plan->static_selections +
        (uint64_t)selection_slot * catalog->fragment_count;
  }
  if (!qstGeneratorFindFragmentIndex(catalog, catalog->root_fragment_id,
                                     &plan->root_fragment_index) ||
      value_cursor != total_values || operation_cursor != total_operations) {
    qst_generator_frag_plan_destroy(plan);
    return QST_E_SHM_CORRUPT;
  }
  plan->catalog = catalog;
  plan->catalog_digest = catalog->digest;
  plan->fragment_count = catalog->fragment_count;
  plan->value_count = (uint32_t)total_values;
  plan->operation_count = (uint32_t)total_operations;
  return QST_OK;
}

static int qstGeneratorCompareBuildableValue(const void *left,
                                             const void *right) {
  const SQstGeneratorBuildableValue *left_value = left;
  const SQstGeneratorBuildableValue *right_value = right;

  if (left_value->value_id < right_value->value_id) return -1;
  if (left_value->value_id > right_value->value_id) return 1;
  return 0;
}

static int qstGeneratorCompareBuildableFragment(const void *left,
                                                const void *right) {
  const SQstGeneratorBuildableFragment *left_fragment = left;
  const SQstGeneratorBuildableFragment *right_fragment = right;

  if (left_fragment->fragment_id < right_fragment->fragment_id) return -1;
  if (left_fragment->fragment_id > right_fragment->fragment_id) return 1;
  return 0;
}

static int32_t qstGeneratorFindBuildableFragment(
    SQstGeneratorBuild *build, uint32_t fragment_id,
    uint32_t *out_fragment_index) {
  uint32_t lower = 0;
  uint32_t upper = 0;
  const uint32_t cache_slot = fragment_id &
                              (QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE - 1U);

  if (NULL == build || NULL == out_fragment_index ||
      NULL == build->buildable_fragment_index ||
      NULL == build->lookup_cache || build->catalog == NULL) {
    return 0;
  }
  if (build->lookup_cache->fragment_ids[cache_slot] == fragment_id) {
    *out_fragment_index = build->lookup_cache->fragment_indices[cache_slot];
    return 1;
  }
  upper = build->catalog->fragment_count;
  while (lower < upper) {
    const uint32_t middle = lower + (upper - lower) / 2U;
    const SQstGeneratorBuildableFragment *candidate =
        &build->buildable_fragment_index[middle];

    if (candidate->fragment_id == fragment_id) {
      *out_fragment_index = candidate->fragment_index;
      build->lookup_cache->fragment_ids[cache_slot] = fragment_id;
      build->lookup_cache->fragment_indices[cache_slot] = candidate->fragment_index;
      return 1;
    }
    if (candidate->fragment_id < fragment_id) {
      lower = middle + 1U;
    } else {
      upper = middle;
    }
  }
  return 0;
}

static int32_t qstGeneratorFindBuildableValue(
    SQstGeneratorBuild *build, const SQstGeneratorValue *value,
    uint32_t *out_index) {
  uint32_t lower = 0;
  uint32_t upper = 0;
  const uint32_t cache_slot = ((uintptr_t)value >> 3U) &
                              (QST_GENERATOR_BUILDABLE_VALUE_CACHE_SIZE - 1U);

  if (NULL == build || NULL == value || NULL == out_index ||
      NULL == build->buildable_value_index ||
      NULL == build->lookup_cache || build->buildable_value_count == 0) {
    return 0;
  }
  if (build->lookup_cache->values[cache_slot] == value) {
    *out_index = build->lookup_cache->value_indices[cache_slot];
    return 1;
  }
  upper = build->buildable_value_count;
  while (lower < upper) {
    const uint32_t middle = lower + (upper - lower) / 2U;
    const SQstGeneratorBuildableValue *candidate =
        &build->buildable_value_index[middle];

    if (candidate->value_id == value->value_id) {
      if (candidate->value != value) return 0;
      *out_index = middle;
      build->lookup_cache->values[cache_slot] = value;
      build->lookup_cache->value_indices[cache_slot] = middle;
      return 1;
    }
    if (candidate->value_id < value->value_id) {
      lower = middle + 1U;
    } else {
      upper = middle;
    }
  }
  return 0;
}

/* Returns whether a fragment has a complete candidate at the given depth. */
static int32_t qstGeneratorFragmentCanBuild(SQstGeneratorBuild *build,
                                            uint32_t fragment_id,
                                            uint32_t depth) {
  uint32_t fragment_index = 0;
  uint64_t state_index = 0;
  uint8_t *state = NULL;
  const SQstGeneratorFragment *fragment = NULL;

  if (NULL == build || NULL == build->catalog ||
      NULL == build->buildable_fragments ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH ||
      !qstGeneratorFindBuildableFragment(build, fragment_id,
                                          &fragment_index)) {
    return 0;
  }
  state_index = (uint64_t)fragment_index *
                    (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) +
                depth;
  state = &build->buildable_fragments[state_index];
  if (build->buildable_fragment_epochs[state_index] !=
      build->buildability_epoch) {
    build->buildable_fragment_epochs[state_index] = build->buildability_epoch;
    *state = 0;
  }
  if (*state == 2U) {
    return 1;
  }
  if (*state != 0U) {
    return 0;
  }
  *state = 1U;
  fragment = &build->catalog->fragments[fragment_index];
  for (uint32_t index = 0; index < fragment->value_count; ++index) {
    if (qstGeneratorValueCanBuild(build, &fragment->values[index], depth)) {
      *state = 2U;
      return 1;
    }
  }
  *state = 3U;
  return 0;
}

/* Applies the active depth limit through all required and loop child calls. */
static int32_t qstGeneratorValueCanBuild(SQstGeneratorBuild *build,
                                         const SQstGeneratorValue *value,
                                         uint32_t depth) {
  uint32_t total_children = 0;
  uint32_t runtime_count = 0;
  uint32_t value_index = 0;
  uint8_t *state = NULL;
  EQstCode code = QST_OK;

  if (NULL == build || NULL == value) {
    return 0;
  }
  if (build->dialect_mask != 0 && value->dialect_mask != 0 &&
      (build->dialect_mask & value->dialect_mask) == 0) {
    return 0;
  }
  if (NULL == build->buildable_values ||
      !qstGeneratorFindBuildableValue(build, value, &value_index)) {
    return 0;
  }
  state = &build->buildable_values[(uint64_t)value_index *
                                       (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) +
                                   depth];
  if (build->buildable_value_epochs[(uint64_t)value_index *
                                        (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) +
                                    depth] != build->buildability_epoch) {
    build->buildable_value_epochs[(uint64_t)value_index *
                                      (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) +
                                  depth] = build->buildability_epoch;
    *state = 0;
  }
  /* Scope changes reset this state before they can alter runtime candidates.
   * Reuse a known answer before repeating catalog and runtime-object scans. */
  if (*state == 2U) return 1;
  if (*state != 0U) return 0;
  if (build->buildable_value_eligibility != NULL &&
      build->buildable_value_eligibility[(uint64_t)value_index *
                                             (QST_GENERATOR_MAX_RECURSION_DEPTH +
                                              1U) +
                                         depth] == 0) {
    *state = 3U;
    return 0;
  }
  if (build->buildable_value_eligibility == NULL &&
      !qstGeneratorIsValueEligible(value, depth)) {
    *state = 3U;
    return 0;
  }
  /* A target-private cached eligibility table is catalog-invariant. The direct
   * builder and FULL enumerator retain their runtime-candidate checks, while
   * the prepared RANDOM plan selects from its immutable weight rows directly. */
  if (build->buildable_value_eligibility != NULL) {
    if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
      code = qstGeneratorValueRuntimeCandidateCount(build, value, &runtime_count);
      if (code != QST_OK || runtime_count == 0) {
        *state = 3U;
        return 0;
      }
    }
    *state = 2U;
    return 1;
  }
  if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
    code = qstGeneratorValueRuntimeCandidateCount(build, value, &runtime_count);
    if (code != QST_OK || runtime_count == 0) {
      *state = 3U;
      return 0;
    }
  }
  *state = 1U;
  /* Runtime candidates depend on bindings established by earlier siblings.
   * qstGeneratorFragmentCanBuild is a structural look-ahead, so it must not
   * reject an otherwise reachable value before those siblings execute. */
  total_children = value->child_count + value->step_count;
  for (uint32_t index = 0; index < total_children; ++index) {
    const SQstGeneratorChildCall *child_call = NULL;
    uint32_t child_depth = depth;

    if (index < value->child_count) {
      child_call = &value->children[index];
    } else {
      const SQstGeneratorStep *step =
          &value->steps[index - value->child_count];

      if (step->kind != QST_GENERATOR_STEP_CALL) {
        continue;
      }
      child_call = &step->child_call;
    }
    if (child_call->recursion_cost != 0) {
      if (depth == 0) {
        /* qstGeneratorBuildChildCall records the required terminal cut. */
        if (child_call->call_kind == QST_GENERATOR_CALL_LOOP &&
            child_call->min_iterations == 0) {
          continue;
        }
        *state = 3U;
        return 0;
      }
      child_depth = depth - 1U;
    }
    if (!qstGeneratorFragmentCanBuild(build, child_call->fragment_id,
                                      child_depth)) {
      *state = 3U;
      return 0;
    }
  }
  *state = 2U;
  return 1;
}

static EQstCode qstGeneratorBuildabilityInitialize(SQstGeneratorBuild *build) {
  uint64_t state_count = 0;
  uint64_t value_count = 0;
  int32_t indexes_prepared = 0;
  int32_t state_prepared = 0;

  if (NULL == build || NULL == build->catalog ||
      build->catalog->fragment_count == 0 ||
      build->catalog->fragment_count >
          UINT64_MAX / (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U)) {
    return QST_E_INVALID_ARGUMENT;
  }
  state_count = (uint64_t)build->catalog->fragment_count *
                (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U);
  for (uint32_t fragment_index = 0;
       fragment_index < build->catalog->fragment_count; ++fragment_index) {
    value_count += build->catalog->fragments[fragment_index].value_count;
  }
  if (state_count > SIZE_MAX || value_count == 0 || value_count > UINT32_MAX ||
      value_count > UINT64_MAX / (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) ||
      state_count > SIZE_MAX / sizeof(*build->buildable_fragment_epochs) ||
      value_count > SIZE_MAX / (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) ||
      value_count * (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) >
          SIZE_MAX / sizeof(*build->buildable_value_epochs) ||
      value_count > SIZE_MAX / sizeof(*build->buildable_value_index) ||
      build->catalog->fragment_count >
          SIZE_MAX / sizeof(*build->buildable_fragment_index)) {
    return QST_E_RESOURCE;
  }
  if (build->buildable_value_index == NULL &&
      build->buildable_fragment_index == NULL) {
    if (pthread_mutex_lock(&g_buildable_index_mutex) != 0) {
      return QST_E_RESOURCE;
    }
    if (g_buildable_index_catalog == build->catalog &&
        g_buildable_index_catalog_version == build->catalog->version &&
        g_buildable_index_fragment_count == build->catalog->fragment_count &&
        memcmp(g_buildable_index_catalog_digest.bytes,
               build->catalog->digest.bytes,
               sizeof(g_buildable_index_catalog_digest.bytes)) == 0) {
      build->buildable_value_index = g_buildable_value_index;
      build->buildable_fragment_index = g_buildable_fragment_index;
      build->buildable_value_count = g_buildable_value_count;
    }
    if (pthread_mutex_unlock(&g_buildable_index_mutex) != 0) {
      return QST_E_RESOURCE;
    }
  }
  indexes_prepared = build->buildable_value_index != NULL ||
                     build->buildable_fragment_index != NULL;
  if (indexes_prepared != 0 &&
      (NULL == build->buildable_value_index ||
       NULL == build->buildable_fragment_index ||
       build->buildable_value_count != value_count)) {
    return QST_E_INVALID_ARGUMENT;
  }
  state_prepared = build->buildable_fragments != NULL ||
                   build->buildable_fragment_epochs != NULL ||
                   build->buildable_values != NULL ||
                   build->buildable_value_epochs != NULL;
  if (state_prepared != 0 &&
      (NULL == build->buildable_fragments ||
       NULL == build->buildable_fragment_epochs ||
       NULL == build->buildable_values ||
       NULL == build->buildable_value_epochs ||
       build->buildability_epoch == 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL == build->lookup_cache) {
    build->lookup_cache = calloc(1, sizeof(*build->lookup_cache));
    if (NULL == build->lookup_cache) {
      qstGeneratorBuildDestroy(build);
      return QST_E_RESOURCE;
    }
    build->lookup_cache_owned = 1;
  }
  if (state_prepared == 0) {
    build->buildable_fragments = calloc((size_t)state_count, 1);
    build->buildable_fragment_epochs = calloc(
        (size_t)state_count, sizeof(*build->buildable_fragment_epochs));
    build->buildable_values = calloc(
        (size_t)(value_count * (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U)), 1);
    build->buildable_value_epochs = calloc(
        (size_t)(value_count * (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U)),
        sizeof(*build->buildable_value_epochs));
    build->buildability_state_owned = 1;
  }
  if (indexes_prepared == 0) {
    build->buildable_value_index = calloc(
        (size_t)value_count, sizeof(*build->buildable_value_index));
    build->buildable_fragment_index = calloc(
        build->catalog->fragment_count, sizeof(*build->buildable_fragment_index));
    build->buildable_indexes_owned = 1;
  }
  if (NULL == build->buildable_fragments ||
      NULL == build->buildable_fragment_epochs ||
      NULL == build->buildable_values ||
      NULL == build->buildable_value_epochs ||
      NULL == build->buildable_value_index ||
      NULL == build->buildable_fragment_index) {
    qstGeneratorBuildDestroy(build);
    return QST_E_RESOURCE;
  }
  if (indexes_prepared == 0) {
    SQstGeneratorBuildableValue *values =
        (SQstGeneratorBuildableValue *)build->buildable_value_index;
    SQstGeneratorBuildableFragment *fragments =
        (SQstGeneratorBuildableFragment *)build->buildable_fragment_index;

    for (uint32_t fragment_index = 0;
         fragment_index < build->catalog->fragment_count; ++fragment_index) {
      const SQstGeneratorFragment *fragment =
          &build->catalog->fragments[fragment_index];

      fragments[fragment_index].fragment_id = fragment->fragment_id;
      fragments[fragment_index].fragment_index = fragment_index;
      for (uint32_t value_index = 0; value_index < fragment->value_count;
           ++value_index) {
        values[build->buildable_value_count].value_id =
            fragment->values[value_index].value_id;
        values[build->buildable_value_count].value =
            &fragment->values[value_index];
        ++build->buildable_value_count;
      }
    }
    qsort(values, build->buildable_value_count, sizeof(*values),
          qstGeneratorCompareBuildableValue);
    qsort(fragments, build->catalog->fragment_count, sizeof(*fragments),
          qstGeneratorCompareBuildableFragment);
    if (pthread_mutex_lock(&g_buildable_index_mutex) != 0) {
      qstGeneratorBuildDestroy(build);
      return QST_E_RESOURCE;
    }
    if (g_buildable_index_catalog == NULL) {
      g_buildable_index_catalog = build->catalog;
      g_buildable_value_index = values;
      g_buildable_fragment_index = fragments;
      g_buildable_value_count = build->buildable_value_count;
      g_buildable_index_catalog_version = build->catalog->version;
      g_buildable_index_fragment_count = build->catalog->fragment_count;
      g_buildable_index_catalog_digest = build->catalog->digest;
      build->buildable_indexes_owned = 0;
    }
    if (pthread_mutex_unlock(&g_buildable_index_mutex) != 0) {
      qstGeneratorBuildDestroy(build);
      return QST_E_RESOURCE;
    }
  }
  if (state_prepared == 0) {
    build->buildability_epoch = 1U;
  }
  return QST_OK;
}

/* Builds the catalog-only lookup indexes once for a RANDOM worker lifetime. */
static EQstCode qstGeneratorRandomRuntimeInitializeBuildabilitySlow(
    SQstGeneratorRandomRuntime *runtime, const SQstGeneratorCatalog *catalog) {
  SQstGeneratorBuild build = {0};
  SQstGeneratorBuildableValue *value_index = NULL;
  SQstGeneratorBuildableFragment *fragment_index = NULL;
  uint8_t *eligibility = NULL;
  uint8_t *buildable_fragments = NULL;
  uint32_t *buildable_fragment_epochs = NULL;
  uint8_t *buildable_values = NULL;
  uint32_t *buildable_value_epochs = NULL;
  SQstGeneratorBuildLookupCache *lookup_cache = NULL;
  uint64_t fragment_state_count = 0;
  uint64_t value_state_count = 0;
  EQstCode code = QST_OK;

  if (NULL == runtime || NULL == catalog) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != runtime->catalog_value_index ||
      NULL != runtime->catalog_fragment_index ||
      NULL != runtime->catalog_value_eligibility ||
      runtime->catalog_value_count != 0 ||
      NULL != runtime->buildable_fragments ||
      NULL != runtime->buildable_fragment_epochs ||
      NULL != runtime->buildable_values ||
      NULL != runtime->buildable_value_epochs ||
      NULL != runtime->buildable_lookup_cache ||
      runtime->buildability_epoch != 0) {
    return NULL != runtime->catalog_value_index &&
                   NULL != runtime->catalog_fragment_index &&
                   NULL != runtime->catalog_value_eligibility &&
                   runtime->catalog_value_count != 0 &&
                   NULL != runtime->buildable_fragments &&
                   NULL != runtime->buildable_fragment_epochs &&
                   NULL != runtime->buildable_values &&
                   NULL != runtime->buildable_value_epochs &&
                   NULL != runtime->buildable_lookup_cache &&
                   runtime->buildability_epoch != 0 ?
               QST_OK :
               QST_E_SHM_CORRUPT;
  }
  build.catalog = catalog;
  code = qstGeneratorBuildabilityInitialize(&build);
  if (code != QST_OK) {
    return code;
  }
  eligibility = calloc(
      (size_t)((uint64_t)build.buildable_value_count *
               (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U)), 1);
  if (NULL == eligibility) {
    qstGeneratorBuildDestroy(&build);
    return QST_E_RESOURCE;
  }
  value_index = calloc(build.buildable_value_count, sizeof(*value_index));
  fragment_index = calloc(catalog->fragment_count, sizeof(*fragment_index));
  if (NULL == value_index || NULL == fragment_index) {
    free(value_index);
    free(fragment_index);
    free(eligibility);
    qstGeneratorBuildDestroy(&build);
    return QST_E_RESOURCE;
  }
  fragment_state_count = (uint64_t)catalog->fragment_count *
                         (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U);
  value_state_count = (uint64_t)build.buildable_value_count *
                      (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U);
  buildable_fragments = calloc((size_t)fragment_state_count, 1);
  buildable_fragment_epochs = calloc(
      (size_t)fragment_state_count, sizeof(*buildable_fragment_epochs));
  buildable_values = calloc((size_t)value_state_count, 1);
  buildable_value_epochs = calloc(
      (size_t)value_state_count, sizeof(*buildable_value_epochs));
  lookup_cache = calloc(1, sizeof(*lookup_cache));
  if (NULL == buildable_fragments || NULL == buildable_fragment_epochs ||
      NULL == buildable_values || NULL == buildable_value_epochs ||
      NULL == lookup_cache) {
    free(buildable_fragments);
    free(buildable_fragment_epochs);
    free(buildable_values);
    free(buildable_value_epochs);
    free(lookup_cache);
    free(value_index);
    free(fragment_index);
    free(eligibility);
    qstGeneratorBuildDestroy(&build);
    return QST_E_RESOURCE;
  }
  memcpy(value_index, build.buildable_value_index,
         (size_t)build.buildable_value_count * sizeof(*value_index));
  memcpy(fragment_index, build.buildable_fragment_index,
         (size_t)catalog->fragment_count * sizeof(*fragment_index));
  for (uint32_t value_index = 0;
       value_index < build.buildable_value_count; ++value_index) {
    for (uint32_t depth = 0; depth <= QST_GENERATOR_MAX_RECURSION_DEPTH;
         ++depth) {
      eligibility[(uint64_t)value_index *
                      (QST_GENERATOR_MAX_RECURSION_DEPTH + 1U) +
                  depth] = (uint8_t)qstGeneratorIsValueEligible(
          build.buildable_value_index[value_index].value, depth);
    }
  }
  runtime->catalog_value_index = value_index;
  runtime->catalog_value_count = build.buildable_value_count;
  runtime->catalog_fragment_index = fragment_index;
  runtime->catalog_value_eligibility = eligibility;
  runtime->buildable_fragments = buildable_fragments;
  runtime->buildable_fragment_epochs = buildable_fragment_epochs;
  runtime->buildable_values = buildable_values;
  runtime->buildable_value_epochs = buildable_value_epochs;
  runtime->buildable_lookup_cache = lookup_cache;
  runtime->buildability_epoch = 1U;
  qstGeneratorBuildDestroy(&build);
  return QST_OK;
}

/* Keeps the per-SQL cache hit out of the large first-time build frame. */
static EQstCode qstGeneratorRandomRuntimeInitializeBuildability(
    SQstGeneratorRandomRuntime *runtime, const SQstGeneratorCatalog *catalog) {
  if (NULL == runtime || NULL == catalog) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != runtime->catalog_value_index ||
      NULL != runtime->catalog_fragment_index ||
      NULL != runtime->catalog_value_eligibility ||
      runtime->catalog_value_count != 0 ||
      NULL != runtime->buildable_fragments ||
      NULL != runtime->buildable_fragment_epochs ||
      NULL != runtime->buildable_values ||
      NULL != runtime->buildable_value_epochs ||
      NULL != runtime->buildable_lookup_cache ||
      runtime->buildability_epoch != 0) {
    return NULL != runtime->catalog_value_index &&
                   NULL != runtime->catalog_fragment_index &&
                   NULL != runtime->catalog_value_eligibility &&
                   runtime->catalog_value_count != 0 &&
                   NULL != runtime->buildable_fragments &&
                   NULL != runtime->buildable_fragment_epochs &&
                   NULL != runtime->buildable_values &&
                   NULL != runtime->buildable_value_epochs &&
                   NULL != runtime->buildable_lookup_cache &&
                   runtime->buildability_epoch != 0 ?
               QST_OK :
               QST_E_SHM_CORRUPT;
  }
  return qstGeneratorRandomRuntimeInitializeBuildabilitySlow(runtime, catalog);
}

EQstCode qst_generator_random_runtime_prepare(
    SQstGeneratorRandomRuntime *runtime, const SQstGeneratorCatalog *catalog) {
  const int32_t profile_enabled = runtime != NULL ? runtime->profile_enabled : 0;
  const uint32_t disable_path_metadata =
      runtime != NULL ? runtime->disable_path_metadata : 0;
  EQstCode code = QST_OK;

  if (NULL == runtime || NULL == catalog) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (runtime->validated_catalog == catalog &&
      memcmp(runtime->validated_catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) == 0) {
    code = qstGeneratorRandomRuntimeInitializeBuildability(runtime, catalog);
    if (code == QST_OK && runtime->catalog_runtime_values_prepared == 0) {
      runtime->catalog_has_runtime_values =
          qstGeneratorCatalogHasRuntimeObject(catalog) != 0 ? 1U : 0U;
      runtime->catalog_runtime_values_prepared = 1;
    }
    return code;
  }
  qst_generator_random_runtime_destroy(runtime);
  runtime->profile_enabled = profile_enabled;
  runtime->disable_path_metadata = disable_path_metadata;
  code = qst_generator_catalog_validate(catalog);
  if (code != QST_OK) {
    return code;
  }
  runtime->validated_catalog = catalog;
  runtime->validated_catalog_digest = catalog->digest;
  runtime->catalog_has_runtime_values =
      qstGeneratorCatalogHasRuntimeObject(catalog) != 0 ? 1U : 0U;
  runtime->catalog_runtime_values_prepared = 1;
  code = qstGeneratorRandomRuntimeInitializeBuildability(runtime, catalog);
  if (code != QST_OK) {
    qst_generator_random_runtime_destroy(runtime);
  }
  return code;
}

static inline int32_t qstGeneratorIsAsciiSpace(unsigned char ch) {
  return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' ||
         ch == '\v';
}

static inline int32_t qstGeneratorNeedsSeparator(const SQstGeneratedSql *output,
                                                 const char *text) {
  char previous = '\0';
  char next = '\0';

  if (output->sql_length == 0 || text[0] == '\0') {
    return 0;
  }
  previous = output->sql[output->sql_length - 1U];
  next = text[0];
  if (qstGeneratorIsAsciiSpace((unsigned char)previous) ||
      qstGeneratorIsAsciiSpace((unsigned char)next)) {
    return 0;
  }
  if (previous == '(' || previous == '.' ||
      next == ')' || next == ',' || next == '.' || next == ';') {
    return 0;
  }
  return 1;
}

static inline __attribute__((always_inline)) EQstCode qstGeneratorAppendBytes(
    SQstGeneratedSql *output, const char *text, uint32_t text_length) {
  uint32_t separator_bytes = 0;

  if (NULL == output || NULL == text) {
    return QST_E_INVALID_ARGUMENT;
  }
  separator_bytes = qstGeneratorNeedsSeparator(output, text) ? 1U : 0U;
  if (text_length > QST_GENERATOR_EXECUTABLE_SQL_BYTES - 1U -
                        output->sql_length ||
      separator_bytes > QST_GENERATOR_EXECUTABLE_SQL_BYTES - 1U -
                            output->sql_length - text_length) {
    return QST_E_SQL_LIMIT;
  }
  if (separator_bytes != 0) {
    output->sql[output->sql_length++] = ' ';
  }
  memcpy(output->sql + output->sql_length, text, (size_t)text_length);
  output->sql_length += (uint32_t)text_length;
  output->sql[output->sql_length] = '\0';
  return QST_OK;
}

/* The recursive RANDOM walker only consumes the buffer by bounded length.
 * Defer the trailing NUL store until finalization to avoid one store per token. */
static inline __attribute__((always_inline)) EQstCode
qstGeneratorAppendBytesFast(SQstGeneratedSql *output, const char *text,
                            uint32_t text_length) {
  uint32_t separator_bytes = 0;

  if (NULL == output || NULL == text) {
    return QST_E_INVALID_ARGUMENT;
  }
  separator_bytes = qstGeneratorNeedsSeparator(output, text) ? 1U : 0U;
  if (text_length > QST_GENERATOR_EXECUTABLE_SQL_BYTES - 1U -
                        output->sql_length ||
      separator_bytes > QST_GENERATOR_EXECUTABLE_SQL_BYTES - 1U -
                            output->sql_length - text_length) {
    return QST_E_SQL_LIMIT;
  }
  if (separator_bytes != 0) {
    output->sql[output->sql_length++] = ' ';
  }
  memcpy(output->sql + output->sql_length, text, (size_t)text_length);
  output->sql_length += text_length;
  return QST_OK;
}

/* Prepared FRAG values have a validated text pointer and precomputed length. */
static inline EQstCode qstGeneratorAppend(SQstGeneratedSql *output, const char *text) {
  const size_t text_length = text != NULL ? strlen(text) : 0;

  if (text_length > UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  return qstGeneratorAppendBytes(output, text, (uint32_t)text_length);
}

/* A deletion-only path is a negative SQL case, never an empty work item. */
static EQstCode qstGeneratorEnsureStatement(SQstGeneratedSql *output) {
  uint32_t index = 0;

  if (NULL == output) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (index = 0; index < output->sql_length; ++index) {
    if (!isspace((unsigned char)output->sql[index])) {
      return QST_OK;
    }
  }
  if (output->sql_length < QST_GENERATOR_SQL_BYTES - 1U) {
    output->sql[output->sql_length++] = '?';
    output->sql[output->sql_length] = '\0';
  } else if (output->sql_length != 0) {
    output->sql[output->sql_length - 1U] = '?';
  } else {
    return QST_E_RESOURCE;
  }
  output->contains_abnormal = 1;
  return QST_OK;
}

/* Converts a dynamic-infeasible or bounded-SQL branch into one short negative
 * statement. Choices remain observable, but occurrence text spans no longer
 * describe the replacement SQL and are therefore explicitly cleared. */
static EQstCode qstGeneratorFinalizeUnsupportedPath(SQstGeneratedSql *output) {
  if (NULL == output) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (output->occurrence_count != 0 && NULL == output->occurrences) {
    return QST_E_INTERNAL;
  }
  output->sql[0] = '?';
  output->sql[1] = '\0';
  output->sql_length = 1;
  for (uint32_t index = 0; index < output->occurrence_count; ++index) {
    output->occurrences[index].text_offset = 0;
    output->occurrences[index].text_length = 0;
  }
  output->contains_abnormal = 1;
  output->contains_unsupported_fallback = 1;
  return QST_OK;
}

/* Hint option names are reserved parser tokens only inside hint comments.
 * A generated SQL path can legally exercise an invalid/abnormal combination,
 * but passing a bare hint token to older client libraries can abort before the
 * server returns a syntax error. Normalize that tool-side combination to the
 * existing short negative statement while retaining the path as abnormal. */
static int32_t qstGeneratorIsUnsafeBareHintToken(const char *text,
                                                 uint32_t length) {
  static const char *const kHintTokens[] = {
      "BATCH_SCAN",          "NO_BATCH_SCAN",      "SORT_FOR_GROUP",
      "PARTITION_FIRST",     "PARA_TABLES_SORT",   "SMALLDATA_TS_SORT",
      "SMALLDATA_SCAN_SORT", "HASH_JOIN",         "SKIP_TSMA",
      "WIN_OPTIMIZE_BATCH",  "WIN_OPTIMIZE_SINGLE", "REMOVE_PARTITION",
  };

  for (uint32_t index = 0;
       index < sizeof(kHintTokens) / sizeof(kHintTokens[0]); ++index) {
    const size_t token_length = strlen(kHintTokens[index]);

    if (token_length == length &&
        memcmp(text, kHintTokens[index], token_length) == 0) {
      return 1;
    }
  }
  return 0;
}

/* Replaces a bare internal hint token emitted by an invalid generator
 * composition. Quotes and comments are skipped so ordinary string/comment
 * payloads remain untouched. */
static EQstCode qstGeneratorNormalizeBareHintTokens(SQstGeneratedSql *output) {
  uint32_t index = 0;

  if (NULL == output) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (index < output->sql_length) {
    const char current = output->sql[index];

    if (current == '\'' || current == '"' || current == '`') {
      const char delimiter = current;

      ++index;
      while (index < output->sql_length) {
        if (output->sql[index] == '\\' && delimiter != '`' &&
            index + 1U < output->sql_length) {
          index += 2U;
          continue;
        }
        if (output->sql[index] == delimiter) {
          if (index + 1U < output->sql_length &&
              output->sql[index + 1U] == delimiter) {
            index += 2U;
          } else {
            ++index;
            break;
          }
        } else {
          ++index;
        }
      }
      continue;
    }
    if (current == '-' && index + 1U < output->sql_length &&
        output->sql[index + 1U] == '-') {
      index += 2U;
      while (index < output->sql_length && output->sql[index] != '\n') {
        ++index;
      }
      continue;
    }
    if (current == '/' && index + 1U < output->sql_length &&
        output->sql[index + 1U] == '*') {
      index += 2U;
      while (index + 1U < output->sql_length &&
             !(output->sql[index] == '*' && output->sql[index + 1U] == '/')) {
        ++index;
      }
      index = index + 1U < output->sql_length ? index + 2U : output->sql_length;
      continue;
    }
    if ((current >= 'A' && current <= 'Z') ||
        (current >= 'a' && current <= 'z') || current == '_') {
      const uint32_t start = index;

      ++index;
      while (index < output->sql_length &&
             (((output->sql[index] >= 'A' && output->sql[index] <= 'Z') ||
               (output->sql[index] >= 'a' && output->sql[index] <= 'z') ||
               (output->sql[index] >= '0' && output->sql[index] <= '9') ||
               output->sql[index] == '_'))) {
        ++index;
      }
      if (qstGeneratorIsUnsafeBareHintToken(output->sql + start,
                                            index - start) != 0) {
        output->sql[0] = '?';
        output->sql[1] = '\0';
        output->sql_length = 1;
        for (uint32_t occurrence = 0;
             occurrence < output->occurrence_count; ++occurrence) {
          output->occurrences[occurrence].text_offset = 0;
          output->occurrences[occurrence].text_length = 0;
        }
        output->contains_abnormal = 1;
        return QST_OK;
      }
      continue;
    }
    ++index;
  }
  return QST_OK;
}

static EQstCode qstGeneratorCheckPublicationBlockBudget(
    const SQstShmMapping *mapping, const SQstGeneratedSql *sql) {
  const SQstRegion *region = NULL;
  uint64_t payload_capacity = 0;
  uint64_t generation_record_bytes = 0;
  uint64_t required_blocks = 0;
  uint64_t reserve_blocks = 0;
  uint32_t free_blocks = 0;
  EQstCode free_count_code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == sql ||
      sql->sql_length == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->block_region;
  if (region->element_bytes <= sizeof(SQstBlockHeader)) {
    return QST_E_SHM_CORRUPT;
  }
  payload_capacity = region->element_bytes - sizeof(SQstBlockHeader);
  generation_record_bytes = sizeof(SQstGenerationRecordHeader) + sql->sql_length;
  required_blocks = (sql->sql_length + payload_capacity - 1U) / payload_capacity;
  required_blocks +=
      (generation_record_bytes + payload_capacity - 1U) / payload_capacity;
  reserve_blocks = (uint64_t)QST_GENERATOR_DOWNSTREAM_BLOCKS_PER_WORKER *
                   ((uint64_t)mapping->header->execution_pair_count +
                    mapping->header->judge_worker_count);
  free_count_code = qst_block_free_count(mapping, &free_blocks);
  if (required_blocks > UINT32_MAX || reserve_blocks > UINT32_MAX ||
      free_count_code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator publication block budget "
                  "validation failed code=%d free_blocks=%u "
                  "required_blocks=%llu reserve_blocks=%llu "
                  "block_bytes=%llu block_count=%llu\n",
                  free_count_code, free_blocks,
                  (unsigned long long)required_blocks,
                  (unsigned long long)reserve_blocks,
                  (unsigned long long)region->element_bytes,
                  (unsigned long long)region->element_count);
    return QST_E_SHM_CORRUPT;
  }
  if ((uint64_t)free_blocks < required_blocks + reserve_blocks) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator publication block budget "
                  "exhausted free_blocks=%u required_blocks=%llu "
                  "reserve_blocks=%llu\n",
                  free_blocks, (unsigned long long)required_blocks,
                  (unsigned long long)reserve_blocks);
    return QST_E_QUEUE_FULL;
  }
  return QST_OK;
}

static EQstCode qstGeneratorWriteGenerationRecord(
    SQstShmMapping *mapping, uint64_t gid, uint32_t generation,
    const SQstGeneratorPublishInput *input, const SQstGeneratedSql *sql,
    SQstRef *out_ref, SQstDigest256 *out_digest) {
  SQstGenerationRecordHeader header = {0};
  static _Thread_local uint8_t bytes[QST_GENERATOR_SQL_BYTES +
                                     sizeof(SQstGenerationRecordHeader)];
  uint64_t total_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == input || NULL == sql || NULL == out_ref ||
      NULL == out_digest || gid == 0 || generation == 0 ||
      input->catalog_version == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (sql->sql_length > UINT64_MAX - sizeof(header)) {
    return QST_E_RESOURCE;
  }
  total_bytes = sizeof(header) + sql->sql_length;
  if (total_bytes > sizeof(bytes)) {
    return QST_E_RESOURCE;
  }
  header.format_version = QST_GENERATION_RECORD_VERSION;
  header.header_bytes = sizeof(header);
  header.target = input->target;
  header.dialect = input->dialect;
  header.generation_seq = input->generation_seq;
  header.weight_profile_version = input->weight_profile_version;
  header.sql_length = sql->sql_length;
  header.contains_abnormal = sql->contains_abnormal;
  header.contains_unsupported_fallback = sql->contains_unsupported_fallback;
  header.catalog_digest = input->catalog_digest;
  header.dataset_digest = input->dataset_digest;
  header.sql_sha256 = sql->sql_sha256;
  memcpy(bytes, &header, sizeof(header));
  memcpy(bytes + sizeof(header), sql->sql, sql->sql_length);
  {
    SQstSha256 digest = {0};

    qst_sha256_init(&digest);
    qst_sha256_update(&digest, bytes, total_bytes);
    qst_sha256_final(&digest, out_digest->bytes);
  }
  code = qst_block_alloc(mapping, gid, generation, total_bytes, out_ref);
  if (code == QST_OK) {
    code = qst_block_write_seal(mapping, out_ref, bytes, total_bytes);
    if (code != QST_OK) {
      (void)fprintf(stderr,
                    "taosQueryTest: generation record seal failed code=%d "
                    "gid=%llu offset=%llu length=%u bytes=%llu\n",
                    code, (unsigned long long)gid,
                    (unsigned long long)out_ref->offset, out_ref->length,
                    (unsigned long long)total_bytes);
      atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                            memory_order_release);
      code = QST_E_SHM_CORRUPT;
    }
  } else {
    (void)fprintf(stderr,
                  "taosQueryTest: generation record block allocation failed "
                  "code=%d gid=%llu bytes=%llu\n",
                  code, (unsigned long long)gid,
                  (unsigned long long)total_bytes);
  }
  return code;
}

static EQstCode qstGeneratorSelectRandomTarget(
    const SQstGenerationState *state, SQstGeneratorRandomState *random_state,
    uint32_t *out_target_index) {
  static const uint64_t kTargetWeights[QST_GENERATOR_TARGET_COUNT] = {
      QST_GENERATOR_TARGET_WEIGHT_TD, QST_GENERATOR_TARGET_WEIGHT_MYSQL,
      QST_GENERATOR_TARGET_WEIGHT_POSTGRESQL,
      QST_GENERATOR_TARGET_WEIGHT_INFLUX};
  uint64_t total_weight = 0;
  uint64_t random_value = 0;
  uint64_t range_end = 0;
  uint32_t selected_count = 0;

  if (NULL == state || NULL == random_state || NULL == out_target_index ||
      state->target_selection_bits == 0 ||
      (state->target_selection_bits & ~UINT32_C(0x0f)) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
    if ((state->target_selection_bits & (UINT32_C(1) << index)) == 0) {
      continue;
    }
    if (UINT64_MAX - total_weight < kTargetWeights[index]) {
      return QST_E_RESOURCE;
    }
    total_weight += kTargetWeights[index];
    ++selected_count;
  }
  if (selected_count == 0 || total_weight == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (selected_count == 1) {
    for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
      if ((state->target_selection_bits & (UINT32_C(1) << index)) != 0) {
        *out_target_index = index;
        return QST_OK;
      }
    }
  }
  if (qst_generator_random_next(random_state, &random_value) != QST_OK) {
    return QST_E_INTERNAL;
  }
  random_value %= total_weight;
  for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
    if ((state->target_selection_bits & (UINT32_C(1) << index)) == 0) {
      continue;
    }
    range_end += kTargetWeights[index];
    if (random_value < range_end) {
      *out_target_index = index;
      return QST_OK;
    }
  }
  return QST_E_INTERNAL;
}

/* Validates that a zero-depth call cannot recurse forever through COMPOSE. */
static EQstCode qstGeneratorValidateTerminalFragment(
    const SQstGeneratorCatalog *catalog, uint32_t fragment_index,
    uint8_t *visit_state) {
  const SQstGeneratorFragment *fragment = NULL;
  int32_t has_terminal_candidate = 0;

  if (NULL == catalog || NULL == visit_state ||
      fragment_index >= catalog->fragment_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (visit_state[fragment_index] == 1) {
    (void)fprintf(stderr,
                  "taosQueryTest: catalog terminal cycle fragment_id=%u\n",
                  catalog->fragments[fragment_index].fragment_id);
    return QST_E_INVALID_ARGUMENT;
  }
  if (visit_state[fragment_index] == 2) {
    return QST_OK;
  }
  visit_state[fragment_index] = 1;
  fragment = &catalog->fragments[fragment_index];
  for (uint32_t value_index = 0; value_index < fragment->value_count;
       ++value_index) {
    const SQstGeneratorValue *value = &fragment->values[value_index];
    int32_t has_recursive_child = 0;

    for (uint32_t child = 0; child < value->child_count + value->step_count;
         ++child) {
      const SQstGeneratorChildCall *child_call = NULL;

      if (child < value->child_count) {
        child_call = &value->children[child];
      } else if (value->steps[child - value->child_count].kind ==
                 QST_GENERATOR_STEP_CALL) {
        child_call = &value->steps[child - value->child_count].child_call;
      }
      if (child_call != NULL && child_call->recursion_cost != 0 &&
          child_call->call_kind != QST_GENERATOR_CALL_LOOP) {
        has_recursive_child = 1;
        break;
      }
    }
    if (value->kind == QST_GENERATOR_VALUE_CALL_SELF ||
        has_recursive_child != 0 ||
        !qstGeneratorIsValueEligible(value, UINT32_MAX)) {
      continue;
    }
    has_terminal_candidate = 1;
    for (uint32_t child = 0; child < value->child_count + value->step_count;
         ++child) {
      uint32_t child_index = 0;
      EQstCode code = QST_OK;
      const SQstGeneratorChildCall *child_call = NULL;

      if (child < value->child_count) {
        child_call = &value->children[child];
      } else if (value->steps[child - value->child_count].kind ==
                 QST_GENERATOR_STEP_CALL) {
        child_call = &value->steps[child - value->child_count].child_call;
      } else {
        continue;
      }
      if (child_call->call_kind == QST_GENERATOR_CALL_LOOP) {
        continue;
      }

      if (!qstGeneratorIsChildCallValid(child_call) ||
          child_call->recursion_cost != 0 ||
          !qstGeneratorFindFragmentIndex(catalog, child_call->fragment_id,
                                         &child_index)) {
        return QST_E_INVALID_ARGUMENT;
      }
      code = qstGeneratorValidateTerminalFragment(catalog, child_index,
                                                  visit_state);
      if (code != QST_OK) {
        fprintf(stderr,
                "taosQueryTest: catalog terminal child validation parent=%u child=%u code=%d\n",
                fragment->fragment_id, child_call->fragment_id, code);
        return code;
      }
    }
  }
  if (has_terminal_candidate == 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: catalog has no terminal candidate "
                  "fragment_id=%u\n",
                  fragment->fragment_id);
    return QST_E_INVALID_ARGUMENT;
  }
  visit_state[fragment_index] = 2;
  return QST_OK;
}

static EQstCode qstGeneratorValueRuntimeCandidateCount(
    SQstGeneratorBuild *build, const SQstGeneratorValue *value,
    uint32_t *out_count) {
  if (NULL == build || NULL == value || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (value->runtime_object_kind == QST_RUNTIME_OBJECT_NONE) {
    *out_count = 1;
    return QST_OK;
  }
  return qstGeneratorRuntimeCandidateCount(build, value->runtime_object_kind,
                                           out_count);
}

static EQstCode qstGeneratorSelectRandom(SQstGeneratorBuild *build,
                                         const SQstGeneratorValue *values,
                                         uint32_t value_count,
                                         uint32_t depth,
                                         SQstGeneratorRandomState *state,
                                         uint32_t *out_index,
                                         uint32_t *out_eligible_count) {
  uint64_t random_value = 0;
  uint64_t selected_weight = 0;
  uint64_t total_weight = 0;
  uint32_t eligible_count = 0;

  if (NULL == build || NULL == values || value_count == 0 || NULL == state ||
      NULL == out_index || NULL == out_eligible_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < value_count; ++index) {
    if (!qstGeneratorValueCanBuild(build, &values[index], depth) ||
        values[index].weight == 0) {
      continue;
    }
    if (UINT64_MAX - total_weight < values[index].weight) {
      return QST_E_RESOURCE;
    }
    total_weight += values[index].weight;
    ++eligible_count;
  }
  if (eligible_count == 0 || total_weight == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qst_generator_random_next(state, &random_value) != QST_OK) {
    return QST_E_INTERNAL;
  }
  random_value %= total_weight;
  for (uint32_t index = 0; index < value_count; ++index) {
    if (!qstGeneratorValueCanBuild(build, &values[index], depth) ||
        values[index].weight == 0) {
      continue;
    }
    if (UINT64_MAX - selected_weight < values[index].weight) {
      return QST_E_RESOURCE;
    }
    selected_weight += values[index].weight;
    if (random_value < selected_weight) {
      *out_index = index;
      *out_eligible_count = eligible_count;
      return QST_OK;
    }
  }
  return QST_E_INTERNAL;
}

static EQstCode qstGeneratorSelectLoopIterations(
    SQstGeneratorBuild *build, const SQstGeneratorChildCall *child_call,
    uint32_t *out_iterations) {
  uint32_t cardinality = 0;
  uint32_t selected = 0;
  uint64_t random_value = 0;

  if (NULL == build || NULL == child_call || NULL == out_iterations ||
      child_call->call_kind != QST_GENERATOR_CALL_LOOP ||
      child_call->max_iterations < child_call->min_iterations) {
    return QST_E_INVALID_ARGUMENT;
  }
  cardinality = child_call->max_iterations - child_call->min_iterations + 1U;
  if (cardinality == 0 || build->choice_index == UINT32_MAX) {
    return QST_E_RESOURCE;
  }
  if (build->disable_path_metadata == 0 &&
      qstGeneratorReserveChoice(build->output, build->choice_index) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (build->is_random != 0) {
    if (qst_generator_random_next(build->random_state, &random_value) != QST_OK) {
      return QST_E_INTERNAL;
    }
    selected = (uint32_t)(random_value % cardinality);
  } else {
    if (build->choice_index < build->full_choice_count) {
      selected = build->full_choices[build->choice_index];
    }
    if (selected >= cardinality) {
      return QST_E_INVALID_ARGUMENT;
    }
  }
  if (build->disable_path_metadata == 0) {
    build->output->choice_cardinalities[build->choice_index] = cardinality;
    build->output->selected_value_ids[build->choice_index] = 0;
  }
  ++build->choice_index;
  *out_iterations = child_call->min_iterations + selected;
  return QST_OK;
}

static EQstCode qstGeneratorBuildChildCall(
    SQstGeneratorBuild *build, const SQstGeneratorChildCall *child_call,
    uint32_t depth, uint32_t parent_occurrence_id, uint32_t child_index) {
  uint32_t child_depth = depth;
  uint32_t iterations = 1;
  uint32_t scope_pushed = 0;
  uint32_t child_start_offset = 0;
  EQstCode code = QST_OK;

  if (NULL == build || NULL == child_call ||
      !qstGeneratorIsChildCallValid(child_call)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (child_call->recursion_cost != 0 && depth == 0) {
    if (build->choice_index == UINT32_MAX ||
        qstGeneratorReserveChoice(build->output, build->choice_index) != QST_OK) {
      return QST_E_RESOURCE;
    }
    build->output->choice_cardinalities[build->choice_index] = 1;
    build->output->selected_value_ids[build->choice_index] = 0;
    ++build->choice_index;
    return QST_OK;
  }
  if (child_call->recursion_cost != 0) {
    child_depth = depth - 1U;
  }
  if (child_call->call_kind == QST_GENERATOR_CALL_LOOP) {
    code = qstGeneratorSelectLoopIterations(build, child_call, &iterations);
    if (code != QST_OK) {
      return code;
    }
  }
  if (child_call->scope_action == QST_GENERATOR_SCOPE_QUERY_BLOCK) {
    code = qstGeneratorScopePush(build);
    if (code != QST_OK) {
      return code;
    }
    scope_pushed = 1;
  }
  child_start_offset = build->output->sql_length;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    uint32_t child_return_state = QST_GENERATOR_RETURN_NON_EMPTY;

    code = qstGeneratorBuildFragment(build, child_call->fragment_id,
                                     child_depth, parent_occurrence_id,
                                     child_index, iteration,
                                     &child_return_state);
    if (code != QST_OK) {
      if (scope_pushed != 0) {
        qstGeneratorScopePop(build);
      }
      return code;
    }
    if (child_call->call_kind == QST_GENERATOR_CALL_LOOP &&
        (child_return_state == QST_GENERATOR_RETURN_EMPTY_RESULT ||
         child_return_state == QST_GENERATOR_RETURN_STOP_CURRENT)) {
      break;
    }
  }
  if (child_call->scope_action == QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS &&
      build->selection_only == 0) {
    code = qstGeneratorScopeBindAlias(build,
                                      build->output->sql + child_start_offset,
                                      build->output->sql_length - child_start_offset);
    if (code != QST_OK) {
      if (scope_pushed != 0) {
        qstGeneratorScopePop(build);
      }
      return code;
    }
  }
  if (scope_pushed != 0) {
    qstGeneratorScopePop(build);
  }
  return QST_OK;
}

static EQstCode qstGeneratorAppendInlineStep(
    SQstGeneratorBuild *build, uint32_t fragment_id, uint32_t parent_occurrence_id,
    uint32_t child_index, const SQstGeneratorStep *step) {
  SQstGenerationOccurrence *occurrence = NULL;
  uint32_t start_offset = 0;

  if (NULL == build || NULL == build->output || NULL == step ||
      !qstGeneratorIsStepValid(step) ||
      step->kind != QST_GENERATOR_STEP_INLINE_VALUE) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (build->disable_occurrence_metadata != 0) {
    return qstGeneratorAppend(build->output, step->text);
  }
  if (build->output->occurrence_count == UINT32_MAX ||
      qstGeneratorReserveOccurrence(build->output,
                                    build->output->occurrence_count) != QST_OK) {
    return QST_E_RESOURCE;
  }
  occurrence = &build->output->occurrences[build->output->occurrence_count];
  memset(occurrence, 0, sizeof(*occurrence));
  occurrence->occurrence_id = build->output->occurrence_count + 1U;
  occurrence->parent_occurrence_id = parent_occurrence_id;
  occurrence->fragment_id = fragment_id;
  occurrence->value_id = step->value_id;
  occurrence->child_index = child_index;
  occurrence->return_state = QST_GENERATOR_RETURN_NON_EMPTY;
  start_offset = build->output->sql_length;
  ++build->output->occurrence_count;
  {
    EQstCode code = qstGeneratorAppend(build->output, step->text);

    if (code != QST_OK) {
      return code;
    }
  }
  occurrence->text_offset = start_offset;
  occurrence->text_length = build->output->sql_length - start_offset;
  return QST_OK;
}

static EQstCode qstGeneratorBuildFragment(SQstGeneratorBuild *build,
                                          uint32_t fragment_id,
                                          uint32_t depth,
                                          uint32_t parent_occurrence_id,
                                          uint32_t child_index,
                                          uint32_t loop_iteration,
                                          uint32_t *out_return_state) {
  const SQstGeneratorFragment *fragment = NULL;
  const SQstGeneratorValue *value = NULL;
  SQstGenerationOccurrence *occurrence = NULL;
  uint32_t occurrence_index = 0;
  uint32_t occurrence_id = 0;
  uint32_t fragment_index = 0;
  uint32_t start_offset = 0;
  uint32_t selected_index = 0;
  uint32_t eligible_count = 0;
  uint32_t runtime_ordinal = 0;
  EQstCode code = QST_OK;

  if (NULL == build || NULL == build->catalog || NULL == build->output ||
      NULL == out_return_state) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (build->choice_index == UINT32_MAX ||
      qstGeneratorReserveChoice(build->output, build->choice_index) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (!qstGeneratorFindBuildableFragment(build, fragment_id,
                                         &fragment_index)) {
    return QST_E_INVALID_ARGUMENT;
  }
  fragment = &build->catalog->fragments[fragment_index];
  if (NULL == fragment || fragment->value_count == 0 || NULL == fragment->values) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (build->is_random != 0) {
    code = qstGeneratorSelectRandom(build, fragment->values,
                                    fragment->value_count,
                                    depth, build->random_state, &selected_index,
                                    &eligible_count);
    if (code != QST_OK) {
      return code;
    }
  } else {
    uint32_t desired = 0;

    for (uint32_t index = 0; index < fragment->value_count; ++index) {
      if (qstGeneratorValueCanBuild(build, &fragment->values[index], depth)) {
        uint32_t variants = 1;

        code = qstGeneratorValueRuntimeCandidateCount(
            build, &fragment->values[index], &variants);
        if (code != QST_OK) return code;
        if (variants == 0) {
          continue;
        }
        if (eligible_count > UINT32_MAX - variants) {
          return QST_E_RESOURCE;
        }
        eligible_count += variants;
      }
    }
    if (eligible_count == 0) {
      return QST_E_INVALID_ARGUMENT;
    }
    if (build->choice_index < build->full_choice_count) {
      desired = build->full_choices[build->choice_index];
    }
    if (desired >= eligible_count) {
      return QST_E_INVALID_ARGUMENT;
    }
    for (uint32_t index = 0; index < fragment->value_count; ++index) {
      uint32_t variants = 1;

      if (!qstGeneratorValueCanBuild(build, &fragment->values[index], depth)) {
        continue;
      }
      code = qstGeneratorValueRuntimeCandidateCount(
          build, &fragment->values[index], &variants);
      if (code != QST_OK) return code;
      if (variants == 0) {
        continue;
      }
      if (desired < variants) {
        selected_index = index;
        runtime_ordinal = desired;
        break;
      }
      desired -= variants;
    }
  }
  value = &fragment->values[selected_index];
  if (build->is_random != 0 &&
      value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
    uint32_t runtime_count = 0;

    code = qstGeneratorRuntimeCandidateCount(build, value->runtime_object_kind,
                                             &runtime_count);
    if (code != QST_OK) {
      runtime_count = 0;
      code = QST_OK;
    }
    /* Binding derives from the already-consumed selection state, not a choice. */
    if (runtime_count != 0) {
      runtime_ordinal = (uint32_t)(build->random_state->state % runtime_count);
    } else {
      runtime_ordinal = UINT32_MAX;
    }
  }
  build->output->choice_cardinalities[build->choice_index] = eligible_count;
  build->output->selected_value_ids[build->choice_index] = value->value_id;
  build->output->contains_abnormal |=
      (uint8_t)(value->generation_class == QST_GENERATOR_CLASS_ABNORMAL);
  ++build->choice_index;
  if (build->disable_occurrence_metadata == 0) {
    if (build->output->occurrence_count == UINT32_MAX ||
        qstGeneratorReserveOccurrence(build->output,
                                      build->output->occurrence_count) != QST_OK) {
      return QST_E_RESOURCE;
    }
    occurrence_id = build->output->occurrence_count + 1U;
    occurrence_index = build->output->occurrence_count;
    occurrence = &build->output->occurrences[occurrence_index];
    memset(occurrence, 0, sizeof(*occurrence));
    occurrence->occurrence_id = occurrence_id;
    occurrence->parent_occurrence_id = parent_occurrence_id;
    occurrence->fragment_id = fragment_id;
    occurrence->value_id = value->value_id;
    occurrence->child_index = child_index;
    occurrence->loop_iteration = loop_iteration;
    occurrence->depth = depth;
    occurrence->runtime_object_kind = value->runtime_object_kind;
    ++build->output->occurrence_count;
  }
  start_offset = build->output->sql_length;
  if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE &&
      (build->runtime != NULL || build->selection_only != 0)) {
    if (NULL == build->runtime) {
      return QST_E_INVALID_ARGUMENT;
    }
    if (runtime_ordinal != UINT32_MAX) {
      code = qstGeneratorRuntimeAppendObject(build, value->runtime_object_kind,
                                             runtime_ordinal, occurrence);
      if (code != QST_OK && build->selection_only == 0) {
        return code;
      }
    }
  } else if (NULL != value->text) {
    code = qstGeneratorAppend(build->output, value->text);
    if (code != QST_OK) {
      return code;
    }
  }
  if (value->kind == QST_GENERATOR_VALUE_CALL_SELF) {
    code = qstGeneratorBuildFragment(build, fragment_id, depth - 1U,
                                     occurrence_id, 0, 0, out_return_state);
    if (code != QST_OK) {
      return code;
    }
    if (build->disable_occurrence_metadata == 0) {
      occurrence = &build->output->occurrences[occurrence_index];
      occurrence->return_state = *out_return_state;
      occurrence->text_offset = start_offset;
      occurrence->text_length = build->output->sql_length - start_offset;
    }
    return QST_OK;
  }
  for (uint32_t child = 0; child < value->child_count; ++child) {
    code = qstGeneratorBuildChildCall(build, &value->children[child], depth,
                                      occurrence_id, child);
    if (code != QST_OK) {
      return code;
    }
  }
  for (uint32_t step_index = 0; step_index < value->step_count; ++step_index) {
    const SQstGeneratorStep *step = &value->steps[step_index];

    if (step->kind == QST_GENERATOR_STEP_CALL) {
      code = qstGeneratorBuildChildCall(build, &step->child_call, depth,
                                        occurrence_id, step_index);
    } else {
      code = qstGeneratorAppendInlineStep(build, fragment_id, occurrence_id,
                                          step_index, step);
    }
    if (code != QST_OK) {
      return code;
    }
  }
  *out_return_state = value->return_state;
  if (build->disable_occurrence_metadata == 0) {
    /* Child construction may grow occurrences and relocate its backing array. */
    occurrence = &build->output->occurrences[occurrence_index];
    occurrence->return_state = value->return_state;
    occurrence->text_offset = start_offset;
    occurrence->text_length = build->output->sql_length - start_offset;
  }
  return QST_OK;
}

EQstCode qst_generator_catalog_digest(const SQstGeneratorCatalog *catalog,
                                      SQstDigest256 *out_digest) {
  SQstSha256 digest = {0};

  if (NULL == catalog || NULL == out_digest || catalog->version == 0 ||
      catalog->root_fragment_id == 0 || catalog->fragment_count == 0 ||
      NULL == catalog->fragments) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, (const uint8_t *)&catalog->version,
                    sizeof(catalog->version));
  qst_sha256_update(&digest, (const uint8_t *)&catalog->root_fragment_id,
                    sizeof(catalog->root_fragment_id));
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];

    qst_sha256_update(&digest, (const uint8_t *)&fragment->fragment_id,
                      sizeof(fragment->fragment_id));
    qst_sha256_update(&digest, (const uint8_t *)&fragment->weight,
                      sizeof(fragment->weight));
    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      const SQstGeneratorValue *value = &fragment->values[value_index];

      qst_sha256_update(&digest, (const uint8_t *)&value->value_id,
                        sizeof(value->value_id));
      qst_sha256_update(&digest, (const uint8_t *)&value->kind,
                        sizeof(value->kind));
      qst_sha256_update(&digest, (const uint8_t *)&value->generation_class,
                        sizeof(value->generation_class));
      qst_sha256_update(&digest, (const uint8_t *)&value->return_state,
                        sizeof(value->return_state));
      qst_sha256_update(&digest, (const uint8_t *)&value->weight,
                        sizeof(value->weight));
      qst_sha256_update(&digest, (const uint8_t *)&value->child_count,
                        sizeof(value->child_count));
      qst_sha256_update(&digest, (const uint8_t *)&value->runtime_object_kind,
                        sizeof(value->runtime_object_kind));
      qst_sha256_update(&digest, (const uint8_t *)&value->dialect_mask,
                        sizeof(value->dialect_mask));
      if (value->child_count != 0 && NULL != value->children) {
        qst_sha256_update(&digest, (const uint8_t *)value->children,
                          (uint64_t)value->child_count *
                              sizeof(SQstGeneratorChildCall));
      }
      qst_sha256_update(&digest, (const uint8_t *)&value->step_count,
                        sizeof(value->step_count));
      for (uint32_t step = 0; step < value->step_count; ++step) {
        const SQstGeneratorStep *item = &value->steps[step];
        const uint64_t text_length = item->text != NULL ? strlen(item->text) : 0;

        qst_sha256_update(&digest, (const uint8_t *)&item->kind,
                          sizeof(item->kind));
        qst_sha256_update(&digest, (const uint8_t *)&item->value_id,
                          sizeof(item->value_id));
        qst_sha256_update(&digest, (const uint8_t *)&item->child_call,
                          sizeof(item->child_call));
        qst_sha256_update(&digest, (const uint8_t *)&text_length,
                          sizeof(text_length));
        if (text_length != 0) {
          qst_sha256_update(&digest, (const uint8_t *)item->text, text_length);
        }
      }
      {
        const uint64_t text_length =
            NULL != value->text ? strlen(value->text) : 0;

        qst_sha256_update(&digest, (const uint8_t *)&text_length,
                          sizeof(text_length));
      }
      if (NULL != value->text) {
        qst_sha256_update(&digest, (const uint8_t *)value->text,
                          strlen(value->text));
      }
    }
  }
  qst_sha256_final(&digest, out_digest->bytes);
  return QST_OK;
}

EQstCode qst_generator_catalog_validate(const SQstGeneratorCatalog *catalog) {
  SQstDigest256 calculated = {0};
  uint8_t *visit_state = NULL;
  EQstCode code = QST_OK;
  if (NULL == catalog || catalog->version == 0 || catalog->fragment_count == 0 ||
      NULL == catalog->fragments ||
      NULL == qstGeneratorFindFragment(catalog, catalog->root_fragment_id)) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];
    if (fragment->fragment_id == 0 || fragment->value_count == 0 ||
        NULL == fragment->values) {
      return QST_E_INVALID_ARGUMENT;
    }
    for (uint32_t previous = 0; previous < fragment_index; ++previous) {
      if (catalog->fragments[previous].fragment_id == fragment->fragment_id) {
        return QST_E_INVALID_ARGUMENT;
      }
    }
    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      const SQstGeneratorValue *value = &fragment->values[value_index];

      if (!qstGeneratorIsValueEligible(value, UINT32_MAX) || value->value_id == 0) {
        return QST_E_INVALID_ARGUMENT;
      }
      for (uint32_t child = 0; child < value->child_count + value->step_count;
           ++child) {
        const SQstGeneratorChildCall *child_call = NULL;

        if (child < value->child_count) {
          child_call = &value->children[child];
        } else if (value->steps[child - value->child_count].kind ==
                   QST_GENERATOR_STEP_CALL) {
          child_call = &value->steps[child - value->child_count].child_call;
        } else {
          continue;
        }
        if (!qstGeneratorIsChildCallValid(child_call) ||
            NULL == qstGeneratorFindFragment(catalog, child_call->fragment_id)) {
          return QST_E_INVALID_ARGUMENT;
        }
      }
      for (uint32_t previous = 0; previous < value_index; ++previous) {
        if (fragment->values[previous].value_id == value->value_id) {
          return QST_E_INVALID_ARGUMENT;
        }
      }
      for (uint32_t previous_fragment = 0; previous_fragment < fragment_index;
           ++previous_fragment) {
        const SQstGeneratorFragment *prior =
            &catalog->fragments[previous_fragment];

        for (uint32_t previous_value = 0; previous_value < prior->value_count;
             ++previous_value) {
          if (prior->values[previous_value].value_id == value->value_id) {
            return QST_E_INVALID_ARGUMENT;
          }
        }
      }
    }
  }
  if (catalog->fragment_count > UINT32_MAX / sizeof(*visit_state)) {
    return QST_E_RESOURCE;
  }
  visit_state = (uint8_t *)calloc(catalog->fragment_count, sizeof(*visit_state));
  if (NULL == visit_state) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < catalog->fragment_count; ++index) {
    code = qstGeneratorValidateTerminalFragment(catalog, index, visit_state);
    if (code != QST_OK) {
      fprintf(stderr,
              "taosQueryTest: catalog terminal validation fragment_id=%u code=%d\n",
              catalog->fragments[index].fragment_id, code);
      free(visit_state);
      return code;
    }
  }
  free(visit_state);
  if (qst_generator_catalog_digest(catalog, &calculated) != QST_OK ||
      memcmp(calculated.bytes, catalog->digest.bytes, sizeof(calculated.bytes)) != 0) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

EQstCode qst_generator_random_init(SQstGeneratorRandomState *state,
                                   uint64_t random_seed,
                                   uint64_t generation_seq) {
  if (NULL == state) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(state, 0, sizeof(*state));
  state->state = random_seed ^ generation_seq;
  for (uint32_t index = 0; index < QST_GENERATOR_RANDOM_ARRAY_SIZE; ++index) {
    state->random_array[index] = qstGeneratorSplitMix64Next(&state->state);
  }
  state->lazy_seed = 0;
  state->lazy_array_state = 0;
  state->lazy_initialized = QST_GENERATOR_RANDOM_ARRAY_SIZE;
  return QST_OK;
}

EQstCode qst_generator_random_init_lazy(SQstGeneratorRandomState *state,
                                        uint64_t random_seed,
                                        uint64_t generation_seq) {
  uint64_t base = 0;

  if (state == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(state, 0, sizeof(*state));
  base = random_seed ^ generation_seq;
  state->state = base +
                 (uint64_t)QST_GENERATOR_RANDOM_ARRAY_SIZE *
                     UINT64_C(0x9E3779B97F4A7C15);
  state->lazy_seed = base;
  state->lazy_array_state = base;
  state->lazy_initialized = 0;
  return QST_OK;
}

EQstCode qst_generator_random_next(SQstGeneratorRandomState *state,
                                   uint64_t *out_value) {
  if (NULL == state || NULL == out_value ||
      state->random_index >= QST_GENERATOR_RANDOM_ARRAY_SIZE) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (state->lazy_initialized != QST_GENERATOR_RANDOM_ARRAY_SIZE &&
      qstGeneratorRandomEnsureLazyValue(state) != QST_OK) {
    return QST_E_RESOURCE;
  }
  state->state ^= state->random_array[state->random_index] ^ state->loop_round;
  *out_value = qstGeneratorSplitMix64Next(&state->state);
  ++state->random_index;
  if (state->random_index == QST_GENERATOR_RANDOM_ARRAY_SIZE) {
    state->random_index = 0;
    if (state->loop_round == UINT64_MAX) {
      return QST_E_RESOURCE;
    }
    ++state->loop_round;
  }
  return QST_OK;
}

/* The compiled RANDOM plan has already validated the stream state. */
static inline EQstCode qstGeneratorRandomNextPrepared(
    SQstGeneratorRandomState *state, uint64_t *out_value) {
  if (NULL == state || NULL == out_value ||
      state->random_index >= QST_GENERATOR_RANDOM_ARRAY_SIZE) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (state->lazy_initialized != QST_GENERATOR_RANDOM_ARRAY_SIZE &&
      qstGeneratorRandomEnsureLazyValue(state) != QST_OK) {
    return QST_E_RESOURCE;
  }
  if (state->random_index + 1U == QST_GENERATOR_RANDOM_ARRAY_SIZE &&
      state->loop_round == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  state->state ^= state->random_array[state->random_index] ^ state->loop_round;
  *out_value = qstGeneratorSplitMix64Next(&state->state);
  ++state->random_index;
  if (state->random_index == QST_GENERATOR_RANDOM_ARRAY_SIZE) {
    state->random_index = 0;
    ++state->loop_round;
  }
  return QST_OK;
}

enum {
  /* Rebuild only paths which the former implementation replaced with "?". */
  QST_GENERATOR_RANDOM_REBUILD_ATTEMPT_LIMIT = 64U,
};

static int32_t qstGeneratorRandomBuildNeedsRetry(EQstCode code) {
  return code == QST_E_INVALID_ARGUMENT || code == QST_E_SQL_LIMIT;
}

static EQstCode qstGeneratorRandomBuildValidatedWithRuntime(
    const SQstGeneratorCatalog *catalog, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *cached_runtime,
    SQstGeneratedSql *out_sql) {
  SQstGeneratorBuild build = {0};
  EQstCode code = QST_OK;

  if (NULL == state || NULL == out_sql ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstGeneratorGeneratedSqlReset(out_sql);
  build.catalog = catalog;
  build.random_state = state;
  build.output = out_sql;
  build.is_random = 1;
  build.profile_runtime =
      cached_runtime != NULL && cached_runtime->profile_enabled != 0 ?
          cached_runtime : NULL;
  build.dialect_mask = runtime != NULL ? qstGeneratorDialectMask(runtime->dialect) : 0;
  if (NULL != cached_runtime) {
    if (NULL == cached_runtime->catalog_value_index ||
        NULL == cached_runtime->catalog_fragment_index ||
        NULL == cached_runtime->catalog_value_eligibility ||
        cached_runtime->catalog_value_count == 0 ||
        NULL == cached_runtime->buildable_fragments ||
        NULL == cached_runtime->buildable_fragment_epochs ||
        NULL == cached_runtime->buildable_values ||
        NULL == cached_runtime->buildable_value_epochs ||
        NULL == cached_runtime->buildable_lookup_cache ||
        cached_runtime->buildability_epoch == 0) {
      return QST_E_SHM_CORRUPT;
    }
    build.buildable_value_index =
        (const SQstGeneratorBuildableValue *)cached_runtime->catalog_value_index;
    build.buildable_value_count = cached_runtime->catalog_value_count;
    build.buildable_fragment_index =
        (const SQstGeneratorBuildableFragment *)cached_runtime->catalog_fragment_index;
    build.buildable_value_eligibility = cached_runtime->catalog_value_eligibility;
    build.buildable_fragments = cached_runtime->buildable_fragments;
    build.buildable_fragment_epochs = cached_runtime->buildable_fragment_epochs;
    build.buildable_values = cached_runtime->buildable_values;
    build.buildable_value_epochs = cached_runtime->buildable_value_epochs;
    build.lookup_cache =
        (SQstGeneratorBuildLookupCache *)cached_runtime->buildable_lookup_cache;
    build.buildability_epoch = cached_runtime->buildability_epoch;
    build.buildability_epoch_owner = &cached_runtime->buildability_epoch;
  }
  code = qstGeneratorRuntimeInitialize(&build, runtime);
  if (code != QST_OK) {
    return code;
  }
  if (NULL == cached_runtime) {
    code = qstGeneratorBuildabilityInitialize(&build);
    if (code != QST_OK) {
      qstGeneratorBuildDestroy(&build);
      return code;
    }
  }
  {
    uint32_t return_state = QST_GENERATOR_RETURN_NON_EMPTY;

    code = qstGeneratorBuildFragment(&build, catalog->root_fragment_id, depth,
                                     0, 0, 0, &return_state);
  }
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(&build);
    qst_generator_generated_sql_destroy(out_sql);
    return code;
  }
  code = qstGeneratorEnsureStatement(out_sql);
  if (code == QST_OK) {
    code = qstGeneratorNormalizeBareHintTokens(out_sql);
  }
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(&build);
    qst_generator_generated_sql_destroy(out_sql);
    return code;
  }
  out_sql->choice_count = build.choice_index;
  {
    SQstSha256 digest = {0};
    qst_sha256_init(&digest);
    qst_sha256_update(&digest, (const uint8_t *)out_sql->sql, out_sql->sql_length);
    qst_sha256_final(&digest, out_sql->sql_sha256.bytes);
  }
  qstGeneratorBuildDestroy(&build);
  return QST_OK;
}

enum {
  QST_GENERATOR_PLAN_STACK_CAPACITY = 512,
};

typedef enum {
  QST_GENERATOR_PLAN_FRAME_FRAGMENT = 1,
  QST_GENERATOR_PLAN_FRAME_VALUE = 2,
  QST_GENERATOR_PLAN_FRAME_CHILD = 3,
} EQstGeneratorPlanFrameKind;

typedef struct {
  uint32_t kind;
  uint32_t state;
  uint32_t parent_frame_index;
  uint32_t fragment_index;
  uint32_t depth;
  uint32_t parent_occurrence_id;
  uint32_t child_index;
  uint32_t loop_iteration;
  uint32_t value_index;
  uint32_t operation_index;
  uint32_t occurrence_index;
  uint32_t occurrence_id;
  uint32_t start_offset;
  uint32_t child_return_state;
  uint32_t return_state;
  uint32_t child_depth;
  uint32_t iterations;
  uint32_t iteration;
  uint32_t scope_pushed;
  const SQstGeneratorFragPlanOperation *operation;
} SQstGeneratorPlanFrame;

/* A RANDOM builder owns this scratch for its lifetime.  Every frame is written
 * before use, so retaining old frame bytes cannot influence a later path. */
typedef struct {
  SQstGeneratorBuild build;
  SQstGeneratorPlanFrame frames[QST_GENERATOR_PLAN_STACK_CAPACITY];
} SQstGeneratorPlanScratch;

static EQstCode qstGeneratorPlanScratchReset(
    SQstGeneratorRandomRuntime *runtime, SQstGeneratorBuild **out_build,
    SQstGeneratorPlanFrame **out_frames) {
  SQstGeneratorPlanScratch *scratch = NULL;
  SQstGeneratorBuild *build = NULL;

  if (NULL == runtime || NULL == out_build || NULL == out_frames) {
    return QST_E_INVALID_ARGUMENT;
  }
  scratch = (SQstGeneratorPlanScratch *)runtime->plan_scratch;
  if (NULL == scratch) {
    scratch = (SQstGeneratorPlanScratch *)calloc(1, sizeof(*scratch));
    if (NULL == scratch) {
      return QST_E_RESOURCE;
    }
    runtime->plan_scratch = scratch;
  }
  build = &scratch->build;
  /* qstGeneratorBuildDestroy has released every owned mutable member. The
   * remaining lookup cache is tied to this immutable runtime and is valid for
   * the next path. Only state that can alter control flow must be reset. */
  build->runtime = NULL;
  build->scope_depth = 0;
  build->choice_index = 0;
  build->contains_abnormal = 0;
  *out_build = build;
  *out_frames = scratch->frames;
  return QST_OK;
}

static EQstCode qstGeneratorPlanSelectRandom(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan,
    uint32_t fragment_index, uint32_t depth, uint32_t *out_value_index,
    uint32_t *out_eligible_count) {
  const SQstGeneratorFragPlanEntry *fragment = NULL;
  uint64_t random_value = 0;
  uint64_t total_weight = 0;
  uint64_t selected_weight = 0;
  uint32_t eligible_count = 0;
  uint64_t started_ns = 0;

  if (NULL != build) {
    started_ns = qstGeneratorProfileNowNs(build->profile_runtime);
  }
  if (NULL == build || NULL == plan || NULL == out_value_index ||
      NULL == out_eligible_count || fragment_index >= plan->fragment_count ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    if (NULL != build && NULL != build->profile_runtime) {
      qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                    &build->profile_runtime->profile_plan_select_ns);
    }
    return QST_E_INVALID_ARGUMENT;
  }
  fragment = &plan->fragments[fragment_index];
  if (fragment->value_count == 0 ||
      fragment->first_value_index > plan->value_count - fragment->value_count) {
    if (NULL != build->profile_runtime) {
      qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                    &build->profile_runtime->profile_plan_select_ns);
    }
    return QST_E_SHM_CORRUPT;
  }
  if (plan->static_selection_prefix_weights != NULL &&
      plan->static_selection_rows != NULL) {
    const uint32_t dialect_slot = qstGeneratorPlanDialectSlot(build->dialect_mask);

    if (dialect_slot != UINT32_MAX) {
      const uint64_t selection_slot =
          qstGeneratorPlanSelectionSlot(depth, dialect_slot);
      const SQstGeneratorFragPlanStaticSelection *selection =
          &plan->static_selection_rows[selection_slot][fragment_index];
      const uint64_t total_weight = selection->total_weight;
      const uint64_t reciprocal = selection->reciprocal;
      const uint32_t static_eligible_count = selection->eligible_count;
      uint32_t lower = 0;
      uint32_t upper = fragment->value_count;

      if (static_eligible_count == 0 || total_weight == 0 ||
          qst_generator_random_next(build->random_state, &random_value) != QST_OK) {
        if (NULL != build->profile_runtime) {
          qstGeneratorProfileAddElapsed(
              build->profile_runtime, started_ns,
              &build->profile_runtime->profile_plan_select_ns);
        }
        return QST_E_INVALID_ARGUMENT;
      }
      if (static_eligible_count == 1U) {
        if (selection->sole_value_index < fragment->first_value_index ||
            selection->sole_value_index >=
                fragment->first_value_index + fragment->value_count) {
          if (NULL != build->profile_runtime) {
            qstGeneratorProfileAddElapsed(
                build->profile_runtime, started_ns,
                &build->profile_runtime->profile_plan_select_ns);
          }
          return QST_E_SHM_CORRUPT;
        }
        *out_value_index = selection->sole_value_index;
        *out_eligible_count = static_eligible_count;
        if (build->profile_runtime != NULL &&
            build->profile_runtime->profile_plan_select_count != UINT64_MAX) {
          ++build->profile_runtime->profile_plan_select_count;
        }
        qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                      &build->profile_runtime->profile_plan_select_ns);
        return QST_OK;
      }
      random_value = qstGeneratorPlanFastModulo(random_value, total_weight,
                                                 reciprocal);
      while (lower < upper) {
        const uint32_t middle = lower + (upper - lower) / 2U;
        const uint64_t prefix = selection->prefix_weights[middle];

        if (random_value < prefix) {
          upper = middle;
        } else {
          lower = middle + 1U;
        }
      }
      if (lower >= fragment->value_count) {
        if (NULL != build->profile_runtime) {
          qstGeneratorProfileAddElapsed(
              build->profile_runtime, started_ns,
              &build->profile_runtime->profile_plan_select_ns);
        }
        return QST_E_INTERNAL;
      }
      *out_value_index = fragment->first_value_index + lower;
      *out_eligible_count = static_eligible_count;
      if (build->profile_runtime != NULL &&
          build->profile_runtime->profile_plan_select_count != UINT64_MAX) {
        ++build->profile_runtime->profile_plan_select_count;
      }
      qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                    &build->profile_runtime->profile_plan_select_ns);
      return QST_OK;
      }
  }
  SQstGeneratorPlanSelectionEntry *selection_scratch = NULL;

  if (build->is_random != 0 && build->runtime != NULL &&
      fragment->value_count > 0) {
    if (build->plan_selection_capacity < fragment->value_count) {
      uint32_t next_capacity = build->plan_selection_capacity == 0 ? 16U :
                                                                    build->plan_selection_capacity;
      while (next_capacity < fragment->value_count) {
        if (next_capacity > UINT32_MAX / 2U) {
          next_capacity = fragment->value_count;
          break;
        }
        next_capacity *= 2U;
      }
      if ((uint64_t)next_capacity >
          SIZE_MAX / sizeof(SQstGeneratorPlanSelectionEntry)) {
        return QST_E_RESOURCE;
      }
      void *next_scratch = realloc(
          build->plan_selection_scratch,
          (size_t)next_capacity * sizeof(SQstGeneratorPlanSelectionEntry));

      if (next_scratch == NULL) {
        return QST_E_RESOURCE;
      }
      build->plan_selection_scratch = next_scratch;
      build->plan_selection_capacity = next_capacity;
    }
    selection_scratch = (SQstGeneratorPlanSelectionEntry *)
        build->plan_selection_scratch;
  }
  for (uint32_t index = 0; index < fragment->value_count; ++index) {
    const SQstGeneratorFragPlanValue *value =
        &plan->values[fragment->first_value_index + index];

    if (NULL != build->profile_runtime &&
        build->profile_runtime->profile_plan_select_value_examined != UINT64_MAX) {
      ++build->profile_runtime->profile_plan_select_value_examined;
    }
    if (value->value->weight == 0) {
      continue;
    }
    if (UINT64_MAX - total_weight < value->value->weight) {
      if (NULL != build->profile_runtime) {
        qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                      &build->profile_runtime->profile_plan_select_ns);
      }
      return QST_E_RESOURCE;
    }
    total_weight += value->value->weight;
    if (selection_scratch != NULL) {
      selection_scratch[eligible_count].value_index =
          fragment->first_value_index + index;
      selection_scratch[eligible_count].prefix_weight = total_weight;
    }
    ++eligible_count;
  }
  if (eligible_count == 0 || total_weight == 0 ||
      qst_generator_random_next(build->random_state, &random_value) != QST_OK) {
    if (NULL != build->profile_runtime) {
      qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                    &build->profile_runtime->profile_plan_select_ns);
    }
    return QST_E_INVALID_ARGUMENT;
  }
  random_value %= total_weight;
  if (selection_scratch != NULL) {
    for (uint32_t index = 0; index < eligible_count; ++index) {
      const SQstGeneratorPlanSelectionEntry *entry = &selection_scratch[index];

      if (random_value < entry->prefix_weight) {
        *out_value_index = entry->value_index;
        *out_eligible_count = eligible_count;
        if (build->profile_runtime != NULL &&
            build->profile_runtime->profile_plan_select_count != UINT64_MAX) {
          ++build->profile_runtime->profile_plan_select_count;
        }
        qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                      &build->profile_runtime->profile_plan_select_ns);
        return QST_OK;
      }
    }
  } else {
    for (uint32_t index = 0; index < fragment->value_count; ++index) {
      const SQstGeneratorFragPlanValue *value =
          &plan->values[fragment->first_value_index + index];

      if (NULL != build->profile_runtime &&
          build->profile_runtime->profile_plan_select_value_examined != UINT64_MAX) {
        ++build->profile_runtime->profile_plan_select_value_examined;
      }
      if (value->value->weight == 0) {
        continue;
      }
      selected_weight += value->value->weight;
      if (random_value < selected_weight) {
        *out_value_index = fragment->first_value_index + index;
        *out_eligible_count = eligible_count;
        if (build->profile_runtime != NULL &&
            build->profile_runtime->profile_plan_select_count != UINT64_MAX) {
          ++build->profile_runtime->profile_plan_select_count;
        }
        qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                      &build->profile_runtime->profile_plan_select_ns);
        return QST_OK;
      }
    }
  }
  if (NULL != build->profile_runtime) {
    qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                  &build->profile_runtime->profile_plan_select_ns);
  }
  return QST_E_INTERNAL;
}

/* Selects from one prepared FRAG without entering the dynamic scan path.
 * This variant is used when profiling is disabled so the hot loop does not
 * execute the clock/profile branches for every immutable choice point. */
static inline EQstCode qstGeneratorPlanSelectStaticRandomUnprofiled(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan,
    const SQstGeneratorFragPlanEntry *fragment, uint32_t fragment_index,
    uint32_t depth, uint32_t *out_value_index, uint32_t *out_eligible_count) {
  const SQstGeneratorFragPlanStaticSelection *selection = NULL;
  uint64_t random_value = 0;
  uint32_t lower = 0;
  uint32_t upper = 0;

  if (depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return qstGeneratorPlanSelectRandom(build, plan, fragment_index, depth,
                                        out_value_index, out_eligible_count);
  }
  selection = build->static_selection_rows[depth];
  if (selection == NULL) {
    return qstGeneratorPlanSelectRandom(build, plan, fragment_index, depth,
                                        out_value_index, out_eligible_count);
  }
  selection += fragment_index;
  if (selection->eligible_count == 0 || selection->total_weight == 0 ||
      qstGeneratorRandomNextPrepared(build->random_state, &random_value) != QST_OK) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (selection->eligible_count == 1U) {
    if (selection->sole_value_index < fragment->first_value_index ||
        selection->sole_value_index >= fragment->first_value_index +
                                          fragment->value_count) {
      return QST_E_SHM_CORRUPT;
    }
    *out_value_index = selection->sole_value_index;
  } else {
    random_value = qstGeneratorPlanFastModulo(
        random_value, selection->total_weight, selection->reciprocal);
    if (fragment->value_count <= 4U) {
      for (lower = 0; lower < fragment->value_count; ++lower) {
        if (random_value < selection->prefix_weights[lower]) {
          break;
        }
      }
    } else {
      upper = fragment->value_count;
      while (lower < upper) {
        const uint32_t middle = lower + (upper - lower) / 2U;

        if (random_value < selection->prefix_weights[middle]) {
          upper = middle;
        } else {
          lower = middle + 1U;
        }
      }
    }
    if (lower >= fragment->value_count) {
      return QST_E_INTERNAL;
    }
    *out_value_index = fragment->first_value_index + lower;
  }
  *out_eligible_count = selection->eligible_count;
  return QST_OK;
}

/* The fast walker is entered only through qstGeneratorRandomBuildPlanWithRuntime
 * after qst_generator_frag_plan_prepare and qstGeneratorPlanBindStaticSelectionRows.
 * For a prepared FRAG, those two steps prove the row, prefix range and sole
 * value index. Keep the random-state error propagation, but do not revalidate
 * immutable plan data at every recursive selection. */
/* Selects from one prepared FRAG and records timing only when requested. */
static inline EQstCode qstGeneratorPlanSelectStaticRandom(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan,
    const SQstGeneratorFragPlanEntry *fragment, uint32_t fragment_index,
    uint32_t depth, uint32_t *out_value_index, uint32_t *out_eligible_count) {
  if (NULL != build && NULL == build->profile_runtime) {
    return qstGeneratorPlanSelectStaticRandomUnprofiled(
        build, plan, fragment, fragment_index, depth, out_value_index,
        out_eligible_count);
  }
  const uint64_t started_ns = qstGeneratorProfileNowNs(build->profile_runtime);
  const SQstGeneratorFragPlanStaticSelection *selection = NULL;
  uint64_t random_value = 0;
  uint32_t lower = 0;
  uint32_t upper = 0;

  if (depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return qstGeneratorPlanSelectRandom(build, plan, fragment_index, depth,
                                        out_value_index, out_eligible_count);
  }
  selection = build->static_selection_rows[depth];
  if (selection == NULL) {
    return qstGeneratorPlanSelectRandom(build, plan, fragment_index, depth,
                                        out_value_index, out_eligible_count);
  }
  selection += fragment_index;
  if (selection->eligible_count == 0 || selection->total_weight == 0 ||
      qstGeneratorRandomNextPrepared(build->random_state, &random_value) != QST_OK) {
    qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                  &build->profile_runtime->profile_plan_select_ns);
    return QST_E_INVALID_ARGUMENT;
  }
  if (selection->eligible_count == 1U) {
    if (selection->sole_value_index < fragment->first_value_index ||
        selection->sole_value_index >= fragment->first_value_index +
                                          fragment->value_count) {
      qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                    &build->profile_runtime->profile_plan_select_ns);
      return QST_E_SHM_CORRUPT;
    }
    *out_value_index = selection->sole_value_index;
  } else {
    random_value = qstGeneratorPlanFastModulo(
        random_value, selection->total_weight, selection->reciprocal);
    if (fragment->value_count <= 4U) {
      for (lower = 0; lower < fragment->value_count; ++lower) {
        if (random_value < selection->prefix_weights[lower]) {
          break;
        }
      }
    } else {
      upper = fragment->value_count;
      while (lower < upper) {
        const uint32_t middle = lower + (upper - lower) / 2U;

        if (random_value < selection->prefix_weights[middle]) {
          upper = middle;
        } else {
          lower = middle + 1U;
        }
      }
    }
    if (lower >= fragment->value_count) {
      qstGeneratorProfileAddElapsed(
          build->profile_runtime, started_ns,
          &build->profile_runtime->profile_plan_select_ns);
      return QST_E_INTERNAL;
    }
    *out_value_index = fragment->first_value_index + lower;
  }
  *out_eligible_count = selection->eligible_count;
  if (build->profile_runtime->profile_plan_select_count != UINT64_MAX) {
    ++build->profile_runtime->profile_plan_select_count;
  }
  qstGeneratorProfileAddElapsed(build->profile_runtime, started_ns,
                                &build->profile_runtime->profile_plan_select_ns);
  return QST_OK;
}

static void qstGeneratorPlanUnwindScopes(SQstGeneratorBuild *build,
                                         SQstGeneratorPlanFrame *frames,
                                         uint32_t frame_count) {
  while (frame_count != 0) {
    SQstGeneratorPlanFrame *frame = &frames[--frame_count];

    if (frame->kind == QST_GENERATOR_PLAN_FRAME_CHILD &&
        frame->scope_pushed != 0) {
      qstGeneratorScopePop(build);
      frame->scope_pushed = 0;
    }
  }
}

/* RANDOM enters this only after plan preparation binds an immutable static
 * selection row for every supported depth. Every VAL participates according to
 * its configured weight; no scope-dependent eligibility scan is performed. */
static inline __attribute__((always_inline)) EQstCode
qstGeneratorPlanSelectStaticRandomFast(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlanEntry *fragment,
    uint32_t fragment_index, uint32_t depth, uint32_t *out_value_index) {
  const SQstGeneratorFragPlanStaticSelection *selection =
      build->static_selection_rows[depth] + fragment_index;
  uint64_t random_value = 0;
  uint32_t lower = 0;
  EQstCode code = QST_OK;

  code = qstGeneratorRandomNextPrepared(build->random_state, &random_value);
  if (code != QST_OK) {
    return code;
  }
  if (selection->eligible_count == 1U) {
    *out_value_index = selection->sole_value_index;
    return QST_OK;
  }
  random_value = qstGeneratorPlanFastModulo(
      random_value, selection->total_weight, selection->reciprocal);
  if (fragment->value_count <= 4U) {
    while (lower + 1U < fragment->value_count &&
           random_value >= selection->prefix_weights[lower]) {
      ++lower;
    }
  } else {
    uint32_t upper = fragment->value_count;

    while (lower < upper) {
      const uint32_t middle = lower + (upper - lower) / 2U;

      if (random_value < selection->prefix_weights[middle]) {
        upper = middle;
      } else {
        lower = middle + 1U;
      }
    }
  }
  if (lower >= fragment->value_count) {
    return QST_E_INTERNAL;
  }
  *out_value_index = fragment->first_value_index + lower;
  return QST_OK;
}

static __attribute__((no_stack_protector)) EQstCode
qstGeneratorPlanBuildFragmentFast(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan,
    uint32_t fragment_index, uint32_t depth, uint32_t stack_depth,
    uint32_t *out_return_state);

static EQstCode qstGeneratorPlanBuildChildFast(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan,
    const SQstGeneratorFragPlanOperation *operation, uint32_t depth,
    uint32_t stack_depth, uint32_t *out_return_state) {
  const SQstGeneratorChildCall *child_call = NULL;
  uint32_t child_depth = 0;
  uint32_t iterations = 1;
  uint32_t child_return_state = 0;
  uint32_t start_offset = 0;
  uint32_t scope_pushed = 0;
  EQstCode code = QST_OK;

  child_call = &operation->child_call;
  if (child_call->recursion_cost != 0 && depth == 0) {
    if (build->choice_index == UINT32_MAX) return QST_E_RESOURCE;
    ++build->choice_index;
    *out_return_state = QST_GENERATOR_RETURN_NON_EMPTY;
    return QST_OK;
  }
  child_depth = child_call->recursion_cost == 0 ? depth : depth - 1U;
  if (child_call->call_kind == QST_GENERATOR_CALL_LOOP) {
    code = qstGeneratorSelectLoopIterations(build, child_call, &iterations);
    if (code != QST_OK) return code;
  }
  if (child_call->scope_action == QST_GENERATOR_SCOPE_QUERY_BLOCK) {
    code = qstGeneratorScopePush(build);
    if (code != QST_OK) return code;
    scope_pushed = 1;
  }
  start_offset = build->output->sql_length;
  for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
    code = qstGeneratorPlanBuildFragmentFast(
        build, plan, operation->child_fragment_index, child_depth,
        stack_depth + 1U, &child_return_state);
    if (code != QST_OK ||
        (child_call->call_kind == QST_GENERATOR_CALL_LOOP &&
         (child_return_state == QST_GENERATOR_RETURN_EMPTY_RESULT ||
          child_return_state == QST_GENERATOR_RETURN_STOP_CURRENT))) {
      break;
    }
  }
  if (code == QST_OK && build->selection_only == 0 &&
      child_call->scope_action == QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS) {
    code = qstGeneratorScopeBindAlias(build, build->output->sql + start_offset,
                                      build->output->sql_length - start_offset);
  }
  if (scope_pushed != 0) qstGeneratorScopePop(build);
  if (code == QST_OK) *out_return_state = child_return_state;
    return code;
}

static __attribute__((no_stack_protector)) EQstCode
qstGeneratorPlanBuildFragmentFast(
    SQstGeneratorBuild *build, const SQstGeneratorFragPlan *plan,
    uint32_t fragment_index, uint32_t depth, uint32_t stack_depth,
    uint32_t *out_return_state) {
#if 0
  uint32_t frame_count = 1;
  SQstGeneratorPlanFrame frames[QST_GENERATOR_PLAN_STACK_CAPACITY] = {0};
  EQstCode code = QST_OK;

  if (NULL == build || NULL == plan ||
      fragment_index >= plan->fragment_count ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return QST_E_INVALID_ARGUMENT;
  }
  frames[0] = (SQstGeneratorPlanFrame){
      .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
      .fragment_index = fragment_index,
      .depth = depth};
  while (frame_count != 0 && code == QST_OK) {
    SQstGeneratorPlanFrame *frame = &frames[frame_count - 1U];

    if (frame->kind == QST_GENERATOR_PLAN_FRAME_FRAGMENT) {
      const SQstGeneratorFragPlanEntry *fragment =
          &plan->fragments[frame->fragment_index];
      const SQstGeneratorFragPlanValue *planned_value = NULL;
      const SQstGeneratorValue *value = NULL;
      uint32_t value_index = 0;
      uint32_t eligible_count = 0;
      uint32_t runtime_ordinal = 0;

      code = fragment->has_runtime_value == 0 ?
          qstGeneratorPlanSelectStaticRandom(build, plan, fragment,
                                             frame->fragment_index, frame->depth,
                                             &value_index, &eligible_count) :
          qstGeneratorPlanSelectRandom(build, plan, frame->fragment_index,
                                       frame->depth, &value_index,
                                       &eligible_count);
      if (code != QST_OK || value_index >= plan->value_count ||
          build->choice_index == UINT32_MAX) {
        return code != QST_OK ? code : QST_E_RESOURCE;
      }
      planned_value = &plan->values[value_index];
      value = planned_value->value;
      if (NULL == value) {
        return QST_E_SHM_CORRUPT;
      }
      ++build->choice_index;
      if (value->generation_class == QST_GENERATOR_CLASS_ABNORMAL) {
        build->output->contains_abnormal = 1;
      }
      if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
        uint32_t runtime_count = 0;
        uint64_t runtime_append_started_ns = 0;

        code = qstGeneratorRuntimeCandidateCount(build, value->runtime_object_kind,
                                                 &runtime_count);
        if (code != QST_OK || runtime_count == 0) {
          return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
        }
        runtime_ordinal = (uint32_t)(build->random_state->state % runtime_count);
        runtime_append_started_ns =
            qstGeneratorProfileNowNs(build->profile_runtime);
        code = qstGeneratorRuntimeAppendObject(
            build, value->runtime_object_kind, runtime_ordinal, NULL);
        if (build->profile_runtime != NULL &&
            build->profile_runtime->profile_runtime_append_count != UINT64_MAX) {
          ++build->profile_runtime->profile_runtime_append_count;
        }
        qstGeneratorProfileAddElapsed(
            build->profile_runtime, runtime_append_started_ns,
            &build->profile_runtime->profile_runtime_append_ns);
      } else if (NULL != value->text) {
        code = qstGeneratorAppendBytes(build->output, value->text,
                                       planned_value->text_length);
      }
      if (code != QST_OK) {
        return code;
      }
      frame->kind = QST_GENERATOR_PLAN_FRAME_VALUE;
      frame->state = 0;
      frame->value_index = value_index;
      frame->operation_index = 0;
      if (value->kind == QST_GENERATOR_VALUE_CALL_SELF) {
        if (frame->depth == 0 || frame_count == QST_GENERATOR_PLAN_STACK_CAPACITY) {
          return QST_E_RESOURCE;
        }
        frame->state = 1;
        frames[frame_count++] = (SQstGeneratorPlanFrame){
            .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
            .fragment_index = frame->fragment_index,
            .depth = frame->depth - 1U};
      }
      continue;
    }
    if (frame->kind == QST_GENERATOR_PLAN_FRAME_VALUE) {
      const SQstGeneratorFragPlanValue *planned_value =
          &plan->values[frame->value_index];
      const SQstGeneratorValue *value = planned_value->value;

      if (NULL == value) {
        return QST_E_SHM_CORRUPT;
      }
      if (frame->state != 0) {
        qstGeneratorPlanFastReturn(frames, &frame_count,
                                   frame->child_return_state);
        continue;
      }
      if (frame->operation_index == planned_value->operation_count) {
        qstGeneratorPlanFastReturn(frames, &frame_count, value->return_state);
        continue;
      }
      {
        const SQstGeneratorFragPlanOperation *operation =
            &plan->operations[planned_value->first_operation_index +
                              frame->operation_index++];

        if (operation->operation_kind == QST_GENERATOR_STEP_INLINE_VALUE) {
          code = qstGeneratorAppendBytes(build->output, operation->step->text,
                                         operation->text_length);
        } else if (operation->operation_kind == QST_GENERATOR_STEP_CALL) {
          const SQstGeneratorChildCall *child_call = &operation->child_call;

          if (frame_count == QST_GENERATOR_PLAN_STACK_CAPACITY) {
            return QST_E_RESOURCE;
          }
          if (child_call->call_kind == QST_GENERATOR_CALL_REQUIRED &&
              child_call->scope_action == QST_GENERATOR_SCOPE_NONE &&
              (child_call->recursion_cost == 0 || frame->depth != 0)) {
            frames[frame_count++] = (SQstGeneratorPlanFrame){
                .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
                .fragment_index = operation->child_fragment_index,
                .depth = child_call->recursion_cost == 0 ? frame->depth :
                                                            frame->depth - 1U};
          } else {
            frames[frame_count++] = (SQstGeneratorPlanFrame){
                .kind = QST_GENERATOR_PLAN_FRAME_CHILD,
                .depth = frame->depth,
                .operation = operation};
          }
        } else {
          return QST_E_SHM_CORRUPT;
        }
      }
      continue;
    }
    if (frame->kind == QST_GENERATOR_PLAN_FRAME_CHILD) {
      const SQstGeneratorChildCall *child_call = &frame->operation->child_call;

      if (frame->state == 0) {
        if (child_call->recursion_cost != 0 && frame->depth == 0) {
          if (build->choice_index == UINT32_MAX) {
            return QST_E_RESOURCE;
          }
          ++build->choice_index;
          qstGeneratorPlanFastReturn(frames, &frame_count,
                                     QST_GENERATOR_RETURN_NON_EMPTY);
          continue;
        }
        frame->child_depth = child_call->recursion_cost == 0 ? frame->depth :
                                                                 frame->depth - 1U;
        frame->iterations = 1;
        if (child_call->call_kind == QST_GENERATOR_CALL_LOOP) {
          code = qstGeneratorSelectLoopIterations(build, child_call,
                                                  &frame->iterations);
          if (code != QST_OK) {
            return code;
          }
        }
        if (child_call->scope_action == QST_GENERATOR_SCOPE_QUERY_BLOCK) {
          code = qstGeneratorScopePush(build);
          if (code != QST_OK) {
            return code;
          }
          frame->scope_pushed = 1;
        }
        frame->start_offset = build->output->sql_length;
        frame->state = 1;
      }
      if (frame->state == 2) {
        ++frame->iteration;
        if (child_call->call_kind == QST_GENERATOR_CALL_LOOP &&
            (frame->child_return_state == QST_GENERATOR_RETURN_EMPTY_RESULT ||
             frame->child_return_state == QST_GENERATOR_RETURN_STOP_CURRENT)) {
          frame->iteration = frame->iterations;
        }
        frame->state = 1;
      }
      if (frame->iteration < frame->iterations) {
        if (frame_count == QST_GENERATOR_PLAN_STACK_CAPACITY) {
          return QST_E_RESOURCE;
        }
        frame->state = 2;
        frames[frame_count++] = (SQstGeneratorPlanFrame){
            .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
            .fragment_index = frame->operation->child_fragment_index,
            .depth = frame->child_depth};
        continue;
      }
      if (child_call->scope_action == QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS) {
        code = qstGeneratorScopeBindAlias(
            build, build->output->sql + frame->start_offset,
            build->output->sql_length - frame->start_offset);
        if (code != QST_OK) {
          return code;
        }
      }
      if (frame->scope_pushed != 0) {
        qstGeneratorScopePop(build);
        frame->scope_pushed = 0;
      }
      qstGeneratorPlanFastReturn(frames, &frame_count,
                                 frame->child_return_state);
      continue;
    }
    return QST_E_SHM_CORRUPT;
  }
  return code;
}
#endif
  const SQstGeneratorFragPlanEntry *fragment = NULL;
  const SQstGeneratorFragPlanValue *planned_value = NULL;
  const SQstGeneratorValue *value = NULL;
  uint32_t value_index = 0;
  uint32_t eligible_count = 0;
  uint32_t return_state = QST_GENERATOR_RETURN_NON_EMPTY;
  EQstCode code = QST_OK;

  if (NULL == build || NULL == plan || NULL == out_return_state ||
      fragment_index >= plan->fragment_count ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH ||
      stack_depth >= QST_GENERATOR_PLAN_STACK_CAPACITY) {
    return QST_E_INVALID_ARGUMENT;
  }
  fragment = &plan->fragments[fragment_index];
  code = qstGeneratorPlanSelectStaticRandomFast(
      build, fragment, fragment_index, depth, &value_index);
  if (code != QST_OK || value_index >= plan->value_count ||
      build->choice_index == UINT32_MAX) {
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  planned_value = &plan->values[value_index];
  value = planned_value->value;
  if (NULL == value) {
    return QST_E_SHM_CORRUPT;
  }
  ++build->choice_index;
  qstGeneratorSelectionMark(
      build, fragment->fragment_id, value->value_id,
      value->generation_class == QST_GENERATOR_CLASS_ABNORMAL);
  build->contains_abnormal |=
      (uint8_t)(value->generation_class == QST_GENERATOR_CLASS_ABNORMAL);
  if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
    uint32_t runtime_count = 0;
    uint32_t runtime_ordinal = 0;
    const uint64_t runtime_append_started_ns =
        qstGeneratorProfileNowNs(build->profile_runtime);

    if (build->selection_only == 0) {
      code = qstGeneratorRuntimeCandidateCount(
          build, value->runtime_object_kind, &runtime_count);
      if (code != QST_OK) {
        runtime_count = 0;
        code = QST_OK;
      }
      if (runtime_count != 0) {
        runtime_ordinal =
            (uint32_t)(build->random_state->state % runtime_count);
        code = qstGeneratorRuntimeAppendObject(
            build, value->runtime_object_kind, runtime_ordinal, NULL);
      }
    }
    if (build->selection_only == 0 && build->profile_runtime != NULL &&
        build->profile_runtime->profile_runtime_append_count != UINT64_MAX) {
      ++build->profile_runtime->profile_runtime_append_count;
    }
    qstGeneratorProfileAddElapsed(
        build->profile_runtime, runtime_append_started_ns,
        &build->profile_runtime->profile_runtime_append_ns);
    if (code != QST_OK) {
      return code;
    }
  } else if (NULL != value->text && build->selection_only == 0) {
    code = qstGeneratorAppendBytesFast(build->output, value->text,
                                       planned_value->text_length);
    if (code != QST_OK) {
      return code;
    }
  }
  if (value->kind == QST_GENERATOR_VALUE_CALL_SELF) {
    if (depth != 0) {
      code = qstGeneratorPlanBuildFragmentFast(
          build, plan, fragment_index, depth - 1U, stack_depth + 1U,
          &return_state);
      if (code != QST_OK) {
        return code;
      }
    }
  }
  for (uint32_t operation_index = 0;
       operation_index < planned_value->operation_count; ++operation_index) {
    const SQstGeneratorFragPlanOperation *operation =
        &plan->operations[planned_value->first_operation_index +
                          operation_index];

    if (operation->operation_kind == QST_GENERATOR_STEP_INLINE_VALUE) {
      code = build->selection_only != 0
                 ? QST_OK
                 : qstGeneratorAppendBytesFast(build->output, operation->step->text,
                                               operation->text_length);
    } else if (operation->operation_kind == QST_GENERATOR_STEP_CALL) {
      const SQstGeneratorChildCall *child_call = &operation->child_call;

      if (child_call->call_kind == QST_GENERATOR_CALL_REQUIRED &&
          child_call->scope_action == QST_GENERATOR_SCOPE_NONE &&
          (child_call->recursion_cost == 0 || depth != 0)) {
        const uint32_t child_depth = child_call->recursion_cost == 0 ?
                                         depth :
                                         depth - 1U;

        code = qstGeneratorPlanBuildFragmentFast(
            build, plan, operation->child_fragment_index, child_depth,
            stack_depth + 1U, &return_state);
      } else {
        code = qstGeneratorPlanBuildChildFast(
            build, plan, operation, depth, stack_depth, &return_state);
      }
    } else {
      return QST_E_SHM_CORRUPT;
    }
    if (code != QST_OK) {
      return code;
    }
  }
  (void)eligible_count;
  *out_return_state = value->return_state;
  return QST_OK;
}

static EQstCode qstGeneratorRandomBuildPlanWithRuntime(
    const SQstGeneratorFragPlan *plan, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *cached_runtime, SQstGeneratedSql *out_sql) {
  SQstGeneratorBuild *build = NULL;
  SQstGeneratorPlanFrame *frames = NULL;
  uint32_t frame_count = 1;
  int32_t collect_path_metadata = 0;
  EQstCode code = QST_OK;

  if (NULL == plan || NULL == plan->catalog || NULL == state || NULL == out_sql ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH ||
      plan->root_fragment_index >= plan->fragment_count || NULL == plan->fragments ||
      NULL == plan->values) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstGeneratorGeneratedSqlReset(out_sql);
  if (NULL == cached_runtime ||
      NULL == cached_runtime->catalog_value_index ||
      NULL == cached_runtime->catalog_fragment_index ||
      NULL == cached_runtime->catalog_value_eligibility ||
      NULL == cached_runtime->buildable_fragments ||
      NULL == cached_runtime->buildable_fragment_epochs ||
      NULL == cached_runtime->buildable_values ||
      NULL == cached_runtime->buildable_value_epochs ||
      NULL == cached_runtime->buildable_lookup_cache ||
      cached_runtime->catalog_value_count == 0 ||
      cached_runtime->buildability_epoch == 0) {
    return QST_E_SHM_CORRUPT;
  }
  code = qstGeneratorPlanScratchReset(cached_runtime, &build, &frames);
  if (code != QST_OK) {
    return code;
  }
  collect_path_metadata = cached_runtime->selection_only == 0 &&
                          cached_runtime->disable_path_metadata == 0;
  build->catalog = plan->catalog;
  build->random_state = state;
  build->output = out_sql;
  build->is_random = 1;
  build->disable_path_metadata = cached_runtime->disable_path_metadata;
  build->selection_only = cached_runtime->selection_only;
  build->disable_occurrence_metadata = cached_runtime->disable_path_metadata;
  build->profile_runtime = cached_runtime->selection_only != 0 ||
                               cached_runtime->profile_enabled != 0
                               ? cached_runtime
                               : NULL;
  build->dialect_mask = runtime != NULL ? qstGeneratorDialectMask(runtime->dialect) : 0;
  build->buildable_value_index =
      (const SQstGeneratorBuildableValue *)cached_runtime->catalog_value_index;
  build->buildable_value_count = cached_runtime->catalog_value_count;
  build->buildable_fragment_index =
      (const SQstGeneratorBuildableFragment *)cached_runtime->catalog_fragment_index;
  build->buildable_value_eligibility = cached_runtime->catalog_value_eligibility;
  build->buildable_fragments = cached_runtime->buildable_fragments;
  build->buildable_fragment_epochs = cached_runtime->buildable_fragment_epochs;
  build->buildable_values = cached_runtime->buildable_values;
  build->buildable_value_epochs = cached_runtime->buildable_value_epochs;
  build->lookup_cache =
      (SQstGeneratorBuildLookupCache *)cached_runtime->buildable_lookup_cache;
  build->runtime_candidate_lists =
      (SQstGeneratorRuntimeCandidate **)cached_runtime->runtime_candidate_lists;
  build->runtime_candidate_capacities =
      cached_runtime->runtime_candidate_capacities;
  build->runtime_object_index =
      (const SQstGeneratorRuntimeObjectIndex *)cached_runtime->runtime_object_index;
  build->plan_selection_scratch = cached_runtime->plan_selection_scratch;
  build->plan_selection_capacity = cached_runtime->plan_selection_capacity;
  build->buildability_epoch = cached_runtime->buildability_epoch;
  build->buildability_epoch_owner = &cached_runtime->buildability_epoch;
  qstGeneratorPlanBindStaticSelectionRows(build, plan);
  if (build->selection_only != 0) {
    build->runtime = runtime;
    code = qstGeneratorScopePush(build);
  } else {
    code = qstGeneratorRuntimeInitialize(build, runtime);
  }
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(build);
    return code;
  }
  if (collect_path_metadata == 0) {
    uint32_t return_state = QST_GENERATOR_RETURN_NON_EMPTY;

    code = qstGeneratorPlanBuildFragmentFast(
        build, plan, plan->root_fragment_index, depth, 1U, &return_state);
    out_sql->contains_abnormal = build->contains_abnormal;
    frame_count = 0;
  } else {
    frames[0] = (SQstGeneratorPlanFrame){
        .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
        .parent_frame_index = UINT32_MAX,
        .fragment_index = plan->root_fragment_index,
        .depth = depth};
  }
  while (frame_count != 0 && code == QST_OK) {
    SQstGeneratorPlanFrame *frame = &frames[frame_count - 1U];

    if (frame->kind == QST_GENERATOR_PLAN_FRAME_FRAGMENT) {
      const SQstGeneratorFragPlanValue *planned_value = NULL;
      const SQstGeneratorValue *value = NULL;
      SQstGenerationOccurrence *occurrence = NULL;
      uint32_t eligible_count = 0;
      uint32_t runtime_ordinal = 0;
      uint32_t runtime_count = 0;

      code = qstGeneratorPlanSelectRandom(build, plan, frame->fragment_index,
                                          frame->depth, &frame->value_index,
                                          &eligible_count);
      if (code != QST_OK || frame->value_index >= plan->value_count ||
          build->choice_index == UINT32_MAX ||
          (collect_path_metadata != 0 &&
           qstGeneratorReserveChoice(out_sql, build->choice_index) != QST_OK)) {
        code = code == QST_OK ? QST_E_RESOURCE : code;
        break;
      }
      planned_value = &plan->values[frame->value_index];
      value = planned_value->value;
      if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE) {
        code = qstGeneratorRuntimeCandidateCount(
            build, value->runtime_object_kind, &runtime_count);
        if (code != QST_OK) {
          runtime_count = 0;
          code = QST_OK;
        }
        if (runtime_count != 0) {
          runtime_ordinal = (uint32_t)(state->state % runtime_count);
        }
      }
      if (collect_path_metadata != 0) {
        out_sql->choice_cardinalities[build->choice_index] = eligible_count;
        out_sql->selected_value_ids[build->choice_index] = value->value_id;
      }
      ++build->choice_index;
      if (collect_path_metadata != 0 &&
          (out_sql->occurrence_count == UINT32_MAX ||
           qstGeneratorReserveOccurrence(out_sql, out_sql->occurrence_count) !=
               QST_OK)) {
        code = QST_E_RESOURCE;
        break;
      }
      occurrence = NULL;
      if (collect_path_metadata != 0) {
        frame->occurrence_index = out_sql->occurrence_count;
        frame->occurrence_id = out_sql->occurrence_count + 1U;
        frame->start_offset = out_sql->sql_length;
        occurrence = &out_sql->occurrences[out_sql->occurrence_count++];
        memset(occurrence, 0, sizeof(*occurrence));
        occurrence->occurrence_id = frame->occurrence_id;
        occurrence->parent_occurrence_id = frame->parent_occurrence_id;
        occurrence->fragment_id = plan->fragments[frame->fragment_index].fragment_id;
        occurrence->value_id = value->value_id;
        occurrence->child_index = frame->child_index;
        occurrence->loop_iteration = frame->loop_iteration;
        occurrence->depth = frame->depth;
        occurrence->runtime_object_kind = value->runtime_object_kind;
      }
      out_sql->contains_abnormal |=
          (uint8_t)(value->generation_class == QST_GENERATOR_CLASS_ABNORMAL);
      if (value->runtime_object_kind != QST_RUNTIME_OBJECT_NONE &&
          runtime_count != 0) {
        const uint64_t runtime_append_started_ns =
            qstGeneratorProfileNowNs(build->profile_runtime);

        code = build->selection_only != 0
                 ? QST_OK
                   : qstGeneratorRuntimeAppendObject(
                         build, value->runtime_object_kind, runtime_ordinal,
                         occurrence);
        if (build->profile_runtime != NULL &&
            build->profile_runtime->profile_runtime_append_count != UINT64_MAX) {
          ++build->profile_runtime->profile_runtime_append_count;
        }
        qstGeneratorProfileAddElapsed(
            build->profile_runtime, runtime_append_started_ns,
            &build->profile_runtime->profile_runtime_append_ns);
      } else if (NULL != value->text &&
                 build->selection_only == 0) {
        code = qstGeneratorAppend(out_sql, value->text);
      }
      if (code != QST_OK) {
        break;
      }
      frame->kind = QST_GENERATOR_PLAN_FRAME_VALUE;
      if (value->kind == QST_GENERATOR_VALUE_CALL_SELF) {
        if (frame->depth == 0 || frame_count == QST_GENERATOR_PLAN_STACK_CAPACITY) {
          code = QST_E_RESOURCE;
          break;
        }
        frame->state = 1;
        frames[frame_count] = (SQstGeneratorPlanFrame){
            .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
            .parent_frame_index = frame_count - 1U,
            .fragment_index = frame->fragment_index,
            .depth = frame->depth - 1U,
            .parent_occurrence_id = frame->occurrence_id};
        ++frame_count;
      }
      continue;
    }
    if (frame->kind == QST_GENERATOR_PLAN_FRAME_VALUE) {
      const SQstGeneratorFragPlanValue *planned_value =
          &plan->values[frame->value_index];
      const SQstGeneratorValue *value = planned_value->value;

      if (frame->state != 0) {
        frame->return_state = frame->child_return_state;
      } else if (frame->operation_index < planned_value->operation_count) {
        const SQstGeneratorFragPlanOperation *operation =
            &plan->operations[planned_value->first_operation_index +
                              frame->operation_index++];

        if (operation->operation_kind == QST_GENERATOR_STEP_INLINE_VALUE) {
          if (collect_path_metadata != 0 && build->selection_only == 0) {
            code = qstGeneratorAppendInlineStep(
                build, plan->fragments[frame->fragment_index].fragment_id,
                frame->occurrence_id, operation->operation_index, operation->step);
          } else if (build->selection_only == 0) {
            code = qstGeneratorAppend(out_sql, operation->step->text);
          }
          continue;
        }
        if (operation->operation_kind != QST_GENERATOR_STEP_CALL ||
            frame_count == QST_GENERATOR_PLAN_STACK_CAPACITY) {
          code = QST_E_SHM_CORRUPT;
          break;
        }
        frames[frame_count] = (SQstGeneratorPlanFrame){
            .kind = QST_GENERATOR_PLAN_FRAME_CHILD,
            .parent_frame_index = frame_count - 1U,
            .depth = frame->depth,
            .parent_occurrence_id = frame->occurrence_id,
            .child_index = operation->operation_index,
            .operation = operation};
        ++frame_count;
        continue;
      } else {
        frame->return_state = value->return_state;
      }
      if (collect_path_metadata != 0) {
        SQstGenerationOccurrence *occurrence = &out_sql->occurrences[frame->occurrence_index];

        occurrence->return_state = frame->return_state;
        occurrence->text_offset = frame->start_offset;
        occurrence->text_length = out_sql->sql_length - frame->start_offset;
      }
    } else {
      const SQstGeneratorChildCall *child_call = &frame->operation->child_call;

      if (frame->state == 0) {
        if (child_call->recursion_cost != 0 && frame->depth == 0) {
          if (build->choice_index == UINT32_MAX ||
              (collect_path_metadata != 0 &&
               qstGeneratorReserveChoice(out_sql, build->choice_index) !=
                   QST_OK)) {
            code = QST_E_RESOURCE;
            break;
          }
          if (collect_path_metadata != 0) {
            out_sql->choice_cardinalities[build->choice_index] = 1;
            out_sql->selected_value_ids[build->choice_index] = 0;
          }
          ++build->choice_index;
          frame->return_state = QST_GENERATOR_RETURN_NON_EMPTY;
          frame->state = 3;
        } else {
          frame->child_depth = child_call->recursion_cost == 0 ? frame->depth :
                                                                  frame->depth - 1U;
          frame->iterations = 1;
          if (child_call->call_kind == QST_GENERATOR_CALL_LOOP) {
            code = qstGeneratorSelectLoopIterations(build, child_call,
                                                    &frame->iterations);
            if (code != QST_OK) {
              break;
            }
          }
          if (child_call->scope_action == QST_GENERATOR_SCOPE_QUERY_BLOCK) {
            code = qstGeneratorScopePush(build);
            if (code != QST_OK) {
              break;
            }
            frame->scope_pushed = 1;
          }
          frame->start_offset = out_sql->sql_length;
          frame->state = 1;
        }
      }
      if (frame->state == 2) {
        ++frame->iteration;
        if (child_call->call_kind == QST_GENERATOR_CALL_LOOP &&
            (frame->child_return_state == QST_GENERATOR_RETURN_EMPTY_RESULT ||
             frame->child_return_state == QST_GENERATOR_RETURN_STOP_CURRENT)) {
          frame->iteration = frame->iterations;
        }
        frame->state = 1;
      }
      if (frame->state == 1 && frame->iteration < frame->iterations) {
        if (frame_count == QST_GENERATOR_PLAN_STACK_CAPACITY) {
          code = QST_E_RESOURCE;
          break;
        }
        frame->state = 2;
        frames[frame_count] = (SQstGeneratorPlanFrame){
            .kind = QST_GENERATOR_PLAN_FRAME_FRAGMENT,
            .parent_frame_index = frame_count - 1U,
            .fragment_index = frame->operation->child_fragment_index,
            .depth = frame->child_depth,
            .parent_occurrence_id = frame->parent_occurrence_id,
            .child_index = frame->child_index,
            .loop_iteration = frame->iteration};
        ++frame_count;
        continue;
      }
      if (frame->state != 3) {
        frame->return_state = frame->child_return_state;
        if (child_call->scope_action == QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS &&
            build->selection_only == 0) {
          code = qstGeneratorScopeBindAlias(
              build, out_sql->sql + frame->start_offset,
              out_sql->sql_length - frame->start_offset);
          if (code != QST_OK) {
            break;
          }
        }
        if (frame->scope_pushed != 0) {
          qstGeneratorScopePop(build);
          frame->scope_pushed = 0;
        }
      }
    }
    if (frame->parent_frame_index == UINT32_MAX) {
      --frame_count;
    } else {
      frames[frame->parent_frame_index].child_return_state = frame->return_state;
      --frame_count;
    }
  }
  if (code == QST_OK && build->selection_only == 0) {
    code = qstGeneratorEnsureStatement(out_sql);
  }
  if (code == QST_OK && build->selection_only == 0) {
    code = qstGeneratorNormalizeBareHintTokens(out_sql);
  }
  if (code == QST_OK && build->selection_only == 0 &&
      out_sql->sql_length < QST_GENERATOR_SQL_BYTES) {
    out_sql->sql[out_sql->sql_length] = '\0';
  }
  if (code == QST_OK) {
    out_sql->choice_count = build->choice_index;
  }
  if (code == QST_OK && build->selection_only == 0) {
    SQstSha256 digest = {0};

    qst_sha256_init(&digest);
    qst_sha256_update(&digest, (const uint8_t *)out_sql->sql, out_sql->sql_length);
    qst_sha256_final(&digest, out_sql->sql_sha256.bytes);
  }
  if (code != QST_OK) {
    qstGeneratorPlanUnwindScopes(build, frames, frame_count);
    qst_generator_generated_sql_destroy(out_sql);
  }
  cached_runtime->plan_selection_scratch = build->plan_selection_scratch;
  cached_runtime->plan_selection_capacity = build->plan_selection_capacity;
  qstGeneratorBuildDestroy(build);
  return code;
}

EQstCode qst_generator_random_build_with_runtime(
    const SQstGeneratorCatalog *catalog, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratedSql *out_sql) {
  EQstCode code = qst_generator_catalog_validate(catalog);

  if (code != QST_OK) {
    return code;
  }
  if (NULL == state || NULL == out_sql ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t attempt = 0;
       attempt < QST_GENERATOR_RANDOM_REBUILD_ATTEMPT_LIMIT; ++attempt) {
    code = qstGeneratorRandomBuildValidatedWithRuntime(catalog, state, depth,
                                                        runtime, NULL, out_sql);
    if (qstGeneratorRandomBuildNeedsRetry(code) == 0) {
      return code;
    }
  }
  qst_generator_generated_sql_destroy(out_sql);
  return QST_E_SQL_LIMIT;
}

EQstCode qst_generator_random_build_prepared_with_runtime(
    const SQstGeneratorCatalog *catalog, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *prepared_runtime,
    SQstGeneratedSql *out_sql) {
  if (NULL == catalog || NULL == prepared_runtime ||
      NULL == state || NULL == out_sql ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH ||
      prepared_runtime->validated_catalog != catalog ||
      memcmp(prepared_runtime->validated_catalog_digest.bytes,
             catalog->digest.bytes, sizeof(catalog->digest.bytes)) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t attempt = 0;
       attempt < QST_GENERATOR_RANDOM_REBUILD_ATTEMPT_LIMIT; ++attempt) {
    EQstCode code = qstGeneratorRandomBuildValidatedWithRuntime(
        catalog, state, depth, runtime, prepared_runtime, out_sql);

    if (qstGeneratorRandomBuildNeedsRetry(code) == 0) {
      return code;
    }
  }
  qst_generator_generated_sql_destroy(out_sql);
  return QST_E_SQL_LIMIT;
}

EQstCode qst_generator_random_build(const SQstGeneratorCatalog *catalog,
                                    SQstGeneratorRandomState *state,
                                    uint32_t depth,
                                    SQstGeneratedSql *out_sql) {
  return qst_generator_random_build_with_runtime(catalog, state, depth, NULL,
                                                 out_sql);
}

EQstCode qst_generator_random_plan_build_with_runtime(
    const SQstGeneratorFragPlan *plan, SQstGeneratorRandomState *state,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *prepared_runtime,
    SQstGeneratedSql *out_sql) {
  EQstCode code = QST_OK;

  if (NULL == plan || NULL == plan->catalog || NULL == state ||
      NULL == prepared_runtime || NULL == out_sql ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH ||
      prepared_runtime->validated_catalog != plan->catalog ||
      memcmp(prepared_runtime->validated_catalog_digest.bytes,
             plan->catalog->digest.bytes,
             sizeof(plan->catalog->digest.bytes)) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstGeneratorRandomBuildPlanWithRuntime(
      plan, state, depth, runtime, prepared_runtime, out_sql);
  if (code == QST_E_INVALID_ARGUMENT || code == QST_E_SQL_LIMIT) {
    qst_generator_generated_sql_destroy(out_sql);
    return QST_E_SQL_LIMIT;
  }
  return code;
}

static EQstCode qstGeneratorBuildFullPath(
    const SQstGeneratorCatalog *catalog, const SQstGeneratorFullPath *path,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratedSql *out_sql) {
  SQstGeneratorBuild build = {0};
  EQstCode code = QST_OK;
  uint32_t return_state = QST_GENERATOR_RETURN_NON_EMPTY;

  if (NULL == catalog || NULL == path || NULL == out_sql) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_generator_generated_sql_destroy(out_sql);
  build.catalog = catalog;
  build.full_choices = path->choices;
  build.full_choice_count = path->choice_count;
  build.output = out_sql;
  build.dialect_mask = runtime != NULL ? qstGeneratorDialectMask(runtime->dialect) : 0;
  code = qstGeneratorRuntimeInitialize(&build, runtime);
  if (code != QST_OK) {
    return code;
  }
  code = qstGeneratorBuildabilityInitialize(&build);
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(&build);
    return code;
  }
  code = qstGeneratorBuildFragment(&build, catalog->root_fragment_id, depth,
                                   0, 0, 0, &return_state);
  if (code == QST_E_INVALID_ARGUMENT || code == QST_E_SQL_LIMIT) {
    code = qstGeneratorFinalizeUnsupportedPath(out_sql);
  }
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(&build);
    qst_generator_generated_sql_destroy(out_sql);
    return code;
  }
  code = qstGeneratorEnsureStatement(out_sql);
  if (code == QST_OK) {
    code = qstGeneratorNormalizeBareHintTokens(out_sql);
  }
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(&build);
    qst_generator_generated_sql_destroy(out_sql);
    return code;
  }
  out_sql->choice_count = build.choice_index;
  {
    SQstSha256 digest = {0};

    qst_sha256_init(&digest);
    qst_sha256_update(&digest, (const uint8_t *)out_sql->sql,
                      out_sql->sql_length);
    qst_sha256_final(&digest, out_sql->sql_sha256.bytes);
  }
  qstGeneratorBuildDestroy(&build);
  return QST_OK;
}

/* Builds one FULL path with a target-private cache prepared before the first
 * path. The cache only contains immutable catalog indexes and builder-local
 * scratch, so it cannot change the fixed DFS selection sequence. */
static EQstCode qstGeneratorBuildFullPathCached(
    const SQstGeneratorCatalog *catalog, const SQstGeneratorFullPath *path,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorRandomRuntime *cached_runtime, SQstGeneratedSql *out_sql) {
  SQstGeneratorBuild build = {0};
  EQstCode code = QST_OK;
  uint32_t return_state = QST_GENERATOR_RETURN_NON_EMPTY;

  if (NULL == catalog || NULL == path || NULL == cached_runtime ||
      NULL == out_sql || NULL == cached_runtime->catalog_value_index ||
      NULL == cached_runtime->catalog_fragment_index ||
      NULL == cached_runtime->catalog_value_eligibility ||
      cached_runtime->catalog_value_count == 0 ||
      NULL == cached_runtime->buildable_fragments ||
      NULL == cached_runtime->buildable_fragment_epochs ||
      NULL == cached_runtime->buildable_values ||
      NULL == cached_runtime->buildable_value_epochs ||
      NULL == cached_runtime->buildable_lookup_cache ||
      cached_runtime->buildability_epoch == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  qstGeneratorGeneratedSqlReset(out_sql);
  build.catalog = catalog;
  build.full_choices = path->choices;
  build.full_choice_count = path->choice_count;
  build.output = out_sql;
  /* FULL only retains choice metadata to advance its DFS cursor. Occurrence
   * records are diagnostic-only and are not consumed by the pending pipeline. */
  build.disable_occurrence_metadata = 1;
  build.dialect_mask = runtime != NULL ? qstGeneratorDialectMask(runtime->dialect) : 0;
  build.buildable_value_index =
      (const SQstGeneratorBuildableValue *)cached_runtime->catalog_value_index;
  build.buildable_value_count = cached_runtime->catalog_value_count;
  build.buildable_fragment_index =
      (const SQstGeneratorBuildableFragment *)cached_runtime->catalog_fragment_index;
  build.buildable_value_eligibility = cached_runtime->catalog_value_eligibility;
  build.buildable_fragments = cached_runtime->buildable_fragments;
  build.buildable_fragment_epochs = cached_runtime->buildable_fragment_epochs;
  build.buildable_values = cached_runtime->buildable_values;
  build.buildable_value_epochs = cached_runtime->buildable_value_epochs;
  build.lookup_cache =
      (SQstGeneratorBuildLookupCache *)cached_runtime->buildable_lookup_cache;
  build.runtime_candidate_lists =
      (SQstGeneratorRuntimeCandidate **)cached_runtime->runtime_candidate_lists;
  build.runtime_candidate_capacities = cached_runtime->runtime_candidate_capacities;
  build.runtime_object_index =
      (const SQstGeneratorRuntimeObjectIndex *)cached_runtime->runtime_object_index;
  build.buildability_epoch = cached_runtime->buildability_epoch;
  build.buildability_epoch_owner = &cached_runtime->buildability_epoch;
  code = qstGeneratorRuntimeInitialize(&build, runtime);
  if (code != QST_OK) {
    qstGeneratorBuildDestroy(&build);
    return code;
  }
  code = qstGeneratorBuildFragment(&build, catalog->root_fragment_id, depth,
                                   0, 0, 0, &return_state);
  if (code == QST_E_INVALID_ARGUMENT || code == QST_E_SQL_LIMIT) {
    code = qstGeneratorFinalizeUnsupportedPath(out_sql);
  }
  code = qstGeneratorEnsureStatement(out_sql);
  if (code == QST_OK) {
    code = qstGeneratorNormalizeBareHintTokens(out_sql);
  }
  if (code == QST_OK) {
    SQstSha256 digest = {0};

    out_sql->choice_count = build.choice_index;
    qst_sha256_init(&digest);
    qst_sha256_update(&digest, (const uint8_t *)out_sql->sql,
                      out_sql->sql_length);
    qst_sha256_final(&digest, out_sql->sql_sha256.bytes);
  }
  qstGeneratorBuildDestroy(&build);
  if (code != QST_OK) {
    qst_generator_generated_sql_destroy(out_sql);
  }
  return code;
}

static EQstCode qstGeneratorFullPathAdvance(SQstGeneratorFullPath *path,
                                            const SQstGeneratedSql *sql) {
  uint32_t previous_count = 0;

  if (NULL == path || NULL == sql) {
    return QST_E_INVALID_ARGUMENT;
  }
  /* A fixed SQL has one valid FULL combination and no successor. */
  if (sql->choice_count == 0) {
    return QST_E_QUEUE_EMPTY;
  }
  if (NULL == sql->choice_cardinalities) {
    return QST_E_INVALID_ARGUMENT;
  }
  previous_count = path->choice_count;
  if (qstGeneratorReserveArray((void **)&path->choices, &path->choice_capacity,
                               sql->choice_count,
                               sizeof(*path->choices)) != QST_OK) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = previous_count; index < sql->choice_count; ++index) {
    path->choices[index] = 0;
  }
  path->choice_count = sql->choice_count;
  for (uint32_t position = sql->choice_count; position > 0; --position) {
    const uint32_t index = position - 1U;

    if (sql->choice_cardinalities[index] == 0 ||
        path->choices[index] >= sql->choice_cardinalities[index]) {
      return QST_E_INVALID_ARGUMENT;
    }
    if (path->choices[index] + 1U < sql->choice_cardinalities[index]) {
      ++path->choices[index];
      path->choice_count = index + 1U;
      return QST_OK;
    }
    path->choices[index] = 0;
  }
  return QST_E_QUEUE_EMPTY;
}

/* Copies the next deterministic DFS path into a builder-private seed. */
EQstCode qst_generator_full_pending_capture_successor(
    const SQstGeneratorFullPending *pending,
    SQstGeneratorFullPathSeed *seed) {
  SQstGeneratorFullPath path = {0};
  EQstCode code = QST_OK;

  if (NULL == pending || NULL == seed || pending->valid == 0 ||
      pending->full_path_ready == 0 ||
      pending->full_path_exhausted != 0 ||
      pending->target_index >= QST_GENERATOR_TARGET_COUNT ||
      pending->full_path_target_index != pending->target_index ||
      pending->full_path_next_ordinal != pending->cursor_next_path_ordinal ||
      pending->cursor_next_path_ordinal == UINT64_MAX ||
      (pending->sql.choice_count != 0 &&
       NULL == pending->sql.choice_cardinalities)) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_generator_full_path_seed_destroy(seed);
  if (pending->full_path_choice_count != 0) {
    code = qstGeneratorReserveArray(
        (void **)&seed->full_path_choices, &seed->full_path_choice_capacity,
        pending->full_path_choice_count, sizeof(*seed->full_path_choices));
    if (code != QST_OK) {
      qst_generator_full_path_seed_destroy(seed);
      return code;
    }
    memcpy(seed->full_path_choices, pending->full_path_choices,
           (size_t)pending->full_path_choice_count *
               sizeof(*seed->full_path_choices));
  }
  if (pending->sql.choice_count != 0) {
    code = qstGeneratorReserveArray(
        (void **)&seed->choice_cardinalities, &seed->choice_capacity,
        pending->sql.choice_count, sizeof(*seed->choice_cardinalities));
    if (code != QST_OK) {
      qst_generator_full_path_seed_destroy(seed);
      return code;
    }
    memcpy(seed->choice_cardinalities, pending->sql.choice_cardinalities,
           (size_t)pending->sql.choice_count *
               sizeof(*seed->choice_cardinalities));
  }
  seed->target_index = pending->target_index;
  seed->choice_count = pending->sql.choice_count;
  seed->full_path_choice_count = pending->full_path_choice_count;
  seed->next_path_ordinal = pending->cursor_next_path_ordinal + 1U;
  path.choices = seed->full_path_choices;
  path.choice_count = seed->full_path_choice_count;
  path.choice_capacity = seed->full_path_choice_capacity;
  code = qstGeneratorFullPathAdvance(&path, &pending->sql);
  seed->full_path_choices = path.choices;
  seed->full_path_choice_count = path.choice_count;
  seed->full_path_choice_capacity = path.choice_capacity;
  if (code == QST_E_QUEUE_EMPTY) {
    seed->exhausted = 1;
    return code;
  }
  if (code != QST_OK) {
    qst_generator_full_path_seed_destroy(seed);
    return code;
  }
  return QST_OK;
}

/* Rebuilds process-local choices from the shared cursor after a generator restart. */
static EQstCode qstGeneratorFullPendingRestorePath(
    const SQstGeneratorCatalog *catalog, const SQstGeneratorCursor *cursor,
    uint32_t target_index, uint32_t depth,
    const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratorFullPending *pending) {
  SQstGeneratorFullPath path = {0};
  SQstGeneratedSql replay = {0};
  EQstCode code = QST_OK;

  if (NULL == catalog || NULL == cursor || NULL == pending ||
      target_index >= QST_GENERATOR_TARGET_COUNT ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (cursor->exhausted != 0) {
    return QST_E_QUEUE_EMPTY;
  }
  if (pending->full_path_restore_count == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  ++pending->full_path_restore_count;
  for (uint64_t ordinal = 0; ordinal < cursor->next_path_ordinal; ++ordinal) {
    code = qstGeneratorBuildFullPath(catalog, &path, depth, runtime, &replay);
    if (code == QST_OK) {
      code = qstGeneratorFullPathAdvance(&path, &replay);
    }
    qst_generator_generated_sql_destroy(&replay);
    if (code != QST_OK) {
      qstGeneratorFullPathDestroy(&path);
      return code;
    }
  }
  qstGeneratorFullPendingPathDestroy(pending);
  qstGeneratorFullPendingSavePath(pending, &path);
  pending->full_path_next_ordinal = cursor->next_path_ordinal;
  pending->full_path_target_index = target_index;
  pending->full_path_ready = 1;
  return QST_OK;
}

EQstCode qst_generator_full_peek_with_runtime(
    const SQstGeneratorCatalog *catalog, const SQstGeneratorCursor *cursor,
    uint32_t depth, const SQstGeneratorRuntimeContext *runtime,
    SQstGeneratedSql *out_sql) {
  SQstGeneratorFullPath path = {0};
  SQstGeneratedSql replay = {0};
  EQstCode code = QST_OK;

  if (NULL == cursor || NULL == out_sql ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_catalog_validate(catalog);
  if (code != QST_OK) {
    return code;
  }
  if (cursor->exhausted != 0) {
    return QST_E_QUEUE_EMPTY;
  }
  qst_generator_generated_sql_destroy(out_sql);
  for (uint64_t ordinal = 0; ordinal < cursor->next_path_ordinal; ++ordinal) {
    code = qstGeneratorBuildFullPath(catalog, &path, depth, runtime, &replay);
    if (code == QST_OK) {
      code = qstGeneratorFullPathAdvance(&path, &replay);
    }
    qst_generator_generated_sql_destroy(&replay);
    if (code != QST_OK) {
      qstGeneratorFullPathDestroy(&path);
      return code;
    }
  }
  code = qstGeneratorBuildFullPath(catalog, &path, depth, runtime, out_sql);
  qstGeneratorFullPathDestroy(&path);
  return code;
}

EQstCode qst_generator_full_peek(const SQstGeneratorCatalog *catalog,
                                 const SQstGeneratorCursor *cursor,
                                 uint32_t depth,
                                 SQstGeneratedSql *out_sql) {
  return qst_generator_full_peek_with_runtime(catalog, cursor, depth, NULL,
                                               out_sql);
}

EQstCode qst_generator_full_commit(SQstGeneratorCursor *cursor,
                                   const SQstGeneratedSql *sql) {
  if (NULL == cursor || NULL == sql || cursor->exhausted != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (cursor->next_path_ordinal == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  ++cursor->next_path_ordinal;
  return QST_OK;
}

EQstCode qst_generator_publish(SQstShmMapping *mapping, EQstRole role,
                               uint32_t generation,
                               const SQstGeneratorPublishInput *input,
                               const SQstGeneratedSql *sql,
                               uint64_t queue_deadline_mono_ns,
                               uint64_t *out_gid) {
  SQstGeneratePayload payload = {0};
  SQstMessage message = {0};
  SQstRef sql_ref = {0};
  SQstRef generation_record_ref = {0};
  SQstDigest256 generation_record_sha256 = {0};
  uint64_t gid = 0;
  uint64_t dispatch_epoch = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->header || NULL == input ||
      NULL == sql || NULL == out_gid || role != QST_ROLE_GENERATOR ||
      generation == 0 || sql->sql_length == 0 ||
      sql->sql_length >= QST_GENERATOR_SQL_BYTES ||
      input->target < QST_TARGET_TD || input->target > QST_TARGET_INFLUX ||
      input->mode < QST_MODE_FULL || input->mode > QST_MODE_RANDOM ||
      input->focus < QST_FOCUS_COVERAGE ||
      input->focus > QST_FOCUS_CORRECTNESS ||
      input->dialect < QST_DIALECT_TDENGINE ||
      input->dialect > QST_DIALECT_INFLUXQL || input->catalog_version == 0) {
    (void)fprintf(
        stderr,
        "taosQueryTest: generator publish invalid argument mapping=%d "
        "header=%d input=%d sql=%d out_gid=%d role=%u generation=%u "
        "sql_length=%u target=%u mode=%u focus=%u dialect=%u "
        "catalog_version=%u\n",
        mapping != NULL, mapping != NULL && mapping->header != NULL,
        input != NULL, sql != NULL, out_gid != NULL, (unsigned int)role,
        generation, sql != NULL ? sql->sql_length : 0U,
        input != NULL ? input->target : 0U, input != NULL ? input->mode : 0U,
        input != NULL ? input->focus : 0U,
        input != NULL ? input->dialect : 0U,
        input != NULL ? input->catalog_version : 0U);
    return QST_E_INVALID_ARGUMENT;
  }
  *out_gid = 0;
  code = qstGeneratorCheckPublicationBlockBudget(mapping, sql);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator publish block budget failed "
                  "code=%d target=%u generation=%u sql_bytes=%u\n",
                  code, input->target, generation, sql->sql_length);
    return code;
  }
  code = qst_task_reserve_gid(mapping, role, generation, &gid);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator publish gid reservation failed "
                  "code=%d target=%u generation=%u\n",
                  code, input->target, generation);
    return code;
  }
  code = qst_block_alloc(mapping, gid, generation, sql->sql_length, &sql_ref);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator SQL block allocation failed "
                  "code=%d gid=%llu bytes=%u\n",
                  code, (unsigned long long)gid, sql->sql_length);
    return code;
  }
  code = qst_block_write_seal(mapping, &sql_ref, sql->sql, sql->sql_length);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator SQL block seal failed code=%d "
                  "gid=%llu offset=%llu length=%u\n",
                  code, (unsigned long long)gid,
                  (unsigned long long)sql_ref.offset, sql_ref.length);
    /* A seal failure may leave a partial sealed chain; retain it for diagnosis. */
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    return QST_E_SHM_CORRUPT;
  }
  code = qstGeneratorWriteGenerationRecord(mapping, gid, generation, input,
                                           sql, &generation_record_ref,
                                           &generation_record_sha256);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator generation record write failed "
                  "code=%d gid=%llu sql_offset=%llu sql_length=%u\n",
                  code, (unsigned long long)gid,
                  (unsigned long long)sql_ref.offset, sql_ref.length);
    if (code == QST_E_BLOCK_INVALID) {
      atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                            memory_order_release);
      return QST_E_SHM_CORRUPT;
    }
    {
      const EQstCode release_code = qst_block_release(mapping, sql_ref, gid);

      if (release_code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: generator SQL block rollback failed "
                      "code=%d gid=%llu offset=%llu length=%u\n",
                      release_code, (unsigned long long)gid,
                      (unsigned long long)sql_ref.offset, sql_ref.length);
        return QST_E_SHM_CORRUPT;
      }
    }
    return code;
  }
  payload.target = input->target;
  payload.mode = input->mode;
  payload.focus = input->focus;
  payload.dialect = input->dialect;
  payload.generation_contains_abnormal = sql->contains_abnormal;
  payload.generation_contains_unsupported_fallback =
      sql->contains_unsupported_fallback;
  payload.generation_seq = input->generation_seq;
  payload.weight_profile_version = input->weight_profile_version;
  payload.sql_sha256 = sql->sql_sha256;
  payload.dataset_digest = input->dataset_digest;
  payload.sql_ref = sql_ref;
  payload.generation_record_ref = generation_record_ref;
  payload.generation_record_sha256 = generation_record_sha256;
  /* The generation record was sealed and hashed immediately above by this
   * generator generation. Retain block ownership/CRC validation while
   * avoiding a second identical digest pass before the queue trust boundary. */
  code = qst_task_create_prepared(mapping, gid, role, generation, &payload,
                                  &dispatch_epoch);
  if (code != QST_OK) {
    const EQstCode generation_record_release_code =
        qst_block_release(mapping, generation_record_ref, gid);
    const EQstCode sql_release_code = qst_block_release(mapping, sql_ref, gid);

    (void)fprintf(stderr,
                  "taosQueryTest: generator task create failed code=%d "
                  "gid=%llu record_offset=%llu record_length=%u "
                  "record_release=%d sql_offset=%llu sql_length=%u "
                  "sql_release=%d\n",
                  code, (unsigned long long)gid,
                  (unsigned long long)generation_record_ref.offset,
                  generation_record_ref.length, generation_record_release_code,
                  (unsigned long long)sql_ref.offset, sql_ref.length,
                  sql_release_code);
    if (generation_record_release_code != QST_OK || sql_release_code != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    return code;
  }
  message.message_type = QST_MSG_GENERATE_WORK;
  message.producer_role = role;
  message.producer_generation = generation;
  message.target = input->target;
  message.run_id = mapping->header->run_id;
  message.gid = gid;
  message.dispatch_epoch = dispatch_epoch;
  message.transition_seq = 1;
  message.body.generate = payload;
  code = qst_queue_publish(mapping, QST_QUEUE_GENERATE, &message,
                           queue_deadline_mono_ns);
  if (code != QST_OK) {
    EQstCode cleanup_code =
        qst_task_abort_generated(mapping, gid, role, generation);
    const EQstCode generation_record_release_code =
        qst_block_release(mapping, generation_record_ref, gid);
    const EQstCode sql_release_code = qst_block_release(mapping, sql_ref, gid);

    if (code != QST_E_QUEUE_FULL) {
      (void)fprintf(stderr,
                    "taosQueryTest: generator queue publish failed code=%d "
                    "gid=%llu task_abort=%d record_offset=%llu "
                    "record_release=%d sql_offset=%llu sql_release=%d\n",
                    code, (unsigned long long)gid, cleanup_code,
                    (unsigned long long)generation_record_ref.offset,
                    generation_record_release_code,
                    (unsigned long long)sql_ref.offset, sql_release_code);
    }
    if (cleanup_code != QST_OK || generation_record_release_code != QST_OK ||
        sql_release_code != QST_OK) {
      return QST_E_SHM_CORRUPT;
    }
    return code;
  }
  atomic_fetch_add_explicit(&mapping->header->generated_task_count, 1,
                            memory_order_release);
  *out_gid = gid;
  return QST_OK;
}

EQstCode qst_generator_state_get(SQstShmMapping *mapping,
                                 SQstGenerationState **out_state) {
  const SQstRegion *region = NULL;
  SQstGenerationState *state = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == out_state) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->generation_state_region;
  if (region->element_bytes != sizeof(SQstGenerationState) ||
      region->element_count != 1 || region->region_bytes != sizeof(*state) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  state = (SQstGenerationState *)((uint8_t *)mapping->base +
                                  region->region_offset);
  if (state->format_version != QST_GENERATION_STATE_FORMAT_VERSION ||
      state->catalog_version == 0 ||
      state->catalog_version != mapping->header->dimension_catalog_version ||
      memcmp(state->catalog_digest.bytes,
             mapping->header->dimension_catalog_digest.bytes,
             sizeof(state->catalog_digest.bytes)) != 0) {
    return QST_E_SHM_CORRUPT;
  }
  *out_state = state;
  return QST_OK;
}

EQstCode qst_generator_state_set_random_builder_count(
    SQstShmMapping *mapping, uint32_t main_generation, uint32_t builder_count) {
  SQstGenerationState *state = NULL;
  EQstCode code = QST_OK;

  if (builder_count == 0 || builder_count > QST_GENERATOR_MAX_RANDOM_BUILDERS ||
      (builder_count & (builder_count - 1U)) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL == mapping || main_generation == 0 ||
      atomic_load_explicit(&mapping->header->main_generation,
                           memory_order_acquire) != main_generation) {
    return QST_E_STALE_OWNER;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK) {
    return code;
  }
  if (state->random_builder_count != 0 &&
      state->random_builder_count != builder_count) {
    return QST_E_STALE_OWNER;
  }
  state->random_builder_count = builder_count;
  return QST_OK;
}

EQstCode qst_generator_state_initialize(SQstShmMapping *mapping,
                                        const SQstGeneratorCatalog *catalog,
                                        uint32_t mode, uint32_t focus,
                                        uint32_t target_selection_bits,
                                        uint64_t random_seed,
                                        uint64_t statement_limit,
                                        uint64_t duration_limit_minutes) {
  SQstGenerationState *state = NULL;
  uint32_t done_mask = 0;
  uint64_t pending_quota_bytes = 0;
  uint64_t log_rotation_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == catalog ||
      (mode != QST_MODE_FULL && mode != QST_MODE_RANDOM) ||
      focus < QST_FOCUS_COVERAGE || focus > QST_FOCUS_CORRECTNESS ||
      target_selection_bits == 0 ||
      (target_selection_bits & ~UINT32_C(0x0f)) != 0 ||
      (mode == QST_MODE_FULL && statement_limit != 0) ||
      (mode == QST_MODE_RANDOM && statement_limit != 0 &&
       duration_limit_minutes != 0) ||
      duration_limit_minutes >
          UINT64_MAX / (UINT64_C(60) * UINT64_C(1000000000))) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_catalog_validate(catalog);
  if (code != QST_OK) {
    return code;
  }
  if (mapping->header->generation_state_region.element_bytes !=
          sizeof(SQstGenerationState) ||
      mapping->header->generation_state_region.element_count != 1 ||
      mapping->header->generation_state_region.region_bytes !=
          sizeof(SQstGenerationState) ||
      mapping->header->generation_state_region.region_offset > mapping->bytes ||
      mapping->header->generation_state_region.region_bytes >
          mapping->bytes -
              mapping->header->generation_state_region.region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  state = (SQstGenerationState *)((uint8_t *)mapping->base +
      mapping->header->generation_state_region.region_offset);
  if (state->format_version != 0) {
    return QST_E_STALE_OWNER;
  }
  pending_quota_bytes = state->pending_quota_bytes;
  log_rotation_bytes = state->log_rotation_bytes;
  code = qst_shm_set_dimension_catalog(mapping, catalog->version, catalog->digest);
  if (code != QST_OK) {
    return code;
  }
  memset(state, 0, sizeof(*state));
  state->catalog_version = catalog->version;
  state->catalog_digest = catalog->digest;
  state->mode = mode;
  state->focus = focus;
  state->target_selection_bits = target_selection_bits;
  state->random_seed = random_seed;
  state->statement_limit = statement_limit;
  state->duration_limit_minutes = duration_limit_minutes;
  state->pending_quota_bytes = pending_quota_bytes;
  state->log_rotation_bytes = log_rotation_bytes;
  for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
    if ((target_selection_bits & (UINT32_C(1) << index)) == 0) {
      done_mask |= UINT32_C(1) << index;
      state->full_cursor[index].exhausted = 1;
    }
  }
  atomic_init(&state->publish_seq, 0);
  atomic_init(&state->random_generation_seq, 1);
  atomic_init(&state->random_activated_count, 0);
  atomic_init(&state->random_duration_deadline_mono_ns, 0);
  atomic_init(&state->full_coverage_round_count, 0);
  for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
    atomic_init(&state->full_coverage_seed_next[index], 0);
  }
  atomic_init(&state->pending_used_bytes, UINT64_MAX);
  atomic_init(&state->full_target_index, 0);
  atomic_init(&state->full_target_done_mask, done_mask);
  atomic_init(&state->dataset_state, QST_GENERATION_DATASET_UNPUBLISHED);
  atomic_thread_fence(memory_order_release);
  state->format_version = QST_GENERATION_STATE_FORMAT_VERSION;
  return QST_OK;
}

EQstCode qst_generator_random_can_publish(SQstShmMapping *mapping,
                                          uint64_t now_mono_ns,
                                          int32_t *out_can_publish,
                                          EQstStopReason *out_stop_reason,
                                          EQstRandomPublishGate *out_gate) {
  SQstGenerationState *state = NULL;
  uint64_t next_generation_seq = 0;
  uint64_t activated_count = 0;
  uint64_t published_count = 0;
  uint64_t publish_window = 0;
  uint64_t duration_deadline = 0;
  int32_t task_has_capacity = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || now_mono_ns == 0 || NULL == out_can_publish ||
      NULL == out_stop_reason) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_can_publish = 0;
  *out_stop_reason = QST_STOP_NONE;
  if (NULL != out_gate) {
    *out_gate = QST_RANDOM_PUBLISH_GATE_READY;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_RANDOM) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  next_generation_seq = atomic_load_explicit(&state->random_generation_seq,
                                              memory_order_acquire);
  activated_count = atomic_load_explicit(&state->random_activated_count,
                                         memory_order_acquire);
  publish_window =
      mapping->header->queue_region[QST_QUEUE_GENERATE - 1U].element_count;
  duration_deadline = atomic_load_explicit(
      &state->random_duration_deadline_mono_ns, memory_order_acquire);
  if (next_generation_seq == 0 || publish_window == 0) {
    return QST_E_SHM_CORRUPT;
  }
  published_count = next_generation_seq - 1U;
  if (activated_count > published_count ||
      (state->statement_limit != 0 &&
       published_count > state->statement_limit)) {
    return QST_E_SHM_CORRUPT;
  }
  if (next_generation_seq == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  if (state->statement_limit != 0 &&
      published_count == state->statement_limit) {
    *out_stop_reason = QST_STOP_STATEMENT_LIMIT;
    if (NULL != out_gate) {
      *out_gate = QST_RANDOM_PUBLISH_GATE_STOP;
    }
    return QST_OK;
  }
  if (duration_deadline != 0 && now_mono_ns >= duration_deadline) {
    *out_stop_reason = QST_STOP_DURATION_LIMIT;
    if (NULL != out_gate) {
      *out_gate = QST_RANDOM_PUBLISH_GATE_STOP;
    }
    return QST_OK;
  }
  code = qst_task_has_capacity(mapping, &task_has_capacity);
  if (code != QST_OK) {
    return code;
  }
  if (task_has_capacity == 0) {
    if (NULL != out_gate) {
      *out_gate = QST_RANDOM_PUBLISH_GATE_TASK_LEDGER_EMPTY;
    }
    return QST_OK;
  }
  /* Keep at most one generate queue worth of unpublished-to-dispatch work.
   * Activation is ordered by generation sequence, so this preserves the
   * deterministic stream while allowing every available pair to start. */
  if (published_count - activated_count < publish_window) {
    *out_can_publish = 1;
  } else if (NULL != out_gate) {
    *out_gate = QST_RANDOM_PUBLISH_GATE_ACTIVATION_WINDOW_FULL;
  }
  return QST_OK;
}

EQstCode qst_generator_full_duration_expired(uint64_t deadline_mono_ns,
                                             uint64_t now_mono_ns,
                                             int32_t *out_expired) {
  if (NULL == out_expired ||
      (deadline_mono_ns != 0 && now_mono_ns == 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_expired = deadline_mono_ns != 0 && now_mono_ns >= deadline_mono_ns;
  return QST_OK;
}

/* Resolves the executable table reference for a coverage seed. External
 * tables retain their provider source in the immutable parent chain, while
 * local table descriptors already carry the complete TDengine table name. */
static EQstCode qstGeneratorBuildFullCoverageSeedTableName(
    const SQstShmMapping *mapping, const SQstDatasetInfo *dataset,
    const SQstObjectDescriptor *table, char *out_name, uint64_t out_bytes) {
  SQstObjectDescriptor namespace_object = {0};
  SQstObjectDescriptor source_object = {0};
  char table_name[QST_GENERATOR_SQL_BYTES] = {0};
  char source_name[QST_GENERATOR_SQL_BYTES] = {0};
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == dataset || NULL == table || NULL == out_name ||
      out_bytes == 0 || table->object_kind != QST_OBJECT_TABLE_OR_MEASUREMENT ||
      table->target < QST_TARGET_TD || table->target > QST_TARGET_INFLUX) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_dataset_copy_object_name(mapping, table, table_name,
                                      sizeof(table_name));
  if (code != QST_OK || table_name[0] == '\0') {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  if (table->target == QST_TARGET_TD) {
    if (strlen(table_name) >= out_bytes) {
      return QST_E_SQL_LIMIT;
    }
    memcpy(out_name, table_name, strlen(table_name) + 1U);
    return QST_OK;
  }
  if (table->parent_object_id == 0 ||
      qst_dataset_get_object_by_id(mapping, dataset, table->parent_object_id,
                                   &namespace_object) != QST_OK ||
      namespace_object.target != table->target ||
      namespace_object.object_kind != QST_OBJECT_DATABASE_OR_SCHEMA ||
      namespace_object.parent_object_id == 0 ||
      qst_dataset_get_object_by_id(mapping, dataset,
                                   namespace_object.parent_object_id,
                                   &source_object) != QST_OK ||
      source_object.target != table->target ||
      source_object.object_kind != QST_OBJECT_SOURCE ||
      source_object.parent_object_id != 0) {
    return QST_E_SHM_CORRUPT;
  }
  code = qst_dataset_copy_object_name(mapping, &source_object, source_name,
                                      sizeof(source_name));
  if (code != QST_OK || source_name[0] == '\0') {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  written = snprintf(out_name, (size_t)out_bytes, "%s.%s", source_name,
                     table_name);
  return written >= 0 && (uint64_t)written < out_bytes ? QST_OK : QST_E_SQL_LIMIT;
}

/* Builds a small, valid runtime-backed query without constraining the catalog
 * DFS. The dataset directory is immutable after publication, so the selected
 * ordinal is stable across generator recovery. */
static EQstCode qstGeneratorBuildFullCoverageSeedSql(
    SQstShmMapping *mapping, const SQstGenerationState *state,
    uint32_t target_index, uint32_t seed_index, SQstGeneratedSql *out_sql) {
  SQstDatasetInfo dataset = {0};
  SQstObjectDescriptor object = {0};
  uint32_t object_count = 0;
  uint32_t table_count = 0;
  uint32_t selected_table = 0;
  int32_t found_table = 0;
  char table_name[QST_GENERATOR_SQL_BYTES] = {0};
  SQstSha256 digest = {0};
  int32_t written = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == state || NULL == out_sql ||
      target_index >= QST_GENERATOR_TARGET_COUNT) {
    return QST_E_INVALID_ARGUMENT;
  }
  dataset.object_directory_ref = state->object_directory_ref;
  dataset.fixture_manifest_ref = state->fixture_manifest_ref;
  code = qst_dataset_get_object_count(mapping, &dataset, &object_count);
  if (code != QST_OK || object_count == 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  for (uint32_t object_index = 0; object_index < object_count; ++object_index) {
    code = qst_dataset_get_object_at(mapping, &dataset, object_index, &object);
    if (code != QST_OK) {
      return code;
    }
    if (object.target == target_index + 1U &&
        object.object_kind == QST_OBJECT_TABLE_OR_MEASUREMENT) {
      if (table_count == seed_index) {
        selected_table = object_index;
        found_table = 1;
        break;
      }
      ++table_count;
    }
  }
  if (found_table == 0) {
    return QST_E_SHM_CORRUPT;
  }
  code = qst_dataset_get_object_at(mapping, &dataset, selected_table, &object);
  if (code != QST_OK) {
    return code;
  }
  code = qstGeneratorBuildFullCoverageSeedTableName(mapping, &dataset, &object,
                                                     table_name,
                                                     sizeof(table_name));
  if (code != QST_OK || table_name[0] == '\0') {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  qst_generator_generated_sql_destroy(out_sql);
  written = snprintf(out_sql->sql, sizeof(out_sql->sql),
                     "SELECT COUNT(*) AS qst_full_coverage_count FROM %s "
                     "WHERE 1 = 1;",
                     table_name);
  if (written < 0 || (uint64_t)written >= sizeof(out_sql->sql)) {
    qst_generator_generated_sql_destroy(out_sql);
    return QST_E_SQL_LIMIT;
  }
  out_sql->sql_length = (uint32_t)written;
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, (const uint8_t *)out_sql->sql,
                    out_sql->sql_length);
  qst_sha256_final(&digest, out_sql->sql_sha256.bytes);
  return QST_OK;
}

EQstCode qst_generator_full_publish_coverage_seed(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint32_t target_index, uint64_t weight_profile_version,
    uint64_t queue_deadline_mono_ns, uint64_t *out_gid) {
  SQstGenerationState *state = NULL;
  SQstGeneratorPublishInput input = {0};
  SQstGeneratedSql sql = {0};
  uint64_t publish_seq = 0;
  uint64_t published_gid = 0;
  uint32_t seed_index = 0;
  int32_t task_has_capacity = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == catalog || NULL == out_gid ||
      role != QST_ROLE_GENERATOR || generation == 0 ||
      target_index >= QST_GENERATOR_TARGET_COUNT) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_gid = 0;
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_FULL ||
      (state->target_selection_bits & (UINT32_C(1) << target_index)) == 0 ||
      state->catalog_version != catalog->version ||
      memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  seed_index = atomic_load_explicit(&state->full_coverage_seed_next[target_index],
                                    memory_order_acquire);
  if (seed_index >= QST_GENERATOR_FULL_COVERAGE_SEED_COUNT) {
    return QST_E_QUEUE_EMPTY;
  }
  code = qst_task_has_capacity(mapping, &task_has_capacity);
  if (code != QST_OK || task_has_capacity == 0) {
    return code != QST_OK ? code : QST_E_QUEUE_FULL;
  }
  code = qstGeneratorBuildFullCoverageSeedSql(mapping, state, target_index,
                                               seed_index, &sql);
  if (code != QST_OK) {
    return code;
  }
  publish_seq = atomic_load_explicit(&state->publish_seq, memory_order_acquire);
  if (publish_seq == UINT64_MAX) {
    qst_generator_generated_sql_destroy(&sql);
    return QST_E_RESOURCE;
  }
  input.target = target_index + 1U;
  input.mode = QST_MODE_FULL;
  input.focus = state->focus;
  input.dialect = target_index + 1U;
  input.generation_seq = publish_seq + 1U;
  input.weight_profile_version = weight_profile_version;
  input.catalog_version = catalog->version;
  input.catalog_digest = catalog->digest;
  input.dataset_digest = dataset_digest;
  code = qst_generator_publish(mapping, role, generation, &input, &sql,
                               queue_deadline_mono_ns, &published_gid);
  qst_generator_generated_sql_destroy(&sql);
  if (code != QST_OK) {
    return code;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &state->publish_seq, &publish_seq, input.generation_seq,
          memory_order_acq_rel, memory_order_acquire)) {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    return QST_E_SHM_CORRUPT;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &state->full_coverage_seed_next[target_index], &seed_index,
          seed_index + 1U, memory_order_acq_rel, memory_order_acquire)) {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    return QST_E_SHM_CORRUPT;
  }
  *out_gid = published_gid;
  return QST_OK;
}

EQstCode qst_generator_random_activate(SQstShmMapping *mapping,
                                       uint64_t generation_seq,
                                       uint64_t now_mono_ns) {
  SQstGenerationState *state = NULL;
  uint64_t expected_count = 0;
  uint64_t duration_ns = 0;
  uint64_t deadline = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || generation_seq == 0 || now_mono_ns == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_RANDOM) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  /* Pair supervisors claim queue entries concurrently, so activation order is
   * not guaranteed to match deterministic generation order. This is a
   * high-water mark for backpressure, not a proof that every prior sequence
   * has already started execution. */
  for (;;) {
    expected_count = atomic_load_explicit(&state->random_activated_count,
                                          memory_order_acquire);
    if (expected_count >= generation_seq) {
      break;
    }
    if (atomic_compare_exchange_weak_explicit(
            &state->random_activated_count, &expected_count, generation_seq,
            memory_order_acq_rel, memory_order_acquire)) {
      break;
    }
  }
  if (generation_seq != 1U || state->duration_limit_minutes == 0) {
    return QST_OK;
  }
  duration_ns = state->duration_limit_minutes * UINT64_C(60) *
                UINT64_C(1000000000);
  if (duration_ns == 0 || now_mono_ns > UINT64_MAX - duration_ns) {
    return QST_E_RESOURCE;
  }
  deadline = now_mono_ns + duration_ns;
  expected_count = 0;
  if (!atomic_compare_exchange_strong_explicit(
          &state->random_duration_deadline_mono_ns, &expected_count, deadline,
          memory_order_release, memory_order_acquire) &&
      expected_count != deadline) {
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

EQstCode qst_generator_full_complete_round(SQstShmMapping *mapping,
                                           int32_t restart) {
  SQstGenerationState *state = NULL;
  uint32_t selected_bits = 0;
  uint32_t completed_mask = 0;
  uint64_t completed_round_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || (restart != 0 && restart != 1)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_FULL) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  selected_bits = state->target_selection_bits;
  completed_mask = atomic_load_explicit(&state->full_target_done_mask,
                                        memory_order_acquire);
  if (selected_bits == 0 || (selected_bits & ~UINT32_C(0x0f)) != 0 ||
      (completed_mask & selected_bits) != selected_bits) {
    return QST_E_SHM_CORRUPT;
  }
  for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
    if ((selected_bits & (UINT32_C(1) << index)) != 0 &&
        state->full_cursor[index].exhausted == 0) {
      return QST_E_SHM_CORRUPT;
    }
  }
  completed_round_count = atomic_load_explicit(&state->full_coverage_round_count,
                                                memory_order_acquire);
  if (completed_round_count == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  if (restart != 0) {
    for (uint32_t index = 0; index < QST_GENERATOR_TARGET_COUNT; ++index) {
      if ((selected_bits & (UINT32_C(1) << index)) != 0) {
        memset(&state->full_cursor[index], 0, sizeof(state->full_cursor[index]));
      }
    }
    atomic_store_explicit(&state->full_target_index, 0, memory_order_release);
    atomic_store_explicit(&state->full_target_done_mask,
                          UINT32_C(0x0f) & ~selected_bits,
                          memory_order_release);
  }
  atomic_store_explicit(&state->full_coverage_round_count,
                        completed_round_count + 1U, memory_order_release);
  return QST_OK;
}

EQstCode qst_generator_state_publish_dataset(
    SQstShmMapping *mapping, uint32_t main_generation,
    SQstDigest256 dataset_digest, SQstDigest256 fixture_manifest_digest,
    SQstRef object_directory_ref, SQstRef fixture_manifest_ref) {
  SQstGenerationState *state = NULL;
  uint32_t current_generation = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || main_generation == 0 ||
      qstGeneratorDigestIsZero(&dataset_digest) ||
      qstGeneratorDigestIsZero(&fixture_manifest_digest) ||
      qstRefIsEmpty(object_directory_ref) || qstRefIsEmpty(fixture_manifest_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_process_find_generation(mapping, QST_ROLE_MAIN,
                                     (int32_t)getpid(), &current_generation);
  if (code != QST_OK || current_generation != main_generation) {
    return code != QST_OK ? code : QST_E_STALE_OWNER;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK) {
    return code;
  }
  if (atomic_load_explicit(&state->dataset_state, memory_order_acquire) !=
      QST_GENERATION_DATASET_UNPUBLISHED) {
    return QST_E_STALE_OWNER;
  }
  if (qst_block_validate(mapping, object_directory_ref, 0) != QST_OK ||
      qst_block_validate(mapping, fixture_manifest_ref, 0) != QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  state->dataset_digest = dataset_digest;
  state->fixture_manifest_digest = fixture_manifest_digest;
  state->object_directory_ref = object_directory_ref;
  state->fixture_manifest_ref = fixture_manifest_ref;
  qstGeneratorDatasetSnapshotDigest(
      &state->dataset_digest, &state->fixture_manifest_digest,
      &state->object_directory_ref, &state->fixture_manifest_ref,
      &state->dataset_snapshot_sha256);
  atomic_store_explicit(&state->dataset_state,
                        QST_GENERATION_DATASET_PUBLISHED,
                        memory_order_release);
  return QST_OK;
}

EQstCode qst_generator_state_get_dataset(
    SQstShmMapping *mapping, SQstDigest256 *out_dataset_digest,
    SQstDigest256 *out_fixture_manifest_digest,
    SQstRef *out_object_directory_ref, SQstRef *out_fixture_manifest_ref) {
  SQstGenerationState *state = NULL;
  SQstDigest256 calculated_snapshot_digest = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_dataset_digest ||
      NULL == out_fixture_manifest_digest || NULL == out_object_directory_ref ||
      NULL == out_fixture_manifest_ref) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code == QST_OK) {
    qstGeneratorDatasetSnapshotDigest(
        &state->dataset_digest, &state->fixture_manifest_digest,
        &state->object_directory_ref, &state->fixture_manifest_ref,
        &calculated_snapshot_digest);
  }
  if (code != QST_OK ||
      atomic_load_explicit(&state->dataset_state, memory_order_acquire) !=
          QST_GENERATION_DATASET_PUBLISHED ||
      qstGeneratorDigestIsZero(&state->dataset_digest) ||
      qstGeneratorDigestIsZero(&state->fixture_manifest_digest) ||
      qstRefIsEmpty(state->object_directory_ref) ||
      qstRefIsEmpty(state->fixture_manifest_ref) ||
      memcmp(calculated_snapshot_digest.bytes,
             state->dataset_snapshot_sha256.bytes,
             sizeof(calculated_snapshot_digest.bytes)) != 0 ||
      qst_block_validate(mapping, state->object_directory_ref, 0) != QST_OK ||
      qst_block_validate(mapping, state->fixture_manifest_ref, 0) != QST_OK) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  *out_dataset_digest = state->dataset_digest;
  *out_fixture_manifest_digest = state->fixture_manifest_digest;
  *out_object_directory_ref = state->object_directory_ref;
  *out_fixture_manifest_ref = state->fixture_manifest_ref;
  return QST_OK;
}

static int32_t qstGeneratorCatalogHasRuntimeObject(
    const SQstGeneratorCatalog *catalog) {
  if (NULL == catalog || NULL == catalog->fragments) {
    return 0;
  }
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];

    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      if (fragment->values[value_index].runtime_object_kind !=
          QST_RUNTIME_OBJECT_NONE) {
        return 1;
      }
    }
  }
  return 0;
}

static EQstCode qstGeneratorRuntimeContextFromState(
    SQstShmMapping *mapping, const SQstGenerationState *state, uint32_t target,
    uint32_t dialect, SQstGeneratorRuntimeContext *out_runtime) {
  SQstDigest256 dataset_digest = {0};
  SQstDigest256 fixture_manifest_digest = {0};

  if (NULL == mapping || NULL == state || NULL == out_runtime ||
      target < QST_TARGET_TD || target > QST_TARGET_INFLUX ||
      dialect < QST_DIALECT_TDENGINE || dialect > QST_DIALECT_INFLUXQL) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_runtime, 0, sizeof(*out_runtime));
  if (qst_generator_state_get_dataset(
          mapping, &dataset_digest, &fixture_manifest_digest,
          &out_runtime->object_directory_ref,
          &out_runtime->fixture_manifest_ref) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  out_runtime->mapping = mapping;
  out_runtime->target = target;
  out_runtime->dialect = dialect;
  return QST_OK;
}

/* Retains one verified immutable object-directory copy for a full generator run. */
static EQstCode qstGeneratorFullPendingInitializeRuntime(
    SQstGeneratorFullPending *pending, SQstGeneratorRuntimeContext *runtime) {
  SQstDatasetInfo dataset = {0};
  uint32_t object_count = 0;
  EQstCode code = QST_OK;

  if (NULL == pending || NULL == runtime || NULL == runtime->mapping ||
      qstRefIsEmpty(runtime->object_directory_ref) ||
      qstRefIsEmpty(runtime->fixture_manifest_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != pending->runtime_objects &&
      pending->runtime_mapping == runtime->mapping &&
      memcmp(&pending->runtime_object_directory_ref,
             &runtime->object_directory_ref,
             sizeof(runtime->object_directory_ref)) == 0 &&
      memcmp(&pending->runtime_fixture_manifest_ref,
             &runtime->fixture_manifest_ref,
             sizeof(runtime->fixture_manifest_ref)) == 0) {
    if (pending->runtime_object_count == 0) {
      return QST_E_SHM_CORRUPT;
    }
    runtime->objects = pending->runtime_objects;
    runtime->object_count = pending->runtime_object_count;
    return QST_OK;
  }
  free(pending->runtime_objects);
  pending->runtime_objects = NULL;
  pending->runtime_object_count = 0;
  pending->runtime_mapping = NULL;
  memset(&pending->runtime_object_directory_ref, 0,
         sizeof(pending->runtime_object_directory_ref));
  memset(&pending->runtime_fixture_manifest_ref, 0,
         sizeof(pending->runtime_fixture_manifest_ref));
  dataset.object_directory_ref = runtime->object_directory_ref;
  dataset.fixture_manifest_ref = runtime->fixture_manifest_ref;
  code = qst_dataset_get_object_count(runtime->mapping, &dataset, &object_count);
  if (code != QST_OK || object_count == 0 ||
      (uint64_t)object_count >
          SIZE_MAX / sizeof(SQstObjectDescriptor)) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  pending->runtime_objects = calloc(object_count, sizeof(SQstObjectDescriptor));
  if (NULL == pending->runtime_objects) {
    return QST_E_RESOURCE;
  }
  code = qst_dataset_copy_object_directory(
      runtime->mapping, &dataset,
      (SQstObjectDescriptor *)pending->runtime_objects, object_count,
      &pending->runtime_object_count);
  if (code != QST_OK || pending->runtime_object_count != object_count) {
    free(pending->runtime_objects);
    pending->runtime_objects = NULL;
    pending->runtime_object_count = 0;
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  pending->runtime_mapping = runtime->mapping;
  pending->runtime_object_directory_ref = runtime->object_directory_ref;
  pending->runtime_fixture_manifest_ref = runtime->fixture_manifest_ref;
  runtime->objects = pending->runtime_objects;
  runtime->object_count = pending->runtime_object_count;
  return QST_OK;
}

/* Initializes the one target-private FULL build cache. It owns no shared
 * state: the caller remains the only writer of the DFS cursor and task queue. */
static EQstCode qstGeneratorFullPendingInitializeBuildRuntime(
    SQstGeneratorFullPending *pending, const SQstGeneratorCatalog *catalog,
    const SQstGeneratorRuntimeContext *runtime) {
  SQstGeneratorRandomRuntime *cached = NULL;
  EQstCode code = QST_OK;

  if (NULL == pending || NULL == catalog) {
    return QST_E_INVALID_ARGUMENT;
  }
  cached = (SQstGeneratorRandomRuntime *)pending->build_runtime;
  if (NULL == cached) {
    cached = calloc(1, sizeof(*cached));
    if (NULL == cached) {
      return QST_E_RESOURCE;
    }
    pending->build_runtime = cached;
  }
  code = qst_generator_random_runtime_prepare(cached, catalog);
  if (code != QST_OK) {
    return code;
  }
  if (runtime == NULL) {
    return QST_OK;
  }
  /* The pending object directory owns the object array for its whole lifetime;
   * this cache owns only the derived index. */
  if (cached->runtime_object_index != NULL) {
    return QST_OK;
  }
  code = qstGeneratorRuntimeObjectIndexCreate(
      (const SQstObjectDescriptor *)runtime->objects, runtime->object_count,
      (SQstGeneratorRuntimeObjectIndex **)&cached->runtime_object_index);
  if (code != QST_OK) {
    return code;
  }
  code = qstGeneratorRuntimeObjectIndexPrepareNames(
      (SQstGeneratorRuntimeObjectIndex *)cached->runtime_object_index,
      runtime->mapping, (const SQstObjectDescriptor *)runtime->objects);
  if (code != QST_OK) {
    qstGeneratorRuntimeObjectIndexDestroy(
        (SQstGeneratorRuntimeObjectIndex *)cached->runtime_object_index);
    cached->runtime_object_index = NULL;
  }
  return code;
}

static int32_t qstGeneratorRandomRuntimeMatchesSnapshot(
    const SQstGeneratorRandomRuntime *cached, const SQstShmMapping *mapping,
    const SQstGenerationState *state, const SQstGeneratorCatalog *catalog) {
  if (NULL == cached || NULL == mapping || NULL == state || NULL == catalog ||
      cached->validated_catalog != catalog ||
      memcmp(cached->validated_catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0 ||
      cached->runtime_mapping != mapping || NULL == cached->runtime_objects ||
      cached->runtime_object_count == 0 || NULL == cached->runtime_object_index ||
      atomic_load_explicit(&state->dataset_state, memory_order_acquire) !=
          QST_GENERATION_DATASET_PUBLISHED ||
      memcmp(cached->runtime_dataset_digest.bytes, state->dataset_digest.bytes,
             sizeof(state->dataset_digest.bytes)) != 0 ||
      memcmp(cached->runtime_fixture_manifest_digest.bytes,
             state->fixture_manifest_digest.bytes,
             sizeof(state->fixture_manifest_digest.bytes)) != 0 ||
      memcmp(&cached->runtime_object_directory_ref,
             &state->object_directory_ref,
             sizeof(state->object_directory_ref)) != 0 ||
      memcmp(&cached->runtime_fixture_manifest_ref, &state->fixture_manifest_ref,
             sizeof(state->fixture_manifest_ref)) != 0) {
    return 0;
  }
  return 1;
}

/* Initializes or reuses the generator-process-local immutable RANDOM snapshot. */
static EQstCode qstGeneratorRandomRuntimeInitialize(
    SQstShmMapping *mapping, const SQstGenerationState *state,
    const SQstGeneratorCatalog *catalog, uint32_t target, uint32_t dialect,
    SQstGeneratorRandomRuntime *cached, SQstGeneratorRuntimeContext *runtime) {
  SQstDatasetInfo dataset = {0};
  SQstDigest256 dataset_digest = {0};
  SQstDigest256 fixture_manifest_digest = {0};
  SQstRef object_directory_ref = {0};
  SQstRef fixture_manifest_ref = {0};
  uint32_t object_count = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == state || NULL == catalog || NULL == cached ||
      NULL == runtime || target < QST_TARGET_TD || target > QST_TARGET_INFLUX ||
      dialect < QST_DIALECT_TDENGINE || dialect > QST_DIALECT_INFLUXQL) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_random_runtime_prepare(cached, catalog);
  if (code != QST_OK) {
    return code;
  }
  memset(runtime, 0, sizeof(*runtime));
  runtime->mapping = mapping;
  runtime->target = target;
  runtime->dialect = dialect;
  if (cached->catalog_runtime_values_prepared == 0) {
    return QST_E_SHM_CORRUPT;
  }
  if (cached->catalog_has_runtime_values == 0) {
    return QST_OK;
  }
  if (qstGeneratorRandomRuntimeMatchesSnapshot(cached, mapping, state,
                                                catalog)) {
    runtime->object_directory_ref = cached->runtime_object_directory_ref;
    runtime->fixture_manifest_ref = cached->runtime_fixture_manifest_ref;
    runtime->objects = cached->runtime_objects;
    runtime->object_count = cached->runtime_object_count;
    return QST_OK;
  }
  qstGeneratorRuntimeObjectIndexDestroy(
      (SQstGeneratorRuntimeObjectIndex *)cached->runtime_object_index);
  cached->runtime_object_index = NULL;
  free(cached->runtime_objects);
  cached->runtime_objects = NULL;
  cached->runtime_object_count = 0;
  cached->runtime_mapping = NULL;
  memset(&cached->runtime_dataset_digest, 0,
         sizeof(cached->runtime_dataset_digest));
  memset(&cached->runtime_fixture_manifest_digest, 0,
         sizeof(cached->runtime_fixture_manifest_digest));
  memset(&cached->runtime_object_directory_ref, 0,
         sizeof(cached->runtime_object_directory_ref));
  memset(&cached->runtime_fixture_manifest_ref, 0,
         sizeof(cached->runtime_fixture_manifest_ref));
  code = qst_generator_state_get_dataset(
      mapping, &dataset_digest, &fixture_manifest_digest, &object_directory_ref,
      &fixture_manifest_ref);
  if (code != QST_OK) {
    return code;
  }
  dataset.object_directory_ref = object_directory_ref;
  dataset.fixture_manifest_ref = fixture_manifest_ref;
  code = qst_dataset_get_object_count(mapping, &dataset, &object_count);
  if (code != QST_OK || object_count == 0 ||
      (uint64_t)object_count > SIZE_MAX / sizeof(SQstObjectDescriptor)) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  cached->runtime_objects = calloc(object_count, sizeof(SQstObjectDescriptor));
  if (NULL == cached->runtime_objects) {
    return QST_E_RESOURCE;
  }
  code = qst_dataset_copy_object_directory(
      mapping, &dataset, (SQstObjectDescriptor *)cached->runtime_objects,
      object_count, &cached->runtime_object_count);
  if (code != QST_OK || cached->runtime_object_count != object_count) {
    free(cached->runtime_objects);
    cached->runtime_objects = NULL;
    cached->runtime_object_count = 0;
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  code = qstGeneratorRuntimeObjectIndexCreate(
      (const SQstObjectDescriptor *)cached->runtime_objects,
      cached->runtime_object_count,
      (SQstGeneratorRuntimeObjectIndex **)&cached->runtime_object_index);
  if (code != QST_OK) {
    free(cached->runtime_objects);
    cached->runtime_objects = NULL;
    cached->runtime_object_count = 0;
    return code;
  }
  code = qstGeneratorRuntimeObjectIndexPrepareNames(
      (SQstGeneratorRuntimeObjectIndex *)cached->runtime_object_index, mapping,
      (const SQstObjectDescriptor *)cached->runtime_objects);
  if (code != QST_OK) {
    qstGeneratorRuntimeObjectIndexDestroy(
        (SQstGeneratorRuntimeObjectIndex *)cached->runtime_object_index);
    cached->runtime_object_index = NULL;
    free(cached->runtime_objects);
    cached->runtime_objects = NULL;
    cached->runtime_object_count = 0;
    return code;
  }
  cached->runtime_mapping = mapping;
  cached->runtime_dataset_digest = dataset_digest;
  cached->runtime_fixture_manifest_digest = fixture_manifest_digest;
  cached->runtime_object_directory_ref = object_directory_ref;
  cached->runtime_fixture_manifest_ref = fixture_manifest_ref;
  runtime->object_directory_ref = object_directory_ref;
  runtime->fixture_manifest_ref = fixture_manifest_ref;
  runtime->objects = cached->runtime_objects;
  runtime->object_count = cached->runtime_object_count;
  return QST_OK;
}

/* Returns the next selected, unfinished target in fixed cyclic order. */
static uint32_t qstGeneratorFullSelectTarget(uint32_t start_target_index,
                                             uint32_t done_mask) {
  if (start_target_index >= QST_GENERATOR_TARGET_COUNT) {
    start_target_index = 0;
  }
  for (uint32_t offset = 0; offset < QST_GENERATOR_TARGET_COUNT; ++offset) {
    const uint32_t target_index =
        (start_target_index + offset) % QST_GENERATOR_TARGET_COUNT;

    if ((done_mask & (UINT32_C(1) << target_index)) == 0) {
      return target_index;
    }
  }
  return QST_GENERATOR_TARGET_COUNT;
}

/* Moves the deterministic FULL scheduler to the target after the current one. */
static void qstGeneratorFullScheduleNextTarget(SQstGenerationState *state,
                                               uint32_t current_target_index,
                                               uint32_t done_mask) {
  uint32_t next_target_index = qstGeneratorFullSelectTarget(
      current_target_index + 1U, done_mask);

  if (next_target_index == QST_GENERATOR_TARGET_COUNT) {
    next_target_index = 0;
  }
  atomic_store_explicit(&state->full_target_index, next_target_index,
                        memory_order_release);
}

/* Marks one target complete only after its last path has been published. */
static void qstGeneratorFullMarkTargetDone(SQstGenerationState *state,
                                           uint32_t target_index) {
  uint32_t done_mask = 0;

  state->full_cursor[target_index].exhausted = 1;
  done_mask = atomic_fetch_or_explicit(&state->full_target_done_mask,
                                       UINT32_C(1) << target_index,
                                       memory_order_acq_rel) |
              (UINT32_C(1) << target_index);
  qstGeneratorFullScheduleNextTarget(state, target_index, done_mask);
}

/* Prepares exactly the shared cursor's next path for one target. This routine
 * has no shared-state side effects, so a target-private builder may run it in
 * parallel with other targets while a single publisher commits in DFS order. */
EQstCode qst_generator_full_prepare_pending_for_target(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint32_t target_index, SQstGeneratorFullPending *pending) {
  SQstGenerationState *state = NULL;
  SQstGeneratorRuntimeContext runtime = {0};
  const SQstGeneratorRuntimeContext *runtime_ptr = NULL;
  SQstGeneratorFullPath full_path = {0};
  SQstGeneratorRandomRuntime *build_runtime = NULL;
  uint32_t done_mask = 0;
  EQstCode code = QST_OK;

  (void)dataset_digest;
  if (NULL == mapping || NULL == catalog || NULL == pending ||
      role != QST_ROLE_GENERATOR || generation == 0 ||
      target_index >= QST_GENERATOR_TARGET_COUNT) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_FULL ||
      state->catalog_version != catalog->version ||
      memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  done_mask = atomic_load_explicit(&state->full_target_done_mask,
                                   memory_order_acquire);
  if ((done_mask & (UINT32_C(1) << target_index)) != 0 ||
      state->full_cursor[target_index].exhausted != 0) {
    return QST_E_QUEUE_EMPTY;
  }
  if (pending->validated_catalog != catalog ||
      memcmp(pending->validated_catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    code = qst_generator_catalog_validate(catalog);
    if (code != QST_OK) {
      return code;
    }
    pending->validated_catalog = catalog;
    pending->validated_catalog_digest = catalog->digest;
  }
  code = qstGeneratorFullPendingInitializeBuildRuntime(pending, catalog, NULL);
  if (code != QST_OK) {
    return code;
  }
  build_runtime = (SQstGeneratorRandomRuntime *)pending->build_runtime;
  if (NULL == build_runtime || build_runtime->catalog_runtime_values_prepared == 0) {
    return QST_E_SHM_CORRUPT;
  }
  if (build_runtime->catalog_has_runtime_values != 0) {
    if (NULL != pending->runtime_objects && pending->runtime_mapping == mapping &&
        !qstRefIsEmpty(pending->runtime_object_directory_ref) &&
        !qstRefIsEmpty(pending->runtime_fixture_manifest_ref) &&
        pending->runtime_object_count != 0) {
      runtime.mapping = mapping;
      runtime.object_directory_ref = pending->runtime_object_directory_ref;
      runtime.fixture_manifest_ref = pending->runtime_fixture_manifest_ref;
      runtime.target = target_index + 1U;
      runtime.dialect = target_index + 1U;
      runtime.objects = pending->runtime_objects;
      runtime.object_count = pending->runtime_object_count;
    } else {
      code = qstGeneratorRuntimeContextFromState(
          mapping, state, target_index + 1U, target_index + 1U, &runtime);
      if (code != QST_OK) {
        return code;
      }
      code = qstGeneratorFullPendingInitializeRuntime(pending, &runtime);
      if (code != QST_OK) {
        return code;
      }
    }
    runtime_ptr = &runtime;
    code = qstGeneratorFullPendingInitializeBuildRuntime(pending, catalog,
                                                          runtime_ptr);
    if (code != QST_OK) {
      return code;
    }
  }
  if (pending->valid != 0) {
    if (pending->target_index != target_index ||
        pending->cursor_next_path_ordinal !=
            state->full_cursor[target_index].next_path_ordinal ||
        pending->full_path_ready == 0 ||
        pending->full_path_target_index != target_index ||
        pending->full_path_next_ordinal !=
            state->full_cursor[target_index].next_path_ordinal ||
        pending->full_path_exhausted != 0) {
      return QST_E_SHM_CORRUPT;
    }
    return QST_OK;
  }
  if (pending->full_path_ready == 0 ||
      pending->full_path_target_index != target_index ||
      pending->full_path_next_ordinal !=
          state->full_cursor[target_index].next_path_ordinal) {
    code = qstGeneratorFullPendingRestorePath(
        catalog, &state->full_cursor[target_index], target_index,
        QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime_ptr, pending);
    if (code != QST_OK) {
      return code;
    }
  }
  if (pending->full_path_exhausted != 0) {
    return QST_E_QUEUE_EMPTY;
  }
  full_path = qstGeneratorFullPendingPath(pending);
  code = qstGeneratorBuildFullPathCached(
      catalog, &full_path, QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime_ptr,
      build_runtime, &pending->sql);
  if (code != QST_OK) {
    return code;
  }
  pending->cursor_next_path_ordinal =
      state->full_cursor[target_index].next_path_ordinal;
  pending->target_index = target_index;
  pending->valid = 1;
  return QST_OK;
}

EQstCode qst_generator_full_prepare_pending_from_seed(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    const SQstGeneratorFullPathSeed *seed, SQstGeneratorFullPending *pending) {
  SQstGenerationState *state = NULL;
  SQstGeneratorRuntimeContext runtime = {0};
  const SQstGeneratorRuntimeContext *runtime_ptr = NULL;
  SQstGeneratorFullPath full_path = {0};
  SQstGeneratorRandomRuntime *build_runtime = NULL;
  EQstCode code = QST_OK;

  (void)dataset_digest;
  if (NULL == mapping || NULL == catalog || NULL == seed || NULL == pending ||
      role != QST_ROLE_GENERATOR || generation == 0 || seed->exhausted != 0 ||
      seed->target_index >= QST_GENERATOR_TARGET_COUNT ||
      seed->next_path_ordinal == 0 || seed->choice_count == 0 ||
      NULL == seed->choice_cardinalities || pending->valid != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: full successor seed state lookup failed "
                  "code=%d target=%u ordinal=%llu\n",
                  code, seed->target_index,
                  (unsigned long long)seed->next_path_ordinal);
    return code;
  }
  if (state->mode != QST_MODE_FULL ||
      state->catalog_version != catalog->version ||
      memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    (void)fprintf(stderr,
                  "taosQueryTest: full successor seed state mismatch "
                  "target=%u seed_ordinal=%llu mode=%u catalog_version=%u\n",
                  seed->target_index,
                  (unsigned long long)seed->next_path_ordinal,
                  state->mode, state->catalog_version);
    return QST_E_SHM_CORRUPT;
  }
  if (pending->validated_catalog != catalog ||
      memcmp(pending->validated_catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    code = qst_generator_catalog_validate(catalog);
    if (code != QST_OK) {
      return code;
    }
    pending->validated_catalog = catalog;
    pending->validated_catalog_digest = catalog->digest;
  }
  code = qstGeneratorFullPendingInitializeBuildRuntime(pending, catalog, NULL);
  if (code != QST_OK) {
    return code;
  }
  build_runtime = (SQstGeneratorRandomRuntime *)pending->build_runtime;
  if (NULL == build_runtime || build_runtime->catalog_runtime_values_prepared == 0) {
    return QST_E_SHM_CORRUPT;
  }
  if (build_runtime->catalog_has_runtime_values != 0) {
    if (NULL != pending->runtime_objects && pending->runtime_mapping == mapping &&
        !qstRefIsEmpty(pending->runtime_object_directory_ref) &&
        !qstRefIsEmpty(pending->runtime_fixture_manifest_ref) &&
        pending->runtime_object_count != 0) {
      runtime.mapping = mapping;
      runtime.object_directory_ref = pending->runtime_object_directory_ref;
      runtime.fixture_manifest_ref = pending->runtime_fixture_manifest_ref;
      runtime.target = seed->target_index + 1U;
      runtime.dialect = seed->target_index + 1U;
      runtime.objects = pending->runtime_objects;
      runtime.object_count = pending->runtime_object_count;
    } else {
      code = qstGeneratorRuntimeContextFromState(
          mapping, state, seed->target_index + 1U, seed->target_index + 1U,
          &runtime);
      if (code != QST_OK) {
        return code;
      }
      code = qstGeneratorFullPendingInitializeRuntime(pending, &runtime);
      if (code != QST_OK) {
        return code;
      }
    }
    runtime_ptr = &runtime;
    code = qstGeneratorFullPendingInitializeBuildRuntime(pending, catalog,
                                                          runtime_ptr);
    if (code != QST_OK) {
      return code;
    }
  }
  qstGeneratorFullPendingPathDestroy(pending);
  if (seed->full_path_choice_count != 0) {
    code = qstGeneratorReserveArray(
        (void **)&pending->full_path_choices,
        &pending->full_path_choice_capacity, seed->full_path_choice_count,
        sizeof(*pending->full_path_choices));
    if (code != QST_OK) {
      return code;
    }
    memcpy(pending->full_path_choices, seed->full_path_choices,
           (size_t)seed->full_path_choice_count *
               sizeof(*pending->full_path_choices));
  }
  pending->full_path_choice_count = seed->full_path_choice_count;
  pending->full_path_next_ordinal = seed->next_path_ordinal;
  pending->full_path_target_index = seed->target_index;
  pending->full_path_ready = 1;
  full_path = qstGeneratorFullPendingPath(pending);
  code = qstGeneratorBuildFullPathCached(
      catalog, &full_path, QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime_ptr,
      build_runtime, &pending->sql);
  if (code != QST_OK) {
    return code;
  }
  pending->cursor_next_path_ordinal = seed->next_path_ordinal;
  pending->target_index = seed->target_index;
  pending->valid = 1;
  return QST_OK;
}

/* Publishes with either one retained path or one retained path per target. */
static EQstCode qstGeneratorFullPublishPending(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorFullPending *pending_by_target, uint32_t pending_count,
    uint64_t *out_gid) {
  SQstGenerationState *state = NULL;
  SQstGeneratorPublishInput input = {0};
  uint64_t published_gid = 0;
  uint64_t publish_seq = 0;
  uint32_t target_index = 0;
  uint32_t done_mask = 0;
  SQstGeneratorRuntimeContext runtime = {0};
  const SQstGeneratorRuntimeContext *runtime_ptr = NULL;
  SQstGeneratorFullPath full_path = {0};
  int32_t task_has_capacity = 0;
  EQstCode code = QST_OK;
  SQstGeneratorFullPending *pending = NULL;
  SQstGeneratorRandomRuntime *build_runtime = NULL;

  if (NULL == catalog || NULL == pending_by_target || NULL == out_gid ||
      (pending_count != 1U && pending_count != QST_GENERATOR_TARGET_COUNT) ||
      role != QST_ROLE_GENERATOR || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_gid = 0;
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_FULL ||
      state->catalog_version != catalog->version ||
      memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
select_target:
  done_mask = atomic_load_explicit(&state->full_target_done_mask,
                                   memory_order_acquire);
  target_index = qstGeneratorFullSelectTarget(
      atomic_load_explicit(&state->full_target_index, memory_order_acquire),
      done_mask);
  if (target_index == QST_GENERATOR_TARGET_COUNT) {
    return QST_E_QUEUE_EMPTY;
  }
  pending = pending_count == 1U ? pending_by_target :
                                 &pending_by_target[target_index];
  if (pending->validated_catalog != catalog ||
      memcmp(pending->validated_catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    code = qst_generator_catalog_validate(catalog);
    if (code != QST_OK) {
      return code;
    }
    pending->validated_catalog = catalog;
    pending->validated_catalog_digest = catalog->digest;
  }
  code = qstGeneratorFullPendingInitializeBuildRuntime(pending, catalog, NULL);
  if (code != QST_OK) {
    return code;
  }
  build_runtime = (SQstGeneratorRandomRuntime *)pending->build_runtime;
  if (NULL == build_runtime || build_runtime->catalog_runtime_values_prepared == 0) {
    return QST_E_SHM_CORRUPT;
  }
  if (build_runtime->catalog_has_runtime_values != 0) {
    /*
     * The dataset snapshot is immutable once main publishes it. Revalidating
     * both blocks here would CRC the complete object directory for every SQL.
     * Retain the first fully validated copy in the generator process and only
     * construct the target-specific view on subsequent publications.
     */
    if (NULL != pending->runtime_objects &&
        pending->runtime_mapping == mapping &&
        !qstRefIsEmpty(pending->runtime_object_directory_ref) &&
        !qstRefIsEmpty(pending->runtime_fixture_manifest_ref) &&
        pending->runtime_object_count != 0) {
      runtime.mapping = mapping;
      runtime.object_directory_ref = pending->runtime_object_directory_ref;
      runtime.fixture_manifest_ref = pending->runtime_fixture_manifest_ref;
      runtime.target = target_index + 1U;
      runtime.dialect = target_index + 1U;
      runtime.objects = pending->runtime_objects;
      runtime.object_count = pending->runtime_object_count;
    } else {
      code = qstGeneratorRuntimeContextFromState(
          mapping, state, target_index + 1U, target_index + 1U, &runtime);
      if (code != QST_OK) {
        return code;
      }
      code = qstGeneratorFullPendingInitializeRuntime(pending, &runtime);
      if (code != QST_OK) {
        return code;
      }
    }
    runtime_ptr = &runtime;
    code = qstGeneratorFullPendingInitializeBuildRuntime(
        pending, catalog, runtime_ptr);
    if (code != QST_OK) {
      return code;
    }
  }
  if (pending->valid != 0) {
    if (pending->target_index != target_index ||
        pending->cursor_next_path_ordinal !=
            state->full_cursor[target_index].next_path_ordinal ||
        pending->full_path_ready == 0 ||
        pending->full_path_target_index != target_index ||
        pending->full_path_next_ordinal !=
            state->full_cursor[target_index].next_path_ordinal ||
        pending->full_path_exhausted != 0 ||
        state->full_cursor[target_index].exhausted != 0) {
      (void)fprintf(stderr,
                    "taosQueryTest: full pending state mismatch "
                    "target=%u pending_target=%u cursor=%llu "
                    "pending_cursor=%llu path_ready=%u path_target=%u "
                    "path_next=%llu path_exhausted=%u cursor_exhausted=%u\n",
                    target_index, pending->target_index,
                    (unsigned long long)state->full_cursor[target_index]
                        .next_path_ordinal,
                    (unsigned long long)pending->cursor_next_path_ordinal,
                    pending->full_path_ready, pending->full_path_target_index,
                    (unsigned long long)pending->full_path_next_ordinal,
                    pending->full_path_exhausted,
                    state->full_cursor[target_index].exhausted);
      qst_generator_full_pending_destroy(pending);
      return QST_E_SHM_CORRUPT;
    }
  } else {
    if (pending->full_path_ready == 0 ||
        pending->full_path_target_index != target_index ||
        pending->full_path_next_ordinal !=
            state->full_cursor[target_index].next_path_ordinal) {
      code = qstGeneratorFullPendingRestorePath(
          catalog, &state->full_cursor[target_index], target_index,
          QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime_ptr, pending);
      if (code != QST_OK) {
        if (code == QST_E_QUEUE_EMPTY) {
          qstGeneratorFullMarkTargetDone(state, target_index);
          goto select_target;
        }
        return code;
      }
    }
    if (pending->full_path_exhausted != 0) {
      qstGeneratorFullMarkTargetDone(state, target_index);
      goto select_target;
    }
    full_path = qstGeneratorFullPendingPath(pending);
    code = qstGeneratorBuildFullPathCached(
        catalog, &full_path, QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime_ptr,
        (SQstGeneratorRandomRuntime *)pending->build_runtime, &pending->sql);
    if (code != QST_OK) {
      return code;
    }
    pending->cursor_next_path_ordinal =
        state->full_cursor[target_index].next_path_ordinal;
    pending->target_index = target_index;
    pending->valid = 1;
  }
  code = qst_task_has_capacity(mapping, &task_has_capacity);
  if (code != QST_OK) {
    return code;
  }
  if (task_has_capacity == 0) {
    return QST_E_QUEUE_FULL;
  }
  input.target = target_index + 1U;
  input.mode = QST_MODE_FULL;
  input.focus = state->focus;
  input.dialect = target_index + 1U;
  publish_seq = atomic_load_explicit(&state->publish_seq, memory_order_acquire);
  if (publish_seq == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  input.generation_seq = publish_seq + 1U;
  input.weight_profile_version = weight_profile_version;
  input.catalog_version = catalog->version;
  input.catalog_digest = catalog->digest;
  input.dataset_digest = dataset_digest;
  code = qst_generator_publish(mapping, role, generation, &input,
                               &pending->sql, queue_deadline_mono_ns,
                               &published_gid);
  if (code != QST_OK) {
    if (code != QST_E_QUEUE_FULL && code != QST_E_BLOCK_NO_SPACE) {
      qst_generator_full_pending_destroy(pending);
    }
    return code;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &state->publish_seq, &publish_seq, input.generation_seq,
          memory_order_acq_rel, memory_order_acquire)) {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    qst_generator_full_pending_destroy(pending);
    return QST_E_SHM_CORRUPT;
  }
  full_path = qstGeneratorFullPendingPath(pending);
  code = qstGeneratorFullPathAdvance(&full_path, &pending->sql);
  if (code == QST_E_QUEUE_EMPTY) {
    pending->full_path_exhausted = 1;
    code = QST_OK;
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: full path advance failed code=%d target=%u "
                  "cursor=%llu choices=%u path_choices=%u\n",
                  code, target_index + 1U,
                  (unsigned long long)state->full_cursor[target_index]
                      .next_path_ordinal,
                  pending->sql.choice_count, full_path.choice_count);
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    qst_generator_full_pending_destroy(pending);
    return QST_E_SHM_CORRUPT;
  }
  qstGeneratorFullPendingSavePath(pending, &full_path);
  code = qst_generator_full_commit(&state->full_cursor[target_index],
                                   &pending->sql);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: full cursor commit failed code=%d target=%u "
                  "cursor=%llu choices=%u\n",
                  code, target_index + 1U,
                  (unsigned long long)state->full_cursor[target_index]
                      .next_path_ordinal,
                  pending->sql.choice_count);
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    qst_generator_full_pending_destroy(pending);
    return QST_E_SHM_CORRUPT;
  }
  pending->full_path_next_ordinal =
      state->full_cursor[target_index].next_path_ordinal;
  if (pending->full_path_exhausted != 0) {
    qstGeneratorFullMarkTargetDone(state, target_index);
  } else {
    qstGeneratorFullScheduleNextTarget(state, target_index, done_mask);
  }
  qstGeneratorFullPendingClearSql(pending);
  *out_gid = published_gid;
  return QST_OK;
}

EQstCode qst_generator_full_publish_pending(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorFullPending *pending, uint64_t *out_gid) {
  return qstGeneratorFullPublishPending(
      mapping, role, generation, catalog, dataset_digest, weight_profile_version,
      queue_deadline_mono_ns, pending, 1, out_gid);
}

EQstCode qst_generator_full_publish_pending_by_target(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorFullPending pending_by_target[QST_GENERATOR_TARGET_COUNT],
    uint64_t *out_gid) {
  return qstGeneratorFullPublishPending(
      mapping, role, generation, catalog, dataset_digest, weight_profile_version,
      queue_deadline_mono_ns, pending_by_target,
      QST_GENERATOR_TARGET_COUNT, out_gid);
}

EQstCode qst_generator_full_publish_next(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    uint64_t *out_gid) {
  SQstGeneratorFullPending pending = {0};
  EQstCode code = qst_generator_full_publish_pending(
      mapping, role, generation, catalog, dataset_digest, weight_profile_version,
      queue_deadline_mono_ns, &pending, out_gid);

  qst_generator_full_pending_destroy(&pending);
  return code;
}

void qst_generator_random_pending_reset(SQstGeneratorRandomPending *pending) {
  if (NULL != pending) {
    qstGeneratorGeneratedSqlReset(&pending->sql);
    memset(&pending->input, 0, sizeof(pending->input));
    pending->prepared_gid = 0;
    memset(&pending->prepared_sql_ref, 0, sizeof(pending->prepared_sql_ref));
    memset(&pending->prepared_generation_record_ref, 0,
           sizeof(pending->prepared_generation_record_ref));
    memset(&pending->prepared_generation_record_sha256, 0,
           sizeof(pending->prepared_generation_record_sha256));
    pending->resources_prepared = 0;
    pending->reserved0 = 0;
  }
}

void qst_generator_random_pending_destroy(SQstGeneratorRandomPending *pending) {
  if (NULL != pending) {
    qst_generator_generated_sql_destroy(&pending->sql);
    memset(&pending->input, 0, sizeof(pending->input));
    pending->prepared_gid = 0;
    memset(&pending->prepared_sql_ref, 0, sizeof(pending->prepared_sql_ref));
    memset(&pending->prepared_generation_record_ref, 0,
           sizeof(pending->prepared_generation_record_ref));
    memset(&pending->prepared_generation_record_sha256, 0,
           sizeof(pending->prepared_generation_record_sha256));
    pending->resources_prepared = 0;
    pending->reserved0 = 0;
  }
}

EQstCode qst_generator_random_release_resources(
    SQstShmMapping *mapping, SQstGeneratorRandomPending *pending) {
  EQstCode code = QST_OK;
  EQstCode release_code = QST_OK;

  if (NULL == mapping || NULL == pending) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pending->resources_prepared == 0) {
    return QST_OK;
  }
  if (pending->prepared_gid == 0 ||
      qstRefIsEmpty(pending->prepared_sql_ref) ||
      qstRefIsEmpty(pending->prepared_generation_record_ref)) {
    return QST_E_SHM_CORRUPT;
  }
  release_code = qst_block_release(mapping, pending->prepared_sql_ref,
                                   pending->prepared_gid);
  if (release_code != QST_OK) {
    code = QST_E_SHM_CORRUPT;
  }
  release_code = qst_block_release(
      mapping, pending->prepared_generation_record_ref, pending->prepared_gid);
  if (release_code != QST_OK) {
    code = QST_E_SHM_CORRUPT;
  }
  pending->prepared_gid = 0;
  memset(&pending->prepared_sql_ref, 0, sizeof(pending->prepared_sql_ref));
  memset(&pending->prepared_generation_record_ref, 0,
         sizeof(pending->prepared_generation_record_ref));
  memset(&pending->prepared_generation_record_sha256, 0,
         sizeof(pending->prepared_generation_record_sha256));
  pending->resources_prepared = 0;
  return code;
}

EQstCode qst_generator_random_prepare_resources(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    SQstGeneratorRandomPending *pending) {
  SQstRef sql_ref = {0};
  SQstRef generation_record_ref = {0};
  SQstDigest256 generation_record_sha256 = {0};
  uint64_t gid = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == pending || role != QST_ROLE_GENERATOR ||
      generation == 0 || pending->input.generation_seq == 0 ||
      pending->sql.sql_length == 0 ||
      pending->sql.sql_length >= QST_GENERATOR_SQL_BYTES) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (pending->resources_prepared != 0) {
    return QST_OK;
  }
  code = qstGeneratorCheckPublicationBlockBudget(mapping, &pending->sql);
  if (code != QST_OK) {
    return code;
  }
  code = qst_task_reserve_gid(mapping, role, generation, &gid);
  if (code != QST_OK) {
    return code;
  }
  code = qst_block_alloc(mapping, gid, generation, pending->sql.sql_length,
                         &sql_ref);
  if (code != QST_OK) {
    return code;
  }
  code = qst_block_write_seal(mapping, &sql_ref, pending->sql.sql,
                              pending->sql.sql_length);
  if (code != QST_OK) {
    (void)qst_block_release(mapping, sql_ref, gid);
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    return QST_E_SHM_CORRUPT;
  }
  code = qstGeneratorWriteGenerationRecord(
      mapping, gid, generation, &pending->input, &pending->sql,
      &generation_record_ref, &generation_record_sha256);
  if (code != QST_OK) {
    (void)qst_block_release(mapping, sql_ref, gid);
    return code;
  }
  pending->prepared_gid = gid;
  pending->prepared_sql_ref = sql_ref;
  pending->prepared_generation_record_ref = generation_record_ref;
  pending->prepared_generation_record_sha256 = generation_record_sha256;
  pending->resources_prepared = 1;
  return QST_OK;
}

EQstCode qst_generator_random_build_sequence(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t generation_seq,
    SQstGeneratorRandomRuntime *cached_runtime,
    SQstGeneratorRandomPending *out_pending) {
  SQstGenerationState *state = NULL;
  SQstGeneratorRandomState random_state = {0};
  SQstGeneratorRuntimeContext runtime = {0};
  uint32_t target_index = 0;
  EQstCode code = QST_OK;

  if (NULL == catalog || NULL == cached_runtime || NULL == out_pending ||
      role != QST_ROLE_GENERATOR || generation == 0 || generation_seq == 0 ||
      generation_seq == UINT64_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_generator_random_pending_reset(out_pending);
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_RANDOM ||
      state->catalog_version != catalog->version ||
      memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  code = qst_generator_random_init(&random_state, state->random_seed,
                                   generation_seq);
  if (code != QST_OK) return code;
  code = qstGeneratorSelectRandomTarget(state, &random_state, &target_index);
  if (code != QST_OK) return code;
  code = qstGeneratorRandomRuntimeInitialize(
      mapping, state, catalog, target_index + 1U, target_index + 1U,
      cached_runtime, &runtime);
  if (code != QST_OK) return code;
  code = qst_generator_random_build_prepared_with_runtime(
      catalog, &random_state, QST_GENERATOR_DEFAULT_QUERY_DEPTH,
      cached_runtime->catalog_has_runtime_values != 0 ? &runtime : NULL,
      cached_runtime, &out_pending->sql);
  if (code != QST_OK) {
    qst_generator_random_pending_destroy(out_pending);
    return code;
  }
  out_pending->input.target = target_index + 1U;
  out_pending->input.mode = QST_MODE_RANDOM;
  out_pending->input.focus = state->focus;
  out_pending->input.dialect = target_index + 1U;
  out_pending->input.generation_seq = generation_seq;
  out_pending->input.weight_profile_version = weight_profile_version;
  out_pending->input.catalog_version = catalog->version;
  out_pending->input.catalog_digest = catalog->digest;
  out_pending->input.dataset_digest = dataset_digest;
  return QST_OK;
}

EQstCode qst_generator_random_plan_build_sequence(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorFragPlan *plan, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t generation_seq,
    SQstGeneratorRandomRuntime *runtime, SQstGeneratorRandomPending *out_pending) {
  SQstGenerationState *state = NULL;
  SQstGeneratorRandomState random_state = {0};

  if (NULL == plan || NULL == plan->catalog ||
      plan->fragment_count != plan->catalog->fragment_count ||
      plan->root_fragment_index >= plan->fragment_count ||
      NULL == plan->fragments || NULL == plan->values ||
      memcmp(plan->catalog_digest.bytes, plan->catalog->digest.bytes,
             sizeof(plan->catalog_digest.bytes)) != 0 ||
      plan->fragments[plan->root_fragment_index].fragment_id !=
          plan->catalog->root_fragment_id) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qst_generator_state_get(mapping, &state) != QST_OK ||
      qst_generator_random_init(&random_state, state->random_seed,
                                generation_seq) != QST_OK) {
    return QST_E_SHM_CORRUPT;
  }
  return qst_generator_random_plan_build_from_state(
      mapping, role, generation, plan, dataset_digest, weight_profile_version,
      generation_seq, &random_state, runtime, out_pending);
}

EQstCode qst_generator_random_plan_build_from_state(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorFragPlan *plan, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t generation_seq,
    SQstGeneratorRandomState *random_state,
    SQstGeneratorRandomRuntime *cached_runtime,
    SQstGeneratorRandomPending *out_pending) {
  SQstGenerationState *state = NULL;
  SQstGeneratorRuntimeContext runtime = {0};
  uint32_t target_index = 0;
  uint64_t started_ns = 0;
  EQstCode code = QST_OK;

  if (NULL == plan || NULL == plan->catalog || NULL == random_state ||
      NULL == cached_runtime || NULL == out_pending || role != QST_ROLE_GENERATOR ||
      generation == 0 || generation_seq == 0 || generation_seq == UINT64_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  qst_generator_random_pending_reset(out_pending);
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_RANDOM ||
      state->catalog_version != plan->catalog->version ||
      memcmp(state->catalog_digest.bytes, plan->catalog_digest.bytes,
             sizeof(plan->catalog_digest.bytes)) != 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  code = qstGeneratorSelectRandomTarget(state, random_state, &target_index);
  if (code != QST_OK) return code;
  started_ns = qstGeneratorProfileNowNs(cached_runtime);
  code = qstGeneratorRandomRuntimeInitialize(
      mapping, state, plan->catalog, target_index + 1U, target_index + 1U,
      cached_runtime, &runtime);
  if (cached_runtime->profile_enabled != 0 &&
      cached_runtime->profile_runtime_initialize_count != UINT64_MAX) {
    ++cached_runtime->profile_runtime_initialize_count;
  }
  qstGeneratorProfileAddElapsed(cached_runtime, started_ns,
                                &cached_runtime->profile_runtime_initialize_ns);
  if (code != QST_OK) return code;
  started_ns = qstGeneratorProfileNowNs(cached_runtime);
  for (uint32_t attempt = 0;
       attempt < QST_GENERATOR_RANDOM_REBUILD_ATTEMPT_LIMIT; ++attempt) {
    code = qstGeneratorRandomBuildPlanWithRuntime(
        plan, random_state, QST_GENERATOR_DEFAULT_QUERY_DEPTH,
        plan->has_runtime_values != 0 ? &runtime : NULL,
        cached_runtime, &out_pending->sql);
    if (qstGeneratorRandomBuildNeedsRetry(code) == 0) {
      break;
    }
  }
  if (qstGeneratorRandomBuildNeedsRetry(code) != 0) {
    code = QST_E_SQL_LIMIT;
  }
  if (cached_runtime->profile_enabled != 0 &&
      cached_runtime->profile_plan_walk_count != UINT64_MAX) {
    ++cached_runtime->profile_plan_walk_count;
  }
  qstGeneratorProfileAddElapsed(cached_runtime, started_ns,
                                &cached_runtime->profile_plan_walk_ns);
  if (code != QST_OK) {
    qst_generator_random_pending_destroy(out_pending);
    return code;
  }
  out_pending->input.target = target_index + 1U;
  out_pending->input.mode = QST_MODE_RANDOM;
  out_pending->input.focus = state->focus;
  out_pending->input.dialect = target_index + 1U;
  out_pending->input.generation_seq = generation_seq;
  out_pending->input.weight_profile_version = weight_profile_version;
  out_pending->input.catalog_version = plan->catalog->version;
  out_pending->input.catalog_digest = plan->catalog_digest;
  out_pending->input.dataset_digest = dataset_digest;
  return QST_OK;
}

EQstCode qst_generator_random_publish_prepared(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    SQstGeneratorRandomPending *pending, uint64_t queue_deadline_mono_ns,
    uint64_t *out_gid) {
  SQstGenerationState *state = NULL;
  uint64_t expected_seq = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == pending || NULL == out_gid ||
      pending->input.generation_seq == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_gid = 0;
  if (pending->resources_prepared != 0) {
    SQstGeneratePayload payload = {0};
    SQstMessage message = {0};
    uint64_t dispatch_epoch = 0;

    if (role != QST_ROLE_GENERATOR || generation == 0 ||
        pending->prepared_gid == 0 ||
        qstRefIsEmpty(pending->prepared_sql_ref) ||
        qstRefIsEmpty(pending->prepared_generation_record_ref)) {
      return QST_E_INVALID_ARGUMENT;
    }
    payload.target = pending->input.target;
    payload.mode = pending->input.mode;
    payload.focus = pending->input.focus;
    payload.dialect = pending->input.dialect;
    payload.generation_contains_abnormal = pending->sql.contains_abnormal;
    payload.generation_contains_unsupported_fallback =
        pending->sql.contains_unsupported_fallback;
    payload.generation_seq = pending->input.generation_seq;
    payload.weight_profile_version = pending->input.weight_profile_version;
    payload.sql_sha256 = pending->sql.sql_sha256;
    payload.dataset_digest = pending->input.dataset_digest;
    payload.sql_ref = pending->prepared_sql_ref;
    payload.generation_record_ref = pending->prepared_generation_record_ref;
    payload.generation_record_sha256 =
        pending->prepared_generation_record_sha256;
    code = qst_task_create_prepared(mapping, pending->prepared_gid, role,
                                    generation, &payload, &dispatch_epoch);
    if (code != QST_OK) {
      (void)qst_generator_random_release_resources(mapping, pending);
      return code;
    }
    message.message_type = QST_MSG_GENERATE_WORK;
    message.producer_role = role;
    message.producer_generation = generation;
    message.target = payload.target;
    message.run_id = mapping->header->run_id;
    message.gid = pending->prepared_gid;
    message.dispatch_epoch = dispatch_epoch;
    message.transition_seq = 1;
    message.body.generate = payload;
    code = qst_queue_publish(mapping, QST_QUEUE_GENERATE, &message,
                             queue_deadline_mono_ns);
    if (code != QST_OK) {
      const EQstCode cleanup_code = qst_task_abort_generated(
          mapping, pending->prepared_gid, role, generation);
      const EQstCode release_code =
          qst_generator_random_release_resources(mapping, pending);

      if (cleanup_code != QST_OK || release_code != QST_OK) {
        return QST_E_SHM_CORRUPT;
      }
      return code;
    }
    atomic_fetch_add_explicit(&mapping->header->generated_task_count, 1,
                              memory_order_release);
    *out_gid = pending->prepared_gid;
    code = qst_generator_state_get(mapping, &state);
    if (code != QST_OK) {
      return code;
    }
    expected_seq = pending->input.generation_seq;
    if (!atomic_compare_exchange_strong_explicit(
            &state->random_generation_seq, &expected_seq, expected_seq + 1U,
            memory_order_acq_rel, memory_order_acquire)) {
      atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                            memory_order_release);
      return QST_E_SHM_CORRUPT;
    }
    /* The sealed blocks now belong to the task ledger. Only drop process-local
     * SQL/metadata storage; never release those shared blocks here. */
    pending->prepared_gid = 0;
    memset(&pending->prepared_sql_ref, 0,
           sizeof(pending->prepared_sql_ref));
    memset(&pending->prepared_generation_record_ref, 0,
           sizeof(pending->prepared_generation_record_ref));
    memset(&pending->prepared_generation_record_sha256, 0,
           sizeof(pending->prepared_generation_record_sha256));
    pending->resources_prepared = 0;
    return QST_OK;
  }
  code = qst_generator_publish(mapping, role, generation, &pending->input,
                               &pending->sql, queue_deadline_mono_ns, out_gid);
  if (code != QST_OK) return code;
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK) return code;
  expected_seq = pending->input.generation_seq;
  if (!atomic_compare_exchange_strong_explicit(
          &state->random_generation_seq, &expected_seq, expected_seq + 1U,
          memory_order_acq_rel, memory_order_acquire)) {
    atomic_store_explicit(&mapping->header->run_state, QST_RUN_FAILED,
                          memory_order_release);
    return QST_E_SHM_CORRUPT;
  }
  return QST_OK;
}

EQstCode qst_generator_random_publish_next(
    SQstShmMapping *mapping, EQstRole role, uint32_t generation,
    const SQstGeneratorCatalog *catalog, SQstDigest256 dataset_digest,
    uint64_t weight_profile_version, uint64_t queue_deadline_mono_ns,
    SQstGeneratorRandomRuntime *cached_runtime, uint64_t *out_gid) {
  SQstGenerationState *state = NULL;
  SQstGeneratorRandomPending pending = {0};
  uint64_t generation_seq = 0;
  EQstCode code = QST_OK;

  if (NULL == catalog || NULL == cached_runtime || NULL == out_gid ||
      role != QST_ROLE_GENERATOR || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_gid = 0;
  code = qst_generator_state_get(mapping, &state);
  if (code != QST_OK || state->mode != QST_MODE_RANDOM ||
      state->catalog_version != catalog->version ||
      memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
             sizeof(catalog->digest.bytes)) != 0) {
    return code != QST_OK ? code : QST_E_SHM_CORRUPT;
  }
  generation_seq = atomic_load_explicit(&state->random_generation_seq,
                                        memory_order_acquire);
  if (generation_seq == 0 || generation_seq == UINT64_MAX) {
    return QST_E_RESOURCE;
  }
  code = qst_generator_random_build_sequence(
      mapping, role, generation, catalog, dataset_digest, weight_profile_version,
      generation_seq, cached_runtime, &pending);
  if (code != QST_OK) return code;
  code = qst_generator_random_publish_prepared(
      mapping, role, generation, &pending, queue_deadline_mono_ns, out_gid);
  if (code != QST_OK) {
    qst_generator_random_pending_destroy(&pending);
    return code;
  }
  qst_generator_random_pending_destroy(&pending);
  return QST_OK;
}

EQstCode qst_generator_full_next(const SQstGeneratorCatalog *catalog,
                                 SQstGeneratorCursor *cursor, uint32_t depth,
                                 SQstGeneratedSql *out_sql) {
  SQstGeneratorCursor candidate = {0};
  SQstGeneratedSql next_sql = {0};
  EQstCode code = qst_generator_full_peek(catalog, cursor, depth, out_sql);

  if (code != QST_OK) {
    if (code == QST_E_QUEUE_EMPTY && NULL != cursor) {
      cursor->exhausted = 1;
    }
    return code;
  }
  candidate = *cursor;
  code = qst_generator_full_commit(&candidate, out_sql);
  if (code != QST_OK) {
    return code;
  }
  code = qst_generator_full_peek(catalog, &candidate, depth, &next_sql);
  qst_generator_generated_sql_destroy(&next_sql);
  if (code == QST_E_QUEUE_EMPTY) {
    candidate.exhausted = 1;
  } else if (code != QST_OK) {
    return code;
  }
  *cursor = candidate;
  return QST_OK;
}

EQstCode qst_generator_full_exhaustive_count(
    const SQstGeneratorCatalog *catalog, uint32_t depth,
    const SQstGeneratorRuntimeContext *runtime, uint64_t *out_path_count,
    uint64_t *out_fallback_count) {
  SQstGeneratorFullPath path = {0};
  SQstGeneratedSql sql = {0};
  uint64_t path_count = 0;
  uint64_t fallback_count = 0;
  EQstCode code = QST_OK;

  if (catalog == NULL || out_path_count == NULL || out_fallback_count == NULL ||
      depth > QST_GENERATOR_MAX_RECURSION_DEPTH) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qst_generator_catalog_validate(catalog);
  if (code != QST_OK) {
    return code;
  }
  for (;;) {
    qst_generator_generated_sql_destroy(&sql);
    code = qstGeneratorBuildFullPath(catalog, &path, depth, runtime, &sql);
    if (code != QST_OK) {
      break;
    }
    if (sql.contains_unsupported_fallback != 0) {
      if (fallback_count == UINT64_MAX) {
        code = QST_E_RESOURCE;
        break;
      }
      ++fallback_count;
    }
    if (path_count == UINT64_MAX) {
      code = QST_E_RESOURCE;
      break;
    }
    ++path_count;
    if (path_count % UINT64_C(1024) == 0) {
      (void)fprintf(stderr,
                    "taosQueryTest: generator full exhaustive paths=%llu\n",
                    (unsigned long long)path_count);
    }
    code = qstGeneratorFullPathAdvance(&path, &sql);
    if (code == QST_E_QUEUE_EMPTY) {
      code = QST_OK;
      break;
    }
    if (code != QST_OK) {
      break;
    }
  }
  qst_generator_generated_sql_destroy(&sql);
  qstGeneratorFullPathDestroy(&path);
  if (code == QST_OK) {
    *out_path_count = path_count;
    *out_fallback_count = fallback_count;
  }
  return code;
}

static const char *qstGeneratorDebugTargetName(uint32_t target) {
  switch (target) {
    case QST_TARGET_TD:
      return "td";
    case QST_TARGET_MYSQL:
      return "mysql";
    case QST_TARGET_PG:
      return "pg";
    case QST_TARGET_INFLUX:
      return "influx";
    default:
      return NULL;
  }
}

static uint64_t qstGeneratorDebugMonotonicNowNs(void) {
  struct timespec now = {0};

  return clock_gettime(CLOCK_MONOTONIC, &now) == 0 && now.tv_sec >= 0 &&
                 now.tv_nsec >= 0
             ? (uint64_t)now.tv_sec * UINT64_C(1000000000) +
                   (uint64_t)now.tv_nsec
             : 0;
}

static EQstCode qstGeneratorDebugWriteAll(int32_t fd, const void *data,
                                          uint64_t bytes) {
  const uint8_t *cursor = (const uint8_t *)data;

  if (fd < 0 || (data == NULL && bytes != 0)) {
    return QST_E_INVALID_ARGUMENT;
  }
  while (bytes != 0) {
    const size_t chunk = bytes > SSIZE_MAX ? (size_t)SSIZE_MAX : (size_t)bytes;
    const ssize_t written = write(fd, cursor, chunk);

    if (written > 0) {
      cursor += written;
      bytes -= (uint64_t)written;
      continue;
    }
    if (written < 0 && errno == EINTR) {
      continue;
    }
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstGeneratorDebugWriteJsonString(int32_t fd, const char *text,
                                                  uint32_t text_bytes) {
  static const char kHex[] = "0123456789abcdef";

  if (fd < 0 || text == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < text_bytes; ++index) {
    const uint8_t byte = (uint8_t)text[index];
    char escaped[6] = {0};
    uint32_t escaped_bytes = 0;

    switch (byte) {
      case '"':
      case '\\':
        escaped[0] = '\\';
        escaped[1] = (char)byte;
        escaped_bytes = 2;
        break;
      case '\b':
        memcpy(escaped, "\\b", 2);
        escaped_bytes = 2;
        break;
      case '\f':
        memcpy(escaped, "\\f", 2);
        escaped_bytes = 2;
        break;
      case '\n':
        memcpy(escaped, "\\n", 2);
        escaped_bytes = 2;
        break;
      case '\r':
        memcpy(escaped, "\\r", 2);
        escaped_bytes = 2;
        break;
      case '\t':
        memcpy(escaped, "\\t", 2);
        escaped_bytes = 2;
        break;
      default:
        if (byte < UINT8_C(0x20)) {
          memcpy(escaped, "\\u00", 4);
          escaped[4] = kHex[byte >> 4U];
          escaped[5] = kHex[byte & UINT8_C(0x0f)];
          escaped_bytes = 6;
        } else {
          escaped[0] = (char)byte;
          escaped_bytes = 1;
        }
        break;
    }
    if (qstGeneratorDebugWriteAll(fd, escaped, escaped_bytes) != QST_OK) {
      return QST_E_RESOURCE;
    }
  }
  return QST_OK;
}

static EQstCode qstGeneratorDebugWriteRecord(
    int32_t fd, const SQstGenerationState *state,
    const SQstGeneratorPublishInput *input, const SQstGeneratedSql *sql) {
  char sha256[65] = {0};
  char random_seed[32] = {0};
  char header[512] = {0};
  const char *target_name = NULL;
  const char *mode_name = NULL;
  int32_t written = 0;

  if (fd < 0 || state == NULL || input == NULL || sql == NULL ||
      sql->sql_length == 0 || input->generation_seq == 0 ||
      input->target < QST_TARGET_TD || input->target > QST_TARGET_INFLUX) {
    return QST_E_INVALID_ARGUMENT;
  }
  target_name = qstGeneratorDebugTargetName(input->target);
  mode_name = input->mode == QST_MODE_FULL ? "full" :
              input->mode == QST_MODE_RANDOM ? "random" : NULL;
  if (target_name == NULL || mode_name == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (input->mode == QST_MODE_RANDOM &&
      snprintf(random_seed, sizeof(random_seed), "%llu",
               (unsigned long long)state->random_seed) < 0) {
    return QST_E_RESOURCE;
  }
  if (input->mode == QST_MODE_FULL) {
    memcpy(random_seed, "null", sizeof("null"));
  }
  for (uint32_t index = 0; index < sizeof(sql->sql_sha256.bytes); ++index) {
    static const char kHex[] = "0123456789abcdef";

    sha256[index * 2U] = kHex[sql->sql_sha256.bytes[index] >> 4U];
    sha256[index * 2U + 1U] = kHex[sql->sql_sha256.bytes[index] & 0x0fU];
  }
  written = snprintf(
      header, sizeof(header),
      "{\"format\":\"taosQueryTest-generated-sql-v1\",\"mode\":\"%s\","
      "\"target\":\"%s\",\"dialect\":%u,\"generation_seq\":%llu,"
      "\"random_seed\":%s,\"weight_profile_version\":%llu,"
      "\"generation_abnormal\":%s,\"generation_unsupported_fallback\":%s,"
      "\"sql_sha256\":\"%s\",\"sql_text\":\"",
      mode_name, target_name, input->dialect,
      (unsigned long long)input->generation_seq,
      random_seed,
      (unsigned long long)input->weight_profile_version,
      sql->contains_abnormal != 0 ? "true" : "false",
      sql->contains_unsupported_fallback != 0 ? "true" : "false", sha256);
  if (written < 0 || (uint64_t)written >= sizeof(header) ||
      qstGeneratorDebugWriteAll(fd, header, (uint64_t)written) != QST_OK ||
      qstGeneratorDebugWriteJsonString(fd, sql->sql, sql->sql_length) != QST_OK ||
      qstGeneratorDebugWriteAll(fd, "\"}\n", 3) != QST_OK) {
    return QST_E_RESOURCE;
  }
  return QST_OK;
}

static EQstCode qstGeneratorDebugWriteRandom(
    SQstShmMapping *mapping, uint32_t main_generation,
    const SQstGeneratorCatalog *catalog, SQstGenerationState *state,
    SQstDigest256 dataset_digest, int32_t fd, uint64_t *out_sql_count) {
  SQstGeneratorFragPlan plan = {0};
  SQstGeneratorRandomRuntime runtime = {0};
  uint64_t generation_seq = 1;
  uint64_t deadline_ns = 0;
  EQstCode code = QST_OK;

  if ((state->statement_limit == 0 && state->duration_limit_minutes == 0) ||
      out_sql_count == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_sql_count = 0;
  if (state->duration_limit_minutes != 0) {
    const uint64_t now_ns = qstGeneratorDebugMonotonicNowNs();
    const uint64_t duration_ns = state->duration_limit_minutes *
                                 UINT64_C(60) * UINT64_C(1000000000);

    if (now_ns == 0 || duration_ns == 0 || now_ns > UINT64_MAX - duration_ns) {
      return QST_E_RESOURCE;
    }
    deadline_ns = now_ns + duration_ns;
  }
  code = qst_generator_frag_plan_prepare(&plan, catalog);
  while (code == QST_OK) {
    SQstGeneratorRandomPending pending = {0};

    if ((state->statement_limit != 0 && generation_seq > state->statement_limit) ||
        (deadline_ns != 0 && qstGeneratorDebugMonotonicNowNs() >= deadline_ns)) {
      atomic_store_explicit(&mapping->header->stop_reason,
                            deadline_ns != 0 &&
                                    qstGeneratorDebugMonotonicNowNs() >= deadline_ns ?
                                QST_STOP_DURATION_LIMIT : QST_STOP_STATEMENT_LIMIT,
                            memory_order_release);
      break;
    }
    code = qst_generator_random_plan_build_sequence(
        mapping, QST_ROLE_GENERATOR, main_generation, &plan, dataset_digest, 1,
        generation_seq, &runtime, &pending);
    if (code == QST_OK) {
      code = qstGeneratorDebugWriteRecord(fd, state, &pending.input,
                                          &pending.sql);
    }
    qst_generator_random_pending_destroy(&pending);
    if (code == QST_OK) {
      if (*out_sql_count == UINT64_MAX) {
        code = QST_E_RESOURCE;
        break;
      }
      ++*out_sql_count;
      ++generation_seq;
    }
  }
  qst_generator_random_runtime_destroy(&runtime);
  qst_generator_frag_plan_destroy(&plan);
  return code;
}

static EQstCode qstGeneratorDebugWriteFull(
    SQstShmMapping *mapping, const SQstGeneratorCatalog *catalog,
    SQstGenerationState *state, int32_t fd, uint64_t *out_sql_count) {
  SQstGeneratorCursor cursors[QST_GENERATOR_TARGET_COUNT] = {{0}};
  SQstGeneratorFullPending runtime_pending = {0};
  uint64_t generation_seq = 0;
  uint64_t deadline_ns = 0;
  uint32_t done_mask = UINT32_C(0x0f) & ~state->target_selection_bits;
  uint32_t next_target_index = 0;
  EQstCode code = QST_OK;

  if (out_sql_count == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_sql_count = 0;
  if (state->duration_limit_minutes != 0) {
    const uint64_t now_ns = qstGeneratorDebugMonotonicNowNs();
    const uint64_t duration_ns = state->duration_limit_minutes *
                                 UINT64_C(60) * UINT64_C(1000000000);

    if (now_ns == 0 || duration_ns == 0 || now_ns > UINT64_MAX - duration_ns) {
      return QST_E_RESOURCE;
    }
    deadline_ns = now_ns + duration_ns;
  }
  while (code == QST_OK) {
    SQstGeneratorRuntimeContext runtime = {0};
    const SQstGeneratorRuntimeContext *runtime_ptr = NULL;
    uint32_t target_index = 0;

    if (deadline_ns != 0 && qstGeneratorDebugMonotonicNowNs() >= deadline_ns) {
      atomic_store_explicit(&mapping->header->stop_reason,
                            QST_STOP_DURATION_LIMIT, memory_order_release);
      break;
    }
    target_index = qstGeneratorFullSelectTarget(next_target_index, done_mask);
    if (target_index == QST_GENERATOR_TARGET_COUNT) {
      break;
    }
    if (qstGeneratorCatalogHasRuntimeObject(catalog) != 0) {
      code = qstGeneratorRuntimeContextFromState(
          mapping, state, target_index + 1U, target_index + 1U, &runtime);
      if (code == QST_OK) {
        code = qstGeneratorFullPendingInitializeRuntime(&runtime_pending,
                                                        &runtime);
      }
      if (code != QST_OK) {
        break;
      }
      runtime_ptr = &runtime;
    }
    SQstGeneratedSql sql = {0};
    SQstGeneratorPublishInput input = {0};

    code = qst_generator_full_peek_with_runtime(
        catalog, &cursors[target_index], QST_GENERATOR_DEFAULT_QUERY_DEPTH,
        runtime_ptr, &sql);
    if (code == QST_E_QUEUE_EMPTY) {
      cursors[target_index].exhausted = 1;
      done_mask |= UINT32_C(1) << target_index;
      next_target_index = target_index + 1U;
      code = QST_OK;
      continue;
    }
    if (code != QST_OK || generation_seq == UINT64_MAX) {
      qst_generator_generated_sql_destroy(&sql);
      code = code != QST_OK ? code : QST_E_RESOURCE;
      break;
    }
    input.target = target_index + 1U;
    input.mode = QST_MODE_FULL;
    input.focus = state->focus;
    input.dialect = target_index + 1U;
    input.generation_seq = ++generation_seq;
    input.weight_profile_version = 1;
    input.catalog_version = catalog->version;
    input.catalog_digest = catalog->digest;
    input.dataset_digest = state->dataset_digest;
    code = qstGeneratorDebugWriteRecord(fd, state, &input, &sql);
    if (code == QST_OK) {
      code = qst_generator_full_commit(&cursors[target_index], &sql);
    }
    qst_generator_generated_sql_destroy(&sql);
    if (code != QST_OK) {
      break;
    }
    if (*out_sql_count == UINT64_MAX) {
      code = QST_E_RESOURCE;
      break;
    }
    ++*out_sql_count;
    next_target_index = target_index + 1U;
  }
  if (code == QST_OK && deadline_ns == 0) {
    atomic_store_explicit(&mapping->header->stop_reason,
                          QST_STOP_FULL_COMPLETE, memory_order_release);
  }
  qst_generator_full_pending_destroy(&runtime_pending);
  return code;
}

EQstCode qst_generator_write_only(SQstShmMapping *mapping,
                                  uint32_t main_generation,
                                  const char *output_path,
                                  uint64_t *out_sql_count) {
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGenerationState *state = NULL;
  SQstDigest256 dataset_digest = {0};
  SQstDigest256 fixture_manifest_digest = {0};
  SQstRef object_directory_ref = {0};
  SQstRef fixture_manifest_ref = {0};
  uint32_t actual_generation = 0;
  int32_t fd = -1;
  EQstCode code = QST_OK;

  if (mapping == NULL || mapping->header == NULL || main_generation == 0 ||
      output_path == NULL || output_path[0] == '\0' ||
      strlen(output_path) >= QST_GENERATOR_DEBUG_OUTPUT_PATH_BYTES ||
      out_sql_count == NULL) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_sql_count = 0;
  code = qst_process_find_generation(mapping, QST_ROLE_MAIN,
                                     (int32_t)getpid(), &actual_generation);
  if (code != QST_OK || actual_generation != main_generation) {
    return code != QST_OK ? code : QST_E_STALE_OWNER;
  }
  code = qst_sql_dimension_catalog(&catalog);
  if (code == QST_OK) {
    code = qst_generator_state_get(mapping, &state);
  }
  if (code == QST_OK) {
    code = qst_generator_state_get_dataset(
        mapping, &dataset_digest, &fixture_manifest_digest,
        &object_directory_ref, &fixture_manifest_ref);
  }
  if (code != QST_OK) {
    return code;
  }
  (void)fixture_manifest_digest;
  (void)object_directory_ref;
  (void)fixture_manifest_ref;
  fd = open(output_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    return QST_E_RESOURCE;
  }
  code = state->mode == QST_MODE_RANDOM ?
             qstGeneratorDebugWriteRandom(mapping, main_generation, catalog,
                                          state, dataset_digest, fd,
                                          out_sql_count) :
             state->mode == QST_MODE_FULL ?
                 qstGeneratorDebugWriteFull(mapping, catalog, state, fd,
                                            out_sql_count) :
                 QST_E_INVALID_ARGUMENT;
  if (close(fd) != 0 && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  return code;
}
