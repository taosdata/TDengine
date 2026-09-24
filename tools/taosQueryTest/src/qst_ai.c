/* Shared AI action ledger with deterministic action IDs and generation checks. */

#define _POSIX_C_SOURCE 200809L

#include "qst_ai.h"

#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "qst_block.h"
#include "qst_process.h"
#include "qst_sha256.h"

static EQstCode qstAiEntries(SQstShmMapping *mapping,
                             SQstAiActionEntry **out_entries,
                             uint32_t *out_count) {
  const SQstRegion *region = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      NULL == out_entries || NULL == out_count) {
    return QST_E_INVALID_ARGUMENT;
  }
  region = &mapping->header->ai_action_region;
  if (region->element_bytes != sizeof(SQstAiActionEntry) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return QST_E_SHM_CORRUPT;
  }
  *out_entries = (SQstAiActionEntry *)((uint8_t *)mapping->base +
                                        region->region_offset);
  *out_count = region->element_count;
  return QST_OK;
}

static int64_t qstAiRealtimeNowNs(void) {
  struct timespec now = {0};

  if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > INT64_MAX / UINT64_C(1000000000)) {
    return 0;
  }
  return (int64_t)((uint64_t)now.tv_sec * UINT64_C(1000000000) +
                   (uint64_t)now.tv_nsec);
}

static void qstAiHashLe64(SQstSha256 *hash, uint64_t value) {
  uint8_t bytes[8] = {0};

  for (uint32_t index = 0; index < sizeof(bytes); ++index) {
    bytes[index] = (uint8_t)(value >> (index * 8U));
  }
  qst_sha256_update(hash, bytes, sizeof(bytes));
}

static void qstAiHashLe32(SQstSha256 *hash, uint32_t value) {
  uint8_t bytes[4] = {0};

  for (uint32_t index = 0; index < sizeof(bytes); ++index) {
    bytes[index] = (uint8_t)(value >> (index * 8U));
  }
  qst_sha256_update(hash, bytes, sizeof(bytes));
}

static int32_t qstAiEntryIsAllowed(EQstAiEntry entry, uint64_t gid) {
  if (entry < QST_AI_ENTRY_MANUAL_UNVERIFIED ||
      entry > QST_AI_ENTRY_MANUAL_ALL) {
    return 0;
  }
  return gid != 0;
}

static int32_t qstAiWorkerIsCurrent(const SQstShmMapping *mapping,
                                    uint32_t generation) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (NULL == mapping || NULL == mapping->header || NULL == mapping->base ||
      generation == 0) {
    return 0;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return 0;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) ==
            QST_ROLE_AI_WORKER &&
        slots[index].slot_id == 0 &&
        atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
            generation &&
        atomic_load_explicit(&slots[index].exit_state, memory_order_acquire) ==
            QST_PROCESS_RUNNING) {
      return 1;
    }
  }
  return 0;
}

static SQstAiActionEntry *qstAiFindAction(SQstAiActionEntry *entries,
                                          uint32_t count,
                                          SQstDigest256 action_id) {
  for (uint32_t index = 0; index < count; ++index) {
    if (atomic_load_explicit(&entries[index].state, memory_order_acquire) != 0 &&
        memcmp(entries[index].action_id.bytes, action_id.bytes,
               sizeof(action_id.bytes)) == 0) {
      return &entries[index];
    }
  }
  return NULL;
}

EQstCode qst_ai_precheck(SQstShmMapping *mapping, uint64_t gid,
                         EQstAiEntry entry, SQstRef prompt_ref,
                         SQstRef material_manifest_ref,
                         SQstDigest256 *out_action_id) {
  SQstAiActionEntry *entries = NULL;
  SQstAiActionEntry *existing = NULL;
  SQstSha256 action_hash = {0};
  SQstDigest256 prompt_sha = {0};
  SQstDigest256 material_sha = {0};
  SQstDigest256 action_id = {0};
  uint32_t count = 0;
  int64_t created_ts_ns = 0;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_action_id || !qstAiEntryIsAllowed(entry, gid) ||
      qstRefIsEmpty(prompt_ref) || qstRefIsEmpty(material_manifest_ref)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qst_block_sha256(mapping, prompt_ref, gid, &prompt_sha) != QST_OK ||
      qst_block_sha256(mapping, material_manifest_ref, gid, &material_sha) !=
          QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  qst_sha256_init(&action_hash);
  qstAiHashLe64(&action_hash, mapping->header->run_id);
  qstAiHashLe64(&action_hash, gid);
  qstAiHashLe32(&action_hash, (uint32_t)entry);
  qst_sha256_update(&action_hash, prompt_sha.bytes, sizeof(prompt_sha.bytes));
  qst_sha256_update(&action_hash, material_sha.bytes, sizeof(material_sha.bytes));
  qst_sha256_final(&action_hash, action_id.bytes);
  code = qstAiEntries(mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  existing = qstAiFindAction(entries, count, action_id);
  if (NULL != existing) {
    const uint32_t state = atomic_load_explicit(&existing->state,
                                                memory_order_acquire);

    if (state == QST_AI_REGISTERING) {
      return QST_E_QUEUE_FULL;
    }
    if (existing->gid != gid || existing->entry != (uint32_t)entry ||
        memcmp(existing->prompt_sha256.bytes, prompt_sha.bytes,
               sizeof(prompt_sha.bytes)) != 0 ||
        memcmp(existing->material_manifest_sha256.bytes, material_sha.bytes,
               sizeof(material_sha.bytes)) != 0 ||
        existing->material_manifest_ref.offset != material_manifest_ref.offset ||
        existing->material_manifest_ref.length != material_manifest_ref.length ||
        existing->material_manifest_ref.crc32c != material_manifest_ref.crc32c) {
      return QST_E_SHM_CORRUPT;
    }
    *out_action_id = action_id;
    return QST_OK;
  }
  created_ts_ns = qstAiRealtimeNowNs();
  if (created_ts_ns == 0) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < count; ++index) {
    uint32_t expected_state = 0;

    if (!atomic_compare_exchange_strong_explicit(
            &entries[index].state, &expected_state, QST_AI_REGISTERING,
            memory_order_acq_rel, memory_order_acquire)) {
      continue;
    }
    entries[index].action_id = action_id;
    entries[index].gid = gid;
    entries[index].entry = (uint32_t)entry;
    entries[index].owner_generation = 0;
    entries[index].action_code = QST_OK;
    entries[index].created_ts_ns = created_ts_ns;
    entries[index].completed_ts_ns = 0;
    entries[index].prompt_sha256 = prompt_sha;
    entries[index].material_manifest_sha256 = material_sha;
    entries[index].material_manifest_ref = material_manifest_ref;
    memset(&entries[index].analysis_ref, 0, sizeof(entries[index].analysis_ref));
    memset(&entries[index].remote_issue_ref, 0,
           sizeof(entries[index].remote_issue_ref));
    memset(entries[index].reserved, 0, sizeof(entries[index].reserved));
    atomic_store_explicit(&entries[index].state, QST_AI_PRECHECKED,
                          memory_order_release);
    *out_action_id = action_id;
    return QST_OK;
  }
  return QST_E_RESOURCE;
}

EQstCode qst_ai_run_action(SQstShmMapping *mapping, SQstDigest256 action_id,
                           uint32_t worker_generation) {
  SQstAiActionEntry *entries = NULL;
  SQstAiActionEntry *action = NULL;
  uint32_t count = 0;
  uint32_t expected_state = QST_AI_PRECHECKED;
  EQstCode code = qstAiEntries(mapping, &entries, &count);

  if (code != QST_OK || worker_generation == 0 ||
      !qstAiWorkerIsCurrent(mapping, worker_generation)) {
    return code != QST_OK ? code : QST_E_STALE_OWNER;
  }
  action = qstAiFindAction(entries, count, action_id);
  if (NULL == action) {
    return QST_E_QUEUE_EMPTY;
  }
  if (atomic_compare_exchange_strong_explicit(
          &action->state, &expected_state, QST_AI_REGISTERING,
          memory_order_acq_rel, memory_order_acquire)) {
    action->owner_generation = worker_generation;
    atomic_store_explicit(&action->state, QST_AI_RUNNING, memory_order_release);
    return QST_OK;
  }
  if (expected_state == QST_AI_RUNNING &&
      action->owner_generation == worker_generation) {
    return QST_OK;
  }
  return expected_state == QST_AI_SUCCEEDED || expected_state == QST_AI_FAILED ||
                 expected_state == QST_AI_ABANDONED ?
             QST_OK : QST_E_STALE_OWNER;
}

EQstCode qst_ai_publish_result(SQstShmMapping *mapping,
                               SQstDigest256 action_id, SQstRef analysis_ref,
                               EQstCode action_code,
                               uint32_t worker_generation) {
  SQstAiActionEntry *entries = NULL;
  SQstAiActionEntry *action = NULL;
  uint32_t count = 0;
  uint32_t expected_state = QST_AI_RUNNING;
  uint32_t final_state = 0;
  int64_t completed_ts_ns = 0;
  EQstCode code = qstAiEntries(mapping, &entries, &count);

  if (code != QST_OK || worker_generation == 0 ||
      !qstAiWorkerIsCurrent(mapping, worker_generation)) {
    return code != QST_OK ? code : QST_E_STALE_OWNER;
  }
  action = qstAiFindAction(entries, count, action_id);
  if (NULL == action || action->owner_generation != worker_generation) {
    return QST_E_STALE_OWNER;
  }
  if (!qstRefIsEmpty(analysis_ref) &&
      qst_block_validate(mapping, analysis_ref, action->gid) != QST_OK) {
    return QST_E_BLOCK_INVALID;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &action->state, &expected_state, QST_AI_REGISTERING,
          memory_order_acq_rel, memory_order_acquire)) {
    return expected_state == QST_AI_SUCCEEDED || expected_state == QST_AI_FAILED ||
                   expected_state == QST_AI_ABANDONED ?
               QST_OK : QST_E_STALE_OWNER;
  }
  completed_ts_ns = qstAiRealtimeNowNs();
  if (completed_ts_ns == 0) {
    atomic_store_explicit(&action->state, QST_AI_RUNNING, memory_order_release);
    return QST_E_RESOURCE;
  }
  final_state = action_code == QST_OK ? QST_AI_SUCCEEDED :
      action_code == QST_E_STOPPING ? QST_AI_ABANDONED : QST_AI_FAILED;
  action->analysis_ref = analysis_ref;
  action->action_code = action_code;
  action->completed_ts_ns = completed_ts_ns;
  atomic_store_explicit(&action->state, final_state, memory_order_release);
  return QST_OK;
}

EQstCode qst_ai_get_action_gid(const SQstShmMapping *mapping,
                               SQstDigest256 action_id, uint64_t *out_gid) {
  SQstAiActionEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = qstAiEntries((SQstShmMapping *)mapping, &entries, &count);

  if (code != QST_OK || NULL == out_gid) {
    return code != QST_OK ? code : QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < count; ++index) {
    if (memcmp(entries[index].action_id.bytes, action_id.bytes,
               sizeof(action_id.bytes)) == 0 &&
        atomic_load_explicit(&entries[index].state, memory_order_acquire) != 0) {
      *out_gid = entries[index].gid;
      return QST_OK;
    }
  }
  return QST_E_QUEUE_EMPTY;
}

EQstCode qst_ai_copy_action_analysis(const SQstShmMapping *mapping,
                                     SQstDigest256 action_id,
                                     uint8_t **out_bytes,
                                     uint32_t *out_length) {
  SQstAiActionEntry *entries = NULL;
  uint32_t count = 0;
  EQstCode code = QST_OK;

  if (NULL == out_bytes || NULL == out_length) {
    return QST_E_INVALID_ARGUMENT;
  }
  *out_bytes = NULL;
  *out_length = 0;
  code = qstAiEntries((SQstShmMapping *)mapping, &entries, &count);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < count; ++index) {
    if (memcmp(entries[index].action_id.bytes, action_id.bytes,
               sizeof(action_id.bytes)) == 0 &&
        atomic_load_explicit(&entries[index].state, memory_order_acquire) != 0) {
      if (qstRefIsEmpty(entries[index].analysis_ref)) {
        return QST_OK;
      }
      if (entries[index].analysis_ref.length == 0 ||
          entries[index].analysis_ref.length >= UINT32_MAX) {
        return QST_E_SHM_CORRUPT;
      }
      *out_bytes = (uint8_t *)calloc(entries[index].analysis_ref.length + 1U, 1);
      if (NULL == *out_bytes) {
        return QST_E_RESOURCE;
      }
      code = qst_block_copy((SQstShmMapping *)mapping, entries[index].analysis_ref,
                            entries[index].gid, 0, *out_bytes,
                            entries[index].analysis_ref.length);
      if (code != QST_OK) {
        free(*out_bytes);
        *out_bytes = NULL;
        return code;
      }
      *out_length = entries[index].analysis_ref.length;
      return QST_OK;
    }
  }
  return QST_E_QUEUE_EMPTY;
}
