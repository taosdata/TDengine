/*
 * Copyright (c) 2026 TAOS Data, Inc.
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */

#include "qst_shm.h"

#include <fcntl.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "qst_block.h"
#include "qst_crc32c.h"
#include "qst_process.h"
#include "qst_queue.h"
#include "qst_runner.h"
#include "qst_task.h"

static const uint8_t kQstShmMagic[8] = {'Q', 'S', 'T', 'S', 'H', 'M', '1', '2'};

/* Creates an unlinked, disk-backed file for the inherited MAP_SHARED mapping.
 * The descriptor is the only handle retained by the launcher and its children,
 * so no visible runtime artifact remains after creation. */
static int32_t qstCreateSharedMappingFile(const SQstRunConfig *config) {
  const char *directory = NULL;
  char path[PATH_MAX] = {0};
  int32_t written = 0;
  int32_t fd = -1;

  if (NULL == config) {
    return -1;
  }
  directory = config->result_dir;
  if (NULL == directory || directory[0] != '/') {
    directory = getenv("QST_RUNTIME_TEMP_ROOT");
  }
  if (NULL == directory || directory[0] != '/') {
    directory = getenv("TMPDIR");
  }
  if (NULL == directory || directory[0] != '/') {
    directory = "/tmp";
  }
  written = snprintf(path, sizeof(path), "%s/.taosQueryTest-map-XXXXXX",
                     directory);
  if (written < 0 || (uint64_t)written >= sizeof(path)) {
    return -1;
  }
  fd = mkstemp(path);
  if (fd < 0) {
    return -1;
  }
  if (unlink(path) != 0) {
    (void)close(fd);
    return -1;
  }
  return fd;
}

static void qstCloseQueueEventfds(SQstShmMapping *mapping) {
  if (NULL == mapping) {
    return;
  }
  for (uint32_t index = 0; index < 4; ++index) {
    if (mapping->queue_eventfd[index] >= 0) {
      (void)close(mapping->queue_eventfd[index]);
      mapping->queue_eventfd[index] = -1;
    }
  }
}

static int32_t qstIsLittleEndian(void) {
  const uint32_t value = 1;
  return *((const uint8_t *)&value) == 1;
}

static int32_t qstRuntimeAbiIsSupported(void) {
  _Atomic uint32_t atomic32 = 0;
  _Atomic uint64_t atomic64 = 0;
  long page_bytes = sysconf(_SC_PAGESIZE);

  return qstIsLittleEndian() && page_bytes > 0 && page_bytes % 4096 == 0 &&
         atomic_is_lock_free(&atomic32) && atomic_is_lock_free(&atomic64);
}

static int32_t qstIsAligned(uint64_t value, uint64_t alignment) {
  return alignment != 0 && value % alignment == 0;
}

static int32_t qstRegionIsValid(const SQstRegion *region, uint64_t total_bytes,
                                uint64_t minimum_offset) {
  uint64_t elements_bytes = 0;

  if (NULL == region || region->region_offset < minimum_offset ||
      !qstIsAligned(region->region_offset, QST_CACHELINE_BYTES) ||
      region->region_offset > total_bytes ||
      region->region_bytes > total_bytes - region->region_offset ||
      region->element_bytes == 0 || region->element_count == 0) {
    return 0;
  }

  if (region->element_count > UINT64_MAX / region->element_bytes) {
    return 0;
  }
  elements_bytes = (uint64_t)region->element_count * region->element_bytes;
  return elements_bytes <= region->region_bytes;
}

static int32_t qstRegionsOverlap(const SQstRegion *left,
                                 const SQstRegion *right) {
  return left->region_offset < right->region_offset + right->region_bytes &&
         right->region_offset < left->region_offset + left->region_bytes;
}

static uint32_t qstHeaderCrc(const SQstShmHeader *header) {
  SQstShmHeader copy = {0};
  const uint64_t immutable_begin = offsetof(SQstShmHeader, process_slot_bytes);
  const uint64_t immutable_end = offsetof(SQstShmHeader, header_crc32c);

  /* Runtime atomics are deliberately excluded without non-atomic reads. */
  memcpy(&copy, header, offsetof(SQstShmHeader, run_state));
  memcpy((uint8_t *)&copy + immutable_begin,
         (const uint8_t *)header + immutable_begin,
         immutable_end - immutable_begin);
  return qstCrc32c(0, &copy, sizeof(copy));
}

static EQstCode qstValidateRegionLayout(const SQstShmHeader *header) {
  const SQstRegion *regions[9] = {
      &header->process_region, &header->task_region, &header->ai_action_region,
      &header->queue_region[0], &header->queue_region[1],
      &header->queue_region[2], &header->queue_region[3],
      &header->block_region, &header->generation_state_region,
  };

  for (uint32_t index = 0; index < 9; ++index) {
    if (!qstRegionIsValid(regions[index], header->total_bytes,
                          header->header_bytes)) {
      return QST_E_SHM_CORRUPT;
    }
    for (uint32_t other = 0; other < index; ++other) {
      if (qstRegionsOverlap(regions[index], regions[other])) {
        return QST_E_SHM_CORRUPT;
      }
    }
  }

  if (header->process_region.element_bytes != sizeof(SQstProcessSlot) ||
      header->process_region.region_bytes !=
          (uint64_t)header->process_region.element_count *
              header->process_region.element_bytes ||
      header->task_region.element_bytes != sizeof(SQstTaskEntry) ||
      header->task_region.region_bytes !=
          (uint64_t)header->task_region.element_count *
              header->task_region.element_bytes ||
      header->ai_action_region.element_bytes != sizeof(SQstAiActionEntry) ||
      header->ai_action_region.region_bytes !=
          (uint64_t)header->ai_action_region.element_count *
              header->ai_action_region.element_bytes ||
      header->block_region.element_bytes < sizeof(SQstBlockHeader) ||
      header->block_region.region_bytes !=
          (uint64_t)header->block_region.element_count *
              header->block_region.element_bytes ||
      header->generation_state_region.element_bytes !=
          sizeof(SQstGenerationState) ||
      header->generation_state_region.element_count != 1 ||
      header->generation_state_region.region_bytes !=
          sizeof(SQstGenerationState)) {
    return QST_E_SHM_FORMAT;
  }

  for (uint32_t index = 0; index < 4; ++index) {
    const SQstRegion *region = &header->queue_region[index];
    if (region->element_bytes != sizeof(SQstQueueSlot) ||
        region->region_bytes != sizeof(SQstQueueDescriptor) +
                                    (uint64_t)region->element_count *
                                        region->element_bytes) {
      return QST_E_SHM_FORMAT;
    }
  }
  return QST_OK;
}

static EQstCode qstValidateHeader(const SQstShmHeader *header, uint64_t bytes,
                                  uint64_t run_id) {
  long system_page_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == header || bytes < sizeof(*header)) {
    return QST_E_SHM_FORMAT;
  }

  system_page_bytes = sysconf(_SC_PAGESIZE);
  if (!qstIsLittleEndian() || system_page_bytes <= 0 ||
      system_page_bytes % 4096 != 0 ||
      !atomic_is_lock_free(&header->run_state) ||
      !atomic_is_lock_free(&header->stop_generation) ||
      !atomic_is_lock_free(&header->stop_reason) ||
      !atomic_is_lock_free(&header->free_block_count) ||
      !atomic_is_lock_free(&header->free_task_count) ||
      !atomic_is_lock_free(&header->next_message_id) ||
      !atomic_is_lock_free(&header->next_gid) ||
      !atomic_is_lock_free(&header->next_record_seq) ||
      !atomic_is_lock_free(&header->generated_task_count) ||
      !atomic_is_lock_free(&header->persisted_task_count) ||
      !atomic_is_lock_free(&header->isolated_task_count) ||
      !atomic_is_lock_free(&header->execution_success_count) ||
      !atomic_is_lock_free(&header->execution_error_count) ||
      !atomic_is_lock_free(&header->execution_timeout_count) ||
      !atomic_is_lock_free(&header->execution_cancelled_count) ||
      !atomic_is_lock_free(&header->result_memory_count) ||
      !atomic_is_lock_free(&header->result_unavailable_count) ||
      !atomic_is_lock_free(&header->verdict_passed_count) ||
      !atomic_is_lock_free(&header->verdict_failed_count) ||
      !atomic_is_lock_free(&header->verdict_unverified_count)) {
    return QST_E_SHM_ABI;
  }

  if (memcmp(header->magic, kQstShmMagic, sizeof(kQstShmMagic)) != 0 ||
      header->format_version != QST_SHM_FORMAT_VERSION ||
      header->header_bytes != sizeof(*header) ||
      header->endian_tag != QST_ABI_ENDIAN_TAG ||
      header->page_bytes != (uint32_t)system_page_bytes ||
      header->total_bytes != bytes || header->run_id != run_id ||
      header->total_bytes % header->page_bytes != 0 ||
      header->process_slot_bytes != sizeof(SQstProcessSlot) ||
      header->task_entry_bytes != sizeof(SQstTaskEntry) ||
      header->ai_action_entry_bytes != sizeof(SQstAiActionEntry) ||
      header->queue_slot_bytes != sizeof(SQstQueueSlot) ||
      header->block_header_bytes != sizeof(SQstBlockHeader) ||
      header->launcher_pid <= 0 || header->execution_pair_count == 0 ||
      header->pair_async_window == 0 ||
      header->pair_async_window > QST_PAIR_MAX_INFLIGHT ||
      header->judge_worker_count == 0 ||
      header->statement_timeout_seconds == 0) {
    return QST_E_SHM_FORMAT;
  }

  code = qstValidateRegionLayout(header);
  if (code != QST_OK) {
    return code;
  }
  return header->header_crc32c == qstHeaderCrc(header) ? QST_OK :
                                                          QST_E_SHM_CORRUPT;
}

static EQstCode qstAlignUp(uint64_t value, uint64_t alignment,
                           uint64_t *out_value) {
  uint64_t padding = 0;

  if (NULL == out_value || alignment == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  padding = (alignment - value % alignment) % alignment;
  if (value > UINT64_MAX - padding) {
    return QST_E_RESOURCE;
  }
  *out_value = value + padding;
  return QST_OK;
}

static EQstCode qstSetRegion(SQstRegion *region, uint64_t *inout_offset,
                             uint32_t element_bytes, uint32_t element_count,
                             uint64_t prefix_bytes) {
  uint64_t element_total = 0;
  uint64_t region_bytes = 0;
  EQstCode code = QST_OK;

  if (NULL == region || NULL == inout_offset || element_bytes == 0 ||
      element_count == 0 || element_count > UINT64_MAX / element_bytes) {
    return QST_E_INVALID_ARGUMENT;
  }
  element_total = (uint64_t)element_bytes * element_count;
  if (prefix_bytes > UINT64_MAX - element_total) {
    return QST_E_RESOURCE;
  }
  region_bytes = prefix_bytes + element_total;
  code = qstAlignUp(*inout_offset, QST_CACHELINE_BYTES, inout_offset);
  if (code != QST_OK || *inout_offset > UINT64_MAX - region_bytes) {
    return code != QST_OK ? code : QST_E_RESOURCE;
  }
  region->region_offset = *inout_offset;
  region->region_bytes = region_bytes;
  region->element_bytes = element_bytes;
  region->element_count = element_count;
  *inout_offset += region_bytes;
  return QST_OK;
}

static int32_t qstIsPowerOfTwo(uint32_t value) {
  return value > 1 && (value & (value - 1)) == 0;
}

static EQstCode qstBuildLayout(const SQstRunConfig *config,
                               SQstShmHeader *header) {
  uint64_t offset = sizeof(*header);
  EQstCode code = QST_OK;

  if (NULL == config || NULL == header || config->run_id == 0 ||
      config->shm_bytes < sizeof(*header) || config->process_slot_count == 0 ||
      config->task_entry_count == 0 || config->ai_action_entry_count == 0 ||
      !qstIsPowerOfTwo(config->queue_capacity) ||
      config->block_bytes < sizeof(SQstBlockHeader) ||
      config->block_bytes % QST_CACHELINE_BYTES != 0 ||
      config->block_count == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstSetRegion(&header->process_region, &offset,
                      sizeof(SQstProcessSlot), config->process_slot_count, 0);
  if (code != QST_OK) {
    return code;
  }
  code = qstSetRegion(&header->task_region, &offset, sizeof(SQstTaskEntry),
                      config->task_entry_count, 0);
  if (code != QST_OK) {
    return code;
  }
  code = qstSetRegion(&header->ai_action_region, &offset,
                      sizeof(SQstAiActionEntry), config->ai_action_entry_count,
                      0);
  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < 4; ++index) {
    code = qstSetRegion(&header->queue_region[index], &offset,
                        sizeof(SQstQueueSlot), config->queue_capacity,
                        sizeof(SQstQueueDescriptor));
    if (code != QST_OK) {
      return code;
    }
  }
  code = qstSetRegion(&header->block_region, &offset, config->block_bytes,
                      config->block_count, 0);
  if (code != QST_OK) {
    return code;
  }
  code = qstSetRegion(&header->generation_state_region, &offset,
                      sizeof(SQstGenerationState), 1, 0);
  if (code != QST_OK) {
    return code;
  }
  return offset <= config->shm_bytes ? QST_OK : QST_E_RESOURCE;
}

static void qstInitializeQueues(SQstShmMapping *mapping) {
  for (uint32_t index = 0; index < 4; ++index) {
    SQstRegion *region = &mapping->header->queue_region[index];
    SQstQueueDescriptor *descriptor =
        (SQstQueueDescriptor *)((uint8_t *)mapping->base + region->region_offset);
    SQstQueueSlot *slots = (SQstQueueSlot *)((uint8_t *)descriptor +
                                              sizeof(*descriptor));

    descriptor->queue_id = index + 1;
    descriptor->slot_bytes = sizeof(*slots);
    descriptor->slot_offset = region->region_offset + sizeof(*descriptor);
    descriptor->capacity = region->element_count;
    descriptor->eventfd_owner_role = QST_ROLE_LAUNCHER;
    atomic_init(&descriptor->enqueue_pos, 0);
    atomic_init(&descriptor->dequeue_pos, 0);
    atomic_init(&descriptor->progress_epoch, 0);
    atomic_init(&descriptor->futex_seq, 0);
    for (uint32_t slot = 0; slot < region->element_count; ++slot) {
      atomic_init(&slots[slot].sequence, slot);
      atomic_init(&slots[slot].state, QST_SLOT_FREE);
      atomic_init(&slots[slot].owner_role, 0);
      atomic_init(&slots[slot].owner_slot_id, 0);
      atomic_init(&slots[slot].owner_generation, 0);
      atomic_init(&slots[slot].claim_generation, 0);
      atomic_init(&slots[slot].claim_seq, 0);
      atomic_init(&slots[slot].lease_deadline_mono_ns, 0);
    }
  }
}

static void qstInitializeLedgersAndBlocks(SQstShmMapping *mapping) {
  SQstProcessSlot *process_slots =
      (SQstProcessSlot *)((uint8_t *)mapping->base +
                          mapping->header->process_region.region_offset);
  SQstTaskEntry *task_entries =
      (SQstTaskEntry *)((uint8_t *)mapping->base +
                        mapping->header->task_region.region_offset);
  SQstAiActionEntry *ai_entries =
      (SQstAiActionEntry *)((uint8_t *)mapping->base +
                            mapping->header->ai_action_region.region_offset);
  uint8_t *block_base = (uint8_t *)mapping->base +
                        mapping->header->block_region.region_offset;

  for (uint32_t index = 0;
       index < mapping->header->process_region.element_count; ++index) {
    atomic_init(&process_slots[index].role, 0);
    atomic_init(&process_slots[index].generation, 0);
    atomic_init(&process_slots[index].flow_seq, 0);
    atomic_init(&process_slots[index].flow_mono_ns, 0);
    atomic_init(&process_slots[index].business_seq, 0);
    atomic_init(&process_slots[index].business_mono_ns, 0);
    atomic_init(&process_slots[index].current_gid, 0);
    atomic_init(&process_slots[index].deadline_mono_ns, 0);
    atomic_init(&process_slots[index].stop_generation, 0);
    atomic_init(&process_slots[index].exit_state, 0);
    atomic_init(&process_slots[index].flow_observed_state, 0);
    atomic_init(&process_slots[index].flow_stage, 0);
    atomic_init(&process_slots[index].flow_state_kind, 0);
  }
  atomic_init(&mapping->header->generated_task_count, 0);
  atomic_init(&mapping->header->persisted_task_count, 0);
  atomic_init(&mapping->header->isolated_task_count, 0);
  atomic_init(&mapping->header->execution_success_count, 0);
  atomic_init(&mapping->header->execution_error_count, 0);
  atomic_init(&mapping->header->execution_timeout_count, 0);
  atomic_init(&mapping->header->execution_cancelled_count, 0);
  atomic_init(&mapping->header->result_memory_count, 0);
  atomic_init(&mapping->header->result_unavailable_count, 0);
  atomic_init(&mapping->header->verdict_passed_count, 0);
  atomic_init(&mapping->header->verdict_failed_count, 0);
  atomic_init(&mapping->header->verdict_unverified_count, 0);
  atomic_init(&mapping->header->free_block_count,
              mapping->header->block_region.element_count);
  atomic_init(&mapping->header->free_task_count,
              mapping->header->task_region.element_count);
  for (uint32_t index = 0;
       index < mapping->header->task_region.element_count; ++index) {
    atomic_init(&task_entries[index].phase, 0);
    atomic_init(&task_entries[index].dispatch_epoch, 0);
    atomic_init(&task_entries[index].transition_seq, 0);
    atomic_init(&task_entries[index].owner_role, 0);
    atomic_init(&task_entries[index].owner_slot_id, 0);
    atomic_init(&task_entries[index].owner_generation, 0);
    atomic_init(&task_entries[index].timeout_attempt, 0);
    atomic_init(&task_entries[index].required_persist_mask, 0);
    atomic_init(&task_entries[index].durable_persist_mask, 0);
    atomic_init(&task_entries[index].result_storage_status, 0);
  }
  for (uint32_t index = 0;
       index < mapping->header->ai_action_region.element_count; ++index) {
    atomic_init(&ai_entries[index].state, 0);
  }
  for (uint32_t index = 0;
       index < mapping->header->block_region.element_count; ++index) {
    SQstBlockHeader *block = (SQstBlockHeader *)(
        block_base + (uint64_t)index * mapping->header->block_region.element_bytes);
    atomic_init(&block->state, QST_BLOCK_FREE);
  }
  {
    SQstGenerationState *state = (SQstGenerationState *)(
        (uint8_t *)mapping->base +
        mapping->header->generation_state_region.region_offset);

    memset(state, 0, sizeof(*state));
    atomic_init(&state->publish_seq, 0);
    atomic_init(&state->random_generation_seq, 1);
    atomic_init(&state->full_target_index, 0);
    atomic_init(&state->full_target_done_mask, 0);
  }
}

EQstCode qst_shm_create(const SQstRunConfig *config,
                        SQstShmMapping *out_mapping) {
  SQstShmMapping mapping = {0};
  SQstShmHeader *header = NULL;
  EQstCode code = QST_OK;

  if (NULL == config || NULL == out_mapping) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstRuntimeAbiIsSupported()) {
    return QST_E_SHM_ABI;
  }
  if (config->shm_bytes == 0 ||
      config->shm_bytes % (uint64_t)sysconf(_SC_PAGESIZE) != 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  memset(out_mapping, 0, sizeof(*out_mapping));
  out_mapping->shm_fd = -1;
  for (uint32_t index = 0; index < 4; ++index) {
    out_mapping->queue_eventfd[index] = -1;
  }
  mapping.shm_fd = qstCreateSharedMappingFile(config);
  if (mapping.shm_fd < 0) {
    return QST_E_RESOURCE;
  }
  if (config->shm_bytes > SIZE_MAX ||
      ftruncate(mapping.shm_fd, (off_t)config->shm_bytes) != 0) {
    (void)close(mapping.shm_fd);
    return QST_E_RESOURCE;
  }
  mapping.base = mmap(NULL, (size_t)config->shm_bytes, PROT_READ | PROT_WRITE,
                      MAP_SHARED, mapping.shm_fd, 0);
  if (mapping.base == MAP_FAILED) {
    (void)close(mapping.shm_fd);
    return QST_E_RESOURCE;
  }
  mapping.bytes = config->shm_bytes;
  mapping.header = mapping.base;
  memset(mapping.base, 0, (size_t)mapping.bytes);
  for (uint32_t index = 0; index < 4; ++index) {
    mapping.queue_eventfd[index] = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (mapping.queue_eventfd[index] < 0) {
      qstCloseQueueEventfds(&mapping);
      (void)munmap(mapping.base, (size_t)mapping.bytes);
      (void)close(mapping.shm_fd);
      return QST_E_RESOURCE;
    }
  }
  header = mapping.header;
  memcpy(header->magic, kQstShmMagic, sizeof(header->magic));
  header->format_version = QST_SHM_FORMAT_VERSION;
  header->header_bytes = sizeof(*header);
  header->endian_tag = QST_ABI_ENDIAN_TAG;
  header->page_bytes = (uint32_t)sysconf(_SC_PAGESIZE);
  header->total_bytes = mapping.bytes;
  header->run_id = config->run_id;
  header->launcher_generation = 1;
  atomic_init(&header->main_generation, 1);
  header->process_slot_bytes = sizeof(SQstProcessSlot);
  header->task_entry_bytes = sizeof(SQstTaskEntry);
  header->ai_action_entry_bytes = sizeof(SQstAiActionEntry);
  header->queue_slot_bytes = sizeof(SQstQueueSlot);
  header->block_header_bytes = sizeof(SQstBlockHeader);
  header->launcher_pid = (int32_t)getpid();
  header->execution_pair_count = config->execution_pair_count == 0 ? 1U :
                                                                  config->execution_pair_count;
  header->pair_async_window = config->pair_async_window == 0 ? 1U :
                                                              config->pair_async_window;
  header->judge_worker_count = config->judge_worker_count == 0 ? 1U :
                                                               config->judge_worker_count;
  header->statement_timeout_seconds = config->statement_timeout_seconds == 0 ?
                                          120U : config->statement_timeout_seconds;
  header->statement_retry_count = config->statement_retry_count;
  header->resource_profile_digest = config->resource_profile_digest;
  header->dependency_lock_digest = config->dependency_lock_digest;
  header->tdengine_version_digest = config->tdengine_version_digest;
  atomic_init(&header->run_state, QST_RUN_PREPARING);
  atomic_init(&header->stop_generation, 0);
  atomic_init(&header->stop_reason, QST_STOP_NONE);
  atomic_init(&header->next_message_id, 1);
  atomic_init(&header->next_gid, 1);
  atomic_init(&header->next_record_seq, 1);
  code = qstBuildLayout(config, header);
  if (code == QST_OK) {
    SQstGenerationState *generation_state =
        (SQstGenerationState *)((uint8_t *)mapping.base +
            header->generation_state_region.region_offset);

    qstInitializeLedgersAndBlocks(&mapping);
    generation_state->pending_quota_bytes = config->pending_quota_bytes;
    generation_state->log_rotation_bytes = config->log_rotation_bytes;
    qstInitializeQueues(&mapping);
    header->header_crc32c = qstHeaderCrc(header);
    code = qstValidateHeader(header, mapping.bytes, config->run_id);
  }
  if (code != QST_OK) {
    qstCloseQueueEventfds(&mapping);
    (void)munmap(mapping.base, (size_t)mapping.bytes);
    (void)close(mapping.shm_fd);
    return code;
  }
  *out_mapping = mapping;
  return QST_OK;
}

EQstCode qst_shm_map_validate(int32_t shm_fd, uint64_t run_id,
                              SQstShmMapping *out_mapping) {
  struct stat stat_buf = {0};
  void *base = NULL;
  int32_t fd_flags = 0;
  int32_t mapping_fd = -1;
  EQstCode code = QST_OK;

  if (shm_fd < 0 || run_id == 0 || NULL == out_mapping) {
    return QST_E_INVALID_ARGUMENT;
  }

  memset(out_mapping, 0, sizeof(*out_mapping));
  out_mapping->shm_fd = -1;
  for (uint32_t index = 0; index < 4; ++index) {
    out_mapping->queue_eventfd[index] = -1;
  }

  mapping_fd = fcntl(shm_fd, F_DUPFD, 0);
  if (mapping_fd < 0) {
    return QST_E_RESOURCE;
  }
  fd_flags = fcntl(mapping_fd, F_GETFD);
  if (fd_flags < 0 || fcntl(mapping_fd, F_SETFD, fd_flags | FD_CLOEXEC) != 0) {
    (void)close(mapping_fd);
    return QST_E_RESOURCE;
  }
  if (fstat(mapping_fd, &stat_buf) != 0 || stat_buf.st_size <= 0 ||
      (uint64_t)stat_buf.st_size > SIZE_MAX) {
    (void)close(mapping_fd);
    return QST_E_SHM_FORMAT;
  }

  base = mmap(NULL, (size_t)stat_buf.st_size, PROT_READ | PROT_WRITE,
              MAP_SHARED, mapping_fd, 0);
  if (base == MAP_FAILED) {
    (void)close(mapping_fd);
    return QST_E_RESOURCE;
  }

  code = qstValidateHeader((const SQstShmHeader *)base,
                           (uint64_t)stat_buf.st_size, run_id);
  if (code != QST_OK) {
    (void)munmap(base, (size_t)stat_buf.st_size);
    (void)close(mapping_fd);
    return code;
  }

  out_mapping->shm_fd = mapping_fd;
  out_mapping->base = base;
  out_mapping->bytes = (uint64_t)stat_buf.st_size;
  out_mapping->header = base;
  return QST_OK;
}

EQstCode qst_shm_validate_ref(const SQstShmMapping *mapping, SQstRef ref) {
  const SQstRegion *region = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstRefIsEmpty(ref)) {
    return QST_OK;
  }

  region = &mapping->header->block_region;
  if (ref.length == 0 || ref.offset < region->region_offset ||
      ref.offset > mapping->bytes ||
      ref.offset >= region->region_offset + region->region_bytes ||
      ref.length > mapping->bytes - ref.offset) {
    return QST_E_BLOCK_INVALID;
  }
  return QST_OK;
}

EQstCode qst_shm_set_dimension_catalog(SQstShmMapping *mapping,
                                       uint32_t catalog_version,
                                       SQstDigest256 catalog_digest) {
  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      catalog_version == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (atomic_load_explicit(&mapping->header->run_state, memory_order_acquire) !=
      QST_RUN_PREPARING) {
    return QST_E_STALE_OWNER;
  }
  mapping->header->dimension_catalog_version = catalog_version;
  mapping->header->dimension_catalog_digest = catalog_digest;
  mapping->header->header_crc32c = qstHeaderCrc(mapping->header);
  return QST_OK;
}

EQstCode qst_shm_set_tdengine_version_digest(SQstShmMapping *mapping,
                                             SQstDigest256 version_digest) {
  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (atomic_load_explicit(&mapping->header->run_state, memory_order_acquire) !=
      QST_RUN_RUNNING) {
    return QST_E_STALE_OWNER;
  }
  if (memcmp(mapping->header->tdengine_version_digest.bytes,
             ((SQstDigest256){{0}}).bytes,
             sizeof(mapping->header->tdengine_version_digest.bytes)) != 0) {
    return QST_E_STALE_OWNER;
  }
  mapping->header->tdengine_version_digest = version_digest;
  mapping->header->header_crc32c = qstHeaderCrc(mapping->header);
  return QST_OK;
}

EQstCode qst_shm_unmap(SQstShmMapping *mapping) {
  EQstCode code = QST_OK;

  if (NULL == mapping) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (NULL != mapping->base && mapping->bytes > 0 &&
      munmap(mapping->base, (size_t)mapping->bytes) != 0) {
    code = QST_E_RESOURCE;
  }
  if (mapping->shm_fd >= 0 && close(mapping->shm_fd) != 0) {
    code = QST_E_RESOURCE;
  }
  qstCloseQueueEventfds(mapping);

  memset(mapping, 0, sizeof(*mapping));
  mapping->shm_fd = -1;
  for (uint32_t index = 0; index < 4; ++index) {
    mapping->queue_eventfd[index] = -1;
  }
  return code;
}
