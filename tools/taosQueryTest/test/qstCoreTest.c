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

#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <arpa/inet.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/eventfd.h>
#include <sys/file.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "qst_ai.h"
#include "qst_ai_runner.h"
#include "qst_audit.h"
#include "qst_block.h"
#include "qst_crc32c.h"
#include "qst_dataset.h"
#include "qst_generator.h"
#include "qst_process.h"
#include "qst_provider.h"
#include "qst_queue.h"
#include "qst_report.h"
#include "qst_result.h"
#include "qst_result_store.h"
#include "qst_runner.h"
#include "qst_mapping.h"
#include "qst_native.h"
#include "qst_supervisor.h"
#include "qst_task.h"
#include "qst_sha256.h"
#include "qst_sql_dimension_config.h"
#include "sql_dimension_ids.h"

void qstTestConfigParsing(void);
void qstTestResourceCapacityCalculation(void);
void qstTestSha256(void);
void qstTestLauncherLifecycle(void);
void qstTestLauncherFailureReleasesRunLock(void);
void qstTestLauncherHoldsRunLock(void);
void qstTestLauncherStopDrainsPersistence(void);
void qstTestLauncherAllowsMainStartupLease(void);
void qstTestLauncherAllowsMainShutdownLease(void);
void qstTestLauncherPairSupervisorCrashRecovery(void);
void qstTestLauncherPairSupervisorStopCrashFailsRun(void);
void qstTestLauncherExecManagerCrashFailsRun(void);
void qstTestLauncherJudgeWorkerCrashFailsRun(void);
void qstTestDependencyLockPreflight(void);
static SQstRef qstSealTestBytes(SQstShmMapping *mapping, const uint8_t *bytes,
                                uint32_t byte_count);
static SQstRef qstSealTestBytesForOwner(SQstShmMapping *mapping,
                                        uint64_t owner_gid,
                                        const uint8_t *bytes,
                                        uint32_t byte_count);
static void qstTestCompiledSqlDimensionCatalog(
    const SQstGeneratorRuntimeContext *runtime);
static void qstTestCompiledSqlDimensionCatalogReachability(void);
static void qstTestCompiledSqlDimensionCatalogFull(
    const SQstGeneratorRuntimeContext *runtime);
static void qstTestCompiledSqlDimensionCatalogRandomStress(
    const SQstGeneratorRuntimeContext *runtime);
static void qstTestGeneratorSqlCapacityFallback(void);
static void qstTestAuditCli(void);

enum {
  kTestBlockOffset = 1024,
  kTestBlockBytes = 128,
  kTestBlockCount = 16,
  kTestTaskOffset = 3072,
  kTestTaskCount = 2,
  kTestProcessOffset = 3840,
  kTestProcessCount = 5,
  kTestQueueOffset = 4800,
  kTestQueueCapacity = 2,
  kTestAiActionOffset = 8192,
  kTestAiActionCount = 2,
  kTestMemoryBytes = 16384,
  kTestPairDeadlineEventTimeoutMilliseconds = 30000,
  kTestPairPortBlockBytes = 16,
  kTestPairPortMin = 20000,
  kTestPairPortBlockCount = 2500,
};

static void qstTestSaveEnvironment(const char *name, char *out_value,
                                   uint64_t out_bytes, int32_t *out_present) {
  const char *value = NULL;
  int32_t written = 0;

  assert(name != NULL && out_value != NULL && out_bytes > 0 &&
         out_present != NULL);
  value = getenv(name);
  *out_present = value != NULL;
  out_value[0] = '\0';
  if (*out_present) {
    written = snprintf(out_value, (size_t)out_bytes, "%s", value);
    assert(written >= 0 && (uint64_t)written < out_bytes);
  }
}

static void qstTestRestoreEnvironment(const char *name, const char *value,
                                      int32_t present) {
  assert(name != NULL && value != NULL);
  if (present) {
    assert(setenv(name, value, 1) == 0);
  } else {
    assert(unsetenv(name) == 0);
  }
}

static int32_t qstTestFileContains(FILE *file, const char *needle) {
  char *line = NULL;
  size_t line_capacity = 0;
  ssize_t line_bytes = 0;
  int32_t found = 0;

  assert(file != NULL && needle != NULL && needle[0] != '\0');
  rewind(file);
  while ((line_bytes = getline(&line, &line_capacity, file)) >= 0) {
    if (strstr(line, needle) != NULL) {
      found = 1;
      break;
    }
  }
  free(line);
  return found;
}

/* Counts complete line matches in a test-only append log. */
static uint32_t qstTestFileLineCount(FILE *file, const char *needle) {
  char *line = NULL;
  size_t line_capacity = 0;
  ssize_t line_bytes = 0;
  uint32_t count = 0;

  assert(file != NULL && needle != NULL && needle[0] != '\0');
  rewind(file);
  while ((line_bytes = getline(&line, &line_capacity, file)) >= 0) {
    if (strstr(line, needle) != NULL) {
      assert(count != UINT32_MAX);
      ++count;
    }
  }
  free(line);
  return count;
}

static const char *qstTestRuntimeTempRoot(void) {
  const char *path = getenv("QST_RUNTIME_TEMP_ROOT");

  if (path != NULL && path[0] == '/') {
    return path;
  }
  path = getenv("TMPDIR");
  if (path != NULL && path[0] == '/') {
    return path;
  }
  return "/tmp";
}

/* Appends an observation while preserving complete records across test processes. */
static void qstTestAppendPortLeaseObservation(uint16_t port) {
  const char *path = getenv("QST_TEST_PORT_LEASE_PORT_LOG");
  char line[64] = {0};
  int32_t fd = -1;
  int32_t written = 0;

  if (NULL == path || path[0] == '\0') {
    return;
  }
  written = snprintf(line, sizeof(line), "%u\n", (unsigned int)port);
  assert(written > 0 && (uint64_t)written < sizeof(line));
  fd = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0600);
  assert(fd >= 0);
  assert(flock(fd, LOCK_EX) == 0);
  assert(write(fd, line, (size_t)written) == written);
  assert(flock(fd, LOCK_UN) == 0);
  assert(close(fd) == 0);
}

static int32_t qstTestPortBlockAvailable(uint16_t port) {
  int32_t sockets[kTestPairPortBlockBytes] = {0};
  int32_t available = 1;

  for (uint32_t index = 0; index < kTestPairPortBlockBytes; ++index) {
    sockets[index] = -1;
  }
  for (uint32_t index = 0; index < kTestPairPortBlockBytes; ++index) {
    struct sockaddr_in address = {0};

    sockets[index] = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sockets[index] < 0) {
      available = 0;
      break;
    }
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)(port + index));
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(sockets[index], (const struct sockaddr *)&address,
             sizeof(address)) != 0) {
      available = 0;
      break;
    }
  }
  for (uint32_t index = 0; index < kTestPairPortBlockBytes; ++index) {
    if (sockets[index] >= 0) {
      assert(close(sockets[index]) == 0);
    }
  }
  return available;
}

static int32_t qstTestBindLoopbackPort(uint16_t port) {
  struct sockaddr_in address = {0};
  int32_t socket_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

  assert(socket_fd >= 0);
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  assert(bind(socket_fd, (const struct sockaddr *)&address, sizeof(address)) == 0);
  return socket_fd;
}

static void qstInitQueue(SQstShmMapping *mapping, uint32_t index,
                         uint64_t offset) {
  SQstRegion *region = &mapping->header->queue_region[index];
  SQstQueueDescriptor *descriptor =
      (SQstQueueDescriptor *)((uint8_t *)mapping->base + offset);
  SQstQueueSlot *slots = (SQstQueueSlot *)((uint8_t *)descriptor +
                                            sizeof(*descriptor));

  region->region_offset = offset;
  region->element_bytes = sizeof(*slots);
  region->element_count = kTestQueueCapacity;
  region->region_bytes = sizeof(*descriptor) +
                       (uint64_t)region->element_bytes * region->element_count;
  descriptor->queue_id = index + 1;
  descriptor->slot_bytes = sizeof(*slots);
  descriptor->slot_offset = offset + sizeof(*descriptor);
  descriptor->capacity = kTestQueueCapacity;
  for (uint32_t slot = 0; slot < kTestQueueCapacity; ++slot) {
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

static void qstInitMapping(SQstShmMapping *mapping, uint8_t *memory,
                           uint64_t memory_bytes) {
  SQstShmHeader *header = (SQstShmHeader *)memory;

  memset(memory, 0, memory_bytes);
  memset(mapping, 0, sizeof(*mapping));
  mapping->base = memory;
  mapping->bytes = memory_bytes;
  mapping->header = header;
  header->run_id = 101;
  header->launcher_generation = 1;
  atomic_init(&header->run_state, QST_RUN_PREPARING);
  atomic_init(&header->stop_generation, 0);
  atomic_init(&header->stop_reason, QST_STOP_NONE);
  atomic_init(&header->next_message_id, 1);
  atomic_init(&header->next_gid, 1);
  atomic_init(&header->next_record_seq, 1);
  atomic_init(&header->main_generation, 1);
  atomic_init(&header->generated_task_count, 0);
  atomic_init(&header->persisted_task_count, 0);
  atomic_init(&header->isolated_task_count, 0);
  atomic_init(&header->execution_success_count, 0);
  atomic_init(&header->execution_error_count, 0);
  atomic_init(&header->execution_timeout_count, 0);
  atomic_init(&header->execution_cancelled_count, 0);
  atomic_init(&header->result_memory_count, 0);
  atomic_init(&header->result_unavailable_count, 0);
  atomic_init(&header->verdict_passed_count, 0);
  atomic_init(&header->verdict_failed_count, 0);
  atomic_init(&header->verdict_unverified_count, 0);
  header->block_region.region_offset = kTestBlockOffset;
  header->block_region.element_bytes = kTestBlockBytes;
  header->block_region.element_count = kTestBlockCount;
  header->block_region.region_bytes =
      (uint64_t)header->block_region.element_bytes *
      header->block_region.element_count;
  header->task_region.region_offset = kTestTaskOffset;
  header->task_region.element_bytes = sizeof(SQstTaskEntry);
  header->task_region.element_count = kTestTaskCount;
  header->task_region.region_bytes =
      (uint64_t)sizeof(SQstTaskEntry) * kTestTaskCount;
  header->process_region.region_offset = kTestProcessOffset;
  header->process_region.element_bytes = sizeof(SQstProcessSlot);
  header->process_region.element_count = kTestProcessCount;
  header->process_region.region_bytes =
      (uint64_t)header->process_region.element_bytes *
      header->process_region.element_count;
  header->ai_action_region.region_offset = kTestAiActionOffset;
  header->ai_action_region.element_bytes = sizeof(SQstAiActionEntry);
  header->ai_action_region.element_count = kTestAiActionCount;
  header->ai_action_region.region_bytes =
      (uint64_t)header->ai_action_region.element_bytes *
      header->ai_action_region.element_count;
  for (uint32_t index = 0; index < header->process_region.element_count;
       ++index) {
    SQstProcessSlot *slot = (SQstProcessSlot *)(
        memory + kTestProcessOffset + index * sizeof(SQstProcessSlot));
    atomic_init(&slot->role, 0);
    atomic_init(&slot->generation, 0);
    atomic_init(&slot->flow_seq, 0);
    atomic_init(&slot->flow_mono_ns, 0);
    atomic_init(&slot->business_seq, 0);
    atomic_init(&slot->business_mono_ns, 0);
    atomic_init(&slot->current_gid, 0);
    atomic_init(&slot->deadline_mono_ns, 0);
    atomic_init(&slot->stop_generation, 0);
    atomic_init(&slot->exit_state, 0);
    atomic_init(&slot->flow_observed_state, 0);
    atomic_init(&slot->flow_stage, 0);
    atomic_init(&slot->flow_state_kind, 0);
  }
  for (uint32_t index = 0; index < header->task_region.element_count; ++index) {
    SQstTaskEntry *entry = (SQstTaskEntry *)(
        memory + kTestTaskOffset + index * sizeof(SQstTaskEntry));
    atomic_init(&entry->phase, 0);
    atomic_init(&entry->dispatch_epoch, 0);
    atomic_init(&entry->transition_seq, 0);
    atomic_init(&entry->owner_role, 0);
    atomic_init(&entry->owner_slot_id, 0);
    atomic_init(&entry->owner_generation, 0);
    atomic_init(&entry->timeout_attempt, 0);
    atomic_init(&entry->required_persist_mask, 0);
    atomic_init(&entry->durable_persist_mask, 0);
    atomic_init(&entry->result_storage_status, 0);
  }
  for (uint32_t index = 0; index < header->ai_action_region.element_count;
       ++index) {
    SQstAiActionEntry *entry = (SQstAiActionEntry *)(
        memory + kTestAiActionOffset + index * sizeof(SQstAiActionEntry));

    atomic_init(&entry->state, 0);
  }
  for (uint32_t index = 0; index < header->block_region.element_count; ++index) {
    SQstBlockHeader *block = (SQstBlockHeader *)(
        memory + kTestBlockOffset + index * kTestBlockBytes);
    atomic_init(&block->state, QST_BLOCK_FREE);
  }
  atomic_init(&header->free_block_count, kTestBlockCount);
  atomic_init(&header->free_task_count, kTestTaskCount);
  for (uint32_t index = 0; index < 4; ++index) {
    qstInitQueue(mapping, index,
                 kTestQueueOffset + index *
                     (sizeof(SQstQueueDescriptor) +
                      kTestQueueCapacity * sizeof(SQstQueueSlot)));
  }
}

static SQstTaskEntry *qstTestFindTaskByGid(const SQstShmMapping *mapping,
                                           uint64_t gid) {
  SQstTaskEntry *entries = NULL;

  assert(mapping != NULL && mapping->header != NULL && mapping->base != NULL &&
         gid != 0);
  entries = (SQstTaskEntry *)((uint8_t *)mapping->base +
                              mapping->header->task_region.region_offset);
  for (uint32_t index = 0;
       index < mapping->header->task_region.element_count; ++index) {
    if (entries[index].gid == gid &&
        atomic_load_explicit(&entries[index].phase, memory_order_acquire) != 0) {
      return &entries[index];
    }
  }
  return NULL;
}

static void qstTestMultiBlockRoundTripAndRelease(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  uint8_t payload[200] = {0};
  uint8_t batch_payload[80] = {0};
  SQstShmMapping mapping = {0};
  SQstRef ref = {0};
  SQstRef batch_refs[2] = {{0}};
  SQstBlockHeader *first = NULL;
  SQstBlockHeader *second = NULL;
  uint32_t free_block_count = 0;

  for (uint32_t index = 0; index < sizeof(payload); ++index) {
    payload[index] = (uint8_t)(index * 13U + 5U);
  }
  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_block_free_count(&mapping, &free_block_count) == QST_OK);
  assert(free_block_count == kTestBlockCount);
  assert(qst_block_alloc(&mapping, 42, 1, sizeof(payload), &ref) == QST_OK);
  assert(qst_block_free_count(&mapping, &free_block_count) == QST_OK);
  assert(free_block_count < kTestBlockCount);
  first = (SQstBlockHeader *)(memory + ref.offset);
  second = (SQstBlockHeader *)(memory + first->next_block_offset);
  assert(first->next_block_offset != 0);
  assert(second->next_block_offset != 0);
  assert(qst_block_write_seal(&mapping, &ref, payload, sizeof(payload)) == QST_OK);
  assert(atomic_load_explicit(&first->state, memory_order_acquire) ==
         QST_BLOCK_SEALED);
  assert(qst_block_validate(&mapping, ref, 42) == QST_OK);
  assert(qst_block_validate(&mapping, ref, 43) == QST_E_BLOCK_INVALID);
  assert(qst_block_release(&mapping, ref, 42) == QST_OK);
  assert(qst_block_free_count(&mapping, &free_block_count) == QST_OK);
  assert(free_block_count == kTestBlockCount);
  assert(atomic_load_explicit(&first->state, memory_order_acquire) ==
         QST_BLOCK_FREE);
  assert(first->payload_capacity == 0);
  assert(first->owner_gid == 0);
  assert(first->owner_generation == 0);
  assert(first->payload_length == 0);
  assert(first->next_block_offset == 0);
  assert(first->payload_crc32c == 0);
  assert(first->header_crc32c == 0);
  assert(qst_block_validate(&mapping, ref, 42) == QST_E_BLOCK_INVALID);
  assert(qst_block_alloc(&mapping, 42, 1, sizeof(batch_payload),
                         &batch_refs[0]) == QST_OK);
  assert(qst_block_alloc(&mapping, 42, 1, sizeof(batch_payload),
                         &batch_refs[1]) == QST_OK);
  assert(qst_block_write_seal(&mapping, &batch_refs[0], batch_payload,
                              sizeof(batch_payload)) == QST_OK);
  assert(qst_block_write_seal(&mapping, &batch_refs[1], batch_payload,
                              sizeof(batch_payload)) == QST_OK);
  assert(qst_block_release_many(&mapping, batch_refs, 2, 42) == QST_OK);
  assert(qst_block_free_count(&mapping, &free_block_count) == QST_OK);
  assert(free_block_count == kTestBlockCount);
  assert(qst_block_validate(&mapping, batch_refs[0], 42) == QST_E_BLOCK_INVALID);
  assert(qst_block_validate(&mapping, batch_refs[1], 42) == QST_E_BLOCK_INVALID);
}

static void qstTestBlockAllocationAndCorruptionFailures(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  uint8_t payload[200] = {0};
  SQstShmMapping mapping = {0};
  SQstRef ref = {0};
  SQstBlockHeader *first = NULL;
  SQstBlockHeader *second = NULL;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_block_alloc(&mapping, 7, 1,
                         kTestBlockCount * (kTestBlockBytes -
                                            sizeof(SQstBlockHeader)) + 1,
                         &ref) == QST_E_BLOCK_NO_SPACE);
  assert(qst_block_alloc(&mapping, 7, 1, sizeof(payload), &ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &ref, payload, sizeof(payload)) == QST_OK);
  first = (SQstBlockHeader *)(memory + ref.offset);
  second = (SQstBlockHeader *)(memory + first->next_block_offset);
  ((uint8_t *)first + sizeof(*first))[0] ^= 1U;
  assert(qst_block_validate(&mapping, ref, 7) == QST_E_BLOCK_INVALID);
  ((uint8_t *)first + sizeof(*first))[0] ^= 1U;
  second->next_block_offset = ref.offset;
  assert(qst_block_validate(&mapping, ref, 7) == QST_E_BLOCK_INVALID);
}

static void qstTestBlockAbortUnpublishedChain(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  uint8_t payload[200] = {0};
  SQstShmMapping mapping = {0};
  SQstRef ref = {0};
  SQstBlockHeader *first = NULL;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_block_alloc(&mapping, 17, 1, sizeof(payload), &ref) == QST_OK);
  first = (SQstBlockHeader *)(memory + ref.offset);
  assert(qst_block_abort(&mapping, ref, 18) == QST_E_BLOCK_INVALID);
  assert(atomic_load_explicit(&first->state, memory_order_acquire) ==
         QST_BLOCK_WRITING);
  assert(qst_block_abort(&mapping, ref, 17) == QST_OK);
  assert(atomic_load_explicit(&first->state, memory_order_acquire) ==
         QST_BLOCK_FREE);
  assert(qst_block_alloc(&mapping, 17, 1, sizeof(payload), &ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &ref, payload, sizeof(payload)) == QST_OK);
  assert(qst_block_abort(&mapping, ref, 17) == QST_E_STALE_OWNER);
  assert(qst_block_validate(&mapping, ref, 17) == QST_OK);
}

static void qstTestReclaimsStaleRunOwnedBlocks(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  uint8_t payload[200] = {0};
  SQstShmMapping mapping = {0};
  SQstRef sealed_ref = {0};
  SQstRef writing_ref = {0};
  SQstRef current_ref = {0};
  uint32_t reclaimed_count = 0;
  SQstBlockHeader *block = NULL;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_block_alloc(&mapping, 0, 7, sizeof(payload), &sealed_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &sealed_ref, payload, sizeof(payload)) ==
         QST_OK);
  assert(qst_block_alloc(&mapping, 0, 7, sizeof(payload), &writing_ref) == QST_OK);
  assert(qst_block_alloc(&mapping, 0, 8, sizeof(payload), &current_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &current_ref, payload, sizeof(payload)) ==
         QST_OK);
  assert(qst_block_reclaim_run_owned(&mapping, 8, &reclaimed_count) == QST_OK);
  assert(reclaimed_count >= 4U);
  block = (SQstBlockHeader *)(memory + sealed_ref.offset);
  assert(atomic_load_explicit(&block->state, memory_order_acquire) ==
         QST_BLOCK_FREE);
  block = (SQstBlockHeader *)(memory + writing_ref.offset);
  assert(atomic_load_explicit(&block->state, memory_order_acquire) ==
         QST_BLOCK_FREE);
  block = (SQstBlockHeader *)(memory + current_ref.offset);
  assert(atomic_load_explicit(&block->state, memory_order_acquire) ==
         QST_BLOCK_SEALED);
  assert(qst_block_validate(&mapping, current_ref, 0) == QST_OK);
  assert(qst_block_reclaim_run_owned(&mapping, 8, &reclaimed_count) == QST_OK);
  assert(reclaimed_count == 0);
  assert(qst_block_reclaim_run_owned(&mapping, 9, &reclaimed_count) == QST_OK);
  assert(reclaimed_count >= 3U);
  assert(qst_block_validate(&mapping, current_ref, 0) == QST_E_BLOCK_INVALID);
  block = (SQstBlockHeader *)(memory + kTestBlockOffset);
  block->owner_gid = 0;
  block->owner_generation = 7;
  atomic_store_explicit(&block->state, 99, memory_order_release);
  assert(qst_block_reclaim_run_owned(&mapping, 8, &reclaimed_count) ==
         QST_E_SHM_CORRUPT);
  memset((uint8_t *)block + sizeof(block->state), 0,
         sizeof(*block) - sizeof(block->state));
  atomic_store_explicit(&block->state, QST_BLOCK_FREE, memory_order_release);
}

static void qstTestBlockCrossChainCopy(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  uint8_t payload[200] = {0};
  uint8_t copied[90] = {0};
  SQstShmMapping mapping = {0};
  SQstRef ref = {0};

  for (uint32_t index = 0; index < sizeof(payload); ++index) {
    payload[index] = (uint8_t)(index + 3U);
  }
  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_block_alloc(&mapping, 9, 1, sizeof(payload), &ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &ref, payload, sizeof(payload)) == QST_OK);
  assert(qst_block_copy(&mapping, ref, 9, 55, copied, sizeof(copied)) == QST_OK);
  assert(memcmp(copied, payload + 55, sizeof(copied)) == 0);
  assert(qst_block_copy(&mapping, ref, 9, 199, copied, 2) == QST_E_BLOCK_INVALID);
  assert(qst_block_copy(&mapping, ref, 10, 0, copied, 1) == QST_E_BLOCK_INVALID);
  assert(qst_block_validate(&mapping, ref, 9) == QST_OK);
}

typedef struct {
  SQstNativeClient *client;
  _Atomic int32_t called;
  int32_t code;
  int32_t event_fd;
} SQstNativeAsyncTestState;

static void qstTestNativeAsyncCallback(void *param, void *result, int32_t code) {
  SQstNativeAsyncTestState *state = (SQstNativeAsyncTestState *)param;

  assert(state != NULL);
  assert(state->client != NULL);
  state->called = 1;
  state->code = code;
  if (state->event_fd >= 0) {
    const uint64_t event_count = 1;
    assert(write(state->event_fd, &event_count, sizeof(event_count)) ==
           (ssize_t)sizeof(event_count));
  }
  if (result != NULL) {
    state->client->free_result(result);
  }
}

static void qstTestNativeAsyncLifecycle(void) {
  SQstNativeClient client = {0};
  SQstResultStore store = {0};
  SQstResultStoreConfig store_config = {0};
  SQstNativeAsyncTestState state = {0};
  struct pollfd event = {0};
  uint64_t event_count = 0;
  char kill_log[] = "/tmp/taosQueryTest-native-kill-XXXXXX";
  char connect_log[] = "/tmp/taosQueryTest-native-connect-XXXXXX";
  char schema_log[] = "/tmp/taosQueryTest-native-schema-XXXXXX";
  int32_t kill_fd = -1;
  int32_t connect_fd = -1;
  int32_t schema_fd = -1;
  FILE *file = NULL;
  char kill_contents[32] = {0};
  char connect_contents[64] = {0};
  char schema_contents[16384] = {0};
  SQstDigest256 fingerprint = {0};

  assert(qst_native_open(&client) == QST_OK);
  state.client = &client;
  state.event_fd = -1;
  assert(qst_native_execute_async(&client, "SELECT 1",
                                  qstTestNativeAsyncCallback, &state) == QST_OK);
  assert(atomic_load_explicit(&state.called, memory_order_acquire) == 1);
  assert(state.code == 0);
  atomic_store_explicit(&state.called, 0, memory_order_release);
  state.event_fd = eventfd(0, EFD_CLOEXEC);
  assert(state.event_fd >= 0);
  assert(setenv("QST_FAKE_TAOS_ASYNC_DELAY_MS", "20", 1) == 0);
  assert(qst_native_execute_async(&client, "SELECT 1",
                                  qstTestNativeAsyncCallback, &state) == QST_OK);
  event.fd = state.event_fd;
  event.events = POLLIN;
  assert(poll(&event, 1, 1000) == 1);
  assert(read(state.event_fd, &event_count, sizeof(event_count)) ==
         (ssize_t)sizeof(event_count));
  assert(atomic_load_explicit(&state.called, memory_order_acquire) == 1);
  assert(unsetenv("QST_FAKE_TAOS_ASYNC_DELAY_MS") == 0);
  atomic_store_explicit(&state.called, 0, memory_order_release);
  assert(setenv("QST_FAKE_TAOS_NO_CALLBACK", "1", 1) == 0);
  assert(qst_native_execute_async(&client, "SELECT 1",
                                  qstTestNativeAsyncCallback, &state) == QST_OK);
  assert(poll(&event, 1, 50) == 0);
  assert(atomic_load_explicit(&state.called, memory_order_acquire) == 0);
  assert(unsetenv("QST_FAKE_TAOS_NO_CALLBACK") == 0);
  kill_fd = mkstemp(kill_log);
  assert(kill_fd >= 0 && close(kill_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_KILL_LOG", kill_log, 1) == 0);
  assert(qst_native_kill_query(&client) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_KILL_LOG") == 0);
  file = fopen(kill_log, "r");
  assert(file != NULL && fgets(kill_contents, sizeof(kill_contents), file) != NULL &&
         fclose(file) == 0);
  assert(strcmp(kill_contents, "kill\n") == 0);
  assert(unlink(kill_log) == 0);
  assert(close(state.event_fd) == 0);
  assert(qst_native_close(&client) == QST_OK);
  connect_fd = mkstemp(connect_log);
  assert(connect_fd >= 0 && close(connect_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_CONNECT_LOG", connect_log, 1) == 0);
  assert(qst_native_open_with_endpoint(&client, "127.0.0.2", 6041, NULL) ==
         QST_OK);
  assert(qst_native_close(&client) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_CONNECT_LOG") == 0);
  file = fopen(connect_log, "r");
  assert(file != NULL &&
         fgets(connect_contents, sizeof(connect_contents), file) != NULL &&
         fclose(file) == 0);
  assert(strcmp(connect_contents, "host=127.0.0.2 port=6041\n") == 0);
  assert(unlink(connect_log) == 0);
  store_config.run_id = 101;
  store_config.endpoint = "127.0.0.3:16035";
  assert(setenv("QST_FAKE_TAOS_CONNECT_LOG", connect_log, 1) == 0);
  assert(qst_result_store_open(&store_config, &store) == QST_OK);
  assert(store.native_taos != NULL);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_CONNECT_LOG") == 0);
  file = fopen(connect_log, "r");
  assert(file != NULL &&
         fgets(connect_contents, sizeof(connect_contents), file) != NULL &&
         fclose(file) == 0);
  assert(strcmp(connect_contents, "host=127.0.0.3 port=16035\n") == 0);
  assert(unlink(connect_log) == 0);
  assert(qst_result_store_open(&store_config, &store) == QST_OK);
  for (uint32_t index = 0; index < sizeof(fingerprint.bytes); ++index) {
    fingerprint.bytes[index] = (uint8_t)(index + 1U);
  }
  schema_fd = mkstemp(schema_log);
  assert(schema_fd >= 0 && close(schema_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", schema_log, 1) == 0);
  assert(qst_result_store_prepare_run(&store, (1U << 0) | (1U << 2),
                                      fingerprint, 3U) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  file = fopen(schema_log, "r");
  assert(file != NULL && fread(schema_contents, 1, sizeof(schema_contents) - 1,
                               file) > 0 && fclose(file) == 0);
  assert(strstr(schema_contents, "CREATE STABLE IF NOT EXISTS res_db.statements") != NULL);
  assert(strstr(schema_contents, "dialect TINYINT") != NULL);
  assert(strstr(schema_contents, "generation_abnormal TINYINT") != NULL);
  assert(strstr(schema_contents,
                "generation_unsupported_fallback TINYINT") != NULL);
  assert(strstr(schema_contents, "sql_shape_mask SMALLINT") != NULL);
  assert(strstr(schema_contents, "DESCRIBE res_db.statements") != NULL);
  assert(strstr(schema_contents, "SELECT dialect FROM res_db.statements LIMIT 1") == NULL);
  assert(strstr(schema_contents, "ALTER STABLE res_db.statements") == NULL);
  assert(strstr(schema_contents, "stmt_101_td USING res_db.statements") != NULL);
  assert(strstr(schema_contents, "stmt_101_td_w1 USING res_db.statements") != NULL);
  assert(strstr(schema_contents, "result_101_td_w2 USING res_db.results") != NULL);
  assert(strstr(schema_contents, "result_101_pg USING res_db.results") != NULL);
  assert(strstr(schema_contents, "verdict_01020304") != NULL);
  file = fopen(schema_log, "w");
  assert(file != NULL && fclose(file) == 0);
  memset(schema_contents, 0, sizeof(schema_contents));
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", schema_log, 1) == 0);
  assert(setenv("QST_FAKE_TAOS_MISSING_STATEMENT_COLUMN", "sql_shape_mask", 1) == 0);
  assert(qst_result_store_prepare_run(&store, (1U << 0) | (1U << 2),
                                      fingerprint, 3U) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_MISSING_STATEMENT_COLUMN") == 0);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  file = fopen(schema_log, "rb");
  assert(file != NULL && fread(schema_contents, 1, sizeof(schema_contents) - 1,
                               file) > 0 && fclose(file) == 0);
  assert(strstr(schema_contents, "DESCRIBE res_db.statements") != NULL);
  assert(strstr(schema_contents,
                "ALTER STABLE res_db.statements ADD COLUMN sql_shape_mask SMALLINT") !=
         NULL);
  assert(unlink(schema_log) == 0);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(store.native_taos == NULL);
}

static void qstTestAuditCli(void) {
  char output_path[] = "/tmp/taosQueryTest-audit-output-XXXXXX";
  char sql_path[] = "/tmp/taosQueryTest-audit-sql-XXXXXX";
  char reset_output_path[] = "/tmp/taosQueryTest-reset-output-XXXXXX";
  char reset_sql_path[] = "/tmp/taosQueryTest-reset-sql-XXXXXX";
  char output[65536] = {0};
  char sql[32768] = {0};
  char endpoint[256] = {0};
  char config_dir[4096] = {0};
  char reset_allowed[16] = {0};
  char *argv[] = {"taosQueryTest", "--audit-run",
                  "18446744073709551615", "--audit-targets",
                  "td,mysql,pg,influx"};
  char *reset_argv[] = {"taosQueryTest", "--reset-result-store",
                        "18446744073709551615"};
  FILE *file = NULL;
  int32_t output_fd = -1;
  int32_t sql_fd = -1;
  int32_t reset_output_fd = -1;
  int32_t reset_sql_fd = -1;
  int32_t stdout_fd = -1;
  int32_t endpoint_present = 0;
  int32_t config_dir_present = 0;
  int32_t reset_allowed_present = 0;

  output_fd = mkstemp(output_path);
  sql_fd = mkstemp(sql_path);
  assert(output_fd >= 0 && sql_fd >= 0);
  assert(close(sql_fd) == 0);
  qstTestSaveEnvironment("QST_RESULT_STORE_ENDPOINT", endpoint,
                         sizeof(endpoint), &endpoint_present);
  qstTestSaveEnvironment("QST_RESULT_STORE_CONFIG_DIR", config_dir,
                         sizeof(config_dir), &config_dir_present);
  qstTestSaveEnvironment("QST_RESULT_STORE_ALLOW_RESET", reset_allowed,
                         sizeof(reset_allowed), &reset_allowed_present);
  assert(unsetenv("QST_RESULT_STORE_ENDPOINT") == 0);
  assert(unsetenv("QST_RESULT_STORE_CONFIG_DIR") == 0);
  assert(unsetenv("QST_RESULT_STORE_ALLOW_RESET") == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_path, 1) == 0);
  fflush(stdout);
  stdout_fd = dup(STDOUT_FILENO);
  assert(stdout_fd >= 0);
  assert(dup2(output_fd, STDOUT_FILENO) == STDOUT_FILENO);
  assert(close(output_fd) == 0);
  assert(qst_audit_run_cli(5, argv) == QST_OK);
  fflush(stdout);
  assert(dup2(stdout_fd, STDOUT_FILENO) == STDOUT_FILENO);
  assert(close(stdout_fd) == 0);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  qstTestRestoreEnvironment("QST_RESULT_STORE_ENDPOINT", endpoint,
                            endpoint_present);
  qstTestRestoreEnvironment("QST_RESULT_STORE_CONFIG_DIR", config_dir,
                            config_dir_present);
  file = fopen(output_path, "rb");
  assert(file != NULL && fread(output, 1, sizeof(output) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(output, "\"run_id\":\"18446744073709551615\"") != NULL);
  assert(strstr(output, "\"complete\":true") != NULL);
  file = fopen(sql_path, "rb");
  assert(file != NULL && fread(sql, 1, sizeof(sql) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(sql, " AS count") == NULL);
  assert(strstr(sql, "ORDER BY COUNT(*) DESC") != NULL);
  assert(strstr(sql, "SELECT sql_sha256,COUNT(*)") != NULL);
  assert(strstr(sql, "SELECT sql_sha256,sql_text") != NULL);
  assert(strstr(sql, "SELECT sql_shape_mask,COUNT(*)") != NULL);
  assert(strstr(sql, "GROUP BY sql_shape_mask") != NULL);
  assert(strstr(sql, "sql_text LIKE '%src_fq_mysql_%'") != NULL);
  assert(strstr(sql, "sql_text LIKE '%src_fq_pg_%'") != NULL);
  assert(strstr(sql, "sql_text LIKE '%src_fq_influx_%'") != NULL);
  assert(strstr(sql, "UPPER(sql_text) REGEXP") == NULL);
  assert(strstr(sql, "GROUP[[:space:]]+BY") == NULL);
  assert(strstr(output, "\"sql_shape\"") != NULL);
  assert(strstr(output, "\"source_reference_count\"") != NULL);
  assert(strstr(output, "\"value\":\"2\"") != NULL);
  assert(unlink(output_path) == 0);
  assert(unlink(sql_path) == 0);

  assert(setenv("QST_FAKE_TAOS_SQL_LOG", reset_sql_path, 1) == 0);
  assert(qst_audit_reset_result_store_cli(3, reset_argv) == QST_E_INVALID_ARGUMENT);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  reset_output_fd = mkstemp(reset_output_path);
  reset_sql_fd = mkstemp(reset_sql_path);
  assert(reset_output_fd >= 0 && reset_sql_fd >= 0);
  assert(close(reset_sql_fd) == 0);
  file = fopen(reset_sql_path, "rb");
  assert(file != NULL && fgetc(file) == EOF && fclose(file) == 0);
  assert(setenv("QST_RESULT_STORE_ALLOW_RESET", "1", 1) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", reset_sql_path, 1) == 0);
  fflush(stdout);
  stdout_fd = dup(STDOUT_FILENO);
  assert(stdout_fd >= 0);
  assert(dup2(reset_output_fd, STDOUT_FILENO) == STDOUT_FILENO);
  assert(close(reset_output_fd) == 0);
  assert(qst_audit_reset_result_store_cli(3, reset_argv) == QST_OK);
  fflush(stdout);
  assert(dup2(stdout_fd, STDOUT_FILENO) == STDOUT_FILENO);
  assert(close(stdout_fd) == 0);
  assert(unsetenv("QST_RESULT_STORE_ALLOW_RESET") == 0);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  qstTestRestoreEnvironment("QST_RESULT_STORE_ALLOW_RESET", reset_allowed,
                            reset_allowed_present);
  memset(output, 0, sizeof(output));
  file = fopen(reset_output_path, "rb");
  assert(file != NULL && fread(output, 1, sizeof(output) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(output, "\"run_id\":\"18446744073709551615\"") != NULL);
  assert(strstr(output, "\"reset\":true") != NULL);
  assert(strstr(output, "\"complete\":true") != NULL);
  memset(sql, 0, sizeof(sql));
  file = fopen(reset_sql_path, "rb");
  assert(file != NULL && fread(sql, 1, sizeof(sql) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(sql, "SELECT COUNT(*) FROM res_db.runs WHERE run_id=") != NULL);
  assert(strstr(sql, "DROP DATABASE res_db") != NULL);
  assert(unlink(reset_output_path) == 0);
  assert(unlink(reset_sql_path) == 0);
}

static void qstTestOverflowContentAtomicAppend(void) {
  const uint8_t text_content[] = {'q', '"', '\n', '\\', 'x'};
  const uint8_t hex_content[] = "00ff";
  SQstResultStore store = {0};
  SQstResultStoreConfig config = {0};
  char root[] = "/tmp/taosQueryTest-overflow-XXXXXX";
  char overflow_path[512] = {0};
  char first_reference[64] = {0};
  char second_reference[64] = {0};
  char contents[8192] = {0};
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(overflow_path, sizeof(overflow_path), "%s/overflow_content.ndjson",
                  root) > 0);
  config.run_id = 101;
  config.overflow_path = overflow_path;
  assert(qst_result_store_open(&config, &store) == QST_OK);
  assert(qst_result_store_write_overflow(
             &store, "statements", "sql_text", "utf8", text_content,
             sizeof(text_content), first_reference, sizeof(first_reference)) == QST_OK);
  assert(qst_result_store_write_overflow(
             &store, "results", "result_content", "hex", hex_content,
             sizeof(hex_content) - 1U, second_reference, sizeof(second_reference)) ==
         QST_OK);
  assert(strcmp(first_reference, "@overflow:1") == 0);
  assert(strcmp(second_reference, "@overflow:2") == 0);
  file = fopen(overflow_path, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents, "\"table\":\"statements\"") != NULL);
  assert(strstr(contents, "\"column\":\"sql_text\"") != NULL);
  assert(strstr(contents, "\"content\":\"q\\\"\\u000a\\\\x\"") != NULL);
  assert(strstr(contents, "\"encoding\":\"hex\"") != NULL);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(unlink(overflow_path) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestRunAndDatasetPersistence(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstResultStore store = {0};
  SQstResultStoreConfig config = {0};
  SQstPendingStore pending = {0};
  SQstRunRecord run = {0};
  SQstDatasetRecord dataset = {0};
  char root[] = "/tmp/taosQueryTest-run-dataset-XXXXXX";
  char pending_dir[512] = {0};
  char quarantine_dir[512] = {0};
  char sql_log[] = "/tmp/taosQueryTest-run-dataset-sql-XXXXXX";
  char sql_contents[32768] = {0};
  int32_t sql_fd = -1;
  FILE *file = NULL;
  uint32_t durable_source = 0;
  uint64_t replayed_count = 0;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(pending_dir, sizeof(pending_dir), "%s/pending-results", root) > 0);
  assert(snprintf(quarantine_dir, sizeof(quarantine_dir), "%s/quarantine",
                  pending_dir) > 0);
  assert(mkdir(pending_dir, 0700) == 0);
  qstInitMapping(&mapping, memory, sizeof(memory));
  config.run_id = mapping.header->run_id;
  config.pending_dir = pending_dir;
  config.quarantine_dir = quarantine_dir;
  assert(qst_result_store_open(&config, &store) == QST_OK);
  pending.run_id = mapping.header->run_id;
  pending.pending_dir = pending_dir;
  pending.quarantine_dir = quarantine_dir;
  run.begin_ts_ns = INT64_C(1000000000);
  run.status = 0;
  run.has_random_seed = 1;
  run.random_seed = 77;
  run.planned_statement_count = -1;
  run.mode = "random";
  run.focus = "correctness";
  run.target_set = "td,mysql";
  run.tool_version = "taosQueryTest-v1";
  run.parallel_test_info = "{\"parallel_pair_count\":1,\"pairs\":[]}";
  dataset.defined_ts_ns = INT64_C(1000000000);
  dataset.ds_fingerprint =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  dataset.config_json = "{\"version\":\"dataset-v1\"}";
  dataset.fingerprint_format = "fixture-v1|config_digest|directory_digest";
  sql_fd = mkstemp(sql_log);
  assert(sql_fd >= 0 && close(sql_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_log, 1) == 0);
  assert(qst_result_store_write_run_record(&mapping, &store, &pending, &run, 1,
                                           &durable_source) == QST_OK);
  assert(durable_source == QST_DURABLE_RESULT_STORE);
  assert(qst_result_store_write_dataset_record(&mapping, &store, &pending,
                                               &dataset, 2,
                                               &durable_source) == QST_OK);
  assert(durable_source == QST_DURABLE_RESULT_STORE);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  file = fopen(sql_log, "rb");
  assert(file != NULL && fread(sql_contents, 1, sizeof(sql_contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(sql_contents, "INSERT INTO res_db.runs VALUES") != NULL);
  assert(strstr(sql_contents, "INSERT INTO res_db.datasets VALUES") != NULL);
  assert(setenv("QST_FAKE_TAOS_FAIL", "INSERT INTO res_db.", 1) == 0);
  run.end_ts_ns = INT64_C(2000000000);
  run.status = 1;
  run.stop_reason = "statement_limit";
  assert(qst_result_store_write_run_record(&mapping, &store, &pending, &run, 3,
                                           &durable_source) == QST_OK);
  assert(durable_source == QST_DURABLE_PENDING_FILE);
  assert(qst_result_store_write_dataset_record(&mapping, &store, &pending,
                                               &dataset, 4,
                                               &durable_source) == QST_OK);
  assert(durable_source == QST_DURABLE_PENDING_FILE);
  assert(unsetenv("QST_FAKE_TAOS_FAIL") == 0);
  memset(sql_contents, 0, sizeof(sql_contents));
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_log, 1) == 0);
  assert(qst_pending_replay(&mapping, &store, mapping.header->run_id,
                            &replayed_count) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  assert(replayed_count == 2);
  file = fopen(sql_log, "rb");
  assert(file != NULL && fread(sql_contents, 1, sizeof(sql_contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(sql_contents, "INSERT INTO res_db.qst_pending_records") == NULL);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(unlink(sql_log) == 0);
  assert(rmdir(quarantine_dir) == 0 || errno == ENOENT);
  assert(rmdir(pending_dir) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestHistoricalVerdictLookup(void) {
  static const char kHistoricalResult[] =
      "{\"format\":\"query-result-v1\",\"columns\":[{\"name\":\"id\","
      "\"type\":\"int64\"}],\"rows\":[[{\"kind\":\"int64\","
      "\"value\":\"42\"}]]}";
  SQstResultStore store = {0};
  SQstResultStoreConfig config = {0};
  SQstDigest256 sql_sha256 = {0};
  SQstQueryResult result = {0};
  char validator[32] = {0};
  int32_t found = 0;

  config.run_id = 101;
  assert(qst_result_store_open(&config, &store) == QST_OK);
  for (uint32_t index = 0; index < sizeof(sql_sha256.bytes); ++index) {
    sql_sha256.bytes[index] = (uint8_t)(index + 1U);
  }
  assert(setenv("QST_FAKE_TAOS_HISTORICAL_RESULT", kHistoricalResult, 1) == 0);
  assert(qst_result_store_lookup_historical(
             &store,
             "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             QST_TARGET_TD, sql_sha256, &result, validator, sizeof(validator),
             &found) == QST_OK);
  assert(found == 1);
  assert(strcmp(validator, "historical") == 0);
  assert(result.bytes == strlen(kHistoricalResult));
  assert(memcmp(result.data, kHistoricalResult, result.bytes) == 0);
  qst_result_reset(&result);
  assert(unsetenv("QST_FAKE_TAOS_HISTORICAL_RESULT") == 0);
  assert(qst_result_store_lookup_historical(
             &store,
             "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             QST_TARGET_TD, sql_sha256, &result, validator, sizeof(validator),
             &found) == QST_OK);
  assert(found == 0 && result.data == NULL && validator[0] == '\0');
  assert(qst_result_store_lookup_historical(
             &store,
             "g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             QST_TARGET_TD, sql_sha256, &result, validator, sizeof(validator),
             &found) == QST_E_INVALID_ARGUMENT);
  assert(setenv("QST_FAKE_TAOS_HISTORICAL_RESULT", kHistoricalResult, 1) == 0);
  assert(setenv("QST_FAKE_TAOS_HISTORICAL_VALIDATOR", "invalid", 1) == 0);
  assert(qst_result_store_lookup_historical(
             &store,
             "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
             QST_TARGET_TD, sql_sha256, &result, validator, sizeof(validator),
             &found) == QST_E_RESULT_STORE);
  assert(unsetenv("QST_FAKE_TAOS_HISTORICAL_RESULT") == 0);
  assert(unsetenv("QST_FAKE_TAOS_HISTORICAL_VALIDATOR") == 0);
  assert(qst_result_store_close(&store) == QST_OK);
}

static void qstTestStoredResultReadAndManualAiWrite(void) {
  static const char kFingerprint[] =
      "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  static const char kAiAnalysis[] = "analysis from ai";
  SQstResultStore store = {0};
  SQstResultStoreConfig config = {0};
  SQstStoredResult *records = NULL;
  SQstStoredStatement *statements = NULL;
  SQstManualAiResult manual = {0};
  SQstQueryResult content = {0};
  SQstPendingStore pending = {0};
  char root[] = "/tmp/taosQueryTest-manual-ai-XXXXXX";
  char pending_dir[512] = {0};
  char sql_log[] = "/tmp/taosQueryTest-manual-ai-sql-XXXXXX";
  char sql_contents[65536] = {0};
  char pending_path[1024] = {0};
  int32_t sql_fd = -1;
  uint32_t count = 0;
  uint32_t statement_count = 0;
  uint32_t durable_source = 0;
  FILE *file = NULL;
  struct stat pending_status = {0};

  assert(mkdtemp(root) != NULL);
  assert(snprintf(pending_dir, sizeof(pending_dir), "%s/pending-results", root) > 0);
  assert(mkdir(pending_dir, 0700) == 0);
  config.run_id = 101;
  config.pending_dir = pending_dir;
  assert(qst_result_store_open(&config, &store) == QST_OK);
  assert(qst_result_store_read_results(&store, QST_TARGET_TD, &records, &count) ==
         QST_OK);
  assert(count == 2 && records != NULL);
  assert(records[0].gid == 11 && records[0].verdict == 2);
  assert(records[0].ts_ns == INT64_C(1704067200000000000));
  assert(strcmp(records[0].validator, "manual") == 0);
  assert(strcmp(records[0].ds_fingerprint, kFingerprint) == 0);
  assert(records[0].result_content.bytes > 0);
  assert(records[1].gid == 12 && records[1].verdict == 0);
  assert(records[1].result_content.data == NULL);
  assert(qst_result_store_read_statements(&store, QST_TARGET_TD, &statements,
                                          &statement_count) == QST_OK);
  assert(statement_count == 1 && statements != NULL);
  assert(statements[0].gid == 11 && statements[0].sid == 22);
  assert(strcmp(statements[0].sql_text, "select * from t") == 0);
  assert(strcmp(statements[0].sql_sha256, kFingerprint) == 0);
  assert(statements[0].execution_status == QST_EXEC_SUCCESS);
  qst_result_store_free_statements(statements, statement_count);
  content = records[0].result_content;
  sql_fd = mkstemp(sql_log);
  assert(sql_fd >= 0 && close(sql_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_log, 1) == 0);
  manual.gid = records[0].gid;
  manual.target = records[0].target;
  manual.verdict = 2;
  manual.ds_fingerprint = kFingerprint;
  manual.result_content = &content;
  manual.validator = "ai";
  manual.diff_summary = "AI confirmed";
  manual.analysis_content = kAiAnalysis;
  assert(qst_result_store_write_manual_ai_result(NULL, &store, NULL, &manual, 1,
                                                 &durable_source) == QST_OK);
  assert(durable_source == QST_DURABLE_RESULT_STORE);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  file = fopen(sql_log, "rb");
  assert(file != NULL && fread(sql_contents, 1, sizeof(sql_contents) - 1, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(sql_contents, "INSERT INTO res_db.result_101_td") != NULL);
  assert(strstr(sql_contents, "'ai'") != NULL);
  assert(strstr(sql_contents, "AI confirmed") != NULL);
  assert(strstr(sql_contents, kAiAnalysis) != NULL);
  assert(strstr(sql_contents, "INSERT INTO res_db.runs") == NULL);
  assert(strstr(sql_contents, "INSERT INTO res_db.stmt_") == NULL);
  assert(setenv("QST_FAKE_TAOS_FAIL", "INSERT INTO res_db.", 1) == 0);
  pending.run_id = store.run_id;
  pending.pending_dir = pending_dir;
  pending.quota_bytes = 1024 * 1024;
  assert(qst_result_store_write_manual_ai_result(NULL, &store, &pending, &manual, 2,
                                                 &durable_source) == QST_OK);
  assert(durable_source == QST_DURABLE_PENDING_FILE);
  assert(unsetenv("QST_FAKE_TAOS_FAIL") == 0);
  assert(snprintf(pending_path, sizeof(pending_path), "%s/pending.stream",
                  pending_dir) > 0);
  assert(stat(pending_path, &pending_status) == 0);
  assert(pending_status.st_size > 0);
  assert(unlink(pending_path) == 0);
  qst_result_store_free_results(records, count);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(unlink(sql_log) == 0);
  assert(rmdir(pending_dir) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestNativeResultSerializationAndComparison(void) {
  static const char kInvalidFormat[] =
      "{\"format\":\"query-result-v0\",\"columns\":[],\"rows\":[]}";
  static const char kExpectedCaptured[] =
      "{\"format\":\"query-result-v1\",\"columns\":[{\"name\":\"id\","
      "\"type\":\"int64\",\"bytes\":8},{\"name\":\"label\","
      "\"type\":\"varchar\",\"bytes\":16}],\"rows\":[[{\"kind\":\"int64\","
      "\"value\":\"42\"},{\"kind\":\"varchar\",\"value\":\"alpha\"}],"
      "[{\"kind\":\"int64\",\"value\":\"7\"},{\"kind\":\"varchar\","
      "\"value\":\"beta\"}]]}";
  static const char kOrderedRows[] =
      "{\"format\":\"query-result-v1\",\"columns\":[{\"name\":\"id\","
      "\"type\":\"int64\"}],\"rows\":[[{\"kind\":\"int64\","
      "\"value\":\"42\"}],[{\"kind\":\"int64\",\"value\":\"7\"}]]}";
  static const char kSwappedRows[] =
      "{\"format\":\"query-result-v1\",\"columns\":[{\"name\":\"id\","
      "\"type\":\"int64\"}],\"rows\":[[{\"kind\":\"int64\","
      "\"value\":\"7\"}],[{\"kind\":\"int64\",\"value\":\"42\"}]]}";
  SQstNativeClient client = {0};
  SQstQueryResult captured = {0};
  SQstQueryResult ordered = {0};
  SQstQueryResult swapped = {0};
  SQstQueryResult invalid = {0};
  SQstQueryResult expected_error = {0};
  SQstQueryResult actual_error = {0};
  SQstQueryResult wrong_error = {0};
  SQstQueryResult raw_captured = {0};
  SQstQueryResult cached_first = {0};
  SQstQueryResult cached_second = {0};
  SQstResultCaptureCache capture_cache = {0};
  SQstDigest256 cache_key = {0};
  SQstResultDiff diff = {0};
  SQstResultStreamSummary stream_summary = {0};
  char readiness[4096] = {0};
  FILE *stream_file = NULL;
  void *result = NULL;

  assert(qst_native_open(&client) == QST_OK);
  result = client.query(client.connection, "SELECT qst_query_result");
  assert(result != NULL);
  stream_file = tmpfile();
  assert(stream_file != NULL);
  assert(qst_native_capture_result_stream(&client, result, stream_file,
                                          1024U * 1024U,
                                          &stream_summary) == QST_OK);
  assert(stream_summary.comparator_version == QST_RESULT_STREAM_COMPARATOR_VERSION);
  assert(stream_summary.row_count == 2U);
  assert(stream_summary.logical_bytes > 0U);
  assert(fclose(stream_file) == 0);
  stream_file = NULL;
  client.free_result(result);
  result = client.query(client.connection, "SET qst_zero_fields");
  assert(result != NULL);
  stream_file = tmpfile();
  assert(stream_file != NULL);
  memset(&stream_summary, 0, sizeof(stream_summary));
  assert(qst_native_capture_result_stream(&client, result, stream_file,
                                          1024U * 1024U,
                                          &stream_summary) == QST_OK);
  assert(stream_summary.row_count == 0U);
  assert(stream_summary.logical_bytes > 0U);
  assert(fflush(stream_file) == 0 && fseek(stream_file, 0, SEEK_SET) == 0);
  {
    char zero_fields_output[256] = {0};
    const size_t read_bytes = fread(zero_fields_output, 1,
                                    sizeof(zero_fields_output) - 1U,
                                    stream_file);
    assert(read_bytes > 0U);
    assert(strstr(zero_fields_output, "\"columns\":[]") != NULL);
    assert(strstr(zero_fields_output, "\"rows\":[]") != NULL);
  }
  assert(fclose(stream_file) == 0);
  stream_file = NULL;
  client.free_result(result);
  result = NULL;
  result = client.query(client.connection, "SELECT qst_query_result");
  assert(result != NULL);
  assert(qst_native_capture_result(&client, result, &captured) == QST_OK);
  client.free_result(result);
  assert(qst_native_readiness_json(&client, readiness, sizeof(readiness)) == QST_OK);
  assert(strstr(readiness, "3.4.3.0.fake") != NULL);
  assert(strstr(readiness, "federatedQueryEnable") == NULL);
  assert(strstr(readiness, "\"federated_query_enable\":\"1\"") != NULL);
  assert(captured.row_count == 2);
  assert(captured.bytes == strlen(kExpectedCaptured));
  assert(memcmp(captured.data, kExpectedCaptured, (size_t)captured.bytes) == 0);
  assert(strstr((const char *)captured.data, "\"format\":\"query-result-v1\"") != NULL);
  assert(strstr((const char *)captured.data, "\"name\":\"label\"") != NULL);
  assert(strstr((const char *)captured.data, "\"bytes\":8") != NULL);
  assert(strstr((const char *)captured.data, "\"value\":\"42\"") != NULL);
  assert(qst_result_compare(&captured, &captured, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_NONE);
  result = client.query(client.connection, "SELECT qst_query_result");
  assert(result != NULL);
  assert(qst_native_capture_raw_result(&client, result, &raw_captured) ==
         QST_OK);
  client.free_result(result);
  result = NULL;
  assert(qst_result_compare(&raw_captured, &captured, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_NONE);
  qst_result_reset(&raw_captured);
  cache_key.bytes[0] = 1U;
  result = client.query(client.connection, "SELECT qst_query_result");
  assert(result != NULL);
  assert(qst_result_capture_verified_cached(&client, result, &cache_key,
                                            &capture_cache,
                                            &cached_first) == QST_OK);
  client.free_result(result);
  result = client.query(client.connection, "SELECT qst_query_result");
  assert(result != NULL);
  assert(qst_result_capture_verified_cached(&client, result, &cache_key,
                                            &capture_cache,
                                            &cached_second) == QST_OK);
  client.free_result(result);
  result = NULL;
  assert(cached_first.bytes == cached_second.bytes);
  assert(memcmp(cached_first.data, cached_second.data,
                (size_t)cached_first.bytes) == 0);
  qst_result_reset(&cached_first);
  qst_result_reset(&cached_second);
  qst_result_capture_cache_destroy(&capture_cache);

  ordered.data = (uint8_t *)kOrderedRows;
  ordered.bytes = strlen(kOrderedRows);
  swapped.data = (uint8_t *)kSwappedRows;
  swapped.bytes = strlen(kSwappedRows);
  assert(qst_result_compare(&ordered, &swapped, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_ORDERED_ROW);
  assert(qst_result_compare(&ordered, &swapped, 0, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_NONE);
  invalid.data = (uint8_t *)kInvalidFormat;
  invalid.bytes = strlen(kInvalidFormat);
  assert(qst_result_compare(&invalid, &ordered, 1, &diff) ==
         QST_E_INVALID_ARGUMENT);
  assert(diff.kind == QST_RESULT_DIFF_FORMAT);
  assert(qst_result_build_error(0x2600, "  bad\n  external   source ",
                                &expected_error) == QST_OK);
  assert(qst_result_build_error(0x2600, "bad external source", &actual_error) ==
         QST_OK);
  assert(qst_result_compare(&expected_error, &actual_error, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_NONE);
  assert(qst_result_build_error(0x2601, "bad external source", &wrong_error) ==
         QST_OK);
  assert(qst_result_compare(&expected_error, &wrong_error, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_ERROR_CODE);
  qst_result_reset(&expected_error);
  qst_result_reset(&actual_error);
  qst_result_reset(&wrong_error);
  qst_result_reset(&captured);
  assert(qst_native_close(&client) == QST_OK);
}

/* Read-only compatibility probe for the public raw-block path against a real
 * result store. The selected columns exercise fixed values, variable values,
 * BLOB bytes, and nullable cells without changing the shared service. */
static void qstTestRawCaptureLiveComparison(void) {
  static const char kSql[] =
      "SELECT ts,gid,verdict,validator,diff_summary,error_code,error_message,"
      "result_content,result_storage_status FROM res_db.results LIMIT 2";
  SQstNativeClient client = {0};
  SQstQueryResult row_result = {0};
  SQstQueryResult raw_result = {0};
  SQstResultDiff diff = {0};
  void *result = NULL;

  assert(qst_native_open(&client) == QST_OK);
  result = client.query(client.connection, kSql);
  assert(result != NULL);
  assert(qst_native_capture_result(&client, result, &row_result) == QST_OK);
  client.free_result(result);
  result = client.query(client.connection, kSql);
  assert(result != NULL);
  assert(qst_native_capture_raw_result(&client, result, &raw_result) ==
         QST_OK);
  client.free_result(result);
  assert(qst_result_compare(&raw_result, &row_result, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_NONE);
  qst_result_reset(&raw_result);
  qst_result_reset(&row_result);
  assert(qst_native_close(&client) == QST_OK);
}

static void qstTestStreamingResultOracle(void) {
  static const char kColumns[] =
      "[{\"name\":\"id\",\"type\":\"int64\"},{\"name\":\"payload\","
      "\"type\":\"varchar\"}]";
  static const char kRows[][160] = {
      "[{\"kind\":\"int64\",\"value\":\"1\"},null]",
      "[{\"kind\":\"int64\",\"value\":\"2\"},{\"kind\":\"text\","
      "\"value\":\"secret-a\"}]",
      "[{\"kind\":\"int64\",\"value\":\"3\"},{\"kind\":\"hex\","
      "\"value\":\"00ff\"}]",
  };
  FILE *ordered_file = NULL;
  FILE *swapped_file = NULL;
  FILE *large_file = NULL;
  SQstResultStream ordered = {0};
  SQstResultStream swapped = {0};
  SQstResultStream large = {0};
  SQstResultStreamSummary ordered_summary = {0};
  SQstResultStreamSummary swapped_summary = {0};
  SQstResultStreamSummary large_summary = {0};
  SQstResultDiff diff = {0};
  char *large_row = NULL;
  uint64_t large_row_bytes = 0;
  long large_file_bytes = 0;

  ordered_file = tmpfile();
  swapped_file = tmpfile();
  large_file = tmpfile();
  assert(ordered_file != NULL && swapped_file != NULL && large_file != NULL);
  assert(qst_result_stream_begin(&ordered, ordered_file, kColumns,
                                 strlen(kColumns), 1024U * 1024U) == QST_OK);
  assert(qst_result_stream_begin(&swapped, swapped_file, kColumns,
                                 strlen(kColumns), 1024U * 1024U) == QST_OK);
  for (uint32_t index = 0; index < 3; ++index) {
    assert(qst_result_stream_append_row(&ordered, kRows[index],
                                        strlen(kRows[index])) == QST_OK);
  }
  assert(qst_result_stream_append_row(&swapped, kRows[2], strlen(kRows[2])) == QST_OK);
  assert(qst_result_stream_append_row(&swapped, kRows[0], strlen(kRows[0])) == QST_OK);
  assert(qst_result_stream_append_row(&swapped, kRows[1], strlen(kRows[1])) == QST_OK);
  assert(qst_result_stream_finish(&ordered, &ordered_summary) == QST_OK);
  assert(qst_result_stream_finish(&swapped, &swapped_summary) == QST_OK);
  assert(ordered_summary.comparator_version == 1U);
  assert(ordered_summary.row_count == 3U);
  assert(ordered_summary.logical_bytes > 0U);
  assert(qst_result_stream_compare(&ordered_summary, &swapped_summary, 1, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_ORDERED_ROW);
  assert(qst_result_stream_compare(&ordered_summary, &swapped_summary, 0, &diff) == QST_OK);
  assert(diff.kind == QST_RESULT_DIFF_NONE);
  assert(strstr(diff.summary, "secret-a") == NULL);
  assert(fflush(ordered_file) == 0 && fseek(ordered_file, 0, SEEK_END) == 0);
  assert(ftell(ordered_file) == (long)ordered_summary.logical_bytes);

  assert(qst_result_stream_begin(&large, large_file, kColumns,
                                 strlen(kColumns), 2U * 1024U * 1024U) == QST_OK);
  large_row_bytes = 4090U;
  large_row = (char *)calloc((size_t)large_row_bytes + 32U, 1);
  assert(large_row != NULL);
  for (uint32_t index = 0; index < 4000U; ++index) {
    int32_t written = snprintf(large_row, (size_t)large_row_bytes + 32U,
                               "[{\"kind\":\"int64\",\"value\":\"%u\"},"
                               "{\"kind\":\"text\",\"value\":\"%0*u\"}]",
                               index, 4000, index);
    assert(written > 0 && (uint64_t)written < large_row_bytes + 32U);
    assert(qst_result_stream_append_row(&large, large_row, (uint64_t)written) == QST_OK);
  }
  assert(qst_result_stream_finish(&large, &large_summary) == QST_OK);
  assert(large_summary.row_count == 4000U);
  assert(large_summary.logical_bytes > 15U * 1000U * 1000U);
  free(large_row);
  large_row = NULL;
  assert(qst_result_stream_begin(&large, large_file, kColumns,
                                 strlen(kColumns), 32U) == QST_OK);
  assert(qst_result_stream_append_row(&large, kRows[0], strlen(kRows[0])) == QST_OK);
  assert(qst_result_stream_append_row(&large, kRows[1], strlen(kRows[1])) == QST_E_RESOURCE);
  qst_result_stream_abort(&large);
  assert(fclose(ordered_file) == 0 && fclose(swapped_file) == 0 && fclose(large_file) == 0);
}

static void qstTestReportAtomicRefresh(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  char result_dir[] = "/tmp/taosQueryTest-report-XXXXXX";
  char json_path[512] = {0};
  char markdown_path[512] = {0};
  char temporary_path[512] = {0};
  char contents[8192] = {0};
  SQstShmMapping mapping = {0};
  SQstRunConfig config = {0};
  SQstTaskEntry *entries = NULL;
  SQstAiActionEntry *actions = NULL;
  FILE *file = NULL;

  assert(mkdtemp(result_dir) != NULL);
  qstInitMapping(&mapping, memory, sizeof(memory));
  entries = (SQstTaskEntry *)(memory + kTestTaskOffset);
  actions = (SQstAiActionEntry *)(memory + kTestAiActionOffset);
  entries[0].gid = 1;
  atomic_store_explicit(&entries[0].phase, QST_TASK_PERSISTED,
                        memory_order_release);
  atomic_store_explicit(&entries[0].result_storage_status, QST_RESULT_MEMORY,
                        memory_order_release);
  entries[1].gid = 2;
  atomic_store_explicit(&entries[1].phase, QST_TASK_ISOLATED,
                        memory_order_release);
  atomic_store_explicit(&entries[1].result_storage_status,
                        QST_RESULT_UNAVAILABLE, memory_order_release);
  atomic_store_explicit(&actions[0].state, QST_AI_SUCCEEDED,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->execution_success_count, 3,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->execution_error_count, 4,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->execution_timeout_count, 5,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->execution_cancelled_count, 6,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->verdict_passed_count, 1,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->verdict_failed_count, 2,
                        memory_order_release);
  atomic_store_explicit(&mapping.header->verdict_unverified_count, 3,
                        memory_order_release);
  config.run_id = mapping.header->run_id;
  config.result_dir = result_dir;
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_CORRECTNESS;
  config.random_seed = 12345;
  config.statement_limit = 7;
  atomic_store_explicit(&mapping.header->stop_reason,
                        QST_STOP_STATEMENT_LIMIT, memory_order_release);
  assert(qst_report_refresh(&mapping, &config) == QST_OK);
  assert(snprintf(json_path, sizeof(json_path), "%s/report.json", result_dir) > 0);
  assert(snprintf(markdown_path, sizeof(markdown_path), "%s/report.md", result_dir) > 0);
  assert(snprintf(temporary_path, sizeof(temporary_path), "%s.tmp", json_path) > 0);
  assert(access(json_path, F_OK) == 0);
  assert(access(markdown_path, F_OK) == 0);
  assert(access(temporary_path, F_OK) != 0);
  file = fopen(json_path, "rb");
  assert(file != NULL);
  assert(fread(contents, 1, sizeof(contents) - 1, file) > 0);
  assert(fclose(file) == 0);
  assert(strstr(contents, "\"persisted\":1") != NULL);
  assert(strstr(contents, "\"processes\":{") != NULL);
  assert(strstr(contents, "\"random_seed\":12345") != NULL);
  assert(strstr(contents, "\"stop_reason\":\"statement_limit\"") != NULL);
  assert(strstr(contents, "\"phases\":{") != NULL);
  assert(strstr(contents, "\"statistics\":{") != NULL);
  assert(strstr(contents,
                "\"execution\":{\"success\":3,\"error\":4,"
                "\"timeout\":5,\"cancelled\":6}") != NULL);
  assert(strstr(contents,
                "\"verdict\":{\"passed\":1,\"failed\":2,"
                "\"unverified\":3}") != NULL);
  assert(strstr(contents, "\"errors\":{") != NULL);
  assert(strstr(contents, "\"artifacts\":{") != NULL);
  assert(strstr(contents, "\"startup_digests\":{") != NULL);
  assert(strstr(contents, "\"resource_profile_sha256\":") != NULL);
  assert(strstr(contents, "\"dependency_lock_sha256\":") != NULL);
  assert(strstr(contents, "\"tdengine_version_sha256\":") != NULL);
  assert(strstr(contents, "\"ai_actions\":{\"prechecked\":0,\"running\":0,"
                          "\"succeeded\":1") != NULL);
  assert(strstr(contents, "\"isolated\":1") != NULL);
  assert(strstr(contents, "\"unavailable\":1") != NULL);
  assert(qst_report_refresh(&mapping, &config) == QST_OK);
  assert(unlink(json_path) == 0);
  assert(unlink(markdown_path) == 0);
  assert(rmdir(result_dir) == 0);
}

static void qstTestPendingStoreContinuousAppend(void) {
  enum { kPendingPathBytes = 4096 };
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t first_payload[] = {0x00, 0x01, 0xfe, 0xff};
  const uint8_t conflicting_payload[] = {0x01, 0x02};
  char root[] = "/tmp/taosQueryTest-pending-XXXXXX";
  char path[kPendingPathBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstPendingStore store = {0};
  SQstPendingRecord record = {0};
  SQstRef first_ref = {0};
  SQstRef duplicate_ref = {0};
  struct stat first_status = {0};
  struct stat second_status = {0};

  assert(mkdtemp(root) != NULL);
  qstInitMapping(&mapping, memory, sizeof(memory));
  store.run_id = mapping.header->run_id;
  store.pending_dir = root;
  store.quota_bytes = 4096;
  record.persist_kind = QST_PERSIST_STATEMENT;
  record.target = QST_TARGET_TD;
  record.run_id = mapping.header->run_id;
  record.gid = 0;
  record.record_seq = 7;
  record.created_at_ns = 123456789;
  record.stable_key = "statements|101|td|1|123";
  record.payload = first_payload;
  record.payload_bytes = sizeof(first_payload);
  assert(qst_pending_write(&mapping, &store, &record, &first_ref) == QST_OK);
  assert(!qstRefIsEmpty(first_ref));
  assert(qst_block_copy(&mapping, first_ref, 0, 0, path,
                        first_ref.length) == QST_OK);
  assert(access(path, F_OK) == 0);
  assert(stat(path, &first_status) == 0);
  assert(first_status.st_size > 0);
  store.quota_bytes = 1;
  record.record_seq = 8;
  record.stable_key = "statements|101|td|8|123";
  assert(qst_pending_write(&mapping, &store, &record, &duplicate_ref) ==
         QST_E_PENDING_IO);
  store.quota_bytes = 4096;
  record.created_at_ns = 987654321;
  record.record_seq = 7;
  record.stable_key = "statements|101|td|1|123";
  assert(qst_pending_write(&mapping, &store, &record, &duplicate_ref) == QST_OK);
  assert(!qstRefIsEmpty(duplicate_ref));
  assert(stat(path, &second_status) == 0);
  assert(second_status.st_size > first_status.st_size);
  record.payload = conflicting_payload;
  record.payload_bytes = sizeof(conflicting_payload);
  assert(qst_pending_write(&mapping, &store, &record, &duplicate_ref) == QST_OK);
  record.stable_key = "invalid\"key";
  assert(qst_pending_write(&mapping, &store, &record, &duplicate_ref) == QST_OK);
  assert(qst_block_release(&mapping, first_ref, 0) == QST_OK);
  assert(qst_block_release(&mapping, duplicate_ref, 0) == QST_OK);
  assert(unlink(path) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestPendingReplayAndDiscard(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t payload_one[] = {'o', 'n', 'e'};
  const uint8_t payload_two[] = {'t', 'w', 'o'};
  char root[] = "/tmp/taosQueryTest-pending-replay-XXXXXX";
  char pending_dir[512] = {0};
  char path_one[512] = {0};
  char path_two[512] = {0};
  char invalid_path[512] = {0};
  SQstShmMapping mapping = {0};
  SQstPendingStore pending = {0};
  SQstPendingRecord record = {0};
  SQstResultStoreConfig store_config = {0};
  SQstResultStore store = {0};
  SQstRef first_ref = {0};
  SQstRef second_ref = {0};
  uint64_t replayed_count = 0;
  FILE *file = NULL;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(pending_dir, sizeof(pending_dir), "%s/pending-results", root) > 0);
  assert(mkdir(pending_dir, 0700) == 0);
  qstInitMapping(&mapping, memory, sizeof(memory));
  pending.run_id = mapping.header->run_id;
  pending.pending_dir = pending_dir;
  pending.quota_bytes = 4096;
  record.persist_kind = QST_PERSIST_STATEMENT;
  record.target = QST_TARGET_TD;
  record.run_id = mapping.header->run_id;
  record.record_seq = 2;
  record.created_at_ns = 200;
  record.stable_key = "statements|101|td|2|200";
  record.payload = payload_two;
  record.payload_bytes = sizeof(payload_two);
  assert(qst_pending_write(&mapping, &pending, &record, &second_ref) == QST_OK);
  assert(qst_block_copy(&mapping, second_ref, 0, 0, path_two,
                        second_ref.length) == QST_OK);
  record.record_seq = 1;
  record.created_at_ns = 100;
  record.stable_key = "statements|101|td|1|100";
  record.payload = payload_one;
  record.payload_bytes = sizeof(payload_one);
  assert(qst_pending_write(&mapping, &pending, &record, &first_ref) == QST_OK);
  assert(qst_block_copy(&mapping, first_ref, 0, 0, path_one,
                        first_ref.length) == QST_OK);
  store_config.run_id = mapping.header->run_id;
  store_config.pending_dir = pending_dir;
  assert(qst_result_store_open(&store_config, &store) == QST_OK);
  assert(qst_pending_replay(&mapping, &store, mapping.header->run_id,
                            &replayed_count) == QST_OK);
  assert(replayed_count == 2);
  assert(access(path_one, F_OK) != 0);
  assert(access(path_two, F_OK) != 0);
  assert(snprintf(invalid_path, sizeof(invalid_path),
                  "%s/p.9.0000000000000000000000000000000000000000000000000000000000000000.json",
                  pending_dir) > 0);
  file = fopen(invalid_path, "w");
  assert(file != NULL);
  assert(fputs("not-json\n", file) >= 0);
  assert(fclose(file) == 0);
  replayed_count = 0;
  assert(qst_pending_replay(&mapping, &store, mapping.header->run_id,
                            &replayed_count) == QST_OK);
  assert(replayed_count == 0);
  assert(access(invalid_path, F_OK) != 0);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(qst_block_release(&mapping, first_ref, 0) == QST_OK);
  assert(qst_block_release(&mapping, second_ref, 0) == QST_OK);
  assert(rmdir(pending_dir) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestPendingStreamReplay(void) {
  enum { kPendingPathBytes = 512, kBatchQuotaBytes = 32 * 1024 * 1024 };
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t payload[] = "batch-pending-payload";
  char root[] = "/tmp/taosQueryTest-pending-batch-XXXXXX";
  char pending_dir[kPendingPathBytes] = {0};
  char quarantine_dir[kPendingPathBytes] = {0};
  char sql_log[kPendingPathBytes] = {0};
  char stable_key[160] = {0};
  SQstShmMapping mapping = {0};
  SQstPendingStore pending = {0};
  SQstPendingRecord record = {0};
  SQstPendingBatch batch = {.fd = -1};
  SQstResultStoreConfig store_config = {0};
  SQstResultStore store = {0};
  uint64_t record_count = 0;
  uint64_t replayed_count = 0;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(pending_dir, sizeof(pending_dir), "%s/pending-results", root) > 0);
  assert(snprintf(quarantine_dir, sizeof(quarantine_dir), "%s/quarantine",
                  pending_dir) > 0);
  assert(snprintf(sql_log, sizeof(sql_log), "%s/replay.sql", root) > 0);
  assert(mkdir(pending_dir, 0700) == 0);
  qstInitMapping(&mapping, memory, sizeof(memory));
  pending.run_id = mapping.header->run_id;
  pending.pending_dir = pending_dir;
  pending.quarantine_dir = quarantine_dir;
  pending.quota_bytes = kBatchQuotaBytes;
  record.persist_kind = QST_PERSIST_STATEMENT;
  record.target = QST_TARGET_TD;
  record.run_id = mapping.header->run_id;
  record.payload = payload;
  record.payload_bytes = sizeof(payload) - 1U;
  assert(qst_pending_batch_begin(&mapping, &pending, &batch) == QST_OK);
  for (record_count = 1; record_count <= 3; ++record_count) {
    assert(snprintf(stable_key, sizeof(stable_key),
                    "statements|%llu|td|%llu|%llu",
                    (unsigned long long)record.run_id,
                    (unsigned long long)record_count,
                    (unsigned long long)record_count) > 0);
    record.gid = record_count;
    record.record_seq = record_count;
    record.created_at_ns = (int64_t)record_count;
    record.stable_key = stable_key;
    assert(qst_pending_batch_append(&batch, &record) == QST_OK);
  }
  assert(qst_pending_batch_flush(&batch) == QST_OK);
  assert(access(batch.final_path, F_OK) == 0);
  qst_pending_batch_abort(&batch);
  store_config.run_id = mapping.header->run_id;
  store_config.pending_dir = pending_dir;
  store_config.quarantine_dir = quarantine_dir;
  assert(qst_result_store_open(&store_config, &store) == QST_OK);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_log, 1) == 0);
  assert(qst_pending_replay(&mapping, &store, mapping.header->run_id,
                            &replayed_count) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  assert(replayed_count == record_count - 1U);
  assert(access(batch.final_path, F_OK) != 0);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(unlink(sql_log) == 0);
  assert(rmdir(pending_dir) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestAiActionLedger(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t prompt[] = "judge this result";
  const uint8_t manifest[] = "result-manifest-v1";
  const uint8_t analysis[] = "analysis";
  SQstShmMapping mapping = {0};
  SQstAiActionEntry *entries = NULL;
  SQstRef prompt_ref = {0};
  SQstRef manifest_ref = {0};
  SQstRef analysis_ref = {0};
  SQstDigest256 action_id = {0};
  SQstDigest256 duplicate_id = {0};
  uint32_t main_generation = 0;
  uint32_t worker_generation = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              101, 101, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_AI_WORKER, 102, 102,
                              &worker_generation) == QST_OK);
  prompt_ref = qstSealTestBytesForOwner(&mapping, 1, prompt, sizeof(prompt));
  manifest_ref = qstSealTestBytesForOwner(&mapping, 1, manifest,
                                          sizeof(manifest));
  analysis_ref = qstSealTestBytesForOwner(&mapping, 1, analysis,
                                          sizeof(analysis));
  assert(qst_ai_precheck(&mapping, 1, QST_AI_ENTRY_MANUAL_ALL, prompt_ref,
                         manifest_ref, &action_id) == QST_OK);
  assert(qst_ai_precheck(&mapping, 1, QST_AI_ENTRY_MANUAL_ALL, prompt_ref,
                         manifest_ref, &duplicate_id) == QST_OK);
  assert(memcmp(action_id.bytes, duplicate_id.bytes, sizeof(action_id.bytes)) ==
         0);
  assert(qst_ai_precheck(&mapping, 1, QST_AI_ENTRY_AUTO_VERDICT, prompt_ref,
                         manifest_ref, &duplicate_id) == QST_E_INVALID_ARGUMENT);
  assert(qst_ai_run_action(&mapping, action_id, worker_generation + 1) ==
         QST_E_STALE_OWNER);
  assert(qst_ai_run_action(&mapping, action_id, worker_generation) == QST_OK);
  assert(qst_ai_publish_result(&mapping, action_id, analysis_ref, QST_OK,
                               worker_generation + 1) == QST_E_STALE_OWNER);
  assert(qst_ai_publish_result(&mapping, action_id, analysis_ref, QST_OK,
                               worker_generation) == QST_OK);
  assert(qst_ai_run_action(&mapping, action_id, worker_generation) == QST_OK);
  entries = (SQstAiActionEntry *)(memory +
      mapping.header->ai_action_region.region_offset);
  assert(atomic_load_explicit(&entries[0].state, memory_order_acquire) ==
         QST_AI_SUCCEEDED);
  assert(entries[0].owner_generation == worker_generation);
  assert(entries[0].analysis_ref.offset == analysis_ref.offset);
  assert(entries[0].action_code == QST_OK);
}

static SQstRef qstSealManifest(SQstShmMapping *mapping, const uint8_t magic[8],
                               const void *entries, uint32_t entry_bytes,
                               uint32_t entry_count) {
  SQstManifestHeader header = {0};
  uint8_t buffer[2048] = {0};
  SQstSha256 digest = {0};
  SQstRef ref = {0};
  uint64_t entries_bytes = (uint64_t)entry_bytes * entry_count;

  assert(entries_bytes <= sizeof(buffer) - sizeof(header));
  memcpy(header.magic, magic, sizeof(header.magic));
  header.format_version = QST_SHM_FORMAT_VERSION;
  header.header_bytes = sizeof(header);
  header.entry_bytes = entry_bytes;
  header.entry_count = entry_count;
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, entries, entries_bytes);
  qst_sha256_final(&digest, header.content_sha256.bytes);
  memcpy(buffer, &header, sizeof(header));
  memcpy(buffer + sizeof(header), entries, entries_bytes);
  ((SQstManifestHeader *)buffer)->crc32c = 0;
  ((SQstManifestHeader *)buffer)->crc32c =
      qstCrc32c(qstCrc32c(0, buffer, sizeof(header)),
                buffer + sizeof(header), entries_bytes);
  assert(qst_block_alloc(mapping, 0, 1, sizeof(header) + entries_bytes, &ref) ==
         QST_OK);
  assert(qst_block_write_seal(mapping, &ref, buffer,
                              sizeof(header) + entries_bytes) == QST_OK);
  return ref;
}

static SQstRef qstSealTestBytes(SQstShmMapping *mapping, const uint8_t *bytes,
                                uint32_t byte_count) {
  return qstSealTestBytesForOwner(mapping, 0, bytes, byte_count);
}

static SQstRef qstSealTestBytesForOwner(SQstShmMapping *mapping,
                                        uint64_t owner_gid,
                                        const uint8_t *bytes,
                                        uint32_t byte_count) {
  SQstRef ref = {0};

  assert(qst_block_alloc(mapping, owner_gid, 1, byte_count, &ref) == QST_OK);
  assert(qst_block_write_seal(mapping, &ref, bytes, byte_count) == QST_OK);
  return ref;
}

static void qstTestObjectDescriptorDigest(SQstObjectDescriptor *entry) {
  SQstObjectDescriptor copy = {0};
  SQstSha256 digest = {0};

  copy = *entry;
  memset(&copy.descriptor_sha256, 0, sizeof(copy.descriptor_sha256));
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, (const uint8_t *)&copy, sizeof(copy));
  qst_sha256_final(&digest, entry->descriptor_sha256.bytes);
}

static void qstTestFixtureDescriptorDigest(SQstFixtureDescriptor *fixture) {
  SQstFixtureDescriptor copy = {0};
  SQstSha256 digest = {0};

  copy = *fixture;
  memset(&copy.fixture_sha256, 0, sizeof(copy.fixture_sha256));
  qst_sha256_init(&digest);
  qst_sha256_update(&digest, (const uint8_t *)&copy, sizeof(copy));
  qst_sha256_final(&digest, fixture->fixture_sha256.bytes);
}

static void qstTestDatasetManifestValidation(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstDatasetInfo dataset = {0};
  SQstObjectDescriptor objects[2] = {{0}};
  SQstFixtureDescriptor fixture = {0};
  SQstObjectDescriptor copied_objects[2] = {{0}};
  SQstRef binding_ref = {0};
  SQstRef object_ref = {0};
  uint32_t object_count = 0;
  const uint8_t binding[] = "binding";
  const uint8_t name[] = "db_frag";
  const uint8_t schema[] = "precision=ms";
  const uint8_t verification[] = "row_count=4";
  static const uint8_t object_magic[8] = {'Q', 'S', 'T', 'O', 'B', 'J', '1', '0'};
  static const uint8_t fixture_magic[8] = {'Q', 'S', 'T', 'F', 'I', 'X', '1', '0'};

  qstInitMapping(&mapping, memory, sizeof(memory));
  objects[0].object_id = 1;
  objects[0].target = QST_TARGET_TD;
  objects[0].object_kind = QST_OBJECT_SOURCE;
  objects[0].qualified_name_ref = qstSealTestBytes(&mapping, name, sizeof(name) - 1);
  objects[0].schema_ref = qstSealTestBytes(&mapping, schema, sizeof(schema) - 1);
  qstTestObjectDescriptorDigest(&objects[0]);
  objects[1].object_id = 2;
  objects[1].target = QST_TARGET_MYSQL;
  objects[1].object_kind = QST_OBJECT_SOURCE;
  objects[1].qualified_name_ref = qstSealTestBytes(&mapping, name, sizeof(name) - 1);
  objects[1].schema_ref = qstSealTestBytes(&mapping, schema, sizeof(schema) - 1);
  qstTestObjectDescriptorDigest(&objects[1]);
  dataset.object_directory_ref = qstSealManifest(
      &mapping, object_magic, objects, sizeof(objects[0]), 2);
  fixture.registry_id = 1;
  fixture.target = QST_TARGET_TD;
  fixture.state = QST_FIXTURE_READY;
  fixture.object_ref = dataset.object_directory_ref;
  fixture.expected_schema_ref = objects[0].schema_ref;
  fixture.verification_ref =
      qstSealTestBytes(&mapping, verification, sizeof(verification) - 1);
  qstTestFixtureDescriptorDigest(&fixture);
  dataset.fixture_manifest_ref = qstSealManifest(
      &mapping, fixture_magic, &fixture, sizeof(fixture), 1);
  assert(qst_block_alloc(&mapping, 77, 1, sizeof(binding) - 1, &binding_ref) ==
         QST_OK);
  assert(qst_block_write_seal(&mapping, &binding_ref, binding,
                              sizeof(binding) - 1) == QST_OK);
  assert(qst_dataset_get_object(&mapping, &dataset, QST_TARGET_TD,
                                QST_OBJECT_SOURCE, 0, &object_ref) == QST_OK);
  assert(object_ref.offset == dataset.object_directory_ref.offset);
  assert(qst_dataset_get_object(&mapping, &dataset, QST_TARGET_MYSQL,
                                QST_OBJECT_SOURCE, 0, &object_ref) == QST_OK);
  assert(object_ref.offset == dataset.object_directory_ref.offset);
  assert(qst_dataset_get_object_count(&mapping, &dataset, &object_count) == QST_OK);
  assert(object_count == 2);
  assert(qst_dataset_copy_object_directory(&mapping, &dataset, copied_objects,
                                           1, &object_count) ==
         QST_E_RESOURCE);
  assert(qst_dataset_copy_object_directory(&mapping, &dataset, copied_objects,
                                           2, &object_count) == QST_OK);
  assert(copied_objects[1].object_id == objects[1].object_id);
  assert(qst_dataset_validate_binding(&mapping, &dataset, 77, QST_TARGET_TD,
                                      binding_ref) == QST_OK);
  fixture.state = QST_FIXTURE_PLANNED;
  qstTestFixtureDescriptorDigest(&fixture);
  assert(qst_block_release(&mapping, dataset.fixture_manifest_ref, 0) == QST_OK);
  dataset.fixture_manifest_ref = qstSealManifest(
      &mapping, fixture_magic, &fixture, sizeof(fixture), 1);
  assert(qst_dataset_validate_binding(&mapping, &dataset, 77, QST_TARGET_TD,
                                      binding_ref) == QST_E_SHM_CORRUPT);
  ((uint8_t *)mapping.base + dataset.object_directory_ref.offset +
   sizeof(SQstBlockHeader))[0] ^= 1U;
  assert(qst_dataset_get_object(&mapping, &dataset, QST_TARGET_TD,
                                QST_OBJECT_SOURCE, 0, &object_ref) ==
         QST_E_SHM_CORRUPT);
}

static void qstTestRuntimeObjectBinding(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstObjectDescriptor objects[11] = {{0}};
  SQstDatasetInfo dataset = {0};
  SQstGeneratorRuntimeContext runtime = {0};
  SQstRef schema_ref = {0};
  static const uint8_t object_magic[8] = {'Q', 'S', 'T', 'O', 'B', 'J', '1', '0'};
  static const char *const names[] = {
      "tdengine", "local_db", "local_db.local_t", "value_col", "tag_col",
      "src_mysql", "remote_db", "remote_t", "remote_col", "local_t2",
      "value_col2",
  };
  static const uint32_t kinds[] = {
      QST_OBJECT_SOURCE, QST_OBJECT_DATABASE_OR_SCHEMA,
      QST_OBJECT_TABLE_OR_MEASUREMENT, QST_OBJECT_COLUMN_OR_TAG,
      QST_OBJECT_COLUMN_OR_TAG, QST_OBJECT_SOURCE,
      QST_OBJECT_DATABASE_OR_SCHEMA, QST_OBJECT_TABLE_OR_MEASUREMENT,
      QST_OBJECT_COLUMN_OR_TAG,
      QST_OBJECT_TABLE_OR_MEASUREMENT, QST_OBJECT_COLUMN_OR_TAG,
  };
  static const uint32_t targets[] = {
      QST_TARGET_TD, QST_TARGET_TD, QST_TARGET_TD, QST_TARGET_TD,
      QST_TARGET_TD, QST_TARGET_MYSQL, QST_TARGET_MYSQL, QST_TARGET_MYSQL,
      QST_TARGET_MYSQL,
      QST_TARGET_TD, QST_TARGET_TD,
  };
  static const uint64_t parents[] = {0, 1, 2, 3, 3, 0, 6, 7, 8, 2, 10};
  static const uint32_t ordinals[] = {0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0};
  static const struct {
    uint32_t target;
    uint32_t kind;
    const char *expected_sql;
  } cases[] = {
      {QST_TARGET_TD, QST_RUNTIME_OBJECT_DATABASE, "local_db"},
      {QST_TARGET_TD, QST_RUNTIME_OBJECT_TABLE, "local_db.local_t"},
      {QST_TARGET_TD, QST_RUNTIME_OBJECT_TABLE_REFERENCE, "local_db.local_t"},
      {QST_TARGET_MYSQL, QST_RUNTIME_OBJECT_TABLE_REFERENCE, "remote_db.remote_t"},
  };

  config.run_id = UINT64_C(6100000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 2;
  config.block_bytes = 256;
  config.block_count = 128;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  schema_ref = qstSealTestBytes(
      &mapping, (const uint8_t *)"schema", sizeof("schema") - 1U);
  for (uint32_t index = 0; index < sizeof(objects) / sizeof(objects[0]); ++index) {
    objects[index].object_id = index + 1U;
    objects[index].parent_object_id = parents[index];
    objects[index].target = targets[index];
    objects[index].object_kind = kinds[index];
    objects[index].ordinal = ordinals[index];
    objects[index].flags = index == 4 ? QST_OBJECT_FLAG_TAG : 0;
    objects[index].qualified_name_ref = qstSealTestBytes(
        &mapping, (const uint8_t *)names[index], (uint32_t)strlen(names[index]));
    objects[index].schema_ref = schema_ref;
    qstTestObjectDescriptorDigest(&objects[index]);
  }
  dataset.object_directory_ref = qstSealManifest(
      &mapping, object_magic, objects, sizeof(objects[0]),
      sizeof(objects) / sizeof(objects[0]));
  /* A nonempty fixture ref identifies the immutable snapshot passed by main. */
  dataset.fixture_manifest_ref = qstSealTestBytes(
      &mapping, (const uint8_t *)"fixture", sizeof("fixture") - 1U);
  runtime.mapping = &mapping;
  runtime.object_directory_ref = dataset.object_directory_ref;
  runtime.fixture_manifest_ref = dataset.fixture_manifest_ref;
  runtime.target = QST_TARGET_TD;
  runtime.dialect = QST_DIALECT_TDENGINE;
  {
    static const SQstGeneratorValue table_values[] = {
        {7051, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_TABLE,
         NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue reference_values[] = {
        {7052, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_TABLE_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue column_values[] = {
        {7053, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_COLUMN,
         NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue column_reference_values[] = {
        {7054, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_COLUMN_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue asterisk_values[] = {
        {7055, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_QUALIFIED_ASTERISK, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue tbname_values[] = {
        {7056, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_QUALIFIED_TBNAME_PSEUDOCOLUMN, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue tag_values[] = {
        {7057, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_ROLLUP_TAG_PLACEHOLDER, NULL, "", 0, NULL},
    };
    static const SQstGeneratorStep root_steps[] = {
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7058, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7059, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {4, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7060, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {5, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7061, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {6, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7062, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {7, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7063, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {8, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
    };
    static const SQstGeneratorValue root_values[] = {
        {7050, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "",
         sizeof(root_steps) / sizeof(root_steps[0]), root_steps},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
        {2, 1, sizeof(table_values) / sizeof(table_values[0]), table_values},
        {3, 1, sizeof(reference_values) / sizeof(reference_values[0]),
         reference_values},
        {4, 1, sizeof(column_values) / sizeof(column_values[0]), column_values},
        {5, 1, sizeof(column_reference_values) /
                   sizeof(column_reference_values[0]),
         column_reference_values},
        {6, 1, sizeof(asterisk_values) / sizeof(asterisk_values[0]),
         asterisk_values},
        {7, 1, sizeof(tbname_values) / sizeof(tbname_values[0]),
         tbname_values},
        {8, 1, sizeof(tag_values) / sizeof(tag_values[0]), tag_values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratedSql sql = {0};

    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &(SQstGeneratorCursor){0}, 0, &runtime, &sql) == QST_OK);
    assert(strcmp(sql.sql,
                  "local_db.local_t | local_db.local_t | value_col | value_col | local_db.local_t.* | local_db.local_t.tbname | %%tag_col") ==
           0);
    qst_generator_generated_sql_destroy(&sql);
  }
  for (uint32_t index = 0; index < sizeof(cases) / sizeof(cases[0]); ++index) {
    SQstGeneratorValue values[] = {
        {7000U + index, QST_GENERATOR_VALUE_ATOMIC,
         QST_GENERATOR_CLASS_NORMAL, QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         cases[index].kind, NULL, "", 0, NULL},
    };
    SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(values) / sizeof(values[0]), values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratorCursor cursor = {0};
    SQstGeneratedSql sql = {0};

    runtime.target = cases[index].target;
    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &cursor, 0, &runtime, &sql) == QST_OK);
    assert(strcmp(sql.sql, cases[index].expected_sql) == 0);
    assert(sql.occurrence_count == 1);
    assert(sql.occurrences[0].runtime_object_kind == cases[index].kind);
    assert(sql.occurrences[0].runtime_object_id != 0);
    assert(sql.occurrences[0].text_length == strlen(cases[index].expected_sql));
    qst_generator_generated_sql_destroy(&sql);
  }
  runtime.target = QST_TARGET_TD;
  {
    static const SQstGeneratorValue reference_values[] = {
        {7601, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_TABLE_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue outer_alias_values[] = {
        {7602, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "AS outer", 0, NULL},
    };
    static const SQstGeneratorValue inner_alias_values[] = {
        {7603, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "AS inner", 0, NULL},
    };
    static const SQstGeneratorValue column_values[] = {
        {7604, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_COLUMN_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorStep nested_steps[] = {
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7605, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {4, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7606, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {5, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
    };
    static const SQstGeneratorValue nested_values[] = {
        {7607, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "",
         sizeof(nested_steps) / sizeof(nested_steps[0]), nested_steps},
    };
    static const SQstGeneratorStep root_steps[] = {
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7608, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_BIND_TABLE_ALIAS}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7609, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {6, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_QUERY_BLOCK}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7610, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {5, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
    };
    static const SQstGeneratorValue root_values[] = {
        {7611, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "",
         sizeof(root_steps) / sizeof(root_steps[0]), root_steps},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
        {2, 1, sizeof(reference_values) / sizeof(reference_values[0]),
         reference_values},
        {3, 1, sizeof(outer_alias_values) / sizeof(outer_alias_values[0]),
         outer_alias_values},
        {4, 1, sizeof(inner_alias_values) / sizeof(inner_alias_values[0]),
         inner_alias_values},
        {5, 1, sizeof(column_values) / sizeof(column_values[0]), column_values},
        {6, 1, sizeof(nested_values) / sizeof(nested_values[0]), nested_values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratorCursor cursor = {0};
    int32_t found = 0;

    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    for (uint32_t path = 0; path < 128U && found == 0; ++path) {
      SQstGeneratedSql sql = {0};

      assert(qst_generator_full_peek_with_runtime(
                 &catalog, &cursor, 0, &runtime, &sql) == QST_OK);
      if (strcmp(sql.sql,
                 "local_db.local_t | AS outer | local_db.local_t2 | AS inner | inner.value_col2 | outer.value_col") ==
          0) {
        found = 1;
      }
      assert(qst_generator_full_commit(&cursor, &sql) == QST_OK);
      qst_generator_generated_sql_destroy(&sql);
    }
    assert(found != 0);
  }
  {
    SQstGeneratorRuntimeContext external_runtime = runtime;
    static const SQstGeneratorValue source_values[] = {
        {7101, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_SOURCE,
         NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue suffix_values[] = {
        {7102, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX, NULL, "", 0, NULL},
    };
    static const SQstGeneratorStep root_steps[] = {
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7103, ".", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0}},
    };
    static const SQstGeneratorValue root_values[] = {
        {7100, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "",
         sizeof(root_steps) / sizeof(root_steps[0]), root_steps},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
        {2, 1, sizeof(source_values) / sizeof(source_values[0]), source_values},
        {3, 1, sizeof(suffix_values) / sizeof(suffix_values[0]), suffix_values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratorCursor cursor = {0};
    SQstGeneratedSql sql = {0};

    external_runtime.target = QST_TARGET_MYSQL;
    external_runtime.dialect = QST_DIALECT_MYSQL;
    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &cursor, 0, &external_runtime, &sql) == QST_OK);
    assert(strcmp(sql.sql, "src_mysql.remote_db.remote_t") == 0);
    assert(sql.occurrences[1].runtime_object_kind == QST_RUNTIME_OBJECT_SOURCE);
    assert(sql.occurrences[3].runtime_object_kind ==
           QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX);
    assert(sql.occurrences[1].runtime_object_id == 6);
    assert(sql.occurrences[3].runtime_object_id == 8);
    qst_generator_generated_sql_destroy(&sql);
  }
  {
    char oversized_name[QST_GENERATOR_EXECUTABLE_SQL_BYTES + 1U] = {0};
    SQstObjectDescriptor oversized_objects[sizeof(objects) / sizeof(objects[0])];
    SQstGeneratorRuntimeContext oversized_runtime = runtime;
    static const SQstGeneratorValue table_values[] = {
        {7104, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_TABLE,
         NULL, "", 0, NULL},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(table_values) / sizeof(table_values[0]), table_values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratedSql sql = {0};

    memset(oversized_name, 't', sizeof(oversized_name) - 1U);
    memcpy(oversized_objects, objects, sizeof(oversized_objects));
    oversized_objects[2].qualified_name_ref = qstSealTestBytes(
        &mapping, (const uint8_t *)oversized_name,
        sizeof(oversized_name) - 1U);
    qstTestObjectDescriptorDigest(&oversized_objects[2]);
    oversized_runtime.objects = &oversized_objects[2];
    oversized_runtime.object_count = 1;
    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &(SQstGeneratorCursor){0}, 0, &oversized_runtime,
               &sql) == QST_OK);
    assert(strcmp(sql.sql, "?") == 0);
    assert(sql.sql_length == 1);
    assert(sql.contains_unsupported_fallback != 0);
    assert(sql.occurrence_count == 1);
    assert(sql.occurrences[0].text_offset == 0);
    assert(sql.occurrences[0].text_length == 0);
    qst_generator_generated_sql_destroy(&sql);
  }
  {
    static const SQstGeneratorValue values[] = {
        {7200, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX, NULL, "", 0, NULL},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(values) / sizeof(values[0]), values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratedSql sql = {0};

    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &(SQstGeneratorCursor){0}, 0, &runtime, &sql) == QST_OK);
    assert(strcmp(sql.sql, "?") == 0);
    assert(sql.contains_abnormal != 0);
    qst_generator_generated_sql_destroy(&sql);
  }
  {
    static const SQstGeneratorValue values[] = {
        {7201, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_EXTERNAL_TABLE_SUFFIX, NULL, "", 0, NULL},
        {7202, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_NONE, NULL,
         "fallback", 0, NULL},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(values) / sizeof(values[0]), values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratorCursor cursor = {0};
    SQstGeneratorRandomState state = {0};

    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    for (uint64_t sequence = 1; sequence <= 64; ++sequence) {
      SQstGeneratedSql sql = {0};

      assert(qst_generator_random_init(&state, 71, sequence) == QST_OK);
      assert(qst_generator_random_build_with_runtime(
                 &catalog, &state, 0, &runtime, &sql) == QST_OK);
      assert(strcmp(sql.sql, "fallback") == 0);
      qst_generator_generated_sql_destroy(&sql);
    }
    {
      SQstGeneratedSql sql = {0};

      assert(qst_generator_full_peek_with_runtime(&catalog, &cursor, 0,
                                                  &runtime, &sql) == QST_OK);
      assert(strcmp(sql.sql, "fallback") == 0);
      qst_generator_generated_sql_destroy(&sql);
    }
  }
  {
    static const SQstGeneratorValue values[] = {
        {7300, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_TABLE_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(values) / sizeof(values[0]), values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGenerationState *state = NULL;
    SQstTaskEntry *entry = NULL;
    SQstDigest256 dataset_digest = {{0}};
    SQstDigest256 fixture_digest = {{0}};
    char published_sql[128] = {0};
    uint32_t main_generation = 0;
    uint32_t generator_generation = 0;
    uint64_t gid = 0;
    void *cached_objects = NULL;
    void *cached_value_index = NULL;
    void *cached_buildable_values = NULL;
    void *cached_lookup_cache = NULL;
    SQstGeneratorRandomRuntime random_runtime = {0};

    dataset_digest.bytes[0] = 1;
    fixture_digest.bytes[0] = 2;
    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                                (int32_t)getpid(), (int32_t)getpgrp(),
                                &main_generation) == QST_OK);
    assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                                QST_ROLE_GENERATOR, (int32_t)getpid(),
                                (int32_t)getpgrp(),
                                &generator_generation) == QST_OK);
    assert(qst_generator_state_initialize(&mapping, &catalog, QST_MODE_RANDOM,
                                          QST_FOCUS_COVERAGE, UINT32_C(1), 73,
                                          0, 0) == QST_OK);
    assert(qst_generator_state_publish_dataset(
               &mapping, main_generation, dataset_digest, fixture_digest,
               dataset.object_directory_ref, dataset.fixture_manifest_ref) ==
           QST_OK);
    assert(qst_generator_random_publish_next(
               &mapping, QST_ROLE_GENERATOR, generator_generation, &catalog,
               dataset_digest, 1, UINT64_MAX, &random_runtime, &gid) == QST_OK);
    assert(gid == 1);
    assert(qst_generator_state_get(&mapping, &state) == QST_OK);
    assert(atomic_load_explicit(&state->random_generation_seq,
                                memory_order_acquire) == 2);
    entry = qstTestFindTaskByGid(&mapping, gid);
    assert(entry != NULL);
    assert(qst_block_copy(&mapping, entry->sql_ref, gid, 0, published_sql,
                          strlen("local_db.local_t")) == QST_OK);
    assert(strcmp(published_sql, "local_db.local_t") == 0);
    cached_objects = random_runtime.runtime_objects;
    cached_value_index = random_runtime.catalog_value_index;
    cached_buildable_values = random_runtime.buildable_values;
    cached_lookup_cache = random_runtime.buildable_lookup_cache;
    assert(cached_objects != NULL && random_runtime.runtime_object_count != 0);
    assert(cached_value_index != NULL && random_runtime.catalog_value_count != 0);
    assert(cached_buildable_values != NULL &&
           random_runtime.buildability_epoch != 0);
    assert(cached_lookup_cache != NULL);
    assert(qst_generator_random_publish_next(
               &mapping, QST_ROLE_GENERATOR, generator_generation, &catalog,
               dataset_digest, 1, UINT64_MAX, &random_runtime, &gid) == QST_OK);
    assert(gid == 2);
    assert(random_runtime.runtime_objects == cached_objects);
    assert(random_runtime.catalog_value_index == cached_value_index);
    assert(random_runtime.buildable_values == cached_buildable_values);
    assert(random_runtime.buildable_lookup_cache == cached_lookup_cache);
    qst_generator_random_runtime_destroy(&random_runtime);
    assert(random_runtime.runtime_objects == NULL);
    assert(random_runtime.catalog_value_index == NULL);
    assert(random_runtime.buildable_values == NULL);
    assert(random_runtime.buildable_lookup_cache == NULL);
  }
  {
    static const SQstGeneratorValue runtime_values[] = {
        {7400, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_TABLE_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue fixed_values[] = {
        {7400, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_NONE, NULL,
         "fixed", 0, NULL},
    };
    static const SQstGeneratorFragment runtime_fragments[] = {
        {1, 1, sizeof(runtime_values) / sizeof(runtime_values[0]), runtime_values},
    };
    static const SQstGeneratorFragment fixed_fragments[] = {
        {1, 1, sizeof(fixed_values) / sizeof(fixed_values[0]), fixed_values},
    };
    SQstGeneratorCatalog runtime_catalog = {1, 1,
                                            sizeof(runtime_fragments) /
                                                sizeof(runtime_fragments[0]),
                                            runtime_fragments, {{0}}};
    SQstGeneratorCatalog fixed_catalog = {1, 1,
                                          sizeof(fixed_fragments) /
                                              sizeof(fixed_fragments[0]),
                                          fixed_fragments, {{0}}};
    SQstGeneratorRandomState runtime_state = {0};
    SQstGeneratorRandomState fixed_state = {0};
    SQstGeneratedSql first = {0};
    SQstGeneratedSql second = {0};
    SQstGeneratedSql fixed = {0};

    assert(qst_generator_catalog_digest(&runtime_catalog,
                                        &runtime_catalog.digest) == QST_OK);
    assert(qst_generator_catalog_digest(&fixed_catalog, &fixed_catalog.digest) ==
           QST_OK);
    assert(qst_generator_random_init(&runtime_state, 74, 7) == QST_OK);
    assert(qst_generator_random_init(&fixed_state, 74, 7) == QST_OK);
    assert(qst_generator_random_build_with_runtime(
               &runtime_catalog, &runtime_state, 0, &runtime, &first) == QST_OK);
    assert(qst_generator_random_build(&fixed_catalog, &fixed_state, 0, &fixed) ==
           QST_OK);
    assert(runtime_state.state == fixed_state.state);
    assert(runtime_state.random_index == fixed_state.random_index);
    assert(runtime_state.loop_round == fixed_state.loop_round);
    assert(qst_generator_random_init(&runtime_state, 74, 7) == QST_OK);
    assert(qst_generator_random_build_with_runtime(
               &runtime_catalog, &runtime_state, 0, &runtime, &second) == QST_OK);
    assert(strcmp(first.sql, second.sql) == 0);
    assert(first.occurrences[0].runtime_object_id ==
           second.occurrences[0].runtime_object_id);
    qst_generator_generated_sql_destroy(&first);
    qst_generator_generated_sql_destroy(&second);
    qst_generator_generated_sql_destroy(&fixed);
  }
  {
    static const SQstGeneratorValue root_values[] = {
        {7500, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "mysql", 0, NULL,
         QST_GENERATOR_DIALECT_MASK_MYSQL},
        {7501, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "postgres", 0, NULL,
         QST_GENERATOR_DIALECT_MASK_POSTGRESQL},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratorRuntimeContext mysql_runtime = runtime;
    SQstGeneratorRuntimeContext postgres_runtime = runtime;
    SQstGeneratedSql mysql = {0};
    SQstGeneratedSql postgres = {0};

    mysql_runtime.dialect = QST_DIALECT_MYSQL;
    postgres_runtime.dialect = QST_DIALECT_POSTGRESQL;
    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &(SQstGeneratorCursor){0}, 0, &mysql_runtime,
               &mysql) == QST_OK);
    assert(strcmp(mysql.sql, "mysql") == 0);
    assert(qst_generator_full_peek_with_runtime(
               &catalog, &(SQstGeneratorCursor){0}, 0, &postgres_runtime,
               &postgres) == QST_OK);
    assert(strcmp(postgres.sql, "postgres") == 0);
    qst_generator_generated_sql_destroy(&mysql);
    qst_generator_generated_sql_destroy(&postgres);
  }
  {
    SQstGeneratorRuntimeContext duplicate_name_runtime = runtime;
    SQstObjectDescriptor duplicate_name_objects[12] = {{0}};
    static const SQstGeneratorValue table_values[] = {
        {7701, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_TABLE_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorValue column_values[] = {
        {7702, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0,
         QST_RUNTIME_OBJECT_COLUMN_REFERENCE, NULL, "", 0, NULL},
    };
    static const SQstGeneratorStep root_steps[] = {
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7703, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
        {QST_GENERATOR_STEP_INLINE_VALUE, 7704, "|", {0}},
        {QST_GENERATOR_STEP_CALL, 0, NULL,
         {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0,
          QST_GENERATOR_SCOPE_NONE}},
    };
    static const SQstGeneratorValue root_values[] = {
        {7700, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_NONE, NULL,
         "", sizeof(root_steps) / sizeof(root_steps[0]), root_steps},
    };
    static const SQstGeneratorFragment fragments[] = {
        {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
        {2, 1, sizeof(table_values) / sizeof(table_values[0]), table_values},
        {3, 1, sizeof(column_values) / sizeof(column_values[0]), column_values},
    };
    SQstGeneratorCatalog catalog = {1, 1,
                                    sizeof(fragments) / sizeof(fragments[0]),
                                    fragments, {{0}}};
    SQstGeneratorCursor cursor = {0};
    uint32_t distinct_table_path_count = 0;
    uint32_t path_count = 0;

    memcpy(duplicate_name_objects, objects, sizeof(objects));
    duplicate_name_objects[10].qualified_name_ref =
        duplicate_name_objects[3].qualified_name_ref;
    qstTestObjectDescriptorDigest(&duplicate_name_objects[10]);
    duplicate_name_objects[11].object_id = 12;
    duplicate_name_objects[11].parent_object_id = 10;
    duplicate_name_objects[11].target = QST_TARGET_TD;
    duplicate_name_objects[11].object_kind = QST_OBJECT_COLUMN_OR_TAG;
    duplicate_name_objects[11].ordinal = 1;
    duplicate_name_objects[11].flags = QST_OBJECT_FLAG_TAG;
    duplicate_name_objects[11].qualified_name_ref =
        duplicate_name_objects[4].qualified_name_ref;
    duplicate_name_objects[11].schema_ref = schema_ref;
    qstTestObjectDescriptorDigest(&duplicate_name_objects[11]);
    duplicate_name_runtime.objects = duplicate_name_objects;
    duplicate_name_runtime.object_count =
        sizeof(duplicate_name_objects) / sizeof(duplicate_name_objects[0]);
    assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
    for (;;) {
      SQstGeneratedSql sql = {0};
      EQstCode code = qst_generator_full_peek_with_runtime(
          &catalog, &cursor, 0, &duplicate_name_runtime, &sql);

      if (code == QST_E_QUEUE_EMPTY) {
        break;
      }
      assert(code == QST_OK);
      if (strstr(sql.sql, "local_db.local_t | local_db.local_t2 |") != NULL ||
          strstr(sql.sql, "local_db.local_t2 | local_db.local_t |") != NULL) {
        assert(strstr(sql.sql, "| value_col") == NULL);
        assert(strstr(sql.sql, "| tag_col") == NULL);
        assert(strstr(sql.sql, ".value_col") != NULL ||
               strstr(sql.sql, ".tag_col") != NULL);
        ++distinct_table_path_count;
      }
      assert(qst_generator_full_commit(&cursor, &sql) == QST_OK);
      qst_generator_generated_sql_destroy(&sql);
      ++path_count;
    }
    assert(path_count == 24U);
    assert(distinct_table_path_count == 8U);
  }
  if (getenv("QST_CORE_GENERATOR_RANDOM_STRESS_ONLY") != NULL) {
    qstTestCompiledSqlDimensionCatalogRandomStress(&runtime);
    assert(qst_shm_unmap(&mapping) == QST_OK);
    return;
  }
  qstTestCompiledSqlDimensionCatalog(&runtime);
  qstTestCompiledSqlDimensionCatalogReachability();
  if (getenv("QST_CORE_GENERATOR_FULL_EXHAUSTIVE") != NULL) {
    qstTestCompiledSqlDimensionCatalogFull(&runtime);
  }
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestLocalTypeRegistry(void) {
  const SQstLocalTypeDescriptor *types = NULL;
  uint32_t type_count = 0;

  assert(qst_dataset_local_type_registry(&types, &type_count) == QST_OK);
  assert(types != NULL);
  assert(type_count == 22);
  for (uint32_t type_id = 0; type_id < type_count; ++type_id) {
    assert(types[type_id].type_id == type_id);
    assert(types[type_id].canonical_name != NULL);
  }
  assert((types[0].roles & QST_TYPE_ROLE_PSEUDO) != 0);
  assert(types[8].ddl_type != NULL);
  assert((types[15].roles & QST_TYPE_ROLE_TAG) != 0);
  assert((types[15].roles & QST_TYPE_ROLE_COLUMN) == 0);
  assert(types[17].ddl_type != NULL);
  assert(types[21].ddl_type != NULL);
}

static SQstGeneratorCatalog qstTestGeneratorCatalog(void) {
  static const SQstGeneratorChildCall root_normal_children[] = {
      {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
  };
  static const SQstGeneratorChildCall root_abnormal_children[] = {
      {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
  };
  static const SQstGeneratorChildCall expression_recursive_children[] = {
      {2, QST_GENERATOR_CALL_REQUIRED, 1, 0, 0, 0},
      {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
  };
  static const SQstGeneratorValue root_values[] = {
      {101, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 7, 1, 0, root_normal_children,
       "SELECT "},
      {102, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_ABNORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 3, 1, 0, root_abnormal_children,
       "BROKEN "},
  };
  static const SQstGeneratorValue expression_values[] = {
      {201, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 5, 0, 0, NULL, "0"},
      {202, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 5, 0, 0, NULL, "1"},
      {203, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 2, 0, expression_recursive_children,
       "("},
  };
  static const SQstGeneratorValue close_values[] = {
      {301, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, ")"},
  };
  static const SQstGeneratorFragment fragments[] = {
      {1, 1, 2, root_values},
      {2, 1, 3, expression_values},
      {3, 1, 1, close_values},
  };
  SQstGeneratorCatalog catalog = {0};

  catalog.version = 1;
  catalog.root_fragment_id = 1;
  catalog.fragment_count = sizeof(fragments) / sizeof(fragments[0]);
  catalog.fragments = fragments;
  assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
  return catalog;
}

static void qstTestGeneratorCatalogAndDepth(void) {
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorCursor cursor = {0};
  SQstGeneratedSql sql = {0};
  SQstGeneratorCatalog invalid = catalog;
  SQstGeneratorCatalog altered = catalog;
  SQstDigest256 original_digest = catalog.digest;
  static const SQstGeneratorChildCall invalid_child[] = {
      {1, QST_GENERATOR_CALL_REQUIRED, 1, 0, 0, 0},
  };
  static const SQstGeneratorValue invalid_values[] = {
      {401, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 1, 0, invalid_child, "("},
  };
  static const SQstGeneratorFragment invalid_fragments[] = {
      {1, 1, 1, invalid_values},
  };

  assert(qst_generator_catalog_validate(&catalog) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "SELECT 0") == 0);
  assert(sql.choice_count == 2);
  assert(sql.choice_cardinalities[1] == 2);
  assert(sql.contains_abnormal == 0);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "SELECT 0") == 0);
  assert(qst_generator_full_commit(&cursor, &sql) == QST_OK);
  assert(qst_generator_full_next(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "SELECT 1") == 0);
  altered.version = catalog.version + 1U;
  assert(qst_generator_catalog_digest(&altered, &altered.digest) == QST_OK);
  assert(memcmp(original_digest.bytes, altered.digest.bytes,
                sizeof(original_digest.bytes)) != 0);
  invalid.digest.bytes[0] ^= 1U;
  assert(qst_generator_catalog_validate(&invalid) == QST_E_SHM_CORRUPT);
  memset(&invalid, 0, sizeof(invalid));
  invalid.version = 1;
  invalid.root_fragment_id = 1;
  invalid.fragment_count = 1;
  invalid.fragments = invalid_fragments;
  assert(qst_generator_catalog_digest(&invalid, &invalid.digest) == QST_OK);
  assert(qst_generator_catalog_validate(&invalid) == QST_E_INVALID_ARGUMENT);
  qst_generator_generated_sql_destroy(&sql);
}

static void qstTestGeneratorFragPlanPreparation(void) {
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorFragPlan plan = {0};

  assert(qst_generator_frag_plan_prepare(&plan, &catalog) == QST_OK);
  assert(plan.catalog == &catalog);
  assert(plan.fragment_count == catalog.fragment_count);
  assert(plan.root_fragment_index == 0);
  assert(plan.fragments[0].fragment == &catalog.fragments[0]);
  assert(plan.fragments[0].ordered_index == 0);
  assert(plan.fragments[1].fragment_id == 2);
  assert(plan.values[0].value == &catalog.fragments[0].values[0]);
  assert(plan.values[0].fragment_index == 0);
  assert(plan.values[0].operation_count == 1);
  assert(plan.operations[0].operation_kind == QST_GENERATOR_STEP_CALL);
  assert(plan.operations[0].child_fragment_index == 1);
  assert((plan.values[0].eligible_depth_mask & UINT32_C(1)) != 0);
  assert((plan.values[3].eligible_depth_mask &
          (UINT32_C(1) << QST_GENERATOR_DEFAULT_QUERY_DEPTH)) != 0);
  qst_generator_frag_plan_destroy(&plan);
  assert(plan.fragments == NULL);
  assert(plan.values == NULL);
  assert(plan.operations == NULL);
}

static void qstTestGeneratedSqlEqual(const SQstGeneratedSql *left,
                                              const SQstGeneratedSql *right) {
  assert(left != NULL && right != NULL);
  assert(left->choice_count == right->choice_count);
  assert(left->occurrence_count == right->occurrence_count);
  assert(left->sql_length == right->sql_length);
  assert(left->contains_abnormal == right->contains_abnormal);
  assert(left->contains_unsupported_fallback ==
         right->contains_unsupported_fallback);
  assert(memcmp(left->sql, right->sql, left->sql_length) == 0);
  assert(memcmp(left->sql_sha256.bytes, right->sql_sha256.bytes,
                sizeof(left->sql_sha256.bytes)) == 0);
  assert(memcmp(left->choice_cardinalities, right->choice_cardinalities,
                (size_t)left->choice_count *
                    sizeof(*left->choice_cardinalities)) == 0);
  assert(memcmp(left->selected_value_ids, right->selected_value_ids,
                (size_t)left->choice_count *
                    sizeof(*left->selected_value_ids)) == 0);
  assert(memcmp(left->occurrences, right->occurrences,
                (size_t)left->occurrence_count *
                    sizeof(*left->occurrences)) == 0);
}

/* The publication walker omits only process-local path metadata. Its SQL and
 * execution-relevant generation attributes must remain byte-for-byte equal. */
static void qstTestGeneratedSqlExecutionEqual(const SQstGeneratedSql *left,
                                              const SQstGeneratedSql *right) {
  assert(left != NULL && right != NULL);
  assert(left->choice_count == right->choice_count);
  assert(left->sql_length == right->sql_length);
  assert(left->contains_abnormal == right->contains_abnormal);
  assert(left->contains_unsupported_fallback ==
         right->contains_unsupported_fallback);
  assert(memcmp(left->sql, right->sql, left->sql_length) == 0);
  assert(memcmp(left->sql_sha256.bytes, right->sql_sha256.bytes,
                sizeof(left->sql_sha256.bytes)) == 0);
}

static void qstTestGeneratorFragPlanSelectionEquivalence(void) {
  static const uint64_t kSeeds[] = {
      UINT64_C(1), UINT64_C(0x1234), UINT64_C(0x5a17c0de),
      UINT64_C(0x9e3779b97f4a7c15),
  };
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorFragPlan plan = {0};
  SQstGeneratorRandomRuntime direct_runtime = {0};
  SQstGeneratorRandomRuntime plan_runtime = {0};
  SQstGeneratorRandomRuntime fast_runtime = {0};
  SQstDigest256 first_seed_sql[128] = {{0}};
  uint32_t first_seed_target[128] = {0};

  config.run_id = UINT64_C(5410000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 2;
  config.block_bytes = 256;
  config.block_count = 64;
  fast_runtime.disable_path_metadata = 1;
  assert(qst_generator_frag_plan_prepare(&plan, &catalog) == QST_OK);

  for (uint32_t seed_index = 0;
       seed_index < sizeof(kSeeds) / sizeof(kSeeds[0]); ++seed_index) {
    uint32_t target_bits = 0;
    uint32_t changed_sql_count = 0;
    config.run_id = UINT64_C(5410000000) + (uint64_t)getpid() + seed_index;
    assert(qst_shm_create(&config, &mapping) == QST_OK);
    assert(qst_generator_state_initialize(
               &mapping, &catalog, QST_MODE_RANDOM, QST_FOCUS_COVERAGE,
               QST_GENERATOR_DIALECT_MASK_ALL, kSeeds[seed_index], 0, 0) ==
           QST_OK);
    for (uint64_t sequence = 1; sequence <= 128; ++sequence) {
      SQstGeneratorRandomPending direct = {0};
      SQstGeneratorRandomPending compiled = {0};
      SQstGeneratorRandomPending fast = {0};

      assert(qst_generator_random_build_sequence(
                 &mapping, QST_ROLE_GENERATOR, 1, &catalog,
                 (SQstDigest256){{0}}, 7, sequence, &direct_runtime,
                 &direct) == QST_OK);
      assert(qst_generator_random_plan_build_sequence(
                 &mapping, QST_ROLE_GENERATOR, 1, &plan,
                 (SQstDigest256){{0}}, 7, sequence, &plan_runtime,
                 &compiled) == QST_OK);
      assert(qst_generator_random_plan_build_sequence(
                 &mapping, QST_ROLE_GENERATOR, 1, &plan,
                 (SQstDigest256){{0}}, 7, sequence, &fast_runtime,
                 &fast) == QST_OK);
      assert(direct.input.target == compiled.input.target);
      assert(direct.input.mode == compiled.input.mode);
      assert(direct.input.focus == compiled.input.focus);
      assert(direct.input.dialect == compiled.input.dialect);
      assert(direct.input.generation_seq == compiled.input.generation_seq);
      assert(direct.input.weight_profile_version ==
             compiled.input.weight_profile_version);
      assert(direct.input.catalog_version == compiled.input.catalog_version);
      assert(memcmp(direct.input.catalog_digest.bytes,
                    compiled.input.catalog_digest.bytes,
                    sizeof(direct.input.catalog_digest.bytes)) == 0);
      assert(memcmp(direct.input.dataset_digest.bytes,
                    compiled.input.dataset_digest.bytes,
                    sizeof(direct.input.dataset_digest.bytes)) == 0);
      qstTestGeneratedSqlEqual(&direct.sql, &compiled.sql);
      target_bits |= UINT32_C(1) << (direct.input.target - 1U);
      if (seed_index == 0) {
        first_seed_sql[sequence - 1U] = direct.sql.sql_sha256;
        first_seed_target[sequence - 1U] = direct.input.target;
      } else if (memcmp(first_seed_sql[sequence - 1U].bytes,
                        direct.sql.sql_sha256.bytes,
                        sizeof(direct.sql.sql_sha256.bytes)) != 0 ||
                 first_seed_target[sequence - 1U] != direct.input.target) {
        ++changed_sql_count;
      }
      assert(compiled.input.target == fast.input.target);
      assert(compiled.input.mode == fast.input.mode);
      assert(compiled.input.focus == fast.input.focus);
      assert(compiled.input.dialect == fast.input.dialect);
      assert(compiled.input.generation_seq == fast.input.generation_seq);
      assert(compiled.input.weight_profile_version ==
             fast.input.weight_profile_version);
      assert(compiled.input.catalog_version == fast.input.catalog_version);
      assert(memcmp(compiled.input.catalog_digest.bytes,
                    fast.input.catalog_digest.bytes,
                    sizeof(compiled.input.catalog_digest.bytes)) == 0);
      assert(memcmp(compiled.input.dataset_digest.bytes,
                    fast.input.dataset_digest.bytes,
                    sizeof(compiled.input.dataset_digest.bytes)) == 0);
      qstTestGeneratedSqlExecutionEqual(&compiled.sql, &fast.sql);
      assert(fast.sql.occurrence_count == 0);
      qst_generator_random_pending_destroy(&direct);
      qst_generator_random_pending_destroy(&compiled);
      qst_generator_random_pending_destroy(&fast);
    }
    /* This is a reachability guard, not an asserted distribution threshold. */
    assert(__builtin_popcount(target_bits) >= 2);
    if (seed_index != 0) {
      assert(changed_sql_count != 0);
    }
    assert(qst_shm_unmap(&mapping) == QST_OK);
  }
  qst_generator_random_runtime_destroy(&direct_runtime);
  qst_generator_random_runtime_destroy(&plan_runtime);
  qst_generator_random_runtime_destroy(&fast_runtime);
  qst_generator_frag_plan_destroy(&plan);
}

enum {
  kGeneratorBenchMaxBuilderCount = 64,
  kGeneratorBenchDefaultPaths = 100000,
};

typedef struct SQstGeneratorBench SQstGeneratorBench;

typedef struct {
  SQstGeneratorBench *bench;
  uint32_t slot;
} SQstGeneratorBenchBuilder;

struct SQstGeneratorBench {
  SQstShmMapping *mapping;
  const SQstGeneratorFragPlan *plan;
  SQstGeneratorBenchBuilder builders[kGeneratorBenchMaxBuilderCount];
  SQstGeneratorRandomRuntime runtime[kGeneratorBenchMaxBuilderCount];
  pthread_t builder_threads[kGeneratorBenchMaxBuilderCount];
  pthread_barrier_t start_barrier;
  uint32_t builder_count;
  uint64_t path_count;
  uint64_t completed[kGeneratorBenchMaxBuilderCount];
  _Atomic int32_t error_code;
};

static void *qstGeneratorBenchBuild(void *arg) {
  SQstGeneratorBenchBuilder *builder = arg;
  SQstGeneratorBench *bench = builder->bench;
  uint64_t completed = 0;

  (void)pthread_barrier_wait(&bench->start_barrier);
  for (uint64_t sequence = (uint64_t)builder->slot + 1U;
       sequence <= bench->path_count; sequence += bench->builder_count) {
    SQstGeneratorRandomState state = {0};
    SQstGeneratorRandomPending pending = {0};

    if (qst_generator_random_init(&state, UINT64_C(0x5a17c0de), sequence) !=
        QST_OK) {
      atomic_store_explicit(&bench->error_code, QST_E_RESOURCE,
                            memory_order_release);
      return NULL;
    }
    if (qst_generator_random_plan_build_from_state(
            bench->mapping, QST_ROLE_GENERATOR, 1, bench->plan,
            (SQstDigest256){{0}}, 1, sequence, &state,
            &bench->runtime[builder->slot], &pending) != QST_OK) {
      qst_generator_random_pending_destroy(&pending);
      atomic_store_explicit(&bench->error_code, QST_E_RESOURCE,
                            memory_order_release);
      return NULL;
    }
    qst_generator_random_pending_destroy(&pending);
    ++completed;
  }
  bench->completed[builder->slot] = completed;
  return NULL;
}

static uint64_t qstTestReadPositiveUInt64(const char *name,
                                          uint64_t default_value) {
  const char *text = NULL;
  char *end = NULL;
  unsigned long long value = 0;

  assert(name != NULL && default_value != 0);
  text = getenv(name);
  if (NULL == text || text[0] == '\0') {
    return default_value;
  }
  errno = 0;
  value = strtoull(text, &end, 10);
  assert(errno == 0 && end != text && *end == '\0' && value != 0);
  return (uint64_t)value;
}

static uint64_t qstTestReadNonnegativeUInt64(const char *name,
                                              uint64_t default_value) {
  const char *text = NULL;
  char *end = NULL;
  unsigned long long value = 0;

  assert(name != NULL);
  text = getenv(name);
  if (NULL == text || text[0] == '\0') {
    return default_value;
  }
  errno = 0;
  value = strtoull(text, &end, 10);
  assert(errno == 0 && end != text && *end == '\0');
  return (uint64_t)value;
}

static uint64_t qstTestCurrentRssBytes(void) {
  FILE *file = NULL;
  unsigned long long total_pages = 0;
  unsigned long long resident_pages = 0;
  long page_size = 0;

  file = fopen("/proc/self/statm", "r");
  if (NULL == file) {
    return 0;
  }
  if (fscanf(file, "%llu %llu", &total_pages, &resident_pages) != 2) {
    (void)fclose(file);
    return 0;
  }
  if (fclose(file) != 0) {
    return 0;
  }
  page_size = sysconf(_SC_PAGESIZE);
  if (page_size <= 0 || resident_pages > UINT64_MAX / (uint64_t)page_size) {
    return 0;
  }
  return (uint64_t)resident_pages * (uint64_t)page_size;
}

static uint64_t qstTestRusageMicroseconds(const struct rusage *usage) {
  assert(usage != NULL);
  return (uint64_t)usage->ru_utime.tv_sec * UINT64_C(1000000) +
         (uint64_t)usage->ru_utime.tv_usec +
         (uint64_t)usage->ru_stime.tv_sec * UINT64_C(1000000) +
         (uint64_t)usage->ru_stime.tv_usec;
}

static void qstTestGeneratorStageBenchmark(uint32_t builder_count,
                                           uint64_t path_count) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorFragPlan plan = {0};
  SQstGeneratorBench *bench = NULL;
  struct timespec started = {0};
  struct timespec finished = {0};
  struct rusage rusage_before = {0};
  struct rusage rusage_after = {0};
  int64_t elapsed_seconds = 0;
  int64_t elapsed_nanoseconds = 0;
  uint64_t elapsed_ns = 0;

  config.run_id = UINT64_C(5510000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  assert(builder_count != 0 && builder_count <= kGeneratorBenchMaxBuilderCount &&
         (builder_count & (builder_count - 1U)) == 0 && path_count != 0);
  config.process_slot_count = 8;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 64;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, &catalog, QST_MODE_RANDOM,
                                        QST_FOCUS_COVERAGE, UINT32_C(1),
                                        UINT64_C(0x5a17c0de), 0, 0) == QST_OK);
  assert(qst_generator_frag_plan_prepare(&plan, &catalog) == QST_OK);
  bench = calloc(1, sizeof(*bench));
  assert(bench != NULL);
  bench->mapping = &mapping;
  bench->plan = &plan;
  bench->builder_count = builder_count;
  bench->path_count = path_count;
  atomic_init(&bench->error_code, QST_OK);
  assert(pthread_barrier_init(&bench->start_barrier, NULL,
                              builder_count + 1U) == 0);
  for (uint32_t slot = 0; slot < builder_count; ++slot) {
    bench->builders[slot].bench = bench;
    bench->builders[slot].slot = slot;
    assert(qst_generator_random_runtime_prepare(&bench->runtime[slot], &catalog) ==
           QST_OK);
  }
  for (uint32_t slot = 0; slot < builder_count; ++slot) {
    assert(pthread_create(&bench->builder_threads[slot], NULL,
                          qstGeneratorBenchBuild, &bench->builders[slot]) == 0);
  }
  assert(getrusage(RUSAGE_SELF, &rusage_before) == 0);
  assert(clock_gettime(CLOCK_MONOTONIC, &started) == 0);
  (void)pthread_barrier_wait(&bench->start_barrier);
  for (uint32_t slot = 0; slot < builder_count; ++slot) {
    assert(pthread_join(bench->builder_threads[slot], NULL) == 0);
  }
  assert(clock_gettime(CLOCK_MONOTONIC, &finished) == 0);
  assert(getrusage(RUSAGE_SELF, &rusage_after) == 0);
  assert(atomic_load_explicit(&bench->error_code, memory_order_acquire) == QST_OK);
  {
    uint64_t completed = 0;

    for (uint32_t slot = 0; slot < builder_count; ++slot) {
      completed += bench->completed[slot];
    }
    assert(completed == path_count);
  }
  elapsed_seconds = (int64_t)finished.tv_sec - (int64_t)started.tv_sec;
  elapsed_nanoseconds = (int64_t)finished.tv_nsec - (int64_t)started.tv_nsec;
  if (elapsed_nanoseconds < 0) {
    --elapsed_seconds;
    elapsed_nanoseconds += INT64_C(1000000000);
  }
  assert(elapsed_seconds >= 0 && elapsed_nanoseconds >= 0);
  elapsed_ns = (uint64_t)elapsed_seconds * UINT64_C(1000000000) +
               (uint64_t)elapsed_nanoseconds;
  assert(elapsed_ns != 0);
  (void)fprintf(stdout,
                "{\"stage\":\"generator_build\",\"builder_threads\":%u,"
                "\"input_count\":%llu,\"completed_count\":%llu,"
                "\"error_count\":0,\"elapsed_ns\":%llu,"
                "\"items_per_second\":%.2f,\"cpu_time_us\":%llu,"
                "\"rss_bytes\":%llu,\"queue\":null}\n",
                builder_count, (unsigned long long)path_count,
                (unsigned long long)path_count, (unsigned long long)elapsed_ns,
                (double)path_count * 1000000000.0 / (double)elapsed_ns,
                (unsigned long long)(qstTestRusageMicroseconds(&rusage_after) -
                                     qstTestRusageMicroseconds(&rusage_before)),
                (unsigned long long)qstTestCurrentRssBytes());
  assert(pthread_barrier_destroy(&bench->start_barrier) == 0);
  for (uint32_t slot = 0; slot < builder_count; ++slot) {
    qst_generator_random_runtime_destroy(&bench->runtime[slot]);
  }
  free(bench);
  qst_generator_frag_plan_destroy(&plan);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestGeneratorStageBenchmarkMatrix(void) {
  static const uint32_t builder_counts[] = {1U, 4U, 8U, 16U, 32U, 64U};
  const uint64_t path_count = qstTestReadPositiveUInt64(
      "QST_STAGE_BENCH_ITEMS", kGeneratorBenchDefaultPaths);

  for (uint32_t index = 0;
       index < sizeof(builder_counts) / sizeof(builder_counts[0]); ++index) {
    qstTestGeneratorStageBenchmark(builder_counts[index], path_count);
  }
}

enum {
  kGeneratorRealStageBuilderCount = 4,
  kGeneratorRealStageDefaultItems = 16384,
};

typedef struct SQstGeneratorRealStageBench SQstGeneratorRealStageBench;

typedef struct {
  SQstGeneratorRealStageBench *bench;
  uint32_t slot;
} SQstGeneratorRealStageBuilder;

struct SQstGeneratorRealStageBench {
  SQstShmMapping *mapping;
  const SQstGeneratorFragPlan *plan;
  uint32_t generator_generation;
  uint64_t item_count;
  int32_t verify_only;
  SQstDigest256 *sql_digests;
  uint32_t *targets;
  pthread_barrier_t start_barrier;
  pthread_t threads[kGeneratorRealStageBuilderCount];
  SQstGeneratorRealStageBuilder builders[kGeneratorRealStageBuilderCount];
  SQstGeneratorRandomRuntime runtimes[kGeneratorRealStageBuilderCount];
  uint64_t completed[kGeneratorRealStageBuilderCount];
  _Atomic int32_t error_code;
};

static void qstTestGeneratorRealStageSetError(SQstGeneratorRealStageBench *bench,
                                              EQstCode code) {
  int32_t expected = QST_OK;

  assert(bench != NULL && code != QST_OK);
  (void)atomic_compare_exchange_strong_explicit(
      &bench->error_code, &expected, code, memory_order_release,
      memory_order_relaxed);
}

static void *qstTestGeneratorRealStageBuild(void *arg) {
  SQstGeneratorRealStageBuilder *builder = arg;
  SQstGeneratorRealStageBench *bench = builder->bench;
  SQstGeneratorRandomRuntime *runtime = &bench->runtimes[builder->slot];
  uint64_t completed = 0;

  assert(builder != NULL && bench != NULL);
  (void)pthread_barrier_wait(&bench->start_barrier);
  for (uint64_t sequence = (uint64_t)builder->slot + 1U;
       sequence <= bench->item_count;
       sequence += kGeneratorRealStageBuilderCount) {
    SQstGeneratorRandomPending pending = {0};
    EQstCode code = qst_generator_random_plan_build_sequence(
        bench->mapping, QST_ROLE_GENERATOR, bench->generator_generation,
        bench->plan, (SQstDigest256){{1}}, 1, sequence, runtime, &pending);
    const uint64_t index = sequence - 1U;

    if (code != QST_OK || pending.input.generation_seq != sequence ||
        pending.sql.choice_count == 0 || pending.sql.occurrence_count != 0 ||
        pending.sql.occurrences != NULL || pending.sql.choice_cardinalities != NULL ||
        pending.sql.selected_value_ids != NULL) {
      qst_generator_random_pending_destroy(&pending);
      qstTestGeneratorRealStageSetError(
          bench, code == QST_OK ? QST_E_SHM_CORRUPT : code);
      return NULL;
    }
    if (bench->verify_only != 0) {
      if (memcmp(bench->sql_digests[index].bytes, pending.sql.sql_sha256.bytes,
                 sizeof(pending.sql.sql_sha256.bytes)) != 0 ||
          bench->targets[index] != pending.input.target) {
        qst_generator_random_pending_destroy(&pending);
        qstTestGeneratorRealStageSetError(bench, QST_E_SHM_CORRUPT);
        return NULL;
      }
    } else {
      bench->sql_digests[index] = pending.sql.sql_sha256;
      bench->targets[index] = pending.input.target;
    }
    qst_generator_random_pending_destroy(&pending);
    ++completed;
  }
  bench->completed[builder->slot] = completed;
  return NULL;
}

static void qstTestGeneratorRealStageRun(SQstGeneratorRealStageBench *bench,
                                         int32_t measure,
                                         uint64_t *out_elapsed_ns,
                                         uint64_t *out_cpu_time_us) {
  struct timespec started = {0};
  struct timespec finished = {0};
  struct rusage rusage_before = {0};
  struct rusage rusage_after = {0};
  int64_t elapsed_seconds = 0;
  int64_t elapsed_nanoseconds = 0;
  uint64_t completed = 0;

  assert(bench != NULL && out_elapsed_ns != NULL && out_cpu_time_us != NULL);
  memset(bench->completed, 0, sizeof(bench->completed));
  atomic_store_explicit(&bench->error_code, QST_OK, memory_order_release);
  assert(pthread_barrier_init(&bench->start_barrier, NULL,
                              kGeneratorRealStageBuilderCount + 1U) == 0);
  for (uint32_t slot = 0; slot < kGeneratorRealStageBuilderCount; ++slot) {
    bench->builders[slot].bench = bench;
    bench->builders[slot].slot = slot;
    assert(pthread_create(&bench->threads[slot], NULL,
                          qstTestGeneratorRealStageBuild,
                          &bench->builders[slot]) == 0);
  }
  assert(getrusage(RUSAGE_SELF, &rusage_before) == 0);
  assert(clock_gettime(CLOCK_MONOTONIC, &started) == 0);
  (void)pthread_barrier_wait(&bench->start_barrier);
  for (uint32_t slot = 0; slot < kGeneratorRealStageBuilderCount; ++slot) {
    assert(pthread_join(bench->threads[slot], NULL) == 0);
    completed += bench->completed[slot];
  }
  assert(clock_gettime(CLOCK_MONOTONIC, &finished) == 0);
  assert(getrusage(RUSAGE_SELF, &rusage_after) == 0);
  assert(pthread_barrier_destroy(&bench->start_barrier) == 0);
  assert(atomic_load_explicit(&bench->error_code, memory_order_acquire) == QST_OK);
  assert(completed == bench->item_count);
  elapsed_seconds = (int64_t)finished.tv_sec - (int64_t)started.tv_sec;
  elapsed_nanoseconds = (int64_t)finished.tv_nsec - (int64_t)started.tv_nsec;
  if (elapsed_nanoseconds < 0) {
    --elapsed_seconds;
    elapsed_nanoseconds += INT64_C(1000000000);
  }
  assert(elapsed_seconds >= 0 && elapsed_nanoseconds >= 0);
  *out_elapsed_ns = (uint64_t)elapsed_seconds * UINT64_C(1000000000) +
                    (uint64_t)elapsed_nanoseconds;
  *out_cpu_time_us = qstTestRusageMicroseconds(&rusage_after) -
                     qstTestRusageMicroseconds(&rusage_before);
  if (measure != 0) {
    assert(*out_elapsed_ns != 0);
  }
}

static void qstTestGeneratorRealStageBenchmark(void) {
  static const uint8_t object_magic[8] = {'Q', 'S', 'T', 'O', 'B', 'J', '1', '0'};
  static const char *const names[] = {
      "tdengine", "local_db", "local_t", "value_col", "tag_col",
      "src_mysql", "remote_db", "remote_t", "remote_col", "local_t2",
      "value_col2",
  };
  static const uint32_t kinds[] = {
      QST_OBJECT_SOURCE, QST_OBJECT_DATABASE_OR_SCHEMA,
      QST_OBJECT_TABLE_OR_MEASUREMENT, QST_OBJECT_COLUMN_OR_TAG,
      QST_OBJECT_COLUMN_OR_TAG, QST_OBJECT_SOURCE,
      QST_OBJECT_DATABASE_OR_SCHEMA, QST_OBJECT_TABLE_OR_MEASUREMENT,
      QST_OBJECT_COLUMN_OR_TAG, QST_OBJECT_TABLE_OR_MEASUREMENT,
      QST_OBJECT_COLUMN_OR_TAG,
  };
  static const uint32_t targets[] = {
      QST_TARGET_TD, QST_TARGET_TD, QST_TARGET_TD, QST_TARGET_TD,
      QST_TARGET_TD, QST_TARGET_MYSQL, QST_TARGET_MYSQL, QST_TARGET_MYSQL,
      QST_TARGET_MYSQL, QST_TARGET_TD, QST_TARGET_TD,
  };
  static const uint64_t parents[] = {0, 1, 2, 3, 3, 0, 6, 7, 8, 2, 10};
  static const uint32_t ordinals[] = {0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0};
  const SQstGeneratorCatalog *catalog = NULL;
  const uint64_t item_count = qstTestReadPositiveUInt64(
      "QST_STAGE_BENCH_ITEMS", kGeneratorRealStageDefaultItems);
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstObjectDescriptor objects[sizeof(names) / sizeof(names[0])] = {{0}};
  SQstGeneratorFragPlan plan = {0};
  SQstGeneratorRealStageBench bench = {0};
  SQstRef schema_ref = {0};
  SQstRef object_directory_ref = {0};
  SQstRef fixture_manifest_ref = {0};
  SQstDigest256 dataset_digest = {{1}};
  SQstDigest256 fixture_digest = {{2}};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint64_t elapsed_ns = 0;
  uint64_t cpu_time_us = 0;

  assert(qst_sql_dimension_catalog(&catalog) == QST_OK && catalog != NULL);
  assert(item_count <= UINT32_MAX);
  config.run_id = UINT64_C(5520000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(1048576);
  config.process_slot_count = 4;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 1024;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  schema_ref = qstSealTestBytes(
      &mapping, (const uint8_t *)"schema", sizeof("schema") - 1U);
  for (uint32_t index = 0; index < sizeof(objects) / sizeof(objects[0]); ++index) {
    objects[index].object_id = index + 1U;
    objects[index].parent_object_id = parents[index];
    objects[index].target = targets[index];
    objects[index].object_kind = kinds[index];
    objects[index].ordinal = ordinals[index];
    objects[index].flags = index == 4 ? QST_OBJECT_FLAG_TAG : 0;
    objects[index].qualified_name_ref = qstSealTestBytes(
        &mapping, (const uint8_t *)names[index], (uint32_t)strlen(names[index]));
    objects[index].schema_ref = schema_ref;
    qstTestObjectDescriptorDigest(&objects[index]);
  }
  object_directory_ref = qstSealManifest(
      &mapping, object_magic, objects, sizeof(objects[0]),
      sizeof(objects) / sizeof(objects[0]));
  fixture_manifest_ref = qstSealTestBytes(
      &mapping, (const uint8_t *)"fixture", sizeof("fixture") - 1U);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, (int32_t)getpid(),
                              (int32_t)getpgrp(), &generator_generation) == QST_OK);
  assert(qst_generator_state_initialize(
             &mapping, catalog, QST_MODE_RANDOM, QST_FOCUS_COVERAGE,
             UINT32_C(1), UINT64_C(20260902), item_count, 0) == QST_OK);
  assert(qst_generator_state_publish_dataset(
             &mapping, main_generation, dataset_digest, fixture_digest,
             object_directory_ref, fixture_manifest_ref) == QST_OK);
  assert(qst_generator_frag_plan_prepare(&plan, catalog) == QST_OK);
  bench.mapping = &mapping;
  bench.plan = &plan;
  bench.generator_generation = generator_generation;
  bench.item_count = item_count;
  bench.sql_digests = calloc((size_t)item_count, sizeof(*bench.sql_digests));
  bench.targets = calloc((size_t)item_count, sizeof(*bench.targets));
  assert(bench.sql_digests != NULL && bench.targets != NULL);
  for (uint32_t slot = 0; slot < kGeneratorRealStageBuilderCount; ++slot) {
    bench.runtimes[slot].disable_path_metadata = 1;
  }
  qstTestGeneratorRealStageRun(&bench, 1, &elapsed_ns, &cpu_time_us);
  bench.verify_only = 1;
  qstTestGeneratorRealStageRun(&bench, 0, &(uint64_t){0}, &(uint64_t){0});
  (void)fprintf(stdout,
                "{\"stage\":\"generator_real_plan_build\","
                "\"builder_threads\":%u,\"input_count\":%llu,"
                "\"completed_count\":%llu,\"error_count\":0,"
                "\"elapsed_ns\":%llu,\"items_per_second\":%.2f,"
                "\"cpu_time_us\":%llu,\"rss_bytes\":%llu,"
                "\"catalog\":\"sql_dimension_catalog_source\","
                "\"path_metadata\":false,\"deterministic_sql_sha256\":true,"
                "\"queue\":null}\n",
                kGeneratorRealStageBuilderCount,
                (unsigned long long)item_count, (unsigned long long)item_count,
                (unsigned long long)elapsed_ns,
                (double)item_count * 1000000000.0 / (double)elapsed_ns,
                (unsigned long long)cpu_time_us,
                (unsigned long long)qstTestCurrentRssBytes());
  for (uint32_t slot = 0; slot < kGeneratorRealStageBuilderCount; ++slot) {
    qst_generator_random_runtime_destroy(&bench.runtimes[slot]);
  }
  free(bench.targets);
  free(bench.sql_digests);
  qst_generator_frag_plan_destroy(&plan);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

enum {
  kQueueStageBenchMaxWorkers = 64,
  kQueueStageBenchDefaultItems = 16384,
  kQueueStageBenchBlockBytes = 128,
};

typedef enum {
  QST_QUEUE_STAGE_PAIR_CLAIM_ACK = 1,
  QST_QUEUE_STAGE_JUDGE_TO_PERSIST = 2,
} EQstQueueStageBenchKind;

typedef struct SQstQueueStageBench SQstQueueStageBench;

typedef struct {
  SQstQueueStageBench *bench;
  uint32_t slot;
  uint32_t generation;
} SQstQueueStageBenchWorker;

struct SQstQueueStageBench {
  SQstShmMapping *mapping;
  EQstQueueStageBenchKind kind;
  uint32_t worker_count;
  uint32_t queue_capacity;
  uint64_t item_count;
  pthread_barrier_t start_barrier;
  pthread_t threads[kQueueStageBenchMaxWorkers];
  SQstQueueStageBenchWorker workers[kQueueStageBenchMaxWorkers];
  _Atomic uint64_t completed_count;
  _Atomic int32_t error_code;
};

static uint32_t qstTestRoundUpPowerOfTwo(uint64_t value) {
  uint64_t rounded = 2;

  assert(value != 0 && value <= UINT32_MAX / 2U);
  while (rounded < value) {
    rounded <<= 1U;
  }
  assert(rounded <= UINT32_MAX);
  return (uint32_t)rounded;
}

static void qstTestQueueStageSetError(SQstQueueStageBench *bench,
                                      EQstCode code) {
  int32_t expected = QST_OK;

  assert(bench != NULL && code != QST_OK);
  (void)atomic_compare_exchange_strong_explicit(
      &bench->error_code, &expected, code, memory_order_release,
      memory_order_relaxed);
}

static SQstMessage qstTestPersistMessage(const SQstClaim *claim,
                                         uint32_t worker_slot,
                                         uint32_t worker_generation) {
  SQstMessage message = {0};

  assert(claim != NULL);
  message.message_type = QST_MSG_PERSIST_CONFIRM;
  message.producer_role = QST_ROLE_JUDGE_WORKER;
  message.producer_slot_id = worker_slot;
  message.producer_generation = worker_generation;
  message.target = claim->message.target;
  message.run_id = claim->message.run_id;
  message.gid = claim->message.gid;
  message.dispatch_epoch = claim->message.dispatch_epoch;
  message.transition_seq = claim->message.transition_seq;
  message.body.persist.persist_kind = QST_PERSIST_STATEMENT;
  message.body.persist.durable_source = QST_DURABLE_RESULT_STORE;
  message.body.persist.target = message.target;
  message.body.persist.gid = message.gid;
  message.body.persist.dispatch_epoch = message.dispatch_epoch;
  message.body.persist.transition_seq = message.transition_seq;
  message.body.persist.record_seq = message.gid;
  return message;
}

static void *qstTestQueueStageWorkerRun(void *arg) {
  SQstQueueStageBenchWorker *worker = arg;
  SQstQueueStageBench *bench = worker->bench;
  const EQstQueueId input_queue =
      bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ? QST_QUEUE_GENERATE :
                                                      QST_QUEUE_EXECUTE;
  const EQstRole role =
      bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ? QST_ROLE_PAIR_SUPERVISOR :
                                                      QST_ROLE_JUDGE_WORKER;
  const EQstAckProofKind ack_kind =
      bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ? QST_ACK_TASK_DISPATCHED :
                                                      QST_ACK_PERSIST_WORK_ACCEPTED;

  (void)pthread_barrier_wait(&bench->start_barrier);
  for (;;) {
    SQstClaim claim = {0};
    EQstCode code = qst_queue_claim_instance(
        bench->mapping, input_queue, role, worker->slot, worker->generation,
        UINT64_MAX, &claim);

    if (code == QST_E_QUEUE_EMPTY) {
      return NULL;
    }
    if (code != QST_OK) {
      qstTestQueueStageSetError(bench, code);
      return NULL;
    }
    if (bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK) {
      SQstTaskDispatch dispatch = {0};

      code = qst_task_claim_dispatch_pair(
          bench->mapping, claim.message.gid, worker->slot, worker->generation,
          claim.message.dispatch_epoch, &claim.message.body.generate, &dispatch);
      if (code == QST_OK) {
        code = qst_task_abort_dispatch(
            bench->mapping, dispatch.gid, dispatch.dispatch_epoch,
            dispatch.transition_seq, QST_ROLE_PAIR_SUPERVISOR, worker->slot,
            worker->generation);
      }
      if (code != QST_OK) {
        qstTestQueueStageSetError(bench, code);
        return NULL;
      }
      claim.message.transition_seq = dispatch.transition_seq;
    }
    if (bench->kind == QST_QUEUE_STAGE_JUDGE_TO_PERSIST) {
      SQstTaskPersistSnapshot snapshot = {0};
      const SQstMessage persist = qstTestPersistMessage(
          &claim, worker->slot, worker->generation);

      code = qst_task_get_persist_snapshot(
          bench->mapping, claim.message.gid, claim.message.dispatch_epoch,
          claim.message.transition_seq, &snapshot);
      if (code == QST_OK) {
        code = qst_task_begin_persist(
            bench->mapping, snapshot.gid, snapshot.dispatch_epoch,
            snapshot.transition_seq, QST_ROLE_JUDGE_WORKER, worker->slot,
            worker->generation, -1, QST_PERSIST_MASK_STATEMENT);
      }
      if (code == QST_OK) {
        code = qst_queue_publish(bench->mapping, QST_QUEUE_PERSIST, &persist,
                                 UINT64_MAX);
      }
      if (code != QST_OK) {
        qstTestQueueStageSetError(bench, code);
        return NULL;
      }
    }
    code = qst_queue_ack(bench->mapping, &claim, ack_kind,
                         bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ?
                             claim.message.transition_seq : claim.message.gid);
    if (code != QST_OK) {
      qstTestQueueStageSetError(bench, code);
      return NULL;
    }
    (void)atomic_fetch_add_explicit(&bench->completed_count, 1,
                                    memory_order_relaxed);
  }
}

static void qstTestQueueStageDrainPersist(SQstShmMapping *mapping,
                                          uint32_t main_generation,
                                          uint64_t item_count,
                                          int32_t reclaim_tasks) {
  for (uint64_t index = 0; index < item_count; ++index) {
    SQstClaim claim = {0};

    assert(qst_queue_claim(mapping, QST_QUEUE_PERSIST, QST_ROLE_MAIN,
                           main_generation, UINT64_MAX, &claim) == QST_OK);
    if (reclaim_tasks != 0) {
      assert(qst_task_mark_durable(
                 mapping, claim.message.gid, claim.message.dispatch_epoch,
                 QST_PERSIST_MASK_STATEMENT,
                 claim.message.body.persist.durable_source,
                 claim.message.body.persist.record_seq, QST_ROLE_MAIN,
                 main_generation) == QST_OK);
    }
    assert(qst_queue_ack(mapping, &claim, QST_ACK_DURABILITY_APPLIED,
                         claim.message.body.persist.record_seq) == QST_OK);
    if (reclaim_tasks != 0) {
      assert(qst_task_release_persisted_blocks(
                 mapping, claim.message.gid, QST_ROLE_MAIN,
                 main_generation) == QST_OK);
      assert(qst_task_reclaim_persisted(mapping, claim.message.gid,
                                        QST_ROLE_MAIN, main_generation) ==
             QST_OK);
    }
  }
  assert(qst_queue_claim(mapping, QST_QUEUE_PERSIST, QST_ROLE_MAIN,
                         main_generation, UINT64_MAX,
                         &(SQstClaim){0}) == QST_E_QUEUE_EMPTY);
}

static void qstTestQueueStagePrintResult(
    const char *stage, uint32_t worker_count, uint64_t item_count,
    uint64_t elapsed_ns, uint64_t cpu_time_us, uint64_t rss_bytes,
    uint32_t queue_capacity, const SQstQueueSnapshot *input_snapshot,
    const SQstQueueSnapshot *output_snapshot) {
  const uint64_t max_depth = input_snapshot->enqueue_pos;
  const double saturation = 100.0 * (double)max_depth / (double)queue_capacity;

  assert(stage != NULL && input_snapshot != NULL);
  (void)fprintf(stdout,
                "{\"stage\":\"%s\",\"worker_threads\":%u,"
                "\"input_count\":%llu,\"completed_count\":%llu,"
                "\"error_count\":0,\"elapsed_ns\":%llu,"
                "\"items_per_second\":%.2f,\"cpu_time_us\":%llu,"
                "\"rss_bytes\":%llu,\"queue\":{"
                "\"capacity\":%u,\"input_enqueue_count\":%llu,"
                "\"input_dequeue_count\":%llu,\"max_depth\":%llu,"
                "\"max_saturation_percent\":%.2f",
                stage, worker_count, (unsigned long long)item_count,
                (unsigned long long)item_count, (unsigned long long)elapsed_ns,
                (double)item_count * 1000000000.0 / (double)elapsed_ns,
                (unsigned long long)cpu_time_us, (unsigned long long)rss_bytes,
                queue_capacity,
                (unsigned long long)input_snapshot->enqueue_pos,
                (unsigned long long)input_snapshot->dequeue_pos,
                (unsigned long long)max_depth, saturation);
  if (NULL != output_snapshot) {
    (void)fprintf(stdout,
                  ",\"output_enqueue_count\":%llu,"
                  "\"output_dequeue_count\":%llu",
                  (unsigned long long)output_snapshot->enqueue_pos,
                  (unsigned long long)output_snapshot->dequeue_pos);
  }
  (void)fprintf(stdout, "}}\n");
}

static void qstTestQueueStageRegisterWorkers(
    SQstQueueStageBench *bench, uint32_t main_generation,
    uint32_t *out_generator_generation, uint32_t *out_exec_generation,
    uint32_t *out_producer_generation) {
  uint32_t parent_generation = 0;
  EQstRole worker_role = QST_ROLE_PAIR_SUPERVISOR;

  assert(bench != NULL && out_generator_generation != NULL &&
         out_exec_generation != NULL &&
         out_producer_generation != NULL);
  assert(main_generation != 0);
  if (bench->kind == QST_QUEUE_STAGE_JUDGE_TO_PERSIST) {
    worker_role = QST_ROLE_JUDGE_WORKER;
  }
  assert(qst_process_register(bench->mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, (int32_t)getpid(),
                              (int32_t)getpgrp(), out_generator_generation) ==
         QST_OK);
  assert(qst_process_register(bench->mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, (int32_t)getpid(),
                              (int32_t)getpgrp(), out_exec_generation) == QST_OK);
  parent_generation = *out_exec_generation;
  if (bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK) {
    *out_producer_generation = parent_generation;
  } else {
    assert(qst_process_register_instance(
               bench->mapping, QST_ROLE_EXEC_MANAGER, 0, parent_generation,
               QST_ROLE_PAIR_SUPERVISOR, 0, (int32_t)getpid(),
               (int32_t)getpgrp(), out_producer_generation) == QST_OK);
  }
  for (uint32_t slot = 0; slot < bench->worker_count; ++slot) {
    uint32_t worker_generation = 0;

    assert(qst_process_register_instance(
               bench->mapping,
               bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ?
                   QST_ROLE_EXEC_MANAGER : QST_ROLE_MAIN,
               0, bench->kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ?
                      parent_generation : main_generation,
               worker_role, slot, (int32_t)getpid(),
               (int32_t)getpgrp(), &worker_generation) == QST_OK);
    bench->workers[slot].bench = bench;
    bench->workers[slot].slot = slot;
    bench->workers[slot].generation = worker_generation;
  }
}

static void qstTestQueueStagePrefillPair(
    SQstQueueStageBench *bench, uint32_t generator_generation,
    SQstRef *sql_refs, SQstRef *record_refs) {
  static const uint8_t sql[] = "SELECT 1";
  static const uint8_t record[] = "{\"generator\":\"bench\"}";

  assert(bench != NULL && sql_refs != NULL && record_refs != NULL);
  for (uint64_t index = 0; index < bench->item_count; ++index) {
    uint64_t gid = 0;
    uint64_t dispatch_epoch = 0;
    SQstMessage message = {0};
    SQstGeneratePayload *payload = &message.body.generate;

    assert(qst_task_reserve_gid(bench->mapping, QST_ROLE_GENERATOR,
                                generator_generation, &gid) == QST_OK);
    assert(qst_block_alloc(bench->mapping, gid, 1, sizeof(sql) - 1U,
                           &sql_refs[index]) == QST_OK);
    assert(qst_block_write_seal(bench->mapping, &sql_refs[index], sql,
                                sizeof(sql) - 1U) == QST_OK);
    assert(qst_block_alloc(bench->mapping, gid, 1, sizeof(record) - 1U,
                           &record_refs[index]) == QST_OK);
    assert(qst_block_write_seal(bench->mapping, &record_refs[index], record,
                                sizeof(record) - 1U) == QST_OK);
    message.message_type = QST_MSG_GENERATE_WORK;
    message.producer_role = QST_ROLE_GENERATOR;
    message.producer_generation = generator_generation;
    message.target = QST_TARGET_TD;
    message.run_id = bench->mapping->header->run_id;
    message.gid = gid;
    message.dispatch_epoch = 0;
    message.transition_seq = 1;
    payload->target = message.target;
    payload->mode = QST_MODE_FULL;
    payload->focus = QST_FOCUS_COVERAGE;
    payload->dialect = QST_DIALECT_TDENGINE;
    payload->generation_seq = gid;
    payload->weight_profile_version = 1;
    payload->sql_ref = sql_refs[index];
    payload->generation_record_ref = record_refs[index];
    assert(qst_block_sha256(bench->mapping, record_refs[index], gid,
                            &payload->generation_record_sha256) == QST_OK);
    assert(qst_task_create(bench->mapping, gid, QST_ROLE_GENERATOR,
                           generator_generation, payload,
                           &dispatch_epoch) == QST_OK);
    message.dispatch_epoch = dispatch_epoch;
    assert(qst_queue_publish(bench->mapping, QST_QUEUE_GENERATE, &message,
                             UINT64_MAX) == QST_OK);
  }
}

static void qstTestQueueStagePrefillJudge(SQstQueueStageBench *bench,
                                          uint32_t generator_generation,
                                          uint32_t exec_generation,
                                          uint32_t pair_generation,
                                          SQstRef *sql_refs,
                                          SQstRef *record_refs) {
  static const uint8_t sql[] = "SELECT 1";
  static const uint8_t record[] = "{\"generator\":\"bench\"}";

  assert(bench != NULL && sql_refs != NULL && record_refs != NULL);
  for (uint64_t index = 0; index < bench->item_count; ++index) {
    uint64_t gid = 0;
    uint64_t dispatch_epoch = 0;
    uint64_t transition_seq = 0;
    SQstMessage message = {0};
    SQstGeneratePayload input = {0};
    SQstExecutePayload final_payload = {0};

    assert(qst_task_reserve_gid(bench->mapping, QST_ROLE_GENERATOR,
                                generator_generation, &gid) == QST_OK);
    assert(qst_block_alloc(bench->mapping, gid, 1, sizeof(sql) - 1U,
                           &sql_refs[index]) == QST_OK);
    assert(qst_block_write_seal(bench->mapping, &sql_refs[index], sql,
                                sizeof(sql) - 1U) == QST_OK);
    assert(qst_block_alloc(bench->mapping, gid, 1, sizeof(record) - 1U,
                           &record_refs[index]) == QST_OK);
    assert(qst_block_write_seal(bench->mapping, &record_refs[index], record,
                                sizeof(record) - 1U) == QST_OK);
    input.target = QST_TARGET_TD;
    input.mode = QST_MODE_FULL;
    input.focus = QST_FOCUS_COVERAGE;
    input.dialect = QST_DIALECT_TDENGINE;
    input.generation_seq = gid;
    input.weight_profile_version = 1;
    input.sql_ref = sql_refs[index];
    input.generation_record_ref = record_refs[index];
    assert(qst_block_sha256(bench->mapping, input.generation_record_ref, gid,
                            &input.generation_record_sha256) == QST_OK);
    assert(qst_task_create(bench->mapping, gid, QST_ROLE_GENERATOR,
                           generator_generation, &input,
                           &dispatch_epoch) == QST_OK);
    assert(qst_task_claim_dispatch(bench->mapping, gid, QST_ROLE_EXEC_MANAGER,
                                   exec_generation, dispatch_epoch, &input,
                                   &transition_seq) == QST_OK);
    assert(qst_task_assign_pair(bench->mapping, gid, QST_ROLE_EXEC_MANAGER,
                                exec_generation, dispatch_epoch, transition_seq,
                                0, pair_generation) == QST_OK);
    final_payload.statement_ts_ns = (int64_t)(gid * UINT64_C(1000));
    final_payload.ended_ts_ns = final_payload.statement_ts_ns;
    final_payload.execution_status = QST_EXEC_SUCCESS;
    assert(qst_task_publish_final(bench->mapping, gid, dispatch_epoch,
                                  transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
                                  pair_generation, &final_payload) == QST_OK);

    message.message_type = QST_MSG_EXECUTE_FINAL;
    message.producer_role = QST_ROLE_PAIR_SUPERVISOR;
    message.producer_generation = pair_generation;
    message.target = QST_TARGET_TD;
    message.run_id = bench->mapping->header->run_id;
    message.gid = gid;
    message.dispatch_epoch = dispatch_epoch;
    message.transition_seq = transition_seq;
    message.body.execute = final_payload;
    assert(qst_queue_publish(bench->mapping, QST_QUEUE_EXECUTE, &message,
                             UINT64_MAX) == QST_OK);
    assert(qst_task_confirm_final_published_pair(
               bench->mapping, gid, dispatch_epoch, transition_seq, 0,
               pair_generation, &final_payload) == QST_OK);
  }
}

static void qstTestQueueStageBenchmark(EQstQueueStageBenchKind kind,
                                       uint32_t worker_count,
                                       uint64_t item_count) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstQueueStageBench bench = {0};
  SQstQueueSnapshot input_before = {0};
  SQstQueueSnapshot input_after = {0};
  SQstQueueSnapshot output_before = {0};
  SQstQueueSnapshot output_after = {0};
  struct timespec started = {0};
  struct timespec finished = {0};
  struct rusage rusage_before = {0};
  struct rusage rusage_after = {0};
  SQstRef *sql_refs = NULL;
  SQstRef *record_refs = NULL;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t producer_generation = 0;
  uint64_t elapsed_ns = 0;
  int64_t elapsed_seconds = 0;
  int64_t elapsed_nanoseconds = 0;
  const uint32_t queue_capacity = qstTestRoundUpPowerOfTwo(item_count);
  const EQstQueueId input_queue =
      kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ? QST_QUEUE_GENERATE :
                                               QST_QUEUE_EXECUTE;

  assert(worker_count != 0 && worker_count <= kQueueStageBenchMaxWorkers &&
         item_count != 0 && kind >= QST_QUEUE_STAGE_PAIR_CLAIM_ACK &&
         kind <= QST_QUEUE_STAGE_JUDGE_TO_PERSIST &&
         item_count <= UINT32_MAX / 2U);
  config.run_id = UINT64_C(5520000000) + (uint64_t)getpid();
  /* Four queues, task entries and two blocks per input must all fit before
   * timing starts; 256 MiB supports the 65,536-item isolation workload. */
  config.shm_bytes = UINT64_C(268435456);
  config.process_slot_count = worker_count + 4U;
  config.task_entry_count = (uint32_t)item_count;
  config.ai_action_entry_count = 1;
  config.queue_capacity = queue_capacity;
  config.block_bytes = kQueueStageBenchBlockBytes;
  config.block_count = (uint32_t)(item_count * 2U);
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  bench.mapping = &mapping;
  bench.kind = kind;
  bench.worker_count = worker_count;
  bench.queue_capacity = queue_capacity;
  bench.item_count = item_count;
  atomic_init(&bench.completed_count, 0);
  atomic_init(&bench.error_code, QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  qstTestQueueStageRegisterWorkers(&bench, main_generation, &generator_generation,
                                   &exec_generation, &producer_generation);
  sql_refs = calloc((size_t)item_count, sizeof(*sql_refs));
  record_refs = calloc((size_t)item_count, sizeof(*record_refs));
  assert(sql_refs != NULL && record_refs != NULL);
  if (kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK) {
    qstTestQueueStagePrefillPair(&bench, generator_generation, sql_refs,
                                 record_refs);
  } else {
    qstTestQueueStagePrefillJudge(&bench, generator_generation, exec_generation,
                                  producer_generation,
                                  sql_refs, record_refs);
  }
  assert(qst_queue_snapshot(&mapping, input_queue, &input_before) == QST_OK);
  assert(input_before.enqueue_pos == item_count && input_before.dequeue_pos == 0);
  if (kind == QST_QUEUE_STAGE_JUDGE_TO_PERSIST) {
    assert(qst_queue_snapshot(&mapping, QST_QUEUE_PERSIST, &output_before) ==
           QST_OK);
    assert(output_before.enqueue_pos == 0 && output_before.dequeue_pos == 0);
  }
  assert(pthread_barrier_init(&bench.start_barrier, NULL, worker_count + 1U) ==
         0);
  for (uint32_t slot = 0; slot < worker_count; ++slot) {
    assert(pthread_create(&bench.threads[slot], NULL, qstTestQueueStageWorkerRun,
                          &bench.workers[slot]) == 0);
  }
  assert(getrusage(RUSAGE_SELF, &rusage_before) == 0);
  assert(clock_gettime(CLOCK_MONOTONIC, &started) == 0);
  (void)pthread_barrier_wait(&bench.start_barrier);
  for (uint32_t slot = 0; slot < worker_count; ++slot) {
    assert(pthread_join(bench.threads[slot], NULL) == 0);
  }
  assert(clock_gettime(CLOCK_MONOTONIC, &finished) == 0);
  assert(getrusage(RUSAGE_SELF, &rusage_after) == 0);
  assert(pthread_barrier_destroy(&bench.start_barrier) == 0);
  assert(atomic_load_explicit(&bench.error_code, memory_order_acquire) == QST_OK);
  assert(atomic_load_explicit(&bench.completed_count, memory_order_relaxed) ==
         item_count);
  assert(qst_queue_snapshot(&mapping, input_queue, &input_after) == QST_OK);
  assert(input_after.enqueue_pos == item_count &&
         input_after.dequeue_pos == item_count);
  if (kind == QST_QUEUE_STAGE_JUDGE_TO_PERSIST) {
    assert(qst_queue_snapshot(&mapping, QST_QUEUE_PERSIST, &output_after) ==
           QST_OK);
    assert(output_after.enqueue_pos == item_count && output_after.dequeue_pos == 0);
    qstTestQueueStageDrainPersist(&mapping, main_generation, item_count, 1);
    assert(qst_queue_snapshot(&mapping, QST_QUEUE_PERSIST, &output_after) ==
           QST_OK);
    assert(output_after.dequeue_pos == item_count);
  }
  elapsed_seconds = (int64_t)finished.tv_sec - (int64_t)started.tv_sec;
  elapsed_nanoseconds = (int64_t)finished.tv_nsec - (int64_t)started.tv_nsec;
  if (elapsed_nanoseconds < 0) {
    --elapsed_seconds;
    elapsed_nanoseconds += INT64_C(1000000000);
  }
  assert(elapsed_seconds >= 0 && elapsed_nanoseconds > 0);
  elapsed_ns = (uint64_t)elapsed_seconds * UINT64_C(1000000000) +
               (uint64_t)elapsed_nanoseconds;
  qstTestQueueStagePrintResult(
      kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK ? "pair_queue_claim_ack" :
                                               "judge_queue_to_persist",
      worker_count, item_count, elapsed_ns,
      qstTestRusageMicroseconds(&rusage_after) -
          qstTestRusageMicroseconds(&rusage_before),
      qstTestCurrentRssBytes(), queue_capacity, &input_after,
      kind == QST_QUEUE_STAGE_JUDGE_TO_PERSIST ? &output_after : NULL);
  if (kind == QST_QUEUE_STAGE_PAIR_CLAIM_ACK) {
    for (uint64_t index = 0; index < item_count; ++index) {
      assert(qst_block_release(&mapping, sql_refs[index], index + 1U) == QST_OK);
      assert(qst_block_release(&mapping, record_refs[index], index + 1U) ==
             QST_OK);
      assert(qst_task_abort_generated(&mapping, index + 1U,
                                      QST_ROLE_GENERATOR,
                                      generator_generation) == QST_OK);
    }
  }
  free(record_refs);
  free(sql_refs);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestQueueStageBenchmarkMatrix(void) {
  static const uint32_t worker_counts[] = {1U, 4U, 8U, 16U, 32U, 64U};
  const uint64_t item_count = qstTestReadPositiveUInt64(
      "QST_STAGE_BENCH_QUEUE_ITEMS", kQueueStageBenchDefaultItems);

  for (uint32_t index = 0;
       index < sizeof(worker_counts) / sizeof(worker_counts[0]); ++index) {
    qstTestQueueStageBenchmark(QST_QUEUE_STAGE_PAIR_CLAIM_ACK,
                               worker_counts[index], item_count);
  }
  for (uint32_t index = 0;
       index < sizeof(worker_counts) / sizeof(worker_counts[0]); ++index) {
    qstTestQueueStageBenchmark(QST_QUEUE_STAGE_JUDGE_TO_PERSIST,
                               worker_counts[index], item_count);
  }
}

enum {
  kNativeStageBenchMaxWorkers = 64,
  kNativeStageBenchMaxClientExecThreads = 10,
  kNativeStageBenchDefaultQueriesPerClientThread = 4096,
  kNativeStageBenchWarmupQueries = 16,
  kNativeStageBenchAsyncBatchSize = 10,
  kNativeStageBenchFixtureRetryCount = 120,
  kNativeStageBenchFixtureRetryMilliseconds = 200,
};

/* The native stage must exercise a real scan and returned-result conversion.
 * DDL and deterministic fixture creation are deliberately outside timing. */
static const char kNativeStageQuery[] =
    "SELECT ts, value FROM qst_stage_native.d1 "
    "WHERE ts >= 1700000000000 AND value BETWEEN 10.0 AND 30.0 "
    "ORDER BY ts DESC LIMIT 16";

typedef struct SQstNativeStageBench SQstNativeStageBench;

typedef struct {
  EQstCode code;
  uint64_t completed_count;
} SQstNativeStageChildResult;

typedef struct {
  SQstNativeClient *client;
  _Atomic uint32_t completed;
  _Atomic int32_t code;
  int32_t event_fd;
  uint32_t row_count;
} SQstNativeStageAsyncRequest;

typedef struct SQstNativeStageClientExecGroup SQstNativeStageClientExecGroup;

typedef struct {
  SQstNativeStageClientExecGroup *group;
  SQstNativeClient *client;
  uint64_t queries_per_thread;
  SQstNativeStageChildResult result;
} SQstNativeStageClientExecThread;

struct SQstNativeStageClientExecGroup {
  pthread_mutex_t mutex;
  pthread_cond_t condition;
  uint32_t expected_count;
  uint32_t ready_count;
  int32_t start_requested;
  int32_t stop_requested;
};

struct SQstNativeStageBench {
  uint16_t ports[kNativeStageBenchMaxWorkers];
  _Atomic uint64_t completed_count;
};

static EQstCode qstTestNativeStagePrepareEndpoint(uint16_t port) {
  static const char *const setup_sql[] = {
      /* A one-pair endpoint has one dnode. Keep fixture DDL to one vgroup so
       * endpoint bootstrap is independent of concurrent pair startup. */
      "CREATE DATABASE IF NOT EXISTS qst_stage_native VGROUPS 1",
      "CREATE STABLE IF NOT EXISTS qst_stage_native.meters "
      "(ts TIMESTAMP, value DOUBLE) TAGS (device INT)",
      "CREATE TABLE IF NOT EXISTS qst_stage_native.d1 USING "
      "qst_stage_native.meters TAGS (1)",
      "INSERT INTO qst_stage_native.d1 VALUES "
      "(1700000000000,10.0) (1700000001000,11.0) "
      "(1700000002000,12.0) (1700000003000,13.0) "
      "(1700000004000,14.0) (1700000005000,15.0) "
      "(1700000006000,16.0) (1700000007000,17.0) "
      "(1700000008000,18.0) (1700000009000,19.0) "
      "(1700000010000,20.0) (1700000011000,21.0) "
      "(1700000012000,22.0) (1700000013000,23.0) "
      "(1700000014000,24.0) (1700000015000,25.0) "
      "(1700000016000,26.0) (1700000017000,27.0) "
      "(1700000018000,28.0) (1700000019000,29.0) "
      "(1700000020000,30.0)",
  };
  SQstNativeClient client = {0};
  SQstQueryResult result = {0};
  EQstCode code = qst_native_open_with_endpoint(&client, "127.0.0.1", port,
                                                 NULL);

  if (code != QST_OK) {
    return code;
  }
  for (uint32_t index = 0; index < sizeof(setup_sql) / sizeof(setup_sql[0]);
       ++index) {
    code = qst_native_execute(&client, setup_sql[index]);
    if (code != QST_OK) {
      break;
    }
  }
  if (code == QST_OK) {
    code = qst_native_query_result(&client, kNativeStageQuery, &result);
    if (code == QST_OK && result.row_count != 16) {
      code = QST_E_RESULT_STORE;
    }
  }
  qst_result_reset(&result);
  if (qst_native_close(&client) != QST_OK && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  return code;
}

/* A native client keeps process-global transport state after taos_close().
 * Prepare each independent pair endpoint in a short-lived child so fixture
 * setup cannot exhaust that state before the timed workers are created. */
static EQstCode qstTestNativeStagePrepareEndpointInChild(uint16_t port) {
  pid_t child = -1;
  pid_t waited = -1;
  int32_t status = 0;

  child = fork();
  if (child < 0) {
    return QST_E_RESOURCE;
  }
  if (child == 0) {
    _exit(qstTestNativeStagePrepareEndpoint(port) == QST_OK ? EXIT_SUCCESS :
                                                               EXIT_FAILURE);
  }
  while ((waited = waitpid(child, &status, 0)) < 0 && errno == EINTR) {
  }
  if (waited == child && WIFEXITED(status) &&
      WEXITSTATUS(status) == EXIT_SUCCESS) {
    return QST_OK;
  }
  (void)fprintf(stderr,
                "taosQueryTest: native stage fixture child failed port=%u "
                "status=%d\n",
                (unsigned int)port, status);
  return QST_E_RESULT_STORE;
}

/* A pair publishes ready after it accepts native connections. Its mnode can
 * still be finishing bootstrap, so prepare the idempotent fixture before the
 * timed region with a bounded retry. */
static EQstCode qstTestNativeStagePrepareEndpointWithRetry(uint16_t port) {
  const struct timespec pause = {
      .tv_sec = 0,
      .tv_nsec = kNativeStageBenchFixtureRetryMilliseconds * 1000000L,
  };
  EQstCode code = QST_E_RESOURCE;

  for (uint32_t attempt = 0; attempt < kNativeStageBenchFixtureRetryCount;
       ++attempt) {
    code = qstTestNativeStagePrepareEndpointInChild(port);
    if (code == QST_OK) {
      return QST_OK;
    }
    if (attempt + 1U < kNativeStageBenchFixtureRetryCount) {
      assert(nanosleep(&pause, NULL) == 0);
    }
  }
  return code;
}

/* Finishes one query after its result stream reaches a zero-row callback.
 * The release store precedes eventfd notification, so the child may inspect
 * the state after it has received the corresponding notification. */
static void qstTestNativeStageCompleteAsyncRequest(
    SQstNativeStageAsyncRequest *request, void *result, EQstCode code,
    int32_t native_code) {
  uint64_t event_count = 1;

  if (NULL == request || NULL == request->client ||
      NULL == request->client->free_result || request->event_fd < 0) {
    return;
  }
  if (result != NULL) {
    request->client->free_result(result);
  }
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: native stage async query failed "
                  "native_code=%d qst_code=%d rows=%u\n",
                  native_code, code, request->row_count);
  }
  atomic_store_explicit(&request->code, code, memory_order_relaxed);
  atomic_store_explicit(&request->completed, 1, memory_order_release);
  while (write(request->event_fd, &event_count, sizeof(event_count)) < 0 &&
         errno == EINTR) {
  }
}

/* Reads one asynchronous result block and requests the next one until done. */
static void qstTestNativeStageFetchRowsCallback(void *param, void *result,
                                                int32_t native_row_count) {
  SQstNativeStageAsyncRequest *request =
      (SQstNativeStageAsyncRequest *)param;
  EQstCode code = QST_OK;

  if (NULL == request || NULL == request->client ||
      NULL == request->client->fetch_row || NULL == request->client->fetch_rows_a ||
      NULL == request->client->free_result) {
    return;
  }
  if (native_row_count < 0 || NULL == result) {
    qstTestNativeStageCompleteAsyncRequest(request, result, QST_E_RESULT_STORE,
                                           native_row_count);
    return;
  }
  if (native_row_count == 0) {
    code = request->row_count == 16U ? QST_OK : QST_E_RESULT_STORE;
    qstTestNativeStageCompleteAsyncRequest(request, result, code,
                                           native_row_count);
    return;
  }
  for (int32_t index = 0; index < native_row_count; ++index) {
    if (request->client->fetch_row(result) == NULL) {
      code = QST_E_RESULT_STORE;
      break;
    }
    ++request->row_count;
  }
  if (code != QST_OK) {
    qstTestNativeStageCompleteAsyncRequest(request, result, code,
                                           native_row_count);
    return;
  }
  request->client->fetch_rows_a(result, qstTestNativeStageFetchRowsCallback,
                                request);
}

/* Query completion returns only a result handle. Fetch result rows through
 * taos_fetch_rows_a before reporting the query itself as complete. */
static void qstTestNativeStageAsyncCallback(void *param, void *result,
                                            int32_t native_code) {
  SQstNativeStageAsyncRequest *request =
      (SQstNativeStageAsyncRequest *)param;

  if (NULL == request || NULL == request->client ||
      NULL == request->client->errno_fn || NULL == request->client->fetch_rows_a ||
      NULL == request->client->free_result) {
    return;
  }
  if (native_code != 0 || NULL == result || request->client->errno_fn(result) != 0) {
    qstTestNativeStageCompleteAsyncRequest(request, result, QST_E_RESULT_STORE,
                                           native_code);
    return;
  }
  request->client->fetch_rows_a(result, qstTestNativeStageFetchRowsCallback,
                                request);
}

/* Waits for all submitted callbacks, then validates their result states. */
static EQstCode qstTestNativeStageWaitAsyncBatch(
    SQstNativeStageAsyncRequest *requests, uint32_t request_count,
    int32_t event_fd) {
  struct pollfd event = {0};
  uint64_t notified_count = 0;

  if (NULL == requests || request_count == 0 || event_fd < 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  event.fd = event_fd;
  event.events = POLLIN;
  while (notified_count < request_count) {
    uint64_t event_count = 0;
    ssize_t read_bytes = 0;
    int32_t poll_result = poll(&event, 1, -1);

    if (poll_result < 0 && errno == EINTR) {
      continue;
    }
    assert(poll_result > 0 && (event.revents & POLLIN) != 0);
    do {
      read_bytes = read(event_fd, &event_count, sizeof(event_count));
    } while (read_bytes < 0 && errno == EINTR);
    assert(read_bytes == (ssize_t)sizeof(event_count));
    assert(event_count != 0 && event_count <= request_count - notified_count);
    notified_count += event_count;
  }
  for (uint32_t index = 0; index < request_count; ++index) {
    if (atomic_load_explicit(&requests[index].completed,
                             memory_order_acquire) == 0 ||
        atomic_load_explicit(&requests[index].code, memory_order_relaxed) !=
            QST_OK) {
      return QST_E_RESULT_STORE;
    }
  }
  return QST_OK;
}

/* Submits one bounded batch from one child thread through one native client.
 * The caller must keep client alive until this function has returned. */
static EQstCode qstTestNativeStageRunAsyncBatch(SQstNativeClient *client,
                                                uint32_t query_count,
                                                uint64_t *out_completed) {
  SQstNativeStageAsyncRequest
      requests[kNativeStageBenchAsyncBatchSize] = {{0}};
  EQstCode code = QST_OK;
  EQstCode wait_code = QST_OK;
  uint32_t submitted_count = 0;
  int32_t event_fd = -1;

  if (NULL == client || NULL == out_completed || query_count == 0 ||
      query_count > kNativeStageBenchAsyncBatchSize) {
    return QST_E_INVALID_ARGUMENT;
  }
  event_fd = eventfd(0, EFD_CLOEXEC);
  if (event_fd < 0) {
    return QST_E_RESOURCE;
  }
  for (uint32_t index = 0; index < query_count; ++index) {
    requests[index].client = client;
    requests[index].event_fd = event_fd;
    atomic_init(&requests[index].completed, 0);
    atomic_init(&requests[index].code, QST_E_RESOURCE);
    code = qst_native_execute_async(client, kNativeStageQuery,
                                    qstTestNativeStageAsyncCallback,
                                    &requests[index]);
    if (code != QST_OK) {
      break;
    }
    ++submitted_count;
  }
  if (submitted_count != 0) {
    wait_code = qstTestNativeStageWaitAsyncBatch(requests, submitted_count,
                                                 event_fd);
  }
  if (code == QST_OK) {
    code = wait_code;
  }
  if (code == QST_OK && submitted_count != query_count) {
    code = QST_E_RESOURCE;
  }
  if (close(event_fd) != 0 && code == QST_OK) {
    code = QST_E_RESOURCE;
  }
  if (code == QST_OK) {
    *out_completed += query_count;
  }
  return code;
}

static EQstCode qstTestNativeStageWarmup(SQstNativeClient *client) {
  SQstQueryResult result = {0};
  EQstCode code = QST_OK;

  if (NULL == client) {
    return QST_E_INVALID_ARGUMENT;
  }
  for (uint32_t index = 0; index < kNativeStageBenchWarmupQueries; ++index) {
    code = qst_native_query_result(client, kNativeStageQuery, &result);
    if (code == QST_OK && result.row_count != 16) {
      code = QST_E_RESULT_STORE;
    }
    qst_result_reset(&result);
    if (code != QST_OK) {
      break;
    }
  }
  return code;
}

/* Every executor shares the pair's native connection and independently keeps
 * a ten-query asynchronous batch in flight. */
static void *qstTestNativeStageClientExecThread(void *param) {
  SQstNativeStageClientExecThread *executor =
      (SQstNativeStageClientExecThread *)param;
  SQstNativeStageClientExecGroup *group = NULL;
  int32_t should_run = 0;

  assert(executor != NULL && executor->group != NULL && executor->client != NULL);
  group = executor->group;
  assert(pthread_mutex_lock(&group->mutex) == 0);
  ++group->ready_count;
  assert(pthread_cond_broadcast(&group->condition) == 0);
  while (group->start_requested == 0 && group->stop_requested == 0) {
    assert(pthread_cond_wait(&group->condition, &group->mutex) == 0);
  }
  should_run = group->start_requested != 0;
  assert(pthread_mutex_unlock(&group->mutex) == 0);
  for (uint64_t index = 0; should_run != 0 && executor->result.code == QST_OK &&
                           index < executor->queries_per_thread;) {
    const uint64_t remaining_count = executor->queries_per_thread - index;
    const uint32_t batch_count = (uint32_t)(remaining_count <
                                                    kNativeStageBenchAsyncBatchSize
                                                ? remaining_count
                                                : kNativeStageBenchAsyncBatchSize);

    executor->result.code = qstTestNativeStageRunAsyncBatch(
        executor->client, batch_count, &executor->result.completed_count);
    index += batch_count;
  }
  return NULL;
}

static void qstTestNativeStageRunChild(uint16_t port,
                                       uint64_t queries_per_client_thread,
                                       uint32_t client_exec_thread_count,
                                       int32_t ready_fd, int32_t start_fd,
                                       int32_t result_fd) {
  SQstNativeClient client = {0};
  SQstNativeStageClientExecGroup group = {0};
  SQstNativeStageClientExecThread
      executors[kNativeStageBenchMaxClientExecThreads] = {{0}};
  pthread_t client_threads[kNativeStageBenchMaxClientExecThreads] = {0};
  SQstNativeStageChildResult child_result = {0};
  uint8_t start = 0;
  int32_t group_initialized = 0;

  assert(client_exec_thread_count != 0 &&
         client_exec_thread_count <= kNativeStageBenchMaxClientExecThreads);
  child_result.code = qst_native_open_with_endpoint(&client, "127.0.0.1", port,
                                                     NULL);
  if (child_result.code == QST_OK) {
    child_result.code = qstTestNativeStageWarmup(&client);
  }
  if (child_result.code == QST_OK) {
    group.expected_count = client_exec_thread_count;
    assert(pthread_mutex_init(&group.mutex, NULL) == 0);
    assert(pthread_cond_init(&group.condition, NULL) == 0);
    group_initialized = 1;
    for (uint32_t index = 0; index < client_exec_thread_count; ++index) {
      executors[index].group = &group;
      executors[index].client = &client;
      executors[index].queries_per_thread = queries_per_client_thread;
      assert(pthread_create(&client_threads[index], NULL,
                            qstTestNativeStageClientExecThread,
                            &executors[index]) == 0);
    }
    assert(pthread_mutex_lock(&group.mutex) == 0);
    while (group.ready_count < group.expected_count) {
      assert(pthread_cond_wait(&group.condition, &group.mutex) == 0);
    }
    assert(pthread_mutex_unlock(&group.mutex) == 0);
  }
  if (write(ready_fd, &child_result, sizeof(child_result)) !=
      (ssize_t)sizeof(child_result)) {
    _exit(EXIT_FAILURE);
  }
  if (child_result.code == QST_OK &&
      read(start_fd, &start, sizeof(start)) != (ssize_t)sizeof(start)) {
    child_result.code = QST_E_RESOURCE;
  }
  if (group_initialized != 0) {
    assert(pthread_mutex_lock(&group.mutex) == 0);
    group.start_requested = child_result.code == QST_OK ? 1 : 0;
    group.stop_requested = child_result.code == QST_OK ? 0 : 1;
    assert(pthread_cond_broadcast(&group.condition) == 0);
    assert(pthread_mutex_unlock(&group.mutex) == 0);
    for (uint32_t index = 0; index < client_exec_thread_count; ++index) {
      assert(pthread_join(client_threads[index], NULL) == 0);
      if (child_result.code == QST_OK &&
          executors[index].result.code != QST_OK) {
        child_result.code = executors[index].result.code;
      }
      child_result.completed_count += executors[index].result.completed_count;
    }
    assert(pthread_cond_destroy(&group.condition) == 0);
    assert(pthread_mutex_destroy(&group.mutex) == 0);
  }
  if (client.connection != NULL && qst_native_close(&client) != QST_OK &&
      child_result.code == QST_OK) {
    child_result.code = QST_E_RESOURCE;
  }
  if (write(result_fd, &child_result, sizeof(child_result)) !=
      (ssize_t)sizeof(child_result)) {
    _exit(EXIT_FAILURE);
  }
  _exit(child_result.code == QST_OK ? EXIT_SUCCESS : EXIT_FAILURE);
}

static void qstTestNativeStageBenchmark(uint32_t worker_count,
                                        uint16_t port_base,
                                        uint16_t port_stride,
                                        uint64_t queries_per_client_thread,
                                        uint32_t client_exec_thread_count,
                                        const uint16_t *ports) {
  SQstNativeStageBench bench = {0};
  struct timespec started = {0};
  struct timespec finished = {0};
  struct rusage rusage_before = {0};
  struct rusage rusage_after = {0};
  int64_t elapsed_seconds = 0;
  int64_t elapsed_nanoseconds = 0;
  uint64_t elapsed_ns = 0;
  const uint64_t queries_per_pair =
      (uint64_t)client_exec_thread_count * queries_per_client_thread;
  const uint64_t total_queries = (uint64_t)worker_count * queries_per_pair;

  assert(worker_count != 0 && worker_count <= kNativeStageBenchMaxWorkers &&
         queries_per_client_thread != 0 && client_exec_thread_count != 0 &&
         client_exec_thread_count <= kNativeStageBenchMaxClientExecThreads &&
         queries_per_client_thread <= UINT64_MAX / client_exec_thread_count &&
         queries_per_pair <= UINT64_MAX / worker_count &&
         (ports != NULL ||
          (port_base != 0 &&
           port_base <= UINT16_MAX - (uint32_t)port_stride * worker_count)));
  atomic_init(&bench.completed_count, 0);
  for (uint32_t slot = 0; slot < worker_count; ++slot) {
    const uint16_t port = ports == NULL ?
        (uint16_t)(port_base + port_stride * slot) : ports[slot];

    assert(port != 0);
    bench.ports[slot] = port;
    if (qstTestNativeStagePrepareEndpointWithRetry(port) != QST_OK) {
      assert(0 && "native stage endpoint fixture preparation failed");
    }
  }
  {
    int32_t ready_pipes[kNativeStageBenchMaxWorkers][2] = {{-1, -1}};
    int32_t start_pipes[kNativeStageBenchMaxWorkers][2] = {{-1, -1}};
    int32_t result_pipes[kNativeStageBenchMaxWorkers][2] = {{-1, -1}};
    pid_t children[kNativeStageBenchMaxWorkers] = {0};
    uint8_t start = 1;

    for (uint32_t slot = 0; slot < worker_count; ++slot) {
      assert(pipe(ready_pipes[slot]) == 0);
      assert(pipe(start_pipes[slot]) == 0);
      assert(pipe(result_pipes[slot]) == 0);
      children[slot] = fork();
      assert(children[slot] >= 0);
      if (children[slot] == 0) {
        (void)close(ready_pipes[slot][0]);
        (void)close(start_pipes[slot][1]);
        (void)close(result_pipes[slot][0]);
        qstTestNativeStageRunChild(
            bench.ports[slot], queries_per_client_thread,
            client_exec_thread_count,
            ready_pipes[slot][1], start_pipes[slot][0], result_pipes[slot][1]);
      }
      assert(close(ready_pipes[slot][1]) == 0);
      assert(close(start_pipes[slot][0]) == 0);
      assert(close(result_pipes[slot][1]) == 0);
    }
    for (uint32_t slot = 0; slot < worker_count; ++slot) {
      SQstNativeStageChildResult child_result = {0};

      assert(read(ready_pipes[slot][0], &child_result, sizeof(child_result)) ==
             (ssize_t)sizeof(child_result));
      assert(close(ready_pipes[slot][0]) == 0);
      assert(child_result.code == QST_OK);
    }
    assert(getrusage(RUSAGE_CHILDREN, &rusage_before) == 0);
    assert(clock_gettime(CLOCK_MONOTONIC, &started) == 0);
    for (uint32_t slot = 0; slot < worker_count; ++slot) {
      assert(write(start_pipes[slot][1], &start, sizeof(start)) ==
             (ssize_t)sizeof(start));
      assert(close(start_pipes[slot][1]) == 0);
    }
    for (uint32_t slot = 0; slot < worker_count; ++slot) {
      SQstNativeStageChildResult child_result = {0};
      int32_t status = 0;

      assert(read(result_pipes[slot][0], &child_result, sizeof(child_result)) ==
             (ssize_t)sizeof(child_result));
      assert(close(result_pipes[slot][0]) == 0);
      assert(waitpid(children[slot], &status, 0) == children[slot]);
      assert(WIFEXITED(status) && WEXITSTATUS(status) == EXIT_SUCCESS);
      assert(child_result.code == QST_OK);
      atomic_fetch_add_explicit(&bench.completed_count,
                                child_result.completed_count,
                                memory_order_relaxed);
    }
  }
  assert(clock_gettime(CLOCK_MONOTONIC, &finished) == 0);
  assert(getrusage(RUSAGE_CHILDREN, &rusage_after) == 0);
  assert(atomic_load_explicit(&bench.completed_count, memory_order_relaxed) ==
         total_queries);
  elapsed_seconds = (int64_t)finished.tv_sec - (int64_t)started.tv_sec;
  elapsed_nanoseconds = (int64_t)finished.tv_nsec - (int64_t)started.tv_nsec;
  if (elapsed_nanoseconds < 0) {
    --elapsed_seconds;
    elapsed_nanoseconds += INT64_C(1000000000);
  }
  assert(elapsed_seconds >= 0 && elapsed_nanoseconds > 0);
  elapsed_ns = (uint64_t)elapsed_seconds * UINT64_C(1000000000) +
               (uint64_t)elapsed_nanoseconds;
  (void)fprintf(stdout,
                "{\"stage\":\"pair_native_actual_query\","
                "\"pair_processes\":%u,\"client_execution_threads\":%u,"
                "\"clients_per_pair\":%u,\"connections_per_pair\":%u,"
                "\"execution_model\":\"shared_client_multi_thread_async_batches\","
                "\"queries_per_pair\":%llu,"
                "\"queries_per_client_thread\":%llu,"
                "\"async_batch_size\":%u,"
                "\"workload\":\"filtered_ordered_timeseries_scan_16_rows\","
                "\"input_count\":%llu,\"completed_count\":%llu,"
                "\"error_count\":0,\"elapsed_ns\":%llu,"
                "\"items_per_second\":%.2f,\"cpu_time_us\":%llu,"
                "\"rss_bytes\":%llu,\"queue\":null}\n",
                worker_count, client_exec_thread_count, 1U, 1U,
                (unsigned long long)queries_per_pair,
                (unsigned long long)queries_per_client_thread,
                kNativeStageBenchAsyncBatchSize,
                (unsigned long long)total_queries,
                (unsigned long long)total_queries, (unsigned long long)elapsed_ns,
                (double)total_queries * 1000000000.0 / (double)elapsed_ns,
                (unsigned long long)(qstTestRusageMicroseconds(&rusage_after) -
                                     qstTestRusageMicroseconds(&rusage_before)),
                (unsigned long long)qstTestCurrentRssBytes());
}

/* Parses the dynamic pair ports recorded by the supervisor runtime facts. */
static int32_t qstTestNativeStageReadPorts(uint16_t *out_ports,
                                           uint32_t port_count) {
  const char *text = getenv("QST_NATIVE_STAGE_PORTS");
  const char *cursor = text;

  assert(out_ports != NULL && port_count != 0);
  if (text == NULL || text[0] == '\0') {
    return 0;
  }
  for (uint32_t index = 0; index < port_count; ++index) {
    char *end = NULL;
    unsigned long value = 0;

    errno = 0;
    value = strtoul(cursor, &end, 10);
    assert(errno == 0 && end != cursor && value != 0 && value <= UINT16_MAX);
    out_ports[index] = (uint16_t)value;
    for (uint32_t previous = 0; previous < index; ++previous) {
      assert(out_ports[previous] != out_ports[index]);
    }
    if (index + 1U == port_count) {
      assert(*end == '\0');
    } else {
      assert(*end == ',');
      cursor = end + 1;
    }
  }
  return 1;
}

static void qstTestNativeStageBenchmarkMatrix(void) {
  static const uint32_t worker_counts[] = {1U, 4U, 8U, 16U};
  uint16_t ports[kNativeStageBenchMaxWorkers] = {0};
  const uint64_t port_base = qstTestReadPositiveUInt64(
      "QST_NATIVE_STAGE_PORT_BASE", 1);
  const uint64_t port_stride = qstTestReadNonnegativeUInt64(
      "QST_NATIVE_STAGE_PORT_STRIDE", 1);
  const uint64_t queries_per_client_thread = qstTestReadPositiveUInt64(
      "QST_NATIVE_STAGE_QUERIES_PER_CLIENT_THREAD",
      kNativeStageBenchDefaultQueriesPerClientThread);
  const uint64_t client_exec_thread_count = qstTestReadPositiveUInt64(
      "QST_NATIVE_STAGE_CLIENT_EXEC_THREADS", 1);
  const uint64_t selected_worker_count = qstTestReadNonnegativeUInt64(
      "QST_NATIVE_STAGE_WORKERS", 0);
  assert(selected_worker_count == 0 || selected_worker_count == 1U ||
         selected_worker_count == 4U || selected_worker_count == 8U ||
         selected_worker_count == 16U || selected_worker_count == 32U ||
         selected_worker_count == 64U);
  const int32_t has_explicit_ports = selected_worker_count == 0 ? 0 :
      qstTestNativeStageReadPorts(ports, (uint32_t)selected_worker_count);

  assert(port_base <= UINT16_MAX && port_stride <= UINT16_MAX &&
         client_exec_thread_count <= kNativeStageBenchMaxClientExecThreads);
  assert(has_explicit_ports == 0 || selected_worker_count != 0);
  if (selected_worker_count != 0) {
    qstTestNativeStageBenchmark((uint32_t)selected_worker_count,
                                (uint16_t)port_base, (uint16_t)port_stride,
                                queries_per_client_thread,
                                (uint32_t)client_exec_thread_count,
                                has_explicit_ports != 0 ? ports : NULL);
    return;
  }
  for (uint32_t index = 0;
       index < sizeof(worker_counts) / sizeof(worker_counts[0]); ++index) {
    qstTestNativeStageBenchmark(worker_counts[index], (uint16_t)port_base,
                                (uint16_t)port_stride,
                                queries_per_client_thread,
                                (uint32_t)client_exec_thread_count,
                                NULL);
  }
}

enum {
  kPersistStageBenchMaxWorkers = 64,
  /* One pending-file worker must finish one independently published 4-8 MiB
   * object, so its measurement never depends on a shared writer. */
  kPersistStageBenchDefaultItems = 512,
  kPersistStageBlockBytes = 256,
  kPersistStagePendingPayloadBytes = 8192,
  kPersistStageDmlBatchRecords = 32,
};

typedef enum {
  QST_PERSIST_STAGE_RESULT_STORE_DML = 1,
  QST_PERSIST_STAGE_PENDING_FILE = 2,
} EQstPersistStageBenchKind;

typedef struct SQstPersistStageBench SQstPersistStageBench;

typedef struct {
  SQstPersistStageBench *bench;
  uint32_t slot;
} SQstPersistStageBenchWorker;

struct SQstPersistStageBench {
  SQstShmMapping *mapping;
  EQstPersistStageBenchKind kind;
  uint32_t worker_count;
  uint64_t item_count;
  uint64_t run_id;
  const char *pending_dir;
  pthread_barrier_t start_barrier;
  pthread_t threads[kPersistStageBenchMaxWorkers];
  SQstPersistStageBenchWorker workers[kPersistStageBenchMaxWorkers];
  _Atomic uint64_t next_index;
  _Atomic uint64_t completed_count;
  _Atomic int32_t error_code;
};

static void qstTestPersistStageSetError(SQstPersistStageBench *bench,
                                        EQstCode code) {
  int32_t expected = QST_OK;

  assert(bench != NULL && code != QST_OK);
  (void)atomic_compare_exchange_strong_explicit(
      &bench->error_code, &expected, code, memory_order_release,
      memory_order_relaxed);
}

static void qstTestPersistStageSetEndpoint(void) {
  const char *host = getenv("QST_PERSIST_STAGE_HOST");
  const char *port = getenv("QST_PERSIST_STAGE_PORT");

  assert(host != NULL && host[0] != '\0' && port != NULL && port[0] != '\0');
  assert(setenv("TAOS_HOST", host, 1) == 0);
  assert(setenv("TAOS_PORT", port, 1) == 0);
}

static int64_t qstTestPersistStageRealtimeNowNs(void) {
  struct timespec now = {0};

  assert(clock_gettime(CLOCK_REALTIME, &now) == 0);
  assert(now.tv_sec > 0 &&
         (uint64_t)now.tv_sec <= (uint64_t)INT64_MAX / UINT64_C(1000000000));
  return (int64_t)((uint64_t)now.tv_sec * UINT64_C(1000000000) +
                   (uint64_t)now.tv_nsec);
}

static void *qstTestPersistStageWorkerRun(void *arg) {
  SQstPersistStageBenchWorker *worker = arg;
  SQstPersistStageBench *bench = worker->bench;
  SQstResultStore store = {0};
  SQstResultStoreConfig store_config = {.run_id = bench->run_id};
  SQstPendingStore pending = {
      .run_id = bench->run_id,
      .pending_dir = bench->pending_dir,
      .quota_bytes = UINT64_C(1073741824),
  };
  EQstCode code = QST_OK;

  if (bench->kind == QST_PERSIST_STAGE_RESULT_STORE_DML) {
    code = qst_result_store_open(&store_config, &store);
  } else {
    store.run_id = bench->run_id;
  }
  if (code != QST_OK) {
    qstTestPersistStageSetError(bench, code);
  }
  (void)pthread_barrier_wait(&bench->start_barrier);
  if (code != QST_OK ||
      atomic_load_explicit(&bench->error_code, memory_order_acquire) != QST_OK) {
    if (store.native_taos != NULL) {
      (void)qst_result_store_close(&store);
    }
    return NULL;
  }
  if (bench->kind == QST_PERSIST_STAGE_PENDING_FILE) {
    SQstPendingBatch batch = {.fd = -1};
    uint8_t *payload = NULL;

    payload = (uint8_t *)calloc(kPersistStagePendingPayloadBytes, 1);
    if (NULL == payload) {
      qstTestPersistStageSetError(bench, QST_E_RESOURCE);
      return NULL;
    }
    memset(payload, 'p', kPersistStagePendingPayloadBytes);
    code = qst_pending_batch_begin(bench->mapping, &pending, &batch);
    const uint64_t first_index =
        bench->item_count * worker->slot / bench->worker_count;
    const uint64_t end_index =
        bench->item_count * (worker->slot + 1U) / bench->worker_count;

    for (uint64_t index = first_index; code == QST_OK && index < end_index;
         ++index) {
      SQstPendingRecord pending_record = {0};
      char stable_key[192] = {0};
      const uint64_t record_seq = index + 1U;

      if (snprintf(stable_key, sizeof(stable_key),
                   "statements|%llu|td|%llu|%llu",
                   (unsigned long long)bench->run_id,
                   (unsigned long long)record_seq,
                   (unsigned long long)(UINT64_C(1700000000000000000) +
                                        record_seq)) < 0) {
        code = QST_E_RESOURCE;
        break;
      }
      pending_record.persist_kind = QST_PERSIST_STATEMENT;
      pending_record.target = QST_TARGET_TD;
      pending_record.run_id = bench->run_id;
      pending_record.gid = record_seq;
      pending_record.record_seq = record_seq;
      pending_record.ds_fingerprint = "qst-persist-stage";
      pending_record.stable_key = stable_key;
      pending_record.payload = payload;
      pending_record.payload_bytes = kPersistStagePendingPayloadBytes;
      pending_record.created_at_ns =
          (int64_t)(UINT64_C(1700000000000000000) + record_seq);
      code = qst_pending_batch_append(&batch, &pending_record);
      if (code == QST_OK && qst_pending_batch_ready(&batch) != 0) {
        code = qst_pending_batch_flush(&batch);
      }
      if (code == QST_OK) {
        (void)atomic_fetch_add_explicit(&bench->completed_count, 1,
                                        memory_order_relaxed);
      }
    }
    if (code == QST_OK) {
      code = qst_pending_batch_flush(&batch);
    }
    if (code != QST_OK) {
      qst_pending_batch_abort(&batch);
      qstTestPersistStageSetError(bench, code);
    }
    free(payload);
    return NULL;
  }
  for (;;) {
    SQstPersistPayload payloads[kPersistStageDmlBatchRecords] = {{0}};
    const uint64_t first_index = atomic_fetch_add_explicit(
        &bench->next_index, kPersistStageDmlBatchRecords, memory_order_relaxed);
    uint32_t payload_count = 0;

    if (first_index >= bench->item_count) {
      break;
    }
    for (; payload_count < kPersistStageDmlBatchRecords &&
           first_index + payload_count < bench->item_count; ++payload_count) {
      SQstPersistPayload *payload = &payloads[payload_count];

      payload->persist_kind = QST_PERSIST_STATEMENT;
      payload->durable_source = QST_DURABLE_RESULT_STORE;
      payload->target = QST_TARGET_TD;
      payload->gid = first_index + payload_count + 1U;
      payload->dispatch_epoch = 1;
      /* qst_task_claim_dispatch advances the created task from seq=1 to seq=2. */
      payload->transition_seq = 2;
      payload->record_seq = first_index + payload_count + 1U;
    }
    {
      int32_t taos_code = 0;

      code = qst_result_store_write_batch(bench->mapping, &store, payloads,
                                          payload_count, &taos_code);
    }
    if (code != QST_OK) {
      qstTestPersistStageSetError(bench, code);
      break;
    }
    (void)atomic_fetch_add_explicit(&bench->completed_count, payload_count,
                                    memory_order_relaxed);
  }
  if (store.native_taos != NULL && qst_result_store_close(&store) != QST_OK) {
    qstTestPersistStageSetError(bench, QST_E_RESOURCE);
  }
  return NULL;
}

static void qstTestPersistStageBenchmark(EQstPersistStageBenchKind kind,
                                         uint32_t worker_count,
                                         uint64_t item_count) {
  static const uint8_t sql[] = "SELECT ts, value FROM qst_stage_native.d1 "
                               "WHERE value BETWEEN 10.0 AND 30.0 LIMIT 16";
  static const uint8_t record[] = "{\"generator\":\"persist-stage\"}";
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstQueueStageBench ledger_bench = {0};
  SQstPersistStageBench bench = {0};
  SQstResultStore setup_store = {0};
  SQstResultStoreConfig setup_config = {0};
  SQstDigest256 dataset_digest = {0};
  SQstRef *sql_refs = NULL;
  SQstRef *record_refs = NULL;
  struct timespec started = {0};
  struct timespec finished = {0};
  struct rusage rusage_before = {0};
  struct rusage rusage_after = {0};
  char pending_root[256] = {0};
  char pending_dir[320] = {0};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  int64_t elapsed_seconds = 0;
  int64_t elapsed_nanoseconds = 0;
  uint64_t elapsed_ns = 0;
  uint64_t stage_item_count = 0;
  int64_t timestamp_base_ns = 0;

  assert(kind == QST_PERSIST_STAGE_RESULT_STORE_DML ||
         kind == QST_PERSIST_STAGE_PENDING_FILE);
  assert(worker_count != 0 && worker_count <= kPersistStageBenchMaxWorkers &&
         item_count != 0 && item_count <= UINT32_MAX / 3U);
  stage_item_count = item_count;
  timestamp_base_ns = qstTestPersistStageRealtimeNowNs();
  assert((uint64_t)timestamp_base_ns <=
         (uint64_t)INT64_MAX - (item_count - 1U) * UINT64_C(1000000) - 1000U);
  config.run_id = (uint64_t)timestamp_base_ns;
  config.shm_bytes = UINT64_C(134217728);
  config.process_slot_count = worker_count + 8U;
  config.task_entry_count = (uint32_t)item_count;
  config.ai_action_entry_count = 1;
  config.queue_capacity = qstTestRoundUpPowerOfTwo(item_count);
  config.block_bytes = kPersistStageBlockBytes;
  config.block_count = (uint32_t)(item_count * 3U);
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, &catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE, UINT32_C(1), 1, 0,
                                        0) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  ledger_bench.mapping = &mapping;
  ledger_bench.kind = QST_QUEUE_STAGE_JUDGE_TO_PERSIST;
  ledger_bench.worker_count = 1;
  qstTestQueueStageRegisterWorkers(&ledger_bench, main_generation,
                                   &generator_generation, &exec_generation,
                                   &pair_generation);
  sql_refs = calloc((size_t)item_count, sizeof(*sql_refs));
  record_refs = calloc((size_t)item_count, sizeof(*record_refs));
  assert(sql_refs != NULL && record_refs != NULL);
  for (uint64_t index = 0; index < item_count; ++index) {
    SQstGeneratePayload input = {0};
    SQstExecutePayload final_payload = {0};
    uint64_t gid = 0;
    uint64_t dispatch_epoch = 0;
    uint64_t transition_seq = 0;

    assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                                generator_generation, &gid) == QST_OK);
    assert(gid == index + 1U);
    assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql) - 1U,
                           &sql_refs[index]) == QST_OK);
    assert(qst_block_write_seal(&mapping, &sql_refs[index], sql,
                                sizeof(sql) - 1U) == QST_OK);
    assert(qst_block_alloc(&mapping, gid, 1, sizeof(record) - 1U,
                           &record_refs[index]) == QST_OK);
    assert(qst_block_write_seal(&mapping, &record_refs[index], record,
                                sizeof(record) - 1U) == QST_OK);
    input.target = QST_TARGET_TD;
    input.mode = QST_MODE_FULL;
    input.focus = QST_FOCUS_COVERAGE;
    input.dialect = QST_DIALECT_TDENGINE;
    input.generation_seq = gid;
    input.weight_profile_version = 1;
    input.sql_ref = sql_refs[index];
    input.generation_record_ref = record_refs[index];
    assert(qst_block_sha256(&mapping, input.generation_record_ref, gid,
                            &input.generation_record_sha256) == QST_OK);
    assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                           generator_generation, &input,
                           &dispatch_epoch) == QST_OK);
    assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                   exec_generation, dispatch_epoch, &input,
                                   &transition_seq) == QST_OK);
    assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                exec_generation, dispatch_epoch, transition_seq,
                                0, pair_generation) == QST_OK);
    final_payload.statement_ts_ns =
        timestamp_base_ns + (int64_t)(index * UINT64_C(1000000));
    final_payload.ended_ts_ns = final_payload.statement_ts_ns + 1000;
    final_payload.sid = gid;
    final_payload.execution_status = QST_EXEC_SUCCESS;
    final_payload.result_row_count = 16;
    assert(qst_task_publish_final(&mapping, gid, dispatch_epoch, transition_seq,
                                  QST_ROLE_PAIR_SUPERVISOR, 0, pair_generation,
                                  &final_payload) == QST_OK);
    assert(qst_task_confirm_final_published_pair(
               &mapping, gid, dispatch_epoch, transition_seq, 0,
               pair_generation, &final_payload) == QST_OK);
  }
  bench.mapping = &mapping;
  bench.kind = kind;
  bench.worker_count = worker_count;
  bench.item_count = item_count;
  bench.run_id = config.run_id;
  atomic_init(&bench.next_index, 0);
  atomic_init(&bench.completed_count, 0);
  atomic_init(&bench.error_code, QST_OK);
  if (kind == QST_PERSIST_STAGE_PENDING_FILE) {
    assert(snprintf(pending_root, sizeof(pending_root),
                    "/tmp/qst-stage-persistence-%ld", (long)getpid()) > 0);
    if (mkdir(pending_root, 0700) != 0) {
      assert(errno == EEXIST);
    }
    assert(snprintf(pending_dir, sizeof(pending_dir), "%s/run-%llu", pending_root,
                    (unsigned long long)config.run_id) > 0);
    assert(mkdir(pending_dir, 0700) == 0);
    bench.pending_dir = pending_dir;
  } else {
    qstTestPersistStageSetEndpoint();
    setup_config.run_id = config.run_id;
    assert(qst_result_store_open(&setup_config, &setup_store) == QST_OK);
    assert(qst_result_store_prepare_run(&setup_store, 1U, dataset_digest, 1U) == QST_OK);
    assert(qst_result_store_close(&setup_store) == QST_OK);
  }
  assert(pthread_barrier_init(&bench.start_barrier, NULL, worker_count + 1U) ==
         0);
  for (uint32_t slot = 0; slot < worker_count; ++slot) {
    bench.workers[slot].bench = &bench;
    bench.workers[slot].slot = slot;
    assert(pthread_create(&bench.threads[slot], NULL, qstTestPersistStageWorkerRun,
                          &bench.workers[slot]) == 0);
  }
  assert(getrusage(RUSAGE_SELF, &rusage_before) == 0);
  assert(clock_gettime(CLOCK_MONOTONIC, &started) == 0);
  (void)pthread_barrier_wait(&bench.start_barrier);
  for (uint32_t slot = 0; slot < worker_count; ++slot) {
    assert(pthread_join(bench.threads[slot], NULL) == 0);
  }
  assert(clock_gettime(CLOCK_MONOTONIC, &finished) == 0);
  assert(getrusage(RUSAGE_SELF, &rusage_after) == 0);
  assert(pthread_barrier_destroy(&bench.start_barrier) == 0);
  if (atomic_load_explicit(&bench.error_code, memory_order_acquire) != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: persist stage failed kind=%u workers=%u code=%d\n",
                  kind, worker_count,
                  atomic_load_explicit(&bench.error_code, memory_order_acquire));
    assert(0 && "persist stage worker failed");
  }
  assert(atomic_load_explicit(&bench.completed_count, memory_order_relaxed) ==
         stage_item_count);
  elapsed_seconds = (int64_t)finished.tv_sec - (int64_t)started.tv_sec;
  elapsed_nanoseconds = (int64_t)finished.tv_nsec - (int64_t)started.tv_nsec;
  if (elapsed_nanoseconds < 0) {
    --elapsed_seconds;
    elapsed_nanoseconds += INT64_C(1000000000);
  }
  assert(elapsed_seconds >= 0 && elapsed_nanoseconds > 0);
  elapsed_ns = (uint64_t)elapsed_seconds * UINT64_C(1000000000) +
               (uint64_t)elapsed_nanoseconds;
  (void)fprintf(stdout,
                "{\"stage\":\"%s\",\"worker_threads\":%u,"
                "\"input_count\":%llu,\"completed_count\":%llu,"
                "\"error_count\":0,\"elapsed_ns\":%llu,"
                "\"items_per_second\":%.2f,\"cpu_time_us\":%llu,"
                "\"rss_bytes\":%llu,\"queue\":null}\n",
                kind == QST_PERSIST_STAGE_RESULT_STORE_DML ?
                    "persistence_result_store_dml" : "persistence_pending_file",
                worker_count, (unsigned long long)stage_item_count,
                (unsigned long long)stage_item_count,
                (unsigned long long)elapsed_ns,
                (double)stage_item_count * 1000000000.0 / (double)elapsed_ns,
                (unsigned long long)(qstTestRusageMicroseconds(&rusage_after) -
                                     qstTestRusageMicroseconds(&rusage_before)),
                (unsigned long long)qstTestCurrentRssBytes());
  free(record_refs);
  free(sql_refs);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestPersistStageBenchmarkMatrix(void) {
  const uint64_t result_store_item_count = qstTestReadPositiveUInt64(
      "QST_PERSIST_STAGE_RESULT_STORE_ITEMS", kPersistStageBenchDefaultItems);
  const uint64_t pending_file_item_count = qstTestReadPositiveUInt64(
      "QST_PERSIST_STAGE_PENDING_FILE_ITEMS", 1536);
  const uint64_t result_store_worker_count = qstTestReadPositiveUInt64(
      "QST_PERSIST_STAGE_RESULT_STORE_THREADS", 1);
  const uint64_t pending_file_worker_count = qstTestReadPositiveUInt64(
      "QST_PERSIST_STAGE_PENDING_FILE_THREADS", 1);

  assert(result_store_worker_count <= kPersistStageBenchMaxWorkers &&
         pending_file_worker_count <= kPersistStageBenchMaxWorkers);
  if (getenv("QST_PERSIST_STAGE_PENDING_ONLY") == NULL) {
    qstTestPersistStageBenchmark(QST_PERSIST_STAGE_RESULT_STORE_DML,
                                 (uint32_t)result_store_worker_count,
                                 result_store_item_count);
  }
  qstTestPersistStageBenchmark(QST_PERSIST_STAGE_PENDING_FILE,
                               (uint32_t)pending_file_worker_count,
                               pending_file_item_count);
}

static void qstTestGeneratorExplicitSelfCall(void) {
  static const SQstGeneratorChildCall root_children[] = {
      {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
  };
  static const SQstGeneratorChildCall compose_children[] = {
      {2, QST_GENERATOR_CALL_REQUIRED, 1, 0, 0, 0},
      {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
  };
  static const SQstGeneratorValue root_values[] = {
      {501, QST_GENERATOR_VALUE_CALL_DIMENSION, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 1, 0, root_children, ""},
  };
  static const SQstGeneratorValue expression_values[] = {
      {502, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "0"},
      {503, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 2, 0, compose_children, "("},
      {504, QST_GENERATOR_VALUE_CALL_SELF, QST_GENERATOR_CLASS_ABNORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, ""},
  };
  static const SQstGeneratorValue close_values[] = {
      {505, QST_GENERATOR_VALUE_FIXED_TOKEN, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, ")"},
  };
  static const SQstGeneratorFragment fragments[] = {
      {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
      {2, 1, sizeof(expression_values) / sizeof(expression_values[0]),
       expression_values},
      {3, 1, sizeof(close_values) / sizeof(close_values[0]), close_values},
  };
  SQstGeneratorCatalog catalog = {0};
  SQstGeneratorCursor cursor = {0};
  SQstGeneratedSql sql = {0};

  catalog.version = 1;
  catalog.root_fragment_id = 1;
  catalog.fragment_count = sizeof(fragments) / sizeof(fragments[0]);
  catalog.fragments = fragments;
  assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
  assert(qst_generator_catalog_validate(&catalog) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "0") == 0);
  memset(&cursor, 0, sizeof(cursor));
  assert(qst_generator_full_peek(&catalog, &cursor, 1, &sql) == QST_OK);
  assert(strcmp(sql.sql, "0") == 0);
  assert(qst_generator_full_commit(&cursor, &sql) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 1, &sql) == QST_OK);
  assert(strcmp(sql.sql, "(0)") == 0);
  assert(qst_generator_full_commit(&cursor, &sql) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 1, &sql) == QST_OK);
  assert(strcmp(sql.sql, "0") == 0);
  assert(sql.contains_abnormal == 1);
  qst_generator_generated_sql_destroy(&sql);
}

static void qstTestGeneratorCatalogDigestIncludesTextLength(void) {
  static const SQstGeneratorValue left_values[] = {
      {601, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "a"},
      {602, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "bc"},
  };
  static const SQstGeneratorValue right_values[] = {
      {601, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "ab"},
      {602, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "c"},
  };
  static const SQstGeneratorFragment left_fragments[] = {
      {1, 1, sizeof(left_values) / sizeof(left_values[0]), left_values},
  };
  static const SQstGeneratorFragment right_fragments[] = {
      {1, 1, sizeof(right_values) / sizeof(right_values[0]), right_values},
  };
  SQstGeneratorCatalog left = {1, 1,
                                sizeof(left_fragments) / sizeof(left_fragments[0]),
                                left_fragments, {{0}}};
  SQstGeneratorCatalog right = {
      1, 1, sizeof(right_fragments) / sizeof(right_fragments[0]),
      right_fragments, {{0}}};

  assert(qst_generator_catalog_digest(&left, &left.digest) == QST_OK);
  assert(qst_generator_catalog_digest(&right, &right.digest) == QST_OK);
  assert(memcmp(left.digest.bytes, right.digest.bytes,
                sizeof(left.digest.bytes)) != 0);
}

static void qstTestGeneratorInlineValueSteps(void) {
  static SQstGeneratorStep root_steps[] = {
      {QST_GENERATOR_STEP_INLINE_VALUE, 9001, "EXPLAIN", {0}},
      {QST_GENERATOR_STEP_INLINE_VALUE, 9004, "SELECT", {0}},
      {QST_GENERATOR_STEP_CALL, 0, NULL,
       {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0}},
      {QST_GENERATOR_STEP_INLINE_VALUE, 9005, "FOR", {0}},
      {QST_GENERATOR_STEP_INLINE_VALUE, 9006, "UPDATE", {0}},
      {QST_GENERATOR_STEP_INLINE_VALUE, 9002, ";", {0}},
  };
  static const SQstGeneratorValue root_values[] = {
      {9000, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "",
       sizeof(root_steps) / sizeof(root_steps[0]), root_steps},
  };
  static const SQstGeneratorValue literal_values[] = {
      {9003, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "1", 0, NULL},
  };
  static const SQstGeneratorFragment fragments[] = {
      {1, 1, sizeof(root_values) / sizeof(root_values[0]), root_values},
      {2, 1, sizeof(literal_values) / sizeof(literal_values[0]), literal_values},
  };
  SQstGeneratorCatalog catalog = {0};
  SQstGeneratorCatalog altered = {0};
  SQstGeneratorCursor cursor = {0};
  SQstGeneratedSql sql = {0};

  catalog.version = 1;
  catalog.root_fragment_id = 1;
  catalog.fragment_count = sizeof(fragments) / sizeof(fragments[0]);
  catalog.fragments = fragments;
  assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
  assert(qst_generator_catalog_validate(&catalog) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "EXPLAIN SELECT 1 FOR UPDATE;") == 0);
  assert(sql.occurrence_count == 7);
  assert(sql.occurrences[1].value_id == 9001);
  assert(sql.occurrences[1].text_length == strlen("EXPLAIN"));
  assert(sql.occurrences[6].value_id == 9002);
  altered = catalog;
  root_steps[0].value_id = 9004;
  assert(qst_generator_catalog_digest(&altered, &altered.digest) == QST_OK);
  assert(memcmp(catalog.digest.bytes, altered.digest.bytes,
                sizeof(catalog.digest.bytes)) != 0);
  root_steps[0].value_id = 0;
  assert(qst_generator_catalog_validate(&catalog) == QST_E_INVALID_ARGUMENT);
  root_steps[0].value_id = 9001;
  qst_generator_generated_sql_destroy(&sql);
}

static void qstTestGeneratorUnboundedLocalTrace(void) {
  enum { kChildCount = 4097 };
  SQstGeneratorChildCall *children = NULL;
  SQstGeneratorValue root_values[1] = {{0}};
  SQstGeneratorValue leaf_values[1] = {{0}};
  SQstGeneratorFragment fragments[2] = {{0}};
  SQstGeneratorCatalog catalog = {0};
  SQstGeneratorCatalog valid = qstTestGeneratorCatalog();
  SQstGeneratorCursor cursor = {0};
  SQstGeneratorRandomState random_state = {0};
  SQstGeneratedSql sql = {0};

  children = calloc(kChildCount, sizeof(*children));
  assert(children != NULL);
  for (uint32_t index = 0; index < kChildCount; ++index) {
    children[index].fragment_id = 2;
    children[index].call_kind = QST_GENERATOR_CALL_REQUIRED;
  }
  root_values[0].value_id = 701;
  root_values[0].kind = QST_GENERATOR_VALUE_COMPOSE;
  root_values[0].generation_class = QST_GENERATOR_CLASS_NORMAL;
  root_values[0].return_state = QST_GENERATOR_RETURN_NON_EMPTY;
  root_values[0].weight = 1;
  root_values[0].child_count = kChildCount;
  root_values[0].children = children;
  root_values[0].text = "X";
  leaf_values[0].value_id = 702;
  leaf_values[0].kind = QST_GENERATOR_VALUE_ATOMIC;
  leaf_values[0].generation_class = QST_GENERATOR_CLASS_NORMAL;
  leaf_values[0].return_state = QST_GENERATOR_RETURN_NON_EMPTY;
  leaf_values[0].weight = 1;
  leaf_values[0].text = "";
  fragments[0] = (SQstGeneratorFragment){1, 1, 1, root_values};
  fragments[1] = (SQstGeneratorFragment){2, 1, 1, leaf_values};
  catalog.version = 1;
  catalog.root_fragment_id = 1;
  catalog.fragment_count = 2;
  catalog.fragments = fragments;
  assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
  assert(qst_generator_catalog_validate(&catalog) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(sql.choice_count == kChildCount + 1U);
  assert(sql.occurrence_count == kChildCount + 1U);
  assert(strcmp(sql.sql, "X") == 0);
  assert(sql.occurrences[0].occurrence_id == 1);
  assert(sql.occurrences[0].return_state == QST_GENERATOR_RETURN_NON_EMPTY);
  assert(sql.occurrences[0].text_offset == 0);
  assert(sql.occurrences[0].text_length == 1);
  assert(qst_generator_full_commit(&cursor, &sql) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) ==
         QST_E_QUEUE_EMPTY);
  qst_generator_generated_sql_destroy(&sql);
  free(children);
  assert(qst_generator_full_peek(&valid, &cursor,
                                 QST_GENERATOR_MAX_RECURSION_DEPTH + 1U,
                                 &sql) == QST_E_INVALID_ARGUMENT);
  assert(qst_generator_random_init(&random_state, 1, 1) == QST_OK);
  assert(qst_generator_random_build(&valid, &random_state,
                                    QST_GENERATOR_MAX_RECURSION_DEPTH + 1U,
                                    &sql) == QST_E_INVALID_ARGUMENT);
}

static void qstTestGeneratorOptionalLoopProtocol(void) {
  static const SQstGeneratorChildCall root_children[] = {
      {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
      {3, QST_GENERATOR_CALL_OPTIONAL, 0, 0, 0, 0},
      {4, QST_GENERATOR_CALL_LOOP, 0, 0, 1, 2},
  };
  static const SQstGeneratorValue root_values[] = {
      {801, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 3, 0, root_children, ""},
  };
  static const SQstGeneratorValue required_values[] = {
      {802, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "A"},
  };
  static const SQstGeneratorValue optional_values[] = {
      {803, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_ABNORMAL,
       QST_GENERATOR_RETURN_EMPTY_RESULT, 1, 0, 0, NULL, ""},
      {804, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "B"},
  };
  static const SQstGeneratorValue loop_values[] = {
      {805, QST_GENERATOR_VALUE_ATOMIC, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, 0, NULL, "C"},
  };
  static const SQstGeneratorFragment fragments[] = {
      {1, 1, 1, root_values}, {2, 1, 1, required_values},
      {3, 1, 2, optional_values}, {4, 1, 1, loop_values},
  };
  SQstGeneratorCatalog catalog = {0};
  SQstGeneratorCursor cursor = {0};
  SQstGeneratedSql sql = {0};

  catalog.version = 1;
  catalog.root_fragment_id = 1;
  catalog.fragment_count = sizeof(fragments) / sizeof(fragments[0]);
  catalog.fragments = fragments;
  assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
  assert(qst_generator_catalog_validate(&catalog) == QST_OK);
  assert(qst_generator_full_next(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "A") == 0);
  assert(sql.occurrence_count == 3);
  assert(sql.occurrences[2].return_state == QST_GENERATOR_RETURN_EMPTY_RESULT);
  assert(qst_generator_full_next(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "A C") == 0);
  assert(qst_generator_full_next(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "A C C") == 0);
  qst_generator_generated_sql_destroy(&sql);
}

static void qstTestGeneratorSqlCapacityFallback(void) {
  char oversized[QST_GENERATOR_SQL_BYTES + 1U] = {0};
  char executable_half[QST_GENERATOR_EXECUTABLE_SQL_BYTES / 2U + 1U] = {0};
  SQstGeneratorValue values[] = {
      {9101, QST_GENERATOR_VALUE_FIXED_TOKEN, QST_GENERATOR_CLASS_NORMAL,
       QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_NONE, NULL,
       oversized, 0, NULL, 0},
  };
  SQstGeneratorFragment fragments[] = {
      {1, 1, sizeof(values) / sizeof(values[0]), values},
  };
  SQstGeneratorCatalog catalog = {1, 1,
                                  sizeof(fragments) / sizeof(fragments[0]),
                                  fragments, {{0}}};
  SQstGeneratorRandomState random_state = {0};
  SQstGeneratorCursor cursor = {0};
  SQstGeneratedSql sql = {0};

  memset(oversized, 'x', sizeof(oversized) - 1U);
  assert(qst_generator_catalog_digest(&catalog, &catalog.digest) == QST_OK);
  assert(qst_generator_catalog_validate(&catalog) == QST_OK);
  assert(qst_generator_random_init(&random_state, 1, 1) == QST_OK);
  assert(qst_generator_random_build(&catalog, &random_state, 0, &sql) ==
         QST_E_SQL_LIMIT);
  assert(sql.sql_length == 0);
  assert(sql.contains_unsupported_fallback == 0);

  {
    const SQstGeneratorChildCall executable_children[] = {
        {2, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
        {3, QST_GENERATOR_CALL_REQUIRED, 0, 0, 0, 0},
    };
    SQstGeneratorValue executable_root_values[] = {
        {9201, QST_GENERATOR_VALUE_COMPOSE, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 2, QST_RUNTIME_OBJECT_NONE,
         executable_children, "A", 0, NULL, 0},
    };
    SQstGeneratorValue executable_left_values[] = {
        {9202, QST_GENERATOR_VALUE_FIXED_TOKEN, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_NONE, NULL,
         executable_half, 0, NULL, 0},
    };
    SQstGeneratorValue executable_right_values[] = {
        {9203, QST_GENERATOR_VALUE_FIXED_TOKEN, QST_GENERATOR_CLASS_NORMAL,
         QST_GENERATOR_RETURN_NON_EMPTY, 1, 0, QST_RUNTIME_OBJECT_NONE, NULL,
         executable_half, 0, NULL, 0},
    };
    SQstGeneratorFragment executable_fragments[] = {
        {1, 1, 1, executable_root_values},
        {2, 1, 1, executable_left_values},
        {3, 1, 1, executable_right_values},
    };
    SQstGeneratorCatalog executable_catalog = {
        1, 1, sizeof(executable_fragments) / sizeof(executable_fragments[0]),
        executable_fragments, {{0}}};
    SQstGeneratorCursor executable_cursor = {0};

    memset(executable_half, 'y', sizeof(executable_half) - 1U);
    assert(qst_generator_catalog_digest(&executable_catalog,
                                        &executable_catalog.digest) == QST_OK);
    assert(qst_generator_catalog_validate(&executable_catalog) == QST_OK);
    assert(qst_generator_random_init(&random_state, 1, 1) == QST_OK);
    assert(qst_generator_random_build(&executable_catalog, &random_state, 0,
                                      &sql) == QST_E_SQL_LIMIT);
    assert(sql.sql_length == 0);
    assert(sql.contains_unsupported_fallback == 0);
    assert(qst_generator_full_next(&executable_catalog, &executable_cursor, 0,
                                   &sql) == QST_OK);
    assert(strcmp(sql.sql, "?") == 0);
    assert(sql.contains_unsupported_fallback != 0);
    qst_generator_generated_sql_destroy(&sql);
  }

  assert(qst_generator_random_init(&random_state, 1, 1) == QST_OK);
  assert(qst_generator_random_build(&catalog, &random_state, 0, &sql) ==
         QST_E_SQL_LIMIT);
  assert(sql.sql_length == 0);

  assert(qst_generator_full_next(&catalog, &cursor, 0, &sql) == QST_OK);
  assert(strcmp(sql.sql, "?") == 0);
  assert(sql.sql_length == 1);
  assert(sql.contains_abnormal != 0);
  assert(sql.contains_unsupported_fallback != 0);
  assert(sql.choice_count == 1 && sql.choice_cardinalities[0] == 1);
  assert(sql.occurrence_count == 1);
  assert(sql.occurrences[0].text_offset == 0);
  assert(sql.occurrences[0].text_length == 0);
  qst_generator_generated_sql_destroy(&sql);
  assert(cursor.exhausted != 0);
}

static const SQstGeneratorValue *qstTestFindCatalogValue(
    const SQstGeneratorCatalog *catalog, uint32_t fragment_id,
    uint32_t value_id) {
  if (catalog == NULL) {
    return NULL;
  }
  for (uint32_t fragment_index = 0;
       fragment_index < catalog->fragment_count; ++fragment_index) {
    const SQstGeneratorFragment *fragment =
        &catalog->fragments[fragment_index];

    if (fragment->fragment_id != fragment_id) {
      continue;
    }
    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      const SQstGeneratorValue *value = &fragment->values[value_index];

      for (uint32_t step_index = 0; step_index < value->step_count;
           ++step_index) {
        if (value->steps[step_index].kind ==
                QST_GENERATOR_STEP_INLINE_VALUE &&
            value->steps[step_index].value_id == value_id) {
          return value;
        }
      }
    }
    return NULL;
  }
  return NULL;
}

static void qstTestCompiledSqlDimensionCatalog(
    const SQstGeneratorRuntimeContext *runtime) {
  enum {
    kRandomAuditMinimumSequences = 256,
    kRandomAuditDigestCapacity = 1024,
    kRandomAuditTrackedFragmentCount = 11,
  };
  static const uint64_t kSeeds[] = {
      UINT64_C(0x1234), UINT64_C(0x5a17c0de), UINT64_C(1),
      UINT64_C(0x9e3779b97f4a7c15),
  };
  static const uint32_t kTrackedFragments[kRandomAuditTrackedFragmentCount] = {
      SQL_DIMENSION_FRAG_QUERY_TOP_LEVEL,
      SQL_DIMENSION_FRAG_QUERY_STMT,
      SQL_DIMENSION_FRAG_QUERY_EXPLAIN_TARGET,
      SQL_DIMENSION_FRAG_QUERY_SELECT,
      SQL_DIMENSION_FRAG_QUERY_VALUES,
      SQL_DIMENSION_FRAG_QUERY_TABLE,
      SQL_DIMENSION_FRAG_QUERY_CTE,
      SQL_DIMENSION_FRAG_QUERY_SET,
      SQL_DIMENSION_FRAG_WHERE,
      SQL_DIMENSION_FRAG_GROUP_BY,
      SQL_DIMENSION_FRAG_ORDER_BY,
  };
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGeneratorRandomState random_state = {0};
  SQstGeneratedSql sql = {0};
  const char *limit_text = getenv("QST_CORE_GENERATOR_STRESS_SEQUENCE_LIMIT");
  uint64_t sequence_limit = 256;
  uint64_t normal_count = 0;
  uint64_t abnormal_count = 0;
  uint64_t fallback_count = 0;
  uint64_t tracked_fragment_hits[kRandomAuditTrackedFragmentCount] = {0};
  SQstDigest256 distinct_digests[kRandomAuditDigestCapacity] = {{0}};
  uint32_t distinct_digest_count = 0;
  uint32_t fallback_shape_checked = 0;

  if (limit_text != NULL) {
    char *end = NULL;

    errno = 0;
    sequence_limit = strtoull(limit_text, &end, 10);
    assert(errno == 0 && end != limit_text && *end == '\0' &&
           sequence_limit != 0);
  }
  if (sequence_limit < kRandomAuditMinimumSequences) {
    sequence_limit = kRandomAuditMinimumSequences;
  }

  assert(runtime != NULL);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(catalog != NULL);
  assert(catalog->version == SQL_DIMENSION_CONFIG_VERSION);
  assert(catalog->root_fragment_id == SQL_DIMENSION_FRAG_QUERY_ROOT);
  assert(catalog->fragment_count > SQL_DIMENSION_FRAG_SOURCE_COUNT);
  {
    const SQstGeneratorFragment *root = NULL;
    const SQstGeneratorValue *empty_value = NULL;
    const SQstGeneratorValue *literal_null = NULL;
    const SQstGeneratorValue *foreign_dialect_value = NULL;
    const SQstGeneratorFragment *runtime_table_reference = NULL;

    for (uint32_t index = 0; index < catalog->fragment_count; ++index) {
      if (catalog->fragments[index].fragment_id ==
          SQL_DIMENSION_FRAG_QUERY_ROOT) {
        root = &catalog->fragments[index];
        break;
      }
    }
    assert(root != NULL && root->value_count == 3);
    assert(root->values[0].generation_class == QST_GENERATOR_CLASS_ABNORMAL);
    assert(root->values[0].weight == UINT64_C(50000));
    assert(root->values[0].kind == QST_GENERATOR_VALUE_COMPOSE);
    assert(root->values[0].step_count == 2);
    assert(root->values[0].steps[0].kind == QST_GENERATOR_STEP_CALL);
    assert(root->values[0].steps[0].child_call.fragment_id ==
           SQL_DIMENSION_FRAG_QUERY_TOP_LEVEL);
    assert(root->values[0].steps[1].kind == QST_GENERATOR_STEP_CALL);
    assert(root->values[0].steps[1].child_call.fragment_id ==
           SQL_DIMENSION_FRAG_QUERY_TERMINATOR);
    assert(root->values[1].generation_class == QST_GENERATOR_CLASS_NORMAL);
    assert(root->values[1].weight == UINT64_C(950000));
    assert(root->values[1].step_count == 3);
    assert(root->values[1].steps[0].value_id == SQL_DIMENSION_VAL_CLAUSE_SELECT);
    assert(root->values[1].steps[1].value_id == SQL_DIMENSION_VAL_INTEGER_1);
    assert(root->values[1].steps[2].value_id ==
           SQL_DIMENSION_VAL_QUERY_TERMINATOR_SEMICOLON);
    assert(root->values[2].generation_class == QST_GENERATOR_CLASS_ABNORMAL);
    assert(root->values[2].weight == UINT64_C(50000));
    empty_value = qstTestFindCatalogValue(
        catalog, SQL_DIMENSION_FRAG_LITERAL_NULL, SQL_DIMENSION_VAL_EMPTY);
    literal_null = qstTestFindCatalogValue(
        catalog, SQL_DIMENSION_FRAG_LITERAL_NULL,
        SQL_DIMENSION_VAL_LITERAL_NULL);
    foreign_dialect_value = qstTestFindCatalogValue(
        catalog, SQL_DIMENSION_FRAG_CLAUSE_ONLY,
        SQL_DIMENSION_VAL_CLAUSE_ONLY);
    for (uint32_t index = 0; index < catalog->fragment_count; ++index) {
      if (catalog->fragments[index].fragment_id ==
          SQL_DIMENSION_FRAG_RUNTIME_TABLE_REFERENCE) {
        runtime_table_reference = &catalog->fragments[index];
        break;
      }
    }
    assert(empty_value != NULL && literal_null != NULL &&
           foreign_dialect_value != NULL &&
           runtime_table_reference != NULL &&
           runtime_table_reference->value_count != 0);
    assert(empty_value->generation_class == QST_GENERATOR_CLASS_NORMAL &&
           empty_value->weight == UINT64_C(50000));
    assert(literal_null->generation_class == QST_GENERATOR_CLASS_NORMAL &&
           literal_null->weight == UINT64_C(950000));
    assert(foreign_dialect_value->generation_class ==
               QST_GENERATOR_CLASS_NORMAL &&
           foreign_dialect_value->weight == UINT64_C(50000));
    assert(runtime_table_reference->values[
               runtime_table_reference->value_count - 1U].generation_class ==
               QST_GENERATOR_CLASS_ABNORMAL);
    assert(runtime_table_reference->values[
               runtime_table_reference->value_count - 1U].weight == 0);
  }
  assert(qst_generator_catalog_validate(catalog) == QST_OK);
  assert(qst_generator_catalog_validate(catalog) == QST_OK);
  qstTestCompiledSqlDimensionCatalogReachability();
  for (uint32_t index = 0; index < sizeof(kSeeds) / sizeof(kSeeds[0]); ++index) {
    for (uint64_t sequence = 1; sequence <= sequence_limit; ++sequence) {
      EQstCode code = qst_generator_random_init(
          &random_state, kSeeds[index], sequence);

      assert(code == QST_OK);
      code = qst_generator_random_build_with_runtime(
          catalog, &random_state, QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime,
          &sql);
      if (code != QST_OK) {
        (void)fprintf(stderr,
                      "taosQueryTest: compiled catalog random build failed "
                      "seed=%llu sequence=%llu code=%d\n",
                      (unsigned long long)kSeeds[index],
                      (unsigned long long)sequence, code);
      }
      assert(code == QST_OK);
      assert(sql.choice_count != 0);
      assert(sql.occurrence_count != 0);
      assert(sql.sql[strspn(sql.sql, " \t\r\n")] != '\0');
      if (sql.contains_abnormal != 0) {
        ++abnormal_count;
      } else {
        assert(strcmp(sql.sql, "SELECT 1;") == 0);
        ++normal_count;
      }
      if (sql.contains_unsupported_fallback != 0) {
        if (fallback_shape_checked == 0) {
          assert(strcmp(sql.sql, "?") == 0);
          assert(sql.sql_length == 1);
          for (uint32_t occurrence_index = 0;
               occurrence_index < sql.occurrence_count; ++occurrence_index) {
            assert(sql.occurrences[occurrence_index].text_offset == 0);
            assert(sql.occurrences[occurrence_index].text_length == 0);
          }
          fallback_shape_checked = 1;
        }
        if (fallback_count < 5) {
          const uint32_t occurrence_begin =
              sql.occurrence_count > 12 ? sql.occurrence_count - 12 : 0;

          (void)fprintf(stderr,
                        "taosQueryTest: generator fallback seed=%llu "
                        "sequence=%llu choices=%u occurrences=%u sql=%.256s\n",
                        (unsigned long long)kSeeds[index],
                        (unsigned long long)sequence, sql.choice_count,
                        sql.occurrence_count, sql.sql);
          for (uint32_t occurrence_index = occurrence_begin;
               occurrence_index < sql.occurrence_count; ++occurrence_index) {
            const SQstGenerationOccurrence *occurrence =
                &sql.occurrences[occurrence_index];

            (void)fprintf(stderr,
                          "taosQueryTest: generator fallback occurrence=%u "
                          "fragment=%u value=%u parent=%u child=%u depth=%u\n",
                          occurrence->occurrence_id, occurrence->fragment_id,
                          occurrence->value_id, occurrence->parent_occurrence_id,
                          occurrence->child_index, occurrence->depth);
          }
        }
        ++fallback_count;
      }
      if (sequence <= kRandomAuditMinimumSequences &&
          distinct_digest_count < kRandomAuditDigestCapacity) {
        uint32_t digest_index = 0;

        for (; digest_index < distinct_digest_count; ++digest_index) {
          if (memcmp(distinct_digests[digest_index].bytes, sql.sql_sha256.bytes,
                     sizeof(sql.sql_sha256.bytes)) == 0) {
            break;
          }
        }
        if (digest_index == distinct_digest_count) {
          distinct_digests[distinct_digest_count++] = sql.sql_sha256;
        }
      }
      for (uint32_t occurrence_index = 0;
           occurrence_index < sql.occurrence_count; ++occurrence_index) {
        for (uint32_t fragment_index = 0;
             fragment_index < kRandomAuditTrackedFragmentCount;
             ++fragment_index) {
          if (sql.occurrences[occurrence_index].fragment_id ==
              kTrackedFragments[fragment_index]) {
            ++tracked_fragment_hits[fragment_index];
          }
        }
      }
      qst_generator_generated_sql_destroy(&sql);
      if (limit_text != NULL && sequence % UINT64_C(1024) == 0) {
        (void)fprintf(stderr,
                      "taosQueryTest: generator random stress seed=%llu "
                      "sequence=%llu limit=%llu\n",
                      (unsigned long long)kSeeds[index],
                      (unsigned long long)sequence,
                      (unsigned long long)sequence_limit);
      }
    }
  }
  (void)fprintf(stderr,
                "taosQueryTest: generator random audit normal=%llu "
                "abnormal=%llu fallback=%llu distinct_sql=%u total=%llu\n",
                (unsigned long long)normal_count,
                (unsigned long long)abnormal_count,
                (unsigned long long)fallback_count,
                distinct_digest_count,
                (unsigned long long)(normal_count + abnormal_count));
  for (uint32_t fragment_index = 0;
       fragment_index < kRandomAuditTrackedFragmentCount; ++fragment_index) {
    (void)fprintf(stderr,
                  "taosQueryTest: generator random fragment=%u hits=%llu\n",
                  kTrackedFragments[fragment_index],
                  (unsigned long long)tracked_fragment_hits[fragment_index]);
  }
  /* Dynamic-infeasible RANDOM paths are rebuilt from the same deterministic
   * stream and must never be published as the old short "?" fallback. */
  assert(normal_count + abnormal_count != 0);
  assert(distinct_digest_count >= 16U);
  for (uint32_t fragment_index = 0;
       fragment_index < kRandomAuditTrackedFragmentCount; ++fragment_index) {
    assert(tracked_fragment_hits[fragment_index] != 0);
  }
  assert(fallback_count == 0);
  assert(fallback_shape_checked == 0);
  {
    enum { kBenchmarkPaths = 1024 };
    SQstDatasetInfo dataset = {0};
    SQstGeneratorRuntimeContext prepared_runtime = *runtime;
    SQstGeneratorRandomRuntime prepared_catalog = {0};
    SQstObjectDescriptor *objects = NULL;
    struct timespec started = {0};
    struct timespec finished = {0};
    uint32_t object_count = 0;
    uint64_t elapsed_ns = 0;
    int64_t elapsed_seconds = 0;
    int64_t elapsed_nanoseconds = 0;
    double paths_per_second = 0.0;

    dataset.object_directory_ref = runtime->object_directory_ref;
    dataset.fixture_manifest_ref = runtime->fixture_manifest_ref;
    assert(qst_dataset_get_object_count(runtime->mapping, &dataset,
                                        &object_count) == QST_OK);
    assert(object_count != 0 &&
           (uint64_t)object_count <= SIZE_MAX / sizeof(*objects));
    objects = calloc(object_count, sizeof(*objects));
    assert(objects != NULL);
    assert(qst_dataset_copy_object_directory(runtime->mapping, &dataset, objects,
                                             object_count, &object_count) == QST_OK);
    prepared_runtime.objects = objects;
    prepared_runtime.object_count = object_count;
    assert(qst_generator_random_runtime_prepare(&prepared_catalog, catalog) ==
           QST_OK);
    assert(clock_gettime(CLOCK_MONOTONIC, &started) == 0);
    for (uint64_t sequence = 1; sequence <= kBenchmarkPaths; ++sequence) {
      EQstCode code = qst_generator_random_init(&random_state,
                                                 UINT64_C(0x5a17c0de), sequence);

      assert(code == QST_OK);
      code = qst_generator_random_build_prepared_with_runtime(
          catalog, &random_state, QST_GENERATOR_DEFAULT_QUERY_DEPTH,
          &prepared_runtime, &prepared_catalog, &sql);
      assert(code == QST_OK);
      qst_generator_generated_sql_destroy(&sql);
    }
    assert(clock_gettime(CLOCK_MONOTONIC, &finished) == 0);
    assert(finished.tv_sec > started.tv_sec ||
           (finished.tv_sec == started.tv_sec &&
            finished.tv_nsec >= started.tv_nsec));
    elapsed_seconds = (int64_t)finished.tv_sec - (int64_t)started.tv_sec;
    elapsed_nanoseconds = (int64_t)finished.tv_nsec - (int64_t)started.tv_nsec;
    if (elapsed_nanoseconds < 0) {
      --elapsed_seconds;
      elapsed_nanoseconds += INT64_C(1000000000);
    }
    assert(elapsed_seconds >= 0 && elapsed_nanoseconds >= 0);
    elapsed_ns = (uint64_t)elapsed_seconds * UINT64_C(1000000000) +
                 (uint64_t)elapsed_nanoseconds;
    assert(elapsed_ns != 0);
    paths_per_second = (double)kBenchmarkPaths * 1000000000.0 /
                       (double)elapsed_ns;
    (void)fprintf(stderr,
                  "taosQueryTest: generator pure benchmark paths=%u "
                  "elapsed_ms=%.3f paths_per_second=%.2f\n",
                  kBenchmarkPaths, (double)elapsed_ns / 1000000.0,
                  paths_per_second);
    qst_generator_random_runtime_destroy(&prepared_catalog);
    free(objects);
  }
}

static int32_t qstTestFindCompiledCatalogFragment(
    const SQstGeneratorCatalog *catalog, uint32_t fragment_id,
    uint32_t *out_fragment_index) {
  if (catalog == NULL || out_fragment_index == NULL) {
    return 0;
  }
  for (uint32_t index = 0; index < catalog->fragment_count; ++index) {
    if (catalog->fragments[index].fragment_id == fragment_id) {
      *out_fragment_index = index;
      return 1;
    }
  }
  return 0;
}

static void qstTestMarkCompiledCatalogReachable(
    const SQstGeneratorCatalog *catalog, uint32_t fragment_index,
    uint8_t *reachable) {
  const SQstGeneratorFragment *fragment = NULL;

  assert(catalog != NULL && reachable != NULL);
  assert(fragment_index < catalog->fragment_count);
  if (reachable[fragment_index] != 0) {
    return;
  }
  reachable[fragment_index] = 1;
  fragment = &catalog->fragments[fragment_index];
  for (uint32_t value_index = 0; value_index < fragment->value_count;
       ++value_index) {
    const SQstGeneratorValue *value = &fragment->values[value_index];

    for (uint32_t child_index = 0; child_index < value->child_count;
         ++child_index) {
      uint32_t next_fragment_index = 0;

      assert(qstTestFindCompiledCatalogFragment(
                 catalog, value->children[child_index].fragment_id,
                 &next_fragment_index) != 0);
      qstTestMarkCompiledCatalogReachable(catalog, next_fragment_index,
                                          reachable);
    }
    for (uint32_t step_index = 0; step_index < value->step_count;
         ++step_index) {
      const SQstGeneratorStep *step = &value->steps[step_index];
      uint32_t next_fragment_index = 0;

      if (step->kind != QST_GENERATOR_STEP_CALL) {
        continue;
      }
      assert(qstTestFindCompiledCatalogFragment(
                 catalog, step->child_call.fragment_id,
                 &next_fragment_index) != 0);
      qstTestMarkCompiledCatalogReachable(catalog, next_fragment_index,
                                          reachable);
    }
  }
}

static void qstTestCompiledSqlDimensionCatalogReachability(void) {
  static const uint32_t kStatementFragments[] = {
      SQL_DIMENSION_FRAG_QUERY_TOP_LEVEL,
      SQL_DIMENSION_FRAG_QUERY_STMT,
      SQL_DIMENSION_FRAG_QUERY_EXPLAIN_TARGET,
      SQL_DIMENSION_FRAG_QUERY_SELECT,
      SQL_DIMENSION_FRAG_QUERY_VALUES,
      SQL_DIMENSION_FRAG_QUERY_TABLE,
      SQL_DIMENSION_FRAG_QUERY_CTE,
      SQL_DIMENSION_FRAG_QUERY_SET,
      SQL_DIMENSION_FRAG_WHERE,
      SQL_DIMENSION_FRAG_GROUP_BY,
      SQL_DIMENSION_FRAG_ORDER_BY,
  };
  const SQstGeneratorCatalog *catalog = NULL;
  uint8_t *reachable = NULL;
  uint32_t root_index = 0;

  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(catalog != NULL && catalog->fragment_count != 0);
  reachable = calloc(catalog->fragment_count, sizeof(*reachable));
  assert(reachable != NULL);
  assert(qstTestFindCompiledCatalogFragment(catalog, catalog->root_fragment_id,
                                            &root_index) != 0);
  qstTestMarkCompiledCatalogReachable(catalog, root_index, reachable);
  for (uint32_t required_index = 0;
       required_index < sizeof(kStatementFragments) / sizeof(kStatementFragments[0]);
       ++required_index) {
    uint32_t fragment_index = 0;

    assert(qstTestFindCompiledCatalogFragment(catalog,
                                              kStatementFragments[required_index],
                                              &fragment_index) != 0);
    assert(reachable[fragment_index] != 0);
  }
  free(reachable);
}

static void qstTestCompiledSqlDimensionCatalogFull(
    const SQstGeneratorRuntimeContext *runtime) {
  const SQstGeneratorCatalog *catalog = NULL;
  uint64_t path_count = 0;
  uint64_t fallback_count = 0;

  assert(runtime != NULL);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(qst_generator_full_exhaustive_count(
             catalog, QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime, &path_count,
             &fallback_count) == QST_OK);
  (void)fprintf(stderr,
                "taosQueryTest: generator full exhaustive complete paths=%llu "
                "fallback=%llu\n",
                (unsigned long long)path_count,
                (unsigned long long)fallback_count);
  assert(path_count != 0);
  assert(fallback_count == 0);
}

static void qstTestCompiledSqlDimensionCatalogRandomStress(
    const SQstGeneratorRuntimeContext *runtime) {
  const char *limit_text = getenv("QST_CORE_GENERATOR_STRESS_SEQUENCE_LIMIT");
  const char *shard_text = getenv("QST_CORE_GENERATOR_STRESS_SHARD_INDEX");
  const uint64_t default_sequence_limit = UINT64_C(100000000);
  static const uint64_t seeds[] = {
      UINT64_C(0x1234), UINT64_C(0x5a17c0de), UINT64_C(1),
      UINT64_C(0x9e3779b97f4a7c15)};
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGeneratorFragPlan plan = {0};
  SQstGeneratorRandomRuntime prepared_runtime = {0};
  SQstGeneratorRandomState random_state = {0};
  SQstGeneratedSql sql = {0};
  uint8_t *selected_fragments = NULL;
  uint8_t *selected_values = NULL;
  uint32_t max_fragment_id = 0;
  uint32_t max_value_id = 0;
  uint64_t selected_fragment_count = 0;
  uint64_t selected_value_count = 0;
  uint64_t unselected_fragment_count = 0;
  uint64_t unselected_value_count = 0;
  uint64_t sequence_limit = default_sequence_limit;
  uint64_t shard_index = 0;
  uint64_t normal_count = 0;
  uint64_t abnormal_count = 0;
  uint64_t text_limit_count = 0;

  assert(runtime != NULL);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(catalog != NULL);
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];

    if (fragment->fragment_id > max_fragment_id) {
      max_fragment_id = fragment->fragment_id;
    }
    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      if (fragment->values[value_index].value_id > max_value_id) {
        max_value_id = fragment->values[value_index].value_id;
      }
    }
  }
  assert(max_fragment_id != UINT32_MAX && max_value_id != UINT32_MAX);
  selected_fragments = calloc((size_t)max_fragment_id + 1U, sizeof(uint8_t));
  selected_values = calloc((size_t)max_value_id + 1U, sizeof(uint8_t));
  assert(selected_fragments != NULL && selected_values != NULL);
  if (limit_text != NULL) {
    char *end = NULL;

    errno = 0;
    sequence_limit = strtoull(limit_text, &end, 10);
    assert(errno == 0 && end != limit_text && *end == '\0' &&
           sequence_limit != 0);
  }
  if (shard_text != NULL) {
    char *end = NULL;

    errno = 0;
    shard_index = strtoull(shard_text, &end, 10);
    assert(errno == 0 && end != shard_text && *end == '\0');
  }
  assert(qst_generator_random_runtime_prepare(&prepared_runtime, catalog) ==
         QST_OK);
  prepared_runtime.selection_only = 1;
  prepared_runtime.profile_enabled = 1;
  prepared_runtime.selection_fragment_count = max_fragment_id + 1U;
  prepared_runtime.selection_fragments = selected_fragments;
  prepared_runtime.selection_value_count = max_value_id + 1U;
  prepared_runtime.selection_values = selected_values;
  assert(qst_generator_frag_plan_prepare(&plan, catalog) == QST_OK);
  for (uint32_t seed_index = 0;
       seed_index < sizeof(seeds) / sizeof(seeds[0]); ++seed_index) {
    SQstGeneratorRandomState eager = {0};
    SQstGeneratorRandomState lazy = {0};

    assert(qst_generator_random_init(&eager, seeds[seed_index], 17) == QST_OK);
    assert(qst_generator_random_init_lazy(&lazy, seeds[seed_index], 17) ==
           QST_OK);
    for (uint32_t value_index = 0;
         value_index < QST_GENERATOR_RANDOM_ARRAY_SIZE * 2U; ++value_index) {
      uint64_t eager_value = 0;
      uint64_t lazy_value = 0;

      assert(qst_generator_random_next(&eager, &eager_value) == QST_OK);
      assert(qst_generator_random_next(&lazy, &lazy_value) == QST_OK);
      assert(eager_value == lazy_value);
    }
  }
  for (uint32_t seed_index = 0;
       seed_index < sizeof(seeds) / sizeof(seeds[0]); ++seed_index) {
    const uint64_t seed_limit =
        sequence_limit / (sizeof(seeds) / sizeof(seeds[0])) +
        (seed_index < sequence_limit % (sizeof(seeds) / sizeof(seeds[0])) ? 1 : 0);

    for (uint64_t sequence = 1; sequence <= seed_limit; ++sequence) {
      assert(qst_generator_random_init_lazy(
                 &random_state,
                 seeds[seed_index] ^
                     (shard_index * UINT64_C(0x9e3779b97f4a7c15)), sequence) ==
             QST_OK);
      {
        const EQstCode build_code = qst_generator_random_plan_build_with_runtime(
            &plan, &random_state, QST_GENERATOR_DEFAULT_QUERY_DEPTH, runtime,
            &prepared_runtime, &sql);

        if (build_code != QST_OK) {
          (void)fprintf(stderr,
                        "taosQueryTest: random-only build failed shard=%llu "
                        "seed_index=%u sequence=%llu code=%d\n",
                        (unsigned long long)shard_index, seed_index,
                        (unsigned long long)sequence, build_code);
        }
        if (build_code == QST_E_SQL_LIMIT) {
          ++text_limit_count;
        } else {
          assert(build_code == QST_OK);
        }
      }
      if (sql.choice_count == 0) {
        qst_generator_generated_sql_destroy(&sql);
        continue;
      }
      if (sql.contains_abnormal != 0) {
        ++abnormal_count;
      } else {
        ++normal_count;
      }
      qst_generator_generated_sql_destroy(&sql);
      if (limit_text != NULL && sequence % UINT64_C(1000000) == 0) {
        (void)fprintf(stderr,
                      "taosQueryTest: generator random-only stress seed=%u "
                      "sequence=%llu limit=%llu\n",
                      seed_index, (unsigned long long)sequence,
                      (unsigned long long)seed_limit);
      }
    }
  }
  for (uint32_t fragment_id = 0; fragment_id <= max_fragment_id;
       ++fragment_id) {
    selected_fragment_count += selected_fragments[fragment_id] != 0;
  }
  for (uint32_t value_id = 1; value_id <= max_value_id; ++value_id) {
    selected_value_count += selected_values[value_id] != 0;
  }
  (void)fprintf(stderr,
                "taosQueryTest: generator random-only stress complete "
                "shard=%llu normal=%llu abnormal=%llu selected_fragments=%llu "
                "selected_values=%llu\n",
                (unsigned long long)shard_index,
                (unsigned long long)normal_count,
                (unsigned long long)abnormal_count,
                (unsigned long long)selected_fragment_count,
                (unsigned long long)selected_value_count);
  (void)fprintf(stderr,
                "taosQueryTest: generator random-only text_limit=%llu\n",
                (unsigned long long)text_limit_count);
  (void)fprintf(stderr,
                "taosQueryTest: generator random-only root "
                "normal=%llu abnormal=%llu\n",
                (unsigned long long)prepared_runtime.selection_root_normal_count,
                (unsigned long long)prepared_runtime.selection_root_abnormal_count);
  (void)fprintf(stderr,
                "taosQueryTest: generator random-only profile "
                "runtime_init=%llu plan_walk=%llu plan_select=%llu "
                "candidate_enum=%llu append=%llu\n",
                (unsigned long long)prepared_runtime.profile_runtime_initialize_count,
                (unsigned long long)prepared_runtime.profile_plan_walk_count,
                (unsigned long long)prepared_runtime.profile_plan_select_count,
                (unsigned long long)prepared_runtime.profile_runtime_candidate_enumeration_count,
                (unsigned long long)prepared_runtime.profile_runtime_append_count);
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const uint32_t fragment_id = catalog->fragments[fragment_index].fragment_id;

    if (selected_fragments[fragment_id] == 0) {
      ++unselected_fragment_count;
    }
  }
  for (uint32_t fragment_index = 0; fragment_index < catalog->fragment_count;
       ++fragment_index) {
    const SQstGeneratorFragment *fragment = &catalog->fragments[fragment_index];

    for (uint32_t value_index = 0; value_index < fragment->value_count;
         ++value_index) {
      const uint32_t value_id = fragment->values[value_index].value_id;

      if (selected_values[value_id] == 0) {
        ++unselected_value_count;
      }
    }
  }
  (void)fprintf(stderr,
                "taosQueryTest: generator random-only unselected "
                "fragments=%llu values=%llu\n",
                (unsigned long long)unselected_fragment_count,
                (unsigned long long)unselected_value_count);
  if (sequence_limit >= 100U) {
    assert(normal_count != 0 && abnormal_count != 0);
  }
  qst_generator_frag_plan_destroy(&plan);
  assert(selected_fragment_count != 0 && selected_value_count != 0);
  qst_generator_random_runtime_destroy(&prepared_runtime);
  free(selected_fragments);
  free(selected_values);
}

static void qstTestSqlDimensionSourceDirectory(void) {
  const SQstSqlDimensionSourceFragment *fragments = NULL;
  const SQstSqlDimensionSourceValue *values = NULL;
  uint32_t fragment_count = 0;
  uint32_t value_count = 0;

  assert(qst_sql_dimension_source(&fragments, &fragment_count, &values,
                                  &value_count) == QST_OK);
  assert(fragments != NULL && values != NULL);
  assert(fragment_count == SQL_DIMENSION_FRAG_SOURCE_COUNT);
  assert(value_count == SQL_DIMENSION_VAL_SOURCE_COUNT);
  assert(fragment_count == 535U);
  assert(value_count == 796U);
  for (uint32_t index = 0; index < fragment_count; ++index) {
    assert(fragments[index].fragment_id == index + 1U);
    assert(strncmp(fragments[index].name, "FRAG_", 5) == 0);
    assert(fragments[index].relation[0] != '\0');
  }
  for (uint32_t index = 0; index < value_count; ++index) {
    assert(values[index].value_id == index + 1U);
    assert(strncmp(values[index].name, "VAL_", 4) == 0);
    assert(values[index].text != NULL);
  }
  assert(fragments[SQL_DIMENSION_FRAG_QUERY_ROOT - 1U].fragment_id ==
         SQL_DIMENSION_FRAG_QUERY_ROOT);
  assert(strcmp(fragments[SQL_DIMENSION_FRAG_QUERY_ROOT - 1U].relation,
                "FRAG_QUERY_TOP_LEVEL FRAG_QUERY_TERMINATOR") == 0);
  assert(strcmp(values[SQL_DIMENSION_VAL_CLAUSE_SELECT - 1U].text, "SELECT") ==
         0);
}

static void qstTestSharedGenerationState(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGenerationState *state = NULL;

  config.run_id = UINT64_C(5100000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 64;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_generator_state_get(&mapping, &state) == QST_E_SHM_CORRUPT);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE,
                                        UINT32_C(1) << 1, 123, 0, 0) == QST_OK);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(state->catalog_version == catalog->version);
  assert(memcmp(state->catalog_digest.bytes, catalog->digest.bytes,
                sizeof(catalog->digest.bytes)) == 0);
  assert(atomic_load_explicit(&state->full_target_done_mask,
                              memory_order_acquire) == UINT32_C(0x0d));
  assert(state->full_cursor[0].exhausted == 1);
  assert(state->full_cursor[1].exhausted == 0);
  assert(state->mode == QST_MODE_FULL);
  assert(state->focus == QST_FOCUS_COVERAGE);
  assert(state->random_seed == 123);
  assert(state->pending_quota_bytes == config.pending_quota_bytes);
  assert(state->log_rotation_bytes == config.log_rotation_bytes);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE,
                                        UINT32_C(1) << 1, 123, 0, 0) ==
         QST_E_STALE_OWNER);
  assert(mapping.header->dimension_catalog_version == catalog->version);
  assert(memcmp(mapping.header->dimension_catalog_digest.bytes,
                catalog->digest.bytes, sizeof(catalog->digest.bytes)) == 0);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestRandomRunControlState(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGenerationState *state = NULL;
  SQstTaskEntry *entries = NULL;
  EQstStopReason stop_reason = QST_STOP_NONE;
  EQstRandomPublishGate gate = QST_RANDOM_PUBLISH_GATE_READY;
  int32_t can_publish = 0;

  config.run_id = UINT64_C(5110000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 64;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_RANDOM,
                                        QST_FOCUS_COVERAGE, UINT32_C(1), 91,
                                        5, 0) == QST_OK);
  assert(qst_generator_random_can_publish(&mapping, 100, &can_publish,
                                          &stop_reason, &gate) == QST_OK);
  assert(can_publish == 1 && stop_reason == QST_STOP_NONE);
  assert(gate == QST_RANDOM_PUBLISH_GATE_READY);
  entries = (SQstTaskEntry *)(mapping.base + mapping.header->task_region.region_offset);
  for (uint32_t index = 0; index < mapping.header->task_region.element_count;
       ++index) {
    atomic_store_explicit(&entries[index].phase, QST_TASK_GENERATED,
                          memory_order_release);
  }
  atomic_store_explicit(&mapping.header->free_task_count, 0,
                        memory_order_release);
  assert(qst_generator_random_can_publish(&mapping, 100, &can_publish,
                                          &stop_reason, &gate) == QST_OK);
  assert(can_publish == 0 && stop_reason == QST_STOP_NONE);
  assert(gate == QST_RANDOM_PUBLISH_GATE_TASK_LEDGER_EMPTY);
  atomic_store_explicit(&entries[0].phase, 0, memory_order_release);
  atomic_store_explicit(&mapping.header->free_task_count, 1,
                        memory_order_release);
  assert(qst_generator_random_can_publish(&mapping, 100, &can_publish,
                                          &stop_reason, &gate) == QST_OK);
  assert(can_publish == 1 && stop_reason == QST_STOP_NONE);
  assert(gate == QST_RANDOM_PUBLISH_GATE_READY);
  atomic_store_explicit(&entries[1].phase, 0, memory_order_release);
  atomic_store_explicit(&mapping.header->free_task_count, 2,
                        memory_order_release);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  atomic_store_explicit(&state->random_generation_seq, 5,
                        memory_order_release);
  assert(qst_generator_random_can_publish(&mapping, 100, &can_publish,
                                          &stop_reason, &gate) == QST_OK);
  assert(can_publish == 0 && stop_reason == QST_STOP_NONE);
  assert(gate == QST_RANDOM_PUBLISH_GATE_ACTIVATION_WINDOW_FULL);
  assert(qst_generator_random_activate(&mapping, 1, 100) == QST_OK);
  assert(qst_generator_random_activate(&mapping, 1, 100) == QST_OK);
  assert(qst_generator_random_activate(&mapping, 3, 100) == QST_OK);
  assert(qst_generator_random_activate(&mapping, 2, 100) == QST_OK);
  assert(atomic_load_explicit(&state->random_activated_count,
                              memory_order_acquire) == 3);
  assert(qst_generator_random_can_publish(&mapping, 101, &can_publish,
                                          &stop_reason, &gate) == QST_OK);
  assert(can_publish == 1 && stop_reason == QST_STOP_NONE);
  assert(gate == QST_RANDOM_PUBLISH_GATE_READY);
  atomic_store_explicit(&state->random_generation_seq, 6,
                        memory_order_release);
  assert(qst_generator_random_can_publish(&mapping, 103, &can_publish,
                                          &stop_reason, &gate) == QST_OK);
  assert(can_publish == 0 && stop_reason == QST_STOP_STATEMENT_LIMIT);
  assert(gate == QST_RANDOM_PUBLISH_GATE_STOP);
  assert(qst_shm_unmap(&mapping) == QST_OK);

  config.run_id += UINT64_C(1);
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_RANDOM,
                                        QST_FOCUS_COVERAGE, UINT32_C(1), 92,
                                        0, 1) == QST_OK);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  atomic_store_explicit(&state->random_generation_seq, 2,
                        memory_order_release);
  assert(qst_generator_random_activate(&mapping, 1, 100) == QST_OK);
  assert(qst_generator_random_can_publish(
             &mapping, 100 + UINT64_C(60) * UINT64_C(1000000000) - 1U,
             &can_publish, &stop_reason, &gate) == QST_OK);
  assert(can_publish == 1 && stop_reason == QST_STOP_NONE);
  assert(gate == QST_RANDOM_PUBLISH_GATE_READY);
  assert(qst_generator_random_can_publish(
             &mapping, 100 + UINT64_C(60) * UINT64_C(1000000000),
             &can_publish, &stop_reason, &gate) == QST_OK);
  assert(can_publish == 0 && stop_reason == QST_STOP_DURATION_LIMIT);
  assert(gate == QST_RANDOM_PUBLISH_GATE_STOP);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestFullRunControlState(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  const SQstGeneratorCatalog *catalog = NULL;
  SQstGenerationState *state = NULL;

  config.run_id = UINT64_C(5120000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 64;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE,
                                        UINT32_C(0x05), 0, 0, 1) == QST_OK);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  {
    int32_t duration_expired = -1;

    assert(qst_generator_full_duration_expired(0, 0, &duration_expired) ==
           QST_OK);
    assert(duration_expired == 0);
    assert(qst_generator_full_duration_expired(100, 99, &duration_expired) ==
           QST_OK);
    assert(duration_expired == 0);
    assert(qst_generator_full_duration_expired(100, 100, &duration_expired) ==
           QST_OK);
    assert(duration_expired == 1);
    assert(qst_generator_full_duration_expired(100, 0, &duration_expired) ==
           QST_E_INVALID_ARGUMENT);
  }
  assert(qst_generator_full_complete_round(&mapping, 1) == QST_E_SHM_CORRUPT);
  state->full_cursor[0].next_path_ordinal = 17;
  state->full_cursor[0].exhausted = 1;
  state->full_cursor[2].next_path_ordinal = 23;
  state->full_cursor[2].exhausted = 1;
  atomic_store_explicit(&state->full_target_done_mask, UINT32_C(0x0f),
                        memory_order_release);
  atomic_store_explicit(&state->full_target_index, QST_GENERATOR_TARGET_COUNT,
                        memory_order_release);
  assert(qst_generator_full_complete_round(&mapping, 1) == QST_OK);
  assert(atomic_load_explicit(&state->full_coverage_round_count,
                              memory_order_acquire) == 1);
  assert(atomic_load_explicit(&state->full_target_done_mask,
                              memory_order_acquire) == UINT32_C(0x0a));
  assert(atomic_load_explicit(&state->full_target_index,
                              memory_order_acquire) == 0);
  assert(state->full_cursor[0].next_path_ordinal == 0 &&
         state->full_cursor[0].exhausted == 0);
  assert(state->full_cursor[2].next_path_ordinal == 0 &&
         state->full_cursor[2].exhausted == 0);
  assert(state->full_cursor[1].exhausted == 1 &&
         state->full_cursor[3].exhausted == 1);
  state->full_cursor[0].exhausted = 1;
  state->full_cursor[2].exhausted = 1;
  atomic_store_explicit(&state->full_target_done_mask, UINT32_C(0x0f),
                        memory_order_release);
  assert(qst_generator_full_complete_round(&mapping, 0) == QST_OK);
  assert(atomic_load_explicit(&state->full_coverage_round_count,
                              memory_order_acquire) == 2);
  assert(state->full_cursor[0].exhausted == 1 &&
         state->full_cursor[2].exhausted == 1);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestGeneratorFullRecoveryDoesNotLosePaths(void) {
  enum { kExpectedPaths = 8 };
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorCursor cursor = {0};
  SQstGeneratorCursor before[kExpectedPaths] = {{0}};
  SQstGeneratorCursor after[kExpectedPaths] = {{0}};
  SQstGeneratedSql baseline[kExpectedPaths] = {{0}};
  uint32_t baseline_count = 0;

  while (baseline_count < kExpectedPaths) {
    before[baseline_count] = cursor;
    assert(qst_generator_full_next(&catalog, &cursor, 1,
                                   &baseline[baseline_count]) == QST_OK);
    after[baseline_count] = cursor;
    ++baseline_count;
  }
  assert(cursor.exhausted == 1);
  assert(qst_generator_full_next(&catalog, &cursor, 1, &baseline[0]) ==
         QST_E_QUEUE_EMPTY);
  for (uint32_t crash_point = 0; crash_point < baseline_count; ++crash_point) {
    SQstGeneratorCursor restored = before[crash_point];
    SQstGeneratedSql rebuilt = {0};

    assert(qst_generator_full_next(&catalog, &restored, 1, &rebuilt) == QST_OK);
    assert(rebuilt.sql_length == baseline[crash_point].sql_length);
    assert(memcmp(rebuilt.sql_sha256.bytes, baseline[crash_point].sql_sha256.bytes,
                  sizeof(rebuilt.sql_sha256.bytes)) == 0);
    restored = after[crash_point];
    for (uint32_t next = crash_point + 1; next < baseline_count; ++next) {
      assert(qst_generator_full_next(&catalog, &restored, 1, &rebuilt) == QST_OK);
      assert(memcmp(rebuilt.sql_sha256.bytes, baseline[next].sql_sha256.bytes,
                    sizeof(rebuilt.sql_sha256.bytes)) == 0);
    }
    assert(restored.exhausted == 1);
    qst_generator_generated_sql_destroy(&rebuilt);
  }
  for (uint32_t index = 0; index < baseline_count; ++index) {
    qst_generator_generated_sql_destroy(&baseline[index]);
  }
}

static void qstTestGeneratorRandomDeterminismAndWrap(void) {
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorRandomState left = {0};
  SQstGeneratorRandomState right = {0};
  SQstGeneratedSql left_sql = {0};
  SQstGeneratedSql right_sql = {0};
  uint64_t left_values[QST_GENERATOR_RANDOM_ARRAY_SIZE] = {0};
  uint64_t right_values[QST_GENERATOR_RANDOM_ARRAY_SIZE] = {0};

  assert(qst_generator_random_init(&left, 12345, 77) == QST_OK);
  assert(qst_generator_random_init(&right, 12345, 77) == QST_OK);
  assert(qst_generator_random_build(&catalog, &left, 1, &left_sql) == QST_OK);
  assert(qst_generator_random_build(&catalog, &right, 1, &right_sql) == QST_OK);
  assert(left_sql.sql_length == right_sql.sql_length);
  assert(memcmp(left_sql.sql_sha256.bytes, right_sql.sql_sha256.bytes,
                sizeof(left_sql.sql_sha256.bytes)) == 0);
  assert(qst_generator_random_init(&left, 12345, 77) == QST_OK);
  assert(qst_generator_random_init(&right, 12345, 77) == QST_OK);
  for (uint32_t index = 0; index < QST_GENERATOR_RANDOM_ARRAY_SIZE; ++index) {
    assert(qst_generator_random_next(&left, &left_values[index]) == QST_OK);
    assert(qst_generator_random_next(&right, &right_values[index]) == QST_OK);
  }
  assert(memcmp(left_values, right_values, sizeof(left_values)) == 0);
  assert(left.random_index == 0);
  assert(left.loop_round == 1);
  assert(qst_generator_random_next(&left, &left_values[0]) == QST_OK);
  assert(qst_generator_random_next(&right, &right_values[0]) == QST_OK);
  assert(left_values[0] == right_values[0]);
  qst_generator_generated_sql_destroy(&left_sql);
  qst_generator_generated_sql_destroy(&right_sql);
}

static void qstTestGeneratorPublishCompensatesQueueFailure(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog catalog = qstTestGeneratorCatalog();
  SQstGeneratorCursor cursor = {0};
  SQstGeneratedSql sql = {0};
  SQstGeneratedSql rebuilt = {0};
  SQstGeneratorPublishInput input = {0};
  SQstTaskEntry *entries = NULL;
  SQstRef first_sql_ref = {0};
  SQstRef first_generation_record_ref = {0};
  SQstGenerationRecordHeader record_header = {0};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint64_t first_gid = 0;
  uint64_t second_gid = 0;
  uint64_t third_gid = 0;
  uint32_t first_index = 0;
  uint32_t second_index = 0;

  config.run_id = UINT64_C(5000000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 4;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 2;
  config.block_bytes = 128;
  /* Leave the generator's downstream reserve intact while exercising queue
   * compensation after two successful publications. */
  config.block_count = 64;
  config.execution_pair_count = 1;
  config.judge_worker_count = 1;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              501, 501, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 502, 502,
                              &generator_generation) == QST_OK);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &sql) == QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_FULL;
  input.focus = QST_FOCUS_COVERAGE;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 1;
  input.weight_profile_version = 1;
  input.catalog_version = catalog.version;
  input.catalog_digest = catalog.digest;
  assert(qst_generator_publish(&mapping, QST_ROLE_GENERATOR,
                               generator_generation, &input, &sql,
                               UINT64_MAX, &first_gid) == QST_OK);
  assert(first_gid != 0);
  assert(qst_generator_publish(&mapping, QST_ROLE_GENERATOR,
                               generator_generation, &input, &sql,
                               UINT64_MAX, &second_gid) == QST_OK);
  assert(second_gid != 0);
  assert(qst_generator_publish(&mapping, QST_ROLE_GENERATOR,
                               generator_generation, &input, &sql,
                               UINT64_MAX, &third_gid) == QST_E_QUEUE_FULL);
  assert(third_gid == 0);
  assert(qst_generator_full_peek(&catalog, &cursor, 0, &rebuilt) == QST_OK);
  assert(memcmp(sql.sql_sha256.bytes, rebuilt.sql_sha256.bytes,
                sizeof(sql.sql_sha256.bytes)) == 0);
  entries = (SQstTaskEntry *)((uint8_t *)mapping.base +
                              mapping.header->task_region.region_offset);
  first_index = (uint32_t)(first_gid % config.task_entry_count);
  second_index = (uint32_t)(second_gid % config.task_entry_count);
  assert(first_index != second_index);
  assert(atomic_load_explicit(&entries[first_index].phase, memory_order_acquire) ==
         QST_TASK_GENERATED);
  assert(atomic_load_explicit(&entries[second_index].phase, memory_order_acquire) ==
         QST_TASK_GENERATED);
  first_sql_ref = entries[first_index].sql_ref;
  first_generation_record_ref = entries[first_index].generation_record_ref;
  assert(qst_block_copy(&mapping, first_generation_record_ref, first_gid, 0,
                        &record_header, sizeof(record_header)) == QST_OK);
  assert(record_header.format_version == QST_GENERATION_RECORD_VERSION);
  assert(first_generation_record_ref.length ==
         sizeof(record_header) + sql.sql_length);
  assert(memcmp(record_header.catalog_digest.bytes, catalog.digest.bytes,
                sizeof(catalog.digest.bytes)) == 0);
  for (uint32_t index = 0; index < config.task_entry_count; ++index) {
    if (index == first_index || index == second_index) {
      continue;
    }
    assert(atomic_load_explicit(&entries[index].phase, memory_order_acquire) == 0);
  }
  assert(qst_task_abort_generated(&mapping, first_gid, QST_ROLE_GENERATOR,
                                  generator_generation) == QST_OK);
  assert(qst_block_release(&mapping, first_generation_record_ref, first_gid) ==
         QST_OK);
  assert(qst_block_release(&mapping, first_sql_ref, first_gid) == QST_OK);
  first_sql_ref = entries[second_index].sql_ref;
  first_generation_record_ref = entries[second_index].generation_record_ref;
  assert(qst_task_abort_generated(&mapping, second_gid, QST_ROLE_GENERATOR,
                                  generator_generation) == QST_OK);
  assert(qst_block_release(&mapping, first_generation_record_ref, second_gid) ==
         QST_OK);
  assert(qst_block_release(&mapping, first_sql_ref, second_gid) == QST_OK);
  qst_generator_generated_sql_destroy(&rebuilt);
  qst_generator_generated_sql_destroy(&sql);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestSharedFullPublishCursorCommit(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog local_catalog = qstTestGeneratorCatalog();
  const SQstGeneratorCatalog *catalog = &local_catalog;
  SQstGenerationState *state = NULL;
  SQstTaskEntry *entries = NULL;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t published_count = 0;
  uint64_t gid = 0;
  EQstCode code = QST_OK;

  config.run_id = UINT64_C(5200000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 32;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 32;
  config.block_bytes = 256;
  config.block_count = 256;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              520, 520, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 521, 521,
                              &generator_generation) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE, 1, 17, 0, 0) == QST_OK);
  while (published_count < config.task_entry_count) {
    code = qst_generator_full_publish_next(
        &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
        (SQstDigest256){{0}}, 1, UINT64_MAX, &gid);
    if (code == QST_E_QUEUE_EMPTY) {
      break;
    }
    assert(code == QST_OK);
    assert(gid == published_count + 1U);
    ++published_count;
  }
  assert(code == QST_E_QUEUE_EMPTY);
  assert(published_count != 0);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(atomic_load_explicit(&state->publish_seq, memory_order_acquire) ==
         published_count);
  assert((atomic_load_explicit(&state->full_target_done_mask,
                               memory_order_acquire) & 1U) != 0);
  assert(state->full_cursor[0].exhausted == 1);
  entries = (SQstTaskEntry *)((uint8_t *)mapping.base +
                              mapping.header->task_region.region_offset);
  for (uint32_t index = 0; index < published_count; ++index) {
    const uint64_t expected_gid = index + 1U;
    const uint32_t slot = (uint32_t)(expected_gid % config.task_entry_count);

    assert(entries[slot].gid == expected_gid);
    assert(entries[slot].target == QST_TARGET_TD);
    assert(entries[slot].generation_seq == expected_gid);
    assert(atomic_load_explicit(&entries[slot].phase, memory_order_acquire) ==
           QST_TASK_GENERATED);
  }
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestSharedFullPublishRoundRobinTargets(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog local_catalog = qstTestGeneratorCatalog();
  const SQstGeneratorCatalog *catalog = &local_catalog;
  SQstGenerationState *state = NULL;
  SQstGeneratorFullPending pending_by_target[QST_GENERATOR_TARGET_COUNT] = {{0}};
  void *target_lookup_cache[QST_GENERATOR_TARGET_COUNT] = {0};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint64_t gid = 0;

  config.run_id = UINT64_C(5250000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 8;
  config.block_bytes = 256;
  config.block_count = 128;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              525, 525, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 526, 526,
                              &generator_generation) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE, UINT32_C(0x03),
                                        17, 0, 0) == QST_OK);
  for (uint32_t index = 0; index < 8; ++index) {
    assert(qst_generator_full_publish_pending_by_target(
               &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
               (SQstDigest256){{0}}, 1, UINT64_MAX, pending_by_target,
               &gid) == QST_OK);
    assert(gid == index + 1U);
    if (index < 2U) {
      target_lookup_cache[index] =
          ((SQstGeneratorRandomRuntime *)pending_by_target[index].build_runtime)
              ->buildable_lookup_cache;
      assert(target_lookup_cache[index] != NULL);
    }
  }
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(state->full_cursor[0].next_path_ordinal == 4);
  assert(state->full_cursor[1].next_path_ordinal == 4);
  assert(atomic_load_explicit(&state->full_target_index,
                              memory_order_acquire) == 0);
  assert(pending_by_target[0].full_path_restore_count == 1);
  assert(pending_by_target[1].full_path_restore_count == 1);
  assert(pending_by_target[0].build_runtime != NULL);
  assert(pending_by_target[1].build_runtime != NULL);
  assert(pending_by_target[0].build_runtime != pending_by_target[1].build_runtime);
  assert(((SQstGeneratorRandomRuntime *)pending_by_target[0].build_runtime)
             ->buildable_lookup_cache == target_lookup_cache[0]);
  assert(((SQstGeneratorRandomRuntime *)pending_by_target[1].build_runtime)
             ->buildable_lookup_cache == target_lookup_cache[1]);
  assert(target_lookup_cache[0] != target_lookup_cache[1]);
  assert(pending_by_target[0].full_path_ready == 1);
  assert(pending_by_target[1].full_path_ready == 1);
  assert(pending_by_target[0].full_path_next_ordinal ==
         state->full_cursor[0].next_path_ordinal);
  assert(pending_by_target[1].full_path_next_ordinal ==
         state->full_cursor[1].next_path_ordinal);
  for (uint64_t sequence = 1; sequence <= 8; ++sequence) {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, sequence);

    assert(entry != NULL);
    assert(entry->generation_seq == sequence);
    assert(entry->target ==
           (sequence % 2U != 0 ? QST_TARGET_TD : QST_TARGET_MYSQL));
  }
  for (uint32_t target_index = 0;
       target_index < QST_GENERATOR_TARGET_COUNT; ++target_index) {
    qst_generator_full_pending_destroy(&pending_by_target[target_index]);
  }
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestSharedFullPreparationKeepsPublicationOrder(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog local_catalog = qstTestGeneratorCatalog();
  const SQstGeneratorCatalog *catalog = &local_catalog;
  SQstGenerationState *state = NULL;
  SQstGeneratorFullPending pending[QST_GENERATOR_TARGET_COUNT] = {{0}};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint64_t gid = 0;

  config.run_id = UINT64_C(5270000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 8;
  config.block_bytes = 256;
  config.block_count = 128;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              527, 527, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 528, 528,
                              &generator_generation) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE, UINT32_C(0x03),
                                        23, 0, 0) == QST_OK);
  assert(qst_generator_full_prepare_pending_for_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 0, &pending[0]) == QST_OK);
  assert(qst_generator_full_prepare_pending_for_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, &pending[1]) == QST_OK);
  assert(pending[0].valid == 1 && pending[1].valid == 1);
  assert(pending[0].sql.occurrence_count == 0 &&
         pending[0].sql.occurrences == NULL);
  assert(pending[1].sql.occurrence_count == 0 &&
         pending[1].sql.occurrences == NULL);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(state->full_cursor[0].next_path_ordinal == 0);
  assert(state->full_cursor[1].next_path_ordinal == 0);
  assert(atomic_load_explicit(&state->publish_seq, memory_order_acquire) == 0);
  assert(qst_generator_full_publish_pending_by_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, pending, &gid) == QST_OK);
  assert(gid == 1 && pending[0].valid == 0 && pending[1].valid == 1);
  assert(qst_generator_full_publish_pending_by_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, pending, &gid) == QST_OK);
  assert(gid == 2 && pending[1].valid == 0);
  assert(state->full_cursor[0].next_path_ordinal == 1);
  assert(state->full_cursor[1].next_path_ordinal == 1);
  {
    SQstTaskEntry *first = qstTestFindTaskByGid(&mapping, 1);
    SQstTaskEntry *second = qstTestFindTaskByGid(&mapping, 2);

    assert(first != NULL && second != NULL);
    assert(first->generation_seq == 1 && first->target == QST_TARGET_TD);
    assert(second->generation_seq == 2 && second->target == QST_TARGET_MYSQL);
  }
  for (uint32_t target_index = 0;
       target_index < QST_GENERATOR_TARGET_COUNT; ++target_index) {
    qst_generator_full_pending_destroy(&pending[target_index]);
  }
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestSharedFullPublishQueueFailureKeepsCursor(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog local_catalog = qstTestGeneratorCatalog();
  const SQstGeneratorCatalog *catalog = &local_catalog;
  SQstGenerationState *state = NULL;
  SQstGeneratedSql expected = {0};
  SQstGeneratedSql rebuilt = {0};
  SQstGeneratorFullPending pending_by_target[QST_GENERATOR_TARGET_COUNT] = {{0}};
  SQstClaim claim = {0};
  SQstTaskEntry *entries = NULL;
  SQstTaskEntry *first_free_entry = NULL;
  SQstTaskEntry *second_free_entry = NULL;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  uint64_t gid = 0;

  config.run_id = UINT64_C(5300000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 2;
  config.block_bytes = 256;
  config.block_count = 64;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              530, 530, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 531, 531,
                              &generator_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, 532, 532,
                              &exec_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 0, 533, 533,
             &pair_generation) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE, 1, 19, 0, 0) == QST_OK);
  assert(qst_generator_full_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &gid) == QST_OK);
  assert(qst_generator_full_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &gid) == QST_OK);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(qst_generator_full_peek(catalog, &state->full_cursor[0],
                                 QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                 &expected) == QST_OK);
  entries = (SQstTaskEntry *)((uint8_t *)mapping.base +
                              mapping.header->task_region.region_offset);
  for (uint32_t index = 0; index < config.task_entry_count; ++index) {
    if (atomic_load_explicit(&entries[index].phase, memory_order_acquire) == 0) {
      if (NULL == first_free_entry) {
        first_free_entry = &entries[index];
      } else if (NULL == second_free_entry) {
        second_free_entry = &entries[index];
      }
      atomic_store_explicit(&entries[index].phase, QST_TASK_GENERATED,
                            memory_order_release);
    }
  }
  assert(first_free_entry != NULL && second_free_entry != NULL);
  atomic_store_explicit(&mapping.header->free_task_count, 0,
                        memory_order_release);
  assert(qst_generator_full_publish_pending_by_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, pending_by_target, &gid) ==
         QST_E_QUEUE_FULL);
  assert(gid == 0);
  assert(pending_by_target[0].valid == 1);
  assert(pending_by_target[0].build_runtime != NULL);
  assert(pending_by_target[0].cursor_next_path_ordinal ==
         state->full_cursor[0].next_path_ordinal);
  assert(memcmp(pending_by_target[0].sql.sql_sha256.bytes,
                expected.sql_sha256.bytes,
                sizeof(expected.sql_sha256.bytes)) == 0);
  assert(atomic_load_explicit(&state->publish_seq, memory_order_acquire) == 2);
  assert(qst_generator_full_peek(catalog, &state->full_cursor[0],
                                 QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                 &rebuilt) == QST_OK);
  assert(memcmp(expected.sql_sha256.bytes, rebuilt.sql_sha256.bytes,
                sizeof(expected.sql_sha256.bytes)) == 0);
  assert(qst_queue_claim_instance(
             &mapping, QST_QUEUE_GENERATE, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, UINT64_MAX, &claim) == QST_OK);
  assert(qst_queue_ack(&mapping, &claim, QST_ACK_TASK_DISPATCHED, 1) == QST_OK);
  atomic_store_explicit(&first_free_entry->phase, 0, memory_order_release);
  atomic_store_explicit(&mapping.header->free_task_count, 1,
                        memory_order_release);
  assert(qst_generator_full_publish_pending_by_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, pending_by_target,
             &gid) == QST_OK);
  assert(pending_by_target[0].valid == 0);
  assert(pending_by_target[0].full_path_ready == 1);
  assert(pending_by_target[0].full_path_next_ordinal ==
         state->full_cursor[0].next_path_ordinal);
  assert(gid == 3);
  {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, gid);

    assert(entry != NULL);
    assert(memcmp(entry->sql_sha256.bytes, expected.sql_sha256.bytes,
                  sizeof(expected.sql_sha256.bytes)) == 0);
  }
  qst_generator_generated_sql_destroy(&expected);
  assert(qst_generator_full_peek(catalog, &state->full_cursor[0],
                                 QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                 &expected) == QST_OK);
  assert(qst_queue_claim_instance(
             &mapping, QST_QUEUE_GENERATE, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, UINT64_MAX, &claim) == QST_OK);
  assert(qst_queue_ack(&mapping, &claim, QST_ACK_TASK_DISPATCHED, 1) == QST_OK);
  atomic_store_explicit(&second_free_entry->phase, 0, memory_order_release);
  atomic_store_explicit(&mapping.header->free_task_count, 1,
                        memory_order_release);
  assert(qst_generator_full_publish_pending_by_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, pending_by_target,
             &gid) == QST_OK);
  assert(gid == 4);
  {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, gid);

    assert(entry != NULL);
    assert(memcmp(entry->sql_sha256.bytes, expected.sql_sha256.bytes,
                  sizeof(expected.sql_sha256.bytes)) == 0);
  }
  assert(pending_by_target[0].full_path_restore_count == 1);
  for (uint32_t target_index = 0;
       target_index < QST_GENERATOR_TARGET_COUNT; ++target_index) {
    qst_generator_full_pending_destroy(&pending_by_target[target_index]);
  }
  qst_generator_generated_sql_destroy(&expected);
  qst_generator_generated_sql_destroy(&rebuilt);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestSharedFullLookAheadKeepsOrderAcrossQueueFailure(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog local_catalog = qstTestGeneratorCatalog();
  const SQstGeneratorCatalog *catalog = &local_catalog;
  SQstGenerationState *state = NULL;
  SQstGeneratorCursor second_cursor = {0};
  SQstGeneratedSql expected_first = {0};
  SQstGeneratedSql expected_second = {0};
  SQstGeneratorFullPending pending[2] = {{0}};
  SQstGeneratorFullPending delayed_pending = {0};
  SQstGeneratorFullPathSeed successor = {0};
  SQstTaskEntry *entries = NULL;
  SQstTaskEntry *first_free_entry = NULL;
  SQstTaskEntry *second_free_entry = NULL;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  uint64_t gid = 0;

  config.run_id = UINT64_C(5310000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 128;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              531, 531, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 532, 532,
                              &generator_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, 533, 533,
                              &exec_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 0, 534, 534,
             &pair_generation) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_FULL,
                                        QST_FOCUS_COVERAGE, 1, 29, 0, 0) == QST_OK);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(qst_generator_full_peek(catalog, &state->full_cursor[0],
                                 QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                 &expected_first) == QST_OK);
  second_cursor = state->full_cursor[0];
  ++second_cursor.next_path_ordinal;
  assert(qst_generator_full_peek(catalog, &second_cursor,
                                 QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                 &expected_second) == QST_OK);
  assert(qst_generator_full_prepare_pending_for_target(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 0, &pending[0]) == QST_OK);
  assert(memcmp(pending[0].sql.sql_sha256.bytes, expected_first.sql_sha256.bytes,
                sizeof(expected_first.sql_sha256.bytes)) == 0);
  assert(qst_generator_full_pending_capture_successor(&pending[0],
                                                       &successor) == QST_OK);
  assert(successor.next_path_ordinal == 1 && successor.exhausted == 0);
  assert(qst_generator_full_prepare_pending_from_seed(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, &successor, &pending[1]) == QST_OK);
  assert(pending[1].cursor_next_path_ordinal == 1);
  assert(memcmp(pending[1].sql.sql_sha256.bytes,
                expected_second.sql_sha256.bytes,
                sizeof(expected_second.sql_sha256.bytes)) == 0);
  entries = (SQstTaskEntry *)((uint8_t *)mapping.base +
                              mapping.header->task_region.region_offset);
  for (uint32_t index = 0; index < config.task_entry_count; ++index) {
    if (NULL == first_free_entry) {
      first_free_entry = &entries[index];
    } else if (NULL == second_free_entry) {
      second_free_entry = &entries[index];
    }
    atomic_store_explicit(&entries[index].phase, QST_TASK_GENERATED,
                          memory_order_release);
  }
  assert(first_free_entry != NULL && second_free_entry != NULL);
  atomic_store_explicit(&mapping.header->free_task_count, 0,
                        memory_order_release);
  assert(qst_generator_full_publish_pending(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &pending[0], &gid) ==
         QST_E_QUEUE_FULL);
  assert(gid == 0 && pending[0].valid == 1 && pending[1].valid == 1);
  assert(state->full_cursor[0].next_path_ordinal == 0);
  assert(atomic_load_explicit(&state->publish_seq, memory_order_acquire) == 0);
  atomic_store_explicit(&first_free_entry->phase, 0, memory_order_release);
  atomic_store_explicit(&mapping.header->free_task_count, 1,
                        memory_order_release);
  assert(qst_generator_full_publish_pending(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &pending[0], &gid) == QST_OK);
  assert(gid == 1 && pending[0].valid == 0 && pending[1].valid == 1);
  assert(state->full_cursor[0].next_path_ordinal == 1);
  {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, gid);

    assert(entry != NULL);
    assert(memcmp(entry->sql_sha256.bytes, expected_first.sql_sha256.bytes,
                  sizeof(expected_first.sql_sha256.bytes)) == 0);
  }
  /* The builder may not run until the publisher has committed the preceding
   * path. The same seed must remain valid when it is no longer one step ahead. */
  assert(qst_generator_full_prepare_pending_from_seed(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, &successor, &delayed_pending) == QST_OK);
  assert(delayed_pending.cursor_next_path_ordinal == 1);
  assert(memcmp(delayed_pending.sql.sql_sha256.bytes,
                expected_second.sql_sha256.bytes,
                sizeof(expected_second.sql_sha256.bytes)) == 0);
  qst_generator_full_pending_destroy(&delayed_pending);
  atomic_store_explicit(&second_free_entry->phase, 0, memory_order_release);
  atomic_store_explicit(&mapping.header->free_task_count, 1,
                        memory_order_release);
  assert(qst_generator_full_publish_pending(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &pending[1], &gid) == QST_OK);
  assert(gid == 2 && pending[1].valid == 0);
  assert(state->full_cursor[0].next_path_ordinal == 2);
  assert(atomic_load_explicit(&state->publish_seq, memory_order_acquire) == 2);
  {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, gid);

    assert(entry != NULL);
    assert(memcmp(entry->sql_sha256.bytes, expected_second.sql_sha256.bytes,
                  sizeof(expected_second.sql_sha256.bytes)) == 0);
  }
  qst_generator_full_path_seed_destroy(&successor);
  qst_generator_full_pending_destroy(&pending[0]);
  qst_generator_full_pending_destroy(&pending[1]);
  qst_generator_generated_sql_destroy(&expected_first);
  qst_generator_generated_sql_destroy(&expected_second);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestSharedRandomPublishTransaction(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratorCatalog local_catalog = qstTestGeneratorCatalog();
  const SQstGeneratorCatalog *catalog = &local_catalog;
  SQstGenerationState *state = NULL;
  SQstGeneratorRandomState expected_state = {0};
  SQstGeneratedSql expected_sql = {0};
  SQstGeneratorRandomRuntime random_runtime = {0};
  SQstClaim claim = {0};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  uint64_t gid = 0;

  config.run_id = UINT64_C(5400000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 4;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 2;
  config.block_bytes = 256;
  config.block_count = 64;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              540, 540, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 541, 541,
                              &generator_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, 542, 542,
                              &exec_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 0, 543, 543,
             &pair_generation) == QST_OK);
  assert(QST_GENERATOR_TARGET_WEIGHT_MYSQL +
             QST_GENERATOR_TARGET_WEIGHT_POSTGRESQL +
             QST_GENERATOR_TARGET_WEIGHT_INFLUX >=
         QST_GENERATOR_TARGET_WEIGHT_TD * 3U);
  assert(qst_generator_state_initialize(&mapping, catalog, QST_MODE_RANDOM,
                                        QST_FOCUS_COVERAGE, UINT32_C(1),
                                        23, 0, 0) == QST_OK);
  assert(qst_generator_random_init(&expected_state, 23, 1) == QST_OK);
  assert(qst_generator_random_build(catalog, &expected_state,
                                    QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                    &expected_sql) == QST_OK);
  assert(qst_generator_random_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &random_runtime, &gid) ==
         QST_OK);
  assert(gid == 1);
  assert(qst_generator_state_get(&mapping, &state) == QST_OK);
  assert(atomic_load_explicit(&state->random_generation_seq,
                              memory_order_acquire) == 2);
  {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, gid);

    assert(entry != NULL);
    assert(entry->target == QST_TARGET_TD);
    assert(entry->generation_seq == 1);
    assert(memcmp(entry->sql_sha256.bytes, expected_sql.sql_sha256.bytes,
                  sizeof(expected_sql.sql_sha256.bytes)) == 0);
  }
  assert(qst_generator_random_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &random_runtime, &gid) ==
         QST_OK);
  assert(gid == 2);
  assert(qst_generator_random_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &random_runtime, &gid) ==
         QST_E_QUEUE_FULL);
  assert(gid == 0);
  assert(atomic_load_explicit(&state->random_generation_seq,
                              memory_order_acquire) == 3);
  assert(qst_generator_random_init(&expected_state, 23, 3) == QST_OK);
  assert(qst_generator_random_build(catalog, &expected_state,
                                    QST_GENERATOR_DEFAULT_QUERY_DEPTH,
                                    &expected_sql) == QST_OK);
  assert(qst_queue_claim_instance(
             &mapping, QST_QUEUE_GENERATE, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, UINT64_MAX, &claim) == QST_OK);
  assert(qst_queue_ack(&mapping, &claim, QST_ACK_TASK_DISPATCHED,
                       claim.message.gid) == QST_OK);
  assert(qst_generator_random_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &random_runtime, &gid) ==
         QST_OK);
  assert(gid == 4);
  assert(atomic_load_explicit(&state->random_generation_seq,
                              memory_order_acquire) == 4);
  {
    SQstTaskEntry *entry = qstTestFindTaskByGid(&mapping, gid);

    assert(entry != NULL);
    assert(entry->generation_seq == 3);
    assert(memcmp(entry->sql_sha256.bytes, expected_sql.sql_sha256.bytes,
                  sizeof(expected_sql.sql_sha256.bytes)) == 0);
  }
  atomic_store_explicit(&state->random_generation_seq, UINT64_MAX,
                        memory_order_release);
  assert(qst_generator_random_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             (SQstDigest256){{0}}, 1, UINT64_MAX, &random_runtime, &gid) ==
         QST_E_RESOURCE);
  assert(gid == 0);
  qst_generator_generated_sql_destroy(&expected_sql);
  qst_generator_random_runtime_destroy(&random_runtime);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestDatasetPrepareAndCleanup(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstDatasetInfo dataset = {0};
  SQstGenerationState *generation_state = NULL;
  const SQstGeneratorCatalog *catalog = NULL;
  SQstDigest256 dataset_digest = {0};
  SQstDigest256 fixture_digest = {0};
  SQstRef directory_ref = {0};
  SQstRef fixture_ref = {0};
  SQstRef object_ref = {0};
  SQstRef coverage_sql_ref = {0};
  SQstRef coverage_record_ref = {0};
  SQstTaskEntry *coverage_entry = NULL;
  SQstObjectDescriptor table = {0};
  SQstObjectDescriptor column = {0};
  char object_name[128] = {0};
  char coverage_sql[256] = {0};
  char sql_log[] = "/tmp/taosQueryTest-dataset-sql-XXXXXX";
  char sql_contents[32768] = {0};
  EQstCode code = QST_OK;
  int32_t sql_fd = -1;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint64_t coverage_gid = 0;
  FILE *file = NULL;

  config.run_id = UINT64_C(4000000000) + (uint64_t)getpid();
  config.mode = QST_MODE_FULL;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = UINT32_C(1);
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 8;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 2;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 256;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, config.mode,
                                        config.focus,
                                        config.target_selection_bits,
                                        config.random_seed,
                                        config.statement_limit,
                                        config.duration_limit_minutes) == QST_OK);
  assert(qst_generator_state_get_dataset(
             &mapping, &dataset_digest, &fixture_digest, &directory_ref,
             &fixture_ref) == QST_E_SHM_CORRUPT);
  sql_fd = mkstemp(sql_log);
  assert(sql_fd >= 0 && close(sql_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_log, 1) == 0);
  assert(setenv("QST_FAKE_TAOS_VERIFY_FIXTURE", "1", 1) == 0);
  code = qst_dataset_prepare(&mapping, &config, &dataset);
  assert(code == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_VERIFY_FIXTURE") == 0);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  file = fopen(sql_log, "rb");
  assert(file != NULL &&
         fread(sql_contents, 1, sizeof(sql_contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(sql_contents,
                "'a','x','text','01','POINT(0 0)',0.00,0.0000000000,") != NULL);
  assert(strstr(sql_contents, "'tag','bin')") != NULL);
  assert(unlink(sql_log) == 0);
  assert(dataset.run_id == config.run_id);
  assert(qst_generator_state_publish_dataset(
             &mapping, main_generation + 1, dataset.ds_fingerprint,
             dataset.fixture_manifest_digest, dataset.object_directory_ref,
             dataset.fixture_manifest_ref) == QST_E_STALE_OWNER);
  assert(qst_generator_state_publish_dataset(
             &mapping, main_generation, dataset.ds_fingerprint,
             dataset.fixture_manifest_digest, dataset.object_directory_ref,
             dataset.fixture_manifest_ref) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, (int32_t)getpid(),
                              (int32_t)getpgrp(), &generator_generation) == QST_OK);
  assert(qst_generator_full_publish_coverage_seed(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             dataset.ds_fingerprint, 0, 1, UINT64_MAX, &coverage_gid) == QST_OK);
  assert(coverage_gid == 1);
  assert(qst_generator_state_get(&mapping, &generation_state) == QST_OK);
  assert(atomic_load_explicit(&generation_state->full_coverage_seed_next[0],
                              memory_order_acquire) ==
         QST_GENERATOR_FULL_COVERAGE_SEED_COUNT);
  assert(generation_state->full_cursor[0].next_path_ordinal == 0);
  assert(qst_generator_full_publish_coverage_seed(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             dataset.ds_fingerprint, 0, 1, UINT64_MAX, &coverage_gid) ==
         QST_E_QUEUE_EMPTY);
  coverage_entry = qstTestFindTaskByGid(&mapping, 1);
  assert(coverage_entry != NULL && coverage_entry->sql_ref.length < sizeof(coverage_sql));
  assert(qst_block_copy(&mapping, coverage_entry->sql_ref, 1, 0, coverage_sql,
                        coverage_entry->sql_ref.length) == QST_OK);
  assert(strstr(coverage_sql, "SELECT COUNT(*) AS qst_full_coverage_count FROM ") !=
         NULL);
  assert(strstr(coverage_sql, " WHERE 1 = 1;") != NULL);
  coverage_sql_ref = coverage_entry->sql_ref;
  coverage_record_ref = coverage_entry->generation_record_ref;
  assert(qst_task_abort_generated(&mapping, 1, QST_ROLE_GENERATOR,
                                  generator_generation) == QST_OK);
  assert(qst_block_release(&mapping, coverage_record_ref, 1) == QST_OK);
  assert(qst_block_release(&mapping, coverage_sql_ref, 1) == QST_OK);
  assert(qst_generator_full_publish_next(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             dataset.ds_fingerprint, 1, UINT64_MAX, &coverage_gid) == QST_OK);
  assert(coverage_gid == 2);
  coverage_entry = qstTestFindTaskByGid(&mapping, coverage_gid);
  assert(coverage_entry != NULL &&
         coverage_entry->sql_ref.length < sizeof(coverage_sql));
  assert(qst_block_copy(&mapping, coverage_entry->sql_ref, coverage_gid, 0,
                        coverage_sql, coverage_entry->sql_ref.length) == QST_OK);
  assert(strstr(coverage_sql, "EXPLAIN") == NULL);
  assert(strstr(coverage_sql, " FROM db_frag.") != NULL);
  coverage_sql_ref = coverage_entry->sql_ref;
  coverage_record_ref = coverage_entry->generation_record_ref;
  assert(qst_task_abort_generated(&mapping, coverage_gid, QST_ROLE_GENERATOR,
                                  generator_generation) == QST_OK);
  assert(qst_block_release(&mapping, coverage_record_ref, coverage_gid) == QST_OK);
  assert(qst_block_release(&mapping, coverage_sql_ref, coverage_gid) == QST_OK);
  assert(qst_generator_state_publish_dataset(
             &mapping, main_generation, dataset.ds_fingerprint,
             dataset.fixture_manifest_digest, dataset.object_directory_ref,
             dataset.fixture_manifest_ref) == QST_E_STALE_OWNER);
  assert(qst_generator_state_get_dataset(&mapping, &dataset_digest,
                                         &fixture_digest, &directory_ref,
                                         &fixture_ref) == QST_OK);
  assert(memcmp(dataset_digest.bytes, dataset.ds_fingerprint.bytes,
                sizeof(dataset_digest.bytes)) == 0);
  assert(memcmp(fixture_digest.bytes, dataset.fixture_manifest_digest.bytes,
                sizeof(fixture_digest.bytes)) == 0);
  assert(directory_ref.offset == dataset.object_directory_ref.offset);
  assert(fixture_ref.offset == dataset.fixture_manifest_ref.offset);
  assert(qst_generator_state_get(&mapping, &generation_state) == QST_OK);
  assert(generation_state->pending_quota_bytes == config.pending_quota_bytes);
  assert(generation_state->log_rotation_bytes == config.log_rotation_bytes);
  generation_state->dataset_snapshot_sha256.bytes[0] ^= 1U;
  assert(qst_generator_state_get_dataset(
             &mapping, &dataset_digest, &fixture_digest, &directory_ref,
             &fixture_ref) == QST_E_SHM_CORRUPT);
  generation_state->dataset_snapshot_sha256.bytes[0] ^= 1U;
  assert(qst_dataset_get_object(&mapping, &dataset, QST_TARGET_TD,
                                QST_OBJECT_TABLE_OR_MEASUREMENT, 3,
                                &object_ref) == QST_OK);
  assert(object_ref.offset == dataset.object_directory_ref.offset);
  assert(qst_dataset_get_object(&mapping, &dataset, QST_TARGET_TD,
                                QST_OBJECT_TABLE_OR_MEASUREMENT, 6,
                                &object_ref) == QST_OK);
  assert(qst_dataset_get_object(&mapping, &dataset, QST_TARGET_TD,
                                QST_OBJECT_COLUMN_OR_TAG, 23,
                                &object_ref) == QST_OK);
  assert(qst_dataset_get_child_object(&mapping, &dataset, QST_TARGET_TD, 3,
                                      QST_OBJECT_COLUMN_OR_TAG, 0,
                                      &column) == QST_OK);
  assert(column.parent_object_id == 3);
  assert(column.ordinal == 0);
  assert(qst_dataset_get_object_by_id(&mapping, &dataset, column.object_id,
                                      &table) == QST_OK);
  assert(table.object_id == column.object_id);
  assert(qst_dataset_copy_object_name(&mapping, &column, object_name,
                                      sizeof(object_name)) == QST_OK);
  assert(strcmp(object_name, "ts") == 0);
  assert(qst_dataset_copy_object_name(&mapping, &column, object_name,
                                      sizeof("ts") - 1) ==
         QST_E_INVALID_ARGUMENT);
  assert(qst_dataset_get_child_object(&mapping, &dataset, QST_TARGET_TD, 3,
                                      QST_OBJECT_COLUMN_OR_TAG, UINT32_MAX,
                                      &column) == QST_E_INVALID_ARGUMENT);
  /* A main-generation crash after dataset publication must not leave the
   * immutable snapshot or its run-owned blocks visible to the next owner. */
  assert(qst_dataset_recover_stale_generation(&mapping, main_generation + 1) ==
         QST_OK);
  assert(qst_generator_state_get_dataset(
             &mapping, &dataset_digest, &fixture_digest, &directory_ref,
             &fixture_ref) == QST_E_SHM_CORRUPT);
  for (uint32_t index = 0; index < mapping.header->block_region.element_count;
       ++index) {
    SQstBlockHeader *block = (SQstBlockHeader *)((uint8_t *)mapping.base +
        mapping.header->block_region.region_offset +
        (uint64_t)index * mapping.header->block_region.element_bytes);
    assert(atomic_load_explicit(&block->state, memory_order_acquire) ==
           QST_BLOCK_FREE);
  }
  memset(&dataset, 0, sizeof(dataset));
  assert(setenv("QST_FAKE_TAOS_VERIFY_FIXTURE", "1", 1) == 0);
  assert(qst_dataset_prepare_for_generation(&mapping, &config, main_generation,
                                            &dataset) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_VERIFY_FIXTURE") == 0);
  assert(qst_generator_state_publish_dataset(
             &mapping, main_generation, dataset.ds_fingerprint,
             dataset.fixture_manifest_digest, dataset.object_directory_ref,
             dataset.fixture_manifest_ref) == QST_OK);
  assert(qst_dataset_cleanup(&mapping, &config, &dataset) == QST_OK);
  assert(dataset.run_id == 0);
  for (uint32_t index = 0; index < mapping.header->block_region.element_count;
       ++index) {
    SQstBlockHeader *block = (SQstBlockHeader *)((uint8_t *)mapping.base +
        mapping.header->block_region.region_offset +
        (uint64_t)index * mapping.header->block_region.element_bytes);
    assert(atomic_load_explicit(&block->state, memory_order_acquire) ==
           QST_BLOCK_FREE);
  }
  assert(setenv("QST_FAKE_TAOS_FAIL", "CREATE TABLE fq_local_device", 1) == 0);
  assert(qst_dataset_prepare(&mapping, &config, &dataset) == QST_E_RESULT_STORE);
  assert(unsetenv("QST_FAKE_TAOS_FAIL") == 0);
  assert(setenv("QST_FAKE_TAOS_BAD_COUNT", "1", 1) == 0);
  assert(qst_dataset_prepare(&mapping, &config, &dataset) == QST_E_RESOURCE);
  assert(unsetenv("QST_FAKE_TAOS_BAD_COUNT") == 0);
  assert(qstRefIsEmpty(dataset.object_directory_ref));
  assert(qstRefIsEmpty(dataset.fixture_manifest_ref));
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestExternalOwnedDatabaseCleanup(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstDatasetInfo dataset = {0};
  char root[] = "/tmp/taosQueryTest-mysql-cleanup-XXXXXX";
  char dataset_dir[512] = {0};
  char mysql_client[512] = {0};
  char mysql_log[512] = {0};
  char pg_client[512] = {0};
  char pg_log[512] = {0};
  char native_log[512] = {0};
  char contents[32768] = {0};
  char path[512] = {0};
  char old_mysql_host[512] = {0};
  char old_mysql_port[512] = {0};
  char old_mysql_user[512] = {0};
  char old_mysql_password[512] = {0};
  char old_mysql_client[512] = {0};
  char old_mysql_stub_log[512] = {0};
  char old_pg_host[512] = {0};
  char old_pg_port[512] = {0};
  char old_pg_user[512] = {0};
  char old_pg_password[512] = {0};
  char old_pg_client[512] = {0};
  char old_path[4096] = {0};
  char old_pg_stub_log[512] = {0};
  char old_fake_taos_sql_log[512] = {0};
  char coverage_sql[256] = {0};
  FILE *file = NULL;
  const SQstGeneratorCatalog *catalog = NULL;
  SQstTaskEntry *coverage_entry = NULL;
  SQstRef coverage_sql_ref = {0};
  SQstRef coverage_record_ref = {0};
  EQstCode code = QST_OK;
  int32_t had_mysql_host = 0;
  int32_t had_mysql_port = 0;
  int32_t had_mysql_user = 0;
  int32_t had_mysql_password = 0;
  int32_t had_mysql_client = 0;
  int32_t had_mysql_stub_log = 0;
  int32_t had_pg_host = 0;
  int32_t had_pg_port = 0;
  int32_t had_pg_user = 0;
  int32_t had_pg_password = 0;
  int32_t had_pg_client = 0;
  int32_t had_path = 0;
  int32_t had_pg_stub_log = 0;
  int32_t had_fake_taos_sql_log = 0;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint64_t coverage_gid = 0;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(dataset_dir, sizeof(dataset_dir), "%s/dataset", root) > 0);
  assert(snprintf(mysql_client, sizeof(mysql_client), "%s/mysql", root) > 0);
  assert(snprintf(mysql_log, sizeof(mysql_log), "%s/mysql.log", root) > 0);
  assert(snprintf(pg_client, sizeof(pg_client), "%s/psql", root) > 0);
  assert(snprintf(pg_log, sizeof(pg_log), "%s/psql.log", root) > 0);
  assert(snprintf(native_log, sizeof(native_log), "%s/native.log", root) > 0);
  file = fopen(mysql_client, "w");
  assert(file != NULL);
  assert(fputs("#!/bin/sh\n"
               "set -eu\n"
               ": \"${QST_MYSQL_STUB_LOG:?QST_MYSQL_STUB_LOG is required}\"\n"
               "if [ \"${1:-}\" = --version ]; then echo 'mysql 8.0.36'; fi\n"
               "printf 'argv:' >> \"$QST_MYSQL_STUB_LOG\"\n"
               "for arg in \"$@\"; do printf ' <%s>' \"$arg\" >> \"$QST_MYSQL_STUB_LOG\"; done\n"
               "printf '\\nstdin:\\n' >> \"$QST_MYSQL_STUB_LOG\"\n"
               "cat >> \"$QST_MYSQL_STUB_LOG\"\n"
               "printf '\\n--\\n' >> \"$QST_MYSQL_STUB_LOG\"\n",
               file) >= 0);
  assert(fclose(file) == 0 && chmod(mysql_client, 0700) == 0);
  file = fopen(pg_client, "w");
  assert(file != NULL);
  assert(fputs("#!/bin/sh\n"
               "set -eu\n"
               ": \"${QST_PG_STUB_LOG:?QST_PG_STUB_LOG is required}\"\n"
               "if [ \"${1:-}\" = --version ]; then echo 'psql (PostgreSQL) 16.2'; fi\n"
               "printf 'argv:' >> \"$QST_PG_STUB_LOG\"\n"
               "for arg in \"$@\"; do printf ' <%s>' \"$arg\" >> \"$QST_PG_STUB_LOG\"; done\n"
               "printf '\\nstdin:\\n' >> \"$QST_PG_STUB_LOG\"\n"
               "cat >> \"$QST_PG_STUB_LOG\"\n"
               "printf '\\n--\\n' >> \"$QST_PG_STUB_LOG\"\n",
               file) >= 0);
  assert(fclose(file) == 0 && chmod(pg_client, 0700) == 0);
  qstTestSaveEnvironment("FQ_MYSQL_HOST", old_mysql_host,
                         sizeof(old_mysql_host), &had_mysql_host);
  qstTestSaveEnvironment("FQ_MYSQL_PORT", old_mysql_port,
                         sizeof(old_mysql_port), &had_mysql_port);
  qstTestSaveEnvironment("FQ_MYSQL_USER", old_mysql_user,
                         sizeof(old_mysql_user), &had_mysql_user);
  qstTestSaveEnvironment("FQ_MYSQL_PASSWORD", old_mysql_password,
                         sizeof(old_mysql_password), &had_mysql_password);
  qstTestSaveEnvironment("QST_MYSQL_CLIENT", old_mysql_client,
                         sizeof(old_mysql_client), &had_mysql_client);
  qstTestSaveEnvironment("QST_MYSQL_STUB_LOG", old_mysql_stub_log,
                         sizeof(old_mysql_stub_log), &had_mysql_stub_log);
  qstTestSaveEnvironment("FQ_PG_HOST", old_pg_host, sizeof(old_pg_host),
                         &had_pg_host);
  qstTestSaveEnvironment("FQ_PG_PORT", old_pg_port, sizeof(old_pg_port),
                         &had_pg_port);
  qstTestSaveEnvironment("FQ_PG_USER", old_pg_user, sizeof(old_pg_user),
                         &had_pg_user);
  qstTestSaveEnvironment("FQ_PG_PASSWORD", old_pg_password,
                         sizeof(old_pg_password), &had_pg_password);
  qstTestSaveEnvironment("QST_PG_CLIENT", old_pg_client, sizeof(old_pg_client),
                         &had_pg_client);
  qstTestSaveEnvironment("PATH", old_path, sizeof(old_path), &had_path);
  qstTestSaveEnvironment("QST_PG_STUB_LOG", old_pg_stub_log,
                         sizeof(old_pg_stub_log), &had_pg_stub_log);
  qstTestSaveEnvironment("QST_FAKE_TAOS_SQL_LOG", old_fake_taos_sql_log,
                         sizeof(old_fake_taos_sql_log),
                         &had_fake_taos_sql_log);
  assert(setenv("FQ_MYSQL_HOST", "127.0.0.1", 1) == 0);
  assert(setenv("FQ_MYSQL_PORT", "03306", 1) == 0);
  assert(setenv("FQ_MYSQL_USER", "qst", 1) == 0);
  assert(setenv("FQ_MYSQL_PASSWORD", "mysql-cleanup-secret", 1) == 0);
  assert(snprintf(path, sizeof(path), "%s:%s", root,
                  had_path != 0 ? old_path : "/usr/bin:/bin") > 0);
  assert(setenv("PATH", path, 1) == 0);
  assert(setenv("QST_MYSQL_STUB_LOG", mysql_log, 1) == 0);
  assert(setenv("FQ_PG_HOST", "127.0.0.1", 1) == 0);
  assert(setenv("FQ_PG_PORT", "5432", 1) == 0);
  assert(setenv("FQ_PG_USER", "qst", 1) == 0);
  assert(setenv("FQ_PG_PASSWORD", "postgres-cleanup-secret", 1) == 0);
  assert(setenv("QST_PG_STUB_LOG", pg_log, 1) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", native_log, 1) == 0);
  config.run_id = UINT64_C(6000000000) + (uint64_t)getpid();
  config.mode = QST_MODE_FULL;
  config.focus = QST_FOCUS_CORRECTNESS;
  config.target_selection_bits = UINT32_C(7);
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 8;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 2;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 256;
  config.pending_quota_bytes = UINT64_C(1048576);
  config.log_rotation_bytes = UINT64_C(1048576);
  config.tool_root = QST_TOOL_ROOT;
  config.result_dir = root;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  assert(qst_sql_dimension_catalog(&catalog) == QST_OK);
  assert(qst_generator_state_initialize(&mapping, catalog, config.mode,
                                        config.focus,
                                        config.target_selection_bits,
                                        config.random_seed,
                                        config.statement_limit,
                                        config.duration_limit_minutes) == QST_OK);
  code = qst_dataset_prepare_for_generation(&mapping, &config, main_generation,
                                            &dataset);
  if (code != QST_OK) {
    (void)fprintf(stderr,
                  "taosQueryTest: external cleanup fixture preparation failed code=%d\n",
                  code);
  }
  assert(code == QST_OK);
  assert(access(dataset_dir, R_OK) == 0);
  assert(snprintf(path, sizeof(path), "%s/cleanup-mysql-data.sh", dataset_dir) >
         0);
  assert(access(path, R_OK) == 0);
  assert(snprintf(path, sizeof(path), "%s/cleanup-postgresql-data.sh",
                  dataset_dir) > 0);
  assert(access(path, R_OK) == 0);
  assert(qst_generator_state_publish_dataset(
             &mapping, main_generation, dataset.ds_fingerprint,
             dataset.fixture_manifest_digest, dataset.object_directory_ref,
             dataset.fixture_manifest_ref) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, (int32_t)getpid(),
                              (int32_t)getpgrp(), &generator_generation) == QST_OK);
  assert(qst_generator_full_publish_coverage_seed(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             dataset.ds_fingerprint, QST_TARGET_MYSQL - 1U, 1, UINT64_MAX,
             &coverage_gid) == QST_OK);
  assert(coverage_gid == 1);
  coverage_entry = qstTestFindTaskByGid(&mapping, coverage_gid);
  assert(coverage_entry != NULL &&
         coverage_entry->sql_ref.length < sizeof(coverage_sql));
  assert(qst_block_copy(&mapping, coverage_entry->sql_ref, coverage_gid, 0,
                        coverage_sql, coverage_entry->sql_ref.length) == QST_OK);
  assert(strstr(coverage_sql,
                "FROM src_fq_mysql_default.fq_series WHERE 1 = 1;") != NULL);
  coverage_sql_ref = coverage_entry->sql_ref;
  coverage_record_ref = coverage_entry->generation_record_ref;
  assert(qst_task_abort_generated(&mapping, coverage_gid, QST_ROLE_GENERATOR,
                                  generator_generation) == QST_OK);
  assert(qst_block_release(&mapping, coverage_record_ref, coverage_gid) == QST_OK);
  assert(qst_block_release(&mapping, coverage_sql_ref, coverage_gid) == QST_OK);
  memset(coverage_sql, 0, sizeof(coverage_sql));
  assert(qst_generator_full_publish_coverage_seed(
             &mapping, QST_ROLE_GENERATOR, generator_generation, catalog,
             dataset.ds_fingerprint, QST_TARGET_PG - 1U, 1, UINT64_MAX,
             &coverage_gid) == QST_OK);
  assert(coverage_gid == 2);
  coverage_entry = qstTestFindTaskByGid(&mapping, coverage_gid);
  assert(coverage_entry != NULL &&
         coverage_entry->sql_ref.length < sizeof(coverage_sql));
  assert(qst_block_copy(&mapping, coverage_entry->sql_ref, coverage_gid, 0,
                        coverage_sql, coverage_entry->sql_ref.length) == QST_OK);
  assert(strstr(coverage_sql,
                "FROM src_fq_pg_default.fq_series WHERE 1 = 1;") != NULL);
  coverage_sql_ref = coverage_entry->sql_ref;
  coverage_record_ref = coverage_entry->generation_record_ref;
  assert(qst_task_abort_generated(&mapping, coverage_gid, QST_ROLE_GENERATOR,
                                  generator_generation) == QST_OK);
  assert(qst_block_release(&mapping, coverage_record_ref, coverage_gid) == QST_OK);
  assert(qst_block_release(&mapping, coverage_sql_ref, coverage_gid) == QST_OK);
  assert(qst_dataset_cleanup(&mapping, &config, &dataset) == QST_OK);
  file = fopen(mysql_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents,
                "stdin:\nDROP DATABASE IF EXISTS fq_alt;\n"
                "DROP DATABASE IF EXISTS fq_default;\n\n--\n") != NULL);
  assert(strstr(contents, "fq_unowned") == NULL);
  assert(strstr(contents, "mysql-cleanup-secret") == NULL);
  memset(contents, 0, sizeof(contents));
  file = fopen(pg_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents,
                "stdin:\nDROP DATABASE IF EXISTS fq_default;\n\n--\n") != NULL);
  assert(strstr(contents, "fq_unowned") == NULL);
  assert(strstr(contents, "postgres-cleanup-secret") == NULL);
  memset(contents, 0, sizeof(contents));
  file = fopen(native_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents,
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_mysql_default") != NULL);
  assert(strstr(contents,
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_mysql_explicit") != NULL);
  assert(strstr(contents,
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_pg_default") != NULL);
  assert(strstr(contents,
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_pg_explicit") != NULL);
  assert(strstr(contents, "PORT = 3306") != NULL);
  assert(strstr(contents,
                "SELECT COUNT(*) FROM information_schema.ins_qnodes") != NULL);
  assert(strstr(contents, "CREATE QNODE ON DNODE 1") != NULL);
  file = fopen(mysql_log, "w");
  assert(file != NULL && fclose(file) == 0);
  file = fopen(pg_log, "w");
  assert(file != NULL && fclose(file) == 0);
  assert(setenv("QST_FAKE_TAOS_FAIL",
                "CREATE EXTERNAL SOURCE IF NOT EXISTS src_fq_mysql_default",
                1) == 0);
  memset(&dataset, 0, sizeof(dataset));
  assert(qst_dataset_prepare_for_generation(&mapping, &config, main_generation,
                                            &dataset) == QST_E_RESULT_STORE);
  assert(unsetenv("QST_FAKE_TAOS_FAIL") == 0);
  memset(contents, 0, sizeof(contents));
  file = fopen(mysql_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents, "CREATE DATABASE fq_default") != NULL);
  assert(strstr(contents,
                "stdin:\nDROP DATABASE IF EXISTS fq_alt;\n"
                "DROP DATABASE IF EXISTS fq_default;\n\n--\n") != NULL);
  memset(contents, 0, sizeof(contents));
  file = fopen(pg_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents, "CREATE DATABASE fq_default") != NULL);
  assert(strstr(contents,
                "stdin:\nDROP DATABASE IF EXISTS fq_default;\n\n--\n") != NULL);
  file = fopen(mysql_log, "w");
  assert(file != NULL && fclose(file) == 0);
  file = fopen(pg_log, "w");
  assert(file != NULL && fclose(file) == 0);
  file = fopen(native_log, "w");
  assert(file != NULL && fclose(file) == 0);
  memset(&dataset, 0, sizeof(dataset));
  assert(qst_dataset_prepare_for_generation(&mapping, &config, main_generation,
                                            &dataset) == QST_OK);
  file = fopen(mysql_log, "w");
  assert(file != NULL && fclose(file) == 0);
  file = fopen(pg_log, "w");
  assert(file != NULL && fclose(file) == 0);
  file = fopen(native_log, "w");
  assert(file != NULL && fclose(file) == 0);
  assert(setenv("QST_FAKE_TAOS_FAIL",
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_mysql_default", 1) ==
         0);
  assert(qst_dataset_cleanup(&mapping, &config, &dataset) == QST_E_RESULT_STORE);
  assert(unsetenv("QST_FAKE_TAOS_FAIL") == 0);
  memset(contents, 0, sizeof(contents));
  file = fopen(mysql_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) == 0 &&
         fclose(file) == 0);
  memset(contents, 0, sizeof(contents));
  file = fopen(pg_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents,
                "stdin:\nDROP DATABASE IF EXISTS fq_default;\n\n--\n") != NULL);
  memset(contents, 0, sizeof(contents));
  file = fopen(native_log, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1U, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents,
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_mysql_default") != NULL);
  assert(strstr(contents,
                "DROP EXTERNAL SOURCE IF EXISTS src_fq_pg_default") != NULL);
  assert(qst_shm_unmap(&mapping) == QST_OK);
  qstTestRestoreEnvironment("QST_FAKE_TAOS_SQL_LOG", old_fake_taos_sql_log,
                            had_fake_taos_sql_log);
  qstTestRestoreEnvironment("QST_MYSQL_STUB_LOG", old_mysql_stub_log,
                            had_mysql_stub_log);
  qstTestRestoreEnvironment("QST_MYSQL_CLIENT", old_mysql_client,
                            had_mysql_client);
  qstTestRestoreEnvironment("FQ_MYSQL_PASSWORD", old_mysql_password,
                            had_mysql_password);
  qstTestRestoreEnvironment("FQ_MYSQL_USER", old_mysql_user, had_mysql_user);
  qstTestRestoreEnvironment("FQ_MYSQL_PORT", old_mysql_port, had_mysql_port);
  qstTestRestoreEnvironment("FQ_MYSQL_HOST", old_mysql_host, had_mysql_host);
  qstTestRestoreEnvironment("QST_PG_STUB_LOG", old_pg_stub_log,
                            had_pg_stub_log);
  qstTestRestoreEnvironment("QST_PG_CLIENT", old_pg_client, had_pg_client);
  qstTestRestoreEnvironment("PATH", old_path, had_path);
  qstTestRestoreEnvironment("FQ_PG_PASSWORD", old_pg_password,
                            had_pg_password);
  qstTestRestoreEnvironment("FQ_PG_USER", old_pg_user, had_pg_user);
  qstTestRestoreEnvironment("FQ_PG_PORT", old_pg_port, had_pg_port);
  qstTestRestoreEnvironment("FQ_PG_HOST", old_pg_host, had_pg_host);
  assert(unlink(native_log) == 0 && unlink(mysql_log) == 0 &&
         unlink(mysql_client) == 0 && unlink(pg_log) == 0 &&
         unlink(pg_client) == 0);
  assert(snprintf(path, sizeof(path), "%s/provider-readiness.json", dataset_dir) >
         0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/tdengine-readiness.json", dataset_dir) >
         0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/cleanup-mysql-data.sh", dataset_dir) >
         0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/cleanup-postgresql-data.sh",
                  dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/prepare-mysql-data.sh", dataset_dir) >
         0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/prepare-postgresql-data.sh",
                  dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/external-mapping-rules.json", dataset_dir) >
         0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/external-mapping-rules.schema.json",
                  dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/find-client.sh", dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/taosbenchmark.json", dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(rmdir(dataset_dir) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestExternalSourcePortValidation(void) {
  static const char *const invalid_ports[] = {
      "", "0", "65536", "999999999999999999999999", " 3306", "+3306",
      "-3306", "3306 ", "3306;DROP EXTERNAL SOURCE x", "3306\n"};
  uint16_t port = 0;

  assert(qst_dataset_parse_external_port("1", &port) == QST_OK);
  assert(port == 1);
  assert(qst_dataset_parse_external_port("00042", &port) == QST_OK);
  assert(port == 42);
  assert(qst_dataset_parse_external_port("65535", &port) == QST_OK);
  assert(port == UINT16_MAX);
  assert(qst_dataset_parse_external_port(NULL, &port) == QST_E_INVALID_ARGUMENT);
  assert(qst_dataset_parse_external_port("3306", NULL) == QST_E_INVALID_ARGUMENT);
  for (uint32_t index = 0; index < sizeof(invalid_ports) / sizeof(invalid_ports[0]);
       ++index) {
    assert(qst_dataset_parse_external_port(invalid_ports[index], &port) ==
           QST_E_INVALID_ARGUMENT);
  }
}

static SQstMessage qstControlMessage(uint64_t run_id) {
  SQstMessage message = {0};

  message.message_type = QST_MSG_CONTROL;
  message.producer_role = QST_ROLE_LAUNCHER;
  message.producer_generation = 1;
  message.run_id = run_id;
  message.body.control.opcode = QST_CONTROL_START;
  return message;
}

static void qstTestQueueClaimAckAndFull(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstMessage message = {0};
  SQstClaim first = {0};
  SQstClaim second = {0};

  qstInitMapping(&mapping, memory, sizeof(memory));
  message = qstControlMessage(mapping.header->run_id);
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_OK);
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_OK);
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_E_QUEUE_FULL);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_LAUNCHER, 1,
                         UINT64_MAX, &first) == QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_LAUNCHER, 1,
                         UINT64_MAX, &second) == QST_OK);
  assert(first.message.message_id != 0);
  assert(second.message.message_id != 0);
  assert(first.message.message_id != second.message.message_id);
  assert(qst_queue_ack(&mapping, &first, QST_ACK_CONTROL_RESPONSE, 5) == QST_OK);
  assert(qst_queue_ack(&mapping, &first, QST_ACK_CONTROL_RESPONSE, 5) ==
         QST_E_STALE_OWNER);
  assert(qst_queue_ack(&mapping, &second, QST_ACK_CONTROL_RESPONSE, 6) == QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_LAUNCHER, 1,
                         UINT64_MAX, &first) == QST_E_QUEUE_EMPTY);
}

static void qstTestQueueAbandonAndRejectBadMessage(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstMessage message = {0};
  SQstClaim claim = {0};

  qstInitMapping(&mapping, memory, sizeof(memory));
  message = qstControlMessage(mapping.header->run_id);
  message.message_type = QST_MSG_EXECUTE_FINAL;
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_E_SHM_CORRUPT);
  message = qstControlMessage(mapping.header->run_id);
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_LAUNCHER, 1,
                         UINT64_MAX, &claim) == QST_OK);
  assert(qst_queue_abandon(&mapping, &claim, QST_E_INTERNAL) == QST_OK);
  {
    SQstQueueSlot *slot = (SQstQueueSlot *)(
        memory + kTestQueueOffset + sizeof(SQstQueueDescriptor));
    slot->message.producer_generation = 999;
    slot->message.message_crc32c = 0;
  }
  assert(qst_queue_requeue_abandoned(&mapping, QST_QUEUE_CONTROL,
                                     claim.message.message_id, QST_ROLE_LAUNCHER,
                                     1, UINT64_MAX) ==
         QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_LAUNCHER, 1,
                         UINT64_MAX, &claim) == QST_OK);
  assert(qst_queue_ack(&mapping, &claim, QST_ACK_CONTROL_RESPONSE, 7) == QST_OK);
}

static void qstTestQueueRejectsCorruptVisibleMessage(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstMessage message = {0};
  SQstClaim claim = {0};
  SQstQueueSlot *slot = NULL;

  qstInitMapping(&mapping, memory, sizeof(memory));
  message = qstControlMessage(mapping.header->run_id);
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_OK);
  slot = (SQstQueueSlot *)(memory + kTestQueueOffset +
                            sizeof(SQstQueueDescriptor));
  slot->message.message_crc32c ^= 1U;
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_LAUNCHER, 1,
                         UINT64_MAX, &claim) == QST_E_SHM_CORRUPT);
}

static void qstTestQueueRejectsWrongConsumerRole(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstClaim claim = {0};

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_queue_claim(&mapping, QST_QUEUE_PERSIST, QST_ROLE_GENERATOR, 1,
                         UINT64_MAX, &claim) == QST_E_INVALID_ARGUMENT);
}

static void qstTestParentReclaimsExitedConsumerClaim(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstMessage message = {0};
  SQstClaim claim = {0};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              300, 300, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 301, 301,
                              &generator_generation) == QST_OK);
  message = qstControlMessage(mapping.header->run_id);
  message.producer_role = QST_ROLE_GENERATOR;
  message.producer_generation = generator_generation;
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_GENERATOR,
                         generator_generation, UINT64_MAX, &claim) == QST_OK);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_MAIN, main_generation,
                                     QST_ROLE_GENERATOR, generator_generation,
                                     0) == QST_OK);
  assert(qst_queue_abandon(&mapping, &claim, QST_E_INTERNAL) ==
         QST_E_STALE_OWNER);
  assert(qst_queue_reclaim_claim(&mapping, &claim, QST_ROLE_LAUNCHER, 0, 1,
                                 QST_E_INTERNAL) == QST_E_STALE_OWNER);
  assert(qst_queue_reclaim_claim(&mapping, &claim, QST_ROLE_MAIN, 1,
                                 main_generation, QST_E_INTERNAL) ==
         QST_E_STALE_OWNER);
  assert(qst_queue_reclaim_claim(&mapping, &claim, QST_ROLE_MAIN, 0,
                                 main_generation, QST_E_INTERNAL) == QST_OK);
  assert(qst_queue_requeue_abandoned(&mapping, QST_QUEUE_CONTROL,
                                     claim.message.message_id, QST_ROLE_MAIN,
                                     main_generation, UINT64_MAX) == QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_CONTROL, QST_ROLE_MAIN,
                         main_generation, UINT64_MAX, &claim) == QST_OK);
  assert(claim.message.producer_role == QST_ROLE_MAIN);
  assert(claim.message.producer_generation == main_generation);
  assert(qst_queue_ack(&mapping, &claim, QST_ACK_CONTROL_RESPONSE,
                       claim.message.message_id) == QST_OK);
}

static void qstRegisterTaskRoles(SQstShmMapping *mapping,
                                 uint32_t *out_main_generation,
                                 uint32_t *out_generator_generation,
                                 uint32_t *out_exec_generation,
                                 uint32_t *out_pair_generation,
                                 uint32_t *out_judge_generation) {
  assert(qst_process_register(mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              200, 200, out_main_generation) == QST_OK);
  assert(qst_process_register(mapping, QST_ROLE_MAIN, *out_main_generation,
                              QST_ROLE_GENERATOR, 201, 201,
                              out_generator_generation) == QST_OK);
  assert(qst_process_register(mapping, QST_ROLE_MAIN, *out_main_generation,
                              QST_ROLE_EXEC_MANAGER, 202, 202,
                              out_exec_generation) == QST_OK);
  assert(qst_process_register(mapping, QST_ROLE_EXEC_MANAGER,
                              *out_exec_generation, QST_ROLE_PAIR_SUPERVISOR,
                              203, 203, out_pair_generation) == QST_OK);
  assert(qst_process_register(mapping, QST_ROLE_MAIN, *out_main_generation,
                              QST_ROLE_JUDGE_WORKER, 204, 204,
                              out_judge_generation) == QST_OK);
}

static void qstTestExecutionInputsDrained(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstTaskEntry *entries = NULL;
  int32_t drained = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  entries = (SQstTaskEntry *)(memory + kTestTaskOffset);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 1);
  for (uint32_t index = 0; index < mapping.header->task_region.element_count;
       ++index) {
    atomic_store_explicit(&entries[index].phase, QST_TASK_FINAL_PUBLISHED,
                          memory_order_release);
  }
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_GENERATED,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_EXECUTING,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_REGISTERING,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_FINAL,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_ISOLATED,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_PERSISTING,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_PERSISTED,
                        memory_order_release);
  assert(qst_task_execution_inputs_drained(&mapping, &drained) == QST_OK);
  assert(drained == 1);
}

static void qstTestPairRetireEligibility(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstTaskEntry *entries = NULL;
  uint32_t main_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_zero_generation = 0;
  uint32_t pair_one_generation = 0;
  int32_t eligible = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  entries = (SQstTaskEntry *)(memory + kTestTaskOffset);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              200, 200, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, 201, 201,
                              &exec_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 0, 202, 202,
             &pair_zero_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 1, 203, 203,
             &pair_one_generation) == QST_OK);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 0);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             2, &eligible) == QST_E_INVALID_ARGUMENT);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             1, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&mapping.header->stop_reason,
                        QST_STOP_STATEMENT_LIMIT, memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_GENERATED,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_REGISTERING,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_EXECUTING,
                        memory_order_release);
  atomic_store_explicit(&entries[0].owner_role, QST_ROLE_PAIR_SUPERVISOR,
                        memory_order_release);
  atomic_store_explicit(&entries[0].owner_slot_id, 0, memory_order_release);
  atomic_store_explicit(&entries[0].owner_generation, pair_zero_generation,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 0);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 1, pair_one_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_FINAL,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 0);
  atomic_store_explicit(&entries[0].owner_slot_id, 1, memory_order_release);
  atomic_store_explicit(&entries[0].owner_generation, pair_one_generation,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_FINAL_PUBLISHED,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_PERSISTING,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_PERSISTED,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 1);
  atomic_store_explicit(&entries[0].phase, QST_TASK_ISOLATED,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 0);
  atomic_store_explicit(&entries[0].phase, QST_TASK_EXECUTING,
                        memory_order_release);
  atomic_store_explicit(&entries[0].owner_role, QST_ROLE_EXEC_MANAGER,
                        memory_order_release);
  atomic_store_explicit(&entries[0].owner_slot_id, 0, memory_order_release);
  atomic_store_explicit(&entries[0].owner_generation, exec_generation,
                        memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_OK);
  assert(eligible == 0);
  atomic_store_explicit(&entries[0].phase, UINT32_MAX, memory_order_release);
  assert(qst_task_pair_retire_eligible(
             &mapping, exec_generation, 0, pair_zero_generation,
             0, &eligible) == QST_E_SHM_CORRUPT);
}

static void qstTestTaskLifecycleAndDurability(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t sql[] =
      "SELECT COUNT(*) AS qst FROM src_fq_mysql_default.fq_series "
      "WHERE provider = 'a\\'b';";
  const uint8_t isolated_sql[] = "SELECT invalid_isolated";
  const uint8_t result_content[] = "{\"format\":\"query-result-v1\"}";
  const uint8_t stable_key[] = "statements|101|td|1|1000000";
  const uint8_t persist_record[] = "{\"object\":\"statement\"}";
  const uint8_t result_record[] =
      "{\"kind\":\"result\",\"verdict\":2,\"validator\":\"manual\","
      "\"diff_summary\":\"no deterministic validator is available\"}";
  SQstShmMapping mapping = {0};
  SQstGeneratePayload input = {0};
  SQstExecutePayload final_payload = {0};
  SQstExecutePayload final_snapshot = {0};
  SQstTaskPersistSnapshot persist_snapshot = {0};
  SQstMessage final_message = {0};
  SQstTaskDispatch dispatch = {0};
  SQstClaim execute_claim = {0};
  SQstPersistPayload persist = {0};
  SQstResultStore store = {0};
  SQstResultStoreConfig store_config = {0};
  SQstTaskEntry *entry = NULL;
  SQstRef sql_ref = {0};
  SQstRef generation_record_ref = {0};
  SQstRef result_ref = {0};
  SQstRef stable_key_ref = {0};
  SQstRef persist_record_ref = {0};
  SQstRef result_record_ref = {0};
  char sql_log[] = "/tmp/taosQueryTest-business-dml-XXXXXX";
  char sql_contents[32768] = {0};
  int32_t sql_fd = -1;
  FILE *sql_file = NULL;
  uint64_t gid = 0;
  uint64_t first_gid = 0;
  uint64_t dispatch_epoch = 0;
  uint64_t transition_seq = 0;
  uint64_t final_gid = 0;
  uint64_t final_dispatch_epoch = 0;
  uint64_t final_transition_seq = 0;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  uint32_t judge_generation = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  qstRegisterTaskRoles(&mapping, &main_generation, &generator_generation,
                       &exec_generation, &pair_generation, &judge_generation);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_MAIN, main_generation,
                              &gid) == QST_E_INVALID_ARGUMENT);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation + 1, &gid) ==
         QST_E_STALE_OWNER);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, &gid) == QST_OK);
  assert(gid != 0);
  first_gid = gid;
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql) - 1U, &sql_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &sql_ref, sql, sizeof(sql) - 1U) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql) - 1U,
                         &generation_record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &generation_record_ref, sql,
                              sizeof(sql) - 1U) == QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_FULL;
  input.focus = QST_FOCUS_CORRECTNESS;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 1;
  input.weight_profile_version = 1;
  input.sql_ref = sql_ref;
  input.generation_record_ref = generation_record_ref;
  assert(qst_block_sha256(&mapping, generation_record_ref, gid,
                          &input.generation_record_sha256) == QST_OK);
  input.generation_record_sha256.bytes[0] ^= 1U;
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input,
                         &dispatch_epoch) == QST_E_BLOCK_INVALID);
  input.generation_record_sha256.bytes[0] ^= 1U;
  assert(qst_task_create(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                         exec_generation, &input, &dispatch_epoch) ==
         QST_E_INVALID_ARGUMENT);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation + 1, &input,
                         &dispatch_epoch) == QST_E_STALE_OWNER);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input, &dispatch_epoch) == QST_OK);
  assert(dispatch_epoch == 1);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input,
                         &dispatch_epoch) == QST_E_TASK_STATE);
  assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                 exec_generation + 1, dispatch_epoch, &input,
                                 &transition_seq) == QST_E_STALE_OWNER);
  input.target = QST_TARGET_MYSQL;
  assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                 exec_generation, dispatch_epoch, &input,
                                 &transition_seq) == QST_E_SHM_CORRUPT);
  input.target = QST_TARGET_TD;
  assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                 exec_generation, dispatch_epoch, &input,
                                 &transition_seq) == QST_OK);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, dispatch_epoch, transition_seq,
                              0,
                              pair_generation + 1) == QST_E_STALE_OWNER);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, dispatch_epoch, transition_seq,
                              0,
                              pair_generation) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(result_content) - 1U,
                         &result_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &result_ref, result_content,
                              sizeof(result_content) - 1U) == QST_OK);
  final_payload.statement_ts_ns = INT64_C(1000000);
  final_payload.ended_ts_ns = INT64_C(2000000);
  final_payload.sid = 7;
  final_payload.result_row_count = 1;
  final_payload.result_ref = result_ref;
  assert(qst_task_set_result_storage(&mapping, gid, dispatch_epoch,
                                     transition_seq, 0, pair_generation,
                                     QST_RESULT_MEMORY) == QST_OK);
  assert(qst_task_find_assigned_pair(&mapping, 0, pair_generation + 1,
                                     &dispatch) == QST_E_STALE_OWNER);
  assert(qst_task_find_assigned_pair(&mapping, 0, pair_generation,
                                     &dispatch) == QST_OK);
  assert(dispatch.gid == gid);
  assert(dispatch.dispatch_epoch == dispatch_epoch);
  assert(dispatch.transition_seq == transition_seq);
  assert(dispatch.sql_ref.offset == sql_ref.offset);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, dispatch_epoch, transition_seq,
                              0,
                              pair_generation) == QST_E_STALE_OWNER);
  assert(qst_task_publish_final(&mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_PAIR_SUPERVISOR, 0, pair_generation,
                                &final_payload) == QST_OK);
  entry = (SQstTaskEntry *)(memory + kTestTaskOffset);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_FINAL);
  assert(qst_task_find_final_for_exec(&mapping, exec_generation, &final_gid,
                                      &final_dispatch_epoch,
                                      &final_transition_seq) ==
         QST_E_QUEUE_EMPTY);
  assert(qst_task_get_final_pair(&mapping, gid, dispatch_epoch, transition_seq,
                                 0, pair_generation, &final_snapshot,
                                 &final_message.target) == QST_OK);
  assert(memcmp(&final_snapshot, &final_payload, sizeof(final_snapshot)) == 0);
  final_message.message_type = QST_MSG_EXECUTE_FINAL;
  final_message.producer_role = QST_ROLE_PAIR_SUPERVISOR;
  final_message.producer_generation = pair_generation;
  final_message.run_id = mapping.header->run_id;
  final_message.gid = gid;
  final_message.dispatch_epoch = dispatch_epoch;
  final_message.transition_seq = transition_seq;
  final_message.body.execute = final_snapshot;
  assert(qst_queue_publish(&mapping, QST_QUEUE_EXECUTE, &final_message,
                           UINT64_MAX) == QST_OK);
  assert(qst_task_confirm_final_published_pair(
             &mapping, gid, dispatch_epoch, transition_seq, 0,
             pair_generation, &final_payload) == QST_OK);
  assert(qst_task_get_persist_snapshot(&mapping, gid, dispatch_epoch,
                                       transition_seq, &persist_snapshot) == QST_OK);
  assert(persist_snapshot.gid == gid);
  assert(persist_snapshot.sql_ref.offset == sql_ref.offset);
  assert(persist_snapshot.final_payload.execution_status == QST_EXEC_SUCCESS);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_FINAL_PUBLISHED);
  assert(qst_task_find_final_for_exec(&mapping, exec_generation, &final_gid,
                                      &final_dispatch_epoch,
                                      &final_transition_seq) ==
         QST_E_QUEUE_EMPTY);
  assert(qst_queue_claim(&mapping, QST_QUEUE_EXECUTE, QST_ROLE_JUDGE_WORKER,
                         judge_generation, UINT64_MAX, &execute_claim) == QST_OK);
  assert(execute_claim.message.gid == gid);
  assert(qst_queue_ack(&mapping, &execute_claim, QST_ACK_PERSIST_WORK_ACCEPTED,
                       1) == QST_OK);
  assert(qst_task_begin_persist(&mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_JUDGE_WORKER, 0, judge_generation,
                                2,
                                QST_PERSIST_MASK_STATEMENT |
                                    QST_PERSIST_MASK_INITIAL_RESULT) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(stable_key) - 1U,
                         &stable_key_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &stable_key_ref, stable_key,
                              sizeof(stable_key) - 1U) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(persist_record) - 1U,
                         &persist_record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &persist_record_ref, persist_record,
                              sizeof(persist_record) - 1U) == QST_OK);
  persist.persist_kind = QST_PERSIST_STATEMENT;
  persist.target = QST_TARGET_TD;
  persist.reserved0 = 2;
  persist.gid = gid;
  persist.dispatch_epoch = dispatch_epoch;
  persist.transition_seq = transition_seq;
  persist.record_seq = 1;
  persist.stable_key_ref = stable_key_ref;
  persist.payload_ref = persist_record_ref;
  assert(qst_block_sha256(&mapping, persist_record_ref, gid,
                          &persist.payload_sha256) == QST_OK);
  store_config.run_id = mapping.header->run_id;
  assert(qst_result_store_open(&store_config, &store) == QST_OK);
  sql_fd = mkstemp(sql_log);
  assert(sql_fd >= 0 && close(sql_fd) == 0);
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", sql_log, 1) == 0);
  assert(qst_result_store_write(&mapping, &store, &persist,
                                &(int32_t){0}) == QST_OK);
  persist.persist_kind = QST_PERSIST_INITIAL_RESULT;
  persist.record_seq = 2;
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(result_record) - 1U,
                         &result_record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &result_record_ref, result_record,
                              sizeof(result_record) - 1U) == QST_OK);
  persist.payload_ref = result_record_ref;
  assert(qst_block_sha256(&mapping, result_record_ref, gid,
                          &persist.payload_sha256) == QST_OK);
  assert(qst_result_store_write(&mapping, &store, &persist,
                                &(int32_t){0}) == QST_OK);
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  sql_file = fopen(sql_log, "rb");
  assert(sql_file != NULL &&
         fread(sql_contents, 1, sizeof(sql_contents) - 1U, sql_file) > 0 &&
         fclose(sql_file) == 0);
  assert(strstr(
             sql_contents,
             "INSERT INTO res_db.stmt_101_td_w2 "
             "(ts,gid,ended_at,sid,sql_text,sql_sha256,sql_shape_mask,"
             "taosd_pid,taosc_pid,execution_status,result_row_count,"
             "error_code,error_message,taosd_log_ranges,taosc_log_ranges,"
             "core_relative_path,dialect,generation_abnormal,"
             "generation_unsupported_fallback) VALUES") != NULL);
  assert(strstr(sql_contents,
                "INSERT INTO res_db.stmt_101_td_w2 (ts,gid,ended_at,sid,") != NULL);
  assert(strstr(sql_contents, ") VALUES (1,1,2,7,") != NULL);
  assert(strstr(sql_contents,
                ",5134,'0','0',0,1,NULL,NULL,NULL,NULL,NULL,1,0,0)") != NULL);
  assert(strstr(sql_contents,
                "SELECT COUNT(*) AS qst FROM src_fq_mysql_default.fq_series") != NULL);
  assert(strstr(sql_contents, "provider = ''a\\\\''b''") != NULL);
  assert(strstr(sql_contents, "INSERT INTO res_db.result_101_td_w2 VALUES") != NULL);
  assert(strstr(sql_contents,
                "INSERT INTO res_db.result_101_td_w2 VALUES (2,1,2,") != NULL);
  assert(strstr(sql_contents, "'\\x7b22666f726d6174223a2271756572792d726573756c742d7631227d'") != NULL);
  assert(unlink(sql_log) == 0);
  assert(qst_result_store_close(&store) == QST_OK);
  assert(qst_task_mark_durable(&mapping, gid, dispatch_epoch,
                               QST_PERSIST_MASK_STATEMENT,
                               QST_DURABLE_RESULT_STORE, 1,
                               QST_ROLE_MAIN, main_generation) == QST_OK);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_PERSISTING);
  assert(qst_task_mark_durable(&mapping, gid, dispatch_epoch,
                               QST_PERSIST_MASK_INITIAL_RESULT,
                               QST_DURABLE_PENDING_FILE, 2,
                               QST_ROLE_MAIN, main_generation) == QST_OK);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_PERSISTED);
  assert(atomic_load_explicit(&mapping.header->persisted_task_count,
                              memory_order_acquire) == 1);
  assert(atomic_load_explicit(&mapping.header->execution_success_count,
                              memory_order_acquire) == 1);
  assert(atomic_load_explicit(&mapping.header->verdict_passed_count,
                              memory_order_acquire) == 0);
  assert(atomic_load_explicit(&mapping.header->verdict_failed_count,
                              memory_order_acquire) == 0);
  assert(atomic_load_explicit(&mapping.header->verdict_unverified_count,
                              memory_order_acquire) == 1);
  assert(qst_task_mark_durable(&mapping, gid, dispatch_epoch,
                               QST_PERSIST_MASK_INITIAL_RESULT,
                               QST_DURABLE_PENDING_FILE, 3,
                               QST_ROLE_MAIN, main_generation) == QST_E_STALE_OWNER);
  assert(qst_task_release_persisted_blocks(&mapping, gid, QST_ROLE_MAIN,
                                           main_generation + 1U) ==
         QST_E_STALE_OWNER);
  assert(qst_task_release_persisted_blocks(&mapping, gid, QST_ROLE_MAIN,
                                           main_generation) == QST_OK);
  assert(qstRefIsEmpty(entry->sql_ref));
  assert(qstRefIsEmpty(entry->generation_record_ref));
  assert(qstRefIsEmpty(entry->final_payload.result_ref));
  assert(qst_block_validate(&mapping, sql_ref, gid) == QST_E_BLOCK_INVALID);
  assert(qst_block_validate(&mapping, generation_record_ref, gid) ==
         QST_E_BLOCK_INVALID);
  assert(qst_block_validate(&mapping, result_ref, gid) == QST_E_BLOCK_INVALID);
  assert(qst_task_reclaim_persisted(&mapping, gid, QST_ROLE_MAIN,
                                    main_generation) == QST_OK);
  assert(qst_block_release(&mapping, stable_key_ref, gid) == QST_OK);
  assert(qst_block_release(&mapping, persist_record_ref, gid) == QST_OK);
  assert(qst_block_release(&mapping, result_record_ref, gid) == QST_OK);

  memset(&input, 0, sizeof(input));
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, &gid) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(isolated_sql) - 1U,
                         &sql_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &sql_ref, isolated_sql,
                              sizeof(isolated_sql) - 1U) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(isolated_sql) - 1U,
                         &generation_record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &generation_record_ref, isolated_sql,
                              sizeof(isolated_sql) - 1U) == QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_RANDOM;
  input.focus = QST_FOCUS_COVERAGE;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 2;
  input.weight_profile_version = 1;
  input.sql_ref = sql_ref;
  input.generation_record_ref = generation_record_ref;
  assert(qst_block_sha256(&mapping, generation_record_ref, gid,
                          &input.generation_record_sha256) == QST_OK);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input,
                         &dispatch_epoch) == QST_OK);
  /* Reusing the released physical slot must not let a cached old gid resolve
   * to the new task. */
  assert(qst_task_get_persist_snapshot(&mapping, first_gid, 1,
                                       transition_seq, &persist_snapshot) ==
         QST_E_TASK_STATE);
  assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                 exec_generation, dispatch_epoch, &input,
                                 &transition_seq) == QST_OK);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, dispatch_epoch, transition_seq,
                              0, pair_generation) == QST_OK);
  assert(qst_task_recover_pair_failure(&mapping, exec_generation, 0,
                                       pair_generation, &dispatch) == QST_OK);
  assert(dispatch.gid == gid && dispatch.dispatch_epoch == dispatch_epoch + 1U &&
         dispatch.transition_seq == transition_seq + 1U);
  memset(&final_payload, 0, sizeof(final_payload));
  final_payload.statement_ts_ns = INT64_C(3000000);
  final_payload.ended_ts_ns = INT64_C(4000000);
  final_payload.execution_status = QST_EXEC_ERROR;
  final_payload.execution_error_code = QST_E_RESOURCE;
  final_payload.result_row_count = -1;
  assert(qst_task_isolate(&mapping, gid, QST_E_RESOURCE,
                          QST_ROLE_EXEC_MANAGER, exec_generation,
                          &final_payload) == QST_OK);
  assert(atomic_load_explicit(&mapping.header->isolated_task_count,
                              memory_order_acquire) == 1);
  assert(qst_task_find_final_for_exec(&mapping, exec_generation, &final_gid,
                                      &final_dispatch_epoch,
                                      &final_transition_seq) == QST_OK);
  assert(final_gid == gid && final_dispatch_epoch == dispatch.dispatch_epoch &&
         final_transition_seq == dispatch.transition_seq);
  assert(qst_task_get_final(&mapping, gid, dispatch.dispatch_epoch,
                            dispatch.transition_seq, QST_ROLE_EXEC_MANAGER,
                            exec_generation, &final_snapshot,
                            &final_message.target) == QST_OK);
  assert(memcmp(&final_snapshot, &final_payload, sizeof(final_snapshot)) == 0);
  assert(qst_task_confirm_final_published(
             &mapping, gid, dispatch.dispatch_epoch, dispatch.transition_seq,
             QST_ROLE_EXEC_MANAGER, exec_generation, &final_payload) == QST_OK);
  assert(qst_task_get_persist_snapshot(&mapping, gid, dispatch.dispatch_epoch,
                                       dispatch.transition_seq,
                                       &persist_snapshot) == QST_OK);
  assert(persist_snapshot.final_payload.execution_status == QST_EXEC_ERROR &&
         persist_snapshot.final_payload.execution_error_code == QST_E_RESOURCE &&
         persist_snapshot.result_storage_status == QST_RESULT_UNAVAILABLE);
  assert(qst_task_begin_persist(&mapping, gid, dispatch.dispatch_epoch,
                                dispatch.transition_seq, QST_ROLE_JUDGE_WORKER,
                                0, judge_generation, -1,
                                QST_PERSIST_MASK_STATEMENT) == QST_OK);
  assert(qst_task_mark_durable(&mapping, gid, dispatch.dispatch_epoch,
                               QST_PERSIST_MASK_STATEMENT,
                               QST_DURABLE_RESULT_STORE, 3,
                               QST_ROLE_MAIN, main_generation) == QST_OK);
  assert(atomic_load_explicit(&mapping.header->execution_error_count,
                              memory_order_acquire) == 1);
  assert(qst_task_release_persisted_blocks(&mapping, gid, QST_ROLE_MAIN,
                                           main_generation) == QST_OK);
  assert(qst_task_reclaim_persisted(&mapping, gid, QST_ROLE_MAIN,
                                    main_generation) == QST_OK);
}

static void qstTestTaskDispatchAbortRollback(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t sql[] = "SELECT 1";
  SQstShmMapping mapping = {0};
  SQstGeneratePayload input = {0};
  SQstMessage message = {0};
  SQstClaim claim = {0};
  SQstTaskEntry *entry = NULL;
  SQstRef sql_ref = {0};
  SQstRef record_ref = {0};
  SQstTaskDispatch dispatch = {0};
  uint64_t gid = 0;
  uint64_t dispatch_epoch = 0;
  uint64_t transition_seq = 0;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  uint32_t judge_generation = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  qstRegisterTaskRoles(&mapping, &main_generation, &generator_generation,
                       &exec_generation, &pair_generation, &judge_generation);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, &gid) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql) - 1U, &sql_ref) ==
         QST_OK);
  assert(qst_block_write_seal(&mapping, &sql_ref, sql, sizeof(sql) - 1U) ==
         QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql) - 1U, &record_ref) ==
         QST_OK);
  assert(qst_block_write_seal(&mapping, &record_ref, sql, sizeof(sql) - 1U) ==
         QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_RANDOM;
  input.focus = QST_FOCUS_COVERAGE;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 17;
  input.weight_profile_version = 3;
  input.sql_ref = sql_ref;
  input.generation_record_ref = record_ref;
  assert(qst_block_sha256(&mapping, record_ref, gid,
                          &input.generation_record_sha256) == QST_OK);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input, &dispatch_epoch) ==
         QST_OK);
  message.message_type = QST_MSG_GENERATE_WORK;
  message.producer_role = QST_ROLE_GENERATOR;
  message.producer_generation = generator_generation;
  message.target = input.target;
  message.run_id = mapping.header->run_id;
  message.gid = gid;
  message.dispatch_epoch = dispatch_epoch;
  message.transition_seq = 1;
  message.body.generate = input;
  assert(qst_queue_publish(&mapping, QST_QUEUE_GENERATE, &message,
                           UINT64_MAX) == QST_OK);
  assert(qst_queue_claim(&mapping, QST_QUEUE_GENERATE,
                         QST_ROLE_EXEC_MANAGER, exec_generation, UINT64_MAX,
                         &claim) == QST_E_INVALID_ARGUMENT);
  assert(qst_queue_claim_instance(
             &mapping, QST_QUEUE_GENERATE, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, UINT64_MAX, &claim) == QST_OK);
  assert(qst_task_claim_dispatch_pair(
             &mapping, gid, 0, pair_generation, dispatch_epoch,
             &claim.message.body.generate, &dispatch) == QST_OK);
  transition_seq = dispatch.transition_seq;
  entry = (SQstTaskEntry *)((uint8_t *)mapping.base +
                            mapping.header->task_region.region_offset);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_EXECUTING);
  assert(qst_task_abort_dispatch(&mapping, gid, dispatch_epoch,
                                 transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
                                 pair_generation + 1U) == QST_E_STALE_OWNER);
  assert(qst_task_abort_dispatch(&mapping, gid, dispatch_epoch,
                                 transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
                                 pair_generation) == QST_OK);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_GENERATED);
  assert(entry->gid == gid && entry->sql_ref.offset == sql_ref.offset &&
         entry->generation_record_ref.offset == record_ref.offset);
  assert(qst_block_validate(&mapping, sql_ref, gid) == QST_OK);
  assert(qst_block_validate(&mapping, record_ref, gid) == QST_OK);
  assert(qst_queue_abandon(&mapping, &claim, QST_E_RESOURCE) == QST_OK);
  message.message_id = 0;
  message.message_crc32c = 0;
  assert(qst_queue_publish(&mapping, QST_QUEUE_GENERATE, &message,
                           UINT64_MAX) == QST_OK);
  assert(qst_queue_claim_instance(
             &mapping, QST_QUEUE_GENERATE, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, UINT64_MAX, &claim) == QST_OK);
  assert(qst_task_claim_dispatch_pair(
             &mapping, gid, 0, pair_generation, dispatch_epoch,
             &claim.message.body.generate, &dispatch) == QST_OK);
  transition_seq = dispatch.transition_seq;
  assert(qst_task_abort_dispatch(&mapping, gid, dispatch_epoch,
                                 transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
                                 pair_generation + 1U) == QST_E_STALE_OWNER);
  assert(qst_task_abort_dispatch(&mapping, gid, dispatch_epoch,
                                 transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
                                 pair_generation) == QST_OK);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_GENERATED);
  memset(&dispatch, 0, sizeof(dispatch));
  assert(qst_task_find_assigned_pair(&mapping, 0, pair_generation,
                                     &dispatch) == QST_E_QUEUE_EMPTY);
  assert(dispatch.gid == 0);
  assert(qst_queue_abandon(&mapping, &claim, QST_E_RESOURCE) == QST_OK);
}

static void qstTestPairSlotOwnershipIsolation(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t sql[] = "SELECT 1";
  SQstShmMapping mapping = {0};
  SQstGeneratePayload input = {0};
  SQstExecutePayload final_payload = {0};
  SQstTaskDispatch dispatch = {0};
  SQstRef sql_ref = {0};
  SQstRef record_ref = {0};
  uint64_t gid = 0;
  uint64_t dispatch_epoch = 0;
  uint64_t transition_seq = 0;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_zero_generation = 0;
  uint32_t pair_one_generation = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              100, 100, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 101, 101,
                              &generator_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, 102, 102,
                              &exec_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 0, 103, 103,
             &pair_zero_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_EXEC_MANAGER, 0, exec_generation,
             QST_ROLE_PAIR_SUPERVISOR, 1, 104, 104,
             &pair_one_generation) == QST_OK);
  assert(pair_zero_generation == pair_one_generation);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, &gid) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql), &sql_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &sql_ref, sql, sizeof(sql)) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql), &record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &record_ref, sql, sizeof(sql)) == QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_FULL;
  input.focus = QST_FOCUS_CORRECTNESS;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 1;
  input.weight_profile_version = 1;
  input.sql_ref = sql_ref;
  input.generation_record_ref = record_ref;
  assert(qst_block_sha256(&mapping, record_ref, gid,
                          &input.generation_record_sha256) == QST_OK);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input,
                         &dispatch_epoch) == QST_OK);
  assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                 exec_generation, dispatch_epoch, &input,
                                 &transition_seq) == QST_OK);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, dispatch_epoch, transition_seq,
                              1, pair_one_generation) == QST_OK);
  assert(qst_task_find_assigned_pair(&mapping, 0, pair_zero_generation,
                                     &dispatch) == QST_E_QUEUE_EMPTY);
  assert(qst_task_find_assigned_pair(&mapping, 1, pair_one_generation,
                                     &dispatch) == QST_OK);
  assert(dispatch.gid == gid);
  assert(qst_task_publish_final(&mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_PAIR_SUPERVISOR, 0,
                                pair_zero_generation, &final_payload) ==
         QST_E_STALE_OWNER);
  assert(qst_task_publish_final(&mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_PAIR_SUPERVISOR, 1,
                                pair_one_generation, &final_payload) == QST_OK);
}

static void qstTestTaskTimeoutRetryEpochIsolation(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  const uint8_t sql[] = "SELECT 1";
  SQstShmMapping mapping = {0};
  SQstGeneratePayload input = {0};
  SQstExecutePayload timeout_payload = {0};
  SQstTaskDispatch retry_dispatch = {0};
  SQstTaskEntry *entry = NULL;
  SQstRef sql_ref = {0};
  SQstRef record_ref = {0};
  uint64_t gid = 0;
  uint64_t dispatch_epoch = 0;
  uint64_t transition_seq = 0;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  uint32_t pair_generation = 0;
  uint32_t judge_generation = 0;
  uint32_t previous_pair_slot = 0;
  uint32_t previous_pair_generation = 0;
  int32_t recycle_required = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  qstRegisterTaskRoles(&mapping, &main_generation, &generator_generation,
                       &exec_generation, &pair_generation, &judge_generation);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, &gid) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql), &sql_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &sql_ref, sql, sizeof(sql)) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql), &record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &record_ref, sql, sizeof(sql)) == QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_FULL;
  input.focus = QST_FOCUS_CORRECTNESS;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 1;
  input.weight_profile_version = 1;
  input.sql_ref = sql_ref;
  input.generation_record_ref = record_ref;
  assert(qst_block_sha256(&mapping, record_ref, gid,
                          &input.generation_record_sha256) == QST_OK);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input,
                         &dispatch_epoch) == QST_OK);
  assert(qst_task_claim_dispatch(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                                 exec_generation, dispatch_epoch, &input,
                                 &transition_seq) == QST_OK);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, dispatch_epoch, transition_seq,
                              0, pair_generation) == QST_OK);
  timeout_payload.execution_status = QST_EXEC_TIMEOUT;
  assert(qst_task_publish_final(&mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_PAIR_SUPERVISOR, 0, pair_generation,
                                &timeout_payload) == QST_OK);
  entry = (SQstTaskEntry *)(memory + kTestTaskOffset);
  assert(qst_task_retry_timeout(
             &mapping, gid, dispatch_epoch, transition_seq,
             exec_generation + 1, 2, &retry_dispatch, &recycle_required,
             &previous_pair_slot, &previous_pair_generation) == QST_E_STALE_OWNER);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_FINAL);
  assert(qst_task_retry_timeout(
             &mapping, gid, dispatch_epoch, transition_seq, exec_generation,
             2, &retry_dispatch, &recycle_required, &previous_pair_slot,
             &previous_pair_generation) == QST_OK);
  assert(retry_dispatch.gid == gid);
  assert(retry_dispatch.dispatch_epoch == dispatch_epoch + 1U);
  assert(retry_dispatch.transition_seq == transition_seq + 1U);
  assert(recycle_required == 0);
  assert(previous_pair_slot == 0);
  assert(previous_pair_generation == pair_generation);
  assert(atomic_load_explicit(&entry->timeout_attempt, memory_order_acquire) == 1);
  assert(qst_task_publish_final(&mapping, gid, dispatch_epoch, transition_seq,
                                QST_ROLE_PAIR_SUPERVISOR, 0, pair_generation,
                                &timeout_payload) == QST_E_STALE_OWNER);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, retry_dispatch.dispatch_epoch,
                              retry_dispatch.transition_seq, 0,
                              pair_generation) == QST_OK);
  assert(qst_task_publish_final(
             &mapping, gid, retry_dispatch.dispatch_epoch,
             retry_dispatch.transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, &timeout_payload) == QST_OK);
  dispatch_epoch = retry_dispatch.dispatch_epoch;
  transition_seq = retry_dispatch.transition_seq;
  assert(qst_task_retry_timeout(
             &mapping, gid, dispatch_epoch, transition_seq, exec_generation,
             2, &retry_dispatch, &recycle_required, &previous_pair_slot,
             &previous_pair_generation) == QST_OK);
  assert(recycle_required == 1);
  assert(retry_dispatch.dispatch_epoch == dispatch_epoch + 1U);
  assert(retry_dispatch.transition_seq == transition_seq + 1U);
  assert(qst_task_assign_pair(&mapping, gid, QST_ROLE_EXEC_MANAGER,
                              exec_generation, retry_dispatch.dispatch_epoch,
                              retry_dispatch.transition_seq, 0,
                              pair_generation) == QST_OK);
  assert(qst_task_publish_final(
             &mapping, gid, retry_dispatch.dispatch_epoch,
             retry_dispatch.transition_seq, QST_ROLE_PAIR_SUPERVISOR, 0,
             pair_generation, &timeout_payload) == QST_OK);
  assert(qst_task_retry_timeout(
             &mapping, gid, retry_dispatch.dispatch_epoch,
             retry_dispatch.transition_seq, exec_generation, 2,
             &retry_dispatch, &recycle_required, &previous_pair_slot,
             &previous_pair_generation) == QST_E_DEADLINE);
  assert(atomic_load_explicit(&entry->phase, memory_order_acquire) ==
         QST_TASK_FINAL);
  assert(atomic_load_explicit(&entry->timeout_attempt, memory_order_acquire) == 3);
  assert(entry->final_payload.execution_status == QST_EXEC_TIMEOUT);
}

static void qstTestPairDeadlineRecovery(void) {
  const uint8_t sql[] = "SELECT 1";
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstGeneratePayload input = {0};
  SQstMessage message = {0};
  SQstTaskEntry *entries = NULL;
  struct pollfd event = {0};
  char eventfd_text[32] = {0};
  uint64_t gid = 0;
  uint64_t dispatch_epoch = 0;
  uint64_t event_count = 0;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t exec_generation = 0;
  int32_t event_fd = -1;
  int32_t generation_pipe[2] = {-1, -1};
  int32_t child_code = QST_E_INTERNAL;
  int32_t wait_status = 0;
  pid_t child = -1;
  SQstRef sql_ref = {0};
  SQstRef record_ref = {0};

  config.run_id = UINT64_C(6000000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 8;
  config.task_entry_count = 16;
  config.ai_action_entry_count = 2;
  config.queue_capacity = 16;
  config.block_bytes = 256;
  config.block_count = 256;
  config.execution_pair_count = 1;
  config.judge_worker_count = 1;
  config.statement_timeout_seconds = 1;
  config.statement_retry_count = 2;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, (int32_t)getpid(),
                              (int32_t)getpgrp(), &generator_generation) ==
         QST_OK);
  assert(qst_task_reserve_gid(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, &gid) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql), &sql_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &sql_ref, sql, sizeof(sql)) == QST_OK);
  assert(qst_block_alloc(&mapping, gid, 1, sizeof(sql), &record_ref) == QST_OK);
  assert(qst_block_write_seal(&mapping, &record_ref, sql, sizeof(sql)) == QST_OK);
  input.target = QST_TARGET_TD;
  input.mode = QST_MODE_FULL;
  input.focus = QST_FOCUS_COVERAGE;
  input.dialect = QST_DIALECT_TDENGINE;
  input.generation_seq = 1;
  input.weight_profile_version = 1;
  input.sql_ref = sql_ref;
  input.generation_record_ref = record_ref;
  assert(qst_block_sha256(&mapping, record_ref, gid,
                          &input.generation_record_sha256) == QST_OK);
  assert(qst_task_create(&mapping, gid, QST_ROLE_GENERATOR,
                         generator_generation, &input,
                         &dispatch_epoch) == QST_OK);
  message.message_type = QST_MSG_GENERATE_WORK;
  message.producer_role = QST_ROLE_GENERATOR;
  message.producer_generation = generator_generation;
  message.target = QST_TARGET_TD;
  message.run_id = config.run_id;
  message.gid = gid;
  message.dispatch_epoch = dispatch_epoch;
  message.transition_seq = 1;
  message.body.generate = input;
  assert(qst_queue_publish(&mapping, QST_QUEUE_GENERATE, &message,
                           UINT64_MAX) == QST_OK);
  event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
  assert(event_fd >= 0);
  assert(snprintf(eventfd_text, sizeof(eventfd_text), "%d", event_fd) > 0);
  assert(setenv("QST_FAKE_TAOS_NO_CALLBACK", "1", 1) == 0);
  assert(setenv("QST_FAKE_TAOS_KILL_EVENTFD", eventfd_text, 1) == 0);
  assert(pipe(generation_pipe) == 0);
  child = fork();
  assert(child >= 0);
  if (child == 0) {
    uint32_t child_generation = 0;

    (void)close(generation_pipe[1]);
    if (read(generation_pipe[0], &child_generation, sizeof(child_generation)) !=
            (ssize_t)sizeof(child_generation) ||
        child_generation == 0) {
      _exit(1);
    }
    (void)close(generation_pipe[0]);
    child_code = qst_exec_manager_run(&mapping, child_generation);
    _exit(child_code == QST_OK ? 0 : 1);
  }
  (void)close(generation_pipe[0]);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_EXEC_MANAGER, (int32_t)child,
                              (int32_t)child, &exec_generation) == QST_OK);
  assert(write(generation_pipe[1], &exec_generation, sizeof(exec_generation)) ==
         (ssize_t)sizeof(exec_generation));
  assert(close(generation_pipe[1]) == 0);
  event.fd = event_fd;
  event.events = POLLIN;
  assert(poll(&event, 1, kTestPairDeadlineEventTimeoutMilliseconds) == 1);
  assert(read(event_fd, &event_count, sizeof(event_count)) ==
         (ssize_t)sizeof(event_count));
  while (event_count < 2U) {
    assert(poll(&event, 1, kTestPairDeadlineEventTimeoutMilliseconds) == 1);
    {
      uint64_t additional_events = 0;

      assert(read(event_fd, &additional_events, sizeof(additional_events)) ==
             (ssize_t)sizeof(additional_events));
      event_count += additional_events;
    }
  }
  entries = (SQstTaskEntry *)((uint8_t *)mapping.base +
                              mapping.header->task_region.region_offset);
  assert(entries[0].gid == gid);
  assert(atomic_load_explicit(&entries[0].phase, memory_order_acquire) ==
         QST_TASK_EXECUTING);
  assert(atomic_load_explicit(&entries[0].dispatch_epoch,
                              memory_order_acquire) == dispatch_epoch + 1U);
  assert(atomic_load_explicit(&entries[0].timeout_attempt,
                              memory_order_acquire) == 0);
  assert(atomic_load_explicit(&entries[0].owner_role, memory_order_acquire) ==
         QST_ROLE_PAIR_SUPERVISOR);
  assert(atomic_load_explicit(&entries[0].owner_generation,
                              memory_order_acquire) >= 2U);
  assert(qst_process_request_stop(&mapping, QST_ROLE_MAIN, main_generation,
                                  QST_ROLE_EXEC_MANAGER, exec_generation, 1) ==
         QST_OK);
  assert(waitpid(child, &wait_status, 0) == child);
  assert(WIFEXITED(wait_status) && WEXITSTATUS(wait_status) == 0);
  assert(unsetenv("QST_FAKE_TAOS_KILL_EVENTFD") == 0);
  assert(unsetenv("QST_FAKE_TAOS_NO_CALLBACK") == 0);
  assert(close(event_fd) == 0);
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

static void qstTestProcessRegistrationAndReaping(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstProcessSlot *slots = NULL;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              100, 100, &main_generation) == QST_OK);
  assert(main_generation == 1);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_MAIN, main_generation, QST_FLOW_STAGE_STARTUP,
             QST_FLOW_STATE_CONTROL_QUEUE, 1, 0, UINT64_MAX) == QST_OK);
  slots = (SQstProcessSlot *)(memory + kTestProcessOffset);
  assert(atomic_load_explicit(&slots[0].flow_stage, memory_order_acquire) ==
         QST_FLOW_STAGE_STARTUP);
  assert(atomic_load_explicit(&slots[0].flow_state_kind,
                              memory_order_acquire) ==
         QST_FLOW_STATE_CONTROL_QUEUE);
  assert(atomic_load_explicit(&slots[0].flow_observed_state,
                              memory_order_acquire) == 1);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_MAIN, main_generation, 0,
             QST_FLOW_STATE_CONTROL_QUEUE, 1, 0, 0) == QST_E_INVALID_ARGUMENT);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_MAIN, main_generation, QST_FLOW_STAGE_IDLE,
             QST_FLOW_STATE_CONTROL_QUEUE, 1, 99, 0) == QST_E_INVALID_ARGUMENT);
  assert(atomic_load_explicit(&slots[0].flow_seq, memory_order_acquire) == 1);
  assert(qst_process_record_business(&mapping, QST_ROLE_MAIN, main_generation,
                                     99, UINT64_MAX) == QST_OK);
  assert(atomic_load_explicit(&slots[0].business_seq, memory_order_acquire) ==
         1);
  assert(atomic_load_explicit(&slots[0].flow_seq, memory_order_acquire) == 1);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 101, 101,
                              &generator_generation) == QST_OK);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_LAUNCHER, 1,
                                     QST_ROLE_GENERATOR, generator_generation,
                                     0) == QST_E_STALE_OWNER);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_MAIN, main_generation,
                                     QST_ROLE_GENERATOR, generator_generation,
                                     0) == QST_OK);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_GENERATOR, generator_generation,
             QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_INPUT_QUEUE, 2, 0,
             UINT64_MAX) == QST_E_STALE_OWNER);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 102, 102,
                              &generator_generation) == QST_OK);
  assert(generator_generation == 2);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_GENERATOR, generator_generation,
             QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_INPUT_QUEUE, 3, 0,
             UINT64_MAX) == QST_OK);
}

/* AUTO instances must recycle a matching reaped slot so a long-running pair
 * replacement loop cannot exhaust the bounded process registry. */
static void qstTestProcessAutoSlotReuse(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  uint32_t main_generation = 0;
  uint32_t supervisor_generation = 0;
  uint32_t first_taosd_slot = QST_PROCESS_SLOT_AUTO;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_LAUNCHER, 0, 1, QST_ROLE_MAIN, 0, 100, 100,
             &main_generation) == QST_OK);
  assert(qst_process_register_instance(
             &mapping, QST_ROLE_MAIN, 0, main_generation,
             QST_ROLE_PAIR_SUPERVISOR, 0, 101, 101,
             &supervisor_generation) == QST_OK);
  for (uint32_t iteration = 0; iteration < kTestProcessCount * 2U;
       ++iteration) {
    uint32_t taosd_slot = QST_PROCESS_SLOT_AUTO;
    uint32_t taosd_generation = 0;

    assert(qst_process_register_instance_ex(
               &mapping, QST_ROLE_PAIR_SUPERVISOR, 0, supervisor_generation,
               QST_ROLE_TAOSD, QST_PROCESS_SLOT_AUTO,
               (int32_t)(200U + iteration), (int32_t)(200U + iteration),
               &taosd_slot, &taosd_generation) == QST_OK);
    if (iteration == 0) {
      first_taosd_slot = taosd_slot;
    } else {
      assert(taosd_slot == first_taosd_slot);
    }
    assert(qst_process_record_business_instance(
               &mapping, QST_ROLE_TAOSD, taosd_slot, taosd_generation,
               (uint64_t)iteration + 1U, UINT64_MAX) == QST_OK);
    assert(qst_process_reap_generation_instance(
               &mapping, QST_ROLE_PAIR_SUPERVISOR, 0, supervisor_generation,
               QST_ROLE_TAOSD, taosd_slot, taosd_generation, 0) == QST_OK);
    assert(qst_process_record_business_instance(
               &mapping, QST_ROLE_TAOSD, taosd_slot, taosd_generation,
               (uint64_t)iteration + 1U, UINT64_MAX) == QST_E_INVALID_ARGUMENT);
  }
}

static void qstTestProcessStopProtocol(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstProcessIdentity children[2] = {0};
  SQstProcessSlot *slots = NULL;
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t child_count = 0;
  uint32_t stop_generation = 0;
  uint32_t exit_state = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              100, 100, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 101, 101,
                              &generator_generation) == QST_OK);
  assert(qst_process_list_direct_children(&mapping, QST_ROLE_MAIN,
                                          main_generation, NULL, 0,
                                          &child_count) == QST_E_RESOURCE);
  assert(child_count == 1);
  assert(qst_process_list_direct_children(&mapping, QST_ROLE_MAIN,
                                          main_generation, children, 2,
                                          &child_count) == QST_OK);
  assert(child_count == 1);
  assert(children[0].role == QST_ROLE_GENERATOR);
  assert(children[0].generation == generator_generation);
  assert(children[0].pid == 101);
  assert(qst_process_request_stop(&mapping, QST_ROLE_LAUNCHER, 1,
                                  QST_ROLE_GENERATOR, generator_generation,
                                  1) == QST_E_STALE_OWNER);
  assert(qst_process_request_stop(&mapping, QST_ROLE_MAIN, main_generation,
                                  QST_ROLE_GENERATOR, generator_generation,
                                  1) == QST_OK);
  assert(qst_process_get_stop_generation(&mapping, QST_ROLE_GENERATOR,
                                         generator_generation,
                                         &stop_generation, &exit_state) == QST_OK);
  assert(stop_generation == 1);
  assert(exit_state == QST_PROCESS_STOP_REQUESTED);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_GENERATOR, generator_generation,
             QST_FLOW_STAGE_IDLE, QST_FLOW_STATE_INPUT_QUEUE, 4, 0, 0) ==
         QST_E_STALE_OWNER);
  assert(qst_process_confirm_flow(
             &mapping, QST_ROLE_GENERATOR, generator_generation,
             QST_FLOW_STAGE_STOPPING, QST_FLOW_STATE_CONTROL_QUEUE, 5, 0,
             0) == QST_OK);
  slots = (SQstProcessSlot *)(memory + kTestProcessOffset);
  assert(atomic_load_explicit(&slots[1].flow_stage, memory_order_acquire) ==
         QST_FLOW_STAGE_STOPPING);
  assert(atomic_load_explicit(&slots[1].flow_observed_state,
                              memory_order_acquire) == 5);
  assert(qst_process_ack_stop(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, 2) == QST_E_STALE_OWNER);
  assert(qst_process_ack_stop(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, 1) == QST_OK);
  assert(qst_process_ack_stop(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, 1) == QST_OK);
  assert(qst_process_mark_exited(&mapping, QST_ROLE_GENERATOR,
                                 generator_generation, 0) == QST_OK);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_MAIN, main_generation,
                                     QST_ROLE_GENERATOR, generator_generation,
                                     0) == QST_OK);
  assert(qst_process_list_direct_children(&mapping, QST_ROLE_MAIN,
                                          main_generation, children, 2,
                                          &child_count) == QST_OK);
  assert(child_count == 0);

  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 102, 102,
                              &generator_generation) == QST_OK);
  assert(qst_process_request_stop(&mapping, QST_ROLE_MAIN, main_generation,
                                  QST_ROLE_GENERATOR, generator_generation,
                                  1) == QST_OK);
  assert(qst_process_mark_exited(&mapping, QST_ROLE_GENERATOR,
                                 generator_generation, 0) == QST_OK);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_MAIN, main_generation,
                                     QST_ROLE_GENERATOR, generator_generation,
                                     0) == QST_OK);
}

static void qstTestProcessDescendantPostorderAndAdoption(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstProcessIdentity descendants[4] = {0};
  uint32_t main_generation = 0;
  uint32_t generator_generation = 0;
  uint32_t supervisor_generation = 0;
  uint32_t judge_generation = 0;
  uint32_t descendant_count = 0;

  qstInitMapping(&mapping, memory, sizeof(memory));
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              100, 100, &main_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_GENERATOR, 101, 101,
                              &generator_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_GENERATOR,
                              generator_generation, QST_ROLE_PAIR_SUPERVISOR,
                              102, 102, &supervisor_generation) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_MAIN, main_generation,
                              QST_ROLE_JUDGE_MANAGER, 103, 103,
                              &judge_generation) == QST_OK);
  assert(qst_process_list_descendants_postorder(&mapping, QST_ROLE_LAUNCHER,
                                                1, NULL, 0,
                                                &descendant_count) ==
         QST_E_RESOURCE);
  assert(descendant_count == 4);
  assert(qst_process_list_descendants_postorder(
             &mapping, QST_ROLE_LAUNCHER, 1, descendants,
             sizeof(descendants) / sizeof(descendants[0]),
             &descendant_count) == QST_OK);
  assert(descendant_count == 4);
  assert(descendants[0].role == QST_ROLE_PAIR_SUPERVISOR);
  assert(descendants[0].generation == supervisor_generation);
  assert(descendants[1].role == QST_ROLE_GENERATOR);
  assert(descendants[1].generation == generator_generation);
  assert(descendants[2].role == QST_ROLE_JUDGE_MANAGER);
  assert(descendants[2].generation == judge_generation);
  assert(descendants[3].role == QST_ROLE_MAIN);
  assert(descendants[3].generation == main_generation);
  assert(qst_process_reap_orphaned_generation(
             &mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_GENERATOR,
             generator_generation, 0) == QST_E_STALE_OWNER);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_GENERATOR,
                                     generator_generation,
                                     QST_ROLE_PAIR_SUPERVISOR,
                                     supervisor_generation, 0) == QST_OK);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_MAIN, main_generation,
                                     QST_ROLE_JUDGE_MANAGER,
                                     judge_generation, 0) == QST_OK);
  assert(qst_process_reap_generation(&mapping, QST_ROLE_LAUNCHER, 1,
                                     QST_ROLE_MAIN, main_generation, 0) == QST_OK);
  assert(qst_process_reap_orphaned_generation(
             &mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_GENERATOR,
             generator_generation, 0) == QST_OK);
}

static void qstTestQueueStallObserverAndRestartBudget(void) {
  uint8_t memory[kTestMemoryBytes] = {0};
  SQstShmMapping mapping = {0};
  SQstQueueStallObserver observer = {0};
  SQstRestartBudget budget = {0};
  SQstMessage message = {0};
  int32_t stalled = 0;

  assert(QST_DRAIN_LEASE_MILLISECONDS > QST_FLOW_LEASE_MILLISECONDS);
  qstInitMapping(&mapping, memory, sizeof(memory));
  message = qstControlMessage(mapping.header->run_id);
  assert(qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message,
                           UINT64_MAX) == QST_OK);
  assert(qst_supervisor_observe_queue(&mapping, QST_QUEUE_CONTROL, 1, 0, 100,
                                      &observer, &stalled) == QST_OK);
  assert(stalled == 0);
  assert(qst_supervisor_observe_queue(&mapping, QST_QUEUE_CONTROL, 1, 0, 101,
                                      &observer, &stalled) == QST_OK);
  assert(stalled == 0);
  assert(observer.stall_deadline_mono_ms ==
         101 + QST_QUEUE_STALL_MILLISECONDS);
  assert(qst_supervisor_observe_queue(
             &mapping, QST_QUEUE_CONTROL, 1, 0,
             100 + QST_QUEUE_STALL_MILLISECONDS, &observer, &stalled) == QST_OK);
  assert(stalled == 0);
  assert(qst_supervisor_observe_queue(
             &mapping, QST_QUEUE_CONTROL, 1, 0,
             101 + QST_QUEUE_STALL_MILLISECONDS, &observer, &stalled) == QST_OK);
  assert(stalled == 1);
  assert(qst_supervisor_observe_queue(&mapping, QST_QUEUE_CONTROL, 1, 1,
                                      130000, &observer, &stalled) == QST_OK);
  assert(stalled == 0 && observer.stall_deadline_mono_ms == 0);
  assert(qst_supervisor_observe_queue(&mapping, QST_QUEUE_CONTROL, 0, 0,
                                      130001, &observer, &stalled) == QST_OK);
  assert(stalled == 0 && observer.stall_deadline_mono_ms == 0);
  for (uint32_t index = 0; index < QST_MANAGER_RESTART_LIMIT; ++index) {
    assert(qst_supervisor_consume_restart(
               &budget, 1000, QST_MANAGER_RESTART_WINDOW_MILLISECONDS,
               QST_MANAGER_RESTART_LIMIT) == QST_OK);
  }
  assert(qst_supervisor_consume_restart(
             &budget, 1000, QST_MANAGER_RESTART_WINDOW_MILLISECONDS,
             QST_MANAGER_RESTART_LIMIT) == QST_E_RESOURCE);
  assert(qst_supervisor_consume_restart(
             &budget, 1000 + QST_MANAGER_RESTART_WINDOW_MILLISECONDS,
             QST_MANAGER_RESTART_WINDOW_MILLISECONDS,
             QST_MANAGER_RESTART_LIMIT) == QST_OK);
  qst_supervisor_reset_restart(&budget);
  assert(budget.restart_count == 0 && budget.window_started_mono_ms == 0);
}

static void qstTestExternalMappingValidation(void) {
  char root[] = "/tmp/taosQueryTest-mapping-XXXXXX";
  char rules_path[512] = {0};
  char schema_path[512] = {0};
  char invalid_path[512] = {0};
  char source_rules[512] = {0};
  char source_schema[512] = {0};
  FILE *source = NULL;
  FILE *destination = NULL;
  uint8_t buffer[1024] = {0};
  size_t bytes = 0;
  EQstCode invalid_code = QST_OK;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(source_rules, sizeof(source_rules), "%s/%s",
                  QST_DATASET_SOURCE_DIR, "external-mapping-rules.json") > 0);
  assert(snprintf(source_schema, sizeof(source_schema), "%s/%s",
                  QST_DATASET_SOURCE_DIR,
                  "external-mapping-rules.schema.json") > 0);
  assert(snprintf(rules_path, sizeof(rules_path), "%s/rules.json", root) > 0);
  assert(snprintf(schema_path, sizeof(schema_path), "%s/schema.json", root) > 0);
  source = fopen(source_rules, "rb");
  destination = fopen(rules_path, "wb");
  assert(source != NULL && destination != NULL);
  while ((bytes = fread(buffer, 1, sizeof(buffer), source)) != 0) {
    assert(fwrite(buffer, 1, bytes, destination) == bytes);
  }
  assert(fclose(destination) == 0);
  assert(fclose(source) == 0);
  source = fopen(source_schema, "rb");
  destination = fopen(schema_path, "wb");
  assert(source != NULL && destination != NULL);
  while ((bytes = fread(buffer, 1, sizeof(buffer), source)) != 0) {
    assert(fwrite(buffer, 1, bytes, destination) == bytes);
  }
  assert(fclose(destination) == 0);
  assert(fclose(source) == 0);
  assert(qst_mapping_validate_files(rules_path, schema_path) == QST_OK);
  assert(snprintf(invalid_path, sizeof(invalid_path), "%s/invalid.json", root) >
         0);
  destination = fopen(invalid_path, "w");
  assert(destination != NULL);
  assert(fputs("{\"version\":1,\"dataset_schema\":\"taosQueryTest-default-v1\",\"targets\":[],\"unexpected\":0}",
               destination) >= 0);
  assert(fclose(destination) == 0);
  invalid_code = qst_mapping_validate_files(invalid_path, schema_path);
  assert(invalid_code == QST_E_INVALID_ARGUMENT);
  destination = fopen(invalid_path, "w");
  assert(destination != NULL);
  assert(fputs("{}", destination) >= 0);
  assert(fclose(destination) == 0);
  assert(qst_mapping_validate_files(rules_path, invalid_path) ==
         QST_E_INVALID_ARGUMENT);
  assert(unlink(invalid_path) == 0);
  assert(unlink(schema_path) == 0);
  assert(unlink(rules_path) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestInvalidMappingPreventsExternalScript(void) {
  char root[] = "/tmp/taosQueryTest-mapping-gate-XXXXXX";
  char dataset_dir[512] = {0};
  char result_dir[512] = {0};
  char path[512] = {0};
  char marker_path[512] = {0};
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};

  assert(mkdtemp(root) != NULL);
  assert(snprintf(dataset_dir, sizeof(dataset_dir), "%s/dataset",
                            root) > 0);
  assert(mkdir(dataset_dir, 0750) == 0);
  assert(snprintf(result_dir, sizeof(result_dir), "%s/result", root) >
                   0);
  assert(mkdir(result_dir, 0750) == 0);
  assert(snprintf(path, sizeof(path), "%s/taosbenchmark.json", dataset_dir) > 0);
  {
    FILE *file = fopen(path, "w");
    assert(file != NULL);
    assert(fputs("{\"fixture\":\"qst-local-fixture-v1\"}\n", file) >= 0);
    assert(fclose(file) == 0);
  }
  assert(snprintf(path, sizeof(path), "%s/find-client.sh", dataset_dir) > 0);
  {
    FILE *file = fopen(path, "w");
    assert(file != NULL);
    assert(fputs("#!/bin/sh\nexit 0\n", file) >= 0);
    assert(fclose(file) == 0);
  }
  assert(snprintf(path, sizeof(path), "%s/external-mapping-rules.json",
                            dataset_dir) > 0);
  {
    FILE *file = fopen(path, "w");
    assert(file != NULL);
    assert(fputs("{}", file) >= 0);
    assert(fclose(file) == 0);
  }
  assert(snprintf(path, sizeof(path),
                            "%s/external-mapping-rules.schema.json",
                            dataset_dir) > 0);
  {
    FILE *file = fopen(path, "w");
    assert(file != NULL);
    assert(fputs("{}", file) >= 0);
    assert(fclose(file) == 0);
  }
  assert(snprintf(path, sizeof(path), "%s/prepare-postgresql-data.sh",
                            dataset_dir) > 0);
  assert(snprintf(marker_path, sizeof(marker_path), "%s/script-ran",
                            root) > 0);
  {
    FILE *file = fopen(path, "w");
    assert(file != NULL);
    assert(fprintf(file, "touch '%s'\n", marker_path) > 0);
    assert(fclose(file) == 0);
  }
  assert(snprintf(path, sizeof(path), "%s/cleanup-postgresql-data.sh",
                  dataset_dir) > 0);
  {
    FILE *file = fopen(path, "w");
    assert(file != NULL);
    assert(fprintf(file, "touch '%s'\n", marker_path) > 0);
    assert(fclose(file) == 0);
  }
  config.run_id = UINT64_C(7000000000) + (uint64_t)getpid();
  config.target_selection_bits = UINT32_C(1) << 2;
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 8;
  config.task_entry_count = 8;
  config.ai_action_entry_count = 2;
  config.queue_capacity = 4;
  config.block_bytes = 256;
  config.block_count = 256;
  config.tool_root = root;
  config.result_dir = result_dir;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_dataset_prepare(&mapping, &config,
                                       &(SQstDatasetInfo){0}) ==
                   QST_E_INVALID_ARGUMENT);
  assert(access(marker_path, F_OK) != 0);
  assert(qst_shm_unmap(&mapping) == QST_OK);
  assert(snprintf(path, sizeof(path), "%s/dataset", result_dir) > 0);
  {
    DIR *directory = opendir(path);
    struct dirent *entry = NULL;

    assert(directory != NULL);
    while ((entry = readdir(directory)) != NULL) {
      char copied_path[512] = {0};

      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      assert(snprintf(copied_path, sizeof(copied_path), "%s/%s", path,
                                entry->d_name) > 0);
      assert(unlink(copied_path) == 0);
    }
    assert(closedir(directory) == 0);
  }
  assert(rmdir(path) == 0);
  assert(rmdir(result_dir) == 0);
  assert(snprintf(path, sizeof(path), "%s/external-mapping-rules.json",
                            dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path),
                            "%s/external-mapping-rules.schema.json",
                            dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/prepare-postgresql-data.sh",
                            dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/cleanup-postgresql-data.sh",
                  dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/taosbenchmark.json", dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(snprintf(path, sizeof(path), "%s/find-client.sh", dataset_dir) > 0);
  assert(unlink(path) == 0);
  assert(rmdir(dataset_dir) == 0);
  assert(rmdir(root) == 0);
}

static void qstTestProviderPreflightManifest(void) {
  SQstRunConfig config = {0};
  char root[] = "/tmp/taosQueryTest-provider-XXXXXX";
  char dataset_dir[512] = {0};
  char manifest_path[512] = {0};
  char fake_bin[512] = {0};
  char fake_mysql[512] = {0};
  char fake_curl[512] = {0};
  char curl_log[512] = {0};
  char original_path[4096] = {0};
  char original_mysql_client[512] = {0};
  char original_influx_client[512] = {0};
  char original_influx_host[512] = {0};
  char original_influx_port[512] = {0};
  char original_influx_token[512] = {0};
  char original_curl_stub_log[512] = {0};
  char contents[8192] = {0};
  FILE *file = NULL;
  int32_t had_path = 0;
  int32_t had_mysql_client = 0;
  int32_t had_influx_client = 0;
  int32_t had_influx_host = 0;
  int32_t had_influx_port = 0;
  int32_t had_influx_token = 0;
  int32_t had_curl_stub_log = 0;

  assert(mkdtemp(root) != NULL);
  assert(snprintf(dataset_dir, sizeof(dataset_dir), "%s/dataset", root) > 0);
  assert(mkdir(dataset_dir, 0750) == 0);
  assert(snprintf(manifest_path, sizeof(manifest_path),
                  "%s/provider-readiness.json", dataset_dir) > 0);
  config.run_id = 987654321;
  config.result_dir = root;
  config.target_selection_bits = UINT32_C(1);
  assert(qst_provider_preflight(&config) == QST_OK);
  assert(access(manifest_path, F_OK) != 0);

  (void)unsetenv("FQ_MYSQL_HOST");
  (void)unsetenv("FQ_MYSQL_PORT");
  (void)unsetenv("FQ_MYSQL_USER");
  (void)unsetenv("FQ_MYSQL_PASSWORD");
  qstTestSaveEnvironment("QST_MYSQL_CLIENT", original_mysql_client,
                         sizeof(original_mysql_client), &had_mysql_client);
  (void)unsetenv("QST_MYSQL_CLIENT");
  config.target_selection_bits = UINT32_C(1) << 1;
  assert(qst_provider_preflight(&config) == QST_E_RESOURCE);
  file = fopen(manifest_path, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents, "\"environment_ready\":false") != NULL);
  assert(unlink(manifest_path) == 0);

  assert(setenv("FQ_MYSQL_HOST", "127.0.0.1", 1) == 0);
  assert(setenv("FQ_MYSQL_PORT", "3306", 1) == 0);
  assert(setenv("FQ_MYSQL_USER", "qst", 1) == 0);
  assert(setenv("FQ_MYSQL_PASSWORD", "secret-do-not-persist", 1) == 0);
  had_path = getenv("PATH") != NULL;
  if (had_path) {
    assert(strlen(getenv("PATH")) < sizeof(original_path));
    strcpy(original_path, getenv("PATH"));
  }
  assert(snprintf(fake_bin, sizeof(fake_bin), "%s/bin", root) > 0);
  assert(mkdir(fake_bin, 0750) == 0);
  assert(snprintf(fake_mysql, sizeof(fake_mysql), "%s/mysql", fake_bin) > 0);
  file = fopen(fake_mysql, "w");
  assert(file != NULL);
  assert(fputs("#!/bin/sh\necho 'mysql 8.0.36'\nexit 0\n", file) > 0);
  assert(fclose(file) == 0 && chmod(fake_mysql, 0750) == 0);
  assert(setenv("QST_MYSQL_CLIENT", fake_mysql, 1) == 0);
  assert(setenv("PATH", "/usr/bin:/bin", 1) == 0);
  memset(contents, 0, sizeof(contents));
  assert(qst_provider_preflight(&config) == QST_OK);
  file = fopen(manifest_path, "rb");
  assert(file != NULL && fread(contents, 1, sizeof(contents) - 1, file) > 0 &&
         fclose(file) == 0);
  assert(strstr(contents, "\"target\":\"mysql\"") != NULL);
  assert(strstr(contents, "mysql 8.0.36") != NULL);
  assert(strstr(contents, "secret-do-not-persist") == NULL);
  assert(unlink(manifest_path) == 0);

  assert(snprintf(fake_curl, sizeof(fake_curl), "%s/curl", fake_bin) > 0);
  assert(snprintf(curl_log, sizeof(curl_log), "%s/curl.log", root) > 0);
  file = fopen(fake_curl, "w");
  assert(file != NULL);
  assert(fputs("#!/bin/sh\n"
               "if [ \"${1:-}\" = --version ]; then echo 'curl 8.5.0'; exit 0; fi\n"
               "case \" $* \" in *'Authorization: Bearer qst-provider-token'*) "
               "printf 'bearer\\n' > \"$QST_CURL_STUB_LOG\"; exit 0;; esac\n"
               "exit 1\n", file) > 0);
  assert(fclose(file) == 0 && chmod(fake_curl, 0750) == 0);
  qstTestSaveEnvironment("FQ_INFLUX_HOST", original_influx_host,
                         sizeof(original_influx_host), &had_influx_host);
  qstTestSaveEnvironment("FQ_INFLUX_PORT", original_influx_port,
                         sizeof(original_influx_port), &had_influx_port);
  qstTestSaveEnvironment("FQ_INFLUX_TOKEN", original_influx_token,
                         sizeof(original_influx_token), &had_influx_token);
  qstTestSaveEnvironment("QST_INFLUX_CLIENT", original_influx_client,
                         sizeof(original_influx_client), &had_influx_client);
  qstTestSaveEnvironment("QST_CURL_STUB_LOG", original_curl_stub_log,
                         sizeof(original_curl_stub_log), &had_curl_stub_log);
  assert(setenv("FQ_INFLUX_HOST", "127.0.0.1", 1) == 0);
  assert(setenv("FQ_INFLUX_PORT", "8181", 1) == 0);
  assert(setenv("FQ_INFLUX_TOKEN", "qst-provider-token", 1) == 0);
  assert(setenv("QST_INFLUX_CLIENT", fake_curl, 1) == 0);
  assert(setenv("QST_CURL_STUB_LOG", curl_log, 1) == 0);
  config.target_selection_bits = UINT32_C(1) << 3;
  assert(qst_provider_preflight(&config) == QST_OK);
  file = fopen(curl_log, "r");
  assert(file != NULL && fgets(contents, sizeof(contents), file) != NULL &&
         fclose(file) == 0);
  assert(strcmp(contents, "bearer\n") == 0);
  qstTestRestoreEnvironment("QST_CURL_STUB_LOG", original_curl_stub_log,
                            had_curl_stub_log);
  qstTestRestoreEnvironment("QST_INFLUX_CLIENT", original_influx_client,
                            had_influx_client);
  qstTestRestoreEnvironment("FQ_INFLUX_TOKEN", original_influx_token,
                            had_influx_token);
  qstTestRestoreEnvironment("FQ_INFLUX_PORT", original_influx_port,
                            had_influx_port);
  qstTestRestoreEnvironment("FQ_INFLUX_HOST", original_influx_host,
                            had_influx_host);
  assert(unsetenv("FQ_MYSQL_HOST") == 0 && unsetenv("FQ_MYSQL_PORT") == 0 &&
         unsetenv("FQ_MYSQL_USER") == 0 && unsetenv("FQ_MYSQL_PASSWORD") == 0);
  qstTestRestoreEnvironment("QST_MYSQL_CLIENT", original_mysql_client,
                            had_mysql_client);
  if (had_path) assert(setenv("PATH", original_path, 1) == 0);
  else assert(unsetenv("PATH") == 0);
  assert(unlink(manifest_path) == 0);
  assert(unlink(curl_log) == 0 && unlink(fake_curl) == 0 &&
         unlink(fake_mysql) == 0 && rmdir(fake_bin) == 0 &&
         rmdir(dataset_dir) == 0 && rmdir(root) == 0);
}

static void qstTestMainManagerLifecycle(void) {
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  SQstProcessSlot *slots = NULL;
  SQstGenerationState *generation_state = NULL;
  SQstTaskEntry *entries = NULL;
  char result_dir[256] = {0};
  char logs_dir[320] = {0};
  char pending_dir[320] = {0};
  char report_path[320] = {0};
  char replay_log[] = "/tmp/taosQueryTest-pending-replay-XXXXXX";
  char recovery_marker[] = "/tmp/taosQueryTest-recovery-count-XXXXXX";
  char core_marker[] = "/tmp/taosQueryTest-core-pids-XXXXXX";
  char pair_start_failure_marker[] =
      "/tmp/taosQueryTest-pair-start-failure-XXXXXX";
  char pair_client_log[] = "/tmp/taosQueryTest-pair-client-XXXXXX";
  char expected_final_statistics[128] = {0};
  int32_t replay_fd = -1;
  int32_t recovery_fd = -1;
  int32_t core_fd = -1;
  int32_t pair_start_failure_fd = -1;
  int32_t pair_client_fd = -1;
  int32_t pair_retry_blocker_fd = -1;
  int32_t stderr_fd = -1;
  FILE *replay_file = NULL;
  FILE *recovery_file = NULL;
  uint32_t main_generation = 0;
  uint32_t reaped_managers = 0;
  uint32_t pair_config_count = 0;
  int32_t pair_profile_seen = 0;
  int32_t judge_profile_seen = 0;
  int32_t main_persist_profile_seen = 0;
  uint32_t configured_pair_count = 8;
  uint32_t statement_count = 28;
  uint16_t pair_ports[8] = {0};
  const int32_t port_lease_only = getenv("QST_CORE_PORT_LEASE_ONLY") != NULL;
  const int32_t pair_port_retry_only =
      getenv("QST_CORE_PAIR_PORT_RETRY_ONLY") != NULL;
  const int32_t final_confirmation_race_only =
      getenv("QST_CORE_FINAL_CONFIRMATION_RACE_ONLY") != NULL;
  uint64_t port_lease_worker_index = 0;
  uint16_t pair_retry_initial_port = 0;
  uint16_t pair_retry_expected_port = 0;

  if (port_lease_only != 0) {
    const char *worker_text = getenv("QST_TEST_PORT_LEASE_WORKER_INDEX");
    char *end = NULL;

    assert(worker_text != NULL && worker_text[0] != '\0');
    errno = 0;
    port_lease_worker_index = strtoull(worker_text, &end, 10);
    assert(errno == 0 && end != worker_text && *end == '\0' &&
           port_lease_worker_index <= UINT64_MAX / kTestPairPortBlockCount);
    config.run_id = UINT64_C(5000000000) +
                    port_lease_worker_index * kTestPairPortBlockCount;
  } else {
    config.run_id = UINT64_C(5000000000) + (uint64_t)getpid();
  }
  if (pair_port_retry_only != 0) {
    uint32_t selected_block = 0;

    for (; selected_block + 2U < kTestPairPortBlockCount; ++selected_block) {
      const uint16_t initial_port = (uint16_t)(
          kTestPairPortMin + selected_block * kTestPairPortBlockBytes);

      if (qstTestPortBlockAvailable(initial_port) != 0 &&
          qstTestPortBlockAvailable(
              (uint16_t)(initial_port + kTestPairPortBlockBytes)) != 0 &&
          qstTestPortBlockAvailable(
              (uint16_t)(initial_port + 2U * kTestPairPortBlockBytes)) != 0) {
        pair_retry_initial_port = initial_port;
        pair_retry_expected_port =
            (uint16_t)(initial_port + 2U * kTestPairPortBlockBytes);
        config.run_id = UINT64_C(5000000000) +
                        (selected_block + kTestPairPortBlockCount - 131U) %
                            kTestPairPortBlockCount;
        break;
      }
    }
    assert(pair_retry_initial_port != 0 && pair_retry_expected_port != 0);
  }
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL) {
    pair_client_fd = mkstemp(pair_client_log);
    assert(pair_client_fd >= 0 && close(pair_client_fd) == 0);
    pair_client_fd = -1;
    assert(setenv("QST_FAKE_TAOS_CONNECT_LOG", pair_client_log, 1) == 0);
  }
  /* FULL intentionally enumerates the complete production catalog.  This
   * lifecycle test needs a bounded manager/pair/persistence run instead. */
  config.mode = QST_MODE_RANDOM;
  config.focus = QST_FOCUS_COVERAGE;
  config.target_selection_bits = 1;
  config.random_seed = UINT64_C(0x5a17c0de);
  if (getenv("QST_CORE_RESULT_STORE_RECOVERY_ONLY") != NULL) {
    statement_count = 4;
  }
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL ||
      getenv("QST_CORE_PAIR_CORE_ONLY") != NULL || port_lease_only != 0 ||
      pair_port_retry_only != 0) {
    configured_pair_count = 1;
    statement_count = QST_PAIR_MAX_INFLIGHT;
  }
  if (final_confirmation_race_only != 0) {
    configured_pair_count = 1;
    statement_count = 10;
  }
  config.statement_limit = statement_count;
  /* The production catalog can emit a large SQL together with a substantial
   * occurrence record. Keep enough blocks for several in-flight statements so
   * this lifecycle test exercises recovery rather than permanent backpressure. */
  config.shm_bytes = UINT64_C(1048576);
  /* Exercise the default eight-pair shutdown profile with one judge worker. */
  config.process_slot_count = 31;
  config.task_entry_count = 32;
  config.ai_action_entry_count = 2;
  config.queue_capacity = 32;
  config.block_bytes = 256;
  config.block_count = 1024;
  config.pending_quota_bytes = UINT64_C(1048576);
  config.log_rotation_bytes = UINT64_C(1048576);
  config.execution_pair_count = configured_pair_count;
  config.pair_async_window =
      getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL ? QST_PAIR_MAX_INFLIGHT : 1U;
  config.judge_worker_count = 1;
  config.tool_root = QST_TOOL_ROOT;
  assert(snprintf(result_dir, sizeof(result_dir),
                  "%s/taosQueryTest-main-manager-%ld",
                  qstTestRuntimeTempRoot(), (long)getpid()) > 0);
  (void)mkdir(result_dir, 0700);
  assert(snprintf(pending_dir, sizeof(pending_dir), "%s/pending-results",
                  result_dir) > 0);
  assert(mkdir(pending_dir, 0700) == 0);
  config.result_dir = result_dir;
  assert(qst_shm_create(&config, &mapping) == QST_OK);
  assert(qst_process_register(&mapping, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              (int32_t)getpid(), (int32_t)getpgrp(),
                              &main_generation) == QST_OK);
  assert(main_generation == 1);
  replay_fd = mkstemp(replay_log);
  assert(replay_fd >= 0 && close(replay_fd) == 0);
  recovery_fd = mkstemp(recovery_marker);
  assert(recovery_fd >= 0 && close(recovery_fd) == 0);
  assert(unlink(recovery_marker) == 0);
  if (getenv("QST_CORE_PAIR_CORE_ONLY") != NULL) {
    core_fd = mkstemp(core_marker);
    assert(core_fd >= 0 && close(core_fd) == 0 && unlink(core_marker) == 0);
  }
  if (pair_port_retry_only != 0) {
    pair_start_failure_fd = mkstemp(pair_start_failure_marker);
    assert(pair_start_failure_fd >= 0 && close(pair_start_failure_fd) == 0);
    pair_start_failure_fd = -1;
    assert(unlink(pair_start_failure_marker) == 0);
    pair_retry_blocker_fd = qstTestBindLoopbackPort(
        (uint16_t)(pair_retry_initial_port + kTestPairPortBlockBytes));
    assert(setenv("QST_FAKE_TAOSD_FAIL_FIRST_START_FILE",
                  pair_start_failure_marker, 1) == 0);
  }
  assert(setenv("QST_FAKE_TAOS_SQL_LOG", replay_log, 1) == 0);
  assert(setenv("QST_FAKE_TAOS_FAIL_COUNT_PATTERN",
                "INSERT INTO res_db.runs VALUES", 1) == 0);
  /* The first business DML fails. The worker must recover only then, rather
   * than probing result-store readiness before every successful DML. */
  assert(setenv("QST_FAKE_TAOS_FAIL_COUNT", "1", 1) == 0);
  assert(setenv("QST_FAKE_TAOS_FAIL_COUNT_FILE", recovery_marker, 1) == 0);
  assert(setenv("TAOS_CONFIG_DIR", result_dir, 1) == 0);
  assert(setenv("QST_FAKE_TAOSD_REQUIRE_CLEAN_CONFIG", "1", 1) == 0);
  assert(setenv("QST_FAKE_TAOSD_REQUIRE_DATA_DIR_LAYOUT", "1", 1) == 0);
  assert(setenv("QST_FAKE_TAOSD_REQUIRE_SHORT_DATA_DIR", "1", 1) == 0);
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL ||
      getenv("QST_CORE_PAIR_CORE_ONLY") != NULL || port_lease_only != 0) {
    assert(setenv("QST_FAKE_TAOS_ASYNC_DELAY_MS",
                  port_lease_only != 0 ? "1000" : "20", 1) == 0);
  }
  if (final_confirmation_race_only != 0) {
    assert(setenv("QST_TEST_PAIR_FINAL_CONFIRM_DELAY_MILLISECONDS", "100", 1) ==
           0);
  }
  if (getenv("QST_CORE_PAIR_CORE_ONLY") != NULL) {
    assert(setenv("QST_FAKE_TAOS_CRASH_PATTERN", "SELECT", 1) == 0);
    assert(setenv("QST_FAKE_TAOS_CRASH_PID_FILE", core_marker, 1) == 0);
    assert(setenv("QST_FAKE_TAOS_CRASH_SERVER_LIMIT", "2", 1) == 0);
  }
  /* qst_main_run() is normally a dedicated child process and redirects its
   * stderr to the run log. Preserve the test runner's diagnostic channel. */
  stderr_fd = dup(STDERR_FILENO);
  assert(stderr_fd >= 0);
  assert(qst_main_run(&mapping, &config) == QST_OK);
  assert(dup2(stderr_fd, STDERR_FILENO) == STDERR_FILENO);
  assert(close(stderr_fd) == 0);
  stderr_fd = -1;
  if (pair_port_retry_only != 0) {
    assert(unsetenv("QST_FAKE_TAOSD_FAIL_FIRST_START_FILE") == 0);
    assert(close(pair_retry_blocker_fd) == 0);
    pair_retry_blocker_fd = -1;
    assert(access(pair_start_failure_marker, F_OK) == 0);
    assert(unlink(pair_start_failure_marker) == 0);
  }
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL) {
    assert(unsetenv("QST_FAKE_TAOS_CONNECT_LOG") == 0);
  }
  assert(unsetenv("QST_FAKE_TAOS_SQL_LOG") == 0);
  assert(unsetenv("QST_FAKE_TAOS_FAIL_COUNT_PATTERN") == 0);
  assert(unsetenv("QST_FAKE_TAOS_FAIL_COUNT") == 0);
  assert(unsetenv("QST_FAKE_TAOS_FAIL_COUNT_FILE") == 0);
  assert(unsetenv("TAOS_CONFIG_DIR") == 0);
  assert(unsetenv("QST_FAKE_TAOSD_REQUIRE_CLEAN_CONFIG") == 0);
  assert(unsetenv("QST_FAKE_TAOSD_REQUIRE_DATA_DIR_LAYOUT") == 0);
  assert(unsetenv("QST_FAKE_TAOSD_REQUIRE_SHORT_DATA_DIR") == 0);
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL ||
      getenv("QST_CORE_PAIR_CORE_ONLY") != NULL || port_lease_only != 0) {
    assert(unsetenv("QST_FAKE_TAOS_ASYNC_DELAY_MS") == 0);
  }
  if (final_confirmation_race_only != 0) {
    assert(unsetenv("QST_TEST_PAIR_FINAL_CONFIRM_DELAY_MILLISECONDS") == 0);
  }
  if (getenv("QST_CORE_PAIR_CORE_ONLY") != NULL) {
    assert(unsetenv("QST_FAKE_TAOS_CRASH_PATTERN") == 0);
    assert(unsetenv("QST_FAKE_TAOS_CRASH_PID_FILE") == 0);
    assert(unsetenv("QST_FAKE_TAOS_CRASH_SERVER_LIMIT") == 0);
    recovery_file = fopen(core_marker, "rb");
    assert(recovery_file != NULL && qstTestFileContains(recovery_file, "pid=") != 0 &&
           fclose(recovery_file) == 0);
    assert(unlink(core_marker) == 0);
  }
  assert(memcmp(mapping.header->tdengine_version_digest.bytes,
                (const uint8_t[sizeof(mapping.header->tdengine_version_digest.bytes)]){0},
                sizeof(mapping.header->tdengine_version_digest.bytes)) != 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/stage-profile.json",
                  result_dir) > 0);
  recovery_file = fopen(report_path, "rb");
  assert(recovery_file != NULL &&
         qstTestFileContains(recovery_file,
                             "\"measurement_scope\":\"actual_pipeline\"") != 0 &&
         qstTestFileContains(recovery_file, "\"phase_timings_ns\":{") != 0 &&
         qstTestFileContains(recovery_file, "\"throughput\":{") != 0 &&
         qstTestFileContains(recovery_file, "\"minimum_persisted_qps\":") != 0 &&
         qstTestFileContains(recovery_file, "\"cpu_efficiency\":{") != 0 &&
         qstTestFileContains(recovery_file,
                             "\"tool_cpu_jiffies_by_role\":{") != 0 &&
         qstTestFileContains(recovery_file,
                             "\"pair_taosd_cpu_jiffies\":") != 0 &&
         qstTestFileContains(
             recovery_file, "\"standalone_taosc_tool_cpu_jiffies\":") != 0 &&
         qstTestFileContains(
             recovery_file, "\"pair_supervisor_tool_cpu_jiffies\":") != 0 &&
         qstTestFileContains(recovery_file,
                             "\"result_store_taosd_included\":") != 0 &&
         (getenv("QST_CORE_PAIR_BATCH_ONLY") == NULL ||
          qstTestFileContains(recovery_file,
                              "\"unreadable_process_count\":0") != 0) &&
         qstTestFileContains(recovery_file, "\"manager_startup\":") != 0 &&
         qstTestFileContains(recovery_file, "\"pipeline_drain\":") != 0 &&
         qstTestFileContains(recovery_file, "\"generator\":{") != 0 &&
         qstTestFileContains(recovery_file, "\"items_per_second\":") != 0 &&
         qstTestFileContains(recovery_file, "\"pair\":{") != 0 &&
         qstTestFileContains(recovery_file, "\"judge\":{") != 0 &&
         qstTestFileContains(recovery_file, "\"persistence\":{") != 0 &&
         qstTestFileContains(recovery_file, "\"max_saturation_percent\":") != 0 &&
         fclose(recovery_file) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/result-store-readiness.json", result_dir) > 0);
  recovery_file = fopen(report_path, "rb");
  assert(recovery_file != NULL &&
         qstTestFileContains(recovery_file, "\"available\":true") != 0 &&
         fclose(recovery_file) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/tdengine-startup.json", result_dir) > 0);
  recovery_file = fopen(report_path, "rb");
  assert(recovery_file != NULL &&
         qstTestFileContains(recovery_file, "\"test_local\":") != 0 &&
         qstTestFileContains(recovery_file, "\"result_store\":") != 0 &&
         fclose(recovery_file) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/taosbenchmark.json", result_dir) > 0);
  assert(access(report_path, R_OK) == 0);
  recovery_file = fopen(recovery_marker, "rb");
  assert(recovery_file != NULL && qstTestFileContains(recovery_file, "1") != 0 &&
         fclose(recovery_file) == 0);
  assert(unlink(recovery_marker) == 0);
  replay_file = fopen(replay_log, "rb");
  assert(replay_file != NULL);
  assert(qstTestFileContains(replay_file, "INSERT INTO res_db.stmt_") != 0);
  assert(qstTestFileContains(replay_file, "INSERT INTO res_db.result_") == 0);
  assert(qstTestFileContains(replay_file, "INSERT INTO res_db.runs VALUES") != 0);
  assert(qstTestFileContains(replay_file, "\"fact_stage\":\"initial\"") != 0);
  assert(qstTestFileContains(replay_file, "\"fact_stage\":\"prepared\"") != 0);
  assert(qstTestFileContains(replay_file, "\"fact_stage\":\"final\"") != 0);
  assert(qstTestFileContains(replay_file,
                             "\"dataset_state\":\"unprepared\"") != 0);
  assert(qstTestFileContains(replay_file,
                             "\"dataset_state\":\"published\"") != 0);
  assert(qstTestFileContains(replay_file,
                             "\"dataset_state\":\"finalized\"") != 0);
  assert(qstTestFileContains(replay_file, "\"parallel_pair_count\":0") != 0);
  assert(snprintf(
             expected_final_statistics, sizeof(expected_final_statistics),
             getenv("QST_CORE_PAIR_CORE_ONLY") != NULL ?
                 ",%u,%u,%u,1,0,0,0,0,0,NULL,NULL)" :
                 ",%u,%u,0,0,0,0,0,0,NULL,NULL)",
             statement_count, statement_count,
             statement_count - 1U) > 0);
  assert(qstTestFileContains(replay_file, expected_final_statistics) != 0);
  assert(snprintf(expected_final_statistics, sizeof(expected_final_statistics),
                  "\"parallel_pair_count\":%u", configured_pair_count) > 0);
  assert(qstTestFileContains(replay_file, expected_final_statistics) != 0);
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL) {
    assert(qstTestFileContains(replay_file,
                               "\"max_async_batch_size\":32") != 0);
    assert(qstTestFileContains(replay_file,
                               "\"native_connection_count\":128") != 0);
    recovery_file = fopen(pair_client_log, "r");
    assert(recovery_file != NULL &&
           qstTestFileLineCount(recovery_file, "host=127.0.0.1") ==
               QST_PAIR_MAX_INFLIGHT &&
           fclose(recovery_file) == 0);
    assert(unlink(pair_client_log) == 0);
  }
  if (getenv("QST_CORE_PAIR_CORE_ONLY") != NULL) {
    assert(qstTestFileContains(replay_file, "fake-core-") != 0);
  }
  assert(qstTestFileContains(replay_file, "\"parallel_id\":0") != 0);
  assert(qstTestFileContains(replay_file, "\"taosc_pid\":") != 0);
  assert(qstTestFileContains(replay_file, "\"begin_ts\":") != 0);
  assert(qstTestFileContains(replay_file, "\"end_ts\":") != 0);
  assert(qstTestFileContains(
             replay_file, getenv("QST_CORE_PAIR_CORE_ONLY") != NULL ?
                 "\"taosd_exit_count\":3" :
                 pair_port_retry_only != 0 ? "\"taosd_exit_count\":2" :
                                             "\"taosd_exit_count\":1") != 0);
  if (getenv("QST_CORE_PAIR_CORE_ONLY") != NULL) {
    assert(qstTestFileContains(replay_file, "\"taosd_crash_count\":2") != 0);
  }
  assert(qstTestFileContains(replay_file, "\"taosc_exit_count\":1") != 0);
  assert(qstTestFileContains(replay_file, "\"state\":\"reaped\"") != 0);
  assert(qstTestFileContains(replay_file, "\"resource_profile\":{") != 0);
  assert(qstTestFileContains(replay_file, "\"startup_digests\":{") != 0);
  assert(qstTestFileContains(replay_file, "\"shared_memory_bytes\":1048576") !=
         0);
  assert(qstTestFileContains(replay_file, "\"tdengine_versions\":{") != 0);
  assert(qstTestFileContains(replay_file, "\"provider_versions\":{") != 0);
  assert(qstTestFileContains(replay_file, "INSERT INTO res_db.datasets VALUES") != 0);
  assert(qstTestFileContains(replay_file,
                             "INSERT INTO res_db.qst_pending_records") == 0);
  assert(fclose(replay_file) == 0);
  assert(unlink(replay_log) == 0);
  if (final_confirmation_race_only != 0) {
    assert(snprintf(report_path, sizeof(report_path), "%s/report.json",
                    result_dir) > 0);
    recovery_file = fopen(report_path, "rb");
    assert(recovery_file != NULL &&
           qstTestFileContains(recovery_file, "\"status\":\"completed\"") !=
               0 &&
           qstTestFileContains(recovery_file, "\"generated\":10") != 0 &&
           qstTestFileContains(recovery_file, "\"persisted\":10") != 0 &&
           qstTestFileContains(recovery_file, "\"final_published\":0") !=
               0 &&
           fclose(recovery_file) == 0);
  }
  {
    DIR *directory = opendir(pending_dir);
    struct dirent *entry = NULL;

    assert(directory != NULL);
    while ((entry = readdir(directory)) != NULL) {
      assert(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0 ||
             strcmp(entry->d_name, "quarantine") == 0);
    }
    assert(closedir(directory) == 0);
  }
  assert(qst_generator_state_get(&mapping, &generation_state) == QST_OK);
  assert(atomic_load_explicit(&generation_state->random_activated_count,
                              memory_order_acquire) == statement_count);
  assert(atomic_load_explicit(&generation_state->random_generation_seq,
                              memory_order_acquire) == statement_count + 1U);
  assert(atomic_load_explicit(&generation_state->publish_seq,
                              memory_order_acquire) == 0);
  entries = (SQstTaskEntry *)((uint8_t *)mapping.base +
                              mapping.header->task_region.region_offset);
  for (uint32_t index = 0;
       index < mapping.header->task_region.element_count; ++index) {
    const uint32_t phase = atomic_load_explicit(&entries[index].phase,
                                                memory_order_acquire);

    /* Durable persistence releases task slots for bounded shared memory use. */
    assert(phase == 0);
    assert(entries[index].gid == 0);
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping.base +
      mapping.header->process_region.region_offset);
  for (uint32_t index = 0; index < mapping.header->process_region.element_count;
       ++index) {
    const uint32_t role = atomic_load_explicit(&slots[index].role,
                                                memory_order_acquire);
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                 memory_order_acquire);
    if (role == QST_ROLE_GENERATOR || role == QST_ROLE_EXEC_MANAGER ||
        role == QST_ROLE_JUDGE_MANAGER) {
      assert(slots[index].ppid == (int32_t)getpid());
      assert(slots[index].pgid == slots[index].pid);
      assert(state == QST_PROCESS_REAPED);
      ++reaped_managers;
    }
  }
  assert(reaped_managers == 3);
  assert(snprintf(report_path, sizeof(report_path), "%s/pairs", result_dir) > 0);
  {
    DIR *pairs = opendir(report_path);
    struct dirent *pair = NULL;

    assert(pairs != NULL);
    while ((pair = readdir(pairs)) != NULL) {
      char config_path[512] = {0};
      char config_contents[4096] = {0};
      FILE *config_file = NULL;
      char *port_text = NULL;
      char *port_end = NULL;
      unsigned long port = 0;

      if (strcmp(pair->d_name, ".") == 0 || strcmp(pair->d_name, "..") == 0) {
        continue;
      }
      assert(pair_config_count < sizeof(pair_ports) / sizeof(pair_ports[0]));
      assert(snprintf(config_path, sizeof(config_path), "%s/%s/config/taos.cfg",
                      report_path, pair->d_name) > 0);
      config_file = fopen(config_path, "r");
      assert(config_file != NULL);
      assert(fread(config_contents, 1, sizeof(config_contents) - 1U,
                   config_file) > 0);
      assert(fclose(config_file) == 0);
      assert(strstr(config_contents, "dataDir ") != NULL);
      assert(strstr(config_contents, "/qst-pair-") != NULL);
      assert(strstr(config_contents, " 0 1\n") != NULL);
      assert(strstr(config_contents, "logDir ") != NULL);
      assert(strstr(config_contents, "federatedQueryEnable 1\n") != NULL);
      assert(strstr(config_contents, "asyncLog 0\n") != NULL);
      assert(strstr(config_contents, "debugFlag 0\n") != NULL);
      assert(strstr(config_contents, "uDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "rpcDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "qDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "cDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "dDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "vDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "mDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "wDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "tsdbDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "tqDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "fsDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "udfDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "smaDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "idxDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "tdbDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "metaDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "xndDebugFlag 0\n") != NULL);
      assert(strstr(config_contents, "numOfRpcThreads 1\n") != NULL);
      assert(strstr(config_contents, "numOfTaskQueueThreads 4\n") != NULL);
      assert(strstr(config_contents, "numOfMnodeQueryThreads 1\n") != NULL);
      assert(strstr(config_contents, "numOfVnodeMgmtThreads 1\n") != NULL);
      assert(strstr(config_contents, "numOfVnodeQueryThreads 4\n") != NULL);
      assert(strstr(config_contents, "numOfVnodeFetchThreads 10\n") != NULL);
      assert(strstr(config_contents, "streamNotifyWorkerNum 1\n") != NULL);
      assert(strstr(config_contents, "disableStream 1\n") != NULL);
      assert(strstr(config_contents, "udf 0\n") != NULL);
      port_text = strstr(config_contents, "serverPort ");
      assert(port_text != NULL);
      port = strtoul(port_text + strlen("serverPort "), &port_end, 10);
      assert(port_end != port_text + strlen("serverPort ") && *port_end == '\n');
      assert(port >= 20000U && port <= 60000U);
      for (uint32_t index = 0; index < pair_config_count; ++index) {
        assert(pair_ports[index] != (uint16_t)port);
      }
      pair_ports[pair_config_count++] = (uint16_t)port;
      qstTestAppendPortLeaseObservation((uint16_t)port);
    }
    assert(closedir(pairs) == 0);
  }
  assert(pair_config_count == mapping.header->execution_pair_count);
  if (pair_port_retry_only != 0) {
    assert(pair_config_count == 1);
    assert(pair_ports[0] == pair_retry_expected_port);
  }
  assert(qst_shm_unmap(&mapping) == QST_OK);
  assert(snprintf(logs_dir, sizeof(logs_dir), "%s/logs", result_dir) > 0);
  {
    DIR *directory = opendir(logs_dir);
    struct dirent *entry = NULL;

    assert(directory != NULL);
    while ((entry = readdir(directory)) != NULL) {
      char path[512] = {0};

      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      assert(snprintf(path, sizeof(path), "%s/%s", logs_dir, entry->d_name) > 0);
      if (getenv("QST_PROFILE_PHASES") != NULL) {
        FILE *log_file = fopen(path, "rb");

        assert(log_file != NULL);
        if (qstTestFileContains(log_file, "pair_async_profile") != 0) {
          assert(qstTestFileContains(log_file, "batches=") != 0);
          assert(qstTestFileContains(log_file, "entries=") != 0);
          assert(qstTestFileContains(log_file, "submit_ns=") != 0);
          assert(qstTestFileContains(log_file, "callback_wait_ns=") != 0);
          assert(qstTestFileContains(log_file, "callback_poll_count=") != 0);
          assert(qstTestFileContains(log_file, "finalize_ns=") != 0);
          assert(qstTestFileContains(log_file, "forward_ns=") != 0);
          pair_profile_seen = 1;
        }
        if (qstTestFileContains(log_file, "judge_profile") != 0) {
          assert(qstTestFileContains(log_file, "claims=") != 0);
          assert(qstTestFileContains(log_file, "verdict_ns=") != 0);
          assert(qstTestFileContains(
                     log_file, "persist_prepare_and_write_ns=") != 0);
          assert(qstTestFileContains(log_file, "persist_batch_write_ns=") != 0);
          assert(qstTestFileContains(log_file, "persist_publish_ns=") != 0);
          assert(qstTestFileContains(log_file, "native_batch_count=") != 0);
          judge_profile_seen = 1;
        }
        if (qstTestFileContains(log_file, "main_persist_profile") != 0) {
          assert(qstTestFileContains(log_file, "claims=") != 0);
          assert(qstTestFileContains(log_file, "claim_ns=") != 0);
          assert(qstTestFileContains(log_file, "mark_durable_ns=") != 0);
          assert(qstTestFileContains(log_file, "queue_ack_ns=") != 0);
          assert(qstTestFileContains(log_file, "payload_release_ns=") != 0);
          assert(qstTestFileContains(log_file, "stable_key_release_ns=") != 0);
          assert(qstTestFileContains(log_file, "pending_file_release_ns=") != 0);
          assert(qstTestFileContains(log_file,
                                     "persisted_blocks_release_ns=") != 0);
          assert(qstTestFileContains(log_file, "business_ns=") != 0);
          assert(qstTestFileContains(log_file, "reclaim_ns=") != 0);
          main_persist_profile_seen = 1;
        }
        assert(fclose(log_file) == 0);
      }
      assert(unlink(path) == 0);
    }
    assert(closedir(directory) == 0);
  }
  if (getenv("QST_PROFILE_PHASES") != NULL) {
    assert(pair_profile_seen != 0);
    assert(judge_profile_seen != 0);
    assert(main_persist_profile_seen != 0);
  }
  assert(rmdir(logs_dir) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/pairs", result_dir) > 0);
  {
    DIR *pairs = opendir(report_path);
    struct dirent *pair = NULL;

    assert(pairs != NULL);
    while ((pair = readdir(pairs)) != NULL) {
      char pair_path[512] = {0};
      const char *names[] = {"config", "data", "logs", "cores"};

      if (strcmp(pair->d_name, ".") == 0 || strcmp(pair->d_name, "..") == 0) {
        continue;
      }
      assert(snprintf(pair_path, sizeof(pair_path), "%s/%s", report_path,
                      pair->d_name) > 0);
      for (uint32_t index = 0; index < sizeof(names) / sizeof(names[0]); ++index) {
        char child_path[512] = {0};

        assert(snprintf(child_path, sizeof(child_path), "%s/%s", pair_path,
                        names[index]) > 0);
        if (strcmp(names[index], "config") == 0) {
          char config_path[512] = {0};

          assert(snprintf(config_path, sizeof(config_path), "%s/taos.cfg",
                          child_path) > 0);
          assert(unlink(config_path) == 0);
        }
        if (strcmp(names[index], "cores") == 0) {
          DIR *cores = opendir(child_path);
          struct dirent *core = NULL;

          assert(cores != NULL);
          while ((core = readdir(cores)) != NULL) {
            char core_path[512] = {0};

            if (strcmp(core->d_name, ".") == 0 ||
                strcmp(core->d_name, "..") == 0) {
              continue;
            }
            assert(snprintf(core_path, sizeof(core_path), "%s/%s", child_path,
                            core->d_name) > 0);
            assert(unlink(core_path) == 0);
          }
          assert(closedir(cores) == 0);
        }
        assert(rmdir(child_path) == 0);
      }
      assert(snprintf(pair_path, sizeof(pair_path), "%s/%s/runtime.json",
                      report_path, pair->d_name) > 0);
      assert(unlink(pair_path) == 0);
      assert(snprintf(pair_path, sizeof(pair_path), "%s/%s", report_path,
                      pair->d_name) > 0);
      assert(rmdir(pair_path) == 0);
    }
    assert(closedir(pairs) == 0);
  }
  assert(rmdir(report_path) == 0);
  {
    DIR *directory = opendir(pending_dir);
    struct dirent *entry = NULL;
    uint32_t pending_count = 0;

    assert(directory != NULL);
    while ((entry = readdir(directory)) != NULL) {
      char path[512] = {0};

      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      assert(snprintf(path, sizeof(path), "%s/%s", pending_dir, entry->d_name) > 0);
      assert(unlink(path) == 0);
      ++pending_count;
    }
    assert(closedir(directory) == 0);
    assert(pending_count == 0);
  }
  assert(rmdir(pending_dir) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/report.json", result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/report.md", result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/stage-profile.json",
                  result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/tdengine-readiness.json", result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/result-store-readiness.json", result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/tdengine-startup.json", result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path),
                  "%s/dataset/taosbenchmark.json", result_dir) > 0);
  assert(unlink(report_path) == 0);
  assert(snprintf(report_path, sizeof(report_path), "%s/dataset", result_dir) > 0);
  assert(rmdir(report_path) == 0);
  assert(rmdir(result_dir) == 0);
}

static void qstTestShmCreateMapAndFormatValidation(void) {
  static const uint8_t kShmMagic[8] = {'Q', 'S', 'T', 'S', 'H', 'M', '1', '2'};
  SQstRunConfig config = {0};
  SQstShmMapping created = {0};
  SQstShmMapping mapped = {0};
  SQstMessage message = {0};
  struct stat mapping_stat = {0};
  uint64_t task_region_offset = 0;
  uint64_t event_count = 0;
  uint32_t first_main_generation = 0;
  uint32_t second_main_generation = 0;

  config.run_id = UINT64_C(2000000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 2;
  config.task_entry_count = 2;
  config.ai_action_entry_count = 1;
  config.queue_capacity = 2;
  config.block_bytes = 128;
  config.block_count = 16;
  config.execution_pair_count = 1;
  config.pair_async_window = 1;
  config.judge_worker_count = 1;
  config.statement_timeout_seconds = 17;
  config.statement_retry_count = 2;
  assert(qst_shm_create(&config, &created) == QST_OK);
  assert(fstat(created.shm_fd, &mapping_stat) == 0);
  assert(mapping_stat.st_nlink == 0);
  assert(sizeof(SQstShmHeader) == 640U);
  assert(sizeof(SQstGeneratorCursor) == 16U);
  assert(sizeof(SQstGenerationState) == 448U);
  assert(memcmp(created.header->magic, kShmMagic, sizeof(kShmMagic)) == 0);
  assert(created.header->format_version == QST_SHM_FORMAT_VERSION);
  assert(created.header->launcher_pid == (int32_t)getpid());
  assert(atomic_load_explicit(&created.header->free_task_count,
                              memory_order_acquire) == config.task_entry_count);
  assert(created.header->execution_pair_count == config.execution_pair_count);
  assert(created.header->pair_async_window == config.pair_async_window);
  assert(created.header->judge_worker_count == config.judge_worker_count);
  assert(created.header->statement_timeout_seconds ==
         config.statement_timeout_seconds);
  assert(created.header->statement_retry_count == config.statement_retry_count);
  assert(created.header->generation_state_region.element_bytes ==
         sizeof(SQstGenerationState));
  assert(qst_process_register(&created, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              100, 100, &first_main_generation) == QST_OK);
  assert(first_main_generation == 1);
  assert(qst_process_reap_generation(&created, QST_ROLE_LAUNCHER, 1,
                                     QST_ROLE_MAIN, first_main_generation,
                                     0) == QST_OK);
  assert(qst_process_register(&created, QST_ROLE_LAUNCHER, 1, QST_ROLE_MAIN,
                              101, 101, &second_main_generation) == QST_OK);
  assert(second_main_generation == 2);
  assert(atomic_load_explicit(&created.header->main_generation,
                              memory_order_acquire) == second_main_generation);
  for (uint32_t index = 0; index < 4; ++index) {
    assert(created.queue_eventfd[index] >= 0);
  }
  message = qstControlMessage(config.run_id);
  assert(qst_queue_publish(&created, QST_QUEUE_CONTROL, &message, UINT64_MAX) ==
         QST_OK);
  assert(read(created.queue_eventfd[QST_QUEUE_CONTROL - 1], &event_count,
              sizeof(event_count)) == (ssize_t)sizeof(event_count));
  assert(event_count == 1);
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) == QST_OK);
  assert(qst_shm_unmap(&mapped) == QST_OK);
  atomic_fetch_add_explicit(&created.header->next_gid, 1, memory_order_relaxed);
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) == QST_OK);
  assert(qst_shm_unmap(&mapped) == QST_OK);
  created.header->magic[7] = '3';
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) ==
         QST_E_SHM_FORMAT);
  created.header->magic[7] = '2';
  created.header->format_version = QST_SHM_FORMAT_VERSION - 1U;
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) ==
         QST_E_SHM_FORMAT);
  created.header->format_version = QST_SHM_FORMAT_VERSION;
  created.header->page_bytes = 1;
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) ==
         QST_E_SHM_FORMAT);
  created.header->page_bytes = (uint32_t)sysconf(_SC_PAGESIZE);
  created.header->endian_tag = 0;
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) ==
         QST_E_SHM_FORMAT);
  created.header->endian_tag = QST_ABI_ENDIAN_TAG;
  task_region_offset = created.header->task_region.region_offset;
  created.header->task_region.region_offset =
      created.header->process_region.region_offset;
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) ==
         QST_E_SHM_CORRUPT);
  created.header->task_region.region_offset = task_region_offset;
  created.header->header_crc32c ^= 1U;
  assert(qst_shm_map_validate(created.shm_fd, config.run_id, &mapped) ==
         QST_E_SHM_CORRUPT);
  assert(qst_shm_unmap(&created) == QST_OK);
}

static void qstTestMultiProcessQueuePublishClaimAck(void) {
  enum {
    kProducerCount = 2,
    kMessagesPerProducer = 32,
    kExpectedMessages = kProducerCount * kMessagesPerProducer,
    kQueueCapacity = kExpectedMessages * 2,
  };
  SQstRunConfig config = {0};
  SQstShmMapping mapping = {0};
  int32_t result_pipe[2] = {-1, -1};
  uint8_t seen[kExpectedMessages] = {0};
  pid_t producer_pids[kProducerCount] = {0};
  pid_t consumer_pids[kProducerCount] = {0};
  uint32_t received = 0;

  config.run_id = UINT64_C(3000000000) + (uint64_t)getpid();
  config.shm_bytes = UINT64_C(262144);
  config.process_slot_count = 2;
  config.task_entry_count = 2;
  config.ai_action_entry_count = 1;
  /* Producers finish before consumers start, so reserve enough queue slots
   * for the whole publish phase rather than depending on scheduler timing. */
  config.queue_capacity = kQueueCapacity;
  config.block_bytes = 128;
  config.block_count = 16;
  assert(qst_shm_create(&config, &mapping) == QST_OK);

  for (uint32_t producer = 0; producer < kProducerCount; ++producer) {
    pid_t pid = fork();
    if (pid == 0) {
      for (uint32_t message_index = 0; message_index < kMessagesPerProducer;
           ++message_index) {
        SQstMessage message = qstControlMessage(config.run_id);
        if (qst_queue_publish(&mapping, QST_QUEUE_CONTROL, &message,
                              UINT64_MAX) != QST_OK) {
          _exit(1);
        }
      }
      _exit(0);
    }
    assert(pid > 0);
    producer_pids[producer] = pid;
  }
  for (uint32_t producer = 0; producer < kProducerCount; ++producer) {
    int32_t status = 0;
    assert(waitpid(producer_pids[producer], &status, 0) ==
           producer_pids[producer]);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }

  assert(pipe(result_pipe) == 0);
  for (uint32_t consumer = 0; consumer < kProducerCount; ++consumer) {
    pid_t pid = fork();
    if (pid == 0) {
      SQstClaim claim = {0};
      uint64_t message_id = 0;
      (void)close(result_pipe[0]);
      while (qst_queue_claim(&mapping, QST_QUEUE_CONTROL,
                             QST_ROLE_LAUNCHER, 1, UINT64_MAX,
                             &claim) == QST_OK) {
        message_id = claim.message.message_id;
        if (qst_queue_ack(&mapping, &claim, QST_ACK_CONTROL_RESPONSE,
                          message_id) != QST_OK ||
            write(result_pipe[1], &message_id, sizeof(message_id)) !=
                (ssize_t)sizeof(message_id)) {
          _exit(1);
        }
      }
      (void)close(result_pipe[1]);
      _exit(0);
    }
    assert(pid > 0);
    consumer_pids[consumer] = pid;
  }
  assert(close(result_pipe[1]) == 0);
  while (received < kExpectedMessages) {
    uint64_t message_id = 0;
    assert(read(result_pipe[0], &message_id, sizeof(message_id)) ==
           (ssize_t)sizeof(message_id));
    assert(message_id > 0 && message_id <= kExpectedMessages);
    assert(seen[message_id - 1] == 0);
    seen[message_id - 1] = 1;
    ++received;
  }
  assert(close(result_pipe[0]) == 0);
  for (uint32_t consumer = 0; consumer < kProducerCount; ++consumer) {
    int32_t status = 0;
    assert(waitpid(consumer_pids[consumer], &status, 0) ==
           consumer_pids[consumer]);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
  }
  for (uint32_t index = 0; index < kExpectedMessages; ++index) {
    assert(seen[index] == 1);
  }
  assert(qst_shm_unmap(&mapping) == QST_OK);
}

int main(void) {
  if (getenv("QST_CORE_BLOCK_ALLOCATOR_ONLY") != NULL) {
    qstTestMultiBlockRoundTripAndRelease();
    qstTestBlockAllocationAndCorruptionFailures();
    qstTestBlockAbortUnpublishedChain();
    qstTestReclaimsStaleRunOwnedBlocks();
    return 0;
  }
  if (getenv("QST_CORE_BLOCK_ONLY") != NULL) {
    qstTestMultiBlockRoundTripAndRelease();
    qstTestBlockAllocationAndCorruptionFailures();
    qstTestBlockAbortUnpublishedChain();
    qstTestReclaimsStaleRunOwnedBlocks();
    qstTestBlockCrossChainCopy();
    qstTestOverflowContentAtomicAppend();
    return 0;
  }
  if (getenv("QST_CORE_AUDIT_ONLY") != NULL) {
    qstTestAuditCli();
    return 0;
  }
  if (getenv("QST_CORE_NATIVE_ONLY") != NULL) {
    qstTestNativeAsyncLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_RESULT_CAPTURE_ONLY") != NULL) {
    qstTestNativeResultSerializationAndComparison();
    return 0;
  }
  if (getenv("QST_CORE_RAW_LIVE_ONLY") != NULL) {
    qstTestRawCaptureLiveComparison();
    return 0;
  }
  if (getenv("QST_CORE_RESULT_ONLY") != NULL) {
    qstTestRunAndDatasetPersistence();
    qstTestHistoricalVerdictLookup();
    qstTestStoredResultReadAndManualAiWrite();
    qstTestNativeResultSerializationAndComparison();
    qstTestStreamingResultOracle();
    qstTestPendingStoreContinuousAppend();
    qstTestPendingReplayAndDiscard();
    qstTestPendingStreamReplay();
    return 0;
  }
  if (getenv("QST_CORE_DATASET_ONLY") != NULL) {
    qstTestDatasetManifestValidation();
    qstTestRuntimeObjectBinding();
    qstTestLocalTypeRegistry();
    qstTestDatasetPrepareAndCleanup();
    qstTestExternalOwnedDatabaseCleanup();
    return 0;
  }
  if (getenv("QST_CORE_PROVIDER_ONLY") != NULL) {
    qstTestExternalSourcePortValidation();
    qstTestExternalMappingValidation();
    qstTestInvalidMappingPreventsExternalScript();
    qstTestProviderPreflightManifest();
    return 0;
  }
  if (getenv("QST_CORE_QUEUE_ONLY") != NULL) {
    qstTestQueueClaimAckAndFull();
    qstTestQueueAbandonAndRejectBadMessage();
    qstTestQueueRejectsCorruptVisibleMessage();
    qstTestQueueRejectsWrongConsumerRole();
    qstTestParentReclaimsExitedConsumerClaim();
    qstTestQueueStallObserverAndRestartBudget();
    qstTestMultiProcessQueuePublishClaimAck();
    return 0;
  }
  if (getenv("QST_CORE_PROCESS_ONLY") != NULL) {
    qstTestProcessRegistrationAndReaping();
    qstTestProcessAutoSlotReuse();
    qstTestProcessStopProtocol();
    qstTestProcessDescendantPostorderAndAdoption();
    qstTestLauncherLifecycle();
    qstTestLauncherFailureReleasesRunLock();
    qstTestLauncherHoldsRunLock();
    qstTestLauncherStopDrainsPersistence();
    return 0;
  }
  if (getenv("QST_CORE_PROCESS_SLOT_REUSE_ONLY") != NULL) {
    qstTestProcessAutoSlotReuse();
    return 0;
  }
  if (getenv("QST_CORE_CONFIG_RESOURCE_ONLY") != NULL) {
    qstTestConfigParsing();
    qstTestResourceCapacityCalculation();
    qstTestDependencyLockPreflight();
    qstTestSha256();
    return 0;
  }
  if (getenv("QST_CORE_AI_REPORT_ONLY") != NULL) {
    qstTestAiActionLedger();
    qstTestReportAtomicRefresh();
    return 0;
  }
  if (getenv("QST_CORE_STARTUP_LEASE_ONLY") != NULL) {
    qstTestLauncherAllowsMainStartupLease();
    return 0;
  }
  if (getenv("QST_CORE_SHUTDOWN_LEASE_ONLY") != NULL) {
    qstTestLauncherAllowsMainShutdownLease();
    return 0;
  }
  if (getenv("QST_CORE_TASK_STATS_ONLY") != NULL) {
    qstTestExecutionInputsDrained();
    qstTestPairRetireEligibility();
    qstTestTaskLifecycleAndDurability();
    return 0;
  }
  if (getenv("QST_CORE_RESULT_STORE_RECOVERY_ONLY") != NULL) {
    qstTestMainManagerLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_PAIR_DEADLINE_ONLY") != NULL) {
    qstTestPairDeadlineRecovery();
    return 0;
  }
  if (getenv("QST_CORE_PAIR_BATCH_ONLY") != NULL) {
    qstTestMainManagerLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_PAIR_PORT_RETRY_ONLY") != NULL) {
    qstTestMainManagerLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_FINAL_CONFIRMATION_RACE_ONLY") != NULL) {
    qstTestMainManagerLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_GENERATOR_RANDOM_STRESS_ONLY") != NULL) {
    qstTestRuntimeObjectBinding();
    return 0;
  }
  if (getenv("QST_CORE_PAIR_CORE_ONLY") != NULL) {
    qstTestMainManagerLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_PORT_LEASE_ONLY") != NULL) {
    qstTestMainManagerLifecycle();
    return 0;
  }
  if (getenv("QST_CORE_RUN_LOCK_ONLY") != NULL) {
    qstTestLauncherHoldsRunLock();
    return 0;
  }
  if (getenv("QST_CORE_DEPENDENCY_LOCK_ONLY") != NULL) {
    qstTestDependencyLockPreflight();
    return 0;
  }
  if (getenv("QST_CORE_PAIR_CRASH_ONLY") != NULL) {
    qstTestLauncherPairSupervisorCrashRecovery();
    return 0;
  }
  if (getenv("QST_CORE_PAIR_STOP_CRASH_ONLY") != NULL) {
    qstTestLauncherPairSupervisorStopCrashFailsRun();
    return 0;
  }
  if (getenv("QST_CORE_EXEC_MANAGER_CRASH_ONLY") != NULL) {
    qstTestLauncherExecManagerCrashFailsRun();
    return 0;
  }
  if (getenv("QST_CORE_JUDGE_WORKER_CRASH_ONLY") != NULL) {
    qstTestLauncherJudgeWorkerCrashFailsRun();
    return 0;
  }
  if (getenv("QST_CORE_GENERATOR_ONLY") != NULL) {
    qstTestGeneratorCatalogAndDepth();
    qstTestGeneratorFragPlanPreparation();
    qstTestGeneratorFragPlanSelectionEquivalence();
    qstTestGeneratorExplicitSelfCall();
    qstTestGeneratorCatalogDigestIncludesTextLength();
    qstTestGeneratorInlineValueSteps();
    qstTestGeneratorUnboundedLocalTrace();
    qstTestGeneratorOptionalLoopProtocol();
    qstTestGeneratorSqlCapacityFallback();
    qstTestRuntimeObjectBinding();
    qstTestSqlDimensionSourceDirectory();
    qstTestGeneratorFullRecoveryDoesNotLosePaths();
    qstTestFullRunControlState();
    qstTestGeneratorRandomDeterminismAndWrap();
    qstTestSharedFullPublishRoundRobinTargets();
    qstTestSharedFullPreparationKeepsPublicationOrder();
    qstTestSharedFullPublishQueueFailureKeepsCursor();
    qstTestSharedFullLookAheadKeepsOrderAcrossQueueFailure();
    return 0;
  }
  if (getenv("QST_CORE_GENERATOR_BENCH_ONLY") != NULL) {
    qstTestGeneratorStageBenchmarkMatrix();
    return 0;
  }
  if (getenv("QST_CORE_GENERATOR_REAL_STAGE_BENCH_ONLY") != NULL) {
    qstTestGeneratorRealStageBenchmark();
    return 0;
  }
  if (getenv("QST_CORE_QUEUE_STAGE_BENCH_ONLY") != NULL) {
    qstTestQueueStageBenchmarkMatrix();
    return 0;
  }
  if (getenv("QST_CORE_NATIVE_STAGE_BENCH_ONLY") != NULL) {
    qstTestNativeStageBenchmarkMatrix();
    return 0;
  }
  if (getenv("QST_CORE_PERSIST_STAGE_BENCH_ONLY") != NULL) {
    qstTestPersistStageBenchmarkMatrix();
    return 0;
  }
  qstTestMultiBlockRoundTripAndRelease();
  qstTestBlockAllocationAndCorruptionFailures();
  qstTestBlockAbortUnpublishedChain();
  qstTestReclaimsStaleRunOwnedBlocks();
  qstTestBlockCrossChainCopy();
  qstTestNativeAsyncLifecycle();
  qstTestAuditCli();
  qstTestOverflowContentAtomicAppend();
  qstTestRunAndDatasetPersistence();
  qstTestHistoricalVerdictLookup();
  qstTestStoredResultReadAndManualAiWrite();
  qstTestNativeResultSerializationAndComparison();
  qstTestStreamingResultOracle();
  qstTestReportAtomicRefresh();
  qstTestPendingStoreContinuousAppend();
  qstTestPendingReplayAndDiscard();
  qstTestPendingStreamReplay();
  qstTestAiActionLedger();
  qstTestDatasetManifestValidation();
  qstTestRuntimeObjectBinding();
  qstTestLocalTypeRegistry();
  qstTestLauncherFailureReleasesRunLock();
  qstTestDependencyLockPreflight();
  qstTestLauncherHoldsRunLock();
  qstTestLauncherStopDrainsPersistence();
  qstTestLauncherAllowsMainStartupLease();
  qstTestLauncherAllowsMainShutdownLease();
  qstTestGeneratorCatalogAndDepth();
  qstTestGeneratorFragPlanPreparation();
  qstTestGeneratorFragPlanSelectionEquivalence();
  qstTestGeneratorExplicitSelfCall();
  qstTestGeneratorCatalogDigestIncludesTextLength();
  qstTestGeneratorInlineValueSteps();
  qstTestGeneratorUnboundedLocalTrace();
  qstTestGeneratorOptionalLoopProtocol();
  qstTestGeneratorSqlCapacityFallback();
  qstTestSqlDimensionSourceDirectory();
  qstTestSharedGenerationState();
  qstTestRandomRunControlState();
  qstTestFullRunControlState();
  qstTestGeneratorFullRecoveryDoesNotLosePaths();
  qstTestGeneratorRandomDeterminismAndWrap();
  qstTestGeneratorPublishCompensatesQueueFailure();
  qstTestSharedFullPublishCursorCommit();
  qstTestSharedFullPublishRoundRobinTargets();
  qstTestSharedFullPreparationKeepsPublicationOrder();
  qstTestSharedFullPublishQueueFailureKeepsCursor();
  qstTestSharedFullLookAheadKeepsOrderAcrossQueueFailure();
  qstTestSharedRandomPublishTransaction();
  qstTestDatasetPrepareAndCleanup();
  qstTestExternalOwnedDatabaseCleanup();
  qstTestExternalSourcePortValidation();
  qstTestQueueClaimAckAndFull();
  qstTestQueueAbandonAndRejectBadMessage();
  qstTestQueueRejectsCorruptVisibleMessage();
  qstTestQueueRejectsWrongConsumerRole();
  qstTestParentReclaimsExitedConsumerClaim();
  qstTestExecutionInputsDrained();
  qstTestPairRetireEligibility();
  qstTestTaskLifecycleAndDurability();
  qstTestTaskDispatchAbortRollback();
  qstTestPairSlotOwnershipIsolation();
  qstTestTaskTimeoutRetryEpochIsolation();
  qstTestPairDeadlineRecovery();
  qstTestProcessRegistrationAndReaping();
  qstTestProcessAutoSlotReuse();
  qstTestProcessStopProtocol();
  qstTestProcessDescendantPostorderAndAdoption();
  qstTestQueueStallObserverAndRestartBudget();
  qstTestExternalMappingValidation();
  qstTestInvalidMappingPreventsExternalScript();
  qstTestProviderPreflightManifest();
  qstTestMainManagerLifecycle();
  qstTestShmCreateMapAndFormatValidation();
  qstTestMultiProcessQueuePublishClaimAck();
  qstTestConfigParsing();
  qstTestResourceCapacityCalculation();
  qstTestSha256();
  qstTestLauncherLifecycle();
  qstTestLauncherPairSupervisorStopCrashFailsRun();
  qstTestLauncherExecManagerCrashFailsRun();
  qstTestLauncherJudgeWorkerCrashFailsRun();
  return 0;
}
