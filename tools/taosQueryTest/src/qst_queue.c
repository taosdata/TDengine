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

#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#include "qst_queue.h"

#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <time.h>

#include <unistd.h>

#include "qst_block.h"
#include "qst_crc32c.h"
#include "qst_process.h"

/* Queue operations repeatedly authorize one local process identity. The cache
 * records only the shared process-slot index; every hit rereads the current
 * role, generation, and exit state so process reaping and slot reuse remain
 * visible to queue authorization. */
typedef struct {
  const SQstShmMapping *mapping;
  uint64_t run_id;
  uint32_t role;
  uint32_t slot_id;
  uint32_t generation;
  uint32_t slot_index;
  uint32_t valid;
} SQstQueueCallerCache;

static _Thread_local SQstQueueCallerCache gQstQueueCallerCache;

static uint32_t qstMessageCrc(const SQstMessage *message) {
  return qstCrc32c(0, message, offsetof(SQstMessage, message_crc32c));
}

static int32_t qstDeadlineExpired(uint64_t deadline_mono_ns) {
  struct timespec now = {0};
  uint64_t now_ns = 0;

  if (deadline_mono_ns == 0 || deadline_mono_ns == UINT64_MAX ||
      clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0) {
    return 0;
  }
  if ((uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
    return 1;
  }
  now_ns = (uint64_t)now.tv_sec * UINT64_C(1000000000) +
           (uint64_t)now.tv_nsec;
  return now_ns >= deadline_mono_ns;
}

static int32_t qstIsPowerOfTwo(uint64_t value) {
  return value > 1 && (value & (value - 1)) == 0;
}

static EQstCode qstGetQueue(const SQstShmMapping *mapping,
                            EQstQueueId queue_id,
                            SQstQueueDescriptor **out_descriptor,
                            SQstQueueSlot **out_slots) {
  const SQstRegion *region = NULL;
  SQstQueueDescriptor *descriptor = NULL;

  if (NULL == mapping || NULL == mapping->base || NULL == mapping->header ||
      NULL == out_descriptor || NULL == out_slots ||
      queue_id < QST_QUEUE_CONTROL || queue_id > QST_QUEUE_PERSIST) {
    return QST_E_INVALID_ARGUMENT;
  }

  region = &mapping->header->queue_region[queue_id - 1];
  if (region->region_offset > mapping->bytes ||
      region->region_bytes > mapping->bytes - region->region_offset ||
      region->element_bytes != sizeof(SQstQueueSlot) ||
      region->element_count == 0 ||
      region->region_bytes != sizeof(SQstQueueDescriptor) +
                                  (uint64_t)region->element_count *
                                      region->element_bytes) {
    return QST_E_SHM_CORRUPT;
  }

  descriptor = (SQstQueueDescriptor *)((uint8_t *)mapping->base +
                                       region->region_offset);
  if (descriptor->queue_id != queue_id ||
      descriptor->slot_bytes != sizeof(SQstQueueSlot) ||
      descriptor->capacity != region->element_count ||
      !qstIsPowerOfTwo(descriptor->capacity) ||
      descriptor->slot_offset != region->region_offset +
                                     sizeof(SQstQueueDescriptor)) {
    return QST_E_SHM_CORRUPT;
  }

  *out_descriptor = descriptor;
  *out_slots = (SQstQueueSlot *)((uint8_t *)mapping->base +
                                  descriptor->slot_offset);
  return QST_OK;
}

static EQstCode qstValidateRef(const SQstShmMapping *mapping, SQstRef ref,
                               uint64_t owner_gid) {
  if (qstRefIsEmpty(ref)) {
    return QST_OK;
  }
  return qst_block_validate(mapping, ref, owner_gid) == QST_OK ? QST_OK :
                                                                   QST_E_SHM_CORRUPT;
}

static int32_t qstIsTarget(uint32_t target) {
  return target >= QST_TARGET_TD && target <= QST_TARGET_INFLUX;
}

static int32_t qstQueueConsumerIsAllowed(EQstQueueId queue_id,
                                         EQstRole role) {
  switch (queue_id) {
    case QST_QUEUE_CONTROL:
      return 1;
    case QST_QUEUE_GENERATE:
      return role == QST_ROLE_PAIR_SUPERVISOR;
    case QST_QUEUE_EXECUTE:
      return role == QST_ROLE_JUDGE_MANAGER || role == QST_ROLE_JUDGE_WORKER;
    case QST_QUEUE_PERSIST:
      return role == QST_ROLE_MAIN;
    default:
      return 0;
  }
}

static int32_t qstQueueCallerIsCurrent(const SQstShmMapping *mapping,
                                       uint32_t role, uint32_t slot_id,
                                       uint32_t generation) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (role == QST_ROLE_LAUNCHER) {
    return generation == mapping->header->launcher_generation;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return 0;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  if (gQstQueueCallerCache.valid != 0 &&
      gQstQueueCallerCache.mapping == mapping &&
      gQstQueueCallerCache.run_id == mapping->header->run_id &&
      gQstQueueCallerCache.role == role &&
      gQstQueueCallerCache.slot_id == slot_id &&
      gQstQueueCallerCache.generation == generation &&
      gQstQueueCallerCache.slot_index < region->element_count) {
    SQstProcessSlot *cached = &slots[gQstQueueCallerCache.slot_index];
    const uint32_t state = atomic_load_explicit(&cached->exit_state,
                                                memory_order_acquire);

    if (atomic_load_explicit(&cached->role, memory_order_acquire) == role &&
        cached->slot_id == slot_id &&
        atomic_load_explicit(&cached->generation, memory_order_acquire) ==
            generation &&
        (state == QST_PROCESS_RUNNING || state == QST_PROCESS_STOP_REQUESTED ||
         state == QST_PROCESS_STOP_ACKED)) {
      return 1;
    }
    gQstQueueCallerCache.valid = 0;
  }
  for (uint32_t index = 0; index < region->element_count; ++index) {
    const uint32_t state = atomic_load_explicit(&slots[index].exit_state,
                                                memory_order_acquire);
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) == role &&
        slots[index].slot_id == slot_id &&
        atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
            generation &&
        (state == QST_PROCESS_RUNNING || state == QST_PROCESS_STOP_REQUESTED ||
         state == QST_PROCESS_STOP_ACKED)) {
      gQstQueueCallerCache.mapping = mapping;
      gQstQueueCallerCache.run_id = mapping->header->run_id;
      gQstQueueCallerCache.role = role;
      gQstQueueCallerCache.slot_id = slot_id;
      gQstQueueCallerCache.generation = generation;
      gQstQueueCallerCache.slot_index = index;
      gQstQueueCallerCache.valid = 1;
      return 1;
    }
  }
  return 0;
}

static EQstCode qstValidateMessage(const SQstShmMapping *mapping,
                                   EQstQueueId queue_id,
                                   const SQstMessage *message,
                                   int32_t verify_message_crc,
                                   int32_t verify_generation_record_digest) {
  EQstCode code = QST_OK;

  if (NULL == message || message->run_id != mapping->header->run_id ||
      message->producer_role < QST_ROLE_LAUNCHER ||
      message->producer_role > QST_ROLE_AI_WORKER ||
      message->producer_generation == 0 || message->message_id == 0 ||
      message->reserved != 0 ||
      (verify_message_crc != 0 &&
       message->message_crc32c != qstMessageCrc(message))) {
    return QST_E_SHM_CORRUPT;
  }

  switch (queue_id) {
    case QST_QUEUE_CONTROL:
      if (message->message_type != QST_MSG_CONTROL ||
          (message->body.control.related_gid != 0 &&
           message->body.control.related_gid != message->gid)) {
        return QST_E_SHM_CORRUPT;
      }
      return qstValidateRef(mapping, message->body.control.diagnostic_ref,
                            message->gid);
    case QST_QUEUE_GENERATE:
      if (message->message_type != QST_MSG_GENERATE_WORK ||
          (message->producer_role != QST_ROLE_GENERATOR &&
           message->producer_role != QST_ROLE_EXEC_MANAGER) ||
          message->gid == 0 ||
          message->dispatch_epoch == 0 || message->transition_seq == 0 ||
          !qstIsTarget(message->target) ||
          message->body.generate.target != message->target ||
          message->body.generate.mode < QST_MODE_FULL ||
          message->body.generate.mode > QST_MODE_RANDOM ||
          message->body.generate.focus < QST_FOCUS_COVERAGE ||
          message->body.generate.focus > QST_FOCUS_CORRECTNESS ||
          message->body.generate.dialect < QST_DIALECT_TDENGINE ||
          message->body.generate.dialect > QST_DIALECT_INFLUXQL) {
        return QST_E_SHM_CORRUPT;
      }
      if (verify_generation_record_digest != 0) {
        SQstDigest256 calculated = {0};

        code = qstValidateRef(mapping, message->body.generate.sql_ref,
                              message->gid);
        if (code != QST_OK) {
          return code;
        }
        /* qst_block_sha256 validates the complete chain before hashing it.
         * Calling qstValidateRef first would walk and CRC-check the same
         * generation record twice at the claim boundary. */
        return qst_block_sha256(
                   mapping, message->body.generate.generation_record_ref,
                   message->gid, &calculated) == QST_OK &&
                       memcmp(calculated.bytes,
                              message->body.generate.generation_record_sha256.bytes,
                              sizeof(calculated.bytes)) == 0 ?
                   QST_OK : QST_E_SHM_CORRUPT;
      }
      code = qstValidateRef(mapping, message->body.generate.sql_ref,
                            message->gid);
      if (code != QST_OK ||
          qstValidateRef(mapping,
                         message->body.generate.generation_record_ref,
                         message->gid) != QST_OK) {
        return code != QST_OK ? code : QST_E_SHM_CORRUPT;
      }
      return QST_OK;
    case QST_QUEUE_EXECUTE:
      if (message->message_type != QST_MSG_EXECUTE_FINAL ||
          (message->producer_role != QST_ROLE_EXEC_MANAGER &&
           message->producer_role != QST_ROLE_PAIR_SUPERVISOR) ||
          message->gid == 0 ||
          message->dispatch_epoch == 0 || message->transition_seq == 0 ||
          !qstIsTarget(message->target)) {
        return QST_E_SHM_CORRUPT;
      }
      code = qstValidateRef(mapping, message->body.execute.result_ref,
                            message->gid);
      if (code != QST_OK) {
        return code;
      }
      code = qstValidateRef(mapping, message->body.execute.error_ref,
                            message->gid);
      if (code != QST_OK) {
        return code;
      }
      code = qstValidateRef(mapping, message->body.execute.log_range_ref,
                            message->gid);
      return code == QST_OK ?
                 qstValidateRef(mapping, message->body.execute.core_path_ref,
                                message->gid) : code;
    case QST_QUEUE_PERSIST:
      if (message->message_type != QST_MSG_PERSIST_CONFIRM ||
          message->producer_role != QST_ROLE_JUDGE_WORKER ||
          message->body.persist.persist_kind < QST_PERSIST_STATEMENT ||
          message->body.persist.persist_kind > QST_PERSIST_DATASET ||
          (message->body.persist.reserved0 & ~QST_PERSIST_CONFIRM_BATCH) != 0 ||
          message->body.persist.target != message->target ||
          message->body.persist.gid != message->gid ||
          message->body.persist.dispatch_epoch != message->dispatch_epoch ||
          message->body.persist.transition_seq != message->transition_seq ||
          (message->body.persist.stats_delta_mask &
           ~(QST_STAT_DELTA_PENDING_REPLAYED * 2 - 1)) != 0) {
        return QST_E_SHM_CORRUPT;
      }
      if (message->body.persist.persist_kind <= QST_PERSIST_MANUAL_AI_RESULT &&
          (message->gid == 0 || message->dispatch_epoch == 0 ||
           message->transition_seq == 0 || !qstIsTarget(message->target))) {
        return QST_E_SHM_CORRUPT;
      }
      if (message->body.persist.persist_kind >= QST_PERSIST_RUN_UPDATE &&
          (message->gid != 0 || message->target != 0 ||
           message->dispatch_epoch != 0 || message->transition_seq != 0)) {
        return QST_E_SHM_CORRUPT;
      }
      code = qstValidateRef(mapping, message->body.persist.stable_key_ref,
                            message->gid);
      if (code != QST_OK) {
        return code;
      }
      code = qstValidateRef(mapping, message->body.persist.payload_ref,
                            message->gid);
      return code == QST_OK ?
                 qstValidateRef(mapping, message->body.persist.pending_file_ref,
                                message->gid) : code;
    default:
      return QST_E_SHM_CORRUPT;
  }
}

static void qstQueueProgress(SQstQueueDescriptor *descriptor) {
  atomic_fetch_add_explicit(&descriptor->progress_epoch, 1,
                            memory_order_release);
}

static void qstNotifyQueue(const SQstShmMapping *mapping,
                           EQstQueueId queue_id) {
  const uint64_t event_count = 1;
  const int32_t fd = mapping->queue_eventfd[queue_id - 1];

  if (fd >= 0 && write(fd, &event_count, sizeof(event_count)) < 0 &&
      errno != EAGAIN && errno != EINTR) {
    /* The shared queue remains authoritative when best-effort wakeup fails. */
  }
}

static void qstReleaseSlot(SQstQueueDescriptor *descriptor, SQstQueueSlot *slot,
                           uint64_t queue_position) {
  memset(&slot->message, 0, sizeof(slot->message));
  atomic_store_explicit(&slot->owner_role, 0, memory_order_relaxed);
  atomic_store_explicit(&slot->owner_slot_id, 0, memory_order_relaxed);
  atomic_store_explicit(&slot->owner_generation, 0, memory_order_relaxed);
  atomic_store_explicit(&slot->claim_generation, 0, memory_order_relaxed);
  atomic_store_explicit(&slot->claim_seq, 0, memory_order_relaxed);
  atomic_store_explicit(&slot->lease_deadline_mono_ns, 0, memory_order_relaxed);
  atomic_store_explicit(&slot->state, QST_SLOT_FREE, memory_order_release);
  atomic_store_explicit(&slot->sequence, queue_position + descriptor->capacity,
                        memory_order_release);
  qstQueueProgress(descriptor);
}

EQstCode qst_queue_publish(SQstShmMapping *mapping, EQstQueueId queue_id,
                           const SQstMessage *message,
                           uint64_t deadline_mono_ns) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slots = NULL;
  SQstMessage local_message = {0};
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == message) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstDeadlineExpired(deadline_mono_ns)) {
    return QST_E_DEADLINE;
  }
  code = qstGetQueue(mapping, queue_id, &descriptor, &slots);
  if (code != QST_OK) {
    return code;
  }
  memcpy(&local_message, message, sizeof(local_message));
  if (local_message.message_id == 0) {
    local_message.message_id = atomic_fetch_add_explicit(
        &mapping->header->next_message_id, 1, memory_order_relaxed);
    if (local_message.message_id == 0) {
      local_message.message_id = atomic_fetch_add_explicit(
          &mapping->header->next_message_id, 1, memory_order_relaxed);
    }
    local_message.message_crc32c = qstMessageCrc(&local_message);
  } else if (local_message.message_crc32c != qstMessageCrc(&local_message)) {
    return QST_E_SHM_CORRUPT;
  }
  if (!qstQueueCallerIsCurrent(mapping, local_message.producer_role,
                               local_message.producer_slot_id,
                               local_message.producer_generation)) {
    return QST_E_STALE_OWNER;
  }
  /* Generator publication follows block sealing and task creation, which have
   * already validated its record digest. Recompute that SHA-256 at the
   * cross-process claim boundary, while retaining producer-side block/CRC
   * validation for every referenced payload. */
  code = qstValidateMessage(mapping, queue_id, &local_message, 0, 0);
  if (code != QST_OK) {
    return code;
  }

  for (;;) {
    uint64_t position = atomic_load_explicit(&descriptor->enqueue_pos,
                                             memory_order_relaxed);
    SQstQueueSlot *slot = &slots[position & (descriptor->capacity - 1)];
    uint64_t sequence = atomic_load_explicit(&slot->sequence, memory_order_acquire);
    int64_t difference = (int64_t)(sequence - position);

    if (difference == 0) {
      if (!atomic_compare_exchange_weak_explicit(
              &descriptor->enqueue_pos, &position, position + 1,
              memory_order_acq_rel, memory_order_relaxed)) {
        continue;
      }
      /* The sequence grants exclusive reuse; do not overwrite atomic objects. */
      memset(slot->reserved, 0, sizeof(slot->reserved));
      atomic_store_explicit(&slot->owner_role, 0, memory_order_relaxed);
      atomic_store_explicit(&slot->owner_slot_id, 0, memory_order_relaxed);
      atomic_store_explicit(&slot->owner_generation, 0, memory_order_relaxed);
      atomic_store_explicit(&slot->claim_generation, 0, memory_order_relaxed);
      atomic_store_explicit(&slot->claim_seq, 0, memory_order_relaxed);
      atomic_store_explicit(&slot->lease_deadline_mono_ns, 0,
                            memory_order_relaxed);
      memcpy(&slot->message, &local_message, sizeof(local_message));
      atomic_store_explicit(&slot->state, QST_SLOT_VISIBLE,
                            memory_order_release);
      atomic_store_explicit(&slot->sequence, position + 1,
                            memory_order_release);
      qstQueueProgress(descriptor);
      if (atomic_load_explicit(&descriptor->dequeue_pos,
                               memory_order_acquire) == position) {
        qstNotifyQueue(mapping, queue_id);
      }
      return QST_OK;
    }
    if (difference < 0) {
      return qstDeadlineExpired(deadline_mono_ns) ? QST_E_DEADLINE :
                                                     QST_E_QUEUE_FULL;
    }
  }
}

EQstCode qst_queue_claim_instance(SQstShmMapping *mapping,
                                  EQstQueueId queue_id, EQstRole role,
                                  uint32_t slot_id, uint32_t generation,
                         uint64_t deadline_mono_ns, SQstClaim *out_claim) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slots = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == out_claim || role < QST_ROLE_LAUNCHER ||
      role > QST_ROLE_AI_WORKER || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstQueueConsumerIsAllowed(queue_id, role)) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (qstDeadlineExpired(deadline_mono_ns)) {
    return QST_E_DEADLINE;
  }
  if (!qstQueueCallerIsCurrent(mapping, role, slot_id, generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetQueue(mapping, queue_id, &descriptor, &slots);
  if (code != QST_OK) {
    return code;
  }

  for (;;) {
    uint64_t position = atomic_load_explicit(&descriptor->dequeue_pos,
                                             memory_order_relaxed);
    SQstQueueSlot *slot = &slots[position & (descriptor->capacity - 1)];
    uint64_t sequence = atomic_load_explicit(&slot->sequence, memory_order_acquire);
    int64_t difference = (int64_t)(sequence - (position + 1));

    if (difference == 0) {
      uint32_t expected_state = QST_SLOT_VISIBLE;
      if (!atomic_compare_exchange_weak_explicit(
              &descriptor->dequeue_pos, &position, position + 1,
              memory_order_acq_rel, memory_order_relaxed)) {
        continue;
      }
      if (!atomic_compare_exchange_strong_explicit(
              &slot->state, &expected_state, QST_SLOT_CLAIMED,
              memory_order_acq_rel, memory_order_acquire)) {
        return QST_E_SHM_CORRUPT;
      }
      atomic_store_explicit(&slot->owner_role, role, memory_order_relaxed);
      atomic_store_explicit(&slot->owner_slot_id, slot_id,
                            memory_order_relaxed);
      atomic_store_explicit(&slot->owner_generation, generation,
                            memory_order_relaxed);
      atomic_store_explicit(&slot->claim_generation, generation,
                            memory_order_relaxed);
      atomic_store_explicit(&slot->claim_seq, position + 1,
                            memory_order_relaxed);
      atomic_store_explicit(&slot->lease_deadline_mono_ns, deadline_mono_ns,
                            memory_order_release);
      memset(out_claim, 0, sizeof(*out_claim));
      out_claim->queue_id = queue_id;
      out_claim->slot_index = (uint32_t)(position & (descriptor->capacity - 1));
      out_claim->owner_role = role;
      out_claim->owner_slot_id = slot_id;
      out_claim->owner_generation = generation;
      out_claim->queue_position = position;
      out_claim->claim_seq = position + 1;
      out_claim->lease_deadline_mono_ns = deadline_mono_ns;
      memcpy(&out_claim->message, &slot->message, sizeof(out_claim->message));
      return qstValidateMessage(mapping, queue_id, &out_claim->message, 1, 1);
    }
    if (difference < 0) {
      return qstDeadlineExpired(deadline_mono_ns) ? QST_E_DEADLINE :
                                                     QST_E_QUEUE_EMPTY;
    }
  }
}

EQstCode qst_queue_claim(SQstShmMapping *mapping, EQstQueueId queue_id,
                         EQstRole role, uint32_t generation,
                         uint64_t deadline_mono_ns, SQstClaim *out_claim) {
  return qst_queue_claim_instance(mapping, queue_id, role, 0, generation,
                                  deadline_mono_ns, out_claim);
}

static EQstCode qstValidateClaim(SQstShmMapping *mapping,
                                 const SQstClaim *claim,
                                 SQstQueueDescriptor **out_descriptor,
                                 SQstQueueSlot **out_slot) {
  SQstQueueSlot *slots = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == claim || NULL == out_descriptor ||
      NULL == out_slot || claim->slot_index >= UINT32_MAX) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstGetQueue(mapping, (EQstQueueId)claim->queue_id, out_descriptor,
                     &slots);
  if (code != QST_OK || claim->slot_index >= (*out_descriptor)->capacity ||
      claim->slot_index !=
          (claim->queue_position & ((*out_descriptor)->capacity - 1))) {
    return code == QST_OK ? QST_E_STALE_OWNER : code;
  }
  *out_slot = &slots[claim->slot_index];
  if (atomic_load_explicit(&(*out_slot)->sequence, memory_order_acquire) !=
          claim->queue_position + 1 ||
      atomic_load_explicit(&(*out_slot)->state, memory_order_acquire) !=
          QST_SLOT_CLAIMED ||
      atomic_load_explicit(&(*out_slot)->owner_role, memory_order_acquire) !=
          claim->owner_role ||
      atomic_load_explicit(&(*out_slot)->owner_slot_id,
                           memory_order_acquire) != claim->owner_slot_id ||
      atomic_load_explicit(&(*out_slot)->owner_generation,
                           memory_order_acquire) != claim->owner_generation ||
      atomic_load_explicit(&(*out_slot)->claim_seq, memory_order_acquire) !=
          claim->claim_seq) {
    return QST_E_STALE_OWNER;
  }
  if (!qstQueueCallerIsCurrent(mapping, claim->owner_role,
                               claim->owner_slot_id,
                               claim->owner_generation)) {
    return QST_E_STALE_OWNER;
  }
  if (qstDeadlineExpired(claim->lease_deadline_mono_ns)) {
    return QST_E_QUEUE_LEASE_EXPIRED;
  }
  return QST_OK;
}

static int32_t qstClaimOwnerHasParent(const SQstShmMapping *mapping,
                                      const SQstClaim *claim,
                                      EQstRole parent_role,
                                      uint32_t parent_slot_id,
                                      uint32_t parent_generation) {
  const SQstRegion *region = NULL;
  SQstProcessSlot *slots = NULL;

  if (claim->owner_role == QST_ROLE_LAUNCHER) {
    return 0;
  }
  region = &mapping->header->process_region;
  if (region->element_bytes != sizeof(SQstProcessSlot) ||
      region->element_count == 0 || region->region_offset > mapping->bytes ||
      region->region_bytes !=
          (uint64_t)region->element_count * region->element_bytes ||
      region->region_bytes > mapping->bytes - region->region_offset) {
    return 0;
  }
  slots = (SQstProcessSlot *)((uint8_t *)mapping->base + region->region_offset);
  for (uint32_t index = 0; index < region->element_count; ++index) {
    if (atomic_load_explicit(&slots[index].role, memory_order_acquire) ==
            claim->owner_role &&
        slots[index].slot_id == claim->owner_slot_id &&
        atomic_load_explicit(&slots[index].generation, memory_order_acquire) ==
            claim->owner_generation &&
        slots[index].parent_role == parent_role &&
        slots[index].parent_slot_id == parent_slot_id &&
        slots[index].parent_generation == parent_generation) {
      return 1;
    }
  }
  return 0;
}

EQstCode qst_queue_ack(SQstShmMapping *mapping, const SQstClaim *claim,
                       EQstAckProofKind proof_kind, uint64_t proof_id) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slot = NULL;
  uint64_t enqueue_position = 0;
  uint64_t dequeue_position = 0;
  int32_t queue_was_full = 0;
  uint32_t expected_state = QST_SLOT_CLAIMED;
  EQstCode code = qstValidateClaim(mapping, claim, &descriptor, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (proof_id == 0 ||
      (claim->queue_id == QST_QUEUE_CONTROL &&
       proof_kind != QST_ACK_CONTROL_RESPONSE) ||
      (claim->queue_id == QST_QUEUE_GENERATE &&
       proof_kind != QST_ACK_TASK_DISPATCHED) ||
      (claim->queue_id == QST_QUEUE_EXECUTE &&
       proof_kind != QST_ACK_PERSIST_WORK_ACCEPTED) ||
      (claim->queue_id == QST_QUEUE_PERSIST &&
       proof_kind != QST_ACK_DURABILITY_APPLIED)) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &slot->state, &expected_state, QST_SLOT_ACKED, memory_order_acq_rel,
          memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  /* Wake a producer only when this acknowledgement releases a slot from a
   * full queue. Unconditional notifications turn every execute ACK into an
   * eventfd write and can overwhelm the pair producers with wakeups. */
  enqueue_position = atomic_load_explicit(&descriptor->enqueue_pos,
                                          memory_order_acquire);
  dequeue_position = atomic_load_explicit(&descriptor->dequeue_pos,
                                          memory_order_acquire);
  queue_was_full = enqueue_position - dequeue_position >= descriptor->capacity;
  qstReleaseSlot(descriptor, slot, claim->queue_position);
  if (queue_was_full != 0) {
    qstNotifyQueue(mapping, (EQstQueueId)claim->queue_id);
  }
  return QST_OK;
}

EQstCode qst_queue_abandon(SQstShmMapping *mapping, const SQstClaim *claim,
                           EQstCode reason) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slot = NULL;
  uint32_t expected_state = QST_SLOT_CLAIMED;
  EQstCode code = qstValidateClaim(mapping, claim, &descriptor, &slot);

  if (code != QST_OK) {
    return code;
  }
  if (reason == QST_OK ||
      !atomic_compare_exchange_strong_explicit(
          &slot->state, &expected_state, QST_SLOT_ABANDONED,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  qstQueueProgress(descriptor);
  return QST_OK;
}

EQstCode qst_queue_reclaim_claim(SQstShmMapping *mapping,
                                 const SQstClaim *claim,
                                 EQstRole parent_role,
                                 uint32_t parent_slot_id,
                                 uint32_t parent_generation,
                                 EQstCode reason) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slots = NULL;
  SQstQueueSlot *slot = NULL;
  uint32_t expected_state = QST_SLOT_CLAIMED;
  EQstCode code = QST_OK;

  if (NULL == mapping || NULL == claim || reason == QST_OK ||
      parent_role < QST_ROLE_LAUNCHER || parent_role > QST_ROLE_AI_WORKER ||
      parent_generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstQueueCallerIsCurrent(mapping, parent_role, parent_slot_id,
                               parent_generation) ||
      !qstClaimOwnerHasParent(mapping, claim, parent_role, parent_slot_id,
                              parent_generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetQueue(mapping, (EQstQueueId)claim->queue_id, &descriptor,
                     &slots);
  if (code != QST_OK || claim->slot_index >= descriptor->capacity ||
      claim->slot_index !=
          (claim->queue_position & (descriptor->capacity - 1))) {
    return code == QST_OK ? QST_E_STALE_OWNER : code;
  }
  slot = &slots[claim->slot_index];
  if (atomic_load_explicit(&slot->sequence, memory_order_acquire) !=
          claim->queue_position + 1 ||
      atomic_load_explicit(&slot->state, memory_order_acquire) !=
          QST_SLOT_CLAIMED ||
      atomic_load_explicit(&slot->owner_role, memory_order_acquire) !=
          claim->owner_role ||
      atomic_load_explicit(&slot->owner_slot_id, memory_order_acquire) !=
          claim->owner_slot_id ||
      atomic_load_explicit(&slot->owner_generation, memory_order_acquire) !=
          claim->owner_generation ||
      atomic_load_explicit(&slot->claim_seq, memory_order_acquire) !=
          claim->claim_seq) {
    return QST_E_STALE_OWNER;
  }
  if (!atomic_compare_exchange_strong_explicit(
          &slot->state, &expected_state, QST_SLOT_ABANDONED,
          memory_order_acq_rel, memory_order_acquire)) {
    return QST_E_STALE_OWNER;
  }
  qstQueueProgress(descriptor);
  return QST_OK;
}

EQstCode qst_queue_requeue_abandoned(SQstShmMapping *mapping,
                                     EQstQueueId queue_id,
                                     uint64_t message_id, EQstRole role,
                                     uint32_t generation,
                                     uint64_t deadline_mono_ns) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slots = NULL;
  EQstCode code = QST_OK;

  if (NULL == mapping || message_id == 0 || role < QST_ROLE_LAUNCHER ||
      role > QST_ROLE_AI_WORKER || generation == 0) {
    return QST_E_INVALID_ARGUMENT;
  }
  if (!qstQueueCallerIsCurrent(mapping, role, 0, generation)) {
    return QST_E_STALE_OWNER;
  }
  code = qstGetQueue(mapping, queue_id, &descriptor, &slots);
  if (code != QST_OK) {
    return code;
  }
  for (uint64_t index = 0; index < descriptor->capacity; ++index) {
    SQstQueueSlot *slot = &slots[index];
    uint32_t expected_state = QST_SLOT_ABANDONED;
    SQstMessage message = {0};
    uint64_t sequence = 0;

    if (atomic_load_explicit(&slot->state, memory_order_acquire) !=
            QST_SLOT_ABANDONED || slot->message.message_id != message_id) {
      continue;
    }
    memcpy(&message, &slot->message, sizeof(message));
    message.producer_role = role;
    message.producer_slot_id = 0;
    message.producer_generation = generation;
    message.message_crc32c = qstMessageCrc(&message);
    code = qst_queue_publish(mapping, queue_id, &message, deadline_mono_ns);
    if (code != QST_OK) {
      return code;
    }
    if (!atomic_compare_exchange_strong_explicit(
            &slot->state, &expected_state, QST_SLOT_ACKED, memory_order_acq_rel,
            memory_order_acquire)) {
      return QST_E_STALE_OWNER;
    }
    sequence = atomic_load_explicit(&slot->sequence, memory_order_acquire);
    if (sequence == 0 || (sequence - 1) % descriptor->capacity != index) {
      return QST_E_SHM_CORRUPT;
    }
    qstReleaseSlot(descriptor, slot, sequence - 1);
    return QST_OK;
  }
  return QST_E_QUEUE_EMPTY;
}

EQstCode qst_queue_snapshot(const SQstShmMapping *mapping,
                            EQstQueueId queue_id,
                            SQstQueueSnapshot *out_snapshot) {
  SQstQueueDescriptor *descriptor = NULL;
  SQstQueueSlot *slots = NULL;
  EQstCode code = QST_OK;

  if (NULL == out_snapshot) {
    return QST_E_INVALID_ARGUMENT;
  }
  code = qstGetQueue(mapping, queue_id, &descriptor, &slots);
  if (code != QST_OK) {
    return code;
  }
  (void)slots;
  out_snapshot->enqueue_pos = atomic_load_explicit(&descriptor->enqueue_pos,
                                                    memory_order_acquire);
  out_snapshot->dequeue_pos = atomic_load_explicit(&descriptor->dequeue_pos,
                                                    memory_order_acquire);
  out_snapshot->progress_epoch = atomic_load_explicit(
      &descriptor->progress_epoch, memory_order_acquire);
  return QST_OK;
}
